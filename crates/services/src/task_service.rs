use crate::{
    agent_service::{compute_effective_status, EffectiveStatus},
    lifecycle::{LifecycleHookContext, LifecycleHookRun, LifecycleHookRunner},
    memory::MemoryService,
    merge_service::MergeService,
    terminal_service::TerminalActivityTracker,
    workflow::{default_states, engine::WorkflowEngine},
    workspace_cleanup::WorkspaceCleanupScheduler,
    workspace_execution_lock::WorkspaceExecutionLockManager,
    Assignee, Result, ServiceError,
};
use ::review::{ReviewRequest, ReviewRunner};
use ::workspace::{RepoCacheLockManager, WorkspaceManager};
use api_types::{Actor, ProjectSettings, UserActionSource};
use cli_adapters::codex::protocol::RESUME_THREAD_ID_CONFIG_KEY;
use db::{
    new_uuid_v4, now_rfc3339, Agent, AgentRepo, ArchiveTask, AssigneeKind, ClaimExecutionLease,
    ClaimTask, ClaimedTask, CommentAuthorType, CreateDomainEvent, CreateExecution, CreateTask,
    CreateTaskComment, CreateTaskRoleAssignment, CreateWorkspace, CreateWorkspaceLease, DbError,
    DomainEventRepo, Execution, ExecutionLeaseDisposition, ExecutionRepo, ExecutionStatus,
    ExecutionTerminalOutcome, ExecutionUsageRepo, PageRequest, ProjectRepo, RepoRepo, Review,
    ReviewRepo, ReviewStatus, SoftDeleteTask, SortBy, SortOrder, SqliteDb, Task, TaskBoardRepo,
    TaskComment, TaskCommentRepo, TaskDependencyRepo, TaskMetadata, TaskRepo, TaskRoleAssignment,
    TaskRoleAssignmentRepo, TaskStatus, TerminalizeExecution, TransitionLogRepo,
    UpsertExecutionUsage, Workspace, WorkspaceLeaseRepo, WorkspaceRepo, WorkspaceStatus,
};
use events::{event_timestamp, EventBus, EventContext, ForgeEvent};
use executors::{
    merge_overrides, resolve_config_value, ExecutionContext, ExecutionOutcome, ExecutionOverrides,
    ExecutorKind, TaskExecutor,
};
use serde_json::{json, Value};
use sqlx::Row;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::process::Command;
use tokio::sync::Mutex;
use uuid::Uuid;

pub mod action_resolver;
mod actions;
pub use actions::task_review_requires_user_decision;
mod adaptive;
mod claim;
mod common;
pub(crate) mod config;
mod create;
mod create_subtasks;
mod execution;
mod governance;
mod lifecycle_test;
pub(crate) mod logs;
mod move_task;
mod proposal;
mod reorder_subtasks;
mod review;
mod review_config;
mod roles;
mod subtask;
mod transition;
mod validation;
pub(crate) mod workspace;

pub use actions::TaskActionResult;
pub use adaptive::{
    AdaptiveTaskChild, AdaptiveTaskCommand, AdaptiveTaskCommandResult, AdaptiveTaskOperation,
};
pub use create_subtasks::NewSubtaskInput;
pub use execution::subtasks::build_first_turn_prompt_from_context;
pub use proposal::{
    DirectTaskProposalInput, TaskProposalCommandResult, TaskProposalPayload, TASK_PROPOSE_COMMAND,
};
pub use subtask::{is_root_task, is_subtask, root_for};

#[cfg(test)]
use self::config::{
    execution_overrides_to_config_layer, merge_config_layers, override_value_or_empty,
    parse_config_override_layer, OverridesApplied,
};
use self::{
    config::{
        build_executor_config_snapshot, create_failed_execution_record,
        executor_snapshot_with_resume_thread, parse_json_value, truncate_utf8_bytes,
    },
    logs::execution_logs_path,
    review_config::review_config_from_json,
    validation::{serialize_config, validate_required},
    workspace::{default_workspace_root, prepare_workspace, reset_workspace},
};

pub(super) const DISPATCH_STATUS_POLL_INTERVAL: Duration = Duration::from_secs(10);
pub(super) const DISPATCH_STATUS_WAIT_CEILING: Duration = Duration::from_secs(10 * 60);
pub(super) const MAX_FOLLOW_UP_DIFF_BYTES: usize = 64 * 1024;

/// Remote terminal notifications are transport input, not trusted event
/// metadata. Keep every free-form value copied into a late-result diagnostic
/// small and single-line, and replace values that look credential-bearing.
const REMOTE_TERMINAL_DIAGNOSTIC_MAX_CHARS: usize = 256;
const REMOTE_TERMINAL_DIAGNOSTIC_REDACTED: &str = "[redacted]";
const REMOTE_TERMINAL_MAX_CAS_RETRIES: u8 = 2;

const REMOTE_TERMINAL_SECRET_MARKERS: &[&str] = &[
    "api_key",
    "api-key",
    "apikey",
    "authorization",
    "bearer ",
    "client_secret",
    "credential",
    "password",
    "private_key",
    "refresh_token",
    "secret",
    "token=",
    "token:",
];

/// Normalize an untrusted remote diagnostic before it is put in a durable
/// execution row or domain event. Detection happens before truncation so a
/// secret placed after the visible prefix cannot evade redaction.
fn bounded_redacted_remote_diagnostic(value: &str) -> String {
    let normalized = value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>();
    let trimmed = normalized.trim();
    let lower = trimmed.to_ascii_lowercase();
    if REMOTE_TERMINAL_SECRET_MARKERS
        .iter()
        .any(|marker| lower.contains(marker))
    {
        REMOTE_TERMINAL_DIAGNOSTIC_REDACTED.to_owned()
    } else {
        trimmed
            .chars()
            .take(REMOTE_TERMINAL_DIAGNOSTIC_MAX_CHARS)
            .collect()
    }
}

fn bounded_redacted_remote_optional(value: Option<&str>) -> Option<String> {
    value.map(bounded_redacted_remote_diagnostic)
}

/// The owner is generated from the authenticated daemon connection, never
/// copied from notification data. It is therefore an opaque, stable server
/// reference and must remain usable for late-result attribution.
fn stable_remote_owner_ref(owner: &str) -> String {
    owner.to_owned()
}

fn remote_execution_is_live_for_terminal(
    execution: &Execution,
    lease_owner: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    execution.status == ExecutionStatus::Running
        && execution.lease_owner.as_deref() == Some(lease_owner)
        && execution
            .lease_expires_at
            .as_deref()
            .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
            .is_some_and(|expires_at| expires_at > now)
        && execution
            .hard_deadline_at
            .as_deref()
            .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
            .is_some_and(|deadline| deadline > now)
}

pub(super) fn is_transient_error_annotation(raw_annotation: &str) -> bool {
    let Ok(annotation) = serde_json::from_str::<Value>(raw_annotation) else {
        return false;
    };

    matches!(
        annotation.get("type").and_then(Value::as_str),
        Some(
            "merge_conflict"
                | "dirty_worktree"
                | "target_repo_dirty"
                | "executor_failed"
                | "review_budget_exhausted"
                | "merge_fix_budget_exhausted"
                | "merge_fix_ci_failed"
        )
    )
}

#[derive(Clone)]
pub struct TaskService {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
    merge_service: Option<Arc<MergeService>>,
    cleanup_scheduler: Option<Arc<WorkspaceCleanupScheduler>>,
    review_runner: Option<Arc<ReviewRunner>>,
    task_executor: Option<Arc<dyn TaskExecutor>>,
    daemon_connections: Option<Arc<crate::daemon_transport::DaemonConnectionRegistry>>,
    workspace_exec_locks: Option<Arc<WorkspaceExecutionLockManager>>,
    terminal_activity: Option<Arc<TerminalActivityTracker>>,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
    workspace_root: PathBuf,
    memory_service: Arc<MemoryService>,
    move_operation_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    credential_env: Option<Arc<crate::embedded_agent_service::EmbeddedAgentService>>,
}

#[derive(Debug)]
pub struct TransitionResult {
    pub task: Task,
    pub review: Option<Review>,
}

pub struct TransitionOptions {
    pub version: i64,
    pub reason: Option<String>,
    pub triggered_by: Actor,
    pub rejection: bool,
    pub defer_dispatch_seconds: Option<i64>,
}

impl From<i64> for TransitionOptions {
    fn from(version: i64) -> Self {
        Self {
            version,
            reason: None,
            triggered_by: Actor::system(api_types::SystemComponent::General),
            rejection: false,
            defer_dispatch_seconds: None,
        }
    }
}

impl From<(i64, Option<String>)> for TransitionOptions {
    fn from((version, reason): (i64, Option<String>)) -> Self {
        Self {
            version,
            reason,
            triggered_by: Actor::user(UserActionSource::Api),
            rejection: false,
            defer_dispatch_seconds: None,
        }
    }
}

impl From<(i64, Option<String>, bool)> for TransitionOptions {
    fn from((version, reason, rejection): (i64, Option<String>, bool)) -> Self {
        Self {
            version,
            reason,
            triggered_by: Actor::user(UserActionSource::Api),
            rejection,
            defer_dispatch_seconds: None,
        }
    }
}

pub struct LaunchExecutionResult {
    pub task: Task,
    pub execution: Execution,
    pub workspace: Workspace,
}

impl TaskService {
    pub fn new(db: Arc<SqliteDb>, event_bus: Arc<EventBus>) -> Self {
        let memory_service = Arc::new(MemoryService::new(Arc::clone(&db)));
        Self {
            db,
            event_bus,
            merge_service: None,
            cleanup_scheduler: None,
            review_runner: None,
            task_executor: None,
            daemon_connections: None,
            workspace_exec_locks: None,
            terminal_activity: None,
            repo_cache_locks: None,
            workspace_root: default_workspace_root(),
            memory_service,
            move_operation_locks: Arc::new(Mutex::new(HashMap::new())),
            credential_env: None,
        }
    }

    pub fn with_merge_service(mut self, merge_service: Arc<MergeService>) -> Self {
        self.merge_service = Some(merge_service);
        self
    }

    pub(crate) async fn publish_domain_event_by_dedupe(&self, dedupe_key: &str) {
        let service =
            crate::DomainEventService::new(Arc::clone(&self.db), Arc::clone(&self.event_bus));
        if let Err(error) = service.publish_by_dedupe(dedupe_key).await {
            tracing::warn!(dedupe_key, %error, "failed to mirror committed domain event");
        }
    }

    pub fn with_review_runner(mut self, review_runner: Arc<ReviewRunner>) -> Self {
        self.review_runner = Some(review_runner);
        self
    }

    pub fn with_task_executor(mut self, task_executor: Arc<dyn TaskExecutor>) -> Self {
        self.task_executor = Some(task_executor);
        self
    }

    pub fn with_daemon_connections(
        mut self,
        daemon_connections: Arc<crate::daemon_transport::DaemonConnectionRegistry>,
    ) -> Self {
        self.daemon_connections = Some(daemon_connections);
        self
    }

    pub fn with_workspace_exec_locks(mut self, locks: Arc<WorkspaceExecutionLockManager>) -> Self {
        self.workspace_exec_locks = Some(locks);
        self
    }

    pub fn with_terminal_activity_tracker(
        mut self,
        terminal_activity: Arc<TerminalActivityTracker>,
    ) -> Self {
        self.terminal_activity = Some(terminal_activity);
        self
    }

    pub fn with_repo_cache_locks(mut self, locks: Arc<RepoCacheLockManager>) -> Self {
        self.repo_cache_locks = Some(locks);
        self
    }

    pub fn with_cleanup_scheduler(
        mut self,
        cleanup_scheduler: Arc<WorkspaceCleanupScheduler>,
    ) -> Self {
        self.cleanup_scheduler = Some(cleanup_scheduler);
        self
    }

    pub fn with_workspace_root(mut self, workspace_root: PathBuf) -> Self {
        self.workspace_root = workspace_root;
        self
    }

    pub fn with_memory_service(mut self, memory_service: Arc<MemoryService>) -> Self {
        self.memory_service = memory_service;
        self
    }

    /// Enables `auth_source: forge_provider` dispatch: harness executions for
    /// agents referencing a provider entry get the entry's API key injected
    /// into their in-memory executor environment.
    pub fn with_provider_credential_env(
        mut self,
        embedded: Arc<crate::embedded_agent_service::EmbeddedAgentService>,
    ) -> Self {
        self.credential_env = Some(embedded);
        self
    }

    fn publish(&self, event: ForgeEvent) {
        self.event_bus.publish(event);
    }

    /// Prepare the scheduler-owned lease installed with a newly-created
    /// running execution.  A currently authenticated daemon connection owns
    /// the attempt from the first row write; all other dispatches receive an
    /// already-expired pending marker which recovery can reclaim.
    pub(crate) async fn initial_execution_lease(
        &self,
        input: &CreateExecution,
    ) -> Result<ClaimExecutionLease> {
        if input.status != ExecutionStatus::Running {
            return Err(ServiceError::invalid_operation(
                "initial execution leases require a running execution",
            ));
        }
        let snapshot = serde_json::from_str::<Value>(
            input
                .executor_config_snapshot_json
                .as_deref()
                .unwrap_or("{}"),
        )
        .unwrap_or(Value::Null);
        let hard_deadline_at = execution::rfc3339_after(
            &input.updated_at,
            i64::try_from(execution::execution_deadline_seconds(&snapshot)).unwrap_or(i64::MAX),
        );
        let remote_owner = if let Some(agent_id) = input.agent_id.as_deref() {
            AgentRepo::get_by_id(&*self.db, agent_id)
                .await?
                .and_then(|agent| {
                    agent.daemon_id.as_deref().and_then(|daemon_id| {
                        self.daemon_connections.as_ref().and_then(|registry| {
                            registry.get(daemon_id).map(|connection| {
                                crate::daemon_transport::execution_lease_owner(
                                    daemon_id,
                                    connection.id(),
                                )
                            })
                        })
                    })
                })
        } else {
            None
        };
        let (owner, lease_expires_at) = match remote_owner {
            Some(owner) => (
                owner,
                execution::bounded_lease_expiry(&input.updated_at, &hard_deadline_at),
            ),
            None => (
                format!("dispatch-pending:{}", input.id),
                input.updated_at.clone(),
            ),
        };
        Ok(ClaimExecutionLease {
            execution_id: input.id.clone(),
            expected_version: 1,
            owner,
            lease_expires_at,
            hard_deadline_at,
            now: input.updated_at.clone(),
        })
    }

    /// Create a running execution and remove a freshly prepared workspace if
    /// the authoritative in-transaction admission guard rejects it. Existing
    /// workspaces are intentionally retained for retries/recovery; only a
    /// workspace created by this attempt is rolled back.
    pub(crate) async fn create_running_execution(
        &self,
        input: CreateExecution,
        workspace_created_by_attempt: bool,
    ) -> Result<Execution> {
        let repository_context = if let Some(workspace_id) = input.workspace_id.as_deref() {
            let task = TaskRepo::get_by_id(&*self.db, &input.task_id, false)
                .await?
                .ok_or_else(|| ServiceError::not_found("task", input.task_id.clone()))?;
            let workspace = WorkspaceRepo::get_by_id(&*self.db, workspace_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("workspace", workspace_id.to_owned()))?;
            Some((task, workspace))
        } else {
            let task = TaskRepo::get_by_id(&*self.db, &input.task_id, false)
                .await?
                .ok_or_else(|| ServiceError::not_found("task", input.task_id.clone()))?;
            if task.repo_id.is_some() {
                return Err(ServiceError::invalid_operation(
                    "repository execution requires a scheduler WorkspaceLease-backed workspace",
                ));
            }
            None
        };

        let create_result = if input.status == ExecutionStatus::Running {
            let lease = self.initial_execution_lease(&input).await?;
            ExecutionRepo::create_with_lease(&*self.db, input.clone(), lease).await
        } else {
            ExecutionRepo::create(&*self.db, input.clone()).await
        };
        let execution = match create_result {
            Ok(execution) => execution,
            Err(error) => {
                if workspace_created_by_attempt {
                    self.cleanup_fresh_execution_workspace_by_id(
                        &input.task_id,
                        input.workspace_id.as_deref(),
                    )
                    .await;
                }
                return Err(error.into());
            }
        };
        if let Some((task, workspace)) = repository_context.as_ref() {
            if let Err(error) = self
                .issue_workspace_lease(
                    task,
                    workspace,
                    &input.role,
                    input.agent_id.as_deref(),
                    &input.id,
                )
                .await
            {
                if let Err(mark_error) = self
                    .fail_execution_before_dispatch(&execution.id, error.to_string())
                    .await
                {
                    tracing::warn!(
                        execution_id = %execution.id,
                        %mark_error,
                        "failed to terminalize execution after WorkspaceLease rejection"
                    );
                }
                if workspace_created_by_attempt {
                    self.cleanup_fresh_execution_workspace(task, workspace)
                        .await;
                }
                return Err(error);
            }
        }
        Ok(execution)
    }

    pub(crate) async fn cleanup_fresh_execution_workspace(
        &self,
        task: &Task,
        workspace: &Workspace,
    ) {
        self.cleanup_fresh_execution_workspace_by_id(&task.id, Some(&workspace.id))
            .await;
    }

    async fn cleanup_fresh_execution_workspace_by_id(
        &self,
        task_id: &str,
        workspace_id: Option<&str>,
    ) {
        let mut removed_workspace = false;
        if let Some(workspace_id) = workspace_id {
            // Delete only our workspace row and only while no execution has
            // acquired it. This protects a concurrent launch which reused
            // the same Task workspace after this attempt lost admission.
            match sqlx::query(
                "DELETE FROM workspace
                 WHERE id = ? AND task_id = ?
                   AND NOT EXISTS (
                       SELECT 1 FROM execution
                       WHERE execution.workspace_id = workspace.id
                   )",
            )
            .bind(workspace_id)
            .bind(task_id)
            .execute(self.db.pool())
            .await
            {
                Ok(result) => removed_workspace = result.rows_affected() == 1,
                Err(cleanup_error) => tracing::warn!(
                    task_id,
                    workspace_id,
                    %cleanup_error,
                    "failed to remove workspace row after rejected execution"
                ),
            }
        }
        let mut manager = WorkspaceManager::new(self.workspace_root.clone());
        if let Some(locks) = self.repo_cache_locks.clone() {
            manager = manager.with_repo_cache_locks(locks);
        }
        if removed_workspace {
            if let Err(cleanup_error) = manager.cleanup_worktree(task_id).await {
                tracing::warn!(
                    task_id,
                    %cleanup_error,
                    "failed to remove fresh worktree after rejected execution"
                );
            }
        }
    }

    /// Retain a bounded diagnostic when a terminal notification arrives after
    /// another actor already won the execution CAS.  The terminal CAS event
    /// persists the owner that was displaced (`previous_lease_owner`), which
    /// lets this method attribute a late result even after terminalization has
    /// cleared the execution row's lease owner.  If that durable proof is
    /// absent, the notification is intentionally dropped.
    pub(crate) async fn record_late_remote_terminal(
        &self,
        daemon_id: &str,
        connection_id: u64,
        notification: &api_types::ExecutionTerminalNotification,
    ) -> Result<()> {
        if !self
            .daemon_connections
            .as_ref()
            .is_none_or(|registry| registry.is_current(daemon_id, connection_id))
        {
            return Ok(());
        }
        validate_required("execution_id", &notification.execution_id)?;
        let lease_owner = crate::daemon_transport::execution_lease_owner(daemon_id, connection_id);
        let Some(execution) =
            ExecutionRepo::get_by_id(&*self.db, &notification.execution_id).await?
        else {
            return Ok(());
        };
        if execution.status == ExecutionStatus::Running {
            return Ok(());
        }
        let terminal_event = sqlx::query(
            "SELECT id
             FROM domain_event
             WHERE entity_type = 'task'
               AND entity_id = ?
               AND event_type IN ('execution.completed', 'execution.failed', 'execution.cancelled')
               AND json_extract(payload_json, '$.previous_lease_owner') = ?
               AND json_extract(payload_json, '$.execution_id') = ?
             ORDER BY sequence DESC
             LIMIT 1",
        )
        .bind(&execution.task_id)
        .bind(&lease_owner)
        .bind(&execution.id)
        .fetch_optional(self.db.pool())
        .await?;
        let Some(terminal_event) = terminal_event else {
            // Monitor/user terminalization did not persist this connection as
            // the winning owner, so the late result cannot be attributed
            // safely and must not create a misleading diagnostic.
            return Ok(());
        };
        let terminal_event_id: String = terminal_event.try_get("id")?;
        let task = TaskRepo::get_by_id(&*self.db, &execution.task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", execution.task_id.clone()))?;
        let event_id = new_uuid_v4();
        let dedupe_key = format!(
            "execution-late-terminal:{}:{}:{}",
            execution.id, lease_owner, terminal_event_id
        );
        let payload_json = serde_json::json!({
            "execution_id": execution.id,
            "task_id": execution.task_id,
            "status": execution.status.to_string(),
            "notification_status": bounded_redacted_remote_optional(notification.status.as_deref()),
            "exit_code": notification.exit_code,
            "signal": bounded_redacted_remote_optional(notification.signal.as_deref()),
            "error": bounded_redacted_remote_optional(notification.error.as_deref()),
            "connection_owner": stable_remote_owner_ref(&lease_owner),
            "reason": "terminal_cas_already_won",
        })
        .to_string();
        DomainEventRepo::append_event(
            &*self.db,
            CreateDomainEvent {
                id: event_id.clone(),
                event_type: "execution.late_terminal_rejected".to_owned(),
                entity_type: "task".to_owned(),
                entity_id: execution.task_id.clone(),
                actor_type: "daemon".to_owned(),
                actor_id: Some(stable_remote_owner_ref(&lease_owner)),
                scope_type: "project".to_owned(),
                scope_id: task.project_id,
                correlation_id: format!("remote-execution-late:{}", execution.id),
                causation_id: Some(terminal_event_id),
                causation_depth: 1,
                dedupe_key: Some(dedupe_key),
                payload_json,
                created_at: now_rfc3339(),
            },
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn complete_remote_execution(
        &self,
        daemon_id: &str,
        connection_id: u64,
        notification: api_types::ExecutionTerminalNotification,
    ) -> Result<ExecutionTerminalOutcome> {
        validate_required("execution_id", &notification.execution_id)?;
        if !self
            .daemon_connections
            .as_ref()
            .is_none_or(|registry| registry.is_current(daemon_id, connection_id))
        {
            return Ok(ExecutionTerminalOutcome::Concurrent { current: None });
        }
        let lease_owner = crate::daemon_transport::execution_lease_owner(daemon_id, connection_id);
        let current_execution = ExecutionRepo::get_by_id(&*self.db, &notification.execution_id)
            .await?
            .ok_or_else(|| {
                ServiceError::not_found("execution", notification.execution_id.clone())
            })?;
        if !remote_execution_is_live_for_terminal(
            &current_execution,
            &lease_owner,
            chrono::Utc::now(),
        ) {
            // A monitor/cancellation/connection replacement may already have
            // won the execution CAS. Return a typed concurrent outcome so the
            // caller cannot cascade Task state or publish a second terminal
            // result from this stale remote notification.
            return Ok(ExecutionTerminalOutcome::Concurrent {
                current: Some(current_execution),
            });
        }

        let task = TaskRepo::get_by_id(&*self.db, &current_execution.task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", current_execution.task_id.clone()))?;
        let signal = notification
            .signal
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let error = notification
            .error
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let succeeded = notification.exit_code == Some(0) && signal.is_none() && error.is_none();
        let outcome = notification.status.as_deref().unwrap_or(if succeeded {
            "completed"
        } else {
            "failed"
        });
        let (status, stop_reason, stopped_by, stopped_at, terminal_error) = match outcome {
            "completed" => (ExecutionStatus::Completed, None, None, None, None),
            "cancelled" => (
                ExecutionStatus::Cancelled,
                Some(db::StopReason::ExecutorCancelled),
                Some(Actor::system(api_types::SystemComponent::Executor).display()),
                Some(notification.ts.clone()),
                None,
            ),
            _ => (
                ExecutionStatus::Failed,
                Some(db::StopReason::ExecutorFailed),
                Some(Actor::system(api_types::SystemComponent::Executor).display()),
                Some(notification.ts.clone()),
                Some(remote_terminal_error_message(
                    notification.exit_code,
                    signal,
                    error,
                )),
            ),
        };

        let executor_unavailable = notification.failure_class
            == Some(api_types::RemoteExecutionFailureClass::ExecutorUnavailable);
        let route_outcome = crate::task_service::config::RouteOutcome {
            selected: notification.resolved_candidate.as_ref().map(|candidate| {
                (
                    candidate.candidate_key.clone(),
                    candidate.executor_type.clone(),
                    candidate.config.clone(),
                )
            }),
            attempts: notification
                .route_attempts
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|attempt| (attempt.candidate_key.clone(), attempt.outcome.clone()))
                .collect(),
            unavailable_retry_at: executor_unavailable.then(|| notification.retry_at.clone()),
        };
        let snapshot_update = match current_execution.executor_config_snapshot_json.as_deref() {
            Some(snapshot) => crate::task_service::config::apply_route_outcome_to_snapshot(
                snapshot,
                &route_outcome,
            )?,
            None => None,
        };

        // Heartbeats advance execution_version.  A terminal notification that
        // raced one of those renewals must retry against the same owner rather
        // than being mistaken for a competing terminal winner.  The owner
        // predicate remains in every attempt, so a replacement connection or
        // expiry monitor still wins immediately.
        let terminal_resume_policy = match &status {
            ExecutionStatus::Completed => Some(None),
            ExecutionStatus::Cancelled | ExecutionStatus::Failed => {
                Some(Some(db::ResumePolicy::Manual))
            }
            ExecutionStatus::Running => unreachable!("remote terminal status cannot be running"),
        };
        let mut terminal_candidate = current_execution.clone();
        let mut terminal_retries = 0u8;
        let terminal_outcome = loop {
            if !self
                .daemon_connections
                .as_ref()
                .is_none_or(|registry| registry.is_current(daemon_id, connection_id))
            {
                break ExecutionTerminalOutcome::Concurrent {
                    current: Some(terminal_candidate),
                };
            }
            if !remote_execution_is_live_for_terminal(
                &terminal_candidate,
                &lease_owner,
                chrono::Utc::now(),
            ) {
                break ExecutionTerminalOutcome::Concurrent {
                    current: Some(terminal_candidate),
                };
            }
            let updated_at = now_rfc3339();
            let attempt = ExecutionRepo::terminalize(
                &*self.db,
                TerminalizeExecution {
                    execution_id: notification.execution_id.clone(),
                    expected_version: terminal_candidate.execution_version,
                    lease_owner: Some(lease_owner.clone()),
                    status: status.clone(),
                    stop_reason: stop_reason.clone().map(Some),
                    stopped_by: stopped_by.clone().map(Some),
                    stopped_at: stopped_at.clone().map(Some),
                    resume_policy: terminal_resume_policy.clone(),
                    agent_session_id: notification.agent_session_id.clone().map(Some),
                    agent_message_id: None,
                    last_activity_at: None,
                    last_progress_at: None,
                    summary: notification.summary.clone().map(Some),
                    logs_path: None,
                    before_sha: None,
                    after_sha: notification.after_sha.clone().map(Some),
                    error: terminal_error.clone().map(Some),
                    executor_config_snapshot_json: snapshot_update.clone().map(Some),
                    updated_at: updated_at.clone(),
                    actor_type: "daemon".to_owned(),
                    actor_id: Some(lease_owner.clone()),
                    correlation_id: Some(format!("remote-execution:{}", notification.execution_id)),
                    causation_id: None,
                    causation_depth: 0,
                    lease_disposition: ExecutionLeaseDisposition::Revoke,
                },
            )
            .await?;

            match attempt {
                committed @ ExecutionTerminalOutcome::Committed { .. } => break committed,
                ExecutionTerminalOutcome::Concurrent {
                    current: Some(current),
                } if terminal_retries < REMOTE_TERMINAL_MAX_CAS_RETRIES
                    && self
                        .daemon_connections
                        .as_ref()
                        .is_none_or(|registry| registry.is_current(daemon_id, connection_id))
                    && current.status == ExecutionStatus::Running
                    && current.lease_owner.as_deref() == Some(lease_owner.as_str())
                    && remote_execution_is_live_for_terminal(
                        &current,
                        &lease_owner,
                        chrono::Utc::now(),
                    )
                    && terminal_candidate.execution_version < current.execution_version =>
                {
                    // The bounded retry handles self-heartbeat version churn;
                    // after two refreshes let the typed concurrent result
                    // propagate so a pathological race cannot spin forever.
                    terminal_retries += 1;
                    terminal_candidate = current;
                    continue;
                }
                outcome => break outcome,
            }
        };
        let committed_outcome = terminal_outcome.clone();
        let updated = match terminal_outcome {
            ExecutionTerminalOutcome::Committed { execution, .. } => execution,
            concurrent @ ExecutionTerminalOutcome::Concurrent { .. } => return Ok(concurrent),
        };

        if let Some(usage) = notification.usage {
            let provider = execution::usage_provider_from_snapshot(
                current_execution.executor_config_snapshot_json.as_deref(),
            );
            let model = usage.model.unwrap_or_else(|| "default".to_owned());
            if let Err(error) = ExecutionUsageRepo::upsert(
                &*self.db,
                UpsertExecutionUsage {
                    execution_id: updated.id.clone(),
                    provider,
                    model,
                    input_tokens: usage.input_tokens,
                    output_tokens: usage.output_tokens,
                    cache_read_tokens: usage.cache_read_tokens,
                    cache_write_tokens: usage.cache_write_tokens,
                    cost_usd: usage.cost_usd,
                },
            )
            .await
            {
                tracing::warn!(
                    execution_id = %updated.id,
                    %error,
                    "failed to record remote execution token usage"
                );
            }
        }

        execution::publish_terminal_execution_event(self, &updated);

        if let Err(error) = self
            .memory_service
            .record_execution_summary_if_present(&task.project_id, &updated)
            .await
        {
            tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
        }

        if updated.status == ExecutionStatus::Completed {
            if let Err(error) = execution::clear_execution_retry_metadata(&self.db, &task).await {
                tracing::warn!(
                    task_id = %task.id,
                    execution_id = %updated.id,
                    %error,
                    "failed to clear execution retry metadata"
                );
            }
            if updated.role == crate::workflow::default_roles::PLANNER
                && task.status == crate::workflow::default_states::PLANNING
            {
                if let Err(error) = execution::set_planning_awaiting_review_metadata(
                    &self.db,
                    &task,
                    Some(&updated.id),
                    true,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %task.id,
                        execution_id = %updated.id,
                        %error,
                        "failed to mark planning awaiting review"
                    );
                }
            }
        } else if updated.status == ExecutionStatus::Failed
            && executor_unavailable
            && execution::should_block_task_for_failed_execution(&updated)
        {
            let attempts = serde_json::Value::Array(
                route_outcome
                    .attempts
                    .iter()
                    .map(|(candidate_key, outcome)| {
                        serde_json::json!({"candidate_key": candidate_key, "outcome": outcome})
                    })
                    .collect(),
            );
            if let Err(error) = self
                .annotate_executor_unavailable_block(
                    &updated,
                    notification.retry_at.clone(),
                    attempts,
                )
                .await
            {
                tracing::warn!(
                    execution_id = %updated.id,
                    task_id = %updated.task_id,
                    %error,
                    "failed to handle executor-unavailable daemon execution"
                );
            }
        } else if updated.status == ExecutionStatus::Failed
            && execution::should_block_task_for_failed_execution(&updated)
        {
            if let Err(error) = self.annotate_executor_failure_block(&updated).await {
                tracing::warn!(
                    execution_id = %updated.id,
                    task_id = %updated.task_id,
                    %error,
                    "failed to block task after daemon execution failure"
                );
            }
        }

        Ok(committed_outcome)
    }
}

fn remote_terminal_error_message(
    exit_code: Option<i32>,
    signal: Option<&str>,
    error: Option<&str>,
) -> String {
    let mut parts = Vec::new();
    if let Some(error) = error {
        parts.push(bounded_redacted_remote_diagnostic(error));
    }
    if let Some(exit_code) = exit_code {
        parts.push(format!("exit code {exit_code}"));
    }
    if let Some(signal) = signal {
        parts.push(format!(
            "signal {}",
            bounded_redacted_remote_diagnostic(signal)
        ));
    }
    if parts.is_empty() {
        "remote execution failed".to_owned()
    } else {
        bounded_redacted_remote_diagnostic(&parts.join("; "))
    }
}

#[cfg(test)]
mod tests;
