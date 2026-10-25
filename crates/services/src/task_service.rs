use crate::{
    agent_service::{compute_effective_status, EffectiveStatus},
    lifecycle::{LifecycleHookContext, LifecycleHookRun, LifecycleHookRunner},
    memory::MemoryService,
    merge_service::MergeService,
    terminal_service::TerminalActivityTracker,
    workflow::{
        default_states,
        engine::{WorkflowAuthority, WorkflowEngine},
    },
    workspace_backend::{EmbeddedWorkspaceBackend, WorkspaceBackendRouter},
    workspace_cleanup::WorkspaceCleanupScheduler,
    workspace_execution_lock::WorkspaceExecutionLockManager,
    Assignee, Result, ServiceError,
};
use ::workspace::{RepoCacheLockManager, WorkspaceManager};
use api_types::{Actor, ProjectSettings, UserActionSource};
use chrono::{DateTime, Utc};
use cli_adapters::codex::protocol::RESUME_THREAD_ID_CONFIG_KEY;
use db::{
    new_uuid_v4, now_rfc3339, Agent, AgentRepo, ArchiveTask, AssigneeKind, ClaimExecutionLease,
    ClaimTask, ClaimedTask, CommentAuthorType, CreateDomainEvent, CreateExecution, CreateTask,
    CreateTaskComment, CreateTaskRoleAssignment, CreateWorkspace, CreateWorkspaceLease, DbError,
    DomainEventRepo, Execution, ExecutionAdmission, ExecutionLeaseDisposition, ExecutionRepo,
    ExecutionStatus, ExecutionTerminalOutcome, ProjectRepo, RepoRepo, Review, ReviewRepo,
    ReviewStatus, SoftDeleteTask, SqliteDb, Task, TaskBoardRepo, TaskComment, TaskCommentRepo,
    TaskDependencyRepo, TaskMetadata, TaskRepo, TaskRoleAssignment, TaskRoleAssignmentRepo,
    TaskStatus, TerminalizeExecution, TransitionLogRepo, Workspace, WorkspaceLeaseRepo,
    WorkspaceRepo, WorkspaceStatus,
};
use events::{event_timestamp, EventBus, EventContext, ForgeEvent};
use executors::{
    merge_overrides, resolve_config_value, ExecutionContext, ExecutionOutcome, ExecutionOverrides,
    ExecutorKind, TaskExecutor,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
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
pub(crate) mod commands;
mod common;
pub(crate) mod config;
mod create;
pub(crate) use create::PreparedProjectHookTask;
mod create_subtasks;
mod dependencies;
pub mod execution;
mod governance;
mod lifecycle_test;
pub(crate) mod logs;
mod move_task;
mod placement_refusal;
pub(crate) mod project_agent_workspace;
mod proposal;
mod remote_terminal;
mod reorder_subtasks;
mod repository_authority;
mod review;
mod review_candidate;
pub(crate) use review::retired_review_rerun;
pub(crate) use review_candidate::MISSING_REVIEW_CANDIDATE;
#[cfg(test)]
pub(crate) use update::retired_auditor_notice_key;
pub(crate) use update::state_retired_auditor_notices;
pub(crate) use update::RETIRED_AUDITOR_CONFIG;
mod roles;
pub(crate) use roles::RoleSweepEvent;
pub(crate) mod lead;
pub(crate) mod replan;
mod subtask;
mod transition;
pub(crate) use transition::next_workflow_state;
mod update;
mod validation;
pub(crate) mod workspace;

pub use actions::TaskActionResult;
pub use adaptive::{
    AdaptiveTaskChild, AdaptiveTaskCommand, AdaptiveTaskCommandResult, AdaptiveTaskOperation,
};
pub(crate) use common::latest_executor_execution_for_task;
pub use create_subtasks::NewSubtaskInput;
pub use dependencies::TaskDependencyAction;
pub use lead::LeadRun;
pub use project_agent_workspace::ensure_project_agent_workspace;
pub use proposal::{
    DirectTaskProposalInput, TaskProposalCommandResult, TaskProposalPayload, TASK_PROPOSE_COMMAND,
};
pub(crate) use repository_authority::resolve_task_repository_authority;
pub(crate) use subtask::coordination_review_pending;
pub use workspace::{
    ensure_inquiry_workspace, ensure_main_agent_workspace, MAIN_AGENT_INQUIRIES_DIR,
};

/// Decode persisted review details before any endpoint or workflow mutates the
/// row. Legacy CI-only arrays are still readable, but every other malformed
/// syntax or shape is persisted server corruption and must fail closed.
pub(crate) fn strict_review_details(review: &Review) -> Result<Value> {
    let corrupt = |reason: String| {
        ServiceError::Db(DbError::ReviewDetailsCorrupt {
            review_id: review.id.clone(),
            reason,
        })
    };
    let value = serde_json::from_str::<Value>(&review.step_results_json)
        .map_err(|error| corrupt(error.to_string()))?;
    if let Value::Array(ci_steps) = value {
        serde_json::from_value::<Vec<api_types::StepResultEntry>>(Value::Array(ci_steps.clone()))
            .map_err(|error| corrupt(error.to_string()))?;
        return Ok(json!({ "ci_steps": ci_steps }));
    }
    if !value.is_object() {
        return Err(corrupt(format!(
            "expected review details object or legacy step array, got {value}"
        )));
    }
    let object = value
        .as_object()
        .expect("value was checked to be an object");
    for key in object.keys() {
        if ![
            "ci_steps",
            "verify",
            "conformance",
            "auditor",
            "user_approval",
            "execution",
            "execution_retry",
            "manual_override",
            "carry_hook_step_id",
            "carry_hook_index",
        ]
        .contains(&key.as_str())
        {
            return Err(corrupt(format!(
                "unknown persisted review detail field: {key}"
            )));
        }
    }
    if let Some(ci_steps) = object.get("ci_steps") {
        if !ci_steps.is_array() {
            return Err(corrupt("persisted review ci_steps must be an array".into()));
        }
    }
    for key in [
        "verify",
        "user_approval",
        "execution",
        "execution_retry",
        "manual_override",
    ] {
        if let Some(value) = object.get(key) {
            if !value.is_object() {
                return Err(corrupt(format!("persisted review {key} must be an object")));
            }
        }
    }
    for (key, valid) in [
        (
            "carry_hook_step_id",
            object
                .get("carry_hook_step_id")
                .is_none_or(Value::is_string),
        ),
        (
            "carry_hook_index",
            object
                .get("carry_hook_index")
                .is_none_or(|value| value.as_u64().is_some()),
        ),
    ] {
        if !valid {
            return Err(corrupt(format!("invalid persisted review {key}")));
        }
    }
    serde_json::from_value::<api_types::ReviewDetails>(value.clone())
        .map_err(|error| corrupt(error.to_string()))?;
    Ok(value)
}

#[cfg(test)]
use self::config::{
    execution_overrides_to_config_layer, merge_config_layers, override_value_or_empty,
    parse_config_override_layer, OverridesApplied,
};

/// Build the immutable facts that a workflow dispatch used to choose an
/// execution. The DB admission transaction re-reads these facts under its
/// writer lock; this is deliberately based on the same governing-workflow
/// rule as `WorkflowEngine::resolve_workflow_for_task`, including inherited
/// subtask states.
pub(crate) async fn execution_admission_for_task(
    db: &SqliteDb,
    task: &Task,
    project_workflow_definition: &str,
    role: &str,
    agent: Option<&Agent>,
    expected_project_version: i64,
) -> Result<ExecutionAdmission> {
    let inherited_subtask_workflow = task.parent_task_id.is_some()
        && WorkflowEngine::resolve_subtask_workflow()
            .states
            .iter()
            .any(|state| state.name == task.status);
    let expected_assignment = if role == crate::workflow::default_roles::INTERACTIVE {
        None
    } else {
        let assignment_role = if role == "executor" {
            crate::workflow::default_roles::CODER
        } else {
            role
        };
        Some(
            crate::task_hierarchy::effective_role_assignment(db, task, assignment_role)
                .await?
                .map(|resolved| resolved.assignment)
                .ok_or_else(|| {
                    ServiceError::conflict(format!(
                        "task {} has no assignment for workflow role {}",
                        task.id, assignment_role
                    ))
                })?,
        )
    };
    Ok(ExecutionAdmission {
        purpose: None,
        expected_queued_recovery_id: None,
        expected_project_version: Some(expected_project_version),
        expected_task_version: task.version,
        expected_task_status: task.status.clone(),
        expected_effective_role: (role != crate::workflow::default_roles::INTERACTIVE)
            .then(|| role.to_owned()),
        expected_agent_version: agent.map(|agent| agent.version),
        expected_agent_max_concurrent_tasks: agent.map(|agent| agent.max_concurrent_tasks),
        expected_reviewer_parent_execution_id: None,
        expected_latest_review_candidate_execution_id: None,
        expected_reviewer_id: None,
        expected_reviewer_attempt_number: None,
        expected_reviewer_status: None,
        expected_reviewer_updated_at: None,
        expected_reviewer_execution_id: None,
        expected_auditor_execution_id: None,
        expected_assignment_id: expected_assignment
            .as_ref()
            .map(|assignment| assignment.id.clone()),
        expected_assignment_updated_at: expected_assignment
            .as_ref()
            .map(|assignment| assignment.updated_at.clone()),
        expected_workflow_definition: (role != crate::workflow::default_roles::INTERACTIVE
            && !inherited_subtask_workflow)
            .then(|| project_workflow_definition.to_owned()),
    })
}

use self::{
    config::{
        build_executor_config_snapshot, create_failed_execution_record,
        executor_snapshot_with_next_fallback, executor_snapshot_with_resume_thread,
        parse_json_value, truncate_utf8_bytes,
    },
    logs::execution_logs_path,
    validation::{serialize_config, validate_required},
    workspace::{prepare_workspace, reset_workspace},
};

pub(super) const DISPATCH_STATUS_POLL_INTERVAL: Duration = Duration::from_secs(10);
pub(super) const DISPATCH_STATUS_WAIT_CEILING: Duration = Duration::from_secs(10 * 60);
pub(super) const MAX_FOLLOW_UP_DIFF_BYTES: usize = 64 * 1024;

/// Remote terminal notifications are transport input, not trusted event
/// metadata. Keep every free-form value copied into a late-result diagnostic
/// small and single-line, and replace values that look credential-bearing.
const REMOTE_TERMINAL_DIAGNOSTIC_MAX_CHARS: usize = 256;
const REMOTE_TERMINAL_DIAGNOSTIC_REDACTED: &str = "[redacted]";

/// Project revision that admitted a workflow-role execution.
///
/// Running executions created by Forge have carried this immutable snapshot
/// since the scheduler began fencing dispatch against Project edits.  A
/// missing, malformed, or non-integer value cannot safely authorize terminal
/// workflow effects: interpreting an old completion under the current
/// workflow could spend retry budget, block the Task, or advance the wrong
/// state.
pub(crate) fn execution_dispatch_project_version(execution: &Execution) -> Option<i64> {
    execution
        .executor_config_snapshot_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .and_then(|snapshot| snapshot.get("project_version").and_then(Value::as_i64))
}

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

/// The owner is generated from the authenticated daemon connection, never
/// copied from notification data. It is therefore an opaque, stable server
/// reference and must remain usable for late-result attribution.
fn stable_remote_owner_ref(owner: &str) -> String {
    owner.to_owned()
}

fn remote_execution_lease_is_active(
    lease_owner: Option<&str>,
    lease_expires_at: Option<&str>,
    hard_deadline_at: Option<&str>,
    expected_owner: &str,
    now: DateTime<Utc>,
) -> bool {
    if lease_owner != Some(expected_owner) {
        return false;
    }
    let lease_is_live = lease_expires_at
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .is_some_and(|expires_at| expires_at.with_timezone(&Utc) > now);
    let hard_deadline_is_live = hard_deadline_at.is_none_or(|value| {
        DateTime::parse_from_rfc3339(value).is_ok_and(|deadline| deadline.with_timezone(&Utc) > now)
    });
    lease_is_live && hard_deadline_is_live
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
                | "review_needs_owner"
                | "merge_fix_budget_exhausted"
                | "merge_fix_ci_failed"
        )
    )
}

/// The check runtime of one Task service, as [`TaskService::compose_checks`]
/// builds it.
pub struct ComposedChecks {
    pub runner: Arc<crate::check_runner::CheckRunner>,
    pub consumers: Arc<crate::check_runner::consumer::TaskCheckConsumers>,
    pub worker: Arc<crate::check_runner::worker::CheckRunWorker>,
    /// The workspace owners the worker dispatches to (one instance: its
    /// live-operation counter is what the garbage-collection sweep reads).
    pub owners: Arc<crate::check_runner::owners::WorkspaceCheckOwners>,
}

#[derive(Clone)]
pub struct TaskService {
    workflow_engine: Arc<WorkflowEngine>,
    task_step_driver:
        Arc<std::sync::Mutex<Option<Arc<crate::worker_runtime::queue::TaskStepWorker>>>>,
    pub(crate) task_step_replies: Arc<
        std::sync::Mutex<HashMap<String, tokio::sync::oneshot::Sender<Result<serde_json::Value>>>>,
    >,
    /// Bound once by the runtime; the `apply_check_result` step applies
    /// check results through it.
    pub(crate) check_consumers:
        Arc<std::sync::OnceLock<Arc<crate::check_runner::consumer::TaskCheckConsumers>>>,
    /// Bound once by the runtime: the worker that executes requested
    /// checks. A harness that starts the Task-step worker on its own starts
    /// this one with it, or no check a step asks for is ever answered.
    pub(crate) check_worker:
        Arc<std::sync::OnceLock<Arc<crate::check_runner::worker::CheckRunWorker>>>,
    pub(crate) check_runner: Arc<std::sync::OnceLock<Arc<crate::check_runner::CheckRunner>>>,
    pub(crate) check_owners:
        Arc<std::sync::OnceLock<Arc<crate::check_runner::owners::WorkspaceCheckOwners>>>,
    /// Bound once: the integration queue worker. Admission (`run_merge`)
    /// wakes it, the Task-step side tells it an answer was written, and
    /// `drain` pumps it. The runtime starts it; a service built without a
    /// runtime composes one over the same production ports.
    pub(crate) integration_worker:
        Arc<std::sync::OnceLock<Arc<crate::integration_worker::IntegrationQueueWorker>>>,
    /// Set while the start-time import of merges that were in flight before
    /// the queue has not completed (it failed and is being retried). The
    /// reconciler admits nothing meanwhile: a Task the importer has not
    /// classified may have a merge whose result is unknown.
    pub(crate) integration_import_pending: Arc<std::sync::atomic::AtomicBool>,
    pub(crate) db: Arc<SqliteDb>,
    pub(crate) event_bus: Arc<EventBus>,
    pub(crate) merge_service: Option<Arc<MergeService>>,
    pub(crate) cleanup_scheduler: Option<Arc<WorkspaceCleanupScheduler>>,
    task_executor: Option<Arc<dyn TaskExecutor>>,
    placement_adapter_registry: Option<Arc<executors::AdapterRegistry>>,
    pub(crate) daemon_connections: Option<Arc<crate::daemon_transport::DaemonConnectionRegistry>>,
    pub(crate) workspace_exec_locks: Option<Arc<WorkspaceExecutionLockManager>>,
    pub(crate) terminal_activity: Option<Arc<TerminalActivityTracker>>,
    pub(crate) repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
    pub(crate) workspace_backend_router: Arc<WorkspaceBackendRouter>,
    test_workspace_backend: bool,
    pub(crate) workspace_root: PathBuf,
    workspace_max_disconnect: Duration,
    memory_service: Arc<MemoryService>,
    move_operation_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    /// Shared with manual checks: a Project has at most one host re-check.
    environment_rechecks: Arc<std::sync::Mutex<HashSet<String>>>,
    dispatch_notify: Arc<tokio::sync::Notify>,
    credential_env: Option<Arc<dyn execution::ExecutionSupport>>,
    pub(crate) dispatch_wake: Arc<tokio::sync::Notify>,
    /// The runtime's dispatcher instance, for commands it queued.
    dispatcher: Arc<std::sync::Mutex<std::sync::Weak<crate::task_dispatcher::DispatcherInstance>>>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct TransitionResult {
    pub task: Task,
    pub review: Option<Review>,
    pub pending_steps: i64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct TransitionOptions {
    #[serde(flatten)]
    pub bridge: api_types::TransitionBridge,
    pub version: i64,
    pub reason: Option<String>,
    pub triggered_by: Actor,
    pub rejection: bool,
    pub defer_dispatch_seconds: Option<i64>,
}

impl From<i64> for TransitionOptions {
    fn from(version: i64) -> Self {
        Self {
            bridge: Default::default(),
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
            bridge: Default::default(),
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
            bridge: Default::default(),
            version,
            reason,
            triggered_by: Actor::user(UserActionSource::Api),
            rejection,
            defer_dispatch_seconds: None,
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct LaunchExecutionResult {
    pub task: Task,
    pub execution: Execution,
    pub workspace: Workspace,
}

impl TaskService {
    /// The durable check runner's Task-step consumer contract, once the
    /// runtime has composed it. Consumer families register through it.
    pub fn check_worker(&self) -> Option<Arc<crate::check_runner::worker::CheckRunWorker>> {
        self.check_worker.get().cloned()
    }
    /// The one place the check runtime of a Task service is composed: the
    /// runner, the consumer contract with EVERY consumer family (review
    /// entry and integration), and the worker with its workspace owners.
    /// The production runtime and every test build it here, so no caller
    /// can end up with a subset of the families. Idempotent: the first
    /// composition wins and later calls return it.
    pub fn compose_checks(
        &self,
        daemon_connections: Arc<crate::daemon_transport::DaemonConnectionRegistry>,
        disconnect_bound: std::time::Duration,
        integration: &crate::integration_worker::IntegrationWorkerConfig,
    ) -> ComposedChecks {
        let runner = self
            .check_runner
            .get_or_init(|| Arc::new(crate::check_runner::CheckRunner::new(self.db.clone())))
            .clone();
        let consumers = self
            .check_consumers
            .get_or_init(|| {
                let consumers = Arc::new(crate::check_runner::consumer::TaskCheckConsumers::new(
                    self.db.clone(),
                    runner.clone(),
                ));
                crate::integration_steps::IntegrationCheckFamily::register(
                    &consumers,
                    // The merge-path family answers the queue worker: both
                    // sides of that handshake use the worker's timers.
                    Arc::new(
                        crate::integration_steps::IntegrationSteps::new(self.clone()).with_timers(
                            crate::integration_steps::IntegrationStepTimers::from(integration),
                        ),
                    ),
                );
                consumers.register(
                    db::CheckConsumerOrigin::Entry,
                    Arc::new(crate::check_runner::review_entry::ReviewEntryChecks::new(
                        self.db.clone(),
                    )),
                );
                consumers
            })
            .clone();
        let owners = self
            .check_owners
            .get_or_init(|| {
                Arc::new(crate::check_runner::owners::WorkspaceCheckOwners::new(
                    self.db.clone(),
                    daemon_connections,
                    disconnect_bound,
                ))
            })
            .clone();
        let worker = self
            .check_worker
            .get_or_init(|| {
                Arc::new(crate::check_runner::worker::CheckRunWorker::new(
                    self.db.clone(),
                    owners.clone(),
                ))
            })
            .clone();
        ComposedChecks {
            runner,
            consumers,
            worker,
            owners,
        }
    }
    /// The check worker `drain` drives. The runtime composes it before any
    /// step runs; a service built without a runtime (tests) composes the
    /// same runtime here, through [`Self::compose_checks`], with the daemon
    /// registry the service was given (or one with no daemon connected).
    pub fn check_worker_or_embedded(&self) -> Arc<crate::check_runner::worker::CheckRunWorker> {
        if let Some(worker) = self.check_worker() {
            return worker;
        }
        self.compose_checks(
            self.embedded_daemon_connections(),
            std::time::Duration::from_secs(60),
            &Self::embedded_integration_config(),
        )
        .worker
    }
    fn embedded_daemon_connections(
        &self,
    ) -> Arc<crate::daemon_transport::DaemonConnectionRegistry> {
        self.daemon_connections.clone().unwrap_or_else(|| {
            Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers())
        })
    }
    /// The timers of a queue worker nothing supervises (a service built
    /// without a runtime, which `drain` pumps): short waits, because nothing
    /// else wakes it.
    fn embedded_integration_config() -> crate::integration_worker::IntegrationWorkerConfig {
        crate::integration_worker::IntegrationWorkerConfig {
            poll: std::time::Duration::from_millis(20),
            sweep_interval: std::time::Duration::from_millis(100),
            conflict_backoff: std::time::Duration::from_millis(5),
            // A head a drain left mid-way is taken over soon after.
            lease: std::time::Duration::from_secs(3),
            renew_every: std::time::Duration::from_millis(750),
            ..Default::default()
        }
    }
    /// The one place the integration queue worker of a Task service is
    /// composed, over its production ports (`integration_ports`). Not
    /// started: the runtime starts it after owner reconciliation and the
    /// importer. Idempotent: the first composition wins. The same `config`
    /// goes to [`Self::compose_checks`].
    pub fn compose_integration(
        &self,
        daemon_connections: Arc<crate::daemon_transport::DaemonConnectionRegistry>,
        config: crate::integration_worker::IntegrationWorkerConfig,
    ) -> Arc<crate::integration_worker::IntegrationQueueWorker> {
        self.integration_worker
            .get_or_init(|| {
                let locations = Arc::new(crate::repo_location::RepoLocationService::new(
                    self.db.clone(),
                    Arc::new(crate::repo_location::RemoteDaemonLocationVerifier::new(
                        daemon_connections.clone(),
                        self.workspace_root.clone(),
                    )),
                ));
                crate::integration_ports::build_integration_worker(
                    self.db.clone(),
                    self.workspace_backend_router.clone(),
                    daemon_connections,
                    locations,
                    config,
                )
            })
            .clone()
    }
    /// See the field. The runtime sets it when the import fails at start and
    /// clears it when a retry completes.
    pub fn integration_import_pending(&self) -> Arc<std::sync::atomic::AtomicBool> {
        Arc::clone(&self.integration_import_pending)
    }
    pub fn integration_worker(
        &self,
    ) -> Option<Arc<crate::integration_worker::IntegrationQueueWorker>> {
        self.integration_worker.get().cloned()
    }
    /// The queue worker `drain` pumps. The runtime composes it before any
    /// step runs; a service built without a runtime composes the same worker
    /// here, with the check runtime on the same timers.
    #[cfg(any(test, feature = "test-support"))]
    pub fn integration_worker_or_embedded(
        &self,
    ) -> Arc<crate::integration_worker::IntegrationQueueWorker> {
        if let Some(worker) = self.integration_worker() {
            return worker;
        }
        let config = Self::embedded_integration_config();
        let daemons = self.embedded_daemon_connections();
        self.compose_checks(daemons.clone(), std::time::Duration::from_secs(60), &config);
        self.compose_integration(daemons, config)
    }
    pub fn check_consumers(
        &self,
    ) -> Option<Arc<crate::check_runner::consumer::TaskCheckConsumers>> {
        self.check_consumers.get().cloned()
    }

    /// Production services receive the runtime's shared owner router and
    /// the workspace root the start settled.
    pub fn new_with_router(
        db: Arc<SqliteDb>,
        event_bus: Arc<EventBus>,
        workspace_backend_router: Arc<WorkspaceBackendRouter>,
        workspace_root: PathBuf,
    ) -> Self {
        db.server_run_cap
            .initialize_identity(&::config::embedded_machine_id());
        let memory_service = Arc::new(MemoryService::new(Arc::clone(&db)));
        let workflow_engine =
            Arc::new(WorkflowEngine::new(Arc::clone(&db), Arc::clone(&event_bus)));
        Self {
            workflow_engine,
            db,
            task_step_driver: Arc::default(),
            task_step_replies: Arc::default(),
            check_consumers: Arc::default(),
            check_runner: Arc::default(),
            check_owners: Arc::default(),
            check_worker: Arc::default(),
            integration_worker: Arc::default(),
            integration_import_pending: Arc::default(),
            event_bus,
            merge_service: None,
            cleanup_scheduler: None,
            task_executor: None,
            placement_adapter_registry: None,
            daemon_connections: None,
            workspace_exec_locks: None,
            terminal_activity: None,
            repo_cache_locks: None,
            workspace_backend_router,
            test_workspace_backend: false,
            workspace_root,
            workspace_max_disconnect: Duration::from_secs(::config::DEFAULT_MAX_DISCONNECT_SECONDS),
            memory_service,
            move_operation_locks: Arc::new(Mutex::new(HashMap::new())),
            environment_rechecks: Arc::default(),
            dispatch_notify: Arc::default(),
            credential_env: None,
            dispatch_wake: Arc::default(),
            dispatcher: Arc::default(),
        }
    }

    /// Embedded-only fixture constructor for tests outside this crate.
    #[cfg(any(test, feature = "test-support"))]
    pub fn new_for_test(db: Arc<SqliteDb>, event_bus: Arc<EventBus>) -> Self {
        let workspace_root = crate::workspace_root::fixture_root();
        let router = crate::lifecycle::context::embedded_workspace_router_for_test(
            Arc::clone(&db),
            workspace_root.clone(),
            None,
        );
        let mut service = Self::new_with_router(db, event_bus, router, workspace_root);
        service.placement_adapter_registry =
            Some(Arc::new(cli_adapters::test_support::test_registry()));
        service.test_workspace_backend = true;
        service
    }

    #[cfg(test)]
    pub fn new(db: Arc<SqliteDb>, event_bus: Arc<EventBus>) -> Self {
        Self::new_for_test(db, event_bus)
    }

    pub fn with_merge_service(mut self, merge_service: Arc<MergeService>) -> Self {
        self.merge_service = Some(merge_service);
        self.configure_workspace_backend();
        self.task_step_driver = Arc::default();
        self
    }

    /// Inject the graph's shared router after configuring workspace dependencies.
    pub fn with_workspace_backend_router(mut self, router: Arc<WorkspaceBackendRouter>) -> Self {
        self.workspace_backend_router = router;
        self.test_workspace_backend = false;
        self.task_step_driver = Arc::default();
        self
    }

    pub fn workspace_root(&self) -> &std::path::Path {
        &self.workspace_root
    }

    pub fn workspace_backend_router(&self) -> Arc<WorkspaceBackendRouter> {
        Arc::clone(&self.workspace_backend_router)
    }

    pub fn repo_cache_locks(&self) -> Option<Arc<RepoCacheLockManager>> {
        self.repo_cache_locks.clone()
    }

    fn configure_workspace_backend(&mut self) {
        if !self.test_workspace_backend {
            return;
        }
        let merge_service = self.merge_service.clone().unwrap_or_else(|| {
            Arc::new(MergeService::new(
                Arc::clone(&self.db),
                self.workspace_root.clone(),
            ))
        });
        let mut embedded = EmbeddedWorkspaceBackend::new(
            Arc::clone(&self.db),
            merge_service,
            self.workspace_root.clone(),
        );
        if let Some(locks) = &self.repo_cache_locks {
            embedded = embedded.with_repo_cache_locks(Arc::clone(locks));
        }
        self.workspace_backend_router = Arc::new(WorkspaceBackendRouter::new(Arc::new(embedded)));
    }

    pub(crate) fn dispatch_notify(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.dispatch_notify)
    }

    pub(crate) fn register_dispatcher(
        &self,
        dispatcher: std::sync::Weak<crate::task_dispatcher::DispatcherInstance>,
    ) {
        *self.dispatcher.lock().expect("dispatcher registration") = dispatcher;
    }

    pub(crate) fn registered_dispatcher(
        &self,
    ) -> Option<Arc<crate::task_dispatcher::DispatcherInstance>> {
        self.dispatcher
            .lock()
            .expect("dispatcher registration")
            .upgrade()
    }

    pub fn with_task_executor(mut self, task_executor: Arc<dyn TaskExecutor>) -> Self {
        self.task_executor = Some(task_executor);
        self.task_step_driver = Arc::default();
        self
    }

    /// Supply the adapters used to determine embedded placement availability.
    pub fn with_placement_adapter_registry(
        mut self,
        registry: Arc<executors::AdapterRegistry>,
    ) -> Self {
        self.placement_adapter_registry = Some(registry);
        self.task_step_driver = Arc::default();
        self
    }

    pub fn with_daemon_connections(
        mut self,
        daemon_connections: Arc<crate::daemon_transport::DaemonConnectionRegistry>,
    ) -> Self {
        self.daemon_connections = Some(daemon_connections);
        self.task_step_driver = Arc::default();
        self
    }

    pub fn with_workspace_exec_locks(mut self, locks: Arc<WorkspaceExecutionLockManager>) -> Self {
        self.workspace_exec_locks = Some(locks);
        self.task_step_driver = Arc::default();
        self
    }

    pub fn with_terminal_activity_tracker(
        mut self,
        terminal_activity: Arc<TerminalActivityTracker>,
    ) -> Self {
        self.terminal_activity = Some(terminal_activity);
        self.task_step_driver = Arc::default();
        self
    }

    pub fn with_repo_cache_locks(mut self, locks: Arc<RepoCacheLockManager>) -> Self {
        if let Some(scheduler) = self.cleanup_scheduler.as_ref() {
            scheduler.set_repo_cache_locks(Arc::clone(&locks));
        }
        self.repo_cache_locks = Some(locks);
        self.configure_workspace_backend();
        self.task_step_driver = Arc::default();
        self
    }

    pub fn with_cleanup_scheduler(
        mut self,
        cleanup_scheduler: Arc<WorkspaceCleanupScheduler>,
    ) -> Self {
        if let Some(locks) = self.repo_cache_locks.as_ref() {
            cleanup_scheduler.set_repo_cache_locks(Arc::clone(locks));
        }
        self.cleanup_scheduler = Some(cleanup_scheduler);
        self.task_step_driver = Arc::default();
        self
    }

    pub(crate) fn owner_wait_timeout(&self) -> Duration {
        self.workspace_max_disconnect
    }

    pub fn with_workspace_max_disconnect(mut self, timeout: Duration) -> Self {
        self.workspace_max_disconnect = timeout;
        self.task_step_driver = Arc::default();
        self
    }

    pub fn with_workspace_root(mut self, workspace_root: PathBuf) -> Self {
        self.workspace_root = workspace_root;
        self.configure_workspace_backend();
        self.task_step_driver = Arc::default();
        self
    }

    pub fn with_memory_service(mut self, memory_service: Arc<MemoryService>) -> Self {
        self.memory_service = memory_service;
        self.task_step_driver = Arc::default();
        self
    }

    /// Enables `auth_source: forge_provider` dispatch: harness executions for
    /// agents referencing a provider entry get the entry's API key injected
    /// into their in-memory executor environment.
    pub fn with_provider_credential_env(
        mut self,
        embedded: Arc<dyn execution::ExecutionSupport>,
    ) -> Self {
        self.credential_env = Some(embedded);
        self.task_step_driver = Arc::default();
        self
    }

    /// Whether runtime composition supplied the agents-owned execution seam.
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn has_provider_credential_env(&self) -> bool {
        self.credential_env.is_some()
    }

    fn publish(&self, event: ForgeEvent) {
        if matches!(
            event.event_type.as_str(),
            "execution.completed" | "execution.failed" | "execution.cancelled"
        ) {
            self.dispatch_wake.notify_one();
        }
        self.event_bus.publish(event);
    }

    /// Phase one of a forced Project deletion. It changes nothing. A check
    /// run that was dispatched to a daemon which is not connected (and not
    /// removed) cannot be stopped or confirmed now, and its row is what
    /// fences the process on that machine, so the deletion will be refused
    /// whatever else is cancelled. The machines of those runs are returned:
    /// the caller refuses before it cancels anything. A queued run, a run on
    /// the server, a run on a connected daemon, a run whose owner already
    /// reported and a run on a removed machine can all be settled.
    pub async fn project_deletion_unreachable_machines(
        &self,
        project_id: &str,
    ) -> Result<Vec<String>> {
        validate_required("project_id", project_id)?;
        let mut machines = std::collections::BTreeSet::new();
        for (id, _, state) in self.db.live_project_check_runs(project_id).await? {
            if state == "queued" {
                continue;
            }
            let record = match db::CheckWorkerRepo::check_worker_record(&*self.db, &id).await {
                Ok(record) => record,
                Err(DbError::NotFound) => continue,
                Err(error) => return Err(error.into()),
            };
            if record.run.state.terminal() || record.receipt.is_some() {
                continue;
            }
            let Some(db::CheckDispatchIntent {
                target: db::CheckDispatchTarget::Daemon { workspace },
                ..
            }) = &record.dispatch
            else {
                continue;
            };
            let daemon: Option<(Option<String>, String)> = self
                .db
                .task_delete_check_machine(&workspace.daemon_id)
                .await?;
            let Some((None, hostname)) = daemon else {
                // Removed or unknown: the check worker settles the run.
                continue;
            };
            if !self
                .daemon_connections
                .as_ref()
                .is_some_and(|registry| registry.is_connected(&workspace.daemon_id))
            {
                machines.insert(hostname);
            }
        }
        // The same for running executions. Phase two stops them one by one
        // and asks each provider first: an execution on a machine that
        // cannot be reached would refuse the delete after earlier ones had
        // already been cancelled.
        for execution in ExecutionRepo::list_running_for_project(&*self.db, project_id).await? {
            let daemon_id = match self.execution_provider_for_execution(&execution).await {
                Err(ServiceError::DaemonUnavailable { daemon_id }) => daemon_id,
                // Reachable, or a failure phase two reports as it does today.
                _ => continue,
            };
            let hostname: Option<String> = self
                .db
                .task_delete_execution_machine_name(&daemon_id)
                .await?;
            machines.insert(hostname.unwrap_or(daemon_id));
        }
        Ok(machines.into_iter().collect())
    }

    /// A forced deletion that is refused after its phase two leaves nothing
    /// failed behind: the review entries whose checks it cancelled are woken,
    /// and each asks again for its check (`run_ci_steps`, the mechanism of a
    /// changed configuration). Returns how many entries were woken.
    pub async fn resume_project_check_waits(&self, project_id: &str) -> Result<usize> {
        validate_required("project_id", project_id)?;
        let mut woken = 0;
        for (task_id, consumer_id) in self.db.cancelled_project_check_waits(project_id).await? {
            if self.db.wake_suspended_hooks(&task_id, &consumer_id).await? != 0 {
                woken += 1;
            }
        }
        Ok(woken)
    }

    /// Phase two of a forced Project deletion: make it safe to commit. Every
    /// running execution is terminalized through the owner/version CAS and
    /// its provider is asked to stop; any provider failure is returned.
    /// Leases that are not attached to one of those executions are revoked
    /// explicitly as well. Unfinished check runs are stopped and given until
    /// `settle_until` to settle; the deadline is the caller's, so a request
    /// that calls this several times waits once. The caller asks
    /// [`Self::project_deletion_unreachable_machines`] first and still
    /// performs a final DB in-use check immediately before deletion to catch
    /// work admitted concurrently with this pass.
    pub async fn prepare_project_deletion(
        &self,
        project_id: &str,
        settle_until: tokio::time::Instant,
    ) -> Result<()> {
        validate_required("project_id", project_id)?;

        let running = ExecutionRepo::list_running_for_project(&*self.db, project_id).await?;
        for execution in running {
            let task = TaskRepo::get_by_id(&*self.db, &execution.task_id, true)
                .await?
                .ok_or_else(|| ServiceError::not_found("task", execution.task_id.clone()))?;
            // A concurrent Task move can change the join result after the
            // scoped read. Never cancel an execution that no longer belongs
            // to this Project; the final DB guard will re-check its current
            // owner before deletion.
            if task.project_id != project_id {
                continue;
            }
            self.cancel_active_execution_for_project_deletion(&execution)
                .await?;
        }

        for mut lease in WorkspaceLeaseRepo::list_active_for_project(&*self.db, project_id).await? {
            // Revocation is versioned because a renewal/revocation may win
            // between the scoped snapshot and this write.  A lease that is
            // already terminal is a successful force-delete outcome; an
            // active lease that keeps changing must remain a blocker.
            let mut revoked = false;
            for _ in 0..3 {
                match WorkspaceLeaseRepo::revoke(
                    &*self.db,
                    &lease.id,
                    lease.version,
                    &now_rfc3339(),
                )
                .await
                {
                    Ok(_) => {
                        revoked = true;
                        break;
                    }
                    Err(DbError::VersionConflict) => {
                        let Some(current) =
                            WorkspaceLeaseRepo::get_by_id(&*self.db, &lease.id).await?
                        else {
                            break;
                        };
                        if current.status != "active" {
                            break;
                        }
                        lease = current;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            if !revoked {
                if let Some(current) = WorkspaceLeaseRepo::get_by_id(&*self.db, &lease.id).await? {
                    if current.status == "active" {
                        return Err(ServiceError::Db(DbError::VersionConflict));
                    }
                }
            }
        }
        self.stop_project_check_runs(project_id, settle_until).await
    }

    /// Force deletion stops the Project's unfinished check runs first and
    /// waits, until `settle_until`, for each to settle. Their consumers are
    /// cancelled, so nobody waits for a result: a queued run is cancelled in
    /// that same write (`cancel_project_check_consumers`; it gets no result
    /// row), a running one is cancelled on its owner by the check worker
    /// (which notices a run without consumers within about a second). A run
    /// that has not settled by then stays live, and the deletion guard then
    /// refuses: the row that fences the process is never deleted from under
    /// it. Without a check worker nothing can settle a run, so nothing is
    /// cancelled either.
    async fn stop_project_check_runs(
        &self,
        project_id: &str,
        settle_until: tokio::time::Instant,
    ) -> Result<()> {
        if self
            .db
            .live_project_check_runs(project_id)
            .await?
            .is_empty()
        {
            return Ok(());
        }
        if self.check_worker().is_none() {
            return Ok(());
        }
        self.db.cancel_project_check_consumers(project_id).await?;
        loop {
            let live = self.db.live_project_check_runs(project_id).await?;
            if live.is_empty() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= settle_until {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
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
        let hard_deadline_at = execution::execution_deadline_seconds(&snapshot)
            .map(|seconds| execution::rfc3339_after(&input.updated_at, i64::from(seconds)));
        let placement = match input.workspace_id.as_deref() {
            Some(id) => db::WorkspacePlacementRepo::get_by_workspace_id(&*self.db, id).await?,
            None => None,
        };
        let daemon_id = placement.as_ref().and_then(|placement| {
            placement
                .execution_daemon_id
                .as_deref()
                .or(placement.daemon_id.as_deref())
        });
        let remote_owner = daemon_id.and_then(|daemon_id| {
            self.daemon_connections
                .as_ref()
                .and_then(|registry| registry.get(daemon_id))
                .map(|connection| {
                    crate::daemon_transport::execution_lease_owner(daemon_id, connection.id())
                })
        });
        let (owner, lease_expires_at) = match remote_owner {
            Some(owner) => (
                owner,
                execution::bounded_lease_expiry(&input.updated_at, hard_deadline_at.as_deref()),
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
        mut input: CreateExecution,
        workspace_created_by_attempt: bool,
    ) -> Result<Execution> {
        if !db::task_writer::owns_task(&input.task_id) {
            return self
                .request_task_command(
                    &input.task_id,
                    "create_running_execution",
                    serde_json::json!([input, workspace_created_by_attempt]),
                    false,
                )
                .await;
        }
        let repository_context = if let Some(workspace_id) = input.workspace_id.as_deref() {
            let task = TaskRepo::get_by_id(&*self.db, &input.task_id, false)
                .await?
                .ok_or_else(|| ServiceError::not_found("task", input.task_id.clone()))?;
            let workspace = WorkspaceRepo::get_by_id(&*self.db, workspace_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("workspace", workspace_id.to_owned()))?;
            Some((task, workspace))
        } else {
            return Err(ServiceError::invalid_operation(
                "repository execution requires a scheduler WorkspaceLease-backed workspace",
            ));
        };

        // Capacity is part of the same snapshot as the task/workflow facts.
        // The authoritative transaction will compare this max and count
        // running executions again before INSERT.
        let agent = match input.agent_id.as_deref() {
            Some(agent_id) => AgentRepo::get_by_id(&*self.db, agent_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("agent", agent_id.to_owned()))?,
            None => {
                return Err(ServiceError::invalid_operation(
                    "running execution requires an agent principal",
                ))
            }
        };

        // Carry the role and exact governing-workflow decision made by the
        // caller into the authoritative transaction. Interactive executions
        // are deliberately outside the workflow-role admission check; every
        // workflow role (including the historical `executor`/`coder` alias)
        // is revalidated immediately before INSERT and fails closed if the
        // Task or workflow no longer grants that role.
        let reviewer_snapshot = if input.role == crate::workflow::default_roles::REVIEWER {
            let reviews = ReviewRepo::list_by_task(&*self.db, &input.task_id).await?;
            // A correctly bound reviewer resume already carries the candidate
            // as its parent. Prefer that exact Review row; legacy reviewer
            // rows used the reviewer execution itself as parent, so fall back
            // to the latest task review while repairing their lineage.
            let bound_review = input.parent_execution_id.as_deref().and_then(|parent_id| {
                reviews
                    .iter()
                    .find(|review| review.execution_id == parent_id)
                    .cloned()
            });
            bound_review
                .or_else(|| {
                    reviews
                        .into_iter()
                        .max_by_key(|review| (review.attempt_number, review.id.clone()))
                })
                .map(|review| {
                    (
                        review.id,
                        review.execution_id,
                        review.attempt_number,
                        review.status.to_string(),
                        review.updated_at,
                        review.reviewer_execution_id,
                        review.auditor_execution_id,
                    )
                })
        } else {
            None
        };
        let mut admission = if input.role == crate::workflow::default_roles::INTERACTIVE {
            let (task, _) = repository_context
                .as_ref()
                .expect("repository context exists for running execution");
            let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
            Some(
                self::execution_admission_for_task(
                    &self.db,
                    task,
                    "",
                    &input.role,
                    Some(&agent),
                    project.version,
                )
                .await?,
            )
        } else {
            let (task, _) = repository_context
                .as_ref()
                .expect("repository context exists for running execution");
            let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
            Some(
                self::execution_admission_for_task(
                    &self.db,
                    task,
                    &project.workflow_definition,
                    &input.role,
                    Some(&agent),
                    project.version,
                )
                .await?,
            )
        };
        if input.role == crate::workflow::default_roles::REVIEWER {
            let Some((
                id,
                execution_id,
                attempt_number,
                status,
                updated_at,
                reviewer_execution_id,
                auditor_execution_id,
            )) = reviewer_snapshot
            else {
                return Err(ServiceError::conflict(
                    "reviewer execution requires a current review candidate",
                ));
            };
            // Review::execution_id is the candidate being reviewed, not the
            // reviewer execution that supplied the resume session. Keep the
            // latter in the executor snapshot/session fields, but persist the
            // candidate as the durable lineage parent used by reviewer
            // cascade/reconciliation.
            input.parent_execution_id = Some(execution_id.clone());
            if let Some(admission) = admission.as_mut() {
                admission.expected_reviewer_parent_execution_id = Some(execution_id.clone());
                admission.expected_latest_review_candidate_execution_id = Some(execution_id);
                admission.expected_reviewer_id = Some(id);
                admission.expected_reviewer_attempt_number = Some(attempt_number);
                admission.expected_reviewer_status = Some(status);
                admission.expected_reviewer_updated_at = Some(updated_at);
                admission.expected_reviewer_execution_id = reviewer_execution_id;
                admission.expected_auditor_execution_id = auditor_execution_id;
            }
        }
        self.create_running_execution_with_admission(
            input,
            workspace_created_by_attempt,
            admission,
            None,
        )
        .await
    }

    async fn create_running_execution_with_admission(
        &self,
        mut input: CreateExecution,
        workspace_created_by_attempt: bool,
        mut admission: Option<ExecutionAdmission>,
        workspace_admission: Option<&workspace::WorkspaceAdmission>,
    ) -> Result<Execution> {
        // Replay authority follows this call chain only; spawned execution
        // runners and unrelated API requests do not inherit the task-local.
        if let Ok((task_id, recovery_id)) = execution::REPLAYING_RECOVERY.try_with(Clone::clone) {
            if task_id == input.task_id {
                let admission = admission.as_mut().ok_or_else(|| {
                    ServiceError::invalid_operation("queued replay requires execution admission")
                })?;
                admission.expected_queued_recovery_id = Some(recovery_id);
            }
        }
        let repository_context = if let Some(workspace_id) = input.workspace_id.as_deref() {
            let task = TaskRepo::get_by_id(&*self.db, &input.task_id, false)
                .await?
                .ok_or_else(|| ServiceError::not_found("task", input.task_id.clone()))?;
            let workspace = WorkspaceRepo::get_by_id(&*self.db, workspace_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("workspace", workspace_id.to_owned()))?;
            Some((task, workspace))
        } else {
            return Err(ServiceError::invalid_operation(
                "repository execution requires a scheduler WorkspaceLease-backed workspace",
            ));
        };

        // The snapshot and INSERT must carry one authority snapshot. Config
        // resolution happens earlier and resume/follow-up paths may clone an
        // older snapshot, so overwrite every workflow field immediately
        // before the authoritative admission transaction.
        if input.status == ExecutionStatus::Running {
            if let Some(snapshot_json) = input.executor_config_snapshot_json.as_deref() {
                let snapshot_value: Value =
                    serde_json::from_str(snapshot_json).map_err(|error| {
                        ServiceError::invalid_operation(format!(
                            "invalid executor config snapshot: {error}"
                        ))
                    })?;
                let brokered = snapshot_value.get("plan_delivery").and_then(Value::as_str)
                    == Some("execution_outbox");
                let admitted = admission.as_ref().and_then(|admission| {
                    admission.expected_project_version.map(|project_version| {
                        (admission.expected_task_status.as_str(), project_version)
                    })
                });
                if brokered && admitted.is_none() {
                    return Err(ServiceError::invalid_operation(
                        "brokered execution requires admitted Task and Project authority",
                    ));
                }
                if let Some((task_state, project_version)) = admitted {
                    let state_entry_token = self::action_resolver::latest_state_entry_authority(
                        &self.db,
                        &input.task_id,
                        task_state,
                    )
                    .await?
                    .map(|entry| entry.id);
                    input.executor_config_snapshot_json =
                        Some(self::config::stamp_executor_snapshot_authority(
                            snapshot_json,
                            task_state,
                            state_entry_token.as_deref(),
                            project_version,
                        )?);
                }
            }
        }

        let create_result: Result<Execution> = async {
            if input.status == ExecutionStatus::Running {
                let (task, workspace) = repository_context
                    .as_ref()
                    .expect("repository context loaded");
                let prepared_admission;
                let workspace_admission = match workspace_admission {
                    Some(admission) => admission,
                    None => {
                        let agent = match input.agent_id.as_deref() {
                            Some(id) => {
                                Some(AgentRepo::get_by_id(&*self.db, id).await?.ok_or_else(
                                    || ServiceError::not_found("agent", id.to_owned()),
                                )?)
                            }
                            None => None,
                        };
                        // Placement adds a large reserve/prepare future to every
                        // recovery path; keep it off the enclosing admission frame.
                        let reservation = Box::pin(self.reserve_claim_workspace(
                            task,
                            agent.as_ref(),
                            &input.role,
                        ))
                        .await?;
                        prepared_admission =
                            Box::pin(self.prepare_claim_workspace(reservation)).await?;
                        &prepared_admission
                    }
                };
                if workspace.id != workspace_admission.workspace.id {
                    return Err(DbError::VersionConflict.into());
                }
                let lease = self.initial_execution_lease(&input).await?;
                let mut transaction = db::begin_immediate(self.db.pool()).await?;
                self.check_claim_placement_in_tx(&mut transaction, task, workspace_admission)
                    .await?;
                self.check_placement_lease_owner(&workspace_admission.placement, &lease)?;
                let execution = ExecutionRepo::create_with_lease_and_admission_in_tx(
                    &*self.db,
                    &mut transaction,
                    input.clone(),
                    lease,
                    admission,
                )
                .await?;
                self.issue_workspace_lease_in_tx(
                    &mut transaction,
                    task,
                    &workspace_admission.workspace,
                    &input.role,
                    input.agent_id.as_deref(),
                    &input.id,
                )
                .await?;
                if let Some(attempt) =
                    crate::workflow::engine::durable::current_hook(&input.task_id)
                {
                    self.db
                        .fence_hook_in_tx(&mut transaction, &attempt.step)
                        .await?;
                    let linked = self
                        .db
                        .link_task_hook_execution_in_tx(
                            &mut transaction,
                            &execution.id,
                            &attempt.step.id,
                            attempt.index,
                        )
                        .await?
                        .rows_affected();
                    if linked != 1 {
                        return Err(DbError::VersionConflict.into());
                    }
                }
                transaction.commit().await?;
                Ok(execution)
            } else {
                Ok(ExecutionRepo::create(&*self.db, input.clone()).await?)
            }
        }
        .await;
        let execution = match create_result {
            Ok(execution) => execution,
            Err(error) => {
                if crate::placement::is_machine_capacity_refusal(&error) {
                    if let Some(task) =
                        TaskRepo::get_by_id(&*self.db, &input.task_id, false).await?
                    {
                        self.defer_placement_refusal(&task, &error).await?;
                    }
                }
                if workspace_created_by_attempt
                    && !crate::placement::is_machine_capacity_refusal(&error)
                {
                    self.cleanup_fresh_execution_workspace_by_id(
                        &input.task_id,
                        input.workspace_id.as_deref(),
                    )
                    .await;
                }
                return Err(error);
            }
        };
        Ok(execution)
    }

    async fn cleanup_fresh_execution_workspace_by_id(
        &self,
        task_id: &str,
        workspace_id: Option<&str>,
    ) {
        let mut removed_workspace = false;
        let workspace = if let Some(workspace_id) = workspace_id {
            WorkspaceRepo::get_by_id(&*self.db, workspace_id)
                .await
                .ok()
                .flatten()
        } else {
            None
        };
        // Capture the server path while its placement row still exists. The
        // conditional DELETE below is the admission/concurrent-launch fence.
        let worktree_path = match workspace.as_ref() {
            Some(workspace) => self
                .workspace_backend_router
                .embedded_path(&self.db, workspace)
                .await
                .ok(),
            None => None,
        };
        if let Some(workspace_id) = workspace_id {
            // Delete only our workspace row and only while no execution has
            // acquired it. This protects a concurrent launch which reused
            // the same Task workspace after this attempt lost admission.
            let deleted: std::result::Result<bool, db::DbError> = async {
                let mut transaction = db::begin_immediate(self.db.pool()).await?;
                let removed = self.db.delete_unacquired_task_workspace_in_tx(&mut transaction, workspace_id, task_id).await?
                .rows_affected()
                    == 1;
                if removed {
                    // A cancellation fences its Task through this row.
                    if let Err(error) = self
                        .db
                        .produce_condition_in_tx(
                            &mut transaction,
                            task_id,
                            db::ConditionChange::Operations,
                        )
                        .await
                    {
                        tracing::warn!(task_id, %error, "Task condition not updated after workspace removal");
                    }
                }
                transaction.commit().await?;
                Ok(removed)
            }
            .await;
            match deleted {
                Ok(removed) => removed_workspace = removed,
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
            let Some(workspace) = workspace else {
                return;
            };
            let Some(worktree_path) = worktree_path else {
                return;
            };
            let source = match RepoRepo::get_by_id(&*self.db, &workspace.repo_id).await {
                Ok(Some(repo)) => {
                    match project_agent_workspace::resolve_repo_source(&repo, &self.workspace_root)
                        .await
                    {
                        Ok(source) => source,
                        Err(error) => {
                            tracing::warn!(task_id, %error, "failed to resolve rejected execution workspace repository");
                            return;
                        }
                    }
                }
                _ => return,
            };
            if let Err(cleanup_error) = manager
                .cleanup_worktree(task_id, std::path::Path::new(&source), &worktree_path)
                .await
            {
                tracing::warn!(
                    task_id,
                    %cleanup_error,
                    "failed to remove fresh worktree after rejected execution"
                );
            }
        }
    }

    pub(crate) async fn execution_owns_current_role_attempt(
        &self,
        task: &Task,
        execution: &Execution,
    ) -> Result<bool> {
        if !execution::execution_belongs_to_current_state_entry(&self.db, task, execution).await? {
            return Ok(false);
        }
        let Some(project) = ProjectRepo::get_by_id(&*self.db, &task.project_id).await? else {
            return Ok(false);
        };
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &Actor::system(api_types::SystemComponent::Workflow),
        );
        let current_role = workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
            .and_then(crate::workflow::effective_role);
        let role_matches = execution.role == "interactive"
            || current_role == Some(execution.role.as_str())
            || (current_role == Some(crate::workflow::default_roles::CODER)
                && execution.role == "executor");
        if !role_matches {
            return Ok(false);
        }
        if execution.role != "interactive" {
            let assignment_role = if execution.role == "executor" {
                crate::workflow::default_roles::CODER
            } else {
                execution.role.as_str()
            };
            // The own row is live ownership for every role but one: a
            // subtask's step check is held by its root's lead, through the
            // resolver. (A default worker stays admission authority only;
            // see the inherited-coder arm below.)
            let assignment = if crate::task_hierarchy::step_check_root(task, assignment_role)
                .is_some()
            {
                crate::task_hierarchy::effective_role_assignment(&self.db, task, assignment_role)
                    .await?
                    .map(|resolved| resolved.assignment)
            } else {
                TaskRoleAssignmentRepo::get_by_task_and_role(&*self.db, &task.id, assignment_role)
                    .await?
            };
            if let Some(assignment) = assignment {
                if assignment.assignee_type.as_ref() != Some(&db::AssigneeKind::Agent)
                    || assignment.assignee_id.as_deref() != execution.agent_id.as_deref()
                {
                    return Ok(false);
                }
                if chrono::DateTime::parse_from_rfc3339(&assignment.updated_at)
                    .ok()
                    .zip(chrono::DateTime::parse_from_rfc3339(&execution.updated_at).ok())
                    .is_some_and(|(assignment, execution)| assignment > execution)
                {
                    return Ok(false);
                }
            } else if assignment_role == crate::workflow::default_roles::CODER
                && task.parent_task_id.is_some()
            {
                // An inherited root assignment is admission authority, not
                // live ownership of an already-running child. Root default
                // changes intentionally affect only children that have not
                // started, so an admitted child with no own coder keeps its
                // current role attempt through completion.
            } else if execution.agent_id.is_some() {
                return Ok(false);
            }
        }
        let roles: &[&str] = if matches!(execution.role.as_str(), "coder" | "executor") {
            &["coder", "executor"]
        } else {
            &[execution.role.as_str()]
        };
        let mut latest: Option<Execution> = None;
        for role in roles {
            let page = ExecutionRepo::list_by_task_and_role(
                &*self.db,
                &task.id,
                role,
                db::PageRequest {
                    cursor: None,
                    limit: 1,
                    include_total: false,
                    sort_by: db::SortBy::CreatedAt,
                    sort_order: db::SortOrder::Desc,
                },
            )
            .await?;
            if let Some(candidate) = page.items.into_iter().next() {
                let replace = latest.as_ref().is_none_or(|current| {
                    (&candidate.created_at, &candidate.id) > (&current.created_at, &current.id)
                });
                if replace {
                    latest = Some(candidate);
                }
            }
        }
        Ok(latest.is_some_and(|latest| latest.id == execution.id))
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

impl TaskService {
    pub fn task_step_worker(&self) -> Arc<crate::worker_runtime::queue::TaskStepWorker> {
        let mut cached = self.task_step_driver.lock().expect("Task step driver");
        if let Some(worker) = cached.as_ref() {
            return Arc::clone(worker);
        }
        let mut service = self.clone();
        // The worker's service must not retain the cache that owns it.
        service.task_step_driver = Arc::default();
        let worker = Arc::new(crate::worker_runtime::queue::TaskStepWorker::new(service));
        let executor: Arc<dyn db::task_writer::TaskStepExecutor> = worker.clone();
        self.db.set_task_step_executor(Arc::downgrade(&executor));
        *cached = Some(worker.clone());
        worker
    }
    #[cfg(any(test, feature = "test-support"))]
    pub async fn drain(&self, task_id: &str) -> Result<Task> {
        self.task_step_worker().drain(task_id).await
    }
}
