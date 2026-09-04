use std::{future::Future, path::PathBuf, pin::Pin, sync::Arc, time::Instant};

use api_types::{
    Actor, FailurePolicy, StateDefinition, StateKind, TaskMovedEventPayload, WorkflowDefinition,
    WorkflowTrigger,
};
use db::{
    new_uuid_v4, now_rfc3339, CompareAndMoveTask, CreateDomainEvent, DomainEventRepo,
    MoveTaskPersistence, MoveTaskResult, ProjectRepo, TaskBoardRepo, TaskRepo, TransitionLog,
    TransitionLogRepo, UpdateTask,
};
use events::{event_timestamp, EventBus, EventContext, ForgeEvent, TASK_MOVED_EVENT};
use executors::TaskExecutor;
use sqlx::query;
use tracing::Instrument;
use workspace::RepoCacheLockManager;

use self::{
    context::{
        has_running_execution, latest_execution_context, latest_executor_context, latest_review,
    },
    hooks::{
        effective_after_enter_hooks, elapsed_ms, hook_audience_matches, hook_result_entry,
        log_hook_result, log_hook_skipped_by_audience, log_hook_start, merged_state_config,
    },
};
use crate::{
    deferred_dispatch,
    merge_service::MergeService,
    terminal_service::TerminalActivityTracker,
    workflow::{default_workflow, inherited_subtask_workflow, registry, HookContext, HookResult},
    workspace_cleanup::WorkspaceCleanupScheduler,
    workspace_execution_lock::WorkspaceExecutionLockManager,
    ServiceError,
};

mod context;
mod hooks;
#[cfg(test)]
mod tests;

/// `error_annotation` type recorded when a dispatch hook fails entering an
/// active state. The task dispatcher treats it as blocking, so a task whose
/// dispatch deterministically fails (e.g. governance says it is not runnable)
/// is parked instead of rescheduled in a loop.
pub(crate) const DISPATCH_FAILED_ANNOTATION: &str = "dispatch_failed";

fn dispatch_failed_annotation_json(state: &str, message: &str) -> String {
    serde_json::json!({
        "type": DISPATCH_FAILED_ANNOTATION,
        "message": message,
        "state": state,
        "detected_at": now_rfc3339(),
        "recovery_actions": ["reexecute", "reset_to_initial", "cancel_task"],
    })
    .to_string()
}

pub(crate) fn is_dispatch_failed_annotation(raw_annotation: Option<&str>) -> bool {
    raw_annotation
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        .and_then(|annotation| {
            annotation
                .get("type")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .is_some_and(|kind| kind == DISPATCH_FAILED_ANNOTATION)
}

/// Persist a `dispatch_failed` error annotation on the task, retrying version
/// conflicts. Shared by the engine's dispatch-failure fallback and the task
/// dispatcher's governance parking.
pub(crate) async fn annotate_dispatch_failure(
    db: &db::SqliteDb,
    task_id: &str,
    state: &str,
    message: &str,
) -> db::Result<()> {
    let annotation = dispatch_failed_annotation_json(state, message);
    let mut current = TaskRepo::get_by_id(db, task_id, false)
        .await?
        .ok_or(db::DbError::NotFound)?;
    for attempt in 0..3 {
        match TaskRepo::update(
            db,
            UpdateTask {
                id: current.id.clone(),
                expected_version: current.version,
                title: None,
                description: None,
                priority: None,
                merge_config: None,
                plan: None,
                error_annotation: Some(Some(annotation.clone())),
                blocked_json: None,
                failed_json: None,
                task_state_config: None,
                parent_task_id: None,
                updated_at: now_rfc3339(),
            },
        )
        .await
        {
            Ok(_) => return Ok(()),
            Err(db::DbError::VersionConflict) if attempt < 2 => {
                current = TaskRepo::get_by_id(db, task_id, false)
                    .await?
                    .ok_or(db::DbError::NotFound)?;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// Clear a `dispatch_failed` error annotation (and only that annotation type)
/// once a dispatch succeeds again, so the task dispatcher resumes scheduling
/// and recovery for the task.
pub(crate) async fn clear_dispatch_failure_annotation(
    db: &db::SqliteDb,
    task_id: &str,
) -> db::Result<()> {
    let mut current = TaskRepo::get_by_id(db, task_id, false)
        .await?
        .ok_or(db::DbError::NotFound)?;
    for attempt in 0..3 {
        if !is_dispatch_failed_annotation(current.error_annotation.as_deref()) {
            return Ok(());
        }
        match TaskRepo::update(
            db,
            UpdateTask {
                id: current.id.clone(),
                expected_version: current.version,
                title: None,
                description: None,
                priority: None,
                merge_config: None,
                plan: None,
                error_annotation: Some(None),
                blocked_json: None,
                failed_json: None,
                task_state_config: None,
                parent_task_id: None,
                updated_at: now_rfc3339(),
            },
        )
        .await
        {
            Ok(_) => return Ok(()),
            Err(db::DbError::VersionConflict) if attempt < 2 => {
                current = TaskRepo::get_by_id(db, task_id, false)
                    .await?
                    .ok_or(db::DbError::NotFound)?;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

pub struct WorkflowEngine {
    pub db: Arc<db::SqliteDb>,
    pub event_bus: Arc<EventBus>,
    pub review_runner: Option<Arc<review::ReviewRunner>>,
    pub merge_service: Option<Arc<MergeService>>,
    pub cleanup_scheduler: Option<Arc<WorkspaceCleanupScheduler>>,
    pub task_executor: Option<Arc<dyn TaskExecutor>>,
    pub daemon_connections: Option<Arc<crate::daemon_transport::DaemonConnectionRegistry>>,
    pub workspace_exec_locks: Option<Arc<WorkspaceExecutionLockManager>>,
    pub terminal_activity: Option<Arc<TerminalActivityTracker>>,
    pub workspace_root: PathBuf,
    pub repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
}

pub struct TransitionResult {
    pub task: db::Task,
    pub review: Option<db::Review>,
    pub cascaded: bool,
    pub board_move: Option<BoardMoveOutcome>,
}

#[derive(Debug, Clone)]
pub struct BoardMoveRequest {
    pub operation_id: String,
    pub project_id: String,
    pub board_revision: i64,
    pub target_column_statuses: Vec<String>,
    pub before_id: Option<String>,
    pub after_id: Option<String>,
}

#[derive(Debug, Clone)]
pub enum BoardMoveOutcome {
    Committed(MoveTaskResult),
    Replayed(MoveTaskResult),
}

impl WorkflowEngine {
    #[tracing::instrument(
        skip(self, workflow),
        fields(
            task_id = %task_id,
            target_state = %target_state,
            version = version,
            actor = %actor,
            reason = %reason,
            rejection = rejection,
        )
    )]
    #[allow(clippy::too_many_arguments)]
    pub async fn transition(
        &self,
        task_id: &str,
        target_state: &str,
        version: i64,
        workflow: &WorkflowDefinition,
        actor: &Actor,
        reason: &str,
        rejection: bool,
    ) -> crate::Result<TransitionResult> {
        self.transition_with_deferred_dispatch(
            task_id,
            target_state,
            version,
            workflow,
            actor,
            reason,
            rejection,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn transition_with_deferred_dispatch(
        &self,
        task_id: &str,
        target_state: &str,
        version: i64,
        workflow: &WorkflowDefinition,
        actor: &Actor,
        reason: &str,
        rejection: bool,
        defer_dispatch_until: Option<String>,
    ) -> crate::Result<TransitionResult> {
        self.transition_inner(
            task_id.to_string(),
            target_state.to_string(),
            version,
            workflow,
            actor.clone(),
            reason.to_string(),
            rejection,
            false,
            defer_dispatch_until,
            None,
            0,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn move_task(
        &self,
        task_id: &str,
        target_state: &str,
        version: i64,
        workflow: &WorkflowDefinition,
        actor: &Actor,
        reason: &str,
        move_request: BoardMoveRequest,
    ) -> crate::Result<TransitionResult> {
        self.transition_inner(
            task_id.to_owned(),
            target_state.to_owned(),
            version,
            workflow,
            actor.clone(),
            reason.to_owned(),
            false,
            false,
            None,
            Some(move_request),
            0,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn manual_override_transition(
        &self,
        task_id: &str,
        target_state: &str,
        version: i64,
        workflow: &WorkflowDefinition,
        actor: Actor,
        reason: &str,
        rejection: bool,
    ) -> crate::Result<TransitionResult> {
        self.transition_inner(
            task_id.to_string(),
            target_state.to_string(),
            version,
            workflow,
            actor,
            reason.to_string(),
            rejection,
            true,
            None,
            None,
            0,
        )
        .await
    }

    pub async fn retry_entry_barrier(
        &self,
        task_id: &str,
        version: i64,
        workflow: &WorkflowDefinition,
        actor: &Actor,
        reason: &str,
    ) -> crate::Result<TransitionResult> {
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        if task.version != version {
            return Err(db::DbError::VersionConflict.into());
        }
        let Some(raw_barrier) = task.entry_barrier_json.as_deref() else {
            return Err(ServiceError::invalid_operation(
                "task has no blocked entry barrier to retry",
            ));
        };
        let barrier: serde_json::Value = serde_json::from_str(raw_barrier).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid entry barrier metadata: {error}"))
        })?;
        if barrier.get("status").and_then(serde_json::Value::as_str) != Some("blocked") {
            return Err(ServiceError::invalid_operation(
                "task entry barrier is not blocked",
            ));
        }
        let target_state = barrier
            .get("state")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(task.status.as_str())
            .to_owned();
        if target_state != task.status {
            return Err(ServiceError::invalid_operation(format!(
                "blocked entry barrier targets state '{}' but task is in '{}'",
                target_state, task.status
            )));
        }
        let state = Self::find_state(workflow, &target_state).ok_or_else(|| {
            ServiceError::InvalidOperation {
                message: Self::undefined_state_message(&target_state, workflow),
            }
        })?;

        let started_at = barrier
            .get("started_at")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(now_rfc3339);
        let retry_started_at = now_rfc3339();
        let running_barrier = serde_json::json!({
            "state": target_state.as_str(),
            "status": "running",
            "started_at": started_at.as_str(),
            "retry_started_at": retry_started_at.as_str(),
            "retry_reason": reason,
        })
        .to_string();
        let mut task = TaskRepo::set_entry_barrier(
            &*self.db,
            task_id,
            task.version,
            Some(running_barrier),
            &retry_started_at,
        )
        .await?;

        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let state_config =
            merged_state_config(state, Some(&project), task.task_state_config.as_deref());
        let workflow_ctx = Arc::new(workflow.clone());
        let latest_execution = latest_execution_context(&self.db, &task.id).await?;
        let latest_executor = latest_executor_context(&self.db, &task.id).await?;
        let workspace_id = latest_execution
            .as_ref()
            .and_then(|execution| execution.workspace_id.clone())
            .or_else(|| {
                latest_executor
                    .as_ref()
                    .and_then(|execution| execution.workspace_id.clone())
            });
        let execution_id = latest_executor
            .as_ref()
            .map(|execution| execution.id.clone())
            .or_else(|| {
                latest_execution
                    .as_ref()
                    .map(|execution| execution.id.clone())
            });
        let enter_ctx = HookContext {
            task_id: task.id.clone(),
            project_id: task.project_id.clone(),
            from_state: target_state.clone(),
            to_state: target_state.clone(),
            db: Arc::clone(&self.db),
            event_bus: Arc::clone(&self.event_bus),
            gate_config: state.gate_config.clone(),
            workflow: Arc::clone(&workflow_ctx),
            triggered_by: actor.clone(),
            review_runner: self.review_runner.clone(),
            merge_service: self.merge_service.clone(),
            cleanup_scheduler: self.cleanup_scheduler.clone(),
            task_executor: self.task_executor.clone(),
            daemon_connections: self.daemon_connections.clone(),
            workspace_exec_locks: self.workspace_exec_locks.clone(),
            terminal_activity: self.terminal_activity.clone(),
            workspace_root: self.workspace_root.clone(),
            repo_cache_locks: self.repo_cache_locks.clone(),
            workspace_id,
            agent_id: latest_execution
                .as_ref()
                .and_then(|execution| execution.agent_id.clone()),
            execution_id,
            state_config,
        };

        let mut cascade: Option<(String, String)> = None;
        let mut blocked = false;
        for hook in &state.hooks.before_enter {
            if !hook_audience_matches(hook.applies_to, actor) {
                log_hook_skipped_by_audience(
                    &task.id,
                    &target_state,
                    &target_state,
                    "before_enter",
                    hook,
                    actor,
                );
                continue;
            }
            let action = registry::resolve_action(&hook.action)?;
            log_hook_start(
                &task.id,
                &target_state,
                &target_state,
                "before_enter",
                hook,
                actor,
            );
            let started = Instant::now();
            let result = action.execute(&enter_ctx).await;
            let duration_ms = elapsed_ms(started);
            log_hook_result(
                &task.id,
                &target_state,
                &target_state,
                "before_enter",
                hook,
                &result,
                duration_ms,
            );
            match result {
                HookResult::Failed { reason: error } => {
                    if matches!(hook.on_failure, FailurePolicy::Block) {
                        let blocked_at = now_rfc3339();
                        let blocked_barrier = serde_json::json!({
                            "state": target_state.as_str(),
                            "status": "blocked",
                            "started_at": started_at.as_str(),
                            "updated_at": blocked_at.as_str(),
                            "blocking_reason": error.as_str(),
                            "retry_reason": reason,
                        })
                        .to_string();
                        task = TaskRepo::set_entry_barrier(
                            &*self.db,
                            task_id,
                            task.version,
                            Some(blocked_barrier),
                            &blocked_at,
                        )
                        .await?;
                        blocked = true;
                        break;
                    }
                }
                HookResult::Cascade {
                    to,
                    reason: cascade_reason,
                } => {
                    cascade = Some((to, cascade_reason));
                    break;
                }
                HookResult::Ok | HookResult::Skipped { .. } => {}
            }
        }

        if blocked {
            let review = latest_review(&self.db, &task.id).await?;
            return Ok(TransitionResult {
                task,
                review,
                cascaded: false,
                board_move: None,
            });
        }

        if cascade.is_none() {
            let cleared_at = now_rfc3339();
            task = TaskRepo::set_entry_barrier(&*self.db, task_id, task.version, None, &cleared_at)
                .await?;
            task = TaskRepo::update(
                &*self.db,
                UpdateTask {
                    id: task.id.clone(),
                    expected_version: task.version,
                    title: None,
                    description: None,
                    priority: None,
                    merge_config: None,
                    plan: None,
                    error_annotation: Some(None),
                    blocked_json: Some(None),
                    failed_json: None,
                    task_state_config: None,
                    parent_task_id: None,
                    updated_at: now_rfc3339(),
                },
            )
            .await?;

            for hook in &state.hooks.on_enter {
                if !hook_audience_matches(hook.applies_to, actor) {
                    continue;
                }
                let action = registry::resolve_action(&hook.action)?;
                let result = action.execute(&enter_ctx).await;
                if let HookResult::Cascade {
                    to,
                    reason: cascade_reason,
                } = result
                {
                    cascade = Some((to, cascade_reason));
                    break;
                }
            }
        }

        if cascade.is_none() {
            let effective_after_enter_hooks = effective_after_enter_hooks(state);
            for hook in &effective_after_enter_hooks {
                if !hook_audience_matches(hook.applies_to, actor) {
                    continue;
                }
                let action = registry::resolve_action(&hook.action)?;
                let result = action.execute(&enter_ctx).await;
                if let HookResult::Cascade {
                    to,
                    reason: cascade_reason,
                } = result
                {
                    cascade = Some((to, cascade_reason));
                    break;
                }
            }
        }

        if let Some((cascade_to, cascade_reason)) = cascade {
            let mut cascaded = self
                .transition_inner(
                    task_id.to_owned(),
                    cascade_to,
                    task.version,
                    workflow,
                    Actor::system(api_types::SystemComponent::Workflow),
                    cascade_reason,
                    false,
                    false,
                    None,
                    None,
                    1,
                )
                .await?;
            cascaded.cascaded = true;
            return Ok(cascaded);
        }

        let review = latest_review(&self.db, &task.id).await?;
        Ok(TransitionResult {
            task,
            review,
            cascaded: false,
            board_move: None,
        })
    }

    #[tracing::instrument(
        skip(self, workflow),
        fields(task_id = %task_id, target_state = %target_state, version = version, actor = %actor, reason = %reason)
    )]
    pub async fn reset_to_initial(
        &self,
        task_id: &str,
        target_state: &str,
        version: i64,
        workflow: &WorkflowDefinition,
        actor: &Actor,
        reason: &str,
    ) -> crate::Result<db::Task> {
        let to_state = Self::find_state(workflow, target_state).ok_or_else(|| {
            ServiceError::InvalidOperation {
                message: Self::undefined_state_message(target_state, workflow),
            }
        })?;
        if to_state.kind != StateKind::Initial {
            return Err(ServiceError::InvalidOperation {
                message: format!("state '{target_state}' is not the workflow initial state"),
            });
        }

        let result = self
            .transition_inner(
                task_id.to_string(),
                target_state.to_string(),
                version,
                workflow,
                actor.clone(),
                reason.to_string(),
                false,
                true,
                None,
                None,
                0,
            )
            .await?;
        Ok(result.task)
    }

    pub fn validate_claimable(
        workflow: &WorkflowDefinition,
        current_status: &str,
    ) -> crate::Result<()> {
        if let Some(state) = Self::find_state(workflow, current_status) {
            if state.kind == StateKind::Backlog {
                return Err(ServiceError::InvalidOperation {
                    message: "task is in backlog and cannot be claimed".to_string(),
                });
            }
        }
        Ok(())
    }

    fn transition_requires_system_actor(
        trigger: WorkflowTrigger,
        from_state: &StateDefinition,
        to_state: &StateDefinition,
    ) -> bool {
        if !trigger.system_only() {
            return false;
        }

        let is_direct_work_start = trigger == WorkflowTrigger::Retry
            && from_state.kind == StateKind::Initial
            && to_state.kind == StateKind::Active;
        !is_direct_work_start
    }

    pub fn resolve_workflow(workflow_definition_json: &str) -> WorkflowDefinition {
        let raw = workflow_definition_json.trim();
        if raw.is_empty() || raw == "{}" {
            return default_workflow::default_workflow();
        }

        serde_json::from_str(raw).unwrap_or_else(|_| default_workflow::default_workflow())
    }

    pub fn resolve_subtask_workflow() -> WorkflowDefinition {
        inherited_subtask_workflow()
    }

    /// Single source of truth for which workflow governs a task at transition entry.
    ///
    /// - Root tasks always use the project workflow.
    /// - Subtasks in a state absent from the inherited subtask workflow use the project
    ///   workflow for every actor.
    /// - Subtasks in a shared subtask-workflow state use the inherited subtask workflow
    ///   for non-user actors and the project workflow for user actors.
    pub fn resolve_workflow_for_task(
        task: &db::Task,
        workflow_definition_json: &str,
        actor: &Actor,
    ) -> WorkflowDefinition {
        let project_workflow = Self::resolve_workflow(workflow_definition_json);
        if task.parent_task_id.is_none() {
            return project_workflow;
        }

        let subtask_wf = inherited_subtask_workflow();
        let current_in_subtask = subtask_wf
            .states
            .iter()
            .any(|s| s.name.as_str() == task.status.as_str());
        if !current_in_subtask {
            return project_workflow;
        }
        if actor.is_user() {
            return project_workflow;
        }
        subtask_wf
    }

    #[allow(clippy::too_many_arguments)]
    fn transition_inner<'a>(
        &'a self,
        task_id: String,
        target_state: String,
        version: i64,
        workflow: &'a WorkflowDefinition,
        actor: Actor,
        reason: String,
        rejection: bool,
        skip_before_exit: bool,
        defer_dispatch_until: Option<String>,
        board_move: Option<BoardMoveRequest>,
        depth: u8,
    ) -> Pin<Box<dyn Future<Output = crate::Result<TransitionResult>> + Send + 'a>> {
        let span = tracing::info_span!(
            "workflow.transition_inner",
            task_id = %task_id,
            target_state = %target_state,
            version = version,
            actor = %actor,
            reason = %reason,
            rejection = rejection,
            skip_before_exit = skip_before_exit,
            defer_dispatch = defer_dispatch_until.is_some(),
            depth = depth,
        );

        Box::pin(async move {
            let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
                .await?
                .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;

            if task.version != version {
                tracing::warn!(
                    task_id = %task.id,
                    expected_version = version,
                    actual_version = task.version,
                    current_state = %task.status,
                    target_state = %target_state,
                    actor = %actor,
                    "workflow transition rejected by version conflict"
                );
                return Err(if board_move.is_some() {
                    db::DbError::TaskVersionConflict {
                        expected: version,
                        actual: task.version,
                    }
                    .into()
                } else {
                    db::DbError::VersionConflict.into()
                });
            }

            let current_status = task.status.to_string();
            tracing::debug!(
                task_id = %task.id,
                from_state = %current_status,
                to_state = %target_state,
                actor = %actor,
                reason = %reason,
                depth = depth,
                "workflow transition requested"
            );
            let from_state = Self::find_state(workflow, &current_status).ok_or_else(|| {
                ServiceError::InvalidOperation {
                    message: Self::undefined_state_message(&current_status, workflow),
                }
            })?;
            let to_state = Self::find_state(workflow, &target_state).ok_or_else(|| {
                ServiceError::InvalidOperation {
                    message: Self::undefined_state_message(&target_state, workflow),
                }
            })?;
            let transition = workflow.trigger_between(&current_status, &target_state);
            let trigger_name = transition.map(|trigger| trigger.as_str().to_owned());
            let is_user_actor = actor.is_user();
            let none_allowance = transition.is_none()
                && (((current_status == target_state || to_state.kind == StateKind::Initial)
                    && skip_before_exit)
                    || (Self::is_cancellation_target(workflow, &target_state)
                        && from_state.kind != StateKind::Terminal));
            let strict_missing_edge = transition.is_none() && !none_allowance;
            let strict_system_only = matches!(
                transition,
                Some(trigger)
                    if !skip_before_exit
                        && Self::transition_requires_system_actor(trigger, from_state, to_state)
                        && !actor.is_system()
            );
            let mut actor = actor;
            let effective_skip_before_exit = if is_user_actor
                && from_state.kind != StateKind::Terminal
                && (strict_missing_edge || strict_system_only)
            {
                actor = actor.into_override();
                false
            } else {
                match transition {
                    Some(_trigger) => {
                        if strict_system_only {
                            tracing::warn!(
                                task_id = %task.id,
                                from_state = %current_status,
                                to_state = %target_state,
                                workflow_trigger = ?transition,
                                actor = %actor,
                                "workflow transition rejected because it is system-only"
                            );
                            return Err(ServiceError::InvalidOperation {
                                message: format!(
                                    "transition {} -> {} is system-only",
                                    current_status, target_state
                                ),
                            });
                        }
                        skip_before_exit
                    }
                    None if skip_before_exit && to_state.kind == StateKind::Initial => true,
                    None if skip_before_exit && current_status == target_state => true,
                    None if Self::is_cancellation_target(workflow, &target_state)
                        && from_state.kind != StateKind::Terminal =>
                    {
                        true
                    }
                    None => {
                        tracing::warn!(
                            task_id = %task.id,
                            from_state = %current_status,
                            to_state = %target_state,
                            from_kind = ?from_state.kind,
                            to_kind = ?to_state.kind,
                            actor = %actor,
                            reason = %reason,
                            "workflow transition rejected because no transition is defined"
                        );
                        return Err(ServiceError::Db(db::DbError::InvalidTransition));
                    }
                }
            };
            tracing::info!(
                task_id = %task.id,
                from_state = %current_status,
                to_state = %target_state,
                from_kind = ?from_state.kind,
                to_kind = ?to_state.kind,
                workflow_trigger = ?transition,
                actor = %actor,
                reason = %reason,
                rejection = rejection,
                skip_before_exit = effective_skip_before_exit,
                depth = depth,
                "workflow transition accepted"
            );

            let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
            let from_state_config =
                merged_state_config(from_state, Some(&project), task.task_state_config.as_deref());
            let to_state_config =
                merged_state_config(to_state, Some(&project), task.task_state_config.as_deref());
            let workflow_ctx = Arc::new(workflow.clone());
            let latest_execution = latest_execution_context(&self.db, &task.id).await?;
            let latest_executor = latest_executor_context(&self.db, &task.id).await?;
            let workspace_id = latest_execution
                .as_ref()
                .and_then(|execution| execution.workspace_id.clone())
                .or_else(|| {
                    latest_executor
                        .as_ref()
                        .and_then(|execution| execution.workspace_id.clone())
                });
            let execution_id = latest_executor
                .as_ref()
                .map(|execution| execution.id.clone())
                .or_else(|| {
                    latest_execution
                        .as_ref()
                        .map(|execution| execution.id.clone())
                });

            let exit_ctx = HookContext {
                task_id: task.id.clone(),
                project_id: task.project_id.clone(),
                from_state: current_status.clone(),
                to_state: target_state.clone(),
                db: Arc::clone(&self.db),
                event_bus: Arc::clone(&self.event_bus),
                gate_config: from_state.gate_config.clone(),
                workflow: Arc::clone(&workflow_ctx),
                triggered_by: actor.clone(),
                review_runner: self.review_runner.clone(),
                merge_service: self.merge_service.clone(),
                cleanup_scheduler: self.cleanup_scheduler.clone(),
                task_executor: self.task_executor.clone(),
                daemon_connections: self.daemon_connections.clone(),
                workspace_exec_locks: self.workspace_exec_locks.clone(),
                terminal_activity: self.terminal_activity.clone(),
                workspace_root: self.workspace_root.clone(),
                repo_cache_locks: self.repo_cache_locks.clone(),
                workspace_id: workspace_id.clone(),
                agent_id: latest_execution
                    .as_ref()
                    .and_then(|execution| execution.agent_id.clone()),
                execution_id: execution_id.clone(),
                state_config: from_state_config,
            };
            let enter_ctx = HookContext {
                task_id: task.id.clone(),
                project_id: task.project_id.clone(),
                from_state: current_status.clone(),
                to_state: target_state.clone(),
                db: Arc::clone(&self.db),
                event_bus: Arc::clone(&self.event_bus),
                gate_config: to_state.gate_config.clone(),
                workflow: Arc::clone(&workflow_ctx),
                triggered_by: actor.clone(),
                review_runner: self.review_runner.clone(),
                merge_service: self.merge_service.clone(),
                cleanup_scheduler: self.cleanup_scheduler.clone(),
                task_executor: self.task_executor.clone(),
                daemon_connections: self.daemon_connections.clone(),
                workspace_exec_locks: self.workspace_exec_locks.clone(),
                terminal_activity: self.terminal_activity.clone(),
                workspace_root: self.workspace_root.clone(),
                repo_cache_locks: self.repo_cache_locks.clone(),
                workspace_id,
                agent_id: latest_execution
                    .as_ref()
                    .and_then(|execution| execution.agent_id.clone()),
                execution_id,
                state_config: to_state_config,
            };

            let mut hook_results = Vec::new();
            let mut cascade: Option<(String, String)> = None;
            // Set when a failed dispatch hook rolls the task back to the
            // workflow's initial state: the rollback must not be blocked by
            // the active state's exit guards (reset_to_initial idiom).
            let mut cascade_skip_before_exit = false;
            let mut before_enter_rejection_cascade = false;
            let mut skip_target_enter_hooks = false;
            let has_blocking_before_enter = to_state
                .hooks
                .before_enter
                .iter()
                .any(|hook| matches!(hook.on_failure, FailurePolicy::Block));

            if !effective_skip_before_exit {
                for hook in &from_state.hooks.before_exit {
                    if !hook_audience_matches(hook.applies_to, &actor) {
                        log_hook_skipped_by_audience(
                            &task.id,
                            &current_status,
                            &target_state,
                            "before_exit",
                            hook,
                            &actor,
                        );
                        continue;
                    }
                    let action = registry::resolve_action(&hook.action)?;
                    log_hook_start(
                        &task.id,
                        &current_status,
                        &target_state,
                        "before_exit",
                        hook,
                        &actor,
                    );
                    let started = Instant::now();
                    let result = action.execute(&exit_ctx).await;
                    let duration_ms = elapsed_ms(started);
                    log_hook_result(
                        &task.id,
                        &current_status,
                        &target_state,
                        "before_exit",
                        hook,
                        &result,
                        duration_ms,
                    );
                    hook_results.push(hook_result_entry(
                        &hook.action,
                        "before_exit",
                        &result,
                        duration_ms,
                    ));

                    if let HookResult::Failed {
                        reason: guard_reason,
                    } = result
                    {
                        if matches!(hook.on_failure, FailurePolicy::Block) {
                            tracing::warn!(
                                task_id = %task.id,
                                from_state = %current_status,
                                to_state = %target_state,
                                guard_name = %hook.action,
                                reason = %guard_reason,
                                "workflow guard rejected transition"
                            );
                            self.event_bus.publish(ForgeEvent {
                                event_type: "transition.guard_rejected".to_string(),
                                entity_id: task.id.clone(),
                                timestamp: event_timestamp(),
                                context: EventContext::TransitionGuardRejected {
                                    task_id: task.id.clone(),
                                    from_state: current_status.clone(),
                                    to_state: target_state.clone(),
                                    guard_name: hook.action.clone(),
                                    reason: guard_reason.clone(),
                                },
                            });

                            return Err(ServiceError::GuardRejection {
                                guard: hook.action.clone(),
                                reason: guard_reason,
                            });
                        }
                    }
                }
            }

            if board_move.is_some() {
                for hook in &to_state.hooks.before_enter {
                    if !hook_audience_matches(hook.applies_to, &actor) {
                        log_hook_skipped_by_audience(
                            &task.id,
                            &current_status,
                            &target_state,
                            "before_enter",
                            hook,
                            &actor,
                        );
                        continue;
                    }
                    let action = registry::resolve_action(&hook.action)?;
                    log_hook_start(
                        &task.id,
                        &current_status,
                        &target_state,
                        "before_enter",
                        hook,
                        &actor,
                    );
                    let started = Instant::now();
                    let result = action.execute(&enter_ctx).await;
                    let duration_ms = elapsed_ms(started);
                    log_hook_result(
                        &task.id,
                        &current_status,
                        &target_state,
                        "before_enter",
                        hook,
                        &result,
                        duration_ms,
                    );
                    hook_results.push(hook_result_entry(
                        &hook.action,
                        "before_enter",
                        &result,
                        duration_ms,
                    ));
                    match result {
                        HookResult::Failed { reason: error }
                            if matches!(hook.on_failure, FailurePolicy::Block) =>
                        {
                            self.event_bus.publish(ForgeEvent {
                                event_type: "transition.guard_rejected".to_owned(),
                                entity_id: task.id.clone(),
                                timestamp: event_timestamp(),
                                context: EventContext::TransitionGuardRejected {
                                    task_id: task.id.clone(),
                                    from_state: current_status.clone(),
                                    to_state: target_state.clone(),
                                    guard_name: hook.action.clone(),
                                    reason: error.clone(),
                                },
                            });
                            return Err(ServiceError::GuardRejection {
                                guard: hook.action.clone(),
                                reason: error,
                            });
                        }
                        HookResult::Failed { reason: error } => {
                            self.event_bus.publish(ForgeEvent {
                                event_type: "transition.effect_failed".to_owned(),
                                entity_id: task.id.clone(),
                                timestamp: event_timestamp(),
                                context: EventContext::TransitionEffectFailed {
                                    task_id: task.id.clone(),
                                    from_state: current_status.clone(),
                                    to_state: target_state.clone(),
                                    action: hook.action.clone(),
                                    error,
                                },
                            });
                        }
                        HookResult::Cascade {
                            to,
                            reason: cascade_reason,
                        } => {
                            cascade = Some((to, cascade_reason));
                            break;
                        }
                        HookResult::Ok | HookResult::Skipped { .. } => {}
                    }
                }
            }

            let updated_at = now_rfc3339();
            let entry_barrier_started_at = updated_at.clone();
            let entry_barrier_json = (board_move.is_none() && has_blocking_before_enter)
                .then(|| {
                    serde_json::json!({
                        "state": target_state.as_str(),
                        "status": "running",
                        "started_at": entry_barrier_started_at.as_str(),
                    })
                    .to_string()
                });
            let reopens_visible_work = from_state.kind == StateKind::Terminal
                && to_state.kind != StateKind::Terminal
                && !task.is_automation;
            let transition_log_id = new_uuid_v4();
            let (mut task, transition_log, board_move_outcome) =
                if let Some(move_request) = board_move {
                    let persistence = TaskBoardRepo::compare_and_move_task(
                        &*self.db,
                        CompareAndMoveTask {
                            operation_id: move_request.operation_id,
                            project_id: move_request.project_id,
                            task_id: task_id.clone(),
                            task_version: version,
                            board_revision: move_request.board_revision,
                            target_status: target_state.clone(),
                            target_column_statuses: move_request.target_column_statuses,
                            before_id: move_request.before_id,
                            after_id: move_request.after_id,
                            entry_barrier_json: entry_barrier_json.clone(),
                            transition_log_id: transition_log_id.clone(),
                            trigger_name: trigger_name.clone(),
                            triggered_by: actor.display(),
                            trigger_reason: reason.clone(),
                            rejection,
                            updated_at: updated_at.clone(),
                        },
                    )
                    .await?;
                    match persistence {
                        MoveTaskPersistence::Replayed(result) => {
                            let review = latest_review(&self.db, &result.task.id).await?;
                            return Ok(TransitionResult {
                                task: result.task.clone(),
                                review,
                                cascaded: false,
                                board_move: Some(BoardMoveOutcome::Replayed(*result)),
                            });
                        }
                        MoveTaskPersistence::Committed {
                            result,
                            transition_log,
                        } => (
                            result.task.clone(),
                            *transition_log,
                            Some(BoardMoveOutcome::Committed(*result)),
                        ),
                    }
                } else {
                    let mut transaction = db::begin_immediate(self.db.pool()).await?;
                    let update = query(
                        "UPDATE task\n                 SET status = ?, version = version + 1, updated_at = ?, blocked_json = NULL, entry_barrier_json = ?\n                 WHERE id = ? AND version = ? AND deleted_at IS NULL",
                    )
                    .bind(&target_state)
                    .bind(&updated_at)
                    .bind(entry_barrier_json.as_deref())
                    .bind(&task_id)
                    .bind(version)
                    .execute(&mut *transaction)
                    .await?;

                    if update.rows_affected() != 1 {
                        return Err(db::DbError::VersionConflict.into());
                    }
                    if reopens_visible_work {
                        ProjectRepo::increment_project_work_epoch(
                            &*self.db,
                            &mut transaction,
                            &task.project_id,
                            1,
                        )
                        .await?;
                    }
                    if task.blocked_json.is_some() {
                        let mut interruption_snapshot = task.clone();
                        interruption_snapshot.status = target_state.clone();
                        interruption_snapshot.blocked_json = None;
                        interruption_snapshot.entry_barrier_json = entry_barrier_json.clone();
                        interruption_snapshot.version = version + 1;
                        interruption_snapshot.updated_at = updated_at.clone();
                        let interruption_event =
                            CreateDomainEvent::task_interruption_changed(&interruption_snapshot);
                        DomainEventRepo::append_event_in_tx(
                            &*self.db,
                            &mut transaction,
                            &interruption_event,
                        )
                        .await?;
                    }
                    let event = CreateDomainEvent::task_transition(
                        transition_log_id.clone(),
                        task_id.clone(),
                        task.project_id.clone(),
                        &current_status,
                        &target_state,
                        trigger_name.as_deref(),
                        actor.display(),
                        &reason,
                        rejection,
                        updated_at.clone(),
                    );
                    DomainEventRepo::append_event_in_tx(&*self.db, &mut transaction, &event).await?;
                    sqlx::query(
                        "INSERT INTO transition_log (
                            id, task_id, from_state, to_state, trigger_name, triggered_by,
                            trigger_reason, hook_results_json, rejection, created_at
                         ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)",
                    )
                    .bind(&transition_log_id)
                    .bind(&task.id)
                    .bind(&current_status)
                    .bind(&target_state)
                    .bind(trigger_name.as_deref())
                    .bind(actor.display())
                    .bind(&reason)
                    .bind(if rejection { 1_i64 } else { 0_i64 })
                    .bind(&updated_at)
                    .execute(&mut *transaction)
                    .await?;
                    transaction.commit().await?;
                    let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
                        .await?
                        .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
                    let transition_log = TransitionLog {
                        id: transition_log_id.clone(),
                        task_id: task.id.clone(),
                        from_state: current_status.clone(),
                        to_state: target_state.clone(),
                        trigger_name: trigger_name.clone(),
                        triggered_by: actor.display(),
                        trigger_reason: reason.clone(),
                        hook_results_json: None,
                        rejection,
                        created_at: updated_at.clone(),
                    };
                    (task, transition_log, None)
                };

            // The authoritative event was committed with the task mutation.
            // Fetching by the transition-log/event id also makes replayed board
            // moves publish at most the already-committed event.
            if let Some(event) = DomainEventRepo::get_event(&*self.db, &transition_log.id).await? {
                crate::DomainEventService::new(
                    Arc::clone(&self.db),
                    Arc::clone(&self.event_bus),
                )
                .publish_committed(&event);
            }
            let should_defer_dispatch = defer_dispatch_until.is_some()
                && to_state.kind != StateKind::Active
                && to_state
                    .hooks
                    .on_enter
                    .iter()
                    .any(|hook| hook.action == "dispatch_role_agent");
            if should_defer_dispatch {
                deferred_dispatch::set(
                    &self.db,
                    &task,
                    &target_state,
                    defer_dispatch_until
                        .as_deref()
                        .expect("deferred dispatch timestamp exists"),
                    "board drag dispatch cooldown",
                )
                .await?;
            } else if deferred_dispatch::pending_until(&task).is_some() {
                deferred_dispatch::clear(&self.db, &task).await?;
                task = TaskRepo::get_by_id(&*self.db, &task_id, false)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
            }

            tracing::info!(
                task_id = %task.id,
                from_state = %current_status,
                to_state = %target_state,
                actor = %actor,
                reason = %reason,
                transition_log_id = %transition_log.id,
                "workflow transition applied"
            );

            let memory_service = crate::MemoryService::new(Arc::clone(&self.db));
            if let Err(error) = memory_service
                .record_transition_if_failure(&task.project_id, &transition_log, None)
                .await
            {
                tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
            }

            if let Some(BoardMoveOutcome::Committed(result)) = &board_move_outcome {
                self.event_bus.publish(ForgeEvent {
                    event_type: TASK_MOVED_EVENT.to_owned(),
                    entity_id: task.id.clone(),
                    timestamp: event_timestamp(),
                    context: EventContext::TaskMoved(TaskMovedEventPayload {
                        project_id: task.project_id.clone(),
                        operation_id: result.operation_id.clone(),
                        old_status: result.old_status.clone(),
                        new_status: result.task.status.clone(),
                        old_board_position: result.old_board_position,
                        new_board_position: result.task.board_position,
                        task_version: result.task.version,
                        board_revision: result.board_revision,
                        before_id: result.before_id.clone(),
                        after_id: result.after_id.clone(),
                    }),
                });
            } else {
                self.event_bus.publish(ForgeEvent {
                    event_type: "task.status_changed".to_string(),
                    entity_id: task.id.clone(),
                    timestamp: event_timestamp(),
                    context: EventContext::TaskStatusChanged {
                        project_id: task.project_id.clone(),
                        old_status: current_status.clone(),
                        new_status: task.status.to_string(),
                    },
                });
            }

            for hook in &from_state.hooks.on_exit {
                if !hook_audience_matches(hook.applies_to, &actor) {
                    log_hook_skipped_by_audience(
                        &task.id,
                        &current_status,
                        &target_state,
                        "on_exit",
                        hook,
                        &actor,
                    );
                    continue;
                }

                let action = registry::resolve_action(&hook.action)?;
                log_hook_start(
                    &task.id,
                    &current_status,
                    &target_state,
                    "on_exit",
                    hook,
                    &actor,
                );
                let started = Instant::now();
                let result = action.execute(&exit_ctx).await;
                let duration_ms = elapsed_ms(started);
                log_hook_result(
                    &task.id,
                    &current_status,
                    &target_state,
                    "on_exit",
                    hook,
                    &result,
                    duration_ms,
                );
                hook_results.push(hook_result_entry(
                    &hook.action,
                    "on_exit",
                    &result,
                    duration_ms,
                ));

                match result {
                    HookResult::Failed { reason: error } => {
                        tracing::warn!(
                            action = %hook.action,
                            task_id = %task.id,
                            from_state = %current_status,
                            to_state = %target_state,
                            %error,
                            "workflow effect failed on_exit"
                        );
                        self.event_bus.publish(ForgeEvent {
                            event_type: "transition.effect_failed".to_string(),
                            entity_id: task.id.clone(),
                            timestamp: event_timestamp(),
                            context: EventContext::TransitionEffectFailed {
                                task_id: task.id.clone(),
                                from_state: current_status.clone(),
                                to_state: target_state.clone(),
                                action: hook.action.clone(),
                                error,
                            },
                        });
                    }
                    HookResult::Cascade {
                        to,
                        reason: cascade_reason,
                    } => {
                        cascade = Some((to, cascade_reason));
                        break;
                    }
                    HookResult::Ok | HookResult::Skipped { .. } => {}
                }
            }

            if board_move_outcome.is_none() && cascade.is_none() {
                let mut before_enter_blocked = false;
                let mut before_enter_barrier_resolved = false;
                for hook in &to_state.hooks.before_enter {
                    if !hook_audience_matches(hook.applies_to, &actor) {
                        log_hook_skipped_by_audience(
                            &task.id,
                            &current_status,
                            &target_state,
                            "before_enter",
                            hook,
                            &actor,
                        );
                        continue;
                    }
                    let action = registry::resolve_action(&hook.action)?;
                    log_hook_start(
                        &task.id,
                        &current_status,
                        &target_state,
                        "before_enter",
                        hook,
                        &actor,
                    );
                    let started = Instant::now();
                    let result = action.execute(&enter_ctx).await;
                    let duration_ms = elapsed_ms(started);
                    log_hook_result(
                        &task.id,
                        &current_status,
                        &target_state,
                        "before_enter",
                        hook,
                        &result,
                        duration_ms,
                    );
                    hook_results.push(hook_result_entry(
                        &hook.action,
                        "before_enter",
                        &result,
                        duration_ms,
                    ));

                    match result {
                        HookResult::Failed { reason: error } => {
                            tracing::warn!(
                                action = %hook.action,
                                task_id = %task.id,
                                from_state = %current_status,
                                to_state = %target_state,
                                %error,
                                "workflow effect failed before_enter"
                            );
                            self.event_bus.publish(ForgeEvent {
                                event_type: "transition.effect_failed".to_string(),
                                entity_id: task.id.clone(),
                                timestamp: event_timestamp(),
                                context: EventContext::TransitionEffectFailed {
                                    task_id: task.id.clone(),
                                    from_state: current_status.clone(),
                                    to_state: target_state.clone(),
                                    action: hook.action.clone(),
                                    error: error.clone(),
                                },
                            });

                            if matches!(hook.on_failure, FailurePolicy::Block) {
                                if target_state == crate::workflow::default_states::REVIEW {
                                    if let Some(reject_target) = to_state
                                        .gate_config
                                        .as_ref()
                                        .and_then(|config| config.reject_target.clone())
                                    {
                                        let existing_rejections =
                                            TransitionLogRepo::count_gate_rejections(
                                                &*self.db,
                                                &task_id,
                                                &target_state,
                                            )
                                            .await
                                            .unwrap_or(0);
                                        let max_rejections = to_state
                                            .gate_config
                                            .as_ref()
                                            .and_then(|gc| gc.max_rejections)
                                            .unwrap_or(i32::MAX);

                                        if existing_rejections + 1 >= i64::from(max_rejections) {
                                            let blocked_at = now_rfc3339();
                                            let barrier = serde_json::json!({
                                                "state": target_state.as_str(),
                                                "status": "blocked",
                                                "started_at": entry_barrier_started_at.as_str(),
                                                "updated_at": blocked_at.as_str(),
                                                "blocking_reason": "review retry budget exhausted",
                                            })
                                            .to_string();
                                            task = TaskRepo::set_entry_barrier(
                                                &*self.db,
                                                &task_id,
                                                task.version,
                                                Some(barrier),
                                                &blocked_at,
                                            )
                                            .await?;
                                            before_enter_blocked = true;
                                            skip_target_enter_hooks = true;
                                        } else {
                                            let clear_updated_at = now_rfc3339();
                                            task = TaskRepo::set_entry_barrier(
                                                &*self.db,
                                                &task_id,
                                                task.version,
                                                None,
                                                &clear_updated_at,
                                            )
                                            .await?;
                                            before_enter_barrier_resolved = true;
                                            before_enter_rejection_cascade = true;
                                            cascade = Some((reject_target, error));
                                        }
                                    } else {
                                        let blocked_at = now_rfc3339();
                                        let barrier = serde_json::json!({
                                            "state": target_state.as_str(),
                                            "status": "blocked",
                                            "started_at": entry_barrier_started_at.as_str(),
                                            "updated_at": blocked_at.as_str(),
                                            "blocking_reason": error.as_str(),
                                        })
                                        .to_string();
                                        task = TaskRepo::set_entry_barrier(
                                            &*self.db,
                                            &task_id,
                                            task.version,
                                            Some(barrier),
                                            &blocked_at,
                                        )
                                        .await?;
                                        before_enter_blocked = true;
                                        skip_target_enter_hooks = true;
                                    }
                                } else {
                                    let blocked_at = now_rfc3339();
                                    let barrier = serde_json::json!({
                                        "state": target_state.as_str(),
                                        "status": "blocked",
                                        "started_at": entry_barrier_started_at.as_str(),
                                        "updated_at": blocked_at.as_str(),
                                        "blocking_reason": error.as_str(),
                                    })
                                    .to_string();
                                    task = TaskRepo::set_entry_barrier(
                                        &*self.db,
                                        &task_id,
                                        task.version,
                                        Some(barrier),
                                        &blocked_at,
                                    )
                                    .await?;
                                    before_enter_blocked = true;
                                    skip_target_enter_hooks = true;
                                }
                                break;
                            }
                        }
                        HookResult::Cascade {
                            to,
                            reason: cascade_reason,
                        } => {
                            cascade = Some((to, cascade_reason));
                            break;
                        }
                        HookResult::Ok | HookResult::Skipped { .. } => {}
                    }
                }

                if has_blocking_before_enter
                    && !before_enter_blocked
                    && !before_enter_barrier_resolved
                {
                    let clear_updated_at = now_rfc3339();
                    let cleared_task = TaskRepo::set_entry_barrier(
                        &*self.db,
                        &task_id,
                        task.version,
                        None,
                        &clear_updated_at,
                    )
                    .await?;
                    task = TaskRepo::get_by_id(&*self.db, &task_id, false)
                        .await?
                        .ok_or_else(|| ServiceError::not_found("task", cleared_task.id))?;
                }
            }

            if cascade.is_none() && !skip_target_enter_hooks {
                for hook in &to_state.hooks.on_enter {
                    if !hook_audience_matches(hook.applies_to, &actor) {
                        log_hook_skipped_by_audience(
                            &task.id,
                            &current_status,
                            &target_state,
                            "on_enter",
                            hook,
                            &actor,
                        );
                        continue;
                    }
                    if should_defer_dispatch && hook.action == "dispatch_role_agent" {
                        let result = HookResult::Skipped {
                            reason: "dispatch deferred after board drag".to_owned(),
                        };
                        log_hook_result(
                            &task.id,
                            &current_status,
                            &target_state,
                            "on_enter",
                            hook,
                            &result,
                            0,
                        );
                        hook_results.push(hook_result_entry(&hook.action, "on_enter", &result, 0));
                        // The deferred dispatch is executed later by the task
                        // dispatcher, which skips tasks carrying a
                        // dispatch_failed annotation — the user's drag is an
                        // explicit restart, so drop the stale annotation now.
                        if is_dispatch_failed_annotation(task.error_annotation.as_deref()) {
                            if let Err(error) =
                                clear_dispatch_failure_annotation(&self.db, &task_id).await
                            {
                                tracing::warn!(
                                    task_id = %task.id,
                                    %error,
                                    "failed to clear dispatch failure annotation before deferred dispatch"
                                );
                            } else {
                                task = TaskRepo::get_by_id(&*self.db, &task_id, false)
                                    .await?
                                    .ok_or_else(|| {
                                        ServiceError::not_found("task", task_id.clone())
                                    })?;
                            }
                        }
                        continue;
                    }

                    let action = registry::resolve_action(&hook.action)?;
                    log_hook_start(
                        &task.id,
                        &current_status,
                        &target_state,
                        "on_enter",
                        hook,
                        &actor,
                    );
                    let started = Instant::now();
                    let result = action.execute(&enter_ctx).await;
                    let duration_ms = elapsed_ms(started);
                    log_hook_result(
                        &task.id,
                        &current_status,
                        &target_state,
                        "on_enter",
                        hook,
                        &result,
                        duration_ms,
                    );
                    hook_results.push(hook_result_entry(
                        &hook.action,
                        "on_enter",
                        &result,
                        duration_ms,
                    ));

                    match result {
                        HookResult::Failed { reason: error } => {
                            tracing::warn!(
                                action = %hook.action,
                                task_id = %task.id,
                                from_state = %current_status,
                                to_state = %target_state,
                                %error,
                                "workflow effect failed on_enter"
                            );
                            self.event_bus.publish(ForgeEvent {
                                event_type: "transition.effect_failed".to_string(),
                                entity_id: task.id.clone(),
                                timestamp: event_timestamp(),
                                context: EventContext::TransitionEffectFailed {
                                    task_id: task.id.clone(),
                                    from_state: current_status.clone(),
                                    to_state: target_state.clone(),
                                    action: hook.action.clone(),
                                    error: error.clone(),
                                },
                            });

                            // A failed dispatch entering an active state means
                            // no execution is driving the task; left alone it
                            // would sit there looking in-flight forever. Roll
                            // it back to the workflow's initial state and
                            // record the dispatch error on the task.
                            if to_state.kind == StateKind::Active
                                && registry::is_dispatch_action(&hook.action)
                            {
                                let fallback = workflow
                                    .states
                                    .iter()
                                    .find(|state| state.kind == StateKind::Initial)
                                    .map(|state| state.name.clone());
                                if let Some(fallback) = fallback {
                                    if !has_running_execution(&self.db, &task.id).await? {
                                        if let Err(annotate_error) = annotate_dispatch_failure(
                                            &self.db,
                                            &task.id,
                                            &target_state,
                                            &error,
                                        )
                                        .await
                                        {
                                            tracing::warn!(
                                                task_id = %task.id,
                                                %annotate_error,
                                                "failed to record dispatch failure annotation"
                                            );
                                        }
                                        tracing::warn!(
                                            task_id = %task.id,
                                            state = %target_state,
                                            fallback = %fallback,
                                            %error,
                                            "dispatch failed entering active state; rolling task back"
                                        );
                                        cascade = Some((
                                            fallback,
                                            format!(
                                                "dispatch failed entering {target_state}: {error}"
                                            ),
                                        ));
                                        cascade_skip_before_exit = true;
                                        break;
                                    }
                                }
                            }
                        }
                        HookResult::Cascade {
                            to,
                            reason: cascade_reason,
                        } => {
                            cascade = Some((to, cascade_reason));
                            break;
                        }
                        HookResult::Ok => {
                            // A dispatch that succeeds again invalidates any
                            // stale dispatch_failed annotation, un-parking the
                            // task for the task dispatcher.
                            if registry::is_dispatch_action(&hook.action)
                                && is_dispatch_failed_annotation(
                                    task.error_annotation.as_deref(),
                                )
                            {
                                if let Err(error) =
                                    clear_dispatch_failure_annotation(&self.db, &task_id).await
                                {
                                    tracing::warn!(
                                        task_id = %task.id,
                                        %error,
                                        "failed to clear dispatch failure annotation after successful dispatch"
                                    );
                                } else {
                                    task = TaskRepo::get_by_id(&*self.db, &task_id, false)
                                        .await?
                                        .ok_or_else(|| {
                                            ServiceError::not_found("task", task_id.clone())
                                        })?;
                                }
                            }
                        }
                        HookResult::Skipped { .. } => {}
                    }
                }
            }

            if cascade.is_none() && !skip_target_enter_hooks {
                let effective_after_enter_hooks = effective_after_enter_hooks(to_state);
                for hook in &effective_after_enter_hooks {
                    if !hook_audience_matches(hook.applies_to, &actor) {
                        log_hook_skipped_by_audience(
                            &task.id,
                            &current_status,
                            &target_state,
                            "after_enter",
                            hook,
                            &actor,
                        );
                        continue;
                    }

                    let action = registry::resolve_action(&hook.action)?;
                    log_hook_start(
                        &task.id,
                        &current_status,
                        &target_state,
                        "after_enter",
                        hook,
                        &actor,
                    );
                    let started = Instant::now();
                    let result = action.execute(&enter_ctx).await;
                    let duration_ms = elapsed_ms(started);
                    log_hook_result(
                        &task.id,
                        &current_status,
                        &target_state,
                        "after_enter",
                        hook,
                        &result,
                        duration_ms,
                    );
                    hook_results.push(hook_result_entry(
                        &hook.action,
                        "after_enter",
                        &result,
                        duration_ms,
                    ));

                    match result {
                        HookResult::Failed { reason: error } => {
                            tracing::warn!(
                                action = %hook.action,
                                task_id = %task.id,
                                from_state = %current_status,
                                to_state = %target_state,
                                %error,
                                "workflow validator failed"
                            );
                            self.event_bus.publish(ForgeEvent {
                                event_type: "transition.effect_failed".to_string(),
                                entity_id: task.id.clone(),
                                timestamp: event_timestamp(),
                                context: EventContext::TransitionEffectFailed {
                                    task_id: task.id.clone(),
                                    from_state: current_status.clone(),
                                    to_state: target_state.clone(),
                                    action: hook.action.clone(),
                                    error,
                                },
                            });
                        }
                        HookResult::Cascade {
                            to,
                            reason: cascade_reason,
                        } => {
                            cascade = Some((to, cascade_reason));
                            break;
                        }
                        HookResult::Ok | HookResult::Skipped { .. } => {}
                    }
                }
            }

            if let Ok(payload) = serde_json::to_string(&hook_results) {
                if let Err(error) =
                    TransitionLogRepo::update_hook_results(&*self.db, &transition_log.id, &payload)
                        .await
                {
                    tracing::warn!(
                        task_id = %task.id,
                        transition_log_id = %transition_log.id,
                        %error,
                        "workflow failed to persist hook results"
                    );
                } else {
                    let memory_service = crate::MemoryService::new(Arc::clone(&self.db));
                    if let Err(error) = memory_service
                        .record_transition_if_failure(
                            &task.project_id,
                            &transition_log,
                            Some(&payload),
                        )
                        .await
                    {
                        tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
                    }
                }
            }

            if let Some((cascade_to, cascade_reason)) = cascade {
                if to_state.kind == StateKind::Gate
                    && to_state
                        .gate_config
                        .as_ref()
                        .is_some_and(|gate_config| gate_config.requires_user_approval())
                    && !before_enter_rejection_cascade
                    && !to_state.gate_config.as_ref().is_some_and(|gate_config| {
                        gate_config.optional_when_unassigned()
                            && cascade_reason.starts_with("gate skipped:")
                    })
                {
                    tracing::info!(
                        task_id = %task.id,
                        state = %target_state,
                        cascade_to = %cascade_to,
                        cascade_reason = %cascade_reason,
                        "workflow cascade paused because gate requires user approval"
                    );
                    let review = latest_review(&self.db, &task.id).await?;
                    return Ok(TransitionResult {
                        task,
                        review,
                        cascaded: false,
                        board_move: board_move_outcome,
                    });
                }

                if depth >= 3 {
                    tracing::warn!(
                        task_id = %task.id,
                        state = %target_state,
                        cascade_to = %cascade_to,
                        cascade_reason = %cascade_reason,
                        depth = depth,
                        "workflow cascade depth exceeded"
                    );
                    self.event_bus.publish(ForgeEvent {
                        event_type: "transition.cascade_depth_exceeded".to_string(),
                        entity_id: task.id.clone(),
                        timestamp: event_timestamp(),
                        context: EventContext::TransitionCascadeDepthExceeded {
                            task_id: task.id.clone(),
                            state: target_state,
                            depth,
                        },
                    });
                } else {
                    task = TaskRepo::get_by_id(&*self.db, &task_id, false)
                        .await?
                        .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
                    let cascade_rejection = to_state.kind == StateKind::Gate
                        && !cascade_reason.starts_with("gate skipped:")
                        && !Self::is_terminal(workflow, &cascade_to);

                    tracing::info!(
                        task_id = %task.id,
                        from_state = %target_state,
                        cascade_to = %cascade_to,
                        cascade_reason = %cascade_reason,
                        cascade_rejection = cascade_rejection,
                        cascade_skip_before_exit = cascade_skip_before_exit,
                        depth = depth,
                        next_depth = depth + 1,
                        "workflow executing cascade transition"
                    );
                    let mut cascaded = self
                        .transition_inner(
                            task_id,
                            cascade_to,
                            task.version,
                            workflow,
                            Actor::system(api_types::SystemComponent::Workflow),
                            cascade_reason,
                            cascade_rejection,
                            cascade_skip_before_exit,
                            None,
                            None,
                            depth + 1,
                        )
                        .await?;
                    cascaded.cascaded = true;
                    cascaded.board_move = board_move_outcome;
                    return Ok(cascaded);
                }
            }

            let review = latest_review(&self.db, &task.id).await?;

            Ok(TransitionResult {
                task,
                review,
                cascaded: false,
                board_move: board_move_outcome,
            })
        }
        .instrument(span))
    }

    /// Canonical undefined-state rejection text. All transition layers must use this helper;
    /// the legacy non-enumerating `state '…' is not defined in workflow` format must not appear elsewhere.
    pub fn undefined_state_message(state_name: &str, workflow: &WorkflowDefinition) -> String {
        let defined_states = workflow
            .states
            .iter()
            .map(|state| state.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "state '{state_name}' is not defined in workflow; defined states are: {defined_states}"
        )
    }

    fn find_state<'a>(workflow: &'a WorkflowDefinition, name: &str) -> Option<&'a StateDefinition> {
        workflow.states.iter().find(|s| s.name == name)
    }

    fn is_terminal(workflow: &WorkflowDefinition, name: &str) -> bool {
        Self::find_state(workflow, name)
            .map(|state| state.kind == StateKind::Terminal)
            .unwrap_or(false)
    }

    fn is_cancellation_target(workflow: &WorkflowDefinition, target_state: &str) -> bool {
        workflow
            .cancellation_state
            .as_deref()
            .map(|state| state == target_state)
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod resolve_workflow_tests {
    use db::{new_uuid_v4, now_rfc3339};

    use super::WorkflowEngine;
    use crate::workflow::{default_states, default_workflow, inherited_subtask_workflow};

    fn task(parent_task_id: Option<String>, status: &str) -> db::Task {
        let now = now_rfc3339();
        db::Task {
            id: new_uuid_v4(),
            project_id: new_uuid_v4(),
            repo_id: None,
            parent_task_id: parent_task_id.clone(),
            subtask_order: parent_task_id.map(|_| 0),
            assignee_type: None,
            assignee_id: None,
            title: "task".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: status.to_owned(),
            is_automation: false,
            priority: 0,
            board_position: 0.0,
            task_state_config: None,
            merge_config: None,
            metadata_json: None,
            plan: None,
            blocked_json: None,
            failed_json: None,
            error_annotation: None,
            review_passed_at: None,
            entry_barrier_json: None,
            version: 1,
            created_at: now.clone(),
            updated_at: now,
            deleted_at: None,
            archived_at: None,
        }
    }

    fn project_workflow_json() -> String {
        serde_json::to_string(&default_workflow::default_workflow()).expect("workflow serializes")
    }

    fn uses_project_workflow(resolved: &api_types::WorkflowDefinition) -> bool {
        resolved
            .states
            .iter()
            .any(|state| state.name == default_states::REVIEW)
    }

    fn uses_subtask_workflow(resolved: &api_types::WorkflowDefinition) -> bool {
        !uses_project_workflow(resolved)
    }

    #[test]
    fn root_task_always_uses_project_workflow() {
        let wf_json = project_workflow_json();
        for (status, actor) in [
            (
                default_states::TODO,
                api_types::Actor::user(api_types::UserActionSource::Board),
            ),
            (
                default_states::IN_PROGRESS,
                api_types::Actor::system(api_types::SystemComponent::General),
            ),
            (default_states::REVIEW, api_types::Actor::agent("abc")),
        ] {
            let resolved =
                WorkflowEngine::resolve_workflow_for_task(&task(None, status), &wf_json, &actor);
            assert!(
                uses_project_workflow(&resolved),
                "root task in {status} with actor {actor} must use project workflow"
            );
        }
    }

    #[test]
    fn subtask_in_shared_state_uses_subtask_workflow_for_non_user_actors() {
        let wf_json = project_workflow_json();
        for actor in [
            api_types::Actor::system(api_types::SystemComponent::General),
            api_types::Actor::agent("runner"),
            api_types::Actor::system(api_types::SystemComponent::Dispatch),
        ] {
            let resolved = WorkflowEngine::resolve_workflow_for_task(
                &task(Some(new_uuid_v4()), default_states::IN_PROGRESS),
                &wf_json,
                &actor,
            );
            assert!(
                uses_subtask_workflow(&resolved),
                "subtask in_progress with actor {actor} must use inherited subtask workflow"
            );
            assert_eq!(
                resolved.states.len(),
                inherited_subtask_workflow().states.len()
            );
        }
    }

    #[test]
    fn subtask_in_shared_state_uses_project_workflow_for_user_actors() {
        let wf_json = project_workflow_json();
        for actor in [
            api_types::Actor::user(api_types::UserActionSource::Board),
            api_types::Actor::user(api_types::UserActionSource::Override(Box::new(
                api_types::UserActionSource::Api,
            ))),
            api_types::Actor::user(api_types::UserActionSource::Test),
        ] {
            let resolved = WorkflowEngine::resolve_workflow_for_task(
                &task(Some(new_uuid_v4()), default_states::TODO),
                &wf_json,
                &actor,
            );
            assert!(
                uses_project_workflow(&resolved),
                "subtask in shared state with actor {actor} must use project workflow"
            );
        }
    }

    #[test]
    fn subtask_in_project_only_state_uses_project_workflow_for_all_actors() {
        let wf_json = project_workflow_json();
        for actor in [
            api_types::Actor::user(api_types::UserActionSource::Board),
            api_types::Actor::system(api_types::SystemComponent::General),
            api_types::Actor::agent("runner"),
        ] {
            let resolved = WorkflowEngine::resolve_workflow_for_task(
                &task(Some(new_uuid_v4()), default_states::REVIEW),
                &wf_json,
                &actor,
            );
            assert!(
                uses_project_workflow(&resolved),
                "subtask in review with actor {actor} must use project workflow"
            );
        }
    }

    #[test]
    fn undefined_state_message_enumerates_defined_states() {
        let workflow = default_workflow::default_workflow();
        let message = WorkflowEngine::undefined_state_message("bogus", &workflow);
        assert!(message.contains("bogus"));
        assert!(message.contains("; defined states are:"));
        for state in &workflow.states {
            assert!(message.contains(state.name.as_str()));
        }
    }

    #[test]
    fn legacy_undefined_state_message_format_absent_from_services_src() {
        let services_src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut offenders = Vec::new();
        scan_rs_sources(&services_src, &services_src, &mut offenders);
        assert!(
            offenders.is_empty(),
            "legacy undefined-state message format found:\n{}",
            offenders.join("\n")
        );
    }

    fn scan_rs_sources(root: &std::path::Path, dir: &std::path::Path, offenders: &mut Vec<String>) {
        let entries = std::fs::read_dir(dir).expect("directory readable");
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                scan_rs_sources(root, &path, offenders);
                continue;
            }
            if path.extension().is_none_or(|ext| ext != "rs") {
                continue;
            }
            if path.file_name().is_some_and(|name| name == "mod.rs")
                && path
                    .parent()
                    .is_some_and(|parent| parent.ends_with("engine"))
            {
                continue;
            }
            let contents = std::fs::read_to_string(&path).expect("source file readable");
            for (line_number, line) in contents.lines().enumerate() {
                if line.contains("is not defined in workflow")
                    && !line.contains("; defined states are:")
                {
                    offenders.push(format!(
                        "{}:{}: {}",
                        path.strip_prefix(root).unwrap_or(&path).display(),
                        line_number + 1,
                        line.trim()
                    ));
                }
            }
        }
    }
}
