use std::{future::Future, path::PathBuf, pin::Pin, sync::Arc, time::Instant};

use api_types::{
    Actor, FailurePolicy, StateDefinition, StateKind, TaskMovedEventPayload, WorkflowDefinition,
    WorkflowTrigger,
};
use db::{
    new_uuid_v4, now_rfc3339, CompareAndMoveTask, CreateDomainEvent, DomainEventRepo,
    MoveTaskPersistence, MoveTaskResult, ProjectRepo, TaskBoardRepo, TaskRepo, TaskStepRepo,
    TransitionLog, TransitionLogRepo, UpdateTask,
};
use events::{event_timestamp, EventBus, EventContext, ForgeEvent, TASK_MOVED_EVENT};
use sqlx::{query, Row};
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
    merge_service::MergeService,
    terminal_service::TerminalActivityTracker,
    workflow::{default_workflow, inherited_subtask_workflow, registry, HookContext, HookResult},
    workspace_cleanup::WorkspaceCleanupScheduler,
    workspace_execution_lock::WorkspaceExecutionLockManager,
    ServiceError,
};

mod context;
pub(crate) mod durable;
mod hooks;
#[cfg(test)]
mod tests;

/// `error_annotation` type recorded when a dispatch hook fails entering an
/// active state. The task dispatcher treats it as blocking, so a task whose
/// dispatch deterministically fails (e.g. governance says it is not runnable)
/// is parked instead of rescheduled in a loop.
pub(crate) const DISPATCH_FAILED_ANNOTATION: &str = "dispatch_failed";

struct ReviewEntryFailure {
    task: db::Task,
    cascade: Option<(String, String)>,
}

fn dispatch_failed_annotation_json(state: &str, message: &str) -> String {
    serde_json::json!({
        "type": api_types::FailureKind::DispatchFailed,
        "message": message,
        "state": state,
        "detected_at": now_rfc3339(),
    })
    .to_string()
}

/// The checks `run_ci_steps` will execute when this Task next enters review,
/// resolved from the same merged review-state config the hook reads. Empty
/// when the workflow has no review state or the config names no steps.
pub(crate) fn review_ci_steps_for_task(
    workflow: &api_types::WorkflowDefinition,
    project: Option<&db::Project>,
    task_state_config_json: Option<&str>,
) -> Vec<String> {
    workflow
        .states
        .iter()
        .find(|state| state.name == crate::workflow::default_states::REVIEW)
        .map(|state| hooks::merged_state_config(state, project, task_state_config_json))
        .and_then(|config| crate::workflow::actions::review_ci_steps(&config).ok())
        .unwrap_or_default()
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

fn preserve_dispatch_annotation(raw_annotation: Option<&str>) -> bool {
    raw_annotation
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        .and_then(|value| value["type"].as_str().map(str::to_owned))
        .is_some_and(|kind| {
            kind != DISPATCH_FAILED_ANNOTATION
                && crate::task_dispatcher::is_blocking_annotation_type(&kind)
        })
}

/// Persist a `dispatch_failed` error annotation through the Task writer.
/// Shared by the engine's dispatch-failure fallback and the task
/// dispatcher's governance parking.
pub(crate) async fn annotate_dispatch_failure(
    db: &db::SqliteDb,
    task_id: &str,
    state: &str,
    message: &str,
    authority: Option<&WorkflowAuthority>,
) -> db::Result<()> {
    annotate_dispatch_failure_details(db, task_id, state, message, authority, None).await
}

pub(crate) async fn annotate_upgrade_dispatch_refusal(
    db: &db::SqliteDb,
    task_id: &str,
    state: &str,
    error: &crate::ServiceError,
) -> db::Result<()> {
    let daemon_ids: Vec<&str> = match error {
        crate::ServiceError::PlacementUnavailable(refusal) if refusal.needs_daemon_upgrade() => {
            refusal.upgrade_daemon_ids().collect()
        }
        crate::ServiceError::DaemonUpgradeRequired { daemon_id } => vec![daemon_id],
        _ => return Ok(()),
    };
    let details =
        serde_json::json!({"code": api_types::DAEMON_UPGRADE_REQUIRED, "daemon_ids": daemon_ids});
    let task = TaskRepo::get_by_id(db, task_id, false)
        .await?
        .ok_or(db::DbError::NotFound)?;
    let expected_annotation = if preserve_dispatch_annotation(task.error_annotation.as_deref()) {
        serde_json::from_str::<serde_json::Value>(task.error_annotation.as_deref().unwrap())
            .expect("blocking annotation JSON")
    } else {
        let mut annotation: serde_json::Value =
            serde_json::from_str(&dispatch_failed_annotation_json(state, &error.to_string()))
                .expect("annotation JSON");
        annotation
            .as_object_mut()
            .unwrap()
            .extend(details.as_object().unwrap().clone());
        // The annotation writer creates its own timestamp. Fence the refusal's
        // content, without comparing separately generated detection times.
        annotation.as_object_mut().unwrap().remove("detected_at");
        annotation
    };
    let metadata: serde_json::Value = task
        .metadata_json
        .as_deref()
        .and_then(|raw| serde_json::from_str(raw).ok())
        .unwrap_or_default();
    let mut marker = details.clone();
    marker["error_annotation"] = expected_annotation;
    marker["dispatch_disposition"] = metadata["dispatch_disposition"].clone();
    marker["deferred_dispatch"] = metadata["deferred_dispatch"].clone();
    TaskRepo::mutate_metadata(
        db,
        task_id,
        Some(task.version),
        vec![
            db::TaskMetadataMutation::Set {
                key: "daemon_upgrade_refusal".into(),
                value: marker,
            },
            db::TaskMetadataMutation::Remove {
                key: "owner_wait".into(),
            },
        ],
        &now_rfc3339(),
    )
    .await?;
    annotate_dispatch_failure_details(db, task_id, state, &error.to_string(), None, Some(details))
        .await
}

async fn annotate_dispatch_failure_details(
    db: &db::SqliteDb,
    task_id: &str,
    state: &str,
    message: &str,
    authority: Option<&WorkflowAuthority>,
    details: Option<serde_json::Value>,
) -> db::Result<()> {
    let mut annotation: serde_json::Value =
        serde_json::from_str(&dispatch_failed_annotation_json(state, message))
            .expect("annotation JSON");
    if let Some(details) = &details {
        annotation
            .as_object_mut()
            .unwrap()
            .extend(details.as_object().unwrap().clone());
    }
    let annotation = annotation.to_string();
    let current = TaskRepo::get_by_id(db, task_id, false)
        .await?
        .ok_or(db::DbError::NotFound)?;
    {
        if let Some(raw) = current.error_annotation.as_deref() {
            if preserve_dispatch_annotation(Some(raw)) {
                return Ok(());
            }
            // The hook retained typed refusal details before the engine's
            // string-only fallback. Preserve them for reconnection recovery.
            if details.is_none()
                && serde_json::from_str::<serde_json::Value>(raw)
                    .ok()
                    .is_some_and(|value| {
                        (value["message"] == message
                            && value["code"] == api_types::DAEMON_UPGRADE_REQUIRED)
                            || (value["state"] == state && value["code"] == "placement_unavailable")
                    })
            {
                return Ok(());
            }
        }
        let update = UpdateTask {
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
        };
        if !db::task_writer::owns_task(task_id) {
            return db
                .run_task_mutation(
                    task_id,
                    db::TaskMutation::TaskUpdateIfAnnotation {
                        input: update,
                        expected_annotation: current.error_annotation.clone(),
                        expected_project_version: authority.map(|a| a.project_version),
                        expected_workflow_definition: authority
                            .map(|a| a.workflow_definition.clone()),
                    },
                )
                .await;
        }
        let result = match authority {
            Some(authority) => {
                TaskRepo::update_with_workflow_authority(
                    db,
                    update,
                    authority.project_version,
                    authority.workflow_definition.clone(),
                )
                .await
            }
            None => TaskRepo::update(db, update).await,
        };
        match result {
            Ok(_) => Ok(()),
            Err(error) => Err(error),
        }
    }
}

/// Clear a `dispatch_failed` error annotation (and only that annotation type)
/// once a dispatch succeeds again, so the task dispatcher resumes scheduling
/// and recovery for the task.
pub(crate) async fn clear_dispatch_failure_annotation(
    db: &db::SqliteDb,
    task_id: &str,
    authority: Option<&WorkflowAuthority>,
) -> db::Result<()> {
    clear_dispatch_failure_matching(db, task_id, authority, |_| true).await
}

async fn clear_dispatch_failure_matching(
    db: &db::SqliteDb,
    task_id: &str,
    authority: Option<&WorkflowAuthority>,
    matches: impl Fn(&serde_json::Value) -> bool,
) -> db::Result<()> {
    let current = TaskRepo::get_by_id(db, task_id, false)
        .await?
        .ok_or(db::DbError::NotFound)?;
    {
        if !is_dispatch_failed_annotation(current.error_annotation.as_deref()) {
            return Ok(());
        }
        let annotation: serde_json::Value =
            serde_json::from_str(current.error_annotation.as_deref().unwrap())
                .expect("dispatch annotation JSON");
        if !matches(&annotation) {
            return Ok(());
        }
        let update = UpdateTask {
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
        };
        if !db::task_writer::owns_task(task_id) {
            return db
                .run_task_mutation(
                    task_id,
                    db::TaskMutation::TaskUpdateIfAnnotation {
                        input: update,
                        expected_annotation: current.error_annotation.clone(),
                        expected_project_version: authority.map(|a| a.project_version),
                        expected_workflow_definition: authority
                            .map(|a| a.workflow_definition.clone()),
                    },
                )
                .await;
        }
        let result = match authority {
            Some(authority) => {
                TaskRepo::update_with_workflow_authority(
                    db,
                    update,
                    authority.project_version,
                    authority.workflow_definition.clone(),
                )
                .await
            }
            None => TaskRepo::update(db, update).await,
        };
        match result {
            Ok(_) => Ok(()),
            Err(error) => Err(error),
        }
    }
}

pub(crate) async fn wake_upgraded_daemon_tasks(
    db: &db::SqliteDb,
    daemon_ids: &[String],
) -> crate::Result<()> {
    let task_ids = sqlx::query_scalar::<_, String>(
        "SELECT id FROM task WHERE deleted_at IS NULL AND CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.daemon_upgrade_refusal.code') END = ?"
    ).bind(api_types::DAEMON_UPGRADE_REQUIRED).fetch_all(db.pool()).await?;
    for task_id in task_ids {
        let matches = |marker: &serde_json::Value| {
            marker["code"] == api_types::DAEMON_UPGRADE_REQUIRED
                && marker["daemon_ids"].as_array().is_some_and(|ids| {
                    ids.iter()
                        .any(|id| daemon_ids.iter().any(|daemon| id == daemon))
                })
        };
        let Some(task) = TaskRepo::get_by_id(db, &task_id, false).await? else {
            continue;
        };
        let Some(metadata) = task
            .metadata_json
            .as_deref()
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        else {
            continue;
        };
        if !matches(&metadata["daemon_upgrade_refusal"]) {
            continue;
        }
        clear_upgrade_dispatch_refusal(db, &task).await?;
    }
    Ok(())
}

async fn clear_upgrade_dispatch_refusal(db: &db::SqliteDb, task: &db::Task) -> crate::Result<bool> {
    // Clearing and waking are one CAS. A manual action that changes the row
    // or metadata after the scan must keep its newly recorded deferral.
    let metadata: serde_json::Value = task
        .metadata_json
        .as_deref()
        .and_then(|raw| serde_json::from_str(raw).ok())
        .unwrap_or_default();
    let marker = &metadata["daemon_upgrade_refusal"];
    let mut annotation = task
        .error_annotation
        .as_deref()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        .unwrap_or_default();
    if annotation["type"] == DISPATCH_FAILED_ANNOTATION {
        annotation.as_object_mut().unwrap().remove("detected_at");
    }
    if marker["code"] != api_types::DAEMON_UPGRADE_REQUIRED
        || marker["error_annotation"] != annotation
        || marker["dispatch_disposition"] != metadata["dispatch_disposition"]
        || marker["deferred_dispatch"] != metadata["deferred_dispatch"]
    {
        return Ok(false);
    }
    let clear_annotation = annotation["type"] == DISPATCH_FAILED_ANNOTATION
        && annotation["code"] == api_types::DAEMON_UPGRADE_REQUIRED;
    let result = db::task_writer::TaskQuery::new(db,&task.id,
        "UPDATE task SET
            error_annotation = CASE WHEN ? THEN NULL ELSE error_annotation END,
            metadata_json = NULLIF(json_remove(metadata_json, '$.daemon_upgrade_refusal', '$.dispatch_disposition', '$.deferred_dispatch'), '{}'),
            version = version + 1, updated_at = ?
         WHERE id = ? AND deleted_at IS NULL AND version = ?
            AND metadata_json IS ? AND error_annotation IS ?
            AND json_extract(metadata_json, '$.daemon_upgrade_refusal.code') = ?"
    )
    .bind(clear_annotation)
    .bind(now_rfc3339())
    .bind(&task.id)
    .bind(task.version)
    .bind(&task.metadata_json)
    .bind(&task.error_annotation)
    .bind(api_types::DAEMON_UPGRADE_REQUIRED)
    .execute(db.pool()).await?;
    let changed = result != 0;
    if changed {
        tracing::info!(task_id = %task.id, "task dispatch woken after daemon upgrade");
    }
    Ok(changed)
}

#[derive(Clone)]
pub struct WorkflowEngine {
    pub db: Arc<db::SqliteDb>,
    pub event_bus: Arc<EventBus>,
    pub review_runner: Option<Arc<review::ReviewRunner>>,
    pub merge_service: Option<Arc<MergeService>>,
    pub cleanup_scheduler: Option<Arc<WorkspaceCleanupScheduler>>,
    /// Single authority for execution dispatch dependencies. A separate
    /// executor on the engine could let hook dispatch observe a stale or
    /// differently configured service clone.
    pub task_service: crate::TaskService,
    pub daemon_connections: Option<Arc<crate::daemon_transport::DaemonConnectionRegistry>>,
    pub workspace_exec_locks: Option<Arc<WorkspaceExecutionLockManager>>,
    pub terminal_activity: Option<Arc<TerminalActivityTracker>>,
    pub workspace_root: PathBuf,
    pub repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
    pub workspace_backend_router: Arc<crate::workspace_backend::WorkspaceBackendRouter>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct TransitionResult {
    pub task: db::Task,
    pub review: Option<db::Review>,
    pub queued_step_id: Option<String>,
    pub pending_steps: i64,
    pub board_move: Option<BoardMoveOutcome>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BoardMoveRequest {
    pub operation_id: String,
    pub project_id: String,
    pub board_revision: i64,
    pub target_column_statuses: Vec<String>,
    pub before_id: Option<String>,
    pub after_id: Option<String>,
}

/// Project workflow authority captured alongside the Task snapshot that a
/// transition used. Both values are checked while the Task mutation holds
/// SQLite's writer transaction, so a stale workflow cannot commit.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WorkflowAuthority {
    pub project_version: i64,
    pub workflow_definition: String,
    /// Clear stale review authority in the same transaction as this status
    /// change. Plan publication uses this when a completed implementation
    /// enters review; a separate pre-transition write would survive if the
    /// project workflow changed before the status CAS.
    pub clear_review_passed_at_on_commit: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum BoardMoveOutcome {
    Committed(MoveTaskResult),
    Replayed(MoveTaskResult),
}

impl WorkflowEngine {
    /// Blocking hooks may settle review authority by advancing the Task
    /// version. Refresh the local transition snapshot after every hook so the
    /// subsequent barrier/CAS uses the version that the hook actually left in
    /// the database. A hook must not silently change this Task's state while
    /// the transition is in flight.
    async fn refresh_task_after_hook(
        &self,
        task: &mut db::Task,
        _expected_status: &str,
        _step: Option<&db::TaskStep>,
    ) -> crate::Result<()> {
        let latest = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
        *task = latest;
        Ok(())
    }

    async fn set_entry_barrier_with_authority(
        &self,
        task_id: &str,
        expected_version: i64,
        entry_barrier_json: Option<String>,
        updated_at: &str,
        authority: Option<&WorkflowAuthority>,
    ) -> crate::Result<db::Task> {
        match authority {
            Some(authority) => Ok(TaskRepo::set_entry_barrier_with_workflow_authority(
                &*self.db,
                task_id,
                expected_version,
                entry_barrier_json,
                updated_at,
                authority.project_version,
                authority.workflow_definition.clone(),
            )
            .await?),
            None => Ok(TaskRepo::set_entry_barrier(
                &*self.db,
                task_id,
                expected_version,
                entry_barrier_json,
                updated_at,
            )
            .await?),
        }
    }

    /// CI has already finalized the Review and invalidated acceptance. Settle
    /// that verdict even when this hook uses Log rather than Block, and use
    /// the same remediation budget as reviewer completion and explicit reruns.
    async fn settle_failed_ci_entry(
        &self,
        task: &db::Task,
        action: &str,
        actor: &Actor,
        entry_started_at: &str,
        authority: Option<&WorkflowAuthority>,
    ) -> crate::Result<Option<ReviewEntryFailure>> {
        if action != "run_ci_steps" || actor.is_user() {
            return Ok(None);
        }
        // A typed owner/preflight refusal was settled atomically by the hook.
        // It may have occurred before any Review row was created.
        let interrupted = task
            .entry_barrier_json
            .as_deref()
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
            .is_some_and(|barrier| {
                barrier["status"] == "blocked"
                    && barrier["interrupted_at"]
                        .as_str()
                        .is_some_and(|at| at >= entry_started_at)
            });
        if interrupted {
            return Ok(Some(ReviewEntryFailure {
                task: task.clone(),
                cascade: None,
            }));
        }
        let Some(review) = latest_review(&self.db, &task.id).await? else {
            return Ok(None);
        };
        // Authority-loss cancellations retain the base routing.
        if review.status != db::ReviewStatus::Failed
            || review.started_at.as_str() < entry_started_at
        {
            return Ok(None);
        }
        let task = self
            .set_entry_barrier_with_authority(
                &task.id,
                task.version,
                None,
                &now_rfc3339(),
                authority,
            )
            .await?;
        let (task, target, reason) = self
            .task_service
            .review_failure_target(&task, Some(&review.execution_id))
            .await?;
        Ok(Some(ReviewEntryFailure {
            task,
            cascade: target.map(|target| (target, reason)),
        }))
    }

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
        self.transition_with_deferred_dispatch_and_authority(
            task_id,
            target_state,
            version,
            workflow,
            actor,
            reason,
            rejection,
            defer_dispatch_until,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn transition_with_deferred_dispatch_and_authority(
        &self,
        task_id: &str,
        target_state: &str,
        version: i64,
        workflow: &WorkflowDefinition,
        actor: &Actor,
        reason: &str,
        rejection: bool,
        defer_dispatch_until: Option<String>,
        authority: Option<WorkflowAuthority>,
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
            None,
            authority,
            false,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn transition_with_authority(
        &self,
        task_id: &str,
        target_state: &str,
        version: i64,
        workflow: &WorkflowDefinition,
        actor: &Actor,
        reason: &str,
        rejection: bool,
        authority: WorkflowAuthority,
    ) -> crate::Result<TransitionResult> {
        self.transition_with_deferred_dispatch_and_authority(
            task_id,
            target_state,
            version,
            workflow,
            actor,
            reason,
            rejection,
            None,
            Some(authority),
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
        self.move_task_with_authority(
            task_id,
            target_state,
            version,
            workflow,
            actor,
            reason,
            move_request,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn move_task_with_authority(
        &self,
        task_id: &str,
        target_state: &str,
        version: i64,
        workflow: &WorkflowDefinition,
        actor: &Actor,
        reason: &str,
        move_request: BoardMoveRequest,
        authority: Option<WorkflowAuthority>,
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
            None,
            authority,
            false,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn manual_override_transition_with_authority(
        &self,
        task_id: &str,
        target_state: &str,
        version: i64,
        workflow: &WorkflowDefinition,
        actor: Actor,
        reason: &str,
        rejection: bool,
        authority: Option<WorkflowAuthority>,
    ) -> crate::Result<TransitionResult> {
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        crate::task_service::execution::ensure_plan_publication_transition_authority(&task, None)?;
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
            None,
            authority,
            false,
        )
        .await
    }

    pub async fn retry_entry_barrier_with_authority(
        &self,
        task_id: &str,
        version: i64,
        workflow: &WorkflowDefinition,
        actor: &Actor,
        reason: &str,
        authority: WorkflowAuthority,
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
        self.transition_inner(
            task_id.to_owned(),
            target_state,
            version,
            workflow,
            actor.clone(),
            reason.to_owned(),
            false,
            true,
            None,
            None,
            None,
            Some(authority),
            true,
        )
        .await
    }

    #[tracing::instrument(
        skip(self, workflow),
        fields(task_id = %task_id, target_state = %target_state, version = version, actor = %actor, reason = %reason)
    )]
    pub async fn restart(
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
                None,
                None,
                false,
            )
            .await?;
        Ok(result.task)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn restart_with_authority(
        &self,
        task_id: &str,
        target_state: &str,
        version: i64,
        workflow: &WorkflowDefinition,
        actor: &Actor,
        reason: &str,
        authority: WorkflowAuthority,
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
                None,
                Some(authority),
                false,
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
    pub(crate) fn transition_inner<'a>(
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
        step: Option<db::TaskStep>,
        authority: Option<WorkflowAuthority>,
        entry_retry: bool,
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
            step_id = ?step.as_ref().map(|step| &step.id),
        );

        Box::pin(async move {
            if !db::task_writer::owns_task(&task_id) {
                let preempt = Self::is_cancellation_target(workflow, &target_state);
                let command=crate::task_service::commands::TaskCommand {
                    operation:"engine_transition".to_owned(),preempt,
                    arguments:serde_json::json!({"task_id":task_id,"target_state":target_state,"version":version,"workflow":workflow,"actor":actor,"reason":reason,"rejection":rejection,"skip_before_exit":skip_before_exit,"defer_dispatch_until":defer_dispatch_until,"board_move":board_move,"authority":authority,"entry_retry":entry_retry}),
                };
                if db::task_writer::current_task_step().is_some_and(|step|step.task_id!=task_id) && reason=="root subtask cascade" {
                    self.task_service.enqueue_task_command(&task_id,&command.operation,command.arguments,preempt).await?;
                    let task=TaskRepo::get_by_id(&*self.db,&task_id,false).await?.ok_or(db::DbError::NotFound)?;
                    return Ok(TransitionResult {task,review:None,queued_step_id:None,pending_steps:self.db.pending_steps(&task_id).await?,board_move:None});
                }
                return Arc::new(crate::worker_runtime::queue::TaskStepWorker::new(self.clone()))
                    .request_command(&task_id,command).await;
            }
            let mut task = TaskRepo::get_by_id(&*self.db, &task_id, false)
                .await?
                .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;

            if step.is_none() && !db::task_writer::owns_task(&task_id) && task.version != version {
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

            let version = task.version;
            let current_status = task.status.to_string();
            tracing::debug!(
                task_id = %task.id,
                from_state = %current_status,
                to_state = %target_state,
                actor = %actor,
                reason = %reason,
                step_id = ?step.as_ref().map(|step| &step.id),
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
            let is_agent_cancellation = actor.is_agent()
                && Self::is_cancellation_target(workflow, &target_state)
                && from_state.kind != StateKind::Terminal;
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
                        && !is_agent_cancellation
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
                step_id = ?step.as_ref().map(|step| &step.id),
                "workflow transition accepted"
            );

            let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
            if let Some(authority) = authority.as_ref() {
                if project.version != authority.project_version
                    || project.workflow_definition != authority.workflow_definition
                {
                    return Err(db::DbError::VersionConflict.into());
                }
            }
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
                project_version: authority
                    .as_ref()
                    .map(|authority| authority.project_version),
                project_workflow_definition: authority
                    .as_ref()
                    .map(|authority| authority.workflow_definition.clone()),
                triggered_by: actor.clone(),
                review_runner: self.review_runner.clone(),
                merge_service: self.merge_service.clone(),
                cleanup_scheduler: self.cleanup_scheduler.clone(),
                task_service: self.task_service.clone(),
                daemon_connections: self.daemon_connections.clone(),
                workspace_exec_locks: self.workspace_exec_locks.clone(),
                terminal_activity: self.terminal_activity.clone(),
                workspace_root: self.workspace_root.clone(),
                repo_cache_locks: self.repo_cache_locks.clone(),
                workspace_backend_router: Arc::clone(&self.workspace_backend_router),
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
                project_version: authority
                    .as_ref()
                    .map(|authority| authority.project_version),
                project_workflow_definition: authority
                    .as_ref()
                    .map(|authority| authority.workflow_definition.clone()),
                triggered_by: actor.clone(),
                review_runner: self.review_runner.clone(),
                merge_service: self.merge_service.clone(),
                cleanup_scheduler: self.cleanup_scheduler.clone(),
                task_service: self.task_service.clone(),
                daemon_connections: self.daemon_connections.clone(),
                workspace_exec_locks: self.workspace_exec_locks.clone(),
                terminal_activity: self.terminal_activity.clone(),
                workspace_root: self.workspace_root.clone(),
                repo_cache_locks: self.repo_cache_locks.clone(),
                workspace_backend_router: Arc::clone(&self.workspace_backend_router),
                workspace_id,
                agent_id: latest_execution
                    .as_ref()
                    .and_then(|execution| execution.agent_id.clone()),
                execution_id,
                state_config: to_state_config,
            };

            let mut hook_results = Vec::new();
            // `merge_failed` is only an edge-compatible bridge for mechanical
            // contention refreshes. It must run its on-enter dispatcher so the
            // task can cascade to review, but it must not run repair-state
            // entry hooks or create a retryable entry barrier along the way.
            let review_refresh_bridge = current_status == crate::workflow::default_states::MERGING
                && target_state == crate::workflow::default_states::MERGE_FAILED
                && reason.contains(crate::workflow::REVIEW_REFRESH_MARKER)
                && matches!(
                    &actor,
                    Actor::System {
                        component: api_types::SystemComponent::Workflow
                    }
                );
            // A review-refresh bridge is bookkeeping for a fresh review, not a
            // rejected merge attempt. Normalize the persisted transition even
            // when a caller supplied `rejection = true`; otherwise the bridge
            // itself spends merge-fix budget before the real conflict is seen.
            let conflict_handoff_bridge = current_status == crate::workflow::default_states::MERGING
                && target_state == crate::workflow::default_states::MERGE_FAILED
                && reason.contains(crate::workflow::CONFLICT_HANDOFF_MARKER)
                && matches!(
                    &actor,
                    Actor::System {
                        component: api_types::SystemComponent::Workflow
                    }
                );
            let rejection = rejection && !review_refresh_bridge && !conflict_handoff_bridge;
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
            self.refresh_task_after_hook(&mut task, &current_status, None)
                .await?;
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

            let updated_at = now_rfc3339();
            let entry_barrier_json = entry_retry.then(|| task.entry_barrier_json.clone()).flatten();
            let cleanup_guard = if from_state.kind == StateKind::Terminal
                && to_state.kind != StateKind::Terminal
            {
                if let Some(scheduler) = self.cleanup_scheduler.as_ref() {
                    Some(scheduler.lock_task(&task).await)
                } else {
                    None
                }
            } else {
                None
            };
            let reopens_visible_work = from_state.kind == StateKind::Terminal
                && to_state.kind != StateKind::Terminal
                && !task.is_automation;
            let transition_log_id = step.as_ref().map(|s| s.id.clone()).unwrap_or_else(new_uuid_v4);
            let workflow_snapshot = crate::workflow::transition_event::transition_workflow_snapshot(
                &task,
                workflow,
                &current_status,
                &target_state,
            )?;
            let mut producing = task.clone();
            producing.status = target_state.clone();
            producing.version = version + 1;
            let input = self.cascade_step_input(&producing, workflow, target_state.clone(), reason.clone(), false, false, authority.clone(), step.as_ref(), transition_log_id.clone(), None).await?;
            let frozen = self.db.store_step_workflow(&serde_json::to_string(&durable::HookDefinition { workflow: workflow.clone(), project_workflow_definition: authority.as_ref().map(|a| a.workflow_definition.clone()) }).map_err(|e| ServiceError::invalid_operation(e.to_string()))?).await?;
            let cascade_payload: crate::worker_runtime::queue::CascadePayload = serde_json::from_str(&input.payload_json).map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
            let mut dispatch_index = to_state.hooks.on_enter.iter().position(|h| registry::is_dispatch_action(&h.action) && hook_audience_matches(h.applies_to, &actor))
                .map(|i| (from_state.hooks.on_exit.len() + to_state.hooks.before_enter.len() + i) as i64);
            let mut admission_agent_id=dispatch_index.and(cascade_payload.admission_agent_id);
            let role_unassigned = if let Some(role)=crate::workflow::effective_role(to_state) { crate::task_hierarchy::effective_role_assignment(&self.db,&producing,role).await?.is_none_or(|r| r.assignment.assignee_id.is_none()) } else { false };
            let action_dispatch = crate::TaskService::task_action_command_active() && !entry_retry;
            let should_defer_dispatch=(defer_dispatch_until.is_some() || action_dispatch) && (to_state.kind!=StateKind::Active || action_dispatch) && dispatch_index.is_some();
            let deferred_marker=should_defer_dispatch.then(||serde_json::json!({"target_state":target_state,"not_before":defer_dispatch_until.clone().unwrap_or_else(now_rfc3339),"reason":if crate::TaskService::task_action_command_active(){"task action dispatch"}else{"board drag dispatch cooldown"}}).to_string());
            // Optional, unassigned planning is an imminent fast role entry:
            // reserve its coder slot while the hook step discovers the skip.
            if admission_agent_id.is_none() && role_unassigned && dispatch_index.is_some() && from_state.hooks.on_exit.is_empty()
                && durable::hooks_lane(workflow,&current_status,&target_state)=="fast"
                && to_state.gate_config.as_ref().is_some_and(|g|g.optional_when_unassigned() && !g.requires_user_approval())
                && to_state.hooks.after_enter.iter().any(|h|h.action=="auto_cascade_on_unassigned_role" && hook_audience_matches(h.applies_to,&actor))
            {
                if let Some(target)=workflow.outgoing_trigger_targets(&target_state).filter(|(trigger,_)|!trigger.system_only()).find_map(|(_,to)|Self::find_state(workflow,&to).filter(|s|s.kind==StateKind::Active)) {
                    if let Some(role)=crate::workflow::effective_role(target) {
                        admission_agent_id=crate::task_hierarchy::effective_role_assignment(&self.db,&producing,role).await?.map(|r|r.assignment).filter(|a|a.assignee_type==Some(db::AssigneeKind::Agent)).and_then(|a|a.assignee_id);
                        if admission_agent_id.is_some() { dispatch_index=None; }
                    }
                }
            }
            let hook_payload = durable::HookPayload {
                from: current_status.clone(), to: target_state.clone(), actor: actor.clone(), reason: reason.clone(), transition_log_id: transition_log_id.clone(),
                workflow_ref: crate::worker_runtime::queue::WorkflowReference::Snapshot(frozen), authority: authority.as_ref().map(|a| a.project_version),
                from_config: exit_ctx.state_config.clone(), to_config: enter_ctx.state_config.clone(), workspace_id: enter_ctx.workspace_id.clone(), execution_id: enter_ctx.execution_id.clone(), agent_id: enter_ctx.agent_id.clone(),
                skip_before_enter: review_refresh_bridge, skip_on_exit: entry_retry, defer_dispatch_until: defer_dispatch_until.clone(), action_dispatch, pre_results: hook_results.clone(), dispatch_index, admission_agent_id, evidence: cascade_payload.evidence,
            };
            let has_hooks = from_state.hooks.on_exit.iter()
                .chain(&to_state.hooks.before_enter).chain(&to_state.hooks.on_enter).chain(effective_after_enter_hooks(to_state).iter())
                .next().is_some() || (to_state.kind == StateKind::Terminal && self.cleanup_scheduler.is_some());
            let hook_json = serde_json::to_string(&hook_payload).map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
            let initial_hook_step = has_hooks.then(|| db::EnqueueTaskStep {
                kind: "hooks".into(), lane: durable::hooks_lane(workflow, &current_status, &target_state).into(),
                payload_json: hook_json,
                chain_position: step.as_ref().filter(|p| p.chain_id == input.chain_id).map(|p| p.chain_position).unwrap_or(input.chain_position),
                ..input
            });
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
                            post_commit_step: initial_hook_step.clone(),
                            transition_log_id: transition_log_id.clone(),
                            workflow_snapshot: workflow_snapshot.clone(),
                            trigger_name: trigger_name.clone(),
                            triggered_by: actor.display(),
                            trigger_reason: reason.clone(),
                            rejection,
                            expected_project_version: authority
                                .as_ref()
                                .map(|authority| authority.project_version),
                            expected_workflow_definition: authority
                                .as_ref()
                                .map(|authority| authority.workflow_definition.clone()),
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
                queued_step_id: None,
                pending_steps: self.db.pending_steps(&task_id).await?,
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
        self.db.fence_current_step_in_tx(&mut transaction).await?;
                    let version: i64 = sqlx::query_scalar("SELECT version FROM task WHERE id=?")
                        .bind(&task_id).fetch_one(&mut *transaction).await?;
                    if let Some(authority) = authority.as_ref() {
                        let project_authority = query(
                            "SELECT version, workflow_definition FROM project WHERE id = ?",
                        )
                        .bind(&task.project_id)
                        .fetch_optional(&mut *transaction)
                        .await?
                        .ok_or(db::DbError::NotFound)?;
                        let current_project_version: i64 =
                            project_authority.try_get("version")?;
                        let current_workflow_definition: String =
                            project_authority.try_get("workflow_definition")?;
                        if current_project_version != authority.project_version
                            || current_workflow_definition != authority.workflow_definition
                        {
                            return Err(db::DbError::VersionConflict.into());
                        }
                    }
                    let clear_review_passed_at = authority
                        .as_ref()
                        .is_some_and(|authority| authority.clear_review_passed_at_on_commit);
                    // A status change bumps status_epoch through its trigger.
                    // A workflow self-transition (planning -> planning on a
                    // gate reject) is also a new entry: bump it here. A step
                    // fences on status plus epoch, never on the version.
                    let update = query(
                        "UPDATE task\n                 SET status = ?, version = version + 1, updated_at = ?, blocked_json = NULL, entry_barrier_json = ?,\n                     review_passed_at = CASE WHEN ? THEN NULL ELSE review_passed_at END,\n                     status_epoch = status_epoch + (status = ?)\n                 WHERE id = ? AND deleted_at IS NULL AND ((? = 0 AND version = ?) OR (? = 1 AND status = ? AND status_epoch = ?))",
                    )
                    .bind(&target_state)
                    .bind(&updated_at)
                    .bind(entry_barrier_json.as_deref())
                    .bind(clear_review_passed_at)
                    .bind(&target_state)
                    .bind(&task_id)
                    .bind(step.is_some())
                    .bind(version)
                    .bind(step.is_some())
                    .bind(step.as_ref().map(|s| s.expected_status.as_str()))
                    .bind(step.as_ref().map(|s| s.expected_epoch))
                    .execute(&mut *transaction)
                    .await?;

                    if update.rows_affected() != 1 {
                        return Err(db::DbError::VersionConflict.into());
                    }
                    if let Some(marker)=&deferred_marker {
                        sqlx::query("UPDATE task SET metadata_json=json_set(COALESCE(metadata_json,'{}'),'$.deferred_dispatch',json(?)) WHERE id=?")
                            .bind(marker).bind(&task_id).execute(&mut *transaction).await?;
                    } else {
                        sqlx::query("UPDATE task SET metadata_json=CASE WHEN json_valid(metadata_json) THEN json_remove(metadata_json,'$.deferred_dispatch') ELSE metadata_json END WHERE id=?").bind(&task_id).execute(&mut *transaction).await?;
                    }
                    // Carry paused integration through the status hop. Only a
                    // durable successful merge consumes this marker.
                    sqlx::query("UPDATE task SET metadata_json=json_set(metadata_json,'$.paused_integration.state',?) WHERE id=? AND CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json,'$.paused_integration.state') END=?")
                        .bind(&target_state).bind(&task_id).bind(&current_status).execute(&mut *transaction).await?;
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
                        workflow_snapshot,
                    );
                    DomainEventRepo::append_event_in_tx(&*self.db, &mut transaction, &event).await?;
                    sqlx::query(
                        "INSERT INTO transition_log (
                            id, task_id, from_state, to_state, trigger_name, triggered_by,
                            trigger_reason, hook_results_json, rejection, created_at, status_epoch
                         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, (SELECT status_epoch FROM task WHERE id = ?))",
                    )
                    .bind(&transition_log_id)
                    .bind(&task.id)
                    .bind(&current_status)
                    .bind(&target_state)
                    .bind(trigger_name.as_deref())
                    .bind(actor.display())
                    .bind(&reason)
                    .bind(serde_json::to_string(&hook_results).map_err(|e| ServiceError::invalid_operation(e.to_string()))?)
                    .bind(if rejection { 1_i64 } else { 0_i64 })
                    .bind(&updated_at)
                    .bind(&task.id)
                    .execute(&mut *transaction)
                    .await?;
                    if let Some(step) = &step {
                        self.db.finish_step_in_tx(&mut transaction, step, "done", None).await?;
                    }
                    if let Some(input) = &initial_hook_step {
                        self.db.enqueue_step_in_tx(&mut transaction, input).await?;
                    }
                    let task = self.db.get_task_in_tx(&mut transaction, &task_id).await?.ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
                    transaction.commit().await?;
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
            drop(cleanup_guard);

            tracing::info!(
                task_id = %task.id,
                from_state = %current_status,
                to_state = %target_state,
                actor = %actor,
                reason = %reason,
                transition_log_id = %transition_log.id,
                "workflow transition applied"
            );

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

            let queued_step_id = initial_hook_step.as_ref().map(|i| i.id.clone());
            crate::deferred_dispatch::finish_machine_wait(&self.db, &mut task, version).await?;
            if let Some(persisted) = TaskRepo::get_by_id(&*self.db, &task_id, false).await? {
                if persisted.status == task.status && persisted.version == task.version {
                    task = persisted;
                }
            }
            let pending_steps = self.db.pending_steps(&task_id).await?;
            if let Some(id) = &queued_step_id { self.db.ready_step(id).await?; }
            let review = latest_review(&self.db, &task.id).await?;

            Ok(TransitionResult {
                task,
                review,
                queued_step_id,
                pending_steps,
                board_move: board_move_outcome,
            })
        }
        .instrument(span))
    }

    pub(crate) async fn transition_step(
        &self,
        step: &db::TaskStep,
        payload: &crate::worker_runtime::queue::CascadePayload,
        workflow: &WorkflowDefinition,
        authority: Option<WorkflowAuthority>,
    ) -> crate::Result<TransitionResult> {
        db::task_writer::in_task_step(
            step.clone(),
            self.transition_inner(
                step.task_id.clone(),
                payload.to.clone(),
                step.expected_version,
                workflow,
                crate::worker_runtime::queue::cascade_actor(),
                payload.reason.clone(),
                payload.rejection,
                payload.skip_before_exit,
                None,
                None,
                Some(step.clone()),
                authority,
                false,
            ),
        )
        .await
    }

    pub(crate) fn cascade_allowed(state: &StateDefinition, reason: &str, rejection: bool) -> bool {
        state.kind != StateKind::Gate
            || rejection
            || !state
                .gate_config
                .as_ref()
                .is_some_and(|g| g.requires_user_approval())
            || state.gate_config.as_ref().is_some_and(|g| {
                g.optional_when_unassigned() && reason.starts_with("gate skipped:")
            })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn cascade_step_input(
        &self,
        task: &db::Task,
        workflow: &WorkflowDefinition,
        to: String,
        reason: String,
        rejection: bool,
        skip_before_exit: bool,
        authority: Option<WorkflowAuthority>,
        parent: Option<&db::TaskStep>,
        causation_key: String,
        expected_epoch: Option<i64>,
    ) -> crate::Result<db::EnqueueTaskStep> {
        let review_evidence = latest_review(&self.db, &task.id)
            .await?
            .filter(|r| {
                matches!(
                    r.status,
                    db::ReviewStatus::Passed | db::ReviewStatus::Failed
                )
            })
            .map(|r| format!("{}:{}", r.id, r.updated_at));
        let entries = TransitionLogRepo::list_by_task(&*self.db, &task.id).await?;
        let rebases = entries
            .iter()
            .filter(|entry| {
                entry
                    .trigger_reason
                    .contains(crate::workflow::TARGET_MOVED_MARKER)
                    && entry.triggered_by == crate::worker_runtime::queue::cascade_actor().display()
            })
            .count();
        let head: Option<String> = sqlx::query_scalar("SELECT h.head_sha FROM workspace_expected_head h JOIN workspace_placement p ON p.id=h.placement_id AND p.generation=h.generation WHERE p.task_id=? ORDER BY h.recorded_at DESC LIMIT 1")
            .bind(&task.id).fetch_optional(self.db.pool()).await?;
        let evidence = Some(format!("{review_evidence:?}|{rebases}|{head:?}"));
        let workflow_ref = if authority.is_some() {
            crate::worker_runtime::queue::WorkflowReference::Project
        } else {
            crate::worker_runtime::queue::WorkflowReference::Snapshot(
                self.db
                    .store_step_workflow(
                        &serde_json::to_string(workflow)
                            .map_err(|e| ServiceError::invalid_operation(e.to_string()))?,
                    )
                    .await?,
            )
        };
        let continuing = parent.filter(|p| {
            serde_json::from_str::<crate::worker_runtime::queue::CascadePayload>(&p.payload_json)
                .map(|p| p.evidence)
                .or_else(|_| {
                    serde_json::from_str::<durable::HookPayload>(&p.payload_json)
                        .map(|p| p.evidence)
                })
                .is_ok_and(|e| e == evidence)
        });
        let admission_agent_id = if reason.contains(crate::workflow::REVIEW_REFRESH_MARKER)
            || reason.contains(crate::workflow::CONFLICT_HANDOFF_MARKER)
        {
            None
        } else if let Some(role) = workflow
            .states
            .iter()
            .find(|state| state.name == to)
            .and_then(crate::workflow::effective_role)
        {
            crate::task_hierarchy::effective_role_assignment(&self.db, task, role)
                .await?
                .map(|resolved| resolved.assignment)
                .filter(|assignment| assignment.assignee_type == Some(db::AssigneeKind::Agent))
                .and_then(|assignment| assignment.assignee_id)
        } else {
            None
        };
        let payload = crate::worker_runtime::queue::CascadePayload {
            to,
            reason,
            rejection,
            skip_before_exit,
            workflow_ref,
            admission_agent_id,
            clear_review_passed_at_on_commit: authority
                .as_ref()
                .is_some_and(|a| a.clear_review_passed_at_on_commit),
            evidence,
        };
        Ok(db::EnqueueTaskStep {
            kind: "cascade".into(),
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            payload_json: serde_json::to_string(&payload)
                .map_err(|e| ServiceError::invalid_operation(e.to_string()))?,
            causation_step_id: parent.map(|p| p.id.clone()),
            causation_key,
            chain_id: continuing
                .map(|p| p.chain_id.clone())
                .unwrap_or_else(new_uuid_v4),
            chain_position: continuing
                .map(|p| {
                    p.chain_position
                        + i64::from(!(p.kind == "hooks" && p.causation_step_id.is_none()))
                })
                .unwrap_or(1),
            expected_status: task.status.clone(),
            expected_version: task.version,
            expected_epoch,
            lane: crate::worker_runtime::queue::cascade_lane(workflow, &payload.to).into(),
            available_at: now_rfc3339(),
        })
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
