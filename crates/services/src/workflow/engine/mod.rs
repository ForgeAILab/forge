use db::task_condition::channels;
use std::{future::Future, pin::Pin, sync::Arc, time::Instant};

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
use tracing::Instrument;

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
    workflow::{default_workflow, inherited_subtask_workflow, registry, HookContext, HookResult},
    ServiceError,
};

mod context;
pub(crate) mod durable;
mod hooks;
#[cfg(test)]
mod tests;
mod transition;

/// `error_annotation` type recorded when a dispatch hook fails entering an
/// active state. The task dispatcher treats it as blocking, so a task whose
/// dispatch deterministically fails (e.g. governance says it is not runnable)
/// is parked instead of rescheduled in a loop.
pub(crate) const DISPATCH_FAILED_ANNOTATION: &str = "dispatch_failed";

struct ReviewEntryFailure {
    task: db::Task,
    cascade: Option<(String, String)>,
    /// The bridge of that cascade: `replan` for a coordination root with a
    /// lead, none otherwise.
    bridge: api_types::TransitionBridge,
}

fn dispatch_failed_annotation(
    state: &str,
    message: &str,
    daemon_ids: Option<&[String]>,
) -> channels::ErrorAnnotation {
    let detected_at = now_rfc3339();
    let shape = match daemon_ids {
        Some(daemon_ids) => {
            channels::AnnotationShape::UpgradeDispatch(channels::UpgradeDispatchAnnotation {
                code: channels::UpgradeCode::DaemonUpgradeRequired,
                daemon_ids: daemon_ids.to_vec(),
                detected_at,
                message: message.into(),
                state: state.into(),
                kind: api_types::FailureKind::DispatchFailed,
            })
        }
        None => channels::AnnotationShape::DetectedState(channels::DetectedStateAnnotation {
            kind: api_types::FailureKind::DispatchFailed,
            message: message.into(),
            state: state.into(),
            detected_at,
        }),
    };
    channels::ErrorAnnotation::new(shape)
}

/// The checks `run_ci_steps` will execute when this Task next enters review,
/// resolved from the same merged review-state config the hook reads. Empty
/// when the workflow has no review state or the config names no steps.
/// A state's configuration as its hooks see it: the workflow's, overlaid by
/// the Project's and the Task's.
pub(crate) fn effective_state_config(
    state: &StateDefinition,
    project: Option<&db::Project>,
    task_state_config_json: Option<&str>,
) -> serde_json::Value {
    hooks::merged_state_config(state, project, task_state_config_json)
}

pub(crate) fn review_ci_steps_for_task(
    workflow: &api_types::WorkflowDefinition,
    project: Option<&db::Project>,
    task_state_config_json: Option<&str>,
) -> Vec<String> {
    workflow
        .states
        .iter()
        .find(|state| db::budget::is_review_state(state))
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
    let daemon_ids: Vec<String> = daemon_ids.into_iter().map(str::to_owned).collect();
    let task = TaskRepo::get_by_id(db, task_id, false)
        .await?
        .ok_or(db::DbError::NotFound)?;
    let expected_annotation = if preserve_dispatch_annotation(task.error_annotation.as_deref()) {
        serde_json::from_str::<serde_json::Value>(task.error_annotation.as_deref().unwrap())
            .expect("blocking annotation JSON")
    } else {
        let mut annotation = serde_json::to_value(dispatch_failed_annotation(
            state,
            &error.to_string(),
            Some(&daemon_ids),
        ))
        .expect("condition channel serializes");
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
    let marker = serde_json::to_value(channels::DaemonUpgradeRefusal {
        code: channels::UpgradeCode::DaemonUpgradeRequired,
        daemon_ids: daemon_ids.clone(),
        error_annotation: channels::CopiedJson::new(expected_annotation),
        dispatch_disposition: Some(channels::CopiedJson::new(
            metadata["dispatch_disposition"].clone(),
        )),
        deferred_dispatch: Some(channels::CopiedJson::new(
            metadata["deferred_dispatch"].clone(),
        )),
    })
    .expect("condition metadata serializes");
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
    annotate_dispatch_failure_details(
        db,
        task_id,
        state,
        &error.to_string(),
        None,
        Some(daemon_ids),
    )
    .await
}

async fn annotate_dispatch_failure_details(
    db: &db::SqliteDb,
    task_id: &str,
    state: &str,
    message: &str,
    authority: Option<&WorkflowAuthority>,
    upgrade_daemon_ids: Option<Vec<String>>,
) -> db::Result<()> {
    let annotation = serde_json::to_string(&dispatch_failed_annotation(
        state,
        message,
        upgrade_daemon_ids.as_deref(),
    ))
    .expect("condition channel serializes");
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
            if upgrade_daemon_ids.is_none()
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
    let task_ids = db
        .daemon_upgrade_refused_task_ids(api_types::DAEMON_UPGRADE_REQUIRED)
        .await?;
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
    let result = db
        .clear_daemon_upgrade_dispatch_refusal(db::ClearDaemonUpgradeDispatchRefusalInput {
            clear_annotation,
            updated_at: now_rfc3339(),
            task_id: &task.id,
            version: task.version,
            metadata_json: &task.metadata_json,
            error_annotation: &task.error_annotation,
            refusal_code: api_types::DAEMON_UPGRADE_REQUIRED,
        })
        .await?;
    let changed = result != 0;
    if changed {
        tracing::info!(task_id = %task.id, "task dispatch woken after daemon upgrade");
    }
    Ok(changed)
}

/// Immutable engine shared by a service and all its clones. Configured dispatch
/// dependencies belong to the service supplied for each execution.
pub struct WorkflowEngine {
    pub db: Arc<db::SqliteDb>,
    pub event_bus: Arc<EventBus>,
    #[cfg(test)]
    invocations: std::sync::atomic::AtomicUsize,
}

/// Borrowed operation context; it owns neither the engine nor its service.
pub struct WorkflowExecution<'a> {
    engine: &'a WorkflowEngine,
    pub task_service: &'a crate::TaskService,
}

impl std::ops::Deref for WorkflowExecution<'_> {
    type Target = WorkflowEngine;
    fn deref(&self) -> &WorkflowEngine {
        self.engine
    }
}

impl WorkflowEngine {
    pub fn new(db: Arc<db::SqliteDb>, event_bus: Arc<EventBus>) -> Self {
        Self {
            db,
            event_bus,
            #[cfg(test)]
            invocations: std::sync::atomic::AtomicUsize::new(0),
        }
    }
    #[cfg(test)]
    pub(crate) fn invocation_count(&self) -> usize {
        self.invocations.load(std::sync::atomic::Ordering::Relaxed)
    }
    pub fn bind<'a>(&'a self, task_service: &'a crate::TaskService) -> WorkflowExecution<'a> {
        WorkflowExecution {
            engine: self,
            task_service,
        }
    }
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

impl WorkflowExecution<'_> {
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
        ctx: &HookContext,
        action: &str,
        entry_started_at: &str,
        authority: Option<&WorkflowAuthority>,
    ) -> crate::Result<Option<ReviewEntryFailure>> {
        if ctx.triggered_by.is_user() {
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
                bridge: Default::default(),
            }));
        }
        let Some(review) = latest_review(&self.db, &task.id).await? else {
            return Ok(None);
        };
        // Infrastructure interruptions were classified and settled by the
        // hook above. A progress receipt does not turn configuration or other
        // deterministic failures into infrastructure retries.
        if action != "run_ci_steps" {
            return Ok(None);
        }
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
        let bridge = match &target {
            Some(target) => {
                self.task_service
                    .review_rejection_bridge(&task, target, &reason)
                    .await?
            }
            None => Default::default(),
        };
        Ok(Some(ReviewEntryFailure {
            task,
            cascade: target.map(|target| (target, reason)),
            bridge,
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
        bridge: api_types::TransitionBridge,
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
            None,
            bridge,
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
            Default::default(),
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
        bridge: api_types::TransitionBridge,
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
            bridge,
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
            Default::default(),
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
            Default::default(),
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
            Default::default(),
        )
        .await
    }

    /// Re-enter the Task's current state because a review re-run was asked
    /// for. The entry is the workflow's own (`actor`); `bridge` records who
    /// asked ([`api_types::TransitionBridge::review_rerun`]).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn review_rerun_entry_with_authority(
        &self,
        task: &db::Task,
        workflow: &WorkflowDefinition,
        actor: Actor,
        reason: &str,
        authority: WorkflowAuthority,
        bridge: api_types::TransitionBridge,
    ) -> crate::Result<TransitionResult> {
        crate::task_service::execution::ensure_plan_publication_transition_authority(task, None)?;
        self.transition_inner(
            task.id.clone(),
            task.status.clone(),
            task.version,
            workflow,
            actor,
            reason.to_string(),
            false,
            true,
            None,
            None,
            None,
            Some(authority),
            false,
            bridge,
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
        // The retry is the same entry run again: a review re-run keeps who
        // asked for it, and with that its budget exemption.
        let entered: Option<(String, Option<String>, Option<String>)> = self
            .db
            .task_current_entry_first_transition_bridge(task_id, &target_state)
            .await?;
        let bridge = entered
            .and_then(|(id, kind, payload)| {
                db::decode_transition_bridge(&id, kind.as_deref(), payload.as_deref()).ok()
            })
            .filter(|bridge| bridge.review_rerun_requester().is_some())
            .unwrap_or_default();
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
            bridge,
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
        let to_state = WorkflowEngine::find_state(workflow, target_state).ok_or_else(|| {
            ServiceError::InvalidOperation {
                message: WorkflowEngine::undefined_state_message(target_state, workflow),
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
                Default::default(),
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
        let to_state = WorkflowEngine::find_state(workflow, target_state).ok_or_else(|| {
            ServiceError::InvalidOperation {
                message: WorkflowEngine::undefined_state_message(target_state, workflow),
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
                Default::default(),
            )
            .await?;
        Ok(result.task)
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
                payload.bridge.clone(),
            ),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn cascade_step_input(
        &self,
        task: &db::Task,
        workflow: &WorkflowDefinition,
        to: String,
        reason: String,
        bridge: api_types::TransitionBridge,
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
                entry.bridge.bridge_kind == Some(api_types::TransitionBridgeKind::TargetMovedRebase)
                    && entry.triggered_by == crate::worker_runtime::queue::cascade_actor().display()
            })
            .count();
        let head: Option<String> = self
            .db
            .task_latest_expected_workspace_head(&task.id)
            .await?;
        let evidence = Some(format!("{review_evidence:?}|{rebases}|{head:?}"));
        let workflow_ref = if authority.is_some() {
            db::WorkflowReference::Project
        } else {
            db::WorkflowReference::Definition(
                serde_json::to_string(workflow)
                    .map_err(|e| ServiceError::invalid_operation(e.to_string()))?,
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
        let admission_agent_id = if bridge.is_review_refresh()
            || bridge.bridge_kind == Some(api_types::TransitionBridgeKind::ConflictHandoff)
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
            bridge,
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
}

impl WorkflowEngine {
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
        let project_workflow = WorkflowEngine::resolve_workflow(workflow_definition_json);
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
        // A user acts under the Project workflow where it has the state. The
        // step check exists only in the inherited workflow, so a subtask
        // resting there is governed by it for every actor.
        if actor.is_user()
            && project_workflow
                .states
                .iter()
                .any(|s| s.name.as_str() == task.status.as_str())
        {
            return project_workflow;
        }
        subtask_wf
    }
    pub(crate) fn cascade_allowed(
        state: &StateDefinition,
        bridge: &api_types::TransitionBridge,
        rejection: bool,
    ) -> bool {
        state.kind != StateKind::Gate
            || rejection
            // The no-change reminder leaves before the gate ever asks for
            // approval; it is not a rejection and charges none.
            || bridge.is_repair_no_change()
            || !state
                .gate_config
                .as_ref()
                .is_some_and(|g| g.requires_user_approval())
            || state.gate_config.as_ref().is_some_and(|g| {
                g.optional_when_unassigned()
                    && bridge.bridge_kind == Some(api_types::TransitionBridgeKind::GateSkipped)
            })
    }
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
    fn find_state<'a>(workflow: &'a WorkflowDefinition, name: &str) -> Option<&'a StateDefinition> {
        workflow.states.iter().find(|s| s.name == name)
    }
}

#[cfg(test)]
pub(crate) mod resolve_workflow_tests {
    use db::{new_uuid_v4, now_rfc3339};

    use super::WorkflowEngine;
    use crate::workflow::{default_states, default_workflow, inherited_subtask_workflow};

    pub(crate) fn task(parent_task_id: Option<String>, status: &str) -> db::Task {
        let now = now_rfc3339();
        db::Task {
            condition_digest: None,
            condition: Default::default(),
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

#[cfg(test)]
mod writer_parity_tests;
