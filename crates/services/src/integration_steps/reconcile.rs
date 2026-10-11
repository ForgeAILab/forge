//! The level-triggered half of admission (plan 3.2 stage D2b).
//!
//! The `run_merge` entry hook admits a Task once, when it enters its merge
//! state. Everything that can leave a Task in a merge state with no attempt
//! anything drives is closed here, by the dispatcher's sweep
//! (`Step::AdmitIntegration`): the hook returned `Skipped`, the start-time
//! import failed or left a row for the Task's owner whose block has since
//! been lifted in place, a manual move entered the state without its hooks.
//! The reconciler calls the admission function the hook calls
//! (`admission::admit_released`), so it is idempotent and identity-fenced.
//!
//! The rule it keeps: a Task in a merge state has a live attempt, or a typed
//! owner-visible condition with an offered action. There is no third state.
use super::admission;
use crate::{integration_worker::IntegrationEnqueuePort, Result, ServiceError};
use db::{IntegrationAttemptState as S, IntegrationQueueRepo, TaskRepo};

/// The state of `workflow` whose entry admits to the integration queue: the
/// role a state has by carrying the `run_merge` entry hook, whatever its
/// name. Nothing in the queue keys on the names of the default workflow.
pub(crate) fn merge_state(workflow: &api_types::WorkflowDefinition, name: &str) -> bool {
    workflow
        .states
        .iter()
        .find(|state| state.name == name)
        .is_some_and(|state| {
            state
                .hooks
                .on_enter
                .iter()
                .any(|hook| hook.action == "run_merge")
        })
}

/// The state a merge outcome moves the Task to, by role. The merge hook and
/// its steps name their targets as the default workflow does (`done`,
/// `merge_failed`, `review`). A workflow that defines a state of that name
/// keeps it; one that renamed its states gets the state that plays the role
/// for `merge_state`: its `accept` target (the merge landed), the state that
/// carries the repair role's own exit guard (the merge needs repair), and the
/// review state the repair returns to.
pub(crate) fn role_target(
    workflow: &api_types::WorkflowDefinition,
    merge_state: &str,
    default_name: &str,
) -> String {
    use crate::workflow::default_states;
    if workflow
        .states
        .iter()
        .any(|state| state.name == default_name)
    {
        return default_name.to_owned();
    }
    let target = |from: &str, trigger: api_types::WorkflowTrigger| {
        workflow
            .outgoing_trigger_targets(from)
            .find(|(candidate, _)| *candidate == trigger)
            .map(|(_, to)| to)
    };
    // The repair state is the one that carries the conflict-marker exit
    // guard (the hook that belongs to that role), else the merge state's
    // `reject` target.
    let hooked = |action: &str| {
        workflow
            .states
            .iter()
            .find(|state| {
                [
                    &state.hooks.before_exit,
                    &state.hooks.before_enter,
                    &state.hooks.on_enter,
                    &state.hooks.after_enter,
                ]
                .into_iter()
                .flatten()
                .any(|hook| hook.action == action)
            })
            .map(|state| state.name.clone())
    };
    let repair = || {
        hooked("require_conflict_markers_resolved")
            .or_else(|| target(merge_state, api_types::WorkflowTrigger::Reject))
    };
    let resolved = if default_name == default_states::DONE {
        target(merge_state, api_types::WorkflowTrigger::Accept)
    } else if default_name == default_states::MERGE_FAILED {
        repair()
    } else if default_name == default_states::REVIEW {
        repair()
            .and_then(|repair| crate::workflow::review_refresh_target(workflow, &repair))
            .or_else(|| hooked("run_ci_steps"))
            .or_else(|| crate::workflow::review_refresh_target(workflow, merge_state))
    } else {
        None
    };
    resolved.unwrap_or_else(|| default_name.to_owned())
}

impl crate::TaskService {
    pub fn wake_integration_worker(&self) {
        if let Some(worker) = self.integration_worker() {
            worker.notify_enqueued("");
        }
    }

    /// An owner command or a status change may have lifted the Task gate
    /// (`gate::task_gate`). The Task's waiting attempt is made due and the
    /// worker woken, so the Task is back in line at once; the worker's sweep
    /// finds the same thing without this call.
    pub async fn integration_gate_lifted(&self, task_id: &str) -> Result<()> {
        if super::gate::task_gate(&self.db, task_id).await?.is_some() {
            return Ok(());
        }
        if self
            .db
            .expedite_parked_integration_attempt(task_id, &db::now_rfc3339(), false)
            .await?
        {
            self.wake_integration_worker();
        }
        Ok(())
    }

    /// The machine that owns the Task's checkout is back and the Task's
    /// placement is `ready` again (`recovery::reconcile_workspace_placement`).
    /// An attempt the worker parked for that machine (`owner_offline`, or a
    /// counted retry it was still waiting out) is due now and the worker is
    /// woken. A Task whose merge was waiting for the machine before the
    /// queue existed (importer row P15: its attempt names no candidate, so
    /// the worker cannot drive it) is admitted here, as the reconciler's
    /// sweep would admit it at its next pass. No owner action is involved.
    pub async fn integration_owner_ready(&self, task_id: &str) -> Result<()> {
        let Some(current) = self.db.current_integration_attempt(task_id).await? else {
            return Ok(());
        };
        if current.state.terminal() {
            return Ok(());
        }
        if matches!(current.state, S::Parked | S::Queued) && admission::unbound(&current) {
            if let Some(task) = TaskRepo::get_by_id(&*self.db, task_id, false).await? {
                self.reconcile_integration_admission(&task).await?;
            }
            return Ok(());
        }
        if self
            .db
            .expedite_parked_integration_attempt(task_id, &db::now_rfc3339(), false)
            .await?
        {
            self.wake_integration_worker();
        }
        Ok(())
    }

    /// A Project was resumed: its Tasks' waiting attempts are due now.
    pub async fn integration_project_resumed(&self, project_id: &str) -> Result<()> {
        if self
            .db
            .expedite_parked_integration_attempts_of_project(project_id, &db::now_rfc3339())
            .await?
            > 0
        {
            self.wake_integration_worker();
        }
        Ok(())
    }

    /// The owner's Retry of a merge that stopped for them (the offer
    /// `integration_retry`): a parked attempt goes back in line with its
    /// counted retries started again; one that left the queue for good, or a
    /// Task with no attempt at all, is admitted again.
    pub(crate) async fn retry_integration(&self, task: &db::Task) -> Result<db::Task> {
        let current = self.db.current_integration_attempt(&task.id).await?;
        let unsettled = |attempt: &db::IntegrationAttempt| {
            attempt.effect_intent_json.is_some()
                || matches!(
                    attempt.current_operation_state,
                    Some(
                        db::IntegrationOperationState::Running
                            | db::IntegrationOperationState::Uncertain
                    )
                )
        };
        match current {
            Some(attempt) if unsettled(&attempt) => {
                return Err(ServiceError::invalid_operation(
                    "the result of this Task's merge is unknown: reconnect the machine that ran it, or remove that machine",
                ));
            }
            Some(attempt) if attempt.state == S::Parked && !admission::unbound(&attempt) => {
                self.db
                    .expedite_parked_integration_attempt(&task.id, &db::now_rfc3339(), true)
                    .await?;
            }
            _ => {
                let merge_service = self.merge_service.as_ref().ok_or_else(|| {
                    ServiceError::invalid_operation("merge service not configured")
                })?;
                let attempt = admission::admit_released_or_replaced(
                    &self.db,
                    merge_service,
                    task,
                    self.integration_worker().as_deref(),
                    true,
                )
                .await?;
                admission::state_waiting(&self.db, &attempt).await?;
            }
        }
        self.wake_integration_worker();
        TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))
    }

    /// The reconciler's step for one Task (`Step::AdmitIntegration`): admit a
    /// Task that is in a merge state and has no attempt anything owns.
    /// Every condition is read again here, under the Task's step lease.
    /// `Ok(true)`: the Task has a queued attempt now.
    pub(crate) async fn reconcile_integration_admission(&self, task: &db::Task) -> Result<bool> {
        if !db::task_writer::owns_task(&task.id) {
            return self
                .request_task_command(
                    &task.id,
                    "reconcile_integration_admission",
                    serde_json::json!([task.id]),
                    false,
                )
                .await;
        }
        if self
            .integration_import_pending
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Ok(false);
        }
        let Some(task) = TaskRepo::get_by_id(&*self.db, &task.id, false).await? else {
            return Ok(false);
        };
        let Some(merge_service) = self.merge_service.as_ref() else {
            return Ok(false);
        };
        if task.parent_task_id.is_some() || task.archived_at.is_some() {
            return Ok(false);
        }
        let project = db::ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = crate::workflow::engine::WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &crate::worker_runtime::queue::cascade_actor(),
        );
        if !merge_state(&workflow, &task.status) {
            return Ok(false);
        }
        // The same guards as the transition into the state: a Task its owner
        // stopped is not admitted, and an entry whose hooks are still to run
        // admits itself.
        if super::gate::task_gate(&self.db, &task.id).await?.is_some()
            || db::TaskStepRepo::entry_hooks_pending(&*self.db, &task.id).await?
        {
            return Ok(false);
        }
        let owned: bool = sqlx::query_scalar(db::INTEGRATION_OWNED_SQL)
            .bind(&task.id)
            .fetch_one(self.db.pool())
            .await
            .map_err(db::DbError::from)?;
        if owned {
            return Ok(false);
        }
        let attempt = match admission::admit_released_or_replaced(
            &self.db,
            merge_service,
            &task,
            self.integration_worker().as_deref(),
            true,
        )
        .await
        {
            Ok(attempt) => attempt,
            Err(error) if crate::worker_runtime::queue::retryable(&error) => return Err(error),
            // No candidate can be named (no workspace, no implementation
            // run, the repository is gone). The Task says so, typed, and its
            // owner's Retry asks again; nothing is declared reviewed.
            Err(error) => {
                let message = format!("the Task cannot join the merge queue: {error}");
                let reason = "integration_admission_failed";
                TaskRepo::update(
                    &*self.db,
                    db::UpdateTask {
                        id: task.id.clone(),
                        expected_version: task.version,
                        title: None,
                        description: None,
                        priority: None,
                        merge_config: None,
                        plan: None,
                        error_annotation: Some(Some(
                            serde_json::json!({
                                "type": api_types::FailureKind::WorkspaceError,
                                "blocking_reason": reason,
                                "message": message,
                            })
                            .to_string(),
                        )),
                        blocked_json: Some(Some(
                            serde_json::json!({
                                "kind": reason,
                                "reason": message,
                                "created_at": db::now_rfc3339(),
                            })
                            .to_string(),
                        )),
                        failed_json: None,
                        task_state_config: None,
                        parent_task_id: None,
                        updated_at: db::now_rfc3339(),
                    },
                )
                .await?;
                return Ok(false);
            }
        };
        admission::state_waiting(&self.db, &attempt).await?;
        if let Some(worker) = self.integration_worker() {
            worker.notify_enqueued(attempt.queue_id.as_deref().unwrap_or_default());
        }
        Ok(true)
    }
}
