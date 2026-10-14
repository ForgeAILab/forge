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

/// Why a subtask leaves a merge state without an attempt (the transition
/// reason of the entry hook and of the sweep).
pub(crate) const SUBTASK_HAS_NO_MERGE: &str =
    "subtasks do not merge: the work lands with the root Task's merge";

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
    let target = |from: &str, trigger: api_types::WorkflowTrigger| {
        workflow
            .outgoing_trigger_targets(from)
            .find(|(candidate, _)| *candidate == trigger)
            .map(|(_, to)| to)
    };
    // A landed merge goes where THIS merge state's `accept` leads. A
    // workflow with two merge states gives each its own target; the state
    // named `done` is only the answer for a merge state that leads there (or
    // names no `accept` target at all).
    if default_name == default_states::DONE {
        if let Some(own) = target(merge_state, api_types::WorkflowTrigger::Accept) {
            return own;
        }
    }
    // Likewise the repair state: this merge state's own `reject` target
    // when that state carries the repair role's exit guard.
    if default_name == default_states::MERGE_FAILED {
        if let Some(own) = target(merge_state, api_types::WorkflowTrigger::Reject).filter(|own| {
            workflow.states.iter().any(|state| {
                state.name == *own
                    && state
                        .hooks
                        .before_exit
                        .iter()
                        .any(|hook| hook.action == "require_conflict_markers_resolved")
            })
        }) {
            return own;
        }
    }
    if workflow
        .states
        .iter()
        .any(|state| state.name == default_name)
    {
        return default_name.to_owned();
    }
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
        if matches!(current.state, S::Parked | S::Queued)
            && (admission::unbound(&current)
                || admission::binding_stale(&self.db, &current).await?)
        {
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
        // An unknown result the importer took over (a merge that was in
        // flight the old way at the upgrade): it has no fenced intent, so no
        // owner receipt will ever settle it. The owner's Retry is the
        // explicit settlement: the old operation is closed as failed, and
        // Git decides from here (the commit is in the target: reported after
        // the target owner's proof; it is not: queued again). Nothing is
        // merged twice: validation and the fast-forward are of the exact
        // commit.
        let current = match current {
            Some(attempt)
                if unsettled(&attempt)
                    && attempt.effect_intent_json.is_none()
                    && attempt.import_source_json.is_some() =>
            {
                self.db
                    .settle_imported_integration_operation(&attempt.id)
                    .await?;
                self.db.current_integration_attempt(&task.id).await?
            }
            other => other,
        };
        match current {
            Some(attempt) if unsettled(&attempt) => {
                return Err(ServiceError::invalid_operation(
                    "the result of this Task's merge is unknown: reconnect the machine that ran it, or remove that machine",
                ));
            }
            Some(attempt)
                if attempt.state == S::Parked
                    && !admission::unbound(&attempt)
                    && !admission::binding_stale(&self.db, &attempt).await? =>
            {
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

    /// A subtask resting in a merge state (its entry answered before the
    /// hook finished subtasks, a manual move entered without hooks, the
    /// entry's cascade was lost): it has nothing to admit, so it leaves by
    /// the state's landed-merge target, as its entry hook does. The guards
    /// are the admission's own: not a Task its owner stopped, and not an
    /// entry whose hooks are still to run. `Ok(true)`: the subtask moved.
    async fn finish_subtask_in_merge_state(
        &self,
        task: &db::Task,
        workflow: &api_types::WorkflowDefinition,
    ) -> Result<bool> {
        if super::gate::task_gate(&self.db, &task.id).await?.is_some()
            || db::TaskStepRepo::entry_hooks_pending(&*self.db, &task.id).await?
        {
            return Ok(false);
        }
        let result = Box::pin(self.transition(
            task.id.clone(),
            role_target(
                workflow,
                &task.status,
                crate::workflow::default_states::DONE,
            ),
            crate::task_service::TransitionOptions {
                bridge: Default::default(),
                version: task.version,
                reason: Some(SUBTASK_HAS_NO_MERGE.to_owned()),
                triggered_by: crate::worker_runtime::queue::cascade_actor(),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        ))
        .await;
        let Err(error) = result else {
            return Ok(true);
        };
        let Some(current_step) = db::task_writer::current_task_step() else {
            return Err(error);
        };
        // A busy database or a concurrent write is retried by the step
        // runtime like any transient failure: it never parks a subtask
        // whose transition would apply once the other writer committed.
        if !counts_toward_merge_sweep_bound(&error) {
            return Err(error);
        }
        // Dispatcher passes create distinct commands. Count their durable
        // failed attempts in this entry, including the current claimed one,
        // rather than resetting a counter on every pass or restart.
        let attempts = db::TaskStepRepo::task_steps(&*self.db, &task.id)
            .await?
            .iter()
            .filter(|step| {
                step.kind == "command"
                    && step.expected_epoch == current_step.expected_epoch
                    && (step.status == "failed" || step.id == current_step.id)
                    && serde_json::from_str::<serde_json::Value>(&step.payload_json)
                        .is_ok_and(|p| p["operation"] == "reconcile_integration_admission")
            })
            .fold(0_i64, |total, step| total.saturating_add(step.attempts));
        if attempts < crate::worker_runtime::queue::MAX_STEP_ATTEMPTS {
            return Err(error);
        }
        // The root admission's existing park and owner actions also own a
        // subtask whose landed-merge transition cannot be applied.
        let reason = "integration_admission_failed";
        let message = format!(
            "the subtask cannot leave its merge state after {attempts} Task-step attempts: {error}"
        );
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
                        "blocking_reason": reason, "message": message,
                    })
                    .to_string(),
                )),
                blocked_json: Some(Some(
                    serde_json::json!({
                        "kind": reason, "reason": message, "created_at": db::now_rfc3339(),
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
        Ok(false)
    }

    /// The reconciler's step for one Task (`Step::AdmitIntegration`): admit a
    /// Task that is in a merge state and has no attempt anything owns. A
    /// subtask is not admitted: it finishes (`finish_subtask_in_merge_state`).
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
        if task.archived_at.is_some() {
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
        if task.parent_task_id.is_some() {
            return self.finish_subtask_in_merge_state(&task, &workflow).await;
        }
        let owned: bool = self.db.integration_reconcile_task_owned(&task.id).await?;
        // Rebind stale released owners through admission first. Healthy
        // attempts can repair their condition behind a real hold/block.
        let incomplete_import = self
            .db
            .unowned_quarantined_import(&task.id)
            .await?
            .is_some();
        if (owned || incomplete_import) && self.restate_integration_condition(&task).await? {
            return Ok(true);
        }
        if incomplete_import {
            // No automatic readmission, even after its source is repaired.
            // The owner must explicitly retry the incomplete import.
            return Ok(false);
        }
        let Some(merge_service) = self.merge_service.as_ref() else {
            return Ok(false);
        };
        // The same guards as the transition into the state: a Task its owner
        // stopped is not admitted, and an entry whose hooks are still to run
        // admits itself.
        if super::gate::task_gate(&self.db, &task.id).await?.is_some()
            || db::TaskStepRepo::entry_hooks_pending(&*self.db, &task.id).await?
        {
            return Ok(false);
        }
        // A released attempt (queued or parked) that is still its queue's
        // head is its driver's: the supersede transaction refuses a head, so
        // admission would only hand the same row back and `waiting` would be
        // restated at every pass with no rebind. Nothing is written here:
        // the head's round either merges from the checkout the attempt names
        // or ends off the slot (an owner that refuses the fence parks it),
        // and a released attempt off the slot is replaced through admission
        // at the next pass.
        if let Some(attempt) = self
            .db
            .current_integration_attempt(&task.id)
            .await?
            .filter(|attempt| matches!(attempt.state, S::Queued | S::Parked))
        {
            let head = match attempt.queue_id.as_deref() {
                Some(queue_id) => self
                    .db
                    .integration_queue(queue_id)
                    .await?
                    .and_then(|queue| queue.head_attempt_id),
                None => None,
            };
            if head.as_deref() == Some(attempt.id.as_str()) {
                return Ok(false);
            }
        }
        if owned {
            let current = self.db.current_integration_attempt(&task.id).await?;
            // Recovery may have verified a different placement binding.
            // Do not replace a head or an unsettled effect: supersede's
            // transaction is the final authority for whether it is free.
            let rebind = match current {
                Some(ref attempt) if matches!(attempt.state, S::Queued | S::Parked) => {
                    admission::binding_stale(&self.db, attempt).await?
                }
                _ => false,
            };
            if !rebind {
                return Ok(false);
            }
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
            // The worktree would have to be recreated under the disk floor:
            // the Task waits for disk (its typed capacity wait, with the
            // recheck that asks again). Not a block, and nothing is charged.
            Err(error)
                if crate::placement::capacity_wait_of(&error)
                    == Some(crate::placement::CapacityWait::Disk) =>
            {
                self.defer_placement_refusal(&task, &error).await?;
                return Ok(false);
            }
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

/// The durable attempt is the operational source; conditions retain no
/// queue clock/rank/lease. A phase label may lag a healthy driver, but a
/// missing/wrong attempt or a parked cause must be repaired by a Task step.
pub(crate) fn statement_reason(
    snapshot: &serde_json::Value,
) -> Option<api_types::IntegrationReason> {
    use api_types::{IntegrationPhase as P, IntegrationReason as R};
    let id = snapshot["attempt_id"].as_str()?;
    let attempt_id = super::attempt_ref(id);
    let state = snapshot["state"].as_str()?;
    if state == "queued" && snapshot["queue_state"] == "open" {
        return Some(R::Waiting { attempt_id });
    }
    let phase = match state {
        "validating" => Some(P::Validating),
        "rebasing" => Some(P::Rebasing),
        "checking" => Some(P::Checking),
        "awaiting_task_step" => Some(P::AwaitingAuthorization),
        "ready_ff" | "ff_inflight" => Some(P::FastForwarding),
        "reconciling" => Some(P::Reconciling),
        _ => None,
    };
    if let Some(phase) = phase {
        return Some(R::Owned { attempt_id, phase });
    }
    if state == "applied" {
        return Some(R::Applied { attempt_id });
    }
    if !matches!(state, "queued" | "parked" | "quarantined") {
        return None;
    }
    let (kind, message) = if state == "queued" {
        (
            snapshot["queue_failure_kind"].as_str(),
            snapshot["queue_failure_message"].as_str(),
        )
    } else {
        (
            snapshot["failure_kind"].as_str(),
            snapshot["failure_message"].as_str(),
        )
    };
    let message = message.unwrap_or("integration is waiting");
    if state == "parked" && crate::integration_worker::GateCause::parked_under(message) {
        return Some(R::Waiting { attempt_id });
    }
    let kind = kind.and_then(|kind| kind.parse::<db::IntegrationFailureKind>().ok());
    // One rule for every writer of this row (`deferral`); as in the `park`
    // step, the importer's classification does not decide the wait of a
    // `queued` member of a suspended queue.
    let (cause, owner_id) = super::deferral::deferral(&super::deferral::DeferralFacts {
        quarantined: state == "quarantined",
        kind,
        message,
        daemon_id: snapshot["daemon_id"].as_str(),
        import: (state != "queued")
            .then(|| super::deferral::snapshot_import_facts(snapshot))
            .flatten(),
    });
    Some(R::Deferred {
        attempt_id,
        cause,
        owner_id,
        message: super::bounded(message),
    })
}

pub(crate) fn statement_stale(
    snapshot: Option<&serde_json::Value>,
    condition: &db::TaskCondition,
) -> bool {
    let Some(snapshot) = snapshot else {
        return false;
    };
    let Some(expected) = statement_reason(snapshot) else {
        return false;
    };
    let Some(actual) = condition.integration_reason() else {
        return true;
    };
    if actual.attempt_id() != expected.attempt_id() {
        return true;
    }
    // Healthy heads state themselves at their deciding steps, avoiding
    // extra writes for every phase. Released/suspended waits are exact.
    let waiting = matches!(snapshot["state"].as_str(), Some("parked" | "quarantined"))
        || (snapshot["state"] == "queued" && snapshot["queue_state"] != "open");
    // Every writer of a deferral (the `park` step, the import continuation,
    // this restatement) decides the cause and the machine with one function
    // (`deferral::deferral`), from facts this snapshot carries too. A stated
    // deferral is stale only when those differ; its message is its writer's
    // wording (the import continuation and the `result` step add detail the
    // row does not hold) and is never a reason to restate.
    waiting
        && match (actual, &expected) {
            (
                api_types::IntegrationReason::Deferred {
                    cause, owner_id, ..
                },
                api_types::IntegrationReason::Deferred {
                    cause: expected_cause,
                    owner_id: expected_owner,
                    ..
                },
            ) => cause != expected_cause || owner_id != expected_owner,
            _ => actual != &expected,
        }
}

impl crate::TaskService {
    async fn restate_integration_condition(&self, task: &db::Task) -> Result<bool> {
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        let snapshot = db::integration_statement_in_tx(&mut tx, &task.id).await?;
        let current = self
            .db
            .get_task_in_tx(&mut tx, &task.id)
            .await?
            .ok_or(db::DbError::NotFound)?;
        if !statement_stale(snapshot.as_ref(), &current.condition) {
            return Ok(false);
        }
        let Some(reason) = snapshot.as_ref().and_then(statement_reason) else {
            return Ok(false);
        };
        if let Some(old) = current
            .condition
            .integration_reason()
            .filter(|old| old.attempt_id() != reason.attempt_id())
        {
            self.db
                .state_integration_condition_in_tx(
                    &mut tx,
                    &task.id,
                    &db::ConditionStatement::IntegrationCleared {
                        attempt_id: old.attempt_id().clone(),
                    },
                )
                .await?;
        }
        let intervention = reason.requires_intervention();
        self.db
            .state_integration_condition_in_tx(
                &mut tx,
                &task.id,
                &db::ConditionStatement::Integration { reason },
            )
            .await?;
        if intervention {
            let task = self
                .db
                .get_task_in_tx(&mut tx, &task.id)
                .await?
                .ok_or(db::DbError::NotFound)?;
            super::IntegrationSteps::new(self.clone())
                .raise_interruption_in_tx(&mut tx, &task)
                .await?;
        }
        tx.commit().await?;
        Ok(true)
    }
}

/// Whether a failed landed-merge transition of a subtask counts toward the
/// sweep's bound: a refusal does, a transient database failure (busy, a
/// version conflict from a concurrent write) does not.
pub(crate) fn counts_toward_merge_sweep_bound(error: &crate::ServiceError) -> bool {
    !matches!(error, crate::ServiceError::Db(db_error) if db_error.is_transient())
}
