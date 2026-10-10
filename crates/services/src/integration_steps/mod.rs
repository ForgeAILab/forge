//! The Task-step consumer of the integration queue (plan 3.2 stage D1b).
//!
//! The queue worker (`integration_worker`) never writes Task state. Each of
//! its requests arrives here as one `integration` Task step and is answered by
//! an acknowledgment (and, for `settle`, a permit) written on the attempt in
//! the same transaction as the step's Task write. Nothing enqueues such a
//! step until the queue is activated (stage D2): today's merge path does not
//! pass through this module.
//!
//! Per action: what the attempt must look like for the step to apply (it is
//! enqueued *before* its attempt transition, so "not yet" is a retry, and an
//! attempt that moved past the step's `effect_seq` or generation finishes the
//! step without a write), what it writes on the Task, and what it answers.
//!
//! | action | attempt | Task effect | answer |
//! |---|---|---|---|
//! | `request_check` | `checking` | asks the check runner for the rebased commit, states `owned/checking` | none; the check's delivery writes the `settle` answer |
//! | `settle` | `awaiting_task_step` | review authority, carry by attempt lineage, states `owned/fast_forwarding`, pre-enqueues the protected `result` step | `settle` ack + permit |
//! | `result` | `applied` | execution evidence, "Changes merged" comment, states `applied`, enqueues the `done` cascade | `result` ack |
//! | `send_back` | `ejected`, `needs_review` | today's conflict handoff / merge failure / review refresh, states the typed reason and the handoff | `send_back` ack |
//! | `park` | `parked`, `quarantined`, or `queued` | states `deferred` (or `waiting` once the queue is open again), `task.interruption_changed` for an intervention | `park` ack |
//! | `clear` | `cancelled` | clears this attempt's integration statement | none (the attempt is terminal) |
use crate::{
    integration_worker::{
        IntegrationStepAck, IntegrationStepAction, IntegrationStepOutcome, IntegrationStepRequest,
    },
    workflow::engine::{WorkflowAuthority, WorkflowEngine},
    Result, ServiceError,
};
use api_types::{
    IntegrationAttemptId, IntegrationDeferralCause, IntegrationPhase, IntegrationReason,
};
use db::{
    ConditionStatement, IntegrationAttempt, IntegrationAttemptState as S, IntegrationFailureKind,
    IntegrationQueueRepo, IntegrationQueueState, IntegrationStepAckOutcome,
    IntegrationStepAckWrite, IntegrationStepPosition, SqliteDb, TaskRepo, TaskStep, TaskStepRepo,
};
use std::sync::Arc;

mod port;
mod send_back;
mod settle;
#[cfg(test)]
mod tests;

pub use port::TaskStepIntegrationPort;
pub use settle::{HeadCheck, HeadCheckSource, IntegrationCheckFamily, ReviewCiHeadCheck};

/// The Task-step kind this module consumes.
pub const INTEGRATION_STEP_KIND: &str = "integration";

/// How long the protected `result` step sleeps before it looks for itself.
/// The worker readies it the moment the fast-forward is recorded; this only
/// bounds how long an unused permit keeps a waiting owner command queued.
pub(crate) const RESULT_WAKE_SECONDS: i64 = 150;
/// The protected step's poll while a fast-forward is in flight or unknown.
const RESULT_POLL_SECONDS: i64 = 15;
/// A `result` that cannot be applied waits this long; `ready_result_step`
/// re-arms it sooner.
const RESULT_PARK_SECONDS: i64 = 600;
const TEXT_LIMIT: usize = 1000;

/// What a handler did with its step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StepOutcome {
    /// The handler's own transaction settled the step.
    Settled,
    /// Nothing to write (answered already, or the attempt moved on).
    Skip(String),
    /// The Task left the status entry the attempt was admitted in.
    TaskLeft,
    /// The attempt is not in the state this step answers yet.
    Retry(String),
    /// Like `Retry`, at a time the handler chose.
    RetryAt(String, chrono::DateTime<chrono::Utc>),
}

pub(crate) fn bounded(text: &str) -> String {
    let mut end = text.len().min(TEXT_LIMIT);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}
pub(crate) fn attempt_ref(attempt_id: &str) -> IntegrationAttemptId {
    IntegrationAttemptId::new(attempt_id)
}
pub(crate) fn ack_value(
    request: &IntegrationStepRequest,
    action: IntegrationStepAction,
    outcome: IntegrationStepOutcome,
    check: Option<db::IntegrationCheckTiming>,
    message: Option<String>,
) -> serde_json::Value {
    serde_json::json!(IntegrationStepAck {
        effect_seq: request.effect_seq,
        generation: request.generation,
        action,
        outcome,
        check,
        message: message.as_deref().map(bounded),
    })
}

/// Executes `integration` Task steps. One per process, built from the
/// service the step worker runs under.
pub struct IntegrationSteps {
    pub(crate) db: Arc<SqliteDb>,
    pub(crate) task_service: crate::TaskService,
    pub(crate) engine: Arc<WorkflowEngine>,
    pub(crate) head_check: Arc<dyn HeadCheckSource>,
}

impl IntegrationSteps {
    pub fn new(task_service: crate::TaskService) -> Self {
        let engine = task_service.workflow_engine();
        Self {
            db: Arc::clone(&engine.db),
            head_check: Arc::new(ReviewCiHeadCheck::new(Arc::clone(&engine.db))),
            engine,
            task_service,
        }
    }
    /// Replace how the head's check identity is built (tests, and a later
    /// stage that shares the review-entry builder).
    pub fn with_head_check(mut self, source: Arc<dyn HeadCheckSource>) -> Self {
        self.head_check = source;
        self
    }

    /// The dispatch arm of the step worker. Runs inside the step's
    /// `in_task_step` scope, under its lease.
    pub(crate) async fn execute(&self, step: &TaskStep) -> Result<()> {
        let request: IntegrationStepRequest =
            serde_json::from_str(&step.payload_json).map_err(|error| {
                ServiceError::invalid_operation(format!("invalid integration step: {error}"))
            })?;
        if request.task_id != step.task_id {
            return Err(ServiceError::invalid_operation(
                "integration step names another Task",
            ));
        }
        if request.action == IntegrationStepAction::Result {
            // The fast-forward is done and recorded: this step is the only
            // way the Task learns of it, so it never settles as a failure.
            return self.result_step(step, &request).await;
        }
        let outcome = match request.action {
            IntegrationStepAction::RequestCheck => self.request_check(step, &request).await?,
            IntegrationStepAction::Settle => {
                self.settle(
                    Some(step),
                    &request,
                    &[S::AwaitingTaskStep],
                    settle::CheckInput::NotRequested,
                )
                .await?
            }
            IntegrationStepAction::SendBack => self.send_back(step, &request).await?,
            IntegrationStepAction::Park => self.park(step, &request).await?,
            IntegrationStepAction::Clear => self.clear(step, &request).await?,
            IntegrationStepAction::Result => unreachable!("handled above"),
        };
        self.conclude(step, outcome).await
    }

    pub(crate) async fn conclude(&self, step: &TaskStep, outcome: StepOutcome) -> Result<()> {
        match outcome {
            StepOutcome::Settled => {}
            StepOutcome::Skip(reason) => self.finish(step, "done", Some(&reason)).await?,
            StepOutcome::TaskLeft => {
                self.finish(
                    step,
                    "superseded",
                    Some("Task left the status entry the attempt was admitted in"),
                )
                .await?
            }
            StepOutcome::Retry(reason) => {
                // The worker enqueues, then transitions: normally matched on
                // the first try. Back off to 30 s for a worker that died in
                // between (its successor asks under a new `effect_seq`).
                let millis = 50_i64 << (step.attempts - 1).clamp(0, 10);
                let due = chrono::Utc::now() + chrono::Duration::milliseconds(millis.min(30_000));
                self.db.retry_step(step, &reason, &due.to_rfc3339()).await?;
            }
            StepOutcome::RetryAt(reason, due) => {
                self.db.retry_step(step, &reason, &due.to_rfc3339()).await?;
            }
        }
        self.db.domain_event_notify().notify_waiters();
        Ok(())
    }

    async fn finish(&self, step: &TaskStep, status: &str, note: Option<&str>) -> Result<()> {
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        self.db
            .finish_step_in_tx(&mut tx, step, status, note)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// The Task is still in the status entry the attempt was admitted in.
    pub(crate) async fn entry_live_in_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        attempt: &IntegrationAttempt,
    ) -> Result<bool> {
        Ok(sqlx::query_scalar(db::STEP_FENCE)
            .bind(&attempt.task_ref)
            .bind(&attempt.expected_status)
            .bind(attempt.expected_epoch)
            .fetch_one(&mut **tx)
            .await?)
    }

    /// State an integration reason for the attempt. The statement is fenced
    /// on the attempt that owns the Task's integration witness: a refusal
    /// there (another attempt still owns it) is logged, never the step's
    /// failure. A lost lease is: it returns the error.
    pub(crate) async fn state_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        task_id: &str,
        statement: &ConditionStatement,
    ) -> Result<bool> {
        match self
            .db
            .state_integration_condition_in_tx(tx, task_id, statement)
            .await
        {
            Ok(()) => Ok(true),
            Err(db::DbError::Check(reason)) => {
                tracing::warn!(target: "services::integration_steps", task_id, %reason, "integration statement refused");
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }

    // ----- park ----------------------------------------------------------

    /// Level-triggered: states what the attempt waits on *now*. It may arrive
    /// for a parked or quarantined attempt, or for a `queued` member of a
    /// suspended queue; for a `queued` member of an open queue the wait is
    /// over and `waiting` is restated.
    async fn park(&self, step: &TaskStep, request: &IntegrationStepRequest) -> Result<StepOutcome> {
        let Some(attempt) = self.db.integration_attempt(&request.attempt_id).await? else {
            return Ok(StepOutcome::Skip("attempt is gone".into()));
        };
        let queue = match attempt.queue_id.as_deref() {
            Some(id) => self.db.integration_queue(id).await?,
            None => None,
        };
        let project_paused: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM task t JOIN project p ON p.id=t.project_id WHERE t.id=? AND p.paused_at IS NOT NULL)",
        )
        .bind(&attempt.task_ref)
        .fetch_one(self.db.pool())
        .await?;
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        let attempt = db::integration_attempt_in_tx(&mut tx, &request.attempt_id).await?;
        match db::integration_step_position(
            &attempt,
            request.generation,
            request.effect_seq,
            false,
            IntegrationStepAction::Park.as_str(),
            &[S::Parked, S::Quarantined, S::Queued],
        ) {
            IntegrationStepPosition::Answered => {
                return Ok(StepOutcome::Skip("already answered".into()))
            }
            IntegrationStepPosition::Stale => {
                return Ok(StepOutcome::Skip("the attempt moved on".into()))
            }
            IntegrationStepPosition::NotYet => {
                return Ok(StepOutcome::Retry("attempt is not parked yet".into()))
            }
            IntegrationStepPosition::Due => {}
        }
        if !Self::entry_live_in_tx(&mut tx, &attempt).await? {
            return Ok(StepOutcome::TaskLeft);
        }
        let queue_kind = queue.as_ref().and_then(|queue| queue.last_error_kind);
        let queue_closed = queue.as_ref().is_some_and(|queue| {
            matches!(
                queue.state,
                IntegrationQueueState::Suspended | IntegrationQueueState::Quarantined
            )
        });
        let reason = if attempt.state == S::Queued && !queue_closed {
            // The queue is open again: the member simply waits its turn.
            Some(IntegrationReason::Waiting {
                attempt_id: attempt_ref(&attempt.id),
            })
        } else if project_paused {
            // The pause is the Task's reason; integration adds none.
            None
        } else {
            let (kind, message) = if attempt.state == S::Queued {
                (
                    queue_kind,
                    queue.as_ref().and_then(|queue| queue.last_error.clone()),
                )
            } else {
                (attempt.failure_kind, attempt.failure_message.clone())
            };
            let message = message.unwrap_or_else(|| "integration is waiting".into());
            let cause = if attempt.state == S::Quarantined {
                IntegrationDeferralCause::UnresolvedResult
            } else if message.starts_with("target_dirty") {
                IntegrationDeferralCause::TargetDirty
            } else {
                match kind {
                    Some(
                        IntegrationFailureKind::Infrastructure | IntegrationFailureKind::Timeout,
                    ) => IntegrationDeferralCause::Infrastructure,
                    Some(IntegrationFailureKind::TargetUnavailable) => {
                        IntegrationDeferralCause::OwnerOffline
                    }
                    Some(IntegrationFailureKind::NeedsFact) => {
                        IntegrationDeferralCause::UnresolvedResult
                    }
                    _ => IntegrationDeferralCause::OwnerRequired,
                }
            };
            Some(IntegrationReason::Deferred {
                attempt_id: attempt_ref(&attempt.id),
                cause,
                owner_id: None,
                message: bounded(&message),
            })
        };
        if let Some(reason) = reason {
            let intervention = reason.requires_intervention();
            let stated = self
                .state_in_tx(
                    &mut tx,
                    &attempt.task_ref,
                    &ConditionStatement::Integration { reason },
                )
                .await?;
            if stated && intervention {
                // A statement alone writes no event: the incident is raised
                // here, with the condition, once per wait.
                let task = self
                    .db
                    .get_task_in_tx(&mut tx, &attempt.task_ref)
                    .await?
                    .ok_or(db::DbError::NotFound)?;
                let event = db::CreateDomainEvent::task_interruption_changed(&task);
                db::DomainEventRepo::append_event_in_tx(&*self.db, &mut tx, &event).await?;
            }
        }
        db::acknowledge_integration_step_in_tx(
            &mut tx,
            &IntegrationStepAckWrite {
                attempt_id: attempt.id.clone(),
                generation: request.generation,
                effect_seq: request.effect_seq,
                bind_generation: false,
                states: vec![S::Parked, S::Quarantined, S::Queued],
                ack: ack_value(
                    request,
                    IntegrationStepAction::Park,
                    IntegrationStepOutcome::Done,
                    None,
                    None,
                ),
                permit: None,
                acknowledged_at: db::now_rfc3339(),
            },
        )
        .await?;
        self.db
            .finish_step_in_tx(&mut tx, step, "done", None)
            .await?;
        tx.commit().await?;
        Ok(StepOutcome::Settled)
    }

    // ----- clear ---------------------------------------------------------

    /// A cancelled attempt: the Task left `merging` without a result (or is
    /// about to be told so). Only this attempt's own statement is cleared.
    async fn clear(
        &self,
        step: &TaskStep,
        request: &IntegrationStepRequest,
    ) -> Result<StepOutcome> {
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        let attempt = match db::integration_attempt_in_tx(&mut tx, &request.attempt_id).await {
            Ok(attempt) => attempt,
            Err(db::DbError::NotFound) => return Ok(StepOutcome::Skip("attempt is gone".into())),
            Err(error) => return Err(error.into()),
        };
        if attempt.state != S::Cancelled {
            return Ok(
                if attempt.state.terminal() || attempt.effect_seq != request.effect_seq {
                    StepOutcome::Skip("the attempt was not cancelled".into())
                } else {
                    StepOutcome::Retry("attempt is not cancelled yet".into())
                },
            );
        }
        let task = self
            .db
            .get_task_in_tx(&mut tx, &attempt.task_ref)
            .await?
            .filter(|task| task.deleted_at.is_none());
        let owned = task.as_ref().is_some_and(|task| {
            task.condition
                .integration_reason()
                .is_some_and(|reason| reason.attempt_id().as_str() == attempt.id)
        });
        if owned {
            self.state_in_tx(
                &mut tx,
                &attempt.task_ref,
                &ConditionStatement::IntegrationCleared {
                    attempt_id: attempt_ref(&attempt.id),
                },
            )
            .await?;
        }
        self.db
            .finish_step_in_tx(&mut tx, step, "done", None)
            .await?;
        tx.commit().await?;
        Ok(StepOutcome::Settled)
    }

    // ----- result --------------------------------------------------------

    /// Never returns an error and never settles the step `failed`: whatever
    /// goes wrong, the step stays pending and the Task says why.
    async fn result_step(&self, step: &TaskStep, request: &IntegrationStepRequest) -> Result<()> {
        let outcome = match self.result(step, request).await {
            Ok(outcome) => outcome,
            Err(error) => {
                tracing::warn!(target: "services::integration_steps", task_id = %step.task_id, attempt_id = %request.attempt_id, %error, "integration result could not be applied; it stays queued");
                self.park_result(step, request, &error.to_string()).await;
                StepOutcome::RetryAt(
                    bounded(&error.to_string()),
                    chrono::Utc::now() + chrono::Duration::seconds(RESULT_PARK_SECONDS),
                )
            }
        };
        if let Err(error) = self.conclude(step, outcome).await {
            // The lease recovers a step whose settlement was lost.
            tracing::warn!(target: "services::integration_steps", task_id = %step.task_id, attempt_id = %request.attempt_id, %error, "integration result step was not settled; its lease recovers it");
        }
        Ok(())
    }

    /// The Task says the merge landed but could not be recorded.
    async fn park_result(&self, step: &TaskStep, request: &IntegrationStepRequest, error: &str) {
        let stated = async {
            let mut tx = db::begin_immediate(self.db.pool()).await?;
            self.state_in_tx(
                &mut tx,
                &step.task_id,
                &ConditionStatement::Integration {
                    reason: IntegrationReason::Deferred {
                        attempt_id: attempt_ref(&request.attempt_id),
                        cause: IntegrationDeferralCause::UnresolvedResult,
                        owner_id: None,
                        message: bounded(&format!(
                            "the merge landed but its result could not be applied: {error}"
                        )),
                    },
                },
            )
            .await?;
            tx.commit().await?;
            Ok::<_, ServiceError>(())
        }
        .await;
        if let Err(error) = stated {
            tracing::warn!(target: "services::integration_steps", task_id = %step.task_id, %error, "integration result park was not stated");
        }
    }

    async fn result(
        &self,
        step: &TaskStep,
        request: &IntegrationStepRequest,
    ) -> Result<StepOutcome> {
        let Some(attempt) = self.db.integration_attempt(&request.attempt_id).await? else {
            return Ok(StepOutcome::Skip("attempt is gone".into()));
        };
        let action = IntegrationStepAction::Result;
        if IntegrationStepAck::current(&attempt, action).is_some()
            && attempt.effect_seq == request.effect_seq
        {
            return Ok(StepOutcome::Skip("already answered".into()));
        }
        if attempt.effect_seq != request.effect_seq || attempt.state.terminal() {
            // Another round took over (a new permit pre-enqueues its own
            // result step), or the attempt ended without this permit.
            return Ok(StepOutcome::Skip("the permit round ended".into()));
        }
        match attempt.state {
            S::Applied => {}
            S::FfInflight | S::Reconciling | S::Quarantined => {
                return Ok(StepOutcome::RetryAt(
                    format!("fast-forward {}", attempt.state),
                    chrono::Utc::now() + chrono::Duration::seconds(RESULT_POLL_SECONDS),
                ));
            }
            S::AwaitingTaskStep | S::ReadyFf => {
                // Woken by its own deadline with the permit unused. Take the
                // permit back (a compare-and-set against the worker's commit
                // to `ff_inflight`) so a waiting owner command can run.
                let mut tx = db::begin_immediate(self.db.pool()).await?;
                let revoked = db::revoke_integration_permit_in_tx(
                    &mut tx,
                    &attempt.id,
                    request.effect_seq,
                    &db::now_rfc3339(),
                )
                .await?;
                if !revoked {
                    drop(tx);
                    let now = self.db.integration_attempt(&attempt.id).await?;
                    if now.is_some_and(|now| {
                        now.effect_seq == request.effect_seq
                            && matches!(now.state, S::FfInflight | S::Reconciling | S::Applied)
                    }) {
                        return Ok(StepOutcome::Retry("fast-forward committed".into()));
                    }
                    return Ok(StepOutcome::Skip("the permit was not used".into()));
                }
                self.db
                    .finish_step_in_tx(&mut tx, step, "done", Some("permit expired unused"))
                    .await?;
                tx.commit().await?;
                return Ok(StepOutcome::Settled);
            }
            _ => return Ok(StepOutcome::Skip("the permit was dropped".into())),
        }

        #[cfg(test)]
        if test_faults::fails(&attempt.task_ref) {
            return Err(ServiceError::invalid_operation("injected result failure"));
        }
        let task = TaskRepo::get_by_id(&*self.db, &attempt.task_ref, false).await?;
        let live: bool = sqlx::query_scalar(db::STEP_FENCE)
            .bind(&attempt.task_ref)
            .bind(&attempt.expected_status)
            .bind(attempt.expected_epoch)
            .fetch_one(self.db.pool())
            .await?;
        let ack = |outcome| IntegrationStepAckWrite {
            attempt_id: attempt.id.clone(),
            generation: request.generation,
            effect_seq: request.effect_seq,
            bind_generation: false,
            states: vec![S::Applied],
            ack: ack_value(request, action, outcome, None, None),
            permit: None,
            acknowledged_at: db::now_rfc3339(),
        };
        let Some(task) = task.filter(|_| live) else {
            // The Task is gone from `merging` (deleted, moved by hand). The
            // Git result stays; the worker is told so it can release.
            let mut tx = db::begin_immediate(self.db.pool()).await?;
            db::acknowledge_integration_step_in_tx(&mut tx, &ack(IntegrationStepOutcome::TaskLeft))
                .await?;
            self.db
                .finish_step_in_tx(&mut tx, step, "done", Some("Task left merging"))
                .await?;
            tx.commit().await?;
            return Ok(StepOutcome::Settled);
        };
        let project = db::ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let queue = match attempt.queue_id.as_deref() {
            Some(id) => self.db.integration_queue(id).await?,
            None => None,
        };
        let branch = queue
            .map(|queue| queue.target_branch)
            .unwrap_or_else(|| "the target branch".into());
        let after_sha = attempt
            .integrated_sha
            .clone()
            .or_else(|| attempt.candidate_sha.clone())
            .unwrap_or_default();
        // What today's merge success leaves behind, in today's order. Both
        // writes are idempotent, so a redelivered step repeats neither.
        if let (Some(merge_service), Some(execution)) = (
            self.task_service.merge_service.as_ref(),
            crate::task_service::latest_executor_execution_for_task(&self.db, &task).await?,
        ) {
            merge_service
                .record_merge_execution_evidence(
                    &execution.id,
                    crate::integration_effects::MergeExecutionEvidence {
                        before_sha: attempt.candidate_sha.clone(),
                        after_sha: Some(after_sha.clone()),
                    },
                )
                .await?;
        }
        crate::workflow::actions::system_comment(
            &self.db,
            &self.engine.event_bus,
            &task.project_id,
            &task.id,
            format!("Changes merged to {branch} (SHA: {after_sha})"),
            Some(format!(
                "integration-result:{}:{}",
                attempt.id, request.effect_seq
            )),
        )
        .await?;
        let task = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or(db::DbError::NotFound)?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &crate::worker_runtime::queue::cascade_actor(),
        );
        let cascade = self
            .engine
            .bind(&self.task_service)
            .cascade_step_input(
                &task,
                &workflow,
                crate::workflow::default_states::DONE.to_owned(),
                "merge succeeded".to_owned(),
                Default::default(),
                false,
                false,
                Some(WorkflowAuthority {
                    project_version: project.version,
                    workflow_definition: project.workflow_definition.clone(),
                    clear_review_passed_at_on_commit: false,
                }),
                Some(step),
                format!("{}:cascade", step.causation_key),
                Some(attempt.expected_epoch),
            )
            .await?;

        let mut tx = db::begin_immediate(self.db.pool()).await?;
        if !Self::entry_live_in_tx(&mut tx, &attempt).await? {
            return Ok(StepOutcome::Retry("Task moved while applying".into()));
        }
        let task = self
            .db
            .get_task_in_tx(&mut tx, &task.id)
            .await?
            .ok_or(db::DbError::NotFound)?;
        // Only a durable successful merge consumes the paused-integration
        // marker (the same write the merge hook's settlement performs).
        if crate::deferred_dispatch::paused_integration(&task)
            .is_some_and(|marker| marker.state == attempt.expected_status)
        {
            sqlx::query("UPDATE task SET metadata_json=json_set(json_remove(COALESCE(metadata_json,'{}'),'$.paused_integration'),'$.paused_integration_generation',COALESCE(json_extract(metadata_json,'$.paused_integration_generation'),0)+1),updated_at=? WHERE id=? AND version=?")
                .bind(db::now_rfc3339()).bind(&task.id).bind(task.version).execute(&mut *tx).await?;
            self.db
                .state_condition_in_tx(&mut tx, &task.id, db::ConditionChange::Legacy)
                .await?;
        }
        self.state_in_tx(
            &mut tx,
            &task.id,
            &ConditionStatement::Integration {
                reason: IntegrationReason::Applied {
                    attempt_id: attempt_ref(&attempt.id),
                },
            },
        )
        .await?;
        match db::acknowledge_integration_step_in_tx(&mut tx, &ack(IntegrationStepOutcome::Done))
            .await?
        {
            IntegrationStepAckOutcome::Written(_)
            | IntegrationStepAckOutcome::AlreadyAcknowledged(_) => {}
            IntegrationStepAckOutcome::Stale(_) | IntegrationStepAckOutcome::NotYet(_) => {
                return Ok(StepOutcome::Retry("attempt moved while applying".into()));
            }
        }
        self.db
            .finish_step_in_tx(&mut tx, step, "done", None)
            .await?;
        let cascade_id = self.db.enqueue_step_in_tx(&mut tx, &cascade).await?;
        // As the merge hook does for its own follow-up: the landed result
        // goes ahead of a waiting owner command.
        sqlx::query("UPDATE task_step SET priority=2 WHERE id=?")
            .bind(&cascade_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        self.task_service.dispatch_wake.notify_one();
        Ok(StepOutcome::Settled)
    }

    /// The phase a `request_check` step states.
    pub(crate) fn owned(attempt_id: &str, phase: IntegrationPhase) -> ConditionStatement {
        ConditionStatement::Integration {
            reason: IntegrationReason::Owned {
                attempt_id: attempt_ref(attempt_id),
                phase,
            },
        }
    }
}

/// Test-only: Tasks whose `result` step fails to apply while listed.
#[cfg(test)]
pub(crate) mod test_faults {
    use std::{collections::HashSet, sync::Mutex};
    static FAILING: Mutex<Option<HashSet<String>>> = Mutex::new(None);
    pub(crate) fn set(task_id: &str, failing: bool) {
        let mut tasks = FAILING.lock().unwrap();
        let tasks = tasks.get_or_insert_with(HashSet::new);
        if failing {
            tasks.insert(task_id.to_owned());
        } else {
            tasks.remove(task_id);
        }
    }
    pub(super) fn fails(task_id: &str) -> bool {
        FAILING
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|tasks| tasks.contains(task_id))
    }
}
