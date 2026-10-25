//! The Task-step consumer of the integration queue (plan 3.2 stage D1b).
//!
//! The queue worker (`integration_worker`) never writes Task state. Each of
//! its requests arrives here as one `integration` Task step and is answered by
//! an acknowledgment (and, for `settle`, a permit) written on the attempt in
//! the same transaction as the step's Task write. This is the only path a
//! merge takes.
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
//! | `result` | `applied` | asks the target's owner whether the target contains the merged commit (`proof`); then execution evidence, "Changes merged" comment, states `applied`, enqueues the `done` cascade. Not contained: nothing is reported, the Task states `deferred/unresolved_result` | `result` ack (`done`, or `not_landed`: the worker takes the attempt off the slot and queues it again) |
//! | `send_back` | `ejected`, `needs_review` | today's conflict handoff / merge failure / review refresh, states the typed reason and the handoff | `send_back` ack |
//! | `park` | `parked`, `quarantined`, or `queued` | states `deferred` (or `waiting` once the queue is open again), `task.interruption_changed` for an intervention | `park` ack |
//! | `clear` | `cancelled` | clears this attempt's integration statement | none (the attempt is terminal) |
//! | `import_continue` | any imported state | the continuation of a merge that was in flight before the queue was active (`import`) | none |
//!
//! Admission (`admission`) is not a step of its own: it is the `run_merge`
//! entry hook of the merge state, which creates the attempt and states
//! `waiting`.
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
#[cfg(test)]
use db::IntegrationFailureKind;
use db::{
    ConditionStatement, IntegrationAttempt, IntegrationAttemptState as S, IntegrationQueueRepo,
    IntegrationQueueState, IntegrationStepAckOutcome, IntegrationStepAckWrite,
    IntegrationStepPosition, SqliteDb, TaskRepo, TaskStep, TaskStepRepo,
};
use std::sync::Arc;

pub(crate) mod admission;
#[cfg(test)]
mod condition_tests;
pub(crate) mod deferral;
pub(crate) mod gate;
pub mod import;
#[cfg(test)]
mod offline_retry_tests;
mod port;
#[cfg(test)]
pub(crate) mod preemption_tests;
mod proof;
pub(crate) mod reconcile;
mod send_back;
mod settle;
pub(crate) use settle::check_authority;
#[cfg(test)]
mod tests;

pub use port::TaskStepIntegrationPort;
pub use proof::{TargetOwnerProof, TargetProof, TargetProofSource};
pub use settle::{HeadCheck, HeadCheckSource, IntegrationCheckFamily, ReviewCiHeadCheck};

/// The Task-step kind this module consumes.
pub const INTEGRATION_STEP_KIND: &str = "integration";

/// The timers of the Task-step side of the handshake. They come from the
/// queue worker's configuration (`IntegrationWorkerConfig`), so both sides
/// are tuned in one place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntegrationStepTimers {
    /// How long the protected `result` step sleeps before it looks for
    /// itself. The worker readies it the moment the fast-forward is
    /// recorded; this only bounds how long an unused permit keeps a waiting
    /// owner command queued.
    pub result_wake: chrono::Duration,
    /// The protected step's poll while a fast-forward is in flight or unknown.
    pub result_poll: chrono::Duration,
    /// A `result` that cannot be applied waits this long; the worker re-arms
    /// it on every sweep (`ready_result_step`).
    pub result_park: chrono::Duration,
}
impl From<&crate::integration_worker::IntegrationWorkerConfig> for IntegrationStepTimers {
    fn from(config: &crate::integration_worker::IntegrationWorkerConfig) -> Self {
        let chrono = |duration: std::time::Duration| {
            chrono::Duration::from_std(duration).unwrap_or_else(|_| chrono::Duration::days(1))
        };
        Self {
            result_wake: chrono(config.result_wake),
            result_poll: chrono(config.result_poll),
            result_park: chrono(config.result_park),
        }
    }
}
impl Default for IntegrationStepTimers {
    fn default() -> Self {
        Self::from(&crate::integration_worker::IntegrationWorkerConfig::default())
    }
}
/// Leads the message of a head that could not identify its check because the
/// owner's environment was not attested.
pub(crate) const CHECK_ENVIRONMENT_UNAVAILABLE: &str = "check environment unavailable";
const TEXT_LIMIT: usize = 1000;
const CI_MESSAGE_LIMIT: usize = 12 * 1024;
pub(crate) fn bounded_ci_failure(text: &str) -> String {
    let mut end = text.len().min(CI_MESSAGE_LIMIT);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}
/// Leads the retry reason of a `result` step that found its fast-forward
/// still in flight.
const FF_POLL: &str = "fast-forward ";

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
        message: message.as_deref().map(|message| {
            if outcome == IntegrationStepOutcome::CandidateCheckFailed {
                bounded_ci_failure(message)
            } else {
                bounded(message)
            }
        }),
    })
}

/// Executes `integration` Task steps. One per process, built from the
/// service the step worker runs under.
pub struct IntegrationSteps {
    pub(crate) db: Arc<SqliteDb>,
    pub(crate) task_service: crate::TaskService,
    pub(crate) engine: Arc<WorkflowEngine>,
    pub(crate) head_check: Arc<dyn HeadCheckSource>,
    /// Asked before a Task is reported merged: the target's owner says
    /// whether the target contains the commit.
    pub(crate) target_proof: Arc<dyn TargetProofSource>,
    pub(crate) timers: IntegrationStepTimers,
}

/// How often in a row the continuation of an imported, landed merge gets no
/// answer about its commit, from an owner that will not come back by itself
/// (a removed machine, a checkout that is gone, a connected machine that
/// keeps failing the read), before it parks the attempt for the Task's
/// owner. An owner machine that is merely not connected is waited for and
/// not counted (`proof::target_owner_away`).
pub(crate) const IMPORT_PROOF_ASKS: i64 = 5;
/// The same bound when the owner answered that the target does not contain
/// the commit: the second answer confirms the first.
pub(crate) const IMPORT_ABSENT_ASKS: i64 = 2;
/// The asks so far are carried on the continuation step itself, as the end
/// of its retry reason (`task_step.last_error`): `... [asked N, answered]`
/// or `... [asked N, no answer]`. A step whose last retry was for anything
/// else (it waited for the entry's hooks, an error, a build before this
/// counter) starts at one, so a row is never parked on runs that asked
/// nothing.
fn import_asks(step: &TaskStep, answered: bool) -> i64 {
    let kind = if answered { "answered" } else { "no answer" };
    step.last_error
        .as_deref()
        .and_then(|last| last.strip_suffix(&format!(", {kind}]")))
        .and_then(|last| last.rsplit_once(" [asked "))
        .and_then(|(_, asked)| asked.parse::<i64>().ok())
        .map_or(1, |asked| asked.saturating_add(1))
}

impl IntegrationSteps {
    pub fn new(task_service: crate::TaskService) -> Self {
        let engine = task_service.workflow_engine();
        Self {
            db: Arc::clone(&engine.db),
            head_check: Arc::new(ReviewCiHeadCheck::new(Arc::clone(&engine.db))),
            target_proof: Arc::new(TargetOwnerProof::new(
                Arc::clone(&engine.db),
                task_service.daemon_connections.clone(),
            )),
            engine,
            task_service,
            timers: IntegrationStepTimers::default(),
        }
    }
    /// Use the timers of the queue worker's configuration (the same
    /// `IntegrationWorkerConfig` the worker is built with).
    pub fn with_timers(mut self, timers: IntegrationStepTimers) -> Self {
        self.timers = timers;
        self
    }
    /// Replace how the head's check identity is built (tests, and a later
    /// stage that shares the review-entry builder).
    pub fn with_head_check(mut self, source: Arc<dyn HeadCheckSource>) -> Self {
        self.head_check = source;
        self
    }
    /// Replace who is asked whether the target contains a merged commit
    /// (tests).
    pub fn with_target_proof(mut self, source: Arc<dyn TargetProofSource>) -> Self {
        self.target_proof = source;
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
            let applied = self.result_step(step, &request).await;
            self.wake_worker();
            return applied;
        }
        if request.action == IntegrationStepAction::ImportContinue {
            // Enqueued once per imported attempt: it never settles failed.
            let continued = self.import_continue_step(step, &request).await;
            self.wake_worker();
            return continued;
        }
        let outcome = match request.action {
            IntegrationStepAction::RequestCheck => match self.request_check(step, &request).await {
                Err(error) if error.is_check_environment_unavailable() => {
                    self.environment_unavailable(step, &request, &[S::Checking], error)
                        .await?
                }
                outcome => outcome?,
            },
            IntegrationStepAction::Settle => match match self.reuse_review_check(&request).await {
                Err(error) if error.is_check_environment_unavailable() => {
                    let outcome = self
                        .environment_unavailable(step, &request, &[S::AwaitingTaskStep], error)
                        .await?;
                    let concluded = self.conclude(step, outcome).await;
                    self.wake_worker();
                    return concluded;
                }
                reuse => reuse?,
            } {
                settle::HeadReuse::Early => StepOutcome::Retry("attempt is not waiting yet".into()),
                reuse => {
                    let evidence = match reuse {
                        settle::HeadReuse::Found(application) => Some(application),
                        _ => None,
                    };
                    let check = if evidence.is_some() {
                        settle::CheckInput::Passed(db::IntegrationCheckTiming::Skipped {
                            reason: db::IntegrationCiSkipReason::TargetUnchanged,
                        })
                    } else {
                        settle::CheckInput::NotRequested
                    };
                    self.settle(
                        Some(step),
                        &request,
                        &[S::AwaitingTaskStep],
                        check,
                        evidence.as_deref(),
                    )
                    .await?
                }
            },
            IntegrationStepAction::SendBack => self.send_back(step, &request).await?,
            IntegrationStepAction::Park => self.park(step, &request).await?,
            IntegrationStepAction::Clear => self.clear(step, &request).await?,
            IntegrationStepAction::Result | IntegrationStepAction::ImportContinue => {
                unreachable!("handled above")
            }
        };
        let concluded = self.conclude(step, outcome).await;
        self.wake_worker();
        concluded
    }

    /// The owner's login shell gave no environment, so the head's check has
    /// no identity and nothing was asked. The first attempts are the step's
    /// own (the same bound as review entry, `ENVIRONMENT_ATTEMPTS`: about
    /// seven seconds of back-off). Then the step answers "no verdict" with
    /// the cause: the worker parks the attempt as an infrastructure wait
    /// that names it, retries on its counted timer and, once that is spent,
    /// waits for the owner's Retry. Letting the error spend every step
    /// attempt and every re-ask instead parked the attempt as
    /// `task_step_failed`, which names the step and not the cause.
    async fn environment_unavailable(
        &self,
        step: &TaskStep,
        request: &IntegrationStepRequest,
        states: &[S],
        error: ServiceError,
    ) -> Result<StepOutcome> {
        if step.attempts < crate::worker_runtime::queue::ENVIRONMENT_ATTEMPTS {
            return Err(error);
        }
        tracing::warn!(target: "services::integration_steps", task_id = %step.task_id, attempt_id = %request.attempt_id, %error, "the head's check has no identity; the attempt waits for the environment");
        self.settle(
            Some(step),
            request,
            states,
            settle::CheckInput::NoVerdict(format!(
                "{CHECK_ENVIRONMENT_UNAVAILABLE}: the owner's login shell did not report its environment, so the head's check could not be identified"
            )),
            None,
        )
        .await
    }

    /// An answer may have been written on the attempt: its driver re-reads
    /// now instead of at its next poll.
    pub(crate) fn wake_worker(&self) {
        if let Some(worker) = self.task_service.integration_worker() {
            worker.notify_answered();
        }
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
        Ok(db::integration_task_entry_matches_in_tx(
            tx,
            &attempt.task_ref,
            &attempt.expected_status,
            attempt.expected_epoch,
        )
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

    /// Raise the incident of an integration wait that needs its owner, with
    /// the statement. A Task that already has an interruption event for its
    /// present version (the owner's Hold, a block: the reason integration
    /// waits behind) has its incident; a second one is not an error.
    pub(crate) async fn raise_interruption_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        task: &db::Task,
    ) -> Result<()> {
        let event = db::CreateDomainEvent::task_interruption_changed(task);
        match db::DomainEventRepo::append_event_in_tx(&*self.db, tx, &event).await {
            Ok(_) => Ok(()),
            Err(db::DbError::Check(reason)) => {
                tracing::debug!(target: "services::integration_steps", task_id = %task.id, %reason, "the Task already has its interruption event");
                Ok(())
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
        let project_paused: bool = self
            .db
            .integration_task_project_paused(&attempt.task_ref)
            .await?;
        // The machine a pre-queue owner wait named (importer row P15).
        let waited_owner: Option<String> = self
            .db
            .integration_task_waited_owner(&attempt.task_ref)
            .await?
            .flatten();
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
        } else if project_paused
            || (attempt.state == S::Parked
                && attempt
                    .failure_message
                    .as_deref()
                    .is_some_and(crate::integration_worker::GateCause::parked_under))
        {
            // Parked under the Task gate: the Task's own stop (a Hold, a
            // block, a failure, a blocked entry, a wait for a person) is its
            // reason and carries the action that lifts it; a paused Project
            // is resumed on the Project. Integration says only that the Task
            // is in line behind it. A `deferred` here would take the place
            // of that reason and offer a Retry of the merge, which cannot
            // lift a stop on the Task; saying nothing would leave a Task
            // that was never told it is in line (one taken over from before
            // the queue) with no reason at all while its Project is paused.
            Some(IntegrationReason::Waiting {
                attempt_id: attempt_ref(&attempt.id),
            })
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
            // One rule for every writer of this row (`deferral`). The
            // suspended queue of a `queued` member is the queue's fact: the
            // importer's classification of the row does not apply to it.
            let (cause, owner_id) = deferral::deferral(&deferral::DeferralFacts {
                quarantined: attempt.state == S::Quarantined,
                kind,
                message: &message,
                daemon_id: attempt.daemon_id.as_deref(),
                import: (attempt.state != S::Queued)
                    .then(|| deferral::import_facts(&attempt, waited_owner.clone()))
                    .flatten(),
            });
            Some(IntegrationReason::Deferred {
                attempt_id: attempt_ref(&attempt.id),
                cause,
                owner_id,
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
                self.raise_interruption_in_tx(&mut tx, &task).await?;
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
        let outcome = match self.result(step, request, false).await {
            Ok(outcome) => outcome,
            Err(error) => {
                tracing::warn!(target: "services::integration_steps", task_id = %step.task_id, attempt_id = %request.attempt_id, %error, "integration result could not be applied; it stays queued");
                self.park_result(step, request, &error.to_string()).await;
                StepOutcome::RetryAt(
                    bounded(&error.to_string()),
                    chrono::Utc::now() + self.timers.result_park,
                )
            }
        };
        // A poll: the step ran while the fast-forward was still in flight.
        let polled =
            matches!(&outcome, StepOutcome::RetryAt(reason, _) if reason.starts_with(FF_POLL));
        if let Err(error) = self.conclude(step, outcome).await {
            // The lease recovers a step whose settlement was lost.
            tracing::warn!(target: "services::integration_steps", task_id = %step.task_id, attempt_id = %request.attempt_id, %error, "integration result step was not settled; its lease recovers it");
        }
        if polled {
            // The worker records `applied` and readies this step in one go.
            // If that happened while this run held the step, the wake found
            // nothing pending and the retry above then pushed the step a
            // whole poll into the future: look once more, now that the step
            // is pending again.
            let applied = self
                .db
                .integration_attempt(&request.attempt_id)
                .await
                .ok()
                .flatten()
                .is_some_and(|attempt| {
                    attempt.state == S::Applied && attempt.effect_seq == request.effect_seq
                });
            if applied {
                if let Err(error) = self
                    .db
                    .ready_integration_step(&step.task_id, &request.causation_key())
                    .await
                {
                    tracing::warn!(target: "services::integration_steps", task_id = %step.task_id, %error, "integration result step was not re-armed; the worker's sweep re-arms it");
                }
            }
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

    /// `imported`: the attempt is a landed merge of before the queue, reported
    /// by its import continuation. No worker holds a slot for it, so nothing
    /// would read a `not_landed` answer: an unproven one is asked again.
    pub(crate) async fn result(
        &self,
        step: &TaskStep,
        request: &IntegrationStepRequest,
        imported: bool,
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
                    format!("{FF_POLL}{}", attempt.state),
                    chrono::Utc::now() + self.timers.result_poll,
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
        let live: bool = self
            .db
            .integration_task_entry_matches(
                &attempt.task_ref,
                &attempt.expected_status,
                attempt.expected_epoch,
            )
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
        let queue = match attempt.queue_id.as_deref() {
            Some(id) => self.db.integration_queue(id).await?,
            None => None,
        };
        // The proof. Nothing below is written, and the Task is not reported
        // merged, unless the owner of the target says now that the target
        // contains the commit. One read on that owner, outside of any
        // transaction.
        let after_sha = match self.prove_landed(&attempt, queue.as_ref()).await {
            Ok(sha) => sha,
            Err(unproven) => {
                return self
                    .result_unproven(step, request, &attempt, queue.as_ref(), unproven, imported)
                    .await
            }
        };
        let project = db::ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let branch = queue
            .map(|queue| queue.target_branch)
            .unwrap_or_else(|| "the target branch".into());
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
                // The state the merge state's `accept` leads to: `done`, or
                // whatever a workflow of the Project's own calls it.
                reconcile::role_target(
                    &workflow,
                    &attempt.expected_status,
                    crate::workflow::default_states::DONE,
                ),
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
            self.db
                .consume_applied_task_integration_pause_in_tx(
                    &mut tx,
                    db::now_rfc3339(),
                    &task.id,
                    task.version,
                )
                .await?;
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
        self.db
            .prioritize_applied_integration_cascade_in_tx(&mut tx, &cascade_id)
            .await?;
        tx.commit().await?;
        self.task_service.dispatch_wake.notify_one();
        Ok(StepOutcome::Settled)
    }

    /// The commit the target's owner confirms the target contains, or why
    /// the attempt's merge is not proven.
    async fn prove_landed(
        &self,
        attempt: &IntegrationAttempt,
        queue: Option<&db::IntegrationQueue>,
    ) -> std::result::Result<String, TargetProof> {
        let Some(queue) = queue else {
            return Err(TargetProof::Unknown(
                "the attempt has no queue, so no target to ask".into(),
            ));
        };
        let integrated = proof::recorded_commit(attempt, queue).map_err(TargetProof::Absent)?;
        // The Task's own commit. After a fast-forward it is the integrated
        // commit itself (one read). A merge of before the queue made a merge
        // commit: then both are asked for.
        let candidate = attempt
            .candidate_sha
            .clone()
            .filter(|sha| *sha != integrated);
        for sha in std::iter::once(integrated.clone()).chain(candidate) {
            match self.target_proof.target_contains(queue, &sha).await {
                TargetProof::Contains => {}
                unproven => return Err(unproven),
            }
        }
        Ok(integrated)
    }

    /// The merge the attempt records is not proven. The Task is not reported
    /// merged and says so, typed (`deferred/unresolved_result`).
    ///
    /// - The owner answered "not contained" (the target was rewound, or the
    ///   row names a commit that never merged): the step answers `not_landed`
    ///   and ends, so the worker takes the attempt off the slot and queues it
    ///   again; validation then reads the target as it is.
    /// - No answer, or an imported row: the step stays and asks again at
    ///   `result_park`; the worker re-arms it on every sweep. An imported
    ///   row has no worker: its continuation parks the attempt for the owner
    ///   (`park_unproven_import`) at the second "not contained"
    ///   ([`IMPORT_ABSENT_ASKS`]) or after [`IMPORT_PROOF_ASKS`] asks nobody
    ///   answered, and waits without a bound for a target owner machine that
    ///   is only not connected.
    async fn result_unproven(
        &self,
        step: &TaskStep,
        request: &IntegrationStepRequest,
        attempt: &IntegrationAttempt,
        queue: Option<&db::IntegrationQueue>,
        unproven: TargetProof,
        imported: bool,
    ) -> Result<StepOutcome> {
        let (answered, why) = match unproven {
            TargetProof::Absent(why) => (true, why),
            TargetProof::Unknown(why) => (false, why),
            TargetProof::Contains => unreachable!("a proven merge is reported"),
        };
        // An imported row has no worker to take it off a slot. Its asks end
        // with a park for the owner, but only on asks that could have been
        // answered: a target owner that is merely not connected is waited
        // for, however long (a park needs the owner to decide something, and
        // "the machine is offline" is not a decision).
        let mut asks = None;
        if imported {
            let away = match queue {
                Some(queue) if !answered => {
                    proof::target_owner_away(
                        &self.db,
                        self.task_service.daemon_connections.as_ref(),
                        queue,
                    )
                    .await?
                }
                _ => None,
            };
            if away.is_none() {
                let asked = import_asks(step, answered);
                let bound = if answered {
                    IMPORT_ABSENT_ASKS
                } else {
                    IMPORT_PROOF_ASKS
                };
                if asked >= bound {
                    return self.park_unproven_import(step, attempt, &why, asked).await;
                }
                asks = Some(asked);
            }
        }
        let message = bounded(&format!(
            "the merge of this Task is not confirmed on its target: {why}"
        ));
        tracing::warn!(target: "services::integration_steps", task_id = %attempt.task_ref, attempt_id = %attempt.id, %message, "integration result not reported");
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        if !Self::entry_live_in_tx(&mut tx, attempt).await? {
            return Ok(StepOutcome::Retry("Task moved while applying".into()));
        }
        let reason = IntegrationReason::Deferred {
            attempt_id: attempt_ref(&attempt.id),
            cause: IntegrationDeferralCause::UnresolvedResult,
            owner_id: None,
            message: message.clone(),
        };
        let already = self
            .db
            .get_task_in_tx(&mut tx, &attempt.task_ref)
            .await?
            .is_some_and(|task| task.condition.integration_reason() == Some(&reason));
        if !already
            && self
                .state_in_tx(
                    &mut tx,
                    &attempt.task_ref,
                    &ConditionStatement::Integration { reason },
                )
                .await?
        {
            let task = self
                .db
                .get_task_in_tx(&mut tx, &attempt.task_ref)
                .await?
                .ok_or(db::DbError::NotFound)?;
            self.raise_interruption_in_tx(&mut tx, &task).await?;
        }
        if !answered || imported {
            tx.commit().await?;
            let reason = match asks {
                Some(asked) => format!(
                    "{message} [asked {asked}, {}]",
                    if answered { "answered" } else { "no answer" }
                ),
                None => message,
            };
            return Ok(StepOutcome::RetryAt(
                reason,
                chrono::Utc::now() + self.timers.result_park,
            ));
        }
        match db::acknowledge_integration_step_in_tx(
            &mut tx,
            &IntegrationStepAckWrite {
                attempt_id: attempt.id.clone(),
                generation: request.generation,
                effect_seq: request.effect_seq,
                bind_generation: false,
                states: vec![S::Applied],
                ack: ack_value(
                    request,
                    IntegrationStepAction::Result,
                    IntegrationStepOutcome::NotLanded,
                    None,
                    Some(message.clone()),
                ),
                permit: None,
                acknowledged_at: db::now_rfc3339(),
            },
        )
        .await?
        {
            IntegrationStepAckOutcome::Written(_)
            | IntegrationStepAckOutcome::AlreadyAcknowledged(_) => {}
            IntegrationStepAckOutcome::Stale(_) | IntegrationStepAckOutcome::NotYet(_) => {
                return Ok(StepOutcome::Retry("attempt moved while applying".into()));
            }
        }
        self.db
            .finish_step_in_tx(&mut tx, step, "done", Some(&message))
            .await?;
        tx.commit().await?;
        Ok(StepOutcome::Settled)
    }

    /// An imported, landed merge is still not proven after its bound of asks
    /// (`result_unproven`). No worker holds a slot for such a row and
    /// nothing else would end the asking, so the continuation ends it: the
    /// attempt is parked for the Task's owner (`applied -> parked`,
    /// `owner_required`, no deadline, its unproven commit claim dropped as
    /// the worker's `not_landed` park does), the Task states
    /// `deferred/owner_required` (an Attention item) and the step is done.
    /// The owner's Retry puts the Task back in line, where validation reads
    /// the target as it is; Cancel ends it.
    async fn park_unproven_import(
        &self,
        step: &TaskStep,
        attempt: &IntegrationAttempt,
        why: &str,
        asked: i64,
    ) -> Result<StepOutcome> {
        let message = bounded(&format!(
            "the merge of this Task from before the upgrade is not confirmed on its target ({why}); asked {asked} times. Retry puts the Task back in line; Cancel ends it."
        ));
        tracing::warn!(target: "services::integration_steps", task_id = %attempt.task_ref, attempt_id = %attempt.id, %message, "imported merge parked for its owner");
        let mut parked = attempt.clone();
        parked.state = S::Parked;
        parked.failure_kind = Some(db::IntegrationFailureKind::OwnerRequired);
        parked.failure_message = Some(message.clone());
        parked.available_at = None;
        parked.integrated_sha = None;
        parked.integrated_before_sha = None;
        let parked = match self.db.transition_integration_attempt(parked).await {
            Ok(parked) => parked,
            Err(db::DbError::VersionConflict) => {
                return Ok(StepOutcome::Retry("attempt moved while parking".into()))
            }
            Err(error) => return Err(error.into()),
        };
        self.import_state(
            step,
            &parked,
            Some(IntegrationReason::Deferred {
                attempt_id: attempt_ref(&parked.id),
                cause: IntegrationDeferralCause::OwnerRequired,
                owner_id: None,
                message,
            }),
        )
        .await
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
    static INTERRUPTED: Mutex<Option<HashSet<String>>> = Mutex::new(None);
    /// The next `send_back` of this Task stops before its transaction, as a
    /// crash or a lost lease there would, and is delivered again.
    pub(crate) fn interrupt_once(task_id: &str) {
        INTERRUPTED
            .lock()
            .unwrap()
            .get_or_insert_with(HashSet::new)
            .insert(task_id.to_owned());
    }
    pub(super) fn interrupts_once(task_id: &str) -> bool {
        INTERRUPTED
            .lock()
            .unwrap()
            .as_mut()
            .is_some_and(|tasks| tasks.remove(task_id))
    }
    pub(super) fn fails(task_id: &str) -> bool {
        FAILING
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|tasks| tasks.contains(task_id))
    }
}
