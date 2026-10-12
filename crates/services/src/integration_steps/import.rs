//! Taking merges that were in flight the old way into the integration queue
//! (plan 3.2 stage D2b).
//!
//! At every start, before any worker runs:
//!
//! 1. [`import_at_start`] runs the storage importer
//!    (`SqliteDb::import_integration_pass`) until nothing is left. It only
//!    classifies and writes attempt rows.
//! 2. For every *current* attempt that came from the importer, or from the
//!    passive shadow recording that preceded the queue, and has no
//!    continuation step yet, it enqueues one identity-fenced
//!    `integration/import_continue` Task step, keyed
//!    `integration-import:<attempt id>`.
//!
//! The step ([`IntegrationSteps::import_continue`]) is the Task writer. It
//! re-reads the attempt and the Task under the Task's step lease and applies
//! the continuation of the attempt's imported state. It is idempotent three
//! ways: the importer's admission identity (`import:<task>:<epoch>`), the
//! step's causation key, and the compare-and-set of every write it makes. No
//! legacy column, transition row, checkpoint, Review, carry or budget row is
//! rewritten; the attempt's `import_source_json` keeps the evidence.
//!
//! What continues an imported attempt, by its state:
//!
//! | imported state | who continues it |
//! |---|---|
//! | `applied` (the merge landed and was recorded) | this step: evidence, comment, `done`, then the attempt is completed |
//! | `reconciling` (unknown result) | the queue worker's owner lookup; this step states the phase |
//! | `queued`, Task in a merge state | the queue worker; this step states `waiting` (a row without the owner fields is admitted again first) |
//! | any state, Task not in a merge state (repair, re-review, the old clean-rebase bounce) | the Task's own flow; nothing is written and the next merge entry supersedes the row |
//! | `parked` under a Project pause marker | this step clears the marker and admits the Task again; the worker holds it while the Project is paused |
//! | `parked` until the candidate is read (a rebase the old path started or finished, a stored review carry) | this step admits the Task again; a rebased commit is admitted against the commit its review passed, so the head checks it and the review is carried |
//! | `parked` with a deadline | the worker's sweep at the deadline; this step states the wait |
//! | `parked` / `quarantined` otherwise | the owner's existing action; this step states why |
//! | `ejected`, `needs_review` | the repair or review already in progress; nothing is written |
use super::{attempt_ref, bounded, IntegrationSteps, StepOutcome, TaskStepIntegrationPort};
use crate::{
    integration_worker::{IntegrationStepAction, IntegrationStepRequest},
    workflow::engine::WorkflowEngine,
    Result, ServiceError,
};
use api_types::{IntegrationDeferralCause, IntegrationPhase, IntegrationReason};
use db::{
    ConditionStatement, IntegrationAttempt, IntegrationAttemptState as S, IntegrationQueueRepo,
    SqliteDb, TaskRepo, TaskStep, TaskStepRepo,
};
use std::sync::Arc;

/// What one start-time import did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ImportReport {
    pub imported: u32,
    pub quarantined: u32,
    pub needs_fact: u32,
    /// Tasks the importer could not read at all; each is quarantined with
    /// the error and the pass continued.
    pub unreadable: u32,
    /// `import_continue` steps enqueued by this call.
    pub continuations: u32,
}

/// Current attempts nothing has continued yet: imported rows and shadow
/// rows, of Tasks that still exist, without their continuation step.
const UNCONTINUED: &str = "SELECT a.id FROM integration_attempt a \
    WHERE a.current=1 AND (a.import_source_json IS NOT NULL OR a.admission_key LIKE 'shadow:%') \
      AND EXISTS(SELECT 1 FROM task t WHERE t.id=a.task_ref AND t.deleted_at IS NULL) \
      AND NOT EXISTS(SELECT 1 FROM task_step s WHERE s.task_id=a.task_ref \
                     AND s.causation_key='integration-import:'||a.id) \
    ORDER BY a.id";

use super::reconcile::merge_state;

/// The machine a pre-queue owner wait named (`metadata.owner_wait.daemon_id`).
fn owner_wait(task: &db::Task) -> Option<String> {
    let metadata: serde_json::Value = serde_json::from_str(task.metadata_json.as_deref()?).ok()?;
    metadata["owner_wait"]["daemon_id"]
        .as_str()
        .map(str::to_owned)
}

fn import_priority(attempt: &IntegrationAttempt) -> Option<u64> {
    attempt
        .import_source_json
        .as_ref()
        .and_then(|source| source["priority"].as_u64())
}

/// The importer classified the row as "continues in the queue once a fact
/// only the Task's checkout can give is read": the candidate's present HEAD
/// (rows P4, P11, P12, P18), a rebase the old path started (P5) or finished
/// (P6), or a stored review carry (P9). Admission reads the HEAD and the
/// head's validation reads the rest: a stopped rebase parks typed, a
/// rebased commit is checked and its review carried, a carried commit is
/// matched against the newest carry.
fn needs_fact_to_queue(attempt: &IntegrationAttempt) -> bool {
    matches!(
        attempt.resume_state,
        Some(S::Queued | S::Rebasing | S::Checking | S::Validating)
    ) && attempt.failure_kind == Some(db::IntegrationFailureKind::NeedsFact)
}

/// Run the importer to its end and enqueue the continuation steps. Called
/// once per start, after owner reconciliation and before the Task-step worker
/// and the queue worker. Safe to call again at any time.
pub async fn import_at_start(db: &Arc<SqliteDb>) -> Result<ImportReport> {
    let mut report = ImportReport::default();
    complete_reported(db).await?;
    // Bounded: each pass commits at least one row or reports nothing left.
    for _ in 0..100_000 {
        let pass = db
            .import_integration_pass(db::INTEGRATION_IMPORT_PAGE)
            .await?;
        report.imported += pass.imported;
        report.quarantined += pass.quarantined;
        report.needs_fact += pass.needs_fact;
        report.unreadable += pass.unreadable;
        if !pass.remaining || pass.examined == 0 {
            break;
        }
    }
    let ids: Vec<String> = sqlx::query_scalar(UNCONTINUED)
        .fetch_all(db.pool())
        .await
        .map_err(db::DbError::from)?;
    for id in ids {
        match enqueue_continuation(db, &id).await {
            Ok(true) => report.continuations += 1,
            Ok(false) => {}
            // One unreadable row must not stop the others; the next start
            // lists it again.
            Err(error) => {
                tracing::warn!(target: "services::integration_steps", attempt_id = %id, %error, "integration import continuation was not enqueued");
            }
        }
    }
    Ok(report)
}

async fn enqueue_continuation(db: &Arc<SqliteDb>, attempt_id: &str) -> Result<bool> {
    let Some(attempt) = db.integration_attempt(attempt_id).await? else {
        return Ok(false);
    };
    let Some(task) = TaskRepo::get_by_id(&**db, &attempt.task_ref, false).await? else {
        return Ok(false);
    };
    let request = IntegrationStepRequest {
        task_id: attempt.task_ref.clone(),
        queue_id: attempt.queue_id.clone().unwrap_or_default(),
        attempt_id: attempt.id.clone(),
        expected_epoch: attempt.expected_epoch,
        generation: attempt.slot_generation,
        effect_seq: attempt.effect_seq,
        action: IntegrationStepAction::ImportContinue,
    };
    let input = TaskStepIntegrationPort::step_input(
        &request,
        &attempt.expected_status,
        task.version,
        None,
        db::now_rfc3339(),
    )?;
    let mut tx = db::begin_immediate(db.pool()).await?;
    let id = db.enqueue_step_in_tx(&mut tx, &input).await?;
    // Identity-fenced: a Task that changed status since the import still
    // gets its continuation, which then finds the entry gone and writes
    // nothing.
    db.mark_step_identity_fenced_in_tx(&mut tx, &id).await?;
    tx.commit().await?;
    db.domain_event_notify().notify_waiters();
    Ok(true)
}

impl IntegrationSteps {
    /// Never settles the step `failed`: an error leaves it pending with a
    /// back-off, as the protected `result` step does, because nothing
    /// enqueues a continuation twice.
    pub(crate) async fn import_continue_step(
        &self,
        step: &TaskStep,
        request: &IntegrationStepRequest,
    ) -> Result<()> {
        let outcome = match crate::workflow::actions::with_comment_key(
            step.causation_key.clone(),
            self.import_continue(step, request),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                tracing::warn!(target: "services::integration_steps", task_id = %step.task_id, attempt_id = %request.attempt_id, %error, "integration import continuation failed; it stays queued");
                let seconds = 5_i64 << (step.attempts - 1).clamp(0, 6);
                StepOutcome::RetryAt(
                    bounded(&error.to_string()),
                    chrono::Utc::now() + chrono::Duration::seconds(seconds),
                )
            }
        };
        self.conclude(step, outcome).await
    }

    async fn import_continue(
        &self,
        step: &TaskStep,
        request: &IntegrationStepRequest,
    ) -> Result<StepOutcome> {
        let Some(attempt) = self.db.integration_attempt(&request.attempt_id).await? else {
            return Ok(StepOutcome::Skip("attempt is gone".into()));
        };
        if !attempt.current || attempt.state.terminal() {
            return Ok(StepOutcome::Skip("the attempt is history".into()));
        }
        let live: bool = sqlx::query_scalar(db::STEP_FENCE)
            .bind(&attempt.task_ref)
            .bind(&attempt.expected_status)
            .bind(attempt.expected_epoch)
            .fetch_one(self.db.pool())
            .await?;
        let task = TaskRepo::get_by_id(&*self.db, &attempt.task_ref, false).await?;
        let Some(task) = task.filter(|_| live) else {
            // The Task moved on since the import. The row is history: a
            // queued one is cancelled by the worker's validation, any other
            // is superseded by the Task's next admission.
            return Ok(StepOutcome::Skip(
                "Task left the entry the attempt was imported in".into(),
            ));
        };
        let project = db::ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &crate::worker_runtime::queue::cascade_actor(),
        );
        let in_merge_state = merge_state(&workflow, &attempt.expected_status);
        let priority = import_priority(&attempt);
        // One writer per merge. A hooks step of this entry that ran before
        // this step and is suspended (on its disk wait, on a check) is the
        // old merge itself: it finishes what it recorded before it hands
        // over through admission. This step neither admits beside it nor
        // settles (a settled continuation is what makes a pre-queue row
        // claimable): it waits. Only an earlier, suspended step is waited
        // for: a pending one runs before this step anyway, and a later one
        // could not run until this step is settled.
        let suspended_entry_hooks: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM task_step h JOIN task t ON t.id=h.task_id \
             WHERE h.task_id=? AND h.kind='hooks' AND h.status='suspended' AND h.seq<? \
               AND h.expected_status=t.status AND h.expected_epoch=t.status_epoch)",
        )
        .bind(&task.id)
        .bind(step.seq)
        .fetch_one(self.db.pool())
        .await?;
        if in_merge_state && suspended_entry_hooks {
            return Ok(StepOutcome::RetryAt(
                "the entry's own hooks step continues this merge first".into(),
                chrono::Utc::now() + chrono::Duration::seconds(2),
            ));
        }

        // The pause marker of a merging Task is obsolete: the queue worker
        // parks and re-queues such a Task across a Project pause itself.
        let marker = crate::deferred_dispatch::paused_integration(&task);
        if let Some(marker) = marker.as_ref().filter(|_| in_merge_state) {
            crate::deferred_dispatch::clear_paused_integration(&self.db, &task.id, marker).await?;
        }

        // Row P15 (the old path was waiting for the machine that holds the
        // Task's checkout): the machine it names, and whether that wait is
        // already over.
        let waited_owner = (priority == Some(15)).then(|| owner_wait(&task)).flatten();
        let owner_back = priority == Some(15)
            && attempt.state == S::Parked
            && in_merge_state
            && self.owner_back(&task).await?;

        match attempt.state {
            S::Applied if in_merge_state => {
                // The one thing this step does to a merge itself: it reports
                // one that landed before the queue. No worker holds a slot
                // for such a row. Everything else it does is a statement or
                // the hand-over (admission).
                let _hold = crate::integration_worker::holders::hold(
                    &self.db,
                    &task.id,
                    crate::integration_worker::holders::MergeHolder::ImportContinuation,
                );
                self.import_applied(step, request, &attempt).await
            }
            // Not in a merge state (a conflict repair, a re-review, or the
            // old clean-rebase bounce between `merging` and `review`): the
            // Task's own flow continues exactly as before the upgrade, and
            // its next entry of the merge state supersedes this row.
            S::Queued | S::Parked if !in_merge_state => Ok(StepOutcome::Skip(
                "the Task is not in a merge state; its own flow continues".into(),
            )),
            // Admitted again, bound to the Task's placement and present
            // candidate: a shadow row (no owner fields, never classified), a
            // row the importer classified as queued, one parked under the
            // legacy pause marker, and one that waits only for a fact of the
            // candidate. NOT a row parked for its owner (blocked, failed,
            // held, waiting for a human, an owner machine or an accepted
            // command, a lost workspace): the importer leaves the candidate
            // of every row unnamed, so "has no owner fields" would take all
            // of those into the queue and merge a Task its owner stopped.
            // The machine a P15 row waited for is back: admitted, whatever
            // retry time the old build had stored.
            S::Parked if owner_back => self.import_readmit(step, &attempt, &task).await,
            S::Queued | S::Parked
                if attempt.available_at.is_none()
                    && ((attempt.import_source_json.is_none()
                        && super::admission::unbound(&attempt))
                        || (attempt.state == S::Queued && super::admission::unbound(&attempt))
                        || (attempt.state == S::Parked
                            && (marker.is_some()
                                || matches!(priority, Some(10..=12))
                                || needs_fact_to_queue(&attempt)))) =>
            {
                self.import_readmit(step, &attempt, &task).await
            }
            S::Queued => {
                self.import_state(
                    step,
                    &attempt,
                    Some(IntegrationReason::Waiting {
                        attempt_id: attempt_ref(&attempt.id),
                    }),
                )
                .await
            }
            S::Reconciling => {
                self.import_state(
                    step,
                    &attempt,
                    Some(IntegrationReason::Owned {
                        attempt_id: attempt_ref(&attempt.id),
                        phase: IntegrationPhase::Reconciling,
                    }),
                )
                .await
            }
            S::Parked | S::Quarantined => {
                let paused = project.paused_at.is_some();
                let message = attempt
                    .failure_message
                    .clone()
                    .unwrap_or_else(|| "integration is waiting".into());
                let cause = if attempt.state == S::Quarantined {
                    if attempt.current_operation_state.is_some() || attempt.operation_id.is_some() {
                        IntegrationDeferralCause::UnresolvedResult
                    } else {
                        IntegrationDeferralCause::OwnerRequired
                    }
                } else if priority == Some(15) {
                    // Ends when the machine is back, whatever retry time
                    // the old build had stored for its own polling.
                    IntegrationDeferralCause::OwnerOffline
                } else if attempt.available_at.is_some() {
                    // A stored deadline: the Task is admitted again at it.
                    IntegrationDeferralCause::Infrastructure
                } else {
                    IntegrationDeferralCause::OwnerRequired
                };
                // A paused Project is the Task's reason; integration adds none.
                let reason = (!paused).then(|| IntegrationReason::Deferred {
                    attempt_id: attempt_ref(&attempt.id),
                    cause,
                    owner_id: waited_owner.clone(),
                    message: bounded(&message),
                });
                self.import_state(step, &attempt, reason).await
            }
            // A coder repair or a review is already in progress for it.
            S::Ejected | S::NeedsReview => Ok(StepOutcome::Skip(
                "the repair or review in progress continues".into(),
            )),
            _ => Ok(StepOutcome::Skip(
                "the queue worker drives this attempt".into(),
            )),
        }
    }

    /// The wait of importer row P15 is over: the Task's placement is `ready`
    /// and its machine (if it is a daemon's) is connected, no daemon upgrade
    /// is refused and no cancellation is still to be confirmed on that
    /// workspace. Anything else keeps the wait; the reconnect makes it due
    /// (`TaskService::integration_owner_ready`).
    async fn owner_back(&self, task: &db::Task) -> Result<bool> {
        let metadata = task
            .metadata_json
            .as_deref()
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
            .unwrap_or_default();
        if metadata.get("daemon_upgrade_refusal").is_some() {
            return Ok(false);
        }
        let placement: Option<(String, String, Option<String>)> = sqlx::query_as(
            "SELECT workspace_id,state,daemon_id FROM workspace_placement WHERE task_id=? \
             ORDER BY generation DESC,updated_at DESC LIMIT 1",
        )
        .bind(&task.id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(db::DbError::from)?;
        let Some((workspace_id, state, daemon_id)) = placement else {
            return Ok(false);
        };
        if state != "ready" {
            return Ok(false);
        }
        // The placement's machine, and the machine the old wait named.
        for daemon_id in daemon_id.into_iter().chain(owner_wait(task)) {
            let connected = self
                .task_service
                .daemon_connections
                .as_ref()
                .is_some_and(|registry| registry.get(&daemon_id).is_some());
            if !connected {
                return Ok(false);
            }
        }
        Ok(self
            .db
            .pending_remote_cancels(None, Some(&workspace_id))
            .await?
            .is_empty())
    }

    /// State `reason` for the imported attempt and finish the step. An
    /// intervention raises its incident with the statement, as `park` does.
    async fn import_state(
        &self,
        step: &TaskStep,
        attempt: &IntegrationAttempt,
        reason: Option<IntegrationReason>,
    ) -> Result<StepOutcome> {
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        if !Self::entry_live_in_tx(&mut tx, attempt).await? {
            return Ok(StepOutcome::Skip(
                "Task left the entry the attempt was imported in".into(),
            ));
        }
        let mut refusal = None;
        if let Some(reason) = reason {
            let intervention = reason.requires_intervention();
            let stated = match self
                .db
                .state_integration_condition_in_tx(
                    &mut tx,
                    &attempt.task_ref,
                    &ConditionStatement::Integration { reason },
                )
                .await
            {
                Ok(()) => true,
                // Kept on the step: a wait the Task could not be told is
                // read there, and the Task's own reason stands meanwhile.
                Err(db::DbError::Check(reason)) => {
                    tracing::warn!(target: "services::integration_steps", task_id = %attempt.task_ref, %reason, "integration statement refused for an imported attempt");
                    refusal = Some(format!("integration statement refused: {reason}"));
                    false
                }
                Err(error) => return Err(error.into()),
            };
            if stated && intervention {
                let task = self
                    .db
                    .get_task_in_tx(&mut tx, &attempt.task_ref)
                    .await?
                    .ok_or(db::DbError::NotFound)?;
                self.raise_interruption_in_tx(&mut tx, &task).await?;
            }
        }
        self.db
            .finish_step_in_tx(&mut tx, step, "done", refusal.as_deref())
            .await?;
        tx.commit().await?;
        Ok(StepOutcome::Settled)
    }

    /// The recorded merge landed before the queue was active: report it as
    /// the `result` step does (evidence, comment, `done`), then complete the
    /// attempt. No queue slot was ever held for it.
    async fn import_applied(
        &self,
        step: &TaskStep,
        request: &IntegrationStepRequest,
        attempt: &IntegrationAttempt,
    ) -> Result<StepOutcome> {
        let result = IntegrationStepRequest {
            action: IntegrationStepAction::Result,
            effect_seq: attempt.effect_seq,
            generation: attempt.slot_generation,
            ..request.clone()
        };
        let outcome = self.result(step, &result, true).await?;
        if outcome == StepOutcome::Settled {
            complete_applied(&self.db, &attempt.id).await;
        }
        Ok(outcome)
    }

    /// Admit the Task again under the hook's rules: the imported row is
    /// superseded by one bound to the Task's placement and candidate.
    async fn import_readmit(
        &self,
        step: &TaskStep,
        imported: &IntegrationAttempt,
        task: &db::Task,
    ) -> Result<StepOutcome> {
        let Some(merge_service) = self.task_service.merge_service.as_ref() else {
            return Ok(StepOutcome::Skip("merge service not configured".into()));
        };
        let attempt = match super::admission::admit_released(
            &self.db,
            merge_service,
            task,
            self.task_service.integration_worker().as_deref(),
        )
        .await
        {
            Ok(attempt) => attempt,
            Err(error) if crate::worker_runtime::queue::retryable(&error) => {
                return Ok(StepOutcome::Retry(bounded(&error.to_string())))
            }
            // The worktree would have to be recreated under the disk floor:
            // the Task waits for disk and the continuation asks again at
            // the recheck.
            Err(error)
                if crate::placement::capacity_wait_of(&error)
                    == Some(crate::placement::CapacityWait::Disk) =>
            {
                self.task_service
                    .defer_placement_refusal(task, &error)
                    .await?;
                return Ok(StepOutcome::RetryAt(
                    bounded(&error.to_string()),
                    chrono::Utc::now() + chrono::Duration::seconds(30),
                ));
            }
            // The candidate cannot be named (its workspace or implementation
            // run is gone): the approval evidence is kept and no other commit
            // is declared reviewed. The owner decides.
            Err(error) => {
                return self
                    .import_state(
                        step,
                        imported,
                        Some(IntegrationReason::Deferred {
                            attempt_id: attempt_ref(&imported.id),
                            cause: IntegrationDeferralCause::OwnerRequired,
                            owner_id: None,
                            message: bounded(&format!(
                                "the merge that was in progress cannot continue: {error}"
                            )),
                        }),
                    )
                    .await
            }
        };
        super::admission::state_waiting(&self.db, &attempt).await?;
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        self.db
            .finish_step_in_tx(&mut tx, step, "done", None)
            .await?;
        tx.commit().await?;
        if let Some(worker) = self.task_service.integration_worker() {
            worker.notify_answered();
        }
        Ok(StepOutcome::Settled)
    }
}

/// `applied -> completed` for an imported attempt whose result was applied.
/// Best effort: a row left `applied` with its `result` acknowledgment is
/// completed by the next start (`complete_reported`).
async fn complete_applied(db: &Arc<SqliteDb>, attempt_id: &str) {
    let completed = async {
        let Some(mut attempt) = db.integration_attempt(attempt_id).await? else {
            return Ok(());
        };
        if attempt.state != S::Applied {
            return Ok(());
        }
        attempt.state = S::Completed;
        db.transition_integration_attempt(attempt).await.map(|_| ())
    }
    .await;
    if let Err::<(), db::DbError>(error) = completed {
        tracing::warn!(target: "services::integration_steps", attempt_id, %error, "imported attempt was reported but not completed; the next start completes it");
    }
}

/// Imported attempts whose result the Task already has but which are still
/// `applied` (the process stopped between the two writes). No queue holds
/// them as its head, so nothing else would complete them.
pub async fn complete_reported(db: &Arc<SqliteDb>) -> Result<()> {
    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT a.id FROM integration_attempt a WHERE a.current=1 AND a.state='applied' \
           AND a.import_source_json IS NOT NULL AND a.acknowledged_at IS NOT NULL \
           AND json_extract(a.effect_ack_json,'$.action')='result' \
           AND NOT EXISTS(SELECT 1 FROM integration_queue q WHERE q.head_attempt_id=a.id)",
    )
    .fetch_all(db.pool())
    .await
    .map_err(db::DbError::from)?;
    for id in ids {
        complete_applied(db, &id).await;
    }
    Ok(())
}
