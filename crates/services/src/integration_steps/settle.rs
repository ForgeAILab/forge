//! `request_check` and `settle`: the head's check, the review authority and
//! carry for the exact candidate, and the permit.
//!
//! One `settle` writes the worker's decision for an `effect_seq`. It is
//! reached two ways and both use [`IntegrationSteps::settle`]:
//! - the `settle` step, when the target did not move (no check is asked);
//! - the check's delivery (`apply_check_result`, through
//!   [`IntegrationCheckFamily`]), after a `request_check` for a rebased head.
use super::{ack_value, bounded, IntegrationSteps, StepOutcome, TaskStepIntegrationPort};
use crate::{
    check_runner::consumer::{
        CheckApplication, CheckConsumerFamily, CheckVerdict, TaskCheckRequest,
    },
    integration_worker::{IntegrationStepAction, IntegrationStepOutcome, IntegrationStepRequest},
    merge_service::ReviewCarryFacts,
    workflow::{
        actions::carry::{
            carry_budget_refusal, carry_path_refusal, gate_refuses_carry, task_carry_checks,
            CarryError,
        },
        default_states,
        engine::WorkflowEngine,
    },
    Result, ServiceError,
};
use api_types::{CheckPurpose, IntegrationPhase};
use async_trait::async_trait;
use db::{
    CheckConsumerOrigin, CheckDeliveryRepo, CheckResultOutcome, CheckRunIdentity, CheckRunRepo,
    IntegrationAttempt, IntegrationAttemptState as S, IntegrationCheckTiming, IntegrationQueue,
    IntegrationQueueRepo, IntegrationStepAckOutcome, IntegrationStepAckWrite,
    IntegrationStepPosition, SqliteDb, TaskRepo, TaskStep, TaskStepRepo,
};
use std::sync::Arc;

/// What the check said, as `settle` needs it.
#[derive(Debug, Clone)]
pub(crate) enum CheckInput {
    /// No check was asked: the worker found the target unchanged.
    NotRequested,
    Passed(IntegrationCheckTiming),
    Failed(IntegrationCheckTiming, String),
    /// No verdict on the candidate (infrastructure retries exhausted).
    NoVerdict(String),
}

/// The check a head asks for: the review state's `ci_steps` on the rebased
/// commit, in the Task's own workspace.
#[derive(Debug, Clone)]
pub struct HeadCheck {
    pub identity: CheckRunIdentity,
    pub workspace_id: Option<String>,
    pub machine_id: Option<String>,
    pub wall_timeout_seconds: u64,
}

/// Builds the head's check identity. `Ok(None)`: nothing is configured to
/// run, so there is no check to wait for.
#[async_trait]
pub trait HeadCheckSource: Send + Sync {
    async fn head_check(
        &self,
        task: &db::Task,
        attempt: &IntegrationAttempt,
        queue: &IntegrationQueue,
    ) -> Result<Option<HeadCheck>>;
}

/// The default source: the same bundle review entry runs (`ci_steps` of the
/// effective review configuration), under the identity review entry asks
/// under, so a result of either is a hit for the other on the same commit in
/// the same worktree.
pub struct ReviewCiHeadCheck {
    db: Arc<SqliteDb>,
}
impl ReviewCiHeadCheck {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }
}
#[async_trait]
impl HeadCheckSource for ReviewCiHeadCheck {
    async fn head_check(
        &self,
        task: &db::Task,
        attempt: &IntegrationAttempt,
        queue: &IntegrationQueue,
    ) -> Result<Option<HeadCheck>> {
        let commit_sha = attempt
            .candidate_sha
            .clone()
            .ok_or_else(|| ServiceError::invalid_operation("the head has no candidate commit"))?;
        let project = db::ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &crate::worker_runtime::queue::cascade_actor(),
        );
        // The review state's effective configuration (workflow, Project and
        // Task), which is what review entry runs.
        let review_config = workflow
            .states
            .iter()
            .find(|state| state.name == default_states::REVIEW)
            .map(|state| {
                crate::workflow::engine::effective_state_config(
                    state,
                    Some(&project),
                    task.task_state_config.as_deref(),
                )
            });
        if crate::workflow::engine::review_ci_steps_for_task(
            &workflow,
            Some(&project),
            task.task_state_config.as_deref(),
        )
        .iter()
        .all(|step| step.trim().is_empty())
        {
            return Ok(None);
        }
        let workspace = match attempt.workspace_id.as_deref() {
            Some(id) => db::WorkspaceRepo::get_by_id(&*self.db, id).await?,
            None => db::WorkspaceRepo::get_by_task_id(&*self.db, &task.id).await?,
        }
        .ok_or_else(|| ServiceError::invalid_operation("the head has no workspace"))?;
        let placement = db::WorkspacePlacementRepo::get_by_workspace_id(&*self.db, &workspace.id)
            .await?
            .ok_or_else(|| ServiceError::invalid_operation("the head has no placement"))?;
        let state_config = review_config.unwrap_or(serde_json::Value::Null);
        let Some(identity) = crate::check_runner::review_entry::review_ci_identity(
            &self.db,
            crate::check_runner::review_entry::ReviewCiIdentityInput {
                task,
                workspace: &workspace,
                placement: &placement,
                commit_sha,
                state_config: &state_config,
                dirty: false,
                target_branch: Some(&queue.target_branch),
            },
        )
        .await?
        else {
            return Ok(None);
        };
        if identity.repo_id != queue.repo_id {
            return Err(ServiceError::invalid_operation(
                "the head's check names another repository",
            ));
        }
        Ok(Some(HeadCheck {
            identity,
            workspace_id: Some(workspace.id),
            machine_id: placement.daemon_id,
            wall_timeout_seconds: u64::from(::review::contract::DEFAULT_CHECK_TIMEOUT_SECONDS),
        }))
    }
}

/// The authority under which a head's check is asked and applied. It names
/// the attempt and the exact commit, not the `effect_seq`: a worker that
/// asks again after a takeover joins the same consumer and the same run.
pub(crate) fn check_authority(attempt: &IntegrationAttempt) -> Option<String> {
    attempt
        .candidate_sha
        .as_deref()
        .map(|sha| format!("integration:{}:{sha}", attempt.id))
}

fn timing(
    application: &CheckApplication,
    result: &db::StoredCheckResult,
) -> IntegrationCheckTiming {
    let parse = |text: &str| chrono::DateTime::parse_from_rfc3339(text).ok();
    let started = result
        .commands
        .first()
        .and_then(|command| parse(&command.started_at));
    let finished = result
        .commands
        .last()
        .and_then(|command| parse(&command.finished_at));
    let asked = parse(&application.run.created_at);
    type At = Option<chrono::DateTime<chrono::FixedOffset>>;
    let millis = |from: At, to: At| match (from, to) {
        (Some(from), Some(to)) => (to - from).num_milliseconds().max(0),
        _ => 0,
    };
    IntegrationCheckTiming::Ran {
        slot_wait_ms: millis(asked, started),
        run_ms: millis(started, finished),
    }
}

pub(super) fn check_input(application: &CheckApplication) -> CheckInput {
    match &application.verdict {
        CheckVerdict::InfrastructureExhausted(_) => {
            CheckInput::NoVerdict("the check produced no verdict after its retries".into())
        }
        CheckVerdict::Result(result) => {
            let timing = timing(application, result);
            match result.outcome {
                CheckResultOutcome::Pass => CheckInput::Passed(timing),
                CheckResultOutcome::InfrastructureFailed | CheckResultOutcome::Cancelled => {
                    CheckInput::NoVerdict(format!("the check ended {}", result.outcome))
                }
                CheckResultOutcome::Fail | CheckResultOutcome::TimedOut => {
                    let failed = result
                        .commands
                        .iter()
                        .find(|command| command.exit_code != 0)
                        .map(|command| {
                            format!(
                                "`{}` exited {}: {}",
                                command.command,
                                command.exit_code,
                                if command.stderr_tail.trim().is_empty() {
                                    command.output_tail.trim()
                                } else {
                                    command.stderr_tail.trim()
                                }
                            )
                        })
                        .unwrap_or_else(|| format!("the check ended {}", result.outcome));
                    CheckInput::Failed(timing, bounded(&failed))
                }
            }
        }
    }
}

/// What `settle` decided before its transaction.
enum Decision {
    Permit(Option<db::NewReviewAuthorityCarry>),
    Refuse(IntegrationStepOutcome, String),
}

impl IntegrationSteps {
    /// `request_check`: ask the check runner for the rebased commit and state
    /// that the head is checking. The verdict arrives as the consumer's own
    /// delivery step, which writes the `settle` answer.
    pub(crate) async fn request_check(
        &self,
        step: &TaskStep,
        request: &IntegrationStepRequest,
    ) -> Result<StepOutcome> {
        let Some(attempt) = self.db.integration_attempt(&request.attempt_id).await? else {
            return Ok(StepOutcome::Skip("attempt is gone".into()));
        };
        match db::integration_step_position(
            &attempt,
            request.generation,
            request.effect_seq,
            true,
            IntegrationStepAction::Settle.as_str(),
            &[S::Checking],
        ) {
            IntegrationStepPosition::Answered => {
                return Ok(StepOutcome::Skip("already answered".into()))
            }
            IntegrationStepPosition::Stale => {
                return Ok(StepOutcome::Skip("the attempt moved on".into()))
            }
            IntegrationStepPosition::NotYet => {
                return Ok(StepOutcome::Retry("attempt is not checking yet".into()))
            }
            IntegrationStepPosition::Due => {}
        }
        let task = TaskRepo::get_by_id(&*self.db, &attempt.task_ref, false).await?;
        let live: bool = sqlx::query_scalar(db::STEP_FENCE)
            .bind(&attempt.task_ref)
            .bind(&attempt.expected_status)
            .bind(attempt.expected_epoch)
            .fetch_one(self.db.pool())
            .await?;
        let Some(task) = task.filter(|_| live) else {
            return self
                .answer_task_left(Some(step), request, &[S::Checking])
                .await;
        };
        let queue = match attempt.queue_id.as_deref() {
            Some(id) => self.db.integration_queue(id).await?,
            None => None,
        }
        .ok_or_else(|| ServiceError::invalid_operation("the head has no queue"))?;
        let Some(check) = self.head_check.head_check(&task, &attempt, &queue).await? else {
            // Nothing is configured to run: the rebased commit has no check
            // behind it, which the carry rule then refuses.
            return self
                .settle(
                    Some(step),
                    request,
                    &[S::Checking],
                    CheckInput::NotRequested,
                    None,
                )
                .await;
        };
        let consumers = self
            .task_service
            .check_consumers()
            .ok_or_else(|| ServiceError::invalid_operation("check consumers are not composed"))?;
        let authority = check_authority(&attempt)
            .ok_or_else(|| ServiceError::invalid_operation("the head has no candidate commit"))?;
        let requested = consumers
            .request(TaskCheckRequest {
                task_id: task.id.clone(),
                status_epoch: attempt.expected_epoch,
                authority,
                origin: CheckConsumerOrigin::Integration,
                purpose: CheckPurpose::QueueHeadCi,
                identity: check.identity,
                workspace_id: check.workspace_id,
                machine_id: check.machine_id,
                wall_timeout_seconds: check.wall_timeout_seconds,
            })
            .await?;
        // A worker that asks again for a commit whose verdict already exists
        // gets no second delivery: apply the stored verdict here. That covers
        // a delivery that was applied for an earlier `effect_seq` and one
        // whose application failed part-way (its step is not run again). A
        // delivery still on its way writes the same answer; whichever commits
        // second finds it answered.
        let delivery = self
            .db
            .check_consumer_delivery(&requested.consumer.id)
            .await?;
        if let Some(delivery) = delivery.filter(|delivery| delivery.cancelled_at.is_none()) {
            if let (Some(run_id), Some(result_id)) =
                (&delivery.consumer.run_id, &delivery.consumer.result_id)
            {
                let run = self.db.check_run(run_id).await?;
                let result = self.db.check_result(result_id).await?;
                // "No verdict after its retries" is not a verdict on the
                // commit. It was told to the round that waited for it; a
                // later round of the same commit (this ask) is not answered
                // with it again, or every retry of the head would park on
                // the same stored answer. The consumer asks again, with a
                // fresh run, and this head waits for that result.
                let no_verdict = result.as_ref().is_some_and(|result| {
                    result.outcome == CheckResultOutcome::InfrastructureFailed
                });
                let asked_again = no_verdict
                    && consumers
                        .ask_again_after_no_verdict(&delivery.consumer)
                        .await?;
                if let (Some(run), Some(result), false) = (run, result, asked_again) {
                    let application = CheckApplication {
                        task_id: task.id.clone(),
                        status_epoch: attempt.expected_epoch,
                        authority: String::new(),
                        consumer_id: delivery.consumer.id.clone(),
                        run,
                        verdict: if result.outcome == CheckResultOutcome::InfrastructureFailed {
                            CheckVerdict::InfrastructureExhausted(result)
                        } else {
                            CheckVerdict::Result(result)
                        },
                    };
                    return self
                        .settle(
                            Some(step),
                            request,
                            &[S::Checking],
                            check_input(&application),
                            Some(&application),
                        )
                        .await;
                }
            }
        }
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        if !Self::entry_live_in_tx(&mut tx, &attempt).await? {
            return Ok(StepOutcome::TaskLeft);
        }
        self.state_in_tx(
            &mut tx,
            &task.id,
            &Self::owned(&attempt.id, IntegrationPhase::Checking),
        )
        .await?;
        self.db
            .finish_step_in_tx(&mut tx, step, "done", None)
            .await?;
        tx.commit().await?;
        Ok(StepOutcome::Settled)
    }

    /// The Task is no longer in the entry the attempt was admitted in: tell
    /// the worker, write nothing on the Task.
    async fn answer_task_left(
        &self,
        step: Option<&TaskStep>,
        request: &IntegrationStepRequest,
        states: &[S],
    ) -> Result<StepOutcome> {
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        let written = db::acknowledge_integration_step_in_tx(
            &mut tx,
            &IntegrationStepAckWrite {
                attempt_id: request.attempt_id.clone(),
                generation: request.generation,
                effect_seq: request.effect_seq,
                bind_generation: true,
                states: states.to_vec(),
                ack: ack_value(
                    request,
                    IntegrationStepAction::Settle,
                    IntegrationStepOutcome::TaskLeft,
                    None,
                    None,
                ),
                permit: None,
                acknowledged_at: db::now_rfc3339(),
            },
        )
        .await?;
        if matches!(written, IntegrationStepAckOutcome::NotYet(_)) {
            return Ok(StepOutcome::Retry("attempt is not waiting yet".into()));
        }
        if let Some(step) = step {
            self.db
                .finish_step_in_tx(
                    &mut tx,
                    step,
                    "superseded",
                    Some("Task left the status entry the attempt was admitted in"),
                )
                .await?;
        }
        tx.commit().await?;
        Ok(StepOutcome::Settled)
    }

    /// Carry by attempt lineage: may the passed review cover the commit the
    /// queue rebased? The same rule a Task entering `review` on a mechanical
    /// bridge is judged by (`workflow::actions::carry`).
    async fn carry_for(
        &self,
        task: &db::Task,
        attempt: &IntegrationAttempt,
        base: &db::AttemptCarryBase,
        check: &CheckInput,
    ) -> Result<Decision> {
        let refuse = |reason: String| {
            Ok(Decision::Refuse(
                IntegrationStepOutcome::NeedsReview,
                reason,
            ))
        };
        let no_verdict = |reason: &str| {
            Ok(Decision::Refuse(
                IntegrationStepOutcome::Infrastructure,
                reason.into(),
            ))
        };
        if !matches!(check, CheckInput::Passed(_)) {
            return refuse("the rebased commit has no passed check".into());
        }
        let project = db::ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        if project.paused_at.is_some() {
            return no_verdict("project paused");
        }
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &crate::worker_runtime::queue::cascade_actor(),
        );
        let review = workflow
            .states
            .iter()
            .find(|state| state.name == default_states::REVIEW);
        if let Some(reason) =
            gate_refuses_carry(review.and_then(|state| state.gate_config.as_ref()))
        {
            return refuse(reason.into());
        }
        let review_config = review
            .map(|state| {
                crate::workflow::engine::effective_state_config(
                    state,
                    Some(&project),
                    task.task_state_config.as_deref(),
                )
            })
            .unwrap_or(serde_json::Value::Null);
        match task_carry_checks(&self.db, task, &review_config).await {
            Ok(_) => {}
            Err(CarryError::Ineligible(reason)) => return refuse(reason),
            Err(CarryError::Failed(reason)) => return Err(ServiceError::invalid_operation(reason)),
        }
        if let Some(reason) = carry_budget_refusal(base.carries_since_review) {
            return refuse(reason);
        }
        let Some(merge_service) = self.task_service.merge_service.as_ref() else {
            return refuse("merge service unavailable".into());
        };
        let (commit_sha, base_sha, changed_paths) =
            match merge_service.review_carry_facts(&task.id).await? {
                ReviewCarryFacts::Ready {
                    commit_sha,
                    base_sha,
                    changed_paths,
                } => (commit_sha, base_sha, changed_paths),
                ReviewCarryFacts::Unavailable { reason } => return refuse(reason),
            };
        if Some(commit_sha.as_str()) != attempt.candidate_sha.as_deref() {
            return no_verdict("the candidate changed after the head read it");
        }
        if Some(base_sha.as_str()) != attempt.target_tip_sha.as_deref() {
            return no_verdict("the target moved after the head read it");
        }
        if let Some(reason) =
            carry_path_refusal(&changed_paths, &base.contract.candidate_changed_paths)
        {
            return refuse(reason);
        }
        // A successor of an attempt ejected on a conflict carries the
        // Worker's repair; anything else is the queue's own clean rebase.
        let repaired = match attempt.predecessor_attempt_id.as_deref() {
            Some(id) => self
                .db
                .integration_attempt(id)
                .await?
                .is_some_and(|previous| previous.conflict_paths_json.is_some()),
            None => false,
        };
        Ok(Decision::Permit(Some(db::NewReviewAuthorityCarry {
            task_id: task.id.clone(),
            contract_execution_id: base.contract.execution_id.clone(),
            commit_sha,
            base_sha,
            kind: if repaired {
                db::ReviewCarryKind::ConflictRepair
            } else {
                db::ReviewCarryKind::CleanRebase
            },
            changed_paths,
        })))
    }

    /// Decide and answer one `effect_seq` of the head. `step` is the step to
    /// settle in the same transaction (`None` when the caller's own command
    /// step carries this, as the check delivery does). `states` are the
    /// attempt states the answer applies in.
    pub(crate) async fn settle(
        &self,
        step: Option<&TaskStep>,
        request: &IntegrationStepRequest,
        states: &[S],
        check: CheckInput,
        evidence: Option<&CheckApplication>,
    ) -> Result<StepOutcome> {
        let action = IntegrationStepAction::Settle;
        let Some(attempt) = self.db.integration_attempt(&request.attempt_id).await? else {
            return Ok(StepOutcome::Skip("attempt is gone".into()));
        };
        match db::integration_step_position(
            &attempt,
            request.generation,
            request.effect_seq,
            true,
            action.as_str(),
            states,
        ) {
            IntegrationStepPosition::Answered => {
                return Ok(StepOutcome::Skip("already answered".into()))
            }
            IntegrationStepPosition::Stale => {
                return Ok(StepOutcome::Skip("the attempt moved on".into()))
            }
            IntegrationStepPosition::NotYet => {
                return Ok(StepOutcome::Retry("attempt is not waiting yet".into()))
            }
            IntegrationStepPosition::Due => {}
        }
        let task = TaskRepo::get_by_id(&*self.db, &attempt.task_ref, false).await?;
        let live: bool = sqlx::query_scalar(db::STEP_FENCE)
            .bind(&attempt.task_ref)
            .bind(&attempt.expected_status)
            .bind(attempt.expected_epoch)
            .fetch_one(self.db.pool())
            .await?;
        let Some(task) = task.filter(|_| live) else {
            return self.answer_task_left(step, request, states).await;
        };

        // Everything that reads Git or the workflow is decided before the
        // write transaction; the transaction re-reads what it depends on.
        let authority = {
            let mut tx = self.db.pool().begin().await?;
            let base = self.db.attempt_carry_base_in_tx(&mut tx, &task.id).await;
            tx.rollback().await?;
            base
        };
        let timing = match &check {
            CheckInput::Passed(timing) | CheckInput::Failed(timing, _) => Some(timing.clone()),
            CheckInput::NotRequested | CheckInput::NoVerdict(_) => None,
        };
        let decision = match (&check, authority) {
            (CheckInput::Failed(_, message), _) => Decision::Refuse(
                IntegrationStepOutcome::CandidateCheckFailed,
                message.clone(),
            ),
            (CheckInput::NoVerdict(message), _) => {
                Decision::Refuse(IntegrationStepOutcome::Infrastructure, message.clone())
            }
            (_, Err(db::DbError::Check(reason))) => {
                Decision::Refuse(IntegrationStepOutcome::NeedsReview, reason)
            }
            (_, Err(error)) => return Err(error.into()),
            // No reviewed object to pin: no agent reviewer, or the owner
            // passed the review by hand.
            (_, Ok(None)) => Decision::Permit(None),
            (_, Ok(Some(base))) => {
                if Some(base.candidate.commit_sha.as_str()) == attempt.candidate_sha.as_deref() {
                    Decision::Permit(None)
                } else {
                    self.carry_for(&task, &attempt, &base, &check).await?
                }
            }
        };

        // A Task its owner stopped (on hold, blocked, failed, waiting for a
        // person, its Project paused) keeps its status entry, so
        // the entry fence alone would let the queue merge it. No permit is
        // written for it. The worker reads the same gate (`gate::task_gate`)
        // and parks the head under it, uncounted, until it lifts.
        let decision = match super::gate::task_gate(&self.db, &task.id).await? {
            Some(cause) if matches!(decision, Decision::Permit(_)) => Decision::Refuse(
                IntegrationStepOutcome::Infrastructure,
                format!("{}: {}", cause.code(), cause.message()),
            ),
            _ => decision,
        };

        let now = db::now_rfc3339();
        #[cfg(test)]
        super::preemption_tests::interrupt_begin_if_armed(self.db.pool(), &task.id).await;
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        let current = db::integration_attempt_in_tx(&mut tx, &attempt.id).await?;
        if current.revision != attempt.revision {
            return Ok(StepOutcome::Retry("attempt changed while deciding".into()));
        }
        // Cancel won: the flag is written in the Cancel's own transaction,
        // so a permit can never follow it.
        if current.cancel_requested_at.is_some()
            || !Self::entry_live_in_tx(&mut tx, &attempt).await?
        {
            drop(tx);
            return self.answer_task_left(step, request, states).await;
        }
        let (outcome, message, permit) = match decision {
            Decision::Refuse(outcome, message) => (outcome, Some(message), None),
            Decision::Permit(carry) => {
                // Cancel-versus-permit: an owner command already queued for
                // this Task runs first. A permit written now would let the
                // fast-forward start under a Task that is about to leave.
                let own = step
                    .map(|step| step.id.clone())
                    .or_else(db::task_writer::current_step_id);
                let preempting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_step WHERE task_id=? AND priority>=1 AND status IN ('pending','claimed') AND (? IS NULL OR id<>?))")
                    .bind(&task.id).bind(&own).bind(&own).fetch_one(&mut *tx).await?;
                if preempting {
                    return Ok(StepOutcome::Retry(
                        "an owner command is ahead of the permit".into(),
                    ));
                }
                // The authority the decision was made on is still the one in force.
                match self.db.attempt_carry_base_in_tx(&mut tx, &task.id).await {
                    Ok(base) => {
                        let covered = match (&base, &carry) {
                            (None, _) => true,
                            (Some(base), None) => {
                                Some(base.candidate.commit_sha.as_str())
                                    == attempt.candidate_sha.as_deref()
                            }
                            (Some(base), Some(carry)) => {
                                base.contract.execution_id == carry.contract_execution_id
                            }
                        };
                        if !covered {
                            return Ok(StepOutcome::Retry(
                                "review authority changed while deciding".into(),
                            ));
                        }
                    }
                    Err(db::DbError::Check(_)) => {
                        return Ok(StepOutcome::Retry(
                            "review authority changed while deciding".into(),
                        ))
                    }
                    Err(error) => return Err(error.into()),
                }
                if let Some(carry) = &carry {
                    match self
                        .db
                        .settle_attempt_review_carry_in_tx(&mut tx, &attempt.id, carry, &now)
                        .await
                    {
                        Ok(()) => {}
                        Err(db::DbError::Check(_)) => {
                            return Ok(StepOutcome::Retry(
                                "review carry allowance changed while deciding".into(),
                            ))
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                let permit = serde_json::json!({
                    "candidate_sha": attempt.candidate_sha,
                    "target_tip_sha": attempt.target_tip_sha,
                    "task_ref": attempt.task_ref,
                    "expected_epoch": attempt.expected_epoch,
                    "slot_generation": attempt.slot_generation,
                    "carried": carry.is_some(),
                    "issued_at": now,
                });
                // Protection: from this commit on, a later Cancel / Hold /
                // move can neither overtake nor supersede the result.
                let result = IntegrationStepRequest {
                    action: IntegrationStepAction::Result,
                    ..request.clone()
                };
                let wake = chrono::Utc::now() + self.timers.result_wake;
                let input = TaskStepIntegrationPort::step_input(
                    &result,
                    &attempt.expected_status,
                    task.version,
                    own.clone(),
                    wake.to_rfc3339(),
                )?;
                self.db
                    .enqueue_protected_integration_step_in_tx(&mut tx, &input)
                    .await?;
                self.state_in_tx(
                    &mut tx,
                    &task.id,
                    &Self::owned(&attempt.id, IntegrationPhase::FastForwarding),
                )
                .await?;
                (IntegrationStepOutcome::Permit, None, Some(permit))
            }
        };
        if let Some(application) = evidence {
            self.record_head_review_in_tx(&mut tx, &attempt, application)
                .await?;
        }
        let written = db::acknowledge_integration_step_in_tx(
            &mut tx,
            &IntegrationStepAckWrite {
                attempt_id: attempt.id.clone(),
                generation: request.generation,
                effect_seq: request.effect_seq,
                bind_generation: true,
                states: states.to_vec(),
                ack: ack_value(request, action, outcome, timing, message),
                permit,
                acknowledged_at: now,
            },
        )
        .await?;
        if !matches!(written, IntegrationStepAckOutcome::Written(_)) {
            return Ok(StepOutcome::Retry("attempt changed while deciding".into()));
        }
        if let Some(step) = step {
            self.db
                .finish_step_in_tx(&mut tx, step, "done", None)
                .await?;
        }
        tx.commit().await?;
        Ok(StepOutcome::Settled)
    }
}

/// The merge-path check consumer family (origin `integration`). Its `apply`
/// runs inside the consumer's own delivery step and writes the `settle`
/// answer for the worker: verdict, check timing and, on a pass, the permit.
pub struct IntegrationCheckFamily {
    steps: Arc<IntegrationSteps>,
}
impl IntegrationCheckFamily {
    pub fn new(steps: Arc<IntegrationSteps>) -> Self {
        Self { steps }
    }
    /// Register the family with the runtime's check consumers.
    pub fn register(
        consumers: &crate::check_runner::consumer::TaskCheckConsumers,
        steps: Arc<IntegrationSteps>,
    ) {
        consumers.register(CheckConsumerOrigin::Integration, Arc::new(Self::new(steps)));
    }
    async fn checking(
        &self,
        task_id: &str,
        status_epoch: i64,
    ) -> Result<Option<IntegrationAttempt>> {
        Ok(self
            .steps
            .db
            .current_integration_attempt(task_id)
            .await?
            .filter(|attempt| {
                attempt.state == S::Checking && attempt.expected_epoch == status_epoch
            }))
    }
}
#[async_trait]
impl CheckConsumerFamily for IntegrationCheckFamily {
    async fn current_authority(&self, task_id: &str, status_epoch: i64) -> Result<Option<String>> {
        Ok(self
            .checking(task_id, status_epoch)
            .await?
            .as_ref()
            .and_then(check_authority))
    }
    async fn apply(&self, application: &CheckApplication) -> Result<()> {
        let Some(attempt) = self
            .checking(&application.task_id, application.status_epoch)
            .await?
            .filter(|attempt| check_authority(attempt).as_deref() == Some(&application.authority))
        else {
            return Ok(());
        };
        // The answer is for the worker that waits now: its `effect_seq` and
        // generation, whichever step first asked for this commit.
        let request = crate::integration_worker::IntegrationQueueWorker::step_request(
            &attempt,
            IntegrationStepAction::RequestCheck,
        );
        // One retry covers the worker writing its own row in between; after
        // that the delivery stays unapplied and the worker asks again.
        for _ in 0..3 {
            match self
                .steps
                .settle(
                    None,
                    &request,
                    &[S::Checking],
                    check_input(application),
                    Some(application),
                )
                .await?
            {
                StepOutcome::Retry(_) | StepOutcome::RetryAt(..) => {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await
                }
                _ => {
                    self.steps.wake_worker();
                    return Ok(());
                }
            }
        }
        Err(db::DbError::VersionConflict.into())
    }
}

impl IntegrationSteps {
    /// An unchanged reviewed commit needs no new run. Try the runner's real
    /// certified cache hit; a frozen/unattested policy retains the passed
    /// Review's own result as authority rather than launching it again.
    pub(crate) async fn reuse_review_check(
        &self,
        request: &IntegrationStepRequest,
    ) -> Result<Option<CheckApplication>> {
        let Some(attempt) = self.db.integration_attempt(&request.attempt_id).await? else {
            return Ok(None);
        };
        if attempt.state != S::AwaitingTaskStep {
            return Ok(None);
        }
        let Some(task) = TaskRepo::get_by_id(&*self.db, &attempt.task_ref, false).await? else {
            return Ok(None);
        };
        let Some(queue) = self.db.integration_queue(&request.queue_id).await? else {
            return Ok(None);
        };
        let Some(check) = self.head_check.head_check(&task, &attempt, &queue).await? else {
            return Ok(None);
        };
        let key = check.identity.key()?;
        let prior: Option<(String,String,String)> = sqlx::query_as(
            "SELECT c.id,c.run_id,c.result_id FROM check_consumer c JOIN check_result r ON r.id=c.result_id JOIN review v ON c.request_key='task-step/1|entry|'||v.task_id||'|'||c.status_epoch||'|'||v.id WHERE c.task_id=? AND c.origin='entry' AND c.identity_key=? AND r.outcome='pass' AND v.status='passed' ORDER BY v.attempt_number DESC,c.created_at DESC LIMIT 1")
            .bind(&task.id).bind(&key).fetch_optional(self.db.pool()).await?;
        let Some((consumer_id, run_id, result_id)) = prior else {
            return Ok(None);
        };
        let authority = check_authority(&attempt)
            .ok_or_else(|| ServiceError::invalid_operation("the head has no candidate"))?;
        let consumers = self
            .task_service
            .check_consumers()
            .ok_or_else(|| ServiceError::invalid_operation("check consumers are not composed"))?;
        let reused = consumers
            .try_reuse(TaskCheckRequest {
                task_id: task.id.clone(),
                status_epoch: attempt.expected_epoch,
                authority: authority.clone(),
                origin: CheckConsumerOrigin::Integration,
                purpose: CheckPurpose::QueueHeadCi,
                identity: check.identity,
                workspace_id: check.workspace_id,
                machine_id: check.machine_id,
                wall_timeout_seconds: check.wall_timeout_seconds,
            })
            .await?;
        let (consumer_id, run, result) = match reused {
            Some(requested) => {
                let crate::check_runner::CheckRequestOutcome::Hit(result) = requested.outcome
                else {
                    return Err(ServiceError::invalid_operation(
                        "reuse-only request scheduled a run",
                    ));
                };
                let run = self
                    .db
                    .check_run(&result.run_id)
                    .await?
                    .ok_or(db::DbError::NotFound)?;
                (requested.consumer.id, run, result)
            }
            None => (
                consumer_id,
                self.db
                    .check_run(&run_id)
                    .await?
                    .ok_or(db::DbError::NotFound)?,
                self.db
                    .check_result(&result_id)
                    .await?
                    .ok_or(db::DbError::NotFound)?,
            ),
        };
        if result.identity_key != key || run.identity_key != key {
            return Err(ServiceError::invalid_operation(
                "the Review's check identity changed",
            ));
        }
        Ok(Some(CheckApplication {
            task_id: task.id,
            status_epoch: attempt.expected_epoch,
            authority,
            consumer_id,
            run,
            verdict: CheckVerdict::Result(result),
        }))
    }

    async fn record_head_review_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        attempt: &IntegrationAttempt,
        application: &CheckApplication,
    ) -> Result<()> {
        let CheckVerdict::Result(result) = &application.verdict else {
            return Ok(());
        };
        if !matches!(
            result.outcome,
            CheckResultOutcome::Pass | CheckResultOutcome::Fail | CheckResultOutcome::TimedOut
        ) {
            return Ok(());
        }
        let row: Option<(String,String)> = sqlx::query_as("SELECT id,step_results_json FROM review WHERE task_id=? ORDER BY attempt_number DESC,id DESC LIMIT 1")
            .bind(&attempt.task_ref).fetch_optional(&mut **tx).await?;
        let (id, mut details) = match row {
            Some((id, raw)) => (
                id,
                serde_json::from_str::<serde_json::Value>(&raw)
                    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
            ),
            None => {
                let execution: Option<String> = sqlx::query_scalar("SELECT id FROM execution WHERE task_id=? AND role IN ('executor','coder','worker') ORDER BY created_at DESC,id DESC LIMIT 1")
                    .bind(&attempt.task_ref).fetch_optional(&mut **tx).await?;
                let execution = attempt.execution_id.clone().or(execution).ok_or_else(|| {
                    ServiceError::invalid_operation(
                        "the head check has no implementation execution for Review evidence",
                    )
                })?;
                let id = db::new_uuid_v4();
                let now = db::now_rfc3339();
                sqlx::query("INSERT INTO review(id,task_id,execution_id,attempt_number,status,step_results_json,started_at,finished_at,created_at,updated_at) VALUES(?,?,?,1,?,'{}',?,?,?,?)")
                    .bind(&id).bind(&attempt.task_ref).bind(execution)
                    .bind(if result.outcome == CheckResultOutcome::Pass { "passed" } else { "failed" })
                    .bind(&now).bind(&now).bind(&now).bind(&now).execute(&mut **tx).await?;
                (id, serde_json::json!({"ci_steps": []}))
            }
        };
        if !details.is_object() {
            return Err(ServiceError::invalid_operation(
                "Review evidence is not an object",
            ));
        }
        let execution = details
            .as_object_mut()
            .unwrap()
            .entry("execution")
            .or_insert_with(|| serde_json::json!({}))
            .as_object_mut()
            .ok_or_else(|| {
                ServiceError::invalid_operation("Review execution evidence is not an object")
            })?;
        let records = execution
            .entry("integration_checks")
            .or_insert_with(|| serde_json::json!([]))
            .as_array_mut()
            .ok_or_else(|| {
                ServiceError::invalid_operation("head-check evidence is not an array")
            })?;
        if records
            .iter()
            .any(|record| record["attempt_id"] == attempt.id && record["result_id"] == result.id)
        {
            return Ok(());
        }
        let consumer: Option<(String, String)> =
            sqlx::query_as("SELECT origin,created_at FROM check_consumer WHERE id=?")
                .bind(&application.consumer_id)
                .fetch_optional(&mut **tx)
                .await?;
        let reuse = consumer.as_ref().is_some_and(|(_, created)| {
            let created = chrono::DateTime::parse_from_rfc3339(created).ok();
            let finished = application
                .run
                .finished_at
                .as_deref()
                .and_then(|at| chrono::DateTime::parse_from_rfc3339(at).ok());
            matches!((created, finished), (Some(created), Some(finished)) if created >= finished)
        });
        let source = if consumer
            .as_ref()
            .is_some_and(|(origin, _)| origin == "entry")
        {
            "review_authority"
        } else if reuse {
            "cache_hit"
        } else {
            "executed"
        };
        let ci_steps: Vec<serde_json::Value> = result
            .commands
            .iter()
            .map(|command| {
                let mut value = serde_json::to_value(command).expect("command facts serialize");
                value["step_id"] = serde_json::json!(db::task_writer::current_step_id());
                value
            })
            .collect();
        records.push(serde_json::json!({
            "attempt_id": attempt.id, "commit_sha": application.run.identity.commit_sha,
            "consumer_id": application.consumer_id, "run_id": application.run.id,
            "result_id": result.id, "identity_key": result.identity_key,
            "outcome": result.outcome, "source": source, "ci_steps": ci_steps,
        }));
        if records.len() > 64 {
            records.drain(..records.len() - 64);
        }
        sqlx::query("UPDATE review SET step_results_json=?,updated_at=? WHERE id=?")
            .bind(details.to_string())
            .bind(db::now_rfc3339())
            .bind(id)
            .execute(&mut **tx)
            .await?;
        self.db
            .produce_condition_in_tx(tx, &attempt.task_ref, db::ConditionChange::Human)
            .await?;
        Ok(())
    }
}
