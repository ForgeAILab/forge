//! `request_check` and `settle`: the head's check, the review authority and
//! carry for the exact candidate, and the permit.
//!
//! One `settle` writes the worker's decision for an `effect_seq`. It is
//! reached two ways and both use [`IntegrationSteps::settle`]:
//! - the `settle` step, when the target did not move (no check is asked);
//! - the check's delivery (`apply_check_result`, through
//!   [`IntegrationCheckFamily`]), after a `request_check` for a rebased head.
use super::{ack_value, IntegrationSteps, StepOutcome, TaskStepIntegrationPort};
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
                    let failed = failed_command_message(&result.commands)
                        .unwrap_or_else(|| format!("the check ended {}", result.outcome));
                    CheckInput::Failed(timing, super::bounded_ci_failure(&failed))
                }
            }
        }
    }
}

/// The first failing command and the end of its combined output. The cause
/// of a red check is where its output ends; the bounded message keeps that
/// end rather than the oldest lines of the tail.
fn failed_command_message(commands: &[api_types::CheckCommandOutcome]) -> Option<String> {
    let command = commands.iter().find(|command| command.exit_code != 0)?;
    let head = format!(
        "`{}` exited {}: ",
        message_prefix(&command.command, 1024),
        command.exit_code
    );
    let failures = failure_signatures(&command.failures, command.failures_truncated);
    let output = output_end(&command.output_tail, &command.stderr_tail, 2 * 1024);
    Some(format!("{head}{failures}\n{output}"))
}

fn failure_signatures(failures: &[String], truncated: bool) -> String {
    let mut out = failures
        .iter()
        .take(20)
        .map(|signature| format!("\n- {}", message_prefix(signature, 300)))
        .collect::<String>();
    if failures.len() > 20 {
        out.push_str(&format!("\n+{} more", failures.len() - 20));
    }
    if truncated {
        out.push_str("\nAdditional failure signatures were truncated.");
    }
    out
}

/// Keep the failing test names and a separate 2 KiB output end. The triage
/// summary may contain many steps; it must not consume the output allowance.
fn triaged_failure_message(entries: &[api_types::StepResultEntry], summary: &str) -> String {
    let Some(step) = entries
        .iter()
        .find(|entry| crate::check_runner::ci_triage::blocks(entry))
    else {
        return super::bounded_ci_failure(summary);
    };
    let mut failures = Vec::new();
    let mut headers = Vec::new();
    let mut truncated = false;
    for entry in entries
        .iter()
        .filter(|entry| crate::check_runner::ci_triage::blocks(entry))
    {
        let mut signatures = entry.failures.clone();
        if let Some(triage) = &entry.triage {
            let classified: Vec<_> = triage
                .failures
                .iter()
                .filter(|failure| {
                    triage.mode != api_types::ReviewCiTriage::Full || failure.class.blocks()
                })
                .map(|failure| failure.signature.clone())
                .collect();
            if !triage.failures.is_empty() {
                signatures = classified;
            }
        }
        failures.extend(
            signatures
                .into_iter()
                .map(|signature| format!("CI-{} {signature}", entry.index)),
        );
        truncated |= entry.failures_truncated;
        let reason = entry
            .triage
            .as_ref()
            .and_then(|triage| triage.reason.as_deref())
            .unwrap_or_default();
        headers.push(format!(
            "`{}` exited {}{}{}",
            entry.command,
            entry.exit_code,
            if reason.is_empty() { "" } else { "; " },
            reason
        ));
    }
    format!(
        "{}{}\n{}",
        message_prefix(&headers.join("\n"), 2048),
        failure_signatures(&failures, truncated),
        output_end(&step.output_tail, &step.stderr_tail, 2 * 1024)
    )
}

fn message_prefix(text: &str, bytes: usize) -> &str {
    let mut end = bytes.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// The end of a step's combined output (stderr only when it is empty).
fn output_end<'a>(output_tail: &'a str, stderr_tail: &'a str, bytes: usize) -> &'a str {
    let output = if output_tail.trim().is_empty() {
        stderr_tail.trim()
    } else {
        output_tail.trim()
    };
    let mut start = output.len().saturating_sub(bytes);
    while !output.is_char_boundary(start) {
        start += 1;
    }
    &output[start..]
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
        let live: bool = self
            .db
            .integration_task_entry_matches(
                &attempt.task_ref,
                &attempt.expected_status,
                attempt.expected_epoch,
            )
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
                        authority: check_authority(&attempt).ok_or_else(|| {
                            ServiceError::invalid_operation("the head has no candidate commit")
                        })?,
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
        let live: bool = self
            .db
            .integration_task_entry_matches(
                &attempt.task_ref,
                &attempt.expected_status,
                attempt.expected_epoch,
            )
            .await?;
        let Some(task) = task.filter(|_| live) else {
            return self.answer_task_left(step, request, states).await;
        };

        // Every candidate failure is triaged before an ejection/send-back can
        // be authorised. Subcheck deliveries retain the head's authority.
        let mut triaged_steps = None;
        let check = if matches!(check, CheckInput::Failed(..)) {
            if let Some(application) = evidence {
                let project = db::ProjectRepo::get_by_id(&*self.db, &task.project_id)
                    .await?
                    .ok_or(db::DbError::NotFound)?;
                let workflow = WorkflowEngine::resolve_workflow_for_task(
                    &task,
                    &project.workflow_definition,
                    &crate::worker_runtime::queue::cascade_actor(),
                );
                let config = workflow
                    .states
                    .iter()
                    .find(|state| state.name == default_states::REVIEW)
                    .map(|state| {
                        crate::workflow::engine::effective_state_config(
                            state,
                            Some(&project),
                            task.task_state_config.as_deref(),
                        )
                    })
                    .unwrap_or_default();
                let consumers = self.task_service.check_consumers().ok_or_else(|| {
                    ServiceError::invalid_operation("check consumers are not composed")
                })?;
                match crate::check_runner::ci_triage::triage(
                    &self.db,
                    &consumers,
                    &task,
                    application,
                    &config,
                    attempt.target_tip_sha.as_deref(),
                )
                .await?
                {
                    crate::check_runner::ci_triage::Progress::Pending(_) => {
                        if let Some(step) = step {
                            let mut tx = db::begin_immediate(self.db.pool()).await?;
                            self.db
                                .finish_step_in_tx(&mut tx, step, "done", None)
                                .await?;
                            tx.commit().await?;
                        }
                        return Ok(StepOutcome::Settled);
                    }
                    crate::check_runner::ci_triage::Progress::Infrastructure(consumer) => {
                        CheckInput::NoVerdict(format!(
                            "CI triage check {consumer} produced no verdict after its infrastructure retries"
                        ))
                    }
                    crate::check_runner::ci_triage::Progress::Complete(entries) => {
                        let blocking = entries.iter().any(crate::check_runner::ci_triage::blocks);
                        let triaged = entries.iter().any(|entry| entry.triage.is_some());
                        let summary = triaged
                            .then(|| crate::check_runner::ci_triage::failure_summary(&entries));
                        triaged_steps = Some(entries);
                        match check {
                            // Only failures triage classified may pass. A red
                            // result it never looked at (timed out, `off`, or
                            // no failing step recorded) stays red.
                            CheckInput::Failed(timing, _) if triaged && !blocking => {
                                CheckInput::Passed(timing)
                            }
                            CheckInput::Failed(timing, _)
                                if summary.as_ref().is_some_and(|summary| !summary.is_empty()) =>
                            {
                                CheckInput::Failed(
                                    timing,
                                    triaged_failure_message(
                                        triaged_steps.as_deref().unwrap_or_default(),
                                        summary.as_deref().unwrap_or_default(),
                                    ),
                                )
                            }
                            other => other,
                        }
                    }
                }
            } else {
                check
            }
        } else {
            check
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
                let preempting: bool = self
                    .db
                    .integration_preempting_owner_command_in_tx(&mut tx, &task.id, &own)
                    .await?;
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
            self.record_head_review_in_tx(&mut tx, &attempt, application, triaged_steps.as_deref())
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
        let subcheck = application
            .run
            .identity
            .inputs
            .spec
            .commands
            .first()
            .is_some_and(|command| {
                command.id.starts_with("triage-rerun:") || command.id.starts_with("triage-base:")
            });
        let original = if subcheck {
            let task = TaskRepo::get_by_id(&*self.steps.db, &application.task_id, false)
                .await?
                .ok_or(db::DbError::NotFound)?;
            Some(
                crate::check_runner::ci_triage::original_application(
                    &self.steps.db,
                    &task,
                    application.status_epoch,
                    CheckConsumerOrigin::Integration,
                    &application.authority,
                )
                .await?,
            )
        } else {
            None
        };
        let application = original.as_ref().unwrap_or(application);
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

/// What the `settle` step of an unchanged head found to reuse.
pub(crate) enum HeadReuse {
    /// The step is ahead of the worker's transition: it retries.
    Early,
    None,
    Found(Box<CheckApplication>),
}

impl IntegrationSteps {
    /// An unchanged reviewed commit needs no new run. Try the runner's real
    /// certified cache hit; a frozen/unattested policy retains the passed
    /// Review's own result as authority rather than launching it again.
    pub(crate) async fn reuse_review_check(
        &self,
        request: &IntegrationStepRequest,
    ) -> Result<HeadReuse> {
        let Some(attempt) = self.db.integration_attempt(&request.attempt_id).await? else {
            return Ok(HeadReuse::None);
        };
        // Only the step that will write this `effect_seq`'s answer records a
        // reuse: a stale or already answered step writes no consumer. A step
        // that arrived before the worker's transition is early for the reuse
        // too: it must not be settled by a second read that finds it due,
        // or the permit would be written without the reuse and its evidence.
        match db::integration_step_position(
            &attempt,
            request.generation,
            request.effect_seq,
            true,
            IntegrationStepAction::Settle.as_str(),
            &[S::AwaitingTaskStep],
        ) {
            IntegrationStepPosition::Due => {}
            IntegrationStepPosition::NotYet => return Ok(HeadReuse::Early),
            IntegrationStepPosition::Answered | IntegrationStepPosition::Stale => {
                return Ok(HeadReuse::None)
            }
        }
        Ok(match self.reuse_review_check_due(request, attempt).await? {
            Some(application) => HeadReuse::Found(Box::new(application)),
            None => HeadReuse::None,
        })
    }

    async fn reuse_review_check_due(
        &self,
        request: &IntegrationStepRequest,
        attempt: IntegrationAttempt,
    ) -> Result<Option<CheckApplication>> {
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
        let check_commit = check.identity.commit_sha.clone();
        // The newest passed Review of this Task whose own entry check passed
        // under exactly this identity (commit, target, commands, policy,
        // environment, workspace). The consumer's key is rebuilt with the
        // function review entry asks with, so the two cannot drift apart.
        let reviews: Vec<String> = self.db.integration_passed_review_ids(&task.id).await?;
        let mut prior = None;
        for review in reviews {
            let row: Option<(String, i64)> = self
                .db
                .integration_passed_review_entry_consumer(&task.id, &key, &review)
                .await?;
            let Some((consumer_id, epoch)) = row else {
                continue;
            };
            let expected = TaskCheckRequest {
                task_id: task.id.clone(),
                status_epoch: epoch,
                authority: review,
                origin: CheckConsumerOrigin::Entry,
                purpose: CheckPurpose::EntryCi,
                identity: check.identity.clone(),
                workspace_id: None,
                machine_id: None,
                wall_timeout_seconds: check.wall_timeout_seconds,
            }
            .request_key();
            let Some(delivery) = self.db.check_consumer_delivery(&consumer_id).await? else {
                continue;
            };
            if delivery.consumer.request_key != expected {
                continue;
            }
            if let (Some(run_id), Some(result_id)) =
                (delivery.consumer.run_id, delivery.consumer.result_id)
            {
                prior = Some((consumer_id, run_id, result_id));
                break;
            }
        }
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
        if result.identity_key != key
            || run.identity_key != key
            || result.run_id != run.id
            || result.outcome != CheckResultOutcome::Pass
            || run.identity.commit_sha != check_commit
        {
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
        triaged_steps: Option<&[api_types::StepResultEntry]>,
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
        let row: Option<(String, String)> = self
            .db
            .integration_latest_review_details_in_tx(tx, &attempt.task_ref)
            .await?;
        let (id, mut details) = match row {
            Some((id, raw)) => match serde_json::from_str::<serde_json::Value>(&raw) {
                Ok(details) => (id, details),
                Err(_) => return Ok(()),
            },
            // A Task that reached `merging` with no Review (the owner moved
            // it, or its workflow has no review) has no Review to carry the
            // evidence. A check result is not a Review: none is created from
            // it (a `passed` row would read as review authority, a `failed`
            // one as a spent review attempt). The verdict stays on the
            // attempt's answer and on the consumer, run and result rows.
            None => return Ok(()),
        };
        // Evidence is an annotation: a Review whose details it cannot be
        // added to (a pre-object array, say) must not fail the settle.
        if !details.is_object() {
            return Ok(());
        }
        let Some(records) = details
            .as_object_mut()
            .and_then(|details| {
                details
                    .entry("execution")
                    .or_insert_with(|| serde_json::json!({}))
                    .as_object_mut()
            })
            .and_then(|execution| {
                execution
                    .entry("integration_checks")
                    .or_insert_with(|| serde_json::json!([]))
                    .as_array_mut()
            })
        else {
            return Ok(());
        };
        if records
            .iter()
            .any(|record| record["attempt_id"] == attempt.id && record["result_id"] == result.id)
        {
            return Ok(());
        }
        // How the head came by this result, from the rows themselves:
        // `review_authority` is the passed Review's own entry consumer (the
        // result is not a certified reusable one); `executed` is a run that
        // was scheduled for this consumer (it is the run's first consumer);
        // anything else is a consumer recorded on a run that already existed.
        let origin: Option<String> = self
            .db
            .integration_check_consumer_origin_in_tx(tx, &application.consumer_id)
            .await?;
        let first: Option<String> = self
            .db
            .integration_first_check_consumer_in_tx(tx, &application.run.id)
            .await?;
        let source = if origin.as_deref() == Some("entry") {
            "review_authority"
        } else if first.as_deref() == Some(application.consumer_id.as_str()) {
            "executed"
        } else {
            "cache_hit"
        };
        let steps = triaged_steps
            .map(<[_]>::to_vec)
            .unwrap_or_else(|| crate::check_runner::ci_triage::steps(result));
        let ci_steps: Vec<serde_json::Value> = steps
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
        self.db
            .annotate_integration_review_details_in_tx(
                tx,
                details.to_string(),
                db::now_rfc3339(),
                id,
            )
            .await?;
        self.db
            .produce_condition_in_tx(tx, &attempt.task_ref, db::ConditionChange::Human)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod failed_message_tests {
    use super::*;

    #[test]
    fn a_red_check_message_keeps_the_end_of_the_combined_output() {
        let command = api_types::CheckCommandOutcome {
            index: 0,
            command: "cargo test".into(),
            exit_code: 101,
            stderr_tail: format!(
                "{}error: test failed, to rerun pass `--lib`",
                "   Compiling dep v0.1.0\n".repeat(100)
            ),
            failures: vec!["parser::empty".into()],
            failures_truncated: false,
            output_tail: format!(
                "{}test parser::empty ... FAILED\ntest result: FAILED. 1 passed; 1 failed",
                "   Compiling dep v0.1.0\n".repeat(100)
            ),
            started_at: "now".into(),
            finished_at: "now".into(),
        };
        let message = failed_command_message(&[command]).unwrap();
        assert!(
            message.starts_with("`cargo test` exited 101: "),
            "{message}"
        );
        assert!(
            message.ends_with("test result: FAILED. 1 passed; 1 failed"),
            "{message}"
        );
        assert!(
            message.contains("test parser::empty ... FAILED"),
            "{message}"
        );
        assert!(message.len() <= super::super::CI_MESSAGE_LIMIT);
    }
    #[test]
    fn merge_ci_failure_names_twenty_tests_and_keeps_two_kib_output_end() {
        let command = api_types::CheckCommandOutcome {
            index: 0,
            command: "cargo test".into(),
            exit_code: 101,
            stderr_tail: String::new(),
            failures: (0..25).map(|i| format!("binary::case_{i}")).collect(),
            failures_truncated: false,
            output_tail: format!("{}END", "x".repeat(4096)),
            started_at: "now".into(),
            finished_at: "now".into(),
        };
        let message = failed_command_message(std::slice::from_ref(&command)).unwrap();
        for i in 0..20 {
            assert!(
                message.contains(&format!("- binary::case_{i}\n")),
                "{message}"
            );
        }
        assert!(message.contains("+5 more"));
        assert!(message.ends_with(&format!("{}END", "x".repeat(2045))));
        let entry: api_types::StepResultEntry =
            serde_json::from_value(serde_json::to_value(command).unwrap()).unwrap();
        let message =
            triaged_failure_message(std::slice::from_ref(&entry), "base comparison unavailable");
        assert!(message.contains("binary::case_19") && message.contains("+5 more"));
        assert!(message.ends_with(&format!("{}END", "x".repeat(2045))));
        let mut second = entry.clone();
        second.index = 1;
        second.command = "pytest".into();
        second.failures = vec!["other-test".into()];
        let message = triaged_failure_message(&[entry, second], "unused");
        assert!(message.contains("+6 more"));
        assert_eq!(message.matches("\n- CI-").count(), 20);
    }
}
