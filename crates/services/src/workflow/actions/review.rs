use std::sync::Arc;

use async_trait::async_trait;
use db::{now_rfc3339, ProjectRepo, ReviewRepo, ReviewStatus, TaskRepo};
use serde_json::{json, Value};

use crate::workflow::{
    default_states, effective_role, engine::WorkflowEngine, HookAction, HookContext, HookResult,
};

use super::common::{
    block_task, cancel_review_after_authority_loss, create_review_attempt_with_authority,
    get_role_assignment, latest_executor_execution, latest_review, publish_review_failed,
    publish_review_passed, review_ci_steps, review_has_auditor_verdict, review_is_ci_only, task,
    task_execution_is_read_only, update_review_status_with_authority_checks, workspace_id,
};

pub(super) async fn resolve_workspace(
    ctx: &HookContext,
    workspace: &db::Workspace,
) -> crate::workspace_backend::Result<crate::workspace_backend::ResolvedWorkspace> {
    ctx.workspace_backend_router
        .resolve(&ctx.db, workspace)
        .await
}

pub struct RunCiSteps;

/// Both transition entry and recovered dispatch resolve the same candidate.
/// An empty configured gate must not silently skip its required validation.
pub(super) async fn prepare_review_entry(
    ctx: &HookContext,
    task: &db::Task,
) -> Result<Option<HookResult>, String> {
    let Some(state) = ctx
        .workflow
        .states
        .iter()
        .find(|state| state.name == ctx.to_state)
    else {
        return Ok(None);
    };
    // Every review-phase gate, also one held by another role or by nobody (an
    // owner-approval gate): wider than the review lifecycle's
    // `db::budget::is_review_state`, which needs the reviewer role.
    if state.kind != api_types::StateKind::Gate
        || state.canonical_phase != Some(api_types::CanonicalPhase::Review)
    {
        return Ok(None);
    }
    // An Agent reviewer is refused dispatch for want of a candidate and
    // configured checks are skipped without one. A human reviewer and an
    // owner-approval gate wait for a person and keep the decisions they
    // always had.
    let agent_reviewer = match effective_role(state) {
        Some(role) => get_role_assignment(ctx, role)
            .await?
            .is_some_and(|assignment| {
                assignment.assignee_type == Some(db::AssigneeKind::Agent)
                    && assignment.assignee_id.is_some()
            }),
        None => false,
    };
    // A read-only Task runs no implementation checks, with or without a
    // candidate: its skipped checks are not a missing validation. Nor are
    // those of a Task with no Workspace: the owner moves such a Task across
    // the board by hand and its checks have nowhere to run, as before.
    let checks = (!review_ci_steps(&ctx.state_config)?.is_empty()
        || review::verify::config(&ctx.state_config)?.is_some())
        && (workspace_id(ctx).await.is_some()
            || match task.parent_task_id.as_deref() {
                Some(parent) => db::WorkspaceRepo::get_by_task_id(&*ctx.db, parent)
                    .await
                    .map_err(|error| error.to_string())?
                    .is_some(),
                None => false,
            })
        && !task_execution_is_read_only(ctx, task).await?;
    if !(agent_reviewer || checks) || task.review_passed_at.is_some() {
        return Ok(None);
    }
    // The branch is on a machine that is away: nothing is decided now.
    if ctx
        .task_service
        .review_candidate_workspace_away(task)
        .await
        .map_err(|error| error.to_string())?
    {
        return Ok(None);
    }
    let project = ProjectRepo::get_by_id(&*ctx.db, &ctx.project_id)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "review Project is missing".to_owned())?;
    if ctx
        .task_service
        .prepare_review_candidate(task, &project)
        .await
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Ok(None);
    }
    let missing = ctx
        .task_service
        .missing_review_candidate(task)
        .await
        .map_err(|error| error.to_string())?;
    Ok(Some(match missing.target {
        Some(to) => HookResult::Cascade {
            to,
            reason: missing.reason,
            bridge: missing.bridge,
        },
        None => HookResult::Skipped {
            reason: missing.reason,
        },
    }))
}

#[async_trait]
impl HookAction for RunCiSteps {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        let task = match task(ctx).await {
            Ok(task) => task,
            Err(reason) => return HookResult::Failed { reason },
        };
        match prepare_review_entry(ctx, &task).await {
            Ok(Some(result)) => return result,
            Ok(None) => {}
            Err(reason) => return HookResult::Failed { reason },
        }
        let read_only = match task_execution_is_read_only(ctx, &task).await {
            Ok(read_only) => read_only,
            Err(reason) => return HookResult::Failed { reason },
        };
        if read_only {
            return HookResult::Skipped {
                reason: "read-only Task does not run implementation ci steps".to_owned(),
            };
        }
        let ci_steps = match review_ci_steps(&ctx.state_config) {
            Ok(ci_steps) => ci_steps,
            Err(reason) => return HookResult::Failed { reason },
        };
        let verify = match review::verify::config(&ctx.state_config) {
            Ok(verify) => verify,
            Err(reason) => return HookResult::Failed { reason },
        };
        if ci_steps.is_empty() && verify.is_none() {
            if let Err(error) =
                crate::placement::admission::resolve_review_ci_attention(&ctx.db, &ctx.task_id)
                    .await
            {
                return HookResult::Failed {
                    reason: error.to_string(),
                };
            }
            return HookResult::Skipped {
                reason: "no ci steps".to_string(),
            };
        }

        let Some(_) = workspace_id(ctx).await else {
            return HookResult::Skipped {
                reason: "no workspace".to_string(),
            };
        };
        // A coordination root has no implementation execution of its own.
        // Review its shared workspace against the latest relevant child
        // execution instead of accidentally binding CI to a reviewer or
        // other non-implementation execution from the root context.
        let execution_id = latest_executor_execution(ctx)
            .await
            .map(|execution| execution.id);
        let Some(execution_id) = execution_id else {
            return HookResult::Skipped {
                reason: "no executor execution".to_string(),
            };
        };

        let consumers = match ctx.task_service.check_consumers() {
            Some(consumers) => consumers,
            None => {
                return HookResult::Failed {
                    reason: "check consumers are not composed".to_owned(),
                }
            }
        };
        // The review attempt this hook opened, when it already ran: a woken
        // or redelivered step continues that attempt and never opens a second.
        let asked =
            match crate::workflow::engine::durable::hook_effect(&ctx.task_id, CI_REVIEW).await {
                // An effect is stored as JSON text.
                Ok(asked) => asked.and_then(|raw| serde_json::from_str::<AskedReview>(&raw).ok()),
                Err(error) => {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    }
                }
            };
        // The entry configuration changed while this step waited for its
        // check: the check that ran answers commands the entry no longer has.
        // Its consumer and the attempt that asked are closed, and the hook
        // asks again below for the current configuration.
        let asked = match asked {
            Some(asked) => match supersede_stale_ask(ctx, &asked).await {
                Ok(true) => None,
                Ok(false) => Some(asked),
                Err(reason) => return HookResult::Failed { reason },
            },
            None => None,
        };
        let reasked = asked.is_none();
        let had_review_passed = task.review_passed_at.is_some();
        let reviewer_assignment =
            match get_role_assignment(ctx, crate::workflow::default_roles::REVIEWER).await {
                Ok(assignment) => assignment,
                Err(reason) => return HookResult::Failed { reason },
            };
        let reviewer_assigned = reviewer_assignment
            .as_ref()
            .is_some_and(|assignment| assignment.assignee_id.is_some());
        let (review, asked) = match asked {
            Some(asked) => match ReviewRepo::get_by_id(&*ctx.db, &asked.review_id).await {
                Ok(Some(review)) => (review, asked),
                Ok(None) => {
                    return HookResult::Failed {
                        reason: "the review attempt of this check no longer exists".to_owned(),
                    }
                }
                Err(error) => {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    }
                }
            },
            None => {
                let review = match create_review_attempt_with_authority(
                    ctx,
                    &execution_id,
                    task.version,
                    &execution_id,
                )
                .await
                {
                    Ok(review) => review,
                    Err(reason) => return HookResult::Failed { reason },
                };
                let asked = AskedReview {
                    review_id: review.id.clone(),
                    task_version: task.version,
                    review_status: review.status.to_string(),
                    review_updated_at: review.updated_at.clone(),
                };
                if let Err(error) = crate::workflow::engine::durable::record_hook_effect(
                    &ctx.task_id,
                    CI_REVIEW,
                    &serde_json::to_value(&asked).expect("asking-time authority serializes"),
                )
                .await
                {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
                (review, asked)
            }
        };
        // An earlier delivery of this step already wrote the CI result.
        if let Some(settled) = settled_ci_result(&review) {
            if review.status == ReviewStatus::Failed {
                if let Err(error) = ctx
                    .task_service
                    .publish_finished_review_comment(&review, None)
                    .await
                {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
            }
            return settled;
        }
        // The settlement below is fenced on the authority this hook asked
        // under, not on what the woken step re-reads: a Task version bump or
        // a Review row change while the check ran refuses the write exactly
        // where the inline run refused it, and cancels the attempt.
        let authority_version = asked.task_version;
        let asked_status = match asked.review_status.parse::<ReviewStatus>() {
            Ok(status) => status,
            Err(_) => {
                return HookResult::Failed {
                    reason: "the review attempt's asking-time authority is unreadable".to_owned(),
                }
            }
        };
        let review = db::Review {
            status: asked_status,
            updated_at: asked.review_updated_at.clone(),
            ..review
        };
        let consumer_id =
            match crate::workflow::engine::durable::hook_effect(&ctx.task_id, CI_CONSUMER).await {
                // A consumer recorded for an attempt this invocation did not
                // open belongs to that attempt.
                Ok(Some(consumer_id)) if !reasked => {
                    serde_json::from_str::<String>(&consumer_id).unwrap_or_default()
                }
                Ok(_) => match request_entry_check(ctx, &task, &review, &consumers).await {
                    Ok(asked) => {
                        let consumer_id = match asked {
                            EntryCheck::NothingToRun => String::new(),
                            EntryCheck::Asked(consumer_id) => consumer_id,
                        };
                        for (key, value) in [
                            (ENTRY_CHECK_CONFIG_EFFECT, ctx.state_config.clone()),
                            (CI_CONSUMER, json!(consumer_id)),
                        ] {
                            if let Err(error) =
                                crate::workflow::engine::durable::record_hook_effect(
                                    &ctx.task_id,
                                    key,
                                    &value,
                                )
                                .await
                            {
                                return HookResult::Failed {
                                    reason: error.to_string(),
                                };
                            }
                        }
                        consumer_id
                    }
                    Err(result) => return result,
                },
                Err(error) => {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    }
                }
            };
        // The verdict is the result's outcome. Whether that result may be
        // reused by a later request is not read here: a pass is a pass.
        let result = if consumer_id.is_empty() {
            None
        } else {
            use crate::check_runner::consumer::CheckWaitState;
            match consumers.wait_state(&consumer_id).await {
                Ok(CheckWaitState::Result(result)) => Some(result),
                // No result yet, or no verdict after the automatic retries
                // (the Task is parked on the typed check condition and the
                // owner's `retry` asks again): the step waits. The deadline
                // is the re-arm for a delivery that never arrives.
                Ok(CheckWaitState::Pending | CheckWaitState::Exhausted) => {
                    let until = (chrono::Utc::now()
                        + chrono::Duration::seconds(entry_check_wake_seconds(&ctx.task_id)))
                    .to_rfc3339();
                    if !crate::workflow::engine::durable::suspend_hook(
                        &ctx.task_id,
                        &consumer_id,
                        until,
                    ) {
                        return HookResult::Failed {
                            reason: "review-entry CI runs from the Task's hooks step".to_owned(),
                        };
                    }
                    return HookResult::Ok;
                }
                // Nothing will ever answer: the consumer was cancelled or its
                // run row is gone. Waiting again would wait forever, so the
                // attempt is cancelled and the hook fails as an unfinished
                // check does.
                Ok(CheckWaitState::Lost) => {
                    // The run this consumer waited for is still being
                    // stopped (the forced deletion that cancelled it was
                    // refused because the run had not settled). A request
                    // now would join that run and get its cancellation, so
                    // the step looks again shortly, for two minutes at most.
                    match ctx
                        .db
                        .cancelled_consumer_run_is_stopping(
                            &consumer_id,
                            WITHDRAWN_RUN_STOPPING_SECONDS,
                        )
                        .await
                    {
                        Ok(true) => {
                            let until = (chrono::Utc::now()
                                + chrono::Duration::seconds(WITHDRAWN_RUN_RECHECK_SECONDS))
                            .to_rfc3339();
                            if crate::workflow::engine::durable::suspend_hook(
                                &ctx.task_id,
                                &consumer_id,
                                until,
                            ) {
                                return HookResult::Ok;
                            }
                        }
                        Ok(false) => {}
                        Err(error) => {
                            return HookResult::Failed {
                                reason: error.to_string(),
                            }
                        }
                    }
                    // A consumer withdrawn under a live step (a forced
                    // Project deletion that was then refused cancels them)
                    // is asked for again, like a changed configuration.
                    match ask_again(
                        ctx,
                        &consumer_id,
                        &review,
                        "superseded_by_withdrawn_check",
                        "the review check was withdrawn before it produced a result",
                    )
                    .await
                    {
                        Ok(true) => return self.execute(ctx).await,
                        Ok(false) => {}
                        Err(reason) => return HookResult::Failed { reason },
                    }
                    let reason = "review check was lost before it produced a result";
                    cancel_review_after_authority_loss(ctx, &review, reason).await;
                    return HookResult::Failed {
                        reason: reason.to_owned(),
                    };
                }
                Err(error) => {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    }
                }
            }
        };
        let (ci_results, mut failed_step_index) = match result {
            None => (Vec::new(), None),
            Some(result) => {
                let failed = result
                    .commands
                    .iter()
                    .position(|command| command.exit_code != 0);
                let unfinished = match result.outcome {
                    db::CheckResultOutcome::Pass => None,
                    db::CheckResultOutcome::Fail if failed.is_some() => None,
                    db::CheckResultOutcome::TimedOut => Some("review command timed out"),
                    _ => Some("review check did not finish"),
                };
                // A cancelled run is nobody's verdict on the candidate: the
                // entry joined a run that was being stopped for another
                // reason. It asks again.
                if result.outcome == db::CheckResultOutcome::Cancelled {
                    match ask_again(
                        ctx,
                        &consumer_id,
                        &review,
                        "superseded_by_withdrawn_check",
                        "the review check was cancelled before it produced a result",
                    )
                    .await
                    {
                        Ok(true) => return self.execute(ctx).await,
                        Ok(false) => {}
                        Err(reason) => return HookResult::Failed { reason },
                    }
                }
                if let Some(reason) = unfinished {
                    cancel_review_after_authority_loss(ctx, &review, reason).await;
                    return HookResult::Failed {
                        reason: reason.to_owned(),
                    };
                }
                let entries = if result.outcome == db::CheckResultOutcome::Fail {
                    let triaged = async {
                        let state =
                            db::CheckDeliveryRepo::check_consumer_delivery(&*ctx.db, &consumer_id)
                                .await?
                                .ok_or(db::DbError::NotFound)?;
                        let run = db::CheckRunRepo::check_run(&*ctx.db, &result.run_id)
                            .await?
                            .ok_or(db::DbError::NotFound)?;
                        let application = crate::check_runner::consumer::CheckApplication {
                            task_id: task.id.clone(),
                            status_epoch: state.consumer.status_epoch,
                            authority: review.id.clone(),
                            consumer_id: consumer_id.clone(),
                            run,
                            verdict: crate::check_runner::consumer::CheckVerdict::Result(
                                result.clone(),
                            ),
                        };
                        crate::check_runner::ci_triage::triage(
                            &ctx.db,
                            &consumers,
                            &task,
                            &application,
                            &ctx.state_config,
                            None,
                        )
                        .await
                    }
                    .await;
                    match triaged {
                        Ok(crate::check_runner::ci_triage::Progress::Complete(entries)) => entries,
                        Ok(
                            crate::check_runner::ci_triage::Progress::Pending(consumer)
                            | crate::check_runner::ci_triage::Progress::Infrastructure(consumer),
                        ) => {
                            let until = (chrono::Utc::now()
                                + chrono::Duration::seconds(entry_check_wake_seconds(
                                    &ctx.task_id,
                                )))
                            .to_rfc3339();
                            if !crate::workflow::engine::durable::suspend_hook(
                                &ctx.task_id,
                                &consumer,
                                until,
                            ) {
                                return HookResult::Failed {
                                    reason: "CI triage requires the Task hooks step".into(),
                                };
                            }
                            return HookResult::Ok;
                        }
                        Err(error) => {
                            return HookResult::Failed {
                                reason: error.to_string(),
                            }
                        }
                    }
                } else {
                    crate::check_runner::ci_triage::steps(&result)
                };
                let failed = entries
                    .iter()
                    .position(crate::check_runner::ci_triage::blocks);
                (
                    entries
                        .into_iter()
                        .map(|entry| serde_json::to_value(entry).expect("CI step serializes"))
                        .collect::<Vec<_>>(),
                    failed,
                )
            }
        };
        if let Err(error) =
            crate::placement::admission::resolve_review_ci_attention(&ctx.db, &ctx.task_id).await
        {
            return HookResult::Failed {
                reason: error.to_string(),
            };
        }
        let mut review_details = json!({ "ci_steps": ci_results });
        if let Some(attempt) = crate::workflow::engine::durable::current_hook(&ctx.task_id) {
            for entry in review_details["ci_steps"]
                .as_array_mut()
                .expect("CI results array")
            {
                entry["step_id"] = json!(attempt.step.id);
            }
        }
        if failed_step_index.is_none() && verify.is_some() {
            match super::verify::run(ctx, &task, &review, &consumers).await {
                Ok(super::verify::Progress::Waiting) => return HookResult::Ok,
                Ok(super::verify::Progress::Complete(verify)) => {
                    if matches!(
                        verify.status,
                        api_types::ReviewVerifyStatus::Fail
                            | api_types::ReviewVerifyStatus::Inconclusive
                    ) {
                        failed_step_index = Some(0);
                    }
                    review_details["verify"] =
                        serde_json::to_value(verify).expect("verify details serialize");
                }
                Err(super::verify::Error::Infrastructure(reason)) => {
                    cancel_review_after_authority_loss(ctx, &review, &reason).await;
                    return interrupt_ci(ctx, &task, &reason, true, false).await;
                }
                Err(super::verify::Error::Refused(reason)) => {
                    cancel_review_after_authority_loss(ctx, &review, &reason).await;
                    return HookResult::Failed { reason };
                }
            }
        }
        let now = now_rfc3339();

        let user_approval_required =
            gate_requires_user_approval(ctx) || human_review_requested(ctx, reviewer_assigned);

        let (status, finished_at) = if let Some(failed_step_index) = failed_step_index {
            let review = match ReviewRepo::update_status_with_review_authority_and_task_projection(
                &*ctx.db,
                &review.id,
                ReviewStatus::Failed,
                review_details.to_string(),
                Some(now.clone()),
                &now,
                authority_version,
                &ctx.to_state,
                ctx.project_version,
                ctx.project_workflow_definition.as_deref(),
                review.status.clone(),
                &review.updated_at,
                &execution_id,
                Some(None),
            )
            .await
            {
                Ok(review) => review,
                Err(error) => {
                    cancel_review_after_authority_loss(ctx, &review, &error.to_string()).await;
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
            };
            let settled_task = match TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false).await {
                Ok(Some(task)) => task,
                Ok(None) => {
                    return HookResult::Failed {
                        reason: "task disappeared after review settlement".to_owned(),
                    };
                }
                Err(error) => {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    }
                }
            };

            let memory_service = crate::MemoryService::new(Arc::clone(&ctx.db));
            if let Err(error) = memory_service
                .record_review_result_if_final(&ctx.project_id, &review)
                .await
            {
                tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
            }
            publish_review_failed(ctx, &review, failed_step_index);
            if let Err(error) = ctx
                .task_service
                .publish_finished_review_comment(&review, None)
                .await
            {
                return HookResult::Failed {
                    reason: error.to_string(),
                };
            }
            if review_details["verify"]["status"] == "inconclusive" {
                let reason = "verify remained inconclusive after its automatic re-run";
                let annotation = json!({"type":api_types::FailureKind::ReviewBlocked,"blocking_reason":"review_verify_inconclusive","message":reason,"blocked_at":now});
                if let Err(error) = super::common::block_task_with_annotation(
                    ctx,
                    &settled_task,
                    reason,
                    api_types::FailureKind::ReviewBlocked,
                    Some("verify"),
                    Some(annotation.to_string()),
                )
                .await
                {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
                let parked = match super::common::task(ctx).await {
                    Ok(task) => task,
                    Err(reason) => return HookResult::Failed { reason },
                };
                let barrier = json!({"state":ctx.to_state,"status":"blocked","interrupted_at":now,"started_at":now,"blocking_reason":reason});
                if let Err(error) = TaskRepo::set_entry_barrier(
                    &*ctx.db,
                    &parked.id,
                    parked.version,
                    Some(barrier.to_string()),
                    &now,
                )
                .await
                {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
                return HookResult::Failed {
                    reason: reason.into(),
                };
            }
            if had_review_passed && ctx.triggered_by.is_user() {
                let reason = "merge-fix follow-up failed: ci";
                if let Err(error) = block_task(
                    ctx,
                    &settled_task,
                    reason,
                    api_types::FailureKind::CiFailed,
                    None,
                )
                .await
                {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
                return HookResult::Failed {
                    reason: reason.to_string(),
                };
            }
            return HookResult::Failed {
                reason: verify_failure_summary(&review_details).unwrap_or_else(|| {
                    ci_failure_summary(&review_details["ci_steps"], failed_step_index)
                }),
            };
        } else if had_review_passed && !reviewer_assigned && !user_approval_required {
            review_details["auditor"] = json!({
                "verdict": "pass_ci_only",
                "reason": "CI-only re-review",
            });
            (ReviewStatus::Passed, Some(now.clone()))
        } else if reviewer_assigned {
            (ReviewStatus::Running, None)
        } else if user_approval_required {
            let reason = if gate_requires_user_approval(ctx) {
                "gate requires user approval"
            } else {
                "manual review requested"
            };
            review_details["user_approval"] = json!({
                "status": "awaiting_human",
                "reason": reason,
            });
            (ReviewStatus::AwaitingHuman, None)
        } else {
            (ReviewStatus::Passed, Some(now.clone()))
        };

        let review = if matches!(status, ReviewStatus::Passed | ReviewStatus::Failed) {
            match ReviewRepo::update_status_with_review_authority_and_task_projection(
                &*ctx.db,
                &review.id,
                status.clone(),
                review_details.to_string(),
                finished_at.clone(),
                &now,
                authority_version,
                &ctx.to_state,
                ctx.project_version,
                ctx.project_workflow_definition.as_deref(),
                review.status.clone(),
                &review.updated_at,
                &execution_id,
                Some((status == ReviewStatus::Passed).then_some(now.clone())),
            )
            .await
            {
                Ok(review) => review,
                Err(error) => {
                    cancel_review_after_authority_loss(ctx, &review, &error.to_string()).await;
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
            }
        } else {
            match update_review_status_with_authority_checks(
                ctx,
                &review,
                status.clone(),
                review_details.to_string(),
                finished_at.clone(),
                &now,
                authority_version,
                &execution_id,
            )
            .await
            {
                Ok(review) => review,
                Err(error) => {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
            }
        };

        let memory_service = crate::MemoryService::new(Arc::clone(&ctx.db));
        if let Err(error) = memory_service
            .record_review_result_if_final(&ctx.project_id, &review)
            .await
        {
            tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
        }

        if status == ReviewStatus::Passed {
            publish_review_passed(ctx, &review);
        }

        HookResult::Ok
    }
}

/// Hook effects of `run_ci_steps`: the review attempt it opened and the check
/// consumer it asked with. They make a woken or redelivered step continue.
const CI_REVIEW: &str = "ci_review";
const CI_CONSUMER: &str = "ci_consumer";
/// The entry state configuration the recorded consumer was asked under, and
/// how often this entry dropped a stale check and asked again.
pub(crate) const ENTRY_CHECK_CONFIG_EFFECT: &str = "ci_config";
pub(crate) const ENTRY_CHECK_REREQUESTS_EFFECT: &str = "ci_rerequests";
/// A withdrawn consumer whose run is still being stopped: the entry waits
/// for the run to end before it asks again, looking this often and for this
/// long after the consumer was cancelled.
const WITHDRAWN_RUN_RECHECK_SECONDS: i64 = if cfg!(test) { 1 } else { 5 };
const WITHDRAWN_RUN_STOPPING_SECONDS: i64 = 120;
/// An entry asks again at most this often (a configuration that keeps
/// changing, or a check that keeps being withdrawn); then it parks typed.
pub(crate) const ENTRY_CHECK_REREQUEST_LIMIT: u32 = 3;

async fn entry_check_rerequests(task_id: &str) -> Result<u32, String> {
    Ok(
        crate::workflow::engine::durable::hook_effect(task_id, ENTRY_CHECK_REREQUESTS_EFFECT)
            .await
            .map_err(|error| error.to_string())?
            .and_then(|raw| raw.parse::<u32>().ok())
            .unwrap_or(0),
    )
}

/// Whether the check this hook asked for is stale because the entry's state
/// configuration is no longer the one it was asked under. If so (and the
/// entry may still ask again), the stale consumer is cancelled, a run it
/// alone waited for on a daemon is fenced, the attempt that asked is closed
/// as superseded and the re-request is counted. No budget is charged: the
/// attempt never produced a verdict. Returns `true` when the caller must ask
/// again.
async fn supersede_stale_ask(ctx: &HookContext, asked: &AskedReview) -> Result<bool, String> {
    let durable_effect = |key: &'static str| async move {
        crate::workflow::engine::durable::hook_effect(&ctx.task_id, key)
            .await
            .map_err(|error| error.to_string())
    };
    let Some(consumer_id) = durable_effect(CI_CONSUMER)
        .await?
        .and_then(|raw| serde_json::from_str::<String>(&raw).ok())
    else {
        return Ok(false);
    };
    let asked_under = durable_effect(ENTRY_CHECK_CONFIG_EFFECT)
        .await?
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok());
    if asked_under.as_ref() == Some(&ctx.state_config) {
        return Ok(false);
    }
    let Some(review) = ReviewRepo::get_by_id(&*ctx.db, &asked.review_id)
        .await
        .map_err(|error| error.to_string())?
    else {
        return Ok(false);
    };
    // An attempt that already has its verdict is not asked again.
    if settled_ci_result(&review).is_some() {
        return Ok(false);
    }
    ask_again(
        ctx,
        &consumer_id,
        &review,
        "superseded_by_config_change",
        "the review configuration changed while its checks ran",
    )
    .await
}

/// The check this entry waits for will not produce a verdict for it (its
/// configuration changed, the check was withdrawn, or the run it joined was
/// cancelled for somebody else). Close the consumer and the attempt that
/// asked and count the re-request; the caller then runs the hook again, which
/// opens a new attempt and asks once for the current configuration. `false`
/// when the entry may not ask again: the bound is reached, the step's
/// authority was withdrawn, or the hook does not run from a durable step.
async fn ask_again(
    ctx: &HookContext,
    consumer_id: &str,
    review: &db::Review,
    status: &str,
    reason: &str,
) -> Result<bool, String> {
    if ctx.project_version.is_some_and(|version| version < 0)
        || crate::workflow::engine::durable::current_hook(&ctx.task_id).is_none()
    {
        return Ok(false);
    }
    let rerequests = entry_check_rerequests(&ctx.task_id).await?;
    if rerequests >= ENTRY_CHECK_REREQUEST_LIMIT {
        return Ok(false);
    }
    // Counted before anything is closed: a step that dies or fails between
    // here and the new request has still used one of its three, so no
    // sequence of failures asks again without bound.
    crate::workflow::engine::durable::record_hook_effect(
        &ctx.task_id,
        ENTRY_CHECK_REREQUESTS_EFFECT,
        &json!(rerequests + 1),
    )
    .await
    .map_err(|error| error.to_string())?;
    if !drop_entry_check(ctx, consumer_id, review, status, reason).await? {
        return Ok(false);
    }
    // No attempt is recorded any more: the hook opens the next one.
    crate::workflow::engine::durable::record_hook_effect(&ctx.task_id, CI_REVIEW, &Value::Null)
        .await
        .map_err(|error| error.to_string())?;
    Ok(true)
}

/// Close a check this entry no longer waits for: first the attempt that
/// asked, then its consumer. `false` when the attempt is no longer the one
/// this hook asked with (somebody else wrote the Review row meanwhile) and
/// is still open: nothing is closed then and the entry does not ask again,
/// so one entry never has two open attempts.
async fn drop_entry_check(
    ctx: &HookContext,
    consumer_id: &str,
    review: &db::Review,
    status: &str,
    reason: &str,
) -> Result<bool, String> {
    let mut details: Value = serde_json::from_str(&review.step_results_json)
        .unwrap_or_else(|_| json!({ "ci_steps": [] }));
    if !details.is_object() {
        details = json!({ "ci_steps": [] });
    }
    let now = now_rfc3339();
    details["execution_retry"] = json!({ "status": status, "reason": reason, "cancelled_at": now });
    let closed = ReviewRepo::cancel_if_unchanged(
        &*ctx.db,
        &review.id,
        review.status.clone(),
        &review.updated_at,
        details.to_string(),
        &now,
        &now,
    )
    .await
    .map_err(|error| error.to_string())?;
    if closed.is_none() {
        // A redelivered step finds the attempt it already closed.
        let current = ReviewRepo::get_by_id(&*ctx.db, &review.id)
            .await
            .map_err(|error| error.to_string())?;
        if current.is_some_and(|current| current.status != ReviewStatus::Cancelled) {
            return Ok(false);
        }
    }
    if let Some(raw) = crate::workflow::engine::durable::hook_effect(&ctx.task_id, "review_verify")
        .await
        .map_err(|e| e.to_string())?
    {
        if let Ok(saved) = serde_json::from_str::<Value>(&raw) {
            if saved["review_id"] == review.id {
                if let Some(verify_consumer) = saved["consumer_id"].as_str() {
                    crate::check_runner::review_entry::withdraw_consumer(
                        &ctx.db,
                        ctx.daemon_connections.clone(),
                        None,
                        verify_consumer,
                    )
                    .await
                    .map_err(|e| e.to_string())?;
                }
            }
        }
    }
    if !consumer_id.is_empty() {
        let step_id = crate::workflow::engine::durable::current_hook(&ctx.task_id)
            .map(|attempt| attempt.step.id.clone());
        crate::check_runner::review_entry::withdraw_consumer(
            &ctx.db,
            ctx.daemon_connections.clone(),
            step_id.as_deref(),
            consumer_id,
        )
        .await
        .map_err(|error| error.to_string())?;
    }
    Ok(true)
}
/// The wall limit of one review-entry CI run.
pub const ENTRY_CHECK_WALL_SECONDS: u64 = 3600;
static ENTRY_CHECK_WALL_OVERRIDE: std::sync::Mutex<Vec<(String, u64)>> =
    std::sync::Mutex::new(Vec::new());
/// The wall limit in force for this Task's entry. A test shortens it for its
/// own Task to drive a timeout through the runner; production never does.
fn entry_check_wall_seconds(task_id: &str) -> u64 {
    ENTRY_CHECK_WALL_OVERRIDE
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .find(|(id, _)| id == task_id)
        .map_or(ENTRY_CHECK_WALL_SECONDS, |(_, seconds)| *seconds)
}
#[doc(hidden)]
pub fn set_entry_check_wall_seconds_for_test(task_id: &str, seconds: u64) {
    ENTRY_CHECK_WALL_OVERRIDE
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .push((task_id.to_owned(), seconds));
}
/// Test seam: a runtime is stopped at a named point of a Task's review
/// entry. An armed Task's step parks there for good and says so; the test
/// then drops the runtime, as a process that died at that point. Nothing is
/// armed outside tests, and an unarmed Task passes straight through.
#[doc(hidden)]
pub mod halt {
    use std::sync::{Arc, Mutex};
    use tokio::sync::Notify;

    /// The woken hooks step is claimed and about to read its check result.
    pub const AFTER_WAKE_BEFORE_SETTLE: &str = "after_wake_before_settle";
    /// The hooks step asked for its entry check (the consumer is stored) and
    /// has not suspended on it yet.
    pub const AFTER_REQUEST_BEFORE_SUSPEND: &str = "after_request_before_suspend";

    type Armed = (String, &'static str, Arc<Notify>);
    static ARMED: Mutex<Vec<Armed>> = Mutex::new(Vec::new());

    /// The returned handle is notified when the Task's step reaches `point`.
    pub fn arm(task_id: &str, point: &'static str) -> Arc<Notify> {
        let reached = Arc::new(Notify::new());
        ARMED.lock().unwrap_or_else(|p| p.into_inner()).push((
            task_id.to_owned(),
            point,
            reached.clone(),
        ));
        reached
    }

    pub fn disarm(task_id: &str) {
        ARMED
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|(id, _, _)| id != task_id);
    }

    pub(crate) async fn reach(task_id: &str, point: &'static str) {
        let reached = ARMED
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .find(|(id, at, _)| id == task_id && *at == point)
            .map(|(_, _, reached)| reached.clone());
        if let Some(reached) = reached {
            reached.notify_one();
            std::future::pending::<()>().await;
        }
    }
}
/// When a suspended step looks again on its own: past the run's wall limit
/// and the worker's settlement grace.
fn entry_check_wake_seconds(task_id: &str) -> i64 {
    entry_check_wall_seconds(task_id) as i64 + 120
}

/// The authority `run_ci_steps` asked under, carried across the suspension:
/// the review attempt it opened and the versions the settlement is fenced on.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct AskedReview {
    review_id: String,
    task_version: i64,
    review_status: String,
    review_updated_at: String,
}

enum EntryCheck {
    /// Every configured step is blank: nothing runs and the entry passes.
    NothingToRun,
    Asked(String),
}

/// What an earlier delivery of this hook already settled on the Review.
fn settled_ci_result(review: &db::Review) -> Option<HookResult> {
    let details: Value = serde_json::from_str(&review.step_results_json).unwrap_or(Value::Null);
    let steps = details.get("ci_steps");
    match review.status {
        ReviewStatus::Failed => {
            let failed = steps
                .and_then(Value::as_array)
                .and_then(|steps| steps.iter().position(|step| step["exit_code"] != json!(0)))
                .unwrap_or(0);
            Some(HookResult::Failed {
                reason: verify_failure_summary(&details)
                    .unwrap_or_else(|| ci_failure_summary(steps.unwrap_or(&Value::Null), failed)),
            })
        }
        ReviewStatus::Cancelled => Some(HookResult::Failed {
            reason: "review check did not finish".to_owned(),
        }),
        // A fresh attempt is created with an empty `ci_steps` list; the
        // settlement writes one entry per command that ran.
        ReviewStatus::Running
            if steps
                .and_then(Value::as_array)
                .is_none_or(|steps| steps.is_empty())
                && details.get("verify").is_none() =>
        {
            None
        }
        _ => Some(HookResult::Ok),
    }
}

/// Ask the durable check runner for this entry's CI. The request is keyed by
/// the Task status entry and the review attempt, so asking again (a
/// redelivered step) returns the same consumer and never a second run.
async fn request_entry_check(
    ctx: &HookContext,
    task: &db::Task,
    review: &db::Review,
    consumers: &crate::check_runner::consumer::TaskCheckConsumers,
) -> Result<EntryCheck, HookResult> {
    let failed = |reason: String| HookResult::Failed { reason };
    if review::verify::config(&ctx.state_config)
        .map_err(failed)?
        .is_some()
        && review_ci_steps(&ctx.state_config)
            .map_err(failed)?
            .iter()
            .all(|command| command.trim().is_empty())
    {
        return Ok(EntryCheck::NothingToRun);
    }
    let workspace = match crate::task_service::workspace::prepare_workspace_for(
        &ctx.db,
        &ctx.workspace_root,
        task,
        &task.id,
        ctx.repo_cache_locks.clone(),
        &ctx.workspace_backend_router,
        crate::workspace_manager::Purpose::Check,
    )
    .await
    {
        Ok(workspace) => {
            super::common::disk_wait_over(ctx, task).await;
            workspace
        }
        Err(error) => {
            // The worktree would have to be made again under the disk
            // floor: the entry waits for disk and asks again. No command
            // ran and no attempt exists; the woken hook opens its own.
            if crate::placement::capacity_wait_of(&error)
                == Some(crate::placement::CapacityWait::Disk)
            {
                let _ = ctx.db.discard_unstarted_review(&review.id).await;
                if let Err(effect) = crate::workflow::engine::durable::record_hook_effect(
                    &ctx.task_id,
                    CI_REVIEW,
                    &Value::Null,
                )
                .await
                {
                    return Err(failed(effect.to_string()));
                }
                return Err(
                    match super::common::wait_for_disk(ctx, task, &error).await {
                        Some(waiting) => waiting,
                        None => failed(error.to_string()),
                    },
                );
            }
            let reset = matches!(error, crate::ServiceError::WorkspaceResetRequired { .. });
            let retry = matches!(
                error,
                crate::ServiceError::DaemonUnavailable { .. }
                    | crate::ServiceError::DaemonTimeout { .. }
            );
            // No command ran; this is not a Review attempt.
            let _ = ctx.db.discard_unstarted_review(&review.id).await;
            if reset || retry {
                return Err(interrupt_ci(ctx, task, &error.to_string(), retry, reset).await);
            }
            return Err(failed(error.to_string()));
        }
    };
    let resolved = resolve_workspace(ctx, &workspace)
        .await
        .map_err(|error| failed(error.to_string()))?;
    let daemon = resolved.placement.owner_kind == db::PlacementOwnerKind::Daemon;
    let state = match resolved.backend.describe(&resolved.placement).await {
        Ok(state) => state,
        Err(error) => {
            let retry = matches!(
                &error,
                crate::workspace_backend::WorkspaceBackendError::OwnerUnreachable { .. }
                    | crate::workspace_backend::WorkspaceBackendError::RpcTimeoutBeforeStart { .. }
            );
            let _ = ctx.db.discard_unstarted_review(&review.id).await;
            if retry || daemon {
                return Err(interrupt_ci(ctx, task, &error.to_string(), retry, false).await);
            }
            return Err(failed(error.to_string()));
        }
    };
    // A tree with uncommitted or untracked files is not what `commit_sha`
    // names: the entry runs and neither reuses a result nor leaves one.
    let dirty = state.dirty;
    let commit_sha = state
        .head_sha
        .ok_or_else(|| failed("review workspace has no HEAD".to_owned()))?;
    let identity = match crate::check_runner::review_entry::review_ci_identity(
        &ctx.db,
        crate::check_runner::review_entry::ReviewCiIdentityInput {
            task,
            workspace: &workspace,
            placement: &resolved.placement,
            commit_sha,
            state_config: &ctx.state_config,
            dirty,
            target_branch: None,
        },
    )
    .await
    {
        Ok(Some(identity)) => identity,
        Ok(None) => return Ok(EntryCheck::NothingToRun),
        Err(error) if error.is_check_environment_unavailable() => {
            // The owner's login shell gave no environment, so no identity
            // exists and nothing was asked. The first attempts are the hooks
            // step's own: its checkpoint (the opened attempt) stays and the
            // step asks again after a short back-off.
            let again =
                crate::workflow::engine::durable::current_hook(&ctx.task_id).is_some_and(|hook| {
                    hook.step.attempts < crate::worker_runtime::queue::ENVIRONMENT_ATTEMPTS
                });
            if again {
                crate::workflow::engine::durable::note_hook_failure(&ctx.task_id, &error);
                return Err(failed(error.to_string()));
            }
            // Still no answer: this is the review-CI infrastructure
            // interruption, as for an owner that cannot be reached. No
            // command ran, so this is not a Review attempt; the Task states
            // the cause, re-enters on the infrastructure budget's back-off
            // and, once that is spent, waits with `retry_hook`.
            let _ = ctx.db.discard_unstarted_review(&review.id).await;
            return Err(interrupt_ci(ctx, task, &error.to_string(), true, false).await);
        }
        Err(error) => return Err(failed(error.to_string())),
    };
    let status_epoch = db::CheckDeliveryRepo::live_task_epoch(&*ctx.db, &task.id)
        .await
        .map_err(|error| failed(error.to_string()))?
        .ok_or_else(|| failed("task disappeared before its review check".to_owned()))?;
    let requested = consumers
        .request(crate::check_runner::consumer::TaskCheckRequest {
            task_id: task.id.clone(),
            status_epoch,
            authority: review.id.clone(),
            origin: db::CheckConsumerOrigin::Entry,
            purpose: api_types::CheckPurpose::EntryCi,
            identity,
            workspace_id: Some(workspace.id.clone()),
            machine_id: resolved.placement.daemon_id.clone().filter(|_| daemon),
            wall_timeout_seconds: entry_check_wall_seconds(&task.id),
        })
        .await
        .map_err(|error| failed(error.to_string()))?;
    halt::reach(&task.id, halt::AFTER_REQUEST_BEFORE_SUSPEND).await;
    Ok(EntryCheck::Asked(requested.consumer.id))
}

/// Run `run_ci_steps` once the way the step queue runs it, for tests that
/// exercise the hook without the engine: as a claimed hooks step of the Task,
/// with the check it asks for executed by `worker` and the hook run again
/// when the result exists. The result's delivery step is not executed; the
/// step and the delivery are removed afterwards.
#[doc(hidden)]
pub async fn run_ci_steps_in_step(
    ctx: &HookContext,
    worker: &Arc<crate::check_runner::worker::CheckRunWorker>,
) -> HookResult {
    use crate::check_runner::consumer::CheckVerdict;
    let db = Arc::clone(&ctx.db);
    let id = db::new_uuid_v4();
    let entered: Option<(String, i64)> = db
        .review_hook_task_status_version(&ctx.task_id)
        .await
        .map_err(|error| -> Box<dyn std::fmt::Debug> {
            match error {
                db::DbError::Sqlx(error) => Box::new(error),
                error => Box::new(error),
            }
        })
        .expect("Task reads");
    let (status, version) = entered.unwrap_or_else(|| (ctx.to_state.clone(), 1));
    db::TaskStepRepo::enqueue_step(
        &*db,
        &db::EnqueueTaskStep {
            kind: "hooks".to_owned(),
            id: id.clone(),
            task_id: ctx.task_id.clone(),
            payload_json: "{}".to_owned(),
            causation_step_id: None,
            causation_key: id.clone(),
            chain_id: id.clone(),
            chain_position: 1,
            expected_status: status,
            expected_version: version,
            expected_epoch: None,
            lane: "long".to_owned(),
            available_at: now_rfc3339(),
        },
    )
    .await
    .expect("hooks step enqueues");
    db.claim_run_ci_helper_step(db::task_writer::lease_deadline(), &id)
        .await
        .map_err(|error| -> Box<dyn std::fmt::Debug> {
            match error {
                db::DbError::Sqlx(error) => Box::new(error),
                error => Box::new(error),
            }
        })
        .expect("hooks step claims");
    let step = db::TaskStepRepo::task_steps(&*db, &ctx.task_id)
        .await
        .expect("steps list")
        .into_iter()
        .find(|step| step.id == id)
        .expect("the hooks step exists");
    let run = async {
        db.start_hook(&step, 0).await.expect("hook starts");
        loop {
            let (result, wait) = crate::workflow::engine::durable::in_hook(
                Arc::clone(&db),
                step.clone(),
                0,
                RunCiSteps.execute(ctx),
            )
            .await;
            let Some(wait) = wait else {
                break result;
            };
            let consumers = ctx
                .task_service
                .check_consumers()
                .expect("check consumers are composed");
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
            while !matches!(
                consumers
                    .verdict(&wait.consumer_id)
                    .await
                    .expect("verdict reads"),
                Some(CheckVerdict::Result(_))
            ) {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "the review-entry check produced no result"
                );
                let mut jobs = tokio::task::JoinSet::new();
                worker.sweep(&mut jobs).await.expect("check sweep");
                while let Some(job) = jobs.join_next().await {
                    job.expect("check job joins").expect("check job");
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
    };
    let result = db::task_writer::in_task_step(step.clone(), run).await;
    db.remove_run_ci_helper_steps(&ctx.task_id, &id)
        .await
        .map_err(|error| -> Box<dyn std::fmt::Debug> {
            match error {
                db::DbError::Sqlx(error) => Box::new(error),
                error => Box::new(error),
            }
        })
        .expect("helper steps are removed");
    result
}

pub struct AutoCascadeOnReviewPass;

#[async_trait]
impl HookAction for AutoCascadeOnReviewPass {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        let user_approval_required = gate_requires_user_approval(ctx);
        let task = match task(ctx).await {
            Ok(task) => task,
            Err(reason) => return HookResult::Failed { reason },
        };

        let reviewer_assigned =
            match get_role_assignment(ctx, crate::workflow::default_roles::REVIEWER).await {
                Ok(assignment) => {
                    assignment.is_some_and(|assignment| assignment.assignee_id.is_some())
                }
                Err(reason) => return HookResult::Failed { reason },
            };
        let latest_review = match latest_review(ctx).await {
            Ok(review) => review,
            Err(reason) => return HookResult::Failed { reason },
        };
        let rerun_entered_at =
            match db::budget::review_rerun_entry(ctx.db.pool(), &ctx.task_id, None).await {
                Ok(entry) => entry.map(|(_, entered_at)| entered_at),
                Err(error) => {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    }
                }
            };
        let review_id = latest_review.as_ref().map(|r| r.id.clone());
        let input = match crate::workflow::engine::durable::hook_effect(
            &ctx.task_id,
            "review_pass_input",
        )
        .await
        {
            Ok(Some(value)) => match serde_json::from_str::<Value>(&value) {
                Ok(value) => value,
                Err(error) => {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    }
                }
            },
            Ok(None) => {
                let value = json!({"review_id":review_id,"had_review_passed":task.review_passed_at.is_some()});
                if let Err(error) = crate::workflow::engine::durable::record_hook_effect(
                    &ctx.task_id,
                    "review_pass_input",
                    &value,
                )
                .await
                {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
                value
            }
            Err(error) => {
                return HookResult::Failed {
                    reason: error.to_string(),
                }
            }
        };
        let had_review_passed = if input["review_id"] == json!(review_id) {
            input["had_review_passed"].as_bool().unwrap_or(false)
        } else {
            task.review_passed_at.is_some()
        };
        match latest_review {
            Some(review)
                if review.status == ReviewStatus::Passed
                    && had_review_passed
                    && !user_approval_required
                    && (!reviewer_assigned || review_has_auditor_verdict(&review)) =>
            {
                let project = match ProjectRepo::get_by_id(&*ctx.db, &ctx.project_id).await {
                    Ok(Some(project)) => project,
                    Ok(None) => {
                        return HookResult::Failed {
                            reason: format!("project {} not found", ctx.project_id),
                        };
                    }
                    Err(error) => {
                        return HookResult::Failed {
                            reason: error.to_string(),
                        };
                    }
                };
                if project.paused_at.is_some() {
                    if let Err(error) =
                        crate::deferred_dispatch::defer_integration_for_pause(&ctx.db, &task).await
                    {
                        return HookResult::Failed {
                            reason: error.to_string(),
                        };
                    }
                    return HookResult::Skipped {
                        reason: "project paused; integration deferred".to_owned(),
                    };
                }
                HookResult::Cascade {
                    to: default_states::MERGING.to_string(),
                    reason: if review_is_ci_only(&review) {
                        "CI-only re-review passed".to_string()
                    } else {
                        "review passed".to_string()
                    },

                    bridge: if review_is_ci_only(&review) {
                        api_types::TransitionBridge::new(
                            api_types::TransitionBridgeKind::CiOnlyReviewPassed,
                        )
                    } else {
                        Default::default()
                    },
                }
            }
            Some(review)
                if review.status == ReviewStatus::Failed
                    && Some(review.execution_id.as_str()) == ctx.execution_id.as_deref()
                    // A review re-run reviews the same candidate again: the
                    // failure it was asked to replace is not its verdict.
                    && !rerun_entered_at
                        .as_deref()
                        .is_some_and(|entered_at| review.created_at.as_str() < entered_at) =>
            {
                if had_review_passed && !review_has_auditor_verdict(&review) {
                    if let Err(error) = TaskRepo::set_review_passed_at_cas(
                        &*ctx.db,
                        &ctx.task_id,
                        task.version,
                        None,
                        &now_rfc3339(),
                    )
                    .await
                    {
                        return HookResult::Failed {
                            reason: error.to_string(),
                        };
                    }
                    let reason = "merge-fix follow-up failed: ci";
                    if let Err(error) =
                        block_task(ctx, &task, reason, api_types::FailureKind::CiFailed, None).await
                    {
                        return HookResult::Failed {
                            reason: error.to_string(),
                        };
                    }
                    return HookResult::Ok;
                }
                // One disposition path, with the hook's merged configuration:
                // D5, progress receipts and lead-root replans cannot drift.
                match ctx
                    .task_service
                    .review_failure_target_with_config(
                        &task,
                        ctx.execution_id.as_deref(),
                        Some(&ctx.state_config),
                    )
                    .await
                {
                    Ok((_, Some(to), reason)) => HookResult::Cascade {
                        to,
                        reason,
                        bridge: Default::default(),
                    },
                    Ok((_, None, _)) => HookResult::Ok,
                    Err(error) => HookResult::Failed {
                        reason: error.to_string(),
                    },
                }
            }
            Some(_) | None => HookResult::Ok,
        }
    }
}

pub struct AutoCascadeOnUnconfiguredReview;

#[async_trait]
impl HookAction for AutoCascadeOnUnconfiguredReview {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        let Some(state) = ctx
            .workflow
            .states
            .iter()
            .find(|state| state.name == ctx.to_state)
        else {
            return HookResult::Failed {
                reason: WorkflowEngine::undefined_state_message(&ctx.to_state, &ctx.workflow),
            };
        };
        let Some(role_name) = effective_role(state) else {
            return HookResult::Skipped {
                reason: "review state has no role".to_string(),
            };
        };
        let assignment = match get_role_assignment(ctx, role_name).await {
            Ok(assignment) => assignment,
            Err(reason) => return HookResult::Failed { reason },
        };
        if assignment
            .as_ref()
            .is_some_and(|assignment| assignment.assignee_id.is_some())
        {
            return HookResult::Skipped {
                reason: format!("{role_name} role assigned"),
            };
        }

        let task = match task(ctx).await {
            Ok(task) => task,
            Err(reason) => return HookResult::Failed { reason },
        };
        let ci_steps = match task_execution_is_read_only(ctx, &task).await {
            Ok(true) => Vec::new(),
            Ok(false) => match review_ci_steps(&ctx.state_config) {
                Ok(ci_steps) => ci_steps,
                Err(reason) => return HookResult::Failed { reason },
            },
            Err(reason) => return HookResult::Failed { reason },
        };
        if !ci_steps.is_empty()
            || (!super::common::task_is_read_only(&ctx.db, &task)
                .await
                .unwrap_or(false)
                && review::verify::config(&ctx.state_config)
                    .ok()
                    .flatten()
                    .is_some())
        {
            return HookResult::Skipped {
                reason: "review checks configured".to_string(),
            };
        }

        if gate_requires_user_approval(ctx) || human_review_requested(ctx, false) {
            if let Err(reason) = super::common::ensure_review_awaiting_human(ctx).await {
                return HookResult::Failed { reason };
            }
            return HookResult::Ok;
        }

        let project = match ProjectRepo::get_by_id(&*ctx.db, &ctx.project_id).await {
            Ok(Some(project)) => project,
            Ok(None) => {
                return HookResult::Failed {
                    reason: format!("project {} not found", ctx.project_id),
                };
            }
            Err(error) => {
                return HookResult::Failed {
                    reason: error.to_string(),
                };
            }
        };
        if project.paused_at.is_some() {
            if let Err(error) =
                crate::deferred_dispatch::defer_integration_for_pause(&ctx.db, &task).await
            {
                return HookResult::Failed {
                    reason: error.to_string(),
                };
            }
            return HookResult::Skipped {
                reason: "project paused; integration deferred".to_owned(),
            };
        }

        // A review re-run with nothing to run still answers the request: it
        // records the attempt it made (no checks, passed), so the Task's
        // latest Review is this entry's and not the one it replaced.
        if let Err(reason) = record_empty_rerun_attempt(ctx, &task).await {
            return HookResult::Failed { reason };
        }

        HookResult::Cascade {
            to: default_states::MERGING.to_string(),
            reason: "review skipped: no checks or reviewer assigned".to_string(),

            bridge: Default::default(),
        }
    }
}

/// See [`AutoCascadeOnUnconfiguredReview`]. Idempotent per entry: a
/// redelivered hook finds the attempt this entry already recorded.
async fn record_empty_rerun_attempt(ctx: &HookContext, task: &db::Task) -> Result<(), String> {
    let Some((_, entered_at)) = db::budget::review_rerun_entry(ctx.db.pool(), &ctx.task_id, None)
        .await
        .map_err(|error| error.to_string())?
    else {
        return Ok(());
    };
    if latest_review(ctx)
        .await?
        .is_some_and(|review| review.created_at.as_str() >= entered_at.as_str())
    {
        return Ok(());
    }
    let Some(candidate) = latest_executor_execution(ctx).await else {
        return Ok(());
    };
    let review =
        create_review_attempt_with_authority(ctx, &candidate.id, task.version, &candidate.id)
            .await?;
    let now = now_rfc3339();
    ReviewRepo::update_status(
        &*ctx.db,
        &review.id,
        ReviewStatus::Passed,
        json!({ "ci_steps": [] }).to_string(),
        Some(now.clone()),
        &now,
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(())
}

fn gate_requires_user_approval(ctx: &HookContext) -> bool {
    ctx.gate_config
        .as_ref()
        .is_some_and(|gate_config| gate_config.requires_user_approval())
}

fn human_review_requested(ctx: &HookContext, reviewer_assigned: bool) -> bool {
    ctx.triggered_by.is_user()
        && crate::workflow::is_review_state(&ctx.workflow, &ctx.to_state)
        && !reviewer_assigned
}

/// One line naming what failed, for the transition log and Task history. The
/// full output stays on the review; this line only has to tell a reader which
/// command broke and how, without opening it.
fn ci_failure_summary(ci_results: &Value, failed_step_index: usize) -> String {
    const MAX_DETAIL_CHARS: usize = 240;
    let step = ci_results.get(failed_step_index);
    let command = step
        .and_then(|step| step.get("command"))
        .and_then(Value::as_str)
        .unwrap_or("unknown command");
    let exit_code = step
        .and_then(|step| step.get("exit_code"))
        .and_then(Value::as_i64)
        .map_or_else(|| "?".to_owned(), |code| code.to_string());
    let mut summary = format!("CI step {failed_step_index} failed (exit {exit_code}): `{command}`");
    let detail = step
        .and_then(|step| step.get("output_tail"))
        .and_then(Value::as_str)
        .and_then(|output| {
            output
                .lines()
                .rev()
                .map(str::trim)
                .find(|line| !line.is_empty())
        });
    if let Some(detail) = detail {
        let detail: String = detail.chars().take(MAX_DETAIL_CHARS).collect();
        summary.push_str(" -- ");
        summary.push_str(&detail);
    }
    summary
}

async fn interrupt_ci(
    ctx: &HookContext,
    task: &db::Task,
    reason: &str,
    retry: bool,
    reset: bool,
) -> HookResult {
    // Whoever caused the entry: the workflow's own cascade, or the owner's
    // Retry of the review entry. The hook runs in the Task's hooks step
    // either way, after the command that asked has returned, so the Task is
    // the only place the cause can be read. An entry its owner asked for
    // that was left unannotated showed only "hook failed", with no cause
    // and no re-entry.
    if let Err(error) = ctx
        .task_service
        .annotate_review_ci_interruption(task, ctx, reason, retry, reset)
        .await
    {
        return HookResult::Failed {
            reason: error.to_string(),
        };
    }
    HookResult::Failed {
        reason: reason.to_owned(),
    }
}

fn verify_failure_summary(details: &Value) -> Option<String> {
    if !matches!(
        details["verify"]["status"].as_str(),
        Some("fail" | "inconclusive")
    ) {
        return None;
    }
    Some(format!(
        "verify failed: {}",
        details["verify"]["checks"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|c| c["result"] != "pass")
            .map(|c| format!(
                "V-{} {}",
                c["name"].as_str().unwrap_or("unknown"),
                c["summary"].as_str().unwrap_or("failed")
            ))
            .collect::<Vec<_>>()
            .join("; ")
    ))
}

#[cfg(test)]
mod ci_failure_summary_tests {
    use super::*;

    #[test]
    fn names_the_command_exit_code_and_last_output_line() {
        let results = json!([
            {"command": "cargo fmt --check", "exit_code": 0, "output_tail": ""},
            {
                "command": "python3 -m unittest discover -s tests",
                "exit_code": 1,
                "output_tail": "Traceback...\nTypeError: unsupported operand\n\nFAILED (errors=3)\n",
            },
        ]);
        assert_eq!(
            ci_failure_summary(&results, 1),
            "CI step 1 failed (exit 1): `python3 -m unittest discover -s tests` -- FAILED (errors=3)"
        );
    }

    #[test]
    fn tolerates_missing_output() {
        let results = json!([{"command": "make check", "exit_code": 2}]);
        assert_eq!(
            ci_failure_summary(&results, 0),
            "CI step 0 failed (exit 2): `make check`"
        );
    }
}
