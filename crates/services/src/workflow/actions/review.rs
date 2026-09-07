use std::sync::Arc;

use async_trait::async_trait;
use db::{
    now_rfc3339, ReviewRepo, ReviewStatus, TaskRepo, TransitionLog, TransitionLogRepo,
    WorkspaceRepo,
};
use serde_json::json;

use crate::workflow::{
    default_states, effective_role, engine::WorkflowEngine, HookAction, HookContext, HookResult,
};

use super::common::{
    block_task, create_review_attempt, get_role_assignment, latest_executor_execution,
    latest_review, publish_domain_event, publish_review_failed, publish_review_passed,
    review_ci_steps, review_has_auditor_verdict, review_is_ci_only, run_ci_steps_in_worktree, task,
    task_execution_is_read_only, workspace_id,
};

pub struct RunCiSteps;

#[async_trait]
impl HookAction for RunCiSteps {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        let task = match task(ctx).await {
            Ok(task) => task,
            Err(reason) => return HookResult::Failed { reason },
        };
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
        if ci_steps.is_empty() {
            return HookResult::Skipped {
                reason: "no ci steps".to_string(),
            };
        }

        let Some(workspace_id) = workspace_id(ctx).await else {
            return HookResult::Skipped {
                reason: "no workspace".to_string(),
            };
        };
        let execution_id = match ctx.execution_id.clone() {
            Some(execution_id) => Some(execution_id),
            None => latest_executor_execution(ctx)
                .await
                .map(|execution| execution.id),
        };
        let Some(execution_id) = execution_id else {
            return HookResult::Skipped {
                reason: "no executor execution".to_string(),
            };
        };

        let workspace = match WorkspaceRepo::get_by_id(&*ctx.db, &workspace_id).await {
            Ok(Some(workspace)) => workspace,
            Ok(None) => {
                return HookResult::Skipped {
                    reason: "workspace not found".to_string(),
                };
            }
            Err(error) => {
                return HookResult::Failed {
                    reason: error.to_string(),
                };
            }
        };

        let review = match create_review_attempt(ctx, &execution_id).await {
            Ok(review) => review,
            Err(reason) => return HookResult::Failed { reason },
        };
        let had_review_passed = task.review_passed_at.is_some();
        let reviewer_assignment =
            match get_role_assignment(ctx, crate::workflow::default_roles::REVIEWER).await {
                Ok(assignment) => assignment,
                Err(reason) => return HookResult::Failed { reason },
            };
        let reviewer_assigned = reviewer_assignment
            .as_ref()
            .is_some_and(|assignment| assignment.assignee_id.is_some());

        let (ci_results, failed_step_index) =
            match run_ci_steps_in_worktree(&workspace.worktree_path, &ci_steps).await {
                Ok(result) => result,
                Err(reason) => return HookResult::Failed { reason },
            };
        let mut review_details = json!({ "ci_steps": ci_results });
        let now = now_rfc3339();

        let user_approval_required =
            gate_requires_user_approval(ctx) || human_review_requested(ctx, reviewer_assigned);

        let (status, finished_at) = if let Some(failed_step_index) = failed_step_index {
            let review = match ReviewRepo::update_status(
                &*ctx.db,
                &review.id,
                ReviewStatus::Failed,
                review_details.to_string(),
                Some(now.clone()),
                &now,
            )
            .await
            {
                Ok(review) => review,
                Err(error) => {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
            };
            publish_domain_event(
                ctx,
                &format!("review-status:{}:{}:{}", review.id, review.status, now),
            )
            .await;
            let memory_service = crate::MemoryService::new(Arc::clone(&ctx.db));
            if let Err(error) = memory_service
                .record_review_result_if_final(&ctx.project_id, &review)
                .await
            {
                tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
            }
            publish_review_failed(ctx, &review, failed_step_index);
            if had_review_passed {
                if let Err(error) =
                    TaskRepo::set_review_passed_at(&*ctx.db, &ctx.task_id, None, &now).await
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
                return HookResult::Failed {
                    reason: reason.to_string(),
                };
            }
            return HookResult::Failed {
                reason: format!("CI step {} failed", failed_step_index),
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

        let review = match ReviewRepo::update_status(
            &*ctx.db,
            &review.id,
            status.clone(),
            review_details.to_string(),
            finished_at.clone(),
            &now,
        )
        .await
        {
            Ok(review) => review,
            Err(error) => {
                return HookResult::Failed {
                    reason: error.to_string(),
                };
            }
        };
        publish_domain_event(
            ctx,
            &format!("review-status:{}:{}:{}", review.id, review.status, now),
        )
        .await;
        let memory_service = crate::MemoryService::new(Arc::clone(&ctx.db));
        if let Err(error) = memory_service
            .record_review_result_if_final(&ctx.project_id, &review)
            .await
        {
            tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
        }

        if status == ReviewStatus::Passed {
            if !had_review_passed {
                if let Err(error) =
                    TaskRepo::set_review_passed_at(&*ctx.db, &ctx.task_id, Some(now.clone()), &now)
                        .await
                {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
            }
            publish_review_passed(ctx, &review);
        }

        HookResult::Ok
    }
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
        match latest_review {
            Some(review)
                if review.status == ReviewStatus::Passed
                    && !user_approval_required
                    && (!reviewer_assigned || review_has_auditor_verdict(&review)) =>
            {
                HookResult::Cascade {
                    to: default_states::MERGING.to_string(),
                    reason: if review_is_ci_only(&review) {
                        "CI-only re-review passed".to_string()
                    } else {
                        "review passed".to_string()
                    },
                }
            }
            Some(review)
                if review.status == ReviewStatus::Failed
                    && Some(review.execution_id.as_str()) == ctx.execution_id.as_deref() =>
            {
                if task.review_passed_at.is_some() && !review_has_auditor_verdict(&review) {
                    if let Err(error) =
                        TaskRepo::set_review_passed_at(&*ctx.db, &ctx.task_id, None, &now_rfc3339())
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
                let budget = match crate::task_service::config::runtime_retry_budget(
                    &task,
                    crate::task_service::config::RetryBudgetKind::Review,
                    Some(&ctx.state_config),
                    ctx.gate_config.as_ref(),
                ) {
                    Ok(budget) => budget,
                    Err(error) => {
                        return HookResult::Failed {
                            reason: error.to_string(),
                        };
                    }
                };
                let existing_count =
                    match TransitionLogRepo::list_by_task(&*ctx.db, &ctx.task_id).await {
                        Ok(entries) => review_rejections_since_boundary(&entries),
                        Err(error) => {
                            return HookResult::Failed {
                                reason: error.to_string(),
                            };
                        }
                    };
                if existing_count + 1 >= i64::from(budget) {
                    let reason = "review retry budget exhausted";
                    if let Err(error) = block_task(
                        ctx,
                        &task,
                        reason,
                        api_types::FailureKind::ReviewGateFailed,
                        None,
                    )
                    .await
                    {
                        return HookResult::Failed {
                            reason: error.to_string(),
                        };
                    }
                    HookResult::Ok
                } else {
                    HookResult::Cascade {
                        to: default_states::IN_PROGRESS.to_string(),
                        reason: "review failed".to_string(),
                    }
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
        if !ci_steps.is_empty() {
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

        HookResult::Cascade {
            to: default_states::MERGING.to_string(),
            reason: "review skipped: no checks or reviewer assigned".to_string(),
        }
    }
}

fn gate_requires_user_approval(ctx: &HookContext) -> bool {
    ctx.gate_config
        .as_ref()
        .is_some_and(|gate_config| gate_config.requires_user_approval())
}

fn human_review_requested(ctx: &HookContext, reviewer_assigned: bool) -> bool {
    ctx.triggered_by.is_user() && ctx.to_state == default_states::REVIEW && !reviewer_assigned
}

fn review_rejections_since_boundary(entries: &[TransitionLog]) -> i64 {
    let boundary = entries.iter().rposition(|entry| {
        entry.from_state == default_states::REVIEW
            && !entry.rejection
            && (entry.to_state != default_states::REVIEW
                || entry.trigger_name.as_deref() == Some("reset_retry_window"))
    });
    let entries = boundary
        .and_then(|index| entries.get(index + 1..))
        .unwrap_or(entries);
    entries
        .iter()
        .filter(|entry| entry.from_state == default_states::REVIEW && entry.rejection)
        .count() as i64
}
