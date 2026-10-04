use std::sync::Arc;

use async_trait::async_trait;
use db::{now_rfc3339, ProjectRepo, ReviewRepo, ReviewStatus, TaskRepo, TransitionLogRepo};
use serde_json::{json, Value};

use crate::workflow::{
    default_states, effective_role, engine::WorkflowEngine, HookAction, HookContext, HookResult,
};

use super::common::{
    block_task, cancel_review_after_authority_loss, create_review_attempt_with_authority,
    get_role_assignment, latest_executor_execution, latest_review, publish_review_failed,
    publish_review_passed, review_ci_steps, review_has_auditor_verdict, review_is_ci_only,
    run_ci_steps_in_worktree, task, task_execution_is_read_only,
    update_review_status_with_authority_checks, workspace_id,
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

        let workspace = match crate::task_service::workspace::prepare_workspace(
            &ctx.db,
            &ctx.workspace_root,
            &task,
            &task.id,
            ctx.repo_cache_locks.clone(),
            &ctx.workspace_backend_router,
        )
        .await
        {
            Ok(workspace) => workspace,
            Err(error) => {
                let reset = matches!(error, crate::ServiceError::WorkspaceResetRequired { .. });
                let retry = matches!(
                    error,
                    crate::ServiceError::DaemonUnavailable { .. }
                        | crate::ServiceError::DaemonTimeout { .. }
                );
                if reset || retry {
                    return interrupt_ci(ctx, &task, &error.to_string(), retry, reset).await;
                }
                return HookResult::Failed {
                    reason: error.to_string(),
                };
            }
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

        let environment = match ::review::contract::project_environment(&ctx.db, &ctx.task_id).await
        {
            Ok(environment) => environment,
            Err(reason) => {
                return HookResult::Failed { reason };
            }
        };
        let resolved = match resolve_workspace(ctx, &workspace).await {
            Ok(resolved) => resolved,
            Err(error) => {
                let reason = error.to_string();
                return HookResult::Failed { reason };
            }
        };
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
        let (ci_results, failed_step_index) = match run_ci_steps_in_worktree(
            &resolved,
            &ci_steps,
            &environment.env,
        )
        .await
        {
            Ok(result) => result,
            Err(failure) => {
                let retry = matches!(
                    &failure.error,
                    crate::workspace_backend::WorkspaceBackendError::OwnerUnreachable { .. }
                        | crate::workspace_backend::WorkspaceBackendError::RpcTimeoutBeforeStart { .. }
                );
                let reason = failure.error.to_string();
                if retry && failure.completed_steps == 0 {
                    // No command ran; this is not a Review attempt.
                    if let Err(error) =
                        sqlx::query("DELETE FROM review WHERE id = ? AND status = 'running'")
                            .bind(&review.id)
                            .execute(ctx.db.pool())
                            .await
                    {
                        return HookResult::Failed {
                            reason: error.to_string(),
                        };
                    }
                } else {
                    cancel_review_after_authority_loss(ctx, &review, &reason).await;
                }
                if retry || resolved.placement.owner_kind == db::PlacementOwnerKind::Daemon {
                    return interrupt_ci(ctx, &task, &reason, retry, false).await;
                }
                return HookResult::Failed { reason };
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
                if attempt.interrupted {
                    entry["rerun_after_interruption"] = json!(true);
                }
                if attempt.interrupted {
                    let log_ctx = crate::lifecycle::LifecycleHookContext {
                        event: api_types::LifecycleEvent::BeforeWork,
                        task_id: ctx.task_id.clone(),
                        task_title: task.title.clone(),
                        task_status: ctx.to_state.clone(),
                        previous_status: ctx.from_state.clone(),
                        project_id: ctx.project_id.clone(),
                        project_name: String::new(),
                        repo_path: String::new(),
                        worktree_path: None,
                        agent_id: ctx.agent_id.clone(),
                        execution_id: ctx.execution_id.clone(),
                        log_dir: Some(
                            std::env::temp_dir()
                                .join("forge")
                                .join("logs")
                                .join(&ctx.task_id)
                                .join("hooks"),
                        ),
                        env: Default::default(),
                    };
                    let mut log = entry.clone();
                    log["event"] = json!("review_ci");
                    log["hook_type"] = json!("script");
                    crate::lifecycle::LifecycleHookRunner::write_log_entry(
                        &log_ctx,
                        entry["index"].as_u64().unwrap_or(0) as usize,
                        &log,
                    );
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
                task.version,
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
                reason: ci_failure_summary(&review_details["ci_steps"], failed_step_index),
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
                task.version,
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
                task.version,
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
                }
            }
            Some(review)
                if review.status == ReviewStatus::Failed
                    && Some(review.execution_id.as_str()) == ctx.execution_id.as_deref() =>
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
                let existing_count = match TransitionLogRepo::list_by_task(&*ctx.db, &ctx.task_id)
                    .await
                {
                    Ok(entries) => crate::task_diagnostics::count_gate_rejections_since_boundary(
                        &entries,
                        default_states::REVIEW,
                    ),
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
    if !ctx.triggered_by.is_user() {
        if let Err(error) = ctx
            .task_service
            .annotate_review_ci_interruption(task, ctx, reason, retry, reset)
            .await
        {
            return HookResult::Failed {
                reason: error.to_string(),
            };
        }
    }
    HookResult::Failed {
        reason: reason.to_owned(),
    }
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
