use db::ReviewStatus;

use crate::workflow::{
    default_roles, default_states,
    dispatch::{
        default_tool_names, AgentDispatchContext, AgentPrompt, PromptBuilder,
        BUILDER_ID_WORKER_AUTONOMOUS_V1, BUILDER_ID_WORKER_MERGE_FIX_V1,
        BUILDER_ID_WORKER_REVIEW_FIX_V1, MANAGED_EXECUTION_CONTRACT,
    },
};

pub struct WorkerAutonomousPromptBuilder;
pub struct WorkerReviewFixPromptBuilder;
pub struct WorkerMergeFixPromptBuilder;

impl PromptBuilder for WorkerAutonomousPromptBuilder {
    fn id(&self) -> &'static str {
        BUILDER_ID_WORKER_AUTONOMOUS_V1
    }

    fn build(&self, ctx: &AgentDispatchContext) -> AgentPrompt {
        AgentPrompt {
            system: worker_system(ctx, None),
            user: implementation_user(ctx),
            tools: default_tool_names(default_roles::WORKER),
        }
    }
}

impl PromptBuilder for WorkerReviewFixPromptBuilder {
    fn id(&self) -> &'static str {
        BUILDER_ID_WORKER_REVIEW_FIX_V1
    }

    fn build(&self, ctx: &AgentDispatchContext) -> AgentPrompt {
        AgentPrompt {
            system: worker_system(ctx, Some(REVIEW_FIX_ROLE_BOUNDARY)),
            user: review_fix_user(ctx),
            tools: default_tool_names(default_roles::WORKER),
        }
    }
}

impl PromptBuilder for WorkerMergeFixPromptBuilder {
    fn id(&self) -> &'static str {
        BUILDER_ID_WORKER_MERGE_FIX_V1
    }

    fn build(&self, ctx: &AgentDispatchContext) -> AgentPrompt {
        AgentPrompt {
            system: worker_system(ctx, Some(MERGE_FIX_ROLE_BOUNDARY)),
            user: merge_fix_user(ctx),
            tools: default_tool_names(default_roles::WORKER),
        }
    }
}

const WORKER_ROLE_BOUNDARY: &str = "\
Worker boundary:
- Own planning, implementation, self-validation, and routine repair for this task in the task worktree.
- Inspect the task contract, repository, existing plan, comments, and prior evidence before acting.
- Keep the plan updated as understanding changes, keep scope tight, and commit completed changes.
- Before finishing, run `git add -A && git commit` in the worktree with a message naming the task; the merge gate rejects uncommitted worktrees, so an uncommitted result is an incomplete task.
- When a safe decision cannot be inferred, stop and ask one structured question instead of guessing.
- Red flags: unrelated refactors, skipped verification, hidden failures, or completion claims without evidence.";

const REVIEW_FIX_ROLE_BOUNDARY: &str = "\
Review-fix boundary:
- Address every actionable review or validation finding precisely, preserving the task contract and implementation direction.
- Re-run relevant self-validation and report any unresolved uncertainty before resubmitting.
- Do not reopen solved work or add unrelated changes.";

const MERGE_FIX_ROLE_BOUNDARY: &str = "\
Merge-fix boundary:
- Resolve merge conflicts minimally while preserving the already-reviewed implementation intent.
- Re-run relevant validation after repair and report the exact conflict and verification evidence.
- Do not redesign the task or add unrelated cleanup.";

const WORKER_HANDOFF_CONTRACT: &str = "\
Completion evidence: End your response with a handoff block containing Summary | Deliverables | Verification | Uncertainty | Scope Changes | Next Step.
Include commands and meaningful results for verification, identify anything not run and why, and state whether the requested scope changed.";

const STRUCTURED_BLOCKED_QUESTION: &str = "\
STRUCTURED BLOCKED QUESTION format:
BLOCKED QUESTION
Context: <what is known>
Decision needed: <the unresolved choice>
Options considered: <short list>
Recommendation: <preferred option, or explain why none is safe>";

fn worker_system(ctx: &AgentDispatchContext, extra_role_boundary: Option<&str>) -> String {
    let mut system = "You are the autonomous worker agent for this Forge workflow task. Your job is to plan internally, implement the requested work, self-test it, repair failures you can safely resolve, and leave the task ready for review. There is no separate planner or routine reviewer role in this workflow.".to_owned();
    system.push_str("\n\n");
    system.push_str(MANAGED_EXECUTION_CONTRACT);
    system.push_str("\n\n");
    system.push_str(WORKER_ROLE_BOUNDARY);
    if let Some(extra_role_boundary) = extra_role_boundary {
        system.push_str("\n\n");
        system.push_str(extra_role_boundary);
    }
    system.push_str("\n\n");
    system.push_str(STRUCTURED_BLOCKED_QUESTION);
    system.push_str("\n\n");
    system.push_str(WORKER_HANDOFF_CONTRACT);

    if let Some(reason) = ctx.last_manual_bounce_reason.as_deref() {
        system.push_str("\n\nThis task was sent back with the following feedback: ");
        system.push_str(reason);
        system.push_str(". Address it in this attempt.");
    }
    if let Some(attempt) = last_failed_review_attempt(ctx) {
        system.push_str(&format!(
            "\n\nThis task has failed validation or review on attempt {attempt}. Use the supplied evidence to repair it before resubmitting."
        ));
    }
    system
}

fn implementation_user(ctx: &AgentDispatchContext) -> String {
    if let Some(ordered_prompt) =
        crate::task_service::build_first_turn_prompt_from_context(&ctx.task, &ctx.sub_tasks)
    {
        return ordered_prompt;
    }

    let mut user = format!(
        "Task: {}\n\nObjective:\nInspect the task contract and repository, make a concise internal plan, implement the requested change, run relevant self-validation, repair failures, and report completion evidence before submitting to review.\n",
        ctx.task.title
    );
    append_task_context(ctx, &mut user);
    user
}

fn review_fix_user(ctx: &AgentDispatchContext) -> String {
    let mut user = format!(
        "Task: {}\n\nThe delivery was sent back for changes. Inspect the current worktree and address all review and validation findings, then run the relevant checks and resubmit with updated evidence.\n",
        ctx.task.title
    );
    append_task_context(ctx, &mut user);

    if let Some(reason) = latest_ci_failure_reason(ctx) {
        user.push_str("\nLatest validation failure:\n");
        user.push_str(&reason);
        user.push('\n');
    }
    if let Some(feedback) = review_feedback(ctx) {
        user.push_str("\nReview feedback:\n");
        user.push_str(&feedback);
        user.push('\n');
    }
    append_continuation_context(ctx, &mut user);
    user
}

fn merge_fix_user(ctx: &AgentDispatchContext) -> String {
    let mut user = format!(
        "Task: {}\n\nMerge repair is required. Rebase onto the latest target branch, resolve conflicts with the smallest safe change, run validation, and leave the task ready to return through review.\n",
        ctx.task.title
    );
    if let Some(reason) = last_merge_failed_reason(ctx) {
        user.push_str("\nMerge failure evidence:\n");
        user.push_str(&reason);
        user.push('\n');
    }
    if ctx.task.review_passed_at.is_some() {
        user.push_str("\nReview already passed before the merge failure. Preserve that reviewed implementation and verify the repair; do not redesign the task.\n");
    }
    append_continuation_context(ctx, &mut user);
    user
}

fn append_task_context(ctx: &AgentDispatchContext, user: &mut String) {
    if let Some(description) = ctx.task.description.as_deref() {
        user.push_str("\nDescription:\n");
        user.push_str(description);
        user.push('\n');
    }
    if let Some(plan) = ctx.plan.as_deref().filter(|plan| !plan.trim().is_empty()) {
        user.push_str("\nExisting plan context (update it if needed):\n");
        user.push_str(plan);
        user.push('\n');
    }
    if !ctx.comments.is_empty() {
        user.push_str("\nRecent comments:\n");
        for comment in &ctx.comments {
            user.push_str("- ");
            user.push_str(&comment.author_name);
            user.push_str(": ");
            user.push_str(&comment.content);
            user.push('\n');
        }
    }
}

fn append_continuation_context(ctx: &AgentDispatchContext, user: &mut String) {
    if let Some(execution_id) = ctx.continuation_of_execution_id.as_deref() {
        user.push_str("\nPrevious worker execution:\n");
        user.push_str(execution_id);
        user.push('\n');
    }
    if let Some(logs_path) = ctx.continuation_logs_path.as_deref() {
        user.push_str("Previous worker log file:\n");
        user.push_str(logs_path);
        user.push('\n');
    }
    if let Some(execution_id) = ctx.latest_review_execution_id.as_deref() {
        user.push_str("Review execution:\n");
        user.push_str(execution_id);
        user.push('\n');
    }
    if let Some(logs_path) = ctx.latest_review_logs_path.as_deref() {
        user.push_str("Review log file:\n");
        user.push_str(logs_path);
        user.push('\n');
    }
}

fn latest_ci_failure_reason(ctx: &AgentDispatchContext) -> Option<String> {
    ctx.prior_reviews
        .iter()
        .filter(|review| review.status == ReviewStatus::Failed)
        .max_by_key(|review| review.attempt_number)
        .and_then(|review| {
            let value =
                serde_json::from_str::<serde_json::Value>(&review.step_results_json).ok()?;
            if value
                .get("auditor")
                .is_some_and(|auditor| !auditor.is_null())
            {
                return None;
            }
            let steps = value
                .get("ci_steps")
                .or_else(|| value.is_array().then_some(&value))?
                .as_array()?;
            let failed = steps.iter().find(|step| {
                step.get("exit_code")
                    .and_then(serde_json::Value::as_i64)
                    .is_some_and(|code| code != 0)
            })?;
            let command = failed
                .get("command")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("validation step");
            let output = failed
                .get("output_tail")
                .or_else(|| failed.get("stderr_tail"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            Some(format!("{command}\n{output}"))
        })
}

fn review_feedback(ctx: &AgentDispatchContext) -> Option<String> {
    ctx.latest_review_feedback
        .as_deref()
        .map(str::trim)
        .filter(|feedback| !feedback.is_empty())
        .map(str::to_owned)
        .or_else(|| {
            ctx.transition_log
                .iter()
                .rev()
                .find(|entry| {
                    entry.from_state == default_states::REVIEW
                        && entry.to_state == ctx.state_name
                        && entry.rejection
                })
                .map(|entry| entry.trigger_reason.clone())
        })
}

fn last_merge_failed_reason(ctx: &AgentDispatchContext) -> Option<String> {
    ctx.transition_log
        .iter()
        .rev()
        .find(|entry| entry.to_state == default_states::MERGE_FAILED)
        .map(|entry| entry.trigger_reason.clone())
}

fn last_failed_review_attempt(ctx: &AgentDispatchContext) -> Option<i64> {
    ctx.prior_reviews
        .iter()
        .filter(|review| review.status == ReviewStatus::Failed)
        .map(|review| review.attempt_number)
        .max()
}
