use db::ReviewStatus;

use crate::workflow::{
    default_roles, default_states,
    dispatch::{
        default_tool_names, AgentDispatchContext, AgentPrompt, PromptBuilder,
        BUILDER_ID_CODER_IMPLEMENTATION_V2, BUILDER_ID_CODER_MERGE_FIX_V2,
        BUILDER_ID_CODER_REVIEW_FIX_V2, MANAGED_EXECUTION_CONTRACT,
    },
};

pub struct CoderImplementationPromptBuilder;
pub struct CoderReviewFixPromptBuilder;
pub struct CoderMergeFixPromptBuilder;

impl PromptBuilder for CoderImplementationPromptBuilder {
    fn id(&self) -> &'static str {
        BUILDER_ID_CODER_IMPLEMENTATION_V2
    }

    fn build(&self, ctx: &AgentDispatchContext) -> AgentPrompt {
        AgentPrompt {
            system: coder_system(ctx, None),
            user: implementation_user(ctx),
            tools: default_tool_names(default_roles::CODER),
        }
    }
}

impl PromptBuilder for CoderReviewFixPromptBuilder {
    fn id(&self) -> &'static str {
        BUILDER_ID_CODER_REVIEW_FIX_V2
    }

    fn build(&self, ctx: &AgentDispatchContext) -> AgentPrompt {
        AgentPrompt {
            system: coder_system(ctx, Some(REVIEW_FIX_ROLE_BOUNDARY)),
            user: review_fix_user(ctx),
            tools: default_tool_names(default_roles::CODER),
        }
    }
}

impl PromptBuilder for CoderMergeFixPromptBuilder {
    fn id(&self) -> &'static str {
        BUILDER_ID_CODER_MERGE_FIX_V2
    }

    fn build(&self, ctx: &AgentDispatchContext) -> AgentPrompt {
        AgentPrompt {
            system: coder_system(ctx, Some(MERGE_FIX_ROLE_BOUNDARY)),
            user: merge_fix_user(ctx),
            tools: default_tool_names(default_roles::CODER),
        }
    }
}

const CODER_ROLE_BOUNDARY: &str = "\
Coder boundary:
- Must implement only the requested task in the task worktree.
- The task worktree is the only checkout you may modify. Never enter, edit, or run commands against the source checkout or another worktree.
- Never merge, rebase, cherry-pick, push, or otherwise integrate this task into the target/default branch. Forge alone may integrate an accepted review.
- Must inspect supplied plans, comments, and review feedback first, keep scope tight, run relevant verification, and complete delivery according to the execution harness contract.
- Commit completed changes unless higher-priority runtime instructions explicitly say the host finalizes them.
- Must not change unrelated behavior, ignore failed verification, treat review feedback as optional, or claim success without running verification.
- Red flags: broad refactors, skipped checks, missing proof media for UI/runtime changes.";

const REVIEW_FIX_ROLE_BOUNDARY: &str = "\
Review-fix boundary:
- Must address prior review or CI feedback precisely while preserving the implementation direction.
- Must not reopen solved work or add unrelated changes.
- Red flags: ignored reviewer evidence, broad rewrites, fixes without verification.";

const MERGE_FIX_ROLE_BOUNDARY: &str = "\
Merge-fix boundary:
- May finish uncommitted Task-worktree changes, but must not rebase or attempt a real branch conflict repair. Forge parks real merge conflicts for manual workspace repair.
- Must complete delivery according to the execution harness contract, then run targeted verification.
- Must not rewrite the feature or add unrelated cleanup.
- Red flags: redesigns, formatting churn outside conflicted areas, unrelated fixes.";

const CODER_WORKLOG_CONTRACT: &str = "\
Worklog: append entries with `task.worklog` (append) as you work, so the reviewer reads what you actually did.
Write one after a meaningful milestone -- your initial approach, a completed slice, validation results, a deviation, a blocker -- and not for every tool call or file edit.
Use kind `progress`, `decision`, `validation`, or `blocker`. Forge derives the Task, execution, role, and identity from your session, and posts the final completion summary itself once the execution is accepted, so no separate handoff block is needed.
Proof of behaviour is a captured artifact, not prose: when a change alters UI or runtime behaviour, capture it with `task.evidence` (capture) -- a screenshot or recording by `path`, or verbatim command output as `content` -- and say in the worklog what you captured. If you could not capture proof, say so and why.
A worklog entry never moves the Task and never satisfies an acceptance check.";

fn coder_system(ctx: &AgentDispatchContext, extra_role_boundary: Option<&str>) -> String {
    let has_plan = ctx
        .plan
        .as_deref()
        .is_some_and(|plan| !plan.trim().is_empty());
    let mut system = "You are the coder agent for this Forge workflow task. Your job is to implement code changes in the worktree. Once you finish, the task moves to the reviewer agent for verification. Keep the scope tight, verify the result compiles and passes locally, and complete delivery according to the execution harness contract.".to_string();
    system.push_str("\n\n");
    system.push_str(MANAGED_EXECUTION_CONTRACT);
    system.push_str("\n\n");
    system.push_str(CODER_ROLE_BOUNDARY);
    if let Some(extra_role_boundary) = extra_role_boundary {
        system.push_str("\n\n");
        system.push_str(extra_role_boundary);
    }
    system.push_str("\n\n");
    system.push_str(CODER_WORKLOG_CONTRACT);
    system.push_str("\n\nProof of work for app-touching changes: If your task modifies user-facing UI or runtime behavior, capture a screenshot (or short walkthrough video) demonstrating the change. Upload it with forge-ctl task media upload --task-id <id> --file <path> and post a comment with forge-ctl task media comment --task-id <id> --content validation-notes --media-url <url> before transitioning to review.");
    if has_plan {
        system.push_str(" A planner agent already investigated and produced a plan — do not redo that work. Treat the provided plan as instructions to execute now.");
    }
    if let Some(reason) = ctx.last_manual_bounce_reason.as_deref() {
        system.push_str("\n\nThis task was sent back with the following feedback: ");
        system.push_str(reason);
        system.push_str(". Address it in this attempt.");
    }
    if let Some(attempt) = last_failed_review_attempt(ctx) {
        system.push_str(&format!(
            "\n\nThis task has failed review {attempt} time(s). Focus on addressing the review feedback precisely."
        ));
    }
    system
}

fn implementation_user(ctx: &AgentDispatchContext) -> String {
    let mut user = format!(
        "Task: {}\n\nImplementation objective:\nMake the requested code changes in the worktree and leave the task ready for review.\n",
        ctx.task.title
    );
    if let Some(description) = ctx.task.description.as_deref() {
        user.push_str("\nDescription:\n");
        user.push_str(description);
        user.push('\n');
    }

    if let Some(reason) = last_merge_failed_reason(ctx) {
        user.push_str("\nMerge failed on the prior attempt:\n");
        user.push_str(&reason);
        user.push_str(
            "\nInspect only the current task-worktree files and do not rebase or integrate branches. If a real branch conflict remains, report the manual repair blocker; Forge handles integration outside the managed sandbox.\n",
        );
    }

    if let Some(plan) = ctx.plan.as_deref().filter(|plan| !plan.trim().is_empty()) {
        user.push_str("\nPlan:\n");
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
    user
}

fn review_fix_user(ctx: &AgentDispatchContext) -> String {
    if let Some(reason) = latest_ci_failure_reason(ctx) {
        let mut user = format!("Task: {}\n", ctx.task.title);
        user.push_str(
            "\nCI failed during review. Fix only the failing check below, keep the existing implementation direction, and complete the minimal correction according to the execution harness contract.\n",
        );
        user.push_str("\nCI failure:\n");
        user.push_str(&reason);
        user.push('\n');
        if let Some(execution_id) = ctx.continuation_of_execution_id.as_deref() {
            user.push_str("\nPrevious coder execution:\n");
            user.push_str(execution_id);
            user.push('\n');
        }
        if let Some(logs_path) = ctx.continuation_logs_path.as_deref() {
            user.push_str("\nPrevious coder log file:\n");
            user.push_str(logs_path);
            user.push('\n');
        }
        return user;
    }

    let mut user = format!("Task: {}\n", ctx.task.title);
    if let Some(description) = ctx.task.description.as_deref() {
        user.push_str("\nDescription:\n");
        user.push_str(description);
        user.push('\n');
    }
    if let Some(plan) = ctx.plan.as_deref().filter(|plan| !plan.trim().is_empty()) {
        user.push_str("\nOriginal plan:\n");
        user.push_str(plan);
        user.push('\n');
    }
    user.push_str(
        "\nThe reviewer agent flagged the previous implementation. Inspect the current worktree diff, address only the review findings, and complete delivery according to the execution harness contract. The task will return to the reviewer agent for re-verification.\n",
    );
    if let Some(reason) = review_feedback(ctx) {
        user.push_str("\nReview feedback:\n");
        user.push_str(&reason);
        user.push('\n');
    }
    if let Some(execution_id) = ctx.latest_review_execution_id.as_deref() {
        user.push_str("\nReviewer execution:\n");
        user.push_str(execution_id);
        user.push('\n');
    }
    if let Some(logs_path) = ctx.latest_review_logs_path.as_deref() {
        user.push_str("\nReviewer log file:\n");
        user.push_str(logs_path);
        user.push('\n');
    }
    if let Some(execution_id) = ctx.continuation_of_execution_id.as_deref() {
        user.push_str("\nPrevious coder execution:\n");
        user.push_str(execution_id);
        user.push('\n');
    }
    if let Some(logs_path) = ctx.continuation_logs_path.as_deref() {
        user.push_str("\nPrevious coder log file:\n");
        user.push_str(logs_path);
        user.push('\n');
    }
    if let Some(attempt) = last_failed_review_attempt(ctx) {
        user.push_str(&format!(
            "\nReview attempt {attempt} failed. Address the review feedback, keep the change scoped, and resubmit.\n"
        ));
    }
    user
}

fn latest_ci_failure_reason(ctx: &AgentDispatchContext) -> Option<String> {
    ctx.prior_reviews
        .iter()
        .filter(|review| review.status == ReviewStatus::Failed)
        .max_by_key(|review| review.attempt_number)
        .and_then(|review| {
            let value =
                serde_json::from_str::<serde_json::Value>(&review.step_results_json).ok()?;
            let has_auditor = value
                .get("auditor")
                .is_some_and(|auditor| !auditor.is_null());
            if has_auditor {
                return None;
            }
            ci_failure_reason(&value)
        })
}

fn merge_fix_user(ctx: &AgentDispatchContext) -> String {
    let mut user = String::new();
    if matches!(
        merge_failure_kind(ctx),
        Some(api_types::FailureKind::DirtyWorktree)
    ) {
        if ctx.task.review_passed_at.is_some() {
            user.push_str("The task previously passed review, but integration found uncommitted changes in the task worktree, not a merge conflict. Preserve the approved intent and the existing work; do not rebase or discard it. The repaired commit will receive a fresh review before Forge retries integration.\n\n");
        } else {
            user.push_str("Integration found uncommitted changes in the task worktree, not a merge conflict. Preserve the existing work; do not rebase or discard it.\n\n");
        }
        user.push_str("Inspect the worktree changes, finish the intended implementation, complete delivery according to the execution harness contract, and verify the focused checks pass.");
    } else {
        if ctx.task.review_passed_at.is_some() {
            user.push_str("The task previously passed review, but integration found a real merge conflict. That conflict requires manual workspace repair and a fresh review before Forge retries integration.\n\n");
        }
        user.push_str("Do not rebase or attempt integration from this managed sandbox. Report that manual task-worktree repair is required and stop without changing unrelated files.");
    }
    user
}

fn merge_failure_kind(ctx: &AgentDispatchContext) -> Option<api_types::FailureKind> {
    let annotation = ctx.task.error_annotation.as_deref()?;
    let value = serde_json::from_str::<serde_json::Value>(annotation).ok()?;
    serde_json::from_value(value.get("type")?.clone()).ok()
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

fn last_review_rejection_reason(ctx: &AgentDispatchContext) -> Option<String> {
    let review_reason = ctx
        .prior_reviews
        .iter()
        .filter(|review| review.status == ReviewStatus::Failed)
        .max_by_key(|review| review.attempt_number)
        .and_then(|review| review_failure_reason(&review.step_results_json));
    if review_reason.is_some() {
        return review_reason;
    }

    ctx.transition_log
        .iter()
        .rev()
        .find(|entry| {
            entry.from_state == default_states::REVIEW
                && entry.to_state == ctx.state_name
                && entry.rejection
        })
        .map(|entry| entry.trigger_reason.clone())
}

fn review_feedback(ctx: &AgentDispatchContext) -> Option<String> {
    ctx.latest_review_feedback
        .as_deref()
        .map(str::trim)
        .filter(|feedback| !feedback.is_empty())
        .map(str::to_owned)
        .or_else(|| last_review_rejection_reason(ctx))
}

fn review_failure_reason(step_results_json: &str) -> Option<String> {
    let value = serde_json::from_str::<serde_json::Value>(step_results_json).ok()?;
    value
        .get("auditor")
        .and_then(|auditor| auditor.get("reason"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .or_else(|| ci_failure_reason(&value))
}

fn ci_failure_reason(value: &serde_json::Value) -> Option<String> {
    let steps = value
        .get("ci_steps")
        .or_else(|| if value.is_array() { Some(value) } else { None })?
        .as_array()?;
    let failed = steps.iter().find(|step| {
        step.get("exit_code")
            .and_then(serde_json::Value::as_i64)
            .is_some_and(|code| code != 0)
    })?;
    let command = failed
        .get("command")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("CI step");
    let output = failed
        .get("output_tail")
        .and_then(serde_json::Value::as_str)
        .filter(|output| !output.trim().is_empty())
        .or_else(|| {
            failed
                .get("stderr_tail")
                .and_then(serde_json::Value::as_str)
        })
        .unwrap_or("");
    Some(format!("CI failed: {command}\n{output}"))
}
