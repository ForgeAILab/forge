use db::ReviewStatus;

use crate::workflow::{
    default_roles, default_states,
    dispatch::{
        default_tool_names, required_checks_section, AgentDispatchContext, AgentPrompt,
        PromptBuilder, BUILDER_ID_WORKER_AUTONOMOUS_V1, BUILDER_ID_WORKER_MERGE_FIX_V1,
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
- The task worktree is the only checkout you may modify. Never enter, edit, or run commands against the source checkout or another worktree.
- Never merge, rebase, cherry-pick, push, or otherwise integrate this task into the target/default branch. Forge alone may integrate an accepted review.
- Inspect the task contract, repository, existing plan, comments, and prior evidence before acting.
- Keep the plan updated as understanding changes, keep scope tight, and leave all completed changes in the task worktree.
- Complete delivery according to the execution harness contract. Commit completed changes unless higher-priority runtime instructions explicitly say the host finalizes them.
- When a safe decision cannot be inferred, stop and ask one structured question instead of guessing.
- Red flags: unrelated refactors, skipped verification, hidden failures, or completion claims without evidence.";

const NATIVE_WORKER_PLAN_CONTRACT: &str = "\
Plan delivery: keep the complete Markdown checklist current with the typed `task.plan` operation.
Call `task.plan` with `{\"action\":\"write\",\"content\":\"<full Markdown checklist>\"}` whenever the plan or an item's status changes. `content` is the entire checklist, not a patch or one item; mark completed items `- [x]`.
This writes an execution-private candidate. Forge publishes it as the canonical Task plan only after this execution completes successfully.";

const OUTBOX_WORKER_PLAN_CONTRACT: &str = "\
Plan delivery: this CLI harness has no `task.plan` tool. Keep the complete Markdown checklist in the file named by `$FORGE_PLAN_PATH` inside the execution outbox.
Read that file when present, then create or replace it with the entire checklist whenever the plan or an item's status changes; do not write a patch or one item, and mark completed items `- [x]`.
Forge publishes this execution-private candidate as the canonical Task plan only after this execution completes successfully.";

const REVIEW_FIX_ROLE_BOUNDARY: &str = "\
Review-fix boundary:
- Address every actionable review or validation finding precisely, preserving the task contract and implementation direction.
- Re-run relevant self-validation and report any unresolved uncertainty before resubmitting.
- Do not reopen solved work or add unrelated changes.";

const MERGE_FIX_ROLE_BOUNDARY: &str = "\
Merge-fix boundary:
- May finish uncommitted Task-worktree changes and reconcile conflict markers Forge committed after rebasing the branch; must never run Git to rebase, merge, or rewrite history — Forge owns the branch.
- Complete delivery according to the execution harness contract, re-run relevant validation, and report the verification evidence.
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
    system.push_str("\n\n");
    system.push_str(match ctx.delivery {
        super::TaskDelivery::NativeTools => NATIVE_WORKER_PLAN_CONTRACT,
        super::TaskDelivery::Outbox => OUTBOX_WORKER_PLAN_CONTRACT,
    });
    if let Some(extra_role_boundary) = extra_role_boundary {
        system.push_str("\n\n");
        system.push_str(extra_role_boundary);
    }
    system.push_str("\n\n");
    system.push_str(STRUCTURED_BLOCKED_QUESTION);
    system.push_str("\n\n");
    system.push_str(WORKER_HANDOFF_CONTRACT);
    if let Some(checks) = required_checks_section(ctx) {
        system.push_str("\n\n");
        system.push_str(&checks);
    }

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
    let mut user = format!(
        "Task: {}\n\nObjective:\nInspect the task contract and repository, make a concise internal plan, implement the requested change, run relevant self-validation, repair failures, and report completion evidence before submitting to review.\n",
        ctx.task.title
    );
    if let Some(notes) = crate::workflow::dispatch::handoff_notes_section(ctx) {
        user.push_str(&notes);
    }
    append_task_context(ctx, &mut user);
    user
}

fn review_fix_user(ctx: &AgentDispatchContext) -> String {
    let mut body = super::repair_brief::RepairBrief::review(ctx).render();
    // The worker owns its plan and acts on the Task's comments; they sit
    // outside the brief's sections as they did before it.
    let mut comments = String::new();
    append_recent_comments(ctx, &mut comments);
    if !comments.is_empty() {
        body.push('\n');
        body.push_str(&comments);
    }
    match super::coder_prompt::no_change_brief(ctx) {
        Some(brief) => format!("{brief}\n\n{body}"),
        None => body,
    }
}

fn merge_fix_user(ctx: &AgentDispatchContext) -> String {
    if let Some(brief) = super::repair_brief::RepairBrief::merge(ctx) {
        return brief.render();
    }
    let mut user = format!("Task: {}\n\n", ctx.task.title);
    if matches!(
        merge_failure_kind(ctx),
        Some(api_types::FailureKind::DirtyWorktree)
    ) {
        user.push_str("Integration found unfinished or uncommitted changes in the Task worktree. Preserve the existing work, finish only the intended implementation, run validation, and complete delivery according to the execution harness contract. Do not rebase or discard changes.\n");
    } else {
        user.push_str("Integration found a real branch conflict. Do not rebase or attempt integration from this managed sandbox. Report that manual task-worktree repair is required and stop without changing unrelated files.\n");
    }
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

fn merge_failure_kind(ctx: &AgentDispatchContext) -> Option<api_types::FailureKind> {
    ctx.task
        .condition
        .read()
        .diagnostic
        .map(|a| a.annotation_type)
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
    append_recent_comments(ctx, user);
}

fn append_recent_comments(ctx: &AgentDispatchContext, user: &mut String) {
    if ctx
        .comments
        .iter()
        .any(|comment| !crate::workflow::dispatch::is_handoff_note(comment))
    {
        user.push_str("\nRecent comments:\n");
        for comment in ctx
            .comments
            .iter()
            .filter(|comment| !crate::workflow::dispatch::is_handoff_note(comment))
        {
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
