use db::ReviewStatus;

use crate::workflow::{
    default_roles, default_states,
    dispatch::{
        default_tool_names, implementation_report_contract, required_checks_section,
        AgentDispatchContext, AgentPrompt, PromptBuilder, BUILDER_ID_CODER_IMPLEMENTATION_V2,
        BUILDER_ID_CODER_MERGE_FIX_V2, BUILDER_ID_CODER_REVIEW_FIX_V2, MANAGED_EXECUTION_CONTRACT,
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
- May finish uncommitted Task-worktree changes and reconcile conflict markers Forge committed after rebasing the branch; must never run Git to rebase, merge, or rewrite history — Forge owns the branch.
- Must complete delivery according to the execution harness contract, then run targeted verification.
- Must not rewrite the feature or add unrelated cleanup.
- Red flags: redesigns, formatting churn outside conflicted areas, unrelated fixes.";

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
    system.push_str(implementation_report_contract(ctx.delivery));
    if let Some(checks) = required_checks_section(ctx) {
        system.push_str("\n\n");
        system.push_str(&checks);
    }
    if has_plan {
        system.push_str("\n\nA planner agent already investigated and produced a plan — do not redo that work. Treat the provided plan as instructions to execute now.");
        match ctx.delivery {
            super::TaskDelivery::NativeTools => system.push_str(
                " Keep the checklist current with `task.plan` (`write`) and mark each completed item `- [x]` as you work.",
            ),
            super::TaskDelivery::Outbox => system.push_str(
                " Forge has copied the canonical checklist to `$FORGE_PLAN_PATH`; update that file and mark each completed item `- [x]` as you work. Forge publishes it only after this execution completes successfully.",
            ),
        }
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
    if let Some(notes) = crate::workflow::dispatch::handoff_notes_section(ctx) {
        user.push_str(&notes);
    }
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

    if let Some(findings) = returned_step_findings(ctx) {
        user.push_str(
            "\nThe lead checked this step and returned it. Your earlier work is in the worktree; change exactly what the findings ask and finish the step again:\n",
        );
        user.push_str(&findings);
        user.push('\n');
    }

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
    user
}

fn review_fix_user(ctx: &AgentDispatchContext) -> String {
    let body = super::repair_brief::RepairBrief::review(ctx).render();
    match no_change_brief(ctx) {
        Some(brief) => format!("{brief}\n\n{body}"),
        None => body,
    }
}

pub(crate) fn no_change_brief(ctx: &AgentDispatchContext) -> Option<&str> {
    ctx.transition_log
        .iter()
        .rev()
        .find(|entry| entry.to_state == ctx.state_name)
        .filter(|entry| entry.bridge.is_repair_no_change())
        .map(|entry| entry.trigger_reason.as_str())
}

fn merge_fix_user(ctx: &AgentDispatchContext) -> String {
    if let Some(brief) = super::repair_brief::RepairBrief::merge(ctx) {
        return brief.render();
    }
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
    ctx.task
        .condition
        .read()
        .diagnostic
        .map(|a| a.annotation_type)
}

/// The lead's findings when this entry into the implementation state is a
/// step returned by the step check (`lead_check`).
fn returned_step_findings(ctx: &AgentDispatchContext) -> Option<String> {
    ctx.transition_log
        .iter()
        .rev()
        .find(|entry| entry.to_state == ctx.state_name)
        .filter(|entry| {
            entry.rejection
                && entry.from_state == default_states::LEAD_CHECK
                && entry.bridge.bridge_kind == Some(api_types::TransitionBridgeKind::GateRejected)
        })
        .map(|entry| entry.trigger_reason.clone())
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
