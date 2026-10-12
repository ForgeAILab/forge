//! The prompt of a coordination root's lead: the planner run on a root Task
//! that has subtasks: at kickoff, and again when the root re-plans (its
//! aggregate review was rejected, or its current subtask parked needing a
//! decision). The doctrine is stable text in the system part; everything
//! that changes between runs (the root's description, the breakdown with its
//! states and the evidence of the re-plan trigger) is user-role input.
use crate::workflow::{
    default_roles,
    dispatch::{
        default_tool_names, AgentDispatchContext, AgentPrompt, PromptBuilder,
        BUILDER_ID_PLANNER_COORDINATION_V1, MANAGED_EXECUTION_CONTRACT,
    },
};

pub struct CoordinationPlannerPromptBuilder;

/// Bound on one embedded description, so a long root or subtask text cannot
/// crowd out the breakdown.
const DESCRIPTION_LIMIT: usize = 4_000;

const LEAD_DOCTRINE: &str = "\
Lead doctrine:
- You lead a feature that is broken into ordered subtasks. You do not implement it: workers run the subtasks one at a time, in order, on one shared workspace and branch owned by this root Task.
- No subtask starts while you are planning. They start, first to last, when this run ends successfully.
- Each subtask must build on the ones before it and leave the branch working for the next. The root's aggregate review judges the combined result after the last subtask.
- At kickoff, read what you need of the repository and check the breakdown: is the order right, does each step have a clear and verifiable outcome, does a step depend on something no earlier step provides, is something missing for the root's goal.
- Report what you find in your final answer: the order rationale, what each step must leave for the next, the risks, and any change to the breakdown you recommend. Forge keeps that answer with this run.
- The subtasks are the plan. Do not write a checklist plan for the root and do not mark anything done.
- When you run again later, your input names why: the aggregate review rejected the combined result, or a subtask is parked and needs a decision. Read the evidence given, look at the repository, and report the fix work or the decision: which step to add, change, retry or drop, in what order, and why. Re-plans are limited; do not report the same plan twice.
- Must not modify code or files in the workspace. Red flags: code edits, a root checklist, vague steps without a verification.";

fn bounded(text: &str) -> String {
    if text.len() <= DESCRIPTION_LIMIT {
        return text.to_owned();
    }
    let mut end = DESCRIPTION_LIMIT;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}\n[truncated]", &text[..end])
}

/// Bound on one piece of trigger evidence (review feedback, a check's
/// output, a subtask's failure message).
const EVIDENCE_LIMIT: usize = 6_000;

fn bounded_to(text: &str, limit: usize) -> String {
    if text.len() <= limit {
        return text.to_owned();
    }
    let mut end = limit;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}\n[truncated]", &text[..end])
}

/// The re-plan trigger section: why the root is planning again, with the
/// evidence. None at kickoff. The trigger is read from the entry into the
/// present planning state (its `replan` bridge), never guessed from states.
fn replan_trigger_section(ctx: &AgentDispatchContext) -> Option<String> {
    let entry = ctx
        .transition_log
        .iter()
        .filter(|entry| entry.to_state == ctx.state_name && entry.from_state != ctx.state_name)
        .max_by(|a, b| a.created_at.cmp(&b.created_at))?;
    if entry.bridge.bridge_kind != Some(api_types::TransitionBridgeKind::Replan) {
        return None;
    }
    let payload = entry.bridge.bridge_payload.as_ref()?;
    let subject = payload
        .get("subject_id")
        .and_then(serde_json::Value::as_str);
    match payload.get("trigger").and_then(serde_json::Value::as_str)? {
        "aggregate_review" => {
            let mut section = String::from(
                "\nRe-plan trigger: the aggregate review of the combined result was rejected.\n",
            );
            if let Some(feedback) = ctx.latest_review_feedback.as_deref() {
                section.push_str("Review findings:\n");
                section.push_str(&bounded_to(feedback, EVIDENCE_LIMIT));
                section.push('\n');
            }
            let failed = ctx
                .prior_reviews
                .iter()
                .filter(|review| review.status == db::ReviewStatus::Failed)
                .max_by_key(|review| review.attempt_number);
            let checks = failed
                .and_then(|review| {
                    serde_json::from_str::<serde_json::Value>(&review.step_results_json).ok()
                })
                .and_then(|details| {
                    details
                        .get("ci_steps")
                        .and_then(|steps| steps.as_array().cloned())
                })
                .unwrap_or_default();
            let mut listed = false;
            for check in checks.iter().filter(|check| {
                check
                    .get("exit_code")
                    .and_then(serde_json::Value::as_i64)
                    .is_some_and(|code| code != 0)
            }) {
                if !listed {
                    section.push_str("Failed review checks:\n");
                    listed = true;
                }
                section.push_str(&format!(
                    "- `{}` exited {}\n",
                    check
                        .get("command")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("check"),
                    check
                        .get("exit_code")
                        .and_then(serde_json::Value::as_i64)
                        .unwrap_or_default()
                ));
                for key in ["stderr_tail", "stdout_tail"] {
                    if let Some(tail) = check
                        .get(key)
                        .and_then(serde_json::Value::as_str)
                        .filter(|tail| !tail.trim().is_empty())
                    {
                        for line in bounded_to(tail, EVIDENCE_LIMIT / 3).lines() {
                            section.push_str("  ");
                            section.push_str(line);
                            section.push('\n');
                        }
                    }
                }
            }
            section.push_str(
                "Every subtask had ended when the review ran. Report the fix work the findings need: which step to add or reopen, and what it must leave. When this run ends the root goes on to its implementation state and, with no step left to run, back to aggregate review; the review's rejection limit bounds that loop.\n",
            );
            Some(section)
        }
        "subtask_parked" => {
            let position = ctx
                .sub_tasks
                .iter()
                .position(|task| Some(task.id.as_str()) == subject)?;
            let task = &ctx.sub_tasks[position];
            let read = task.condition.read();
            let mut section = format!(
                "\nRe-plan trigger: subtask {} \"{}\" [{}] is parked and needs a decision.\n",
                position + 1,
                task.title,
                task.status
            );
            if let Some(kind) = read.failure_kind {
                section.push_str(&format!("Failure kind: {kind}\n"));
            }
            let why = read
                .failure_message
                .or(read.blocked_message)
                .or(read.exception_message)
                .or_else(|| read.diagnostic.and_then(|diagnostic| diagnostic.message));
            if let Some(why) = why {
                section.push_str("Why it parked:\n");
                section.push_str(&bounded_to(&why, EVIDENCE_LIMIT));
                section.push('\n');
            }
            section.push_str(
                "No later subtask starts while it is parked. Report the decision: retry it as it is, change or split it, hand it to another worker, or drop it, and why. This park gets one re-plan: when this run ends the root returns to its implementation state, and if the subtask is still parked the Project Agent is asked to decide.\n",
            );
            Some(section)
        }
        _ => None,
    }
}

impl PromptBuilder for CoordinationPlannerPromptBuilder {
    fn id(&self) -> &'static str {
        BUILDER_ID_PLANNER_COORDINATION_V1
    }

    fn build(&self, ctx: &AgentDispatchContext) -> AgentPrompt {
        let mut user = format!("Lead this feature: {}\n", ctx.task.title);
        if let Some(description) = ctx.task.description.as_deref() {
            user.push_str("\nFeature description:\n");
            user.push_str(&bounded(description));
            user.push('\n');
        }
        user.push_str("\nBreakdown (ordered subtasks and their current states):\n");
        for (index, task) in ctx.sub_tasks.iter().enumerate() {
            user.push_str(&format!(
                "{}. [{}] {}\n",
                index + 1,
                task.status,
                task.title
            ));
            if let Some(description) = task.description.as_deref() {
                for line in bounded(description).lines() {
                    user.push_str("   ");
                    user.push_str(line);
                    user.push('\n');
                }
            }
        }
        // Say only what the states above show: a lead assigned late, or run
        // again, meets a breakdown that is already under way.
        let untouched = ctx
            .sub_tasks
            .iter()
            .all(|task| task.status == crate::workflow::default_states::TODO);
        if let Some(trigger) = replan_trigger_section(ctx) {
            user.push_str(&trigger);
        } else {
            user.push_str(if untouched {
                "\nThis is the kickoff: no subtask has started. Review the breakdown and report.\n"
            } else {
                "\nSome subtasks have already started or ended (see their states). Review what remains and report.\n"
            });
        }

        AgentPrompt {
            system: format!(
                "You are the lead of this Forge coordination root: the planner of a feature that is delivered through ordered subtasks.\n\n{MANAGED_EXECUTION_CONTRACT}\n\n{LEAD_DOCTRINE}"
            ),
            user,
            tools: default_tool_names(default_roles::PLANNER),
        }
    }
}
