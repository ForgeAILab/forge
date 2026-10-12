//! The prompt of a coordination root's lead: the planner run on a root Task
//! that has subtasks. Stage 1 covers kickoff. The doctrine is stable text in
//! the system part; everything that changes between runs (the root's
//! description and the breakdown with its states) is user-role input.
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
        user.push_str(if untouched {
            "\nThis is the kickoff: no subtask has started. Review the breakdown and report.\n"
        } else {
            "\nSome subtasks have already started or ended (see their states). Review what remains and report.\n"
        });

        AgentPrompt {
            system: format!(
                "You are the lead of this Forge coordination root: the planner of a feature that is delivered through ordered subtasks.\n\n{MANAGED_EXECUTION_CONTRACT}\n\n{LEAD_DOCTRINE}"
            ),
            user,
            tools: default_tool_names(default_roles::PLANNER),
        }
    }
}
