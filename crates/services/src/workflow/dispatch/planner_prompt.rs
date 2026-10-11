use crate::plan_artifact::{
    OUTBOX_PLAN_ARTIFACT_AGENT_INSTRUCTION, PLAN_ARTIFACT_AGENT_INSTRUCTION,
};
use crate::workflow::{
    default_roles,
    dispatch::{
        default_tool_names, AgentDispatchContext, AgentPrompt, PromptBuilder, TaskDelivery,
        BUILDER_ID_PLANNER_DEFAULT_V2, MANAGED_EXECUTION_CONTRACT,
    },
};

pub struct PlannerPromptBuilder;

const PLANNER_ROLE_BOUNDARY: &str = forge_agent_host::merge_friendly_guidance!("\
Planner boundary:
- Must investigate enough to produce an executable plan with risks, tests, and acceptance criteria.
- Must not modify code or mark implementation items done; leave implementation work unchecked unless already complete.
- Red flags: code edits, completed implementation checkboxes, plans without verification steps.
- For sub-task planning: ", "");

impl PromptBuilder for PlannerPromptBuilder {
    fn id(&self) -> &'static str {
        BUILDER_ID_PLANNER_DEFAULT_V2
    }

    fn build(&self, ctx: &AgentDispatchContext) -> AgentPrompt {
        let mut user = format!("Plan task: {}\n", ctx.task.title);

        if let Some(description) = ctx.task.description.as_deref() {
            user.push_str("\nTask description:\n");
            user.push_str(description);
            user.push('\n');
        }

        if let Some(parent) = &ctx.parent_task {
            user.push_str("\nParent task:\n");
            user.push_str(&parent.title);
            if let Some(description) = parent.description.as_deref() {
                user.push('\n');
                user.push_str(description);
            }
            user.push('\n');
        }

        if !ctx.sub_tasks.is_empty() {
            user.push_str("\nExisting sub-tasks:\n");
            for task in &ctx.sub_tasks {
                user.push_str("- ");
                user.push_str(&task.title);
                user.push('\n');
            }
        }

        user.push_str("\nPlanning output:\n");
        user.push_str(match ctx.delivery {
            TaskDelivery::NativeTools => PLAN_ARTIFACT_AGENT_INSTRUCTION,
            TaskDelivery::Outbox => OUTBOX_PLAN_ARTIFACT_AGENT_INSTRUCTION,
        });
        user.push('\n');

        AgentPrompt {
            system: format!(
                "You are the planner for this Forge workflow task. Your job is to investigate the codebase, design an approach, and produce a concrete implementation plan with clear verification steps. You do not implement the code; the coder agent will receive your plan and execute it.\n\n{MANAGED_EXECUTION_CONTRACT}\n\n{PLANNER_ROLE_BOUNDARY}"
            ),
            user,
            tools: default_tool_names(default_roles::PLANNER),
        }
    }
}
