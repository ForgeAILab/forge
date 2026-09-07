use crate::workflow::dispatch::{
    AgentDispatchContext, AgentPrompt, PromptBuilder, BUILDER_ID_READ_ONLY_TASK_V1,
    MANAGED_EXECUTION_CONTRACT,
};

pub struct ReadOnlyTaskPromptBuilder;

const READ_ONLY_ROLE_BOUNDARY: &str = "\
Read-only Task boundary:
- Investigate the Task question using repository and Project evidence available to this execution.
- Do not modify, create, delete, stage, or commit repository files.
- Report concrete findings, sources inspected, implications, uncertainty, and the recommended next action.
- Use a Task worklog entry for material progress or validation evidence when that operation is available.
- A clean unchanged worktree is successful delivery for this Task type.";

impl PromptBuilder for ReadOnlyTaskPromptBuilder {
    fn id(&self) -> &'static str {
        BUILDER_ID_READ_ONLY_TASK_V1
    }

    fn build(&self, ctx: &AgentDispatchContext) -> AgentPrompt {
        let mut user = format!(
            "Read-only Task: {}\n\nObjective:\nInvestigate the requested question and return an evidence-based result without changing the repository.\n",
            ctx.task.title
        );
        if let Some(description) = ctx.task.description.as_deref() {
            user.push_str("\nDescription:\n");
            user.push_str(description);
            user.push('\n');
        }
        if let Some(plan) = ctx.plan.as_deref().filter(|plan| !plan.trim().is_empty()) {
            user.push_str("\nExisting plan or notes:\n");
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
        user.push_str(
            "\nCompletion report:\nState the answer or plan, evidence inspected, commands or checks run, uncertainty, and the next implementation decision. Leave the worktree unchanged.\n",
        );

        AgentPrompt {
            system: format!(
                "You are the read-only investigator for this Forge workflow Task. Your job is to resolve a bounded discovery or planning question and report usable evidence.\n\n{MANAGED_EXECUTION_CONTRACT}\n\n{READ_ONLY_ROLE_BOUNDARY}"
            ),
            user,
            tools: vec!["read_files".to_owned()],
        }
    }
}
