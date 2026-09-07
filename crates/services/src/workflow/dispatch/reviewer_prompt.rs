use crate::workflow::{
    default_roles,
    dispatch::{
        default_tool_names, AgentDispatchContext, AgentPrompt, PromptBuilder,
        BUILDER_ID_REVIEWER_CONFORMANCE_V1, MANAGED_EXECUTION_CONTRACT,
    },
};

pub struct ReviewerPromptBuilder;

const REVIEWER_ROLE_BOUNDARY: &str = "\
Reviewer boundary:
- Must remain read-only, inspect diff and relevant logs, run or verify configured checks, and produce structured findings using the frozen contract Forge appends at launch.
- Must not edit files, stage changes, commit changes, provide vague fail reasons, or fail on style preferences without policy basis.
- Red flags: workspace mutations, missing evidence, blocking findings without expected vs actual behavior, contradictory assessments.";

const REVIEWER_FINDINGS_CONTRACT: &str = "\
Reviewer findings: Put findings in the JSON findings array. Each BLOCKING finding must include evidence (file/line when available, command output when relevant) plus expected vs actual behavior. Separate NON-BLOCKING findings from BLOCKING findings.";

impl PromptBuilder for ReviewerPromptBuilder {
    fn id(&self) -> &'static str {
        BUILDER_ID_REVIEWER_CONFORMANCE_V1
    }

    fn build(&self, ctx: &AgentDispatchContext) -> AgentPrompt {
        let review_config = ctx.state_config.get("review").unwrap_or(&ctx.state_config);
        let ci_steps = review_config
            .get("ci_steps")
            .and_then(|value| value.as_array())
            .map(|steps| {
                steps
                    .iter()
                    .filter_map(|step| step.as_str())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let review_prompt = review_config
            .get("review_prompt")
            .and_then(|value| value.as_str());

        let mut user = format!("Review task: {}\n", ctx.task.title);

        user.push_str(&format!("Task ID: {}\n", ctx.task.id));
        user.push_str(&format!("Status: {}\n", ctx.task.status));

        if let Some(description) = ctx.task.description.as_deref() {
            user.push_str("\nDescription:\n");
            user.push_str(description);
            user.push('\n');
        }

        if let Some(parent) = &ctx.parent_task {
            user.push_str(&format!(
                "\nParent task: {} ({})\n",
                parent.title, parent.id
            ));
            if let Some(parent_desc) = parent.description.as_deref() {
                user.push_str("Parent description:\n");
                user.push_str(parent_desc);
                user.push('\n');
            }
        }

        if !ctx.sub_tasks.is_empty() {
            user.push_str("\nSubtasks:\n");
            for sub in &ctx.sub_tasks {
                user.push_str(&format!("- [{}] {} ({})\n", sub.status, sub.title, sub.id));
                if let Some(desc) = sub.description.as_deref() {
                    for line in desc.lines() {
                        user.push_str("  ");
                        user.push_str(line);
                        user.push('\n');
                    }
                }
            }
        }

        if let Some(review_prompt) = review_prompt {
            user.push_str("\nReview prompt:\n");
            user.push_str(review_prompt);
            user.push('\n');
        }

        if !ctx.read_only_task && !ci_steps.is_empty() {
            user.push_str("\nRequired CI steps:\n");
            user.push_str(
                "Forge executes these checks independently against the reviewed content:\n",
            );
            for step in ci_steps {
                user.push_str("- ");
                user.push_str(step);
                user.push('\n');
            }
        }

        if !ctx.prior_reviews.is_empty() {
            user.push_str("\nPrior reviews:\n");
            for review in &ctx.prior_reviews {
                let status_str = match review.status {
                    db::ReviewStatus::Running => "Running",
                    db::ReviewStatus::AwaitingHuman => "Awaiting human",
                    db::ReviewStatus::Passed => "Passed",
                    db::ReviewStatus::Failed => "Failed",
                    db::ReviewStatus::Cancelled => "Cancelled",
                };
                user.push_str(&format!(
                    "- Attempt {}: {}\n",
                    review.attempt_number, status_str
                ));
            }
        }

        AgentPrompt {
            system: format!(
                "You are the reviewer agent for this Forge workflow task. This is a read-only audit. Verify correctness, run the configured checks, and report clear pass/fail feedback. If you fail the review, your feedback will be sent to the coder agent to address in a follow-up attempt.\n\n{MANAGED_EXECUTION_CONTRACT}\n\n{REVIEWER_ROLE_BOUNDARY}\n\n{REVIEWER_FINDINGS_CONTRACT}"
            ),
            user,
            tools: default_tool_names(default_roles::REVIEWER),
        }
    }
}
