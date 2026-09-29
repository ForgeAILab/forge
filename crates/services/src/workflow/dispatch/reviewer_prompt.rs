use crate::workflow::{
    default_roles,
    dispatch::{
        default_tool_names, read_only_report_contract, AgentDispatchContext, AgentPrompt,
        PromptBuilder, BUILDER_ID_REVIEWER_CONFORMANCE_V1,
    },
};

pub struct ReviewerPromptBuilder;

const REVIEWER_ROLE_BOUNDARY: &str = "\
Reviewer boundary:
- Must remain read-only toward the repository: do not edit, stage, or commit tracked files.
- May install dependencies, build, start the app or its services, and write scratch scripts, logs, or screenshots outside tracked files; Forge discards the worktree afterwards and fails the run if any tracked file changed.
- Start from the context Forge supplies -- the candidate diff, the coder's worklog and evidence, check results, and the previous review -- before exploring the repository yourself.
- Must not provide vague fail reasons, fail on style preferences, or fail on problems that already existed at base_sha.
- Never hide a failed verification. If the environment, not the code, stops you, say what is missing and use the \"blocked\" result.
- Report in the format of the frozen contract Forge appends at launch.
- Red flags: tracked-file mutations, a verdict backed only by reading code, blocking findings without expected vs actual behavior, a result that contradicts the review.";

const REVIEWER_FINDINGS_CONTRACT: &str = "\
Reviewer findings: Write findings in your Markdown review. Each BLOCKING finding must be reproducible: the command you ran or the steps you took, what you expected, and what actually happened (with output or a screenshot), plus file/line when it helps the coder. Separate NON-BLOCKING findings from BLOCKING findings.";

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
            match current_ci_results(ctx) {
                Some(results) => {
                    user.push_str(
                        "Forge already ran these checks against the reviewed content before dispatching you. Results (output truncated to the tail):\n",
                    );
                    user.push_str(&results);
                }
                None => {
                    user.push_str(
                        "Forge executes these checks independently against the reviewed content:\n",
                    );
                    for step in ci_steps {
                        user.push_str("- ");
                        user.push_str(step);
                        user.push('\n');
                    }
                }
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
                "You are the reviewer agent for this Forge workflow task. Decide whether the change works by exercising it: build it, run its tests, and drive the changed behavior programmatically or visually the way a user or caller would. Reading code tells you what to run and explains a failure you observed; it does not prove correctness on its own. If you fail the review, your feedback goes to the coder agent to fix in a follow-up attempt.\n\n{REVIEWER_ROLE_BOUNDARY}\n\n{REVIEWER_FINDINGS_CONTRACT}\n\n{}",
                read_only_report_contract(ctx.delivery)
            ),
            user,
            tools: default_tool_names(default_roles::REVIEWER),
        }
    }
}

/// Bytes of each CI step's output tail shown to the reviewer.
const CI_OUTPUT_TAIL_BYTES: usize = 1500;

/// Render the CI results Forge recorded on the in-flight Review attempt, which
/// runs before the reviewer is dispatched.
fn current_ci_results(ctx: &AgentDispatchContext) -> Option<String> {
    let review = ctx
        .prior_reviews
        .iter()
        .filter(|review| review.status == db::ReviewStatus::Running)
        .max_by_key(|review| review.attempt_number)?;
    let details: serde_json::Value = serde_json::from_str(&review.step_results_json).ok()?;
    let steps = details.get("ci_steps")?.as_array()?;
    if steps.is_empty() {
        return None;
    }
    let mut rendered = String::new();
    for step in steps {
        let command = step.get("command").and_then(|v| v.as_str()).unwrap_or("?");
        let exit_code = step
            .get("exit_code")
            .and_then(serde_json::Value::as_i64)
            .map_or_else(|| "unknown".to_owned(), |code| code.to_string());
        rendered.push_str(&format!("- `{command}` exited {exit_code}\n"));
        let output = step
            .get("output_tail")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .trim_end();
        if !output.is_empty() {
            let start = output.len().saturating_sub(CI_OUTPUT_TAIL_BYTES);
            let start = (start..output.len())
                .find(|&index| output.is_char_boundary(index))
                .unwrap_or(output.len());
            rendered.push_str("```\n");
            rendered.push_str(&output[start..]);
            rendered.push_str("\n```\n");
        }
    }
    Some(rendered)
}
