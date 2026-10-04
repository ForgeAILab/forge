//! A reviewer run fixes a missing result block before it completes.
//!
//! A review is free Markdown ending in one small result block. A model that
//! forgets the block, or names an unknown result, used to lose the whole
//! review and start a fresh full-cost reviewer run against the retry budget.
//! Instead the same run gets a short follow-up turn asking for the block
//! alone; its Markdown review from the first turn is kept.

use std::future::Future;

use forge_agent_host::{AgentHostError, AgentTurnOutput};

/// Follow-up turns one reviewer run may spend fixing its result block.
pub(super) const MAX_REPORT_CORRECTIONS: usize = 2;

/// The follow-up turn's input. The model still has its review in context,
/// so only the missing block is asked for.
pub(super) fn correction_prompt(problem: &str) -> String {
    format!(
        "Forge could not read the result of your review, so nothing has been recorded yet.\n\n\
         Problem: {problem}\n\n\
         Reply with ONLY the result block for the review you just wrote, for example:\n\
         {{\"result\": \"fail\", \"reason\": \"one sentence\", \"fixable_by\": \"coder\", \"repeat\": false}}\n\
         result is \"pass\", \"fail\", or \"blocked\". Do not repeat the investigation.\n\n{}",
        crate::workflow::dispatch::REVIEW_FINDING_ROUTING_CONTRACT
    )
}

/// Run correction turns until `problem_of` accepts the reply or the budget is
/// spent. Each correction is appended to the reply so the first turn's
/// Markdown review survives; the parser reads the last result block.
///
/// A correction turn that fails keeps the last report; the normal review
/// path then settles it exactly as it would have without a correction.
pub(super) async fn correct_report<C, R, Fut>(
    mut output: AgentTurnOutput,
    mut problem_of: C,
    mut rerun: R,
) -> AgentTurnOutput
where
    C: FnMut(&str) -> Option<String>,
    R: FnMut(String, String) -> Fut,
    Fut: Future<Output = Result<AgentTurnOutput, AgentHostError>>,
{
    for _ in 0..MAX_REPORT_CORRECTIONS {
        let Some(problem) = problem_of(&output.text) else {
            break;
        };
        match rerun(problem, output.text.clone()).await {
            Ok(next) => output = merge_turns(output, next),
            Err(AgentHostError::RuntimeWithUsage { usage_reports, .. }) => {
                output.usage_reports.extend(usage_reports);
                break;
            }
            Err(_) => break,
        }
    }
    output
}

/// Both turns' text and usage. Usage is recorded per provider attempt under
/// the one execution, so nothing is dropped.
fn merge_turns(previous: AgentTurnOutput, next: AgentTurnOutput) -> AgentTurnOutput {
    let text = format!("{}\n\n{}", previous.text.trim_end(), next.text.trim());
    let mut usage_reports = previous.usage_reports;
    usage_reports.extend(next.usage_reports);
    AgentTurnOutput {
        input_tokens: previous.input_tokens + next.input_tokens,
        output_tokens: previous.output_tokens + next.output_tokens,
        cache_read_tokens: previous.cache_read_tokens + next.cache_read_tokens,
        cache_write_tokens: previous.cache_write_tokens + next.cache_write_tokens,
        usage_reports,
        context_manifest: next.context_manifest.or(previous.context_manifest),
        text,
        ..next
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use forge_agent_host::{AgentTurnTelemetryState, AgentTurnUsageReport};

    use super::*;

    fn turn(text: &str, report_id: &str) -> AgentTurnOutput {
        AgentTurnOutput {
            runtime_session_id: "session".to_owned(),
            text: text.to_owned(),
            input_tokens: 10,
            output_tokens: 5,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            usage_reports: vec![AgentTurnUsageReport {
                report_id: report_id.to_owned(),
                request_id: None,
                attempt_id: None,
                provider_id: None,
                model_id: None,
                input_tokens: Some(10),
                output_tokens: Some(5),
                cache_read_tokens: None,
                cache_write_tokens: None,
                telemetry_state: AgentTurnTelemetryState::Metered,
                failed: false,
            }],
            telemetry_state: AgentTurnTelemetryState::Metered,
            context_manifest: None,
            pending_interaction_id: None,
        }
    }

    fn problem_unless_valid(text: &str) -> Option<String> {
        (!text.ends_with("valid") || text.ends_with("invalid"))
            .then(|| format!("cannot parse {text}"))
    }

    #[tokio::test]
    async fn a_malformed_report_is_corrected_in_the_same_run() {
        let prompts = RefCell::new(Vec::new());
        let output = correct_report(
            turn("garbled", "first"),
            problem_unless_valid,
            |problem, _| {
                prompts.borrow_mut().push(correction_prompt(&problem));
                async { Ok(turn("valid", "second")) }
            },
        )
        .await;

        assert_eq!(
            output.text, "garbled\n\nvalid",
            "the first turn's review is kept"
        );
        let ids: Vec<_> = output
            .usage_reports
            .iter()
            .map(|r| r.report_id.as_str())
            .collect();
        assert_eq!(ids, ["first", "second"], "both turns' usage is kept");
        assert_eq!(output.input_tokens, 20);
        let prompts = prompts.into_inner();
        assert_eq!(prompts.len(), 1);
        assert!(prompts[0].contains("Problem: cannot parse garbled"));
        assert!(prompts[0].contains("`fixable_by` is \"coder\" (default) or \"owner\""));
        assert!(prompts[0].contains("previous review attempt raised the same blocking finding"));
        assert!(prompts[0].contains("Owner example:") && prompts[0].contains("Repeat example:"));
    }

    #[tokio::test]
    async fn a_valid_report_runs_no_correction() {
        let output = correct_report(turn("valid", "only"), problem_unless_valid, |_, _| async {
            panic!("a valid report must not be corrected");
            #[allow(unreachable_code)]
            Ok(turn("", ""))
        })
        .await;
        assert_eq!(output.usage_reports.len(), 1);
    }

    #[tokio::test]
    async fn corrections_stop_at_the_budget() {
        let calls = RefCell::new(0);
        let output = correct_report(turn("bad", "0"), problem_unless_valid, |_, _| {
            *calls.borrow_mut() += 1;
            let n = *calls.borrow();
            async move { Ok(turn("still invalid", &n.to_string())) }
        })
        .await;
        assert_eq!(calls.into_inner(), MAX_REPORT_CORRECTIONS);
        assert!(output.text.ends_with("still invalid"));
        assert_eq!(output.usage_reports.len(), 1 + MAX_REPORT_CORRECTIONS);
    }

    #[tokio::test]
    async fn a_failed_correction_keeps_the_report_and_its_usage() {
        let output = correct_report(turn("bad", "first"), problem_unless_valid, |_, _| async {
            Err(AgentHostError::RuntimeWithUsage {
                failure: api_types::TurnFailure::Unclassified,
                provider_auth_rejected: false,
                message: "provider failed".to_owned(),
                usage_reports: turn("", "failed-attempt").usage_reports,
            })
        })
        .await;
        assert_eq!(output.text, "bad");
        let ids: Vec<_> = output
            .usage_reports
            .iter()
            .map(|r| r.report_id.as_str())
            .collect();
        assert_eq!(ids, ["first", "failed-attempt"]);
    }
}
