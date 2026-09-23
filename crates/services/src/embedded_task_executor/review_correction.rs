//! A reviewer run corrects its own malformed report before it completes.
//!
//! Forge only accepts a review it can verify: one JSON object in the frozen
//! contract's schema. A model that garbles a key or omits a field used to lose
//! the whole review, which started a fresh full-cost reviewer run against the
//! retry budget. Instead the same run gets one short follow-up turn carrying
//! the exact parse error and its own report, and is asked for the corrected
//! object only.

use std::future::Future;

use forge_agent_host::{AgentHostError, AgentTurnOutput};

/// Follow-up turns one reviewer run may spend fixing its report's shape.
pub(super) const MAX_REPORT_CORRECTIONS: usize = 2;

/// The rejected report is echoed back so the model does not have to redo its
/// investigation; a report past this size is cut, and the model re-emits it.
const MAX_ECHOED_REPORT_BYTES: usize = 64 * 1024;

/// The follow-up turn's input: what was wrong and the report to fix.
pub(super) fn correction_prompt(problem: &str, report: &str) -> String {
    let mut end = report.len().min(MAX_ECHOED_REPORT_BYTES);
    while !report.is_char_boundary(end) {
        end -= 1;
    }
    let cut = if end < report.len() {
        "\n[report truncated]"
    } else {
        ""
    };
    format!(
        "Forge could not accept your review report, so nothing has been verified yet.\n\n\
         Problem: {problem}\n\n\
         Reply with ONLY the corrected report: one JSON object in exactly the schema from \
         your instructions, with the same contract_digest, and no prose or code fences \
         around it. Keep your verdict, dispositions, and findings unless fixing the problem \
         requires changing them. Do not repeat the investigation.\n\n\
         Your previous report:\n{}{cut}",
        &report[..end]
    )
}

/// Run correction turns until `problem_of` accepts the report or the budget
/// is spent, returning the last report with every turn's usage attached.
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

/// The later turn's report, with both turns' usage. Usage is recorded per
/// provider attempt under the one execution, so nothing is dropped.
fn merge_turns(previous: AgentTurnOutput, next: AgentTurnOutput) -> AgentTurnOutput {
    let mut usage_reports = previous.usage_reports;
    usage_reports.extend(next.usage_reports);
    AgentTurnOutput {
        input_tokens: previous.input_tokens + next.input_tokens,
        output_tokens: previous.output_tokens + next.output_tokens,
        cache_read_tokens: previous.cache_read_tokens + next.cache_read_tokens,
        cache_write_tokens: previous.cache_write_tokens + next.cache_write_tokens,
        usage_reports,
        context_manifest: next.context_manifest.or(previous.context_manifest),
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
        (text != "valid").then(|| format!("cannot parse {text}"))
    }

    #[tokio::test]
    async fn a_malformed_report_is_corrected_in_the_same_run() {
        let prompts = RefCell::new(Vec::new());
        let output = correct_report(
            turn("garbled", "first"),
            problem_unless_valid,
            |problem, report| {
                prompts
                    .borrow_mut()
                    .push(correction_prompt(&problem, &report));
                async { Ok(turn("valid", "second")) }
            },
        )
        .await;

        assert_eq!(output.text, "valid");
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
        assert!(prompts[0].contains("Your previous report:\ngarbled"));
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
            async move { Ok(turn("still bad", &n.to_string())) }
        })
        .await;
        assert_eq!(calls.into_inner(), MAX_REPORT_CORRECTIONS);
        assert_eq!(output.text, "still bad");
        assert_eq!(output.usage_reports.len(), 1 + MAX_REPORT_CORRECTIONS);
    }

    #[tokio::test]
    async fn a_failed_correction_keeps_the_report_and_its_usage() {
        let output = correct_report(turn("bad", "first"), problem_unless_valid, |_, _| async {
            Err(AgentHostError::RuntimeWithUsage {
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

    #[test]
    fn an_oversized_report_is_echoed_truncated_on_a_char_boundary() {
        let report = "é".repeat(MAX_ECHOED_REPORT_BYTES);
        let prompt = correction_prompt("too big", &report);
        assert!(prompt.ends_with("[report truncated]"));
        assert!(prompt.len() < report.len());
    }
}
