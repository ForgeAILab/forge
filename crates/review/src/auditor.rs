#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditorVerdict {
    Passed,
    Failed { reason: String },
}

pub fn render_auditor_prompt(
    task_title: &str,
    task_description: Option<&str>,
    diff_text: &str,
    override_template: Option<&str>,
) -> String {
    match override_template {
        Some(template) => template.to_owned(),
        None => {
            let description = task_description
                .filter(|value| !value.trim().is_empty())
                .unwrap_or("(no description)");
            format!(
                "Review this task implementation.\n\nTask title:\n{task_title}\n\nTask description:\n{description}\n\nGit diff:\n```diff\n{diff_text}\n```"
            )
        }
    }
}

pub fn parse_verdict(final_message: &str) -> AuditorVerdict {
    match serde_json::from_str::<api_types::ReviewAssessment>(
        crate::contract::extract_assessment_json(final_message),
    ) {
        Ok(report) if report.verdict == api_types::ConformanceVerdict::Pass => {
            AuditorVerdict::Passed
        }
        Ok(report) => AuditorVerdict::Failed {
            reason: report
                .findings
                .iter()
                .find(|f| f.blocking)
                .map(|f| format!("Expected {}; actual {}", f.expected, f.actual))
                .unwrap_or_else(|| "review conformance failed".into()),
        },
        Err(_) => AuditorVerdict::Failed {
            reason: "structured review assessment missing".into(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn assessment_json(verdict: &str) -> String {
        format!(
            r#"{{"contract_digest":"digest","verdict":"{verdict}","requirements":[],"findings":[]}}"#
        )
    }
    #[test]
    fn a_narrated_preamble_before_the_json_still_yields_the_verdict() {
        // GLM emits the required object after a sentence of wrap-up roughly two
        // runs in three; discarding it re-ran the whole review for nothing.
        let message = format!(
            "Evidence gathering is complete. Analysis summary before the verdict:\n\n{}",
            assessment_json("pass")
        );
        assert_eq!(parse_verdict(&message), AuditorVerdict::Passed);
    }
    #[test]
    fn a_fenced_assessment_still_yields_the_verdict() {
        let message = format!("```json\n{}\n```", assessment_json("pass"));
        assert_eq!(parse_verdict(&message), AuditorVerdict::Passed);
    }
    #[test]
    fn narration_does_not_turn_a_fail_into_a_pass() {
        let message = format!("Here is my assessment.\n{}", assessment_json("fail"));
        assert!(matches!(
            parse_verdict(&message),
            AuditorVerdict::Failed { .. }
        ));
    }
    #[test]
    fn old_markers_are_not_an_assessment() {
        assert!(matches!(
            parse_verdict("===REVIEW: PASS==="),
            AuditorVerdict::Failed { .. }
        ));
    }
    #[test]
    fn override_is_preserved_for_final_contract_assembly() {
        let prompt = render_auditor_prompt("ignored", None, "diff", Some("Use my rubric."));
        assert_eq!(prompt, "Use my rubric.");
    }
}
