use crate::runner::StepResult;
use std::path::PathBuf;

pub fn render_review_fail_prompt(reason: &str, diff: &str) -> String {
    format!(
        "The reviewer flagged this implementation as needing changes. Reason: {reason}. Current diff:\n\n{diff}\n\nPlease address the issues above and commit the fix."
    )
}

pub fn render_ci_fail_prompt(failing_steps: &[StepResult]) -> String {
    let formatted_list = failing_steps
        .iter()
        .map(|step| {
            let output = if step.output_tail.trim().is_empty() {
                &step.stderr_tail
            } else {
                &step.output_tail
            };
            format!("- {} (exit {}): {}", step.command, step.exit_code, output)
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        "CI steps failed during review. Failing steps:\n{formatted_list}\n\nPlease fix and commit."
    )
}

pub fn render_merge_conflict_prompt(conflict_paths: &[PathBuf], conflict_summary: &str) -> String {
    let paths = conflict_paths
        .iter()
        .map(|path| format!("- {}", path.display()))
        .collect::<Vec<_>>()
        .join("\n");

    let mut prompt = format!(
        "Merge conflict encountered on prior attempt. This is a merge-conflict re-review. Do not rebase or attempt integration from a managed execution. Manual Task-worktree repair is required before a fresh review. Conflict summary:\n{paths}"
    );

    if !conflict_summary.is_empty() {
        prompt.push_str("\n\n");
        prompt.push_str(conflict_summary);
    }

    prompt
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_review_fail_prompt_starts_with_opening_sentence() {
        let prompt =
            render_review_fail_prompt("missing error handling", "--- a/foo.rs\n+++ b/foo.rs");

        assert!(prompt.starts_with(
            "The reviewer flagged this implementation as needing changes. Reason: missing error handling. Current diff:"
        ));
    }

    #[test]
    fn render_ci_fail_prompt_starts_with_opening_sentence() {
        let steps = vec![
            StepResult {
                index: 0,
                command: "cargo build -p review".to_string(),
                exit_code: 101,
                stderr_tail: "compile error".to_string(),
                output_tail: "compile error".to_string(),
                started_at: "2026-01-01T00:00:00Z".to_string(),
                finished_at: "2026-01-01T00:00:01Z".to_string(),
            },
            StepResult {
                index: 1,
                command: "cargo test -p review".to_string(),
                exit_code: 1,
                stderr_tail: "test failure".to_string(),
                output_tail: "test failure".to_string(),
                started_at: "2026-01-01T00:00:00Z".to_string(),
                finished_at: "2026-01-01T00:00:01Z".to_string(),
            },
        ];

        let prompt = render_ci_fail_prompt(&steps);

        assert!(prompt.starts_with("CI steps failed during review. Failing steps:"));
    }

    #[test]
    fn render_merge_conflict_prompt_starts_with_opening_sentence() {
        let paths = vec![
            PathBuf::from("crates/review/src/lib.rs"),
            PathBuf::from("crates/review/src/follow_up.rs"),
        ];

        let prompt = render_merge_conflict_prompt(&paths, "both files changed on main");

        assert!(prompt.starts_with(
            "Merge conflict encountered on prior attempt. This is a merge-conflict re-review."
        ));
    }
}
