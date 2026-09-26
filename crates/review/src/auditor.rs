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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn override_is_preserved_for_final_contract_assembly() {
        let prompt = render_auditor_prompt("ignored", None, "diff", Some("Use my rubric."));
        assert_eq!(prompt, "Use my rubric.");
    }
}
