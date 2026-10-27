//! Project repository naming and scaffold guidance tests.

use super::{repo_directory_name, FORGE_AGENTS_SECTION};

#[test]
fn scaffold_exports_merge_friendly_layout_guidance_once() {
    const RULE: &str = forge_agent_host::MERGE_FRIENDLY_LAYOUT_GUIDANCE;
    assert_eq!(FORGE_AGENTS_SECTION.matches(RULE).count(), 1);
    assert!(FORGE_AGENTS_SECTION.contains("For implementation and follow-up proposals:"));
    assert!(FORGE_AGENTS_SECTION.contains("Report a required out-of-scope edit"));
}

#[test]
fn repo_directory_name_is_slugged_and_deterministic() {
    assert_eq!(
        repo_directory_name("Simple Todo!", "ab591984-975f-4406"),
        "simple-todo-ab591984"
    );
    assert_eq!(repo_directory_name("---", "12345678-x"), "project-12345678");
}
