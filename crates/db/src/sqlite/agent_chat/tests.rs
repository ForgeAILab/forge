//! Tests for supported frozen Main baseline revisions.

use super::supported_main_baseline_revision;

#[test]
fn only_frozen_main_baseline_revisions_are_supported() {
    assert!(supported_main_baseline_revision("forge.main.baseline/v1@1"));
    assert!(supported_main_baseline_revision("forge.main.baseline/v1@2"));
    assert!(supported_main_baseline_revision("forge.main.baseline/v1@3"));
    assert!(supported_main_baseline_revision("forge.main.baseline/v1@4"));
    assert!(supported_main_baseline_revision("forge.main.baseline/v1@5"));
    // A revision this build has no compiled body for is refused, so a
    // downgrade cannot render a contract it does not have.
    assert!(!supported_main_baseline_revision(
        "forge.main.baseline/v1@6"
    ));
    assert!(!supported_main_baseline_revision(
        "forge.main.project-discovery/v2@2"
    ));
    assert!(!supported_main_baseline_revision(""));
}
