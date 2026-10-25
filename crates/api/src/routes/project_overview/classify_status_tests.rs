//! Test Task status classification for overview counts.

use super::*;

/// A renamed review gate counts as review whatever its name, and a state
/// whose name says review or merge stays in the review bucket, as before.
#[test]
fn review_bucket_adds_renamed_review_gates_and_keeps_review_names() {
    let mut workflow = services::workflow::default_workflow::default_workflow();
    for state in &mut workflow.states {
        if state.name == "review" {
            state.name = "signoff".into();
        }
    }
    for status in ["signoff", "security_review", "review", "merge_failed"] {
        assert!(
            matches!(classify_status(status, &workflow), TaskBucket::Review),
            "{status}"
        );
    }
    assert!(matches!(
        classify_status("in_progress", &workflow),
        TaskBucket::Active
    ));
}
