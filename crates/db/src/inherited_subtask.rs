//! The names of the inherited subtask workflow that the database layer reads.
//!
//! The workflow itself is defined in the services crate
//! (`workflow::inherited_subtask_workflow`). The database decides three things
//! from these names without loading a workflow: which workflow governs a
//! subtask, which role a state runs, and whose assignment row holds the
//! step-check role. This module is the one place those names live; the
//! services crate reads them from here and a test there fails when the
//! workflow definition and this module diverge.

/// Every state of the inherited subtask workflow.
pub const STATES: [&str; 5] = ["todo", "in_progress", STEP_CHECK_STATE, "done", "cancelled"];

/// The step check: the gate between implementation and `done`.
pub const STEP_CHECK_STATE: &str = "lead_check";

/// The role of the step check. On a subtask it is held by the root's row
/// (its lead) and never by a row of the subtask.
pub const STEP_CHECK_ROLE: &str = "planner";

/// Whether `status` is a state of the inherited subtask workflow.
pub fn is_state(status: &str) -> bool {
    STATES.contains(&status)
}

/// The role the inherited subtask workflow runs in `status`.
pub fn role(status: &str) -> Option<&'static str> {
    match status {
        "in_progress" => Some("coder"),
        STEP_CHECK_STATE => Some(STEP_CHECK_ROLE),
        _ => None,
    }
}

/// The Task whose own row decides who holds `role_name` on a Task with this
/// parent and status: the root for the step-check role of a subtask in an
/// inherited state, `None` (the Task itself) for everything else.
pub fn step_check_holder<'a>(
    parent_task_id: Option<&'a str>,
    status: &str,
    role_name: &str,
) -> Option<&'a str> {
    parent_task_id.filter(|_| role_name == STEP_CHECK_ROLE && is_state(status))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_step_check_role_is_held_by_the_root_in_inherited_states_only() {
        for state in STATES {
            assert_eq!(
                step_check_holder(Some("root"), state, "planner"),
                Some("root")
            );
            assert_eq!(step_check_holder(Some("root"), state, "coder"), None);
            assert_eq!(step_check_holder(Some("root"), state, "reviewer"), None);
            assert_eq!(step_check_holder(None, state, "planner"), None);
        }
        for state in ["planning", "review", "merging", "merge_failed", "backlog"] {
            assert_eq!(step_check_holder(Some("root"), state, "planner"), None);
        }
        assert_eq!(role(STEP_CHECK_STATE), Some(STEP_CHECK_ROLE));
    }
}
