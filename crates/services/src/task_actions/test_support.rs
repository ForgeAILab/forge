//! Task snapshot fixtures shared with dependent crates.

use super::*;
pub use db::task_condition::test_support::set_task_condition;
use serde_json::json;

pub fn snapshot(state: &str, kind: Option<FailureKind>) -> TaskSnapshot {
    let mut task: Task = serde_json::from_value(json!({
            "id":"task", "project_id":"project", "parent_task_id":null, "assignee_type":"agent", "assignee_id":"agent", "title":"Fixture", "description":null, "task_type":"task", "status":state, "is_automation":false, "priority":0, "board_position":0.0, "subtask_order":null, "task_state_config":null, "merge_config":null, "metadata_json":null, "plan":null, "error_annotation":null, "blocked_json":null, "failed_json":null, "entry_barrier_json":null, "review_passed_at":null, "archived_at":null, "deleted_at":null, "version":1, "created_at":"2026-10-02T00:00:00Z", "updated_at":"2026-10-02T00:00:00Z"
        })).expect("Task fixture");
    task.error_annotation = kind.map(|kind| json!({ "type":kind, "blocking_reason":"fixture", "blocked_by":null, "blocked_at":null, "blocked_execution_id":null, "artifact":null, "message":"fixture", "hook":null }).to_string());
    TaskSnapshot {
        task,
        workflow: crate::workflow::default_workflow::default_workflow(),
        executions: Vec::new(),
        latest_review: None,
        role_assignments: Vec::new(),
        transition_logs: Vec::new(),
        budget_spent: Default::default(),
        recovery_budget_limit: 0,
        caller: ActionCaller::owner(),
        has_agent: true,
        dependencies_satisfied: true,
        owner_supports_resume: true,
        coordination_root: false,
        owner_disconnected: false,
        project_paused: false,
        wait_cause: None,
        action_agent_id: None,
        planning_approval_ready: true,
        advance_target: None,
        entry_hooks_running: false,
    }
}
