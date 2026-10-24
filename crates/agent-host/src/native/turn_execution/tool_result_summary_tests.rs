//! Structured and bounded fallback tool-result summary tests.

use super::*;
use api_types::{CanonicalScopeRef, OutcomeCode, OutcomeScopeType, OutcomeStatus, RetryAction};

#[test]
fn reunites_a_structured_forge_outcome_with_the_runtime_event_boundary() {
    // Reproduces F14: the runtime event only carries `is_error`, so a
    // native Forge command's typed outcome must be recovered from the
    // tool's JSON value rather than lost at this boundary.
    let outcome = OrchestrationOutcome::failed(
        OutcomeCode::PolicyDenied,
        "task.adaptive",
        CanonicalScopeRef::new(OutcomeScopeType::Task, "task-1"),
        "corr-task-1",
        "the operation is not admitted for the current Forge scope",
    );
    let tool_outcome = ToolOutcome {
        value: serde_json::to_value(&outcome).expect("outcome serializes"),
        content: Default::default(),
        is_error: true,
    };

    let summary = tool_result_summary("call-1", &Ok(tool_outcome));

    assert_eq!(summary.status, OutcomeStatus::Failed);
    assert_eq!(summary.code, OutcomeCode::PolicyDenied);
    assert_eq!(
        summary.safe_message,
        "the operation is not admitted for the current Forge scope"
    );
    assert_eq!(summary.correlation_id, "corr-task-1");
}

#[test]
fn preserves_retry_and_recovery_from_a_structured_outcome() {
    let mut outcome = OrchestrationOutcome::failed(
        OutcomeCode::VersionConflict,
        "project.execution_baseline",
        CanonicalScopeRef::new(OutcomeScopeType::Project, "project-1"),
        "corr-baseline-1",
        "the authorized resource changed; refresh current state and retry",
    );
    outcome.retry = Some(api_types::RetryInstruction::new(
        RetryAction::RefreshAndRetry,
        true,
    ));
    let tool_outcome = ToolOutcome {
        value: serde_json::to_value(&outcome).expect("outcome serializes"),
        content: Default::default(),
        is_error: true,
    };

    let summary = tool_result_summary("call-2", &Ok(tool_outcome));

    assert!(summary.retryable);
    assert_eq!(summary.recovery_action, Some(RetryAction::RefreshAndRetry));
}

#[test]
fn falls_back_to_a_generic_bounded_summary_for_worktree_results() {
    // A Task worktree command (e.g. `forge_task_command` running `git
    // commit`) never carries an `OrchestrationOutcome` — its JSON value
    // is arbitrary process stdout/stderr, which must not be echoed as a
    // safe message.
    let tool_outcome = ToolOutcome {
        value: serde_json::json!({
            "program": "git",
            "args": ["commit"],
            "status": 1,
            "success": false,
            "stdout": "",
            "stderr": "fatal: uncommitted worktree changes with SECRET_TOKEN=abc123",
        }),
        content: Default::default(),
        is_error: false,
    };

    let summary = tool_result_summary("call-3", &Ok(tool_outcome));

    assert_eq!(summary.status, OutcomeStatus::Succeeded);
    assert_eq!(summary.code, OutcomeCode::Ok);
    assert_eq!(summary.correlation_id, "call-3");
    let serialized = serde_json::to_string(&summary).expect("summary serializes");
    assert!(!serialized.contains("SECRET_TOKEN"));
}

#[test]
fn falls_back_to_a_generic_bounded_summary_for_a_raw_runtime_failure() {
    let error = RuntimeError::tool("Task command failed: SECRET_TOKEN=abc123 leaked in stderr");

    let summary = tool_result_summary("call-4", &Err(error));

    assert_eq!(summary.status, OutcomeStatus::Failed);
    assert_eq!(summary.code, OutcomeCode::InternalFailure);
    assert_eq!(summary.correlation_id, "call-4");
    let serialized = serde_json::to_string(&summary).expect("summary serializes");
    assert!(!serialized.contains("SECRET_TOKEN"));
}
