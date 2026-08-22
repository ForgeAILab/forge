use super::super::{
    bounded_redacted_remote_diagnostic, remote_execution_is_live_for_terminal,
    remote_terminal_error_message, stable_remote_owner_ref, REMOTE_TERMINAL_DIAGNOSTIC_MAX_CHARS,
    REMOTE_TERMINAL_DIAGNOSTIC_REDACTED,
};
use chrono::{Duration, Utc};
use db::{Execution, ExecutionStatus};

#[test]
fn remote_diagnostics_are_bounded_single_line_and_redacted() {
    let oversized = format!("{}\napi_key=super-secret\0", "safe ".repeat(200));
    let bounded = bounded_redacted_remote_diagnostic(&oversized);

    assert_eq!(bounded, REMOTE_TERMINAL_DIAGNOSTIC_REDACTED);
    assert!(bounded.chars().count() <= REMOTE_TERMINAL_DIAGNOSTIC_MAX_CHARS);
    assert!(!bounded.contains("super-secret"));
    assert!(!bounded.contains('\n'));
    assert!(!bounded.contains('\0'));
}

#[test]
fn remote_terminal_error_message_does_not_persist_raw_adversarial_text() {
    let error = format!("provider failed: {} bearer secret-value", "x".repeat(1_000));
    let signal = format!("SIGTERM\n{}", "y".repeat(1_000));
    let message = remote_terminal_error_message(Some(i32::MIN), Some(&signal), Some(&error));

    assert!(message.chars().count() <= REMOTE_TERMINAL_DIAGNOSTIC_MAX_CHARS);
    assert!(message.starts_with(REMOTE_TERMINAL_DIAGNOSTIC_REDACTED));
    assert!(!message.contains("secret-value"));
    assert!(!message.contains('\n'));
}

#[test]
fn remote_owner_reference_is_server_owned_and_stable() {
    let owner = "daemon:opaque-daemon-id:connection:42";
    assert_eq!(stable_remote_owner_ref(owner), owner);
    assert_eq!(
        stable_remote_owner_ref(owner),
        stable_remote_owner_ref(owner)
    );
}

fn running_execution(owner: &str, lease_expires_at: String, hard_deadline_at: String) -> Execution {
    let now = Utc::now().to_rfc3339();
    Execution {
        id: "execution-1".to_owned(),
        task_id: "task-1".to_owned(),
        agent_id: Some("agent-1".to_owned()),
        role: "coder".to_owned(),
        status: ExecutionStatus::Running,
        stop_reason: None,
        stopped_by: None,
        resume_policy: None,
        stopped_at: None,
        parent_execution_id: None,
        agent_session_id: None,
        agent_message_id: None,
        last_activity_at: None,
        prompt: None,
        summary: None,
        logs_path: None,
        before_sha: None,
        after_sha: None,
        error: None,
        executor_config_snapshot_json: None,
        workspace_id: None,
        execution_version: 7,
        lease_owner: Some(owner.to_owned()),
        lease_expires_at: Some(lease_expires_at),
        hard_deadline_at: Some(hard_deadline_at),
        last_heartbeat_at: Some(now.clone()),
        last_progress_at: Some(now.clone()),
        created_at: now.clone(),
        updated_at: now,
    }
}

#[test]
fn remote_terminal_retry_live_check_rejects_expired_or_other_owner() {
    let owner = "daemon:opaque-daemon-id:connection:42";
    let now = Utc::now();
    let live = running_execution(
        owner,
        (now + Duration::minutes(1)).to_rfc3339(),
        (now + Duration::minutes(5)).to_rfc3339(),
    );
    assert!(remote_execution_is_live_for_terminal(&live, owner, now));

    let expired = running_execution(
        owner,
        (now - Duration::seconds(1)).to_rfc3339(),
        (now + Duration::minutes(5)).to_rfc3339(),
    );
    assert!(!remote_execution_is_live_for_terminal(&expired, owner, now));

    let wrong_owner = running_execution(
        "daemon:other-daemon:connection:9",
        (now + Duration::minutes(1)).to_rfc3339(),
        (now + Duration::minutes(5)).to_rfc3339(),
    );
    assert!(!remote_execution_is_live_for_terminal(
        &wrong_owner,
        owner,
        now
    ));
}
