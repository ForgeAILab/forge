//! Solo session boundary validation and safe snapshot projection tests.
#[test]
fn solo_condition_projection_ignores_legacy_columns_for_all_seven_kinds() {
    let e = db::ConditionEvidence::default();
    let reason = db::ParkReason::Held {
        actor: "user".into(),
    };
    for c in [
        db::TaskCondition::Clear {
            evidence: e.clone(),
        },
        db::TaskCondition::Entering {
            state: "review".into(),
            epoch: 1,
            step_id: "step".into(),
            phase: "checks".into(),
            since: "now".into(),
            evidence: e.clone(),
        },
        db::TaskCondition::Running {
            execution_id: "run".into(),
            role: "coder".into(),
            epoch: 1,
            since: "now".into(),
            evidence: e.clone(),
        },
        db::TaskCondition::Deferred {
            until: None,
            reason: db::RetryCause::Legacy,
            resume: db::ConditionContinuation::Reconcile,
            evidence: e.clone(),
        },
        db::TaskCondition::Parked {
            primary: reason.clone(),
            additional: vec![],
            resume: db::ConditionContinuation::Reconcile,
            since: None,
            evidence: e.clone(),
        },
        db::TaskCondition::Failed {
            failure: reason,
            additional: vec![],
            resume: db::ConditionContinuation::Reconcile,
            since: None,
            evidence: e.clone(),
        },
        db::TaskCondition::Settled {
            outcome: db::TerminalOutcome::Completed,
            evidence: e,
        },
    ] {
        let mut task = services::task_actions::test_support::snapshot("in_progress", None).task;
        task.condition = c;
        let expected = serde_json::to_value(super::task_interruption(&task)).unwrap();
        task.error_annotation = Some(
            serde_json::json!({"type":"workspace_error","message":"legacy poison"}).to_string(),
        );
        task.blocked_json =
            Some(serde_json::json!({"reason":"legacy poison","created_at":"now"}).to_string());
        task.failed_json = task.blocked_json.clone();
        assert_eq!(
            expected,
            serde_json::to_value(super::task_interruption(&task)).unwrap()
        );
    }
}

use super::*;
#[test]
fn review_decisions_preserve_typed_guidance_and_reason() {
    assert!(solo_review_action(SoloReviewDecision::RequestChanges, None).is_err());
    assert!(solo_review_action(SoloReviewDecision::RequestChanges, Some("  ".into())).is_err());
    assert_eq!(
        solo_review_action(
            SoloReviewDecision::RequestChanges,
            Some("Fix the missing coverage".into())
        )
        .unwrap(),
        api_types::TaskAction::SendBack {
            guidance: "Fix the missing coverage".into()
        }
    );
    assert_eq!(
        solo_review_action(
            SoloReviewDecision::Accept,
            Some("Track in follow-up".into())
        )
        .unwrap(),
        api_types::TaskAction::Approve {
            override_checks: Some(false),
            reason: Some("Track in follow-up".into())
        }
    );
}

#[test]
fn finite_turn_states_keep_terminal_and_waiting_distinct() {
    assert!(!SoloTurnStatus::Queued.is_terminal());
    assert!(!SoloTurnStatus::Leased.is_terminal());
    assert!(!SoloTurnStatus::AwaitingInput.is_terminal());
    assert!(!SoloTurnStatus::RetryWait.is_terminal());
    assert!(SoloTurnStatus::Succeeded.is_terminal());
    assert!(SoloTurnStatus::Failed.is_terminal());
    assert!(SoloTurnStatus::Cancelled.is_terminal());
}

#[test]
fn retry_input_requires_positive_version_and_bounded_key() {
    let input = SoloRetryTurnInput {
        turn_job_id: "turn-123".to_owned(),
        expected_version: 7,
        idempotency_key: "retry-command-7".to_owned(),
    };
    let (turn_job_id, idempotency_key, version) =
        validate_retry_input(input).expect("valid retry input");
    assert_eq!(turn_job_id, "turn-123");
    assert_eq!(idempotency_key, "retry-command-7");
    assert_eq!(version, 7);

    assert!(validate_retry_input(SoloRetryTurnInput {
        turn_job_id: "turn-123".to_owned(),
        expected_version: 0,
        idempotency_key: "retry-command".to_owned(),
    })
    .is_err());
    assert!(validate_retry_input(SoloRetryTurnInput {
        turn_job_id: "../turn".to_owned(),
        expected_version: 1,
        idempotency_key: "retry-command".to_owned(),
    })
    .is_err());
    assert!(validate_retry_input(SoloRetryTurnInput {
        turn_job_id: "turn-123".to_owned(),
        expected_version: 1,
        idempotency_key: " ".to_owned(),
    })
    .is_err());
    assert!(validate_retry_input(SoloRetryTurnInput {
        turn_job_id: "turn-123".to_owned(),
        expected_version: 1,
        idempotency_key: "k".repeat(SOLO_MAX_ID_CHARS + 1),
    })
    .is_err());
}

#[test]
fn scope_and_refresh_limits_fail_closed_at_the_boundary() {
    let invalid_scope = SoloSessionScope::new("", "project", "repo", "chat");
    assert!(invalid_scope.validate().is_err());
    let escaping_scope = SoloSessionScope::new("owner", "../project", "repo", "chat");
    assert!(escaping_scope.validate().is_err());
    let invalid_limits = SoloRefreshLimits {
        messages: SOLO_MAX_MESSAGES + 1,
        ..SoloRefreshLimits::default()
    };
    assert!(invalid_limits.bounded().is_err());
}

#[test]
fn activity_projection_hides_reasoning_body_and_unknown_payloads() {
    let thinking = LogEntry {
        schema_version: 1,
        sequence: 1,
        timestamp: "2026-01-01T00:00:00Z".to_owned(),
        execution_id: "turn".to_owned(),
        kind: LogKind::Thinking,
        stream: executors::LogStream::Main,
        payload: json!({"text": "private reasoning"}),
        truncated: false,
    };
    let unknown = LogEntry {
        kind: LogKind::Stdout,
        ..thinking.clone()
    };
    let projected = activity_entry(&thinking).expect("thinking marker");
    assert_eq!(projected.kind, SoloActivityKind::Thinking);
    assert!(projected.text.is_none());
    assert!(activity_entry(&unknown).is_none());
}

#[test]
fn suspicious_content_is_replaced_with_a_fixed_marker() {
    let (content, redacted) = visible_content("password=hunter2", "internal");
    assert_eq!(content, "[redacted]");
    assert!(redacted);
    let (content, redacted) = visible_content("ordinary answer", "internal");
    assert_eq!(content, "ordinary answer");
    assert!(!redacted);
    let (content, redacted) = visible_content("task-123 completed", "internal");
    assert_eq!(content, "task-123 completed");
    assert!(!redacted);
    assert!(contains_secret_marker("sk-live-secret"));
    assert!(!contains_secret_marker("task-123"));
}

#[test]
fn approval_input_requires_positive_versions_and_bounded_ids() {
    let target = SoloCharterApprovalTarget {
        kind: SoloCharterApprovalKind::Adoption,
        project_id: "project".to_owned(),
        charter_id: "charter".to_owned(),
        revision_id: "revision".to_owned(),
        content_digest: "content".to_owned(),
        rendered_digest: "render".to_owned(),
        expected_charter_version: 1,
        expected_project_version: 1,
        approved_project_name: "Project".to_owned(),
        approved_project_slug: None,
        project_mode: "compact".to_owned(),
        selected_project_agent_identity_id: "agent".to_owned(),
        selected_project_agent_profile_revision_id: "profile".to_owned(),
        selected_project_agent_operating_skill_revision: "forge.project.orchestration/v1@1"
            .to_owned(),
        selected_project_agent_policy_digest: "digest".to_owned(),
    };
    let input = SoloCharterApprovalInput {
        target,
        idempotency_key: "approval-key".to_owned(),
        authorization: SoloCharterApprovalAuthorization {
            authorization_event_id: "event".to_owned(),
            authorization_basis: "focused approval".to_owned(),
            authorization_occurred_at: "2026-01-01T00:00:00Z".to_owned(),
        },
    };
    assert!(validate_approval_input(&input).is_ok());
    let mut invalid = input;
    invalid.target.expected_project_version = 0;
    assert!(validate_approval_input(&invalid).is_err());
}

#[test]
fn activity_identifiers_reject_path_escape_components() {
    assert!(path_component_id("turn_job_id", "turn-123".to_owned()).is_ok());
    for value in [
        "../turn",
        "turn/other",
        "turn\\other",
        "/tmp/turn",
        ".",
        "..",
    ] {
        assert!(
            path_component_id("turn_job_id", value.to_owned()).is_err(),
            "{value}"
        );
    }
}

#[test]
fn review_checks_derive_completion_from_exit_code() {
    let checks = parse_review_checks(
        r#"{"ci_steps":[
                {"index":0,"command":"cargo test","exit_code":0},
                {"index":1,"command":"cargo clippy","exit_code":1},
                {"index":2,"command":"manual","success":true}
            ]}"#,
    );

    assert_eq!(checks.len(), 3);
    assert_eq!(checks[0].success, Some(true));
    assert_eq!(checks[1].success, Some(false));
    assert_eq!(checks[2].success, Some(true));
}

#[test]
fn execution_log_path_requires_exact_bound_canonical_location() {
    let root = Path::new("/tmp/solo-worktrees");
    let expected = root
        .join(".forge/logs/project/task/execution.jsonl")
        .to_string_lossy()
        .into_owned();
    assert!(scoped_execution_log_path(root, "project", "task", "execution", &expected).is_ok());
    assert!(scoped_execution_log_path(
        root,
        "project",
        "task",
        "execution",
        "/tmp/other/execution.jsonl"
    )
    .is_err());
    assert!(scoped_execution_log_path(
        root,
        "project",
        "task",
        "execution",
        &root
            .join(".forge/logs/project/task/other.jsonl")
            .to_string_lossy()
    )
    .is_err());
    assert!(scoped_execution_log_path(
        root,
        "project",
        "task",
        "execution",
        &root
            .join(".forge/logs/project/../other/execution.jsonl")
            .to_string_lossy()
    )
    .is_err());
    assert!(normalize_scoped_path(root, &root.join("../outside.jsonl")).is_err());
}

#[cfg(unix)]
#[tokio::test]
async fn activity_reader_rejects_symlinked_log_files() {
    use std::{fs, os::unix::fs::symlink};

    let temporary = tempfile::tempdir().expect("temporary directory");
    let root = temporary.path().join("logs");
    fs::create_dir_all(&root).expect("log root");
    let target = temporary.path().join("outside.jsonl");
    fs::write(&target, "{}").expect("outside log");
    let linked = root.join("turn.jsonl");
    symlink(&target, &linked).expect("symlink log");

    let result = read_activity_path(&linked, &root, "turn", SoloActivityCursor::default(), 1).await;
    assert!(result.is_err());
}
