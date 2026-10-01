use api_types::AgentChatTurnStatus;
use chrono::{DateTime, Utc};
use services::agent_chat_policy::guard_agent_chat_content;
use services::{
    claim_agent_chat_turn, fail_agent_chat_turn_after_claim, recover_expired_agent_chat_turn,
    AgentChatOperation, AgentChatPolicyError, AgentChatScope,
};

fn at(seconds: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(seconds, 0).expect("valid timestamp")
}

#[test]
fn main_chat_denies_task_and_repository_authority_for_every_untrusted_target() {
    let scope = AgentChatScope::main("account-opaque");
    for target in [None, Some("project-other"), Some("task-other")] {
        assert_eq!(
            scope.authorize(AgentChatOperation::TaskManagement, target),
            Err(AgentChatPolicyError::MainTaskDenied)
        );
    }
    for operation in [
        AgentChatOperation::RepositoryRead,
        AgentChatOperation::RepositoryWrite,
    ] {
        assert_eq!(
            scope.authorize(operation, Some("private-project")),
            Err(AgentChatPolicyError::RepositoryDenied)
        );
    }
}

#[test]
fn project_chat_cannot_cross_binding_or_gain_repository_access() {
    let scope = AgentChatScope::project("account-opaque", "project-a");
    assert_eq!(
        scope.authorize(AgentChatOperation::TaskManagement, Some("project-a")),
        Ok(())
    );
    assert_eq!(
        scope.authorize(AgentChatOperation::TaskManagement, Some("project-b")),
        Err(AgentChatPolicyError::ProjectTaskOutsideBinding)
    );
    assert_eq!(
        scope.authorize(AgentChatOperation::TaskManagement, None),
        Err(AgentChatPolicyError::ProjectTaskTargetRequired)
    );
    assert_eq!(
        scope.authorize(AgentChatOperation::RepositoryRead, Some("project-a")),
        Err(AgentChatPolicyError::RepositoryDenied)
    );
}

#[test]
fn denial_text_never_echoes_opaque_scope_or_target_ids() {
    let scope = AgentChatScope::project("account-secret", "project-secret");
    let error = scope
        .authorize(AgentChatOperation::TaskManagement, Some("project-private"))
        .expect_err("cross-project Task management must be denied")
        .to_string();
    assert!(!error.contains("project-secret"));
    assert!(!error.contains("project-private"));
    assert!(!error.contains("account-secret"));
}

#[test]
fn handoff_and_chat_content_reject_common_protected_patterns() {
    for content in [
        "Authorization: Bearer redacted-token",
        "authorization : bearer redacted-token",
        "OPENAI_API_KEY = redacted-value",
        "openai api key: redacted-value",
        "ghp_redacted-github-token",
        "github_pat_redacted-token",
        "-----BEGIN OPENSSH PRIVATE KEY-----",
        "-----BEGIN PRIVATE KEY-----",
    ] {
        assert!(guard_agent_chat_content(content).is_err(), "{content}");
    }
    let ordinary = guard_agent_chat_content("bounded handoff with project reference")
        .expect("ordinary handoff content is admitted");
    assert_eq!(ordinary.content, "bounded handoff with project reference");
    assert!(ordinary.guard_json.contains("forge-content-guard-v1"));
}

#[test]
fn chat_turn_recovery_is_finite_and_does_not_reinvoke_after_budget() {
    let now = at(100);
    let lease = claim_agent_chat_turn(AgentChatTurnStatus::Queued, 0, 2, now, at(120), "owner-a")
        .expect("queued turn is claimable");
    assert_eq!(lease.status, AgentChatTurnStatus::Leased);

    let first_failure = fail_agent_chat_turn_after_claim(
        &api_types::TurnFailure::Unclassified,
        1,
        2,
        0,
        services::agent_chat_turn_policy::UsageLimitDeferrals::default(),
        now,
        "provider\nerror",
    );
    assert_eq!(first_failure.status, AgentChatTurnStatus::RetryWait);
    assert_eq!(first_failure.attempt_count, 1);
    assert_eq!(first_failure.error, "providererror");
    let terminal = fail_agent_chat_turn_after_claim(
        &api_types::TurnFailure::Unclassified,
        first_failure.attempt_count + 1,
        2,
        0,
        services::agent_chat_turn_policy::UsageLimitDeferrals::default(),
        at(105),
        "last",
    );
    assert_eq!(terminal.status, AgentChatTurnStatus::Failed);
    assert_eq!(terminal.attempt_count, 2);
    assert!(terminal.next_attempt_at.is_none());
    assert!(claim_agent_chat_turn(
        AgentChatTurnStatus::RetryWait,
        terminal.attempt_count,
        2,
        at(200),
        at(220),
        "owner-b",
    )
    .is_none());

    let recovered =
        recover_expired_agent_chat_turn(AgentChatTurnStatus::Leased, Some(at(99)), 1, 3, at(100))
            .expect("expired lease is recovered without invoking the model");
    assert_eq!(recovered.status, AgentChatTurnStatus::RetryWait);
    // The lease was already charged at claim time; recovery records the same
    // consumed attempt rather than charging the stale invocation twice.
    assert_eq!(recovered.attempt_count, 1);
}
