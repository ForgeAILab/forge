#![allow(dead_code)]

mod common;

use std::{sync::Arc, time::Duration};

use api_types::{ErrorResponse, ProtectedInteractionSummaryResponse};
use axum::http::{Method, StatusCode};
use db::{
    AgentContextScopeRepo, AgentRepo, AgentSessionRepo, AgentStatus, CreateAgentContextScope,
    CreateAgentIdentity, CreateAgentProfile, CreateAgentSession, UserRepo,
};
use forge_agent_host::{
    Deadline, InteractionBroker, InteractionOrigin, InteractionOutcomeKind, InteractionRequest,
    InteractionRequestId, InteractionSensitivity, Question, QuestionId, Questionnaire, SessionId,
    ToolCallId, TurnId,
};
use serde_json::json;

const REQUEST_SECRET: &str = "protected-question-secret-marker";
const ANSWER_SECRET: &str = "protected-answer-secret-marker";

#[tokio::test]
async fn protected_interactions_are_owner_scoped_versioned_and_never_reflect_answers() {
    let workspace = common::TestDir::new("protected-interactions");
    let harness = common::test_app(workspace.path(), "protected-interactions").await;
    let session_id = seed_owned_native_session(&harness.state.db).await;
    seed_other_user(&harness.state.db).await;

    let broker = harness.state.embedded_agent_service.interaction_broker();
    let request = interaction_request("interaction-answer", REQUEST_SECRET);
    let request_for_runtime = request.clone();
    let broker_for_runtime = broker.clone();
    let runtime =
        tokio::spawn(async move { broker_for_runtime.interact(&request_for_runtime).await });

    let summaries = wait_for_pending(&harness.app, &session_id, common::test_jwt()).await;
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].id, "interaction-answer");
    assert_eq!(summaries[0].session_id, session_id);
    assert!(!serde_json::to_string(&summaries)
        .expect("summaries serialize")
        .contains(REQUEST_SECRET));

    let other_token = jwt_for("other-user-id", "other@example.com");
    let hidden: Vec<ProtectedInteractionSummaryResponse> = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/agent-sessions/{session_id}/interactions"),
        &other_token,
        StatusCode::OK,
    )
    .await;
    assert!(hidden.is_empty());
    let denied: ErrorResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &format!("/api/v1/agent-sessions/{session_id}/interactions/interaction-answer/answer"),
        &other_token,
        json!({
            "expected_version": 1,
            "values": [{
                "kind": "free_form",
                "question_id": "secret-question",
                "value": ANSWER_SECRET
            }]
        }),
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(denied.code, "protected_interaction.not_found");
    assert!(!serde_json::to_string(&denied)
        .expect("error serializes")
        .contains(ANSWER_SECRET));

    let answered: ProtectedInteractionSummaryResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &format!("/api/v1/agent-sessions/{session_id}/interactions/interaction-answer/answer"),
        &common::test_jwt(),
        json!({
            "expected_version": 1,
            "values": [{
                "kind": "free_form",
                "question_id": "secret-question",
                "value": ANSWER_SECRET
            }]
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(answered.status, "answered");
    assert_eq!(answered.version, 2);
    assert!(!serde_json::to_string(&answered)
        .expect("answer summary serializes")
        .contains(ANSWER_SECRET));

    let stale: ErrorResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &format!("/api/v1/agent-sessions/{session_id}/interactions/interaction-answer/answer"),
        &common::test_jwt(),
        json!({
            "expected_version": 1,
            "values": [{
                "kind": "free_form",
                "question_id": "secret-question",
                "value": ANSWER_SECRET
            }]
        }),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(stale.code, "protected_interaction.version_conflict");
    assert!(!serde_json::to_string(&stale)
        .expect("conflict serializes")
        .contains(ANSWER_SECRET));

    let response = tokio::time::timeout(Duration::from_secs(3), runtime)
        .await
        .expect("runtime receives protected answer")
        .expect("runtime task joins");
    assert_eq!(response.outcome_kind(), InteractionOutcomeKind::Answered);

    let cancel_request = interaction_request("interaction-cancel", "cancel-question-secret");
    let broker_for_cancel = broker.clone();
    let cancel_runtime =
        tokio::spawn(async move { broker_for_cancel.interact(&cancel_request).await });
    let pending = wait_for_pending(&harness.app, &session_id, common::test_jwt()).await;
    assert!(pending.iter().any(|item| item.id == "interaction-cancel"));
    let cancelled: ProtectedInteractionSummaryResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &format!("/api/v1/agent-sessions/{session_id}/interactions/interaction-cancel/cancel"),
        &common::test_jwt(),
        json!({ "expected_version": 1 }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(cancelled.status, "cancelled");
    assert_eq!(cancelled.version, 2);
    let cancelled_response = tokio::time::timeout(Duration::from_secs(3), cancel_runtime)
        .await
        .expect("runtime receives cancellation")
        .expect("cancel task joins");
    assert_eq!(
        cancelled_response.outcome_kind(),
        InteractionOutcomeKind::Cancelled
    );

    for (table, column) in [
        ("domain_event", "payload_json"),
        ("agent_chat_message", "content"),
        ("memory_item", "body"),
        ("memory_item", "metadata_json"),
        ("context_manifest", "request_fingerprint"),
        ("context_manifest_source", "selection_reason"),
    ] {
        let sql = format!("SELECT COUNT(*) FROM {table} WHERE instr({column}, ?) > 0");
        let count: i64 = sqlx::query_scalar(&sql)
            .bind(ANSWER_SECRET)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("ordinary surface scans");
        assert_eq!(count, 0, "secret leaked into {table}.{column}");
    }
}

async fn wait_for_pending(
    app: &axum::Router,
    session_id: &str,
    token: String,
) -> Vec<ProtectedInteractionSummaryResponse> {
    for _ in 0..100 {
        let summaries: Vec<ProtectedInteractionSummaryResponse> =
            common::empty_request_with_bearer(
                app,
                Method::GET,
                &format!("/api/v1/agent-sessions/{session_id}/interactions"),
                &token,
                StatusCode::OK,
            )
            .await;
        if !summaries.is_empty() {
            return summaries;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("protected interaction was not persisted");
}

fn interaction_request(id: &str, prompt: &str) -> InteractionRequest {
    InteractionRequest::questionnaire(
        InteractionRequestId::new(id),
        InteractionOrigin::new(
            SessionId::new("runtime-session"),
            TurnId::new(format!("turn-{id}")),
            ToolCallId::new(format!("tool-{id}")),
        ),
        Questionnaire::new(vec![Question::new(
            QuestionId::new("secret-question"),
            "Sensitive question",
            prompt,
        )
        .allow_free_form(true)])
        .expect("questionnaire is valid"),
        Deadline::never(),
        InteractionSensitivity::Sensitive,
    )
    .expect("interaction request is valid")
}

async fn seed_owned_native_session(db: &Arc<db::SqliteDb>) -> String {
    let now = db::now_rfc3339();
    AgentRepo::create_identity_with_profile(
        db.as_ref(),
        CreateAgentIdentity {
            id: "interaction-identity".to_owned(),
            name: "Interaction identity".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some("test-user-id".to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: "interaction-profile".to_owned(),
            identity_id: "interaction-identity".to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("test".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("identity/profile creates");
    AgentContextScopeRepo::create_context_scope(
        db.as_ref(),
        CreateAgentContextScope {
            id: "interaction-scope".to_owned(),
            identity_id: "interaction-identity".to_owned(),
            scope_type: "account".to_owned(),
            scope_id: "test-user-id".to_owned(),
            project_id: None,
            task_id: None,
            task_role: None,
            workspace_access: "deny".to_owned(),
            workspace_path: None,
            authority_json: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("context scope creates");
    AgentSessionRepo::create_agent_session(
        db.as_ref(),
        CreateAgentSession {
            id: "interaction-session".to_owned(),
            identity_id: "interaction-identity".to_owned(),
            profile_id: "interaction-profile".to_owned(),
            context_scope_id: "interaction-scope".to_owned(),
            backend_kind: "native".to_owned(),
            runtime_session_id: Some("runtime-session".to_owned()),
            status: "ready".to_owned(),
            capabilities_json: "{}".to_owned(),
            connection_status: "healthy".to_owned(),
            predecessor_session_id: None,
            last_activity_at: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("session creates");
    "interaction-session".to_owned()
}

async fn seed_other_user(db: &Arc<db::SqliteDb>) {
    let now = db::now_rfc3339();
    UserRepo::create_user(
        db.as_ref(),
        &db::User {
            id: "other-user-id".to_owned(),
            email: "other@example.com".to_owned(),
            password_hash: "$2b$04$placeholder".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("other user creates");
}

fn jwt_for(user_id: &str, email: &str) -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_secs();
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &json!({
            "sub": user_id,
            "email": email,
            "is_admin": false,
            "iat": now,
            "exp": now + 900,
        }),
        &EncodingKey::from_secret(b"test-jwt-secret-for-development"),
    )
    .expect("test JWT encodes")
}
