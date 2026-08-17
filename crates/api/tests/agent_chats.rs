#![allow(dead_code)]

mod common;

use api_types::{
    AgentChatMessageListResponse, AgentChatSwitcherResponse, AgentChatTurnJobResponse,
    AgentChatTurnStatus, ConnectedEmbeddedAgentResponse, ErrorResponse, MainAgentBindingResponse,
    ProjectAgentBindingResponse, ProjectResponse, SendAgentChatMessageResponse,
};
use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use serde_json::json;
use tower::ServiceExt;

#[tokio::test]
async fn agent_chat_switcher_requires_authentication_and_hides_unknown_chat() {
    let workspace = common::TestDir::new("agent-chat-route-auth");
    let harness = common::test_app(workspace.path(), "agent-chat-route-auth").await;

    let response = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/agent-chats")
                .body(Body::empty())
                .expect("build unauthenticated request"),
        )
        .await
        .expect("router response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let switcher: AgentChatSwitcherResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/agent-chats",
        &common::test_jwt(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        switcher
            .items
            .iter()
            .filter(|item| item.kind == api_types::AgentChatKind::Main)
            .count(),
        1
    );
    let main = switcher
        .items
        .iter()
        .find(|item| item.kind == api_types::AgentChatKind::Main)
        .expect("main chat switcher item");
    assert_eq!(
        main.binding_state,
        api_types::AgentBindingState::SetupRequired
    );
    assert_eq!(main.chat_status, api_types::AgentChatStatus::SetupRequired);

    let error: ErrorResponse = common::json_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/agent-chats/unknown-chat",
        &common::test_jwt(),
        json!(null),
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(error.code, "not_found");
}

#[tokio::test]
async fn project_creation_records_chat_binding_events_and_stale_binding_replacements_conflict() {
    let workspace = common::TestDir::new("agent-chat-project-events");
    let harness = common::test_app(workspace.path(), "agent-chat-project-events").await;
    let token = common::test_jwt();
    let connected: ConnectedEmbeddedAgentResponse = common::connect_embedded_agent(
        &harness.app,
        &token,
        "project-event-agent",
        "project-event",
        "project-event-secret",
        json!({"permissions": ["read_agent_chat", "propose_message"]}),
        json!({"allowed": ["read_agent_chat", "propose_message"]}),
    )
    .await;

    let initial_main: MainAgentBindingResponse = common::json_request_with_bearer(
        &harness.app,
        Method::PUT,
        "/api/v1/account/main-agent",
        &token,
        json!({
            "identity_id": connected.agent.id,
            "profile_id": connected.profile.id,
            "expected_version": 0,
            "autonomy_policy": {}
        }),
        StatusCode::OK,
    )
    .await;
    let _replacement: MainAgentBindingResponse = common::json_request_with_bearer(
        &harness.app,
        Method::PUT,
        "/api/v1/account/main-agent",
        &token,
        json!({
            "identity_id": connected.agent.id,
            "profile_id": connected.profile.id,
            "expected_version": initial_main.version,
            "autonomy_policy": {}
        }),
        StatusCode::OK,
    )
    .await;
    let stale_main: ErrorResponse = common::json_request_with_bearer(
        &harness.app,
        Method::PUT,
        "/api/v1/account/main-agent",
        &token,
        json!({
            "identity_id": connected.agent.id,
            "profile_id": connected.profile.id,
            "expected_version": initial_main.version,
            "autonomy_policy": {}
        }),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(stale_main.code, "version_conflict");

    let project: ProjectResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({"name": "atomic agent project"}),
        StatusCode::OK,
    )
    .await;
    let setup: ProjectAgentBindingResponse = common::json_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{}/project-agent", project.id),
        &token,
        json!(null),
        StatusCode::OK,
    )
    .await;
    assert_eq!(setup.state, api_types::AgentBindingState::SetupRequired);
    assert_eq!(setup.identity_id, None);

    let _active: ProjectAgentBindingResponse = common::json_request_with_bearer(
        &harness.app,
        Method::PUT,
        &format!("/api/v1/projects/{}/project-agent", project.id),
        &token,
        json!({
            "identity_id": connected.agent.id,
            "profile_id": connected.profile.id,
            "expected_version": setup.version,
            "permission_ceiling": {},
            "autonomy_policy": {},
            "subscriptions": [],
            "wake_budget": 0
        }),
        StatusCode::OK,
    )
    .await;
    let stale_project: ErrorResponse = common::json_request_with_bearer(
        &harness.app,
        Method::PUT,
        &format!("/api/v1/projects/{}/project-agent", project.id),
        &token,
        json!({
            "identity_id": connected.agent.id,
            "profile_id": connected.profile.id,
            "expected_version": setup.version,
            "permission_ceiling": {},
            "autonomy_policy": {},
            "subscriptions": [],
            "wake_budget": 0
        }),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(stale_project.code, "version_conflict");

    let event_types: Vec<String> = sqlx::query_scalar(
        "SELECT event_type FROM domain_event
         WHERE entity_id IN (?, ?, ?) ORDER BY sequence ASC",
    )
    .bind(&project.id)
    .bind(&setup.id)
    .bind(&setup.chat_id)
    .fetch_all(harness.state.db.pool())
    .await
    .expect("project creation events are queryable");
    assert_eq!(event_types.len(), 3);
    assert!(event_types.iter().any(|value| value == "project.created"));
    assert!(event_types
        .iter()
        .any(|value| value == "project_agent_binding.created"));
    assert!(event_types
        .iter()
        .any(|value| value == "agent_chat.created"));
}

#[tokio::test]
async fn agent_chat_turn_cancel_is_versioned_idempotent_and_cursor_bounded() {
    let workspace = common::TestDir::new("agent-chat-turn-cancel");
    let harness = common::test_app(workspace.path(), "agent-chat-turn-cancel").await;
    let token = common::test_jwt();
    let connected: ConnectedEmbeddedAgentResponse = common::connect_embedded_agent(
        &harness.app,
        &token,
        "turn-cancel-agent",
        "turn-cancel",
        "turn-cancel-secret",
        json!({"permissions": ["read_agent_chat", "propose_message"]}),
        json!({"allowed": ["read_agent_chat", "propose_message"]}),
    )
    .await;
    let binding: MainAgentBindingResponse = common::json_request_with_bearer(
        &harness.app,
        Method::PUT,
        "/api/v1/account/main-agent",
        &token,
        json!({
            "identity_id": connected.agent.id,
            "profile_id": connected.profile.id,
            "expected_version": 0,
            "autonomy_policy": {}
        }),
        StatusCode::OK,
    )
    .await;

    let first: SendAgentChatMessageResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &format!("/api/v1/agent-chats/{}/messages", binding.chat_id),
        &token,
        json!({"content": "first turn", "dedupe_key": "cancel-first"}),
        StatusCode::CREATED,
    )
    .await;
    let second: SendAgentChatMessageResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &format!("/api/v1/agent-chats/{}/messages", binding.chat_id),
        &token,
        json!({"content": "second turn", "dedupe_key": "cancel-second"}),
        StatusCode::CREATED,
    )
    .await;
    let first_turn = first.turn_job.expect("first turn is admitted");
    assert_eq!(first_turn.version, 1);

    let page: AgentChatMessageListResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/agent-chats/{}/messages?limit=1", binding.chat_id),
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(page.items.len(), 1);
    assert!(page.has_more);
    let before: AgentChatMessageListResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!(
            "/api/v1/agent-chats/{}/messages?before_sequence={}",
            binding.chat_id, second.message.sequence
        ),
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(before.items.len(), 1);
    assert_eq!(before.items[0].id, first.message.id);

    let cancelled: AgentChatTurnJobResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &format!(
            "/api/v1/agent-chats/{}/turns/{}/cancel",
            binding.chat_id, first_turn.id
        ),
        &token,
        json!({
            "expected_version": first_turn.version,
            "idempotency_key": "cancel-first-request"
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(cancelled.status, AgentChatTurnStatus::Cancelled);
    assert_eq!(cancelled.version, first_turn.version + 1);

    let replay: AgentChatTurnJobResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &format!(
            "/api/v1/agent-chats/{}/turns/{}/cancel",
            binding.chat_id, first_turn.id
        ),
        &token,
        json!({
            "expected_version": first_turn.version,
            "idempotency_key": "cancel-first-request"
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(replay.status, AgentChatTurnStatus::Cancelled);
    assert_eq!(replay.version, cancelled.version);

    let stale: ErrorResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &format!(
            "/api/v1/agent-chats/{}/turns/{}/cancel",
            binding.chat_id, first_turn.id
        ),
        &token,
        json!({
            "expected_version": first_turn.version,
            "idempotency_key": "cancel-second-request"
        }),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(stale.code, "version_conflict");

    let event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event
         WHERE event_type = 'agent_chat.turn.cancelled' AND entity_id = ?",
    )
    .bind(&first_turn.id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("cancellation event is durable");
    assert_eq!(event_count, 1);
}

#[tokio::test]
async fn agent_chat_turn_parks_awaiting_input_and_can_be_cancelled() {
    let workspace = common::TestDir::new("agent-chat-awaiting-input");
    let harness = common::test_app(workspace.path(), "agent-chat-awaiting-input").await;
    let token = common::test_jwt();

    let connected: ConnectedEmbeddedAgentResponse = common::connect_embedded_agent(
        &harness.app,
        &token,
        "main-awaiting-agent",
        "main-awaiting",
        "main-awaiting-secret",
        json!({"permissions": ["read_agent_chat", "propose_message"]}),
        json!({"allowed": ["read_agent_chat", "propose_message"]}),
    )
    .await;

    let binding: MainAgentBindingResponse = common::json_request_with_bearer(
        &harness.app,
        Method::PUT,
        "/api/v1/account/main-agent",
        &token,
        json!({
            "identity_id": connected.agent.id,
            "profile_id": connected.profile.id,
            "expected_version": 0,
            "autonomy_policy": {}
        }),
        StatusCode::OK,
    )
    .await;

    let send_response: SendAgentChatMessageResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &format!("/api/v1/agent-chats/{}/messages", binding.chat_id),
        &token,
        json!({
            "content": "A message that will park awaiting input"
        }),
        StatusCode::CREATED,
    )
    .await;
    let turn_job = send_response.turn_job.expect("admitted turn job");

    // Since turn_job was queued and not leased with 'test-lease-owner', update to leased first
    let _leased = db::AgentChatTurnJobRepo::update_agent_chat_turn_job(
        &*harness.state.db,
        db::UpdateAgentChatTurnJob {
            id: turn_job.id.clone(),
            expected_version: turn_job.version,
            status: db::AgentChatTurnState::Leased,
            pending_interaction_id: None,
            lease_owner: Some(Some("test-lease-owner".to_owned())),
            leased_until: Some(Some(db::now_rfc3339())),
            attempt_count: Some(1),
            next_attempt_at: None,
            response_message_id: None,
            error_code: None,
            error_message: None,
            updated_at: db::now_rfc3339(),
        },
    )
    .await
    .expect("lease turn");

    // Create an embedded agent session and protected interaction
    let session_row: (String,) =
        sqlx::query_as("SELECT id FROM agent_session WHERE identity_id = ? LIMIT 1")
            .bind(&connected.agent.id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("session exists");

    sqlx::query(
        "INSERT INTO protected_interaction (
            id, session_id, interaction_kind, prompt_redacted, status, version, created_at, updated_at
         ) VALUES (?, ?, 'questionnaire', 'questionnaire (1 question)', 'pending', 1, ?, ?)",
    )
    .bind("test-interaction-1")
    .bind(&session_row.0)
    .bind(db::now_rfc3339())
    .bind(db::now_rfc3339())
    .execute(harness.state.db.pool())
    .await
    .expect("insert protected interaction");

    let parked = db::AgentChatTransactionRepo::park_agent_chat_turn(
        &*harness.state.db,
        db::ParkAgentChatTurn {
            turn_job_id: turn_job.id.clone(),
            expected_version: _leased.version,
            lease_owner: "test-lease-owner".to_owned(),
            pending_interaction_id: "test-interaction-1".to_owned(),
            updated_at: db::now_rfc3339(),
        },
    )
    .await
    .expect("park turn");
    assert_eq!(parked.status, db::AgentChatTurnState::AwaitingInput);
    assert_eq!(
        parked.pending_interaction_id.as_deref(),
        Some("test-interaction-1")
    );
    assert_eq!(parked.attempt_count, 0); // attempt_count decremented so no burn

    // Read via API
    let turns: Vec<AgentChatTurnJobResponse> = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/agent-chats/{}/turns", binding.chat_id),
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].status, AgentChatTurnStatus::AwaitingInput);
    assert_eq!(
        turns[0].pending_interaction_id.as_deref(),
        Some("test-interaction-1")
    );

    // Cancel while parked in awaiting_input
    let cancelled: AgentChatTurnJobResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &format!(
            "/api/v1/agent-chats/{}/turns/{}/cancel",
            binding.chat_id, parked.id
        ),
        &token,
        json!({
            "expected_version": parked.version,
            "idempotency_key": "cancel-parked-turn"
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(cancelled.status, AgentChatTurnStatus::Cancelled);
}
