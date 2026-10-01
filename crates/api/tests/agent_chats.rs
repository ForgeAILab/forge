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

#[tokio::test]
async fn agent_chat_turn_logs_serve_the_turns_durable_activity() {
    let workspace = common::TestDir::new("agent-chat-turn-logs");
    let harness = common::test_app(workspace.path(), "agent-chat-turn-logs").await;
    // The harness has no `--data-dir`; keep this test's logs inside its own
    // workspace instead of the default `~/.forge`.
    harness
        .state
        .agent_chat_turn_logs
        .set_root(workspace.path().join("agent-chat-logs"));
    let token = common::test_jwt();

    let connected: ConnectedEmbeddedAgentResponse = common::connect_embedded_agent(
        &harness.app,
        &token,
        "main-logs-agent",
        "main-logs",
        "main-logs-secret",
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
        json!({"content": "What is the Agent doing?"}),
        StatusCode::CREATED,
    )
    .await;
    let turn = send_response.turn_job.expect("admitted turn job");
    let logs_uri = format!(
        "/api/v1/agent-chats/{}/turns/{}/logs",
        binding.chat_id, turn.id
    );

    // A queued turn has recorded nothing yet: an empty page, not an error.
    let empty: serde_json::Value = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &logs_uri,
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(empty["items"], json!([]));
    assert_eq!(empty["has_more"], false);

    // The worker writes through the same root the route reads.
    let sink = services::turn_log_sink::TurnLogSink::new(
        harness.state.agent_chat_turn_logs.path_for(&turn.id),
        &turn.id,
        None,
        None,
    );
    let argument_preview =
        forge_agent_host::build_tool_argument_preview(&json!({"operation": "read"}));
    forge_agent_host::TurnEventSink::tool_call_started(
        &sink,
        "call-1",
        "forge_scope_read",
        &["operation".to_owned()],
        &argument_preview,
    )
    .await;
    let mut summary = api_types::ToolResultSummary::unclassified(false, "call-1");
    summary.operation = Some("skill.section".to_owned());
    forge_agent_host::TurnEventSink::tool_call_finished(
        &sink,
        "call-1",
        "forge_scope_read",
        false,
        &summary,
    )
    .await;
    forge_agent_host::TurnEventSink::text_delta(&sink, "Reading the operating skill.").await;

    let page: serde_json::Value = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &logs_uri,
        &token,
        StatusCode::OK,
    )
    .await;
    let items = page["items"].as_array().expect("items array");
    assert_eq!(items.len(), 3);
    assert_eq!(items[0]["kind"], "tool_call");
    assert_eq!(items[0]["execution_id"], turn.id);
    assert_eq!(items[0]["payload"]["name"], "forge_scope_read");
    assert_eq!(items[0]["payload"]["input"], json!({"operation": "read"}));
    assert_eq!(items[1]["kind"], "tool_result");
    assert_eq!(items[1]["payload"]["summary"]["operation"], "skill.section");
    assert_eq!(items[2]["kind"], "assistant_delta");
    assert_eq!(page["has_more"], false);
    assert_eq!(page["next_sequence"], 3);

    // Keyset paging matches `/executions/{id}/logs`.
    let first: serde_json::Value = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("{logs_uri}?limit=1"),
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(first["items"].as_array().map(Vec::len), Some(1));
    assert_eq!(first["has_more"], true);
    assert_eq!(first["next_sequence"], 1);
    let rest: serde_json::Value = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("{logs_uri}?from_sequence=2"),
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(rest["items"].as_array().map(Vec::len), Some(1));
    assert_eq!(rest["items"][0]["sequence"], 2);

    // A turn id is only readable through its own chat, and only by its owner.
    let _missing: ErrorResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!(
            "/api/v1/agent-chats/{}/turns/not-a-turn/logs",
            binding.chat_id
        ),
        &token,
        StatusCode::NOT_FOUND,
    )
    .await;
    let response = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(&logs_uri)
                .body(Body::empty())
                .expect("build unauthenticated request"),
        )
        .await
        .expect("router response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn typed_turn_failure_response_and_redrive_require_version_and_replay_one_enqueue() {
    let workspace = common::TestDir::new("typed-turn-redrive");
    let harness = common::test_app(workspace.path(), "typed-turn-redrive").await;
    let token = common::test_jwt();
    let connected = common::connect_embedded_agent(
        &harness.app,
        &token,
        "typed-turn-agent",
        "typed-turn",
        "typed-turn-secret",
        json!({"permissions": ["read_agent_chat", "propose_message"]}),
        json!({"allowed": ["read_agent_chat", "propose_message"]}),
    )
    .await;
    let binding: MainAgentBindingResponse = common::json_request_with_bearer(&harness.app, Method::PUT, "/api/v1/account/main-agent", &token,
        json!({"identity_id": connected.agent.id, "profile_id": connected.profile.id, "expected_version": 0, "autonomy_policy": {}}), StatusCode::OK).await;
    let admitted: SendAgentChatMessageResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &format!("/api/v1/agent-chats/{}/messages", binding.chat_id),
        &token,
        json!({"content": "original request", "dedupe_key": "typed-redrive"}),
        StatusCode::CREATED,
    )
    .await;
    let turn = admitted.turn_job.unwrap();
    sqlx::query("UPDATE agent_chat_turn_job SET status = 'failed', attempt_count = 1, invocation_count = 1, failure_class_json = ?, retry_decision = 'fail', error_code = 'configuration_invalid', error_message = 'provider configuration is invalid', version = version + 1 WHERE id = ?")
        .bind(serde_json::to_string(&api_types::TurnFailure::Configuration).unwrap()).bind(&turn.id).execute(harness.state.db.pool()).await.unwrap();
    let listed: Vec<AgentChatTurnJobResponse> = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/agent-chats/{}/turns", binding.chat_id),
        &token,
        StatusCode::OK,
    )
    .await;
    let failed = &listed[0];
    assert_eq!(
        failed.failure_class,
        Some(api_types::TurnFailure::Configuration)
    );
    assert_eq!(
        failed.retry_decision,
        Some(api_types::TurnRetryDecision::Fail)
    );
    assert_eq!(
        failed.retry_action.as_ref().unwrap().expected_version,
        failed.version
    );
    let path = format!(
        "/api/v1/agent-chats/{}/turns/{}/retry",
        binding.chat_id, turn.id
    );
    let stale: ErrorResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &path,
        &token,
        json!({"expected_version": turn.version, "idempotency_key": "stale"}),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(stale.code, "version_conflict");
    let body = json!({"expected_version": failed.version, "idempotency_key": "manual-fix"});
    let queued: AgentChatTurnJobResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &path,
        &token,
        body.clone(),
        StatusCode::OK,
    )
    .await;
    let replay: AgentChatTurnJobResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &path,
        &token,
        body,
        StatusCode::OK,
    )
    .await;
    assert_eq!(queued, replay);
    assert_ne!(queued.id, turn.id);
    assert_eq!(queued.status, AgentChatTurnStatus::Queued);
    assert_eq!(queued.attempt_count, 0);
    assert!(queued.retry_action.is_none());
    let source = db::AgentChatTurnJobRepo::get_agent_chat_turn_job(&*harness.state.db, &turn.id)
        .await
        .unwrap()
        .unwrap();
    assert!(source.retry_action().is_none());
    let live_refusal: ErrorResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &path,
        &token,
        json!({"expected_version": source.version, "idempotency_key": "while-live"}),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(live_refusal.code, "turn_not_retryable");
    let child_path = format!(
        "/api/v1/agent-chats/{}/turns/{}/retry",
        binding.chat_id, queued.id
    );
    let not_retryable: ErrorResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &child_path,
        &token,
        json!({"expected_version": queued.version, "idempotency_key": "nonterminal"}),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(not_retryable.code, "turn_not_retryable");
    sqlx::query("UPDATE agent_chat_turn_job SET status = 'succeeded' WHERE id = ?")
        .bind(&queued.id)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let after_success: ErrorResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &path,
        &token,
        json!({"expected_version": source.version, "idempotency_key": "after-success"}),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(after_success.code, "turn_not_retryable");

    // A failed retry child is the sole eligible turn for this message.
    sqlx::query(
        "UPDATE agent_chat_turn_job SET status = 'failed', version = version + 1 WHERE id = ?",
    )
    .bind(&queued.id)
    .execute(harness.state.db.pool())
    .await
    .unwrap();
    let listed: Vec<AgentChatTurnJobResponse> = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/agent-chats/{}/turns", binding.chat_id),
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        listed
            .iter()
            .filter(|turn| turn.retry_action.is_some())
            .count(),
        1
    );
    assert!(listed
        .iter()
        .find(|turn| turn.id == queued.id)
        .unwrap()
        .retry_action
        .is_some());
    assert!(listed
        .iter()
        .find(|turn| turn.id == source.id)
        .unwrap()
        .retry_action
        .is_none());
    let child = db::AgentChatTurnJobRepo::get_agent_chat_turn_job(&*harness.state.db, &queued.id)
        .await
        .unwrap()
        .unwrap();
    // An older parked turn still holds the chat's single live slot.
    sqlx::query("UPDATE agent_chat_turn_job SET status = 'awaiting_input' WHERE id = ?")
        .bind(&source.id)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let parked_refusal: ErrorResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &child_path,
        &token,
        json!({"expected_version": child.version, "idempotency_key": "while-parked"}),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(parked_refusal.code, "another_turn_live");
    sqlx::query("UPDATE agent_chat_turn_job SET status = 'failed' WHERE id = ?")
        .bind(&source.id)
        .execute(harness.state.db.pool())
        .await
        .unwrap();

    // Cancellation of the newest turn also permits fresh admission.
    sqlx::query("UPDATE agent_chat_turn_job SET status = 'cancelled', failure_class_json = NULL, retry_decision = NULL, version = version + 1 WHERE id = ?")
        .bind(&child.id).execute(harness.state.db.pool()).await.unwrap();
    let cancelled =
        db::AgentChatTurnJobRepo::get_agent_chat_turn_job(&*harness.state.db, &child.id)
            .await
            .unwrap()
            .unwrap();
    let cancelled_retry: AgentChatTurnJobResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &child_path,
        &token,
        json!({"expected_version": cancelled.version, "idempotency_key": "cancelled-retry"}),
        StatusCode::OK,
    )
    .await;
    assert_ne!(cancelled_retry.id, cancelled.id);
    assert_eq!(
        cancelled_retry.input_message_id,
        cancelled.triggering_message_id
    );
    assert_eq!(cancelled_retry.status, AgentChatTurnStatus::Queued);
    // Historical codes remain visible and the newest historical failure is eligible.
    sqlx::query("UPDATE agent_chat_turn_job SET status = 'failed', failure_class_json = NULL, retry_decision = NULL, error_code = 'credential_unavailable', error_message = 'old failure', version = version + 1 WHERE id = ?")
        .bind(&cancelled_retry.id).execute(harness.state.db.pool()).await.unwrap();
    let listed: Vec<AgentChatTurnJobResponse> = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/agent-chats/{}/turns", binding.chat_id),
        &token,
        StatusCode::OK,
    )
    .await;
    let historical = listed
        .iter()
        .find(|turn| turn.id == cancelled_retry.id)
        .unwrap();
    assert_eq!(
        historical.error_code.as_deref(),
        Some("credential_unavailable")
    );
    assert_eq!(historical.error_message.as_deref(), Some("old failure"));
    assert!(historical.failure_class.is_none());
    assert!(historical.retry_action.is_some());
    let messages: AgentChatMessageListResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/agent-chats/{}/messages", binding.chat_id),
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(messages.items.len(), 1);
    assert_eq!(messages.items[0].id, admitted.message.id);

    // A topic divider has no turn, but still closes retries into the old topic.
    let _: api_types::StartAgentChatTopicResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &format!("/api/v1/agent-chats/{}/topics", binding.chat_id),
        &token,
        json!({"label": "new topic"}),
        StatusCode::OK,
    )
    .await;
    let source =
        db::AgentChatTurnJobRepo::get_agent_chat_turn_job(&*harness.state.db, &cancelled_retry.id)
            .await
            .unwrap()
            .unwrap();
    assert!(source.retry_action().is_none());
    let superseded: ErrorResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &format!(
            "/api/v1/agent-chats/{}/turns/{}/retry",
            binding.chat_id, source.id
        ),
        &token,
        json!({"expected_version": source.version, "idempotency_key": "too-late"}),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(superseded.code, "turn_not_retryable");
}
