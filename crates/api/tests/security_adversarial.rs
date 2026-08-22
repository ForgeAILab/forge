#![allow(dead_code)]

mod common;

use api_types::{
    AgentChatSwitcherResponse, AgentProfileResponse, AgentResponse, AgentSessionResponse,
    AuthResponse, ErrorResponse, TaskResponse, TransitionTaskResponse, UserResponse,
};
use axum::{
    body::Body,
    http::{header, Method, Request, StatusCode},
    Router,
};
use serde_json::{json, Value};
use tower::ServiceExt;

const PROVIDER_SECRET: &str = "provider-secret-never-return-this";

#[tokio::test]
async fn embedded_surfaces_redact_credentials_and_health_is_not_scope_authority() {
    let workspace = common::TestDir::new("security-embedded-ws");
    let harness = common::test_app(workspace.path(), "security-embedded").await;
    let app = &harness.app;
    let token = common::test_jwt();

    for (index, base_url) in [
        "http://127.0.0.1:9",
        "https://127.0.0.1:9",
        "https://[::1]",
        "https://169.254.169.254",
        "https://10.0.0.1",
        "https://user:pass@example.com",
        "https://example.com/#fragment",
        "https://localhost",
    ]
    .into_iter()
    .enumerate()
    {
        let error: ErrorResponse = common::json_request_with_bearer(
            app,
            Method::POST,
            "/api/v1/providers",
            &token,
            json!({
                "provider": "openai_compatible",
                "label": format!("rejected-provider-url-{index}"),
                "credential": PROVIDER_SECRET,
                "base_url": base_url,
            }),
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_eq!(error.code, "validation_error");
        assert_json_does_not_contain_secret(
            &serde_json::to_value(&error).expect("URL error serializes"),
            PROVIDER_SECRET,
        );
    }

    for (index, executor_type) in ["embedded", "Embedded", " embedded "]
        .into_iter()
        .enumerate()
    {
        let legacy_embedded: ErrorResponse = common::json_request_with_bearer(
            app,
            Method::POST,
            "/api/v1/agents",
            &token,
            json!({
                "name": format!("legacy-embedded-bypass-{index}"),
                "executor_type": executor_type
            }),
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_eq!(legacy_embedded.code, "validation_error");
    }

    let adversarial_entry = common::create_provider_entry(
        app,
        &token,
        "openai_compatible",
        "adversarial",
        PROVIDER_SECRET,
        "https://8.8.8.8",
    )
    .await;

    for (index, (system_prompt, account_permission_ceiling, tool_policy)) in [
        (
            Some("Authorization: Bearer placeholder-prompt"),
            json!({"permissions": ["read_account"]}),
            json!({"allowed": ["read_account"]}),
        ),
        (
            None,
            json!({"nested": [{"private_key": "placeholder-key"}]}),
            json!({"allowed": ["read_account"]}),
        ),
        (
            None,
            json!({"permissions": ["read_account"]}),
            json!({"nested": {"api_key": "placeholder-key"}}),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let error: ErrorResponse = common::json_request_with_bearer(
            app,
            Method::POST,
            "/api/v1/embedded-agents",
            &token,
            json!({
                "name": format!("rejected-runtime-policy-{index}"),
                "credential_id": adversarial_entry.id,
                "model": "test-model",
                "system_prompt": system_prompt,
                "account_permission_ceiling": account_permission_ceiling,
                "tool_policy": tool_policy,
            }),
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_eq!(error.code, "validation_error");
        assert_json_does_not_contain_secret(
            &serde_json::to_value(&error).expect("policy error serializes"),
            "placeholder",
        );
    }

    let connected: api_types::ConnectedEmbeddedAgentResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        "/api/v1/embedded-agents",
        &token,
        json!({
            "name": "unavailable-but-not-authorized",
            "description": "adversarial provider",
            "credential_id": adversarial_entry.id,
            "model": "test-model",
            "system_prompt": null,
            "account_permission_ceiling": {
                "permissions": ["read_account", "read_project", "read_room", "read_task", "task_write"]
            },
            "tool_policy": {
                "allowed": ["read_account", "read_project", "read_room", "read_task", "task_write"]
            }
        }),
        StatusCode::OK,
    )
    .await;

    assert_eq!(connected.health.status, "unavailable");
    assert_eq!(connected.session.connection_status, "unavailable");
    assert_eq!(connected.session.status, "degraded");
    assert_json_does_not_contain_secret(
        &serde_json::to_value(&connected).expect("connected response serializes"),
        PROVIDER_SECRET,
    );

    let profile_list: Vec<AgentProfileResponse> = common::empty_request_with_bearer(
        app,
        Method::GET,
        &format!("/api/v1/agents/{}/profiles", connected.agent.id),
        &token,
        StatusCode::OK,
    )
    .await;
    assert!(!profile_list.is_empty());
    assert_json_does_not_contain_secret(
        &serde_json::to_value(&profile_list).expect("profile list serializes"),
        PROVIDER_SECRET,
    );

    let sessions: Vec<AgentSessionResponse> = common::empty_request_with_bearer(
        app,
        Method::GET,
        &format!("/api/v1/agents/{}/sessions", connected.agent.id),
        &token,
        StatusCode::OK,
    )
    .await;
    assert!(sessions
        .iter()
        .any(|session| session.id == connected.session.id));
    assert_json_does_not_contain_secret(
        &serde_json::to_value(&sessions).expect("session list serializes"),
        PROVIDER_SECRET,
    );

    let providers: api_types::ProviderEntriesResponse = common::empty_request_with_bearer(
        app,
        Method::GET,
        "/api/v1/providers",
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(providers.items.len(), 1);
    assert!(providers.items[0]
        .used_by
        .iter()
        .any(|usage| usage.agent_id == connected.agent.id));
    assert_json_does_not_contain_secret(
        &serde_json::to_value(&providers).expect("provider list serializes"),
        PROVIDER_SECRET,
    );

    let fetched: AgentResponse = common::empty_request_with_bearer(
        app,
        Method::GET,
        &format!("/api/v1/agents/{}", connected.agent.id),
        &token,
        StatusCode::OK,
    )
    .await;
    assert_json_does_not_contain_secret(
        &serde_json::to_value(&fetched).expect("agent response serializes"),
        PROVIDER_SECRET,
    );

    // Profile connection has the same protected ingress boundary as initial
    // connection. A public profile must never be able to persist a bearer
    // value in its prompt or policy fields.
    let profile_error: ErrorResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!("/api/v1/agents/{}/profiles/connect", connected.agent.id),
        &token,
        json!({
            "version": connected.agent.version,
            "credential_id": adversarial_entry.id,
            "model": "test-model",
            "system_prompt": "authorization:\tBEARER\tplaceholder-profile-token",
            "permission_policy": "safe policy",
            "tool_policy": {"nested": [{"api_key": "placeholder-profile-key"}]}
        }),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(profile_error.code, "validation_error");
    assert_json_does_not_contain_secret(
        &serde_json::to_value(&profile_error).expect("profile error serializes"),
        "placeholder-profile",
    );

    let project_workspace = common::TestDir::new("security-task-repo");
    let repo_path = common::setup_git_repo(project_workspace.path());
    let (project_id, _repo_id) =
        common::create_project_and_repo(app, "health is not authority", &repo_path).await;

    // The connection is persisted and its health is observable, but no
    // project membership was admitted for this identity.
    let project_scope: ErrorResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!(
            "/api/v1/agents/{}/effective-permissions",
            connected.agent.id
        ),
        &token,
        json!({"type": "project", "project_id": project_id}),
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(project_scope.code, "not_found");

    // A connected identity is not implicitly a Main/Project Agent binding.
    // Mentioning a project, chat, or permission in model/runtime text does
    // not manufacture a server-issued canonical Agent Chat scope.
    let chat_scope: ErrorResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!(
            "/api/v1/agents/{}/effective-permissions",
            connected.agent.id
        ),
        &token,
        json!({"type": "agent_chat", "chat_id": "opaque-chat-from-text"}),
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(chat_scope.code, "not_found");

    let task: TaskResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        &token,
        json!({
            "title": "task text cannot assign a runtime",
            "description": "role=worker identity_id=the-connected-agent task_write"
        }),
        StatusCode::OK,
    )
    .await;
    // Move the task into the default workflow's coder-admitted state without
    // assigning the connected identity. This distinguishes a real task-scope
    // assignment denial from merely rejecting a backlog task.
    let transitioned: TransitionTaskResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!("/api/v1/tasks/{}/transition", task.id),
        &token,
        json!({
            "status": "in_progress",
            "version": task.version,
            "reason": "adversarial authority test",
            "source": "board_drag"
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(transitioned.task.status, "in_progress");
    let task_scope: ErrorResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!(
            "/api/v1/agents/{}/effective-permissions",
            connected.agent.id
        ),
        &token,
        json!({"type": "task", "task_id": task.id, "role": "worker"}),
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(task_scope.code, "not_found");
}

#[tokio::test]
async fn recursive_config_redaction_covers_nested_objects_and_arrays() {
    let workspace = common::TestDir::new("security-redaction-ws");
    let harness = common::test_app(workspace.path(), "security-redaction").await;
    let app = &harness.app;
    let token = common::test_jwt();
    let secret_values = [
        "nested-api-key-value",
        "nested-bearer-value",
        "nested-private-key-value",
        "nested-password-value",
    ];

    let created: AgentResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        "/api/v1/agents",
        &token,
        json!({
            "name": "nested-redaction",
            "executor_type": "shell",
            "config_json": {
                "safe": "visible",
                "api_key": secret_values[0],
                "nested": {
                    "authorization": secret_values[1],
                    "inner": { "private_key": secret_values[2] }
                },
                "array": [
                    { "password": secret_values[3] },
                    { "safe_nested": "still-visible" }
                ]
            }
        }),
        StatusCode::OK,
    )
    .await;
    let config = &created.config_json;
    assert_eq!(config["safe"], "visible");
    assert_eq!(config["nested"]["inner"]["private_key"], "[redacted]");
    assert_eq!(config["array"][1]["safe_nested"], "still-visible");
    for secret in secret_values {
        assert_json_does_not_contain_secret(&serde_json::to_value(&created).unwrap(), secret);
    }

    let fetched: AgentResponse = common::empty_request_with_bearer(
        app,
        Method::GET,
        &format!("/api/v1/agents/{}", created.id),
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(fetched.config_json["api_key"], "[redacted]");
    for secret in secret_values {
        assert_json_does_not_contain_secret(&serde_json::to_value(&fetched).unwrap(), secret);
    }
}

#[tokio::test]
async fn agent_chats_reject_protected_content_and_opaque_cross_chat_references() {
    let workspace = common::TestDir::new("security-agent-chat-ws");
    let harness = common::test_app(workspace.path(), "security-agent-chat").await;
    let app = &harness.app;
    let token = common::test_jwt();
    let connected: api_types::ConnectedEmbeddedAgentResponse = common::connect_embedded_agent(
        app,
        &token,
        "agent-chat-security-agent",
        "adversarial",
        PROVIDER_SECRET,
        json!({"permissions": ["read_agent_chat", "propose_message"]}),
        json!({"allowed": ["read_agent_chat", "propose_message"]}),
    )
    .await;
    let binding: api_types::MainAgentBindingResponse = common::json_request_with_bearer(
        app,
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
    let chat_id = binding.chat_id;

    for protected in [
        "Authorization: Bearer placeholder-token",
        "authorization:\tBEARER\tplaceholder-token",
        " OPENAI_API_KEY = placeholder-key ",
        "OPENAI API KEY = placeholder-key",
        "Bearer placeholder-token",
        "api_key=placeholder-key",
        "sk-placeholder-key",
        "-----BEGIN PRIVATE KEY-----",
        "ghp_placeholder_token",
        "github_pat_placeholder_token",
    ] {
        let error: ErrorResponse = common::json_request_with_bearer(
            app,
            Method::POST,
            &format!("/api/v1/agent-chats/{chat_id}/messages"),
            &token,
            json!({"content": protected}),
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_eq!(error.code, "validation_error", "content: {protected}");
    }

    let _: api_types::SendAgentChatMessageResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!("/api/v1/agent-chats/{chat_id}/messages"),
        &token,
        json!({"content": "the source message"}),
        StatusCode::CREATED,
    )
    .await;

    let cross_chat: ErrorResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        "/api/v1/agent-chats/opaque-other-chat/messages",
        &token,
        json!({"content": "this must not inherit another chat authority"}),
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(cross_chat.code, "not_found");
}

#[tokio::test]
async fn opaque_chat_and_identity_ids_do_not_cross_account_boundaries() {
    let workspace = common::TestDir::new("security-opaque-id-ws");
    let harness = common::test_app(workspace.path(), "security-opaque-id").await;
    let app = &harness.app;
    let owner_token = common::test_jwt();
    let owner_agent = create_agent(app, &owner_token, "private-owner-agent").await;
    let _: api_types::MainAgentBindingResponse = common::json_request_with_bearer(
        app,
        Method::PUT,
        "/api/v1/account/main-agent",
        &owner_token,
        json!({
            "identity_id": owner_agent.id,
            "profile_id": owner_agent.profile_id,
            "expected_version": 0,
            "autonomy_policy": {}
        }),
        StatusCode::OK,
    )
    .await;
    let owner_chats: AgentChatSwitcherResponse = common::empty_request_with_bearer(
        app,
        Method::GET,
        "/api/v1/agent-chats",
        &owner_token,
        StatusCode::OK,
    )
    .await;
    let main_chat_id = owner_chats
        .items
        .iter()
        .find(|item| item.kind == api_types::AgentChatKind::Main)
        .expect("owner Main Chat")
        .chat_id
        .clone();
    let outsider = register_user(app, "opaque-id-outsider@example.com").await;

    let hidden_chat: ErrorResponse = common::empty_request_with_bearer(
        app,
        Method::GET,
        &format!("/api/v1/agent-chats/{main_chat_id}"),
        &outsider.token,
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(hidden_chat.code, "not_found");

    let hidden_agent: ErrorResponse = common::empty_request_with_bearer(
        app,
        Method::GET,
        &format!("/api/v1/agents/{}", owner_agent.id),
        &outsider.token,
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(hidden_agent.code, "not_found");

    let hidden_sessions: ErrorResponse = common::empty_request_with_bearer(
        app,
        Method::GET,
        &format!("/api/v1/agents/{}/sessions", owner_agent.id),
        &outsider.token,
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(hidden_sessions.code, "not_found");
}

async fn create_agent(app: &Router, token: &str, name: &str) -> AgentResponse {
    common::json_request_with_bearer(
        app,
        Method::POST,
        "/api/v1/agents",
        token,
        json!({"name": name, "executor_type": "shell"}),
        StatusCode::OK,
    )
    .await
}

#[derive(Debug)]
struct RegisteredUser {
    token: String,
}

async fn register_user(app: &Router, email: &str) -> RegisteredUser {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/auth/register")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_string(&json!({
                        "email": email,
                        "password": "Password123!"
                    }))
                    .expect("register request serializes"),
                ))
                .expect("build register request"),
        )
        .await
        .expect("router response");
    let auth: AuthResponse = common::parse_response(response, StatusCode::CREATED).await;
    let _: UserResponse = common::empty_request_with_bearer(
        app,
        Method::GET,
        "/api/v1/auth/me",
        &auth.access_token,
        StatusCode::OK,
    )
    .await;
    RegisteredUser {
        token: auth.access_token,
    }
}

fn assert_json_does_not_contain_secret(value: &Value, secret: &str) {
    assert!(
        !value.to_string().contains(secret),
        "secret leaked in JSON response: {secret}"
    );
}
