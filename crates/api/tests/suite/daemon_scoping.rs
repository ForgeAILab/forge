#![allow(dead_code)]
use api_types::{
    AuthResponse, DaemonRegisterResponse, DaemonResponse, PaginatedResponse, TokenResponse,
};
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use serde::de::DeserializeOwned;
use serde_json::json;
use tower::ServiceExt;

use crate::common;

async fn auth_header_value(app: &Router, email: &str) -> (String, AuthResponse) {
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
                        "password": "password123"
                    }))
                    .unwrap(),
                ))
                .expect("build request"),
        )
        .await
        .expect("router response");
    let auth: AuthResponse = parse_response(response, StatusCode::CREATED).await;
    (format!("Bearer {}", auth.access_token), auth)
}

async fn register_daemon(app: &Router, machine_id: &str) -> DaemonRegisterResponse {
    register_daemon_with_auth(app, machine_id, None).await
}

async fn register_daemon_with_auth(
    app: &Router,
    machine_id: &str,
    auth_header: Option<&str>,
) -> DaemonRegisterResponse {
    let mut builder = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/daemons/register")
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(auth_header) = auth_header {
        builder = builder.header(header::AUTHORIZATION, auth_header);
    }

    let response = app
        .clone()
        .oneshot(
            builder
                .body(Body::from(
                    serde_json::to_string(&json!({
                        "machine_id": machine_id,
                        "hostname": "test-host",
                        "os": "linux",
                        "arch": "x86_64"
                    }))
                    .unwrap(),
                ))
                .expect("build request"),
        )
        .await
        .expect("router response");
    parse_response(response, StatusCode::OK).await
}

async fn create_pat(app: &Router, auth_header: &str, name: &str) -> TokenResponse {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/auth/tokens")
                .header(header::AUTHORIZATION, auth_header)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_string(&json!({ "name": name })).unwrap(),
                ))
                .expect("build request"),
        )
        .await
        .expect("router response");
    parse_response(response, StatusCode::CREATED).await
}

async fn parse_response<T: DeserializeOwned>(
    response: axum::response::Response,
    expected_status: StatusCode,
) -> T {
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    assert_eq!(
        status,
        expected_status,
        "unexpected status: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).expect("parse JSON")
}

#[tokio::test]
async fn daemon_register_with_user_token_claims_daemon() {
    let workspace = common::TestDir::new("daemon-register-user-token");
    let harness = common::test_app(workspace.path(), "daemon-register-user-token").await;

    let (auth_val, _auth) = auth_header_value(&harness.app, "daemon-owner@test.com").await;
    let token = create_pat(&harness.app, &auth_val, "Daemon link test").await;
    let raw_token = token.token.expect("raw token returned on create");
    let bearer = format!("Bearer {raw_token}");

    let reg = register_daemon_with_auth(&harness.app, "machine-account-owned", Some(&bearer)).await;

    let (expected_user_id,): (String,) = sqlx::query_as("SELECT id FROM user WHERE email = ?")
        .bind("daemon-owner@test.com")
        .fetch_one(harness.state.db.pool())
        .await
        .expect("query user");
    let (owner_id, visibility): (Option<String>, String) =
        sqlx::query_as("SELECT owner_id, visibility FROM daemon WHERE id = ?")
            .bind(&reg.daemon_id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("query daemon");

    assert_eq!(owner_id.as_deref(), Some(expected_user_id.as_str()));
    assert_eq!(visibility, "account");
}

#[tokio::test]
async fn daemon_register_with_invalid_user_token_returns_401() {
    let workspace = common::TestDir::new("daemon-register-invalid-token");
    let harness = common::test_app(workspace.path(), "daemon-register-invalid-token").await;

    let response = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/daemons/register")
                .header(header::AUTHORIZATION, "Bearer fg_invalid")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_string(&json!({
                        "machine_id": "machine-invalid-owner",
                        "hostname": "test-host",
                        "os": "linux",
                        "arch": "x86_64"
                    }))
                    .unwrap(),
                ))
                .expect("build request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn daemon_report_provisions_owned_agents_for_authenticated_clis() {
    let workspace = common::TestDir::new("daemon-report-agent-provision");
    let harness = common::test_app(workspace.path(), "daemon-report-agent-provision").await;

    let (auth_val, _auth) = auth_header_value(&harness.app, "daemon-agent-owner@test.com").await;
    let token = create_pat(&harness.app, &auth_val, "Daemon link agent test").await;
    let raw_token = token.token.expect("raw token returned on create");
    let bearer = format!("Bearer {raw_token}");
    let reg =
        register_daemon_with_auth(&harness.app, "machine-agent-provision", Some(&bearer)).await;

    for _ in 0..2 {
        let response = harness
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!("/api/v1/daemons/{}/report", reg.daemon_id))
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", reg.registration_token),
                    )
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_string(&json!({
                            "detected_clis": [
                                {
                                    "kind": "shell",
                                    "availability": "authenticated",
                                    "config_path": null,
                                    "version": "1.0",
                                    "path": "/bin/sh"
                                },
                                {
                                    "kind": "codex",
                                    "availability": "installed",
                                    "config_path": null,
                                    "version": null,
                                    "path": "/usr/bin/codex"
                                }
                            ]
                        }))
                        .unwrap(),
                    ))
                    .expect("build request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    let (expected_user_id,): (String,) = sqlx::query_as("SELECT id FROM user WHERE email = ?")
        .bind("daemon-agent-owner@test.com")
        .fetch_one(harness.state.db.pool())
        .await
        .expect("query user");
    let shell_agents: Vec<(String, String, Option<String>, String)> = sqlx::query_as(
        "SELECT id, executor_type, owner_id, visibility FROM agent_current WHERE daemon_id = ? ORDER BY executor_type",
    )
    .bind(&reg.daemon_id)
    .fetch_all(harness.state.db.pool())
    .await
    .expect("query agents");

    assert_eq!(
        shell_agents.len(),
        1,
        "reports should not duplicate daemon agents"
    );
    let (_id, executor_type, owner_id, visibility) = &shell_agents[0];
    assert_eq!(executor_type, "shell");
    assert_eq!(owner_id.as_deref(), Some(expected_user_id.as_str()));
    assert_eq!(visibility, "account");
}

#[tokio::test]
async fn daemon_list_returns_only_visible_daemons() {
    let workspace = common::TestDir::new("daemon-list-vis");
    let harness = common::test_app(workspace.path(), "daemon-list-vis").await;

    let (auth_val, _auth) = auth_header_value(&harness.app, "alice@test.com").await;

    let reg1 = register_daemon(&harness.app, "machine-global").await;

    let response = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/daemons?limit=100")
                .header(header::AUTHORIZATION, &auth_val)
                .body(Body::empty())
                .expect("build request"),
        )
        .await
        .expect("response");

    let page: PaginatedResponse<DaemonResponse> = parse_response(response, StatusCode::OK).await;
    assert!(!page.items.is_empty(), "should see global daemon");
    let found = page.items.iter().any(|d| d.id == reg1.daemon_id);
    assert!(found, "global daemon should be visible");
}

#[tokio::test]
async fn daemon_list_requires_admin() {
    let workspace = common::TestDir::new("daemon-list-admin");
    let harness = common::test_app(workspace.path(), "daemon-list-admin").await;

    let (_admin_auth_val, _admin) = auth_header_value(&harness.app, "admin-daemon@test.com").await;
    let (user_auth_val, _user) = auth_header_value(&harness.app, "user-daemon@test.com").await;

    let response = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/daemons?limit=100")
                .header(header::AUTHORIZATION, &user_auth_val)
                .body(Body::empty())
                .expect("build request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn mcp_request_without_token_returns_401() {
    let workspace = common::TestDir::new("mcp-no-auth");
    let harness = common::test_app(workspace.path(), "mcp-no-auth").await;

    let response = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/mcp")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_string(&json!({
                        "jsonrpc": "2.0",
                        "method": "initialize",
                        "id": 1
                    }))
                    .unwrap(),
                ))
                .expect("build request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn mcp_query_token_takes_precedence_over_oauth_bearer() {
    let workspace = common::TestDir::new("mcp-query-token");
    let harness = common::test_app(workspace.path(), "mcp-query-token").await;

    let (auth_val, _auth) = auth_header_value(&harness.app, "mcp-token@test.com").await;
    let response = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/auth/tokens")
                .header(header::AUTHORIZATION, &auth_val)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_string(&json!({ "name": "Forge MCP test" })).unwrap(),
                ))
                .expect("build request"),
        )
        .await
        .expect("router response");
    let token: TokenResponse = parse_response(response, StatusCode::CREATED).await;
    let raw = token.token.expect("raw token returned on create");

    let response = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/mcp?token={raw}"))
                .header(
                    header::AUTHORIZATION,
                    "Bearer oauth-token-that-forge-cannot-verify",
                )
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_string(&json!({
                        "jsonrpc": "2.0",
                        "method": "initialize",
                        "id": 1
                    }))
                    .unwrap(),
                ))
                .expect("build request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn cli_list_without_token_returns_401() {
    let workspace = common::TestDir::new("cli-no-auth");
    let harness = common::test_app(workspace.path(), "cli-no-auth").await;

    let response = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/clis")
                .body(Body::empty())
                .expect("build request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn daemon_report_with_valid_registration_token_succeeds() {
    let workspace = common::TestDir::new("daemon-report-valid");
    let harness = common::test_app(workspace.path(), "daemon-report-valid").await;

    let reg = register_daemon(&harness.app, "machine-report-valid").await;

    let response = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/api/v1/daemons/{}/report", reg.daemon_id))
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", reg.registration_token),
                )
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_string(&json!({
                        "detected_clis": [],
                    }))
                    .unwrap(),
                ))
                .expect("build request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn daemon_report_with_invalid_token_returns_401() {
    let workspace = common::TestDir::new("daemon-report-invalid");
    let harness = common::test_app(workspace.path(), "daemon-report-invalid").await;

    let reg = register_daemon(&harness.app, "machine-report-invalid").await;

    let response = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/api/v1/daemons/{}/report", reg.daemon_id))
                .header(header::AUTHORIZATION, "Bearer wrong-token-value")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_string(&json!({
                        "detected_clis": [],
                    }))
                    .unwrap(),
                ))
                .expect("build request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn bootstrap_assigns_orphaned_resources_to_first_user() {
    let workspace = common::TestDir::new("bootstrap-first");
    let harness = common::test_app(workspace.path(), "bootstrap-first").await;

    let reg = register_daemon(&harness.app, "machine-bootstrap").await;

    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO project (id, name, settings, workflow_definition, created_at, updated_at) VALUES ('bp1', 'bootstrap-proj', '{}', '{}', ?, ?)")
        .bind(&now).bind(&now)
        .execute(harness.state.db.pool())
        .await
        .expect("insert project");

    let _auth = auth_header_value(&harness.app, "admin@test.com").await;

    let (daemon_owner, daemon_visibility): (Option<String>, String) =
        sqlx::query_as("SELECT owner_id, visibility FROM daemon WHERE id = ?")
            .bind(&reg.daemon_id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("query");
    assert!(
        daemon_owner.is_some(),
        "daemon should be claimed by first user"
    );
    assert_eq!(
        daemon_visibility, "account",
        "claimed daemon should become account-scoped"
    );

    let (agent_owner, agent_visibility): (Option<String>, String) = sqlx::query_as(
        "SELECT owner_id, visibility FROM agent_current WHERE is_default = 1 LIMIT 1",
    )
    .fetch_one(harness.state.db.pool())
    .await
    .expect("query");
    assert!(
        agent_owner.is_some(),
        "default agents should be claimed by first user"
    );
    assert_eq!(
        agent_visibility, "account",
        "claimed default agents should become account-scoped"
    );

    let project_owner: Option<String> =
        sqlx::query_scalar("SELECT owner_id FROM project WHERE id = 'bp1'")
            .fetch_one(harness.state.db.pool())
            .await
            .expect("query");
    assert!(
        project_owner.is_some(),
        "project should be claimed by first user"
    );

    let member_count: i64 = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM project_member WHERE project_id = 'bp1'",
    )
    .fetch_one(harness.state.db.pool())
    .await
    .expect("query");
    assert_eq!(member_count, 1, "first user should be project owner member");
}
