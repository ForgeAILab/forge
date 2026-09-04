// MCP OAuth auth middleware integration tests.
mod common;

use api_types::{
    AuthResponse, McpAccessTokenClaims, OAuthApproveRequest, OAuthDecision, OAuthRegisterRequest,
    OAuthTokenRequest, ProjectResponse, TokenResponse, UserResponse,
};
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tower::ServiceExt;

const CODE_VERIFIER: &str = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
const CODE_CHALLENGE: &str = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";
const REDIRECT_URI: &str = "http://127.0.0.1/callback";

#[tokio::test]
async fn mcp_request_without_token_returns_401_with_oauth_challenge() {
    let workspace = common::TestDir::new("mcp-oauth-no-token");
    let harness = common::test_app(workspace.path(), "mcp-oauth-no-token").await;

    let response = mcp_post(&harness.app, "/mcp", None, tools_list_request()).await;

    assert_challenged_unauthorized(response, "missing_token").await;
}

#[tokio::test]
async fn mcp_request_with_invalid_oauth_token_returns_challenge() {
    let workspace = common::TestDir::new("mcp-oauth-invalid-token");
    let harness = common::test_app(workspace.path(), "mcp-oauth-invalid-token").await;

    let response = mcp_post(
        &harness.app,
        "/mcp",
        Some("not-a-real-jwt"),
        tools_list_request(),
    )
    .await;

    assert_challenged_unauthorized(response, "invalid_token").await;
}

#[tokio::test]
async fn mcp_request_with_valid_pat_processed() {
    let workspace = common::TestDir::new("mcp-oauth-pat");
    let harness = common::test_app(workspace.path(), "mcp-oauth-pat").await;

    let auth = register_user(&harness.app, "mcp-pat@example.com").await;
    let pat = create_pat(&harness.app, &auth.access_token, "mcp-pat").await;

    let response = mcp_post(&harness.app, "/mcp", Some(&pat), tools_list_request()).await;

    assert_tools_list_success(response).await;
}

#[tokio::test]
async fn mcp_request_with_valid_oauth_token_processed() {
    let workspace = common::TestDir::new("mcp-oauth-valid");
    let harness = common::test_app(workspace.path(), "mcp-oauth-valid").await;

    let auth = register_user(&harness.app, "mcp-oauth@example.com").await;
    let user = current_user(&harness.app, &auth.access_token).await;
    let access_token = mint_oauth_access_token(&harness, &user.id, &user.email).await;

    let response = mcp_post(
        &harness.app,
        "/mcp",
        Some(&access_token),
        tools_list_request(),
    )
    .await;

    assert_tools_list_success(response).await;
}

#[tokio::test]
async fn mcp_request_with_wrong_audience_oauth_token_rejected() {
    let workspace = common::TestDir::new("mcp-oauth-wrong-audience");
    let harness = common::test_app(workspace.path(), "mcp-oauth-wrong-audience").await;
    let now = unix_timestamp();
    let token = harness
        .state
        .auth_service
        .issue_mcp_token(McpAccessTokenClaims {
            sub: "test-user-id".to_owned(),
            email: "test@example.com".to_owned(),
            iat: now,
            exp: now + 3600,
            aud: "https://wrong.example.test/mcp".to_owned(),
            scope: "mcp".to_owned(),
            client_id: "wrong-audience-test".to_owned(),
            token_use: "mcp".to_owned(),
        })
        .expect("wrong-audience token signs");

    let response = mcp_post(&harness.app, "/mcp", Some(&token), tools_list_request()).await;

    assert_challenged_unauthorized(response, "invalid_token").await;
}

#[tokio::test]
async fn mcp_oauth_token_subject_is_used_for_membership() {
    let workspace = common::TestDir::new("mcp-oauth-membership");
    let harness = common::test_app(workspace.path(), "mcp-oauth-membership").await;

    let auth = register_user(&harness.app, "mcp-member-subject@example.com").await;
    let user = current_user(&harness.app, &auth.access_token).await;
    let access_token = mint_oauth_access_token(&harness, &user.id, &user.email).await;
    let project: ProjectResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "MCP OAuth private project" }),
        StatusCode::OK,
    )
    .await;

    let response = mcp_post(
        &harness.app,
        &format!("/mcp?project_id={}", project.id),
        Some(&access_token),
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "forge_list_tasks",
                "arguments": {}
            }
        }),
    )
    .await;

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "project membership failures must be JSON-RPC errors, not authentication failures"
    );
    let body = response_json(response).await;
    assert_eq!(body["result"]["isError"], true);
    // An inaccessible Project is indistinguishable from a missing one.
    assert_eq!(body["result"]["structuredContent"]["code"], "not_found");
    assert_eq!(body["result"]["structuredContent"]["status"], "failed");
    assert_eq!(
        body["result"]["structuredContent"]["operation"],
        "forge_list_tasks"
    );
    assert_eq!(
        body["result"]["structuredContent"]["scope"],
        json!({
            "scope_type": "project",
            "scope_id": project.id,
        })
    );
    assert_eq!(
        body["result"]["structuredContent"]["safe_message"],
        "the requested resource was not found"
    );
    assert!(!body.to_string().contains("project not accessible"));
}

#[tokio::test]
async fn mcp_query_token_takes_precedence_over_header() {
    let workspace = common::TestDir::new("mcp-oauth-query-token");
    let harness = common::test_app(workspace.path(), "mcp-oauth-query-token").await;

    let auth = register_user(&harness.app, "mcp-query-token@example.com").await;
    let pat = create_pat(&harness.app, &auth.access_token, "mcp-query-pat").await;

    let response = mcp_post(
        &harness.app,
        &format!("/mcp?token={pat}"),
        Some("not-a-real-token"),
        tools_list_request(),
    )
    .await;

    assert_tools_list_success(response).await;
}

async fn mcp_post(
    app: &Router,
    uri: &str,
    bearer: Option<&str>,
    body: Value,
) -> axum::response::Response {
    let mut builder = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(token) = bearer {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    app.clone()
        .oneshot(
            builder
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .expect("build MCP request"),
        )
        .await
        .expect("router response")
}

async fn register_user(app: &Router, email: &str) -> AuthResponse {
    json_request(
        app,
        Method::POST,
        "/api/v1/auth/register",
        json!({ "email": email, "password": "password123" }),
        StatusCode::CREATED,
    )
    .await
}

async fn current_user(app: &Router, access_token: &str) -> UserResponse {
    bearer_empty_request(
        app,
        Method::GET,
        "/api/v1/auth/me",
        access_token,
        StatusCode::OK,
    )
    .await
}

async fn create_pat(app: &Router, access_token: &str, name: &str) -> String {
    let token: TokenResponse = bearer_json_request(
        app,
        Method::POST,
        "/api/v1/auth/tokens",
        access_token,
        json!({ "name": name }),
        StatusCode::CREATED,
    )
    .await;
    token.token.expect("raw PAT returned on create")
}

async fn mint_oauth_access_token(harness: &common::Harness, user_id: &str, email: &str) -> String {
    let client = harness
        .state
        .oauth_service
        .register_public_client(OAuthRegisterRequest {
            redirect_uris: vec![REDIRECT_URI.to_owned()],
            client_name: Some("MCP OAuth test client".to_owned()),
            grant_types: None,
            response_types: None,
            token_endpoint_auth_method: None,
            scope: Some("mcp".to_owned()),
        })
        .await
        .expect("OAuth client registers");
    let redirect_to = harness
        .state
        .oauth_service
        .approve_or_deny(
            OAuthApproveRequest {
                response_type: "code".to_owned(),
                client_id: client.client_id.clone(),
                redirect_uri: REDIRECT_URI.to_owned(),
                resource: harness.state.effective_config.mcp_resource_url(),
                scope: "mcp".to_owned(),
                state: None,
                code_challenge: CODE_CHALLENGE.to_owned(),
                code_challenge_method: "S256".to_owned(),
                decision: OAuthDecision::Approve,
            },
            user_id,
            email,
        )
        .await
        .expect("OAuth authorization approves");
    let code = query_param(&redirect_to, "code").expect("redirect includes code");
    let token = harness
        .state
        .oauth_service
        .exchange_token(OAuthTokenRequest {
            grant_type: "authorization_code".to_owned(),
            code: Some(code),
            redirect_uri: Some(REDIRECT_URI.to_owned()),
            client_id: Some(client.client_id),
            code_verifier: Some(CODE_VERIFIER.to_owned()),
            resource: Some(harness.state.effective_config.mcp_resource_url()),
            refresh_token: None,
            scope: None,
        })
        .await
        .expect("OAuth token exchange succeeds");
    token.access_token
}

async fn json_request<T>(
    app: &Router,
    method: Method,
    uri: &str,
    body: Value,
    expected_status: StatusCode,
) -> T
where
    T: DeserializeOwned,
{
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .expect("build JSON request"),
        )
        .await
        .expect("router response");
    parse_response(response, expected_status).await
}

async fn bearer_json_request<T>(
    app: &Router,
    method: Method,
    uri: &str,
    token: &str,
    body: Value,
    expected_status: StatusCode,
) -> T
where
    T: DeserializeOwned,
{
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .expect("build authorized JSON request"),
        )
        .await
        .expect("router response");
    parse_response(response, expected_status).await
}

async fn bearer_empty_request<T>(
    app: &Router,
    method: Method,
    uri: &str,
    token: &str,
    expected_status: StatusCode,
) -> T
where
    T: DeserializeOwned,
{
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("build authorized empty request"),
        )
        .await
        .expect("router response");
    parse_response(response, expected_status).await
}

async fn parse_response<T>(response: axum::response::Response, expected_status: StatusCode) -> T
where
    T: DeserializeOwned,
{
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response body");
    assert_eq!(
        status,
        expected_status,
        "unexpected response status with body: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).expect("parse JSON response")
}

async fn assert_challenged_unauthorized(response: axum::response::Response, code: &str) {
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let challenge = response
        .headers()
        .get(header::WWW_AUTHENTICATE)
        .and_then(|value| value.to_str().ok())
        .expect("WWW-Authenticate challenge is present")
        .to_owned();
    assert!(challenge.starts_with("Bearer "));
    assert!(challenge.contains("resource_metadata="));
    assert!(challenge.contains("scope=\"mcp\""));

    let body = response_json(response).await;
    assert_eq!(body["code"].as_str(), Some(code));
}

async fn assert_tools_list_success(response: axum::response::Response) {
    let body: Value = parse_response(response, StatusCode::OK).await;
    assert!(
        body["result"]["tools"].as_array().is_some(),
        "tools/list response must include result.tools array: {body}"
    );
}

async fn response_json(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response body");
    serde_json::from_slice(&bytes).expect("parse JSON response")
}

fn tools_list_request() -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/list",
        "params": {}
    })
}

fn query_param(url: &str, name: &str) -> Option<String> {
    url::Url::parse(url)
        .ok()?
        .query_pairs()
        .find_map(|(key, value)| (key == name).then(|| value.into_owned()))
}

fn unix_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time after UNIX epoch")
        .as_secs()
}
