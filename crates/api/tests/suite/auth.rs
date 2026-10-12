#![allow(dead_code)]
use crate::common;

use api_types::{AuthResponse, UserResponse};
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
};
use serde::de::DeserializeOwned;
use serde_json::json;
use tower::ServiceExt;

// ── helpers ───────────────────────────────────────────────────────────────

async fn register(app: &axum::Router, email: &str, password: &str) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/auth/register")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_string(&json!({ "email": email, "password": password }))
                        .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn login(app: &axum::Router, email: &str, password: &str) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/auth/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_string(&json!({ "email": email, "password": password }))
                        .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn register_and_login(app: &axum::Router, email: &str, password: &str) -> AuthResponse {
    parse_body(register(app, email, password).await, StatusCode::CREATED).await
}

async fn parse_body<T: DeserializeOwned>(
    resp: axum::response::Response,
    expected: StatusCode,
) -> T {
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        status,
        expected,
        "unexpected status; body: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).expect("parse response body")
}

async fn do_refresh(app: &axum::Router, refresh_token: &str) -> axum::response::Response {
    let body = serde_json::to_string(&json!({ "refresh_token": refresh_token })).unwrap();
    app.clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/auth/refresh")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn do_logout(app: &axum::Router, refresh_token: &str) -> axum::response::Response {
    let body = serde_json::to_string(&json!({ "refresh_token": refresh_token })).unwrap();
    app.clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/auth/logout")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
}

// ── 4.7: full auth flow ───────────────────────────────────────────────────

#[tokio::test]
async fn auth_full_flow() {
    let workspace_root = common::TestDir::new("auth-flow-ws");
    let harness = common::test_app(workspace_root.path(), "auth-flow").await;
    let app = &harness.app;

    let email = "flow@example.com";
    let password = "Password123!";

    // Register
    let reg_resp = register(app, email, password).await;
    assert_eq!(reg_resp.status(), StatusCode::CREATED);

    // Login
    let auth: AuthResponse = parse_body(login(app, email, password).await, StatusCode::OK).await;
    let access = auth.access_token.clone();
    let refresh = auth.refresh_token.clone();

    // Access protected route
    let me_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/auth/me")
                .header(header::AUTHORIZATION, format!("Bearer {access}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let _me: UserResponse = parse_body(me_resp, StatusCode::OK).await;

    // Refresh — issues a new pair and invalidates the old token
    let refreshed: AuthResponse = parse_body(do_refresh(app, &refresh).await, StatusCode::OK).await;
    assert_ne!(refreshed.refresh_token, refresh, "new refresh token issued");

    // Old refresh token must now be rejected
    let old_refresh_resp = do_refresh(app, &refresh).await;
    assert_eq!(
        old_refresh_resp.status(),
        StatusCode::UNAUTHORIZED,
        "old refresh token must be invalid"
    );

    // New access token works
    let me_resp2 = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/auth/me")
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", refreshed.access_token),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(me_resp2.status(), StatusCode::OK);

    // Logout
    let logout_resp = do_logout(app, &refreshed.refresh_token).await;
    assert_eq!(logout_resp.status(), StatusCode::OK);

    // Refresh after logout must fail
    let post_logout = do_refresh(app, &refreshed.refresh_token).await;
    assert_eq!(
        post_logout.status(),
        StatusCode::UNAUTHORIZED,
        "refresh after logout must fail"
    );
}

// ── 4.8: SSE with ?token= query param ─────────────────────────────────────

#[tokio::test]
async fn sse_accepts_token_query_param() {
    let workspace_root = common::TestDir::new("auth-sse-ws");
    let harness = common::test_app(workspace_root.path(), "auth-sse").await;
    let app = &harness.app;

    let auth = register_and_login(app, "sse@example.com", "Password123!").await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/api/v1/events?token={}", auth.access_token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "SSE with ?token= must be accepted"
    );
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        ct.contains("text/event-stream"),
        "response must be SSE stream"
    );
}

#[tokio::test]
async fn sse_rejects_missing_token() {
    let workspace_root = common::TestDir::new("auth-sse-noauth-ws");
    let harness = common::test_app(workspace_root.path(), "auth-sse-noauth").await;

    let resp = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// ── 4.9: daemon routes exempt from user JWT ────────────────────────────────

#[tokio::test]
async fn daemon_register_requires_no_user_jwt() {
    let workspace_root = common::TestDir::new("auth-daemon-ws");
    let harness = common::test_app(workspace_root.path(), "auth-daemon").await;

    // Deliberately send NO Authorization header
    let resp = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/daemons/register")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_string(&json!({
                        "machine_id": "no-jwt-machine",
                        "hostname":   "host",
                        "os":         "linux",
                        "arch":       "x86_64",
                        "agent_version": "test"
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "daemon register must work without a user JWT"
    );
}

// ── 4.10: concurrent refresh — single winner ──────────────────────────────

#[tokio::test]
async fn concurrent_refresh_single_winner() {
    let workspace_root = common::TestDir::new("auth-race-ws");
    let harness = common::test_app(workspace_root.path(), "auth-race").await;
    let app = &harness.app;

    let auth = register_and_login(app, "race@example.com", "Password123!").await;
    let token = auth.refresh_token.clone();

    let body = serde_json::to_string(&json!({ "refresh_token": token })).unwrap();

    let app1 = app.clone();
    let app2 = app.clone();
    let body1 = body.clone();
    let body2 = body.clone();

    let make_req = |b: String| {
        Request::builder()
            .method(Method::POST)
            .uri("/api/v1/auth/refresh")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(b))
            .unwrap()
    };

    let (r1, r2) = tokio::join!(
        tokio::spawn(async move { app1.oneshot(make_req(body1)).await.unwrap() }),
        tokio::spawn(async move { app2.oneshot(make_req(body2)).await.unwrap() }),
    );
    let s1 = r1.unwrap().status();
    let s2 = r2.unwrap().status();

    let successes = [s1, s2].iter().filter(|&&s| s == StatusCode::OK).count();
    let failures = [s1, s2]
        .iter()
        .filter(|&&s| s == StatusCode::UNAUTHORIZED)
        .count();

    assert_eq!(successes, 1, "exactly one concurrent refresh must succeed");
    assert_eq!(failures, 1, "the other concurrent refresh must fail");
}
