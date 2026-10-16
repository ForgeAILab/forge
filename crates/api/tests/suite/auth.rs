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

// Browser sessions keep the existing JSON token API and add two host-only cookies.
fn session_cookie_headers(response: &axum::response::Response) -> Vec<String> {
    response
        .headers()
        .get_all(header::SET_COOKIE)
        .iter()
        .map(|value| value.to_str().unwrap().to_owned())
        .collect()
}

fn assert_session_cookies(cookies: &[String], auth: &AuthResponse, secure: bool) {
    assert_eq!(cookies.len(), 2);
    for (name, token, path, max_age) in [
        ("forge_session", &auth.access_token, "/", auth.expires_in),
        (
            "forge_refresh",
            &auth.refresh_token,
            "/api/v1/auth",
            services::auth_service::REFRESH_TOKEN_EXPIRY_SECS,
        ),
    ] {
        let cookie = cookies
            .iter()
            .find(|cookie| cookie.starts_with(&format!("{name}=")))
            .unwrap();
        let attributes: Vec<_> = cookie.split("; ").collect();
        assert_eq!(attributes[0], format!("{name}={token}"));
        assert!(attributes.contains(&"HttpOnly"));
        assert!(attributes.contains(&"SameSite=Strict"));
        assert!(attributes.contains(&format!("Path={path}").as_str()));
        assert!(attributes.contains(&format!("Max-Age={max_age}").as_str()));
        assert_eq!(attributes.contains(&"Secure"), secure);
        assert!(!cookie.contains("Domain="), "session must be host-only");
    }
}

fn session_request(method: Method, path: &str, cookie: &str) -> axum::http::request::Builder {
    Request::builder()
        .method(method)
        .uri(path)
        .header(header::HOST, "127.0.0.1:8080")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::COOKIE, cookie)
}

#[tokio::test]
async fn cookie_session_register_login_refresh_cookie_attributes_and_scheme() {
    use axum::extract::ConnectInfo;
    use std::{net::SocketAddr, sync::Arc};

    let workspace = common::TestDir::new("cookie-scheme");
    let harness = common::test_app(workspace.path(), "cookie-scheme").await;
    // Forwarding is trusted only from loopback. A configured HTTPS public URL
    // also works for remote/container proxies, independent of their headers.
    for (index, public_url, peer, forwarded, secure) in [
        (0, None, "127.0.0.1:4000", None, false),
        (
            1,
            Some("https://forge.example.com/app"),
            "10.0.0.2:4000",
            None,
            true,
        ),
        (2, None, "127.0.0.1:4000", Some("https"), true),
        (3, None, "10.0.0.2:4000", Some("https"), false),
        (
            4,
            Some("https://forge.example.com"),
            "127.0.0.1:4000",
            Some("http"),
            true,
        ),
    ] {
        let mut state = (*harness.state).clone();
        let mut config = (*state.effective_config).clone();
        config.server.public_base_url = public_url.map(str::to_owned);
        state.effective_config = Arc::new(config);
        let app = api::api_router(state);
        let mut refresh_token = String::new();
        for action in ["register", "login", "refresh"] {
            let body = if action == "refresh" {
                json!({ "refresh_token": refresh_token })
            } else {
                json!({ "email": format!("cookie-{index}@example.com"), "password": "Password123!" })
            };
            let mut request = Request::builder()
                .method(Method::POST)
                .uri(format!("/api/v1/auth/{action}"))
                .extension(ConnectInfo(peer.parse::<SocketAddr>().unwrap()))
                .header(header::CONTENT_TYPE, "application/json");
            if let Some(proto) = forwarded {
                request = request.header("x-forwarded-proto", proto);
            }
            let response = app
                .clone()
                .oneshot(request.body(Body::from(body.to_string())).unwrap())
                .await
                .unwrap();
            let cookies = session_cookie_headers(&response);
            let status = if action == "register" {
                StatusCode::CREATED
            } else {
                StatusCode::OK
            };
            let auth: AuthResponse = parse_body(response, status).await;
            assert_session_cookies(&cookies, &auth, secure);
            refresh_token = auth.refresh_token;
        }
    }
}

#[tokio::test]
async fn cookie_session_get_and_events_accept_cookie_and_bearer_takes_precedence() {
    let workspace = common::TestDir::new("cookie-get");
    let harness = common::test_app(workspace.path(), "cookie-get").await;
    let app = &harness.app;
    let auth = register_and_login(app, "cookie-get@example.com", "Password123!").await;
    let cookie = format!(
        "other=value; forge_session={}; another=value",
        auth.access_token
    );
    for path in ["/api/v1/auth/me", "/api/v1/events"] {
        let response = app
            .clone()
            .oneshot(
                session_request(Method::GET, path, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
    }
    let response = app
        .clone()
        .oneshot(
            session_request(Method::GET, "/api/v1/auth/me", &cookie)
                .header(header::AUTHORIZATION, "Bearer invalid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let error: serde_json::Value = parse_body(response, StatusCode::UNAUTHORIZED).await;
    assert_eq!(error["code"], "invalid_token");

    let response = app
        .clone()
        .oneshot(
            session_request(Method::GET, "/api/v1/auth/me", "forge_session=invalid")
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", auth.access_token),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let user: UserResponse = parse_body(response, StatusCode::OK).await;
    assert_eq!(user.email, "cookie-get@example.com");
}

#[tokio::test]
async fn cookie_session_csrf_blocks_unsafe_methods_and_accepts_same_origin() {
    let workspace = common::TestDir::new("cookie-csrf");
    let harness = common::test_app(workspace.path(), "cookie-csrf").await;
    let app = &harness.app;
    let auth = register_and_login(app, "cookie-csrf@example.com", "Password123!").await;
    let cookie = format!("forge_session={}", auth.access_token);
    for method in [Method::POST, Method::PUT, Method::PATCH, Method::DELETE] {
        for (name, value) in [
            ("sec-fetch-site", "cross-site"),
            ("sec-fetch-site", "same-site"),
            ("origin", "https://foreign.example.com"),
            ("origin", "http://127.0.0.1:8081"),
            ("origin", "null"),
        ] {
            let response = app
                .clone()
                .oneshot(
                    session_request(method.clone(), "/api/v1/auth/tokens", &cookie)
                        .header(name, value)
                        .body(Body::from(r#"{"name":"csrf"}"#))
                        .unwrap(),
                )
                .await
                .unwrap();
            let error: serde_json::Value = parse_body(response, StatusCode::FORBIDDEN).await;
            assert_eq!(error["code"], "csrf_rejected");
        }
    }
    let response = app
        .clone()
        .oneshot(
            session_request(Method::POST, "/api/v1/auth/tokens", &cookie)
                .body(Body::from(r#"{"name":"csrf"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "missing origin evidence"
    );
    for (name, value) in [
        ("sec-fetch-site", "same-origin"),
        ("sec-fetch-site", "none"),
        ("origin", "http://127.0.0.1:8080"),
    ] {
        let response = app
            .clone()
            .oneshot(
                session_request(Method::POST, "/api/v1/auth/tokens", &cookie)
                    .header(name, value)
                    .body(Body::from(r#"{"name":"csrf"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED, "{name}: {value}");
    }
    // Sec-Fetch-Site takes precedence; a matching Origin cannot rescue cross-site.
    let response = app
        .clone()
        .oneshot(
            session_request(Method::POST, "/api/v1/auth/tokens", &cookie)
                .header("sec-fetch-site", "cross-site")
                .header(header::ORIGIN, "http://127.0.0.1:8080")
                .body(Body::from(r#"{"name":"csrf"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    // Explicit Bearer credentials are unaffected even when cookies also arrive.
    let response = app
        .clone()
        .oneshot(
            session_request(Method::POST, "/api/v1/auth/tokens", &cookie)
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", auth.access_token),
                )
                .header("sec-fetch-site", "cross-site")
                .header(header::ORIGIN, "https://foreign.example.com")
                .body(Body::from(r#"{"name":"bearer"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn cookie_session_csrf_uses_configured_public_origin() {
    use std::sync::Arc;
    let workspace = common::TestDir::new("cookie-origin");
    let harness = common::test_app(workspace.path(), "cookie-origin").await;
    let auth = register_and_login(&harness.app, "cookie-origin@example.com", "Password123!").await;
    let mut state = (*harness.state).clone();
    let mut config = (*state.effective_config).clone();
    config.server.public_base_url = Some("https://forge.example.com/app".to_owned());
    state.effective_config = Arc::new(config);
    let app = api::api_router(state);
    let cookie = format!("forge_session={}", auth.access_token);
    for (origin, expected) in [
        ("https://forge.example.com", StatusCode::CREATED),
        ("https://forge.example.com:443", StatusCode::CREATED),
        ("http://forge.example.com", StatusCode::FORBIDDEN),
        ("https://forge.example.com.evil.test", StatusCode::FORBIDDEN),
        ("http://127.0.0.1:8080", StatusCode::FORBIDDEN),
    ] {
        let response = app
            .clone()
            .oneshot(
                session_request(Method::POST, "/api/v1/auth/tokens", &cookie)
                    .header(header::ORIGIN, origin)
                    .body(Body::from(r#"{"name":"origin"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected, "{origin}");
    }
}

#[tokio::test]
async fn cookie_session_refresh_rotates_cookies_logout_clears_and_revokes() {
    let workspace = common::TestDir::new("cookie-refresh");
    let harness = common::test_app(workspace.path(), "cookie-refresh").await;
    let app = &harness.app;
    let auth = register_and_login(app, "cookie-refresh@example.com", "Password123!").await;
    let cookie = format!("forge_refresh={}", auth.refresh_token);
    // Refresh must use only the refresh cookie: the access cookie may be expired.
    let response = app
        .clone()
        .oneshot(
            session_request(Method::POST, "/api/v1/auth/refresh", &cookie)
                .header("sec-fetch-site", "same-origin")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    let cookies = session_cookie_headers(&response);
    let refreshed: AuthResponse = parse_body(response, StatusCode::OK).await;
    assert_session_cookies(&cookies, &refreshed, false);
    assert_ne!(auth.refresh_token, refreshed.refresh_token);
    assert_eq!(
        do_refresh(app, &auth.refresh_token).await.status(),
        StatusCode::UNAUTHORIZED
    );

    let cookie = format!(
        "forge_refresh={}; forge_session={}",
        refreshed.refresh_token, refreshed.access_token
    );
    let response = app
        .clone()
        .oneshot(
            session_request(Method::POST, "/api/v1/auth/logout", &cookie)
                .header("sec-fetch-site", "same-origin")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let cookies = session_cookie_headers(&response);
    assert_eq!(cookies.len(), 2);
    assert!(cookies
        .iter()
        .any(|cookie| cookie == "forge_session=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0"));
    assert!(cookies.iter().any(|cookie| cookie
        == "forge_refresh=; HttpOnly; SameSite=Strict; Path=/api/v1/auth; Max-Age=0"));
    assert_eq!(
        do_refresh(app, &refreshed.refresh_token).await.status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn cookie_session_refresh_logout_csrf_and_body_precedence() {
    let workspace = common::TestDir::new("cookie-refresh-csrf");
    let harness = common::test_app(workspace.path(), "cookie-refresh-csrf").await;
    let app = &harness.app;
    let auth = register_and_login(app, "cookie-refresh-csrf@example.com", "Password123!").await;
    let cookie = format!("forge_refresh={}", auth.refresh_token);
    for path in ["/api/v1/auth/refresh", "/api/v1/auth/logout"] {
        for (name, value) in [
            ("sec-fetch-site", "cross-site"),
            ("origin", "https://foreign.example.com"),
        ] {
            let response = app
                .clone()
                .oneshot(
                    session_request(Method::POST, path, &cookie)
                        .header(name, value)
                        .header(header::AUTHORIZATION, "Bearer invalid")
                        .body(Body::from("{}"))
                        .unwrap(),
                )
                .await
                .unwrap();
            let error: serde_json::Value = parse_body(response, StatusCode::FORBIDDEN).await;
            assert_eq!(error["code"], "csrf_rejected");
        }
    }
    let response = app
        .clone()
        .oneshot(
            session_request(
                Method::POST,
                "/api/v1/auth/refresh",
                "forge_refresh=invalid",
            )
            .header("sec-fetch-site", "cross-site")
            .body(Body::from(
                json!({ "refresh_token": auth.refresh_token }).to_string(),
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    let refreshed: AuthResponse = parse_body(response, StatusCode::OK).await;
    let response = app
        .clone()
        .oneshot(
            session_request(Method::POST, "/api/v1/auth/logout", "forge_refresh=invalid")
                .body(Body::from(
                    json!({ "refresh_token": refreshed.refresh_token }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(session_cookie_headers(&response).len(), 2);
}

#[tokio::test]
async fn cookie_session_invalid_and_expired_cookies_are_unauthorized() {
    use jsonwebtoken::{EncodingKey, Header};
    let workspace = common::TestDir::new("cookie-invalid");
    let harness = common::test_app(workspace.path(), "cookie-invalid").await;
    let expired = jsonwebtoken::encode(
        &Header::default(),
        &json!({
            "sub": "test-user-id", "email": "test@example.com", "iat": 1, "exp": 2
        }),
        &EncodingKey::from_secret(&api::state::test_jwt_secret()),
    )
    .unwrap();
    for (token, code) in [
        ("invalid", "invalid_token"),
        (expired.as_str(), "token_expired"),
    ] {
        let response = harness
            .app
            .clone()
            .oneshot(
                session_request(
                    Method::GET,
                    "/api/v1/auth/me",
                    &format!("forge_session={token}"),
                )
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        let error: serde_json::Value = parse_body(response, StatusCode::UNAUTHORIZED).await;
        assert_eq!(error["code"], code);
    }
    let response = harness
        .app
        .clone()
        .oneshot(
            session_request(
                Method::POST,
                "/api/v1/auth/refresh",
                "forge_refresh=invalid",
            )
            .header("sec-fetch-site", "same-origin")
            .body(Body::from("{}"))
            .unwrap(),
        )
        .await
        .unwrap();
    let error: serde_json::Value = parse_body(response, StatusCode::UNAUTHORIZED).await;
    assert_eq!(error["code"], "invalid_refresh_token");
}

#[tokio::test]
async fn cookie_session_empty_refresh_and_logout_bodies_use_cookies() {
    let workspace = common::TestDir::new("cookie-empty-body");
    let harness = common::test_app(workspace.path(), "cookie-empty-body").await;
    let app = &harness.app;
    register_and_login(app, "cookie-empty@example.com", "Password123!").await;
    // A cross-site form can POST an empty body while SameSite withholds the
    // cookies. It still must not receive Set-Cookie headers that log a user out.
    for path in ["/api/v1/auth/refresh", "/api/v1/auth/logout"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(path)
                    .header("sec-fetch-site", "cross-site")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(session_cookie_headers(&response).is_empty());
        let error: serde_json::Value = parse_body(response, StatusCode::FORBIDDEN).await;
        assert_eq!(error["code"], "csrf_rejected");
    }
    for content_type in [Some("application/json"), None] {
        let auth: AuthResponse = parse_body(
            login(app, "cookie-empty@example.com", "Password123!").await,
            StatusCode::OK,
        )
        .await;
        let cookie = format!("forge_refresh={}", auth.refresh_token);
        let malformed = app
            .clone()
            .oneshot(
                session_request(Method::POST, "/api/v1/auth/refresh", &cookie)
                    .header("sec-fetch-site", "same-origin")
                    .body(Body::from("{"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
        let mut refresh_token = auth.refresh_token;
        for action in ["refresh", "logout"] {
            let mut request = Request::builder()
                .method(Method::POST)
                .uri(format!("/api/v1/auth/{action}"))
                .header(header::COOKIE, format!("forge_refresh={refresh_token}"))
                .header("sec-fetch-site", "same-origin");
            if let Some(content_type) = content_type {
                request = request.header(header::CONTENT_TYPE, content_type);
            }
            let response = app
                .clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            let cookies = session_cookie_headers(&response);
            assert_eq!(cookies.len(), 2);
            if action == "refresh" {
                let auth: AuthResponse = parse_body(response, StatusCode::OK).await;
                assert_session_cookies(&cookies, &auth, false);
                refresh_token = auth.refresh_token;
            } else {
                assert_eq!(response.status(), StatusCode::OK);
                assert!(cookies.iter().all(|cookie| cookie.contains("Max-Age=0")));
            }
        }
        assert_eq!(
            do_refresh(app, &refresh_token).await.status(),
            StatusCode::UNAUTHORIZED
        );
    }
}

async fn me_email(app: &axum::Router, request: Request<Body>) -> Result<String, StatusCode> {
    let response = app.clone().oneshot(request).await.unwrap();
    if response.status() != StatusCode::OK {
        return Err(response.status());
    }
    let user: UserResponse = parse_body(response, StatusCode::OK).await;
    Ok(user.email)
}

#[tokio::test]
async fn cookie_session_explicit_credentials_beat_the_cookie() {
    let workspace = common::TestDir::new("cookie-precedence");
    let harness = common::test_app(workspace.path(), "cookie-precedence").await;
    let app = &harness.app;
    let a = register_and_login(app, "cookie-a@example.com", "Password123!").await;
    let b = register_and_login(app, "cookie-b@example.com", "Password123!").await;
    let cookie_a = format!("forge_session={}", a.access_token);

    // Cookie of user A, Bearer of user B: the request is user B.
    let request = session_request(Method::GET, "/api/v1/auth/me", &cookie_a)
        .header(header::AUTHORIZATION, format!("Bearer {}", b.access_token))
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        me_email(app, request).await,
        Ok("cookie-b@example.com".into())
    );

    // EventSource sends its explicit `?token=` and the ambient cookie together:
    // the query credential wins, whether the cookie is another user's or stale.
    for cookie in [cookie_a.as_str(), "forge_session=stale"] {
        let request = session_request(
            Method::GET,
            &format!("/api/v1/auth/me?token={}", b.access_token),
            cookie,
        )
        .body(Body::empty())
        .unwrap();
        assert_eq!(
            me_email(app, request).await,
            Ok("cookie-b@example.com".into()),
            "{cookie}"
        );
    }

    // An Authorization header that is not a usable Bearer token never falls
    // back to the cookie.
    for authorization in ["bearer invalid", "Basic Zm9yZ2U6Zm9yZ2U="] {
        let request = session_request(Method::GET, "/api/v1/auth/me", &cookie_a)
            .header(header::AUTHORIZATION, authorization)
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            me_email(app, request).await,
            Err(StatusCode::UNAUTHORIZED),
            "{authorization}"
        );
    }
}

#[tokio::test]
async fn cookie_session_refuses_cross_origin_fetch_metadata_on_every_method() {
    let workspace = common::TestDir::new("cookie-fetch-site");
    let harness = common::test_app(workspace.path(), "cookie-fetch-site").await;
    let app = &harness.app;
    let auth = register_and_login(app, "cookie-site@example.com", "Password123!").await;
    let cookie = format!("forge_session={}", auth.access_token);

    // SameSite=Strict still sends the cookie from another port on the same host
    // or from a sibling subdomain, e.g. as an <img> or no-cors read.
    for site in ["same-site", "cross-site"] {
        for path in ["/api/v1/auth/me", "/api/v1/events"] {
            let response = app
                .clone()
                .oneshot(
                    session_request(Method::GET, path, &cookie)
                        .header("sec-fetch-site", site)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let error: serde_json::Value = parse_body(response, StatusCode::FORBIDDEN).await;
            assert_eq!(error["code"], "csrf_rejected", "{site} {path}");
        }
    }
    // The app's own reads, direct navigation and clients without Fetch
    // Metadata keep working; explicit Bearer requests are unaffected.
    for site in [Some("same-origin"), Some("none"), None] {
        let mut request = session_request(Method::GET, "/api/v1/auth/me", &cookie);
        if let Some(site) = site {
            request = request.header("sec-fetch-site", site);
        }
        assert_eq!(
            me_email(app, request.body(Body::empty()).unwrap()).await,
            Ok("cookie-site@example.com".into()),
            "{site:?}"
        );
    }
    let request = session_request(Method::GET, "/api/v1/auth/me", &cookie)
        .header("sec-fetch-site", "same-site")
        .header(
            header::AUTHORIZATION,
            format!("Bearer {}", auth.access_token),
        )
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        me_email(app, request).await,
        Ok("cookie-site@example.com".into())
    );

    // MCP never reads the session cookie.
    let response = app
        .clone()
        .oneshot(
            session_request(Method::POST, "/mcp", &cookie)
                .header("sec-fetch-site", "same-origin")
                .body(Body::from(
                    json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}
