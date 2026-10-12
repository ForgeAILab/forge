#![allow(dead_code)]
use crate::common;

use api_types::{AdminUserResponse, ErrorResponse, SettingResponse};
use axum::{
    body::Body,
    http::{header, Method, Request, StatusCode},
    Router,
};
use db::now_rfc3339;
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

const TEST_USER_ID: &str = "test-user-id";
const JWT_SECRET: &[u8] = b"test-jwt-secret-for-development";

#[tokio::test]
async fn non_admin_cannot_list_admin_users() {
    let harness = test_harness("admin-api-non-admin").await;
    let token = test_jwt(TEST_USER_ID, "test@example.com", false);

    let response =
        raw_empty_request_with_bearer(&harness.app, Method::GET, "/api/v1/admin/users", &token)
            .await;
    let error: ErrorResponse = common::parse_response(response, StatusCode::FORBIDDEN).await;

    assert_eq!(error.code, "admin_required");
}

#[tokio::test]
async fn admin_can_grant_admin_to_another_user() {
    let harness = admin_harness("admin-api-grant").await;
    let target = seed_user(
        &harness.state.db,
        "grant-target-user",
        "grant-target@example.com",
        false,
    )
    .await;
    let admin_token = test_jwt(TEST_USER_ID, "test@example.com", true);

    let updated: AdminUserResponse = common::json_request_with_bearer(
        &harness.app,
        Method::PATCH,
        &format!("/api/v1/admin/users/{}", target.id),
        &admin_token,
        json!({ "is_admin": true }),
        StatusCode::OK,
    )
    .await;

    assert_eq!(updated.id, target.id);
    assert!(updated.is_admin);

    let stored = db::UserRepo::get_user_by_id(&*harness.state.db, &target.id)
        .await
        .expect("target user query succeeds")
        .expect("target user exists");
    assert!(stored.is_admin);

    let target_admin_token = test_jwt(&target.id, &target.email, true);
    let _: Value = common::parse_response(
        raw_empty_request_with_bearer(
            &harness.app,
            Method::GET,
            "/api/v1/admin/users",
            &target_admin_token,
        )
        .await,
        StatusCode::OK,
    )
    .await;
}

#[tokio::test]
async fn last_admin_cannot_revoke_self() {
    let harness = admin_harness("admin-api-self-revoke").await;
    let admin_token = test_jwt(TEST_USER_ID, "test@example.com", true);

    let response = raw_json_request_with_bearer(
        &harness.app,
        Method::PATCH,
        &format!("/api/v1/admin/users/{TEST_USER_ID}"),
        &admin_token,
        json!({ "is_admin": false }),
    )
    .await;
    let error: ErrorResponse = common::parse_response(response, StatusCode::CONFLICT).await;

    assert_eq!(error.code, "last_admin");

    let stored = db::UserRepo::get_user_by_id(&*harness.state.db, TEST_USER_ID)
        .await
        .expect("admin user query succeeds")
        .expect("admin user exists");
    assert!(stored.is_admin);
}

#[tokio::test]
async fn last_admin_cannot_be_deleted() {
    let harness = admin_harness("admin-api-delete-last").await;
    let admin_token = test_jwt(TEST_USER_ID, "test@example.com", true);

    let response = raw_empty_request_with_bearer(
        &harness.app,
        Method::DELETE,
        &format!("/api/v1/admin/users/{TEST_USER_ID}"),
        &admin_token,
    )
    .await;
    let error: ErrorResponse = common::parse_response(response, StatusCode::CONFLICT).await;

    assert_eq!(error.code, "last_admin");

    let stored = db::UserRepo::get_user_by_id(&*harness.state.db, TEST_USER_ID)
        .await
        .expect("admin user query succeeds");
    assert!(stored.is_some());
}

#[tokio::test]
async fn admin_can_upsert_and_delete_setting_but_not_protected_setting() {
    let harness = admin_harness("admin-api-settings").await;
    let admin_token = test_jwt(TEST_USER_ID, "test@example.com", true);

    let created: SettingResponse = common::json_request_with_bearer(
        &harness.app,
        Method::PUT,
        "/api/v1/admin/settings/smtp_host",
        &admin_token,
        json!({ "value": "mail.example.com" }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(created.key, "smtp_host");
    assert_eq!(created.value, "mail.example.com");

    let persisted = db::SystemSettingRepo::get_setting(&*harness.state.db, "smtp_host")
        .await
        .expect("setting query succeeds");
    assert_eq!(persisted.as_deref(), Some("mail.example.com"));

    let delete_response = raw_empty_request_with_bearer(
        &harness.app,
        Method::DELETE,
        "/api/v1/admin/settings/smtp_host",
        &admin_token,
    )
    .await;
    assert_eq!(delete_response.status(), StatusCode::NO_CONTENT);

    let deleted = db::SystemSettingRepo::get_setting(&*harness.state.db, "smtp_host")
        .await
        .expect("setting query succeeds");
    assert!(deleted.is_none());

    let protected_put = raw_json_request_with_bearer(
        &harness.app,
        Method::PUT,
        "/api/v1/admin/settings/bootstrap_completed",
        &admin_token,
        json!({ "value": "false" }),
    )
    .await;
    let error: ErrorResponse = common::parse_response(protected_put, StatusCode::FORBIDDEN).await;
    assert_eq!(error.code, "protected_setting");

    let protected_delete = raw_empty_request_with_bearer(
        &harness.app,
        Method::DELETE,
        "/api/v1/admin/settings/bootstrap_completed",
        &admin_token,
    )
    .await;
    let error: ErrorResponse =
        common::parse_response(protected_delete, StatusCode::FORBIDDEN).await;
    assert_eq!(error.code, "protected_setting");
}

async fn test_harness(prefix: &str) -> common::Harness {
    let workspace_root = common::TestDir::new(&format!("{prefix}-ws"));
    common::test_app(workspace_root.path(), prefix).await
}

async fn admin_harness(prefix: &str) -> common::Harness {
    let harness = test_harness(prefix).await;
    db::UserRepo::set_admin(&*harness.state.db, TEST_USER_ID, true)
        .await
        .expect("seed user becomes admin");
    harness
}

async fn seed_user(db: &Arc<db::SqliteDb>, id: &str, email: &str, is_admin: bool) -> db::User {
    let now = now_rfc3339();
    let user = db::User {
        id: id.to_owned(),
        email: email.to_owned(),
        password_hash: "$2b$04$placeholder".to_owned(),
        display_name: None,
        is_admin,
        created_at: now.clone(),
        updated_at: now,
    };
    db::UserRepo::create_user(&**db, &user)
        .await
        .expect("seed user");
    user
}

fn test_jwt(user_id: &str, email: &str, is_admin: bool) -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header};

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time is after epoch")
        .as_secs();
    let claims = json!({
        "sub": user_id,
        "email": email,
        "is_admin": is_admin,
        "iat": now,
        "exp": now + 900,
    });

    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(JWT_SECRET),
    )
    .expect("encode test jwt")
}

async fn raw_json_request_with_bearer(
    app: &Router,
    method: Method,
    uri: &str,
    token: &str,
    body: Value,
) -> axum::response::Response {
    app.clone()
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
        .expect("router response")
}

async fn raw_empty_request_with_bearer(
    app: &Router,
    method: Method,
    uri: &str,
    token: &str,
) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("build authorized empty request"),
        )
        .await
        .expect("router response")
}
