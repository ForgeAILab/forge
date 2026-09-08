#![allow(dead_code)]

mod common;

use api_types::{
    AccountUsageAnalyticsResponse, ErrorResponse, ProjectAnalyticsResponse, ProjectResponse,
};
use axum::http::{Method, StatusCode};
use serde_json::json;

fn jwt_for_user(user_id: &str, email: &str) -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time")
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
    .expect("encode test jwt")
}

#[tokio::test]
async fn analytics_routes_require_visibility_and_preserve_the_404_shape() {
    let workspace = common::TestDir::new("analytics-route-visibility");
    let harness = common::test_app(workspace.path(), "analytics-route-visibility").await;
    let owner_token = common::test_jwt();
    let project: ProjectResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        &owner_token,
        json!({"name": "Analytics visibility project"}),
        StatusCode::OK,
    )
    .await;

    let project_analytics: ProjectAnalyticsResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{}/analytics", project.id),
        &owner_token,
        StatusCode::OK,
    )
    .await;
    assert!(project_analytics.token_usage.by_surface.is_empty());

    let account: AccountUsageAnalyticsResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/analytics/usage",
        &owner_token,
        StatusCode::OK,
    )
    .await;
    assert!(account.token_usage.by_surface.is_empty());
    assert!(account.by_project.is_empty());

    let other_token = jwt_for_user("analytics-other-user", "analytics-other@example.com");
    let hidden_project: ErrorResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{}", project.id),
        &other_token,
        StatusCode::NOT_FOUND,
    )
    .await;
    let hidden_analytics: ErrorResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{}/analytics", project.id),
        &other_token,
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(hidden_analytics.code, hidden_project.code);
    assert_eq!(hidden_analytics.message, hidden_project.message);
    assert_eq!(hidden_analytics.details, hidden_project.details);
}

#[tokio::test]
async fn analytics_routes_validate_rfc3339_and_half_open_windows() {
    let workspace = common::TestDir::new("analytics-route-window");
    let harness = common::test_app(workspace.path(), "analytics-route-window").await;
    let token = common::test_jwt();
    let project: ProjectResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({"name": "Analytics window project"}),
        StatusCode::OK,
    )
    .await;

    let encoded_window =
        "?from=2026-09-08T00%3A00%3A00%2B05%3A30&to=2026-09-09T00%3A00%3A00%2B05%3A30";
    let response: ProjectAnalyticsResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{}/analytics{encoded_window}", project.id),
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        response.window.from.as_deref(),
        Some("2026-09-08T00:00:00+05:30")
    );
    assert_eq!(
        response.window.to.as_deref(),
        Some("2026-09-09T00:00:00+05:30")
    );

    let invalid: ErrorResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{}/analytics?from=not-rfc3339", project.id),
        &token,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(invalid.code, "bad_request");

    let reversed: ErrorResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/analytics/usage?from=2026-09-09T00%3A00%3A00Z&to=2026-09-08T00%3A00%3A00Z",
        &token,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(reversed.code, "bad_request");
}
