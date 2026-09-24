#![allow(dead_code)]

mod common;

use api_types::ProviderEntriesResponse;
use axum::http::{Method, StatusCode};

#[tokio::test]
async fn provider_list_reports_live_backoff_without_exposing_error_detail() {
    let workspace = common::TestDir::new("provider-health-route");
    let harness = common::test_app(workspace.path(), "provider-health-route").await;
    let now = db::now_rfc3339();
    sqlx::query(
        "INSERT INTO credential_handle (id, owner_user_id, provider, label, created_at, updated_at)
         VALUES ('entry-health', 'test-user-id', 'openai', 'Test entry', ?, ?)",
    )
    .bind(&now)
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("entry");

    services::provider_health::record_entry_outcome(
        &harness.state.db,
        "entry-health",
        Err("provider returned HTTP 429; internal=do-not-show"),
    )
    .await
    .expect("health recorded");

    let response: ProviderEntriesResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/providers",
        &common::test_jwt(),
        StatusCode::OK,
    )
    .await;
    let entry = response
        .items
        .iter()
        .find(|entry| entry.id == "entry-health")
        .expect("listed entry");
    let health = entry.health.as_ref().expect("health visible");
    assert_eq!(health.status, "backoff");
    assert_eq!(health.consecutive_failures, 1);
    assert_eq!(
        health.last_error_message.as_deref(),
        Some("Provider returned HTTP 429")
    );
    assert!(health.backoff_until.is_some());
}
