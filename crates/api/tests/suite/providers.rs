use crate::common;

use api_types::CliRuntimeEntryResponse;
use axum::http::{Method, StatusCode};
use serde_json::{json, Value};

async fn runtime_harness(root: &std::path::Path) -> common::Harness {
    let harness = common::test_app(root, "cli-runtime-errors").await;
    harness.stop_step_worker();
    let now = db::now_rfc3339();
    sqlx::query(
        "INSERT INTO daemon (id, machine_id, hostname, os, arch, status,
             owner_id, detected_clis_json, created_at, updated_at)
         VALUES ('daemon', 'runtime-test', 'test', 'test', 'test', 'online',
             'test-user-id', ?, ?, ?)",
    )
    .bind(json!([{"kind": "codex", "availability": "authenticated"}]).to_string())
    .bind(&now)
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .unwrap();
    harness
}

const AVAILABILITY: &str = "/api/v1/providers/cli-runtimes/daemon/codex/availability";

async fn assert_artifact_error(response: axum::response::Response) {
    let body: Value = common::parse_response(response, StatusCode::INTERNAL_SERVER_ERROR).await;
    assert!(!body["request_id"].as_str().unwrap().is_empty());
    assert_eq!(
        body,
        json!({
            "code": "internal_error",
            "message": "Project artifact mutation failed",
            "details": null,
            "request_id": body["request_id"],
        })
    );
}

#[tokio::test]
async fn database_errors_preserve_project_artifact_body() {
    let root = tempfile::tempdir().unwrap();
    let harness = runtime_harness(root.path()).await;
    sqlx::query("DROP TABLE cli_runtime_policy")
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let response = common::raw_json_request(
        &harness.app,
        Method::PATCH,
        AVAILABILITY,
        json!({"version": 0, "enabled": false}),
    )
    .await;
    assert_artifact_error(response).await;
    let response = common::raw_empty_request(&harness.app, Method::GET, "/api/v1/providers").await;
    assert_artifact_error(response).await;
}

#[tokio::test]
async fn cli_runtime_insert_foreign_key_error_preserves_project_artifact_body() {
    let root = tempfile::tempdir().unwrap();
    let harness = runtime_harness(root.path()).await;
    // Delete the parent after the route's lookup, at the version-zero insert.
    sqlx::query(
        "CREATE TRIGGER delete_daemon_before_policy_insert
         BEFORE INSERT ON cli_runtime_policy BEGIN
             DELETE FROM daemon WHERE id = NEW.daemon_id;
         END",
    )
    .execute(harness.state.db.pool())
    .await
    .unwrap();
    let response = common::raw_json_request(
        &harness.app,
        Method::PATCH,
        AVAILABILITY,
        json!({"version": 0, "enabled": false}),
    )
    .await;
    assert_artifact_error(response).await;
}

#[tokio::test]
async fn cli_runtime_stale_version_returns_its_conflict_code_and_message() {
    let root = tempfile::tempdir().unwrap();
    let harness = runtime_harness(root.path()).await;
    let created: CliRuntimeEntryResponse = common::json_request(
        &harness.app,
        Method::PATCH,
        AVAILABILITY,
        json!({"version": 0, "enabled": false}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(created.policy_version, 1);
    assert!(!created.enabled);
    let updated: CliRuntimeEntryResponse = common::json_request(
        &harness.app,
        Method::PATCH,
        AVAILABILITY,
        json!({"version": created.policy_version, "enabled": true}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(updated.policy_version, 2);
    assert!(updated.enabled);
    let body: Value = common::json_request(
        &harness.app,
        Method::PATCH,
        AVAILABILITY,
        json!({"version": created.policy_version, "enabled": false}),
        StatusCode::CONFLICT,
    )
    .await;
    assert!(!body["request_id"].as_str().unwrap().is_empty());
    assert_eq!(
        body,
        json!({
            "code": "cli_runtime.version_conflict",
            "message": "CLI runtime availability changed before it could be updated",
            "details": null,
            "request_id": body["request_id"],
        })
    );
    let policies = harness
        .state
        .db
        .cli_runtime_policies_for_daemon("test-user-id", "daemon")
        .await
        .unwrap();
    assert_eq!(policies["codex"], (true, updated.policy_version));
}
