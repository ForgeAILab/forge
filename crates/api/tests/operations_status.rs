#![allow(dead_code, clippy::assertions_on_constants)]
mod common;

use api_types::{OperatorSeverity, OperatorStatusResponse, ProjectResponse, RepoResponse};
use axum::http::{Method, StatusCode};
use db::now_rfc3339;
use serde_json::{json, Value};

#[tokio::test]
async fn operations_status_empty_db_is_healthy() {
    let workspace_root = common::TestDir::new("operations-status-empty");
    let harness = common::test_app(workspace_root.path(), "operations-status-empty").await;
    let admin_token = common::admin_jwt();

    let status: OperatorStatusResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/operations/status",
        &admin_token,
        StatusCode::OK,
    )
    .await;

    assert_eq!(status.overall_severity, OperatorSeverity::Healthy);
    assert!(status.active_executions.is_empty());
    assert!(status.blocked_tasks.is_empty());
    assert!(status.daemon_issues.is_empty());
    assert!(status.workspace_cleanup.is_empty());
    assert!(status.retry_pressure.is_empty());
    assert!(status.recent_errors.is_empty());
}

#[tokio::test]
async fn operations_status_reports_blocked_task_as_degraded() {
    let workspace_root = common::TestDir::new("operations-status-blocked");
    let harness = common::test_app(workspace_root.path(), "operations-status-blocked").await;
    let task_id = seed_blocked_task(&harness, "Blocked operations task").await;
    let admin_token = common::admin_jwt();

    let status: OperatorStatusResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/operations/status",
        &admin_token,
        StatusCode::OK,
    )
    .await;

    assert_eq!(status.overall_severity, OperatorSeverity::Blocked);
    assert_eq!(status.blocked_tasks.len(), 1);
    assert_eq!(status.blocked_tasks[0].task_id, task_id);
    assert_eq!(status.blocked_tasks[0].title, "Blocked operations task");
}

#[tokio::test]
async fn operations_status_response_has_expected_structure() {
    let workspace_root = common::TestDir::new("operations-status-structure");
    let harness = common::test_app(workspace_root.path(), "operations-status-structure").await;
    let admin_token = common::admin_jwt();

    let status: Value = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/operations/status",
        &admin_token,
        StatusCode::OK,
    )
    .await;

    assert!(status.get("overall_severity").is_some());
    assert!(status.get("computed_at").is_some());
    assert!(status.get("active_executions").is_some());
    assert!(status.get("blocked_tasks").is_some());
    assert!(status.get("daemon_issues").is_some());
    assert!(status.get("workspace_cleanup").is_some());
    assert!(status.get("retry_pressure").is_some());
    assert!(status.get("usage_summary").is_some());
    assert!(status.get("recent_errors").is_some());
}

#[tokio::test]
async fn operations_status_requires_admin() {
    let workspace_root = common::TestDir::new("operations-status-non-admin");
    let harness = common::test_app(workspace_root.path(), "operations-status-non-admin").await;

    let error: Value = common::empty_request(
        &harness.app,
        Method::GET,
        "/api/v1/operations/status",
        StatusCode::FORBIDDEN,
    )
    .await;

    assert_eq!(error["code"], "admin_required");
}

async fn seed_blocked_task(harness: &common::Harness, title: &str) -> String {
    let project: ProjectResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Operations status" }),
        StatusCode::OK,
    )
    .await;
    let _repo: RepoResponse = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{}/repos", project.id),
        json!({
            "name": "repo",
            "kind": "remote",
            "remote_url": "https://example.com/repo.git",
            "default_branch": "main"
        }),
        StatusCode::OK,
    )
    .await;

    let task_id = db::new_uuid_v4();
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO task (
            id, project_id, parent_task_id, subtask_order, title, description,
            status, priority, task_state_config, merge_config, plan, created_at, updated_at
         )
         VALUES (?, ?, NULL, NULL, ?, NULL, 'blocked', 0, NULL, NULL, NULL, ?, ?)",
    )
    .bind(&task_id)
    .bind(&project.id)
    .bind(title)
    .bind(&now)
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("blocked task inserts");

    task_id
}
