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
    assert!(status["event_consumers"].is_array());
    assert!(status["database"]["incremental_vacuum"].is_boolean());
    assert!(status["database"]["free_pages"].is_number());
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

#[tokio::test]
async fn operations_status_reports_stalled_consumer_and_database_storage() {
    let workspace_root = common::TestDir::new("operations-status-outbox");
    let harness = common::test_app(workspace_root.path(), "operations-status-outbox").await;
    // This route harness has no supervisor; model a started durable coordination worker.
    harness
        .state
        .operator_status_service
        .set_runtime_workers(&[services::RuntimeWorker::Coordination]);
    let old = (chrono::Utc::now() - chrono::Duration::minutes(10)).to_rfc3339();
    let health = db::WorkerHealth::new(
        harness.state.db.clone(),
        services::coordination_consumer_name(),
    );
    let mut tx = db::begin_immediate(harness.state.db.pool()).await.unwrap();
    harness
        .state
        .db
        .initialize_event_worker_in_tx(
            &mut tx,
            &health,
            &db::EventSubscription::Exact(vec!["task.done".into()]),
        )
        .await
        .unwrap();
    sqlx::query("UPDATE worker_health SET created_at = ? WHERE worker_name = ?")
        .bind(&old)
        .bind(services::coordination_consumer_name())
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("UPDATE event_consumer_cursor SET updated_at = ? WHERE consumer_name = ?")
        .bind(&old)
        .bind(services::coordination_consumer_name())
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    sqlx::query("INSERT INTO domain_event (id, event_type, entity_type, entity_id, actor_type, scope_type, scope_id, correlation_id, created_at) VALUES ('pending', 'task.done', 'test', 'test', 'system', 'system', 'system', 'test', ?)").bind(old).execute(harness.state.db.pool()).await.unwrap();
    let status: OperatorStatusResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/operations/status",
        &common::admin_jwt(),
        StatusCode::OK,
    )
    .await;
    let consumer = status
        .event_consumers
        .iter()
        .find(|c| c.consumer_name == services::coordination_consumer_name())
        .unwrap();
    assert_eq!(consumer.lag, 1);
    assert!(consumer.stalled);
    assert!(consumer.oldest_unprocessed_age_seconds.unwrap() >= 600.0);
    assert_eq!(status.overall_severity, OperatorSeverity::Attention);
    assert!(status.recent_errors.iter().any(|alert| alert.entity_id
        == services::coordination_consumer_name()
        && alert.severity == OperatorSeverity::Attention));
    assert!(status.database.incremental_vacuum);
    assert!(status.database.free_pages >= 0);
}
