#![allow(dead_code, clippy::assertions_on_constants)]
mod common;

use std::sync::Arc;

use api::{build_router, AppState};
use api_types::{ProjectResponse, RepoResponse, TaskResponse, TransitionTaskResponse};
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tower::ServiceExt;

#[tokio::test]
async fn transition_log_records_review_retry_and_completion_history() {
    let harness = test_app().await;
    let (project_id, _repo_id) = create_project_and_repo(&harness.app).await;
    let mut task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Transition timeline" }),
        StatusCode::OK,
    )
    .await;
    task = set_task_status(&harness, &task.id, "in_progress").await;
    assign_human_reviewer(&harness.app, &task).await;

    task = transition(&harness.app, &task, "review", "ready for review").await;
    task = gate(&harness.app, &task, "review", "reject", "missing tests").await;
    task = transition(&harness.app, &task, "review", "retry ready").await;
    task = gate(&harness.app, &task, "review", "approve", "looks good").await;
    task = transition(&harness.app, &task, "done", "manual merge complete").await;
    assert_eq!(task.status, "done");

    let log: Value = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/transitions", task.id),
        StatusCode::OK,
    )
    .await;
    let entries = log["items"].as_array().expect("transition items");
    let pairs = entries
        .iter()
        .map(|entry| {
            (
                entry["from_state"].as_str().unwrap().to_owned(),
                entry["to_state"].as_str().unwrap().to_owned(),
                entry["trigger_reason"].as_str().unwrap().to_owned(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        pairs,
        vec![
            (
                "in_progress".to_owned(),
                "review".to_owned(),
                "ready for review".to_owned()
            ),
            (
                "review".to_owned(),
                "in_progress".to_owned(),
                "gate rejected: missing tests".to_owned()
            ),
            (
                "in_progress".to_owned(),
                "review".to_owned(),
                "retry ready".to_owned()
            ),
            (
                "review".to_owned(),
                "merging".to_owned(),
                "gate approved: looks good".to_owned()
            ),
            (
                "merging".to_owned(),
                "done".to_owned(),
                "manual merge complete".to_owned()
            ),
        ]
    );
    assert!(entries
        .iter()
        .all(|entry| entry["triggered_by"] == "user:api" || entry["triggered_by"] == "system"));
}

async fn assign_human_reviewer(app: &Router, task: &TaskResponse) {
    let _: Value = json_request(
        app,
        Method::PUT,
        &format!("/api/v1/tasks/{}/roles/reviewer", task.id),
        json!({ "assignee_type": "user", "assignee_id": "test-user-id" }),
        StatusCode::OK,
    )
    .await;
}

async fn transition(app: &Router, task: &TaskResponse, status: &str, reason: &str) -> TaskResponse {
    let response: TransitionTaskResponse = json_request(
        app,
        Method::POST,
        &format!("/api/v1/tasks/{}/transition", task.id),
        json!({ "status": status, "version": task.version, "reason": reason }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(response.task.status, status);
    response.task
}

async fn gate(
    app: &Router,
    task: &TaskResponse,
    gate_state: &str,
    decision: &str,
    reason: &str,
) -> TaskResponse {
    let task: TaskResponse = json_request(
        app,
        Method::POST,
        &format!("/api/v1/tasks/{}/gates/{gate_state}/{decision}", task.id),
        json!({ "version": task.version, "reason": reason }),
        StatusCode::OK,
    )
    .await;
    assert!(!task.status.is_empty());
    task
}

struct Harness {
    app: Router,
    db: Arc<db::SqliteDb>,
    _state: Arc<AppState>,
    _web_dist_dir: common::TestDir,
}

async fn test_app() -> Harness {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(db::SqliteDb::new(pool));
    let now = db::now_rfc3339();
    db::UserRepo::create_user(
        &*db,
        &db::User {
            id: "test-user-id".to_owned(),
            email: "test@example.com".to_owned(),
            password_hash: "$2b$04$placeholder".to_owned(),
            display_name: None,
            is_admin: true,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("seed test user");
    let adapter_registry = Arc::new(cli_adapters::default_registry());
    services::ensure_default_agents(db.as_ref(), &adapter_registry)
        .await
        .expect("default agents upsert");
    let event_bus = Arc::new(events::EventBus::new(64));
    let state = Arc::new(AppState::with_adapter_registry(
        Arc::clone(&db),
        event_bus,
        true,
        adapter_registry,
    ));
    let web_dist_dir = common::TestDir::new("forge-transition-log-web");
    std::fs::write(web_dist_dir.path().join("index.html"), "<html></html>").expect("write index");
    let app = build_router((*state).clone(), web_dist_dir.path().to_path_buf());
    Harness {
        app,
        db,
        _state: state,
        _web_dist_dir: web_dist_dir,
    }
}

async fn set_task_status(harness: &Harness, task_id: &str, status: &str) -> TaskResponse {
    sqlx::query("UPDATE task SET status = ?, version = version + 1 WHERE id = ?")
        .bind(status)
        .bind(task_id)
        .execute(harness.db.pool())
        .await
        .expect("task status updates");
    empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{task_id}"),
        StatusCode::OK,
    )
    .await
}

async fn create_project_and_repo(app: &Router) -> (String, String) {
    let project: ProjectResponse = json_request(
        app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Transition Log" }),
        StatusCode::OK,
    )
    .await;
    let repo: RepoResponse = json_request(
        app,
        Method::POST,
        &format!("/api/v1/projects/{}/repos", project.id),
        json!({ "name": "repo", "remote_url": "https://example.com/repo.git", "default_branch": "main" }),
        StatusCode::OK,
    )
    .await;
    (project.id, repo.id)
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
                .header(header::AUTHORIZATION, format!("Bearer {}", test_jwt()))
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .expect("build request"),
        )
        .await
        .expect("router response");
    parse_response(response, expected_status).await
}

async fn empty_request<T>(app: &Router, method: Method, uri: &str, expected_status: StatusCode) -> T
where
    T: DeserializeOwned,
{
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {}", test_jwt()))
                .body(Body::empty())
                .expect("build request"),
        )
        .await
        .expect("router response");
    parse_response(response, expected_status).await
}

fn test_jwt() -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let claims = serde_json::json!({
        "sub": "test-user-id",
        "email": "test@example.com",
        "is_admin": true,
        "iat": now,
        "exp": now + 900,
    });
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(b"test-jwt-secret-for-development"),
    )
    .expect("encode test jwt")
}

async fn parse_response<T>(response: axum::response::Response, expected_status: StatusCode) -> T
where
    T: DeserializeOwned,
{
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    assert_eq!(
        status,
        expected_status,
        "body: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).expect("parse JSON")
}
