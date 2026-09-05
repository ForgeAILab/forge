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
async fn review_retry_budget_reports_exhaustion_without_blocking_gate_entry() {
    let harness = test_app().await;
    let (project_id, _repo_id) = create_project_and_repo(&harness.app).await;
    let _: Value = json_request(
        &harness.app,
        Method::PUT,
        &format!("/api/v1/projects/{project_id}/workflow"),
        json!({ "definition": review_budget_workflow() }),
        StatusCode::OK,
    )
    .await;

    let mut task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Retry budget" }),
        StatusCode::OK,
    )
    .await;

    task = transition(&harness.app, &task, "in_progress").await;
    task = transition(&harness.app, &task, "review").await;
    task = gate_reject(&harness.app, &task).await;
    assert_eq!(task.status, "in_progress");

    task = transition(&harness.app, &task, "review").await;
    assert_eq!(
        task.status, "review",
        "the second review cycle should stay in review; review execution failure enforces the exhausted budget"
    );
    assert_eq!(task.remaining_retries.get("review"), Some(&0));
    assert!(
        task.blocked.is_none(),
        "entering review should not block before review execution fails"
    );
}

fn review_budget_workflow() -> Value {
    json!({
        "roles": [],
        "states": [
            state("todo", "initial", Value::Null, trigger("in_progress")),
            state("in_progress", "active", Value::Null, trigger("review")),
            state("review", "gate", json!({ "max_rejections": 1 }), json!({
                "accept": { "to": "done", "dispatch": null },
                "reject": { "to": "in_progress", "dispatch": null }
            })),
            state("done", "terminal", Value::Null, json!({}))
        ],
        "cancellation_state": null
    })
}

async fn transition(app: &Router, task: &TaskResponse, status: &str) -> TaskResponse {
    let response: TransitionTaskResponse = json_request(
        app,
        Method::POST,
        &format!("/api/v1/tasks/{}/transition", task.id),
        json!({ "status": status, "version": task.version, "reason": format!("to {status}") }),
        StatusCode::OK,
    )
    .await;
    assert!(!response.task.status.is_empty());
    response.task
}

async fn gate_reject(app: &Router, task: &TaskResponse) -> TaskResponse {
    json_request(
        app,
        Method::POST,
        &format!("/api/v1/tasks/{}/gates/review/reject", task.id),
        json!({ "version": task.version, "reason": "needs another pass" }),
        StatusCode::OK,
    )
    .await
}

fn state(name: &str, kind: &str, gate_config: Value, triggers: Value) -> Value {
    let canonical_phase = match kind {
        "backlog" => "backlog",
        "initial" => "ready",
        "gate" => "review",
        "terminal" => "done",
        _ => "working",
    };
    json!({ "name": name, "kind": kind, "canonical_phase": canonical_phase, "column": name, "display_name": name, "role": null, "hooks": {}, "gate_config": gate_config, "triggers": triggers, "config": {} })
}

fn trigger(to: &str) -> Value {
    json!({ "accept": { "to": to, "dispatch": null } })
}

struct Harness {
    app: Router,
    _state: Arc<AppState>,
    _web_dist_dir: common::TestDir,
}

async fn test_app() -> Harness {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(db::SqliteDb::new(pool));
    let adapter_registry = Arc::new(cli_adapters::default_registry());
    services::ensure_default_agents(db.as_ref(), &adapter_registry)
        .await
        .expect("default agents upsert");
    let event_bus = Arc::new(events::EventBus::new(64));
    let state = Arc::new(AppState::with_adapter_registry(
        db,
        event_bus,
        true,
        adapter_registry,
    ));
    let web_dist_dir = common::TestDir::new("forge-retry-budget-web");
    std::fs::write(web_dist_dir.path().join("index.html"), "<html></html>").expect("write index");
    let app = build_router((*state).clone(), web_dist_dir.path().to_path_buf());
    Harness {
        app,
        _state: state,
        _web_dist_dir: web_dist_dir,
    }
}

async fn create_project_and_repo(app: &Router) -> (String, String) {
    let project: ProjectResponse = json_request(
        app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Retry Budget" }),
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
                .expect("build empty request"),
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
