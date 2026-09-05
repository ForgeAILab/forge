#![allow(dead_code, clippy::assertions_on_constants)]
mod common;

use std::{sync::Arc, time::Duration};

use api::{build_router, AppState};
use api_types::{ProjectResponse, RepoResponse, TaskResponse, TransitionTaskResponse};
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use events::{EventBus, EventContext, ForgeEvent};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tower::ServiceExt;

#[tokio::test]
async fn todo_skips_unassigned_planning_without_planner_dispatch() {
    let harness = test_app().await;
    let (project_id, _repo_id) = create_project_and_repo(&harness.app).await;
    let task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Skip planning" }),
        StatusCode::OK,
    )
    .await;

    let mut rx = harness.event_bus.subscribe();
    let moved: TransitionTaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/transition", task.id),
        json!({ "status": "planning", "version": task.version, "reason": "start planning gate" }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(moved.task.status, "in_progress");

    let log: Value = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/transitions", task.id),
        StatusCode::OK,
    )
    .await;
    let entries = log["items"].as_array().expect("transition items");
    assert!(
        entries.iter().any(|entry| entry["to_state"] == "planning")
            && entries
                .iter()
                .any(|entry| entry["to_state"] == "in_progress"),
        "default planning should skip the unassigned planning gate without dispatching a planner"
    );

    let events = drain_events(&mut rx).await;
    assert!(
        !events.iter().any(|event| matches!(
            &event.context,
            EventContext::TaskRoleAgentDispatched { role, .. } if role == "planner"
        )),
        "planner should not be dispatched; got {events:?}"
    );
}

async fn drain_events(rx: &mut tokio::sync::broadcast::Receiver<ForgeEvent>) -> Vec<ForgeEvent> {
    let mut events = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Ok(event)) => events.push(event),
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => continue,
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) | Err(_) => break,
        }
    }
    events
}

struct Harness {
    app: Router,
    event_bus: Arc<EventBus>,
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
    let event_bus = Arc::new(EventBus::new(64));
    let state = Arc::new(AppState::with_adapter_registry(
        db,
        Arc::clone(&event_bus),
        true,
        adapter_registry,
    ));
    let web_dist_dir = common::TestDir::new("forge-skip-planning-web");
    std::fs::write(web_dist_dir.path().join("index.html"), "<html></html>").expect("write index");
    let app = build_router((*state).clone(), web_dist_dir.path().to_path_buf());
    Harness {
        app,
        event_bus,
        _state: state,
        _web_dist_dir: web_dist_dir,
    }
}

async fn create_project_and_repo(app: &Router) -> (String, String) {
    let project: ProjectResponse = json_request(
        app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Skip Planning" }),
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
