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
async fn review_rejection_notifies_role_holder_via_event_bus() {
    let harness = test_app().await;
    let (project_id, _repo_id) = create_project_and_repo(&harness.app).await;
    let _: Value = json_request(
        &harness.app,
        Method::PUT,
        &format!("/api/v1/projects/{project_id}/workflow"),
        json!({ "definition": notify_workflow() }),
        StatusCode::OK,
    )
    .await;

    let mut task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Notify coder" }),
        StatusCode::OK,
    )
    .await;
    let _: Value = json_request(
        &harness.app,
        Method::PUT,
        &format!("/api/v1/tasks/{}/roles/coder", task.id),
        json!({ "assignee_type": "user", "assignee_id": "test-user-id" }),
        StatusCode::OK,
    )
    .await;
    task = transition(&harness.app, &task, "in_progress").await;
    task = transition(&harness.app, &task, "review").await;

    let mut rx = harness.event_bus.subscribe();
    let _: Vec<ForgeEvent> = drain_events(&mut rx).await;
    let rejected: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/gates/review/reject", task.id),
        json!({ "version": task.version, "reason": "please revise" }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(rejected.status, "in_progress");

    let events = drain_events(&mut rx).await;
    assert!(
        events.iter().any(|event| matches!(
            &event.context,
            EventContext::TaskRoleNotified {
                task_id,
                role,
                notified_user_handle,
                ..
            } if task_id == &task.id
                && role == "coder"
                && notified_user_handle.as_deref() == Some("test-user-id")
        )),
        "missing task.role_notified event; got {events:?}"
    );
}

fn notify_workflow() -> Value {
    json!({
        "roles": [{ "name": "coder", "display_name": "Coder", "description": "Implements" }],
        "states": [
            state(
                "todo",
                "initial",
                Value::Null,
                json!({}),
                json!({ "accept": { "to": "in_progress" } })
            ),
            state("in_progress", "active", "coder", json!({
                "on_enter": [{ "action": "notify_role_holder", "params": {}, "applies_to": "all", "on_failure": "log" }]
            }), json!({ "accept": { "to": "review" } })),
            state(
                "review",
                "gate",
                Value::Null,
                json!({}),
                json!({
                    "accept": { "to": "done" },
                    "reject": { "to": "in_progress" }
                })
            ),
            state("done", "terminal", Value::Null, json!({}), json!({}))
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
    assert_eq!(response.task.status, status);
    response.task
}

fn state(name: &str, kind: &str, role: impl Into<Value>, hooks: Value, triggers: Value) -> Value {
    let canonical_phase = match kind {
        "backlog" => "backlog",
        "initial" => "ready",
        "gate" => "review",
        "terminal" => "done",
        _ => "working",
    };
    json!({ "name": name, "kind": kind, "column": name, "display_name": name, "role": role.into(), "hooks": hooks, "canonical_phase": canonical_phase, "gate_config": null, "triggers": triggers, "config": {} })
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
    let now = db::now_rfc3339();
    db::UserRepo::create_user(
        &*db,
        &db::User {
            id: "test-user-id".to_owned(),
            email: "test@example.com".to_owned(),
            password_hash: "$2b$04$placeholder".to_owned(),
            display_name: None,
            is_admin: false,
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
    let event_bus = Arc::new(EventBus::new(64));
    let state = Arc::new(AppState::with_adapter_registry(
        db,
        Arc::clone(&event_bus),
        true,
        adapter_registry,
    ));
    let web_dist_dir = common::TestDir::new("forge-assignee-notification-web");
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
        json!({ "name": "Assignee Notification" }),
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
    let token = {
        use jsonwebtoken::{Algorithm, EncodingKey, Header};
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let claims = serde_json::json!({
            "sub": "test-user-id",
            "email": "test@example.com",
            "iat": now,
            "exp": now + 900,
        });
        jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(b"test-jwt-secret-for-development"),
        )
        .expect("encode test jwt")
    };
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .expect("build request"),
        )
        .await
        .expect("router response");
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
