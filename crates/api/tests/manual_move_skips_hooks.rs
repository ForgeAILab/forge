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
use db::{ReviewRepo, TaskRepo, UpdateTask};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tower::ServiceExt;

#[tokio::test]
async fn manual_transitions_without_workspace_skip_contextual_hooks() {
    let harness = test_app().await;
    let (project_id, _repo_id) = create_project_and_repo(&harness.app).await;
    let _: Value = json_request(
        &harness.app,
        Method::PUT,
        &format!("/api/v1/projects/{project_id}/workflow"),
        json!({ "definition": manual_workflow() }),
        StatusCode::OK,
    )
    .await;

    let task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Manual no workspace" }),
        StatusCode::OK,
    )
    .await;
    let in_progress = transition(&harness.app, &task, "in_progress").await;
    let reviewed = transition(&harness.app, &in_progress, "review").await;
    let done = transition(&harness.app, &reviewed, "done").await;
    assert_eq!(done.status, "done");

    let reviews = ReviewRepo::list_by_task(&*harness.state.db, &task.id)
        .await
        .expect("reviews load");
    assert_eq!(
        reviews.len(),
        0,
        "manual review move must not create review rows"
    );

    let log: Value = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/transitions", task.id),
        StatusCode::OK,
    )
    .await;
    let entries = log["items"].as_array().expect("transition items");
    assert!(entries.iter().any(|entry| {
        entry["to_state"] == "review"
            && entry["hook_results_json"]
                .as_array()
                .unwrap()
                .iter()
                .any(|hook| {
                    hook["action"] == "run_ci_steps"
                        && hook["outcome"] == "skipped"
                        && hook["error"]
                            .as_str()
                            .is_some_and(|reason| reason.contains("no workspace"))
                })
    }));
    assert!(entries.iter().any(|entry| {
        entry["to_state"] == "done"
            && entry["hook_results_json"]
                .as_array()
                .unwrap()
                .iter()
                .any(|hook| {
                    hook["action"] == "cleanup_workspace_now" && hook["outcome"] == "skipped"
                })
    }));
}

#[tokio::test]
async fn board_drag_to_active_state_does_not_defer_role_dispatch_hook() {
    let harness = test_app().await;
    let (project_id, _repo_id) = create_project_and_repo(&harness.app).await;
    let _: Value = json_request(
        &harness.app,
        Method::PUT,
        &format!("/api/v1/projects/{project_id}/workflow"),
        json!({ "definition": dispatch_workflow() }),
        StatusCode::OK,
    )
    .await;

    let task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Drag to work" }),
        StatusCode::OK,
    )
    .await;
    let response: TransitionTaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/transition", task.id),
        json!({
            "status": "in_progress",
            "version": task.version,
            "reason": "drag card",
            "source": "board_drag"
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(response.task.status, "in_progress");

    let stored = TaskRepo::get_by_id(&*harness.state.db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let metadata = stored.metadata().expect("metadata parses");
    assert!(
        metadata.extra.get("deferred_dispatch").is_none(),
        "board drag into active work should not defer dispatch"
    );

    let log: Value = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/transitions", task.id),
        StatusCode::OK,
    )
    .await;
    let entries = log["items"].as_array().expect("transition items");
    assert!(!entries.iter().any(|entry| {
        entry["to_state"] == "in_progress"
            && entry["triggered_by"] == "user:board_drag"
            && entry["hook_results_json"]
                .as_array()
                .unwrap()
                .iter()
                .any(|hook| {
                    hook["action"] == "dispatch_role_agent"
                        && hook["outcome"] == "skipped"
                        && hook["error"] == "dispatch deferred after board drag"
                })
    }));
}

#[tokio::test]
async fn default_workflow_board_drag_from_todo_to_in_progress_succeeds() {
    let harness = test_app().await;
    let (project_id, _repo_id) = create_project_and_repo(&harness.app).await;

    let task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Human starts implementation" }),
        StatusCode::OK,
    )
    .await;
    let response: TransitionTaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/transition", task.id),
        json!({
            "status": "in_progress",
            "version": task.version,
            "reason": "drag card",
            "source": "board_drag"
        }),
        StatusCode::OK,
    )
    .await;

    assert_eq!(response.task.status, "in_progress");
    let log: Value = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/transitions", task.id),
        StatusCode::OK,
    )
    .await;
    let entries = log["items"].as_array().expect("transition items");
    assert!(entries.iter().any(|entry| {
        entry["from_state"] == "todo"
            && entry["to_state"] == "in_progress"
            && entry["triggered_by"] == "user:board_drag"
    }));
    assert!(!entries.iter().any(|entry| entry["to_state"] == "planning"));
}

#[tokio::test]
async fn default_workflow_user_move_to_review_waits_for_human() {
    let harness = test_app().await;
    let (project_id, _repo_id) = create_project_and_repo(&harness.app).await;

    let task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Human review hold" }),
        StatusCode::OK,
    )
    .await;
    let in_progress = transition(&harness.app, &task, "in_progress").await;
    let review = transition(&harness.app, &in_progress, "review").await;

    assert_eq!(review.status, "review");
    assert!(review.awaiting_human);
    let log: Value = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/transitions", task.id),
        StatusCode::OK,
    )
    .await;
    let entries = log["items"].as_array().expect("transition items");
    assert!(!entries
        .iter()
        .any(|entry| { entry["from_state"] == "review" && entry["to_state"] == "merging" }));

    let merging = transition(&harness.app, &review, "merging").await;
    assert_eq!(merging.status, "merging");
    assert!(!merging.awaiting_human);
}

#[tokio::test]
async fn manual_transition_clears_executor_failure_annotation() {
    let harness = test_app().await;
    let (project_id, _repo_id) = create_project_and_repo(&harness.app).await;
    let _: Value = json_request(
        &harness.app,
        Method::PUT,
        &format!("/api/v1/projects/{project_id}/workflow"),
        json!({ "definition": manual_workflow() }),
        StatusCode::OK,
    )
    .await;

    let task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Manual clears execution failure" }),
        StatusCode::OK,
    )
    .await;
    let annotation = json!({
        "type": "executor_failed",
        "blocking_reason": "executor_failed",
        "blocked_by": "system:executor",
        "blocked_at": "2026-05-01T00:00:00Z",
        "blocked_execution_id": "execution-old",
        "artifact": null,
        "message": "Execution failed before workflow processing finished",
        "recovery_actions": ["reexecute", "cancel_task"],
    });
    let annotated = TaskRepo::update(
        &*harness.state.db,
        UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(annotation.to_string())),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: db::now_rfc3339(),
        },
    )
    .await
    .expect("annotation stored");
    assert!(annotated.error_annotation.is_some());

    let annotated_response: TaskResponse = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", task.id),
        StatusCode::OK,
    )
    .await;
    assert!(annotated_response.error_annotation.is_some());

    let moved = transition(&harness.app, &annotated_response, "in_progress").await;
    assert_eq!(moved.status, "in_progress");
    assert!(
        moved.error_annotation.is_none(),
        "manual transition should clear stale executor failure annotation"
    );
}

#[tokio::test]
async fn board_drag_to_passive_state_defers_role_dispatch_hook() {
    let harness = test_app().await;
    let (project_id, _repo_id) = create_project_and_repo(&harness.app).await;
    let _: Value = json_request(
        &harness.app,
        Method::PUT,
        &format!("/api/v1/projects/{project_id}/workflow"),
        json!({ "definition": gate_dispatch_workflow() }),
        StatusCode::OK,
    )
    .await;

    let task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Drag to planning" }),
        StatusCode::OK,
    )
    .await;
    let response: TransitionTaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/transition", task.id),
        json!({
            "status": "planning",
            "version": task.version,
            "reason": "drag card",
            "source": "board_drag"
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(response.task.status, "planning");

    let stored = TaskRepo::get_by_id(&*harness.state.db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let metadata = stored.metadata().expect("metadata parses");
    assert!(
        metadata.extra.get("deferred_dispatch").is_some(),
        "board drag into a passive state should record deferred dispatch metadata"
    );

    let log: Value = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/transitions", task.id),
        StatusCode::OK,
    )
    .await;
    let entries = log["items"].as_array().expect("transition items");
    assert!(entries.iter().any(|entry| {
        entry["to_state"] == "planning"
            && entry["triggered_by"] == "user:board_drag"
            && entry["hook_results_json"]
                .as_array()
                .unwrap()
                .iter()
                .any(|hook| {
                    hook["action"] == "dispatch_role_agent"
                        && hook["outcome"] == "skipped"
                        && hook["error"] == "dispatch deferred after board drag"
                })
    }));
}

fn manual_workflow() -> Value {
    json!({
        "roles": [],
        "states": [
            state("todo", "initial", json!({}), json!({}), trigger("in_progress")),
            state("in_progress", "active", json!({}), json!({}), trigger("review")),
            state("review", "gate", json!({
                "on_enter": [{ "action": "run_ci_steps", "params": {}, "applies_to": "all", "on_failure": "log" }],
                "after_enter": [{ "action": "auto_cascade_on_review_pass", "params": {}, "applies_to": "all", "on_failure": "log" }]
            }), json!({ "review": { "ci_steps": ["true"] } }), trigger("done")),
            state("done", "terminal", json!({
                "on_enter": [
                    { "action": "cleanup_workspace_now", "params": {}, "applies_to": "all", "on_failure": "log" },
                    { "action": "satisfy_dependents", "params": {}, "applies_to": "all", "on_failure": "log" }
                ]
            }), json!({}), json!({}))
        ],
        "cancellation_state": null
    })
}

fn dispatch_workflow() -> Value {
    json!({
        "roles": [{
            "name": "coder",
            "display_name": "Coder",
            "description": "Implements the work"
        }],
        "states": [
            state("todo", "initial", json!({}), json!({}), trigger("in_progress")),
            state("in_progress", "active", json!({
                "on_enter": [{ "action": "dispatch_role_agent", "params": {}, "applies_to": "all", "on_failure": "log" }]
            }), json!({}), trigger("done")),
            state("done", "terminal", json!({}), json!({}), json!({}))
        ],
        "cancellation_state": null
    })
}

fn gate_dispatch_workflow() -> Value {
    json!({
        "roles": [{
            "name": "planner",
            "display_name": "Planner",
            "description": "Plans the work"
        }],
        "states": [
            state("todo", "initial", json!({}), json!({}), trigger("planning")),
            state("planning", "gate", json!({
                "on_enter": [{ "action": "dispatch_role_agent", "params": {}, "applies_to": "all", "on_failure": "log" }]
            }), json!({}), trigger("in_progress")),
            state("in_progress", "active", json!({}), json!({}), trigger("done")),
            state("done", "terminal", json!({}), json!({}), json!({}))
        ],
        "cancellation_state": null
    })
}

async fn transition(app: &Router, task: &TaskResponse, status: &str) -> TaskResponse {
    let response: TransitionTaskResponse = json_request(
        app,
        Method::POST,
        &format!("/api/v1/tasks/{}/transition", task.id),
        json!({ "status": status, "version": task.version, "reason": format!("manual {status}") }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(response.task.status, status);
    response.task
}

fn state(name: &str, kind: &str, hooks: Value, config: Value, triggers: Value) -> Value {
    let canonical_phase = match kind {
        "backlog" => "backlog",
        "initial" => "ready",
        "gate" => "review",
        "terminal" => "done",
        _ => "working",
    };
    json!({ "name": name, "kind": kind, "canonical_phase": canonical_phase, "column": name, "display_name": name, "role": null, "hooks": hooks, "gate_config": null, "triggers": triggers, "config": config })
}

fn trigger(to: &str) -> Value {
    json!({ "accept": { "to": to, "dispatch": null } })
}

struct Harness {
    app: Router,
    state: Arc<AppState>,
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
    let web_dist_dir = common::TestDir::new("forge-manual-skips-web");
    std::fs::write(web_dist_dir.path().join("index.html"), "<html></html>").expect("write index");
    let app = build_router((*state).clone(), web_dist_dir.path().to_path_buf());
    Harness {
        app,
        state,
        _web_dist_dir: web_dist_dir,
    }
}

async fn create_project_and_repo(app: &Router) -> (String, String) {
    let project: ProjectResponse = json_request(
        app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Manual Skips" }),
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

fn test_jwt() -> String {
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
    let token = test_jwt();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header("authorization", format!("Bearer {token}"))
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
    let token = test_jwt();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .expect("build request"),
        )
        .await
        .expect("router response");
    parse_response(response, expected_status).await
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
