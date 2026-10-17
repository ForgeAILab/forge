#![allow(dead_code, clippy::assertions_on_constants)]
use api::{build_router, AppState};
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use events::EventBus;
use serde_json::{json, Value};
use tower::ServiceExt;

async fn setup() -> Router {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = std::sync::Arc::new(db::SqliteDb::new(pool));
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
    let event_bus = std::sync::Arc::new(EventBus::new(16));
    let state = AppState::new(db, event_bus, true);
    state.workflow_template_service.initialize().await.unwrap();
    let web = std::env::temp_dir().join(format!("forge-wf-tpl-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&web).unwrap();
    std::fs::write(web.join("index.html"), "<html></html>").unwrap();
    build_router(state, web)
}

async fn request(
    app: &Router,
    method: Method,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {}", test_jwt()));
    let body = if let Some(body) = body {
        builder = builder.header(header::CONTENT_TYPE, "application/json");
        Body::from(serde_json::to_string(&body).unwrap())
    } else {
        Body::empty()
    };
    let response = app
        .clone()
        .oneshot(builder.body(body).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
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

fn minimal_workflow() -> Value {
    json!({
        "roles": [],
        "states": [
            {"name": "todo", "kind": "initial", "canonical_phase": "ready", "column": "Todo", "display_name": "Todo", "role": null, "hooks": {"before_exit": [], "on_exit": [], "before_enter": [], "on_enter": [], "after_enter": []}, "cleanup": null, "gate_config": null, "dispatch": null, "triggers": {"accept": {"to": "in_progress", "dispatch": null}}, "config": {}},
            {"name": "in_progress", "kind": "active", "canonical_phase": "working", "column": "In Progress", "display_name": "In Progress", "role": null, "hooks": {"before_exit": [], "on_exit": [], "before_enter": [], "on_enter": [], "after_enter": []}, "cleanup": null, "gate_config": null, "dispatch": null, "triggers": {"accept": {"to": "done", "dispatch": null}}, "config": {}},
            {"name": "done", "kind": "terminal", "canonical_phase": "done", "column": "Done", "display_name": "Done", "role": null, "hooks": {"before_exit": [], "on_exit": [], "before_enter": [], "on_enter": [], "after_enter": []}, "cleanup": null, "gate_config": null, "dispatch": null, "triggers": {}, "config": {}}
        ],
        "configuration": [],
        "cancellation_state": null
    })
}

#[tokio::test]
async fn list_workflow_templates_includes_default_builtin_template() {
    let app = setup().await;

    let (status, body) = request(&app, Method::GET, "/api/v1/workflow-templates", None).await;

    assert_eq!(status, StatusCode::OK);
    let templates = body.as_array().expect("template list should be an array");
    assert!(templates
        .iter()
        .any(|template| template["name"] == "default" && template["is_builtin"] == true));
}

#[tokio::test]
async fn workflow_template_full_crud_flow() {
    let app = setup().await;

    let (status, _) = request(
        &app,
        Method::PUT,
        "/api/v1/workflow-templates/fast-track",
        Some(json!({
            "display_name": "Fast Track",
            "description": "quick",
            "definition": minimal_workflow()
        })),
    )
    .await;
    assert!(matches!(status, StatusCode::OK | StatusCode::CREATED));

    let (status, body) = request(&app, Method::GET, "/api/v1/workflow-templates", None).await;
    assert_eq!(status, StatusCode::OK);
    let templates = body.as_array().expect("template list should be an array");
    assert!(templates
        .iter()
        .any(|template| template["name"] == "fast-track"));

    let (status, body) = request(
        &app,
        Method::GET,
        "/api/v1/workflow-templates/fast-track",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["name"], "fast-track");

    let (status, body) = request(
        &app,
        Method::PUT,
        "/api/v1/workflow-templates/fast-track",
        Some(json!({
            "display_name": "Fast Track Updated",
            "description": "quick",
            "definition": minimal_workflow()
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["display_name"], "Fast Track Updated");

    let (status, _) = request(
        &app,
        Method::DELETE,
        "/api/v1/workflow-templates/fast-track",
        None,
    )
    .await;
    assert!(matches!(status, StatusCode::OK | StatusCode::NO_CONTENT));

    let (status, _) = request(
        &app,
        Method::GET,
        "/api/v1/workflow-templates/fast-track",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn project_workflow_can_be_applied_from_template() {
    let app = setup().await;

    let (status, project) = request(
        &app,
        Method::POST,
        "/api/v1/projects",
        Some(json!({ "name": "Workflow Template Project" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let project_id = project["id"].as_str().expect("project id").to_owned();

    let (status, _) = request(
        &app,
        Method::PUT,
        "/api/v1/workflow-templates/fast-track",
        Some(json!({
            "display_name": "Fast Track",
            "description": "quick",
            "definition": minimal_workflow()
        })),
    )
    .await;
    assert!(matches!(status, StatusCode::OK | StatusCode::CREATED));

    let (status, body) = request(
        &app,
        Method::PUT,
        &format!("/api/v1/projects/{project_id}/workflow"),
        Some(json!({ "template_name": "fast-track" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, minimal_workflow());

    let (status, body) = request(
        &app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["workflow_template_name"], "fast-track");

    let (status, body) = request(
        &app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}/workflow"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, minimal_workflow());
}

#[tokio::test]
async fn project_workflow_can_apply_autonomous_v1_from_builtin_template() {
    let app = setup().await;

    let (status, project) = request(
        &app,
        Method::POST,
        "/api/v1/projects",
        Some(json!({ "name": "Autonomous Workflow Project" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let project_id = project["id"].as_str().expect("project id");

    let (status, body) = request(
        &app,
        Method::PUT,
        &format!("/api/v1/projects/{project_id}/workflow"),
        Some(json!({ "template_name": "autonomous_v1" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["roles"].as_array().expect("roles"),
        &vec![json!({
            "name": "worker",
            "display_name": "Worker",
            "description": "Owns planning, implementation, self-validation, and routine recovery for the task."
        })]
    );
    assert!(body["states"]
        .as_array()
        .expect("states")
        .iter()
        .any(|state| state["name"] == "working" && state["canonical_phase"] == "working"));

    let (status, project) = request(
        &app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(project["workflow_template_name"], "autonomous_v1");
}

#[tokio::test]
async fn deleting_default_workflow_template_is_forbidden() {
    let app = setup().await;

    let (status, _) = request(
        &app,
        Method::DELETE,
        "/api/v1/workflow-templates/default",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn saving_invalid_workflow_template_definition_returns_bad_request() {
    let app = setup().await;

    let (status, _) = request(
        &app,
        Method::PUT,
        "/api/v1/workflow-templates/bad-wf",
        Some(json!({
            "display_name": "Bad",
            "description": "",
            "definition": {
                "states": [],
                "roles": []
            }
        })),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn saving_workflow_template_with_invalid_name_returns_bad_request() {
    let app = setup().await;

    let (status, _) = request(
        &app,
        Method::PUT,
        "/api/v1/workflow-templates/Bad%20Name!",
        Some(json!({
            "display_name": "Bad Name",
            "description": "quick",
            "definition": minimal_workflow()
        })),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn applying_template_blocked_by_active_task_in_removed_state() {
    let app = setup().await;

    let (status, project) = request(
        &app,
        Method::POST,
        "/api/v1/projects",
        Some(json!({ "name": "Safety Test" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let project_id = project["id"].as_str().expect("project id").to_owned();

    let temp_path = std::env::temp_dir().join(format!("forge-safety-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&temp_path).unwrap();

    let (status, repo) = request(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/repos"),
        Some(json!({
            "name": "repo",
            "local_path": temp_path.to_string_lossy(),
            "remote_url": temp_path.to_string_lossy(),
            "default_branch": "main"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let _repo_id = repo["id"].as_str().expect("repo id").to_owned();

    // Create a task — starts in "todo" state by default
    let (status, _) = request(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        Some(json!({ "title": "Blocking task" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Template without "todo" state
    let no_todo_workflow = json!({
        "roles": [],
        "states": [
            {"name": "start", "kind": "initial", "canonical_phase": "ready", "column": "Start", "display_name": "Start",
             "role": null, "hooks": {"before_exit": [], "on_exit": [], "before_enter": [], "on_enter": [], "after_enter": []},
             "cleanup": null, "gate_config": null, "triggers": {"accept": {"to": "running", "dispatch": null}}, "config": {}},
            {"name": "running", "kind": "active", "canonical_phase": "working", "column": "Running", "display_name": "Running",
             "role": null, "hooks": {"before_exit": [], "on_exit": [], "before_enter": [], "on_enter": [], "after_enter": []},
             "cleanup": null, "gate_config": null, "triggers": {"accept": {"to": "done", "dispatch": null}}, "config": {}},
            {"name": "done", "kind": "terminal", "canonical_phase": "done", "column": "Done", "display_name": "Done",
             "role": null, "hooks": {"before_exit": [], "on_exit": [], "before_enter": [], "on_enter": [], "after_enter": []},
             "cleanup": null, "gate_config": null, "triggers": {}, "config": {}}
        ],
        "cancellation_state": null
    });

    let (status, _) = request(
        &app,
        Method::PUT,
        "/api/v1/workflow-templates/no-todo",
        Some(json!({
            "display_name": "No Todo",
            "description": "Workflow that removes the todo state",
            "definition": no_todo_workflow
        })),
    )
    .await;
    assert!(matches!(status, StatusCode::OK | StatusCode::CREATED));

    // Applying the template should fail because a task is in "todo" which is absent from the new workflow
    let (status, body) = request(
        &app,
        Method::PUT,
        &format!("/api/v1/projects/{project_id}/workflow"),
        Some(json!({ "template_name": "no-todo" })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["code"], "workflow_state_in_use");

    // Clean up template
    let _ = request(
        &app,
        Method::DELETE,
        "/api/v1/workflow-templates/no-todo",
        None,
    )
    .await;
}

#[tokio::test]
async fn applying_template_that_removes_state_with_active_tasks_is_rejected() {
    let app = setup().await;

    let (status, project) = request(
        &app,
        Method::POST,
        "/api/v1/projects",
        Some(json!({ "name": "Safety Test" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let project_id = project["id"].as_str().expect("project id").to_owned();

    let (status, repo) = request(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/repos"),
        Some(json!({
            "name": "repo",
            "remote_url": "https://example.com/r.git",
            "default_branch": "main"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let _repo_id = repo["id"].as_str().expect("repo id").to_owned();

    let (status, _) = request(
        &app,
        Method::PUT,
        &format!("/api/v1/projects/{project_id}/workflow"),
        Some(json!({ "definition": minimal_workflow() })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, task) = request(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        Some(json!({ "title": "test task" })),
    )
    .await;
    assert!(matches!(status, StatusCode::OK | StatusCode::CREATED));
    let task_id = task["id"].as_str().expect("task id").to_owned();

    let (status, _) = request(
        &app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/transition"),
        Some(json!({
            "status": "in_progress",
            "version": 1,
            "reason": "test"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = request(
        &app,
        Method::PUT,
        "/api/v1/workflow-templates/minimal",
        Some(json!({
            "display_name": "Minimal",
            "description": "Workflow that removes in_progress",
            "definition": {
                "roles": [],
                "states": [
                    {"name": "todo", "kind": "initial", "canonical_phase": "ready", "column": "Todo", "display_name": "Todo", "role": null, "hooks": {"before_exit": [], "on_exit": [], "before_enter": [], "on_enter": [], "after_enter": []}, "cleanup": null, "gate_config": null, "dispatch": null, "triggers": {"accept": {"to": "coding", "dispatch": null}}, "config": {}},
                    {"name": "coding", "kind": "active", "canonical_phase": "working", "column": "Coding", "display_name": "Coding", "role": null, "hooks": {"before_exit": [], "on_exit": [], "before_enter": [], "on_enter": [], "after_enter": []}, "cleanup": null, "gate_config": null, "dispatch": null, "triggers": {"accept": {"to": "done", "dispatch": null}}, "config": {}},
                    {"name": "done", "kind": "terminal", "canonical_phase": "done", "column": "Done", "display_name": "Done", "role": null, "hooks": {"before_exit": [], "on_exit": [], "before_enter": [], "on_enter": [], "after_enter": []}, "cleanup": null, "gate_config": null, "dispatch": null, "triggers": {}, "config": {}}
                ],
                "cancellation_state": null
            }
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = request(
        &app,
        Method::PUT,
        &format!("/api/v1/projects/{project_id}/workflow"),
        Some(json!({ "template_name": "minimal" })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let body_string = body.to_string().to_lowercase();
    assert!(
        body_string.contains("in_progress")
            || body_string.contains("active")
            || body_string.contains("cannot remove")
    );
}
