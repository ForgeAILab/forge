#![allow(dead_code, clippy::assertions_on_constants)]
mod common;

use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

use api::{build_router, AppState};
use api_types::{
    AgentResponse, DaemonRegisterResponse, DaemonResponse, ProjectResponse, RepoResponse,
    TaskResponse, TransitionTaskResponse,
};
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tower::ServiceExt;

#[tokio::test]
async fn agent_cannot_claim_task_in_backlog_state() {
    let repo_dir = common::TestDir::new("forge-backlog-repo");
    let repo_path = setup_git_repo(repo_dir.path());
    let workspace_root = common::TestDir::new("forge-backlog-workspaces");
    let harness = test_app(workspace_root.path()).await;
    let (project_id, _repo_id) = create_project_and_repo(&harness.app, &repo_path).await;
    let _: Value = json_request(
        &harness.app,
        Method::PUT,
        &format!("/api/v1/projects/{project_id}/workflow"),
        json!({ "definition": backlog_workflow() }),
        StatusCode::OK,
    )
    .await;
    let agent_id = create_shell_agent(&harness.app, workspace_root.path()).await;

    let task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Parked task" }),
        StatusCode::OK,
    )
    .await;
    let moved: TransitionTaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/transition", task.id),
        json!({ "status": "backlog", "version": task.version, "reason": "park" }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(moved.task.status, "backlog");

    let response = raw_json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/claim", task.id),
        json!({ "agent_id": agent_id, "overrides": null }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

fn backlog_workflow() -> Value {
    json!({
        "roles": [],
        "states": [
            state("todo", "initial", json!({
                "accept": { "to": "in_progress", "dispatch": null },
                "reject": { "to": "backlog", "dispatch": null }
            })),
            state("backlog", "backlog", json!({
                "accept": { "to": "todo", "dispatch": null }
            })),
            state("in_progress", "active", json!({
                "accept": { "to": "done", "dispatch": null }
            })),
            state("done", "terminal", json!({}))
        ],
        "cancellation_state": null
    })
}

fn state(name: &str, kind: &str, triggers: Value) -> Value {
    let canonical_phase = match kind {
        "backlog" => "backlog",
        "initial" => "ready",
        "gate" => "review",
        "terminal" => "done",
        _ => "working",
    };
    json!({ "name": name, "kind": kind, "canonical_phase": canonical_phase, "column": name, "display_name": name, "role": null, "hooks": {}, "gate_config": null, "triggers": triggers, "config": {} })
}

struct Harness {
    app: Router,
    _state: Arc<AppState>,
    _web_dist_dir: common::TestDir,
}

async fn test_app(workspace_root: &Path) -> Harness {
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
    let merge_service = Arc::new(services::MergeService::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        workspace_root.to_path_buf(),
    ));
    let cleanup_scheduler = Arc::new(services::WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        workspace_root.to_path_buf(),
    ));
    let review_runner = Arc::new(review::ReviewRunner::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        Arc::clone(&adapter_registry),
    ));
    let state = Arc::new(AppState::with_adapter_registry_services_and_shutdown(
        db,
        event_bus,
        true,
        adapter_registry,
        merge_service,
        cleanup_scheduler,
        review_runner,
        api::state::ShutdownSignal::new(),
        api::state::test_workflows_dir(),
        api::state::test_jwt_secret(),
        api::state::test_bcrypt_cost(),
    ));
    let web_dist_dir = common::TestDir::new("forge-backlog-web");
    std::fs::write(web_dist_dir.path().join("index.html"), "<html></html>").expect("write index");
    let app = build_router((*state).clone(), web_dist_dir.path().to_path_buf());
    Harness {
        app,
        _state: state,
        _web_dist_dir: web_dist_dir,
    }
}

async fn create_project_and_repo(app: &Router, repo_path: &Path) -> (String, String) {
    let project: ProjectResponse = json_request(
        app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Backlog Claim" }),
        StatusCode::OK,
    )
    .await;
    let default_branch = run_git(repo_path, &["symbolic-ref", "--short", "HEAD"]);
    let repo: RepoResponse = json_request(
        app,
        Method::POST,
        &format!("/api/v1/projects/{}/repos", project.id),
        json!({
            "name": "repo",
            "local_path": repo_path.to_string_lossy(),
            "remote_url": repo_path.to_string_lossy(),
            "default_branch": default_branch
        }),
        StatusCode::OK,
    )
    .await;
    (project.id, repo.id)
}

async fn create_shell_agent(app: &Router, workspace_root: &Path) -> String {
    let registration: DaemonRegisterResponse = json_request(
        app,
        Method::POST,
        "/api/v1/daemons/register",
        json!({ "machine_id": services::embedded_daemon::embedded_machine_id(), "hostname": "backlog-host", "os": "linux", "arch": "x86_64", "agent_version": "test" }),
        StatusCode::OK,
    )
    .await;
    let daemon_id = registration.daemon_id;
    let _: DaemonResponse = json_request_with_bearer(
        app,
        Method::POST,
        &format!("/api/v1/daemons/{daemon_id}/report"),
        &registration.registration_token,
        json!({
            "detected_clis": [{ "kind": "shell", "availability": "authenticated", "path": "/bin/sh" }],
            "runtimes": [{ "kind": "local", "workspace_root": workspace_root.to_string_lossy(), "status": "ready" }]
        }),
        StatusCode::OK,
    )
    .await;
    let agent: AgentResponse = json_request(
        app,
        Method::POST,
        "/api/v1/agents",
        json!({
            "name": "backlog-agent",
            "executor_type": "shell",
            "daemon_id": daemon_id,
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(agent.effective_status.as_deref(), Some("active"));
    agent.id
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
    let response = raw_json_request(app, method, uri, body).await;
    parse_response(response, expected_status).await
}

async fn json_request_with_bearer<T>(
    app: &Router,
    method: Method,
    uri: &str,
    token: &str,
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
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
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

async fn raw_json_request(
    app: &Router,
    method: Method,
    uri: &str,
    body: Value,
) -> axum::response::Response {
    app.clone()
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
        .expect("router response")
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

fn setup_git_repo(path: &Path) -> PathBuf {
    let repo_path = path.join("repo");
    std::fs::create_dir_all(&repo_path).expect("repo dir creates");
    run_git(&repo_path, &["init"]);
    run_git(&repo_path, &["config", "user.email", "test@forge.dev"]);
    run_git(&repo_path, &["config", "user.name", "Forge Test"]);
    std::fs::write(repo_path.join("README.md"), "# Backlog\n").expect("README writes");
    run_git(&repo_path, &["add", "-A"]);
    run_git(&repo_path, &["commit", "-m", "initial commit"]);
    repo_path
}

fn run_git(path: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .expect("git runs");
    assert!(
        output.status.success(),
        "git failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}
