#![allow(dead_code, clippy::assertions_on_constants)]
mod common;

use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::Duration,
};

use api::{build_router, AppState};
use api_types::{
    AgentResponse, CliProjectionItem, CliProjectionResponse, DaemonRegisterResponse,
    DaemonResponse, ExecutionResponse, ExecutionStatus, PaginatedResponse, ProjectResponse,
    RepoResponse, TaskResponse,
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
async fn daemon_onboarding_shell_task_flow_end_to_end() {
    let (app, state) = test_app_with_state().await;

    let registration: DaemonRegisterResponse = json_request(
        &app,
        Method::POST,
        "/api/v1/daemons/register",
        json!({
            "machine_id": services::embedded_daemon::embedded_machine_id(),
            "hostname": "e2e-test-host",
            "os": "linux",
            "arch": "x86_64",
            "agent_version": "e2e-test",
            "labels": { "suite": "daemon_onboarding" }
        }),
        StatusCode::OK,
    )
    .await;
    assert!(!registration.daemon_id.is_empty());
    assert!(!registration.registration_token.is_empty());
    let daemon_id = registration.daemon_id;

    let report: DaemonResponse = json_request_with_bearer(
        &app,
        Method::POST,
        &format!("/api/v1/daemons/{daemon_id}/report"),
        &registration.registration_token,
        json!({
            "detected_clis": [{
                "kind": "shell",
                "availability": "authenticated",
                "path": "/bin/sh"
            }],
            "runtimes": [{
                "kind": "local",
                "workspace_root": "/tmp/forge-e2e",
                "status": "ready"
            }]
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(report.id, daemon_id);
    assert_eq!(report.status, "online");
    assert_eq!(report.detected_clis[0]["kind"], "shell");
    assert_eq!(report.detected_clis[0]["availability"], "authenticated");

    let clis: CliProjectionResponse = empty_request(
        &app,
        Method::GET,
        &format!("/api/v1/clis?daemon_id={daemon_id}"),
        StatusCode::OK,
    )
    .await;
    let shell_cli = shell_cli_for_daemon(&clis, &daemon_id);
    assert_eq!(shell_cli.availability, "authenticated");
    assert!(shell_cli.agents.is_empty());

    let (project_id, repo_id, _repo_dir) = create_project_and_repo(&app).await;

    let agent: AgentResponse = json_request(
        &app,
        Method::POST,
        "/api/v1/agents",
        json!({
            "name": "daemon-onboarding-shell-agent",
            "executor_type": "shell",
            "daemon_id": daemon_id,
            "config_json": {
                "command": "echo",
                "args": ["forge-e2e-ok"]
            }
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(agent.executor_type, "shell");
    assert_eq!(agent.daemon_id.as_deref(), Some(daemon_id.as_str()));
    assert_eq!(agent.effective_status.as_deref(), Some("active"));
    let agent_id = agent.id;

    let reviewer: AgentResponse = json_request(
        &app,
        Method::POST,
        "/api/v1/agents",
        json!({
            "name": "daemon-onboarding-shell-reviewer",
            "executor_type": "shell",
            "daemon_id": daemon_id,
            "config_json": {
                "command": "printf",
                "args": ["===REVIEW: PASS===\\n"]
            }
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(reviewer.executor_type, "shell");
    assert_eq!(reviewer.daemon_id.as_deref(), Some(daemon_id.as_str()));
    assert_eq!(reviewer.effective_status.as_deref(), Some("active"));
    let reviewer_id = reviewer.id;
    common::configure_execution_test_setup(
        &state.db,
        &project_id,
        &repo_id,
        &agent_id,
        &reviewer_id,
    )
    .await;

    let clis_after_agent: CliProjectionResponse = empty_request(
        &app,
        Method::GET,
        &format!("/api/v1/clis?daemon_id={daemon_id}"),
        StatusCode::OK,
    )
    .await;
    let shell_cli_after_agent = shell_cli_for_daemon(&clis_after_agent, &daemon_id);
    let cli_agent = shell_cli_after_agent
        .agents
        .iter()
        .find(|projection_agent| projection_agent.id == agent_id)
        .expect("created agent is listed under shell CLI projection");
    if let Some(effective_status) = cli_agent.effective_status.as_deref() {
        assert_eq!(effective_status, "active");
    }

    let task: TaskResponse = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Daemon onboarding shell task",
            "description": "echo forge-e2e-ok"
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(task.status, "todo".to_owned());

    let claimed: TaskResponse = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/tasks/{}/claim", task.id),
        json!({ "agent_id": agent_id, "overrides": null }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(claimed.status, "in_progress".to_owned());
    assert!(claimed.role_assignments.iter().any(|assignment| {
        assignment.role_name == "coder"
            && assignment.assignee_type.as_deref() == Some("agent")
            && assignment.assignee_id.as_deref() == Some(agent_id.as_str())
    }));

    let running_execution = single_execution_for_task(&app, &task.id).await;
    assert_eq!(running_execution.status, ExecutionStatus::Running);
    assert_eq!(
        running_execution.agent_id.as_deref(),
        Some(agent_id.as_str())
    );

    let workspace = db::WorkspaceRepo::get_by_task_id(&*state.db, &task.id)
        .await
        .expect("workspace query succeeds")
        .expect("claim created a workspace");
    std::fs::create_dir_all(&workspace.worktree_path).expect("create shell worktree");

    let completed_execution = poll_execution_status(
        &app,
        &task.id,
        &running_execution.id,
        ExecutionStatus::Completed,
    )
    .await;
    assert_eq!(completed_execution.status, ExecutionStatus::Completed);
    let snapshot = completed_execution
        .executor_config_snapshot
        .as_ref()
        .expect("execution has config snapshot");
    assert_eq!(snapshot["executor_type"], "shell");
    assert_eq!(snapshot["config"]["command"], "echo");
    assert_eq!(snapshot["config"]["args"], json!(["forge-e2e-ok"]));

    let logs = text_request(
        &app,
        Method::GET,
        &format!("/api/v1/executions/{}/logs", running_execution.id),
        StatusCode::OK,
    )
    .await;
    assert!(logs.contains("forge-e2e-ok"));

    let agent_detail = poll_agent_active(&app, &agent_id).await;
    assert_eq!(agent_detail.effective_status.as_deref(), Some("active"));
    assert_eq!(agent_detail.active_task_count, Some(0));
}

async fn test_app_with_state() -> (Router, Arc<AppState>) {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");

    let db = Arc::new(db::SqliteDb::new(pool));
    let adapter_registry = Arc::new(cli_adapters::default_registry());
    services::ensure_default_agents(db.as_ref(), &adapter_registry)
        .await
        .expect("default agents upsert");
    let event_bus = Arc::new(events::EventBus::new(16));
    let state = Arc::new(AppState::with_adapter_registry(
        db,
        event_bus,
        true,
        adapter_registry,
    ));

    let web_dist_dir =
        std::env::temp_dir().join(format!("forge-api-daemon-e2e-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&web_dist_dir).expect("create web dist dir");
    std::fs::write(web_dist_dir.join("index.html"), "<html></html>").expect("write index");

    (build_router((*state).clone(), web_dist_dir), state)
}

async fn create_project_and_repo(app: &Router) -> (String, String, TestDir) {
    let project: ProjectResponse = json_request(
        app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Daemon Onboarding E2E" }),
        StatusCode::OK,
    )
    .await;
    let repo_dir = TestDir::new("forge-daemon-repo");
    let repo_path = setup_git_repo(repo_dir.path());
    let default_branch = run_git(&repo_path, &["symbolic-ref", "--short", "HEAD"]);
    let repo: RepoResponse = json_request(
        app,
        Method::POST,
        &format!("/api/v1/projects/{}/repos", project.id),
        json!({
            "name": "forge",
            "local_path": repo_path.to_string_lossy(),
            "remote_url": repo_path.to_string_lossy(),
            "default_branch": default_branch
        }),
        StatusCode::OK,
    )
    .await;
    (project.id, repo.id, repo_dir)
}

async fn single_execution_for_task(app: &Router, task_id: &str) -> ExecutionResponse {
    let executions: PaginatedResponse<ExecutionResponse> = empty_request(
        app,
        Method::GET,
        &format!("/api/v1/tasks/{task_id}/executions"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(executions.items.len(), 1);
    executions.items.into_iter().next().unwrap()
}

async fn poll_execution_status(
    app: &Router,
    task_id: &str,
    execution_id: &str,
    expected_status: ExecutionStatus,
) -> ExecutionResponse {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let executions: PaginatedResponse<ExecutionResponse> = empty_request(
            app,
            Method::GET,
            &format!("/api/v1/tasks/{task_id}/executions"),
            StatusCode::OK,
        )
        .await;
        let execution = executions
            .items
            .into_iter()
            .find(|execution| execution.id == execution_id)
            .expect("execution remains listed for task");
        if execution.status == expected_status {
            return execution;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "execution {execution_id} did not reach {expected_status:?}; last status was {:?}",
            execution.status
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn poll_agent_active(app: &Router, agent_id: &str) -> AgentResponse {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let agent: AgentResponse = empty_request(
            app,
            Method::GET,
            &format!("/api/v1/agents/{agent_id}"),
            StatusCode::OK,
        )
        .await;
        if agent.effective_status.as_deref() == Some("active") && agent.active_task_count == Some(0)
        {
            return agent;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "agent did not return active with zero tasks; last effective_status={:?} active_task_count={:?}",
            agent.effective_status,
            agent.active_task_count
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn setup_git_repo(path: &Path) -> PathBuf {
    let repo_path = path.to_path_buf();
    run_git(&repo_path, &["init"]);
    run_git(&repo_path, &["config", "user.email", "test@forge.dev"]);
    run_git(&repo_path, &["config", "user.name", "Forge Test"]);
    std::fs::write(repo_path.join("README.md"), "# Test").expect("write README");
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
        .expect("run git command");
    assert!(
        output.status.success(),
        "git {:?} failed\nstdout: {}\nstderr: {}",
        args,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(prefix: &str) -> Self {
        let path = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn shell_cli_for_daemon<'a>(
    response: &'a CliProjectionResponse,
    daemon_id: &str,
) -> &'a CliProjectionItem {
    response
        .items
        .iter()
        .find(|item| item.daemon_id == daemon_id && item.kind == "shell")
        .expect("shell CLI projection exists for daemon")
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
                .expect("build authorized JSON request"),
        )
        .await
        .expect("router response");
    parse_response(response, expected_status).await
}

async fn empty_request<T>(app: &Router, method: Method, uri: &str, expected_status: StatusCode) -> T
where
    T: DeserializeOwned,
{
    let response = raw_empty_request(app, method, uri).await;
    parse_response(response, expected_status).await
}

async fn text_request(
    app: &Router,
    method: Method,
    uri: &str,
    expected_status: StatusCode,
) -> String {
    let response = raw_empty_request(app, method, uri).await;
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response body");
    assert_eq!(
        status,
        expected_status,
        "unexpected response status with body: {}",
        String::from_utf8_lossy(&bytes)
    );
    String::from_utf8(bytes.to_vec()).expect("response is UTF-8")
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
                .expect("build JSON request"),
        )
        .await
        .expect("router response")
}

async fn raw_empty_request(app: &Router, method: Method, uri: &str) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {}", test_jwt()))
                .body(Body::empty())
                .expect("build empty request"),
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
        .expect("read response body");
    assert_eq!(
        status,
        expected_status,
        "unexpected response status with body: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).expect("parse JSON response")
}
