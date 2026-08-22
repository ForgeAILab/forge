#![allow(dead_code, clippy::assertions_on_constants)]
mod common;

use std::{
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
};

use api::{build_router, AppState};
use api_types::{
    AgentResponse, DaemonRegisterResponse, DaemonResponse, ErrorResponse, LaunchExecutionResponse,
    ProjectResponse, RepoResponse,
};
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use events::EventBus;
use executors::{
    AvailabilityInfo, AvailabilityStatus, CodingExecutorAdapter, DiscoverContext,
    DiscoveredOptions, ExecutionContext, ExecutionOutcome, ExecutionResult, ExecutorError,
    ExecutorKind,
};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tower::ServiceExt;

const PARENT_SESSION_ID: &str = "test-session-123";

#[tokio::test]
async fn follow_up_execution_happy_path_returns_task_execution_and_workspace() {
    let repo_dir = TestDir::new("forge-follow-up-chat-repo");
    let repo_path = setup_git_repo(repo_dir.path()).await;
    let default_branch = run_git(&repo_path, &["symbolic-ref", "--short", "HEAD"]);

    let workspaces_root = TestDir::new("forge-follow-up-chat-workspaces");
    let harness = test_app(workspaces_root.path(), CompletingShellAdapter).await;

    let project: ProjectResponse = json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Follow Up Chat" }),
        StatusCode::OK,
    )
    .await;
    let repo: RepoResponse = json_request(
        &harness.app,
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

    let daemon_id = register_daemon_and_report_shell(&harness.app, workspaces_root.path()).await;
    let agent = create_agent(&harness.app, "shell-agent", &daemon_id).await;
    common::configure_execution_test_setup(
        &harness.state.db,
        &project.id,
        &repo.id,
        &agent.id,
        &agent.id,
    )
    .await;

    let seeded = seed_parent_execution(
        &harness,
        &project.id,
        &repo.id,
        &agent.id,
        "in_progress".to_owned(),
        db::ExecutionStatus::Completed,
        Some(PARENT_SESSION_ID),
    )
    .await;

    let response: LaunchExecutionResponse = json_request(
        &harness.app,
        Method::POST,
        &format!(
            "/api/v1/executions/{}/follow-up",
            seeded.parent_execution_id
        ),
        json!({ "message": "do more work" }),
        StatusCode::OK,
    )
    .await;

    assert_eq!(response.data.task.id, seeded.task_id);
    assert_eq!(response.data.execution.role, "interactive");
    assert_eq!(
        response.data.execution.parent_execution_id.as_deref(),
        Some(seeded.parent_execution_id.as_str())
    );
    assert_eq!(
        response.data.execution.summary.as_deref(),
        Some("do more work")
    );
    assert_eq!(
        response.data.execution.agent_id.as_deref(),
        Some(agent.id.as_str())
    );
    assert_eq!(
        response.data.workspace.id, seeded.workspace_id,
        "follow-up returns workspace payload"
    );
}

#[tokio::test]
async fn follow_up_execution_rejects_running_parent_with_conflict_code() {
    let repo_dir = TestDir::new("forge-follow-up-chat-running-repo");
    let repo_path = setup_git_repo(repo_dir.path()).await;
    let default_branch = run_git(&repo_path, &["symbolic-ref", "--short", "HEAD"]);

    let workspaces_root = TestDir::new("forge-follow-up-chat-running-workspaces");
    let harness = test_app(workspaces_root.path(), CompletingShellAdapter).await;

    let project: ProjectResponse = json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Follow Up Running" }),
        StatusCode::OK,
    )
    .await;
    let repo: RepoResponse = json_request(
        &harness.app,
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

    let daemon_id = register_daemon_and_report_shell(&harness.app, workspaces_root.path()).await;
    let agent = create_agent(&harness.app, "shell-agent", &daemon_id).await;

    let seeded = seed_parent_execution(
        &harness,
        &project.id,
        &repo.id,
        &agent.id,
        "in_progress".to_owned(),
        db::ExecutionStatus::Running,
        Some(PARENT_SESSION_ID),
    )
    .await;

    let error: ErrorResponse = json_request(
        &harness.app,
        Method::POST,
        &format!(
            "/api/v1/executions/{}/follow-up",
            seeded.parent_execution_id
        ),
        json!({ "message": "do more work" }),
        StatusCode::CONFLICT,
    )
    .await;

    assert_eq!(error.code, "follow_up.execution_active");
}

#[tokio::test]
async fn follow_up_execution_rejects_missing_session_with_conflict_code() {
    let repo_dir = TestDir::new("forge-follow-up-chat-no-session-repo");
    let repo_path = setup_git_repo(repo_dir.path()).await;
    let default_branch = run_git(&repo_path, &["symbolic-ref", "--short", "HEAD"]);

    let workspaces_root = TestDir::new("forge-follow-up-chat-no-session-workspaces");
    let harness = test_app(workspaces_root.path(), CompletingShellAdapter).await;

    let project: ProjectResponse = json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Follow Up No Session" }),
        StatusCode::OK,
    )
    .await;
    let repo: RepoResponse = json_request(
        &harness.app,
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

    let daemon_id = register_daemon_and_report_shell(&harness.app, workspaces_root.path()).await;
    let agent = create_agent(&harness.app, "shell-agent", &daemon_id).await;

    let seeded = seed_parent_execution(
        &harness,
        &project.id,
        &repo.id,
        &agent.id,
        "in_progress".to_owned(),
        db::ExecutionStatus::Completed,
        None,
    )
    .await;

    let error: ErrorResponse = json_request(
        &harness.app,
        Method::POST,
        &format!(
            "/api/v1/executions/{}/follow-up",
            seeded.parent_execution_id
        ),
        json!({ "message": "do more work" }),
        StatusCode::CONFLICT,
    )
    .await;

    assert_eq!(error.code, "follow_up.no_session");
}

#[tokio::test]
async fn follow_up_execution_rejects_terminal_task_with_invalid_operation_code() {
    let repo_dir = TestDir::new("forge-follow-up-chat-terminal-repo");
    let repo_path = setup_git_repo(repo_dir.path()).await;
    let default_branch = run_git(&repo_path, &["symbolic-ref", "--short", "HEAD"]);

    let workspaces_root = TestDir::new("forge-follow-up-chat-terminal-workspaces");
    let harness = test_app(workspaces_root.path(), CompletingShellAdapter).await;

    let project: ProjectResponse = json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Follow Up Terminal" }),
        StatusCode::OK,
    )
    .await;
    let repo: RepoResponse = json_request(
        &harness.app,
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

    let daemon_id = register_daemon_and_report_shell(&harness.app, workspaces_root.path()).await;
    let agent = create_agent(&harness.app, "shell-agent", &daemon_id).await;

    let seeded = seed_parent_execution(
        &harness,
        &project.id,
        &repo.id,
        &agent.id,
        "done".to_owned(),
        db::ExecutionStatus::Completed,
        Some(PARENT_SESSION_ID),
    )
    .await;

    let error: ErrorResponse = json_request(
        &harness.app,
        Method::POST,
        &format!(
            "/api/v1/executions/{}/follow-up",
            seeded.parent_execution_id
        ),
        json!({ "message": "do more work" }),
        StatusCode::CONFLICT,
    )
    .await;

    assert_eq!(error.code, "task.terminal");
}

struct CompletingShellAdapter;

impl CodingExecutorAdapter for CompletingShellAdapter {
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Shell
    }

    fn check_availability(&self) -> AvailabilityInfo {
        AvailabilityInfo {
            status: AvailabilityStatus::Authenticated,
            authenticated_at: None,
            config_path: None,
        }
    }

    fn discover_options<'life0, 'async_trait>(
        &'life0 self,
        _ctx: DiscoverContext,
    ) -> Pin<Box<dyn Future<Output = Result<DiscoveredOptions, ExecutorError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async { Ok(DiscoveredOptions::default()) })
    }

    fn execute<'life0, 'async_trait>(
        &'life0 self,
        _ctx: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = Result<ExecutionResult, ExecutorError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async {
            Ok(ExecutionResult {
                status: ExecutionOutcome::Completed,
                after_sha: None,
                agent_session_id: Some("interactive-follow-up-session".to_owned()),
                summary: Some("interactive execution completed".to_owned()),
                error: None,
                usage: None,
                ..Default::default()
            })
        })
    }

    fn cancel<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _execution_id: &'life1 str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExecutorError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async { Ok(()) })
    }
}

struct TestHarness {
    app: Router,
    state: Arc<AppState>,
    _web_dist_dir: TestDir,
}

async fn test_app(
    workspace_root: &Path,
    adapter: impl CodingExecutorAdapter + 'static,
) -> TestHarness {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");

    let db = Arc::new(db::SqliteDb::new(pool));
    let mut registry = executors::AdapterRegistry::new();
    registry.register(Box::new(adapter));
    let adapter_registry = Arc::new(registry);
    services::ensure_default_agents(db.as_ref(), &adapter_registry)
        .await
        .expect("default agents upsert");
    let event_bus = Arc::new(EventBus::new(256));
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

    let web_dist_dir = TestDir::new("forge-follow-up-chat-web");
    std::fs::write(web_dist_dir.path().join("index.html"), "<html></html>").expect("write index");
    let app = build_router((*state).clone(), web_dist_dir.path().to_path_buf());

    TestHarness {
        app,
        state,
        _web_dist_dir: web_dist_dir,
    }
}

struct SeededParent {
    task_id: String,
    workspace_id: String,
    parent_execution_id: String,
}

async fn seed_parent_execution(
    harness: &TestHarness,
    project_id: &str,
    repo_id: &str,
    agent_id: &str,
    task_status: db::TaskStatus,
    parent_execution_status: db::ExecutionStatus,
    parent_agent_session_id: Option<&str>,
) -> SeededParent {
    let now = db::now_rfc3339();
    let task_id = db::new_uuid_v4();
    let workspace_id = db::new_uuid_v4();
    let parent_execution_id = db::new_uuid_v4();

    db::TaskRepo::create(
        &*harness.state.db,
        db::CreateTask {
            id: task_id.clone(),
            project_id: project_id.to_owned(),
            repo_id: Some(repo_id.to_owned()),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: Some("agent".to_owned()),
            assignee_id: Some(agent_id.to_owned()),
            title: "Follow-up task".to_owned(),
            description: Some("execute follow-up".to_owned()),
            task_type: "task".to_owned(),
            status: task_status,
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task creates");

    let worktree_path = std::env::temp_dir()
        .join("forge-follow-up-chat-worktree")
        .join(&task_id)
        .join("repo");
    std::fs::create_dir_all(&worktree_path).expect("worktree path creates");
    db::WorkspaceRepo::create(
        &*harness.state.db,
        db::CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task_id.clone(),
            repo_id: repo_id.to_owned(),
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch: ::workspace::task_branch_name(&task_id),
            status: db::WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("workspace creates");

    db::ExecutionRepo::create(
        &*harness.state.db,
        db::CreateExecution {
            id: parent_execution_id.clone(),
            task_id: task_id.clone(),
            agent_id: Some(agent_id.to_owned()),
            role: "coder".to_owned(),
            status: parent_execution_status,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: parent_agent_session_id.map(str::to_owned),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("parent execution".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: Some(workspace_id.clone()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");

    SeededParent {
        task_id,
        workspace_id,
        parent_execution_id,
    }
}

async fn register_daemon_and_report_shell(app: &Router, workspace_root: &Path) -> String {
    let registration: DaemonRegisterResponse = json_request(
        app,
        Method::POST,
        "/api/v1/daemons/register",
        json!({
            "machine_id": services::embedded_daemon::embedded_machine_id(),
            "hostname": "follow-up-chat-host",
            "os": "linux",
            "arch": "x86_64",
            "agent_version": "follow-up-chat-test",
            "labels": { "suite": "follow_up_chat" }
        }),
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
            "detected_clis": [{
                "kind": "shell",
                "availability": "authenticated",
                "path": "/bin/sh"
            }],
            "runtimes": [{
                "kind": "local",
                "workspace_root": workspace_root.to_string_lossy(),
                "status": "ready"
            }]
        }),
        StatusCode::OK,
    )
    .await;

    daemon_id
}

async fn create_agent(app: &Router, name: &str, daemon_id: &str) -> AgentResponse {
    let agent: AgentResponse = json_request(
        app,
        Method::POST,
        "/api/v1/agents",
        json!({ "name": name, "executor_type": "shell", "daemon_id": daemon_id }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(agent.effective_status.as_deref(), Some("active"));
    agent
}

async fn setup_git_repo(path: &Path) -> PathBuf {
    let repo_path = path.join("repo");
    std::fs::create_dir_all(&repo_path).expect("repo dir creates");
    run_git(&repo_path, &["init"]);
    run_git(&repo_path, &["config", "user.email", "test@forge.dev"]);
    run_git(&repo_path, &["config", "user.name", "Forge Test"]);
    std::fs::write(repo_path.join("README.md"), "# Follow Up Chat\n").expect("README writes");
    run_git(&repo_path, &["add", "-A"]);
    run_git(&repo_path, &["commit", "-m", "initial commit"]);
    repo_path
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

fn run_git(path: &Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .expect("git command runs");
    assert!(
        output.status.success(),
        "git {} failed\nstdout: {}\nstderr: {}",
        args.join(" "),
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
        std::fs::create_dir_all(&path).expect("temp dir creates");
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
