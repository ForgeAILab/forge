#![allow(dead_code, clippy::assertions_on_constants)]
mod common;

use std::{
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use api::{build_router, AppState};
use api_types::{
    AgentResponse, DaemonRegisterResponse, DaemonResponse, ExecutionResponse, ExecutionStatus,
    PaginatedResponse, ProjectResponse, RepoResponse, TaskResponse,
};
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use events::{EventBus, EventContext, ForgeEvent};
use executors::{
    AvailabilityInfo, AvailabilityStatus, CodingExecutorAdapter, DiscoverContext,
    DiscoveredOptions, ExecutionContext, ExecutionOutcome, ExecutionResult, ExecutorError,
    ExecutorKind, LogKind, LogStream, LogWriter,
};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tower::ServiceExt;

const FIRST_EXECUTOR_SESSION_ID: &str = "11111111-1111-4111-8111-111111111111";
const REVIEW_FAIL_REASON: &str = "missing error handling";

#[tokio::test]
async fn auditor_failure_dispatches_follow_up_executor_with_thread_reuse() {
    let repo_dir = TestDir::new("forge-review-fail-repo");
    let repo_path = setup_git_repo(repo_dir.path()).await;
    let default_branch = run_git(&repo_path, &["symbolic-ref", "--short", "HEAD"]);

    let workspaces_root = TestDir::new("forge-review-fail-workspaces");
    let harness = test_app(workspaces_root.path(), ReviewFailCodexAdapter::new()).await;
    let mut events_rx = harness.event_bus.subscribe();

    let project: ProjectResponse = json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Review Fail" }),
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
    assert!(repo.local_path.is_some());

    let daemon_id = register_daemon_and_report_codex(&harness.app, workspaces_root.path()).await;
    let executor_agent = create_agent(&harness.app, "executor", &daemon_id).await;
    let auditor_agent = create_agent(&harness.app, "auditor", &daemon_id).await;
    common::configure_execution_test_setup(
        &harness.state.db,
        &project.id,
        &repo.id,
        &executor_agent.id,
        &auditor_agent.id,
    )
    .await;

    let created_task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{}/tasks", project.id),
        json!({ "title": "Review fail task",
            "description": "make a small implementation",
            "role_assignments": [{"role_name": "reviewer", "assignee_type": "agent", "assignee_id": auditor_agent.id}],
            "review_config": {
                "review_prompt": "Always fail with the requested reason."
            }
        }),
        StatusCode::OK,
    )
    .await;
    set_auditor_review_config(&harness, &created_task.id, &auditor_agent.id).await;

    let claimed: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/claim", created_task.id),
        json!({ "agent_id": executor_agent.id, "overrides": null }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(claimed.status, "in_progress".to_owned());

    let initial_executions = poll_until_executor_attempts(&harness.app, &created_task.id, 1).await;
    let first_executor = initial_executions
        .iter()
        .find(|execution| execution.role == "coder" && execution.parent_execution_id.is_none())
        .expect("initial executor execution exists");
    let first_executor_id = first_executor.id.clone();
    let reviewer_execution = initial_executions
        .iter()
        .find(|execution| execution.role == "reviewer")
        .expect("reviewer execution exists");
    assert_eq!(reviewer_execution.role, "reviewer");

    let executions = poll_until_executor_attempts(&harness.app, &created_task.id, 2).await;
    let follow_up_execution = executions
        .iter()
        .find(|execution| {
            execution.role == "coder"
                && execution.parent_execution_id.is_some()
                && execution
                    .executor_config_snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot["config"]["resume_thread_id"].as_str())
                    == Some(FIRST_EXECUTOR_SESSION_ID)
        })
        .expect("coder follow-up execution with session continuity exists");
    assert_eq!(follow_up_execution.status, ExecutionStatus::Running);
    let follow_up_snapshot = follow_up_execution
        .executor_config_snapshot
        .as_ref()
        .expect("follow-up execution records config snapshot");
    assert_eq!(
        follow_up_snapshot["config"]["resume_thread_id"], FIRST_EXECUTOR_SESSION_ID,
        "coder follow-up resumes the initial executor thread"
    );
    assert_eq!(follow_up_snapshot["config"]["resume_thread_in_place"], true);
    assert!(
        follow_up_snapshot["config"]
            .get("resume_fallback_prompt")
            .is_none(),
        "execution follow-up must not carry a reconstructed fallback prompt"
    );
    let follow_up_prompt = follow_up_execution.summary.as_deref().unwrap_or_default();
    assert!(
        follow_up_prompt.contains("flagged the previous implementation"),
        "review follow-up should send only focused remediation text: {follow_up_prompt}"
    );
    assert!(
        !follow_up_prompt.contains("Implementation objective:")
            && !follow_up_prompt.contains("Make the requested code changes in the worktree"),
        "review follow-up prompt should not rebuild the initial coder prompt: {follow_up_prompt}"
    );

    let task: TaskResponse = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", created_task.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(task.status, "in_progress".to_owned());

    let events = drain_events(&mut events_rx).await;
    let follow_up_events = events
        .iter()
        .filter(|event| {
            matches!(
                &event.context,
                EventContext::FollowUpDispatched { task_id, trigger, .. }
                    if task_id == &created_task.id && trigger == "review_failed"
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        follow_up_events.len(),
        1,
        "workflow-hook auditor failures should emit one follow-up dispatch event; got {events:?}"
    );
    match &follow_up_events[0].context {
        EventContext::FollowUpDispatched {
            parent_execution_id,
            execution_id,
            ..
        } => {
            assert_eq!(parent_execution_id, &first_executor_id);
            assert_ne!(
                execution_id, parent_execution_id,
                "follow-up creates a new execution"
            );
        }
        other => panic!("unexpected follow-up event context: {other:?}"),
    }
}

struct ReviewFailCodexAdapter {
    executor_calls: Arc<AtomicUsize>,
}

impl ReviewFailCodexAdapter {
    fn new() -> Self {
        Self {
            executor_calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl CodingExecutorAdapter for ReviewFailCodexAdapter {
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Codex
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
        ctx: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = Result<ExecutionResult, ExecutorError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let executor_calls = Arc::clone(&self.executor_calls);
        Box::pin(async move {
            if ctx
                .description
                .contains("===REVIEW: FAIL: <short reason>===")
            {
                write_auditor_failure(&ctx).await?;
                return Ok(ExecutionResult {
                    status: ExecutionOutcome::Completed,
                    after_sha: None,
                    agent_session_id: Some("auditor-session".to_owned()),
                    summary: Some("auditor failed the implementation".to_owned()),
                    error: None,
                    usage: None,
                    ..Default::default()
                });
            }

            let call_index = executor_calls.fetch_add(1, Ordering::SeqCst);
            if call_index == 1 {
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            let session_id = if call_index == 0 {
                FIRST_EXECUTOR_SESSION_ID.to_owned()
            } else {
                format!("follow-up-session-{call_index}")
            };
            Ok(ExecutionResult {
                status: ExecutionOutcome::Completed,
                after_sha: None,
                agent_session_id: Some(session_id),
                summary: Some("executor completed".to_owned()),
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

async fn write_auditor_failure(ctx: &ExecutionContext) -> Result<(), ExecutorError> {
    if let Some(parent) = Path::new(&ctx.logs_path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut writer = LogWriter::new(&ctx.logs_path, ctx.execution_id.clone(), 1024 * 1024);
    writer
        .write(
            LogKind::Assistant,
            LogStream::Main,
            json!({ "text": format!("No.\n===REVIEW: FAIL: {REVIEW_FAIL_REASON}===") }),
        )
        .await?;
    Ok(())
}

struct TestHarness {
    app: Router,
    state: Arc<AppState>,
    event_bus: Arc<EventBus>,
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
        Arc::clone(&event_bus),
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

    let web_dist_dir = TestDir::new("forge-review-fail-web");
    std::fs::write(web_dist_dir.path().join("index.html"), "<html></html>").expect("write index");
    let app = build_router((*state).clone(), web_dist_dir.path().to_path_buf());

    TestHarness {
        app,
        state,
        event_bus,
        _web_dist_dir: web_dist_dir,
    }
}

async fn setup_git_repo(path: &Path) -> PathBuf {
    let repo_path = path.join("repo");
    std::fs::create_dir_all(&repo_path).expect("repo dir creates");
    run_git(&repo_path, &["init"]);
    run_git(&repo_path, &["config", "user.email", "test@forge.dev"]);
    run_git(&repo_path, &["config", "user.name", "Forge Test"]);
    std::fs::write(repo_path.join("README.md"), "# Review Fail\n").expect("README writes");
    run_git(&repo_path, &["add", "-A"]);
    run_git(&repo_path, &["commit", "-m", "initial commit"]);
    repo_path
}

async fn register_daemon_and_report_codex(app: &Router, workspace_root: &Path) -> String {
    let registration: DaemonRegisterResponse = json_request(
        app,
        Method::POST,
        "/api/v1/daemons/register",
        json!({
            "machine_id": services::embedded_daemon::embedded_machine_id(),
            "hostname": "review-fail-host",
            "os": "linux",
            "arch": "x86_64",
            "agent_version": "review-fail-test",
            "labels": { "suite": "follow_up_review_fail" }
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
                "kind": "codex",
                "availability": "authenticated",
                "path": "/bin/codex"
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
        json!({ "name": name, "executor_type": "codex", "daemon_id": daemon_id }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(agent.effective_status.as_deref(), Some("active"));
    agent
}

async fn set_auditor_review_config(harness: &TestHarness, task_id: &str, auditor_agent_id: &str) {
    let config = serde_json::to_string(&json!({
        "review": {
            "auditor_agent_id": auditor_agent_id,
            "review_prompt": "Always fail with the requested reason."
        }
    }))
    .expect("review config serializes");
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(config)
        .bind(task_id)
        .execute(harness.state.db.pool())
        .await
        .expect("task review config updates");
}

async fn poll_until_executor_attempts(
    app: &Router,
    task_id: &str,
    expected_executor_count: usize,
) -> Vec<ExecutionResponse> {
    for _ in 0..100 {
        let executions = executions_for_task(app, task_id).await;
        let executor_count = executions
            .iter()
            .filter(|execution| execution.role == "coder")
            .count();
        if executor_count >= expected_executor_count
            && executions
                .iter()
                .any(|execution| execution.role == "reviewer")
        {
            return executions;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("task did not reach {expected_executor_count} executor attempts within timeout");
}

async fn poll_until_reused_execution_running(
    app: &Router,
    task_id: &str,
    role: &str,
    execution_id: &str,
) -> Vec<ExecutionResponse> {
    let mut last = Vec::new();
    for _ in 0..100 {
        let executions = executions_for_task(app, task_id).await;
        last = executions.clone();
        if executions.iter().any(|execution| {
            execution.role == role
                && execution.id == execution_id
                && execution.status == ExecutionStatus::Running
                && execution
                    .executor_config_snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot["config"]["resume_thread_id"].as_str())
                    == Some(FIRST_EXECUTOR_SESSION_ID)
        }) {
            return executions;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "task did not resume {role} execution {execution_id} within timeout; executions: {last:?}"
    );
}

async fn executions_for_task(app: &Router, task_id: &str) -> Vec<ExecutionResponse> {
    let executions: PaginatedResponse<ExecutionResponse> = empty_request(
        app,
        Method::GET,
        &format!("/api/v1/tasks/{task_id}/executions?limit=20"),
        StatusCode::OK,
    )
    .await;
    executions.items
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
