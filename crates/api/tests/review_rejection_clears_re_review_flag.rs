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
    AgentResponse, DaemonRegisterResponse, DaemonResponse, ProjectResponse, RepoResponse,
    ReviewDecisionResponse, ReviewResponse, TaskResponse,
};
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use db::{
    new_uuid_v4, now_rfc3339, CreateExecution, CreateReview, CreateTask, CreateTaskRoleAssignment,
    CreateWorkspace, ExecutionRepo, ExecutionStatus, ReviewRepo, ReviewStatus, TaskRepo,
    TaskRoleAssignmentRepo, WorkspaceRepo, WorkspaceStatus,
};
use events::EventBus;
use executors::{
    AvailabilityInfo, AvailabilityStatus, CodingExecutorAdapter, DiscoverContext,
    DiscoveredOptions, ExecutionContext, ExecutionOutcome, ExecutionResult, ExecutorError,
    ExecutorKind, LogKind, LogStream, LogWriter,
};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tower::ServiceExt;

const EXECUTOR_SESSION_ID: &str = "66666666-6666-4666-8666-666666666666";

#[tokio::test]
async fn reviewer_rejection_clears_review_passed_at_and_next_review_runs_full_auditor() {
    let repo_dir = TestDir::new("forge-review-reject-repo");
    let repo_path = setup_git_repo(repo_dir.path());

    let workspaces_root = TestDir::new("forge-review-reject-workspaces");
    let adapter = RejectClearsFlagCodexAdapter::new();
    let harness = test_app(workspaces_root.path(), adapter).await;

    let project: ProjectResponse = json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({
            "name": "Review Rejection Clears Flag",
            "settings": { "retry_budgets": { "review": 3, "merge_fix": 1 } }
        }),
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
            "default_branch": "main"
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

    let task_id = new_uuid_v4();
    let worktree_path = workspaces_root.path().join(&task_id).join("repo");
    prepare_clean_worktree(&repo_path, &worktree_path, &task_id);
    let review_id = seed_awaiting_human_review_with_passed_flag(
        &harness,
        &project.id,
        &repo.id,
        &task_id,
        &worktree_path,
        &executor_agent.id,
        &auditor_agent.id,
    )
    .await;
    assert!(!review_id.is_empty());

    let seeded: TaskResponse = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{task_id}"),
        StatusCode::OK,
    )
    .await;
    assert!(
        seeded.review_passed_at.is_some(),
        "seeded task should start as already auditor-passed"
    );

    let rejected: ReviewDecisionResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/review/reject"),
        json!({ "reason": "needs a targeted follow-up" }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(rejected.review.status, api_types::ReviewStatus::Failed);
    assert_eq!(rejected.task.status, "in_progress".to_owned());
    assert_eq!(
        rejected.task.review_passed_at, None,
        "review rejection must clear review_passed_at"
    );
}

struct RejectClearsFlagCodexAdapter {
    executor_calls: Arc<AtomicUsize>,
    auditor_calls: Arc<AtomicUsize>,
}

impl RejectClearsFlagCodexAdapter {
    fn new() -> Self {
        Self {
            executor_calls: Arc::new(AtomicUsize::new(0)),
            auditor_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn auditor_calls(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.auditor_calls)
    }
}

impl CodingExecutorAdapter for RejectClearsFlagCodexAdapter {
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
        let auditor_calls = Arc::clone(&self.auditor_calls);
        Box::pin(async move {
            if ctx
                .description
                .contains("===REVIEW: FAIL: <short reason>===")
            {
                auditor_calls.fetch_add(1, Ordering::SeqCst);
                write_auditor_pass(&ctx).await?;
                return Ok(ExecutionResult {
                    status: ExecutionOutcome::Completed,
                    after_sha: None,
                    agent_session_id: Some("auditor-session".to_owned()),
                    summary: Some("auditor passed after reviewer rejection".to_owned()),
                    error: None,
                    usage: None,
                    ..Default::default()
                });
            }

            let call_index = executor_calls.fetch_add(1, Ordering::SeqCst);
            let worktree_path = PathBuf::from(&ctx.worktree_path);
            let file_name = format!("follow-up-{call_index}.txt");
            std::fs::write(worktree_path.join(file_name), "fixed\n")?;
            run_git(&worktree_path, &["add", "-A"]);
            run_git(
                &worktree_path,
                &["commit", "-m", "review rejection follow-up"],
            );
            Ok(ExecutionResult {
                status: ExecutionOutcome::Completed,
                after_sha: None,
                agent_session_id: Some(format!("follow-up-session-{call_index}")),
                summary: Some("coder follow-up completed".to_owned()),
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

async fn write_auditor_pass(ctx: &ExecutionContext) -> Result<(), ExecutorError> {
    if let Some(parent) = Path::new(&ctx.logs_path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut writer = LogWriter::new(&ctx.logs_path, ctx.execution_id.clone(), 1024 * 1024);
    writer
        .write(
            LogKind::Assistant,
            LogStream::Main,
            json!({ "text": "Looks good.\n===REVIEW: PASS===" }),
        )
        .await?;
    Ok(())
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

    let web_dist_dir = TestDir::new("forge-review-reject-web");
    std::fs::write(web_dist_dir.path().join("index.html"), "<html></html>").expect("write index");
    let app = build_router((*state).clone(), web_dist_dir.path().to_path_buf());

    TestHarness {
        app,
        state,
        _web_dist_dir: web_dist_dir,
    }
}

async fn seed_awaiting_human_review_with_passed_flag(
    harness: &TestHarness,
    project_id: &str,
    repo_id: &str,
    task_id: &str,
    worktree_path: &Path,
    executor_agent_id: &str,
    auditor_agent_id: &str,
) -> String {
    let now = now_rfc3339();
    let workspace_id = new_uuid_v4();
    let execution_id = new_uuid_v4();
    let review_id = new_uuid_v4();

    TaskRepo::create(
        &*harness.state.db,
        CreateTask {
            id: task_id.to_owned(),
            project_id: project_id.to_owned(),
            repo_id: Some(repo_id.to_owned()),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Reviewer rejection task".to_owned(),
            description: Some("exercise reviewer rejection".to_owned()),
            task_type: "task".to_owned(),
            status: "review".to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: Some(
                json!({
                    "review": {
                        "auditor_agent_id": auditor_agent_id,
                        "review_prompt": "Pass after the reviewer rejection.",
                        "ci_steps": ["true"]
                    }
                })
                .to_string(),
            ),
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task creates");
    TaskRepo::set_review_passed_at(&*harness.state.db, task_id, Some(now.clone()), &now)
        .await
        .expect("review_passed_at sets");
    TaskRoleAssignmentRepo::assign(
        &*harness.state.db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            role_name: "coder".to_owned(),
            assignee_type: Some(db::AssigneeKind::Agent),
            assignee_id: Some(executor_agent_id.to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("coder role assignment creates");
    WorkspaceRepo::create(
        &*harness.state.db,
        CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task_id.to_owned(),
            repo_id: repo_id.to_owned(),
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch: ::workspace::task_branch_name(task_id),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("workspace creates");
    ExecutionRepo::create(
        &*harness.state.db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task_id.to_owned(),
            agent_id: Some(executor_agent_id.to_owned()),
            role: "coder".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some(EXECUTOR_SESSION_ID.to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("initial executor completed".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                json!({
                    "executor_type": "codex",
                    "config": {},
                    "capabilities": [],
                    "overrides_applied": { "profile": [], "agent": [], "execution": [] },
                    "snapshotted_at": now
                })
                .to_string(),
            ),
            workspace_id: Some(workspace_id),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("execution creates");
    ReviewRepo::create(
        &*harness.state.db,
        CreateReview {
            id: review_id.clone(),
            task_id: task_id.to_owned(),
            execution_id,
            attempt_number: 1,
            status: ReviewStatus::AwaitingHuman,
            step_results_json: json!({
                "auditor": { "verdict": "pass" },
                "ci_steps": []
            })
            .to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("review creates");
    review_id
}

fn setup_git_repo(path: &Path) -> PathBuf {
    let repo_path = path.join("repo");
    std::fs::create_dir_all(&repo_path).expect("repo dir creates");
    run_git(&repo_path, &["init"]);
    run_git(&repo_path, &["checkout", "-B", "main"]);
    run_git(&repo_path, &["config", "user.email", "test@forge.dev"]);
    run_git(&repo_path, &["config", "user.name", "Forge Test"]);
    std::fs::write(repo_path.join("file.txt"), "base\n").expect("file writes");
    run_git(&repo_path, &["add", "-A"]);
    run_git(&repo_path, &["commit", "-m", "initial commit"]);
    repo_path
}

fn prepare_clean_worktree(repo_path: &Path, worktree_path: &Path, task_id: &str) {
    let branch = ::workspace::task_branch_name(task_id);
    let parent = worktree_path.parent().expect("worktree path has parent");
    std::fs::create_dir_all(parent).expect("worktree parent creates");
    run_git(
        repo_path,
        &[
            "worktree",
            "add",
            "-b",
            branch.as_str(),
            worktree_path.to_str().expect("worktree path is UTF-8"),
            "main",
        ],
    );
}

async fn register_daemon_and_report_codex(app: &Router, workspace_root: &Path) -> String {
    let registration: DaemonRegisterResponse = json_request(
        app,
        Method::POST,
        "/api/v1/daemons/register",
        json!({
            "machine_id": services::embedded_daemon::embedded_machine_id(),
            "hostname": "review-reject-host",
            "os": "linux",
            "arch": "x86_64",
            "agent_version": "review-reject-test",
            "labels": { "suite": "review_rejection_clears_re_review_flag" }
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

async fn poll_until_auditor_calls(calls: &Arc<AtomicUsize>, expected: usize) {
    for _ in 0..200 {
        if calls.load(Ordering::SeqCst) >= expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "auditor execution count did not reach {expected}; got {}",
        calls.load(Ordering::SeqCst)
    );
}

async fn poll_until_latest_review_auditor_verdict(
    app: &Router,
    task_id: &str,
    expected_verdict: &str,
) -> ReviewResponse {
    let mut last_reviews = Vec::new();
    for _ in 0..200 {
        let reviews: Vec<ReviewResponse> = empty_request(
            app,
            Method::GET,
            &format!("/api/v1/tasks/{task_id}/reviews"),
            StatusCode::OK,
        )
        .await;
        if let Some(latest_review) = reviews.iter().max_by_key(|review| review.attempt_number) {
            if latest_review
                .details
                .auditor
                .as_ref()
                .is_some_and(|auditor| auditor.verdict == expected_verdict)
            {
                return latest_review.clone();
            }
        }
        last_reviews = reviews;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "task did not record latest review auditor verdict {expected_verdict:?} within timeout; last reviews: {last_reviews:?}"
    );
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
