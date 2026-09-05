#![allow(dead_code, clippy::assertions_on_constants)]
mod common;

use std::{
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use api::{build_router, AppState};
use api_types::{
    AgentResponse, DaemonRegisterResponse, DaemonResponse, ExecutionResponse, PaginatedResponse,
    ProjectResponse, RepoResponse, TaskResponse,
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

const EXECUTOR_SESSION_ID: &str = "33333333-3333-4333-8333-333333333333";

#[tokio::test]
async fn merge_fix_zero_blocks_first_conflict_without_follow_up() {
    let repo_dir = common::TestDir::new("forge-merge-fix-zero-repo");
    let repo_path = setup_git_repo(repo_dir.path());

    let workspaces_root = common::TestDir::new("forge-merge-fix-zero-workspaces");
    let harness = test_app(workspaces_root.path(), CompletingCodexAdapter).await;

    let project: ProjectResponse = json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({
            "name": "Merge Fix Zero",
            "settings": { "retry_budgets": { "review": 3, "merge_fix": 0 } }
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

    let task_id = db::new_uuid_v4();
    let worktree_path = workspaces_root.path().join(&task_id).join("repo");
    prepare_conflicting_worktree(&repo_path, &worktree_path, &task_id);
    seed_review_task_with_executor(
        &harness,
        &project.id,
        &repo.id,
        &task_id,
        &worktree_path,
        &executor_agent.id,
    )
    .await;

    let seeded_task = db::TaskRepo::get_by_id(&*harness.state.db, &task_id, false)
        .await
        .expect("task lookup succeeds")
        .expect("task exists");
    let transition = harness
        .state
        .task_service
        .transition(task_id.clone(), "merging".to_owned(), seeded_task.version)
        .await
        .expect("merge transition runs");
    assert_eq!(transition.task.status, "merging".to_owned());

    let task = poll_until_blocked_metadata(&harness.app, &task_id).await;
    assert_eq!(task.status, "merging".to_owned());
    let transition_logs = db::TransitionLogRepo::list_by_task(&*harness.state.db, &task_id)
        .await
        .expect("transition logs load");
    assert!(
        !transition_logs
            .iter()
            .any(|entry| entry.to_state == "merge_failed"),
        "merge_fix = 0 should block at the merge gate without dispatching merge-fix work"
    );
    let blocked = task
        .blocked
        .as_ref()
        .expect("task should have blocked metadata");
    assert!(
        matches!(
            blocked.kind,
            Some(
                api_types::FailureKind::MergeConflict
                    | api_types::FailureKind::MergeFixBudgetExhausted
            )
        ),
        "blocked metadata should record merge-fix budget exhaustion; got {blocked:?}"
    );

    let executions = executions_for_task(&harness.app, &task_id).await;
    assert!(
        executions
            .iter()
            .any(|execution| execution.role == "coder" && execution.parent_execution_id.is_none()),
        "seeded task should have an initial executor execution"
    );
    let coder_follow_ups = executions
        .iter()
        .filter(|execution| execution.role == "coder" && execution.parent_execution_id.is_some())
        .count();
    assert_eq!(
        coder_follow_ups, 0,
        "merge_fix = 0 blocks before dispatching a merge-fix follow-up"
    );
}

struct CompletingCodexAdapter;

impl CodingExecutorAdapter for CompletingCodexAdapter {
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
        _ctx: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = Result<ExecutionResult, ExecutorError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            Ok(ExecutionResult {
                status: ExecutionOutcome::Completed,
                after_sha: None,
                agent_session_id: Some("merge-follow-up-session".to_owned()),
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

struct TestHarness {
    app: Router,
    state: Arc<AppState>,
    _web_dist_dir: common::TestDir,
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

    let web_dist_dir = common::TestDir::new("forge-merge-fix-zero-web");
    std::fs::write(web_dist_dir.path().join("index.html"), "<html></html>").expect("write index");
    let app = build_router((*state).clone(), web_dist_dir.path().to_path_buf());

    TestHarness {
        app,
        state,
        _web_dist_dir: web_dist_dir,
    }
}

async fn seed_review_task_with_executor(
    harness: &TestHarness,
    project_id: &str,
    repo_id: &str,
    task_id: &str,
    worktree_path: &Path,
    agent_id: &str,
) {
    let now = db::now_rfc3339();
    let workspace_id = db::new_uuid_v4();
    db::TaskRepo::create(
        &*harness.state.db,
        db::CreateTask {
            id: task_id.to_owned(),
            project_id: project_id.to_owned(),
            repo_id: Some(repo_id.to_owned()),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Merge conflict task".to_owned(),
            description: Some("resolve a conflicting change".to_owned()),
            task_type: "task".to_owned(),
            status: "review".to_owned(),
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
    db::TaskRepo::set_review_passed_at(&*harness.state.db, task_id, Some(now.clone()), &now)
        .await
        .expect("review_passed_at sets");
    db::TaskRoleAssignmentRepo::assign(
        &*harness.state.db,
        db::CreateTaskRoleAssignment {
            id: db::new_uuid_v4(),
            task_id: task_id.to_owned(),
            role_name: "coder".to_owned(),
            assignee_type: Some(db::AssigneeKind::Agent),
            assignee_id: Some(agent_id.to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("coder role assignment creates");
    db::WorkspaceRepo::create(
        &*harness.state.db,
        db::CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task_id.to_owned(),
            repo_id: repo_id.to_owned(),
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch: ::workspace::task_branch_name(task_id),
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
            id: db::new_uuid_v4(),
            task_id: task_id.to_owned(),
            agent_id: Some(agent_id.to_owned()),
            role: "coder".to_owned(),
            status: db::ExecutionStatus::Completed,
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
            created_at: db::now_rfc3339(),
            updated_at: db::now_rfc3339(),
        },
    )
    .await
    .expect("execution creates");
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

fn prepare_conflicting_worktree(repo_path: &Path, worktree_path: &Path, task_id: &str) {
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
    std::fs::write(worktree_path.join("file.txt"), "feature\n").expect("feature writes");
    run_git(worktree_path, &["add", "-A"]);
    run_git(worktree_path, &["commit", "-m", "feature change"]);

    std::fs::write(repo_path.join("file.txt"), "main\n").expect("main writes");
    run_git(repo_path, &["add", "-A"]);
    run_git(repo_path, &["commit", "-m", "main change"]);
}

async fn register_daemon_and_report_codex(app: &Router, workspace_root: &Path) -> String {
    let registration: DaemonRegisterResponse = json_request(
        app,
        Method::POST,
        "/api/v1/daemons/register",
        json!({
            "machine_id": services::embedded_daemon::embedded_machine_id(),
            "hostname": "merge-fix-zero-host",
            "os": "linux",
            "arch": "x86_64",
            "agent_version": "merge-fix-zero-test",
            "labels": { "suite": "follow_up_merge_fix_zero" }
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

async fn poll_until_blocked_metadata(app: &Router, task_id: &str) -> TaskResponse {
    let mut last_task: Option<TaskResponse> = None;
    let start = Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(30) {
            panic!("timed out waiting for blocked metadata to be set; last task: {last_task:?}");
        }
        let task: TaskResponse = empty_request(
            app,
            Method::GET,
            &format!("/api/v1/tasks/{task_id}"),
            StatusCode::OK,
        )
        .await;
        if task.blocked.is_some() {
            return task;
        }
        last_task = Some(task);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
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
