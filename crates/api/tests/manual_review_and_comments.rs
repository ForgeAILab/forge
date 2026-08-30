#![allow(dead_code, clippy::assertions_on_constants)]
use std::{
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use api::{build_router, AppState};
use api_types::{
    AuthorType, CommentResponse, PaginatedResponse, ProjectResponse, RepoResponse,
    ReviewDecisionResponse, TaskResponse,
};
use axum::{
    body::{to_bytes, Body},
    http::{Method, Request, StatusCode},
    Router,
};
use db::{
    new_uuid_v4, now_rfc3339, AgentRepo, AgentStatus, CommentAuthorType, CreateAgent,
    CreateExecution, CreateProject, CreateRepo, CreateReview, CreateTask, CreateTaskComment,
    CreateWorkspace, DaemonRepo, DaemonStatus, ExecutionRepo, ExecutionStatus, ProjectRepo,
    RepoRepo, ReviewRepo, ReviewStatus, TaskCommentRepo, TaskRepo, UpdateProject, UpsertDaemon,
    WorkspaceStatus,
};
use events::EventBus;
use executors::{
    AvailabilityInfo, AvailabilityStatus, CodingExecutorAdapter, DiscoverContext,
    DiscoveredOptions, ExecutionContext, ExecutionOutcome, ExecutionResult, ExecutorError,
    ExecutorKind,
};
use serde::de::DeserializeOwned;
use serde_json::json;
use std::process::Stdio;
use tower::ServiceExt;

// ── Test 13.1: ReviewConfig serialization ──

#[test]
fn review_config_serializes_without_auditor_agent_id() {
    use api_types::ReviewConfig;

    let config = ReviewConfig {
        ci_steps: vec!["cargo test".to_owned()],
        review_prompt: Some("Check for correctness".to_owned()),
    };
    let json_str = serde_json::to_string(&config).expect("serializes");
    let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("parses");

    assert_eq!(parsed["ci_steps"][0], "cargo test");
    assert_eq!(parsed["review_prompt"], "Check for correctness");
    assert!(
        parsed.get("auditor_agent_id").is_none(),
        "auditor_agent_id should not be present"
    );

    let deserialized: ReviewConfig = serde_json::from_str(&json_str).expect("deserializes");
    assert_eq!(deserialized.ci_steps, vec!["cargo test".to_owned()]);
    assert_eq!(
        deserialized.review_prompt,
        Some("Check for correctness".to_owned())
    );

    let empty = ReviewConfig {
        ci_steps: vec![],
        review_prompt: None,
    };
    let json_str = serde_json::to_string(&empty).expect("serializes");
    let deserialized: ReviewConfig = serde_json::from_str(&json_str).expect("deserializes");
    assert!(deserialized.ci_steps.is_empty());
    assert_eq!(deserialized.review_prompt, None);
}

// ── Test 13.5: reject_review bounces to in_progress ──

#[tokio::test]
async fn reject_review_bounces_to_in_progress() {
    let workspace_root = TestDir::new("forge-manual-review-reject");
    let harness = test_app(workspace_root.path()).await;

    let (task_id, _review_id) = seed_awaiting_human_review(
        &harness.state.db,
        r#"{"retry_budgets":{"review":3,"merge_fix":1}}"#,
        workspace_root.path(),
    )
    .await;

    let result: ReviewDecisionResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/review/reject"),
        json!({ "reason": "bad code quality" }),
        StatusCode::OK,
    )
    .await;

    assert_eq!(result.review.status, api_types::ReviewStatus::Failed);
    assert_eq!(result.task.status, "in_progress".to_owned());
}

#[tokio::test]
async fn reject_review_without_reason_uses_default() {
    let workspace_root = TestDir::new("forge-manual-review-reject-default");
    let harness = test_app(workspace_root.path()).await;

    let (task_id, _review_id) = seed_awaiting_human_review(
        &harness.state.db,
        r#"{"retry_budgets":{"review":3,"merge_fix":1}}"#,
        workspace_root.path(),
    )
    .await;

    let result: ReviewDecisionResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/review/reject"),
        json!({}),
        StatusCode::OK,
    )
    .await;

    assert_eq!(result.review.status, api_types::ReviewStatus::Failed);
    assert_eq!(result.task.status, "in_progress".to_owned());
}

#[tokio::test]
async fn approve_review_returns_409_when_not_awaiting_human() {
    let workspace_root = TestDir::new("forge-manual-review-409");
    let harness = test_app(workspace_root.path()).await;

    let task_id = seed_review_with_status(&harness.state.db, ReviewStatus::Passed).await;

    let response = raw_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/review/approve"),
        json!({}),
    )
    .await;

    // Returns 409 because the review status doesn't match the expected state
    assert_eq!(response.status(), StatusCode::CONFLICT);
}

// ── Test 13.6: comment CRUD ──

#[tokio::test]
async fn comment_create_list_delete() {
    let workspace_root = TestDir::new("forge-comments-crud");
    let harness = test_app(workspace_root.path()).await;
    let (repo_path, _) = setup_git_repo(workspace_root.path(), "comments");

    let project: ProjectResponse = json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Comment Test" }),
        StatusCode::OK,
    )
    .await;

    let _repo: RepoResponse = json_request(
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

    let task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{}/tasks", project.id),
        json!({ "title": "Comment test task",
            "description": "test"
        }),
        StatusCode::OK,
    )
    .await;

    // Create a user comment
    let comment: CommentResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/comments", task.id),
        json!({ "content": "Hello world", "author_name": "Tester" }),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(comment.content, "Hello world");
    assert_eq!(comment.author_name, "Tester");
    assert_eq!(comment.author_type, AuthorType::User);

    // List comments
    let listed: PaginatedResponse<CommentResponse> = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/comments", task.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(listed.items.len(), 1);
    assert_eq!(listed.items[0].content, "Hello world");

    // Delete user comment
    let delete_response = raw_request(
        &harness.app,
        Method::DELETE,
        &format!("/api/v1/comments/{}", comment.id),
        json!(null),
    )
    .await;
    assert_eq!(delete_response.status(), StatusCode::NO_CONTENT);

    // Verify it's gone
    let listed: PaginatedResponse<CommentResponse> = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/comments", task.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(listed.items.len(), 0);
}

#[tokio::test]
async fn system_comments_cannot_be_deleted() {
    let workspace_root = TestDir::new("forge-comments-system-delete");
    let harness = test_app(workspace_root.path()).await;

    let task_id = seed_task_with_system_comment(&harness.state.db).await;

    let listed: PaginatedResponse<CommentResponse> = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{task_id}/comments"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(listed.items.len(), 1);
    let system_comment_id = &listed.items[0].id;

    let delete_response = raw_request(
        &harness.app,
        Method::DELETE,
        &format!("/api/v1/comments/{system_comment_id}"),
        json!(null),
    )
    .await;
    assert_eq!(delete_response.status(), StatusCode::FORBIDDEN);
}

// ── Test 13.7: system comments auto-created ──

#[tokio::test]
async fn reject_review_creates_system_comment_with_reason() {
    let workspace_root = TestDir::new("forge-sys-comment-reject");
    let harness = test_app(workspace_root.path()).await;

    let (task_id, _review_id) = seed_awaiting_human_review(
        &harness.state.db,
        r#"{"retry_budgets":{"review":3,"merge_fix":1}}"#,
        workspace_root.path(),
    )
    .await;

    let _result: ReviewDecisionResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/review/reject"),
        json!({ "reason": "needs refactoring" }),
        StatusCode::OK,
    )
    .await;

    let comments: PaginatedResponse<CommentResponse> = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{task_id}/comments"),
        StatusCode::OK,
    )
    .await;

    let system_comments: Vec<_> = comments
        .items
        .iter()
        .filter(|c| c.author_type == AuthorType::System)
        .collect();
    assert!(
        system_comments.iter().any(
            |c| c.content.contains("Review failed") && c.content.contains("needs refactoring")
        ),
        "expected system comment with rejection reason, got: {:?}",
        system_comments
            .iter()
            .map(|c| &c.content)
            .collect::<Vec<_>>()
    );
}

// ── Test 13.4: approve + full cascade with real git ──

#[tokio::test]
async fn approve_review_cascades_via_merge() {
    let temp = TestDir::new("forge-approve-cascade");
    let repo_path = temp.path().join("repo");
    std::fs::create_dir_all(&repo_path).expect("create repo dir");
    run_git(&repo_path, &["init", "--initial-branch=main"]);
    run_git(&repo_path, &["config", "user.email", "test@test.com"]);
    run_git(&repo_path, &["config", "user.name", "Test"]);
    std::fs::write(repo_path.join("file.txt"), "base\n").expect("write file");
    run_git(&repo_path, &["add", "."]);
    run_git(&repo_path, &["commit", "-m", "initial"]);

    let task_id = new_uuid_v4();
    let worktree_path = temp.path().join("worktrees").join(&task_id).join("repo");
    std::fs::create_dir_all(worktree_path.parent().unwrap()).expect("create worktree parent");
    run_git(
        &repo_path,
        &[
            "worktree",
            "add",
            worktree_path.to_str().unwrap(),
            "-b",
            &::workspace::task_branch_name(&task_id),
        ],
    );
    std::fs::write(worktree_path.join("feature.txt"), "hello\n").expect("write feature");
    run_git(&worktree_path, &["add", "."]);
    run_git(&worktree_path, &["commit", "-m", "feature"]);

    let workspace_root = TestDir::new("forge-approve-cascade-workspaces");
    let harness = test_app(workspace_root.path()).await;

    let (seeded_task_id, _review_id) = seed_awaiting_human_review_with_workspace(
        &harness.state.db,
        &task_id,
        &repo_path,
        &worktree_path,
    )
    .await;

    let result: ReviewDecisionResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{seeded_task_id}/review/approve"),
        json!({}),
        StatusCode::OK,
    )
    .await;

    assert_eq!(result.review.status, api_types::ReviewStatus::Passed);
    assert_eq!(result.task.status, "done".to_owned());

    // Verify system comment was created
    let comments: PaginatedResponse<CommentResponse> = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{seeded_task_id}/comments"),
        StatusCode::OK,
    )
    .await;
    let system_comments: Vec<_> = comments
        .items
        .iter()
        .filter(|c| c.author_type == AuthorType::System)
        .collect();
    assert!(
        system_comments
            .iter()
            .any(|c| c.content.contains("Review passed")),
        "expected 'Review passed' comment, got: {:?}",
        system_comments
            .iter()
            .map(|c| &c.content)
            .collect::<Vec<_>>()
    );
    assert!(
        system_comments
            .iter()
            .any(|c| c.content.contains("Changes merged to")),
        "expected 'Changes merged' comment, got: {:?}",
        system_comments
            .iter()
            .map(|c| &c.content)
            .collect::<Vec<_>>()
    );
}

// ── Harness ──

struct TestHarness {
    app: Router,
    state: Arc<AppState>,
    _web_dist_dir: TestDir,
}

async fn test_app(workspace_root: &Path) -> TestHarness {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");

    let db = Arc::new(db::SqliteDb::new(pool));
    let mut registry = executors::AdapterRegistry::new();
    registry.register(Box::new(DelayedCodexAdapter));
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

    let web_dist_dir = TestDir::new("forge-manual-review-web");
    std::fs::write(web_dist_dir.path().join("index.html"), "<html></html>").expect("write index");
    let app = build_router((*state).clone(), web_dist_dir.path().to_path_buf());

    TestHarness {
        app,
        state,
        _web_dist_dir: web_dist_dir,
    }
}

struct DelayedCodexAdapter;

impl CodingExecutorAdapter for DelayedCodexAdapter {
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
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok(ExecutionResult {
                status: ExecutionOutcome::Completed,
                after_sha: None,
                agent_session_id: Some("manual-review-follow-up-session".to_owned()),
                summary: Some("follow-up deferred".to_owned()),
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

// ── Seed helpers ──

async fn seed_awaiting_human_review(
    db: &db::SqliteDb,
    settings: &str,
    workspace_root: &Path,
) -> (String, String) {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let task_id = new_uuid_v4();
    let execution_id = new_uuid_v4();
    let review_id = new_uuid_v4();
    let workspace_id = new_uuid_v4();
    let (repo_path, worktree_path) = setup_git_repo(workspace_root, &task_id);
    let agent_id = seed_codex_agent(db).await;

    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "ManualReview".to_owned(),
            settings: settings.to_owned(),
            workflow_definition: "{}".to_string(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");

    RepoRepo::create(
        db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "repo".to_owned(),
            local_path: Some(repo_path.to_string_lossy().into_owned()),
            work_mode: db::WorkMode::DirectMerge,
            remote_url: String::new(),
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update(
        db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("project primary repo updates");

    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.clone(),
            project_id,
            repo_id: Some(repo_id.clone()),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Manual review task".to_owned(),
            description: None,
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

    db::WorkspaceRepo::create(
        db,
        CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task_id.clone(),
            repo_id: repo_id.clone(),
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch: ::workspace::task_branch_name(&task_id),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("workspace creates");

    ExecutionRepo::create(
        db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task_id.clone(),
            agent_id: Some(agent_id),
            role: "coder".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("manual-review-parent-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
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
        db,
        CreateReview {
            id: review_id.clone(),
            task_id: task_id.clone(),
            execution_id,
            attempt_number: 1,
            status: ReviewStatus::AwaitingHuman,
            step_results_json: "[]".to_owned(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("review creates");

    (task_id, review_id)
}

async fn seed_codex_agent(db: &db::SqliteDb) -> String {
    let now = now_rfc3339();
    let daemon_id = new_uuid_v4();
    DaemonRepo::upsert_by_machine_id(
        db,
        UpsertDaemon {
            id: daemon_id.clone(),
            machine_id: services::embedded_daemon::embedded_machine_id(),
            hostname: "manual-review-host".to_owned(),
            os: "linux".to_owned(),
            arch: "x86_64".to_owned(),
            agent_version: None,
            labels_json: "{}".to_owned(),
            status: DaemonStatus::Online,
            registration_token_hash: None,
            owner_id: None,
            visibility: "global".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("daemon creates");
    DaemonRepo::update_report(
        db,
        db::UpdateDaemonReport {
            id: daemon_id.clone(),
            detected_clis_json: r#"[{"kind":"codex","availability":"authenticated"}]"#.to_owned(),
            labels_json: None,
            status: DaemonStatus::Online,
            last_report_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("daemon report updates");

    let agent_id = new_uuid_v4();
    AgentRepo::create(
        db,
        CreateAgent {
            id: agent_id.clone(),
            name: "manual-review-codex".to_owned(),
            description: None,
            executor_type: "codex".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: Some(daemon_id),
            max_concurrent_tasks: 2,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("agent creates");

    agent_id
}

async fn seed_awaiting_human_review_with_workspace(
    db: &db::SqliteDb,
    task_id: &str,
    repo_path: &Path,
    worktree_path: &Path,
) -> (String, String) {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let execution_id = new_uuid_v4();
    let review_id = new_uuid_v4();
    let workspace_id = new_uuid_v4();

    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "CascadeApprove".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_string(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");

    RepoRepo::create(
        db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "repo".to_owned(),
            local_path: Some(repo_path.to_string_lossy().into_owned()),
            work_mode: db::WorkMode::DirectMerge,
            remote_url: String::new(),
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update(
        db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("project primary repo updates");

    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.to_owned(),
            project_id,
            repo_id: Some(repo_id.clone()),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Cascade approve task".to_owned(),
            description: None,
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

    db::WorkspaceRepo::create(
        db,
        CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task_id.to_owned(),
            repo_id,
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
        db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task_id.to_owned(),
            agent_id: None,
            role: "coder".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace_id),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("execution creates");

    ReviewRepo::create(
        db,
        CreateReview {
            id: review_id.clone(),
            task_id: task_id.to_owned(),
            execution_id,
            attempt_number: 1,
            status: ReviewStatus::AwaitingHuman,
            step_results_json: "[]".to_owned(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("review creates");

    (task_id.to_owned(), review_id)
}

async fn seed_review_with_status(db: &db::SqliteDb, status: ReviewStatus) -> String {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let task_id = new_uuid_v4();
    let execution_id = new_uuid_v4();
    let review_id = new_uuid_v4();

    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "ReviewStatus".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_string(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");
    RepoRepo::create(
        db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "repo".to_owned(),
            local_path: Some("/tmp/forge-review-status-repo".to_owned()),
            work_mode: db::WorkMode::DirectMerge,
            remote_url: String::new(),
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update(
        db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("project primary repo updates");
    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.clone(),
            project_id,
            repo_id: Some(repo_id),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Status test".to_owned(),
            description: None,
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
    ExecutionRepo::create(
        db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task_id.clone(),
            agent_id: None,
            role: "coder".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("execution creates");
    ReviewRepo::create(
        db,
        CreateReview {
            id: review_id,
            task_id: task_id.clone(),
            execution_id,
            attempt_number: 1,
            status,
            step_results_json: "[]".to_owned(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("review creates");

    task_id
}

async fn seed_task_with_system_comment(db: &db::SqliteDb) -> String {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let task_id = new_uuid_v4();

    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "SystemComment".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_string(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");
    RepoRepo::create(
        db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "repo".to_owned(),
            local_path: Some("/tmp/forge-sys-comment-repo".to_owned()),
            work_mode: db::WorkMode::DirectMerge,
            remote_url: String::new(),
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update(
        db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("project primary repo updates");
    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.clone(),
            project_id,
            repo_id: Some(repo_id),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "System comment test".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "todo".to_owned(),
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

    TaskCommentRepo::create_comment(
        db,
        CreateTaskComment {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            author_type: CommentAuthorType::System,
            author_id: None,
            author_name: "Forge".to_owned(),
            content: "System generated comment".to_owned(),
            execution_id: None,
            role: None,
            worklog_kind: None,
            idempotency_key: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("system comment creates");

    task_id
}

// ── Git helper ──

fn setup_git_repo(workspace_root: &Path, task_id: &str) -> (PathBuf, PathBuf) {
    let repo_path = workspace_root.join("repos").join(task_id);
    std::fs::create_dir_all(&repo_path).expect("create repo dir");
    run_git(&repo_path, &["init", "--initial-branch=main"]);
    run_git(&repo_path, &["config", "user.email", "test@test.com"]);
    run_git(&repo_path, &["config", "user.name", "Test"]);
    std::fs::write(repo_path.join("file.txt"), "base\n").expect("write base file");
    run_git(&repo_path, &["add", "."]);
    run_git(&repo_path, &["commit", "-m", "initial"]);

    let worktree_path = workspace_root.join("worktrees").join(task_id).join("repo");
    std::fs::create_dir_all(worktree_path.parent().unwrap()).expect("create worktree parent");
    run_git(
        &repo_path,
        &[
            "worktree",
            "add",
            worktree_path.to_str().unwrap(),
            "-b",
            &::workspace::task_branch_name(task_id),
        ],
    );
    std::fs::write(worktree_path.join("file.txt"), "feature\n").expect("write feature file");
    run_git(&worktree_path, &["add", "."]);
    run_git(&worktree_path, &["commit", "-m", "feature"]);

    (repo_path, worktree_path)
}

fn run_git(path: &Path, args: &[&str]) {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("git runs");
    assert!(
        output.status.success(),
        "git {} failed\nstdout: {}\nstderr: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

// ── Request helpers ──

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
    body: serde_json::Value,
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
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(body.to_string()))
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
        "unexpected status with body: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).expect("parse JSON")
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
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    assert_eq!(
        status,
        expected_status,
        "unexpected status with body: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).expect("parse JSON")
}

async fn raw_request(
    app: &Router,
    method: Method,
    uri: &str,
    body: serde_json::Value,
) -> axum::http::Response<Body> {
    let token = test_jwt();
    let body = if body.is_null() {
        Body::empty()
    } else {
        Body::from(body.to_string())
    };
    app.clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(body)
                .expect("build request"),
        )
        .await
        .expect("router response")
}

// ── TestDir ──

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
