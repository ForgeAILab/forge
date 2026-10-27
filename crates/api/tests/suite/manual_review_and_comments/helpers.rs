//! Shared manual-review harness, task fixtures and HTTP helpers.

use super::*;

pub(super) async fn review_budget_spent(db: &db::SqliteDb, task_id: &str) -> (i64, i64) {
    (
        db::budget::spent(db.pool(), task_id, "review")
            .await
            .expect("review budget reads"),
        db::budget::spent(db.pool(), task_id, db::budget::REVIEW_GATE_KEY)
            .await
            .expect("review gate budget reads"),
    )
}

/// A Task in `review` with a worktree, green-able checks and, when asked, a
/// reviewer Agent assigned (paused or not).
pub(super) async fn seed_rerun_task(
    name: &str,
    reviewer_paused: Option<bool>,
) -> (TestHarness, String, Option<String>, Vec<common::TestDir>) {
    seed_rerun_task_with_reviewer(name, reviewer_paused, false).await
}

pub(super) async fn seed_rerun_task_with_reviewer(
    name: &str,
    reviewer_paused: Option<bool>,
    reviewer_passes: bool,
) -> (TestHarness, String, Option<String>, Vec<common::TestDir>) {
    let temp = common::TestDir::new(name);
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
    let workspace_root = common::TestDir::new(&format!("{name}-workspaces"));
    let harness = test_app_with_reviewer(workspace_root.path(), reviewer_passes).await;
    let (task_id, _) = seed_awaiting_human_review_with_workspace(
        &harness.state.db,
        &task_id,
        &repo_path,
        &worktree_path,
    )
    .await;
    set_review_ci_steps(&harness.state.db, &task_id, &["true"]).await;
    let reviewer = match reviewer_paused {
        None => None,
        Some(paused) => {
            let agent_id = seed_codex_agent(&harness.state.db).await;
            if paused {
                AgentRepo::set_paused(&*harness.state.db, &agent_id, true)
                    .await
                    .expect("reviewer pauses");
            }
            let now = now_rfc3339();
            TaskRoleAssignmentRepo::assign(
                &*harness.state.db,
                CreateTaskRoleAssignment {
                    id: new_uuid_v4(),
                    task_id: task_id.clone(),
                    role_name: "reviewer".to_owned(),
                    assignee_type: Some(AssigneeKind::Agent),
                    assignee_id: Some(agent_id.clone()),
                    created_at: now.clone(),
                    updated_at: now,
                },
            )
            .await
            .expect("reviewer assignment creates");
            Some(agent_id)
        }
    };
    (harness, task_id, reviewer, vec![temp, workspace_root])
}

pub(super) async fn count(db: &db::SqliteDb, sql: &str, task_id: &str) -> i64 {
    sqlx::query_scalar(sql)
        .bind(task_id)
        .fetch_one(db.pool())
        .await
        .expect("count reads")
}

pub(super) async fn post_retry(
    harness: &TestHarness,
    task_id: &str,
    expected: StatusCode,
) -> serde_json::Value {
    let uri = format!("/api/v1/tasks/{task_id}/actions");
    json_request(
        &harness.app,
        Method::POST,
        &uri,
        json!({ "version": common::task_action_version(&harness.app, &uri).await, "action": {"verb": "retry"} }),
        expected,
    )
    .await
}

/// Start from the shared Git fixture, retaining only the human branch and
/// configured validation. No implementation or Review record is supplied.
pub(super) async fn human_submission_fixture(
    name: &str,
    commits: bool,
    reviewer_passes: bool,
) -> (TestHarness, String, Vec<common::TestDir>) {
    let (harness, task_id, _, dirs) =
        seed_rerun_task_with_reviewer(name, Some(false), reviewer_passes).await;
    let db = &harness.state.db;
    sqlx::query("DELETE FROM review WHERE task_id=?")
        .bind(&task_id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM execution WHERE task_id=?")
        .bind(&task_id)
        .execute(db.pool())
        .await
        .unwrap();
    let workspace = db::WorkspaceRepo::get_by_task_id(&**db, &task_id)
        .await
        .unwrap()
        .unwrap();
    let repo = RepoRepo::get_by_id(&**db, &workspace.repo_id)
        .await
        .unwrap()
        .unwrap();
    let base = git::get_current_sha(Path::new(repo.local_path.as_deref().unwrap()))
        .await
        .unwrap();
    if !commits {
        run_git(
            Path::new(workspace.embedded_worktree_path_for_backend()),
            &[
                "update-ref",
                &format!("refs/heads/{}", workspace.branch),
                &base,
            ],
        );
    }
    sqlx::query("UPDATE workspace SET before_sha=? WHERE id=?")
        .bind(base)
        .bind(workspace.id)
        .execute(db.pool())
        .await
        .unwrap();
    let task = TaskRepo::get_by_id(&**db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    TaskRepo::update_status(
        &**db,
        db::UpdateTaskStatus {
            id: task_id.clone(),
            expected_version: task.version,
            status: "in_progress".into(),
            assignee_id: None,
            error_annotation: Some(None),
            blocked_json: Some(None),
            failed_json: Some(None),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    (harness, task_id, dirs)
}

pub(super) async fn set_review_ci_steps(db: &db::SqliteDb, task_id: &str, steps: &[&str]) {
    let task = TaskRepo::get_by_id(db, task_id, false)
        .await
        .expect("Task lookup")
        .expect("Task exists");
    TaskRepo::update(
        db,
        UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            task_state_config: Some(Some(json!({"review":{"ci_steps":steps}}).to_string())),
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("review CI config updates");
}

/// A review-check retry runs no reviewer execution of its own: the only
/// executions a Task has afterwards are the ones real roles ran.
pub(super) async fn assert_no_synthetic_reviewer_execution(db: &db::SqliteDb, task_id: &str) {
    let agentless: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution WHERE task_id = ? AND role = 'reviewer' AND agent_id IS NULL",
    )
    .bind(task_id)
    .fetch_one(db.pool())
    .await
    .expect("executions count");
    assert_eq!(agentless, 0, "no synthetic reviewer execution is created");
}

// ── Harness ──

pub(super) struct TestHarness {
    pub(super) app: Router,
    pub(super) state: Arc<AppState>,
    pub(super) _web_dist_dir: common::TestDir,
}

pub(super) async fn test_app(workspace_root: &Path) -> TestHarness {
    test_app_with_reviewer(workspace_root, false).await
}

pub(super) async fn test_app_with_reviewer(
    workspace_root: &Path,
    reviewer_passes: bool,
) -> TestHarness {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");

    let db = Arc::new(db::SqliteDb::new(pool));
    let mut registry = executors::AdapterRegistry::new();
    registry.register(Box::new(DelayedCodexAdapter { reviewer_passes }));
    let adapter_registry = Arc::new(registry);
    services_agents::ensure_default_agents(db.as_ref(), &adapter_registry)
        .await
        .expect("default agents upsert");
    let event_bus = Arc::new(EventBus::new(256));
    let merge_service = Arc::new(services::MergeService::new_for_test(
        Arc::clone(&db),
        workspace_root.to_path_buf(),
    ));
    let cleanup_scheduler = Arc::new(services::WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        workspace_root.to_path_buf(),
    ));
    let state = Arc::new(AppState::with_adapter_registry_services_and_shutdown(
        db,
        event_bus,
        true,
        adapter_registry,
        merge_service,
        cleanup_scheduler,
        api::state::ShutdownSignal::new(),
        api::state::test_workflows_dir(),
        api::state::test_jwt_secret(),
        api::state::test_bcrypt_cost(),
    ));

    let web_dist_dir = common::TestDir::new("forge-manual-review-web");
    std::fs::write(web_dist_dir.path().join("index.html"), "<html></html>").expect("write index");
    let app = build_router((*state).clone(), web_dist_dir.path().to_path_buf());

    TestHarness {
        app,
        state,
        _web_dist_dir: web_dist_dir,
    }
}

/// Every run takes 30 seconds, except, when `reviewer_passes`, a reviewer's:
/// it reports a passing assessment at once.
pub(super) struct DelayedCodexAdapter {
    pub(super) reviewer_passes: bool,
}

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
        ctx: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = Result<ExecutionResult, ExecutorError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let reviewer_passes = self.reviewer_passes;
        Box::pin(async move {
            if reviewer_passes && common::is_conformance_review_prompt(&ctx.description) {
                if let Some(parent) = Path::new(&ctx.logs_path).parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let mut writer = executors::LogWriter::new(
                    &ctx.logs_path,
                    ctx.execution_id.clone(),
                    1024 * 1024,
                );
                writer
                    .write(
                        executors::LogKind::Assistant,
                        executors::LogStream::Main,
                        json!({
                            "text": common::passing_review_assessment(
                                &ctx.description,
                                Path::new(&ctx.worktree_path),
                            )
                        }),
                    )
                    .await?;
                return Ok(ExecutionResult {
                    status: ExecutionOutcome::Completed,
                    after_sha: None,
                    agent_session_id: Some("manual-review-reviewer-session".to_owned()),
                    summary: Some("reviewer passed the implementation".to_owned()),
                    error: None,
                    ..Default::default()
                });
            }
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok(ExecutionResult {
                status: ExecutionOutcome::Completed,
                after_sha: None,
                agent_session_id: Some("manual-review-follow-up-session".to_owned()),
                summary: Some("follow-up deferred".to_owned()),
                error: None,
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

pub(super) async fn seed_awaiting_human_review(
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
            remote_url: None,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project primary repo updates");

    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.clone(),
            project_id,
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

    TaskRoleAssignmentRepo::assign(
        db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            role_name: "coder".to_owned(),
            assignee_type: Some(AssigneeKind::Agent),
            assignee_id: Some(agent_id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("coder assignment creates");

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

pub(super) async fn seed_codex_agent(db: &db::SqliteDb) -> String {
    let now = now_rfc3339();
    let daemon_id = new_uuid_v4();
    DaemonRepo::upsert_by_machine_id(
        db,
        UpsertDaemon {
            max_concurrent_runs: None,
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
            max_concurrent_runs: None,
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

pub(super) async fn seed_awaiting_human_review_with_workspace(
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
            remote_url: None,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project primary repo updates");

    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.to_owned(),
            project_id,
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

pub(super) async fn seed_review_with_status(db: &db::SqliteDb, status: ReviewStatus) -> String {
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
            remote_url: None,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project primary repo updates");
    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.clone(),
            project_id,
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

pub(super) async fn seed_task_with_system_comment(db: &db::SqliteDb) -> String {
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
            remote_url: None,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project primary repo updates");
    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.clone(),
            project_id,
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

pub(super) fn setup_git_repo(workspace_root: &Path, task_id: &str) -> (PathBuf, PathBuf) {
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

pub(super) fn run_git(path: &Path, args: &[&str]) {
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

pub(super) fn test_jwt() -> String {
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

pub(super) async fn json_request<T>(
    app: &Router,
    method: Method,
    uri: &str,
    body: serde_json::Value,
    expected_status: StatusCode,
) -> T
where
    T: DeserializeOwned,
{
    common::json_request(app, method, uri, body, expected_status).await
}

pub(super) async fn empty_request<T>(
    app: &Router,
    method: Method,
    uri: &str,
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

pub(super) async fn raw_request(
    app: &Router,
    method: Method,
    uri: &str,
    body: serde_json::Value,
) -> axum::http::Response<Body> {
    common::raw_json_request(app, method, uri, body).await
}
