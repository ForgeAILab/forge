//! Shared fixtures and helpers for workflow action tests.

use super::*;

pub(super) async fn sqlite_db() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    SqliteDb::new(pool)
}

pub(super) async fn seed_project_repo_and_task(
    db: &SqliteDb,
    task_id: &str,
    status: &str,
) -> String {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();

    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "Forge".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
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
            name: "forge".to_owned(),
            remote_url: Some("https://example.com/forge.git".to_owned()),
            local_path: None,
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
            project_id: project_id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "test task".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: status.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("task creates");
    project_id
}

pub(super) async fn seed_project_without_repo_and_task(
    db: &SqliteDb,
    task_id: &str,
    status: &str,
) -> String {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();

    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "Forge".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");

    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.to_owned(),
            project_id: project_id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "test task".to_owned(),
            description: Some("echo test".to_owned()),
            task_type: "task".to_owned(),
            status: status.to_owned(),
            priority: 0,
            is_automation: false,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("task creates");
    project_id
}

pub(super) fn setup_git_repo(path: &std::path::Path) -> String {
    run_git(path, &["init", "--initial-branch=main"]);
    run_git(path, &["config", "user.email", "test@forge.dev"]);
    run_git(path, &["config", "user.name", "Forge Test"]);
    std::fs::write(path.join("README.md"), "# Forge\n").expect("README writes");
    run_git(path, &["add", "-A"]);
    run_git(path, &["commit", "-m", "initial commit"]);
    run_git(path, &["symbolic-ref", "--short", "HEAD"])
}

pub(super) fn run_git(path: &std::path::Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .expect("git runs");
    assert!(
        output.status.success(),
        "git {:?} failed\nstdout:\n{}\nstderr:\n{}",
        args,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("stdout utf8")
        .trim()
        .to_owned()
}

pub(super) async fn seed_local_project_repo_and_task(
    db: &SqliteDb,
    repo_path: &std::path::Path,
    task_id: &str,
    status: &str,
) -> String {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let default_branch = setup_git_repo(repo_path);

    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "Forge".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
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
            name: "forge".to_owned(),
            remote_url: Some(repo_path.to_string_lossy().into_owned()),
            local_path: Some(repo_path.to_string_lossy().into_owned()),
            default_branch,
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
            project_id: project_id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "test task".to_owned(),
            description: Some("echo test".to_owned()),
            task_type: "task".to_owned(),
            status: status.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("task creates");
    project_id
}

pub(super) async fn seed_agent(db: &SqliteDb, agent_id: &str) {
    seed_agent_with_max(db, agent_id, 1).await;
}

pub(super) async fn seed_agent_with_max(db: &SqliteDb, agent_id: &str, max_concurrent_tasks: i64) {
    seed_agent_with_executor(db, agent_id, max_concurrent_tasks, "shell").await;
}

pub(super) async fn seed_agent_with_executor(
    db: &SqliteDb,
    agent_id: &str,
    max_concurrent_tasks: i64,
    executor_type: &str,
) {
    let now = now_rfc3339();

    let daemon_id = DaemonRepo::upsert_by_machine_id(
        db,
        UpsertDaemon {
            max_concurrent_runs: None,
            id: new_uuid_v4(),
            machine_id: crate::embedded_daemon::embedded_machine_id(),
            hostname: "test-host".to_owned(),
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
    .expect("daemon creates")
    .id;
    DaemonRepo::update_report(
        db,
        UpdateDaemonReport {
            max_concurrent_runs: None,
            id: daemon_id.clone(),
            detected_clis_json:
                serde_json::json!([{ "kind": executor_type, "availability": "authenticated" }])
                    .to_string(),
            labels_json: None,
            status: DaemonStatus::Online,
            last_report_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("daemon report updates");

    AgentRepo::create(
        db,
        CreateAgent {
            id: agent_id.to_owned(),
            name: "test-agent".to_owned(),
            description: None,
            executor_type: executor_type.to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: Some(daemon_id),
            max_concurrent_tasks: max_concurrent_tasks + 1,
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
}

pub(super) struct PendingExecutor {
    pub(super) sender: mpsc::UnboundedSender<ExecutionContext>,
}

#[async_trait]
impl TaskExecutor for PendingExecutor {
    async fn execute(
        &self,
        ctx: ExecutionContext,
    ) -> std::result::Result<ExecutionResult, ExecutorError> {
        let _ = self.sender.send(ctx);
        std::future::pending::<()>().await;
        unreachable!()
    }

    async fn cancel(&self, _execution_id: &str) -> std::result::Result<(), ExecutorError> {
        Ok(())
    }
}

pub(super) struct DispatchHarness {
    pub(super) ctx: HookContext,
    pub(super) rx: mpsc::UnboundedReceiver<ExecutionContext>,
    pub(super) _repo_dir: TempDir,
    pub(super) _workspace_root: TempDir,
}

/// Move the Task into the state whose entry hooks are under test.
///
/// The engine persists a transition with a blocking `before_enter` hook
/// before it runs that hook: the Task is already in the target state, behind
/// an entry barrier, when `run_ci_steps` and the review hooks execute. A
/// fixture that leaves the Task in the previous state instead makes every
/// review hook fail its authority check on a state mismatch that cannot
/// happen in production.
pub(super) async fn enter_target_state(ctx: &HookContext) {
    sqlx::query("UPDATE task SET status = ?, version = version + 1, updated_at = ? WHERE id = ?")
        .bind(&ctx.to_state)
        .bind(now_rfc3339())
        .bind(&ctx.task_id)
        .execute(ctx.db.pool())
        .await
        .expect("task enters the state under test");
}

/// Re-snapshot the Project authority a hook context carries.
///
/// A hook context is built once per transition in production, so its Project
/// version and workflow definition are always the live ones. A fixture that
/// mutates the Project after building the context (pausing it, changing
/// review defaults) has to re-snapshot, or every review hook fails closed
/// with "review authority changed: project workflow changed" -- which is the
/// contract working, not the case under test.
pub(super) async fn refresh_project_authority(ctx: &mut HookContext) {
    let project = ProjectRepo::get_by_id(&*ctx.db, &ctx.project_id)
        .await
        .expect("project reloads")
        .expect("project exists");
    ctx.project_version = Some(project.version);
    ctx.project_workflow_definition = Some(project.workflow_definition);
}

pub(super) async fn build_role_dispatch_harness(
    task_id: &str,
    from_state: &str,
    to_state: &str,
    role: &str,
    agent_id: &str,
    max_concurrent_tasks: i64,
) -> DispatchHarness {
    build_role_dispatch_harness_with_executor(
        task_id,
        from_state,
        to_state,
        role,
        agent_id,
        max_concurrent_tasks,
        "shell",
    )
    .await
}

pub(super) async fn build_role_dispatch_harness_with_executor(
    task_id: &str,
    from_state: &str,
    to_state: &str,
    role: &str,
    agent_id: &str,
    max_concurrent_tasks: i64,
    executor_type: &str,
) -> DispatchHarness {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_root = TempDir::new().expect("workspace dir creates");
    let project_id =
        seed_local_project_repo_and_task(&db, repo_dir.path(), task_id, to_state).await;
    seed_agent_with_executor(&db, agent_id, max_concurrent_tasks, executor_type).await;
    assign_agent_role(&db, task_id, role, agent_id).await;
    if role == default_roles::REVIEWER {
        configure_review_defaults(&db, &project_id, &[json!("test -d .")]).await;
    }
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project reloads")
        .expect("project exists");

    let workflow = Arc::new(default_workflow::default_workflow());
    let gate_config = workflow
        .states
        .iter()
        .find(|state| state.name == to_state)
        .and_then(|state| state.gate_config.clone());
    let (tx, rx) = mpsc::unbounded_channel();
    let event_bus = Arc::new(EventBus::new(16));
    let task_executor: Arc<dyn TaskExecutor> = Arc::new(PendingExecutor { sender: tx });
    let repo_cache_locks = Arc::new(RepoCacheLockManager::default());
    let task_service = crate::TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::clone(&task_executor))
        .with_workspace_root(workspace_root.path().to_path_buf())
        .with_repo_cache_locks(Arc::clone(&repo_cache_locks));

    DispatchHarness {
        ctx: HookContext {
            task_id: task_id.to_owned(),
            project_id,
            from_state: from_state.to_owned(),
            to_state: to_state.to_owned(),
            workspace_backend_router: task_service.workspace_backend_router(),
            db,
            event_bus,
            gate_config,
            workflow,
            project_version: Some(project.version),
            project_workflow_definition: Some(project.workflow_definition),
            triggered_by: api_types::Actor::system(api_types::SystemComponent::Test),
            merge_service: None,
            cleanup_scheduler: None,
            task_service,
            daemon_connections: None,
            workspace_exec_locks: None,
            terminal_activity: None,
            workspace_root: workspace_root.path().to_path_buf(),
            repo_cache_locks: Some(repo_cache_locks),
            workspace_id: None,
            agent_id: None,
            execution_id: None,
            state_config: json!({}),
        },
        rx,
        _repo_dir: repo_dir,
        _workspace_root: workspace_root,
    }
}

pub(super) async fn build_no_repo_dispatch_harness(
    task_id: &str,
    agent_id: &str,
    max_concurrent_tasks: i64,
) -> DispatchHarness {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_root = TempDir::new().expect("workspace dir creates");
    let project_id =
        seed_project_without_repo_and_task(&db, task_id, default_states::IN_PROGRESS).await;
    seed_agent_with_max(&db, agent_id, max_concurrent_tasks).await;
    assign_agent_role(&db, task_id, default_roles::CODER, agent_id).await;

    let workflow = Arc::new(default_workflow::default_workflow());
    let gate_config = workflow
        .states
        .iter()
        .find(|state| state.name == default_states::IN_PROGRESS)
        .and_then(|state| state.gate_config.clone());
    let (tx, rx) = mpsc::unbounded_channel();
    let event_bus = Arc::new(EventBus::new(16));
    let task_executor: Arc<dyn TaskExecutor> = Arc::new(PendingExecutor { sender: tx });
    let repo_cache_locks = Arc::new(RepoCacheLockManager::default());
    let task_service = crate::TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::clone(&task_executor))
        .with_workspace_root(workspace_root.path().to_path_buf())
        .with_repo_cache_locks(Arc::clone(&repo_cache_locks));

    DispatchHarness {
        ctx: HookContext {
            task_id: task_id.to_owned(),
            project_id,
            from_state: default_states::TODO.to_owned(),
            to_state: default_states::IN_PROGRESS.to_owned(),
            workspace_backend_router: crate::diff::embedded_read_router_for_test(Arc::clone(&db)),
            db,
            event_bus,
            gate_config,
            workflow,
            project_version: None,
            project_workflow_definition: None,
            triggered_by: api_types::Actor::system(api_types::SystemComponent::Test),
            merge_service: None,
            cleanup_scheduler: None,
            task_service,
            daemon_connections: None,
            workspace_exec_locks: None,
            terminal_activity: None,
            workspace_root: workspace_root.path().to_path_buf(),
            repo_cache_locks: Some(repo_cache_locks),
            workspace_id: None,
            agent_id: None,
            execution_id: None,
            state_config: json!({}),
        },
        rx,
        _repo_dir: repo_dir,
        _workspace_root: workspace_root,
    }
}

pub(super) async fn build_initial_dispatch_harness(
    task_id: &str,
    agent_id: &str,
    max_concurrent_tasks: i64,
) -> DispatchHarness {
    build_role_dispatch_harness(
        task_id,
        default_states::TODO,
        default_states::IN_PROGRESS,
        default_roles::CODER,
        agent_id,
        max_concurrent_tasks,
    )
    .await
}

pub(super) async fn assign_agent_role(db: &SqliteDb, task_id: &str, role: &str, agent_id: &str) {
    let now = now_rfc3339();
    TaskRoleAssignmentRepo::assign(
        db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            role_name: role.to_owned(),
            assignee_type: Some(db::AssigneeKind::Agent),
            assignee_id: Some(agent_id.to_owned()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("role assignment creates");
    let project_id: String = sqlx::query_scalar("SELECT project_id FROM task WHERE id = ?")
        .bind(task_id)
        .fetch_one(db.pool())
        .await
        .expect("task project lookup");
    crate::test_support::configure_project_execution_test_setup(
        db,
        &project_id,
        agent_id,
        agent_id,
    )
    .await;
}

/// `run_ci_steps` as the step queue runs it: review-entry CI is a check the
/// durable runner executes, asked for from the Task's hooks step, and the
/// hook settles the Review when it runs again with the result.
pub(super) async fn run_ci(ctx: &HookContext) -> HookResult {
    let worker = ctx.task_service.check_worker_or_embedded();
    crate::workflow::actions::run_ci_steps_in_step(ctx, &worker).await
}

pub(super) async fn build_test_ctx(
    task_id: &str,
    from_state: &str,
    to_state: &str,
    role_agent_setup: Option<(&str, &str)>,
) -> HookContext {
    let db = Arc::new(sqlite_db().await);
    let project_id = seed_project_repo_and_task(&db, task_id, from_state).await;

    if let Some((role, agent_id)) = role_agent_setup {
        seed_agent(&db, agent_id).await;
        assign_agent_role(&db, task_id, role, agent_id).await;
    }

    let workflow = Arc::new(default_workflow::default_workflow());
    let gate_config = workflow
        .states
        .iter()
        .find(|state| state.name == to_state)
        .and_then(|state| state.gate_config.clone());
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project reloads")
        .expect("project exists");
    let event_bus = Arc::new(EventBus::new(16));
    let task_service = crate::TaskService::new(Arc::clone(&db), Arc::clone(&event_bus));

    HookContext {
        task_id: task_id.to_owned(),
        project_id,
        from_state: from_state.to_owned(),
        to_state: to_state.to_owned(),
        workspace_backend_router: crate::diff::embedded_read_router_for_test(Arc::clone(&db)),
        db,
        event_bus,
        gate_config,
        workflow,
        project_version: Some(project.version),
        project_workflow_definition: Some(project.workflow_definition),
        triggered_by: api_types::Actor::system(api_types::SystemComponent::Test),
        merge_service: None,
        cleanup_scheduler: None,
        task_service,
        daemon_connections: None,
        workspace_exec_locks: None,
        terminal_activity: None,
        workspace_root: PathBuf::new(),
        repo_cache_locks: None,
        workspace_id: None,
        agent_id: None,
        execution_id: None,
        state_config: json!({}),
    }
}

pub(super) async fn seed_transition_log(
    db: &SqliteDb,
    task_id: &str,
    from_state: &str,
    to_state: &str,
    rejection: bool,
) {
    seed_transition_log_at(db, task_id, from_state, to_state, rejection, &now_rfc3339()).await;
}

pub(super) async fn seed_transition_log_at(
    db: &SqliteDb,
    task_id: &str,
    from_state: &str,
    to_state: &str,
    rejection: bool,
    created_at: &str,
) {
    sqlx::query(
        "INSERT INTO transition_log (id, task_id, from_state, to_state, trigger_name, triggered_by, trigger_reason, hook_results_json, rejection, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(new_uuid_v4())
    .bind(task_id)
    .bind(from_state)
    .bind(to_state)
    .bind::<Option<String>>(None)
    .bind("system")
    .bind("test")
    .bind(Option::<&str>::None)
    .bind(if rejection { 1_i64 } else { 0_i64 })
    .bind(created_at)
    .execute(db.pool())
    .await
    .expect("transition log inserts");
}

pub(super) async fn seed_completed_executor_execution(ctx: &HookContext) -> String {
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let repo_id = ProjectRepo::get_by_id(&*ctx.db, &task.project_id)
        .await
        .expect("project loads")
        .expect("project exists")
        .primary_repo_id
        .expect("project has a primary repository");
    let now = now_rfc3339();
    let workspace_id = new_uuid_v4();
    let worktree_path = std::env::temp_dir().join(format!("forge-workflow-action-{workspace_id}"));
    std::fs::create_dir_all(&worktree_path).expect("worktree path creates");
    setup_git_repo(&worktree_path);
    let before_sha = run_git(&worktree_path, &["rev-parse", "HEAD"]);
    WorkspaceRepo::create(
        &*ctx.db,
        CreateWorkspace {
            id: workspace_id.clone(),
            task_id: ctx.task_id.clone(),
            repo_id,
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch: ::workspace::task_branch_name(&ctx.task_id),
            status: WorkspaceStatus::Ready,
            before_sha: Some(before_sha.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("workspace creates");

    let execution_id = new_uuid_v4();
    ExecutionRepo::create(
        &*ctx.db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: ctx.task_id.clone(),
            agent_id: None,
            role: "executor".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("executor summary".to_owned()),
            logs_path: None,
            before_sha: Some(before_sha.clone()),
            after_sha: Some(before_sha),
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace_id),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("execution creates");
    execution_id
}

pub(super) async fn seed_review(
    ctx: &HookContext,
    status: ReviewStatus,
    attempt_number: i64,
) -> String {
    let now = now_rfc3339();
    let execution_id = ctx.execution_id.clone().unwrap_or_else(new_uuid_v4);
    ExecutionRepo::create(
        &*ctx.db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: ctx.task_id.clone(),
            agent_id: None,
            role: "executor".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("executor summary".to_owned()),
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
        &*ctx.db,
        CreateReview {
            id: new_uuid_v4(),
            task_id: ctx.task_id.clone(),
            execution_id: execution_id.clone(),
            attempt_number,
            status,
            step_results_json: "[]".to_owned(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("review creates");

    execution_id
}

pub(super) async fn seed_running_execution_for_task(
    db: &SqliteDb,
    task_id: &str,
    agent_id: &str,
    role: &str,
) {
    let now = now_rfc3339();
    ExecutionRepo::create(
        db,
        CreateExecution {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            agent_id: Some(agent_id.to_owned()),
            role: role.to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("running".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("running execution creates");
}

/// Two sibling Tasks each add an entry to the same export list; the first
/// merges and the second's rebase conflicts. Built as a linked worktree on a
/// real repository so the rebase meets the same Git metadata layout as a
/// managed Task.
pub(super) async fn seed_sibling_conflict_workspace(ctx: &HookContext) -> (TempDir, PathBuf) {
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let repo_id = ProjectRepo::get_by_id(&*ctx.db, &task.project_id)
        .await
        .expect("project loads")
        .expect("project exists")
        .primary_repo_id
        .expect("project has a primary repository");
    let dir = TempDir::new().expect("temp dir creates");
    let repo_path = dir.path().join("repo");
    std::fs::create_dir_all(&repo_path).expect("repo dir creates");
    setup_git_repo(&repo_path);
    std::fs::write(repo_path.join("exports.py"), "core = 1\n").expect("exports write");
    run_git(&repo_path, &["add", "-A"]);
    run_git(&repo_path, &["commit", "-m", "exports"]);

    let branch = ::workspace::task_branch_name(&ctx.task_id);
    let worktree_path = dir.path().join("worktree");
    run_git(
        &repo_path,
        &[
            "worktree",
            "add",
            "-b",
            &branch,
            worktree_path.to_str().expect("worktree path is UTF-8"),
            "main",
        ],
    );
    std::fs::write(worktree_path.join("exports.py"), "core = 1\napi = 1\n")
        .expect("task export writes");
    std::fs::write(worktree_path.join("api.py"), "API = True\n").expect("task module writes");
    run_git(&worktree_path, &["add", "-A"]);
    run_git(&worktree_path, &["commit", "-m", "add api"]);
    let before_sha = run_git(&worktree_path, &["rev-parse", "HEAD"]);

    std::fs::write(repo_path.join("exports.py"), "core = 1\ncli = 1\n")
        .expect("sibling export writes");
    run_git(&repo_path, &["add", "-A"]);
    run_git(&repo_path, &["commit", "-m", "sibling adds cli"]);

    let now = now_rfc3339();
    WorkspaceRepo::create(
        &*ctx.db,
        CreateWorkspace {
            id: new_uuid_v4(),
            task_id: ctx.task_id.clone(),
            repo_id,
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch,
            status: WorkspaceStatus::Ready,
            before_sha: Some(before_sha),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("workspace creates");
    sqlx::query("UPDATE repo SET local_path = ? WHERE id = ?")
        .bind(repo_path.to_string_lossy().as_ref())
        .bind(
            ProjectRepo::get_by_id(&*ctx.db, &ctx.project_id)
                .await
                .expect("project loads")
                .expect("project exists")
                .primary_repo_id
                .expect("primary repo exists"),
        )
        .execute(ctx.db.pool())
        .await
        .expect("fixture repository path updates");
    let workspace = WorkspaceRepo::get_by_task_id(&*ctx.db, &ctx.task_id)
        .await
        .expect("workspace loads")
        .expect("workspace exists");
    ExecutionRepo::create(
        &*ctx.db,
        CreateExecution {
            id: new_uuid_v4(),
            task_id: ctx.task_id.clone(),
            agent_id: None,
            role: "executor".to_owned(),
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
            workspace_id: Some(workspace.id),
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("executor execution creates");
    (dir, worktree_path)
}

pub(super) async fn record_conflict_handoff(ctx: &HookContext, reason: String, rejection: bool) {
    db::TransitionLogRepo::insert(
        &*ctx.db,
        db::CreateTransitionLog {
            id: new_uuid_v4(),
            task_id: ctx.task_id.clone(),
            from_state: default_states::MERGING.to_owned(),
            to_state: default_states::MERGE_FAILED.to_owned(),
            trigger_name: None,
            triggered_by: api_types::Actor::system(api_types::SystemComponent::Workflow).display(),
            bridge: api_types::TransitionBridge::conflict_handoff(&["exports.py".into()]),
            trigger_reason: reason,
            hook_results_json: None,
            rejection,
            created_at: now_rfc3339(),
        },
    )
    .await
    .expect("handoff transition records");
}

/// The `merging` entry hook as the worker runs it: inside the claimed hooks
/// step of that entry (the step worker scopes every step it executes with
/// `db::task_writer::in_task_step`). Admission states
/// the Task's integration condition, which only the holder of the Task's
/// step lease may write.
pub(super) async fn run_merge_entry_hook(ctx: &HookContext) -> HookResult {
    crate::test_support::with_task_lease(&ctx.db, &ctx.task_id, RunMerge.execute(ctx)).await
}

/// The queue path of a sibling conflict, up to the handoff: the entry hook
/// admits the Task, the queue's rebase meets the sibling's change and commits
/// the conflict with its markers, and its `send_back` step hands the files to
/// the Worker. Returns the fixture, the worktree and the handoff transition.
pub(super) async fn queue_conflict_handoff(
    task_id: &str,
) -> (HookContext, TempDir, PathBuf, db::TransitionLog) {
    let mut ctx = build_test_ctx(
        task_id,
        default_states::MERGING,
        default_states::MERGING,
        None,
    )
    .await;
    let (dir, worktree_path) = seed_sibling_conflict_workspace(&ctx).await;
    ctx.merge_service = Some(Arc::new(crate::merge_service::MergeService::new_for_test(
        Arc::clone(&ctx.db),
        dir.path().to_path_buf(),
    )));
    let result = run_merge_entry_hook(&ctx).await;
    assert!(matches!(result, HookResult::Ok), "{result:?}");
    ctx.task_service
        .drain(&ctx.task_id)
        .await
        .expect("the queue hands the conflict back");
    let handoff = db::TransitionLogRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("transitions load")
        .into_iter()
        .find(|entry| {
            entry.from_state == default_states::MERGING
                && entry.to_state == default_states::MERGE_FAILED
        })
        .expect("a sibling conflict goes back to the Worker");
    (ctx, dir, worktree_path, handoff)
}

/// The Task enters `merging` again (a new status entry), as it does when the
/// Worker's repair has passed its review.
pub(super) async fn reenter_merging(ctx: &HookContext) {
    sqlx::query("UPDATE task SET status = 'merging', status_epoch = status_epoch + 1, version = version + 1, error_annotation = NULL WHERE id = ?")
        .bind(&ctx.task_id)
        .execute(ctx.db.pool())
        .await
        .expect("the Task re-enters merging");
}

// --- Reviewer dispatch harness for review-state tests (7.3–7.6) ---

pub(super) async fn build_reviewer_dispatch_harness(
    task_id: &str,
    reviewer_agent_id: &str,
    max_concurrent_tasks: i64,
    ci_steps: Vec<&str>,
) -> DispatchHarness {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_root = TempDir::new().expect("workspace dir creates");
    let project_id =
        seed_local_project_repo_and_task(&db, repo_dir.path(), task_id, default_states::REVIEW)
            .await;
    seed_agent_with_max(&db, reviewer_agent_id, max_concurrent_tasks).await;
    assign_agent_role(&db, task_id, default_roles::REVIEWER, reviewer_agent_id).await;

    let workflow = Arc::new(default_workflow::default_workflow());
    let gate_config = workflow
        .states
        .iter()
        .find(|state| state.name == default_states::REVIEW)
        .and_then(|state| state.gate_config.clone());
    let (tx, rx) = mpsc::unbounded_channel();

    let ci_steps_value: Vec<serde_json::Value> = ci_steps
        .into_iter()
        .map(|step| serde_json::Value::String(step.to_owned()))
        .collect();

    // The reviewer admission now creates the immutable review contract at
    // execution start. Keep this direct action harness equivalent to the
    // default workflow's merged Review state config by persisting the same CI
    // commands in the Project defaults that the contract source reads.
    configure_review_defaults(&db, &project_id, &ci_steps_value).await;
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project reloads")
        .expect("project exists");
    let event_bus = Arc::new(EventBus::new(16));
    let task_executor: Arc<dyn TaskExecutor> = Arc::new(PendingExecutor { sender: tx });
    let repo_cache_locks = Arc::new(RepoCacheLockManager::default());
    let task_service = crate::TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::clone(&task_executor))
        .with_workspace_root(workspace_root.path().to_path_buf())
        .with_repo_cache_locks(Arc::clone(&repo_cache_locks));

    let mut harness = DispatchHarness {
        ctx: HookContext {
            task_id: task_id.to_owned(),
            project_id,
            from_state: default_states::IN_PROGRESS.to_owned(),
            to_state: default_states::REVIEW.to_owned(),
            workspace_backend_router: crate::diff::embedded_read_router_for_test(Arc::clone(&db)),
            db,
            event_bus,
            gate_config,
            workflow,
            project_version: Some(project.version),
            project_workflow_definition: Some(project.workflow_definition),
            triggered_by: api_types::Actor::system(api_types::SystemComponent::Test),
            merge_service: None,
            cleanup_scheduler: None,
            task_service,
            daemon_connections: None,
            workspace_exec_locks: None,
            terminal_activity: None,
            workspace_root: workspace_root.path().to_path_buf(),
            repo_cache_locks: Some(repo_cache_locks),
            workspace_id: None,
            agent_id: None,
            execution_id: None,
            state_config: json!({ "ci_steps": ci_steps_value }),
        },
        rx,
        _repo_dir: repo_dir,
        _workspace_root: workspace_root,
    };

    let execution_id = seed_completed_executor_execution(&harness.ctx).await;
    harness.ctx.execution_id = Some(execution_id);
    harness
}

pub(super) async fn configure_review_defaults(
    db: &SqliteDb,
    project_id: &str,
    ci_steps: &[serde_json::Value],
) {
    let project = ProjectRepo::get_by_id(db, project_id)
        .await
        .expect("project reloads")
        .expect("project exists");
    let mut settings: serde_json::Value =
        serde_json::from_str(&project.settings).expect("project settings are valid JSON");
    settings["default_review_config"] = json!({ "ci_steps": ci_steps });
    ProjectRepo::update_at_version(
        db,
        UpdateProject {
            id: project_id.to_owned(),
            name: None,
            settings: Some(settings.to_string()),
            primary_repo_id: None,
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        project.version,
        None,
    )
    .await
    .expect("review defaults update");
}

// --- Review authority carry across mechanical rebases and conflict repairs ---

pub(super) const CARRY_REFRESH_REASON: &str =
    "[review-refresh] mechanical merge contention resolved; fresh review required";

pub(super) struct CarryScenario {
    pub(super) worktree: PathBuf,
    pub(super) repo_path: PathBuf,
    pub(super) workspace_id: String,
    pub(super) executor_execution_id: String,
    pub(super) commit_sha: String,
    pub(super) changed_paths: Vec<String>,
}

/// A Task branch reviewed and approved against `main`, after which a sibling
/// lands on `main`. With `conflict` the sibling edits the same line of
/// `shared.txt` the Task edited; without it the sibling touches another file.
pub(super) async fn build_carry_scenario(
    task_id: &str,
    agent_id: &str,
    conflict: bool,
) -> (DispatchHarness, CarryScenario) {
    let harness = build_role_dispatch_harness(
        task_id,
        default_states::MERGING,
        default_states::MERGING,
        default_roles::REVIEWER,
        agent_id,
        1,
    )
    .await;
    let ctx = &harness.ctx;
    let repo_path = harness._repo_dir.path().to_path_buf();
    std::fs::write(repo_path.join("shared.txt"), "core = 1\n").expect("shared writes");
    run_git(&repo_path, &["add", "-A"]);
    run_git(&repo_path, &["commit", "-m", "shared"]);

    let branch = ::workspace::task_branch_name(task_id);
    let worktree = harness._workspace_root.path().join("worktree");
    run_git(
        &repo_path,
        &[
            "worktree",
            "add",
            "-b",
            &branch,
            worktree.to_str().expect("worktree path is UTF-8"),
            "main",
        ],
    );
    run_git(&worktree, &["config", "user.email", "test@forge.dev"]);
    run_git(&worktree, &["config", "user.name", "Forge Test"]);
    std::fs::write(worktree.join("shared.txt"), "core = 1\napi = 1\n").expect("task edit writes");
    std::fs::write(worktree.join("feature.txt"), "feature\n").expect("feature writes");
    run_git(&worktree, &["add", "-A"]);
    run_git(&worktree, &["commit", "-m", "add api"]);
    let commit_sha = run_git(&worktree, &["rev-parse", "HEAD"]);
    let base_sha = run_git(&repo_path, &["rev-parse", "main"]);

    if conflict {
        std::fs::write(repo_path.join("shared.txt"), "core = 1\ncli = 1\n")
            .expect("sibling edit writes");
    } else {
        std::fs::write(repo_path.join("other.txt"), "other\n").expect("sibling file writes");
    }
    run_git(&repo_path, &["add", "-A"]);
    run_git(&repo_path, &["commit", "-m", "sibling lands"]);

    let repo_id = ProjectRepo::get_by_id(&*ctx.db, &ctx.project_id)
        .await
        .expect("project loads")
        .expect("project exists")
        .primary_repo_id
        .expect("primary repo exists");
    let now = now_rfc3339();
    let workspace_id = new_uuid_v4();
    WorkspaceRepo::create(
        &*ctx.db,
        CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task_id.to_owned(),
            repo_id,
            worktree_path: worktree.to_string_lossy().into_owned(),
            branch,
            status: WorkspaceStatus::Ready,
            before_sha: Some(base_sha.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("workspace creates");
    let executor_execution_id = new_uuid_v4();
    create_carry_execution(
        ctx,
        &executor_execution_id,
        "executor",
        &workspace_id,
        false,
    )
    .await;

    let scenario = CarryScenario {
        worktree,
        repo_path,
        workspace_id,
        executor_execution_id,
        commit_sha,
        changed_paths: vec!["feature.txt".to_owned(), "shared.txt".to_owned()],
    };
    let mut harness = harness;
    harness.ctx.merge_service = Some(Arc::new(crate::merge_service::MergeService::new_for_test(
        Arc::clone(&harness.ctx.db),
        harness._workspace_root.path().to_path_buf(),
    )));
    seed_passed_carry_review(
        &harness.ctx,
        &scenario,
        &scenario.commit_sha,
        &base_sha,
        &scenario.changed_paths,
        1,
    )
    .await;
    (harness, scenario)
}

pub(super) async fn create_carry_execution(
    ctx: &HookContext,
    id: &str,
    role: &str,
    workspace_id: &str,
    running: bool,
) {
    let now = now_rfc3339();
    // A running execution must belong to the agent holding the role.
    let agent_id: Option<String> = if running {
        sqlx::query_scalar(
            "SELECT assignee_id FROM task_role_assignment WHERE task_id = ? AND role_name = ?",
        )
        .bind(&ctx.task_id)
        .bind(role)
        .fetch_optional(ctx.db.pool())
        .await
        .expect("role assignment loads")
        .flatten()
    } else {
        None
    };
    ExecutionRepo::create(
        &*ctx.db,
        CreateExecution {
            id: id.to_owned(),
            task_id: ctx.task_id.clone(),
            agent_id,
            role: role.to_owned(),
            status: if running {
                ExecutionStatus::Running
            } else {
                ExecutionStatus::Completed
            },
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
            workspace_id: Some(workspace_id.to_owned()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("execution creates");
}

/// Freeze a review contract and a passed assessment for `commit_sha`/`base_sha`
/// and record the passed Review row that carries them, exactly as a reviewer
/// run that passed would leave them. Returns the contract's execution id.
pub(super) async fn seed_passed_carry_review(
    ctx: &HookContext,
    scenario: &CarryScenario,
    commit_sha: &str,
    base_sha: &str,
    changed_paths: &[String],
    attempt_number: i64,
) -> String {
    let reviewer_execution_id = new_uuid_v4();
    create_carry_execution(
        ctx,
        &reviewer_execution_id,
        "reviewer",
        &scenario.workspace_id,
        true,
    )
    .await;
    let context =
        ::review::contract::load_context(&ctx.db, &ctx.task_id, Some(&reviewer_execution_id))
            .await
            .expect("governing context loads");
    let mut contract = api_types::ReviewContract {
        execution_id: reviewer_execution_id.clone(),
        policy: api_types::REVIEW_CONFORMANCE_POLICY.to_owned(),
        commit_sha: commit_sha.to_owned(),
        base_sha: base_sha.to_owned(),
        candidate_changed_paths: changed_paths.to_vec(),
        context,
        check_results: Vec::new(),
        digest: String::new(),
    };
    contract.digest = api_types::canonical_digest(&contract).expect("contract digests");
    ctx.db
        .create_review_contract(&contract)
        .await
        .expect("contract freezes");
    let conformance = api_types::ReviewConformance {
        status: api_types::ConformanceStatus::Passed,
        contract: Some(contract),
        assessment: Some(api_types::ReviewAssessment {
            fixable_by: api_types::FixableBy::Coder,
            repeat: false,
            findings: Vec::new(),
            resolved: Vec::new(),
            result: api_types::ReviewResult::Pass,
            reason: "looks right".to_owned(),
            report: String::new(),
        }),
        checks: Vec::new(),
        reason: None,
    };
    ctx.db
        .record_review_conformance(&conformance)
        .await
        .expect("assessment freezes");
    sqlx::query("UPDATE execution SET status = 'completed' WHERE id = ?")
        .bind(&reviewer_execution_id)
        .execute(ctx.db.pool())
        .await
        .expect("reviewer execution completes");
    let now = now_rfc3339();
    ReviewRepo::create(
        &*ctx.db,
        CreateReview {
            id: new_uuid_v4(),
            task_id: ctx.task_id.clone(),
            execution_id: scenario.executor_execution_id.clone(),
            attempt_number,
            status: ReviewStatus::Passed,
            step_results_json: json!({
                "ci_steps": [],
                "conformance": conformance,
                "auditor": { "verdict": "pass", "reason": "looks right" },
            })
            .to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("passed review records");
    TaskRepo::set_review_passed_at(&*ctx.db, &ctx.task_id, Some(now.clone()), &now)
        .await
        .expect("review_passed_at seeds");
    reviewer_execution_id
}

pub(super) async fn record_transition_entry(
    ctx: &HookContext,
    from: &str,
    to: &str,
    actor: api_types::Actor,
    reason: &str,
    bridge: api_types::TransitionBridge,
) {
    db::TransitionLogRepo::insert(
        &*ctx.db,
        db::CreateTransitionLog {
            id: new_uuid_v4(),
            task_id: ctx.task_id.clone(),
            from_state: from.to_owned(),
            to_state: to.to_owned(),
            trigger_name: None,
            triggered_by: actor.display(),
            bridge,
            trigger_reason: reason.to_owned(),
            hook_results_json: None,
            rejection: false,
            created_at: now_rfc3339(),
        },
    )
    .await
    .expect("transition records");
}

/// Log the bridge Forge wrote when it sent the Task to `merge_failed`, then the
/// move back into `review`, and put `ctx` where the review entry hooks run.
pub(super) async fn enter_review_after_bridge(
    ctx: &mut HookContext,
    bridge_reason: &str,
    bridge: &api_types::TransitionBridge,
    entry_actor: api_types::Actor,
    entry_reason: &str,
    ci_steps: serde_json::Value,
) {
    let workflow_actor = api_types::Actor::system(api_types::SystemComponent::Workflow);
    record_transition_entry(
        ctx,
        default_states::MERGING,
        default_states::MERGE_FAILED,
        workflow_actor,
        bridge_reason,
        bridge.clone(),
    )
    .await;
    record_transition_entry(
        ctx,
        default_states::MERGE_FAILED,
        default_states::REVIEW,
        entry_actor.clone(),
        entry_reason,
        api_types::TransitionBridge::new(api_types::TransitionBridgeKind::ReviewRefresh),
    )
    .await;
    ctx.from_state = default_states::MERGE_FAILED.to_owned();
    ctx.to_state = default_states::REVIEW.to_owned();
    ctx.triggered_by = entry_actor;
    ctx.state_config = json!({ "ci_steps": ci_steps });
    ctx.gate_config = ctx
        .workflow
        .states
        .iter()
        .find(|state| state.name == default_states::REVIEW)
        .and_then(|state| state.gate_config.clone());
    enter_target_state(ctx).await;
    refresh_project_authority(ctx).await;
}

/// What the merge hook did before the integration queue when the target had
/// moved under a reviewed candidate: rebase the branch in place and bounce
/// the Task through `merge_failed` with a typed bridge. The queue rebases
/// inside `merging` now, so nothing produces the clean-rebase bridge any
/// more; Tasks that were bounced before the upgrade still enter `review` on
/// it, and a conflict the queue hands to the Worker still uses the
/// conflict-handoff bridge. Returns the bridge's reason and the bridge.
pub(super) async fn pre_queue_bounce(ctx: &HookContext) -> (String, api_types::TransitionBridge) {
    let workspace = WorkspaceRepo::get_by_task_id(&*ctx.db, &ctx.task_id)
        .await
        .expect("workspace loads")
        .expect("workspace exists");
    let resolved = crate::workspace_backend::EmbeddedWorkspaceBackend::resolve_workspace(
        &ctx.workspace_backend_router,
        &ctx.db,
        &workspace,
        &ctx.workspace_root,
    )
    .await
    .expect("workspace resolves");
    let outcome = resolved
        .rebase_target("main", true)
        .await
        .expect("owner rebases");
    let now = now_rfc3339();
    TaskRepo::set_review_passed_at(&*ctx.db, &ctx.task_id, None, &now)
        .await
        .expect("the bounce clears the passed review");
    match outcome {
        api_types::WorkspaceOwnerOperationOutcome::Rebased => (
            "main advanced; rebased onto main, re-review required".to_owned(),
            api_types::TransitionBridge::new(api_types::TransitionBridgeKind::TargetMovedRebase),
        ),
        api_types::WorkspaceOwnerOperationOutcome::Conflict { conflict_paths, .. } => (
            format!(
                "rebased onto main; conflicts were committed with markers in: {}",
                conflict_paths.join(", ")
            ),
            api_types::TransitionBridge::conflict_handoff(&conflict_paths),
        ),
        other => panic!("unexpected rebase outcome {other:?}"),
    }
}

/// The Task's current integration attempt.
pub(super) async fn current_attempt(ctx: &HookContext) -> db::IntegrationAttempt {
    db::IntegrationQueueRepo::current_integration_attempt(&*ctx.db, &ctx.task_id)
        .await
        .expect("attempt loads")
        .expect("the Task is in the integration queue")
}

pub(super) async fn carry_rows(ctx: &HookContext) -> Vec<(String, String, String, String)> {
    sqlx::query_as::<_, (String, String, String, String)>(
        "SELECT contract_execution_id, commit_sha, base_sha, kind
         FROM review_authority_carry WHERE task_id = ? ORDER BY created_at, rowid",
    )
    .bind(&ctx.task_id)
    .fetch_all(ctx.db.pool())
    .await
    .expect("carry rows load")
}

pub(super) async fn reviewer_execution_count(ctx: &HookContext) -> i64 {
    ExecutionRepo::count_by_task_and_role(&*ctx.db, &ctx.task_id, default_roles::REVIEWER)
        .await
        .expect("reviewer execution count loads")
}

pub(super) async fn merge_again_from_review(ctx: &mut HookContext) -> HookResult {
    ctx.from_state = default_states::REVIEW.to_owned();
    ctx.to_state = default_states::MERGING.to_owned();
    enter_target_state(ctx).await;
    refresh_project_authority(ctx).await;
    RunMerge.execute(ctx).await
}
