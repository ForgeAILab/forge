use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentRepo, AgentStatus,
    CreateAgent, CreateExecution, CreateProject, CreateRepo, CreateReview, CreateTask,
    CreateTaskRoleAssignment, CreateWorkspace, DaemonRepo, DaemonStatus, ExecutionRepo,
    ExecutionStatus, PageRequest, ProjectRepo, RepoRepo, ReviewRepo, ReviewStatus, SortBy,
    SortOrder, SqliteDb, TaskRepo, TaskRoleAssignmentRepo, UpdateDaemonReport, UpdateProject,
    UpdateTaskStatus, UpsertDaemon, WorkspaceRepo, WorkspaceStatus,
};
use events::{EventBus, EventContext};
use executors::{ExecutionContext, ExecutionResult, ExecutorError, TaskExecutor};
use serde_json::json;
use tempfile::TempDir;
use tokio::sync::mpsc;
use workspace::RepoCacheLockManager;

use super::merge::merge_failure_result;
use super::{
    AutoCascadeOnReviewPass, CheckRetryBudget, DependencyGate, DispatchRoleAgent, NotifyRoleHolder,
    RequireUpstreamRolesCompleted, RunCiSteps,
};
use crate::workflow::{
    default_roles, default_states, default_workflow, HookAction, HookContext, HookResult,
};

async fn sqlite_db() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    SqliteDb::new(pool)
}

async fn seed_project_repo_and_task(db: &SqliteDb, task_id: &str, status: &str) -> String {
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
            remote_url: "https://example.com/forge.git".to_owned(),
            local_path: None,
            work_mode: db::WorkMode::DirectMerge,
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

async fn seed_project_without_repo_and_task(db: &SqliteDb, task_id: &str, status: &str) -> String {
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

fn setup_git_repo(path: &std::path::Path) -> String {
    run_git(path, &["init", "--initial-branch=main"]);
    run_git(path, &["config", "user.email", "test@forge.dev"]);
    run_git(path, &["config", "user.name", "Forge Test"]);
    std::fs::write(path.join("README.md"), "# Forge\n").expect("README writes");
    run_git(path, &["add", "-A"]);
    run_git(path, &["commit", "-m", "initial commit"]);
    run_git(path, &["symbolic-ref", "--short", "HEAD"])
}

fn run_git(path: &std::path::Path, args: &[&str]) -> String {
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

async fn seed_local_project_repo_and_task(
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
            remote_url: repo_path.to_string_lossy().into_owned(),
            local_path: Some(repo_path.to_string_lossy().into_owned()),
            work_mode: db::WorkMode::DirectMerge,
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

async fn seed_agent(db: &SqliteDb, agent_id: &str) {
    seed_agent_with_max(db, agent_id, 1).await;
}

async fn seed_agent_with_max(db: &SqliteDb, agent_id: &str, max_concurrent_tasks: i64) {
    let now = now_rfc3339();
    let daemon_id = new_uuid_v4();

    DaemonRepo::upsert_by_machine_id(
        db,
        UpsertDaemon {
            id: daemon_id.clone(),
            machine_id: format!("machine-{daemon_id}"),
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
    .expect("daemon creates");
    DaemonRepo::update_report(
        db,
        UpdateDaemonReport {
            id: daemon_id.clone(),
            detected_clis_json: r#"[{"kind":"shell","availability":"authenticated"}]"#.to_owned(),
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
            executor_type: "shell".to_owned(),
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

struct PendingExecutor {
    sender: mpsc::UnboundedSender<ExecutionContext>,
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

struct DispatchHarness {
    ctx: HookContext,
    rx: mpsc::UnboundedReceiver<ExecutionContext>,
    _repo_dir: TempDir,
    _workspace_root: TempDir,
}

/// Move the Task into the state whose entry hooks are under test.
///
/// The engine persists a transition with a blocking `before_enter` hook
/// before it runs that hook: the Task is already in the target state, behind
/// an entry barrier, when `run_ci_steps` and the review hooks execute. A
/// fixture that leaves the Task in the previous state instead makes every
/// review hook fail its authority check on a state mismatch that cannot
/// happen in production.
async fn enter_target_state(ctx: &HookContext) {
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
async fn refresh_project_authority(ctx: &mut HookContext) {
    let project = ProjectRepo::get_by_id(&*ctx.db, &ctx.project_id)
        .await
        .expect("project reloads")
        .expect("project exists");
    ctx.project_version = Some(project.version);
    ctx.project_workflow_definition = Some(project.workflow_definition);
}

async fn build_role_dispatch_harness(
    task_id: &str,
    from_state: &str,
    to_state: &str,
    role: &str,
    agent_id: &str,
    max_concurrent_tasks: i64,
) -> DispatchHarness {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_root = TempDir::new().expect("workspace dir creates");
    let project_id =
        seed_local_project_repo_and_task(&db, repo_dir.path(), task_id, to_state).await;
    seed_agent_with_max(&db, agent_id, max_concurrent_tasks).await;
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

    DispatchHarness {
        ctx: HookContext {
            task_id: task_id.to_owned(),
            project_id,
            from_state: from_state.to_owned(),
            to_state: to_state.to_owned(),
            db,
            event_bus: Arc::new(EventBus::new(16)),
            gate_config,
            workflow,
            project_version: Some(project.version),
            project_workflow_definition: Some(project.workflow_definition),
            triggered_by: api_types::Actor::system(api_types::SystemComponent::Test),
            review_runner: None,
            merge_service: None,
            cleanup_scheduler: None,
            task_executor: Some(Arc::new(PendingExecutor { sender: tx })),
            daemon_connections: None,
            workspace_exec_locks: None,
            terminal_activity: None,
            workspace_root: workspace_root.path().to_path_buf(),
            repo_cache_locks: Some(Arc::new(RepoCacheLockManager::default())),
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

async fn build_no_repo_dispatch_harness(
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

    DispatchHarness {
        ctx: HookContext {
            task_id: task_id.to_owned(),
            project_id,
            from_state: default_states::TODO.to_owned(),
            to_state: default_states::IN_PROGRESS.to_owned(),
            db,
            event_bus: Arc::new(EventBus::new(16)),
            gate_config,
            workflow,
            project_version: None,
            project_workflow_definition: None,
            triggered_by: api_types::Actor::system(api_types::SystemComponent::Test),
            review_runner: None,
            merge_service: None,
            cleanup_scheduler: None,
            task_executor: Some(Arc::new(PendingExecutor { sender: tx })),
            daemon_connections: None,
            workspace_exec_locks: None,
            terminal_activity: None,
            workspace_root: workspace_root.path().to_path_buf(),
            repo_cache_locks: Some(Arc::new(RepoCacheLockManager::default())),
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

async fn build_initial_dispatch_harness(
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

async fn assign_agent_role(db: &SqliteDb, task_id: &str, role: &str, agent_id: &str) {
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

async fn build_test_ctx(
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

    HookContext {
        task_id: task_id.to_owned(),
        project_id,
        from_state: from_state.to_owned(),
        to_state: to_state.to_owned(),
        db,
        event_bus: Arc::new(EventBus::new(16)),
        gate_config,
        workflow,
        project_version: Some(project.version),
        project_workflow_definition: Some(project.workflow_definition),
        triggered_by: api_types::Actor::system(api_types::SystemComponent::Test),
        review_runner: None,
        merge_service: None,
        cleanup_scheduler: None,
        task_executor: None,
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

#[tokio::test]
async fn dependency_gate_skips_user_managed_transitions() {
    let mut ctx = build_test_ctx(
        "task-dependency-user-skip",
        default_states::TODO,
        default_states::PLANNING,
        None,
    )
    .await;
    ctx.triggered_by = api_types::Actor::user(api_types::UserActionSource::Api);

    let result = DependencyGate.execute(&ctx).await;

    assert!(matches!(
        result,
        HookResult::Skipped { reason } if reason.contains("user-managed transition")
    ));
}

async fn seed_transition_log(
    db: &SqliteDb,
    task_id: &str,
    from_state: &str,
    to_state: &str,
    rejection: bool,
) {
    seed_transition_log_at(db, task_id, from_state, to_state, rejection, &now_rfc3339()).await;
}

async fn seed_transition_log_at(
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

async fn seed_completed_executor_execution(ctx: &HookContext) -> String {
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

async fn seed_review(ctx: &HookContext, status: ReviewStatus, attempt_number: i64) -> String {
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

async fn seed_running_execution_for_task(db: &SqliteDb, task_id: &str, agent_id: &str, role: &str) {
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

#[tokio::test]
async fn run_ci_steps_empty_reason_contains_ci_steps() {
    let mut ctx = build_test_ctx(
        "task-run-review-ci-steps",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    ctx.state_config = json!({ "ci_steps": [] });

    let result = RunCiSteps.execute(&ctx).await;

    match result {
        HookResult::Skipped { reason } => assert!(reason.contains("ci steps")),
        other => panic!("expected skipped result, got {other:?}"),
    }
}

#[tokio::test]
async fn run_ci_steps_skips_when_ci_steps_empty() {
    let mut ctx = build_test_ctx(
        "task-run-review-empty",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    ctx.state_config = json!({ "ci_steps": [] });

    let result = RunCiSteps.execute(&ctx).await;

    match result {
        HookResult::Skipped { reason } => assert!(reason.contains("ci steps")),
        other => panic!("expected skipped result, got {other:?}"),
    }
}

#[tokio::test]
async fn run_ci_steps_skips_project_implementation_checks_for_discovery_tasks() {
    let mut ctx = build_test_ctx(
        "task-run-review-read-only",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    sqlx::query("UPDATE task SET task_type = 'discovery' WHERE id = ?")
        .bind(&ctx.task_id)
        .execute(ctx.db.pool())
        .await
        .expect("Task kind updates");
    ctx.state_config = json!({ "ci_steps": ["false"] });

    let result = RunCiSteps.execute(&ctx).await;

    match result {
        HookResult::Skipped { reason } => assert!(reason.contains("read-only Task")),
        other => panic!("expected read-only skip, got {other:?}"),
    }
    assert!(
        ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
            .await
            .expect("reviews load")
            .is_empty(),
        "a read-only Task must not create an implementation CI review attempt"
    );
}

#[tokio::test]
async fn run_ci_steps_skips_when_workspace_missing() {
    let mut ctx = build_test_ctx(
        "task-run-review-no-workspace",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    ctx.state_config = json!({ "ci_steps": ["cargo test"] });

    let result = RunCiSteps.execute(&ctx).await;

    match result {
        HookResult::Skipped { reason } => assert!(reason.contains("no workspace")),
        other => panic!("expected skipped result, got {other:?}"),
    }
}

#[tokio::test]
async fn run_ci_steps_skips_when_executor_execution_missing() {
    let mut ctx = build_test_ctx(
        "task-run-review-runner-missing",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    ctx.state_config = json!({ "ci_steps": ["cargo test"] });
    ctx.workspace_id = Some("workspace-1".to_owned());

    let result = RunCiSteps.execute(&ctx).await;

    match result {
        HookResult::Skipped { reason } => assert!(reason.contains("no executor execution")),
        other => panic!("expected skipped result, got {other:?}"),
    }
}

#[tokio::test]
async fn run_ci_steps_creates_passed_review_record() {
    let task_id = new_uuid_v4();
    let mut ctx = build_test_ctx(
        &task_id,
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    let execution_id = seed_completed_executor_execution(&ctx).await;
    ctx.execution_id = Some(execution_id);
    ctx.state_config = json!({ "ci_steps": ["test -d ."] });

    enter_target_state(&ctx).await;
    let result = RunCiSteps.execute(&ctx).await;

    assert!(matches!(result, HookResult::Ok));
    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::Passed);
}

#[tokio::test]
async fn run_ci_steps_pass_then_dispatches_reviewer() {
    let agent_id = "agent-reviewer-dispatch";
    let mut harness = build_role_dispatch_harness(
        "task-run-ci-reviewer-dispatch",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        default_roles::REVIEWER,
        agent_id,
        1,
    )
    .await;
    let mut ctx = harness.ctx.clone();
    let execution_id = seed_completed_executor_execution(&ctx).await;
    ctx.execution_id = Some(execution_id);
    ctx.state_config = json!({ "ci_steps": ["test -d ."] });

    let ci_result = RunCiSteps.execute(&ctx).await;
    assert!(matches!(ci_result, HookResult::Ok), "{ci_result:?}");

    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::Running);

    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    assert!(
        matches!(dispatch_result, HookResult::Ok),
        "{dispatch_result:?}"
    );

    let execution_ctx = tokio::time::timeout(std::time::Duration::from_secs(1), harness.rx.recv())
        .await
        .expect("reviewer executor spawned in time")
        .expect("reviewer execution context received");
    assert_eq!(execution_ctx.task_id, ctx.task_id);
    // The shell reviewer receives the frozen contract instead of a canned verdict.
    assert!(execution_ctx.description.contains("FORGE_REVIEW_CONTRACT"));
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(&*ctx.db, &ctx.task_id, default_roles::REVIEWER)
            .await
            .expect("reviewer execution count loads"),
        1
    );
}

#[tokio::test]
async fn review_create_rejects_stale_task_snapshot_without_inserting_attempt() {
    let task_id = new_uuid_v4();
    let harness =
        build_reviewer_dispatch_harness(&task_id, "agent-review-authority-create", 1, vec!["true"])
            .await;
    let ctx = harness.ctx;
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let candidate_id = ctx
        .execution_id
        .clone()
        .expect("implementation candidate seeded");
    let stale_version = task.version;

    TaskRepo::set_entry_barrier(
        &*ctx.db,
        &task.id,
        stale_version,
        Some(json!({ "authority_race": true }).to_string()),
        &now_rfc3339(),
    )
    .await
    .expect("competing Task update wins");

    let now = now_rfc3339();
    let error = ReviewRepo::create_with_task_authority(
        &*ctx.db,
        CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate_id.clone(),
            attempt_number: 0,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
        stale_version,
        default_states::REVIEW,
        ctx.project_version,
        ctx.project_workflow_definition.as_deref(),
        Some(&candidate_id),
    )
    .await
    .expect_err("stale Task authority must reject the Review insert");
    assert!(matches!(error, db::DbError::VersionConflict));
    assert!(ReviewRepo::list_by_task(&*ctx.db, &task.id)
        .await
        .expect("reviews load")
        .is_empty());
}

#[tokio::test]
async fn review_settlement_rejects_stale_project_authority_without_mutation() {
    let task_id = new_uuid_v4();
    let harness =
        build_reviewer_dispatch_harness(&task_id, "agent-review-authority-settle", 1, vec!["true"])
            .await;
    let ctx = harness.ctx;
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let project = ProjectRepo::get_by_id(&*ctx.db, &task.project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    let candidate_id = ctx
        .execution_id
        .clone()
        .expect("implementation candidate seeded");
    let now = now_rfc3339();
    let review = ReviewRepo::create_with_task_authority(
        &*ctx.db,
        CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate_id.clone(),
            attempt_number: 0,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
        task.version,
        default_states::REVIEW,
        Some(project.version),
        Some(project.workflow_definition.as_str()),
        Some(&candidate_id),
    )
    .await
    .expect("review creates under the captured authority");

    ProjectRepo::update_workflow(
        &*ctx.db,
        &project.id,
        "{\"workflow\":\"edited\"}",
        None,
        project.version,
        &now_rfc3339(),
    )
    .await
    .expect("competing Project workflow update wins");

    let error = ReviewRepo::update_status_with_review_authority_and_task_projection(
        &*ctx.db,
        &review.id,
        ReviewStatus::Failed,
        json!({ "ci_steps": [] }).to_string(),
        Some(now_rfc3339()),
        &now_rfc3339(),
        task.version,
        default_states::REVIEW,
        Some(project.version),
        Some(project.workflow_definition.as_str()),
        review.status.clone(),
        &review.updated_at,
        &candidate_id,
        Some(None),
    )
    .await
    .expect_err("stale Project authority must reject settlement");
    assert!(matches!(error, db::DbError::VersionConflict));

    let review_after = ReviewRepo::get_by_id(&*ctx.db, &review.id)
        .await
        .expect("review reloads")
        .expect("review remains present");
    assert_eq!(review_after.status, ReviewStatus::Running);
    let task_after = TaskRepo::get_by_id(&*ctx.db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task remains present");
    assert_eq!(task_after.version, task.version);
    assert!(task_after.review_passed_at.is_none());
}

#[tokio::test]
async fn review_authority_loss_cancels_after_candidate_moves_without_task_projection() {
    let task_id = new_uuid_v4();
    let harness =
        build_reviewer_dispatch_harness(&task_id, "agent-review-authority-cancel", 1, Vec::new())
            .await;
    let ctx = harness.ctx;
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let project = ProjectRepo::get_by_id(&*ctx.db, &task.project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    let candidate_id = ctx
        .execution_id
        .clone()
        .expect("implementation candidate seeded");
    let now = now_rfc3339();
    let review = ReviewRepo::create_with_task_authority(
        &*ctx.db,
        CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate_id,
            attempt_number: 0,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
        task.version,
        default_states::REVIEW,
        Some(project.version),
        Some(project.workflow_definition.as_str()),
        ctx.execution_id.as_deref(),
    )
    .await
    .expect("review creates under the captured authority");
    let workspace = WorkspaceRepo::get_by_task_id(&*ctx.db, &ctx.task_id)
        .await
        .expect("workspace loads")
        .expect("workspace exists");
    let replacement_candidate_id = new_uuid_v4();
    let now = now_rfc3339();
    ExecutionRepo::create(
        &*ctx.db,
        CreateExecution {
            id: replacement_candidate_id,
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
            summary: Some("replacement executor summary".to_owned()),
            logs_path: None,
            before_sha: workspace.before_sha.clone(),
            after_sha: workspace.before_sha,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace.id),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("replacement execution creates");

    super::common::cancel_review_after_authority_loss(&ctx, &review, "candidate moved").await;

    let cancelled = ReviewRepo::get_by_id(&*ctx.db, &review.id)
        .await
        .expect("review reloads")
        .expect("review exists");
    assert_eq!(cancelled.status, ReviewStatus::Cancelled);
    let details: serde_json::Value =
        serde_json::from_str(&cancelled.step_results_json).expect("review details parse");
    assert_eq!(
        details["execution_retry"]["status"],
        "cancelled_authority_lost"
    );
    let task_after = TaskRepo::get_by_id(&*ctx.db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert_eq!(task_after.version, task.version);
    assert_eq!(task_after.review_passed_at, task.review_passed_at);
}

#[tokio::test]
async fn merge_fix_re_review_still_requires_an_assigned_reviewer() {
    let agent_id = "agent-reviewer-ci-only";
    let mut harness = build_role_dispatch_harness(
        "task-run-ci-reviewer-ci-only",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        default_roles::REVIEWER,
        agent_id,
        1,
    )
    .await;
    let mut ctx = harness.ctx.clone();
    let execution_id = seed_completed_executor_execution(&ctx).await;
    ctx.execution_id = Some(execution_id);
    ctx.state_config = json!({ "ci_steps": ["test -d ."] });
    TaskRepo::set_review_passed_at(&*ctx.db, &ctx.task_id, Some(now_rfc3339()), &now_rfc3339())
        .await
        .expect("review_passed_at seeds");

    let ci_result = RunCiSteps.execute(&ctx).await;
    assert!(matches!(ci_result, HookResult::Ok), "{ci_result:?}");

    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load");
    // Content changed under the merge fix, so a passing CI run no longer
    // substitutes for the assigned reviewer's conformance assessment.
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::Running);
    assert!(!reviews[0].step_results_json.contains("pass_ci_only"));

    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    assert!(
        matches!(dispatch_result, HookResult::Ok),
        "{dispatch_result:?}"
    );
    let execution_ctx = tokio::time::timeout(std::time::Duration::from_secs(1), harness.rx.recv())
        .await
        .expect("reviewer executor spawned in time")
        .expect("reviewer execution context received");
    assert_eq!(execution_ctx.task_id, ctx.task_id);
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(&*ctx.db, &ctx.task_id, default_roles::REVIEWER)
            .await
            .expect("reviewer execution count loads"),
        1
    );
}

#[tokio::test]
async fn run_ci_steps_failure_prevents_reviewer_dispatch() {
    let agent_id = "agent-reviewer-ci-fail";
    let mut harness = build_role_dispatch_harness(
        "task-run-ci-reviewer-fail",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        default_roles::REVIEWER,
        agent_id,
        1,
    )
    .await;
    let mut ctx = harness.ctx.clone();
    let execution_id = seed_completed_executor_execution(&ctx).await;
    ctx.execution_id = Some(execution_id);
    ctx.state_config = json!({ "ci_steps": ["false"] });

    let ci_result = RunCiSteps.execute(&ctx).await;
    assert!(
        matches!(ci_result, HookResult::Failed { .. }),
        "{ci_result:?}"
    );

    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::Failed);

    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    match dispatch_result {
        HookResult::Skipped { reason } => assert_eq!(reason, "review already failed"),
        other => panic!("expected skipped dispatch, got {other:?}"),
    }
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), harness.rx.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn run_ci_steps_keeps_review_running_when_reviewer_at_capacity() {
    let agent_id = "agent-reviewer-capacity";
    let mut harness = build_role_dispatch_harness(
        "task-run-ci-reviewer-capacity",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        default_roles::REVIEWER,
        agent_id,
        1,
    )
    .await;
    let mut ctx = harness.ctx.clone();
    let execution_id = seed_completed_executor_execution(&ctx).await;
    ctx.execution_id = Some(execution_id);
    ctx.state_config = json!({ "ci_steps": ["test -d ."] });

    let current_task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let other_task_id = "task-run-ci-reviewer-capacity-other";
    TaskRepo::create(
        &*ctx.db,
        CreateTask {
            id: other_task_id.to_owned(),
            project_id: current_task.project_id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "other review task".to_owned(),
            description: Some("echo other".to_owned()),
            task_type: "task".to_owned(),
            status: default_states::REVIEW.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("other task creates");
    assign_agent_role(&ctx.db, other_task_id, default_roles::REVIEWER, agent_id).await;
    seed_running_execution_for_task(&ctx.db, other_task_id, agent_id, default_roles::REVIEWER)
        .await;
    crate::test_support::set_test_agent_capacity(&ctx.db, agent_id, 1).await;

    let ci_result = RunCiSteps.execute(&ctx).await;
    assert!(matches!(ci_result, HookResult::Ok), "{ci_result:?}");

    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    match dispatch_result {
        HookResult::Skipped { reason } => assert_eq!(reason, "agent at capacity"),
        other => panic!("expected capacity skip, got {other:?}"),
    }

    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::Running);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), harness.rx.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn no_ci_review_attempt_starts_before_pause_and_capacity_checks() {
    let agent_id = "agent-reviewer-no-ci-capacity";
    let mut harness = build_role_dispatch_harness(
        "task-review-no-ci-capacity",
        default_states::MERGE_FAILED,
        default_states::REVIEW,
        default_roles::REVIEWER,
        agent_id,
        1,
    )
    .await;
    let mut ctx = harness.ctx.clone();
    ctx.state_config = json!({ "ci_steps": [] });

    let old_candidate = seed_review(&ctx, ReviewStatus::Passed, 1).await;
    sqlx::query("UPDATE execution SET created_at = ?, updated_at = ? WHERE id = ?")
        .bind("2000-01-01T00:00:00Z")
        .bind("2000-01-01T00:00:01Z")
        .bind(&old_candidate)
        .execute(ctx.db.pool())
        .await
        .expect("old candidate timestamp updates");
    let current_candidate = seed_completed_executor_execution(&ctx).await;
    ctx.execution_id = Some(current_candidate.clone());

    let current_task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let other_task_id = "task-review-no-ci-capacity-other";
    TaskRepo::create(
        &*ctx.db,
        CreateTask {
            id: other_task_id.to_owned(),
            project_id: current_task.project_id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "other review task".to_owned(),
            description: Some("echo other".to_owned()),
            task_type: "task".to_owned(),
            status: default_states::REVIEW.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("other task creates");
    assign_agent_role(&ctx.db, other_task_id, default_roles::REVIEWER, agent_id).await;
    seed_running_execution_for_task(&ctx.db, other_task_id, agent_id, default_roles::REVIEWER)
        .await;
    crate::test_support::set_test_agent_capacity(&ctx.db, agent_id, 1).await;

    ProjectRepo::set_paused_at(&*ctx.db, &current_task.project_id, Some(now_rfc3339()))
        .await
        .expect("project pauses");
    refresh_project_authority(&mut ctx).await;
    let paused_result = DispatchRoleAgent.execute(&ctx).await;
    match paused_result {
        HookResult::Skipped { reason } => assert_eq!(reason, "project paused"),
        other => panic!("expected pause skip, got {other:?}"),
    }
    let paused_reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load while paused");
    assert_eq!(
        paused_reviews.len(),
        1,
        "paused dispatch must not create a review attempt"
    );
    ProjectRepo::set_paused_at(&*ctx.db, &current_task.project_id, None)
        .await
        .expect("project resumes");
    refresh_project_authority(&mut ctx).await;

    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    match dispatch_result {
        HookResult::Skipped { reason } => assert_eq!(reason, "agent at capacity"),
        other => panic!("expected capacity skip, got {other:?}"),
    }

    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 2);
    let current_review = reviews
        .iter()
        .max_by_key(|review| review.attempt_number)
        .expect("current review exists");
    assert_eq!(current_review.status, ReviewStatus::Running);
    assert_eq!(current_review.execution_id, current_candidate);
    assert!(matches!(
        AutoCascadeOnReviewPass.execute(&ctx).await,
        HookResult::Ok
    ));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), harness.rx.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn run_ci_steps_without_reviewer_cascades_to_merging() {
    let task_id = new_uuid_v4();
    let mut ctx = build_test_ctx(
        &task_id,
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    let execution_id = seed_completed_executor_execution(&ctx).await;
    ctx.execution_id = Some(execution_id);
    ctx.state_config = json!({ "ci_steps": ["test -d ."] });

    enter_target_state(&ctx).await;
    let ci_result = RunCiSteps.execute(&ctx).await;
    assert!(matches!(ci_result, HookResult::Ok), "{ci_result:?}");

    match AutoCascadeOnReviewPass.execute(&ctx).await {
        HookResult::Cascade { to, reason } => {
            assert_eq!(to, default_states::MERGING);
            assert_eq!(reason, "review passed");
        }
        other => panic!("expected cascade to merging, got {other:?}"),
    }
}

#[tokio::test]
async fn cached_passed_review_cannot_cascade_after_its_authority_is_cleared() {
    let task_id = new_uuid_v4();
    let mut ctx = build_test_ctx(
        &task_id,
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    let execution_id = seed_completed_executor_execution(&ctx).await;
    ctx.execution_id = Some(execution_id);
    ctx.state_config = json!({ "ci_steps": ["test -d ."] });
    enter_target_state(&ctx).await;
    let ci_result = RunCiSteps.execute(&ctx).await;
    assert!(matches!(ci_result, HookResult::Ok), "{ci_result:?}");
    TaskRepo::set_review_passed_at(&*ctx.db, &ctx.task_id, None, &now_rfc3339())
        .await
        .expect("cached review authority clears");

    let cascade_result = AutoCascadeOnReviewPass.execute(&ctx).await;

    assert!(
        matches!(cascade_result, HookResult::Ok),
        "an old passed row without current review authority must be inert: {cascade_result:?}"
    );
}

#[tokio::test]
async fn passed_review_defers_integration_while_project_is_paused() {
    let task_id = new_uuid_v4();
    let mut ctx = build_test_ctx(
        &task_id,
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    TaskRepo::update_status(
        &*ctx.db,
        UpdateTaskStatus {
            id: task.id,
            expected_version: task.version,
            status: default_states::REVIEW.to_owned(),
            assignee_id: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("task enters review");
    let execution_id = seed_completed_executor_execution(&ctx).await;
    ctx.execution_id = Some(execution_id);
    ctx.state_config = json!({ "ci_steps": ["test -d ."] });
    let ci_result = RunCiSteps.execute(&ctx).await;
    assert!(matches!(ci_result, HookResult::Ok), "{ci_result:?}");
    ProjectRepo::set_paused_at(&*ctx.db, &ctx.project_id, Some(now_rfc3339()))
        .await
        .expect("project pauses");
    refresh_project_authority(&mut ctx).await;

    let cascade_result = AutoCascadeOnReviewPass.execute(&ctx).await;

    match cascade_result {
        HookResult::Skipped { reason } => {
            assert_eq!(reason, "project paused; integration deferred")
        }
        other => panic!("expected paused integration skip, got {other:?}"),
    }
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let deferred = crate::deferred_dispatch::paused_integration(&task)
        .expect("paused integration marker exists");
    assert_eq!(deferred.state, default_states::REVIEW);
}

#[tokio::test]
async fn run_ci_steps_with_user_approval_gate_waits_for_human() {
    let task_id = new_uuid_v4();
    let mut ctx = build_test_ctx(
        &task_id,
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    ctx.gate_config
        .as_mut()
        .expect("review gate config")
        .requires_user_approval = Some(true);
    let execution_id = seed_completed_executor_execution(&ctx).await;
    ctx.execution_id = Some(execution_id);
    ctx.state_config = json!({ "ci_steps": ["test -d ."] });

    enter_target_state(&ctx).await;
    let ci_result = RunCiSteps.execute(&ctx).await;
    assert!(matches!(ci_result, HookResult::Ok), "{ci_result:?}");

    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::AwaitingHuman);
    assert!(reviews[0].finished_at.is_none());

    let cascade_result = AutoCascadeOnReviewPass.execute(&ctx).await;
    assert!(
        matches!(cascade_result, HookResult::Ok),
        "{cascade_result:?}"
    );
}

#[tokio::test]
async fn unconfigured_review_with_user_approval_gate_waits_for_human() {
    let task_id = new_uuid_v4();
    let mut ctx = build_test_ctx(
        &task_id,
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    ctx.gate_config
        .as_mut()
        .expect("review gate config")
        .requires_user_approval = Some(true);
    let execution_id = seed_completed_executor_execution(&ctx).await;
    ctx.execution_id = Some(execution_id);

    enter_target_state(&ctx).await;
    let result = super::AutoCascadeOnUnconfiguredReview.execute(&ctx).await;

    assert!(matches!(result, HookResult::Ok), "{result:?}");
    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::AwaitingHuman);
}

#[tokio::test]
async fn check_retry_budget_allows_review_entry_when_rejections_reach_max() {
    let ctx = build_test_ctx(
        "task-retry-budget-cascade",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    seed_transition_log(
        &ctx.db,
        &ctx.task_id,
        default_states::REVIEW,
        default_states::IN_PROGRESS,
        true,
    )
    .await;
    seed_transition_log(
        &ctx.db,
        &ctx.task_id,
        default_states::REVIEW,
        default_states::IN_PROGRESS,
        true,
    )
    .await;
    let result = CheckRetryBudget.execute(&ctx).await;

    assert!(matches!(result, HookResult::Ok), "{result:?}");
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(task.blocked_json, None);
}

#[tokio::test]
async fn check_retry_budget_ok_when_below_max() {
    let ctx = build_test_ctx(
        "task-retry-budget-ok",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    seed_transition_log(
        &ctx.db,
        &ctx.task_id,
        default_states::REVIEW,
        default_states::IN_PROGRESS,
        true,
    )
    .await;

    let result = CheckRetryBudget.execute(&ctx).await;

    assert!(matches!(result, HookResult::Ok));
}

#[tokio::test]
async fn check_retry_budget_ok_without_gate_config() {
    let mut ctx = build_test_ctx(
        "task-retry-budget-no-config",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    ctx.gate_config = None;

    let result = CheckRetryBudget.execute(&ctx).await;

    assert!(matches!(result, HookResult::Ok));
}

#[tokio::test]
async fn auto_cascade_review_failure_at_budget_blocks_with_metadata() {
    let mut ctx = build_test_ctx(
        "task-review-budget-final-attempt",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    ctx.execution_id = Some(new_uuid_v4());
    seed_review(&ctx, ReviewStatus::Failed, 1).await;
    seed_transition_log_at(
        &ctx.db,
        &ctx.task_id,
        default_states::REVIEW,
        default_states::IN_PROGRESS,
        true,
        "2026-04-17T00:00:00Z",
    )
    .await;

    match AutoCascadeOnReviewPass.execute(&ctx).await {
        HookResult::Ok => {}
        other => panic!("expected review budget block, got {other:?}"),
    }
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let blocked = task.blocked_json.expect("blocked metadata is set");
    let blocked: serde_json::Value = serde_json::from_str(&blocked).expect("blocked json parses");
    assert_eq!(blocked["kind"], "review_gate_failed");
    assert_eq!(blocked["reason"], "review retry budget exhausted");
}

#[tokio::test]
async fn auto_cascade_review_failure_budget_blocks_with_metadata() {
    let mut ctx = build_test_ctx(
        "task-review-budget-blocks",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    ctx.execution_id = Some(new_uuid_v4());
    seed_review(&ctx, ReviewStatus::Failed, 1).await;
    seed_transition_log_at(
        &ctx.db,
        &ctx.task_id,
        default_states::REVIEW,
        default_states::IN_PROGRESS,
        true,
        "2026-04-17T00:00:00Z",
    )
    .await;
    seed_transition_log_at(
        &ctx.db,
        &ctx.task_id,
        default_states::REVIEW,
        default_states::IN_PROGRESS,
        true,
        "2026-04-17T00:01:00Z",
    )
    .await;
    let mut rx = ctx.event_bus.subscribe();

    match AutoCascadeOnReviewPass.execute(&ctx).await {
        HookResult::Ok => {}
        other => panic!("expected review budget block, got {other:?}"),
    }
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(task.status, default_states::IN_PROGRESS);
    let blocked = task.blocked_json.expect("blocked metadata is set");
    let blocked: serde_json::Value = serde_json::from_str(&blocked).expect("blocked json parses");
    assert_eq!(blocked["kind"], "review_gate_failed");
    assert_eq!(blocked["reason"], "review retry budget exhausted");
    let event = rx.try_recv().expect("blocked event emits");
    assert_eq!(event.event_type, "task.blocked");
    match event.context {
        EventContext::TaskBlocked { reason, kind, .. } => {
            assert_eq!(reason, "review retry budget exhausted");
            assert_eq!(kind, Some(api_types::FailureKind::ReviewGateFailed));
        }
        other => panic!("expected task blocked event, got {other:?}"),
    }
}

#[tokio::test]
async fn require_upstream_roles_completed_fails_when_planner_assigned_without_planning_log() {
    let ctx = build_test_ctx(
        "task-upstream-missing-planning",
        default_states::TODO,
        default_states::IN_PROGRESS,
        Some((default_roles::PLANNER, "agent-planner-missing-log")),
    )
    .await;

    let result = RequireUpstreamRolesCompleted.execute(&ctx).await;

    match result {
        HookResult::Failed { reason } => {
            assert!(reason.contains(default_roles::PLANNER));
            assert!(reason.contains(default_states::PLANNING));
        }
        other => panic!("expected failed result, got {other:?}"),
    }
}

#[tokio::test]
async fn require_upstream_roles_completed_ok_when_no_planner_assigned() {
    let ctx = build_test_ctx(
        "task-upstream-no-planner",
        default_states::TODO,
        default_states::IN_PROGRESS,
        None,
    )
    .await;

    let result = RequireUpstreamRolesCompleted.execute(&ctx).await;

    assert!(matches!(result, HookResult::Ok));
}

#[tokio::test]
async fn require_upstream_roles_completed_ok_when_planning_log_exists() {
    let ctx = build_test_ctx(
        "task-upstream-planning-log",
        default_states::TODO,
        default_states::IN_PROGRESS,
        Some((default_roles::PLANNER, "agent-planner-with-log")),
    )
    .await;
    seed_transition_log(
        &ctx.db,
        &ctx.task_id,
        default_states::TODO,
        default_states::PLANNING,
        false,
    )
    .await;

    let result = RequireUpstreamRolesCompleted.execute(&ctx).await;

    assert!(matches!(result, HookResult::Ok));
}

#[tokio::test]
async fn dispatch_role_agent_skips_without_coder_assignment() {
    let ctx = build_test_ctx(
        "task-dispatch-no-coder",
        default_states::TODO,
        default_states::IN_PROGRESS,
        None,
    )
    .await;

    let result = DispatchRoleAgent.execute(&ctx).await;

    assert!(matches!(result, HookResult::Skipped { .. }));
}

#[tokio::test]
async fn dispatch_role_agent_emits_event_for_coder_assignment() {
    let agent_id = "agent-coder-dispatch";
    let mut harness = build_initial_dispatch_harness("task-dispatch-coder", agent_id, 1).await;
    let ctx = harness.ctx.clone();
    let mut rx = ctx.event_bus.subscribe();

    let result = DispatchRoleAgent.execute(&ctx).await;

    assert!(matches!(result, HookResult::Ok));
    let event = rx.try_recv().expect("dispatch event emits");
    assert_eq!(event.event_type, "task.role_agent_dispatched");
    assert_eq!(event.entity_id, ctx.task_id);
    match event.context {
        EventContext::TaskRoleAgentDispatched {
            task_id,
            role,
            agent_id: dispatched_agent_id,
            state,
            prompt_system,
            prompt_user,
            parent_execution_id: _,
        } => {
            assert_eq!(task_id, ctx.task_id);
            assert_eq!(role, default_roles::CODER);
            assert_eq!(dispatched_agent_id, agent_id);
            assert_eq!(state, default_states::IN_PROGRESS);
            assert!(prompt_system.contains("coder"));
            assert!(prompt_user.contains("test task"));
        }
        other => panic!("unexpected event context: {other:?}"),
    }

    let execution_ctx = tokio::time::timeout(std::time::Duration::from_secs(1), harness.rx.recv())
        .await
        .expect("executor receives dispatch")
        .expect("executor context exists");
    assert!(execution_ctx
        .description
        .contains("Forge role contract (authoritative):"));
    assert!(execution_ctx.description.contains("Coder boundary:"));
    assert!(execution_ctx.description.contains("test task"));
}

#[tokio::test]
async fn dispatch_role_agent_uses_dirty_worktree_prompt_from_task_annotation() {
    let agent_id = "agent-coder-dirty-worktree";
    let mut harness = build_role_dispatch_harness(
        "task-dispatch-dirty-worktree",
        default_states::MERGING,
        default_states::MERGE_FAILED,
        default_roles::CODER,
        agent_id,
        1,
    )
    .await;
    sqlx::query(
        "UPDATE task
         SET review_passed_at = ?, error_annotation = ?
         WHERE id = ?",
    )
    .bind("2026-04-17T10:00:00Z")
    .bind(
        json!({
            "type": "dirty_worktree",
            "message": "worktree has uncommitted changes: src/lib.rs"
        })
        .to_string(),
    )
    .bind(&harness.ctx.task_id)
    .execute(harness.ctx.db.pool())
    .await
    .expect("dirty worktree annotation persists");

    let dispatch_result = DispatchRoleAgent.execute(&harness.ctx).await;
    assert!(
        matches!(dispatch_result, HookResult::Ok),
        "{dispatch_result:?}"
    );

    let execution_ctx = tokio::time::timeout(std::time::Duration::from_secs(1), harness.rx.recv())
        .await
        .expect("coder executor spawned in time")
        .expect("coder execution context received");
    assert!(execution_ctx.description.contains("uncommitted changes"));
    assert!(execution_ctx
        .description
        .contains("do not rebase or discard it"));
    assert!(!execution_ctx
        .description
        .contains("Rebase your worktree branch onto the latest default branch"));
}

#[tokio::test]
async fn review_refresh_bypasses_merge_fix_notification_and_dispatch() {
    let agent_id = "agent-coder-review-refresh";
    let mut harness = build_role_dispatch_harness(
        "task-review-refresh",
        default_states::MERGING,
        default_states::MERGE_FAILED,
        default_roles::CODER,
        agent_id,
        1,
    )
    .await;
    sqlx::query(
        "INSERT INTO transition_log
         (id, task_id, from_state, to_state, trigger_name, triggered_by,
          trigger_reason, hook_results_json, rejection, created_at)
         VALUES (?, ?, ?, ?, 'retry', 'system:workflow', ?, NULL, 0, ?)",
    )
    .bind(new_uuid_v4())
    .bind(&harness.ctx.task_id)
    .bind(default_states::MERGING)
    .bind(default_states::MERGE_FAILED)
    .bind(format!(
        "{} reviewed commit changed; fresh review required",
        crate::workflow::REVIEW_REFRESH_MARKER
    ))
    .bind(now_rfc3339())
    .execute(harness.ctx.db.pool())
    .await
    .expect("review-refresh transition inserts");

    let notify = NotifyRoleHolder.execute(&harness.ctx).await;
    assert!(matches!(
        notify,
        HookResult::Skipped { reason } if reason.contains("requires review")
    ));

    let dispatch = DispatchRoleAgent.execute(&harness.ctx).await;
    assert!(matches!(
        dispatch,
        HookResult::Cascade { to, reason }
            if to == default_states::REVIEW
                && reason.contains(crate::workflow::REVIEW_REFRESH_MARKER)
    ));
    assert!(harness.rx.try_recv().is_err());
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*harness.ctx.db,
            &harness.ctx.task_id,
            default_roles::CODER,
        )
        .await
        .expect("coder execution count loads"),
        0
    );
}

#[test]
fn review_refresh_bridge_does_not_reset_or_spend_merge_fix_window() {
    let task_id = "task-merge-fix-boundary".to_owned();
    let workflow_actor = api_types::Actor::system(api_types::SystemComponent::Workflow).display();
    let now = now_rfc3339();
    let rejection = |reason: &str| db::TransitionLog {
        id: new_uuid_v4(),
        task_id: task_id.clone(),
        from_state: default_states::MERGING.to_owned(),
        to_state: default_states::MERGE_FAILED.to_owned(),
        trigger_name: Some("retry".to_owned()),
        triggered_by: workflow_actor.clone(),
        trigger_reason: reason.to_owned(),
        hook_results_json: None,
        rejection: true,
        created_at: now.clone(),
    };
    let bridge = db::TransitionLog {
        id: new_uuid_v4(),
        task_id: task_id.clone(),
        from_state: default_states::MERGING.to_owned(),
        to_state: default_states::MERGE_FAILED.to_owned(),
        trigger_name: Some("retry".to_owned()),
        triggered_by: workflow_actor.clone(),
        trigger_reason: format!(
            "{} target advanced; re-review required",
            crate::workflow::REVIEW_REFRESH_MARKER
        ),
        hook_results_json: None,
        // A direct caller may supply rejection=true; the engine normalizes the
        // durable row, and the counter remains defensive for pre-existing rows.
        rejection: true,
        created_at: now.clone(),
    };

    assert_eq!(
        crate::task_diagnostics::count_gate_rejections_since_boundary(
            &[rejection("merge conflict"), bridge],
            default_states::MERGING,
        ),
        1,
        "a review-refresh bridge must not erase or consume the prior window"
    );
}

#[tokio::test]
async fn coordination_root_merge_conflict_becomes_manual_repair_block() {
    let ctx = build_test_ctx(
        "task-root-merge-conflict",
        default_states::MERGING,
        default_states::MERGING,
        None,
    )
    .await;
    let now = now_rfc3339();
    TaskRepo::create(
        &*ctx.db,
        CreateTask {
            id: new_uuid_v4(),
            project_id: ctx.project_id.clone(),
            parent_task_id: Some(ctx.task_id.clone()),
            subtask_order: Some(0),
            assignee_type: None,
            assignee_id: None,
            title: "completed child".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: default_states::DONE.to_owned(),
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
    .expect("coordination child creates");
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("root loads")
        .expect("root exists");

    let result = merge_failure_result(
        &ctx,
        &task,
        "merge conflict in shared branch".to_owned(),
        api_types::FailureKind::MergeConflict,
    )
    .await;

    assert!(matches!(result, HookResult::Ok));
    let blocked = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("blocked root loads")
        .expect("blocked root exists");
    assert!(blocked.blocked_json.is_some());
    let annotation: api_types::TaskAnnotation = serde_json::from_str(
        blocked
            .error_annotation
            .as_deref()
            .expect("root has recovery annotation"),
    )
    .expect("recovery annotation parses");
    let api_types::TaskAnnotation::Blocking(annotation) = annotation else {
        panic!("coordination merge block must be typed")
    };
    assert_eq!(annotation.blocked_by.as_deref(), Some("coordination_root"));
    assert_eq!(
        annotation.recovery_actions,
        vec![
            api_types::RecoveryAction::RetryHook,
            api_types::RecoveryAction::CancelTask
        ]
    );
}

#[tokio::test]
async fn ordinary_merge_conflict_parks_for_manual_repair_and_invalidates_review() {
    let ctx = build_test_ctx(
        "task-merge-conflict",
        default_states::MERGING,
        default_states::MERGING,
        None,
    )
    .await;
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let task = TaskRepo::set_review_passed_at_cas(
        &*ctx.db,
        &task.id,
        task.version,
        Some("2026-09-12T10:00:00Z".to_owned()),
        &now_rfc3339(),
    )
    .await
    .expect("review marker sets");

    let result = merge_failure_result(
        &ctx,
        &task,
        "conflict in src/lib.rs".to_owned(),
        api_types::FailureKind::MergeConflict,
    )
    .await;

    assert!(matches!(result, HookResult::Ok));
    let blocked = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("blocked task loads")
        .expect("blocked task exists");
    assert!(blocked.blocked_json.is_some());
    assert_eq!(blocked.review_passed_at, None);
    let annotation: api_types::TaskAnnotation = serde_json::from_str(
        blocked
            .error_annotation
            .as_deref()
            .expect("task has recovery annotation"),
    )
    .expect("recovery annotation parses");
    let api_types::TaskAnnotation::Blocking(annotation) = annotation else {
        panic!("merge conflict block must be typed")
    };
    assert_eq!(
        annotation.blocked_by.as_deref(),
        Some("manual_workspace_repair")
    );
    assert_eq!(
        annotation.recovery_actions,
        vec![
            api_types::RecoveryAction::RetryHook,
            api_types::RecoveryAction::CancelTask
        ]
    );
}

#[tokio::test]
async fn dispatch_role_agent_initial_dispatch_creates_execution_with_capacity() {
    let agent_id = "agent-coder-initial";
    let mut harness = build_initial_dispatch_harness("task-dispatch-initial", agent_id, 1).await;
    let ctx = harness.ctx.clone();

    let result = DispatchRoleAgent.execute(&ctx).await;

    assert!(matches!(result, HookResult::Ok), "{result:?}");
    let execution_ctx = tokio::time::timeout(std::time::Duration::from_secs(1), harness.rx.recv())
        .await
        .expect("executor spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, ctx.task_id);

    let executions = ExecutionRepo::list_by_task_and_role(
        &*ctx.db,
        &ctx.task_id,
        default_roles::CODER,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions load");
    assert_eq!(executions.items.len(), 1);
    assert_eq!(executions.items[0].status, ExecutionStatus::Running);
    assert_eq!(executions.items[0].agent_id.as_deref(), Some(agent_id));
}

#[tokio::test]
async fn dispatch_role_agent_fails_initial_dispatch_without_project_repo() {
    let agent_id = "agent-coder-no-repo";
    let mut harness = build_no_repo_dispatch_harness("task-dispatch-no-repo", agent_id, 1).await;
    let ctx = harness.ctx.clone();

    let result = DispatchRoleAgent.execute(&ctx).await;

    match result {
        HookResult::Failed { reason } => assert_eq!(
            reason,
            format!("project {} has no primary repo", ctx.project_id)
        ),
        other => panic!("expected failed result, got {other:?}"),
    }
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), harness.rx.recv())
            .await
            .is_err()
    );
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(&*ctx.db, &ctx.task_id, default_roles::CODER)
            .await
            .expect("execution count loads"),
        0
    );
}

#[tokio::test]
async fn dispatch_role_agent_initial_dispatch_skips_at_capacity() {
    let agent_id = "agent-coder-capacity";
    let mut harness = build_initial_dispatch_harness("task-dispatch-capacity", agent_id, 1).await;
    let ctx = harness.ctx.clone();
    let other_task_id = "task-dispatch-capacity-other";
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    TaskRepo::create(
        &*ctx.db,
        CreateTask {
            id: other_task_id.to_owned(),
            project_id: task.project_id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "other task".to_owned(),
            description: Some("echo other".to_owned()),
            task_type: "task".to_owned(),
            status: default_states::IN_PROGRESS.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("other task creates");
    assign_agent_role(&ctx.db, other_task_id, default_roles::CODER, agent_id).await;
    seed_running_execution_for_task(&ctx.db, other_task_id, agent_id, default_roles::CODER).await;
    crate::test_support::set_test_agent_capacity(&ctx.db, agent_id, 1).await;

    let result = DispatchRoleAgent.execute(&ctx).await;

    match result {
        HookResult::Skipped { reason } => assert_eq!(reason, "agent at capacity"),
        other => panic!("expected skipped result, got {other:?}"),
    }
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), harness.rx.recv())
            .await
            .is_err()
    );
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(&*ctx.db, &ctx.task_id, default_roles::CODER)
            .await
            .expect("execution count loads"),
        0
    );
}

#[tokio::test]
async fn dispatch_role_agent_initial_dispatch_skips_when_execution_already_running() {
    let agent_id = "agent-coder-running";
    let mut harness = build_initial_dispatch_harness("task-dispatch-running", agent_id, 1).await;
    let ctx = harness.ctx.clone();
    seed_running_execution_for_task(&ctx.db, &ctx.task_id, agent_id, "executor").await;

    let result = DispatchRoleAgent.execute(&ctx).await;

    match result {
        HookResult::Skipped { reason } => assert_eq!(reason, "execution already running"),
        other => panic!("expected skipped result, got {other:?}"),
    }
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), harness.rx.recv())
            .await
            .is_err()
    );
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(&*ctx.db, &ctx.task_id, default_roles::CODER)
            .await
            .expect("coder execution count loads"),
        0
    );
}

#[tokio::test]
async fn notify_role_holder_skips_without_coder_assignment() {
    let ctx = build_test_ctx(
        "task-notify-no-coder",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;

    let result = NotifyRoleHolder.execute(&ctx).await;

    assert!(matches!(result, HookResult::Skipped { .. }));
}

#[tokio::test]
async fn notify_role_holder_emits_event_for_coder_assignment() {
    let agent_id = "agent-coder-notify";
    let ctx = build_test_ctx(
        "task-notify-coder",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        Some((default_roles::CODER, agent_id)),
    )
    .await;
    let mut rx = ctx.event_bus.subscribe();

    let result = NotifyRoleHolder.execute(&ctx).await;

    assert!(matches!(result, HookResult::Ok));
    let event = rx.try_recv().expect("notification event emits");
    assert_eq!(event.event_type, "task.role_notified");
    assert_eq!(event.entity_id, ctx.task_id);
    match event.context {
        EventContext::TaskRoleNotified {
            task_id,
            role,
            notified_agent_id,
            notified_user_handle,
            state,
            reason,
        } => {
            assert_eq!(task_id, ctx.task_id);
            assert_eq!(role, default_roles::CODER);
            assert_eq!(notified_agent_id.as_deref(), Some(agent_id));
            assert_eq!(notified_user_handle, None);
            assert_eq!(state, default_states::REVIEW);
            assert!(reason.contains(default_states::REVIEW));
        }
        other => panic!("unexpected event context: {other:?}"),
    }
}

// --- Reviewer dispatch harness for review-state tests (7.3–7.6) ---

async fn build_reviewer_dispatch_harness(
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

    let mut harness = DispatchHarness {
        ctx: HookContext {
            task_id: task_id.to_owned(),
            project_id,
            from_state: default_states::IN_PROGRESS.to_owned(),
            to_state: default_states::REVIEW.to_owned(),
            db,
            event_bus: Arc::new(EventBus::new(16)),
            gate_config,
            workflow,
            project_version: Some(project.version),
            project_workflow_definition: Some(project.workflow_definition),
            triggered_by: api_types::Actor::system(api_types::SystemComponent::Test),
            review_runner: None,
            merge_service: None,
            cleanup_scheduler: None,
            task_executor: Some(Arc::new(PendingExecutor { sender: tx })),
            daemon_connections: None,
            workspace_exec_locks: None,
            terminal_activity: None,
            workspace_root: workspace_root.path().to_path_buf(),
            repo_cache_locks: Some(Arc::new(RepoCacheLockManager::default())),
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

async fn configure_review_defaults(
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

#[tokio::test]
async fn ci_passes_then_reviewer_dispatched_via_dispatch_role_agent() {
    let task_id = new_uuid_v4();
    let reviewer_id = "agent-reviewer-dispatch";
    let mut harness =
        build_reviewer_dispatch_harness(&task_id, reviewer_id, 2, vec!["test -d ."]).await;
    let ctx = harness.ctx.clone();

    let ci_result = RunCiSteps.execute(&ctx).await;
    assert!(
        matches!(ci_result, HookResult::Ok),
        "CI should pass: {ci_result:?}"
    );

    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(
        reviews[0].status,
        ReviewStatus::Running,
        "reviewer assigned so review stays Running"
    );

    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    assert!(
        matches!(dispatch_result, HookResult::Ok),
        "dispatch should succeed: {dispatch_result:?}"
    );

    let execution_ctx = tokio::time::timeout(std::time::Duration::from_secs(1), harness.rx.recv())
        .await
        .expect("executor spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, ctx.task_id);

    let executions = ExecutionRepo::list_by_task_and_role(
        &*ctx.db,
        &ctx.task_id,
        default_roles::REVIEWER,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("reviewer executions load");
    assert_eq!(executions.items.len(), 1);
    assert_eq!(executions.items[0].status, db::ExecutionStatus::Running);
    assert_eq!(executions.items[0].agent_id.as_deref(), Some(reviewer_id));
    assert_eq!(executions.items[0].role, default_roles::REVIEWER);
}

#[tokio::test]
async fn reviewer_follow_up_dispatch_uses_captured_review_admission() {
    let task_id = new_uuid_v4();
    let reviewer_id = "agent-reviewer-follow-up-admission";
    let mut harness = build_reviewer_dispatch_harness(&task_id, reviewer_id, 2, Vec::new()).await;
    let mut ctx = harness.ctx.clone();

    let candidate_id = ctx
        .execution_id
        .clone()
        .expect("implementation candidate seeded");
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let now = now_rfc3339();
    let review = ReviewRepo::create_with_task_authority(
        &*ctx.db,
        CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate_id.clone(),
            attempt_number: 0,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
        task.version,
        default_states::REVIEW,
        ctx.project_version,
        ctx.project_workflow_definition.as_deref(),
        Some(&candidate_id),
    )
    .await
    .expect("review creates under captured authority");
    let candidate = ExecutionRepo::get_by_id(&*ctx.db, &candidate_id)
        .await
        .expect("candidate loads")
        .expect("candidate exists");

    let prior_reviewer_id = new_uuid_v4();
    let now = now_rfc3339();
    ExecutionRepo::create(
        &*ctx.db,
        CreateExecution {
            id: prior_reviewer_id.clone(),
            task_id: task.id.clone(),
            agent_id: Some(reviewer_id.to_owned()),
            role: default_roles::REVIEWER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: Some(candidate_id.clone()),
            agent_session_id: Some("review-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("previous reviewer turn".to_owned()),
            logs_path: None,
            before_sha: candidate.before_sha.clone(),
            after_sha: candidate.after_sha.clone(),
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: candidate.workspace_id.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("prior reviewer execution creates");
    sqlx::query("UPDATE review SET reviewer_execution_id = ? WHERE id = ?")
        .bind(&prior_reviewer_id)
        .bind(&review.id)
        .execute(ctx.db.pool())
        .await
        .expect("review attempt binds prior reviewer");

    let mut workflow = default_workflow::default_workflow();
    workflow
        .states
        .iter_mut()
        .find(|state| state.name == default_states::REVIEW)
        .expect("review state exists")
        .dispatch
        .as_mut()
        .expect("review dispatch exists")
        .execution_policy = Some(api_types::WorkflowExecutionPolicy::ResumeLatestTargetRoleThread);
    ctx.workflow = Arc::new(workflow);

    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    assert!(
        matches!(dispatch_result, HookResult::Ok),
        "reviewer follow-up dispatch should succeed: {dispatch_result:?}"
    );
    let execution_ctx = tokio::time::timeout(std::time::Duration::from_secs(1), harness.rx.recv())
        .await
        .expect("follow-up executor spawned in time")
        .expect("follow-up execution context received");

    let executions = ExecutionRepo::list_by_task_and_role(
        &*ctx.db,
        &ctx.task_id,
        default_roles::REVIEWER,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("reviewer executions load");
    assert_eq!(executions.items.len(), 2);
    let follow_up = executions
        .items
        .iter()
        .find(|execution| execution.id == execution_ctx.execution_id)
        .expect("follow-up reviewer execution persists");
    assert_eq!(
        follow_up.parent_execution_id.as_deref(),
        Some(candidate_id.as_str())
    );
    let review_after = ReviewRepo::get_by_id(&*ctx.db, &review.id)
        .await
        .expect("review reloads")
        .expect("review exists");
    assert_eq!(
        review_after.reviewer_execution_id.as_deref(),
        Some(follow_up.id.as_str())
    );
}

#[tokio::test]
async fn subtask_root_still_dispatches_reviewer_after_coder_completion() {
    let task_id = new_uuid_v4();
    let reviewer_id = "agent-reviewer-subtask-root";
    let mut harness =
        build_reviewer_dispatch_harness(&task_id, reviewer_id, 2, vec!["test -d ."]).await;
    let mut ctx = harness.ctx.clone();
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let now = now_rfc3339();
    let subtask_id = new_uuid_v4();
    TaskRepo::create(
        &*ctx.db,
        CreateTask {
            id: subtask_id.clone(),
            project_id: task.project_id.clone(),
            parent_task_id: Some(task.id.clone()),
            subtask_order: Some(0),
            assignee_type: None,
            assignee_id: None,
            title: "ordered child".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: default_states::DONE.to_owned(),
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
    .expect("subtask creates");
    let root_execution = ExecutionRepo::get_by_id(
        &*ctx.db,
        ctx.execution_id
            .as_deref()
            .expect("root execution is seeded"),
    )
    .await
    .expect("root execution loads")
    .expect("root execution exists");
    let child_execution_id = new_uuid_v4();
    ExecutionRepo::create(
        &*ctx.db,
        CreateExecution {
            id: child_execution_id.clone(),
            task_id: subtask_id,
            agent_id: None,
            role: "executor".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: Some(root_execution.id),
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("subtask executor summary".to_owned()),
            logs_path: None,
            before_sha: root_execution.before_sha.clone(),
            after_sha: root_execution.after_sha,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: root_execution.workspace_id,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("subtask execution creates");
    ctx.execution_id = Some(child_execution_id);
    let ci_result = RunCiSteps.execute(&ctx).await;
    assert!(matches!(ci_result, HookResult::Ok), "{ci_result:?}");

    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    assert!(
        matches!(dispatch_result, HookResult::Ok),
        "reviewer dispatch should not be skipped for subtask roots: {dispatch_result:?}"
    );

    let execution_ctx = tokio::time::timeout(std::time::Duration::from_secs(1), harness.rx.recv())
        .await
        .expect("executor spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, ctx.task_id);
}

#[tokio::test]
async fn reviewer_dispatch_ignores_waiting_review_tasks_without_running_execution() {
    let task_id = new_uuid_v4();
    let reviewer_id = "agent-reviewer-waiting-review";
    let mut harness =
        build_reviewer_dispatch_harness(&task_id, reviewer_id, 1, vec!["test -d ."]).await;
    let ctx = harness.ctx.clone();
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let other_task_id = "task-waiting-review-no-execution";
    TaskRepo::create(
        &*ctx.db,
        CreateTask {
            id: other_task_id.to_owned(),
            project_id: task.project_id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "other waiting review task".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: default_states::REVIEW.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("other task creates");
    assign_agent_role(&ctx.db, other_task_id, default_roles::REVIEWER, reviewer_id).await;

    let ci_result = RunCiSteps.execute(&ctx).await;
    assert!(matches!(ci_result, HookResult::Ok), "{ci_result:?}");

    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    assert!(
        matches!(dispatch_result, HookResult::Ok),
        "dispatch should not be blocked by waiting review tasks: {dispatch_result:?}"
    );

    let execution_ctx = tokio::time::timeout(std::time::Duration::from_secs(1), harness.rx.recv())
        .await
        .expect("executor spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, ctx.task_id);
}

#[tokio::test]
async fn ci_fails_reviewer_not_dispatched_cascade_handles_bounce() {
    let task_id = new_uuid_v4();
    let reviewer_id = "agent-reviewer-ci-fail";
    let harness = build_reviewer_dispatch_harness(&task_id, reviewer_id, 2, vec!["exit 1"]).await;
    let ctx = harness.ctx.clone();

    let ci_result = RunCiSteps.execute(&ctx).await;
    assert!(
        matches!(ci_result, HookResult::Failed { .. }),
        "CI hook returns Failed on CI failure: {ci_result:?}"
    );

    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(
        reviews[0].status,
        ReviewStatus::Failed,
        "failing CI sets review to Failed"
    );

    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    match dispatch_result {
        HookResult::Skipped { reason } => assert_eq!(reason, "review already failed"),
        other => panic!("expected skipped, got {other:?}"),
    }

    let cascade_result = AutoCascadeOnReviewPass.execute(&ctx).await;
    match cascade_result {
        HookResult::Cascade { to, reason } => {
            assert_eq!(to, default_states::IN_PROGRESS);
            assert_eq!(reason, "review failed");
        }
        other => panic!("expected cascade to in_progress, got {other:?}"),
    }

    assert_eq!(
        ExecutionRepo::count_by_task_and_role(&*ctx.db, &ctx.task_id, default_roles::REVIEWER)
            .await
            .expect("reviewer execution count"),
        0,
        "no reviewer execution should be created when CI fails"
    );
}

#[tokio::test]
async fn reviewer_dispatch_creates_fresh_attempt_after_prior_failure() {
    let task_id = new_uuid_v4();
    let reviewer_id = "agent-reviewer-fresh-attempt";
    let mut harness = build_reviewer_dispatch_harness(&task_id, reviewer_id, 2, Vec::new()).await;
    let mut ctx = harness.ctx.clone();

    let failed_candidate_id = ctx
        .execution_id
        .clone()
        .expect("prior review candidate is present");
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let project = ProjectRepo::get_by_id(&*ctx.db, &task.project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    let now = now_rfc3339();
    ReviewRepo::create_with_task_authority(
        &*ctx.db,
        CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: failed_candidate_id.clone(),
            attempt_number: 0,
            status: ReviewStatus::Failed,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
        task.version,
        default_states::REVIEW,
        Some(project.version),
        Some(project.workflow_definition.as_str()),
        Some(failed_candidate_id.as_str()),
    )
    .await
    .expect("prior failed review creates");

    // A remediation run supplies a new implementation candidate. The old
    // Failed Review remains immutable; reviewer dispatch must create a new
    // Running attempt bound to this candidate before launching the reviewer.
    let workspace = WorkspaceRepo::get_by_task_id(&*ctx.db, &ctx.task_id)
        .await
        .expect("workspace loads")
        .expect("workspace exists");
    let replacement_candidate_id = new_uuid_v4();
    let now = now_rfc3339();
    ExecutionRepo::create(
        &*ctx.db,
        CreateExecution {
            id: replacement_candidate_id.clone(),
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
            summary: Some("replacement executor summary".to_owned()),
            logs_path: None,
            before_sha: workspace.before_sha.clone(),
            after_sha: workspace.before_sha.clone(),
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace.id),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("replacement execution creates");
    ctx.execution_id = Some(replacement_candidate_id.clone());
    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    assert!(
        matches!(dispatch_result, HookResult::Ok),
        "fresh reviewer attempt should dispatch: {dispatch_result:?}"
    );
    let _execution_ctx = tokio::time::timeout(std::time::Duration::from_secs(1), harness.rx.recv())
        .await
        .expect("replacement reviewer spawned in time")
        .expect("replacement reviewer context received");

    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 2);
    let failed = reviews
        .iter()
        .find(|review| review.execution_id == failed_candidate_id)
        .expect("failed review remains present");
    assert_eq!(failed.status, ReviewStatus::Failed);
    let running = reviews
        .iter()
        .find(|review| review.execution_id == replacement_candidate_id)
        .expect("fresh review binds replacement candidate");
    assert_eq!(running.status, ReviewStatus::Running);
    assert!(
        running.attempt_number > failed.attempt_number,
        "fresh reviewer attempt must advance the terminal review attempt"
    );
}

#[tokio::test]
async fn reviewer_at_capacity_ci_runs_dispatch_queues() {
    let task_id = new_uuid_v4();
    let reviewer_id = "agent-reviewer-capacity";
    let harness =
        build_reviewer_dispatch_harness(&task_id, reviewer_id, 1, vec!["test -d ."]).await;
    let ctx = harness.ctx.clone();

    let other_task_id = new_uuid_v4();
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    TaskRepo::create(
        &*ctx.db,
        CreateTask {
            id: other_task_id.clone(),
            project_id: task.project_id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "other review task".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: default_states::REVIEW.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("other task creates");
    assign_agent_role(
        &ctx.db,
        &other_task_id,
        default_roles::REVIEWER,
        reviewer_id,
    )
    .await;
    seed_running_execution_for_task(
        &ctx.db,
        &other_task_id,
        reviewer_id,
        default_roles::REVIEWER,
    )
    .await;
    crate::test_support::set_test_agent_capacity(&ctx.db, reviewer_id, 1).await;

    let ci_result = RunCiSteps.execute(&ctx).await;
    assert!(
        matches!(ci_result, HookResult::Ok),
        "CI should pass even when reviewer at capacity: {ci_result:?}"
    );

    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::Running);

    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    match dispatch_result {
        HookResult::Skipped { reason } => assert_eq!(reason, "agent at capacity"),
        other => panic!("expected skipped at capacity, got {other:?}"),
    }

    assert_eq!(
        ExecutionRepo::count_by_task_and_role(&*ctx.db, &ctx.task_id, default_roles::REVIEWER)
            .await
            .expect("reviewer execution count"),
        0,
        "no reviewer execution when agent at capacity"
    );
}

#[tokio::test]
async fn no_reviewer_assigned_auto_cascade_to_merging() {
    let task_id = new_uuid_v4();
    let mut ctx = build_test_ctx(
        &task_id,
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    ctx.state_config = json!({});

    let ci_result = RunCiSteps.execute(&ctx).await;
    match ci_result {
        HookResult::Skipped { reason } => assert!(reason.contains("ci steps")),
        other => panic!("expected skipped for empty ci, got {other:?}"),
    }

    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    match dispatch_result {
        HookResult::Skipped { reason } => assert_eq!(reason, "no reviewer role assigned"),
        other => panic!("expected skipped for no reviewer, got {other:?}"),
    }

    let cascade_result = super::AutoCascadeOnUnconfiguredReview.execute(&ctx).await;
    match cascade_result {
        HookResult::Cascade { to, reason } => {
            assert_eq!(to, default_states::MERGING);
            assert!(reason.contains("no checks or reviewer"));
        }
        other => panic!("expected cascade to merging, got {other:?}"),
    }
}

#[tokio::test]
async fn read_only_task_without_reviewer_ignores_implementation_checks_and_cascades() {
    let task_id = new_uuid_v4();
    let mut ctx = build_test_ctx(
        &task_id,
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    sqlx::query("UPDATE task SET task_type = 'planning_task' WHERE id = ?")
        .bind(&ctx.task_id)
        .execute(ctx.db.pool())
        .await
        .expect("Task kind updates");
    ctx.state_config = json!({"ci_steps":["false"]});

    let cascade = super::AutoCascadeOnUnconfiguredReview.execute(&ctx).await;

    match cascade {
        HookResult::Cascade { to, reason } => {
            assert_eq!(to, default_states::MERGING);
            assert!(reason.contains("no checks or reviewer"));
        }
        other => panic!("expected cascade to merging, got {other:?}"),
    }
}
