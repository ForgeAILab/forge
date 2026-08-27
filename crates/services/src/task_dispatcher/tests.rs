use std::{future::pending, path::Path, sync::Arc};

use async_trait::async_trait;
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentRepo, AgentStatus,
    CreateAgent, CreateProject, CreateRepo, CreateTask, CreateTaskRoleAssignment, DaemonRepo,
    DaemonStatus, ExecutionRepo, ExecutionStatus, PageRequest, RepoRepo, ResumePolicy, ReviewRepo,
    ReviewStatus, SortBy, SortOrder, StopReason, TaskRepo, TaskRoleAssignmentRepo,
    TransitionLogRepo, UpdateDaemonReport, UpdateProject, UpdateTask, UpsertDaemon,
};
use executors::{ExecutionContext, ExecutionResult, ExecutorError, TaskExecutor};
use tempfile::TempDir;
use tokio::sync::mpsc;
use workspace::RepoCacheLockManager;

use crate::deferred_dispatch;

use super::*;

struct RecordingExecutor {
    sender: mpsc::UnboundedSender<ExecutionContext>,
}

#[async_trait]
impl TaskExecutor for RecordingExecutor {
    async fn execute(
        &self,
        ctx: ExecutionContext,
    ) -> std::result::Result<ExecutionResult, ExecutorError> {
        let _ = self.sender.send(ctx);
        pending::<()>().await;
        unreachable!()
    }

    async fn cancel(&self, _execution_id: &str) -> std::result::Result<(), ExecutorError> {
        Ok(())
    }
}

async fn sqlite_db() -> db::SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    db::SqliteDb::new(pool)
}

fn setup_git_repo(path: &Path) -> String {
    run_git(path, &["init"]);
    run_git(path, &["config", "user.email", "test@forge.dev"]);
    run_git(path, &["config", "user.name", "Forge Test"]);
    std::fs::write(path.join("README.md"), "# Forge\n").expect("README writes");
    run_git(path, &["add", "-A"]);
    run_git(path, &["commit", "-m", "initial commit"]);
    run_git(path, &["symbolic-ref", "--short", "HEAD"])
}

fn run_git(path: &Path, args: &[&str]) -> String {
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

async fn seed_project_repo(db: &db::SqliteDb, repo_path: &Path) -> (String, String) {
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
            updated_at: now,
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

    (project_id, repo_id)
}

async fn seed_agent(
    db: &db::SqliteDb,
    max_concurrent_tasks: i64,
    daemon_status: DaemonStatus,
    agent_status: AgentStatus,
) -> String {
    let now = now_rfc3339();
    let daemon_id = new_uuid_v4();
    DaemonRepo::upsert_by_machine_id(
        db,
        UpsertDaemon {
            id: daemon_id.clone(),
            machine_id: format!("machine-{daemon_id}"),
            hostname: "host".to_owned(),
            os: "linux".to_owned(),
            arch: "x86_64".to_owned(),
            agent_version: None,
            labels_json: "{}".to_owned(),
            status: daemon_status.clone(),
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
            status: daemon_status,
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
            name: "shell".to_owned(),
            description: None,
            executor_type: "shell".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: Some(daemon_id),
            max_concurrent_tasks: max_concurrent_tasks + 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: agent_status,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            prompt_template: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("agent creates");
    agent_id
}

async fn seed_task(
    db: &db::SqliteDb,
    project_id: &str,
    repo_id: &str,
    title: &str,
    status: &str,
    priority: i64,
) -> Task {
    let now = now_rfc3339();
    TaskRepo::create(
        db,
        CreateTask {
            id: new_uuid_v4(),
            project_id: project_id.to_owned(),
            repo_id: Some(repo_id.to_owned()),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: title.to_owned(),
            description: Some("echo test".to_owned()),
            task_type: "task".to_owned(),
            status: status.to_owned(),
            is_automation: false,
            priority,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("task creates")
}

async fn assign_role(db: &db::SqliteDb, task_id: &str, role_name: &str, agent_id: &str) {
    let now = now_rfc3339();
    TaskRoleAssignmentRepo::assign(
        db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            role_name: role_name.to_owned(),
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

async fn set_review_ci_config(db: &db::SqliteDb, task: &Task) -> Task {
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
            task_state_config: Some(Some(r#"{"review":{"ci_steps":["test -d ."]}}"#.to_owned())),
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("task review config updates")
}

async fn set_planning_gate_auto_approval(db: &db::SqliteDb, project_id: &str) {
    let mut workflow = crate::workflow::default_workflow::default_workflow();
    let planning = workflow
        .states
        .iter_mut()
        .find(|state| state.name == crate::workflow::default_states::PLANNING)
        .expect("default workflow has planning state");
    planning
        .gate_config
        .as_mut()
        .expect("planning has gate config")
        .requires_user_approval = Some(false);
    let workflow_definition =
        serde_json::to_string(&workflow).expect("workflow serializes for test");
    sqlx::query(
            "UPDATE project SET workflow_definition = ?, workflow_template_name = ?, updated_at = ? WHERE id = ?",
        )
        .bind(workflow_definition)
        .bind("no-user-approval")
        .bind(now_rfc3339())
        .bind(project_id)
        .execute(db.pool())
        .await
        .expect("project workflow updates");
}

async fn seed_running_review(
    db: &db::SqliteDb,
    task_id: &str,
    execution_id: &str,
    step_results_json: &str,
) {
    let now = now_rfc3339();
    ReviewRepo::create(
        db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            execution_id: execution_id.to_owned(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: step_results_json.to_owned(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("review creates");
}

async fn seed_completed_coder_execution(db: &db::SqliteDb, task_id: &str) -> String {
    let now = now_rfc3339();
    let execution_id = new_uuid_v4();
    ExecutionRepo::create(
        db,
        db::CreateExecution {
            id: execution_id.clone(),
            task_id: task_id.to_owned(),
            agent_id: None,
            role: crate::workflow::default_roles::CODER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("completed".to_owned()),
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
    .expect("execution creates");
    execution_id
}

async fn seed_running_execution(db: &db::SqliteDb, task_id: &str, agent_id: &str, role: &str) {
    let now = now_rfc3339();
    ExecutionRepo::create(
        db,
        db::CreateExecution {
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
    .expect("execution creates");
}

async fn seed_cancelled_execution(
    db: &db::SqliteDb,
    task_id: &str,
    agent_id: &str,
    role: &str,
    stop_reason: Option<StopReason>,
    resume_policy: Option<ResumePolicy>,
) -> db::Execution {
    let now = now_rfc3339();
    ExecutionRepo::create(
        db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            agent_id: Some(agent_id.to_owned()),
            role: role.to_owned(),
            status: ExecutionStatus::Cancelled,
            stop_reason,
            stopped_by: Some("system:test".to_owned()),
            resume_policy,
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some("intentional stop".to_owned()),
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("execution creates")
}

async fn build_dispatcher(
    db: Arc<db::SqliteDb>,
    workspace_root: &Path,
) -> (TaskDispatcher, mpsc::UnboundedReceiver<ExecutionContext>) {
    let event_bus = Arc::new(EventBus::new(64));
    let (tx, rx) = mpsc::unbounded_channel();
    let task_executor: Arc<dyn TaskExecutor> = Arc::new(RecordingExecutor { sender: tx });
    let task_service = Arc::new(
        TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
            .with_task_executor(task_executor)
            .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
            .with_workspace_root(workspace_root.to_path_buf()),
    );
    (
        TaskDispatcher::with_check_interval(
            Arc::clone(&db),
            Arc::clone(&event_bus),
            task_service,
            Duration::from_millis(10),
        ),
        rx,
    )
}

#[tokio::test]
async fn dispatcher_check_once_does_not_dispatch_after_stop() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, &repo_id, "high", "todo", 1).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    dispatcher.stop();

    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 0);
    let updated = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(updated.status, "todo");
    assert!(tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn dispatcher_skips_unassigned_planning_gate_before_coder_dispatch() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, &repo_id, "high", "todo", 1).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    let updated = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(updated.status, "in_progress");
    let execution_ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("execution spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, task.id);
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER
        )
        .await
        .expect("execution count loads"),
        1
    );
}

#[tokio::test]
async fn dispatcher_waits_for_deferred_dispatch_cooldown() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(
        &db,
        &project_id,
        &repo_id,
        "deferred",
        crate::workflow::default_states::IN_PROGRESS,
        1,
    )
    .await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    deferred_dispatch::set(
        &db,
        &task,
        crate::workflow::default_states::IN_PROGRESS,
        &(chrono::Utc::now() + chrono::Duration::seconds(30)).to_rfc3339(),
        "test cooldown",
    )
    .await
    .expect("deferred dispatch metadata writes");

    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 0);
    assert!(rx.try_recv().is_err());
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER
        )
        .await
        .expect("execution count loads"),
        0
    );

    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    deferred_dispatch::set(
        &db,
        &task,
        crate::workflow::default_states::IN_PROGRESS,
        &(chrono::Utc::now() - chrono::Duration::seconds(1)).to_rfc3339(),
        "test cooldown expired",
    )
    .await
    .expect("deferred dispatch metadata updates");
    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    let execution_ctx = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .expect("executor spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, task.id);
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert!(deferred_dispatch::pending_until(&task).is_none());
}

#[tokio::test]
async fn dispatcher_enters_unassigned_auto_planning_gate_before_coder_dispatch() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    set_planning_gate_auto_approval(&db, &project_id).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, &repo_id, "auto plan", "todo", 1).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    let updated = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(updated.status, "in_progress");
    let execution_ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("execution spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, task.id);
    let executions = ExecutionRepo::list_by_task_and_role(
        &*db,
        &task.id,
        crate::workflow::default_roles::CODER,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Asc,
        },
    )
    .await
    .expect("executions load");
    assert_eq!(executions.items.len(), 1);
    assert_eq!(
        executions.items[0].agent_id.as_deref(),
        Some(agent_id.as_str())
    );
    let transitions = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .expect("transition logs load");
    assert!(transitions
        .iter()
        .any(|entry| entry.from_state == "todo" && entry.to_state == "planning"));
    assert!(transitions
        .iter()
        .any(|entry| entry.from_state == "planning" && entry.to_state == "in_progress"));
}

#[tokio::test]
async fn dispatcher_recovers_task_stuck_in_unassigned_optional_planning_gate() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(
        &db,
        &project_id,
        &repo_id,
        "stuck planning",
        crate::workflow::default_states::PLANNING,
        1,
    )
    .await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    let updated = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(updated.status, crate::workflow::default_states::IN_PROGRESS);
    let execution_ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("execution spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, task.id);
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER
        )
        .await
        .expect("execution count loads"),
        1
    );
}

#[tokio::test]
async fn dispatcher_skips_task_when_agent_at_capacity() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let blocked = seed_task(&db, &project_id, &repo_id, "blocked", "in_progress", 0).await;
    assign_role(
        &db,
        &blocked.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    seed_running_execution(
        &db,
        &blocked.id,
        &agent_id,
        crate::workflow::default_roles::CODER,
    )
    .await;
    crate::test_support::set_test_agent_capacity(&db, &agent_id, 1).await;
    let task = seed_task(&db, &project_id, &repo_id, "todo", "todo", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 0);
    let updated = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(updated.status, "todo");
    assert!(tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn dispatcher_skips_task_when_agent_offline() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Offline, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, &repo_id, "todo", "todo", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 0);
    let updated = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(updated.status, "todo");
    assert!(tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn dispatcher_skips_paused_project() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, &repo_id, "todo", "todo", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    sqlx::query("UPDATE project SET paused_at = ? WHERE id = ?")
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project paused");
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 0);
    let updated = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(updated.status, "todo");
    assert!(tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn dispatcher_recovers_undispatched_active_task() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, &repo_id, "active", "in_progress", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("execution spawned in time")
        .expect("execution context received");
    assert_eq!(ctx.task_id, task.id);
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER
        )
        .await
        .expect("execution count loads"),
        1
    );
}

#[tokio::test]
async fn dispatcher_recovers_undispatched_reviewer_task() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, &repo_id, "review", "review", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::REVIEWER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("reviewer execution spawned in time")
        .expect("reviewer execution context received");
    assert_eq!(ctx.task_id, task.id);
    assert!(ctx.description.contains("===REVIEW: PASS==="));
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::REVIEWER
        )
        .await
        .expect("reviewer execution count loads"),
        1
    );
}

#[tokio::test]
async fn reviewer_assignment_after_stopped_attempt_dispatches_without_separate_resume() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, &repo_id, "review retry", "review", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::REVIEWER,
        &agent_id,
    )
    .await;
    seed_cancelled_execution(
        &db,
        &task.id,
        &agent_id,
        crate::workflow::default_roles::REVIEWER,
        None,
        None,
    )
    .await;

    TaskRoleAssignmentRepo::assign(
        &*db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            role_name: crate::workflow::default_roles::REVIEWER.to_owned(),
            assignee_type: Some(db::AssigneeKind::Agent),
            assignee_id: Some(agent_id.clone()),
            created_at: "2099-01-01T00:00:00Z".to_owned(),
            updated_at: "2099-01-01T00:00:00Z".to_owned(),
        },
    )
    .await
    .expect("reviewer assignment confirms after stopped attempt");

    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("reviewer execution spawned in time")
        .expect("reviewer execution context received");
    assert_eq!(ctx.task_id, task.id);
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::REVIEWER
        )
        .await
        .expect("reviewer execution count loads"),
        2
    );
}

#[tokio::test]
async fn dispatcher_respects_priority_ordering() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let low = seed_task(&db, &project_id, &repo_id, "low", "todo", 1).await;
    assign_role(
        &db,
        &low.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let high = seed_task(
        &db,
        &project_id,
        &repo_id,
        "high",
        "todo",
        "10".parse().unwrap(),
    )
    .await;
    assign_role(
        &db,
        &high.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    crate::test_support::force_task_version_conflict_after_transition(
        &db,
        &high.id,
        crate::workflow::default_states::IN_PROGRESS,
        &low.id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("execution spawned in time")
        .expect("execution context received");
    assert_eq!(ctx.task_id, high.id);

    let high_task = TaskRepo::get_by_id(&*db, &high.id, false)
        .await
        .expect("high task loads")
        .expect("high task exists");
    let low_task = TaskRepo::get_by_id(&*db, &low.id, false)
        .await
        .expect("low task loads")
        .expect("low task exists");
    assert_eq!(high_task.status, "in_progress");
    assert_eq!(low_task.status, "todo");
}

#[tokio::test]
async fn dispatcher_skips_auto_restart_for_user_cancelled_execution() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, &repo_id, "cancelled", "in_progress", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let manual_stop = serde_json::json!({
        "type": "manual_stop",
        "blocking_reason": "user_cancelled",
        "blocked_by": "user:test",
        "blocked_at": now_rfc3339(),
        "message": "user stop",
        "recovery_actions": ["reexecute", "reset_to_initial", "cancel_task"],
    })
    .to_string();
    let before = now_rfc3339();
    TaskRepo::update(
        &*db,
        UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(manual_stop)),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: before.clone(),
        },
    )
    .await
    .expect("task update creates");
    seed_cancelled_execution(
        &db,
        &task.id,
        &agent_id,
        crate::workflow::default_roles::CODER,
        Some(StopReason::UserCancelled),
        Some(ResumePolicy::Manual),
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 0);
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER
        )
        .await
        .expect("execution count loads"),
        1
    );
    assert!(tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn dispatcher_skips_auto_restart_for_task_cancelled_execution() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, &repo_id, "cancelled", "in_progress", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    seed_cancelled_execution(
        &db,
        &task.id,
        &agent_id,
        crate::workflow::default_roles::CODER,
        Some(StopReason::TaskCancelled),
        None,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 0);
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER
        )
        .await
        .expect("execution count loads"),
        1
    );
    assert!(tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn dispatcher_dispatches_when_graceful_shutdown_stop_is_auto() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, &repo_id, "cancelled", "in_progress", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    seed_cancelled_execution(
        &db,
        &task.id,
        &agent_id,
        crate::workflow::default_roles::CODER,
        Some(StopReason::GracefulShutdown),
        Some(ResumePolicy::Auto),
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER
        )
        .await
        .expect("execution count loads"),
        2
    );
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("execution spawned in time")
        .expect("execution context received");
    assert_eq!(ctx.task_id, task.id);
}

#[tokio::test]
async fn dispatcher_does_not_dispatch_when_graceful_shutdown_stop_is_manual() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, &repo_id, "cancelled", "in_progress", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    seed_cancelled_execution(
        &db,
        &task.id,
        &agent_id,
        crate::workflow::default_roles::CODER,
        Some(StopReason::GracefulShutdown),
        Some(ResumePolicy::Manual),
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 0);
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER
        )
        .await
        .expect("execution count loads"),
        1
    );
    assert!(tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn dispatcher_skips_legacy_stopped_execution_without_resume_policy() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, &repo_id, "legacy", "in_progress", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    seed_cancelled_execution(
        &db,
        &task.id,
        &agent_id,
        crate::workflow::default_roles::CODER,
        None,
        None,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 0);
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER
        )
        .await
        .expect("execution count loads"),
        1
    );
    assert!(tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn dispatcher_skips_active_task_with_blocking_annotation() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, &repo_id, "blocked", "in_progress", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let blocked = serde_json::json!({
        "type": "manual_stop",
        "blocking_reason": "user_cancelled",
        "blocked_by": "user:test",
        "blocked_at": now_rfc3339(),
        "message": "blocked for review",
        "recovery_actions": ["resume_session", "reexecute", "reset_to_initial", "cancel_task"],
    })
    .to_string();
    TaskRepo::update(
        &*db,
        UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(blocked)),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("task update creates");
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 0);
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER
        )
        .await
        .expect("execution count loads"),
        0
    );
    assert!(tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn dispatcher_skips_todo_task_with_dispatch_failed_annotation() {
    // A task parked in todo after a deterministic dispatch failure (e.g.
    // governance says it is not runnable) must not be rescheduled until a
    // user restarts it — otherwise the dispatcher loops todo -> ... -> todo.
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, &repo_id, "parked", "todo", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let annotation = serde_json::json!({
        "type": "dispatch_failed",
        "message": "repository Task is not runnable: an active user-approved execution baseline with matching traceability is required",
        "state": "in_progress",
        "detected_at": now_rfc3339(),
        "recovery_actions": ["reexecute", "reset_to_initial", "cancel_task"],
    })
    .to_string();
    TaskRepo::update(
        &*db,
        UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(annotation)),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("task annotation updates");
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 0);
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, "todo", "parked task stays in todo");
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER
        )
        .await
        .expect("execution count loads"),
        0
    );
    assert!(tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn dispatcher_skips_reviewer_until_configured_ci_has_finished() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, &repo_id, "review", "review", 0).await;
    let task = set_review_ci_config(&db, &task).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::REVIEWER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 0);
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::REVIEWER
        )
        .await
        .expect("execution count loads"),
        0
    );
    assert!(tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .is_err());

    let coder_execution_id = seed_completed_coder_execution(&db, &task.id).await;
    seed_running_review(
        &db,
        &task.id,
        &coder_execution_id,
        r#"{"ci_steps":[{"index":0,"command":"test -d .","exit_code":0}]}"#,
    )
    .await;

    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("execution spawned in time")
        .expect("execution context received");
    assert_eq!(ctx.task_id, task.id);
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::REVIEWER
        )
        .await
        .expect("execution count loads"),
        1
    );
}

/// Attach an approved Charter so the Project becomes `charter_backed` and
/// `ensure_task_runnable` applies the governance gate instead of the legacy
/// pre-Charter passthrough. With no execution baseline, an implementation
/// Task is then deterministically refused for as long as nothing changes.
async fn make_project_charter_backed(db: &db::SqliteDb, project_id: &str) {
    let now = now_rfc3339();
    let owner_id = new_uuid_v4();
    let charter_id = format!("{project_id}-charter");
    let revision_id = format!("{charter_id}-revision-1");
    sqlx::query(
        "INSERT OR IGNORE INTO user (id, email, password_hash, display_name, created_at, updated_at)
         VALUES (?, ?, 'test', NULL, ?, ?)",
    )
    .bind(&owner_id)
    .bind(format!("{owner_id}@example.test"))
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("owning account exists");
    // The Charter's account must own the Project (V076 owner guard).
    sqlx::query("UPDATE project SET owner_id = ? WHERE id = ?")
        .bind(&owner_id)
        .bind(project_id)
        .execute(db.pool())
        .await
        .expect("project ownership attaches");
    sqlx::query(
        "INSERT INTO project_charter (
             id, account_id, project_id, project_mode, maturity, lifecycle,
             version, created_at, updated_at
         ) VALUES (?, ?, ?, 'compact', 'prototype', 'attached', 1, ?, ?)",
    )
    .bind(&charter_id)
    .bind(&owner_id)
    .bind(project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("charter creates");
    sqlx::query(
        "INSERT INTO project_charter_revision (
             id, charter_id, revision, base_revision, lifecycle, schema_version,
             render_version, content_json, rendered_view, change_summary,
             author_type, author_id, source_refs_json, content_digest,
             rendered_digest, created_at
         ) VALUES (?, ?, 1, 0, 'approved', 'forge.project-charter/v1',
                   'forge.project-charter-render/v1', '{}', '# Project',
                   'dispatch quiescence fixture', 'user', ?, '[]',
                   'dispatch-charter-content-digest', 'dispatch-charter-render-digest', ?)",
    )
    .bind(&revision_id)
    .bind(&charter_id)
    .bind(&owner_id)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("charter revision creates");
    sqlx::query(
        "UPDATE project_charter
         SET current_approved_revision_id = ?, current_draft_revision_id = ?, version = 2
         WHERE id = ?",
    )
    .bind(&revision_id)
    .bind(&revision_id)
    .bind(&charter_id)
    .execute(db.pool())
    .await
    .expect("charter approval attaches");
    sqlx::query(
        "UPDATE project
         SET current_charter_id = ?, current_charter_revision_id = ?,
             current_charter_version = 1, charter_status = 'charter_backed',
             charter_setup_required = 0, version = version + 1, updated_at = ?
         WHERE id = ?",
    )
    .bind(&charter_id)
    .bind(&revision_id)
    .bind(&now)
    .bind(project_id)
    .execute(db.pool())
    .await
    .expect("approved Charter attaches to Project");
}

#[tokio::test]
async fn dispatcher_stays_quiescent_for_unchanged_deterministic_governance_denial() {
    // F11: a governance denial is deterministic — re-running the identical
    // admission against the identical Task version cannot succeed. The old
    // dispatcher re-attempted and re-logged it on every ten-second scan
    // forever. One disposition must be recorded on the first observation and
    // every later scan must skip the Task without touching it again.
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    make_project_charter_backed(&db, &project_id).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, &repo_id, "blocked", "todo", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    for scan in 0..3 {
        let dispatched = dispatcher.check_once().await.expect("dispatcher runs");
        assert_eq!(
            dispatched, 0,
            "blocked task must not dispatch on scan {scan}"
        );
    }

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, "todo");
    assert_eq!(
        current.version, task.version,
        "a quiesced blocker must not churn the Task version"
    );
    let disposition = deferred_dispatch::dispatch_disposition_for_test(&current)
        .expect("the first observation records exactly one disposition");
    assert_eq!(disposition.task_version, task.version);
    assert_eq!(
        disposition.capability,
        crate::workflow::default_roles::CODER
    );
    assert!(disposition.blocker_digest.starts_with("sha256:"));
    assert!(
        !disposition.safe_message.trim().is_empty(),
        "the disposition must carry the observed refusal for the blocker projection"
    );
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER
        )
        .await
        .expect("execution count loads"),
        0
    );
    assert!(tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn wake_task_dispatch_unblocks_exactly_once_and_survives_restart() {
    // F11's other half: resolving the blocker must wake exactly the affected
    // Task exactly once. Governance state lives outside the `task` row, so
    // nothing bumps `version` — without an explicit wake the disposition
    // would keep the Task quiesced forever. A restart (a fresh dispatcher
    // over the same database) must neither duplicate the dispatch nor lose
    // the wake.
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    make_project_charter_backed(&db, &project_id).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, &repo_id, "blocked", "todo", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    assert_eq!(dispatcher.check_once().await.expect("dispatcher runs"), 0);
    let blocked = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(deferred_dispatch::dispatch_disposition_for_test(&blocked).is_some());

    // Resolve the governing blocker the way a reconciliation resolution or a
    // baseline activation does: change authority on another table, then wake
    // exactly this Task.
    sqlx::query(
        "UPDATE project
         SET current_charter_id = NULL, current_charter_revision_id = NULL,
             charter_status = 'legacy_unverified', charter_setup_required = 1
         WHERE id = ?",
    )
    .bind(&project_id)
    .execute(db.pool())
    .await
    .expect("governing state changes");
    deferred_dispatch::wake_task_dispatch(&db, &task.id, "test: blocker resolved")
        .await
        .expect("wake succeeds");

    let woken = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(
        deferred_dispatch::dispatch_disposition_for_test(&woken).is_none(),
        "the wake must clear the disposition"
    );

    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");
    assert_eq!(dispatched, 1, "the woken Task dispatches exactly once");
    assert!(tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("executor receives the woken dispatch")
        .is_some());

    // Restart: a fresh dispatcher over the same database must not re-dispatch
    // the Task the previous instance already moved on.
    let (restarted, mut restarted_rx) =
        build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    assert_eq!(
        restarted.check_once().await.expect("dispatcher runs"),
        0,
        "restart must not duplicate the dispatch"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), restarted_rx.recv())
            .await
            .is_err()
    );
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER
        )
        .await
        .expect("execution count loads"),
        1,
        "exactly one execution exists after wake + restart"
    );
}
