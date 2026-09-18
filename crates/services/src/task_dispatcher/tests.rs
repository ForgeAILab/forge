use std::{future::pending, path::Path, sync::Arc};

use async_trait::async_trait;
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentRepo, AgentStatus,
    CreateAgent, CreateProject, CreateRepo, CreateTask, CreateTaskRoleAssignment, DaemonRepo,
    DaemonStatus, ExecutionRepo, ExecutionStatus, PageRequest, RepoRepo, ResumePolicy, ReviewRepo,
    ReviewStatus, SortBy, SortOrder, StopReason, TaskRepo, TaskRoleAssignmentRepo,
    TransitionLogRepo, UpdateDaemonReport, UpdateProject, UpdateTask, UpsertDaemon,
    WorkspaceLeaseRepo, WorkspaceRepo,
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
    run_git(path, &["init", "--initial-branch=main"]);
    // Pin the initial branch the way `git::init` does. Repository readiness
    // requires the registered default branch to be `main` and to exist on
    // disk, so inheriting the host's `init.defaultBranch` makes an
    // implementation Task park on "execution setup required" wherever git
    // still creates `master` — which is every CI runner, and no machine with
    // an ambient `init.defaultBranch=main`.
    run_git(path, &["symbolic-ref", "HEAD", "refs/heads/main"]);
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
        .bind("default")
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

async fn seed_completed_reviewer_execution(
    db: &db::SqliteDb,
    task_id: &str,
    agent_id: &str,
    parent_execution_id: Option<&str>,
) -> db::Execution {
    let now = now_rfc3339();
    ExecutionRepo::create(
        db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            agent_id: Some(agent_id.to_owned()),
            role: crate::workflow::default_roles::REVIEWER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: parent_execution_id.map(str::to_owned),
            agent_session_id: Some("reviewer-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("reviewer returned without contract evidence".to_owned()),
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
    .expect("completed reviewer execution creates")
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

/// Budget for "a dispatch should arrive". The waiting loops break on the first
/// execution, so the whole budget is only ever spent on a failure — it is sized
/// for a saturated CI runner, not for the expected path.
const DISPATCH_WAIT_ATTEMPTS: usize = 40;
const DISPATCH_WAIT_STEP: Duration = Duration::from_millis(250);

/// Why a dispatch never arrived, in the dispatcher's own terms. A parked Task
/// and a starved one look identical from a bare timeout, and only the first is
/// a product failure.
async fn describe_stalled_dispatch(db: &db::SqliteDb, task_id: &str) -> String {
    let Ok(Some(task)) = TaskRepo::get_by_id(db, task_id, false).await else {
        return format!("task {task_id} no longer loads");
    };
    match deferred_dispatch::dispatch_disposition_for_test(&task) {
        Some(disposition) => format!(
            "task is {} and parked at version {} on capability {}: {}",
            task.status, disposition.task_version, disposition.capability, disposition.safe_message
        ),
        None => format!(
            "task is {} with no recorded dispatch disposition",
            task.status
        ),
    }
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

/// A bare Project with no repository, as it looks before provisioning
/// attaches one — the shape `dispatcher_pauses_a_project_with_no_repository`
/// and its neighbors exercise.
async fn seed_unprovisioned_project(db: &db::SqliteDb, name: &str) -> String {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: name.to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("project creates");
    project_id
}

#[tokio::test]
async fn dispatcher_pauses_a_project_with_no_primary_repository() {
    let db = Arc::new(sqlite_db().await);
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let project_id = seed_unprovisioned_project(&db, "Unprovisioned").await;
    let (dispatcher, _rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    dispatcher.check_once().await.expect("dispatcher runs");

    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    assert!(project.paused_at.is_some());
    assert_eq!(
        project.system_pause_reason.as_deref(),
        Some(super::repo_pause_sync::MISSING_REPOSITORY)
    );
}

#[tokio::test]
async fn dispatcher_pauses_a_project_with_cross_project_primary_repository() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let project_id = seed_unprovisioned_project(&db, "Invalid repository owner").await;
    let (_other_project_id, other_repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    ProjectRepo::update_at_version(
        &*db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(other_repo_id)),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        project.version,
        None,
    )
    .await
    .expect("cross-Project pointer stores for legacy-corruption fixture");
    let (dispatcher, _rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    assert_eq!(dispatcher.check_once().await.expect("dispatcher runs"), 0);

    let paused = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project reloads")
        .expect("project exists");
    assert!(paused.paused_at.is_some());
    assert_eq!(
        paused.system_pause_reason.as_deref(),
        Some(super::repo_pause_sync::INVALID_REPOSITORY)
    );
}

#[tokio::test]
async fn dispatcher_dispatches_pre_repository_task_after_primary_repo_is_attached() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let project_id = seed_unprovisioned_project(&db, "Unprovisioned").await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "created before repository", "todo", 1).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    assert_eq!(dispatcher.check_once().await.expect("dispatcher runs"), 0);
    let paused = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    assert!(paused.paused_at.is_some());

    // Provisioning succeeds later and attaches the repository, the way a
    // retried scaffold does.
    setup_git_repo(repo_dir.path());
    let repo_id = new_uuid_v4();
    RepoRepo::create(
        &*db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "forge".to_owned(),
            remote_url: repo_dir.path().to_string_lossy().into_owned(),
            local_path: Some(repo_dir.path().to_string_lossy().into_owned()),
            work_mode: db::WorkMode::DirectMerge,
            default_branch: "main".to_owned(),
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        &*db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(&*db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project repository attaches");

    // The first scan after attachment only clears the system-owned pause so
    // this check never dispatches from the stale in-memory Project snapshot.
    assert_eq!(dispatcher.check_once().await.expect("dispatcher runs"), 0);

    let resumed = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    assert!(resumed.paused_at.is_none());
    assert!(resumed.system_pause_reason.is_none());

    let (first_scan, second_scan) = tokio::join!(dispatcher.check_once(), dispatcher.check_once());
    assert_eq!(
        first_scan.expect("first concurrent dispatcher scan runs")
            + second_scan.expect("second concurrent dispatcher scan runs"),
        1,
        "concurrent scans must accept exactly one dispatch"
    );
    let execution_ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("execution spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, task.id);

    let dispatched_task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("Task reloads")
        .expect("Task exists");
    assert_eq!(
        dispatched_task.status,
        crate::workflow::default_states::IN_PROGRESS
    );
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER,
        )
        .await
        .expect("execution count loads"),
        1
    );
    let workspace = WorkspaceRepo::get_by_task_id(&*db, &task.id)
        .await
        .expect("Workspace loads")
        .expect("Workspace exists");
    assert_eq!(workspace.repo_id, repo_id);
    let lease = WorkspaceLeaseRepo::get_active_for_task(&*db, &task.id)
        .await
        .expect("Workspace lease loads")
        .expect("Workspace lease exists");
    assert_eq!(lease.repository_binding_id, workspace.repo_id);

    assert_eq!(
        dispatcher.check_once().await.expect("dispatcher runs"),
        0,
        "an already-running Task must not be dispatched twice"
    );
    assert!(rx.try_recv().is_err());
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER,
        )
        .await
        .expect("execution count reloads"),
        1
    );
}

#[tokio::test]
async fn dispatcher_leaves_a_deliberately_paused_project_alone() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let project_id = seed_unprovisioned_project(&db, "Manually paused").await;
    // A user pause carries no system reason, exactly like the real
    // `POST /projects/{id}/pause` route.
    let paused_at = now_rfc3339();
    ProjectRepo::set_paused_at(&*db, &project_id, Some(paused_at.clone()))
        .await
        .expect("manual pause sets paused_at");
    let (dispatcher, _rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    dispatcher.check_once().await.expect("dispatcher runs");

    let still_paused = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    assert_eq!(still_paused.paused_at.as_deref(), Some(paused_at.as_str()));
    assert!(still_paused.system_pause_reason.is_none());

    // Attaching a repository must not auto-resume a pause the dispatcher
    // never issued.
    let default_branch = setup_git_repo(repo_dir.path());
    let repo_id = new_uuid_v4();
    RepoRepo::create(
        &*db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "manual-pause-repo".to_owned(),
            remote_url: repo_dir.path().to_string_lossy().into_owned(),
            local_path: Some(repo_dir.path().to_string_lossy().into_owned()),
            work_mode: db::WorkMode::DirectMerge,
            default_branch,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("same-Project Repo creates");
    ProjectRepo::update_at_version(
        &*db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id)),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(&*db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project repository attaches");

    dispatcher.check_once().await.expect("dispatcher runs");

    let untouched = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    assert_eq!(untouched.paused_at.as_deref(), Some(paused_at.as_str()));
}

#[tokio::test]
async fn stale_repository_resume_cannot_clear_a_later_manual_pause() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let project_id = seed_unprovisioned_project(&db, "stale repository resume").await;
    let (dispatcher, _rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    dispatcher
        .check_once()
        .await
        .expect("dispatcher pauses project");
    let auto_paused = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("auto-paused Project loads")
        .expect("Project exists");
    let auto_pause_reason = auto_paused
        .system_pause_reason
        .clone()
        .expect("dispatcher records its pause reason");
    let auto_pause_at = auto_paused
        .paused_at
        .clone()
        .expect("dispatcher records its pause timestamp");

    // Attaching a repository makes the stale snapshot eligible for an
    // automatic resume, but deliberately keep that snapshot in hand while a
    // user pause wins the race.
    let default_branch = setup_git_repo(repo_dir.path());
    let repo_id = new_uuid_v4();
    RepoRepo::create(
        &*db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "stale-resume-repo".to_owned(),
            remote_url: repo_dir.path().to_string_lossy().into_owned(),
            local_path: Some(repo_dir.path().to_string_lossy().into_owned()),
            work_mode: db::WorkMode::DirectMerge,
            default_branch,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("repository creates");
    let attached = ProjectRepo::update_at_version(
        &*db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id)),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        auto_paused.version,
        None,
    )
    .await
    .expect("repository attaches");
    assert_eq!(attached.version, auto_paused.version + 1);
    assert_eq!(attached.paused_at.as_deref(), Some(auto_pause_at.as_str()));
    assert_eq!(
        attached.system_pause_reason.as_deref(),
        Some(auto_pause_reason.as_str())
    );

    let manual_pause_at = "2099-01-01T00:00:00Z".to_owned();
    ProjectRepo::set_paused_at(&*db, &project_id, Some(manual_pause_at.clone()))
        .await
        .expect("manual pause wins race");

    // The dispatcher still has the exact post-attachment snapshot, but its
    // CAS must reject the now-stale system pause rather than clearing the
    // user's pause.
    assert!(!dispatcher
        .sync_repository_pause(&attached)
        .await
        .expect("stale auto-resume is benign"));
    let current = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("Project reloads")
        .expect("Project exists");
    assert_eq!(current.paused_at.as_deref(), Some(manual_pause_at.as_str()));
    assert!(current.system_pause_reason.is_none());
    assert_eq!(current.version, attached.version + 1);
}

#[tokio::test]
async fn stale_repository_pause_cannot_pause_after_repository_attachment() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let project_id = seed_unprovisioned_project(&db, "stale repository pause").await;
    let stale_active = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("Project loads")
        .expect("Project exists");
    let (dispatcher, _rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let default_branch = setup_git_repo(repo_dir.path());
    let repo_id = new_uuid_v4();
    RepoRepo::create(
        &*db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "stale-pause-repo".to_owned(),
            remote_url: repo_dir.path().to_string_lossy().into_owned(),
            local_path: Some(repo_dir.path().to_string_lossy().into_owned()),
            work_mode: db::WorkMode::DirectMerge,
            default_branch,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("repository creates");
    ProjectRepo::update_at_version(
        &*db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id)),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        stale_active.version,
        None,
    )
    .await
    .expect("repository attaches");

    // This is the old scanner snapshot: it observed no primary repository,
    // but the repository authority changed before its write boundary.
    assert!(!dispatcher
        .sync_repository_pause(&stale_active)
        .await
        .expect("stale auto-pause is benign"));
    let current = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("Project reloads")
        .expect("Project exists");
    assert!(current.paused_at.is_none());
    assert!(current.system_pause_reason.is_none());
}

#[tokio::test]
async fn dispatcher_gives_unassigned_initial_tasks_the_project_defaults() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    // Already released to `todo` but never assigned: exactly the shape a Task
    // proposed before provisioning has once it leaves backlog.
    let task = seed_task(&db, &project_id, "released but unassigned", "todo", 1).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Offline, AgentStatus::Idle).await;
    ProjectRepo::update_at_version(
        &*db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: Some(
                serde_json::json!({
                    "default_role_assignments": [
                        { "role_name": "coder", "assignee_type": "agent", "assignee_id": agent_id }
                    ]
                })
                .to_string(),
            ),
            primary_repo_id: None,
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(&*db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project defaults update");
    let (dispatcher, _rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    dispatcher.check_once().await.expect("dispatcher runs");

    let coder = TaskRoleAssignmentRepo::get_by_task_and_role(
        &*db,
        &task.id,
        crate::workflow::default_roles::CODER,
    )
    .await
    .expect("assignment loads")
    .expect("unassigned Task received the Project's default coder");
    assert_eq!(coder.assignee_id.as_deref(), Some(agent_id.as_str()));
}

#[tokio::test]
async fn dispatcher_check_once_does_not_dispatch_after_stop() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "high", "todo", 1).await;
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
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "high", "todo", 1).await;
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
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(
        &db,
        &project_id,
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
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    set_planning_gate_auto_approval(&db, &project_id).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "auto plan", "todo", 1).await;
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
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(
        &db,
        &project_id,
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
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let blocked = seed_task(&db, &project_id, "blocked", "in_progress", 0).await;
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
    let task = seed_task(&db, &project_id, "todo", "todo", 0).await;
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
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Offline, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "todo", "todo", 0).await;
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
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "todo", "todo", 0).await;
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
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "active", "in_progress", 0).await;
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
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "review", "review", 0).await;
    // Review is entered from a finished implementation attempt, and that
    // attempt is what the recovered reviewer reviews.
    seed_completed_coder_execution(&db, &task.id).await;
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
    // The shell reviewer no longer emits a hardcoded PASS; the dispatcher must
    // thread the frozen review contract to it instead.
    assert!(ctx.description.contains("FORGE_REVIEW_CONTRACT"));
    assert!(ctx.description.contains("FORGE_GOVERNING_CONTEXT"));
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
async fn dispatcher_reconciles_completed_reviewer_and_launches_its_retry() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "stuck review", "review", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::REVIEWER,
        &agent_id,
    )
    .await;
    let candidate_execution = seed_completed_coder_execution(&db, &task.id).await;
    seed_running_review(&db, &task.id, &candidate_execution, r#"{"ci_steps":[]}"#).await;
    let execution =
        seed_completed_reviewer_execution(&db, &task.id, &agent_id, Some(&candidate_execution))
            .await;
    let review = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("review loads")
        .into_iter()
        .next()
        .expect("review exists");
    sqlx::query("UPDATE review SET reviewer_execution_id = ? WHERE id = ?")
        .bind(&execution.id)
        .bind(&review.id)
        .execute(db.pool())
        .await
        .expect("reviewer attempt binding records");
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let first = dispatcher
        .check_once()
        .await
        .expect("dispatcher reconciles");

    assert_eq!(first, 0);
    let review = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load")
        .into_iter()
        .next()
        .expect("review exists");
    assert_eq!(review.status, ReviewStatus::Running);
    let details: serde_json::Value =
        serde_json::from_str(&review.step_results_json).expect("review details parse");
    assert_eq!(details["execution_retry"]["execution_id"], execution.id);
    let execution = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(execution.resume_policy, Some(ResumePolicy::Auto));

    let deferred = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(deferred_dispatch::pending_until(&deferred).is_some());
    deferred_dispatch::clear(&db, &deferred)
        .await
        .expect("retry backoff elapses");

    let second = dispatcher
        .check_once()
        .await
        .expect("dispatcher launches retry");

    assert_eq!(second, 1);
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("review retry spawned in time")
        .expect("review retry context received");
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
async fn dispatcher_never_reuses_reviewer_execution_for_newer_review_attempt() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "fresh review", "review", 0).await;
    sqlx::query("UPDATE task SET task_type = 'discovery' WHERE id = ?")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("task becomes read-only");
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::REVIEWER,
        &agent_id,
    )
    .await;
    let stale_candidate_execution = seed_completed_coder_execution(&db, &task.id).await;
    let stale = seed_completed_reviewer_execution(
        &db,
        &task.id,
        &agent_id,
        Some(&stale_candidate_execution),
    )
    .await;
    sqlx::query("UPDATE execution SET created_at = ?, updated_at = ?, stopped_at = ? WHERE id = ?")
        .bind("2000-01-01T00:00:00Z")
        .bind("2000-01-01T00:00:01Z")
        .bind("2000-01-01T00:00:01Z")
        .bind(&stale.id)
        .execute(db.pool())
        .await
        .expect("stale reviewer parent binding moves behind current review");
    let candidate_execution = seed_completed_coder_execution(&db, &task.id).await;
    seed_running_review(&db, &task.id, &candidate_execution, r#"{"ci_steps":[]}"#).await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("fresh reviewer spawned in time")
        .expect("reviewer execution context received");
    assert_eq!(ctx.task_id, task.id);
    assert_ne!(ctx.execution_id, stale.id);
    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1, "no synthetic review row is created");
    assert_eq!(reviews[0].status, ReviewStatus::Running);
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::REVIEWER,
        )
        .await
        .expect("reviewer execution count loads"),
        2
    );
}

#[tokio::test]
async fn reviewer_completion_guard_fails_closed_without_exact_binding() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "ambiguous review", "review", 0).await;
    let candidate_execution = seed_completed_coder_execution(&db, &task.id).await;
    seed_running_review(&db, &task.id, &candidate_execution, r#"{"ci_steps":[]}"#).await;
    let stale = seed_completed_reviewer_execution(&db, &task.id, &agent_id, None).await;

    // The reviewer is intentionally newer by wall-clock time, but it has no
    // exact Review-attempt binding. Candidate parentage and timestamps cannot
    // repair that missing identity.
    sqlx::query("UPDATE execution SET created_at = ? WHERE id = ?")
        .bind("2026-01-01T00:00:01Z")
        .bind(&stale.id)
        .execute(db.pool())
        .await
        .expect("execution timestamp updates");
    sqlx::query("UPDATE review SET started_at = ? WHERE task_id = ?")
        .bind("2026-01-01T00:00:00Z")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("review timestamp updates");
    let execution = ExecutionRepo::get_by_id(&*db, &stale.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    let review = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load")
        .into_iter()
        .next()
        .expect("review exists");
    assert!(
        crate::task_service::execution::reviewer_execution_lacks_exact_review_binding(
            &execution, &review,
        )
    );

    let mut rebound = review.clone();
    rebound.reviewer_execution_id = Some("replacement-reviewer".to_owned());
    assert!(
        crate::task_service::execution::reviewer_execution_lacks_exact_review_binding(
            &execution, &rebound,
        ),
        "replacing a Review attempt binding must reject the old execution"
    );

    sqlx::query("UPDATE execution SET created_at = ? WHERE id = ?")
        .bind("not-an-rfc3339-timestamp")
        .bind(&stale.id)
        .execute(db.pool())
        .await
        .expect("malformed execution timestamp updates");
    let execution = ExecutionRepo::get_by_id(&*db, &stale.id)
        .await
        .expect("execution reloads")
        .expect("execution exists");
    assert!(
        crate::task_service::execution::reviewer_execution_lacks_exact_review_binding(
            &execution, &review,
        )
    );

    sqlx::query("UPDATE execution SET created_at = ? WHERE id = ?")
        .bind("2026-01-01T00:00:01Z")
        .bind(&stale.id)
        .execute(db.pool())
        .await
        .expect("execution timestamp restores");
    sqlx::query("UPDATE review SET started_at = ? WHERE task_id = ?")
        .bind("not-an-rfc3339-timestamp")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("malformed review timestamp updates");
    let execution = ExecutionRepo::get_by_id(&*db, &stale.id)
        .await
        .expect("execution reloads")
        .expect("execution exists");
    let review = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews reload")
        .into_iter()
        .next()
        .expect("review exists");
    assert!(
        crate::task_service::execution::reviewer_execution_lacks_exact_review_binding(
            &execution, &review,
        )
    );
}

#[tokio::test]
async fn dispatcher_resumes_integration_deferred_by_project_pause() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let task = seed_task(&db, &project_id, "accepted review", "review", 0).await;
    crate::deferred_dispatch::defer_integration_for_pause(&db, &task)
        .await
        .expect("paused integration marker records");
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    // This fixture has no workspace, so the resumed merge hook is skipped.
    // Success-atomic recovery must keep the marker for a later retry instead
    // of reporting a dispatched integration that never ran.
    assert_eq!(dispatched, 0);
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, crate::workflow::default_states::MERGING);
    assert!(crate::deferred_dispatch::paused_integration(&current).is_some());
    assert!(tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn losing_paused_integration_refresh_cannot_resurrect_after_clear() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let task = seed_task(&db, &project_id, "accepted review", "review", 0).await;
    let stale_before_marker = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("pre-marker task loads")
        .expect("pre-marker task exists");
    crate::deferred_dispatch::defer_integration_for_pause(&db, &task)
        .await
        .expect("paused integration marker records");
    let stale = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("stale task loads")
        .expect("stale task exists");
    let marker = crate::deferred_dispatch::paused_integration(&stale).expect("stale marker exists");

    crate::deferred_dispatch::clear_paused_integration(&db, &task.id, &marker)
        .await
        .expect("successful worker clears marker");
    crate::deferred_dispatch::defer_integration_for_pause(&db, &stale_before_marker)
        .await
        .expect("stale producer is conditionally ignored");
    crate::deferred_dispatch::refresh_paused_integration_for_pause(&db, &stale)
        .await
        .expect("losing worker refreshes conditionally");

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("current task loads")
        .expect("current task exists");
    assert!(crate::deferred_dispatch::paused_integration(&current).is_none());
}

#[tokio::test]
async fn reviewer_assignment_after_stopped_attempt_dispatches_without_separate_resume() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "review retry", "review", 0).await;
    // The reviewer retry reviews the implementation attempt that put this
    // Task in review.
    seed_completed_coder_execution(&db, &task.id).await;
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
async fn coordination_root_target_moved_rebase_returns_to_aggregate_review() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;

    // Keep aggregate review parked for a human decision so this test observes
    // the recovery destination without needing a reviewer execution/workspace.
    let mut workflow = crate::workflow::default_workflow::default_workflow();
    let review = workflow
        .states
        .iter_mut()
        .find(|state| state.name == crate::workflow::default_states::REVIEW)
        .expect("default workflow has review state");
    review.role = None;
    review.dispatch = None;
    let review_gate = review.gate_config.as_mut().expect("review has gate config");
    review_gate.requires_user_approval = Some(true);
    review_gate.optional_when_unassigned = Some(false);
    sqlx::query(
        "UPDATE project SET workflow_definition = ?, workflow_template_name = ?, updated_at = ? WHERE id = ?",
    )
    .bind(serde_json::to_string(&workflow).expect("workflow serializes"))
    .bind("human-required")
    .bind(now_rfc3339())
    .bind(&project_id)
    .execute(db.pool())
    .await
    .expect("project workflow updates");

    let root = seed_task(
        &db,
        &project_id,
        "coordination root",
        crate::workflow::default_states::MERGE_FAILED,
        0,
    )
    .await;
    let now = now_rfc3339();
    TaskRepo::create(
        &*db,
        CreateTask {
            id: new_uuid_v4(),
            project_id: project_id.clone(),
            parent_task_id: Some(root.id.clone()),
            subtask_order: Some(0),
            assignee_type: None,
            assignee_id: None,
            title: "completed child".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: crate::workflow::default_states::DONE.to_owned(),
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
    .expect("completed child creates");
    TransitionLogRepo::insert(
        &*db,
        db::CreateTransitionLog {
            id: new_uuid_v4(),
            task_id: root.id.clone(),
            from_state: crate::workflow::default_states::MERGING.to_owned(),
            to_state: crate::workflow::default_states::MERGE_FAILED.to_owned(),
            trigger_name: Some("retry".to_owned()),
            triggered_by: api_types::Actor::system(api_types::SystemComponent::Workflow).display(),
            trigger_reason: format!(
                "{} {} target advanced; re-review required",
                crate::workflow::REVIEW_REFRESH_MARKER,
                crate::workflow::TARGET_MOVED_MARKER
            ),
            hook_results_json: None,
            rejection: false,
            created_at: now_rfc3339(),
        },
    )
    .await
    .expect("review-refresh transition records");

    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    let recovered = TaskRepo::get_by_id(&*db, &root.id, false)
        .await
        .expect("coordination root reloads")
        .expect("coordination root exists");
    assert_eq!(
        recovered.status,
        crate::workflow::default_states::REVIEW,
        "a clean target-moved rebase must return the root to aggregate review"
    );
    assert!(!crate::task_service::coordination_review_pending(
        &recovered
    ));
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &root.id,
            crate::workflow::default_roles::CODER,
        )
        .await
        .expect("root coder execution count loads"),
        0,
        "coordination roots must never receive a merge-fix coder turn"
    );
    assert!(
        rx.try_recv().is_err(),
        "aggregate review should wait for a user"
    );
}

/// Regression coverage for F10: a Task whose newest coder Execution is the
/// normal, *successful* run that preceded review/merge (e.g. a Task sitting
/// in `merge_failed` after the coder's own attempt completed cleanly) must
/// never be gated behind the "explicit retry required" rule. That rule
/// exists for genuinely stopped attempts, not for a completed one.
#[tokio::test]
async fn latest_stopped_execution_blocks_dispatch_ignores_completed_execution() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "merge failed", "merge_failed", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    seed_completed_coder_execution(&db, &task.id).await;

    let blocked = super::helpers::latest_stopped_execution_blocks_dispatch(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
    )
    .await
    .expect("guard evaluates");

    assert!(
        !blocked,
        "a completed coder execution must not gate re-dispatch out of merge_failed"
    );
}

/// The other half of the F10 fix: a genuinely stopped attempt (failed or
/// cancelled, with no auto-retry policy) must keep gating re-dispatch exactly
/// as before — only `Completed` is now exempt.
#[tokio::test]
async fn latest_stopped_execution_blocks_dispatch_still_gates_failed_and_cancelled_executions() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;

    let cancelled_task = seed_task(&db, &project_id, "cancelled coder", "merge_failed", 0).await;
    assign_role(
        &db,
        &cancelled_task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    seed_cancelled_execution(
        &db,
        &cancelled_task.id,
        &agent_id,
        crate::workflow::default_roles::CODER,
        Some(StopReason::AgentTimeout),
        None,
    )
    .await;

    let cancelled_blocked = super::helpers::latest_stopped_execution_blocks_dispatch(
        &db,
        &cancelled_task.id,
        crate::workflow::default_roles::CODER,
    )
    .await
    .expect("guard evaluates");
    assert!(
        cancelled_blocked,
        "a cancelled coder execution with no explicit retry must still gate re-dispatch"
    );

    let failed_task = seed_task(&db, &project_id, "failed coder", "merge_failed", 0).await;
    assign_role(
        &db,
        &failed_task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let now = now_rfc3339();
    ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: failed_task.id.clone(),
            agent_id: Some(agent_id.clone()),
            role: crate::workflow::default_roles::CODER.to_owned(),
            status: ExecutionStatus::Failed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some("intentional failure".to_owned()),
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("failed execution creates");

    let failed_blocked = super::helpers::latest_stopped_execution_blocks_dispatch(
        &db,
        &failed_task.id,
        crate::workflow::default_roles::CODER,
    )
    .await
    .expect("guard evaluates");
    assert!(
        failed_blocked,
        "a failed coder execution with no explicit retry must still gate re-dispatch"
    );
}

#[tokio::test]
async fn dispatcher_respects_priority_ordering() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let low = seed_task(&db, &project_id, "low", "todo", 1).await;
    assign_role(
        &db,
        &low.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let high = seed_task(&db, &project_id, "high", "todo", "10".parse().unwrap()).await;
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
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "cancelled", "in_progress", 0).await;
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
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "cancelled", "in_progress", 0).await;
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
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "cancelled", "in_progress", 0).await;
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
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "cancelled", "in_progress", 0).await;
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
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "legacy", "in_progress", 0).await;
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
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "blocked", "in_progress", 0).await;
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
    // A task parked in todo after a deterministic dispatch failure (e.g. its
    // configured Agent source is disabled) must not be rescheduled until a
    // user restarts it — otherwise the dispatcher loops todo -> ... -> todo.
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "parked", "todo", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let annotation = serde_json::json!({
        "type": "dispatch_failed",
        "message": "repository Task cannot run while its configured Agent source is disabled",
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
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "review", "review", 0).await;
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
        r#"{"ci_steps":[{"index":0,"command":"test -d .","exit_code":0,"stderr_tail":"","output_tail":""}]}"#,
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

#[tokio::test]
async fn dispatcher_dispatches_read_only_reviewer_without_ci_review_record() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "research review", "review", 0).await;
    let task = set_review_ci_config(&db, &task).await;
    sqlx::query("UPDATE task SET task_type = 'discovery' WHERE id = ?")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("task becomes read-only discovery work");
    seed_completed_coder_execution(&db, &task.id).await;
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
/// implementation Tasks derive their authority from that Charter.
async fn make_project_charter_backed(db: &db::SqliteDb, project_id: &str) {
    let now = now_rfc3339();
    let owner_id = new_uuid_v4();
    let charter_id = format!("{project_id}-charter");
    let revision_id = format!("{charter_id}-revision-1");
    // The review contract recomputes this digest from the stored content, so a
    // placeholder would fail admission before any role dispatches.
    let charter_content = serde_json::json!({
        "identity": {
            "working_name": "Dispatch Charter",
            "slug_proposal": "dispatch-charter",
            "one_line_vision": "Keep charter-backed work dispatchable.",
            "maturity": "mvp"
        },
        "problem_and_people": {
            "problem_or_opportunity": "Charter-backed tasks must reach their worker.",
            "target_users": ["Forge maintainers"]
        },
        "core_experience": {
            "primary_outcome": "The dispatcher starts charter-backed work."
        },
        "scope": {
            "must_have_outcomes": ["Dispatch charter-backed work."],
            "explicit_non_goals": []
        },
        "success": {
            "acceptance_statements": ["The configured worker receives the task."]
        },
        "constraints_and_risks": {},
        "knowledge_ledger": {"items": []}
    });
    let charter_typed: api_types::ProjectCharterContent =
        serde_json::from_value(charter_content.clone()).expect("fixture Charter content is valid");
    let charter_digest = crate::project_orchestration::charter_content_digest(&charter_typed);
    let charter_content_json = charter_content.to_string();
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
                   'forge.project-charter-render/v1', ?, '# Project',
                   'dispatch quiescence fixture', 'user', ?, '[]',
                   ?, 'dispatch-charter-render-digest', ?)",
    )
    .bind(&revision_id)
    .bind(&charter_id)
    .bind(&charter_content_json)
    .bind(&owner_id)
    .bind(&charter_digest)
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

async fn make_task_charter_governed(db: &db::SqliteDb, task: &Task) {
    let now = now_rfc3339();
    let charter_revision_id: String = sqlx::query_scalar::<_, Option<String>>(
        "SELECT current_charter_revision_id FROM project WHERE id = ?",
    )
    .bind(&task.project_id)
    .fetch_one(db.pool())
    .await
    .expect("Charter revision reads")
    .expect("Project has a current Charter revision");
    sqlx::query(
        "INSERT INTO project_task_governance (
             task_id, project_id, charter_revision_id, document_revisions_json,
             capability_class, risk_class, runnable, provenance_json,
             version, created_at, updated_at
         ) VALUES (?, ?, ?, '[]', 'repository_write', 'low', 1,
                   '{\"charter_authority\":true}', 1, ?, ?)",
    )
    .bind(&task.id)
    .bind(&task.project_id)
    .bind(&charter_revision_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("Task Charter governance inserts");
}

#[tokio::test]
async fn dispatcher_parks_charter_task_with_missing_governance_before_execution() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    make_project_charter_backed(&db, &project_id).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(
        &db,
        &project_id,
        "Charter work missing governance",
        "in_progress",
        0,
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

    assert_eq!(dispatcher.check_once().await.expect("dispatcher runs"), 0);
    assert!(rx.try_recv().is_err());
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER,
        )
        .await
        .expect("execution count loads"),
        0
    );
    assert!(
        WorkspaceRepo::get_by_task_id(&*db, &task.id)
            .await
            .expect("Workspace lookup succeeds")
            .is_none(),
        "governance admission must fail before Workspace creation"
    );
    let parked = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("Task reloads")
        .expect("Task exists");
    let disposition = deferred_dispatch::dispatch_disposition_for_test(&parked)
        .expect("missing governance records a visible deterministic blocker");
    assert!(disposition.safe_message.contains("Charter governance"));
    assert!(disposition.safe_message.contains("missing or stale"));
}

#[tokio::test]
async fn dispatcher_starts_charter_backed_work_without_a_baseline_gate() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    make_project_charter_backed(&db, &project_id).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "charter work", "in_progress", 0).await;
    make_task_charter_governed(&db, &task).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let mut total_progress = 0;
    let mut execution_received = false;
    // The loop exits on the first dispatch, so the budget only matters on a
    // runner slow enough to starve the spawned executor. Keep it generous: a
    // dispatch that never lands is a real refusal, and the report below says
    // which one rather than leaving a bare timeout.
    for _ in 0..DISPATCH_WAIT_ATTEMPTS {
        total_progress += dispatcher.check_once().await.expect("dispatcher runs");
        if tokio::time::timeout(DISPATCH_WAIT_STEP, rx.recv())
            .await
            .is_ok()
        {
            execution_received = true;
            break;
        }
    }
    if total_progress == 0 {
        panic!(
            "Charter-backed work leaves its initial state: {}",
            describe_stalled_dispatch(&db, &task.id).await
        );
    }
    assert!(
        execution_received,
        "the workflow reaches its configured worker"
    );

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, "in_progress");
    assert!(deferred_dispatch::dispatch_disposition_for_test(&current).is_none());
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
async fn wake_does_not_duplicate_already_dispatched_charter_work() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    make_project_charter_backed(&db, &project_id).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "charter work", "in_progress", 0).await;
    make_task_charter_governed(&db, &task).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let mut execution_received = false;
    for _ in 0..DISPATCH_WAIT_ATTEMPTS {
        dispatcher.check_once().await.expect("dispatcher runs");
        if tokio::time::timeout(DISPATCH_WAIT_STEP, rx.recv())
            .await
            .is_ok()
        {
            execution_received = true;
            break;
        }
    }
    if !execution_received {
        panic!(
            "executor receives initial dispatch: {}",
            describe_stalled_dispatch(&db, &task.id).await
        );
    }
    deferred_dispatch::wake_task_dispatch(&db, &task.id, "test: redundant wake")
        .await
        .expect("wake succeeds");

    let woken = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(
        deferred_dispatch::dispatch_disposition_for_test(&woken).is_none(),
        "an already-dispatched Task has no deferred disposition"
    );

    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");
    assert_eq!(dispatched, 0, "the wake does not duplicate active work");

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

#[test]
fn a_running_execution_slot_is_not_a_deterministic_dispatch_refusal() {
    // A slot held by a concurrently running execution frees itself when that
    // execution terminalises, and nothing about that touches the Task row. If
    // this were classified as deterministic, the dispatcher would record a
    // disposition and never reconsider the Task — the wedge this guards.
    assert!(!helpers::is_deterministic_dispatch_refusal(
        &crate::ServiceError::ExecutionAlreadyRunning {
            scope: "repository".to_owned(),
            execution_id: new_uuid_v4(),
        }
    ));
    assert!(!helpers::is_deterministic_dispatch_refusal(
        &crate::ServiceError::ExecutionAlreadyRunning {
            scope: crate::workflow::default_roles::REVIEWER.to_owned(),
            execution_id: new_uuid_v4(),
        }
    ));

    // The contrast that must keep working: a governance refusal is durable and
    // still earns a disposition.
    assert!(helpers::is_deterministic_dispatch_refusal(
        &crate::ServiceError::GuardRejection {
            guard: "dependency_gate".to_owned(),
            reason: "task has 1 unsatisfied dependency".to_owned(),
        }
    ));
}
