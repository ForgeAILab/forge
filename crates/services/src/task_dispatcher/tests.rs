use std::{future::pending, path::Path, sync::Arc};

use api_types::{Actor, SystemComponent};
use async_trait::async_trait;
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentRepo, AgentStatus,
    CreateAgent, CreateProject, CreateRepo, CreateTask, CreateTaskRoleAssignment, CreateWorkspace,
    DaemonRepo, DaemonStatus, ExecutionRepo, ExecutionStatus, PageRequest, RepoRepo, ResumePolicy,
    ReviewRepo, ReviewStatus, SortBy, SortOrder, StopReason, TaskRepo, TaskRoleAssignmentRepo,
    TransitionLogRepo, UpdateDaemonReport, UpdateProject, UpdateTask, UpsertDaemon,
    WorkspaceLeaseRepo, WorkspaceRepo, WorkspaceStatus,
};
use executors::{ExecutionContext, ExecutionResult, ExecutorError, TaskExecutor};
use tempfile::TempDir;
use tokio::sync::mpsc;
use workspace::RepoCacheLockManager;

use crate::{deferred_dispatch, ServiceError};

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
            remote_url: Some(repo_path.to_string_lossy().into_owned()),
            local_path: Some(repo_path.to_string_lossy().into_owned()),
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
    seed_agent_with_executor(
        db,
        max_concurrent_tasks,
        daemon_status,
        agent_status,
        "shell",
    )
    .await
}

async fn seed_agent_with_executor(
    db: &db::SqliteDb,
    max_concurrent_tasks: i64,
    daemon_status: DaemonStatus,
    agent_status: AgentStatus,
    executor_type: &str,
) -> String {
    let now = now_rfc3339();
    let daemon_id = DaemonRepo::upsert_by_machine_id(
        db,
        UpsertDaemon {
            max_concurrent_runs: None,
            id: new_uuid_v4(),
            machine_id: crate::embedded_daemon::embedded_machine_id(),
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
            name: executor_type.to_owned(),
            description: None,
            executor_type: executor_type.to_owned(),
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

// Keep stopped execution snapshots aligned with their prompt-consuming Agent.
async fn set_prompt_execution_snapshots(db: &db::SqliteDb, agent_id: &str) {
    sqlx::query("UPDATE execution SET executor_config_snapshot_json = json_set(executor_config_snapshot_json, '$.executor_type', 'codex') WHERE agent_id = ?")
        .bind(agent_id).execute(db.pool()).await.unwrap();
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

async fn seed_subtask(
    db: &db::SqliteDb,
    root: &Task,
    title: &str,
    status: &str,
    subtask_order: i64,
) -> Task {
    let now = now_rfc3339();
    TaskRepo::create(
        db,
        CreateTask {
            id: new_uuid_v4(),
            project_id: root.project_id.clone(),
            parent_task_id: Some(root.id.clone()),
            subtask_order: Some(subtask_order),
            assignee_type: None,
            assignee_id: None,
            title: title.to_owned(),
            description: Some("echo subtask".to_owned()),
            task_type: "sub_task".to_owned(),
            status: status.to_owned(),
            is_automation: false,
            priority: root.priority,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("subtask creates")
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
    set_planning_gate_user_approval(db, project_id, false).await;
}

async fn set_planning_gate_user_approval(
    db: &db::SqliteDb,
    project_id: &str,
    requires_user_approval: bool,
) {
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
        .requires_user_approval = Some(requires_user_approval);
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
    let project_version: i64 = sqlx::query_scalar(
        "SELECT project.version
         FROM project
         JOIN task ON task.project_id = project.id
         WHERE task.id = ?",
    )
    .bind(task_id)
    .fetch_one(db.pool())
    .await
    .expect("reviewer execution Project version loads");
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
                serde_json::json!({
                    "executor_type": "shell",
                    "config": {},
                    "project_version": project_version,
                })
                .to_string(),
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
            remote_url: Some(repo_dir.path().to_string_lossy().into_owned()),
            local_path: Some(repo_dir.path().to_string_lossy().into_owned()),
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
            remote_url: Some(repo_dir.path().to_string_lossy().into_owned()),
            local_path: Some(repo_dir.path().to_string_lossy().into_owned()),
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
            remote_url: Some(repo_dir.path().to_string_lossy().into_owned()),
            local_path: Some(repo_dir.path().to_string_lossy().into_owned()),
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
async fn dispatcher_holds_a_project_with_an_unborn_repository_until_its_first_commit() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let project_id = seed_unprovisioned_project(&db, "Unborn repository").await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(
        &db,
        &project_id,
        "created before the first commit",
        "todo",
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

    // A scaffold links its Repo before `main` has a commit.
    run_git(repo_dir.path(), &["init", "--initial-branch=main"]);
    run_git(
        repo_dir.path(),
        &["symbolic-ref", "HEAD", "refs/heads/main"],
    );
    run_git(repo_dir.path(), &["config", "user.email", "test@forge.dev"]);
    run_git(repo_dir.path(), &["config", "user.name", "Forge Test"]);
    let repo_id = new_uuid_v4();
    RepoRepo::create(
        &*db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "unborn".to_owned(),
            remote_url: Some(repo_dir.path().to_string_lossy().into_owned()),
            local_path: Some(repo_dir.path().to_string_lossy().into_owned()),
            default_branch: "main".to_owned(),
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("repo creates");
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
            primary_repo_id: Some(Some(repo_id)),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        project.version,
        None,
    )
    .await
    .expect("repository attaches");
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    assert_eq!(dispatcher.check_once().await.expect("dispatcher runs"), 0);
    let paused = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    assert!(paused.paused_at.is_some());
    assert_eq!(
        paused.system_pause_reason.as_deref(),
        Some(super::repo_pause_sync::REPOSITORY_NOT_READY)
    );
    // Held at the Project, not parked on a setup refusal nothing would wake.
    assert_eq!(dispatcher.check_once().await.expect("dispatcher runs"), 0);
    let held = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("Task reloads")
        .expect("Task exists");
    assert!(deferred_dispatch::dispatch_disposition_for_test(&held).is_none());

    // The first commit lands outside Forge.
    std::fs::write(repo_dir.path().join("README.md"), "# Forge\n").expect("README writes");
    run_git(repo_dir.path(), &["add", "-A"]);
    run_git(repo_dir.path(), &["commit", "-m", "initial commit"]);

    assert_eq!(dispatcher.check_once().await.expect("dispatcher runs"), 0);
    let resumed = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    assert!(resumed.paused_at.is_none());
    assert!(resumed.system_pause_reason.is_none());

    assert_eq!(dispatcher.check_once().await.expect("dispatcher runs"), 1);
    let execution_ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("execution spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, task.id);
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
            remote_url: Some(repo_dir.path().to_string_lossy().into_owned()),
            local_path: Some(repo_dir.path().to_string_lossy().into_owned()),
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
async fn dispatcher_inherits_root_default_coder_without_copying_it_to_subtask() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let next_agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let root = seed_task(&db, &project_id, "root", "todo", 1).await;
    let child = seed_subtask(&db, &root, "child", "todo", 0).await;
    assign_role(
        &db,
        &root.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    assert_eq!(dispatcher.check_once().await.expect("dispatcher runs"), 1);
    let execution_ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("execution spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, child.id);
    let execution = ExecutionRepo::latest_agent_execution_by_task(&*db, &child.id)
        .await
        .expect("execution loads")
        .expect("child execution exists");
    assert_eq!(execution.agent_id.as_deref(), Some(agent_id.as_str()));
    assert!(
        TaskRoleAssignmentRepo::get_by_task_and_role(
            &*db,
            &child.id,
            crate::workflow::default_roles::CODER,
        )
        .await
        .expect("child assignment loads")
        .is_none(),
        "inherited dispatch must not materialize a child assignment"
    );

    crate::test_support::configure_project_execution_test_setup(
        &db,
        &project_id,
        &next_agent_id,
        &next_agent_id,
    )
    .await;
    let now = now_rfc3339();
    dispatcher
        .task_service
        .reassign_role(
            CreateTaskRoleAssignment {
                id: new_uuid_v4(),
                task_id: root.id.clone(),
                role_name: crate::workflow::default_roles::CODER.to_owned(),
                assignee_type: Some(db::AssigneeKind::Agent),
                assignee_id: Some(next_agent_id.clone()),
                created_at: now.clone(),
                updated_at: now,
            },
            false,
            false,
        )
        .await
        .expect("root default worker changes while child runs");
    let running_child = TaskRepo::get_by_id(&*db, &child.id, false)
        .await
        .expect("child reloads")
        .expect("child exists");
    assert!(
        dispatcher
            .task_service
            .execution_owns_current_role_attempt(&running_child, &execution)
            .await
            .expect("execution ownership resolves"),
        "a root default change must not supersede the admitted child execution"
    );
    assert_eq!(
        ExecutionRepo::latest_agent_execution_by_task(&*db, &child.id)
            .await
            .expect("execution reloads")
            .expect("child execution still exists")
            .agent_id
            .as_deref(),
        Some(agent_id.as_str())
    );
}

#[tokio::test]
async fn dispatcher_prefers_subtask_coder_over_root_default() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let root_agent = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let child_agent = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let root = seed_task(&db, &project_id, "root", "todo", 1).await;
    let child = seed_subtask(&db, &root, "child", "todo", 0).await;
    assign_role(
        &db,
        &root.id,
        crate::workflow::default_roles::CODER,
        &root_agent,
    )
    .await;
    assign_role(
        &db,
        &child.id,
        crate::workflow::default_roles::CODER,
        &child_agent,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    assert_eq!(dispatcher.check_once().await.expect("dispatcher runs"), 1);
    let execution_ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("execution spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, child.id);
    let execution = ExecutionRepo::latest_agent_execution_by_task(&*db, &child.id)
        .await
        .expect("execution loads")
        .expect("child execution exists");
    assert_eq!(execution.agent_id.as_deref(), Some(child_agent.as_str()));
}

#[tokio::test]
async fn changing_root_default_wakes_and_dispatches_unstarted_subtask() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let first_agent = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let next_agent = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let root = seed_task(&db, &project_id, "root", "todo", 1).await;
    let child = seed_subtask(&db, &root, "child", "todo", 0).await;
    assign_role(
        &db,
        &root.id,
        crate::workflow::default_roles::CODER,
        &first_agent,
    )
    .await;
    deferred_dispatch::record_dispatch_disposition(
        &db,
        &child,
        crate::workflow::default_roles::CODER,
        "waiting for a usable default worker",
    )
    .await
    .expect("child dispatch parks");
    crate::test_support::configure_project_execution_test_setup(
        &db,
        &project_id,
        &next_agent,
        &next_agent,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    let now = now_rfc3339();
    dispatcher
        .task_service
        .reassign_role(
            CreateTaskRoleAssignment {
                id: new_uuid_v4(),
                task_id: root.id.clone(),
                role_name: crate::workflow::default_roles::CODER.to_owned(),
                assignee_type: Some(db::AssigneeKind::Agent),
                assignee_id: Some(next_agent.clone()),
                created_at: now.clone(),
                updated_at: now,
            },
            false,
            false,
        )
        .await
        .expect("root default worker changes");

    let woken_child = TaskRepo::get_by_id(&*db, &child.id, false)
        .await
        .expect("child reloads")
        .expect("child exists");
    assert!(deferred_dispatch::dispatch_disposition_for_test(&woken_child).is_none());
    assert_eq!(dispatcher.check_once().await.expect("dispatcher runs"), 1);
    let execution_ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("execution spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, child.id);
    let execution = ExecutionRepo::latest_agent_execution_by_task(&*db, &child.id)
        .await
        .expect("execution loads")
        .expect("child execution exists");
    assert_eq!(execution.agent_id.as_deref(), Some(next_agent.as_str()));
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

struct MergeGateFixture {
    db: Arc<db::SqliteDb>,
    _repo_dir: TempDir,
    _workspace_dir: TempDir,
    task: Task,
    merge_service: Arc<crate::merge_service::MergeService>,
    dispatcher: TaskDispatcher,
}

struct FailedReviewFixture {
    db: Arc<db::SqliteDb>,
    task: Task,
    dispatcher: TaskDispatcher,
    _repo_dir: TempDir,
}

async fn failed_review_fixture(failed_ago: chrono::Duration, budget: i32) -> FailedReviewFixture {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    crate::test_support::configure_project_execution_test_setup(
        &db,
        &project_id,
        &agent_id,
        &agent_id,
    )
    .await;
    let task = seed_task(&db, &project_id, "stranded failed CI", "review", 0).await;
    let candidate = seed_completed_coder_execution(&db, &task.id).await;
    let failed_at = (chrono::Utc::now() - failed_ago).to_rfc3339();
    let entered_at = (chrono::Utc::now() - failed_ago - chrono::Duration::seconds(1)).to_rfc3339();
    ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate,
            attempt_number: 1,
            status: ReviewStatus::Failed,
            step_results_json: r#"{"ci_steps":[{"index":0,"command":"false","exit_code":1,"stderr_tail":"CI failed"}]}"#
                .to_owned(),
            started_at: failed_at.clone(),
            created_at: failed_at.clone(),
            updated_at: failed_at,
        },
    )
    .await
    .unwrap();
    TransitionLogRepo::insert(
        &*db,
        db::CreateTransitionLog {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            from_state: "merge_failed".to_owned(),
            to_state: "review".to_owned(),
            trigger_name: None,
            triggered_by: Actor::system(SystemComponent::Workflow).display(),
            trigger_reason: "user action".to_owned(),
            hook_results_json: None,
            rejection: false,
            created_at: entered_at,
        },
    )
    .await
    .unwrap();
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(serde_json::json!({"retry_budgets":{"review":budget}}).to_string())
        .bind(&task.id)
        .execute(db.pool())
        .await
        .unwrap();
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    let event_bus = Arc::new(EventBus::new(32));
    let service = Arc::new(TaskService::new(Arc::clone(&db), Arc::clone(&event_bus)));
    let dispatcher = TaskDispatcher::new(Arc::clone(&db), event_bus, service);
    FailedReviewFixture {
        db,
        task,
        dispatcher,
        _repo_dir: repo_dir,
    }
}

#[tokio::test]
async fn dispatcher_failed_review_recovers_after_grace_idempotently() {
    let fixture = failed_review_fixture(chrono::Duration::minutes(3), 3).await;
    assert_eq!(fixture.dispatcher.check_once().await.unwrap(), 1);
    let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.status, "in_progress");
    assert!(current.blocked_json.is_none());
    assert!(current.review_passed_at.is_none());
    let entries = TransitionLogRepo::list_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap();
    assert_eq!(
        crate::task_diagnostics::count_gate_rejections_since_boundary(&entries, "review"),
        1
    );
    // The remediation transition fences a duplicate delivery of this entry.
    assert!(!fixture
        .dispatcher
        .recover_failed_review(&fixture.task)
        .await
        .unwrap());
    let entries = TransitionLogRepo::list_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(
        ReviewRepo::list_by_task(&*fixture.db, &fixture.task.id)
            .await
            .unwrap()[0]
            .status,
        ReviewStatus::Failed
    );
}

#[tokio::test]
async fn dispatcher_failed_review_corrupt_details_routes_once() {
    for details in [
        "{corrupt",
        "true",
        r#"{"conformance":{"status":"unknown"}}"#,
    ] {
        let fixture = failed_review_fixture(chrono::Duration::minutes(3), 3).await;
        sqlx::query("UPDATE review SET step_results_json = ? WHERE task_id = ?")
            .bind(details)
            .bind(&fixture.task.id)
            .execute(fixture.db.pool())
            .await
            .unwrap();
        let review = ReviewRepo::list_by_task(&*fixture.db, &fixture.task.id)
            .await
            .unwrap()
            .remove(0);
        let execution = ExecutionRepo::get_by_id(&*fixture.db, &review.execution_id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            fixture
                .dispatcher
                .task_service
                .reconcile_settled_reviewer_completion(&fixture.task, &execution, &review, true)
                .await
                .is_err(),
            "live completion remains strict"
        );
        assert!(fixture
            .dispatcher
            .recover_failed_review(&fixture.task)
            .await
            .unwrap());
        let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.status, "in_progress");
        assert!(!fixture
            .dispatcher
            .recover_failed_review(&current)
            .await
            .unwrap());
        assert_eq!(
            TaskRepo::get_by_id(&*fixture.db, &current.id, false)
                .await
                .unwrap()
                .unwrap()
                .version,
            current.version
        );
    }
}

#[tokio::test]
async fn dispatcher_failed_review_composes_finding_routing_with_ci_budget() {
    for (owner, repeat, failed_check, expected_rejections) in [
        (true, false, false, 0),
        (false, true, false, 0),
        (true, true, true, 1),
    ] {
        let fixture = failed_review_fixture(chrono::Duration::minutes(3), 3).await;
        let review = ReviewRepo::list_by_task(&*fixture.db, &fixture.task.id)
            .await
            .unwrap()
            .remove(0);
        let reason = "needs an owner decision";
        let checks = if failed_check {
            serde_json::json!([{"check_id":"ci", "command":"false", "exit_code":1, "output":"CI failed"}])
        } else {
            serde_json::json!([])
        };
        let details = serde_json::json!({
            "conformance": {
                "status":"failed", "contract":null, "checks":checks,
                "reason": if failed_check { "CI failed" } else { reason },
                "assessment": {"result":"fail", "reason":reason,
                    "fixable_by":if owner { "owner" } else { "coder" }, "repeat":repeat}
            }
        });
        if repeat {
            sqlx::query("UPDATE review SET attempt_number = 2 WHERE id = ?")
                .bind(&review.id)
                .execute(fixture.db.pool())
                .await
                .unwrap();
            ReviewRepo::create(
                &*fixture.db,
                db::CreateReview {
                    id: new_uuid_v4(),
                    task_id: fixture.task.id.clone(),
                    execution_id: review.execution_id.clone(),
                    attempt_number: 1,
                    status: ReviewStatus::Failed,
                    step_results_json: details.to_string(),
                    started_at: review.started_at.clone(),
                    created_at: review.created_at.clone(),
                    updated_at: review.updated_at.clone(),
                },
            )
            .await
            .unwrap();
        }
        sqlx::query("UPDATE review SET step_results_json = ? WHERE id = ?")
            .bind(details.to_string())
            .bind(&review.id)
            .execute(fixture.db.pool())
            .await
            .unwrap();
        assert!(fixture
            .dispatcher
            .recover_failed_review(&fixture.task)
            .await
            .unwrap());
        let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
            .await
            .unwrap()
            .unwrap();
        if failed_check {
            assert_eq!(current.status, "in_progress");
            assert!(current.blocked_json.is_none());
        } else {
            assert_eq!(current.status, "review");
            let annotation: serde_json::Value =
                serde_json::from_str(current.error_annotation.as_deref().unwrap()).unwrap();
            assert_eq!(annotation["type"], "review_needs_owner");
            assert!(
                ExecutionRepo::list_running_by_task(&*fixture.db, &current.id)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        let entries = TransitionLogRepo::list_by_task(&*fixture.db, &current.id)
            .await
            .unwrap();
        assert_eq!(
            crate::task_diagnostics::count_gate_rejections_since_boundary(&entries, "review"),
            expected_rejections
        );
        assert!(!fixture
            .dispatcher
            .recover_failed_review(&current)
            .await
            .unwrap());
    }
}

#[tokio::test]
async fn dispatcher_failed_review_records_exhausted_budget_once() {
    let fixture = failed_review_fixture(chrono::Duration::minutes(3), 1).await;
    assert_eq!(fixture.dispatcher.check_once().await.unwrap(), 1);
    let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.status, "review");
    let annotation: serde_json::Value =
        serde_json::from_str(current.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["type"], "review_budget_exhausted");
    assert!(!fixture
        .dispatcher
        .recover_failed_review(&current)
        .await
        .unwrap());
    let unchanged = TaskRepo::get_by_id(&*fixture.db, &current.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unchanged.version, current.version);
}

#[tokio::test]
async fn dispatcher_failed_review_respects_grace_and_recovery_fences() {
    for fence in [
        "grace",
        "annotation",
        "blocked",
        "running",
        "awaiting_human",
        "user",
        "barrier",
        "cascade",
        "old_review",
        "new_review",
    ] {
        let age = if fence == "grace" {
            chrono::Duration::seconds(30)
        } else {
            chrono::Duration::minutes(3)
        };
        let fixture = failed_review_fixture(age, 3).await;
        let cascade_slot = if fence == "cascade" {
            fixture
                .dispatcher
                .task_service
                .claim_completion_cascade(&fixture.task.id)
        } else {
            None
        };
        match fence {
            "annotation" => {
                sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
                    .bind(r#"{"type":"review_blocked","message":"owner action required"}"#)
                    .bind(&fixture.task.id)
                    .execute(fixture.db.pool())
                    .await
                    .unwrap();
            }
            "blocked" => {
                sqlx::query("UPDATE task SET blocked_json = '{}' WHERE id = ?")
                    .bind(&fixture.task.id)
                    .execute(fixture.db.pool())
                    .await
                    .unwrap();
            }
            "running" => {
                let agent =
                    seed_agent(&fixture.db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
                seed_running_execution(&fixture.db, &fixture.task.id, &agent, "coder").await;
            }
            "awaiting_human" => {
                sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
                    .bind(r#"{"awaiting_human":true}"#)
                    .bind(&fixture.task.id)
                    .execute(fixture.db.pool())
                    .await
                    .unwrap();
            }
            "user" => {
                sqlx::query("UPDATE transition_log SET triggered_by = 'user:override:api' WHERE task_id = ?")
                .bind(&fixture.task.id).execute(fixture.db.pool()).await.unwrap();
            }
            "barrier" => {
                sqlx::query("UPDATE task SET entry_barrier_json = ? WHERE id = ?")
                    .bind(serde_json::json!({"state":"review","status":"running","retry_started_at":now_rfc3339()}).to_string())
                    .bind(&fixture.task.id)
                    .execute(fixture.db.pool())
                    .await
                    .unwrap();
            }
            "old_review" => {
                sqlx::query("UPDATE transition_log SET created_at = ? WHERE task_id = ?")
                    .bind(now_rfc3339())
                    .bind(&fixture.task.id)
                    .execute(fixture.db.pool())
                    .await
                    .unwrap();
            }
            "new_review" => {
                let candidate = seed_completed_coder_execution(&fixture.db, &fixture.task.id).await;
                let now = now_rfc3339();
                ReviewRepo::create(
                    &*fixture.db,
                    db::CreateReview {
                        id: new_uuid_v4(),
                        task_id: fixture.task.id.clone(),
                        execution_id: candidate,
                        attempt_number: 2,
                        status: ReviewStatus::Running,
                        step_results_json: r#"{"ci_steps":[]}"#.to_owned(),
                        started_at: now.clone(),
                        created_at: now.clone(),
                        updated_at: now,
                    },
                )
                .await
                .unwrap();
            }
            _ => {}
        }
        let task = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert!(
            !fixture
                .dispatcher
                .recover_failed_review(&task)
                .await
                .unwrap(),
            "{fence}"
        );
        let current = TaskRepo::get_by_id(&*fixture.db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.version, task.version, "{fence}");
        assert_eq!(current.status, "review", "{fence}");
        drop(cascade_slot);
    }
}

#[tokio::test]
async fn dispatcher_failed_review_clears_abandoned_running_entry_barrier() {
    let fixture = failed_review_fixture(chrono::Duration::minutes(3), 3).await;
    sqlx::query("UPDATE task SET entry_barrier_json = ? WHERE id = ?")
        .bind(
            serde_json::json!({"state":"review", "status":"running",
            "started_at":(chrono::Utc::now() - chrono::Duration::minutes(4)).to_rfc3339()})
            .to_string(),
        )
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    assert_eq!(fixture.dispatcher.check_once().await.unwrap(), 1);
    let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.status, "in_progress");
    assert!(current.entry_barrier_json.is_none());
}

#[tokio::test]
async fn dispatcher_failed_review_rejects_stale_task_version() {
    for budget in [1, 3] {
        let fixture = failed_review_fixture(chrono::Duration::minutes(3), budget).await;
        sqlx::query("UPDATE task SET version = version + 1 WHERE id = ?")
            .bind(&fixture.task.id)
            .execute(fixture.db.pool())
            .await
            .unwrap();
        let error = fixture
            .dispatcher
            .recover_failed_review(&fixture.task)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ServiceError::Db(db::DbError::VersionConflict)
        ));
        let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.status, "review");
        assert!(current.blocked_json.is_none());
    }
}

async fn merge_gate_fixture(entered_ago: chrono::Duration) -> MergeGateFixture {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let task = seed_task(&db, &project_id, "landed merge", "merging", 1).await;
    let worktree_path = workspace_dir.path().join("worktree");
    let branch = workspace::task_branch_name(&task.id);
    git::create_worktree(repo_dir.path(), &branch, &worktree_path)
        .await
        .expect("task worktree creates");
    std::fs::write(worktree_path.join("feature.txt"), "landed feature\n").unwrap();
    run_git(&worktree_path, &["add", "-A"]);
    run_git(&worktree_path, &["commit", "-m", "candidate"]);
    run_git(repo_dir.path(), &["merge", "--ff-only", &branch]);
    // The target has moved beyond the landed candidate, not merely to it.
    std::fs::write(repo_dir.path().join("sibling.txt"), "sibling feature\n").unwrap();
    run_git(repo_dir.path(), &["add", "-A"]);
    run_git(repo_dir.path(), &["commit", "-m", "sibling landed"]);
    let now = now_rfc3339();
    let workspace = WorkspaceRepo::create(
        &*db,
        CreateWorkspace {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            repo_id,
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch,
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("workspace creates");
    let execution_id = seed_completed_coder_execution(&db, &task.id).await;
    sqlx::query("UPDATE execution SET workspace_id = ? WHERE id = ?")
        .bind(workspace.id)
        .bind(execution_id)
        .execute(db.pool())
        .await
        .unwrap();
    TransitionLogRepo::insert(
        &*db,
        db::CreateTransitionLog {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            from_state: "review".to_owned(),
            to_state: "merging".to_owned(),
            trigger_name: Some("accept".to_owned()),
            triggered_by: api_types::Actor::system(api_types::SystemComponent::Workflow).display(),
            trigger_reason: "review passed".to_owned(),
            hook_results_json: None,
            rejection: false,
            created_at: (chrono::Utc::now() - entered_ago).to_rfc3339(),
        },
    )
    .await
    .expect("merge entry records");
    let event_bus = Arc::new(EventBus::new(32));
    let merge_service = Arc::new(crate::merge_service::MergeService::new_for_test(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        workspace_dir.path().to_owned(),
    ));
    let task_service = Arc::new(
        TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
            .with_merge_service(Arc::clone(&merge_service)),
    );
    let dispatcher = TaskDispatcher::new(Arc::clone(&db), event_bus, task_service);
    MergeGateFixture {
        db,
        _repo_dir: repo_dir,
        _workspace_dir: workspace_dir,
        task,
        merge_service,
        dispatcher,
    }
}

async fn assert_merge_gate_untouched(fixture: &MergeGateFixture) {
    assert_eq!(fixture.dispatcher.check_once().await.unwrap(), 0);
    let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.status, "merging");
    assert_eq!(current.version, fixture.task.version);
    assert_eq!(
        TransitionLogRepo::list_by_task(&*fixture.db, &current.id)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn dispatcher_merge_gate_completes_already_merged_branch() {
    let fixture = merge_gate_fixture(chrono::Duration::minutes(3)).await;
    assert_eq!(fixture.dispatcher.check_once().await.unwrap(), 1);
    let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.status, "done");
    let logs = TransitionLogRepo::list_by_task(&*fixture.db, &current.id)
        .await
        .unwrap();
    assert_eq!(logs.len(), 3);
    assert_eq!(logs[1].from_state, "merging");
    assert_eq!(logs[1].to_state, "merging");
    assert_eq!(logs[2].to_state, "done");
    assert_eq!(logs[2].trigger_reason, "merge succeeded");
    assert_eq!(fixture.dispatcher.check_once().await.unwrap(), 0);
}

#[tokio::test]
async fn pull_request_merge_wait_requires_human_retry_before_direct_merge() {
    let fixture = merge_gate_fixture(chrono::Duration::minutes(3)).await;
    run_git(fixture._repo_dir.path(), &["reset", "--hard", "HEAD~2"]);
    let candidate_sha = run_git(
        fixture._workspace_dir.path().join("worktree").as_path(),
        &["rev-parse", "HEAD"],
    );
    assert_ne!(
        run_git(fixture._repo_dir.path(), &["rev-parse", "HEAD"]),
        candidate_sha
    );
    sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
        .bind(
            serde_json::json!({
                "awaiting_human": true,
                "awaiting_human_reason": "pull_request_merge",
                "awaiting_human_marker_id": "legacy-pr-marker",
            })
            .to_string(),
        )
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();

    assert_eq!(fixture.dispatcher.check_once().await.unwrap(), 0);
    let parked = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(parked.status, "merging");
    assert_eq!(
        fixture
            .dispatcher
            .task_service
            .available_recovery_actions(&parked.id)
            .await
            .unwrap(),
        vec![api_types::RecoveryAction::RetryHook]
    );

    let merged = fixture
        .dispatcher
        .task_service
        .recover_task(
            &parked.id,
            api_types::RecoveryAction::RetryHook,
            Some("operator approved direct merge".to_owned()),
            None,
        )
        .await
        .expect("human merge retry integrates the reviewed candidate");
    assert_eq!(merged.status, "done");
    assert_eq!(
        run_git(fixture._repo_dir.path(), &["rev-parse", "HEAD"]),
        candidate_sha
    );
    let metadata = db::TaskMetadata::parse(merged.metadata_json.as_deref()).unwrap();
    assert!(metadata.extra.get("awaiting_human").is_none());
    assert!(metadata.extra.get("awaiting_human_reason").is_none());
    assert!(metadata.extra.get("awaiting_human_marker_id").is_none());
}

#[tokio::test]
async fn dispatcher_merge_gate_respects_entry_grace() {
    let fixture = merge_gate_fixture(chrono::Duration::seconds(30)).await;
    assert_merge_gate_untouched(&fixture).await;
}

#[tokio::test]
async fn dispatcher_merge_gate_skips_blocked_task() {
    let mut fixture = merge_gate_fixture(chrono::Duration::minutes(3)).await;
    sqlx::query("UPDATE task SET error_annotation = ?, version = version + 1 WHERE id = ?")
        .bind(r#"{"type":"manual_stop","message":"integration paused by user"}"#)
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    fixture.task = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_merge_gate_untouched(&fixture).await;
}

#[tokio::test]
async fn dispatcher_merge_gate_skips_blocking_record() {
    let mut fixture = merge_gate_fixture(chrono::Duration::minutes(3)).await;
    sqlx::query("UPDATE task SET blocked_json = ?, version = version + 1 WHERE id = ?")
        .bind(r#"{"kind":"merge_conflict","reason":"manual repair required"}"#)
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    fixture.task = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_merge_gate_untouched(&fixture).await;
}

#[tokio::test]
async fn dispatcher_merge_gate_skips_running_execution() {
    let fixture = merge_gate_fixture(chrono::Duration::minutes(3)).await;
    let agent_id = seed_agent(&fixture.db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    seed_running_execution(&fixture.db, &fixture.task.id, &agent_id, "coder").await;
    assert_merge_gate_untouched(&fixture).await;
}

#[tokio::test]
async fn dispatcher_merge_gate_skips_running_merge_hook() {
    let fixture = merge_gate_fixture(chrono::Duration::minutes(3)).await;
    let _slot = fixture
        .merge_service
        .claim_merge_hook(&fixture.task.id)
        .unwrap();
    assert_merge_gate_untouched(&fixture).await;
}

#[tokio::test]
async fn dispatcher_merge_gate_skips_running_completion_cascade() {
    let fixture = merge_gate_fixture(chrono::Duration::minutes(3)).await;
    let _slot = fixture
        .dispatcher
        .task_service
        .claim_completion_cascade(&fixture.task.id)
        .unwrap();
    assert_merge_gate_untouched(&fixture).await;
}

#[tokio::test]
async fn dispatcher_merge_gate_reentry_rejects_stale_task_version() {
    let fixture = merge_gate_fixture(chrono::Duration::minutes(3)).await;
    sqlx::query("UPDATE task SET version = version + 1 WHERE id = ?")
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let project = ProjectRepo::get_by_id(&*fixture.db, &fixture.task.project_id)
        .await
        .unwrap()
        .unwrap();
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let error = fixture
        .dispatcher
        .task_service
        .retry_merge_state_entry(
            &fixture.task,
            &project,
            &workflow,
            "recover interrupted merge gate",
        )
        .await
        .expect_err("stale recovery cannot run entry hooks");
    assert!(matches!(
        error,
        crate::ServiceError::Db(db::DbError::VersionConflict)
    ));
    let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.status, "merging");
    assert_eq!(current.version, fixture.task.version + 1);
    assert_eq!(
        TransitionLogRepo::list_by_task(&*fixture.db, &current.id)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn dispatcher_does_not_relaunch_planner_awaiting_plan_review() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    set_planning_gate_user_approval(&db, &project_id, true).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(
        &db,
        &project_id,
        "planned",
        crate::workflow::default_states::PLANNING,
        1,
    )
    .await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::PLANNER,
        &agent_id,
    )
    .await;

    let task_workspace_dir = workspace_dir.path().join(&task.id);
    let worktree_path = task_workspace_dir.join("forge");
    std::fs::create_dir_all(&worktree_path).expect("worktree creates");
    let now = now_rfc3339();
    let workspace = WorkspaceRepo::create(
        &*db,
        CreateWorkspace {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            repo_id,
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch: ::workspace::task_branch_name(&task.id),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("workspace creates");
    std::fs::write(
        task_workspace_dir.join("plan.md"),
        "- [ ] implement the planned change\n",
    )
    .expect("plan artifact writes");
    let execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: crate::workflow::default_roles::PLANNER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("plan ready for review".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                serde_json::json!({
                    "executor_type": "shell",
                    "config": {},
                    "task_state": crate::workflow::default_states::PLANNING,
                    "state_entry_token": null,
                })
                .to_string(),
            ),
            workspace_id: Some(workspace.id),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("completed planner execution creates");
    sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
        .bind(
            serde_json::json!({
                "awaiting_human": true,
                "awaiting_human_reason": "plan_review",
                "planning_completed_at": now,
                "planning_execution_id": execution.id,
                "planning_state_entry_token": null,
                "awaiting_human_marker_id": new_uuid_v4(),
            })
            .to_string(),
        )
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("metadata update");
    let waiting = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let first_scan = dispatcher.check_once().await.expect("dispatcher runs");
    let second_scan = dispatcher
        .check_once()
        .await
        .expect("dispatcher runs again");

    assert_eq!((first_scan, second_scan), (0, 0));
    assert!(rx.try_recv().is_err());
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::PLANNER
        )
        .await
        .expect("execution count loads"),
        1
    );
    let still_waiting = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert_eq!(
        still_waiting.status,
        crate::workflow::default_states::PLANNING
    );
    assert_eq!(still_waiting.version, waiting.version);
    let metadata: serde_json::Value = serde_json::from_str(
        still_waiting
            .metadata_json
            .as_deref()
            .expect("waiting metadata exists"),
    )
    .expect("waiting metadata parses");
    assert_eq!(
        metadata
            .get("planning_execution_id")
            .and_then(serde_json::Value::as_str),
        Some(execution.id.as_str())
    );
    assert_eq!(
        metadata
            .get("awaiting_human")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
}

#[tokio::test]
async fn dispatcher_clears_plan_review_wait_from_an_older_planning_entry() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    set_planning_gate_user_approval(&db, &project_id, true).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(
        &db,
        &project_id,
        "re-entered planning",
        crate::workflow::default_states::PLANNING,
        1,
    )
    .await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::PLANNER,
        &agent_id,
    )
    .await;

    let old_entry_id = new_uuid_v4();
    TransitionLogRepo::insert(
        &*db,
        db::CreateTransitionLog {
            id: old_entry_id.clone(),
            task_id: task.id.clone(),
            from_state: crate::workflow::default_states::BACKLOG.to_owned(),
            to_state: crate::workflow::default_states::PLANNING.to_owned(),
            trigger_name: Some("plan".to_owned()),
            triggered_by: api_types::Actor::system(api_types::SystemComponent::Workflow).display(),
            trigger_reason: "first planning entry".to_owned(),
            hook_results_json: None,
            rejection: false,
            created_at: "2026-01-01T00:00:00+00:00".to_owned(),
        },
    )
    .await
    .expect("old planning entry records");
    let execution_id = new_uuid_v4();
    ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: execution_id.clone(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: crate::workflow::default_roles::PLANNER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: None,
            stopped_at: Some("2026-01-01T00:00:01+00:00".to_owned()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("old plan ready".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                serde_json::json!({
                    "executor_type": "shell",
                    "config": {},
                    "task_state": crate::workflow::default_states::PLANNING,
                    "state_entry_token": old_entry_id.clone(),
                })
                .to_string(),
            ),
            workspace_id: None,
            created_at: "2026-01-01T00:00:01+00:00".to_owned(),
            updated_at: "2026-01-01T00:00:01+00:00".to_owned(),
        },
    )
    .await
    .expect("old planner execution creates");
    sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
        .bind(
            serde_json::json!({
                "awaiting_human": true,
                "awaiting_human_reason": "plan_review",
                "planning_completed_at": "2026-01-01T00:00:01+00:00",
                "planning_execution_id": execution_id,
                "planning_state_entry_token": old_entry_id,
                "awaiting_human_marker_id": new_uuid_v4(),
            })
            .to_string(),
        )
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("old wait metadata writes");
    TransitionLogRepo::insert(
        &*db,
        db::CreateTransitionLog {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            from_state: crate::workflow::default_states::IN_PROGRESS.to_owned(),
            to_state: crate::workflow::default_states::PLANNING.to_owned(),
            trigger_name: Some("replan".to_owned()),
            triggered_by: api_types::Actor::system(api_types::SystemComponent::Workflow).display(),
            trigger_reason: "second planning entry".to_owned(),
            hook_results_json: None,
            rejection: false,
            created_at: "2026-01-01T00:00:02+00:00".to_owned(),
        },
    )
    .await
    .expect("new planning entry records");

    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    assert_eq!(dispatcher.check_once().await.expect("dispatcher runs"), 1);
    let replacement = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("replacement planner dispatches")
        .expect("replacement execution context arrives");
    assert_eq!(replacement.task_id, task.id);
    let recovered = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let metadata = recovered.metadata().expect("metadata parses");
    assert!(metadata.extra.get("awaiting_human").is_none());
    assert!(metadata.extra.get("planning_state_entry_token").is_none());
}

#[tokio::test]
async fn project_wide_sweep_cleans_plan_files_for_paused_removed_state() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let task = seed_task(&db, &project_id, "retired gate", "removed_gate", 1).await;
    let task_root = workspace_dir.path().join(&task.id);
    let worktree = task_root.join("forge");
    std::fs::create_dir_all(&worktree).expect("worktree creates");
    WorkspaceRepo::create(
        &*db,
        CreateWorkspace {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            repo_id,
            worktree_path: worktree.to_string_lossy().into_owned(),
            branch: ::workspace::task_branch_name(&task.id),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("workspace creates");
    let execution_id = new_uuid_v4();
    crate::plan_artifact::prepare_execution_plan_outbox(&worktree, &execution_id, false, None)
        .expect("outbox prepares");
    crate::plan_artifact::write_execution_outbox_plan(
        &worktree,
        &execution_id,
        "- [ ] remove private bytes\n",
    )
    .expect("candidate writes");
    let outbox = executors::existing_execution_outbox(&worktree, &execution_id)
        .expect("outbox resolves")
        .expect("prepared outbox exists");
    crate::plan_artifact::stage_execution_outbox_plan(&outbox, &worktree, &execution_id)
        .expect("candidate stages");
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    let marker = serde_json::json!({
        "execution_id": execution_id.clone(),
        "state": "removed_gate",
        "state_entry_token": null,
        "project_version": project.version,
    });
    sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
        .bind(
            serde_json::json!({
                "terminal_execution_settlement": marker.clone(),
                "plan_publication_cleanup": marker,
            })
            .to_string(),
        )
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("cleanup marker writes");
    sqlx::query("UPDATE project SET paused_at = ? WHERE id = ?")
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project pauses");

    let (dispatcher, _rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    let paused_project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project reloads")
        .expect("project exists");
    let stage_path = task_root
        .join(".forge-plan-staging")
        .join(format!("{execution_id}.md"));
    std::fs::remove_file(&stage_path).expect("staged file removes for failure injection");
    std::fs::create_dir(&stage_path).expect("stage-path directory injects cleanup failure");
    assert_eq!(
        dispatcher
            .reconcile_plan_publication_claims(&paused_project)
            .await
            .expect("failed private cleanup is contained"),
        0
    );
    let still_pending = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert!(still_pending
        .metadata()
        .expect("metadata parses")
        .extra
        .get("plan_publication_cleanup")
        .is_some());
    std::fs::remove_dir(&stage_path).expect("failure injection removes");
    assert_eq!(
        dispatcher
            .reconcile_plan_publication_claims(&paused_project)
            .await
            .expect("publication cleanup reconciles"),
        1
    );
    assert!(!stage_path.exists());
    assert!(!executors::execution_outbox_path(&worktree, &execution_id)
        .expect("outbox path is contained")
        .exists());
    let cleaned = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let metadata = cleaned.metadata().expect("metadata parses");
    assert!(metadata.extra.get("plan_publication_cleanup").is_none());
    assert!(metadata
        .extra
        .get("terminal_execution_settlement")
        .is_some());
    assert_eq!(
        dispatcher
            .reconcile_plan_publication_claims(&paused_project)
            .await
            .expect("completed cleanup is quiescent"),
        0
    );
}

#[tokio::test]
async fn dispatcher_clears_stale_plan_review_wait_on_default_auto_approval_gate() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(
        &db,
        &project_id,
        "legacy planning wait",
        crate::workflow::default_states::PLANNING,
        1,
    )
    .await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::PLANNER,
        &agent_id,
    )
    .await;
    sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
        .bind(
            serde_json::json!({
                "awaiting_human": true,
                "awaiting_human_reason": "plan_review",
                "planning_completed_at": now_rfc3339(),
                "awaiting_human_marker_id": new_uuid_v4(),
            })
            .to_string(),
        )
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("legacy wait metadata writes");
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    assert_eq!(dispatcher.check_once().await.expect("dispatcher runs"), 1);
    let execution_ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("planner dispatches in time")
        .expect("planner execution context arrives");
    assert_eq!(execution_ctx.task_id, task.id);
    assert_eq!(
        executors::task_role(&execution_ctx.agent_config),
        Some(crate::workflow::default_roles::PLANNER)
    );
    let recovered = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let metadata: serde_json::Value = recovered
        .metadata_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .expect("task metadata parses")
        .unwrap_or_else(|| serde_json::json!({}));
    assert!(metadata.get("awaiting_human").is_none());
    assert!(metadata.get("awaiting_human_reason").is_none());
    assert!(metadata.get("planning_completed_at").is_none());
    assert!(metadata.get("awaiting_human_marker_id").is_none());
}

#[tokio::test]
async fn dispatcher_repeated_scans_do_not_churn_or_redispatch_completed_custom_gate() {
    const CUSTOM_GATE: &str = "manual_qa";

    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;

    let mut workflow = crate::workflow::default_workflow::default_workflow();
    let mut custom_gate = workflow
        .states
        .iter()
        .find(|state| state.name == crate::workflow::default_states::PLANNING)
        .expect("default planning gate exists")
        .clone();
    custom_gate.name = CUSTOM_GATE.to_owned();
    custom_gate.display_name = "Manual QA".to_owned();
    custom_gate.role = Some(crate::workflow::default_roles::CODER.to_owned());
    let gate_config = custom_gate
        .gate_config
        .as_mut()
        .expect("cloned gate has config");
    gate_config.requires_user_approval = Some(true);
    gate_config.optional_when_unassigned = Some(false);
    workflow.states.push(custom_gate);
    sqlx::query(
        "UPDATE project SET workflow_definition = ?, workflow_template_name = ?, updated_at = ? WHERE id = ?",
    )
    .bind(serde_json::to_string(&workflow).expect("workflow serializes"))
    .bind("custom")
    .bind(now_rfc3339())
    .bind(&project_id)
    .execute(db.pool())
    .await
    .expect("custom workflow updates");

    let task = seed_task(&db, &project_id, "manual QA", CUSTOM_GATE, 1).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let project_version = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("Project loads")
        .expect("Project exists")
        .version;
    let now = now_rfc3339();
    ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: crate::workflow::default_roles::CODER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("custom gate work complete".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                serde_json::json!({
                    "executor_type": "shell",
                    "config": {},
                    "project_version": project_version,
                    "task_state": CUSTOM_GATE,
                    "state_entry_token": null,
                })
                .to_string(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("completed custom gate execution creates");
    let before = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let first_scan = dispatcher.check_once().await.expect("dispatcher runs");
    let second_scan = dispatcher
        .check_once()
        .await
        .expect("dispatcher runs again");

    assert_eq!((first_scan, second_scan), (0, 0));
    assert!(rx.try_recv().is_err());
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
    let after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert_eq!(after.status, CUSTOM_GATE);
    assert_eq!(after.version, before.version);
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
async fn recovery_on_full_agent_queues_and_dispatches_after_capacity_frees() {
    for (action, bind_execution) in [
        (api_types::RecoveryAction::Reexecute, false),
        (api_types::RecoveryAction::Reexecute, true),
        (api_types::RecoveryAction::ResumeSession, true),
        (api_types::RecoveryAction::OpenInteractive, true),
    ] {
        let db = Arc::new(sqlite_db().await);
        let repo_dir = TempDir::new().expect("repo dir creates");
        let workspace_dir = TempDir::new().expect("workspace dir creates");
        let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
        let agent_id =
            seed_agent_with_executor(&db, 1, DaemonStatus::Online, AgentStatus::Idle, "codex")
                .await;
        let busy = seed_task(&db, &project_id, "occupies capacity", "in_progress", 0).await;
        assign_role(&db, &busy.id, "coder", &agent_id).await;
        seed_running_execution(&db, &busy.id, &agent_id, "coder").await;
        crate::test_support::set_test_agent_capacity(&db, &agent_id, 1).await;

        let task = seed_task(&db, &project_id, "recover", "in_progress", 0).await;
        assign_role(&db, &task.id, "coder", &agent_id).await;
        let stopped = seed_cancelled_execution(
            &db,
            &task.id,
            &agent_id,
            "coder",
            Some(StopReason::UserCancelled),
            Some(ResumePolicy::Manual),
        )
        .await;
        sqlx::query("UPDATE execution SET agent_session_id = ? WHERE id = ?")
            .bind("recovery-session")
            .bind(&stopped.id)
            .execute(db.pool())
            .await
            .expect("stopped session persists");
        let annotation = serde_json::json!({
            "type": "recovery_required",
            "blocking_reason": "crash_recovery",
            "blocked_execution_id": bind_execution.then_some(&stopped.id),
            "recovery_actions": [action],
        });
        let task = TaskRepo::update(
            &*db,
            UpdateTask {
                id: task.id.clone(),
                expected_version: task.version,
                title: None,
                description: None,
                priority: None,
                merge_config: None,
                plan: None,
                error_annotation: Some(Some(annotation.to_string())),
                blocked_json: Some(Some(
                    serde_json::json!({
                        "kind": "recovery_required",
                        "reason": "crash_recovery",
                    })
                    .to_string(),
                )),
                failed_json: None,
                task_state_config: None,
                parent_task_id: None,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .expect("interruption persists");
        set_prompt_execution_snapshots(&db, &agent_id).await;
        let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
        let queued = dispatcher
            .task_service
            .recover_task(
                &task.id,
                action,
                Some("operator retry".to_owned()),
                Some("keep this recovery guidance".to_owned()),
            )
            .await
            .expect("capacity-only recovery is accepted");
        assert!(queued.version > task.version);
        assert!(queued.error_annotation.is_none());
        assert!(queued.blocked_json.is_none());
        let intent = deferred_dispatch::queued_recovery(&queued).expect("intent persists");
        let deferred_dispatch::QueuedRecoveryRequest::Recover(request) = &intent.request else {
            panic!("recovery request expected")
        };
        assert_eq!(request.action, action);
        assert_eq!(request.reason.as_deref(), Some("operator retry"));
        assert_eq!(
            request.context.as_deref(),
            Some("keep this recovery guidance")
        );
        let assignments = TaskRoleAssignmentRepo::list_by_task(&*db, &task.id)
            .await
            .expect("assignments load");
        let health = crate::task_diagnostics::derive_workflow_health(
            &queued,
            &crate::workflow::default_workflow::default_workflow(),
            &assignments,
            None,
            None,
            false,
            None,
        );
        assert_eq!(health.kind, api_types::WorkflowHealthKind::WaitingForAgent);
        assert_eq!(health.label, "Retry Queued");
        assert_eq!(dispatcher.check_once().await.expect("full scan runs"), 0);
        assert!(rx.try_recv().is_err());
        assert!(ExecutionRepo::list_running_by_task(&*db, &task.id)
            .await
            .expect("executions load")
            .is_empty());

        sqlx::query(
            "UPDATE execution SET status = 'cancelled', resume_policy = 'manual' WHERE task_id = ?",
        )
        .bind(&busy.id)
        .execute(db.pool())
        .await
        .expect("capacity frees");
        // A new dispatcher proves the accepted intent survives runtime restart.
        let (restarted, mut restarted_rx) =
            build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
        assert_eq!(restarted.check_once().await.expect("recovery scan runs"), 1);
        let ctx = tokio::time::timeout(Duration::from_secs(30), restarted_rx.recv())
            .await
            .expect("recovery dispatch completes")
            .expect("execution context arrives");
        assert_eq!(ctx.task_id, task.id);
        assert!(ctx.description.contains("keep this recovery guidance"));
        let execution = ExecutionRepo::get_by_id(&*db, &ctx.execution_id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        assert!(execution
            .prompt
            .as_deref()
            .unwrap_or_default()
            .contains("keep this recovery guidance"));
        if action == api_types::RecoveryAction::ResumeSession {
            assert_eq!(
                execution.agent_session_id.as_deref(),
                Some("recovery-session")
            );
            assert_eq!(
                execution.parent_execution_id.as_deref(),
                Some(stopped.id.as_str())
            );
        }
        let current = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        assert!(deferred_dispatch::queued_recovery(&current).is_none());
        assert!(deferred_dispatch::pending_until(&current).is_none());
        assert_eq!(restarted.check_once().await.expect("replay scan runs"), 0);
        assert!(restarted_rx.try_recv().is_err());
    }
}

async fn seed_capacity_recovery(
    db: &Arc<db::SqliteDb>,
    dispatcher: &TaskDispatcher,
    project_id: &str,
    action: api_types::RecoveryAction,
) -> (db::Task, String, String) {
    let agent_id = seed_agent(db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let busy = seed_task(db, project_id, "occupies capacity", "in_progress", 0).await;
    seed_running_execution(db, &busy.id, &agent_id, "coder").await;
    crate::test_support::set_test_agent_capacity(db, &agent_id, 1).await;
    let task = seed_task(db, project_id, "recover", "in_progress", 0).await;
    assign_role(db, &task.id, "coder", &agent_id).await;
    let blocked_execution_id = if action == api_types::RecoveryAction::ResumeSession {
        let stopped = seed_cancelled_execution(
            db,
            &task.id,
            &agent_id,
            "coder",
            Some(StopReason::UserCancelled),
            Some(ResumePolicy::Manual),
        )
        .await;
        sqlx::query("UPDATE execution SET agent_session_id = 'recovery-session' WHERE id = ?")
            .bind(&stopped.id)
            .execute(db.pool())
            .await
            .unwrap();
        Some(stopped.id)
    } else {
        None
    };
    sqlx::query("UPDATE task SET error_annotation = ?, blocked_json = ?, version = version + 1 WHERE id = ?")
        .bind(serde_json::json!({
            "type": "recovery_required", "blocking_reason": "crash_recovery",
            "blocked_by": "original-owner", "blocked_execution_id": blocked_execution_id, "recovery_actions": [action],
        }).to_string())
        .bind(serde_json::json!({ "kind": "recovery_required", "reason": "crash_recovery", "blocked_by": "original-owner" }).to_string())
        .bind(&task.id).execute(db.pool()).await.unwrap();
    let queued = dispatcher
        .task_service
        .recover_task(&task.id, action, None, None)
        .await
        .expect("recovery queues");
    (queued, agent_id, busy.id)
}

#[tokio::test]
async fn recovery_on_full_agent_restores_blocker_on_permanent_replay_error() {
    for failure in [
        "deleted",
        "unassigned",
        "interactive",
        "paused",
        "offline",
        "unrelated",
        "unrelated_moved",
        "session",
        "unassigned_session",
    ] {
        let db = Arc::new(sqlite_db().await);
        let repo_dir = TempDir::new().unwrap();
        let workspace_dir = TempDir::new().unwrap();
        let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
        let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
        let action = match failure {
            "interactive" => api_types::RecoveryAction::OpenInteractive,
            "session" | "unassigned_session" => api_types::RecoveryAction::ResumeSession,
            _ => api_types::RecoveryAction::Reexecute,
        };
        let (queued, agent_id, busy_id) =
            seed_capacity_recovery(&db, &dispatcher, &project_id, action).await;
        match failure {
            "deleted" => {
                AgentRepo::archive(&*db, &agent_id, &now_rfc3339())
                    .await
                    .unwrap();
            }
            "unassigned" | "interactive" | "unassigned_session" => {
                sqlx::query("DELETE FROM task_role_assignment WHERE task_id = ?")
                    .bind(&queued.id)
                    .execute(db.pool())
                    .await
                    .unwrap();
            }
            "paused" => {
                sqlx::query(
                    "UPDATE agent_identity SET paused = 1, version = version + 1 WHERE id = ?",
                )
                .bind(&agent_id)
                .execute(db.pool())
                .await
                .unwrap();
            }
            "offline" => {
                let agent = AgentRepo::get_by_id(&*db, &agent_id)
                    .await
                    .unwrap()
                    .unwrap();
                sqlx::query("UPDATE daemon SET status = 'offline' WHERE id = ?")
                    .bind(agent.daemon_id)
                    .execute(db.pool())
                    .await
                    .unwrap();
            }
            "unrelated" | "unrelated_moved" => {
                let other_agent = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
                seed_running_execution(&db, &queued.id, &other_agent, "interactive").await;
                let current = TaskRepo::get_by_id(&*db, &queued.id, false)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    current.metadata_json, queued.metadata_json,
                    "unrelated admission preserves intent"
                );
                if failure == "unrelated_moved" {
                    sqlx::query(
                        "UPDATE task SET status = 'planning', version = version + 1 WHERE id = ?",
                    )
                    .bind(&queued.id)
                    .execute(db.pool())
                    .await
                    .unwrap();
                }
            }
            "session" => {
                sqlx::query("UPDATE execution SET agent_session_id = NULL WHERE task_id = ?")
                    .bind(&queued.id)
                    .execute(db.pool())
                    .await
                    .unwrap();
                sqlx::query("UPDATE execution SET status = 'cancelled', resume_policy = 'manual' WHERE task_id = ?")
                    .bind(&busy_id).execute(db.pool()).await.unwrap();
            }
            _ => unreachable!(),
        }
        let latest = TaskRepo::get_by_id(&*db, &queued.id, false)
            .await
            .unwrap()
            .unwrap();
        let error = dispatcher
            .task_service
            .dispatch_queued_recovery(&latest)
            .await
            .expect_err("permanent replay refusal is reported once");
        let restored = TaskRepo::get_by_id(&*db, &queued.id, false)
            .await
            .unwrap()
            .unwrap();
        assert!(restored.version > queued.version);
        assert!(
            deferred_dispatch::queued_recovery(&restored).is_none(),
            "{failure}"
        );
        assert!(deferred_dispatch::pending_until(&restored).is_none());
        let annotation: serde_json::Value =
            serde_json::from_str(restored.error_annotation.as_deref().unwrap()).unwrap();
        assert_eq!(annotation["blocked_by"], "original-owner");
        assert_eq!(annotation["blocking_reason"], error.to_string());
        let blocked: serde_json::Value =
            serde_json::from_str(restored.blocked_json.as_deref().unwrap()).unwrap();
        assert_eq!(blocked["blocked_by"], "original-owner");
        assert_eq!(blocked["reason"], error.to_string());
        assert!(dispatcher
            .task_service
            .available_recovery_actions(&queued.id)
            .await
            .unwrap()
            .contains(&action));
        assert!(!dispatcher
            .task_service
            .dispatch_queued_recovery(&restored)
            .await
            .unwrap());
        assert!(rx.try_recv().is_err());
        if failure == "unassigned" {
            assign_role(&db, &queued.id, "coder", &agent_id).await;
            let requeued = dispatcher
                .task_service
                .recover_task(&queued.id, action, None, None)
                .await
                .expect("restored blocker can be recovered after reassignment");
            assert!(deferred_dispatch::queued_recovery(&requeued).is_some());
        }
    }
}

#[tokio::test]
async fn recovery_on_full_agent_resume_fallback_queues_and_replays() {
    for previous_session in [None, Some(false), Some(true)] {
        let db = Arc::new(sqlite_db().await);
        let repo_dir = TempDir::new().unwrap();
        let workspace_dir = TempDir::new().unwrap();
        let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
        let agent_id =
            seed_agent_with_executor(&db, 1, DaemonStatus::Online, AgentStatus::Idle, "codex")
                .await;
        let busy = seed_task(&db, &project_id, "busy", "in_progress", 0).await;
        seed_running_execution(&db, &busy.id, &agent_id, "coder").await;
        crate::test_support::set_test_agent_capacity(&db, &agent_id, 1).await;
        let task = seed_task(&db, &project_id, "resume", "in_progress", 0).await;
        assign_role(&db, &task.id, "coder", &agent_id).await;
        let stopped = if let Some(session) = previous_session {
            let stopped = seed_cancelled_execution(
                &db,
                &task.id,
                &agent_id,
                "coder",
                Some(StopReason::UserCancelled),
                Some(ResumePolicy::Manual),
            )
            .await;
            if session {
                sqlx::query(
                    "UPDATE execution SET agent_session_id = 'resume-session' WHERE id = ?",
                )
                .bind(&stopped.id)
                .execute(db.pool())
                .await
                .unwrap();
            }
            Some(stopped)
        } else {
            None
        };
        let current = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        set_prompt_execution_snapshots(&db, &agent_id).await;
        let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
        let queued = dispatcher
            .task_service
            .perform_task_action(
                &task.id,
                api_types::TaskAction::Resume,
                Some("resume guidance".to_owned()),
                Some(current.version),
            )
            .await
            .expect("Resume queues at capacity")
            .task;
        assert!(deferred_dispatch::queued_recovery(&queued).is_some());
        ProjectRepo::set_paused_at(&*db, &project_id, Some(now_rfc3339()))
            .await
            .unwrap();
        let paused_task = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        let repeated = dispatcher
            .task_service
            .perform_task_action(
                &task.id,
                api_types::TaskAction::Resume,
                Some("resume guidance".to_owned()),
                Some(paused_task.version),
            )
            .await
            .expect("repeating a queued Resume is a no-op while the Project is paused")
            .task;
        assert_eq!(repeated, paused_task);
        ProjectRepo::set_paused_at(&*db, &project_id, None)
            .await
            .unwrap();
        let queued = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert!(ExecutionRepo::list_running_by_task(&*db, &task.id)
            .await
            .unwrap()
            .is_empty());
        assert!(!dispatcher
            .task_service
            .dispatch_queued_recovery(&queued)
            .await
            .unwrap());
        sqlx::query(
            "UPDATE execution SET status = 'cancelled', resume_policy = 'manual' WHERE task_id = ?",
        )
        .bind(&busy.id)
        .execute(db.pool())
        .await
        .unwrap();
        assert!(dispatcher
            .task_service
            .dispatch_queued_recovery(&queued)
            .await
            .unwrap());
        let ctx = tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .expect("recovery dispatch completes")
            .unwrap();
        assert!(ctx.description.contains("resume guidance"));
        let execution = ExecutionRepo::get_by_id(&*db, &ctx.execution_id)
            .await
            .unwrap()
            .unwrap();
        assert!(execution
            .prompt
            .as_deref()
            .unwrap_or_default()
            .contains("resume guidance"));
        if previous_session == Some(true) {
            assert_eq!(
                execution.parent_execution_id,
                stopped.map(|stopped| stopped.id)
            );
            let snapshot: serde_json::Value =
                serde_json::from_str(execution.executor_config_snapshot_json.as_deref().unwrap())
                    .unwrap();
            assert_eq!(
                snapshot["dispatch"]["execution_policy"],
                "resume_latest_target_role_thread"
            );
        }
        let current = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert!(deferred_dispatch::queued_recovery(&current).is_none());
        assert!(deferred_dispatch::pending_until(&current).is_none());
    }
}

#[tokio::test]
async fn recovery_on_full_agent_keeps_non_capacity_refusals() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let busy = seed_task(&db, &project_id, "occupies capacity", "in_progress", 0).await;
    seed_running_execution(&db, &busy.id, &agent_id, "coder").await;
    crate::test_support::set_test_agent_capacity(&db, &agent_id, 1).await;
    let task = seed_task(&db, &project_id, "recover", "in_progress", 0).await;
    assign_role(&db, &task.id, "coder", &agent_id).await;
    let stopped = seed_cancelled_execution(
        &db,
        &task.id,
        &agent_id,
        "coder",
        Some(StopReason::UserCancelled),
        Some(ResumePolicy::Manual),
    )
    .await;
    let annotation = serde_json::json!({
        "type": "recovery_required",
        "blocking_reason": "crash_recovery",
        "blocked_execution_id": stopped.id,
        "recovery_actions": ["resume_session", "reexecute"],
    });
    let task = TaskRepo::update(
        &*db,
        UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(annotation.to_string())),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("interruption persists");
    let (dispatcher, _) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    for (action, message) in [
        (
            api_types::RecoveryAction::ResumeSession,
            "no resumable session",
        ),
        (api_types::RecoveryAction::RetryHook, "not allowed"),
    ] {
        let error = dispatcher
            .task_service
            .recover_task(&task.id, action, None, None)
            .await
            .expect_err("invalid recovery is refused even at capacity");
        assert!(
            matches!(error, crate::ServiceError::InvalidOperation { message: ref actual } if actual.contains(message))
        );
        let current = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        assert_eq!(current.error_annotation, task.error_annotation);
        assert_eq!(current.version, task.version);
        assert!(deferred_dispatch::queued_recovery(&current).is_none());
    }
    sqlx::query("UPDATE agent_identity SET paused = 1, version = version + 1 WHERE id = ?")
        .bind(&agent_id)
        .execute(db.pool())
        .await
        .expect("agent pauses");
    let error = dispatcher
        .task_service
        .recover_task(&task.id, api_types::RecoveryAction::Reexecute, None, None)
        .await
        .expect_err("paused agent is refused even at capacity");
    assert!(matches!(error, crate::ServiceError::AgentPaused { .. }));
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.error_annotation, task.error_annotation);
    assert!(deferred_dispatch::queued_recovery(&current).is_none());
    sqlx::query("UPDATE agent_identity SET paused = 0, version = version + 1 WHERE id = ?")
        .bind(&agent_id)
        .execute(db.pool())
        .await
        .unwrap();
    let agent = AgentRepo::get_by_id(&*db, &agent_id)
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE daemon SET status = 'offline' WHERE id = ?")
        .bind(agent.daemon_id)
        .execute(db.pool())
        .await
        .unwrap();
    let error = dispatcher
        .task_service
        .recover_task(&task.id, api_types::RecoveryAction::Reexecute, None, None)
        .await
        .expect_err("offline agent is refused even at capacity");
    assert!(
        matches!(error, crate::ServiceError::InvalidOperation { ref message } if message.contains("offline"))
    );
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.error_annotation, task.error_annotation);
    assert!(deferred_dispatch::queued_recovery(&current).is_none());
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
async fn dispatcher_replaces_completed_custom_role_from_superseded_project_revision() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let custom_role = "analyst";
    let mut workflow = crate::workflow::default_workflow::default_workflow();
    let planner_role = workflow
        .roles
        .iter_mut()
        .find(|role| role.name == crate::workflow::default_roles::PLANNER)
        .expect("default workflow defines planner role");
    planner_role.name = custom_role.to_owned();
    planner_role.display_name = "Analyst".to_owned();
    workflow
        .states
        .iter_mut()
        .find(|state| state.name == crate::workflow::default_states::PLANNING)
        .expect("default workflow defines planning state")
        .role = Some(custom_role.to_owned());
    sqlx::query("UPDATE project SET workflow_definition = ?, updated_at = ? WHERE id = ?")
        .bind(serde_json::to_string(&workflow).expect("custom workflow serializes"))
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("custom workflow persists");

    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(
        &db,
        &project_id,
        "stale custom role",
        crate::workflow::default_states::PLANNING,
        0,
    )
    .await;
    assign_role(&db, &task.id, custom_role, &agent_id).await;
    let dispatch_project_version = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("Project loads")
        .expect("Project exists")
        .version;
    let now = now_rfc3339();
    let stale = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: custom_role.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: Some("custom-role-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("completed under old Project authority".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                serde_json::json!({
                    "backend_kind": "native",
                    "executor_type": "shell",
                    "config": {},
                    "plan_delivery": "execution_outbox",
                    "project_version": dispatch_project_version,
                    "task_state": crate::workflow::default_states::PLANNING,
                    "state_entry_token": null,
                })
                .to_string(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("completed custom-role execution creates");
    assert!(
        !crate::task_service::execution::execution_uses_brokered_plan(&stale),
        "custom roles do not own plan-publication authority"
    );
    assert!(
        crate::task_service::execution::execution_belongs_to_current_state_entry(
            &db, &task, &stale,
        )
        .await
        .expect("same-state entry authority resolves"),
        "the stale execution must reach project-authority reconciliation"
    );

    sqlx::query("UPDATE project SET version = version + 1, updated_at = ? WHERE id = ?")
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("Project revision advances");
    let current_project_version = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("Project loads")
        .expect("Project exists")
        .version;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("replacement custom-role execution spawned in time")
        .expect("replacement execution context received");
    assert_eq!(ctx.task_id, task.id);
    assert_ne!(ctx.execution_id, stale.id);
    let replacement = ExecutionRepo::get_by_id(&*db, &ctx.execution_id)
        .await
        .expect("replacement execution loads")
        .expect("replacement execution exists");
    assert_eq!(replacement.role, custom_role);
    assert_eq!(
        crate::task_service::execution_dispatch_project_version(&replacement),
        Some(current_project_version)
    );
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(&*db, &task.id, custom_role)
            .await
            .expect("custom-role execution count loads"),
        2
    );
}

#[tokio::test]
async fn dispatcher_replaces_completed_reviewer_from_superseded_project_revision() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "stale review", "review", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::REVIEWER,
        &agent_id,
    )
    .await;
    let candidate_execution = seed_completed_coder_execution(&db, &task.id).await;
    seed_running_review(&db, &task.id, &candidate_execution, r#"{"ci_steps":[]}"#).await;
    let stale =
        seed_completed_reviewer_execution(&db, &task.id, &agent_id, Some(&candidate_execution))
            .await;
    let review = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("review loads")
        .into_iter()
        .next()
        .expect("review exists");
    sqlx::query("UPDATE review SET reviewer_execution_id = ? WHERE id = ?")
        .bind(&stale.id)
        .bind(&review.id)
        .execute(db.pool())
        .await
        .expect("reviewer attempt binding records");

    sqlx::query("UPDATE project SET version = version + 1, updated_at = ? WHERE id = ?")
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("Project revision advances");
    let current_project_version = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("Project loads")
        .expect("Project exists")
        .version;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("replacement reviewer spawned in time")
        .expect("replacement reviewer execution context received");
    assert_eq!(ctx.task_id, task.id);
    assert_ne!(ctx.execution_id, stale.id);

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, crate::workflow::default_states::REVIEW);
    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::Running);
    assert_eq!(
        reviews[0].reviewer_execution_id.as_deref(),
        Some(ctx.execution_id.as_str())
    );
    let replacement = ExecutionRepo::get_by_id(&*db, &ctx.execution_id)
        .await
        .expect("replacement execution loads")
        .expect("replacement execution exists");
    assert_eq!(
        crate::task_service::execution_dispatch_project_version(&replacement),
        Some(current_project_version)
    );
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

/// A restart stopped this Task's coder with an automatic resume while a fresh
/// Task waits in `todo` for the same single-slot agent. The interrupted Task
/// must get the slot; before, the fresh one always won and the interrupted
/// Task waited behind the whole ready queue.
#[tokio::test]
async fn interrupted_task_resumes_before_a_fresh_task_claims_the_only_slot() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    // `seed_agent` grants one slot more than it is given: this agent has one.
    let agent_id = seed_agent(&db, 0, DaemonStatus::Online, AgentStatus::Idle).await;
    let fresh = seed_task(&db, &project_id, "fresh", "todo", 0).await;
    assign_role(
        &db,
        &fresh.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let interrupted = seed_task(&db, &project_id, "interrupted", "in_progress", 1).await;
    assign_role(
        &db,
        &interrupted.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    seed_cancelled_execution(
        &db,
        &interrupted.id,
        &agent_id,
        crate::workflow::default_roles::CODER,
        Some(StopReason::GracefulShutdown),
        Some(ResumePolicy::Auto),
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    dispatcher.check_once().await.expect("dispatcher runs");

    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("execution spawned in time")
        .expect("execution context received");
    assert_eq!(
        ctx.task_id, interrupted.id,
        "the interrupted Task gets the slot"
    );
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &fresh.id,
            crate::workflow::default_roles::CODER
        )
        .await
        .expect("fresh execution count loads"),
        0,
        "the fresh Task waits for capacity"
    );
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

async fn seed_failed_coder_execution(
    db: &db::SqliteDb,
    task_id: &str,
    project_id: &str,
    agent_id: &str,
) -> String {
    let project_version = db::ProjectRepo::get_by_id(db, project_id)
        .await
        .expect("project loads")
        .expect("project exists")
        .version;
    let now = now_rfc3339();
    let execution_id = new_uuid_v4();
    ExecutionRepo::create(
        db,
        db::CreateExecution {
            id: execution_id.clone(),
            task_id: task_id.to_owned(),
            agent_id: Some(agent_id.to_owned()),
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
            error: Some("io error: No space left on device".to_owned()),
            executor_config_snapshot_json: Some(format!(
                r#"{{"executor_type":"shell","config":{{}},"project_version":{project_version}}}"#
            )),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("failed execution creates");
    execution_id
}

async fn set_zero_execution_retry_budget(db: &db::SqliteDb, task: &Task) -> Task {
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
            task_state_config: Some(Some(r#"{"retry_budgets":{"execution":0}}"#.to_owned())),
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("task retry budget updates")
}

async fn set_task_error_annotation(db: &db::SqliteDb, task: &Task, annotation: &str) {
    TaskRepo::update_status(
        db,
        db::UpdateTaskStatus {
            id: task.id.clone(),
            expected_version: task.version,
            status: task.status.clone(),
            assignee_id: None,
            error_annotation: Some(Some(annotation.to_owned())),
            blocked_json: None,
            failed_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("annotation persists");
}

/// A run that died without its failure being recorded (full disk) leaves the
/// Task in its working state with a failed newest execution and no blocker.
/// The dispatcher must give it the normal executor-failure annotation so it
/// offers recovery, keep the merge-conflict context's state, and not churn or
/// relaunch on later ticks.
#[tokio::test]
async fn dispatcher_blocks_task_whose_failed_execution_was_never_recorded() {
    for status in ["in_progress", "merge_failed"] {
        let db = Arc::new(sqlite_db().await);
        let repo_dir = TempDir::new().expect("repo dir creates");
        let workspace_dir = TempDir::new().expect("workspace dir creates");
        let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
        let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
        let mut task = seed_task(&db, &project_id, "lost failure", status, 0).await;
        task = set_zero_execution_retry_budget(&db, &task).await;
        assign_role(
            &db,
            &task.id,
            crate::workflow::default_roles::CODER,
            &agent_id,
        )
        .await;
        if status == "merge_failed" {
            set_task_error_annotation(
                &db,
                &task,
                r#"{"type":"merge_conflict","blocking_reason":"merge_conflict","recovery_actions":[]}"#,
            )
            .await;
            task = TaskRepo::get_by_id(&*db, &task.id, false)
                .await
                .expect("task loads")
                .expect("task exists");
        }
        let execution_id = seed_failed_coder_execution(&db, &task.id, &project_id, &agent_id).await;
        let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

        dispatcher.check_once().await.expect("dispatcher runs");

        let healed = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        let annotation: serde_json::Value =
            serde_json::from_str(healed.error_annotation.as_deref().expect("annotated"))
                .expect("annotation parses");
        assert_eq!(annotation["type"], "executor_failed", "{status}");
        assert_eq!(annotation["blocked_execution_id"], execution_id.as_str());
        assert!(annotation["recovery_actions"]
            .as_array()
            .expect("recovery actions")
            .iter()
            .any(|action| action == "reexecute"));
        assert!(healed.blocked_json.is_some());
        assert_eq!(healed.status, status);
        assert!(rx.try_recv().is_err(), "no replacement run is launched");

        // Idempotent: a second tick changes nothing.
        dispatcher.check_once().await.expect("dispatcher runs");
        let again = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        assert_eq!(again.version, healed.version, "{status}");
        assert!(rx.try_recv().is_err());
    }
}

/// The reconciliation must not touch a Task that has a live execution
/// launching or running after the failed one.
#[tokio::test]
async fn dispatcher_leaves_failed_execution_alone_while_a_newer_run_is_live() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "relaunched", "in_progress", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    seed_failed_coder_execution(&db, &task.id, &project_id, &agent_id).await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    seed_running_execution(
        &db,
        &task.id,
        &agent_id,
        crate::workflow::default_roles::CODER,
    )
    .await;
    let (dispatcher, _rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    dispatcher.check_once().await.expect("dispatcher runs");

    let after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(after.blocked_json.is_none());
    assert!(after.error_annotation.is_none());
}

/// With execution retry budget left, the reconciliation takes the same
/// deferred-retry path as the live failure handler (and only once).
#[tokio::test]
async fn dispatcher_schedules_retry_for_unrecorded_failure_once() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "lost failure", "in_progress", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let execution_id = seed_failed_coder_execution(&db, &task.id, &project_id, &agent_id).await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    dispatcher.check_once().await.expect("dispatcher runs");
    let scheduled = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(deferred_dispatch::is_pending(
        &scheduled,
        chrono::Utc::now()
    ));
    assert!(scheduled
        .metadata_json
        .as_deref()
        .expect("metadata")
        .contains(&execution_id));

    dispatcher.check_once().await.expect("dispatcher runs");
    let again = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(again.version, scheduled.version);
    assert!(rx.try_recv().is_err());
}

async fn seed_environment_pause(
    db: &db::SqliteDb,
    project_id: &str,
    environment: serde_json::Value,
    checks: &[&str],
    next_check_at: &str,
) -> Project {
    let project = ProjectRepo::get_by_id(db, project_id)
        .await
        .unwrap()
        .unwrap();
    let mut settings: serde_json::Value = serde_json::from_str(&project.settings).unwrap();
    settings["environment"] = environment;
    let project = ProjectRepo::update_at_version(
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
    .unwrap();
    let paused_at = "2026-01-01T00:00:00Z";
    let detail = api_types::ProjectEnvironmentPause {
        workspace_id: None,
        checks: checks.iter().map(|name| (*name).to_owned()).collect(),
        role: Some("reviewer".to_owned()),
        output: "root free: 7G".to_owned(),
        paused_at: paused_at.to_owned(),
        last_checked_at: paused_at.to_owned(),
        next_check_at: next_check_at.to_owned(),
    };
    assert!(ProjectRepo::set_environment_pause_if_unchanged(
        db,
        project_id,
        project.version,
        paused_at,
        &serde_json::to_string(&detail).unwrap(),
    )
    .await
    .unwrap());
    ProjectRepo::get_by_id(db, project_id)
        .await
        .unwrap()
        .unwrap()
}

async fn finish_environment_recheck(dispatcher: &TaskDispatcher, project_id: &str) {
    let job = dispatcher
        .environment_rechecks
        .lock()
        .unwrap()
        .remove(project_id)
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), job)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn environment_recheck_is_single_flight_off_tick_and_stale_result_preserves_user_pause() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let other_repo = TempDir::new().unwrap();
    let workspace = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    seed_environment_pause(&db, &project_id, serde_json::json!({"checks":[{
        "name":"disk", "command":"echo started >> starts; while ! test -f release-check; do sleep 0.05; done", "timeout_seconds":30
    }]}), &["disk"], "2026-01-01T00:00:00Z").await;
    let (other, _) = seed_project_repo(&db, other_repo.path()).await;
    let agent = seed_agent(&db, 2, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &other, "unrelated admission", "todo", 0).await;
    assign_role(&db, &task.id, "coder", &agent).await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace.path()).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), dispatcher.check_once())
            .await
            .unwrap()
            .unwrap(),
        1
    );
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !repo_dir.path().join("starts").exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), dispatcher.check_once())
            .await
            .unwrap()
            .unwrap(),
        0
    );
    assert!(matches!(
        dispatcher
            .task_service
            .recheck_project_environment(&project_id)
            .await,
        Err(crate::ServiceError::Conflict(_))
    ));
    assert_eq!(
        std::fs::read_to_string(repo_dir.path().join("starts"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    ProjectRepo::set_paused_at(&*db, &project_id, Some(now_rfc3339()))
        .await
        .unwrap();
    let user_pause = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    std::fs::write(repo_dir.path().join("release-check"), "yes").unwrap();
    finish_environment_recheck(&dispatcher, &project_id).await;
    let current = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.version, user_pause.version);
    assert_eq!(current.paused_at, user_pause.paused_at);
    assert!(current.system_pause_reason.is_none());
}

#[tokio::test]
async fn environment_recheck_without_rerunnable_checks_stays_paused() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    seed_environment_pause(
        &db,
        &project_id,
        serde_json::json!({}),
        &[],
        "2026-01-01T00:00:00Z",
    )
    .await;
    let (dispatcher, _) = build_dispatcher(Arc::clone(&db), workspace.path()).await;
    dispatcher.check_once().await.unwrap();
    let current = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    assert!(current.paused_at.is_some());
    assert!(crate::project_environment::pause_detail(&current)
        .unwrap()
        .unwrap()
        .output
        .contains("No re-runnable"));
    dispatcher.check_once().await.unwrap();
    assert_eq!(
        ProjectRepo::get_by_id(&*db, &project_id)
            .await
            .unwrap()
            .unwrap()
            .version,
        current.version
    );
    let (results, current) = dispatcher
        .task_service
        .recheck_project_environment(&project_id)
        .await
        .unwrap();
    assert!(results.is_empty());
    assert!(current.paused_at.is_some());
    sqlx::query("UPDATE project SET settings = ?, version = version + 1 WHERE id = ?")
        .bind(r#"{"environment":{"checks":[{"name":"fixed","command":"true"}]}}"#)
        .bind(&project_id)
        .execute(db.pool())
        .await
        .unwrap();
    // The manual empty check rescheduled the detail; force it due again.
    sqlx::query("UPDATE project SET environment_pause_json = json_set(environment_pause_json, '$.next_check_at', '2026-01-01T00:00:00Z') WHERE id = ?")
        .bind(&project_id).execute(db.pool()).await.unwrap();
    // A configured check that passes says nothing about the failure that
    // caused this pause, so the dispatcher must not resume on it.
    dispatcher.check_once().await.unwrap();
    assert!(dispatcher
        .environment_rechecks
        .lock()
        .unwrap()
        .get(&project_id)
        .is_none());
    assert!(ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap()
        .paused_at
        .is_some());
    // The owner's own "Check now" is an explicit decision and does resume.
    let (results, current) = dispatcher
        .task_service
        .recheck_project_environment(&project_id)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert!(current.paused_at.is_none());
}

#[tokio::test]
async fn environment_and_project_limit_errors_do_not_abort_later_projects() {
    let db = Arc::new(sqlite_db().await);
    let workspace = TempDir::new().unwrap();
    let agent = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let mut repos = Vec::new();
    for (index, corruption) in ["pause", "environment-settings", "slot-settings"]
        .into_iter()
        .enumerate()
    {
        let repo = TempDir::new().unwrap();
        let (project_id, _) = seed_project_repo(&db, repo.path()).await;
        if corruption != "slot-settings" {
            seed_environment_pause(
                &db,
                &project_id,
                serde_json::json!({"checks":[{"name":"disk","command":"true"}]}),
                &["disk"],
                "2026-01-01T00:00:00Z",
            )
            .await;
        } else {
            let task = seed_task(&db, &project_id, "waiting for admission", "todo", 0).await;
            assign_role(&db, &task.id, "coder", &agent).await;
        }
        if corruption == "pause" {
            sqlx::query("UPDATE project SET environment_pause_json = 'invalid' WHERE id = ?")
                .bind(&project_id)
                .execute(db.pool())
                .await
                .unwrap();
        } else {
            sqlx::query("UPDATE project SET settings = 'invalid' WHERE id = ?")
                .bind(&project_id)
                .execute(db.pool())
                .await
                .unwrap();
        }
        sqlx::query("UPDATE project SET created_at = ? WHERE id = ?")
            .bind(format!("2000-01-0{}T00:00:00Z", index + 1))
            .bind(&project_id)
            .execute(db.pool())
            .await
            .unwrap();
        repos.push(repo);
    }
    let repo = TempDir::new().unwrap();
    let (healthy, _) = seed_project_repo(&db, repo.path()).await;
    let task = seed_task(&db, &healthy, "healthy admission", "todo", 0).await;
    assign_role(&db, &task.id, "coder", &agent).await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace.path()).await;
    assert_eq!(dispatcher.check_once().await.unwrap(), 1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap()
            .task_id,
        task.id
    );
}

#[tokio::test]
async fn environment_recheck_resumes_and_redispatches_current_task_states() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 2, DaemonStatus::Online, AgentStatus::Idle).await;
    let active = seed_task(
        &db,
        &project_id,
        "active before environment failure",
        "in_progress",
        1,
    )
    .await;
    let initial = seed_task(&db, &project_id, "waiting for environment", "todo", 0).await;
    for task in [&active, &initial] {
        assign_role(&db, &task.id, "coder", &agent_id).await;
        let failed = seed_cancelled_execution(
            &db,
            &task.id,
            &agent_id,
            "coder",
            Some(StopReason::ExecutorFailed),
            Some(ResumePolicy::Manual),
        )
        .await;
        sqlx::query("UPDATE execution SET status = 'failed', error = 'environment not ready: disk exited 1' WHERE id = ?")
            .bind(failed.id).execute(db.pool()).await.unwrap();
        assert!(
            !helpers::latest_stopped_execution_blocks_dispatch(&db, &task.id, "coder")
                .await
                .unwrap()
        );
        assert!(
            !helpers::latest_execution_awaits_completion_cascade(&db, &task.id, "coder")
                .await
                .unwrap()
        );
    }
    let paused = seed_environment_pause(
        &db,
        &project_id,
        serde_json::json!({
            "env": {"READY": "yes"},
            "checks": [
                {"name": "disk", "command": "test \"$READY\" = yes", "roles": ["reviewer"]},
                {"name": "unrelated", "command": "touch unrelated-check; exit 1"}
            ]
        }),
        &["disk"],
        "2026-01-01T00:00:00Z",
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    let mut events = dispatcher.event_bus.subscribe();
    assert_eq!(
        dispatcher.check_once().await.unwrap(),
        0,
        "resume skips the stale snapshot"
    );
    finish_environment_recheck(&dispatcher, &project_id).await;
    let resumed = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    assert!(resumed.paused_at.is_none());
    assert!(resumed.system_pause_reason.is_none());
    assert!(resumed.environment_pause_json.is_none());
    assert!(resumed.version > paused.version);
    assert!(
        !repo_dir.path().join("unrelated-check").exists(),
        "only recorded checks run"
    );
    let event = events.try_recv().unwrap();
    assert_eq!(event.event_type, "project.resumed");
    assert_eq!(event.entity_id, project_id);
    assert_eq!(
        TaskRepo::get_by_id(&*db, &active.id, false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "in_progress"
    );
    assert_eq!(
        TaskRepo::get_by_id(&*db, &initial.id, false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "todo"
    );
    // Remove the intentionally failing unrelated probe before the two launches,
    // which apply all checks for their own role at the normal launch boundary.
    sqlx::query("UPDATE project SET settings = json_set(settings, '$.environment.checks', json('[]')), version = version + 1 WHERE id = ?")
        .bind(&project_id)
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(dispatcher.check_once().await.unwrap(), 2);
    let first = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let ids = std::collections::HashSet::from([first.task_id, second.task_id]);
    assert_eq!(
        ids,
        std::collections::HashSet::from([active.id.clone(), initial.id.clone()])
    );
    for task in [&active, &initial] {
        let current = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.status, "in_progress");
        assert!(current.error_annotation.is_none());
        assert!(current.blocked_json.is_none());
    }
}

#[tokio::test]
async fn environment_recheck_still_failing_reschedules_without_dispatch() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 2, DaemonStatus::Online, AgentStatus::Idle).await;
    let waiting = seed_task(&db, &project_id, "waiting while paused", "todo", 0).await;
    let running = seed_task(&db, &project_id, "already running", "in_progress", 1).await;
    for task in [&waiting, &running] {
        assign_role(&db, &task.id, "coder", &agent_id).await;
    }
    seed_running_execution(&db, &running.id, &agent_id, "coder").await;
    let paused = seed_environment_pause(&db, &project_id, serde_json::json!({
        "recheck_interval_seconds": 60,
        "checks": [{"name": "disk", "command": "printf 'root free: 7G'; exit 1", "roles": ["reviewer"]}]
    }), &["disk"], "2026-01-01T00:00:00Z").await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    let mut events = dispatcher.event_bus.subscribe();
    let before = chrono::Utc::now();
    assert_eq!(dispatcher.check_once().await.unwrap(), 0);
    finish_environment_recheck(&dispatcher, &project_id).await;
    let current = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.paused_at, paused.paused_at);
    assert_eq!(
        current.system_pause_reason.as_deref(),
        Some("environment_not_ready")
    );
    let detail = crate::project_environment::pause_detail(&current)
        .unwrap()
        .unwrap();
    assert_eq!(detail.checks, vec!["disk"]);
    assert!(detail.output.contains("root free: 7G"));
    let last = chrono::DateTime::parse_from_rfc3339(&detail.last_checked_at).unwrap();
    let next = chrono::DateTime::parse_from_rfc3339(&detail.next_check_at).unwrap();
    assert!(last >= before);
    assert_eq!((next - last).num_seconds(), 60);
    assert_eq!(
        ExecutionRepo::list_running_by_task(&*db, &running.id)
            .await
            .unwrap()
            .len(),
        1,
        "existing executions keep running"
    );
    let waiting_now = TaskRepo::get_by_id(&*db, &waiting.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(waiting_now.status, "todo");
    assert!(waiting_now.error_annotation.is_none());
    assert!(rx.try_recv().is_err());
    assert_eq!(events.try_recv().unwrap().event_type, "project.updated");
    assert_eq!(dispatcher.check_once().await.unwrap(), 0);
    assert_eq!(
        ProjectRepo::get_by_id(&*db, &project_id)
            .await
            .unwrap()
            .unwrap()
            .version,
        current.version,
        "not due again yet"
    );
}

#[tokio::test]
async fn environment_recheck_preserves_user_pause_and_stale_resume_loses() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let paused = seed_environment_pause(
        &db,
        &project_id,
        serde_json::json!({
            "checks": [{"name": "disk", "command": "touch checked"}]
        }),
        &["disk"],
        "2026-01-01T00:00:00Z",
    )
    .await;
    let (dispatcher, _) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    ProjectRepo::set_paused_at(&*db, &project_id, Some(now_rfc3339()))
        .await
        .unwrap();
    let user_paused = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    assert!(user_paused.environment_pause_json.is_none());
    assert!(user_paused.system_pause_reason.is_none());
    assert!(!dispatcher
        .task_service
        .clear_environment_pause(&paused)
        .await
        .unwrap());
    assert_eq!(dispatcher.check_once().await.unwrap(), 0);
    assert!(!repo_dir.path().join("checked").exists());
    let (checks, current) = dispatcher
        .task_service
        .recheck_project_environment(&project_id)
        .await
        .unwrap();
    assert!(checks[0].passed);
    assert_eq!(current.paused_at, user_paused.paused_at);
    assert_eq!(current.version, user_paused.version);
    assert!(!ProjectRepo::set_environment_pause_if_unchanged(
        &*db,
        &project_id,
        current.version,
        &now_rfc3339(),
        "{}"
    )
    .await
    .unwrap());
}

#[tokio::test]
async fn environment_recheck_missing_checkout_reschedules_without_immediate_respawn() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let workspace = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo.path()).await;
    let paused = seed_environment_pause(
        &db,
        &project_id,
        serde_json::json!({"checks":[{"name":"disk","command":"true"}], "recheck_interval_seconds": 60}),
        &["disk"],
        "2026-01-01T00:00:00Z",
    )
    .await;
    // Repository sync leaves an environment-owned pause alone, but the
    // re-check cannot resolve a usable primary checkout.
    std::fs::remove_dir_all(repo.path().join(".git")).unwrap();
    let (dispatcher, _) = build_dispatcher(Arc::clone(&db), workspace.path()).await;
    let started_at = chrono::Utc::now();
    assert_eq!(dispatcher.check_once().await.unwrap(), 0);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if dispatcher.environment_rechecks.lock().unwrap()[&project_id].is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let current = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let detail = crate::project_environment::pause_detail(&current)
        .unwrap()
        .unwrap();
    assert_eq!(current.paused_at, paused.paused_at);
    assert_eq!(detail.checks, vec!["disk"]);
    assert!(detail.output.contains("primary checkout is unavailable"));
    assert!(chrono::DateTime::parse_from_rfc3339(&detail.last_checked_at).unwrap() >= started_at);
    assert!(
        chrono::DateTime::parse_from_rfc3339(&detail.next_check_at).unwrap()
            >= started_at + chrono::Duration::seconds(60)
    );
    // Observe the finished job, then scan once more against the new due time.
    assert_eq!(dispatcher.check_once().await.unwrap(), 0);
    assert_eq!(dispatcher.check_once().await.unwrap(), 0);
    assert!(!dispatcher
        .environment_rechecks
        .lock()
        .unwrap()
        .contains_key(&project_id));
    assert_eq!(
        ProjectRepo::get_by_id(&*db, &project_id)
            .await
            .unwrap()
            .unwrap()
            .version,
        current.version
    );
}

async fn set_project_active_task_limit(db: &db::SqliteDb, project_id: &str, limit: u32) -> Project {
    let project = ProjectRepo::get_by_id(db, project_id)
        .await
        .unwrap()
        .unwrap();
    let mut settings: serde_json::Value = serde_json::from_str(&project.settings).unwrap();
    settings["max_active_tasks"] = serde_json::json!(limit);
    ProjectRepo::update_at_version(
        db,
        UpdateProject {
            id: project.id.clone(),
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
    .unwrap()
}

#[tokio::test]
async fn dispatcher_project_capacity_disposition_refreshes_only_on_change() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
    seed_task(&db, &project_id, "admitted", "in_progress", 0).await;
    let queued = seed_task(&db, &project_id, "queued", "todo", 0).await;
    assign_role(&db, &queued.id, "coder", &agent_id).await;
    let project = set_project_active_task_limit(&db, &project_id, 1).await;
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    let mut events = dispatcher.event_bus.subscribe();

    assert_eq!(
        dispatcher
            .dispatch_initial_tasks(&project, &workflow)
            .await
            .unwrap(),
        0
    );
    let recorded = events.try_recv().unwrap();
    assert_eq!(recorded.event_type, "task.updated");
    assert_eq!(recorded.entity_id, queued.id);
    assert!(matches!(
        recorded.context,
        events::EventContext::TaskUpdated { project_id: id } if id == project_id
    ));
    assert!(
        events.try_recv().is_err(),
        "first record emits exactly once"
    );
    let waiting = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        deferred_dispatch::current_dispatch_disposition(&waiting)
            .unwrap()
            .capability,
        "project_capacity"
    );

    assert_eq!(
        dispatcher
            .dispatch_initial_tasks(&project, &workflow)
            .await
            .unwrap(),
        0
    );
    assert!(events.try_recv().is_err(), "unchanged tick emits nothing");
    assert!(rx.try_recv().is_err());

    dispatcher
        .clear_dispatch_disposition(&waiting)
        .await
        .unwrap();
    let cleared = events.try_recv().unwrap();
    assert_eq!(cleared.event_type, "task.updated");
    assert_eq!(cleared.entity_id, queued.id);
    assert!(matches!(
        cleared.context,
        events::EventContext::TaskUpdated { project_id: id } if id == project_id
    ));
    assert!(events.try_recv().is_err(), "clear emits exactly once");
    let after_clear = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(deferred_dispatch::dispatch_disposition(&after_clear).is_none());

    // Even a stale clearer still carrying the marker must not emit again.
    dispatcher
        .clear_dispatch_disposition(&waiting)
        .await
        .unwrap();
    dispatcher
        .clear_dispatch_disposition(&after_clear)
        .await
        .unwrap();
    assert!(events.try_recv().is_err(), "no-op clears emit nothing");
}

#[tokio::test]
async fn dispatcher_full_project_rechecks_capacity_without_task_version_change() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
    let mut active_tasks = Vec::new();
    for status in [
        "in_progress",
        "in_progress",
        "review",
        "review",
        "merge_failed",
    ] {
        active_tasks.push(seed_task(&db, &project_id, "admitted", status, 0).await);
    }
    let queued = seed_task(&db, &project_id, "NK-50", "todo", 0).await;
    assign_role(&db, &queued.id, "coder", &agent_id).await;
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    assert_eq!(
        dispatcher
            .dispatch_initial_tasks(&project, &workflow)
            .await
            .unwrap(),
        0
    );
    let waiting = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(waiting.status, "todo");
    assert_eq!(waiting.version, queued.version);
    let disposition = deferred_dispatch::current_dispatch_disposition(&waiting).unwrap();
    assert_eq!(disposition.capability, "project_capacity");
    assert_eq!(
        disposition.safe_message,
        "project_at_capacity: waiting for a slot (5/5 active)"
    );
    assert!(!deferred_dispatch::dispatch_disposition_is_current(
        &waiting, "coder"
    ));
    assert!(rx.try_recv().is_err());

    // A sentinel timestamp makes even a same-clock repeated write observable.
    sqlx::query("UPDATE task SET metadata_json = json_set(metadata_json, '$.dispatch_disposition.recorded_at', '2026-01-01T00:00:00Z'), updated_at = '2026-01-01T00:00:00Z' WHERE id = ?")
        .bind(&queued.id).execute(db.pool()).await.unwrap();
    let unchanged = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks(&project, &workflow)
            .await
            .unwrap(),
        0
    );
    let repeated = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(repeated.metadata_json, unchanged.metadata_json);
    assert_eq!(repeated.updated_at, unchanged.updated_at);

    TaskRepo::update_status(
        &*db,
        db::UpdateTaskStatus {
            id: active_tasks[0].id.clone(),
            expected_version: active_tasks[0].version,
            status: "done".to_owned(),
            assignee_id: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks(&project, &workflow)
            .await
            .unwrap(),
        1
    );
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ctx.task_id, queued.id);
    let admitted = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(admitted.status, "in_progress");
    assert!(deferred_dispatch::dispatch_disposition_for_test(&admitted).is_none());
    assert_eq!(
        super::slots::load_project_slots(&db, &project)
            .await
            .unwrap()
            .active,
        5
    );
}

#[tokio::test]
async fn dispatcher_project_limit_preserves_ready_queue_order_and_counts_each_admission() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
    let low = seed_task(&db, &project_id, "low", "todo", 1).await;
    let high = seed_task(&db, &project_id, "high", "todo", 10).await;
    assign_role(&db, &low.id, "coder", &agent_id).await;
    assign_role(&db, &high.id, "coder", &agent_id).await;
    let project = set_project_active_task_limit(&db, &project_id, 1).await;
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    assert_eq!(
        dispatcher
            .dispatch_initial_tasks(&project, &workflow)
            .await
            .unwrap(),
        1
    );
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ctx.task_id, high.id);
    let waiting = TaskRepo::get_by_id(&*db, &low.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(waiting.status, "todo");
    assert_eq!(
        deferred_dispatch::current_dispatch_disposition(&waiting)
            .unwrap()
            .safe_message,
        "project_at_capacity: waiting for a slot (1/1 active)"
    );
}

#[tokio::test]
async fn dispatcher_parked_review_frees_a_project_slot() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
    for _ in 0..4 {
        seed_task(&db, &project_id, "admitted", "in_progress", 0).await;
    }
    let parked = seed_task(&db, &project_id, "owner review", "review", 0).await;
    set_task_error_annotation(
        &db,
        &parked,
        r#"{"type":"review_needs_owner","message":"needs macOS","recovery_actions":["reexecute"]}"#,
    )
    .await;
    let queued = seed_task(&db, &project_id, "ready", "todo", 0).await;
    assign_role(&db, &queued.id, "coder", &agent_id).await;
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks(&project, &workflow)
            .await
            .unwrap(),
        1
    );
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ctx.task_id, queued.id);
    let slots = super::slots::load_project_slots(&db, &project)
        .await
        .unwrap();
    assert_eq!((slots.active, slots.parked, slots.queued), (5, 1, 0));
}

#[tokio::test]
async fn dispatcher_parked_guard_rechecks_when_owner_clears_a_park() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
    for _ in 0..2 {
        seed_task(&db, &project_id, "admitted", "in_progress", 0).await;
    }
    let mut parked_tasks = Vec::new();
    for _ in 0..10 {
        let task = seed_task(&db, &project_id, "owner review", "review", 0).await;
        set_task_error_annotation(&db, &task, r#"{"type":"review_needs_owner"}"#).await;
        parked_tasks.push(task);
    }
    let queued = seed_task(&db, &project_id, "ready", "todo", 0).await;
    assign_role(&db, &queued.id, "coder", &agent_id).await;
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks(&project, &workflow)
            .await
            .unwrap(),
        0
    );
    let waiting = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        deferred_dispatch::current_dispatch_disposition(&waiting)
            .unwrap()
            .safe_message,
        "project_waiting_on_owner: 10 parked tasks waiting on the owner"
    );

    sqlx::query("UPDATE task SET error_annotation = NULL, version = version + 1 WHERE id = ?")
        .bind(&parked_tasks[0].id)
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks(&project, &workflow)
            .await
            .unwrap(),
        1
    );
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ctx.task_id, queued.id);
    let admitted = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(deferred_dispatch::dispatch_disposition_for_test(&admitted).is_none());
}

#[tokio::test]
async fn dispatcher_owner_reexecute_resumes_over_project_limit() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
    for _ in 0..5 {
        seed_task(&db, &project_id, "admitted", "in_progress", 0).await;
    }
    let parked = seed_task(&db, &project_id, "interrupted", "in_progress", 0).await;
    assign_role(&db, &parked.id, "coder", &agent_id).await;
    set_task_error_annotation(&db, &parked,
        r#"{"type":"recovery_required","blocking_reason":"crash_recovery","recovery_actions":["reexecute"]}"#).await;
    let queued = seed_task(&db, &project_id, "new work", "todo", 0).await;
    assign_role(&db, &queued.id, "coder", &agent_id).await;
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    assert_eq!(
        super::slots::load_project_slots(&db, &project)
            .await
            .unwrap()
            .active,
        5
    );

    let recovered = dispatcher
        .task_service
        .recover_task(
            &parked.id,
            api_types::RecoveryAction::Reexecute,
            Some("host is ready".to_owned()),
            None,
        )
        .await
        .expect("owner recovery is admitted over the limit");
    assert!(recovered.error_annotation.is_none());
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ctx.task_id, parked.id);
    let slots = super::slots::load_project_slots(&db, &project)
        .await
        .unwrap();
    assert_eq!((slots.active, slots.parked), (6, 0));
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks(&project, &workflow)
            .await
            .unwrap(),
        0
    );
    let waiting = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        deferred_dispatch::current_dispatch_disposition(&waiting)
            .unwrap()
            .safe_message,
        "project_at_capacity: waiting for a slot (6/5 active)"
    );
}

#[tokio::test]
async fn dispatcher_project_limit_skips_projection_without_admission_candidate() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let workspace = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo.path()).await;
    let mut project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    project.settings = "invalid".to_owned();
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let (dispatcher, _) = build_dispatcher(Arc::clone(&db), workspace.path()).await;
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks(&project, &workflow)
            .await
            .unwrap(),
        0
    );
    let task = seed_task(&db, &project_id, "blocked initial Task", "todo", 0).await;
    sqlx::query("UPDATE task SET blocked_json = '{}' WHERE id = ?")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks(&project, &workflow)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn dispatcher_zero_project_limit_disables_capacity_and_parked_guard() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
    for _ in 0..5 {
        seed_task(&db, &project_id, "admitted", "in_progress", 0).await;
    }
    for _ in 0..10 {
        let parked = seed_task(&db, &project_id, "owner review", "review", 0).await;
        set_task_error_annotation(&db, &parked, r#"{"type":"review_needs_owner"}"#).await;
    }
    let queued = seed_task(&db, &project_id, "new work", "todo", 0).await;
    assign_role(&db, &queued.id, "coder", &agent_id).await;
    let project = set_project_active_task_limit(&db, &project_id, 0).await;
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks(&project, &workflow)
            .await
            .unwrap(),
        1
    );
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ctx.task_id, queued.id);
    let slots = super::slots::load_project_slots(&db, &project)
        .await
        .unwrap();
    assert_eq!((slots.limit, slots.active, slots.parked), (0, 0, 0));
}

#[tokio::test]
async fn dispatcher_active_recovery_runs_over_project_limit() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
    for _ in 0..5 {
        seed_task(&db, &project_id, "admitted", "in_progress", 0).await;
    }
    let recovering = seed_task(&db, &project_id, "undispatched", "in_progress", 0).await;
    assign_role(&db, &recovering.id, "coder", &agent_id).await;
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    assert_eq!(
        super::slots::load_project_slots(&db, &project)
            .await
            .unwrap()
            .active,
        6
    );
    assert_eq!(
        dispatcher
            .recover_active_tasks(&project, &workflow)
            .await
            .unwrap(),
        1
    );
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ctx.task_id, recovering.id);
}

#[tokio::test]
async fn dispatcher_coordination_root_advance_is_not_capacity_gated() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    seed_task(&db, &project_id, "admitted", "in_progress", 0).await;
    let root = seed_task(&db, &project_id, "coordination", "todo", 0).await;
    let child = seed_task(&db, &project_id, "completed child", "done", 0).await;
    sqlx::query(
        "UPDATE task SET parent_task_id = ?, subtask_order = 0, version = version + 1 WHERE id = ?",
    )
    .bind(&root.id)
    .bind(&child.id)
    .execute(db.pool())
    .await
    .unwrap();
    // Exercise the coordination transitions without starting aggregate
    // review execution, which has its own independent admission checks.
    let mut workflow = crate::workflow::default_workflow::default_workflow();
    for state in &mut workflow.states {
        state.hooks = api_types::StateHooks::default();
    }
    sqlx::query("UPDATE project SET workflow_definition = ?, version = version + 1 WHERE id = ?")
        .bind(serde_json::to_string(&workflow).unwrap())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .unwrap();
    let project = set_project_active_task_limit(&db, &project_id, 1).await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    assert_eq!(
        super::slots::load_project_slots(&db, &project)
            .await
            .unwrap()
            .active,
        1
    );
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks(&project, &workflow)
            .await
            .unwrap(),
        1
    );
    let advanced = TaskRepo::get_by_id(&*db, &root.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(advanced.status, "review");
    assert!(deferred_dispatch::dispatch_disposition_for_test(&advanced).is_none());
    // The advance itself was not gated; the root's own aggregate review now
    // holds a slot because no child does.
    assert_eq!(
        super::slots::load_project_slots(&db, &project)
            .await
            .unwrap()
            .active,
        2
    );
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn disconnected_owner_is_skipped_before_dispatch_and_never_falls_back() {
    let db = Arc::new(sqlite_db().await);
    let (task, placement, execution) = crate::recovery::tests::daemon_owned_fixture(&db).await;
    let bus = Arc::new(events::EventBus::new(16));
    let service = Arc::new(TaskService::new(db.clone(), bus.clone()));
    let dispatcher = TaskDispatcher::new(db.clone(), bus, service);
    assert_eq!(
        dispatcher
            .list_tasks(&task.project_id, vec![task.status.clone()])
            .await
            .unwrap()
            .len(),
        1
    );
    let project = ProjectRepo::get_by_id(&*db, &task.project_id)
        .await
        .unwrap()
        .unwrap();
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    assert_eq!(
        dispatcher
            .recover_active_tasks(&project, &workflow)
            .await
            .unwrap(),
        0
    );
    assert!(!dispatcher
        .dispatch_initial_task(
            &task,
            &super::initial_scheduling::InitialScheduleTarget {
                transition_to: "in_progress".to_owned(),
                role: "coder".to_owned(),
                agent_id: execution.agent_id.clone().unwrap(),
            }
        )
        .await
        .unwrap());
    assert_eq!(
        db::WorkspacePlacementRepo::get_for_task(&*db, &task.id)
            .await
            .unwrap()
            .unwrap()
            .id,
        placement.id
    );
    assert_eq!(
        ExecutionRepo::list_running_by_task(&*db, &task.id)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ExecutionStatus::Running
    );
}

#[tokio::test]
async fn revision_two_refused_task_dispatches_after_upgrade_without_manual_action() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let agent = AgentRepo::get_by_id(&*db, &agent_id)
        .await
        .unwrap()
        .unwrap();
    let daemon_id = agent.daemon_id.as_deref().unwrap();
    sqlx::query("UPDATE daemon SET machine_id = 'remote-upgrade-test' WHERE id = ?")
        .bind(daemon_id)
        .execute(db.pool())
        .await
        .unwrap();
    let now = now_rfc3339();
    let runtime = db::RuntimeRepo::create(
        &*db,
        db::CreateRuntime {
            id: new_uuid_v4(),
            daemon_id: daemon_id.into(),
            kind: "local".into(),
            workspace_root: workspace_dir.path().to_string_lossy().into(),
            status: db::RuntimeStatus::Ready,
            labels_json: "{}".into(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    db::RepoLocationRepo::create(
        &*db,
        db::CreateRepoLocation {
            id: new_uuid_v4(),
            repo_id,
            owner_kind: db::RepoLocationOwnerKind::Server,
            daemon_id: Some(daemon_id.into()),
            runtime_id: Some(runtime.id),
            path: repo_dir.path().to_string_lossy().into(),
            kind: db::RepoLocationKind::SharedMount,
            is_default: true,
            status: db::RepoLocationStatus::Ready,
            last_verified_at: Some(now.clone()),
            last_error: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    let task = seed_task(&db, &project_id, "upgrade refused", "todo", 0).await;
    assign_role(&db, &task.id, "coder", &agent_id).await;
    let registry = Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
    let (connection, _outbound) = crate::daemon_transport::DaemonConnection::new(daemon_id.into());
    let id = connection.id();
    registry.register(daemon_id.into(), connection);
    registry.dispatch_incoming_for_connection(daemon_id, id, api_types::DaemonFrame::Notification {
        method:api_types::METHOD_DAEMON_HANDSHAKE.into(),
        params:serde_json::json!({"protocol_revision":2,"capabilities":["execution.terminal.usage_reports","execution.terminal.ack"]}),
    });
    let (dispatcher, mut rx) = build_dispatcher(db.clone(), workspace_dir.path()).await;
    let service = Arc::new(
        dispatcher
            .task_service
            .as_ref()
            .clone()
            .with_daemon_connections(registry.clone()),
    );
    let dispatcher = TaskDispatcher::new(db.clone(), dispatcher.event_bus.clone(), service);
    dispatcher.check_once().await.unwrap();
    let refused = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(refused.status, "todo");
    let annotation: serde_json::Value = serde_json::from_str(
        refused
            .error_annotation
            .as_deref()
            .expect("upgrade annotation"),
    )
    .unwrap();
    assert_eq!(annotation["code"], api_types::DAEMON_UPGRADE_REQUIRED);
    assert_eq!(annotation["daemon_ids"], serde_json::json!([daemon_id]));
    assert!(ExecutionRepo::list_running_by_task(&*db, &task.id)
        .await
        .unwrap()
        .is_empty());
    assert!(db::WorkspacePlacementRepo::get_for_task(&*db, &task.id)
        .await
        .unwrap()
        .is_none());
    assert!(rx.try_recv().is_err());
    // The refusal is parked; the accepted handshake wakes it through the monitor.
    dispatcher.check_once().await.unwrap();
    let (connection, mut upgraded_outbound) =
        crate::daemon_transport::DaemonConnection::new(daemon_id.into());
    let id = connection.id();
    registry.register(daemon_id.into(), connection);
    registry.dispatch_incoming_for_connection(daemon_id, id, api_types::DaemonFrame::Notification {
        method:api_types::METHOD_DAEMON_HANDSHAKE.into(),
        params:serde_json::json!({"protocol_revision":3,"capabilities":["workspace.v1","journal.ack","execution.terminal.usage_reports"],
            "executor_capabilities":{"shell":{"cancel_ack":true,"terminal_observed":true}},"workspace_run_policy":{"allowed_purposes":["ci_step","hook","environment_setup"]}}),
    });
    let responses = registry.clone();
    let response_daemon = daemon_id.to_owned();
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let responder = tokio::spawn(async move {
        while let Some(api_types::DaemonFrame::Request {
            id: request_id,
            method,
            params,
        }) = upgraded_outbound.recv().await
        {
            let result = match method.as_str() {
                api_types::METHOD_REPO_LOCATION_VERIFY => serde_json::json!({
                    "repo_location_id":params["repo_location_id"],"path":params["path"],
                    "default_branch_sha":"verified-head","origin_url":null,
                    "probe_content":params["probe"]["content"],
                }),
                api_types::METHOD_EXECUTION_START => {
                    started_tx
                        .send(params["execution_id"].as_str().unwrap().to_owned())
                        .unwrap();
                    serde_json::json!({"execution_id":params["execution_id"],"accepted":true})
                }
                _ => panic!("unexpected owner request {method}"),
            };
            responses.dispatch_incoming_for_connection(
                &response_daemon,
                id,
                api_types::DaemonFrame::Response {
                    id: request_id,
                    result,
                },
            );
        }
    });
    crate::HeartbeatMonitor::new(db.clone(), dispatcher.event_bus.clone())
        .with_daemon_connections(registry)
        .check_once()
        .await
        .unwrap();
    let woken = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(woken.error_annotation.is_none());
    dispatcher.check_once().await.unwrap();
    let executions = ExecutionRepo::list_running_by_task(&*db, &task.id)
        .await
        .unwrap();
    assert_eq!(
        executions.len(),
        1,
        "{:?}",
        TaskRepo::get_by_id(&*db, &task.id, false).await.unwrap()
    );
    let started = tokio::time::timeout(Duration::from_secs(5), started_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(started, executions[0].id);
    responder.abort();
}

#[tokio::test]
async fn worker_robustness_dispatcher_isolates_projects_and_tasks() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let workspace = TempDir::new().unwrap();
    let (broken, _) = seed_project_repo(&db, repo.path()).await;
    sqlx::query("UPDATE project SET settings = 'invalid', created_at = ? WHERE id = ?")
        .bind("2000-01-01T00:00:00Z")
        .bind(&broken)
        .execute(db.pool())
        .await
        .unwrap();
    let healthy_repo = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, healthy_repo.path()).await;
    sqlx::query("UPDATE project SET created_at = ? WHERE id = ?")
        .bind("2001-01-01T00:00:00Z")
        .bind(&project_id)
        .execute(db.pool())
        .await
        .unwrap();
    let bad = seed_task(&db, &project_id, "bad metadata", "in_progress", 0).await;
    sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
        .bind(r#"{"owner_wait":{"daemon_id":"owner","started_at":"2000-01-01T00:00:00Z"},"plan_publication_claim":42}"#)
        .bind(&bad.id).execute(db.pool()).await.unwrap();
    let agent = seed_agent(&db, 2, DaemonStatus::Online, AgentStatus::Idle).await;
    let healthy = seed_task(&db, &project_id, "healthy", "todo", 0).await;
    assign_role(&db, &healthy.id, "coder", &agent).await;
    let (dispatcher, mut rx) = build_dispatcher(db, workspace.path()).await;
    assert_eq!(dispatcher.check_once().await.unwrap(), 1);
    assert_eq!(rx.recv().await.unwrap().task_id, healthy.id);

    let healthy_again = seed_task(&dispatcher.db, &project_id, "healthy again", "todo", 0).await;
    assign_role(&dispatcher.db, &healthy_again.id, "coder", &agent).await;
    assert_eq!(dispatcher.check_once().await.unwrap(), 1);
    assert_eq!(rx.recv().await.unwrap().task_id, healthy_again.id);
}

#[tokio::test]
async fn machine_capacity_waits_in_initial_state_then_starts_after_run_ends() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let workspaces = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo.path()).await;
    let agent_id = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
    let running = seed_task(&db, &project_id, "RUN", "in_progress", 0).await;
    seed_running_execution(&db, &running.id, &agent_id, "coder").await;
    let queued = seed_task(&db, &project_id, "WAIT", "todo", 0).await;
    assign_role(&db, &queued.id, "coder", &agent_id).await;
    db.server_run_cap.set(
        Some(1),
        config::resolved_run_cap(Some(1)),
        &config::embedded_machine_id(),
    );
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let (dispatcher, mut rx) = build_dispatcher(db.clone(), workspaces.path()).await;
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks(&project, &workflow)
            .await
            .unwrap(),
        0
    );
    let waiting = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(waiting.status, queued.status);
    assert_eq!(waiting.version, queued.version);
    let disposition = deferred_dispatch::current_dispatch_disposition(&waiting).unwrap();
    assert_eq!(
        disposition.capability, "machine_capacity",
        "{}",
        disposition.safe_message
    );
    let health = crate::task_diagnostics::derive_workflow_health(
        &waiting,
        &workflow,
        &[],
        None,
        None,
        false,
        None,
    );
    assert_eq!(health.kind, api_types::WorkflowHealthKind::WaitingForAgent);
    assert_eq!(health.severity, api_types::HealthSeverity::Info);
    assert_eq!(health.stale_reason.as_deref(), Some("machine_capacity"));
    assert!(waiting.error_annotation.is_none());
    assert!(waiting.failed_json.is_none());
    let attention: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM attention_projection WHERE scope_type = 'task' AND scope_id = ?",
    )
    .bind(&queued.id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(attention, 0);
    sqlx::query("UPDATE execution SET status = 'completed' WHERE task_id = ?")
        .bind(&running.id)
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks(&project, &workflow)
            .await
            .unwrap(),
        1
    );
    let started = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(started.task_id, queued.id);
    let admitted = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(deferred_dispatch::current_dispatch_disposition(&admitted).is_none());
}

#[tokio::test]
async fn machine_capacity_active_waiter_is_parked_at_final_version() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let workspaces = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo.path()).await;
    let agent_id = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
    let running = seed_task(&db, &project_id, "RUN", "in_progress", 0).await;
    seed_running_execution(&db, &running.id, &agent_id, "coder").await;
    let queued = seed_task(&db, &project_id, "WAIT", "todo", 0).await;
    assign_role(&db, &queued.id, "coder", &agent_id).await;
    db.server_run_cap
        .set(Some(1), 1, &config::embedded_machine_id());
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let revision_before: i64 = sqlx::query_scalar("SELECT list_revision FROM project WHERE id = ?")
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let (dispatcher, mut rx) = build_dispatcher(db.clone(), workspaces.path()).await;
    dispatcher
        .task_service
        .transition(
            queued.id.clone(),
            "in_progress".to_owned(),
            crate::task_service::TransitionOptions {
                version: queued.version,
                reason: Some("manual start".to_owned()),
                triggered_by: Actor::system(SystemComponent::TaskDispatcher),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await
        .unwrap();
    let waiting = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    let marker = deferred_dispatch::current_dispatch_disposition(&waiting).unwrap();
    assert_eq!(marker.capability, "machine_capacity");
    assert_eq!(marker.task_version, waiting.version);
    let health = crate::task_diagnostics::derive_workflow_health(
        &waiting,
        &workflow,
        &[],
        None,
        None,
        false,
        None,
    );
    assert_eq!(health.stale_reason.as_deref(), Some("machine_capacity"));
    let current = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let revision_after: i64 = sqlx::query_scalar("SELECT list_revision FROM project WHERE id = ?")
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert!(revision_after > revision_before);
    let slots = super::slots::load_project_slots(&db, &current)
        .await
        .unwrap();
    assert_eq!((slots.active, slots.parked), (1, 1));
    let batch = super::slots::load_projects_slots(&db, std::slice::from_ref(&current))
        .await
        .unwrap();
    assert_eq!(batch[&project_id].slots, slots);
    assert_eq!(
        batch[&project_id].revision,
        Some((revision_after, current.version))
    );
    assert!(waiting.error_annotation.is_none());
    for _ in 0..4 {
        dispatcher.check_once().await.unwrap();
    }
    let stable = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stable.version, waiting.version);
    sqlx::query("UPDATE execution SET status = 'completed' WHERE task_id = ?")
        .bind(&running.id)
        .execute(db.pool())
        .await
        .unwrap();
    dispatcher.check_once().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .unwrap()
            .unwrap()
            .task_id,
        queued.id
    );
}
#[tokio::test]
async fn machine_capacity_queued_recovery_is_quiet_until_slot_frees() {
    let action = api_types::RecoveryAction::Reexecute;
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id =
        seed_agent_with_executor(&db, 4, DaemonStatus::Online, AgentStatus::Idle, "codex").await;
    let busy = seed_task(&db, &project_id, "occupies machine", "in_progress", 0).await;
    assign_role(&db, &busy.id, "coder", &agent_id).await;
    seed_running_execution(&db, &busy.id, &agent_id, "coder").await;
    db.server_run_cap
        .set(Some(1), 1, &config::embedded_machine_id());

    let task = seed_task(&db, &project_id, "recover", "in_progress", 0).await;
    assign_role(&db, &task.id, "coder", &agent_id).await;
    let stopped = seed_cancelled_execution(
        &db,
        &task.id,
        &agent_id,
        "coder",
        Some(StopReason::UserCancelled),
        Some(ResumePolicy::Manual),
    )
    .await;
    let annotation = serde_json::json!({
        "type": "recovery_required",
        "blocking_reason": "crash_recovery",
        "blocked_execution_id": serde_json::Value::Null,
        "recovery_actions": [action],
    });
    let _ = stopped;
    let task = TaskRepo::update(
        &*db,
        UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(annotation.to_string())),
            blocked_json: Some(Some(
                serde_json::json!({"kind": "recovery_required", "reason": "crash_recovery"})
                    .to_string(),
            )),
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("interruption persists");
    set_prompt_execution_snapshots(&db, &agent_id).await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    let mut events = dispatcher.event_bus.subscribe();
    let queued = dispatcher
        .task_service
        .recover_task(&task.id, action, Some("operator retry".to_owned()), None)
        .await
        .unwrap();
    let marker = deferred_dispatch::queued_recovery(&queued).unwrap();
    while events.try_recv().is_ok() {}
    for _ in 0..4 {
        dispatcher.check_once().await.unwrap();
        let current = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.version, queued.version);
        assert_eq!(
            deferred_dispatch::queued_recovery(&current).unwrap().id,
            marker.id
        );
        assert_eq!(current.metadata_json, queued.metadata_json);
        while let Ok(event) = events.try_recv() {
            assert_ne!(event.entity_id, task.id, "{}", event.event_type);
        }
    }
    sqlx::query("UPDATE execution SET status = 'completed' WHERE task_id = ?")
        .bind(&busy.id)
        .execute(db.pool())
        .await
        .unwrap();
    dispatcher.check_once().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .unwrap()
            .unwrap()
            .task_id,
        task.id
    );
    let started = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(deferred_dispatch::queued_recovery(&started).is_none());
}
