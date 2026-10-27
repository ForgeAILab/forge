//! Shared fixtures and helpers for task dispatcher tests.

use super::*;
#[async_trait]
pub(super) trait DispatcherDrain {
    async fn drain_steps(&self) -> crate::Result<()>;
    async fn check_once_and_drain(&self) -> crate::Result<u64>;
    async fn dispatch_initial_tasks_and_drain(
        &self,
        project: &db::Project,
        workflow: &api_types::WorkflowDefinition,
    ) -> crate::Result<u64>;
}
#[async_trait]
impl DispatcherDrain for TaskDispatcher {
    async fn drain_steps(&self) -> crate::Result<()> {
        for _ in 0..100 {
            let ids: Vec<String> = sqlx::query_scalar("SELECT DISTINCT task_id FROM task_step WHERE status IN ('pending','claimed') ORDER BY task_id")
                .fetch_all(self.db.pool()).await?;
            if ids.is_empty() {
                return Ok(());
            }
            for id in ids {
                self.task_service.drain(&id).await?;
            }
        }
        Err(crate::ServiceError::invalid_operation(
            "dispatcher test steps did not settle",
        ))
    }
    async fn check_once_and_drain(&self) -> crate::Result<u64> {
        let count = self.check_once().await?;
        self.drain_steps().await?;
        Ok(count)
    }
    async fn dispatch_initial_tasks_and_drain(
        &self,
        p: &db::Project,
        w: &api_types::WorkflowDefinition,
    ) -> crate::Result<u64> {
        let count = self.dispatch_initial_tasks(p, w).await?;
        self.drain_steps().await?;
        Ok(count)
    }
}

pub(super) struct RecordingExecutor {
    pub(super) sender: mpsc::UnboundedSender<ExecutionContext>,
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

pub(super) async fn sqlite_db() -> db::SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    db::SqliteDb::new(pool)
}

pub(super) fn setup_git_repo(path: &Path) -> String {
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

pub(super) fn run_git(path: &Path, args: &[&str]) -> String {
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

pub(super) async fn seed_project_repo(db: &db::SqliteDb, repo_path: &Path) -> (String, String) {
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

pub(super) async fn seed_agent(
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

pub(super) async fn seed_agent_with_executor(
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
pub(super) async fn set_prompt_execution_snapshots(db: &db::SqliteDb, agent_id: &str) {
    sqlx::query("UPDATE execution SET executor_config_snapshot_json = json_set(executor_config_snapshot_json, '$.executor_type', 'codex') WHERE agent_id = ?")
        .bind(agent_id).execute(db.pool()).await.unwrap();
}

pub(super) async fn seed_task(
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

pub(super) async fn seed_subtask(
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

pub(super) async fn assign_role(db: &db::SqliteDb, task_id: &str, role_name: &str, agent_id: &str) {
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

pub(super) async fn set_review_ci_config(db: &db::SqliteDb, task: &Task) -> Task {
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

pub(super) async fn set_planning_gate_auto_approval(db: &db::SqliteDb, project_id: &str) {
    set_planning_gate_user_approval(db, project_id, false).await;
}

pub(super) async fn set_planning_gate_user_approval(
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

pub(super) async fn seed_running_review(
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

pub(super) async fn seed_completed_coder_execution(db: &db::SqliteDb, task_id: &str) -> String {
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

pub(super) async fn seed_completed_reviewer_execution(
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

pub(super) async fn seed_running_execution(
    db: &db::SqliteDb,
    task_id: &str,
    agent_id: &str,
    role: &str,
) {
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

pub(super) async fn seed_cancelled_execution(
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
pub(super) const DISPATCH_WAIT_ATTEMPTS: usize = 40;
pub(super) const DISPATCH_WAIT_STEP: Duration = Duration::from_millis(250);

/// Why a dispatch never arrived, in the dispatcher's own terms. A parked Task
/// and a starved one look identical from a bare timeout, and only the first is
/// a product failure.
pub(super) async fn describe_stalled_dispatch(db: &db::SqliteDb, task_id: &str) -> String {
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

pub(in crate::task_dispatcher) struct FixtureWorker {
    pub(super) stop: tokio::sync::watch::Sender<bool>,
    pub(super) handle: tokio::task::JoinHandle<()>,
}
impl Drop for FixtureWorker {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        self.handle.abort();
    }
}

pub(super) async fn build_dispatcher(
    db: Arc<db::SqliteDb>,
    workspace_root: &Path,
) -> (TaskDispatcher, mpsc::UnboundedReceiver<ExecutionContext>) {
    build_dispatcher_runtime(db, workspace_root, false).await
}
pub(super) async fn build_dispatcher_runtime(
    db: Arc<db::SqliteDb>,
    workspace_root: &Path,
    start_worker: bool,
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
    let dispatcher = TaskDispatcher::with_check_interval(
        db,
        event_bus,
        task_service.clone(),
        Duration::from_millis(10),
    );
    if start_worker {
        let (stop, shutdown) = tokio::sync::watch::channel(false);
        let _ = dispatcher.fixture_worker.set(FixtureWorker {
            stop,
            handle: task_service.task_step_worker().start(shutdown),
        });
    }
    (dispatcher, rx)
}

/// A bare Project with no repository, as it looks before provisioning
/// attaches one — the shape `dispatcher_pauses_a_project_with_no_repository`
/// and its neighbors exercise.
pub(super) async fn seed_unprovisioned_project(db: &db::SqliteDb, name: &str) -> String {
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

pub(super) struct MergeGateFixture {
    pub(super) db: Arc<db::SqliteDb>,
    pub(super) _repo_dir: TempDir,
    pub(super) _workspace_dir: TempDir,
    pub(super) task: Task,
    pub(super) merge_service: Arc<crate::merge_service::MergeService>,
    pub(super) dispatcher: TaskDispatcher,
}

pub(super) struct FailedReviewFixture {
    pub(super) db: Arc<db::SqliteDb>,
    pub(super) task: Task,
    pub(super) dispatcher: TaskDispatcher,
    pub(super) _repo_dir: TempDir,
}

pub(super) async fn failed_review_fixture(
    failed_ago: chrono::Duration,
    budget: i32,
) -> FailedReviewFixture {
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
            bridge: Default::default(),
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

pub(super) async fn merge_gate_fixture(entered_ago: chrono::Duration) -> MergeGateFixture {
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
            bridge: Default::default(),
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

pub(super) async fn seed_capacity_recovery(
    db: &Arc<db::SqliteDb>,
    dispatcher: &TaskDispatcher,
    project_id: &str,
    action: api_types::TaskAction,
) -> (db::Task, String, String) {
    let agent_id = seed_agent(db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let busy = seed_task(db, project_id, "occupies capacity", "in_progress", 0).await;
    seed_running_execution(db, &busy.id, &agent_id, "coder").await;
    crate::test_support::set_test_agent_capacity(db, &agent_id, 1).await;
    let task = seed_task(db, project_id, "recover", "in_progress", 0).await;
    assign_role(db, &task.id, "coder", &agent_id).await;
    let blocked_execution_id = if action
        == (api_types::TaskAction::Retry {
            reason: None,
            fresh_session: Some(false),
            refresh_workspace: None,
            reset_budget: None,
            guidance: None,
        }) {
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
        .test_apply_action(&task.id, action, None, None)
        .await
        .expect("recovery queues");
    (queued, agent_id, busy.id)
}

/// Attach an approved Charter so the Project becomes `charter_backed` and
/// implementation Tasks derive their authority from that Charter.
pub(super) async fn make_project_charter_backed(db: &db::SqliteDb, project_id: &str) {
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

pub(super) async fn make_task_charter_governed(db: &db::SqliteDb, task: &Task) {
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

pub(super) async fn seed_failed_coder_execution(
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

pub(super) async fn set_zero_execution_retry_budget(db: &db::SqliteDb, task: &Task) -> Task {
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

pub(super) async fn set_task_error_annotation(db: &db::SqliteDb, task: &Task, annotation: &str) {
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

pub(super) async fn refuse_subtask_merge_target(db: &db::SqliteDb, project_id: &str) {
    let mut workflow = WorkflowEngine::resolve_workflow("{}");
    let index = workflow
        .states
        .iter()
        .position(|s| s.name == "merging")
        .unwrap();
    let mut merging = workflow.states.remove(index);
    merging.triggers.remove(&api_types::WorkflowTrigger::Accept);
    // No implicit next-state Accept either: every named state is valid, but
    // this merge state has no landed-merge edge and refuses the fallback Done.
    workflow.states.push(merging);
    let project = ProjectRepo::get_by_id(db, project_id)
        .await
        .unwrap()
        .unwrap();
    ProjectRepo::update_workflow(
        db,
        project_id,
        &serde_json::to_string(&workflow).unwrap(),
        None,
        project.version,
        &now_rfc3339(),
    )
    .await
    .unwrap();
}

pub(super) async fn seed_environment_pause(
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

pub(super) async fn task_versions(db: &db::SqliteDb, tasks: &[Task]) -> Vec<i64> {
    let mut versions = Vec::new();
    for task in tasks {
        versions.push(
            TaskRepo::get_by_id(db, &task.id, false)
                .await
                .unwrap()
                .unwrap()
                .version,
        );
    }
    versions
}

pub(super) async fn finish_environment_recheck(dispatcher: &TaskDispatcher, project_id: &str) {
    let job = dispatcher
        .environment_rechecks
        .lock()
        .unwrap()
        .remove(project_id)
        .unwrap();
    // The job ends when its check does (released by the test, or at the
    // check's own timeout): no second clock here.
    job.await.unwrap().unwrap();
}

/// The Project's re-check job is still running: its check is blocked on a
/// file only the test writes, so a pass that returned while this holds did
/// not wait for the check.
pub(super) fn environment_recheck_runs(dispatcher: &TaskDispatcher, project_id: &str) -> bool {
    dispatcher
        .environment_rechecks
        .lock()
        .unwrap()
        .get(project_id)
        .is_some_and(|job| !job.is_finished())
}

pub(super) async fn set_project_active_task_limit(
    db: &db::SqliteDb,
    project_id: &str,
    limit: u32,
) -> Project {
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

pub(super) async fn machine_capacity_interrupted_task(
    db: &db::SqliteDb,
    project_id: &str,
    agent_id: &str,
    title: &str,
) -> Task {
    let task = seed_task(db, project_id, title, "in_progress", 0).await;
    assign_role(db, &task.id, "coder", agent_id).await;
    seed_cancelled_execution(
        db,
        &task.id,
        agent_id,
        "coder",
        Some(StopReason::UserCancelled),
        Some(ResumePolicy::Manual),
    )
    .await;
    let annotation = serde_json::json!({
        "type": "recovery_required",
        "blocking_reason": "crash_recovery",
        "blocked_execution_id": serde_json::Value::Null,
        "recovery_actions": ["reexecute"],
    });
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
    .expect("interruption persists")
}

pub(super) async fn machine_capacity_task(db: &db::SqliteDb, task_id: &str) -> Task {
    TaskRepo::get_by_id(db, task_id, false)
        .await
        .unwrap()
        .unwrap()
}

#[derive(Clone)]
pub(super) struct MachineCapacityWriteCounter(pub(super) Arc<std::sync::atomic::AtomicUsize>);
impl tracing::Subscriber for MachineCapacityWriteCounter {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        if event.metadata().target() == "forge_db::write_transaction" {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

/// The fixture's Task as a passed review leaves it.
pub(super) async fn review_passed(fixture: &MergeGateFixture) {
    let now = now_rfc3339();
    TaskRepo::set_review_passed_at(&*fixture.db, &fixture.task.id, Some(now.clone()), &now)
        .await
        .unwrap();
}

/// Enter `merging` again, so the entry's hooks step (admission to the
/// integration queue) is enqueued for the fixture's Task.
pub(super) async fn enter_merging(fixture: &MergeGateFixture) {
    let project = ProjectRepo::get_by_id(&*fixture.db, &fixture.task.project_id)
        .await
        .unwrap()
        .unwrap();
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let task = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    fixture
        .dispatcher
        .task_service
        .workflow_execution()
        .manual_override_transition_with_authority(
            &task.id,
            "merging",
            task.version,
            &workflow,
            api_types::Actor::system(api_types::SystemComponent::TaskDispatcher),
            "resume integration",
            false,
            Some(crate::workflow::engine::WorkflowAuthority {
                project_version: project.version,
                workflow_definition: project.workflow_definition,
                clear_review_passed_at_on_commit: false,
            }),
        )
        .await
        .unwrap();
}
