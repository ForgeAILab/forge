use super::*;
use crate::terminal_service::TerminalActivityTracker;
use crate::workflow::default_roles;
use crate::workspace_execution_lock::WorkspaceExecutionLockManager;
use ::workspace::RepoCacheLockManager;
use api_types::{
    CanonicalPhase, FailurePolicy, HookAudience, HookSpec, StateDefinition, StateHooks, StateKind,
    WorkflowDefinition, WorkflowTrigger, WorkflowTriggerDefinition,
};
use async_trait::async_trait;
use db::{
    create_sqlite_pool, run_migrations, AgentRepo, AgentStatus, CreateAgent,
    CreateProjectAgentBinding, CreateTask, DaemonRepo, DaemonStatus, ProjectAgentBindingRepo,
    ReplaceProjectAgentBinding, UpdateProject, UpsertDaemon,
};
use executors::{ExecutionResult, ExecutorError};
use sqlx::Row;
use tempfile::TempDir;

struct NoDiffExecutor;

#[async_trait]
impl TaskExecutor for NoDiffExecutor {
    async fn execute(
        &self,
        _ctx: ExecutionContext,
    ) -> std::result::Result<ExecutionResult, ExecutorError> {
        Ok(ExecutionResult {
            status: ExecutionOutcome::Completed,
            after_sha: None,
            agent_session_id: None,
            summary: None,
            error: None,
            usage: None,
            ..Default::default()
        })
    }

    async fn cancel(&self, _execution_id: &str) -> std::result::Result<(), ExecutorError> {
        Ok(())
    }
}

struct UncommittedWorktreeFailureExecutor;

#[async_trait]
impl TaskExecutor for UncommittedWorktreeFailureExecutor {
    async fn execute(
        &self,
        ctx: ExecutionContext,
    ) -> std::result::Result<ExecutionResult, ExecutorError> {
        tokio::fs::write(
            std::path::Path::new(&ctx.worktree_path).join("uncommitted.txt"),
            "preserve me\n",
        )
        .await?;
        Ok(ExecutionResult {
            status: ExecutionOutcome::Failed,
            after_sha: git::get_current_sha(std::path::Path::new(&ctx.worktree_path))
                .await
                .ok(),
            agent_session_id: Some("native-task-session".to_owned()),
            error: Some(
                ExecutorError::Other(
                    crate::embedded_task_executor::UNCOMMITTED_WORKTREE_FAILURE.to_owned(),
                )
                .to_string(),
            ),
            ..Default::default()
        })
    }

    async fn cancel(&self, _execution_id: &str) -> std::result::Result<(), ExecutorError> {
        Ok(())
    }
}

struct ExecutorUnavailableExecutor;

#[async_trait]
impl TaskExecutor for ExecutorUnavailableExecutor {
    async fn execute(
        &self,
        _ctx: ExecutionContext,
    ) -> std::result::Result<ExecutionResult, ExecutorError> {
        Ok(ExecutionResult {
            status: ExecutionOutcome::Failed,
            error: Some("provider unavailable after exhausting runtime retry attempts".to_owned()),
            failure_class: Some(executors::ExecutionFailureClass::ExecutorUnavailable),
            retry_after: Some(std::time::Duration::from_secs(60)),
            ..ExecutionResult::default()
        })
    }

    async fn cancel(&self, _execution_id: &str) -> std::result::Result<(), ExecutorError> {
        Ok(())
    }
}

struct BurstLogExecutor {
    count: u64,
}

#[async_trait]
impl TaskExecutor for BurstLogExecutor {
    async fn execute(
        &self,
        ctx: ExecutionContext,
    ) -> std::result::Result<ExecutionResult, ExecutorError> {
        if let Some(sender) = ctx.log_sender.as_ref() {
            for index in 0..self.count {
                let _ = sender.send(executors::LogEntry {
                    schema_version: 1,
                    sequence: index,
                    timestamp: format!("2026-05-05T00:00:{:02}Z", index % 60),
                    execution_id: ctx.execution_id.clone(),
                    kind: executors::LogKind::Stdout,
                    stream: executors::LogStream::Main,
                    payload: json!({ "line": index }),
                    truncated: false,
                });
            }
        }
        Ok(ExecutionResult {
            status: ExecutionOutcome::Completed,
            after_sha: None,
            agent_session_id: None,
            summary: None,
            error: None,
            usage: None,
            ..Default::default()
        })
    }

    async fn cancel(&self, _execution_id: &str) -> std::result::Result<(), ExecutorError> {
        Ok(())
    }
}

#[derive(Default)]
struct RecordingCancelExecutor {
    cancelled: std::sync::Mutex<Vec<String>>,
}

#[async_trait]
impl TaskExecutor for RecordingCancelExecutor {
    async fn execute(
        &self,
        _ctx: ExecutionContext,
    ) -> std::result::Result<ExecutionResult, ExecutorError> {
        Err(ExecutorError::Other(
            "execute should not be called".to_owned(),
        ))
    }

    async fn cancel(&self, execution_id: &str) -> std::result::Result<(), ExecutorError> {
        self.cancelled
            .lock()
            .expect("cancel log lock")
            .push(execution_id.to_owned());
        Ok(())
    }
}

#[derive(Default)]
struct CountingExecutor {
    calls: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl TaskExecutor for CountingExecutor {
    async fn execute(
        &self,
        _ctx: ExecutionContext,
    ) -> std::result::Result<ExecutionResult, ExecutorError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(ExecutionResult {
            status: ExecutionOutcome::Completed,
            after_sha: None,
            agent_session_id: None,
            summary: None,
            error: None,
            usage: None,
            ..Default::default()
        })
    }

    async fn cancel(&self, _execution_id: &str) -> std::result::Result<(), ExecutorError> {
        Ok(())
    }
}

async fn sqlite_db() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    SqliteDb::new(pool)
}

async fn seed_project_repo(db: &SqliteDb) -> (String, String, TempDir) {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let repo_dir = TempDir::new().expect("repo temp dir creates");
    let default_branch = setup_test_git_repo(repo_dir.path());
    db::ProjectRepo::create(
        db,
        db::CreateProject {
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
    db::RepoRepo::create(
        db,
        db::CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "forge".to_owned(),
            remote_url: repo_dir.path().to_string_lossy().into_owned(),
            local_path: Some(repo_dir.path().to_string_lossy().into_owned()),
            work_mode: db::WorkMode::DirectMerge,
            default_branch,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("repo creates");
    db::ProjectRepo::update(
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
    (project_id, repo_id, repo_dir)
}

fn setup_test_git_repo(path: &std::path::Path) -> String {
    run_git(path, &["init"]);
    run_git(path, &["config", "user.email", "test@forge.dev"]);
    run_git(path, &["config", "user.name", "Forge Test"]);
    std::fs::write(path.join("README.md"), "# Forge\n").expect("README writes");
    run_git(path, &["add", "-A"]);
    run_git(path, &["commit", "-m", "init"]);
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
        .expect("git command runs");
    assert!(
        output.status.success(),
        "git {:?} failed\nstdout:\n{}\nstderr:\n{}",
        args,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("git stdout utf8")
        .trim()
        .to_owned()
}

async fn seed_agent(db: &SqliteDb) -> String {
    seed_shell_agent_with_config(db, "{}").await
}

async fn seed_shell_agent_with_config(db: &SqliteDb, config_json: &str) -> String {
    seed_agent_with_executor_type(db, "shell", config_json).await
}

async fn seed_agent_with_executor_type(
    db: &SqliteDb,
    executor_type: &str,
    config_json: &str,
) -> String {
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
        db::UpdateDaemonReport {
            id: daemon_id.clone(),
            detected_clis_json: format!(
                r#"[{{"kind":"{executor_type}","availability":"authenticated"}}]"#
            ),
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
            name: executor_type.to_owned(),
            description: None,
            executor_type: executor_type.to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: r#"["rust"]"#.to_owned(),
            config_json: config_json.to_owned(),
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
    let project_ids: Vec<String> = sqlx::query_scalar("SELECT id FROM project")
        .fetch_all(db.pool())
        .await
        .expect("test project ids load");
    for project_id in project_ids {
        crate::test_support::configure_project_execution_test_setup(
            db,
            &project_id,
            &agent_id,
            &agent_id,
        )
        .await;
    }
    agent_id
}

async fn seed_task_with_status(
    db: &SqliteDb,
    project_id: &str,
    repo_id: &str,
    status: TaskStatus,
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
            title: format!("task-{status}"),
            description: Some("seeded task".to_owned()),
            task_type: "task".to_owned(),
            status,
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
    .expect("task creates")
}

async fn seed_subtask_with_status(
    db: &SqliteDb,
    parent: &Task,
    title: &str,
    status: TaskStatus,
    subtask_order: i64,
) -> Task {
    let now = now_rfc3339();
    TaskRepo::create(
        db,
        CreateTask {
            id: new_uuid_v4(),
            project_id: parent.project_id.clone(),
            repo_id: parent.repo_id.clone(),
            parent_task_id: Some(parent.id.clone()),
            subtask_order: Some(subtask_order),
            assignee_type: None,
            assignee_id: None,
            title: title.to_owned(),
            description: Some("seeded subtask".to_owned()),
            task_type: "task".to_owned(),
            status,
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
    .expect("subtask creates")
}

async fn seed_ordered_sequence_started(db: &SqliteDb, task: &Task) {
    let mut metadata = task.metadata().expect("task metadata parses");
    metadata.ordered_sequence_started = Some(true);
    TaskRepo::set_metadata_json(db, &task.id, metadata.to_json(), &now_rfc3339())
        .await
        .expect("task metadata updates");
}

fn role_assignment_input(
    task_id: &str,
    role_name: &str,
    agent_id: Option<String>,
    user_id: Option<String>,
) -> db::CreateTaskRoleAssignment {
    let now = now_rfc3339();
    let (assignee_type, assignee_id) = if let Some(agent_id) = agent_id {
        (db::AssigneeKind::Agent, agent_id)
    } else {
        (
            db::AssigneeKind::User,
            user_id.expect("user assignment includes id"),
        )
    };
    db::CreateTaskRoleAssignment {
        id: new_uuid_v4(),
        task_id: task_id.to_owned(),
        role_name: role_name.to_owned(),
        assignee_type: Some(assignee_type),
        assignee_id: Some(assignee_id),
        created_at: now.clone(),
        updated_at: now,
    }
}

fn hook(action: &str) -> HookSpec {
    HookSpec {
        action: action.to_owned(),
        params: json!({}),
        applies_to: HookAudience::All,
        on_failure: FailurePolicy::Log,
    }
}

fn workflow_state(
    name: &str,
    kind: StateKind,
    role: Option<&str>,
    hooks: StateHooks,
) -> StateDefinition {
    StateDefinition {
        name: name.to_owned(),
        kind,
        column: name.to_owned(),
        display_name: name.to_owned(),
        role: role.map(str::to_owned),
        hooks,
        cleanup: None,
        canonical_phase: Some(match kind {
            StateKind::Backlog => CanonicalPhase::Backlog,
            StateKind::Initial => CanonicalPhase::Ready,
            StateKind::Active => CanonicalPhase::Working,
            StateKind::Gate => CanonicalPhase::Working,
            StateKind::Terminal => CanonicalPhase::Done,
            StateKind::Custom => CanonicalPhase::Working,
        }),
        gate_config: None,
        dispatch: None,
        triggers: std::collections::BTreeMap::new(),
        config: json!({}),
    }
}

fn implicit_assignee_workflow() -> WorkflowDefinition {
    let mut todo = workflow_state("todo", StateKind::Initial, None, StateHooks::default());
    todo.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "in_progress".to_owned(),
            dispatch: None,
        },
    );
    let mut in_progress = workflow_state(
        "in_progress",
        StateKind::Active,
        None,
        StateHooks {
            on_enter: vec![hook("dispatch_role_agent")],
            ..StateHooks::default()
        },
    );
    in_progress.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "done".to_owned(),
            dispatch: None,
        },
    );
    WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            todo,
            in_progress,
            workflow_state("done", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    }
}

async fn update_project_workflow(db: &SqliteDb, project_id: &str, workflow: &WorkflowDefinition) {
    sqlx::query("UPDATE project SET workflow_definition = ?, updated_at = ? WHERE id = ?")
        .bind(serde_json::to_string(workflow).expect("workflow serializes"))
        .bind(now_rfc3339())
        .bind(project_id)
        .execute(db.pool())
        .await
        .expect("project workflow updates");
}

async fn update_project_default_roles(db: &SqliteDb, project_id: &str, agent_id: &str) {
    let settings = json!({
        "default_role_assignments": [
            {"role_name": default_roles::PLANNER, "assignee_type": "agent", "assignee_id": agent_id},
            {"role_name": default_roles::CODER, "assignee_type": "agent", "assignee_id": agent_id},
            {"role_name": default_roles::REVIEWER, "assignee_type": "agent", "assignee_id": agent_id}
        ]
    });
    sqlx::query("UPDATE project SET settings = ?, updated_at = ? WHERE id = ?")
        .bind(settings.to_string())
        .bind(now_rfc3339())
        .bind(project_id)
        .execute(db.pool())
        .await
        .expect("project settings updates");
}

async fn wait_until_execution_has_logs_path(db: &SqliteDb, execution_id: &str) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
    loop {
        let execution = ExecutionRepo::get_by_id(db, execution_id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        if execution.logs_path.is_some() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "execution did not reach pre-launch setup"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

async fn seed_running_coder_execution(
    db: &SqliteDb,
    task_id: &str,
    agent_id: Option<String>,
    workspace_id: Option<String>,
) -> Execution {
    seed_running_role_execution(db, task_id, agent_id, "executor", workspace_id).await
}

async fn seed_running_role_execution(
    db: &SqliteDb,
    task_id: &str,
    agent_id: Option<String>,
    role: &str,
    workspace_id: Option<String>,
) -> Execution {
    let now = now_rfc3339();
    ExecutionRepo::create(
        db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            agent_id,
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
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("running execution creates")
}

async fn seed_workspace_for_task(
    db: &SqliteDb,
    task: &Task,
    workspace_root: &std::path::Path,
) -> String {
    let now = now_rfc3339();
    let workspace_id = new_uuid_v4();
    let worktree_path = workspace_root.join(&task.id).join("forge");
    WorkspaceRepo::create(
        db,
        CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task.id.clone(),
            repo_id: task.repo_id.clone().unwrap(),
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch: ::workspace::task_branch_name(&task.id),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("workspace creates");
    std::fs::create_dir_all(&worktree_path).expect("worktree dir creates");
    workspace_id
}

async fn next_role_reassigned_event(
    rx: &mut tokio::sync::broadcast::Receiver<ForgeEvent>,
) -> ForgeEvent {
    loop {
        let event = rx.recv().await.expect("event receives");
        if event.event_type == "task.role_reassigned" {
            return event;
        }
    }
}

#[tokio::test]
async fn add_user_comment_indexes_comment_memory_item() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "todo".to_owned()).await;

    let comment = service
        .add_user_comment(
            &task.id,
            "Mai".to_owned(),
            "remember this user-provided context".to_owned(),
        )
        .await
        .expect("user comment creates");

    assert_eq!(comment.author_type, db::CommentAuthorType::User);
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM memory_item \
         WHERE project_id = ? AND task_id = ? AND source_type = 'comment' \
           AND kind = 'comment' AND metadata_json = ?",
    )
    .bind(&project_id)
    .bind(&task.id)
    .bind(json!({ "source_ref": comment.id }).to_string())
    .fetch_one(db.pool())
    .await
    .expect("comment memory item count loads");
    assert_eq!(count, 1);
}

mod cases;
