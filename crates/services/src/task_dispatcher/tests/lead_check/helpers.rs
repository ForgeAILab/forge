//! Shared fixtures and helpers for lead check tests.

use super::*;
/// Completes every implementation run at once. A planner run on a root (the
/// kickoff) completes at once too; a planner run on a subtask (the step check)
/// replies with the next scripted reply, and stays running when none is left.
pub(super) struct StepExecutor {
    pub(super) db: Arc<db::SqliteDb>,
    pub(super) replies: Arc<std::sync::Mutex<VecDeque<String>>>,
    /// Keep implementation runs of this Task title running.
    pub(super) hold_coder: Option<&'static str>,
    /// A scripted reply that starts with `@hold:` is given only after this
    /// gets a permit (a step check that is still running).
    pub(super) release: Arc<tokio::sync::Semaphore>,
    /// Bytes every implementation run commits to the shared workspace, in a
    /// file named after its Task (0: it commits nothing).
    pub(super) commit_bytes: Arc<std::sync::atomic::AtomicUsize>,
    pub(super) hold_root_planner: Arc<std::sync::atomic::AtomicBool>,
}

pub(super) fn git_in(worktree: &str, args: &[&str]) {
    let output = std::process::Command::new("git")
        .args(["-c", "user.name=step", "-c", "user.email=step@example.com"])
        .args(args)
        .current_dir(worktree)
        .output()
        .expect("git runs");
    assert!(
        output.status.success(),
        "git {args:?} in {worktree}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[async_trait]
impl TaskExecutor for StepExecutor {
    async fn execute(
        &self,
        ctx: ExecutionContext,
    ) -> std::result::Result<ExecutionResult, ExecutorError> {
        let role = ExecutionRepo::get_by_id(&*self.db, &ctx.execution_id)
            .await
            .ok()
            .flatten()
            .map(|execution| execution.role)
            .unwrap_or_default();
        let task = TaskRepo::get_by_id(&*self.db, &ctx.task_id, false)
            .await
            .ok()
            .flatten()
            .expect("the executed Task exists");
        let completed = |summary: Option<String>| {
            Ok(ExecutionResult {
                status: executors::ExecutionOutcome::Completed,
                summary,
                ..Default::default()
            })
        };
        if role != crate::workflow::default_roles::PLANNER {
            if self.hold_coder == Some(task.title.as_str()) {
                pending::<()>().await;
            }
            let bytes = self.commit_bytes.load(std::sync::atomic::Ordering::SeqCst);
            if bytes > 0 {
                let file = format!("{}.txt", task.title);
                let line = format!("work of {}\n", task.title);
                std::fs::write(
                    Path::new(&ctx.worktree_path).join(&file),
                    line.repeat(bytes / line.len() + 1),
                )
                .expect("the step writes its file");
                git_in(&ctx.worktree_path, &["add", "-A"]);
                git_in(&ctx.worktree_path, &["commit", "-m", &file]);
            }
            return completed(None);
        }
        if task.parent_task_id.is_none() {
            if self
                .hold_root_planner
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                self.release.acquire().await.unwrap().forget();
            }
            return completed(None);
        }
        let reply = self.replies.lock().unwrap().pop_front();
        match reply {
            Some(reply) => {
                if let Some(held) = reply.strip_prefix("@hold:") {
                    self.release.acquire().await.unwrap().forget();
                    return completed(Some(held.to_owned()));
                }
                // The run ends with a session the host can ask again.
                if let Some(said) = reply.strip_prefix("@session:") {
                    return Ok(ExecutionResult {
                        status: executors::ExecutionOutcome::Completed,
                        summary: Some(said.to_owned()),
                        agent_session_id: Some("lead-session".to_owned()),
                        ..Default::default()
                    });
                }
                // A lead that edits the workspace although it may only read.
                if let Some(said) = reply.strip_prefix("@write:") {
                    std::fs::write(
                        Path::new(&ctx.worktree_path).join("lead-was-here.txt"),
                        "the lead wrote this\n",
                    )
                    .expect("the lead writes a file");
                    git_in(&ctx.worktree_path, &["add", "-A"]);
                    return completed(Some(said.to_owned()));
                }
                completed(Some(reply))
            }
            None => {
                pending::<()>().await;
                unreachable!()
            }
        }
    }

    async fn cancel(&self, _execution_id: &str) -> std::result::Result<(), ExecutorError> {
        Ok(())
    }
}

pub(super) struct Steps {
    pub(super) db: Arc<db::SqliteDb>,
    pub(super) project: String,
    pub(super) coder: String,
    pub(super) planner: String,
    pub(super) dispatcher: TaskDispatcher,
    pub(super) release: Arc<tokio::sync::Semaphore>,
    pub(super) commit_bytes: Arc<std::sync::atomic::AtomicUsize>,
    /// The scripted replies no step check has taken yet.
    pub(super) replies: Arc<std::sync::Mutex<VecDeque<String>>>,
    pub(super) hold_root_planner: Arc<std::sync::atomic::AtomicBool>,
    pub(super) _dirs: (TempDir, TempDir),
}

pub(super) const PASS: &str =
    "The step does what it says.\n{\"result\":\"pass\",\"reason\":\"verified\"}";
pub(super) const FAIL: &str =
    "The parser drops the last row.\n{\"result\":\"fail\",\"reason\":\"the last row is dropped\"}";

pub(super) async fn steps(replies: &[&str], hold_coder: Option<&'static str>) -> Steps {
    let db = Arc::new(sqlite_db().await);
    let (repo, workspace) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (project, _) = seed_project_repo(&db, repo.path()).await;
    let coder = seed_agent(&db, 3, DaemonStatus::Online, AgentStatus::Idle).await;
    let planner = crate::test_support::seed_native_agent(&db).await;
    let event_bus = Arc::new(EventBus::new(64));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let commit_bytes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hold_root_planner = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let replies = Arc::new(std::sync::Mutex::new(
        replies
            .iter()
            .map(|reply| (*reply).to_owned())
            .collect::<VecDeque<_>>(),
    ));
    let task_executor: Arc<dyn TaskExecutor> = Arc::new(StepExecutor {
        commit_bytes: Arc::clone(&commit_bytes),
        release: Arc::clone(&release),
        db: Arc::clone(&db),
        replies: Arc::clone(&replies),
        hold_coder,
        hold_root_planner: Arc::clone(&hold_root_planner),
    });
    let task_service = Arc::new(
        TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
            .with_task_executor(task_executor)
            .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
            .with_workspace_root(workspace.path().to_path_buf()),
    );
    let dispatcher = TaskDispatcher::with_check_interval(
        Arc::clone(&db),
        event_bus,
        task_service,
        Duration::from_millis(10),
    );
    Steps {
        db,
        project,
        coder,
        planner,
        dispatcher,
        release,
        commit_bytes,
        replies,
        hold_root_planner,
        _dirs: (repo, workspace),
    }
}

/// A root in `todo` with a default worker and two ordered subtasks.
pub(super) async fn root_with_two_steps(steps: &Steps, with_lead: bool) -> (Task, Task, Task) {
    let root = seed_task(&steps.db, &steps.project, "root", "todo", 1).await;
    let first = seed_subtask(&steps.db, &root, "first", "todo", 0).await;
    let second = seed_subtask(&steps.db, &root, "second", "todo", 1).await;
    assign_role(
        &steps.db,
        &root.id,
        crate::workflow::default_roles::CODER,
        &steps.coder,
    )
    .await;
    if with_lead {
        assign_role(
            &steps.db,
            &root.id,
            crate::workflow::default_roles::PLANNER,
            &steps.planner,
        )
        .await;
    }
    (root, first, second)
}

pub(super) async fn reload(db: &db::SqliteDb, id: &str) -> Task {
    TaskRepo::get_by_id(db, id, false).await.unwrap().unwrap()
}

pub(super) async fn executions(db: &db::SqliteDb, task: &str) -> Vec<(String, String)> {
    sqlx::query_as("SELECT role, status FROM execution WHERE task_id=? ORDER BY created_at, id")
        .bind(task)
        .fetch_all(db.pool())
        .await
        .unwrap()
}

/// `(from, to, bridge kind, rejection)` of every transition of a Task.
pub(super) async fn log(
    db: &db::SqliteDb,
    task: &str,
) -> Vec<(String, String, Option<String>, bool)> {
    sqlx::query_as(
        "SELECT from_state, to_state, bridge_kind, rejection FROM transition_log
         WHERE task_id=? ORDER BY created_at, rowid",
    )
    .bind(task)
    .fetch_all(db.pool())
    .await
    .unwrap()
}

pub(super) async fn count(db: &db::SqliteDb, sql: &str, task: &str) -> i64 {
    sqlx::query_scalar(sql)
        .bind(task)
        .fetch_one(db.pool())
        .await
        .unwrap()
}

pub(super) async fn dump(db: &db::SqliteDb) -> String {
    let tasks: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT title, status, COALESCE(condition_json, '') FROM task ORDER BY created_at",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    let runs: Vec<(String, String, String, Option<String>)> = sqlx::query_as(
        "SELECT t.title, e.role, e.status, e.error FROM execution e JOIN task t ON t.id=e.task_id
         ORDER BY e.created_at",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    let transitions: Vec<(String, String, String, Option<String>, String)> = sqlx::query_as(
        "SELECT t.title, l.from_state, l.to_state, l.bridge_kind, l.trigger_reason
         FROM transition_log l JOIN task t ON t.id=l.task_id ORDER BY l.created_at, l.rowid",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    let queue: Vec<(String, String, Option<String>)> =
        sqlx::query_as("SELECT kind, status, last_error FROM task_step ORDER BY rowid")
            .fetch_all(db.pool())
            .await
            .unwrap();
    format!("tasks: {tasks:#?}\nexecutions: {runs:#?}\ntransitions: {transitions:#?}\nsteps: {queue:#?}")
}

/// Let spawned executor tasks settle, then let the dispatcher act on it.
pub(super) async fn until(steps: &Steps, what: &str, done: impl AsyncFn() -> bool) {
    for _ in 0..240 {
        if done().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
        steps.dispatcher.check_once_and_drain().await.unwrap();
    }
    panic!("timed out waiting until {what}\n{}", dump(&steps.db).await);
}

pub(super) async fn passes(steps: &Steps, count: usize) {
    for _ in 0..count {
        tokio::time::sleep(Duration::from_millis(25)).await;
        steps.dispatcher.check_once_and_drain().await.unwrap();
    }
}

pub(super) fn hop(
    from: &str,
    to: &str,
    bridge: Option<&str>,
    rejection: bool,
) -> (String, String, Option<String>, bool) {
    (
        from.to_owned(),
        to.to_owned(),
        bridge.map(str::to_owned),
        rejection,
    )
}

pub(super) async fn gate_spent(db: &db::SqliteDb, task: &str) -> i64 {
    db::budget::spent(db.pool(), task, &db::budget::gate_key("lead_check"))
        .await
        .unwrap()
}

pub(super) async fn check_runs(db: &db::SqliteDb) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM check_run")
        .fetch_one(db.pool())
        .await
        .unwrap()
}

pub(super) async fn in_progress_root_with_a_step_in_the_gate(
    steps: &Steps,
    with_lead: bool,
) -> (Task, Task) {
    let root = seed_task(&steps.db, &steps.project, "root", "in_progress", 1).await;
    let child = seed_subtask(&steps.db, &root, "first", "lead_check", 0).await;
    assign_role(
        &steps.db,
        &root.id,
        crate::workflow::default_roles::CODER,
        &steps.coder,
    )
    .await;
    if with_lead {
        assign_role(
            &steps.db,
            &root.id,
            crate::workflow::default_roles::PLANNER,
            &steps.planner,
        )
        .await;
    }
    (root, child)
}

// ---------------------------------------------------------------------------
// Review round: the cases the first pass left open.
// ---------------------------------------------------------------------------

pub(super) const HELD_FAIL: &str =
    "@hold:Late findings.\n{\"result\":\"fail\",\"reason\":\"a verdict nobody asked for\"}";
pub(super) const BLOCKED: &str =
    "I could not open the repository.\n{\"result\":\"blocked\",\"reason\":\"no access\"}";

pub(super) async fn running_check(db: &db::SqliteDb, task: &str) -> bool {
    executions(db, task)
        .await
        .last()
        .is_some_and(|run| run == &("planner".to_owned(), "running".to_owned()))
}

pub(super) async fn condition(db: &db::SqliteDb, task: &str) -> String {
    sqlx::query_scalar::<_, String>("SELECT COALESCE(condition_json, '') FROM task WHERE id=?")
        .bind(task)
        .fetch_one(db.pool())
        .await
        .unwrap()
}

/// Open Attention items about `task`, other than the follow-up every
/// completed Task gets.
pub(super) async fn open_attention(steps: &Steps, task: &str) -> i64 {
    let attention = crate::AttentionService::new(
        Arc::clone(&steps.db),
        crate::test_support::noop_wake_turns(),
    );
    while attention.project_once(100).await.unwrap().processed_events > 0 {}
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM attention_projection WHERE status = 'open'
         AND attention_type != 'delivery_followup'
         AND (scope_id = ?1 OR json_extract(details_json, '$.entity_id') = ?1
              OR json_extract(details_json, '$.task_id') = ?1)",
    )
    .bind(task)
    .fetch_one(steps.db.pool())
    .await
    .unwrap()
}

// ---------------------------------------------------------------------------
// Stage 3 follow-ups.
// ---------------------------------------------------------------------------

pub(super) const HELD_PASS: &str = "@hold:Checked.\n{\"result\":\"pass\",\"reason\":\"verified\"}";

/// Change `agent` with `set` at the moment a run of `task` has its execution
/// row and workspace grant, before `start_execution` verifies the grant.
pub(super) async fn at_admission_before_launch(steps: &Steps, task: &str, agent: &str, set: &str) {
    sqlx::query(&format!(
        "CREATE TRIGGER change_agent_before_launch AFTER INSERT ON workspace_lease
         WHEN NEW.task_id='{task}'
         BEGIN UPDATE agent_identity SET {set}, version=version+1 WHERE id='{agent}'; END"
    ))
    .execute(steps.db.pool())
    .await
    .unwrap();
}

pub(super) fn parked_on_paused(stored: &str, agent: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(stored).is_ok_and(|stored| {
        stored["kind"] == "parked"
            && stored["primary"]["kind"] == "agent"
            && stored["primary"]["agent_id"] == agent
            && stored["primary"]["status"] == "paused"
    })
}

pub(super) async fn check_prompt(db: &db::SqliteDb, task: &str) -> String {
    sqlx::query_scalar::<_, Option<String>>(
        "SELECT prompt FROM execution WHERE task_id=? AND role='planner' ORDER BY created_at DESC, id DESC LIMIT 1",
    )
    .bind(task)
    .fetch_one(db.pool())
    .await
    .unwrap()
    .unwrap_or_default()
}

pub(super) async fn seed_finished_check(steps: &Steps, task: &Task, reply: &str) -> String {
    let project_version: i64 = sqlx::query_scalar("SELECT version FROM project WHERE id=?")
        .bind(&steps.project)
        .fetch_one(steps.db.pool())
        .await
        .unwrap();
    let now = now_rfc3339();
    let id = new_uuid_v4();
    ExecutionRepo::create(
        &*steps.db,
        db::CreateExecution {
            id: id.clone(),
            task_id: task.id.clone(),
            agent_id: Some(steps.planner.clone()),
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
            summary: Some(reply.to_owned()),
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
    .expect("the finished check is stored");
    id
}

pub(super) async fn verdict_comments(db: &db::SqliteDb, task: &str) -> i64 {
    count(
        db,
        "SELECT COUNT(*) FROM task_comment WHERE task_id=? AND idempotency_key LIKE 'step-check:%'",
        task,
    )
    .await
}
