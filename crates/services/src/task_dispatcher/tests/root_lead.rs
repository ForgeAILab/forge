//! A coordination root runs its planner (the lead) before its subtasks: the
//! root moves to its planning state at kickoff, its subtasks are held with a
//! typed park while it plans, and they start once the planner succeeded. A
//! root without a planner moves straight to its implementation state.
use super::*;

/// Completes a planner execution at once and leaves every other role running.
struct LeadExecutor {
    db: Arc<db::SqliteDb>,
    sender: mpsc::UnboundedSender<ExecutionContext>,
    fail_planner: bool,
}

#[async_trait]
impl TaskExecutor for LeadExecutor {
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
        let _ = self.sender.send(ctx);
        if role == crate::workflow::default_roles::PLANNER {
            if self.fail_planner {
                return Err(ExecutorError::Other("planner failed".to_owned()));
            }
            return Ok(ExecutionResult {
                status: executors::ExecutionOutcome::Completed,
                ..Default::default()
            });
        }
        pending::<()>().await;
        unreachable!()
    }

    async fn cancel(&self, _execution_id: &str) -> std::result::Result<(), ExecutorError> {
        Ok(())
    }
}

struct Lead {
    db: Arc<db::SqliteDb>,
    project: String,
    coder: String,
    planner: String,
    dispatcher: TaskDispatcher,
    rx: mpsc::UnboundedReceiver<ExecutionContext>,
    _dirs: (TempDir, TempDir),
}

async fn lead_fixture(planner_completes: bool) -> Lead {
    let db = Arc::new(sqlite_db().await);
    let (repo, workspace) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (project, _) = seed_project_repo(&db, repo.path()).await;
    let coder = seed_agent(&db, 3, DaemonStatus::Online, AgentStatus::Idle).await;
    let planner = crate::test_support::seed_native_agent(&db).await;
    let event_bus = Arc::new(EventBus::new(64));
    let (tx, rx) = mpsc::unbounded_channel();
    let task_executor: Arc<dyn TaskExecutor> = if planner_completes {
        Arc::new(LeadExecutor {
            db: Arc::clone(&db),
            sender: tx,
            fail_planner: false,
        })
    } else {
        Arc::new(RecordingExecutor { sender: tx })
    };
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
    Lead {
        db,
        project,
        coder,
        planner,
        dispatcher,
        rx,
        _dirs: (repo, workspace),
    }
}

async fn reload(db: &db::SqliteDb, id: &str) -> Task {
    TaskRepo::get_by_id(db, id, false).await.unwrap().unwrap()
}

async fn executions(db: &db::SqliteDb, task: &str) -> Vec<(String, String)> {
    sqlx::query_as("SELECT role, status FROM execution WHERE task_id=? ORDER BY created_at, id")
        .bind(task)
        .fetch_all(db.pool())
        .await
        .unwrap()
}

async fn park(db: &db::SqliteDb, task: &str) -> String {
    sqlx::query_scalar::<_, String>("SELECT reason_json FROM task_schedule_park WHERE task_id=?")
        .bind(task)
        .fetch_optional(db.pool())
        .await
        .unwrap()
        .unwrap_or_default()
}

async fn passes(lead: &Lead, count: usize) {
    for _ in 0..count {
        lead.dispatcher.check_once_and_drain().await.unwrap();
    }
}

/// Wait for the spawned executor task to settle its execution, then let the
/// dispatcher act on what it left.
async fn until(lead: &Lead, what: &str, done: impl AsyncFn() -> bool) {
    for _ in 0..200 {
        if done().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
        lead.dispatcher.check_once_and_drain().await.unwrap();
    }
    let tasks: Vec<(String, String, Option<String>, Option<String>, String)> = sqlx::query_as(
        "SELECT title, status, metadata_json, error_annotation, condition_json FROM task ORDER BY created_at",
    )
    .fetch_all(lead.db.pool())
    .await
    .unwrap();
    let runs: Vec<(String, String, String, Option<String>)> =
        sqlx::query_as("SELECT task_id, role, status, error FROM execution ORDER BY created_at")
            .fetch_all(lead.db.pool())
            .await
            .unwrap();
    let steps: Vec<(String, String, String, Option<String>)> = sqlx::query_as(
        "SELECT kind, status, substr(payload_json, 1, 200), last_error FROM task_step ORDER BY rowid",
    )
    .fetch_all(lead.db.pool())
    .await
    .unwrap();
    panic!("timed out waiting until {what}\ntasks: {tasks:#?}\nexecutions: {runs:#?}\nsteps: {steps:#?}");
}

async fn root_with_child(lead: &Lead, with_planner: bool) -> (Task, Task) {
    let root = seed_task(&lead.db, &lead.project, "root", "todo", 1).await;
    let child = seed_subtask(&lead.db, &root, "child", "todo", 0).await;
    assign_role(
        &lead.db,
        &root.id,
        crate::workflow::default_roles::CODER,
        &lead.coder,
    )
    .await;
    if with_planner {
        assign_role(
            &lead.db,
            &root.id,
            crate::workflow::default_roles::PLANNER,
            &lead.planner,
        )
        .await;
    }
    (reload(&lead.db, &root.id).await, child)
}

/// The failing-first test of stage 1. While the root plans, its subtask is
/// held with a typed park and has no execution; the planner's success moves
/// the root to its implementation state and the subtask is dispatched.
#[tokio::test]
async fn a_coordination_root_runs_its_planner_before_its_subtasks() {
    let lead = lead_fixture(false).await;
    let (root, child) = root_with_child(&lead, true).await;
    passes(&lead, 3).await;

    let planning = reload(&lead.db, &root.id).await;
    assert_eq!(planning.status, "planning");
    assert_eq!(
        executions(&lead.db, &root.id).await,
        vec![("planner".to_owned(), "running".to_owned())]
    );
    let held = reload(&lead.db, &child.id).await;
    assert_eq!(held.status, "todo");
    assert_eq!(executions(&lead.db, &child.id).await, Vec::new());
    let reason = park(&lead.db, &child.id).await;
    assert!(
        reason.contains("ParentWait") && reason.contains("\"planning\""),
        "the held subtask names its planning root: {reason}"
    );
    assert!(
        matches!(
            held.condition.reasons().next(),
            Some(db::ParkReason::Parent { parent_id, cause })
                if *parent_id == root.id && cause == "planning"
        ),
        "{:?}",
        held.condition
    );
    // The hold is stable: further passes start nothing and write nothing.
    passes(&lead, 3).await;
    assert_eq!(reload(&lead.db, &child.id).await.version, held.version);
    assert_eq!(executions(&lead.db, &root.id).await.len(), 1);
}

#[tokio::test]
async fn a_planner_that_succeeds_releases_the_subtasks() {
    let mut lead = lead_fixture(true).await;
    let (root, child) = root_with_child(&lead, true).await;
    until(&lead, "the root reaches implementation", async || {
        reload(&lead.db, &root.id).await.status == "in_progress"
    })
    .await;
    until(&lead, "the subtask runs", async || {
        executions(&lead.db, &child.id).await == vec![("coder".to_owned(), "running".to_owned())]
    })
    .await;
    assert_eq!(
        executions(&lead.db, &root.id).await,
        vec![("planner".to_owned(), "completed".to_owned())]
    );
    assert_eq!(reload(&lead.db, &child.id).await.status, "in_progress");
    // The planner ran in the root workspace and the subtask reuses it.
    let planner_ctx = tokio::time::timeout(Duration::from_secs(5), lead.rx.recv())
        .await
        .expect("planner context arrives")
        .expect("planner context");
    let coder_ctx = tokio::time::timeout(Duration::from_secs(5), lead.rx.recv())
        .await
        .expect("coder context arrives")
        .expect("coder context");
    assert_eq!(planner_ctx.task_id, root.id);
    assert_eq!(coder_ctx.task_id, child.id);
    assert_eq!(planner_ctx.worktree_path, coder_ctx.worktree_path);
    let workspaces: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM workspace WHERE task_id IN (?, ?)")
            .bind(&root.id)
            .bind(&child.id)
            .fetch_one(lead.db.pool())
            .await
            .unwrap();
    assert_eq!(workspaces, 1, "one root-owned workspace");
    // The root waits on its children and never runs the planner again.
    passes(&lead, 3).await;
    assert_eq!(executions(&lead.db, &root.id).await.len(), 1);
    assert!(park(&lead.db, &root.id).await.contains("Children"));
}

/// No lead is a first-class mode: the root moves to its implementation state
/// when its first subtask can start, and no planner ever runs on it.
#[tokio::test]
async fn a_root_without_a_planner_moves_to_implementation_and_starts_its_subtask() {
    let lead = lead_fixture(false).await;
    let (root, child) = root_with_child(&lead, false).await;
    passes(&lead, 3).await;
    assert_eq!(reload(&lead.db, &root.id).await.status, "in_progress");
    assert_eq!(executions(&lead.db, &root.id).await, Vec::new());
    assert_eq!(
        executions(&lead.db, &child.id).await,
        vec![("coder".to_owned(), "running".to_owned())]
    );
    let log: Vec<(String, String)> = sqlx::query_as(
        "SELECT from_state, to_state FROM transition_log WHERE task_id=? ORDER BY created_at, rowid",
    )
    .bind(&root.id)
    .fetch_all(lead.db.pool())
    .await
    .unwrap();
    assert_eq!(
        log.last().map(|(_, to)| to.as_str()),
        Some("in_progress"),
        "{log:?}"
    );
}
