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

/// The lead cancels (here: the owner cancels) the last incomplete subtask
/// while the root plans. The terminal subtask runs the root advance directly;
/// it must leave the root in planning with its planner, and must not hop on
/// to aggregate review.
#[tokio::test]
async fn cancelling_the_last_subtask_while_the_root_plans_keeps_the_planner() {
    let lead = lead_fixture(false).await;
    let (root, child) = root_with_child(&lead, true).await;
    passes(&lead, 3).await;
    assert_eq!(reload(&lead.db, &root.id).await.status, "planning");

    let held = reload(&lead.db, &child.id).await;
    lead.dispatcher
        .task_service
        .perform_task_action(
            &child.id,
            api_types::TaskAction::Cancel { reason: None },
            held.version,
        )
        .await
        .expect("a held subtask is cancelled");
    passes(&lead, 4).await;

    assert_eq!(reload(&lead.db, &child.id).await.status, "cancelled");
    let planning = reload(&lead.db, &root.id).await;
    assert_eq!(planning.status, "planning", "no hop to aggregate review");
    assert!(
        !crate::task_service::coordination_review_pending(&planning),
        "a flag left set would starve the planner: {:?}",
        planning.metadata_json
    );
    assert_eq!(
        executions(&lead.db, &root.id).await,
        vec![("planner".to_owned(), "running".to_owned())]
    );
    let log: Vec<(String, String)> = sqlx::query_as(
        "SELECT from_state, to_state FROM transition_log WHERE task_id=? ORDER BY created_at, rowid",
    )
    .bind(&root.id)
    .fetch_all(lead.db.pool())
    .await
    .unwrap();
    assert_eq!(log, vec![("todo".to_owned(), "planning".to_owned())]);
}

/// With every subtask already terminal, a root that has a lead still goes
/// through its planner before aggregate review.
#[tokio::test]
async fn a_planner_that_succeeds_with_nothing_left_sends_the_root_to_review() {
    let lead = lead_fixture(true).await;
    let root = seed_task(&lead.db, &lead.project, "root", "todo", 1).await;
    seed_subtask(&lead.db, &root, "child", "cancelled", 0).await;
    assign_role(
        &lead.db,
        &root.id,
        crate::workflow::default_roles::PLANNER,
        &lead.planner,
    )
    .await;
    let entries = async || -> Vec<(String, String)> {
        sqlx::query_as(
            "SELECT from_state, to_state FROM transition_log WHERE task_id=? ORDER BY created_at, rowid",
        )
        .bind(&root.id)
        .fetch_all(lead.db.pool())
        .await
        .unwrap()
    };
    until(&lead, "the root reaches aggregate review", async || {
        entries().await.iter().any(|(_, to)| to == "review")
    })
    .await;
    // With no reviewer assigned the review gate passes on by itself; the
    // path up to it is what this stage decides.
    let log = entries().await;
    assert_eq!(
        log[..3],
        [
            ("todo".to_owned(), "planning".to_owned()),
            ("planning".to_owned(), "in_progress".to_owned()),
            ("in_progress".to_owned(), "review".to_owned()),
        ]
    );
    assert_eq!(
        executions(&lead.db, &root.id).await[0],
        ("planner".to_owned(), "completed".to_owned())
    );
}

/// Holding a root that plans stops its planner and names the hold on its
/// subtask; the release runs the planner again.
#[tokio::test]
async fn a_hold_and_release_in_planning_stops_and_reruns_the_planner() {
    let lead = lead_fixture(false).await;
    let (root, child) = root_with_child(&lead, true).await;
    passes(&lead, 3).await;
    let planning = reload(&lead.db, &root.id).await;
    assert_eq!(planning.status, "planning");
    let service = &lead.dispatcher.task_service;

    service
        .perform_task_action(
            &root.id,
            api_types::TaskAction::Hold { reason: None },
            planning.version,
        )
        .await
        .expect("a planning root is held");
    passes(&lead, 3).await;
    let held = reload(&lead.db, &root.id).await;
    assert_eq!(held.status, "planning");
    assert!(
        !executions(&lead.db, &root.id)
            .await
            .iter()
            .any(|(_, status)| status == "running"),
        "the hold stops the planner"
    );
    let reason = park(&lead.db, &child.id).await;
    assert!(
        reason.contains("ParentWait") && reason.contains("\"held\""),
        "held takes precedence over planning: {reason}"
    );
    assert_eq!(executions(&lead.db, &child.id).await, Vec::new());

    let offers = service.test_action_values(&root.id).await.unwrap();
    assert!(
        offers.contains(&api_types::TaskAction::Release { reason: None }),
        "a held planning root offers its release: {offers:?}"
    );
    service
        .perform_task_action(
            &root.id,
            api_types::TaskAction::Release { reason: None },
            held.version,
        )
        .await
        .expect("the root is released");
    passes(&lead, 4).await;
    assert_eq!(reload(&lead.db, &root.id).await.status, "planning");
    let runs = executions(&lead.db, &root.id).await;
    assert_eq!(
        runs.iter().filter(|(_, status)| status == "running").count(),
        1,
        "the release runs the planner again: {runs:?}"
    );
    assert_eq!(executions(&lead.db, &child.id).await, Vec::new());
    assert!(park(&lead.db, &child.id).await.contains("\"planning\""));
}

/// Kickoff does not take a Project slot: a root that moved to its
/// implementation state counts for nothing by itself, and a root whose lead
/// runs holds one slot, as any running Task does.
#[tokio::test]
async fn kickoff_leaves_the_project_slot_count_to_running_work() {
    let lead = lead_fixture(false).await;
    let project = set_project_active_task_limit(&lead.db, &lead.project, 5).await;
    let slots = async || {
        let project = ProjectRepo::get_by_id(&*lead.db, &project.id)
            .await
            .unwrap()
            .unwrap();
        let slots = crate::task_dispatcher::slots::load_project_slots(&lead.db, &project)
            .await
            .unwrap();
        (slots.active, slots.queued)
    };
    // No lead: the root is in_progress, its one running subtask is the slot.
    let (plain, plain_child) = root_with_child(&lead, false).await;
    assert_eq!(slots().await, (0, 2));
    passes(&lead, 3).await;
    assert_eq!(reload(&lead.db, &plain.id).await.status, "in_progress");
    assert_eq!(executions(&lead.db, &plain_child.id).await.len(), 1);
    assert_eq!(slots().await, (1, 0), "the root holds no slot of its own");

    // With a lead: the planning root's running planner is one slot, and its
    // held subtask is still queued.
    let (led, _) = root_with_child(&lead, true).await;
    passes(&lead, 3).await;
    assert_eq!(reload(&lead.db, &led.id).await.status, "planning");
    assert_eq!(slots().await, (2, 1));
}

/// Upgrade: a root left in `todo` by the previous release while its subtask
/// was already running. The first pass moves the root; the running subtask
/// is not interrupted and nothing is started twice.
#[tokio::test]
async fn a_root_left_in_todo_with_a_running_subtask_is_moved_without_interrupting_it() {
    let lead = lead_fixture(false).await;
    let root = seed_task(&lead.db, &lead.project, "root", "todo", 1).await;
    let child = seed_subtask(&lead.db, &root, "child", "in_progress", 0).await;
    let later = seed_subtask(&lead.db, &root, "later", "todo", 1).await;
    assign_role(
        &lead.db,
        &root.id,
        crate::workflow::default_roles::CODER,
        &lead.coder,
    )
    .await;
    let now = now_rfc3339();
    let running = ExecutionRepo::create(
        &*lead.db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: child.id.clone(),
            agent_id: Some(lead.coder.clone()),
            role: crate::workflow::default_roles::CODER.to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: Some(now.clone()),
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                serde_json::json!({
                    "executor_type": "shell",
                    "config": {},
                    "task_state": "in_progress",
                })
                .to_string(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("the running execution of the previous release");

    passes(&lead, 4).await;
    assert_eq!(reload(&lead.db, &root.id).await.status, "in_progress");
    assert_eq!(executions(&lead.db, &root.id).await, Vec::new());
    assert_eq!(
        executions(&lead.db, &child.id).await,
        vec![("coder".to_owned(), "running".to_owned())],
        "the running subtask is neither stopped nor started again"
    );
    let still = ExecutionRepo::get_by_id(&*lead.db, &running.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(still.status, ExecutionStatus::Running);
    assert_eq!(reload(&lead.db, &child.id).await.status, "in_progress");
    assert_eq!(executions(&lead.db, &later.id).await, Vec::new());
}
