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

pub(super) struct Lead {
    pub(super) db: Arc<db::SqliteDb>,
    pub(super) project: String,
    pub(super) coder: String,
    pub(super) planner: String,
    pub(super) dispatcher: TaskDispatcher,
    pub(super) rx: mpsc::UnboundedReceiver<ExecutionContext>,
    _dirs: (TempDir, TempDir),
}

pub(super) async fn lead_fixture(planner_completes: bool) -> Lead {
    fixture(planner_completes, false).await
}

pub(super) async fn fixture(planner_completes: bool, fail_planner: bool) -> Lead {
    fixture_with(|db, tx| {
        if planner_completes || fail_planner {
            Arc::new(LeadExecutor {
                db,
                sender: tx,
                fail_planner,
            })
        } else {
            Arc::new(RecordingExecutor { sender: tx })
        }
    })
    .await
}

/// The fixture with an executor the caller builds (stage 4 scripts what the
/// lead's run does).
pub(super) async fn fixture_with(
    executor: impl FnOnce(
        Arc<db::SqliteDb>,
        mpsc::UnboundedSender<ExecutionContext>,
    ) -> Arc<dyn TaskExecutor>,
) -> Lead {
    let db = Arc::new(sqlite_db().await);
    let (repo, workspace) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (project, _) = seed_project_repo(&db, repo.path()).await;
    let coder = seed_agent(&db, 3, DaemonStatus::Online, AgentStatus::Idle).await;
    let planner = crate::test_support::seed_native_agent(&db).await;
    let event_bus = Arc::new(EventBus::new(64));
    let (tx, rx) = mpsc::unbounded_channel();
    let task_executor = executor(Arc::clone(&db), tx);
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

pub(super) async fn park(db: &db::SqliteDb, task: &str) -> String {
    sqlx::query_scalar::<_, String>("SELECT reason_json FROM task_schedule_park WHERE task_id=?")
        .bind(task)
        .fetch_optional(db.pool())
        .await
        .unwrap()
        .unwrap_or_default()
}

pub(super) async fn passes(lead: &Lead, count: usize) {
    for _ in 0..count {
        lead.dispatcher.check_once_and_drain().await.unwrap();
    }
}

/// Wait for the spawned executor task to settle its execution, then let the
/// dispatcher act on what it left.
pub(super) async fn until(lead: &Lead, what: &str, done: impl AsyncFn() -> bool) {
    for _ in 0..600 {
        if done().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
        lead.dispatcher.check_once_and_drain().await.unwrap();
    }
    let tasks: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT title, status, COALESCE(condition_json, '') FROM task ORDER BY created_at",
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

pub(super) async fn root_with_child(lead: &Lead, with_planner: bool) -> (Task, Task) {
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
        runs.iter()
            .filter(|(_, status)| status == "running")
            .count(),
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

// ---------------------------------------------------------------------------
// Review round: upgrade states, roots without a lead, and liveness.
// ---------------------------------------------------------------------------

async fn state_dump(db: &db::SqliteDb) -> String {
    let tasks: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT title, status, COALESCE(condition_json, '') FROM task ORDER BY created_at",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    let runs: Vec<(String, String, String, Option<String>)> =
        sqlx::query_as("SELECT task_id, role, status, error FROM execution ORDER BY created_at")
            .fetch_all(db.pool())
            .await
            .unwrap();
    let parks: Vec<(String, String)> =
        sqlx::query_as("SELECT task_id, reason_json FROM task_schedule_park")
            .fetch_all(db.pool())
            .await
            .unwrap();
    format!("tasks: {tasks:#?}\nexecutions: {runs:#?}\nparks: {parks:#?}")
}

/// The default workflow with its planning gate requiring a person's approval
/// and, when `only_through_planning`, no direct `todo -> in_progress` edge.
async fn set_approval_planning_workflow(
    db: &db::SqliteDb,
    project: &str,
    only_through_planning: bool,
) {
    let mut workflow = crate::workflow::default_workflow::default_workflow();
    for state in &mut workflow.states {
        if state.name == "planning" {
            state.gate_config.as_mut().unwrap().requires_user_approval = Some(true);
        }
        if state.name == "todo" && only_through_planning {
            state
                .triggers
                .retain(|_, trigger| trigger.to != "in_progress");
        }
    }
    sqlx::query("UPDATE project SET workflow_definition = ?, workflow_template_name = 'default', updated_at = ? WHERE id = ?")
        .bind(serde_json::to_string(&workflow).unwrap())
        .bind(now_rfc3339())
        .bind(project)
        .execute(db.pool())
        .await
        .unwrap();
}

/// A root that waits on an unfinished dependency holds its subtasks with a
/// typed park, and the dependency's completion starts it with nobody's help.
#[tokio::test]
async fn a_root_behind_a_dependency_starts_when_the_dependency_completes() {
    let lead = lead_fixture(false).await;
    let prerequisite = seed_task(&lead.db, &lead.project, "prerequisite", "backlog", 1).await;
    let (root, child) = root_with_child(&lead, false).await;
    db::TaskDependencyRepo::add_dependency(&*lead.db, &root.id, &prerequisite.id, &now_rfc3339())
        .await
        .unwrap();
    passes(&lead, 4).await;
    eprintln!("REPORT dependency before:\n{}", state_dump(&lead.db).await);
    assert_eq!(executions(&lead.db, &child.id).await, Vec::new());
    assert_ne!(
        park(&lead.db, &root.id).await,
        "",
        "the waiting root has a park"
    );
    assert_ne!(
        park(&lead.db, &child.id).await,
        "",
        "the held subtask has a park"
    );

    sqlx::query("UPDATE task SET status='done', version=version+1 WHERE id=?")
        .bind(&prerequisite.id)
        .execute(lead.db.pool())
        .await
        .unwrap();
    let done = reload(&lead.db, &prerequisite.id).await;
    lead.dispatcher
        .task_service
        .wake_dependents_of_completed_task(&done)
        .await
        .unwrap();
    passes(&lead, 4).await;
    eprintln!("REPORT dependency after:\n{}", state_dump(&lead.db).await);
    assert_eq!(reload(&lead.db, &root.id).await.status, "in_progress");
    assert_eq!(
        executions(&lead.db, &child.id).await,
        vec![("coder".to_owned(), "running".to_owned())]
    );
}

/// A root without a lead never stops at a planning gate, whether the gate
/// asks for a person's approval or is the only way to implementation.
#[tokio::test]
async fn a_root_without_a_lead_does_not_stop_at_an_approval_planning_gate() {
    for (only_through_planning, start) in [
        (false, "todo"),
        (true, "todo"),
        (true, "planning"),
        (false, "planning"),
    ] {
        let lead = lead_fixture(false).await;
        set_approval_planning_workflow(&lead.db, &lead.project, only_through_planning).await;
        let root = seed_task(&lead.db, &lead.project, "root", start, 1).await;
        let child = seed_subtask(&lead.db, &root, "child", "todo", 0).await;
        assign_role(&lead.db, &root.id, "coder", &lead.coder).await;
        passes(&lead, 4).await;
        let dump = state_dump(&lead.db).await;
        let moved = reload(&lead.db, &root.id).await;
        assert_eq!(
            moved.status, "in_progress",
            "{only_through_planning} {start}\n{dump}"
        );
        assert!(
            !moved
                .metadata_json
                .as_deref()
                .unwrap_or("")
                .contains("\"awaiting_human\":true"),
            "{only_through_planning} {start}\n{dump}"
        );
        assert_eq!(executions(&lead.db, &root.id).await, Vec::new(), "{dump}");
        assert_eq!(
            executions(&lead.db, &child.id).await,
            vec![("coder".to_owned(), "running".to_owned())],
            "{only_through_planning} {start}\n{dump}"
        );
    }
}

/// Kickoff is fast: the root's move marks its subtask for the dispatcher, so
/// the subtask starts from the dirty set alone, with no sweep and no re-read
/// of every Task.
#[tokio::test]
async fn kickoff_reaches_the_subtask_from_the_dirty_set_alone() {
    let lead = lead_fixture(false).await;
    // The first direct pass of an instance reads every Task; spend it.
    passes(&lead, 1).await;
    let (root, child) = root_with_child(&lead, false).await;
    let mut needed = 0;
    for pass in 1..=6 {
        lead.dispatcher.startup_reconcile().await.unwrap();
        lead.dispatcher.drain_steps().await.unwrap();
        if !executions(&lead.db, &child.id).await.is_empty() {
            needed = pass;
            break;
        }
    }
    eprintln!("REPORT dirty-set passes to dispatch the subtask: {needed}");
    assert_eq!(reload(&lead.db, &root.id).await.status, "in_progress");
    assert!(
        (1..=2).contains(&needed),
        "dirty-only passes: {needed}\n{}",
        state_dump(&lead.db).await
    );
}

/// Upgrade states of a root without a lead that must not change: a backlog
/// root still holds its subtasks, and a paused Project starts nothing until it
/// resumes. (A `todo` root offers no hold, before and after this change.)
#[tokio::test]
async fn upgrade_states_that_held_subtasks_before_still_hold_and_release() {
    // Backlog.
    let lead = lead_fixture(false).await;
    let root = seed_task(&lead.db, &lead.project, "root", "backlog", 1).await;
    let child = seed_subtask(&lead.db, &root, "child", "todo", 0).await;
    assign_role(&lead.db, &root.id, "coder", &lead.coder).await;
    passes(&lead, 3).await;
    assert_eq!(reload(&lead.db, &root.id).await.status, "backlog");
    assert_eq!(executions(&lead.db, &child.id).await, Vec::new());
    assert_ne!(park(&lead.db, &child.id).await, "");

    // Paused Project.
    let lead = lead_fixture(false).await;
    let (root, child) = root_with_child(&lead, false).await;
    ProjectRepo::set_paused_at(&*lead.db, &lead.project, Some(now_rfc3339()))
        .await
        .unwrap();
    passes(&lead, 3).await;
    assert_eq!(reload(&lead.db, &root.id).await.status, "todo");
    assert_eq!(executions(&lead.db, &child.id).await, Vec::new());
    ProjectRepo::set_paused_at(&*lead.db, &lead.project, None)
        .await
        .unwrap();
    passes(&lead, 4).await;
    assert_eq!(
        reload(&lead.db, &root.id).await.status,
        "in_progress",
        "{}",
        state_dump(&lead.db).await
    );
    assert_eq!(executions(&lead.db, &child.id).await.len(), 1);
}

/// Removing the lead stops its planning run before releasing the subtasks.
#[tokio::test]
async fn removing_the_lead_while_it_plans_stops_the_planner_and_starts_the_subtask() {
    let lead = lead_fixture(false).await;
    let (root, child) = root_with_child(&lead, true).await;
    passes(&lead, 3).await;
    assert_eq!(reload(&lead.db, &root.id).await.status, "planning");
    lead.dispatcher
        .task_service
        .remove_role(&root.id, "planner", false, false)
        .await
        .unwrap();
    until(
        &lead,
        "the child runs without the removed lead",
        async || !executions(&lead.db, &child.id).await.is_empty(),
    )
    .await;
    assert_eq!(reload(&lead.db, &root.id).await.status, "in_progress");
    assert_eq!(
        executions(&lead.db, &root.id).await,
        [("planner".to_owned(), "cancelled".to_owned())]
    );
    assert_eq!(
        executions(&lead.db, &child.id).await,
        [("coder".to_owned(), "running".to_owned())]
    );
}

/// A planner that fails ends in a typed park on the root with a working
/// retry, and its subtasks name the blocked root: nothing waits unseen.
#[tokio::test]
async fn a_planner_that_fails_parks_the_root_and_its_subtask_with_a_way_out() {
    let lead = fixture(false, true).await;
    let (root, child) = root_with_child(&lead, true).await;
    until(&lead, "the planner run fails", async || {
        executions(&lead.db, &root.id)
            .await
            .iter()
            .any(|(_, status)| status == "failed")
    })
    .await;
    passes(&lead, 3).await;
    let dump = state_dump(&lead.db).await;
    eprintln!("REPORT planner failed:\n{dump}");
    let failed = reload(&lead.db, &root.id).await;
    assert_eq!(failed.status, "planning", "{dump}");
    assert_eq!(executions(&lead.db, &child.id).await, Vec::new(), "{dump}");
    // The root waits for its retry deadline or shows its failure; either way
    // it has an owner, and so has its subtask.
    assert!(
        !park(&lead.db, &root.id).await.is_empty()
            || executions(&lead.db, &root.id)
                .await
                .iter()
                .any(|(_, status)| status == "running"),
        "{dump}"
    );
    let reason = park(&lead.db, &child.id).await;
    assert!(
        reason.contains("\"planning\"") || reason.contains("\"blocked\""),
        "{dump}"
    );
}

/// A lead on a planning gate that asks for a person's approval: the root
/// waits for that approval with a typed park and an offered action, and the
/// approval starts the subtasks.
#[tokio::test]
async fn a_lead_on_an_approval_gate_waits_for_the_approval_and_then_starts() {
    let lead = lead_fixture(true).await;
    set_approval_planning_workflow(&lead.db, &lead.project, false).await;
    let (root, child) = root_with_child(&lead, true).await;
    until(&lead, "the root waits for plan approval", async || {
        reload(&lead.db, &root.id)
            .await
            .metadata_json
            .as_deref()
            .unwrap_or("")
            .contains("plan_review")
    })
    .await;
    passes(&lead, 3).await;
    let dump = state_dump(&lead.db).await;
    eprintln!("REPORT approval gate with lead:\n{dump}");
    let waiting = reload(&lead.db, &root.id).await;
    assert_eq!(waiting.status, "planning", "{dump}");
    assert_eq!(
        executions(&lead.db, &root.id).await.len(),
        1,
        "no planner relaunch: {dump}"
    );
    assert_eq!(executions(&lead.db, &child.id).await, Vec::new(), "{dump}");
    let service = &lead.dispatcher.task_service;
    let offers = service.test_action_values(&root.id).await.unwrap();
    eprintln!("REPORT approval offers: {offers:?}");
    let approve = offers
        .iter()
        .find(|offer| matches!(offer, api_types::TaskAction::Approve { .. }))
        .cloned()
        .unwrap_or_else(|| {
            panic!("a root waiting for plan approval offers approve: {offers:?}\n{dump}")
        });
    service
        .perform_task_action(&root.id, approve, waiting.version)
        .await
        .expect("the plan is approved");
    until(&lead, "the subtask runs after approval", async || {
        !executions(&lead.db, &child.id).await.is_empty()
    })
    .await;
    assert_eq!(reload(&lead.db, &root.id).await.status, "in_progress");
}

/// The lead's Agent is paused while the root plans and the planner has
/// stopped: the root is parked on its Agent, the subtask on its root, and
/// resuming the Agent runs the planner again.
#[tokio::test]
async fn a_paused_lead_agent_parks_the_root_until_it_resumes() {
    let lead = lead_fixture(false).await;
    let (root, child) = root_with_child(&lead, true).await;
    db::AgentRepo::set_paused(&*lead.db, &lead.planner, true)
        .await
        .unwrap();
    passes(&lead, 4).await;
    let dump = state_dump(&lead.db).await;
    eprintln!("REPORT lead agent paused:\n{dump}");
    assert_eq!(
        reload(&lead.db, &root.id).await.status,
        "planning",
        "{dump}"
    );
    assert_eq!(executions(&lead.db, &root.id).await, Vec::new(), "{dump}");
    assert_ne!(park(&lead.db, &root.id).await, "", "{dump}");
    assert!(
        park(&lead.db, &child.id).await.contains("\"planning\""),
        "{dump}"
    );
    db::AgentRepo::set_paused(&*lead.db, &lead.planner, false)
        .await
        .unwrap();
    passes(&lead, 4).await;
    assert_eq!(
        executions(&lead.db, &root.id).await,
        vec![("planner".to_owned(), "running".to_owned())],
        "{}",
        state_dump(&lead.db).await
    );
}

/// A user stops the running planner: the root is parked for the user with a
/// retry, and the retry runs the planner again.
#[tokio::test]
async fn a_stopped_planner_parks_the_root_with_a_retry() {
    let lead = lead_fixture(false).await;
    let (root, child) = root_with_child(&lead, true).await;
    passes(&lead, 3).await;
    let execution: String = sqlx::query_scalar("SELECT id FROM execution WHERE task_id=?")
        .bind(&root.id)
        .fetch_one(lead.db.pool())
        .await
        .unwrap();
    let service = &lead.dispatcher.task_service;
    service
        .stop_execution(execution, "stopped by its owner".to_owned())
        .await
        .expect("planner stops");
    passes(&lead, 4).await;
    let dump = state_dump(&lead.db).await;
    eprintln!("REPORT planner stopped:\n{dump}");
    let stopped = reload(&lead.db, &root.id).await;
    assert_eq!(stopped.status, "planning", "{dump}");
    assert_ne!(park(&lead.db, &root.id).await, "", "{dump}");
    assert_ne!(park(&lead.db, &child.id).await, "", "{dump}");
    assert_eq!(executions(&lead.db, &child.id).await, Vec::new(), "{dump}");
    let offers = service.test_action_values(&root.id).await.unwrap();
    eprintln!("REPORT planner stopped offers: {offers:?}");
    let retry = offers
        .iter()
        .find(|offer| {
            matches!(
                offer,
                api_types::TaskAction::Retry { .. } | api_types::TaskAction::Release { .. }
            )
        })
        .cloned()
        .unwrap_or_else(|| panic!("a stopped planning root offers a way on: {offers:?}\n{dump}"));
    service
        .perform_task_action(&root.id, retry, stopped.version)
        .await
        .expect("the root continues");
    passes(&lead, 4).await;
    let runs = executions(&lead.db, &root.id).await;
    assert_eq!(
        runs.iter()
            .filter(|(_, status)| status == "running")
            .count(),
        1,
        "{runs:?}\n{}",
        state_dump(&lead.db).await
    );
}

/// Every (root state x first-subtask state x lead / no lead) a live database
/// can hold at upgrade settles into owned work: after the dispatcher's
/// passes every Task that is not settled has a step, a live execution or a
/// park (the sweep invariant), no subtask runs under a root that is not
/// implementing, a root and its subtasks run one execution at most, and a
/// root without a lead never plans and never rests before implementation
/// while it has a subtask to run.
#[tokio::test]
async fn upgrade_matrix_every_root_and_subtask_state_is_owned() {
    let mut table = String::new();
    for root_status in ["backlog", "todo", "planning", "in_progress"] {
        for first in ["todo", "in_progress", "done", "cancelled"] {
            for with_lead in [false, true] {
                let lead = lead_fixture(false).await;
                let root = seed_task(&lead.db, &lead.project, "root", root_status, 1).await;
                let one = seed_subtask(&lead.db, &root, "first", first, 0).await;
                let two = seed_subtask(&lead.db, &root, "later", "todo", 1).await;
                assign_role(&lead.db, &root.id, "coder", &lead.coder).await;
                if with_lead {
                    assign_role(&lead.db, &root.id, "planner", &lead.planner).await;
                }
                passes(&lead, 5).await;
                let case = format!("root={root_status} first={first} lead={with_lead}");
                let dump = state_dump(&lead.db).await;
                lead.dispatcher
                    .sweep_and_assert()
                    .await
                    .unwrap_or_else(|error| panic!("{case}: {error}\n{dump}"));
                let now = reload(&lead.db, &root.id).await;
                let running = |runs: &[(String, String)]| {
                    runs.iter()
                        .filter(|(_, status)| status == "running")
                        .count()
                };
                let root_runs = executions(&lead.db, &root.id).await;
                let child_running = running(&executions(&lead.db, &one.id).await)
                    + running(&executions(&lead.db, &two.id).await);
                table.push_str(&format!(
                    "{case} -> root={} root_runs={root_runs:?} child_running={child_running}\n",
                    now.status
                ));
                assert!(
                    running(&root_runs) + child_running <= 1,
                    "{case}: one execution at most\n{dump}"
                );
                assert!(
                    child_running == 0 || now.status == "in_progress",
                    "{case}: a subtask runs only under an implementing root\n{dump}"
                );
                for task in [&root, &one, &two] {
                    let task = reload(&lead.db, &task.id).await;
                    if matches!(task.status.as_str(), "done" | "cancelled") {
                        continue;
                    }
                    assert!(
                        !park(&lead.db, &task.id).await.is_empty()
                            || running(&executions(&lead.db, &task.id).await) == 1,
                        "{case}: {} has an owner\n{dump}",
                        task.title
                    );
                }
                if root_status == "backlog" {
                    assert_eq!(now.status, "backlog", "{case}\n{dump}");
                    assert_eq!(child_running, 0, "{case}\n{dump}");
                    continue;
                }
                if with_lead && root_status != "in_progress" {
                    // Kickoff: the lead plans first.
                    assert_eq!(now.status, "planning", "{case}\n{dump}");
                    assert_eq!(running(&root_runs), 1, "{case}\n{dump}");
                } else {
                    // No lead (or already implementing): the next subtask
                    // runs with nobody's help.
                    assert_eq!(now.status, "in_progress", "{case}\n{dump}");
                    assert!(root_runs.is_empty(), "{case}\n{dump}");
                    assert_eq!(child_running, 1, "{case}\n{dump}");
                }
            }
        }
    }
    eprintln!("REPORT upgrade matrix:\n{table}");
}

/// All-cancelled is a complete sequence. The aggregate gate is entered in
/// both modes. Missing work parks for the owner, or spends the lead re-plan
/// budget; an unassigned, check-less gate keeps its configured skip.
#[tokio::test]
async fn all_cancelled_subtasks_enter_aggregate_review_with_and_without_a_lead() {
    for with_lead in [false, true] {
        for with_reviewer in [false, true] {
            let lead = lead_fixture(true).await;
            let root = seed_task(&lead.db, &lead.project, "root", "todo", 1).await;
            for order in 0..2 {
                seed_subtask(&lead.db, &root, "cancelled step", "cancelled", order).await;
            }
            if with_lead {
                assign_role(&lead.db, &root.id, "planner", &lead.planner).await;
            }
            if with_reviewer {
                assign_role(&lead.db, &root.id, "reviewer", &lead.coder).await;
            }
            until(&lead, "the aggregate review gate is entered", async || {
                super::root_replan::log(&lead, &root.id)
                    .await
                    .iter()
                    .any(|(_, to, _)| to == "review")
            })
            .await;
            if with_reviewer {
                // No implementation ever ran: review is entered, but the
                // configured reviewer has no candidate to review.
                until(
                    &lead,
                    "the missing aggregate candidate is stated",
                    async || {
                        reload(&lead.db, &root.id)
                            .await
                            .condition
                            .reasons()
                            .any(|reason| {
                                if with_lead {
                                    matches!(
                                        reason,
                                        db::ParkReason::BudgetExhausted {
                                            budget_kind: Some(db::budget::Kind::Replan),
                                            ..
                                        }
                                    )
                                } else {
                                    matches!(
                                        reason,
                                        db::ParkReason::Failure {
                                            failure_kind: api_types::FailureKind::ReviewBlocked
                                        }
                                    )
                                }
                            })
                    },
                )
                .await;
                let task = reload(&lead.db, &root.id).await;
                assert_eq!(task.status, "review");
                assert!(task
                    .error_annotation
                    .as_deref()
                    .unwrap_or_default()
                    .contains("every subtask was cancelled"));
                assert!(ReviewRepo::list_by_task(&*lead.db, &root.id)
                    .await
                    .unwrap()
                    .is_empty());
            } else {
                until(&lead, "the unconfigured review passes", async || {
                    super::root_replan::log(&lead, &root.id)
                        .await
                        .iter()
                        .any(|(from, to, _)| from == "review" && to == "merging")
                })
                .await;
                assert!(
                    ReviewRepo::list_by_task(&*lead.db, &root.id)
                        .await
                        .unwrap()
                        .is_empty(),
                    "an ordinary empty gate creates no Review attempt"
                );
            }
            passes(&lead, 4).await;
            let hops = super::root_replan::log(&lead, &root.id).await;
            assert_eq!(
                hops.iter().filter(|(_, to, _)| to == "review").count(),
                if with_lead && with_reviewer {
                    usize::try_from(db::budget::Kind::Replan.default_limit()).unwrap() + 1
                } else {
                    1
                },
                "{hops:?}"
            );
            assert!(
                hops.iter()
                    .filter(|(_, to, _)| to == "merging")
                    .all(|(from, _, _)| from == "review"),
                "{hops:?}"
            );
            let runs = executions(&lead.db, &root.id).await;
            assert_eq!(
                runs.iter().filter(|(role, _)| role == "planner").count(),
                usize::from(with_lead)
                    + if with_lead && with_reviewer {
                        usize::try_from(db::budget::Kind::Replan.default_limit()).unwrap()
                    } else {
                        0
                    }
            );
            assert_eq!(
                runs.iter().filter(|(role, _)| role == "reviewer").count(),
                0,
                "cancelled work supplied no candidate"
            );
            let child_runs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution e JOIN task t ON e.task_id=t.id WHERE t.parent_task_id=?")
                .bind(&root.id).fetch_one(lead.db.pool()).await.unwrap();
            assert_eq!(child_runs, 0);
        }
    }
}

struct PausedPlanningExecutor {
    db: Arc<db::SqliteDb>,
    sender: mpsc::UnboundedSender<ExecutionContext>,
    finish: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

#[async_trait]
impl TaskExecutor for PausedPlanningExecutor {
    async fn execute(
        &self,
        ctx: ExecutionContext,
    ) -> std::result::Result<ExecutionResult, ExecutorError> {
        let role = ExecutionRepo::get_by_id(&*self.db, &ctx.execution_id)
            .await
            .unwrap()
            .unwrap()
            .role;
        let _ = self.sender.send(ctx);
        if role == "planner" {
            let finish = self.finish.lock().unwrap().take();
            if let Some(finish) = finish {
                finish.await.unwrap();
                return Ok(ExecutionResult {
                    status: executors::ExecutionOutcome::Failed,
                    error: Some("planning interrupted by provider failure".to_owned()),
                    ..Default::default()
                });
            }
        }
        pending::<()>().await;
        unreachable!()
    }
    async fn cancel(&self, _execution_id: &str) -> std::result::Result<(), ExecutorError> {
        Ok(())
    }
}

#[tokio::test]
async fn a_lead_paused_mid_planning_retries_a_failed_run_after_resume() {
    let (finish, wait) = tokio::sync::oneshot::channel();
    let mut lead = fixture_with(|db, sender| {
        Arc::new(PausedPlanningExecutor {
            db,
            sender,
            finish: std::sync::Mutex::new(Some(wait)),
        })
    })
    .await;
    let (root, child) = root_with_child(&lead, true).await;
    until(&lead, "the planner runs", async || {
        executions(&lead.db, &root.id).await == [("planner".to_owned(), "running".to_owned())]
    })
    .await;
    let ctx = tokio::time::timeout(Duration::from_secs(5), lead.rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        ctx.task_id, root.id,
        "pause happens inside the planning run, after launch"
    );
    db::AgentRepo::set_paused(&*lead.db, &lead.planner, true)
        .await
        .unwrap();
    passes(&lead, 3).await;
    assert_eq!(
        executions(&lead.db, &root.id).await,
        [("planner".to_owned(), "running".to_owned())],
        "pause allows the admitted run to finish"
    );
    finish.send(()).unwrap();
    until(&lead, "the failed run ends", async || {
        executions(&lead.db, &root.id).await == [("planner".to_owned(), "failed".to_owned())]
    })
    .await;
    let task = reload(&lead.db, &root.id).await;
    if let Some(mut deferred) = task
        .metadata_json
        .as_deref()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
        .map(|m| m["deferred_dispatch"].clone())
        .filter(serde_json::Value::is_object)
    {
        deferred["not_before"] = serde_json::json!("1970-01-01T00:00:00Z");
        TaskRepo::mutate_metadata(
            &*lead.db,
            &root.id,
            None,
            vec![db::TaskMetadataMutation::Set {
                key: "deferred_dispatch".to_owned(),
                value: deferred,
            }],
            &now_rfc3339(),
        )
        .await
        .unwrap();
    }
    until(&lead, "the root parks on its paused lead", async || {
        serde_json::to_value(reload(&lead.db, &root.id).await.condition.public()).unwrap()
            ["primary"]["kind"]
            == "agent"
    })
    .await;
    let condition =
        serde_json::to_value(reload(&lead.db, &root.id).await.condition.public()).unwrap();
    assert_eq!(condition["primary"]["agent_id"], lead.planner);
    assert_eq!(condition["primary"]["status"], "paused");
    assert_eq!(reload(&lead.db, &root.id).await.status, "planning");
    assert!(executions(&lead.db, &child.id).await.is_empty());
    passes(&lead, 4).await;
    assert_eq!(executions(&lead.db, &root.id).await.len(), 1);
    db::AgentRepo::set_paused(&*lead.db, &lead.planner, false)
        .await
        .unwrap();
    until(
        &lead,
        "the recorded planner retry resumes after the Agent",
        async || executions(&lead.db, &root.id).await.len() == 2,
    )
    .await;
    assert_eq!(
        db::budget::spent(lead.db.pool(), &root.id, "execution")
            .await
            .unwrap(),
        1,
        "resuming repairs the existing retry without another charge"
    );
    assert_eq!(
        executions(&lead.db, &root.id).await.last().unwrap(),
        &("planner".to_owned(), "running".to_owned())
    );
    assert!(executions(&lead.db, &child.id).await.is_empty());
}

/// Pause after planning entry admitted its run, before the provider launches.
/// This pause is a typed Agent wait; resume alone starts exactly one run.
#[tokio::test]
async fn a_lead_paused_during_planning_admission_parks_typed_and_resumes_once() {
    let lead = lead_fixture(false).await;
    let (root, child) = root_with_child(&lead, true).await;
    sqlx::query(&format!("CREATE TRIGGER pause_planning_lead AFTER INSERT ON workspace_lease WHEN NEW.task_id='{}' BEGIN UPDATE agent_identity SET paused=1,version=version+1 WHERE id='{}'; END",root.id,lead.planner))
        .execute(lead.db.pool()).await.unwrap();
    until(
        &lead,
        "the planning root parks on its paused Agent",
        async || {
            let condition =
                serde_json::to_value(reload(&lead.db, &root.id).await.condition.public()).unwrap();
            condition["primary"]["kind"] == "agent" && condition["primary"]["status"] == "paused"
        },
    )
    .await;
    let task = reload(&lead.db, &root.id).await;
    assert_eq!(task.status, "planning");
    let condition = serde_json::to_value(task.condition.public()).unwrap();
    assert_eq!(condition["primary"]["agent_id"], lead.planner);
    assert!(executions(&lead.db, &child.id).await.is_empty());
    let before = executions(&lead.db, &root.id).await;
    assert_eq!(
        before,
        [("planner".to_owned(), "failed".to_owned())],
        "the admitted run failed before launch"
    );
    passes(&lead, 4).await;
    assert_eq!(executions(&lead.db, &root.id).await, before);
    sqlx::query("DROP TRIGGER pause_planning_lead")
        .execute(lead.db.pool())
        .await
        .unwrap();
    db::AgentRepo::set_paused(&*lead.db, &lead.planner, false)
        .await
        .unwrap();
    until(&lead, "the paused planning run resumes", async || {
        executions(&lead.db, &root.id).await.len() == 2
    })
    .await;
    passes(&lead, 4).await;
    assert_eq!(
        executions(&lead.db, &root.id).await,
        [
            ("planner".to_owned(), "failed".to_owned()),
            ("planner".to_owned(), "running".to_owned())
        ]
    );
    assert!(executions(&lead.db, &child.id).await.is_empty());
}

/// Startup recovers both a root's planning gate and its inherited step-check gate.
#[tokio::test]
async fn recovery_of_orphaned_planning_and_step_check_runs_redispatches_once() {
    for state in ["planning", "lead_check"] {
        let lead = lead_fixture(false).await;
        let root = seed_task(
            &lead.db,
            &lead.project,
            "root",
            if state == "planning" {
                "planning"
            } else {
                "in_progress"
            },
            1,
        )
        .await;
        let child = seed_subtask(
            &lead.db,
            &root,
            "child",
            if state == "planning" {
                "todo"
            } else {
                "lead_check"
            },
            0,
        )
        .await;
        assign_role(&lead.db, &root.id, "coder", &lead.coder).await;
        assign_role(&lead.db, &root.id, "planner", &lead.planner).await;
        let task_id = if state == "planning" {
            &root.id
        } else {
            &child.id
        };
        seed_running_execution(&lead.db, task_id, &lead.planner, "planner").await;
        let orphan = ExecutionRepo::list_running_by_task(&*lead.db, task_id)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let recovery =
            crate::recovery::CrashRecovery::new(Arc::clone(&lead.db), Arc::new(EventBus::new(32)));
        recovery.run_recovery().await.unwrap();
        recovery.run_recovery().await.unwrap();
        lead.dispatcher.task_service.drain(task_id).await.unwrap();
        let recovered = ExecutionRepo::get_by_id(&*lead.db, &orphan.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recovered.status, ExecutionStatus::Cancelled, "{state}");
        assert_eq!(recovered.resume_policy, Some(db::ResumePolicy::Auto));
        until(&lead, "the recovered gate runs", async || {
            ExecutionRepo::list_running_by_task(&*lead.db, task_id)
                .await
                .unwrap()
                .len()
                == 1
        })
        .await;
        passes(&lead, 4).await;
        assert_eq!(executions(&lead.db, task_id).await.len(), 2, "{state}");
        assert_eq!(reload(&lead.db, task_id).await.status, state);
    }
}

#[tokio::test]
async fn a_project_pause_over_a_held_planning_root_parks_every_child_and_resume_restores_parent_wait(
) {
    let lead = lead_fixture(false).await;
    let (root, child) = root_with_child(&lead, true).await;
    passes(&lead, 3).await;
    let root = reload(&lead.db, &root.id).await;
    lead.dispatcher
        .task_service
        .perform_task_action(
            &root.id,
            api_types::TaskAction::Hold { reason: None },
            root.version,
        )
        .await
        .unwrap();
    ProjectRepo::set_paused_at(&*lead.db, &lead.project, Some(now_rfc3339()))
        .await
        .unwrap();
    passes(&lead, 4).await;
    let paused = reload(&lead.db, &child.id).await;
    assert!(
        paused
            .condition
            .reasons()
            .any(|reason| matches!(reason, db::ParkReason::ProjectPaused { .. })),
        "{:?}",
        paused.condition
    );
    assert!(executions(&lead.db, &child.id).await.is_empty());
    ProjectRepo::set_paused_at(&*lead.db, &lead.project, None)
        .await
        .unwrap();
    passes(&lead, 4).await;
    let resumed = reload(&lead.db, &child.id).await;
    assert!(
        resumed.condition.reasons().any(
            |reason| matches!(reason, db::ParkReason::Parent { cause, .. } if cause == "held")
        ),
        "{:?}",
        resumed.condition
    );
    assert!(executions(&lead.db, &child.id).await.is_empty());
}

/// Fifty Tasks without a lead, in mixed states. Under a pause every
/// non-terminal Task without a run is parked and none is `clear`; a failure
/// keeps its own primary reason; a settled Task and a live run are left
/// alone. After the resume no Task is left on `project_paused` and every
/// Task shows the reasons it showed before the pause.
#[tokio::test]
async fn a_project_pause_parks_fifty_mixed_tasks_and_resume_leaves_none_parked_on_it() {
    let lead = lead_fixture(false).await;
    let mut tasks = Vec::new();
    for n in 0..12 {
        tasks.push(
            seed_task(
                &lead.db,
                &lead.project,
                &format!("backlog {n}"),
                "backlog",
                1,
            )
            .await,
        );
        tasks.push(seed_task(&lead.db, &lead.project, &format!("todo {n}"), "todo", 1).await);
    }
    let mut failed = Vec::new();
    for n in 0..6 {
        let task = seed_task(
            &lead.db,
            &lead.project,
            &format!("failed {n}"),
            "in_progress",
            1,
        )
        .await;
        assign_role(&lead.db, &task.id, "coder", &lead.coder).await;
        failed.push(
            super::root_replan::park_child(&lead, &task, "failed work")
                .await
                .id,
        );
        tasks.push(task);
    }
    let root = seed_task(&lead.db, &lead.project, "root", "in_progress", 1).await;
    assign_role(&lead.db, &root.id, "coder", &lead.coder).await;
    for n in 0..9 {
        tasks.push(seed_subtask(&lead.db, &root, &format!("child {n}"), "todo", n).await);
    }
    tasks.push(root);
    for n in 0..10 {
        tasks.push(seed_task(&lead.db, &lead.project, &format!("done {n}"), "done", 1).await);
    }
    assert_eq!(tasks.len(), 50);
    let reasons = |task: &Task| format!("{:?}", task.condition.reasons().collect::<Vec<_>>());
    let paused_on = |task: &Task| {
        task.condition
            .reasons()
            .any(|reason| matches!(reason, db::ParkReason::ProjectPaused { state: None }))
    };
    passes(&lead, 4).await;
    let mut before = Vec::new();
    for task in &tasks {
        let task = reload(&lead.db, &task.id).await;
        assert!(!paused_on(&task), "{} {:?}", task.title, task.condition);
        before.push((reasons(&task), executions(&lead.db, &task.id).await));
    }

    ProjectRepo::set_paused_at(&*lead.db, &lead.project, Some(now_rfc3339()))
        .await
        .unwrap();
    passes(&lead, 4).await;
    for (task, (_, runs)) in tasks.iter().zip(&before) {
        let task = reload(&lead.db, &task.id).await;
        // The pause starts nothing.
        assert_eq!(
            &executions(&lead.db, &task.id).await,
            runs,
            "{}",
            task.title
        );
        if runs.iter().any(|(_, status)| status == "running") {
            // A live run stays `running`: the pause holds future dispatch.
            assert!(
                matches!(task.condition, db::TaskCondition::Running { .. }),
                "{} {:?}",
                task.title,
                task.condition
            );
            continue;
        }
        if task.status == "done" {
            assert!(!paused_on(&task), "{} {:?}", task.title, task.condition);
            continue;
        }
        assert!(
            !matches!(task.condition, db::TaskCondition::Clear { .. }),
            "{} is clear under a pause",
            task.title
        );
        if failed.contains(&task.id) {
            // The failure stays the primary reason, with its own exit.
            assert_eq!(
                task.condition.read().failure_kind,
                Some(api_types::FailureKind::RetryExhausted),
                "{:?}",
                task.condition
            );
            assert!(
                !matches!(
                    &task.condition,
                    db::TaskCondition::Parked {
                        primary: db::ParkReason::ProjectPaused { .. },
                        ..
                    }
                ),
                "{:?}",
                task.condition
            );
        } else {
            assert!(paused_on(&task), "{} {:?}", task.title, task.condition);
        }
    }

    ProjectRepo::set_paused_at(&*lead.db, &lead.project, None)
        .await
        .unwrap();
    passes(&lead, 4).await;
    for (task, (was, _)) in tasks.iter().zip(&before) {
        let task = reload(&lead.db, &task.id).await;
        assert!(!paused_on(&task), "{} {:?}", task.title, task.condition);
        assert_eq!(&reasons(&task), was, "{}", task.title);
    }
}

#[tokio::test]
async fn a_later_subtask_names_its_sequence_wait_without_changing_the_no_lead_root() {
    let lead = lead_fixture(false).await;
    let (root, first, later) = super::root_replan::root_with_parked_child(&lead, false).await;
    passes(&lead, 4).await;
    let waiting = reload(&lead.db, &later.id).await;
    assert!(waiting.condition.reasons().any(
        |reason| matches!(reason, db::ParkReason::Parent { cause, .. } if cause == "sequence")
    ));
    assert!(!lead
        .dispatcher
        .task_service
        .test_action_values(&later.id)
        .await
        .unwrap()
        .iter()
        .any(|action| matches!(action, api_types::TaskAction::Start)));
    assert_eq!(reload(&lead.db, &root.id).await.status, "in_progress");
    assert!(executions(&lead.db, &root.id).await.is_empty());
    let first = reload(&lead.db, &first.id).await;
    lead.dispatcher
        .task_service
        .perform_task_action(
            &first.id,
            api_types::TaskAction::Cancel { reason: None },
            first.version,
        )
        .await
        .unwrap();
    until(
        &lead,
        "the next sibling runs after the first settles",
        async || !executions(&lead.db, &later.id).await.is_empty(),
    )
    .await;
    assert_eq!(reload(&lead.db, &root.id).await.status, "in_progress");
    assert!(executions(&lead.db, &root.id).await.is_empty());
}
