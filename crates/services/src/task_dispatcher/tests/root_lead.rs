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
    let db = Arc::new(sqlite_db().await);
    let (repo, workspace) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (project, _) = seed_project_repo(&db, repo.path()).await;
    let coder = seed_agent(&db, 3, DaemonStatus::Online, AgentStatus::Idle).await;
    let planner = crate::test_support::seed_native_agent(&db).await;
    let event_bus = Arc::new(EventBus::new(64));
    let (tx, rx) = mpsc::unbounded_channel();
    let task_executor: Arc<dyn TaskExecutor> = if planner_completes || fail_planner {
        Arc::new(LeadExecutor {
            db: Arc::clone(&db),
            sender: tx,
            fail_planner,
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
    for _ in 0..200 {
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

/// Removing the lead while its planner runs must not start a subtask beside
/// the planner: a root and its subtasks have one running execution at most.
#[tokio::test]
async fn removing_the_lead_while_it_plans_never_runs_a_subtask_beside_the_planner() {
    let lead = lead_fixture(false).await;
    let (root, child) = root_with_child(&lead, true).await;
    passes(&lead, 3).await;
    assert_eq!(reload(&lead.db, &root.id).await.status, "planning");
    sqlx::query("DELETE FROM task_role_assignment WHERE task_id=? AND role_name='planner'")
        .bind(&root.id)
        .execute(lead.db.pool())
        .await
        .unwrap();
    passes(&lead, 5).await;
    let dump = state_dump(&lead.db).await;
    eprintln!("REPORT lead removed while planning:\n{dump}");
    let running = |runs: &[(String, String)]| {
        runs.iter()
            .filter(|(_, status)| status == "running")
            .count()
    };
    // The run holds the root: nothing starts beside it, and both wait typed.
    assert_eq!(
        reload(&lead.db, &root.id).await.status,
        "planning",
        "{dump}"
    );
    assert_eq!(running(&executions(&lead.db, &root.id).await), 1, "{dump}");
    assert_eq!(executions(&lead.db, &child.id).await, Vec::new(), "{dump}");
    assert!(
        park(&lead.db, &root.id).await.contains("InFlight"),
        "{dump}"
    );
    assert!(
        park(&lead.db, &child.id).await.contains("\"planning\""),
        "{dump}"
    );
    let version = reload(&lead.db, &root.id).await.version;
    passes(&lead, 3).await;
    assert_eq!(
        reload(&lead.db, &root.id).await.version,
        version,
        "the hold writes nothing"
    );

    // And it ends: the run stops, the root has no lead, so it implements and
    // its subtask runs.
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
    passes(&lead, 3).await;
    let stopped = reload(&lead.db, &root.id).await;
    let offers = service.test_action_values(&root.id).await.unwrap();
    eprintln!(
        "REPORT lead removed, run stopped: {} {offers:?}",
        stopped.status
    );
    if stopped.status == "planning" {
        service
            .perform_task_action(
                &root.id,
                api_types::TaskAction::Release { reason: None },
                stopped.version,
            )
            .await
            .expect("the stopped root is released");
        passes(&lead, 4).await;
    }
    let dump = state_dump(&lead.db).await;
    assert_eq!(
        reload(&lead.db, &root.id).await.status,
        "in_progress",
        "{dump}"
    );
    assert_eq!(running(&executions(&lead.db, &child.id).await), 1, "{dump}");
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
