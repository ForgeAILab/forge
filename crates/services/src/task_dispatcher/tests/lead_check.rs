//! The step check (`lead_check`): an optional gate in the working phase of the
//! inherited subtask workflow, between implementation and `done`. Its role is
//! held by the root's lead. Without a lead the gate is skipped on entry; with
//! one, the lead runs a read-only execution on the subtask and its result
//! block accepts the step or returns it to the same worker thread.
use std::collections::VecDeque;

use super::*;

/// Completes every implementation run at once. A planner run on a root (the
/// kickoff) completes at once too; a planner run on a subtask (the step check)
/// replies with the next scripted reply, and stays running when none is left.
struct StepExecutor {
    db: Arc<db::SqliteDb>,
    replies: Arc<std::sync::Mutex<VecDeque<String>>>,
    /// Keep implementation runs of this Task title running.
    hold_coder: Option<&'static str>,
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
            return completed(None);
        }
        if task.parent_task_id.is_none() {
            return completed(None);
        }
        let reply = self.replies.lock().unwrap().pop_front();
        match reply {
            Some(reply) => completed(Some(reply)),
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

struct Steps {
    db: Arc<db::SqliteDb>,
    project: String,
    coder: String,
    planner: String,
    dispatcher: TaskDispatcher,
    _dirs: (TempDir, TempDir),
}

const PASS: &str = "The step does what it says.\n{\"result\":\"pass\",\"reason\":\"verified\"}";
const FAIL: &str =
    "The parser drops the last row.\n{\"result\":\"fail\",\"reason\":\"the last row is dropped\"}";

async fn steps(replies: &[&str], hold_coder: Option<&'static str>) -> Steps {
    let db = Arc::new(sqlite_db().await);
    let (repo, workspace) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (project, _) = seed_project_repo(&db, repo.path()).await;
    let coder = seed_agent(&db, 3, DaemonStatus::Online, AgentStatus::Idle).await;
    let planner = crate::test_support::seed_native_agent(&db).await;
    let event_bus = Arc::new(EventBus::new(64));
    let task_executor: Arc<dyn TaskExecutor> = Arc::new(StepExecutor {
        db: Arc::clone(&db),
        replies: Arc::new(std::sync::Mutex::new(
            replies.iter().map(|reply| (*reply).to_owned()).collect(),
        )),
        hold_coder,
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
        _dirs: (repo, workspace),
    }
}

/// A root in `todo` with a default worker and two ordered subtasks.
async fn root_with_two_steps(steps: &Steps, with_lead: bool) -> (Task, Task, Task) {
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

/// `(from, to, bridge kind, rejection)` of every transition of a Task.
async fn log(db: &db::SqliteDb, task: &str) -> Vec<(String, String, Option<String>, bool)> {
    sqlx::query_as(
        "SELECT from_state, to_state, bridge_kind, rejection FROM transition_log
         WHERE task_id=? ORDER BY created_at, rowid",
    )
    .bind(task)
    .fetch_all(db.pool())
    .await
    .unwrap()
}

async fn count(db: &db::SqliteDb, sql: &str, task: &str) -> i64 {
    sqlx::query_scalar(sql)
        .bind(task)
        .fetch_one(db.pool())
        .await
        .unwrap()
}

async fn dump(db: &db::SqliteDb) -> String {
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
async fn until(steps: &Steps, what: &str, done: impl AsyncFn() -> bool) {
    for _ in 0..240 {
        if done().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
        steps.dispatcher.check_once_and_drain().await.unwrap();
    }
    panic!("timed out waiting until {what}\n{}", dump(&steps.db).await);
}

async fn passes(steps: &Steps, count: usize) {
    for _ in 0..count {
        tokio::time::sleep(Duration::from_millis(25)).await;
        steps.dispatcher.check_once_and_drain().await.unwrap();
    }
}

fn hop(
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

/// The failing-first test of stage 3. No lead is a first-class mode: a
/// finished subtask enters the step check, the gate is skipped at once (no
/// execution, no person, no extra dispatcher decision), and the next sibling
/// starts.
#[tokio::test]
async fn without_a_lead_a_finished_subtask_passes_the_step_check_at_once() {
    let steps = steps(&[], Some("second")).await;
    let (root, first, second) = root_with_two_steps(&steps, false).await;

    until(&steps, "the first subtask is done", async || {
        reload(&steps.db, &first.id).await.status == "done"
    })
    .await;
    assert_eq!(
        log(&steps.db, &first.id).await,
        vec![
            hop("todo", "in_progress", None, false),
            hop("in_progress", "lead_check", None, false),
            hop("lead_check", "done", Some("gate_skipped"), false),
        ],
        "{}",
        dump(&steps.db).await
    );
    // Nothing ran in the gate, and nothing of the review machinery was used.
    assert_eq!(
        executions(&steps.db, &first.id).await,
        vec![("coder".to_owned(), "completed".to_owned())]
    );
    assert_eq!(
        count(
            &steps.db,
            "SELECT COUNT(*) FROM review WHERE task_id=?",
            &first.id
        )
        .await,
        0
    );
    // The next sibling was woken and runs.
    until(&steps, "the second subtask runs", async || {
        executions(&steps.db, &second.id).await == vec![("coder".to_owned(), "running".to_owned())]
    })
    .await;
    assert_eq!(reload(&steps.db, &root.id).await.status, "in_progress");
    assert_eq!(executions(&steps.db, &root.id).await, Vec::new());
}

async fn gate_spent(db: &db::SqliteDb, task: &str) -> i64 {
    db::budget::spent(db.pool(), task, &db::budget::gate_key("lead_check"))
        .await
        .unwrap()
}

async fn check_runs(db: &db::SqliteDb) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM check_run")
        .fetch_one(db.pool())
        .await
        .unwrap()
}

/// With a lead, the lead runs on the subtask (a planner execution under the
/// root's lead Agent, not a review) and its `pass` moves the subtask to
/// `done`; the next sibling starts only then.
#[tokio::test]
async fn a_lead_that_passes_the_step_moves_it_to_done_and_releases_the_next() {
    let steps = steps(&[PASS], Some("second")).await;
    let (root, first, second) = root_with_two_steps(&steps, true).await;

    until(&steps, "the first subtask is done", async || {
        reload(&steps.db, &first.id).await.status == "done"
    })
    .await;
    assert_eq!(
        log(&steps.db, &first.id).await,
        vec![
            hop("todo", "in_progress", None, false),
            hop("in_progress", "lead_check", None, false),
            hop("lead_check", "done", None, false),
        ],
        "{}",
        dump(&steps.db).await
    );
    assert_eq!(
        executions(&steps.db, &first.id).await,
        vec![
            ("coder".to_owned(), "completed".to_owned()),
            ("planner".to_owned(), "completed".to_owned()),
        ]
    );
    let check_agent: Option<String> =
        sqlx::query_scalar("SELECT agent_id FROM execution WHERE task_id=? AND role='planner'")
            .bind(&first.id)
            .fetch_one(steps.db.pool())
            .await
            .unwrap();
    assert_eq!(check_agent.as_deref(), Some(steps.planner.as_str()));
    // No row was copied onto the subtask, and no review machinery ran.
    assert_eq!(
        count(
            &steps.db,
            "SELECT COUNT(*) FROM task_role_assignment WHERE task_id=?",
            &first.id
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &steps.db,
            "SELECT COUNT(*) FROM review WHERE task_id=?",
            &first.id
        )
        .await,
        0
    );
    assert_eq!(check_runs(&steps.db).await, 0);
    assert_eq!(gate_spent(&steps.db, &first.id).await, 0);
    let verdict: Vec<String> =
        sqlx::query_scalar("SELECT content FROM task_comment WHERE task_id=? ORDER BY created_at")
            .bind(&first.id)
            .fetch_all(steps.db.pool())
            .await
            .unwrap();
    assert!(
        verdict
            .iter()
            .any(|content| content == "The lead accepted this step: verified"),
        "{verdict:?}"
    );
    until(&steps, "the second subtask runs", async || {
        executions(&steps.db, &second.id).await == vec![("coder".to_owned(), "running".to_owned())]
    })
    .await;
    // The root planned once (kickoff) and no step check ran on it.
    assert_eq!(
        executions(&steps.db, &root.id).await,
        vec![("planner".to_owned(), "completed".to_owned())]
    );
}

/// A `fail` returns the subtask to implementation as a rejection of the gate:
/// the gate's own budget is charged, the worker is dispatched again with the
/// lead's findings, and the next sibling keeps waiting.
#[tokio::test]
async fn a_lead_that_fails_the_step_returns_it_to_the_same_worker_with_the_findings() {
    let steps = steps(&[FAIL], None).await;
    let (_root, first, second) = root_with_two_steps(&steps, true).await;

    // First check fails, the worker runs again, the second check is running.
    until(&steps, "the second step check runs", async || {
        executions(&steps.db, &first.id).await
            == vec![
                ("coder".to_owned(), "completed".to_owned()),
                ("planner".to_owned(), "completed".to_owned()),
                ("coder".to_owned(), "completed".to_owned()),
                ("planner".to_owned(), "running".to_owned()),
            ]
    })
    .await;
    assert_eq!(
        log(&steps.db, &first.id).await,
        vec![
            hop("todo", "in_progress", None, false),
            hop("in_progress", "lead_check", None, false),
            hop("lead_check", "in_progress", Some("gate_rejected"), true),
            hop("in_progress", "lead_check", None, false),
        ],
        "{}",
        dump(&steps.db).await
    );
    assert_eq!(gate_spent(&steps.db, &first.id).await, 1);
    // The returned run is the same worker, told what the lead found.
    let coder_runs: Vec<(Option<String>, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT agent_id, prompt, parent_execution_id FROM execution
         WHERE task_id=? AND role='coder' ORDER BY created_at, id",
    )
    .bind(&first.id)
    .fetch_all(steps.db.pool())
    .await
    .unwrap();
    assert_eq!(coder_runs[0].0, coder_runs[1].0);
    let prompt = coder_runs[1].1.clone().unwrap_or_default();
    assert!(
        prompt.contains("The lead returned this step: the last row is dropped")
            && prompt.contains("The parser drops the last row."),
        "{prompt}"
    );
    // The next sibling has not started, and nothing reviewed anything.
    assert_eq!(reload(&steps.db, &second.id).await.status, "todo");
    assert_eq!(executions(&steps.db, &second.id).await, Vec::new());
    assert_eq!(
        count(
            &steps.db,
            "SELECT COUNT(*) FROM review WHERE task_id=?",
            &first.id
        )
        .await,
        0
    );
    assert_eq!(check_runs(&steps.db).await, 0);
    // The Review budget is not the step check's.
    assert_eq!(
        db::budget::spent(steps.db.pool(), &first.id, db::budget::Kind::Review.key())
            .await
            .unwrap(),
        0
    );
}

/// `max_rejections` bounds the returns: after the second return the next
/// entry into the gate parks the subtask (typed, with a failure kind) and
/// starts no third check. The sequence stops at that subtask.
#[tokio::test]
async fn the_entry_after_the_last_allowed_return_parks_the_subtask() {
    let steps = steps(&[FAIL, FAIL], None).await;
    let (_root, first, second) = root_with_two_steps(&steps, true).await;

    until(
        &steps,
        "the subtask is parked on its spent budget",
        async || {
            let task = reload(&steps.db, &first.id).await;
            task.status == "lead_check" && task.condition.read().interruption_present
        },
    )
    .await;
    passes(&steps, 4).await;
    let parked = reload(&steps.db, &first.id).await;
    assert_eq!(
        parked.condition.read().failure_kind,
        Some(api_types::FailureKind::RetryExhausted),
        "{}",
        dump(&steps.db).await
    );
    assert_eq!(gate_spent(&steps.db, &first.id).await, 2);
    assert_eq!(
        executions(&steps.db, &first.id).await,
        vec![
            ("coder".to_owned(), "completed".to_owned()),
            ("planner".to_owned(), "completed".to_owned()),
            ("coder".to_owned(), "completed".to_owned()),
            ("planner".to_owned(), "completed".to_owned()),
            ("coder".to_owned(), "completed".to_owned()),
        ],
        "no third step check"
    );
    assert_eq!(executions(&steps.db, &second.id).await, Vec::new());
    // The park has a working action for its owner.
    let snapshot = crate::task_actions::load_snapshot(
        &steps.db,
        parked.clone(),
        WorkflowEngine::resolve_workflow_for_task(
            &parked,
            "{}",
            &Actor::system(SystemComponent::TaskDispatcher),
        ),
        &Actor::user(api_types::UserActionSource::Test),
        None,
    )
    .await
    .expect("the snapshot loads");
    let actions = crate::available_actions(&snapshot);
    assert!(
        actions
            .iter()
            .any(|offer| matches!(offer.action, api_types::TaskAction::Retry { .. })),
        "{actions:?}"
    );
}

/// A step check that ends without a verdict is not a pass and not a return:
/// it goes through the bounded completion-guard path, and with no session to
/// ask again the subtask is blocked with a visible failure.
#[tokio::test]
async fn a_step_check_without_a_result_block_blocks_the_subtask_visibly() {
    let steps = steps(&["Looks fine to me."], None).await;
    let (_root, first, second) = root_with_two_steps(&steps, true).await;

    until(&steps, "the subtask is blocked", async || {
        reload(&steps.db, &first.id)
            .await
            .condition
            .read()
            .interruption_present
    })
    .await;
    let blocked = reload(&steps.db, &first.id).await;
    assert_eq!(blocked.status, "lead_check");
    assert_eq!(
        blocked.condition.read().failure_kind,
        Some(api_types::FailureKind::WorkflowGuardRejected),
        "{}",
        dump(&steps.db).await
    );
    assert_eq!(gate_spent(&steps.db, &first.id).await, 0, "not a return");
    assert_eq!(executions(&steps.db, &second.id).await, Vec::new());
    passes(&steps, 3).await;
    assert_eq!(
        executions(&steps.db, &first.id).await.len(),
        2,
        "the check is not started again on its own"
    );
}

async fn in_progress_root_with_a_step_in_the_gate(steps: &Steps, with_lead: bool) -> (Task, Task) {
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

/// A subtask that rests in the gate with nobody holding it (its root's lead
/// was removed after it entered, or the entry was interrupted) is passed by
/// the dispatcher from stored state: no person, no run, and never back to
/// implementation.
#[tokio::test]
async fn a_step_resting_in_the_gate_without_a_lead_is_passed_from_stored_state() {
    let steps = steps(&[], None).await;
    let (_root, child) = in_progress_root_with_a_step_in_the_gate(&steps, false).await;
    // An own planner row on the subtask holds nothing: the role is the
    // root's lead and nothing else.
    assign_role(
        &steps.db,
        &child.id,
        crate::workflow::default_roles::PLANNER,
        &steps.planner,
    )
    .await;

    until(&steps, "the step is done", async || {
        reload(&steps.db, &child.id).await.status == "done"
    })
    .await;
    assert_eq!(
        log(&steps.db, &child.id).await,
        vec![hop("lead_check", "done", Some("gate_skipped"), false)],
        "{}",
        dump(&steps.db).await
    );
    assert_eq!(executions(&steps.db, &child.id).await, Vec::new());
}

/// Removing the lead while a step waits in the gate wakes that step, and the
/// dispatcher passes it.
#[tokio::test]
async fn removing_the_lead_passes_a_step_that_waits_in_the_gate() {
    let steps = steps(&[], None).await;
    let (root, child) = in_progress_root_with_a_step_in_the_gate(&steps, true).await;
    // Hold the Project so the lead's check is not started before the lead
    // is removed.
    let project = ProjectRepo::get_by_id(&*steps.db, &steps.project)
        .await
        .unwrap()
        .unwrap();
    ProjectRepo::update_at_version(
        &*steps.db,
        UpdateProject {
            id: project.id.clone(),
            name: None,
            settings: None,
            primary_repo_id: None,
            paused_at: Some(Some(now_rfc3339())),
            updated_at: now_rfc3339(),
        },
        project.version,
        None,
    )
    .await
    .unwrap();
    passes(&steps, 2).await;
    assert_eq!(reload(&steps.db, &child.id).await.status, "lead_check");
    assert_eq!(executions(&steps.db, &child.id).await, Vec::new());

    steps
        .dispatcher
        .task_service
        .remove_role(
            &root.id,
            crate::workflow::default_roles::PLANNER,
            false,
            false,
        )
        .await
        .expect("the lead is removed");
    let dirty: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM task_schedule_dirty WHERE task_id=? AND dirty=1)",
    )
    .bind(&child.id)
    .fetch_one(steps.db.pool())
    .await
    .unwrap();
    assert!(dirty, "removing the lead marks the waiting step");
    let project = ProjectRepo::get_by_id(&*steps.db, &steps.project)
        .await
        .unwrap()
        .unwrap();
    ProjectRepo::update_at_version(
        &*steps.db,
        UpdateProject {
            id: project.id.clone(),
            name: None,
            settings: None,
            primary_repo_id: None,
            paused_at: Some(None),
            updated_at: now_rfc3339(),
        },
        project.version,
        None,
    )
    .await
    .unwrap();
    until(&steps, "the step is done", async || {
        reload(&steps.db, &child.id).await.status == "done"
    })
    .await;
    assert_eq!(executions(&steps.db, &child.id).await, Vec::new());
}

/// A crash between the entry into the gate and the lead's run leaves a
/// subtask in the gate with no execution: the dispatcher starts the check
/// from stored state, under the root's lead.
#[tokio::test]
async fn a_step_resting_in_the_gate_under_a_lead_gets_its_check_from_stored_state() {
    let steps = steps(&[], None).await;
    let (root, child) = in_progress_root_with_a_step_in_the_gate(&steps, true).await;

    until(&steps, "the step check runs", async || {
        executions(&steps.db, &child.id).await == vec![("planner".to_owned(), "running".to_owned())]
    })
    .await;
    assert_eq!(reload(&steps.db, &child.id).await.status, "lead_check");
    // Stable: no second check, nothing on the root.
    passes(&steps, 3).await;
    assert_eq!(executions(&steps.db, &child.id).await.len(), 1);
    assert_eq!(executions(&steps.db, &root.id).await, Vec::new());
    let prompt: Option<String> =
        sqlx::query_scalar("SELECT prompt FROM execution WHERE task_id=? AND role='planner'")
            .bind(&child.id)
            .fetch_one(steps.db.pool())
            .await
            .unwrap();
    let prompt = prompt.unwrap_or_default();
    assert!(
        prompt.contains("Step-check doctrine:")
            && prompt.contains("Check this finished step: first"),
        "{prompt}"
    );
}
