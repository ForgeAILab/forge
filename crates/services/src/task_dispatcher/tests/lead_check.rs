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
    /// A scripted reply that starts with `@hold:` is given only after this
    /// gets a permit (a step check that is still running).
    release: Arc<tokio::sync::Semaphore>,
    /// Bytes every implementation run commits to the shared workspace, in a
    /// file named after its Task (0: it commits nothing).
    commit_bytes: Arc<std::sync::atomic::AtomicUsize>,
}

fn git_in(worktree: &str, args: &[&str]) {
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

struct Steps {
    db: Arc<db::SqliteDb>,
    project: String,
    coder: String,
    planner: String,
    dispatcher: TaskDispatcher,
    release: Arc<tokio::sync::Semaphore>,
    commit_bytes: Arc<std::sync::atomic::AtomicUsize>,
    /// The scripted replies no step check has taken yet.
    replies: Arc<std::sync::Mutex<VecDeque<String>>>,
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
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let commit_bytes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
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

// ---------------------------------------------------------------------------
// Review round: the cases the first pass left open.
// ---------------------------------------------------------------------------

const HELD_FAIL: &str =
    "@hold:Late findings.\n{\"result\":\"fail\",\"reason\":\"a verdict nobody asked for\"}";
const BLOCKED: &str =
    "I could not open the repository.\n{\"result\":\"blocked\",\"reason\":\"no access\"}";

async fn running_check(db: &db::SqliteDb, task: &str) -> bool {
    executions(db, task)
        .await
        .last()
        .is_some_and(|run| run == &("planner".to_owned(), "running".to_owned()))
}

async fn condition(db: &db::SqliteDb, task: &str) -> String {
    sqlx::query_scalar::<_, String>("SELECT COALESCE(condition_json, '') FROM task WHERE id=?")
        .bind(task)
        .fetch_one(db.pool())
        .await
        .unwrap()
}

/// Open Attention items about `task`, other than the follow-up every
/// completed Task gets.
async fn open_attention(steps: &Steps, task: &str) -> i64 {
    let attention = crate::AttentionService::new(Arc::clone(&steps.db));
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

/// No lead: the skip is enqueued by the entry into the gate (its own hook
/// step cascades in the same chain); it does not wait for a dispatcher pass
/// or a sweep.
#[tokio::test]
async fn without_a_lead_the_skip_rides_the_entry_s_own_chain() {
    let steps = steps(&[], Some("second")).await;
    let (_root, first, _second) = root_with_two_steps(&steps, false).await;
    until(&steps, "the first subtask is done", async || {
        reload(&steps.db, &first.id).await.status == "done"
    })
    .await;
    let skips: Vec<(String, String)> = sqlx::query_as(
        "SELECT triggered_by, trigger_reason FROM transition_log
         WHERE task_id=? AND from_state='lead_check'",
    )
    .bind(&first.id)
    .fetch_all(steps.db.pool())
    .await
    .unwrap();
    // Exactly one skip. It is the cascade of the entry's own hook step
    // (same chain, enqueued without any dispatcher decision). A dispatcher
    // pass that happens to see the subtask between those two queue steps
    // may issue the same skip (`Step::SkipGate`); whichever commits first is
    // the one transition, and the subtask needs no further pass either way.
    assert_eq!(skips.len(), 1, "{skips:?}");
    assert!(
        skips[0].1 == "No planner role assigned; optional gate skipped"
            || skips[0].1.starts_with("Nobody holds the lead_check gate"),
        "{skips:?}"
    );
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT kind, chain_id FROM task_step WHERE task_id=? AND expected_status='lead_check'
         ORDER BY seq",
    )
    .bind(&first.id)
    .fetch_all(steps.db.pool())
    .await
    .unwrap();
    let chain = |kind: &str| -> Vec<&str> {
        rows.iter()
            .filter(|row| row.0 == kind)
            .map(|row| row.1.as_str())
            .collect()
    };
    assert_eq!(chain("hooks").len(), 1, "{rows:?}");
    assert_eq!(chain("cascade"), chain("hooks"), "one chain: {rows:?}");
    // Nobody is asked for anything: no Attention item, no park.
    let open = open_attention(&steps, &first.id).await;
    let items: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT attention_type, status, summary, details_json FROM attention_projection",
    )
    .fetch_all(steps.db.pool())
    .await
    .unwrap();
    assert_eq!(open, 0, "{items:#?}");
}

/// A `planner` row on a subtask would hold nothing (the step check is the
/// root's lead): assigning one is refused and names the root.
#[tokio::test]
async fn assigning_the_step_check_role_on_a_subtask_is_refused_and_names_the_root() {
    let steps = steps(&[], Some("first")).await;
    let (root, first, _second) = root_with_two_steps(&steps, false).await;
    let now = now_rfc3339();
    let refused = steps
        .dispatcher
        .task_service
        .reassign_role(
            db::CreateTaskRoleAssignment {
                id: new_uuid_v4(),
                task_id: first.id.clone(),
                role_name: crate::workflow::default_roles::PLANNER.to_owned(),
                assignee_type: Some(db::AssigneeKind::Agent),
                assignee_id: Some(steps.planner.clone()),
                created_at: now.clone(),
                updated_at: now,
            },
            false,
            false,
        )
        .await;
    let error = refused.expect_err("the assignment is refused").to_string();
    assert!(
        error.contains("held by the lead of its root") && error.contains(&root.id),
        "{error}"
    );
    assert_eq!(
        count(
            &steps.db,
            "SELECT COUNT(*) FROM task_role_assignment WHERE task_id=? AND role_name='planner'",
            &first.id
        )
        .await,
        0
    );
}

/// A `blocked` verdict is not a pass and not a return: the lead is asked
/// again under the completion-guard bound and the subtask parks, typed.
#[tokio::test]
async fn a_blocked_verdict_parks_the_subtask_without_spending_a_return() {
    let steps = steps(&[BLOCKED], None).await;
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
        Some(api_types::FailureKind::WorkflowGuardRejected)
    );
    assert_eq!(gate_spent(&steps.db, &first.id).await, 0);
    assert_eq!(executions(&steps.db, &second.id).await, Vec::new());
    assert!(
        open_attention(&steps, &first.id).await >= 1,
        "the park is shown"
    );
}

/// The spent return budget: one budget check per entry, one typed park with
/// an Attention item, and the owner's Retry opens a new window and has the
/// LEAD check the work that is already there (validate first; the worker is
/// not run again before anybody looked).
#[tokio::test]
async fn the_spent_budget_park_is_shown_once_and_retry_has_the_lead_check_the_work() {
    let steps = steps(&[FAIL, FAIL, PASS], Some("second")).await;
    let (_root, first, second) = root_with_two_steps(&steps, true).await;
    until(&steps, "the subtask is parked", async || {
        let task = reload(&steps.db, &first.id).await;
        task.status == "lead_check" && task.condition.read().interruption_present
    })
    .await;
    passes(&steps, 3).await;
    let parked = reload(&steps.db, &first.id).await;
    assert_eq!(
        parked.condition.read().failure_kind,
        Some(api_types::FailureKind::RetryExhausted)
    );
    // The budget is checked once per entry into the gate.
    let hooks: Option<String> = sqlx::query_scalar(
        "SELECT hook_results_json FROM transition_log
         WHERE task_id=? AND to_state='lead_check' ORDER BY created_at DESC, rowid DESC LIMIT 1",
    )
    .bind(&first.id)
    .fetch_one(steps.db.pool())
    .await
    .unwrap();
    let hooks = hooks.unwrap_or_default();
    assert_eq!(
        hooks.matches("check_retry_budget").count(),
        1,
        "one budget check per entry: {hooks}"
    );
    assert_eq!(
        open_attention(&steps, &first.id).await,
        1,
        "one Attention item"
    );

    steps
        .dispatcher
        .task_service
        .perform_task_action(
            &parked.id,
            api_types::TaskAction::Retry {
                reason: None,
                fresh_session: None,
                refresh_workspace: None,
                reset_budget: Some(true),
                guidance: None,
            },
            parked.version,
        )
        .await
        .expect("the owner retries the parked step");
    until(&steps, "the step is done", async || {
        reload(&steps.db, &first.id).await.status == "done"
    })
    .await;
    assert_eq!(
        executions(&steps.db, &first.id).await,
        vec![
            ("coder".to_owned(), "completed".to_owned()),
            ("planner".to_owned(), "completed".to_owned()),
            ("coder".to_owned(), "completed".to_owned()),
            ("planner".to_owned(), "completed".to_owned()),
            ("coder".to_owned(), "completed".to_owned()),
            ("planner".to_owned(), "completed".to_owned()),
        ],
        "Retry starts the lead's check of the last rework, not another rework\n{}",
        dump(&steps.db).await
    );
    until(&steps, "the next step runs", async || {
        !executions(&steps.db, &second.id).await.is_empty()
    })
    .await;
}

/// The lead is removed while its check runs: the run is stopped, its verdict
/// (whatever it would have been) moves nothing, and the subtask passes the
/// gate once, as under any root without a lead.
#[tokio::test]
async fn removing_the_lead_while_its_check_runs_stops_the_check_and_passes_the_step() {
    let steps = steps(&[HELD_FAIL], Some("second")).await;
    let (root, first, second) = root_with_two_steps(&steps, true).await;
    until(&steps, "the step check runs", async || {
        running_check(&steps.db, &first.id).await
    })
    .await;
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
    // Without the run ever answering.
    until(&steps, "the step is done", async || {
        reload(&steps.db, &first.id).await.status == "done"
    })
    .await;
    assert!(
        !running_check(&steps.db, &first.id).await,
        "the removed lead's run is stopped: {}",
        dump(&steps.db).await
    );
    // A late answer changes nothing.
    steps.release.add_permits(8);
    passes(&steps, 4).await;
    let hops = log(&steps.db, &first.id).await;
    assert_eq!(
        hops.iter().filter(|hop| hop.0 == "lead_check").count(),
        1,
        "{hops:?}"
    );
    assert_eq!(
        hops.last(),
        Some(&hop("lead_check", "done", Some("gate_skipped"), false))
    );
    assert_eq!(gate_spent(&steps.db, &first.id).await, 0);
    until(&steps, "the next step runs", async || {
        !executions(&steps.db, &second.id).await.is_empty()
    })
    .await;
}

/// Hold and Release on a subtask whose check is running: the hold stops the
/// check and parks the subtask as held; the release has the lead check it
/// again. No verdict of the stopped run moves it.
#[tokio::test]
async fn a_hold_on_a_step_in_the_gate_stops_the_check_and_release_starts_it_again() {
    let steps = steps(&[HELD_FAIL, PASS], Some("second")).await;
    let (_root, first, _second) = root_with_two_steps(&steps, true).await;
    until(&steps, "the step check runs", async || {
        running_check(&steps.db, &first.id).await
    })
    .await;
    let service = &steps.dispatcher.task_service;
    let resting = reload(&steps.db, &first.id).await;
    service
        .perform_task_action(
            &first.id,
            api_types::TaskAction::Hold { reason: None },
            resting.version,
        )
        .await
        .expect("a step in the gate is held");
    passes(&steps, 3).await;
    steps.release.add_permits(8);
    passes(&steps, 3).await;
    let held = reload(&steps.db, &first.id).await;
    assert_eq!(held.status, "lead_check", "{}", dump(&steps.db).await);
    assert!(!running_check(&steps.db, &first.id).await);
    assert_eq!(gate_spent(&steps.db, &first.id).await, 0);
    let offers = service.test_action_values(&first.id).await.unwrap();
    assert!(
        offers.contains(&api_types::TaskAction::Release { reason: None }),
        "{offers:?}\n{}",
        condition(&steps.db, &first.id).await
    );
    service
        .perform_task_action(
            &first.id,
            api_types::TaskAction::Release { reason: None },
            held.version,
        )
        .await
        .expect("the step is released");
    until(&steps, "the step is done", async || {
        reload(&steps.db, &first.id).await.status == "done"
    })
    .await;
    assert_eq!(
        log(&steps.db, &first.id).await.last(),
        Some(&hop("lead_check", "done", None, false)),
        "the lead's own pass, not a skip"
    );
}

/// Cancelling a subtask in the gate ends the check; a late verdict moves
/// nothing.
#[tokio::test]
async fn cancelling_a_step_in_the_gate_ends_the_check_and_drops_its_verdict() {
    let steps = steps(&[HELD_FAIL], Some("second")).await;
    let (_root, first, _second) = root_with_two_steps(&steps, true).await;
    until(&steps, "the step check runs", async || {
        running_check(&steps.db, &first.id).await
    })
    .await;
    let resting = reload(&steps.db, &first.id).await;
    steps
        .dispatcher
        .task_service
        .perform_task_action(
            &first.id,
            api_types::TaskAction::Cancel { reason: None },
            resting.version,
        )
        .await
        .expect("a step in the gate is cancelled");
    passes(&steps, 3).await;
    steps.release.add_permits(8);
    passes(&steps, 3).await;
    assert_eq!(reload(&steps.db, &first.id).await.status, "cancelled");
    assert!(!running_check(&steps.db, &first.id).await);
    assert_eq!(gate_spent(&steps.db, &first.id).await, 0);
    assert_eq!(
        log(&steps.db, &first.id)
            .await
            .iter()
            .filter(|hop| hop.0 == "lead_check")
            .count(),
        1
    );
}

/// Cancelling the root while a subtask is in the gate ends the check and the
/// subtask with it.
#[tokio::test]
async fn cancelling_the_root_ends_a_step_check_and_its_subtask() {
    let steps = steps(&[HELD_FAIL], Some("second")).await;
    let (root, first, second) = root_with_two_steps(&steps, true).await;
    until(&steps, "the step check runs", async || {
        running_check(&steps.db, &first.id).await
    })
    .await;
    let resting = reload(&steps.db, &root.id).await;
    steps
        .dispatcher
        .task_service
        .perform_task_action(
            &root.id,
            api_types::TaskAction::Cancel { reason: None },
            resting.version,
        )
        .await
        .expect("the root is cancelled");
    passes(&steps, 3).await;
    steps.release.add_permits(8);
    passes(&steps, 3).await;
    assert_eq!(reload(&steps.db, &root.id).await.status, "cancelled");
    for child in [&first, &second] {
        assert_eq!(
            reload(&steps.db, &child.id).await.status,
            "cancelled",
            "{}",
            dump(&steps.db).await
        );
    }
    assert!(!running_check(&steps.db, &first.id).await);
}

/// A root whose subtasks are running offers no Hold (as before this gate
/// existed): a step in the gate is held on the subtask itself.
#[tokio::test]
async fn a_root_with_a_step_in_the_gate_offers_no_hold() {
    let steps = steps(&[HELD_FAIL], Some("second")).await;
    let (root, first, _second) = root_with_two_steps(&steps, true).await;
    until(&steps, "the step check runs", async || {
        running_check(&steps.db, &first.id).await
    })
    .await;
    let offers = steps
        .dispatcher
        .task_service
        .test_action_values(&root.id)
        .await
        .unwrap();
    assert!(
        !offers.contains(&api_types::TaskAction::Hold { reason: None }),
        "{offers:?}"
    );
}

/// A Project pause while a check runs does not lose the step: after the
/// resume the subtask reaches its verdict.
#[tokio::test]
async fn a_project_pause_during_a_step_check_does_not_strand_the_step() {
    let steps = steps(&[HELD_FAIL, PASS, PASS], Some("second")).await;
    let (_root, first, _second) = root_with_two_steps(&steps, true).await;
    until(&steps, "the step check runs", async || {
        running_check(&steps.db, &first.id).await
    })
    .await;
    for paused_at in [Some(now_rfc3339()), None] {
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
                paused_at: Some(paused_at),
                updated_at: now_rfc3339(),
            },
            project.version,
            None,
        )
        .await
        .unwrap();
        passes(&steps, 3).await;
        steps.release.add_permits(8);
        passes(&steps, 2).await;
    }
    until(&steps, "the step is done", async || {
        reload(&steps.db, &first.id).await.status == "done"
    })
    .await;
}

/// A subtask in the gate holds the one Project slot it held while it was
/// implemented: the lead's check is that Task's run, not a second slot, and
/// its root holds none beside it. A parked step holds none.
#[tokio::test]
async fn a_step_in_the_gate_holds_one_project_slot_and_a_parked_one_none() {
    let steps = steps(&[HELD_FAIL, FAIL], None).await;
    let project = ProjectRepo::get_by_id(&*steps.db, &steps.project)
        .await
        .unwrap()
        .unwrap();
    let mut settings: serde_json::Value = serde_json::from_str(&project.settings).unwrap();
    settings["max_active_tasks"] = serde_json::json!(1);
    ProjectRepo::update_at_version(
        &*steps.db,
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
    .unwrap();
    let (_root, first, _second) = root_with_two_steps(&steps, true).await;
    // With a limit of one, the whole flow still runs: the check is admitted
    // in the slot the subtask already holds.
    until(&steps, "the step check runs", async || {
        running_check(&steps.db, &first.id).await
    })
    .await;
    let slots = async || {
        let project = ProjectRepo::get_by_id(&*steps.db, &steps.project)
            .await
            .unwrap()
            .unwrap();
        crate::task_dispatcher::slots::load_project_slots(&steps.db, &project)
            .await
            .unwrap()
    };
    let resting = slots().await;
    assert_eq!((resting.active, resting.parked), (1, 0), "{resting:?}");
    steps.release.add_permits(8);
    until(
        &steps,
        "the subtask is parked on its spent budget",
        async || {
            let task = reload(&steps.db, &first.id).await;
            task.status == "lead_check" && task.condition.read().interruption_present
        },
    )
    .await;
    let parked = slots().await;
    assert_eq!(parked.parked, 1, "{parked:?}");
}

/// A lead assigned while a step rests in the gate (nothing skipped it yet:
/// the Project is paused) checks that step: the assignment marks the subtask
/// and the dispatcher starts the check instead of the skip.
#[tokio::test]
async fn a_lead_assigned_while_a_step_rests_in_the_gate_checks_it() {
    let steps = steps(&[PASS], None).await;
    let (root, child) = in_progress_root_with_a_step_in_the_gate(&steps, false).await;
    let pause = async |paused_at: Option<String>| {
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
                paused_at: Some(paused_at),
                updated_at: now_rfc3339(),
            },
            project.version,
            None,
        )
        .await
        .unwrap();
    };
    pause(Some(now_rfc3339())).await;
    passes(&steps, 2).await;
    assert_eq!(reload(&steps.db, &child.id).await.status, "lead_check");
    let now = now_rfc3339();
    steps
        .dispatcher
        .task_service
        .reassign_role(
            db::CreateTaskRoleAssignment {
                id: new_uuid_v4(),
                task_id: root.id.clone(),
                role_name: crate::workflow::default_roles::PLANNER.to_owned(),
                assignee_type: Some(db::AssigneeKind::Agent),
                assignee_id: Some(steps.planner.clone()),
                created_at: now.clone(),
                updated_at: now,
            },
            false,
            false,
        )
        .await
        .expect("the lead is assigned on the root");
    pause(None).await;
    until(&steps, "the step is done", async || {
        reload(&steps.db, &child.id).await.status == "done"
    })
    .await;
    assert_eq!(
        executions(&steps.db, &child.id).await,
        vec![("planner".to_owned(), "completed".to_owned())],
        "the new lead checked it"
    );
    assert_eq!(
        log(&steps.db, &child.id).await,
        vec![hop("lead_check", "done", None, false)]
    );
}

// ---------------------------------------------------------------------------
// Stage 3 follow-ups.
// ---------------------------------------------------------------------------

const HELD_PASS: &str = "@hold:Checked.\n{\"result\":\"pass\",\"reason\":\"verified\"}";

/// The lead's Agent is archived while its check runs. The assignment row is
/// cleared by the archive sweep; that is a lead change like any other: the
/// run is stopped and the subtask passes the gate now, not when the run ends.
#[tokio::test]
async fn archiving_the_lead_s_agent_while_its_check_runs_stops_the_check_and_passes_the_step() {
    let steps = steps(&[HELD_FAIL], Some("second")).await;
    let (_root, first, second) = root_with_two_steps(&steps, true).await;
    until(&steps, "the step check runs", async || {
        running_check(&steps.db, &first.id).await
    })
    .await;
    steps
        .dispatcher
        .task_service
        .on_agent_deleted(&steps.planner)
        .await
        .expect("the lead's Agent is swept");
    assert!(
        !running_check(&steps.db, &first.id).await,
        "the archived lead's run is stopped by the sweep itself: {}",
        dump(&steps.db).await
    );
    // Without the run ever answering.
    until(&steps, "the step is done", async || {
        reload(&steps.db, &first.id).await.status == "done"
    })
    .await;
    steps.release.add_permits(8);
    passes(&steps, 4).await;
    let hops = log(&steps.db, &first.id).await;
    assert_eq!(
        hops.last(),
        Some(&hop("lead_check", "done", Some("gate_skipped"), false)),
        "{hops:?}"
    );
    assert_eq!(gate_spent(&steps.db, &first.id).await, 0);
    until(&steps, "the next step runs", async || {
        !executions(&steps.db, &second.id).await.is_empty()
    })
    .await;
}

/// The archive sweep wakes a subtask that rests in the gate with no run (here
/// behind a Project pause): it is marked for the dispatcher at once.
#[tokio::test]
async fn archiving_the_lead_s_agent_wakes_a_step_that_waits_in_the_gate() {
    let steps = steps(&[], None).await;
    let (_root, child) = in_progress_root_with_a_step_in_the_gate(&steps, true).await;
    ProjectRepo::set_paused_at(&*steps.db, &steps.project, Some(now_rfc3339()))
        .await
        .unwrap();
    passes(&steps, 2).await;
    assert_eq!(executions(&steps.db, &child.id).await, Vec::new());
    sqlx::query("UPDATE task_schedule_dirty SET dirty=0 WHERE task_id=?")
        .bind(&child.id)
        .execute(steps.db.pool())
        .await
        .unwrap();
    steps
        .dispatcher
        .task_service
        .on_agent_deleted(&steps.planner)
        .await
        .expect("the lead's Agent is swept");
    let dirty: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM task_schedule_dirty WHERE task_id=? AND dirty=1)",
    )
    .bind(&child.id)
    .fetch_one(steps.db.pool())
    .await
    .unwrap();
    assert!(dirty, "the sweep marks the waiting step");
    ProjectRepo::set_paused_at(&*steps.db, &steps.project, None)
        .await
        .unwrap();
    until(&steps, "the step is done", async || {
        reload(&steps.db, &child.id).await.status == "done"
    })
    .await;
    assert_eq!(executions(&steps.db, &child.id).await, Vec::new());
}

/// The lead's Agent is paused and no check runs: the subtask does not pass
/// (a pause is temporary, and nobody validated the step) and does not rest
/// silently: it is parked on that Agent by name, as a planning root is on its
/// paused planner. Resuming the Agent starts the check.
#[tokio::test]
async fn a_paused_lead_agent_parks_the_step_on_that_agent_until_it_resumes() {
    let steps = steps(&[PASS], None).await;
    let (_root, child) = in_progress_root_with_a_step_in_the_gate(&steps, true).await;
    db::AgentRepo::set_paused(&*steps.db, &steps.planner, true)
        .await
        .unwrap();
    passes(&steps, 4).await;
    let parked = reload(&steps.db, &child.id).await;
    assert_eq!(parked.status, "lead_check", "{}", dump(&steps.db).await);
    assert_eq!(executions(&steps.db, &child.id).await, Vec::new());
    let stored = condition(&steps.db, &child.id).await;
    let stored: serde_json::Value = serde_json::from_str(&stored).expect("a condition is stored");
    assert_eq!(stored["kind"], "parked", "{stored}");
    assert_eq!(stored["primary"]["kind"], "agent", "{stored}");
    assert_eq!(
        stored["primary"]["agent_id"],
        steps.planner.as_str(),
        "the park names the paused lead: {stored}"
    );
    assert_eq!(stored["primary"]["status"], "paused", "{stored}");

    db::AgentRepo::set_paused(&*steps.db, &steps.planner, false)
        .await
        .unwrap();
    until(&steps, "the step is done", async || {
        reload(&steps.db, &child.id).await.status == "done"
    })
    .await;
    assert_eq!(
        log(&steps.db, &child.id).await,
        vec![hop("lead_check", "done", None, false)],
        "the lead's own pass, not a skip"
    );
}

/// The lead's Agent is paused while its check runs: a pause starts nothing
/// new and stops nothing, as for every other run. The check finishes and its
/// verdict settles the step.
#[tokio::test]
async fn pausing_the_lead_s_agent_lets_its_running_check_finish_and_settle() {
    let steps = steps(&[HELD_PASS], Some("second")).await;
    let (_root, first, _second) = root_with_two_steps(&steps, true).await;
    // The run is past its launch: the executor holds its reply.
    until(&steps, "the step check runs", async || {
        running_check(&steps.db, &first.id).await && steps.replies.lock().unwrap().is_empty()
    })
    .await;
    db::AgentRepo::set_paused(&*steps.db, &steps.planner, true)
        .await
        .unwrap();
    passes(&steps, 3).await;
    assert!(
        running_check(&steps.db, &first.id).await,
        "{}",
        dump(&steps.db).await
    );
    steps.release.add_permits(1);
    until(&steps, "the step is done", async || {
        reload(&steps.db, &first.id).await.status == "done"
    })
    .await;
    assert_eq!(
        log(&steps.db, &first.id).await.last(),
        Some(&hop("lead_check", "done", None, false))
    );
}

/// Project default role assignments are given to a subtask the dispatcher
/// releases (`Step::ApplyDefaults`). The step-check role is not among them:
/// on a subtask it is the root's lead and nothing else, and no row is copied.
#[tokio::test]
async fn project_defaults_do_not_copy_the_step_check_role_onto_a_subtask() {
    let steps = steps(&[], None).await;
    let settings = serde_json::json!({"default_role_assignments": [
        {"role_name": "planner", "assignee_type": "agent", "assignee_id": steps.planner},
        {"role_name": "coder", "assignee_type": "agent", "assignee_id": steps.coder},
    ]});
    sqlx::query("UPDATE project SET settings=? WHERE id=?")
        .bind(settings.to_string())
        .bind(&steps.project)
        .execute(steps.db.pool())
        .await
        .unwrap();
    let root = seed_task(&steps.db, &steps.project, "root", "in_progress", 1).await;
    let child = seed_subtask(&steps.db, &root, "first", "todo", 0).await;
    steps
        .dispatcher
        .task_service
        .assign_project_default_roles(&child)
        .await
        .expect("defaults are applied");
    let roles: Vec<String> = sqlx::query_scalar(
        "SELECT role_name FROM task_role_assignment WHERE task_id=? ORDER BY role_name",
    )
    .bind(&child.id)
    .fetch_all(steps.db.pool())
    .await
    .unwrap();
    assert_eq!(roles, vec!["coder".to_owned()]);
    // A root still gets every default role.
    let other = seed_task(&steps.db, &steps.project, "single", "todo", 1).await;
    steps
        .dispatcher
        .task_service
        .assign_project_default_roles(&other)
        .await
        .expect("defaults are applied");
    let roles: Vec<String> = sqlx::query_scalar(
        "SELECT role_name FROM task_role_assignment WHERE task_id=? ORDER BY role_name",
    )
    .bind(&other.id)
    .fetch_all(steps.db.pool())
    .await
    .unwrap();
    assert_eq!(roles, vec!["coder".to_owned(), "planner".to_owned()]);
}

