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
