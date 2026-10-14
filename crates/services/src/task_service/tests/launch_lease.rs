use super::*;
use helpers::{initialize_primary_repository, seed_agent, seed_project_repo, sqlite_db};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex as StdMutex, OnceLock,
};

type Writer = Arc<dyn Fn(usize) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

struct Probe {
    db: Arc<SqliteDb>,
    writer: Writer,
    attempts: AtomicUsize,
}

fn probes() -> &'static StdMutex<HashMap<String, Arc<Probe>>> {
    static PROBES: OnceLock<StdMutex<HashMap<String, Arc<Probe>>>> = OnceLock::new();
    PROBES.get_or_init(StdMutex::default)
}

fn probe(task: &str) -> Option<Arc<Probe>> {
    probes().lock().unwrap().get(task).cloned()
}

struct Registered(String);

impl Drop for Registered {
    fn drop(&mut self) {
        probes().lock().unwrap().remove(&self.0);
    }
}

fn register(task: &str, db: Arc<SqliteDb>, writer: Writer) -> Registered {
    probes().lock().unwrap().insert(
        task.to_owned(),
        Arc::new(Probe {
            db,
            writer,
            attempts: AtomicUsize::new(0),
        }),
    );
    Registered(task.to_owned())
}

pub(crate) async fn before_launch(task: &str) {
    if let Some(probe) = probe(task) {
        // Force a stale grant only at the final launch check, after preparation.
        bump(&probe.db, task).await;
    }
}

pub(crate) async fn before_reissue(task: &str) {
    if let Some(probe) = probe(task) {
        let attempt = probe.attempts.fetch_add(1, Ordering::SeqCst);
        (probe.writer)(attempt).await;
    }
}

fn after_probes() -> &'static StdMutex<HashMap<String, Writer>> {
    static AFTER: OnceLock<StdMutex<HashMap<String, Writer>>> = OnceLock::new();
    AFTER.get_or_init(StdMutex::default)
}

struct RegisteredAfter(String);

impl Drop for RegisteredAfter {
    fn drop(&mut self) {
        after_probes().lock().unwrap().remove(&self.0);
    }
}

fn register_after(task: &str, writer: Writer) -> RegisteredAfter {
    after_probes()
        .lock()
        .unwrap()
        .insert(task.to_owned(), writer);
    RegisteredAfter(task.to_owned())
}

/// Right after a lease replacement committed.
pub(crate) async fn after_reissue(task: &str) {
    let writer = after_probes().lock().unwrap().get(task).cloned();
    if let Some(writer) = writer {
        writer(0).await;
    }
}

async fn bump(db: &SqliteDb, task: &str) {
    let fresh = TaskRepo::get_by_id(db, task, false).await.unwrap().unwrap();
    TaskRepo::mutate_metadata_and_bump_version(
        db,
        task,
        fresh.version,
        vec![db::TaskMetadataMutation::Set {
            key: "launch_probe".into(),
            value: json!(fresh.version),
        }],
        &now_rfc3339(),
    )
    .await
    .unwrap();
}

struct CountingExecutor(AtomicUsize);

#[async_trait::async_trait]
impl TaskExecutor for CountingExecutor {
    async fn execute(
        &self,
        _: ExecutionContext,
    ) -> std::result::Result<executors::ExecutionResult, executors::ExecutorError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(executors::ExecutionResult {
            status: ExecutionOutcome::Completed,
            ..Default::default()
        })
    }
    async fn cancel(&self, _: &str) -> std::result::Result<(), executors::ExecutorError> {
        Ok(())
    }
}

async fn fixture() -> (
    Arc<SqliteDb>,
    TaskService,
    db::Task,
    db::Execution,
    tempfile::TempDir,
) {
    let db = Arc::new(sqlite_db().await);
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(16)));
    let (project, _, repo) = seed_project_repo(&db).await;
    initialize_primary_repository(&repo);
    let agent = seed_agent(&db).await;
    sqlx::query("UPDATE daemon SET machine_id = ?")
        .bind(::config::embedded_machine_id())
        .execute(db.pool())
        .await
        .unwrap();
    let task = service
        .create_task(
            project,
            "Launch lease contention",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let claimed = service
        .claim_task(task.id, Assignee::Agent(agent), None)
        .await
        .unwrap();
    (db, service, claimed.task, claimed.execution, repo)
}

#[tokio::test]
async fn launch_lease_retries_two_task_writes_without_execution_retry() {
    let (db, service, task, execution, _repo) = fixture().await;
    let (writer_db, id) = (Arc::clone(&db), task.id.clone());
    let _registered = register(
        &task.id,
        Arc::clone(&db),
        Arc::new(move |attempt| {
            let (db, id) = (Arc::clone(&writer_db), id.clone());
            Box::pin(async move {
                if attempt < 2 {
                    bump(&db, &id).await;
                }
            })
        }),
    );
    let executor = CountingExecutor(AtomicUsize::new(0));
    let result = service.run_execution(execution.id.clone(), &executor).await;
    let terminal = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .unwrap()
        .unwrap();
    let spent = db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
        .await
        .unwrap();
    eprintln!("launch result={:?}; terminal={:?}; stopped_by={:?}; stop_reason={:?}; execution budget spent={spent}", result.as_ref().map(|run| &run.status), terminal.status, terminal.stopped_by, terminal.stop_reason);
    assert_eq!(spent, 0);
    assert_eq!(
        result
            .expect("two harmless writes must not fail launch")
            .status,
        ExecutionStatus::Completed
    );
    assert_eq!(executor.0.load(Ordering::SeqCst), 1);
    assert!(probe(&task.id).unwrap().attempts.load(Ordering::SeqCst) >= 3);
}

#[tokio::test]
async fn launch_lease_persistent_contention_is_infrastructure_without_budget_charge() {
    let (db, service, task, execution, _repo) = fixture().await;
    let (writer_db, id) = (Arc::clone(&db), task.id.clone());
    let _registered = register(
        &task.id,
        Arc::clone(&db),
        Arc::new(move |_| {
            let (db, id) = (Arc::clone(&writer_db), id.clone());
            Box::pin(async move {
                bump(&db, &id).await;
            })
        }),
    );
    let executor = Arc::new(CountingExecutor(AtomicUsize::new(0)));
    let error = service
        .run_execution(execution.id.clone(), executor.as_ref())
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("WorkspaceLease launch contention"),
        "{error}"
    );
    let terminal = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminal.status, ExecutionStatus::Failed);
    assert_eq!(terminal.resume_policy, Some(db::ResumePolicy::Auto));
    assert!(!execution::should_block_task_for_failed_execution(
        &terminal
    ));
    // Exercise the same failure helper the dispatcher's recovery calls.
    service
        .annotate_executor_failure_block(&terminal)
        .await
        .unwrap();
    assert_eq!(
        db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
            .await
            .unwrap(),
        0
    );
    let fresh = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(fresh.error_annotation.is_none());
    assert!(fresh.blocked_json.is_none());
    assert_eq!(executor.0.load(Ordering::SeqCst), 0);
    assert!(probe(&task.id).unwrap().attempts.load(Ordering::SeqCst) <= 32);
    assert!(db::WorkspaceLeaseRepo::get_active_for_task(&*db, &task.id)
        .await
        .unwrap()
        .is_none());
    drop(_registered);
    let service = service.with_task_executor(executor.clone());
    let retry = service
        .dispatch_initial_role_execution(
            &task.id,
            execution.agent_id.as_deref().unwrap(),
            "coder",
            "Retry the infrastructure refusal".into(),
        )
        .await
        .unwrap();
    let terminal = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let run = ExecutionRepo::get_by_id(&*db, &retry.id)
                .await
                .unwrap()
                .unwrap();
            if run.status != ExecutionStatus::Running {
                break run;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the infrastructure refusal is re-dispatched and executed");
    assert_eq!(terminal.status, ExecutionStatus::Completed);
    assert_eq!(executor.0.load(Ordering::SeqCst), 1);
}

async fn changed_precondition(change: &'static str, expected: &str) {
    let (db, service, task, execution, _repo) = fixture().await;
    let other = seed_agent(&db).await;
    let (writer_db, id) = (Arc::clone(&db), task.id.clone());
    let _registered = register(
        &task.id,
        Arc::clone(&db),
        Arc::new(move |attempt| {
            let (db, id, other) = (Arc::clone(&writer_db), id.clone(), other.clone());
            Box::pin(async move {
                if attempt == 0 {
                    bump(&db, &id).await;
                    return;
                }
                if attempt == 1 {
                    match change {
                        "cancelled" => {
                            sqlx::query(
                                "UPDATE task SET status='cancelled', version=version+1 WHERE id=?",
                            )
                            .bind(&id)
                            .execute(db.pool())
                            .await
                            .unwrap();
                        }
                        "held" => {
                            let annotation = db::ConditionStatement::hold_operator_text(
                                "user",
                                "hold during launch",
                                &now_rfc3339(),
                            );
                            sqlx::query("UPDATE task SET error_annotation=?, blocked_json='{}', version=version+1 WHERE id=?")
                            .bind(annotation).bind(&id).execute(db.pool()).await.unwrap();
                            let condition = db::TaskCondition::Parked {
                                primary: db::ParkReason::Held {
                                    actor: "user".into(),
                                },
                                additional: Vec::new(),
                                resume: db::ConditionContinuation::Reconcile,
                                since: None,
                                evidence: Default::default(),
                            };
                            sqlx::query("UPDATE task SET condition_json=? WHERE id=?")
                                .bind(serde_json::to_string(&condition).unwrap())
                                .bind(&id)
                                .execute(db.pool())
                                .await
                                .unwrap();
                        }
                        "assignment" => {
                            sqlx::query(
                                "UPDATE task SET assignee_id=?, version=version+1 WHERE id=?",
                            )
                            .bind(&other)
                            .bind(&id)
                            .execute(db.pool())
                            .await
                            .unwrap();
                            sqlx::query(
                                "UPDATE task_role_assignment SET assignee_id=? WHERE task_id=?",
                            )
                            .bind(&other)
                            .bind(&id)
                            .execute(db.pool())
                            .await
                            .unwrap();
                        }
                        "entry" => {
                            sqlx::query("UPDATE task SET status_epoch=status_epoch+1, version=version+1,
                                condition_json=json_set(condition_json, '$.evidence.witnesses[0].epoch', status_epoch+1) WHERE id=?")
                                .bind(&id).execute(db.pool()).await.unwrap();
                        }
                        _ => unreachable!(),
                    }
                }
            })
        }),
    );
    let executor = CountingExecutor(AtomicUsize::new(0));
    let error = service
        .run_execution(execution.id.clone(), &executor)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains(expected), "{change}: {error}");
    assert!(!error.contains("version conflict"), "{error}");
    assert_eq!(executor.0.load(Ordering::SeqCst), 0);
    assert_eq!(
        db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
            .await
            .unwrap(),
        0
    );
    assert!(db::WorkspaceLeaseRepo::get_active_for_task(&*db, &task.id)
        .await
        .unwrap()
        .is_none());
    if change == "held" {
        let fresh = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert!(fresh
            .condition
            .reasons()
            .any(|reason| matches!(reason, db::ParkReason::Held { .. })));
    }
}

#[tokio::test]
async fn launch_lease_cancellation_after_second_conflict_reports_cancelled() {
    changed_precondition("cancelled", "cancelled").await;
}

#[tokio::test]
async fn launch_lease_hold_after_second_conflict_reports_held() {
    changed_precondition("held", "held").await;
}

#[tokio::test]
async fn launch_lease_reassignment_after_second_conflict_reports_assignment() {
    changed_precondition("assignment", "assigned").await;
}

#[tokio::test]
async fn launch_lease_same_state_reentry_after_second_conflict_stops_launch() {
    changed_precondition("entry", "state entry").await;
}

#[tokio::test]
async fn launch_lease_superseded_run_stops_reissue_without_budget_charge() {
    let (db, service, task, execution, _repo) = fixture().await;
    let (writer_db, id, agent) = (Arc::clone(&db), task.id.clone(), execution.agent_id.clone());
    let _registered = register(
        &task.id,
        Arc::clone(&db),
        Arc::new(move |attempt| {
            let (db, id, agent) = (Arc::clone(&writer_db), id.clone(), agent.clone());
            Box::pin(async move {
                if attempt == 1 {
                    let at = (chrono::Utc::now() + chrono::Duration::seconds(1)).to_rfc3339();
                    helpers::seed_execution(
                        &db,
                        &id,
                        agent.as_deref(),
                        "coder",
                        ExecutionStatus::Completed,
                        None,
                        &at,
                    )
                    .await;
                }
                bump(&db, &id).await;
            })
        }),
    );
    let executor = CountingExecutor(AtomicUsize::new(0));
    let error = service
        .run_execution(execution.id, &executor)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("superseded"), "{error}");
    assert_eq!(executor.0.load(Ordering::SeqCst), 0);
    assert_eq!(
        db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn launch_lease_raw_version_conflict_is_not_an_executor_failure() {
    let (db, service, task, execution, _repo) = fixture().await;
    let terminal = service
        .fail_execution_before_dispatch(&execution.id, db::DbError::VersionConflict.to_string())
        .await
        .unwrap();
    assert!(!execution::should_block_task_for_failed_execution(
        &terminal
    ));
    service
        .annotate_executor_failure_block(&terminal)
        .await
        .unwrap();
    assert_eq!(
        db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
            .await
            .unwrap(),
        0
    );
    assert!(TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap()
        .blocked_json
        .is_none());
}

struct FailedExecutor;

#[async_trait::async_trait]
impl TaskExecutor for FailedExecutor {
    async fn execute(
        &self,
        _: ExecutionContext,
    ) -> std::result::Result<executors::ExecutionResult, executors::ExecutorError> {
        // The same words from a worker that actually ran remain a real failure.
        Ok(executors::ExecutionResult {
            status: ExecutionOutcome::Failed,
            error: Some("version conflict".into()),
            ..Default::default()
        })
    }
    async fn cancel(&self, _: &str) -> std::result::Result<(), executors::ExecutorError> {
        Ok(())
    }
}

#[tokio::test]
async fn launch_lease_real_executor_failure_still_charges_execution_retry() {
    let (db, service, task, execution, _repo) = fixture().await;
    let terminal = service
        .run_execution(execution.id, &FailedExecutor)
        .await
        .unwrap();
    assert_eq!(terminal.status, ExecutionStatus::Failed);
    assert_eq!(terminal.stopped_by.as_deref(), Some("system:executor"));
    assert_eq!(
        db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
            .await
            .unwrap(),
        1
    );
}

async fn settled(db: &SqliteDb, task: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        while !ExecutionRepo::list_running_by_task(db, task)
            .await
            .unwrap()
            .is_empty()
        {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the Task's launch settles");
}

/// The run's start is a separate step from its claim (manual Launch, a
/// session follow-up), and it is the only server-side lease check a
/// daemon-placed run gets. A harmless Task write between the two must not
/// fail the run.
#[tokio::test]
async fn a_task_write_between_claim_and_start_does_not_fail_the_run() {
    let (db, service, task, execution, _repo) = fixture().await;
    bump(&db, &task.id).await;
    let executor = Arc::new(CountingExecutor(AtomicUsize::new(0)));
    let service = service.with_task_executor(executor.clone());
    let started = service.start_execution(execution.id.clone()).await;
    settled(&db, &task.id).await;
    let terminal = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        started.as_ref().is_ok_and(|started| started.accepted),
        "{started:?}; terminal error: {:?}",
        terminal.error
    );
    assert_eq!(terminal.status, ExecutionStatus::Completed);
    assert_eq!(executor.0.load(Ordering::SeqCst), 1);
    let fresh = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(fresh.blocked_json.is_none(), "{:?}", fresh.blocked_json);
}

/// A Task that is written through every launch is not launched forever:
/// each refused launch is re-dispatched by the scheduler, a bounded number
/// of times, and then the Task parks. No execution retry is spent.
#[tokio::test]
async fn launch_contention_on_every_dispatch_parks_after_a_bound_without_budget() {
    let (db, service, task, execution, _repo) = fixture().await;
    let (writer_db, id) = (Arc::clone(&db), task.id.clone());
    let _registered = register(
        &task.id,
        Arc::clone(&db),
        Arc::new(move |_| {
            let (db, id) = (Arc::clone(&writer_db), id.clone());
            Box::pin(async move {
                bump(&db, &id).await;
            })
        }),
    );
    let executor = Arc::new(CountingExecutor(AtomicUsize::new(0)));
    let service = Arc::new(service.with_task_executor(executor.clone()));
    service
        .run_execution(execution.id.clone(), executor.as_ref())
        .await
        .unwrap_err();
    let dispatcher = crate::task_dispatcher::TaskDispatcher::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(64)),
        Arc::clone(&service),
    );
    for _ in 0..8 {
        dispatcher.check_once().await.unwrap();
        service.drain(&task.id).await.unwrap();
        settled(&db, &task.id).await;
    }
    let refused: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution WHERE task_id = ? AND status = 'failed' AND error LIKE '%WorkspaceLease launch contention%'",
    )
    .bind(&task.id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    let fresh = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    eprintln!(
        "contention refusals={refused}; blocked={:?}; condition={:?}",
        fresh.blocked_json, fresh.condition
    );
    assert!(refused >= 2, "the refused launch is re-dispatched: {refused}");
    assert!(refused <= 3, "launches under contention are bounded: {refused}");
    assert!(fresh.blocked_json.is_some(), "the Task parks after the bound");
    assert_eq!(executor.0.load(Ordering::SeqCst), 0);
    assert_eq!(
        db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
            .await
            .unwrap(),
        0
    );
}

/// A replacement that committed is the run's grant, verified exactly. A Task
/// write right after it is like a write right after a strict verify, which
/// the launch does not re-check: it must not send the reissue round again.
#[tokio::test]
async fn a_task_write_after_a_committed_reissue_does_not_retry_the_launch() {
    let (db, service, task, execution, _repo) = fixture().await;
    let _registered = register(
        &task.id,
        Arc::clone(&db),
        Arc::new(|_| Box::pin(async {})),
    );
    let (writer_db, id) = (Arc::clone(&db), task.id.clone());
    let _after = register_after(
        &task.id,
        Arc::new(move |_| {
            let (db, id) = (Arc::clone(&writer_db), id.clone());
            Box::pin(async move {
                bump(&db, &id).await;
            })
        }),
    );
    let executor = CountingExecutor(AtomicUsize::new(0));
    let result = service.run_execution(execution.id.clone(), &executor).await;
    assert_eq!(
        result
            .expect("a write after the committed replacement must not fail launch")
            .status,
        ExecutionStatus::Completed
    );
    assert_eq!(executor.0.load(Ordering::SeqCst), 1);
    assert_eq!(probe(&task.id).unwrap().attempts.load(Ordering::SeqCst), 1);
}

/// An Agent paused while its run's lease is being reissued does not launch:
/// the run ends as the Agent wait (no retry spent, no blocker), as a pause
/// before an unchanged grant's check does.
#[tokio::test]
async fn an_agent_paused_during_the_reissue_defers_the_launch() {
    let (db, service, task, execution, _repo) = fixture().await;
    let agent = execution.agent_id.clone().unwrap();
    let (writer_db, id) = (Arc::clone(&db), task.id.clone());
    let _registered = register(
        &task.id,
        Arc::clone(&db),
        Arc::new(move |attempt| {
            let (db, id, agent) = (Arc::clone(&writer_db), id.clone(), agent.clone());
            Box::pin(async move {
                if attempt == 1 {
                    sqlx::query("UPDATE agent_identity SET paused = 1, version = version + 1 WHERE id = ?")
                        .bind(&agent)
                        .execute(db.pool())
                        .await
                        .unwrap();
                }
                if attempt < 2 {
                    bump(&db, &id).await;
                }
            })
        }),
    );
    let executor = CountingExecutor(AtomicUsize::new(0));
    let ended = service
        .run_execution(execution.id.clone(), &executor)
        .await
        .expect("a paused Agent defers the launch");
    assert_eq!(ended.status, ExecutionStatus::Failed);
    assert!(
        execution::is_agent_paused_pre_dispatch_failure(&ended),
        "{:?}",
        ended.error
    );
    assert_eq!(executor.0.load(Ordering::SeqCst), 0);
    assert_eq!(
        db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
            .await
            .unwrap(),
        0
    );
    let fresh = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(fresh.blocked_json.is_none(), "{:?}", fresh.blocked_json);
}
