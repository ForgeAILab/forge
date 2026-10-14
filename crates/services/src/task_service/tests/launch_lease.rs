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
