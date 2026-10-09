use super::*;
use api_types::*;
use db::{
    CheckCleanup, CheckConsumerOrigin, CheckDeliveryRepo, CheckResultEvidence, CheckResultOutcome,
    CheckRunFence, CheckRunIdentity, CheckRunState, SqliteDb, TaskStepRepo,
};

async fn fixture() -> (tempfile::TempDir, Arc<SqliteDb>, Arc<CheckRunner>) {
    let temp = tempfile::tempdir().unwrap();
    let pool = db::create_sqlite_pool(&format!(
        "sqlite://{}",
        temp.path().join("checks.sqlite").display()
    ))
    .await
    .unwrap();
    db::run_migrations(&pool).await.unwrap();
    let store = Arc::new(SqliteDb::new(pool));
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES('p','project',?,?)")
        .bind(&now)
        .bind(&now)
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO repo(id,project_id,name,default_branch,created_at,updated_at) VALUES('r','p','repo','main',?,?)").bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('t','p','task','review',?,?)").bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    let runner = Arc::new(CheckRunner::new(store.clone()));
    (temp, store, runner)
}
fn request(key: &str) -> CheckRunRequest {
    CheckRunRequest {
        identity: CheckRunIdentity {
            project_id: "p".into(),
            repo_id: "r".into(),
            commit_sha: "a".repeat(40),
            inputs: CheckDigestInput {
                spec: check_executor::legacy_ci_spec("true", &Default::default(), 0, false),
                environment: Default::default(),
                environment_identity: CheckEnvironmentIdentity::Attested {
                    input_digest: "a".repeat(64),
                },
                execution_revision: CheckExecutionRevision {
                    number: 0,
                    audit_ref: None,
                },
            },
        },
        request_key: key.into(),
        task_id: Some("t".into()),
        status_epoch: 0,
        origin: CheckConsumerOrigin::Entry,
        purpose: CheckPurpose::EntryCi,
        workspace_id: None,
        machine_id: None,
        wall_timeout_seconds: 1800,
    }
}
fn cacheable_request(key: &str) -> CheckRunRequest {
    let mut req = request(key);
    req.identity.inputs.spec.commands[0].cacheability = CheckCacheability::DeclaredControlledInputs;
    req
}
fn fence(run: &StoredCheckRun) -> CheckRunFence {
    CheckRunFence {
        run_id: run.id.clone(),
        version: run.version,
        lease_owner: run.lease_owner.clone(),
        lease_generation: run.lease_generation,
    }
}
async fn settle(
    store: &SqliteDb,
    run: &StoredCheckRun,
    outcome: CheckResultOutcome,
    cleanup: CheckCleanup,
) -> StoredCheckResult {
    let now = db::now_rfc3339();
    let until = (chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339();
    let run = store
        .claim_check_run(&run.id, run.version, "test", &now, &until)
        .await
        .unwrap();
    let run = store
        .transition_check_run(&fence(&run), CheckRunState::Cleaning, &now)
        .await
        .unwrap();
    store
        .finish_check_run(
            &fence(&run),
            CheckResultEvidence {
                outcome,
                cleanup,
                commands: vec![CheckCommandOutcome {
                    index: 0,
                    command: "true".into(),
                    exit_code: if outcome == CheckResultOutcome::Pass {
                        0
                    } else {
                        1
                    },
                    stderr_tail: String::new(),
                    output_tail: String::new(),
                    started_at: now.clone(),
                    finished_at: now.clone(),
                }],
                output_truncated: false,
                redaction_values: vec![],
            },
            &now,
        )
        .await
        .unwrap()
}
fn scheduled(reply: RequestedCheck) -> StoredCheckRun {
    match reply.outcome {
        CheckRequestOutcome::Scheduled(run) => run,
        other => panic!("expected scheduled, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_requests_share_one_run_and_each_consumer_gets_one_durable_step() {
    let (_temp, store, runner) = fixture().await;
    let barrier = Arc::new(tokio::sync::Barrier::new(16));
    let mut tasks = Vec::new();
    for index in 0..16 {
        let (runner, barrier) = (runner.clone(), barrier.clone());
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            runner
                .request(request(&format!("consumer-{index}")))
                .await
                .unwrap()
        }));
    }
    let mut one_run = None;
    let mut scheduled_count = 0;
    let mut ids = std::collections::BTreeSet::new();
    for task in tasks {
        let reply = task.await.unwrap();
        assert!(ids.insert(reply.consumer.id));
        let run = match reply.outcome {
            CheckRequestOutcome::Scheduled(run) => {
                scheduled_count += 1;
                run
            }
            CheckRequestOutcome::Joined(run) => run,
            other => panic!("unexpected {other:?}"),
        };
        if let Some(first) = &one_run {
            assert_eq!(&run.id, first);
        } else {
            one_run = Some(run.id);
        }
    }
    assert_eq!(scheduled_count, 1);
    let run = store
        .check_run(one_run.as_deref().unwrap())
        .await
        .unwrap()
        .unwrap();
    let result = settle(
        &store,
        &run,
        CheckResultOutcome::Pass,
        CheckCleanup::NotPerformed,
    )
    .await;
    // Crash after result commit and before delivery: close/reopen storage.
    store.pool().close().await;
    let reopened = Arc::new(SqliteDb::new(
        db::create_sqlite_pool(&format!(
            "sqlite://{}",
            _temp.path().join("checks.sqlite").display()
        ))
        .await
        .unwrap(),
    ));
    let mut deliveries = Vec::new();
    for _ in 0..4 {
        let store = reopened.clone();
        deliveries.push(tokio::spawn(async move {
            store.enqueue_check_result_steps(100).await.unwrap()
        }));
    }
    let mut count = 0;
    for delivery in deliveries {
        count += delivery.await.unwrap();
    }
    assert_eq!(count, 16);
    let steps = reopened.task_steps("t").await.unwrap();
    assert_eq!(steps.len(), 16);
    for step in &steps {
        let payload: db::CheckResultDelivery = serde_json::from_value(
            serde_json::from_str::<serde_json::Value>(&step.payload_json).unwrap()["arguments"]
                .clone(),
        )
        .unwrap();
        assert!(ids.remove(&payload.consumer_id));
        assert_eq!(payload.result_id, result.id);
        assert_eq!(payload.identity_key, result.identity_key);
    }
    assert!(ids.is_empty());
    // Crash after enqueue, and even subsequent outbox pruning, cannot deliver
    // again: the independent check-consumer marker survives both.
    sqlx::query("DELETE FROM task_step WHERE task_id='t'")
        .execute(reopened.pool())
        .await
        .unwrap();
    assert_eq!(reopened.enqueue_check_result_steps(100).await.unwrap(), 0);
    reopened.pool().close().await;
}

#[tokio::test]
async fn certified_cache_hit_executes_nothing_and_idempotency_keeps_one_consumer() {
    let (_temp, store, runner) = fixture().await;
    let run = scheduled(runner.request(cacheable_request("first")).await.unwrap());
    let result = settle(
        &store,
        &run,
        CheckResultOutcome::Pass,
        CheckCleanup::NotPerformed,
    )
    .await;
    let second = runner.request(cacheable_request("second")).await.unwrap();
    let id = second.consumer.id.clone();
    assert!(matches!(second.outcome, CheckRequestOutcome::Hit(hit) if hit.id==result.id));
    let again = runner.request(cacheable_request("second")).await.unwrap();
    assert_eq!(again.consumer.id, id);
    assert!(matches!(again.outcome, CheckRequestOutcome::Hit(hit) if hit.id==result.id));
    let counts = store.check_run_counts().await.unwrap();
    assert_eq!(counts.by_state["succeeded"], 1);
    assert_eq!(counts.by_state["queued"], 0);
    assert_eq!(store.enqueue_check_result_steps(100).await.unwrap(), 2);
    assert_eq!(store.enqueue_check_result_steps(100).await.unwrap(), 0);
}

#[tokio::test]
async fn new_consumers_never_hit_uncacheable_uncertified_or_different_inputs() {
    for case in [
        "uncacheable",
        "not_attested",
        "failed",
        "timed_out",
        "cleanup_failed",
        "revision",
    ] {
        let (_temp, store, runner) = fixture().await;
        let mut first = cacheable_request("first");
        if case == "uncacheable" {
            first = request("first");
        }
        if case == "not_attested" {
            first.identity.inputs.environment_identity = CheckEnvironmentIdentity::NotAttested;
        }
        let mut second = first.clone();
        second.request_key = "second".into();
        if case == "revision" {
            second.identity.inputs.execution_revision = CheckExecutionRevision {
                number: 1,
                audit_ref: Some("force-1".into()),
            };
        }
        let run = scheduled(runner.request(first).await.unwrap());
        let outcome = match case {
            "failed" => CheckResultOutcome::Fail,
            "timed_out" => CheckResultOutcome::TimedOut,
            _ => CheckResultOutcome::Pass,
        };
        let cleanup = if case == "cleanup_failed" {
            CheckCleanup::Failed
        } else {
            CheckCleanup::NotPerformed
        };
        settle(&store, &run, outcome, cleanup).await;
        let next = scheduled(runner.request(second).await.unwrap());
        assert_ne!(next.id, run.id, "{case}");
        store.pool().close().await;
    }
}

#[tokio::test]
async fn uncertain_run_is_joined_until_reconciliation_and_has_no_delivery() {
    let (_temp, store, runner) = fixture().await;
    let run = scheduled(runner.request(cacheable_request("first")).await.unwrap());
    settle(
        &store,
        &run,
        CheckResultOutcome::Pass,
        CheckCleanup::Uncertain,
    )
    .await;
    assert!(
        matches!(runner.request(cacheable_request("second")).await.unwrap().outcome, CheckRequestOutcome::Joined(joined) if joined.id==run.id && joined.state==CheckRunState::Uncertain)
    );
    assert_eq!(store.enqueue_check_result_steps(100).await.unwrap(), 0);
}

#[tokio::test]
async fn idempotent_failure_replays_its_evidence_without_a_silent_new_run() {
    let (_temp, store, runner) = fixture().await;
    let run = scheduled(runner.request(request("first")).await.unwrap());
    let result = settle(
        &store,
        &run,
        CheckResultOutcome::Fail,
        CheckCleanup::NotPerformed,
    )
    .await;
    assert!(
        matches!(runner.request(request("first")).await.unwrap().outcome, CheckRequestOutcome::Joined(joined) if joined.id==result.run_id && joined.state==CheckRunState::Failed)
    );
    let mut stale = request("first");
    stale.status_epoch = 1;
    assert!(matches!(
        runner.request(stale).await,
        Err(ServiceError::Db(db::DbError::IdempotencyConflict))
    ));
}

#[tokio::test]
async fn a_stale_epoch_is_superseded_before_the_delivery_can_apply() {
    let (_temp, store, runner) = fixture().await;
    let run = scheduled(runner.request(request("first")).await.unwrap());
    settle(
        &store,
        &run,
        CheckResultOutcome::Pass,
        CheckCleanup::NotPerformed,
    )
    .await;
    sqlx::query("UPDATE task SET status_epoch=1 WHERE id='t'")
        .execute(store.pool())
        .await
        .unwrap();
    let before: (String, i64, i64) =
        sqlx::query_as("SELECT status,status_epoch,version FROM task WHERE id='t'")
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(store.enqueue_check_result_steps(100).await.unwrap(), 1);
    let until = (chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339();
    assert!(store
        .claim_step("test", Some("t"), &until)
        .await
        .unwrap()
        .is_none());
    assert_eq!(store.task_steps("t").await.unwrap()[0].status, "superseded");
    let after: (String, i64, i64) =
        sqlx::query_as("SELECT status,status_epoch,version FROM task WHERE id='t'")
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(before, after);
}

use super::worker::{CheckOwnerPort, CheckRunWorker};
use db::{
    CheckAdmission, CheckDispatchIntent, CheckDispatchTarget, CheckWorkerRecord, CheckWorkerRepo,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct TestOwner {
    runs: AtomicUsize,
    lookups: AtomicUsize,
    cancels: AtomicUsize,
    acknowledgments: AtomicUsize,
    interrupted: bool,
    gone: bool,
    /// The owner keeps reporting the operation as running.
    running: bool,
    refuse_ack: bool,
}
fn intent() -> CheckDispatchIntent {
    CheckDispatchIntent {
        target: CheckDispatchTarget::Server {
            path: "owner-local-handle".into(),
            workspace_id: "w".into(),
            placement_id: "pl".into(),
            generation: 1,
        },
        owner: CheckOwnerIdentity {
            owner_kind: "server".into(),
            machine_id: None,
            runtime_id: "test-runtime".into(),
        },
        environment_task_id: "t".into(),
    }
}
fn receipt(record: &CheckWorkerRecord) -> CheckReceipt {
    let now = db::now_rfc3339();
    CheckReceipt {
        operation_id: record.run.operation_id.clone(),
        owner: record.dispatch.as_ref().unwrap().owner.clone(),
        execution_inputs: record.run.identity.inputs.environment_identity.clone(),
        commands: record
            .run
            .identity
            .inputs
            .spec
            .commands
            .iter()
            .map(|command| CheckCommandReceipt {
                id: command.id.clone(),
                command: command.shell_text.clone(),
                exit_code: Some(0),
                outcome: CheckExecutionOutcome::Passed,
                duration_ms: 0,
                stdout_tail: "passed".into(),
                stderr_tail: String::new(),
                stdout_truncated: false,
                stderr_truncated: false,
                stdout_drain_incomplete: false,
                stderr_drain_incomplete: false,
                process_tree_stopped: true,
                started_at: now.clone(),
                finished_at: now.clone(),
            })
            .collect(),
        outcome: CheckExecutionOutcome::Passed,
        cleanup: CheckCleanupReceipt {
            outcome: CheckCleanupOutcome::NotPerformed,
            commands: vec![],
            checkout_removed: false,
            message: None,
        },
        prepared_head: Some(record.run.identity.commit_sha.clone()),
        finished_head: Some(record.run.identity.commit_sha.clone()),
        tracked_changes: Some(false),
        started_at: now.clone(),
        finished_at: now,
        infrastructure_message: None,
    }
}
#[async_trait::async_trait]
impl CheckOwnerPort for TestOwner {
    async fn prepare(&self, _run: &StoredCheckRun) -> Result<CheckDispatchIntent> {
        Ok(intent())
    }
    async fn run(
        &self,
        record: &CheckWorkerRecord,
        _cancel: &CancellationToken,
    ) -> Result<DaemonCheckResult> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        assert!(record.dispatch.is_some() && record.deadline_at.is_some());
        if self.interrupted {
            Err(ServiceError::invalid_operation("transport uncertain"))
        } else {
            Ok(DaemonCheckResult::Completed {
                receipt: Box::new(receipt(record)),
            })
        }
    }
    async fn lookup(&self, record: &CheckWorkerRecord) -> Result<DaemonCheckResult> {
        self.lookups.fetch_add(1, Ordering::SeqCst);
        if self.running {
            return Ok(DaemonCheckResult::Running {
                operation_id: record.run.operation_id.clone(),
            });
        }
        if self.interrupted {
            Ok(DaemonCheckResult::Unknown {
                operation_id: record.run.operation_id.clone(),
            })
        } else {
            Ok(DaemonCheckResult::Completed {
                receipt: Box::new(receipt(record)),
            })
        }
    }
    async fn cancel(&self, record: &CheckWorkerRecord) -> Result<DaemonCheckResult> {
        self.cancels.fetch_add(1, Ordering::SeqCst);
        if self.running {
            return Ok(DaemonCheckResult::Running {
                operation_id: record.run.operation_id.clone(),
            });
        }
        Ok(DaemonCheckResult::Unknown {
            operation_id: record.run.operation_id.clone(),
        })
    }
    async fn owner_gone(&self, _record: &CheckWorkerRecord) -> Result<bool> {
        Ok(self.gone)
    }
    async fn acknowledge(&self, _record: &CheckWorkerRecord) -> Result<()> {
        self.acknowledgments.fetch_add(1, Ordering::SeqCst);
        if self.refuse_ack {
            return Err(ServiceError::invalid_operation("owner unreachable"));
        }
        Ok(())
    }
}
async fn admit(store: &SqliteDb, run: &StoredCheckRun) -> StoredCheckRun {
    let now = db::now_rfc3339();
    let until = (chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339();
    match store
        .admit_check_run(run, "test-worker", &now, &until)
        .await
        .unwrap()
    {
        CheckAdmission::Admitted(run) => *run,
        other => panic!("not admitted {other:?}"),
    }
}

#[tokio::test]
async fn worker_records_intent_receipt_result_and_delivery_then_acknowledges_once() {
    let (_temp, store, runner) = fixture().await;
    let run = scheduled(runner.request(cacheable_request("worker")).await.unwrap());
    let owner = Arc::new(TestOwner::default());
    let worker = Arc::new(CheckRunWorker::new(store.clone(), owner.clone()));
    worker.drive(admit(&store, &run).await).await.unwrap();
    assert_eq!(owner.runs.load(Ordering::SeqCst), 1);
    let stored = store.check_worker_record(&run.id).await.unwrap();
    assert_eq!(stored.run.state, CheckRunState::Succeeded);
    assert_eq!(stored.receipt.unwrap().operation_id, run.operation_id);
    assert_eq!(store.task_steps("t").await.unwrap().len(), 1);
    let mut jobs = tokio::task::JoinSet::new();
    worker.sweep(&mut jobs).await.unwrap();
    worker.sweep(&mut jobs).await.unwrap();
    assert!(jobs.is_empty());
    assert_eq!(owner.acknowledgments.load(Ordering::SeqCst), 1);
    assert!(store
        .check_worker_record(&run.id)
        .await
        .unwrap()
        .acknowledged_at
        .is_some());
    assert!(matches!(
        runner
            .request(cacheable_request("cache-hit"))
            .await
            .unwrap()
            .outcome,
        CheckRequestOutcome::Hit(_)
    ));
    assert_eq!(owner.runs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn restart_at_intent_dispatch_and_receipt_boundaries_uses_the_same_operation_key() {
    for boundary in ["intent", "dispatch", "receipt"] {
        let (temp, store, runner) = fixture().await;
        let queued = scheduled(runner.request(request("restart")).await.unwrap());
        let run = admit(&store, &queued).await;
        let deadline = store
            .check_worker_record(&run.id)
            .await
            .unwrap()
            .deadline_at
            .unwrap();
        store
            .record_check_dispatch(&fence(&run), &intent(), &db::now_rfc3339(), &deadline)
            .await
            .unwrap();
        let record = store.check_worker_record(&run.id).await.unwrap();
        let owner = Arc::new(TestOwner {
            interrupted: boundary == "intent",
            ..Default::default()
        });
        if boundary != "intent" {
            let result = owner.run(&record, &CancellationToken::new()).await.unwrap();
            if boundary == "receipt" {
                let DaemonCheckResult::Completed { receipt } = result else {
                    panic!()
                };
                store
                    .record_check_owner_receipt(&run.id, &run.operation_id, &receipt)
                    .await
                    .unwrap();
            }
        }
        let dispatched = owner.runs.load(Ordering::SeqCst);
        // Recover without waiting a real lease minute. A restarted worker
        // takes the expired lease, and can only look up, never dispatch.
        sqlx::query("UPDATE check_run SET lease_until='2000-01-01T00:00:00Z' WHERE id=?")
            .bind(&run.id)
            .execute(store.pool())
            .await
            .unwrap();
        store.pool().close().await;
        let reopened = Arc::new(SqliteDb::new(
            db::create_sqlite_pool(&format!(
                "sqlite://{}",
                temp.path().join("checks.sqlite").display()
            ))
            .await
            .unwrap(),
        ));
        let old = reopened.check_run(&run.id).await.unwrap().unwrap();
        let claimed = admit(&reopened, &old).await;
        assert_eq!(claimed.state, CheckRunState::Uncertain);
        let worker = CheckRunWorker::new(reopened.clone(), owner.clone());
        worker.drive(claimed).await.unwrap();
        assert_eq!(owner.runs.load(Ordering::SeqCst), dispatched, "{boundary}");
        assert_eq!(
            reopened
                .check_worker_record(&run.id)
                .await
                .unwrap()
                .run
                .operation_id,
            queued.operation_id
        );
        if boundary == "intent" {
            assert_eq!(
                reopened.check_run(&run.id).await.unwrap().unwrap().state,
                CheckRunState::Failed
            );
            let next = reopened
                .runnable_check_runs(&db::now_rfc3339(), 10)
                .await
                .unwrap()
                .pop()
                .unwrap();
            let retry_owner = Arc::new(TestOwner::default());
            CheckRunWorker::new(reopened.clone(), retry_owner.clone())
                .drive(admit(&reopened, &next).await)
                .await
                .unwrap();
            assert_eq!(retry_owner.runs.load(Ordering::SeqCst), 1);
        } else {
            assert_eq!(
                reopened.check_run(&run.id).await.unwrap().unwrap().state,
                CheckRunState::Succeeded
            );
        }
        assert_eq!(reopened.task_steps("t").await.unwrap().len(), 1);
        reopened.pool().close().await;
    }
}

#[tokio::test]
async fn infrastructure_retry_is_bounded_per_consumer_and_unknown_is_cancel_fenced() {
    let (_temp, store, runner) = fixture().await;
    runner.request(request("retry")).await.unwrap();
    let owner = Arc::new(TestOwner {
        interrupted: true,
        ..Default::default()
    });
    let worker = CheckRunWorker::new(store.clone(), owner.clone());
    let mut operations = std::collections::BTreeSet::new();
    for retry in 0..=2 {
        let run = store
            .runnable_check_runs(&db::now_rfc3339(), 10)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert!(operations.insert(run.operation_id.clone()));
        worker.drive(admit(&store, &run).await).await.unwrap();
        assert_eq!(
            store.task_steps("t").await.unwrap().len(),
            if retry == 2 { 1 } else { 0 }
        );
    }
    assert_eq!(owner.runs.load(Ordering::SeqCst), 3);
    assert_eq!(owner.cancels.load(Ordering::SeqCst), 3);
    assert_eq!(
        store.check_run_counts().await.unwrap().by_state["failed"],
        3
    );
    assert!(store
        .runnable_check_runs(&db::now_rfc3339(), 10)
        .await
        .unwrap()
        .is_empty());
    assert!(store.retryable_check_runs(10).await.unwrap().is_empty());
    // A repeated request and sweep cannot reset the consumer's retry allowance.
    assert!(
        matches!(runner.request(request("retry")).await.unwrap().outcome,CheckRequestOutcome::Joined(run) if run.state==CheckRunState::Failed)
    );
}

#[tokio::test]
async fn owner_gone_settles_uncertainty_and_cancelled_consumers_never_launch() {
    let (_temp, store, runner) = fixture().await;
    let run = scheduled(runner.request(request("cancel")).await.unwrap());
    sqlx::query("UPDATE task SET status='cancelled',status_epoch=1 WHERE id='t'")
        .execute(store.pool())
        .await
        .unwrap();
    let owner = Arc::new(TestOwner::default());
    let worker = CheckRunWorker::new(store.clone(), owner.clone());
    worker.drive(admit(&store, &run).await).await.unwrap();
    assert_eq!(owner.runs.load(Ordering::SeqCst), 0);
    assert_eq!(
        store.check_run(&run.id).await.unwrap().unwrap().state,
        CheckRunState::Cancelled
    );
    assert!(store.task_steps("t").await.unwrap().is_empty());

    sqlx::query("UPDATE task SET status='review',status_epoch=0 WHERE id='t'")
        .execute(store.pool())
        .await
        .unwrap();
    let mut req = request("owner-gone");
    req.status_epoch = sqlx::query_scalar("SELECT status_epoch FROM task WHERE id='t'")
        .fetch_one(store.pool())
        .await
        .unwrap();
    let run = scheduled(runner.request(req).await.unwrap());
    let owner = Arc::new(TestOwner {
        interrupted: true,
        gone: true,
        ..Default::default()
    });
    CheckRunWorker::new(store.clone(), owner.clone())
        .drive(admit(&store, &run).await)
        .await
        .unwrap();
    assert_eq!(
        store.check_run(&run.id).await.unwrap().unwrap().state,
        CheckRunState::Failed
    );
}

#[tokio::test]
async fn full_owner_machine_queues_without_charging_wall_time_and_join_and_hit_take_no_slot() {
    let (_temp, store, runner) = fixture().await;
    store.server_run_cap.set(Some(1), 1, "server-machine");
    let first = scheduled(
        runner
            .request(cacheable_request("capacity-a"))
            .await
            .unwrap(),
    );
    let first = admit(&store, &first).await;
    let mut second_request = cacheable_request("capacity-b");
    second_request.identity.commit_sha = "b".repeat(40);
    let second = scheduled(runner.request(second_request).await.unwrap());
    let now = db::now_rfc3339();
    let until = (chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339();
    assert!(matches!(
        store
            .admit_check_run(&second, "waiting", &now, &until)
            .await
            .unwrap(),
        CheckAdmission::Waiting
    ));
    let waiting = store.check_worker_record(&second.id).await.unwrap();
    assert!(waiting.admitted_at.is_none() && waiting.deadline_at.is_none());
    assert!(
        matches!(runner.request(cacheable_request("capacity-join")).await.unwrap().outcome,CheckRequestOutcome::Joined(run) if run.id==first.id)
    );
    assert_eq!(store.check_run_counts().await.unwrap().admitted_runs, 1);
    assert_eq!(
        store.check_run_counts().await.unwrap().waiting_for_capacity,
        1
    );
    let owner = Arc::new(TestOwner::default());
    CheckRunWorker::new(store.clone(), owner)
        .drive(first)
        .await
        .unwrap();
    assert!(matches!(
        runner
            .request(cacheable_request("capacity-hit"))
            .await
            .unwrap()
            .outcome,
        CheckRequestOutcome::Hit(_)
    ));
    let second = admit(&store, &waiting.run).await;
    let started = store.check_worker_record(&second.id).await.unwrap();
    let elapsed = (chrono::DateTime::parse_from_rfc3339(started.deadline_at.as_deref().unwrap())
        .unwrap()
        - chrono::DateTime::parse_from_rfc3339(started.admitted_at.as_deref().unwrap()).unwrap())
    .num_seconds();
    assert_eq!(elapsed, 1800);
    assert_eq!(
        store.check_run_counts().await.unwrap().waiting_for_capacity,
        0
    );
    let mut tx = db::begin_immediate(store.pool()).await.unwrap();
    let count =
        db::machine_capacity::count_machine_capacity(&mut tx, None, Some(1), "server-machine")
            .await
            .unwrap();
    assert_eq!(count.check_runs, 1);
    assert_eq!(count.active_runs(), 1);
}

#[tokio::test]
async fn a_check_borrows_the_same_tasks_reservation_and_keeps_its_slot_when_it_expires() {
    let (temp, store, runner) = fixture().await;
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO workspace(id,task_id,repo_id,worktree_path,branch,status,created_at,updated_at) VALUES('w','t','r',?,'task/branch','ready',?,?)").bind(temp.path().to_str()).bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,is_default,status,created_at,updated_at) VALUES('l','r','server',?,'primary_checkout',1,'ready',?,?)").bind(temp.path().to_str()).bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    sqlx::query("INSERT INTO workspace_placement(id,workspace_id,task_id,owner_kind,repo_location_id,workspace_handle,generation,state,selected_by,selection_reason,reserved_until,created_at,updated_at) VALUES('pl','w','t','server','l',?,1,'reserved','scheduler','{}','2099-01-01T00:00:00Z',?,?)").bind(temp.path().to_str()).bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    store.server_run_cap.set(Some(1), 1, "server-machine");
    let run = scheduled(runner.request(request("borrow")).await.unwrap());
    admit(&store, &run).await;
    let mut tx = db::begin_immediate(store.pool()).await.unwrap();
    let count =
        db::machine_capacity::count_machine_capacity(&mut tx, None, Some(1), "server-machine")
            .await
            .unwrap();
    assert_eq!(count.reservations, 1);
    assert_eq!(count.borrowed_check_runs, 1);
    assert_eq!(count.check_runs, 0);
    assert_eq!(count.active_runs(), 1);
    tx.commit().await.unwrap();
    sqlx::query(
        "UPDATE workspace_placement SET reserved_until='2000-01-01T00:00:00Z' WHERE id='pl'",
    )
    .execute(store.pool())
    .await
    .unwrap();
    let mut tx = db::begin_immediate(store.pool()).await.unwrap();
    let count =
        db::machine_capacity::count_machine_capacity(&mut tx, None, Some(1), "server-machine")
            .await
            .unwrap();
    assert_eq!(count.borrowed_check_runs, 0);
    assert_eq!(count.check_runs, 1);
    assert_eq!(count.active_runs(), 1);
}

#[tokio::test]
async fn receipt_validation_rejects_foreign_incomplete_unattested_and_unwitnessed_passes() {
    let (_temp, store, runner) = fixture().await;
    let run = scheduled(runner.request(cacheable_request("certify")).await.unwrap());
    let run = admit(&store, &run).await;
    let deadline = store
        .check_worker_record(&run.id)
        .await
        .unwrap()
        .deadline_at
        .unwrap();
    store
        .record_check_dispatch(&fence(&run), &intent(), &db::now_rfc3339(), &deadline)
        .await
        .unwrap();
    let record = store.check_worker_record(&run.id).await.unwrap();
    let good = receipt(&record);
    super::receipt::validate_receipt(&run, record.dispatch.as_ref().unwrap(), &good).unwrap();
    for change in [
        |r: &mut CheckReceipt| r.operation_id = "foreign".into(),
        |r: &mut CheckReceipt| r.owner.runtime_id = "foreign".into(),
        |r: &mut CheckReceipt| r.execution_inputs = CheckEnvironmentIdentity::NotAttested,
        |r: &mut CheckReceipt| r.commands.clear(),
        |r: &mut CheckReceipt| r.commands[0].exit_code = Some(1),
        |r: &mut CheckReceipt| r.finished_head = None,
        |r: &mut CheckReceipt| r.tracked_changes = Some(true),
    ] {
        let mut bad = good.clone();
        change(&mut bad);
        assert!(
            super::receipt::validate_receipt(&run, record.dispatch.as_ref().unwrap(), &bad)
                .is_err()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_concurrent_capacity_admission_on_a_file_backed_database_has_one_winner() {
    let (_temp, store, runner) = fixture().await;
    store.server_run_cap.set(Some(1), 1, "server-machine");
    let mut candidates = Vec::new();
    for i in 1..=8 {
        let mut req = request(&format!("admission-{i}"));
        req.identity.commit_sha = format!("{i:040x}");
        candidates.push(scheduled(runner.request(req).await.unwrap()));
    }
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut claims = Vec::new();
    for run in candidates {
        let (db, barrier) = (store.clone(), barrier.clone());
        claims.push(tokio::spawn(async move {
            barrier.wait().await;
            let now = db::now_rfc3339();
            let until = (chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339();
            db.admit_check_run(&run, "concurrent", &now, &until)
                .await
                .unwrap()
        }));
    }
    let mut winners = 0;
    for claim in claims {
        if matches!(claim.await.unwrap(), CheckAdmission::Admitted(_)) {
            winners += 1;
        }
    }
    assert_eq!(winners, 1);
    let counts = store.check_run_counts().await.unwrap();
    assert_eq!(counts.admitted_runs, 1);
    assert_eq!(counts.waiting_for_capacity, 7);
}

#[tokio::test]
async fn capacity_is_charged_to_the_owner_machine_and_not_to_an_agent_or_a_join() {
    let (_temp, store, runner) = fixture().await;
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO daemon(id,machine_id,hostname,os,arch,status,max_concurrent_runs,created_at,updated_at) VALUES('m','physical-m','owner','linux','aarch64','online',1,?,?)").bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    let mut req = request("owner-capacity");
    req.machine_id = Some("m".into());
    let run = scheduled(runner.request(req.clone()).await.unwrap());
    admit(&store, &run).await;
    req.request_key = "joined-owner-capacity".into();
    assert!(matches!(
        runner.request(req).await.unwrap().outcome,
        CheckRequestOutcome::Joined(_)
    ));
    let mut tx = db::begin_immediate(store.pool()).await.unwrap();
    let remote =
        db::machine_capacity::count_machine_capacity(&mut tx, Some("m"), Some(1), "server-machine")
            .await
            .unwrap();
    let server =
        db::machine_capacity::count_machine_capacity(&mut tx, None, Some(1), "server-machine")
            .await
            .unwrap();
    let agent = db::machine_capacity::count_agent_capacity(&mut tx, "some-agent")
        .await
        .unwrap();
    assert_eq!(remote.active_runs(), 1);
    assert_eq!(remote.check_runs, 1);
    assert_eq!(server.active_runs(), 0);
    assert_eq!(agent.occupied_slots(), 0);
}

#[tokio::test]
async fn a_server_owner_restart_replays_durable_receipt_without_a_second_process() {
    let (temp, store, runner) = fixture().await;
    let checkout = temp.path().join("repo");
    std::fs::create_dir(&checkout).unwrap();
    async fn git(path: &std::path::Path, args: &[&str]) {
        let output = tokio::process::Command::new("git")
            .args(args)
            .current_dir(path)
            .output()
            .await
            .unwrap();
        assert!(output.status.success(), "fixture Git command failed");
    }
    git(&checkout, &["init", "-q"]).await;
    git(&checkout, &["config", "user.name", "Check test"]).await;
    git(
        &checkout,
        &["config", "user.email", "check@example.invalid"],
    )
    .await;
    git(
        &checkout,
        &["commit", "-q", "--allow-empty", "-m", "candidate"],
    )
    .await;
    let head = git::get_current_sha(&checkout).await.unwrap();
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO workspace(id,task_id,repo_id,worktree_path,branch,status,created_at,updated_at) VALUES('w','t','r',?,'task/branch','ready',?,?)").bind(checkout.to_str()).bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,is_default,status,created_at,updated_at) VALUES('l','r','server',?,'primary_checkout',1,'ready',?,?)").bind(checkout.to_str()).bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    sqlx::query("INSERT INTO workspace_placement(id,workspace_id,task_id,owner_kind,repo_location_id,workspace_handle,generation,state,selected_by,selection_reason,created_at,updated_at) VALUES('pl','w','t','server','l',?,1,'ready','scheduler','{}',?,?)").bind(checkout.to_str()).bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    let mut req = request("real-server");
    req.identity.commit_sha = head;
    req.workspace_id = Some("w".into());
    req.identity.inputs.environment_identity = CheckEnvironmentIdentity::NotAttested;
    req.identity.inputs.spec.commands[0].shell_text = "printf x >> process-count".into();
    let queued = scheduled(runner.request(req).await.unwrap());
    let run = admit(&store, &queued).await;
    let owners = super::owners::WorkspaceCheckOwners::new(
        store.clone(),
        Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers()),
        std::time::Duration::from_secs(60),
    );
    let dispatch = owners.prepare(&run).await.unwrap();
    let deadline = store
        .check_worker_record(&run.id)
        .await
        .unwrap()
        .deadline_at
        .unwrap();
    store
        .record_check_dispatch(&fence(&run), &dispatch, &db::now_rfc3339(), &deadline)
        .await
        .unwrap();
    let record = store.check_worker_record(&run.id).await.unwrap();
    let first = owners
        .run(&record, &CancellationToken::new())
        .await
        .unwrap();
    assert!(
        matches!(first,DaemonCheckResult::Completed { receipt } if receipt.outcome==CheckExecutionOutcome::Passed)
    );
    assert_eq!(
        std::fs::read_to_string(checkout.join("process-count")).unwrap(),
        "x"
    );
    // Crash after receipt but before result. Both worker and server owner
    // are recreated from disk; the operation is never dispatched again.
    sqlx::query("UPDATE check_run SET lease_until='2000-01-01T00:00:00Z' WHERE id=?")
        .bind(&run.id)
        .execute(store.pool())
        .await
        .unwrap();
    store.pool().close().await;
    let reopened = Arc::new(SqliteDb::new(
        db::create_sqlite_pool(&format!(
            "sqlite://{}",
            temp.path().join("checks.sqlite").display()
        ))
        .await
        .unwrap(),
    ));
    let owners = Arc::new(super::owners::WorkspaceCheckOwners::new(
        reopened.clone(),
        Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers()),
        std::time::Duration::from_secs(60),
    ));
    let current = reopened.check_run(&run.id).await.unwrap().unwrap();
    CheckRunWorker::new(reopened.clone(), owners)
        .drive(admit(&reopened, &current).await)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(checkout.join("process-count")).unwrap(),
        "x"
    );
    assert_eq!(
        reopened.check_run(&run.id).await.unwrap().unwrap().state,
        CheckRunState::Succeeded
    );
    assert_eq!(reopened.task_steps("t").await.unwrap().len(), 1);
}

#[tokio::test]
async fn recovery_reuses_a_pass_certified_after_its_failure_instead_of_silently_rerunning() {
    let (_temp, store, runner) = fixture().await;
    let failed = scheduled(
        runner
            .request(cacheable_request("failed-before-crash"))
            .await
            .unwrap(),
    );
    settle(
        &store,
        &failed,
        CheckResultOutcome::InfrastructureFailed,
        CheckCleanup::NotPerformed,
    )
    .await;
    let other = scheduled(
        runner
            .request(cacheable_request("other-consumer"))
            .await
            .unwrap(),
    );
    let owner = Arc::new(TestOwner::default());
    CheckRunWorker::new(store.clone(), owner.clone())
        .drive(admit(&store, &other).await)
        .await
        .unwrap();
    let reused = store
        .retry_infrastructure_check(&failed.id, &db::now_rfc3339())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reused.id, other.id);
    assert!(matches!(
        runner
            .request(cacheable_request("failed-before-crash"))
            .await
            .unwrap()
            .outcome,
        CheckRequestOutcome::Hit(_)
    ));
    assert_eq!(
        store.check_run_counts().await.unwrap().by_state["queued"],
        0
    );
    assert_eq!(owner.runs.load(Ordering::SeqCst), 1);
    assert_eq!(store.enqueue_check_result_steps(100).await.unwrap(), 1);
}

#[tokio::test]
async fn queue_expiry_progresses_even_when_all_worker_effect_jobs_are_occupied() {
    let (_temp, store, runner) = fixture().await;
    let run = scheduled(runner.request(request("expired-slot")).await.unwrap());
    sqlx::query("UPDATE check_run SET created_at='2000-01-01T00:00:00Z' WHERE id=?")
        .bind(&run.id)
        .execute(store.pool())
        .await
        .unwrap();
    let owner = Arc::new(TestOwner::default());
    let worker = Arc::new(CheckRunWorker::new(store.clone(), owner.clone()));
    let mut jobs = tokio::task::JoinSet::new();
    for _ in 0..32 {
        jobs.spawn(std::future::pending::<Result<()>>());
    }
    worker.sweep(&mut jobs).await.unwrap();
    assert_eq!(
        store.check_run(&run.id).await.unwrap().unwrap().state,
        CheckRunState::Failed
    );
    assert_eq!(owner.runs.load(Ordering::SeqCst), 0);
    assert_eq!(
        store.check_run_counts().await.unwrap().by_state["queued"],
        1
    );
    jobs.abort_all();
    while jobs.join_next().await.is_some() {}
}

#[tokio::test]
async fn failed_ack_rotates_behind_later_results_instead_of_starving_their_owner() {
    let (_temp, store, runner) = fixture().await;
    let first = scheduled(runner.request(request("ack-first")).await.unwrap());
    let owner = Arc::new(TestOwner::default());
    let worker = CheckRunWorker::new(store.clone(), owner);
    worker.drive(admit(&store, &first).await).await.unwrap();
    let mut req = request("ack-second");
    req.identity.commit_sha = "b".repeat(40);
    let second = scheduled(runner.request(req).await.unwrap());
    worker.drive(admit(&store, &second).await).await.unwrap();
    let now = db::now_rfc3339();
    assert_eq!(
        store.unacknowledged_checks(&now, 1).await.unwrap()[0]
            .run
            .id,
        first.id
    );
    store
        .defer_check_ack(&first.id, &first.operation_id, &now)
        .await
        .unwrap();
    assert_eq!(
        store.unacknowledged_checks(&now, 1).await.unwrap()[0]
            .run
            .id,
        second.id
    );
    // The refused acknowledgement is not retried every sweep: it comes back
    // only after the retry interval.
    let ids = |rows: Vec<CheckWorkerRecord>| rows.into_iter().map(|r| r.run.id).collect::<Vec<_>>();
    assert!(!ids(store.unacknowledged_checks(&now, 10).await.unwrap()).contains(&first.id));
    let later = (chrono::Utc::now() + chrono::Duration::seconds(db::CHECK_ACK_RETRY_SECONDS + 1))
        .to_rfc3339();
    assert!(ids(store.unacknowledged_checks(&later, 10).await.unwrap()).contains(&first.id));
}

/// The worker loop runs in every server. With nothing to do, a sweep takes
/// no write lock and changes no row; one unreadable run does not stop it.
#[tokio::test]
async fn an_idle_sweep_writes_nothing_and_a_poison_run_does_not_stop_the_sweep() {
    use sqlx::Connection;
    let (temp, store, runner) = fixture().await;
    let owner = Arc::new(TestOwner::default());
    let worker = Arc::new(CheckRunWorker::new(store.clone(), owner.clone()));
    let mut observer = sqlx::SqliteConnection::connect(&format!(
        "sqlite://{}",
        temp.path().join("checks.sqlite").display()
    ))
    .await
    .unwrap();
    // `data_version` moves whenever another connection commits a change.
    let before: i64 = sqlx::query_scalar("PRAGMA data_version")
        .fetch_one(&mut observer)
        .await
        .unwrap();
    let mut jobs = tokio::task::JoinSet::new();
    for _ in 0..5 {
        worker.sweep(&mut jobs).await.unwrap();
    }
    assert!(jobs.is_empty());
    let after: i64 = sqlx::query_scalar("PRAGMA data_version")
        .fetch_one(&mut observer)
        .await
        .unwrap();
    assert_eq!(after, before);
    // The sweep reads through indexes that stay empty while idle.
    for sql in [
        "SELECT 1 FROM check_consumer c WHERE c.result_id IS NOT NULL AND c.delivery_step_id IS NULL AND c.cancelled_at IS NULL",
        "SELECT 1 FROM check_run WHERE dispatch_json IS NOT NULL AND acknowledged_at IS NULL AND finished_at IS NOT NULL ORDER BY updated_at",
        "SELECT 1 FROM check_run WHERE state IN ('queued','running','cancelling','cleaning','uncertain') AND lease_until IS NULL",
    ] {
        let rows = sqlx::query(&format!("EXPLAIN QUERY PLAN {sql}"))
            .fetch_all(store.pool())
            .await
            .unwrap();
        for row in rows {
            let detail: String = sqlx::Row::get(&row, "detail");
            assert!(detail.contains("INDEX"), "{sql}: {detail}");
        }
    }

    // A run whose stored intent cannot be read fails alone.
    let poison = scheduled(runner.request(request("poison")).await.unwrap());
    let poison = admit(&store, &poison).await;
    sqlx::query("UPDATE check_run SET dispatch_json='{}',lease_until=? WHERE id=?")
        .bind((chrono::Utc::now() - chrono::Duration::seconds(1)).to_rfc3339())
        .bind(&poison.id)
        .execute(store.pool())
        .await
        .unwrap();
    let mut healthy = request("healthy");
    healthy.identity.commit_sha = "c".repeat(40);
    let healthy = scheduled(runner.request(healthy).await.unwrap());
    for _ in 0..3 {
        worker.sweep(&mut jobs).await.unwrap();
        while let Some(job) = jobs.join_next().await {
            let _ = job.unwrap();
        }
    }
    assert_eq!(
        store.check_run(&healthy.id).await.unwrap().unwrap().state,
        CheckRunState::Succeeded
    );
}

struct TestFamily {
    authority: std::sync::Mutex<Option<String>>,
    applied: std::sync::Mutex<Vec<consumer::CheckApplication>>,
}
#[async_trait::async_trait]
impl consumer::CheckConsumerFamily for TestFamily {
    async fn current_authority(&self, _task: &str, _epoch: i64) -> Result<Option<String>> {
        Ok(self.authority.lock().unwrap().clone())
    }
    async fn apply(&self, application: &consumer::CheckApplication) -> Result<()> {
        self.applied.lock().unwrap().push(application.clone());
        Ok(())
    }
}
fn task_request(authority: &str, commit: char) -> consumer::TaskCheckRequest {
    let base = request("unused");
    let mut identity = base.identity;
    identity.commit_sha = commit.to_string().repeat(40);
    consumer::TaskCheckRequest {
        task_id: "t".into(),
        status_epoch: 0,
        authority: authority.into(),
        origin: CheckConsumerOrigin::Integration,
        purpose: base.purpose,
        identity,
        workspace_id: None,
        machine_id: None,
        wall_timeout_seconds: 1800,
    }
}
async fn consumers(
    store: &Arc<SqliteDb>,
    runner: &Arc<CheckRunner>,
    authority: &str,
) -> (consumer::TaskCheckConsumers, Arc<TestFamily>) {
    let family = Arc::new(TestFamily {
        authority: std::sync::Mutex::new(Some(authority.into())),
        applied: Default::default(),
    });
    let consumers = consumer::TaskCheckConsumers::new(store.clone(), runner.clone());
    consumers.register(CheckConsumerOrigin::Integration, family.clone());
    (consumers, family)
}
/// Run `future` under a claimed step of Task `t`, as the hook or command
/// that asks for a check runs. The step is this harness's own and is removed
/// afterwards, so the Task's queue holds only what the code under test put there.
async fn in_step<T>(store: &SqliteDb, future: impl std::future::Future<Output = T>) -> T {
    let id = db::new_uuid_v4();
    store
        .enqueue_step(&db::EnqueueTaskStep {
            id: id.clone(),
            task_id: "t".into(),
            kind: "command".into(),
            payload_json: "{}".into(),
            causation_step_id: None,
            causation_key: id.clone(),
            chain_id: id.clone(),
            chain_position: 1,
            expected_status: "review".into(),
            expected_version: 1,
            expected_epoch: None,
            lane: "fast".into(),
            available_at: db::now_rfc3339(),
        })
        .await
        .unwrap();
    let out = in_claimed(store, &id, future).await;
    sqlx::query("DELETE FROM task_step WHERE id=?")
        .bind(&id)
        .execute(store.pool())
        .await
        .unwrap();
    out
}
/// Claim exactly `step_id` (whatever else is queued) and run `future` as it.
async fn in_claimed<T>(
    store: &SqliteDb,
    step_id: &str,
    future: impl std::future::Future<Output = T>,
) -> T {
    sqlx::query(
        "UPDATE task_step SET status='claimed',claimed_by='check-tests',lease_until=? WHERE id=?",
    )
    .bind(db::task_writer::lease_deadline())
    .bind(step_id)
    .execute(store.pool())
    .await
    .unwrap();
    let step = store
        .task_steps("t")
        .await
        .unwrap()
        .into_iter()
        .find(|step| step.id == step_id)
        .unwrap();
    db::task_writer::in_task_step(step, future).await
}
/// A request made the way production makes it: from inside the Task's step.
async fn ask(
    store: &SqliteDb,
    consumers: &consumer::TaskCheckConsumers,
    request: consumer::TaskCheckRequest,
) -> Result<RequestedCheck> {
    in_step(store, consumers.request(request)).await
}
/// The delivery step of the Task's only undelivered-then-delivered consumer.
async fn delivery(store: &SqliteDb, consumer_id: &str) -> (String, db::CheckResultDelivery) {
    for step in store.task_steps("t").await.unwrap() {
        let payload: serde_json::Value = serde_json::from_str(&step.payload_json).unwrap();
        if payload["operation"] == "apply_check_result"
            && payload["arguments"]["consumer_id"] == consumer_id
        {
            return (
                step.id,
                serde_json::from_value(payload["arguments"].clone()).unwrap(),
            );
        }
    }
    panic!("no delivery step for {consumer_id}");
}

#[tokio::test]
async fn a_task_step_consumer_is_woken_once_and_applies_through_the_real_runner() {
    let (_temp, store, runner) = fixture().await;
    let (consumers, family) = consumers(&store, &runner, "attempt-1").await;
    let owner = Arc::new(TestOwner::default());
    let worker = Arc::new(CheckRunWorker::new(store.clone(), owner.clone()));

    let asked = ask(&store, &consumers, task_request("attempt-1", 'a'))
        .await
        .unwrap();
    // Asking again is the same consumer: no second run, no second delivery.
    let again = ask(&store, &consumers, task_request("attempt-1", 'a'))
        .await
        .unwrap();
    assert_eq!(asked.consumer.id, again.consumer.id);
    let run = scheduled(asked);
    worker.drive(admit(&store, &run).await).await.unwrap();
    assert_eq!(owner.runs.load(Ordering::SeqCst), 1);
    let mut jobs = tokio::task::JoinSet::new();
    worker.sweep(&mut jobs).await.unwrap();
    let steps = store.task_steps("t").await.unwrap();
    assert_eq!(steps.len(), 1, "one wake for one consumer");

    let (step_id, envelope) = delivery(&store, &again.consumer.id).await;
    // Another step cannot apply this consumer's result.
    assert!(consumers.apply("another-step", &envelope).await.is_err());
    // Nor can an envelope that names another commit or result.
    let mut forged = envelope.clone();
    forged.commit_sha = "f".repeat(40);
    assert!(consumers.apply(&step_id, &forged).await.is_err());
    assert!(family.applied.lock().unwrap().is_empty());

    assert_eq!(
        consumers.apply(&step_id, &envelope).await.unwrap(),
        consumer::CheckApplyOutcome::Applied
    );
    // A redelivered step does not apply twice.
    assert_eq!(
        consumers.apply(&step_id, &envelope).await.unwrap(),
        consumer::CheckApplyOutcome::AlreadyApplied
    );
    let applied = family.applied.lock().unwrap();
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].authority, "attempt-1");
    assert_eq!(applied[0].run.identity.commit_sha, "a".repeat(40));
    assert!(matches!(
        &applied[0].verdict,
        consumer::CheckVerdict::Result(result) if result.outcome == CheckResultOutcome::Pass
    ));
}

#[tokio::test]
async fn a_result_is_refused_when_the_asking_attempt_or_status_entry_has_passed() {
    let (_temp, store, runner) = fixture().await;
    let (consumers, family) = consumers(&store, &runner, "attempt-1").await;
    let worker = Arc::new(CheckRunWorker::new(
        store.clone(),
        Arc::new(TestOwner::default()),
    ));
    let mut jobs = tokio::task::JoinSet::new();

    // A later attempt took over while the check ran.
    let first = ask(&store, &consumers, task_request("attempt-1", 'a'))
        .await
        .unwrap();
    let consumer_id = first.consumer.id.clone();
    worker
        .drive(admit(&store, &scheduled(first)).await)
        .await
        .unwrap();
    worker.sweep(&mut jobs).await.unwrap();
    *family.authority.lock().unwrap() = Some("attempt-2".into());
    let (step_id, envelope) = delivery(&store, &consumer_id).await;
    assert_eq!(
        consumers.apply(&step_id, &envelope).await.unwrap(),
        consumer::CheckApplyOutcome::Stale(consumer::CheckStaleReason::Authority)
    );

    // The Task left the status entry between delivery and application.
    let second = ask(&store, &consumers, task_request("attempt-2", 'b'))
        .await
        .unwrap();
    let consumer_id = second.consumer.id.clone();
    worker
        .drive(admit(&store, &scheduled(second)).await)
        .await
        .unwrap();
    worker.sweep(&mut jobs).await.unwrap();
    let (step_id, envelope) = delivery(&store, &consumer_id).await;
    sqlx::query("UPDATE task SET status_epoch=status_epoch+1 WHERE id='t'")
        .execute(store.pool())
        .await
        .unwrap();
    assert_eq!(
        consumers.apply(&step_id, &envelope).await.unwrap(),
        consumer::CheckApplyOutcome::Stale(consumer::CheckStaleReason::TaskEpoch)
    );
    assert!(family.applied.lock().unwrap().is_empty());
    // A step of the old status entry can no longer ask.
    assert!(ask(&store, &consumers, task_request("attempt-3", 'c'))
        .await
        .is_err());
    // A family nobody registered cannot ask either: its result could never be applied.
    let mut unregistered = task_request("attempt-4", 'd');
    unregistered.status_epoch = 1;
    unregistered.origin = CheckConsumerOrigin::Conformance;
    assert!(ask(&store, &consumers, unregistered).await.is_err());
}

#[tokio::test]
async fn exhausted_infrastructure_retries_reach_the_consumer_as_no_verdict() {
    let (_temp, store, runner) = fixture().await;
    let (consumers, family) = consumers(&store, &runner, "attempt-1").await;
    let asked = ask(&store, &consumers, task_request("attempt-1", 'a'))
        .await
        .unwrap();
    let consumer_id = asked.consumer.id.clone();
    let mut run = scheduled(asked);
    // Two automatic retries: nothing is delivered, the consumer moves on.
    for _ in 0..2 {
        settle(
            &store,
            &run,
            CheckResultOutcome::InfrastructureFailed,
            CheckCleanup::NotPerformed,
        )
        .await;
        assert_eq!(store.enqueue_check_result_steps(10).await.unwrap(), 0);
        run = store
            .retry_infrastructure_check(&run.id, &db::now_rfc3339())
            .await
            .unwrap()
            .expect("an automatic retry");
    }
    // The third infrastructure failure is the consumer's answer.
    settle(
        &store,
        &run,
        CheckResultOutcome::InfrastructureFailed,
        CheckCleanup::NotPerformed,
    )
    .await;
    assert!(store
        .retry_infrastructure_check(&run.id, &db::now_rfc3339())
        .await
        .unwrap()
        .is_none());
    assert_eq!(store.enqueue_check_result_steps(10).await.unwrap(), 1);
    let (step_id, envelope) = delivery(&store, &consumer_id).await;
    assert_eq!(
        consumers.apply(&step_id, &envelope).await.unwrap(),
        consumer::CheckApplyOutcome::Applied
    );
    assert!(matches!(
        family.applied.lock().unwrap()[0].verdict,
        consumer::CheckVerdict::InfrastructureExhausted(_)
    ));
}

/// An owner that still reports the operation as running is not asked again
/// every sweep: the run keeps its identity and slot behind a short lease.
#[tokio::test]
async fn an_uncertain_run_is_looked_up_again_only_after_its_backoff() {
    let (_temp, store, runner) = fixture().await;
    let run = scheduled(runner.request(request("backoff")).await.unwrap());
    let owner = Arc::new(TestOwner {
        interrupted: true,
        running: true,
        ..Default::default()
    });
    let worker = Arc::new(CheckRunWorker::new(store.clone(), owner.clone()));
    worker.drive(admit(&store, &run).await).await.unwrap();
    let stored = store.check_run(&run.id).await.unwrap().unwrap();
    assert_eq!(stored.state, CheckRunState::Uncertain);
    assert!(stored.lease_owner.is_some());
    let lookups = owner.lookups.load(Ordering::SeqCst);
    let mut jobs = tokio::task::JoinSet::new();
    for _ in 0..3 {
        worker.sweep(&mut jobs).await.unwrap();
    }
    assert!(jobs.is_empty());
    assert_eq!(owner.lookups.load(Ordering::SeqCst), lookups);
    assert_eq!(
        owner.runs.load(Ordering::SeqCst),
        1,
        "never dispatched twice"
    );
    let due = (chrono::Utc::now()
        + chrono::Duration::seconds(db::CHECK_RECONCILE_BACKOFF_SECONDS + 1))
    .to_rfc3339();
    assert_eq!(
        store.runnable_check_runs(&due, 10).await.unwrap()[0].id,
        run.id
    );
    // A second request for the identity joins; it never launches a duplicate.
    assert!(matches!(
        runner
            .request(request("backoff-join"))
            .await
            .unwrap()
            .outcome,
        CheckRequestOutcome::Joined(_)
    ));

    // The owner ignores cancel long past the run's own bound: the run does
    // not stay uncertain forever. Both consumers are answered, and nothing
    // is launched again on its own.
    let past =
        |seconds: i64| (chrono::Utc::now() - chrono::Duration::seconds(seconds)).to_rfc3339();
    sqlx::query("UPDATE check_run SET deadline_at=?,lease_until=? WHERE id=?")
        .bind(past(7200))
        .bind(past(1))
        .bind(&run.id)
        .execute(store.pool())
        .await
        .unwrap();
    let stored = store.check_run(&run.id).await.unwrap().unwrap();
    worker.drive(admit(&store, &stored).await).await.unwrap();
    assert_eq!(
        store.check_run(&run.id).await.unwrap().unwrap().state,
        CheckRunState::Failed
    );
    assert_eq!(owner.runs.load(Ordering::SeqCst), 1);
    assert_eq!(store.task_steps("t").await.unwrap().len(), 2);
    let live: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM check_run WHERE state NOT IN ('succeeded','failed','cancelled')",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(live, 0, "no automatic retry of an unconfirmed operation");
}

/// An acknowledgement nobody can receive ends: the machine is gone, or the
/// owner's own retention of the entry has passed.
#[tokio::test]
async fn an_acknowledgement_for_a_gone_owner_or_an_expired_entry_is_settled() {
    let (_temp, store, runner) = fixture().await;
    let run = scheduled(runner.request(request("ack-gone")).await.unwrap());
    let reachable = Arc::new(TestOwner::default());
    CheckRunWorker::new(store.clone(), reachable)
        .drive(admit(&store, &run).await)
        .await
        .unwrap();
    let refused = Arc::new(TestOwner {
        refuse_ack: true,
        ..Default::default()
    });
    let worker = Arc::new(CheckRunWorker::new(store.clone(), refused.clone()));
    let mut jobs = tokio::task::JoinSet::new();
    worker.sweep(&mut jobs).await.unwrap();
    worker.sweep(&mut jobs).await.unwrap();
    assert_eq!(refused.acknowledgments.load(Ordering::SeqCst), 1);
    let record = store.check_worker_record(&run.id).await.unwrap();
    assert!(record.acknowledged_at.is_none());
    // Past the owner's retention the retry gives up.
    let late = (chrono::Utc::now() + chrono::Duration::seconds(db::CHECK_ACK_GIVE_UP_SECONDS + 1))
        .to_rfc3339();
    store
        .defer_check_ack(&run.id, &run.operation_id, &late)
        .await
        .unwrap();
    assert!(store
        .check_worker_record(&run.id)
        .await
        .unwrap()
        .acknowledged_at
        .is_some());

    // A removed machine is settled on the first refused attempt.
    let mut req = request("ack-removed");
    req.identity.commit_sha = "d".repeat(40);
    let run = scheduled(runner.request(req).await.unwrap());
    let gone = Arc::new(TestOwner {
        refuse_ack: true,
        gone: true,
        ..Default::default()
    });
    let worker = Arc::new(CheckRunWorker::new(store.clone(), gone));
    worker.drive(admit(&store, &run).await).await.unwrap();
    worker.sweep(&mut jobs).await.unwrap();
    assert!(store
        .check_worker_record(&run.id)
        .await
        .unwrap()
        .acknowledged_at
        .is_some());
}

async fn condition(store: &SqliteDb) -> db::TaskCondition {
    store.task_condition("t").await.unwrap()
}
fn phase(condition: &db::TaskCondition) -> Option<CheckWaitPhase> {
    condition.check_wait().map(|wait| wait.phase)
}
/// The Task's progress notes, oldest first.
async fn progress_notes(store: &SqliteDb) -> Vec<(String, db::CheckProgressDelivery)> {
    let mut notes = Vec::new();
    for step in store.task_steps("t").await.unwrap() {
        let payload: serde_json::Value = serde_json::from_str(&step.payload_json).unwrap();
        if payload["operation"] == "apply_check_progress" {
            notes.push((
                step.id,
                serde_json::from_value(payload["arguments"].clone()).unwrap(),
            ));
        }
    }
    notes
}

#[tokio::test]
async fn a_check_wait_is_stated_at_request_follows_the_slot_and_ends_with_the_result() {
    let (_temp, store, runner) = fixture().await;
    let (consumers, family) = consumers(&store, &runner, "attempt-1").await;
    // Asked from outside the Task's step: refused, because the wait could
    // not be stated and the Task would wait invisibly.
    assert!(consumers
        .request(task_request("attempt-1", 'a'))
        .await
        .is_err());
    assert!(store.task_steps("t").await.unwrap().is_empty());

    // Another check holds the machine's only slot.
    store.server_run_cap.set(Some(1), 1, "server-machine");
    let mut holder = request("holder");
    holder.identity.commit_sha = "b".repeat(40);
    let holder = admit(&store, &scheduled(runner.request(holder).await.unwrap())).await;

    let asked = ask(&store, &consumers, task_request("attempt-1", 'a'))
        .await
        .unwrap();
    let consumer_id = asked.consumer.id.clone();
    let run = scheduled(asked);
    let waiting = condition(&store).await;
    assert_eq!(phase(&waiting), Some(CheckWaitPhase::Result));
    assert!(!waiting.is_blocked(), "a check wait is not a blocker");

    // The worker finds no slot, sweep after sweep: the Task is told once.
    let worker = Arc::new(CheckRunWorker::new(
        store.clone(),
        Arc::new(TestOwner::default()),
    ));
    let mut jobs = tokio::task::JoinSet::new();
    for _ in 0..3 {
        worker.sweep(&mut jobs).await.unwrap();
    }
    assert!(jobs.is_empty(), "nothing was admitted");
    let notes = progress_notes(&store).await;
    assert_eq!(notes.len(), 1, "one note per wait, not per sweep");
    assert_eq!(notes[0].1.progress, db::CheckProgress::SlotWait);
    // The worker wrote no condition; the Task's own step restates it.
    assert_eq!(
        phase(&condition(&store).await),
        Some(CheckWaitPhase::Result)
    );
    assert_eq!(
        in_claimed(&store, &notes[0].0, consumers.progress(&notes[0].1))
            .await
            .unwrap(),
        consumer::CheckApplyOutcome::Applied
    );
    let slot = condition(&store).await;
    assert_eq!(phase(&slot), Some(CheckWaitPhase::Slot));
    assert!(!slot.is_blocked());
    // Asking again while it waits names the slot wait at once.
    ask(&store, &consumers, task_request("attempt-1", 'a'))
        .await
        .unwrap();
    assert_eq!(phase(&condition(&store).await), Some(CheckWaitPhase::Slot));

    // The slot frees: the run is admitted and the Task is told, once.
    worker.drive(holder).await.unwrap();
    let run = store.check_run(&run.id).await.unwrap().unwrap();
    let admitted = admit(&store, &run).await;
    let notes = progress_notes(&store).await;
    assert_eq!(notes.len(), 2);
    assert_eq!(notes[1].1.progress, db::CheckProgress::Admitted);
    assert_eq!(
        in_claimed(&store, &notes[1].0, consumers.progress(&notes[1].1))
            .await
            .unwrap(),
        consumer::CheckApplyOutcome::Applied
    );
    assert_eq!(
        phase(&condition(&store).await),
        Some(CheckWaitPhase::Result)
    );
    // A late copy of the slot-wait note cannot put the wait back.
    assert_eq!(
        in_step(&store, consumers.progress(&notes[0].1))
            .await
            .unwrap(),
        consumer::CheckApplyOutcome::Applied
    );
    assert_eq!(
        phase(&condition(&store).await),
        Some(CheckWaitPhase::Result)
    );

    // The result arrives: applying it ends the wait.
    worker.drive(admitted).await.unwrap();
    worker.sweep(&mut jobs).await.unwrap();
    let (step_id, envelope) = delivery(&store, &consumer_id).await;
    assert_eq!(
        in_claimed(&store, &step_id, consumers.apply(&step_id, &envelope))
            .await
            .unwrap(),
        consumer::CheckApplyOutcome::Applied
    );
    assert_eq!(family.applied.lock().unwrap().len(), 1);
    let done = condition(&store).await;
    assert!(done.check_witness().is_none(), "{done:?}");
    // A progress note that the answer overtook changes nothing.
    assert_eq!(
        in_step(&store, consumers.progress(&notes[1].1))
            .await
            .unwrap(),
        consumer::CheckApplyOutcome::Stale(consumer::CheckStaleReason::Overtaken)
    );
    assert!(condition(&store).await.check_witness().is_none());
    // Repeating the answered request restates no wait.
    ask(&store, &consumers, task_request("attempt-1", 'a'))
        .await
        .unwrap();
    assert!(condition(&store).await.check_witness().is_none());
    let task = db::TaskRepo::get_by_id(&*store, "t", false)
        .await
        .unwrap()
        .unwrap();
    store.check_task_condition_invariant(&task).await.unwrap();
}

#[tokio::test]
async fn exhausted_retries_park_the_task_and_the_owners_retry_asks_again_with_a_fresh_budget() {
    let (_temp, store, runner) = fixture().await;
    let (consumers, family) = consumers(&store, &runner, "attempt-1").await;
    let asked = ask(&store, &consumers, task_request("attempt-1", 'a'))
        .await
        .unwrap();
    let consumer_id = asked.consumer.id.clone();
    let mut run = scheduled(asked);
    let task = || async {
        db::TaskRepo::get_by_id(&*store, "t", false)
            .await
            .unwrap()
            .unwrap()
    };
    // Nothing to retry while the check is merely running.
    assert!(!in_step(&store, async {
        consumers.retry_exhausted(&task().await).await
    })
    .await
    .unwrap());
    // The automatic budget: two retries, then the failure is the answer.
    for _ in 0..2 {
        settle(
            &store,
            &run,
            CheckResultOutcome::InfrastructureFailed,
            CheckCleanup::NotPerformed,
        )
        .await;
        assert_eq!(store.enqueue_check_result_steps(10).await.unwrap(), 0);
        run = store
            .retry_infrastructure_check(&run.id, &db::now_rfc3339())
            .await
            .unwrap()
            .expect("an automatic retry");
    }
    settle(
        &store,
        &run,
        CheckResultOutcome::InfrastructureFailed,
        CheckCleanup::NotPerformed,
    )
    .await;
    assert_eq!(store.enqueue_check_result_steps(10).await.unwrap(), 1);
    let (step_id, envelope) = delivery(&store, &consumer_id).await;
    assert_eq!(
        in_claimed(&store, &step_id, consumers.apply(&step_id, &envelope))
            .await
            .unwrap(),
        consumer::CheckApplyOutcome::Applied
    );
    // Parked on the typed reason; not a verdict on the candidate.
    let parked = condition(&store).await;
    assert_eq!(
        phase(&parked),
        Some(CheckWaitPhase::InfrastructureExhausted)
    );
    assert!(parked.is_blocked());
    assert!(matches!(
        family.applied.lock().unwrap()[0].verdict,
        consumer::CheckVerdict::InfrastructureExhausted(_)
    ));
    // The owner's retry: the same consumer, a fresh budget, waiting again.
    assert!(in_step(&store, async {
        consumers.retry_exhausted(&task().await).await
    })
    .await
    .unwrap());
    let waiting = condition(&store).await;
    assert_eq!(phase(&waiting), Some(CheckWaitPhase::Result));
    assert!(!waiting.is_blocked());
    assert_eq!(
        store.retryable_check_runs(10).await.unwrap(),
        vec![run.id.clone()]
    );
    // One more infrastructure failure is retried automatically again.
    run = store
        .retry_infrastructure_check(&run.id, &db::now_rfc3339())
        .await
        .unwrap()
        .expect("the owner's retry schedules a run");
    settle(
        &store,
        &run,
        CheckResultOutcome::InfrastructureFailed,
        CheckCleanup::NotPerformed,
    )
    .await;
    assert_eq!(store.enqueue_check_result_steps(10).await.unwrap(), 0);
    run = store
        .retry_infrastructure_check(&run.id, &db::now_rfc3339())
        .await
        .unwrap()
        .expect("a fresh automatic retry");
    // The retried check passes; its result is delivered by a second step.
    let worker = Arc::new(CheckRunWorker::new(
        store.clone(),
        Arc::new(TestOwner::default()),
    ));
    worker.drive(admit(&store, &run).await).await.unwrap();
    let mut jobs = tokio::task::JoinSet::new();
    worker.sweep(&mut jobs).await.unwrap();
    let deliveries: Vec<_> = store
        .task_steps("t")
        .await
        .unwrap()
        .into_iter()
        .filter(|step| step.payload_json.contains("apply_check_result"))
        .collect();
    assert_eq!(deliveries.len(), 2, "one delivery per answer");
    let step = deliveries.last().unwrap();
    let envelope: db::CheckResultDelivery = serde_json::from_value(
        serde_json::from_str::<serde_json::Value>(&step.payload_json).unwrap()["arguments"].clone(),
    )
    .unwrap();
    assert_eq!(
        in_claimed(&store, &step.id, consumers.apply(&step.id, &envelope))
            .await
            .unwrap(),
        consumer::CheckApplyOutcome::Applied
    );
    let applied = family.applied.lock().unwrap();
    assert_eq!(applied.len(), 2);
    assert!(matches!(
        &applied[1].verdict,
        consumer::CheckVerdict::Result(result) if result.outcome == CheckResultOutcome::Pass
    ));
    assert_eq!(applied[1].consumer_id, consumer_id);
    drop(applied);
    assert!(condition(&store).await.check_witness().is_none());
    assert!(!in_step(&store, async {
        consumers.retry_exhausted(&task().await).await
    })
    .await
    .unwrap());
}

#[tokio::test]
async fn a_stale_delivery_ends_only_its_own_wait_and_applies_nothing() {
    let (_temp, store, runner) = fixture().await;
    let (consumers, family) = consumers(&store, &runner, "attempt-1").await;
    let worker = Arc::new(CheckRunWorker::new(
        store.clone(),
        Arc::new(TestOwner::default()),
    ));
    let mut jobs = tokio::task::JoinSet::new();
    let first = ask(&store, &consumers, task_request("attempt-1", 'a'))
        .await
        .unwrap();
    let first_id = first.consumer.id.clone();
    worker
        .drive(admit(&store, &scheduled(first)).await)
        .await
        .unwrap();
    worker.sweep(&mut jobs).await.unwrap();
    // A later attempt took over and asked for its own check.
    *family.authority.lock().unwrap() = Some("attempt-2".into());
    let second = ask(&store, &consumers, task_request("attempt-2", 'b'))
        .await
        .unwrap();
    assert_eq!(
        condition(&store).await.check_wait().unwrap().consumer_id,
        second.consumer.id
    );
    let (step_id, envelope) = delivery(&store, &first_id).await;
    assert_eq!(
        in_claimed(&store, &step_id, consumers.apply(&step_id, &envelope))
            .await
            .unwrap(),
        consumer::CheckApplyOutcome::Stale(consumer::CheckStaleReason::Authority)
    );
    assert!(family.applied.lock().unwrap().is_empty());
    // The successor's wait stands.
    let current = condition(&store).await;
    assert_eq!(
        current.check_wait().unwrap().consumer_id,
        second.consumer.id
    );
    assert_eq!(phase(&current), Some(CheckWaitPhase::Result));
}

#[tokio::test]
async fn an_owners_retry_is_idempotent_and_never_reruns_a_stale_identity() {
    let (_temp, store, runner) = fixture().await;
    let (consumers, family) = consumers(&store, &runner, "attempt-1").await;
    let asked = ask(&store, &consumers, task_request("attempt-1", 'a'))
        .await
        .unwrap();
    let mut run = scheduled(asked);
    let task = || async {
        db::TaskRepo::get_by_id(&*store, "t", false)
            .await
            .unwrap()
            .unwrap()
    };
    let fail = |run: StoredCheckRun| {
        let store = store.clone();
        async move {
            settle(
                &store,
                &run,
                CheckResultOutcome::InfrastructureFailed,
                CheckCleanup::NotPerformed,
            )
            .await
        }
    };
    // The newest delivery step is the one that is still to be applied.
    let apply_latest = || async {
        let step = store
            .task_steps("t")
            .await
            .unwrap()
            .into_iter()
            .rfind(|step| step.payload_json.contains("apply_check_result"))
            .unwrap();
        let envelope: db::CheckResultDelivery = serde_json::from_value(
            serde_json::from_str::<serde_json::Value>(&step.payload_json).unwrap()["arguments"]
                .clone(),
        )
        .unwrap();
        in_claimed(&store, &step.id, consumers.apply(&step.id, &envelope))
            .await
            .unwrap()
    };
    for _ in 0..2 {
        fail(run.clone()).await;
        run = store
            .retry_infrastructure_check(&run.id, &db::now_rfc3339())
            .await
            .unwrap()
            .unwrap();
    }
    fail(run.clone()).await;
    assert_eq!(store.enqueue_check_result_steps(10).await.unwrap(), 1);
    assert_eq!(apply_latest().await, consumer::CheckApplyOutcome::Applied);
    let exhausted = task().await;
    assert_eq!(
        phase(&exhausted.condition),
        Some(CheckWaitPhase::InfrastructureExhausted)
    );

    // First click re-arms; a second click finds nothing exhausted.
    assert!(in_step(&store, async {
        consumers.retry_exhausted(&exhausted).await
    })
    .await
    .unwrap());
    assert!(!in_step(&store, async {
        consumers.retry_exhausted(&task().await).await
    })
    .await
    .unwrap());
    // A redelivered retry step still holds the exhausted snapshot: it only
    // restates the wait. Neither repeat schedules a second run or delivery.
    assert!(in_step(&store, async {
        consumers.retry_exhausted(&exhausted).await
    })
    .await
    .unwrap());
    assert_eq!(
        phase(&condition(&store).await),
        Some(CheckWaitPhase::Result)
    );
    assert_eq!(
        store.retryable_check_runs(10).await.unwrap(),
        vec![run.id.clone()]
    );
    assert_eq!(store.enqueue_check_result_steps(10).await.unwrap(), 0);

    // The fresh budget is used up as well: the Task parks again, through a
    // second delivery of its own.
    for _ in 0..2 {
        run = store
            .retry_infrastructure_check(&run.id, &db::now_rfc3339())
            .await
            .unwrap()
            .unwrap();
        fail(run.clone()).await;
    }
    assert_eq!(store.enqueue_check_result_steps(10).await.unwrap(), 1);
    assert_eq!(apply_latest().await, consumer::CheckApplyOutcome::Applied);
    assert_eq!(
        phase(&condition(&store).await),
        Some(CheckWaitPhase::InfrastructureExhausted)
    );
    assert_eq!(family.applied.lock().unwrap().len(), 2);

    // The candidate moved on inside the same status entry: retry lifts the
    // stale park and does not run the old commit again.
    *family.authority.lock().unwrap() = Some("attempt-2".into());
    assert!(in_step(&store, async {
        consumers.retry_exhausted(&task().await).await
    })
    .await
    .unwrap());
    let lifted = condition(&store).await;
    assert_eq!(phase(&lifted), None);
    assert!(lifted.check_witness().is_none() && !lifted.is_blocked());
    assert!(store.retryable_check_runs(10).await.unwrap().is_empty());
    assert_eq!(store.enqueue_check_result_steps(10).await.unwrap(), 0);
}
