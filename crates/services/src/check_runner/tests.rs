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
        Ok(DaemonCheckResult::Unknown {
            operation_id: record.run.operation_id.clone(),
        })
    }
    async fn owner_gone(&self, _record: &CheckWorkerRecord) -> Result<bool> {
        Ok(self.gone)
    }
    async fn acknowledge(&self, _record: &CheckWorkerRecord) -> Result<()> {
        self.acknowledgments.fetch_add(1, Ordering::SeqCst);
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
    assert_eq!(
        store.unacknowledged_checks(1).await.unwrap()[0].run.id,
        first.id
    );
    store
        .defer_check_ack(&first.id, &first.operation_id, &db::now_rfc3339())
        .await
        .unwrap();
    assert_eq!(
        store.unacknowledged_checks(1).await.unwrap()[0].run.id,
        second.id
    );
}
