//! Check worker dispatch, cancellation, recovery, receipts, and sweep fairness.

use super::*;

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

/// The worker looks after the lease and the consumers beside the owner's
/// operation, never instead of it. A store write awaited while the operation
/// was not polled waited for the write lock the operation held, and the
/// operation could not go on to release it: every writer of the database
/// stood still until SQLite gave up after its busy timeout (30 s), and the
/// run failed with "database is locked".
#[tokio::test]
async fn the_worker_keeps_polling_an_operation_that_is_inside_a_write_transaction() {
    let (_temp, store, runner) = fixture().await;
    let run = scheduled(runner.request(cacheable_request("held")).await.unwrap());
    let owner = Arc::new(WritesWhileRunning {
        store: store.clone(),
    });
    let worker = Arc::new(CheckRunWorker::new(store.clone(), owner));
    let started = std::time::Instant::now();
    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        worker.drive(admit(&store, &run).await),
    )
    .await
    .expect("the operation finishes while the worker looks at its consumers")
    .expect("the run settles");
    assert!(started.elapsed() < std::time::Duration::from_secs(10));
    assert_eq!(
        store.check_worker_record(&run.id).await.unwrap().run.state,
        CheckRunState::Succeeded
    );
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
    let _worker = CheckRunWorker::new(store.clone(), owner.clone());
    // Nobody waits for it (the Task left the entry that asked): the
    // admission look cancels the queued run itself, before any slot.
    let now = db::now_rfc3339();
    let until = (chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339();
    assert!(matches!(
        store
            .admit_check_run(&run, "test-worker", &now, &until)
            .await,
        Err(db::DbError::VersionConflict)
    ));
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

/// A cancel between the worker's prepare and its dispatch record cannot
/// fence the operation (there is no record to read yet). The worker looks
/// for a waiting consumer again after it wrote the record and before it
/// sends anything: the operation never starts, so there is nothing to fence.
#[tokio::test]
async fn a_cancel_between_prepare_and_the_dispatch_record_starts_no_operation() {
    let (_temp, store, runner) = fixture().await;
    let run = scheduled(runner.request(request("cancel-in-prepare")).await.unwrap());
    let owner = Arc::new(CancelledWhilePreparing {
        store: store.clone(),
        inner: TestOwner::default(),
    });
    CheckRunWorker::new(store.clone(), owner.clone())
        .drive(admit(&store, &run).await)
        .await
        .unwrap();
    assert_eq!(owner.inner.runs.load(Ordering::SeqCst), 0, "dispatched");
    let record = store.check_worker_record(&run.id).await.unwrap();
    assert_eq!(record.run.state, CheckRunState::Cancelled);
    assert!(record.receipt.is_none());
    // The slot is free again.
    let mut tx = db::begin_immediate(store.pool()).await.unwrap();
    let count =
        db::machine_capacity::count_machine_capacity(&mut tx, None, Some(1), "server-machine")
            .await
            .unwrap();
    assert_eq!(count.active_runs(), 0);
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
        "SELECT 1 FROM check_consumer c WHERE c.result_id IS NOT NULL AND c.delivery_step_id IS NULL AND c.cancelled_at IS NULL AND c.applied_at IS NULL",
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
