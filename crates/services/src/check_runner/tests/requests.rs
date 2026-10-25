//! Check request sharing, cache eligibility, replay, and stale epochs.

use super::*;

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
