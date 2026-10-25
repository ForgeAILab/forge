//! Task step check consumers and durable result delivery.

use super::*;

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
