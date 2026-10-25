//! Check wait progress, stale deliveries, and owner retry budgets.

use super::*;

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
    {
        let applied = family.applied.lock().unwrap();
        assert_eq!(applied.len(), 2);
        assert!(matches!(
            &applied[1].verdict,
            consumer::CheckVerdict::Result(result) if result.outcome == CheckResultOutcome::Pass
        ));
        assert_eq!(applied[1].consumer_id, consumer_id);
    }
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
