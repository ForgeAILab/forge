//! Integration parking, cancellation and protected-result races.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn park_states_the_wait_once_and_waiting_is_restored_when_the_queue_reopens() {
    let world = World::new().await;
    let a = world.add_task("t", "f", "one\n").await;
    let interruptions = || async {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event WHERE event_type='task.interruption_changed' AND entity_id='t'",
        )
        .fetch_one(world.db.pool())
        .await
        .unwrap()
    };
    let before = interruptions().await;
    let asked = world.place(&a.id, "validating", 2).await;
    world.ask(&asked, IntegrationStepAction::Park).await;
    world
        .set(
            &a.id,
            "state='parked',failure_kind='owner_required',failure_message='target_dirty: notes.txt'",
        )
        .await;
    settled(&world, "t", IntegrationStepAction::Park, "done").await;
    let task = world.task("t").await;
    assert_eq!(
        integration(&task),
        json!({"kind":"deferred","attempt_id":a.id,"cause":"target_dirty","owner_id":null,"message":"target_dirty: notes.txt"})
    );
    assert_eq!(task.status, "merging");
    assert_eq!(interruptions().await, before + 1, "one incident per wait");
    // The same request again is the same step: nothing more is written.
    world
        .ask(&world.attempt(&a.id).await, IntegrationStepAction::Park)
        .await;
    assert_eq!(world.step("t", IntegrationStepAction::Park).await.len(), 1);
    assert_eq!(interruptions().await, before + 1);

    // A wait that ends on its own raises no incident.
    world
        .set(
            &a.id,
            "effect_seq=3,failure_kind='infrastructure',failure_message='infrastructure: owner busy'",
        )
        .await;
    world
        .ask(&world.attempt(&a.id).await, IntegrationStepAction::Park)
        .await;
    eventually("the infrastructure wait was stated", || async {
        integration(&world.task("t").await)["cause"] == "infrastructure"
    })
    .await;
    assert_eq!(interruptions().await, before + 1);

    // A member of a suspended queue reads the queue's reason ...
    let queue = a.queue_id.clone().unwrap();
    world
        .set(
            &a.id,
            "state='queued',effect_seq=4,failure_kind=NULL,failure_message=NULL",
        )
        .await;
    sqlx::query("UPDATE integration_queue SET state='suspended',last_error_kind='target_unavailable',last_error='the default checkout is offline' WHERE id=?")
        .bind(&queue).execute(world.db.pool()).await.unwrap();
    world
        .ask(&world.attempt(&a.id).await, IntegrationStepAction::Park)
        .await;
    eventually("the suspended queue was stated", || async {
        integration(&world.task("t").await)["cause"] == "owner_offline"
    })
    .await;
    // ... and once the queue is open again the same request restores `waiting`.
    sqlx::query(
        "UPDATE integration_queue SET state='open',last_error_kind=NULL,last_error=NULL WHERE id=?",
    )
    .bind(&queue)
    .execute(world.db.pool())
    .await
    .unwrap();
    world.set(&a.id, "effect_seq=5").await;
    world
        .ask(&world.attempt(&a.id).await, IntegrationStepAction::Park)
        .await;
    eventually("waiting was restored", || async {
        integration(&world.task("t").await) == json!({"kind":"waiting","attempt_id":a.id})
    })
    .await;
}

/// A parked member holds no slot and has no owner effect outstanding: the
/// Task's Cancel ends its attempt in the Cancel's own transaction (a start
/// before the worker's next sweep finds no live attempt behind the terminal
/// Task), and the worker's release-step sweep asks for the `clear` that no
/// worker asked for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn task_cancel_ends_a_parked_attempt_in_its_own_transaction_and_the_sweep_asks_the_clear() {
    let world = World::new().await;
    let a = world.add_task("t", "f", "one\n").await;
    let asked = world.place(&a.id, "parked", 1).await;
    world
        .set(
            &a.id,
            "failure_kind='owner_required',failure_message='transfer_too_large: 300 MiB'",
        )
        .await;
    world.ask(&asked, IntegrationStepAction::Park).await;
    settled(&world, "t", IntegrationStepAction::Park, "done").await;
    assert_eq!(integration(&world.task("t").await)["kind"], "deferred");

    let cancelled = world.service.cancel_task("t").await.unwrap();
    assert_eq!(cancelled.status, "cancelled");
    let ended = world.attempt(&a.id).await;
    assert_eq!(ended.state, S::Cancelled, "ended with the Task write");
    assert!(!ended.current && ended.cancel_requested_at.is_none());
    assert!(world
        .step("t", IntegrationStepAction::Clear)
        .await
        .is_empty());

    let (worker, _daemons, _config) = production_ports(&world);
    assert!(worker.sweep_once().await.unwrap().is_empty());
    settled(&world, "t", IntegrationStepAction::Clear, "done").await;
    let task = world.task("t").await;
    assert_eq!(task.status, "cancelled");
    assert!(task.condition.integration_wait().is_none());
    // Asked once: a second sweep adds no step.
    worker.sweep_once().await.unwrap();
    assert_eq!(world.step("t", IntegrationStepAction::Clear).await.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn task_cancel_flags_the_attempt_in_its_own_transaction_and_clear_drops_the_statement() {
    let world = World::new().await;
    let a = world.add_task("t", "f", "one\n").await;
    // A queued member that was told something, then cancelled by its owner.
    let asked = world.place(&a.id, "parked", 1).await;
    world
        .set(
            &a.id,
            "failure_kind='owner_required',failure_message='transfer_too_large: 300 MiB'",
        )
        .await;
    world.ask(&asked, IntegrationStepAction::Park).await;
    settled(&world, "t", IntegrationStepAction::Park, "done").await;
    assert_eq!(integration(&world.task("t").await)["kind"], "deferred");
    // A settle the worker asked for that has not run yet (held back so the
    // Cancel finds it pending).
    let pending = world.place(&a.id, "awaiting_task_step", 2).await;
    let request = IntegrationQueueWorker::step_request(&pending, IntegrationStepAction::Settle);
    let held = TaskStepIntegrationPort::step_input(
        &request,
        "merging",
        world.task("t").await.version,
        None,
        "2099-01-01T00:00:00Z".into(),
    )
    .unwrap();
    world.db.enqueue_step(&held).await.unwrap();

    let cancelled = world.service.cancel_task("t").await.unwrap();
    assert_eq!(cancelled.status, "cancelled");
    let flagged = world.attempt(&a.id).await;
    assert!(
        flagged.cancel_requested_at.is_some(),
        "flagged with the Task write"
    );
    assert!(flagged.permit_json.is_none());
    // The unprotected step was dropped at once, not run to a refusal.
    let settle = &world.step("t", IntegrationStepAction::Settle).await[0];
    assert_eq!(settle.status, "superseded", "{:?}", settle.last_error);
    assert!(world
        .step("t", IntegrationStepAction::Result)
        .await
        .is_empty());

    // The worker releases the attempt and asks for the clear. Nothing else
    // ends an attempt: the flag alone leaves it where it was.
    world.ask(&flagged, IntegrationStepAction::Clear).await;
    assert_ne!(world.attempt(&a.id).await.state, S::Cancelled);
    let mut released = world.attempt(&a.id).await;
    released.state = S::Cancelled;
    world
        .db
        .transition_integration_attempt(released)
        .await
        .unwrap();
    settled(&world, "t", IntegrationStepAction::Clear, "done").await;
    let task = world.task("t").await;
    assert_eq!(task.status, "cancelled");
    assert!(task.condition.integration_wait().is_none());
    assert!(!world.step("t", IntegrationStepAction::Clear).await[0].entry_fenced);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_waits_behind_the_protected_result_step_and_the_merge_wins() {
    let world = World::new().await;
    let a = world.add_task("t", "f", "one\n").await;
    let asked = world.place(&a.id, "awaiting_task_step", 1).await;
    world.ask(&asked, IntegrationStepAction::Settle).await;
    settled(&world, "t", IntegrationStepAction::Settle, "done").await;
    // The worker committed the fast-forward: storage refuses a cancel flag.
    world.set(&a.id, "state='ff_inflight'").await;
    let flagged = world
        .db
        .request_integration_cancel(
            &a.id,
            world.attempt(&a.id).await.revision,
            &db::now_rfc3339(),
        )
        .await;
    assert!(matches!(flagged, Err(db::DbError::InvalidTransition)));

    let service = world.service.clone();
    let cancel = tokio::spawn(async move { service.cancel_task("t").await });
    // The Cancel is queued behind the protected step and cannot be claimed.
    eventually("the cancel command is queued", || async {
        world
            .db
            .task_steps("t")
            .await
            .unwrap()
            .iter()
            .any(|step| step.kind == "command" && step.status == "pending")
    })
    .await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(world.task("t").await.status, "merging");
    assert!(!cancel.is_finished());
    assert!(world.attempt(&a.id).await.cancel_requested_at.is_none());

    // The fast-forward lands; the result runs first.
    world.apply(&a.id).await;
    world.port.ready_result_step(&a.id, 1).await.unwrap();
    eventually("the merge was applied", || async {
        !world.transitions("t").await.is_empty()
    })
    .await;
    // The result ran first: the Task's first move is the merge, and the
    // attempt was answered without ever carrying a cancel flag.
    let first = world.transitions("t").await[0].clone();
    assert_eq!(
        (first.0.as_str(), first.1.as_str(), first.2.as_str()),
        ("merging", "done", "merge succeeded")
    );
    let _ = tokio::time::timeout(Duration::from_secs(20), cancel).await;
    let a = world.attempt(&a.id).await;
    assert!(a.cancel_requested_at.is_none());
    let answer: IntegrationStepAck = serde_json::from_value(a.effect_ack_json.unwrap()).unwrap();
    assert_eq!(
        (answer.action, answer.outcome),
        (IntegrationStepAction::Result, IntegrationStepOutcome::Done)
    );
    assert_eq!(
        world.comments("t").await[0],
        format!("Changes merged to main (SHA: {})", a.candidate_sha.unwrap())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cancel_racing_the_permit_has_exactly_one_winner_under_real_concurrency() {
    let world = World::new().await;
    let mut permits = 0;
    let started = std::time::Instant::now();
    // Eight rounds: a round the permit wins waits out the Cancel's own busy
    // wait, so twenty of them outran the round deadline on a loaded runner.
    for task in ["r00", "r01", "r02", "r03", "r04", "r05", "r06", "r07"] {
        permits += usize::from(race_once(&world, task).await);
    }
    // Either order is legal; each round asserted exactly one winner.
    println!(
        "permit won {permits} of 8 rounds in {:?}",
        started.elapsed()
    );
}
