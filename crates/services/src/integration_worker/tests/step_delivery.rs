//! Task-step redelivery, bounded reasks and release acknowledgments.
use super::*;

/// D2 checklist item 1. A counted retry that is re-queued, and a waiting
/// member of a queue that re-opens, each get one `park` step under a fresh
/// `effect_seq`: the level-triggered handler then restates `waiting`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_requeued_retry_and_a_reopened_queue_tell_the_task_it_waits_again() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    let mut pump = Pump::new(world.worker());
    world.facts.fail.store(true, Ordering::SeqCst);
    world.settle(&mut pump).await;
    let parked = world.attempt(&a.id).await;
    assert_eq!(parked.state, S::Parked);
    assert_eq!(park_seqs(&world, "a"), vec![parked.effect_seq]);
    // Due: re-queued with `waiting` restated, claimed, and parked again.
    world.clock.advance(31);
    world.settle(&mut pump).await;
    let again = world.attempt(&a.id).await;
    assert_eq!(again.state, S::Parked);
    assert_eq!(
        park_seqs(&world, "a"),
        vec![parked.effect_seq, parked.effect_seq + 1, again.effect_seq],
        "deferred, waiting again, deferred again"
    );
    assert_eq!(again.effect_seq, parked.effect_seq + 2);
    world.facts.fail.store(false, Ordering::SeqCst);
    world.clock.advance(121);
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&a.id).await.state, S::Completed);
    assert_eq!(park_seqs(&world, "a").len(), 4, "waiting once more");

    // A suspended queue: both members are told once. A new process re-opens
    // it: the member that is not claimed is told it waits in line again.
    world.add_repo("s").await;
    let b = world.add_task("s", "b", "b.txt", "b\n").await;
    let c = world.add_task("s", "c", "c.txt", "c\n").await;
    sqlx::query("UPDATE repo_location SET status='unavailable' WHERE id='l-s'")
        .execute(world.db.pool())
        .await
        .unwrap();
    for _ in 0..3 {
        world.settle(&mut pump).await;
        world.clock.advance(30);
    }
    assert_eq!(
        world.queue_of(&b).await.state,
        db::IntegrationQueueState::Suspended
    );
    let told = world.attempt(&c.id).await;
    assert_eq!(park_seqs(&world, "b").len(), 1);
    assert_eq!(park_seqs(&world, "c"), vec![told.effect_seq]);
    sqlx::query("UPDATE repo_location SET status='ready' WHERE id='l-s'")
        .execute(world.db.pool())
        .await
        .unwrap();
    world.steps.hold("b", IntegrationStepAction::Settle);
    let mut restarted = Pump::new(world.worker());
    world.settle(&mut restarted).await;
    assert_eq!(world.attempt(&c.id).await.state, S::Queued);
    assert_eq!(
        park_seqs(&world, "c"),
        vec![told.effect_seq, told.effect_seq + 1]
    );
    assert_eq!(
        park_seqs(&world, "b").len(),
        1,
        "the head is told by its steps"
    );
    // Not told a third time by later claims.
    world.steps.unhold("b", IntegrationStepAction::Settle);
    world.settle(&mut restarted).await;
    for attempt in [&b, &c] {
        assert_eq!(world.attempt(&attempt.id).await.state, S::Completed);
    }
    assert_eq!(park_seqs(&world, "c").len(), 2);
}

/// D2 checklist item 8. A deciding step that settled without answering is
/// asked again under a new `effect_seq`, a bounded number of times; then the
/// head parks with a typed cause and a counted retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dead_deciding_step_is_asked_again_then_the_head_parks_typed() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    world.steps.dying.lock().unwrap().insert("a".into());
    let mut pump = Pump::new(world.worker());
    world.settle(&mut pump).await;
    let parked = world.attempt(&a.id).await;
    assert_eq!(parked.state, S::Parked);
    assert_eq!(
        parked.failure_kind,
        Some(IntegrationFailureKind::Infrastructure)
    );
    assert!(parked
        .failure_message
        .as_deref()
        .unwrap()
        .starts_with("task_step_failed"));
    assert!(parked.available_at.is_some(), "a counted retry");
    let settles: Vec<i64> = world
        .steps
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|request| request.action == IntegrationStepAction::Settle)
        .map(|request| request.effect_seq)
        .collect();
    assert_eq!(
        settles.len(),
        3,
        "the first ask and two re-asks: {settles:?}"
    );
    assert!(settles.windows(2).all(|pair| pair[1] == pair[0] + 1));
    assert!(world.queue_of(&a).await.head_attempt_id.is_none());
    // The step side recovers: the retry merges.
    world.steps.dying.lock().unwrap().clear();
    world.clock.advance(31);
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&a.id).await.state, S::Completed);
    assert_eq!(ff_successes(&world, &a.id).await, 1);
}

/// D2 checklist items 9 and 6. A head waiting for its check asks again on a
/// timer (the ask applies a verdict whose delivery failed) instead of waiting
/// the check timeout out; an applied head re-arms its `result` step on every
/// sweep interval, not once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_waiting_check_is_asked_again_and_an_applied_head_re_arms_its_result() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    // The target moves after the Task forked: the head is rebased and checked.
    world.external_commit("r", "outside").await;
    world.steps.hold("a", IntegrationStepAction::RequestCheck);
    let mut pump = Pump::new(world.worker_with(IntegrationWorkerConfig {
        check_reask: Duration::from_secs(20),
        ..config()
    }));
    world.settle(&mut pump).await;
    let checking = world.attempt(&a.id).await;
    assert_eq!(checking.state, S::Checking);
    let asks = |world: &World| {
        world
            .steps
            .actions("a")
            .into_iter()
            .filter(|action| *action == IntegrationStepAction::RequestCheck)
            .count()
    };
    assert_eq!(asks(&world), 1);
    world.clock.advance(10);
    world.settle(&mut pump).await;
    assert_eq!(asks(&world), 1, "not due yet");
    world.clock.advance(11);
    world.settle(&mut pump).await;
    let asked = world.attempt(&a.id).await;
    assert_eq!(asked.state, S::Checking);
    assert_eq!(asks(&world), 2);
    assert_eq!(asked.effect_seq, checking.effect_seq + 1);
    assert_eq!(
        asked.slot_generation, checking.slot_generation,
        "same claim"
    );

    world.steps.hold("a", IntegrationStepAction::Result);
    world.steps.unhold("a", IntegrationStepAction::RequestCheck);
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&a.id).await.state, S::Applied);
    let mut readied = world.steps.readied.load(Ordering::SeqCst);
    assert!(readied >= 1);
    for sweep in 0..3 {
        world.clock.advance(31);
        world.settle(&mut pump).await;
        let now = world.steps.readied.load(Ordering::SeqCst);
        assert!(now > readied, "re-armed on sweep {sweep}");
        readied = now;
        assert_eq!(world.attempt(&a.id).await.state, S::Applied);
    }
    world.steps.unhold("a", IntegrationStepAction::Result);
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&a.id).await.state, S::Completed);
}

/// The `clear` path of the release-step sweep. An attempt cancelled because
/// its Task left `merging` tells the Task through one `clear` step. One that
/// settled without applying (dead, or refused because the Task had left its
/// entry, which a cancelled attempt's Task has by definition) is asked again
/// under the next key, three times, without writing the terminal attempt
/// row; then the queue says so in `last_error`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dead_clear_step_is_asked_again_three_times_then_the_queue_says_so() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "base", "from a\n").await;
    // The Task leaves `merging` before its head is validated.
    sqlx::query("UPDATE task SET status='cancelled' WHERE id='a'")
        .execute(world.db.pool())
        .await
        .unwrap();
    let mut pump = Pump::new(world.worker());
    world.settle(&mut pump).await;
    let cancelled = world.attempt(&a.id).await;
    assert_eq!(cancelled.state, S::Cancelled);
    assert!(cancelled.cancel_requested_at.is_none());
    let clears = || -> Vec<i64> {
        world
            .steps
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| {
                request.task_id == "a" && request.action == IntegrationStepAction::Clear
            })
            .map(|request| request.effect_seq)
            .collect()
    };
    let key = |seq: i64| format!("a|integration:{}:{seq}:clear", a.id);
    assert_eq!(clears(), vec![cancelled.effect_seq]);
    // A healthy step is never asked again.
    assert!(pump.worker.sweep_once().await.unwrap().is_empty());
    assert_eq!(clears(), vec![cancelled.effect_seq]);
    for round in 0..3 {
        // Dead, or refused because the Task left: both are asked again.
        let settled = key(cancelled.effect_seq + round);
        if round == 1 {
            world.steps.left.lock().unwrap().insert(settled);
        } else {
            world.steps.dead.lock().unwrap().insert(settled);
        }
        assert!(pump.worker.sweep_once().await.unwrap().is_empty());
        assert_eq!(
            clears(),
            (0..=round + 1)
                .map(|extra| cancelled.effect_seq + extra)
                .collect::<Vec<_>>()
        );
        // Asked once per settled step, not once per sweep.
        assert!(pump.worker.sweep_once().await.unwrap().is_empty());
        assert_eq!(clears().len() as i64, round + 2);
        assert!(world.queue_of(&a).await.last_error.is_none());
        // The terminal row is not written again.
        assert_eq!(world.attempt(&a.id).await.revision, cancelled.revision);
    }
    // The third re-ask settled without applying too: the owner is told, once.
    world
        .steps
        .dead
        .lock()
        .unwrap()
        .insert(key(cancelled.effect_seq + 3));
    assert!(pump.worker.sweep_once().await.unwrap().is_empty());
    let queue = world.queue_of(&a).await;
    let message = queue.last_error.clone().unwrap();
    assert!(
        message.starts_with("task_step_failed: the `clear` step of Task a"),
        "{message}"
    );
    for _ in 0..3 {
        assert!(pump.worker.sweep_once().await.unwrap().is_empty());
    }
    assert_eq!(clears().len(), 4, "the first ask and three re-asks");
    assert_eq!(world.queue_of(&a).await.revision, queue.revision);
    let now = world.attempt(&a.id).await;
    assert_eq!(now.state, S::Cancelled);
    assert_eq!(now.revision, cancelled.revision);
}

/// The `TaskLeft` path of the release-step sweep. A `send_back` step that
/// settled without applying because its Task had left the status entry of
/// the attempt is not asked again: the Task moved on by another route and
/// there is nothing left to tell it. The attempt records that once, and the
/// queue's owner is not bothered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_release_step_whose_task_left_is_not_asked_again() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "base", "from a\n").await;
    let b = world.add_task("r", "b", "base", "from b\n").await;
    let mut pump = Pump::new(world.worker());
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&a.id).await.state, S::Completed);
    let ejected = world.attempt(&b.id).await;
    assert_eq!(ejected.state, S::Ejected);
    let send_backs = || {
        world
            .steps
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| {
                request.task_id == "b" && request.action == IntegrationStepAction::SendBack
            })
            .count()
    };
    assert_eq!(send_backs(), 1);
    world.steps.left.lock().unwrap().insert(format!(
        "b|integration:{}:{}:send_back",
        b.id, ejected.effect_seq
    ));
    assert!(pump.worker.sweep_once().await.unwrap().is_empty());
    let now = world.attempt(&b.id).await;
    assert_eq!(now.state, S::Ejected);
    assert_eq!(now.effect_seq, ejected.effect_seq, "no new ask");
    assert_eq!(send_backs(), 1);
    let journal = now.operation_receipts_json.as_array().unwrap().clone();
    let noted: Vec<_> = journal
        .iter()
        .filter(|entry| entry["kind"] == "worker_reask_exhausted")
        .collect();
    assert_eq!(noted.len(), 1, "{journal:?}");
    assert_eq!(noted[0]["reason"], "task_left");
    assert_eq!(noted[0]["action"], "send_back");
    assert!(journal.iter().all(|entry| entry["kind"] != "worker_reask"));
    assert!(world.queue_of(&b).await.last_error.is_none());
    // Noted once: later sweeps leave the attempt and the queue alone.
    let queue_revision = world.queue_of(&b).await.revision;
    for _ in 0..3 {
        assert!(pump.worker.sweep_once().await.unwrap().is_empty());
    }
    assert_eq!(world.attempt(&b.id).await.revision, now.revision);
    assert_eq!(world.queue_of(&b).await.revision, queue_revision);
    assert_eq!(send_backs(), 1);
}

/// Blocker 6 of the D2a review. A released attempt's `send_back` step that
/// settled without applying is asked again by the sweep under a new
/// `effect_seq`, three times; then the queue says so in `last_error` and the
/// attempt is left alone. A step that answers ends the asking.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_release_step_is_asked_again_three_times_then_the_queue_says_so() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "base", "from a\n").await;
    let b = world.add_task("r", "b", "base", "from b\n").await;
    let mut pump = Pump::new(world.worker());
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&a.id).await.state, S::Completed);
    let ejected = world.attempt(&b.id).await;
    assert_eq!(ejected.state, S::Ejected);
    let send_backs = || -> Vec<i64> {
        world
            .steps
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| {
                request.task_id == "b" && request.action == IntegrationStepAction::SendBack
            })
            .map(|request| request.effect_seq)
            .collect()
    };
    let kill = |seq: i64| {
        world
            .steps
            .dead
            .lock()
            .unwrap()
            .insert(format!("b|integration:{}:{seq}:send_back", b.id));
    };
    // A healthy step is never asked again.
    assert!(pump.worker.sweep_once().await.unwrap().is_empty());
    assert_eq!(send_backs(), vec![ejected.effect_seq]);
    // The step died: three re-asks, each under the next `effect_seq`.
    for round in 0..3 {
        kill(ejected.effect_seq + round);
        assert!(pump.worker.sweep_once().await.unwrap().is_empty());
        let now = world.attempt(&b.id).await;
        assert_eq!(now.state, S::Ejected);
        assert_eq!(now.effect_seq, ejected.effect_seq + round + 1);
        assert_eq!(send_backs().len() as i64, round + 2);
        // Asked once per death, not once per sweep.
        assert!(pump.worker.sweep_once().await.unwrap().is_empty());
        assert_eq!(send_backs().len() as i64, round + 2);
        assert!(world.queue_of(&b).await.last_error.is_none());
    }
    // The third re-ask died too: the queue's owner is told, once.
    kill(ejected.effect_seq + 3);
    assert!(pump.worker.sweep_once().await.unwrap().is_empty());
    let queue = world.queue_of(&b).await;
    let message = queue.last_error.clone().unwrap();
    assert!(
        message.starts_with("task_step_failed: the `send_back` step of Task b"),
        "{message}"
    );
    assert_eq!(
        queue.last_error_kind,
        Some(IntegrationFailureKind::Infrastructure)
    );
    let (revision, attempt_revision) = (queue.revision, world.attempt(&b.id).await.revision);
    for _ in 0..3 {
        assert!(pump.worker.sweep_once().await.unwrap().is_empty());
    }
    assert_eq!(send_backs().len(), 4, "the first ask and three re-asks");
    assert_eq!(world.queue_of(&b).await.revision, revision);
    assert_eq!(world.attempt(&b.id).await.revision, attempt_revision);
    assert_eq!(world.attempt(&b.id).await.state, S::Ejected);
}
