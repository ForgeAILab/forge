//! Supervised queue progress, shutdown and imported merge outcomes.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queues_progress_independently_and_no_queue_starves_under_the_head_cap() {
    let world = World::new().await;
    world.add_repo("s").await;
    let slow = world.add_task("r", "slow", "slow.txt", "slow\n").await;
    let fast = world.add_task("s", "fast", "fast.txt", "fast\n").await;
    world.steps.hold("slow", IntegrationStepAction::Settle);
    let mut pump = Pump::new(world.worker());
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&slow.id).await.state, S::AwaitingTaskStep);
    assert_eq!(world.attempt(&fast.id).await.state, S::Completed);
    assert_eq!(pump.worker.active_heads(), 1);
    world.steps.unhold("slow", IntegrationStepAction::Settle);
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&slow.id).await.state, S::Completed);

    // One head at a time over three queues with two members each: the
    // rotation serves every queue before any queue is served twice.
    let world = World::new().await;
    world.add_repo("s").await;
    world.add_repo("t").await;
    for repo in ["r", "s", "t"] {
        for n in 1..=2 {
            let task = format!("{repo}{n}");
            world
                .add_task(repo, &task, &format!("{task}.txt"), "x\n")
                .await;
        }
    }
    let mut pump = Pump::new(world.worker_with(IntegrationWorkerConfig {
        max_heads: 1,
        ..config()
    }));
    world.settle(&mut pump).await;
    let order: Vec<String> = world
        .steps
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|request| request.action == IntegrationStepAction::Result)
        .map(|request| request.task_id.clone())
        .collect();
    assert_eq!(order.len(), 6);
    let firsts: HashSet<char> = order[..3]
        .iter()
        .map(|task| task.chars().next().unwrap())
        .collect();
    assert_eq!(firsts.len(), 3, "{order:?}");
    assert!(
        order[..3].iter().all(|task| task.ends_with('1')),
        "{order:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_worker_writes_nothing() {
    let world = World::new().await;
    let done = world.add_task("r", "done", "done.txt", "x\n").await;
    let mut pump = Pump::new(world.worker());
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&done.id).await.state, S::Completed);
    // A parked member that is not due, and an open queue with no member.
    world.add_repo("s").await;
    world
        .db
        .create_or_get_integration_queue("s", "main")
        .await
        .unwrap();
    std::fs::write(world.repo("r").join("dirty"), "dirty").unwrap();
    let parked = world.add_task("r", "parked", "parked.txt", "x\n").await;
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&parked.id).await.state, S::Parked);
    let before = digest(&world.db).await;
    let steps = world.steps.requests.lock().unwrap().len();
    for _ in 0..5 {
        assert!(pump.worker.sweep_once().await.unwrap().is_empty());
        world.clock.advance(30);
    }
    assert_eq!(digest(&world.db).await, before);
    assert_eq!(world.steps.requests.lock().unwrap().len(), steps);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn twenty_members_drain_in_order_under_the_supervised_loop() {
    let world = World::new().await;
    let mut attempts = Vec::new();
    for n in 0..20 {
        let task = format!("t{n:02}");
        attempts.push(
            world
                .add_task("r", &task, &format!("{task}.txt"), "x\n")
                .await,
        );
    }
    let worker = world.worker();
    let (stop, shutdown) = watch::channel(false);
    let running = tokio::spawn(worker.clone().run(shutdown));
    // Bounded by progress, not by a total: under a loaded machine twenty
    // heads take as long as they take, and only a queue that stops moving is
    // a failure. One head never needs a minute.
    let mut completed = 0;
    let mut stalled = tokio::time::Instant::now();
    loop {
        world.steps.answer(&world.db).await;
        let mut now = 0;
        for attempt in &attempts {
            now += usize::from(world.attempt(&attempt.id).await.state == S::Completed);
        }
        if now == attempts.len() {
            break;
        }
        if now > completed {
            completed = now;
            stalled = tokio::time::Instant::now();
        }
        assert!(
            stalled.elapsed() < Duration::from_secs(60),
            "the queue stopped draining at {completed} of {}",
            attempts.len()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("bounded shutdown")
        .unwrap()
        .unwrap();
    let mut completed = Vec::new();
    for attempt in &attempts {
        let end = world.attempt(&attempt.id).await;
        assert_eq!(end.state, S::Completed);
        completed.push((end.completed_at.unwrap(), end.queue_seq));
    }
    let mut by_time = completed.clone();
    by_time.sort();
    assert_eq!(by_time, completed, "members left in queue order");
    assert_eq!(
        git_out(&world.repo("r"), &["rev-list", "--count", "main"])
            .await
            .unwrap(),
        "21"
    );
    assert_eq!(worker.active_heads(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_mid_head_is_clean_and_the_next_worker_finishes() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    world.steps.hold("a", IntegrationStepAction::Settle);
    let worker = world.worker();
    let (stop, shutdown) = watch::channel(false);
    let running = tokio::spawn(worker.clone().run(shutdown));
    wait_for!(
        "the head to wait for its permit",
        world.attempt(&a.id).await.state == S::AwaitingTaskStep
    );
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("bounded shutdown")
        .unwrap()
        .unwrap();
    assert_eq!(worker.active_heads(), 0);
    let left = world.attempt(&a.id).await;
    assert_eq!(left.state, S::AwaitingTaskStep);
    assert!(left.effect_intent_json.is_none());
    let before = digest(&world.db).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        digest(&world.db).await,
        before,
        "nothing runs after shutdown"
    );
    world.steps.unhold("a", IntegrationStepAction::Settle);
    world.clock.advance(120);
    let mut pump = Pump::new(world.worker());
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&a.id).await.state, S::Completed);
}

/// Importer rows P1 and P2 at the head of a queue: a merge whose result the
/// old path never recorded. A candidate the target already contains is
/// reported, never merged again and never reported failed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_imported_unknown_merge_that_landed_is_reported_and_never_repeated() {
    for shape in [Legacy::RunningRemoteOperation, Legacy::IntentWithoutOutcome] {
        let world = World::new().await;
        let head = world.legacy_task("a", "a.txt", shape).await;
        // The old merge landed; only its record is missing.
        git_out(&world.repo("r"), &["merge", "--ff-only", &head])
            .await
            .unwrap();
        let imported = world.import("a").await;
        assert_eq!(imported.state, S::Reconciling, "{shape:?}");
        assert_eq!(
            world.queue_of(&imported).await.head_attempt_id.as_deref(),
            Some(imported.id.as_str()),
            "{shape:?}: an unknown result pins its queue"
        );
        let b = world.add_task("r", "b", "b.txt", "b\n").await;
        // Run twice: a second start imports nothing more.
        assert_eq!(
            world
                .db
                .import_integration_pass(100)
                .await
                .unwrap()
                .imported,
            0
        );
        let mut pump = Pump::new(world.worker());
        world.settle(&mut pump).await;
        let end = world.attempt(&imported.id).await;
        assert_eq!(
            end.state,
            S::Completed,
            "{shape:?}: {:?}",
            end.failure_message
        );
        assert_eq!(end.integrated_sha.as_deref(), Some(head.as_str()));
        assert_eq!(ff_successes(&world, &imported.id).await, 0, "{shape:?}");
        assert_eq!(
            world.steps.actions("a"),
            vec![IntegrationStepAction::Result],
            "{shape:?}: reported once, nothing else asked of the Task"
        );
        // The queue is open again and the next member merged behind it.
        assert_eq!(
            world.queue_of(&imported).await.state,
            db::IntegrationQueueState::Open
        );
        assert_eq!(world.attempt(&b.id).await.state, S::Completed);
    }
}

/// The same rows when the old merge did not land: the queue merges the
/// candidate, once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_imported_unknown_merge_that_did_not_land_is_merged_once_by_the_queue() {
    for shape in [Legacy::RunningRemoteOperation, Legacy::IntentWithoutOutcome] {
        let world = World::new().await;
        let head = world.legacy_task("a", "a.txt", shape).await;
        let imported = world.import("a").await;
        assert_eq!(imported.state, S::Reconciling, "{shape:?}");
        let mut pump = Pump::new(world.worker());
        world.settle(&mut pump).await;
        let end = world.attempt(&imported.id).await;
        assert_eq!(
            end.state,
            S::Completed,
            "{shape:?}: {:?}",
            end.failure_message
        );
        assert_eq!(world.tip("r").await, head, "{shape:?}");
        assert_eq!(ff_successes(&world, &imported.id).await, 1, "{shape:?}");
        assert_eq!(
            world.queue_of(&imported).await.state,
            db::IntegrationQueueState::Open
        );
    }
}

/// Importer row P2 while the checkouts cannot be read: the head is
/// quarantined with a typed reason and asked again on the reconcile timer.
/// It never loops on a refused write, never leaves as `queued` or `parked`,
/// and nothing behind it starts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_imported_unknown_merge_with_unreadable_checkouts_is_quarantined_not_looped() {
    let world = World::new().await;
    let head = world
        .legacy_task("a", "a.txt", Legacy::RunningRemoteOperation)
        .await;
    let imported = world.import("a").await;
    assert_eq!(
        imported.current_operation_state,
        Some(db::IntegrationOperationState::Uncertain)
    );
    let b = world.add_task("r", "b", "b.txt", "b\n").await;
    world.facts.fail.store(true, Ordering::SeqCst);
    let mut pump = Pump::new(world.worker());
    for round in 0..3 {
        // `settle` panics on a driver error: none in any round.
        world.settle(&mut pump).await;
        let unknown = world.attempt(&imported.id).await;
        assert_eq!(unknown.state, S::Quarantined, "round {round}");
        assert_eq!(
            unknown.failure_kind,
            Some(IntegrationFailureKind::NeedsFact)
        );
        assert_eq!(
            world.queue_of(&imported).await.state,
            db::IntegrationQueueState::Quarantined
        );
        assert_eq!(world.attempt(&b.id).await.state, S::Queued);
        world.clock.advance(600);
    }
    assert!(world
        .steps
        .actions("a")
        .contains(&IntegrationStepAction::Park));
    // Readable again: Git shows it never landed, and the queue merges it.
    world.facts.fail.store(false, Ordering::SeqCst);
    world.clock.advance(600);
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&imported.id).await.state, S::Completed);
    assert_eq!(ff_successes(&world, &imported.id).await, 1);
    assert_eq!(world.attempt(&b.id).await.state, S::Completed);
    let log = git_out(&world.repo("r"), &["log", "--format=%H", "main"])
        .await
        .unwrap();
    assert_eq!(log.lines().filter(|line| *line == head).count(), 1);
}
