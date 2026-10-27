//! Cancellation at queue-head states and permit races.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_in_each_cancellable_head_state_releases_without_a_merge() {
    for state in [
        S::Validating,
        S::Rebasing,
        S::Checking,
        S::AwaitingTaskStep,
        S::ReadyFf,
    ] {
        let world = World::new().await;
        let mut pump = Pump::new(world.worker());
        let held = hold_in(&world, &mut pump, state).await;
        let tip = world.tip("r").await;
        world
            .db
            .request_integration_cancel(&held.id, held.revision, &db::now_rfc3339())
            .await
            .unwrap_or_else(|error| panic!("{state}: {error}"));
        world.settle(&mut pump).await;
        let cancelled = world.attempt(&held.id).await;
        assert_eq!(cancelled.state, S::Cancelled, "{state}");
        assert!(cancelled.effect_intent_json.is_none(), "{state}");
        assert_eq!(world.tip("r").await, tip, "{state}");
        assert!(
            world
                .steps
                .actions("a")
                .contains(&IntegrationStepAction::Clear),
            "{state}"
        );
        assert!(
            world.queue_of(&held).await.head_attempt_id.is_none(),
            "{state}"
        );
    }
    // Critical states refuse the request; the merge runs to its result.
    for state in [S::FfInflight, S::Applied] {
        let world = World::new().await;
        let mut pump = Pump::new(world.worker());
        let held = hold_in(&world, &mut pump, state).await;
        assert!(matches!(
            world
                .db
                .request_integration_cancel(&held.id, held.revision, &db::now_rfc3339())
                .await,
            Err(db::DbError::InvalidTransition)
        ));
        world.steps.unhold("a", IntegrationStepAction::Result);
        world.settle(&mut pump).await;
        assert_eq!(world.attempt(&held.id).await.state, S::Completed, "{state}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_of_released_and_waiting_members_is_applied_by_the_sweep() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "base", "from a\n").await;
    let b = world.add_task("r", "b", "base", "from b\n").await;
    let c = world.add_task("r", "c", "c.txt", "c\n").await;
    let mut pump = Pump::new(world.worker());
    // c waits behind a held head; b will be ejected by its conflict.
    world.steps.hold("a", IntegrationStepAction::Settle);
    world.settle(&mut pump).await;
    let queued = world.attempt(&c.id).await;
    assert_eq!(queued.state, S::Queued);
    world
        .db
        .request_integration_cancel(&queued.id, queued.revision, &db::now_rfc3339())
        .await
        .unwrap();
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&c.id).await.state, S::Cancelled);
    world.steps.unhold("a", IntegrationStepAction::Settle);
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&a.id).await.state, S::Completed);
    let ejected = world.attempt(&b.id).await;
    assert_eq!(ejected.state, S::Ejected);
    world
        .db
        .request_integration_cancel(&ejected.id, ejected.revision, &db::now_rfc3339())
        .await
        .unwrap();
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&b.id).await.state, S::Cancelled);
    for task in ["b", "c"] {
        assert!(world
            .steps
            .actions(task)
            .contains(&IntegrationStepAction::Clear));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_against_the_permit_has_exactly_one_winner() {
    // Both orders; the real race is the next test.
    for order in ["cancel_first", "commit_first"] {
        let world = World::new().await;
        let mut pump = Pump::new(world.worker());
        let ready = hold_in(&world, &mut pump, S::ReadyFf).await;
        assert!(ready.permit_json.is_some());
        let tip = world.tip("r").await;
        let mut driver = pump.drivers.pop().unwrap();
        let cancel = || async {
            world
                .db
                .request_integration_cancel(&ready.id, ready.revision, &db::now_rfc3339())
                .await
        };
        let cancelled = match order {
            "cancel_first" => {
                let cancelled = cancel().await;
                driver.pass().await.unwrap();
                cancelled
            }
            "commit_first" => {
                assert_eq!(driver.pass().await.unwrap(), Pass::Progress);
                assert_eq!(world.attempt(&ready.id).await.state, S::FfInflight);
                cancel().await
            }
            _ => unreachable!(),
        };
        pump.drivers.push(driver);
        world.settle(&mut pump).await;
        let end = world.attempt(&ready.id).await;
        match cancelled {
            Ok(_) => {
                assert_eq!(end.state, S::Cancelled, "{order}");
                assert_eq!(world.tip("r").await, tip, "{order}");
                assert_eq!(ff_successes(&world, &ready.id).await, 0, "{order}");
            }
            Err(_) => {
                assert_eq!(end.state, S::Completed, "{order}");
                assert_eq!(Some(world.tip("r").await), end.candidate_sha, "{order}");
            }
        }
        match order {
            "cancel_first" => assert_eq!(end.state, S::Cancelled),
            "commit_first" => assert_eq!(end.state, S::Completed),
            _ => {}
        }
    }
}

/// The cancel request runs on another runtime thread, released together with
/// the driver's cancel-or-commit pass. Whatever the interleaving: a granted
/// cancel means no fast-forward, a refused one means the merge completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_racing_the_permit_on_another_thread_has_exactly_one_winner() {
    let (mut cancels, mut merges) = (0, 0);
    for round in 0..10 {
        let world = World::new().await;
        let mut pump = Pump::new(world.worker());
        let ready = hold_in(&world, &mut pump, S::ReadyFf).await;
        let tip = world.tip("r").await;
        let mut driver = pump.drivers.pop().unwrap();
        let gate = Arc::new(tokio::sync::Barrier::new(2));
        let cancel = tokio::spawn({
            let (db, gate, ready) = (world.db.clone(), gate.clone(), ready.clone());
            async move {
                gate.wait().await;
                // Vary who gets to the row first.
                for _ in 0..round % 4 {
                    tokio::task::yield_now().await;
                }
                db.request_integration_cancel(&ready.id, ready.revision, &db::now_rfc3339())
                    .await
            }
        });
        let pass = tokio::spawn(async move {
            gate.wait().await;
            let pass = driver.pass().await;
            (driver, pass)
        });
        let cancelled = cancel.await.unwrap();
        let (driver, pass) = pass.await.unwrap();
        pass.unwrap();
        pump.drivers.push(driver);
        world.settle(&mut pump).await;
        let end = world.attempt(&ready.id).await;
        match cancelled {
            Ok(_) => {
                cancels += 1;
                assert_eq!(end.state, S::Cancelled, "round {round}");
                assert_eq!(world.tip("r").await, tip, "round {round}");
                assert!(receipts_of(&world, &ready.id).await.is_empty());
            }
            // Lost the compare-and-set to the commit, or refused after it.
            Err(db::DbError::VersionConflict | db::DbError::InvalidTransition) => {
                merges += 1;
                assert_eq!(end.state, S::Completed, "round {round}");
                assert_eq!(Some(world.tip("r").await), end.candidate_sha);
                assert_eq!(ff_successes(&world, &ready.id).await, 1);
            }
            Err(error) => panic!("round {round}: {error}"),
        }
    }
    assert_eq!(cancels + merges, 10);
}

/// A real `git rebase` is running (held by a `pre-rebase` hook) when the
/// cancel request arrives: the driver stops the effect, the owner records
/// the cancelled receipt, and only then is the head released.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_during_a_running_rebase_stops_git_and_releases_after_the_receipt() {
    let world = World::new().await;
    let mut pump = Pump::new(world.worker());
    let held = hold_in(&world, &mut pump, S::Rebasing).await;
    let candidate = held.candidate_sha.clone().unwrap();
    let tip = world.tip("r").await;
    let (running, hook) = (
        world.temp.path().join("rebase-running"),
        world.repo("r").join(".git/hooks/pre-rebase"),
    );
    std::fs::create_dir_all(hook.parent().unwrap()).unwrap();
    process_supervisor::executable::write_executable(
        &hook,
        format!(
            "#!/bin/sh\ntouch '{}'\nwhile :; do sleep 0.05; done\n",
            running.display()
        ),
    )
    .unwrap();
    let mut driver = pump.drivers.pop().unwrap();
    let pass = tokio::spawn(async move {
        let pass = driver.pass().await;
        (driver, pass)
    });
    wait_for!("git rebase to run", running.exists());
    let running_attempt = world.attempt(&held.id).await;
    assert_eq!(running_attempt.state, S::Rebasing);
    assert_eq!(running_attempt.effect_intent_json.unwrap()["started"], true);
    assert!(!pass.is_finished());
    world
        .db
        .request_integration_cancel(&held.id, running_attempt.revision, &db::now_rfc3339())
        .await
        .unwrap();
    let (driver, pass) = tokio::time::timeout(Duration::from_secs(20), pass)
        .await
        .expect("the cancel reaches the running rebase")
        .unwrap();
    assert_eq!(pass.unwrap(), Pass::Released);
    drop(driver);
    let end = world.attempt(&held.id).await;
    assert_eq!(end.state, S::Cancelled);
    assert!(end.effect_intent_json.is_none());
    let receipts = receipts_of(&world, &held.id).await;
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].request.kind, IntegrationOperationKind::Rebase);
    assert_eq!(receipts[0].result["kind"], "cancelled");
    assert_eq!(
        receipts[0].operation_state,
        db::IntegrationOperationState::Failed
    );
    // Git was stopped before it changed anything.
    assert_eq!(
        git_out(&world.tree("a"), &["rev-parse", "HEAD"]).await,
        Some(candidate)
    );
    assert!(!git::detect_rebase_in_progress(&world.tree("a"))
        .await
        .unwrap());
    assert_eq!(world.tip("r").await, tip);
    assert!(world
        .steps
        .actions("a")
        .contains(&IntegrationStepAction::Clear));
    assert!(world.queue_of(&held).await.head_attempt_id.is_none());
    std::fs::remove_file(&hook).unwrap();
}
