//! Queue merge effects, review checks and rebased-head placement.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unchanged_target_merges_the_reviewed_commit_without_a_check() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    let candidate = a.original_candidate_sha.clone().unwrap();
    let mut pump = Pump::new(world.worker());
    world.settle(&mut pump).await;
    let done = world.attempt(&a.id).await;
    assert_eq!(done.state, S::Completed);
    assert_eq!(world.tip("r").await, candidate);
    assert_eq!(done.integrated_sha.as_deref(), Some(candidate.as_str()));
    assert!(done.started_at.is_some());
    assert_eq!(
        world.steps.actions("a"),
        vec![IntegrationStepAction::Settle, IntegrationStepAction::Result]
    );
    let timings = done.phase_timings.unwrap();
    assert_eq!(
        timings.check,
        Some(IntegrationCheckTiming::Skipped {
            reason: IntegrationCiSkipReason::TargetUnchanged
        })
    );
    assert_eq!(timings.rounds, 1);
    assert!(timings.queued_ms.is_some() && timings.ff_ms.is_some());
    assert!(timings.head_total_ms.is_some() && timings.lost_races.is_empty());
    let queue = world.queue_of(&a).await;
    assert!(queue.head_attempt_id.is_none() && queue.lease_owner.is_none());
    assert!(world.steps.readied.load(Ordering::SeqCst) >= 1);
    // Snapshot and enqueue ports.
    let snapshot = pump
        .worker
        .integration_queue_snapshot(&queue.id, 10)
        .await
        .unwrap()
        .unwrap();
    assert!(snapshot.members.is_empty() && !snapshot.driven_here);
    pump.worker.notify_enqueued(&queue.id);
}

/// The Task's checkout stands on a commit of the target that is not the
/// admitted candidate (another writer is rebasing it and HEAD is on the
/// target tip, or it was reset there). That HEAD is in the target, the
/// reviewed commit is not: nothing landed, and the Task is not reported
/// merged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_checkout_standing_on_the_target_is_not_read_as_the_candidate_landed() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    let candidate = a.original_candidate_sha.clone().unwrap();
    world.external_commit("r", "other.txt").await;
    let tip = world.tip("r").await;
    git_out(&world.tree("a"), &["checkout", "--detach", &tip])
        .await
        .expect("detach");
    let mut pump = Pump::new(world.worker());
    world.settle(&mut pump).await;
    let end = world.attempt(&a.id).await;
    assert!(
        !matches!(end.state, S::Applied | S::Completed),
        "reported landed: {:?}",
        end.state
    );
    assert_eq!(end.integrated_sha, None);
    assert_eq!(world.tip("r").await, tip);
    assert!(git_out(
        &world.repo("r"),
        &["merge-base", "--is-ancestor", &candidate, &tip]
    )
    .await
    .is_none());
    assert!(!world
        .steps
        .actions("a")
        .contains(&IntegrationStepAction::Result));
    assert_eq!(end.state, S::NeedsReview);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn moved_target_is_rebased_checked_and_fast_forwarded_in_one_pass_per_receipt() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    let b = world.add_task("r", "b", "b.txt", "b\n").await;
    let mut pump = Pump::new(world.worker());
    // Members behind the head are shown in order.
    world.steps.hold("a", IntegrationStepAction::Settle);
    world.settle(&mut pump).await;
    let snapshot = pump
        .worker
        .integration_queue_snapshot(a.queue_id.as_deref().unwrap(), 10)
        .await
        .unwrap()
        .unwrap();
    assert!(snapshot.driven_here);
    assert_eq!(
        snapshot
            .members
            .iter()
            .map(|member| (member.task_id.as_str(), member.position))
            .collect::<Vec<_>>(),
        vec![("a", Some(0)), ("b", Some(1))]
    );
    world.steps.unhold("a", IntegrationStepAction::Settle);
    // Stop b before its rebase, then run exactly one pass: receipt and
    // transition happen in it although the receipt bumped the revision (R13).
    arm(&pump.worker, S::Rebasing, CrashPoint::BeforeEffect);
    assert!(world.pump(&mut pump).await.is_some());
    assert_eq!(world.attempt(&a.id).await.state, S::Completed);
    assert_eq!(world.attempt(&b.id).await.state, S::Rebasing);
    pump.worker.set_fault(None);
    let mut driver = pump.drivers.pop().expect("the stopped driver");
    assert_eq!(driver.pass().await.unwrap(), Pass::Progress);
    let checking = world.attempt(&b.id).await;
    assert_eq!(checking.state, S::Checking);
    assert_ne!(checking.candidate_sha, checking.original_candidate_sha);
    pump.drivers.push(driver);
    world.settle(&mut pump).await;
    let done = world.attempt(&b.id).await;
    assert_eq!(done.state, S::Completed);
    assert_eq!(Some(world.tip("r").await), done.candidate_sha);
    assert_eq!(
        world.steps.actions("b"),
        vec![
            IntegrationStepAction::RequestCheck,
            IntegrationStepAction::Result
        ]
    );
    let timings = done.phase_timings.unwrap();
    assert!(matches!(
        timings.check,
        Some(IntegrationCheckTiming::Ran { .. })
    ));
    assert!(timings.rebase_ms.is_some());
    // Caught up with an earlier queue member: recorded, and free.
    assert_eq!(timings.lost_races.len(), 1);
    assert_eq!(
        timings.lost_races[0].kind,
        IntegrationLostRaceKind::QueueMember
    );
    assert_eq!(timings.external_target_moves(), 0);
    assert_eq!(ff_successes(&world, &b.id).await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflict_and_red_check_eject_with_handoff_data_and_free_the_slot() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "base", "from a\n").await;
    let b = world.add_task("r", "b", "base", "from b\n").await;
    let c = world.add_task("r", "c", "c.txt", "c\n").await;
    let d = world.add_task("r", "d", "d.txt", "d\n").await;
    world.steps.red.lock().unwrap().insert("c".into());
    let mut pump = Pump::new(world.worker());
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&a.id).await.state, S::Completed);
    let conflicted = world.attempt(&b.id).await;
    assert_eq!(conflicted.state, S::Ejected);
    assert_eq!(conflicted.conflict_paths_json, Some(json!(["base"])));
    assert!(conflicted.current);
    assert_eq!(
        world.steps.actions("b"),
        vec![IntegrationStepAction::SendBack]
    );
    let red = world.attempt(&c.id).await;
    assert_eq!(red.state, S::Ejected);
    assert_eq!(
        red.failure_kind,
        Some(IntegrationFailureKind::CandidateCheckFailed)
    );
    assert_eq!(red.failure_message.as_deref(), Some("tests failed"));
    assert_eq!(
        world.steps.actions("c"),
        vec![
            IntegrationStepAction::RequestCheck,
            IntegrationStepAction::SendBack
        ]
    );
    // The member behind both ejections still merged.
    let last = world.attempt(&d.id).await;
    assert_eq!(last.state, S::Completed);
    assert_eq!(Some(world.tip("r").await), last.candidate_sha);
    assert!(world.queue_of(&a).await.head_attempt_id.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_target_moves_spend_the_allowance_and_the_sixth_parks() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    let mut pump = Pump::new(world.worker_with(IntegrationWorkerConfig {
        max_rounds: 50,
        ..config()
    }));
    let deciding = [
        IntegrationStepAction::Settle,
        IntegrationStepAction::RequestCheck,
    ];
    deciding
        .iter()
        .for_each(|action| world.steps.hold("a", *action));
    for round in 1..=6 {
        // The head holds the slot and waits for its permit; a writer outside
        // Forge moves the target under it; then the permit arrives and the
        // fast-forward finds the moved target.
        world.settle(&mut pump).await;
        let waiting = world.attempt(&a.id).await;
        assert!(
            matches!(waiting.state, S::AwaitingTaskStep | S::Checking),
            "round {round}: {}",
            waiting.state
        );
        world
            .external_commit("r", &format!("outside-{round}"))
            .await;
        deciding
            .iter()
            .for_each(|action| world.steps.unhold("a", *action));
        assert!(world.steps.answer(&world.db).await);
        deciding
            .iter()
            .for_each(|action| world.steps.hold("a", *action));
        world.settle(&mut pump).await;
        if round < 6 {
            let moved = world.attempt(&a.id).await.phase_timings.unwrap();
            assert_eq!(moved.external_target_moves(), round, "round {round}");
        }
    }
    world.settle(&mut pump).await;
    let parked = world.attempt(&a.id).await;
    assert_eq!(parked.state, S::Parked);
    assert_eq!(
        parked.failure_kind,
        Some(IntegrationFailureKind::OwnerRequired)
    );
    assert!(parked
        .failure_message
        .as_deref()
        .unwrap()
        .starts_with("external_target_moves_exhausted"));
    assert!(parked.available_at.is_none());
    let timings = parked.phase_timings.unwrap();
    assert_eq!(timings.external_target_moves(), 6);
    assert!(world
        .steps
        .actions("a")
        .contains(&IntegrationStepAction::Park));
    assert!(world.queue_of(&a).await.head_attempt_id.is_none());
}

/// After its owner reconnects, a Task's placement stays `disconnected` until
/// the owner reconciliation has looked at it, and a check asked for
/// meanwhile is refused for good. A head whose rebase is done waits in
/// `rebasing` until the placement is ready: the receipt is kept and adopted,
/// the rebase is not repeated, and no check is asked for. The wait is
/// bounded by the Task-step wait; then the head leaves the slot waiting for
/// its owner machine (`owner_offline`): no retry is counted, however long
/// the machine is away, and the attempt always has a time it is asked again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rebased_head_waits_for_a_ready_placement_before_it_asks_for_the_check() {
    let rebases = |attempt: &IntegrationAttempt| {
        serde_json::from_value::<Vec<db::IntegrationEffectReceipt>>(
            attempt.effect_receipts_json.clone(),
        )
        .unwrap()
        .into_iter()
        .filter(|receipt| receipt.request.kind == db::IntegrationOperationKind::Rebase)
        .count()
    };
    for becomes_ready in [true, false] {
        let world = World::new().await;
        let a = world.add_task("r", "a", "a.txt", "a\n").await;
        world.external_commit("r", "outside").await;
        world.facts.placement_down.store(true, Ordering::SeqCst);
        let mut pump = Pump::new(world.worker());
        for _ in 0..2 {
            world.settle(&mut pump).await;
            let waiting = world.attempt(&a.id).await;
            assert_eq!(waiting.state, S::Rebasing);
            assert!(waiting.effect_intent_json.is_none());
            assert_eq!(rebases(&waiting), 1, "rebased once, receipt kept");
            assert!(!world
                .steps
                .actions("a")
                .contains(&IntegrationStepAction::RequestCheck));
        }
        if becomes_ready {
            world.facts.placement_down.store(false, Ordering::SeqCst);
            world.settle(&mut pump).await;
            let merged = world.attempt(&a.id).await;
            assert_eq!(merged.state, S::Completed);
            assert_eq!(rebases(&merged), 1, "the rebase was not repeated");
            assert!(world
                .steps
                .actions("a")
                .contains(&IntegrationStepAction::RequestCheck));
        } else {
            // In steps shorter than the lease, so the session keeps it, and
            // only until the head has parked. The test clock is the wall
            // clock plus an offset: on a slow host the step wait runs out a
            // few steps early, and steps taken after the park reach its
            // retry (`a_rebased_head_parked_for_its_owner_continues_from_its_
            // rebase_when_asked_again` is that order).
            let step = (pump.worker.config.renew_every.as_secs() as i64).max(1);
            let mut waited = 0;
            while waited <= pump.worker.config.step_wait.as_secs() as i64
                && world.attempt(&a.id).await.state != S::Parked
            {
                world.clock.advance(step);
                waited += step;
                world.settle(&mut pump).await;
            }
            let parked = world.attempt(&a.id).await;
            assert_eq!(parked.state, S::Parked);
            assert_eq!(
                parked.failure_kind,
                Some(IntegrationFailureKind::TargetUnavailable)
            );
            assert!(
                parked.failure_message.as_deref().is_some_and(|message| {
                    message.starts_with("owner_offline")
                        && message.contains("not ready on its machine")
                }),
                "{:?}",
                parked.failure_message
            );
            assert!(parked.available_at.is_some(), "retried by the sweep");
            // Parked means out of the slot: the queue goes on without it.
            assert!(world.queue_of(&a).await.head_attempt_id.is_none());
            let journal = parked.operation_receipts_json.as_array().unwrap();
            assert!(
                journal
                    .iter()
                    .filter(|entry| entry["kind"] == "worker_park")
                    .all(|entry| entry["counted"] == false),
                "an owner wait counts no retry: {journal:?}"
            );
        }
    }
}

/// A rebased head that waited for its placement and parked `owner_offline`
/// is asked again by the sweep. Its checkout stands on the rebased commit;
/// the attempt still names the commit it was admitted with (the rebase was
/// never applied), and the receipt of the rebase says why. The new round
/// validates first: it continues from that rebase. It does not send the Task
/// back to review for a candidate "changed after it was admitted", which
/// nobody changed, and it does not rebase again.
///
/// This is the order a throttled host produced in the test above: the test
/// clock is the wall clock plus an offset, so the step wait ran out a few
/// passes early and the passes left over reached the retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rebased_head_parked_for_its_owner_continues_from_its_rebase_when_asked_again() {
    let rebases = |attempt: &IntegrationAttempt| {
        serde_json::from_value::<Vec<db::IntegrationEffectReceipt>>(
            attempt.effect_receipts_json.clone(),
        )
        .unwrap()
        .into_iter()
        .filter(|receipt| receipt.request.kind == db::IntegrationOperationKind::Rebase)
        .count()
    };
    let due = |parked: &IntegrationAttempt, world: &World| {
        let at = DateTime::parse_from_rfc3339(parked.available_at.as_deref().unwrap())
            .unwrap()
            .with_timezone(&Utc);
        (at - world.clock.now()).num_seconds().max(0) + 1
    };
    for ready_when_asked_again in [false, true] {
        let world = World::new().await;
        let a = world.add_task("r", "a", "a.txt", "a\n").await;
        world.external_commit("r", "outside").await;
        world.facts.placement_down.store(true, Ordering::SeqCst);
        let mut pump = Pump::new(world.worker());
        world.settle(&mut pump).await;
        let admitted = world.attempt(&a.id).await;
        assert_eq!(admitted.state, S::Rebasing);
        // Past the step wait, in steps shorter than the lease so the
        // session keeps it: the head parks.
        let step = (pump.worker.config.renew_every.as_secs() as i64).max(1);
        let mut waited = 0;
        while waited <= pump.worker.config.step_wait.as_secs() as i64
            && world.attempt(&a.id).await.state != S::Parked
        {
            world.clock.advance(step);
            waited += step;
            world.settle(&mut pump).await;
        }
        let parked = world.attempt(&a.id).await;
        assert_eq!(parked.state, S::Parked, "{:?}", parked.failure_message);
        assert_eq!(rebases(&parked), 1);
        assert_eq!(
            parked.candidate_sha, admitted.candidate_sha,
            "the rebase was never applied"
        );
        // The sweep asks again.
        world
            .facts
            .placement_down
            .store(!ready_when_asked_again, Ordering::SeqCst);
        world.clock.advance(due(&parked, &world));
        world.settle(&mut pump).await;
        let asked = world.attempt(&a.id).await;
        assert_ne!(
            asked.state,
            S::NeedsReview,
            "its own rebase is not a changed candidate: {:?}",
            asked.failure_message
        );
        assert_eq!(rebases(&asked), 1, "the rebase was not repeated");
        if !ready_when_asked_again {
            // Waiting for the placement again, in the slot.
            assert_eq!(asked.state, S::Rebasing, "{:?}", asked.failure_message);
            assert!(!world
                .steps
                .actions("a")
                .contains(&IntegrationStepAction::RequestCheck));
            world.facts.placement_down.store(false, Ordering::SeqCst);
            world.settle(&mut pump).await;
        }
        let merged = world.attempt(&a.id).await;
        assert_eq!(merged.state, S::Completed, "{:?}", merged.failure_message);
        assert_eq!(rebases(&merged), 1, "the rebase was not repeated");
        assert!(world
            .steps
            .actions("a")
            .contains(&IntegrationStepAction::RequestCheck));
        assert_eq!(
            merged.checks_commit_sha, merged.integrated_sha,
            "the rebased commit was checked before it merged"
        );
    }
}

/// The `result` step's proof failed: the owner of the target says the target
/// does not contain the commit the attempt recorded as merged (it was rewound
/// after the fast-forward). The head is not completed: it leaves the slot
/// with a typed cause and its recorded result dropped, the retry timer
/// queues it again, and validation reads the target as it is: the Task
/// merges again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_result_the_target_does_not_confirm_leaves_the_slot_typed_and_merges_again() {
    let world = World::new().await;
    let mut pump = Pump::new(world.worker());
    let held = hold_in(&world, &mut pump, S::Applied).await;
    let merged = held.integrated_sha.clone().expect("recorded as merged");
    assert_eq!(world.tip("r").await, merged);
    // The force-push: the target goes back to where it was.
    let before = held.integrated_before_sha.clone().expect("before");
    git_out(&world.repo("r"), &["reset", "--hard", &before])
        .await
        .unwrap();
    // What the `result` step answers when its proof fails.
    let mut answered = held.clone();
    answered.effect_ack_json = Some(json!(IntegrationStepAck {
        effect_seq: held.effect_seq,
        generation: held.slot_generation,
        action: IntegrationStepAction::Result,
        outcome: IntegrationStepOutcome::NotLanded,
        check: None,
        message: Some("`main` does not contain the commit".into()),
    }));
    answered.acknowledged_at = Some(db::now_rfc3339());
    world
        .db
        .transition_integration_attempt(answered)
        .await
        .unwrap();
    world.settle(&mut pump).await;

    let parked = world.attempt(&held.id).await;
    assert_eq!(parked.state, S::Parked);
    assert_eq!(parked.failure_kind, Some(IntegrationFailureKind::NeedsFact));
    assert!(
        parked
            .failure_message
            .as_deref()
            .unwrap()
            .starts_with("target_missing_commit"),
        "{:?}",
        parked.failure_message
    );
    assert!(parked.integrated_sha.is_none(), "no merge is recorded");
    assert!(parked.available_at.is_some(), "asked again on the timer");
    assert!(world.queue_of(&held).await.head_attempt_id.is_none());
    assert!(world
        .steps
        .actions("a")
        .contains(&IntegrationStepAction::Park));
    let status: String = sqlx::query_scalar("SELECT status FROM task WHERE id='a'")
        .fetch_one(world.db.pool())
        .await
        .unwrap();
    assert_eq!(status, "merging", "never done without the proof");

    // The timer: the attempt is queued again and merges for real.
    world.steps.unhold("a", IntegrationStepAction::Result);
    world.clock.advance(3600);
    world.settle(&mut pump).await;
    let done = world.attempt(&held.id).await;
    assert_eq!(done.state, S::Completed);
    assert_eq!(done.integrated_sha.as_deref(), Some(merged.as_str()));
    assert_eq!(world.tip("r").await, merged);
    assert_eq!(ff_successes(&world, &held.id).await, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ci_triage_head_wait_extends_for_each_subcheck_and_expired_runs_are_bounded() {
    for phase in ["rerun", "base"] {
        for state in ["queued", "running"] {
            let world = World::new().await;
            let a = world.add_task("r", "a", "a.txt", "a\n").await;
            world.external_commit("r", "outside").await;
            world.steps.hold("a", IntegrationStepAction::RequestCheck);
            // Advancing the clock tests one head session's timer, not lease
            // takeover (which starts a fresh session with its own wait).
            let mut pump = Pump::new(world.worker_with(IntegrationWorkerConfig {
                lease: Duration::from_secs(12 * 3600),
                ..config()
            }));
            world.settle(&mut pump).await;
            let checking = world.attempt(&a.id).await;
            assert_eq!(checking.state, S::Checking);
            let now = db::now_rfc3339();
            let deadline = (world.clock.now() + chrono::Duration::hours(12)).to_rfc3339();
            sqlx::query("INSERT INTO check_run(id,project_id,repo_id,commit_sha,spec_digest,identity_key,input_json,cacheable,state,operation_id,deadline_at,created_at,updated_at) VALUES('sub','p','r',?,'digest','identity','{}',0,?,'operation',?,?,?)")
                .bind(checking.candidate_sha.as_deref()).bind(state).bind(&deadline).bind(&now).bind(&now).execute(world.db.pool()).await.unwrap();
            let authority = crate::integration_steps::check_authority(&checking).unwrap();
            let key = format!(
                "task-step/1|integration|a|{}|ci-triage:{phase}:{authority}",
                checking.expected_epoch
            );
            sqlx::query("INSERT INTO check_consumer(id,project_id,repo_id,task_id,status_epoch,origin,request_key,identity_key,run_id,created_at) VALUES('sub','p','r','a',?,'integration',?,'identity','sub',?)")
                .bind(checking.expected_epoch).bind(key).bind(&now).execute(world.db.pool()).await.unwrap();
            for _ in 0..2 {
                world.clock.advance(2101);
                world.settle(&mut pump).await;
                assert_eq!(
                    world.attempt(&a.id).await.state,
                    S::Checking,
                    "{phase} {state} must retain its head past 35 minutes"
                );
            }
            // A lost worker cannot keep extending the head after the check's
            // own deadline. No ack is supplied: the normal wait then expires.
            sqlx::query("UPDATE check_run SET deadline_at=? WHERE id='sub'")
                .bind(&now)
                .execute(world.db.pool())
                .await
                .unwrap();
            world.clock.advance(2101);
            world.settle(&mut pump).await;
            let end = world.attempt(&a.id).await;
            assert_eq!(
                (end.state, end.failure_kind),
                (S::Parked, Some(IntegrationFailureKind::Timeout))
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn review_round_merge_ci_worker_preserves_signature_message_and_output_end() {
    let world = World::new().await;
    world.add_task("r", "first", "first.txt", "first\n").await;
    let attempt = world
        .add_task("r", "red-message", "second.txt", "second\n")
        .await;
    world.steps.red.lock().unwrap().insert("red-message".into());
    let message = format!(
        "cargo test\n{}\n{}END",
        (0..20)
            .map(|i| format!("binary::case_{i}"))
            .collect::<Vec<_>>()
            .join("\n"),
        "x".repeat(2045)
    );
    assert!(message.len() > 2048);
    *world.steps.red_message.lock().unwrap() = Some(message.clone());
    let mut pump = Pump::new(world.worker());
    world.settle(&mut pump).await;
    let red = world.attempt(&attempt.id).await;
    assert_eq!(red.state, S::Ejected);
    assert_eq!(red.failure_message.as_deref(), Some(message.as_str()));
    assert!(world
        .steps
        .actions("red-message")
        .contains(&IntegrationStepAction::SendBack));
}
