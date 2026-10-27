//! Lease takeover, infrastructure parking and transfer cleanup.
use super::*;

/// The process died inside the rebase: the intent is started and has no
/// receipt. The owner settles it from Git under its own lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn takeover_of_a_started_rebase_is_settled_by_the_owner_from_git() {
    for ran in [false, true] {
        let world = World::new().await;
        let mut pump = Pump::new(world.worker());
        let a = hold_in(&world, &mut pump, S::Rebasing).await;
        started_intent(&world, &a, IntegrationOperationKind::Rebase).await;
        if ran {
            // Git finished; the receipt was never written.
            git_out(
                &world.tree("a"),
                &["rebase", a.target_tip_sha.as_deref().unwrap()],
            )
            .await
            .unwrap();
        }
        drop(pump);
        world.clock.advance(120);
        let mut pump = Pump::new(world.worker());
        let mut drivers = pump.worker.sweep_once().await.unwrap();
        assert_eq!(world.attempt(&a.id).await.state, S::Reconciling);
        pump.drivers.append(&mut drivers);
        world.settle(&mut pump).await;
        let end = world.attempt(&a.id).await;
        assert_eq!(end.state, S::Completed, "ran={ran}");
        assert_eq!(Some(world.tip("r").await), end.candidate_sha);
        let receipts = receipts_of(&world, &a.id).await;
        let kinds: Vec<_> = receipts
            .iter()
            .map(|receipt| (receipt.request.kind, receipt.result["kind"].to_string()))
            .collect();
        use IntegrationOperationKind::{FastForward, Rebase};
        let expected = if ran {
            // The finished rebase is proven and adopted: no second rebase.
            vec![(Rebase, "\"completed\""), (FastForward, "\"completed\"")]
        } else {
            vec![
                (Rebase, "\"not_performed\""),
                (Rebase, "\"completed\""),
                (FastForward, "\"completed\""),
            ]
        };
        assert_eq!(
            kinds,
            expected
                .into_iter()
                .map(|(kind, result)| (kind, result.to_owned()))
                .collect::<Vec<_>>(),
            "ran={ran}"
        );
        // The rebased commit was checked before it merged.
        assert!(world
            .steps
            .actions("a")
            .contains(&IntegrationStepAction::RequestCheck));
        assert_eq!(
            world.queue_of(&a).await.state,
            db::IntegrationQueueState::Open
        );
    }
}

/// A permit that does not bind the head is asked for again, but not for
/// ever: each ask is a Task step.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_permit_that_never_binds_parks_after_a_bounded_number_of_asks() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    world.steps.bad_permit.store(true, Ordering::SeqCst);
    let mut pump = Pump::new(world.worker());
    // A refused permit ends a pump round (the driver waits one poll).
    for _ in 0..6 {
        world.settle(&mut pump).await;
    }
    let parked = world.attempt(&a.id).await;
    assert_eq!(parked.state, S::Parked);
    assert_eq!(
        parked.failure_kind,
        Some(IntegrationFailureKind::Infrastructure)
    );
    let asks = world
        .steps
        .actions("a")
        .iter()
        .filter(|action| **action == IntegrationStepAction::Settle)
        .count();
    assert_eq!(asks, 3);
    assert!(world.queue_of(&a).await.head_attempt_id.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_holder_that_lost_its_lease_is_fenced_by_generation() {
    let world = World::new().await;
    world.steps.hold("a", IntegrationStepAction::Settle);
    let mut old = Pump::new(world.worker());
    let a = hold_in(&world, &mut old, S::AwaitingTaskStep).await;
    let old_fence = world
        .db
        .integration_owner_fence(&a.id)
        .await
        .unwrap()
        .unwrap();
    // The old lease runs out on the worker clock and another worker claims.
    world.clock.advance(120);
    let mut new = Pump::new(world.worker());
    let mut drivers = new.worker.sweep_once().await.unwrap();
    assert_eq!(drivers.len(), 1);
    assert_eq!(drivers[0].generation(), old_fence.generation + 1);
    new.drivers.append(&mut drivers);
    // The old driver stops without a write, and the storage fence refuses
    // its generation whatever the wall clock says about the old lease.
    let before = world.attempt(&a.id).await.revision;
    assert_eq!(old.drivers[0].pass().await.unwrap(), Pass::Lost);
    assert_eq!(world.attempt(&a.id).await.revision, before);
    let stale = IntegrationEffectRequest {
        fence: old_fence,
        kind: IntegrationOperationKind::Rebase,
        witness: json!({"workspace":{"handle":"x"}}),
    };
    assert!(matches!(
        world.db.begin_integration_effect(stale).await.unwrap(),
        IntegrationEffectAdmission::Refused(db::IntegrationEffectRefusal::StaleFence)
    ));
    world.steps.unhold("a", IntegrationStepAction::Settle);
    world.settle(&mut new).await;
    assert_eq!(world.attempt(&a.id).await.state, S::Completed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_suspended_queue_tells_its_members_once_and_reopens_when_the_target_is_ready() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    let b = world.add_task("r", "b", "b.txt", "b\n").await;
    sqlx::query("UPDATE repo_location SET status='unavailable' WHERE id='l-r'")
        .execute(world.db.pool())
        .await
        .unwrap();
    let mut pump = Pump::new(world.worker());
    for _ in 0..3 {
        world.settle(&mut pump).await;
        world.clock.advance(30);
    }
    let queue = world.queue_of(&a).await;
    assert_eq!(queue.state, db::IntegrationQueueState::Suspended);
    assert_eq!(
        queue.last_error_kind,
        Some(IntegrationFailureKind::TargetUnavailable)
    );
    for task in ["a", "b"] {
        assert_eq!(
            world.steps.actions(task),
            vec![IntegrationStepAction::Park],
            "{task}"
        );
    }
    assert_eq!(world.attempt(&a.id).await.state, S::Queued);
    sqlx::query("UPDATE repo_location SET status='ready' WHERE id='l-r'")
        .execute(world.db.pool())
        .await
        .unwrap();
    world.settle(&mut pump).await;
    assert_eq!(
        world.queue_of(&a).await.state,
        db::IntegrationQueueState::Open
    );
    for attempt in [&a, &b] {
        assert_eq!(world.attempt(&attempt.id).await.state, S::Completed);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_quarantined_queue_without_a_head_reopens_only_when_nothing_ran() {
    let world = World::new().await;
    let imported = world.add_task("r", "imported", "i.txt", "i\n").await;
    let b = world.add_task("r", "b", "b.txt", "b\n").await;
    // What the importer leaves: a quarantined member, a quarantined queue,
    // no head and no lease.
    sqlx::query("UPDATE integration_attempt SET state='quarantined',current_operation_state='uncertain' WHERE id=?")
        .bind(&imported.id)
        .execute(world.db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE integration_queue SET state='quarantined'")
        .execute(world.db.pool())
        .await
        .unwrap();
    let mut pump = Pump::new(world.worker());
    // An unknown result in the queue: it stays shut, without a write loop.
    world.settle(&mut pump).await;
    let shut = world.queue_of(&b).await;
    assert_eq!(shut.state, db::IntegrationQueueState::Quarantined);
    world.settle(&mut pump).await;
    assert_eq!(world.queue_of(&b).await.revision, shut.revision);
    assert_eq!(world.attempt(&b.id).await.state, S::Queued);
    // The lookup resolved it: nothing was ever admitted for that member.
    sqlx::query("UPDATE integration_attempt SET current_operation_state=NULL WHERE id=?")
        .bind(&imported.id)
        .execute(world.db.pool())
        .await
        .unwrap();
    world.clock.advance(61);
    world.settle(&mut pump).await;
    assert_eq!(
        world.queue_of(&b).await.state,
        db::IntegrationQueueState::Open
    );
    assert_eq!(world.attempt(&b.id).await.state, S::Completed);
    // The pinned attempt is its owner's to retry.
    assert_eq!(world.attempt(&imported.id).await.state, S::Quarantined);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_paused_project_parks_with_backoff_and_one_task_step() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    world.facts.paused.store(true, Ordering::SeqCst);
    let mut pump = Pump::new(world.worker());
    let mut gaps = Vec::new();
    for _ in 0..4 {
        world.settle(&mut pump).await;
        let parked = world.attempt(&a.id).await;
        assert_eq!(parked.state, S::Parked);
        assert!(parked
            .failure_message
            .as_deref()
            .unwrap()
            .starts_with("project_paused"));
        let due = parse_time(parked.available_at.as_deref()).unwrap();
        gaps.push((due - world.clock.now()).num_seconds());
        world.clock.advance(gaps.last().unwrap() + 1);
    }
    assert!(
        gaps.windows(2).all(|pair| pair[1] > pair[0]),
        "the retry backs off: {gaps:?}"
    );
    // A wait with no end of its own tells the Task once, not on each retry.
    assert_eq!(world.steps.actions("a"), vec![IntegrationStepAction::Park]);
    world.facts.paused.store(false, Ordering::SeqCst);
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&a.id).await.state, S::Completed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn takeover_of_an_inflight_fast_forward_reconciles_applied_or_not_performed() {
    for landed in [false, true] {
        let world = World::new().await;
        let mut pump = Pump::new(world.worker());
        let a = hold_in(&world, &mut pump, S::FfInflight).await;
        started_ff_intent(&world, &a).await;
        if landed {
            // Git ran; the receipt was never written.
            git_out(
                &world.repo("r"),
                &["merge", "--ff-only", a.candidate_sha.as_deref().unwrap()],
            )
            .await
            .unwrap();
        }
        drop(pump);
        world.clock.advance(120);
        let mut pump = Pump::new(world.worker());
        // The claim alone forces reconciliation.
        let mut drivers = pump.worker.sweep_once().await.unwrap();
        assert_eq!(world.attempt(&a.id).await.state, S::Reconciling);
        pump.drivers.append(&mut drivers);
        world.settle(&mut pump).await;
        let end = world.attempt(&a.id).await;
        assert_eq!(end.state, S::Completed, "landed={landed}");
        assert_eq!(Some(world.tip("r").await), end.candidate_sha);
        // Proven applied: no second fast-forward. Proven not performed: one.
        assert_eq!(ff_successes(&world, &a.id).await, 1, "landed={landed}");
        let receipts: Vec<db::IntegrationEffectReceipt> =
            serde_json::from_value(end.effect_receipts_json).unwrap();
        assert_eq!(
            receipts.len(),
            if landed { 1 } else { 2 },
            "landed={landed}"
        );
        assert_eq!(
            world.queue_of(&a).await.state,
            db::IntegrationQueueState::Open
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_fast_forward_quarantines_and_reopens_only_with_a_witness() {
    let world = World::new().await;
    let mut pump = Pump::new(world.worker());
    let a = hold_in(&world, &mut pump, S::FfInflight).await;
    let b = world.add_task("r", "b", "b.txt", "b\n").await;
    started_ff_intent(&world, &a).await;
    git_out(
        &world.repo("r"),
        &["merge", "--ff-only", a.candidate_sha.as_deref().unwrap()],
    )
    .await
    .unwrap();
    // The checkout cannot be read: the owner has no proof either way.
    let (git_dir, hidden) = (world.repo("r").join(".git"), world.repo("r").join("hidden"));
    std::fs::rename(&git_dir, &hidden).unwrap();
    drop(pump);
    world.clock.advance(120);
    let mut pump = Pump::new(world.worker());
    world.settle(&mut pump).await;
    let unknown = world.attempt(&a.id).await;
    assert_eq!(unknown.state, S::Quarantined);
    assert_eq!(
        unknown.failure_kind,
        Some(IntegrationFailureKind::NeedsFact)
    );
    assert!(unknown.effect_intent_json.is_some());
    let queue = world.queue_of(&a).await;
    assert_eq!(queue.state, db::IntegrationQueueState::Quarantined);
    assert_eq!(
        queue.last_error_kind,
        Some(IntegrationFailureKind::NeedsFact)
    );
    assert!(world
        .steps
        .actions("a")
        .contains(&IntegrationStepAction::Park));
    // Nothing behind it is started, and waiting does not change that.
    world.clock.advance(600);
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&a.id).await.state, S::Quarantined);
    assert_eq!(world.attempt(&b.id).await.state, S::Queued);
    // No witness, no re-open.
    let queue = world.queue_of(&a).await;
    assert!(world
        .db
        .reopen_integration_queue(
            &queue.id,
            queue.revision,
            &db::IntegrationQueueReopenWitness::NoEffect {
                attempt_id: a.id.clone()
            }
        )
        .await
        .is_err());
    // The checkout is readable again: the owner proves the exact merge, and
    // that settled receipt is the witness.
    std::fs::rename(&hidden, &git_dir).unwrap();
    world.clock.advance(600);
    world.settle(&mut pump).await;
    let end = world.attempt(&a.id).await;
    assert_eq!(end.state, S::Completed);
    assert_eq!(ff_successes(&world, &a.id).await, 1);
    assert_eq!(
        world.queue_of(&a).await.state,
        db::IntegrationQueueState::Open
    );
    assert_eq!(world.attempt(&b.id).await.state, S::Completed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parked_infrastructure_retries_then_needs_the_owner_and_transfer_cap_parks_typed() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    let mut pump = Pump::new(world.worker());
    world.facts.fail.store(true, Ordering::SeqCst);
    for retry in 0..3 {
        world.settle(&mut pump).await;
        let parked = world.attempt(&a.id).await;
        assert_eq!(parked.state, S::Parked, "retry {retry}");
        assert_eq!(
            parked.failure_kind,
            Some(IntegrationFailureKind::Infrastructure)
        );
        assert!(parked.available_at.is_some());
        assert!(world.queue_of(&a).await.head_attempt_id.is_none());
        // Not due yet: the sweep leaves it alone.
        world.settle(&mut pump).await;
        assert_eq!(world.attempt(&a.id).await.revision, parked.revision);
        world.clock.advance(601);
    }
    world.settle(&mut pump).await;
    let exhausted = world.attempt(&a.id).await;
    assert_eq!(exhausted.state, S::Parked);
    assert_eq!(
        exhausted.failure_kind,
        Some(IntegrationFailureKind::OwnerRequired)
    );
    assert!(exhausted.available_at.is_none());
    assert!(exhausted
        .failure_message
        .as_deref()
        .unwrap()
        .starts_with("retries_exhausted"));

    // A Task on a non-default location: commits are moved in and out.
    world.facts.fail.store(false, Ordering::SeqCst);
    let b = world.add_task("r", "b", "b.txt", "b\n").await;
    world.transfer.remote.lock().unwrap().insert("b".into());
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&b.id).await.state, S::Completed);
    assert_eq!(
        *world.transfer.calls.lock().unwrap(),
        vec![
            ObjectTransferDirection::Inbound,
            ObjectTransferDirection::Outbound
        ]
    );
    // One key per attempt, claim generation and direction; the refs of the
    // finished attempt are released on both ends.
    let keys = world.transfer.keys.lock().unwrap().clone();
    assert_eq!(keys.len(), 2);
    assert!(keys[0].starts_with(&format!("{}-", b.id)) && keys[0].ends_with("-in"));
    assert!(keys[1].ends_with("-out"));
    let released = world.transfer.released.lock().unwrap().clone();
    assert_eq!(released.len(), 1);
    assert_eq!(released[0].attempt_id, b.id);
    assert_eq!(released[0].target.repo_location_id, "l-r");
    let c = world.add_task("r", "c", "c.txt", "c\n").await;
    world.transfer.remote.lock().unwrap().insert("c".into());
    world.transfer.too_large.store(true, Ordering::SeqCst);
    world.settle(&mut pump).await;
    let capped = world.attempt(&c.id).await;
    assert_eq!(capped.state, S::Parked);
    assert_eq!(
        capped.failure_kind,
        Some(IntegrationFailureKind::OwnerRequired)
    );
    assert!(capped.available_at.is_none());
    assert!(capped
        .failure_message
        .as_deref()
        .unwrap()
        .starts_with("transfer_too_large"));
    // Nothing was moved, the slot is free, and whatever the refused transfer
    // left on either end is released like any other exit's.
    assert!(world.queue_of(&c).await.head_attempt_id.is_none());
    let released = world.transfer.released.lock().unwrap().clone();
    assert!(released.iter().any(|release| release.attempt_id == c.id));
    // The cap a production worker runs with.
    assert_eq!(
        IntegrationWorkerConfig::default().transfer_cap_bytes,
        256 * 1024 * 1024
    );
    assert_eq!(
        IntegrationWorkerConfig::default().transfer_cap_bytes,
        git::integration::MAX_OBJECT_TRANSFER_BYTES
    );
}

/// The machine that holds the head's Task checkout is removed while its
/// rebase is in flight there (the default checkout is on the server). The
/// removal settles the effect as failed for the owner that performed it and
/// parks the attempt. A parked attempt holds no slot: the worker's session
/// for it ends, and the member behind it is claimed and merges. The queue
/// does not wait for the removed machine's Task.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_head_whose_task_machine_is_removed_mid_rebase_does_not_stall_the_queue() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    let b = world.add_task("r", "b", "b.txt", "b\n").await;
    let now = db::now_rfc3339();
    let pool = world.db.pool();
    sqlx::query("INSERT INTO daemon (id,machine_id,hostname,os,arch,status,created_at,updated_at) VALUES ('d','remote-machine','host','linux','aarch64','offline',?,?)").bind(&now).bind(&now).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO runtime(id,daemon_id,kind,workspace_root,status,labels_json,created_at,updated_at) VALUES ('rt','d','codex','root','offline','{}',?,?)").bind(&now).bind(&now).execute(pool).await.unwrap();
    // Admission recorded the Task's owner: daemon `d`.
    sqlx::query("UPDATE integration_attempt SET owner_kind='daemon',daemon_id='d',runtime_id='rt' WHERE id=?")
        .bind(&a.id)
        .execute(pool)
        .await
        .unwrap();
    let mut pump = Pump::new(world.worker());
    let mut drivers = pump.worker.sweep_once().await.unwrap();
    assert_eq!(drivers.len(), 1);
    let queue = world.queue_of(&a).await;
    assert_eq!(queue.head_attempt_id.as_deref(), Some(a.id.as_str()));
    // The head is rebasing on `d`: a started intent that names it as the
    // performing owner, as the routing owner records it.
    sqlx::query("UPDATE integration_attempt SET state='rebasing',revision=revision+1 WHERE id=?")
        .bind(&a.id)
        .execute(pool)
        .await
        .unwrap();
    let request = db::IntegrationEffectRequest {
        fence: world
            .db
            .integration_owner_fence(&a.id)
            .await
            .unwrap()
            .unwrap(),
        kind: IntegrationOperationKind::Rebase,
        witness: json!({"workspace":{"handle":"tree-on-d"},"target_branch":"main","effect_owner":{"location_id":"l-d","owner_kind":"daemon","daemon_id":"d","runtime_id":"rt"}}),
    };
    let db::IntegrationEffectAdmission::Started(mut guard) = world
        .db
        .begin_integration_effect(request.clone())
        .await
        .unwrap()
    else {
        panic!("the rebase was not admitted");
    };
    assert!(guard.start().await.unwrap().is_none());
    drop(guard);

    world
        .db
        .remove_daemon("d", "admin", true, "local-host", false)
        .await
        .unwrap();
    // Settled by the removal, for the owner that performed it.
    let parked = world.attempt(&a.id).await;
    assert_eq!(parked.state, S::Parked);
    assert!(parked.effect_intent_json.is_none());
    let receipt = world
        .db
        .integration_effect_receipt(&request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        receipt.operation_state,
        db::IntegrationOperationState::Failed
    );
    let queue = world.queue_of(&a).await;
    assert_eq!(queue.state, db::IntegrationQueueState::Open);
    assert_eq!(queue.head_attempt_id, None);
    // The session that drove the removed head ends without a write.
    assert_eq!(drivers[0].pass().await.unwrap(), Pass::Lost);
    drivers.clear();
    // The member behind it proceeds to the merge.
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&b.id).await.state, S::Completed);
    assert_eq!(ff_successes(&world, &b.id).await, 1);
    let after = world.attempt(&a.id).await;
    assert_eq!(after.state, S::Parked);
    assert_eq!(
        after.failure_kind,
        Some(IntegrationFailureKind::Infrastructure)
    );
    assert_eq!(ff_successes(&world, &a.id).await, 0);
    assert!(world.queue_of(&a).await.head_attempt_id.is_none());
}

/// The session that made a head's transfers dies after the fast-forward.
/// The session that takes the head over makes no transfer itself, but it is
/// the one that releases the head: it takes the two ends from the attempt's
/// durable owner fields and the queue's target, so the refs do not stay
/// until the next server start.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_takeover_session_releases_the_transfer_refs_of_the_head_it_finishes() {
    let world = World::new().await;
    let now = db::now_rfc3339();
    // The Task's own (non-default) location, as admission records it.
    sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,is_default,status,created_at,updated_at) VALUES('l-task','r','server',?,'managed_clone',0,'ready',?,?)")
        .bind(world.temp.path().join("clone-task").to_str())
        .bind(&now)
        .bind(&now)
        .execute(world.db.pool())
        .await
        .unwrap();
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    sqlx::query(
        "UPDATE integration_attempt SET repo_location_id='l-task',owner_kind='server' WHERE id=?",
    )
    .bind(&a.id)
    .execute(world.db.pool())
    .await
    .unwrap();
    world.transfer.remote.lock().unwrap().insert("a".into());
    world.external_commit("r", "outside").await;
    let worker = world.worker();
    let fired = arm(&worker, S::FfInflight, CrashPoint::AfterEffectReceipt);
    let mut pump = Pump::new(worker);
    assert!(world.pump(&mut pump).await.is_some(), "the session dies");
    assert!(fired.load(Ordering::SeqCst));
    assert!(!world.transfer.calls.lock().unwrap().is_empty());
    assert!(world.transfer.released.lock().unwrap().is_empty());
    drop(pump);
    world.clock.advance(120);
    let transfers = world.transfer.calls.lock().unwrap().len();
    let mut pump = Pump::new(world.worker());
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&a.id).await.state, S::Completed);
    assert_eq!(ff_successes(&world, &a.id).await, 1);
    assert_eq!(
        world.transfer.calls.lock().unwrap().len(),
        transfers,
        "the takeover moved nothing itself"
    );
    let released = world.transfer.released.lock().unwrap().clone();
    assert_eq!(released.len(), 1, "{}", released.len());
    assert_eq!(released[0].attempt_id, a.id);
    assert_eq!(released[0].task.repo_location_id, "l-task");
    assert_eq!(released[0].task.owner, EffectOwner::Server);
    assert_eq!(released[0].target.repo_location_id, "l-r");
    assert_eq!(released[0].target.owner, EffectOwner::Server);
}

/// The reply to an object transfer is lost (inbound, then outbound). The
/// objects may or may not have arrived, and the worker does not guess: the
/// head leaves the slot as a counted infrastructure retry, whatever the
/// transfer left on either end is released, and the retry transfers under
/// the key of its own claim generation. The Task is rebased and merged once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_transfer_reply_releases_both_ends_and_the_retry_moves_under_a_new_key() {
    for lost in [
        ObjectTransferDirection::Inbound,
        ObjectTransferDirection::Outbound,
    ] {
        let world = World::new().await;
        let a = world.add_task("r", "a", "a.txt", "a\n").await;
        world.transfer.remote.lock().unwrap().insert("a".into());
        world.external_commit("r", "outside").await;
        let mut pump = Pump::new(world.worker());
        if lost == ObjectTransferDirection::Inbound {
            world.transfer.lose_reply.store(true, Ordering::SeqCst);
        } else {
            // Run up to the permit, then lose the outbound reply.
            for _ in 0..400 {
                if world.attempt(&a.id).await.state == S::ReadyFf {
                    break;
                }
                let mut drivers = pump.worker.sweep_once().await.unwrap();
                pump.drivers.append(&mut drivers);
                for driver in pump.drivers.iter_mut() {
                    if world.attempt(&a.id).await.state == S::ReadyFf {
                        break;
                    }
                    let _ = driver.pass().await.unwrap();
                }
                world.steps.answer(&world.db).await;
            }
            assert_eq!(world.attempt(&a.id).await.state, S::ReadyFf);
            world.transfer.lose_reply.store(true, Ordering::SeqCst);
        }
        world.settle(&mut pump).await;
        let parked = world.attempt(&a.id).await;
        assert_eq!(parked.state, S::Parked, "{lost:?}");
        assert_eq!(
            parked.failure_kind,
            Some(IntegrationFailureKind::Infrastructure)
        );
        assert!(parked.available_at.is_some());
        assert_eq!(ff_successes(&world, &a.id).await, 0);
        let released = world.transfer.released.lock().unwrap().len();
        assert_eq!(released, 1, "{lost:?}: both ends released with the park");
        let lost_key = world.transfer.keys.lock().unwrap().last().cloned().unwrap();

        world.clock.advance(601);
        world.settle(&mut pump).await;
        let merged = world.attempt(&a.id).await;
        assert_eq!(merged.state, S::Completed, "{lost:?}");
        assert_eq!(ff_successes(&world, &a.id).await, 1);
        assert_eq!(Some(world.tip("r").await), merged.candidate_sha);
        let rebases = receipts_of(&world, &a.id)
            .await
            .into_iter()
            .filter(|receipt| {
                receipt.request.kind == IntegrationOperationKind::Rebase
                    && receipt.operation_state == db::IntegrationOperationState::Succeeded
            })
            .count();
        assert_eq!(rebases, 1, "{lost:?}: the Task was rebased once");
        let keys = world.transfer.keys.lock().unwrap().clone();
        let after: Vec<&String> = keys
            .iter()
            .skip_while(|key| **key != lost_key)
            .skip(1)
            .collect();
        assert!(!after.is_empty(), "{lost:?}: the retry transferred again");
        assert!(
            after.iter().all(|key| **key != lost_key),
            "{lost:?}: the retry used its own generation's key: {keys:?}"
        );
        assert_eq!(world.transfer.released.lock().unwrap().len(), 2);
    }
}

/// A rebase that cannot even be asked for (no intent was written, nothing
/// ran) does not keep the head in `rebasing`: the head parks with a typed,
/// counted retry, so the queue is not held by it, and it merges when the
/// owner can be reached again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rebase_that_cannot_be_asked_for_parks_the_head_instead_of_holding_the_queue() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    world.external_commit("r", "outside").await;
    let owner = Arc::new(UnreachableOwner {
        inner: ServerIntegrationOwner::new(world.db.clone()),
        down: AtomicBool::new(true),
    });
    let mut pump = Pump::new(Arc::new(IntegrationQueueWorker::new(
        world.db.clone(),
        owner.clone(),
        world.steps.clone(),
        world.facts.clone(),
        world.transfer.clone(),
        world.clock.clone(),
        config(),
    )));
    world.settle(&mut pump).await;
    let parked = world.attempt(&a.id).await;
    assert_eq!(parked.state, S::Parked);
    assert_eq!(
        parked.failure_kind,
        Some(IntegrationFailureKind::Infrastructure)
    );
    assert!(
        parked
            .failure_message
            .as_deref()
            .is_some_and(|message| message.contains("daemon is not connected")),
        "{:?}",
        parked.failure_message
    );
    assert!(parked.available_at.is_some(), "retried by the sweep");
    assert!(parked.effect_intent_json.is_none());
    assert!(receipts_of(&world, &a.id).await.is_empty());
    assert!(world.queue_of(&a).await.head_attempt_id.is_none());
    assert!(world
        .steps
        .actions("a")
        .contains(&IntegrationStepAction::Park));

    owner.down.store(false, Ordering::SeqCst);
    world.clock.advance(601);
    world.settle(&mut pump).await;
    let merged = world.attempt(&a.id).await;
    assert_eq!(merged.state, S::Completed);
    assert_eq!(ff_successes(&world, &a.id).await, 1);
    assert_eq!(Some(world.tip("r").await), merged.candidate_sha);
}
