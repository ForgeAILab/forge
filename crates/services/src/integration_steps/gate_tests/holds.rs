//! Task holds, project pauses and merge-gate release.
use super::*;

/// `settle`'s own gate read. The head driver read the Task gate, found it
/// open and asked for the decision; the owner's Hold lands before the step
/// runs. The step is held here by the test (nothing runs it until it is
/// asked), which is the seam between the driver's read and the step. The
/// step reads the gate itself: no permit is written, no protected `result`
/// step exists, the answer names the gate, and the Task did not move.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hold_between_the_drivers_gate_read_and_settle_writes_no_permit() {
    let world = World::new().await;
    let a = world.reconciled("t", "t.txt").await;
    // The driver's side: gate read open, the head waits for the decision.
    assert_eq!(
        crate::integration_steps::gate::task_gate(&world.db, "t")
            .await
            .unwrap(),
        None
    );
    let asked = world.place(&a.id, "awaiting_task_step", 1).await;
    // The Hold lands now.
    let held = world.act("t", TaskAction::Hold { reason: None }).await;
    assert_eq!(held.status, "merging");
    world.ask(&asked, IntegrationStepAction::Settle).await;
    settled(&world, "t", IntegrationStepAction::Settle, "done").await;

    let after = world.attempt(&a.id).await;
    assert!(after.permit_json.is_none(), "no permit under a Hold");
    let answer = ack(&after, IntegrationStepAction::Settle).expect("settle answered");
    assert_eq!(answer.outcome, IntegrationStepOutcome::Infrastructure);
    assert!(
        answer
            .message
            .as_deref()
            .is_some_and(crate::integration_worker::GateCause::parked_under),
        "the answer names the gate: {:?}",
        answer.message
    );
    assert!(world
        .step("t", IntegrationStepAction::Result)
        .await
        .is_empty());
    assert_eq!(world.task("t").await.status, "merging");
    assert_eq!(world.merges("t").await, 0);
}

/// An attempt was sent back (`ejected`, `needs_review`) but its `send_back`
/// step never moved the Task: the Task is still in the `merging` entry the
/// attempt was admitted in, with an attempt nothing drives. The reconciler
/// gives the Task a successor (same place in line), which merges; the
/// sent-back row is history. Nothing is merged twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_sent_back_attempt_whose_task_stayed_in_the_entry_gets_a_successor() {
    for state in ["ejected", "needs_review"] {
        let world = World::new().await;
        let attempt = world.reconciled("t", "t.txt").await;
        world
            .set(
                &attempt.id,
                &format!("state='{state}',failure_kind='infrastructure',failure_message='task_step_failed: send_back'"),
            )
            .await;
        assert_eq!(world.task("t").await.status, "merging");
        let production = Production::new(&world);
        let running = production.run();
        // The worker alone does not take it up: the row left the queue.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(world.task("t").await.status, "merging", "{state}");
        assert_eq!(world.merges("t").await, 0, "{state}");
        let dispatcher = dispatcher(&world).await;
        world
            .until("t", &format!("{state}: the successor merges"), || async {
                dispatcher.reconcile_all().await.unwrap();
                world.task("t").await.status == "done"
            })
            .await;
        let _ = production.stop.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
        assert_eq!(world.merges("t").await, 1, "{state}");
        let old = world.attempt(&attempt.id).await;
        assert_eq!(old.state, S::Superseded, "{state}");
        let successor = world
            .db
            .current_integration_attempt("t")
            .await
            .unwrap()
            .map(|current| (current.id != attempt.id, current.queue_seq == old.queue_seq));
        // The successor is the Task's record now, in the same place in line.
        if let Some((different, same_seq)) = successor {
            assert!(different && same_seq, "{state}");
        }
    }
}

/// Hold while queued, then Release. The held member is skipped (it leaves
/// the slot at once, with no Git effect, no check and no counted retry), the
/// member behind it merges, and the Release alone puts it back: it merges
/// with one rebase.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_task_held_while_queued_is_skipped_and_merges_after_its_release() {
    let world = World::new().await;
    let held = world.reconciled("held", "held.txt").await;
    let other = world.reconciled("other", "other.txt").await;
    assert!(
        world
            .offers("held")
            .await
            .contains(&("hold".into(), "integration_wait".into())),
        "{:?}",
        world.offers("held").await
    );
    let task = world.act("held", TaskAction::Hold { reason: None }).await;
    assert_eq!(task.status, "merging");
    assert_eq!(
        crate::integration_steps::gate::task_row_gate(&world.task("held").await),
        Some(crate::integration_worker::GateCause::Held),
        "the Hold is recorded on the Task"
    );
    let production = Production::new(&world);
    let running = production.run();
    eventually("the Task behind the held one merges", || async {
        world.task("other").await.status == "done"
    })
    .await;
    eventually("the held Task left the slot", || async {
        world.attempt(&held.id).await.state == S::Parked
    })
    .await;
    let parked = world.attempt(&held.id).await;
    assert_eq!(gate_code(&parked), "task_held", "{parked:?}");
    println!(
        "held: integration={} offers={:?}",
        integration(&world.task("held").await),
        world.offers("held").await
    );
    assert!(parked.available_at.is_some(), "the sweep looks again");
    assert_eq!(world.rebases("held").await, 0);
    assert!(world
        .step("held", IntegrationStepAction::RequestCheck)
        .await
        .is_empty());
    assert!(world
        .step("held", IntegrationStepAction::Settle)
        .await
        .is_empty());
    assert!(!world.on_main("held.txt").await);
    let journal = parked.operation_receipts_json.as_array().unwrap().clone();
    assert!(
        journal.iter().all(|entry| entry["counted"] != true),
        "no retry is spent on a gate: {journal:?}"
    );
    // Held for many sweeps: still one journal entry, no round counted.
    tokio::time::sleep(Duration::from_millis(600)).await;
    let later = world.attempt(&held.id).await;
    assert_eq!(
        later.operation_receipts_json.as_array().map_or(0, Vec::len),
        1,
        "{}",
        world.dump("held").await
    );
    assert_eq!(later.phase_timings.map_or(0, |timings| timings.rounds), 0);
    assert_eq!(world.task("held").await.status, "merging");
    // The owner's Release is the only action needed.
    let offers = world.offers("held").await;
    assert!(
        offers.iter().any(|(verb, _)| verb == "release"),
        "{offers:?}"
    );
    world
        .act("held", TaskAction::Release { reason: None })
        .await;
    world
        .until("held", "the released Task merges", || async {
            world.task("held").await.status == "done"
                && world.attempt(&held.id).await.state == S::Completed
                && world.attempt(&other.id).await.state == S::Completed
        })
        .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    assert!(world.on_main("held.txt").await && world.on_main("other.txt").await);
    assert_eq!(
        world.rebases("held").await,
        1,
        "rebased once, after release"
    );
    assert_eq!(world.merges("held").await, 1);
    assert_eq!(world.attempt(&held.id).await.state, S::Completed);
    assert_eq!(world.attempt(&other.id).await.state, S::Completed);
}

/// Hold while the Task is the head, before its rebase: the rebase is never
/// asked for. After the Release it is rebased once, checked once and merged.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_head_held_before_its_rebase_runs_no_git_effect_and_merges_after_release() {
    let world = World::new().await;
    let attempt = world.reconciled("t", "t.txt").await;
    world.move_target("moved").await;
    let head = git::get_current_sha(&world.tree("t")).await.unwrap();
    let production = Production::new(&world);
    let mut driver = claim(&production).await;
    assert_eq!(
        step_to(&world, &mut driver, &attempt.id, S::Rebasing).await,
        S::Rebasing
    );
    world.act("t", TaskAction::Hold { reason: None }).await;
    assert_eq!(
        step_to(&world, &mut driver, &attempt.id, S::Checking).await,
        S::Parked,
        "the head leaves the slot instead of rebasing"
    );
    let parked = world.attempt(&attempt.id).await;
    assert_eq!(gate_code(&parked), "task_held");
    assert_eq!(world.rebases("t").await, 0);
    assert_eq!(git::get_current_sha(&world.tree("t")).await.unwrap(), head);
    assert!(world
        .step("t", IntegrationStepAction::RequestCheck)
        .await
        .is_empty());
    world.act("t", TaskAction::Release { reason: None }).await;
    merged(&world, "t", &production).await;
    assert_eq!(world.rebases("t").await, 1);
    assert_eq!(world.merges("t").await, 1);
    assert!(world.on_main("t.txt").await);
}

/// Hold while the head's check runs. The head leaves the slot; the verdict
/// of the running check arrives for a parked attempt and is applied to
/// nothing (no permit, no merge, no Review of a held Task's merge). After
/// the Release the rebase is not repeated; the check is asked again and the
/// Task merges.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_head_held_during_its_check_discards_the_verdict_and_does_not_rebase_twice() {
    let world = World::new().await;
    let attempt = world.reconciled("t", "t.txt").await;
    review_ci(&world, "t", &["sleep 1 && test -f t.txt"]).await;
    world.move_target("moved").await;
    let production = Production::new(&world);
    let mut driver = claim(&production).await;
    assert_eq!(
        step_to(&world, &mut driver, &attempt.id, S::Checking).await,
        S::Checking
    );
    assert_eq!(world.rebases("t").await, 1);
    let rebased = git::get_current_sha(&world.tree("t")).await.unwrap();
    world.act("t", TaskAction::Hold { reason: None }).await;
    assert_eq!(
        step_to(&world, &mut driver, &attempt.id, S::AwaitingTaskStep).await,
        S::Parked
    );
    let tip = world.main_tip().await;
    // The check that was running ends; its verdict finds nothing to apply to.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let parked = world.attempt(&attempt.id).await;
    assert_eq!(parked.state, S::Parked);
    assert_eq!(gate_code(&parked), "task_held");
    assert!(parked.permit_json.is_none());
    assert_eq!(world.main_tip().await, tip);
    assert_eq!(world.task("t").await.status, "merging");
    assert!(world
        .step("t", IntegrationStepAction::Result)
        .await
        .is_empty());
    let asked = world
        .step("t", IntegrationStepAction::RequestCheck)
        .await
        .len();
    world.act("t", TaskAction::Release { reason: None }).await;
    merged(&world, "t", &production).await;
    assert_eq!(world.rebases("t").await, 1, "the rebase is not repeated");
    assert_eq!(
        git::get_current_sha(&world.tree("t")).await.unwrap(),
        rebased
    );
    assert!(
        world
            .step("t", IntegrationStepAction::RequestCheck)
            .await
            .len()
            > asked,
        "the check is asked again after the gate lifted"
    );
    assert_eq!(world.merges("t").await, 1);
    assert_eq!(world.main_tip().await, rebased);
}

/// Hold while the Task is the head on an unchanged target, before the
/// permit: no `settle` step is asked for and nothing lands. After the
/// Release the reviewed commit merges as it is, with no rebase and no check.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_head_held_before_the_permit_gets_none_and_merges_as_reviewed_after_release() {
    let world = World::new().await;
    let attempt = world.reconciled("t", "t.txt").await;
    let reviewed = git::get_current_sha(&world.tree("t")).await.unwrap();
    let production = Production::new(&world);
    let mut driver = claim(&production).await;
    assert_eq!(
        step_to(&world, &mut driver, &attempt.id, S::Validating).await,
        S::Validating
    );
    world.act("t", TaskAction::Hold { reason: None }).await;
    assert_eq!(
        step_to(&world, &mut driver, &attempt.id, S::ReadyFf).await,
        S::Parked
    );
    assert!(world
        .step("t", IntegrationStepAction::Settle)
        .await
        .is_empty());
    assert!(world.attempt(&attempt.id).await.permit_json.is_none());
    assert!(!world.on_main("t.txt").await);
    world.act("t", TaskAction::Release { reason: None }).await;
    merged(&world, "t", &production).await;
    assert_eq!(world.rebases("t").await, 0);
    assert!(world
        .step("t", IntegrationStepAction::RequestCheck)
        .await
        .is_empty());
    assert_eq!(world.main_tip().await, reviewed);
    assert_eq!(world.merges("t").await, 1);
}

/// Each gate that is not a Hold, set and lifted by a plain row write with
/// nothing notifying the worker: the head leaves the slot under the typed
/// cause, and the worker's sweep alone re-queues the Task when the gate has
/// lifted. One merge, no check, no counted retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn every_gate_parks_typed_and_the_sweep_alone_requeues_when_it_lifts() {
    let cases: [(&str, &str, &str); 5] = [
        (
            "task_blocked",
            "UPDATE task SET blocked_json='{\"reason\":\"stopped by the owner\"}' WHERE id='t'",
            "UPDATE task SET blocked_json=NULL WHERE id='t'",
        ),
        (
            "task_failed",
            "UPDATE task SET failed_json='{\"reason\":\"failed\"}' WHERE id='t'",
            "UPDATE task SET failed_json=NULL WHERE id='t'",
        ),
        (
            "task_entry_blocked",
            "UPDATE task SET entry_barrier_json='{\"state\":\"merging\",\"status\":\"blocked\"}' WHERE id='t'",
            "UPDATE task SET entry_barrier_json=NULL WHERE id='t'",
        ),
        (
            "task_awaiting_human",
            "UPDATE task SET metadata_json='{\"awaiting_human\":true}' WHERE id='t'",
            "UPDATE task SET metadata_json=NULL WHERE id='t'",
        ),
        (
            "project_paused",
            "UPDATE project SET paused_at='2026-10-10T00:00:00Z' WHERE id='p'",
            "UPDATE project SET paused_at=NULL WHERE id='p'",
        ),
    ];
    for (code, set, lift) in cases {
        let world = World::new().await;
        let attempt = world.reconciled("t", "t.txt").await;
        sqlx::query(set).execute(world.db.pool()).await.unwrap();
        let tip = world.main_tip().await;
        let production = Production::new(&world);
        let running = production.run();
        eventually(&format!("{code}: the head leaves the slot"), || async {
            world.attempt(&attempt.id).await.state == S::Parked
        })
        .await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        let parked = world.attempt(&attempt.id).await;
        assert_eq!(gate_code(&parked), code, "{parked:?}");
        println!(
            "{code}: integration={} offers={:?}",
            integration(&world.task("t").await),
            world.offers("t").await
        );
        assert!(parked.permit_json.is_none(), "{code}");
        assert_eq!(world.main_tip().await, tip, "{code}: nothing landed");
        assert_eq!(world.task("t").await.status, "merging", "{code}");
        assert!(
            world
                .step("t", IntegrationStepAction::Settle)
                .await
                .is_empty(),
            "{code}: no decision is asked for under a gate"
        );
        let journal = parked.operation_receipts_json.as_array().unwrap().clone();
        assert!(
            journal.iter().all(|entry| entry["counted"] != true),
            "{code}: {journal:?}"
        );
        sqlx::query(lift).execute(world.db.pool()).await.unwrap();
        world
            .until(
                "t",
                &format!("{code}: the Task merges after the gate"),
                || async { world.task("t").await.status == "done" },
            )
            .await;
        let _ = production.stop.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
        assert_eq!(world.merges("t").await, 1, "{code}");
        assert_eq!(world.rebases("t").await, 0, "{code}");
        assert!(world.on_main("t.txt").await, "{code}");
    }
}

/// A paused Agent does not gate a merge: the Task its paused Agent is assigned
/// to is rebased, checked and fast-forwarded like any other (machine work, no
/// Agent turn), the attempt is never parked for it, and it merges once.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_task_whose_agent_is_paused_still_merges() {
    let world = World::new().await;
    let attempt = world.reconciled("t", "t.txt").await;
    world.legacy_task("agent-home", "home.txt", "done").await;
    let agent = world.seed_reviewer("agent-home").await;
    sqlx::query("UPDATE task SET assignee_type='agent',assignee_id=? WHERE id='t'")
        .bind(&agent)
        .execute(world.db.pool())
        .await
        .unwrap();
    let paused = sqlx::query("UPDATE agent_identity SET paused=1 WHERE id=?")
        .bind(&agent)
        .execute(world.db.pool())
        .await
        .unwrap();
    assert_eq!(paused.rows_affected(), 1, "the assigned Agent is paused");
    assert_eq!(
        crate::integration_steps::gate::task_gate(&world.db, "t")
            .await
            .unwrap(),
        None,
        "a paused Agent is no gate"
    );
    // The target moved: the queue has to rebase and check, all under the pause.
    world.move_target("other.txt").await;
    let production = Production::new(&world);
    let running = production.run();
    world
        .until("t", "the Task merges under its paused Agent", || async {
            world.task("t").await.status == "done"
        })
        .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    let journal: String = world
        .scalar(
            "SELECT operation_receipts_json FROM integration_attempt WHERE id=?",
            &attempt.id,
        )
        .await;
    assert!(!journal.contains("agent_paused"), "{journal}");
    assert_eq!(world.merges("t").await, 1);
    assert_eq!(world.rebases("t").await, 1);
    assert!(world.on_main("t.txt").await);
}

/// Project pause and resume through the service call the resume route
/// makes: the waiting attempt is due at once, not at its next look.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_project_resume_puts_its_waiting_merges_back_in_line_at_once() {
    let world = World::new().await;
    let attempt = world.reconciled("t", "t.txt").await;
    db::ProjectRepo::set_paused_at(&*world.db, "p", Some(db::now_rfc3339()))
        .await
        .unwrap();
    let production = Production::new(&world);
    let mut driver = claim(&production).await;
    assert_eq!(
        step_to(&world, &mut driver, &attempt.id, S::Validating).await,
        S::Parked
    );
    let parked = world.attempt(&attempt.id).await;
    assert_eq!(gate_code(&parked), "project_paused");
    // Far in the future: only the resume can make it due.
    world
        .set(&attempt.id, "available_at='2099-01-01T00:00:00Z'")
        .await;
    db::ProjectRepo::set_paused_at(&*world.db, "p", None)
        .await
        .unwrap();
    world
        .service
        .integration_project_resumed("p")
        .await
        .unwrap();
    merged(&world, "t", &production).await;
    assert_eq!(world.merges("t").await, 1);
}

/// Manual approve authority in the merge state. The owner approves a Task
/// that is held in `merging`: where the Task offers `approve`, performing it
/// declares the Task done with nothing merged (the owner's override, as
/// before the queue), and the Task's attempt is settled: it ends terminal
/// and is not left parked or current behind a done Task. Where `approve` is
/// not offered the Task stays put with its attempt parked under the Hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn approving_a_held_merging_task_settles_its_attempt() {
    let world = World::new().await;
    let attempt = world.reconciled("t", "t.txt").await;
    world.act("t", TaskAction::Hold { reason: None }).await;
    let production = Production::new(&world);
    let running = production.run();
    eventually("the held Task left the slot", || async {
        world.attempt(&attempt.id).await.state == S::Parked
    })
    .await;
    let offers = world.offers("t").await;
    println!("held merging Task offers: {offers:?}");
    if offers.iter().any(|(verb, _)| verb == "approve") {
        let approved = world
            .act(
                "t",
                TaskAction::Approve {
                    reason: Some("landed by hand".into()),
                    override_checks: None,
                },
            )
            .await;
        assert_eq!(approved.status, "done", "the owner's override");
        assert_eq!(world.merges("t").await, 0, "nothing was merged");
        assert!(!world.on_main("t.txt").await);
        world
            .until("t", "the attempt is settled", || async {
                let settled = world.attempt(&attempt.id).await;
                settled.state.terminal() && !settled.current
            })
            .await;
        assert_eq!(world.attempt(&attempt.id).await.state, S::Cancelled);
    } else {
        assert_eq!(world.task("t").await.status, "merging");
        assert_eq!(world.attempt(&attempt.id).await.state, S::Parked);
    }
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
}
