//! Integration settle permits, merge results and target-owner proof.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settle_on_an_unchanged_target_writes_a_bound_permit_and_protects_the_result() {
    let world = World::new().await;
    let a = world.add_task("t", "f", "one\n").await;
    // The step is enqueued before the attempt transition it belongs to.
    let asked = world.place(&a.id, "validating", 1).await;
    world.ask(&asked, IntegrationStepAction::Settle).await;
    eventually("the settle step retried", || async {
        world.step("t", IntegrationStepAction::Settle).await[0].attempts >= 2
    })
    .await;
    let waiting = world.attempt(&a.id).await;
    assert!(waiting.acknowledged_at.is_none() && waiting.permit_json.is_none());
    assert_eq!(waiting.revision, asked.revision, "a retry writes nothing");
    world.set(&a.id, "state='awaiting_task_step'").await;
    settled(&world, "t", IntegrationStepAction::Settle, "done").await;

    let a = world.attempt(&a.id).await;
    let answer = ack(&a, IntegrationStepAction::Settle).expect("settle answered");
    assert_eq!(answer.outcome, IntegrationStepOutcome::Permit);
    assert_eq!(
        (answer.effect_seq, answer.generation),
        (1, a.slot_generation)
    );
    let permit = a.permit_json.clone().unwrap();
    assert_eq!(permit["candidate_sha"], json!(a.candidate_sha));
    assert_eq!(permit["target_tip_sha"], json!(a.target_tip_sha));
    assert_eq!(permit["task_ref"], "t");
    assert_eq!(permit["expected_epoch"], a.expected_epoch);
    assert_eq!(permit["slot_generation"], a.slot_generation);
    // Storage accepts exactly this permit for `ready_ff`.
    let mut ready = a.clone();
    ready.state = S::ReadyFf;
    let ready = world
        .db
        .transition_integration_attempt(ready)
        .await
        .unwrap();

    // The protected result step exists from the permit's own commit.
    let result = world.step("t", IntegrationStepAction::Result).await;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].status, "pending");
    assert!(!result[0].entry_fenced);
    assert!(result[0].available_at > db::now_rfc3339());
    let protected: bool = world
        .scalar(
            "SELECT integration_started_at IS NOT NULL FROM task_step WHERE id=?",
            &result[0].id,
        )
        .await;
    assert!(protected);
    assert_eq!(
        integration(&world.task("t").await),
        json!({"kind":"owned","attempt_id":a.id,"phase":"fast_forwarding"})
    );
    // The Task itself did not move.
    assert_eq!(world.task("t").await.status, "merging");

    // Duplicate delivery of the same request: no second step, one answer.
    world.ask(&ready, IntegrationStepAction::Settle).await;
    assert_eq!(
        world.step("t", IntegrationStepAction::Settle).await.len(),
        1
    );
    assert_eq!(world.attempt(&a.id).await.revision, ready.revision);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_step_for_a_past_effect_seq_or_generation_finishes_without_a_write() {
    let world = World::new().await;
    let a = world.add_task("t", "f", "one\n").await;
    let asked = world.place(&a.id, "awaiting_task_step", 4).await;
    // The worker moved on (a takeover asks again under a new effect_seq).
    let mut old_seq = IntegrationQueueWorker::step_request(&asked, IntegrationStepAction::Settle);
    old_seq.effect_seq = 3;
    world.port.enqueue_step(&old_seq).await.unwrap();
    settled(&world, "t", IntegrationStepAction::Settle, "done").await;
    let mut old_generation =
        IntegrationQueueWorker::step_request(&asked, IntegrationStepAction::SendBack);
    old_generation.generation = 0;
    world.set(&a.id, "slot_generation=2").await;
    let mut settle = old_generation.clone();
    settle.action = IntegrationStepAction::Settle;
    settle.effect_seq = 4;
    world.port.enqueue_step(&settle).await.unwrap();
    eventually("both stale settles finished", || async {
        world
            .step("t", IntegrationStepAction::Settle)
            .await
            .iter()
            .filter(|step| step.status == "done")
            .count()
            == 2
    })
    .await;
    let after = world.attempt(&a.id).await;
    assert!(after.acknowledged_at.is_none() && after.permit_json.is_none());
    assert!(world
        .step("t", IntegrationStepAction::Result)
        .await
        .is_empty());
    assert!(world
        .task("t")
        .await
        .condition
        .integration_reason()
        .is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn result_applies_a_landed_merge_exactly_as_the_merge_hook_does() {
    let world = World::new().await;
    let a = world.add_task("t", "f", "one\n").await;
    let mut events = world.bus.subscribe();
    let applied = land(&world, "t", &a.id).await;
    // The worker asks for the result it already has a protected step for.
    world.ask(&applied, IntegrationStepAction::Result).await;
    assert_eq!(
        world.step("t", IntegrationStepAction::Result).await.len(),
        1
    );
    world.port.ready_result_step(&a.id, 1).await.unwrap();
    eventually("the Task is done", || async {
        world.task("t").await.status == "done"
    })
    .await;

    let sha = applied.candidate_sha.clone().unwrap();
    // Today's merge success: this comment, then `merging -> done` by the
    // workflow with this reason and no bridge.
    assert_eq!(
        world.comments("t").await,
        vec![format!("Changes merged to main (SHA: {sha})")]
    );
    assert_eq!(
        world.transitions("t").await,
        vec![(
            "merging".to_owned(),
            "done".to_owned(),
            "merge succeeded".to_owned(),
            None,
            "system:workflow".to_owned(),
            0
        )]
    );
    let a = world.attempt(&a.id).await;
    let answer = ack(&a, IntegrationStepAction::Result).expect("result answered");
    assert_eq!(answer.outcome, IntegrationStepOutcome::Done);
    let result = &world.step("t", IntegrationStepAction::Result).await[0];
    assert_eq!(result.status, "done");
    // The landed result outranks a waiting owner command, as the merge
    // hook's own follow-up does.
    let priority: i64 = world
        .scalar(
            "SELECT priority FROM task_step WHERE causation_step_id=? AND kind='cascade'",
            &result.id,
        )
        .await;
    assert_eq!(priority, 2);
    let mut seen = Vec::new();
    while let Ok(event) = events.try_recv() {
        seen.push(event.event_type);
    }
    assert!(seen.contains(&"comment.created".to_owned()), "{seen:?}");
    // A redelivered request changes nothing.
    world.ask(&a, IntegrationStepAction::Result).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(world.comments("t").await.len(), 1);
    assert_eq!(world.attempt(&a.id).await.revision, a.revision);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_result_that_cannot_apply_never_dead_letters_and_is_rearmed() {
    // The fault is keyed by Task id in a process-wide set: an id no other
    // test uses (it made the plain `result` test time out under `"t"`).
    let world = World::new().await;
    let a = world.add_task("unapplied", "f", "one\n").await;
    test_faults::set("unapplied", true);
    land(&world, "unapplied", &a.id).await;
    world.port.ready_result_step(&a.id, 1).await.unwrap();
    // More deliveries than any step's retry allowance.
    for round in 1..=10 {
        eventually("the result step ran again", || async {
            world.step("unapplied", IntegrationStepAction::Result).await[0].attempts >= round
        })
        .await;
        let step = &world.step("unapplied", IntegrationStepAction::Result).await[0];
        assert_ne!(step.status, "failed");
        assert_ne!(step.status, "parked");
        // Parked far out; `ready_result_step` is what re-arms it.
        eventually("the result step is parked for later", || async {
            let step = &world.step("unapplied", IntegrationStepAction::Result).await[0];
            step.status == "pending" && step.available_at > db::now_rfc3339()
        })
        .await;
        world.port.ready_result_step(&a.id, 1).await.unwrap();
    }
    let task = world.task("unapplied").await;
    assert_eq!(task.status, "merging");
    assert_eq!(integration(&task)["kind"], "deferred");
    assert_eq!(integration(&task)["cause"], "unresolved_result");
    assert!(task.error_annotation.is_none(), "no failure annotation");
    assert!(
        world.attempt(&a.id).await.acknowledged_at.is_some(),
        "settle ack stays"
    );
    assert!(ack(&world.attempt(&a.id).await, IntegrationStepAction::Result).is_none());

    test_faults::set("unapplied", false);
    world.port.ready_result_step(&a.id, 1).await.unwrap();
    eventually("the Task is done", || async {
        world.task("unapplied").await.status == "done"
    })
    .await;
    assert!(ack(&world.attempt(&a.id).await, IntegrationStepAction::Result).is_some());
}

/// Design rule: a Task is never reported merged unless the target contains
/// its commit. The attempt's `integrated_sha` is only a claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn result_reports_done_only_when_the_target_owner_proves_the_commit() {
    let world = World::new().await;
    let main = |world: &World| {
        let repo = world.repo();
        async move {
            git_out(&repo, &["rev-parse", "refs/heads/main"])
                .await
                .unwrap()
        }
    };
    let base = main(&world).await;

    // 1. Forged: the row says `applied` with the candidate as the merged
    //    commit, and nothing ever moved the target.
    let a = world.add_task("forged", "f1", "one\n").await;
    let a = world.place(&a.id, "applied", 1).await;
    world.set(&a.id, "integrated_sha=candidate_sha").await;
    let a = world.attempt(&a.id).await;
    assert!(deliver_result(&world, &a).await, "answered, not reported");
    assert_eq!(reads(a.candidate_sha.as_deref().unwrap()), 1);
    unresolved(&world, "forged").await;
    assert!(
        world.comments("forged").await.is_empty(),
        "no merged comment"
    );
    let answer = ack(&world.attempt(&a.id).await, IntegrationStepAction::Result).unwrap();
    assert_eq!(answer.outcome, IntegrationStepOutcome::NotLanded);

    // 2. Mismatched: the row names a commit the target does contain (its
    //    old tip), which is not the Task's commit.
    let b = world.add_task("mismatched", "f2", "two\n").await;
    let b = world.place(&b.id, "applied", 1).await;
    world.set(&b.id, &format!("integrated_sha='{base}'")).await;
    let b = world.attempt(&b.id).await;
    let asked_before = reads(&base);
    assert!(deliver_result(&world, &b).await);
    // The merged commit, then the Task's own commit.
    assert_eq!(reads(&base), asked_before + 1);
    assert_eq!(reads(b.candidate_sha.as_deref().unwrap()), 1);
    unresolved(&world, "mismatched").await;

    // 3. A fast-forward receipt of another attempt, of a later claim
    //    generation, or of another owner refuses the report with no Git
    //    read at all, although the target contains the commit.
    let c = world.add_task("receipt", "f3", "three\n").await;
    let c = world.place(&c.id, "applied", 1).await;
    world.apply(&c.id).await;
    let c = world.attempt(&c.id).await;
    let sha = c.candidate_sha.clone().unwrap();
    let queue = world
        .db
        .integration_queue(c.queue_id.as_deref().unwrap())
        .await
        .unwrap()
        .unwrap();
    let owner = json!({"location_id":"l-r","owner_kind":"server","daemon_id":null,"runtime_id":null,"generation":1});
    sqlx::query(
        "UPDATE integration_queue SET target_location_id='l-r',target_owner_json=? WHERE id=?",
    )
    .bind(owner.to_string())
    .bind(&queue.id)
    .execute(world.db.pool())
    .await
    .unwrap();
    let receipt = |attempt_id: &str, generation: i64, owner: Value, after: &str| {
        json!([{
            "request": {
                "fence": {"queue_id": queue.id, "attempt_id": attempt_id, "generation": generation, "lease_owner": "w", "target_owner": owner},
                "kind": "fast_forward",
                "witness": {}
            },
            "result": {"kind": "completed", "outcome": {"Done": {"before_sha": base, "after_sha": after, "branch": "main"}}},
            "operation_state": "succeeded",
            "recorded_at": db::now_rfc3339()
        }])
        .to_string()
    };
    let other_owner = json!({"location_id":"l-r","owner_kind":"daemon","daemon_id":"d-other","runtime_id":"r","generation":1});
    for (seq, (key, forged)) in [
        (
            "other-attempt",
            receipt("someone-else", 1, owner.clone(), &sha),
        ),
        ("later-generation", receipt(&c.id, 7, owner.clone(), &sha)),
        ("other-owner", receipt(&c.id, 1, other_owner, &sha)),
        ("other-commit", receipt(&c.id, 1, owner.clone(), &base)),
    ]
    .into_iter()
    .enumerate()
    {
        sqlx::query("UPDATE integration_attempt SET effect_receipts_json=?,effect_seq=?,effect_ack_json=NULL,acknowledged_at=NULL,revision=revision+1 WHERE id=?")
            .bind(forged)
            .bind(seq as i64 + 2)
            .bind(&c.id)
            .execute(world.db.pool())
            .await
            .unwrap();
        let c = world.attempt(&c.id).await;
        assert!(deliver_result(&world, &c).await, "{key}");
        assert_eq!(reads(&sha), 0, "{key}: refused before Git");
        unresolved(&world, "receipt").await;
        assert_eq!(
            ack(&world.attempt(&c.id).await, IntegrationStepAction::Result)
                .unwrap()
                .outcome,
            IntegrationStepOutcome::NotLanded,
            "{key}"
        );
    }
    // Its own receipt, from the target's owner: one read, reported.
    sqlx::query("UPDATE integration_attempt SET effect_receipts_json=?,effect_seq=9,effect_ack_json=NULL,acknowledged_at=NULL,revision=revision+1 WHERE id=?")
        .bind(receipt(&c.id, 1, owner.clone(), &sha))
        .bind(&c.id)
        .execute(world.db.pool())
        .await
        .unwrap();
    let c = world.attempt(&c.id).await;
    assert!(deliver_result(&world, &c).await);
    assert_eq!(reads(&sha), 1, "the normal path costs one ancestry read");
    assert_eq!(
        ack(&world.attempt(&c.id).await, IntegrationStepAction::Result)
            .unwrap()
            .outcome,
        IntegrationStepOutcome::Done
    );
    eventually("the proven Task is done", || async {
        world.task("receipt").await.status == "done"
    })
    .await;

    // 4. Rewound: the fast-forward landed and the target was then moved
    //    back (a force-push).
    let d = world.add_task("rewound", "f4", "four\n").await;
    let d = world.place(&d.id, "applied", 1).await;
    let before = main(&world).await;
    world.apply(&d.id).await;
    git_out(&world.repo(), &["reset", "--hard", &before])
        .await
        .unwrap();
    let d = world.attempt(&d.id).await;
    assert!(deliver_result(&world, &d).await);
    assert_eq!(reads(d.candidate_sha.as_deref().unwrap()), 1);
    unresolved(&world, "rewound").await;
    assert!(world.comments("rewound").await.is_empty());
    assert_eq!(
        ack(&world.attempt(&d.id).await, IntegrationStepAction::Result)
            .unwrap()
            .outcome,
        IntegrationStepOutcome::NotLanded
    );
}

/// An owner that cannot be asked is not a "no": the step stays, the Task
/// says why, and the proof is asked again on the result timer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_target_owner_that_does_not_answer_keeps_the_result_step_asking() {
    let world = World::new().await;
    let a = world.add_task("mute", "f", "one\n").await;
    let a = world.place(&a.id, "applied", 1).await;
    world.apply(&a.id).await;
    // The default checkout cannot be read (its owner is not answering).
    let repo = world.repo();
    let away = world.temp.path().join("repo-away");
    std::fs::rename(&repo, &away).unwrap();
    let a = world.attempt(&a.id).await;
    assert!(
        !deliver_result(&world, &a).await,
        "no answer: the step asks again later"
    );
    unresolved(&world, "mute").await;
    assert!(ack(&world.attempt(&a.id).await, IntegrationStepAction::Result).is_none());
    assert_eq!(world.attempt(&a.id).await.state, S::Applied);
    // The owner answers again.
    std::fs::rename(&away, &repo).unwrap();
    assert!(deliver_result(&world, &a).await);
    eventually("the Task is done", || async {
        world.task("mute").await.status == "done"
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unused_permit_is_taken_back_when_the_protected_step_wakes() {
    let world = World::new().await;
    let a = world.add_task("t", "f", "one\n").await;
    let asked = world.place(&a.id, "awaiting_task_step", 1).await;
    world.ask(&asked, IntegrationStepAction::Settle).await;
    settled(&world, "t", IntegrationStepAction::Settle, "done").await;
    assert!(world.attempt(&a.id).await.permit_json.is_some());
    // No worker used the permit (it died): the step's own deadline passes.
    let result = world.step("t", IntegrationStepAction::Result).await;
    sqlx::query("UPDATE task_step SET available_at=? WHERE id=?")
        .bind(db::now_rfc3339())
        .bind(&result[0].id)
        .execute(world.db.pool())
        .await
        .unwrap();
    settled(&world, "t", IntegrationStepAction::Result, "done").await;
    let a = world.attempt(&a.id).await;
    assert!(a.permit_json.is_none() && a.acknowledged_at.is_none());
    assert_eq!(world.task("t").await.status, "merging");
    assert!(world.comments("t").await.is_empty());
}
