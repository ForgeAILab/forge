//! Integration quarantine recovery, leases, and settled effects.

use super::*;

#[tokio::test]
async fn quarantine_needs_the_lease_holder_and_an_unresolved_head() {
    let db = fixture().await;
    let q = main_queue(&db).await;
    admit(&db, &q, "a").await;
    let quarantine = |q: &IntegrationQueue, revision: i64, owner: &'static str, fence: i64| {
        let (db, id) = (&db, q.id.clone());
        async move {
            db.quarantine_integration_queue(
                &id,
                revision,
                owner,
                fence,
                IntegrationFailureKind::Timeout,
                "merge result unknown",
            )
            .await
        }
    };
    // No head, no lease.
    assert!(matches!(
        quarantine(&q, q.revision + 1, "worker", 0).await,
        Err(DbError::VersionConflict)
    ));
    let q = claim(&db, &q.id, NOW, FAR).await.unwrap();
    // A head with nothing in flight has no unknown result to quarantine.
    assert!(matches!(
        quarantine(&q, q.revision, "worker", q.fence_generation).await,
        Err(DbError::InvalidTransition)
    ));
    sqlx::query("UPDATE integration_attempt SET state='reconciling'")
        .execute(db.pool())
        .await
        .unwrap();
    assert!(matches!(
        quarantine(&q, q.revision, "someone-else", q.fence_generation).await,
        Err(DbError::VersionConflict)
    ));
    assert!(matches!(
        quarantine(&q, q.revision, "worker", q.fence_generation + 1).await,
        Err(DbError::VersionConflict)
    ));
    assert!(matches!(
        db.quarantine_integration_queue(
            &q.id,
            q.revision,
            "worker",
            q.fence_generation,
            IntegrationFailureKind::Timeout,
            &"x".repeat(4097)
        )
        .await,
        Err(DbError::Check(_))
    ));
    let quarantined = quarantine(&q, q.revision, "worker", q.fence_generation)
        .await
        .unwrap();
    assert_eq!(quarantined.state, IntegrationQueueState::Quarantined);
    assert_eq!(quarantined.revision, q.revision + 1);
    assert_eq!(
        quarantined.last_error_kind,
        Some(IntegrationFailureKind::Timeout)
    );
    assert_eq!(
        quarantined.last_error.as_deref(),
        Some("merge result unknown")
    );
    assert_eq!(
        (&quarantined.lease_owner, &quarantined.head_attempt_id),
        (&q.lease_owner, &q.head_attempt_id)
    );
    assert!(matches!(
        quarantine(
            &quarantined,
            quarantined.revision,
            "worker",
            quarantined.fence_generation
        )
        .await,
        Err(DbError::InvalidTransition)
    ));
}

#[tokio::test]
async fn a_quarantined_queue_reopens_only_with_a_settled_receipt_of_its_own() {
    let db = fixture().await;
    let (q, request) = uncertain_head(&db).await;
    let fence = request.fence.clone();
    let q = db
        .quarantine_integration_queue(
            &q.id,
            db.integration_queue(&q.id).await.unwrap().unwrap().revision,
            "worker",
            fence.generation,
            IntegrationFailureKind::Timeout,
            "merge result unknown",
        )
        .await
        .unwrap();
    let witness = IntegrationQueueReopenWitness::SettledEffect {
        attempt_id: fence.attempt_id.clone(),
        generation: fence.generation,
        operation: IntegrationOperationKind::FastForward,
    };
    // The receipt is still uncertain: no witness yet, whatever the caller says.
    assert!(matches!(
        db.reopen_integration_queue(&q.id, q.revision, &witness)
            .await,
        Err(DbError::Check(_))
    ));
    // The owner comes back and reports what happened.
    db.lock_integration_reconciliation(&request)
        .await
        .unwrap()
        .unwrap()
        .record(
            serde_json::json!({"kind":"completed"}),
            IntegrationOperationState::Succeeded,
        )
        .await
        .unwrap();
    assert!(matches!(
        db.reopen_integration_queue(&q.id, q.revision + 1, &witness)
            .await,
        Err(DbError::VersionConflict)
    ));
    for wrong in [
        IntegrationQueueReopenWitness::SettledEffect {
            attempt_id: fence.attempt_id.clone(),
            generation: fence.generation + 1,
            operation: IntegrationOperationKind::FastForward,
        },
        IntegrationQueueReopenWitness::SettledEffect {
            attempt_id: fence.attempt_id.clone(),
            generation: fence.generation,
            operation: IntegrationOperationKind::Rebase,
        },
        IntegrationQueueReopenWitness::SettledEffect {
            attempt_id: "another-attempt".into(),
            generation: fence.generation,
            operation: IntegrationOperationKind::FastForward,
        },
    ] {
        assert!(matches!(
            db.reopen_integration_queue(&q.id, q.revision, &wrong).await,
            Err(DbError::Check(_))
        ));
    }
    // The worker has not applied the receipt to its head yet: the queue stays
    // shut, or a crash here would leave an open queue with an unread result.
    for unresolved in ["reconciling", "ff_inflight"] {
        exec(
            &db,
            &format!("UPDATE integration_attempt SET state='{unresolved}'"),
        )
        .await;
        assert!(matches!(
            db.reopen_integration_queue(&q.id, q.revision, &witness)
                .await,
            Err(DbError::Check(_))
        ));
    }
    exec(&db, "UPDATE integration_attempt SET state='queued'").await;
    // A settled receipt of an earlier member says nothing about this head.
    add_task(&db, "old").await;
    let mut earlier = admit(&db, &q, "old").await;
    earlier.state = IntegrationAttemptState::Cancelled;
    let earlier = db.transition_integration_attempt(earlier).await.unwrap();
    // The admission moved the queue's revision.
    let q = db.integration_queue(&q.id).await.unwrap().unwrap();
    assert_eq!(q.state, IntegrationQueueState::Quarantined);
    let mut old_receipt = serde_json::to_value(
        db.integration_effect_receipt(&request)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    old_receipt["request"]["fence"]["attempt_id"] = earlier.id.clone().into();
    sqlx::query("UPDATE integration_attempt SET effect_receipts_json=? WHERE id=?")
        .bind(serde_json::json!([old_receipt]).to_string())
        .bind(&earlier.id)
        .execute(db.pool())
        .await
        .unwrap();
    let stale = IntegrationQueueReopenWitness::SettledEffect {
        attempt_id: earlier.id.clone(),
        generation: fence.generation,
        operation: IntegrationOperationKind::FastForward,
    };
    assert!(matches!(
        db.reopen_integration_queue(&q.id, q.revision, &stale).await,
        Err(DbError::Check(_))
    ));
    // A ready target says nothing about an unknown merge.
    assert!(matches!(
        db.reopen_integration_queue(
            &q.id,
            q.revision,
            &IntegrationQueueReopenWitness::TargetReady {
                location_id: "l".into(),
                generation: 1
            }
        )
        .await,
        Err(DbError::InvalidTransition)
    ));
    assert_eq!(
        db.integration_queue(&q.id).await.unwrap().unwrap().state,
        IntegrationQueueState::Quarantined
    );
    let open = db
        .reopen_integration_queue(&q.id, q.revision, &witness)
        .await
        .unwrap();
    assert_eq!(open.state, IntegrationQueueState::Open);
    assert_eq!(open.revision, q.revision + 1);
    assert_eq!(
        (open.last_error_kind, open.last_error.clone()),
        (None, None)
    );
    assert_eq!(
        (&open.lease_owner, &open.head_attempt_id),
        (&q.lease_owner, &q.head_attempt_id)
    );
    // An open queue has nothing to re-open.
    assert!(matches!(
        db.reopen_integration_queue(&open.id, open.revision, &witness)
            .await,
        Err(DbError::InvalidTransition)
    ));
}

#[tokio::test]
async fn reopening_after_quarantine_with_no_ready_target_suspends_instead_of_opening() {
    let db = fixture().await;
    let (q, request) = uncertain_head(&db).await;
    exec(
        &db,
        "UPDATE integration_queue SET state='quarantined',revision=revision+1",
    )
    .await;
    // The removal settlement records a failed receipt: a valid witness.
    db.lock_integration_reconciliation(&request)
        .await
        .unwrap()
        .unwrap()
        .record(
            serde_json::json!({"kind":"infrastructure"}),
            IntegrationOperationState::Failed,
        )
        .await
        .unwrap();
    let witness = IntegrationQueueReopenWitness::SettledEffect {
        attempt_id: request.fence.attempt_id.clone(),
        generation: request.fence.generation,
        operation: IntegrationOperationKind::FastForward,
    };
    // Another operation still running on a member keeps the queue shut.
    exec(
        &db,
        "UPDATE integration_attempt SET current_operation_state='running'",
    )
    .await;
    let current = db.integration_queue(&q.id).await.unwrap().unwrap();
    assert!(matches!(
        db.reopen_integration_queue(&q.id, current.revision, &witness)
            .await,
        Err(DbError::Check(_))
    ));
    exec(
        &db,
        "UPDATE integration_attempt SET current_operation_state='failed'",
    )
    .await;
    exec(&db, "UPDATE repo_location SET status='unavailable'").await;
    let suspended = db
        .reopen_integration_queue(&q.id, current.revision, &witness)
        .await
        .unwrap();
    assert_eq!(suspended.state, IntegrationQueueState::Suspended);
    assert_eq!(
        suspended.last_error_kind,
        Some(IntegrationFailureKind::TargetUnavailable)
    );
}

#[tokio::test]
async fn a_suspended_queue_reopens_only_for_the_repos_ready_default_checkout() {
    let db = fixture().await;
    exec(&db, "UPDATE repo_location SET status='unavailable'").await;
    let q = main_queue(&db).await;
    assert_eq!(q.state, IntegrationQueueState::Suspended);
    let generation: i64 = sqlx::query_scalar("SELECT version FROM repo_location WHERE id='l'")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let ready = IntegrationQueueReopenWitness::TargetReady {
        location_id: "l".into(),
        generation,
    };
    // Still unavailable.
    assert!(matches!(
        db.reopen_integration_queue(&q.id, q.revision, &ready).await,
        Err(DbError::Check(_))
    ));
    exec(&db, "UPDATE repo_location SET status='ready'").await;
    for wrong in [
        IntegrationQueueReopenWitness::TargetReady {
            location_id: "l".into(),
            generation: generation + 1,
        },
        IntegrationQueueReopenWitness::TargetReady {
            location_id: "elsewhere".into(),
            generation,
        },
    ] {
        assert!(matches!(
            db.reopen_integration_queue(&q.id, q.revision, &wrong).await,
            Err(DbError::Check(_))
        ));
    }
    assert!(matches!(
        db.reopen_integration_queue(
            &q.id,
            q.revision,
            &IntegrationQueueReopenWitness::SettledEffect {
                attempt_id: "a".into(),
                generation: 1,
                operation: IntegrationOperationKind::FastForward
            }
        )
        .await,
        Err(DbError::InvalidTransition)
    ));
    assert!(matches!(
        db.reopen_integration_queue(&q.id, q.revision + 1, &ready)
            .await,
        Err(DbError::VersionConflict)
    ));
    assert!(matches!(
        db.reopen_integration_queue("missing", 1, &ready).await,
        Err(DbError::NotFound)
    ));
    let open = db
        .reopen_integration_queue(&q.id, q.revision, &ready)
        .await
        .unwrap();
    assert_eq!(open.state, IntegrationQueueState::Open);
    assert_eq!(open.target_location_id.as_deref(), Some("l"));
    assert_eq!(open.last_error_kind, None);
}

#[tokio::test]
async fn only_the_live_lease_holder_starts_another_round_and_the_old_fence_is_refused() {
    let db = fixture().await;
    let q = main_queue(&db).await;
    let a = admit(&db, &q, "a").await;
    let q = claim(&db, &q.id, NOW, FAR).await.unwrap();
    let old_fence = db.integration_owner_fence(&a.id).await.unwrap().unwrap();
    let round = |revision: i64, owner: &'static str, fence: i64, now: &'static str| {
        let (db, id) = (&db, q.id.clone());
        async move {
            db.start_integration_round(&id, revision, owner, fence, now, "2099-06-01T00:00:00Z")
                .await
        }
    };
    for (revision, owner, fence, now) in [
        (q.revision + 1, "worker", q.fence_generation, NOW),
        (q.revision, "someone-else", q.fence_generation, NOW),
        (q.revision, "worker", q.fence_generation + 1, NOW),
        // The lease has run out: that is a takeover claim, not a new round.
        (
            q.revision,
            "worker",
            q.fence_generation,
            "2099-02-01T00:00:00Z",
        ),
    ] {
        assert!(matches!(
            round(revision, owner, fence, now).await,
            Err(DbError::VersionConflict)
        ));
    }
    // Another worker still cannot claim under the live lease.
    assert!(matches!(
        db.claim_integration_queue(&q.id, q.revision, "someone-else", NOW, LATER)
            .await,
        Err(DbError::VersionConflict)
    ));
    let next = round(q.revision, "worker", q.fence_generation, NOW)
        .await
        .unwrap();
    assert_eq!(next.fence_generation, q.fence_generation + 1);
    assert_eq!(next.head_attempt_id, Some(a.id.clone()));
    assert_eq!(next.lease_until.as_deref(), Some("2099-06-01T00:00:00Z"));
    let head = reread(&db, &a.id).await;
    assert_eq!(head.slot_generation, next.fence_generation);
    let new_fence = db.integration_owner_fence(&a.id).await.unwrap().unwrap();
    assert_eq!(new_fence.generation, next.fence_generation);
    assert!(matches!(
        db.begin_integration_effect(effect(old_fence))
            .await
            .unwrap(),
        IntegrationEffectAdmission::Refused(IntegrationEffectRefusal::StaleFence)
    ));
    assert!(matches!(
        db.begin_integration_effect(effect(new_fence))
            .await
            .unwrap(),
        IntegrationEffectAdmission::Started(_)
    ));
    // A queue with no head has no round to start.
    let other = db
        .create_or_get_integration_queue("r", "other")
        .await
        .unwrap();
    assert!(matches!(
        db.start_integration_round(&other.id, other.revision, "worker", 0, NOW, LATER)
            .await,
        Err(DbError::VersionConflict)
    ));
}

/// The crash the witness rule must survive: the receipt arrived, the worker
/// finished the attempt (slot released, attempt terminal) and died before it
/// re-opened the queue. No claim accepts a quarantined queue without a head,
/// so the finished attempt's own receipt has to be enough.
#[tokio::test]
async fn a_quarantined_queue_whose_head_already_finished_still_reopens() {
    let db = fixture().await;
    let (q, request) = uncertain_head(&db).await;
    let q = db
        .quarantine_integration_queue(
            &q.id,
            db.integration_queue(&q.id).await.unwrap().unwrap().revision,
            "worker",
            request.fence.generation,
            IntegrationFailureKind::Timeout,
            "merge result unknown",
        )
        .await
        .unwrap();
    db.lock_integration_reconciliation(&request)
        .await
        .unwrap()
        .unwrap()
        .record(
            serde_json::json!({"kind":"not_performed"}),
            IntegrationOperationState::Failed,
        )
        .await
        .unwrap();
    let mut head = reread(&db, &request.fence.attempt_id).await;
    head.state = IntegrationAttemptState::Cancelled;
    db.transition_integration_attempt(head).await.unwrap();
    let released = db.integration_queue(&q.id).await.unwrap().unwrap();
    assert_eq!(
        (released.state, released.head_attempt_id.clone()),
        (IntegrationQueueState::Quarantined, None)
    );
    // Nothing else moves it: not a claim, and the sweep keeps returning it.
    assert!(matches!(
        claim(&db, &q.id, NOW, LATER).await,
        Err(DbError::VersionConflict)
    ));
    assert_eq!(
        db.claimable_integration_queues(NOW, None, 10)
            .await
            .unwrap()
            .len(),
        1
    );
    // Retention never removes the witness while the queue is quarantined.
    assert_eq!(db.prune_integration_evidence(FAR, 10).await.unwrap(), 0);
    let open = db
        .reopen_integration_queue(
            &q.id,
            released.revision,
            &IntegrationQueueReopenWitness::SettledEffect {
                attempt_id: request.fence.attempt_id.clone(),
                generation: request.fence.generation,
                operation: IntegrationOperationKind::FastForward,
            },
        )
        .await
        .unwrap();
    assert_eq!(open.state, IntegrationQueueState::Open);
}

/// A result proven not landed returns to validation directly: the head keeps
/// its slot and lease, nothing waits for `available_at`. An unknown result
/// cannot take that edge.
#[tokio::test]
async fn a_reconciled_head_requeues_in_place_only_once_its_effect_is_settled() {
    let db = fixture().await;
    let (q, request) = uncertain_head(&db).await;
    exec(&db, "UPDATE integration_attempt SET state='reconciling'").await;
    let requeue = |db: SqliteDb, id: String| async move {
        let mut a = reread(&db, &id).await;
        a.state = IntegrationAttemptState::Queued;
        db.transition_integration_attempt(a).await
    };
    let id = request.fence.attempt_id.clone();
    // Uncertain receipt, intent still recorded.
    assert!(matches!(
        requeue(db.clone(), id.clone()).await,
        Err(DbError::InvalidTransition)
    ));
    db.lock_integration_reconciliation(&request)
        .await
        .unwrap()
        .unwrap()
        .record(
            serde_json::json!({"kind":"not_performed"}),
            IntegrationOperationState::Failed,
        )
        .await
        .unwrap();
    // No intent, but an operation still marked running is not settled either.
    exec(
        &db,
        "UPDATE integration_attempt SET current_operation_state='running'",
    )
    .await;
    assert!(matches!(
        requeue(db.clone(), id.clone()).await,
        Err(DbError::InvalidTransition)
    ));
    exec(
        &db,
        "UPDATE integration_attempt SET current_operation_state='failed'",
    )
    .await;
    let queued = requeue(db.clone(), id.clone()).await.unwrap();
    assert_eq!(queued.state, IntegrationAttemptState::Queued);
    assert!(queued.current && queued.available_at.is_none());
    let after = db.integration_queue(&q.id).await.unwrap().unwrap();
    assert_eq!(
        (&after.head_attempt_id, &after.lease_owner, after.revision),
        (&q.head_attempt_id, &q.lease_owner, q.revision)
    );
    // The same guard closes the existing quarantined -> queued edge.
    exec(
        &db,
        "UPDATE integration_attempt SET state='quarantined',current_operation_state='uncertain'",
    )
    .await;
    assert!(matches!(
        requeue(db.clone(), id).await,
        Err(DbError::InvalidTransition)
    ));
}

/// An imported uncertain merge pins its queue as a `reconciling` head with no
/// intent and no receipt, and none can ever be recorded for it. Once the
/// lookup has resolved the head, the queue must still be able to open.
#[tokio::test]
async fn a_queue_quarantined_by_a_head_with_no_effect_reopens_once_the_head_is_resolved() {
    let db = fixture().await;
    let q = main_queue(&db).await;
    let head = admit(&db, &q, "a").await;
    let waiting = admit(&db, &q, "b").await;
    // As the importer writes it.
    sqlx::query("UPDATE integration_attempt SET state='reconciling' WHERE id=?")
        .bind(&head.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE integration_queue SET state='quarantined',head_attempt_id=?,revision=revision+1 WHERE id=?")
        .bind(&head.id)
        .bind(&q.id)
        .execute(db.pool())
        .await
        .unwrap();
    let no_effect = |attempt_id: &str| IntegrationQueueReopenWitness::NoEffect {
        attempt_id: attempt_id.to_owned(),
    };
    let reopen = |witness: IntegrationQueueReopenWitness| {
        let (db, id) = (&db, q.id.clone());
        async move {
            let revision = db.integration_queue(&id).await.unwrap().unwrap().revision;
            db.reopen_integration_queue(&id, revision, &witness).await
        }
    };
    // Still unresolved: the worker claims it and looks the result up first.
    assert!(matches!(
        reopen(no_effect(&head.id)).await,
        Err(DbError::Check(_))
    ));
    claim(&db, &q.id, NOW, LATER).await.unwrap();
    let mut resolved = reread(&db, &head.id).await;
    assert_eq!(resolved.state, IntegrationAttemptState::Reconciling);
    // Proven not landed.
    resolved.state = IntegrationAttemptState::Queued;
    db.transition_integration_attempt(resolved).await.unwrap();
    // Another member cannot speak for the head, nor can an unknown attempt.
    assert!(matches!(
        reopen(no_effect(&waiting.id)).await,
        Err(DbError::Check(_))
    ));
    assert!(matches!(
        reopen(no_effect("missing")).await,
        Err(DbError::Check(_))
    ));
    let open = reopen(no_effect(&head.id)).await.unwrap();
    assert_eq!(open.state, IntegrationQueueState::Open);
    // Lease and head are untouched, so the holder goes on with its head.
    assert_eq!(
        (open.head_attempt_id.as_deref(), open.lease_owner.as_deref()),
        (Some(head.id.as_str()), Some("worker"))
    );
    db.start_integration_round(
        &open.id,
        open.revision,
        "worker",
        open.fence_generation,
        NOW,
        LATER,
    )
    .await
    .unwrap();
}

/// `NoEffect` is only for an attempt that never had an effect admitted: one
/// with an intent or any receipt must be witnessed by its settled receipt.
#[tokio::test]
async fn no_effect_cannot_stand_in_for_an_effect_that_was_admitted() {
    let db = fixture().await;
    let (q, request) = uncertain_head(&db).await;
    let q = db
        .quarantine_integration_queue(
            &q.id,
            db.integration_queue(&q.id).await.unwrap().unwrap().revision,
            "worker",
            request.fence.generation,
            IntegrationFailureKind::Timeout,
            "merge result unknown",
        )
        .await
        .unwrap();
    let witness = IntegrationQueueReopenWitness::NoEffect {
        attempt_id: request.fence.attempt_id.clone(),
    };
    assert!(matches!(
        db.reopen_integration_queue(&q.id, q.revision, &witness)
            .await,
        Err(DbError::Check(_))
    ));
    db.lock_integration_reconciliation(&request)
        .await
        .unwrap()
        .unwrap()
        .record(
            serde_json::json!({"kind":"not_performed"}),
            IntegrationOperationState::Failed,
        )
        .await
        .unwrap();
    let mut head = reread(&db, &request.fence.attempt_id).await;
    head.state = IntegrationAttemptState::Cancelled;
    db.transition_integration_attempt(head).await.unwrap();
    let q = db.integration_queue(&q.id).await.unwrap().unwrap();
    // Settled and released, but it has a receipt: that receipt is the witness.
    assert!(matches!(
        db.reopen_integration_queue(&q.id, q.revision, &witness)
            .await,
        Err(DbError::Check(_))
    ));
    assert_eq!(
        serde_json::to_value(&witness).unwrap(),
        serde_json::json!({"kind":"no_effect","attempt_id":request.fence.attempt_id})
    );
}
