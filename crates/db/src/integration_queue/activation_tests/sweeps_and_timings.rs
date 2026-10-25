//! Integration sweeps, timing bounds, pruning, and query indexes.

use super::*;

#[tokio::test]
async fn head_timings_are_cas_head_only_validated_and_bounded() {
    let db = fixture().await;
    let q = main_queue(&db).await;
    let a = admit(&db, &q, "a").await;
    let b = admit(&db, &q, "b").await;
    let timings = IntegrationPhaseTimings {
        rounds: 1,
        queued_ms: Some(900),
        rebase_ms: Some(40),
        check: Some(IntegrationCheckTiming::Ran {
            slot_wait_ms: 10,
            run_ms: 2000,
        }),
        ff_ms: Some(7),
        lost_races: vec![IntegrationLostRace {
            kind: IntegrationLostRaceKind::External,
            round: 1,
            at: NOW.into(),
        }],
        ..Default::default()
    };
    // Not a head yet.
    assert!(matches!(
        db.record_integration_head_timings(&a.id, a.revision, &timings)
            .await,
        Err(DbError::InvalidTransition)
    ));
    claim(&db, &q.id, NOW, LATER).await.unwrap();
    assert!(matches!(
        db.record_integration_head_timings(&a.id, a.revision, &timings)
            .await,
        Err(DbError::VersionConflict)
    ));
    let head = reread(&db, &a.id).await;
    let recorded = db
        .record_integration_head_timings(&head.id, head.revision, &timings)
        .await
        .unwrap();
    assert_eq!(recorded.revision, head.revision + 1);
    assert_eq!(recorded.phase_timings, Some(timings.clone()));
    assert_eq!(recorded, reread(&db, &a.id).await);
    assert!(matches!(
        db.record_integration_head_timings(&b.id, b.revision, &timings)
            .await,
        Err(DbError::InvalidTransition)
    ));
    assert!(matches!(
        db.record_integration_head_timings("missing", 1, &timings)
            .await,
        Err(DbError::NotFound)
    ));
    let mut negative = timings.clone();
    negative.ff_ms = Some(-1);
    let mut undated = timings.clone();
    undated.lost_races[0].at = "soon".into();
    let mut padded = timings.clone();
    padded.lost_races[0].at = format!("2026-10-08T00:00:00.{}Z", "0".repeat(40));
    let mut uncounted = timings.clone();
    uncounted.lost_races_folded.external = -1;
    for invalid in [negative, undated, padded, uncounted] {
        assert!(matches!(
            db.record_integration_head_timings(&recorded.id, recorded.revision, &invalid)
                .await,
            Err(DbError::Check(_))
        ));
    }
    // The 33rd lost race never fails the write: the oldest entries are folded
    // into per-kind counts, the allowance count stays exact, and the largest
    // document the repository can store is far inside the column bound.
    let mut crowded = timings.clone();
    crowded.rounds = i64::MAX;
    for field in [
        &mut crowded.queued_ms,
        &mut crowded.validate_ms,
        &mut crowded.transfer_ms,
        &mut crowded.rebase_ms,
        &mut crowded.step_wait_ms,
        &mut crowded.ff_ms,
        &mut crowded.head_total_ms,
    ] {
        *field = Some(i64::MAX);
    }
    crowded.check = Some(IntegrationCheckTiming::Ran {
        slot_wait_ms: i64::MAX,
        run_ms: i64::MAX,
    });
    crowded.lost_races_folded = IntegrationLostRaceCounts {
        queue_member: 1 << 60,
        external: 1 << 60,
    };
    crowded.lost_races = (0..INTEGRATION_LOST_RACES_MAX as i64 + 3)
        .map(|n| IntegrationLostRace {
            kind: if n < 2 {
                IntegrationLostRaceKind::External
            } else {
                IntegrationLostRaceKind::QueueMember
            },
            round: i64::MAX - n,
            at: "2026-10-08T00:00:00.123456789+00:00".into(),
        })
        .collect();
    let external_before = crowded.external_target_moves();
    let folded = db
        .record_integration_head_timings(&recorded.id, recorded.revision, &crowded)
        .await
        .unwrap();
    let stored = folded.phase_timings.clone().unwrap();
    assert_eq!(stored.lost_races.len(), INTEGRATION_LOST_RACES_MAX);
    assert_eq!(stored.lost_races[..], crowded.lost_races[3..]);
    assert_eq!(
        stored.lost_races_folded,
        IntegrationLostRaceCounts {
            queue_member: (1 << 60) + 1,
            external: (1 << 60) + 2,
        }
    );
    assert_eq!(stored.external_target_moves(), external_before);
    assert_eq!(reread(&db, &a.id).await, folded);
    let bytes: i64 = sqlx::query_scalar(
        "SELECT length(CAST(phase_timings_json AS BLOB)) FROM integration_attempt WHERE id=?",
    )
    .bind(&a.id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert!(bytes < 8192, "largest timings document is {bytes} bytes");
    let recorded = db
        .record_integration_head_timings(&folded.id, folded.revision, &timings)
        .await
        .unwrap();
    // A later state write keeps the document and cannot replace it.
    let mut moved = recorded.clone();
    moved.state = IntegrationAttemptState::Validating;
    let moved = db.transition_integration_attempt(moved).await.unwrap();
    assert_eq!(
        reread(&db, &a.id).await.phase_timings,
        Some(timings.clone())
    );
    let mut replaced = moved;
    replaced.phase_timings = None;
    assert!(matches!(
        db.transition_integration_attempt(replaced).await,
        Err(DbError::Check(_))
    ));
}

#[tokio::test]
async fn queue_sweeps_return_the_exact_set_in_bounded_indexed_pages() {
    let db = fixture().await;
    for task in ["c", "d", "e", "f"] {
        add_task(&db, task).await;
    }
    let branch = |name: &'static str| {
        let db = &db;
        async move { db.create_or_get_integration_queue("r", name).await.unwrap() }
    };
    // due: a queued member. later: its only member is not due yet.
    // flagged: its only member asked to cancel. live / expired: leased heads.
    // unknown: quarantined with nobody holding it. closed: never returned.
    let due = branch("due").await;
    admit(&db, &due, "a").await;
    let later = branch("later").await;
    let waiting = admit(&db, &later, "b").await;
    sqlx::query(
        "UPDATE integration_attempt SET available_at='2026-10-08T03:00:00+02:00' WHERE id=?",
    )
    .bind(&waiting.id)
    .execute(db.pool())
    .await
    .unwrap();
    let flagged = branch("flagged").await;
    let asked = admit(&db, &flagged, "c").await;
    db.request_integration_cancel(&asked.id, asked.revision, NOW)
        .await
        .unwrap();
    let live = branch("live").await;
    admit(&db, &live, "d").await;
    claim(&db, &live.id, NOW, FAR).await.unwrap();
    let expired = branch("expired").await;
    admit(&db, &expired, "e").await;
    claim(
        &db,
        &expired.id,
        "2026-10-07T23:00:00Z",
        "2026-10-07T23:01:00Z",
    )
    .await
    .unwrap();
    let unknown = branch("unknown").await;
    let closed = branch("closed").await;
    admit(&db, &closed, "f").await;
    sqlx::query("UPDATE integration_queue SET state='quarantined' WHERE id=?")
        .bind(&unknown.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE integration_queue SET state='closed' WHERE id=?")
        .bind(&closed.id)
        .execute(db.pool())
        .await
        .unwrap();

    let ids = |queues: Vec<IntegrationQueue>| queues.into_iter().map(|q| q.id).collect::<Vec<_>>();
    let mut expected = vec![due.id.clone(), unknown.id.clone()];
    expected.sort();
    assert_eq!(
        ids(db
            .claimable_integration_queues(NOW, None, 10)
            .await
            .unwrap()),
        expected
    );
    // 01:00Z is the instant 03:00+02:00: the member is due by time, not text.
    let mut at_one = expected.clone();
    at_one.push(later.id.clone());
    at_one.sort();
    assert_eq!(
        ids(db
            .claimable_integration_queues("2026-10-08T01:00:00Z", None, 10)
            .await
            .unwrap()),
        at_one
    );
    let first = db.claimable_integration_queues(NOW, None, 1).await.unwrap();
    assert_eq!(ids(first.clone()), expected[..1]);
    assert_eq!(
        ids(db
            .claimable_integration_queues(NOW, Some(&first[0].id), 10)
            .await
            .unwrap()),
        expected[1..]
    );
    // Every claimable open queue is in fact accepted by the claim.
    claim(&db, &due.id, NOW, LATER).await.unwrap();

    assert_eq!(
        ids(db.expired_integration_heads(NOW, None, 10).await.unwrap()),
        vec![expired.id.clone()]
    );
    // After LATER the lease taken just above has run out too.
    let mut both = vec![expired.id.clone(), due.id.clone()];
    both.sort();
    let after_later = "2026-10-08T00:02:00Z";
    assert_eq!(
        ids(db
            .expired_integration_heads(after_later, None, 10)
            .await
            .unwrap()),
        both
    );
    let page = db
        .expired_integration_heads(after_later, None, 1)
        .await
        .unwrap();
    assert_eq!(ids(page.clone()), both[..1]);
    assert_eq!(
        ids(db
            .expired_integration_heads(after_later, Some(&page[0].id), 10)
            .await
            .unwrap()),
        both[1..]
    );
    assert!(matches!(
        db.claimable_integration_queues("soon", None, 1).await,
        Err(DbError::Check(_))
    ));
    assert_plan_uses(&db, CLAIMABLE_QUEUES_SQL, "integration_attempt_members").await;
    assert_plan_uses(&db, EXPIRED_HEADS_SQL, "integration_queue_expired_lease").await;
}

#[tokio::test]
async fn due_parked_sweep_compares_instants_and_pages_by_id() {
    let db = fixture().await;
    for task in ["c", "d"] {
        add_task(&db, task).await;
    }
    let q = main_queue(&db).await;
    let mut parked = Vec::new();
    for (task, available_at) in [
        ("a", Some("2026-10-07T23:59:00Z")),
        // 00:00Z written with an offset: due at NOW, though it sorts later as text.
        ("b", Some("2026-10-08T02:00:00+02:00")),
        ("c", Some("2026-10-08T06:00:00Z")),
        ("d", None),
    ] {
        let mut a = admit(&db, &q, task).await;
        a.state = IntegrationAttemptState::Parked;
        a.available_at = available_at.map(Into::into);
        parked.push(db.transition_integration_attempt(a).await.unwrap());
    }
    let ids =
        |attempts: Vec<IntegrationAttempt>| attempts.into_iter().map(|a| a.id).collect::<Vec<_>>();
    let mut expected = vec![parked[0].id.clone(), parked[1].id.clone()];
    expected.sort();
    assert_eq!(
        ids(db
            .due_parked_integration_attempts(NOW, None, 10)
            .await
            .unwrap()),
        expected
    );
    let first = db
        .due_parked_integration_attempts(NOW, None, 1)
        .await
        .unwrap();
    assert_eq!(ids(first.clone()), expected[..1]);
    assert_eq!(
        ids(db
            .due_parked_integration_attempts(NOW, Some(&first[0].id), 10)
            .await
            .unwrap()),
        expected[1..]
    );
    // A re-queued attempt leaves the sweep.
    let mut requeued = reread(&db, &expected[0]).await;
    requeued.state = IntegrationAttemptState::Queued;
    db.transition_integration_attempt(requeued).await.unwrap();
    assert_eq!(
        ids(db
            .due_parked_integration_attempts(NOW, None, 10)
            .await
            .unwrap()),
        expected[1..]
    );
    assert_plan_uses(&db, DUE_PARKED_SQL, "integration_attempt_timed").await;
}

#[tokio::test]
async fn pruning_is_bounded_and_keeps_live_recent_and_uncertain_evidence() {
    let db = fixture().await;
    for task in ["c", "d", "e", "f", "g", "h", "i"] {
        add_task(&db, task).await;
    }
    let q = main_queue(&db).await;
    const OLD: &str = "2026-01-01T00:00:00+00:00";
    const CUTOFF: &str = "2026-09-01T00:00:00Z";
    let evidence = "effect_receipts_json='[{\"operation_state\":\"succeeded\"}]',operation_receipts_json='[{\"kept\":1}]',observations_json='[{\"kept\":1}]'";
    let finish = |task: &'static str, completed_at: &'static str| {
        let (db, q) = (&db, &q);
        async move {
            let mut a = admit(db, q, task).await;
            a.state = IntegrationAttemptState::Cancelled;
            let a = db.transition_integration_attempt(a).await.unwrap();
            sqlx::query(&format!(
                "UPDATE integration_attempt SET {evidence},completed_at=? WHERE id=?"
            ))
            .bind(completed_at)
            .bind(&a.id)
            .execute(db.pool())
            .await
            .unwrap();
            a.id
        }
    };
    let old = [
        finish("a", OLD).await,
        finish("b", OLD).await,
        finish("c", OLD).await,
    ];
    let recent = finish("d", "2026-09-15T00:00:00+00:00").await;
    // Old by its text, recent by its instant: 23:30-02:00 is 01:30Z, after the cutoff.
    let offset = finish("e", "2026-08-31T23:30:00-02:00").await;
    let uncertain = finish("f", OLD).await;
    sqlx::query("UPDATE integration_attempt SET effect_receipts_json='[{\"operation_state\":\"succeeded\"},{\"operation_state\":\"uncertain\"}]' WHERE id=?").bind(&uncertain).execute(db.pool()).await.unwrap();
    let running = finish("g", OLD).await;
    sqlx::query("UPDATE integration_attempt SET current_operation_state='uncertain' WHERE id=?")
        .bind(&running)
        .execute(db.pool())
        .await
        .unwrap();
    // A superseded attempt whose Task still has a live successor.
    let mut predecessor = admit(&db, &q, "h").await;
    predecessor.state = IntegrationAttemptState::Parked;
    let predecessor = db
        .transition_integration_attempt(predecessor)
        .await
        .unwrap();
    db.supersede_integration_attempt(
        &predecessor.id,
        predecessor.revision,
        admission(&q, "h", "again"),
    )
    .await
    .unwrap();
    sqlx::query(&format!(
        "UPDATE integration_attempt SET {evidence},completed_at=? WHERE id=?"
    ))
    .bind(OLD)
    .bind(&predecessor.id)
    .execute(db.pool())
    .await
    .unwrap();
    // A live attempt created long ago.
    let live = admit(&db, &q, "i").await;
    sqlx::query(&format!(
        "UPDATE integration_attempt SET {evidence},created_at=?,enqueued_at=? WHERE id=?"
    ))
    .bind(OLD)
    .bind(OLD)
    .bind(&live.id)
    .execute(db.pool())
    .await
    .unwrap();

    let kept = |id: String| {
        let db = &db;
        async move {
            let a = reread(db, &id).await;
            a.effect_receipts_json != serde_json::json!([])
                && a.operation_receipts_json != serde_json::json!([])
                && a.observations_json != serde_json::json!([])
        }
    };
    assert!(matches!(
        db.prune_integration_evidence("last month", 10).await,
        Err(DbError::Check(_))
    ));
    assert_eq!(db.prune_integration_evidence(CUTOFF, 2).await.unwrap(), 2);
    assert_eq!(db.prune_integration_evidence(CUTOFF, 2).await.unwrap(), 1);
    assert_eq!(db.prune_integration_evidence(CUTOFF, 2).await.unwrap(), 0);
    for id in old {
        let a = reread(&db, &id).await;
        assert_eq!(a.effect_receipts_json, serde_json::json!([]));
        assert_eq!(a.operation_receipts_json, serde_json::json!([]));
        assert_eq!(a.observations_json, serde_json::json!([]));
        // The row, its outcome and its identity stay.
        assert_eq!(a.state, IntegrationAttemptState::Cancelled);
        assert_eq!(a.completed_at.as_deref(), Some(OLD));
    }
    for id in [
        recent,
        offset,
        uncertain,
        running.clone(),
        predecessor.id.clone(),
        live.id.clone(),
    ] {
        assert!(kept(id.clone()).await, "{id} lost evidence");
    }
    // A quarantined queue keeps everything, however old.
    sqlx::query("UPDATE integration_attempt SET current_operation_state=NULL WHERE id=?")
        .bind(&running)
        .execute(db.pool())
        .await
        .unwrap();
    exec(&db, "UPDATE integration_queue SET state='quarantined'").await;
    assert_eq!(db.prune_integration_evidence(CUTOFF, 10).await.unwrap(), 0);
    exec(&db, "UPDATE integration_queue SET state='open'").await;
    assert_eq!(db.prune_integration_evidence(CUTOFF, 10).await.unwrap(), 1);
    assert_plan_uses(&db, PRUNE_SQL, "integration_attempt_prunable").await;
}

#[tokio::test]
async fn operator_ages_and_timing_samples_read_the_stored_times() {
    let db = fixture().await;
    for task in ["c", "d"] {
        add_task(&db, task).await;
    }
    assert_eq!(
        db.integration_queue_ages(NOW).await.unwrap(),
        IntegrationQueueAges::default()
    );
    let q = main_queue(&db).await;
    let a = admit(&db, &q, "a").await;
    let b = admit(&db, &q, "b").await;
    let c = admit(&db, &q, "c").await;
    let d = admit(&db, &q, "d").await;
    // a: head since 23:59, lease ended 23:59:30. b, c: queued since 23:50 and
    // 23:58, c asked to cancel at 23:59:50. d: parked since 23:55.
    claim(&db, &q.id, "2026-10-07T23:59:00Z", "2026-10-07T23:59:30Z")
        .await
        .unwrap();
    let mut head = reread(&db, &a.id).await;
    head.state = IntegrationAttemptState::Validating;
    db.transition_integration_attempt(head).await.unwrap();
    let c = db
        .request_integration_cancel(&c.id, c.revision, "2026-10-07T23:59:50Z")
        .await
        .unwrap();
    let mut parked = d.clone();
    parked.state = IntegrationAttemptState::Parked;
    db.transition_integration_attempt(parked).await.unwrap();
    for (id, column, value) in [
        (&a.id, "started_at", "2026-10-07T23:59:00Z"),
        (&b.id, "enqueued_at", "2026-10-07T23:50:00Z"),
        (&c.id, "enqueued_at", "2026-10-07T23:58:00Z"),
        (&d.id, "updated_at", "2026-10-07T23:55:00Z"),
    ] {
        sqlx::query(&format!(
            "UPDATE integration_attempt SET {column}=? WHERE id=?"
        ))
        .bind(value)
        .bind(id)
        .execute(db.pool())
        .await
        .unwrap();
    }
    let ages = db.integration_queue_ages(NOW).await.unwrap();
    let age = |count, oldest_seconds| IntegrationAge {
        count,
        oldest_seconds: Some(oldest_seconds),
    };
    assert_eq!(ages.queued, age(2, 600));
    assert_eq!(ages.heads, age(1, 60));
    assert_eq!(ages.expired_heads, age(1, 30));
    assert_eq!(ages.parked, age(1, 300));
    assert_eq!(ages.cancel_requested, age(1, 10));
    assert_eq!(ages.unsettled_effects, IntegrationAge::default());

    // Samples: completed attempts with a timings document, newest first.
    let timings = |ff_ms| IntegrationPhaseTimings {
        rounds: 1,
        ff_ms: Some(ff_ms),
        ..Default::default()
    };
    for (id, completed_at, ff_ms) in [
        (&b.id, "2026-10-07T10:00:00+00:00", 1),
        (&c.id, "2026-10-07T12:00:00+00:00", 2),
        (&d.id, "2026-10-01T00:00:00+00:00", 3),
    ] {
        sqlx::query("UPDATE integration_attempt SET state='completed',current=0,completed_at=?,phase_timings_json=? WHERE id=?")
            .bind(completed_at)
            .bind(serde_json::to_string(&timings(ff_ms)).unwrap())
            .bind(id)
            .execute(db.pool())
            .await
            .unwrap();
    }
    let since = "2026-10-07T00:00:00+00:00";
    assert_eq!(
        db.integration_timing_samples(since, 10).await.unwrap(),
        vec![timings(2), timings(1)]
    );
    assert_eq!(
        db.integration_timing_samples(since, 1).await.unwrap(),
        vec![timings(2)]
    );
    // The bound is an instant: 13:00+02:00 is 11:00Z, between the two samples,
    // though as text it sorts after both.
    assert_eq!(
        db.integration_timing_samples("2026-10-07T13:00:00+02:00", 10)
            .await
            .unwrap(),
        vec![timings(2)]
    );
    assert_plan_uses(&db, TIMING_SAMPLES_SQL, "integration_attempt_retention").await;
}

/// Timings are measurements: a document this build cannot read must not make
/// the attempt unreadable, or every claim and transition of its queue fails.
#[tokio::test]
async fn an_unreadable_timings_document_reads_as_absent_and_is_replaced() {
    let db = fixture().await;
    let q = main_queue(&db).await;
    let a = admit(&db, &q, "a").await;
    exec(
        &db,
        "UPDATE integration_attempt SET phase_timings_json='{\"rounds\":\"many\"}'",
    )
    .await;
    assert_eq!(reread(&db, &a.id).await.phase_timings, None);
    claim(&db, &q.id, NOW, LATER).await.unwrap();
    let mut head = reread(&db, &a.id).await;
    head.state = IntegrationAttemptState::Validating;
    let head = db.transition_integration_attempt(head).await.unwrap();
    let timings = IntegrationPhaseTimings {
        rounds: 1,
        ..Default::default()
    };
    let head = db
        .record_integration_head_timings(&head.id, head.revision, &timings)
        .await
        .unwrap();
    assert_eq!(head.phase_timings, Some(timings));
    exec(
        &db,
        "UPDATE integration_attempt SET state='completed',current=0,completed_at='2026-10-08T00:00:00+00:00',phase_timings_json='{\"rounds\":\"many\"}'",
    )
    .await;
    assert!(db
        .integration_timing_samples("2026-10-01T00:00:00Z", 10)
        .await
        .unwrap()
        .is_empty());
}

/// A later migration that drops, renames or re-scopes an index named by
/// `INDEXED BY` must fail here, not as a refused sweep in production.
#[tokio::test]
async fn hinted_statements_prepare_and_their_indexes_are_pinned() {
    let db = fixture().await;
    for (sql, index, definition) in [
        (
            EXPIRED_HEADS_SQL,
            "integration_queue_expired_lease",
            "CREATE INDEX integration_queue_expired_lease ON integration_queue(lease_until) WHERE lease_until IS NOT NULL",
        ),
        (
            PRUNE_SQL,
            "integration_attempt_prunable",
            "CREATE INDEX integration_attempt_prunable ON integration_attempt(completed_at,id) WHERE completed_at IS NOT NULL AND (effect_receipts_json<>'[]' OR operation_receipts_json<>'[]' OR observations_json<>'[]')",
        ),
        (
            RETAINED_EVIDENCE_SQL,
            "integration_attempt_prunable",
            "CREATE INDEX integration_attempt_prunable ON integration_attempt(completed_at,id) WHERE completed_at IS NOT NULL AND (effect_receipts_json<>'[]' OR operation_receipts_json<>'[]' OR observations_json<>'[]')",
        ),
    ] {
        assert!(sql.contains(&format!("INDEXED BY {index} ")), "{index}");
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT sql FROM sqlite_master WHERE type='index' AND name=?"
            )
            .bind(index)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            definition
        );
        assert_plan_uses(&db, sql, index).await;
    }
    for unhinted in [
        CLAIMABLE_QUEUES_SQL,
        DUE_PARKED_SQL,
        CANCEL_REQUESTED_SQL,
        TIMING_SAMPLES_SQL,
    ] {
        assert!(!unhinted.contains("INDEXED BY"));
    }
}

/// Replay reads the attempt's receipts. After pruning there is nothing to
/// replay, and the same attempt key must be refused, never performed again.
#[tokio::test]
async fn a_pruned_attempt_key_is_refused_and_never_performed_again() {
    let db = fixture().await;
    let q = main_queue(&db).await;
    let a = admit(&db, &q, "a").await;
    claim(&db, &q.id, NOW, FAR).await.unwrap();
    let request = effect(db.integration_owner_fence(&a.id).await.unwrap().unwrap());
    let IntegrationEffectAdmission::Started(mut guard) =
        db.begin_integration_effect(request.clone()).await.unwrap()
    else {
        panic!("effect not admitted");
    };
    assert!(guard.start().await.unwrap().is_none());
    guard
        .record(
            serde_json::json!({"kind":"merged"}),
            IntegrationOperationState::Succeeded,
        )
        .await
        .unwrap();
    assert!(matches!(
        db.begin_integration_effect(request.clone()).await.unwrap(),
        IntegrationEffectAdmission::Replay(_)
    ));
    let mut done = reread(&db, &a.id).await;
    for state in [
        IntegrationAttemptState::Validating,
        IntegrationAttemptState::Applied,
        IntegrationAttemptState::Completed,
    ] {
        done.state = state;
        done = db.transition_integration_attempt(done).await.unwrap();
    }
    sqlx::query(
        "UPDATE integration_attempt SET completed_at='2026-01-01T00:00:00+00:00' WHERE id=?",
    )
    .bind(&a.id)
    .execute(db.pool())
    .await
    .unwrap();
    assert_eq!(
        db.prune_integration_evidence("2026-09-01T00:00:00Z", 10)
            .await
            .unwrap(),
        1
    );
    assert!(matches!(
        db.begin_integration_effect(request).await.unwrap(),
        IntegrationEffectAdmission::Refused(IntegrationEffectRefusal::StaleFence)
    ));
    let pruned = reread(&db, &a.id).await;
    assert!(pruned.effect_intent_json.is_none());
    assert_eq!(pruned.effect_receipts_json, serde_json::json!([]));
}
