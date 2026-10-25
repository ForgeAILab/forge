//! Integration cancellation admission, claim races, and transitions.

use super::*;

#[tokio::test]
async fn cancel_request_is_cas_idempotent_state_checked_and_survives_transitions() {
    let db = fixture().await;
    let q = main_queue(&db).await;
    let a = admit(&db, &q, "a").await;
    let b = admit(&db, &q, "b").await;
    assert!(matches!(
        db.request_integration_cancel("missing", 1, NOW).await,
        Err(DbError::NotFound)
    ));
    assert!(matches!(
        db.request_integration_cancel(&a.id, a.revision + 1, NOW)
            .await,
        Err(DbError::VersionConflict)
    ));
    assert!(matches!(
        db.request_integration_cancel(&a.id, a.revision, "yesterday")
            .await,
        Err(DbError::Check(_))
    ));
    let requested = db
        .request_integration_cancel(&a.id, a.revision, NOW)
        .await
        .unwrap();
    assert_eq!(requested.cancel_requested_at.as_deref(), Some(NOW));
    assert_eq!(requested.revision, a.revision + 1);
    assert_eq!(requested, reread(&db, &a.id).await);
    // The old copy lost the race; a repeat with the new revision keeps the
    // first request and writes nothing.
    assert!(matches!(
        db.request_integration_cancel(&a.id, a.revision, LATER)
            .await,
        Err(DbError::VersionConflict)
    ));
    assert_eq!(
        db.request_integration_cancel(&a.id, requested.revision, LATER)
            .await
            .unwrap(),
        requested
    );

    // A state write can neither clear nor invent the flag.
    let mut cleared = requested.clone();
    cleared.cancel_requested_at = None;
    cleared.state = IntegrationAttemptState::Parked;
    assert!(matches!(
        db.transition_integration_attempt(cleared).await,
        Err(DbError::Check(_))
    ));
    let mut invented = b.clone();
    invented.cancel_requested_at = Some(NOW.into());
    assert!(matches!(
        db.transition_integration_attempt(invented).await,
        Err(DbError::Check(_))
    ));
    let mut parked = requested.clone();
    parked.state = IntegrationAttemptState::Parked;
    let parked = db.transition_integration_attempt(parked).await.unwrap();
    assert_eq!(
        reread(&db, &a.id).await.cancel_requested_at.as_deref(),
        Some(NOW)
    );
    // The worker applies the request with the ordinary guarded transition.
    let mut cancelled = parked;
    cancelled.state = IntegrationAttemptState::Cancelled;
    let cancelled = db.transition_integration_attempt(cancelled).await.unwrap();
    assert!(matches!(
        db.request_integration_cancel(&cancelled.id, cancelled.revision, NOW)
            .await,
        Err(DbError::InvalidTransition)
    ));

    // Critical and unknown-result states refuse the request.
    for state in ["ff_inflight", "reconciling", "applied", "quarantined"] {
        sqlx::query("UPDATE integration_attempt SET state=? WHERE id=?")
            .bind(state)
            .bind(&b.id)
            .execute(db.pool())
            .await
            .unwrap();
        assert!(
            matches!(
                db.request_integration_cancel(&b.id, b.revision, NOW).await,
                Err(DbError::InvalidTransition)
            ),
            "{state}"
        );
    }
    assert!(reread(&db, &b.id).await.cancel_requested_at.is_none());
}

#[tokio::test]
async fn a_cancel_requested_member_is_never_claimed_and_a_head_can_still_be_asked() {
    let db = fixture().await;
    let q = main_queue(&db).await;
    let a = admit(&db, &q, "a").await;
    let b = admit(&db, &q, "b").await;
    db.request_integration_cancel(&a.id, a.revision, NOW)
        .await
        .unwrap();
    let claimed = claim(&db, &q.id, NOW, LATER).await.unwrap();
    assert_eq!(claimed.head_attempt_id, Some(b.id.clone()));
    // The claim rewrote the head, so the pre-claim copy is stale.
    assert!(matches!(
        db.request_integration_cancel(&b.id, b.revision, NOW).await,
        Err(DbError::VersionConflict)
    ));
    let head = reread(&db, &b.id).await;
    let head = db
        .request_integration_cancel(&head.id, head.revision, NOW)
        .await
        .unwrap();
    assert_eq!(head.cancel_requested_at.as_deref(), Some(NOW));
    let listed = db
        .cancel_requested_integration_attempts(None, 10)
        .await
        .unwrap();
    let mut expected = vec![a.id.clone(), b.id.clone()];
    expected.sort();
    assert_eq!(
        listed.iter().map(|a| a.id.clone()).collect::<Vec<_>>(),
        expected
    );
    let first = db
        .cancel_requested_integration_attempts(None, 1)
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    let rest = db
        .cancel_requested_integration_attempts(Some(&first[0].id), 10)
        .await
        .unwrap();
    assert_eq!(rest.len(), 1);
    assert_ne!(rest[0].id, first[0].id);
    assert_plan_uses(
        &db,
        CANCEL_REQUESTED_SQL,
        "integration_attempt_cancel_requested",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_and_claim_race_on_real_connections_and_exactly_one_wins() {
    let dir = tempfile::tempdir().unwrap();
    let pool = create_sqlite_pool(&format!("sqlite://{}", dir.path().join("q.db").display()))
        .await
        .unwrap();
    run_migrations(&pool).await.unwrap();
    seed(&pool).await;
    let db = Arc::new(SqliteDb::new(pool));
    let (mut cancel_wins, mut claim_wins) = (0, 0);
    for round in 0..24 {
        let q = db
            .create_or_get_integration_queue("r", &format!("race-{round}"))
            .await
            .unwrap();
        let a = db
            .admit_integration_attempt(admission(&q, "a", &format!("race-{round}")))
            .await
            .unwrap();
        let q = db.integration_queue(&q.id).await.unwrap().unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(4));
        let cancel = {
            let (db, barrier, a) = (db.clone(), barrier.clone(), a.clone());
            tokio::spawn(async move {
                barrier.wait().await;
                db.request_integration_cancel(&a.id, a.revision, NOW).await
            })
        };
        let mut claims = Vec::new();
        for worker in 0..3 {
            let (db, barrier, q) = (db.clone(), barrier.clone(), q.clone());
            claims.push(tokio::spawn(async move {
                barrier.wait().await;
                db.claim_integration_queue(
                    &q.id,
                    q.revision,
                    &format!("worker-{worker}"),
                    NOW,
                    LATER,
                )
                .await
            }));
        }
        let cancel = cancel.await.unwrap();
        let mut claimed = 0;
        for claim in claims {
            match claim.await.unwrap() {
                Ok(queue) => {
                    claimed += 1;
                    assert_eq!(queue.head_attempt_id, Some(a.id.clone()));
                }
                Err(DbError::NotFound | DbError::VersionConflict) => {}
                Err(other) => panic!("unexpected claim error {other:?}"),
            }
        }
        let after = db.integration_attempt(&a.id).await.unwrap().unwrap();
        let queue = db.integration_queue(&q.id).await.unwrap().unwrap();
        match cancel {
            Ok(requested) => {
                cancel_wins += 1;
                assert_eq!(claimed, 0, "round {round}: cancelled member was claimed");
                assert_eq!(requested.cancel_requested_at.as_deref(), Some(NOW));
                assert_eq!(after.cancel_requested_at.as_deref(), Some(NOW));
                assert_eq!((queue.head_attempt_id, queue.fence_generation), (None, 0));
            }
            Err(DbError::VersionConflict) => {
                claim_wins += 1;
                assert_eq!(claimed, 1, "round {round}");
                assert!(after.cancel_requested_at.is_none());
                assert_eq!(queue.fence_generation, 1);
                // The loser re-reads and asks the head instead.
                db.request_integration_cancel(&after.id, after.revision, NOW)
                    .await
                    .unwrap();
            }
            Err(other) => panic!("unexpected cancel error {other:?}"),
        }
        // Whoever won, the request is now durable and the worker's sweep
        // returns it: as a member no claim will pick, or as the leased head.
        let flagged = db
            .cancel_requested_integration_attempts(None, 10)
            .await
            .unwrap();
        assert_eq!(flagged.len(), 1, "round {round}");
        assert_eq!(
            (flagged[0].id.as_str(), flagged[0].state),
            (a.id.as_str(), IntegrationAttemptState::Queued)
        );
        // Free the Task for the next round.
        sqlx::query("DELETE FROM integration_queue WHERE id=?")
            .bind(&q.id)
            .execute(db.pool())
            .await
            .unwrap();
    }
    assert_eq!(cancel_wins + claim_wins, 24);
}
