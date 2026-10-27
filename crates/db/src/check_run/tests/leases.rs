//! Leases repository tests.
use super::*;

#[tokio::test]
async fn request_is_idempotent_and_conflicting_request_keys_are_refused() {
    let db = fixture().await;
    let first = db.request_check_run(request("one")).await.unwrap();
    assert!(matches!(
        first.disposition,
        CheckRequestDisposition::Scheduled
    ));
    let replay = db.request_check_run(request("one")).await.unwrap();
    assert_eq!(replay.consumer.id, first.consumer.id);
    assert!(matches!(
        replay.disposition,
        CheckRequestDisposition::Idempotent
    ));
    let mut changed = request("one");
    changed.identity.commit_sha = "b".repeat(40);
    assert!(matches!(
        db.request_check_run(changed).await,
        Err(DbError::IdempotencyConflict)
    ));
    let joined = db.request_check_run(request("two")).await.unwrap();
    assert_eq!(joined.consumer.run_id, first.consumer.run_id);
    assert!(matches!(
        joined.disposition,
        CheckRequestDisposition::Joined
    ));
}

#[tokio::test]
async fn lease_takeover_renews_generation_and_fences_stale_finish() {
    let db = fixture().await;
    let response = db.request_check_run(request("one")).await.unwrap();
    let id = response.consumer.run_id.unwrap();
    let first = db
        .claim_check_run(&id, 1, "first", NOW, UNTIL)
        .await
        .unwrap();
    let takeover = db
        .claim_check_run(
            &id,
            first.version,
            "second",
            "2026-10-08T00:01:01Z",
            "2026-10-08T00:02:00Z",
        )
        .await
        .unwrap();
    assert_eq!(takeover.lease_generation, first.lease_generation + 1);
    assert_eq!(takeover.operation_id, first.operation_id);
    assert_eq!(takeover.state, CheckRunState::Uncertain);
    assert!(matches!(
        db.renew_check_run(&fence(&first), NOW, UNTIL).await,
        Err(DbError::VersionConflict)
    ));
    assert!(matches!(
        db.finish_check_run(
            &fence(&first),
            evidence(CheckResultOutcome::Pass, CheckCleanup::Success),
            NOW
        )
        .await,
        Err(DbError::VersionConflict)
    ));
    let renewed = db
        .renew_check_run(
            &fence(&takeover),
            "2026-10-08T00:01:02Z",
            "2026-10-08T00:03:00Z",
        )
        .await
        .unwrap();
    assert_eq!(renewed.lease_generation, takeover.lease_generation);
    assert_eq!(renewed.version, takeover.version + 1);
}

#[tokio::test]
async fn uncertain_run_blocks_duplicate_and_reconciliation_claim_never_relaunches_it() {
    let db = fixture().await;
    let run = cleaning(&db, request("one")).await;
    let uncertain = db
        .mark_check_run_uncertain(&fence(&run), NOW)
        .await
        .unwrap();
    let joined = db.request_check_run(request("two")).await.unwrap();
    assert!(matches!(
        joined.disposition,
        CheckRequestDisposition::Joined
    ));
    assert_eq!(
        joined.consumer.run_id.as_deref(),
        Some(uncertain.id.as_str())
    );
    assert!(db
        .reusable_check_result(&identity())
        .await
        .unwrap()
        .is_none());
    let reconciler = db
        .claim_check_run(
            &uncertain.id,
            uncertain.version,
            "reconciler",
            "2026-10-08T00:01:01Z",
            "2026-10-08T00:02:00Z",
        )
        .await
        .unwrap();
    assert_eq!(reconciler.state, CheckRunState::Uncertain);
    assert_eq!(reconciler.operation_id, run.operation_id);
    assert_eq!(reconciler.lease_generation, uncertain.lease_generation + 1);
}

#[tokio::test]
async fn taking_over_an_expired_lease_makes_a_dispatched_run_uncertain() {
    for state in [
        CheckRunState::Running,
        CheckRunState::Cancelling,
        CheckRunState::Cleaning,
    ] {
        let db = fixture().await;
        let lost = run_in(&db, state).await;
        // An unexpired lease is not taken over at all.
        assert!(matches!(
            db.claim_check_run(&lost.id, lost.version, "second", NOW, UNTIL)
                .await,
            Err(DbError::VersionConflict)
        ));
        let taken = db
            .claim_check_run(&lost.id, lost.version, "second", AFTER, AFTER_UNTIL)
            .await
            .unwrap();
        assert_eq!(taken.state, CheckRunState::Uncertain, "{state}");
        assert_eq!(taken.lease_owner.as_deref(), Some("second"));
        assert_eq!(taken.operation_id, lost.operation_id);
        assert_eq!(taken.finished_at, None);
        // The lost owner is fenced out; the new one cannot continue the run.
        assert!(matches!(
            db.transition_check_run(&fence(&lost), CheckRunState::Cleaning, AFTER)
                .await,
            Err(DbError::VersionConflict)
        ));
        assert!(matches!(
            db.transition_check_run(&fence(&taken), CheckRunState::Running, AFTER)
                .await,
            Err(DbError::InvalidTransition)
        ));
        assert!(matches!(
            db.finish_check_run(
                &fence(&taken),
                evidence(CheckResultOutcome::Pass, CheckCleanup::Success),
                AFTER
            )
            .await,
            Err(DbError::InvalidTransition)
        ));
        // It still occupies the identity: nobody launches a duplicate.
        assert!(matches!(
            db.request_check_run(request("late"))
                .await
                .unwrap()
                .disposition,
            CheckRequestDisposition::Joined
        ));
        // The only exits are stage A's: reconcile with evidence, or cancel.
        let reconciling = db
            .transition_check_run(&fence(&taken), CheckRunState::Cleaning, AFTER)
            .await
            .unwrap();
        let settled = db
            .finish_check_run(
                &fence(&reconciling),
                evidence(CheckResultOutcome::Pass, CheckCleanup::Success),
                AFTER,
            )
            .await
            .unwrap();
        assert!(settled.certified);
    }
    // Nobody started a queued run: claiming it simply starts it.
    let db = fixture().await;
    let queued = run_in(&db, CheckRunState::Queued).await;
    let started = db
        .claim_check_run(&queued.id, queued.version, "second", AFTER, AFTER_UNTIL)
        .await
        .unwrap();
    assert_eq!(started.state, CheckRunState::Running);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_backed_concurrent_requests_and_claims_have_one_live_run() {
    let dir = tempfile::tempdir().unwrap();
    let pool = create_sqlite_pool(&format!(
        "sqlite://{}",
        dir.path().join("checks.db").display()
    ))
    .await
    .unwrap();
    run_migrations(&pool).await.unwrap();
    let db = Arc::new(SqliteDb::new(pool));
    seed(&db).await;
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut tasks = Vec::new();
    for n in 0..8 {
        let db = db.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            db.request_check_run(request(&format!("concurrent-{n}")))
                .await
                .unwrap()
        }));
    }
    let mut run_ids = std::collections::BTreeSet::new();
    let mut scheduled = 0;
    for task in tasks {
        let response = task.await.unwrap();
        run_ids.insert(response.consumer.run_id.unwrap());
        scheduled += usize::from(matches!(
            response.disposition,
            CheckRequestDisposition::Scheduled
        ));
    }
    assert_eq!(run_ids.len(), 1);
    assert_eq!(scheduled, 1);
    let id = run_ids.first().unwrap().clone();
    let mut claims = Vec::new();
    let barrier = Arc::new(tokio::sync::Barrier::new(4));
    for n in 0..4 {
        let db = db.clone();
        let id = id.clone();
        let barrier = barrier.clone();
        claims.push(tokio::spawn(async move {
            barrier.wait().await;
            db.claim_check_run(&id, 1, &format!("worker-{n}"), NOW, UNTIL)
                .await
        }));
    }
    let mut wins = 0;
    for claim in claims {
        match claim.await.unwrap() {
            Ok(_) => wins += 1,
            Err(DbError::VersionConflict) => {}
            other => panic!("unexpected claim {other:?}"),
        }
    }
    assert_eq!(wins, 1);
    db.pool().close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_takeover_of_an_expired_running_lease_has_one_winner_and_is_uncertain() {
    let dir = tempfile::tempdir().unwrap();
    let pool = create_sqlite_pool(&format!(
        "sqlite://{}",
        dir.path().join("takeover.db").display()
    ))
    .await
    .unwrap();
    run_migrations(&pool).await.unwrap();
    let db = Arc::new(SqliteDb::new(pool));
    seed(&db).await;
    let running = run_in(&db, CheckRunState::Running).await;
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut claims = Vec::new();
    for n in 0..8 {
        let (db, barrier, id) = (db.clone(), barrier.clone(), running.id.clone());
        claims.push(tokio::spawn(async move {
            barrier.wait().await;
            db.claim_check_run(
                &id,
                running.version,
                &format!("taker-{n}"),
                AFTER,
                AFTER_UNTIL,
            )
            .await
        }));
    }
    let mut winners = Vec::new();
    for claim in claims {
        match claim.await.unwrap() {
            Ok(run) => winners.push(run),
            Err(DbError::VersionConflict) => {}
            other => panic!("unexpected takeover {other:?}"),
        }
    }
    assert_eq!(winners.len(), 1);
    let stored = db.check_run(&running.id).await.unwrap().unwrap();
    assert_eq!(winners[0].state, CheckRunState::Uncertain);
    assert_eq!(stored.state, CheckRunState::Uncertain);
    assert_eq!(stored.lease_owner, winners[0].lease_owner);
    assert_eq!(stored.lease_generation, running.lease_generation + 1);
    assert_eq!(stored.version, running.version + 1);
    db.pool().close().await;
}

const AFTER: &str = "2026-10-08T00:01:01Z";

const AFTER_UNTIL: &str = "2026-10-08T00:02:00Z";
