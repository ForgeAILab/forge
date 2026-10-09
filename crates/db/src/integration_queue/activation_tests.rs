use super::activation::{
    CANCEL_REQUESTED_SQL, CLAIMABLE_QUEUES_SQL, DUE_PARKED_SQL, EXPIRED_HEADS_SQL, PRUNE_SQL,
    TIMING_SAMPLES_SQL,
};
use super::tests::{admission, fixture, legacy_digest, seed, FIXED_TIME};
use super::*;
use crate::{create_sqlite_pool, run_migrations};
use std::sync::Arc;

const NEW_MIGRATION: &str = "V202610091612__integration_activation.sql";
const NOW: &str = "2026-10-08T00:00:00Z";
const LATER: &str = "2026-10-08T00:01:00Z";
const FAR: &str = "2099-01-01T00:00:00Z";

async fn add_task(db: &SqliteDb, id: &str) {
    sqlx::query("INSERT INTO task(id,project_id,title,status,review_passed_at,created_at,updated_at) VALUES(?,'p',?,'merging',?,?,?)").bind(id).bind(id).bind(FIXED_TIME).bind(FIXED_TIME).bind(FIXED_TIME).execute(db.pool()).await.unwrap();
}
async fn main_queue(db: &SqliteDb) -> IntegrationQueue {
    db.create_or_get_integration_queue("r", "main")
        .await
        .unwrap()
}
async fn admit(db: &SqliteDb, q: &IntegrationQueue, task: &str) -> IntegrationAttempt {
    db.admit_integration_attempt(admission(q, task, &format!("entry-{task}")))
        .await
        .unwrap()
}
async fn claim(db: &SqliteDb, queue_id: &str, now: &str, until: &str) -> Result<IntegrationQueue> {
    let q = db.integration_queue(queue_id).await.unwrap().unwrap();
    db.claim_integration_queue(queue_id, q.revision, "worker", now, until)
        .await
}
async fn reread(db: &SqliteDb, id: &str) -> IntegrationAttempt {
    db.integration_attempt(id).await.unwrap().unwrap()
}
async fn exec(db: &SqliteDb, sql: &str) {
    sqlx::query(sql).execute(db.pool()).await.unwrap();
}
/// Every table access in the plan goes through an index, and `index` is one.
async fn assert_plan_uses(db: &SqliteDb, sql: &str, index: &str) {
    let rows = sqlx::query(&format!("EXPLAIN QUERY PLAN {sql}"))
        .fetch_all(db.pool())
        .await
        .unwrap();
    let plan: Vec<String> = rows
        .iter()
        .map(|row| row.try_get::<String, _>("detail").unwrap())
        .collect();
    assert!(
        plan.iter().any(|line| line.contains(index)),
        "{index} unused: {plan:?}"
    );
    for line in &plan {
        if (line.starts_with("SCAN") || line.starts_with("SEARCH"))
            && !line.contains("VIRTUAL TABLE")
        {
            assert!(line.contains("USING"), "unindexed access: {plan:?}");
        }
    }
}

#[test]
fn queue_graph_is_total_and_cancel_is_requestable_exactly_where_the_design_says() {
    let mut seen = std::collections::HashSet::new();
    for (state, exits) in INTEGRATION_QUEUE_TRANSITIONS {
        assert!(
            seen.insert(state.to_string()),
            "duplicate queue row {state}"
        );
        assert_eq!(
            exits.is_empty(),
            *state == IntegrationQueueState::Closed,
            "queue state {state}"
        );
        assert!(exits.iter().all(|exit| exit != state));
    }
    assert_eq!(seen.len(), IntegrationQueueState::ALL.len());
    // A quarantined queue leaves only through the witnessed re-open.
    assert!(!IntegrationQueueState::Suspended
        .exits()
        .contains(&IntegrationQueueState::Quarantined));

    use IntegrationAttemptState::*;
    let requestable: Vec<_> = IntegrationAttemptState::ALL
        .iter()
        .copied()
        .filter(|state| state.cancel_requestable())
        .collect();
    assert_eq!(
        requestable,
        vec![
            Queued,
            PathWait,
            Validating,
            Rebasing,
            Checking,
            AwaitingTaskStep,
            ReadyFf,
            Ejected,
            NeedsReview,
            Parked
        ]
    );
    for refused in [
        FfInflight,
        Reconciling,
        Applied,
        Quarantined,
        Completed,
        Cancelled,
        Superseded,
    ] {
        assert!(!refused.cancel_requestable(), "{refused}");
    }
    // Proven not landed: back to validation without a park in between.
    assert!(Reconciling.exits().contains(&Queued));
    // Unchanged target: straight to the Task-step authorization, no rebase.
    assert!(Validating.exits().contains(&AwaitingTaskStep));
    // Every non-terminal state still has an exit after the additions.
    for state in IntegrationAttemptState::ALL {
        assert_eq!(state.exits().is_empty(), state.terminal(), "{state}");
    }
}

#[test]
fn timing_enums_and_document_round_trip_and_reject_unknown_values() {
    for kind in IntegrationLostRaceKind::ALL {
        assert_eq!(
            kind.to_string().parse::<IntegrationLostRaceKind>().unwrap(),
            *kind
        );
    }
    for reason in IntegrationCiSkipReason::ALL {
        assert_eq!(
            reason
                .to_string()
                .parse::<IntegrationCiSkipReason>()
                .unwrap(),
            *reason
        );
    }
    assert!("future".parse::<IntegrationLostRaceKind>().is_err());
    assert!("future".parse::<IntegrationCiSkipReason>().is_err());
    let timings = IntegrationPhaseTimings {
        rounds: 2,
        queued_ms: Some(1200),
        rebase_ms: Some(30),
        check: Some(IntegrationCheckTiming::Skipped {
            reason: IntegrationCiSkipReason::TargetUnchanged,
        }),
        ff_ms: Some(5),
        lost_races: vec![
            IntegrationLostRace {
                kind: IntegrationLostRaceKind::QueueMember,
                round: 1,
                at: NOW.into(),
            },
            IntegrationLostRace {
                kind: IntegrationLostRaceKind::External,
                round: 2,
                at: LATER.into(),
            },
        ],
        ..Default::default()
    };
    let json = serde_json::to_value(&timings).unwrap();
    assert_eq!(
        json["check"],
        serde_json::json!({"kind":"skipped","reason":"target_unchanged"})
    );
    assert_eq!(json["lost_races"][1]["kind"], "external");
    assert_eq!(
        serde_json::from_value::<IntegrationPhaseTimings>(json).unwrap(),
        timings
    );
    assert_eq!(timings.external_target_moves(), 1);
    assert!(serde_json::from_value::<IntegrationPhaseTimings>(
        serde_json::json!({"check":{"kind":"skipped","reason":"future"}})
    )
    .is_err());
}

#[tokio::test]
async fn activation_migration_keeps_rows_in_every_state_and_defaults_the_new_columns() {
    let dir = tempfile::tempdir().unwrap();
    let source = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"));
    for file in std::fs::read_dir(source).unwrap() {
        let file = file.unwrap();
        if file.file_name().to_str().unwrap() != NEW_MIGRATION {
            std::fs::copy(file.path(), dir.path().join(file.file_name())).unwrap();
        }
    }
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    crate::run_migrations_from(&pool, dir.path()).await.unwrap();
    seed(&pool).await;
    // One queue per queue state and one attempt per attempt state, written
    // with the pre-upgrade columns only.
    for state in IntegrationQueueState::ALL {
        sqlx::query("INSERT INTO integration_queue(id,repo_id,target_branch,target_location_id,target_owner_json,state,created_at,updated_at,last_error_kind,last_error) VALUES(?,'r',?,'l','{\"location_id\":\"l\"}',?,?,?,'timeout','kept')")
            .bind(format!("queue-{state}")).bind(format!("branch-{state}")).bind(state.to_string()).bind(FIXED_TIME).bind(FIXED_TIME).execute(&pool).await.unwrap();
    }
    for (seq, state) in IntegrationAttemptState::ALL.iter().enumerate() {
        sqlx::query("INSERT INTO integration_attempt(id,queue_id,task_ref,project_ref,queue_seq,current,admission_key,expected_status,expected_epoch,observed_task_version,enqueued_at,state,available_at,completed_at,candidate_sha,effect_receipts_json,observations_json,updated_at,created_at) VALUES(?,'queue-open',?,'p',?,?,?,'merging',0,1,?,?,?,?,'candidate','[{\"kept\":true}]','[{\"kept\":true}]',?,?)")
            .bind(format!("attempt-{state}")).bind(format!("task-{state}")).bind(seq as i64 + 1).bind(!state.terminal()).bind(format!("entry-{state}")).bind(FIXED_TIME).bind(state.to_string()).bind(FIXED_TIME).bind(state.terminal().then_some(FIXED_TIME)).bind(FIXED_TIME).bind(FIXED_TIME).execute(&pool).await.unwrap();
    }
    async fn rows(pool: &crate::SqlitePool, table: &str) -> Vec<String> {
        let columns: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT name FROM pragma_table_info('{table}') WHERE name NOT IN ('cancel_requested_at','phase_timings_json') ORDER BY cid"
        ))
        .fetch_all(pool)
        .await
        .unwrap();
        let select = columns
            .iter()
            .map(|column| format!("quote(\"{column}\")"))
            .collect::<Vec<_>>()
            .join("||'|'||");
        sqlx::query_scalar(&format!("SELECT {select} FROM {table} ORDER BY id"))
            .fetch_all(pool)
            .await
            .unwrap()
    }
    let legacy = legacy_digest(&pool, true).await;
    let queues = rows(&pool, "integration_queue").await;
    let attempts = rows(&pool, "integration_attempt").await;
    assert_eq!((queues.len(), attempts.len()), (4, 17));
    let applied: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _migration")
        .fetch_one(&pool)
        .await
        .unwrap();

    run_migrations(&pool).await.unwrap();

    assert_eq!(legacy, legacy_digest(&pool, true).await);
    assert_eq!(queues, rows(&pool, "integration_queue").await);
    assert_eq!(attempts, rows(&pool, "integration_attempt").await);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _migration")
            .fetch_one(&pool)
            .await
            .unwrap(),
        applied + 1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM integration_attempt WHERE cancel_requested_at IS NOT NULL OR phase_timings_json IS NOT NULL")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    let db = SqliteDb::new(pool);
    for state in IntegrationAttemptState::ALL {
        let a = reread(&db, &format!("attempt-{state}")).await;
        assert_eq!(a.state, *state);
        assert_eq!((a.cancel_requested_at, a.phase_timings), (None, None));
    }
    let indexes: Vec<String> = sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type='index' AND name IN ('integration_attempt_timed','integration_attempt_cancel_requested','integration_attempt_prunable') ORDER BY name")
        .fetch_all(db.pool())
        .await
        .unwrap();
    assert_eq!(indexes.len(), 3);
    let triggers: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND tbl_name IN ('integration_queue','integration_attempt')")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(triggers, 0);
    // Replay is a no-op.
    let migrated = legacy_digest(db.pool(), false).await;
    run_migrations(db.pool()).await.unwrap();
    assert_eq!(migrated, legacy_digest(db.pool(), false).await);
    assert_eq!(attempts, rows(db.pool(), "integration_attempt").await);
}

#[tokio::test]
async fn fresh_database_has_the_activation_columns_and_bounds_the_timings_document() {
    let db = fixture().await;
    let q = main_queue(&db).await;
    let a = admit(&db, &q, "a").await;
    assert_eq!((a.cancel_requested_at, a.phase_timings), (None, None));
    for invalid in ["[]", "not json"] {
        assert!(
            sqlx::query("UPDATE integration_attempt SET phase_timings_json=?")
                .bind(invalid)
                .execute(db.pool())
                .await
                .is_err(),
            "{invalid}"
        );
    }
    let oversized = serde_json::json!({"pad":"x".repeat(16384)}).to_string();
    assert!(
        sqlx::query("UPDATE integration_attempt SET phase_timings_json=?")
            .bind(oversized)
            .execute(db.pool())
            .await
            .is_err()
    );
    let mut fabricated = admission(&q, "b", "fabricated");
    fabricated.cancel_requested_at = Some(NOW.into());
    assert!(matches!(
        db.admit_integration_attempt(fabricated).await,
        Err(DbError::Check(_))
    ));
}

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

fn effect(fence: IntegrationOwnerFence) -> IntegrationEffectRequest {
    IntegrationEffectRequest {
        fence,
        kind: IntegrationOperationKind::FastForward,
        witness: serde_json::json!({"workspace":"frozen","head":"head","target":"target"}),
    }
}
/// A claimed head whose fast-forward reply was lost: an uncertain receipt.
async fn uncertain_head(db: &SqliteDb) -> (IntegrationQueue, IntegrationEffectRequest) {
    let q = main_queue(db).await;
    let a = admit(db, &q, "a").await;
    let q = claim(db, &q.id, NOW, FAR).await.unwrap();
    let request = effect(db.integration_owner_fence(&a.id).await.unwrap().unwrap());
    let IntegrationEffectAdmission::Started(mut guard) =
        db.begin_integration_effect(request.clone()).await.unwrap()
    else {
        panic!("effect not admitted");
    };
    assert!(guard.start().await.unwrap().is_none());
    guard
        .record(
            serde_json::json!({"kind":"infrastructure"}),
            IntegrationOperationState::Uncertain,
        )
        .await
        .unwrap();
    (q, request)
}

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
