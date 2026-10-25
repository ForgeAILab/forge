//! Integration activation schema, state graphs, and timing serialization.

use super::*;

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
