//! Legacy journal import state and crash recovery.

use super::*;

#[tokio::test]
async fn legacy_writes_refuse_and_baseline_keeps_two_blobs() {
    use sqlx::Row;
    let id = SessionId::new("legacy-baseline");
    let (f, store) = fixture(&id).await;
    let mut snapshot: SessionSnapshot =
        serde_json::from_value(serde_json::json!({"id":id,"history":[],"updated":1})).unwrap();
    snapshot.history.push(Message::user("accepted"));
    let cp = TurnCheckpoint::accepted(
        TurnId::new("baseline"),
        UserInput::text("accepted"),
        snapshot.clone(),
        0,
        Deadline::never(),
        1,
        0,
        Timestamp(1),
    )
    .unwrap();
    for result in [
        SessionStore::save(&*store, &snapshot).await,
        CheckpointStore::save(&*store, &cp).await,
    ] {
        let error = result.unwrap_err();
        assert_eq!(error.kind, agent_runtime::core::error::ErrorKind::Conflict);
        assert_eq!(error.message, "legacy session store is read-only");
    }
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM protected_agent_session_state")
        .fetch_one(f.database().pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    // Exact former ordering: checkpoint clears the standalone payload; unchanged
    // snapshots skip by digest; a later canonical boundary/cache change writes
    // the independently sealed ordinary copy, producing the real idle L2 pair.
    store
        .save_legacy_snapshot_baseline(&snapshot)
        .await
        .unwrap();
    store.save_legacy_checkpoint_baseline(&cp).await.unwrap();
    store
        .save_legacy_snapshot_baseline(&snapshot)
        .await
        .unwrap();
    let one: bool = sqlx::query_scalar("SELECT snapshot_ciphertext IS NULL AND checkpoint_ciphertext IS NOT NULL FROM protected_agent_session_state")
        .fetch_one(f.database().pool()).await.unwrap();
    assert!(one);
    snapshot.extension_state.insert(
        "host.restore".into(),
        VersionedSessionState::new(
            RegistryRevision::new("idle"),
            serde_json::json!({"cache":"idle state"}),
        ),
    );
    store
        .save_legacy_snapshot_baseline(&snapshot)
        .await
        .unwrap();
    let row = sqlx::query("SELECT snapshot_ciphertext,checkpoint_ciphertext,quote(snapshot_ciphertext) AS snapshot,quote(checkpoint_ciphertext) AS checkpoint,state_revision FROM protected_agent_session_state")
        .fetch_one(f.database().pool()).await.unwrap();
    let ordinary: Vec<u8> = row.get("snapshot_ciphertext");
    let protected: Vec<u8> = row.get("checkpoint_ciphertext");
    assert!(!ordinary.is_empty() && !protected.is_empty());
    assert_ne!(ordinary, protected);
    assert_eq!(
        SessionStore::load(&*store, &id).await.unwrap(),
        Some(snapshot.clone())
    );
    assert_eq!(
        CheckpointStore::load_latest(&*store, &id).await.unwrap(),
        Some(cp.clone())
    );
    let before: (String, String, i64) = (
        row.get("snapshot"),
        row.get("checkpoint"),
        row.get("state_revision"),
    );
    sqlx::query("CREATE TRIGGER refuse_legacy_update BEFORE UPDATE ON protected_agent_session_state BEGIN SELECT RAISE(ABORT,'legacy write'); END")
        .execute(f.database().pool()).await.unwrap();
    snapshot.updated = Timestamp(999);
    store
        .save_legacy_snapshot_baseline(&snapshot)
        .await
        .unwrap();
    assert!(SessionStore::save(&*store, &snapshot).await.is_err());
    assert!(CheckpointStore::save(&*store, &cp).await.is_err());
    let after: (String,String,i64) = sqlx::query_as("SELECT quote(snapshot_ciphertext),quote(checkpoint_ciphertext),state_revision FROM protected_agent_session_state")
        .fetch_one(f.database().pool()).await.unwrap();
    assert_eq!(
        before, after,
        "refusals and digest skips leave both sealed blobs byte-exact"
    );
}

#[tokio::test]
async fn legacy_import_state_matrix() {
    use import_matrix::*;
    use std::sync::atomic::Ordering;
    let cases = cases().await;
    assert_eq!(cases.len(), 9);
    for case in cases {
        let (f, store) = fixture(&case.snapshot.id).await;
        seed(&store, &case).await;
        audit_publications(&f).await;
        let before = legacy_row(&f).await;
        let stores = Stores::new(store.clone());
        let io = Arc::new(IoCounts::default());
        let (rt, s, p) = deferred(stores.clone(), &case, io.clone()).await;
        assert_import(&store, &case).await;
        assert!(p.requests().is_empty());
        assert_eq!(stores.loads.load(Ordering::SeqCst), 2);
        let order: Vec<String> =
            sqlx::query_scalar("SELECT role FROM import_publication_order ORDER BY n")
                .fetch_all(f.database().pool())
                .await
                .unwrap();
        assert_eq!(
            order,
            if case.checkpoint.is_some() {
                vec!["protected", "ordinary"]
            } else {
                vec!["ordinary"]
            },
            "{} exact-first",
            case.label
        );
        drop(s);
        drop(rt);
        let loads = stores.loads.load(Ordering::SeqCst);
        let (rt, s, p) = deferred(stores.clone(), &case, io.clone()).await;
        assert_import(&store, &case).await;
        assert_eq!(
            stores.loads.load(Ordering::SeqCst),
            loads,
            "{} R5",
            case.label
        );
        assert_eq!(stores.saves.load(Ordering::SeqCst), 0);
        assert!(p.requests().is_empty());
        assert_eq!(io.tools.load(Ordering::SeqCst), 0);
        assert_eq!(io.interactions.load(Ordering::SeqCst), 0);
        assert_eq!(legacy_row(&f).await, before);
        println!(
            "IMPORT {} exact={} ordinary=projected second_legacy_loads=0 provider=0 tool=0 interaction=0 legacy=byte-unchanged",
            case.label,
            case.checkpoint.is_some()
        );
        drop(s);
        drop(rt);
    }
}

#[tokio::test]
async fn import_and_commit_crash_cuts() {
    let imports = crash_matrix::import_cuts().await;
    crash_matrix::stale_legacy_is_not_overlaid().await;
    let turns = crash_matrix::turn_cuts().await;
    assert_eq!(imports, 119);
    println!(
        "CRASH MATRIX import={imports} turn={turns} cuts={}",
        JournalCommitFailurePoint::ALL.len()
    );
}
