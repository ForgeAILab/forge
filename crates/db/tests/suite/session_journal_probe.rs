#[path = "../support/journal_probe.rs"]
mod support;

#[tokio::test]
async fn publication_and_checkpoint_flags() {
    let db = support::fixture().await;
    let flags = db.session_persistence_flags("missing").await.unwrap();
    assert!(!flags.saved && !flags.checkpoint_present && flags.legacy_eligible);
    let flags = db.session_persistence_flags("probe-session").await.unwrap();
    assert!(!flags.saved && !flags.retired);
    support::native(&db, "ordinary", false, false, false).await;
    support::native(&db, "protected", false, false, false).await;
    assert!(
        !db.session_persistence_flags("probe-session")
            .await
            .unwrap()
            .saved
    );
    support::legacy(&db, false).await;
    let flags = db.session_persistence_flags("probe-session").await.unwrap();
    assert!(flags.saved && flags.legacy_eligible && !flags.checkpoint_present);
    support::legacy(&db, true).await;
    assert!(
        db.session_persistence_flags("probe-session")
            .await
            .unwrap()
            .checkpoint_present
    );
    support::native(&db, "ordinary", true, false, false).await;
    let flags = db.session_persistence_flags("probe-session").await.unwrap();
    assert!(flags.saved && flags.ordinary_published && !flags.protected_published);
    assert!(
        !flags.legacy_eligible && !flags.checkpoint_present,
        "native authority suppresses stale legacy checkpoint"
    );
    support::native(&db, "protected", true, true, false).await;
    assert!(
        db.session_persistence_flags("probe-session")
            .await
            .unwrap()
            .checkpoint_present
    );
    support::native(&db, "protected", true, false, false).await;
    assert!(
        !db.session_persistence_flags("probe-session")
            .await
            .unwrap()
            .checkpoint_present
    );
    support::native(&db, "protected", true, false, true).await;
    let flags = db.session_persistence_flags("probe-session").await.unwrap();
    assert!(flags.retired && !flags.saved && !flags.checkpoint_present && !flags.legacy_eligible);
    // Retirement of the other domain also fences fallback and a surviving exact head.
    support::native(&db, "protected", true, true, false).await;
    support::native(&db, "ordinary", true, false, true).await;
    let flags = db.session_persistence_flags("probe-session").await.unwrap();
    assert!(flags.retired && !flags.saved && !flags.checkpoint_present);
    sqlx::query("DELETE FROM agent_session_journal_session")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE protected_agent_session_state SET checkpoint_ciphertext=NULL,checkpoint_nonce=NULL,snapshot_ciphertext=NULL,snapshot_nonce=NULL")
        .execute(db.pool()).await.unwrap();
    let flags = db.session_persistence_flags("probe-session").await.unwrap();
    assert!(
        !flags.saved && !flags.checkpoint_present,
        "turn id alone is not durable payload"
    );
}
