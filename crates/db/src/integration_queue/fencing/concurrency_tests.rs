//! Test concurrent claimants installing one durable integration owner fence.

use super::*;
use crate::integration_queue::tests::{admission, seed};
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_real_claimants_install_exactly_one_durable_owner_fence() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("fences.sqlite").display()
    );
    let pool = crate::create_sqlite_pool(&url).await.unwrap();
    crate::run_migrations(&pool).await.unwrap();
    seed(&pool).await;
    let db = std::sync::Arc::new(SqliteDb::new(pool));
    let q = db
        .create_or_get_integration_queue("r", "main")
        .await
        .unwrap();
    let a = db
        .admit_integration_attempt(admission(&q, "a", "race"))
        .await
        .unwrap();
    let q = db.integration_queue(&q.id).await.unwrap().unwrap();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
    let run = |owner: &'static str| {
        let db = db.clone();
        let q = q.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            db.claim_integration_queue(
                &q.id,
                q.revision,
                owner,
                "2026-10-08T00:00:00Z",
                "2099-01-01T00:00:00Z",
            )
            .await
        })
    };
    let first = run("first");
    let second = run("second");
    let first = first.await.unwrap();
    let second = second.await.unwrap();
    assert_ne!(first.is_ok(), second.is_ok());
    let winner = match (first, second) {
        (Ok(q), Err(DbError::VersionConflict)) | (Err(DbError::VersionConflict), Ok(q)) => q,
        other => panic!("unexpected race outcome: {other:?}"),
    };
    let fence = db.integration_owner_fence(&a.id).await.unwrap().unwrap();
    assert_eq!(fence.generation, 1);
    assert_eq!(Some(fence.lease_owner), winner.lease_owner);
    assert_eq!(fence.target_owner, winner.target_owner_json.unwrap());
}
