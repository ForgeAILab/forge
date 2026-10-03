use db::{create_sqlite_pool, run_migrations_from, FailureState, SqliteDb, WorkItem, WorkerHealth};
use std::{path::PathBuf, sync::Arc};

#[tokio::test]
async fn audit2_metadata_upgrade_preserves_dead_letters_and_resets_transient_strikes() {
    let full = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"));
    let base = tempfile::tempdir().unwrap();
    for entry in std::fs::read_dir(&full).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().into_string().unwrap();
        if name.as_str() < "V202610021051" {
            std::fs::copy(entry.path(), base.path().join(name)).unwrap();
        }
    }
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations_from(&pool, base.path()).await.unwrap();
    sqlx::query("INSERT INTO worker_dead_letter (worker_name, source_key, item_type, attempts, last_error, first_failed_at, last_failed_at, dead_lettered_at) VALUES ('old-worker', '12', 'test', 8, 'old failure', '2000-01-01T00:00:00Z', '2000-01-01T00:00:00Z', '2000-01-01T00:00:00Z')").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO worker_item_failure (worker_name, source_key, attempts, first_failed_at, last_error, error_kind, retry_not_before) VALUES ('old-worker', 'wake-retry:old-row', 7, '2000-01-01T00:00:00Z', 'busy', 'transient', '2999-01-01T00:00:00Z')").execute(&pool).await.unwrap();
    run_migrations_from(&pool, &full).await.unwrap();
    let row: (String, String, String) =
        sqlx::query_as("SELECT id, source_key, last_error FROM worker_dead_letter")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!row.0.is_empty());
    assert_eq!(
        (&row.1, &row.2),
        (&"12".to_owned(), &"old failure".to_owned())
    );
    let counters: (i64, i64) =
        sqlx::query_as("SELECT attempts, transient_attempts FROM worker_item_failure")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(counters, (0, 7));
    run_migrations_from(&pool, &full).await.unwrap();
    let db = Arc::new(SqliteDb::new(pool));
    let health = WorkerHealth::new(Arc::clone(&db), "old-worker");
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    health
        .dead_letter_in_tx(
            &mut tx,
            WorkItem {
                source_key: "12",
                item_type: "test",
            },
            FailureState {
                attempts: 8,
                first_failed_at: "2000-01-01T00:00:00Z",
            },
            "replayed failure",
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let again: (String, String, String) =
        sqlx::query_as("SELECT id, source_key, last_error FROM worker_dead_letter")
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(row.0, again.0, "identity survives repeated quarantine");
    assert_eq!(row.1, again.1);
    assert_eq!(again.2, "replayed failure");
    let updated = db.get_dead_letter(&row.0).await.unwrap();
    assert_eq!(updated.version, 1);
    assert_eq!(updated.attempts, 9);
}
