use db::{create_sqlite_pool, run_migrations_from};

#[tokio::test]
async fn resolution_migration_preserves_existing_dead_letter_identity_and_failure() {
    let migrations = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    let before = tempfile::tempdir().unwrap();
    for entry in std::fs::read_dir(&migrations).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name().to_string_lossy().as_ref() < "V202610030300" {
            std::fs::copy(entry.path(), before.path().join(entry.file_name())).unwrap();
        }
    }
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations_from(&pool, before.path()).await.unwrap();
    sqlx::query("INSERT INTO worker_dead_letter (id, worker_name, source_key, item_type, attempts, last_error, first_failed_at, last_failed_at, dead_lettered_at) VALUES ('existing-id', 'consumer', '42', 'test', 8, 'original error', '2000', '2001', '2002')").execute(&pool).await.unwrap();
    run_migrations_from(&pool, &migrations).await.unwrap();
    run_migrations_from(&pool, &migrations).await.unwrap();
    let db = db::SqliteDb::new(pool);
    let row = db.get_dead_letter("existing-id").await.unwrap();
    assert_eq!(row.source_key, "42");
    assert_eq!(row.attempts, 8);
    assert_eq!(row.last_error, "original error");
    assert_eq!(row.first_failed_at, "2000");
    assert_eq!(row.last_failed_at, "2001");
    assert_eq!(row.dead_lettered_at, "2002");
    assert_eq!(row.version, 0);
    assert!(row.resolved_at.is_none());
    assert!(row.resolved_by.is_none());
    assert_eq!(
        db.worker_dead_letter_history("consumer").await.unwrap().0,
        1
    );
}
