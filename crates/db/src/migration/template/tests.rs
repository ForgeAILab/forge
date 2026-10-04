use super::*;
use crate::{create_sqlite_pool, run_migrations, run_migrations_from};
use sqlx::Row;

async fn assert_equivalent(replayed: &SqlitePool, copied: &SqlitePool) {
    // Include root pages and exact SQL text, not just the object names.
    type SchemaRow = (String, String, String, i64, Option<String>);
    let schema_sql = "SELECT type, name, tbl_name, rootpage, sql
                      FROM sqlite_master ORDER BY type, name";
    let expected: Vec<SchemaRow> = sqlx::query_as(schema_sql)
        .fetch_all(replayed)
        .await
        .unwrap();
    let actual: Vec<SchemaRow> = sqlx::query_as(schema_sql).fetch_all(copied).await.unwrap();
    assert_eq!(actual, expected);

    let history_sql = "SELECT version, name, applied_at FROM _migration ORDER BY version";
    let expected: Vec<(i64, String, String)> = sqlx::query_as(history_sql)
        .fetch_all(replayed)
        .await
        .unwrap();
    let actual: Vec<(i64, String, String)> =
        sqlx::query_as(history_sql).fetch_all(copied).await.unwrap();
    assert_eq!(actual, expected); // Timestamps must be copied verbatim too.

    // Also compare every table's data, including migration-created seed rows.
    for row in sqlx::query("SELECT name FROM sqlite_master WHERE type = 'table'")
        .fetch_all(replayed)
        .await
        .unwrap()
    {
        let table: String = row.get("name");
        let quoted_table = table.replace('"', "\"\"");
        let columns: Vec<(String,)> = sqlx::query_as("SELECT name FROM pragma_table_info(?)")
            .bind(&table)
            .fetch_all(replayed)
            .await
            .unwrap();
        let projection = columns
            .iter()
            .map(|(column,)| format!("quote(\"{}\")", column.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(" || ',' || ");
        let query = format!("SELECT {projection} FROM \"{quoted_table}\" ORDER BY 1");
        let expected: Vec<String> = sqlx::query_scalar(&query)
            .fetch_all(replayed)
            .await
            .unwrap();
        let actual: Vec<String> = sqlx::query_scalar(&query).fetch_all(copied).await.unwrap();
        assert_eq!(actual, expected, "seed data in {table}");
    }
    let violations = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(copied)
        .await
        .unwrap();
    assert!(violations.is_empty());
}

#[tokio::test]
async fn snapshots_match_replay_in_memory_and_on_disk() {
    let replayed = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations_replay(&replayed).await.unwrap();
    let bytes = replayed
        .acquire()
        .await
        .unwrap()
        .serialize(None)
        .await
        .unwrap()
        .to_vec();
    let memory = create_sqlite_pool("sqlite::memory:").await.unwrap();
    restore(&mut memory.acquire().await.unwrap(), Path::new(""), &bytes)
        .await
        .unwrap();
    assert_equivalent(&replayed, &memory).await;
    assert_eq!(
        memory
            .acquire()
            .await
            .unwrap()
            .serialize(None)
            .await
            .unwrap()
            .as_ref(),
        bytes.as_slice(),
        "the in-memory image is byte-for-byte identical to replay"
    );

    let dir = tempfile::tempdir_in(std::env::temp_dir()).unwrap();
    let path = dir.path().join("copy.sqlite");
    let url = format!("sqlite://{}", path.display());
    let disk = create_sqlite_pool(&url).await.unwrap();
    restore(&mut disk.acquire().await.unwrap(), &path, &bytes)
        .await
        .unwrap();
    assert_equivalent(&replayed, &disk).await;
    disk.close().await;
    let reopened = create_sqlite_pool(&url).await.unwrap();
    assert_equivalent(&replayed, &reopened).await;
    reopened.close().await;
    memory.close().await;
    replayed.close().await;
}

#[tokio::test]
async fn public_runner_reuses_the_template_and_keeps_databases_isolated() {
    let first = create_sqlite_pool("sqlite::memory:").await.unwrap();
    let second = create_sqlite_pool("sqlite::memory:").await.unwrap();
    super::super::ensure_migration_table(&second).await.unwrap();
    let (a, b) = tokio::join!(run_migrations(&first), run_migrations(&second));
    a.unwrap();
    b.unwrap();
    assert_equivalent(&first, &second).await;
    sqlx::query(
        "INSERT INTO system_setting (key, value, updated_at) VALUES ('isolated', 'yes', 'now')",
    )
    .execute(&first)
    .await
    .unwrap();
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM system_setting WHERE key = 'isolated'")
            .fetch_one(&second)
            .await
            .unwrap();
    assert_eq!(count, 0);
    first.close().await;
    second.close().await;
}

#[tokio::test]
async fn file_pool_connections_keep_wal_pragmas_and_persistent_writes() {
    let dir = tempfile::tempdir_in(std::env::temp_dir()).unwrap();
    let url = format!("sqlite://{}", dir.path().join("pool.sqlite").display());
    let pool = create_sqlite_pool(&url).await.unwrap();
    super::super::ensure_migration_table(&pool).await.unwrap();
    let mut connections = Vec::new();
    for _ in 0..4 {
        let mut connection = pool.acquire().await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _migration")
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        assert_eq!(count, 0);
        connections.push(connection);
    }
    assert!(try_restore(&pool).await.unwrap());
    for connection in &mut connections {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _migration")
            .fetch_one(&mut **connection)
            .await
            .unwrap();
        assert_eq!(count, super::super::MIGRATIONS_DIR.files().count() as i64);
        for pragma in ["foreign_keys", "recursive_triggers"] {
            let enabled: i64 = sqlx::query_scalar(&format!("PRAGMA {pragma}"))
                .fetch_one(&mut **connection)
                .await
                .unwrap();
            assert_eq!(enabled, 1);
        }
        let journal: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&mut **connection)
            .await
            .unwrap();
        assert_eq!(journal, "wal");
        let timeout: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
            .fetch_one(&mut **connection)
            .await
            .unwrap();
        assert_eq!(timeout, 30_000);
    }
    sqlx::query(
        "INSERT INTO system_setting (key, value, updated_at) VALUES ('persistent', 'yes', 'now')",
    )
    .execute(&mut *connections[0])
    .await
    .unwrap();
    drop(connections);
    pool.close().await;
    let reopened = create_sqlite_pool(&url).await.unwrap();
    assert!(!try_restore(&reopened).await.unwrap());
    run_migrations(&reopened).await.unwrap();
    let value: String =
        sqlx::query_scalar("SELECT value FROM system_setting WHERE key = 'persistent'")
            .fetch_one(&reopened)
            .await
            .unwrap();
    assert_eq!(value, "yes");
    reopened.close().await;

    // An empty tracking table in a read-only WAL file is eligible by schema,
    // but the separate backup connection must not bypass sqlx's access mode.
    let path = dir.path().join("readonly.sqlite");
    let writable_url = format!("sqlite://{}", path.display());
    let writable = create_sqlite_pool(&writable_url).await.unwrap();
    super::super::ensure_migration_table(&writable)
        .await
        .unwrap();
    writable.close().await;
    let read_only = create_sqlite_pool(&format!("{writable_url}?mode=ro"))
        .await
        .unwrap();
    let error = run_migrations(&read_only).await.unwrap_err();
    assert!(matches!(
        error,
        DbError::Sqlx(sqlx::Error::Database(ref error)) if error.code().as_deref() == Some("8")
    ));
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _migration")
        .fetch_one(&read_only)
        .await
        .unwrap();
    assert_eq!(count, 0);
    read_only.close().await;
}

#[tokio::test]
async fn existing_databases_keep_replay_and_the_applied_name_guard() {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    // Even an unrelated table with no applied migrations must survive replay.
    sqlx::raw_sql("CREATE TABLE marker(value TEXT); INSERT INTO marker VALUES ('preserved')")
        .execute(&pool)
        .await
        .unwrap();
    assert!(!try_restore(&pool).await.unwrap());
    run_migrations(&pool).await.unwrap();
    assert!(!try_restore(&pool).await.unwrap());
    let marker: String = sqlx::query_scalar("SELECT value FROM marker")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(marker, "preserved");
    sqlx::query("UPDATE _migration SET name = 'different' WHERE version = 1")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        run_migrations(&pool).await,
        Err(DbError::AppliedMigrationMismatch { version: 1, .. })
    ));
    pool.close().await;
}

#[tokio::test]
async fn custom_directories_keep_replay_and_the_duplicate_version_guard() {
    let dir = tempfile::tempdir_in(std::env::temp_dir()).unwrap();
    std::fs::write(
        dir.path().join("V001__custom.sql"),
        "CREATE TABLE custom(id TEXT)",
    )
    .unwrap();
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations_from(&pool, dir.path()).await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _migration")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    std::fs::write(
        dir.path().join("V001__duplicate.sql"),
        "CREATE TABLE duplicate(id TEXT)",
    )
    .unwrap();
    assert!(matches!(
        run_migrations_from(&pool, dir.path()).await,
        Err(DbError::DuplicateMigrationVersion { version: 1, .. })
    ));
    pool.close().await;
}

#[tokio::test]
async fn shared_memory_pools_keep_replay() {
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(format!(
            "file:shared-template-test-{}",
            crate::new_uuid_v4()
        ))
        .in_memory(true)
        .shared_cache(true);
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(2)
        .connect_with(options)
        .await
        .unwrap();
    assert!(!try_restore(&pool).await.unwrap());
    run_migrations(&pool).await.unwrap();
    let mut first = pool.acquire().await.unwrap();
    let mut second = pool.acquire().await.unwrap();
    let a: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _migration")
        .fetch_one(&mut *first)
        .await
        .unwrap();
    let b: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _migration")
        .fetch_one(&mut *second)
        .await
        .unwrap();
    assert_eq!(a, b);
    drop((first, second));
    pool.close().await;

    let query_only = create_sqlite_pool("sqlite::memory:").await.unwrap();
    sqlx::query("PRAGMA query_only = ON")
        .execute(&query_only)
        .await
        .unwrap();
    assert!(!try_restore(&query_only).await.unwrap());
    let error = run_migrations(&query_only).await.unwrap_err();
    assert!(matches!(
        error,
        DbError::Sqlx(sqlx::Error::Database(ref error)) if error.code().as_deref() == Some("8")
    ));
    query_only.close().await;
}

#[tokio::test]
async fn fresh_pools_take_the_copy_and_custom_settings_keep_replay() {
    // Guards the speed-up itself: a default pool must stay eligible.
    let memory = create_sqlite_pool("sqlite::memory:").await.unwrap();
    assert!(try_restore(&memory).await.unwrap());
    memory.close().await;

    let dir = tempfile::tempdir_in(std::env::temp_dir()).unwrap();
    let url = format!("sqlite://{}", dir.path().join("custom.sqlite").display());
    let pool = create_sqlite_pool(&url).await.unwrap();
    // A header setting the copy would overwrite. (Auto-vacuum cannot be used
    // here: the pool's WAL switch already wrote page 1, which fixes that mode.)
    sqlx::query("PRAGMA application_id = 42")
        .execute(&pool)
        .await
        .unwrap();
    assert!(!try_restore(&pool).await.unwrap());
    run_migrations(&pool).await.unwrap();
    pool.close().await;
    let reopened = create_sqlite_pool(&url).await.unwrap();
    let application_id: i64 = sqlx::query_scalar("PRAGMA application_id")
        .fetch_one(&reopened)
        .await
        .unwrap();
    assert_eq!(application_id, 42);
    reopened.close().await;
}
