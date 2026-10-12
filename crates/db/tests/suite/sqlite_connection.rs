use db::create_sqlite_pool;

#[tokio::test]
async fn recursive_triggers_block_replace_on_immutable_rows() {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");

    let recursive_triggers: i64 = sqlx::query_scalar("PRAGMA recursive_triggers")
        .fetch_one(&pool)
        .await
        .expect("recursive trigger pragma");
    assert_eq!(recursive_triggers, 1);

    sqlx::query(
        "CREATE TABLE immutable_fixture (
            id TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .expect("fixture table");
    sqlx::query(
        "CREATE TRIGGER immutable_fixture_immutable_update
         BEFORE UPDATE ON immutable_fixture
         BEGIN
             SELECT RAISE(ABORT, 'immutable fixture rows are immutable');
         END",
    )
    .execute(&pool)
    .await
    .expect("fixture update trigger");
    sqlx::query(
        "CREATE TRIGGER immutable_fixture_immutable_delete
         BEFORE DELETE ON immutable_fixture
         BEGIN
             SELECT RAISE(ABORT, 'immutable fixture rows are immutable');
         END",
    )
    .execute(&pool)
    .await
    .expect("fixture delete trigger");
    sqlx::query("INSERT INTO immutable_fixture (id, value) VALUES ('row-1', 'original')")
        .execute(&pool)
        .await
        .expect("fixture row");

    let replace = sqlx::query(
        "INSERT OR REPLACE INTO immutable_fixture (id, value)
         VALUES ('row-1', 'replacement')",
    )
    .execute(&pool)
    .await;
    assert!(
        replace.is_err(),
        "REPLACE must honor immutable delete guards"
    );

    let row: (i64, String) = sqlx::query_as(
        "SELECT COUNT(*) AS count, MAX(value) AS value
         FROM immutable_fixture
         WHERE id = 'row-1'",
    )
    .fetch_one(&pool)
    .await
    .expect("immutable row");
    assert_eq!(row.0, 1);
    assert_eq!(row.1, "original");
}

fn database_url(label: &str) -> (std::path::PathBuf, String) {
    let path = std::env::temp_dir().join(format!("forge-outbox-{label}-{}.db", db::new_uuid_v4()));
    let url = format!("sqlite:{}", path.display());
    (path, url)
}

#[tokio::test]
async fn outbox_pool_connections_use_normal_and_new_databases_use_incremental_vacuum() {
    let (path, url) = database_url("new");
    let pool = create_sqlite_pool(&url).await.unwrap();
    sqlx::query("CREATE TABLE fixture (value TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    let mut first = pool.acquire().await.unwrap();
    let mut second = pool.acquire().await.unwrap();
    for conn in [&mut first, &mut second] {
        let synchronous: i64 = sqlx::query_scalar("PRAGMA synchronous")
            .fetch_one(&mut **conn)
            .await
            .unwrap();
        assert_eq!(synchronous, 1);
        let mode: i64 = sqlx::query_scalar("PRAGMA auto_vacuum")
            .fetch_one(&mut **conn)
            .await
            .unwrap();
        assert_eq!(mode, 2);
        let journal: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&mut **conn)
            .await
            .unwrap();
        assert_eq!(journal, "wal");
    }
    drop(first);
    drop(second);
    pool.close().await;
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn outbox_existing_database_conversion_is_explicit_and_preserves_data() {
    use sqlx::{sqlite::SqliteConnectOptions, Connection, SqliteConnection};
    use std::str::FromStr;
    let (path, url) = database_url("existing");
    let options = SqliteConnectOptions::from_str(&url)
        .unwrap()
        .create_if_missing(true);
    let mut old = SqliteConnection::connect_with(&options).await.unwrap();
    sqlx::query("PRAGMA journal_mode = WAL")
        .execute(&mut old)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE _migration (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL)").execute(&mut old).await.unwrap();
    sqlx::query("INSERT INTO _migration VALUES (144, 'fixture', '2026-09-01T00:00:00Z')")
        .execute(&mut old)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE fixture (id INTEGER PRIMARY KEY, body BLOB)")
        .execute(&mut old)
        .await
        .unwrap();
    sqlx::query("WITH RECURSIVE n(value) AS (SELECT 1 UNION ALL SELECT value+1 FROM n WHERE value<300) INSERT INTO fixture SELECT value, zeroblob(4096) FROM n").execute(&mut old).await.unwrap();
    sqlx::query("DELETE FROM fixture WHERE id > 1")
        .execute(&mut old)
        .await
        .unwrap();
    old.close().await.unwrap();

    let pool = create_sqlite_pool(&url).await.unwrap();
    let before = db::sqlite_storage_status(&pool).await.unwrap();
    assert!(!before.incremental_vacuum);
    assert!(before.free_pages > 100);
    db::incremental_vacuum(&pool).await.unwrap();
    assert_eq!(db::sqlite_storage_status(&pool).await.unwrap(), before);
    // The conversion opens the file exclusively and does not wait. A plain
    // `pool.close()` can leave the connection the status read just released
    // open (it is returned by a background task), which failed this test
    // once with `database is locked`.
    db::close_sqlite_pool(&pool).await;
    let size_before = std::fs::metadata(&path).unwrap().len();
    db::convert_sqlite_to_incremental(&url).await.unwrap();
    assert!(std::fs::metadata(&path).unwrap().len() < size_before);
    let pool = create_sqlite_pool(&url).await.unwrap();
    let after = db::sqlite_storage_status(&pool).await.unwrap();
    assert!(after.incremental_vacuum);
    assert_eq!(after.free_pages, 0);
    let row: (i64, i64) = sqlx::query_as("SELECT id, length(body) FROM fixture")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row, (1, 4096));
    let migration: (i64, String, String) =
        sqlx::query_as("SELECT version, name, applied_at FROM _migration")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        migration,
        (144, "fixture".to_owned(), "2026-09-01T00:00:00Z".to_owned())
    );
    let journal: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(journal, "wal");
    db::close_sqlite_pool(&pool).await;
    // Re-running conversion is safe and does not rebuild incremental databases.
    db::convert_sqlite_to_incremental(&url).await.unwrap();
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn outbox_incremental_vacuum_reclaims_at_most_100_pages_per_tick() {
    let (path, url) = database_url("bounded-vacuum");
    let pool = create_sqlite_pool(&url).await.unwrap();
    sqlx::query("CREATE TABLE fixture (id INTEGER PRIMARY KEY, body BLOB)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("WITH RECURSIVE n(value) AS (SELECT 1 UNION ALL SELECT value+1 FROM n WHERE value<300) INSERT INTO fixture SELECT value, zeroblob(4096) FROM n").execute(&pool).await.unwrap();
    sqlx::query("DELETE FROM fixture")
        .execute(&pool)
        .await
        .unwrap();
    let before = db::sqlite_storage_status(&pool).await.unwrap();
    assert!(before.free_pages > 100);
    db::incremental_vacuum(&pool).await.unwrap();
    let after = db::sqlite_storage_status(&pool).await.unwrap();
    assert!(after.free_pages < before.free_pages);
    assert!(before.free_pages - after.free_pages <= 100);
    pool.close().await;
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn outbox_conversion_refuses_busy_or_missing_databases() {
    use sqlx::{sqlite::SqliteConnectOptions, Connection, SqliteConnection};
    use std::str::FromStr;
    let (path, url) = database_url("busy");
    assert!(db::convert_sqlite_to_incremental(&url).await.is_err());
    assert!(
        !path.exists(),
        "conversion must not create a missing database"
    );
    let options = SqliteConnectOptions::from_str(&url)
        .unwrap()
        .create_if_missing(true);
    let mut live = SqliteConnection::connect_with(&options).await.unwrap();
    sqlx::query("PRAGMA journal_mode = WAL")
        .execute(&mut live)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE fixture (value TEXT)")
        .execute(&mut live)
        .await
        .unwrap();
    sqlx::query("INSERT INTO fixture VALUES ('keep')")
        .execute(&mut live)
        .await
        .unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut live)
        .await
        .unwrap();
    assert!(db::convert_sqlite_to_incremental(&url).await.is_err());
    sqlx::query("ROLLBACK").execute(&mut live).await.unwrap();
    sqlx::query("BEGIN").execute(&mut live).await.unwrap();
    let _: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM fixture")
        .fetch_one(&mut live)
        .await
        .unwrap();
    assert!(db::convert_sqlite_to_incremental(&url).await.is_err());
    sqlx::query("ROLLBACK").execute(&mut live).await.unwrap();
    let mode: i64 = sqlx::query_scalar("PRAGMA auto_vacuum")
        .fetch_one(&mut live)
        .await
        .unwrap();
    assert_eq!(mode, 0);
    let value: String = sqlx::query_scalar("SELECT value FROM fixture")
        .fetch_one(&mut live)
        .await
        .unwrap();
    assert_eq!(value, "keep");
    live.close().await.unwrap();
    std::fs::remove_file(path).unwrap();
}
