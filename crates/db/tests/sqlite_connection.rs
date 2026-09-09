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
