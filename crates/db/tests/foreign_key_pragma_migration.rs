use db::{create_sqlite_pool, run_migrations_from};
use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

fn unique_temp_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time is after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("forge-{name}-{}-{nanos}", std::process::id()))
}

#[tokio::test]
async fn migration_runner_allows_foreign_key_pragmas_to_take_effect() {
    let migration_dir = unique_temp_path("pragma-migrations");
    fs::create_dir_all(&migration_dir).expect("temp migration dir");
    fs::write(
        migration_dir.join("V001__initial.sql"),
        r#"
        CREATE TABLE parent (
            id TEXT PRIMARY KEY
        );

        CREATE TABLE child (
            id TEXT PRIMARY KEY,
            parent_id TEXT NOT NULL REFERENCES parent(id) ON DELETE CASCADE
        );

        INSERT INTO parent (id) VALUES ('p1');
        INSERT INTO child (id, parent_id) VALUES ('c1', 'p1');
        "#,
    )
    .expect("writes initial migration");
    fs::write(
        migration_dir.join("V002__rebuild_parent.sql"),
        r#"
        PRAGMA foreign_keys = OFF;

        CREATE TABLE parent_new (
            id TEXT PRIMARY KEY
        );

        INSERT INTO parent_new (id)
        SELECT id
        FROM parent;

        DROP TABLE parent;
        ALTER TABLE parent_new RENAME TO parent;

        PRAGMA foreign_keys = ON;
        "#,
    )
    .expect("writes rebuild migration");

    let db_path = unique_temp_path("pragma-migration-db").with_extension("db");
    let url = format!("sqlite://{}", db_path.display());
    let pool = create_sqlite_pool(&url).await.expect("pool");

    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("migrations apply");

    let applied_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _migration")
        .fetch_one(&pool)
        .await
        .expect("migration count loads");
    assert_eq!(applied_count, 2);

    let foreign_key_violations: Vec<(String, i64, String, i64)> =
        sqlx::query_as("PRAGMA foreign_key_check")
            .fetch_all(&pool)
            .await
            .expect("foreign key check runs");
    assert!(foreign_key_violations.is_empty());

    let _ = fs::remove_file(db_path);
    let _ = fs::remove_dir_all(migration_dir);
}

#[tokio::test]
async fn review_conformance_rebuild_preserves_history_and_allows_parent_cascade() {
    let migration_dir = unique_temp_path("review-conformance-migrations");
    fs::create_dir_all(&migration_dir).expect("temp migration dir");
    fs::write(
        migration_dir.join("V001__review_conformance_v132.sql"),
        r#"
        CREATE TABLE task (
            id TEXT PRIMARY KEY
        );

        CREATE TABLE execution (
            id TEXT PRIMARY KEY,
            task_id TEXT NOT NULL REFERENCES task(id) ON DELETE CASCADE
        );

        CREATE TABLE execution_review_contract (
            execution_id TEXT PRIMARY KEY REFERENCES execution(id),
            task_id TEXT NOT NULL REFERENCES task(id),
            contract_digest TEXT NOT NULL UNIQUE,
            source_digest TEXT NOT NULL,
            contract_json TEXT NOT NULL CHECK(json_valid(contract_json)),
            created_at TEXT NOT NULL
        );

        CREATE TABLE execution_review_assessment (
            execution_id TEXT PRIMARY KEY REFERENCES execution_review_contract(execution_id),
            conformance_json TEXT NOT NULL CHECK(json_valid(conformance_json)),
            created_at TEXT NOT NULL
        );

        CREATE TRIGGER execution_review_contract_immutable_update
        BEFORE UPDATE ON execution_review_contract
        BEGIN
            SELECT RAISE(ABORT, 'review contracts are immutable');
        END;

        CREATE TRIGGER execution_review_contract_immutable_delete
        BEFORE DELETE ON execution_review_contract
        BEGIN
            SELECT RAISE(ABORT, 'review contracts are immutable');
        END;

        CREATE TRIGGER execution_review_assessment_immutable_update
        BEFORE UPDATE ON execution_review_assessment
        BEGIN
            SELECT RAISE(ABORT, 'review assessments are immutable');
        END;

        CREATE TRIGGER execution_review_assessment_immutable_delete
        BEFORE DELETE ON execution_review_assessment
        BEGIN
            SELECT RAISE(ABORT, 'review assessments are immutable');
        END;

        INSERT INTO task (id) VALUES ('task-1');
        INSERT INTO execution (id, task_id) VALUES ('execution-1', 'task-1');
        INSERT INTO execution_review_contract (
            execution_id, task_id, contract_digest, source_digest, contract_json, created_at
        ) VALUES (
            'execution-1', 'task-1', 'contract-digest', 'source-digest',
            '{"policy":"forge.review-conformance/2"}', '2026-09-07T00:00:00Z'
        );
        INSERT INTO execution_review_assessment (
            execution_id, conformance_json, created_at
        ) VALUES (
            'execution-1', '{"status":"passed"}', '2026-09-07T00:00:01Z'
        );
        "#,
    )
    .expect("writes V132-shaped migration");
    fs::write(
        migration_dir.join("V002__review_conformance_project_delete.sql"),
        include_str!("../migrations/V134__review_conformance_project_delete.sql"),
    )
    .expect("writes V134 migration");

    let db_path = unique_temp_path("review-conformance-migration-db").with_extension("db");
    let url = format!("sqlite://{}", db_path.display());
    let pool = create_sqlite_pool(&url).await.expect("pool");

    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("migrations apply");

    let contract_json: String = sqlx::query_scalar(
        "SELECT contract_json FROM execution_review_contract WHERE execution_id = 'execution-1'",
    )
    .fetch_one(&pool)
    .await
    .expect("contract history survives");
    let assessment_json: String = sqlx::query_scalar(
        "SELECT conformance_json FROM execution_review_assessment WHERE execution_id = 'execution-1'",
    )
    .fetch_one(&pool)
    .await
    .expect("assessment history survives");
    assert_eq!(contract_json, r#"{"policy":"forge.review-conformance/2"}"#);
    assert_eq!(assessment_json, r#"{"status":"passed"}"#);

    let direct_delete =
        sqlx::query("DELETE FROM execution_review_assessment WHERE execution_id = 'execution-1'")
            .execute(&pool)
            .await
            .expect_err("direct assessment deletion remains forbidden");
    assert!(direct_delete
        .to_string()
        .contains("review assessments are immutable"));

    sqlx::query("DELETE FROM task WHERE id = 'task-1'")
        .execute(&pool)
        .await
        .expect("parent deletion cascades through immutable review history");
    for table in [
        "execution",
        "execution_review_contract",
        "execution_review_assessment",
    ] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(&pool)
            .await
            .expect("row count");
        assert_eq!(count, 0, "{table} rows cascade");
    }
    let violations: Vec<(String, i64, String, i64)> = sqlx::query_as("PRAGMA foreign_key_check")
        .fetch_all(&pool)
        .await
        .expect("foreign key check runs");
    assert!(violations.is_empty());

    drop(pool);
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_dir_all(migration_dir);
}
