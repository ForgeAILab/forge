//! Additive digest migration and installed-trigger contract.
use db::{create_sqlite_pool, run_migrations, run_migrations_from};
use std::path::Path;

#[tokio::test]
async fn digest_migration_preserves_cache_bytes_and_requires_the_revision_walk() {
    let fixture = tempfile::tempdir().unwrap();
    let migrations = fixture.path().join("migrations");
    std::fs::create_dir(&migrations).unwrap();
    for entry in
        std::fs::read_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations")).unwrap()
    {
        let entry = entry.unwrap();
        if entry.file_name().to_str().unwrap() < "V202610270500" {
            std::fs::copy(entry.path(), migrations.join(entry.file_name())).unwrap();
        }
    }
    let pool = create_sqlite_pool(&format!(
        "sqlite://{}",
        fixture.path().join("forge.db").display()
    ))
    .await
    .unwrap();
    run_migrations_from(&pool, &migrations).await.unwrap();
    sqlx::raw_sql(r#"INSERT INTO project(id,name,created_at,updated_at) VALUES('p','p','now','now');
        INSERT INTO task(id,project_id,title,status,version,condition_json,created_at,updated_at)
        VALUES('t','p','retained','todo',42,' {"sentinel":true} ','then','later');
        INSERT OR REPLACE INTO system_setting(key,value,updated_at) VALUES('task_condition_mapping_revision','8','now');
        INSERT OR REPLACE INTO system_setting(key,value,updated_at) VALUES('task_condition_backfill_cursor','{"revision":8,"after":"t"}','now');"#)
        .execute(&pool).await.unwrap();
    let before: (String, i64, String, String) = sqlx::query_as(
        "SELECT condition_json,version,created_at,updated_at FROM task WHERE id='t'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    run_migrations(&pool).await.unwrap();
    let after: (String, i64, String, String) = sqlx::query_as(
        "SELECT condition_json,version,created_at,updated_at FROM task WHERE id='t'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(after, before);
    let digest: Option<String> =
        sqlx::query_scalar("SELECT condition_digest FROM task WHERE id='t'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(digest.is_none());
    let marker: Option<String> = sqlx::query_scalar(
        "SELECT value FROM system_setting WHERE key='task_condition_mapping_revision'",
    )
    .fetch_optional(&pool)
    .await
    .unwrap();
    assert!(marker.is_none());
    let cursor: Option<String> = sqlx::query_scalar(
        "SELECT value FROM system_setting WHERE key='task_condition_backfill_cursor'",
    )
    .fetch_optional(&pool)
    .await
    .unwrap();
    assert!(
        cursor.is_none(),
        "a pre-column cursor must not skip the newly NULL prefix"
    );

    let quick: String = sqlx::query_scalar("PRAGMA quick_check")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(quick, "ok");
    let db = db::SqliteDb::new(pool.clone());
    while db.backfill_task_conditions_if_stale().await.unwrap() {}
    let walk = db.condition_check_status();
    assert_eq!(
        (walk.walk.checked, walk.walk.repaired),
        (1, 1),
        "the old cursor must not skip t"
    );
    assert!(
        walk.last_pass.is_none(),
        "historical rewrites do not create operator issues"
    );
    let (digest, version): (Option<String>, i64) =
        sqlx::query_as("SELECT condition_digest,version FROM task WHERE id='t'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(digest.is_some());
    assert_eq!(version, 42);

    pool.close().await;
}

#[tokio::test]
async fn no_installed_task_trigger_observes_digest_only_updates() {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    let triggers: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT name,tbl_name,sql FROM sqlite_schema WHERE type='trigger' ORDER BY name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    let mut task_updates = Vec::new();
    for (name, table, sql) in triggers {
        assert!(
            !sql.contains("condition_digest"),
            "{name} observes the digest"
        );
        let header = sql.split("BEGIN").next().unwrap().to_uppercase();
        if table == "task" && header.contains("UPDATE") {
            assert!(
                header.contains("UPDATE OF ") || header.contains(" WHEN "),
                "{name} fires on every Task column update"
            );
            task_updates.push(name);
        }
    }
    assert!(task_updates
        .iter()
        .any(|name| name == "task_schedule_update"));
    assert!(task_updates
        .iter()
        .any(|name| name == "task_schedule_relationship"));
    assert!(task_updates
        .iter()
        .any(|name| name == "task_list_revision_task_update"));
}
