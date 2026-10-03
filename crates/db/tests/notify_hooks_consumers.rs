use db::{create_sqlite_pool, run_migrations, run_migrations_from};
use std::{fs, path::Path};

#[tokio::test]
async fn upgrade_seeds_notify_hook_cursors_at_head_and_preserves_history() {
    let root = tempfile::tempdir().unwrap();
    let previous = root.path().join("previous");
    fs::create_dir(&previous).unwrap();
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    for entry in fs::read_dir(&source).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|extension| extension == "sql")
            && path.file_name().unwrap() != "V202610030200__notify_hooks_consumers.sql"
        {
            fs::copy(&path, previous.join(path.file_name().unwrap())).unwrap();
        }
    }
    let pool = create_sqlite_pool(&format!(
        "sqlite://{}",
        root.path().join("upgrade.db").display()
    ))
    .await
    .unwrap();
    run_migrations_from(&pool, &previous).await.unwrap();
    sqlx::query("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 5000) INSERT INTO domain_event (id, event_type, entity_type, entity_id, actor_type, scope_type, scope_id, correlation_id, payload_json, created_at) SELECT 'history-' || i, 'task.transitioned', 'task', 'old-task', 'system', 'task', 'old-task', 'history-' || i, '{\"from_state\":\"review\",\"to_state\":\"done\"}', '2026-10-01T00:00:00Z' FROM n").execute(&pool).await.unwrap();
    let head: i64 = sqlx::query_scalar("SELECT MAX(sequence) FROM domain_event")
        .fetch_one(&pool)
        .await
        .unwrap();
    run_migrations(&pool).await.unwrap();
    let cursors: Vec<(String, i64)> = sqlx::query_as("SELECT consumer_name, last_sequence FROM event_consumer_cursor WHERE consumer_name IN ('project-hooks', 'notifications') ORDER BY consumer_name").fetch_all(&pool).await.unwrap();
    assert_eq!(
        cursors,
        vec![
            ("notifications".to_owned(), head),
            ("project-hooks".to_owned(), head)
        ]
    );
    let retained: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM domain_event WHERE id LIKE 'history-%'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(retained, 5000);
    sqlx::query("UPDATE event_consumer_cursor SET last_sequence = last_sequence + 1, version = version + 1 WHERE consumer_name = 'notifications'").execute(&pool).await.unwrap();
    run_migrations(&pool).await.unwrap();
    let retained_cursor: i64 = sqlx::query_scalar(
        "SELECT last_sequence FROM event_consumer_cursor WHERE consumer_name = 'notifications'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(retained_cursor, head + 1);
    pool.close().await;
}

#[tokio::test]
async fn bundled_schema_keeps_all_six_notification_and_hook_task_triggers() {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    let actual: std::collections::BTreeSet<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = 'task'
         AND name IN ('project_hooks_task_created', 'project_hooks_task_archived',
            'notifications_blocked_json', 'notifications_failed_json',
            'notifications_task_recovered', 'notifications_merge_failed')",
    )
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    .collect();
    let expected = [
        "project_hooks_task_created",
        "project_hooks_task_archived",
        "notifications_blocked_json",
        "notifications_failed_json",
        "notifications_task_recovered",
        "notifications_merge_failed",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    assert_eq!(actual, expected);
}
