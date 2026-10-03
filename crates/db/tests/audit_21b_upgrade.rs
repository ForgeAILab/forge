//! AUDIT 2.1b: upgrade through the real migration runner from the base schema
//! (migrations before V202610020700), with real legacy rows.
use db::{create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations_from};
use std::path::PathBuf;

const RETIRE: &str = "V202610020700__retire_event_delivery_leases.sql";

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("forge-21b-upgrade")
        .join(format!("{name}-{}", new_uuid_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test]
async fn audit_real_runner_upgrade_from_base_schema() {
    let full = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"));
    let base = scratch("base-migrations");
    for entry in std::fs::read_dir(&full).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().into_string().unwrap();
        if name != RETIRE && name.as_str() < "V202610020700" {
            std::fs::copy(entry.path(), base.join(&name)).unwrap();
        }
    }
    let db_dir = scratch("upgrade-db");
    let path = db_dir.join("forge.sqlite");
    let pool = create_sqlite_pool(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    run_migrations_from(&pool, &base).await.unwrap();

    // The base schema really has the three tables (and the V065 index).
    let legacy: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE name IN ('event_processing_lease', 'event_projection_receipt', 'attention_consumer_health', 'idx_attention_consumer_health_stale')")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(legacy, 4, "base schema has the legacy tables + index");

    let now = now_rfc3339();
    let mut ids = Vec::new();
    for n in 1..=4 {
        let id = format!("upgrade-event-{n}");
        sqlx::query("INSERT INTO domain_event (id, event_type, entity_type, entity_id, actor_type, scope_type, scope_id, correlation_id, causation_depth, dedupe_key, payload_json, created_at) VALUES (?, 'task.transitioned', 'task', 't', 'system', 'project', 'p', ?, 0, ?, '{}', ?)")
            .bind(&id).bind(&id).bind(&id).bind(&now).execute(&pool).await.unwrap();
        ids.push(id);
    }
    let consumers = [
        "scoped-memory-agent-chat-indexer",
        "agent-coordination-outcomes",
        "attention_projection",
        "agent-wake-turns",
        "sse-broadcast",
    ];
    for name in consumers {
        // Cursor at N = 2, receipts for 1..=2, a live lease on 3.
        sqlx::query("INSERT INTO event_consumer_cursor (consumer_name, last_sequence, version, updated_at) VALUES (?, 2, 7, ?) ON CONFLICT(consumer_name) DO UPDATE SET last_sequence = 2, version = 7, updated_at = excluded.updated_at")
            .bind(name).bind("2026-10-01T12:00:00Z").execute(&pool).await.unwrap();
        for id in &ids[..2] {
            sqlx::query("INSERT INTO event_projection_receipt (consumer_name, event_id, dedupe_key, processed_at) VALUES (?, ?, ?, ?)")
                .bind(name).bind(id).bind(id).bind(&now).execute(&pool).await.unwrap();
        }
        sqlx::query("INSERT INTO event_processing_lease (consumer_name, event_sequence, lease_owner, leased_until, attempts, updated_at) VALUES (?, 3, 'legacy', '2999-01-01T00:00:00Z', 2, ?)")
            .bind(name).bind(&now).execute(&pool).await.unwrap();
    }
    sqlx::query("INSERT INTO attention_consumer_health (consumer_name, last_sequence, last_success_at, processed_events, updated_at) VALUES ('attention_projection', 2, ?, 2, ?) ON CONFLICT(consumer_name) DO UPDATE SET last_sequence = 2, processed_events = 2")
        .bind(&now).bind(&now).execute(&pool).await.unwrap();
    let cursors_before: Vec<(String, i64, i64, String)> = sqlx::query_as("SELECT consumer_name, last_sequence, version, updated_at FROM event_consumer_cursor WHERE consumer_name <> 'sse-broadcast' ORDER BY consumer_name")
        .fetch_all(&pool).await.unwrap();
    let events_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_event")
        .fetch_one(&pool)
        .await
        .unwrap();
    let cutover_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM event_consumer_cutover")
        .fetch_one(&pool)
        .await
        .unwrap();

    // The upgrade proper: the real runner applies only pending migrations.
    run_migrations_from(&pool, &full).await.unwrap();

    let legacy: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE name IN ('event_processing_lease', 'event_projection_receipt', 'attention_consumer_health', 'idx_attention_consumer_health_stale')")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(legacy, 0);
    let cursors_after: Vec<(String, i64, i64, String)> = sqlx::query_as("SELECT consumer_name, last_sequence, version, updated_at FROM event_consumer_cursor WHERE consumer_name NOT IN ('notifications', 'project-hooks') ORDER BY consumer_name")
        .fetch_all(&pool).await.unwrap();
    assert_eq!(cursors_before, cursors_after, "cursors kept; sse removed");
    let head: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(sequence), 0) FROM domain_event")
        .fetch_one(&pool)
        .await
        .unwrap();
    let new_cursors: Vec<(String, i64)> = sqlx::query_as("SELECT consumer_name, last_sequence FROM event_consumer_cursor WHERE consumer_name IN ('notifications', 'project-hooks') ORDER BY consumer_name").fetch_all(&pool).await.unwrap();
    assert_eq!(
        new_cursors,
        vec![
            ("notifications".to_owned(), head),
            ("project-hooks".to_owned(), head)
        ]
    );

    let events_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_event")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(events_before, events_after);
    let cutover_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM event_consumer_cutover")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(cutover_before, cutover_after);
    let fk: Vec<(String, i64, String, i64)> = sqlx::query_as("PRAGMA foreign_key_check")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert!(fk.is_empty(), "foreign key violations: {fk:?}");
    let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(integrity, "ok");
    // Anything in the schema still naming a dropped table (views, triggers)?
    let dangling: Vec<String> = sqlx::query_scalar("SELECT name FROM sqlite_master WHERE sql LIKE '%event_processing_lease%' OR sql LIKE '%event_projection_receipt%' OR sql LIKE '%attention_consumer_health%'")
        .fetch_all(&pool).await.unwrap();
    assert!(dangling.is_empty(), "dangling schema objects: {dangling:?}");
    // Idempotent restart.
    run_migrations_from(&pool, &full).await.unwrap();
    pool.close().await;
    let _ = std::fs::remove_dir_all(db_dir);
    let _ = std::fs::remove_dir_all(base);
}
