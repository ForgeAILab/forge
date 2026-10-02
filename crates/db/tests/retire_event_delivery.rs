use db::{
    create_sqlite_pool, now_rfc3339, run_migrations, CreateDomainEvent, DomainEventRepo, SqliteDb,
};
use sqlx::Row;
use std::collections::BTreeMap;

async fn snapshot(db: &SqliteDb) -> BTreeMap<String, Vec<String>> {
    let tables: Vec<String> = sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name NOT IN ('event_processing_lease', 'event_projection_receipt', 'attention_consumer_health', 'event_consumer_cursor')")
        .fetch_all(db.pool()).await.unwrap();
    let mut result = BTreeMap::new();
    for table in tables {
        let columns = sqlx::query(&format!("PRAGMA table_info(\"{table}\")"))
            .fetch_all(db.pool())
            .await
            .unwrap();
        let fields: Vec<String> = columns
            .iter()
            .map(|row| format!("quote(\"{}\")", row.get::<String, _>("name")))
            .collect();
        let query = format!(
            "SELECT {} FROM \"{table}\" ORDER BY 1",
            fields.join(" || '|' || ")
        );
        result.insert(
            table,
            sqlx::query_scalar(&query)
                .fetch_all(db.pool())
                .await
                .unwrap(),
        );
    }
    result
}

#[tokio::test]
async fn retirement_preserves_all_domain_rows_and_durable_cursors_and_drops_delivery_tables() {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    let event = db
        .append_event(CreateDomainEvent {
            id: "migration-event".into(),
            event_type: "agent.wake.suppressed".into(),
            entity_type: "agent_wake".into(),
            entity_id: "incident".into(),
            actor_type: "system".into(),
            actor_id: None,
            scope_type: "project".into(),
            scope_id: "p".into(),
            correlation_id: "migration".into(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some("migration-event".into()),
            payload_json: "{}".into(),
            created_at: now_rfc3339(),
        })
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("fixtures/event_delivery.sql"))
        .execute(db.pool())
        .await
        .unwrap();
    for name in [
        "scoped-memory-agent-chat-indexer",
        "agent-coordination-outcomes",
        "attention_projection",
        "agent-wake-turns",
        "sse-broadcast",
    ] {
        sqlx::query("INSERT INTO event_consumer_cursor (consumer_name, last_sequence, version, updated_at) VALUES (?, ?, 9, ?) ON CONFLICT(consumer_name) DO UPDATE SET last_sequence = excluded.last_sequence, version = 9, updated_at = excluded.updated_at")
            .bind(name).bind(event.sequence).bind("2026-10-01T12:00:00Z").execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO event_processing_lease (consumer_name, event_sequence, lease_owner, leased_until, attempts, updated_at) VALUES (?, ?, 'legacy', '2999-01-01T00:00:00Z', 3, ?)")
            .bind(name).bind(event.sequence).bind(now_rfc3339()).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO event_projection_receipt (consumer_name, event_id, dedupe_key, processed_at) VALUES (?, ?, ?, ?)")
            .bind(name).bind(&event.id).bind(&event.id).bind(now_rfc3339()).execute(db.pool()).await.unwrap();
    }
    sqlx::query("INSERT INTO attention_projection (id, attention_type, scope_type, scope_id, source_event_id, priority, status, summary, details_json, dedupe_key, occurred_at, updated_at) VALUES ('migration-incident', 'test', 'project', 'p', ?, 1, 'open', 'User history', '{}', 'migration-incident', ?, ?)")
        .bind(&event.id).bind(now_rfc3339()).bind(now_rfc3339()).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO attention_consumer_health (consumer_name, last_sequence, last_success_at, last_error_code, last_error_message, lease_owner, lease_until, processed_events, updated_at) VALUES ('attention_projection', 42, ?, 'projection_error', 'bounded diagnostic', 'legacy', ?, 42, ?)")
        .bind(now_rfc3339()).bind("2999-01-01T00:00:00Z").bind(now_rfc3339()).execute(db.pool()).await.unwrap();
    let before = snapshot(&db).await;
    let cursors: Vec<(String, i64, i64, String)> = sqlx::query_as("SELECT consumer_name, last_sequence, version, updated_at FROM event_consumer_cursor WHERE consumer_name <> 'sse-broadcast' ORDER BY consumer_name").fetch_all(db.pool()).await.unwrap();
    sqlx::raw_sql(include_str!(
        "../migrations/V202610020700__retire_event_delivery_leases.sql"
    ))
    .execute(db.pool())
    .await
    .unwrap();
    assert_eq!(before, snapshot(&db).await);
    let after: Vec<(String, i64, i64, String)> = sqlx::query_as("SELECT consumer_name, last_sequence, version, updated_at FROM event_consumer_cursor ORDER BY consumer_name").fetch_all(db.pool()).await.unwrap();
    assert_eq!(cursors, after);
    let retired: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE name IN ('event_processing_lease', 'event_projection_receipt', 'attention_consumer_health')").fetch_one(db.pool()).await.unwrap();
    assert_eq!(retired, 0);
}
