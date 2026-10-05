//! Upgrade through V202610042145 with live wake state: budget charges keep
//! their own hour and the restored constraints, and every Attention item's
//! latest wake decision is carried into the keyed sweep table.
use db::{create_sqlite_pool, new_uuid_v4, run_migrations_from};
use std::path::PathBuf;

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("forge-level-triggered-wakes-upgrade")
        .join(format!("{name}-{}", new_uuid_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test]
async fn budget_charges_keep_their_hour_and_latest_decisions_are_backfilled() {
    let full = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"));
    let base = scratch("base-migrations");
    for entry in std::fs::read_dir(&full).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().into_string().unwrap();
        if name.as_str() < "V202610042145" {
            std::fs::copy(entry.path(), base.join(&name)).unwrap();
        }
    }
    let db_dir = scratch("upgrade-db");
    let pool = create_sqlite_pool(&format!(
        "sqlite://{}",
        db_dir.join("forge.sqlite").display()
    ))
    .await
    .unwrap();
    run_migrations_from(&pool, &base).await.unwrap();

    let now = chrono::Utc::now();
    let at = |minutes: i64| (now - chrono::Duration::minutes(minutes)).to_rfc3339();
    for project in ["p", "q"] {
        sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES (?,?,?,?)")
            .bind(project)
            .bind(project)
            .bind(at(0))
            .bind(at(0))
            .execute(&pool)
            .await
            .unwrap();
    }
    for identity in ["a", "b", "c"] {
        sqlx::query(
            "INSERT INTO agent_identity (id, name, created_at, updated_at) VALUES (?, ?, ?, ?)",
        )
        .bind(identity)
        .bind(identity)
        .bind(at(0))
        .bind(at(0))
        .execute(&pool)
        .await
        .unwrap();
    }
    // p: two live windows (10 and 30 minutes old) and one expired one.
    for (identity, project, started, count) in [
        ("a", "p", at(10), 3),
        ("b", "p", at(30), 2),
        ("c", "p", at(120), 5),
        ("a", "q", at(90), 4),
    ] {
        sqlx::query("INSERT INTO agent_wake_budget_window (identity_id,scope_type,scope_id,window_started_at,window_seconds,admitted_count,version,updated_at) VALUES (?,'project',?,?,3600,?,1,?)")
            .bind(identity).bind(project).bind(&started).bind(count).bind(&started)
            .execute(&pool).await.unwrap();
    }
    // A setup-required decision for one open Attention item.
    let event_id = new_uuid_v4();
    sqlx::query("INSERT INTO domain_event(id,event_type,entity_type,entity_id,actor_type,scope_type,scope_id,correlation_id,causation_depth,payload_json,created_at) VALUES (?,'agent.wake.setup_required','agent_wake','k','attention_projection','project','p',?,1,json_object('attention_id','att-1','identity_id','a'),?)")
        .bind(&event_id).bind(&event_id).bind(at(5)).execute(&pool).await.unwrap();
    let sequence: i64 = sqlx::query_scalar("SELECT sequence FROM domain_event WHERE id=?")
        .bind(&event_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO attention_projection(id,attention_type,scope_type,scope_id,source_event_id,priority,status,summary,details_json,dedupe_key,occurred_at,updated_at,recommended_action) VALUES ('att-1','execution_failed','project','p',?,80,'open','s','{}','k',?,?,'inspect')")
        .bind(&event_id).bind(at(5)).bind(at(5)).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO agent_wake_disposition(id,consumer_name,source_event_id,source_event_sequence,attempt_number,max_attempts,disposition,reason,attention_id,incident_digest,created_at,updated_at) VALUES (?,'agent-wake-turns',?,?,1,3,'setup_required','responder_binding_missing','att-1','old-digest',?,?)")
        .bind(new_uuid_v4()).bind(&event_id).bind(sequence).bind(at(5)).bind(at(5))
        .execute(&pool).await.unwrap();

    run_migrations_from(&pool, &full).await.unwrap();

    let rows: Vec<(String, String, String, i64)> = sqlx::query_as(
        "SELECT scope_id, category, window_started_at, admitted_count FROM agent_wake_budget_window ORDER BY scope_id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        rows,
        vec![("p".to_owned(), "delivery".to_owned(), at(30), 5)],
        "only live charges survive, starting at the oldest live window"
    );
    for (sql, why) in [
        ("INSERT INTO agent_wake_budget_window (scope_type,scope_id,category,window_started_at,window_seconds,admitted_count,updated_at) VALUES ('project','x','blocker','t',0,0,'t')", "window_seconds > 0"),
        ("INSERT INTO agent_wake_budget_window (scope_type,scope_id,category,window_started_at,window_seconds,admitted_count,updated_at) VALUES ('bogus','x','blocker','t',3600,0,'t')", "scope_type"),
        ("INSERT INTO agent_wake_budget_window (scope_type,scope_id,category,window_started_at,window_seconds,admitted_count,updated_at) VALUES ('room','x','blocker','t',3600,0,'t')", "retired room scope"),
    ] {
        assert!(sqlx::query(sql).execute(&pool).await.is_err(), "{why}");
    }
    let latest: (String, String, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT disposition, reason, incident_digest, identity_id FROM agent_wake_attention_latest WHERE attention_id='att-1'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        latest,
        (
            "setup_required".to_owned(),
            "responder_binding_missing".to_owned(),
            Some("old-digest".to_owned()),
            Some("a".to_owned())
        )
    );
    let refunds: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('agent_chat_turn_job') WHERE name='lease_refund_count' AND dflt_value='0'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(refunds, 1);
}
