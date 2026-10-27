//! Usage repository tests.
use super::*;

#[tokio::test]
async fn usage_ledger_revision_tracks_terminal_edits_and_rolls_back() {
    let db = sqlite_db().await;
    let (project, _, agent) = seed_project_repo_agent(&db).await;
    let task = seed_task(&db, &project, None, "todo".into(), "revision").await;
    let mut tx = db.pool().begin().await.unwrap();
    sqlx::query("INSERT INTO execution (id,task_id,agent_id,role,status,created_at,updated_at) VALUES ('usage-delta',?,?,'coder','completed','2026-01-01T00:00:00Z','2026-01-01T00:00:01Z')").bind(&task).bind(&agent).execute(&mut *tx).await.unwrap();
    let inside: i64 =
        sqlx::query_scalar("SELECT execution_rowid FROM usage_ledger_revision WHERE id=1")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert!(inside > 0);
    tx.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT execution_rowid FROM usage_ledger_revision WHERE id=1"
        )
        .fetch_one(db.pool())
        .await
        .unwrap(),
        0
    );
    sqlx::query("INSERT INTO execution (id,task_id,agent_id,role,status,created_at,updated_at) VALUES ('usage-delta',?,?,'coder','completed','2026-01-01T00:00:00Z','2026-01-01T00:00:01Z')").bind(task).bind(agent).execute(db.pool()).await.unwrap();
    sqlx::query("UPDATE execution SET agent_id=NULL WHERE id='usage-delta'")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT execution_revision FROM usage_ledger_revision WHERE id=1"
        )
        .fetch_one(db.pool())
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT revision FROM usage_changed_execution WHERE id='usage-delta'"
        )
        .fetch_one(db.pool())
        .await
        .unwrap(),
        1
    );
    sqlx::query("DELETE FROM execution WHERE id='usage-delta'")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT deletion_generation FROM usage_ledger_revision WHERE id=1"
        )
        .fetch_one(db.pool())
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT execution_rowid FROM usage_ledger_revision WHERE id=1"
        )
        .fetch_one(db.pool())
        .await
        .unwrap(),
        0
    );
    for (query, expected) in [
        ("EXPLAIN QUERY PLAN SELECT id,error FROM execution INDEXED BY idx_execution_usage_failed_recent WHERE status='failed' AND COALESCE(stopped_at,updated_at)>='2026-01-01' ORDER BY COALESCE(stopped_at,updated_at) DESC,id ASC", "idx_execution_usage_failed_recent"),
        ("EXPLAIN QUERY PLAN SELECT error FROM execution WHERE status='failed' AND task_id='task' ORDER BY COALESCE(stopped_at,updated_at) DESC,id DESC LIMIT 1", "idx_execution_usage_failed_task"),
    ] {
        let rows=sqlx::query(query).fetch_all(db.pool()).await.unwrap();
        let plan=rows.iter().map(|r|sqlx::Row::get::<String,_>(r, "detail")).collect::<Vec<_>>().join(" ");
        assert!(plan.contains(expected), "{plan}");assert!(!plan.contains("TEMP B-TREE"),"{plan}");
    }
}

#[tokio::test]
async fn usage_read_schema_contains_all_revision_triggers_and_indexes() {
    let db = sqlite_db().await;
    let names:Vec<String>=sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'usage_read_%' ORDER BY name").fetch_all(db.pool()).await.unwrap();
    let mut expected = Vec::new();
    for table in ["invocation", "event", "estimate", "execution"] {
        for action in ["insert", "delete"] {
            expected.push(format!("usage_read_{table}_{action}"));
        }
    }
    expected.extend([
        "usage_read_invocation_update".to_owned(),
        "usage_read_execution_update".to_owned(),
    ]);
    expected.sort();
    assert_eq!(names, expected);
    for name in [
        "idx_usage_changed_invocation_revision",
        "idx_usage_changed_execution_revision",
        "idx_execution_usage_running_agent",
        "idx_execution_usage_failed_recent",
        "idx_execution_usage_failed_task",
    ] {
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?"
            )
            .bind(name)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            1,
            "{name}"
        );
    }
    let header = sqlx::query("SELECT * FROM usage_ledger_revision WHERE id=1")
        .fetch_one(db.pool())
        .await
        .unwrap();
    for (column, table) in [
        ("invocation_count", "usage_invocation"),
        ("event_count", "usage_event"),
        ("estimate_count", "cost_estimate_revision"),
        ("execution_count", "execution"),
    ] {
        let count = sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(sqlx::Row::get::<i64, _>(&header, column), count);
    }
    assert_eq!(
        sqlx::Row::get::<i64, _>(&header, "owned_execution_count"),
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM execution WHERE agent_id IS NOT NULL")
            .fetch_one(db.pool())
            .await
            .unwrap()
    );
}
