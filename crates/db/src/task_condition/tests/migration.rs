//! Tests for migration in task conditions.

use super::*;
#[tokio::test]
async fn condition_migration_replay_preserves_populated_legacy_rows() {
    let pool = crate::create_sqlite_pool("sqlite::memory:").await.unwrap();
    // A populated pre-stage-one schema, including archives/deletions and malformed bytes.
    sqlx::raw_sql("CREATE TABLE task(id TEXT PRIMARY KEY,version INTEGER,updated_at TEXT,archived_at TEXT,deleted_at TEXT,error_annotation TEXT,blocked_json TEXT,failed_json TEXT,entry_barrier_json TEXT,metadata_json TEXT);")
        .execute(&pool).await.unwrap();
    let rows = [
        input(
            Some(json!({"type":"review_budget_exhausted","custom":"retained"})),
            Some(json!({"kind":"retry_exhausted"})),
            None,
            Some(json!({"status":"blocked"})),
            Some(
                json!({"paused_integration":{"state":"review"},"queued_recovery":{"id":"intent","request":{"guidance":"private"}},"custom":1}),
            ),
        ),
        ConditionInput {
            error_annotation: Some("{bad".into()),
            metadata_json: Some("[1,2]".into()),
            ..Default::default()
        },
        input(
            None,
            None,
            Some(json!({"kind":"executor_failed"})),
            None,
            Some(
                json!({"plan_publication_claim":{"execution_id":"e"},"terminal_execution_settlement":{"execution_id":"e"}}),
            ),
        ),
        ConditionInput::default(),
    ];
    for (i, input) in rows.iter().enumerate() {
        sqlx::query("INSERT INTO task VALUES(?,7,'unchanged','archived','deleted',?,?,?,?,?)")
            .bind(i.to_string())
            .bind(&input.error_annotation)
            .bind(&input.blocked_json)
            .bind(&input.failed_json)
            .bind(&input.entry_barrier_json)
            .bind(&input.metadata_json)
            .execute(&pool)
            .await
            .unwrap();
    }
    let mut conn = pool.acquire().await.unwrap();
    migrate(&mut conn).await;
    for (i, expected) in rows.iter().enumerate() {
        let row = sqlx::query(&format!(
            "SELECT {CHANNEL_SELECT},version,updated_at,archived_at,deleted_at FROM task WHERE id=?"
        ))
        .bind(i.to_string())
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        assert_eq!(ConditionInput::from_row(&row).unwrap(), *expected);
        assert_eq!(row.get::<i64, _>("version"), 7);
        assert_eq!(row.get::<String, _>("updated_at"), "unchanged");
        assert_eq!(row.get::<String, _>("archived_at"), "archived");
        assert_eq!(row.get::<String, _>("deleted_at"), "deleted");
        let condition = decode(row.get("condition_json")).unwrap();
        assert_eq!(condition, compose_condition(expected));
        let evidence = serde_json::to_value(condition).unwrap()["evidence"].clone();
        assert!(evidence["metadata"].get("queued_recovery").is_none());
        assert!(evidence["metadata"].get("plan_publication_claim").is_none());
        assert!(evidence["metadata"]
            .get("terminal_execution_settlement")
            .is_none());
    }
    // B1: the migration leaves nothing that could put the mapping back into a
    // Task write statement.
    let schema_objects: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type IN ('trigger','view') AND sql LIKE '%condition%'",
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap();
    assert!(schema_objects.is_empty(), "{schema_objects:?}");
}

/// A row that never passes a writer seam keeps the column default, so the
/// default must be exactly the stored form of the empty mapping.
#[tokio::test]
async fn column_default_is_the_mapping_of_no_condition_facts() {
    let db = db().await;
    let default: String = sqlx::query_scalar(
        "SELECT dflt_value FROM pragma_table_info('task') WHERE name='condition_json'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    let old_default = default.trim_matches(char::from(39));
    let condition = decode(old_default).unwrap();
    assert_eq!(
        condition.read(),
        compose_condition(&ConditionInput::default()).read()
    );
    assert!(
        condition.evidence().witnesses.is_empty(),
        "column defaults are rederived by the first producer"
    );
}

/// Port of `audit_hostile_backfill`, extended with m3: every hostile value in
/// every column (alone and together), plus BLOBs and invalid UTF-8, survives
/// the backfill and the writer seam. Legacy bytes, version, updated_at and
/// storage type never change, and every produced condition decodes.
#[tokio::test]
async fn hostile_legacy_values_survive_backfill_and_writer_seam() {
    let db = db().await;
    let template = task(&db, "tmpl").await;
    let values = hostile();
    let metadata = hostile_metadata();
    let n = values.len() * 5 + metadata.len() + 6;
    clone_tasks(&db, &template.id, n, "h").await;
    unmigrate(&db).await;
    // Pre-existing notification triggers parse some legacy JSON; a value they
    // refuse simply stays NULL in the fixture.
    let mut i = 0usize;
    let mut tx = db.pool().begin().await.unwrap();
    for (_, value) in &values {
        for column in &LEGACY_COLUMNS[..4] {
            let _ = sqlx::query(&format!("UPDATE task SET {column}=? WHERE id=?"))
                .bind(value)
                .bind(format!("h{i}"))
                .execute(&mut *tx)
                .await;
            i += 1;
        }
        let _ = sqlx::query("UPDATE task SET error_annotation=?1,blocked_json=?1,failed_json=?1,entry_barrier_json=?1,metadata_json=?1 WHERE id=?2").bind(value).bind(format!("h{i}")).execute(&mut *tx).await;
        i += 1;
    }
    for value in &metadata {
        let _ = sqlx::query("UPDATE task SET metadata_json=? WHERE id=?")
            .bind(value)
            .bind(format!("h{i}"))
            .execute(&mut *tx)
            .await;
        i += 1;
    }
    // m3: non-text storage in each legacy column, and invalid UTF-8 text.
    for column in LEGACY_COLUMNS {
        sqlx::query(&format!("UPDATE task SET {column}=x'ff00fe' WHERE id=?"))
            .bind(format!("h{i}"))
            .execute(&mut *tx)
            .await
            .unwrap_or_else(|e| panic!("blob fixture {column}: {e}"));
        i += 1;
    }
    sqlx::query("UPDATE task SET blocked_json=CAST(x'fffe41' AS TEXT) WHERE id=?")
        .bind(format!("h{i}"))
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(i + 1, n);

    let before = legacy_snapshot(&db).await;
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    migrate(&mut tx).await;
    tx.commit().await.unwrap();
    assert_eq!(
        before,
        legacy_snapshot(&db).await,
        "legacy bytes/version/updated_at/storage changed"
    );
    assert_eq!(
        db.task_condition_violations().await.unwrap(),
        Vec::<String>::new()
    );
    let stored: Vec<(String, String)> =
        sqlx::query_as("SELECT id, condition_json FROM task ORDER BY id")
            .fetch_all(db.pool())
            .await
            .unwrap();
    let mut kinds = std::collections::BTreeSet::new();
    for (id, raw) in &stored {
        let condition = decode(raw).unwrap_or_else(|e| panic!("{id}: {e}"));
        assert!(
            raw.len() < 200_000,
            "{id}: evidence is bounded ({})",
            raw.len()
        );
        kinds.insert(serde_json::to_value(&condition).unwrap()["kind"].to_string());
    }
    assert_eq!(
        kinds.len(),
        4,
        "clear, deferred, parked and failed all occur: {kinds:?}"
    );
    for (offset, field) in CHANNEL_FIELDS.iter().enumerate() {
        let id = format!("h{}", n - 6 + offset);
        let condition = db.task_condition(&id).await.unwrap();
        assert!(
            reasons(&condition).iter().any(|reason| matches!(
                reason,
                ParkReason::UnknownCondition { source, problem: UnknownConditionProblem::NonText }
                    if source.field == *field
            )),
            "{id}: {condition:?}"
        );
    }
    assert!(matches!(
        reasons(&db.task_condition(&format!("h{}", n - 1)).await.unwrap())[0],
        ParkReason::UnknownCondition {
            problem: UnknownConditionProblem::NonText,
            ..
        }
    ));

    // The same values through the live writer seam (was: the triggers).
    let write = |column: &'static str, value: Option<String>| {
        let db = db.clone();
        async move {
            let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
            // A refusal here is a pre-existing notification trigger rejecting
            // the legacy value itself; the seam must never be the one to fail.
            let _ = sqlx::query(&format!("UPDATE task SET {column}=? WHERE id='tmpl'"))
                .bind(value)
                .execute(&mut *tx)
                .await;
            db.sync_condition_in_tx(&mut tx, "tmpl")
                .await
                .unwrap_or_else(|e| panic!("{column}: {e}"));
            tx.commit().await.unwrap();
        }
    };
    for (_, value) in &values {
        for column in LEGACY_COLUMNS {
            write(column, value.clone()).await;
        }
    }
    for value in &metadata {
        write("metadata_json", value.clone()).await;
    }
    for column in LEGACY_COLUMNS {
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        sqlx::query(&format!(
            "UPDATE task SET {column}=x'ff00fe' WHERE id='tmpl'"
        ))
        .execute(&mut *tx)
        .await
        .unwrap();
        db.sync_condition_in_tx(&mut tx, "tmpl").await.unwrap();
        sqlx::query(&format!(
            "UPDATE task SET {column}=CAST(x'fffe41' AS TEXT) WHERE id='tmpl'"
        ))
        .execute(&mut *tx)
        .await
        .unwrap();
        db.sync_condition_in_tx(&mut tx, "tmpl").await.unwrap();
        tx.commit().await.unwrap();
        assert!(
            db.task_condition("tmpl").await.unwrap().is_blocked(),
            "{column}"
        );
        sqlx::query(&format!("UPDATE task SET {column}=NULL WHERE id='tmpl'"))
            .execute(db.pool())
            .await
            .unwrap();
    }
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    db.sync_condition_in_tx(&mut tx, "tmpl").await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        db.task_condition_violations().await.unwrap(),
        Vec::<String>::new()
    );
}
