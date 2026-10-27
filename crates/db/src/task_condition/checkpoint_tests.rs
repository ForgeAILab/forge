use super::tests::{db, task};
use super::*;
use crate::{SystemSettingRepo, TaskRepo};
use serde_json::json;

async fn raw(db: &SqliteDb, id: &str) -> String {
    sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
        .bind(id)
        .fetch_one(db.pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn a_failed_corrupt_row_does_not_stop_the_cursor_or_revision_marker() {
    let db = db().await;
    for id in ["a-good", "b-bad", "c-good"] {
        task(&db, id).await;
        sqlx::query("UPDATE task SET condition_json='{}' WHERE id=?")
            .bind(id)
            .execute(db.pool())
            .await
            .unwrap();
    }
    // A row-specific storage failure after recomputation, with private error text.
    sqlx::raw_sql("CREATE TRIGGER reject_bad_text BEFORE INSERT ON task_diagnostic_text WHEN NEW.task_id='b-bad' BEGIN SELECT RAISE(ABORT,'private diagnostic contents'); END;")
        .execute(db.pool()).await.unwrap();
    db.set_setting(MAPPING_REVISION_KEY, "6", &crate::now_rfc3339())
        .await
        .unwrap();
    while db.backfill_task_conditions_if_stale().await.unwrap() {}
    assert!(matches!(
        db.task_condition("a-good").await.unwrap(),
        TaskCondition::Clear { .. }
    ));
    assert_eq!(raw(&db, "b-bad").await, "{}");
    assert!(matches!(
        db.task_condition("c-good").await.unwrap(),
        TaskCondition::Clear { .. }
    ));
    assert_eq!(
        db.get_setting(MAPPING_REVISION_KEY).await.unwrap(),
        Some(MAPPING_REVISION.to_string())
    );
    let status = db.condition_check_status();
    assert_eq!((status.checked, status.repaired), (3, 2));
    assert!(db.condition_checks.lock().unwrap().after.is_none());
}

#[tokio::test]
async fn operations_reason_keeps_base_json_limits_through_writes_reads_and_typed_projection() {
    let db = db().await;
    task(&db, "operations").await;
    let annotation = json!({"type":"workspace_error", "blocking_reason":"b".repeat(900),
        "message":"m".repeat(900), "blocked_by":"a".repeat(900),
        "hook":{"one":"1".repeat(900),"two":"2".repeat(900)}})
    .to_string();
    assert!(annotation.len() > 4096);
    sqlx::query("UPDATE task SET error_annotation=? WHERE id='operations'")
        .bind(&annotation)
        .execute(db.pool())
        .await
        .unwrap();
    let mut c = db.pool().acquire().await.unwrap();
    storage::sync_condition(&mut c, "operations").await.unwrap();
    drop(c);
    let condition = db.task_condition("operations").await.unwrap();
    let expected = readers::bound_operator_reason(&annotation);
    assert_eq!(expected, annotation);
    assert_eq!(
        condition.read().operator_reason.as_deref(),
        Some(expected.as_str())
    );
    assert_eq!(
        condition.typed().read().operator_reason.as_deref(),
        Some(expected.as_str())
    );
    assert!(
        serde_json::from_str::<serde_json::Value>(&condition.read().operator_reason.unwrap())
            .is_ok()
    );
    super::statement_tests::hold(&db, "operations", &"h".repeat(4096)).await;
    let held = db.task_condition("operations").await.unwrap();
    let operator = held.read().operator_reason.unwrap();
    assert!(operator.len() > PRESENTATION_TEXT_LIMIT);
    assert!(operator.len() <= 16 * PRESENTATION_TEXT_LIMIT);
    let value: serde_json::Value = serde_json::from_str(&operator).unwrap();
    assert_eq!(value["type"], "manual_stop");
    assert!(value["blocking_reason"]
        .as_str()
        .unwrap()
        .ends_with(PRESENTATION_TRUNCATION_MARKER));
}

#[tokio::test]
async fn scoped_statement_records_one_occurrence() {
    let db = db().await;
    task(&db, "once").await;
    let hold = ConditionStatement::Hold {
        actor: "owner".into(),
        reason: "one occurrence".into(),
        at: crate::now_rfc3339(),
    };
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    write::with_statement("once", hold, async {
        let first = producers::derive(&mut tx, "once", ConditionChange::Full, None, None)
            .await
            .unwrap()
            .unwrap();
        let second = producers::derive(&mut tx, "once", ConditionChange::Full, None, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.condition.typed(), second.condition.typed());
        set_condition(&mut tx, "once", &second.encoded, Some(second.version))
            .await
            .unwrap();
        produce(&mut tx, "once", ConditionChange::Full)
            .await
            .unwrap();
        produce(&mut tx, "once", ConditionChange::Full)
            .await
            .unwrap();
    })
    .await;
    tx.commit().await.unwrap();
    let texts = db.task_diagnostic_texts("once", None, 100).await.unwrap();
    assert_eq!(texts.items.len(), 1);
    assert_eq!(texts.items[0].occurrence_count, 1);
}

#[tokio::test]
async fn diagnostic_pages_use_creation_order_uuid_ties_and_a_lookahead() {
    let db = db().await;
    task(&db, "pages").await;
    task(&db, "other-task").await;
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    let mut ids = Vec::new();
    for text in ["one", "two", "three", "four"] {
        ids.push(
            db.record_task_diagnostic_text_in_tx(
                &mut tx,
                "pages",
                RecordTaskDiagnosticText {
                    kind: "test",
                    source: "first-source",
                    owner: &DiagnosticOwner::StoredCondition,
                    text,
                },
            )
            .await
            .unwrap(),
        );
    }
    // Force non-UUID chronological order and a tie that must order by id.
    let mut sorted_ids = ids.clone();
    sorted_ids.sort();
    for (id, at) in sorted_ids
        .iter()
        .zip(["2030-01-03", "2030-01-02", "2030-01-02", "2030-01-01"])
    {
        sqlx::query("UPDATE task_diagnostic_text SET created_at=? WHERE id=?")
            .bind(at)
            .bind(id)
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    let again = db
        .record_task_diagnostic_text_in_tx(
            &mut tx,
            "pages",
            RecordTaskDiagnosticText {
                kind: "test",
                source: "later-source",
                owner: &DiagnosticOwner::Reason {
                    reason: ParkReason::Held {
                        actor: "later-owner".into(),
                    },
                },
                text: "one",
            },
        )
        .await
        .unwrap();
    assert_eq!(again, ids[0]);
    tx.commit().await.unwrap();
    let row = db
        .task_diagnostic_text("pages", &ids[0])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.source, "first-source");
    assert_eq!(row.owner, DiagnosticOwner::StoredCondition);
    assert_eq!(row.occurrence_count, 2);
    let first = db.task_diagnostic_texts("pages", None, 2).await.unwrap();
    assert!(first.has_more);
    assert_eq!(first.items.len(), 2);
    let last = db
        .task_diagnostic_texts("pages", first.next_cursor.as_deref(), 2)
        .await
        .unwrap();
    assert!(!last.has_more);
    assert!(last.next_cursor.is_none());
    let actual: Vec<_> = first
        .items
        .into_iter()
        .chain(last.items)
        .map(|r| r.id)
        .collect();
    assert_eq!(
        actual,
        vec![
            sorted_ids[3].clone(),
            sorted_ids[1].clone(),
            sorted_ids[2].clone(),
            sorted_ids[0].clone()
        ]
    );
    assert!(db
        .task_diagnostic_texts("other-task", None, 100)
        .await
        .unwrap()
        .items
        .is_empty());
    assert!(matches!(
        db.task_diagnostic_texts("pages", Some("invalid"), 2).await,
        Err(DbError::InvalidCursor)
    ));
}

#[tokio::test]
async fn additive_upgrade_and_revision_seven_preserve_existing_condition_bytes_and_attention() {
    let db = db().await;
    let mut before = Vec::new();
    for (id, annotation, blocked, failed, metadata) in [
        ("clear", None, None, None, json!({})),
        (
            "held",
            Some(json!({"type":"manual_stop","blocking_reason":"owner wait","blocked_by":"user"})),
            None,
            None,
            json!({}),
        ),
        (
            "failed",
            None,
            None,
            Some(
                json!({"kind":"executor_failed","reason":"execution failed","created_at":"2026-01-01"}),
            ),
            json!({}),
        ),
        (
            "wait",
            None,
            None,
            None,
            json!({"owner_wait":{"daemon_id":"exact-daemon","started_at":"2026-01-01"}}),
        ),
        (
            "large",
            Some(
                json!({"type":"workspace_error","blocking_reason":"problem","hook":(0..40).map(|i| (format!("field-{i}"), json!("x".repeat(1024)))).collect::<serde_json::Map<_,_>>()}),
            ),
            None,
            None,
            json!({}),
        ),
    ] {
        task(&db, id).await;
        sqlx::query("UPDATE task SET error_annotation=?,blocked_json=?,failed_json=?,metadata_json=? WHERE id=?")
            .bind(annotation.map(|v| v.to_string())).bind(blocked.map(|v: serde_json::Value| v.to_string()))
            .bind(failed.map(|v| v.to_string())).bind(metadata.to_string()).bind(id).execute(db.pool()).await.unwrap();
        let row = TaskRepo::get_by_id(&db, id, false).await.unwrap().unwrap();
        let mut c = db.pool().acquire().await.unwrap();
        let facts = ConditionFacts::load(&mut c, id).await.unwrap();
        let legacy = facts.condition(&ConditionInput::from(&row));
        let raw = encode(&legacy); // Same legacy mapper and encoding as MAIN.
        sqlx::query("UPDATE task SET condition_json=? WHERE id=?")
            .bind(&raw)
            .bind(id)
            .execute(&mut *c)
            .await
            .unwrap();
        drop(c);
        before.push((id, raw, material_blocker(&legacy), legacy.public()));
    }
    db.set_setting(
        MAPPING_REVISION_KEY,
        &MAPPING_REVISION.to_string(),
        &crate::now_rfc3339(),
    )
    .await
    .unwrap();
    sqlx::raw_sql(
        "DROP TABLE task_diagnostic_text; DELETE FROM _migration WHERE version=202610250745;",
    )
    .execute(db.pool())
    .await
    .unwrap();
    crate::run_migrations(db.pool()).await.unwrap();
    assert_eq!(
        MAPPING_REVISION, 8,
        "no forced re-encoding walk for the additive checkpoint"
    );
    assert!(!db.backfill_task_conditions_if_stale().await.unwrap());
    assert_eq!(db.check_task_conditions(100).await.unwrap().repaired, 0);
    for (id, bytes, material, public) in before {
        assert_eq!(
            raw(&db, id).await,
            bytes,
            "{id}: upgrade leaves condition bytes intact"
        );
        let condition = db.task_condition(id).await.unwrap();
        assert_eq!(
            material_blocker(&condition),
            material,
            "{id}: Attention digest input"
        );
        assert_eq!(condition.public(), public, "{id}: public condition");
    }
}

#[tokio::test]
async fn corrupt_legacy_blockers_rebuild_with_the_base_condition_and_attention_in_one_sweep() {
    let db = db().await;
    let mut expected = Vec::new();
    for (id, annotation, blocked, failed, metadata) in [
        (
            "corrupt-held",
            Some(json!({"type":"manual_stop","blocking_reason":"wait","blocked_by":"user"})),
            None,
            None,
            json!({}),
        ),
        (
            "corrupt-failure",
            None,
            None,
            Some(json!({"kind":"executor_failed","reason":"failure","created_at":"2026-01-01"})),
            json!({}),
        ),
        (
            "corrupt-blocked",
            None,
            Some(json!({"kind":"workspace_error","reason":"blocked","created_at":"2026-01-01"})),
            None,
            json!({}),
        ),
        (
            "corrupt-wait",
            None,
            None,
            None,
            json!({"owner_wait":{"daemon_id":"exact-owner","started_at":"2026-01-01"}}),
        ),
    ] {
        task(&db, id).await;
        sqlx::query("UPDATE task SET error_annotation=?,blocked_json=?,failed_json=?,metadata_json=?,condition_json='{}' WHERE id=?")
            .bind(annotation.map(|v| v.to_string())).bind(blocked.map(|v| v.to_string()))
            .bind(failed.map(|v| v.to_string())).bind(metadata.to_string()).bind(id).execute(db.pool()).await.unwrap();
        let row = TaskRepo::get_by_id(&db, id, false).await.unwrap().unwrap();
        let mut c = db.pool().acquire().await.unwrap();
        let facts = ConditionFacts::load(&mut c, id).await.unwrap();
        expected.push((id, facts.condition(&ConditionInput::from(&row))));
    }
    assert_eq!(db.check_task_conditions(100).await.unwrap().repaired, 4);
    for (id, legacy) in expected {
        let actual = db.task_condition(id).await.unwrap();
        assert_eq!(actual.typed(), legacy.typed(), "{id}");
        assert_eq!(actual.public(), legacy.public(), "{id}");
        assert_eq!(material_blocker(&actual), material_blocker(&legacy), "{id}");
        assert!(!recovery::unreadable(&actual));
        assert_eq!(
            db.task_diagnostic_texts(id, None, 100).await.unwrap().items[0].text,
            "{}"
        );
    }
    assert_eq!(
        db.check_task_conditions(100).await.unwrap().repaired,
        4,
        "the clean second lap adds no repairs"
    );
}

#[tokio::test]
async fn implicit_statement_preserves_a_new_unconverted_condition_between_recomputes() {
    let db = std::sync::Arc::new(db().await);
    task(&db, "fresh-scope").await;
    let at = crate::now_rfc3339();
    let statement = ConditionStatement::Hold {
        actor: "owner".into(),
        reason: "hold".into(),
        at: at.clone(),
    };
    write::with_statement("fresh-scope", statement, async {
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        sqlx::query("UPDATE task SET error_annotation=?,blocked_json=? WHERE id='fresh-scope'")
            .bind(ConditionStatement::hold_operator_text("owner", "hold", &at))
            .bind(json!({"kind":"manual_stop","reason":"hold","created_at":at}).to_string())
            .execute(&mut *tx).await.unwrap();
        produce(&mut tx, "fresh-scope", ConditionChange::Full).await.unwrap();
        tx.commit().await.unwrap();
        let other = db.clone();
        // A separate producer does not inherit this mutation's task-local statement.
        tokio::spawn(async move {
            let mut tx = crate::begin_immediate(other.pool()).await.unwrap();
            sqlx::query("UPDATE task SET metadata_json='{\"owner_wait\":{\"daemon_id\":\"new-owner\"}}' WHERE id='fresh-scope'")
                .execute(&mut *tx).await.unwrap();
            produce(&mut tx, "fresh-scope", ConditionChange::Legacy).await.unwrap();
            tx.commit().await.unwrap();
        }).await.unwrap();
        let latest = db.task_condition("fresh-scope").await.unwrap();
        assert!(latest.reasons().any(|r| matches!(r, ParkReason::OwnerOffline { daemon_id: Some(id), .. } if id == "new-owner")));
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        produce(&mut tx, "fresh-scope", ConditionChange::Full).await.unwrap();
        tx.commit().await.unwrap();
        assert_eq!(db.task_condition("fresh-scope").await.unwrap().typed(), latest.typed(), "a consumed statement preserves the latest row rather than its old cached result");
    }).await;
}

#[tokio::test]
async fn real_hold_with_200kb_of_json_escaped_text_keeps_operator_json_and_material() {
    let db = db().await;
    task(&db, "escaped-hold").await;
    let reason = "\0".repeat(200_000);
    super::statement_tests::hold(&db, "escaped-hold", &reason).await;
    let row = TaskRepo::get_by_id(&db, "escaped-hold", false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row.condition.read().operator_reason,
        row.error_annotation
            .as_deref()
            .map(readers::bound_operator_reason)
    );
    let mut c = db.pool().acquire().await.unwrap();
    let expected = ConditionFacts::load(&mut c, "escaped-hold")
        .await
        .unwrap()
        .condition(&ConditionInput::from(&row));
    drop(c);
    assert_eq!(
        material_blocker(&row.condition),
        material_blocker(&expected)
    );
    assert_eq!(
        row.condition.evidence().material,
        expected.evidence().material
    );
    let texts = db
        .task_diagnostic_texts("escaped-hold", None, 100)
        .await
        .unwrap();
    assert!(texts
        .items
        .iter()
        .any(|text| text.text == reason && text.byte_length == 200_000));
    assert_eq!(
        db.check_task_conditions_of(&["escaped-hold".into()])
            .await
            .unwrap(),
        0
    );
}
