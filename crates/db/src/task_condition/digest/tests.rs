use super::*;
use crate::task_condition::tests::{db, task};
use crate::{SystemSettingRepo, TaskRepo};

#[test]
fn raw_members_frame_absent_null_empty_and_preserve_rendered_bytes() {
    for field in CHANNEL_FIELDS.iter().take(4) {
        let mut hashes = Vec::new();
        for raw in [
            None,
            Some("null"),
            Some("{}"),
            Some(" { } "),
            Some("\"{}\""),
        ] {
            let mut input = ConditionInput::default();
            *match field {
                ConditionField::ErrorAnnotation => &mut input.error_annotation,
                ConditionField::BlockedJson => &mut input.blocked_json,
                ConditionField::FailedJson => &mut input.failed_json,
                ConditionField::EntryBarrierJson => &mut input.entry_barrier_json,
                _ => unreachable!(),
            } = raw.map(str::to_owned);
            hashes.push(input.channel_digest());
        }
        hashes.sort();
        hashes.dedup();
        assert_eq!(hashes.len(), 5, "{field:?}");
    }
    let mut input = ConditionInput {
        error_annotation: Some(format!("{}a", "x".repeat(EVIDENCE_VALUE_LIMIT + 100))),
        ..Default::default()
    };
    let before = compose_condition(&input);
    input.error_annotation.as_mut().unwrap().pop();
    input.error_annotation.as_mut().unwrap().push('b');
    let after = compose_condition(&input);
    assert_eq!(
        before.evidence().error_annotation,
        after.evidence().error_annotation
    );
    assert_ne!(
        before.evidence().channel_digest,
        after.evidence().channel_digest
    );
}

#[test]
fn metadata_hashes_exactly_the_producers_members_with_raw_framing() {
    let input = |raw: &str| ConditionInput {
        metadata_json: Some(raw.into()),
        ..Default::default()
    };
    assert_eq!(
        ConditionInput::default().channel_digest(),
        input("{}").channel_digest()
    );
    assert_eq!(
        input("{}").channel_digest(),
        input(r#"{"unrelated":{"a":1}}"#).channel_digest()
    );
    for key in EVIDENCE_METADATA_KEYS
        .iter()
        .copied()
        .chain(["queued_recovery"])
    {
        let hashes = [
            "{}".to_owned(),
            format!(r#"{{"{key}":null}}"#),
            format!(r#"{{"{key}":{{}}}}"#),
            format!(r#"{{"{key}":{{ }}}}"#),
        ]
        .map(|raw| input(&raw).channel_digest());
        let unique: std::collections::BTreeSet<_> = hashes.into_iter().collect();
        assert_eq!(unique.len(), 4, "{key}");
    }
}

#[tokio::test]
async fn raw_sql_staleness_and_unrelated_metadata_are_checked_without_versions() {
    let db = db().await;
    let row = task(&db, "raw").await;
    let mut tx = db.pool().begin().await.unwrap();
    sqlx::query("UPDATE task SET metadata_json='{}' WHERE id='raw'")
        .execute(&mut *tx)
        .await
        .unwrap();
    db.sync_condition_in_tx(&mut tx, "raw").await.unwrap();
    tx.commit().await.unwrap();
    sqlx::query("UPDATE task SET metadata_json='{\"unrelated\":123}' WHERE id='raw'")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        db.check_task_conditions_of(&["raw".into()]).await.unwrap(),
        0
    );
    let current = TaskRepo::get_by_id(&db, "raw", false)
        .await
        .unwrap()
        .unwrap();
    db.check_task_condition_invariant(&current).await.unwrap();
    sqlx::query("UPDATE task SET blocked_json='{}' WHERE id='raw'")
        .execute(db.pool())
        .await
        .unwrap();
    let current = TaskRepo::get_by_id(&db, "raw", false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.version, current.version);
    assert!(db.check_task_condition_invariant(&current).await.is_err());
    assert_eq!(db.task_condition_violations().await.unwrap(), ["raw"]);
    assert_eq!(
        db.check_task_conditions_of(&["raw".into()]).await.unwrap(),
        1
    );
    assert!(db.task_condition_violations().await.unwrap().is_empty());
}

#[tokio::test]
async fn repair_fences_a_channel_write_that_does_not_move_version_or_cache() {
    let db = db().await;
    let row = task(&db, "race").await;
    let stored: Vec<u8> = sqlx::query_scalar("SELECT condition_json FROM task WHERE id='race'")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let expected = String::from_utf8(stored.clone()).unwrap();
    sqlx::query("UPDATE task SET blocked_json='{}' WHERE id='race'")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        db.repair_condition("race", row.version, &stored, &expected)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn revision_repair_resumes_archived_rows_without_versions_attention_or_events() {
    let db = db().await;
    for id in ["a", "b", "c"] {
        task(&db, id).await;
        let condition = db.task_condition(id).await.unwrap();
        sqlx::query("UPDATE task SET condition_json=?,archived_at='2026-10-01' WHERE id=?")
            .bind(test_support::without_channel_digest(&encode(&condition)))
            .bind(id)
            .execute(db.pool())
            .await
            .unwrap();
    }
    let versions: Vec<(String, i64)> = sqlx::query_as("SELECT id,version FROM task ORDER BY id")
        .fetch_all(db.pool())
        .await
        .unwrap();
    let counts = |db: SqliteDb| async move {
        sqlx::query_as::<_, (i64, i64)>(
            "SELECT (SELECT count(*) FROM attention_projection),(SELECT count(*) FROM domain_event)",
        )
        .fetch_one(db.pool())
        .await
        .unwrap()
    };
    let before = counts(db.clone()).await;
    db.set_setting(MAPPING_REVISION_KEY, "7", &crate::now_rfc3339())
        .await
        .unwrap();
    assert!(db.backfill_task_conditions_if_stale().await.unwrap());
    assert_eq!(
        db.condition_check_status().checked,
        0,
        "discovery tick does no row work"
    );
    let page = db.check_task_conditions(1).await.unwrap();
    assert_eq!((page.checked, page.repaired), (1, 1));
    let restarted = SqliteDb::new(db.pool().clone());
    while restarted.backfill_task_conditions_if_stale().await.unwrap() {}
    assert_eq!(
        restarted.condition_check_status().checked,
        2,
        "resume after the committed first page"
    );
    assert!(restarted.condition_check_status().last_pass.is_none());
    assert_eq!(counts(db.clone()).await, before);
    let after: Vec<(String, i64)> = sqlx::query_as("SELECT id,version FROM task ORDER BY id")
        .fetch_all(db.pool())
        .await
        .unwrap();
    assert_eq!(after, versions);
    assert!(db.task_condition_violations().await.unwrap().is_empty());
    assert_eq!(
        db.get_setting(MAPPING_REVISION_KEY).await.unwrap(),
        Some(MAPPING_REVISION.to_string())
    );
}

#[tokio::test]
async fn an_encoding_only_sweep_repair_is_not_an_operator_invariant_issue() {
    let db = db().await;
    task(&db, "encoding").await;
    let condition = db.task_condition("encoding").await.unwrap();
    sqlx::query("UPDATE task SET condition_json=? WHERE id='encoding'")
        .bind(test_support::without_channel_digest(&encode(&condition)))
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        db.check_task_conditions_of(&["encoding".into()])
            .await
            .unwrap(),
        1
    );
    db.complete_condition_pass(0);
    let status = db.condition_check_status();
    assert_eq!(status.repaired, 1, "the repair still counts as work");
    assert_eq!(
        status.last_pass.unwrap().repaired,
        0,
        "upgrading adds no operator issue"
    );
}

#[tokio::test]
async fn non_text_channel_bytes_are_hashed_even_when_their_diagnosis_is_the_same() {
    let db = db().await;
    task(&db, "blob").await;
    let mut hashes = Vec::new();
    for bytes in [vec![0x80], vec![0x81], b"{}".to_vec()] {
        sqlx::query("UPDATE task SET error_annotation=? WHERE id='blob'")
            .bind(bytes)
            .execute(db.pool())
            .await
            .unwrap();
        let row = sqlx::query(&format!(
            "SELECT {CHANNEL_SELECT} FROM task WHERE id='blob'"
        ))
        .fetch_one(db.pool())
        .await
        .unwrap();
        let input = ConditionInput::from_row(&row).unwrap();
        assert_eq!(input.non_text, [ConditionField::ErrorAnnotation]);
        assert!(input.error_annotation.is_none());
        hashes.push(input.channel_digest());
    }
    let unique: std::collections::BTreeSet<_> = hashes.into_iter().collect();
    assert_eq!(unique.len(), 3);
}
