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
    let before_digest = input.channel_digest();
    let before = compose_condition(&input);
    input.error_annotation.as_mut().unwrap().pop();
    input.error_annotation.as_mut().unwrap().push('b');
    let after = compose_condition(&input);
    assert_eq!(
        before.evidence().error_annotation,
        after.evidence().error_annotation
    );
    assert_ne!(before_digest, input.channel_digest());
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
        db.repair_condition(
            "race",
            &checks::Repair {
                version: row.version,
                stored,
                read_digest: row.condition_digest.clone(),
                expected,
                digest: ConditionInput::from(&row).channel_digest(),
                stamp_only: false
            }
        )
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
        sqlx::query("UPDATE task SET condition_digest=NULL,condition_json=?,archived_at='2026-10-01' WHERE id=?")
            .bind(encode(&condition))
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
    sqlx::query("UPDATE task SET condition_digest=NULL,condition_json=? WHERE id='encoding'")
        .bind(encode(&condition))
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

/// Probe A: a sweep lap that completes while the revision walk is mid-flight
/// takes the shared pass, including the walk's encoding-only repairs.
#[tokio::test]
async fn clean_sweep_lap_during_revision_walk_excludes_upgrades() {
    let db = db().await;
    for id in ["a", "b", "c"] {
        task(&db, id).await;
        let condition = db.task_condition(id).await.unwrap();
        sqlx::query("UPDATE task SET condition_digest=NULL,condition_json=? WHERE id=?")
            .bind(encode(&condition))
            .bind(id)
            .execute(db.pool())
            .await
            .unwrap();
    }
    db.set_setting(MAPPING_REVISION_KEY, "7", &crate::now_rfc3339())
        .await
        .unwrap();
    assert!(db.backfill_task_conditions_if_stale().await.unwrap());
    let page = db.check_task_conditions(1).await.unwrap();
    assert_eq!((page.checked, page.repaired), (1, 1));
    // The scheduler sweep's lap over open Tasks ends here with no repairs.
    db.complete_condition_pass(0);
    let last = db.condition_check_status().last_pass.unwrap();
    assert_eq!(
        (last.checked, last.repaired),
        (0, 0),
        "clean sweep must exclude upgrade work"
    );
    eprintln!(
        "PROBE A last_pass after mid-backfill sweep lap: checked={} repaired={}",
        last.checked, last.repaired
    );
}

/// Probe C: channel edits that leave typed() equal but change evidence copies
/// that public() still reads; the encoding-only path keeps the stale copies.
#[tokio::test]
async fn null_digest_repair_refreshes_workflow_loop_public_evidence() {
    let db = db().await;
    task(&db, "loop").await;
    let a = r#"{"source":"workflow_loop","blocked_at":"2026-10-01T00:00:00Z","reason":"loop"}"#;
    let b = r#"{"source":"workflow_loop","blocked_at":"2026-10-02T00:00:00Z","reason":"loop"}"#;
    let mut tx = db.pool().begin().await.unwrap();
    sqlx::query("UPDATE task SET blocked_json=? WHERE id='loop'")
        .bind(a)
        .execute(&mut *tx)
        .await
        .unwrap();
    db.sync_condition_in_tx(&mut tx, "loop").await.unwrap();
    tx.commit().await.unwrap();
    let condition = db.task_condition("loop").await.unwrap();
    sqlx::query(
        "UPDATE task SET condition_digest=NULL,condition_json=?, blocked_json=? WHERE id='loop'",
    )
    .bind(encode(&condition))
    .bind(b)
    .execute(db.pool())
    .await
    .unwrap();
    let old = condition;
    let current_input = ConditionInput {
        blocked_json: Some(b.into()),
        ..Default::default()
    };
    assert_eq!(old.evidence().blocked_json.as_deref(), Some(a));
    assert_eq!(
        compose_condition(&ConditionInput {
            blocked_json: Some(a.into()),
            ..Default::default()
        })
        .typed(),
        compose_condition(&current_input).typed()
    );
    let repaired = db.check_task_conditions_of(&["loop".into()]).await.unwrap();
    assert_eq!(repaired, 1);
    let after = db.task_condition("loop").await.unwrap();
    let current = TaskRepo::get_by_id(&db, "loop", false)
        .await
        .unwrap()
        .unwrap();
    let truth = compose_condition(&ConditionInput::from(&current));
    assert_eq!(
        after.public(),
        truth.public(),
        "upgrade must refresh public evidence"
    );
    assert_eq!(after.evidence().blocked_json.as_deref(), Some(b));
    assert!(db.task_condition_violations().await.unwrap().is_empty());
    eprintln!(
        "PROBE C e2e: repaired={repaired} stored_blocked_copy={:?}\n   public_equal_to_truth={} violations={:?}",
        after.evidence().blocked_json,
        after.public() == truth.public(),
        db.task_condition_violations().await.unwrap(),
    );
    eprintln!(
        "   stored public={}\n   truth  public={}",
        serde_json::to_string(&after.public()).unwrap(),
        serde_json::to_string(&truth.public()).unwrap()
    );
}

/// Probe E: schedule-dirty fan-out of the revision walk under one flat root.
#[tokio::test]
async fn revision_walk_stamps_400_children_without_invalidating_schedule_or_lists() {
    let db = db().await;
    task(&db, "root").await;
    let n = 400;
    let mut tx = db.pool().begin().await.unwrap();
    for i in 0..n {
        sqlx::query("INSERT INTO task(id,project_id,parent_task_id,title,status,subtask_order,created_at,updated_at) VALUES (?,?,?,'child','todo',?,'2026-10-25','2026-10-25')")
            .bind(format!("c{i:04}")).bind("p").bind("root").bind(i).execute(&mut *tx).await.unwrap();
    }
    tx.commit().await.unwrap();
    for _ in 0..3 {
        let ids: Vec<String> = sqlx::query_scalar("SELECT id FROM task")
            .fetch_all(db.pool())
            .await
            .unwrap();
        db.check_task_conditions_of(&ids).await.unwrap();
    }
    let rows: Vec<(String, String)> = sqlx::query_as("SELECT id, condition_json FROM task")
        .fetch_all(db.pool())
        .await
        .unwrap();
    for (id, raw) in rows {
        sqlx::query("UPDATE task SET condition_digest=NULL,condition_json=? WHERE id=?")
            .bind(raw)
            .bind(id)
            .execute(db.pool())
            .await
            .unwrap();
    }
    sqlx::query("DELETE FROM task_schedule_dirty")
        .execute(db.pool())
        .await
        .unwrap();
    let lr0: i64 = sqlx::query_scalar("SELECT list_revision FROM project WHERE id='p'")
        .fetch_one(db.pool())
        .await
        .unwrap();
    db.set_setting(MAPPING_REVISION_KEY, "7", &crate::now_rfc3339())
        .await
        .unwrap();
    let started = std::time::Instant::now();
    while db.backfill_task_conditions_if_stale().await.unwrap() {}
    let elapsed = started.elapsed();
    let (rows, gens): (i64, i64) =
        sqlx::query_as("SELECT count(*), COALESCE(sum(generation),0) FROM task_schedule_dirty")
            .fetch_one(db.pool())
            .await
            .unwrap();
    let lr1: i64 = sqlx::query_scalar("SELECT list_revision FROM project WHERE id='p'")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let status = db.condition_check_status();
    assert_eq!(
        (rows, gens, lr1 - lr0),
        (0, 0, 0),
        "stamps must cause no scheduler or list invalidation"
    );
    eprintln!("PROBE E tasks={} repaired={} dirty_rows={rows} dirty_generation_sum={gens} list_revision_bumps={} elapsed={elapsed:?}", n + 1, status.repaired, lr1 - lr0);
}

#[tokio::test]
async fn historical_walk_rewrites_do_not_enter_a_clean_sweep_pass() {
    let db = db().await;
    task(&db, "historical").await;
    sqlx::query("UPDATE task SET condition_digest=NULL,blocked_json='{}' WHERE id='historical'")
        .execute(db.pool())
        .await
        .unwrap();
    db.set_setting(MAPPING_REVISION_KEY, "7", &crate::now_rfc3339())
        .await
        .unwrap();
    while db.backfill_task_conditions_if_stale().await.unwrap() {}
    let status = db.condition_check_status();
    assert_eq!(
        (
            status.walk.checked,
            status.walk.repaired,
            status.walk.stamped
        ),
        (1, 1, 0)
    );
    assert!(status.last_pass.is_none());
    db.complete_condition_pass(0);
    let pass = db.condition_check_status().last_pass.unwrap();
    assert_eq!((pass.checked, pass.repaired), (0, 0));
    assert!(db.task_condition_violations().await.unwrap().is_empty());
}

#[tokio::test]
async fn invariant_checks_the_task_returned_by_the_writer() {
    let db = db().await;
    let current = task(&db, "returned").await;
    db.check_task_condition_invariant(&current).await.unwrap();
    let mut wrong_channels = current.clone();
    wrong_channels.blocked_json = Some("{}".into());
    assert!(db
        .check_task_condition_invariant(&wrong_channels)
        .await
        .is_err());
    let mut wrong_cache = current;
    wrong_cache.condition = TaskCondition::default();
    assert!(db
        .check_task_condition_invariant(&wrong_cache)
        .await
        .is_err());
}

#[tokio::test]
async fn metadata_override_clears_both_non_text_collections() {
    let db = db().await;
    task(&db, "metadata-blob").await;
    sqlx::query("UPDATE task SET metadata_json=? WHERE id='metadata-blob'")
        .bind(vec![0x80_u8])
        .execute(db.pool())
        .await
        .unwrap();
    let mut tx = db.pool().begin().await.unwrap();
    let produced = producers::derive(
        &mut tx,
        "metadata-blob",
        ConditionChange::Legacy,
        Some(Some("{}")),
        None,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(produced.input.non_text.is_empty());
    assert!(produced.input.non_text_bytes.is_empty());
    assert_eq!(
        produced.input.channel_digest(),
        ConditionInput::default().channel_digest()
    );
}

#[tokio::test]
async fn a_stated_null_digest_stamp_preserves_exact_cache_bytes_and_diagnostic_ids() {
    let db = db().await;
    task(&db, "stated-stamp").await;
    let input = ConditionInput {
        error_annotation: Some(
            r#"{"type":"manual_stop","blocking_reason":"owner wait","blocked_by":"owner"}"#.into(),
        ),
        ..Default::default()
    };
    let mut tx = db.pool().begin().await.unwrap();
    sqlx::query("UPDATE task SET error_annotation=? WHERE id='stated-stamp'")
        .bind(&input.error_annotation)
        .execute(&mut *tx)
        .await
        .unwrap();
    db.sync_condition_in_tx(&mut tx, "stated-stamp")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let composed = db.task_condition("stated-stamp").await.unwrap();
    let mut stored = serde_json::to_value(&composed).unwrap();
    stored["evidence"]["stated"] = serde_json::json!(true);
    stored["evidence"]["error_annotation"] = serde_json::Value::Null;
    stored["evidence"]["diagnostic_ids"] = serde_json::json!(["retained-diagnostic"]);
    let raw = serde_json::to_string_pretty(&stored).unwrap();
    let prior = decode(&raw).unwrap();
    assert_eq!(prior.typed(), composed.typed());
    assert!(
        !prior.bounded_channels_equal(&composed),
        "the stated branch must allow missing copies"
    );
    sqlx::query("UPDATE task SET condition_json=?,condition_digest=NULL WHERE id='stated-stamp'")
        .bind(&raw)
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        db.check_task_conditions_of(&["stated-stamp".into()])
            .await
            .unwrap(),
        1
    );
    let (after, digest): (String, String) =
        sqlx::query_as("SELECT condition_json,condition_digest FROM task WHERE id='stated-stamp'")
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(after, raw);
    assert_eq!(digest, input.channel_digest());
    assert!(decode(&after).unwrap().evidence().stated);
}
