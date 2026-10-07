use super::tests::{claim, db, input, task};
use super::*;
use crate::{CreateDomainEvent, TaskRepo, TaskStepRepo};
use serde_json::{json, Value};

/// `material_blocker(condition)` is the interruption the
/// `task.interruption_changed` event states, minus what the incident digest
/// strips. The digest half, against incidents the Attention service really
/// materializes, is
/// `services::attention_service::tests::material_blocker_keeps_the_real_incident_digest`.
#[tokio::test]
async fn material_blocker_matches_the_interruption_event() {
    let db = db().await;
    let mut t = task(&db, "material").await;
    for (a, b, f) in material_shapes() {
        t.error_annotation = a.clone();
        t.blocked_json = b.clone();
        t.failed_json = f.clone();
        t.condition = map_legacy_condition(&LegacyConditionInput::from(&t));
        let old: Value =
            serde_json::from_str(&CreateDomainEvent::task_interruption_changed(&t).payload_json)
                .unwrap();
        let condition = map_legacy_condition(&LegacyConditionInput::from(&t));
        let projected = material_blocker(&condition);
        assert_eq!(
            json!(projected.requires_intervention),
            old["material_blocker"]["requires_intervention"],
            "{a:?} {b:?} {f:?}"
        );
        let mut interruption = old["material_blocker"]["interruption"].clone();
        crate::strip_attention_delivery_metadata(&mut interruption);
        assert_eq!(json!(projected.interruption), interruption);
        // The reporting execution is delivery metadata, not blocker identity.
        assert!(!json!(projected.interruption)
            .to_string()
            .contains("execution_id"));
    }
}
/// Every blocker shape today's writers produce, plus malformed and
/// non-object values in each column.
fn material_shapes() -> Vec<(Option<String>, Option<String>, Option<String>)> {
    let text = |value: Value| Some(value.to_string());
    let mut shapes = vec![
        (
            text(json!({"type":"manual_stop","blocking_reason":"held"})),
            None,
            None,
        ),
        (
            text(
                json!({"type":"review_needs_owner","blocking_reason":"finding","blocked_execution_id":"e"}),
            ),
            text(json!({"kind":"review_needs_owner","reason":"owner","execution_id":"e"})),
            None,
        ),
        (
            text(json!({"type":"merge_conflict","message":"advisory"})),
            None,
            None,
        ),
        (
            text(json!({"type":"retry_exhausted","message":"spent"})),
            text(json!({"kind":"retry_exhausted","reason":"spent"})),
            None,
        ),
        (
            None,
            None,
            text(json!({"kind":"executor_failed","reason":"failure"})),
        ),
        (
            text(json!({"type":"unknown","message":"retained"})),
            None,
            None,
        ),
        (
            None,
            text(json!({"kind":"manual_stop","reason":"manual"})),
            None,
        ),
        (
            text(json!({"type":"workspace_error","message":"m".repeat(10000)})),
            None,
            None,
        ),
        // Malformed and non-object values: the event decodes them as `{}`.
        (Some("not json".into()), None, None),
        (Some("[1,2]".into()), None, None),
        (Some("\"text\"".into()), None, None),
        (None, Some("not json".into()), None),
        (None, Some("42".into()), None),
        (None, None, Some("{".into())),
        (None, None, Some("[]".into())),
        (
            Some("not json".into()),
            text(json!({"kind":"workspace_error","reason":"real","execution_id":"e"})),
            None,
        ),
    ];
    for kind in LEGACY_BLOCKING_ANNOTATION_KINDS {
        let annotation = json!({"type":kind,"message":"detail","blocking_reason":"reason","blocked_by":"owner","blocked_execution_id":"execution"});
        let interruption = json!({"kind":kind,"reason":"reason","execution_id":"execution","details":{"key":"detail"}});
        shapes.push((text(annotation.clone()), None, None));
        shapes.push((text(annotation.clone()), text(interruption.clone()), None));
        shapes.push((
            text(annotation),
            text(interruption.clone()),
            text(interruption),
        ));
    }
    shapes
}
/// Legacy reads a wrongly typed wait key as absent, so the condition keeps
/// the diagnosis and does not park.
#[test]
fn wrongly_typed_wait_keys_stay_non_parking_observations() {
    for metadata in [
        r#"{"awaiting_human":"yes"}"#,
        r#"{"coordination_review_pending":"yes"}"#,
        r#"{"deferred_dispatch":42}"#,
        r#"{"dispatch_disposition":"x"}"#,
        r#"{"paused_integration":[1]}"#,
        r#"{"owner_wait":42}"#,
        r#"{"environment_wait":"x"}"#,
        r#"{"placement_refusal":7}"#,
        r#"{"daemon_upgrade_refusal":"x"}"#,
        r#"{"plan_settlement_wait":true}"#,
    ] {
        let mapped = map_legacy_condition(&LegacyConditionInput {
            metadata_json: Some(metadata.into()),
            ..Default::default()
        });
        assert!(!mapped.is_blocked(), "{metadata}: {mapped:?}");
        assert!(!mapped.evidence().observations.is_empty(), "{metadata}");
    }
    let environment = input(
        None,
        None,
        None,
        None,
        Some(json!({"deferred_dispatch":{"kind":"environment_probe_pending","reason":"probe"}})),
    );
    assert!(matches!(
        map_legacy_condition(&environment),
        TaskCondition::Deferred {
            until: None,
            reason: RetryCause::Environment,
            ..
        }
    ));
}
/// Legacy refuses to dispatch a Task with any entry barrier
/// (`active_recovery.rs`, `stranded_hooks.rs`, `task_hierarchy::root_blocked`)
/// and skips one whose metadata does not parse (`task.metadata()?` in both
/// scans). Both park.
#[test]
fn entry_barriers_and_unparsable_metadata_park_like_legacy() {
    for barrier in [
        json!({"state":"review","status":"running"}).to_string(),
        json!({"state":"review","status":"passed"}).to_string(),
        json!({"state":"review"}).to_string(),
        json!({}).to_string(),
        json!([1]).to_string(),
        "not json".to_owned(),
    ] {
        let mapped = map_legacy_condition(&LegacyConditionInput {
            entry_barrier_json: Some(barrier.clone()),
            ..Default::default()
        });
        assert!(
            matches!(
                &mapped,
                TaskCondition::Parked {
                    primary: ParkReason::UnknownCondition { source, .. },
                    ..
                } if source.field == LegacyConditionField::EntryBarrierJson
            ),
            "{barrier}: {mapped:?}"
        );
        assert!(mapped.evidence().observations.is_empty(), "{barrier}");
    }
    for metadata in ["not JSON", "[]", "42"] {
        let mapped = map_legacy_condition(&LegacyConditionInput {
            metadata_json: Some(metadata.into()),
            ..Default::default()
        });
        assert!(
            matches!(
                &mapped,
                TaskCondition::Parked {
                    primary: ParkReason::UnknownCondition { source, .. },
                    ..
                } if source.field == LegacyConditionField::MetadataJson
            ),
            "{metadata}: {mapped:?}"
        );
    }
}
#[tokio::test]
async fn bounded_repair_changes_only_shadow_and_reports_counts() {
    let db = db().await;
    for i in 0..12 {
        task(&db, &format!("r{i:02}")).await;
    }
    sqlx::query("UPDATE task SET condition_json='{\"kind\":\"corrupt\"}'")
        .execute(db.pool())
        .await
        .unwrap();
    let before: Vec<(String, i64, String, String, i64)> =
        sqlx::query_as("SELECT id,version,status,updated_at,status_epoch FROM task ORDER BY id")
            .fetch_all(db.pool())
            .await
            .unwrap();
    let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_event")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let first = db.check_task_conditions(3).await.unwrap();
    assert!(first.checked <= 3);
    assert_eq!(first.checked, first.repaired);
    for _ in 0..20 {
        db.check_task_conditions(3).await.unwrap();
        if db.task_condition_violations().await.unwrap().is_empty() {
            break;
        }
    }
    assert!(db.task_condition_violations().await.unwrap().is_empty());
    let after: Vec<(String, i64, String, String, i64)> =
        sqlx::query_as("SELECT id,version,status,updated_at,status_epoch FROM task ORDER BY id")
            .fetch_all(db.pool())
            .await
            .unwrap();
    assert_eq!(before, after);
    assert_eq!(
        events,
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM domain_event")
            .fetch_one(db.pool())
            .await
            .unwrap()
    );
    assert!(db.condition_check_status().repaired >= 12);
}
#[tokio::test]
async fn children_and_lifecycle_have_real_durable_witnesses() {
    let db = db().await;
    let root = task(&db, "root-witness").await;
    let child = task(&db, "child-witness").await;
    sqlx::query("UPDATE task SET parent_task_id=? WHERE id=?")
        .bind(&root.id)
        .bind(&child.id)
        .execute(db.pool())
        .await
        .unwrap();
    let step = claim(&db, &root.id).await;
    crate::task_writer::in_task_step(step, async {
        TaskRepo::mutate_metadata(
            &db,
            &root.id,
            None,
            vec![crate::TaskMetadataMutation::Set {
                key: "coordination_review_pending".into(),
                value: json!(true),
            }],
            &crate::now_rfc3339(),
        )
        .await
        .unwrap();
        let condition = db.task_condition(&root.id).await.unwrap();
        assert!(matches!(
            condition,
            TaskCondition::Parked {
                primary: ParkReason::Children { .. },
                ..
            }
        ));
        assert!(condition
            .evidence()
            .witnesses
            .iter()
            .any(|w| matches!(w,ConditionWitness::Child{task_id,..}if task_id==&child.id)));
        TaskRepo::mutate_metadata(&db, &root.id, None, vec![crate::TaskMetadataMutation::Set {key:"awaiting_human".into(),value:json!(true)}], &crate::now_rfc3339()).await.unwrap();
        assert!(matches!(db.task_condition(&root.id).await.unwrap(), TaskCondition::Parked {primary:ParkReason::HumanDecision {..}, additional,..} if additional.iter().any(|r| matches!(r,ParkReason::Children{..}))));
        TaskRepo::mutate_metadata(&db, &root.id, None, vec![crate::TaskMetadataMutation::Remove {key:"awaiting_human".into()}], &crate::now_rfc3339()).await.unwrap();
    })
    .await;
    let id = crate::new_uuid_v4();
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    db.enqueue_step_in_tx(
        &mut tx,
        &crate::EnqueueTaskStep {
            id: id.clone(),
            task_id: child.id.clone(),
            kind: "hooks".into(),
            payload_json: "{}".into(),
            causation_step_id: None,
            causation_key: id.clone(),
            chain_id: id.clone(),
            chain_position: 1,
            expected_status: "todo".into(),
            expected_version: child.version,
            expected_epoch: None,
            lane: "fast".into(),
            available_at: crate::now_rfc3339(),
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert!(
        matches!(db.task_condition(&child.id).await.unwrap(),TaskCondition::Entering{step_id,..}if step_id==id)
    );
    // Content version changes do not change the child wait witness.
    let before = db.task_condition(&root.id).await.unwrap();
    sqlx::query("UPDATE task SET version=version+1 WHERE id=?")
        .bind(&child.id)
        .execute(db.pool())
        .await
        .unwrap();
    let mut connection = db.pool().acquire().await.unwrap();
    let facts = ConditionFacts::load(&mut connection, &root.id)
        .await
        .unwrap();
    drop(connection);
    let root_task = TaskRepo::get_by_id(&db, &root.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        before,
        facts.apply(map_legacy_condition(&LegacyConditionInput::from(
            &root_task
        )))
    );
    // A real terminal workflow entry (a custom workflow would use its own kind).
    sqlx::query("UPDATE task SET status='done',version=version+1 WHERE id=?")
        .bind(&child.id)
        .execute(db.pool())
        .await
        .unwrap();
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    db.sync_condition_in_tx(&mut tx, &child.id).await.unwrap();
    tx.commit().await.unwrap();
    assert!(matches!(
        db.task_condition(&child.id).await.unwrap(),
        TaskCondition::Settled { .. }
    ));
    assert!(matches!(
        db.task_condition(&root.id).await.unwrap(),
        TaskCondition::Deferred {
            reason: RetryCause::ChildrenReady,
            ..
        }
    ));
    assert!(db.task_condition_violations().await.unwrap().is_empty());
}

#[tokio::test]
async fn ledger_and_remote_cancel_producers_refresh_durable_witnesses() {
    let db = db().await;
    let t = task(&db, "receipt-witness").await;
    let step = claim(&db, &t.id).await;
    crate::task_writer::in_task_step(step.clone(), async {
        let now = crate::now_rfc3339();
        crate::task_writer::TaskQuery::new(&db, &t.id, "UPDATE task SET blocked_json=? WHERE id=?")
            .bind(json!({"kind":"retry_exhausted"}).to_string()).bind(&t.id)
            .execute(db.pool()).await.unwrap();
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        crate::budget::charge(&mut tx, &t.id, "execution", 3, &step.id).await.unwrap();
        tx.commit().await.unwrap();
        let condition = db.task_condition(&t.id).await.unwrap();
        assert!(condition.evidence().witnesses.iter().any(|w| matches!(w, ConditionWitness::Budget { key, spent: 1, .. } if key == "execution")));
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().to_string_lossy();
        sqlx::query("INSERT INTO repo(id,project_id,name,default_branch,created_at,updated_at) VALUES('receipt-repo','p','r','main',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO workspace(id,task_id,repo_id,worktree_path,branch,status,created_at,updated_at) VALUES('removed-workspace',?,'receipt-repo',?,'task/receipt','ready',?,?)").bind(&t.id).bind(path.as_ref()).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,status,created_at,updated_at) VALUES('receipt-location','receipt-repo','server',?,'primary_checkout','ready',?,?)").bind(path.as_ref()).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO workspace_placement(id,workspace_id,task_id,owner_kind,repo_location_id,workspace_handle,state,selected_by,selection_reason,created_at,updated_at) VALUES('placement','removed-workspace',?,'server','receipt-location','handle','ready','backfill','{}',?,?)").bind(&t.id).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO task_remote_operation(operation_id,step_id,workspace_id,placement_id,daemon_id,runtime_id,generation,expected_epoch,state,created_at) VALUES('receipt-op',?,'removed-workspace','placement','owner','runtime',1,0,'running',?)")
            .bind(&step.id).bind(&now).execute(db.pool()).await.unwrap();
        crate::task_writer::TaskQuery::new(&db,&t.id,"UPDATE task SET blocked_json=NULL WHERE id=?").bind(&t.id).execute(db.pool()).await.unwrap();
        let operation = db.running_remote_task_operations(&step.id).await.unwrap().remove(0);
        db.mark_pending_remote_cancel(&operation).await.unwrap();
        assert!(db.task_condition(&t.id).await.unwrap().evidence().witnesses.iter().any(|w| matches!(w, ConditionWitness::Operation { operation_id, .. } if operation_id == "receipt-op")));
        assert!(matches!(db.task_condition(&t.id).await.unwrap(), TaskCondition::Parked {primary:ParkReason::RemoteCancelPending {..},..}));
        db.acknowledge_remote_cancel(&operation).await.unwrap();
        assert!(!db.task_condition(&t.id).await.unwrap().evidence().witnesses.iter().any(|w| matches!(w, ConditionWitness::Operation { .. })));
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        crate::budget::reset(&mut tx, &t.id, "execution", "new-window").await.unwrap();
        tx.commit().await.unwrap();
        assert!(!db.task_condition(&t.id).await.unwrap().evidence().witnesses.iter().any(|w| matches!(w, ConditionWitness::Budget { .. })));
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        crate::budget::charge(&mut tx, &t.id, "gate:custom", 3, &step.id).await.unwrap();
        crate::budget::reset_all(&mut tx, &t.id, "all-reset").await.unwrap();
        tx.commit().await.unwrap();
        assert!(db.task_condition_violations().await.unwrap().is_empty());
    }).await;
}

#[tokio::test]
async fn inherited_child_execution_and_custom_terminal_use_their_actual_workflows() {
    let db = db().await;
    let root = task(&db, "custom-root").await;
    let child = task(&db, "custom-child").await;
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    sqlx::query("UPDATE project SET workflow_definition=? WHERE id='p'")
        .bind(json!({"states":[{"name":"root_work","kind":"active","role":"planner"},{"name":"released","kind":"terminal"}]}).to_string())
        .execute(&mut *tx).await.unwrap();
    sqlx::query("UPDATE task SET parent_task_id=?,status='in_progress' WHERE id=?")
        .bind(&root.id)
        .bind(&child.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    // The raw workflow rewrite also changed whether the root's `todo` is an
    // initial state; a real workflow edit re-states such Tasks itself.
    db.sync_condition_in_tx(&mut tx, &root.id).await.unwrap();
    let now = crate::now_rfc3339();
    sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,created_at,status_epoch) VALUES('child-entry',?,'todo','in_progress','system','entry',?,(SELECT status_epoch FROM task WHERE id=?))").bind(&child.id).bind(&now).bind(&child.id).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at,executor_config_snapshot_json) VALUES('child-run',?,'executor','running',?,?,'{\"state_entry_token\":\"child-entry\",\"task_state\":\"in_progress\"}')")
        .bind(&child.id).bind(&now).bind(&now).execute(&mut *tx).await.unwrap();
    db.sync_condition_in_tx(&mut tx, &child.id).await.unwrap();
    tx.commit().await.unwrap();
    assert!(
        matches!(db.task_condition(&child.id).await.unwrap(), TaskCondition::Running { execution_id, .. } if execution_id == "child-run")
    );
    let before = db.task_condition(&child.id).await.unwrap();
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,created_at,status_epoch) VALUES('child-audit',?,'in_progress','in_progress','user','reorder',?,(SELECT status_epoch FROM task WHERE id=?))")
        .bind(&child.id).bind(crate::now_rfc3339()).bind(&child.id).execute(&mut *tx).await.unwrap();
    db.sync_condition_in_tx(&mut tx, &child.id).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        before,
        db.task_condition(&child.id).await.unwrap(),
        "same-entry audit cannot replace the running owner or since"
    );
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    sqlx::query("UPDATE task SET status='released' WHERE id=?")
        .bind(&child.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    db.sync_condition_in_tx(&mut tx, &child.id).await.unwrap();
    tx.commit().await.unwrap();
    assert!(matches!(
        db.task_condition(&child.id).await.unwrap(),
        TaskCondition::Settled {
            outcome: TerminalOutcome::Completed,
            ..
        }
    ));
    assert!(db.task_condition_violations().await.unwrap().is_empty());
}

async fn file_db(directory: &tempfile::TempDir) -> SqliteDb {
    let pool = crate::create_sqlite_pool(&format!(
        "sqlite:{}",
        directory.path().join("conditions.sqlite").display()
    ))
    .await
    .unwrap();
    crate::run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    crate::ProjectRepo::create(
        &db,
        crate::CreateProject {
            id: "p".into(),
            owner_id: None,
            name: "Conditions".into(),
            primary_repo_id: None,
            updated_at: crate::now_rfc3339(),
            settings: "{}".into(),
            workflow_definition: "{}".into(),
            created_at: crate::now_rfc3339(),
        },
    )
    .await
    .unwrap();
    db
}

/// The check reads off the writer: a healthy page completes while another
/// connection holds the write lock. Only a repair waits for it, and a tick
/// cancelled there does not disable later ticks.
#[tokio::test]
async fn check_reads_without_the_writer_and_survives_a_cancelled_repair() {
    let directory = tempfile::tempdir().unwrap();
    let db = file_db(&directory).await;
    task(&db, "healthy").await;
    let lock = crate::begin_immediate(db.pool()).await.unwrap();
    let status = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        db.check_task_conditions(CONDITION_CHECK_PAGE),
    )
    .await
    .expect("a healthy page needs no write lock")
    .unwrap();
    assert_eq!((status.checked, status.repaired, status.ticks), (1, 0, 1));
    lock.rollback().await.unwrap();

    sqlx::query("UPDATE task SET condition_json='{\"kind\":\"corrupt\"}'")
        .execute(db.pool())
        .await
        .unwrap();
    let mut lock = crate::begin_immediate(db.pool()).await.unwrap();
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(200),
            db.check_task_conditions(CONDITION_CHECK_PAGE)
        )
        .await
        .is_err(),
        "the repair waits for the writer"
    );
    // Dropping the tick does not recall its repair statement: SQLite keeps
    // waiting for the write lock and would race the next tick for the row.
    // The writer moves the Task version, so that statement's fence fails and
    // the repair below can only come from the tick after the cancelled one.
    sqlx::query("UPDATE task SET version=version+1")
        .execute(&mut *lock)
        .await
        .unwrap();
    lock.commit().await.unwrap();
    let status = db
        .check_task_conditions(CONDITION_CHECK_PAGE)
        .await
        .unwrap();
    assert_eq!(status.repaired, 1, "the cancelled tick released its guard");
    assert!(db.task_condition_violations().await.unwrap().is_empty());
}

/// A row written between the check's read and its repair is left alone.
#[tokio::test]
async fn repair_is_fenced_on_the_row_it_read() {
    let db = db().await;
    let t = task(&db, "raced").await;
    let stored: Vec<u8> = sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
        .bind(&t.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    let repair = "{\"kind\":\"repair\"}";
    // The Task version moved, or its condition was restated, since the read.
    assert_eq!(
        db.repair_condition(&t.id, t.version + 1, &stored, repair)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        db.repair_condition(&t.id, t.version, b"{\"kind\":\"older\"}", repair)
            .await
            .unwrap(),
        0
    );
    assert!(db.task_condition_violations().await.unwrap().is_empty());
}

/// Operator status reports a completed pass, and only one that repaired.
#[tokio::test]
async fn completed_pass_is_recorded_with_its_own_counts() {
    let db = db().await;
    for i in 0..3 {
        task(&db, &format!("pass{i}")).await;
    }
    assert!(db.condition_check_status().last_pass.is_none());
    db.check_task_conditions(2).await.unwrap();
    assert!(
        db.condition_check_status().last_pass.is_none(),
        "a page is not a pass"
    );
    db.check_task_conditions(2).await.unwrap();
    let pass = db.condition_check_status().last_pass.unwrap();
    assert_eq!((pass.checked, pass.repaired), (3, 0));
    sqlx::query("UPDATE task SET condition_json='{\"kind\":\"corrupt\"}' WHERE id='pass1'")
        .execute(db.pool())
        .await
        .unwrap();
    db.check_task_conditions(CONDITION_CHECK_PAGE)
        .await
        .unwrap();
    let pass = db.condition_check_status().last_pass.unwrap();
    assert_eq!((pass.checked, pass.repaired), (3, 1));
    db.check_task_conditions(CONDITION_CHECK_PAGE)
        .await
        .unwrap();
    let pass = db.condition_check_status().last_pass.unwrap();
    assert_eq!((pass.checked, pass.repaired), (3, 0), "the next clean pass");
}

/// A database whose recorded mapping revision is stale (here: rows a stage-1
/// binary wrote, bare of witnesses) is recomputed once by the supervised
/// tick, off the startup path, and the revision is then recorded.
#[tokio::test]
async fn stale_mapping_revision_reruns_the_backfill_once() {
    let db = db().await;
    for i in 0..(CONDITION_CHECK_PAGE + 20) {
        let t = task(&db, &format!("bf{i:04}")).await;
        if i % 5 == 0 {
            sqlx::query("UPDATE task SET status='done', error_annotation=? WHERE id=?")
                .bind(json!({"type":"ci_failed"}).to_string())
                .bind(&t.id)
                .execute(db.pool())
                .await
                .unwrap();
        }
    }
    // What a stage-1 binary stored: the bare mapping, no facts.
    let rows: Vec<(String,)> = sqlx::query_as("SELECT id FROM task")
        .fetch_all(db.pool())
        .await
        .unwrap();
    for (id,) in &rows {
        let t = TaskRepo::get_by_id(&db, id, true).await.unwrap().unwrap();
        sqlx::query("UPDATE task SET condition_json=? WHERE id=?")
            .bind(encode(&map_legacy_condition(&LegacyConditionInput::from(
                &t,
            ))))
            .bind(id)
            .execute(db.pool())
            .await
            .unwrap();
    }
    assert_eq!(
        db.task_condition_violations().await.unwrap().len(),
        rows.len(),
        "every stage-1 row is stale under this revision"
    );
    // The migration that created the column recorded this revision. A
    // database a stage-1 binary migrated has no record of it.
    assert_eq!(
        crate::SystemSettingRepo::get_setting(&db, MAPPING_REVISION_KEY)
            .await
            .unwrap(),
        Some(MAPPING_REVISION.to_string())
    );
    crate::SystemSettingRepo::delete_setting(&db, MAPPING_REVISION_KEY)
        .await
        .unwrap();
    for _ in 0..50 {
        db.backfill_task_conditions_if_stale().await.unwrap();
        if db.task_condition_violations().await.unwrap().is_empty() {
            break;
        }
    }
    assert!(db.task_condition_violations().await.unwrap().is_empty());
    assert_eq!(
        crate::SystemSettingRepo::get_setting(&db, MAPPING_REVISION_KEY)
            .await
            .unwrap(),
        Some(MAPPING_REVISION.to_string())
    );
    let status = db.condition_check_status();
    assert!(status.repaired >= rows.len() as u64);
    assert!(
        status.last_pass.is_none(),
        "rewriting after a mapping change is not an operator issue"
    );
    // Once the revision is recorded the backfill does nothing further: the
    // steady check belongs to the scheduler sweep.
    let ticks = status.ticks;
    assert!(!db.backfill_task_conditions_if_stale().await.unwrap());
    assert!(!db.backfill_task_conditions_if_stale().await.unwrap());
    assert_eq!(db.condition_check_status().ticks, ticks);
    // The stage-1 migration backfill itself still agrees with the producers.
    let mut connection = db.pool().acquire().await.unwrap();
    backfill(&mut connection).await.unwrap();
    drop(connection);
    assert!(db.task_condition_violations().await.unwrap().is_empty());
}

#[tokio::test]
async fn opaque_metadata_values_preserve_shadow_until_the_bounded_check() {
    let db = db().await;
    let t = task(&db, "opaque-value").await;
    let corrupt = r#"{"kind":"future_tag"}"#;
    sqlx::query("UPDATE task SET condition_json=? WHERE id=?")
        .bind(corrupt)
        .bind(&t.id)
        .execute(db.pool())
        .await
        .unwrap();
    let step = claim(&db, &t.id).await;
    crate::task_writer::in_task_step(step, async {
        TaskRepo::mutate_metadata(
            &db,
            &t.id,
            None,
            vec![crate::TaskMetadataMutation::Set {
                key: "custom".into(),
                value: json!({"Budget":"awaiting_human queued_recovery"}),
            }],
            &crate::now_rfc3339(),
        )
        .await
        .unwrap();
    })
    .await;
    let stored: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
        .bind(&t.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(
        stored, corrupt,
        "an opaque value is not a condition producer"
    );
    db.check_task_conditions(1).await.unwrap();
    assert!(db.task_condition_violations().await.unwrap().is_empty());
}

/// The scheduler admits a Task from an initial state without reading its
/// entry barrier, so there the barrier is evidence and a diagnosis; in every
/// other state it parks, as the dispatcher holds on it.
#[tokio::test]
async fn entry_barrier_parks_everywhere_but_an_initial_state() {
    let db = db().await;
    let t = task(&db, "barrier-by-state").await;
    let barrier = json!({"status":"blocked","state":"review"}).to_string();
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    sqlx::query("UPDATE task SET entry_barrier_json=? WHERE id=?")
        .bind(&barrier)
        .bind(&t.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    db.sync_condition_in_tx(&mut tx, &t.id).await.unwrap();
    tx.commit().await.unwrap();
    let initial = db.task_condition(&t.id).await.unwrap();
    assert!(!initial.is_blocked(), "{initial:?}");
    assert_eq!(
        initial.evidence().entry_barrier_json.as_deref(),
        Some(barrier.as_str())
    );
    assert!(initial
        .evidence()
        .observations
        .iter()
        .any(|reason| matches!(
            reason,
            ParkReason::UnknownCondition { source, .. }
                if source.field == LegacyConditionField::EntryBarrierJson
        )));
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    sqlx::query("UPDATE task SET status='in_progress',status_epoch=status_epoch+1 WHERE id=?")
        .bind(&t.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    db.sync_condition_in_tx(&mut tx, &t.id).await.unwrap();
    tx.commit().await.unwrap();
    let active = db.task_condition(&t.id).await.unwrap();
    assert!(
        matches!(
            &active,
            TaskCondition::Parked {
                primary: ParkReason::EntryBlocked { .. },
                ..
            }
        ),
        "{active:?}"
    );
    assert!(db.task_condition_violations().await.unwrap().is_empty());
}
