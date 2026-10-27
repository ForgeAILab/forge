//! Tests for writers in task conditions.

use super::*;
/// B1: every writer family dual-writes at its own Rust seam, in its own
/// transaction. No trigger backs these up, so each family is driven through
/// its real function and the whole table is swept afterwards.
#[tokio::test]
async fn each_writer_family_keeps_condition_invariant_and_legacy_observability() {
    let db = db().await;
    for family in [
        "create",
        "update",
        "status",
        "recovery",
        "annotation",
        "barrier",
        "barrier_authority",
        "budget_entry",
        "metadata",
        "metadata_bump",
        "wake",
        "wake_project",
        "task_query",
        "task_query_tx",
        "task_query_sql_computed",
        "bulk_query",
        "direct_sql_seam",
        "removed_machine_cancel_clear",
        "removed_machine_owner_wait_clear",
        "removed_machine_annotation",
        // Writers of a mapped fact that is not a Task column.
        "preempt_supersede",
        "subtask_reorder",
        "workspace_delete",
        "workflow_edit",
    ] {
        let t = task(&db, family).await;
        db.check_task_condition_invariant(&t).await.unwrap();
        if family == "create" {
            continue;
        }
        let step = claim(&db, family).await;
        let step_id = step.id.clone();
        crate::task_writer::in_task_step(step,async {
            match family {
                "update"=>{let mut u=update(&t);u.error_annotation=Some(Some(json!({"type":"workspace_error"}).to_string()));u.blocked_json=Some(Some(json!({"kind":"workspace_error"}).to_string()));TaskRepo::update(&db,u).await.unwrap();},
                "status"=>{TaskRepo::update_status(&db,UpdateTaskStatus{id:t.id.clone(),expected_version:t.version,status:"review".into(),assignee_id:None,error_annotation:Some(Some(json!({"type":"review_needs_owner"}).to_string())),blocked_json:None,failed_json:None,updated_at:crate::now_rfc3339()}).await.unwrap();},
                "recovery"=>{TaskRepo::update_recovery_metadata_if_no_running_execution(&db,&t.id,t.version,Some(json!({"type":"recovery_required"}).to_string()),Some(json!({"kind":"workspace_error"}).to_string()),None,&crate::now_rfc3339(),None,vec![],vec![], None).await.unwrap();},
                "annotation"=>{TaskRepo::set_error_annotation_if_no_running_execution(&db,&t.id,t.version,&t.status,None,"{}",None,None,&json!({"type":"before_work_hook_failed"}).to_string(),&crate::now_rfc3339(),"stopped",None,vec![]).await.unwrap();},
                "barrier"=>{TaskRepo::set_entry_barrier(&db,&t.id,t.version,Some(json!({"status":"blocked","state":"review"}).to_string()),&crate::now_rfc3339()).await.unwrap();},
                "barrier_authority"=>{let p=ProjectRepo::get_by_id(&db,"p").await.unwrap().unwrap();TaskRepo::set_entry_barrier_with_workflow_authority(&db,&t.id,t.version,Some(json!({"status":"blocked","state":"review"}).to_string()),&crate::now_rfc3339(),p.version,p.workflow_definition).await.unwrap();},
                "budget_entry"=>{let (_,allowed)=crate::budget::failed_review_entry(&db,&t,0,&step_id,&crate::now_rfc3339()).await.unwrap();assert!(!allowed);},
                "metadata"=>{TaskRepo::mutate_metadata(&db,&t.id,Some(t.version),vec![TaskMetadataMutation::Set{key:"owner_wait".into(),value:json!({"daemon_id":"d"})}],&crate::now_rfc3339()).await.unwrap();},
                "metadata_bump"=>{TaskRepo::mutate_metadata_and_bump_version(&db,&t.id,t.version,vec![TaskMetadataMutation::Set{key:"awaiting_human".into(),value:json!(true)}],&crate::now_rfc3339()).await.unwrap();},
                "wake"=>{TaskRepo::mutate_metadata(&db,&t.id,Some(t.version),vec![TaskMetadataMutation::Set{key:"dispatch_disposition".into(),value:json!({"capability":"coder"})}],&crate::now_rfc3339()).await.unwrap();assert!(db.task_condition(&t.id).await.unwrap().is_blocked());TaskRepo::wake_dispatch_for_task(&db,&t.id,&crate::now_rfc3339()).await.unwrap();},
                "wake_project"=>{TaskRepo::mutate_metadata(&db,&t.id,Some(t.version),vec![TaskMetadataMutation::Set{key:"dispatch_disposition".into(),value:json!({"capability":"coder"})}],&crate::now_rfc3339()).await.unwrap();TaskRepo::wake_dispatch_for_project(&db,"p",&crate::now_rfc3339()).await.unwrap();},
                "task_query"=>{crate::task_writer::TaskQuery::new(&db,&t.id,"UPDATE task SET blocked_json=? WHERE id=?", crate::ConditionChange::Legacy).bind(json!({"kind":"executor_failed"}).to_string()).bind(&t.id).execute(db.pool()).await.unwrap();},
                "task_query_tx"=>{let mut tx=crate::begin_immediate(db.pool()).await.unwrap();crate::task_writer::TaskQuery::new(&db,&t.id,"UPDATE task SET error_annotation=? WHERE id=?", crate::ConditionChange::Legacy).bind(json!({"type":"manual_stop"}).to_string()).bind(&t.id).execute_in_tx(&mut tx).await.unwrap().require_applied().unwrap();tx.commit().await.unwrap();},
                // Placement/environment/reconnect shape: the value exists only in SQL.
                "task_query_sql_computed"=>{crate::task_writer::TaskQuery::new(&db,&t.id,"UPDATE task SET metadata_json=json_set(COALESCE(metadata_json,'{}'),'$.environment_wait',json(?),'$.deferred_dispatch.reason',?) WHERE id=?", crate::ConditionChange::Legacy).bind(json!({"kind":"environment_not_ready"}).to_string()).bind("environment_not_ready: server").bind(&t.id).execute(db.pool()).await.unwrap();},
                "bulk_query"=>{let mut tx=crate::begin_immediate(db.pool()).await.unwrap();crate::task_writer::BulkTaskQuery::new(&db,"UPDATE task SET error_annotation=? WHERE id=?", crate::ConditionChange::Legacy).bind(json!({"type":"dispatch_failed"}).to_string()).bind(&t.id).execute_in_tx(&mut tx).await.unwrap();tx.commit().await.unwrap();},
                // Queue settlement, lifecycle and transition writers in services
                // run raw SQL and then call the public seam in the same transaction.
                "direct_sql_seam"=>{let before:(i64,String)=sqlx::query_as("SELECT version,updated_at FROM task WHERE id=?").bind(&t.id).fetch_one(db.pool()).await.unwrap();
                    let mut tx=crate::begin_immediate(db.pool()).await.unwrap();
                    sqlx::query("UPDATE task SET error_annotation='{\"type\":\"workflow_loop\"}',blocked_json='{\"source\":\"workflow_loop\"}' WHERE id=?").bind(&t.id).execute(&mut *tx).await.unwrap();
                    db.sync_condition_in_tx(&mut tx,&t.id).await.unwrap();tx.commit().await.unwrap();
                    let after:(i64,String)=sqlx::query_as("SELECT version,updated_at FROM task WHERE id=?").bind(&t.id).fetch_one(db.pool()).await.unwrap();assert_eq!(before,after);
                },
                "removed_machine_cancel_clear"=>{crate::task_writer::TaskQuery::new(&db,&t.id,"UPDATE task SET error_annotation=NULL,metadata_json=CASE WHEN json_valid(metadata_json) THEN json_remove(metadata_json,'$.deferred_dispatch','$.dispatch_disposition') ELSE metadata_json END,version=version+1 WHERE id=?", crate::ConditionChange::Legacy).bind(&t.id).identity_fenced().execute(db.pool()).await.unwrap();},
                "removed_machine_owner_wait_clear"=>{TaskRepo::mutate_metadata(&db,&t.id,None,vec![TaskMetadataMutation::Set{key:"owner_wait".into(),value:json!({"daemon_id":"removed"})}],&crate::now_rfc3339()).await.unwrap();crate::task_writer::TaskQuery::new(&db,&t.id,"UPDATE task SET metadata_json=json_remove(metadata_json,'$.owner_wait','$.deferred_dispatch','$.dispatch_disposition') WHERE id=? AND json_valid(metadata_json) AND json_extract(metadata_json,'$.owner_wait.daemon_id')=?", crate::ConditionChange::Legacy).bind(&t.id).bind("removed").identity_fenced().execute(db.pool()).await.unwrap();},
                "removed_machine_annotation"=>{crate::task_writer::TaskQuery::new(&db,&t.id,"UPDATE task SET error_annotation=?,version=version+1 WHERE id=?", crate::ConditionChange::Legacy).bind(json!({"type":"recovery_required","blocking_reason":"machine_removed","blocked_by":"removed"}).to_string()).bind(&t.id).identity_fenced().execute(db.pool()).await.unwrap();},
                // An owner command preempts and supersedes the pending entry hooks step.
                "preempt_supersede"=>{
                    let hook=enqueue(&db,&t.id,"hooks","todo","{}").await;
                    enqueue(&db,&t.id,"command","todo",&json!({"preempt":true}).to_string()).await;
                    let status:String=sqlx::query_scalar("SELECT status FROM task_step WHERE id=?").bind(&hook).fetch_one(db.pool()).await.unwrap();assert_eq!(status,"superseded");
                },
                // Child witnesses follow the sequence order.
                "subtask_reorder"=>{
                    let (a,b)=(task(&db,"reorder-a").await,task(&db,"reorder-b").await);
                    set_parent(&db,&t.id,&[&a.id,&b.id]).await;
                    TaskRepo::mutate_metadata(&db,&t.id,None,vec![TaskMetadataMutation::Set{key:"coordination_review_pending".into(),value:json!(true)}],&crate::now_rfc3339()).await.unwrap();
                    TaskRepo::reorder_subtasks(&db,&t.id,&[b.id.clone(),a.id.clone()],&crate::now_rfc3339()).await.unwrap();
                    assert!(matches!(db.task_condition(&t.id).await.unwrap(),TaskCondition::Parked{primary:ParkReason::Children{remaining,..},..} if remaining==vec![b.id.clone(),a.id.clone()]));
                },
                // A cancellation fences its Task through the workspace row.
                "workspace_delete"=>{
                    let _directory=workspace(&db,"family-workspace",&t.id).await;
                    pending_cancel(&db,"family-op",&step_id,"family-workspace").await;
                    assert!(db.task_condition(&t.id).await.unwrap().is_blocked());
                    sqlx::query("UPDATE task_step SET status='superseded',completed_at=? WHERE id=?").bind(crate::now_rfc3339()).bind(&step_id).execute(db.pool()).await.unwrap();
                    crate::WorkspaceRepo::delete(&db,"family-workspace").await.unwrap();
                },
                // Terminal classification reads the Project workflow.
                "workflow_edit"=>{
                    TaskRepo::update_status(&db,UpdateTaskStatus{id:t.id.clone(),expected_version:t.version,status:"released".into(),assignee_id:None,error_annotation:None,blocked_json:None,failed_json:None,updated_at:crate::now_rfc3339()}).await.unwrap();
                    assert!(matches!(db.task_condition(&t.id).await.unwrap(),TaskCondition::Clear{..}));
                    let p=ProjectRepo::get_by_id(&db,"p").await.unwrap().unwrap();
                    ProjectRepo::update_workflow(&db,"p",&json!({"states":[{"name":"todo","kind":"initial"},{"name":"review","kind":"gate"},{"name":"released","kind":"terminal"}]}).to_string(),None,p.version,&crate::now_rfc3339()).await.unwrap();
                    assert!(matches!(db.task_condition(&t.id).await.unwrap(),TaskCondition::Settled{..}));
                },
                _=>unreachable!(),
            }
            let current=TaskRepo::get_by_id(&db,&t.id,true).await.unwrap().unwrap();
            db.check_task_condition_invariant(&current).await.unwrap_or_else(|e|panic!("{family}: {e}"));
            let condition=db.task_condition(&t.id).await.unwrap();
            assert_eq!(condition.is_blocked(),!matches!(family,"barrier"|"barrier_authority"|"budget_entry"|"wake"|"wake_project"|"removed_machine_cancel_clear"|"removed_machine_owner_wait_clear"|"preempt_supersede"|"workspace_delete"|"workflow_edit"),"{family}: {condition:?}");
            // A shadow-only sync must leave legacy versions/revisions/events alone.
            let before:(i64,i64,i64,i64)=sqlx::query_as("SELECT t.version,p.board_revision,p.list_revision,(SELECT COUNT(*) FROM domain_event) FROM task t JOIN project p ON p.id=t.project_id WHERE t.id=?").bind(&t.id).fetch_one(db.pool()).await.unwrap();
            let mut tx=crate::begin_immediate(db.pool()).await.unwrap();db.sync_condition_in_tx(&mut tx,&t.id).await.unwrap();tx.commit().await.unwrap();
            let after:(i64,i64,i64,i64)=sqlx::query_as("SELECT t.version,p.board_revision,p.list_revision,(SELECT COUNT(*) FROM domain_event) FROM task t JOIN project p ON p.id=t.project_id WHERE t.id=?").bind(&t.id).fetch_one(db.pool()).await.unwrap();assert_eq!(before,after,"{family}");
        }).await;
    }
    assert_eq!(
        db.task_condition_violations().await.unwrap(),
        Vec::<String>::new()
    );
    // The sweep itself must see a writer that skipped its seam.
    sqlx::query("UPDATE task SET blocked_json='{\"kind\":\"ci_failed\"}' WHERE id='create'")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        db.task_condition_violations().await.unwrap(),
        vec!["create".to_owned()]
    );
}

#[tokio::test]
async fn stale_condition_version_source_and_lease_are_rejected() {
    let db = db().await;
    let t = task(&db, "fence").await;
    let step = claim(&db, &t.id).await;
    crate::task_writer::in_task_step(step.clone(), async {
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        let input = ConditionInput::from(&t);
        let condition = compose_condition(&input);
        assert!(matches!(
            db.set_condition(&mut tx, &t.id, t.version - 1, &input, &condition)
                .await,
            Err(DbError::VersionConflict)
        ));
        // A version-neutral metadata change must invalidate the observed source too.
        sqlx::query(
            "UPDATE task SET metadata_json='{\"owner_wait\":{\"daemon_id\":\"d\"}}' WHERE id=?",
        )
        .bind(&t.id)
        .execute(&mut *tx)
        .await
        .unwrap();
        assert!(matches!(
            db.set_condition(&mut tx, &t.id, t.version, &input, &condition)
                .await,
            Err(DbError::VersionConflict)
        ));
        // m1: the seam is real. With the current source it writes the shadow,
        // and it refuses a condition the legacy fields do not map to.
        let current = ConditionInput {
            metadata_json: Some("{\"owner_wait\":{\"daemon_id\":\"d\"}}".into()),
            ..input.clone()
        };
        assert!(matches!(
            db.set_condition(&mut tx, &t.id, t.version, &current, &condition)
                .await,
            Err(DbError::Check(_))
        ));
        let facts = ConditionFacts::load(&mut tx, &t.id).await.unwrap();
        let parked = facts.apply(compose_condition(&current));
        db.set_condition(&mut tx, &t.id, t.version, &current, &parked)
            .await
            .unwrap();
        let stored: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
            .bind(&t.id)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(decode(&stored).unwrap(), parked);
        assert!(parked.is_blocked());
        tx.rollback().await.unwrap();
        sqlx::query("UPDATE task_step SET claimed_by='successor' WHERE id=?")
            .bind(&step.id)
            .execute(db.pool())
            .await
            .unwrap();
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        assert!(matches!(
            db.set_condition(&mut tx, &t.id, t.version, &input, &condition)
                .await,
            Err(DbError::VersionConflict)
        ));
        tx.rollback().await.unwrap();
    })
    .await;
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    let input = ConditionInput::from(&t);
    let condition = compose_condition(&input);
    assert!(matches!(
        db.set_condition(&mut tx, &t.id, t.version, &input, &condition)
            .await,
        Err(DbError::Check(_))
    ));
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn every_condition_metadata_key_and_owner_record_keeps_shadow_boundary() {
    let db = db().await;
    let t = task(&db, "metadata-boundary").await;
    let step = claim(&db, &t.id).await;
    let entries = [
        ("awaiting_human", json!(true)),
        ("awaiting_human_reason", json!("plan_review")),
        ("awaiting_human_marker_id", json!("marker")),
        ("planning_completed_at", json!("original-time")),
        ("planning_execution_id", json!("planner")),
        ("planning_state_entry_token", json!("entry")),
        (
            "deferred_dispatch",
            json!({"not_before":"2026-10-06T00:01:00Z","reason":"retry","target_state":"review"}),
        ),
        (
            "dispatch_disposition",
            json!({"capability":"project_capacity"}),
        ),
        (
            "paused_integration",
            json!({"state":"merging","deferred_at":"original-time"}),
        ),
        ("paused_integration_generation", json!(2)),
        (
            "owner_wait",
            json!({"daemon_id":"d","started_at":"original-time"}),
        ),
        (
            "environment_wait",
            json!({"kind":"provision_failed","machine":{"daemon_id":"d"}}),
        ),
        (
            "placement_refusal",
            json!({"eligibility_key":"key","annotation":{"type":"dispatch_failed"}}),
        ),
        ("daemon_upgrade_refusal", json!({"daemon_ids":["d"]})),
        ("plan_settlement_wait", json!({"execution_id":"planner"})),
        ("coordination_review_pending", json!(true)),
        ("coordination_review_pending_id", json!("sequence")),
        ("last_execution_failure_at", json!("original-time")),
        (
            "last_execution_failure_execution_id",
            json!("failed-execution"),
        ),
        ("executor_unavailable_execution_id", json!("unavailable")),
        ("last_workflow_guard_rejection_at", json!("original-time")),
        ("last_workflow_guard_name", json!("require_plan")),
        ("last_workflow_guard_reason", json!("plan missing")),
        ("last_workflow_guard_execution_id", json!("planner")),
    ];
    crate::task_writer::in_task_step(step, async {
        for (key, value) in entries {
            let current = TaskRepo::get_by_id(&db, &t.id, false).await.unwrap().unwrap();
            let updated = TaskRepo::mutate_metadata(&db, &t.id, Some(current.version), vec![TaskMetadataMutation::Set {key:key.into(),value:value.clone()}], &crate::now_rfc3339()).await.unwrap();
            db.check_task_condition_invariant(&updated).await.unwrap();
            let evidence = serde_json::to_value(db.task_condition(&t.id).await.unwrap()).unwrap();
            assert_eq!(evidence["evidence"]["metadata"][key],json!(value.to_string()),"{key}");
            let cleared = TaskRepo::mutate_metadata(&db, &t.id, Some(updated.version), vec![TaskMetadataMutation::Remove {key:key.into()}], &crate::now_rfc3339()).await.unwrap();
            db.check_task_condition_invariant(&cleared).await.unwrap();
        }
        for key in ["queued_recovery","plan_publication_claim","plan_publication_cleanup","terminal_execution_settlement","unknown_user_key"] {
            let current=TaskRepo::get_by_id(&db,&t.id,false).await.unwrap().unwrap();
            let value=json!({"id":"owner-record","state":"todo","request":{"guidance":"retained-only-in-owner"}});
            let updated=TaskRepo::mutate_metadata(&db,&t.id,Some(current.version),vec![TaskMetadataMutation::Set {key:key.into(),value:value.clone()}],&crate::now_rfc3339()).await.unwrap();
            db.check_task_condition_invariant(&updated).await.unwrap();
            assert_eq!(updated.metadata().unwrap().extra[key],value);
            let evidence=serde_json::to_value(db.task_condition(&t.id).await.unwrap()).unwrap();
            assert!(evidence["evidence"]["metadata"].get(key).is_none(),"owner body {key}");
        }
    }).await;
}

#[tokio::test]
async fn queued_task_and_bulk_effects_write_condition_only_when_their_lease_applies() {
    let db = db().await;
    let root = task(&db, "bulk-root").await;
    let child = task(&db, "bulk-child").await;
    let root_step = claim(&db, &root.id).await;
    crate::task_writer::in_task_step(root_step, async {
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        let queued = crate::task_writer::TaskQuery::new(
            &db,
            &child.id,
            "UPDATE task SET error_annotation=? WHERE id=?",
            crate::ConditionChange::Legacy,
        )
        .bind(json!({"type":"manual_stop"}).to_string())
        .bind(&child.id)
        .identity_fenced()
        .execute_in_tx(&mut tx)
        .await
        .unwrap();
        assert!(matches!(
            queued,
            crate::task_writer::TaskQueryResult::Queued { .. }
        ));
        let counts = crate::task_writer::BulkTaskQuery::new(
            &db,
            "UPDATE task SET metadata_json=? WHERE id IN (?,?)",
            crate::ConditionChange::Legacy,
        )
        .bind(json!({"owner_wait":{"daemon_id":"d"}}).to_string())
        .bind(&root.id)
        .bind(&child.id)
        .execute_in_tx(&mut tx)
        .await
        .unwrap();
        assert_eq!((counts.applied, counts.queued), (1, 1));
        tx.commit().await.unwrap();
        assert!(matches!(
            db.task_condition(&child.id).await.unwrap(),
            TaskCondition::Clear { .. }
        ));
        db.check_task_condition_invariant(
            &TaskRepo::get_by_id(&db, &root.id, false)
                .await
                .unwrap()
                .unwrap(),
        )
        .await
        .unwrap();
        // Identity-fenced effects survive a different entry and map its latest legacy fields.
        sqlx::query("UPDATE task SET status='backlog',version=version+1 WHERE id=?")
            .bind(&child.id)
            .execute(db.pool())
            .await
            .unwrap();
        for _ in 0..2 {
            let step = db
                .claim_step(
                    "child-writer",
                    Some(&child.id),
                    &crate::task_writer::lease_deadline(),
                )
                .await
                .unwrap()
                .unwrap();
            let owned = step.clone();
            crate::task_writer::in_task_step(step, db.execute_task_mutation(&owned))
                .await
                .unwrap();
            db.release_step(&owned.id, owned.claimed_by.as_deref().unwrap())
                .await
                .unwrap();
            let current = TaskRepo::get_by_id(&db, &child.id, false)
                .await
                .unwrap()
                .unwrap();
            db.check_task_condition_invariant(&current).await.unwrap();
        }
        let condition = db.task_condition(&child.id).await.unwrap();
        assert!(reasons(&condition)
            .iter()
            .any(|r| matches!(r, ParkReason::Held { .. })));
        assert!(reasons(&condition)
            .iter()
            .any(|r| matches!(r, ParkReason::OwnerOffline { .. })));
    })
    .await;
}

#[tokio::test]
async fn sqlite_legacy_number_unicode_and_deep_evidence_remain_lossless() {
    let db = db().await;
    let annotation = r#"{"type":"manual_stop","blocked_by":"\ud800","message":"retained"}"#;
    let metadata = r#"{"paused_integration_generation":1e999,"last_workflow_guard_reason":"\ud800","owner_wait":{"daemon_id":"Nڠ","started_at":"now"}}"#;
    let legacy = ConditionInput {
        error_annotation: Some(annotation.into()),
        metadata_json: Some(metadata.into()),
        ..Default::default()
    };
    let condition = compose_condition(&legacy);
    let evidence = serde_json::to_value(&condition).unwrap();
    assert_eq!(evidence["evidence"]["error_annotation"], annotation);
    assert_eq!(
        evidence["evidence"]["metadata"]["paused_integration_generation"],
        "1e999"
    );
    assert_eq!(
        evidence["evidence"]["metadata"]["last_workflow_guard_reason"],
        r#""\ud800""#
    );
    assert!(
        reasons(&condition)
            .iter()
            .any(|r| matches!(r,ParkReason::OwnerOffline{daemon_id:Some(id),..} if id=="Nڠ")),
        "valid UTF-8 must not be mistaken for an unpaired surrogate"
    );
    let deep = format!(
        "{{\"owner_wait\":{{\"snapshot\":{}{}}}}}",
        "{\"nested\":".repeat(200),
        format_args!("0{}", "}".repeat(200))
    );
    let legacy = ConditionInput {
        metadata_json: Some(deep.clone()),
        ..Default::default()
    };
    let condition = compose_condition(&legacy);
    let value = serde_json::to_value(condition).unwrap();
    let fragment = value["evidence"]["metadata"]["owner_wait"]
        .as_str()
        .unwrap();
    assert!(fragment.contains(&"{\"nested\":".repeat(200)));
    // A raw INSERT is a direct SQL writer: it syncs through the seam.
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,error_annotation,metadata_json,created_at,updated_at) VALUES('unicode','p','unicode',?,?,?,?)")
        .bind(annotation).bind(metadata).bind(crate::now_rfc3339()).bind(crate::now_rfc3339()).execute(&mut *tx).await.unwrap();
    db.sync_condition_in_tx(&mut tx, "unicode").await.unwrap();
    tx.commit().await.unwrap();
    let task = TaskRepo::get_by_id(&db, "unicode", false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.error_annotation.as_deref(), Some(annotation));
    assert_eq!(task.metadata_json.as_deref(), Some(metadata));
    db.check_task_condition_invariant(&task).await.unwrap();
}
