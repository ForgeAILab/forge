use super::*;
use crate::{
    CreateProject, CreateTask, EnqueueTaskStep, ProjectRepo, TaskMetadataMutation, TaskRepo,
    TaskStepRepo, UpdateTask, UpdateTaskStatus,
};
use serde_json::{json, Value};

async fn db() -> SqliteDb {
    let pool = crate::create_sqlite_pool("sqlite::memory:").await.unwrap();
    crate::run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    ProjectRepo::create(
        &db,
        CreateProject {
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
async fn task(db: &SqliteDb, id: &str) -> Task {
    TaskRepo::create(
        db,
        CreateTask {
            id: id.into(),
            project_id: "p".into(),
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: id.into(),
            description: None,
            task_type: "task".into(),
            status: "todo".into(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            subtask_order: None,
            plan: None,
            updated_at: crate::now_rfc3339(),
            created_at: crate::now_rfc3339(),
        },
    )
    .await
    .unwrap()
}
async fn claim(db: &SqliteDb, id: &str) -> crate::TaskStep {
    let t = TaskRepo::get_by_id(db, id, false).await.unwrap().unwrap();
    let step_id = crate::new_uuid_v4();
    db.enqueue_step(&EnqueueTaskStep {
        id: step_id.clone(),
        task_id: id.into(),
        kind: "command".into(),
        payload_json: "{}".into(),
        causation_step_id: None,
        causation_key: step_id.clone(),
        chain_id: step_id,
        chain_position: 1,
        expected_status: t.status,
        expected_version: t.version,
        expected_epoch: None,
        lane: "fast".into(),
        available_at: crate::now_rfc3339(),
    })
    .await
    .unwrap();
    db.claim_step(
        "condition-tests",
        Some(id),
        &crate::task_writer::lease_deadline(),
    )
    .await
    .unwrap()
    .unwrap()
}
fn input(
    annotation: Option<Value>,
    blocked: Option<Value>,
    failed: Option<Value>,
    barrier: Option<Value>,
    metadata: Option<Value>,
) -> LegacyConditionInput {
    LegacyConditionInput {
        error_annotation: annotation.map(|v| v.to_string()),
        blocked_json: blocked.map(|v| v.to_string()),
        failed_json: failed.map(|v| v.to_string()),
        entry_barrier_json: barrier.map(|v| v.to_string()),
        metadata_json: metadata.map(|v| v.to_string()),
    }
}
fn reasons(condition: &TaskCondition) -> Vec<&ParkReason> {
    match condition {
        TaskCondition::Parked {
            primary,
            additional,
            ..
        } => std::iter::once(primary).chain(additional).collect(),
        TaskCondition::Failed {
            failure,
            additional,
            ..
        } => std::iter::once(failure).chain(additional).collect(),
        _ => vec![],
    }
}

#[tokio::test]
async fn mapping_table_preserves_known_unknown_and_combined_conditions() {
    let db = db().await;
    let mut conn = db.pool().acquire().await.unwrap();
    let cases = [
        ("clear", LegacyConditionInput::default(), "clear", None),
        (
            "held",
            input(
                Some(json!({"type":"manual_stop","blocked_by":"user:owner"})),
                None,
                None,
                None,
                None,
            ),
            "parked",
            Some("held"),
        ),
        (
            "failed",
            input(
                None,
                None,
                Some(json!({"kind":"executor_failed","details":{"custom":[1,2]}})),
                None,
                None,
            ),
            "failed",
            Some("failure"),
        ),
        (
            "blocked",
            input(
                None,
                Some(json!({"kind":"workspace_error"})),
                None,
                None,
                None,
            ),
            "parked",
            Some("failure"),
        ),
        (
            "budget",
            input(
                Some(json!({"type":"review_budget_exhausted"})),
                None,
                None,
                None,
                None,
            ),
            "parked",
            Some("budget_exhausted"),
        ),
        (
            "entry_budget",
            input(
                None,
                None,
                None,
                Some(json!({"status":"blocked","blocking_reason":"review retry budget exhausted"})),
                None,
            ),
            "parked",
            Some("budget_exhausted"),
        ),
        (
            "barrier",
            input(
                None,
                None,
                None,
                Some(json!({"state":"review","status":"blocked"})),
                None,
            ),
            "parked",
            Some("entry_blocked"),
        ),
        (
            "old_running",
            input(
                None,
                None,
                None,
                Some(json!({"state":"review","status":"running"})),
                None,
            ),
            "parked",
            Some("unknown_condition"),
        ),
        (
            "human",
            input(
                None,
                None,
                None,
                None,
                Some(json!({"awaiting_human":true,"awaiting_human_reason":"plan_review"})),
            ),
            "parked",
            Some("human_decision"),
        ),
        (
            "capacity",
            input(
                None,
                None,
                None,
                None,
                Some(json!({"dispatch_disposition":{"capability":"machine_capacity"}})),
            ),
            "parked",
            Some("capacity"),
        ),
        (
            "refusal",
            input(
                None,
                None,
                None,
                None,
                Some(
                    json!({"dispatch_disposition":{"capability":"coder","blocker_digest":"sha256:abc"}}),
                ),
            ),
            "parked",
            Some("dispatch_refusal"),
        ),
        (
            "pause",
            input(
                None,
                None,
                None,
                None,
                Some(
                    json!({"paused_integration":{"state":"merging"},"paused_integration_generation":7}),
                ),
            ),
            "parked",
            Some("project_paused"),
        ),
        (
            "owner",
            input(
                None,
                None,
                None,
                None,
                Some(json!({"owner_wait":{"daemon_id":"d","started_at":"2026-10-06T00:00:00Z"}})),
            ),
            "parked",
            Some("owner_offline"),
        ),
        (
            "environment",
            input(
                None,
                None,
                None,
                None,
                Some(
                    json!({"environment_wait":{"kind":"environment_unverified","machine":{"daemon_id":"d"}}}),
                ),
            ),
            "parked",
            Some("environment"),
        ),
        (
            "placement",
            input(
                None,
                None,
                None,
                None,
                Some(json!({"placement_refusal":{"eligibility_key":"unchanged"}})),
            ),
            "parked",
            Some("placement_denied"),
        ),
        (
            "upgrade",
            input(
                None,
                None,
                None,
                None,
                Some(json!({"daemon_upgrade_refusal":{"daemon_ids":["d"]}})),
            ),
            "parked",
            Some("daemon_upgrade_required"),
        ),
        (
            "plan_wait",
            input(
                None,
                None,
                None,
                None,
                Some(json!({"plan_settlement_wait":{"execution_id":"e"}})),
            ),
            "parked",
            Some("plan_settlement_wait"),
        ),
        (
            "timer",
            input(
                None,
                None,
                None,
                None,
                Some(
                    json!({"deferred_dispatch":{"not_before":"2026-10-06T00:01:00Z","reason":"retry","target_state":"working"}}),
                ),
            ),
            "deferred",
            None,
        ),
        (
            "malformed_deadline",
            input(
                None,
                None,
                None,
                None,
                Some(
                    json!({"deferred_dispatch":{"not_before":"not a timestamp","reason":"retry","target_state":"review"}}),
                ),
            ),
            "parked",
            Some("unknown_condition"),
        ),
        (
            "remote_cancel",
            input(
                Some(
                    json!({"type":"workspace_reset_required","blocking_reason":"pending_remote_cancel"}),
                ),
                None,
                None,
                None,
                None,
            ),
            "parked",
            Some("remote_cancel_pending"),
        ),
        (
            "ci_exhausted",
            input(
                Some(
                    json!({"type":"before_work_hook_failed","blocking_reason":"review_ci_infrastructure_exhausted"}),
                ),
                None,
                None,
                Some(json!({"status":"blocked","state":"review"})),
                None,
            ),
            "parked",
            Some("budget_exhausted"),
        ),
        (
            "unknown",
            input(
                Some(json!({"type":"future_failure","secret_preserved":{"x":true}})),
                None,
                None,
                None,
                None,
            ),
            "parked",
            Some("unknown_condition"),
        ),
        (
            "unknown_key",
            input(
                None,
                None,
                None,
                None,
                Some(json!({"custom_user_key":{"x":1}})),
            ),
            "clear",
            None,
        ),
        (
            "bad_shape",
            input(
                None,
                None,
                None,
                None,
                Some(json!({"deferred_dispatch":[1],"awaiting_human":"yes"})),
            ),
            "parked",
            Some("unknown_condition"),
        ),
        (
            "bad_nested_types",
            input(
                Some(json!({"type":"manual_stop","blocked_by":[1]})),
                None,
                None,
                None,
                Some(json!({"owner_wait":{"daemon_id":[1],"started_at":42}})),
            ),
            "parked",
            Some("held"),
        ),
    ];
    for (name, legacy, expected, primary) in cases {
        let condition = map_legacy_condition(&mut conn, &legacy)
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        let value = serde_json::to_value(&condition).unwrap();
        assert_eq!(value["kind"], expected, "{name}");
        if let Some(kind) = primary {
            assert_eq!(
                serde_json::to_value(reasons(&condition)[0]).unwrap()["kind"],
                kind,
                "{name}"
            );
        }
        assert_eq!(
            value["evidence"]["error_annotation"],
            json!(legacy.error_annotation),
            "{name}"
        );
        assert_eq!(
            value["evidence"]["blocked_json"],
            json!(legacy.blocked_json),
            "{name}"
        );
        assert_eq!(
            value["evidence"]["failed_json"],
            json!(legacy.failed_json),
            "{name}"
        );
        assert_eq!(
            value["evidence"]["entry_barrier_json"],
            json!(legacy.entry_barrier_json),
            "{name}"
        );
        let again = map_legacy_condition(&mut conn, &legacy).await.unwrap();
        assert_eq!(condition, again, "deterministic {name}");
    }
    for field in 0..5 {
        for raw in ["", "{invalid", "null", "[]", "42", "\"scalar\""] {
            let mut legacy = LegacyConditionInput::default();
            *match field {
                0 => &mut legacy.error_annotation,
                1 => &mut legacy.blocked_json,
                2 => &mut legacy.failed_json,
                3 => &mut legacy.entry_barrier_json,
                _ => &mut legacy.metadata_json,
            } = Some(raw.into());
            let condition = map_legacy_condition(&mut conn, &legacy).await.unwrap();
            assert!(
                matches!(reasons(&condition)[0], ParkReason::UnknownCondition { .. }),
                "{field}:{raw}"
            );
        }
    }
    let combined = input(
        Some(json!({"type":"before_work_hook_failed"})),
        Some(json!({"kind":"retry_exhausted"})),
        None,
        Some(json!({"state":"review","status":"blocked"})),
        Some(
            json!({"owner_wait":{"daemon_id":"d"},"paused_integration":{"state":"review"},"deferred_dispatch":{"not_before":"2026-10-06T00:01:00Z","reason":"wait","target_state":"review"}}),
        ),
    );
    let condition = map_legacy_condition(&mut conn, &combined).await.unwrap();
    assert_eq!(reasons(&condition).len(), 5);
    assert!(reasons(&condition)
        .iter()
        .any(|r| matches!(r, ParkReason::BudgetExhausted { .. })));
    assert!(reasons(&condition)
        .iter()
        .any(|r| matches!(r, ParkReason::EntryBlocked { .. })));
    assert!(reasons(&condition)
        .iter()
        .any(|r| matches!(r, ParkReason::OwnerOffline { .. })));
    // retry_exhausted is also written by generic gate budgets. Legacy inputs
    // alone cannot resolve its ledger kind; do not invent an Execution window.
    let ambiguous = input(
        None,
        Some(json!({"kind":"retry_exhausted"})),
        None,
        None,
        None,
    );
    assert!(matches!(
        reasons(&map_legacy_condition(&mut conn, &ambiguous).await.unwrap())[0],
        ParkReason::BudgetExhausted {
            budget_kind: None,
            ..
        }
    ));
    let duplicate = input(
        Some(json!({"type":"workflow_loop"})),
        Some(json!({"source":"workflow_loop","reason":"loop"})),
        None,
        None,
        None,
    );
    assert_eq!(
        reasons(&map_legacy_condition(&mut conn, &duplicate).await.unwrap()).len(),
        1
    );
    let ci = input(
        Some(
            json!({"type":"before_work_hook_failed","blocking_reason":"review_ci_infrastructure"}),
        ),
        None,
        None,
        Some(json!({"state":"review","status":"blocked"})),
        Some(
            json!({"deferred_dispatch":{"not_before":"2026-10-06T00:00:05Z","reason":"retry CI","target_state":"review"}}),
        ),
    );
    assert!(matches!(
        map_legacy_condition(&mut conn, &ci).await.unwrap(),
        TaskCondition::Deferred {
            reason: RetryCause::ReviewCiInfrastructure,
            resume: ConditionContinuation::RetryEntry { .. },
            ..
        }
    ));
}

#[test]
fn every_condition_variant_round_trips_without_fabricated_owners() {
    let evidence = ConditionEvidence::default();
    let source = ConditionSource {
        field: LegacyConditionField::EntryBarrierJson,
        key: None,
    };
    let primary = ParkReason::UnknownCondition {
        source,
        problem: UnknownConditionProblem::UnownedEntry,
    };
    let variants = vec![
        TaskCondition::Clear {
            evidence: evidence.clone(),
        },
        TaskCondition::Entering {
            state: "review".into(),
            epoch: 2,
            step_id: "step".into(),
            phase: "before_enter".into(),
            since: "now".into(),
            evidence: evidence.clone(),
        },
        TaskCondition::Running {
            execution_id: "execution".into(),
            role: "coder".into(),
            epoch: 2,
            since: "now".into(),
            evidence: evidence.clone(),
        },
        TaskCondition::Deferred {
            until: "later".into(),
            reason: RetryCause::ExecutionFailure,
            resume: ConditionContinuation::Dispatch {
                target_state: "working".into(),
            },
            evidence: evidence.clone(),
        },
        TaskCondition::Parked {
            primary: primary.clone(),
            additional: vec![],
            resume: ConditionContinuation::Reconcile,
            since: None,
            evidence: evidence.clone(),
        },
        TaskCondition::Failed {
            failure: primary,
            additional: vec![],
            resume: ConditionContinuation::Reconcile,
            since: None,
            evidence: evidence.clone(),
        },
        TaskCondition::Settled {
            outcome: TerminalOutcome::Completed,
            evidence,
        },
    ];
    for condition in variants {
        assert_eq!(
            decode(&serde_json::to_string(&condition).unwrap()).unwrap(),
            condition
        );
    }
}

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
        LegacyConditionInput {
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
    sqlx::raw_sql(include_str!(
        "../../migrations/V202610060030__task_condition.sql"
    ))
    .execute(&pool)
    .await
    .unwrap();
    let mut conn = pool.acquire().await.unwrap();
    for (i, expected) in rows.iter().enumerate() {
        let row = sqlx::query("SELECT * FROM task WHERE id=?")
            .bind(i.to_string())
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(LegacyConditionInput::from_row(&row), *expected);
        assert_eq!(row.get::<i64, _>("version"), 7);
        assert_eq!(row.get::<String, _>("updated_at"), "unchanged");
        assert_eq!(row.get::<String, _>("archived_at"), "archived");
        assert_eq!(row.get::<String, _>("deleted_at"), "deleted");
        let condition = decode(row.get("condition_json")).unwrap();
        assert_eq!(
            condition,
            map_legacy_condition(&mut conn, expected).await.unwrap()
        );
        let evidence = serde_json::to_value(condition).unwrap()["evidence"].clone();
        assert!(evidence["metadata"].get("queued_recovery").is_none());
        assert!(evidence["metadata"].get("plan_publication_claim").is_none());
        assert!(evidence["metadata"]
            .get("terminal_execution_settlement")
            .is_none());
    }
    let plan: Vec<String> = sqlx::query(
        "EXPLAIN QUERY PLAN SELECT condition_json FROM task_condition_legacy WHERE id='1'",
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap()
    .iter()
    .map(|r| r.get("detail"))
    .collect();
    assert!(
        plan.iter().any(|p| p.contains("SEARCH t USING INDEX")),
        "{plan:?}"
    );
    assert!(
        !plan.iter().any(|p| p == "SCAN t"),
        "projection must only map the affected row: {plan:?}"
    );
}

fn update(t: &Task) -> UpdateTask {
    UpdateTask {
        id: t.id.clone(),
        expected_version: t.version,
        title: None,
        description: None,
        priority: None,
        merge_config: None,
        plan: None,
        error_annotation: None,
        blocked_json: None,
        failed_json: None,
        task_state_config: None,
        parent_task_id: None,
        updated_at: crate::now_rfc3339(),
    }
}
#[tokio::test]
async fn each_writer_family_keeps_condition_invariant_and_legacy_observability() {
    let db = db().await;
    // Each named family applies its distinct persistence seam. Direct SQL rows
    // represent the inventory's queue/budget/admission/reconnect writers too.
    for family in [
        "create",
        "update",
        "status",
        "recovery",
        "annotation",
        "barrier",
        "barrier_authority",
        "metadata",
        "wake",
        "task_query",
        "task_query_tx",
        "bulk_query",
        "raw_queue",
        "raw_budget",
        "raw_admission",
        "raw_owner_clear",
    ] {
        let t = task(&db, family).await;
        db.check_task_condition_invariant(&t).await.unwrap();
        if family == "create" {
            continue;
        }
        let step = claim(&db, family).await;
        crate::task_writer::in_task_step(step,async {
            match family {
                "update"=>{let mut u=update(&t);u.error_annotation=Some(Some(json!({"type":"workspace_error"}).to_string()));TaskRepo::update(&db,u).await.unwrap();},
                "status"=>{TaskRepo::update_status(&db,UpdateTaskStatus{id:t.id.clone(),expected_version:t.version,status:"review".into(),assignee_id:None,error_annotation:Some(Some(json!({"type":"review_needs_owner"}).to_string())),blocked_json:None,failed_json:None,updated_at:crate::now_rfc3339()}).await.unwrap();},
                "recovery"=>{TaskRepo::update_recovery_metadata_if_no_running_execution(&db,&t.id,t.version,Some(json!({"type":"recovery_required"}).to_string()),Some(json!({"kind":"workspace_error"}).to_string()),None,&crate::now_rfc3339(),None,vec![],vec![]).await.unwrap();},
                "annotation"=>{TaskRepo::set_error_annotation_if_no_running_execution(&db,&t.id,t.version,&t.status,None,"{}",None,None,&json!({"type":"before_work_hook_failed"}).to_string(),&crate::now_rfc3339(),"stopped",None,vec![]).await.unwrap();},
                "barrier"=>{TaskRepo::set_entry_barrier(&db,&t.id,t.version,Some(json!({"status":"blocked","state":"review"}).to_string()),&crate::now_rfc3339()).await.unwrap();},
                "barrier_authority"=>{let p=ProjectRepo::get_by_id(&db,"p").await.unwrap().unwrap();TaskRepo::set_entry_barrier_with_workflow_authority(&db,&t.id,t.version,Some(json!({"status":"blocked","state":"review"}).to_string()),&crate::now_rfc3339(),p.version,p.workflow_definition).await.unwrap();},
                "metadata"=>{TaskRepo::mutate_metadata(&db,&t.id,Some(t.version),vec![TaskMetadataMutation::Set{key:"owner_wait".into(),value:json!({"daemon_id":"d"})}],&crate::now_rfc3339()).await.unwrap();},
                "wake"=>{TaskRepo::mutate_metadata(&db,&t.id,Some(t.version),vec![TaskMetadataMutation::Set{key:"dispatch_disposition".into(),value:json!({"capability":"coder"})}],&crate::now_rfc3339()).await.unwrap();TaskRepo::wake_dispatch_for_task(&db,&t.id,&crate::now_rfc3339()).await.unwrap();},
                "task_query"=>{crate::task_writer::TaskQuery::new(&db,&t.id,"UPDATE task SET blocked_json=? WHERE id=?").bind(json!({"kind":"executor_failed"}).to_string()).bind(&t.id).execute(db.pool()).await.unwrap();},
                "task_query_tx"=>{let mut tx=crate::begin_immediate(db.pool()).await.unwrap();crate::task_writer::TaskQuery::new(&db,&t.id,"UPDATE task SET error_annotation=? WHERE id=?").bind(json!({"type":"manual_stop"}).to_string()).bind(&t.id).execute_in_tx(&mut tx).await.unwrap().require_applied().unwrap();tx.commit().await.unwrap();},
                "bulk_query"=>{let mut tx=crate::begin_immediate(db.pool()).await.unwrap();crate::task_writer::BulkTaskQuery::new(&db,"UPDATE task SET error_annotation=? WHERE id=?").bind(json!({"type":"dispatch_failed"}).to_string()).bind(&t.id).execute_in_tx(&mut tx).await.unwrap();tx.commit().await.unwrap();},
                "raw_queue"|"raw_budget"|"raw_admission"|"raw_owner_clear"=>{let before:(i64,String,i64,i64)=sqlx::query_as("SELECT t.version,t.updated_at,p.board_revision,p.list_revision FROM task t JOIN project p ON p.id=t.project_id WHERE t.id=?").bind(&t.id).fetch_one(db.pool()).await.unwrap();
                    let sql=match family {"raw_queue"=>"UPDATE task SET error_annotation='{\"type\":\"workflow_loop\"}',blocked_json='{\"source\":\"workflow_loop\"}' WHERE id=?","raw_budget"=>"UPDATE task SET entry_barrier_json='{\"status\":\"blocked\",\"blocking_reason\":\"review retry budget exhausted\"}' WHERE id=?","raw_admission"=>"UPDATE task SET error_annotation=NULL,blocked_json=NULL,metadata_json='{\"dispatch_disposition\":{\"capability\":\"machine_capacity\"}}' WHERE id=?",_=>"UPDATE task SET metadata_json='{\"owner_wait\":{\"daemon_id\":\"d\"}}' WHERE id=?"};
                    sqlx::query(sql).bind(&t.id).execute(db.pool()).await.unwrap();
                    let after:(i64,String)=sqlx::query_as("SELECT version,updated_at FROM task WHERE id=?").bind(&t.id).fetch_one(db.pool()).await.unwrap();assert_eq!((before.0,before.1),after);
                },
                _=>unreachable!(),
            }
            let current=TaskRepo::get_by_id(&db,&t.id,true).await.unwrap().unwrap();
            db.check_task_condition_invariant(&current).await.unwrap_or_else(|e|panic!("{family}: {e}"));
            // A shadow-only sync must leave legacy versions/revisions/events alone.
            let before:(i64,i64,i64,i64)=sqlx::query_as("SELECT t.version,p.board_revision,p.list_revision,(SELECT COUNT(*) FROM domain_event) FROM task t JOIN project p ON p.id=t.project_id WHERE t.id=?").bind(&t.id).fetch_one(db.pool()).await.unwrap();
            let mut tx=crate::begin_immediate(db.pool()).await.unwrap();db.sync_condition_in_tx(&mut tx,&t.id).await.unwrap();tx.commit().await.unwrap();
            let after:(i64,i64,i64,i64)=sqlx::query_as("SELECT t.version,p.board_revision,p.list_revision,(SELECT COUNT(*) FROM domain_event) FROM task t JOIN project p ON p.id=t.project_id WHERE t.id=?").bind(&t.id).fetch_one(db.pool()).await.unwrap();assert_eq!(before,after,"{family}");
        }).await;
    }
}

#[tokio::test]
async fn stale_condition_version_source_and_lease_are_rejected() {
    let db = db().await;
    let t = task(&db, "fence").await;
    let step = claim(&db, &t.id).await;
    crate::task_writer::in_task_step(step.clone(), async {
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        let input = LegacyConditionInput::from(&t);
        let condition = map_legacy_condition(&mut tx, &input).await.unwrap();
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
    let input = LegacyConditionInput::from(&t);
    let condition = map_legacy_condition(&mut tx, &input).await.unwrap();
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
    let mut connection = db.pool().acquire().await.unwrap();
    let annotation = r#"{"type":"manual_stop","blocked_by":"\ud800","message":"retained"}"#;
    let metadata = r#"{"paused_integration_generation":1e999,"last_workflow_guard_reason":"\ud800","owner_wait":{"daemon_id":"Nڠ","started_at":"now"}}"#;
    let legacy = LegacyConditionInput {
        error_annotation: Some(annotation.into()),
        metadata_json: Some(metadata.into()),
        ..Default::default()
    };
    let condition = map_legacy_condition(&mut connection, &legacy)
        .await
        .unwrap();
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
    let legacy = LegacyConditionInput {
        metadata_json: Some(deep.clone()),
        ..Default::default()
    };
    let condition = map_legacy_condition(&mut connection, &legacy)
        .await
        .unwrap();
    let value = serde_json::to_value(condition).unwrap();
    let fragment = value["evidence"]["metadata"]["owner_wait"]
        .as_str()
        .unwrap();
    assert!(fragment.contains(&"{\"nested\":".repeat(200)));
    sqlx::query("INSERT INTO task(id,project_id,title,error_annotation,metadata_json,created_at,updated_at) VALUES('unicode','p','unicode',?,?,?,?)")
        .bind(annotation).bind(metadata).bind(crate::now_rfc3339()).bind(crate::now_rfc3339()).execute(&mut *connection).await.unwrap();
    drop(connection);
    let task = TaskRepo::get_by_id(&db, "unicode", false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.error_annotation.as_deref(), Some(annotation));
    assert_eq!(task.metadata_json.as_deref(), Some(metadata));
    db.check_task_condition_invariant(&task).await.unwrap();
}
