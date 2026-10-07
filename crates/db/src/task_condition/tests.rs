use super::producer_tests::{enqueue, pending_cancel, set_parent, workspace};
use super::*;
use crate::{
    CreateProject, CreateTask, EnqueueTaskStep, ProjectRepo, TaskMetadataMutation, TaskRepo,
    TaskStepRepo, UpdateTask, UpdateTaskStatus,
};
use serde_json::{json, Value};
use sqlx::SqliteConnection;

pub(super) async fn db() -> SqliteDb {
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
pub(super) async fn task(db: &SqliteDb, id: &str) -> Task {
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
pub(super) async fn claim(db: &SqliteDb, id: &str) -> crate::TaskStep {
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
pub(super) fn input(
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
        non_text: Vec::new(),
        facts: None,
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
        _ => condition.evidence().observations.iter().collect(),
    }
}

#[tokio::test]
async fn mapping_table_preserves_known_unknown_and_combined_conditions() {
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
                Some(json!({"kind":"review_budget_exhausted"})),
                None,
                None,
                None,
            ),
            "parked",
            Some("budget_exhausted"),
        ),
        // M1: the dispatcher does not block on these annotations alone.
        (
            "budget_annotation_only",
            input(
                Some(json!({"type":"review_budget_exhausted"})),
                None,
                None,
                None,
                None,
            ),
            "clear",
            None,
        ),
        (
            "advisory_merge_annotation",
            input(
                Some(json!({"type":"merge_conflict","message":"auto repair"})),
                None,
                None,
                None,
                None,
            ),
            "clear",
            None,
        ),
        (
            "empty_annotation",
            input(Some(json!({})), None, None, None, None),
            "clear",
            None,
        ),
        // m4: a blocking kind that is not a FailureKind.
        (
            "agent_timeout",
            input(
                Some(json!({"type":"agent_timeout"})),
                None,
                None,
                None,
                None,
            ),
            "parked",
            Some("agent_timeout"),
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
            "clear",
            None,
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
            "clear",
            None,
        ),
        (
            "unknown_blocked_kind",
            input(
                None,
                Some(json!({"kind":"future_failure"})),
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
            "clear",
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
        let condition = map_legacy_condition(&legacy);
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
        let again = map_legacy_condition(&legacy);
        assert_eq!(condition, again, "deterministic {name}");
        assert_eq!(
            decode(&encode(&condition)).unwrap(),
            condition,
            "stored form round-trips {name}"
        );
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
            let condition = map_legacy_condition(&legacy);
            if field == 0 {
                // M1: the dispatcher dispatches through an annotation it
                // cannot read, so it is evidence, not a park.
                assert!(
                    matches!(&condition, TaskCondition::Clear { evidence }
                        if evidence.error_annotation.as_deref() == Some(raw)),
                    "{field}:{raw}"
                );
                continue;
            }
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
    let condition = map_legacy_condition(&combined);
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
        reasons(&map_legacy_condition(&ambiguous))[0],
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
    assert_eq!(reasons(&map_legacy_condition(&duplicate)).len(), 1);
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
        map_legacy_condition(&ci),
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
            until: Some("later".into()),
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

const MIGRATION: &str = include_str!("../../migrations/V202610060030__task_condition.sql");

/// The migration as the runner applies it: SQL, then the Rust backfill.
async fn migrate(connection: &mut SqliteConnection) {
    sqlx::raw_sql(MIGRATION)
        .execute(&mut *connection)
        .await
        .unwrap();
    backfill(connection).await.unwrap();
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
        LegacyConditionInput::default(),
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
            "SELECT {LEGACY_SELECT},version,updated_at,archived_at,deleted_at FROM task WHERE id=?"
        ))
        .bind(i.to_string())
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        assert_eq!(LegacyConditionInput::from_row(&row).unwrap(), *expected);
        assert_eq!(row.get::<i64, _>("version"), 7);
        assert_eq!(row.get::<String, _>("updated_at"), "unchanged");
        assert_eq!(row.get::<String, _>("archived_at"), "archived");
        assert_eq!(row.get::<String, _>("deleted_at"), "deleted");
        let condition = decode(row.get("condition_json")).unwrap();
        assert_eq!(condition, map_legacy_condition(expected));
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
        map_legacy_condition(&LegacyConditionInput::default()).read()
    );
    assert!(
        condition.evidence().witnesses.is_empty(),
        "column defaults are rederived by the first producer"
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
                "task_query"=>{crate::task_writer::TaskQuery::new(&db,&t.id,"UPDATE task SET blocked_json=? WHERE id=?").bind(json!({"kind":"executor_failed"}).to_string()).bind(&t.id).execute(db.pool()).await.unwrap();},
                "task_query_tx"=>{let mut tx=crate::begin_immediate(db.pool()).await.unwrap();crate::task_writer::TaskQuery::new(&db,&t.id,"UPDATE task SET error_annotation=? WHERE id=?").bind(json!({"type":"manual_stop"}).to_string()).bind(&t.id).execute_in_tx(&mut tx).await.unwrap().require_applied().unwrap();tx.commit().await.unwrap();},
                // Placement/environment/reconnect shape: the value exists only in SQL.
                "task_query_sql_computed"=>{crate::task_writer::TaskQuery::new(&db,&t.id,"UPDATE task SET metadata_json=json_set(COALESCE(metadata_json,'{}'),'$.environment_wait',json(?),'$.deferred_dispatch.reason',?) WHERE id=?").bind(json!({"kind":"environment_not_ready"}).to_string()).bind("environment_not_ready: server").bind(&t.id).execute(db.pool()).await.unwrap();},
                "bulk_query"=>{let mut tx=crate::begin_immediate(db.pool()).await.unwrap();crate::task_writer::BulkTaskQuery::new(&db,"UPDATE task SET error_annotation=? WHERE id=?").bind(json!({"type":"dispatch_failed"}).to_string()).bind(&t.id).execute_in_tx(&mut tx).await.unwrap();tx.commit().await.unwrap();},
                // Queue settlement, lifecycle and transition writers in services
                // run raw SQL and then call the public seam in the same transaction.
                "direct_sql_seam"=>{let before:(i64,String)=sqlx::query_as("SELECT version,updated_at FROM task WHERE id=?").bind(&t.id).fetch_one(db.pool()).await.unwrap();
                    let mut tx=crate::begin_immediate(db.pool()).await.unwrap();
                    sqlx::query("UPDATE task SET error_annotation='{\"type\":\"workflow_loop\"}',blocked_json='{\"source\":\"workflow_loop\"}' WHERE id=?").bind(&t.id).execute(&mut *tx).await.unwrap();
                    db.sync_condition_in_tx(&mut tx,&t.id).await.unwrap();tx.commit().await.unwrap();
                    let after:(i64,String)=sqlx::query_as("SELECT version,updated_at FROM task WHERE id=?").bind(&t.id).fetch_one(db.pool()).await.unwrap();assert_eq!(before,after);
                },
                "removed_machine_cancel_clear"=>{crate::task_writer::TaskQuery::new(&db,&t.id,"UPDATE task SET error_annotation=NULL,metadata_json=CASE WHEN json_valid(metadata_json) THEN json_remove(metadata_json,'$.deferred_dispatch','$.dispatch_disposition') ELSE metadata_json END,version=version+1 WHERE id=?").bind(&t.id).identity_fenced().execute(db.pool()).await.unwrap();},
                "removed_machine_owner_wait_clear"=>{TaskRepo::mutate_metadata(&db,&t.id,None,vec![TaskMetadataMutation::Set{key:"owner_wait".into(),value:json!({"daemon_id":"removed"})}],&crate::now_rfc3339()).await.unwrap();crate::task_writer::TaskQuery::new(&db,&t.id,"UPDATE task SET metadata_json=json_remove(metadata_json,'$.owner_wait','$.deferred_dispatch','$.dispatch_disposition') WHERE id=? AND json_valid(metadata_json) AND json_extract(metadata_json,'$.owner_wait.daemon_id')=?").bind(&t.id).bind("removed").identity_fenced().execute(db.pool()).await.unwrap();},
                "removed_machine_annotation"=>{crate::task_writer::TaskQuery::new(&db,&t.id,"UPDATE task SET error_annotation=?,version=version+1 WHERE id=?").bind(json!({"type":"recovery_required","blocking_reason":"machine_removed","blocked_by":"removed"}).to_string()).bind(&t.id).identity_fenced().execute(db.pool()).await.unwrap();},
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
        let input = LegacyConditionInput::from(&t);
        let condition = map_legacy_condition(&input);
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
        let current = LegacyConditionInput {
            metadata_json: Some("{\"owner_wait\":{\"daemon_id\":\"d\"}}".into()),
            ..input.clone()
        };
        assert!(matches!(
            db.set_condition(&mut tx, &t.id, t.version, &current, &condition)
                .await,
            Err(DbError::Check(_))
        ));
        let facts = ConditionFacts::load(&mut tx, &t.id).await.unwrap();
        let parked = facts.apply(map_legacy_condition(&current));
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
    let input = LegacyConditionInput::from(&t);
    let condition = map_legacy_condition(&input);
    assert!(matches!(
        db.set_condition(&mut tx, &t.id, t.version, &input, &condition)
            .await,
        Err(DbError::Check(_))
    ));
    tx.rollback().await.unwrap();
}

/// m1: a stored condition that no longer decodes must not fail a legitimate
/// legacy write. The seam compares text and overwrites.
#[tokio::test]
async fn undecodable_stored_condition_is_recomputed_not_fatal() {
    let db = db().await;
    let t = task(&db, "undecodable").await;
    sqlx::query("UPDATE task SET condition_json='{\"kind\":\"from_a_future_release\"}' WHERE id=?")
        .bind(&t.id)
        .execute(db.pool())
        .await
        .unwrap();
    assert!(db.task_condition(&t.id).await.is_err());
    let step = claim(&db, &t.id).await;
    crate::task_writer::in_task_step(step, async {
        let updated = TaskRepo::mutate_metadata(
            &db,
            &t.id,
            Some(t.version),
            vec![TaskMetadataMutation::Set {
                key: "owner_wait".into(),
                value: json!({"daemon_id":"d"}),
            }],
            &crate::now_rfc3339(),
        )
        .await
        .unwrap();
        db.check_task_condition_invariant(&updated).await.unwrap();
        sqlx::query(
            "UPDATE task SET condition_json='{\"kind\":\"from_a_future_release\"}' WHERE id=?",
        )
        .bind(&t.id)
        .execute(db.pool())
        .await
        .unwrap();
        crate::task_writer::TaskQuery::new(
            &db,
            &t.id,
            "UPDATE task SET metadata_json=json_remove(metadata_json,'$.owner_wait') WHERE id=?",
        )
        .bind(&t.id)
        .execute(db.pool())
        .await
        .unwrap();
    })
    .await;
    assert!(matches!(
        db.task_condition(&t.id).await.unwrap(),
        TaskCondition::Clear { .. }
    ));
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
    let annotation = r#"{"type":"manual_stop","blocked_by":"\ud800","message":"retained"}"#;
    let metadata = r#"{"paused_integration_generation":1e999,"last_workflow_guard_reason":"\ud800","owner_wait":{"daemon_id":"Nڠ","started_at":"now"}}"#;
    let legacy = LegacyConditionInput {
        error_annotation: Some(annotation.into()),
        metadata_json: Some(metadata.into()),
        ..Default::default()
    };
    let condition = map_legacy_condition(&legacy);
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
    let condition = map_legacy_condition(&legacy);
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

// ---------------------------------------------------------------------------
// Ported audit probes (audit-31-s1): each asserts the fixed behaviour.
// ---------------------------------------------------------------------------

/// Hostile legacy values for the four condition columns.
fn hostile() -> Vec<(String, Option<String>)> {
    let big = "x".repeat(1_000_000);
    let deep_ok = format!("{}1{}", "[".repeat(900), "]".repeat(900));
    let deep_bad = format!("{}1{}", "[".repeat(5000), "]".repeat(5000));
    let mut v: Vec<(String, Option<String>)> = [
        ("null", None),
        ("empty", Some("".to_owned())),
        ("space", Some(" ".to_owned())),
        ("bad", Some("{bad".to_owned())),
        ("jnull", Some("null".to_owned())),
        ("arr", Some("[]".to_owned())),
        ("str", Some("\"s\"".to_owned())),
        ("num", Some("123".to_owned())),
        ("true", Some("true".to_owned())),
        ("json5", Some("{kind:'manual_stop'}".to_owned())),
        ("nan", Some("NaN".to_owned())),
        ("dupkey", Some(r#"{"kind":"a","kind":"manual_stop","type":"x","type":"ci_failed"}"#.to_owned())),
        ("surrogate", Some(r#"{"kind":"\ud800","type":"\ud800","blocked_by":"\udfff","blocking_reason":"\ud800","state":"\ud800","status":"\ud800"}"#.to_owned())),
        ("nul", Some("{\"kind\":\"a\\u0000b\",\"type\":\"a\\u0000b\"}".to_owned())),
        ("rawnul", Some("{\"kind\":\"a\0b\"}".to_owned())),
        ("types", Some(r#"{"kind":1,"type":[1],"blocked_by":{},"blocking_reason":false,"state":1.5,"status":null}"#.to_owned())),
        ("bignum", Some(r#"{"kind":1e999,"type":123456789012345678901234567890,"n":-0.0}"#.to_owned())),
        ("deep_ok", Some(deep_ok)),
        ("deep_bad", Some(deep_bad)),
        ("big_str", Some(format!("{{\"kind\":\"{big}\",\"type\":\"{big}\",\"message\":\"{big}\"}}"))),
        ("big_arab", Some(format!("{{\"kind\":\"{0}\",\"type\":\"{0}\"}}", "\u{06A9}".repeat(40_000)))),
        ("cjk", Some(r#"{"kind":"中文한국어😀","type":"中文한국어😀","message":"失败"}"#.to_owned())),
        ("bom", Some("\u{feff}{\"kind\":\"manual_stop\"}".to_owned())),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_owned(), value))
    .collect();
    for k in [
        "pull_request_merge",
        "merge_fix_ci_failed",
        "agent_timeout",
        "dispatch_failed",
        "workflow_loop",
        "cascade_failed",
        "dependency_cancelled",
        "merge_conflict",
        "target_repo_dirty",
        "dirty_worktree",
        "ci_failed",
        "review_gate_failed",
        "review_budget_exhausted",
        "review_blocked",
        "review_needs_owner",
        "environment_not_ready",
        "retry_exhausted",
        "merge_fix_budget_exhausted",
        "workflow_guard_rejected",
        "internal_command_failed",
        "executor_failed",
        "workspace_failed",
        "workspace_reset_required",
        "workspace_error",
        "before_work_hook_timeout",
        "before_work_hook_failed",
        "max_turns_exceeded",
        "manual_stop",
        "recovery_required",
        "executor_unavailable",
        "unknown",
        "Unknown",
        "",
    ] {
        v.push((format!("k:{k}"), Some(json!({"kind":k,"type":k,"blocking_reason":k,"status":"blocked","state":"review","source":"workflow_loop","blocked_by":"user:1"}).to_string())));
    }
    for r in [
        "review_ci_infrastructure_exhausted",
        "pending_remote_cancel",
        "dependency_cancelled",
        "review_ci_infrastructure",
        "review_ci_unavailable",
        "review retry budget exhausted",
    ] {
        v.push((format!("r:{r}"), Some(json!({"type":"before_work_hook_failed","kind":"review_gate_failed","blocking_reason":r,"status":"blocked","state":"review"}).to_string())));
    }
    for s in ["running", "blocked", "passed", ""] {
        v.push((
            format!("s:{s}"),
            Some(json!({"status":s,"state":"x"}).to_string()),
        ));
    }
    v
}
fn hostile_metadata() -> Vec<Option<String>> {
    let mut v = vec![
        None,
        Some("".into()),
        Some("{bad".into()),
        Some("null".into()),
        Some("[1]".into()),
        Some("\"s\"".into()),
        Some("{}".into()),
        Some(format!("{{\"custom\":\"{}\"}}", "y".repeat(1_000_000))),
        Some(
            r#"{"a\"b":1,"$":2,"x.y":3,"[0]":4,"":5,"awaiting_human":true,"awaiting_human":false}"#
                .into(),
        ),
    ];
    let keys = [
        "awaiting_human",
        "awaiting_human_marker_id",
        "awaiting_human_reason",
        "coordination_review_pending",
        "coordination_review_pending_id",
        "daemon_upgrade_refusal",
        "deferred_dispatch",
        "dispatch_disposition",
        "environment_wait",
        "executor_unavailable_execution_id",
        "last_execution_failure_at",
        "last_execution_failure_execution_id",
        "last_workflow_guard_execution_id",
        "last_workflow_guard_name",
        "last_workflow_guard_reason",
        "last_workflow_guard_rejection_at",
        "owner_wait",
        "paused_integration",
        "paused_integration_generation",
        "placement_refusal",
        "plan_settlement_wait",
        "planning_completed_at",
        "planning_execution_id",
        "planning_state_entry_token",
        "queued_recovery",
        "plan_publication_claim",
        "plan_publication_cleanup",
        "terminal_execution_settlement",
    ];
    let vals = [
        json!(null),
        json!(true),
        json!(false),
        json!(1),
        json!(1e300),
        json!("s"),
        json!("\u{06A9}x"),
        json!([1,{"a":[]}]),
        json!({}),
        json!({"kind":"environment_probe_pending","reason":"r","not_before":"later","target_state":7,"capability":"project_capacity","state":"review","daemon_id":1,"execution_id":["e"],"started_at":{},"blocker_digest":3}),
        json!({"not_before":"2026-10-05T12:00:00.123456789+00:00","reason":"r","target_state":"todo","kind":"x"}),
        json!({"not_before":"9999-99-99T99:99:99Z","reason":"r","target_state":"todo"}),
        json!({"not_before":"2026-10-05 12:00:00","reason":"r","target_state":"todo"}),
        json!({"capability":"machine_capacity"}),
        json!({"capability":"\u{06A9}"}),
        json!({"kind":"provision_failed"}),
    ];
    for k in keys {
        for val in &vals {
            v.push(Some(json!({k: val}).to_string()));
        }
    }
    // Raw escapes that serde_json::json! would normalise.
    v.push(Some(r#"{"awaiting_human":true,"awaiting_human_reason":"\ud800","deferred_dispatch":{"not_before":"\ud800","reason":"\udc00","target_state":"\ud800"},"owner_wait":{"daemon_id":"\ud800"},"dispatch_disposition":{"capability":"\ud800"},"environment_wait":{"kind":"\ud800"},"paused_integration":{"state":"\ud800"},"plan_settlement_wait":{"execution_id":"\ud800"}}"#.into()));
    v.push(Some(r#"{"deferred_dispatch":{"not_before":1e999,"reason":123456789012345678901234567890,"target_state":"\u0000"}}"#.into()));
    let all: serde_json::Map<String, Value> = keys
        .iter()
        .map(|k| (k.to_string(), vals[9].clone()))
        .collect();
    v.push(Some(Value::Object(all).to_string()));
    v
}
const LEGACY_COLUMNS: [&str; 5] = [
    "error_annotation",
    "blocked_json",
    "failed_json",
    "entry_barrier_json",
    "metadata_json",
];
async fn clone_tasks(db: &SqliteDb, template: &str, n: usize, prefix: &str) {
    let columns: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM pragma_table_info('task') WHERE name NOT IN ('id','condition_json')",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    let columns = columns.join(",");
    let mut tx = db.pool().begin().await.unwrap();
    for i in 0..n {
        sqlx::query(&format!(
            "INSERT INTO task(id,{columns}) SELECT ?,{columns} FROM task WHERE id=?"
        ))
        .bind(format!("{prefix}{i}"))
        .bind(template)
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
}
/// Back to the pre-stage-one schema, keeping every other table and trigger.
async fn unmigrate(db: &SqliteDb) {
    sqlx::raw_sql(
        "DROP TRIGGER task_schedule_update; DROP TRIGGER task_schedule_relationship; DROP INDEX IF EXISTS idx_task_schedule_open; DROP INDEX IF EXISTS idx_task_condition_kind; DROP INDEX IF EXISTS idx_task_condition_retry_project; DROP TRIGGER IF EXISTS task_list_revision_task_update; ALTER TABLE task DROP COLUMN condition_json;",
    )
    .execute(db.pool())
    .await
    .unwrap();
}
async fn legacy_snapshot(db: &SqliteDb) -> Vec<(String, String)> {
    sqlx::query_as("SELECT id, hex(COALESCE(error_annotation,'~N'))||'|'||hex(COALESCE(blocked_json,'~N'))||'|'||hex(COALESCE(failed_json,'~N'))||'|'||hex(COALESCE(entry_barrier_json,'~N'))||'|'||hex(COALESCE(metadata_json,'~N'))||'|'||version||'|'||updated_at||'|'||typeof(error_annotation)||typeof(blocked_json)||typeof(failed_json)||typeof(entry_barrier_json)||typeof(metadata_json) FROM task ORDER BY id").fetch_all(db.pool()).await.unwrap()
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
    for (offset, field) in LEGACY_FIELDS.iter().enumerate() {
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

/// Port of `audit_mapping_shapes`: what real producer shapes map to, now as
/// assertions. M1: nothing the dispatcher dispatches through is parked.
#[test]
fn real_producer_shapes_map_to_what_legacy_blocks_on() {
    let now = (chrono::Utc::now() + chrono::Duration::seconds(30)).to_rfc3339();
    let meta = |value: Value| input(None, None, None, None, Some(value));
    let ann = |value: Value| input(Some(value), None, None, None, None);
    let cases: Vec<(&str, LegacyConditionInput, &str, Option<&str>)> = vec![
        (
            "deferred real rfc3339",
            meta(
                json!({"deferred_dispatch":{"not_before":now,"reason":"board drag dispatch cooldown","target_state":"in_progress"}}),
            ),
            "deferred",
            None,
        ),
        (
            "deferred Z micro",
            meta(
                json!({"deferred_dispatch":{"not_before":"2026-10-05T12:00:00.123456Z","reason":"r","target_state":"todo"}}),
            ),
            "deferred",
            None,
        ),
        (
            "deferred offset -07:00",
            meta(
                json!({"deferred_dispatch":{"not_before":"2026-10-05T12:00:00-07:00","reason":"r","target_state":"todo"}}),
            ),
            "deferred",
            None,
        ),
        (
            "env probe deferral (kind+reason only)",
            meta(
                json!({"environment_wait":{"kind":"environment_probe_pending","machine":{"owner_kind":"server"}},"deferred_dispatch":{"kind":"environment_probe_pending","reason":"environment_probe_pending: server"}}),
            ),
            "parked",
            Some("environment"),
        ),
        (
            "env deferral reason only",
            meta(json!({"deferred_dispatch":{"reason":"environment_probe_pending: server"}})),
            "clear",
            None,
        ),
        (
            "review CI infra co-write",
            input(
                Some(
                    json!({"type":"before_work_hook_failed","blocking_reason":"review_ci_infrastructure","blocked_at":now,"blocked_by":"system:workflow","message":"x"}),
                ),
                None,
                None,
                Some(json!({"state":"review","status":"blocked","blocking_reason":"x"})),
                Some(
                    json!({"deferred_dispatch":{"not_before":now,"reason":"x","target_state":"review"}}),
                ),
            ),
            "deferred",
            None,
        ),
        (
            "deferred + last_execution_failure",
            meta(
                json!({"last_execution_failure_at":now,"last_execution_failure_execution_id":"e","deferred_dispatch":{"not_before":now,"reason":"retry","target_state":"in_progress"}}),
            ),
            "deferred",
            None,
        ),
        (
            "deferred + capacity disposition",
            meta(
                json!({"dispatch_disposition":{"capability":"project_capacity"},"deferred_dispatch":{"not_before":now,"reason":"retry","target_state":"in_progress"}}),
            ),
            "parked",
            Some("capacity"),
        ),
        (
            "ann pull_request_merge",
            ann(json!({"type":"pull_request_merge"})),
            "clear",
            None,
        ),
        (
            "ann merge_fix_ci_failed",
            ann(json!({"type":"merge_fix_ci_failed","message":"m"})),
            "clear",
            None,
        ),
        (
            "ann merge_conflict",
            ann(json!({"type":"merge_conflict"})),
            "clear",
            None,
        ),
        (
            "ann ci_failed",
            ann(json!({"type":"ci_failed"})),
            "clear",
            None,
        ),
        (
            "ann executor_failed",
            ann(json!({"type":"executor_failed"})),
            "clear",
            None,
        ),
        (
            "ann retry_exhausted",
            ann(json!({"type":"retry_exhausted"})),
            "clear",
            None,
        ),
        (
            "ann agent_timeout",
            ann(json!({"type":"agent_timeout"})),
            "parked",
            Some("agent_timeout"),
        ),
        ("ann no type", ann(json!({"message":"only"})), "clear", None),
        ("ann empty object", ann(json!({})), "clear", None),
        (
            "ann kind-not-type",
            ann(json!({"kind":"ci_failed"})),
            "clear",
            None,
        ),
        (
            "ann annotation_type",
            ann(json!({"annotation_type":"ci_failed"})),
            "clear",
            None,
        ),
        (
            "stale nonblocking ann",
            ann(json!({"type":"merge_conflict","blocking":false})),
            "clear",
            None,
        ),
        (
            "blocked empty object",
            input(None, Some(json!({})), None, None, None),
            "parked",
            Some("unknown_condition"),
        ),
        (
            "blocked no kind",
            input(
                None,
                Some(json!({"blocked_by":"user","reason":"x"})),
                None,
                None,
                None,
            ),
            "parked",
            Some("unknown_condition"),
        ),
        (
            "blocked agent_timeout",
            input(
                None,
                Some(json!({"kind":"agent_timeout"})),
                None,
                None,
                None,
            ),
            "parked",
            Some("agent_timeout"),
        ),
        (
            "barrier passed",
            input(
                None,
                None,
                None,
                Some(json!({"state":"review","status":"passed"})),
                None,
            ),
            "parked",
            Some("unknown_condition"),
        ),
        (
            "barrier no status",
            input(None, None, None, Some(json!({"state":"review"})), None),
            "parked",
            Some("unknown_condition"),
        ),
        (
            "barrier running",
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
            "awaiting_human string true",
            meta(json!({"awaiting_human":"true"})),
            "clear",
            Some("unknown_condition"),
        ),
        (
            "awaiting_human false + reason",
            meta(json!({"awaiting_human":false,"awaiting_human_reason":"plan_review"})),
            "clear",
            None,
        ),
        (
            "awaiting_human pull_request_merge",
            meta(json!({"awaiting_human":true,"awaiting_human_reason":"pull_request_merge"})),
            "parked",
            Some("human_decision"),
        ),
        // m5: stays Clear until stage 2 adds the child witness.
        (
            "coordination_review_pending",
            meta(json!({"coordination_review_pending":true,"coordination_review_pending_id":"s"})),
            "clear",
            None,
        ),
        (
            "queued_recovery only",
            meta(json!({"queued_recovery":{"id":"q"}})),
            "clear",
            None,
        ),
        (
            "disposition safe_message only",
            meta(json!({"dispatch_disposition":{"safe_message":"denied"}})),
            "parked",
            Some("dispatch_refusal"),
        ),
        (
            "disposition null",
            meta(json!({"dispatch_disposition":null,"owner_wait":null})),
            "clear",
            None,
        ),
        (
            "owner_wait + deferred + plan wait",
            meta(
                json!({"owner_wait":{"daemon_id":"d","started_at":now},"plan_settlement_wait":{"execution_id":"e"},"deferred_dispatch":{"not_before":now,"reason":"r","target_state":"planning"}}),
            ),
            "parked",
            Some("owner_offline"),
        ),
        (
            "plan wait + deferred",
            meta(
                json!({"plan_settlement_wait":{"execution_id":"e"},"deferred_dispatch":{"not_before":now,"reason":"r","target_state":"planning"}}),
            ),
            "deferred",
            None,
        ),
        (
            "failed + manual_stop block",
            input(
                Some(json!({"type":"retry_exhausted"})),
                Some(json!({"kind":"manual_stop"})),
                Some(json!({"kind":"executor_failed"})),
                None,
                None,
            ),
            "failed",
            Some("failure"),
        ),
    ];
    for (name, legacy, expected, primary) in cases {
        let condition = map_legacy_condition(&legacy);
        let value = serde_json::to_value(&condition).unwrap();
        assert_eq!(value["kind"], expected, "{name}: {value}");
        assert_eq!(
            condition.is_blocked(),
            matches!(expected, "parked" | "failed"),
            "{name}"
        );
        if let Some(kind) = primary {
            assert_eq!(
                serde_json::to_value(reasons(&condition)[0]).unwrap()["kind"],
                kind,
                "{name}: {value}"
            );
        }
    }
    assert!(matches!(
        map_legacy_condition(&meta(
            json!({"last_execution_failure_execution_id":"e","deferred_dispatch":{"not_before":now,"reason":"retry","target_state":"in_progress"}})
        )),
        TaskCondition::Deferred {
            reason: RetryCause::ExecutionFailure,
            resume: ConditionContinuation::Dispatch { .. },
            ..
        }
    ));
    assert!(matches!(
        map_legacy_condition(&meta(
            json!({"plan_settlement_wait":{"execution_id":"e"},"deferred_dispatch":{"not_before":now,"reason":"r","target_state":"planning"}})
        )),
        TaskCondition::Deferred {
            reason: RetryCause::PlanTransport,
            resume: ConditionContinuation::SettlePlan { .. },
            ..
        }
    ));
}

/// M1: an annotation contributes a park reason exactly when the dispatcher's
/// predicate blocks on it, for every FailureKind name and hostile text. The
/// services crate ties the same list to `has_blocking_annotation`.
#[test]
fn annotation_parks_exactly_where_the_dispatcher_blocks() {
    for (name, value) in hostile() {
        let Some(raw) = value else { continue };
        let blocks = serde_json::from_str::<Value>(&raw)
            .ok()
            .and_then(|annotation| annotation.get("type")?.as_str().map(str::to_owned))
            .is_some_and(|kind| LEGACY_BLOCKING_ANNOTATION_KINDS.contains(&kind.as_str()));
        assert_eq!(legacy_annotation_blocks(Some(&raw)), blocks, "{name}");
        let condition = map_legacy_condition(&LegacyConditionInput {
            error_annotation: Some(raw),
            ..Default::default()
        });
        assert_eq!(condition.is_blocked(), blocks, "{name}: {condition:?}");
        if !blocks {
            assert!(matches!(condition, TaskCondition::Clear { .. }), "{name}");
        }
    }
    for kind in LEGACY_BLOCKING_ANNOTATION_KINDS {
        let condition =
            map_legacy_condition(&input(Some(json!({"type":kind})), None, None, None, None));
        let primary = serde_json::to_value(reasons(&condition)[0]).unwrap();
        assert_ne!(
            primary["kind"], "unknown_condition",
            "{kind} has a typed mapping"
        );
    }
    assert!(!legacy_annotation_blocks(None));
}

/// m2 (port of `audit_size_scaling`'s concern): the stored condition is
/// bounded whatever the row holds, and an unrelated large metadata key is
/// neither copied nor needed.
#[tokio::test]
async fn evidence_is_bounded_and_large_unrelated_fields_are_not_copied() {
    let big = "m".repeat(1_000_000);
    let legacy = LegacyConditionInput {
        error_annotation: Some(json!({"type":"workspace_error","message":big}).to_string()),
        blocked_json: Some(json!({"kind":"workspace_error","reason":big}).to_string()),
        metadata_json: Some(json!({"custom":big,"owner_wait":{"daemon_id":"d","snapshot":big},"last_workflow_guard_reason":big}).to_string()),
        ..Default::default()
    };
    let condition = map_legacy_condition(&legacy);
    let stored = encode(&condition);
    assert!(
        stored.len() < 6 * (EVIDENCE_VALUE_LIMIT + 64),
        "{}",
        stored.len()
    );
    assert!(!stored.contains("custom"));
    let TaskCondition::Parked { evidence, .. } = &condition else {
        panic!("{condition:?}")
    };
    let annotation = evidence.error_annotation.as_deref().unwrap();
    assert!(annotation.ends_with(&format!(
        "…[truncated; {} bytes]",
        legacy.error_annotation.as_ref().unwrap().len()
    )));
    assert!(evidence.metadata["owner_wait"].contains("[truncated; "));
    assert!(reasons(&condition)
        .iter()
        .any(|r| matches!(r, ParkReason::OwnerOffline { daemon_id: Some(id), .. } if id == "d")));
    // Truncation never splits a character.
    let wide = LegacyConditionInput {
        blocked_json: Some(format!(
            "{{\"kind\":\"x\",\"reason\":\"{}\"}}",
            "\u{06A9}".repeat(40_000)
        )),
        ..Default::default()
    };
    assert_eq!(
        decode(&encode(&map_legacy_condition(&wide))).unwrap(),
        map_legacy_condition(&wide)
    );

    // The legacy columns keep the full data.
    let db = db().await;
    let t = task(&db, "big").await;
    let step = claim(&db, &t.id).await;
    crate::task_writer::in_task_step(step, async {
        let mut u = update(&t);
        u.error_annotation = Some(legacy.error_annotation.clone());
        let updated = TaskRepo::update(&db, u).await.unwrap();
        assert_eq!(updated.error_annotation, legacy.error_annotation);
        db.check_task_condition_invariant(&updated).await.unwrap();
    })
    .await;
    let length: i64 = sqlx::query_scalar("SELECT length(condition_json) FROM task WHERE id='big'")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert!(length < 2 * EVIDENCE_VALUE_LIMIT as i64, "{length}");
}

/// B1: the queue adapters skip the seam for SQL that cannot change a legacy
/// condition column.
#[test]
fn seam_is_skipped_for_sql_that_cannot_change_a_condition() {
    assert_eq!(
        sql_change(
            "UPDATE task SET status='done',version=version+1 WHERE id=? AND blocked_json IS NULL"
        ),
        Some(ConditionChange::Entry)
    );
    assert_eq!(
        sql_change("UPDATE task SET title=? WHERE id=? AND blocked_json IS NULL"),
        None
    );
    assert_eq!(
        sql_change(
            "UPDATE task SET metadata_json = json_remove(metadata_json, '$.owner_wait') WHERE id = ?"
        ),
        Some(ConditionChange::Legacy)
    );
    assert_eq!(
        sql_change("UPDATE task\n SET error_annotation=NULL\n WHERE id=?"),
        Some(ConditionChange::Legacy)
    );
    // The statements the services queue for role rows, as they write them.
    for role_write in [
        "UPDATE task_role_assignment SET assignee_id=NULL,updated_at=? WHERE task_id=? AND assignee_type='agent' AND assignee_id=?",
        "DELETE FROM task_role_assignment WHERE task_id = ? AND role_name = ?",
        "DELETE FROM review WHERE id = ? AND status = 'running'",
    ] {
        assert_eq!(sql_change(role_write), Some(ConditionChange::Human), "{role_write}");
    }
    // A Task statement that only reads the role table keeps its own family.
    assert_eq!(
        sql_change("UPDATE task SET title=? WHERE id IN (SELECT task_id FROM task_role_assignment WHERE assignee_id=?)"),
        None
    );
    assert_eq!(
        sql_change("UPDATE review_requirement SET x=? WHERE id=?"),
        None
    );
}

/// Port of `audit_backfill_timing_5k` (measurement, not a correctness check).
/// `cargo test -p db --lib task_condition::tests::measure_ -- --ignored --nocapture`
#[tokio::test]
#[ignore = "measurement"]
async fn measure_backfill_5k() {
    let path = std::env::temp_dir().join(format!("condition-backfill-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let pool = crate::create_sqlite_pool(&format!("sqlite:{}", path.display()))
        .await
        .unwrap();
    crate::run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    ProjectRepo::create(
        &db,
        CreateProject {
            id: "p".into(),
            owner_id: None,
            name: "A".into(),
            primary_repo_id: None,
            updated_at: crate::now_rfc3339(),
            settings: "{}".into(),
            workflow_definition: "{}".into(),
            created_at: crate::now_rfc3339(),
        },
    )
    .await
    .unwrap();
    let template = task(&db, "tmpl").await;
    clone_tasks(&db, &template.id, 5000, "r").await;
    unmigrate(&db).await;
    let meta = json!({"planning_completed_at":"2026-10-01T00:00:00Z","planning_execution_id":"e1","last_execution_failure_at":"2026-10-01T00:00:00Z","last_execution_failure_execution_id":"e2","custom":"z".repeat(1500),"dispatch_disposition":{"capability":"project_capacity","safe_message":"limit"}}).to_string();
    let ann =
        json!({"type":"ci_failed","message":"m".repeat(800),"blocked_at":"2026-10-01T00:00:00Z"})
            .to_string();
    sqlx::query("UPDATE task SET metadata_json=?")
        .bind(&meta)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task SET error_annotation=?, blocked_json=? WHERE (rowid % 5)=0")
        .bind(&ann)
        .bind(json!({"kind":"ci_failed","blocked_by":"system"}).to_string())
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(db.pool())
        .await
        .unwrap();
    let size0 = std::fs::metadata(&path).unwrap().len();
    let started = std::time::Instant::now();
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    migrate(&mut tx).await;
    let in_tx = started.elapsed();
    tx.commit().await.unwrap();
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(db.pool())
        .await
        .unwrap();
    let size1 = std::fs::metadata(&path).unwrap().len();
    let avg: f64 = sqlx::query_scalar("SELECT avg(length(condition_json)) FROM task")
        .fetch_one(db.pool())
        .await
        .unwrap();
    println!("MEASURE 5k backfill in BEGIN IMMEDIATE: {in_tx:?}; file {size0} -> {size1} bytes; avg condition_json {avg:.0} bytes");
    assert_eq!(
        db.task_condition_violations().await.unwrap(),
        Vec::<String>::new()
    );
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

/// Port of `audit_size_scaling` (measurement): cost of the pure mapping as the
/// row's legacy values grow.
#[test]
#[ignore = "measurement"]
fn measure_mapping_size_scaling() {
    for size in [1_000usize, 10_000, 100_000, 1_000_000] {
        let legacy = LegacyConditionInput {
            error_annotation: Some(json!({"type":"ci_failed","message":"m".repeat(size)}).to_string()),
            metadata_json: Some(json!({"custom":"y".repeat(size),"deferred_dispatch":{"not_before":"2026-10-05T12:00:00Z","target_state":"todo","reason":"\u{06A9}".repeat(size / 2)}}).to_string()),
            ..Default::default()
        };
        let started = std::time::Instant::now();
        for _ in 0..20 {
            std::hint::black_box(encode(&map_legacy_condition(&legacy)));
        }
        println!(
            "MEASURE map+encode size={size}: {:?}",
            started.elapsed() / 20
        );
    }
}
