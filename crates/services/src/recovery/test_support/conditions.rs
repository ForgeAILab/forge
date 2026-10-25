//! Shared two-write acceptance fixtures for the two diagnostic owners.
use super::*;
use db::{
    ConditionIdentity, ConditionReasonOwner, EnqueueTaskStep, TaskCondition, TaskMetadataMutation,
    TaskStep, TaskStepRepo,
};
use serde_json::{json, Value};

// The archive harness changes only this representation expectation; all semantic oracles stay identical.
pub(crate) const CONVERTED: bool = true;
pub(crate) const SHAPES: &[&str] = &[
    "clear",
    "annotation_hold",
    "interruption_hold",
    "interruption_hold_assignment",
    "retry_exhausted",
    "review_budget_exhausted",
    "merge_fix_budget_exhausted",
    "review_gate_failed",
    "workflow_loop",
    "agent_timeout",
    "owner_wait_queued",
    "held_owner_wait_queued",
    "held_owner_wait_queued_deferred",
    "deferred",
    "long_reason",
    "coordination_root",
];
const AT: &str = "2026-10-25T00:00:00Z";

pub(crate) async fn read(db: &SqliteDb, id: &str) -> Task {
    TaskRepo::get_by_id(db, id, false).await.unwrap().unwrap()
}
pub(crate) async fn claim(db: &SqliteDb, task: &Task) -> TaskStep {
    let id = new_uuid_v4();
    db.enqueue_step(&EnqueueTaskStep {
        id: id.clone(),
        task_id: task.id.clone(),
        kind: "command".into(),
        payload_json: "{}".into(),
        causation_step_id: None,
        causation_key: id.clone(),
        chain_id: id,
        chain_position: 1,
        expected_status: task.status.clone(),
        expected_version: task.version,
        expected_epoch: None,
        lane: "fast".into(),
        available_at: AT.into(),
    })
    .await
    .unwrap();
    db.claim_step(
        "condition-acceptance",
        Some(&task.id),
        &db::task_writer::lease_deadline(),
    )
    .await
    .unwrap()
    .unwrap()
}
pub(crate) fn placement(task: &Task) -> db::WorkspacePlacement {
    db::WorkspacePlacement {
        id: "placement".into(),
        workspace_id: "workspace".into(),
        task_id: task.id.clone(),
        agent_id: None,
        owner_kind: db::PlacementOwnerKind::Server,
        daemon_id: None,
        runtime_id: None,
        repo_location_id: "location".into(),
        execution_daemon_id: None,
        workspace_handle: None,
        generation: 7,
        state: db::PlacementState::Failed,
        selected_by: db::PlacementSelectedBy::Scheduler,
        selection_reason: "{}".into(),
        reserved_until: None,
        disconnected_at: None,
        failure_cause: None,
        version: 1,
        created_at: AT.into(),
        updated_at: AT.into(),
    }
}
pub(crate) async fn seed_shape(db: &SqliteDb, task: &Task, shape: &str) {
    let mut annotation = None;
    let mut blocked = None;
    let mut metadata = json!({});
    match shape {
        "annotation_hold" | "interruption_hold" | "interruption_hold_assignment" => {
            annotation = Some(db::ConditionStatement::hold_operator_text(
                "user",
                "owner measurements",
                AT,
            ));
            if shape != "annotation_hold" {
                blocked = Some(json!({"kind":"manual_stop","reason": if shape == "interruption_hold_assignment" { "Assign a coder Agent before retrying, or Cancel. owner measurements" } else { "owner measurements" }, "created_at":AT,
                    "details":{"assignment_required":true,"assignment_guidance":"assign the coder role before retry"}}).to_string());
            }
        }
        "retry_exhausted"
        | "review_budget_exhausted"
        | "merge_fix_budget_exhausted"
        | "review_gate_failed" => {
            blocked = Some(json!({"kind":shape,"reason":"re-plan budget exhausted","created_at":AT,"source":"replan","execution_id":null}).to_string());
        }
        "agent_timeout" => {
            blocked = Some(
                json!({"kind":"agent_timeout","reason":"historical timeout","created_at":AT})
                    .to_string(),
            )
        }
        "workflow_loop" => {
            blocked = Some(
                json!({"reason":"loop detected","blocked_at":AT,"source":"workflow_loop"})
                    .to_string(),
            )
        }
        "owner_wait_queued" => {
            metadata = json!({"owner_wait":{"daemon_id":"waiting-daemon","started_at":AT}, "queued_recovery":{"id":"queued-owner-command"}})
        }
        "held_owner_wait_queued" | "held_owner_wait_queued_deferred" => {
            annotation = Some(db::ConditionStatement::hold_operator_text(
                "user",
                "owner measurements",
                AT,
            ));
            blocked = Some(
                json!({"kind":"manual_stop", "reason":"owner measurements", "created_at":AT})
                    .to_string(),
            );
            metadata = json!({"owner_wait":{"daemon_id":"waiting-daemon", "started_at":AT}, "queued_recovery":{"id":"queued-review-command"}});
            if shape.ends_with("_deferred") {
                metadata["deferred_dispatch"] = json!({"not_before":"2099-01-01T00:00:00Z", "reason":"retry later", "target_state":task.status});
            }
        }
        "deferred" => {
            metadata = json!({"deferred_dispatch":{"not_before":"2099-01-01T00:00:00Z","reason":"retry later","target_state":task.status}})
        }
        "long_reason" => {
            TaskRepo::update_recovery_metadata_if_no_running_execution(
                db,
                &task.id,
                task.version,
                Some(db::ConditionStatement::hold_operator_text(
                    "user",
                    &"h".repeat(200_000),
                    AT,
                )),
                Some(
                    json!({"kind":"manual_stop","reason":"h".repeat(200_000),"created_at":AT})
                        .to_string(),
                ),
                None,
                AT,
                None,
                vec![],
                vec![],
                Some(db::ConditionStatement::Hold {
                    actor: "user".into(),
                    reason: "h".repeat(200_000),
                    at: AT.into(),
                }),
            )
            .await
            .unwrap();
            return;
        }
        "coordination_root" => metadata = json!({"coordination_review_pending":true}),
        "clear" => {}
        _ => panic!("unknown shape {shape}"),
    }
    let mut tx = db.pool().begin().await.unwrap();
    sqlx::query("UPDATE task SET error_annotation=?,blocked_json=?,metadata_json=? WHERE id=?")
        .bind(annotation)
        .bind(blocked)
        .bind(metadata.to_string())
        .bind(&task.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    if shape == "coordination_root" {
        for i in 0..400 {
            sqlx::query("INSERT INTO task(id,project_id,parent_task_id,title,status,subtask_order,created_at,updated_at) VALUES (?,?,?,'child','todo',?,?,?)")
                .bind(new_uuid_v4()).bind(&task.project_id).bind(&task.id).bind(i).bind(AT).bind(AT).execute(&mut *tx).await.unwrap();
        }
    }
    db.produce_condition_in_tx(&mut tx, &task.id, db::ConditionChange::Full)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

// Legacy-side expectations are the writer's literal column updates, independent of the statement/producer.
pub(crate) fn reset_columns(before: &Task, row: &Task, message: &str) -> db::LegacyConditionInput {
    let annotation: Value = serde_json::from_str(row.error_annotation.as_ref().unwrap()).unwrap();
    assert_eq!(
        annotation,
        json!({"type":"workspace_reset_required","blocking_reason":"owner_workspace_reset_required",
        "blocked_by":api_types::Actor::system(api_types::SystemComponent::HeartbeatMonitor).display(),
        "blocked_at":annotation["blocked_at"],"message":message})
    );
    let mut expected = db::LegacyConditionInput::from(before);
    expected.error_annotation = row.error_annotation.clone();
    expected
}
pub(crate) fn candidate_columns(before: &Task, row: &Task) -> db::LegacyConditionInput {
    let annotation: Value = serde_json::from_str(row.error_annotation.as_ref().unwrap()).unwrap();
    let reason = "nothing to review: no implementation run or branch commits were delivered";
    assert_eq!(
        annotation,
        json!({"type":"review_blocked","blocking_reason":"review_candidate_missing",
        "message":format!("{reason}; send back to add work, or cancel the Task"),"blocked_at":annotation["blocked_at"]})
    );
    let mut expected = db::LegacyConditionInput::from(before);
    expected.error_annotation = row.error_annotation.clone();
    expected.blocked_json = Some(json!({"kind":"review_blocked","reason":reason,"created_at":annotation["blocked_at"],"execution_id":null}).to_string());
    expected.failed_json = None;
    expected
}
pub(crate) fn assert_columns(row: &Task, expected: &db::LegacyConditionInput) {
    assert_eq!(&db::LegacyConditionInput::from(row), expected);
}
pub(crate) async fn oracle(db: &SqliteDb, row: &Task) -> TaskCondition {
    let mut c = db.pool().acquire().await.unwrap();
    let facts = db::ConditionFacts::load(&mut c, &row.id).await.unwrap();
    drop(c);
    let mut input = db::LegacyConditionInput::from(row);
    input.facts = Some(facts);
    db::map_legacy_condition(&input)
}
pub(crate) fn ownership_encoding(
    mapped: &TaskCondition,
    identity: ConditionIdentity,
) -> TaskCondition {
    let mut value = serde_json::to_value(mapped.typed()).unwrap();
    value["evidence"]["stated"] = json!(true);
    value["evidence"]["ownership"] = json!([db::ConditionOwnership {
        owner: ConditionReasonOwner::Diagnostic,
        identity,
        reason: None,
        state: None,
        material: None,
    }]);
    serde_json::from_value(value).unwrap()
}
async fn raw(db: &SqliteDb, id: &str) -> String {
    sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
        .bind(id)
        .fetch_one(db.pool())
        .await
        .unwrap()
}
async fn assert_semantics(db: &SqliteDb, row: &Task, mapped: &TaskCondition, shape: &str) {
    assert_eq!(row.condition.typed(), mapped.typed(), "{shape}: typed");
    assert_eq!(row.condition.public(), mapped.public(), "{shape}: public");
    assert_eq!(
        row.condition.read(),
        mapped.read(),
        "{shape}: reader, failure_kind, Operations text, interruption guidance"
    );
    assert_eq!(
        db::material_blocker(&row.condition),
        db::material_blocker(mapped),
        "{shape}: material"
    );
    crate::task_actions::test_support::assert_legacy_offers(db, row, mapped).await;
    assert!(
        !crate::task_dispatcher::condition_is_stale_for_test(row),
        "{shape}: main's dispatcher accepts a synchronized row"
    );
    // Main trusts stated rows; unstated rows compare their legacy copies. Exercise a missed sync too.
    let mut missed = row.clone();
    missed.error_annotation =
        Some("{\"type\":\"workspace_error\",\"blocking_reason\":\"missed sync\"}".into());
    assert_eq!(
        crate::task_dispatcher::condition_is_stale_for_test(&missed),
        !row.condition.evidence().stated,
        "{shape}: main staleness decision"
    );
    db.check_task_condition_invariant(row).await.unwrap();
    let before = raw(db, &row.id).await;
    assert_eq!(
        db.check_task_conditions_of(std::slice::from_ref(&row.id))
            .await
            .unwrap(),
        0,
        "{shape}: no repair"
    );
    assert_eq!(
        db.check_task_conditions_of(std::slice::from_ref(&row.id))
            .await
            .unwrap(),
        0,
        "{shape}: no repair loop"
    );
    assert_eq!(
        raw(db, &row.id).await,
        before,
        "{shape}: check keeps encoding"
    );
}
pub(crate) async fn assert_write_and_next(
    db: &SqliteDb,
    row: &Task,
    identity: ConditionIdentity,
    shape: &str,
    writer: &str,
) {
    let mapped = oracle(db, row).await;
    assert_semantics(db, row, &mapped, shape).await;
    let expected = if CONVERTED {
        ownership_encoding(&mapped, identity)
    } else {
        mapped.clone()
    };
    assert_eq!(
        raw(db, &row.id).await,
        serde_json::to_string(&expected).unwrap(),
        "{writer}/{shape}: exact stored representation"
    );
    assert_eq!(
        row.condition.evidence().stated,
        CONVERTED,
        "{writer}/{shape}: activated writer"
    );
    let after = TaskRepo::mutate_metadata(
        db,
        &row.id,
        None,
        vec![TaskMetadataMutation::Set {
            key: "awaiting_human".into(),
            value: json!(false),
        }],
        AT,
    )
    .await
    .unwrap();
    // mutate_metadata returns its pre-producer Task snapshot on main. Inspect persisted representation by rereading it.
    let reply = after;
    let after = read(db, &row.id).await;
    assert_eq!(
        reply.condition.typed(),
        after.condition.typed(),
        "{shape}: mutation reply retains semantic parity"
    );
    assert_eq!(
        reply.condition.public(),
        after.condition.public(),
        "{shape}: mutation reply public projection"
    );
    let after_oracle = oracle(db, &after).await;
    assert_eq!(
        row.condition.typed(),
        after.condition.typed(),
        "{shape}: two-write stability"
    );
    assert_semantics(db, &after, &after_oracle, shape).await;
    assert_eq!(
        db::CreateDomainEvent::task_interruption_changed(row).dedupe_key,
        db::CreateDomainEvent::task_interruption_changed(&after).dedupe_key,
        "{shape}: two-write event digest"
    );
    // Just like Hold/Release: the next unconverted write remaps the columns, removing stated and ownership.
    assert_eq!(
        raw(db, &row.id).await,
        serde_json::to_string(&after_oracle).unwrap(),
        "{shape}: main's next legacy write bytes"
    );
    assert!(!after.condition.evidence().stated);
    assert!(after.condition.evidence().ownership.is_empty());
}
pub(crate) async fn assert_full_text(db: &SqliteDb, id: &str, message: &str, source: &str) {
    let texts = db.task_diagnostic_texts(id, None, 100).await.unwrap();
    assert!(
        texts.items.iter().any(|text| text.text == message
            && text.source == source
            && text.byte_length == message.len() as i64),
        "full source text retained"
    );
}
pub(crate) async fn clear_own_columns(
    db: &SqliteDb,
    id: &str,
    clear_interruption: bool,
    shape: &str,
) {
    let row = TaskRepo::mutate_metadata(db, id, None, vec![
        TaskMetadataMutation::Set {key:"awaiting_human".into(),value:json!(true)},
        TaskMetadataMutation::Set {key:"queued_recovery".into(),value:json!({"id":"surviving-command"})},
        TaskMetadataMutation::Set {key:"deferred_dispatch".into(),value:json!({"not_before":"2099-01-01T00:00:00Z","reason":"queued retry","target_state":"in_progress"})},
    ], AT).await.unwrap();
    let after = TaskRepo::update(
        db,
        db::UpdateTask {
            id: id.into(),
            expected_version: row.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(None),
            blocked_json: clear_interruption.then_some(None),
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: AT.into(),
        },
    )
    .await
    .unwrap();
    assert!(after.error_annotation.is_none());
    if clear_interruption {
        assert!(after.blocked_json.is_none());
    } else {
        assert_eq!(after.blocked_json, row.blocked_json);
    }
    assert_eq!(
        after.metadata_json, row.metadata_json,
        "{shape}: clear retains all secondary owners and continuation"
    );
    assert!(after.condition.read().human_wait);
    assert!(after.condition.read().retry_recorded);
    assert!(after.condition.read().queued_command);
    assert!(after
        .condition
        .reasons()
        .any(|r| matches!(r, db::ParkReason::HumanDecision { .. })));
    let mapped = oracle(db, &after).await;
    assert_semantics(db, &after, &mapped, shape).await;
}
