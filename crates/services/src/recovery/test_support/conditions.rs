//! The 16 PR #119 input shapes, shared by both real writer parity tests.
use super::*;
use db::{EnqueueTaskStep, TaskStep, TaskStepRepo};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

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
pub(crate) const ESCAPED_REASON: &str = "é😀\n\"\\\u{7}";
const MAIN_ORACLE: &str = include_str!("fixtures/conditions-main-37309bb9.jsonl");

pub(crate) fn assert_writer_cases(writer: &str, visited: usize) {
    let prefix = format!("{writer}/");
    let expected = MAIN_ORACLE
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|record| record["case"].as_str().unwrap().starts_with(&prefix))
        .count();
    assert_eq!(expected, 48, "main oracle case count for {writer}");
    assert_eq!(visited, expected, "visited case count for {writer}");
}

pub(crate) async fn read(db: &SqliteDb, id: &str) -> Task {
    TaskRepo::get_by_id(db, id, false).await.unwrap().unwrap()
}
pub(crate) async fn claim(db: &SqliteDb, task: &Task) -> TaskStep {
    let id = format!("fixture-step-{}", task.id);
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
        available_at: task.created_at.clone(),
    })
    .await
    .unwrap();
    db.claim_step(
        "condition-acceptance",
        Some(&task.id),
        "2099-01-01T00:00:00+00:00",
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
                .bind(format!("child-{i:03}")).bind(&task.project_id).bind(&task.id).bind(i).bind(AT).bind(AT).execute(&mut *tx).await.unwrap();
        }
    }
    db.produce_condition_in_tx(&mut tx, &task.id, db::ConditionChange::Full)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

pub(crate) fn freeze_clock() -> db::test_clock::Guard {
    assert_eq!(
        tokio::runtime::Handle::current().runtime_flavor(),
        tokio::runtime::RuntimeFlavor::CurrentThread,
        "the parity clock must stay on its test thread"
    );
    db::test_clock::freeze("2026-10-25T00:00:00.123456+00:00".parse().unwrap())
}

pub(crate) fn normalize_ids(raw: &str, replacements: &[(String, String)]) -> String {
    replacements
        .iter()
        .fold(raw.to_owned(), |raw, (from, to)| raw.replace(from, to))
}

pub(crate) fn fingerprint(raw: &str) -> Value {
    json!({"bytes":raw.len(), "sha256":Sha256::digest(raw.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect::<String>()})
}

pub(crate) type AttentionProjector =
    crate::worker_runtime::WorkerRuntime<crate::AttentionService, Option<(String, String)>>;

pub(crate) fn attention_projector(db: &SqliteDb) -> AttentionProjector {
    let db = std::sync::Arc::new(db.clone());
    let service = crate::AttentionService::new(
        std::sync::Arc::clone(&db),
        crate::test_support::noop_wake_turns(),
    );
    AttentionProjector::new(db, std::sync::Arc::new(service))
}

pub(crate) async fn attention_incident_digest(
    db: &SqliteDb,
    projector: &AttentionProjector,
    row: &Task,
    phase: &str,
    replacements: &[(String, String)],
) -> Option<String> {
    projector.run_once(100).await.unwrap();
    let mut event = db::CreateDomainEvent::task_interruption_changed(row);
    // A separate delivery lets the initialized projector observe this exact
    // snapshot even when the writer's own event predates its first cursor.
    event.dedupe_key = Some(format!("condition-parity:{}:{phase}", row.id));
    let event = db::DomainEventRepo::append_event(db, event).await.unwrap();
    // The 400-child fixture also has 400 task.created trigger events. Drain
    // through this receipt, rather than assuming two batches reach it.
    const MAX_BATCHES: usize = 10;
    let mut through = 0;
    for _ in 0..MAX_BATCHES {
        projector.run_once(100).await.unwrap();
        through = db::DomainEventRepo::get_consumer_cursor(
            db,
            crate::attention_service::attention_consumer_name(),
        )
        .await
        .unwrap()
        .expect("Attention worker initialized its cursor")
        .last_sequence;
        if through >= event.sequence {
            break;
        }
    }
    assert!(
        through >= event.sequence,
        "Attention did not reach snapshot event {} (sequence {}, task {}, phase {}) after {} batches of 100; durable cursor stopped at {}",
        event.id, event.sequence, row.id, phase, MAX_BATCHES, through
    );
    let incident: Option<String> = sqlx::query_scalar(
        "SELECT id FROM attention_projection WHERE json_extract(details_json,'$.entity_id')=? AND attention_type IN ('execution_failed','review_needs_owner')",
    ).bind(&row.id).fetch_optional(db.pool()).await.unwrap();
    let Some(id) = incident else {
        assert!(!db::material_blocker(&row.condition).requires_intervention);
        return None;
    };
    let mut attention = db::AttentionRepo::get_attention(db, &id)
        .await
        .unwrap()
        .unwrap();
    if db::material_blocker(&row.condition).requires_intervention {
        assert_eq!(
            attention.source_event_id, event.id,
            "snapshot incident source"
        );
        assert_eq!(
            attention.source_sequence,
            Some(event.sequence),
            "snapshot incident sequence"
        );
    }
    attention.scope_id = attention
        .scope_id
        .replace(&row.project_id, "fixture-project")
        .replace(&row.id, "fixture-task");
    attention.details_json = normalize_ids(&attention.details_json, replacements);
    Some(db::canonical_attention_incident_digest(&attention))
}

pub(crate) async fn assert_main_snapshot(
    db: &SqliteDb,
    projector: &AttentionProjector,
    row: &Task,
    writer: &str,
    shape: &str,
    phase: &str,
) {
    let raw = row.error_annotation.as_deref().unwrap();
    let annotation: db::task_condition::channels::ErrorAnnotation =
        serde_json::from_str(raw).unwrap();
    assert_eq!(
        serde_json::to_string(&annotation).unwrap(),
        raw,
        "strict annotation roundtrip"
    );
    if writer == "executor" {
        let raw = row.blocked_json.as_deref().unwrap();
        let interruption: db::task_condition::channels::Interruption =
            serde_json::from_str(raw).unwrap();
        assert_eq!(
            serde_json::to_string(&interruption).unwrap(),
            raw,
            "strict interruption roundtrip"
        );
    }
    let condition: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
        .bind(&row.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    let replacements = [
        (row.id.clone(), "fixture-task".into()),
        (row.project_id.clone(), "fixture-project".into()),
    ];
    let mut canonical = row.clone();
    canonical.id = "fixture-task".into();
    canonical.condition = serde_json::from_str(&normalize_ids(&condition, &replacements)).unwrap();
    let offers = crate::task_actions::test_support::condition_offers(db, row).await;
    let event = db::CreateDomainEvent::task_interruption_changed(&canonical);
    let attention_incident_digest =
        attention_incident_digest(db, projector, row, phase, &replacements).await;
    let fields = json!({
        "error_annotation":row.error_annotation,
        "blocked_json":row.blocked_json,
        "failed_json":row.failed_json,
        "entry_barrier_json":row.entry_barrier_json,
        "metadata_json":row.metadata_json,
        "condition_json":condition,
        "public":row.condition.public(),
        "read":row.condition.read(),
        "failure_kind":row.condition.read().failure_kind,
        "operations_text":row.condition.read().operator_reason,
        "offers":offers,
        "attention_digest":event.dedupe_key,
        "attention_incident_digest":attention_incident_digest,
    });
    let fingerprints: serde_json::Map<String, Value> = fields
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| {
            (
                key.clone(),
                fingerprint(&normalize_ids(&value.to_string(), &replacements)),
            )
        })
        .collect();
    let actual = json!({"case":format!("{writer}/{shape}/{phase}"), "fields":fingerprints});
    assert_expected(&actual);
}

fn assert_expected(actual: &Value) {
    // Generated only by the identical harness over git archive 37309bb9. The
    // checked-in hashes cover bytes, not a mapper run by the code under test.
    let expected = MAIN_ORACLE
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|record| record["case"] == actual["case"])
        .expect("main oracle contains this case");
    assert_eq!(
        *actual, expected,
        "main byte/projection and canonical Attention parity: {}",
        actual["case"]
    );
}

pub(crate) async fn assert_next_write(
    db: &SqliteDb,
    projector: &AttentionProjector,
    row: &Task,
    writer: &str,
    shape: &str,
    step: &TaskStep,
) {
    TaskRepo::mutate_metadata(
        db,
        &row.id,
        None,
        vec![db::TaskMetadataMutation::Set {
            key: "awaiting_human".into(),
            value: json!(false),
        }],
        AT,
    )
    .await
    .unwrap();
    let next = read(db, &row.id).await;
    assert_main_snapshot(db, projector, &next, writer, shape, "second").await;
    let merged =
        crate::worker_runtime::queue::condition_tests::merge_after_writer(db, &next, step).await;
    assert_main_snapshot(db, projector, &merged, writer, shape, "queue").await;
}
