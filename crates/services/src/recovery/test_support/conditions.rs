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
const MAIN_ORACLE: &str = include_str!("fixtures/conditions-main-9296fb76.jsonl");

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
                .bind(format!("child-{i:03}")).bind(&task.project_id).bind(&task.id).bind(i).bind(AT).bind(AT).execute(&mut *tx).await.unwrap();
        }
    }
    db.produce_condition_in_tx(&mut tx, &task.id, db::ConditionChange::Full)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

fn normalized(raw: &str, row: &Task) -> String {
    fn times(value: &Value, found: &mut Vec<String>) {
        match value {
            Value::String(text) => {
                if chrono::DateTime::parse_from_rfc3339(text).is_ok() && !text.starts_with("2099-")
                {
                    found.push(text.clone());
                } else if let Ok(nested) = serde_json::from_str::<Value>(text) {
                    if nested.is_object() || nested.is_array() {
                        times(&nested, found);
                    }
                }
            }
            Value::Object(fields) => fields.values().for_each(|value| times(value, found)),
            Value::Array(values) => values.iter().for_each(|value| times(value, found)),
            _ => {}
        }
    }
    let mut timestamps = vec![row.created_at.clone(), row.updated_at.clone()];
    if let Ok(value) = serde_json::from_str(raw) {
        times(&value, &mut timestamps);
    }
    timestamps.sort_by_key(|time| std::cmp::Reverse(time.len()));
    timestamps.dedup();
    let mut output = raw
        .replace(&row.id, "fixture-task")
        .replace(&row.project_id, "fixture-project");
    for time in timestamps {
        let without_precision = if let Some((seconds, fraction)) = time.split_once('.') {
            let timezone = fraction.trim_start_matches(|ch: char| ch.is_ascii_digit());
            format!("{seconds}.<frac>{timezone}")
        } else {
            time.clone()
        };
        let placeholder: String = without_precision
            .chars()
            .map(|ch| if ch.is_ascii_digit() { '0' } else { ch })
            .collect();
        output = output.replace(&time, &placeholder);
    }
    output
}

fn fingerprint(raw: &str) -> Value {
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

async fn attention_incident_digest(
    db: &SqliteDb,
    projector: &AttentionProjector,
    row: &Task,
    phase: &str,
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
    attention.details_json = normalized(&attention.details_json, row);
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
    let mut canonical = row.clone();
    canonical.id = "fixture-task".into();
    canonical.condition = serde_json::from_str(&normalized(&condition, row)).unwrap();
    let offers = crate::task_actions::test_support::condition_offers(db, row).await;
    let event = db::CreateDomainEvent::task_interruption_changed(&canonical);
    let attention_incident_digest = attention_incident_digest(db, projector, row, phase).await;
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
                fingerprint(&normalized(&value.to_string(), row)),
            )
        })
        .collect();
    let actual = json!({"case":format!("{writer}/{shape}/{phase}"), "fields":fingerprints});
    assert_expected(&actual);
}

fn assert_expected(actual: &Value) {
    // Generated only by the identical harness over git archive 9296fb76. The
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

#[tokio::test]
async fn timestamp_normalization_preserves_format_and_fields() {
    let db = sqlite_db().await;
    let (project, _) = seed_project_repo(&db).await;
    let mut row = seed_task(&db, project, "in_progress".into(), None).await;
    row.created_at = "2026-10-25T01:02:03.456Z".into();
    row.updated_at = "2026-10-25T04:05:06+00:00".into();
    let raw = json!({
        "created_at": row.created_at,
        "blocked_at": "2026-10-25T07:08:09.123456789+02:30",
        "updated_at": row.updated_at,
        "nested": r#"{"detected_at":"2026-10-25T10:11:12+02:30"}"#,
        "not_before": "2099-01-01T00:00:00Z",
    });
    let expected = json!({
        "created_at": "0000-00-00T00:00:00.<frac>Z",
        "blocked_at": "0000-00-00T00:00:00.<frac>+00:00",
        "updated_at": "0000-00-00T00:00:00+00:00",
        "nested": r#"{"detected_at":"0000-00-00T00:00:00+00:00"}"#,
        "not_before": "2099-01-01T00:00:00Z",
    });
    assert_eq!(normalized(&raw.to_string(), &row), expected.to_string());
}

#[tokio::test]
async fn timestamp_normalization_ignores_platform_fractional_precision() {
    let db = sqlite_db().await;
    let (project, _) = seed_project_repo(&db).await;
    let row = seed_task(&db, project, "in_progress".into(), None).await;
    for timezone in ["Z", "+02:30", "-04:00"] {
        let microseconds = json!({"detected_at": format!("2026-10-25T01:02:03.123456{timezone}")});
        let nanoseconds =
            json!({"detected_at": format!("2026-10-25T01:02:03.123456789{timezone}")});
        assert_eq!(
            normalized(&microseconds.to_string(), &row),
            normalized(&nanoseconds.to_string(), &row),
            "platform clock precision with timezone {timezone}"
        );
    }
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
