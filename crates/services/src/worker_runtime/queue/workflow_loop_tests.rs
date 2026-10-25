use super::tests::{fixture, step};
use super::*;

#[tokio::test]
async fn loop_event_is_broadcast_once_with_durable_audit_envelope() {
    let (db, worker) = fixture().await;
    let mut events = worker.engine.event_bus.subscribe();
    let relay = crate::domain_event_broadcast::DomainEventBroadcastConsumer::new(
        db.clone(),
        worker.engine.event_bus.clone(),
        Some(db.domain_event_head().await.unwrap()),
    );
    let mut input = step("done");
    input.chain_position = CHAIN_LIMIT + 1;
    db.enqueue_step(&input).await.unwrap();
    let task = worker.drain("t").await.unwrap();
    let blocked: serde_json::Value =
        serde_json::from_str(task.blocked_json.as_deref().unwrap()).unwrap();
    assert_eq!(blocked["kind"], "workflow_loop");
    assert_eq!(blocked["created_at"], blocked["blocked_at"]);
    let public = serde_json::to_value(task.condition.public()).unwrap();
    let interruption: api_types::InterruptionMetadata =
        serde_json::from_value(public["details"]["interruption"].clone())
            .expect("visible loop interruption");
    assert_eq!(interruption.reason, blocked["reason"]);
    assert_eq!(
        interruption.kind,
        Some(api_types::FailureKind::WorkflowLoop)
    );
    assert_eq!(interruption.created_at, blocked["blocked_at"]);
    assert_eq!(interruption.source.as_deref(), Some("workflow_loop"));
    assert!(task.condition.reasons().all(|reason| matches!(
        reason,
        db::ParkReason::Failure {
            failure_kind: api_types::FailureKind::WorkflowLoop
        }
    )));
    let mut connection = db.pool().acquire().await.unwrap();
    let mut input = db::LegacyConditionInput::from(&task);
    input.facts = Some(
        db::ConditionFacts::load(&mut connection, "t")
            .await
            .unwrap(),
    );
    drop(connection);
    assert_eq!(
        task.condition.typed(),
        db::map_legacy_condition(&input).typed()
    );
    let mut old_block = blocked.clone();
    old_block.as_object_mut().unwrap().remove("kind");
    old_block.as_object_mut().unwrap().remove("created_at");
    input.blocked_json = Some(old_block.to_string());
    assert_eq!(
        task.condition.typed(),
        db::map_legacy_condition(&input).typed()
    );
    relay.broadcast_once(100).await.unwrap();
    relay.broadcast_once(100).await.unwrap();
    let collected = std::iter::from_fn(|| events.try_recv().ok()).collect::<Vec<_>>();
    assert_eq!(
        collected
            .iter()
            .filter(|event| event.event_type == "transition.loop_detected")
            .count(),
        1
    );
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type='transition.loop_detected'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(count, 1);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(task.error_annotation.as_deref().unwrap())
            .unwrap()["type"],
        "workflow_loop"
    );
    let snapshot = worker
        .task_service
        .task_action_snapshot("t", &Actor::user(api_types::UserActionSource::Test))
        .await
        .unwrap();
    assert_eq!(
        snapshot.condition(),
        Some(api_types::FailureKind::WorkflowLoop)
    );
    let offers = crate::available_actions(&snapshot);
    assert!(offers.iter().any(|offer| matches!(
        offer.action,
        api_types::TaskAction::Restart { .. } | api_types::TaskAction::Approve { .. }
    )));
}

#[tokio::test]
async fn pre_upgrade_loop_survives_first_service_pass_without_new_incident_or_wake() {
    use db::{AttentionRepo, DomainEventRepo};
    const BLOCKED: &str = r#"{"reason":"Automatic workflow loop detected","blocked_at":"2026-10-01T00:00:00Z","source":"workflow_loop"}"#;
    const ANNOTATION: &str = r#"{"type":"workflow_loop","state":"todo","message":"Automatic workflow loop detected","detected_at":"2026-10-01T00:00:00Z","task_step_id":"old-loop-step"}"#;
    let (db, worker) = fixture().await;
    let attention =
        crate::AttentionService::new(db.clone(), crate::test_support::noop_wake_turns());
    attention.project_once(1).await.unwrap();
    sqlx::query("UPDATE task SET blocked_json=?,error_annotation=? WHERE id='t'")
        .bind(BLOCKED)
        .bind(ANNOTATION)
        .execute(db.pool())
        .await
        .unwrap();
    db.check_task_conditions_of(&["t".into()]).await.unwrap();
    let before = TaskRepo::get_by_id(&*db, "t", false)
        .await
        .unwrap()
        .unwrap();
    assert!(before
        .condition
        .evidence()
        .presentation
        .as_ref()
        .unwrap()
        .interruption
        .is_none());
    let typed = before.condition.typed();
    let material = db::material_blocker(&before.condition);
    assert_eq!(
        serde_json::to_value(&material).unwrap(),
        serde_json::json!({
            "requires_intervention":true,
            "interruption":{"source":"blocked","kind":null,"reason":"Automatic workflow loop detected"}
        })
    );
    let mut old_event = db::CreateDomainEvent::task_interruption_changed(&before);
    let mut payload: serde_json::Value = serde_json::from_str(&old_event.payload_json).unwrap();
    payload["condition"]["details"]["interruption"] = serde_json::Value::Null;
    payload["condition"]["details"]["blocked"] = serde_json::json!(false);
    old_event.payload_json = payload.to_string();
    db.append_event(old_event.clone()).await.unwrap();
    attention.project_once(100).await.unwrap();
    let incident: String = sqlx::query_scalar(
        "SELECT id FROM attention_projection WHERE json_extract(details_json,'$.entity_id')='t'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    let prior = db.get_attention(&incident).await.unwrap().unwrap();
    let digest = db::canonical_attention_incident_digest(&prior);
    let head = db.domain_event_head().await.unwrap();
    let dispatcher = crate::TaskDispatcher::new(
        db.clone(),
        worker.engine.event_bus.clone(),
        Arc::new(worker.task_service.clone()),
    );
    dispatcher.startup_reconcile().await.unwrap();
    worker.drain("t").await.unwrap();
    attention.project_once(100).await.unwrap();
    let after = TaskRepo::get_by_id(&*db, "t", false)
        .await
        .unwrap()
        .unwrap();
    let public = serde_json::to_value(after.condition.public()).unwrap();
    let interruption: api_types::InterruptionMetadata =
        serde_json::from_value(public["details"]["interruption"].clone())
            .expect("pre-upgrade loop is visible");
    assert!(
        after.condition.read().interruption.is_none(),
        "internal reader inputs stay unchanged"
    );
    assert_eq!(interruption.reason, "Automatic workflow loop detected");
    assert_eq!(
        interruption.kind,
        Some(api_types::FailureKind::WorkflowLoop)
    );
    assert_eq!(interruption.created_at, "2026-10-01T00:00:00Z");
    assert_eq!(after.condition.typed(), typed);
    assert_eq!(db::material_blocker(&after.condition), material);
    assert_eq!(after.blocked_json.as_deref(), Some(BLOCKED));
    assert_eq!(after.version, before.version);
    assert_eq!(after.updated_at, before.updated_at);
    assert_eq!(
        db::CreateDomainEvent::task_interruption_changed(&after).dedupe_key,
        old_event.dedupe_key
    );
    assert_eq!(
        db.domain_event_head().await.unwrap(),
        head,
        "no new interruption or wake event"
    );
    let current = db.get_attention(&incident).await.unwrap().unwrap();
    assert_eq!(db::canonical_attention_incident_digest(&current), digest);
    assert_eq!(current, prior);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM attention_projection WHERE json_extract(details_json,'$.entity_id')='t'")
        .fetch_one(db.pool()).await.unwrap();
    assert_eq!(count, 1);
}
