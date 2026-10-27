//! Domain events repository tests.
use super::*;

#[tokio::test]
async fn domain_event_checkpoints_are_ordered_and_deduplicated() {
    let db = sqlite_db().await;
    let make = |id: &str| crate::CreateDomainEvent {
        id: id.into(),
        event_type: "wanted".into(),
        entity_type: "task".into(),
        entity_id: "task".into(),
        actor_type: "system".into(),
        actor_id: None,
        scope_type: "project".into(),
        scope_id: "p".into(),
        correlation_id: id.into(),
        causation_id: None,
        causation_depth: 0,
        dedupe_key: Some(id.into()),
        payload_json: "{}".into(),
        created_at: crate::now_rfc3339(),
    };
    let input = make("checkpoint-a");
    let a = db.append_event(input.clone()).await.unwrap();
    let duplicate = db
        .append_event(CreateDomainEvent {
            id: "duplicate-id".into(),
            ..input.clone()
        })
        .await
        .unwrap();
    assert_eq!(duplicate.id, a.id);
    let conflict = db
        .append_event(CreateDomainEvent {
            id: "conflict-id".into(),
            entity_id: "different-task".into(),
            ..input
        })
        .await;
    assert!(matches!(conflict, Err(DbError::Check(_))));
    let b = db.append_event(make("checkpoint-b")).await.unwrap();
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    db.ensure_domain_event_cursor_in_tx(&mut tx, "checkpoint-test", &crate::now_rfc3339())
        .await
        .unwrap();
    assert!(db
        .validate_event_worker_in_tx(
            &mut tx,
            "checkpoint-test",
            0,
            0,
            b.sequence,
            &crate::EventSubscription::All
        )
        .await
        .is_err());
    db.validate_event_worker_in_tx(
        &mut tx,
        "checkpoint-test",
        0,
        0,
        a.sequence,
        &crate::EventSubscription::All,
    )
    .await
    .unwrap();
    db.advance_domain_event_cursor_in_tx(
        &mut tx,
        "checkpoint-test",
        0,
        a.sequence,
        &crate::now_rfc3339(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        db.get_consumer_cursor("checkpoint-test")
            .await
            .unwrap()
            .unwrap()
            .last_sequence,
        a.sequence
    );
}

#[tokio::test]
async fn domain_event_append_in_tx_rolls_back_with_the_mutation() {
    let db = sqlite_db().await;
    let event = CreateDomainEvent {
        id: "rolled-back-event".to_owned(),
        event_type: "task.transitioned".to_owned(),
        entity_type: "task".to_owned(),
        entity_id: "task-rollback".to_owned(),
        actor_type: "system".to_owned(),
        actor_id: None,
        scope_type: "task".to_owned(),
        scope_id: "task-rollback".to_owned(),
        correlation_id: "corr-rollback".to_owned(),
        causation_id: None,
        causation_depth: 0,
        dedupe_key: Some("rollback-event".to_owned()),
        payload_json: "{}".to_owned(),
        created_at: "2026-08-12T20:00:00Z".to_owned(),
    };
    let mut transaction = crate::begin_immediate(db.pool())
        .await
        .expect("transaction begins");
    DomainEventRepo::append_event_in_tx(&db, &mut transaction, &event)
        .await
        .expect("event appends inside transaction");
    transaction
        .rollback()
        .await
        .expect("transaction rolls back");
    assert!(DomainEventRepo::get_event(&db, &event.id)
        .await
        .expect("event lookup succeeds")
        .is_none());
}

#[tokio::test]
async fn direct_task_status_updates_emit_a_ledger_event_atomically() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, _) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(&db, &project_id, None, "todo".to_owned(), "ledger task").await;
    let task = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task lookup succeeds")
        .expect("task exists");
    let updated = TaskRepo::update_status(
        &db,
        UpdateTaskStatus {
            id: task_id.clone(),
            expected_version: task.version,
            status: "in_progress".to_owned(),
            assignee_id: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            updated_at: "2026-08-12T20:00:00Z".to_owned(),
        },
    )
    .await
    .expect("task status updates");
    assert_eq!(updated.status, "in_progress");
    let events = DomainEventRepo::list_events_after(&db, 0, 100)
        .await
        .expect("task event lists");
    let event = events
        .iter()
        .find(|event| event.event_type == "task.status_changed" && event.entity_id == task_id)
        .expect("task status event exists");
    let payload: serde_json::Value = serde_json::from_str(&event.payload_json).unwrap();
    assert_eq!(payload["from_status"], "todo");
    assert_eq!(payload["to_status"], "in_progress");
}
