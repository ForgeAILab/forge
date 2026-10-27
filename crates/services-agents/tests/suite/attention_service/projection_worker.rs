//! Attention service projection worker tests.

use super::*;

#[tokio::test]
async fn projection_health_uses_buffered_checkpoints_and_idle_polls_do_not_write() {
    let db = database().await;
    for sequence in 0..100 {
        DomainEventRepo::append_event(
            &*db,
            CreateDomainEvent {
                id: new_uuid_v4(),
                event_type: "test.health_progress".to_owned(),
                entity_type: "task".to_owned(),
                entity_id: format!("health-task-{sequence}"),
                actor_type: "system".to_owned(),
                actor_id: None,
                scope_type: "account".to_owned(),
                scope_id: "health-account".to_owned(),
                correlation_id: new_uuid_v4(),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: Some(new_uuid_v4()),
                payload_json: "{}".to_owned(),
                created_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
    }
    let service = Arc::new(AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    ));
    let runtime =
        services::worker_runtime::WorkerRuntime::new(Arc::clone(&db), Arc::clone(&service));
    assert_eq!(
        runtime.run_once(100).await.unwrap(),
        0,
        "ignored events have no per-event write"
    );
    tokio::time::sleep(std::time::Duration::from_millis(5100)).await;
    runtime.run_once(100).await.unwrap(); // buffered scan checkpoint
    let health = service.consumer_health().await.unwrap().unwrap();
    assert!(!health.stale);
    assert_eq!(health.last_sequence, 100);
    sqlx::query("CREATE TRIGGER reject_health_write BEFORE UPDATE ON worker_health BEGIN SELECT RAISE(ABORT, 'unexpected health write'); END")
        .execute(db.pool()).await.unwrap();
    for _ in 0..10 {
        assert_eq!(runtime.run_once(100).await.unwrap(), 0);
    }
    let idle_health = service.consumer_health().await.unwrap().unwrap();
    assert_eq!(idle_health.updated_at, health.updated_at);
    assert!(!health.stale);
}

#[tokio::test]
async fn projection_worker_drains_events_reports_health_and_stops() {
    let db = database().await;
    let project_id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO project (
            id, name, settings, workflow_definition, owner_id, created_at, updated_at
         ) VALUES (?, 'attention-worker-project', '{}', '{}', NULL, ?, ?)",
    )
    .bind(&project_id)
    .bind(now_rfc3339())
    .bind(now_rfc3339())
    .execute(db.pool())
    .await
    .unwrap();
    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "task.validation_failed".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: new_uuid_v4(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: project_id,
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(new_uuid_v4()),
            payload_json: "{}".to_owned(),
            created_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();

    let service = std::sync::Arc::new(AttentionService::new(
        std::sync::Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(std::sync::Arc::clone(&db))),
    ));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle = std::sync::Arc::clone(&service).start(shutdown_rx);
    let mut health = None;
    for _ in 0..100 {
        health = service.consumer_health().await.unwrap();
        if health
            .as_ref()
            .is_some_and(|value| value.last_sequence >= 1)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(health.is_some_and(|value| value.last_sequence >= 1));

    shutdown_tx.send(true).unwrap();
    handle.await.unwrap();
}

#[tokio::test]
async fn semantic_progress_warning_is_typed_deduped_and_resolved_by_progress() {
    let db = database().await;
    let project_id = new_uuid_v4();
    let execution_id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO project (
            id, name, settings, workflow_definition, owner_id, created_at, updated_at
         ) VALUES (?, 'progress-warning-project', '{}', '{}', NULL, ?, ?)",
    )
    .bind(&project_id)
    .bind(now_rfc3339())
    .bind(now_rfc3339())
    .execute(db.pool())
    .await
    .unwrap();

    let append = |db: Arc<SqliteDb>, id: String, event_type: &str, payload: String| {
        let event_type = event_type.to_owned();
        let project_id = project_id.clone();
        let execution_id = execution_id.clone();
        async move {
            DomainEventRepo::append_event(
                &*db,
                CreateDomainEvent {
                    id,
                    event_type,
                    entity_type: "execution".to_owned(),
                    entity_id: execution_id.clone(),
                    actor_type: "system".to_owned(),
                    actor_id: None,
                    scope_type: "project".to_owned(),
                    scope_id: project_id.clone(),
                    correlation_id: new_uuid_v4(),
                    causation_id: None,
                    causation_depth: 0,
                    dedupe_key: None,
                    payload_json: payload,
                    created_at: now_rfc3339(),
                },
            )
            .await
            .unwrap();
        }
    };

    append(
        Arc::clone(&db),
        new_uuid_v4(),
        "execution.progress_warning",
        serde_json::json!({
            "execution_id": execution_id,
            "episode_id": "episode-1",
            "owner_lease_live": true,
        })
        .to_string(),
    )
    .await;
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    service.project_once(100).await.unwrap();

    let first: (String, String, String, String) = sqlx::query_as(
        "SELECT attention_type, dedupe_key, status, source_event_id
         FROM attention_projection WHERE attention_type = 'progress_warning'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(first.0, "progress_warning");
    assert_eq!(
        first.1,
        format!("attention:progress_warning:project:{project_id}:execution:{execution_id}")
    );
    assert_eq!(first.2, "open");

    // A second warning for the same execution episode updates the one
    // projection row instead of creating a second Attention incident.
    let replay_event_id = new_uuid_v4();
    append(
        Arc::clone(&db),
        replay_event_id.clone(),
        "execution.progress_warning",
        serde_json::json!({
            "execution_id": execution_id,
            "episode_id": "episode-1",
            "owner_lease_live": true,
        })
        .to_string(),
    )
    .await;
    service.project_once(100).await.unwrap();
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM attention_projection WHERE attention_type = 'progress_warning'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(count, 1);
    let source_event_id: String = sqlx::query_scalar(
        "SELECT source_event_id FROM attention_projection
         WHERE attention_type = 'progress_warning'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(source_event_id, replay_event_id);

    append(
        Arc::clone(&db),
        new_uuid_v4(),
        "execution.progressed",
        serde_json::json!({
            "execution_id": execution_id,
            "episode_id": "episode-1",
        })
        .to_string(),
    )
    .await;
    service.project_once(100).await.unwrap();
    let status: String = sqlx::query_scalar(
        "SELECT status FROM attention_projection
         WHERE attention_type = 'progress_warning'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(status, "resolved");
}

#[tokio::test]
async fn projected_incident_emits_one_durable_wake_action_and_replay_is_suppressed() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let source_event_id = new_uuid_v4();
    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent {
            id: source_event_id.clone(),
            event_type: "runtime.connection_unavailable".to_owned(),
            entity_type: "agent_session".to_owned(),
            entity_id: new_uuid_v4(),
            actor_type: "agent".to_owned(),
            actor_id: Some(identity_id.clone()),
            scope_type: "account".to_owned(),
            scope_id: "account-1".to_owned(),
            correlation_id: "wake-correlation".to_owned(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some("wake-source-event".to_owned()),
            payload_json: r#"{"status":"unavailable"}"#.to_owned(),
            created_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();

    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    let run = service.project_once(100).await.unwrap();
    assert_eq!(run.processed_events, 1);

    // This fixture has an identity but no current Main binding.  Attention
    // remains durable while wake policy emits an explicit setup trigger; the
    // event is never silently consumed as if a turn had been delivered.
    let setup_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type = 'agent.wake.setup_required'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(setup_count, 1);
    let payload: serde_json::Value = sqlx::query_scalar(
        "SELECT payload_json FROM domain_event
         WHERE event_type = 'agent.wake.setup_required'",
    )
    .fetch_one(db.pool())
    .await
    .map(|value: String| serde_json::from_str(&value).unwrap())
    .unwrap();
    assert_eq!(payload["decision"], "setup_required");
    assert_eq!(payload["reason"], "responder_binding_missing");
    assert_eq!(payload["source_event_id"], source_event_id);
    assert!(payload["attention_id"].as_str().is_some());
    assert!(payload["incident_digest"].as_str().is_some());
}
