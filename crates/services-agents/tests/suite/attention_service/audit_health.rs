//! Attention service audit health tests.

use super::*;
use crate::test_services as services;

#[tokio::test]
async fn audit_skipped_activity_is_not_stale() {
    let db = database().await;
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    service.project_once(1).await.unwrap(); // initialize the worker row
    for _ in 0..5 {
        append_attention_event(&db, "ignored.kind", "t", "p", serde_json::json!({})).await;
    }
    assert_eq!(service.project_once(100).await.unwrap().processed_events, 0);
    tokio::time::sleep(std::time::Duration::from_millis(5100)).await;
    service.project_once(100).await.unwrap(); // lazy scan checkpoint
    let health = service.consumer_health().await.unwrap().unwrap();
    println!(
        "health: checkpoint={} success={:?} stale={}",
        health.last_sequence, health.last_success_at, health.stale
    );
    // Whether or not the lazy checkpoint has been written by a fresh runtime,
    // the events were consumed without ever recording a success time.
    assert!(health.last_success_at.is_none());
    assert!(
        !health.stale,
        "skipped activity must not make the consumer stale"
    );
}

#[tokio::test]
async fn audit_in_window_budget_exhaustion_does_not_publish_autonomy_stall() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let project_id = configured_project(&db, &identity_id, "stall-project").await;
    let task = project_task(&db, &project_id, "Recover actionable interruption").await;
    let now = now_rfc3339();
    let blocked_json = serde_json::json!({
        "reason": "executor failed",
        "created_at": now,
        "kind": "executor_failed",
        "execution_id": new_uuid_v4()
    })
    .to_string();
    let error_annotation = serde_json::json!({
        "type": "executor_failed",
        "recovery_actions": ["reexecute", "cancel_task"]
    })
    .to_string();
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: current.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(error_annotation)),
            blocked_json: Some(Some(blocked_json)),
            failed_json: Some(None),
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    // The hourly window is already full (budget 10, 10 admitted, in window).
    sqlx::query(
        "INSERT INTO agent_wake_budget_window (identity_id, scope_type, scope_id, category, window_started_at, window_seconds, admitted_count, version, updated_at)
         VALUES (?, 'project', ?, 'blocker', ?, 3600, 4, 1, ?)",
    )
    .bind(&identity_id)
    .bind(&project_id)
    .bind(now_rfc3339())
    .bind(now_rfc3339())
    .execute(db.pool())
    .await
    .unwrap();
    let bus = Arc::new(events::EventBus::new(64));
    let mut rx = bus.subscribe();
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    )
    .with_event_bus(Arc::clone(&bus));
    service.project_once(100).await.unwrap();
    let suppressed: Vec<String> = sqlx::query_scalar(
        "SELECT payload_json FROM domain_event WHERE event_type = 'agent.wake.suppressed'",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    println!("AUDIT suppressed decisions: {suppressed:?}");
    let mut stalls = 0;
    while let Ok(event) = rx.try_recv() {
        println!("AUDIT bus event: {}", event.event_type);
        if event.event_type == "project.autonomy_stalled" {
            stalls += 1;
        }
    }
    println!("AUDIT stalls published for in-window exhaustion: {stalls}");
    assert!(suppressed.iter().any(|p| p.contains("budget_exhausted")));
    assert_eq!(
        stalls, 0,
        "full hourly window must not send a stall notification"
    );
}

#[tokio::test]
async fn audit_idle_and_new_upgrade_health_are_not_stale_and_stalled_backlog_is() {
    let db = database().await;
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    service.project_once(1).await.unwrap();
    sqlx::query("UPDATE worker_health SET last_success_at = '2000-01-01T00:00:00Z' WHERE worker_name = 'attention_projection'").execute(db.pool()).await.unwrap();
    assert!(!service.consumer_health().await.unwrap().unwrap().stale);
    append_attention_event_at(
        &db,
        "ignored.kind",
        "t",
        "p",
        serde_json::json!({}),
        "2000-01-01T00:00:00Z",
    )
    .await;
    sqlx::query("UPDATE event_consumer_cursor SET updated_at = '2000-01-01T00:00:00Z' WHERE consumer_name = 'attention_projection'").execute(db.pool()).await.unwrap();
    assert!(
        !service.consumer_health().await.unwrap().unwrap().stale,
        "initialization grace applies to upgraded checkpoints"
    );
    sqlx::query("UPDATE worker_health SET created_at = '2000-01-01T00:00:00Z' WHERE worker_name = 'attention_projection'").execute(db.pool()).await.unwrap();
    assert!(
        service.consumer_health().await.unwrap().unwrap().stale,
        "old pending event plus old checkpoint is stalled"
    );
    sqlx::query("UPDATE event_consumer_cursor SET last_sequence = (SELECT MAX(sequence) FROM domain_event) WHERE consumer_name = 'attention_projection'").execute(db.pool()).await.unwrap();
    assert!(
        !service.consumer_health().await.unwrap().unwrap().stale,
        "caught-up idle consumer is healthy even with old success timestamps"
    );
}

#[tokio::test]
async fn audit_health_reports_worker_error_kind_and_message() {
    use services::worker_runtime::{HealthErrorKind, WorkItem, WorkerHealth};

    let db = database().await;
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    service.project_once(1).await.unwrap();
    let health = WorkerHealth::new(Arc::clone(&db), "attention_projection");
    for kind in ["failure", "transient", "terminal"] {
        let message = format!("diagnostic for {kind}");
        health
            .report_classified_error(HealthErrorKind::Tick, kind, &message)
            .await
            .unwrap();
        let view = service.consumer_health().await.unwrap().unwrap();
        assert_eq!(view.last_error_code.as_deref(), Some(kind));
        assert_eq!(view.last_error_message.as_deref(), Some(message.as_str()));
        assert!(
            !view.stale,
            "an idle worker is not stale even after an error"
        );
    }
    health
        .clear_error_if_set(HealthErrorKind::Tick)
        .await
        .unwrap();
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    health
        .failure_in_tx(
            &mut tx,
            WorkItem {
                source_key: "test-terminal",
                item_type: "test",
            },
            db::RetryPolicy::default(),
            "deterministic projection rejection",
            true,
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let view = service.consumer_health().await.unwrap().unwrap();
    assert_eq!(view.last_error_code.as_deref(), Some("terminal"));
    assert_eq!(
        view.last_error_message.as_deref(),
        Some("deterministic projection rejection")
    );
}

#[tokio::test]
async fn audit_zero_budget_chat_stall_uses_project_scope() {
    let db = database().await;
    let id = new_uuid_v4();
    identity(&db, &id).await;
    let project = configured_project(&db, &id, "chat-budget").await;
    let chat = services::AgentChatService::new(Arc::clone(&db))
        .ensure_project_chat(&project)
        .await
        .unwrap();
    sqlx::query("UPDATE agent_chat SET status = 'ready' WHERE id = ?")
        .bind(&chat.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE project_agent_binding SET wake_budget = 0 WHERE project_id = ?")
        .bind(&project)
        .execute(db.pool())
        .await
        .unwrap();
    append_attention_event(
        &db,
        "validation.failed",
        "task-budget",
        &project,
        serde_json::json!({}),
    )
    .await;
    let source: String = sqlx::query_scalar("SELECT id FROM domain_event WHERE entity_id = 'task-budget' ORDER BY sequence DESC LIMIT 1").fetch_one(db.pool()).await.unwrap();
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO attention_projection (id, attention_type, scope_type, scope_id, identity_id, source_event_id, priority, status, summary, details_json, dedupe_key, occurred_at, updated_at) VALUES (?, 'validation_failed', 'project', ?, ?, ?, 1, 'open', 'Needs work', '{}', 'chat-budget-incident', ?, ?)")
        .bind(new_uuid_v4()).bind(&project).bind(&id).bind(source).bind(now_rfc3339()).bind(now_rfc3339()).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let bus = Arc::new(events::EventBus::new(16));
    let mut rx = bus.subscribe();
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    )
    .with_event_bus(bus);
    let result = service
        .admit_wake(services::WakeAdmissionRequest {
            identity_id: id,
            scope_type: "agent_chat".into(),
            scope_id: chat.id,
            incident_key: "chat-budget-incident".into(),
            lease_owner: "test".into(),
            correlation_id: new_uuid_v4(),
            causation_id: None,
            caused_by_identity_id: None,
            reaction_depth: 0,
            now: now_rfc3339(),
            lease_seconds: 60,
            cooldown_seconds: 300,
        })
        .await
        .unwrap();
    assert!(matches!(
        result,
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::BudgetExhausted
        }
    ));
    let requests: Vec<(String, String)> = sqlx::query_as("SELECT scope_id, payload_json FROM domain_event WHERE event_type = 'notification.requested'").fetch_all(db.pool()).await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].0, project);
    let request: serde_json::Value = serde_json::from_str(&requests[0].1).unwrap();
    assert_eq!(request["event_type"], "project.autonomy_stalled");
    assert_eq!(request["project_id"], project);
    // Delivery reads only the committed request, independently of the live hint.
    let notifications = Arc::new(services::NotificationService::new(
        Arc::clone(&db),
        Arc::new(events::EventBus::new(1)),
    ));
    services::worker_runtime::WorkerRuntime::new(Arc::clone(&db), notifications)
        .run_once(100)
        .await
        .unwrap();
    let inbox_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM notification WHERE project_id = ? AND event_type = 'project.autonomy_stalled'").bind(&project).fetch_one(db.pool()).await.unwrap();
    assert_eq!(inbox_count, 1);
    let event = rx.try_recv().unwrap();
    assert!(
        matches!(event.context, events::EventContext::ProjectAutonomyStalled { project_id, .. } if project_id == project)
    );
}

#[tokio::test]
async fn audit_admitted_source_replay_is_a_noop() {
    let db = database().await;
    let id = new_uuid_v4();
    identity(&db, &id).await;
    let project = configured_project(&db, &id, "replay-noop").await;
    append_attention_event(
        &db,
        "validation.failed",
        "task-replay",
        &project,
        serde_json::json!({}),
    )
    .await;
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    service.project_once(100).await.unwrap();
    let before: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT COUNT(*) FROM domain_event WHERE event_type LIKE 'agent.wake.%'), (SELECT version FROM attention_projection LIMIT 1), (SELECT admitted_count FROM agent_wake_budget_window LIMIT 1)").fetch_one(db.pool()).await.unwrap();
    sqlx::query("UPDATE event_consumer_cursor SET last_sequence = 0 WHERE consumer_name = 'attention_projection'").execute(db.pool()).await.unwrap();
    service.project_once(100).await.unwrap();
    let after: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT COUNT(*) FROM domain_event WHERE event_type LIKE 'agent.wake.%'), (SELECT version FROM attention_projection LIMIT 1), (SELECT admitted_count FROM agent_wake_budget_window LIMIT 1)").fetch_one(db.pool()).await.unwrap();
    assert_eq!(
        before, after,
        "already admitted source does not increment budget/version or append suppression"
    );
}

#[tokio::test]
async fn audit_lost_wake_lease_cas_suppresses_without_charging_budget() {
    let db = database().await;
    let id = new_uuid_v4();
    identity(&db, &id).await;
    let project = configured_project(&db, &id, "lease-cas").await;
    let now = now_rfc3339();
    let event = db
        .append_event(CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "test.incident".to_owned(),
            entity_type: "project".to_owned(),
            entity_id: project.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: project.clone(),
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: None,
            payload_json: "{}".to_owned(),
            created_at: now.clone(),
        })
        .await
        .unwrap();
    db.insert_attention(CreateAttentionProjection {
        id: new_uuid_v4(),
        attention_type: "validation_failed".to_owned(),
        scope_type: "project".to_owned(),
        scope_id: project.clone(),
        identity_id: Some(id.clone()),
        source_event_id: event.id,
        priority: 80,
        status: "open".to_owned(),
        summary: "CAS loss".to_owned(),
        details_json: "{}".to_owned(),
        dedupe_key: "cas-loss".to_owned(),
        occurred_at: now.clone(),
        updated_at: now,
        acknowledged_at: None,
        snoozed_until: None,
        resolved_at: None,
        updated_by_user_id: None,
        recommended_action: "inspect".to_owned(),
        source_sequence: Some(event.sequence),
    })
    .await
    .unwrap();
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    sqlx::raw_sql("CREATE TRIGGER lose_wake_lease BEFORE INSERT ON agent_wake_lease BEGIN SELECT RAISE(IGNORE); END;").execute(db.pool()).await.unwrap();
    let mut input = request(&id, "cas-loss");
    input.scope_type = "project".to_owned();
    input.scope_id = project;
    let result = service.admit_wake(input).await.unwrap();
    assert!(matches!(
        result,
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::DuplicateIncident
        }
    ));
    let budgets: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_wake_budget_window")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(budgets, 0);
}

#[tokio::test]
async fn conflict_hotspots_project_per_path_reopen_and_keep_wake_rules() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let project_id = configured_project(&db, &identity_id, "conflict-hotspots").await;
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    let mut first_id: Option<String> = None;
    for (path, count) in [("src/共有.rs", 3), ("src/共有.rs", 4), ("src/other.rs", 3)] {
        if count == 4 {
            let current = db
                .get_attention(first_id.as_ref().unwrap())
                .await
                .unwrap()
                .unwrap();
            db.update_attention_lifecycle(UpdateAttentionLifecycle {
                id: current.id,
                expected_version: current.version,
                status: "resolved".into(),
                acknowledged_at: None,
                snoozed_until: Some(None),
                resolved_at: Some(Some(now_rfc3339())),
                updated_by_user_id: None,
                updated_at: now_rfc3339(),
            })
            .await
            .unwrap();
        }
        db.append_event(CreateDomainEvent {
            id: new_uuid_v4(), event_type: "project.conflict_hotspot.detected".into(),
            entity_type: "project".into(), entity_id: project_id.clone(),
            actor_type: "system".into(), actor_id: Some("conflict-hotspots".into()),
            scope_type: "project".into(), scope_id: project_id.clone(),
            correlation_id: new_uuid_v4(), causation_id: None, causation_depth: 0,
            dedupe_key: Some(new_uuid_v4()),
            payload_json: serde_json::json!({"project_id": project_id, "path": path,
                "task_ids": ["task-3", "task-2", "task-1"], "handoff_count": count, "window_days": 7}).to_string(),
            created_at: now_rfc3339(),
        }).await.unwrap();
        service.project_once(100).await.unwrap();
        let rows = sqlx::query_as::<_, (String, String, String, String, String, i64, String, i64)>(
            "SELECT id, scope_type, scope_id, summary, recommended_action, priority, details_json, version
             FROM attention_projection WHERE attention_type = 'conflict_hotspot' ORDER BY source_sequence",
        ).fetch_all(db.pool()).await.unwrap();
        let item = rows.last().unwrap();
        assert_eq!(item.1, "project");
        assert_eq!(item.2, project_id);
        assert_eq!(
            item.3,
            format!("{path} conflicted in {count} Tasks this week")
        );
        assert_eq!(item.4, "split_hotspot");
        assert_eq!(item.5, 60);
        let details: serde_json::Value = serde_json::from_str(&item.6).unwrap();
        assert_eq!(details["conflict_hotspot"]["path"], path);
        if path == "src/共有.rs" {
            assert_eq!(rows.len(), 1);
            match &first_id {
                None => first_id = Some(item.0.clone()),
                Some(first) => {
                    assert_eq!(&item.0, first);
                    assert!(item.7 > 1);
                }
            }
        } else {
            assert_eq!(rows.len(), 2);
            assert_ne!(Some(&item.0), first_id.as_ref());
        }
        let projection = db.get_attention(&item.0).await.unwrap().unwrap();
        assert_eq!(projection.status, "open");
        assert!(projection.resolved_at.is_none());
        assert_eq!(
            services::attention_service::attention_item(projection)
                .unwrap()
                .category,
            api_types::AttentionCategory::ConflictHotspot
        );
    }
    let admitted: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type = 'agent.wake.admitted'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(admitted, 2); // A later episode still obeys the same path's live lease/cooldown.
    let ineligible: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_event WHERE event_type = 'agent.wake.suppressed' AND json_extract(payload_json, '$.reason') = 'ineligible_scope'")
        .fetch_one(db.pool()).await.unwrap();
    assert_eq!(ineligible, 0);
}
