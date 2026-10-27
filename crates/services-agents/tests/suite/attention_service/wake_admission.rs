//! Attention service wake admission tests.

use super::*;

#[tokio::test]
async fn wake_admission_deduplicates_and_suppresses_recursive_events() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let service = AttentionService::new(
        db.clone(),
        Arc::new(services::WakeTurnConsumer::new(db.clone())),
    );
    assert!(matches!(
        service
            .admit_wake(request(&identity_id, "missing-incident"))
            .await
            .unwrap(),
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::ResolvedIncident
        }
    ));
    let mut recursive = request(&identity_id, "incident-2");
    recursive.reaction_depth = 9;
    assert!(matches!(
        service.admit_wake(recursive).await.unwrap(),
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::ReactionDepthExceeded
        }
    ));

    let mut self_event = request(&identity_id, "incident-3");
    self_event.reaction_depth = 1;
    self_event.caused_by_identity_id = Some(identity_id.clone());
    assert!(matches!(
        service.admit_wake(self_event).await.unwrap(),
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::SelfEvent
        }
    ));

    let mut depth_boundary = request(&identity_id, "incident-depth-boundary");
    depth_boundary.reaction_depth = 8;
    assert!(matches!(
        service.admit_wake(depth_boundary).await.unwrap(),
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::ReactionDepthExceeded
        }
    ));

    let mut retry_exhausted = request(&identity_id, "attention:retry_exhausted:agent_chat:chat-1");
    retry_exhausted.scope_type = "agent_chat".to_owned();
    retry_exhausted.scope_id = "chat-1".to_owned();
    assert!(matches!(
        service.admit_wake(retry_exhausted).await.unwrap(),
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::RecursiveAgentResponse
        }
    ));

    let suppressed_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type = 'agent.wake.suppressed'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert!(suppressed_count >= 3);
}

#[tokio::test]
async fn identical_suppressed_wake_decision_is_deduped() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    let mut request = request(&identity_id, "incident-deduped-suppression");
    request.reaction_depth = 8;

    for _ in 0..2 {
        assert!(matches!(
            service.admit_wake(request.clone()).await.unwrap(),
            WakeAdmissionResult::Suppressed {
                reason: WakeSuppressionReason::ReactionDepthExceeded
            }
        ));
    }
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event
         WHERE event_type = 'agent.wake.suppressed'
           AND json_extract(payload_json, '$.reason') = 'reaction_depth_exceeded'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn retry_exhausted_agent_chat_is_suppressed_before_binding_setup() {
    let db = database().await;
    let project_id = new_uuid_v4();
    let turn_job_id = new_uuid_v4();
    let now = now_rfc3339();
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: project_id.clone(),
            name: "retry-suppression-project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let chat_id: String =
        sqlx::query_scalar("SELECT id FROM agent_chat WHERE project_id = ? AND kind = 'project'")
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    sqlx::query("UPDATE agent_chat SET status = 'ready' WHERE id = ?")
        .bind(&chat_id)
        .execute(db.pool())
        .await
        .unwrap();
    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "agent_chat.turn.failed".to_owned(),
            entity_type: "agent_chat_turn_job".to_owned(),
            entity_id: turn_job_id,
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "agent_chat".to_owned(),
            scope_id: chat_id.clone(),
            correlation_id: "retry-suppression-correlation".to_owned(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some("retry-suppression-source".to_owned()),
            payload_json: r#"{"status":"failed","failure_class":{"kind":"configuration"}}"#
                .to_owned(),
            created_at: now,
        },
    )
    .await
    .unwrap();

    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    service.project_once(100).await.unwrap();
    let (reason, attention_scope): (String, String) = sqlx::query_as(
        "SELECT
            json_extract(payload_json, '$.reason'),
            (SELECT scope_type FROM attention_projection LIMIT 1)
         FROM domain_event
         WHERE event_type = 'agent.wake.suppressed'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(reason, "retry_exhausted_same_chat");
    assert_eq!(attention_scope, "project");
    let budget_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_wake_budget_window")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(budget_count, 0);
}

#[tokio::test]
async fn binding_replacement_cannot_create_a_second_active_incident_lease() {
    let db = database().await;
    let old_identity = new_uuid_v4();
    let new_identity = new_uuid_v4();
    identity(&db, &old_identity).await;
    identity(&db, &new_identity).await;
    let project_id = configured_project(&db, &old_identity, "binding-replacement-project").await;
    let now = now_rfc3339();
    let owner: String = sqlx::query_scalar("SELECT owner_id FROM project WHERE id=?")
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE agent_identity SET owner_id=? WHERE id=?")
        .bind(&owner)
        .bind(&new_identity)
        .execute(db.pool())
        .await
        .unwrap();
    let entity_id = new_uuid_v4();
    let first_source = new_uuid_v4();
    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent {
            id: first_source,
            event_type: "runtime.connection_unavailable".to_owned(),
            entity_type: "agent_session".to_owned(),
            entity_id: entity_id.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: "replacement-correlation-1".to_owned(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some("replacement-source-1".to_owned()),
            payload_json: r#"{"status":"unavailable"}"#.to_owned(),
            created_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    service.project_once(100).await.unwrap();
    let admitted_identity: String = sqlx::query_scalar(
        "SELECT json_extract(payload_json, '$.identity_id') FROM domain_event
         WHERE event_type = 'agent.wake.admitted'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(admitted_identity, old_identity);

    let version: i64 = sqlx::query_scalar(
        "SELECT version FROM project_agent_binding WHERE project_id=? AND state='active'",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    services::AgentChatService::new(db.clone())
        .set_project_binding(services::SetProjectAgentBindingInput {
            actor_user_id: owner,
            project_id: project_id.clone(),
            identity_id: Some(new_identity),
            state: "active".to_owned(),
            autonomy_policy_json: "{}".to_owned(),
            permission_ceiling_json: "{}".to_owned(),
            subscriptions_json: "[]".to_owned(),
            wake_budget: 10,
            expected_version: Some(version),
            replacement_reason: Some("binding replacement test".to_owned()),
        })
        .await
        .unwrap();
    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "runtime.connection_unavailable".to_owned(),
            entity_type: "agent_session".to_owned(),
            entity_id: entity_id.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: "replacement-correlation-2".to_owned(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some("replacement-source-2".to_owned()),
            payload_json: r#"{"status":"unavailable"}"#.to_owned(),
            created_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    service.project_once(100).await.unwrap();
    let duplicate_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event
         WHERE event_type = 'agent.wake.suppressed'
           AND json_extract(payload_json, '$.reason') = 'duplicate_incident'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(duplicate_count, 1);
    let lease_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_wake_lease
         WHERE incident_key = 'attention:runtime_offline:project:' || ? || ':agent_session:' || ?",
    )
    .bind(&project_id)
    .bind(&entity_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(lease_count, 1);
}

#[tokio::test]
async fn resolved_attention_is_suppressed_with_incident_reference() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let source_event_id = new_uuid_v4();
    let entity_id = new_uuid_v4();
    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent {
            id: source_event_id.clone(),
            event_type: "runtime.connection_unavailable".to_owned(),
            entity_type: "agent_session".to_owned(),
            entity_id: entity_id.clone(),
            actor_type: "agent".to_owned(),
            actor_id: Some(identity_id.clone()),
            scope_type: "account".to_owned(),
            scope_id: "account-1".to_owned(),
            correlation_id: "resolved-correlation".to_owned(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some("resolved-source".to_owned()),
            payload_json: r#"{"status":"unavailable"}"#.to_owned(),
            created_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    let incident_key =
        format!("attention:runtime_offline:account:account-1:agent_session:{entity_id}");
    let attention = AttentionRepo::insert_attention(
        &*db,
        CreateAttentionProjection {
            id: new_uuid_v4(),
            attention_type: "runtime_offline".to_owned(),
            scope_type: "account".to_owned(),
            scope_id: "account-1".to_owned(),
            identity_id: Some(identity_id.clone()),
            source_event_id: source_event_id.clone(),
            priority: 95,
            status: "open".to_owned(),
            summary: "Agent runtime is unavailable".to_owned(),
            details_json: "{}".to_owned(),
            dedupe_key: incident_key.clone(),
            occurred_at: now_rfc3339(),
            updated_at: now_rfc3339(),
            acknowledged_at: None,
            snoozed_until: None,
            resolved_at: None,
            updated_by_user_id: None,
            recommended_action: "restore_runtime".to_owned(),
            source_sequence: Some(1),
        },
    )
    .await
    .unwrap();
    AttentionRepo::update_attention_lifecycle(
        &*db,
        UpdateAttentionLifecycle {
            id: attention.id.clone(),
            expected_version: attention.version,
            status: "resolved".to_owned(),
            acknowledged_at: None,
            snoozed_until: Some(None),
            resolved_at: Some(Some(now_rfc3339())),
            updated_by_user_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();

    let result = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    )
    .admit_wake(WakeAdmissionRequest {
        identity_id,
        scope_type: "account".to_owned(),
        scope_id: "account-1".to_owned(),
        incident_key,
        lease_owner: "resolved-worker".to_owned(),
        correlation_id: "resolved-correlation".to_owned(),
        causation_id: Some(source_event_id),
        caused_by_identity_id: None,
        reaction_depth: 0,
        now: "2026-01-01T00:00:00Z".to_owned(),
        lease_seconds: 30,
        cooldown_seconds: 60,
    })
    .await
    .unwrap();
    assert!(matches!(
        result,
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::ResolvedIncident
        }
    ));
    let payload: serde_json::Value = sqlx::query_scalar(
        "SELECT payload_json FROM domain_event
         WHERE event_type = 'agent.wake.suppressed'
           AND json_extract(payload_json, '$.reason') = 'resolved_incident'",
    )
    .fetch_one(db.pool())
    .await
    .map(|value: String| serde_json::from_str(&value).unwrap())
    .unwrap();
    assert_eq!(payload["attention_id"], attention.id);
    assert_eq!(payload["source_event_id"], attention.source_event_id);
}

#[tokio::test]
async fn recovered_task_suppresses_a_stale_interruption_wake_before_budget() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let project_id = configured_project(&db, &identity_id, "stale-recovery-project").await;
    let task = project_task(&db, &project_id, "Already recovered").await;
    let source_event = DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "task.interruption_changed".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: task.id.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "task".to_owned(),
            scope_id: task.id.clone(),
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(new_uuid_v4()),
            payload_json: serde_json::json!({
                "task_id": task.id,
                "task_version": task.version,
                "requires_intervention": true,
                "interruption": {
                    "source": "blocked",
                    "recovery_actions": ["reexecute"]
                }
            })
            .to_string(),
            created_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    let incident_key = format!(
        "attention:execution_failed:project:{project_id}:task:{}",
        task.id
    );
    let attention = AttentionRepo::insert_attention(
        &*db,
        CreateAttentionProjection {
            id: new_uuid_v4(),
            attention_type: "execution_failed".to_owned(),
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            identity_id: Some(identity_id.clone()),
            source_event_id: source_event.id.clone(),
            priority: 85,
            status: "open".to_owned(),
            summary: "Task needs recovery".to_owned(),
            details_json: serde_json::json!({
                "source_event_type": "task.interruption_changed",
                "entity_type": "task",
                "entity_id": task.id,
                "scope_type": "project",
                "scope_id": project_id,
                "recovery": {
                    "requires_intervention": true,
                    "actions": ["reexecute"],
                    "automatic_retry": false
                }
            })
            .to_string(),
            dedupe_key: incident_key.clone(),
            occurred_at: source_event.created_at.clone(),
            updated_at: now_rfc3339(),
            acknowledged_at: None,
            snoozed_until: None,
            resolved_at: None,
            updated_by_user_id: None,
            recommended_action: "inspect_task".to_owned(),
            source_sequence: Some(source_event.sequence),
        },
    )
    .await
    .unwrap();

    let result = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    )
    .admit_wake(WakeAdmissionRequest {
        identity_id,
        scope_type: "project".to_owned(),
        scope_id: project_id.clone(),
        incident_key,
        lease_owner: "stale-recovery-worker".to_owned(),
        correlation_id: source_event.correlation_id,
        causation_id: Some(source_event.id),
        caused_by_identity_id: None,
        reaction_depth: 0,
        now: now_rfc3339(),
        lease_seconds: 30,
        cooldown_seconds: 60,
    })
    .await
    .unwrap();
    assert!(matches!(
        result,
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::ResolvedIncident
        }
    ));
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM attention_projection WHERE id = ?")
            .bind(&attention.id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
        "resolved"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_wake_budget_window WHERE scope_id = ?",
        )
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'agent.wake.admitted' AND scope_id = ?",
        )
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap(),
        0
    );
}
