use std::sync::Arc;

use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentRepo, AgentStatus,
    AttentionRepo, CreateAgentIdentity, CreateAgentProfile, CreateAttentionProjection,
    CreateDomainEvent, DomainEventRepo, SqliteDb, UpdateAttentionLifecycle,
};
use services::{
    AttentionService, WakeAdmissionRequest, WakeAdmissionResult, WakeSuppressionReason,
};
use tokio::sync::watch;

async fn database() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    Arc::new(SqliteDb::new(pool))
}

async fn identity(db: &SqliteDb, id: &str) {
    let now = now_rfc3339();
    AgentRepo::create_identity_with_profile(
        db,
        CreateAgentIdentity {
            id: id.to_owned(),
            name: "wake-test".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "account".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: new_uuid_v4(),
            identity_id: id.to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("test".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
}

fn request(identity_id: &str, incident_key: &str) -> WakeAdmissionRequest {
    WakeAdmissionRequest {
        identity_id: identity_id.to_owned(),
        scope_type: "account".to_owned(),
        scope_id: "account-1".to_owned(),
        incident_key: incident_key.to_owned(),
        lease_owner: "worker-1".to_owned(),
        correlation_id: "correlation-1".to_owned(),
        causation_id: Some("event-1".to_owned()),
        caused_by_identity_id: None,
        reaction_depth: 0,
        now: "2026-01-01T00:00:00Z".to_owned(),
        lease_seconds: 30,
        cooldown_seconds: 60,
    }
}

#[tokio::test]
async fn wake_admission_deduplicates_and_suppresses_recursive_events() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let service = AttentionService::new(Arc::clone(&db));

    let first = service
        .admit_wake(request(&identity_id, "incident-1"))
        .await
        .unwrap();
    assert!(matches!(first, WakeAdmissionResult::Admitted { .. }));

    let duplicate = service
        .admit_wake(request(&identity_id, "incident-1"))
        .await
        .unwrap();
    assert!(matches!(
        duplicate,
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::DuplicateIncident
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
async fn wake_policy_persists_cooldown_budget_and_global_identity_suppression() {
    let db = database().await;
    let first_identity = new_uuid_v4();
    let replacement_identity = new_uuid_v4();
    identity(&db, &first_identity).await;
    identity(&db, &replacement_identity).await;
    let service = AttentionService::new(Arc::clone(&db));

    let mut first = request(&first_identity, "incident-cooldown");
    first.causation_id = Some("source-cooldown-1".to_owned());
    assert!(matches!(
        service.admit_wake(first.clone()).await.unwrap(),
        WakeAdmissionResult::Admitted { .. }
    ));

    let mut cooldown = first.clone();
    cooldown.lease_owner = "replacement-worker".to_owned();
    cooldown.causation_id = Some("source-cooldown-2".to_owned());
    cooldown.now = "2026-01-01T00:00:31Z".to_owned();
    assert!(matches!(
        service.admit_wake(cooldown).await.unwrap(),
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::Cooldown
        }
    ));

    // Once the cooldown expires, replaying the same source decision returns
    // its original admission metadata without charging the wake budget a
    // second time.
    let mut replay = first.clone();
    replay.now = "2026-01-01T00:10:00Z".to_owned();
    assert!(matches!(
        service.admit_wake(replay).await.unwrap(),
        WakeAdmissionResult::Admitted { .. }
    ));
    let replay_count: i64 = sqlx::query_scalar(
        "SELECT admitted_count FROM agent_wake_budget_window
         WHERE identity_id = ? AND scope_type = 'account' AND scope_id = 'account-1'",
    )
    .bind(&first_identity)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(replay_count, 1);

    let mut replacement = request(&replacement_identity, "incident-global");
    replacement.causation_id = Some("source-global-1".to_owned());
    assert!(matches!(
        service.admit_wake(replacement.clone()).await.unwrap(),
        WakeAdmissionResult::Admitted { .. }
    ));
    replacement.now = "2026-01-01T00:00:10Z".to_owned();
    replacement.identity_id = first_identity.clone();
    replacement.lease_owner = "old-binding-worker".to_owned();
    replacement.causation_id = Some("source-global-2".to_owned());
    assert!(matches!(
        service.admit_wake(replacement).await.unwrap(),
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::DuplicateIncident
        }
    ));

    // Saturating the persisted window must not be counted again by a
    // suppression decision.
    sqlx::query(
        "INSERT INTO agent_wake_budget_window (
             identity_id, scope_type, scope_id, window_started_at,
             window_seconds, admitted_count, version, updated_at
         ) VALUES (?, 'account', 'account-1', ?, 3600, 10, 1, ?)
         ON CONFLICT(identity_id, scope_type, scope_id) DO UPDATE SET
             window_started_at = excluded.window_started_at,
             admitted_count = excluded.admitted_count,
             version = agent_wake_budget_window.version + 1,
             updated_at = excluded.updated_at",
    )
    .bind(&first_identity)
    .bind("2026-01-01T00:00:00Z")
    .bind("2026-01-01T00:00:00Z")
    .execute(db.pool())
    .await
    .unwrap();
    let mut budget = request(&first_identity, "incident-budget");
    budget.now = "2026-01-01T00:00:30Z".to_owned();
    budget.causation_id = Some("source-budget".to_owned());
    assert!(matches!(
        service.admit_wake(budget).await.unwrap(),
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::BudgetExhausted
        }
    ));
    let admitted_count: i64 = sqlx::query_scalar(
        "SELECT admitted_count FROM agent_wake_budget_window
         WHERE identity_id = ? AND scope_type = 'account' AND scope_id = 'account-1'",
    )
    .bind(&first_identity)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(admitted_count, 10);

    let decisions: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event
         WHERE event_type = 'agent.wake.suppressed'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert!(decisions >= 2);
}

#[tokio::test]
async fn identical_suppressed_wake_decision_is_deduped() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let service = AttentionService::new(Arc::clone(&db));
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
    sqlx::query(
        "INSERT INTO project (
             id, name, settings, workflow_definition, owner_id, created_at, updated_at
         ) VALUES (?, 'retry-suppression-project', '{}', '{}', NULL, ?, ?)",
    )
    .bind(&project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
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
            payload_json: r#"{"status":"failed"}"#.to_owned(),
            created_at: now,
        },
    )
    .await
    .unwrap();

    AttentionService::new(Arc::clone(&db))
        .project_once(100)
        .await
        .unwrap();
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
    let old_profile: String =
        sqlx::query_scalar("SELECT selected_profile_id FROM agent_identity WHERE id = ?")
            .bind(&old_identity)
            .fetch_one(db.pool())
            .await
            .unwrap();
    let new_profile: String =
        sqlx::query_scalar("SELECT selected_profile_id FROM agent_identity WHERE id = ?")
            .bind(&new_identity)
            .fetch_one(db.pool())
            .await
            .unwrap();
    let project_id = new_uuid_v4();
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO project (
             id, name, settings, workflow_definition, owner_id, created_at, updated_at
         ) VALUES (?, 'binding-replacement-project', '{}', '{}', NULL, ?, ?)",
    )
    .bind(&project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .unwrap();
    let binding_id: String = sqlx::query_scalar(
        "SELECT id FROM project_agent_binding
         WHERE project_id = ? AND state = 'agent_setup_required'",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE project_agent_binding
         SET identity_id = ?, profile_id = ?, state = 'active', wake_budget = 10,
             version = version + 1, updated_at = ?
         WHERE id = ?",
    )
    .bind(&old_identity)
    .bind(&old_profile)
    .bind(&now)
    .bind(&binding_id)
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
    let service = AttentionService::new(Arc::clone(&db));
    service.project_once(100).await.unwrap();
    let admitted_identity: String = sqlx::query_scalar(
        "SELECT json_extract(payload_json, '$.identity_id') FROM domain_event
         WHERE event_type = 'agent.wake.admitted'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(admitted_identity, old_identity);

    sqlx::query(
        "UPDATE project_agent_binding
         SET state = 'replaced', version = version + 1, updated_at = ?
         WHERE id = ? AND state = 'active'",
    )
    .bind(&now)
    .bind(&binding_id)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO project_agent_binding (
             id, project_id, identity_id, profile_id, state,
             autonomy_policy_json, permission_ceiling_json, subscriptions_json,
             wake_budget, version, created_at, updated_at
         ) VALUES (?, ?, ?, ?, 'active', '{}', '{}', '[]', 10, 1, ?, ?)",
    )
    .bind(new_uuid_v4())
    .bind(&project_id)
    .bind(&new_identity)
    .bind(&new_profile)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
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

    let result = AttentionService::new(Arc::clone(&db))
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

    let service = std::sync::Arc::new(AttentionService::new(std::sync::Arc::clone(&db)));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle = std::sync::Arc::clone(&service).start(shutdown_rx);
    let mut health = None;
    for _ in 0..100 {
        health = AttentionRepo::get_attention_consumer_health(&*db, "attention_projection")
            .await
            .unwrap();
        if health
            .as_ref()
            .is_some_and(|value| value.processed_events >= 1)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(health.is_some_and(|value| value.processed_events >= 1));

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
    let service = AttentionService::new(Arc::clone(&db));
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

    let service = AttentionService::new(Arc::clone(&db));
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

#[tokio::test]
async fn completed_task_wakes_project_agent_until_readiness_is_reconciled() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let profile_id: String =
        sqlx::query_scalar("SELECT selected_profile_id FROM agent_identity WHERE id = ?")
            .bind(&identity_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    let project_id = new_uuid_v4();
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO project (
            id, name, settings, workflow_definition, owner_id, created_at, updated_at
         ) VALUES (?, 'delivery-followup-project', '{}', '{}', NULL, ?, ?)",
    )
    .bind(&project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE project_agent_binding
         SET identity_id = ?, profile_id = ?, state = 'active', wake_budget = 10,
             version = version + 1, updated_at = ?
         WHERE project_id = ? AND state = 'agent_setup_required'",
    )
    .bind(&identity_id)
    .bind(&profile_id)
    .bind(&now)
    .bind(&project_id)
    .execute(db.pool())
    .await
    .unwrap();

    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "task.transitioned".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: new_uuid_v4(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(new_uuid_v4()),
            payload_json: r#"{"to_state":"done"}"#.to_owned(),
            created_at: now.clone(),
        },
    )
    .await
    .unwrap();

    let service = AttentionService::new(Arc::clone(&db));
    service.project_once(100).await.unwrap();
    let attention: (String, String, String, Option<String>) = sqlx::query_as(
        "SELECT attention_type, status, recommended_action, identity_id
         FROM attention_projection WHERE scope_type = 'project' AND scope_id = ?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(attention.0, "delivery_followup");
    assert_eq!(attention.1, "open");
    assert_eq!(attention.2, "reconcile_delivery");
    assert_eq!(attention.3.as_deref(), Some(identity_id.as_str()));
    let admitted_identity: String = sqlx::query_scalar(
        "SELECT json_extract(payload_json, '$.identity_id')
         FROM domain_event WHERE event_type = 'agent.wake.admitted'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(admitted_identity, identity_id);

    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "milestone.readiness.evaluated".to_owned(),
            entity_type: "project_milestone".to_owned(),
            entity_id: new_uuid_v4(),
            actor_type: "agent".to_owned(),
            actor_id: Some(identity_id),
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 1,
            dedupe_key: Some(new_uuid_v4()),
            payload_json: r#"{"result":"blocked"}"#.to_owned(),
            created_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    service.project_once(100).await.unwrap();
    let resolved: String = sqlx::query_scalar(
        "SELECT status FROM attention_projection
         WHERE attention_type = 'delivery_followup' AND scope_id = ?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(resolved, "resolved");
}
