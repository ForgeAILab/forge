//! Wake turn consumer responder admission tests.

use super::*;

#[tokio::test]
async fn user_handoff_and_retry_turns_freeze_admitted_profile_after_edit_and_rebinding() {
    let fixture = chat_turn_fixture().await;
    let current_profile = select_profile(
        &fixture.db,
        &fixture.identity_id,
        &new_uuid_v4(),
        "profile-at-admission",
    )
    .await;
    let chats = AgentChatService::new(Arc::clone(&fixture.db));

    let user_turn = chats
        .send_message(SendAgentChatMessageInput {
            actor_user_id: fixture.account_id.clone(),
            chat_id: fixture.chat_id.clone(),
            content: "user trigger".to_owned(),
            dedupe_key: Some("characterization:user".to_owned()),
        })
        .await
        .unwrap()
        .turn_job;
    assert_eq!(
        user_turn.responder_identity_id.as_deref(),
        Some(fixture.identity_id.as_str())
    );
    assert_eq!(
        user_turn.profile_id.as_deref(),
        Some(current_profile.as_str())
    );

    let main_chat = AgentChatRepo::get_main_chat(&*fixture.db, &fixture.account_id)
        .await
        .unwrap()
        .unwrap();
    let handoff_turn = chats
        .create_handoff(CreateAgentHandoffInput {
            actor_user_id: fixture.account_id.clone(),
            source_chat_id: main_chat.id,
            source_message_id: None,
            source_turn_job_id: None,
            target_project_id: fixture.project_id.clone(),
            content: "handoff trigger".to_owned(),
            source_revisions_json: "{}".to_owned(),
            dedupe_key: "characterization:handoff".to_owned(),
        })
        .await
        .unwrap()
        .target_turn_job;
    assert_eq!(
        handoff_turn.responder_identity_id.as_deref(),
        Some(fixture.identity_id.as_str())
    );
    assert_eq!(
        handoff_turn.profile_id.as_deref(),
        Some(current_profile.as_str())
    );

    let wake_consumer = WakeTurnConsumer::new(Arc::clone(&fixture.db));
    wake_consumer.run_once(100).await.unwrap();
    let incident_key = format!("attention:provenance:project:{}", fixture.project_id);
    let source_event = new_uuid_v4();
    append_event(
        &fixture.db,
        CreateDomainEvent {
            id: source_event.clone(),
            event_type: "execution.failed".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: new_uuid_v4(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: fixture.project_id.clone(),
            correlation_id: source_event.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!("provenance-source:{source_event}")),
            payload_json: "{}".to_owned(),
            created_at: now_rfc3339(),
        },
    )
    .await;
    sqlx::query(
        "INSERT INTO attention_projection (
            id, attention_type, scope_type, scope_id, identity_id, source_event_id,
            priority, status, summary, details_json, dedupe_key, occurred_at,
            updated_at, recommended_action
         ) VALUES (?, 'execution_failed', 'project', ?, ?, ?, 85, 'open',
                   'Provenance incident', ?, ?, ?, ?, 'inspect_run')",
    )
    .bind(new_uuid_v4())
    .bind(&fixture.project_id)
    .bind(&fixture.identity_id)
    .bind(&source_event)
    .bind(
        serde_json::json!({
            "scope_type": "project",
            "scope_id": fixture.project_id,
        })
        .to_string(),
    )
    .bind(&incident_key)
    .bind(now_rfc3339())
    .bind(now_rfc3339())
    .execute(fixture.db.pool())
    .await
    .unwrap();
    let attention_id: String =
        sqlx::query_scalar("SELECT id FROM attention_projection WHERE dedupe_key=?")
            .bind(&incident_key)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
    let attention = fixture
        .db
        .get_attention(&attention_id)
        .await
        .unwrap()
        .unwrap();
    AttentionService::new(
        fixture.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(fixture.db.clone())),
    )
    .admit_wake(wake_request(&fixture, &attention, &now_rfc3339()))
    .await
    .unwrap();
    wake_consumer.run_once(100).await.unwrap();
    let wake_turn = AgentChatTurnJobRepo::get_agent_chat_turn_job(
        &*fixture.db,
        &sqlx::query_scalar::<_, String>(
            "SELECT id FROM agent_chat_turn_job
             WHERE chat_id = ? AND triggering_message_id IN (SELECT id FROM agent_chat_message WHERE outcome='attention_wake')",
        )
        .bind(&fixture.chat_id)
        .fetch_one(fixture.db.pool())
        .await
        .unwrap(),
    )
    .await
    .unwrap()
    .unwrap();

    macro_rules! assert_frozen_provenance {
        ($left:expr, $right:expr) => {{
            assert_eq!($left.responder_identity_id, $right.responder_identity_id);
            assert_eq!($left.profile_id, $right.profile_id);
            assert_eq!($left.responder_binding_id, $right.responder_binding_id);
            assert_eq!(
                $left.responder_binding_version,
                $right.responder_binding_version
            );
            assert_eq!(
                $left.responder_identity_version,
                $right.responder_identity_version
            );
            assert_eq!($left.profile_version, $right.profile_version);
            assert_eq!(
                $left.operating_skill_revision_id,
                $right.operating_skill_revision_id
            );
            assert_eq!($left.policy_revision, $right.policy_revision);
            assert_eq!($left.policy_digest, $right.policy_digest);
            assert_eq!(
                $left.permission_policy_digest,
                $right.permission_policy_digest
            );
            assert_eq!($left.tool_policy_digest, $right.tool_policy_digest);
            assert_eq!($left.canonical_scope_type, $right.canonical_scope_type);
            assert_eq!($left.canonical_scope_id, $right.canonical_scope_id);
            assert!($left
                .admission_digest
                .as_deref()
                .is_some_and(|value| !value.is_empty()));
            assert!($left
                .canonical_scope_provenance_json
                .as_deref()
                .is_some_and(|value| !value.is_empty()));
        }};
    }
    assert_frozen_provenance!(user_turn, handoff_turn);
    assert_frozen_provenance!(user_turn, wake_turn);

    // A worker retry reuses the admitted turn job. The profile on that job is
    // the retry's provenance, rather than a later binding/profile lookup.
    let retry_turn = AgentChatTurnJobRepo::update_agent_chat_turn_job(
        &*fixture.db,
        UpdateAgentChatTurnJob {
            id: user_turn.id.clone(),
            expected_version: user_turn.version,
            status: AgentChatTurnState::RetryWait,
            pending_interaction_id: None,
            lease_owner: Some(None),
            leased_until: Some(None),
            attempt_count: Some(1),
            next_attempt_at: Some(Some(now_rfc3339())),
            response_message_id: None,
            error_code: Some(Some("transient".to_owned())),
            error_message: Some(Some("retry characterization".to_owned())),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    assert_eq!(retry_turn.status, AgentChatTurnState::RetryWait);
    assert_eq!(
        retry_turn.responder_identity_id.as_deref(),
        Some(fixture.identity_id.as_str())
    );
    assert_eq!(
        retry_turn.profile_id.as_deref(),
        Some(current_profile.as_str())
    );
    assert_frozen_provenance!(user_turn, retry_turn);

    // Change the selected Profile and replace the Project binding after all
    // three turns were admitted. Their frozen responder provenance must not
    // be rewritten by either later mutation.
    let later_profile = select_profile(
        &fixture.db,
        &fixture.identity_id,
        &new_uuid_v4(),
        "profile-after-admission",
    )
    .await;
    assert_ne!(later_profile, current_profile);
    let replacement_identity = new_uuid_v4();
    let replacement_profile = new_uuid_v4();
    owned_identity_with_profile(
        &fixture.db,
        &replacement_identity,
        &fixture.account_id,
        &replacement_profile,
    )
    .await;
    let current_binding: (i64,) = sqlx::query_as(
        "SELECT version FROM project_agent_binding
         WHERE project_id = ? AND state IN ('active', 'agent_setup_required')",
    )
    .bind(&fixture.project_id)
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    chats
        .set_project_binding(SetProjectAgentBindingInput {
            actor_user_id: fixture.account_id.clone(),
            project_id: fixture.project_id.clone(),
            identity_id: Some(replacement_identity),
            state: "active".to_owned(),
            autonomy_policy_json: "{}".to_owned(),
            permission_ceiling_json: "{}".to_owned(),
            subscriptions_json: "[]".to_owned(),
            wake_budget: 10,
            expected_version: Some(current_binding.0),
            replacement_reason: Some("characterization rebinding".to_owned()),
        })
        .await
        .unwrap();

    for turn_id in [user_turn.id, handoff_turn.id] {
        let frozen = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*fixture.db, &turn_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            frozen.responder_identity_id.as_deref(),
            Some(fixture.identity_id.as_str())
        );
        assert_eq!(frozen.profile_id.as_deref(), Some(current_profile.as_str()));
    }
    let frozen_retry = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*fixture.db, &retry_turn.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        frozen_retry.responder_identity_id.as_deref(),
        Some(fixture.identity_id.as_str())
    );
    assert_eq!(
        frozen_retry.profile_id.as_deref(),
        Some(current_profile.as_str())
    );
}

#[tokio::test]
async fn wake_re_evaluates_current_replacement_binding() {
    let db = database().await;
    let original_identity = new_uuid_v4();
    let original_profile = identity_with_profile(&db, &original_identity).await;
    let (project_id, chat_id) = bound_project(&db, &original_identity, &original_profile).await;
    let replacement_identity = new_uuid_v4();
    let replacement_profile = identity_with_profile(&db, &replacement_identity).await;
    sqlx::query("UPDATE agent_identity SET owner_id = (SELECT owner_id FROM project WHERE id = ?) WHERE id = ?")
        .bind(&project_id)
        .bind(&replacement_identity)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE project_agent_binding
         SET identity_id = ?, profile_id = ?, state = 'active',
             updated_at = ?, version = version + 1
         WHERE project_id = ?",
    )
    .bind(&replacement_identity)
    .bind(&replacement_profile)
    .bind(now_rfc3339())
    .bind(&project_id)
    .execute(db.pool())
    .await
    .unwrap();
    let a = seed_incident(
        &db,
        &project_id,
        "execution_failed",
        &format!("attention:binding_replaced:project:{project_id}"),
        "Binding was replaced",
        serde_json::json!({"scope_type": "project", "scope_id": project_id}),
        "inspect_run",
    )
    .await;
    // The incident still names the old identity; admission resolves the
    // current binding and freezes the replacement identity and Profile.
    sqlx::query("UPDATE attention_projection SET identity_id = ? WHERE id = ?")
        .bind(&original_identity)
        .bind(&a.id)
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        AttentionService::new(
            Arc::clone(&db),
            Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db)))
        )
        .sweep_once_at(&now_rfc3339())
        .await
        .unwrap(),
        1
    );
    let (responder, profile): (String, String) = sqlx::query_as(
        "SELECT responder_identity_id, profile_id FROM agent_chat_turn_job WHERE chat_id = ?",
    )
    .bind(&chat_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(responder, replacement_identity);
    assert_eq!(profile, replacement_profile);
}

#[tokio::test]
async fn wake_turn_resolves_identity_current_profile_after_profile_edit() {
    let fixture = chat_turn_fixture().await;
    incident(&fixture, "execution_failed", "profile-edit").await;
    // The binding still names the same identity, but its Profile snapshot is
    // now stale; admission uses the newly selected Profile.
    let current_profile = select_profile(
        &fixture.db,
        &fixture.identity_id,
        &new_uuid_v4(),
        "wake-profile-after-edit",
    )
    .await;
    assert_eq!(
        AttentionService::new(
            Arc::clone(&fixture.db),
            Arc::new(services::WakeTurnConsumer::new(Arc::clone(&fixture.db)))
        )
        .sweep_once_at(&now_rfc3339())
        .await
        .unwrap(),
        1
    );
    let profile: String =
        sqlx::query_scalar("SELECT profile_id FROM agent_chat_turn_job WHERE chat_id = ?")
            .bind(&fixture.chat_id)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
    assert_eq!(profile, current_profile);
}

#[tokio::test]
async fn alternate_wake_producer_cannot_override_server_resolved_responder() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let bound_profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &bound_profile_id).await;
    let current_profile_id = select_profile(
        &db,
        &identity_id,
        &new_uuid_v4(),
        "current-wake-producer-profile",
    )
    .await;
    let a = seed_incident(
        &db,
        &project_id,
        "execution_failed",
        &format!("attention:alternate_producer:project:{project_id}"),
        "Alternate producer incident",
        serde_json::json!({"scope_type": "project", "scope_id": project_id}),
        "inspect_run",
    )
    .await;
    // A forged audit event naming a spoofed responder never admits a turn.
    db.append_event(CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "agent.wake.admitted".to_owned(),
        entity_type: "agent_wake".to_owned(),
        entity_id: a.dedupe_key.clone(),
        actor_type: "attention_projection".to_owned(),
        actor_id: None,
        scope_type: "project".to_owned(),
        scope_id: project_id.clone(),
        correlation_id: new_uuid_v4(),
        causation_id: None,
        causation_depth: 1,
        dedupe_key: None,
        payload_json: serde_json::json!({
            "identity_id": "spoofed-identity",
            "responder_identity_id": "spoofed-identity",
            "responder_profile_id": "spoofed-profile",
            "incident_key": a.dedupe_key,
            "attention_id": a.id,
        })
        .to_string(),
        created_at: now_rfc3339(),
    })
    .await
    .unwrap();
    WakeTurnConsumer::new(Arc::clone(&db))
        .run_once(100)
        .await
        .unwrap();
    assert_eq!(chat_turn_count(&db, &chat_id).await, 0);
    AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    )
    .sweep_once_at(&now_rfc3339())
    .await
    .unwrap();
    let (responder, profile): (String, String) = sqlx::query_as(
        "SELECT responder_identity_id, profile_id FROM agent_chat_turn_job WHERE chat_id = ?",
    )
    .bind(&chat_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(responder, identity_id);
    assert_eq!(profile, current_profile_id);
}

#[tokio::test]
async fn deferred_wake_retries_after_authoritative_responder_recovery() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    // A paused responder is transient: the incident waits as setup-required
    // and the sweep admits it once the responder recovers.
    sqlx::query("UPDATE agent_identity SET paused = 1, version = version + 1 WHERE id = ?")
        .bind(&identity_id)
        .execute(db.pool())
        .await
        .unwrap();
    let a = seed_incident(
        &db,
        &project_id,
        "execution_failed",
        &format!("attention:deferred_recovery:project:{project_id}"),
        "Responder temporarily unavailable",
        serde_json::json!({"scope_type": "project", "scope_id": project_id}),
        "restore_responder",
    )
    .await;
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 0);
    let latest: String = sqlx::query_scalar(
        "SELECT disposition FROM agent_wake_attention_latest WHERE attention_id = ?",
    )
    .bind(&a.id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(latest, "setup_required");
    sqlx::query("UPDATE agent_identity SET paused = 0, version = version + 1 WHERE id = ?")
        .bind(&identity_id)
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 0);
    assert_eq!(admitted_count(&db).await, 1);
    assert_eq!(chat_turn_count(&db, &chat_id).await, 1);
}

#[tokio::test]
async fn deferred_wake_rechecks_changed_incident_material_before_delivery() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    sqlx::query("UPDATE agent_identity SET paused = 1, version = version + 1 WHERE id = ?")
        .bind(&identity_id)
        .execute(db.pool())
        .await
        .unwrap();
    let key = format!("attention:changed_material:project:{project_id}");
    seed_incident(
        &db,
        &project_id,
        "execution_failed",
        &key,
        "Changing incident",
        serde_json::json!({"scope_type": "project", "scope_id": project_id, "state": "initial"}),
        "inspect_run",
    )
    .await;
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 0);
    sqlx::query(
        "UPDATE attention_projection SET details_json = ?, version = version + 1, updated_at = ?
         WHERE dedupe_key = ?",
    )
    .bind(
        serde_json::json!({"scope_type": "project", "scope_id": project_id, "state": "materially-changed"})
            .to_string(),
    )
    .bind(now_rfc3339())
    .bind(&key)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE agent_identity SET paused = 0, version = version + 1 WHERE id = ?")
        .bind(&identity_id)
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    let content: String = sqlx::query_scalar(
        "SELECT m.content FROM agent_chat_turn_job j
         JOIN agent_chat_message m ON m.id = j.triggering_message_id WHERE j.chat_id = ?",
    )
    .bind(&chat_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert!(content.contains("materially-changed"));
    assert!(
        !content.contains("\"initial\""),
        "stale deferred content must not be delivered"
    );
}

/// One failing Project is logged and skipped; its admission rolls back whole,
/// and another Project's blocker is still admitted in the same sweep.
#[tokio::test]
async fn runtime_deferred_agent_and_failing_tick_do_not_block_other_agent_and_admission_is_atomic()
{
    let db = database().await;
    let a = new_uuid_v4();
    let pa = identity_with_profile(&db, &a).await;
    let (project_a, chat_a) = bound_project(&db, &a, &pa).await;
    let b = new_uuid_v4();
    let pb = identity_with_profile(&db, &b).await;
    let (project_b, chat_b) = bound_project(&db, &b, &pb).await;
    let incident_a = seed_incident(
        &db,
        &project_a,
        "execution_failed",
        &format!("attention:execution_failed:project:{project_a}:a"),
        "A blocker",
        serde_json::json!({"scope_type": "project", "scope_id": project_a}),
        "inspect_run",
    )
    .await;
    seed_incident(
        &db,
        &project_b,
        "execution_failed",
        &format!("attention:execution_failed:project:{project_b}:b"),
        "B blocker",
        serde_json::json!({"scope_type": "project", "scope_id": project_b}),
        "inspect_run",
    )
    .await;
    sqlx::raw_sql(&format!(
        "CREATE TRIGGER fail_project_a_batch BEFORE INSERT ON agent_wake_batch
         WHEN NEW.project_id = '{project_a}'
         BEGIN SELECT RAISE(ABORT, 'synthetic batch failure'); END;"
    ))
    .execute(db.pool())
    .await
    .unwrap();
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    assert_eq!(chat_turn_count(&db, &chat_b).await, 1);
    assert_eq!(chat_turn_count(&db, &chat_a).await, 0);
    let (audits, charged_a, linked_a): (i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT COUNT(*) FROM domain_event WHERE event_type = 'agent.wake.admitted' AND scope_id = ?),
            (SELECT COALESCE(SUM(admitted_count), 0) FROM agent_wake_budget_window WHERE scope_id = ?),
            (SELECT COUNT(*) FROM agent_wake_blocker WHERE attention_id = ?)",
    )
    .bind(&project_a)
    .bind(&project_a)
    .bind(&incident_a.id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        (audits, charged_a, linked_a),
        (0, 0, 0),
        "failed admission rolls back its audit, charge and links"
    );
    sqlx::query("DROP TRIGGER fail_project_a_batch")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    assert_eq!(chat_turn_count(&db, &chat_a).await, 1);
}

/// A decision settles only with the turn that answers it.
#[tokio::test]
async fn decision_resolves_only_when_its_turn_is_admitted() {
    let f = chat_turn_fixture().await;
    let decision = incident(&f, "decision_recorded", "approved-plan").await;
    sqlx::raw_sql("CREATE TRIGGER reject_turn BEFORE INSERT ON agent_chat_turn_job BEGIN SELECT RAISE(ABORT,'synthetic turn failure'); END;")
        .execute(f.db.pool())
        .await
        .unwrap();
    let service = AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    );
    assert!(service
        .admit_wake(wake_request(&f, &decision, &now_rfc3339()))
        .await
        .is_err());
    let status: String = sqlx::query_scalar("SELECT status FROM attention_projection WHERE id = ?")
        .bind(&decision.id)
        .fetch_one(f.db.pool())
        .await
        .unwrap();
    assert_eq!(
        status, "open",
        "a failed admission must not settle the decision"
    );
    sqlx::query("DROP TRIGGER reject_turn")
        .execute(f.db.pool())
        .await
        .unwrap();
    service
        .admit_wake(wake_request(&f, &decision, &now_rfc3339()))
        .await
        .unwrap();
    let status: String = sqlx::query_scalar("SELECT status FROM attention_projection WHERE id = ?")
        .bind(&decision.id)
        .fetch_one(f.db.pool())
        .await
        .unwrap();
    assert_eq!(status, "resolved");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn audit_two_runtimes_race_one_wake_event() {
    let (db, database_path) = file_database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    seed_incident(
        &db,
        &project_id,
        "execution_failed",
        &format!("attention:execution_failed:project:{project_id}:race"),
        "Race incident",
        serde_json::json!({"scope_type": "project", "scope_id": project_id}),
        "inspect_run",
    )
    .await;
    let a = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    let b = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    let now = now_rfc3339();
    let (run_a, run_b) = tokio::join!(a.sweep_once_at(&now), b.sweep_once_at(&now));
    assert_eq!(
        run_a.unwrap() + run_b.unwrap(),
        1,
        "two sweeps admit one turn"
    );
    let charged: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(admitted_count), 0) FROM agent_wake_budget_window WHERE scope_id = ?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(chat_turn_count(&db, &chat_id).await, 1);
    assert_eq!(charged, 1, "the losing sweep charges nothing");
    drop(a);
    drop(b);
    drop(db);
    let _ = std::fs::remove_file(database_path);
}

#[tokio::test]
async fn audit2_responder_read_failure_keeps_existing_deferral() {
    let f = chat_turn_fixture().await;
    let a = incident(&f, "execution_failed", "unreadable").await;
    // The resolver translates an unreadable legacy enum into a readiness wait.
    let mut tx = f.db.pool().begin().await.unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("UPDATE agent_identity SET status = 'unreadable_legacy_status' WHERE id = ?")
        .bind(&f.identity_id)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = OFF")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    // The unreadable responder fails only this Project's scope: the sweep
    // still succeeds, nothing is dead-lettered, and the blocker stays open
    // for the next sweep.
    let service = AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    );
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 0);
    assert_eq!(turns(&f).await, 0);
    let dead: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM worker_dead_letter")
        .fetch_one(f.db.pool())
        .await
        .unwrap();
    assert_eq!(dead, 0);
    sqlx::query("UPDATE agent_identity SET status = 'idle' WHERE id = ?")
        .bind(&f.identity_id)
        .execute(f.db.pool())
        .await
        .unwrap();
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    let status: String = sqlx::query_scalar("SELECT status FROM attention_projection WHERE id = ?")
        .bind(&a.id)
        .fetch_one(f.db.pool())
        .await
        .unwrap();
    assert_eq!(status, "open");
}
