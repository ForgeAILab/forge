use db::{
    canonical_attention_incident_digest, create_sqlite_pool, run_migrations,
    AgentChatMessageAuthorType, AgentChatMessageStatus, AgentChatRepo, AgentProfileRepo, AgentRepo,
    AgentStatus, AgentWakeDispositionKind, AgentWakeDispositionRepo, AttentionRepo,
    ClaimDomainEvents, CompleteClaimedWake, CompleteDomainEvent, CreateAgentChatMessage,
    CreateAgentChatTurnJob, CreateAgentIdentity, CreateAgentProfile, CreateAgentWakeDisposition,
    CreateAttentionProjection, CreateDomainEvent, CreateProject, CreateProjectAgentBinding,
    DomainEventRepo, ExpectedAttentionSnapshot, ProjectAgentBindingRepo, ProjectRepo,
    ReplaceProjectAgentBinding, RetryAgentWakeDisposition, SqliteDb, User, UserRepo,
};

async fn database() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    SqliteDb::new(pool)
}

async fn seed_lease_identity(db: &SqliteDb, identity_id: &str, profile_id: &str) {
    AgentRepo::create_identity_with_profile(
        db,
        CreateAgentIdentity {
            id: identity_id.to_owned(),
            name: format!("Lease {identity_id}"),
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
            created_at: "2026-08-21T00:00:00Z".to_owned(),
            updated_at: "2026-08-21T00:00:00Z".to_owned(),
        },
        CreateAgentProfile {
            id: profile_id.to_owned(),
            identity_id: identity_id.to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("test-model".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: "2026-08-21T00:00:00Z".to_owned(),
            updated_at: "2026-08-21T00:00:00Z".to_owned(),
        },
    )
    .await
    .expect("lease identity inserts");
}

fn source_event(id: &str, created_at: &str) -> CreateDomainEvent {
    CreateDomainEvent {
        id: id.to_owned(),
        event_type: "agent.wake.admitted".to_owned(),
        entity_type: "attention".to_owned(),
        entity_id: format!("incident-{id}"),
        actor_type: "system".to_owned(),
        actor_id: None,
        scope_type: "project".to_owned(),
        scope_id: "project-wake".to_owned(),
        correlation_id: format!("corr-{id}"),
        causation_id: None,
        causation_depth: 0,
        dedupe_key: Some(format!("wake-event-{id}")),
        payload_json: "{}".to_owned(),
        created_at: created_at.to_owned(),
    }
}

async fn append_and_claim(db: &SqliteDb, id: &str) -> (db::DomainEvent, String) {
    let now = "2026-08-21T00:00:00Z";
    let event = DomainEventRepo::append_event(db, source_event(id, now))
        .await
        .expect("event appends");
    let claimed = DomainEventRepo::claim_event_batch(
        db,
        ClaimDomainEvents {
            consumer_name: "agent-wake-turns".to_owned(),
            lease_owner: "wake-test".to_owned(),
            now: now.to_owned(),
            leased_until: "2026-08-21T00:01:00Z".to_owned(),
            limit: 10,
        },
    )
    .await
    .expect("event claims");
    assert_eq!(claimed, vec![event.clone()]);
    (event, "wake-test".to_owned())
}

async fn drain_existing_events(db: &SqliteDb) {
    let now = "2026-08-21T00:00:00Z";
    let owner = "wake-drain";
    let events = DomainEventRepo::claim_event_batch(
        db,
        ClaimDomainEvents {
            consumer_name: "agent-wake-turns".to_owned(),
            lease_owner: owner.to_owned(),
            now: now.to_owned(),
            leased_until: "2026-08-21T00:01:00Z".to_owned(),
            limit: 100,
        },
    )
    .await
    .expect("existing events claim");
    for event in events {
        AgentWakeDispositionRepo::complete_claimed_agent_wake(
            db,
            CompleteClaimedWake {
                disposition: disposition(
                    &event,
                    AgentWakeDispositionKind::DeterministicallySuppressed,
                    "preexisting_event",
                    "2026-08-21T00:00:01Z",
                ),
                completion: completion(&event, owner),
                admission: None,
                expected_attention: None,
            },
        )
        .await
        .expect("existing event completion");
    }
}

fn completion(event: &db::DomainEvent, lease_owner: &str) -> CompleteDomainEvent {
    CompleteDomainEvent {
        consumer_name: "agent-wake-turns".to_owned(),
        lease_owner: lease_owner.to_owned(),
        event_sequence: event.sequence,
        event_id: event.id.clone(),
        dedupe_key: event.dedupe_key.clone().expect("test event dedupe key"),
        completed_at: "2026-08-21T00:00:01Z".to_owned(),
    }
}

fn disposition(
    event: &db::DomainEvent,
    kind: AgentWakeDispositionKind,
    reason: &str,
    updated_at: &str,
) -> CreateAgentWakeDisposition {
    CreateAgentWakeDisposition {
        id: format!("disposition-{}-{}", event.id, reason),
        consumer_name: "agent-wake-turns".to_owned(),
        source_event_id: event.id.clone(),
        source_event_sequence: event.sequence,
        attempt_number: 1,
        max_attempts: 3,
        disposition: kind,
        reason: reason.to_owned(),
        turn_job_id: None,
        attention_id: None,
        retry_at: (kind == AgentWakeDispositionKind::Deferred)
            .then(|| "2026-08-21T00:02:00Z".to_owned()),
        incident_key: Some("incident-1".to_owned()),
        incident_digest: Some("digest-1".to_owned()),
        binding_id: None,
        binding_version: None,
        profile_id: None,
        profile_version: None,
        provenance_json: Some("{}".to_owned()),
        parent_disposition_id: None,
        created_at: updated_at.to_owned(),
        updated_at: updated_at.to_owned(),
    }
}

#[tokio::test]
async fn install_cutover_records_reason_and_preserves_post_cutover_events() {
    let db = database().await;
    let (cutover_sequence, cutover_reason): (i64, String) = sqlx::query_as(
        "SELECT cutover_sequence, reason FROM event_consumer_cutover
         WHERE consumer_name = 'agent-wake-turns'",
    )
    .fetch_one(db.pool())
    .await
    .expect("wake cutover row");
    let cursor = DomainEventRepo::get_consumer_cursor(&db, "agent-wake-turns")
        .await
        .expect("cursor lookup")
        .expect("installed wake cursor");
    assert_eq!(cursor.last_sequence, cutover_sequence);
    assert_eq!(cutover_reason, "agent-wake-turns-install-cutover");

    let max_at_install: i64 = sqlx::query_scalar("SELECT MAX(sequence) FROM domain_event")
        .fetch_one(db.pool())
        .await
        .expect("event max");
    assert_eq!(cutover_sequence, max_at_install);

    let (_event, _owner) = append_and_claim(&db, "cutover-event").await;
}

#[tokio::test]
async fn wake_lease_renewal_is_allowed_but_rebinding_cannot_parallelize_incident() {
    let db = database().await;
    seed_lease_identity(&db, "lease-identity-a", "lease-profile-a").await;
    seed_lease_identity(&db, "lease-identity-b", "lease-profile-b").await;
    sqlx::query(
        "INSERT INTO agent_wake_lease (
            identity_id, scope_type, scope_id, incident_key, lease_owner,
            leased_until, reaction_depth, updated_at
         ) VALUES (?, 'agent_chat', 'lease-chat', 'incident-1', ?, ?, 0, ?)",
    )
    .bind("lease-identity-a")
    .bind("owner-a")
    .bind("2026-08-21T00:10:00Z")
    .bind("2026-08-21T00:00:00Z")
    .execute(db.pool())
    .await
    .expect("initial lease");
    sqlx::query(
        "INSERT INTO agent_wake_lease (
            identity_id, scope_type, scope_id, incident_key, lease_owner,
            leased_until, reaction_depth, updated_at
         ) VALUES (?, 'agent_chat', 'lease-chat', 'incident-1', ?, ?, 0, ?)
         ON CONFLICT(identity_id, scope_type, scope_id, incident_key)
         DO UPDATE SET leased_until = excluded.leased_until,
                       updated_at = excluded.updated_at",
    )
    .bind("lease-identity-a")
    .bind("owner-a")
    .bind("2026-08-21T00:20:00Z")
    .bind("2026-08-21T00:01:00Z")
    .execute(db.pool())
    .await
    .expect("same identity renews lease");
    let replacement = sqlx::query(
        "INSERT INTO agent_wake_lease (
            identity_id, scope_type, scope_id, incident_key, lease_owner,
            leased_until, reaction_depth, updated_at
         ) VALUES (?, 'agent_chat', 'lease-chat', 'incident-1', ?, ?, 0, ?)",
    )
    .bind("lease-identity-b")
    .bind("owner-b")
    .bind("2026-08-21T00:30:00Z")
    .bind("2026-08-21T00:02:00Z")
    .execute(db.pool())
    .await;
    assert!(replacement.is_err());
}

#[tokio::test]
async fn disposition_cursor_receipt_and_lease_commit_atomically() {
    let db = database().await;
    let (event, owner) = append_and_claim(&db, "atomic-event").await;
    let input = disposition(
        &event,
        AgentWakeDispositionKind::DeterministicallySuppressed,
        "unchanged_incident",
        "2026-08-21T00:00:01Z",
    );
    let recorded = AgentWakeDispositionRepo::complete_claimed_agent_wake(
        &db,
        CompleteClaimedWake {
            disposition: input.clone(),
            completion: completion(&event, &owner),
            admission: None,
            expected_attention: None,
        },
    )
    .await
    .expect("disposition commits");
    assert_eq!(
        recorded.disposition,
        AgentWakeDispositionKind::DeterministicallySuppressed
    );

    let current = AgentWakeDispositionRepo::get_current_agent_wake_disposition(
        &db,
        "agent-wake-turns",
        &event.id,
    )
    .await
    .expect("current lookup")
    .expect("current disposition");
    assert_eq!(current.id, input.id);
    let cursor = DomainEventRepo::get_consumer_cursor(&db, "agent-wake-turns")
        .await
        .expect("cursor lookup")
        .expect("cursor");
    assert_eq!(cursor.last_sequence, event.sequence);
    let receipt_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM event_projection_receipt
         WHERE consumer_name = 'agent-wake-turns' AND event_id = ?",
    )
    .bind(&event.id)
    .fetch_one(db.pool())
    .await
    .expect("receipt count");
    assert_eq!(receipt_count, 1);
    let lease_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM event_processing_lease
         WHERE consumer_name = 'agent-wake-turns' AND event_sequence = ?",
    )
    .bind(event.sequence)
    .fetch_one(db.pool())
    .await
    .expect("lease count");
    assert_eq!(lease_count, 0);

    let (rollback_event, rollback_owner) = append_and_claim(&db, "atomic-rollback-event").await;
    let mut invalid = disposition(
        &rollback_event,
        AgentWakeDispositionKind::TurnAdmitted,
        "bad_turn",
        "2026-08-21T00:00:02Z",
    );
    invalid.source_event_id = "atomic-rollback-event".to_owned();
    invalid.source_event_sequence = rollback_event.sequence;
    invalid.turn_job_id = Some("missing-turn".to_owned());
    let error = AgentWakeDispositionRepo::complete_claimed_agent_wake(
        &db,
        CompleteClaimedWake {
            disposition: invalid,
            completion: CompleteDomainEvent {
                event_id: "atomic-rollback-event".to_owned(),
                event_sequence: rollback_event.sequence,
                ..completion(&rollback_event, &rollback_owner)
            },
            admission: None,
            expected_attention: None,
        },
    )
    .await
    .expect_err("invalid admission is rejected");
    assert!(matches!(error, db::DbError::Check(_)));
    let disposition_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_wake_disposition
         WHERE source_event_id = 'atomic-rollback-event'",
    )
    .fetch_one(db.pool())
    .await
    .expect("disposition count");
    assert_eq!(disposition_count, 0);
}

#[tokio::test]
async fn replay_is_idempotent_but_changed_disposition_conflicts() {
    let db = database().await;
    let (event, owner) = append_and_claim(&db, "replay-event").await;
    let input = disposition(
        &event,
        AgentWakeDispositionKind::DeterministicallySuppressed,
        "self_causation",
        "2026-08-21T00:00:01Z",
    );
    let first = AgentWakeDispositionRepo::complete_claimed_agent_wake(
        &db,
        CompleteClaimedWake {
            disposition: input.clone(),
            completion: completion(&event, &owner),
            admission: None,
            expected_attention: None,
        },
    )
    .await
    .expect("first disposition");
    let replay = AgentWakeDispositionRepo::complete_claimed_agent_wake(
        &db,
        CompleteClaimedWake {
            disposition: input.clone(),
            completion: completion(&event, &owner),
            admission: None,
            expected_attention: None,
        },
    )
    .await
    .expect("replay disposition");
    assert_eq!(first, replay);
    let mut changed = input;
    changed.reason = "resolved_incident".to_owned();
    let error = AgentWakeDispositionRepo::complete_claimed_agent_wake(
        &db,
        CompleteClaimedWake {
            disposition: changed,
            completion: completion(&event, &owner),
            admission: None,
            expected_attention: None,
        },
    )
    .await
    .expect_err("changed disposition conflicts");
    assert!(matches!(error, db::DbError::IdempotencyConflict));
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_wake_disposition
         WHERE consumer_name = 'agent-wake-turns' AND source_event_id = ?",
    )
    .bind(&event.id)
    .fetch_one(db.pool())
    .await
    .expect("attempt count");
    assert_eq!(count, 1);
}

#[tokio::test]
async fn deferred_retry_appends_attempt_and_moves_current_pointer() {
    let db = database().await;
    let (event, owner) = append_and_claim(&db, "deferred-event").await;
    let first = disposition(
        &event,
        AgentWakeDispositionKind::Deferred,
        "chat_temporarily_unavailable",
        "2026-08-21T00:00:01Z",
    );
    AgentWakeDispositionRepo::complete_claimed_agent_wake(
        &db,
        CompleteClaimedWake {
            disposition: first.clone(),
            completion: completion(&event, &owner),
            admission: None,
            expected_attention: None,
        },
    )
    .await
    .expect("deferred disposition");
    assert!(AgentWakeDispositionRepo::list_due_agent_wake_dispositions(
        &db,
        "agent-wake-turns",
        "2026-08-21T00:01:00Z",
        10,
    )
    .await
    .expect("due query")
    .is_empty());

    let mut retry = disposition(
        &event,
        AgentWakeDispositionKind::Deferred,
        "runtime_still_unavailable",
        "2026-08-21T00:03:00Z",
    );
    retry.id = "deferred-attempt-2".to_owned();
    retry.attempt_number = 2;
    retry.parent_disposition_id = Some(first.id.clone());
    retry.retry_at = Some("2026-08-21T00:04:00Z".to_owned());
    let second = AgentWakeDispositionRepo::retry_agent_wake(
        &db,
        RetryAgentWakeDisposition {
            disposition: retry.clone(),
            expected_parent_id: first.id.clone(),
            now: "2026-08-21T00:03:00Z".to_owned(),
            admission: None,
            expected_attention: None,
        },
    )
    .await
    .expect("retry appends");
    assert_eq!(second.attempt_number, 2);
    let old =
        AgentWakeDispositionRepo::get_agent_wake_disposition(&db, "agent-wake-turns", &event.id, 1)
            .await
            .expect("old attempt lookup")
            .expect("old attempt");
    assert_eq!(old.reason, "chat_temporarily_unavailable");
    let current = AgentWakeDispositionRepo::get_current_agent_wake_disposition(
        &db,
        "agent-wake-turns",
        &event.id,
    )
    .await
    .expect("current lookup")
    .expect("current");
    assert_eq!(current.id, retry.id);
    assert_eq!(current.attempt_number, 2);
    assert_eq!(
        AgentWakeDispositionRepo::list_due_agent_wake_dispositions(
            &db,
            "agent-wake-turns",
            "2026-08-21T00:05:00Z",
            10,
        )
        .await
        .expect("due query")
        .len(),
        1
    );
}

#[tokio::test]
async fn setup_required_reconsideration_is_attention_change_driven() {
    let db = database().await;
    seed_lease_identity(&db, "setup-identity", "setup-profile").await;
    let (event, owner) = append_and_claim(&db, "setup-event").await;
    let attention = AttentionRepo::insert_attention(
        &db,
        CreateAttentionProjection {
            id: "setup-attention".to_owned(),
            attention_type: "agent_setup_required".to_owned(),
            scope_type: "project".to_owned(),
            scope_id: "project-wake".to_owned(),
            identity_id: Some("setup-identity".to_owned()),
            source_event_id: event.id.clone(),
            priority: 70,
            status: "open".to_owned(),
            summary: "Configure wake responder".to_owned(),
            details_json: "{}".to_owned(),
            dedupe_key: "setup-attention".to_owned(),
            occurred_at: "2026-08-21T00:00:00Z".to_owned(),
            updated_at: "2026-08-21T00:00:01Z".to_owned(),
            acknowledged_at: None,
            snoozed_until: None,
            resolved_at: None,
            updated_by_user_id: None,
            recommended_action: "configure_agent".to_owned(),
            source_sequence: Some(event.sequence),
        },
    )
    .await
    .expect("attention inserts");
    let mut setup = disposition(
        &event,
        AgentWakeDispositionKind::SetupRequired,
        "binding_missing",
        "2026-08-21T00:00:02Z",
    );
    setup.id = "setup-attempt-1".to_owned();
    setup.attention_id = Some(attention.id.clone());
    setup.profile_id = Some("setup-profile".to_owned());
    setup.profile_version = Some(1);
    AgentWakeDispositionRepo::complete_claimed_agent_wake(
        &db,
        CompleteClaimedWake {
            disposition: setup,
            completion: completion(&event, &owner),
            admission: None,
            expected_attention: None,
        },
    )
    .await
    .expect("setup disposition");
    assert!(
        AgentWakeDispositionRepo::list_reconsiderable_agent_wake_dispositions(
            &db,
            "agent-wake-turns",
            "2026-08-21T00:03:00Z",
            10,
        )
        .await
        .expect("reconsideration query")
        .is_empty()
    );
    AgentProfileRepo::create_profile(
        &db,
        CreateAgentProfile {
            id: "setup-profile-new".to_owned(),
            identity_id: "setup-identity".to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("changed-model".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: "2026-08-21T00:00:03Z".to_owned(),
            updated_at: "2026-08-21T00:00:03Z".to_owned(),
        },
    )
    .await
    .expect("profile setup change");
    let profile_change = AgentWakeDispositionRepo::list_reconsiderable_agent_wake_dispositions(
        &db,
        "agent-wake-turns",
        "2026-08-21T00:03:00Z",
        10,
    )
    .await
    .expect("profile reconsideration query");
    assert_eq!(profile_change.len(), 1);
    sqlx::query(
        "UPDATE agent_identity
         SET selected_profile_id = ?, version = version + 1, updated_at = ?
         WHERE id = ?",
    )
    .bind("setup-profile-new")
    .bind("2026-08-21T00:04:00Z")
    .bind("setup-identity")
    .execute(db.pool())
    .await
    .expect("selected profile setup change");
    UserRepo::create_user(
        &db,
        &User {
            id: "setup-account".to_owned(),
            email: "setup-account@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: "2026-08-21T00:00:03Z".to_owned(),
            updated_at: "2026-08-21T00:00:03Z".to_owned(),
        },
    )
    .await
    .expect("setup account inserts");
    ProjectRepo::create(
        &db,
        CreateProject {
            id: "project-wake".to_owned(),
            name: "Setup Project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some("setup-account".to_owned()),
            created_at: "2026-08-21T00:00:03Z".to_owned(),
            updated_at: "2026-08-21T00:00:03Z".to_owned(),
        },
    )
    .await
    .expect("setup project inserts");
    assert_eq!(
        AgentWakeDispositionRepo::list_reconsiderable_agent_wake_dispositions(
            &db,
            "agent-wake-turns",
            "2026-08-21T00:00:03Z",
            10,
        )
        .await
        .expect("binding-change reconsideration query")
        .len(),
        1
    );
    AttentionRepo::update_attention_lifecycle(
        &db,
        db::UpdateAttentionLifecycle {
            id: attention.id,
            expected_version: attention.version,
            status: "acknowledged".to_owned(),
            acknowledged_at: Some(Some("2026-08-21T00:04:00Z".to_owned())),
            snoozed_until: None,
            resolved_at: None,
            updated_by_user_id: None,
            updated_at: "2026-08-21T00:04:00Z".to_owned(),
        },
    )
    .await
    .expect("attention changes");
    let reconsiderable = AgentWakeDispositionRepo::list_reconsiderable_agent_wake_dispositions(
        &db,
        "agent-wake-turns",
        "2026-08-21T00:05:00Z",
        10,
    )
    .await
    .expect("reconsideration query");
    assert_eq!(reconsiderable.len(), 1);
}

#[tokio::test]
async fn turn_admission_is_committed_with_wake_disposition() {
    let db = database().await;
    let now = "2026-08-21T00:00:00Z".to_owned();
    UserRepo::create_user(
        &db,
        &User {
            id: "account-wake".to_owned(),
            email: "account-wake@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: Some("Wake Account".to_owned()),
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("account inserts");
    ProjectRepo::create(
        &db,
        CreateProject {
            id: "project-wake".to_owned(),
            name: "Wake Project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some("account-wake".to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project inserts");
    AgentRepo::create_identity_with_profile(
        &db,
        CreateAgentIdentity {
            id: "identity-wake".to_owned(),
            name: "Wake Agent".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some("account-wake".to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: "profile-wake".to_owned(),
            identity_id: "identity-wake".to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("test-model".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("identity/profile inserts");
    let binding = ProjectAgentBindingRepo::get_active_project_binding(&db, "project-wake")
        .await
        .expect("setup binding lookup")
        .expect("setup binding exists");
    ProjectAgentBindingRepo::replace_project_binding(
        &db,
        ReplaceProjectAgentBinding {
            project_id: "project-wake".to_owned(),
            expected_version: binding.version,
            replacement: CreateProjectAgentBinding {
                id: "binding-wake".to_owned(),
                project_id: "project-wake".to_owned(),
                identity_id: Some("identity-wake".to_owned()),
                profile_id: Some("profile-wake".to_owned()),
                state: "active".to_owned(),
                autonomy_policy_json: "{}".to_owned(),
                permission_ceiling_json: "{}".to_owned(),
                subscriptions_json: "[]".to_owned(),
                wake_budget: 3,
                operating_skill_revision_id: None,
                policy_revision: "default".to_owned(),
                policy_digest: String::new(),
                charter_id: None,
                charter_revision_id: None,
                charter_setup_required: true,
                admission_receipt_id: None,
                charter_approval_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            replacement_reason: Some("wake test setup".to_owned()),
        },
    )
    .await
    .expect("binding replacement");
    let chat = AgentChatRepo::get_project_chat(&db, "project-wake")
        .await
        .expect("project chat lookup")
        .expect("project chat exists");
    let chat_id = chat.id.clone();
    AgentChatRepo::update_agent_chat(
        &db,
        db::UpdateAgentChat {
            id: chat.id,
            expected_version: chat.version,
            status: Some("ready".to_owned()),
            instruction_revision: None,
            updated_at: now.clone(),
        },
    )
    .await
    .expect("chat ready");
    drain_existing_events(&db).await;
    let (event, owner) = append_and_claim(&db, "admitted-event").await;
    let attention = AttentionRepo::insert_attention(
        &db,
        CreateAttentionProjection {
            id: "wake-attention".to_owned(),
            attention_type: "agent_wake".to_owned(),
            scope_type: "project".to_owned(),
            scope_id: "project-wake".to_owned(),
            identity_id: Some("identity-wake".to_owned()),
            source_event_id: event.id.clone(),
            priority: 70,
            status: "open".to_owned(),
            summary: "Wake incident".to_owned(),
            details_json: "{\"state\":\"initial\"}".to_owned(),
            dedupe_key: "wake-attention".to_owned(),
            occurred_at: "2026-08-21T00:00:00Z".to_owned(),
            updated_at: "2026-08-21T00:00:01Z".to_owned(),
            acknowledged_at: None,
            snoozed_until: None,
            resolved_at: None,
            updated_by_user_id: None,
            recommended_action: "inspect".to_owned(),
            source_sequence: Some(event.sequence),
        },
    )
    .await
    .expect("wake attention inserts");
    let expected_attention = ExpectedAttentionSnapshot {
        id: attention.id.clone(),
        version: attention.version,
        digest: Some(canonical_attention_incident_digest(&attention)),
        status: attention.status.clone(),
        canonical_scope_type: attention.scope_type.clone(),
        canonical_scope_id: attention.scope_id.clone(),
        source_event_id: attention.source_event_id.clone(),
        source_sequence: attention.source_sequence,
        dedupe_key: attention.dedupe_key.clone(),
    };
    let changed_attention = AttentionRepo::insert_attention(
        &db,
        CreateAttentionProjection {
            id: "wake-attention-replay".to_owned(),
            attention_type: attention.attention_type.clone(),
            scope_type: attention.scope_type.clone(),
            scope_id: attention.scope_id.clone(),
            identity_id: attention.identity_id.clone(),
            source_event_id: attention.source_event_id.clone(),
            priority: attention.priority,
            status: attention.status.clone(),
            summary: attention.summary.clone(),
            details_json: "{\"state\":\"changed\"}".to_owned(),
            dedupe_key: attention.dedupe_key.clone(),
            occurred_at: attention.occurred_at.clone(),
            updated_at: "2026-08-21T00:00:02Z".to_owned(),
            acknowledged_at: None,
            snoozed_until: None,
            resolved_at: None,
            updated_by_user_id: None,
            recommended_action: attention.recommended_action.clone(),
            source_sequence: attention.source_sequence,
        },
    )
    .await
    .expect("material Attention upsert");
    assert_eq!(changed_attention.version, attention.version + 1);
    let message_id = "wake-message";
    let turn_id = "wake-turn";
    let turn = CreateAgentChatTurnJob {
        id: turn_id.to_owned(),
        chat_id: chat_id.clone(),
        triggering_message_id: message_id.to_owned(),
        responder_identity_id: "identity-wake".to_owned(),
        profile_id: "profile-wake".to_owned(),
        responder_binding_id: None,
        responder_binding_version: None,
        responder_identity_version: None,
        profile_version: None,
        operating_skill_revision_id: None,
        policy_revision: None,
        policy_digest: None,
        permission_policy_digest: None,
        tool_policy_digest: None,
        admission_digest: None,
        canonical_scope_provenance_json: None,
        canonical_scope_type: "agent_chat".to_owned(),
        canonical_scope_id: chat_id.clone(),
        dedupe_key: "wake-turn-dedupe".to_owned(),
        max_attempts: 3,
        correlation_id: "corr-wake-turn".to_owned(),
        causation_id: Some(event.id.clone()),
        causation_depth: 1,
        created_at: "2026-08-21T00:00:01Z".to_owned(),
        updated_at: "2026-08-21T00:00:01Z".to_owned(),
    };
    let message = CreateAgentChatMessage {
        id: message_id.to_owned(),
        chat_id: chat_id.clone(),
        sequence: 0,
        author_type: AgentChatMessageAuthorType::System,
        author_id: Some("identity-wake".to_owned()),
        content: "wake content".to_owned(),
        content_guard_json: "{}".to_owned(),
        sensitivity: "internal".to_owned(),
        status: AgentChatMessageStatus::Complete,
        outcome: None,
        model: None,
        profile_id: Some("profile-wake".to_owned()),
        session_id: None,
        context_manifest_id: None,
        token_usage_json: None,
        duration_ms: None,
        error: None,
        correlation_id: "corr-wake-turn".to_owned(),
        causation_id: Some(event.id.clone()),
        handoff_id: None,
        source_type: "native".to_owned(),
        source_id: Some(event.id.clone()),
        source_message_id: None,
        source_room_id: None,
        source_conversation_id: None,
        source_sequence: Some(event.sequence),
        source_metadata_json: "{}".to_owned(),
        created_at: "2026-08-21T00:00:01Z".to_owned(),
    };
    let mut admitted = disposition(
        &event,
        AgentWakeDispositionKind::TurnAdmitted,
        "wake_admitted",
        "2026-08-21T00:00:01Z",
    );
    admitted.id = "admitted-disposition".to_owned();
    admitted.turn_job_id = Some(turn_id.to_owned());
    let stale_error = AgentWakeDispositionRepo::complete_claimed_agent_wake(
        &db,
        CompleteClaimedWake {
            disposition: admitted.clone(),
            completion: completion(&event, &owner),
            admission: Some(db::AdmitAgentChatTurn {
                message: message.clone(),
                turn: turn.clone(),
            }),
            expected_attention: Some(expected_attention.clone()),
        },
    )
    .await
    .expect_err("stale Attention admission is rejected");
    assert!(matches!(stale_error, db::DbError::VersionConflict));
    let stale_turn_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_chat_turn_job WHERE id = 'wake-turn'")
            .fetch_one(db.pool())
            .await
            .expect("stale turn count");
    assert_eq!(stale_turn_count, 0);
    let stale_disposition_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_wake_disposition WHERE source_event_id = ?")
            .bind(&event.id)
            .fetch_one(db.pool())
            .await
            .expect("stale disposition count");
    assert_eq!(stale_disposition_count, 0);

    let current_attention = ExpectedAttentionSnapshot {
        version: changed_attention.version,
        digest: Some(canonical_attention_incident_digest(&changed_attention)),
        ..expected_attention
    };
    AgentWakeDispositionRepo::complete_claimed_agent_wake(
        &db,
        CompleteClaimedWake {
            disposition: admitted,
            completion: completion(&event, &owner),
            admission: Some(db::AdmitAgentChatTurn { message, turn }),
            expected_attention: Some(current_attention),
        },
    )
    .await
    .expect("atomic turn admission");
    let turn_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_chat_turn_job WHERE id = 'wake-turn'")
            .fetch_one(db.pool())
            .await
            .expect("turn count");
    assert_eq!(turn_count, 1);
    let event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event
         WHERE event_type = 'agent_chat.message.admitted' AND entity_id = 'wake-message'",
    )
    .fetch_one(db.pool())
    .await
    .expect("turn event count");
    assert_eq!(event_count, 1);
}
