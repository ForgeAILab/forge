use db::{
    create_sqlite_pool, run_migrations, AccountMainAgentBindingRepo, AgentActionPolicyResult,
    AgentActionRepo, AgentActionStatus, AgentChatRepo, AgentContextScopeRepo, AgentLcmEntryRecord,
    AgentLcmRepo, AgentRepo, AgentSessionRepo, CreateAccountMainAgentBinding, CreateAgent,
    CreateAgentAction, CreateAgentContextScope, CreateAgentIdentity, CreateAgentLcmTimeline,
    CreateAgentProfile, CreateAgentSession, CreateContextManifest, CreateContextManifestSource,
    CreateDomainEvent, CreateForgeMemorySourceBinding, DomainEventRepo, MemoryItem,
    ReplaceAccountMainAgentBinding, ScopedMemoryRepository, SqliteDb, User, UserRepo,
};

async fn database() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    SqliteDb::new(pool)
}

#[tokio::test]
async fn agent_chat_scope_flows_through_session_lcm_memory_manifest_and_action_repositories() {
    let db = database().await;
    let now = "2026-08-13T00:00:00.000Z";
    let account_id = "agent-chat-scope-account";
    let identity_id = "agent-chat-scope-identity";

    UserRepo::create_user(
        &db,
        &User {
            id: account_id.to_owned(),
            email: "agent-chat-scope@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: Some("Agent Chat Scope".to_owned()),
            is_admin: false,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("account creates");

    let identity = AgentRepo::create(
        &db,
        CreateAgent {
            id: identity_id.to_owned(),
            name: "Agent Chat Scope Agent".to_owned(),
            description: None,
            executor_type: "null".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: Some("scoped".to_owned()),
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: "idle".parse().expect("agent status"),
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some(account_id.to_owned()),
            visibility: "account".to_owned(),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("identity creates");

    let retired_context_scope = AgentContextScopeRepo::create_context_scope(
        &db,
        CreateAgentContextScope {
            id: "retired-context-scope".to_owned(),
            identity_id: identity.id.clone(),
            scope_type: "room".to_owned(),
            scope_id: "retired-room".to_owned(),
            project_id: None,
            task_id: None,
            task_role: None,
            workspace_access: "deny".to_owned(),
            authority_json: "{}".to_owned(),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await;
    assert!(retired_context_scope.is_err());

    let retired_lcm = sqlx::query(
        "INSERT INTO agent_lcm_timeline (
            id, identity_id, scope_type, scope_id, authorization_revision,
            revision, created_at, updated_at
         ) VALUES (?, ?, 'room', ?, ?, 0, ?, ?)",
    )
    .bind("retired-lcm-timeline")
    .bind(&identity.id)
    .bind("retired-room")
    .bind("retired-auth")
    .bind(now)
    .bind(now)
    .execute(db.pool())
    .await
    .expect_err("V075 rejects new Room-scoped LCM authority");
    assert!(retired_lcm
        .to_string()
        .contains("Room scopes are retired; use an Agent Chat scope"));

    let chat = AgentChatRepo::get_main_chat(&db, account_id)
        .await
        .expect("main chat lookup")
        .expect("main chat exists");

    let scope = AgentContextScopeRepo::create_context_scope(
        &db,
        CreateAgentContextScope {
            id: "agent-chat-context-scope".to_owned(),
            identity_id: identity.id.clone(),
            scope_type: "agent_chat".to_owned(),
            scope_id: chat.id.clone(),
            project_id: None,
            task_id: None,
            task_role: None,
            workspace_access: "deny".to_owned(),
            authority_json: "{\"source\":\"test\"}".to_owned(),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("Agent Chat context scope creates");
    assert_eq!(scope.scope_type, "agent_chat");
    assert_eq!(scope.scope_id, chat.id);

    let session = AgentSessionRepo::create_agent_session(
        &db,
        CreateAgentSession {
            id: "agent-chat-session".to_owned(),
            identity_id: identity.id.clone(),
            profile_id: identity.profile_id.clone(),
            context_scope_id: scope.id.clone(),
            backend_kind: "cli".to_owned(),
            runtime_session_id: None,
            status: "ready".to_owned(),
            capabilities_json: "{}".to_owned(),
            connection_status: "unknown".to_owned(),
            predecessor_session_id: None,
            last_activity_at: None,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("Agent Chat session creates");
    assert_eq!(session.context_scope_id, scope.id);

    let timeline = AgentLcmRepo::create_or_get_lcm_timeline(
        &db,
        CreateAgentLcmTimeline {
            id: "agent-chat-lcm".to_owned(),
            identity_id: identity.id.clone(),
            scope_type: "agent_chat".to_owned(),
            scope_id: chat.id.clone(),
            authorization_revision: "auth-1".to_owned(),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("Agent Chat LCM timeline creates");
    AgentLcmRepo::append_lcm_entries(
        &db,
        db::AppendAgentLcmEntries {
            timeline_id: timeline.id.clone(),
            expected_revision: 0,
            operation_id: "agent-chat-lcm-append".to_owned(),
            operation_fingerprint: "agent-chat-lcm-append-fingerprint".to_owned(),
            entries: vec![AgentLcmEntryRecord {
                timeline_id: timeline.id.clone(),
                entry_id: "agent-chat-lcm-entry".to_owned(),
                sequence: 0,
                content_json: "{\"text\":\"hello\"}".to_owned(),
                content_fingerprint: "entry-fingerprint".to_owned(),
                source_json: "{\"source\":\"agent_chat\"}".to_owned(),
                created_at: now.to_owned(),
            }],
            expected_sequence: 0,
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("Agent Chat LCM entry appends");

    let memory = MemoryItem {
        row_id: 0,
        id: "agent-chat-memory".to_owned(),
        project_id: None,
        task_id: None,
        execution_id: None,
        scope_type: "agent_chat".to_owned(),
        scope_id: chat.id.clone(),
        visibility: "chat".to_owned(),
        owner_identity_id: Some(identity.id.clone()),
        authority: "observation".to_owned(),
        sensitivity: "internal".to_owned(),
        retention_priority: 10,
        provenance_json: "{\"source\":\"agent_chat\"}".to_owned(),
        publication_source_id: None,
        supersedes_id: None,
        valid_from: None,
        valid_until: None,
        source_event_id: None,
        source_scope_type: Some("agent_chat".to_owned()),
        source_scope_id: Some(chat.id.clone()),
        source_revision: Some("1".to_owned()),
        source_type: "agent_chat_message".to_owned(),
        kind: "observation".to_owned(),
        title: "Agent Chat memory".to_owned(),
        summary: Some("Persisted in the Agent Chat scope".to_owned()),
        body: "The Agent Chat scope is durable.".to_owned(),
        metadata_json: "{\"source_ref\":\"agent-chat-message\"}".to_owned(),
        confidence: Some("confirmed".to_owned()),
        quality_score: Some(80),
        created_by_type: Some("agent".to_owned()),
        created_by_id: Some(identity.id.clone()),
        created_at: now.to_owned(),
    };
    let (stored_memory, inserted) = ScopedMemoryRepository::insert_memory_item_if_source_absent(
        &db,
        &memory,
        "agent_chat_message",
        "agent-chat-message",
    )
    .await
    .expect("Agent Chat memory creates");
    assert!(inserted);
    assert_eq!(stored_memory.scope_type, "agent_chat");

    let binding = ScopedMemoryRepository::create_memory_source_binding(
        &db,
        CreateForgeMemorySourceBinding {
            id: "agent-chat-memory-binding".to_owned(),
            identity_id: identity.id.clone(),
            context_scope_id: scope.id.clone(),
            scope_type: "agent_chat".to_owned(),
            scope_id: chat.id.clone(),
            account_id: Some(account_id.to_owned()),
            project_id: None,
            task_id: None,
            policy_revision: "policy-1".to_owned(),
            created_at: now.to_owned(),
        },
    )
    .await
    .expect("Agent Chat memory binding creates");
    assert_eq!(binding.scope_type, "agent_chat");

    let manifest = ScopedMemoryRepository::create_context_manifest(
        &db,
        CreateContextManifest {
            id: "agent-chat-manifest".to_owned(),
            identity_id: identity.id.clone(),
            agent_session_id: Some(session.id.clone()),
            context_scope_id: scope.id.clone(),
            scope_type: "agent_chat".to_owned(),
            scope_id: chat.id.clone(),
            policy_revision: "policy-1".to_owned(),
            domain_revision: "domain-1".to_owned(),
            lcm_binding_revision: Some("0".to_owned()),
            runtime_manifest_id: Some("runtime-manifest".to_owned()),
            runtime_manifest_fingerprint: Some("runtime-fingerprint".to_owned()),
            combined_fingerprint: "combined-fingerprint".to_owned(),
            request_fingerprint: "request-fingerprint".to_owned(),
            created_at: now.to_owned(),
        },
    )
    .await
    .expect("Agent Chat context manifest creates");
    ScopedMemoryRepository::append_context_manifest_source(
        &db,
        CreateContextManifestSource {
            manifest_id: manifest.id.clone(),
            ordinal: 0,
            source_id: stored_memory.id.clone(),
            source_type: "memory_item".to_owned(),
            source_revision: stored_memory.created_at.clone(),
            selection_reason: "same canonical Agent Chat scope".to_owned(),
            disposition: "included".to_owned(),
            retention_priority: 10,
            fragment_fingerprint: "fragment-fingerprint".to_owned(),
        },
    )
    .await
    .expect("Agent Chat manifest source appends");

    let action = AgentActionRepo::create_action(
        &db,
        CreateAgentAction {
            id: "agent-chat-action".to_owned(),
            actor_identity_id: identity.id.clone(),
            scope_type: "agent_chat".to_owned(),
            scope_id: chat.id.clone(),
            operation: "agent_chat.observe".to_owned(),
            payload_json: "{\"manifest_id\":\"agent-chat-manifest\"}".to_owned(),
            payload_hash: "action-payload-hash".to_owned(),
            dedupe_key: "agent-chat-action-dedupe".to_owned(),
            correlation_id: "agent-chat-correlation".to_owned(),
            causation_id: None,
            causation_depth: 0,
            requested_permission: "agent_chat.summary".to_owned(),
            policy_result: AgentActionPolicyResult::Allowed,
            policy_reason: Some("Agent Chat scope admission".to_owned()),
            status: AgentActionStatus::Proposed,
            target_type: None,
            target_id: None,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("Agent Chat action creates");
    assert_eq!(action.scope_type, "agent_chat");

    let event = DomainEventRepo::append_event(
        &db,
        CreateDomainEvent {
            id: "agent-chat-event".to_owned(),
            event_type: "agent_chat.memory.admitted".to_owned(),
            entity_type: "memory_item".to_owned(),
            entity_id: stored_memory.id,
            actor_type: "agent".to_owned(),
            actor_id: Some(identity.id),
            scope_type: "agent_chat".to_owned(),
            scope_id: chat.id,
            correlation_id: "agent-chat-correlation".to_owned(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some("agent-chat-event-dedupe".to_owned()),
            payload_json: "{\"manifest_id\":\"agent-chat-manifest\"}".to_owned(),
            created_at: now.to_owned(),
        },
    )
    .await
    .expect("Agent Chat event creates");
    assert_eq!(event.scope_type, "agent_chat");
}

#[tokio::test]
async fn legacy_agent_chat_scope_upgrades_to_frozen_authority_without_data_loss() {
    let db = database().await;
    let now = "2026-08-21T00:00:00.000Z";
    let account_id = "legacy-frozen-account";
    let identity_id = "legacy-frozen-identity";
    let profile_id = "legacy-frozen-profile";
    UserRepo::create_user(
        &db,
        &User {
            id: account_id.to_owned(),
            email: "legacy-frozen@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: Some("Legacy Frozen".to_owned()),
            is_admin: false,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("account creates");
    AgentRepo::create_identity_with_profile(
        &db,
        CreateAgentIdentity {
            id: identity_id.to_owned(),
            name: "Legacy Frozen Agent".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: "idle".parse().expect("identity status"),
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some(account_id.to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
        CreateAgentProfile {
            id: profile_id.to_owned(),
            identity_id: identity_id.to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("legacy-frozen".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("identity/profile creates");
    let chat = AgentChatRepo::get_main_chat(&db, account_id)
        .await
        .expect("Main Chat lookup")
        .expect("Main Chat exists");

    let legacy_authority = serde_json::json!({
        "issued_to_user_id": account_id,
        "revision": 1,
        "scope": {"chat_id": chat.id.clone(), "type": "agent_chat"},
        "legacy_marker": "retain-this-claim"
    });
    let scope = AgentContextScopeRepo::create_context_scope(
        &db,
        CreateAgentContextScope {
            id: "legacy-frozen-scope".to_owned(),
            identity_id: identity_id.to_owned(),
            scope_type: "agent_chat".to_owned(),
            scope_id: chat.id.clone(),
            project_id: None,
            task_id: None,
            task_role: None,
            workspace_access: "deny".to_owned(),
            authority_json: legacy_authority.to_string(),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("legacy context scope creates");
    assert_eq!(scope.version, 1);

    let frozen_authority = serde_json::json!({
        "issued_to_user_id": account_id,
        "revision": 1,
        "scope": {"chat_id": chat.id.clone(), "type": "agent_chat"},
        "frozen_binding_id": "binding-1",
        "frozen_binding_version": 1,
        "frozen_profile_id": profile_id,
        "frozen_profile_version": 1,
        "frozen_policy_revision": "default",
        "frozen_policy_digest": "policy-digest",
        "frozen_permission_policy_digest": "permission-digest",
        "frozen_tool_policy_digest": "tool-digest"
    });
    let reconciled = AgentContextScopeRepo::create_context_scope(
        &db,
        CreateAgentContextScope {
            id: "new-id-is-ignored-on-replay".to_owned(),
            identity_id: identity_id.to_owned(),
            scope_type: "agent_chat".to_owned(),
            scope_id: chat.id.clone(),
            project_id: None,
            task_id: None,
            task_role: None,
            workspace_access: "deny".to_owned(),
            authority_json: frozen_authority.to_string(),
            created_at: now.to_owned(),
            updated_at: "2026-08-21T00:00:01.000Z".to_owned(),
        },
    )
    .await
    .expect("legacy authority reconciles");
    assert_eq!(reconciled.id, scope.id);
    assert_eq!(reconciled.version, 2);
    let stored_authority: serde_json::Value =
        serde_json::from_str(&reconciled.authority_json).expect("stored authority JSON");
    assert_eq!(stored_authority["legacy_marker"], "retain-this-claim");
    assert_eq!(stored_authority["frozen_binding_id"], "binding-1");

    let replay = AgentContextScopeRepo::create_context_scope(
        &db,
        CreateAgentContextScope {
            id: "another-id-is-ignored-on-replay".to_owned(),
            identity_id: identity_id.to_owned(),
            scope_type: "agent_chat".to_owned(),
            scope_id: chat.id.clone(),
            project_id: None,
            task_id: None,
            task_role: None,
            workspace_access: "deny".to_owned(),
            authority_json: frozen_authority.to_string(),
            created_at: now.to_owned(),
            updated_at: "2026-08-21T00:00:02.000Z".to_owned(),
        },
    )
    .await
    .expect("frozen authority replay succeeds");
    assert_eq!(replay.id, scope.id);
    assert_eq!(replay.version, 2);

    let stable_authority = serde_json::json!({
        "issued_to_user_id": account_id,
        "revision": 1,
        "scope": {"chat_id": chat.id.clone(), "type": "agent_chat"},
    });
    let stable_replay = AgentContextScopeRepo::create_context_scope(
        &db,
        CreateAgentContextScope {
            id: "stable-authority-replay".to_owned(),
            identity_id: identity_id.to_owned(),
            scope_type: "agent_chat".to_owned(),
            scope_id: chat.id.clone(),
            project_id: None,
            task_id: None,
            task_role: None,
            workspace_access: "deny".to_owned(),
            authority_json: stable_authority.to_string(),
            created_at: now.to_owned(),
            updated_at: "2026-08-21T00:00:03.000Z".to_owned(),
        },
    )
    .await
    .expect("stable scope authority accepts a previously frozen row");
    assert_eq!(stable_replay.id, scope.id);
    assert_eq!(stable_replay.version, 2);

    let mut changed_authority = frozen_authority;
    changed_authority["frozen_binding_version"] = serde_json::json!(2);
    let conflict = AgentContextScopeRepo::create_context_scope(
        &db,
        CreateAgentContextScope {
            id: "conflicting-id".to_owned(),
            identity_id: identity_id.to_owned(),
            scope_type: "agent_chat".to_owned(),
            scope_id: chat.id,
            project_id: None,
            task_id: None,
            task_role: None,
            workspace_access: "deny".to_owned(),
            authority_json: changed_authority.to_string(),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await;
    assert!(conflict
        .expect_err("changed frozen authority must fail closed")
        .to_string()
        .contains("different authority"));
}

#[tokio::test]
async fn identity_profile_session_replacement_preserves_per_identity_chat_continuity() {
    let db = database().await;
    let now = "2026-08-13T00:00:00.000Z";
    let account_id = "continuity-account";
    UserRepo::create_user(
        &db,
        &User {
            id: account_id.to_owned(),
            email: "continuity@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: Some("Continuity".to_owned()),
            is_admin: false,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("continuity account creates");
    let chat = AgentChatRepo::get_main_chat(&db, account_id)
        .await
        .expect("continuity Main Chat lookup")
        .expect("continuity Main Chat exists");

    let mut identities = Vec::new();
    for (identity_id, profile_id, name) in [
        (
            "continuity-identity-a",
            "continuity-profile-a",
            "Continuity A",
        ),
        (
            "continuity-identity-b",
            "continuity-profile-b",
            "Continuity B",
        ),
    ] {
        let identity = AgentRepo::create_identity_with_profile(
            &db,
            CreateAgentIdentity {
                id: identity_id.to_owned(),
                name: name.to_owned(),
                description: None,
                max_concurrent_tasks: 1,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status: "idle".parse().expect("identity status"),
                last_heartbeat_at: None,
                is_default: false,
                paused: false,
                owner_id: Some(account_id.to_owned()),
                visibility: "account".to_owned(),
                account_permission_ceiling: "{}".to_owned(),
                created_at: now.to_owned(),
                updated_at: now.to_owned(),
            },
            CreateAgentProfile {
                id: profile_id.to_owned(),
                identity_id: identity_id.to_owned(),
                backend_kind: "native".to_owned(),
                executor_type: "embedded".to_owned(),
                provider: Some("test".to_owned()),
                model: Some("continuity-model".to_owned()),
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: "{}".to_owned(),
                tool_policy_json: "{}".to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: None,
                daemon_id: None,
                created_at: now.to_owned(),
                updated_at: now.to_owned(),
            },
        )
        .await
        .expect("continuity identity/profile creates");
        identities.push((identity.id, identity.profile_id));
    }

    let mut timeline_ids = Vec::new();
    for (index, (identity_id, profile_id)) in identities.iter().enumerate() {
        let scope = AgentContextScopeRepo::create_context_scope(
            &db,
            CreateAgentContextScope {
                id: format!("continuity-scope-{index}"),
                identity_id: identity_id.clone(),
                scope_type: "agent_chat".to_owned(),
                scope_id: chat.id.clone(),
                project_id: None,
                task_id: None,
                task_role: None,
                workspace_access: "deny".to_owned(),
                authority_json: format!("{{\"identity\":\"{identity_id}\"}}"),
                created_at: now.to_owned(),
                updated_at: now.to_owned(),
            },
        )
        .await
        .expect("continuity context scope creates");
        let session = AgentSessionRepo::create_agent_session(
            &db,
            CreateAgentSession {
                id: format!("continuity-session-{index}"),
                identity_id: identity_id.clone(),
                profile_id: profile_id.clone(),
                context_scope_id: scope.id,
                backend_kind: "native".to_owned(),
                runtime_session_id: Some(format!("runtime-{index}")),
                status: "ready".to_owned(),
                capabilities_json: "{}".to_owned(),
                connection_status: "healthy".to_owned(),
                predecessor_session_id: None,
                last_activity_at: Some(now.to_owned()),
                created_at: now.to_owned(),
                updated_at: now.to_owned(),
            },
        )
        .await
        .expect("continuity session creates");
        assert_eq!(session.identity_id, *identity_id);

        let timeline = AgentLcmRepo::create_or_get_lcm_timeline(
            &db,
            CreateAgentLcmTimeline {
                id: format!("continuity-timeline-{index}"),
                identity_id: identity_id.clone(),
                scope_type: "agent_chat".to_owned(),
                scope_id: chat.id.clone(),
                authorization_revision: format!("auth-{index}"),
                created_at: now.to_owned(),
                updated_at: now.to_owned(),
            },
        )
        .await
        .expect("continuity timeline creates");
        AgentLcmRepo::append_lcm_entries(
            &db,
            db::AppendAgentLcmEntries {
                timeline_id: timeline.id.clone(),
                expected_revision: 0,
                operation_id: format!("continuity-operation-{index}"),
                operation_fingerprint: format!("continuity-fingerprint-{index}"),
                entries: vec![AgentLcmEntryRecord {
                    timeline_id: timeline.id.clone(),
                    entry_id: format!("continuity-entry-{index}"),
                    sequence: 0,
                    content_json: format!("{{\"identity\":\"{identity_id}\"}}"),
                    content_fingerprint: format!("entry-fingerprint-{index}"),
                    source_json: format!("{{\"session\":\"{}\"}}", session.id),
                    created_at: now.to_owned(),
                }],
                expected_sequence: 0,
                updated_at: now.to_owned(),
            },
        )
        .await
        .expect("continuity LCM entry appends");
        timeline_ids.push(timeline.id);
    }

    let first_binding = AccountMainAgentBindingRepo::create_main_binding(
        &db,
        CreateAccountMainAgentBinding {
            id: "continuity-binding-a".to_owned(),
            account_id: account_id.to_owned(),
            identity_id: identities[0].0.clone(),
            profile_id: identities[0].1.clone(),
            autonomy_policy_json: "{}".to_owned(),
            tool_policy_revision: "continuity-policy".to_owned(),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("initial continuity binding creates");
    let replacement = AccountMainAgentBindingRepo::replace_main_binding(
        &db,
        ReplaceAccountMainAgentBinding {
            account_id: account_id.to_owned(),
            expected_version: first_binding.version,
            replacement: CreateAccountMainAgentBinding {
                id: "continuity-binding-b".to_owned(),
                account_id: account_id.to_owned(),
                identity_id: identities[1].0.clone(),
                profile_id: identities[1].1.clone(),
                autonomy_policy_json: "{}".to_owned(),
                tool_policy_revision: "continuity-policy".to_owned(),
                created_at: now.to_owned(),
                updated_at: now.to_owned(),
            },
            replacement_reason: Some("continuity replacement".to_owned()),
        },
    )
    .await
    .expect("continuity binding replacement commits");
    assert_eq!(replacement.identity_id, identities[1].0);

    let history = AccountMainAgentBindingRepo::list_main_binding_history(&db, account_id)
        .await
        .expect("continuity binding history loads");
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].identity_id, identities[0].0);
    assert_eq!(history[0].state, "replaced");
    assert_eq!(history[1].identity_id, identities[1].0);
    assert_eq!(history[1].state, "active");

    let chat_after = AgentChatRepo::get_main_chat(&db, account_id)
        .await
        .expect("Main Chat continuity lookup")
        .expect("Main Chat remains durable");
    assert_eq!(chat_after.id, chat.id);

    for (index, (identity_id, _)) in identities.iter().enumerate() {
        let timeline =
            AgentLcmRepo::get_lcm_timeline_for_binding(&db, identity_id, "agent_chat", &chat.id)
                .await
                .expect("identity timeline lookup")
                .expect("identity timeline remains bound");
        assert_eq!(timeline.id, timeline_ids[index]);
        let entries = AgentLcmRepo::list_lcm_entries(&db, &timeline.id, 0, 1, 10)
            .await
            .expect("identity timeline entries load");
        assert_eq!(entries.len(), 1);
        assert!(entries[0].content_json.contains(identity_id));
        assert!(entries[0]
            .source_json
            .contains(&format!("continuity-session-{index}")));
    }
    let timeline_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_lcm_timeline
         WHERE scope_type = 'agent_chat' AND scope_id = ?",
    )
    .bind(&chat.id)
    .fetch_one(db.pool())
    .await
    .expect("continuity timeline count");
    assert_eq!(timeline_count, 2);
}
