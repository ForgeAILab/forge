use std::sync::Arc;

use async_trait::async_trait;
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentChatRepo,
    AgentChatTurnJobRepo, AgentChatTurnState, AgentProfileRepo, AgentRepo, AgentStatus,
    AttentionRepo, ClaimDomainEvents, ClaimExecutionLease, CreateAgentIdentity, CreateAgentProfile,
    CreateDomainEvent, CreateExecution, CreateProject, DomainEventRepo, ExecutionLeaseDisposition,
    ExecutionRepo, ExecutionStatus, ProjectRepo, ResumePolicy, SelectAgentProfile, SqliteDb,
    StopReason, TerminalizeExecution, UpdateAgentChatTurnJob, User, UserRepo,
};
use services::{
    wake_attention_incident_digest, AgentChatService, AgentChatTurnRunner, AgentChatTurnWorker,
    AttentionService, CompletedAgentChatTurn, CreateAgentHandoffInput, SendAgentChatMessageInput,
    ServiceError, SetMainAgentBindingInput, SetProjectAgentBindingInput, WakeTurnConsumer,
};
use tokio_util::sync::CancellationToken;

async fn database() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    Arc::new(SqliteDb::new(pool))
}

async fn file_database() -> (Arc<SqliteDb>, String) {
    let path = format!("/tmp/forge-wake-turn-{}.sqlite", new_uuid_v4());
    let pool = create_sqlite_pool(&format!("sqlite://{path}"))
        .await
        .unwrap();
    run_migrations(&pool).await.unwrap();
    (Arc::new(SqliteDb::new(pool)), path)
}

async fn identity_with_profile(db: &SqliteDb, id: &str) -> String {
    let now = now_rfc3339();
    let profile_id = new_uuid_v4();
    AgentRepo::create_identity_with_profile(
        db,
        CreateAgentIdentity {
            id: id.to_owned(),
            name: "wake-turn-test".to_owned(),
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
            id: profile_id.clone(),
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
    profile_id
}

async fn owned_identity_with_profile(
    db: &SqliteDb,
    id: &str,
    owner_id: &str,
    profile_id: &str,
) -> String {
    let now = now_rfc3339();
    AgentRepo::create_identity_with_profile(
        db,
        CreateAgentIdentity {
            id: id.to_owned(),
            name: "chat-turn-test".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some(owner_id.to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: profile_id.to_owned(),
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
    profile_id.to_owned()
}

async fn select_profile(db: &SqliteDb, identity_id: &str, profile_id: &str, model: &str) -> String {
    let identity = AgentRepo::get_by_id(db, identity_id)
        .await
        .unwrap()
        .unwrap();
    let now = now_rfc3339();
    AgentProfileRepo::create_and_select_profile(
        db,
        CreateAgentProfile {
            id: profile_id.to_owned(),
            identity_id: identity_id.to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some(model.to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        SelectAgentProfile {
            identity_id: identity_id.to_owned(),
            profile_id: profile_id.to_owned(),
            expected_version: identity.version,
            updated_at: now,
        },
    )
    .await
    .unwrap();
    profile_id.to_owned()
}

struct ChatTurnFixture {
    db: Arc<SqliteDb>,
    account_id: String,
    project_id: String,
    chat_id: String,
    identity_id: String,
}

struct FailingWakeRunner;

#[async_trait]
impl AgentChatTurnRunner for FailingWakeRunner {
    async fn run_turn(
        &self,
        _job: &db::AgentChatTurnJob,
        _cancellation: CancellationToken,
    ) -> services::Result<CompletedAgentChatTurn> {
        Err(ServiceError::Conflict(
            "synthetic admitted-wake runner failure".to_owned(),
        ))
    }
}

struct ProseOnlyWakeRunner;

#[async_trait]
impl AgentChatTurnRunner for ProseOnlyWakeRunner {
    async fn run_turn(
        &self,
        job: &db::AgentChatTurnJob,
        _cancellation: CancellationToken,
    ) -> services::Result<CompletedAgentChatTurn> {
        Ok(CompletedAgentChatTurn {
            identity_id: job
                .responder_identity_id
                .clone()
                .expect("admitted identity"),
            profile_id: job.profile_id.clone().expect("admitted Profile"),
            session_id: "prose-only-wake-session".to_owned(),
            model: Some("test".to_owned()),
            content: "Everything is complete and ready to release.".to_owned(),
            token_usage_json: None,
            duration_ms: 1,
            context_manifest_id: None,
            pending_interaction_id: None,
        })
    }
}

async fn chat_turn_fixture() -> ChatTurnFixture {
    let db = database().await;
    let account_id = new_uuid_v4();
    let now = now_rfc3339();
    UserRepo::create_user(
        &*db,
        &User {
            id: account_id.clone(),
            email: format!("{account_id}@example.test"),
            password_hash: "test".to_owned(),
            display_name: Some("Chat Turn Test".to_owned()),
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();

    let project_id = new_uuid_v4();
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: project_id.clone(),
            name: "chat-turn-project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(account_id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    // ProjectRepo creates the canonical chat/binding, but membership is a
    // separate durable record used by the chat service's authorization gate.
    sqlx::query(
        "INSERT INTO project_member (id, project_id, user_id, role, created_at, updated_at)
         VALUES (?, ?, ?, 'owner', ?, ?)",
    )
    .bind(new_uuid_v4())
    .bind(&project_id)
    .bind(&account_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .unwrap();

    let identity_id = new_uuid_v4();
    let profile_id = new_uuid_v4();
    owned_identity_with_profile(&db, &identity_id, &account_id, &profile_id).await;

    let chats = AgentChatService::new(Arc::clone(&db));
    chats
        .set_main_binding(SetMainAgentBindingInput {
            actor_user_id: account_id.clone(),
            account_id: account_id.clone(),
            identity_id: identity_id.clone(),
            autonomy_policy_json: "{}".to_owned(),
            tool_policy_revision: "test".to_owned(),
            expected_version: None,
            replacement_reason: None,
        })
        .await
        .unwrap();
    let setup_binding_version: (i64,) = sqlx::query_as(
        "SELECT version FROM project_agent_binding
         WHERE project_id = ? AND state = 'agent_setup_required'",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    chats
        .set_project_binding(SetProjectAgentBindingInput {
            actor_user_id: account_id.clone(),
            project_id: project_id.clone(),
            identity_id: Some(identity_id.clone()),
            state: "active".to_owned(),
            autonomy_policy_json: "{}".to_owned(),
            permission_ceiling_json: "{}".to_owned(),
            subscriptions_json: "[]".to_owned(),
            wake_budget: 10,
            expected_version: Some(setup_binding_version.0),
            replacement_reason: None,
        })
        .await
        .unwrap();
    let chat_id = AgentChatRepo::get_project_chat(&*db, &project_id)
        .await
        .unwrap()
        .unwrap()
        .id;

    ChatTurnFixture {
        db,
        account_id,
        project_id,
        chat_id,
        identity_id,
    }
}

/// Insert a Project (the schema trigger creates its chat and setup binding),
/// promote the chat to ready, and bind the identity as the active responder.
async fn bound_project(db: &SqliteDb, identity_id: &str, profile_id: &str) -> (String, String) {
    let project_id = new_uuid_v4();
    let now = now_rfc3339();
    sqlx::query("INSERT INTO project (id, name, created_at, updated_at) VALUES (?, ?, ?, ?)")
        .bind(&project_id)
        .bind("wake-turn-project")
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .unwrap();
    let chat_id: String =
        sqlx::query_scalar("SELECT id FROM agent_chat WHERE kind = 'project' AND project_id = ?")
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    sqlx::query("UPDATE agent_chat SET status = 'ready' WHERE id = ?")
        .bind(&chat_id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE project_agent_binding
         SET identity_id = ?, profile_id = ?, state = 'active'
             , operating_skill_revision_id = (
                 SELECT id FROM operating_skill_revision
                 WHERE skill_key = 'forge.project.orchestration/v1'
                 ORDER BY revision DESC LIMIT 1
             ), policy_revision = 'test-policy', policy_digest = 'test-policy-digest'
         WHERE project_id = ?",
    )
    .bind(identity_id)
    .bind(profile_id)
    .bind(&project_id)
    .execute(db.pool())
    .await
    .unwrap();
    (project_id, chat_id)
}

async fn append_event(db: &SqliteDb, event: CreateDomainEvent) {
    DomainEventRepo::append_event(db, event).await.unwrap();
}

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

    let wake_consumer = WakeTurnConsumer::new(Arc::clone(&fixture.db), "provenance-wake");
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
    append_event(
        &fixture.db,
        wake_event_for_attention(
            &fixture.db,
            &fixture.identity_id,
            &fixture.project_id,
            &incident_key,
        )
        .await,
    )
    .await;
    wake_consumer.run_once(100).await.unwrap();
    let wake_turn = AgentChatTurnJobRepo::get_agent_chat_turn_job(
        &*fixture.db,
        &sqlx::query_scalar::<_, String>(
            "SELECT id FROM agent_chat_turn_job
             WHERE chat_id = ? AND dedupe_key LIKE 'wake-turn:%'",
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
async fn wake_turn_resolves_identity_current_profile_after_profile_edit() {
    let fixture = chat_turn_fixture().await;
    let consumer = WakeTurnConsumer::new(Arc::clone(&fixture.db), "characterization-wake-profile");
    consumer.run_once(100).await.unwrap();

    let incident_key = format!("attention:profile_edit:project:{}", fixture.project_id);
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
            dedupe_key: Some(format!("characterization-profile-source:{source_event}")),
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
         ) VALUES (?, 'execution_failed', 'project', ?, NULL, ?, 85, 'open',
                   'Profile edit characterization', ?, ?, ?, ?, 'inspect_run')",
    )
    .bind(new_uuid_v4())
    .bind(&fixture.project_id)
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

    // The binding still names the same identity, but its Profile snapshot is
    // now stale. Manual admission uses this newly selected Profile already.
    let current_profile = select_profile(
        &fixture.db,
        &fixture.identity_id,
        &new_uuid_v4(),
        "wake-profile-after-edit",
    )
    .await;
    append_event(
        &fixture.db,
        wake_event_for_attention(
            &fixture.db,
            &fixture.identity_id,
            &fixture.project_id,
            &incident_key,
        )
        .await,
    )
    .await;
    let run = consumer.run_once(100).await.unwrap();
    assert_eq!(run.delivered_turns, 1);

    let profile: String = sqlx::query_scalar(
        "SELECT profile_id FROM agent_chat_turn_job
         WHERE chat_id = ? AND dedupe_key LIKE 'wake-turn:%'",
    )
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
    // The binding's profile snapshot is intentionally stale by the time the
    // alternate producer emits its event. Admission must follow the identity
    // to its current selected Profile and ignore responder fields in payload.
    let current_profile_id = select_profile(
        &db,
        &identity_id,
        &new_uuid_v4(),
        "current-wake-producer-profile",
    )
    .await;
    let consumer = WakeTurnConsumer::new(Arc::clone(&db), "alternate-producer-consumer");
    consumer.run_once(100).await.unwrap();

    let incident_key = format!("attention:alternate_producer:project:{project_id}");
    let source_event = new_uuid_v4();
    append_event(
        &db,
        CreateDomainEvent {
            id: source_event.clone(),
            event_type: "execution.failed".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: new_uuid_v4(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: source_event.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!("alternate-producer-source:{source_event}")),
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
                   'Alternate producer incident', ?, ?, ?, ?, 'inspect_run')",
    )
    .bind(new_uuid_v4())
    .bind(&project_id)
    .bind(&identity_id)
    .bind(&source_event)
    .bind(
        serde_json::json!({
            "scope_type": "project",
            "scope_id": project_id,
        })
        .to_string(),
    )
    .bind(&incident_key)
    .bind(now_rfc3339())
    .bind(now_rfc3339())
    .execute(db.pool())
    .await
    .unwrap();
    let mut wake =
        wake_event_for_attention(&db, "spoofed-identity", &project_id, &incident_key).await;
    let mut payload: serde_json::Value = serde_json::from_str(&wake.payload_json).unwrap();
    payload["responder_identity_id"] = serde_json::json!("spoofed-identity");
    payload["responder_profile_id"] = serde_json::json!("spoofed-profile");
    wake.payload_json = payload.to_string();
    append_event(&db, wake).await;

    let run = consumer.run_once(100).await.unwrap();
    assert_eq!(run.delivered_turns, 1);
    let (responder, profile): (String, String) = sqlx::query_as(
        "SELECT responder_identity_id, profile_id
         FROM agent_chat_turn_job
         WHERE chat_id = ? AND dedupe_key LIKE 'wake-turn:%'",
    )
    .bind(&chat_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(responder, identity_id);
    assert_eq!(profile, current_profile_id);
}

async fn wake_event_for_attention(
    db: &SqliteDb,
    identity_id: &str,
    project_id: &str,
    incident_key: &str,
) -> CreateDomainEvent {
    wake_event_for_attention_in_scope(db, identity_id, project_id, incident_key).await
}

async fn append_project_attention_wake(
    db: &SqliteDb,
    identity_id: &str,
    project_id: &str,
    incident_key: &str,
) -> String {
    let source_event = new_uuid_v4();
    append_event(
        db,
        CreateDomainEvent {
            id: source_event.clone(),
            event_type: "execution.failed".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: new_uuid_v4(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: project_id.to_owned(),
            correlation_id: source_event.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!("wake-test-source:{source_event}")),
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
                   'Wake test incident', ?, ?, ?, ?, 'inspect_run')",
    )
    .bind(new_uuid_v4())
    .bind(project_id)
    .bind(identity_id)
    .bind(&source_event)
    .bind(
        serde_json::json!({
            "scope_type": "project",
            "scope_id": project_id,
            "state": "initial",
        })
        .to_string(),
    )
    .bind(incident_key)
    .bind(now_rfc3339())
    .bind(now_rfc3339())
    .execute(db.pool())
    .await
    .unwrap();
    let wake_event = wake_event_for_attention(db, identity_id, project_id, incident_key).await;
    let wake_event_id = wake_event.id.clone();
    append_event(db, wake_event).await;
    wake_event_id
}

/// One milestone whose acceptance matrix an Agent is expected to settle.
/// Written as fixture rows rather than through the command services so the
/// wake tests exercise delivery, not milestone authoring.
struct DeliveryMilestoneFixture {
    milestone_id: String,
    milestone_revision_id: String,
    agent_check_id: String,
    manual_check_id: String,
}

async fn seed_delivery_milestone(db: &SqliteDb, project_id: &str) -> DeliveryMilestoneFixture {
    let now = now_rfc3339();
    let user_id = new_uuid_v4();
    UserRepo::create_user(
        db,
        &User {
            id: user_id.clone(),
            email: format!("{user_id}@example.test"),
            password_hash: "test".to_owned(),
            display_name: Some("Delivery Fixture".to_owned()),
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    // The Charter's account and the Project's owner are the same principal;
    // the schema enforces it.
    sqlx::query("UPDATE project SET owner_id = ? WHERE id = ?")
        .bind(&user_id)
        .bind(project_id)
        .execute(db.pool())
        .await
        .unwrap();
    let charter_id = new_uuid_v4();
    let charter_revision_id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO project_charter
            (id, account_id, genesis_session_id, project_id,
             current_draft_revision_id, current_approved_revision_id,
             project_mode, maturity, lifecycle, version, created_at, updated_at)
         VALUES (?, ?, NULL, ?, NULL, NULL, 'compact', 'mvp', 'attached', 1, ?, ?)",
    )
    .bind(&charter_id)
    .bind(&user_id)
    .bind(project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO project_charter_revision
            (id, charter_id, revision, base_revision, base_revision_id,
             lifecycle, schema_version, render_version, content_json,
             rendered_view, change_summary, author_type, author_id,
             source_message_id, source_turn_job_id, source_refs_json,
             content_digest, rendered_digest, created_at)
         VALUES (?, ?, 1, 0, NULL, 'approved', 'charter@1', 'render@1', '{}',
                 '# Charter', 'fixture', 'user', ?, NULL, NULL, '[]',
                 'charter-content', 'charter-rendered', ?)",
    )
    .bind(&charter_revision_id)
    .bind(&charter_id)
    .bind(&user_id)
    .bind(&now)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE project_charter SET current_approved_revision_id = ? WHERE id = ?")
        .bind(&charter_revision_id)
        .bind(&charter_id)
        .execute(db.pool())
        .await
        .unwrap();

    let milestone_id = new_uuid_v4();
    let milestone_revision_id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO project_milestone
            (id, project_id, milestone_sequence, milestone_key, display_label,
             lifecycle, blocker_reason_json, stale_reason_json,
             reconciliation_reason_json, version, created_at, updated_at)
         VALUES (?, ?, 1, 'M001', 'Delivery milestone', 'active', '[]', '[]',
                 '[]', 3, ?, ?)",
    )
    .bind(&milestone_id)
    .bind(project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO project_milestone_revision
            (id, milestone_id, revision, base_revision, base_revision_id,
             lifecycle, display_label, outcome, included_scope_json,
             excluded_scope_json, charter_revision_id, document_revisions_json,
             task_selection_json, dependencies_json, risks_json,
             acceptance_checks_json, evidence_requirements_json,
             known_issues_json, change_summary, schema_version, render_version,
             rendered_view, content_digest, rendered_digest, author_type,
             author_id, source_refs_json, created_at)
         VALUES (?, ?, 1, 0, NULL, 'approved', 'Delivery milestone',
                 'The delivery outcome is exercised end to end', '[]', '[]',
                 ?, '[]', '[]', '[]', '[]', '[]', '[]', '[]', 'fixture',
                 'milestone@1', 'milestone-render@1', '# Milestone',
                 'milestone-content', 'milestone-rendered', 'user', ?, '[]', ?)",
    )
    .bind(&milestone_revision_id)
    .bind(&milestone_id)
    .bind(&charter_revision_id)
    .bind(&user_id)
    .bind(&now)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE project_milestone SET current_definition_revision_id = ? WHERE id = ?")
        .bind(&milestone_revision_id)
        .bind(&milestone_id)
        .execute(db.pool())
        .await
        .unwrap();

    let agent_check_id = "ac-integrated-flow".to_owned();
    let manual_check_id = "ac-user-judgment".to_owned();
    for (check_id, source_kind) in [
        (agent_check_id.as_str(), "task_validation"),
        (manual_check_id.as_str(), "manual"),
    ] {
        sqlx::query(
            "INSERT INTO project_milestone_check
                (id, project_id, milestone_id, definition_revision_id, check_key,
                 description, required, source_kind, expected_result,
                 evidence_required, version, current_result_id, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, 'fixture check', 1, ?, 'passes', 0, 1, NULL, ?, ?)",
        )
        .bind(check_id)
        .bind(project_id)
        .bind(&milestone_id)
        .bind(&milestone_revision_id)
        .bind(check_id)
        .bind(source_kind)
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .unwrap();
    }

    DeliveryMilestoneFixture {
        milestone_id,
        milestone_revision_id,
        agent_check_id,
        manual_check_id,
    }
}

/// Bind one Task to the milestone in the state a delivery follow-up sees.
async fn seed_governed_task(
    db: &SqliteDb,
    project_id: &str,
    milestone_id: &str,
    status: &str,
) -> String {
    let now = now_rfc3339();
    let task_id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO task (id, project_id, title, description, status, priority,
                           created_at, updated_at)
         VALUES (?, ?, 'Delivery task', 'fixture', ?, 0, ?, ?)",
    )
    .bind(&task_id)
    .bind(project_id)
    .bind(status)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO project_task_governance
            (task_id, project_id, charter_revision_id, plan_item_id, milestone_id,
             document_revisions_json, capability_class, risk_class, runnable,
             replacement_of_task_id, provenance_json, version, created_at, updated_at)
         VALUES (?, ?, NULL, NULL, ?, '[]', NULL, NULL, 0, NULL,
                 '{}', 1, ?, ?)",
    )
    .bind(&task_id)
    .bind(project_id)
    .bind(milestone_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .unwrap();
    task_id
}

/// Give one acceptance check a current authoritative result.
async fn settle_check(
    db: &SqliteDb,
    project_id: &str,
    fixture: &DeliveryMilestoneFixture,
    check_id: &str,
    outcome: &str,
) {
    let source_kind: String =
        sqlx::query_scalar("SELECT source_kind FROM project_milestone_check WHERE id = ?")
            .bind(check_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    let now = now_rfc3339();
    let result_id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO project_milestone_check_result
            (id, project_id, milestone_id, check_id, definition_revision_id,
             outcome, source_kind, source_manifest_json, input_digest,
             governing_charter_revision_id,
             principal_type, principal_id, authorization_basis,
             authorization_action, authorization_occurred_at, expected_version,
             explicit_event, idempotency_key, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, '{}', 'digest', NULL,
                 'agent', 'fixture-agent', 'project_agent_binding_policy',
                 'project.validation.record', ?, 1, ?, ?, ?)",
    )
    .bind(&result_id)
    .bind(project_id)
    .bind(&fixture.milestone_id)
    .bind(check_id)
    .bind(&fixture.milestone_revision_id)
    .bind(outcome)
    .bind(&source_kind)
    .bind(&now)
    .bind(new_uuid_v4())
    .bind(new_uuid_v4())
    .bind(&now)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE project_milestone_check SET current_result_id = ? WHERE id = ?")
        .bind(&result_id)
        .bind(check_id)
        .execute(db.pool())
        .await
        .unwrap();
}

async fn append_project_delivery_attention_wake(
    db: &SqliteDb,
    identity_id: &str,
    project_id: &str,
    incident_key: &str,
    task_id: &str,
) -> String {
    let source_event = new_uuid_v4();
    append_event(
        db,
        CreateDomainEvent {
            id: source_event.clone(),
            event_type: "task.completed".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: task_id.to_owned(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: project_id.to_owned(),
            correlation_id: source_event.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!("delivery-wake-test-source:{source_event}")),
            payload_json: r#"{"to_state":"done"}"#.to_owned(),
            created_at: now_rfc3339(),
        },
    )
    .await;
    sqlx::query(
        "INSERT INTO attention_projection (
            id, attention_type, scope_type, scope_id, identity_id, source_event_id,
            priority, status, summary, details_json, dedupe_key, occurred_at,
            updated_at, recommended_action, source_sequence
         ) VALUES (?, 'delivery_followup', 'project', ?, ?, ?, 70, 'open',
                   'Task completed; reconcile validation, evidence, and readiness',
                   ?, ?, ?, ?, 'reconcile_delivery',
                   (SELECT sequence FROM domain_event WHERE id = ?))",
    )
    .bind(new_uuid_v4())
    .bind(project_id)
    .bind(identity_id)
    .bind(&source_event)
    .bind(
        serde_json::json!({
            "scope_type": "project",
            "scope_id": project_id,
            "entity_type": "task",
            "entity_id": task_id,
        })
        .to_string(),
    )
    .bind(incident_key)
    .bind(now_rfc3339())
    .bind(now_rfc3339())
    .bind(&source_event)
    .execute(db.pool())
    .await
    .unwrap();
    let wake_event = wake_event_for_attention(db, identity_id, project_id, incident_key).await;
    let wake_event_id = wake_event.id.clone();
    append_event(db, wake_event).await;
    wake_event_id
}

async fn wake_event_for_attention_in_scope(
    db: &SqliteDb,
    identity_id: &str,
    event_scope_project_id: &str,
    incident_key: &str,
) -> CreateDomainEvent {
    let attention_id: String =
        sqlx::query_scalar("SELECT id FROM attention_projection WHERE dedupe_key = ?")
            .bind(incident_key)
            .fetch_one(db.pool())
            .await
            .unwrap();
    let attention = AttentionRepo::get_attention(db, &attention_id)
        .await
        .unwrap()
        .unwrap();
    let event_id = new_uuid_v4();
    CreateDomainEvent {
        id: event_id.clone(),
        event_type: "agent.wake.admitted".to_owned(),
        entity_type: "agent_wake".to_owned(),
        entity_id: incident_key.to_owned(),
        actor_type: "attention_projection".to_owned(),
        actor_id: None,
        scope_type: "project".to_owned(),
        scope_id: event_scope_project_id.to_owned(),
        correlation_id: event_id.clone(),
        causation_id: None,
        causation_depth: 1,
        dedupe_key: Some(format!("test-wake-admitted:{event_id}")),
        payload_json: serde_json::json!({
            "decision": "admitted",
            "identity_id": identity_id,
            "scope_type": "project",
            "scope_id": event_scope_project_id,
            "incident_key": incident_key,
            "incident_digest": wake_attention_incident_digest(&attention),
            "attention_id": attention.id,
            "reaction_depth": 0,
        })
        .to_string(),
        created_at: attention.occurred_at,
    }
}

#[tokio::test]
async fn admitted_wake_becomes_a_project_agent_turn() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;

    // Arm both consumers before any event exists; the migration-installed
    // cutover cursor is already authoritative for this consumer.
    let consumer = WakeTurnConsumer::new(Arc::clone(&db), "test-lease-owner");
    consumer.run_once(100).await.unwrap();
    let replay = WakeTurnConsumer::new(Arc::clone(&db), "other-lease-owner")
        .with_consumer_name("agent-wake-turns-replay");
    replay.run_once(100).await.unwrap();

    let incident_key = format!("attention:execution_failed:project:{project_id}:task:task-1");
    sqlx::query(
        "INSERT INTO attention_projection (
            id, attention_type, scope_type, scope_id, identity_id, source_event_id,
            priority, status, summary, details_json, dedupe_key, occurred_at,
            updated_at, recommended_action
         ) VALUES (?, 'execution_failed', 'project', ?, NULL, ?, 85, 'open',
                   'Task execution failed', ?, ?, ?, ?, 'inspect_run')",
    )
    .bind(new_uuid_v4())
    .bind(&project_id)
    .bind({
        let source = new_uuid_v4();
        append_event(
            &db,
            CreateDomainEvent {
                id: source.clone(),
                event_type: "execution.failed".to_owned(),
                entity_type: "task".to_owned(),
                entity_id: "task-1".to_owned(),
                actor_type: "system".to_owned(),
                actor_id: None,
                scope_type: "project".to_owned(),
                scope_id: project_id.clone(),
                correlation_id: source.clone(),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: Some(format!("execution-terminal:{}:failed", new_uuid_v4())),
                payload_json: "{}".to_owned(),
                created_at: now_rfc3339(),
            },
        )
        .await;
        source
    })
    .bind(
        serde_json::json!({
            "scope_type": "project",
            "scope_id": project_id,
        })
        .to_string(),
    )
    .bind(&incident_key)
    .bind(now_rfc3339())
    .bind(now_rfc3339())
    .execute(db.pool())
    .await
    .unwrap();

    append_event(
        &db,
        wake_event_for_attention(&db, &identity_id, &project_id, &incident_key).await,
    )
    .await;

    let run = consumer.run_once(100).await.unwrap();
    assert!(run.delivered_turns >= 1, "wake must deliver a turn");

    let (turn_count, status, responder): (i64, String, String) = sqlx::query_as(
        "SELECT COUNT(*), MAX(status), MAX(responder_identity_id)
         FROM agent_chat_turn_job WHERE chat_id = ? AND dedupe_key LIKE 'wake-turn:%'",
    )
    .bind(&chat_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(turn_count, 1);
    assert_eq!(status, "queued");
    assert_eq!(responder, identity_id);

    let message: (String, String) = sqlx::query_as(
        "SELECT author_type, content FROM agent_chat_message
         WHERE chat_id = ? ORDER BY sequence DESC LIMIT 1",
    )
    .bind(&chat_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(message.0, "system");
    assert!(message.1.contains("Task execution failed"));
    assert!(message.1.contains("inspect_run"));

    // Replay by a second consumer instance must not create a second turn.
    replay.run_once(100).await.unwrap();
    let turn_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_chat_turn_job WHERE chat_id = ? AND dedupe_key LIKE 'wake-turn:%'",
    )
    .bind(&chat_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(turn_count, 1, "replay must reuse the deduped turn");
}

/// The wake that fires when the last Task finishes is the moment validation is
/// owed. It has to hand the Agent the exact ids to record against and require
/// the record itself -- readiness evaluated first can only re-report the same
/// missing results, which is what a delivery follow-up used to do forever.
#[tokio::test]
async fn delivery_followup_with_all_tasks_done_orders_validation_before_readiness() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    let fixture = seed_delivery_milestone(&db, &project_id).await;
    let task_id = seed_governed_task(&db, &project_id, &fixture.milestone_id, "done").await;
    let consumer = WakeTurnConsumer::new(Arc::clone(&db), "delivery-validation-consumer");
    consumer.run_once(100).await.unwrap();

    let incident_key = format!("attention:delivery_followup:project:{project_id}:task:done");
    let wake_event_id = append_project_delivery_attention_wake(
        &db,
        &identity_id,
        &project_id,
        &incident_key,
        &task_id,
    )
    .await;
    assert_eq!(consumer.run_once(100).await.unwrap().delivered_turns, 1);

    let (content, source_metadata_json): (String, String) = sqlx::query_as(
        "SELECT message.content, message.source_metadata_json
         FROM agent_chat_turn_job AS job
         JOIN agent_chat_message AS message ON message.id = job.triggering_message_id
         WHERE job.chat_id = ? AND job.dedupe_key = ?",
    )
    .bind(&chat_id)
    .bind(format!("wake-turn:{wake_event_id}"))
    .fetch_one(db.pool())
    .await
    .unwrap();

    assert!(content.contains("every Task bound to it is done"));
    assert!(content.contains(&format!("milestone_id={}", fixture.milestone_id)));
    assert!(content.contains("milestone_version=3"));
    assert!(content.contains(&format!(
        "definition_revision_id={}",
        fixture.milestone_revision_id
    )));
    assert!(content.contains("`project.validation` (action `record`)"));
    assert!(
        content.contains(&fixture.agent_check_id),
        "the Agent-settleable check must be named"
    );
    assert!(
        content.contains(&fixture.manual_check_id),
        "the user-attested check must be named as the user's"
    );
    assert!(content.contains("you may never record one yourself"));

    let source_metadata: serde_json::Value = serde_json::from_str(&source_metadata_json).unwrap();
    assert_eq!(
        source_metadata["turn_postcondition"]["required_event_type"],
        "project.milestone.check.recorded",
        "the turn owes the validation record, not a readiness evaluation"
    );

    // Once the Agent-settleable check has an authoritative result, the same
    // wake shape asks for readiness instead.
    settle_check(
        &db,
        &project_id,
        &fixture,
        &fixture.agent_check_id,
        "passed",
    )
    .await;
    let second_incident = format!("attention:delivery_followup:project:{project_id}:task:done:2");
    let second_wake = append_project_delivery_attention_wake(
        &db,
        &identity_id,
        &project_id,
        &second_incident,
        &task_id,
    )
    .await;
    assert_eq!(consumer.run_once(100).await.unwrap().delivered_turns, 1);
    let (second_content, second_metadata): (String, String) = sqlx::query_as(
        "SELECT message.content, message.source_metadata_json
         FROM agent_chat_turn_job AS job
         JOIN agent_chat_message AS message ON message.id = job.triggering_message_id
         WHERE job.chat_id = ? AND job.dedupe_key = ?",
    )
    .bind(&chat_id)
    .bind(format!("wake-turn:{second_wake}"))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert!(!second_content.contains(&format!(
        "Settle yourself, in this turn: {}",
        fixture.agent_check_id
    )));
    assert!(second_content.contains(&fixture.manual_check_id));
    let second_metadata: serde_json::Value = serde_json::from_str(&second_metadata).unwrap();
    assert_eq!(
        second_metadata["turn_postcondition"]["required_event_type"],
        "milestone.readiness.evaluated",
        "with nothing left to record, readiness is what the turn owes"
    );
}

/// A milestone with open Tasks still names its outstanding checks, but says so
/// honestly instead of claiming the delivery is finished.
#[tokio::test]
async fn delivery_followup_reports_open_tasks_without_claiming_completion() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    let fixture = seed_delivery_milestone(&db, &project_id).await;
    let done_task = seed_governed_task(&db, &project_id, &fixture.milestone_id, "done").await;
    seed_governed_task(&db, &project_id, &fixture.milestone_id, "in_progress").await;
    let consumer = WakeTurnConsumer::new(Arc::clone(&db), "delivery-open-task-consumer");
    consumer.run_once(100).await.unwrap();

    let incident_key = format!("attention:delivery_followup:project:{project_id}:task:done");
    let wake_event_id = append_project_delivery_attention_wake(
        &db,
        &identity_id,
        &project_id,
        &incident_key,
        &done_task,
    )
    .await;
    assert_eq!(consumer.run_once(100).await.unwrap().delivered_turns, 1);
    let content: String = sqlx::query_scalar(
        "SELECT message.content
         FROM agent_chat_turn_job AS job
         JOIN agent_chat_message AS message ON message.id = job.triggering_message_id
         WHERE job.chat_id = ? AND job.dedupe_key = ?",
    )
    .bind(&chat_id)
    .bind(format!("wake-turn:{wake_event_id}"))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert!(content.contains("1 Task(s) still open"));
    assert!(!content.contains("every Task bound to it is done"));
}
#[tokio::test]
async fn delivery_followup_requires_newer_readiness_before_turn_success() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    // Every acceptance check already settled, so readiness is what this
    // delivery still owes.
    let fixture = seed_delivery_milestone(&db, &project_id).await;
    settle_check(
        &db,
        &project_id,
        &fixture,
        &fixture.agent_check_id,
        "passed",
    )
    .await;
    settle_check(
        &db,
        &project_id,
        &fixture,
        &fixture.manual_check_id,
        "passed",
    )
    .await;
    let task_id = seed_governed_task(&db, &project_id, &fixture.milestone_id, "done").await;
    let consumer = WakeTurnConsumer::new(Arc::clone(&db), "delivery-postcondition-consumer");
    consumer.run_once(100).await.unwrap();

    let incident_key = format!("attention:delivery_followup:project:{project_id}:task:done");
    let wake_event_id = append_project_delivery_attention_wake(
        &db,
        &identity_id,
        &project_id,
        &incident_key,
        &task_id,
    )
    .await;
    let wake_event_sequence: i64 =
        sqlx::query_scalar("SELECT sequence FROM domain_event WHERE id = ?")
            .bind(&wake_event_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    let admitted = consumer.run_once(100).await.unwrap();
    assert_eq!(admitted.delivered_turns, 1);

    let (turn_id, content, source_metadata_json): (String, String, String) = sqlx::query_as(
        "SELECT job.id, message.content, message.source_metadata_json
         FROM agent_chat_turn_job AS job
         JOIN agent_chat_message AS message ON message.id = job.triggering_message_id
         WHERE job.chat_id = ? AND job.dedupe_key = ?",
    )
    .bind(&chat_id)
    .bind(format!("wake-turn:{wake_event_id}"))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert!(content
        .contains("Every required acceptance check already has a current authoritative result"));
    assert!(content.contains("project.readiness"));
    let source_metadata: serde_json::Value = serde_json::from_str(&source_metadata_json).unwrap();
    assert_eq!(
        source_metadata["turn_postcondition"]["schema_version"],
        "forge.delivery-followup-postcondition/v1"
    );
    assert_eq!(
        source_metadata["turn_postcondition"]["after_event_sequence"],
        wake_event_sequence
    );

    let worker = AgentChatTurnWorker::with_runner(
        Arc::clone(&db),
        Arc::new(ProseOnlyWakeRunner) as Arc<dyn AgentChatTurnRunner>,
    );
    assert_eq!(worker.run_once().await.unwrap(), 1);
    let first = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*db, &turn_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.status, AgentChatTurnState::RetryWait);
    assert_eq!(
        first.error_code.as_deref(),
        Some("delivery_followup_postcondition_failed")
    );
    let agent_responses: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_chat_message
         WHERE chat_id = ? AND author_type = 'agent'",
    )
    .bind(&chat_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        agent_responses, 0,
        "prose-only success must not be committed"
    );

    let readiness_event_id = new_uuid_v4();
    append_event(
        &db,
        CreateDomainEvent {
            id: readiness_event_id.clone(),
            event_type: "milestone.readiness.evaluated".to_owned(),
            entity_type: "project_milestone".to_owned(),
            entity_id: "milestone-test".to_owned(),
            actor_type: "project_agent".to_owned(),
            actor_id: Some(identity_id),
            scope_type: "project".to_owned(),
            scope_id: project_id,
            correlation_id: readiness_event_id.clone(),
            causation_id: Some(turn_id.clone()),
            causation_depth: 1,
            dedupe_key: Some(format!("delivery-readiness:{readiness_event_id}")),
            payload_json: r#"{"result":"blocked"}"#.to_owned(),
            created_at: now_rfc3339(),
        },
    )
    .await;
    sqlx::query(
        "UPDATE agent_chat_turn_job
         SET next_attempt_at = '1970-01-01T00:00:00Z',
             version = version + 1, updated_at = ?
         WHERE id = ? AND status = 'retry_wait'",
    )
    .bind(now_rfc3339())
    .bind(&turn_id)
    .execute(db.pool())
    .await
    .unwrap();

    assert_eq!(worker.run_once().await.unwrap(), 1);
    let completed = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*db, &turn_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.status, AgentChatTurnState::Succeeded);
    assert_eq!(completed.attempt_count, 2);
    assert!(completed.response_message_id.is_some());
}

#[tokio::test]
async fn wake_incident_for_another_project_fails_closed_without_cross_project_turn() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (event_project_id, event_chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    let (attention_project_id, attention_chat_id) =
        bound_project(&db, &identity_id, &profile_id).await;
    let consumer = WakeTurnConsumer::new(Arc::clone(&db), "cross-project-consumer");
    consumer.run_once(100).await.unwrap();

    let incident_key = format!("attention:cross_project:project:{attention_project_id}");
    let source_event = new_uuid_v4();
    append_event(
        &db,
        CreateDomainEvent {
            id: source_event.clone(),
            event_type: "execution.failed".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: new_uuid_v4(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: attention_project_id.clone(),
            correlation_id: source_event.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!("cross-project-source:{source_event}")),
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
                   'Other project incident', ?, ?, ?, ?, 'inspect_run')",
    )
    .bind(new_uuid_v4())
    .bind(&attention_project_id)
    .bind(&identity_id)
    .bind(&source_event)
    .bind(
        serde_json::json!({
            "scope_type": "project",
            "scope_id": attention_project_id,
        })
        .to_string(),
    )
    .bind(&incident_key)
    .bind(now_rfc3339())
    .bind(now_rfc3339())
    .execute(db.pool())
    .await
    .unwrap();

    // The event and payload claim the first Project scope, while the
    // attention reference and incident key belong to the other Project.
    // Scope matching must reject this before chat lookup/admission.
    let wake =
        wake_event_for_attention_in_scope(&db, &identity_id, &event_project_id, &incident_key)
            .await;
    let wake_event_id = wake.id.clone();
    append_event(&db, wake).await;

    let run = consumer.run_once(100).await.unwrap();
    assert_eq!(run.delivered_turns, 0);
    let (disposition, reason): (String, String) = sqlx::query_as(
        "SELECT disposition.disposition, disposition.reason
         FROM agent_wake_disposition_current AS current
         JOIN agent_wake_disposition AS disposition
           ON disposition.id = current.disposition_id
         WHERE current.consumer_name = ? AND current.source_event_id = ?",
    )
    .bind("agent-wake-turns")
    .bind(&wake_event_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(disposition, "deterministically_suppressed");
    assert_eq!(reason, "cross_scope_incident");
    for chat_id in [event_chat_id, attention_chat_id] {
        let turn_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM agent_chat_turn_job
             WHERE chat_id = ? AND dedupe_key LIKE 'wake-turn:%'",
        )
        .bind(chat_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(turn_count, 0, "cross-project wake must not enqueue a turn");
    }
}

#[tokio::test]
async fn admitted_wake_runner_failure_is_terminal_on_budget_and_keeps_admission_disposition() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    let wake_consumer = WakeTurnConsumer::new(Arc::clone(&db), "runner-failure-consumer");
    wake_consumer.run_once(100).await.unwrap();

    let incident_key = format!("attention:runner_failure:project:{project_id}");
    let wake_event_id =
        append_project_attention_wake(&db, &identity_id, &project_id, &incident_key).await;
    let admitted = wake_consumer.run_once(100).await.unwrap();
    assert_eq!(admitted.delivered_turns, 1);
    let turn_id: String = sqlx::query_scalar(
        "SELECT id FROM agent_chat_turn_job
         WHERE chat_id = ? AND dedupe_key = ?",
    )
    .bind(&chat_id)
    .bind(format!("wake-turn:{wake_event_id}"))
    .fetch_one(db.pool())
    .await
    .unwrap();

    let worker = AgentChatTurnWorker::with_runner(
        Arc::clone(&db),
        Arc::new(FailingWakeRunner) as Arc<dyn AgentChatTurnRunner>,
    );
    assert_eq!(worker.run_once().await.unwrap(), 1);
    let first = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*db, &turn_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.status, AgentChatTurnState::RetryWait);
    assert_eq!(first.attempt_count, 1);

    // Make each finite retry due without changing the admitted wake job or
    // its frozen authority fields.
    for expected_attempt in [1_i64, 2_i64] {
        sqlx::query(
            "UPDATE agent_chat_turn_job
             SET next_attempt_at = '1970-01-01T00:00:00Z',
                 version = version + 1, updated_at = ?
             WHERE id = ? AND status = 'retry_wait' AND attempt_count = ?",
        )
        .bind(now_rfc3339())
        .bind(&turn_id)
        .bind(expected_attempt)
        .execute(db.pool())
        .await
        .unwrap();
        assert_eq!(worker.run_once().await.unwrap(), 1);
    }

    let terminal = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*db, &turn_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminal.status, AgentChatTurnState::Failed);
    assert_eq!(terminal.attempt_count, 3);
    assert!(terminal.next_attempt_at.is_none());
    assert_eq!(terminal.error_code.as_deref(), Some("backend_failed"));

    let (disposition_count, disposition, disposition_turn_id, reason): (
        i64,
        String,
        String,
        String,
    ) = sqlx::query_as(
        "SELECT COUNT(*), MAX(disposition), MAX(turn_job_id), MAX(reason)
         FROM agent_wake_disposition
         WHERE consumer_name = ? AND source_event_id = ?",
    )
    .bind("agent-wake-turns")
    .bind(&wake_event_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(disposition_count, 1);
    assert_eq!(disposition, "turn_admitted");
    assert_eq!(disposition_turn_id, turn_id);
    assert_eq!(reason, "turn_admitted");
}

#[tokio::test]
async fn malformed_wake_is_terminally_suppressed_with_one_disposition() {
    let db = database().await;
    let consumer = WakeTurnConsumer::new(Arc::clone(&db), "malformed-wake-lease")
        .with_consumer_name("malformed-wake-consumer");
    consumer.run_once(100).await.unwrap();
    let event_id = new_uuid_v4();
    append_event(
        &db,
        CreateDomainEvent {
            id: event_id.clone(),
            event_type: "agent.wake.admitted".to_owned(),
            entity_type: "agent_wake".to_owned(),
            entity_id: "malformed".to_owned(),
            actor_type: "attention_projection".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: new_uuid_v4(),
            correlation_id: event_id.clone(),
            causation_id: None,
            causation_depth: 1,
            dedupe_key: Some(format!("malformed-wake:{event_id}")),
            payload_json: serde_json::json!({
                "scope_type": "project",
                "incident_key": "missing-scope-id",
            })
            .to_string(),
            created_at: now_rfc3339(),
        },
    )
    .await;

    let run = consumer.run_once(100).await.unwrap();
    assert_eq!(run.delivered_turns, 0);
    let (count, disposition, reason): (i64, String, String) = sqlx::query_as(
        "SELECT COUNT(*), MAX(disposition), MAX(reason)
         FROM agent_wake_disposition
         WHERE consumer_name = ? AND source_event_id = ?",
    )
    .bind("malformed-wake-consumer")
    .bind(&event_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(count, 1);
    assert_eq!(disposition, "deterministically_suppressed");
    assert_eq!(reason, "scope_id_missing");
    let receipt_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM event_projection_receipt
         WHERE consumer_name = ? AND event_id = ?",
    )
    .bind("malformed-wake-consumer")
    .bind(&event_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(receipt_count, 1);
}

#[tokio::test]
async fn setup_required_wake_reconsiders_after_binding_change() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    let consumer = WakeTurnConsumer::new(Arc::clone(&db), "setup-reconsideration-consumer");
    consumer.run_once(100).await.unwrap();

    let setup_at = now_rfc3339();
    sqlx::query(
        "UPDATE project_agent_binding
         SET identity_id = NULL, profile_id = NULL, state = 'agent_setup_required',
             updated_at = ?, version = version + 1
         WHERE project_id = ?",
    )
    .bind(&setup_at)
    .bind(&project_id)
    .execute(db.pool())
    .await
    .unwrap();

    let incident_key = format!("attention:setup:project:{project_id}");
    let source_event = new_uuid_v4();
    append_event(
        &db,
        CreateDomainEvent {
            id: source_event.clone(),
            event_type: "execution.failed".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: new_uuid_v4(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: source_event.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!("setup-source:{source_event}")),
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
                   'Setup incident', ?, ?, ?, ?, 'configure_binding')",
    )
    .bind(new_uuid_v4())
    .bind(&project_id)
    .bind(&identity_id)
    .bind(&source_event)
    .bind(
        serde_json::json!({
            "scope_type": "project",
            "scope_id": project_id,
        })
        .to_string(),
    )
    .bind(&incident_key)
    .bind(now_rfc3339())
    .bind(now_rfc3339())
    .execute(db.pool())
    .await
    .unwrap();
    append_event(
        &db,
        wake_event_for_attention(&db, &identity_id, &project_id, &incident_key).await,
    )
    .await;

    consumer.run_once(100).await.unwrap();
    let (first_disposition, attention_id): (String, Option<String>) = sqlx::query_as(
        "SELECT disposition, attention_id FROM agent_wake_disposition
         WHERE consumer_name = ? ORDER BY attempt_number LIMIT 1",
    )
    .bind("agent-wake-turns")
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(first_disposition, "setup_required");
    assert!(attention_id.is_some(), "setup must link an Attention row");

    let restored_at = "9999-01-01T00:00:00Z";
    sqlx::query(
        "UPDATE project_agent_binding
         SET identity_id = ?, profile_id = ?, state = 'active',
             updated_at = ?, version = version + 1
         WHERE project_id = ?",
    )
    .bind(&identity_id)
    .bind(&profile_id)
    .bind(restored_at)
    .bind(&project_id)
    .execute(db.pool())
    .await
    .unwrap();

    let retry_run = consumer.run_once(100).await.unwrap();
    assert_eq!(retry_run.delivered_turns, 1);
    let (attempt_count, admitted_count): (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*), SUM(disposition = 'turn_admitted')
         FROM agent_wake_disposition
         WHERE consumer_name = ?",
    )
    .bind("agent-wake-turns")
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(attempt_count, 2);
    assert_eq!(admitted_count, 1);
    let turn_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_chat_turn_job
         WHERE chat_id = ? AND dedupe_key LIKE 'wake-turn:%'",
    )
    .bind(&chat_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(turn_count, 1);
}

/// The whole autonomy loop, end to end: a Task execution fails → the durable
/// `execution.failed` event → Attention projects an incident and admits a
/// wake for the Project Agent binding → the wake consumer queues a turn on
/// the Project chat. No user message anywhere.
#[tokio::test]
async fn failed_execution_wakes_the_project_agent_end_to_end() {
    let (db, database_path) = file_database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    let consumer = WakeTurnConsumer::new(Arc::clone(&db), "loop-lease-owner");
    consumer.run_once(100).await.unwrap();

    let task_id = new_uuid_v4();
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO task (id, project_id, title, status, created_at, updated_at)
         VALUES (?, ?, 'wake loop task', 'in_progress', ?, ?)",
    )
    .bind(&task_id)
    .bind(&project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .unwrap();

    let execution_id = new_uuid_v4();
    let running_execution = ExecutionRepo::create_with_lease(
        &*db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task_id.clone(),
            agent_id: None,
            role: "worker".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        ClaimExecutionLease {
            execution_id: execution_id.clone(),
            expected_version: 1,
            owner: "embedded:wake-turn-test".to_owned(),
            lease_expires_at: "9999-01-01T00:00:00+00:00".to_owned(),
            hard_deadline_at: "9999-01-01T00:00:00+00:00".to_owned(),
            now: now.clone(),
        },
    )
    .await
    .unwrap();
    ExecutionRepo::terminalize(
        &*db,
        TerminalizeExecution {
            execution_id: execution_id.clone(),
            expected_version: running_execution.execution_version,
            lease_owner: running_execution.lease_owner.clone(),
            status: ExecutionStatus::Failed,
            stop_reason: Some(Some(StopReason::ExecutorFailed)),
            stopped_by: Some(Some("embedded:wake-turn-test".to_owned())),
            stopped_at: Some(Some(now.clone())),
            resume_policy: Some(Some(ResumePolicy::Manual)),
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            last_progress_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some(Some("gemini exited with status 1".to_owned())),
            executor_config_snapshot_json: None,
            updated_at: now.clone(),
            actor_type: "system".to_owned(),
            actor_id: Some("wake-turn-test".to_owned()),
            correlation_id: Some(format!("wake-turn:{execution_id}")),
            causation_id: None,
            causation_depth: 0,
            lease_disposition: ExecutionLeaseDisposition::Expire,
        },
    )
    .await
    .unwrap();
    AttentionService::new(Arc::clone(&db))
        .project_once(100)
        .await
        .unwrap();
    let wake_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type = 'agent.wake.admitted'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        wake_count, 1,
        "failed execution must admit exactly one wake"
    );

    consumer.run_once(100).await.unwrap();

    let (status, responder, content): (String, String, String) = sqlx::query_as(
        "SELECT job.status, job.responder_identity_id, message.content
         FROM agent_chat_turn_job AS job
         JOIN agent_chat_message AS message ON message.id = job.triggering_message_id
         WHERE job.chat_id = ? AND job.dedupe_key LIKE 'wake-turn:%'",
    )
    .bind(&chat_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(status, "queued");
    assert_eq!(responder, identity_id);
    assert!(content.contains("Task execution stopped"));
    drop(db);
    let _ = std::fs::remove_file(database_path);
}

#[tokio::test]
async fn wake_re_evaluates_current_replacement_binding() {
    let db = database().await;
    let original_identity = new_uuid_v4();
    let original_profile = identity_with_profile(&db, &original_identity).await;
    let (project_id, chat_id) = bound_project(&db, &original_identity, &original_profile).await;

    let consumer = WakeTurnConsumer::new(Arc::clone(&db), "replacement-binding-consumer");
    consumer.run_once(100).await.unwrap();

    let replacement_identity = new_uuid_v4();
    let replacement_profile = identity_with_profile(&db, &replacement_identity).await;
    let updated_at = now_rfc3339();
    sqlx::query(
        "UPDATE project_agent_binding
         SET identity_id = ?, profile_id = ?, state = 'active',
             updated_at = ?, version = version + 1
         WHERE project_id = ?",
    )
    .bind(&replacement_identity)
    .bind(&replacement_profile)
    .bind(&updated_at)
    .bind(&project_id)
    .execute(db.pool())
    .await
    .unwrap();

    let incident_key = format!("attention:binding_replaced:project:{project_id}");
    let source_event = new_uuid_v4();
    append_event(
        &db,
        CreateDomainEvent {
            id: source_event.clone(),
            event_type: "execution.failed".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: new_uuid_v4(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: source_event.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!("replacement-source:{source_event}")),
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
                   'Binding was replaced', ?, ?, ?, ?, 'inspect_run')",
    )
    .bind(new_uuid_v4())
    .bind(&project_id)
    .bind(&original_identity)
    .bind(&source_event)
    .bind(
        serde_json::json!({
            "scope_type": "project",
            "scope_id": project_id,
        })
        .to_string(),
    )
    .bind(&incident_key)
    .bind(now_rfc3339())
    .bind(now_rfc3339())
    .execute(db.pool())
    .await
    .unwrap();

    // The decision names the old identity, but delivery must resolve the
    // current binding and freeze the replacement identity/Profile.
    append_event(
        &db,
        wake_event_for_attention(&db, &original_identity, &project_id, &incident_key).await,
    )
    .await;

    let run = consumer.run_once(100).await.unwrap();
    assert_eq!(run.delivered_turns, 1);

    let (responder, profile): (String, String) = sqlx::query_as(
        "SELECT responder_identity_id, profile_id
         FROM agent_chat_turn_job
         WHERE chat_id = ? AND dedupe_key LIKE 'wake-turn:%'",
    )
    .bind(&chat_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(responder, replacement_identity);
    assert_eq!(profile, replacement_profile);
}

#[tokio::test]
async fn deferred_wake_retries_after_authoritative_responder_recovery() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    let consumer = WakeTurnConsumer::new(Arc::clone(&db), "deferred-recovery-owner");
    consumer.run_once(100).await.unwrap();

    // A paused responder is a transient authoritative-unavailable state,
    // rather than malformed wake input. Delivery must checkpoint it as a
    // bounded deferred attempt and leave the source receipt/cursor durable.
    sqlx::query(
        "UPDATE agent_identity
         SET paused = 1, version = version + 1, updated_at = ?
         WHERE id = ?",
    )
    .bind(now_rfc3339())
    .bind(&identity_id)
    .execute(db.pool())
    .await
    .unwrap();

    let incident_key = format!("attention:deferred_recovery:project:{project_id}");
    let source_event_id = new_uuid_v4();
    append_event(
        &db,
        CreateDomainEvent {
            id: source_event_id.clone(),
            event_type: "execution.failed".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: new_uuid_v4(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: source_event_id.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!("deferred-recovery-source:{source_event_id}")),
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
                   'Responder temporarily unavailable', ?, ?, ?, ?, 'restore_responder')",
    )
    .bind(new_uuid_v4())
    .bind(&project_id)
    .bind(&identity_id)
    .bind(&source_event_id)
    .bind(
        serde_json::json!({
            "scope_type": "project",
            "scope_id": project_id,
        })
        .to_string(),
    )
    .bind(&incident_key)
    .bind(now_rfc3339())
    .bind(now_rfc3339())
    .execute(db.pool())
    .await
    .unwrap();

    let wake_event = wake_event_for_attention(&db, &identity_id, &project_id, &incident_key).await;
    let wake_event_id = wake_event.id.clone();
    append_event(&db, wake_event).await;

    let first = consumer.run_once(100).await.unwrap();
    assert_eq!(first.delivered_turns, 0);

    let wake_row = DomainEventRepo::get_event(&*db, &wake_event_id)
        .await
        .unwrap()
        .unwrap();
    let (attempt, max_attempts, disposition, reason, retry_at): (
        i64,
        i64,
        String,
        String,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT disposition.attempt_number, disposition.max_attempts,
                disposition.disposition, disposition.reason, disposition.retry_at
         FROM agent_wake_disposition_current AS current
         JOIN agent_wake_disposition AS disposition
           ON disposition.id = current.disposition_id
         WHERE current.consumer_name = ? AND current.source_event_id = ?",
    )
    .bind("agent-wake-turns")
    .bind(&wake_event_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(attempt, 1);
    assert_eq!(max_attempts, 3);
    assert_eq!(disposition, "deferred");
    assert_eq!(reason, "responder_unavailable");
    assert!(
        retry_at.is_some(),
        "deferred wake must have a retry deadline"
    );

    let receipt_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM event_projection_receipt
         WHERE consumer_name = ? AND event_id = ?",
    )
    .bind("agent-wake-turns")
    .bind(&wake_event_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        receipt_count, 1,
        "deferred wake must still checkpoint its receipt"
    );
    let cursor = DomainEventRepo::get_consumer_cursor(&*db, "agent-wake-turns")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor.last_sequence, wake_row.sequence);

    // Repair the authoritative responder, then let the immutable retry
    // deadline become due. The retry lineage must admit one turn, not replay
    // the source event or create a second turn.
    sqlx::query(
        "UPDATE agent_identity
         SET paused = 0, version = version + 1, updated_at = ?
         WHERE id = ?",
    )
    .bind(now_rfc3339())
    .bind(&identity_id)
    .execute(db.pool())
    .await
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;

    let retry = consumer.run_once(100).await.unwrap();
    assert_eq!(retry.delivered_turns, 1);
    let (disposition_count, admitted_count): (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*), SUM(disposition = 'turn_admitted')
         FROM agent_wake_disposition
         WHERE consumer_name = ? AND source_event_id = ?",
    )
    .bind("agent-wake-turns")
    .bind(&wake_event_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(disposition_count, 2);
    assert_eq!(admitted_count, 1);
    let turn_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_chat_turn_job
         WHERE chat_id = ? AND dedupe_key = ?",
    )
    .bind(&chat_id)
    .bind(format!("wake-turn:{wake_event_id}"))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(turn_count, 1);
    let receipt_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM event_projection_receipt
         WHERE consumer_name = ? AND event_id = ?",
    )
    .bind("agent-wake-turns")
    .bind(&wake_event_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(receipt_count, 1, "retry must preserve one source receipt");
}

#[tokio::test]
async fn deferred_wake_rechecks_changed_incident_material_before_delivery() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    let consumer = WakeTurnConsumer::new(Arc::clone(&db), "changed-material-consumer");
    consumer.run_once(100).await.unwrap();

    // Force a durable deferred disposition without making the Attention
    // itself malformed. The source wake remains the immutable trigger whose
    // original digest must not be replayed after the incident changes.
    sqlx::query(
        "UPDATE agent_identity
         SET paused = 1, version = version + 1, updated_at = ?
         WHERE id = ?",
    )
    .bind(now_rfc3339())
    .bind(&identity_id)
    .execute(db.pool())
    .await
    .unwrap();
    let incident_key = format!("attention:changed_material:project:{project_id}");
    let wake_event_id =
        append_project_attention_wake(&db, &identity_id, &project_id, &incident_key).await;
    let first = consumer.run_once(100).await.unwrap();
    assert_eq!(first.delivered_turns, 0);
    let (first_attempt, first_disposition, first_digest): (i64, String, String) = sqlx::query_as(
        "SELECT disposition.attempt_number, disposition.disposition,
                    disposition.incident_digest
             FROM agent_wake_disposition_current AS current
             JOIN agent_wake_disposition AS disposition
               ON disposition.id = current.disposition_id
             WHERE current.consumer_name = ? AND current.source_event_id = ?",
    )
    .bind("agent-wake-turns")
    .bind(&wake_event_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(first_attempt, 1);
    assert_eq!(first_disposition, "deferred");

    // A material Attention update changes both the version and canonical
    // incident digest. Once the retry is due, the consumer must evaluate the
    // current projection and suppress the stale wake rather than admit its
    // old content or replay the deferred disposition.
    sqlx::query(
        "UPDATE attention_projection
         SET details_json = ?, version = version + 1, updated_at = ?
         WHERE dedupe_key = ?",
    )
    .bind(
        serde_json::json!({
            "scope_type": "project",
            "scope_id": project_id,
            "state": "materially-changed",
        })
        .to_string(),
    )
    .bind(now_rfc3339())
    .bind(&incident_key)
    .execute(db.pool())
    .await
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;

    let retry = consumer.run_once(100).await.unwrap();
    assert_eq!(retry.delivered_turns, 0);
    let (attempt, disposition, reason, current_digest): (i64, String, String, String) =
        sqlx::query_as(
            "SELECT disposition.attempt_number, disposition.disposition,
                    disposition.reason, disposition.incident_digest
             FROM agent_wake_disposition_current AS current
             JOIN agent_wake_disposition AS disposition
               ON disposition.id = current.disposition_id
             WHERE current.consumer_name = ? AND current.source_event_id = ?",
        )
        .bind("agent-wake-turns")
        .bind(&wake_event_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(attempt, 2);
    assert_eq!(disposition, "deterministically_suppressed");
    assert_eq!(reason, "attention_changed");
    assert_ne!(current_digest, first_digest);
    let turn_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_chat_turn_job
         WHERE chat_id = ? AND dedupe_key = ?",
    )
    .bind(&chat_id)
    .bind(format!("wake-turn:{wake_event_id}"))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        turn_count, 0,
        "stale deferred content must not be delivered"
    );
}

#[tokio::test]
async fn file_backed_restart_race_recovers_expired_lease_and_preserves_post_cutover_event() {
    let (db, database_path) = file_database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    let cutover_consumer = WakeTurnConsumer::new(Arc::clone(&db), "cutover-owner");
    cutover_consumer.run_once(100).await.unwrap();

    let incident_key = format!("attention:restart_race:project:{project_id}");
    let source_event_id = new_uuid_v4();
    append_event(
        &db,
        CreateDomainEvent {
            id: source_event_id.clone(),
            event_type: "execution.failed".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: new_uuid_v4(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: source_event_id.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!("restart-race-source:{source_event_id}")),
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
                   'Restart race incident', ?, ?, ?, ?, 'inspect_run')",
    )
    .bind(new_uuid_v4())
    .bind(&project_id)
    .bind(&identity_id)
    .bind(&source_event_id)
    .bind(
        serde_json::json!({
            "scope_type": "project",
            "scope_id": project_id,
        })
        .to_string(),
    )
    .bind(&incident_key)
    .bind(now_rfc3339())
    .bind(now_rfc3339())
    .execute(db.pool())
    .await
    .unwrap();
    // The source event is before the wake and is consumed before the process
    // loss seam; the wake itself is the post-cutover event under test.
    cutover_consumer.run_once(100).await.unwrap();
    let wake_event = wake_event_for_attention(&db, &identity_id, &project_id, &incident_key).await;
    let wake_event_id = wake_event.id.clone();
    append_event(&db, wake_event).await;
    let wake_row = DomainEventRepo::get_event(&*db, &wake_event_id)
        .await
        .unwrap()
        .unwrap();

    // Simulate a process that claimed the event and died before writing its
    // disposition. A live replacement must remain blocked at the cursor
    // head; only after lease expiry may a restarted owner claim it.
    let claimed = DomainEventRepo::claim_event_batch(
        &*db,
        ClaimDomainEvents {
            consumer_name: "agent-wake-turns".to_owned(),
            lease_owner: "crashed-process".to_owned(),
            now: now_rfc3339(),
            leased_until: "9999-12-31T00:00:00Z".to_owned(),
            limit: 1,
        },
    )
    .await
    .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, wake_event_id);
    let blocked = WakeTurnConsumer::new(Arc::clone(&db), "blocked-observer")
        .run_once(1)
        .await
        .unwrap();
    assert_eq!(blocked.claimed_events, 0);
    assert_eq!(blocked.delivered_turns, 0);

    sqlx::query(
        "UPDATE event_processing_lease
         SET leased_until = '2000-01-01T00:00:00Z', updated_at = '2000-01-01T00:00:00Z'
         WHERE consumer_name = ? AND event_sequence = ?",
    )
    .bind("agent-wake-turns")
    .bind(wake_row.sequence)
    .execute(db.pool())
    .await
    .unwrap();

    let restart_a = WakeTurnConsumer::new(Arc::clone(&db), "restart-a");
    let restart_b = WakeTurnConsumer::new(Arc::clone(&db), "restart-b");
    let (run_a, run_b) = tokio::join!(restart_a.run_once(1), restart_b.run_once(1));
    let run_a = run_a.unwrap();
    let run_b = run_b.unwrap();
    // Turn admission itself may append a follow-up domain event while the
    // losing race participant is still polling. The wake source is still
    // claimed exactly once, as proved by its one receipt/disposition below.
    assert!(run_a.claimed_events + run_b.claimed_events >= 1);
    assert_eq!(run_a.delivered_turns + run_b.delivered_turns, 1);

    let (disposition_count, current_count, turn_count, receipt_count): (i64, i64, i64, i64) =
        sqlx::query_as(
            "SELECT
                 (SELECT COUNT(*) FROM agent_wake_disposition
                  WHERE consumer_name = 'agent-wake-turns' AND source_event_id = ?),
                 (SELECT COUNT(*) FROM agent_wake_disposition_current
                  WHERE consumer_name = 'agent-wake-turns' AND source_event_id = ?),
                 (SELECT COUNT(*) FROM agent_chat_turn_job
                  WHERE chat_id = ? AND dedupe_key = ?),
                 (SELECT COUNT(*) FROM event_projection_receipt
                  WHERE consumer_name = 'agent-wake-turns' AND event_id = ?)",
        )
        .bind(&wake_event_id)
        .bind(&wake_event_id)
        .bind(&chat_id)
        .bind(format!("wake-turn:{wake_event_id}"))
        .bind(&wake_event_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(
        disposition_count, 1,
        "race must write one disposition attempt"
    );
    assert_eq!(current_count, 1, "race must leave one current disposition");
    assert_eq!(turn_count, 1, "race must admit one turn");
    assert_eq!(receipt_count, 1, "race must write one receipt");

    // A new event appended after the immutable migration cutover must remain
    // visible after recovery; cursor repair cannot fast-forward over it.
    let post_cutover_event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "task.transitioned".to_owned(),
        entity_type: "task".to_owned(),
        entity_id: new_uuid_v4(),
        actor_type: "system".to_owned(),
        actor_id: None,
        scope_type: "project".to_owned(),
        scope_id: project_id,
        correlation_id: new_uuid_v4(),
        causation_id: None,
        causation_depth: 0,
        dedupe_key: Some(format!("restart-race-post-cutover:{}", new_uuid_v4())),
        payload_json: "{}".to_owned(),
        created_at: now_rfc3339(),
    };
    let post_cutover_id = post_cutover_event.id.clone();
    append_event(&db, post_cutover_event).await;
    WakeTurnConsumer::new(Arc::clone(&db), "restart-after-race")
        .run_once(100)
        .await
        .unwrap();
    let post_receipt_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM event_projection_receipt
         WHERE consumer_name = 'agent-wake-turns' AND event_id = ?",
    )
    .bind(&post_cutover_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(post_receipt_count, 1, "post-cutover event must not be lost");

    drop(restart_a);
    drop(restart_b);
    drop(cutover_consumer);
    drop(db);
    let _ = std::fs::remove_file(database_path);
}
