use std::sync::Arc;

use async_trait::async_trait;
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentChatRepo,
    AgentChatTurnJobRepo, AgentChatTurnState, AgentProfileRepo, AgentRepo, AgentStatus,
    AttentionRepo, CreateAgentIdentity, CreateAgentProfile, CreateDomainEvent, CreateProject,
    DomainEventRepo, ProjectRepo, SelectAgentProfile, SqliteDb, UpdateAgentChatTurnJob, User,
    UserRepo,
};
use services::project_escalation::EscalationAuthority::Agent;
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
        Err(ServiceError::TurnFailure {
            failure: api_types::TurnFailure::UsageLimit { resets_at: None },
            error: Box::new(ServiceError::InvalidOperation {
                message: "provider capacity exhausted".to_owned(),
            }),
        })
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
    AttentionService::new(fixture.db.clone())
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

async fn incident(f: &ChatTurnFixture, category: &str, key: &str) -> db::AttentionProjection {
    let now = now_rfc3339();
    let event =
        f.db.append_event(CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "test.incident".to_owned(),
            entity_type: "project".to_owned(),
            entity_id: f.project_id.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: f.project_id.clone(),
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: None,
            payload_json: "{}".to_owned(),
            created_at: now.clone(),
        })
        .await
        .unwrap();
    f.db.insert_attention(db::CreateAttentionProjection {
        id: new_uuid_v4(),
        attention_type: category.to_owned(),
        scope_type: "project".to_owned(),
        scope_id: f.project_id.clone(),
        identity_id: Some(f.identity_id.clone()),
        source_event_id: event.id,
        priority: 85,
        status: "open".to_owned(),
        summary: format!("Blocker {key}"),
        details_json:
            serde_json::json!({"scope_type":"project","scope_id":f.project_id,"need":key})
                .to_string(),
        dedupe_key: format!("attention:{category}:project:{}:{key}", f.project_id),
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
    .unwrap()
}
fn wake_request(
    f: &ChatTurnFixture,
    a: &db::AttentionProjection,
    now: &str,
) -> services::WakeAdmissionRequest {
    services::WakeAdmissionRequest {
        identity_id: f.identity_id.clone(),
        scope_type: "project".to_owned(),
        scope_id: f.project_id.clone(),
        incident_key: a.dedupe_key.clone(),
        lease_owner: new_uuid_v4(),
        correlation_id: a.id.clone(),
        causation_id: Some(a.source_event_id.clone()),
        caused_by_identity_id: None,
        reaction_depth: 0,
        now: now.to_owned(),
        lease_seconds: 60,
        cooldown_seconds: 300,
    }
}
async fn turns(f: &ChatTurnFixture) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM agent_chat_turn_job WHERE chat_id=?")
        .bind(&f.chat_id)
        .fetch_one(f.db.pool())
        .await
        .unwrap()
}
async fn charged(f: &ChatTurnFixture, category: &str) -> i64 {
    sqlx::query_scalar("SELECT COALESCE(SUM(admitted_count),0) FROM agent_wake_budget_window WHERE scope_id=? AND category=?").bind(&f.project_id).bind(category).fetch_one(f.db.pool()).await.unwrap()
}
#[tokio::test]
async fn suppressed_blocker_is_readmitted_after_cooldown_without_another_event() {
    let f = chat_turn_fixture().await;
    let a = incident(&f, "execution_failed", "cooling").await;
    let now = chrono::Utc::now();
    let service = AttentionService::new(f.db.clone());
    let request = wake_request(&f, &a, &now.to_rfc3339());
    sqlx::query("INSERT INTO agent_wake_lease(identity_id,scope_type,scope_id,incident_key,lease_owner,leased_until,reaction_depth,updated_at,cooldown_until) VALUES(?,'project',?,?,?, ?,0,?,?)")
        .bind(&f.identity_id).bind(&f.project_id).bind(&a.dedupe_key).bind("old-lease").bind((now-chrono::Duration::seconds(1)).to_rfc3339()).bind(now.to_rfc3339()).bind((now+chrono::Duration::seconds(300)).to_rfc3339()).execute(f.db.pool()).await.unwrap();
    assert!(matches!(
        service.admit_wake(request).await.unwrap(),
        services::WakeAdmissionResult::Suppressed {
            reason: services::WakeSuppressionReason::Cooldown
        }
    ));
    assert_eq!(charged(&f, "blocker").await, 0);
    assert_eq!(
        service
            .sweep_once_at(&(now + chrono::Duration::seconds(301)).to_rfc3339())
            .await
            .unwrap(),
        1
    );
    assert_eq!(turns(&f).await, 1);
    assert_eq!(charged(&f, "blocker").await, 1);
}
#[tokio::test]
async fn unchanged_admitted_blocker_escalates_once_after_a_silent_turn() {
    let f = chat_turn_fixture().await;
    incident(&f, "execution_failed", "cannot-fix").await;
    let service = AttentionService::new(f.db.clone());
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    let worker = AgentChatTurnWorker::with_runner(f.db.clone(), Arc::new(ProseOnlyWakeRunner));
    assert_eq!(worker.run_once().await.unwrap(), 1);
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_wake_escalation WHERE project_id=?")
            .bind(&f.project_id)
            .fetch_one(f.db.pool())
            .await
            .unwrap();
    assert_eq!(count, 1);
    service
        .sweep_once_at(&(chrono::Utc::now() + chrono::Duration::hours(2)).to_rfc3339())
        .await
        .unwrap();
    let notices: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM notification WHERE project_id=? AND event_type='project.escalated'",
    )
    .bind(&f.project_id)
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    assert_eq!(notices, 1);
    assert_eq!(turns(&f).await, 1);
}
#[tokio::test]
async fn budget_is_charged_only_with_an_atomic_turn_admission() {
    let f = chat_turn_fixture().await;
    let a = incident(&f, "delivery_followup", "ready").await;
    sqlx::query("UPDATE agent_chat SET status='agent_setup_required' WHERE id=?")
        .bind(&f.chat_id)
        .execute(f.db.pool())
        .await
        .unwrap();
    let service = AttentionService::new(f.db.clone());
    let result = service
        .admit_wake(wake_request(&f, &a, &now_rfc3339()))
        .await
        .unwrap();
    assert!(matches!(
        result,
        services::WakeAdmissionResult::SetupRequired { .. }
    ));
    assert_eq!(turns(&f).await, 0);
    assert_eq!(charged(&f, "delivery").await, 0);
    sqlx::query("UPDATE agent_chat SET status='ready' WHERE id=?")
        .bind(&f.chat_id)
        .execute(f.db.pool())
        .await
        .unwrap();
    service
        .admit_wake(wake_request(&f, &a, &now_rfc3339()))
        .await
        .unwrap();
    assert_eq!(turns(&f).await, 1);
    assert_eq!(charged(&f, "delivery").await, 1);
}
#[tokio::test]
async fn delivery_spam_cannot_consume_the_blocker_share() {
    let f = chat_turn_fixture().await;
    let service = AttentionService::new(f.db.clone());
    for n in 0..4 {
        let a = incident(&f, "delivery_followup", &format!("delivery-{n}")).await;
        assert!(matches!(
            service
                .admit_wake(wake_request(&f, &a, &now_rfc3339()))
                .await
                .unwrap(),
            services::WakeAdmissionResult::Admitted { .. }
        ));
        sqlx::query("UPDATE agent_chat_turn_job SET status='succeeded' WHERE chat_id=?")
            .bind(&f.chat_id)
            .execute(f.db.pool())
            .await
            .unwrap();
    }
    let extra = incident(&f, "delivery_followup", "overflow").await;
    assert!(matches!(
        service
            .admit_wake(wake_request(&f, &extra, &now_rfc3339()))
            .await
            .unwrap(),
        services::WakeAdmissionResult::Suppressed {
            reason: services::WakeSuppressionReason::BudgetExhausted
        }
    ));
    let blocker = incident(&f, "execution_failed", "repair").await;
    assert!(matches!(
        service
            .admit_wake(wake_request(&f, &blocker, &now_rfc3339()))
            .await
            .unwrap(),
        services::WakeAdmissionResult::Admitted { .. }
    ));
    assert_eq!(charged(&f, "delivery").await, 4);
    assert_eq!(charged(&f, "blocker").await, 1);
}
#[tokio::test]
async fn two_project_blockers_share_one_wake_and_one_directive() {
    let f = chat_turn_fixture().await;
    let a = incident(&f, "execution_failed", "one").await;
    let b = incident(&f, "review_risk", "two").await;
    assert_eq!(
        AttentionService::new(f.db.clone())
            .sweep_once_at(&now_rfc3339())
            .await
            .unwrap(),
        1
    );
    assert_eq!(turns(&f).await, 1);
    assert_eq!(charged(&f, "blocker").await, 1);
    let content: String = sqlx::query_scalar(
        "SELECT content FROM agent_chat_message WHERE chat_id=? AND outcome='attention_wake'",
    )
    .bind(&f.chat_id)
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    assert!(content.contains(&a.dedupe_key));
    assert!(content.contains(&b.dedupe_key));
    assert_eq!(
        content.matches("Assess the current state").count()
            + content.matches("EXECUTION FAILURE RECOVERY").count(),
        1
    );
    let linked: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_wake_blocker")
        .fetch_one(f.db.pool())
        .await
        .unwrap();
    assert_eq!(linked, 2);
    let payload:String=sqlx::query_scalar("SELECT payload_json FROM domain_event WHERE event_type='agent.wake.admitted' AND scope_id=?").bind(&f.project_id).fetch_one(f.db.pool()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(payload["reason"], "turn_admitted");
    assert_eq!(payload["admission_phase"], "turn");
    assert!(payload.get("action").is_none());
    let id: String = sqlx::query_scalar("SELECT id FROM agent_chat_turn_job WHERE chat_id=?")
        .bind(&f.chat_id)
        .fetch_one(f.db.pool())
        .await
        .unwrap();
    assert_eq!(payload["turn_job_id"], id);
}
#[tokio::test]
async fn deterministic_provider_failure_is_terminal_on_attempt_one() {
    let f = chat_turn_fixture().await;
    let a = incident(&f, "delivery_followup", "capacity").await;
    AttentionService::new(f.db.clone())
        .admit_wake(wake_request(&f, &a, &now_rfc3339()))
        .await
        .unwrap();
    let worker = AgentChatTurnWorker::with_runner(f.db.clone(), Arc::new(FailingWakeRunner));
    worker.run_once().await.unwrap();
    let (status, count, code): (String, i64, String) = sqlx::query_as(
        "SELECT status,attempt_count,error_code FROM agent_chat_turn_job WHERE chat_id=?",
    )
    .bind(&f.chat_id)
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    assert_eq!(
        (status, count, code),
        ("failed".to_owned(), 1, "usage_limit".to_owned())
    );
    assert_eq!(worker.run_once().await.unwrap(), 0);
}
#[tokio::test]
async fn restart_lease_expiry_refunds_the_attempt_before_reclaim() {
    let f = chat_turn_fixture().await;
    let a = incident(&f, "delivery_followup", "restart").await;
    AttentionService::new(f.db.clone())
        .admit_wake(wake_request(&f, &a, &now_rfc3339()))
        .await
        .unwrap();
    sqlx::query("UPDATE agent_chat_turn_job SET status='leased',attempt_count=1,invocation_count=1,lease_owner='previous-process',leased_until='2000-01-01T00:00:00Z' WHERE chat_id=?").bind(&f.chat_id).execute(f.db.pool()).await.unwrap();
    AgentChatTurnWorker::with_runner(f.db.clone(), Arc::new(FailingWakeRunner))
        .run_once_at(chrono::Utc::now() + chrono::Duration::seconds(1))
        .await
        .unwrap();
    let (status, count): (String, i64) =
        sqlx::query_as("SELECT status,attempt_count FROM agent_chat_turn_job WHERE chat_id=?")
            .bind(&f.chat_id)
            .fetch_one(f.db.pool())
            .await
            .unwrap();
    assert_eq!(status, "failed");
    assert_eq!(count, 1, "restart did not consume the first attempt");
}
#[tokio::test]
async fn escalate_creates_one_owner_notification_and_attention_then_answer_wakes() {
    let f = chat_turn_fixture().await;
    let service = services::project_escalation::ProjectEscalationService::new(f.db.clone());
    assert!(service
        .escalate(
            &f.project_id,
            Agent("foreign-agent"),
            api_types::ProjectEscalateRequest {
                need: "Need disk space".to_owned(),
                task_ids: vec![]
            },
            "need"
        )
        .await
        .is_err());
    let request = api_types::ProjectEscalateRequest {
        need: "Free disk to restore the Project environment".to_owned(),
        task_ids: vec![],
    };
    let e = service
        .escalate(
            &f.project_id,
            Agent(&f.identity_id),
            request.clone(),
            "need",
        )
        .await
        .unwrap();
    let replay = service
        .escalate(&f.project_id, Agent(&f.identity_id), request, "need")
        .await
        .unwrap();
    assert_eq!(e.id, replay.id);
    assert!(service
        .answer(
            &f.project_id,
            &e.id,
            "foreign-owner",
            api_types::AnswerProjectEscalationRequest {
                expected_version: 1,
                answer: "Done".to_owned()
            }
        )
        .await
        .is_err());
    let item = f.db.get_attention(&e.attention_id).await.unwrap().unwrap();
    assert_eq!(item.recommended_action, "answer_escalation");
    assert_eq!(
        AttentionService::new(f.db.clone())
            .sweep_once_at(&now_rfc3339())
            .await
            .unwrap(),
        0,
        "owner escalation never wakes the blocked Agent"
    );
    service
        .answer(
            &f.project_id,
            &e.id,
            &f.account_id,
            api_types::AnswerProjectEscalationRequest {
                expected_version: 1,
                answer: "Disk space is now available".to_owned(),
            },
        )
        .await
        .unwrap();
    AttentionService::new(f.db.clone())
        .project_once(100)
        .await
        .unwrap();
    assert_eq!(turns(&f).await, 1);
    assert_eq!(
        charged(&f, "decision").await,
        0,
        "an owner answer is owner-initiated and never charged"
    );
    let content: String = sqlx::query_scalar(
        "SELECT content FROM agent_chat_message WHERE chat_id=? AND outcome='attention_wake'",
    )
    .bind(&f.chat_id)
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    assert!(content.contains("Disk space is now available"));
}
#[tokio::test]
async fn wake_audit_events_cannot_admit_a_second_turn() {
    let f = chat_turn_fixture().await;
    append_event(
        &f.db,
        CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "agent.wake.admitted".to_owned(),
            entity_type: "agent_wake".to_owned(),
            entity_id: "forged".to_owned(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: f.project_id.clone(),
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: None,
            payload_json:
                serde_json::json!({"identity_id":f.identity_id,"decision":"turn_admitted"})
                    .to_string(),
            created_at: now_rfc3339(),
        },
    )
    .await;
    WakeTurnConsumer::new(f.db.clone())
        .run_once(100)
        .await
        .unwrap();
    assert_eq!(turns(&f).await, 0);
}
#[tokio::test]
async fn unchanged_delivery_metadata_does_not_create_a_new_blocker_digest() {
    let f = chat_turn_fixture().await;
    let a = incident(&f, "execution_failed", "stable").await;
    let digest = wake_attention_incident_digest(&a);
    let mut updated = a;
    updated.version += 1;
    updated.source_event_id = new_uuid_v4();
    updated.source_sequence = Some(999);
    assert_eq!(digest, wake_attention_incident_digest(&updated));
    updated.details_json = serde_json::json!({"need":"different blocker"}).to_string();
    assert_ne!(digest, wake_attention_incident_digest(&updated));
    // n2: a renamed Task or a changed action offer is not a new blocker.
    let with = |title: &str, actions: serde_json::Value, reason: &str| {
        let mut a = updated.clone();
        a.details_json = serde_json::json!({
            "task": {"task_title": title, "task_status": "failed"},
            "recovery": {"requires_intervention": true, "actions": actions},
            "interruption": {"reason": reason},
        })
        .to_string();
        wake_attention_incident_digest(&a)
    };
    let base = with(
        "Build login",
        serde_json::json!([{"verb": "retry"}]),
        "tests failed",
    );
    assert_eq!(
        base,
        with(
            "Build the login page",
            serde_json::json!([]),
            "tests failed"
        )
    );
    assert_ne!(
        base,
        with(
            "Build login",
            serde_json::json!([{"verb": "retry"}]),
            "disk full"
        )
    );
}

#[tokio::test]
async fn unchanged_failed_delivery_is_suppressed_as_repeated_failure() {
    let f = chat_turn_fixture().await;
    let service = AttentionService::new(f.db.clone());
    let a = incident(&f, "delivery_followup", "repeated").await;
    service
        .admit_wake(wake_request(&f, &a, &now_rfc3339()))
        .await
        .unwrap();
    AgentChatTurnWorker::with_runner(f.db.clone(), Arc::new(FailingWakeRunner))
        .run_once()
        .await
        .unwrap();
    let refreshed = incident(&f, "delivery_followup", "repeated").await;
    assert!(matches!(
        service
            .admit_wake(wake_request(&f, &refreshed, &now_rfc3339()))
            .await
            .unwrap(),
        services::WakeAdmissionResult::Suppressed {
            reason: services::WakeSuppressionReason::RepeatedFailure
        }
    ));
    assert_eq!(turns(&f).await, 1);
    assert_eq!(charged(&f, "delivery").await, 1);
}
#[tokio::test]
async fn turn_admission_failure_rolls_back_budget_lease_and_audit() {
    let f = chat_turn_fixture().await;
    let a = incident(&f, "delivery_followup", "atomic").await;
    sqlx::raw_sql("CREATE TRIGGER reject_turn BEFORE INSERT ON agent_chat_turn_job BEGIN SELECT RAISE(ABORT,'synthetic turn failure'); END;").execute(f.db.pool()).await.unwrap();
    assert!(AttentionService::new(f.db.clone())
        .admit_wake(wake_request(&f, &a, &now_rfc3339()))
        .await
        .is_err());
    assert_eq!(turns(&f).await, 0);
    assert_eq!(charged(&f, "delivery").await, 0);
    let audit: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type='agent.wake.admitted'",
    )
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    assert_eq!(audit, 0);
}

#[tokio::test]
async fn zero_budget_sweep_does_not_repeat_the_owner_stall_notice() {
    let f = chat_turn_fixture().await;
    incident(&f, "execution_failed", "zero-budget").await;
    sqlx::query("UPDATE project_agent_binding SET wake_budget=0 WHERE project_id=?")
        .bind(&f.project_id)
        .execute(f.db.pool())
        .await
        .unwrap();
    let service = AttentionService::new(f.db.clone());
    service.sweep_once_at(&now_rfc3339()).await.unwrap();
    service
        .sweep_once_at(&(chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339())
        .await
        .unwrap();
    let notices:i64=sqlx::query_scalar("SELECT COUNT(*) FROM domain_event WHERE event_type='notification.requested' AND scope_id=?").bind(&f.project_id).fetch_one(f.db.pool()).await.unwrap();
    assert_eq!(notices, 1);
    assert_eq!(turns(&f).await, 0);
    assert_eq!(charged(&f, "blocker").await, 0);
}
// ---- audit 3.5 probes -------------------------------------------------------
async fn audit35_reproject(
    f: &ChatTurnFixture,
    a: &db::AttentionProjection,
    need: &str,
) -> db::AttentionProjection {
    let now = now_rfc3339();
    let event =
        f.db.append_event(CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "test.incident".to_owned(),
            entity_type: "project".to_owned(),
            entity_id: f.project_id.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: f.project_id.clone(),
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: None,
            payload_json: "{}".to_owned(),
            created_at: now.clone(),
        })
        .await
        .unwrap();
    f.db.insert_attention(db::CreateAttentionProjection {
        id: new_uuid_v4(),
        attention_type: a.attention_type.clone(),
        scope_type: "project".to_owned(),
        scope_id: f.project_id.clone(),
        identity_id: Some(f.identity_id.clone()),
        source_event_id: event.id,
        priority: 85,
        status: "open".to_owned(),
        summary: a.summary.clone(),
        details_json:
            serde_json::json!({"scope_type":"project","scope_id":f.project_id,"need":need})
                .to_string(),
        dedupe_key: a.dedupe_key.clone(),
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
    .unwrap()
}
async fn audit35_escalations(f: &ChatTurnFixture) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM agent_wake_escalation WHERE project_id=?")
        .bind(&f.project_id)
        .fetch_one(f.db.pool())
        .await
        .unwrap()
}
async fn audit35_finish_turns(f: &ChatTurnFixture) {
    sqlx::query("UPDATE agent_chat_turn_job SET status='succeeded' WHERE chat_id=?")
        .bind(&f.chat_id)
        .execute(f.db.pool())
        .await
        .unwrap();
}

/// Focus 2: a digest change must make the blocker eligible again.
#[tokio::test]
async fn audit35_changed_blocker_digest_after_an_admitted_turn_is_admitted_again() {
    let f = chat_turn_fixture().await;
    let a = incident(&f, "execution_failed", "first-cause").await;
    let service = AttentionService::new(f.db.clone());
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    audit35_finish_turns(&f).await;
    let changed = audit35_reproject(&f, &a, "a different, new cause").await;
    assert_eq!(changed.id, a.id, "same dedupe key keeps the Attention row");
    assert_eq!(changed.status, "open");
    assert_ne!(
        wake_attention_incident_digest(&changed),
        wake_attention_incident_digest(&a)
    );
    let later = (chrono::Utc::now() + chrono::Duration::minutes(10)).to_rfc3339();
    let admitted = service.sweep_once_at(&later).await.unwrap();
    assert_eq!(
        (admitted, turns(&f).await, audit35_escalations(&f).await),
        (1, 2, 0),
        "changed blocker is neither re-woken nor escalated: stranded"
    );
}

/// Focus 4/8: a blocker turn that failed before the agent could act consumes
/// the digest's only turn and goes straight to the owner.
#[tokio::test]
async fn audit35_failed_blocker_turn_does_not_consume_the_digest() {
    let f = chat_turn_fixture().await;
    incident(&f, "execution_failed", "provider-down").await;
    let service = AttentionService::new(f.db.clone());
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    AgentChatTurnWorker::with_runner(f.db.clone(), Arc::new(FailingWakeRunner))
        .run_once()
        .await
        .unwrap();
    let later = (chrono::Utc::now() + chrono::Duration::hours(2)).to_rfc3339();
    service.sweep_once_at(&later).await.unwrap();
    assert_eq!(
        (turns(&f).await, audit35_escalations(&f).await),
        (2, 0),
        "a failed (never-acted) blocker turn is treated as the digest's one turn"
    );
}

/// Focus 4: a recorded recovery outcome should stop the automatic escalation.
#[tokio::test]
async fn audit35_recorded_recovery_outcome_prevents_owner_escalation() {
    let f = chat_turn_fixture().await;
    incident(&f, "environment_not_ready", "env").await;
    let service = AttentionService::new(f.db.clone());
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    let turn_id: String = sqlx::query_scalar("SELECT id FROM agent_chat_turn_job WHERE chat_id=?")
        .bind(&f.chat_id)
        .fetch_one(f.db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE agent_wake_blocker SET recorded_outcome='recovery_action'")
        .execute(f.db.pool())
        .await
        .unwrap();
    audit35_finish_turns(&f).await;
    services::project_escalation::ProjectEscalationService::new(f.db.clone())
        .escalate_silent_turn(&turn_id)
        .await
        .unwrap();
    assert_eq!(
        audit35_escalations(&f).await,
        0,
        "silent path honours the outcome"
    );
    service
        .sweep_once_at(&(chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339())
        .await
        .unwrap();
    assert_eq!(
        audit35_escalations(&f).await,
        0,
        "sweep escalates even though the turn recorded a recovery action"
    );
}

/// Focus 3: wake_budget=3 leaves the decision bucket at 0, so an owner answer never wakes.
#[tokio::test]
async fn audit35_small_wake_budget_still_wakes_on_owner_answer() {
    let f = chat_turn_fixture().await;
    sqlx::query(
        "UPDATE project_agent_binding SET wake_budget=3 WHERE project_id=? AND state='active'",
    )
    .bind(&f.project_id)
    .execute(f.db.pool())
    .await
    .unwrap();
    let service = services::project_escalation::ProjectEscalationService::new(f.db.clone());
    let e = service
        .escalate(
            &f.project_id,
            Agent(&f.identity_id),
            api_types::ProjectEscalateRequest {
                need: "Need the staging token".to_owned(),
                task_ids: vec![],
            },
            "k",
        )
        .await
        .unwrap();
    service
        .answer(
            &f.project_id,
            &e.id,
            &f.account_id,
            api_types::AnswerProjectEscalationRequest {
                expected_version: 1,
                answer: "Token added".to_owned(),
            },
        )
        .await
        .unwrap();
    AttentionService::new(f.db.clone())
        .project_once(100)
        .await
        .unwrap();
    let stalled: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type='notification.requested' AND scope_id=?",
    )
    .bind(&f.project_id)
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    assert_eq!(
        (turns(&f).await, stalled),
        (1, 0),
        "owner answer dropped and a false 'Project Agent stopped' notice raised"
    );
}

/// Focus 3: a decision wake suppressed by its 2/h bucket is never reconsidered.
/// Ported: owner answers are uncharged (M3), so the bucket is driven by user
/// decisions; the owner-answer half is asserted on its own.
#[tokio::test]
async fn audit35_budget_suppressed_owner_answer_is_reconsidered() {
    let f = chat_turn_fixture().await;
    let attention = AttentionService::new(f.db.clone());
    let mut results = Vec::new();
    for n in 0..3 {
        let decision = incident(&f, "decision_recorded", &format!("decision-{n}")).await;
        results.push(
            attention
                .admit_wake(wake_request(&f, &decision, &now_rfc3339()))
                .await
                .unwrap(),
        );
        audit35_finish_turns(&f).await;
    }
    assert!(matches!(
        results[2],
        services::WakeAdmissionResult::Suppressed {
            reason: services::WakeSuppressionReason::BudgetExhausted
        }
    ));
    assert_eq!(
        turns(&f).await,
        2,
        "third decision suppressed by the 2/h bucket"
    );
    attention
        .sweep_once_at(&(chrono::Utc::now() + chrono::Duration::hours(2)).to_rfc3339())
        .await
        .unwrap();
    assert_eq!(
        turns(&f).await,
        3,
        "the suppressed decision is never delivered"
    );
    // Owner answers never draw on that bucket.
    let service = services::project_escalation::ProjectEscalationService::new(f.db.clone());
    for n in 0..3 {
        let e = service
            .escalate(
                &f.project_id,
                Agent(&f.identity_id),
                api_types::ProjectEscalateRequest {
                    need: format!("Need {n}"),
                    task_ids: vec![],
                },
                &format!("k{n}"),
            )
            .await
            .unwrap();
        service
            .answer(
                &f.project_id,
                &e.id,
                &f.account_id,
                api_types::AnswerProjectEscalationRequest {
                    expected_version: 1,
                    answer: format!("Answer {n}"),
                },
            )
            .await
            .unwrap();
        attention.project_once(100).await.unwrap();
        audit35_finish_turns(&f).await;
    }
    assert_eq!(turns(&f).await, 6, "every owner answer wakes the Agent");
}

/// Focus 1/5: upgrade path. An imported admitted digest whose Attention source
/// is unchanged is rekeyed and escalated on the first sweep (one notice per row).
#[tokio::test]
async fn audit35_imported_admitted_blocker_escalates_on_first_sweep() {
    let f = chat_turn_fixture().await;
    let a = incident(&f, "execution_failed", "legacy").await;
    let service = AttentionService::new(f.db.clone());
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    audit35_finish_turns(&f).await;
    // Shape of a row the V202610042145 import writes for a pre-upgrade turn.
    sqlx::query(
        "UPDATE agent_wake_blocker SET incident_digest='legacy-format', legacy_source_event_id=?",
    )
    .bind(&a.source_event_id)
    .execute(f.db.pool())
    .await
    .unwrap();
    service
        .sweep_once_at(&(chrono::Utc::now() + chrono::Duration::minutes(10)).to_rfc3339())
        .await
        .unwrap();
    // Ported to the owner's M5 rule: an imported blocker gets one normal
    // re-admission and no escalation.
    assert_eq!(
        (turns(&f).await, audit35_escalations(&f).await),
        (2, 0),
        "first post-upgrade sweep escalates every imported unchanged blocker"
    );
}

/// Focus 5: a busy chat is audited as a missing responder binding.
#[tokio::test]
async fn audit35_busy_chat_is_not_audited_as_missing_binding() {
    let f = chat_turn_fixture().await;
    let service = AttentionService::new(f.db.clone());
    let a = incident(&f, "delivery_followup", "first").await;
    service
        .admit_wake(wake_request(&f, &a, &now_rfc3339()))
        .await
        .unwrap();
    let b = incident(&f, "delivery_followup", "second").await;
    let result = service
        .admit_wake(wake_request(&f, &b, &now_rfc3339()))
        .await
        .unwrap();
    assert!(
        !matches!(result, services::WakeAdmissionResult::SetupRequired { .. }),
        "busy chat recorded as {result:?}"
    );
}

/// Focus 1: a non-first batch member re-arms while the batch turn is still queued.
#[tokio::test]
async fn audit35_second_batch_member_is_not_rewoken_while_batch_turn_is_queued() {
    let f = chat_turn_fixture().await;
    let _a = incident(&f, "execution_failed", "one").await;
    let b = incident(&f, "review_risk", "two").await;
    let service = AttentionService::new(f.db.clone());
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    // Batch turn is still queued; B's material state changes.
    audit35_reproject(&f, &b, "review risk changed").await;
    service
        .sweep_once_at(&(chrono::Utc::now() + chrono::Duration::minutes(10)).to_rfc3339())
        .await
        .unwrap();
    assert_eq!(
        (turns(&f).await, charged(&f, "blocker").await),
        (1, 1),
        "second turn admitted for an Attention whose batch turn has not run"
    );
}

// ---- restored wake coverage (fix round M6), on the single admission stage ----
// These were consumer tests driven by synthetic `agent.wake.admitted` events.
// Attention now admits the turn itself, so each drives `admit_wake` or the
// sweep and keeps the original behavioural assertions.

async fn file_database() -> (Arc<SqliteDb>, String) {
    let path = std::env::temp_dir()
        .join(format!("forge-wake-turn-{}.sqlite", new_uuid_v4()))
        .display()
        .to_string();
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

async fn bound_project(db: &SqliteDb, identity_id: &str, profile_id: &str) -> (String, String) {
    let project_id = new_uuid_v4();
    let account_id = new_uuid_v4();
    let now = now_rfc3339();
    UserRepo::create_user(
        db,
        &User {
            id: account_id.clone(),
            email: format!("{account_id}@example.test"),
            password_hash: "test".to_owned(),
            display_name: Some("Wake Turn Test".to_owned()),
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO project (id, name, owner_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(&project_id)
    .bind("wake-turn-project")
    .bind(&account_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE agent_identity SET owner_id = ? WHERE id = ?")
        .bind(&account_id)
        .bind(identity_id)
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

/// One open Project incident with its own source event.
async fn seed_incident(
    db: &SqliteDb,
    project_id: &str,
    category: &str,
    incident_key: &str,
    summary: &str,
    details: serde_json::Value,
    recommended_action: &str,
) -> db::AttentionProjection {
    let now = now_rfc3339();
    let event = db
        .append_event(CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "execution.failed".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: new_uuid_v4(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: project_id.to_owned(),
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: None,
            payload_json: "{}".to_owned(),
            created_at: now.clone(),
        })
        .await
        .unwrap();
    db.insert_attention(db::CreateAttentionProjection {
        id: new_uuid_v4(),
        attention_type: category.to_owned(),
        scope_type: "project".to_owned(),
        scope_id: project_id.to_owned(),
        identity_id: None,
        source_event_id: event.id,
        priority: 85,
        status: "open".to_owned(),
        summary: summary.to_owned(),
        details_json: details.to_string(),
        dedupe_key: incident_key.to_owned(),
        occurred_at: now.clone(),
        updated_at: now,
        acknowledged_at: None,
        snoozed_until: None,
        resolved_at: None,
        updated_by_user_id: None,
        recommended_action: recommended_action.to_owned(),
        source_sequence: Some(event.sequence),
    })
    .await
    .unwrap()
}

fn project_request(
    identity_id: &str,
    project_id: &str,
    a: &db::AttentionProjection,
    now: &str,
) -> services::WakeAdmissionRequest {
    services::WakeAdmissionRequest {
        identity_id: identity_id.to_owned(),
        scope_type: "project".to_owned(),
        scope_id: project_id.to_owned(),
        incident_key: a.dedupe_key.clone(),
        lease_owner: new_uuid_v4(),
        correlation_id: a.id.clone(),
        causation_id: Some(a.source_event_id.clone()),
        caused_by_identity_id: None,
        reaction_depth: 0,
        now: now.to_owned(),
        lease_seconds: 60,
        cooldown_seconds: 300,
    }
}

async fn admitted_count(db: &SqliteDb) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_wake_disposition WHERE disposition = 'turn_admitted'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap()
}

async fn chat_turn_count(db: &SqliteDb, chat_id: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM agent_chat_turn_job WHERE chat_id = ?")
        .bind(chat_id)
        .fetch_one(db.pool())
        .await
        .unwrap()
}

/// The admitted wake turn for one incident occurrence: (id, content, metadata).
async fn wake_turn_for(
    db: &SqliteDb,
    chat_id: &str,
    a: &db::AttentionProjection,
) -> (String, String, String) {
    sqlx::query_as(
        "SELECT job.id, message.content, message.source_metadata_json
         FROM agent_chat_turn_job AS job
         JOIN agent_chat_message AS message ON message.id = job.triggering_message_id
         WHERE job.chat_id = ? AND job.dedupe_key LIKE ?",
    )
    .bind(chat_id)
    .bind(format!("%:{}:{}:%", a.dedupe_key, a.source_event_id))
    .fetch_one(db.pool())
    .await
    .unwrap()
}

/// A runner whose backend fails without typed evidence (retryable, unclassified).
struct BackendFailingWakeRunner;
#[async_trait]
impl AgentChatTurnRunner for BackendFailingWakeRunner {
    async fn run_turn(
        &self,
        _job: &db::AgentChatTurnJob,
        _cancellation: CancellationToken,
    ) -> services::Result<CompletedAgentChatTurn> {
        Err(ServiceError::InvalidOperation {
            message: "backend unavailable".to_owned(),
        })
    }
}

#[tokio::test]
async fn admitted_wake_becomes_a_project_agent_turn() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    let a = seed_incident(
        &db,
        &project_id,
        "execution_failed",
        &format!("attention:execution_failed:project:{project_id}:task:task-1"),
        "Task execution failed",
        serde_json::json!({"scope_type": "project", "scope_id": project_id}),
        "inspect_run",
    )
    .await;
    let service = AttentionService::new(Arc::clone(&db));
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    let (turn_count, status, responder): (i64, String, String) = sqlx::query_as(
        "SELECT COUNT(*), MAX(status), MAX(responder_identity_id)
         FROM agent_chat_turn_job WHERE chat_id = ?",
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

    // Replays (another sweep, a direct admission) never create a second turn.
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 0);
    assert!(matches!(
        service
            .admit_wake(project_request(
                &identity_id,
                &project_id,
                &a,
                &now_rfc3339()
            ))
            .await
            .unwrap(),
        services::WakeAdmissionResult::Suppressed {
            reason: services::WakeSuppressionReason::DuplicateIncident
        }
    ));
    assert_eq!(
        chat_turn_count(&db, &chat_id).await,
        1,
        "replay must reuse the turn"
    );
}

/// One milestone whose acceptance matrix an Agent is expected to settle.
struct DeliveryMilestoneFixture {
    milestone_id: String,
    milestone_revision_id: String,
    agent_check_id: String,
    manual_check_id: String,
}

async fn seed_delivery_milestone(db: &SqliteDb, project_id: &str) -> DeliveryMilestoneFixture {
    let now = now_rfc3339();
    let user_id: String = sqlx::query_scalar("SELECT owner_id FROM project WHERE id = ?")
        .bind(project_id)
        .fetch_one(db.pool())
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

async fn seed_delivery_incident(
    db: &SqliteDb,
    project_id: &str,
    incident_key: &str,
    task_id: &str,
) -> db::AttentionProjection {
    seed_incident(
        db,
        project_id,
        "delivery_followup",
        incident_key,
        "Task completed; reconcile validation, evidence, and readiness",
        serde_json::json!({
            "scope_type": "project",
            "scope_id": project_id,
            "entity_type": "task",
            "entity_id": task_id,
        }),
        "reconcile_delivery",
    )
    .await
}

/// The wake that fires when the last Task finishes is the moment validation is
/// owed: it names the exact ids and requires the record itself.
#[tokio::test]
async fn delivery_followup_with_all_tasks_done_orders_validation_before_readiness() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    let fixture = seed_delivery_milestone(&db, &project_id).await;
    let task_id = seed_governed_task(&db, &project_id, &fixture.milestone_id, "done").await;
    let service = AttentionService::new(Arc::clone(&db));
    let first = seed_delivery_incident(
        &db,
        &project_id,
        &format!("attention:delivery_followup:project:{project_id}:task:done"),
        &task_id,
    )
    .await;
    let before = admitted_count(&db).await;
    service
        .admit_wake(project_request(
            &identity_id,
            &project_id,
            &first,
            &now_rfc3339(),
        ))
        .await
        .unwrap();
    assert_eq!(admitted_count(&db).await - before, 1);
    let (_, content, source_metadata_json) = wake_turn_for(&db, &chat_id, &first).await;
    assert!(content.contains("every Task bound to it is done"));
    assert!(content.contains(&format!("milestone_id={}", fixture.milestone_id)));
    assert!(content.contains("milestone_version=3"));
    assert!(content.contains(&format!(
        "definition_revision_id={}",
        fixture.milestone_revision_id
    )));
    assert!(content.contains("`project.validation` (action `record`)"));
    assert!(content.contains(&fixture.agent_check_id));
    assert!(content.contains(&fixture.manual_check_id));
    assert!(content.contains("you may never record one yourself"));
    let source_metadata: serde_json::Value = serde_json::from_str(&source_metadata_json).unwrap();
    assert_eq!(
        source_metadata["turn_postcondition"]["required_event_type"],
        "project.milestone.check.recorded",
        "the turn owes the validation record, not a readiness evaluation"
    );

    settle_check(
        &db,
        &project_id,
        &fixture,
        &fixture.agent_check_id,
        "passed",
    )
    .await;
    let second = seed_delivery_incident(
        &db,
        &project_id,
        &format!("attention:delivery_followup:project:{project_id}:task:done:2"),
        &task_id,
    )
    .await;
    let before = admitted_count(&db).await;
    service
        .admit_wake(project_request(
            &identity_id,
            &project_id,
            &second,
            &now_rfc3339(),
        ))
        .await
        .unwrap();
    assert_eq!(admitted_count(&db).await - before, 1);
    let (_, second_content, second_metadata) = wake_turn_for(&db, &chat_id, &second).await;
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

#[tokio::test]
async fn delivery_followup_reports_open_tasks_without_claiming_completion() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    let fixture = seed_delivery_milestone(&db, &project_id).await;
    let done_task = seed_governed_task(&db, &project_id, &fixture.milestone_id, "done").await;
    seed_governed_task(&db, &project_id, &fixture.milestone_id, "in_progress").await;
    let a = seed_delivery_incident(
        &db,
        &project_id,
        &format!("attention:delivery_followup:project:{project_id}:task:done"),
        &done_task,
    )
    .await;
    AttentionService::new(Arc::clone(&db))
        .admit_wake(project_request(
            &identity_id,
            &project_id,
            &a,
            &now_rfc3339(),
        ))
        .await
        .unwrap();
    let (_, content, _) = wake_turn_for(&db, &chat_id, &a).await;
    assert!(content.contains("1 Task(s) still open"));
    assert!(!content.contains("every Task bound to it is done"));
}

#[tokio::test]
async fn delivery_followup_requires_newer_readiness_before_turn_success() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
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
    let a = seed_delivery_incident(
        &db,
        &project_id,
        &format!("attention:delivery_followup:project:{project_id}:task:done"),
        &task_id,
    )
    .await;
    AttentionService::new(Arc::clone(&db))
        .admit_wake(project_request(
            &identity_id,
            &project_id,
            &a,
            &now_rfc3339(),
        ))
        .await
        .unwrap();
    let (turn_id, content, source_metadata_json) = wake_turn_for(&db, &chat_id, &a).await;
    // The postcondition is anchored on the admitting audit event.
    let wake_event_sequence: i64 = sqlx::query_scalar(
        "SELECT sequence FROM domain_event WHERE event_type = 'agent.wake.admitted'
         AND json_extract(payload_json, '$.turn_job_id') = ?",
    )
    .bind(&turn_id)
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
        "SELECT COUNT(*) FROM agent_chat_message WHERE chat_id = ? AND author_type = 'agent'",
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
    db.append_event(CreateDomainEvent {
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
    })
    .await
    .unwrap();
    sqlx::query(
        "UPDATE agent_chat_turn_job SET next_attempt_at = '1970-01-01T00:00:00Z',
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
    let a = seed_incident(
        &db,
        &attention_project_id,
        "delivery_followup",
        &format!("attention:cross_project:project:{attention_project_id}"),
        "Other project incident",
        serde_json::json!({"scope_type": "project", "scope_id": attention_project_id}),
        "inspect_run",
    )
    .await;
    // The request claims the first Project; the incident belongs to the other.
    let service = AttentionService::new(Arc::clone(&db));
    let before = admitted_count(&db).await;
    let result = service
        .admit_wake(project_request(
            &identity_id,
            &event_project_id,
            &a,
            &now_rfc3339(),
        ))
        .await
        .unwrap();
    assert!(matches!(
        result,
        services::WakeAdmissionResult::Suppressed {
            reason: services::WakeSuppressionReason::IneligibleScope
        }
    ));
    assert_eq!(admitted_count(&db).await - before, 0);
    for chat_id in [&event_chat_id, &attention_chat_id] {
        assert_eq!(
            chat_turn_count(&db, chat_id).await,
            0,
            "cross-project wake must not enqueue a turn"
        );
    }
    // An incident with no Attention row is resolved, not admitted.
    let mut missing = project_request(&identity_id, &event_project_id, &a, &now_rfc3339());
    missing.incident_key = format!("attention:missing:project:{event_project_id}");
    assert!(matches!(
        service.admit_wake(missing).await.unwrap(),
        services::WakeAdmissionResult::Suppressed {
            reason: services::WakeSuppressionReason::ResolvedIncident
        }
    ));
}

#[tokio::test]
async fn admitted_wake_runner_failure_is_terminal_on_budget_and_keeps_admission_disposition() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    let a = seed_incident(
        &db,
        &project_id,
        "delivery_followup",
        &format!("attention:runner_failure:project:{project_id}"),
        "Runner failure",
        serde_json::json!({"scope_type": "project", "scope_id": project_id}),
        "inspect_run",
    )
    .await;
    AttentionService::new(Arc::clone(&db))
        .admit_wake(project_request(
            &identity_id,
            &project_id,
            &a,
            &now_rfc3339(),
        ))
        .await
        .unwrap();
    let (turn_id, _, _) = wake_turn_for(&db, &chat_id, &a).await;
    let worker = AgentChatTurnWorker::with_runner(
        Arc::clone(&db),
        Arc::new(BackendFailingWakeRunner) as Arc<dyn AgentChatTurnRunner>,
    );
    assert_eq!(worker.run_once().await.unwrap(), 1);
    let first = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*db, &turn_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.status, AgentChatTurnState::RetryWait);
    assert_eq!(first.attempt_count, 1);
    for expected_attempt in [1_i64, 2_i64] {
        sqlx::query(
            "UPDATE agent_chat_turn_job SET next_attempt_at = '1970-01-01T00:00:00Z',
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
    let (count, disposition, reason): (i64, String, String) = sqlx::query_as(
        "SELECT COUNT(*), MAX(disposition), MAX(reason) FROM agent_wake_disposition
         WHERE turn_job_id = ?",
    )
    .bind(&turn_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        (count, disposition.as_str(), reason.as_str()),
        (1, "turn_admitted", "turn_admitted")
    );
}

#[tokio::test]
async fn setup_required_wake_reconsiders_after_binding_change() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    sqlx::query(
        "UPDATE project_agent_binding
         SET identity_id = NULL, profile_id = NULL, state = 'agent_setup_required',
             updated_at = ?, version = version + 1
         WHERE project_id = ?",
    )
    .bind(now_rfc3339())
    .bind(&project_id)
    .execute(db.pool())
    .await
    .unwrap();
    let a = seed_incident(
        &db,
        &project_id,
        "execution_failed",
        &format!("attention:setup:project:{project_id}"),
        "Setup incident",
        serde_json::json!({"scope_type": "project", "scope_id": project_id}),
        "configure_binding",
    )
    .await;
    let service = AttentionService::new(Arc::clone(&db));
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 0);
    let (disposition, linked): (String, Option<String>) = sqlx::query_as(
        "SELECT disposition, attention_id FROM agent_wake_disposition ORDER BY created_at LIMIT 1",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(disposition, "setup_required");
    assert_eq!(
        linked.as_deref(),
        Some(a.id.as_str()),
        "setup must link an Attention row"
    );
    sqlx::query(
        "UPDATE project_agent_binding
         SET identity_id = ?, profile_id = ?, state = 'active',
             updated_at = ?, version = version + 1
         WHERE project_id = ?",
    )
    .bind(&identity_id)
    .bind(&profile_id)
    .bind("9999-01-01T00:00:00Z")
    .bind(&project_id)
    .execute(db.pool())
    .await
    .unwrap();
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    assert_eq!(admitted_count(&db).await, 1);
    assert_eq!(chat_turn_count(&db, &chat_id).await, 1);
}

/// The whole autonomy loop: a Task execution fails, the durable attempt event
/// stays audit-only, the Task interruption commits, and only that wakes the
/// Project Agent, through the projection and its sweep.
#[tokio::test]
async fn actionable_task_interruption_wakes_the_project_agent_end_to_end() {
    use db::{
        ClaimExecutionLease, CreateExecution, ExecutionLeaseDisposition, ExecutionRepo,
        ExecutionStatus, ResumePolicy, StopReason, TaskRepo, TerminalizeExecution, UpdateTask,
    };
    let (db, database_path) = file_database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
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
    let running = ExecutionRepo::create_with_lease(
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
            hard_deadline_at: Some("9999-01-01T00:00:00+00:00".to_owned()),
            now: now.clone(),
        },
    )
    .await
    .unwrap();
    ExecutionRepo::terminalize(
        &*db,
        TerminalizeExecution {
            execution_id: execution_id.clone(),
            expected_version: running.execution_version,
            lease_owner: running.lease_owner.clone(),
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
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    let annotation = serde_json::json!({
        "type": "executor_failed",
        "blocking_reason": "executor_failed",
        "blocked_by": "system:executor",
        "blocked_at": now,
        "blocked_execution_id": execution_id,
        "artifact": {"kind": "execution", "id": execution_id},
        "recovery_actions": ["reexecute", "reset_to_initial", "cancel_task"]
    });
    TaskRepo::update(
        &*db,
        UpdateTask {
            id: task.id,
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(annotation.to_string())),
            blocked_json: Some(Some(
                serde_json::json!({
                    "reason": "gemini exited with status 1",
                    "kind": "internal_command_failed",
                    "execution_id": execution_id
                })
                .to_string(),
            )),
            failed_json: Some(None),
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
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
        "only the actionable Task disposition must admit one wake"
    );
    let (status, responder, content): (String, String, String) = sqlx::query_as(
        "SELECT job.status, job.responder_identity_id, message.content
         FROM agent_chat_turn_job AS job
         JOIN agent_chat_message AS message ON message.id = job.triggering_message_id
         WHERE job.chat_id = ?",
    )
    .bind(&chat_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(status, "queued");
    assert_eq!(responder, identity_id);
    assert!(content.contains("Task needs recovery"));
    drop(db);
    let _ = std::fs::remove_file(database_path);
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
        AttentionService::new(Arc::clone(&db))
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
        AttentionService::new(Arc::clone(&fixture.db))
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
    AttentionService::new(Arc::clone(&db))
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
    let service = AttentionService::new(Arc::clone(&db));
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
    let service = AttentionService::new(Arc::clone(&db));
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
    let service = AttentionService::new(Arc::clone(&db));
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
    let service = AttentionService::new(f.db.clone());
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
    let a = AttentionService::new(Arc::clone(&db));
    let b = AttentionService::new(Arc::clone(&db));
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
    let service = AttentionService::new(f.db.clone());
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

#[tokio::test]
async fn conflict_hotspot_wake_routes_to_project_agent_with_bounded_directive() {
    let fixture = chat_turn_fixture().await;
    fixture
        .db
        .append_event(CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project.conflict_hotspot.detected".into(),
            entity_type: "project".into(),
            entity_id: fixture.project_id.clone(),
            actor_type: "system".into(),
            actor_id: Some("conflict-hotspots".into()),
            scope_type: "project".into(),
            scope_id: fixture.project_id.clone(),
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(new_uuid_v4()),
            payload_json:
                serde_json::json!({"project_id": fixture.project_id, "path": "src/共有.rs",
            "task_ids": ["task-3", "task-2", "task-1"], "handoff_count": 3, "window_days": 7})
                .to_string(),
            created_at: now_rfc3339(),
        })
        .await
        .unwrap();
    AttentionService::new(Arc::clone(&fixture.db))
        .project_once(100)
        .await
        .unwrap();
    let admitted: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_event WHERE event_type = 'agent.wake.admitted' AND scope_type = 'project' AND scope_id = ?")
        .bind(&fixture.project_id).fetch_one(fixture.db.pool()).await.unwrap();
    assert_eq!(admitted, 1);
    let (chat_id, responder, content): (String, String, String) = sqlx::query_as(
        "SELECT j.chat_id, j.responder_identity_id, m.content FROM agent_chat_turn_job j
         JOIN agent_chat_message m ON m.id = j.triggering_message_id
         WHERE j.chat_id = ? AND m.content LIKE '%Category: conflict_hotspot%'",
    )
    .bind(&fixture.chat_id)
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    assert_eq!(chat_id, fixture.chat_id);
    assert_eq!(responder, fixture.identity_id);
    let directive = content.lines().last().unwrap();
    assert!(content.contains("src/共有.rs"));
    for task in ["task-1", "task-2", "task-3"] {
        assert_eq!(content.matches(task).count(), 1);
    }
    assert!(directive.contains("path and Tasks in Details"));
    assert!(directive.contains("Propose one Task via `task.propose`"));
    assert!(directive.contains("unless an open Task already does"));
    assert!(!directive.contains("Resolve"));
    assert!(directive.split_whitespace().count() <= 40);
}

// Regression scenarios copied from the independent 3.9(b) audit.

async fn audit39b_detection(fixture: &ChatTurnFixture, path: &str, count: i64) {
    let existing: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task WHERE project_id = ? AND title LIKE 'audit39b:%'",
    )
    .bind(&fixture.project_id)
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    for number in existing..count {
        let task_id = new_uuid_v4();
        let id = new_uuid_v4();
        let now = now_rfc3339();
        let reason = format!("Conflict handed to worker: {path}");
        sqlx::query("INSERT INTO task (id, project_id, title, status, created_at, updated_at) VALUES (?, ?, ?, 'merge_failed', ?, ?)")
            .bind(&task_id).bind(&fixture.project_id).bind(format!("audit39b:{number}"))
            .bind(&now).bind(&now).execute(fixture.db.pool()).await.unwrap();
        let mut tx = db::begin_immediate(fixture.db.pool()).await.unwrap();
        sqlx::query("INSERT INTO transition_log (id, task_id, from_state, to_state, triggered_by, trigger_reason, created_at, bridge_kind, bridge_payload) VALUES (?, ?, 'merging', 'merge_failed', 'system:workflow', ?, ?, 'conflict_handoff', ?)")
            .bind(&id).bind(&task_id).bind(&reason).bind(&now).bind(serde_json::json!({"paths":[path]}).to_string()).execute(&mut *tx).await.unwrap();
        fixture
            .db
            .append_event_in_tx(
                &mut tx,
                &CreateDomainEvent::task_transition(
                    id,
                    task_id,
                    &fixture.project_id,
                    "merging",
                    "merge_failed",
                    None,
                    "system:workflow",
                    reason,
                    false,
                    now,
                    serde_json::Value::Null,
                ),
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    services::worker_runtime::WorkerRuntime::new(
        Arc::clone(&fixture.db),
        Arc::new(
            services::worker_runtime::conflict_hotspot::ConflictHotspotConsumer::new(Arc::clone(
                &fixture.db,
            )),
        ),
    )
    .run_once(100)
    .await
    .unwrap();
}

async fn audit39b_hotspot_turns(fixture: &ChatTurnFixture) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_chat_turn_job j
         JOIN agent_chat_message m ON m.id = j.triggering_message_id
         WHERE j.chat_id = ? AND m.content LIKE '%Category: conflict_hotspot%'",
    )
    .bind(&fixture.chat_id)
    .fetch_one(fixture.db.pool())
    .await
    .unwrap()
}

/// Two adjacent handoffs at/above threshold produce one stable detection and
/// exactly one Project Agent turn.
#[tokio::test]
async fn audit39b_refresh_before_delivery_loses_the_only_wake() {
    let fixture = chat_turn_fixture().await;
    audit39b_detection(&fixture, "src/shared.rs", 3).await;
    audit39b_detection(&fixture, "src/shared.rs", 4).await;
    AttentionService::new(Arc::clone(&fixture.db))
        .project_once(100)
        .await
        .unwrap();
    let detections: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type = 'project.conflict_hotspot.detected'",
    )
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    assert_eq!(detections, 1);
    assert_eq!(
        audit39b_hotspot_turns(&fixture).await,
        1,
        "open conflict_hotspot incident produced no Project Agent turn"
    );
}

#[tokio::test]
async fn audit39b_each_refresh_after_cooldown_rewakes_project_agent() {
    let fixture = chat_turn_fixture().await;
    let attention = AttentionService::new(Arc::clone(&fixture.db));
    audit39b_detection(&fixture, "src/shared.rs", 3).await;
    attention.project_once(100).await.unwrap();
    for count in 4..=20 {
        // Simulate the 300 s cooldown elapsing between two later handoffs.
        sqlx::query("UPDATE agent_wake_lease SET leased_until = '2000-01-01T00:00:00Z', cooldown_until = '2000-01-01T00:00:00Z'")
            .execute(fixture.db.pool()).await.unwrap();
        audit39b_detection(&fixture, "src/shared.rs", count).await;
        attention.project_once(100).await.unwrap();
    }
    let admitted: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type = 'agent.wake.admitted'",
    )
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    let budget: Option<i64> =
        sqlx::query_scalar("SELECT MAX(admitted_count) FROM agent_wake_budget_window")
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
    assert_eq!(audit39b_hotspot_turns(&fixture).await, 1);
    assert_eq!(budget, Some(1));
    assert_eq!(
        admitted, 1,
        "every refresh after cooldown re-woke the Project Agent for the same open incident"
    );
}

/// Ported from attention_service.rs (needs a ready Project chat now): a live
/// lease suppresses a duplicate, an expired lease inside the cooldown
/// suppresses as cooldown, a replay of the admitted source charges nothing,
/// a binding replacement cannot double-analyse a leased incident, and a
/// saturated bucket suppresses without another charge.
#[tokio::test]
async fn wake_policy_persists_cooldown_budget_and_global_identity_suppression() {
    let f = chat_turn_fixture().await;
    let service = AttentionService::new(f.db.clone());
    let t0 = chrono::Utc::now();
    let at = |seconds: i64| (t0 + chrono::Duration::seconds(seconds)).to_rfc3339();
    let a = incident(&f, "delivery_followup", "cooldown").await;
    let mut first = wake_request(&f, &a, &at(0));
    first.lease_seconds = 30;
    first.cooldown_seconds = 60;
    assert!(matches!(
        service.admit_wake(first.clone()).await.unwrap(),
        services::WakeAdmissionResult::Admitted { .. }
    ));
    // Replaying the same source after the cooldown returns the original
    // admission without charging again.
    let mut replay = first.clone();
    replay.now = at(600);
    assert!(matches!(
        service.admit_wake(replay).await.unwrap(),
        services::WakeAdmissionResult::Admitted { .. }
    ));
    assert_eq!(charged(&f, "delivery").await, 1);
    // A new occurrence of the same incident while the lease is live.
    let refreshed = incident(&f, "delivery_followup", "cooldown").await;
    let mut duplicate = wake_request(&f, &refreshed, &at(10));
    duplicate.lease_seconds = 30;
    duplicate.cooldown_seconds = 60;
    assert!(matches!(
        service.admit_wake(duplicate.clone()).await.unwrap(),
        services::WakeAdmissionResult::Suppressed {
            reason: services::WakeSuppressionReason::DuplicateIncident
        }
    ));
    let mut cooling = duplicate;
    cooling.now = at(31);
    cooling.lease_owner = "replacement-worker".to_owned();
    assert!(matches!(
        service.admit_wake(cooling).await.unwrap(),
        services::WakeAdmissionResult::Suppressed {
            reason: services::WakeSuppressionReason::Cooldown
        }
    ));
    // A replacement binding cannot analyse an incident the old identity holds.
    let global = incident(&f, "delivery_followup", "global").await;
    assert!(matches!(
        service
            .admit_wake(wake_request(&f, &global, &at(0)))
            .await
            .unwrap(),
        services::WakeAdmissionResult::Admitted { .. }
    ));
    let replacement = new_uuid_v4();
    let replacement_profile =
        owned_identity_with_profile(&f.db, &replacement, &f.account_id, &new_uuid_v4()).await;
    sqlx::query("UPDATE project_agent_binding SET identity_id = ?, profile_id = ?, version = version + 1 WHERE project_id = ? AND state = 'active'")
        .bind(&replacement)
        .bind(&replacement_profile)
        .bind(&f.project_id)
        .execute(f.db.pool())
        .await
        .unwrap();
    let refreshed_global = incident(&f, "delivery_followup", "global").await;
    let mut other = wake_request(&f, &refreshed_global, &at(10));
    other.identity_id = replacement.clone();
    assert!(matches!(
        service.admit_wake(other).await.unwrap(),
        services::WakeAdmissionResult::Suppressed {
            reason: services::WakeSuppressionReason::DuplicateIncident
        }
    ));
    // A saturated bucket suppresses without counting the suppression.
    sqlx::query("UPDATE agent_wake_budget_window SET admitted_count = 4 WHERE scope_id = ? AND category = 'delivery'")
        .bind(&f.project_id)
        .execute(f.db.pool())
        .await
        .unwrap();
    let budget = incident(&f, "delivery_followup", "budget").await;
    let mut request = wake_request(&f, &budget, &at(30));
    request.identity_id = replacement;
    assert!(matches!(
        service.admit_wake(request).await.unwrap(),
        services::WakeAdmissionResult::Suppressed {
            reason: services::WakeSuppressionReason::BudgetExhausted
        }
    ));
    assert_eq!(charged(&f, "delivery").await, 4);
}

// ---- fix round 3.5: new coverage ------------------------------------------

struct TransientWakeRunner;
#[async_trait]
impl AgentChatTurnRunner for TransientWakeRunner {
    async fn run_turn(
        &self,
        _job: &db::AgentChatTurnJob,
        _cancellation: CancellationToken,
    ) -> services::Result<CompletedAgentChatTurn> {
        Err(ServiceError::TurnFailure {
            failure: api_types::TurnFailure::Transient { retry_after: None },
            error: Box::new(ServiceError::InvalidOperation {
                message: "provider outage".to_owned(),
            }),
        })
    }
}

async fn stall_notices(f: &ChatTurnFixture) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type='notification.requested' AND scope_id=?",
    )
    .bind(&f.project_id)
    .fetch_one(f.db.pool())
    .await
    .unwrap()
}

/// M1: an infrastructure failure never consumes the digest; the sweep
/// re-admits after the cooldown and nothing reaches the owner.
#[tokio::test]
async fn infrastructure_failed_blocker_turn_is_readmitted_after_cooldown() {
    let f = chat_turn_fixture().await;
    incident(&f, "execution_failed", "outage").await;
    let service = AttentionService::new(f.db.clone());
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    let worker = AgentChatTurnWorker::with_runner(f.db.clone(), Arc::new(TransientWakeRunner));
    for _ in 0..3 {
        sqlx::query("UPDATE agent_chat_turn_job SET next_attempt_at = NULL WHERE chat_id = ? AND status = 'retry_wait'")
            .bind(&f.chat_id)
            .execute(f.db.pool())
            .await
            .unwrap();
        worker.run_once().await.unwrap();
    }
    let status: String =
        sqlx::query_scalar("SELECT status FROM agent_chat_turn_job WHERE chat_id=?")
            .bind(&f.chat_id)
            .fetch_one(f.db.pool())
            .await
            .unwrap();
    assert_eq!(status, "failed");
    // Inside the Project's batch cooldown nothing is re-admitted.
    service.sweep_once_at(&now_rfc3339()).await.unwrap();
    assert_eq!(turns(&f).await, 1);
    let later = (chrono::Utc::now() + chrono::Duration::minutes(6)).to_rfc3339();
    assert_eq!(service.sweep_once_at(&later).await.unwrap(), 1);
    assert_eq!(
        (
            turns(&f).await,
            audit35_escalations(&f).await,
            stall_notices(&f).await
        ),
        (2, 0, 0)
    );
}

/// M1: a deterministic provider failure raises "can't run" once, never
/// escalates, and holds the blocker until the responder changes.
#[tokio::test]
async fn deterministic_blocker_failure_notifies_once_and_holds_until_the_profile_changes() {
    let f = chat_turn_fixture().await;
    incident(&f, "execution_failed", "auth").await;
    let service = AttentionService::new(f.db.clone());
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    // A usage limit with no reset hint holds for an hour.
    AgentChatTurnWorker::with_runner(f.db.clone(), Arc::new(FailingWakeRunner))
        .run_once()
        .await
        .unwrap();
    let title: String = sqlx::query_scalar(
        "SELECT json_extract(payload_json,'$.title') FROM domain_event
         WHERE event_type='notification.requested' AND scope_id=?",
    )
    .bind(&f.project_id)
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    assert_eq!(title, "Project Agent can't run: usage_limit");
    let soon = (chrono::Utc::now() + chrono::Duration::minutes(30)).to_rfc3339();
    service.sweep_once_at(&soon).await.unwrap();
    assert_eq!(
        (
            turns(&f).await,
            audit35_escalations(&f).await,
            stall_notices(&f).await
        ),
        (1, 0, 1),
        "held: no re-wake, no escalation, one notice"
    );
    // A new Profile re-arms the blocker immediately.
    select_profile(&f.db, &f.identity_id, &new_uuid_v4(), "repaired").await;
    assert_eq!(service.sweep_once_at(&soon).await.unwrap(), 1);
    assert_eq!(turns(&f).await, 2);
    assert_eq!(audit35_escalations(&f).await, 0);
}

/// M2: a turn whose lease keeps expiring is refunded three times, then each
/// expiry counts against max_attempts until the turn fails.
#[tokio::test]
async fn lease_expiry_refunds_are_capped_at_three_per_turn() {
    let f = chat_turn_fixture().await;
    let a = incident(&f, "delivery_followup", "crash-loop").await;
    AttentionService::new(f.db.clone())
        .admit_wake(wake_request(&f, &a, &now_rfc3339()))
        .await
        .unwrap();
    let worker = AgentChatTurnWorker::with_runner(f.db.clone(), Arc::new(FailingWakeRunner));
    let mut seen = Vec::new();
    for _ in 0..6 {
        sqlx::query("UPDATE agent_chat_turn_job SET status='leased',attempt_count=attempt_count+1,invocation_count=invocation_count+1,lease_owner='crashed',leased_until='2000-01-01T00:00:00Z',next_attempt_at=NULL WHERE chat_id=? AND status IN ('queued','retry_wait')")
            .bind(&f.chat_id)
            .execute(f.db.pool())
            .await
            .unwrap();
        // A clock in the past recovers the lease but never re-claims the turn.
        worker
            .run_once_at(chrono::Utc::now() - chrono::Duration::days(1))
            .await
            .unwrap();
        let row: (String, i64, i64) = sqlx::query_as(
            "SELECT status, attempt_count, lease_refund_count FROM agent_chat_turn_job WHERE chat_id=?",
        )
        .bind(&f.chat_id)
        .fetch_one(f.db.pool())
        .await
        .unwrap();
        seen.push(row.clone());
        if row.0 == "failed" {
            break;
        }
    }
    assert_eq!(
        seen,
        vec![
            ("retry_wait".to_owned(), 0, 1),
            ("retry_wait".to_owned(), 0, 2),
            ("retry_wait".to_owned(), 0, 3),
            ("retry_wait".to_owned(), 1, 3),
            ("retry_wait".to_owned(), 2, 3),
            ("failed".to_owned(), 3, 3),
        ]
    );
}

/// M3: a small budget is one shared pool, so no category is starved and no
/// false "stopped" notice is raised; the pool still bounds the hour.
#[tokio::test]
async fn small_wake_budget_shares_one_pool_across_categories() {
    let f = chat_turn_fixture().await;
    sqlx::query(
        "UPDATE project_agent_binding SET wake_budget=3 WHERE project_id=? AND state='active'",
    )
    .bind(&f.project_id)
    .execute(f.db.pool())
    .await
    .unwrap();
    let service = AttentionService::new(f.db.clone());
    for (category, key) in [
        ("decision_recorded", "decision"),
        ("delivery_followup", "delivery-1"),
        ("delivery_followup", "delivery-2"),
    ] {
        let a = incident(&f, category, key).await;
        assert!(
            matches!(
                service
                    .admit_wake(wake_request(&f, &a, &now_rfc3339()))
                    .await
                    .unwrap(),
                services::WakeAdmissionResult::Admitted { .. }
            ),
            "{category} admitted from the shared pool"
        );
        audit35_finish_turns(&f).await;
    }
    let overflow = incident(&f, "decision_recorded", "overflow").await;
    assert!(matches!(
        service
            .admit_wake(wake_request(&f, &overflow, &now_rfc3339()))
            .await
            .unwrap(),
        services::WakeAdmissionResult::Suppressed {
            reason: services::WakeSuppressionReason::BudgetExhausted
        }
    ));
    assert_eq!(stall_notices(&f).await, 0);
    assert_eq!(turns(&f).await, 3);
}

async fn answer_by_resolve(f: &ChatTurnFixture, escalation: &api_types::ProjectEscalationResponse) {
    let item =
        f.db.get_attention(&escalation.attention_id)
            .await
            .unwrap()
            .unwrap();
    AttentionService::new(f.db.clone())
        .resolve(&f.account_id, &item.id, item.version)
        .await
        .unwrap();
}

/// M4: a generic Resolve on an escalation item is the owner's answer: it
/// closes the escalation, wakes the Agent uncharged, and re-arms the blocker.
#[tokio::test]
async fn resolving_an_escalation_answers_it_and_never_strands_the_blocker() {
    let f = chat_turn_fixture().await;
    incident(&f, "execution_failed", "needs-owner").await;
    let attention = AttentionService::new(f.db.clone());
    assert_eq!(attention.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    let worker = AgentChatTurnWorker::with_runner(f.db.clone(), Arc::new(ProseOnlyWakeRunner));
    worker.run_once().await.unwrap();
    let escalations = services::project_escalation::ProjectEscalationService::new(f.db.clone());
    let open = escalations
        .list_for_owner(&f.project_id, &f.account_id, Some("open"), None, None)
        .await
        .unwrap();
    assert_eq!(open.items.len(), 1);
    let escalation = open.items[0].clone();
    assert!(escalation.need.starts_with("- Blocker needs-owner"));
    // A stranger cannot see it; the escalation is invisible to them.
    assert!(matches!(
        escalations
            .list_for_owner(&f.project_id, "stranger", None, None, None)
            .await,
        Err(ServiceError::NotFound { .. })
    ));
    answer_by_resolve(&f, &escalation).await;
    let answered = escalations
        .get_for_owner(&f.project_id, &escalation.id, &f.account_id)
        .await
        .unwrap();
    assert_eq!(
        (answered.status.as_str(), answered.answer.as_deref()),
        ("answered", Some("Resolved by the owner."))
    );
    assert!(escalations
        .list_for_owner(&f.project_id, &f.account_id, Some("open"), None, None)
        .await
        .unwrap()
        .items
        .is_empty());
    // The answer wakes the Agent without spending the decision bucket.
    attention.project_once(100).await.unwrap();
    assert_eq!(turns(&f).await, 2);
    assert_eq!(charged(&f, "decision").await, 0);
    // The blocker is linked to the answer turn; if it is still unchanged
    // after that turn, it escalates again rather than going silent.
    worker.run_once().await.unwrap();
    assert_eq!(audit35_escalations(&f).await, 2);
}

/// n1: a Project member who is not the owner gets 403, a stranger 404.
#[tokio::test]
async fn escalation_reads_are_owner_only() {
    let f = chat_turn_fixture().await;
    let member = new_uuid_v4();
    let now = now_rfc3339();
    UserRepo::create_user(
        &*f.db,
        &User {
            id: member.clone(),
            email: format!("{member}@example.test"),
            password_hash: "test".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO project_member (id, project_id, user_id, role, created_at, updated_at) VALUES (?, ?, ?, 'member', ?, ?)")
        .bind(new_uuid_v4()).bind(&f.project_id).bind(&member).bind(&now).bind(&now)
        .execute(f.db.pool()).await.unwrap();
    let service = services::project_escalation::ProjectEscalationService::new(f.db.clone());
    let e = service
        .escalate(
            &f.project_id,
            Agent(&f.identity_id),
            api_types::ProjectEscalateRequest {
                need: "Need a staging token".to_owned(),
                task_ids: vec![],
            },
            "token",
        )
        .await
        .unwrap();
    assert!(matches!(
        service.get_for_owner(&f.project_id, &e.id, &member).await,
        Err(ServiceError::AuthorizationDenied { .. })
    ));
    assert!(matches!(
        service
            .get_for_owner(&f.project_id, &e.id, "stranger")
            .await,
        Err(ServiceError::NotFound { .. })
    ));
    let page = service
        .list_for_owner(&f.project_id, &f.account_id, None, None, Some(1))
        .await
        .unwrap();
    assert_eq!((page.items.len(), page.has_more), (1, false));
}

/// M5: imported pre-upgrade admissions get one batched re-admission per
/// Project (80 blockers, 2 Projects: 2 wakes, 0 escalations); escalation
/// applies only after a post-upgrade turn for the same digest.
#[tokio::test]
async fn imported_blockers_get_one_batched_readmission_per_project() {
    let f = chat_turn_fixture().await;
    let second_identity = new_uuid_v4();
    let second_profile = identity_with_profile(&f.db, &second_identity).await;
    let (second_project, second_chat) =
        bound_project(&f.db, &second_identity, &second_profile).await;
    let legacy_turn = |chat_id: String, identity: String| {
        let db = f.db.clone();
        async move {
            let message_id = new_uuid_v4();
            let turn_id = new_uuid_v4();
            let now = now_rfc3339();
            sqlx::query("INSERT INTO agent_chat_message (id, chat_id, sequence, author_type, content, content_guard_json, sensitivity, status, source_type, source_metadata_json, correlation_id, created_at) VALUES (?, ?, (SELECT COALESCE(MAX(sequence),0)+1 FROM agent_chat_message WHERE chat_id = ?), 'system', 'legacy wake', '{}', 'internal', 'complete', 'native', '{}', ?, ?)")
                .bind(&message_id).bind(&chat_id).bind(&chat_id).bind(&message_id).bind(&now)
                .execute(db.pool()).await.unwrap();
            sqlx::query("INSERT INTO agent_chat_turn_job (id, chat_id, triggering_message_id, responder_identity_id, profile_id, canonical_scope_type, canonical_scope_id, status, dedupe_key, attempt_count, max_attempts, correlation_id, causation_depth, version, created_at, updated_at) VALUES (?, ?, ?, ?, (SELECT selected_profile_id FROM agent_identity WHERE id = ?), 'agent_chat', ?, 'succeeded', ?, 1, 3, ?, 0, 1, ?, ?)")
                .bind(&turn_id).bind(&chat_id).bind(&message_id).bind(&identity).bind(&identity)
                .bind(&chat_id).bind(format!("wake-turn:{turn_id}")).bind(&turn_id).bind(&now).bind(&now)
                .execute(db.pool()).await.unwrap();
            turn_id
        }
    };
    for (project_id, chat_id, identity) in [
        (
            f.project_id.clone(),
            f.chat_id.clone(),
            f.identity_id.clone(),
        ),
        (
            second_project.clone(),
            second_chat.clone(),
            second_identity.clone(),
        ),
    ] {
        let turn_id = legacy_turn(chat_id, identity).await;
        for n in 0..40 {
            let a = seed_incident(
                &f.db,
                &project_id,
                "execution_failed",
                &format!("attention:execution_failed:project:{project_id}:legacy:{n}"),
                &format!("Legacy blocker {n}"),
                serde_json::json!({"scope_type": "project", "scope_id": project_id, "n": n}),
                "inspect_run",
            )
            .await;
            // The shape V202610042145 imports: the pre-upgrade digest and source.
            sqlx::query("INSERT INTO agent_wake_blocker(attention_id,incident_digest,turn_job_id,admitted_at,legacy_source_event_id) VALUES(?,?,?,?,?)")
                .bind(&a.id).bind(format!("legacy-digest-{n}")).bind(&turn_id).bind(now_rfc3339()).bind(&a.source_event_id)
                .execute(f.db.pool()).await.unwrap();
        }
    }
    let service = AttentionService::new(f.db.clone());
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 2);
    let wakes: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type = 'agent.wake.admitted'",
    )
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    let escalations: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_wake_escalation")
        .fetch_one(f.db.pool())
        .await
        .unwrap();
    assert_eq!((wakes, escalations), (2, 0));
    // After the post-upgrade turn leaves them unchanged, each Project's
    // owner gets one escalation covering its batch.
    sqlx::query("UPDATE agent_chat_turn_job SET status='succeeded' WHERE dedupe_key LIKE 'agent-wake-admitted:%'")
        .execute(f.db.pool())
        .await
        .unwrap();
    service
        .sweep_once_at(&(chrono::Utc::now() + chrono::Duration::minutes(10)).to_rfc3339())
        .await
        .unwrap();
    let per_project: Vec<(String, i64)> = sqlx::query_as(
        "SELECT project_id, COUNT(*) FROM agent_wake_escalation GROUP BY project_id ORDER BY project_id",
    )
    .fetch_all(f.db.pool())
    .await
    .unwrap();
    assert_eq!(per_project.len(), 2);
    assert!(per_project.iter().all(|(_, count)| *count == 1));
}

/// m1: a persisted Task transition by the turn's Agent is a recorded outcome;
/// the blocker escalates only if it recurs after that turn.
#[tokio::test]
async fn recorded_task_transition_prevents_escalation_until_the_blocker_recurs() {
    let f = chat_turn_fixture().await;
    let task_id = new_uuid_v4();
    let now = now_rfc3339();
    sqlx::query("INSERT INTO task (id, project_id, title, status, created_at, updated_at) VALUES (?, ?, 'retry me', 'failed', ?, ?)")
        .bind(&task_id).bind(&f.project_id).bind(&now).bind(&now)
        .execute(f.db.pool()).await.unwrap();
    let a = seed_incident(
        &f.db,
        &f.project_id,
        "review_risk",
        &format!(
            "attention:review_risk:project:{}:task:{task_id}",
            f.project_id
        ),
        "Review risk",
        serde_json::json!({"entity_type": "task", "entity_id": task_id}),
        "inspect_run",
    )
    .await;
    let service = AttentionService::new(f.db.clone());
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    let transition_id = new_uuid_v4();
    f.db.append_event(CreateDomainEvent::task_transition(
        transition_id.clone(),
        task_id.clone(),
        &f.project_id,
        "failed",
        "todo",
        None,
        format!("agent:{}", f.identity_id),
        "retry after fixing the cause",
        false,
        now_rfc3339(),
        serde_json::Value::Null,
    ))
    .await
    .unwrap();
    audit35_finish_turns(&f).await;
    service
        .sweep_once_at(&(chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339())
        .await
        .unwrap();
    assert_eq!(
        audit35_escalations(&f).await,
        0,
        "the recovery was recorded"
    );
    // The same blocker recurs after the turn: the recovery did not take.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let recurred = seed_incident(
        &f.db,
        &f.project_id,
        "review_risk",
        &a.dedupe_key,
        "Review risk",
        serde_json::json!({"entity_type": "task", "entity_id": task_id}),
        "inspect_run",
    )
    .await;
    assert_eq!(recurred.id, a.id);
    service
        .sweep_once_at(&(chrono::Utc::now() + chrono::Duration::minutes(2)).to_rfc3339())
        .await
        .unwrap();
    assert_eq!(audit35_escalations(&f).await, 1);
}

/// m7: the sweep reads open Attention plus one keyed decision row; it never
/// scans the disposition history, however large.
#[tokio::test]
async fn sweep_candidate_query_does_not_scan_dispositions() {
    let f = chat_turn_fixture().await;
    let plan: Vec<(i64, i64, i64, String)> = sqlx::query_as(
        "EXPLAIN QUERY PLAN
         SELECT a.id, a.scope_type, a.scope_id, l.incident_digest, l.disposition, l.reason, l.identity_id
         FROM attention_projection a
         LEFT JOIN agent_wake_attention_latest l ON l.attention_id = a.id
         WHERE a.status = 'open' AND a.recommended_action <> 'answer_escalation'
           AND (a.snoozed_until IS NULL OR a.snoozed_until <= ?)
         ORDER BY a.scope_type, a.scope_id, a.occurred_at, a.id",
    )
    .bind(now_rfc3339())
    .fetch_all(f.db.pool())
    .await
    .unwrap();
    let detail = plan
        .iter()
        .map(|row| row.3.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!detail.contains("agent_wake_disposition"), "{detail}");
    assert!(!detail.contains("SCAN l"), "{detail}");
    assert!(
        detail.contains("idx_attention_projection_status_priority"),
        "{detail}"
    );
    let blocker: Vec<(i64, i64, i64, String)> = sqlx::query_as(
        "EXPLAIN QUERY PLAN SELECT j.status FROM agent_wake_blocker b
         JOIN agent_chat_turn_job j ON j.id = b.turn_job_id WHERE b.attention_id = ?",
    )
    .bind("x")
    .fetch_all(f.db.pool())
    .await
    .unwrap();
    let detail = blocker
        .iter()
        .map(|row| row.3.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!detail.contains("SCAN"), "{detail}");
}

/// B1: a resolve followed by a reopen re-arms the blocker, even with the same
/// digest and even after its turn was escalated.
#[tokio::test]
async fn resolved_then_reopened_blocker_is_woken_again() {
    let f = chat_turn_fixture().await;
    let a = incident(&f, "execution_failed", "flaky").await;
    let service = AttentionService::new(f.db.clone());
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    AgentChatTurnWorker::with_runner(f.db.clone(), Arc::new(ProseOnlyWakeRunner))
        .run_once()
        .await
        .unwrap();
    assert_eq!(
        audit35_escalations(&f).await,
        1,
        "the silent turn escalated"
    );
    let current = f.db.get_attention(&a.id).await.unwrap().unwrap();
    service
        .resolve(&f.account_id, &a.id, current.version)
        .await
        .unwrap();
    let reopened = incident(&f, "execution_failed", "flaky").await;
    assert_eq!(
        (reopened.id.as_str(), reopened.status.as_str()),
        (a.id.as_str(), "open")
    );
    assert_eq!(
        wake_attention_incident_digest(&reopened),
        wake_attention_incident_digest(&a)
    );
    let later = (chrono::Utc::now() + chrono::Duration::minutes(6)).to_rfc3339();
    assert_eq!(service.sweep_once_at(&later).await.unwrap(), 1);
    assert_eq!((turns(&f).await, audit35_escalations(&f).await), (2, 1));
}
