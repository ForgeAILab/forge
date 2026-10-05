use std::sync::Arc;

use async_trait::async_trait;
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentChatRepo,
    AgentChatTurnJobRepo, AgentChatTurnState, AgentProfileRepo, AgentRepo, AgentStatus,
    AttentionRepo, CreateAgentIdentity, CreateAgentProfile, CreateDomainEvent, CreateProject,
    DomainEventRepo, ProjectRepo, SelectAgentProfile, SqliteDb, UpdateAgentChatTurnJob, User,
    UserRepo,
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
            "foreign-agent",
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
        .escalate(&f.project_id, &f.identity_id, request.clone(), "need")
        .await
        .unwrap();
    let replay = service
        .escalate(&f.project_id, &f.identity_id, request, "need")
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
    assert_eq!(charged(&f, "decision").await, 1);
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
