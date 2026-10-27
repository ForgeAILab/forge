//! Shared fixtures and helpers for wake turn consumer tests.

use super::*;

pub(super) async fn database() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    Arc::new(SqliteDb::new(pool))
}

pub(super) async fn owned_identity_with_profile(
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

pub(super) async fn select_profile(
    db: &SqliteDb,
    identity_id: &str,
    profile_id: &str,
    model: &str,
) -> String {
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

pub(super) struct ChatTurnFixture {
    pub(super) db: Arc<SqliteDb>,
    pub(super) account_id: String,
    pub(super) project_id: String,
    pub(super) chat_id: String,
    pub(super) identity_id: String,
}

pub(super) struct FailingWakeRunner;
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
pub(super) struct ProseOnlyWakeRunner;

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

pub(super) async fn chat_turn_fixture() -> ChatTurnFixture {
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

pub(super) async fn append_event(db: &SqliteDb, event: CreateDomainEvent) {
    DomainEventRepo::append_event(db, event).await.unwrap();
}

pub(super) async fn incident(
    f: &ChatTurnFixture,
    category: &str,
    key: &str,
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
pub(super) fn wake_request(
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
pub(super) async fn turns(f: &ChatTurnFixture) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM agent_chat_turn_job WHERE chat_id=?")
        .bind(&f.chat_id)
        .fetch_one(f.db.pool())
        .await
        .unwrap()
}
pub(super) async fn charged(f: &ChatTurnFixture, category: &str) -> i64 {
    sqlx::query_scalar("SELECT COALESCE(SUM(admitted_count),0) FROM agent_wake_budget_window WHERE scope_id=? AND category=?").bind(&f.project_id).bind(category).fetch_one(f.db.pool()).await.unwrap()
}
// ---- audit 3.5 probes -------------------------------------------------------
pub(super) async fn audit35_reproject(
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
pub(super) async fn audit35_escalations(f: &ChatTurnFixture) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM agent_wake_escalation WHERE project_id=?")
        .bind(&f.project_id)
        .fetch_one(f.db.pool())
        .await
        .unwrap()
}
pub(super) async fn audit35_finish_turns(f: &ChatTurnFixture) {
    sqlx::query("UPDATE agent_chat_turn_job SET status='succeeded' WHERE chat_id=?")
        .bind(&f.chat_id)
        .execute(f.db.pool())
        .await
        .unwrap();
}

// ---- restored wake coverage (fix round M6), on the single admission stage ----
// These were consumer tests driven by synthetic `agent.wake.admitted` events.
// Attention now admits the turn itself, so each drives `admit_wake` or the
// sweep and keeps the original behavioural assertions.

pub(super) async fn file_database() -> (Arc<SqliteDb>, String) {
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

pub(super) async fn identity_with_profile(db: &SqliteDb, id: &str) -> String {
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

pub(super) async fn bound_project(
    db: &SqliteDb,
    identity_id: &str,
    profile_id: &str,
) -> (String, String) {
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
    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "wake-turn-project".to_owned(),
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
pub(super) async fn seed_incident(
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

pub(super) fn project_request(
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

pub(super) async fn admitted_count(db: &SqliteDb) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_wake_disposition WHERE disposition = 'turn_admitted'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap()
}

pub(super) async fn chat_turn_count(db: &SqliteDb, chat_id: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM agent_chat_turn_job WHERE chat_id = ?")
        .bind(chat_id)
        .fetch_one(db.pool())
        .await
        .unwrap()
}

/// The admitted wake turn for one incident occurrence: (id, content, metadata).
pub(super) async fn wake_turn_for(
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
pub(super) struct BackendFailingWakeRunner;
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

/// One milestone whose acceptance matrix an Agent is expected to settle.
pub(super) struct DeliveryMilestoneFixture {
    pub(super) milestone_id: String,
    pub(super) milestone_revision_id: String,
    pub(super) agent_check_id: String,
    pub(super) manual_check_id: String,
}

pub(super) async fn seed_delivery_milestone(
    db: &SqliteDb,
    project_id: &str,
) -> DeliveryMilestoneFixture {
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

pub(super) async fn seed_governed_task(
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

pub(super) async fn settle_check(
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

pub(super) async fn seed_delivery_incident(
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

// Regression scenarios copied from the independent 3.9(b) audit.

pub(super) async fn audit39b_detection(fixture: &ChatTurnFixture, path: &str, count: i64) {
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

pub(super) async fn audit39b_hotspot_turns(fixture: &ChatTurnFixture) -> i64 {
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

// ---- fix round 3.5: new coverage ------------------------------------------

pub(super) struct TransientWakeRunner;
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

pub(super) async fn stall_notices(f: &ChatTurnFixture) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type='notification.requested' AND scope_id=?",
    )
    .bind(&f.project_id)
    .fetch_one(f.db.pool())
    .await
    .unwrap()
}

pub(super) async fn answer_by_resolve(
    f: &ChatTurnFixture,
    escalation: &api_types::ProjectEscalationResponse,
) {
    let item =
        f.db.get_attention(&escalation.attention_id)
            .await
            .unwrap()
            .unwrap();
    AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    )
    .resolve(&f.account_id, &item.id, item.version)
    .await
    .unwrap();
}
