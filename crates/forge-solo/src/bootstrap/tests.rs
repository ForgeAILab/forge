//! Solo bootstrap reconciliation, registration, and candidate eligibility tests.

use super::*;
use db::{
    create_sqlite_pool, run_migrations, AgentRepo, CreateAgent, CreateAgentIdentity,
    CreateAgentProfile, DaemonStatus, UpdateDaemonReport, UpsertDaemon,
};
use tempfile::tempdir;

async fn fixture() -> (Arc<SqliteDb>, SoloBootstrapService) {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    let db = Arc::new(SqliteDb::new(pool));
    let service = SoloBootstrapService::new(Arc::clone(&db));
    (db, service)
}

fn request(path: &Path, repo_id: &str) -> SoloBootstrapRequest {
    let mut request = SoloBootstrapRequest::new(
        repo_id,
        path.to_string_lossy().to_string(),
        path.join(".forge-solo-data").to_string_lossy().to_string(),
        "main",
    );
    request.repository_name = Some("demo".to_owned());
    request
}

#[tokio::test]
async fn config_embedded_candidates_are_eligible_for_solo_selection() {
    let (db, service) = fixture().await;
    let source = tempdir().expect("temp repository");
    let repo_id = db::new_uuid_v4();
    let initial = service
        .bootstrap(request(source.path(), &repo_id))
        .await
        .expect("owner bootstrap");
    let owner_id = initial.owner_id.clone();
    let now = db::now_rfc3339();

    // The config sync creates protected entries through the embedded
    // service; the eligibility gate only reads the handle row, so the
    // test seeds that row directly.
    let handle_id = db::new_uuid_v4();
    sqlx::query(
            "INSERT INTO credential_handle (
                id, owner_user_id, provider, label, status, enabled,
                credential_method, metadata_json, version, created_at, updated_at
             ) VALUES (?, ?, 'openai_compatible', 'config:zai', 'configured', 1, 'api_key', '{}', 1, ?, ?)",
        )
        .bind(&handle_id)
        .bind(&owner_id)
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("credential handle");

    let identity_id = db::new_uuid_v4();
    let profile_id = db::new_uuid_v4();
    let agent = AgentRepo::create_identity_with_profile(
        &*db,
        CreateAgentIdentity {
            id: identity_id.clone(),
            name: "zai/glm-5.3".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: Some(now.clone()),
            is_default: false,
            paused: false,
            owner_id: Some(owner_id.clone()),
            visibility: "account".to_owned(),
            account_permission_ceiling: "null".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: profile_id.clone(),
            identity_id: identity_id.clone(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("openai_compatible".to_owned()),
            model: Some("glm-5.3".to_owned()),
            reasoning_effort: None,
            permission_policy: Some("scoped_proposals".to_owned()),
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            tool_policy_json: services_agents::embedded_agent_service::default_profile_tool_policy(
            )
            .to_string(),
            config_json: "{}".to_owned(),
            credential_ref: Some(handle_id.clone()),
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("native agent");

    db::AgentConnectionHealthRepo::upsert_connection_health(
        &*db,
        db::UpsertAgentConnectionHealth {
            profile_id: profile_id.clone(),
            status: "healthy".to_owned(),
            capability_status_json: "{}".to_owned(),
            checked_at: Some(now.clone()),
            error_code: None,
            updated_at: now.clone(),
        },
    )
    .await
    .expect("connection health");

    let mut confirmed = request(source.path(), &repo_id);
    confirmed.agent_candidates = vec![SoloAgentCandidateInput {
        identity_id: agent.id.clone(),
        profile_id: Some(agent.profile_id.clone()),
        executor_type: "embedded".to_owned(),
        availability: SoloAgentAvailability::Authenticated,
        display_name: Some("zai/glm-5.3".to_owned()),
    }];
    confirmed.selected_project_agent_id = Some(agent.id.clone());
    confirmed.selected_worker_agent_id = Some(agent.id.clone());
    let selected = service
        .bootstrap(confirmed)
        .await
        .expect("embedded agent selection");
    assert_eq!(
        selected.readiness,
        SoloBootstrapReadiness::CharterAdoptionRequired
    );
    assert!(
        selected.agent_candidates[0].eligible,
        "config-backed embedded candidate should be eligible"
    );
    assert_eq!(
        selected.agent_candidates[0].reason.as_deref(),
        None,
        "eligible candidate carries no blocking reason"
    );
    assert_eq!(
        selected.selected_project_agent_id.as_deref(),
        Some(agent.id.as_str())
    );
    assert_eq!(
        selected.worker_identity_id.as_deref(),
        Some(agent.id.as_str())
    );
}

#[tokio::test]
async fn embedded_candidates_with_misaligned_backends_are_rejected() {
    let (db, service) = fixture().await;
    let source = tempdir().expect("temp repository");
    let repo_id = db::new_uuid_v4();
    let initial = service
        .bootstrap(request(source.path(), &repo_id))
        .await
        .expect("owner bootstrap");
    let owner_id = initial.owner_id.clone();
    let now = db::now_rfc3339();

    let handle_id = db::new_uuid_v4();
    sqlx::query(
        "INSERT INTO credential_handle (
                id, owner_user_id, provider, label, status, enabled,
                credential_method, metadata_json, version, created_at, updated_at
             ) VALUES (?, ?, 'gemini', 'config:google', 'configured', 1, 'api_key', '{}', 1, ?, ?)",
    )
    .bind(&handle_id)
    .bind(&owner_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("credential handle");

    // backend_kind "cli" with executor "embedded" is a misalignment that
    // must never pass the native gate.
    let identity_id = db::new_uuid_v4();
    let profile_id = db::new_uuid_v4();
    let agent = AgentRepo::create_identity_with_profile(
        &*db,
        CreateAgentIdentity {
            id: identity_id.clone(),
            name: "misaligned".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: Some(now.clone()),
            is_default: false,
            paused: false,
            owner_id: Some(owner_id.clone()),
            visibility: "account".to_owned(),
            account_permission_ceiling: "null".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: profile_id.clone(),
            identity_id: identity_id.clone(),
            backend_kind: "cli".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("gemini".to_owned()),
            model: Some("gemini-3.8-flash".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            tool_policy_json: "null".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: Some(handle_id),
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("misaligned agent");

    let mut listed = request(source.path(), &repo_id);
    listed.agent_candidates = vec![SoloAgentCandidateInput {
        identity_id: agent.id.clone(),
        profile_id: Some(agent.profile_id.clone()),
        executor_type: "embedded".to_owned(),
        availability: SoloAgentAvailability::Authenticated,
        display_name: None,
    }];
    let result = service.bootstrap(listed).await.expect("candidate listed");
    let candidate = &result.agent_candidates[0];
    assert!(
        !candidate.eligible,
        "misaligned cli-backend/embedded-executor candidate must not be eligible"
    );
    assert!(
        candidate.reason.is_some(),
        "ineligible candidate carries a structured reason"
    );
}

#[tokio::test]
async fn bootstrap_is_idempotent_and_keeps_charter_gate() {
    let (_db, service) = fixture().await;
    let source = tempdir().expect("temp repository");
    let repo_id = db::new_uuid_v4();
    let first = service
        .bootstrap(request(source.path(), &repo_id))
        .await
        .expect("first bootstrap");
    let second = service
        .bootstrap(request(source.path(), &repo_id))
        .await
        .expect("replay bootstrap");

    assert_eq!(first.owner_id, second.owner_id);
    assert_eq!(first.project_id, second.project_id);
    assert_eq!(first.repo_id, second.repo_id);
    assert_eq!(first.project_chat_id, second.project_chat_id);
    assert_eq!(
        first.project_agent_binding_id,
        second.project_agent_binding_id
    );
    assert_eq!(
        first.readiness,
        SoloBootstrapReadiness::AgentSelectionRequired
    );
    assert!(first.adoption_required);
    assert!(!first.mutation_authority_granted);

    let projects = list_projects(&_db).await.expect("projects");
    assert_eq!(projects.len(), 1);
    let repos = list_project_repos(&_db, &first.project_id)
        .await
        .expect("repos");
    assert_eq!(repos.len(), 1);
    assert_eq!(repos[0].id, first.repo_id);
    assert_eq!(
        repos[0].local_path.as_deref(),
        Some(source.path().to_string_lossy().as_ref())
    );
    let members = ProjectMemberRepo::list_members(_db.as_ref(), &first.project_id)
        .await
        .expect("members");
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].user_id, first.owner_id);
    assert_eq!(members[0].role, "owner");
    let chat = AgentChatRepo::get_project_chat(_db.as_ref(), &first.project_id)
        .await
        .expect("chat")
        .expect("canonical chat");
    assert_eq!(chat.id, first.project_chat_id);
    let binding =
        ProjectAgentBindingRepo::get_active_project_binding(_db.as_ref(), &first.project_id)
            .await
            .expect("binding")
            .expect("setup binding");
    assert_eq!(binding.state, PROJECT_AGENT_BINDING_SETUP_STATE);
    assert!(binding.identity_id.is_none());
    assert!(binding.profile_id.is_none());
}

#[tokio::test]
async fn a_mismatched_repository_fails_closed_without_a_second_project() {
    let (db, service) = fixture().await;
    let source = tempdir().expect("temp repository");
    let repo_id = db::new_uuid_v4();
    service
        .bootstrap(request(source.path(), &repo_id))
        .await
        .expect("first bootstrap");

    let mut mismatch = request(source.path(), &repo_id);
    mismatch.canonical_repository = source.path().join("moved").to_string_lossy().to_string();
    let error = service.bootstrap(mismatch).await.expect_err("mismatch");
    assert!(
        matches!(error, ServiceError::Conflict(message) if message.contains("different repository") || message.contains("data root"))
    );
    assert_eq!(list_projects(&db).await.expect("projects").len(), 1);
}

#[tokio::test]
async fn a_moved_checkout_reuses_the_canonical_repo_binding() {
    let (db, service) = fixture().await;
    let source = tempdir().expect("original repository");
    let moved = tempdir().expect("moved repository");
    let repo_id = db::new_uuid_v4();
    let first = service
        .bootstrap(request(source.path(), &repo_id))
        .await
        .expect("first bootstrap");

    let mut restart = request(source.path(), &repo_id);
    restart.source_path = Some(moved.path().to_string_lossy().to_string());
    let resumed = service
        .bootstrap(restart)
        .await
        .expect("moved checkout resumes");
    assert_eq!(first.project_id, resumed.project_id);
    assert_eq!(first.repo_id, resumed.repo_id);
    let repos = list_project_repos(&db, &first.project_id)
        .await
        .expect("repos");
    assert_eq!(repos.len(), 1);
    assert_eq!(
        repos[0].local_path.as_deref(),
        Some(moved.path().to_string_lossy().as_ref())
    );
}

#[tokio::test]
async fn only_authenticated_supported_cli_candidates_are_eligible() {
    let (db, service) = fixture().await;
    let source = tempdir().expect("temp repository");
    let repo_id = db::new_uuid_v4();
    let initial = service
        .bootstrap(request(source.path(), &repo_id))
        .await
        .expect("owner bootstrap");
    let now = db::now_rfc3339();
    let daemon = db::DaemonRepo::upsert_by_machine_id(
        &*db,
        UpsertDaemon {
            max_concurrent_runs: None,
            id: db::new_uuid_v4(),
            machine_id: "solo-test-machine".to_owned(),
            hostname: "localhost".to_owned(),
            os: "test".to_owned(),
            arch: "test".to_owned(),
            agent_version: None,
            labels_json: "{}".to_owned(),
            status: DaemonStatus::Online,
            registration_token_hash: None,
            owner_id: Some(initial.owner_id.clone()),
            visibility: "account".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("daemon");
    db::DaemonRepo::update_report(
        &*db,
        UpdateDaemonReport {
            max_concurrent_runs: None,
            id: daemon.id.clone(),
            last_report_at: now.clone(),
            status: DaemonStatus::Online,
            detected_clis_json: r#"[{"kind":"codex","availability":"authenticated"}]"#.to_owned(),
            labels_json: None,
            updated_at: now.clone(),
        },
    )
    .await
    .expect("daemon report");
    let agent = AgentRepo::create(
        &*db,
        CreateAgent {
            id: db::new_uuid_v4(),
            name: "Codex local".to_owned(),
            description: None,
            executor_type: "codex".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: Some(daemon.id),
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: Some(now.clone()),
            is_default: false,
            paused: false,
            owner_id: Some(initial.owner_id.clone()),
            visibility: "account".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("agent");
    let mut confirmed = request(source.path(), &repo_id);
    confirmed.agent_candidates = vec![SoloAgentCandidateInput {
        identity_id: agent.id.clone(),
        profile_id: Some(agent.profile_id.clone()),
        executor_type: "codex".to_owned(),
        availability: SoloAgentAvailability::Authenticated,
        display_name: None,
    }];
    confirmed.selected_project_agent_id = Some(agent.id.clone());
    confirmed.selected_worker_agent_id = Some(agent.id.clone());
    let selected = service
        .bootstrap(confirmed)
        .await
        .expect("selected bootstrap");
    assert_eq!(
        selected.readiness,
        SoloBootstrapReadiness::CharterAdoptionRequired
    );
    assert_eq!(
        selected.selected_project_agent_id.as_deref(),
        Some(agent.id.as_str())
    );
    assert_eq!(
        selected.project_agent_identity_id.as_deref(),
        Some(agent.id.as_str())
    );
    assert_eq!(
        selected.project_agent_profile_id.as_deref(),
        Some(agent.profile_id.as_str())
    );
    assert_eq!(
        selected.worker_identity_id.as_deref(),
        Some(agent.id.as_str())
    );
    assert!(!selected.mutation_authority_granted);

    let chat = AgentChatRepo::get_project_chat(&*db, &selected.project_id)
        .await
        .expect("project chat")
        .expect("canonical project chat");
    assert_eq!(chat.status, PROJECT_CHAT_READY_STATUS);
    let binding = ProjectAgentBindingRepo::get_active_project_binding(&*db, &selected.project_id)
        .await
        .expect("project binding")
        .expect("active adoption binding");
    assert_eq!(binding.state, "active");
    assert!(binding.charter_setup_required);
    assert_eq!(
        binding.permission_ceiling_json,
        SOLO_PROJECT_PERMISSION_CEILING_JSON
    );
    assert!(binding.admission_receipt_id.is_none());
    assert!(binding.charter_approval_id.is_none());
    assert!(binding.charter_id.is_none());
    assert!(binding.charter_revision_id.is_none());

    // Selection activates only the existing adoption conversation. The
    // typed Agent Chat admission path can now accept a user message, but
    // the Project remains Charter-gated and therefore has no Task
    // mutation authority.
    let admitted = services_agents::AgentChatService::new(Arc::clone(&db))
        .send_message(services_agents::SendAgentChatMessageInput {
            actor_user_id: selected.owner_id.clone(),
            chat_id: selected.project_chat_id.clone(),
            content: "Please explain the Project Charter adoption choices.".to_owned(),
            dedupe_key: Some("solo-adoption-chat-test".to_owned()),
        })
        .await
        .expect("adoption chat message");
    assert_eq!(admitted.message.chat_id, selected.project_chat_id);
    assert!(!selected.mutation_authority_granted);
    assert_eq!(
        selected.readiness,
        SoloBootstrapReadiness::CharterAdoptionRequired
    );

    db::DaemonRepo::update_report(
        &*db,
        UpdateDaemonReport {
            max_concurrent_runs: None,
            id: agent.daemon_id.expect("daemon id"),
            last_report_at: db::now_rfc3339(),
            status: DaemonStatus::Offline,
            detected_clis_json: "[]".to_owned(),
            labels_json: None,
            updated_at: db::now_rfc3339(),
        },
    )
    .await
    .expect("daemon offline");
    let error = service
        .bootstrap({
            let mut retry = request(source.path(), &repo_id);
            retry.agent_candidates = vec![SoloAgentCandidateInput::authenticated(
                agent.id.clone(),
                "codex",
            )];
            retry.selected_project_agent_id = Some(agent.id.clone());
            retry.selected_worker_agent_id = Some(agent.id);
            retry
        })
        .await
        .expect_err("unavailable selection must fail closed");
    assert!(matches!(error, ServiceError::Conflict(message) if message.contains("unavailable")));
}

#[tokio::test]
async fn authenticated_registration_creates_and_replays_owned_agent_profile() {
    let (db, service) = fixture().await;
    let source = tempdir().expect("temp repository");
    let repo_id = db::new_uuid_v4();
    let initial = service
        .bootstrap(request(source.path(), &repo_id))
        .await
        .expect("owner bootstrap");
    let now = db::now_rfc3339();
    let daemon = db::DaemonRepo::upsert_by_machine_id(
        &*db,
        UpsertDaemon {
            max_concurrent_runs: None,
            id: db::new_uuid_v4(),
            machine_id: "solo-registration-machine".to_owned(),
            hostname: "localhost".to_owned(),
            os: "test".to_owned(),
            arch: "test".to_owned(),
            agent_version: None,
            labels_json: "{}".to_owned(),
            status: DaemonStatus::Online,
            registration_token_hash: None,
            owner_id: Some(initial.owner_id.clone()),
            visibility: "account".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("daemon");
    db::DaemonRepo::update_report(
        &*db,
        UpdateDaemonReport {
            max_concurrent_runs: None,
            id: daemon.id.clone(),
            last_report_at: now.clone(),
            status: DaemonStatus::Online,
            detected_clis_json: r#"[{"kind":"codex","availability":"authenticated"}]"#.to_owned(),
            labels_json: None,
            updated_at: now,
        },
    )
    .await
    .expect("daemon report");

    let registration = SoloAuthenticatedAgentRegistration::authenticated(
        "daemon:solo-registration-machine:codex",
        "Codex local",
        "codex",
        Some(daemon.id),
    );
    let first = service
        .ensure_authenticated_agent(&initial.owner_id, registration.clone())
        .await
        .expect("agent registration");
    let second = service
        .ensure_authenticated_agent(&initial.owner_id, registration)
        .await
        .expect("agent registration replay");
    assert!(first.eligible);
    assert_eq!(first.identity_id, second.identity_id);
    assert_eq!(first.profile_id, second.profile_id);
    assert_eq!(list_agents(&db).await.expect("agents").len(), 1);
}

#[test]
fn supported_executor_matrix_excludes_embedded_shell_and_null() {
    assert!(allowed_cli_executor("codex"));
    assert!(allowed_cli_executor("claude_code"));
    assert!(allowed_cli_executor("cursor"));
    assert!(allowed_cli_executor("opencode"));
    assert!(allowed_cli_executor("gemini"));
    assert!(allowed_cli_executor("smith"));
    assert!(!allowed_cli_executor("embedded"));
    assert!(!allowed_cli_executor("shell"));
    assert!(!allowed_cli_executor("null"));
}
