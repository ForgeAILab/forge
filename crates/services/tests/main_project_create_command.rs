//! Gate A acceptance coverage for the action-backed Main Project command.
//!
//! The proposal is deliberately scoped to the Main Chat, while the command
//! receipt is scoped to the owning account.  This test exercises that scope
//! normalization through the public service boundary and verifies that the
//! Project/Chat/binding/handoff/event/receipt/action execution are one durable
//! outcome.

use std::{
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use api_types::ProductMaturity;
use db::{
    create_sqlite_pool, now_rfc3339, run_migrations, AccountMainAgentBindingRepo,
    AgentActionPolicyResult, AgentActionRepo, AgentActionStatus, AgentChatMessageAuthorType,
    AgentChatMessageRepo, AgentChatMessageStatus, AgentChatTurnJobRepo, AgentRepo,
    CreateAccountMainAgentBinding, CreateAgentAction, CreateAgentChatMessage,
    CreateAgentChatTurnJob, CreateAgentIdentity, CreateAgentProfile, CreateProjectCharter,
    CreateProjectCharterRevision, ProjectOrchestrationRepo, SqliteDb, User, UserRepo,
};
use forge_agent_host::MAIN_PROJECT_CREATE_OPERATION;
use serde_json::{json, Value};
use services::{
    create_project_from_charter_approval, project_agent_policy_digest, CreateProjectAuthorization,
    CreateProjectFromCharterApprovalInput, ExecuteMainOrchestrationActionInput,
    GenesisPromptContext, MainOrchestrationActionService, ProductGenesisService,
};
use sqlx::Row;

const ACCOUNT_ID: &str = "main-command-account";
const MAIN_IDENTITY_ID: &str = "main-command-identity";
const MAIN_PROFILE_ID: &str = "main-command-profile";
const PROJECT_IDENTITY_ID: &str = "main-command-project-identity";
const PROJECT_PROFILE_ID: &str = "main-command-project-profile";
const GENESIS_ID: &str = "main-command-genesis";
const CHARTER_ID: &str = "main-command-charter";
const CHARTER_REVISION_ID: &str = "main-command-charter-revision";
const APPROVAL_ID: &str = "main-command-approval";
const ACTION_ID: &str = "main-command-action";
const ACTION_DEDUPE_KEY: &str = "main-command-action-dedupe";
const EXECUTION_IDEMPOTENCY_KEY: &str = "main-command-project-create";
const NOW: &str = "2026-08-20T00:00:00.000Z";

struct Fixture {
    db: Arc<SqliteDb>,
    main_chat_id: String,
    action_version: i64,
}

async fn database() -> Arc<SqliteDb> {
    database_with_url("sqlite::memory:").await
}

async fn database_with_url(url: &str) -> Arc<SqliteDb> {
    let pool = create_sqlite_pool(url).await.expect("SQLite pool creates");
    run_migrations(&pool).await.expect("migrations run");
    Arc::new(SqliteDb::new(pool))
}

async fn create_identity(
    db: &SqliteDb,
    identity_id: &str,
    profile_id: &str,
    name: &str,
    tool_policy_json: &str,
    backend_kind: &str,
) {
    AgentRepo::create_identity_with_profile(
        db,
        CreateAgentIdentity {
            id: identity_id.to_owned(),
            name: name.to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: db::AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some(ACCOUNT_ID.to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
        CreateAgentProfile {
            id: profile_id.to_owned(),
            identity_id: identity_id.to_owned(),
            backend_kind: backend_kind.to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("test-model".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: tool_policy_json.to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("identity/profile creates");
}

fn charter_content() -> api_types::ProjectCharterContent {
    serde_json::from_value(json!({
        "identity": {
            "working_name": "Main Command Project",
            "slug_proposal": "main-command-project",
            "one_line_vision": "A Project created by one exact Main command.",
            "maturity": "mvp",
            "lifecycle_intent": "validate the command boundary",
            "project_type": "product",
            "value_proposition": "Make the Main-to-Project handoff durable."
        },
        "problem_and_people": {
            "problem_or_opportunity": "A lost command response must not duplicate a Project.",
            "target_users": ["Forge users"],
            "beneficiaries": ["Project collaborators"],
            "jobs_pains_opportunity": ["Continue from an approved Charter."],
            "current_alternatives": ["Manual handoff"],
            "stakeholders": ["Project owner"],
            "excluded_audiences": ["Unrelated accounts"]
        },
        "core_experience": {
            "primary_outcome": "One approved command creates one exact Project handoff.",
            "core_loop": "approve, create, replay",
            "principal_journeys": ["User retries after response loss"]
        },
        "scope": {
            "must_have_outcomes": ["Persist Project and handoff."],
            "required_deliverables": ["One Project Chat and one queued turn."],
            "later_possibilities": ["Project-local planning"],
            "explicit_non_goals": ["Managing another account"]
        },
        "success": {
            "qualitative_outcome": "The handoff is exact.",
            "success_signals": ["Replay returns the same identifiers."],
            "acceptance_statements": ["A retry creates no duplicate Project."],
            "required_evidence": ["Durable receipt and event"],
            "non_claims": ["This does not prove implementation quality."]
        },
        "constraints_and_risks": {
            "product": ["Local-first single-user operation"],
            "time_and_budget": [],
            "technology": ["SQLite"],
            "data": ["Do not copy hidden Main Chat history"],
            "integrations": [],
            "security_privacy_compliance": ["Require explicit approval"],
            "accessibility": [],
            "operations": [],
            "migration": [],
            "launch": [],
            "agent_authority": ["Project Agent remains Project-scoped"],
            "risks": []
        },
        "knowledge_ledger": {"items": []},
        "handoff_note": {
            "recommended_first_action": "Validate the approved outcome.",
            "bounded_summary": "Continue from the exact Charter.",
            "unresolved_item_ids": []
        }
    }))
    .expect("valid Charter content")
}

async fn fixture() -> Fixture {
    let db = database().await;
    fixture_with_db(db).await
}

async fn fixture_with_url(url: &str) -> Fixture {
    let db = database_with_url(url).await;
    fixture_with_db(db).await
}

async fn fixture_with_db(db: Arc<SqliteDb>) -> Fixture {
    fixture_with_project_backend(db, "native").await
}

/// The Project Agent's backend kind is a Profile field, and Profiles are
/// immutable once created, so a test that needs a deterministic non-provider
/// backend must select it here.
async fn fixture_with_project_backend(db: Arc<SqliteDb>, project_backend_kind: &str) -> Fixture {
    let authorization_now = now_rfc3339();
    UserRepo::create_user(
        &*db,
        &User {
            id: ACCOUNT_ID.to_owned(),
            email: "main-command@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: Some("Main command owner".to_owned()),
            is_admin: false,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("account creates");

    create_identity(
        &db,
        MAIN_IDENTITY_ID,
        MAIN_PROFILE_ID,
        "Main Agent",
        "{}",
        "native",
    )
    .await;
    let project_tool_policy = r#"{"permissions":["read_project","handoff"]}"#;
    create_identity(
        &db,
        PROJECT_IDENTITY_ID,
        PROJECT_PROFILE_ID,
        "Project Agent",
        project_tool_policy,
        project_backend_kind,
    )
    .await;

    AccountMainAgentBindingRepo::create_main_binding(
        &*db,
        CreateAccountMainAgentBinding {
            id: "main-command-binding".to_owned(),
            account_id: ACCOUNT_ID.to_owned(),
            identity_id: MAIN_IDENTITY_ID.to_owned(),
            profile_id: MAIN_PROFILE_ID.to_owned(),
            autonomy_policy_json: "{}".to_owned(),
            tool_policy_revision: "main-policy@1".to_owned(),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("Main binding creates");
    let main_chat_id: String = sqlx::query_scalar(
        "SELECT id FROM agent_chat WHERE kind = 'account_main' AND account_id = ?",
    )
    .bind(ACCOUNT_ID)
    .fetch_one(db.pool())
    .await
    .expect("Main Chat exists");
    sqlx::query("UPDATE agent_chat SET status = 'ready' WHERE id = ?")
        .bind(&main_chat_id)
        .execute(db.pool())
        .await
        .expect("Main Chat becomes ready");

    let genesis = ProductGenesisService::for_sqlite(Arc::clone(&db))
        .start(
            ACCOUNT_ID,
            Some(&main_chat_id),
            ProductMaturity::Mvp,
            Some("Create a durable Main-to-Project handoff".to_owned()),
            Some(PROJECT_IDENTITY_ID.to_owned()),
            GenesisPromptContext::default(),
        )
        .await
        .expect("Genesis starts");

    let source_message = AgentChatMessageRepo::append_agent_chat_message(
        &*db,
        CreateAgentChatMessage {
            id: "main-command-source-message".to_owned(),
            chat_id: main_chat_id.clone(),
            sequence: 0,
            author_type: AgentChatMessageAuthorType::User,
            author_id: Some(ACCOUNT_ID.to_owned()),
            content: "Create the approved Main command Project.".to_owned(),
            content_guard_json: "{}".to_owned(),
            sensitivity: "internal".to_owned(),
            status: AgentChatMessageStatus::Complete,
            outcome: Some("accepted".to_owned()),
            model: None,
            profile_id: None,
            session_id: None,
            context_manifest_id: None,
            token_usage_json: None,
            duration_ms: None,
            error: None,
            correlation_id: "main-command-source-correlation".to_owned(),
            causation_id: None,
            handoff_id: None,
            source_type: "native".to_owned(),
            source_id: Some(GENESIS_ID.to_owned()),
            source_message_id: None,
            source_room_id: None,
            source_conversation_id: None,
            source_sequence: None,
            source_metadata_json: "{}".to_owned(),
            created_at: NOW.to_owned(),
        },
    )
    .await
    .expect("Genesis source message creates");
    let source_turn = AgentChatTurnJobRepo::create_agent_chat_turn_job(
        &*db,
        CreateAgentChatTurnJob {
            id: "main-command-source-turn".to_owned(),
            chat_id: main_chat_id.clone(),
            triggering_message_id: source_message.id.clone(),
            responder_identity_id: MAIN_IDENTITY_ID.to_owned(),
            profile_id: MAIN_PROFILE_ID.to_owned(),
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
            canonical_scope_id: main_chat_id.clone(),
            dedupe_key: "main-command-source-turn-dedupe".to_owned(),
            max_attempts: 3,
            correlation_id: "main-command-source-correlation".to_owned(),
            causation_id: None,
            causation_depth: 0,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("Genesis source turn creates");
    ProductGenesisService::for_sqlite(Arc::clone(&db))
        .record_source_message(
            &genesis.session.id,
            genesis.session.version,
            &source_message.id,
        )
        .await
        .expect("Genesis source message records");

    let content = charter_content();
    let rendered = services::render_and_digest_charter(&content);
    let content_json = serde_json::to_string(&content).expect("Charter serializes");
    ProjectOrchestrationRepo::create_project_charter(
        &*db,
        CreateProjectCharter {
            id: CHARTER_ID.to_owned(),
            account_id: ACCOUNT_ID.to_owned(),
            genesis_session_id: Some(genesis.session.id.clone()),
            project_mode: "compact".to_owned(),
            maturity: "mvp".to_owned(),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("Charter creates");
    ProjectOrchestrationRepo::create_project_charter_revision(
        &*db,
        CreateProjectCharterRevision {
            id: CHARTER_REVISION_ID.to_owned(),
            charter_id: CHARTER_ID.to_owned(),
            expected_charter_version: 1,
            project_mode: "compact".to_owned(),
            maturity: "mvp".to_owned(),
            base_revision: 0,
            base_revision_id: None,
            lifecycle: "proposed".to_owned(),
            schema_version: "forge.project-charter/v1".to_owned(),
            render_version: rendered.render_version.clone(),
            content_json,
            rendered_view: rendered.rendered_view.clone(),
            change_summary: "Initial exact Main command Charter".to_owned(),
            author_type: "user".to_owned(),
            author_id: Some(ACCOUNT_ID.to_owned()),
            source_message_id: Some(source_message.id.clone()),
            source_turn_job_id: Some(source_turn.id.clone()),
            source_refs_json: "[]".to_owned(),
            content_digest: rendered.content_digest.clone(),
            rendered_digest: rendered.render_digest.clone(),
            created_at: NOW.to_owned(),
            command_receipt: None,
            action_execution: None,
        },
    )
    .await
    .expect("Charter revision creates");

    let skill_revision_id: String = sqlx::query_scalar(
        "SELECT revision.id
         FROM operating_skill AS skill
         JOIN operating_skill_revision AS revision
           ON revision.id = skill.current_revision_id
          AND revision.operating_skill_id = skill.id
         WHERE skill.skill_key = ? AND skill.lifecycle = 'active'
         LIMIT 1",
    )
    .bind(services::PROJECT_OPERATING_SKILL_KEY)
    .fetch_one(db.pool())
    .await
    .expect("Project operating skill revision exists");
    let policy_digest = project_agent_policy_digest(project_tool_policy);
    ProjectOrchestrationRepo::approve_project_charter(
        &*db,
        db::ApproveProjectCharter {
            id: APPROVAL_ID.to_owned(),
            approval_type: "project_creation".to_owned(),
            charter_id: CHARTER_ID.to_owned(),
            revision_id: CHARTER_REVISION_ID.to_owned(),
            content_digest: rendered.content_digest,
            rendered_digest: rendered.render_digest,
            expected_charter_version: 2,
            approved_name: Some("Main Command Project".to_owned()),
            approved_slug: Some("main-command-project".to_owned()),
            approved_project_mode: "compact".to_owned(),
            selected_identity_id: Some(PROJECT_IDENTITY_ID.to_owned()),
            selected_profile_id: Some(PROJECT_PROFILE_ID.to_owned()),
            selected_operating_skill_revision_id: Some(skill_revision_id),
            selected_policy_revision: Some("project-policy@1".to_owned()),
            selected_policy_digest: Some(policy_digest),
            approving_principal_type: "user".to_owned(),
            approving_principal_id: ACCOUNT_ID.to_owned(),
            authorization_basis: "explicit user approval".to_owned(),
            authorization_action: "project.charter.approve".to_owned(),
            explicit_event: "approve exact Main command Charter".to_owned(),
            authorization_occurred_at: authorization_now.clone(),
            source_action: "product_genesis.charter_approval".to_owned(),
            idempotency_key: "main-command-approval-key".to_owned(),
            event_id: "main-command-approval-event".to_owned(),
            created_at: authorization_now.clone(),
            updated_at: authorization_now.clone(),
        },
    )
    .await
    .expect("Charter approval creates");

    let action = AgentActionRepo::create_action(
        &*db,
        CreateAgentAction {
            id: ACTION_ID.to_owned(),
            actor_identity_id: MAIN_IDENTITY_ID.to_owned(),
            scope_type: "agent_chat".to_owned(),
            scope_id: main_chat_id.clone(),
            operation: MAIN_PROJECT_CREATE_OPERATION.to_owned(),
            payload_json: json!({"approval_id": APPROVAL_ID}).to_string(),
            payload_hash: "main-command-payload-hash".to_owned(),
            dedupe_key: ACTION_DEDUPE_KEY.to_owned(),
            correlation_id: "main-command-correlation".to_owned(),
            causation_id: None,
            causation_depth: 0,
            requested_permission: "propose_project".to_owned(),
            policy_result: AgentActionPolicyResult::Allowed,
            policy_reason: None,
            status: AgentActionStatus::Proposed,
            target_type: Some("project".to_owned()),
            target_id: Some(APPROVAL_ID.to_owned()),
            created_at: authorization_now.clone(),
            updated_at: authorization_now,
        },
    )
    .await
    .expect("Main Project action creates");

    Fixture {
        db,
        main_chat_id,
        action_version: action.version,
    }
}

async fn file_fixture() -> (Fixture, PathBuf) {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time is after epoch")
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "forge-main-project-service-race-{}-{nanos}.db",
        std::process::id()
    ));
    (
        fixture_with_url(&format!("sqlite://{}", path.display())).await,
        path,
    )
}

fn direct_project_input(authorization_basis: &str) -> CreateProjectFromCharterApprovalInput {
    CreateProjectFromCharterApprovalInput {
        approval_id: APPROVAL_ID.to_owned(),
        idempotency_key: "main-service-project-create-race".to_owned(),
        account_id: ACCOUNT_ID.to_owned(),
        authorization: CreateProjectAuthorization {
            principal_type: "user".to_owned(),
            principal_id: ACCOUNT_ID.to_owned(),
            action: "product_genesis.create_project_from_approval".to_owned(),
            authorization_basis: authorization_basis.to_owned(),
            event_id: "main-service-project-create-event".to_owned(),
            occurred_at: now_rfc3339(),
        },
        correlation_id: "transport-correlation-is-not-canonical".to_owned(),
        causation_depth: 0,
        command_receipt: None,
        action_execution: None,
    }
}

fn command_input(fixture: &Fixture) -> ExecuteMainOrchestrationActionInput {
    ExecuteMainOrchestrationActionInput {
        action_id: ACTION_ID.to_owned(),
        expected_version: fixture.action_version,
        executed_by_type: "user".to_owned(),
        executed_by_id: ACCOUNT_ID.to_owned(),
        idempotency_key: EXECUTION_IDEMPOTENCY_KEY.to_owned(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_project_creation_service_replays_frozen_packet_ids() {
    let (fixture, path) = file_fixture().await;
    let first_input = direct_project_input("authenticated user executed Project creation");
    let second_input = first_input.clone();
    let (first, second) = tokio::join!(
        create_project_from_charter_approval(Arc::clone(&fixture.db), first_input),
        create_project_from_charter_approval(Arc::clone(&fixture.db), second_input),
    );
    let first = first.expect("first ProjectCreationService submission");
    let second = second.expect("second ProjectCreationService replay");
    assert_eq!(first, second);
    assert_eq!(first.charter_id, CHARTER_ID);
    assert_eq!(first.charter_revision_id, CHARTER_REVISION_ID);
    for (query, label) in [
        ("SELECT COUNT(*) FROM project", "Project"),
        ("SELECT COUNT(*) FROM agent_handoff", "handoff"),
        (
            "SELECT COUNT(*) FROM agent_chat_message WHERE handoff_id IS NOT NULL",
            "target message",
        ),
        (
            "SELECT COUNT(*) FROM agent_chat_turn_job WHERE dedupe_key LIKE 'handoff:%'",
            "target turn",
        ),
        ("SELECT COUNT(*) FROM command_receipt", "command receipt"),
    ] {
        assert_eq!(
            sqlx::query_scalar::<_, i64>(query)
                .fetch_one(fixture.db.pool())
                .await
                .expect(label),
            1,
            "one {label} after the exact race"
        );
    }
    let stored_packet: String =
        sqlx::query_scalar("SELECT source_revisions_json FROM agent_handoff WHERE id = ?")
            .bind(&first.handoff_id)
            .fetch_one(fixture.db.pool())
            .await
            .expect("frozen handoff packet");
    let stored_packet: Value = serde_json::from_str(&stored_packet).expect("packet JSON");
    assert_eq!(stored_packet["approval_id"], APPROVAL_ID);
    assert_eq!(
        stored_packet["project"]["id"].as_str(),
        Some(first.project.id.as_str())
    );
    assert_eq!(
        stored_packet["target"]["binding_id"].as_str(),
        Some(first.project_agent_binding_id.as_str())
    );
    assert_eq!(
        stored_packet["target"]["message_id"].as_str(),
        Some(first.target_message_id.as_str())
    );
    assert_eq!(
        stored_packet["target"]["turn_id"].as_str(),
        Some(first.target_turn_id.as_str())
    );

    let changed_input = create_project_from_charter_approval(
        Arc::clone(&fixture.db),
        direct_project_input("changed authorization basis"),
    )
    .await;
    assert!(
        matches!(
            changed_input,
            Err(services::ServiceError::Db(db::DbError::IdempotencyConflict))
        ),
        "changed semantic authorization input must conflict"
    );

    // The loser of the race re-resolves once against the now-consumed
    // approval, so a genuine conflict must not be retried into someone else's
    // committed receipt. A different request key against the same consumed
    // approval owns no handoff and therefore stays a conflict.
    let mut foreign_key_input =
        direct_project_input("authenticated user executed Project creation");
    foreign_key_input.idempotency_key = "main-service-project-create-race-other".to_owned();
    let foreign_key =
        create_project_from_charter_approval(Arc::clone(&fixture.db), foreign_key_input).await;
    assert!(
        matches!(foreign_key, Err(services::ServiceError::Conflict(_))),
        "a request key with no committed handoff must not adopt the consumed receipt, got \
         {foreign_key:?}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project")
            .fetch_one(fixture.db.pool())
            .await
            .expect("Project count after the foreign-key attempt"),
        1,
        "a refused foreign request key materializes no second Project"
    );
    fixture.db.pool().close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn main_project_create_is_one_account_receipt_and_exactly_replayable() {
    let fixture = fixture().await;
    let service = MainOrchestrationActionService::new(Arc::clone(&fixture.db));
    let input = command_input(&fixture);

    let execution = service
        .execute(input.clone())
        .await
        .expect("Main Project command executes");
    let result = execution
        .result_json
        .clone()
        .expect("execution freezes outcome");
    let outcome: Value = serde_json::from_str(&result).expect("execution outcome is JSON");
    assert_eq!(outcome["operation"], MAIN_PROJECT_CREATE_OPERATION);
    for field in [
        "project_id",
        "project_agent_binding_id",
        "project_chat_id",
        "charter_id",
        "charter_revision_id",
        "handoff_id",
        "target_message_id",
        "target_turn_id",
    ] {
        assert!(outcome[field].as_str().is_some(), "{field} is frozen");
    }
    assert!(outcome["execution_setup"].is_object());

    let project_id = outcome["project_id"].as_str().unwrap();
    let binding_id = outcome["project_agent_binding_id"].as_str().unwrap();
    let project_chat_id = outcome["project_chat_id"].as_str().unwrap();
    let handoff_id = outcome["handoff_id"].as_str().unwrap();
    let target_message_id = outcome["target_message_id"].as_str().unwrap();
    let target_turn_id = outcome["target_turn_id"].as_str().unwrap();

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project WHERE id = ?")
            .bind(project_id)
            .fetch_one(fixture.db.pool())
            .await
            .expect("Project count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_agent_binding
             WHERE id = ? AND project_id = ? AND state = 'active'",
        )
        .bind(binding_id)
        .bind(project_id)
        .fetch_one(fixture.db.pool())
        .await
        .expect("active binding count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_chat WHERE id = ? AND project_id = ? AND kind = 'project'",
        )
        .bind(project_chat_id)
        .bind(project_id)
        .fetch_one(fixture.db.pool())
        .await
        .expect("Project Chat count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_handoff WHERE id = ?")
            .bind(handoff_id)
            .fetch_one(fixture.db.pool())
            .await
            .expect("handoff count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_chat_message WHERE id = ?")
            .bind(target_message_id)
            .fetch_one(fixture.db.pool())
            .await
            .expect("target message count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_chat_turn_job WHERE id = ?")
            .bind(target_turn_id)
            .fetch_one(fixture.db.pool())
            .await
            .expect("target turn count"),
        1
    );

    let receipt = sqlx::query(
        "SELECT principal_type, principal_id, scope_type, scope_id, operation,
                idempotency_key, input_digest, event_id, agent_action_execution_id,
                outcome_json
         FROM command_receipt
         WHERE operation = ? AND idempotency_key = ?",
    )
    .bind(MAIN_PROJECT_CREATE_OPERATION)
    .bind(EXECUTION_IDEMPOTENCY_KEY)
    .fetch_one(fixture.db.pool())
    .await
    .expect("Main command receipt");
    assert_eq!(receipt.get::<String, _>("principal_type"), "user");
    assert_eq!(receipt.get::<String, _>("principal_id"), ACCOUNT_ID);
    assert_eq!(receipt.get::<String, _>("scope_type"), "account");
    assert_eq!(receipt.get::<String, _>("scope_id"), ACCOUNT_ID);
    assert_eq!(
        receipt.get::<String, _>("operation"),
        MAIN_PROJECT_CREATE_OPERATION
    );
    let action_scope: (String, String) =
        sqlx::query_as("SELECT scope_type, scope_id FROM agent_action WHERE id = ?")
            .bind(ACTION_ID)
            .fetch_one(fixture.db.pool())
            .await
            .expect("originating Main action scope");
    assert_eq!(
        action_scope,
        ("agent_chat".to_owned(), fixture.main_chat_id)
    );
    assert!(!receipt.get::<String, _>("input_digest").is_empty());
    assert_eq!(
        receipt.get::<String, _>("agent_action_execution_id"),
        execution.id
    );
    let receipt_outcome = serde_json::from_str::<Value>(&receipt.get::<String, _>("outcome_json"))
        .expect("receipt outcome JSON");
    for field in [
        "operation",
        "project_id",
        "project_agent_binding_id",
        "project_chat_id",
        "charter_id",
        "charter_revision_id",
        "handoff_id",
        "target_message_id",
        "target_turn_id",
    ] {
        assert_eq!(receipt_outcome[field], outcome[field], "frozen {field}");
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM domain_event WHERE id = ?")
            .bind(receipt.get::<String, _>("event_id"))
            .fetch_one(fixture.db.pool())
            .await
            .expect("receipt event count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'project.created_from_charter_approval'
               AND entity_id = ?",
        )
        .bind(project_id)
        .fetch_one(fixture.db.pool())
        .await
        .expect("Project event count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_action_execution WHERE action_id = ?",
        )
        .bind(ACTION_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("action execution count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM agent_action WHERE id = ?")
            .bind(ACTION_ID)
            .fetch_one(fixture.db.pool())
            .await
            .expect("action status"),
        "executed"
    );

    // A response lost after commit is an exact replay of the same execution;
    // the consumed approval and all generated handoff identifiers remain
    // frozen, and no second action execution/event/domain row appears.
    let replay = service
        .execute(input.clone())
        .await
        .expect("response-loss replay");
    assert_eq!(replay.id, execution.id);
    assert_eq!(replay.result_json, execution.result_json);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project WHERE owner_id = ?")
            .bind(ACCOUNT_ID)
            .fetch_one(fixture.db.pool())
            .await
            .expect("one account Project"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_action_execution WHERE action_id = ?",
        )
        .bind(ACTION_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("one action execution after replay"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'project.created_from_charter_approval'",
        )
        .fetch_one(fixture.db.pool())
        .await
        .expect("one Project event after replay"),
        1
    );

    let principal_conflict = service
        .execute(ExecuteMainOrchestrationActionInput {
            executed_by_type: "agent".to_owned(),
            executed_by_id: MAIN_IDENTITY_ID.to_owned(),
            ..input.clone()
        })
        .await;
    assert!(
        principal_conflict.is_err(),
        "the same key with a different principal must conflict"
    );

    // The proposal payload is part of the canonical command digest. Simulate
    // a persisted payload change and ensure it cannot replay the original
    // action execution under the same key.
    sqlx::query("UPDATE agent_action SET payload_json = ? WHERE id = ?")
        .bind(json!({"approval_id": "another-approval"}).to_string())
        .bind(ACTION_ID)
        .execute(fixture.db.pool())
        .await
        .expect("test payload mutation");
    let payload_conflict = service.execute(input).await;
    assert!(
        payload_conflict.is_err(),
        "the same key with a changed payload must conflict"
    );
}

#[tokio::test]
async fn main_project_create_receipt_trigger_rolls_back_the_entire_bundle_and_retry_succeeds() {
    let fixture = fixture().await;
    let trigger = format!(
        "CREATE TEMP TRIGGER main_project_create_receipt_failpoint
         BEFORE INSERT ON command_receipt
         WHEN NEW.operation = '{MAIN_PROJECT_CREATE_OPERATION}'
         BEGIN SELECT RAISE(ABORT, 'Main Project receipt failpoint'); END;"
    );
    sqlx::query(&trigger)
        .execute(fixture.db.pool())
        .await
        .expect("Main Project receipt failpoint");

    let service = MainOrchestrationActionService::new(Arc::clone(&fixture.db));
    let stopped = service
        .execute(command_input(&fixture))
        .await
        .expect_err("receipt trigger stops the Main Project command");
    assert!(
        stopped
            .to_string()
            .contains("Main Project receipt failpoint"),
        "unexpected Main Project trigger error: {stopped}"
    );

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project")
            .fetch_one(fixture.db.pool())
            .await
            .expect("Project count after rollback"),
        0,
        "Project shell rolls back with the receipt"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_agent_binding WHERE state = 'active'",
        )
        .fetch_one(fixture.db.pool())
        .await
        .expect("active Project binding count after rollback"),
        0,
        "active Project Agent binding rolls back with the receipt"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_chat WHERE kind = 'project'",)
            .fetch_one(fixture.db.pool())
            .await
            .expect("Project Chat count after rollback"),
        0,
        "Project Chat rolls back with the receipt"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_handoff WHERE dedupe_key = ?",)
            .bind(EXECUTION_IDEMPOTENCY_KEY)
            .fetch_one(fixture.db.pool())
            .await
            .expect("handoff count after rollback"),
        0,
        "handoff rolls back with the receipt"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_chat_message WHERE handoff_id IS NOT NULL",
        )
        .fetch_one(fixture.db.pool())
        .await
        .expect("handoff message count after rollback"),
        0,
        "handoff target message rolls back with the receipt"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_chat_turn_job WHERE dedupe_key = ?",
        )
        .bind(format!("handoff:{EXECUTION_IDEMPOTENCY_KEY}"))
        .fetch_one(fixture.db.pool())
        .await
        .expect("handoff turn count after rollback"),
        0,
        "handoff target turn rolls back with the receipt"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'project.created_from_charter_approval'",
        )
        .fetch_one(fixture.db.pool())
        .await
        .expect("Project event count after rollback"),
        0,
        "Project domain event rolls back with the receipt"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE operation = ? AND idempotency_key = ?",
        )
        .bind(MAIN_PROJECT_CREATE_OPERATION)
        .bind(EXECUTION_IDEMPOTENCY_KEY)
        .fetch_one(fixture.db.pool())
        .await
        .expect("receipt count after rollback"),
        0,
        "failed Main Project command leaves no receipt"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_action_execution WHERE action_id = ?",
        )
        .bind(ACTION_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("action execution count after rollback"),
        0,
        "action execution rolls back with the receipt"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM agent_action WHERE id = ?")
            .bind(ACTION_ID)
            .fetch_one(fixture.db.pool())
            .await
            .expect("action status after rollback"),
        "proposed",
        "the originating action remains retryable"
    );

    sqlx::query("DROP TRIGGER main_project_create_receipt_failpoint")
        .execute(fixture.db.pool())
        .await
        .expect("remove Main Project receipt failpoint");
    let committed = service
        .execute(command_input(&fixture))
        .await
        .expect("retry succeeds after the receipt trigger is removed");
    let outcome: Value = serde_json::from_str(
        committed
            .result_json
            .as_deref()
            .expect("retry freezes the Main Project outcome"),
    )
    .expect("retry outcome JSON");
    for field in [
        "project_id",
        "project_agent_binding_id",
        "project_chat_id",
        "handoff_id",
        "target_message_id",
        "target_turn_id",
    ] {
        assert!(outcome[field].as_str().is_some(), "{field} is frozen");
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project")
            .fetch_one(fixture.db.pool())
            .await
            .expect("Project count after retry"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_agent_binding WHERE state = 'active'",
        )
        .fetch_one(fixture.db.pool())
        .await
        .expect("active binding count after retry"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_chat WHERE kind = 'project'")
            .fetch_one(fixture.db.pool())
            .await
            .expect("Project Chat count after retry"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_handoff WHERE dedupe_key = ?",)
            .bind(EXECUTION_IDEMPOTENCY_KEY)
            .fetch_one(fixture.db.pool())
            .await
            .expect("handoff count after retry"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'project.created_from_charter_approval'",
        )
        .fetch_one(fixture.db.pool())
        .await
        .expect("Project event count after retry"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE operation = ? AND idempotency_key = ?",
        )
        .bind(MAIN_PROJECT_CREATE_OPERATION)
        .bind(EXECUTION_IDEMPOTENCY_KEY)
        .fetch_one(fixture.db.pool())
        .await
        .expect("receipt count after retry"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_action_execution WHERE action_id = ?",
        )
        .bind(ACTION_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("action execution count after retry"),
        1
    );
}

#[tokio::test]
async fn main_project_create_returns_committed_ids_when_provisioning_fails_after_commit() {
    let fixture = fixture().await;
    sqlx::query(
        "CREATE TEMP TRIGGER project_provisioning_failure_failpoint
         BEFORE UPDATE ON project_provisioning_operation
         WHEN OLD.status = 'setup_required' AND NEW.status = 'provisioning'
         BEGIN SELECT RAISE(ABORT, 'provisioning failpoint'); END;",
    )
    .execute(fixture.db.pool())
    .await
    .expect("provisioning failure failpoint");

    let service = MainOrchestrationActionService::new(Arc::clone(&fixture.db));
    let committed = service
        .execute(command_input(&fixture))
        .await
        .expect("committed Project returns despite post-commit provisioning failure");
    let outcome: Value = serde_json::from_str(
        committed
            .result_json
            .as_deref()
            .expect("Project-create outcome"),
    )
    .expect("Project-create outcome JSON");
    for field in [
        "project_id",
        "project_agent_binding_id",
        "project_chat_id",
        "handoff_id",
        "target_message_id",
        "target_turn_id",
    ] {
        assert!(
            outcome[field].as_str().is_some(),
            "{field} remains committed"
        );
    }
    assert_eq!(
        outcome["execution_setup"]["execution_setup_state"],
        "failed"
    );
    assert_eq!(
        outcome["execution_setup"]["provisioning"]["last_error_code"],
        "provisioning_reconciliation_failed"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status FROM project_provisioning_operation
             WHERE project_id = ?",
        )
        .bind(outcome["project_id"].as_str().expect("Project id"))
        .fetch_one(fixture.db.pool())
        .await
        .expect("failed provisioning operation"),
        "failed"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project")
            .fetch_one(fixture.db.pool())
            .await
            .expect("committed Project count"),
        1
    );

    sqlx::query("DROP TRIGGER project_provisioning_failure_failpoint")
        .execute(fixture.db.pool())
        .await
        .expect("remove provisioning failure failpoint");
    let replay = service
        .execute(command_input(&fixture))
        .await
        .expect("replay returns current failed setup projection");
    let replay_outcome: Value = serde_json::from_str(
        replay
            .result_json
            .as_deref()
            .expect("replayed Project-create outcome"),
    )
    .expect("replayed Project-create outcome JSON");
    assert_eq!(
        replay_outcome["project_id"], outcome["project_id"],
        "replay preserves the committed Project id"
    );
    assert_eq!(
        replay_outcome["execution_setup"]["execution_setup_state"],
        "failed"
    );
    assert_eq!(
        replay_outcome["execution_setup"]["provisioning"]["last_error_code"],
        "provisioning_reconciliation_failed"
    );
}

#[tokio::test]
async fn main_project_create_replay_refreshes_current_provisioning_projection() {
    let fixture = fixture().await;
    let service = MainOrchestrationActionService::new(Arc::clone(&fixture.db));
    let input = command_input(&fixture);
    let first = service
        .execute(input.clone())
        .await
        .expect("Main Project command executes");
    let first_outcome: Value = serde_json::from_str(
        first
            .result_json
            .as_deref()
            .expect("first outcome is present"),
    )
    .expect("first outcome is JSON");
    let project_id = first_outcome["project_id"]
        .as_str()
        .expect("Project id is present")
        .to_owned();
    assert_eq!(
        first_outcome["execution_setup"]["execution_setup_state"],
        "ready"
    );

    sqlx::query(
        "UPDATE project_provisioning_operation
         SET status = 'failed', retryable = 0,
             last_error_code = 'provisioning_retry_exhausted',
             last_error_message = 'finite retry budget exhausted',
             next_retry_at = NULL, completed_at = NULL,
             version = version + 1, updated_at = ?
         WHERE project_id = ?",
    )
    .bind(db::now_rfc3339())
    .bind(&project_id)
    .execute(fixture.db.pool())
    .await
    .expect("provisioning state advances after first response");

    let replay = service
        .execute(input)
        .await
        .expect("replay resolves frozen command and refreshes setup");
    assert_eq!(replay.id, first.id);
    let replay_outcome: Value = serde_json::from_str(
        replay
            .result_json
            .as_deref()
            .expect("replay outcome is present"),
    )
    .expect("replay outcome is JSON");
    assert_eq!(replay_outcome["project_id"], first_outcome["project_id"]);
    assert_eq!(
        replay_outcome["execution_setup"]["execution_setup_state"],
        "ready"
    );
    assert_eq!(
        replay_outcome["execution_setup"]["provisioning"]["last_error_code"],
        "provisioning_retry_exhausted"
    );
}

#[tokio::test]
async fn main_project_create_reopens_service_after_committed_response_loss() {
    let (fixture, path) = file_fixture().await;
    let input = command_input(&fixture);
    let first = MainOrchestrationActionService::new(Arc::clone(&fixture.db))
        .execute(input.clone())
        .await
        .expect("Main Project command commits before the response is lost");
    let first_result = first.result_json.clone().expect("frozen command outcome");
    let first_outcome: Value = serde_json::from_str(&first_result).expect("outcome JSON");
    let first_receipt = sqlx::query(
        "SELECT id, event_id, agent_action_execution_id, outcome_json
         FROM command_receipt
         WHERE operation = ? AND idempotency_key = ?",
    )
    .bind(MAIN_PROJECT_CREATE_OPERATION)
    .bind(EXECUTION_IDEMPOTENCY_KEY)
    .fetch_one(fixture.db.pool())
    .await
    .expect("committed Main Project receipt");
    let first_receipt_id = first_receipt.get::<String, _>("id");
    let first_event_id = first_receipt.get::<String, _>("event_id");
    let first_action_execution_id = first_receipt.get::<String, _>("agent_action_execution_id");
    assert_eq!(
        first_action_execution_id, first.id,
        "receipt freezes the action execution identity"
    );
    let first_receipt_outcome: Value =
        serde_json::from_str(&first_receipt.get::<String, _>("outcome_json"))
            .expect("receipt outcome JSON");
    for field in [
        "operation",
        "project_id",
        "project_agent_binding_id",
        "project_chat_id",
        "charter_id",
        "charter_revision_id",
        "handoff_id",
        "target_message_id",
        "target_turn_id",
    ] {
        assert_eq!(
            first_receipt_outcome[field], first_outcome[field],
            "frozen {field}"
        );
    }
    let frozen_ids = [
        "project_id",
        "project_agent_binding_id",
        "project_chat_id",
        "handoff_id",
        "target_message_id",
        "target_turn_id",
    ]
    .into_iter()
    .map(|field| {
        (
            field,
            first_outcome[field]
                .as_str()
                .expect("outcome identifier")
                .to_owned(),
        )
    })
    .collect::<std::collections::BTreeMap<_, _>>();

    // Model process loss after the transaction has committed: close the old
    // pool, reopen the file-backed database, and resolve the same command via
    // a newly constructed service.
    fixture.db.pool().close().await;
    let reopened = database_with_url(&format!("sqlite://{}", path.display())).await;
    let replay = MainOrchestrationActionService::new(Arc::clone(&reopened))
        .execute(input)
        .await
        .expect("reopened service replays the committed command");
    assert_eq!(replay.id, first.id, "action execution id is frozen");
    assert_eq!(replay.result_json, first.result_json);
    let replay_outcome: Value = serde_json::from_str(
        replay
            .result_json
            .as_deref()
            .expect("replay freezes command outcome"),
    )
    .expect("replay outcome JSON");
    for (field, value) in &frozen_ids {
        assert_eq!(
            replay_outcome[field].as_str(),
            Some(value.as_str()),
            "{field}"
        );
    }
    let replay_receipt = sqlx::query(
        "SELECT id, event_id, agent_action_execution_id, outcome_json
         FROM command_receipt
         WHERE operation = ? AND idempotency_key = ?",
    )
    .bind(MAIN_PROJECT_CREATE_OPERATION)
    .bind(EXECUTION_IDEMPOTENCY_KEY)
    .fetch_one(reopened.pool())
    .await
    .expect("replayed Main Project receipt");
    assert_eq!(replay_receipt.get::<String, _>("id"), first_receipt_id);
    assert_eq!(replay_receipt.get::<String, _>("event_id"), first_event_id);
    assert_eq!(
        replay_receipt.get::<String, _>("agent_action_execution_id"),
        first_action_execution_id
    );
    let replay_receipt_outcome: Value =
        serde_json::from_str(&replay_receipt.get::<String, _>("outcome_json"))
            .expect("replay receipt outcome JSON");
    assert_eq!(replay_receipt_outcome, first_receipt_outcome);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project")
            .fetch_one(reopened.pool())
            .await
            .expect("one Project after process-loss replay"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_agent_binding WHERE state = 'active'",
        )
        .fetch_one(reopened.pool())
        .await
        .expect("one active binding after process-loss replay"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_chat WHERE kind = 'project'")
            .fetch_one(reopened.pool())
            .await
            .expect("one Project Chat after process-loss replay"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_handoff WHERE dedupe_key = ?",)
            .bind(EXECUTION_IDEMPOTENCY_KEY)
            .fetch_one(reopened.pool())
            .await
            .expect("one handoff after process-loss replay"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_chat_message WHERE handoff_id IS NOT NULL",
        )
        .fetch_one(reopened.pool())
        .await
        .expect("one handoff message after process-loss replay"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_chat_turn_job WHERE dedupe_key = ?",
        )
        .bind(format!("handoff:{EXECUTION_IDEMPOTENCY_KEY}"))
        .fetch_one(reopened.pool())
        .await
        .expect("one handoff turn after process-loss replay"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'project.created_from_charter_approval'",
        )
        .fetch_one(reopened.pool())
        .await
        .expect("one Project event after process-loss replay"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE operation = ? AND idempotency_key = ?",
        )
        .bind(MAIN_PROJECT_CREATE_OPERATION)
        .bind(EXECUTION_IDEMPOTENCY_KEY)
        .fetch_one(reopened.pool())
        .await
        .expect("one receipt after process-loss replay"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_action_execution WHERE action_id = ?",
        )
        .bind(ACTION_ID)
        .fetch_one(reopened.pool())
        .await
        .expect("one action execution after process-loss replay"),
        1
    );
    reopened.pool().close().await;
    let _ = std::fs::remove_file(path);
}

/// A `TaskExecutor` that must never be reached. The Project Agent turns below
/// deliberately pair the `cli` backend with an executor that backend refuses,
/// so a turn that gets past handoff authentication fails at backend dispatch
/// instead of requiring a live provider.
#[derive(Debug)]
struct UnreachableExecutor;

#[async_trait::async_trait]
impl executors::TaskExecutor for UnreachableExecutor {
    async fn execute(
        &self,
        _ctx: executors::ExecutionContext,
    ) -> std::result::Result<executors::ExecutionResult, executors::ExecutorError> {
        panic!("a rejected or unsupported Agent Chat turn must never reach an executor");
    }

    async fn cancel(
        &self,
        _execution_id: &str,
    ) -> std::result::Result<(), executors::ExecutorError> {
        panic!("a rejected or unsupported Agent Chat turn must never reach an executor");
    }
}

/// The message that proves the turn got all the way past handoff
/// authentication and reached backend dispatch. The Project Agent Profile in
/// this test pairs the `cli` backend with the `embedded` executor, which the
/// CLI backend refuses by contract, so a consumed handoff has exactly one
/// deterministic outcome and never needs a live provider.
const HANDOFF_ACCEPTED: &str = "selected executor cannot run a legacy CLI Agent Chat turn";

/// `HAND-04` / `HAND-05` — one end-to-end Project-Agent turn per historical
/// source drift case.
///
/// Every case below runs the real `FederatedAgentChatTurnRunner` against a
/// real server-issued handoff produced by the atomic Project-create command,
/// so the receipt/current-authority consumption path is exercised. The
/// control case proves the exact packet is accepted; the other cases prove a
/// later turn does not re-walk mutable Main identity, Project display name, or
/// Genesis lifecycle after that packet was admitted atomically.
///
/// Forged-packet cases (a tampered digest, a cross-Project id, a rewritten
/// authorization envelope) are deliberately *not* reproduced here by editing a
/// stored packet: `agent_handoff` is immutable by trigger and its only writer
/// is the atomic create transaction, which rolls the entire Project back on an
/// invalid packet (`db::project_orchestration::
/// charter_approval_create_rolls_back_on_invalid_handoff_packet`). Those cases
/// are covered field-by-field by the validator matrix in
/// `services::agent_chat_turn_worker`.
#[tokio::test]
async fn project_turn_uses_issued_handoff_without_rewalking_mutable_main_history() {
    #[derive(Clone, Copy)]
    enum Case {
        /// The exact server-issued packet against unchanged server truth.
        Matching,
        /// Main ownership changed after the immutable Project admission.
        UnauthenticatedSourceAuthor,
        /// The Project display name changed after admission.
        ProjectIdentityDrift,
        /// The historical Genesis lifecycle changed after admission.
        StaleGenesisProvenance,
    }

    for (case, label, expected) in [
        (Case::Matching, "matching", HANDOFF_ACCEPTED),
        (
            Case::UnauthenticatedSourceAuthor,
            "unauthenticated source author",
            HANDOFF_ACCEPTED,
        ),
        (
            Case::ProjectIdentityDrift,
            "project identity drift",
            HANDOFF_ACCEPTED,
        ),
        (
            Case::StaleGenesisProvenance,
            "stale Genesis provenance",
            HANDOFF_ACCEPTED,
        ),
    ] {
        let db = database().await;
        let _fixture = fixture_with_project_backend(Arc::clone(&db), "cli").await;
        let created = create_project_from_charter_approval(
            Arc::clone(&db),
            direct_project_input("authenticated user executed Project creation"),
        )
        .await
        .expect("the atomic Project-create command issues the exact handoff");

        match case {
            Case::Matching => {}
            Case::UnauthenticatedSourceAuthor => {
                sqlx::query("UPDATE agent_identity SET owner_id = NULL WHERE id = ?")
                    .bind(MAIN_IDENTITY_ID)
                    .execute(db.pool())
                    .await
                    .expect("Main author ownership changes");
            }
            Case::ProjectIdentityDrift => {
                sqlx::query("UPDATE project SET name = ? WHERE id = ?")
                    .bind("Renamed After Handoff")
                    .bind(&created.project.id)
                    .execute(db.pool())
                    .await
                    .expect("Project is renamed after the packet froze");
            }
            Case::StaleGenesisProvenance => {
                sqlx::query("UPDATE product_genesis_session SET lifecycle = 'cancelled'")
                    .execute(db.pool())
                    .await
                    .expect("Genesis lifecycle moves off handed_off");
            }
        }

        let job = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*db, &created.target_turn_id)
            .await
            .expect("target turn lookup")
            .expect("the create command queued the Project Agent turn");
        let runner = services::FederatedAgentChatTurnRunner::new(
            Arc::clone(&db),
            Arc::new(services::EmbeddedAgentService::new(
                Arc::clone(&db),
                b"hand04-characterization-key",
            )),
            Arc::new(UnreachableExecutor),
            services::AgentChatTurnLogRoot::new(std::env::temp_dir().join("forge-test-turn-logs")),
        );
        let error = services::AgentChatTurnRunner::run_turn(
            &runner,
            &job,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("no provider backend is configured for this characterization");
        let message = error.to_string();
        assert!(
            message.contains(expected),
            "{label}: expected {expected:?}, got {message:?}"
        );
    }
}
