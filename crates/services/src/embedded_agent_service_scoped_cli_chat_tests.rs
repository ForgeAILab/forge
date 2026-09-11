use std::sync::Arc;

use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentChatRepo, AgentRepo,
    AgentStatus, CreateAgent, CreateProject, ProjectAgentBindingRepo, ProjectRepo, SqliteDb,
};
use forge_agent_host::{
    CanonicalScope, CanonicalScopeType, WorkspaceAccess, FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
    FORGE_PROJECT_ORCHESTRATION_READ_TOOL, PROJECT_CHARTER_ADOPTION_OPERATION,
};
use serde_json::json;

use super::{CreateScopedSession, EmbeddedAgentService, RequestedCanonicalScope};

const OWNER_ID: &str = "cli-chat-tools-owner";

struct CliChatFixture {
    db: Arc<SqliteDb>,
    service: EmbeddedAgentService,
    _workspace_root: tempfile::TempDir,
    identity_id: String,
    project_id: String,
    session_id: String,
    scope: CanonicalScope,
}

async fn fixture() -> CliChatFixture {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("SQLite pool creates");
    run_migrations(&pool).await.expect("SQLite migrations run");
    let db = Arc::new(SqliteDb::new(pool));
    let now = now_rfc3339();
    db::UserRepo::create_user(
        &*db,
        &db::User {
            id: OWNER_ID.to_owned(),
            email: "cli-chat-tools-owner@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: Some("CLI Chat tools owner".to_owned()),
            is_admin: true,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("owner creates");

    // AgentRepo::create persists the CLI backend/profile pair used by the
    // production CLI adapter. Its default ceilings are intentionally broad;
    // the Project binding below is the narrower persisted authority.
    let agent = AgentRepo::create(
        &*db,
        CreateAgent {
            id: new_uuid_v4(),
            name: "CLI Chat tools agent".to_owned(),
            description: None,
            executor_type: "codex".to_owned(),
            model: Some("test-model".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: Some(now.clone()),
            is_default: false,
            paused: false,
            owner_id: Some(OWNER_ID.to_owned()),
            visibility: "account".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("CLI identity/profile creates");

    let project_id = new_uuid_v4();
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: project_id.clone(),
            name: "CLI Chat tools Project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(OWNER_ID.to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("Project creates");
    let setup_binding = ProjectAgentBindingRepo::get_active_project_binding(&*db, &project_id)
        .await
        .expect("setup binding lookup")
        .expect("Project starts with setup binding");
    crate::AgentChatService::new(Arc::clone(&db))
        .set_project_binding(crate::SetProjectAgentBindingInput {
            actor_user_id: OWNER_ID.to_owned(),
            project_id: project_id.clone(),
            identity_id: Some(agent.id.clone()),
            state: "active".to_owned(),
            autonomy_policy_json: "{}".to_owned(),
            permission_ceiling_json: json!({
                "permissions": [
                    "read_agent_chat",
                    "read_memory",
                    "propose_project",
                    "propose_message"
                ]
            })
            .to_string(),
            subscriptions_json: "[]".to_owned(),
            wake_budget: 1,
            expected_version: Some(setup_binding.version),
            replacement_reason: Some("CLI Chat scoped-tools test".to_owned()),
        })
        .await
        .expect("Project Agent binding activates");

    let chat = AgentChatRepo::get_project_chat(&*db, &project_id)
        .await
        .expect("Project Chat lookup")
        .expect("Project Chat exists");
    let service = EmbeddedAgentService::new(Arc::clone(&db), b"cli-chat-tools-test-key");
    let workspace_root = tempfile::tempdir().expect("workspace root creates");
    service.set_workspace_root(
        workspace_root.path().to_path_buf(),
        workspace_root.path().to_path_buf(),
    );
    let session = service
        .create_or_resume_session(CreateScopedSession {
            actor_user_id: OWNER_ID.to_owned(),
            identity_id: agent.id.clone(),
            profile_id: Some(agent.profile_id.clone()),
            scope: RequestedCanonicalScope::AgentChat {
                chat_id: chat.id.clone(),
            },
        })
        .await
        .expect("persisted CLI Chat session creates");
    assert_eq!(session.backend_kind, "cli");
    assert!(session.runtime_session_id.is_none());

    CliChatFixture {
        db,
        service,
        _workspace_root: workspace_root,
        identity_id: agent.id,
        project_id,
        session_id: session.id,
        scope: CanonicalScope {
            scope_type: CanonicalScopeType::AgentChat,
            scope_id: chat.id,
            workspace_access: WorkspaceAccess::Deny,
        },
    }
}

#[tokio::test]
async fn persisted_cli_project_chat_exposes_setup_adoption_catalog() {
    let fixture = fixture().await;
    let context_scope_id: String =
        sqlx::query_scalar("SELECT context_scope_id FROM agent_session WHERE id = ?")
            .bind(&fixture.session_id)
            .fetch_one(fixture.db.pool())
            .await
            .expect("context scope lookup");
    let persisted: (String, Option<String>) = sqlx::query_as(
        "SELECT workspace_access, workspace_path
         FROM agent_context_scope WHERE id = ?",
    )
    .bind(context_scope_id)
    .fetch_one(fixture.db.pool())
    .await
    .expect("persisted Project Agent workspace lookup");
    assert_eq!(persisted.0, "project_verify");
    assert!(
        persisted.1.is_some(),
        "Project Agent scope owns its workspace"
    );

    let composition = fixture
        .service
        .cli_chat_tools(&fixture.session_id, &fixture.identity_id, &fixture.scope)
        .await
        .expect("persisted CLI Chat authority composes");

    assert_eq!(composition.actor_identity_id(), fixture.identity_id);
    assert_eq!(composition.scope(), &fixture.scope);
    let names = composition.tool_names();
    assert!(names.contains(&FORGE_PROJECT_ORCHESTRATION_READ_TOOL.to_owned()));
    assert!(names.contains(&FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL.to_owned()));

    let proposal = composition
        .tools()
        .into_iter()
        .find(|tool| tool.spec().name == FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL)
        .expect("Project proposal tool is exposed");
    let spec = proposal.spec();
    let operations = spec.input_schema["properties"]["operation"]["enum"]
        .as_array()
        .expect("proposal operation enum");
    assert_eq!(operations, &[json!(PROJECT_CHARTER_ADOPTION_OPERATION)]);
}

#[tokio::test]
async fn persisted_cli_main_chat_narrows_account_scratch_scope() {
    let fixture = fixture().await;
    crate::AgentChatService::new(Arc::clone(&fixture.db))
        .set_main_binding(crate::SetMainAgentBindingInput {
            actor_user_id: OWNER_ID.to_owned(),
            account_id: OWNER_ID.to_owned(),
            identity_id: fixture.identity_id.clone(),
            autonomy_policy_json: "{}".to_owned(),
            tool_policy_revision: "cli-main-scratch-test".to_owned(),
            expected_version: None,
            replacement_reason: Some("CLI Chat scratch narrowing test".to_owned()),
        })
        .await
        .expect("Main Agent binding creates");
    let chat = AgentChatRepo::get_main_chat(&*fixture.db, OWNER_ID)
        .await
        .expect("Main Chat lookup")
        .expect("Main Chat exists");
    let session = fixture
        .service
        .create_or_resume_session(CreateScopedSession {
            actor_user_id: OWNER_ID.to_owned(),
            identity_id: fixture.identity_id.clone(),
            profile_id: None,
            scope: RequestedCanonicalScope::AgentChat {
                chat_id: chat.id.clone(),
            },
        })
        .await
        .expect("persisted Main Chat session creates");

    let context_scope_id: String =
        sqlx::query_scalar("SELECT context_scope_id FROM agent_session WHERE id = ?")
            .bind(&session.id)
            .fetch_one(fixture.db.pool())
            .await
            .expect("context scope lookup");
    let persisted: (String, Option<String>) = sqlx::query_as(
        "SELECT workspace_access, workspace_path
         FROM agent_context_scope WHERE id = ?",
    )
    .bind(context_scope_id)
    .fetch_one(fixture.db.pool())
    .await
    .expect("persisted Main Agent workspace lookup");
    assert_eq!(persisted.0, "account_scratch");
    assert!(
        persisted.1.is_some(),
        "Main Agent scope owns its scratch directory"
    );

    let expected_scope = CanonicalScope {
        scope_type: CanonicalScopeType::AgentChat,
        scope_id: chat.id,
        workspace_access: WorkspaceAccess::Deny,
    };
    let composition = fixture
        .service
        .cli_chat_tools(&session.id, &fixture.identity_id, &expected_scope)
        .await
        .expect("CLI Main Chat authority narrows to denied scope");
    assert_eq!(composition.scope(), &expected_scope);
    assert!(
        composition
            .tool_names()
            .iter()
            .all(|name| !name.starts_with("forge_task_")),
        "CLI Main Chat never receives filesystem tools"
    );
}

#[tokio::test]
async fn cli_chat_tools_rejects_wrong_authority_missing_or_native_sessions() {
    let fixture = fixture().await;

    let wrong_identity = fixture
        .service
        .cli_chat_tools(&fixture.session_id, "wrong-identity", &fixture.scope)
        .await
        .expect_err("wrong identity must be rejected");
    assert!(matches!(
        wrong_identity,
        crate::ServiceError::InvalidOperation { ref message }
            if message == "CLI Chat session authority does not match the admitted turn"
    ));

    let wrong_scope = CanonicalScope {
        scope_type: CanonicalScopeType::AgentChat,
        scope_id: "wrong-chat".to_owned(),
        workspace_access: WorkspaceAccess::Deny,
    };
    let wrong_chat = fixture
        .service
        .cli_chat_tools(&fixture.session_id, &fixture.identity_id, &wrong_scope)
        .await
        .expect_err("wrong chat scope must be rejected");
    assert!(matches!(
        wrong_chat,
        crate::ServiceError::InvalidOperation { ref message }
            if message == "CLI Chat session authority does not match the admitted turn"
    ));

    let missing = fixture
        .service
        .cli_chat_tools("missing-session", &fixture.identity_id, &fixture.scope)
        .await
        .expect_err("missing session must be rejected");
    assert!(matches!(
        missing,
        crate::ServiceError::NotFound { entity, id }
            if entity == "protected_runtime_resource" && id == "unavailable"
    ));

    // The CLI resolver must not fall through to a native session merely
    // because its identity and canonical scope happen to match.
    sqlx::query(
        "UPDATE agent_session
         SET backend_kind = 'native', runtime_session_id = ?
         WHERE id = ?",
    )
    .bind("native-runtime")
    .bind(&fixture.session_id)
    .execute(fixture.db.pool())
    .await
    .expect("native session mutation");
    let native_profile = db::AgentProfileRepo::create_profile(
        &*fixture.db,
        db::CreateAgentProfile {
            id: new_uuid_v4(),
            identity_id: fixture.identity_id.clone(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("openai".to_owned()),
            model: Some("test-model".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("new immutable native profile");
    sqlx::query("UPDATE agent_session SET profile_id = ? WHERE id = ?")
        .bind(&native_profile.id)
        .bind(&fixture.session_id)
        .execute(fixture.db.pool())
        .await
        .expect("native session profile");
    let native = fixture
        .service
        .cli_chat_tools(&fixture.session_id, &fixture.identity_id, &fixture.scope)
        .await
        .expect_err("native session must be rejected by the CLI resolver");
    assert!(matches!(
        native,
        crate::ServiceError::NotFound { entity, id }
            if entity == "protected_runtime_resource" && id == "unavailable"
    ));
}

#[tokio::test]
async fn cli_chat_tools_intersects_tightened_permissions_and_rejects_revocation() {
    let fixture = fixture().await;
    sqlx::query(
        "UPDATE project_agent_binding
         SET permission_ceiling_json = ?
         WHERE project_id = ? AND state = 'active'",
    )
    .bind(json!({"permissions": ["read_agent_chat", "read_memory"]}).to_string())
    .bind(&fixture.project_id)
    .execute(fixture.db.pool())
    .await
    .expect("binding ceiling tightens");

    let narrowed = fixture
        .service
        .cli_chat_tools(&fixture.session_id, &fixture.identity_id, &fixture.scope)
        .await
        .expect("narrowed read-only binding still composes");
    let names = narrowed.tool_names();
    assert!(names.contains(&FORGE_PROJECT_ORCHESTRATION_READ_TOOL.to_owned()));
    assert!(
        !names.contains(&FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL.to_owned()),
        "adoption proposal must be denied after the persisted ceiling drops propose_project"
    );

    sqlx::query(
        "UPDATE project_agent_binding
         SET state = 'revoked', updated_at = ?
         WHERE project_id = ? AND state = 'active'",
    )
    .bind(now_rfc3339())
    .bind(&fixture.project_id)
    .execute(fixture.db.pool())
    .await
    .expect("binding revokes");
    assert!(fixture
        .service
        .cli_chat_tools(&fixture.session_id, &fixture.identity_id, &fixture.scope)
        .await
        .is_err());
}
