use std::sync::Arc;

use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentRepo, AgentStatus,
    CreateAgentIdentity, CreateAgentProfile, CreateProject, ProjectAgentBindingRepo,
    ProjectMemberRepo, ProjectRepo, SqliteDb,
};
use services::embedded_agent_service::RequestedCanonicalScope;
use services::{
    AgentChatService, EmbeddedAgentService, SetMainAgentBindingInput, SetProjectAgentBindingInput,
};

async fn sqlite_db() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(SqliteDb::new(pool));
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
         VALUES ('user-1', 'user-1@example.test', 'test', NULL, ?, ?)",
    )
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("user creates");
    db
}

async fn identity(db: &SqliteDb) -> (String, String) {
    let identity_id = new_uuid_v4();
    let profile_id = new_uuid_v4();
    let now = now_rfc3339();
    AgentRepo::create_identity_with_profile(
        db,
        CreateAgentIdentity {
            id: identity_id.clone(),
            name: "embedded-agent".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some("user-1".to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: serde_json::json!({
                "permissions": [
                    "read_account", "read_project", "read_agent_chat", "read_memory",
                    "propose_task", "propose_message", "propose_commitment", "propose_memory",
                    "propose_session"
                ]
            })
            .to_string(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: profile_id.clone(),
            identity_id: identity_id.clone(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("test".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: serde_json::json!({
                "allowed": [
                    "read_account", "read_project", "read_agent_chat", "read_memory",
                    "propose_task", "propose_message", "propose_commitment", "propose_memory",
                    "propose_session"
                ]
            })
            .to_string(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("identity creates");
    (identity_id, profile_id)
}

async fn attach_approved_charter(db: &SqliteDb, project_id: &str) {
    let now = now_rfc3339();
    let charter_id = format!("{project_id}-charter");
    let revision_id = format!("{charter_id}-revision-1");
    sqlx::query(
        "INSERT INTO project_charter (
             id, account_id, project_id, project_mode, maturity, lifecycle,
             version, created_at, updated_at
         ) VALUES (?, 'user-1', ?, 'compact', 'prototype', 'attached', 1, ?, ?)",
    )
    .bind(&charter_id)
    .bind(project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("charter creates");
    sqlx::query(
        "INSERT INTO project_charter_revision (
             id, charter_id, revision, base_revision, lifecycle, schema_version,
             render_version, content_json, rendered_view, change_summary,
             author_type, author_id, source_refs_json, content_digest,
             rendered_digest, created_at
         ) VALUES (?, ?, 1, 0, 'approved', 'forge.project-charter/v1',
                   'forge.project-charter-render/v1', '{}', '# Project A',
                   'test fixture approval', 'user', 'user-1', '[]',
                   'charter-content-digest', 'charter-render-digest', ?)",
    )
    .bind(&revision_id)
    .bind(&charter_id)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("charter revision creates");
    sqlx::query(
        "UPDATE project_charter
         SET current_approved_revision_id = ?, current_draft_revision_id = ?, version = 2
         WHERE id = ?",
    )
    .bind(&revision_id)
    .bind(&revision_id)
    .bind(&charter_id)
    .execute(db.pool())
    .await
    .expect("charter approval attaches");
    sqlx::query(
        "UPDATE project
         SET current_charter_id = ?, current_charter_revision_id = ?,
             current_charter_version = 1, charter_status = 'charter_backed',
             charter_setup_required = 0, version = version + 1, updated_at = ?
         WHERE id = ?",
    )
    .bind(&charter_id)
    .bind(&revision_id)
    .bind(&now)
    .bind(project_id)
    .execute(db.pool())
    .await
    .expect("approved Charter attaches to Project");
}

#[tokio::test]
async fn main_chat_is_not_task_capable_even_with_broad_identity_policy() {
    let db = sqlite_db().await;
    let (identity_id, _profile_id) = identity(&db).await;
    let chats = AgentChatService::new(Arc::clone(&db));
    chats
        .set_main_binding(SetMainAgentBindingInput {
            actor_user_id: "user-1".to_owned(),
            account_id: "user-1".to_owned(),
            identity_id: identity_id.clone(),
            autonomy_policy_json: "{}".to_owned(),
            tool_policy_revision: "test".to_owned(),
            expected_version: None,
            replacement_reason: None,
        })
        .await
        .expect("Main binding");
    let chat = db::AgentChatRepo::get_main_chat(&*db, "user-1")
        .await
        .expect("Main chat lookup")
        .expect("Main chat");
    let service = EmbeddedAgentService::new(Arc::clone(&db), b"test-protected-key");
    let permissions = service
        .effective_permissions(
            "user-1",
            &identity_id,
            &RequestedCanonicalScope::AgentChat {
                chat_id: chat.id.clone(),
            },
        )
        .await
        .expect("Main permissions");
    assert!(!permissions.allowed.contains("propose_task"));
    assert!(permissions.denied.contains("propose_task"));
}

#[tokio::test]
async fn project_chat_gets_task_management_only_after_charter_setup_for_its_owning_project() {
    let db = sqlite_db().await;
    let (identity_id, _profile_id) = identity(&db).await;
    let now = now_rfc3339();
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: "project-a".to_owned(),
            name: "Project A".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some("user-1".to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("Project A");
    ProjectMemberRepo::add_member(
        &*db,
        db::CreateProjectMember {
            id: new_uuid_v4(),
            project_id: "project-a".to_owned(),
            user_id: "user-1".to_owned(),
            role: "owner".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("Project member");
    let chats = AgentChatService::new(Arc::clone(&db));
    chats
        .set_project_binding(SetProjectAgentBindingInput {
            actor_user_id: "user-1".to_owned(),
            project_id: "project-a".to_owned(),
            identity_id: Some(identity_id.clone()),
            state: "active".to_owned(),
            autonomy_policy_json: "{}".to_owned(),
            permission_ceiling_json: serde_json::json!({
                "permissions": ["read_agent_chat", "read_memory", "propose_task"]
            })
            .to_string(),
            subscriptions_json: "[]".to_owned(),
            wake_budget: 1,
            expected_version: Some(
                ProjectAgentBindingRepo::get_active_project_binding(&*db, "project-a")
                    .await
                    .expect("binding lookup")
                    .expect("setup binding")
                    .version,
            ),
            replacement_reason: Some("test binding".to_owned()),
        })
        .await
        .expect("Project binding");
    let chat = db::AgentChatRepo::get_project_chat(&*db, "project-a")
        .await
        .expect("Project chat lookup")
        .expect("Project chat");
    let service = EmbeddedAgentService::new(Arc::clone(&db), b"test-protected-key");
    let setup_required = service
        .effective_permissions(
            "user-1",
            &identity_id,
            &RequestedCanonicalScope::AgentChat {
                chat_id: chat.id.clone(),
            },
        )
        .await
        .expect("Project permissions");
    assert!(!setup_required.allowed.contains("propose_task"));
    assert!(setup_required.denied.contains("propose_task"));

    attach_approved_charter(&db, "project-a").await;
    let own = service
        .effective_permissions(
            "user-1",
            &identity_id,
            &RequestedCanonicalScope::AgentChat {
                chat_id: chat.id.clone(),
            },
        )
        .await
        .expect("charter-backed Project permissions");
    assert!(own.allowed.contains("propose_task"));
    assert!(!own.allowed.contains("task_write"));
    assert!(!own.allowed.contains("propose_session"));

    let forged = service
        .effective_permissions(
            "user-1",
            &identity_id,
            &RequestedCanonicalScope::AgentChat {
                chat_id: "project-b".to_owned(),
            },
        )
        .await;
    assert!(
        forged.is_err(),
        "opaque or forged chat ids cannot grant authority"
    );
}
