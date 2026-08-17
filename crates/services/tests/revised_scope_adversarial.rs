use std::sync::Arc;

use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentRepo, AgentStatus,
    CreateAgentIdentity, CreateAgentProfile, CreateProject, CreateProjectAgentBinding,
    CreateProjectMember, CreateTask, MemoryItem, MemoryRepository, ProjectAgentBindingRepo,
    ProjectMemberRepo, ProjectRepo, SqliteDb, TaskRepo,
};
use forge_agent_host::{CanonicalScope, CanonicalScopeType, ForgeToolProvider, WorkspaceAccess};
use serde_json::json;
use services::{
    AgentChatService, CoordinationToolProvider, SendAgentChatMessageInput, SetMainAgentBindingInput,
};

async fn database() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    Arc::new(SqliteDb::new(pool))
}

async fn project(db: &SqliteDb, id: &str) {
    let now = now_rfc3339();
    sqlx::query(
        "INSERT OR IGNORE INTO user (id, email, password_hash, display_name, created_at, updated_at)
         VALUES (?, ?, 'test', NULL, ?, ?)",
    )
    .bind("user-1")
    .bind("user-1@example.test")
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("test user creates");
    ProjectRepo::create(
        db,
        CreateProject {
            id: id.to_owned(),
            name: id.to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some("user-1".to_owned()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("project creates");
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
                   'forge.project-charter-render/v1', '{}', '# Project',
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

async fn main_identity(db: &Arc<SqliteDb>, identity_id: &str) -> String {
    let now = now_rfc3339();
    let profile_id = new_uuid_v4();
    let permissions = json!({"permissions": [
        "read_account", "read_agent_chat", "read_memory",
        "propose_discovery", "propose_project", "propose_handoff",
        "propose_message", "propose_commitment", "propose_memory", "propose_session"
    ]})
    .to_string();
    AgentRepo::create_identity_with_profile(
        db.as_ref(),
        CreateAgentIdentity {
            id: identity_id.to_owned(),
            name: identity_id.to_owned(),
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
            account_permission_ceiling: permissions.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: profile_id.clone(),
            identity_id: identity_id.to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("test".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: permissions,
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("Main identity creates");
    let chat_service = AgentChatService::new(Arc::clone(db));
    let chat = chat_service
        .ensure_main_chat("user-1")
        .await
        .expect("Main Chat creates");
    chat_service
        .set_main_binding(SetMainAgentBindingInput {
            actor_user_id: "user-1".to_owned(),
            account_id: "user-1".to_owned(),
            identity_id: identity_id.to_owned(),
            autonomy_policy_json: "{}".to_owned(),
            tool_policy_revision: "test".to_owned(),
            expected_version: None,
            replacement_reason: None,
        })
        .await
        .expect("Main binding creates");
    chat.id
}

async fn identity_with_project_permission(
    db: &SqliteDb,
    identity_id: &str,
    project_id: &str,
    bind_as_project_agent: bool,
) {
    let now = now_rfc3339();
    AgentRepo::create_identity_with_profile(
        db,
        CreateAgentIdentity {
            id: identity_id.to_owned(),
            name: identity_id.to_owned(),
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
            account_permission_ceiling: json!({"permissions":["read_project","propose_task"]})
                .to_string(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: new_uuid_v4(),
            identity_id: identity_id.to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("test".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: json!({"allowed":["read_project","propose_task"]}).to_string(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("identity creates");
    if !bind_as_project_agent {
        return;
    }
    let agent = AgentRepo::get_by_id(db, identity_id)
        .await
        .expect("identity lookup")
        .expect("identity exists");
    let setup = ProjectAgentBindingRepo::get_active_project_binding(db, project_id)
        .await
        .expect("binding lookup")
        .expect("setup binding exists");
    ProjectAgentBindingRepo::replace_project_binding(
        db,
        db::ReplaceProjectAgentBinding {
            project_id: project_id.to_owned(),
            expected_version: setup.version,
            replacement: CreateProjectAgentBinding {
                id: new_uuid_v4(),
                project_id: project_id.to_owned(),
                identity_id: Some(agent.id),
                profile_id: Some(agent.profile_id),
                state: "active".to_owned(),
                autonomy_policy_json: "{}".to_owned(),
                permission_ceiling_json: json!({"permissions":["propose_task"]}).to_string(),
                subscriptions_json: "[]".to_owned(),
                wake_budget: 1,
                created_at: now.clone(),
                updated_at: now,
            },
            replacement_reason: Some("scope test binding".to_owned()),
        },
    )
    .await
    .expect("binding creates");
}

fn task(id: &str, project_id: &str, title: &str) -> CreateTask {
    let now = now_rfc3339();
    CreateTask {
        id: id.to_owned(),
        project_id: project_id.to_owned(),
        repo_id: None,
        parent_task_id: None,
        assignee_type: None,
        assignee_id: None,
        title: title.to_owned(),
        description: None,
        task_type: "task".to_owned(),
        status: "backlog".to_owned(),
        is_automation: false,
        priority: 0,
        subtask_order: None,
        task_state_config: None,
        merge_config: None,
        plan: None,
        created_at: now.clone(),
        updated_at: now,
    }
}

fn memory_item(project_id: &str, body: &str, visibility: &str, sensitivity: &str) -> MemoryItem {
    let now = now_rfc3339();
    MemoryItem {
        row_id: 0,
        id: new_uuid_v4(),
        project_id: Some(project_id.to_owned()),
        task_id: None,
        execution_id: None,
        scope_type: "project".to_owned(),
        scope_id: project_id.to_owned(),
        visibility: visibility.to_owned(),
        owner_identity_id: None,
        authority: "observation".to_owned(),
        sensitivity: sensitivity.to_owned(),
        retention_priority: 10,
        provenance_json: "{}".to_owned(),
        publication_source_id: None,
        supersedes_id: None,
        valid_from: Some(now.clone()),
        valid_until: None,
        source_event_id: None,
        source_scope_type: Some("project".to_owned()),
        source_scope_id: Some(project_id.to_owned()),
        source_revision: Some("1".to_owned()),
        source_type: "comment".to_owned(),
        kind: "observation".to_owned(),
        title: "scope test".to_owned(),
        summary: None,
        body: body.to_owned(),
        metadata_json: "{}".to_owned(),
        confidence: Some("confirmed".to_owned()),
        quality_score: Some(1),
        created_by_type: Some("test".to_owned()),
        created_by_id: None,
        created_at: now,
    }
}

#[tokio::test]
async fn main_provider_cannot_submit_task_mutation() {
    let db = database().await;
    let provider = CoordinationToolProvider::new(Arc::clone(&db));
    let scope = CanonicalScope {
        scope_type: CanonicalScopeType::Account,
        scope_id: "user-1".to_owned(),
        workspace_access: WorkspaceAccess::Deny,
    };
    let result = provider
        .propose(
            "main-identity",
            &scope,
            "task.propose",
            json!({
                "payload": {"title":"forged task", "project_id":"project-b"},
                "dedupe_key":"main-task-denial",
                "correlation_id":"main-task-denial-correlation"
            }),
        )
        .await;
    assert!(
        result.is_err(),
        "Account/Main scope must reject task proposals"
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task")
        .fetch_one(db.pool())
        .await
        .expect("task count");
    assert_eq!(count, 0);
}

#[tokio::test]
async fn main_provider_global_catalog_operations_are_bounded_and_live() {
    let db = database().await;
    project(&db, "portfolio-project").await;
    let chat_id = main_identity(&db, "main-global-agent").await;
    let provider = CoordinationToolProvider::new(Arc::clone(&db));
    let scope = CanonicalScope {
        scope_type: CanonicalScopeType::AgentChat,
        scope_id: chat_id,
        workspace_access: WorkspaceAccess::Deny,
    };

    let portfolio = provider
        .read("main-global-agent", &scope, "portfolio.read", json!({}))
        .await
        .expect("Main portfolio projection is implemented");
    let projects = portfolio["items"].as_array().expect("portfolio items");
    assert_eq!(projects.len(), 1);
    assert_eq!(projects[0]["id"], "portfolio-project");
    assert!(!portfolio.to_string().contains("settings"));

    let summary = provider
        .read(
            "main-global-agent",
            &scope,
            "project.summary",
            json!({"project_id":"portfolio-project"}),
        )
        .await
        .expect("Main project summary is implemented");
    assert_eq!(summary["id"], "portfolio-project");

    let search = provider
        .propose(
            "main-global-agent",
            &scope,
            "web.search",
            json!({
                "payload": {"query":"bounded discovery query", "limit": 5},
                "dedupe_key":"main-search-1",
                "correlation_id":"main-search-correlation"
            }),
        )
        .await;
    assert!(search.is_err(), "web search must not become an AgentAction");

    let forged = provider
        .propose(
            "main-global-agent",
            &scope,
            "project.lifecycle",
            json!({
                "payload": {"action":"pause", "project_id":"not-owned"},
                "dedupe_key":"main-project-forged",
                "correlation_id":"main-project-forged-correlation"
            }),
        )
        .await;
    assert!(forged.is_err(), "Main cannot target an unowned Project");
}

#[tokio::test]
async fn project_reads_are_bound_to_scope_not_model_ids_or_text() {
    let db = database().await;
    project(&db, "project-a").await;
    project(&db, "project-b").await;
    TaskRepo::create(db.as_ref(), task("task-a", "project-a", "A work"))
        .await
        .expect("task A");
    TaskRepo::create(db.as_ref(), task("task-b", "project-b", "B work"))
        .await
        .expect("task B");

    let allowed = memory_item("project-a", "needle project-b text", "project", "internal");
    let other = memory_item(
        "project-b",
        "needle project-b private",
        "project",
        "internal",
    );
    let secret = memory_item("project-a", "needle secret", "private", "secret");
    for item in [&allowed, &other, &secret] {
        MemoryRepository::insert_memory_item(db.as_ref(), item)
            .await
            .expect("memory inserts");
    }

    let provider = CoordinationToolProvider::new(Arc::clone(&db));
    let scope = CanonicalScope {
        scope_type: CanonicalScopeType::Project,
        scope_id: "project-a".to_owned(),
        workspace_access: WorkspaceAccess::Deny,
    };
    let work = provider
        .read(
            "project-agent-a",
            &scope,
            "work.read",
            json!({"project_id":"project-b", "limit":50}),
        )
        .await
        .expect("scoped work read");
    assert_eq!(work["items"].as_array().unwrap().len(), 1);
    assert_eq!(work["items"][0]["id"], "task-a");

    let memories = provider
        .read(
            "project-agent-a",
            &scope,
            "memory.read",
            json!({"query":"needle project-b", "project_id":"project-b", "limit":50}),
        )
        .await
        .expect("scoped memory read");
    let ids = memories["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|item| item["id"].as_str())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![allowed.id.as_str()]);
    assert!(!ids.iter().any(|id| *id == other.id));
    assert!(!ids.iter().any(|id| *id == secret.id));
}

#[tokio::test]
async fn project_proposal_target_is_derived_from_scope() {
    let db = database().await;
    project(&db, "project-a").await;
    project(&db, "project-b").await;
    attach_approved_charter(&db, "project-a").await;
    identity_with_project_permission(&db, "project-agent-a", "project-a", true).await;
    let provider = CoordinationToolProvider::new(Arc::clone(&db));
    let scope = CanonicalScope {
        scope_type: CanonicalScopeType::Project,
        scope_id: "project-a".to_owned(),
        workspace_access: WorkspaceAccess::Deny,
    };
    let action = provider
        .propose(
            "project-agent-a",
            &scope,
            "task.propose",
            json!({
                "payload": {"title":"bounded"},
                "dedupe_key":"scope-target-1",
                "correlation_id":"scope-target-correlation"
            }),
        )
        .await
        .expect("proposal is audited");
    assert_eq!(action["scope_type"], "project");
    assert_eq!(action["scope_id"], "project-a");
    assert_eq!(action["target_type"], "project");
    assert_eq!(action["target_id"], "project-a");
    let task_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task")
        .fetch_one(db.pool())
        .await
        .expect("task count");
    assert_eq!(
        task_count, 0,
        "a proposal envelope is not an authoritative Task mutation"
    );
}

#[tokio::test]
async fn project_chat_never_infers_worker_as_binding() {
    let db = database().await;
    project(&db, "project-worker-primary").await;
    identity_with_project_permission(&db, "worker-identity", "project-worker-primary", false).await;
    ProjectMemberRepo::add_member(
        db.as_ref(),
        CreateProjectMember {
            id: new_uuid_v4(),
            project_id: "project-worker-primary".to_owned(),
            user_id: "user-1".to_owned(),
            role: "owner".to_owned(),
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("project member creates");
    let binding =
        ProjectAgentBindingRepo::get_active_project_binding(db.as_ref(), "project-worker-primary")
            .await
            .expect("setup-required binding reads")
            .expect("project always has one singular binding");
    assert_eq!(binding.state, "agent_setup_required");
    assert_eq!(binding.identity_id, None);
    let chat = AgentChatService::new(Arc::clone(&db))
        .ensure_project_chat("project-worker-primary")
        .await
        .expect("Project Chat creates");
    let service = AgentChatService::new(Arc::clone(&db));
    let error = service
        .send_message(SendAgentChatMessageInput {
            actor_user_id: "user-1".to_owned(),
            chat_id: chat.id.clone(),
            content: "must not route to worker".to_owned(),
            dedupe_key: Some("worker-primary-denial".to_owned()),
        })
        .await
        .expect_err("a primary Worker must not infer a Project binding");
    assert!(error.to_string().contains("not ready"));
    let turns: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_chat_turn_job WHERE chat_id = ?")
            .bind(chat.id)
            .fetch_one(db.pool())
            .await
            .expect("turn count");
    assert_eq!(turns, 0, "denied routing must not admit a turn");
}
