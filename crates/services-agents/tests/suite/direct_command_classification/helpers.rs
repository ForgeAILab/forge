//! Shared fixtures and helpers for direct command classification tests.

use super::*;

pub(super) const USER_ID: &str = "direct-classification-user";
pub(super) const AGENT_ID: &str = "direct-classification-project-agent";
pub(super) const PROFILE_ID: &str = "direct-classification-project-agent-profile";
pub(super) const PROJECT_ID: &str = "direct-classification-project";
pub(super) const TASK_PROJECT_ID: &str = "direct-classification-task-project";
pub(super) const REPO_ID: &str = "direct-classification-repo";
pub(super) const TASK_REPO_ID: &str = "direct-classification-task-repo";
pub(super) const CHARTER_ID: &str = "direct-classification-charter";
pub(super) const CHARTER_REVISION_ID: &str = "direct-classification-charter-revision";
pub(super) const TASK_CHARTER_ID: &str = "direct-classification-task-charter";
pub(super) const TASK_CHARTER_REVISION_ID: &str = "direct-classification-task-charter-revision";
pub(super) const MILESTONE_ID: &str = "direct-classification-milestone";
pub(super) const MILESTONE_REVISION_ID: &str = "direct-classification-milestone-revision";
pub(super) const RUNTIME_SESSION_ID: &str = "direct-classification-runtime-session";
pub(super) const NOW: &str = "2026-08-21T00:00:00.000Z";

pub(super) struct Fixture {
    pub(super) db: Arc<SqliteDb>,
    pub(super) provider: CoordinationToolProvider,
    pub(super) project_scope: CanonicalScope,
    pub(super) task_project_scope: CanonicalScope,
}

pub(super) async fn database() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    Arc::new(SqliteDb::new(pool))
}

pub(super) async fn seed_identity(db: &SqliteDb, permissions: &str) {
    UserRepo::create_user(
        db,
        &User {
            id: USER_ID.to_owned(),
            email: "direct-classification@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: Some("Direct command classification user".to_owned()),
            is_admin: false,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("user");

    AgentRepo::create_identity_with_profile(
        db,
        CreateAgentIdentity {
            id: AGENT_ID.to_owned(),
            name: "Direct classification Project Agent".to_owned(),
            description: None,
            max_concurrent_tasks: 4,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some(USER_ID.to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: permissions.to_owned(),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
        CreateAgentProfile {
            id: PROFILE_ID.to_owned(),
            identity_id: AGENT_ID.to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("test-model".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: permissions.to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("identity/profile");
}

pub(super) async fn seed_project(db: &SqliteDb, project_id: &str, repo_id: &str) {
    ProjectRepo::create_with_agent_binding(
        db,
        CreateProject {
            id: project_id.to_owned(),
            name: project_id.to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(USER_ID.to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
        Some(AGENT_ID.to_owned()),
        Some(PROFILE_ID.to_owned()),
    )
    .await
    .expect("Project and active binding");

    // Keep the binding ceiling explicit in the fixture.  This makes a policy
    // failure in the provider a classification failure, not a fixture default
    // assumption.
    sqlx::query(
        "UPDATE project_agent_binding
         SET permission_ceiling_json = ?
         WHERE project_id = ? AND state = 'active'",
    )
    .bind(r#"{"allowed":["read_project","propose_project","propose_task","propose_commitment"]}"#)
    .bind(project_id)
    .execute(db.pool())
    .await
    .expect("binding permissions");

    RepoRepo::create(
        db,
        CreateRepo {
            id: repo_id.to_owned(),
            project_id: project_id.to_owned(),
            name: format!("{project_id}-repo"),
            remote_url: Some(format!("file:///tmp/{repo_id}")),
            local_path: None,
            default_branch: "main".to_owned(),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("repository");
    ProjectRepo::update_at_version(
        db,
        UpdateProject {
            id: project_id.to_owned(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.to_owned())),
            paused_at: None,
            updated_at: NOW.to_owned(),
        },
        ProjectRepo::get_by_id(db, project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("primary repository");
}

pub(super) async fn seed_charter(
    db: &SqliteDb,
    project_id: &str,
    charter_id: &str,
    charter_revision_id: &str,
) {
    sqlx::query(
        "INSERT INTO project_charter
         (id, account_id, project_id, project_mode, maturity, lifecycle,
          current_approved_revision_id, version, created_at, updated_at)
         VALUES (?, ?, ?, 'standard', 'mvp', 'attached', NULL, 1, ?, ?)",
    )
    .bind(charter_id)
    .bind(USER_ID)
    .bind(project_id)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("Charter");
    sqlx::query(
        "INSERT INTO project_charter_revision
         (id, charter_id, revision, base_revision, lifecycle, schema_version,
          render_version, content_json, rendered_view, change_summary,
          author_type, author_id, source_refs_json, content_digest,
          rendered_digest, created_at)
         VALUES (?, ?, 1, 0, 'approved', 'charter@1', 'render@1', '{}',
                 '# Direct Classification Charter', 'fixture', 'user', ?, '[]',
                 'charter-content', 'charter-rendered', ?)",
    )
    .bind(charter_revision_id)
    .bind(charter_id)
    .bind(USER_ID)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("Charter revision");
    sqlx::query("UPDATE project_charter SET current_approved_revision_id = ? WHERE id = ?")
        .bind(charter_revision_id)
        .bind(charter_id)
        .execute(db.pool())
        .await
        .expect("Charter approval pointer");
    sqlx::query(
        "UPDATE project
         SET charter_status = 'charter_backed', charter_setup_required = 0,
             current_charter_id = ?, current_charter_revision_id = ?,
             current_charter_version = 1
         WHERE id = ?",
    )
    .bind(charter_id)
    .bind(charter_revision_id)
    .bind(project_id)
    .execute(db.pool())
    .await
    .expect("Project Charter pointer");
}

pub(super) async fn seed_milestone(db: &SqliteDb) {
    sqlx::query(
        "INSERT INTO project_milestone
         (id, project_id, milestone_sequence, milestone_key, display_label,
          current_definition_revision_id, lifecycle, blocker_reason_json,
          stale_reason_json, reconciliation_reason_json, version, created_at,
          updated_at)
         VALUES (?, ?, 1, 'M001', 'Direct classification milestone', NULL,
                 'planned', '[]', '[]', '[]', 1, ?, ?)",
    )
    .bind(MILESTONE_ID)
    .bind(PROJECT_ID)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("milestone");
    sqlx::query(
        "INSERT INTO project_milestone_revision
         (id, milestone_id, revision, base_revision, base_revision_id,
          lifecycle, display_label, outcome, included_scope_json,
          excluded_scope_json, charter_revision_id, document_revisions_json,
          task_selection_json, dependencies_json, risks_json,
          acceptance_checks_json, evidence_requirements_json, known_issues_json,
          change_summary, schema_version, render_version, rendered_view,
          content_digest, rendered_digest, author_type, author_id,
          source_refs_json, created_at)
         VALUES (?, ?, 1, 0, NULL, 'approved',
                 'Direct classification milestone', 'Classify commands',
                 '[]', '[]', ?, '[]', '[]', '[]', '[]', '[]', '[]', '[]',
                 'fixture', 'milestone@1', 'milestone-render@1', '# Milestone',
                 'milestone-content', 'milestone-rendered', 'user', ?, '[]', ?)",
    )
    .bind(MILESTONE_REVISION_ID)
    .bind(MILESTONE_ID)
    .bind(CHARTER_REVISION_ID)
    .bind(USER_ID)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("milestone revision");
    sqlx::query("UPDATE project_milestone SET current_definition_revision_id = ? WHERE id = ?")
        .bind(MILESTONE_REVISION_ID)
        .bind(MILESTONE_ID)
        .execute(db.pool())
        .await
        .expect("milestone pointer");
}

pub(super) async fn fixture() -> Fixture {
    fixture_with_permissions(
        r#"{"permissions":["read_project","propose_project","propose_task","propose_commitment"]}"#,
    )
    .await
}

pub(super) async fn fixture_with_permissions(permissions: &str) -> Fixture {
    let db = database().await;
    seed_identity(&db, permissions).await;
    seed_project(&db, PROJECT_ID, REPO_ID).await;
    seed_project(&db, TASK_PROJECT_ID, TASK_REPO_ID).await;
    seed_charter(&db, PROJECT_ID, CHARTER_ID, CHARTER_REVISION_ID).await;
    seed_charter(
        &db,
        TASK_PROJECT_ID,
        TASK_CHARTER_ID,
        TASK_CHARTER_REVISION_ID,
    )
    .await;
    seed_milestone(&db).await;
    let provider = CoordinationToolProvider::new(Arc::clone(&db));
    provider.set_task_service(Arc::new(TaskService::new_for_test(
        Arc::clone(&db),
        Arc::new(EventBus::new(32)),
    )));
    Fixture {
        db,
        provider,
        project_scope: CanonicalScope {
            scope_type: CanonicalScopeType::Project,
            scope_id: PROJECT_ID.to_owned(),
            workspace_access: WorkspaceAccess::Deny,
        },
        task_project_scope: CanonicalScope {
            scope_type: CanonicalScopeType::Project,
            scope_id: TASK_PROJECT_ID.to_owned(),
            workspace_access: WorkspaceAccess::Deny,
        },
    }
}

pub(super) fn research_content() -> Value {
    json!({
        "question": "Which command boundary is authoritative?",
        "decision_informed": "Whether direct commands need an approval queue.",
        "scope": "The Project-local coordination boundary.",
        "stopping_condition": "The durable receipt and event are present.",
        "sources": [],
        "findings": [],
        "evidence": [],
        "inferences": [],
        "alternatives": [],
        "recommendation": null,
        "uncertainty": [],
        "unresolved_questions": [],
        "affected_artifact_ids": [],
        "affected_decision_ids": []
    })
}

pub(super) fn document_arguments(key: &str) -> Value {
    json!({
        "operation": PROJECT_DOCUMENT_OPERATION,
        "payload": {
            "action": "draft_revision",
            "document_id": "direct-classification-document",
            "kind": "research",
            "title": "Direct command research",
            "expected_document_version": 1,
            "base_revision_id": null,
            "content": research_content()
        },
        "dedupe_key": key,
        "correlation_id": format!("correlation-{key}")
    })
}

pub(super) fn task_arguments(key: &str) -> Value {
    json!({
        "operation": "task.propose",
        "payload": {
            "title": "Direct planning task",
            "description": "A safe Project coordination task.",
            "task_type": "planning_task",
            "priority": 3,
            "review_requirement_ids": [],
            "merge_config": null,
            "role_assignments": null,
            "governance": null
        },
        "dedupe_key": key,
        "correlation_id": format!("correlation-{key}")
    })
}

pub(super) fn approval_required_release_arguments(key: &str) -> Value {
    json!({
        "operation": "project.release.request",
        "payload": {
            "action": "propose_candidate",
            "milestone_id": MILESTONE_ID,
            "milestone_version": 1,
            "readiness_snapshot_id": "direct-classification-readiness",
            "readiness_digest": "direct-classification-readiness-digest"
        },
        "dedupe_key": key,
        "correlation_id": format!("correlation-{key}")
    })
}

pub(super) fn milestone_content() -> Value {
    json!({
        "name": "Direct classification milestone",
        "outcome": "The structured outcome boundary is exercised.",
        "included_scope": [],
        "excluded_scope": [],
        "charter_revision": null,
        "document_revisions": [],
        "task_ids": [],
        "dependencies": [],
        "risks": [],
        "acceptance_checks": [],
        "evidence_requirements": [],
        "known_issues": [],
        "target_date": null
    })
}

pub(super) fn stale_milestone_arguments(key: &str, expected_version: i64) -> Value {
    json!({
        "operation": PROJECT_MILESTONE_OPERATION,
        "payload": {
            "action": "revise",
            "milestone_id": MILESTONE_ID,
            "expected_milestone_version": expected_version,
            "content": milestone_content()
        },
        "dedupe_key": key,
        "correlation_id": format!("correlation-{key}")
    })
}

pub(super) fn assert_success_envelope(
    value: &Value,
    operation: &str,
    scope_type: &str,
    scope_id: &str,
    correlation_id: &str,
) {
    assert_eq!(value["code"], "ok", "outcome code: {value}");
    assert_eq!(value["status"], "succeeded", "outcome status: {value}");
    assert_eq!(value["operation"], operation, "outcome operation: {value}");
    assert_eq!(value["scope"]["scope_type"], scope_type);
    assert_eq!(value["scope"]["scope_id"], scope_id);
    if correlation_id.is_empty() {
        assert!(
            value["correlation_id"]
                .as_str()
                .is_some_and(|value| !value.is_empty()),
            "server-generated correlation id: {value}"
        );
    } else {
        assert_eq!(value["correlation_id"], correlation_id);
    }
    assert!(value["safe_message"].as_str().is_some());
}

pub(super) fn structured_error(error: AgentHostError) -> api_types::OrchestrationOutcome {
    match error {
        AgentHostError::StructuredOutcome(outcome) => *outcome,
        other => panic!("expected a structured Forge outcome, got {other:?}"),
    }
}

pub(super) async fn count(db: &SqliteDb, sql: &str) -> i64 {
    sqlx::query_scalar(sql)
        .fetch_one(db.pool())
        .await
        .expect("count")
}

pub(super) fn result_field<'a>(value: &'a Value, field: &str) -> Option<&'a Value> {
    value
        .get(field)
        .or_else(|| {
            value
                .get("domain_result")
                .and_then(|result| result.get(field))
        })
        .or_else(|| value.get("result").and_then(|result| result.get(field)))
        .or_else(|| {
            value
                .get("result")
                .and_then(|result| result.get("domain_result"))
                .and_then(|result| result.get(field))
        })
}

// Exercise the actual host wrapper, provider and policy in one model turn.
pub(super) fn native_tool(
    fixture: &Fixture,
    scope: CanonicalScope,
    project_chat: bool,
    name: &str,
) -> Arc<dyn agent_runtime::core::prelude::Tool> {
    let permissions = [
        "read_account",
        "read_agent_chat",
        "read_project",
        "propose_project",
        "propose_task",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    forge_agent_host::ScopeToolComposition::for_scope_with_permissions_and_project_chat(
        AGENT_ID,
        scope,
        None,
        None,
        &permissions,
        project_chat,
        Some(Arc::new(fixture.provider.clone())),
    )
    .unwrap()
    .tools()
    .into_iter()
    .find(|tool| tool.spec().name == name)
    .unwrap()
}

pub(super) async fn invoke_native(
    tool: &dyn agent_runtime::core::prelude::Tool,
    arguments: Value,
    turn: &str,
    call: &str,
) -> agent_runtime::core::prelude::ToolOutcome {
    use agent_runtime::core::{
        cancel::Cancellation, clock::*, ids::*, prelude::*, workspace::DenyAllWorkspace,
    };
    let preparation = PreparationContext {
        session: SessionId::new(RUNTIME_SESSION_ID),
        turn: Some(TurnId::new(turn)),
        call_id: ToolCallId::new(call),
        request: RequestId::new(call),
        workspace: Arc::new(DenyAllWorkspace),
        clock: Arc::new(SystemClock),
        cancel: Cancellation::new(),
        deadline: Deadline::never(),
    };
    let invocation = InvocationContext {
        session: preparation.session.clone(),
        turn: preparation.turn.clone(),
        call_id: preparation.call_id.clone(),
        request: preparation.request.clone(),
        workspace: preparation.workspace.clone(),
        clock: preparation.clock.clone(),
        cancel: preparation.cancel.clone(),
        deadline: preparation.deadline,
        output_limit: 4096,
    };
    tool.invoke(
        tool.prepare(arguments, &preparation).await.unwrap(),
        &invocation,
    )
    .await
    .unwrap()
}

pub(super) async fn project_chat_session(fixture: &Fixture) -> (CanonicalScope, String) {
    let chat_id: String = sqlx::query_scalar("SELECT id FROM agent_chat WHERE project_id = ?")
        .bind(PROJECT_ID)
        .fetch_one(fixture.db.pool())
        .await
        .unwrap();
    let context_id = db::new_uuid_v4();
    db::AgentContextScopeRepo::create_context_scope(
        &*fixture.db,
        db::CreateAgentContextScope {
            id: context_id.clone(),
            identity_id: AGENT_ID.to_owned(),
            scope_type: "agent_chat".to_owned(),
            scope_id: chat_id.clone(),
            project_id: Some(PROJECT_ID.to_owned()),
            task_id: None,
            task_role: None,
            workspace_access: "deny".to_owned(),
            authority_json: "{}".to_owned(),
            workspace_path: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .unwrap();
    let session_id = db::new_uuid_v4();
    db::AgentSessionRepo::create_agent_session(
        &*fixture.db,
        db::CreateAgentSession {
            id: session_id.clone(),
            identity_id: AGENT_ID.to_owned(),
            profile_id: PROFILE_ID.to_owned(),
            context_scope_id: context_id,
            backend_kind: "native".to_owned(),
            runtime_session_id: Some(RUNTIME_SESSION_ID.to_owned()),
            status: "ready".to_owned(),
            capabilities_json: "{}".to_owned(),
            connection_status: "healthy".to_owned(),
            predecessor_session_id: None,
            last_activity_at: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .unwrap();
    (
        CanonicalScope {
            scope_type: CanonicalScopeType::AgentChat,
            scope_id: chat_id,
            workspace_access: WorkspaceAccess::Deny,
        },
        session_id,
    )
}

pub(super) async fn recovery_fixture() -> (Fixture, tempfile::TempDir, String, String, String) {
    let fixture = fixture().await;
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    std::fs::create_dir(&repo).unwrap();
    for args in [
        vec!["init", "-q", "-b", "main"],
        vec![
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.test",
            "commit",
            "-q",
            "--allow-empty",
            "-m",
            "Initial",
        ],
    ] {
        let output = std::process::Command::new("git")
            .current_dir(&repo)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    sqlx::query("UPDATE repo SET local_path = ?, remote_url = NULL WHERE id = ?")
        .bind(repo.to_string_lossy().as_ref())
        .bind(REPO_ID)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let worker = db::new_uuid_v4();
    let profile = db::new_uuid_v4();
    AgentRepo::create_identity_with_profile(
        &*fixture.db,
        CreateAgentIdentity {
            id: worker.clone(),
            name: "Target worker".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some(USER_ID.to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
        CreateAgentProfile {
            id: profile,
            identity_id: worker.clone(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: None,
            model: Some("test-model".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .unwrap();
    let mut tasks = Vec::new();
    for label in ["Task A", "Task B"] {
        let task = db::new_uuid_v4();
        sqlx::query("INSERT INTO task (id, project_id, title, status, task_type, blocked_json, created_at, updated_at) VALUES (?, ?, ?, 'planning', 'planning_task', '{\"reason\":\"execution stopped\",\"kind\":\"executor_failed\"}', ?, ?)")
            .bind(&task).bind(PROJECT_ID).bind(label).bind(NOW).bind(NOW).execute(fixture.db.pool()).await.unwrap();
        sqlx::query("INSERT INTO project_task_governance (task_id, project_id, charter_revision_id, capability_class, runnable, created_at, updated_at) VALUES (?, ?, ?, 'repository_read', 1, ?, ?)")
            .bind(&task).bind(PROJECT_ID).bind(CHARTER_REVISION_ID).bind(NOW).bind(NOW).execute(fixture.db.pool()).await.unwrap();
        sqlx::query("INSERT INTO task_role_assignment (id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at) VALUES (?, ?, 'planner', 'agent', ?, ?, ?)")
            .bind(db::new_uuid_v4()).bind(&task).bind(&worker).bind(NOW).bind(NOW).execute(fixture.db.pool()).await.unwrap();
        tasks.push(task);
    }
    fixture.provider.set_task_service(Arc::new(
        TaskService::new_for_test(fixture.db.clone(), Arc::new(EventBus::new(32)))
            .with_workspace_root(temp.path().join("workspaces")),
    ));
    (fixture, temp, worker, tasks.remove(0), tasks.remove(0))
}

pub(super) async fn recovery_arguments(
    fixture: &Fixture,
    task: &str,
    action: &str,
    key: &str,
) -> Value {
    let version = db::TaskRepo::get_by_id(&*fixture.db, task, false)
        .await
        .unwrap()
        .unwrap()
        .version;
    json!({"operation":"task.action", "payload":{"task_id":task,"action":{"verb":action,"reason":"Repair or cancel this interrupted work"},"version":version},
        "dedupe_key":key,"correlation_id":key})
}
