//! Shared fixtures and helpers for native scope composition tests.

use super::*;
pub(super) const USER_ID: &str = "scope-composition-user";
pub(super) const AGENT_ID: &str = "scope-composition-agent";
pub(super) const PROFILE_ID: &str = "scope-composition-profile";
pub(super) const PROJECT_AGENT_CANDIDATE_ID: &str = "scope-composition-project-agent-candidate";
pub(super) const PROJECT_AGENT_CANDIDATE_PROFILE_ID: &str =
    "scope-composition-project-agent-candidate-profile";
pub(super) const PROJECT_ID: &str = "scope-composition-project";
pub(super) const REPO_ID: &str = "scope-composition-repo";
pub(super) const PROJECT_CHARTER_ID: &str = "scope-composition-project-charter";
pub(super) const PROJECT_CHARTER_REVISION_ID: &str = "scope-composition-project-charter-revision";
pub(super) const MAIN_CHAT_ID: &str = "scope-composition-main-chat";
pub(super) const MAIN_GENESIS_ID: &str = "scope-composition-genesis";
pub(super) const MAIN_CHARTER_ID: &str = "scope-composition-main-charter";
pub(super) const MAIN_REVISION_ID: &str = "scope-composition-main-revision";
pub(super) const NOW: &str = "2026-08-21T00:00:00.000Z";

pub(super) struct Fixture {
    pub(super) db: Arc<SqliteDb>,
    pub(super) provider: CoordinationToolProvider,
    pub(super) main_scope: CanonicalScope,
    pub(super) project_scope: CanonicalScope,
}

pub(super) async fn fixture(with_task_service: bool) -> Fixture {
    fixture_with_charter(with_task_service, true).await
}
pub(super) async fn fixture_with_charter(with_task_service: bool, adopted: bool) -> Fixture {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    let db = Arc::new(SqliteDb::new(pool));

    UserRepo::create_user(
        &*db,
        &User {
            id: USER_ID.to_owned(),
            email: "scope-composition@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: Some("Scope composition user".to_owned()),
            is_admin: false,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("user");

    AgentRepo::create_identity_with_profile(
        &*db,
        CreateAgentIdentity {
            id: AGENT_ID.to_owned(),
            name: "Scope composition Agent".to_owned(),
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
            account_permission_ceiling: broad_permission_json(),
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
            tool_policy_json: broad_permission_json(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("identity/profile");

    AgentRepo::create_identity_with_profile(
        &*db,
        CreateAgentIdentity {
            id: PROJECT_AGENT_CANDIDATE_ID.to_owned(),
            name: "Scope composition Project Agent candidate".to_owned(),
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
            account_permission_ceiling: broad_permission_json(),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
        CreateAgentProfile {
            id: PROJECT_AGENT_CANDIDATE_PROFILE_ID.to_owned(),
            identity_id: PROJECT_AGENT_CANDIDATE_ID.to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("test-model".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: broad_permission_json(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("Project Agent candidate identity/profile");

    sqlx::query(
        "UPDATE agent_chat SET id = ?, status = 'ready'
         WHERE account_id = ? AND kind = 'account_main'",
    )
    .bind(MAIN_CHAT_ID)
    .bind(USER_ID)
    .execute(db.pool())
    .await
    .expect("Main Chat");
    sqlx::query(
        "INSERT INTO account_main_agent_binding
         (id, account_id, identity_id, profile_id, state, autonomy_policy_json,
          tool_policy_revision, version, created_at, updated_at)
         VALUES ('scope-composition-main-binding', ?, ?, ?, 'active', '{}', 'test', 1, ?, ?)",
    )
    .bind(USER_ID)
    .bind(AGENT_ID)
    .bind(PROFILE_ID)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("Main binding");

    seed_main_genesis(&db).await;
    seed_project(&db).await;
    if adopted {
        seed_project_charter(&db).await;
    }

    let provider = CoordinationToolProvider::new(Arc::clone(&db));
    if with_task_service {
        provider.set_task_service(Arc::new(TaskService::new_for_test(
            Arc::clone(&db),
            Arc::new(EventBus::new(32)),
        )));
    }

    Fixture {
        db,
        provider,
        main_scope: CanonicalScope {
            scope_type: CanonicalScopeType::Account,
            scope_id: USER_ID.to_owned(),
            workspace_access: WorkspaceAccess::Deny,
        },
        project_scope: CanonicalScope {
            scope_type: CanonicalScopeType::Project,
            scope_id: PROJECT_ID.to_owned(),
            workspace_access: WorkspaceAccess::Deny,
        },
    }
}

pub(super) fn broad_permission_json() -> String {
    r#"{"permissions":["read_account","read_project","read_agent_chat","read_task","read_memory","propose_task","propose_project","propose_discovery","propose_message","propose_review","propose_commitment","propose_memory","propose_decision","propose_session"]}"#.to_owned()
}

pub(super) fn broad_permissions() -> BTreeSet<String> {
    [
        "read_account",
        "read_project",
        "read_agent_chat",
        "read_task",
        "read_memory",
        "propose_task",
        "propose_project",
        "propose_discovery",
        "propose_message",
        "propose_review",
        "propose_commitment",
        "propose_memory",
        "propose_decision",
        "propose_session",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

pub(super) async fn seed_main_genesis(db: &SqliteDb) {
    sqlx::query(
        "INSERT INTO product_genesis_session
         (id, account_id, main_chat_id, prompt_revision, prompt_body, maturity,
          lifecycle, source_message_ids_json, version, created_at, updated_at)
         VALUES (?, ?, ?, 'scope-prompt', 'Scope composition fixture', 'mvp',
                 'discovering', '[]', 1, ?, ?)",
    )
    .bind(MAIN_GENESIS_ID)
    .bind(USER_ID)
    .bind(MAIN_CHAT_ID)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("Genesis");

    sqlx::query(
        "INSERT INTO project_charter
         (id, account_id, genesis_session_id, project_mode, maturity, lifecycle,
          version, created_at, updated_at)
         VALUES (?, ?, ?, 'compact', 'mvp', 'ready_for_approval', 1, ?, ?)",
    )
    .bind(MAIN_CHARTER_ID)
    .bind(USER_ID)
    .bind(MAIN_GENESIS_ID)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("Main Charter");

    let content = charter_content("Main scope composition");
    sqlx::query(
        "INSERT INTO project_charter_revision
         (id, charter_id, revision, base_revision, base_revision_id, lifecycle,
          schema_version, render_version, content_json, rendered_view, change_summary,
          author_type, author_id, source_refs_json, content_digest, rendered_digest, created_at)
         VALUES (?, ?, 1, 0, NULL, 'proposed', 'charter-v1', 'render-v1', ?,
                 '# Main scope composition', 'fixture', 'agent', ?, '[]',
                 'main-content-1', 'main-render-1', ?)",
    )
    .bind(MAIN_REVISION_ID)
    .bind(MAIN_CHARTER_ID)
    .bind(content.to_string())
    .bind(AGENT_ID)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("Main Charter revision");
    sqlx::query(
        "UPDATE project_charter
         SET current_draft_revision_id = ? WHERE id = ?",
    )
    .bind(MAIN_REVISION_ID)
    .bind(MAIN_CHARTER_ID)
    .execute(db.pool())
    .await
    .expect("Main Charter pointer");
    sqlx::query(
        "UPDATE product_genesis_session
         SET charter_id = ?, charter_revision_id = ?, charter_version = 1
         WHERE id = ?",
    )
    .bind(MAIN_CHARTER_ID)
    .bind(MAIN_REVISION_ID)
    .bind(MAIN_GENESIS_ID)
    .execute(db.pool())
    .await
    .expect("Genesis Charter pointer");
}

pub(super) async fn seed_project(db: &SqliteDb) {
    ProjectRepo::create_with_agent_binding(
        db,
        CreateProject {
            id: PROJECT_ID.to_owned(),
            name: "Scope composition Project".to_owned(),
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
    .expect("Project and binding");
    sqlx::query(
        "UPDATE project_agent_binding
         SET permission_ceiling_json = ?
         WHERE project_id = ? AND state = 'active'",
    )
    .bind(broad_permission_json())
    .bind(PROJECT_ID)
    .execute(db.pool())
    .await
    .expect("Project policy");
    RepoRepo::create(
        db,
        CreateRepo {
            id: REPO_ID.to_owned(),
            project_id: PROJECT_ID.to_owned(),
            name: "Scope composition repository".to_owned(),
            remote_url: Some("file:///tmp/scope-composition-repo".to_owned()),
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
            id: PROJECT_ID.to_owned(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(REPO_ID.to_owned())),
            paused_at: None,
            updated_at: NOW.to_owned(),
        },
        ProjectRepo::get_by_id(db, PROJECT_ID)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("primary repository");
}

pub(super) async fn seed_project_charter(db: &SqliteDb) {
    sqlx::query(
        "INSERT INTO project_charter
         (id, account_id, project_id, project_mode, maturity, lifecycle,
          current_approved_revision_id, version, created_at, updated_at)
         VALUES (?, ?, ?, 'standard', 'mvp', 'attached', NULL, 1, ?, ?)",
    )
    .bind(PROJECT_CHARTER_ID)
    .bind(USER_ID)
    .bind(PROJECT_ID)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("Project Charter");
    sqlx::query(
        "INSERT INTO project_charter_revision
         (id, charter_id, revision, base_revision, base_revision_id, lifecycle,
          schema_version, render_version, content_json, rendered_view, change_summary,
          author_type, author_id, source_refs_json, content_digest, rendered_digest, created_at)
         VALUES (?, ?, 1, 0, NULL, 'approved', 'charter-v1', 'render-v1', ?,
                 '# Project scope composition', 'fixture', 'user', ?, '[]',
                 'project-content-1', 'project-render-1', ?)",
    )
    .bind(PROJECT_CHARTER_REVISION_ID)
    .bind(PROJECT_CHARTER_ID)
    .bind(charter_content("Project scope composition").to_string())
    .bind(USER_ID)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("Project Charter revision");
    sqlx::query("UPDATE project_charter SET current_approved_revision_id = ? WHERE id = ?")
        .bind(PROJECT_CHARTER_REVISION_ID)
        .bind(PROJECT_CHARTER_ID)
        .execute(db.pool())
        .await
        .expect("Project Charter revision pointer");
    sqlx::query(
        "UPDATE project
         SET charter_status = 'charter_backed', charter_setup_required = 0,
             current_charter_id = ?, current_charter_revision_id = ?,
             current_charter_version = 1
         WHERE id = ?",
    )
    .bind(PROJECT_CHARTER_ID)
    .bind(PROJECT_CHARTER_REVISION_ID)
    .bind(PROJECT_ID)
    .execute(db.pool())
    .await
    .expect("Project Charter pointer");
}

pub(super) fn charter_content(name: &str) -> Value {
    json!({
        "identity": {
            "working_name": name,
            "slug_proposal": "scope-composition",
            "one_line_vision": "Exercise the native orchestration boundary",
            "maturity": "mvp"
        },
        "problem_and_people": {
            "problem_or_opportunity": "Native operation outcomes need one typed contract.",
            "target_users": ["maintainers"]
        },
        "core_experience": {"primary_outcome": "Bounded structured outcomes"},
        "scope": {
            "must_have_outcomes": ["One composition boundary"],
            "explicit_non_goals": ["Transport-specific branching"]
        },
        "success": {"acceptance_statements": ["Every migrated operation is exercised"]},
        "constraints_and_risks": {},
        "knowledge_ledger": {"items": []}
    })
}

pub(super) fn main_draft_arguments(key: &str) -> Value {
    json!({
        "operation": MAIN_CHARTER_DRAFT_OPERATION,
        "payload": {
            "action": "save_revision",
            "genesis_session_id": MAIN_GENESIS_ID,
            "charter_id": MAIN_CHARTER_ID,
            "expected_charter_version": 1,
            "base_revision_id": MAIN_REVISION_ID,
            "project_mode": "compact",
            "maturity": "mvp",
            "content": charter_content("Main scope composition draft"),
            "provenance": {
                "author": {"kind": "agent", "id": AGENT_ID},
                "change_summary": "Exercise Main composition"
            }
        },
        "dedupe_key": key,
        "correlation_id": format!("correlation-{key}")
    })
}

pub(super) fn genesis_start_arguments(key: &str) -> Value {
    json!({
        "operation": MAIN_GENESIS_START_OPERATION,
        "payload": {
            "action": "start",
            "maturity": "mvp"
        },
        "dedupe_key": key,
        "correlation_id": format!("correlation-{key}")
    })
}

pub(super) fn document_arguments(key: &str, title: &str, document_id: &str) -> Value {
    json!({
        "operation": PROJECT_DOCUMENT_OPERATION,
        "payload": {
            "action": "draft_revision",
            "document_id": document_id,
            "kind": "research",
            "title": title,
            "expected_document_version": 1,
            "base_revision_id": null,
            "content": {
                "question": "Which boundary is authoritative?",
                "decision_informed": "Whether adapters should validate domain fields.",
                "scope": "The Project-native boundary.",
                "stopping_condition": "The service returns a typed outcome.",
                "sources": [], "findings": [], "evidence": [], "inferences": [],
                "alternatives": [], "recommendation": null, "uncertainty": [],
                "unresolved_questions": [], "affected_artifact_ids": [],
                "affected_decision_ids": []
            }
        },
        "dedupe_key": key,
        "correlation_id": format!("correlation-{key}")
    })
}

pub(super) fn task_arguments(key: &str) -> Value {
    json!({
        "operation": TASK_PROPOSE_OPERATION,
        "payload": {
            "title": "Scope composition task",
            "description": "A native Task proposal exercised through Project scope.",
            "task_type": "planning_task",
            "review_requirement_ids": [],
            "priority": 3,
            "merge_config": null,
            "role_assignments": null,
            "governance": null
        },
        "dedupe_key": key,
        "correlation_id": format!("correlation-{key}")
    })
}

pub(super) fn review_config_arguments(
    key: &str,
    expected_project_version: i64,
    ci_steps: &[&str],
) -> Value {
    json!({
        "operation": PROJECT_REVIEW_CONFIG_OPERATION,
        "payload": {
            "action": "set_ci_steps",
            "expected_project_version": expected_project_version,
            "ci_steps": ci_steps,
        },
        "dedupe_key": key,
        "correlation_id": format!("correlation-{key}")
    })
}

pub(super) fn adaptive_task_arguments(
    key: &str,
    source_task_id: &str,
    expected_task_version: i64,
    expected_board_revision: i64,
) -> Value {
    json!({
        "operation": TASK_ADAPTIVE_OPERATION,
        "payload": {
            "action": "split",
            "source_task_id": source_task_id,
            "expected_task_version": expected_task_version,
            "expected_board_revision": expected_board_revision,
            "rationale": "Exercise the composed adaptive Task command.",
            "items": [{
                "title": "Scope composition adaptive child",
                "description": "A bounded child created through the native composition."
            }]
        },
        "dedupe_key": key,
        "correlation_id": format!("correlation-{key}")
    })
}

pub(super) fn task_dependency_arguments(
    key: &str,
    action: &str,
    task_id: &str,
    depends_on: &str,
) -> Value {
    json!({
        "operation": TASK_DEPENDENCY_OPERATION,
        "payload": {
            "action": action,
            "task_id": task_id,
            "depends_on_task_id": depends_on,
            "rationale": "The graph is re-planned without changing either Task id."
        },
        "dedupe_key": key,
        "correlation_id": format!("correlation-{key}")
    })
}

pub(super) fn cancel_task_arguments(key: &str, task_id: &str, expected_task_version: i64) -> Value {
    json!({
        "operation": TASK_ACTION_OPERATION,
        "payload": {
            "action": {"verb":"cancel", "reason":"Cancel obsolete work"},
            "task_id": task_id,
            "version": expected_task_version
        },
        "dedupe_key": key,
        "correlation_id": format!("correlation-{key}")
    })
}

pub(super) fn release_arguments(key: &str) -> Value {
    json!({
        "operation": PROJECT_RELEASE_OPERATION,
        "payload": {
            "action": "propose_candidate",
            "milestone_id": "scope-composition-milestone",
            "milestone_version": 1,
            "readiness_snapshot_id": "scope-composition-readiness",
            "readiness_digest": "scope-composition-readiness-digest"
        },
        "dedupe_key": key,
        "correlation_id": format!("correlation-{key}")
    })
}

pub(super) fn adoption_arguments(key: &str) -> Value {
    json!({
        "operation": PROJECT_CHARTER_ADOPTION_OPERATION,
        "payload": {
            "action": "draft_revision",
            "expected_charter_version": 1,
            "project_mode": "standard",
            "maturity": "mvp",
            "content": charter_content("Setup adoption"),
            "provenance": {
                "author": {"kind": "agent", "id": AGENT_ID},
                "change_summary": "Exercise setup adoption"
            }
        },
        "dedupe_key": key,
        "correlation_id": format!("correlation-{key}")
    })
}

pub(super) fn project_proposal_arguments(operation: &str, action: &str, key: &str) -> Value {
    json!({
        "operation": operation,
        "payload": {"action": action},
        "dedupe_key": key,
        "correlation_id": format!("correlation-{key}")
    })
}

pub(super) fn preparation_context(call_id: &str) -> PreparationContext {
    PreparationContext {
        session: SessionId::new("scope-composition-session"),
        turn: None,
        call_id: ToolCallId::new(call_id),
        request: RequestId::new("scope-composition-request"),
        workspace: Arc::new(DenyAllWorkspace),
        clock: Arc::new(SystemClock),
        cancel: Cancellation::new(),
        deadline: Deadline::never(),
    }
}

pub(super) fn invocation_context(call_id: &str) -> InvocationContext {
    InvocationContext {
        session: SessionId::new("scope-composition-session"),
        turn: None,
        call_id: ToolCallId::new(call_id),
        request: RequestId::new("scope-composition-request"),
        workspace: Arc::new(DenyAllWorkspace),
        clock: Arc::new(SystemClock),
        cancel: Cancellation::new(),
        deadline: Deadline::never(),
        output_limit: 16_384,
    }
}

pub(super) async fn invoke_tool(
    composition: &ScopeToolComposition,
    tool_name: &str,
    arguments: Value,
    call_id: &str,
) -> Result<ToolOutcome, RuntimeError> {
    let tool = composition
        .tools()
        .into_iter()
        .find(|tool| tool.spec().name == tool_name)
        .unwrap_or_else(|| panic!("composed tool {tool_name} is missing"));
    let mut registry = agent_runtime::tool::ToolRegistry::new();
    registry.register(tool.clone())?;
    let registry = registry.seal();
    let arguments = tool.normalize_arguments(arguments)?;
    registry.validate_arguments(tool_name, &arguments)?;
    let prepared = tool
        .prepare(arguments, &preparation_context(call_id))
        .await?;
    tool.invoke(prepared, &invocation_context(call_id)).await
}

pub(super) fn assert_outcome_operation(outcome: &ToolOutcome, operation: &str) {
    assert_eq!(
        outcome.value["operation"], operation,
        "outcome: {}",
        outcome.value
    );
    assert!(outcome.value["correlation_id"].as_str().is_some());
    assert!(outcome.value["safe_message"].as_str().is_some());
}

pub(super) fn assert_structured_error(outcome: &ToolOutcome, operation: &str, code: &str) {
    assert!(
        outcome.is_error,
        "expected in-band error: {}",
        outcome.value
    );
    assert_outcome_operation(outcome, operation);
    assert_eq!(outcome.value["code"], code, "outcome: {}", outcome.value);
}

#[derive(Debug, Default)]
pub(super) struct RegistryInquiryRunner(pub(super) std::sync::Mutex<Vec<services::InquiryRequest>>);
#[async_trait::async_trait]
impl services::InquiryRunner for RegistryInquiryRunner {
    async fn dispatch(
        &self,
        request: services::InquiryRequest,
        _: tokio_util::sync::CancellationToken,
    ) -> services::Result<services::InquiryOutcome> {
        self.0.lock().unwrap().push(request);
        Ok(services::InquiryOutcome {
            inquiry_id: "registry-inquiry".into(),
            status: db::AgentInquiryStatus::Succeeded,
            findings: "Bounded findings".into(),
            findings_path: Some("inquiries/registry-inquiry/findings.md".into()),
            input_tokens: 11,
            output_tokens: 12,
            cache_read_tokens: 13,
            cache_write_tokens: 14,
            duration_ms: 15,
        })
    }
    async fn cancel_inquiry(&self, _: &str) -> bool {
        false
    }
}

pub(super) fn main_registered_input(id: &str) -> Value {
    match id {
        "genesis.project_agents.read" => json!({"genesis_session_id":MAIN_GENESIS_ID}),
        "charter.read" => {
            json!({"charter_id":MAIN_CHARTER_ID,"revision_id":MAIN_REVISION_ID,"genesis_session_id":MAIN_GENESIS_ID})
        }
        "charter.readiness" | "charter.approval_target" => {
            json!({"charter_id":MAIN_CHARTER_ID,"revision_id":MAIN_REVISION_ID,"content_digest":"main-content-1","render_digest":"main-render-1","expected_charter_version":1,"genesis_session_id":MAIN_GENESIS_ID})
        }
        "charter.diff" => {
            json!({"charter_id":MAIN_CHARTER_ID,"base_revision_id":MAIN_REVISION_ID,"candidate_revision_id":MAIN_REVISION_ID,"genesis_session_id":MAIN_GENESIS_ID})
        }
        "discovery.read" | "portfolio.read" => json!({"limit":1}),
        "inquiry.run" => {
            json!({"title":"  Question  ","question":"  Find the answer  ","context":"  Supporting material  "})
        }
        _ => panic!("unexpected Main operation {id}"),
    }
}

pub(super) async fn capture_base_selection(
    f: &Fixture,
) -> services::MainGenesisProjectAgentSelectResult {
    services::MainGenesisCommandService::new(f.db.clone())
        .select_project_agent(services::MainGenesisProjectAgentSelectCommandInput {
            principal: services::MainGenesisDraftPrincipal::MainAgent {
                identity_id: AGENT_ID.into(),
                scope: f.main_scope.clone(),
            },
            request: services::MainGenesisProjectAgentSelectRequest {
                genesis_session_id: Some(MAIN_GENESIS_ID.into()),
                expected_session_version: 1,
                project_agent_identity_id: PROJECT_AGENT_CANDIDATE_ID.into(),
            },
            idempotency_key: "pre-change-selection".into(),
            correlation_id: "pre-change-correlation".into(),
            causation_id: Some("pre-change-cause".into()),
            causation_depth: 1,
            policy_result: "allowed".into(),
            requested_permission: "propose_discovery".into(),
        })
        .await
        .unwrap()
}
pub(super) async fn capture_project_proposal_case(operation: &'static str) {
    use forge_agent_host::ForgeToolProvider;
    use sqlx::Row;
    let f = fixture(false).await;
    let project_version = ProjectRepo::get_by_id(&*f.db, PROJECT_ID)
        .await
        .unwrap()
        .unwrap()
        .version;
    if matches!(
        operation,
        PROJECT_EVIDENCE_OPERATION | PROJECT_VALIDATION_OPERATION | PROJECT_READINESS_OPERATION
    ) {
        sqlx::query("INSERT INTO project_milestone (id, project_id, milestone_sequence, milestone_key, lifecycle, created_at, updated_at) VALUES ('registry-milestone', ?, 1, 'M001', 'active', ?, ?)").bind(PROJECT_ID).bind(NOW).bind(NOW).execute(f.db.pool()).await.unwrap();
        sqlx::query("INSERT INTO project_milestone_revision (id, milestone_id, revision, lifecycle, outcome, display_label, schema_version, render_version, rendered_view, content_digest, rendered_digest, author_type, created_at) VALUES ('registry-definition','registry-milestone',1,'approved','Deliver the requirement','Delivery','milestone-v1','render-v1','Delivered','definition-content','definition-render','user',?)").bind(NOW).execute(f.db.pool()).await.unwrap();
        sqlx::query("UPDATE project_milestone SET current_definition_revision_id = 'registry-definition' WHERE id = 'registry-milestone'").execute(f.db.pool()).await.unwrap();
        sqlx::query("INSERT INTO project_milestone_check (id, project_id, milestone_id, definition_revision_id, check_key, description, source_kind, expected_result, created_at, updated_at) VALUES ('delivery',?,'registry-milestone','registry-definition','delivery','Observe delivery','task_validation','Delivered',?,?)").bind(PROJECT_ID).bind(NOW).bind(NOW).execute(f.db.pool()).await.unwrap();
    }
    let key = format!("registry-{operation}");
    let payload = match operation {
        PROJECT_REVIEW_CONFIG_OPERATION => {
            json!({"action":"set_ci_steps","expected_project_version":project_version,"ci_steps":["cargo test --lib"]})
        }
        PROJECT_DOCUMENT_OPERATION => document_arguments(
            "registry-document",
            "Registry research",
            "registry-document",
        )["payload"]
            .clone(),
        PROJECT_DECISION_OPERATION => {
            json!({"action":"record_effective","expected_project_version":project_version,"decision_id":"registry-decision","question":"Choose implementation","options":["A","B"],"selected_outcome":"A","rationale":"Keep the scope bounded","decision_class":"project_implementation"})
        }
        PROJECT_MILESTONE_OPERATION => {
            json!({"action":"define","expected_milestone_version":project_version,"content":{"name":"Delivery","outcome":"Deliver the requirement"}})
        }
        PROJECT_CHARTER_ADOPTION_OPERATION => {
            sqlx::query("UPDATE project SET charter_status='legacy_unverified',charter_setup_required=1,current_charter_id=NULL,current_charter_revision_id=NULL WHERE id=?").bind(PROJECT_ID).execute(f.db.pool()).await.unwrap();
            sqlx::query("DELETE FROM project_charter WHERE project_id=?")
                .bind(PROJECT_ID)
                .execute(f.db.pool())
                .await
                .unwrap();
            let mut payload = adoption_arguments("registry-adoption")["payload"].clone();
            payload["expected_charter_version"] = json!(0);
            payload
        }
        PROJECT_EVIDENCE_OPERATION => {
            sqlx::query("INSERT INTO media_asset (id,project_id,display_filename,content_type,byte_size,storage_key,checksum,availability,gc_state,created_at,updated_at) VALUES ('registry-asset',?,'proof.txt','text/plain',5,'proof.txt','registry-checksum','available','referenced',?,?)").bind(PROJECT_ID).bind(NOW).bind(NOW).execute(f.db.pool()).await.unwrap();
            json!({"action":"attach","milestone_id":"registry-milestone","expected_milestone_version":1,"asset_id":"registry-asset","checksum":"registry-checksum","acceptance_check_ids":["delivery"],"caption":"Observed requirement","kind":"report"})
        }
        PROJECT_VALIDATION_OPERATION => {
            json!({"action":"record","milestone_id":"registry-milestone","milestone_version":1,"check_id":"delivery","definition_revision_id":"registry-definition","status":"unavailable","result":"Independent execution has not run","input_digest":"registry-input"})
        }
        PROJECT_READINESS_OPERATION => {
            json!({"action":"evaluate","milestone_id":"registry-milestone","milestone_version":1})
        }
        PROJECT_RELEASE_OPERATION => release_arguments("registry-release")["payload"].clone(),
        "project.escalate" => json!({"need":"Confirm the delivery requirement","task_ids":[]}),
        "session.action" => json!({"action":"cancel","session_id":"pending-session"}),
        _ => json!({"content":"Pending proposal"}),
    };
    let arguments = json!({"operation":operation,"payload":payload,"dedupe_key":key,"correlation_id":format!("correlation-{key}")});
    let first = f
        .provider
        .propose(
            AGENT_ID,
            &f.project_scope,
            "session",
            operation,
            arguments.clone(),
        )
        .await
        .unwrap();
    if operation == "project.escalate" {
        let replay = f
            .provider
            .propose(
                AGENT_ID,
                &f.project_scope,
                "session",
                operation,
                arguments.clone(),
            )
            .await
            .unwrap();
        assert_eq!(first, replay);
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM agent_wake_escalation WHERE project_id = ?")
                .bind(PROJECT_ID)
                .fetch_one(f.db.pool())
                .await
                .unwrap();
        assert_eq!(count, 1);
        let snapshot = normalized_generated_values(
            json!({"arguments":arguments,"first":first,"escalations":count}),
        );
        let fixtures: Value = serde_json::from_str(include_str!(
            "../../../../services/tests/fixtures/project_registry_base.json"
        ))
        .unwrap();
        assert_eq!(
            snapshot, fixtures[operation],
            "{operation} hand-path fixture"
        );
        return;
    }
    if !first["receipt_id"].is_string() {
        assert!(
            matches!(
                first["status"].as_str(),
                Some("pending_approval" | "approval_required")
            ),
            "{operation}: {first}"
        );
        assert_ne!(first["status"], "succeeded");
        let replay = f
            .provider
            .propose(
                AGENT_ID,
                &f.project_scope,
                "session",
                operation,
                arguments.clone(),
            )
            .await
            .unwrap();
        assert_eq!(first, replay);
        let row = sqlx::query("SELECT payload_json, payload_hash, policy_result, status FROM agent_action WHERE operation = ? AND dedupe_key = ?")
                .bind(operation).bind(arguments["dedupe_key"].as_str().unwrap()).fetch_one(f.db.pool()).await.unwrap();
        let snapshot = normalized_generated_values(
            json!({"arguments":arguments,"first":first,"action":{"payload":serde_json::from_str::<Value>(&row.get::<String,_>("payload_json")).unwrap(),"payload_hash":row.get::<String,_>("payload_hash"),"policy_result":row.get::<String,_>("policy_result"),"status":row.get::<String,_>("status")}}),
        );
        let fixtures: Value = serde_json::from_str(include_str!(
            "../../../../services/tests/fixtures/project_registry_base.json"
        ))
        .unwrap();
        assert_eq!(
            snapshot, fixtures[operation],
            "{operation} hand-path fixture"
        );
        return;
    }
    let receipt_id = first["receipt_id"].as_str().unwrap();
    let row = sqlx::query("SELECT principal_type, principal_id, scope_type, scope_id, operation, idempotency_key, input_digest, policy_result, outcome_json FROM command_receipt WHERE id = ?")
            .bind(receipt_id).fetch_one(f.db.pool()).await.unwrap();
    let receipt = json!({
        "principal_type":row.get::<String,_>("principal_type"), "principal_id":row.get::<String,_>("principal_id"),
        "scope_type":row.get::<String,_>("scope_type"), "scope_id":row.get::<String,_>("scope_id"),
        "operation":row.get::<String,_>("operation"), "idempotency_key":row.get::<String,_>("idempotency_key"),
        "input_digest":row.get::<String,_>("input_digest"), "policy_result":row.get::<String,_>("policy_result"),
        "outcome":serde_json::from_str::<Value>(&row.get::<String,_>("outcome_json")).unwrap()
    });
    let replay = f
        .provider
        .propose(
            AGENT_ID,
            &f.project_scope,
            "session",
            operation,
            arguments.clone(),
        )
        .await
        .unwrap();
    assert_eq!(first["receipt_id"], replay["receipt_id"]);
    assert_eq!(first["event_id"], replay["event_id"]);
    assert_eq!(
        first["result"]["domain_result"],
        replay["result"]["domain_result"]
    );
    assert_eq!(replay["replayed"], true);
    // UUIDs and commit time are generated by the same domain command.
    // Preserve every other field (including every receipt digest).
    let snapshot =
        normalized_generated_values(json!({"arguments":arguments,"first":first,"receipt":receipt}));
    let fixtures: Value = serde_json::from_str(include_str!(
        "../../../../services/tests/fixtures/project_registry_base.json"
    ))
    .unwrap();
    assert_eq!(
        snapshot, fixtures[operation],
        "{operation} hand-path fixture"
    );
}

pub(super) fn normalized_generated_values(value: Value) -> Value {
    fn visit(value: &mut Value, ids: &mut std::collections::BTreeMap<String, String>) {
        match value {
            Value::String(text) if uuid::Uuid::parse_str(text).is_ok() => {
                let next = format!("generated-id-{}", ids.len() + 1);
                *text = ids.entry(text.clone()).or_insert(next).clone();
            }
            Value::String(text)
                if text.contains('T') && text.ends_with('Z') && text.starts_with("2026-") =>
            {
                *text = "generated-time".into()
            }
            Value::Object(map) => {
                for value in map.values_mut() {
                    visit(value, ids);
                }
            }
            Value::Array(values) => {
                for value in values {
                    visit(value, ids);
                }
            }
            _ => {}
        }
    }
    let mut value = value;
    visit(&mut value, &mut std::collections::BTreeMap::new());
    value
}
