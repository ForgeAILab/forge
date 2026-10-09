//! Gate A / task 2.11 acceptance coverage for the native composition boundary.
//!
//! These tests intentionally invoke the public `ScopeToolComposition` tools,
//! rather than calling `CoordinationToolProvider` directly.  The provider is
//! still real and backed by SQLite; the composition is what proves that the
//! Main, Project, and Task operation registries deliver the same structured
//! outcome contract to the runtime.

use std::{collections::BTreeSet, sync::Arc};

use agent_runtime::core::{
    cancel::Cancellation,
    clock::{Deadline, SystemClock},
    ids::{RequestId, SessionId, ToolCallId},
    prelude::{InvocationContext, PreparationContext, RuntimeError, ToolOutcome},
    workspace::DenyAllWorkspace,
};
use agent_runtime::harness::{FetchTransport, FETCH_TOOL_NAME};
use api_types::WorkflowTrigger;
use db::{
    create_sqlite_pool, run_migrations, AgentRepo, AgentStatus, CreateAgentIdentity,
    CreateAgentProfile, CreateProject, CreateRepo, ProjectRepo, RepoRepo, SqliteDb, UpdateProject,
};
use events::EventBus;
use forge_agent_host::{
    CanonicalScope, CanonicalScopeType, ForgeFetchTransport, ProjectChatToolContext,
    ScopeToolComposition, ScopeToolRuntime, WorkspaceAccess, FORGE_MAIN_ORCHESTRATION_PROPOSE_TOOL,
    FORGE_MAIN_ORCHESTRATION_READ_TOOL, FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
    FORGE_PROJECT_ORCHESTRATION_READ_TOOL, MAIN_CHARTER_APPROVAL_TARGET_OPERATION,
    MAIN_CHARTER_DIFF_OPERATION, MAIN_CHARTER_DRAFT_OPERATION, MAIN_CHARTER_READINESS_OPERATION,
    MAIN_CHARTER_READ_OPERATION, MAIN_GENESIS_PROJECT_AGENTS_READ_OPERATION,
    MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION, MAIN_GENESIS_START_OPERATION,
    MAIN_INQUIRY_RUN_OPERATION, MAIN_PROJECT_CREATE_OPERATION, MIGRATED_OPERATION_CONTRACTS,
    PROJECT_CHARTER_ADOPTION_OPERATION, PROJECT_CHARTER_READ_OPERATION,
    PROJECT_CURRENT_STATE_OPERATION, PROJECT_DECISION_OPERATION, PROJECT_DOCUMENT_OPERATION,
    PROJECT_EVIDENCE_OPERATION, PROJECT_MILESTONE_OPERATION, PROJECT_OBSERVATIONS_OPERATION,
    PROJECT_READINESS_OPERATION, PROJECT_RELEASE_OPERATION, PROJECT_REVIEW_CONFIG_OPERATION,
    PROJECT_SKILL_SECTION_OPERATION, PROJECT_VALIDATION_OPERATION, TASK_ACTION_OPERATION,
    TASK_ADAPTIVE_OPERATION, TASK_DEPENDENCY_OPERATION, TASK_EVIDENCE_OPERATION,
    TASK_PLAN_OPERATION, TASK_PROPOSE_OPERATION, TASK_WORKLOG_OPERATION,
};
use serde_json::{json, Value};
use services::{CoordinationToolProvider, TaskService};

const USER_ID: &str = "scope-composition-user";
const AGENT_ID: &str = "scope-composition-agent";
const PROFILE_ID: &str = "scope-composition-profile";
const PROJECT_AGENT_CANDIDATE_ID: &str = "scope-composition-project-agent-candidate";
const PROJECT_AGENT_CANDIDATE_PROFILE_ID: &str =
    "scope-composition-project-agent-candidate-profile";
const PROJECT_ID: &str = "scope-composition-project";
const REPO_ID: &str = "scope-composition-repo";
const PROJECT_CHARTER_ID: &str = "scope-composition-project-charter";
const PROJECT_CHARTER_REVISION_ID: &str = "scope-composition-project-charter-revision";
const MAIN_CHAT_ID: &str = "scope-composition-main-chat";
const MAIN_GENESIS_ID: &str = "scope-composition-genesis";
const MAIN_CHARTER_ID: &str = "scope-composition-main-charter";
const MAIN_REVISION_ID: &str = "scope-composition-main-revision";
const NOW: &str = "2026-08-21T00:00:00.000Z";

struct Fixture {
    db: Arc<SqliteDb>,
    provider: CoordinationToolProvider,
    main_scope: CanonicalScope,
    project_scope: CanonicalScope,
}

async fn fixture(with_task_service: bool) -> Fixture {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    let db = Arc::new(SqliteDb::new(pool));

    sqlx::query(
        "INSERT INTO user
         (id, email, password_hash, display_name, created_at, updated_at)
         VALUES (?, ?, 'test', 'Scope composition user', ?, ?)",
    )
    .bind(USER_ID)
    .bind("scope-composition@example.test")
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
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
    seed_project_charter(&db).await;

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

fn broad_permission_json() -> String {
    r#"{"permissions":["read_account","read_project","read_agent_chat","read_task","read_memory","propose_task","propose_project","propose_discovery","propose_message","propose_review","propose_commitment","propose_memory","propose_decision","propose_session"]}"#.to_owned()
}

fn broad_permissions() -> BTreeSet<String> {
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

async fn seed_main_genesis(db: &SqliteDb) {
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

async fn seed_project(db: &SqliteDb) {
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

async fn seed_project_charter(db: &SqliteDb) {
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

fn charter_content(name: &str) -> Value {
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

fn main_draft_arguments(key: &str) -> Value {
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

fn genesis_start_arguments(key: &str) -> Value {
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

fn document_arguments(key: &str, title: &str, document_id: &str) -> Value {
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

fn task_arguments(key: &str) -> Value {
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

fn review_config_arguments(key: &str, expected_project_version: i64, ci_steps: &[&str]) -> Value {
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

fn adaptive_task_arguments(
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

fn task_dependency_arguments(key: &str, action: &str, task_id: &str, depends_on: &str) -> Value {
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

fn cancel_task_arguments(key: &str, task_id: &str, expected_task_version: i64) -> Value {
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

fn release_arguments(key: &str) -> Value {
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

fn adoption_arguments(key: &str) -> Value {
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

fn project_proposal_arguments(operation: &str, action: &str, key: &str) -> Value {
    json!({
        "operation": operation,
        "payload": {"action": action},
        "dedupe_key": key,
        "correlation_id": format!("correlation-{key}")
    })
}

fn preparation_context(call_id: &str) -> PreparationContext {
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

fn invocation_context(call_id: &str) -> InvocationContext {
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

async fn invoke_tool(
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

fn assert_outcome_operation(outcome: &ToolOutcome, operation: &str) {
    assert_eq!(
        outcome.value["operation"], operation,
        "outcome: {}",
        outcome.value
    );
    assert!(outcome.value["correlation_id"].as_str().is_some());
    assert!(outcome.value["safe_message"].as_str().is_some());
}

fn assert_structured_error(outcome: &ToolOutcome, operation: &str, code: &str) {
    assert!(
        outcome.is_error,
        "expected in-band error: {}",
        outcome.value
    );
    assert_outcome_operation(outcome, operation);
    assert_eq!(outcome.value["code"], code, "outcome: {}", outcome.value);
}

#[tokio::test]
async fn scope_composition_drives_every_migrated_main_project_and_task_operation() {
    let fixture = fixture(true).await;
    let permissions = broad_permissions();
    let main = ScopeToolComposition::for_scope_with_permissions(
        AGENT_ID,
        fixture.main_scope.clone(),
        None,
        None,
        &permissions,
        Some(Arc::new(fixture.provider.clone())),
    )
    .expect("Main composition");
    let project = ScopeToolComposition::for_scope_with_permissions(
        AGENT_ID,
        fixture.project_scope.clone(),
        None,
        None,
        &permissions,
        Some(Arc::new(fixture.provider.clone())),
    )
    .expect("Project composition");
    let mut covered_operations = BTreeSet::new();

    let read_cases = [
        (
            MAIN_GENESIS_PROJECT_AGENTS_READ_OPERATION,
            json!({"genesis_session_id": MAIN_GENESIS_ID}),
        ),
        (
            MAIN_CHARTER_READ_OPERATION,
            json!({"charter_id": MAIN_CHARTER_ID, "genesis_session_id": MAIN_GENESIS_ID}),
        ),
        (
            MAIN_CHARTER_READINESS_OPERATION,
            json!({
                "charter_id": MAIN_CHARTER_ID,
                "revision_id": MAIN_REVISION_ID,
                "content_digest": "main-content-1",
                "render_digest": "main-render-1",
                "expected_charter_version": 1,
                "genesis_session_id": MAIN_GENESIS_ID
            }),
        ),
        (
            MAIN_CHARTER_DIFF_OPERATION,
            json!({
                "charter_id": MAIN_CHARTER_ID,
                "base_revision_id": MAIN_REVISION_ID,
                "candidate_revision_id": MAIN_REVISION_ID,
                "genesis_session_id": MAIN_GENESIS_ID
            }),
        ),
        (
            MAIN_CHARTER_APPROVAL_TARGET_OPERATION,
            json!({
                "charter_id": MAIN_CHARTER_ID,
                "revision_id": MAIN_REVISION_ID,
                "content_digest": "main-content-1",
                "render_digest": "main-render-1",
                "expected_charter_version": 1,
                "genesis_session_id": MAIN_GENESIS_ID
            }),
        ),
        (PROJECT_CURRENT_STATE_OPERATION, json!({"limit": 10})),
    ];
    for (operation, arguments) in read_cases {
        let target = if operation == PROJECT_CURRENT_STATE_OPERATION {
            &project
        } else {
            &main
        };
        let tool_name = if operation == PROJECT_CURRENT_STATE_OPERATION {
            FORGE_PROJECT_ORCHESTRATION_READ_TOOL
        } else {
            FORGE_MAIN_ORCHESTRATION_READ_TOOL
        };
        let outcome = invoke_tool(
            target,
            tool_name,
            json!({"operation": operation, "arguments": arguments}),
            operation,
        )
        .await
        .unwrap_or_else(|error| panic!("{operation} failed at composition boundary: {error:?}"));
        assert_outcome_operation(&outcome, operation);
        assert!(
            !outcome.is_error,
            "successful query outcome: {}",
            outcome.value
        );
        covered_operations.insert(operation.to_owned());
    }

    let main_proposals = [
        (
            MAIN_GENESIS_START_OPERATION,
            genesis_start_arguments("matrix-genesis-start"),
        ),
        (
            MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
            json!({
                "operation": MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
                "payload": {
                    "action": "select",
                    "genesis_session_id": MAIN_GENESIS_ID,
                    "expected_session_version": 1,
                    "project_agent_identity_id": PROJECT_AGENT_CANDIDATE_ID
                },
                "dedupe_key": "matrix-project-agent-select",
                "correlation_id": "correlation-matrix-project-agent-select"
            }),
        ),
        (
            MAIN_CHARTER_DRAFT_OPERATION,
            main_draft_arguments("matrix-main-draft"),
        ),
        (
            MAIN_PROJECT_CREATE_OPERATION,
            // A new Project-create call must name its Charter approval.
            json!({
                "operation": MAIN_PROJECT_CREATE_OPERATION,
                "payload": {"action": "create_from_approval", "approval_id": "matrix-approval"},
                "dedupe_key": "matrix-main-project",
                "correlation_id": "correlation-matrix-main-project"
            }),
        ),
    ];
    for (operation, arguments) in main_proposals {
        let outcome = invoke_tool(
            &main,
            FORGE_MAIN_ORCHESTRATION_PROPOSE_TOOL,
            arguments,
            operation,
        )
        .await
        .unwrap_or_else(|error| panic!("{operation} failed at composition boundary: {error:?}"));
        assert_outcome_operation(&outcome, operation);
        if operation == MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION {
            assert!(
                !outcome.is_error,
                "successful Main Project-Agent selection outcome: {}",
                outcome.value
            );
        }
        covered_operations.insert(operation.to_owned());
    }

    let ready_propose = project
        .tools()
        .into_iter()
        .find(|tool| tool.spec().name == FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL)
        .expect("ready Project proposal tool");
    let ready_spec = ready_propose.spec();
    let ready_operations = ready_spec.input_schema["properties"]["operation"]["enum"]
        .as_array()
        .expect("ready Project operation enum");
    assert!(
        ready_operations
            .iter()
            .any(|value| value == PROJECT_CHARTER_ADOPTION_OPERATION),
        "amendment drafting must be present in a ready Project scope"
    );

    let project_proposals = [
        (
            PROJECT_REVIEW_CONFIG_OPERATION,
            project_proposal_arguments(
                PROJECT_REVIEW_CONFIG_OPERATION,
                "set_ci_steps",
                "matrix-review-config",
            ),
        ),
        (
            PROJECT_DOCUMENT_OPERATION,
            document_arguments(
                "matrix-document",
                "Matrix document",
                "scope-composition-document",
            ),
        ),
        (
            PROJECT_DECISION_OPERATION,
            project_proposal_arguments(
                PROJECT_DECISION_OPERATION,
                "record_candidate",
                "matrix-decision",
            ),
        ),
        (
            PROJECT_MILESTONE_OPERATION,
            project_proposal_arguments(PROJECT_MILESTONE_OPERATION, "revise", "matrix-milestone"),
        ),
        (
            PROJECT_EVIDENCE_OPERATION,
            project_proposal_arguments(PROJECT_EVIDENCE_OPERATION, "attach", "matrix-evidence"),
        ),
        (
            PROJECT_VALIDATION_OPERATION,
            project_proposal_arguments(PROJECT_VALIDATION_OPERATION, "record", "matrix-validation"),
        ),
        (
            PROJECT_READINESS_OPERATION,
            project_proposal_arguments(PROJECT_READINESS_OPERATION, "evaluate", "matrix-readiness"),
        ),
        (
            PROJECT_RELEASE_OPERATION,
            release_arguments("matrix-release"),
        ),
    ];
    let migrated_inputs: Value = serde_json::from_str(include_str!(
        "../../operation-registry/tests/project_inputs.json"
    ))
    .unwrap();
    for (operation, mut arguments) in project_proposals {
        if operation_registry::project_proposals::IDS.contains(&operation)
            && operation != PROJECT_DOCUMENT_OPERATION
        {
            arguments["payload"] = migrated_inputs[operation].clone();
        }
        let outcome = invoke_tool(
            &project,
            FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
            arguments,
            operation,
        )
        .await
        .unwrap_or_else(|error| panic!("{operation} failed at composition boundary: {error:?}"));
        assert_outcome_operation(&outcome, operation);
        covered_operations.insert(operation.to_owned());
    }

    let task = invoke_tool(
        &project,
        "forge_scope_propose",
        task_arguments("matrix-task"),
        TASK_PROPOSE_OPERATION,
    )
    .await
    .expect("task.propose composition call");
    assert_outcome_operation(&task, TASK_PROPOSE_OPERATION);
    covered_operations.insert(TASK_PROPOSE_OPERATION.to_owned());
    assert!(
        !task.is_error,
        "Task proposal should commit: {}",
        task.value
    );

    let source_task_id = task.value["result"]["domain_result"]["task_id"]
        .as_str()
        .expect("task.propose source Task id");
    let source_task_version: i64 = sqlx::query_scalar("SELECT version FROM task WHERE id = ?")
        .bind(source_task_id)
        .fetch_one(fixture.db.pool())
        .await
        .expect("adaptive source Task version");
    let board_revision: i64 = sqlx::query_scalar("SELECT board_revision FROM project WHERE id = ?")
        .bind(PROJECT_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("adaptive board revision");
    let adaptive = invoke_tool(
        &project,
        "forge_scope_propose",
        adaptive_task_arguments(
            "matrix-task-adaptive",
            source_task_id,
            source_task_version,
            board_revision,
        ),
        TASK_ADAPTIVE_OPERATION,
    )
    .await
    .expect("task.adaptive composition call");
    assert_outcome_operation(&adaptive, TASK_ADAPTIVE_OPERATION);
    assert!(
        !adaptive.is_error,
        "adaptive Task should commit: {}",
        adaptive.value
    );
    covered_operations.insert(TASK_ADAPTIVE_OPERATION.to_owned());

    let cancellable = invoke_tool(
        &project,
        "forge_scope_propose",
        task_arguments("matrix-cancellable-task"),
        TASK_PROPOSE_OPERATION,
    )
    .await
    .expect("cancellable Task proposal");
    assert!(
        !cancellable.is_error,
        "Task proposal: {}",
        cancellable.value
    );
    let cancellable_task_id = cancellable.value["result"]["domain_result"]["task_id"]
        .as_str()
        .expect("cancellable Task id");
    let cancellable_task_version: i64 = sqlx::query_scalar("SELECT version FROM task WHERE id = ?")
        .bind(cancellable_task_id)
        .fetch_one(fixture.db.pool())
        .await
        .expect("cancellable Task version");
    // The graph command: point the cancellable Task at the Task proposed
    // earlier, then take the edge away again. Neither id changes, which is the
    // whole reason this operation exists.
    let edge_added = invoke_tool(
        &project,
        "forge_scope_propose",
        task_dependency_arguments(
            "matrix-task-dependency-add",
            "add",
            cancellable_task_id,
            source_task_id,
        ),
        TASK_DEPENDENCY_OPERATION,
    )
    .await
    .expect("task.dependency add composition call");
    assert_outcome_operation(&edge_added, TASK_DEPENDENCY_OPERATION);
    assert!(
        !edge_added.is_error,
        "adding a prerequisite edge should commit: {}",
        edge_added.value
    );
    let edge_removed = invoke_tool(
        &project,
        "forge_scope_propose",
        task_dependency_arguments(
            "matrix-task-dependency-remove",
            "remove",
            cancellable_task_id,
            source_task_id,
        ),
        TASK_DEPENDENCY_OPERATION,
    )
    .await
    .expect("task.dependency remove composition call");
    assert_outcome_operation(&edge_removed, TASK_DEPENDENCY_OPERATION);
    assert!(
        !edge_removed.is_error,
        "removing a prerequisite edge should commit: {}",
        edge_removed.value
    );
    covered_operations.insert(TASK_DEPENDENCY_OPERATION.to_owned());

    let cancelled = invoke_tool(
        &project,
        "forge_scope_propose",
        cancel_task_arguments(
            "matrix-task-cancel",
            cancellable_task_id,
            cancellable_task_version,
        ),
        TASK_ACTION_OPERATION,
    )
    .await
    .expect("task.action composition call");
    assert_outcome_operation(&cancelled, TASK_ACTION_OPERATION);
    assert!(
        !cancelled.is_error,
        "healthy Task cancellation should commit: {}",
        cancelled.value
    );
    assert_eq!(cancelled.value["result"]["task_status"], "cancelled");
    covered_operations.insert(TASK_ACTION_OPERATION.to_owned());

    let mut human_review_workflow = services::workflow::default_workflow::default_workflow();
    let review_state = human_review_workflow
        .states
        .iter_mut()
        .find(|state| state.name == "review")
        .expect("default review state");
    review_state
        .gate_config
        .as_mut()
        .expect("default review gate")
        .requires_user_approval = Some(true);
    let reject = review_state
        .triggers
        .get_mut(&WorkflowTrigger::Reject)
        .expect("default review rejection");
    reject.to = "cancelled".to_owned();
    reject.dispatch = None;
    sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
        .bind(serde_json::to_string(&human_review_workflow).expect("workflow serializes"))
        .bind(PROJECT_ID)
        .execute(fixture.db.pool())
        .await
        .expect("human-required workflow installs");
    sqlx::query(
        "UPDATE task SET status = 'review', version = version + 1, updated_at = ? WHERE id = ?",
    )
    .bind(db::now_rfc3339())
    .bind(source_task_id)
    .execute(fixture.db.pool())
    .await
    .expect("Task enters human-required review");
    let review_task_version: i64 = sqlx::query_scalar("SELECT version FROM task WHERE id = ?")
        .bind(source_task_id)
        .fetch_one(fixture.db.pool())
        .await
        .expect("review Task version");
    let review = invoke_tool(
        &project,
        "forge_scope_propose",
        json!({
            "operation": TASK_ACTION_OPERATION,
            "payload": {
                "task_id": source_task_id,
                "action": {"verb":"send_back","guidance":"Exercise the Project Agent human-review decision."},
                "version": review_task_version
            },
            "dedupe_key": "matrix-task-review",
            "correlation_id": "correlation-matrix-task-review"
        }),
        TASK_ACTION_OPERATION,
    )
    .await
    .expect("task.review composition call");
    assert_outcome_operation(&review, TASK_ACTION_OPERATION);
    assert!(
        !review.is_error,
        "Task review should commit: {}",
        review.value
    );
    assert_eq!(review.value["result"]["task_status"], "cancelled");
    covered_operations.insert(TASK_ACTION_OPERATION.to_owned());

    let setup = ScopeToolComposition::for_scope_with_permissions_and_project_context(
        AGENT_ID,
        fixture.project_scope.clone(),
        None,
        None,
        &permissions,
        ProjectChatToolContext {
            is_project_agent_chat: true,
            charter_setup_required: true,
        },
        Some(Arc::new(fixture.provider.clone())),
        ScopeToolRuntime::default(),
    )
    .expect("setup Project composition");
    let setup_propose = setup
        .tools()
        .into_iter()
        .find(|tool| tool.spec().name == FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL)
        .expect("setup adoption proposal tool");
    let setup_spec = setup_propose.spec();
    let setup_operations = setup_spec.input_schema["properties"]["operation"]["enum"]
        .as_array()
        .expect("setup operation enum");
    assert_eq!(
        setup_operations,
        &[json!(PROJECT_CHARTER_ADOPTION_OPERATION)]
    );
    let adoption = invoke_tool(
        &setup,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        adoption_arguments("matrix-adoption"),
        PROJECT_CHARTER_ADOPTION_OPERATION,
    )
    .await
    .unwrap_or_else(|error| {
        panic!("{PROJECT_CHARTER_ADOPTION_OPERATION} failed at composition boundary: {error:?}")
    });
    assert_outcome_operation(&adoption, PROJECT_CHARTER_ADOPTION_OPERATION);
    covered_operations.insert(PROJECT_CHARTER_ADOPTION_OPERATION.to_owned());

    // The remaining migrated contracts are asserted by composition exposure
    // rather than by invocation. `project.observations` and `task.action` are
    // Project-scoped; `task.plan`, `task.worklog`, and `task.evidence` are
    // Task-scoped and need a leased Task session this fixture does not build.
    // Exposure is the property this test is named for: every migrated contract
    // must be surfaced by scope composition in a scope that supports it.
    let project_read = project
        .tools()
        .into_iter()
        .find(|tool| tool.spec().name == "forge_project_orchestration_read")
        .expect("Project read tool");
    let project_read_operations = project_read.spec().input_schema["properties"]["operation"]
        ["enum"]
        .as_array()
        .expect("Project read operation enum")
        .clone();
    for operation in [
        PROJECT_OBSERVATIONS_OPERATION,
        PROJECT_CHARTER_READ_OPERATION,
        PROJECT_SKILL_SECTION_OPERATION,
    ] {
        assert!(
            project_read_operations
                .iter()
                .any(|value| value == operation),
            "Project read composition must expose {operation}"
        );
        covered_operations.insert(operation.to_owned());
    }

    let project_propose_operations = project
        .tools()
        .into_iter()
        .find(|tool| tool.spec().name == "forge_scope_propose")
        .expect("Project propose tool")
        .spec()
        .input_schema["properties"]["operation"]["enum"]
        .as_array()
        .expect("Project propose operation enum")
        .clone();
    assert!(
        project_propose_operations
            .iter()
            .any(|value| value == TASK_ACTION_OPERATION),
        "Project propose composition must expose {TASK_ACTION_OPERATION}"
    );
    covered_operations.insert(TASK_ACTION_OPERATION.to_owned());

    let task_composition = ScopeToolComposition::for_scope_with_permissions_and_project_context(
        AGENT_ID,
        CanonicalScope {
            scope_type: CanonicalScopeType::Task,
            scope_id: source_task_id.to_owned(),
            workspace_access: WorkspaceAccess::TaskWrite,
        },
        Some("coder"),
        Some("/tmp/forge-scope-composition-worktree"),
        &{
            let mut permissions = broad_permissions();
            // Task-scope evidence capture is admitted at read level; the
            // session's write authority is the bounded worktree tools.
            permissions.insert("task_read".to_owned());
            permissions
        },
        ProjectChatToolContext {
            is_project_agent_chat: false,
            charter_setup_required: false,
        },
        Some(Arc::new(fixture.provider.clone())),
        ScopeToolRuntime::default(),
    )
    .expect("Task composition");
    let task_tool_names = task_composition
        .tools()
        .into_iter()
        .map(|tool| tool.spec().name.clone())
        .collect::<Vec<_>>();
    let task_propose_operations = task_composition
        .tools()
        .into_iter()
        .find(|tool| tool.spec().name == "forge_scope_propose")
        .unwrap_or_else(|| panic!("Task propose tool among {task_tool_names:?}"))
        .spec()
        .input_schema["properties"]["operation"]["enum"]
        .as_array()
        .expect("Task propose operation enum")
        .clone();
    for operation in [
        TASK_PLAN_OPERATION,
        TASK_WORKLOG_OPERATION,
        TASK_EVIDENCE_OPERATION,
    ] {
        assert!(
            task_propose_operations
                .iter()
                .any(|value| value == operation),
            "Task propose composition must expose {operation}"
        );
        covered_operations.insert(operation.to_owned());
    }

    // Dispatching an inquiry belongs to a Main *Chat*, not to the bare
    // Account scope above: the Account scope is what an inquiry sub-agent
    // itself runs under, and withholding the operation there is what caps
    // dispatch depth at one. So the coverage for it lives on its own
    // composition rather than on `main`.
    let main_chat_composition =
        ScopeToolComposition::for_scope_with_permissions_and_project_context(
            AGENT_ID,
            CanonicalScope {
                scope_type: CanonicalScopeType::AgentChat,
                scope_id: "main-chat-scope-composition".to_owned(),
                workspace_access: WorkspaceAccess::Deny,
            },
            None,
            None,
            &broad_permissions(),
            ProjectChatToolContext {
                is_project_agent_chat: false,
                charter_setup_required: false,
            },
            Some(Arc::new(fixture.provider.clone())),
            ScopeToolRuntime::default(),
        )
        .expect("Main Chat composition");
    let main_chat_reads = main_chat_composition
        .tools()
        .into_iter()
        .find(|tool| tool.spec().name == FORGE_MAIN_ORCHESTRATION_READ_TOOL)
        .expect("Main Chat orchestration read tool")
        .spec()
        .input_schema["properties"]["operation"]["enum"]
        .as_array()
        .expect("Main Chat read operation enum")
        .clone();
    assert!(
        main_chat_reads
            .iter()
            .any(|value| value == MAIN_INQUIRY_RUN_OPERATION),
        "Main Chat composition must expose {MAIN_INQUIRY_RUN_OPERATION}"
    );
    covered_operations.insert(MAIN_INQUIRY_RUN_OPERATION.to_owned());

    let escalation = invoke_tool(&project, FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        json!({"operation":"project.escalate","payload":{"need":"Free disk before recovery","task_ids":[]},"dedupe_key":"matrix-escalation","correlation_id":"matrix-escalation"}),"matrix-escalation").await.expect("native Project escalation");
    assert!(!escalation.is_error, "escalation: {}", escalation.value);
    assert_eq!(
        escalation.value["result"]["domain_result"]["need"],
        "Free disk before recovery"
    );
    let notifications: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM notification WHERE project_id=? AND event_type='project.escalated'",
    )
    .bind(PROJECT_ID)
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    assert_eq!(notifications, 1);
    covered_operations.insert("project.escalate".to_owned());

    let expected_operations = MIGRATED_OPERATION_CONTRACTS
        .iter()
        .map(|contract| contract.operation.to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(covered_operations, expected_operations);
}

#[tokio::test]
async fn project_agent_sets_replay_safe_default_ci_steps_without_erasing_review_policy() {
    let fixture = fixture(false).await;
    let project = ProjectRepo::get_by_id(&*fixture.db, PROJECT_ID)
        .await
        .expect("Project lookup")
        .expect("Project exists");
    let project = ProjectRepo::update_at_version(
        &*fixture.db,
        UpdateProject {
            id: PROJECT_ID.to_owned(),
            name: None,
            settings: Some(
                json!({
                    "retry_budgets": {"review": 5},
                    "default_review_config": {
                        "review_prompt": "Preserve this reviewer instruction.",
                        "requirement_ids": ["project-requirement"]
                    }
                })
                .to_string(),
            ),
            primary_repo_id: None,
            paused_at: None,
            updated_at: db::now_rfc3339(),
        },
        project.version,
        None,
    )
    .await
    .expect("seed existing Project review policy");
    let expected_project_version = project.version;
    let composition = ScopeToolComposition::for_scope_with_permissions(
        AGENT_ID,
        fixture.project_scope.clone(),
        None,
        None,
        &broad_permissions(),
        Some(Arc::new(fixture.provider.clone())),
    )
    .expect("Project composition");
    let mut arguments = review_config_arguments(
        "project-review-config",
        expected_project_version,
        &[
            "cargo test --workspace",
            "cargo clippy --workspace --all-targets -- -D warnings",
        ],
    );
    arguments["payload"]["setup_steps"] = json!(["cargo fetch --locked"]);

    let first = invoke_tool(
        &composition,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        arguments.clone(),
        "project-review-config-first",
    )
    .await
    .expect("Project review config command");
    assert!(!first.is_error, "review config outcome: {}", first.value);
    assert_eq!(
        first.value["result"]["domain_result"]["project_version"],
        expected_project_version + 1
    );
    assert_eq!(
        first.value["result"]["domain_result"]["ci_steps"],
        json!([
            "cargo test --workspace",
            "cargo clippy --workspace --all-targets -- -D warnings"
        ])
    );
    assert_eq!(
        first.value["result"]["domain_result"]["setup_steps"],
        json!(["cargo fetch --locked"])
    );

    let updated = ProjectRepo::get_by_id(&*fixture.db, PROJECT_ID)
        .await
        .expect("updated Project lookup")
        .expect("updated Project exists");
    assert_eq!(updated.version, expected_project_version + 1);
    let settings: Value = serde_json::from_str(&updated.settings).expect("Project settings JSON");
    assert_eq!(settings["retry_budgets"]["review"], 5);
    assert_eq!(
        settings["default_review_config"]["review_prompt"],
        "Preserve this reviewer instruction."
    );
    assert_eq!(
        settings["default_review_config"]["requirement_ids"],
        json!(["project-requirement"])
    );
    assert_eq!(
        settings["default_review_config"]["ci_steps"],
        json!([
            "cargo test --workspace",
            "cargo clippy --workspace --all-targets -- -D warnings"
        ])
    );
    assert_eq!(
        settings["default_review_config"]["setup_steps"],
        json!(["cargo fetch --locked"])
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_action WHERE operation = 'project.review_config'"
        )
        .fetch_one(fixture.db.pool())
        .await
        .expect("AgentAction count"),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt WHERE operation = 'project.review_config'"
        )
        .fetch_one(fixture.db.pool())
        .await
        .expect("command receipt count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT event_type FROM domain_event
             WHERE entity_id = ? AND event_type = 'project.review_config.updated'"
        )
        .bind(PROJECT_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("review config event"),
        "project.review_config.updated"
    );

    let replay = invoke_tool(
        &composition,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        arguments.clone(),
        "project-review-config-replay",
    )
    .await
    .expect("Project review config replay");
    assert!(!replay.is_error, "replay outcome: {}", replay.value);
    assert_eq!(replay.value["result"]["replayed"], true);
    assert_eq!(
        replay.value["result"]["receipt_id"],
        first.value["result"]["receipt_id"]
    );
    assert_eq!(
        ProjectRepo::get_by_id(&*fixture.db, PROJECT_ID)
            .await
            .expect("replayed Project lookup")
            .expect("replayed Project exists")
            .version,
        expected_project_version + 1
    );

    let stale = invoke_tool(
        &composition,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        review_config_arguments(
            "project-review-config-stale",
            expected_project_version,
            &["cargo test --workspace"],
        ),
        "project-review-config-stale",
    )
    .await
    .expect("stale Project review config outcome");
    assert_structured_error(&stale, PROJECT_REVIEW_CONFIG_OPERATION, "version_conflict");
    assert_eq!(
        stale.value["retry"]["arguments"]["expected_project_version"],
        expected_project_version + 1
    );

    let mismatch = invoke_tool(
        &composition,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        review_config_arguments(
            "project-review-config",
            expected_project_version,
            &["cargo test -p services"],
        ),
        "project-review-config-mismatch",
    )
    .await
    .expect("Project review config idempotency outcome");
    assert_structured_error(
        &mismatch,
        PROJECT_REVIEW_CONFIG_OPERATION,
        "idempotency_conflict",
    );

    let current = invoke_tool(
        &composition,
        FORGE_PROJECT_ORCHESTRATION_READ_TOOL,
        json!({"operation": PROJECT_CURRENT_STATE_OPERATION, "arguments": {"limit": 10}}),
        "project-review-config-current-state",
    )
    .await
    .expect("Project current state");
    assert!(
        !current.is_error,
        "current-state outcome: {}",
        current.value
    );
    assert_eq!(
        current.value["result"]["effective_state"]["project"]["default_review_ci_steps"],
        json!([
            "cargo test --workspace",
            "cargo clippy --workspace --all-targets -- -D warnings"
        ])
    );
    assert_eq!(
        current.value["result"]["effective_state"]["project"]["default_review_setup_steps"],
        json!(["cargo fetch --locked"])
    );
}

#[tokio::test]
async fn project_task_cancel_is_scoped_versioned_and_outcome_idempotent() {
    let fixture = fixture(true).await;
    let project = ScopeToolComposition::for_scope_with_permissions(
        AGENT_ID,
        fixture.project_scope.clone(),
        None,
        None,
        &broad_permissions(),
        Some(Arc::new(fixture.provider.clone())),
    )
    .expect("Project composition");
    let proposed = invoke_tool(
        &project,
        "forge_scope_propose",
        task_arguments("cancel-versioned-task"),
        TASK_PROPOSE_OPERATION,
    )
    .await
    .expect("Task proposal");
    assert!(!proposed.is_error, "Task proposal: {}", proposed.value);
    let task_id = proposed.value["result"]["domain_result"]["task_id"]
        .as_str()
        .expect("Task id")
        .to_owned();
    let original_version: i64 = sqlx::query_scalar("SELECT version FROM task WHERE id = ?")
        .bind(&task_id)
        .fetch_one(fixture.db.pool())
        .await
        .expect("Task version");
    sqlx::query(
        "UPDATE task SET priority = priority + 1, version = version + 1, updated_at = ? WHERE id = ?",
    )
    .bind(db::now_rfc3339())
    .bind(&task_id)
    .execute(fixture.db.pool())
    .await
    .expect("concurrent Task update");
    let current_version = original_version + 1;

    let stale = invoke_tool(
        &project,
        "forge_scope_propose",
        cancel_task_arguments("cancel-stale", &task_id, original_version),
        TASK_ACTION_OPERATION,
    )
    .await
    .expect("stale cancellation is structured");
    assert_structured_error(&stale, TASK_ACTION_OPERATION, "version_conflict");
    assert_eq!(
        stale.value["current_version_or_revision"]["resource_type"],
        "task"
    );
    assert_eq!(
        stale.value["current_version_or_revision"]["version"],
        current_version
    );
    assert_eq!(
        stale.value["retry"]["arguments"]["version"],
        current_version
    );

    let other_project_id = "scope-composition-other-project";
    ProjectRepo::create(
        &*fixture.db,
        CreateProject {
            id: other_project_id.to_owned(),
            name: "Other Project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(USER_ID.to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("other Project");
    let other_task =
        TaskService::new_for_test(Arc::clone(&fixture.db), Arc::new(EventBus::new(16)))
            .create_task(
                other_project_id,
                "Other Project Task",
                None,
                None,
                None,
                Some("planning_task".to_owned()),
                None,
                None,
                None,
            )
            .await
            .expect("other Project Task");
    let cross_project = invoke_tool(
        &project,
        "forge_scope_propose",
        cancel_task_arguments("cancel-cross-project", &other_task.id, other_task.version),
        TASK_ACTION_OPERATION,
    )
    .await
    .expect("cross-Project cancellation is structured");
    assert_structured_error(&cross_project, TASK_ACTION_OPERATION, "validation_error");

    let cancelled = invoke_tool(
        &project,
        "forge_scope_propose",
        cancel_task_arguments("cancel-current", &task_id, current_version),
        TASK_ACTION_OPERATION,
    )
    .await
    .expect("current cancellation");
    assert!(!cancelled.is_error, "cancellation: {}", cancelled.value);
    assert_eq!(cancelled.value["result"]["task_status"], "cancelled");

    let retry = invoke_tool(
        &project,
        "forge_scope_propose",
        cancel_task_arguments("cancel-current", &task_id, current_version),
        TASK_ACTION_OPERATION,
    )
    .await
    .expect("response-loss retry");
    // An exact Task action version is still required after response loss. The
    // cancelled outcome remains unchanged, and its fresh offer set is empty.
    assert_structured_error(&retry, TASK_ACTION_OPERATION, "version_conflict");
    let cancelled_task = db::TaskRepo::get_by_id(&*fixture.db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cancelled_task.status, "cancelled");
    assert_eq!(cancelled_task.version, current_version + 1);
    let terminal = invoke_tool(
        &project,
        "forge_scope_propose",
        cancel_task_arguments("cancel-terminal", &task_id, cancelled_task.version),
        TASK_ACTION_OPERATION,
    )
    .await
    .unwrap();
    assert_structured_error(&terminal, TASK_ACTION_OPERATION, "action_unavailable");
    assert_eq!(terminal.value["details"]["available_actions"], json!([]));
    let unchanged = db::TaskRepo::get_by_id(&*fixture.db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unchanged.version, cancelled_task.version);
    assert_eq!(unchanged.updated_at, cancelled_task.updated_at);
}

#[tokio::test]
async fn scope_composition_preserves_replay_approval_policy_version_and_idempotency_outcomes() {
    let fixture = fixture(true).await;
    let permissions = broad_permissions();
    let project = ScopeToolComposition::for_scope_with_permissions(
        AGENT_ID,
        fixture.project_scope.clone(),
        None,
        None,
        &permissions,
        Some(Arc::new(fixture.provider.clone())),
    )
    .expect("Project composition");

    let first = invoke_tool(
        &project,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        document_arguments(
            "composition-replay",
            "Replay document",
            "scope-composition-document",
        ),
        "composition-replay-1",
    )
    .await
    .expect("first document command");
    assert!(!first.is_error);
    assert_outcome_operation(&first, PROJECT_DOCUMENT_OPERATION);
    assert_eq!(first.value["code"], "ok");
    assert_eq!(first.value["replayed"], false);

    let replay = invoke_tool(
        &project,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        document_arguments(
            "composition-replay",
            "Replay document",
            "scope-composition-document",
        ),
        "composition-replay-2",
    )
    .await
    .expect("document replay");
    assert!(!replay.is_error);
    assert_eq!(replay.value["code"], "ok");
    assert_eq!(replay.value["replayed"], true);
    assert_eq!(replay.value["receipt_id"], first.value["receipt_id"]);

    let approval = invoke_tool(
        &project,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        release_arguments("composition-approval"),
        "composition-approval",
    )
    .await
    .expect("approval proposal");
    assert!(!approval.is_error);
    assert_eq!(approval.value["code"], "approval_required");
    assert_eq!(approval.value["status"], "approval_required");
    assert_eq!(
        approval.value["approval_target"]["operation"],
        PROJECT_RELEASE_OPERATION
    );

    sqlx::query(
        "UPDATE project_agent_binding SET permission_ceiling_json = ?
         WHERE project_id = ? AND state = 'active'",
    )
    .bind(r#"{"allowed":["read_project"]}"#)
    .bind(PROJECT_ID)
    .execute(fixture.db.pool())
    .await
    .expect("restrict Project policy");
    let denied = invoke_tool(
        &project,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        document_arguments(
            "composition-policy",
            "Policy document",
            "scope-composition-document",
        ),
        "composition-policy",
    )
    .await
    .expect("policy denial remains in-band");
    assert_structured_error(&denied, PROJECT_DOCUMENT_OPERATION, "policy_denied");

    // Restoring live policy does not lift an operation-wide denial in its
    // existing turn. Start a new turn composition for subsequent cases.
    sqlx::query(
        "UPDATE project_agent_binding SET permission_ceiling_json = ?
         WHERE project_id = ? AND state = 'active'",
    )
    .bind(broad_permission_json())
    .bind(PROJECT_ID)
    .execute(fixture.db.pool())
    .await
    .expect("restore Project policy");

    let still_denied = invoke_tool(
        &project,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        document_arguments(
            "composition-restored",
            "Restored policy",
            "another-document",
        ),
        "composition-restored",
    )
    .await
    .expect("same-turn denial remains in-band");
    assert_eq!(still_denied.value, denied.value);
    let project = ScopeToolComposition::for_scope_with_permissions(
        AGENT_ID,
        fixture.project_scope.clone(),
        None,
        None,
        &permissions,
        Some(Arc::new(fixture.provider.clone())),
    )
    .expect("next-turn Project composition");

    let stale = invoke_tool(
        &project,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        document_arguments(
            "composition-stale",
            "Stale document",
            "scope-composition-document",
        ),
        "composition-stale",
    )
    .await
    .expect("first stale-document write");
    assert!(!stale.is_error);
    let stale_retry = invoke_tool(
        &project,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        document_arguments(
            "composition-stale-retry",
            "Stale document",
            stale.value["result"]["domain_result"]["document_id"]
                .as_str()
                .expect("created document id"),
        ),
        "composition-stale-retry",
    )
    .await
    .expect("stale-document conflict");
    assert_structured_error(&stale_retry, PROJECT_DOCUMENT_OPERATION, "version_conflict");
    assert_eq!(stale_retry.value["retry"]["action"], "refresh_and_retry");
    assert!(stale_retry.value["current_version_or_revision"]["version"].is_number());

    let mismatch = invoke_tool(
        &project,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        document_arguments(
            "composition-idempotency",
            "Initial input",
            "scope-composition-idempotency-document",
        ),
        "composition-idempotency-1",
    )
    .await
    .expect("idempotency first command");
    assert!(!mismatch.is_error);
    let mismatch_input = document_arguments(
        "composition-idempotency",
        "Changed input",
        "scope-composition-idempotency-document",
    );
    let mismatch = invoke_tool(
        &project,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        mismatch_input,
        "composition-idempotency-2",
    )
    .await
    .expect("idempotency mismatch");
    assert_structured_error(
        &mismatch,
        PROJECT_DOCUMENT_OPERATION,
        "idempotency_conflict",
    );
    assert!(mismatch.value["current_version_or_revision"].is_null());
    assert_eq!(mismatch.value["retry"]["action"], "use_new_idempotency_key");
}

#[tokio::test]
async fn scope_composition_keeps_setup_not_found_and_internal_failures_structured() {
    let setup_fixture = fixture(false).await;
    let permissions = broad_permissions();
    let project = ScopeToolComposition::for_scope_with_permissions(
        AGENT_ID,
        setup_fixture.project_scope.clone(),
        None,
        None,
        &permissions,
        Some(Arc::new(setup_fixture.provider.clone())),
    )
    .expect("Project composition");
    let setup = invoke_tool(
        &project,
        "forge_scope_propose",
        task_arguments("composition-setup"),
        TASK_PROPOSE_OPERATION,
    )
    .await
    .expect("missing TaskService is a structured setup outcome");
    assert_structured_error(&setup, TASK_PROPOSE_OPERATION, "setup_required");
    assert!(setup.value["setup_requirements"].is_array());

    let not_found = ScopeToolComposition::for_scope_with_permissions(
        AGENT_ID,
        setup_fixture.main_scope.clone(),
        None,
        None,
        &permissions,
        Some(Arc::new(setup_fixture.provider.clone())),
    )
    .expect("Main composition");
    let not_found = invoke_tool(
        &not_found,
        FORGE_MAIN_ORCHESTRATION_READ_TOOL,
        json!({
            "operation": MAIN_CHARTER_READINESS_OPERATION,
            "arguments": {
                "charter_id": "missing-charter",
                "revision_id": MAIN_REVISION_ID,
                "content_digest": "main-content-1",
                "render_digest": "main-render-1",
                "expected_charter_version": 1,
                "genesis_session_id": MAIN_GENESIS_ID
            }
        }),
        "composition-not-found",
    )
    .await
    .expect("missing Charter is a structured not-found outcome");
    assert_structured_error(&not_found, MAIN_CHARTER_READINESS_OPERATION, "not_found");

    setup_fixture.db.pool().close().await;
    let internal = invoke_tool(
        &project,
        FORGE_PROJECT_ORCHESTRATION_READ_TOOL,
        json!({"operation": PROJECT_CURRENT_STATE_OPERATION, "arguments": {"limit": 10}}),
        "composition-internal",
    )
    .await
    .expect("persistence failure is a structured internal outcome");
    assert_structured_error(
        &internal,
        PROJECT_CURRENT_STATE_OPERATION,
        "internal_failure",
    );
    let rendered = internal.value.to_string();
    assert!(!rendered.contains("no such table"));
    assert!(!rendered.contains("sqlx"));
}

/// Reaching the public web is authority, so it is gated twice: the host must
/// supply a transport, and the scope must hold the same permission that gates
/// the search tool. `fetch` differs from search in needing no configured
/// endpoint, which would otherwise make it the one web surface that appears
/// without the owner deciding anything.
#[tokio::test]
async fn fetch_needs_both_a_host_transport_and_the_scopes_web_permission() {
    let fixture = fixture(false).await;
    let compose = |permissions: BTreeSet<String>, transport: bool| {
        ScopeToolComposition::for_scope_with_permissions_and_project_context(
            AGENT_ID,
            fixture.main_scope.clone(),
            None,
            None,
            &permissions,
            ProjectChatToolContext::default(),
            Some(Arc::new(fixture.provider.clone())),
            ScopeToolRuntime {
                environment: Default::default(),
                command_allowlist: None,
                fetch_transport: transport
                    .then(|| Arc::new(ForgeFetchTransport::new()) as Arc<dyn FetchTransport>),
            },
        )
        .expect("Main chat composition")
        .tools()
        .into_iter()
        .any(|tool| tool.spec().name == FETCH_TOOL_NAME)
    };

    assert!(
        compose(broad_permissions(), true),
        "a Main chat that may propose discovery reaches the public web"
    );
    assert!(
        !compose(broad_permissions(), false),
        "without a host transport there is nothing to compose"
    );
    let mut narrowed = broad_permissions();
    narrowed.remove("propose_discovery");
    assert!(
        !compose(narrowed, true),
        "a scope without the web permission must not receive fetch"
    );
}

/// A Task worker writes code against real dependencies, so it reads their
/// documentation through the same bounded tool — gated on the Task read
/// permission, because a Task that may not read its own record has no
/// business reaching the network either.
#[tokio::test]
async fn a_task_worker_reaches_documentation_only_with_its_read_permission() {
    let fixture = fixture(false).await;
    let compose = |permissions: BTreeSet<String>| {
        ScopeToolComposition::for_scope_with_permissions_and_project_context(
            AGENT_ID,
            CanonicalScope {
                scope_type: CanonicalScopeType::Task,
                scope_id: "scope-composition-task".to_owned(),
                workspace_access: WorkspaceAccess::TaskWrite,
            },
            Some("coder"),
            Some("/tmp/forge-scope-composition-worktree"),
            &permissions,
            ProjectChatToolContext::default(),
            Some(Arc::new(fixture.provider.clone())),
            ScopeToolRuntime {
                environment: Default::default(),
                command_allowlist: None,
                fetch_transport: Some(Arc::new(ForgeFetchTransport::new())),
            },
        )
        .expect("Task composition")
        .tools()
        .into_iter()
        .any(|tool| tool.spec().name == FETCH_TOOL_NAME)
    };

    let mut worker = broad_permissions();
    worker.insert("task_read".to_owned());
    worker.insert("task_write".to_owned());
    assert!(compose(worker.clone()), "a coder may read documentation");

    worker.remove("task_read");
    assert!(
        !compose(worker),
        "a Task without read authority does not get a network tool"
    );
}

#[tokio::test]
async fn registry_reads_use_real_handlers_and_unmoved_reads_keep_the_hand_path() {
    let fixture = fixture(false).await;
    let permissions = broad_permissions();
    for (scope, operation, input, tool_name, expected_field, expected_value) in [
        (
            fixture.main_scope.clone(),
            "account.summary",
            json!({}),
            "forge_scope_read",
            "id",
            AGENT_ID,
        ),
        (
            CanonicalScope {
                scope_type: CanonicalScopeType::AgentChat,
                scope_id: MAIN_CHAT_ID.to_owned(),
                workspace_access: WorkspaceAccess::Deny,
            },
            "agent_chat.summary",
            json!({}),
            "forge_scope_read",
            "id",
            MAIN_CHAT_ID,
        ),
        (
            fixture.project_scope.clone(),
            "project.charter",
            json!({}),
            FORGE_PROJECT_ORCHESTRATION_READ_TOOL,
            "charter_id",
            PROJECT_CHARTER_ID,
        ),
        (
            fixture.project_scope.clone(),
            "skill.section",
            json!({"section":"research"}),
            FORGE_PROJECT_ORCHESTRATION_READ_TOOL,
            "section",
            "research",
        ),
    ] {
        assert!(operation_registry::READ_CATALOG.lookup(operation).is_some());
        let composition = ScopeToolComposition::for_scope_with_permissions(
            AGENT_ID,
            scope,
            None,
            None,
            &permissions,
            Some(Arc::new(fixture.provider.clone())),
        )
        .unwrap();
        for arguments in [
            json!({"operation":operation,"arguments":input}),
            json!({"parameters":{"operation":operation,"arguments":input}}),
        ] {
            let outcome = invoke_tool(&composition, tool_name, arguments, operation)
                .await
                .unwrap();
            assert!(!outcome.is_error, "{operation}: {}", outcome.value);
            let result = if matches!(operation, "project.charter" | "skill.section") {
                &outcome.value["result"]
            } else {
                &outcome.value
            };
            assert_eq!(
                result[expected_field], expected_value,
                "{operation}: {}",
                outcome.value
            );
        }
    }
    // A contract violation is refused before any handler runs, as a tool
    // error naming the operation and the field; the provider enforces the
    // same contract if it is reached directly.
    let composition = ScopeToolComposition::for_scope_with_permissions(
        AGENT_ID,
        fixture.project_scope.clone(),
        None,
        None,
        &permissions,
        Some(Arc::new(fixture.provider.clone())),
    )
    .unwrap();
    for (arguments, expected) in [
        (
            json!({"operation":"skill.section"}),
            "skill.section: argument `section` is required",
        ),
        (
            json!({"parameters":{"operation":"project.charter","arguments":{"limit":1}}}),
            "project.charter: argument `limit` is not admitted",
        ),
    ] {
        let error = invoke_tool(
            &composition,
            FORGE_PROJECT_ORCHESTRATION_READ_TOOL,
            arguments,
            "violation",
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains(expected), "{error}");
    }
    let direct = forge_agent_host::ForgeToolProvider::read(
        &fixture.provider,
        AGENT_ID,
        &fixture.project_scope,
        "skill.section",
        json!({"section":"everything"}),
    )
    .await
    .unwrap_err();
    assert!(
        format!("{direct:?}").contains("skill.section: argument `section` must be one of"),
        "{direct:?}"
    );

    assert!(operation_registry::READ_CATALOG
        .lookup("inbox.read")
        .is_none());
    let composition = ScopeToolComposition::for_scope_with_permissions(
        AGENT_ID,
        fixture.main_scope,
        None,
        None,
        &permissions,
        Some(Arc::new(fixture.provider)),
    )
    .unwrap();
    let outcome = invoke_tool(
        &composition,
        "forge_scope_read",
        json!({"operation":"inbox.read","arguments":{"limit":1}}),
        "unmoved",
    )
    .await
    .unwrap();
    assert!(!outcome.is_error);
    assert!(outcome.value["items"].is_array());
}

#[derive(Debug, Default)]
struct RegistryInquiryRunner(std::sync::Mutex<Vec<services::InquiryRequest>>);
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

fn main_registered_input(id: &str) -> Value {
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

#[tokio::test]
async fn main_registry_reads_preserve_payloads_and_scope_on_each_surface() {
    use forge_agent_host::ForgeToolProvider;
    let fixture = fixture(false).await;
    let runner = Arc::new(RegistryInquiryRunner::default());
    fixture.provider.set_inquiry_runner(runner.clone());
    for scope in [
        fixture.main_scope.clone(),
        CanonicalScope {
            scope_type: CanonicalScopeType::AgentChat,
            scope_id: MAIN_CHAT_ID.to_owned(),
            workspace_access: WorkspaceAccess::Deny,
        },
    ] {
        let composition = ScopeToolComposition::for_scope_with_permissions(
            AGENT_ID,
            scope.clone(),
            None,
            None,
            &broad_permissions(),
            Some(Arc::new(fixture.provider.clone())),
        )
        .unwrap();
        for id in operation_registry::main_reads::IDS {
            if *id == "inquiry.run" && scope.scope_type == CanonicalScopeType::Account {
                continue;
            }
            let input = main_registered_input(id);
            let tool_name = operation_registry::READ_CATALOG
                .lookup(id)
                .unwrap()
                .surfaces[0]
                .native_aggregate;
            let direct = fixture
                .provider
                .read(AGENT_ID, &scope, id, input.clone())
                .await
                .unwrap();
            // The only two values excluded from equality: the fresh server
            // correlation id each read mints for its outcome envelope, and a
            // readiness evaluation's wall-clock timestamp. Everything else in
            // the envelope and the domain payload must be equal.
            let comparable = |mut outcome: Value| {
                if outcome.get("result").is_some() {
                    assert!(outcome["correlation_id"].as_str().is_some(), "{id}");
                    outcome.as_object_mut().unwrap().remove("correlation_id");
                }
                let payload = if outcome.get("result").is_some() {
                    &mut outcome["result"]
                } else {
                    &mut outcome
                };
                if let Some(readiness) = payload.get_mut("readiness") {
                    assert!(readiness["evaluated_at"].as_str().is_some(), "{id}");
                    readiness.as_object_mut().unwrap().remove("evaluated_at");
                }
                outcome
            };
            // The domain payload inside the outcome envelope, where the
            // operation has one.
            let payload = |outcome: &Value| match outcome.get("result") {
                Some(result) => result.clone(),
                None => outcome.clone(),
            };
            // Call the same domain handlers the base dispatcher selected,
            // independently of the registry's handler binding.
            let queries = services::MainOrchestrationQueryService::new(fixture.db.clone());
            let reference = match *id {
                "genesis.project_agents.read" => Some(
                    queries
                        .project_agents(
                            AGENT_ID,
                            &scope,
                            serde_json::from_value(input.clone()).unwrap(),
                        )
                        .await
                        .unwrap(),
                ),
                "charter.read" => Some(
                    queries
                        .charter_read(
                            AGENT_ID,
                            &scope,
                            serde_json::from_value(input.clone()).unwrap(),
                        )
                        .await
                        .unwrap(),
                ),
                "charter.readiness" => Some(
                    queries
                        .charter_readiness(
                            AGENT_ID,
                            &scope,
                            serde_json::from_value(input.clone()).unwrap(),
                        )
                        .await
                        .unwrap(),
                ),
                "charter.diff" => Some(
                    queries
                        .charter_diff(
                            AGENT_ID,
                            &scope,
                            serde_json::from_value(input.clone()).unwrap(),
                        )
                        .await
                        .unwrap(),
                ),
                "charter.approval_target" => Some(
                    queries
                        .charter_approval_target(
                            AGENT_ID,
                            &scope,
                            serde_json::from_value(input.clone()).unwrap(),
                        )
                        .await
                        .unwrap(),
                ),
                "discovery.read" => {
                    assert_eq!(direct["items"][0]["id"], MAIN_GENESIS_ID);
                    None
                }
                "portfolio.read" => {
                    assert_eq!(direct["items"][0]["id"], PROJECT_ID);
                    None
                }
                "inquiry.run" => None,
                _ => unreachable!(),
            };
            if let Some(reference) = reference {
                assert_eq!(
                    comparable(payload(&direct)),
                    comparable(reference),
                    "{id} handler parity"
                );
            }
            for raw in [
                json!({"operation":id,"arguments":input}),
                json!({"parameters":{"operation":id,"arguments":input}}),
            ] {
                let outcome = invoke_tool(&composition, tool_name, raw, "main-registry")
                    .await
                    .unwrap();
                assert!(!outcome.is_error, "{id}: {}", outcome.value);
                assert_eq!(
                    comparable(outcome.value),
                    comparable(direct.clone()),
                    "{id}"
                );
            }
            for denied_scope in [
                fixture.project_scope.clone(),
                CanonicalScope {
                    scope_type: CanonicalScopeType::Task,
                    scope_id: "foreign-task".into(),
                    workspace_access: WorkspaceAccess::Deny,
                },
            ] {
                assert!(
                    fixture
                        .provider
                        .read(AGENT_ID, &denied_scope, id, input.clone())
                        .await
                        .is_err(),
                    "{id}"
                );
            }
            // Same owner, but no active Main binding for this identity. The
            // denial is the same whether or not the arguments satisfy the
            // contract: a denied caller is told nothing about it.
            let denial = |error: forge_agent_host::AgentHostError| match error {
                forge_agent_host::AgentHostError::StructuredOutcome(outcome) => {
                    let mut outcome = serde_json::to_value(&*outcome).unwrap();
                    outcome.as_object_mut().unwrap().remove("correlation_id");
                    outcome.to_string()
                }
                other => format!("{other:?}"),
            };
            let denied = denial(
                fixture
                    .provider
                    .read(PROJECT_AGENT_CANDIDATE_ID, &scope, id, input.clone())
                    .await
                    .unwrap_err(),
            );
            for malformed in [json!({"foreign_project":"x"}), json!(null), json!([])] {
                let probed = denial(
                    fixture
                        .provider
                        .read(PROJECT_AGENT_CANDIDATE_ID, &scope, id, malformed)
                        .await
                        .unwrap_err(),
                );
                assert!(
                    !probed.contains("expected") && !probed.contains("foreign_project"),
                    "{id}: {probed}"
                );
                assert_eq!(probed, denied, "{id}");
            }
            let error = invoke_tool(
                &composition,
                tool_name,
                json!({"operation":id,"arguments":{"foreign_project":"x"}}),
                "invalid-registry",
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(
                error.contains(id)
                    && error.contains("foreign_project")
                    && error.contains("expected"),
                "{error}"
            );
            let error = fixture
                .provider
                .read(AGENT_ID, &scope, id, json!({"foreign_project":"x"}))
                .await
                .unwrap_err();
            let error = format!("{error:?}");
            assert!(
                error.contains(id)
                    && error.contains("foreign_project")
                    && error.contains("expected"),
                "{error}"
            );
        }
    }
    // An integer sent as a string or a float reaches the real handler as the
    // integer: one row, as `limit: 1` returns, not the default page.
    for id in ["discovery.read", "portfolio.read"] {
        let page = |limit: Value| {
            let provider = fixture.provider.clone();
            let scope = fixture.main_scope.clone();
            async move {
                provider
                    .read(AGENT_ID, &scope, id, json!({"limit":limit}))
                    .await
            }
        };
        let one = page(json!(1)).await.unwrap();
        assert_eq!(one["items"].as_array().unwrap().len(), 1, "{id}");
        for spelling in [json!("1"), json!(1.0)] {
            assert_eq!(page(spelling).await.unwrap(), one, "{id}");
        }
        for malformed in [json!("one"), json!(1.5), json!(-1), json!(true)] {
            let error = format!("{:?}", page(malformed).await.unwrap_err());
            assert!(
                error.contains(id) && error.contains("limit") && error.contains("expected"),
                "{error}"
            );
        }
        // Clamping is the handler's: 0 and 1000 are admitted.
        for clamped in [json!(0), json!(1000), json!(null)] {
            page(clamped).await.unwrap();
        }
    }
    {
        let requests = runner.0.lock().unwrap();
        assert_eq!(requests.len(), 3);
        for request in requests.iter() {
            assert_eq!(request.chat_id, MAIN_CHAT_ID);
            assert_eq!(request.identity_id, AGENT_ID);
            assert_eq!(request.account_id, USER_ID);
            assert_eq!(request.title, "Question");
            assert_eq!(request.question, "Find the answer");
            assert_eq!(request.context.as_deref(), Some("Supporting material"));
        }
    }
    assert_eq!(
        fixture
            .provider
            .read(
                AGENT_ID,
                &CanonicalScope {
                    scope_type: CanonicalScopeType::AgentChat,
                    scope_id: MAIN_CHAT_ID.into(),
                    workspace_access: WorkspaceAccess::Deny
                },
                "inquiry.run",
                main_registered_input("inquiry.run")
            )
            .await
            .unwrap()["result"],
        json!({
            "inquiry_id":"registry-inquiry","status":"succeeded","findings":"Bounded findings",
            "findings_path":"inquiries/registry-inquiry/findings.md","duration_ms":15,
            "token_usage":{"input_tokens":11,"output_tokens":12,"cache_read_tokens":13,"cache_write_tokens":14}
        })
    );
}

/// The baseline is the unchanged command boundary called with the exact old
/// adapter's decoded request/envelope (gate main_genesis_commands.rs:3482).
/// The registry must replay that receipt, rather than create a second effect.
#[tokio::test]
async fn main_selection_registry_replays_base_command_receipt_and_preserves_effect() {
    use forge_agent_host::ForgeToolProvider;
    let f = fixture(false).await;
    let base = capture_base_selection(&f).await;
    let frozen_base_receipt: String =
        sqlx::query_scalar("SELECT outcome_json FROM command_receipt WHERE id = ?")
            .bind(&base.receipt_id)
            .fetch_one(f.db.pool())
            .await
            .unwrap();
    // Written from the gate, not emitted by new normalization/preparation.
    let stored: Value = serde_json::from_str(r#"{"operation":"genesis.project_agent.select","payload":{"action":"select","genesis_session_id":"scope-composition-genesis","expected_session_version":1,"project_agent_identity_id":"scope-composition-project-agent-candidate"},"dedupe_key":"pre-change-selection","correlation_id":"pre-change-correlation","causation_id":"pre-change-cause","causation_depth":1}"#).unwrap();
    for prepared in [false, true] {
        let outcome = if prepared {
            ForgeToolProvider::propose_prepared(
                &f.provider,
                AGENT_ID,
                &f.main_scope,
                "session",
                MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
                stored.clone(),
            )
            .await
        } else {
            ForgeToolProvider::propose(
                &f.provider,
                AGENT_ID,
                &f.main_scope,
                "session",
                MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
                stored.clone(),
            )
            .await
        }
        .unwrap();
        assert_eq!(outcome["receipt_id"], base.receipt_id);
        assert_eq!(outcome["result"]["event_id"], base.event_id);
        let mut base_replay = base.result.clone();
        base_replay["replayed"] = json!(true);
        assert_eq!(outcome["result"]["domain_result"], base_replay);
        let frozen: String =
            sqlx::query_scalar("SELECT outcome_json FROM command_receipt WHERE id = ?")
                .bind(&base.receipt_id)
                .fetch_one(f.db.pool())
                .await
                .unwrap();
        assert_eq!(frozen, frozen_base_receipt);
        assert_eq!(outcome["replayed"], true);
    }
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM command_receipt WHERE operation = 'genesis.project_agent.select'",
    )
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    assert_eq!(count, 1);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_event WHERE event_type = 'product_genesis.project_agent_selected'").fetch_one(f.db.pool()).await.unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn main_proposal_dispatch_denies_unbound_and_forged_callers_before_contract_detail() {
    use forge_agent_host::ForgeToolProvider;
    let f = fixture(false).await;
    for operation in operation_registry::main_proposals::IDS {
        for actor in [AGENT_ID, PROJECT_AGENT_CANDIDATE_ID] {
            for field in ["identity_id", "authority", "unexpected"] {
                let mut args = json!({"operation":operation,"payload":{field:"forged"},"dedupe_key":"denial-key","correlation_id":"corr"});
                if field == "unexpected" {
                    args["payload"] = json!(null);
                }
                let composition = ScopeToolComposition::for_scope_with_permissions(
                    actor,
                    f.main_scope.clone(),
                    None,
                    None,
                    &broad_permissions(),
                    Some(Arc::new(f.provider.clone())),
                )
                .unwrap();
                let tool = composition
                    .tools()
                    .into_iter()
                    .find(|tool| tool.spec().name == FORGE_MAIN_ORCHESTRATION_PROPOSE_TOOL)
                    .unwrap();
                let prepared_error = tool
                    .prepare(args.clone(), &preparation_context("denial-order"))
                    .await
                    .unwrap_err()
                    .to_string();
                assert_eq!(
                    prepared_error.contains("expected "),
                    actor == AGENT_ID && field == "unexpected",
                    "{operation} {actor} {field}: {prepared_error}"
                );
                let error = ForgeToolProvider::propose(
                    &f.provider,
                    actor,
                    &f.main_scope,
                    "session",
                    operation,
                    args,
                )
                .await
                .unwrap_err();
                let text = format!("{error:?}");
                if actor != AGENT_ID || field != "unexpected" {
                    assert!(
                        !text.contains("expected "),
                        "{operation} {actor} {field}: {text}"
                    );
                    let forge_agent_host::AgentHostError::StructuredOutcome(outcome) = error else {
                        panic!("{text}");
                    };
                    assert_eq!(
                        serde_json::to_value(outcome).unwrap()["code"],
                        "policy_denied"
                    );
                } else {
                    assert!(
                        text.contains(operation) && text.contains("expected "),
                        "{text}"
                    );
                }
            }
        }
    }
    // A revoked current policy also denies valid and malformed payloads equally.
    sqlx::query("UPDATE agent_identity SET account_permission_ceiling = '{}' WHERE id = ?")
        .bind(AGENT_ID)
        .execute(f.db.pool())
        .await
        .unwrap();
    for operation in operation_registry::main_proposals::IDS {
        let error = ForgeToolProvider::propose(
            &f.provider,
            AGENT_ID,
            &f.main_scope,
            "session",
            operation,
            json!({"payload":{},"dedupe_key":"denial-key","correlation_id":"corr"}),
        )
        .await
        .unwrap_err();
        assert!(!format!("{error:?}").contains("expected "));
    }
}

#[tokio::test]
async fn main_create_registry_preserves_base_pending_proposal_and_exact_dedupe() {
    use forge_agent_host::ForgeToolProvider;
    let f = fixture(false).await;
    // Capture the original enqueue boundary's values, without the registry.
    let payload = r#"{"action":"create_from_approval","approval_id":"pre-change-approval"}"#;
    let actions = services::AgentActionService::new(f.db.clone());
    let base = actions
        .propose(services::ProposeActionInput {
            id: Some("pre-change-main-create-action".into()),
            actor_identity_id: AGENT_ID.into(),
            scope_type: "account".into(),
            scope_id: USER_ID.into(),
            operation: MAIN_PROJECT_CREATE_OPERATION.into(),
            payload_json: payload.into(),
            dedupe_key: "pre-change-create".into(),
            correlation_id: "pre-change-correlation".into(),
            causation_id: Some("pre-change-cause".into()),
            causation_depth: 1,
            requested_permission: "propose_project".into(),
            policy_reason: None,
            target_type: Some("account".into()),
            target_id: Some(USER_ID.into()),
        })
        .await
        .unwrap();
    assert_eq!(
        base.policy_result,
        db::AgentActionPolicyResult::ApprovalRequired
    );
    assert_eq!(base.status, db::AgentActionStatus::PendingApproval);
    let stored: Value = serde_json::from_str(r#"{"operation":"project.create","payload":{"action":"create_from_approval","approval_id":"pre-change-approval"},"dedupe_key":"pre-change-create","correlation_id":"pre-change-correlation","causation_id":"pre-change-cause","causation_depth":1}"#).unwrap();
    for prepared in [false, true] {
        let outcome = if prepared {
            ForgeToolProvider::propose_prepared(
                &f.provider,
                AGENT_ID,
                &f.main_scope,
                "session",
                MAIN_PROJECT_CREATE_OPERATION,
                stored.clone(),
            )
            .await
        } else {
            ForgeToolProvider::propose(
                &f.provider,
                AGENT_ID,
                &f.main_scope,
                "session",
                MAIN_PROJECT_CREATE_OPERATION,
                stored.clone(),
            )
            .await
        }
        .unwrap();
        assert_eq!(outcome["code"], "approval_required");
        assert_eq!(outcome["status"], "approval_required");
        assert_eq!(outcome["approval_target"]["target_id"], USER_ID);
        assert!(outcome["receipt_id"].is_null());
        let replay = db::AgentActionRepo::get_action(&*f.db, &base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replay, base);
    }
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_action WHERE operation = 'project.create'")
            .fetch_one(f.db.pool())
            .await
            .unwrap();
    assert_eq!(count, 1);
    // There is no command receipt or domain creation from a direct proposal.
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM command_receipt WHERE operation = 'project.create'",
    )
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    assert_eq!(count, 0);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM project_charter_approval")
        .fetch_one(f.db.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
}

async fn capture_base_selection(f: &Fixture) -> services::MainGenesisProjectAgentSelectResult {
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

#[path = "common/pre_change_main_runtime.rs"]
mod pre_change_main_runtime;

#[tokio::test]
async fn main_selection_literal_checkpoints_resume_real_provider_and_same_base_receipt() {
    let fixtures: Value = serde_json::from_str(include_str!(
        "common/main_proposal_checkpoints_pre_change.json"
    ))
    .unwrap();
    for form in ["prepared", "approval_pending", "approval_edited"] {
        let f = fixture(false).await;
        let base = capture_base_selection(&f).await;
        let original: (String, String, String) =
            sqlx::query_as("SELECT id,event_id,outcome_json FROM command_receipt WHERE id = ?")
                .bind(&base.receipt_id)
                .fetch_one(f.db.pool())
                .await
                .unwrap();
        let composition = ScopeToolComposition::for_scope_with_permissions(
            AGENT_ID,
            f.main_scope.clone(),
            None,
            None,
            &BTreeSet::from(["propose_discovery".into()]),
            Some(Arc::new(f.provider.clone())),
        )
        .unwrap();
        pre_change_main_runtime::resume(
            composition,
            &fixtures[format!("genesis.project_agent.select:{form}")]["checkpoint"],
        )
        .await;
        let replay: (String, String, String) =
            sqlx::query_as("SELECT id,event_id,outcome_json FROM command_receipt WHERE id = ?")
                .bind(&base.receipt_id)
                .fetch_one(f.db.pool())
                .await
                .unwrap();
        assert_eq!(original, replay, "{form}: exact base receipt bytes");
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM command_receipt WHERE operation = 'genesis.project_agent.select'",
        )
        .fetch_one(f.db.pool())
        .await
        .unwrap();
        assert_eq!(count, 1);
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_event WHERE event_type = 'product_genesis.project_agent_selected'").fetch_one(f.db.pool()).await.unwrap();
        assert_eq!(count, 1);
    }
}

/// The base provider read authority replacements only from payload. These
/// envelope fields were ignored by direct provider callers; the registry's
/// dispatch guard now rejects them, just as named native preparation already did.
#[tokio::test]
async fn main_proposal_root_authority_fields_are_denied_before_payload_contracts() {
    use forge_agent_host::ForgeToolProvider;
    let f = fixture(false).await;
    let fields = [
        "actor_identity_id",
        "identity_id",
        "scope_type",
        "scope_id",
        "project_id",
        "authority",
        "permission",
        "workspace",
        "workspace_path",
        "workspace_lease",
        "repository_path",
        "repository_url",
        "credential",
        "target_type",
        "target_id",
    ];
    for operation in operation_registry::main_proposals::IDS {
        for actor in [AGENT_ID, PROJECT_AGENT_CANDIDATE_ID] {
            let composition = ScopeToolComposition::for_scope_with_permissions(
                actor,
                f.main_scope.clone(),
                None,
                None,
                &broad_permissions(),
                Some(Arc::new(f.provider.clone())),
            )
            .unwrap();
            let tool = composition
                .tools()
                .into_iter()
                .find(|tool| tool.spec().name == FORGE_MAIN_ORCHESTRATION_PROPOSE_TOOL)
                .unwrap();
            for field in fields {
                let mut arguments = json!({"operation":operation,"payload":null,"dedupe_key":"root-authority","correlation_id":"root-correlation"});
                arguments[field] = json!("ignored-envelope-value");
                // Even a malformed payload does not reveal contract details.
                let error = tool
                    .prepare(arguments.clone(), &preparation_context("root-authority"))
                    .await
                    .unwrap_err()
                    .to_string();
                assert!(
                    !error.contains("expected "),
                    "{operation} {actor} {field}: {error}"
                );
                let error = ForgeToolProvider::propose(
                    &f.provider,
                    actor,
                    &f.main_scope,
                    "session",
                    operation,
                    arguments,
                )
                .await
                .unwrap_err();
                let forge_agent_host::AgentHostError::StructuredOutcome(outcome) = error else {
                    panic!("unexpected error");
                };
                assert_eq!(
                    serde_json::to_value(&*outcome).unwrap()["code"],
                    "policy_denied"
                );
                assert!(!outcome.safe_message.contains("expected "));
            }
        }
    }
}

/// Selection through the provider: the former `action` is optional, an
/// integer spelling of the session version reaches the command as the integer,
/// both forms hit the one receipt, and a stale version is refused by the
/// unchanged command service with no second receipt.
#[tokio::test]
async fn main_selection_dispatch_accepts_spellings_and_refuses_a_stale_version() {
    use forge_agent_host::ForgeToolProvider;
    let f = fixture(false).await;
    let call = |version: Value, action: Option<&str>, key: &str| {
        let mut payload = json!({
            "genesis_session_id": MAIN_GENESIS_ID,
            "expected_session_version": version,
            "project_agent_identity_id": PROJECT_AGENT_CANDIDATE_ID
        });
        if let Some(action) = action {
            payload["action"] = json!(action);
        }
        json!({"operation":MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,"payload":payload,"dedupe_key":key,"correlation_id":"spelling-correlation"})
    };
    let mut receipts = Vec::new();
    for (version, action) in [
        (json!(1), None),
        (json!("1"), Some("select")),
        (json!(1.0), Some("anything")),
    ] {
        let outcome = ForgeToolProvider::propose(
            &f.provider,
            AGENT_ID,
            &f.main_scope,
            "session",
            MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
            call(version, action, "spelling-key"),
        )
        .await
        .unwrap();
        assert_eq!(outcome["code"], "ok", "{outcome}");
        receipts.push((
            outcome["receipt_id"].clone(),
            outcome["result"]["event_id"].clone(),
        ));
    }
    assert!(receipts[0].0.is_string());
    assert!(receipts.iter().all(|receipt| receipt == &receipts[0]));
    // The session moved to version 2. A new key at version 1 is stale.
    let stale = ForgeToolProvider::propose(
        &f.provider,
        AGENT_ID,
        &f.main_scope,
        "session",
        MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
        call(json!("1"), None, "stale-key"),
    )
    .await
    .unwrap_err();
    let text = format!("{stale:?}");
    assert!(
        !text.contains("expected genesis.project_agent.select"),
        "{text}"
    );
    for malformed in [json!("one"), json!(1.5), json!(true), json!(0)] {
        let error = ForgeToolProvider::propose(
            &f.provider,
            AGENT_ID,
            &f.main_scope,
            "session",
            MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
            call(malformed, None, "malformed-key"),
        )
        .await
        .unwrap_err();
        let text = format!("{error:?}");
        assert!(text.contains("expected_session_version"), "{text}");
    }
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM command_receipt WHERE operation = 'genesis.project_agent.select'",
    )
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    assert_eq!(count, 1);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_event WHERE event_type = 'product_genesis.project_agent_selected'").fetch_one(f.db.pool()).await.unwrap();
    assert_eq!(count, 1);
}

/// A new Project-create call without a usable Charter approval reference is
/// refused at preparation and at dispatch and queues nothing. A reference the
/// caller invents still only queues a pending action: nothing executes.
#[tokio::test]
async fn main_create_requires_an_approval_reference_and_only_ever_queues() {
    use forge_agent_host::ForgeToolProvider;
    let f = fixture(false).await;
    let composition = ScopeToolComposition::for_scope_with_permissions(
        AGENT_ID,
        f.main_scope.clone(),
        None,
        None,
        &broad_permissions(),
        Some(Arc::new(f.provider.clone())),
    )
    .unwrap();
    let tool = composition
        .tools()
        .into_iter()
        .find(|tool| tool.spec().name == FORGE_MAIN_ORCHESTRATION_PROPOSE_TOOL)
        .unwrap();
    for payload in [
        json!({}),
        json!({"action":"create_from_approval"}),
        json!({"approval_id":null}),
        json!({"approval_id":""}),
        json!({"approval_id":7}),
    ] {
        let arguments = json!({"operation":MAIN_PROJECT_CREATE_OPERATION,"payload":payload,"dedupe_key":"no-reference","correlation_id":"no-reference"});
        let error = tool
            .prepare(arguments.clone(), &preparation_context("no-reference"))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("argument `approval_id`")
                && error.ends_with("expected project.create: {approval_id}"),
            "{error}"
        );
        let error = ForgeToolProvider::propose(
            &f.provider,
            AGENT_ID,
            &f.main_scope,
            "session",
            MAIN_PROJECT_CREATE_OPERATION,
            arguments,
        )
        .await
        .unwrap_err();
        let text = format!("{error:?}");
        assert!(text.contains("approval_id"), "{text}");
    }
    let queued = |f: &Fixture| {
        let pool = f.db.pool().clone();
        async move {
            sqlx::query_as::<_, (String, String)>(
                "SELECT status, policy_result FROM agent_action WHERE operation = 'project.create'",
            )
            .fetch_all(&pool)
            .await
            .unwrap()
        }
    };
    assert!(queued(&f).await.is_empty());
    // `project_id` is refused at preparation too, not only at dispatch.
    let error = tool
        .prepare(
            json!({"operation":MAIN_PROJECT_CREATE_OPERATION,"payload":{"approval_id":"invented","project_id":"forged"},"dedupe_key":"forged","correlation_id":"forged"}),
            &preparation_context("forged-project"),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("server-derived"), "{error}");
    let outcome = ForgeToolProvider::propose(
        &f.provider,
        AGENT_ID,
        &f.main_scope,
        "session",
        MAIN_PROJECT_CREATE_OPERATION,
        json!({"operation":MAIN_PROJECT_CREATE_OPERATION,"payload":{"approval_id":"invented"},"dedupe_key":"invented","correlation_id":"invented"}),
    )
    .await
    .unwrap();
    assert_eq!(outcome["code"], "approval_required", "{outcome}");
    assert_eq!(
        queued(&f).await,
        vec![(
            "pending_approval".to_owned(),
            "approval_required".to_owned()
        )]
    );
    for query in [
        "SELECT COUNT(*) FROM project_charter_approval",
        "SELECT COUNT(*) FROM command_receipt WHERE operation = 'project.create'",
    ] {
        let count: i64 = sqlx::query_scalar(query)
            .fetch_one(f.db.pool())
            .await
            .unwrap();
        assert_eq!(count, 0, "{query}");
    }
}

/// Each case still uses the hand path when its literal fixture is captured.
#[tokio::test]
async fn project_registry_base_project_review_config() {
    capture_project_proposal_case("project.review_config").await;
}
#[tokio::test]
async fn project_registry_base_project_document() {
    capture_project_proposal_case("project.document").await;
}
#[tokio::test]
async fn project_registry_base_project_decision() {
    capture_project_proposal_case("project.decision").await;
}
#[tokio::test]
async fn project_registry_base_project_milestone() {
    capture_project_proposal_case("project.milestone").await;
}
#[tokio::test]
async fn project_registry_base_project_validation() {
    capture_project_proposal_case("project.validation").await;
}
#[tokio::test]
async fn project_registry_base_project_release_request() {
    capture_project_proposal_case("project.release.request").await;
}
#[tokio::test]
async fn project_registry_base_project_escalate() {
    capture_project_proposal_case("project.escalate").await;
}
#[tokio::test]
async fn project_registry_base_message_send() {
    capture_project_proposal_case("message.send").await;
}
#[tokio::test]
async fn project_registry_base_commitment_update() {
    capture_project_proposal_case("commitment.update").await;
}
#[tokio::test]
async fn project_registry_base_memory_publish() {
    capture_project_proposal_case("memory.publish").await;
}
#[tokio::test]
async fn project_registry_base_memory_supersede() {
    capture_project_proposal_case("memory.supersede").await;
}
#[tokio::test]
async fn project_registry_base_review_request() {
    capture_project_proposal_case("review.request").await;
}
#[tokio::test]
async fn project_registry_base_session_action() {
    capture_project_proposal_case("session.action").await;
}
async fn capture_project_proposal_case(operation: &'static str) {
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
        let fixtures: Value =
            serde_json::from_str(include_str!("fixtures/project_registry_base.json")).unwrap();
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
        let fixtures: Value =
            serde_json::from_str(include_str!("fixtures/project_registry_base.json")).unwrap();
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
    let fixtures: Value =
        serde_json::from_str(include_str!("fixtures/project_registry_base.json")).unwrap();
    assert_eq!(
        snapshot, fixtures[operation],
        "{operation} hand-path fixture"
    );
}

fn normalized_generated_values(value: Value) -> Value {
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

#[tokio::test]
async fn pending_legacy_replay_cannot_claim_a_receiptless_completed_effect() {
    use forge_agent_host::ForgeToolProvider;
    let f = fixture(false).await;
    let args = json!({"operation":"message.send","payload":{"content":"Pending intent"},"dedupe_key":"legacy-completion","correlation_id":"legacy-completion"});
    let pending = f
        .provider
        .propose(
            AGENT_ID,
            &f.project_scope,
            "session",
            "message.send",
            args.clone(),
        )
        .await
        .unwrap();
    assert_eq!(pending["status"], "pending_approval");
    sqlx::query("UPDATE agent_action SET status='executed',version=version+1 WHERE id=?")
        .bind(pending["id"].as_str().unwrap())
        .execute(f.db.pool())
        .await
        .unwrap();
    let error = f
        .provider
        .propose(AGENT_ID, &f.project_scope, "session", "message.send", args)
        .await
        .unwrap_err();
    assert!(!format!("{error:?}").contains("status: Succeeded"));
}
