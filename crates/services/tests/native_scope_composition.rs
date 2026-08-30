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
use api_types::WorkflowTrigger;
use db::{
    create_sqlite_pool, run_migrations, AgentRepo, AgentStatus, CreateAgentIdentity,
    CreateAgentProfile, CreateProject, CreateRepo, ProjectRepo, RepoRepo, SqliteDb, UpdateProject,
};
use events::EventBus;
use forge_agent_host::{
    CanonicalScope, CanonicalScopeType, ProjectChatToolContext, ScopeToolComposition,
    WorkspaceAccess, FORGE_MAIN_ORCHESTRATION_PROPOSE_TOOL, FORGE_MAIN_ORCHESTRATION_READ_TOOL,
    FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL, FORGE_PROJECT_ORCHESTRATION_READ_TOOL,
    MAIN_CHARTER_APPROVAL_TARGET_OPERATION, MAIN_CHARTER_DIFF_OPERATION,
    MAIN_CHARTER_DRAFT_OPERATION, MAIN_CHARTER_READINESS_OPERATION, MAIN_CHARTER_READ_OPERATION,
    MAIN_GENESIS_PROJECT_AGENTS_READ_OPERATION, MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
    MAIN_GENESIS_START_OPERATION, MAIN_PROJECT_CREATE_OPERATION, MIGRATED_OPERATION_CONTRACTS,
    PROJECT_CHARTER_ADOPTION_OPERATION, PROJECT_CURRENT_STATE_OPERATION,
    PROJECT_DECISION_OPERATION, PROJECT_DOCUMENT_OPERATION, PROJECT_EVIDENCE_OPERATION,
    PROJECT_MILESTONE_OPERATION, PROJECT_OBSERVATIONS_OPERATION, PROJECT_READINESS_OPERATION,
    PROJECT_RELEASE_OPERATION, PROJECT_VALIDATION_OPERATION, TASK_ADAPTIVE_OPERATION,
    TASK_EVIDENCE_OPERATION, TASK_PROPOSE_OPERATION, TASK_RECOVER_OPERATION, TASK_REVIEW_OPERATION,
    TASK_WORKLOG_OPERATION,
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
        provider.set_task_service(Arc::new(TaskService::new(
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
            remote_url: "file:///tmp/scope-composition-repo".to_owned(),
            local_path: None,
            work_mode: db::WorkMode::DirectMerge,
            default_branch: "main".to_owned(),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("repository");
    ProjectRepo::update(
        db,
        UpdateProject {
            id: PROJECT_ID.to_owned(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(REPO_ID.to_owned())),
            paused_at: None,
            updated_at: NOW.to_owned(),
        },
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
            "priority": 3,
            "merge_config": null,
            "role_assignments": null,
            "governance": null
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
            project_proposal_arguments(
                MAIN_PROJECT_CREATE_OPERATION,
                "create_from_approval",
                "matrix-main-project",
            ),
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
        !ready_operations
            .iter()
            .any(|value| value == PROJECT_CHARTER_ADOPTION_OPERATION),
        "setup-only adoption must be absent from a ready Project scope"
    );

    let project_proposals = [
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
    for (operation, arguments) in project_proposals {
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
            "operation": TASK_REVIEW_OPERATION,
            "payload": {
                "task_id": source_task_id,
                "decision": "reject",
                "expected_task_version": review_task_version,
                "reason": "Exercise the Project Agent human-review decision."
            },
            "dedupe_key": "matrix-task-review",
            "correlation_id": "correlation-matrix-task-review"
        }),
        TASK_REVIEW_OPERATION,
    )
    .await
    .expect("task.review composition call");
    assert_outcome_operation(&review, TASK_REVIEW_OPERATION);
    assert!(
        !review.is_error,
        "Task review should commit: {}",
        review.value
    );
    assert_eq!(review.value["result"]["task_status"], "cancelled");
    covered_operations.insert(TASK_REVIEW_OPERATION.to_owned());

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
    // rather than by invocation. `project.observations` and `task.recover` are
    // Project-scoped; `task.worklog` and `task.evidence` are Task-scoped and
    // need a leased Task session this fixture does not build. Exposure is the
    // property this test is named for: every migrated contract must be
    // surfaced by scope composition in a scope that supports it.
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
    assert!(
        project_read_operations
            .iter()
            .any(|value| value == PROJECT_OBSERVATIONS_OPERATION),
        "Project read composition must expose {PROJECT_OBSERVATIONS_OPERATION}"
    );
    covered_operations.insert(PROJECT_OBSERVATIONS_OPERATION.to_owned());

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
            .any(|value| value == TASK_RECOVER_OPERATION),
        "Project propose composition must expose {TASK_RECOVER_OPERATION}"
    );
    covered_operations.insert(TASK_RECOVER_OPERATION.to_owned());

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
    for operation in [TASK_WORKLOG_OPERATION, TASK_EVIDENCE_OPERATION] {
        assert!(
            task_propose_operations
                .iter()
                .any(|value| value == operation),
            "Task propose composition must expose {operation}"
        );
        covered_operations.insert(operation.to_owned());
    }

    let expected_operations = MIGRATED_OPERATION_CONTRACTS
        .iter()
        .map(|contract| contract.operation.to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(covered_operations, expected_operations);
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

    // Restore the binding for the version and idempotency cases.  The
    // composition remains the same server-derived registry; only the durable
    // policy row is changed between calls.
    sqlx::query(
        "UPDATE project_agent_binding SET permission_ceiling_json = ?
         WHERE project_id = ? AND state = 'active'",
    )
    .bind(broad_permission_json())
    .bind(PROJECT_ID)
    .execute(fixture.db.pool())
    .await
    .expect("restore Project policy");

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
