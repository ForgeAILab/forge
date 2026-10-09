//! Gate A / task 2.10 acceptance coverage for operation classification.
//!
//! These tests enter the same public provider boundary used by native Agent
//! Runtime tools.  They intentionally do not call an AgentAction materializer
//! directly: query operations must stay read-only, safe Project coordination
//! writes and `task.propose` must use a direct command receipt, and
//! consequential/denied operations must stop at the policy boundary.

use std::sync::Arc;

use db::{
    create_sqlite_pool, run_migrations, AgentRepo, AgentStatus, CreateAgentIdentity,
    CreateAgentProfile, CreateProject, CreateRepo, ProjectRepo, RepoRepo, SqliteDb, UpdateProject,
};
use events::EventBus;
use forge_agent_host::{
    classify_operation, operation_descriptor, AgentHostError, CanonicalScope, CanonicalScopeType,
    ForgeToolProvider, OperationClassification, WorkspaceAccess, PROJECT_CURRENT_STATE_OPERATION,
    PROJECT_DOCUMENT_OPERATION, PROJECT_MILESTONE_OPERATION,
};
use serde_json::{json, Value};
use services::{CoordinationToolProvider, OrchestrationAuthorizationService, TaskService};
use sqlx::Row;

const USER_ID: &str = "direct-classification-user";
const AGENT_ID: &str = "direct-classification-project-agent";
const PROFILE_ID: &str = "direct-classification-project-agent-profile";
const PROJECT_ID: &str = "direct-classification-project";
const TASK_PROJECT_ID: &str = "direct-classification-task-project";
const REPO_ID: &str = "direct-classification-repo";
const TASK_REPO_ID: &str = "direct-classification-task-repo";
const CHARTER_ID: &str = "direct-classification-charter";
const CHARTER_REVISION_ID: &str = "direct-classification-charter-revision";
const TASK_CHARTER_ID: &str = "direct-classification-task-charter";
const TASK_CHARTER_REVISION_ID: &str = "direct-classification-task-charter-revision";
const MILESTONE_ID: &str = "direct-classification-milestone";
const MILESTONE_REVISION_ID: &str = "direct-classification-milestone-revision";
const RUNTIME_SESSION_ID: &str = "direct-classification-runtime-session";
const NOW: &str = "2026-08-21T00:00:00.000Z";

struct Fixture {
    db: Arc<SqliteDb>,
    provider: CoordinationToolProvider,
    project_scope: CanonicalScope,
    task_project_scope: CanonicalScope,
}

async fn database() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    Arc::new(SqliteDb::new(pool))
}

async fn seed_identity(db: &SqliteDb, permissions: &str) {
    sqlx::query(
        "INSERT INTO user
         (id, email, password_hash, display_name, created_at, updated_at)
         VALUES (?, ?, 'test', 'Direct command classification user', ?, ?)",
    )
    .bind(USER_ID)
    .bind("direct-classification@example.test")
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
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

async fn seed_project(db: &SqliteDb, project_id: &str, repo_id: &str) {
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

async fn seed_charter(
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

async fn seed_milestone(db: &SqliteDb) {
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

async fn fixture() -> Fixture {
    fixture_with_permissions(
        r#"{"permissions":["read_project","propose_project","propose_task","propose_commitment"]}"#,
    )
    .await
}

async fn fixture_with_permissions(permissions: &str) -> Fixture {
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

fn research_content() -> Value {
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

fn document_arguments(key: &str) -> Value {
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

fn task_arguments(key: &str) -> Value {
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

fn approval_required_release_arguments(key: &str) -> Value {
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

fn milestone_content() -> Value {
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

fn stale_milestone_arguments(key: &str, expected_version: i64) -> Value {
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

fn assert_success_envelope(
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

fn structured_error(error: AgentHostError) -> api_types::OrchestrationOutcome {
    match error {
        AgentHostError::StructuredOutcome(outcome) => *outcome,
        other => panic!("expected a structured Forge outcome, got {other:?}"),
    }
}

#[test]
fn canonical_catalog_classifies_query_direct_approval_and_denied_operations() {
    assert_eq!(
        classify_operation(PROJECT_CURRENT_STATE_OPERATION, None),
        OperationClassification::Query
    );
    assert_eq!(
        classify_operation("task.propose", None),
        OperationClassification::DirectCommand
    );
    assert_eq!(
        classify_operation(
            PROJECT_DOCUMENT_OPERATION,
            Some(&json!({"action": "draft_revision"}))
        ),
        OperationClassification::DirectCommand
    );
    assert_eq!(
        classify_operation("commitment.update", None),
        OperationClassification::ApprovalRequiredAction
    );
    let denied = operation_descriptor(CanonicalScopeType::Project, "release.execute", None);
    assert_eq!(denied.classification, OperationClassification::Denied);
    assert!(!denied.is_exposed());
    assert_eq!(denied.required_permission, None);
}

async fn count(db: &SqliteDb, sql: &str) -> i64 {
    sqlx::query_scalar(sql)
        .fetch_one(db.pool())
        .await
        .expect("count")
}

#[tokio::test]
async fn named_native_operations_return_stable_success_envelopes() {
    let fixture = fixture().await;

    let query = fixture
        .provider
        .read(
            AGENT_ID,
            &fixture.project_scope,
            PROJECT_CURRENT_STATE_OPERATION,
            json!({"limit": 10}),
        )
        .await
        .expect("structured Project query");
    assert_success_envelope(
        &query,
        PROJECT_CURRENT_STATE_OPERATION,
        "project",
        PROJECT_ID,
        "",
    );
    assert_eq!(
        query["result"]["effective_state"]["project"]["id"],
        PROJECT_ID
    );

    let direct_arguments = document_arguments("structured-direct-success-key");
    let direct = fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.project_scope,
            RUNTIME_SESSION_ID,
            PROJECT_DOCUMENT_OPERATION,
            direct_arguments.clone(),
        )
        .await
        .expect("structured direct Project command");
    assert_success_envelope(
        &direct,
        PROJECT_DOCUMENT_OPERATION,
        "project",
        PROJECT_ID,
        direct_arguments["correlation_id"].as_str().unwrap(),
    );
    assert_eq!(direct["result"]["domain_committed"], true);
    assert!(direct["result"]["receipt_id"].as_str().is_some());

    let approval_arguments = approval_required_release_arguments("structured-approval-key");
    let approval = fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.project_scope,
            RUNTIME_SESSION_ID,
            "project.release.request",
            approval_arguments.clone(),
        )
        .await
        .expect("structured approval proposal");
    assert_eq!(approval["code"], "approval_required");
    assert_eq!(approval["status"], "approval_required");
    assert_eq!(approval["operation"], "project.release.request");
    assert_eq!(approval["scope"]["scope_type"], "project");
    assert_eq!(approval["scope"]["scope_id"], PROJECT_ID);
    assert_eq!(
        approval["correlation_id"],
        approval_arguments["correlation_id"]
    );
    let target = &approval["approval_target"];
    assert!(target["target_type"].as_str().is_some());
    assert!(target["target_id"].as_str().is_some());
    assert_eq!(target["operation"], "project.release.request");
    assert_eq!(target["requires_user_authorization"], true);
    assert!(
        approval["result"].is_null(),
        "approval has no domain result"
    );
    assert!(approval["domain_result"].is_null());
}

fn result_field<'a>(value: &'a Value, field: &str) -> Option<&'a Value> {
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

#[tokio::test]
async fn query_operations_do_not_create_action_receipt_or_event() {
    let fixture = fixture().await;
    let action_count_before = count(&fixture.db, "SELECT COUNT(*) FROM agent_action").await;
    let receipt_count_before = count(&fixture.db, "SELECT COUNT(*) FROM command_receipt").await;
    let event_count_before = count(&fixture.db, "SELECT COUNT(*) FROM domain_event").await;
    let result = fixture
        .provider
        .read(
            AGENT_ID,
            &fixture.project_scope,
            PROJECT_CURRENT_STATE_OPERATION,
            json!({"limit": 10}),
        )
        .await
        .expect("Project current-state query");
    assert_success_envelope(
        &result,
        PROJECT_CURRENT_STATE_OPERATION,
        "project",
        PROJECT_ID,
        "",
    );
    assert_eq!(result["result"]["scope"], "project");
    assert_eq!(
        result["result"]["effective_state"]["project"]["id"],
        PROJECT_ID
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM agent_action").await,
        action_count_before,
        "queries never enter the approval queue"
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM command_receipt").await,
        receipt_count_before,
        "queries have no side-effecting command receipt"
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM domain_event").await,
        event_count_before,
        "queries do not append a domain event"
    );
}

#[tokio::test]
async fn shared_authorization_resolves_project_and_project_chat_targets() {
    let fixture = fixture().await;
    let authorization = OrchestrationAuthorizationService::new(Arc::clone(&fixture.db));

    assert_eq!(
        authorization
            .direct_project_target(&fixture.project_scope)
            .await
            .expect("Project direct target"),
        PROJECT_ID
    );
    assert_eq!(
        authorization
            .project_orchestration_target(AGENT_ID, &fixture.project_scope)
            .await
            .expect("Project orchestration target"),
        PROJECT_ID
    );

    let chat_id = sqlx::query_scalar::<_, String>(
        "SELECT id FROM agent_chat WHERE project_id = ? AND kind = 'project' LIMIT 1",
    )
    .bind(PROJECT_ID)
    .fetch_one(fixture.db.pool())
    .await
    .expect("Project chat");
    let chat_scope = CanonicalScope {
        scope_type: CanonicalScopeType::AgentChat,
        scope_id: chat_id,
        workspace_access: WorkspaceAccess::Deny,
    };
    assert_eq!(
        authorization
            .direct_project_target(&chat_scope)
            .await
            .expect("Project chat direct target"),
        PROJECT_ID
    );
    assert_eq!(
        authorization
            .project_orchestration_target(AGENT_ID, &chat_scope)
            .await
            .expect("Project chat orchestration target"),
        PROJECT_ID
    );

    assert!(
        authorization
            .project_orchestration_target("unknown-agent", &fixture.project_scope)
            .await
            .is_err(),
        "scope authorization must reject an actor without an active Project binding"
    );
}

#[tokio::test]
async fn allowed_project_write_and_task_proposal_use_direct_receipts_without_actions() {
    let fixture = fixture().await;
    let document = fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.project_scope,
            RUNTIME_SESSION_ID,
            PROJECT_DOCUMENT_OPERATION,
            document_arguments("direct-document-key"),
        )
        .await
        .expect("direct Project document command");
    assert_success_envelope(
        &document,
        PROJECT_DOCUMENT_OPERATION,
        "project",
        PROJECT_ID,
        "correlation-direct-document-key",
    );
    assert_eq!(document["result"]["domain_committed"], true);
    assert!(result_field(&document, "receipt_id")
        .and_then(Value::as_str)
        .is_some());
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM agent_action").await,
        0,
        "allowed Project writes do not create an AgentAction"
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM command_receipt WHERE operation = 'project.document'",
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM project_document_revision",
        )
        .await,
        1
    );
    let document_receipt = sqlx::query(
        "SELECT event_id, agent_action_execution_id, outcome_json
         FROM command_receipt
         WHERE operation = 'project.document' AND idempotency_key = 'direct-document-key'",
    )
    .fetch_one(fixture.db.pool())
    .await
    .expect("direct document receipt");
    assert!(document_receipt
        .get::<Option<String>, _>("agent_action_execution_id")
        .is_none());
    let document_event = sqlx::query(
        "SELECT event_type, entity_type, scope_type, scope_id
         FROM domain_event WHERE id = ?",
    )
    .bind(document_receipt.get::<String, _>("event_id"))
    .fetch_one(fixture.db.pool())
    .await
    .expect("direct document event");
    assert_eq!(
        document_event.get::<String, _>("event_type"),
        "project.document.revision_created"
    );
    assert_eq!(
        document_event.get::<String, _>("entity_type"),
        "project_document_revision"
    );
    assert_eq!(document_event.get::<String, _>("scope_type"), "project");
    assert_eq!(document_event.get::<String, _>("scope_id"), PROJECT_ID);

    let task = fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.task_project_scope,
            RUNTIME_SESSION_ID,
            "task.propose",
            task_arguments("direct-task-key"),
        )
        .await
        .expect("direct Task proposal command");
    assert_success_envelope(
        &task,
        "task.propose",
        "project",
        TASK_PROJECT_ID,
        "correlation-direct-task-key",
    );
    assert_eq!(task["result"]["domain_committed"], true);
    let task_id = result_field(&task, "task_id")
        .and_then(Value::as_str)
        .expect("direct Task result contains task id");
    assert!(!task_id.is_empty());
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM agent_action").await,
        0,
        "task.propose is a direct command when policy admits it"
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM command_receipt WHERE operation = 'task.propose'",
        )
        .await,
        1
    );
    let task_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task WHERE id = ?")
        .bind(task_id)
        .fetch_one(fixture.db.pool())
        .await
        .expect("Task count");
    assert_eq!(task_count, 1);
    let task_receipt = sqlx::query(
        "SELECT event_id, agent_action_execution_id, outcome_json
         FROM command_receipt
         WHERE operation = 'task.propose' AND idempotency_key = 'direct-task-key'",
    )
    .fetch_one(fixture.db.pool())
    .await
    .expect("direct Task receipt");
    assert!(task_receipt
        .get::<Option<String>, _>("agent_action_execution_id")
        .is_none());
    let task_event = sqlx::query(
        "SELECT event_type, entity_type, entity_id, scope_type, scope_id
         FROM domain_event WHERE id = ?",
    )
    .bind(task_receipt.get::<String, _>("event_id"))
    .fetch_one(fixture.db.pool())
    .await
    .expect("direct Task event");
    assert_eq!(task_event.get::<String, _>("event_type"), "task.created");
    assert_eq!(task_event.get::<String, _>("entity_type"), "task");
    assert_eq!(task_event.get::<String, _>("entity_id"), task_id);
    assert_eq!(task_event.get::<String, _>("scope_type"), "project");
    assert_eq!(task_event.get::<String, _>("scope_id"), TASK_PROJECT_ID);
}

#[tokio::test]
async fn direct_command_retry_returns_frozen_result_without_duplicate_effect() {
    let fixture = fixture().await;
    let first = fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.project_scope,
            RUNTIME_SESSION_ID,
            PROJECT_DOCUMENT_OPERATION,
            document_arguments("direct-replay-key"),
        )
        .await
        .expect("first direct document command");
    assert_success_envelope(
        &first,
        PROJECT_DOCUMENT_OPERATION,
        "project",
        PROJECT_ID,
        "correlation-direct-replay-key",
    );
    assert_eq!(first["replayed"], false);
    let first_document_id = result_field(&first, "document_id")
        .and_then(Value::as_str)
        .expect("first document id")
        .to_owned();
    let first_revision_id = result_field(&first, "revision_id")
        .and_then(Value::as_str)
        .expect("first revision id")
        .to_owned();
    let first_receipt_id = result_field(&first, "receipt_id")
        .and_then(Value::as_str)
        .expect("first receipt id")
        .to_owned();

    // Change only the live projection.  Exact replay must return the frozen
    // command outcome rather than reconstructing a new revision or adopting
    // a changed current row.
    sqlx::query("UPDATE project_document SET title = 'Live projection changed' WHERE id = ?")
        .bind(&first_document_id)
        .execute(fixture.db.pool())
        .await
        .expect("mutate live document projection");
    let second = fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.project_scope,
            RUNTIME_SESSION_ID,
            PROJECT_DOCUMENT_OPERATION,
            document_arguments("direct-replay-key"),
        )
        .await
        .expect("exact direct replay");
    assert_success_envelope(
        &second,
        PROJECT_DOCUMENT_OPERATION,
        "project",
        PROJECT_ID,
        "correlation-direct-replay-key",
    );
    assert_eq!(second["replayed"], true);
    assert_eq!(
        result_field(&second, "document_id").and_then(Value::as_str),
        Some(first_document_id.as_str())
    );
    assert_eq!(
        result_field(&second, "revision_id").and_then(Value::as_str),
        Some(first_revision_id.as_str())
    );
    assert_eq!(
        result_field(&second, "receipt_id").and_then(Value::as_str),
        Some(first_receipt_id.as_str())
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM project_document_revision",
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM command_receipt WHERE operation = 'project.document'",
        )
        .await,
        1
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM agent_action").await,
        0
    );
}

#[tokio::test]
async fn approval_required_release_candidate_stays_pending_without_domain_result() {
    let fixture = fixture().await;
    let pending = fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.project_scope,
            RUNTIME_SESSION_ID,
            "project.release.request",
            approval_required_release_arguments("approval-required-release-key"),
        )
        .await
        .expect("approval-required operation creates pending action");
    assert_eq!(pending["code"], "approval_required");
    assert_eq!(pending["status"], "approval_required");
    assert_eq!(pending["operation"], "project.release.request");
    assert_eq!(pending["scope"]["scope_type"], "project");
    assert_eq!(pending["scope"]["scope_id"], PROJECT_ID);
    assert_eq!(
        pending["correlation_id"],
        "correlation-approval-required-release-key"
    );
    assert!(pending["approval_target"]["target_type"].as_str().is_some());
    assert!(pending["approval_target"]["target_id"].as_str().is_some());
    assert_eq!(
        pending["approval_target"]["operation"],
        "project.release.request"
    );
    assert_eq!(
        pending["approval_target"]["requires_user_authorization"],
        true
    );
    assert!(pending["result"].is_null());
    assert!(pending["domain_result"].is_null());
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM agent_action WHERE operation = 'project.release.request'",
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM agent_action WHERE status = 'pending_approval'",
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM command_receipt WHERE operation = 'project.release.request'",
        )
        .await,
        0,
        "pending approval has no successful command receipt"
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM project_release").await,
        0,
        "approval admission does not create a release candidate"
    );
}

/// Before the release request was registered its queueing step wrote the
/// action row first, so a request the policy denies stayed on the ledger as
/// `denied`. The registered path refuses it at admission and must still
/// leave that row; a refused call that was never queueable leaves none.
#[tokio::test]
async fn denied_release_request_is_recorded_on_the_action_ledger() {
    let fixture = fixture().await;
    sqlx::query(
        "UPDATE project_agent_binding
         SET permission_ceiling_json = '{\"allowed\":[\"read_project\"]}'
         WHERE project_id = ? AND state = 'active'",
    )
    .bind(PROJECT_ID)
    .execute(fixture.db.pool())
    .await
    .expect("restrict Project policy");

    let mut malformed = approval_required_release_arguments("denied-release-malformed");
    malformed["payload"]["unexpected"] = json!(true);
    for (arguments, rows) in [
        (malformed, 0),
        (approval_required_release_arguments("denied-release-key"), 1),
        // A retry of the same denied call does not add a second row.
        (approval_required_release_arguments("denied-release-key"), 1),
    ] {
        let denied = fixture
            .provider
            .propose(
                AGENT_ID,
                &fixture.project_scope,
                RUNTIME_SESSION_ID,
                "project.release.request",
                arguments,
            )
            .await
            .expect_err("a release request without propose_project is denied");
        let denied = structured_error(denied);
        assert_eq!(denied.code, api_types::OutcomeCode::PolicyDenied);
        assert_eq!(
            denied.denied_by,
            Some(api_types::DeniedBy::PermissionMissing(
                "propose_project".to_owned()
            ))
        );
        assert_eq!(
            count(
                &fixture.db,
                "SELECT COUNT(*) FROM agent_action WHERE operation = 'project.release.request'",
            )
            .await,
            rows
        );
    }
    let (status, policy_result, permission, target_type, target_id): (
        String,
        String,
        String,
        String,
        String,
    ) = sqlx::query_as(
        "SELECT status, policy_result, requested_permission, target_type, target_id
         FROM agent_action WHERE dedupe_key = 'denied-release-key'",
    )
    .fetch_one(fixture.db.pool())
    .await
    .expect("denied release request row");
    assert_eq!(
        (
            status.as_str(),
            policy_result.as_str(),
            permission.as_str(),
            target_type.as_str(),
            target_id.as_str()
        ),
        ("denied", "denied", "propose_project", "project", PROJECT_ID)
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM command_receipt").await,
        0
    );
}

#[tokio::test]
async fn denied_operation_is_not_admitted_and_cannot_mutate() {
    let fixture = fixture().await;
    let revision_count_before = count(
        &fixture.db,
        "SELECT COUNT(*) FROM project_document_revision",
    )
    .await;
    let receipt_count_before = count(
        &fixture.db,
        "SELECT COUNT(*) FROM command_receipt WHERE idempotency_key = 'denied-document-key'",
    )
    .await;
    let event_count_before = count(&fixture.db, "SELECT COUNT(*) FROM domain_event").await;
    sqlx::query(
        "UPDATE project_agent_binding
         SET permission_ceiling_json = '{\"allowed\":[\"read_project\"]}'
         WHERE project_id = ? AND state = 'active'",
    )
    .bind(PROJECT_ID)
    .execute(fixture.db.pool())
    .await
    .expect("restrict Project policy");

    let denied = fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.project_scope,
            RUNTIME_SESSION_ID,
            PROJECT_DOCUMENT_OPERATION,
            document_arguments("denied-document-key"),
        )
        .await;
    let denied = match denied {
        Err(error) => structured_error(error),
        Ok(value) => panic!("policy denial must not be a successful envelope: {value}"),
    };
    assert_eq!(denied.code, api_types::OutcomeCode::PolicyDenied);
    assert_eq!(denied.status, api_types::OutcomeStatus::Failed);
    assert_eq!(denied.operation, PROJECT_DOCUMENT_OPERATION);
    assert_eq!(
        denied.scope.scope_type,
        api_types::OutcomeScopeType::Project
    );
    assert_eq!(denied.scope.scope_id, PROJECT_ID);
    assert_eq!(denied.correlation_id, "correlation-denied-document-key");
    assert!(denied.result.is_none());
    assert!(denied.approval_target.is_none());
    assert!(
        !denied.safe_message.contains("read_project"),
        "policy details must not reach the model"
    );
    assert_eq!(
        denied
            .denied_by
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("permission_missing(propose_project)")
    );
    assert!(denied.safe_message.contains("Do not retry"));
    assert_eq!(
        denied.retry.as_ref().unwrap().action,
        api_types::RetryAction::None
    );
    assert_eq!(
        denied.retry.as_ref().unwrap().scope,
        Some(api_types::RetryScope::Session)
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM project_document_revision",
        )
        .await,
        revision_count_before
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM command_receipt WHERE idempotency_key = 'denied-document-key'",
        )
        .await,
        receipt_count_before
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM domain_event").await,
        event_count_before
    );
}

#[tokio::test]
async fn authorized_stale_milestone_returns_typed_version_correction() {
    let fixture = fixture().await;
    sqlx::query("UPDATE project_milestone SET version = 2 WHERE id = ?")
        .bind(MILESTONE_ID)
        .execute(fixture.db.pool())
        .await
        .expect("advance authorized milestone version");

    let arguments = stale_milestone_arguments("stale-milestone-key", 1);
    let error = fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.project_scope,
            RUNTIME_SESSION_ID,
            PROJECT_MILESTONE_OPERATION,
            arguments.clone(),
        )
        .await
        .expect_err("stale milestone must be a structured conflict");
    let outcome = structured_error(error);

    assert_eq!(outcome.code, api_types::OutcomeCode::VersionConflict);
    assert_eq!(outcome.status, api_types::OutcomeStatus::Failed);
    assert_eq!(outcome.operation, PROJECT_MILESTONE_OPERATION);
    assert_eq!(
        outcome.scope.scope_type,
        api_types::OutcomeScopeType::Project
    );
    assert_eq!(outcome.scope.scope_id, PROJECT_ID);
    assert_eq!(outcome.correlation_id, "correlation-stale-milestone-key");
    let current = outcome
        .current_version_or_revision
        .expect("authorized current milestone state");
    assert_eq!(current.resource_type, "project_milestone");
    assert_eq!(current.resource_id, MILESTONE_ID);
    assert_eq!(current.version, Some(2));
    assert_eq!(current.revision_id.as_deref(), Some(MILESTONE_REVISION_ID));
    assert_eq!(current.revision, Some(1));
    let retry = outcome.retry.expect("typed stale retry");
    assert_eq!(retry.action, api_types::RetryAction::RefreshAndRetry);
    assert!(retry.retryable);
    assert_eq!(
        retry.arguments.get("expected_milestone_version"),
        Some(&json!(2))
    );
    assert!(outcome.result.is_none());
    assert!(
        !outcome.safe_message.contains("milestone-content"),
        "raw persistence details must not reach the model"
    );
}

#[tokio::test]
async fn idempotency_mismatch_has_no_current_state_disclosure() {
    let fixture = fixture().await;
    let first_arguments = document_arguments("idempotency-mismatch-key");
    fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.project_scope,
            RUNTIME_SESSION_ID,
            PROJECT_DOCUMENT_OPERATION,
            first_arguments,
        )
        .await
        .expect("initial direct document command");

    let mut changed_arguments = document_arguments("idempotency-mismatch-key");
    changed_arguments["payload"]["title"] = json!("Changed command input");
    let error = fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.project_scope,
            RUNTIME_SESSION_ID,
            PROJECT_DOCUMENT_OPERATION,
            changed_arguments,
        )
        .await
        .expect_err("changed input on a bound key must conflict");
    let outcome = structured_error(error);

    assert_eq!(outcome.code, api_types::OutcomeCode::IdempotencyConflict);
    assert_eq!(outcome.status, api_types::OutcomeStatus::Failed);
    assert_eq!(outcome.operation, PROJECT_DOCUMENT_OPERATION);
    assert_eq!(
        outcome.scope.scope_type,
        api_types::OutcomeScopeType::Project
    );
    assert_eq!(outcome.scope.scope_id, PROJECT_ID);
    assert_eq!(
        outcome.correlation_id,
        "correlation-idempotency-mismatch-key"
    );
    assert!(outcome.current_version_or_revision.is_none());
    assert!(outcome.result.is_none());
    let retry = outcome.retry.expect("new key retry");
    assert_eq!(retry.action, api_types::RetryAction::UseNewIdempotencyKey);
    assert!(!retry.retryable);
    assert!(retry.arguments.is_empty());
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM project_document_revision",
        )
        .await,
        1,
        "an idempotency mismatch must not create another revision"
    );
}

#[tokio::test]
async fn terminal_native_denial_offers_only_a_held_owner_escalation_operation() {
    let fixture =
        fixture_with_permissions(r#"{"permissions":["read_project","propose_message"]}"#).await;
    sqlx::query("UPDATE project_agent_binding SET permission_ceiling_json = '{\"allowed\":[\"read_project\",\"propose_message\"]}' WHERE project_id = ? AND state = 'active'")
        .bind(PROJECT_ID).execute(fixture.db.pool()).await.unwrap();
    let deny = |key| {
        fixture.provider.propose(
            AGENT_ID,
            &fixture.project_scope,
            RUNTIME_SESSION_ID,
            PROJECT_DOCUMENT_OPERATION,
            document_arguments(key),
        )
    };
    let held = structured_error(deny("held-alternative").await.unwrap_err());
    assert_eq!(
        held.denied_by.as_ref().map(ToString::to_string).as_deref(),
        Some("permission_missing(propose_project)")
    );
    assert_eq!(held.alternatives, Some(vec!["message.send".to_owned()]));
    sqlx::query("UPDATE project_agent_binding SET permission_ceiling_json = '{\"allowed\":[\"read_project\"]}' WHERE project_id = ? AND state = 'active'")
        .bind(PROJECT_ID).execute(fixture.db.pool()).await.unwrap();
    let revoked = structured_error(deny("revoked-alternative").await.unwrap_err());
    assert!(revoked.alternatives.is_none());
}

#[tokio::test]
async fn terminal_native_task_commands_preserve_evaluator_permission_and_identity_denials() {
    let fixture = fixture().await;
    let task = db::TaskRepo::create(
        &*fixture.db,
        db::CreateTask {
            id: db::new_uuid_v4(),
            project_id: PROJECT_ID.to_owned(),
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "Blocked Task".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "blocked".to_owned(),
            is_automation: false,
            priority: 0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .unwrap();
    // Revoke the canonical Task permission after catalog composition.
    // Exercise the real evaluator rather than a fabricated adapter error.
    sqlx::query("UPDATE project_agent_binding SET permission_ceiling_json = '{\"allowed\":[\"read_project\"]}' WHERE project_id = ? AND state = 'active'")
        .bind(PROJECT_ID).execute(fixture.db.pool()).await.unwrap();
    let review = fixture.provider.propose(AGENT_ID, &fixture.project_scope, RUNTIME_SESSION_ID,
        "task.action", json!({"operation":"task.action", "payload":{
            "task_id":task.id, "action":{"verb":"approve","override":false,"reason":"Check refusal"}, "version":task.version}, "dedupe_key":"denied-review", "correlation_id":"denied-review"})
    ).await.unwrap_err();
    let review = structured_error(review);
    assert_eq!(
        review
            .denied_by
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("permission_missing(propose_task)")
    );
    assert_eq!(review.retry.unwrap().action, api_types::RetryAction::None);
    sqlx::query("UPDATE project_agent_binding SET permission_ceiling_json = '{\"allowed\":[\"read_project\",\"propose_task\"]}' WHERE project_id = ? AND state = 'active'")
        .bind(PROJECT_ID).execute(fixture.db.pool()).await.unwrap();
    sqlx::query("UPDATE agent_identity SET paused = 1 WHERE id = ?")
        .bind(AGENT_ID)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let recover = fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.project_scope,
            RUNTIME_SESSION_ID,
            "task.action",
            json!({"operation":"task.action", "payload":{
            "task_id":task.id, "action":{"verb":"retry","fresh_session":true}, "version":task.version},
            "dedupe_key":"denied-recover", "correlation_id":"denied-recover"}),
        )
        .await
        .unwrap_err();
    let recover = structured_error(recover);
    assert_eq!(
        recover
            .denied_by
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("identity_paused")
    );
    assert_eq!(recover.retry.unwrap().action, api_types::RetryAction::None);
    assert!(recover
        .safe_message
        .contains("repeating this call will be refused again"));
}

#[tokio::test]
async fn terminal_native_charter_denial_preserves_evaluator_cause() {
    let fixture = fixture().await;
    let unadopted_project = "terminal-denial-unadopted-project";
    seed_project(
        &fixture.db,
        unadopted_project,
        "terminal-denial-unadopted-repo",
    )
    .await;
    let scope = CanonicalScope {
        scope_id: unadopted_project.to_owned(),
        ..fixture.project_scope.clone()
    };
    let outcome = fixture
        .provider
        .propose(
            AGENT_ID,
            &scope,
            RUNTIME_SESSION_ID,
            PROJECT_DOCUMENT_OPERATION,
            document_arguments("charter-denial"),
        )
        .await
        .unwrap_err();
    let outcome = structured_error(outcome);
    assert_eq!(outcome.code, api_types::OutcomeCode::PolicyDenied);
    assert_eq!(
        outcome
            .denied_by
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("charter_not_adopted")
    );
    assert_eq!(outcome.retry.unwrap().action, api_types::RetryAction::None);
    assert!(outcome.safe_message.contains("Do not retry"));
}

// Exercise the actual host wrapper, provider and policy in one model turn.
fn native_tool(
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

async fn invoke_native(
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

async fn project_chat_session(fixture: &Fixture) -> (CanonicalScope, String) {
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

#[tokio::test]
async fn terminal_denial_per_document_refusal_does_not_block_a_different_document_in_the_same_turn()
{
    let fixture = fixture().await;
    let (scope, _) = project_chat_session(&fixture).await;
    let tool = native_tool(
        &fixture,
        scope,
        true,
        forge_agent_host::FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
    );
    let draft = invoke_native(
        &*tool,
        document_arguments("first-draft"),
        "same-turn",
        "draft",
    )
    .await;
    assert!(!draft.is_error, "{draft:?}");
    let (document_id, revision_id, version): (String, String, i64) = sqlx::query_as(
        "SELECT id, current_draft_revision_id, version FROM project_document WHERE project_id = 'direct-classification-project'",
    ).fetch_one(fixture.db.pool()).await.unwrap();
    let (content_digest, render_digest): (String, String) = sqlx::query_as(
        "SELECT content_digest, rendered_digest FROM project_document_revision WHERE id = ?",
    )
    .bind(&revision_id)
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE project_document SET approval_policy = 'user' WHERE id = ?")
        .bind(&document_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let denied = invoke_native(&*tool, json!({"operation":"project.document", "payload":{
        "action":"approve", "document_id":document_id, "revision_id":revision_id,
        "content_digest":content_digest, "render_digest":render_digest,
        "expected_document_version":version}, "dedupe_key":"forbidden-approval", "correlation_id":"forbidden-approval"}), "same-turn", "approve").await;
    assert!(denied.is_error, "{denied:?}");
    assert_eq!(denied.value["denied_by"], "unspecified");
    assert_eq!(denied.value["retry"]["action"], "none");
    assert!(!denied.value["safe_message"]
        .as_str()
        .unwrap()
        .contains("Do not retry"));
    let mut second = document_arguments("second-document");
    second["payload"]["document_id"] = json!("different-document");
    assert!(
        !invoke_native(&*tool, second, "same-turn", "second")
            .await
            .is_error
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM chat_session_denied_operation"
        )
        .await,
        0
    );
}

#[tokio::test]
async fn terminal_denial_current_state_load_error_is_internal_and_not_recorded() {
    // `native_tool` composes the Project Chat with `read_agent_chat`; the
    // registered read enforces what that surface advertises, so the identity
    // and binding must really hold it.
    let fixture = fixture_with_permissions(
        r#"{"permissions":["read_project","read_agent_chat","propose_project","propose_task","propose_commitment"]}"#,
    )
    .await;
    sqlx::query(
        "UPDATE project_agent_binding SET permission_ceiling_json = ?
         WHERE project_id = ? AND state = 'active'",
    )
    .bind(r#"{"allowed":["read_project","read_agent_chat","propose_project","propose_task","propose_commitment"]}"#)
    .bind(PROJECT_ID)
    .execute(fixture.db.pool())
    .await
    .unwrap();
    let (scope, _) = project_chat_session(&fixture).await;
    let tool = native_tool(
        &fixture,
        scope,
        true,
        forge_agent_host::FORGE_PROJECT_ORCHESTRATION_READ_TOOL,
    );
    // Authorization still succeeds; the state projection query fails.
    sqlx::query("ALTER TABLE task RENAME COLUMN status TO unavailable_status")
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let failed = invoke_native(
        &*tool,
        json!({"operation":"project.current_state"}),
        "same-turn",
        "failed-load",
    )
    .await;
    assert!(failed.is_error);
    assert_eq!(failed.value["code"], "internal_failure", "{failed:?}");
    assert!(failed.value.get("denied_by").is_none());
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM chat_session_denied_operation"
        )
        .await,
        0
    );
    sqlx::query("ALTER TABLE task RENAME COLUMN unavailable_status TO status")
        .execute(fixture.db.pool())
        .await
        .unwrap();
    assert!(
        !invoke_native(
            &*tool,
            json!({"operation":"project.current_state"}),
            "same-turn",
            "restored-load"
        )
        .await
        .is_error
    );
}

#[tokio::test]
async fn terminal_denial_permission_grant_invalidates_reminder_and_next_turn_call_succeeds() {
    use db::ChatSessionDenialRepo;
    let fixture = fixture().await;
    let (scope, session_id) = project_chat_session(&fixture).await;
    let tool = native_tool(
        &fixture,
        scope.clone(),
        true,
        forge_agent_host::FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
    );
    sqlx::query("UPDATE project_agent_binding SET permission_ceiling_json = '{\"allowed\":[\"read_project\"]}' WHERE project_id = ? AND state = 'active'")
        .bind(PROJECT_ID).execute(fixture.db.pool()).await.unwrap();
    let denied = invoke_native(&*tool, document_arguments("denied"), "first-turn", "denied").await;
    assert_eq!(
        denied.value["denied_by"],
        "permission_missing(propose_project)"
    );
    assert_eq!(
        fixture
            .provider
            .chat_session_denials(AGENT_ID, PROFILE_ID, &scope.scope_id, Some(&session_id))
            .await
            .len(),
        1
    );
    let created_at: String =
        sqlx::query_scalar("SELECT created_at FROM chat_session_denied_operation")
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
    assert!(!created_at.is_empty());
    sqlx::query("UPDATE project_agent_binding SET permission_ceiling_json = '{\"allowed\":[\"read_project\",\"propose_project\"]}' WHERE project_id = ? AND state = 'active'")
        .bind(PROJECT_ID).execute(fixture.db.pool()).await.unwrap();
    assert!(fixture
        .provider
        .chat_session_denials(AGENT_ID, PROFILE_ID, &scope.scope_id, Some(&session_id))
        .await
        .is_empty());
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM chat_session_denied_operation"
        )
        .await,
        0
    );
    // Only a reminder read into this turn's card is cleared by success.
    fixture
        .db
        .record_chat_session_denial(
            AGENT_ID,
            &scope.scope_id,
            RUNTIME_SESSION_ID,
            "project.document",
            &api_types::DeniedBy::PermissionMissing("propose_project".to_owned()),
        )
        .await
        .unwrap();
    // The card still sees a refusal; authority changes before invocation.
    sqlx::query("UPDATE project_agent_binding SET permission_ceiling_json = '{\"allowed\":[\"read_project\"]}' WHERE project_id = ? AND state = 'active'")
        .bind(PROJECT_ID).execute(fixture.db.pool()).await.unwrap();
    assert_eq!(
        fixture
            .provider
            .chat_session_denials(AGENT_ID, PROFILE_ID, &scope.scope_id, Some(&session_id))
            .await
            .len(),
        1
    );
    sqlx::query("UPDATE project_agent_binding SET permission_ceiling_json = '{\"allowed\":[\"read_project\",\"propose_project\"]}' WHERE project_id = ? AND state = 'active'")
        .bind(PROJECT_ID).execute(fixture.db.pool()).await.unwrap();
    assert!(
        !invoke_native(
            &*tool,
            document_arguments("granted"),
            "next-turn",
            "granted"
        )
        .await
        .is_error
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM chat_session_denied_operation"
        )
        .await,
        0
    );
}

#[tokio::test]
async fn terminal_denial_bad_project_summary_id_does_not_withdraw_operation() {
    let fixture = fixture().await;
    sqlx::query("INSERT INTO account_main_agent_binding (id, account_id, identity_id, profile_id, created_at, updated_at) VALUES ('summary-main', ?, ?, ?, ?, ?)")
        .bind(USER_ID).bind(AGENT_ID).bind(PROFILE_ID).bind(NOW).bind(NOW).execute(fixture.db.pool()).await.unwrap();
    let chat_id: String = sqlx::query_scalar(
        "SELECT id FROM agent_chat WHERE kind = 'account_main' AND account_id = ?",
    )
    .bind(USER_ID)
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    let scope = CanonicalScope {
        scope_type: CanonicalScopeType::AgentChat,
        scope_id: chat_id,
        workspace_access: WorkspaceAccess::Deny,
    };
    let tool = native_tool(&fixture, scope, false, "forge_scope_read");
    for (call, args) in [
        ("missing", json!({"operation":"project.summary"})),
        (
            "wrong",
            json!({"operation":"project.summary", "arguments":{"project_id":"deleted-or-wrong"}}),
        ),
    ] {
        let failed = invoke_native(&*tool, args, "same-turn", call).await;
        assert_eq!(failed.value["code"], "validation_error", "{failed:?}");
        assert!(failed.value.get("denied_by").is_none());
    }
    let success = invoke_native(
        &*tool,
        json!({"operation":"project.summary", "arguments":{"project_id":PROJECT_ID}}),
        "same-turn",
        "corrected",
    )
    .await;
    assert!(!success.is_error, "{success:?}");
    assert_eq!(success.value["id"], PROJECT_ID);
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM chat_session_denied_operation"
        )
        .await,
        0
    );
}

#[tokio::test]
async fn terminal_denial_scope_permission_reminder_uses_real_policy_and_read_failures_are_advisory()
{
    use db::ChatSessionDenialRepo;
    let fixture = fixture_with_permissions(
        r#"{"permissions":["read_project","propose_project","propose_review"]}"#,
    )
    .await;
    let (scope, session_id) = project_chat_session(&fixture).await;
    // All editable ceilings allow propose_review. The static Project Chat
    // scope set does not, so this cause must remain in the card.
    sqlx::query("UPDATE project_agent_binding SET permission_ceiling_json = '{\"allowed\":[\"read_project\",\"propose_project\",\"propose_review\"]}' WHERE project_id = ? AND state = 'active'")
        .bind(PROJECT_ID).execute(fixture.db.pool()).await.unwrap();
    fixture
        .db
        .record_chat_session_denial(
            AGENT_ID,
            &scope.scope_id,
            RUNTIME_SESSION_ID,
            "review.request",
            &api_types::DeniedBy::PermissionMissing("propose_review".to_owned()),
        )
        .await
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            fixture
                .provider
                .chat_session_denials(AGENT_ID, PROFILE_ID, &scope.scope_id, Some(&session_id))
                .await,
            vec![(
                "review.request".to_owned(),
                api_types::DeniedBy::PermissionMissing("propose_review".to_owned())
            )]
        );
    }
    sqlx::query("DROP TABLE chat_session_denied_operation")
        .execute(fixture.db.pool())
        .await
        .unwrap();
    assert!(fixture
        .provider
        .chat_session_denials(AGENT_ID, PROFILE_ID, &scope.scope_id, Some(&session_id))
        .await
        .is_empty());
}

#[tokio::test]
async fn terminal_denial_success_without_card_read_does_not_delete_and_unknown_permissions_are_not_recorded(
) {
    use db::ChatSessionDenialRepo;
    let fixture = fixture().await;
    let (scope, _) = project_chat_session(&fixture).await;
    let tool = native_tool(
        &fixture,
        scope.clone(),
        true,
        forge_agent_host::FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
    );
    fixture
        .db
        .record_chat_session_denial(
            AGENT_ID,
            &scope.scope_id,
            RUNTIME_SESSION_ID,
            "project.document",
            &api_types::DeniedBy::OperationNotInScope,
        )
        .await
        .unwrap();
    assert!(
        !invoke_native(
            &*tool,
            document_arguments("no-card-read"),
            "same-turn",
            "success"
        )
        .await
        .is_error
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM chat_session_denied_operation"
        )
        .await,
        1
    );
    for permission in ["unknown", "invented_permission"] {
        fixture
            .provider
            .record_terminal_denial(
                AGENT_ID,
                &scope,
                RUNTIME_SESSION_ID,
                "task.action",
                &api_types::DeniedBy::PermissionMissing(permission.to_owned()),
            )
            .await
            .unwrap();
    }
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM chat_session_denied_operation"
        )
        .await,
        1
    );
}

async fn recovery_fixture() -> (Fixture, tempfile::TempDir, String, String, String) {
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

async fn recovery_arguments(fixture: &Fixture, task: &str, action: &str, key: &str) -> Value {
    let version = db::TaskRepo::get_by_id(&*fixture.db, task, false)
        .await
        .unwrap()
        .unwrap()
        .version;
    json!({"operation":"task.action", "payload":{"task_id":task,"action":{"verb":action,"reason":"Repair or cancel this interrupted work"},"version":version},
        "dedupe_key":key,"correlation_id":key})
}

#[tokio::test]
async fn queued_retry_for_paused_worker_does_not_withdraw_cancellation_for_another_task() {
    let (fixture, _temp, worker, task_a, task_b) = recovery_fixture().await;
    let (scope, _) = project_chat_session(&fixture).await;
    let tool = native_tool(&fixture, scope, true, "forge_scope_propose");
    sqlx::query("UPDATE agent_identity SET paused = 1 WHERE id = ?")
        .bind(worker)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let refused = invoke_native(
        &*tool,
        recovery_arguments(&fixture, &task_a, "retry", "paused-worker").await,
        "same-turn",
        "paused-worker",
    )
    .await;
    assert!(refused.is_error, "paused worker refuses retry: {refused:?}");
    assert_eq!(refused.value["denied_by"], "target_agent_paused");
    assert_eq!(refused.value["retry"]["scope"], "turn");
    assert!(refused.value["details"]["available_actions"]
        .as_array()
        .unwrap()
        .iter()
        .all(|offer| offer["action"]["verb"] != "retry"));
    let recovered = invoke_native(
        &*tool,
        recovery_arguments(&fixture, &task_b, "cancel", "other-task").await,
        "same-turn",
        "other-task",
    )
    .await;
    assert!(!recovered.is_error, "{recovered:?}");
    assert_eq!(recovered.value["result"]["task_status"], "cancelled");
}

#[tokio::test]
async fn queued_retry_for_paused_project_does_not_withdraw_cancellation() {
    let (fixture, _temp, _worker, task_a, _) = recovery_fixture().await;
    let (scope, _) = project_chat_session(&fixture).await;
    let tool = native_tool(&fixture, scope, true, "forge_scope_propose");
    sqlx::query("UPDATE project SET paused_at = 'now', system_pause_reason = 'environment_not_ready' WHERE id = ?")
        .bind(PROJECT_ID).execute(fixture.db.pool()).await.unwrap();
    let refused = invoke_native(
        &*tool,
        recovery_arguments(&fixture, &task_a, "retry", "paused-project").await,
        "same-turn",
        "paused-project",
    )
    .await;
    assert!(
        refused.is_error,
        "paused Project refuses retry: {refused:?}"
    );
    assert_eq!(
        refused.value["denied_by"],
        "project_paused(environment_not_ready)"
    );
    assert_eq!(refused.value["retry"]["scope"], "turn");
    let recovered = invoke_native(
        &*tool,
        recovery_arguments(&fixture, &task_a, "cancel", "cancel-paused-task").await,
        "same-turn",
        "cancel-paused-task",
    )
    .await;
    assert!(!recovered.is_error, "{recovered:?}");
    assert_eq!(recovered.value["result"]["task_status"], "cancelled");
}
