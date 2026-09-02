//! Characterization coverage for the current REST/native/MCP orchestration
//! adapter seams.  These assertions intentionally describe today's behavior;
//! Gate A can update them to the shared parity contract once that boundary is
//! implemented.

mod common;

use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use db::{
    AgentRepo, AgentStatus, CreateAgentIdentity, CreateAgentProfile, CreateProjectAgentBinding,
    ProjectAgentBindingRepo, ReplaceProjectAgentBinding,
};
use forge_agent_host::{CanonicalScope, CanonicalScopeType, ForgeToolProvider, WorkspaceAccess};
use serde_json::{json, Value};
use tower::ServiceExt;

#[derive(Debug, Clone)]
struct CharterFixture {
    project_id: String,
    #[allow(dead_code)]
    charter_id: String,
    #[allow(dead_code)]
    charter_revision_id: String,
    #[allow(dead_code)]
    charter_content_digest: String,
    #[allow(dead_code)]
    charter_render_digest: String,
    #[allow(dead_code)]
    milestone_id: String,
    #[allow(dead_code)]
    milestone_revision_id: String,
    native_identity_id: String,
}

#[tokio::test]
async fn task_proposal_rest_and_native_share_command_receipts_mcp_is_direct_task_api() {
    let workspace = common::TestDir::new("adapter-parity-task");
    let harness = common::test_app(workspace.path(), "adapter-parity-task").await;
    let fixture = charter_backed_project(&harness, "adapter-parity-task").await;

    let rest_request = json!({
        "project_id": fixture.project_id,
        "title": "REST proposal",
        "description": "characterize proposal receipt",
        "parent_task_id": null,
        "priority": 1,
        "task_type": "task",
        "task_state_config": null,
        "merge_config": null,
        "role_assignments": [],
        "governance": null,
        "dedupe_key": "adapter-parity-task-rest",
        "correlation_id": "adapter-parity-task-rest-correlation",
        "causation_id": null,
        "causation_depth": 0
    });
    let rest_proposal = common::json_request::<Value>(
        &harness.app,
        Method::POST,
        &format!(
            "/api/v1/agents/{}/task-proposals",
            fixture.native_identity_id
        ),
        rest_request.clone(),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(rest_proposal["operation"], "task.propose");
    assert_eq!(rest_proposal["status"], "succeeded");
    assert_eq!(rest_proposal["materialized"], true);
    assert_eq!(rest_proposal["domain_committed"], true);
    assert_eq!(rest_proposal["requires_user_authorization"], false);
    assert_eq!(rest_proposal["policy_result"], "allowed");
    assert_eq!(
        rest_proposal["correlation_id"],
        "adapter-parity-task-rest-correlation"
    );
    assert_eq!(rest_proposal["replayed"], false);
    assert!(rest_proposal["input_digest"].as_str().is_some());
    assert!(rest_proposal["receipt_id"].as_str().is_some());
    assert!(rest_proposal["event_id"].as_str().is_some());
    assert_eq!(rest_proposal["task"]["title"], "REST proposal");
    assert_eq!(rest_proposal["task"]["status"], "backlog");
    let rest_task_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task WHERE project_id = ? AND title = 'REST proposal'",
    )
    .bind(&fixture.project_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("REST task count");
    assert_eq!(rest_task_count, 1, "REST directly materializes the Task");
    let rest_action_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_action
         WHERE operation = 'task.propose' AND dedupe_key = 'adapter-parity-task-rest'",
    )
    .fetch_one(harness.state.db.pool())
    .await
    .expect("REST action count");
    assert_eq!(
        rest_action_count, 0,
        "direct REST commands do not enqueue actions"
    );

    let provider = native_provider(&harness);
    let scope = CanonicalScope {
        scope_type: CanonicalScopeType::Project,
        scope_id: fixture.project_id.clone(),
        workspace_access: WorkspaceAccess::Deny,
    };
    let native_result = ForgeToolProvider::propose(
        &provider,
        &fixture.native_identity_id,
        &scope,
        "task.propose",
        json!({
            "payload": {
                "title": "Native proposal",
                "description": "characterize inline materialization",
                "parent_task_id": null,
                "priority": 1,
                "task_type": "task",
                "task_state_config": null,
                "merge_config": null,
                "role_assignments": [],
                "governance": null
            },
            "dedupe_key": "adapter-parity-task-native",
            "correlation_id": "adapter-parity-task-native-correlation"
        }),
    )
    .await
    .expect("native task proposal");
    let native_result_body = assert_native_success_outcome(
        &native_result,
        "task.propose",
        "project",
        &fixture.project_id,
        "adapter-parity-task-native-correlation",
    );
    assert_eq!(native_result_body["materialized"], true);
    assert_eq!(native_result_body["domain_committed"], true);
    assert_eq!(native_result_body["requires_user_authorization"], false);
    let native_task_id = native_result_body["domain_result"]["task_id"]
        .as_str()
        .expect("native task id")
        .to_owned();
    assert_eq!(
        native_result_body["domain_result"]["task_status"],
        "backlog"
    );
    let native_receipt_id = native_result["receipt_id"]
        .as_str()
        .expect("native receipt id");
    let native_receipt_without_action_execution: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM command_receipt
         WHERE id = ? AND agent_action_execution_id IS NULL",
    )
    .bind(native_receipt_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("native command receipt lookup");
    assert_eq!(native_receipt_without_action_execution, 1);
    let native_action_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_action
         WHERE operation = 'task.propose' AND dedupe_key = 'adapter-parity-task-native'",
    )
    .fetch_one(harness.state.db.pool())
    .await
    .expect("native action count");
    assert_eq!(
        native_action_count, 0,
        "direct native commands do not enqueue actions"
    );

    // A committed REST response can be replayed after the mutable Project
    // binding is paused. The route must let the exact receipt reach the
    // shared command before applying current-user mutation authorization.
    sqlx::query(
        "UPDATE project_agent_binding SET state = 'paused'
         WHERE project_id = ? AND identity_id = ? AND state = 'active'",
    )
    .bind(&fixture.project_id)
    .bind(&fixture.native_identity_id)
    .execute(harness.state.db.pool())
    .await
    .expect("pause binding for REST replay");
    let rest_replay = common::json_request::<Value>(
        &harness.app,
        Method::POST,
        &format!(
            "/api/v1/agents/{}/task-proposals",
            fixture.native_identity_id
        ),
        rest_request,
        StatusCode::OK,
    )
    .await;
    assert_eq!(rest_replay["task"]["title"], "REST proposal");
    assert_eq!(rest_replay["receipt_id"], rest_proposal["receipt_id"]);
    assert_eq!(rest_replay["status"], "succeeded");
    assert_eq!(rest_replay["replayed"], true);
    assert_eq!(rest_replay["input_digest"], rest_proposal["input_digest"]);

    // MCP exposes `forge_create_task`, a separate direct TaskService create
    // API rather than the `task.propose` approval/receipt operation. Its
    // result is intentionally a Task JSON value with no action/execution id;
    // this is an explicit surface distinction, not an adapter parity gap.
    let mcp_result = mcp_call(
        &harness.app,
        &format!("/mcp?project_id={}", fixture.project_id),
        json!({
            "jsonrpc": "2.0",
            "id": 20,
            "method": "tools/call",
            "params": {
                "name": "forge_create_task",
                "arguments": {
                    "project_id": fixture.project_id,
                    "title": "MCP direct task",
                    "description": "characterize direct creation",
                    "type": "task",
                    "priority": 1
                }
            }
        }),
    )
    .await;
    let mcp_task: Value = serde_json::from_str(
        mcp_result["result"]["content"][0]["text"]
            .as_str()
            .expect("MCP task text"),
    )
    .expect("MCP task JSON");
    assert_eq!(mcp_task["title"], "MCP direct task");
    assert!(mcp_task["action_id"].is_null());
    assert!(mcp_task["execution_id"].is_null());
    let mcp_task_id = mcp_task["id"].as_str().expect("MCP task id");
    assert_ne!(mcp_task_id, native_task_id);
    let mcp_action_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_action WHERE dedupe_key = 'adapter-parity-task-mcp'",
    )
    .fetch_one(harness.state.db.pool())
    .await
    .expect("MCP action count");
    assert_eq!(
        mcp_action_count, 0,
        "MCP direct creation has no proposal receipt"
    );

    // A known MCP tool keeps domain validation in-band as a structured
    // outcome. Unknown tools remain top-level JSON-RPC method-not-found
    // errors (as characterized by the baseline call above).
    let mcp_invalid = mcp_call(
        &harness.app,
        &format!("/mcp?project_id={}", fixture.project_id),
        json!({
            "jsonrpc": "2.0",
            "id": 21,
            "method": "tools/call",
            "params": {
                "name": "forge_create_task",
                "arguments": { "project_id": fixture.project_id }
            }
        }),
    )
    .await;
    assert_eq!(mcp_invalid["result"]["isError"], true);
    let mcp_invalid_outcome: Value = serde_json::from_str(
        mcp_invalid["result"]["content"][0]["text"]
            .as_str()
            .expect("MCP validation outcome text"),
    )
    .expect("MCP validation outcome JSON");
    assert_eq!(mcp_invalid_outcome["code"], "validation_error");
    assert_eq!(mcp_invalid_outcome["status"], "failed");
    assert_eq!(mcp_invalid_outcome["operation"], "forge_create_task");
    assert_eq!(mcp_invalid_outcome["scope"]["scope_type"], "project");
    assert_eq!(mcp_invalid_outcome["scope"]["scope_id"], fixture.project_id);

    let native_invalid = ForgeToolProvider::propose(
        &provider,
        &fixture.native_identity_id,
        &scope,
        "task.propose",
        json!({
            "payload": { "description": "missing title" },
            "dedupe_key": "adapter-parity-task-native-invalid",
            "correlation_id": "adapter-parity-task-native-invalid-correlation"
        }),
    )
    .await
    .expect_err("native invalid task payload must be rejected");
    match native_invalid {
        forge_agent_host::AgentHostError::StructuredOutcome(outcome) => {
            assert_eq!(outcome.code.as_str(), "validation_error");
            assert_eq!(outcome.status.as_str(), "failed");
            assert_eq!(outcome.operation, "task.propose");
            assert_eq!(outcome.scope.scope_type.as_str(), "project");
            assert_eq!(outcome.scope.scope_id, fixture.project_id);
            assert_eq!(
                outcome.correlation_id,
                "adapter-parity-task-native-invalid-correlation"
            );
            assert!(!outcome.replayed);
            assert!(outcome.receipt_id.is_none());
            assert_eq!(
                outcome.retry.as_ref().map(|retry| retry.action.as_str()),
                Some("correct_input")
            );
        }
        other => panic!("native invalid task payload returned {other:?}"),
    }
}

fn assert_native_success_outcome<'a>(
    outcome: &'a Value,
    operation: &str,
    scope_type: &str,
    scope_id: &str,
    correlation_id: &str,
) -> &'a Value {
    assert_eq!(outcome["code"], "ok");
    assert_eq!(outcome["status"], "succeeded");
    assert_eq!(outcome["operation"], operation);
    assert_eq!(outcome["scope"]["scope_type"], scope_type);
    assert_eq!(outcome["scope"]["scope_id"], scope_id);
    assert_eq!(outcome["correlation_id"], correlation_id);
    assert_eq!(outcome["replayed"], false);

    let receipt_id = outcome["receipt_id"]
        .as_str()
        .expect("successful native outcome receipt id");
    assert!(!receipt_id.is_empty());
    let result = &outcome["result"];
    assert!(result.is_object(), "successful native outcome result");
    assert_eq!(result["receipt_id"], outcome["receipt_id"]);
    result
}

async fn charter_backed_project(harness: &common::Harness, name: &str) -> CharterFixture {
    let project = common::json_request::<Value>(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": name }),
        StatusCode::OK,
    )
    .await;
    let project_id = project["id"].as_str().expect("project id").to_owned();
    let charter_id = format!("{project_id}-charter");
    let charter_revision_id = format!("{charter_id}-revision-1");
    let charter_content_digest = format!("{charter_id}-content-digest");
    let charter_render_digest = format!("{charter_id}-render-digest");
    let now = db::now_rfc3339();

    sqlx::query(
        "INSERT INTO project_charter (
             id, account_id, project_id, project_mode, maturity, lifecycle,
             version, created_at, updated_at
         ) VALUES (?, 'test-user-id', ?, 'compact', 'prototype', 'attached', 1, ?, ?)",
    )
    .bind(&charter_id)
    .bind(&project_id)
    .bind(&now)
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("charter fixture");
    sqlx::query(
        "INSERT INTO project_charter_revision (
             id, charter_id, revision, base_revision, lifecycle, schema_version,
             render_version, content_json, rendered_view, change_summary,
             author_type, author_id, source_refs_json, content_digest,
             rendered_digest, created_at
         ) VALUES (?, ?, 1, 0, 'approved', 'forge.project-charter/v1',
                   'forge.project-charter-render/v1', '{}', '# Project',
                   'adapter parity fixture', 'user', 'test-user-id', '[]', ?, ?, ?)",
    )
    .bind(&charter_revision_id)
    .bind(&charter_id)
    .bind(&charter_content_digest)
    .bind(&charter_render_digest)
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("charter revision fixture");
    sqlx::query(
        "UPDATE project_charter
         SET current_approved_revision_id = ?, current_draft_revision_id = ?, version = 2
         WHERE id = ?",
    )
    .bind(&charter_revision_id)
    .bind(&charter_revision_id)
    .bind(&charter_id)
    .execute(harness.state.db.pool())
    .await
    .expect("charter pointer fixture");
    sqlx::query(
        "UPDATE project
         SET current_charter_id = ?, current_charter_revision_id = ?,
             current_charter_version = 1, charter_status = 'charter_backed',
             charter_setup_required = 0, version = version + 1, updated_at = ?
         WHERE id = ?",
    )
    .bind(&charter_id)
    .bind(&charter_revision_id)
    .bind(&now)
    .bind(&project_id)
    .execute(harness.state.db.pool())
    .await
    .expect("project Charter pointer fixture");

    let milestone_id = format!("{project_id}-milestone");
    let milestone_revision_id = format!("{milestone_id}-revision-1");
    sqlx::query(
        "INSERT INTO project_milestone (
             id, project_id, milestone_sequence, milestone_key, display_label,
             lifecycle, blocker_reason_json, stale_reason_json,
             reconciliation_reason_json, version, created_at, updated_at
         ) VALUES (?, ?, 1, 'M001', 'Adapter parity milestone', 'planned',
                   '[]', '[]', '[]', 1, ?, ?)",
    )
    .bind(&milestone_id)
    .bind(&project_id)
    .bind(&now)
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("milestone fixture");
    sqlx::query(
        "INSERT INTO project_milestone_revision (
             id, milestone_id, revision, base_revision, base_revision_id,
             lifecycle, display_label, outcome, included_scope_json,
             excluded_scope_json, charter_revision_id, document_revisions_json,
             task_selection_json, dependencies_json, risks_json,
             acceptance_checks_json, evidence_requirements_json,
             known_issues_json, change_summary, schema_version, render_version,
             rendered_view, content_digest, rendered_digest, author_type,
             author_id, source_refs_json, created_at
         ) VALUES (?, ?, 1, 0, NULL, 'approved', 'Adapter parity milestone',
                   'Adapter parity acceptance', '[]', '[]', ?, '[]', '[]',
                   '[]', '[]', '[]', '[]', '[]', 'fixture',
                   'forge.milestone-definition/v1',
                   'forge.milestone-definition-render/v1', '# Milestone',
                   ?, ?, 'user', ?, '[]', ?)",
    )
    .bind(&milestone_revision_id)
    .bind(&milestone_id)
    .bind(&charter_revision_id)
    .bind(format!("{milestone_revision_id}-content"))
    .bind(format!("{milestone_revision_id}-render"))
    .bind("test-user-id")
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("milestone revision fixture");
    sqlx::query(
        "UPDATE project_milestone
         SET current_definition_revision_id = ?
         WHERE id = ?",
    )
    .bind(&milestone_revision_id)
    .bind(&milestone_id)
    .execute(harness.state.db.pool())
    .await
    .expect("milestone pointer fixture");

    let native_identity_id = format!("{project_id}-native");
    let native_profile_id = format!("{native_identity_id}-profile");
    let permissions = r#"{"permissions":["read_project","propose_project","propose_task"]}"#;
    AgentRepo::create_identity_with_profile(
        &*harness.state.db,
        CreateAgentIdentity {
            id: native_identity_id.clone(),
            name: "Adapter parity native".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some("test-user-id".to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: permissions.to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: native_profile_id.clone(),
            identity_id: native_identity_id.clone(),
            backend_kind: "native".to_owned(),
            executor_type: "native".to_owned(),
            provider: None,
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: permissions.to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("native identity fixture");

    let setup =
        ProjectAgentBindingRepo::get_active_project_binding(&*harness.state.db, &project_id)
            .await
            .expect("setup binding lookup")
            .expect("setup binding");
    ProjectAgentBindingRepo::replace_project_binding(
        &*harness.state.db,
        ReplaceProjectAgentBinding {
            project_id: project_id.clone(),
            expected_version: setup.version,
            replacement: CreateProjectAgentBinding {
                id: format!("{project_id}-native-binding"),
                project_id: project_id.clone(),
                identity_id: Some(native_identity_id.clone()),
                profile_id: Some(native_profile_id),
                state: "active".to_owned(),
                autonomy_policy_json: "{}".to_owned(),
                permission_ceiling_json: permissions.to_owned(),
                subscriptions_json: "[]".to_owned(),
                wake_budget: 10,
                operating_skill_revision_id: None,
                policy_revision: "default".to_owned(),
                policy_digest: String::new(),
                charter_id: None,
                charter_revision_id: None,
                charter_setup_required: true,
                admission_receipt_id: None,
                charter_approval_id: None,
                created_at: now.clone(),
                updated_at: now,
            },
            replacement_reason: Some("adapter parity fixture".to_owned()),
        },
    )
    .await
    .expect("native project binding fixture");

    CharterFixture {
        project_id,
        charter_id,
        charter_revision_id,
        charter_content_digest,
        charter_render_digest,
        milestone_id,
        milestone_revision_id,
        native_identity_id,
    }
}

fn native_provider(harness: &common::Harness) -> services::CoordinationToolProvider {
    let provider = services::CoordinationToolProvider::new(harness.state.db.clone());
    provider.set_task_service(harness.state.task_service.clone());
    provider
}

async fn mcp_call(app: &Router, uri: &str, body: Value) -> Value {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", common::test_jwt()),
                )
                .body(Body::from(serde_json::to_string(&body).expect("MCP body")))
                .expect("MCP request"),
        )
        .await
        .expect("MCP response");
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("MCP response body");
    serde_json::from_slice(&bytes).expect("MCP response JSON")
}
