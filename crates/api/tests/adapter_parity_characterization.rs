//! Characterization coverage for the current REST/native/MCP orchestration
//! adapter seams.  These assertions intentionally describe today's behavior;
//! Gate A can update them to the shared parity contract once that boundary is
//! implemented.

mod common;

use api_types::{
    AdaptiveEnvelope, ArtifactRef, ExecutionBaselineContent, ExecutionBaselineReleasePolicy,
};
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use chrono::Utc;
use db::{
    AgentRepo, AgentStatus, CreateAgentIdentity, CreateAgentProfile, CreateProjectAgentBinding,
    ProjectAgentBindingRepo, ReplaceProjectAgentBinding,
};
use forge_agent_host::{
    CanonicalScope, CanonicalScopeType, ForgeToolProvider, WorkspaceAccess,
    PROJECT_EXECUTION_BASELINE_OPERATION,
};
use serde_json::{json, Value};
use tower::ServiceExt;

#[derive(Debug, Clone)]
struct CharterFixture {
    project_id: String,
    charter_id: String,
    charter_revision_id: String,
    charter_content_digest: String,
    charter_render_digest: String,
    milestone_id: String,
    milestone_revision_id: String,
    native_identity_id: String,
}

#[tokio::test]
async fn baseline_rest_and_native_adapters_share_command_parity() {
    let workspace = common::TestDir::new("adapter-parity-baseline");
    let harness = common::test_app(workspace.path(), "adapter-parity-baseline").await;
    let fixture = charter_backed_project(&harness, "adapter-parity-baseline").await;
    let incomplete = incomplete_baseline_content(&fixture);
    let incomplete_render = services::render_execution_baseline(&incomplete).expect("render draft");

    // The collection endpoint is now the same explicit save-draft command as
    // the native `draft_revision` action.  A first draft is a real revision,
    // not a shell that silently becomes proposed on the next transport call.
    let rest_draft_request = json!({
        "mutation": {
            "expected_version": 0,
            "idempotency_key": "adapter-parity-baseline-rest-draft",
            "authorization": user_authorization(
                "project.execution_baseline.save_draft",
                "adapter-parity-baseline-rest-draft-event"
            )
        },
        "operation": "save_draft",
        "base_revision_id": null,
        "content": incomplete,
        "rendered_view": incomplete_render.rendered_view,
        "render_version": services::EXECUTION_BASELINE_RENDER_VERSION,
        "content_digest": incomplete_render.content_digest,
        "render_digest": incomplete_render.render_digest,
        "provenance": user_provenance("REST baseline draft")
    });
    let rest_draft = common::json_request::<Value>(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{}/execution-baseline", fixture.project_id),
        rest_draft_request.clone(),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(rest_draft["baseline"]["lifecycle"], "draft");
    assert_eq!(rest_draft["current_revision"]["lifecycle"], "draft");
    assert_eq!(rest_draft["requires_user_authorization"], false);
    assert!(rest_draft["approval_target"].is_null());
    let rest_baseline_id = rest_draft["baseline"]["id"]
        .as_str()
        .expect("REST baseline id")
        .to_owned();
    let rest_baseline_version = rest_draft["baseline"]["version"]
        .as_i64()
        .expect("REST baseline version");
    let rest_draft_revision_id = rest_draft["current_revision"]["id"]
        .as_str()
        .expect("REST draft revision id")
        .to_owned();

    // Replaying the exact request is the response-loss path for REST.  The
    // response is reconstructed from the frozen receipt and no revision is
    // appended.
    let rest_replay = common::json_request::<Value>(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{}/execution-baseline", fixture.project_id),
        rest_draft_request,
        StatusCode::OK,
    )
    .await;
    assert_eq!(rest_replay, rest_draft);
    let rest_revision_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_execution_baseline_revision WHERE baseline_id = ?",
    )
    .bind(&rest_baseline_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("REST draft revision count");
    assert_eq!(rest_revision_count, 1);

    // The native adapter receives the exact same canonical rendered view and
    // digests. Its direct receipt freezes the domain result without creating
    // an AgentAction or AgentActionExecution envelope.
    let provider = native_provider(&harness);
    let scope = CanonicalScope {
        scope_type: CanonicalScopeType::Project,
        scope_id: fixture.project_id.clone(),
        workspace_access: WorkspaceAccess::Deny,
    };
    let native_draft_request = json!({
        "payload": {
            "action": "draft_revision",
            "expected_baseline_version": 0,
            "content": incomplete,
            "rendered_view": incomplete_render.rendered_view,
            "render_version": services::EXECUTION_BASELINE_RENDER_VERSION,
            "content_digest": incomplete_render.content_digest,
            "render_digest": incomplete_render.render_digest,
            "provenance": agent_provenance(&fixture.native_identity_id)
        },
        "dedupe_key": "adapter-parity-baseline-native-draft",
        "correlation_id": "adapter-parity-baseline-native-draft-correlation"
    });
    let native_draft = ForgeToolProvider::propose(
        &provider,
        &fixture.native_identity_id,
        &scope,
        PROJECT_EXECUTION_BASELINE_OPERATION,
        native_draft_request.clone(),
    )
    .await
    .expect("native draft");
    let native_draft_result = assert_native_success_outcome(
        &native_draft,
        PROJECT_EXECUTION_BASELINE_OPERATION,
        "project",
        &fixture.project_id,
        "adapter-parity-baseline-native-draft-correlation",
    );
    assert_eq!(native_draft_result["materialized"], true);
    assert_eq!(native_draft_result["domain_committed"], true);
    assert_eq!(native_draft_result["requires_user_authorization"], false);
    assert_eq!(native_draft_result["domain_result"]["lifecycle"], "draft");
    assert_eq!(
        native_draft_result["domain_result"]["requires_user_authorization"],
        false
    );
    let native_baseline_id = native_draft_result["domain_result"]["baseline_id"]
        .as_str()
        .expect("native baseline id")
        .to_owned();
    let native_baseline_version = native_draft_result["domain_result"]["baseline_version"]
        .as_i64()
        .expect("native baseline version");
    let native_draft_revision_id = native_draft_result["domain_result"]["revision_id"]
        .as_str()
        .expect("native draft revision id")
        .to_owned();
    let native_receipt_id = native_draft["receipt_id"]
        .as_str()
        .expect("native receipt id")
        .to_owned();
    let actionless_receipt_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM command_receipt
         WHERE id = ? AND agent_action_execution_id IS NULL",
    )
    .bind(&native_receipt_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("native direct receipt lookup");
    assert_eq!(actionless_receipt_count, 1);
    let direct_action_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_action
         WHERE operation = ? AND dedupe_key = ?",
    )
    .bind(PROJECT_EXECUTION_BASELINE_OPERATION)
    .bind("adapter-parity-baseline-native-draft")
    .fetch_one(harness.state.db.pool())
    .await
    .expect("native direct action count");
    assert_eq!(
        direct_action_count, 0,
        "direct native baseline commands do not enqueue actions"
    );

    // Rotate the Project binding after the native response has committed. A
    // native response-loss retry must resolve the direct receipt before
    // checking the current binding and return the exact frozen result.
    let rotated_identity_id = rotate_native_binding(&harness, &fixture, "rotated").await;
    let native_replay = ForgeToolProvider::propose(
        &provider,
        &fixture.native_identity_id,
        &scope,
        PROJECT_EXECUTION_BASELINE_OPERATION,
        native_draft_request,
    )
    .await
    .expect("native draft replay after binding rotation");
    assert_eq!(native_replay["code"], "ok");
    assert_eq!(native_replay["status"], "succeeded");
    assert_eq!(native_replay["operation"], native_draft["operation"]);
    assert_eq!(native_replay["scope"], native_draft["scope"]);
    assert_eq!(
        native_replay["correlation_id"],
        native_draft["correlation_id"]
    );
    assert_eq!(native_replay["receipt_id"], native_draft["receipt_id"]);
    assert_eq!(native_replay["event_id"], native_draft["event_id"]);
    assert_eq!(
        native_replay["result"]["domain_result"],
        native_draft["result"]["domain_result"]
    );
    assert_eq!(native_replay["replayed"], true);
    let native_revision_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_execution_baseline_revision WHERE baseline_id = ?",
    )
    .bind(&native_baseline_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("native draft revision count");
    assert_eq!(native_revision_count, 1);

    // Incomplete proposals fail at the same shared validation boundary. Both
    // adapters use the stable validation_error category, and neither creates
    // a proposed revision.
    let rest_incomplete_proposal = json!({
        "mutation": {
            "expected_version": rest_baseline_version,
            "idempotency_key": "adapter-parity-baseline-rest-incomplete-proposal",
            "authorization": user_authorization(
                "project.execution_baseline.propose_for_approval",
                "adapter-parity-baseline-rest-incomplete-proposal-event"
            )
        },
        "operation": "propose_for_approval",
        "base_revision_id": rest_draft_revision_id,
        "content": incomplete,
        "rendered_view": incomplete_render.rendered_view,
        "render_version": services::EXECUTION_BASELINE_RENDER_VERSION,
        "content_digest": incomplete_render.content_digest,
        "render_digest": incomplete_render.render_digest,
        "provenance": user_provenance("REST incomplete baseline proposal")
    });
    let rest_rejection = common::json_request::<Value>(
        &harness.app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/execution-baseline/{}/revisions",
            fixture.project_id, rest_baseline_id
        ),
        rest_incomplete_proposal,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(rest_rejection["code"], "validation_error");
    assert_eq!(
        rest_rejection["message"],
        "execution baseline requires plan items, milestones, release policy, capability classes, and risk classes"
    );
    let native_incomplete = ForgeToolProvider::propose(
        &provider,
        &rotated_identity_id,
        &scope,
        PROJECT_EXECUTION_BASELINE_OPERATION,
        json!({
            "payload": {
                "action": "propose_approval",
                "baseline_id": native_baseline_id,
                "base_revision_id": native_draft_revision_id,
                "expected_baseline_version": native_baseline_version,
                "content": incomplete,
                "rendered_view": incomplete_render.rendered_view,
                "render_version": services::EXECUTION_BASELINE_RENDER_VERSION,
                "content_digest": incomplete_render.content_digest,
                "render_digest": incomplete_render.render_digest,
                "provenance": agent_provenance(&rotated_identity_id)
            },
            "dedupe_key": "adapter-parity-baseline-native-incomplete-proposal",
            "correlation_id": "adapter-parity-baseline-native-incomplete-proposal-correlation"
        }),
    )
    .await
    .expect_err("native incomplete proposal must fail");
    match native_incomplete {
        forge_agent_host::AgentHostError::StructuredOutcome(outcome) => {
            assert_eq!(outcome.code.as_str(), "validation_error");
            assert_eq!(outcome.status.as_str(), "failed");
            assert_eq!(outcome.operation, PROJECT_EXECUTION_BASELINE_OPERATION);
            assert_eq!(outcome.scope.scope_type.as_str(), "project");
            assert_eq!(outcome.scope.scope_id, fixture.project_id);
            assert_eq!(
                outcome.correlation_id,
                "adapter-parity-baseline-native-incomplete-proposal-correlation"
            );
            assert!(!outcome.replayed);
            assert!(outcome.receipt_id.is_none());
            assert_eq!(
                outcome.retry.as_ref().map(|retry| retry.action.as_str()),
                Some("correct_input")
            );
        }
        other => panic!("native incomplete proposal returned {other:?}"),
    }
    let rest_proposed_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_execution_baseline_revision
         WHERE baseline_id = ? AND lifecycle = 'proposed'",
    )
    .bind(&rest_baseline_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("REST proposed revision count");
    let native_proposed_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_execution_baseline_revision
         WHERE baseline_id = ? AND lifecycle = 'proposed'",
    )
    .bind(&native_baseline_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("native proposed revision count");
    assert_eq!(rest_proposed_count, 0);
    assert_eq!(native_proposed_count, 0);

    // A complete proposal through either adapter returns the exact immutable
    // approval target and explicitly asks the interactive user to authorize.
    let complete = complete_baseline_content(&fixture);
    let complete_render = services::render_execution_baseline(&complete).expect("render proposal");
    let rest_complete_request = json!({
        "mutation": {
            "expected_version": rest_baseline_version,
            "idempotency_key": "adapter-parity-baseline-rest-complete-proposal",
            "authorization": user_authorization(
                "project.execution_baseline.propose_for_approval",
                "adapter-parity-baseline-rest-complete-proposal-event"
            )
        },
        "operation": "propose_for_approval",
        "base_revision_id": rest_draft_revision_id,
        "content": complete,
        "rendered_view": complete_render.rendered_view,
        "render_version": services::EXECUTION_BASELINE_RENDER_VERSION,
        "content_digest": complete_render.content_digest,
        "render_digest": complete_render.render_digest,
        "provenance": user_provenance("REST complete baseline proposal")
    });
    let rest_complete = common::json_request::<Value>(
        &harness.app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/execution-baseline/{}/revisions",
            fixture.project_id, rest_baseline_id
        ),
        rest_complete_request.clone(),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(rest_complete["baseline"]["lifecycle"], "proposed");
    assert_eq!(rest_complete["requires_user_authorization"], true);
    assert_eq!(
        rest_complete["approval_target"]["requires_user_authorization"],
        true
    );
    assert_eq!(
        rest_complete["approval_target"]["content"],
        serde_json::to_value(&complete).expect("complete content JSON")
    );
    assert_eq!(
        rest_complete["approval_target"]["rendered_view"],
        complete_render.rendered_view
    );
    assert_eq!(
        rest_complete["approval_target"]["render_version"],
        services::EXECUTION_BASELINE_RENDER_VERSION
    );
    assert_eq!(
        rest_complete["approval_target"]["content_digest"],
        complete_render.content_digest
    );
    assert_eq!(
        rest_complete["approval_target"]["render_digest"],
        complete_render.render_digest
    );
    let rest_complete_replay = common::json_request::<Value>(
        &harness.app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/execution-baseline/{}/revisions",
            fixture.project_id, rest_baseline_id
        ),
        rest_complete_request,
        StatusCode::OK,
    )
    .await;
    assert_eq!(rest_complete_replay, rest_complete);

    let native_complete = ForgeToolProvider::propose(
        &provider,
        &rotated_identity_id,
        &scope,
        PROJECT_EXECUTION_BASELINE_OPERATION,
        json!({
            "payload": {
                "action": "propose_approval",
                "baseline_id": native_baseline_id,
                "base_revision_id": native_draft_revision_id,
                "expected_baseline_version": native_baseline_version,
                "content": complete,
                "rendered_view": complete_render.rendered_view,
                "render_version": services::EXECUTION_BASELINE_RENDER_VERSION,
                "content_digest": complete_render.content_digest,
                "render_digest": complete_render.render_digest,
                "provenance": agent_provenance(&rotated_identity_id)
            },
            "dedupe_key": "adapter-parity-baseline-native-complete-proposal",
            "correlation_id": "adapter-parity-baseline-native-complete-proposal-correlation"
        }),
    )
    .await
    .expect("native complete proposal");
    assert_eq!(native_complete["code"], "approval_required");
    assert_eq!(native_complete["status"], "approval_required");
    assert_eq!(
        native_complete["operation"],
        PROJECT_EXECUTION_BASELINE_OPERATION
    );
    assert_eq!(native_complete["scope"]["scope_type"], "project");
    assert_eq!(native_complete["scope"]["scope_id"], fixture.project_id);
    assert_eq!(
        native_complete["correlation_id"],
        "adapter-parity-baseline-native-complete-proposal-correlation"
    );
    assert_eq!(native_complete["replayed"], false);
    assert!(native_complete["receipt_id"].as_str().is_some());
    assert_eq!(
        native_complete["receipt_id"],
        native_complete["result"]["receipt_id"]
    );
    let native_complete_result = native_complete["result"].clone();
    assert_eq!(native_complete_result["materialized"], true);
    assert_eq!(native_complete_result["domain_committed"], true);
    assert_eq!(native_complete_result["requires_user_authorization"], true);
    assert_eq!(
        native_complete_result["domain_result"]["lifecycle"],
        "proposed"
    );
    assert_eq!(
        native_complete_result["domain_result"]["requires_user_authorization"],
        true
    );
    assert_eq!(
        native_complete_result["domain_result"]["approval_target"]["baseline_id"],
        native_baseline_id
    );
    assert_eq!(
        native_complete_result["domain_result"]["approval_target"]["content"],
        serde_json::to_value(&complete).expect("native complete content JSON")
    );
    assert_eq!(
        native_complete_result["domain_result"]["approval_target"]["rendered_view"],
        complete_render.rendered_view
    );
    assert_eq!(
        native_complete_result["domain_result"]["approval_target"]["render_version"],
        services::EXECUTION_BASELINE_RENDER_VERSION
    );
    assert_eq!(
        native_complete_result["domain_result"]["approval_target"]["content_digest"],
        complete_render.content_digest
    );
    assert_eq!(
        native_complete_result["domain_result"]["approval_target"]["render_digest"],
        complete_render.render_digest
    );
    assert_eq!(
        native_complete["approval_target"]["target_type"],
        "execution_baseline"
    );
    assert_eq!(
        native_complete["approval_target"]["target_id"],
        native_complete_result["domain_result"]["approval_target"]["baseline_id"]
    );
    assert_eq!(
        native_complete["approval_target"]["operation"],
        PROJECT_EXECUTION_BASELINE_OPERATION
    );
    assert_eq!(
        native_complete["approval_target"]["version"],
        native_complete_result["domain_result"]["baseline_version"]
    );
    assert_eq!(
        native_complete["approval_target"]["revision_id"],
        native_complete_result["domain_result"]["approval_target"]["revision_id"]
    );
    assert_eq!(
        native_complete["approval_target"]["revision"],
        native_complete_result["domain_result"]["approval_target"]["revision"]
    );
    assert_eq!(
        native_complete["approval_target"]["content_digest"],
        rest_complete["approval_target"]["content_digest"]
    );
    assert_eq!(
        native_complete["approval_target"]["rendered_digest"],
        rest_complete["approval_target"]["render_digest"]
    );

    // MCP has no baseline proposal operation in its current descriptor or
    // dispatcher surface. Its unknown operation remains a JSON-RPC method
    // error; no adapter is added merely to make the catalog look symmetric.
    let tools = mcp_call(
        &harness.app,
        &format!("/mcp?project_id={}", fixture.project_id),
        json!({
            "jsonrpc": "2.0",
            "id": 10,
            "method": "tools/list",
            "params": {}
        }),
    )
    .await;
    let tool_names = tools["result"]["tools"]
        .as_array()
        .expect("MCP tool descriptors")
        .iter()
        .filter_map(|tool| tool["name"].as_str())
        .collect::<Vec<_>>();
    assert!(tool_names
        .iter()
        .all(|name| { !name.contains("baseline") && *name != "forge_propose_task" }));

    let mcp_baseline = mcp_call(
        &harness.app,
        &format!("/mcp?project_id={}", fixture.project_id),
        json!({
            "jsonrpc": "2.0",
            "id": 11,
            "method": "tools/call",
            "params": {
                "name": "forge_propose_execution_baseline",
                "arguments": { "baseline_id": rest_baseline_id }
            }
        }),
    )
    .await;
    assert_eq!(mcp_baseline["error"]["code"], -32601);
    assert_eq!(mcp_baseline["error"]["message"], "method not found");
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

fn incomplete_baseline_content(fixture: &CharterFixture) -> ExecutionBaselineContent {
    let release_policy = ExecutionBaselineReleasePolicy {
        schema_version: services::EXECUTION_BASELINE_RELEASE_POLICY_SCHEMA.to_owned(),
        revision: "adapter-parity-policy-r1".to_owned(),
        required_check_definition_revisions: vec![fixture.milestone_revision_id.clone()],
        reviewer_independence_rules: vec!["independent-reviewer".to_owned()],
        manual_attestation_rules: vec!["manual-attestation".to_owned()],
        waiver_rules: vec!["user-waiver".to_owned()],
        evidence_kinds: vec!["ci-log".to_owned(), "media".to_owned()],
        evidence_contexts: vec!["milestone".to_owned()],
        evidence_freshness_rules: vec!["current-milestone".to_owned()],
        dependency_rules: vec!["dependencies-green".to_owned()],
        stale_input_rules: vec!["stale-baseline-blocks".to_owned()],
        forbidden_side_effects: vec!["cross-project-write".to_owned()],
        known_issue_rules: vec!["known-issue-blocks".to_owned()],
        correction_rules: vec!["correction-required".to_owned()],
        purge_rules: vec!["purge-stale-evidence".to_owned()],
    };
    let release_policy_digest =
        services::execution_baseline::release_policy_digest(&release_policy)
            .expect("release policy digest");
    ExecutionBaselineContent {
        charter_revision: ArtifactRef {
            artifact_id: fixture.charter_id.clone(),
            revision_id: fixture.charter_revision_id.clone(),
            content_digest: fixture.charter_content_digest.clone(),
            render_version: Some("forge.project-charter-render/v1".to_owned()),
            render_digest: Some(fixture.charter_render_digest.clone()),
        },
        document_revisions: Vec::new(),
        plan_item_ids: Vec::new(),
        milestone_ids: Vec::new(),
        milestone_definition_revision_ids: Vec::new(),
        primary_milestone_id: None,
        release_policy_revision: release_policy.revision.clone(),
        release_policy_digest,
        release_policy,
        acceptance_evidence_matrix: Vec::new(),
        capability_classes: Vec::new(),
        risk_classes: Vec::new(),
        reviewer_independence_rules: Vec::new(),
        elevated_operations: Vec::new(),
        adaptive_envelope: AdaptiveEnvelope {
            allowed_task_operations: Vec::new(),
            fixed_outcomes: Vec::new(),
            fixed_acceptance: Vec::new(),
            fixed_risk_classes: Vec::new(),
            forbidden_side_effects: Vec::new(),
            elevated_operations: Vec::new(),
        },
        rollback_and_recovery: Vec::new(),
        exclusions: Vec::new(),
    }
}

fn complete_baseline_content(fixture: &CharterFixture) -> ExecutionBaselineContent {
    let mut content = incomplete_baseline_content(fixture);
    content.plan_item_ids = vec!["adapter-parity-plan-item".to_owned()];
    content.milestone_ids = vec![fixture.milestone_id.clone()];
    content.milestone_definition_revision_ids = vec![fixture.milestone_revision_id.clone()];
    content.primary_milestone_id = Some(fixture.milestone_id.clone());
    content.capability_classes = vec!["repository_write".to_owned()];
    content.risk_classes = vec!["low".to_owned()];
    content.adaptive_envelope.allowed_task_operations =
        api_types::AdaptiveTaskOperation::ALL.to_vec();
    content
}

async fn rotate_native_binding(
    harness: &common::Harness,
    fixture: &CharterFixture,
    suffix: &str,
) -> String {
    let identity_id = format!("{}-{suffix}", fixture.project_id);
    let profile_id = format!("{identity_id}-profile");
    let permissions = r#"{"permissions":["read_project","propose_project","propose_task"]}"#;
    let now = db::now_rfc3339();
    AgentRepo::create_identity_with_profile(
        &*harness.state.db,
        CreateAgentIdentity {
            id: identity_id.clone(),
            name: format!("Adapter parity native {suffix}"),
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
            id: profile_id.clone(),
            identity_id: identity_id.clone(),
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
    .expect("rotated native identity fixture");
    let binding = ProjectAgentBindingRepo::get_active_project_binding(
        &*harness.state.db,
        &fixture.project_id,
    )
    .await
    .expect("active binding lookup")
    .expect("active binding");
    ProjectAgentBindingRepo::replace_project_binding(
        &*harness.state.db,
        ReplaceProjectAgentBinding {
            project_id: fixture.project_id.clone(),
            expected_version: binding.version,
            replacement: CreateProjectAgentBinding {
                id: format!("{}-{suffix}-binding", fixture.project_id),
                project_id: fixture.project_id.clone(),
                identity_id: Some(identity_id.clone()),
                profile_id: Some(profile_id),
                state: "active".to_owned(),
                autonomy_policy_json: "{}".to_owned(),
                permission_ceiling_json: permissions.to_owned(),
                subscriptions_json: "[]".to_owned(),
                wake_budget: 10,
                created_at: now.clone(),
                updated_at: now,
            },
            replacement_reason: Some(format!("adapter parity binding rotation {suffix}")),
        },
    )
    .await
    .expect("rotate native binding");
    identity_id
}

fn user_authorization(action: &str, event_id: &str) -> Value {
    json!({
        "principal": { "kind": "user", "id": "test-user-id" },
        "authorization_basis": "adapter parity characterization",
        "action": action,
        "event_id": event_id,
        "occurred_at": Utc::now().to_rfc3339()
    })
}

fn user_provenance(summary: &str) -> Value {
    json!({
        "author": { "kind": "user", "id": "test-user-id" },
        "change_summary": summary,
        "source_refs": []
    })
}

fn agent_provenance(identity_id: &str) -> Value {
    json!({
        "author": { "kind": "agent", "id": identity_id },
        "change_summary": "Native incomplete baseline candidate",
        "source_refs": []
    })
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
