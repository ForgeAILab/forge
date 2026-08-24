#![allow(dead_code)]

//! Focused V076 acceptance coverage for the approved Main/Project orchestration
//! flow.  These tests intentionally use the public API surface for mutations;
//! SQL is used only for read-only projections which have no compact API
//! projection (for example, counting the atomic handoff rows).

mod common;

use api_types::ProjectCharterContent;
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use chrono::{Duration, Utc};
use forge_agent_host::{CanonicalScope, CanonicalScopeType, ForgeToolProvider, WorkspaceAccess};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::Row;
use tower::ServiceExt;

const PROVIDER_SECRET: &str = "v076-acceptance-provider-secret";

#[derive(Debug, Clone)]
struct GenesisFixture {
    project_id: String,
    project_version: i64,
    project_identity_id: String,
    project_profile_id: String,
    project_chat_id: String,
    main_chat_id: String,
    genesis_session_id: String,
    create_response: Value,
    create_request: Value,
    approval_id: String,
    create_idempotency_key: String,
    charter_id: String,
    charter_revision_id: String,
    charter_version: i64,
    charter_content_digest: String,
    charter_render_digest: String,
    milestone_id: String,
    milestone_version: i64,
    milestone_definition_revision_id: String,
    milestone_acceptance_check_id: String,
    milestone_acceptance_check_description: String,
}

#[derive(Debug, Clone)]
struct BaselineFixture {
    baseline_id: String,
    baseline_revision_id: String,
    baseline_version: i64,
    approval_expected_baseline_version: i64,
    approval_expected_project_version: i64,
    content_digest: String,
    render_digest: String,
    approval_id: String,
    approval_authorization: Value,
}

#[tokio::test]
async fn v076_genesis_handoff_is_atomic_and_legacy_adoption_is_explicit() {
    let workspace = common::TestDir::new("v076-genesis-handoff-adoption");
    let harness = common::test_app(workspace.path(), "v076-genesis-handoff-adoption").await;
    let app = &harness.app;
    let token = common::test_jwt();

    let genesis = create_genesis_project(app, &token, "v076-genesis").await;
    let created_project_id = genesis.project_id.clone();

    // Product Genesis no longer exposes the old mutable `/ready` transition;
    // exact Charter approval is the only route into Project creation.
    let legacy_ready = common::raw_empty_request(
        app,
        Method::POST,
        &format!(
            "/api/v1/account/main-agent/product-genesis/{}/ready",
            genesis.genesis_session_id
        ),
    )
    .await;
    assert_eq!(legacy_ready.status(), StatusCode::NOT_FOUND);

    let project_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM project WHERE id = ?")
        .bind(&created_project_id)
        .fetch_one(harness.state.db.pool())
        .await
        .expect("created project count");
    let chat_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_chat WHERE project_id = ? AND kind = 'project'",
    )
    .bind(&created_project_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("created project chat count");
    let binding_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_agent_binding WHERE project_id = ? AND state = 'active'",
    )
    .bind(&created_project_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("created project binding count");
    assert_eq!(project_count, 1);
    assert_eq!(chat_count, 1);
    assert_eq!(binding_count, 1);
    let approval = sqlx::query(
        "SELECT approval_type, lifecycle, approving_principal_type,
                approving_principal_id, consumed_project_id
         FROM project_charter_approval WHERE id = ?",
    )
    .bind(&genesis.approval_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("consumed user Charter approval");
    assert_eq!(
        approval.get::<String, _>("approval_type"),
        "project_creation"
    );
    assert_eq!(approval.get::<String, _>("lifecycle"), "consumed");
    assert_eq!(
        approval.get::<String, _>("approving_principal_type"),
        "user"
    );
    assert_eq!(
        approval.get::<String, _>("approving_principal_id"),
        "test-user-id"
    );
    assert_eq!(
        approval
            .get::<Option<String>, _>("consumed_project_id")
            .as_deref(),
        Some(created_project_id.as_str())
    );

    let handoff = sqlx::query(
        "SELECT source_chat_id, target_chat_id, target_message_id, target_turn_job_id, status
         FROM agent_handoff WHERE target_chat_id = ?",
    )
    .bind(&genesis.project_chat_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("atomic handoff row");
    assert_eq!(
        handoff.get::<String, _>("source_chat_id"),
        genesis.main_chat_id
    );
    assert_eq!(
        handoff.get::<String, _>("target_chat_id"),
        genesis.project_chat_id
    );
    assert!(!handoff.get::<String, _>("target_message_id").is_empty());
    assert!(!handoff.get::<String, _>("target_turn_job_id").is_empty());
    assert_eq!(handoff.get::<String, _>("status"), "delivered");

    // Replaying the exact approval receipt is a no-op and returns the exact
    // original response.  Reusing the key for a different receipt conflicts.
    let create_body = genesis.create_request.clone();
    let first = request_json(
        app,
        Method::POST,
        "/api/v1/projects",
        &token,
        create_body.clone(),
        &[StatusCode::OK, StatusCode::CREATED],
    )
    .await;
    let replay = request_json(
        app,
        Method::POST,
        "/api/v1/projects",
        &token,
        create_body,
        &[StatusCode::OK, StatusCode::CREATED],
    )
    .await;
    // The command receipt freezes the materialized Project/handoff identity,
    // while execution_setup is a live provisioning projection that may have
    // advanced between the original response and a response-loss replay.
    for field in [
        "project_id",
        "project_agent_binding_id",
        "project_chat_id",
        "charter_id",
        "charter_revision_id",
        "handoff_id",
        "target_message_id",
        "target_turn_id",
    ] {
        assert_eq!(
            first[field], genesis.create_response[field],
            "frozen {field}"
        );
        assert_eq!(
            replay[field], genesis.create_response[field],
            "frozen {field}"
        );
    }
    assert!(first["execution_setup"].is_object());
    assert!(replay["execution_setup"].is_object());
    let current_setup =
        services::load_project_execution_setup(&harness.state.db, &genesis.project_id)
            .await
            .expect("replayed Project setup projection remains readable");
    assert_eq!(
        replay["execution_setup"],
        serde_json::to_value(current_setup).expect("setup projection serializes")
    );
    let conflict = request_json(
        app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({
            "approval_id": genesis.approval_id,
            "idempotency_key": genesis.create_idempotency_key,
            "authorization": {
                "principal": {"kind": "user", "id": "test-user-id"},
                "authorization_basis": "altered_authorization",
                "action": "product_genesis.create_project_from_approval",
                "event_id": "v076-create-altered-event",
                "occurred_at": Utc::now().to_rfc3339()
            }
        }),
        &[StatusCode::CONFLICT],
    )
    .await;
    assert!(conflict
        .get("message")
        .and_then(Value::as_str)
        .is_some_and(|message| message.contains("idempotency")));

    // Direct legacy projects remain setup_required until the user explicitly
    // adopts and approves a Charter revision.
    let legacy = request_json(
        app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({"name": "V076 legacy project"}),
        &[StatusCode::OK, StatusCode::CREATED],
    )
    .await;
    let legacy_id = required_string(&legacy, &["id"]);
    assert_eq!(
        required_string(&legacy, &["charter_status"]),
        "legacy_unverified"
    );
    assert_eq!(legacy["charter_setup_required"], json!(true));

    let legacy_agent = connect_agent(
        app,
        &token,
        "v076-legacy-project",
        &["read_project", "handoff", "propose_task"],
    )
    .await;
    let legacy_identity = required_string(&legacy_agent, &["agent", "id"]);
    let legacy_profile = required_string(&legacy_agent, &["profile", "id"]);
    let legacy_content = charter_content("V076 Legacy Adopted", "legacy adoption is explicit");
    let legacy_rendered = services::render_and_digest_charter(&legacy_content);
    let legacy_save = request_json(
        app,
        Method::POST,
        &format!("/api/v1/projects/{legacy_id}/charter/revisions"),
        &token,
        json!({
            "mutation": {
                "expected_version": 1,
                "idempotency_key": "v076-legacy-charter-save",
                "authorization": user_authorization("project_charter.revision.save", "v076-legacy-save")
            },
            "charter_id": "v076-legacy-charter",
            "project_mode": "compact",
            "maturity": "mvp",
            "content": legacy_content,
            "rendered_view": legacy_rendered.rendered_view,
            "render_version": legacy_rendered.render_version,
            "provenance": user_provenance("legacy Charter adoption")
        }),
        &[StatusCode::CREATED, StatusCode::OK],
    )
    .await;
    let legacy_revision_id = required_string(&legacy_save, &["id"]);
    let legacy_projection = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{legacy_id}/charter"),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let legacy_charter_version = legacy_projection["charter"]["version"]
        .as_i64()
        .expect("legacy charter version");
    let legacy_project = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{legacy_id}"),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let legacy_project_version = legacy_project["version"]
        .as_i64()
        .expect("legacy project version");
    let legacy_policy = project_policy_digest(&legacy_agent["profile"]["tool_policy"]);
    let adopted = request_json(
        app,
        Method::POST,
        &format!("/api/v1/projects/{legacy_id}/charter/revisions/{legacy_revision_id}/approve"),
        &token,
        json!({
            "mutation": {
                "expected_version": legacy_charter_version,
                "expected_digest": legacy_rendered.content_digest,
                "idempotency_key": "v076-legacy-charter-approve",
                "authorization": user_authorization("project_charter.approval", "v076-legacy-approve")
            },
            "charter_id": "v076-legacy-charter",
            "revision_id": legacy_revision_id,
            "content_digest": legacy_rendered.content_digest,
            "render_digest": legacy_rendered.render_digest,
            "expected_charter_version": legacy_charter_version,
            "expected_project_version": legacy_project_version,
            "approved_project_name": "V076 Legacy Adopted",
            "approved_project_slug": "v076-legacy-adopted",
            "project_mode": "compact",
            "selected_project_agent_identity_id": legacy_identity,
            "selected_project_agent_profile_revision_id": legacy_profile,
            "selected_project_agent_operating_skill_revision": "forge.project.orchestration/v1@4",
            "selected_project_agent_policy_digest": legacy_policy
        }),
        &[StatusCode::CREATED, StatusCode::OK],
    )
    .await;
    assert_eq!(adopted["approval_type"], json!("adoption"));
    let legacy_after = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{legacy_id}"),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(legacy_after["charter_status"], json!("charter_backed"));
    assert_eq!(legacy_after["charter_setup_required"], json!(false));
}

#[tokio::test]
async fn v076_project_task_is_gated_by_active_user_approved_baseline() {
    let workspace = common::TestDir::new("v076-baseline-task-gate");
    let harness = common::test_app(workspace.path(), "v076-baseline-task-gate").await;
    harness
        .state
        .workflow_template_service
        .initialize()
        .await
        .expect("builtin workflows initialize");
    let app = &harness.app;
    let token = common::test_jwt();
    let fixture = create_genesis_project(app, &token, "v076-baseline").await;

    // Genesis provisioning already registered an initialized primary
    // repository, which is what makes the Task below repository-capable.
    let baseline = create_baseline(app, &token, &fixture, false).await;
    let baseline_approval = sqlx::query(
        "SELECT principal_type, principal_id, lifecycle, baseline_id,
                revision_id, content_digest, rendered_digest
         FROM project_execution_baseline_approval WHERE id = ?",
    )
    .bind(&baseline.approval_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("user baseline approval");
    assert_eq!(baseline_approval.get::<String, _>("principal_type"), "user");
    assert_eq!(
        baseline_approval.get::<String, _>("principal_id"),
        "test-user-id"
    );
    assert_eq!(baseline_approval.get::<String, _>("lifecycle"), "active");
    assert_eq!(
        baseline_approval.get::<String, _>("baseline_id"),
        baseline.baseline_id
    );
    assert_eq!(
        baseline_approval.get::<String, _>("revision_id"),
        baseline.baseline_revision_id
    );
    assert_eq!(
        baseline_approval.get::<String, _>("content_digest"),
        baseline.content_digest
    );
    assert_eq!(
        baseline_approval.get::<String, _>("rendered_digest"),
        baseline.render_digest
    );
    let task = request_json(
        app,
        Method::POST,
        &format!("/api/v1/projects/{}/tasks", fixture.project_id),
        &token,
        json!({
            "title": "V076 repository task",
            "description": "Must remain blocked until the approved baseline is active.",
            "task_type": "task",
            "governance": {
                "charter_revision_id": fixture.charter_revision_id,
                "baseline_id": baseline.baseline_id,
                "baseline_revision_id": baseline.baseline_revision_id,
                "plan_item_id": "v076-plan-item-1",
                "milestone_id": fixture.milestone_id,
                "document_revision_ids": [],
                "capability_class": "repository_write",
                "risk_class": "low"
            }
        }),
        &[StatusCode::CREATED, StatusCode::OK],
    )
    .await;
    let task_id = required_string(&task, &["id"]);
    let governance = sqlx::query(
        "SELECT runnable, baseline_id, baseline_revision_id, milestone_id
         FROM project_task_governance WHERE task_id = ?",
    )
    .bind(&task_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("repository task governance row");
    assert_eq!(governance.get::<i64, _>("runnable"), 0);
    assert_eq!(
        governance.get::<String, _>("baseline_id"),
        baseline.baseline_id
    );
    assert_eq!(
        governance.get::<String, _>("baseline_revision_id"),
        baseline.baseline_revision_id
    );
    assert_eq!(
        governance.get::<String, _>("milestone_id"),
        fixture.milestone_id
    );

    activate_baseline(app, &token, &fixture, &baseline).await;
    let consumed: String = sqlx::query_scalar(
        "SELECT lifecycle FROM project_execution_baseline_approval WHERE id = ?",
    )
    .bind(&baseline.approval_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("consumed baseline approval");
    assert_eq!(consumed, "consumed");
    let promoted: i64 =
        sqlx::query_scalar("SELECT runnable FROM project_task_governance WHERE task_id = ?")
            .bind(&task_id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("promoted repository task governance");
    assert_eq!(promoted, 1);
}

#[tokio::test]
async fn v076_typed_project_proposals_are_scoped_and_task_materializes() {
    let workspace = common::TestDir::new("v076-typed-project-proposals");
    let harness = common::test_app(workspace.path(), "v076-typed-project-proposals").await;
    let app = &harness.app;
    let token = common::test_jwt();
    let fixture = create_genesis_project(app, &token, "v076-typed").await;
    let baseline = create_active_baseline(app, &token, &fixture).await;

    // A repository implementation proposal must carry the exact active
    // baseline plan item and milestone. The direct command still derives the
    // authenticated Project/agent scope and materializes the Task atomically.
    let governed_proposal = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/agents/{}/task-proposals",
            fixture.project_identity_id
        ),
        &token,
        json!({
            "project_id": fixture.project_id,
            "title": "V076 governed task",
            "description": "The active baseline supplies stable implementation provenance.",
            "role_assignments": [],
            "governance": {
                "charter_revision_id": fixture.charter_revision_id,
                "baseline_id": baseline.baseline_id,
                "baseline_revision_id": baseline.baseline_revision_id,
                "plan_item_id": "v076-plan-item-2",
                "milestone_id": fixture.milestone_id,
                "capability_class": "repository_write",
                "risk_class": "low",
                "provenance": {"source": "v076-active-baseline"}
            },
            "dedupe_key": "v076-task-proposal-governed",
            "correlation_id": "v076-task-proposal-governed-correlation"
        }),
        &[StatusCode::CREATED, StatusCode::OK],
    )
    .await;
    assert_eq!(governed_proposal["materialized"], json!(true));
    assert_eq!(governed_proposal["domain_committed"], json!(true));
    assert_eq!(governed_proposal["policy_result"], json!("allowed"));
    let derived = &governed_proposal;
    let derived_task_id = required_string(derived, &["task", "id"]);
    let derived_governance = sqlx::query(
        "SELECT charter_revision_id, baseline_id, baseline_revision_id, runnable
         FROM project_task_governance WHERE task_id = ?",
    )
    .bind(&derived_task_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("derived Task governance");
    assert_eq!(
        derived_governance.get::<String, _>("charter_revision_id"),
        fixture.charter_revision_id
    );
    assert_eq!(
        derived_governance
            .get::<Option<String>, _>("baseline_id")
            .as_deref(),
        Some(baseline.baseline_id.as_str())
    );
    assert_eq!(
        derived_governance
            .get::<Option<String>, _>("baseline_revision_id")
            .as_deref(),
        Some(baseline.baseline_revision_id.as_str())
    );
    assert_eq!(derived_governance.get::<i64, _>("runnable"), 1);

    // The typed Task endpoint is the authoritative Project Agent direct
    // command: one request commits the Task and its receipt.
    let proposal = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/agents/{}/task-proposals",
            fixture.project_identity_id
        ),
        &token,
        json!({
            "project_id": fixture.project_id,
            "title": "V076 typed task",
            "description": "Task proposal must materialize only through its executor.",
            "role_assignments": [],
            "governance": {
                "charter_revision_id": fixture.charter_revision_id,
                "baseline_id": baseline.baseline_id,
                "baseline_revision_id": baseline.baseline_revision_id,
                "plan_item_id": "v076-plan-item-1",
                "milestone_id": fixture.milestone_id,
                "capability_class": "repository_write",
                "risk_class": "low",
                "provenance": {"source": "v076-typed-task"}
            },
            "dedupe_key": "v076-task-proposal",
            "correlation_id": "v076-task-proposal-correlation"
        }),
        &[StatusCode::CREATED, StatusCode::OK],
    )
    .await;
    assert_eq!(proposal["operation"], json!("task.propose"));
    assert_eq!(proposal["materialized"], json!(true));
    assert_eq!(proposal["domain_committed"], json!(true));
    let task_id = required_string(&proposal, &["task", "id"]);
    let task_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task WHERE id = ? AND project_id = ?")
            .bind(&task_id)
            .bind(&fixture.project_id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("materialized task count");
    assert_eq!(task_count, 1);

    // Project-local typed proposals are bound to the selected Project and the
    // production provider immediately invokes the typed materializer when the
    // operation is admitted.  Create the target Document through its public
    // route first; the proposal itself must create the authoritative revision.
    let project = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{}", fixture.project_id),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let document = request_json(
        app,
        Method::POST,
        &format!("/api/v1/projects/{}/documents", fixture.project_id),
        &token,
        json!({
            "mutation": {
                "expected_version": project["version"],
                "idempotency_key": "v076-document-create",
                "authorization": user_authorization("project.document.create", "v076-document-create-event")
            },
            "kind": "delivery_brief",
            "title": "V076 delivery brief",
            "approval_policy": "user"
        }),
        &[StatusCode::CREATED, StatusCode::OK],
    )
    .await;
    let document_id = required_string(&document, &["id"]);
    let provider = services::CoordinationToolProvider::new(harness.state.db.clone());
    let scope = CanonicalScope {
        scope_type: CanonicalScopeType::Project,
        scope_id: fixture.project_id.clone(),
        workspace_access: WorkspaceAccess::Deny,
    };
    let document_proposal = provider
        .propose(
            &fixture.project_identity_id,
            &scope,
            forge_agent_host::PROJECT_DOCUMENT_OPERATION,
            json!({
                "payload": {
                    "action": "draft_revision",
                    "document_id": document_id,
                    "kind": "delivery_brief",
                    "title": "V076 delivery brief",
                    "base_revision_id": null,
                    "expected_document_version": document["version"],
                    "content": {}
                },
                "dedupe_key": "v076-document-proposal",
                "correlation_id": "v076-document-proposal-correlation"
            }),
        )
        .await
        .expect("typed Project Document proposal is admitted");
    assert_eq!(document_proposal["result"]["materialized"], json!(true));
    assert_eq!(document_proposal["result"]["domain_committed"], json!(true));
    let document_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_document_revision
         WHERE document_id = ? AND author_type = 'agent'",
    )
    .bind(&document_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("document proposal revision count");
    assert_eq!(document_count, 1);

    // User approval of a typed Project Document is a separate immutable
    // receipt. The same key must not authorize a changed user receipt or a
    // different exact revision/digest target.
    let document_projection = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/documents/{document_id}",
            fixture.project_id
        ),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let document_revision = sqlx::query(
        "SELECT id, content_digest, rendered_digest
         FROM project_document_revision
         WHERE document_id = ? ORDER BY revision DESC, id DESC LIMIT 1",
    )
    .bind(&document_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("typed Project Document revision target");
    let document_revision_id: String = document_revision.get("id");
    let document_content_digest: String = document_revision.get("content_digest");
    let document_render_digest: String = document_revision.get("rendered_digest");
    let document_approval_body = json!({
        "mutation": {
            "expected_version": document_projection["version"],
            "idempotency_key": "v076-document-approve",
            "authorization": user_authorization("project.document.approve", "v076-document-approve-event")
        },
        "document_id": document_id,
        "revision_id": document_revision_id,
        "content_digest": document_content_digest,
        "render_digest": document_render_digest
    });
    let document_approval = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/documents/{document_id}/approve",
            fixture.project_id
        ),
        &token,
        document_approval_body.clone(),
        &[StatusCode::CREATED],
    )
    .await;
    let document_approval_replay = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/documents/{document_id}/approve",
            fixture.project_id
        ),
        &token,
        document_approval_body.clone(),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(document_approval_replay, document_approval);
    for (label, altered) in user_authorization_replay_variants(&document_approval_body, false) {
        let conflict = request_json(
            app,
            Method::POST,
            &format!(
                "/api/v1/projects/{}/documents/{document_id}/approve",
                fixture.project_id
            ),
            &token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }
    for (label, altered) in [
        ("document approval revision", {
            let mut value = document_approval_body.clone();
            value["revision_id"] = json!("v076-different-document-revision");
            value
        }),
        ("document approval content digest", {
            let mut value = document_approval_body.clone();
            value["content_digest"] = json!("v076-different-document-digest");
            value
        }),
    ] {
        let conflict = request_json(
            app,
            Method::POST,
            &format!(
                "/api/v1/projects/{}/documents/{document_id}/approve",
                fixture.project_id
            ),
            &token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }
    let project_after_baseline = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{}", fixture.project_id),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let decision_proposal = provider
        .propose(
            &fixture.project_identity_id,
            &scope,
            forge_agent_host::PROJECT_DECISION_OPERATION,
            json!({
                "payload": {
                    "action": "record_candidate",
                    "question": "Should the V076 typed flow be accepted?",
                    "options": ["yes", "no"],
                    "selected_outcome": "yes",
                    "rationale": "The acceptance suite covers the approved path.",
                    "decision_class": "project_implementation",
                    "baseline_id": baseline.baseline_id,
                    "baseline_revision_id": baseline.baseline_revision_id,
                    "expected_project_version": project_after_baseline["version"],
                    "affected_artifact_refs": [],
                    "affected_task_ids": [],
                    "affected_milestone_ids": []
                },
                "dedupe_key": "v076-decision-proposal",
                "correlation_id": "v076-decision-proposal-correlation"
            }),
        )
        .await
        .expect("typed Project Decision proposal is admitted");
    assert_eq!(decision_proposal["result"]["materialized"], json!(true));
    assert_eq!(decision_proposal["result"]["domain_committed"], json!(true));
    let decision_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM project_decision_candidate WHERE project_id = ?")
            .bind(&fixture.project_id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("decision candidate count");
    assert_eq!(decision_count, 1);

    // A Project Agent cannot self-release through the user-only release API.
    let self_release = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/release",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        json!({
            "mutation": {
                "expected_version": fixture.milestone_version,
                "idempotency_key": "v076-agent-self-release",
                "authorization": {
                    "principal": {"kind": "agent", "id": fixture.project_identity_id},
                    "authorization_basis": "project_agent",
                    "action": "project.milestone.release",
                    "event_id": "v076-agent-self-release-event",
                    "occurred_at": Utc::now().to_rfc3339()
                }
            },
            "milestone_id": fixture.milestone_id,
            "readiness_snapshot_id": "not-ready",
            "readiness_digest": "not-ready"
        }),
        &[StatusCode::FORBIDDEN],
    )
    .await;
    assert!(self_release.get("message").is_some());
}

#[tokio::test]
async fn v076_project_evidence_is_scoped_pinned_and_user_attested() {
    let workspace = common::TestDir::new("v076-project-evidence");
    let harness = common::test_app(workspace.path(), "v076-project-evidence").await;
    let app = &harness.app;
    let token = common::test_jwt();
    let fixture = create_genesis_project(app, &token, "v076-evidence").await;
    let baseline = create_active_baseline(app, &token, &fixture).await;

    let project = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{}", fixture.project_id),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    // Project ownership is authoritative.  A lost/repaired membership row
    // must not lock the owner out of Project media routes.
    sqlx::query("DELETE FROM project_member WHERE project_id = ?")
        .bind(&fixture.project_id)
        .execute(harness.state.db.pool())
        .await
        .expect("remove redundant owner membership");
    let expected_project_version = project["version"]
        .as_i64()
        .expect("project version for media upload");
    let proof_bytes: &[u8] = b"\x89PNG\r\n\x1a\nV076 proof";
    let proof_checksum = hex::encode(Sha256::digest(proof_bytes));
    let upload_authorization =
        user_authorization("project.media.upload", "v076-project-proof-event");
    let asset = upload_project_media(
        app,
        &token,
        &fixture.project_id,
        expected_project_version,
        "v076-project-proof-upload",
        &upload_authorization,
        proof_bytes,
        StatusCode::CREATED,
    )
    .await;
    let asset_id = required_string(&asset, &["id"]);
    assert_eq!(asset["project_id"], json!(fixture.project_id));
    assert!(
        asset.get("storage_key").is_none(),
        "Project media responses must not expose internal storage handles"
    );
    assert_eq!(asset["checksum"], json!(proof_checksum));
    assert_eq!(asset["availability"], json!("available"));
    assert_eq!(
        asset["stable_project_url"],
        json!(format!(
            "/api/v1/projects/{}/media/{asset_id}",
            fixture.project_id
        ))
    );

    // A lost upload response is replay-safe even after the Project version is
    // unchanged only by the upload itself: the same idempotency key returns
    // the original asset identity and storage metadata.
    let upload_replay = upload_project_media(
        app,
        &token,
        &fixture.project_id,
        expected_project_version,
        "v076-project-proof-upload",
        &upload_authorization,
        proof_bytes,
        StatusCode::OK,
    )
    .await;
    assert_eq!(upload_replay, asset);

    let media_list = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{}/media", fixture.project_id),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(media_list["items"].as_array().map(Vec::len), Some(1));
    assert_eq!(media_list["items"][0]["id"], json!(asset_id));

    let (media_status, media_body) = raw_request(
        app,
        Method::GET,
        &format!("/api/v1/projects/{}/media/{asset_id}", fixture.project_id),
        &token,
        Body::empty(),
        None,
    )
    .await;
    assert_eq!(media_status, StatusCode::OK);
    assert_eq!(media_body, proof_bytes);

    // Media disposition is an audited user-only mutation. The tombstone
    // receipt is replay-exact even though the public response is only the
    // current media projection.
    let asset_version: i64 = sqlx::query_scalar("SELECT version FROM media_asset WHERE id = ?")
        .bind(&asset_id)
        .fetch_one(harness.state.db.pool())
        .await
        .expect("project media asset version");
    let tombstone_body = json!({
        "mutation": {
            "expected_version": asset_version,
            "idempotency_key": "v076-project-media-redact",
            "authorization": user_authorization(
                "project.media.redact",
                "v076-project-media-redact-event"
            )
        },
        "reason": "The user removed this bounded proof from active media."
    });
    let tombstoned = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/media/{asset_id}/redact",
            fixture.project_id
        ),
        &token,
        tombstone_body.clone(),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(tombstoned["availability"], json!("redacted"));
    let tombstone_replay = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/media/{asset_id}/redact",
            fixture.project_id
        ),
        &token,
        tombstone_body.clone(),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(tombstone_replay, tombstoned);
    for (label, altered) in user_authorization_replay_variants(&tombstone_body, false) {
        let conflict = request_json(
            app,
            Method::POST,
            &format!(
                "/api/v1/projects/{}/media/{asset_id}/redact",
                fixture.project_id
            ),
            &token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }
    for (label, altered) in [
        ("media tombstone version", {
            let mut value = tombstone_body.clone();
            value["mutation"]["expected_version"] = json!(asset_version + 1);
            value
        }),
        ("media tombstone reason", {
            let mut value = tombstone_body.clone();
            value["reason"] = json!("A different audited removal reason.");
            value
        }),
        ("media tombstone target asset", tombstone_body.clone()),
    ] {
        let target = if label == "media tombstone target asset" {
            format!(
                "/api/v1/projects/{}/media/{}/redact",
                fixture.project_id, "v076-different-media-asset"
            )
        } else {
            format!(
                "/api/v1/projects/{}/media/{asset_id}/redact",
                fixture.project_id
            )
        };
        let conflict = request_json(
            app,
            Method::POST,
            &target,
            &token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }

    // A Task upload is the same Project-owned asset, not a second byte copy.
    // The later evidence attachment deliberately references this existing
    // Task asset to cover the shared-media path used by release proof.
    let task = request_json(
        app,
        Method::POST,
        &format!("/api/v1/projects/{}/tasks", fixture.project_id),
        &token,
        json!({
            "title": "V076 shared evidence task",
            "description": "The Task upload is reused by Project evidence.",
            "governance": {
                "charter_revision_id": fixture.charter_revision_id,
                "baseline_id": baseline.baseline_id,
                "baseline_revision_id": baseline.baseline_revision_id,
                "plan_item_id": "v076-plan-item-1",
                "milestone_id": fixture.milestone_id,
                "document_revision_ids": [],
                "capability_class": "repository_write",
                "risk_class": "low"
            }
        }),
        &[StatusCode::OK, StatusCode::CREATED],
    )
    .await;
    let task_id = required_string(&task, &["id"]);
    let task_asset = upload_task_media(
        app,
        &token,
        &task_id,
        "v076-task-proof.png",
        "image/png",
        proof_bytes,
        StatusCode::CREATED,
    )
    .await;
    let task_asset_id = required_string(&task_asset, &["id"]);
    assert_eq!(task_asset["id"], json!(task_asset_id));
    let task_media_body = raw_request(
        app,
        Method::GET,
        &format!("/api/v1/media/{task_asset_id}"),
        &token,
        Body::empty(),
        None,
    )
    .await;
    assert_eq!(task_media_body.0, StatusCode::OK);
    assert_eq!(task_media_body.1, proof_bytes);
    let media_after_task_upload = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{}/media", fixture.project_id),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let task_asset_projection = media_after_task_upload["items"]
        .as_array()
        .and_then(|items| items.iter().find(|item| item["id"] == task_asset["id"]))
        .cloned()
        .expect("Task media is projected as a Project asset");
    assert_eq!(task_asset_projection["checksum"], json!(proof_checksum));
    assert_eq!(
        task_asset_projection["task_media_ids"],
        json!([task_asset_id.clone()])
    );

    // Governed Tasks gate milestone readiness; complete the helper Task so
    // the readiness evaluation below can compute "ready".
    let task_version = task["version"].as_i64().expect("task version");
    let done_task = request_json(
        app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/transition"),
        &token,
        json!({
            "status": "done",
            "version": task_version,
            "reason": "V076 evidence helper Task completed"
        }),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(done_task["task"]["status"], json!("done"));
    let milestone = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/milestones/{}",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let milestone_version = milestone["version"]
        .as_i64()
        .expect("milestone version before evidence attach");
    let attach_body = json!({
        "mutation": {
            "expected_version": milestone_version,
            "idempotency_key": "v076-project-proof-attach",
            "authorization": user_authorization(
                "project.evidence.attach",
                "v076-project-proof-attach-event"
            )
        },
        "milestone_id": fixture.milestone_id,
        "asset_id": task_asset_id,
        "acceptance_check_ids": [fixture.milestone_acceptance_check_id],
        "caption": "A bounded Project proof screenshot.",
        "kind": "screenshot",
        "checksum": proof_checksum
    });

    // Evidence attachment is an explicit user action. A Project Agent
    // principal cannot attest or attach a proof item through the user API.
    let mut agent_attach = attach_body.clone();
    agent_attach["mutation"]["authorization"] = json!({
        "principal": {"kind": "agent", "id": fixture.project_identity_id},
        "authorization_basis": "project_agent",
        "action": "project.evidence.attach",
        "event_id": "v076-agent-evidence-event",
        "occurred_at": Utc::now().to_rfc3339()
    });
    let agent_denial = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/evidence",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        agent_attach,
        &[StatusCode::FORBIDDEN],
    )
    .await;
    assert_eq!(agent_denial["code"], json!("authorization.invalid"));

    let evidence = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/evidence",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        attach_body.clone(),
        &[StatusCode::OK],
    )
    .await;
    let evidence_id = required_string(&evidence, &["id"]);
    assert_eq!(evidence["project_id"], json!(fixture.project_id));
    assert_eq!(evidence["milestone_id"], json!(fixture.milestone_id));
    assert_eq!(evidence["asset_id"], json!(task_asset_id));
    assert_eq!(evidence["checksum"], json!(proof_checksum));
    assert_eq!(evidence["author"]["kind"], json!("user"));
    let shared_asset_rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM media_asset WHERE id = ? AND project_id = ?")
            .bind(&task_asset_id)
            .bind(&fixture.project_id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("shared Project asset count");
    let shared_attachment_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_media_attachment
         WHERE asset_id = ? AND project_id = ? AND deleted_at IS NULL",
    )
    .bind(&task_asset_id)
    .bind(&fixture.project_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("shared Project attachment count");
    assert_eq!(shared_asset_rows, 1);
    assert_eq!(shared_attachment_rows, 2);

    let evidence_replay = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/evidence",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        attach_body,
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(evidence_replay, evidence);
    let evidence_list = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/milestones/{}/evidence",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(evidence_list["items"], json!([evidence.clone()]));
    let evidence_get = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/milestones/{}/evidence/{evidence_id}",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(evidence_get, evidence);

    // A Project-owned asset cannot be attached to another Project's
    // milestone, and the failure must not reveal its metadata.
    let other_project = request_json(
        app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({"name": "V076 evidence boundary project"}),
        &[StatusCode::OK, StatusCode::CREATED],
    )
    .await;
    let other_project_id = required_string(&other_project, &["id"]);
    let cross_scope = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{other_project_id}/milestones/{}/evidence",
            fixture.milestone_id
        ),
        &token,
        json!({
            "mutation": {
                "expected_version": milestone_version,
                "idempotency_key": "v076-cross-project-evidence",
                "authorization": user_authorization(
                    "project.evidence.attach",
                    "v076-cross-project-evidence-event"
                )
            },
            "milestone_id": fixture.milestone_id,
            "asset_id": task_asset_id,
            "acceptance_check_ids": [],
            "caption": "must not cross Project scope",
            "kind": "screenshot",
            "checksum": proof_checksum
        }),
        &[StatusCode::NOT_FOUND],
    )
    .await;
    assert_eq!(cross_scope["code"], json!("not_found"));
    assert!(!cross_scope.to_string().contains(&proof_checksum));

    // The evidence participates in the exact readiness candidate and is
    // pinned by the user-approved immutable release.
    record_passed_check(app, &harness, &token, &fixture, &baseline).await;
    let readiness = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/readiness",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        json!({
            "mutation": {
                "expected_version": milestone_version,
                "idempotency_key": "v076-evidence-readiness",
                "authorization": user_authorization(
                    "project.milestone.readiness",
                    "v076-evidence-readiness-event"
                )
            },
            "milestone_id": fixture.milestone_id,
            "baseline_id": baseline.baseline_id,
            "baseline_revision_id": baseline.baseline_revision_id,
            "release_policy_revision": "v076-policy-r1"
        }),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(readiness["result"], json!("ready"));
    assert_eq!(
        readiness["evidence_attachment_ids"],
        json!([evidence_id.clone()])
    );
    let readiness_snapshot_id = required_string(&readiness, &["id"]);
    let readiness_digest = required_string(&readiness, &["readiness_digest"]);
    let ready_milestone = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/milestones/{}",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let ready_version = ready_milestone["version"]
        .as_i64()
        .expect("ready milestone version");
    let release = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/release",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        json!({
            "mutation": {
                "expected_version": ready_version,
                "idempotency_key": "v076-evidence-release",
                "authorization": user_authorization(
                    "project.milestone.release",
                    "v076-evidence-release-event"
                )
            },
            "milestone_id": fixture.milestone_id,
            "readiness_snapshot_id": readiness_snapshot_id,
            "readiness_digest": readiness_digest
        }),
        &[StatusCode::OK],
    )
    .await;
    let release_id = required_string(&release, &["id"]);
    let pin_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_release_media_pin
         WHERE release_id = ? AND asset_id = ? AND attachment_id = ?",
    )
    .bind(&release_id)
    .bind(&task_asset_id)
    .bind(&evidence_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("release evidence pin count");
    assert_eq!(pin_count, 1);

    let (remove_status, remove_body) = raw_request(
        app,
        Method::DELETE,
        &format!(
            "/api/v1/projects/{}/milestones/{}/evidence/{evidence_id}",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        Body::from(
            serde_json::to_vec(&json!({
            "expected_version": evidence["version"],
            "idempotency_key": "v076-evidence-remove",
            "authorization": user_authorization(
                "project.evidence.remove",
                "v076-evidence-remove-event"
            )
            }))
            .expect("evidence removal serializes"),
        ),
        Some("application/json"),
    )
    .await;
    assert_eq!(remove_status, StatusCode::NO_CONTENT);
    assert!(remove_body.is_empty());
    let removed_evidence = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/milestones/{}/evidence/{evidence_id}",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        Value::Null,
        &[StatusCode::NOT_FOUND],
    )
    .await;
    assert_eq!(removed_evidence["code"], json!("not_found"));

    // Removing the evidence attachment does not erase the immutable release
    // pin. The old Task URL can then disappear independently of the retained
    // Project-authorized proof URL.
    let delete_task_media = common::raw_empty_request(
        app,
        Method::DELETE,
        &format!("/api/v1/media/{task_asset_id}"),
    )
    .await;
    assert_eq!(delete_task_media.status(), StatusCode::NO_CONTENT);
    let deleted_task_media = raw_request(
        app,
        Method::GET,
        &format!("/api/v1/media/{task_asset_id}"),
        &token,
        Body::empty(),
        None,
    )
    .await;
    assert_eq!(deleted_task_media.0, StatusCode::NOT_FOUND);
    let (retained_status, retained_body) = raw_request(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/media/{task_asset_id}",
            fixture.project_id
        ),
        &token,
        Body::empty(),
        None,
    )
    .await;
    assert_eq!(retained_status, StatusCode::OK);
    assert_eq!(retained_body, proof_bytes);
}

#[tokio::test]
async fn v076_ready_milestone_releases_once_and_rejects_cross_project_scope() {
    let workspace = common::TestDir::new("v076-readiness-release");
    let harness = common::test_app(workspace.path(), "v076-readiness-release").await;
    let app = &harness.app;
    let token = common::test_jwt();
    let fixture = create_genesis_project(app, &token, "v076-release").await;
    let baseline = create_active_baseline(app, &token, &fixture).await;
    record_passed_check(app, &harness, &token, &fixture, &baseline).await;
    let current_milestone = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/milestones/{}",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;

    let readiness = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/readiness",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        json!({
            "mutation": {
                "expected_version": current_milestone["version"],
                "idempotency_key": "v076-readiness",
                "authorization": user_authorization("project.milestone.readiness", "v076-readiness-event")
            },
            "milestone_id": fixture.milestone_id,
            "baseline_id": baseline.baseline_id,
            "baseline_revision_id": baseline.baseline_revision_id,
            "release_policy_revision": "v076-policy-r1"
        }),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(readiness["result"], json!("ready"));
    let snapshot_id = required_string(&readiness, &["id"]);
    let readiness_digest = required_string(&readiness, &["readiness_digest"]);
    let ready_milestone = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/milestones/{}",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(ready_milestone["lifecycle"], json!("ready_for_release"));
    let ready_version = ready_milestone["version"]
        .as_i64()
        .expect("ready milestone version");

    // The Project Agent may only submit a release candidate. This audited
    // attention/projection must not change the exact readiness inputs that a
    // user release re-authorizes in the transaction below.
    let candidate = services::ProjectMilestoneCommandService::new(harness.state.db.clone())
        .request_release(
            services::ProjectReleaseRequestCommand {
                project_id: fixture.project_id.clone(),
                milestone_id: fixture.milestone_id.clone(),
                expected_milestone_version: ready_version,
                readiness_snapshot_id: snapshot_id.clone(),
                readiness_digest: readiness_digest.clone(),
                status: "pending_user_release_approval".to_owned(),
                idempotency_key: "v076-release-candidate".to_owned(),
                authorization: services::ProjectCommandAuthorization {
                    principal_type: "agent".to_owned(),
                    principal_id: fixture.project_identity_id.clone(),
                    policy_result: "allowed".to_owned(),
                    policy_revision: Some("project-agent-policy@1".to_owned()),
                    policy_digest: Some("project-agent-policy-digest".to_owned()),
                    requested_permission: Some("propose_project".to_owned()),
                    correlation_id: "v076-release-candidate-correlation".to_owned(),
                    causation_id: None,
                    causation_depth: 0,
                    authorization_event_id: "v076-release-candidate-event".to_owned(),
                    authorization_basis: "bound Project Agent authorization".to_owned(),
                    authorization_action: "project.milestone.release.request".to_owned(),
                    authorization_occurred_at: Utc::now().to_rfc3339(),
                    authorization_json: json!({
                        "principal": {
                            "kind": "agent",
                            "id": fixture.project_identity_id
                        },
                        "action": "project.milestone.release.request"
                    })
                    .to_string(),
                },
            },
            None,
        )
        .await
        .expect("Project Agent release candidate is admitted");
    assert_eq!(candidate.status, "pending_user_release_approval");
    assert!(!candidate.event_id.is_empty());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'project_release.candidate_requested'",
        )
        .bind(&fixture.project_id)
        .fetch_one(harness.state.db.pool())
        .await
        .expect("candidate request event count"),
        1
    );

    let release_body = json!({
        "mutation": {
            "expected_version": ready_version,
            "idempotency_key": "v076-release",
            "authorization": user_authorization("project.milestone.release", "v076-release-event")
        },
        "milestone_id": fixture.milestone_id,
        "readiness_snapshot_id": snapshot_id,
        "readiness_digest": readiness_digest
    });
    let release = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/release",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        release_body.clone(),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(release["release_identity"], json!("M001-r1"));
    let replay = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/release",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        release_body.clone(),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(replay, release);

    // A stale dialog can submit the same immutable readiness target with a
    // fresh UI key after the first release committed. Return the existing
    // release instead of recomputing against the released lifecycle.
    let mut fresh_key_release = release_body.clone();
    fresh_key_release["mutation"]["idempotency_key"] = json!("v076-release-fresh-ui-key");
    fresh_key_release["mutation"]["authorization"] =
        user_authorization("project.milestone.release", "v076-release-fresh-ui-event");
    let semantic_replay = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/release",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        fresh_key_release,
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(semantic_replay, release);
    let primary_milestone_id: Option<String> =
        sqlx::query_scalar("SELECT primary_milestone_id FROM project WHERE id = ?")
            .bind(&fixture.project_id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("released Project primary milestone");
    assert_eq!(
        primary_milestone_id.as_deref(),
        Some(fixture.milestone_id.as_str())
    );
    let overview = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{}/overview", fixture.project_id),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    assert!(overview["active_milestones"]
        .as_array()
        .is_some_and(|milestones| milestones.iter().any(|entry| {
            entry["milestone"]["id"] == fixture.milestone_id
                && entry["milestone"]["lifecycle"] == "released"
        })));

    // The release idempotency key is bound to the complete candidate. A
    // replay after the milestone has moved to `released` is exact, while the
    // same key with a changed readiness digest is a conflict.
    let mut altered_release = release_body;
    altered_release["readiness_digest"] = json!("v076-altered-readiness-digest");
    let altered = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/release",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        altered_release,
        &[StatusCode::CONFLICT],
    )
    .await;
    assert!(altered.get("message").is_some());

    // Inspect the public immutable release projection as well as the
    // canonical row. The row must retain the exact readiness and closed
    // release-policy references used by the release transaction.
    let release_id = required_string(&release, &["id"]);
    let inspected = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/releases/{release_id}",
            fixture.project_id
        ),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(inspected, release);
    assert_eq!(
        inspected["snapshot"]["readiness_snapshot_id"],
        json!(snapshot_id)
    );
    assert_eq!(
        inspected["snapshot"]["readiness_digest"],
        json!(readiness_digest)
    );
    assert_eq!(
        inspected["snapshot"]["release_policy_revision"],
        json!("v076-policy-r1")
    );
    assert!(!required_string(&inspected["snapshot"], &["snapshot_digest"]).is_empty());

    let release_row = sqlx::query(
        "SELECT project_id, milestone_id, release_sequence, release_revision,
                release_identifier, readiness_snapshot_id, readiness_digest,
                baseline_id, baseline_revision_id, baseline_digest,
                release_policy_revision, release_policy_digest,
                releasing_principal_type, releasing_principal_id,
                authorization_basis, explicit_event, schema_version,
                snapshot_digest, idempotency_key
         FROM project_release WHERE id = ?",
    )
    .bind(&release_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("immutable release row");
    assert_eq!(
        release_row.get::<String, _>("project_id"),
        fixture.project_id
    );
    assert_eq!(
        release_row.get::<String, _>("milestone_id"),
        fixture.milestone_id
    );
    assert_eq!(release_row.get::<i64, _>("release_sequence"), 1);
    assert_eq!(release_row.get::<i64, _>("release_revision"), 1);
    assert_eq!(
        release_row.get::<String, _>("release_identifier"),
        "M001-r1"
    );
    assert_eq!(
        release_row.get::<String, _>("readiness_snapshot_id"),
        snapshot_id
    );
    assert_eq!(
        release_row.get::<String, _>("readiness_digest"),
        readiness_digest
    );
    assert_eq!(
        release_row.get::<String, _>("baseline_id"),
        baseline.baseline_id
    );
    assert_eq!(
        release_row.get::<String, _>("baseline_revision_id"),
        baseline.baseline_revision_id
    );
    assert_eq!(
        release_row.get::<String, _>("baseline_digest"),
        baseline.content_digest
    );
    assert_eq!(
        release_row.get::<String, _>("release_policy_revision"),
        "v076-policy-r1"
    );
    assert_eq!(
        release_row.get::<String, _>("release_policy_digest"),
        readiness["release_policy_digest"]
            .as_str()
            .expect("readiness policy digest")
    );
    assert_eq!(
        release_row.get::<String, _>("releasing_principal_type"),
        "user"
    );
    assert_eq!(
        release_row.get::<String, _>("releasing_principal_id"),
        "test-user-id"
    );
    assert_eq!(
        release_row.get::<String, _>("authorization_basis"),
        "explicit_user_authorization"
    );
    assert_eq!(
        release_row.get::<String, _>("explicit_event"),
        "v076-release-event"
    );
    assert_eq!(
        release_row.get::<String, _>("idempotency_key"),
        "v076-release"
    );
    assert!(!release_row.get::<String, _>("schema_version").is_empty());
    assert_eq!(
        release_row.get::<String, _>("snapshot_digest"),
        inspected["snapshot"]["snapshot_digest"]
            .as_str()
            .expect("release snapshot digest")
    );
    let release_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_release WHERE project_id = ? AND milestone_id = ?",
    )
    .bind(&fixture.project_id)
    .bind(&fixture.milestone_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("one release row");
    assert_eq!(release_count, 1);

    // A different Project cannot use this milestone identity, even though
    // the same authenticated user owns both Projects.
    let other_project = request_json(
        app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({"name": "V076 unrelated project"}),
        &[StatusCode::CREATED, StatusCode::OK],
    )
    .await;
    let other_project_id = required_string(&other_project, &["id"]);
    let cross_scope = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/release",
            other_project_id, fixture.milestone_id
        ),
        &token,
        json!({
            "mutation": {
                "expected_version": ready_version,
                "idempotency_key": "v076-cross-project-release",
                "authorization": user_authorization("project.milestone.release", "v076-cross-project-release-event")
            },
            "milestone_id": fixture.milestone_id,
            "readiness_snapshot_id": snapshot_id,
            "readiness_digest": readiness_digest
        }),
        &[StatusCode::NOT_FOUND, StatusCode::CONFLICT],
    )
    .await;
    assert!(cross_scope.get("message").is_some());
}

#[tokio::test]
async fn v076_relevant_post_readiness_mutation_conflicts_with_release() {
    let workspace = common::TestDir::new("v076-readiness-mutation-conflict");
    let harness = common::test_app(workspace.path(), "v076-readiness-mutation-conflict").await;
    let app = &harness.app;
    let token = common::test_jwt();
    let fixture = create_genesis_project(app, &token, "v076-readiness-mutation").await;
    let baseline = create_active_baseline(app, &token, &fixture).await;
    record_passed_check(app, &harness, &token, &fixture, &baseline).await;

    let current_milestone = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/milestones/{}",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let readiness = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/readiness",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        json!({
            "mutation": {
                "expected_version": current_milestone["version"],
                "idempotency_key": "v076-readiness-mutation-readiness",
                "authorization": user_authorization(
                    "project.milestone.readiness",
                    "v076-readiness-mutation-readiness-event"
                )
            },
            "milestone_id": fixture.milestone_id,
            "baseline_id": baseline.baseline_id,
            "baseline_revision_id": baseline.baseline_revision_id,
            "release_policy_revision": "v076-policy-r1"
        }),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(readiness["result"], json!("ready"));
    let snapshot_id = required_string(&readiness, &["id"]);
    let readiness_digest = required_string(&readiness, &["readiness_digest"]);
    let ready_milestone = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/milestones/{}",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let ready_version = ready_milestone["version"]
        .as_i64()
        .expect("ready milestone version");
    // A newer validation result is a governed readiness input. Unlike a
    // release-candidate attention event, it must invalidate the immutable
    // readiness candidate and force a fresh evaluation before release.
    let check = sqlx::query(
        "SELECT id, definition_revision_id, version FROM project_milestone_check
         WHERE project_id = ? AND milestone_id = ? ORDER BY id LIMIT 1",
    )
    .bind(&fixture.project_id)
    .bind(&fixture.milestone_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("current acceptance check");
    let check_id: String = check.get("id");
    let definition_revision_id: String = check.get("definition_revision_id");
    let check_version: i64 = check.get("version");
    let replacement_result = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/checks/{check_id}/result",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        json!({
            "mutation": {
                "expected_version": check_version,
                "idempotency_key": "v076-readiness-mutation-check",
                "authorization": user_authorization(
                    "project.milestone.check.record",
                    "v076-readiness-mutation-check-event"
                )
            },
            "check_id": check_id,
            "definition_revision_id": definition_revision_id,
            "status": "pass",
            "result": "passed after readiness with new authoritative input",
            "input_digest": "v076-check-input-after-readiness",
            "governing_revision_ids": [
                fixture.charter_revision_id,
                baseline.baseline_revision_id
            ]
        }),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(replacement_result["status"], json!("pass"));

    let conflict = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/release",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        json!({
            "mutation": {
                "expected_version": ready_version,
                "idempotency_key": "v076-readiness-mutation-release",
                "authorization": user_authorization(
                    "project.milestone.release",
                    "v076-readiness-mutation-release-event"
                )
            },
            "milestone_id": fixture.milestone_id,
            "readiness_snapshot_id": snapshot_id,
            "readiness_digest": readiness_digest
        }),
        &[StatusCode::CONFLICT],
    )
    .await;
    assert!(conflict.get("message").is_some());
}

#[tokio::test]
async fn v076_user_authority_receipts_are_exact_replays_across_gates() {
    let workspace = common::TestDir::new("v076-authority-replay-matrix");
    let harness = common::test_app(workspace.path(), "v076-authority-replay-matrix").await;
    let app = &harness.app;
    let token = common::test_jwt();
    let fixture = create_genesis_project(app, &token, "v076-authority").await;
    let baseline = create_baseline(app, &token, &fixture, false).await;

    // Baseline approval binds both the user receipt and the exact revision
    // digests. It must remain replay-safe even after the baseline version has
    // advanced from the approval itself.
    let approval_body = json!({
        "mutation": {
            "expected_version": baseline.approval_expected_baseline_version,
            "idempotency_key": "v076-baseline-approve",
            "authorization": baseline.approval_authorization.clone()
        },
        "revision_id": baseline.baseline_revision_id,
        "content_digest": baseline.content_digest,
        "render_digest": baseline.render_digest,
        "expected_project_version": baseline.approval_expected_project_version
    });
    let approval_replay = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/execution-baseline/{}/revisions/{}/approve",
            fixture.project_id, baseline.baseline_id, baseline.baseline_revision_id
        ),
        &token,
        approval_body.clone(),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(approval_replay["baseline"]["lifecycle"], json!("approved"));
    for (label, altered) in user_authorization_replay_variants(&approval_body, false) {
        let conflict = request_json(
            app,
            Method::POST,
            &format!(
                "/api/v1/projects/{}/execution-baseline/{}/revisions/{}/approve",
                fixture.project_id, baseline.baseline_id, baseline.baseline_revision_id
            ),
            &token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }
    for (label, altered) in [
        ("baseline approval revision", {
            let mut value = approval_body.clone();
            value["revision_id"] = json!("v076-different-baseline-revision");
            value
        }),
        ("baseline approval content digest", {
            let mut value = approval_body.clone();
            value["content_digest"] = json!("v076-different-baseline-digest");
            value
        }),
    ] {
        let conflict = request_json(
            app,
            Method::POST,
            &format!(
                "/api/v1/projects/{}/execution-baseline/{}/revisions/{}/approve",
                fixture.project_id, baseline.baseline_id, baseline.baseline_revision_id
            ),
            &token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }

    // The URL target is part of the replay identity too. A stale or
    // substituted revision in the path must not be re-authorized (or leak a
    // not-found) after this key has already committed.
    let approval_target_conflict = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/execution-baseline/{}/revisions/v076-different-path-revision/approve",
            fixture.project_id, baseline.baseline_id
        ),
        &token,
        approval_body.clone(),
        &[StatusCode::CONFLICT],
    )
    .await;
    assert_eq!(
        approval_target_conflict["code"],
        json!("idempotency_conflict")
    );

    // Activation is a second user-only receipt. Its exact approval id,
    // revision and rendered/content digests are part of the replay identity.
    let project = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{}", fixture.project_id),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let activation_body = json!({
        "mutation": {
            "expected_version": project["version"],
            "idempotency_key": "v076-baseline-activate",
            "authorization": user_authorization(
                "project.execution_baseline.activate",
                "v076-baseline-activate-event"
            )
        },
        "baseline_id": baseline.baseline_id,
        "revision_id": baseline.baseline_revision_id,
        "approval_id": baseline.approval_id,
        "expected_baseline_version": baseline.baseline_version,
        "content_digest": baseline.content_digest,
        "render_digest": baseline.render_digest
    });
    let activation = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/execution-baseline/{}/activate",
            fixture.project_id, baseline.baseline_id
        ),
        &token,
        activation_body.clone(),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(activation["baseline"]["lifecycle"], json!("active"));
    let activation_replay = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/execution-baseline/{}/activate",
            fixture.project_id, baseline.baseline_id
        ),
        &token,
        activation_body.clone(),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(activation_replay, activation);
    for (label, altered) in user_authorization_replay_variants(&activation_body, false) {
        let conflict = request_json(
            app,
            Method::POST,
            &format!(
                "/api/v1/projects/{}/execution-baseline/{}/activate",
                fixture.project_id, baseline.baseline_id
            ),
            &token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }
    for (label, altered) in [
        ("baseline activation revision", {
            let mut value = activation_body.clone();
            value["revision_id"] = json!("v076-different-activation-revision");
            value
        }),
        ("baseline activation approval", {
            let mut value = activation_body.clone();
            value["approval_id"] = json!("v076-different-activation-approval");
            value
        }),
        ("baseline activation content digest", {
            let mut value = activation_body.clone();
            value["content_digest"] = json!("v076-different-activation-digest");
            value
        }),
    ] {
        let conflict = request_json(
            app,
            Method::POST,
            &format!(
                "/api/v1/projects/{}/execution-baseline/{}/activate",
                fixture.project_id, baseline.baseline_id
            ),
            &token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }
    let activation_target_conflict = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/execution-baseline/v076-different-path-baseline/activate",
            fixture.project_id
        ),
        &token,
        activation_body.clone(),
        &[StatusCode::CONFLICT],
    )
    .await;
    assert_eq!(
        activation_target_conflict["code"],
        json!("idempotency_conflict")
    );

    record_passed_check(app, &harness, &token, &fixture, &baseline).await;
    let check = sqlx::query(
        "SELECT id, definition_revision_id, version
         FROM project_milestone_check
         WHERE project_id = ? AND milestone_id = ? ORDER BY id LIMIT 1",
    )
    .bind(&fixture.project_id)
    .bind(&fixture.milestone_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("manual-check waiver target");
    let check_id: String = check.get("id");
    let definition_revision_id: String = check.get("definition_revision_id");
    let check_version: i64 = check.get("version");
    let waiver_body = json!({
        "mutation": {
            "expected_version": check_version,
            "idempotency_key": "v076-check-waiver",
            "authorization": user_authorization(
                "project.milestone.check.waive",
                "v076-check-waiver-event"
            )
        },
        "check_id": check_id,
        "definition_revision_id": definition_revision_id,
        "reason": "An independent user records the bounded waiver decision.",
        "input_digest": "v076-waiver-input",
        "governing_revision_ids": [fixture.charter_revision_id, baseline.baseline_revision_id]
    });
    let waiver = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/checks/{}/waive",
            fixture.project_id, fixture.milestone_id, check_id
        ),
        &token,
        waiver_body.clone(),
        &[StatusCode::OK],
    )
    .await;
    let waiver_replay = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/checks/{}/waive",
            fixture.project_id, fixture.milestone_id, check_id
        ),
        &token,
        waiver_body.clone(),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(waiver_replay, waiver);
    for (label, altered) in user_authorization_replay_variants(&waiver_body, false) {
        let conflict = request_json(
            app,
            Method::POST,
            &format!(
                "/api/v1/projects/{}/milestones/{}/checks/{}/waive",
                fixture.project_id, fixture.milestone_id, check_id
            ),
            &token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }
    for (label, altered) in [
        ("waiver definition revision", {
            let mut value = waiver_body.clone();
            value["definition_revision_id"] = json!("v076-different-check-revision");
            value
        }),
        ("waiver input digest", {
            let mut value = waiver_body.clone();
            value["input_digest"] = json!("v076-different-waiver-input");
            value
        }),
        ("waiver expected version", {
            let mut value = waiver_body.clone();
            value["mutation"]["expected_version"] = json!(check_version + 1);
            value
        }),
        ("waiver governing Charter", {
            let mut value = waiver_body.clone();
            value["governing_revision_ids"][0] = json!("v076-different-charter-revision");
            value
        }),
        ("waiver governing baseline", {
            let mut value = waiver_body.clone();
            value["governing_revision_ids"][1] = json!("v076-different-baseline-revision");
            value
        }),
        ("waiver request target", {
            let mut value = waiver_body.clone();
            value["check_id"] = json!("v076-different-check-target");
            value
        }),
    ] {
        let conflict = request_json(
            app,
            Method::POST,
            &format!(
                "/api/v1/projects/{}/milestones/{}/checks/{}/waive",
                fixture.project_id, fixture.milestone_id, check_id
            ),
            &token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }
    let waiver_path_target_conflict = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/checks/v076-different-path-check/waive",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        waiver_body.clone(),
        &[StatusCode::CONFLICT],
    )
    .await;
    assert_eq!(
        waiver_path_target_conflict["code"],
        json!("idempotency_conflict")
    );

    // Readiness and release each carry a complete user receipt plus immutable
    // baseline/snapshot target pins. Replays must compare both dimensions.
    let current_milestone = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/milestones/{}",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let readiness_body = json!({
        "mutation": {
            "expected_version": current_milestone["version"],
            "idempotency_key": "v076-authority-readiness",
            "authorization": user_authorization(
                "project.milestone.readiness",
                "v076-authority-readiness-event"
            )
        },
        "milestone_id": fixture.milestone_id,
        "baseline_id": baseline.baseline_id,
        "baseline_revision_id": baseline.baseline_revision_id,
        "release_policy_revision": "v076-policy-r1"
    });
    let readiness = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/readiness",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        readiness_body.clone(),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(readiness["result"], json!("ready"));
    let readiness_replay = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/readiness",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        readiness_body.clone(),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(readiness_replay, readiness);
    for (label, altered) in user_authorization_replay_variants(&readiness_body, false) {
        let conflict = request_json(
            app,
            Method::POST,
            &format!(
                "/api/v1/projects/{}/milestones/{}/readiness",
                fixture.project_id, fixture.milestone_id
            ),
            &token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }
    for (label, altered) in [
        ("readiness baseline revision", {
            let mut value = readiness_body.clone();
            value["baseline_revision_id"] = json!("v076-different-readiness-revision");
            value
        }),
        ("readiness policy revision", {
            let mut value = readiness_body.clone();
            value["release_policy_revision"] = json!("v076-different-policy-revision");
            value
        }),
    ] {
        let conflict = request_json(
            app,
            Method::POST,
            &format!(
                "/api/v1/projects/{}/milestones/{}/readiness",
                fixture.project_id, fixture.milestone_id
            ),
            &token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }

    let ready_milestone = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/milestones/{}",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let release_body = json!({
        "mutation": {
            "expected_version": ready_milestone["version"],
            "idempotency_key": "v076-authority-release",
            "authorization": user_authorization(
                "project.milestone.release",
                "v076-authority-release-event"
            )
        },
        "milestone_id": fixture.milestone_id,
        "readiness_snapshot_id": readiness["id"],
        "readiness_digest": readiness["readiness_digest"]
    });
    let release = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/release",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        release_body.clone(),
        &[StatusCode::OK],
    )
    .await;
    let release_replay = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/release",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        release_body.clone(),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(release_replay, release);
    for (label, altered) in user_authorization_replay_variants(&release_body, false) {
        let conflict = request_json(
            app,
            Method::POST,
            &format!(
                "/api/v1/projects/{}/milestones/{}/release",
                fixture.project_id, fixture.milestone_id
            ),
            &token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }
    for (label, altered) in [
        ("release readiness snapshot", {
            let mut value = release_body.clone();
            value["readiness_snapshot_id"] = json!("v076-different-readiness-snapshot");
            value
        }),
        ("release readiness digest", {
            let mut value = release_body.clone();
            value["readiness_digest"] = json!("v076-different-readiness-digest");
            value
        }),
    ] {
        let conflict = request_json(
            app,
            Method::POST,
            &format!(
                "/api/v1/projects/{}/milestones/{}/release",
                fixture.project_id, fixture.milestone_id
            ),
            &token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }
}

async fn record_passed_check(
    app: &Router,
    harness: &common::Harness,
    token: &str,
    fixture: &GenesisFixture,
    baseline: &BaselineFixture,
) {
    let db = harness.state.db.clone();
    let project_id = fixture.project_id.clone();
    let milestone_id = fixture.milestone_id.clone();
    let baseline_revision_id = baseline.baseline_revision_id.clone();
    let check = sqlx::query(
        "SELECT id, definition_revision_id, version FROM project_milestone_check
         WHERE project_id = ? AND milestone_id = ? ORDER BY id LIMIT 1",
    )
    .bind(&project_id)
    .bind(&milestone_id)
    .fetch_one(db.pool())
    .await
    .expect("default acceptance check");
    let check_id: String = check.get("id");
    let definition_revision_id: String = check.get("definition_revision_id");
    let check_version: i64 = check.get("version");
    let result_body = json!({
        "mutation": {
            "expected_version": check_version,
            "idempotency_key": "v076-check-result",
            "authorization": user_authorization(
                "project.milestone.check.record",
                "v076-check-event"
            )
        },
        "check_id": check_id,
        "definition_revision_id": definition_revision_id,
        "status": "pass",
        "result": "passed",
        "input_digest": "v076-check-input",
        "governing_revision_ids": [fixture.charter_revision_id, baseline_revision_id]
    });
    let result = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{project_id}/milestones/{milestone_id}/checks/{check_id}/result"
        ),
        token,
        result_body.clone(),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(result["status"], json!("pass"));
    assert_eq!(result["principal"]["kind"], json!("user"));
    ensure_required_evidence(app, token, fixture).await;

    for (label, altered) in user_authorization_replay_variants(&result_body, false) {
        let conflict = request_json(
            app,
            Method::POST,
            &format!(
                "/api/v1/projects/{project_id}/milestones/{milestone_id}/checks/{check_id}/result"
            ),
            token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }
    for (label, altered) in [
        ("manual check definition revision", {
            let mut value = result_body.clone();
            value["definition_revision_id"] = json!("v076-different-check-revision");
            value
        }),
        ("manual check input digest", {
            let mut value = result_body.clone();
            value["input_digest"] = json!("v076-different-check-input");
            value
        }),
        ("manual check expected version", {
            let mut value = result_body.clone();
            value["mutation"]["expected_version"] = json!(check_version + 1);
            value
        }),
        ("manual check governing Charter", {
            let mut value = result_body.clone();
            value["governing_revision_ids"][0] = json!("v076-different-charter-revision");
            value
        }),
        ("manual check governing baseline", {
            let mut value = result_body.clone();
            value["governing_revision_ids"][1] = json!("v076-different-baseline-revision");
            value
        }),
        ("manual check request target", {
            let mut value = result_body.clone();
            value["check_id"] = json!("v076-different-check-target");
            value
        }),
    ] {
        let conflict = request_json(
            app,
            Method::POST,
            &format!(
                "/api/v1/projects/{project_id}/milestones/{milestone_id}/checks/{check_id}/result"
            ),
            token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }
    let result_path_target_conflict = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{project_id}/milestones/{milestone_id}/checks/v076-different-path-check/result"
        ),
        token,
        result_body.clone(),
        &[StatusCode::CONFLICT],
    )
    .await;
    assert_eq!(
        result_path_target_conflict["code"],
        json!("idempotency_conflict")
    );

    // Manual check attestation is explicitly user-only: a Project Agent's
    // principal-bound authorization is rejected before any check state is
    // touched.
    let agent_denial = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{project_id}/milestones/{milestone_id}/checks/{check_id}/result"
        ),
        token,
        json!({
            "mutation": {
                "expected_version": check_version + 1,
                "idempotency_key": "v076-agent-check-result",
                "authorization": {
                    "principal": {"kind": "agent", "id": fixture.project_identity_id},
                    "authorization_basis": "project_agent",
                    "action": "project.milestone.check.record",
                    "event_id": "v076-agent-check-event",
                    "occurred_at": Utc::now().to_rfc3339()
                }
            },
            "check_id": check_id,
            "definition_revision_id": definition_revision_id,
            "status": "pass",
            "result": "agent must not attest",
            "input_digest": "v076-agent-check-input",
            "governing_revision_ids": [fixture.charter_revision_id, baseline_revision_id]
        }),
        &[StatusCode::FORBIDDEN],
    )
    .await;
    assert_eq!(agent_denial["code"], json!("authorization.invalid"));

    // The same user key cannot be reused for a different result, even after
    // the check version has advanced from the original attestation.
    let altered = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{project_id}/milestones/{milestone_id}/checks/{check_id}/result"
        ),
        token,
        json!({
            "mutation": {
                "expected_version": check_version + 1,
                "idempotency_key": "v076-check-result",
                "authorization": user_authorization(
                    "project.milestone.check.record",
                    "v076-check-event"
                )
            },
            "check_id": check_id,
            "definition_revision_id": definition_revision_id,
            "status": "pass",
            "result": "altered result",
            "input_digest": "v076-altered-check-input",
            "governing_revision_ids": [fixture.charter_revision_id, baseline_revision_id]
        }),
        &[StatusCode::CONFLICT],
    )
    .await;
    assert_eq!(altered["code"], json!("idempotency_conflict"));
}

async fn ensure_required_evidence(app: &Router, token: &str, fixture: &GenesisFixture) {
    let evidence = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/milestones/{}/evidence",
            fixture.project_id, fixture.milestone_id
        ),
        token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    if evidence["items"].as_array().is_some_and(|items| {
        items.iter().any(|item| {
            item["availability"] == json!("available")
                && item["acceptance_check_ids"].as_array().is_some_and(|ids| {
                    ids.iter()
                        .any(|id| id == &json!(fixture.milestone_acceptance_check_id))
                })
        })
    }) {
        return;
    }

    let project = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{}", fixture.project_id),
        token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let project_version = project["version"]
        .as_i64()
        .expect("Project version for required evidence");
    let mut proof = b"\x89PNG\r\n\x1a\nrequired evidence ".to_vec();
    proof.extend_from_slice(fixture.milestone_acceptance_check_id.as_bytes());
    let asset = upload_project_media(
        app,
        token,
        &fixture.project_id,
        project_version,
        "v076-required-evidence-upload",
        &user_authorization(
            "project.media.upload",
            "v076-required-evidence-upload-event",
        ),
        &proof,
        StatusCode::CREATED,
    )
    .await;
    let asset_id = required_string(&asset, &["id"]);
    let checksum = required_string(&asset, &["checksum"]);
    let milestone = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/milestones/{}",
            fixture.project_id, fixture.milestone_id
        ),
        token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/evidence",
            fixture.project_id, fixture.milestone_id
        ),
        token,
        json!({
            "mutation": {
                "expected_version": milestone["version"],
                "idempotency_key": "v076-required-evidence-attach",
                "authorization": user_authorization(
                    "project.evidence.attach",
                    "v076-required-evidence-attach-event"
                )
            },
            "milestone_id": fixture.milestone_id,
            "asset_id": asset_id,
            "acceptance_check_ids": [fixture.milestone_acceptance_check_id],
            "caption": "Required acceptance evidence fixture.",
            "kind": "screenshot",
            "checksum": checksum
        }),
        &[StatusCode::OK],
    )
    .await;
}

async fn create_genesis_project(app: &Router, token: &str, prefix: &str) -> GenesisFixture {
    let main = connect_agent(
        app,
        token,
        &format!("{prefix}-main"),
        &["read_account", "read_project", "handoff"],
    )
    .await;
    let project_agent = connect_agent(
        app,
        token,
        &format!("{prefix}-project"),
        &[
            "read_project",
            "handoff",
            "propose_project",
            "propose_task",
            "propose_decision",
            "read_task",
        ],
    )
    .await;
    let main_identity = required_string(&main, &["agent", "id"]);
    let main_profile = required_string(&main, &["profile", "id"]);
    let project_identity = required_string(&project_agent, &["agent", "id"]);
    let project_profile = required_string(&project_agent, &["profile", "id"]);
    let binding = request_json(
        app,
        Method::PUT,
        "/api/v1/account/main-agent",
        token,
        json!({
            "identity_id": main_identity,
            "profile_id": main_profile,
            "expected_version": 0,
            "autonomy_policy": {}
        }),
        &[StatusCode::OK, StatusCode::CREATED],
    )
    .await;
    let main_chat_id = required_string(&binding, &["chat_id"]);
    let started = request_json(
        app,
        Method::POST,
        "/api/v1/account/main-agent/product-genesis",
        token,
        json!({
            "idempotency_key": format!("{prefix}-genesis-start"),
            "maturity": "mvp",
            "initial_idea": "A V076 bounded project",
            "preferred_project_agent_identity_id": project_identity
        }),
        &[StatusCode::CREATED],
    )
    .await;
    let session_id = required_string(&started, &["session", "id"]);
    let content = charter_content(
        &format!("{prefix} Project"),
        "the approved V076 outcome is observable",
    );
    let rendered = services::render_and_digest_charter(&content);
    let charter_id = format!("{prefix}-charter");
    let saved = request_json(
        app,
        Method::POST,
        &format!("/api/v1/account/main-agent/product-genesis/{session_id}/charter/revisions"),
        token,
        json!({
            "mutation": {
                "expected_version": 1,
                "idempotency_key": format!("{prefix}-charter-save"),
                "authorization": user_authorization("project_charter.revision.save", format!("{prefix}-save"))
            },
            "charter_id": charter_id,
            "project_mode": "compact",
            "maturity": "mvp",
            "content": content.clone(),
            "rendered_view": rendered.rendered_view.clone(),
            "render_version": rendered.render_version,
            "provenance": user_provenance("V076 approved Charter")
        }),
        &[StatusCode::CREATED],
    )
    .await;
    let revision_id = required_string(&saved, &["id"]);
    let projection = request_json(
        app,
        Method::GET,
        &format!("/api/v1/account/main-agent/product-genesis/{session_id}/charter"),
        token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let charter_version = projection["charter"]["version"]
        .as_i64()
        .expect("charter version");
    let policy_digest = project_policy_digest(&project_agent["profile"]["tool_policy"]);
    let approval_body = json!({
        "mutation": {
            "expected_version": charter_version,
            "expected_digest": rendered.content_digest,
            "idempotency_key": format!("{prefix}-charter-approve"),
            "authorization": user_authorization("product_genesis.charter_approval", format!("{prefix}-approve"))
        },
        "charter_id": charter_id,
        "revision_id": revision_id,
        "content_digest": rendered.content_digest,
        "render_digest": rendered.render_digest,
        "expected_charter_version": charter_version,
        "approved_project_name": format!("{prefix} Project"),
        "approved_project_slug": format!("{prefix}-project"),
        "project_mode": "compact",
        "selected_project_agent_identity_id": project_identity,
        "selected_project_agent_profile_revision_id": project_profile,
        "selected_project_agent_operating_skill_revision": "forge.project.orchestration/v1@4",
        "selected_project_agent_policy_digest": policy_digest
    });
    let approval = request_json(
        app,
        Method::POST,
        &format!("/api/v1/account/main-agent/product-genesis/{session_id}/charter/revisions/{revision_id}/approve"),
        token,
        approval_body.clone(),
        &[StatusCode::CREATED],
    )
    .await;
    // A Charter approval receipt is immutable. Reusing the same key with any
    // changed authority field or exact review target must be a conflict,
    // rather than a second approval or a successful replay.
    for (label, altered) in user_authorization_replay_variants(&approval_body, false) {
        let conflict = request_json(
            app,
            Method::POST,
            &format!("/api/v1/account/main-agent/product-genesis/{session_id}/charter/revisions/{revision_id}/approve"),
            token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }
    for (label, altered) in [
        ("charter approval revision", {
            let mut value = approval_body.clone();
            value["revision_id"] = json!("v076-different-charter-revision");
            value
        }),
        ("charter approval content digest", {
            let mut value = approval_body.clone();
            value["content_digest"] = json!("v076-different-charter-digest");
            value
        }),
    ] {
        let conflict = request_json(
            app,
            Method::POST,
            &format!("/api/v1/account/main-agent/product-genesis/{session_id}/charter/revisions/{revision_id}/approve"),
            token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }
    let approval_id = required_string(&approval, &["id"]);
    let create_body = json!({
        "approval_id": approval_id,
        "idempotency_key": format!("{prefix}-project-create"),
        "authorization": user_authorization("product_genesis.create_project_from_approval", format!("{prefix}-create"))
    });
    let created = request_json(
        app,
        Method::POST,
        "/api/v1/projects",
        token,
        create_body.clone(),
        &[StatusCode::CREATED],
    )
    .await;

    // Project creation consumes the exact approval receipt atomically. The
    // create key is independently replay-bound to its complete user receipt.
    // Once that key is committed, changing the approval target is an altered
    // replay and must return the shared idempotency conflict without looking
    // up or exposing the alternate target.
    for (label, altered) in user_authorization_replay_variants(&create_body, true) {
        let conflict = request_json(
            app,
            Method::POST,
            "/api/v1/projects",
            token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }
    let mut altered_create_target = create_body.clone();
    altered_create_target["approval_id"] = json!("v076-different-approval-receipt");
    let altered_target = request_json(
        app,
        Method::POST,
        "/api/v1/projects",
        token,
        altered_create_target,
        &[StatusCode::CONFLICT],
    )
    .await;
    assert_eq!(altered_target["code"], json!("idempotency_conflict"));
    let project_id = required_string(&created, &["project_id"]);
    let project_chat_id = required_string(&created, &["project_chat_id"]);
    let project = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}"),
        token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let project_version = project["version"].as_i64().expect("project version");
    let milestone_id = required_string(&project, &["primary_milestone_id"]);
    let milestone = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}/milestones/{milestone_id}"),
        token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let milestone_version = milestone["version"].as_i64().expect("milestone version");
    let milestone_definition_revision_id = required_string(&milestone, &["definition_revision_id"]);
    let milestone_definition = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{project_id}/milestones/{milestone_id}/revisions/{milestone_definition_revision_id}"
        ),
        token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let milestone_acceptance_check_id = required_string(
        &milestone_definition["content"]["acceptance_checks"][0],
        &["id"],
    );
    let milestone_acceptance_check_description = required_string(
        &milestone_definition["content"]["acceptance_checks"][0],
        &["description"],
    );
    GenesisFixture {
        project_id,
        project_version,
        project_identity_id: project_identity,
        project_profile_id: project_profile,
        project_chat_id,
        main_chat_id,
        genesis_session_id: session_id,
        create_response: created,
        create_request: create_body,
        approval_id,
        create_idempotency_key: format!("{prefix}-project-create"),
        charter_id,
        charter_revision_id: revision_id,
        charter_version,
        charter_content_digest: rendered.content_digest,
        charter_render_digest: rendered.render_digest,
        milestone_id,
        milestone_version,
        milestone_definition_revision_id,
        milestone_acceptance_check_id,
        milestone_acceptance_check_description,
    }
}

async fn create_active_baseline(
    app: &Router,
    token: &str,
    fixture: &GenesisFixture,
) -> BaselineFixture {
    create_baseline(app, token, fixture, true).await
}

async fn create_baseline(
    app: &Router,
    token: &str,
    fixture: &GenesisFixture,
    activate: bool,
) -> BaselineFixture {
    let project = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{}", fixture.project_id),
        token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let project_version = project["version"]
        .as_i64()
        .expect("baseline project version");
    let release_policy: api_types::ExecutionBaselineReleasePolicy = serde_json::from_value(json!({
        "schema_version": services::EXECUTION_BASELINE_RELEASE_POLICY_SCHEMA,
        "revision": "v076-policy-r1",
        "required_check_definition_revisions": [fixture.milestone_definition_revision_id],
        "reviewer_independence_rules": ["independent-reviewer"],
        "manual_attestation_rules": ["manual-attestation"],
        "waiver_rules": ["user-waiver"],
        "evidence_kinds": ["ci-log", "media"],
        "evidence_contexts": ["milestone"],
        "evidence_freshness_rules": ["current-milestone"],
        "dependency_rules": ["dependencies-green"],
        "stale_input_rules": ["stale-baseline-blocks"],
        "forbidden_side_effects": ["cross-project-write"],
        "known_issue_rules": ["known-issue-blocks"],
        "correction_rules": ["correction-required"],
        "purge_rules": ["purge-stale-evidence"]
    }))
    .expect("closed release policy parses");
    let release_policy_digest = api_types::canonical_digest_with_schema(
        services::EXECUTION_BASELINE_RELEASE_POLICY_SCHEMA,
        &release_policy,
    )
    .expect("closed release policy digest");
    let content = json!({
        "charter_revision": {
            "artifact_id": fixture.charter_id,
            "revision_id": fixture.charter_revision_id,
            "content_digest": fixture.charter_content_digest,
            "render_version": "forge.project-charter/v1",
            "render_digest": fixture.charter_render_digest
        },
        "document_revisions": [],
        "plan_item_ids": ["v076-plan-item-1", "v076-plan-item-2"],
        "milestone_ids": [fixture.milestone_id],
        "milestone_definition_revision_ids": [fixture.milestone_definition_revision_id],
        "primary_milestone_id": fixture.milestone_id,
        "release_policy_revision": "v076-policy-r1",
        "release_policy_digest": release_policy_digest,
        "release_policy": release_policy,
        "acceptance_evidence_matrix": [{
            "id": fixture.milestone_acceptance_check_id,
            "description": fixture.milestone_acceptance_check_description,
            "required": true,
            "evidence_kind": null,
            "check_definition_revision": fixture.milestone_definition_revision_id
        }],
        "capability_classes": ["repository_write"],
        "risk_classes": ["low"],
        "reviewer_independence_rules": ["independent-reviewer"],
        "elevated_operations": [],
        "adaptive_envelope": {
            "allowed_task_operations": ["split"],
            "fixed_outcomes": [],
            "fixed_acceptance": [],
            "fixed_risk_classes": ["low"],
            "forbidden_side_effects": [],
            "elevated_operations": []
        },
        "rollback_and_recovery": ["retry"],
        "exclusions": []
    });
    let rendered = services::render_execution_baseline(
        &serde_json::from_value(content.clone()).expect("baseline content type"),
    )
    .expect("baseline renderer");
    let draft = request_json(
        app,
        Method::POST,
        &format!("/api/v1/projects/{}/execution-baseline", fixture.project_id),
        token,
        json!({
            "mutation": {
                "expected_version": 0,
                "idempotency_key": "v076-baseline-draft",
                "authorization": user_authorization("project.execution_baseline.save_draft", "v076-baseline-draft-event")
            },
            "operation": "save_draft",
            "base_revision_id": null,
            "content": content.clone(),
            "rendered_view": rendered.rendered_view.clone(),
            "render_version": services::EXECUTION_BASELINE_RENDER_VERSION,
            "content_digest": rendered.content_digest.clone(),
            "render_digest": rendered.render_digest.clone(),
            "provenance": user_provenance("V076 baseline draft")
        }),
        &[StatusCode::CREATED, StatusCode::OK],
    )
    .await;
    assert_eq!(draft["baseline"]["lifecycle"], json!("draft"));
    assert_eq!(draft["requires_user_authorization"], json!(false));
    let baseline_id = required_string(&draft, &["baseline", "id"]);
    let baseline_id = baseline_id.as_str();
    let baseline_version = draft["baseline"]["version"]
        .as_i64()
        .expect("baseline version");
    let draft_revision_id = required_string(&draft, &["current_revision", "id"]);
    let saved = request_json(
        app,
        Method::POST,
        &format!("/api/v1/projects/{}/execution-baseline/{baseline_id}/revisions", fixture.project_id),
        token,
        json!({
            "mutation": {
                "expected_version": baseline_version,
                "idempotency_key": "v076-baseline-revision",
                "authorization": user_authorization("project.execution_baseline.propose_for_approval", "v076-baseline-revision-event")
            },
            "operation": "propose_for_approval",
            "base_revision_id": draft_revision_id,
            "content": content.clone(),
            "rendered_view": rendered.rendered_view.clone(),
            "render_version": services::EXECUTION_BASELINE_RENDER_VERSION,
            "content_digest": rendered.content_digest.clone(),
            "render_digest": rendered.render_digest.clone(),
            "provenance": user_provenance("V076 baseline revision")
        }),
        &[StatusCode::CREATED, StatusCode::OK],
    )
    .await;
    assert_eq!(saved["requires_user_authorization"], json!(true));
    assert_eq!(
        saved["approval_target"]["requires_user_authorization"],
        json!(true)
    );
    let revision_id = required_string(&saved, &["approval_target", "revision_id"]);
    let revised_version = saved["baseline"]["version"]
        .as_i64()
        .expect("revised baseline version");
    let approval_authorization = user_authorization(
        "project.execution_baseline.approve",
        "v076-baseline-approve-event",
    );
    let approved = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/execution-baseline/{baseline_id}/revisions/{revision_id}/approve",
            fixture.project_id
        ),
        token,
        json!({
            "mutation": {
                "expected_version": revised_version,
                "idempotency_key": "v076-baseline-approve",
                "authorization": approval_authorization.clone()
            },
            "revision_id": revision_id,
            "content_digest": rendered.content_digest.clone(),
            "render_digest": rendered.render_digest.clone(),
            "expected_project_version": project_version
        }),
        &[StatusCode::CREATED, StatusCode::OK],
    )
    .await;
    let approval_id = required_string(&approved, &["approval", "id"]);
    let approved_version = approved["baseline"]["version"]
        .as_i64()
        .expect("approved baseline version");
    if activate {
        let activation_body = json!({
            "mutation": {
                "expected_version": project_version,
                "idempotency_key": "v076-baseline-activate",
                "authorization": user_authorization("project.execution_baseline.activate", "v076-baseline-activate-event")
            },
            "baseline_id": baseline_id,
            "revision_id": revision_id,
            "approval_id": approval_id,
            "expected_baseline_version": approved_version,
            "content_digest": rendered.content_digest.clone(),
            "render_digest": rendered.render_digest.clone()
        });
        let activated = request_json(
            app,
            Method::POST,
            &format!(
                "/api/v1/projects/{}/execution-baseline/{baseline_id}/activate",
                fixture.project_id
            ),
            token,
            activation_body.clone(),
            &[StatusCode::OK],
        )
        .await;
        assert_eq!(activated["baseline"]["lifecycle"], json!("active"));
        assert_eq!(
            activated["baseline"]["version"],
            json!(approved_version + 1)
        );
        let activation_replay = request_json(
            app,
            Method::POST,
            &format!(
                "/api/v1/projects/{}/execution-baseline/{baseline_id}/activate",
                fixture.project_id
            ),
            token,
            activation_body,
            &[StatusCode::OK],
        )
        .await;
        assert_eq!(activation_replay, activated);
    }
    BaselineFixture {
        baseline_id: baseline_id.to_owned(),
        baseline_revision_id: revision_id,
        baseline_version: if activate {
            approved_version + 1
        } else {
            approved_version
        },
        approval_expected_baseline_version: revised_version,
        approval_expected_project_version: project_version,
        content_digest: rendered.content_digest,
        render_digest: rendered.render_digest,
        approval_id,
        approval_authorization,
    }
}

async fn activate_baseline(
    app: &Router,
    token: &str,
    fixture: &GenesisFixture,
    baseline: &BaselineFixture,
) {
    let project = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{}", fixture.project_id),
        token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let project_version = project["version"]
        .as_i64()
        .expect("activation project version");
    let activation_body = json!({
        "mutation": {
            "expected_version": project_version,
            "idempotency_key": "v076-baseline-activate",
            "authorization": user_authorization("project.execution_baseline.activate", "v076-baseline-activate-event")
        },
        "baseline_id": baseline.baseline_id,
        "revision_id": baseline.baseline_revision_id,
        "approval_id": baseline.approval_id,
        "expected_baseline_version": baseline.baseline_version,
        "content_digest": baseline.content_digest,
        "render_digest": baseline.render_digest
    });
    let activated = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/execution-baseline/{}/activate",
            fixture.project_id, baseline.baseline_id
        ),
        token,
        activation_body.clone(),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(activated["baseline"]["lifecycle"], json!("active"));
    let replay = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/execution-baseline/{}/activate",
            fixture.project_id, baseline.baseline_id
        ),
        token,
        activation_body,
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(replay, activated);
}

#[allow(clippy::too_many_arguments)]
async fn upload_project_media(
    app: &Router,
    token: &str,
    project_id: &str,
    expected_project_version: i64,
    idempotency_key: &str,
    authorization: &Value,
    bytes: &[u8],
    expected_status: StatusCode,
) -> Value {
    let boundary = format!("----v076-project-media-{idempotency_key}");
    let mutation = serde_json::to_vec(&json!({
        "mutation": {
            "expected_version": expected_project_version,
            "idempotency_key": idempotency_key,
            "authorization": authorization
        }
    }))
    .expect("media mutation serializes");
    let mut payload = Vec::new();
    payload.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"mutation\"\r\nContent-Type: application/json\r\n\r\n"
        )
        .as_bytes(),
    );
    payload.extend_from_slice(&mutation);
    payload.extend_from_slice(b"\r\n");
    payload.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"v076-proof.png\"\r\nContent-Type: image/png\r\n\r\n"
        )
        .as_bytes(),
    );
    payload.extend_from_slice(bytes);
    payload.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    let (status, response_body) = raw_request(
        app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/media"),
        token,
        Body::from(payload),
        Some(&format!("multipart/form-data; boundary={boundary}")),
    )
    .await;
    assert_eq!(
        status,
        expected_status,
        "unexpected media upload response: {}",
        String::from_utf8_lossy(&response_body)
    );
    serde_json::from_slice(&response_body).expect("media upload response JSON parses")
}

async fn upload_task_media(
    app: &Router,
    token: &str,
    task_id: &str,
    filename: &str,
    content_type: &str,
    bytes: &[u8],
    expected_status: StatusCode,
) -> Value {
    let boundary = format!("----v076-task-media-{task_id}");
    let mut payload = Vec::new();
    payload.extend_from_slice(
        format!("--{boundary}\r\nContent-Disposition: form-data; name=\"author_name\"\r\n\r\n")
            .as_bytes(),
    );
    payload.extend_from_slice(b"V076 user\r\n");
    payload.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\nContent-Type: {content_type}\r\n\r\n"
        )
        .as_bytes(),
    );
    payload.extend_from_slice(bytes);
    payload.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    let (status, response_body) = raw_request(
        app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/media"),
        token,
        Body::from(payload),
        Some(&format!("multipart/form-data; boundary={boundary}")),
    )
    .await;
    assert_eq!(
        status,
        expected_status,
        "unexpected Task media upload response: {}",
        String::from_utf8_lossy(&response_body)
    );
    serde_json::from_slice(&response_body).expect("Task media upload response JSON parses")
}

async fn raw_request(
    app: &Router,
    method: Method,
    uri: &str,
    token: &str,
    body: Body,
    content_type: Option<&str>,
) -> (StatusCode, Vec<u8>) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"));
    if let Some(content_type) = content_type {
        builder = builder.header(header::CONTENT_TYPE, content_type);
    }
    let response = app
        .clone()
        .oneshot(builder.body(body).expect("raw request builds"))
        .await
        .expect("raw request responds");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("raw response body reads")
        .to_vec();
    (status, bytes)
}

async fn connect_agent(app: &Router, token: &str, name: &str, permissions: &[&str]) -> Value {
    let entry = request_json(
        app,
        Method::POST,
        "/api/v1/providers",
        token,
        json!({
            "provider": "openai_compatible",
            "label": name,
            "credential": PROVIDER_SECRET,
            "base_url": "https://8.8.8.8"
        }),
        &[StatusCode::OK],
    )
    .await;
    request_json(
        app,
        Method::POST,
        "/api/v1/embedded-agents",
        token,
        json!({
            "name": name,
            "description": "V076 acceptance identity",
            "credential_id": entry["id"],
            "model": "v076-acceptance-model",
            "account_permission_ceiling": {"permissions": permissions},
            "tool_policy": {"allowed": permissions}
        }),
        &[StatusCode::OK],
    )
    .await
}

fn charter_content(name: &str, acceptance: &str) -> ProjectCharterContent {
    serde_json::from_value(json!({
        "identity": {
            "working_name": name,
            "slug_proposal": "v076-project",
            "one_line_vision": "An auditable orchestration flow.",
            "maturity": "mvp"
        },
        "problem_and_people": {
            "problem_or_opportunity": "Approved intent must survive handoff.",
            "target_users": ["Forge users"],
            "beneficiaries": ["Project collaborators"]
        },
        "core_experience": {"primary_outcome": "A bounded Project starts from approved intent."},
        "scope": {
            "must_have_outcomes": ["Persist the exact approved Charter."],
            "explicit_non_goals": ["Cross-project mutation"]
        },
        "success": {
            "success_signals": ["The handoff is replay-safe."],
            "acceptance_statements": [acceptance]
        },
        "constraints_and_risks": {
            "product": ["Single-user local-first operation."],
            "technology": ["SQLite and the existing API."],
            "security_privacy_compliance": ["Explicit user approval is required."]
        },
        "knowledge_ledger": {"items": []}
    }))
    .expect("V076 Charter content parses")
}

#[tokio::test]
async fn v076_baseline_authority_replay_checks_identity_before_auth() {
    let workspace = common::TestDir::new("v076-baseline-authority-replay-order");
    let harness = common::test_app(workspace.path(), "v076-baseline-authority-replay-order").await;
    let app = &harness.app;
    let token = common::test_jwt();
    let fixture = create_genesis_project(app, &token, "v076-replay-order").await;
    let baseline = create_baseline(app, &token, &fixture, false).await;

    let approval_body = json!({
        "mutation": {
            "expected_version": baseline.approval_expected_baseline_version,
            "idempotency_key": "v076-baseline-approve",
            "authorization": baseline.approval_authorization.clone()
        },
        "revision_id": baseline.baseline_revision_id,
        "content_digest": baseline.content_digest,
        "render_digest": baseline.render_digest,
        "expected_project_version": baseline.approval_expected_project_version
    });
    let approval_replay = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/execution-baseline/{}/revisions/{}/approve",
            fixture.project_id, baseline.baseline_id, baseline.baseline_revision_id
        ),
        &token,
        approval_body.clone(),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(approval_replay["baseline"]["lifecycle"], json!("approved"));

    let mut altered_authority = approval_body.clone();
    altered_authority["mutation"]["authorization"]["action"] =
        json!("project.execution_baseline.activate");
    let approval_authority_conflict = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/execution-baseline/{}/revisions/{}/approve",
            fixture.project_id, baseline.baseline_id, baseline.baseline_revision_id
        ),
        &token,
        altered_authority,
        &[StatusCode::CONFLICT],
    )
    .await;
    assert_eq!(
        approval_authority_conflict["code"],
        json!("idempotency_conflict")
    );

    let mut altered_target = approval_body.clone();
    altered_target["revision_id"] = json!("v076-different-revision");
    let approval_target_conflict = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/execution-baseline/{}/revisions/{}/approve",
            fixture.project_id, baseline.baseline_id, baseline.baseline_revision_id
        ),
        &token,
        altered_target,
        &[StatusCode::CONFLICT],
    )
    .await;
    assert_eq!(
        approval_target_conflict["code"],
        json!("idempotency_conflict")
    );

    let project = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{}", fixture.project_id),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let activation_body = json!({
        "mutation": {
            "expected_version": project["version"],
            "idempotency_key": "v076-baseline-activate",
            "authorization": user_authorization(
                "project.execution_baseline.activate",
                "v076-baseline-activate-event"
            )
        },
        "baseline_id": baseline.baseline_id,
        "revision_id": baseline.baseline_revision_id,
        "approval_id": baseline.approval_id,
        "expected_baseline_version": baseline.baseline_version,
        "content_digest": baseline.content_digest,
        "render_digest": baseline.render_digest
    });
    let activation = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/execution-baseline/{}/activate",
            fixture.project_id, baseline.baseline_id
        ),
        &token,
        activation_body.clone(),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(activation["baseline"]["lifecycle"], json!("active"));

    let mut altered_activation_authority = activation_body.clone();
    altered_activation_authority["mutation"]["authorization"]["action"] =
        json!("project.execution_baseline.approve");
    let activation_authority_conflict = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/execution-baseline/{}/activate",
            fixture.project_id, baseline.baseline_id
        ),
        &token,
        altered_activation_authority,
        &[StatusCode::CONFLICT],
    )
    .await;
    assert_eq!(
        activation_authority_conflict["code"],
        json!("idempotency_conflict")
    );

    let activation_target_conflict = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/execution-baseline/different-path-baseline/activate",
            fixture.project_id
        ),
        &token,
        activation_body,
        &[StatusCode::CONFLICT],
    )
    .await;
    assert_eq!(
        activation_target_conflict["code"],
        json!("idempotency_conflict")
    );
}

fn user_authorization(action: &str, event_id: impl Into<String>) -> Value {
    json!({
        "principal": {"kind": "user", "id": "test-user-id"},
        "authorization_basis": "explicit_user_authorization",
        "action": action,
        "event_id": event_id.into(),
        "occurred_at": Utc::now().to_rfc3339()
    })
}

/// Return replay requests that differ from `body` in exactly one authority
/// receipt field. `top_level_authorization` is true for the Project creation
/// envelope; all other orchestration mutations nest the receipt under
/// `mutation.authorization`.
fn user_authorization_replay_variants(
    body: &Value,
    top_level_authorization: bool,
) -> Vec<(&'static str, Value)> {
    let mut variants = Vec::new();
    for (label, field, replacement) in [
        (
            "authorization action",
            "action",
            json!("altered.authority.action"),
        ),
        (
            "authorization occurred_at",
            "occurred_at",
            json!(Utc::now()
                .checked_add_signed(Duration::seconds(1))
                .expect("authorization timestamp remains representable")
                .to_rfc3339()),
        ),
        (
            "authorization basis",
            "authorization_basis",
            json!("altered_authorization_basis"),
        ),
        (
            "authorization event",
            "event_id",
            json!("altered-authority-event"),
        ),
    ] {
        let mut altered = body.clone();
        let authorization = if top_level_authorization {
            altered
                .get_mut("authorization")
                .expect("top-level authorization")
        } else {
            altered
                .get_mut("mutation")
                .and_then(Value::as_object_mut)
                .and_then(|mutation| mutation.get_mut("authorization"))
                .expect("mutation authorization")
        };
        authorization[field] = replacement;
        variants.push((label, altered));
    }
    let mut altered = body.clone();
    let authorization = if top_level_authorization {
        altered
            .get_mut("authorization")
            .expect("top-level authorization")
    } else {
        altered
            .get_mut("mutation")
            .and_then(Value::as_object_mut)
            .and_then(|mutation| mutation.get_mut("authorization"))
            .expect("mutation authorization")
    };
    authorization["principal"]["id"] = json!("different-principal");
    variants.push(("authorization principal", altered));
    variants
}

fn user_provenance(summary: &str) -> Value {
    json!({
        "author": {"kind": "user", "id": "test-user-id"},
        "operating_skill_revision": "forge.project.orchestration/v1@4",
        "source_refs": [],
        "change_summary": summary
    })
}

fn project_policy_digest(policy: &Value) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"forge.project-agent-policy/v1\0");
    hasher.update(policy.to_string().as_bytes());
    hex::encode(hasher.finalize())
}

async fn request_json(
    app: &Router,
    method: Method,
    uri: &str,
    token: &str,
    body: Value,
    expected_statuses: &[StatusCode],
) -> Value {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::from(
            serde_json::to_vec(&body).expect("request JSON serializes"),
        ))
        .expect("request builds");
    let response = app.clone().oneshot(request).await.expect("router responds");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body reads");
    assert!(
        expected_statuses.contains(&status),
        "unexpected {status} from {uri}: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).expect("response JSON parses")
}

fn required_string(value: &Value, path: &[&str]) -> String {
    let mut current = value;
    for segment in path {
        current = current
            .get(*segment)
            .unwrap_or_else(|| panic!("missing JSON field {}", path.join(".")));
    }
    current
        .as_str()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| panic!("JSON field {} is not a non-empty string", path.join(".")))
        .to_owned()
}

fn git_default_branch(path: &std::path::Path) -> String {
    let output = std::process::Command::new("git")
        .args(["symbolic-ref", "--short", "HEAD"])
        .current_dir(path)
        .output()
        .expect("git default branch reads");
    assert!(output.status.success());
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}
