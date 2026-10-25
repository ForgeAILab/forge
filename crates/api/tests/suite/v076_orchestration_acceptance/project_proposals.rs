//! Scoped Project proposals and task materialization.

use super::*;

#[tokio::test]
async fn v076_typed_project_proposals_are_scoped_and_task_materializes() {
    let workspace = common::TestDir::new("v076-typed-project-proposals");
    let harness = common::test_app(workspace.path(), "v076-typed-project-proposals").await;
    let app = &harness.app;
    let token = common::test_jwt();
    let fixture = create_genesis_project(app, &token, "v076-typed").await;

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
            "review_requirement_ids": [format!(
                "{}:/scope/must_have_outcomes/0",
                fixture.charter_revision_id
            )],
            "role_assignments": [],
            "governance": {
                "charter_revision_id": fixture.charter_revision_id,
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
        "SELECT charter_revision_id, runnable
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
            "review_requirement_ids": [format!(
                "{}:/scope/must_have_outcomes/0",
                fixture.charter_revision_id
            )],
            "role_assignments": [],
            "governance": {
                "charter_revision_id": fixture.charter_revision_id,
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
    let provider = services_agents::CoordinationToolProvider::new(harness.state.db.clone());
    let scope = CanonicalScope {
        scope_type: CanonicalScopeType::Project,
        scope_id: fixture.project_id.clone(),
        workspace_access: WorkspaceAccess::Deny,
    };
    let document_proposal = provider
        .propose(
            &fixture.project_identity_id,
            &scope,
            "v076-document-proposal-session",
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
            "v076-decision-proposal-session",
            forge_agent_host::PROJECT_DECISION_OPERATION,
            json!({
                "payload": {
                    "action": "record_candidate",
                    "question": "Should the V076 typed flow be accepted?",
                    "options": ["yes", "no"],
                    "selected_outcome": "yes",
                    "rationale": "The acceptance suite covers the approved path.",
                    "decision_class": "project_implementation",
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
    // D19/F15: an in-envelope, already-authorized, already-decided
    // implementation choice (a firm `selected_outcome` plus `rationale`,
    // inside the active baseline's adaptive envelope) is written as an
    // effective Decision rather than left as a pending approval candidate,
    // even though this call self-declares `record_candidate`.
    let decision_candidate_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM project_decision_candidate WHERE project_id = ?")
            .bind(&fixture.project_id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("decision candidate count");
    assert_eq!(decision_candidate_count, 0);
    let decision_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM project_decision WHERE project_id = ?")
            .bind(&fixture.project_id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("effective decision count");
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
