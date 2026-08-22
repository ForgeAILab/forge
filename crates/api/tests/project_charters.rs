#![allow(dead_code)]

mod common;

use api_types::{
    ConnectedEmbeddedAgentResponse, ProductGenesisCharterResponse, ProjectCharterApproval,
    ProjectCharterRevision, ProjectResponse,
};
use axum::{http::Method, http::StatusCode};
use db::{
    CreateProjectCharter, CreateProjectCharterRevision, CreateProjectCharterRevisionAtomically,
    ProjectOrchestrationRepo,
};
use serde_json::json;

#[tokio::test]
async fn legacy_project_charter_is_explicitly_unverified_and_member_scoped() {
    let workspace = common::TestDir::new("project-charter-legacy");
    let harness = common::test_app(workspace.path(), "project-charter-legacy").await;
    let token = common::test_jwt();

    let project: ProjectResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({ "name": "Legacy Charter Project" }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(project.charter_status, "legacy_unverified");
    assert!(project.charter_setup_required);
    assert!(project.current_charter_id.is_none());
    assert!(project.current_charter_revision_id.is_none());

    let projection: ProductGenesisCharterResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{}/charter", project.id),
        &token,
        StatusCode::OK,
    )
    .await;
    assert!(projection.charter.is_none());
    assert!(projection.revisions.is_empty());
    assert!(projection.approval.is_none());
}

#[tokio::test]
async fn failed_first_draft_does_not_leave_an_empty_attached_charter() {
    let workspace = common::TestDir::new("project-charter-first-draft-failure");
    let harness = common::test_app(workspace.path(), "project-charter-first-draft-failure").await;
    let app = &harness.app;
    let token = common::test_jwt();
    let project: ProjectResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({ "name": "Failed Charter Project" }),
        StatusCode::OK,
    )
    .await;
    let content = adoption_content("Failed Charter Project", "invalid-render");
    let rendered = services::render_and_digest_charter(&content);
    let mut body = adoption_save_body(
        &content,
        &rendered,
        "failed-first-draft-charter",
        "failed-first-draft-save",
        "failed-first-draft-event",
    );
    body["rendered_view"] = json!("not-the-server-rendered-view");
    let response = common::raw_json_request(
        app,
        Method::POST,
        &format!("/api/v1/projects/{}/charter/revisions", project.id),
        body,
    )
    .await;
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let charter_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_charter WHERE id = 'failed-first-draft-charter'",
    )
    .fetch_one(harness.state.db.pool())
    .await
    .expect("failed draft charter count is queryable");
    let revision_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_charter_revision
         WHERE charter_id = 'failed-first-draft-charter'",
    )
    .fetch_one(harness.state.db.pool())
    .await
    .expect("failed draft revision count is queryable");
    assert_eq!(charter_count, 0);
    assert_eq!(revision_count, 0);

    let now = db::now_rfc3339();
    let direct_failure = ProjectOrchestrationRepo::create_project_charter_revision_atomically(
        &*harness.state.db,
        CreateProjectCharterRevisionAtomically {
            project_id: Some(project.id.clone()),
            genesis_session_id: None,
            account_id: "test-user-id".to_owned(),
            charter: CreateProjectCharter {
                id: "failed-transaction-charter".to_owned(),
                account_id: "test-user-id".to_owned(),
                genesis_session_id: None,
                project_mode: "compact".to_owned(),
                maturity: "mvp".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            revision: CreateProjectCharterRevision {
                id: "failed-transaction-revision".to_owned(),
                charter_id: "failed-transaction-charter".to_owned(),
                expected_charter_version: 1,
                project_mode: "compact".to_owned(),
                maturity: "mvp".to_owned(),
                base_revision: 0,
                base_revision_id: None,
                lifecycle: "proposed".to_owned(),
                schema_version: "forge.project-charter/v1".to_owned(),
                render_version: "forge.project-charter-render/v1".to_owned(),
                content_json: "not-json".to_owned(),
                rendered_view: "invalid".to_owned(),
                change_summary: "failure fixture".to_owned(),
                author_type: "user".to_owned(),
                author_id: Some("test-user-id".to_owned()),
                source_message_id: None,
                source_turn_job_id: None,
                source_refs_json: "[]".to_owned(),
                content_digest: "content-digest".to_owned(),
                rendered_digest: "rendered-digest".to_owned(),
                created_at: now,
                command_receipt: None,
                action_execution: None,
            },
            command_receipt: None,
            action_execution: None,
        },
    )
    .await;
    assert!(direct_failure.is_err());
    let rolled_back_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_charter
         WHERE id = 'failed-transaction-charter'",
    )
    .fetch_one(harness.state.db.pool())
    .await
    .expect("failed transaction charter count is queryable");
    assert_eq!(rolled_back_count, 0);
}

#[tokio::test]
async fn project_charter_claim_is_exclusive_for_concurrent_first_drafts() {
    let workspace = common::TestDir::new("project-charter-concurrent-claim");
    let harness = common::test_app(workspace.path(), "project-charter-concurrent-claim").await;
    let app = &harness.app;
    let token = common::test_jwt();
    let project_a: ProjectResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({ "name": "Concurrent Charter Project A" }),
        StatusCode::OK,
    )
    .await;
    let project_b: ProjectResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({ "name": "Concurrent Charter Project B" }),
        StatusCode::OK,
    )
    .await;
    let content = adoption_content("Concurrent Charter", "race");
    let rendered = services::render_and_digest_charter(&content);
    let body_a = adoption_save_body(
        &content,
        &rendered,
        "concurrent-shared-charter",
        "concurrent-charter-save-a",
        "concurrent-charter-event-a",
    );
    let body_b = adoption_save_body(
        &content,
        &rendered,
        "concurrent-shared-charter",
        "concurrent-charter-save-b",
        "concurrent-charter-event-b",
    );
    let project_a_path = format!("/api/v1/projects/{}/charter/revisions", project_a.id);
    let project_b_path = format!("/api/v1/projects/{}/charter/revisions", project_b.id);

    let (response_a, response_b) = tokio::join!(
        common::raw_json_request(app, Method::POST, &project_a_path, body_a,),
        common::raw_json_request(app, Method::POST, &project_b_path, body_b,),
    );
    let status_a = response_a.status();
    let status_b = response_b.status();
    assert_eq!(
        [status_a, status_b]
            .into_iter()
            .filter(|status| *status == StatusCode::CREATED)
            .count(),
        1,
        "exactly one Project may claim a first-draft Charter: {status_a} / {status_b}"
    );
    assert!(
        [status_a, status_b]
            .into_iter()
            .any(|status| matches!(status, StatusCode::NOT_FOUND | StatusCode::CONFLICT)),
        "the losing Project must receive an ownership response: {status_a} / {status_b}"
    );

    let winning_project_id = if status_a == StatusCode::CREATED {
        project_a.id
    } else {
        project_b.id
    };
    let charter_project_id: String = sqlx::query_scalar(
        "SELECT project_id FROM project_charter WHERE id = 'concurrent-shared-charter'",
    )
    .fetch_one(harness.state.db.pool())
    .await
    .expect("the shared Charter is claimed");
    assert_eq!(charter_project_id, winning_project_id);
    let revision_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_charter_revision
         WHERE charter_id = 'concurrent-shared-charter'",
    )
    .fetch_one(harness.state.db.pool())
    .await
    .expect("the shared Charter revision count is queryable");
    assert_eq!(revision_count, 1);
}

#[tokio::test]
async fn project_charter_adoption_and_amendment_commit_with_binding_history() {
    let workspace = common::TestDir::new("project-charter-adoption-amendment");
    let harness = common::test_app(workspace.path(), "project-charter-adoption-amendment").await;
    let app = &harness.app;
    let token = common::test_jwt();

    let project: ProjectResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({ "name": "Legacy Charter Project" }),
        StatusCode::OK,
    )
    .await;
    let connected = connect_embedded_agent(app, &token, "project-charter-fixture-agent-1").await;
    let content = adoption_content("Legacy Charter Project", "one");
    let rendered = services::render_and_digest_charter(&content);

    let save: ProjectCharterRevision = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!("/api/v1/projects/{}/charter/revisions", project.id),
        &token,
        adoption_save_body(
            &content,
            &rendered,
            "project-adoption-charter",
            "project-adoption-revision-1",
            "project-adoption-save-1",
        ),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(save.revision_number, 1);
    assert!(save.base_revision_id.is_none());

    let replay: ProjectCharterRevision = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!("/api/v1/projects/{}/charter/revisions", project.id),
        &token,
        adoption_save_body(
            &content,
            &rendered,
            "project-adoption-charter",
            "project-adoption-revision-replay",
            "project-adoption-save-replay",
        ),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(replay.id, save.id);

    let projection: ProductGenesisCharterResponse = common::empty_request_with_bearer(
        app,
        Method::GET,
        &format!("/api/v1/projects/{}/charter", project.id),
        &token,
        StatusCode::OK,
    )
    .await;
    let charter = projection
        .charter
        .expect("adoption draft is attached after revision");
    assert_eq!(charter.version, 2);

    let stale_approval = common::raw_json_request(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/charter/revisions/{}/approve",
            project.id, save.id
        ),
        approval_body(
            &charter.id,
            &save,
            charter.version,
            project.version + 1,
            &connected,
            &rendered,
            "project-adoption-approval-stale-project",
        ),
    )
    .await;
    assert_eq!(stale_approval.status(), StatusCode::CONFLICT);

    let approval: ProjectCharterApproval = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/charter/revisions/{}/approve",
            project.id, save.id
        ),
        &token,
        approval_body(
            &charter.id,
            &save,
            charter.version,
            project.version,
            &connected,
            &rendered,
            "project-adoption-approval-1",
        ),
        StatusCode::CREATED,
    )
    .await;
    assert!(matches!(
        approval.state,
        api_types::CharterApprovalState::Consumed
    ));

    let project_after_adoption: ProjectResponse = common::empty_request_with_bearer(
        app,
        Method::GET,
        &format!("/api/v1/projects/{}", project.id),
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(project_after_adoption.charter_status, "charter_backed");
    assert_eq!(
        project_after_adoption
            .current_charter_revision_id
            .as_deref(),
        Some(save.id.as_str())
    );
    let approval_lifecycle: String =
        sqlx::query_scalar("SELECT lifecycle FROM project_charter_approval WHERE id = ?")
            .bind(&approval.id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("adoption approval lifecycle is durable");
    assert_eq!(approval_lifecycle, "consumed");
    let approval_event_occurred_at: String = sqlx::query_scalar(
        "SELECT occurred_at FROM project_charter_approval_event WHERE id = ? AND approval_id = ?",
    )
    .bind(&approval.approval_event_id)
    .bind(&approval.id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("adoption approval event keeps the exact authorization timestamp");
    assert_eq!(
        approval_event_occurred_at,
        approval.authorization.occurred_at
    );
    let bootstrap_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_chat_message
         WHERE source_type = 'native' AND source_metadata_json LIKE '%project_charter_adoption%'",
    )
    .fetch_one(harness.state.db.pool())
    .await
    .expect("adoption bootstrap message is durable");
    assert_eq!(bootstrap_count, 1);

    let rotated = connect_embedded_agent(app, &token, "project-charter-fixture-agent-2").await;
    let mut amendment = content.clone();
    amendment.scope.must_have_outcomes = vec![
        "Preserve the existing Project while recording an exact Charter amendment.".to_owned(),
    ];
    let amendment_rendered = services::render_and_digest_charter(&amendment);
    let save_amendment: ProjectCharterRevision = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!("/api/v1/projects/{}/charter/revisions", project.id),
        &token,
        json!({
            "mutation": {
                "expected_version": charter.version + 1,
                "expected_digest": save.content_digest,
                "idempotency_key": "project-amendment-revision-2",
                "authorization": user_authorization(
                    "project_charter.revision.save", "project-amendment-save-2"
                )
            },
            "charter_id": charter.id,
            "base_revision_id": save.id,
            "project_mode": "compact",
            "maturity": "mvp",
            "content": amendment,
            "rendered_view": amendment_rendered.rendered_view,
            "render_version": amendment_rendered.render_version,
            "provenance": {
                "author": { "kind": "user", "id": "test-user-id" },
                "source_refs": [],
                "change_summary": "Amend the adopted outcome"
            }
        }),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(
        save_amendment.base_revision_id.as_deref(),
        Some(save.id.as_str())
    );

    let amendment_approval: ProjectCharterApproval = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/charter/revisions/{}/approve",
            project.id, save_amendment.id
        ),
        &token,
        approval_body(
            &charter.id,
            &save_amendment,
            charter.version + 2,
            project_after_adoption.version,
            &rotated,
            &amendment_rendered,
            "project-amendment-approval-2",
        ),
        StatusCode::CREATED,
    )
    .await;
    assert!(matches!(
        amendment_approval.approval_type,
        api_types::CharterApprovalType::CharterAmendment
    ));
    assert!(matches!(
        amendment_approval.state,
        api_types::CharterApprovalState::Consumed
    ));

    let current_binding_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM project_agent_binding WHERE project_id = ?")
            .bind(&project.id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("binding history is queryable");
    assert_eq!(current_binding_count, 2);
    let replaced_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_agent_binding
         WHERE project_id = ? AND state = 'replaced'",
    )
    .bind(&project.id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("binding replacement history is durable");
    assert_eq!(replaced_count, 1);
    let active_identity: String = sqlx::query_scalar(
        "SELECT identity_id FROM project_agent_binding
         WHERE project_id = ? AND state = 'active'",
    )
    .bind(&project.id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("active replacement binding is queryable");
    assert_eq!(active_identity, rotated.agent.id);
    let amendment_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_charter_amendment
         WHERE project_id = ? AND lifecycle = 'approved' AND approval_id = ?",
    )
    .bind(&project.id)
    .bind(&amendment_approval.id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("typed Charter amendment is durable");
    assert_eq!(amendment_count, 1);
    let current: ProjectResponse = common::empty_request_with_bearer(
        app,
        Method::GET,
        &format!("/api/v1/projects/{}", project.id),
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        current.current_charter_revision_id.as_deref(),
        Some(save_amendment.id.as_str())
    );

    let milestone_content = json!({
        "name": "First usable todo CLI",
        "outcome": "The repository contains a tested local todo CLI.",
        "included_scope": ["CLI implementation", "persistent storage", "automated tests"],
        "excluded_scope": ["network service", "graphical UI"],
        "charter_revision": {
            "artifact_id": charter.id,
            "revision_id": save_amendment.id,
            "content_digest": save_amendment.content_digest,
            "render_version": save_amendment.render_version,
            "render_digest": save_amendment.render_digest
        },
        "document_revisions": [],
        "task_ids": [],
        "dependencies": [],
        "risks": [],
        "acceptance_checks": [],
        "evidence_requirements": [],
        "known_issues": [],
        "target_date": null
    });
    let milestone_rendered =
        api_types::canonical_json(&milestone_content).expect("milestone renders canonically");
    let milestone: api_types::ProjectMilestone = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!("/api/v1/projects/{}/milestones", project.id),
        &token,
        json!({
            "mutation": {
                "expected_version": current.version,
                "idempotency_key": "project-charter-milestone-create",
                "authorization": user_authorization(
                    "project.milestone.create", "project-charter-milestone-create-event"
                )
            },
            "display_label": "Todo CLI v0.1",
            "lifecycle": "proposed",
            "content": milestone_content,
            "rendered_view": milestone_rendered,
            "render_version": "forge.milestone-definition-render/v1",
            "change_summary": "Define the first implementation milestone",
            "provenance": {
                "author": { "kind": "user", "id": "test-user-id" },
                "source_refs": [],
                "change_summary": "Define the first implementation milestone"
            }
        }),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(milestone.project_id, project.id);
    assert!(!milestone.definition_revision_id.is_empty());
}

async fn connect_embedded_agent(
    app: &axum::Router,
    token: &str,
    name: &str,
) -> ConnectedEmbeddedAgentResponse {
    common::connect_embedded_agent(
        app,
        token,
        name,
        "project-charter-test",
        "fixture-secret",
        json!({"permissions": ["read_project", "handoff"]}),
        json!({"allowed": ["read_project", "handoff"]}),
    )
    .await
}

fn project_agent_policy_digest(policy: &serde_json::Value) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"forge.project-agent-policy/v1\0");
    digest.update(policy.to_string().as_bytes());
    hex::encode(digest.finalize())
}

fn user_authorization(action: &str, event_id: &str) -> serde_json::Value {
    static AUTHORIZATION_OCCURRED_AT: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    json!({
        "principal": { "kind": "user", "id": "test-user-id" },
        "authorization_basis": "explicit_user_authorization",
        "action": action,
        "event_id": event_id,
        "occurred_at": AUTHORIZATION_OCCURRED_AT.get_or_init(db::now_rfc3339)
    })
}

fn adoption_save_body(
    content: &api_types::ProjectCharterContent,
    rendered: &services::CharterRender,
    charter_id: &str,
    idempotency_key: &str,
    event_id: &str,
) -> serde_json::Value {
    json!({
        "mutation": {
            "expected_version": 1,
            "idempotency_key": idempotency_key,
            "authorization": user_authorization("project_charter.revision.save", event_id)
        },
        "charter_id": charter_id,
        "base_revision_id": null,
        "project_mode": "compact",
        "maturity": "mvp",
        "content": content,
        "rendered_view": rendered.rendered_view,
        "render_version": rendered.render_version,
        "provenance": {
            "author": { "kind": "user", "id": "test-user-id" },
            "source_refs": [],
            "change_summary": "Adopt the existing Project explicitly"
        }
    })
}

fn approval_body(
    charter_id: &str,
    revision: &ProjectCharterRevision,
    expected_charter_version: i64,
    expected_project_version: i64,
    connected: &ConnectedEmbeddedAgentResponse,
    rendered: &services::CharterRender,
    idempotency_key: &str,
) -> serde_json::Value {
    json!({
        "mutation": {
            "expected_version": expected_charter_version,
            "expected_digest": revision.content_digest,
            "idempotency_key": idempotency_key,
            "authorization": user_authorization(
                "project_charter.approval", &format!("{idempotency_key}-event")
            )
        },
        "charter_id": charter_id,
        "revision_id": revision.id,
        "content_digest": revision.content_digest,
        "render_digest": rendered.render_digest,
        "expected_charter_version": expected_charter_version,
        "expected_project_version": expected_project_version,
        "approved_project_name": revision.content.identity.working_name,
        "approved_project_slug": revision.content.identity.slug_proposal,
        "project_mode": "compact",
        "selected_project_agent_identity_id": connected.agent.id,
        "selected_project_agent_profile_revision_id": connected.profile.id,
        "selected_project_agent_operating_skill_revision": "forge.project.orchestration/v1@2",
        "selected_project_agent_policy_digest": project_agent_policy_digest(&connected.profile.tool_policy)
    })
}

fn adoption_content(name: &str, suffix: &str) -> api_types::ProjectCharterContent {
    serde_json::from_value(json!({
        "identity": {
            "working_name": name,
            "slug_proposal": "legacy-charter-project",
            "one_line_vision": "Keep an existing Project intent durable and auditable.",
            "maturity": "mvp",
            "lifecycle_intent": "validate the smallest useful workflow",
            "project_type": "product",
            "value_proposition": "Preserve exact approved project intent."
        },
        "problem_and_people": {
            "problem_or_opportunity": "Existing Project intent is scattered across mutable history.",
            "target_users": ["Forge builders"],
            "beneficiaries": ["Project collaborators"],
            "jobs_pains_opportunity": ["Keep one bounded source of truth."],
            "current_alternatives": ["Unversioned chat notes"],
            "stakeholders": ["Project owner"],
            "excluded_audiences": ["Unrelated projects"]
        },
        "core_experience": {
            "primary_outcome": "A Project Agent can continue from the approved Charter.",
            "core_loop": "discover, approve, hand off, validate",
            "principal_journeys": ["Owner approves then Project Agent executes."]
        },
        "scope": {
            "must_have_outcomes": [format!("Persist the approved Project outcome ({suffix}).")],
            "required_deliverables": ["One durable Project Chat."],
            "later_possibilities": ["Expand Project-local execution."],
            "explicit_non_goals": ["Managing unrelated Projects"]
        },
        "success": {
            "qualitative_outcome": "Project intent remains verifiable.",
            "success_signals": ["Agent starts without restating intent."],
            "acceptance_statements": ["A replay does not create a second Project."],
            "required_evidence": ["Database assertions and API integration test."],
            "non_claims": ["This does not prove implementation quality."]
        },
        "constraints_and_risks": {
            "product": ["Single Project local-first operation."],
            "time_and_budget": ["One bounded iteration."],
            "technology": ["SQLite and the existing Forge API."],
            "data": ["Do not copy hidden chat history."],
            "integrations": [],
            "security_privacy_compliance": ["Require explicit user approval."],
            "accessibility": [],
            "operations": [],
            "migration": [],
            "launch": [],
            "agent_authority": ["Project Agent remains Project-scoped."],
            "risks": []
        },
        "knowledge_ledger": { "items": [] },
        "handoff_note": {
            "recommended_first_action": "Create the first Project-local execution plan.",
            "bounded_summary": "Start from the approved Project outcome.",
            "unresolved_item_ids": []
        }
    }))
    .expect("valid Project Charter fixture")
}
