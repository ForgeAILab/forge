//! REST coverage for the shared reconciliation list/detail/resolve routes
//! (design D15, finding F10).
//!
//! Before this change, `crates/api/src/routes/project_overview.rs` advertised
//! `project.reconciliation.resolve` as the Project's next action while no
//! route existed anywhere for it. This file proves the full path: a
//! `project_reconciliation_record` in state `required` is visible through
//! `GET /reconciliations`, resolvable through `POST
//! .../reconciliations/{id}/resolve`, and Project Overview's `next_action`
//! names this exact route while the conflict is unresolved.

mod common;

use api_types::{
    ProjectOverview, ProjectReconciliation, ProjectReconciliationListResponse, ProjectResponse,
};
use axum::http::{Method, StatusCode};
use db::{CreateProjectCanonicalConflict, CreateProjectReconciliation, ProjectOrchestrationRepo};
use serde_json::json;

const TEST_USER_ID: &str = "test-user-id";

/// Attach a minimal approved Charter directly, bypassing the full Genesis
/// adoption flow.  Mirrors `project_overview.rs`'s own fixture: a fresh
/// `POST /projects` project is `legacy_unverified`/`charter_setup_required`,
/// which -- correctly -- outranks a reconciliation in `next_action()`'s
/// priority order.  Reaching the reconciliation next-action requires a
/// Charter-backed Project first.
async fn seed_charter_backed_project(harness: &common::Harness, project_id: &str) {
    let now = db::now_rfc3339();
    let charter_content = json!({
        "identity": {
            "working_name": "TaskBoard",
            "slug_proposal": "taskboard",
            "one_line_vision": "Ship TaskBoard.",
            "maturity": "mvp"
        },
        "problem_and_people": {
            "problem_or_opportunity": "TaskBoard needs a first release."
        },
        "core_experience": {
            "primary_outcome": "A user can complete the TaskBoard workflow."
        },
        "scope": {},
        "success": {},
        "constraints_and_risks": {},
        "knowledge_ledger": {}
    });
    sqlx::query(
        "INSERT INTO project_charter (
            id, account_id, project_id, project_mode, maturity, lifecycle,
            version, created_at, updated_at
         ) VALUES ('reconciliation-test-charter', 'test-user-id', ?, 'compact', 'mvp',
                   'attached', 1, ?, ?)",
    )
    .bind(project_id)
    .bind(&now)
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("insert compact charter");
    sqlx::query(
        "INSERT INTO project_charter_revision (
            id, charter_id, revision, base_revision, lifecycle, schema_version,
            render_version, content_json, rendered_view, change_summary,
            author_type, author_id, source_refs_json, content_digest,
            rendered_digest, created_at
         ) VALUES ('reconciliation-test-charter-r1', 'reconciliation-test-charter', 1, 0,
            'approved', 'charter-test', 'v1', ?, '# TaskBoard', 'initial', 'user',
            'test-user-id', '[]', 'charter-content-r1', 'charter-render-r1', ?)",
    )
    .bind(charter_content.to_string())
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("insert compact charter revision");
    sqlx::query(
        "INSERT INTO project_charter_approval (
            id, approval_type, charter_id, revision_id, content_digest,
            rendered_digest, expected_charter_version, approving_principal_type,
            approving_principal_id, authorization_basis, authorization_action,
            authorization_occurred_at, explicit_event,
            source_action, lifecycle, consumed_project_id, consumed_at,
            idempotency_key, version, created_at, updated_at,
            approved_project_mode
         ) VALUES ('reconciliation-test-charter-approval-r1', 'project_creation',
            'reconciliation-test-charter', 'reconciliation-test-charter-r1',
            'charter-content-r1', 'charter-render-r1', 1, 'user', 'test-user-id', 'test',
            'project_charter.approval', ?, 'charter.approved',
            'project_charter.approval', 'consumed', ?, ?,
            'reconciliation-test-approval-r1', 1, ?, ?, 'compact')",
    )
    .bind(&now)
    .bind(project_id)
    .bind(&now)
    .bind(&now)
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("insert charter approval");
    sqlx::query(
        "UPDATE project_charter
         SET current_approved_revision_id = 'reconciliation-test-charter-r1', version = 2,
             updated_at = ?
         WHERE id = 'reconciliation-test-charter'",
    )
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("point charter at approved revision");
    sqlx::query(
        "UPDATE project
         SET current_charter_id = 'reconciliation-test-charter',
             current_charter_revision_id = 'reconciliation-test-charter-r1',
             current_charter_version = 1, charter_status = 'charter_backed',
             charter_setup_required = 0, version = version + 1
         WHERE id = ?",
    )
    .bind(project_id)
    .execute(harness.state.db.pool())
    .await
    .expect("mark project charter-backed");
}

async fn seed_adaptive_boundary_reconciliation(state: &api::AppState, project_id: &str) -> String {
    let now = db::now_rfc3339();
    let conflict = ProjectOrchestrationRepo::create_project_canonical_conflict(
        &*state.db,
        CreateProjectCanonicalConflict {
            id: format!("{project_id}-conflict"),
            project_id: project_id.to_owned(),
            domain: "execution".to_owned(),
            governing_record_type: "execution_baseline".to_owned(),
            governing_record_id: format!("{project_id}-baseline"),
            governing_record_revision: "2".to_owned(),
            governing_record_digest: "digest-governing".to_owned(),
            conflicting_record_type: "task".to_owned(),
            conflicting_record_id: format!("{project_id}-task"),
            conflicting_record_revision: "1".to_owned(),
            conflicting_record_digest: "digest-conflicting".to_owned(),
            affected_paths_json: r#"["outcome","acceptance"]"#.to_owned(),
            conflict_code: "adaptive_task_boundary_crossed".to_owned(),
            description: "adaptive Task operation 'replace' is outside the approved envelope"
                .to_owned(),
            detected_by_type: "system".to_owned(),
            detected_by_id: Some("task-service".to_owned()),
            authorization_basis: "adaptive_task_boundary".to_owned(),
            authorization_action: "task.adaptive.reject".to_owned(),
            explicit_event: "task.adaptive.replace.rejected".to_owned(),
            authorization_occurred_at: now.clone(),
            idempotency_key: format!("{project_id}-adaptive-boundary"),
            created_at: now.clone(),
        },
    )
    .await
    .expect("canonical conflict creates");

    let reconciliation = ProjectOrchestrationRepo::create_project_reconciliation(
        &*state.db,
        CreateProjectReconciliation {
            id: format!("{project_id}-reconciliation"),
            project_id: project_id.to_owned(),
            conflict_id: conflict.id,
            record_type: "task".to_owned(),
            record_id: format!("{project_id}-task"),
            record_revision: "1".to_owned(),
            record_digest: "digest-conflicting".to_owned(),
            governing_record_type: "execution_baseline".to_owned(),
            governing_record_id: format!("{project_id}-baseline"),
            governing_record_revision: "2".to_owned(),
            governing_record_digest: "digest-governing".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("reconciliation record creates");
    assert_eq!(reconciliation.state, "required");
    reconciliation.id
}

fn resolve_body(expected_version: i64, idempotency_key: &str) -> serde_json::Value {
    json!({
        "mutation": {
            "expected_version": expected_version,
            "expected_digest": null,
            "idempotency_key": idempotency_key,
            "deduplication_key": null,
            "authorization": {
                "principal": { "kind": "user", "id": TEST_USER_ID, "display_name": null },
                "authorization_basis": "interactive_user_reconciliation_resolution",
                "action": "project.reconciliation.resolve",
                "event_id": idempotency_key,
                "occurred_at": db::now_rfc3339(),
            }
        },
        "action": "retained",
        "replacement_ref": null,
        "reason": "The approved envelope remains authoritative; the adaptive replace is rejected."
    })
}

#[tokio::test]
async fn required_reconciliation_has_a_reachable_list_detail_and_resolve_route() {
    let workspace = common::TestDir::new("project-reconciliations-happy-path");
    let harness = common::test_app(workspace.path(), "project-reconciliations-happy-path").await;
    let token = common::test_jwt();

    let project: ProjectResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({ "name": "TaskBoard" }),
        StatusCode::OK,
    )
    .await;
    seed_charter_backed_project(&harness, &project.id).await;

    let reconciliation_id =
        seed_adaptive_boundary_reconciliation(&harness.state, &project.id).await;

    // F10 regression: Project Overview's next action names this exact route
    // while the conflict is unresolved, not a dead label.
    let overview: ProjectOverview = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{}/overview", project.id),
        &token,
        StatusCode::OK,
    )
    .await;
    let next_action = overview
        .next_action
        .expect("a required reconciliation is a next action");
    assert_eq!(
        next_action.route_or_operation,
        "project.reconciliation.resolve"
    );

    let listed: ProjectReconciliationListResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{}/reconciliations", project.id),
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(listed.items.len(), 1);
    assert_eq!(listed.items[0].id, reconciliation_id);
    assert_eq!(listed.items[0].allowed_actions.len(), 5);

    let detail: ProjectReconciliation = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/reconciliations/{}",
            project.id, reconciliation_id
        ),
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        detail.conflict.conflict_code,
        "adaptive_task_boundary_crossed"
    );

    let resolved: serde_json::Value = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/reconciliations/{}/resolve",
            project.id, reconciliation_id
        ),
        &token,
        resolve_body(1, "reconciliation-resolve-rest-1"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(resolved["reconciliation"]["state"], "retained");
    assert!(resolved["reconciliation"]["allowed_actions"]
        .as_array()
        .expect("allowed_actions array")
        .is_empty());
    assert!(resolved["receipt_id"]
        .as_str()
        .is_some_and(|id| !id.is_empty()));
    assert!(resolved["event_id"]
        .as_str()
        .is_some_and(|id| !id.is_empty()));

    // Overview's next action moves past the resolved reconciliation.
    let overview_after: ProjectOverview = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{}/overview", project.id),
        &token,
        StatusCode::OK,
    )
    .await;
    let after_action = overview_after
        .next_action
        .map(|action| action.route_or_operation);
    assert_ne!(
        after_action.as_deref(),
        Some("project.reconciliation.resolve")
    );
}

#[tokio::test]
async fn resolve_conflicts_on_a_stale_expected_version() {
    let workspace = common::TestDir::new("project-reconciliations-version-conflict");
    let harness =
        common::test_app(workspace.path(), "project-reconciliations-version-conflict").await;
    let token = common::test_jwt();

    let project: ProjectResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({ "name": "TaskBoard" }),
        StatusCode::OK,
    )
    .await;
    let reconciliation_id =
        seed_adaptive_boundary_reconciliation(&harness.state, &project.id).await;

    let response = common::json_request_with_bearer::<serde_json::Value>(
        &harness.app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/reconciliations/{}/resolve",
            project.id, reconciliation_id
        ),
        &token,
        resolve_body(99, "reconciliation-resolve-stale"),
        StatusCode::CONFLICT,
    )
    .await;
    assert!(response.get("code").is_some());
}
