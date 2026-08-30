#![allow(dead_code)]

mod common;

use api_types::{ErrorResponse, PaginatedResponse, ProjectOverview, ProjectResponse};
use axum::{http::Method, http::StatusCode};
use serde_json::json;

#[tokio::test]
async fn project_overview_returns_truthful_setup_projection() {
    let workspace = common::TestDir::new("project-overview-setup");
    let harness = common::test_app(workspace.path(), "project-overview-setup").await;
    let token = common::test_jwt();

    let project: ProjectResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({"name": "Overview setup project"}),
        StatusCode::OK,
    )
    .await;

    // Ownership is authoritative even if the best-effort membership insert
    // was lost; the Overview must not lock the Project owner out.
    sqlx::query("DELETE FROM project_member WHERE project_id = ?")
        .bind(&project.id)
        .execute(harness.state.db.pool())
        .await
        .expect("remove redundant owner membership");

    let overview: ProjectOverview = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{}/overview", project.id),
        &token,
        StatusCode::OK,
    )
    .await;

    assert_eq!(overview.project_id, project.id);
    assert_eq!(overview.project_name, "Overview setup project");
    assert_eq!(
        overview.charter_state,
        api_types::ProjectCharterState::CharterSetupRequired
    );
    assert_eq!(
        overview.projection_state,
        api_types::OverviewProjectionState::Stale
    );
    assert!(overview.current_charter.is_none());
    assert!(overview.active_milestones.is_empty());
    let next_action = overview.next_action.as_ref().expect("next action");
    assert_eq!(next_action.code, "charter_adoption");
    assert_eq!(next_action.required_principal, "user");
    assert_eq!(overview.task_counts.total, 0);
    assert_eq!(overview.check_summary.required_total, 0);
}

#[tokio::test]
async fn project_owner_can_list_and_get_without_membership_row() {
    let workspace = common::TestDir::new("project-owner-visibility");
    let harness = common::test_app(workspace.path(), "project-owner-visibility").await;
    let token = common::test_jwt();
    let project: ProjectResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({"name": "Owner visibility project"}),
        StatusCode::OK,
    )
    .await;

    // Direct/API creation labels the owner on the Project itself; membership
    // is not the authority source and may be absent on a legacy row.
    sqlx::query("DELETE FROM project_member WHERE project_id = ?")
        .bind(&project.id)
        .execute(harness.state.db.pool())
        .await
        .expect("remove owner membership row");

    let fetched: ProjectResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{}", project.id),
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(fetched.id, project.id);

    let listed: PaginatedResponse<ProjectResponse> = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/projects",
        &token,
        StatusCode::OK,
    )
    .await;
    assert!(listed.items.iter().any(|item| item.id == project.id));
}

#[tokio::test]
async fn project_overview_does_not_probe_an_unknown_project() {
    let workspace = common::TestDir::new("project-overview-auth");
    let harness = common::test_app(workspace.path(), "project-overview-auth").await;

    let error: ErrorResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/projects/not-a-project/overview",
        &common::test_jwt(),
        StatusCode::NOT_FOUND,
    )
    .await;

    assert_eq!(error.code, "not_found");
    assert!(error.message.contains("project"));
}

#[tokio::test]
async fn project_overview_denies_a_non_member_without_probing_project_rows() {
    let workspace = common::TestDir::new("project-overview-denied");
    let harness = common::test_app(workspace.path(), "project-overview-denied").await;
    let project: ProjectResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        &common::test_jwt(),
        json!({"name": "Private overview project"}),
        StatusCode::OK,
    )
    .await;
    let error: ErrorResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{}/overview", project.id),
        &jwt_for_user("different-user-id", "different@example.com"),
        StatusCode::NOT_FOUND,
    )
    .await;

    assert_eq!(error.code, "not_found");
    assert!(error.message.contains("project"));
}

#[tokio::test]
async fn project_overview_projects_canonical_active_milestone_state() {
    let workspace = common::TestDir::new("project-overview-milestone");
    let harness = common::test_app(workspace.path(), "project-overview-milestone").await;
    let token = common::test_jwt();

    let project: ProjectResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({"name": "Milestone projection project"}),
        StatusCode::OK,
    )
    .await;
    let now = db::now_rfc3339();
    let milestone_id = "overview-milestone";
    let definition_id = "overview-milestone-definition";
    sqlx::query(
        "INSERT INTO project_milestone (
            id, project_id, milestone_sequence, milestone_key, display_label,
            lifecycle, blocker_reason_json, stale_reason_json,
            reconciliation_reason_json, version, created_at, updated_at
         ) VALUES (?, ?, 1, 'M001', 'First bounded outcome', 'active', '[]', '[]', '[]', 1, ?, ?)",
    )
    .bind(milestone_id)
    .bind(&project.id)
    .bind(&now)
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("insert overview milestone");
    sqlx::query(
        r#"INSERT INTO project_milestone_revision (
            id, milestone_id, revision, base_revision, lifecycle,
            display_label, outcome, included_scope_json, excluded_scope_json,
            document_revisions_json, task_selection_json, dependencies_json,
            risks_json, acceptance_checks_json, evidence_requirements_json,
            known_issues_json, change_summary, schema_version, render_version,
            rendered_view, content_digest, rendered_digest, author_type, author_id,
            source_refs_json, created_at
         ) VALUES (?, ?, 1, 0, 'approved', 'First bounded outcome',
            'Ship the first bounded outcome.', '[]', '[]', '[]', '[]', '[]',
            '[]', '[{"id":"overview-current-check","description":"Current manual check","required":true,"source_kind":"manual","expected_result":"pass"}]',
            '[{"id":"overview-current-check","description":"Current proof","required":true,"evidence_kind":"report"}]',
            '[]', 'initial', 'test', 'v1', 'First bounded outcome',
            'digest-definition', 'digest-render', 'user', 'test-user-id', '[]', ?)"#,
    )
    .bind(definition_id)
    .bind(milestone_id)
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("insert overview milestone definition");
    sqlx::query("UPDATE project_milestone SET current_definition_revision_id = ? WHERE id = ?")
        .bind(definition_id)
        .bind(milestone_id)
        .execute(harness.state.db.pool())
        .await
        .expect("point milestone at definition");
    sqlx::query(
        "INSERT INTO project_milestone_check (
            id, project_id, milestone_id, definition_revision_id, check_key,
            description, required, source_kind, expected_result,
            evidence_required, version, current_result_id, created_at, updated_at
         ) VALUES ('overview-current-check', ?, ?, ?, 'overview-current-check',
            'Current manual check', 1, 'manual', 'pass', 1, 3, NULL, ?, ?)",
    )
    .bind(&project.id)
    .bind(milestone_id)
    .bind(definition_id)
    .bind(&now)
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("insert current milestone check");
    sqlx::query(
        "INSERT INTO project_milestone_revision (
            id, milestone_id, revision, base_revision, base_revision_id, lifecycle,
            display_label, outcome, included_scope_json, excluded_scope_json,
            document_revisions_json, task_selection_json, dependencies_json,
            risks_json, acceptance_checks_json, evidence_requirements_json,
            known_issues_json, change_summary, schema_version, render_version,
            rendered_view, content_digest, rendered_digest, author_type, author_id,
            source_refs_json, created_at
         ) VALUES ('overview-stale-definition', ?, 2, 1, ?, 'superseded',
            'Stale outcome', 'Stale definition.', '[]', '[]', '[]', '[]', '[]',
            '[]', '[]', '[]', '[]', 'stale', 'test', 'v1', 'Stale outcome',
            'digest-stale-definition', 'digest-stale-render', 'user',
            'test-user-id', '[]', ?)",
    )
    .bind(milestone_id)
    .bind(definition_id)
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("insert stale milestone definition");
    sqlx::query(
        "INSERT INTO project_milestone_check (
            id, project_id, milestone_id, definition_revision_id, check_key,
            description, required, source_kind, expected_result,
            evidence_required, version, current_result_id, created_at, updated_at
         ) VALUES ('overview-stale-check', ?, ?, 'overview-stale-definition',
            'overview-stale-check', 'Stale required check', 1, 'manual', 'pass',
            0, 1, NULL, ?, ?)",
    )
    .bind(&project.id)
    .bind(milestone_id)
    .bind(&now)
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("insert stale milestone check");
    sqlx::query("UPDATE project SET primary_milestone_id = ? WHERE id = ?")
        .bind(milestone_id)
        .bind(&project.id)
        .execute(harness.state.db.pool())
        .await
        .expect("set project primary milestone");

    let overview: ProjectOverview = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{}/overview", project.id),
        &token,
        StatusCode::OK,
    )
    .await;

    assert_eq!(overview.active_milestones.len(), 1);
    assert_eq!(overview.active_milestones[0].milestone.canonical_id, "M001");
    assert_eq!(
        overview.active_milestones[0].definition.content.outcome,
        "Ship the first bounded outcome."
    );
    assert_eq!(overview.primary_milestone_id.as_deref(), Some(milestone_id));
    assert_eq!(overview.check_summary.required_total, 1);
    assert_eq!(
        overview.active_milestones[0].check_summary.required_total, 1,
        "superseded definition checks are not current validation truth"
    );
    assert_eq!(overview.active_milestones[0].current_checks.len(), 1);
    assert_eq!(
        overview.active_milestones[0].current_checks[0].id,
        "overview-current-check"
    );
    assert_eq!(overview.active_milestones[0].current_checks[0].version, 3);
    let effective =
        services::load_effective_project_state(harness.state.db.as_ref(), &project.id, Some(32))
            .await
            .expect("effective Project state");
    assert_eq!(
        effective.validation_summary.total, 1,
        "Project Agent context excludes superseded definition checks"
    );
    assert_eq!(effective.active_milestones[0].acceptance_checks.len(), 1);
    assert_eq!(
        effective.active_milestones[0].acceptance_checks[0].id,
        "overview-current-check"
    );
    assert_eq!(
        effective.active_milestones[0].evidence_requirements.len(),
        1
    );
    assert_eq!(
        overview.projection_state,
        api_types::OverviewProjectionState::Stale
    );
}

async fn seed_charter_backed_compact_project(harness: &common::Harness, project_id: &str) {
    let now = db::now_rfc3339();
    let charter_content = json!({
        "identity": {
            "working_name": "Bounded product",
            "slug_proposal": "bounded-product",
            "one_line_vision": "Ship the bounded product.",
            "maturity": "mvp"
        },
        "problem_and_people": {
            "problem_or_opportunity": "The bounded product needs a first release."
        },
        "core_experience": {
            "primary_outcome": "A user can complete the bounded product workflow."
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
         ) VALUES ('overview-charter', 'test-user-id', ?, 'compact', 'mvp',
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
         ) VALUES ('overview-charter-r1', 'overview-charter', 1, 0, 'approved',
            'charter-test', 'v1',
            ?,
            '# Bounded product', 'initial', 'user', 'test-user-id', '[]',
            'charter-content-r1', 'charter-render-r1', ?)",
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
         ) VALUES ('overview-charter-approval-r1', 'project_creation',
            'overview-charter', 'overview-charter-r1', 'charter-content-r1',
            'charter-render-r1', 1, 'user', 'test-user-id', 'test',
            'project_charter.approval', ?, 'charter.approved',
            'project_charter.approval', 'consumed', ?, ?,
            'overview-approval-r1', 1, ?, ?, 'compact')",
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
         SET current_approved_revision_id = 'overview-charter-r1', version = 2,
             updated_at = ?
         WHERE id = 'overview-charter'",
    )
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("point charter at approved revision");
    sqlx::query(
        "UPDATE project
         SET current_charter_id = 'overview-charter',
             current_charter_revision_id = 'overview-charter-r1',
             current_charter_version = 1, charter_status = 'charter_backed',
             charter_setup_required = 0, version = version + 1
         WHERE id = ?",
    )
    .bind(project_id)
    .execute(harness.state.db.pool())
    .await
    .expect("attach charter to project");
}

async fn seed_compact_milestone_readiness_and_release(harness: &common::Harness, project_id: &str) {
    let now = db::now_rfc3339();
    sqlx::query(
        "INSERT INTO project_milestone (
            id, project_id, milestone_sequence, milestone_key, display_label,
            lifecycle, blocker_reason_json, stale_reason_json,
            reconciliation_reason_json, current_definition_revision_id, version,
            created_at, updated_at
         ) VALUES ('overview-milestone-valid', ?, 1, 'M001', 'Bounded product',
            'active', '[]', '[]', '[]', NULL, 1, ?, ?)",
    )
    .bind(project_id)
    .bind(&now)
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("insert valid overview milestone");
    sqlx::query(
        "INSERT INTO project_milestone_revision (
            id, milestone_id, revision, base_revision, lifecycle, display_label,
            outcome, included_scope_json, excluded_scope_json, charter_revision_id,
            document_revisions_json, task_selection_json, dependencies_json,
            risks_json, acceptance_checks_json, evidence_requirements_json,
            known_issues_json, change_summary, schema_version, render_version,
            rendered_view, content_digest, rendered_digest, author_type, author_id,
            source_refs_json, created_at
         ) VALUES ('overview-milestone-valid-r1', 'overview-milestone-valid', 1,
            0, 'approved', 'Bounded product', 'Ship the bounded product.',
            '[]', '[]', 'overview-charter-r1', '[]', '[]', '[]', '[]', '[]',
            '[]', '[]', 'initial', 'milestone-test', 'v1',
            '# Bounded product', 'milestone-content-r1', 'milestone-render-r1',
            'user', 'test-user-id', '[]', ?)",
    )
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("insert valid overview definition");
    sqlx::query("UPDATE project SET primary_milestone_id = ? WHERE id = ?")
        .bind("overview-milestone-valid")
        .bind(project_id)
        .execute(harness.state.db.pool())
        .await
        .expect("point project at valid milestone");
    sqlx::query(
        "UPDATE project_milestone
         SET current_definition_revision_id = 'overview-milestone-valid-r1'
         WHERE id = 'overview-milestone-valid'",
    )
    .execute(harness.state.db.pool())
    .await
    .expect("point overview milestone at its definition");
    let readiness_principal = api_types::PrincipalRef {
        kind: api_types::PrincipalKind::User,
        id: "test-user-id".to_owned(),
        display_name: None,
    };
    let readiness_authorization = api_types::AuthorizationProvenance {
        principal: readiness_principal.clone(),
        authorization_basis: "test".to_owned(),
        action: "project.milestone.readiness".to_owned(),
        event_id: "readiness.created".to_owned(),
        occurred_at: now.clone(),
    };
    let readiness = services::MilestoneRuntime::new(harness.state.db.clone())
        .evaluate(
            project_id,
            &readiness_principal,
            &readiness_authorization,
            "overview-milestone-valid",
            1,
            "overview-readiness-idem",
        )
        .await
        .expect("compute exact current overview readiness");
    assert_eq!(readiness.result, api_types::ReadinessResult::Ready);

    let releasing_principal = readiness_principal;
    let release_authorization = api_types::AuthorizationProvenance {
        principal: releasing_principal.clone(),
        authorization_basis: "test".to_owned(),
        action: "project.milestone.release".to_owned(),
        event_id: "release.created".to_owned(),
        occurred_at: now.clone(),
    };
    let mut release_snapshot = api_types::ReleaseSnapshot {
        schema_version: "forge.milestone-release/v1".to_owned(),
        project_id: project_id.to_owned(),
        milestone_id: "overview-milestone-valid".to_owned(),
        milestone_canonical_id: "M001".to_owned(),
        release_revision: 1,
        release_identity: "M001-r1".to_owned(),
        milestone_definition_revision_id: "overview-milestone-valid-r1".to_owned(),
        milestone_definition_digest: "milestone-content-r1".to_owned(),
        expected_milestone_version: readiness.expected_milestone_version,
        display_label: Some("Bounded product".to_owned()),
        summary: "First release".to_owned(),
        changelog: vec!["Bounded product shipped".to_owned()],
        known_issues: Vec::new(),
        readiness_snapshot_id: readiness.id.clone(),
        readiness_digest: readiness.readiness_digest.clone(),
        source_event_watermark: readiness.source_event_watermark.clone(),
        charter_revision: api_types::ArtifactRef {
            artifact_id: "overview-charter".to_owned(),
            revision_id: "overview-charter-r1".to_owned(),
            content_digest: "charter-content-r1".to_owned(),
            render_version: Some("v1".to_owned()),
            render_digest: Some("charter-render-r1".to_owned()),
        },
        document_revisions: Vec::new(),
        included_decisions: Vec::new(),
        included_tasks: Vec::new(),
        validation_results: Vec::new(),
        repository_references: Vec::new(),
        evidence_pins: Vec::new(),
        waived_check_ids: Vec::new(),
        released_by: releasing_principal,
        authorization: release_authorization,
        released_at: now.clone(),
        idempotency_key: "overview-release-idem".to_owned(),
        snapshot_digest: String::new(),
    };
    release_snapshot.snapshot_digest = services::release_snapshot_digest(&release_snapshot)
        .expect("compute overview release digest");
    sqlx::query(
        "INSERT INTO project_release (
            id, project_id, milestone_id, release_sequence, release_revision,
            release_identifier, milestone_revision_id, readiness_snapshot_id,
            readiness_digest, summary, changelog,
            known_issues_json, charter_revision_id, document_revisions_json,
            decision_ids_json, task_references_json, validation_references_json,
            git_references_json, evidence_references_json, waivers_json,
            releasing_principal_type, releasing_principal_id, authorization_basis,
            authorization_action, authorization_occurred_at, explicit_event,
            schema_version, snapshot_digest, idempotency_key,
            created_at
         ) VALUES ('overview-release-valid', ?, 'overview-milestone-valid', 1, 1,
            'M001-r1', 'overview-milestone-valid-r1', ?,
            ?, 'First release',
            '[\"Bounded product shipped\"]', '[]', 'overview-charter-r1', '[]',
            '[]', '[]', '[]', '[]', '[]', '[]', 'user', 'test-user-id',
            'test', 'project.milestone.release', ?, 'release.created',
            'forge.milestone-release/v1', ?,
            'overview-release-idem', ?)",
    )
    .bind(project_id)
    .bind(&readiness.id)
    .bind(&readiness.readiness_digest)
    .bind(&now)
    .bind(&release_snapshot.snapshot_digest)
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("insert valid overview release");
}

#[tokio::test]
async fn project_overview_projects_current_compact_readiness_and_release() {
    let workspace = common::TestDir::new("project-overview-current-release");
    let harness = common::test_app(workspace.path(), "project-overview-current-release").await;
    let token = common::test_jwt();
    let project: ProjectResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({"name": "Current release overview"}),
        StatusCode::OK,
    )
    .await;

    seed_charter_backed_compact_project(&harness, &project.id).await;
    seed_compact_milestone_readiness_and_release(&harness, &project.id).await;

    let overview: ProjectOverview = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{}/overview", project.id),
        &token,
        StatusCode::OK,
    )
    .await;

    assert_eq!(
        overview.charter_state,
        api_types::ProjectCharterState::Approved
    );
    assert_eq!(overview.vision, "Ship the bounded product.");
    assert!(overview
        .current_charter
        .as_ref()
        .and_then(|charter| charter.approved_at.as_ref())
        .is_some());
    assert_eq!(
        overview.projection_state,
        api_types::OverviewProjectionState::Current
    );
    assert_eq!(overview.active_milestones.len(), 1);
    assert_eq!(overview.active_milestones[0].milestone.canonical_id, "M001");
    overview.active_milestones[0]
        .latest_readiness
        .as_ref()
        .expect("current readiness");
    assert_eq!(overview.releases.len(), 1);
    assert_eq!(
        overview.releases[0].snapshot.changelog,
        vec!["Bounded product shipped".to_owned()]
    );

    let release: api_types::ProjectRelease = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/releases/overview-release-valid",
            project.id
        ),
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(release.id, "overview-release-valid");
}

#[tokio::test]
async fn project_overview_release_keeps_historic_charter_revision_after_pointer_advances() {
    let workspace = common::TestDir::new("project-overview-historic-release");
    let harness = common::test_app(workspace.path(), "project-overview-historic-release").await;
    let token = common::test_jwt();
    let project: ProjectResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({"name": "Historic release overview"}),
        StatusCode::OK,
    )
    .await;

    seed_charter_backed_compact_project(&harness, &project.id).await;
    seed_compact_milestone_readiness_and_release(&harness, &project.id).await;
    let now = db::now_rfc3339();
    let amended_charter_content = json!({
        "identity": {
            "working_name": "Bounded product",
            "slug_proposal": "bounded-product",
            "one_line_vision": "The revised product vision.",
            "maturity": "mvp"
        },
        "problem_and_people": {
            "problem_or_opportunity": "The bounded product needs a first release."
        },
        "core_experience": {
            "primary_outcome": "A user can complete the revised bounded product workflow."
        },
        "scope": {},
        "success": {},
        "constraints_and_risks": {},
        "knowledge_ledger": {}
    });
    sqlx::query(
        "INSERT INTO project_charter_revision (
            id, charter_id, revision, base_revision, base_revision_id, lifecycle, schema_version,
            render_version, content_json, rendered_view, change_summary,
            author_type, author_id, source_refs_json, content_digest,
            rendered_digest, created_at
         ) VALUES ('overview-charter-r2', 'overview-charter', 2, 1, 'overview-charter-r1', 'approved',
            'charter-test', 'v1',
            ?,
            '# Revised product', 'amended', 'user', 'test-user-id', '[]',
            'charter-content-r2', 'charter-render-r2', ?)",
    )
    .bind(amended_charter_content.to_string())
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("insert amended charter revision");
    sqlx::query(
        "INSERT INTO project_charter_approval (
            id, approval_type, charter_id, revision_id, content_digest,
            rendered_digest, expected_charter_version, approving_principal_type,
            approving_principal_id, authorization_basis, authorization_action,
            authorization_occurred_at, explicit_event, source_action, lifecycle,
            idempotency_key, version, created_at, updated_at, approved_project_mode
         ) VALUES ('overview-charter-approval-r2', 'charter_amendment',
            'overview-charter', 'overview-charter-r2', 'charter-content-r2',
            'charter-render-r2', 2, 'user', 'test-user-id', 'test',
            'project_charter.amendment.approve', ?, 'charter.amended', 'test',
            'active', 'overview-approval-r2', 1, ?, ?, 'compact')",
    )
    .bind(&now)
    .bind(&now)
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("insert amended charter approval");
    sqlx::query(
        "UPDATE project_charter
         SET current_approved_revision_id = 'overview-charter-r2', version = 3,
             updated_at = ?
         WHERE id = 'overview-charter'",
    )
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("advance charter pointer");
    sqlx::query(
        "UPDATE project
         SET current_charter_revision_id = 'overview-charter-r2',
             current_charter_version = 2, version = version + 1
         WHERE id = ?",
    )
    .bind(&project.id)
    .execute(harness.state.db.pool())
    .await
    .expect("advance project charter pointer");

    let overview: ProjectOverview = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{}/overview", project.id),
        &token,
        StatusCode::OK,
    )
    .await;

    assert_eq!(
        overview
            .current_charter
            .as_ref()
            .map(|charter| charter.id.as_str()),
        Some("overview-charter-r2")
    );
    assert_eq!(overview.releases.len(), 1);
    assert_eq!(
        overview.releases[0].snapshot.charter_revision.revision_id,
        "overview-charter-r1"
    );
    assert_eq!(
        overview.releases[0].snapshot.charter_revision.artifact_id,
        "overview-charter"
    );

    let historic_release: api_types::ProjectRelease = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/releases/overview-release-valid",
            project.id
        ),
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        historic_release.snapshot.charter_revision.revision_id,
        "overview-charter-r1"
    );
    assert_eq!(
        historic_release.snapshot.charter_revision.artifact_id,
        "overview-charter"
    );
}

fn jwt_for_user(user_id: &str, email: &str) -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time")
        .as_secs();
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &json!({
            "sub": user_id,
            "email": email,
            "is_admin": false,
            "iat": now,
            "exp": now + 900,
        }),
        &EncodingKey::from_secret(b"test-jwt-secret-for-development"),
    )
    .expect("encode test jwt")
}
