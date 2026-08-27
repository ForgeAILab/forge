#![allow(dead_code)]

//! Route-level characterization for atomic optional-baseline acceptance
//! command (D18, F13, tasks 8.3.1/8.3.2).
//!
//! F13: the web mapped any approval/activation 409/412 to a stale-baseline
//! failure message even when the baseline path had already committed and
//! dispatched the Task. These tests drive the real HTTP route -- not the
//! service directly -- to prove the *response* is commit-truthful: a lost
//! response replays as success, a conflict caused by the exact requested
//! revision already being active renders success instead of a reported
//! failure, and a post-commit projection failure never downgrades a
//! successful commit into an error.

mod common;

use api_types::{
    AcceptanceEvidenceRequirement, AdaptiveEnvelope, AdaptiveTaskOperation,
    ApproveAndActivateExecutionBaselineResponse, ArtifactRef, ExecutionBaselineContent,
    ExecutionBaselineLifecycle, ExecutionBaselineReleasePolicy, ExecutionBaselineResponse,
};
use axum::http::{Method, StatusCode};
use chrono::Utc;
use db::SqliteDb;
use serde_json::{json, Value};

const USER_ID: &str = "test-user-id";
const PROJECT_ID: &str = "baseline-route-project";
const CHARTER_ID: &str = "baseline-route-charter";
const CHARTER_REVISION_ID: &str = "baseline-route-charter-revision";
const MILESTONE_ID: &str = "baseline-route-milestone";
const MILESTONE_REVISION_ID: &str = "baseline-route-milestone-revision";
const NOW: &str = "2026-08-25T00:00:00.000Z";

/// Seed a charter-backed Project with one approved milestone definition
/// directly through SQL, the same shape
/// `crates/services/tests/execution_baseline_command.rs` uses. Going through
/// the full Genesis/Charter-approval HTTP flow is not the thing under test
/// here; the route contract for the atomic approve-and-activate command is.
async fn seed_charter_backed_project(db: &SqliteDb) {
    db::ProjectRepo::create(
        db,
        db::CreateProject {
            id: PROJECT_ID.to_owned(),
            name: "Baseline route project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(USER_ID.to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("project");
    sqlx::query(
        "INSERT INTO project_charter
         (id, account_id, project_id, project_mode, maturity, lifecycle,
          created_at, updated_at)
         VALUES (?, ?, ?, 'standard', 'mvp', 'attached', ?, ?)",
    )
    .bind(CHARTER_ID)
    .bind(USER_ID)
    .bind(PROJECT_ID)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("charter");
    sqlx::query(
        "INSERT INTO project_charter_revision
         (id, charter_id, revision, lifecycle, schema_version, render_version,
          content_json, rendered_view, author_type, author_id, content_digest,
          rendered_digest, created_at)
         VALUES (?, ?, 1, 'approved', 'charter@1', 'render@1', '{}',
                 '# Charter', 'user', ?, 'charter-content',
                 'charter-rendered', ?)",
    )
    .bind(CHARTER_REVISION_ID)
    .bind(CHARTER_ID)
    .bind(USER_ID)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("charter revision");
    sqlx::query("UPDATE project_charter SET current_approved_revision_id = ? WHERE id = ?")
        .bind(CHARTER_REVISION_ID)
        .bind(CHARTER_ID)
        .execute(db.pool())
        .await
        .expect("charter pointer");
    sqlx::query(
        "UPDATE project
         SET charter_status = 'charter_backed', charter_setup_required = 0,
             current_charter_id = ?, current_charter_revision_id = ?,
             current_charter_version = 1
         WHERE id = ?",
    )
    .bind(CHARTER_ID)
    .bind(CHARTER_REVISION_ID)
    .bind(PROJECT_ID)
    .execute(db.pool())
    .await
    .expect("charter project pointer");
    sqlx::query(
        "INSERT INTO project_milestone
         (id, project_id, milestone_sequence, milestone_key, display_label,
          lifecycle, blocker_reason_json, stale_reason_json,
          reconciliation_reason_json, version, created_at, updated_at)
         VALUES (?, ?, 1, 'M001', 'First milestone', 'planned', '[]', '[]',
                 '[]', 1, ?, ?)",
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
          acceptance_checks_json, evidence_requirements_json,
          known_issues_json, change_summary, schema_version, render_version,
          rendered_view, content_digest, rendered_digest, author_type,
          author_id, source_refs_json, created_at)
         VALUES (?, ?, 1, 0, NULL, 'approved', 'First milestone',
                 'Outcome', '[]', '[]', ?, '[]', '[]', '[]', '[]', ?,
                 ?, '[]', 'Initial definition', 'milestone@1',
                 'milestone-render@1', '# Milestone', 'milestone-content',
                 'milestone-rendered', 'user', ?, '[]', ?)",
    )
    .bind(MILESTONE_REVISION_ID)
    .bind(MILESTONE_ID)
    .bind(CHARTER_REVISION_ID)
    .bind(
        serde_json::json!([{
            "id": "check-1",
            "description": "Verify the first milestone",
            "required": true,
            "source_kind": "manual",
            "expected_result": "pass"
        }])
        .to_string(),
    )
    .bind(
        serde_json::json!([{
            "id": "check-1",
            "description": "Evidence for the first milestone",
            "required": true,
            "evidence_kind": "report",
            "check_definition_revision": MILESTONE_REVISION_ID
        }])
        .to_string(),
    )
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

fn release_policy() -> ExecutionBaselineReleasePolicy {
    ExecutionBaselineReleasePolicy {
        schema_version: services::EXECUTION_BASELINE_RELEASE_POLICY_SCHEMA.to_owned(),
        revision: "policy-1".to_owned(),
        required_check_definition_revisions: vec![MILESTONE_REVISION_ID.to_owned()],
        reviewer_independence_rules: vec!["independent-reviewer".to_owned()],
        manual_attestation_rules: vec!["manual-attestation".to_owned()],
        waiver_rules: vec!["user-waiver".to_owned()],
        evidence_kinds: vec!["test-report".to_owned()],
        evidence_contexts: vec!["repository".to_owned()],
        evidence_freshness_rules: vec!["current-commit".to_owned()],
        dependency_rules: vec!["dependencies-green".to_owned()],
        stale_input_rules: vec!["stale-baseline-blocks".to_owned()],
        forbidden_side_effects: vec!["publish".to_owned()],
        known_issue_rules: vec!["record-known-issue".to_owned()],
        correction_rules: vec!["correct-before-release".to_owned()],
        purge_rules: vec!["purge-invalid-evidence".to_owned()],
    }
}

fn content(complete: bool) -> ExecutionBaselineContent {
    let policy = release_policy();
    ExecutionBaselineContent {
        charter_revision: ArtifactRef {
            artifact_id: CHARTER_ID.to_owned(),
            revision_id: CHARTER_REVISION_ID.to_owned(),
            content_digest: "charter-content".to_owned(),
            render_version: Some("render@1".to_owned()),
            render_digest: Some("charter-rendered".to_owned()),
        },
        document_revisions: Vec::new(),
        plan_item_ids: if complete {
            vec!["plan-1".to_owned()]
        } else {
            Vec::new()
        },
        milestone_ids: if complete {
            vec![MILESTONE_ID.to_owned()]
        } else {
            Vec::new()
        },
        milestone_definition_revision_ids: if complete {
            vec![MILESTONE_REVISION_ID.to_owned()]
        } else {
            Vec::new()
        },
        primary_milestone_id: if complete {
            Some(MILESTONE_ID.to_owned())
        } else {
            None
        },
        release_policy_revision: if complete {
            "policy-1".to_owned()
        } else {
            String::new()
        },
        release_policy_digest: if complete {
            services::release_policy_digest(&policy).expect("policy digest")
        } else {
            String::new()
        },
        release_policy: if complete {
            policy
        } else {
            ExecutionBaselineReleasePolicy::default()
        },
        acceptance_evidence_matrix: if complete {
            vec![AcceptanceEvidenceRequirement {
                id: "check-1".to_owned(),
                description: "Verify the first milestone".to_owned(),
                required: true,
                evidence_kind: Some("report".to_owned()),
                check_definition_revision: Some(MILESTONE_REVISION_ID.to_owned()),
            }]
        } else {
            Vec::new()
        },
        capability_classes: if complete {
            vec!["repository_write".to_owned()]
        } else {
            Vec::new()
        },
        risk_classes: if complete {
            vec!["low".to_owned()]
        } else {
            Vec::new()
        },
        reviewer_independence_rules: Vec::new(),
        elevated_operations: Vec::new(),
        adaptive_envelope: AdaptiveEnvelope {
            allowed_task_operations: vec![AdaptiveTaskOperation::Split],
            fixed_outcomes: Vec::new(),
            fixed_acceptance: Vec::new(),
            fixed_risk_classes: vec!["low".to_owned()],
            forbidden_side_effects: Vec::new(),
            elevated_operations: Vec::new(),
        },
        rollback_and_recovery: Vec::new(),
        exclusions: Vec::new(),
    }
}

fn user_authorization(action: &str, event_id: &str) -> Value {
    json!({
        "principal": {"kind": "user", "id": USER_ID},
        "authorization_basis": "explicit_user_authorization",
        "action": action,
        "event_id": event_id,
        "occurred_at": Utc::now().to_rfc3339(),
    })
}

fn user_provenance(summary: &str) -> Value {
    json!({
        "author": {"kind": "user", "id": USER_ID},
        "source_refs": [],
        "change_summary": summary,
    })
}

/// Save a first draft and immediately propose it, returning
/// `(baseline_id, revision_id, baseline_version, content_digest, render_digest)`
/// for the exact `proposed` revision that `approve-and-activate` targets.
async fn propose_fresh_baseline(
    harness: &common::Harness,
    key_prefix: &str,
) -> (String, String, i64, String, String) {
    let draft_content = content(false);
    let draft_rendered = services::render_execution_baseline(&draft_content).expect("draft render");
    let draft: ExecutionBaselineResponse = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{PROJECT_ID}/execution-baseline"),
        json!({
            "mutation": {
                "expected_version": 0,
                "idempotency_key": format!("{key_prefix}-draft"),
                "authorization": user_authorization(
                    "project.execution_baseline.save_draft",
                    &format!("{key_prefix}-draft-event"),
                ),
            },
            "operation": "save_draft",
            "base_revision_id": null,
            "content": draft_content,
            "rendered_view": draft_rendered.rendered_view,
            "render_version": services::EXECUTION_BASELINE_RENDER_VERSION,
            "content_digest": draft_rendered.content_digest,
            "render_digest": draft_rendered.render_digest,
            "provenance": user_provenance("draft"),
        }),
        StatusCode::CREATED,
    )
    .await;
    let baseline_id = draft.baseline.id.clone();
    let baseline_version = draft.baseline.version;
    let draft_revision_id = draft.current_revision.expect("draft revision").id;

    let proposal_content = content(true);
    let proposal_rendered =
        services::render_execution_baseline(&proposal_content).expect("proposal render");
    let proposed: ExecutionBaselineResponse = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{PROJECT_ID}/execution-baseline/{baseline_id}/revisions"),
        json!({
            "mutation": {
                "expected_version": baseline_version,
                "idempotency_key": format!("{key_prefix}-propose"),
                "authorization": user_authorization(
                    "project.execution_baseline.propose_for_approval",
                    &format!("{key_prefix}-propose-event"),
                ),
            },
            "operation": "propose_for_approval",
            "base_revision_id": draft_revision_id,
            "content": proposal_content,
            "rendered_view": proposal_rendered.rendered_view,
            "render_version": services::EXECUTION_BASELINE_RENDER_VERSION,
            "content_digest": proposal_rendered.content_digest,
            "render_digest": proposal_rendered.render_digest,
            "provenance": user_provenance("proposal"),
        }),
        StatusCode::CREATED,
    )
    .await;
    let revision_id = proposed
        .approval_target
        .expect("approval target")
        .revision_id;
    let revised_baseline_version = proposed.baseline.version;
    (
        baseline_id,
        revision_id,
        revised_baseline_version,
        proposal_rendered.content_digest,
        proposal_rendered.render_digest,
    )
}

async fn project_version(harness: &common::Harness) -> i64 {
    sqlx::query_scalar("SELECT version FROM project WHERE id = ?")
        .bind(PROJECT_ID)
        .fetch_one(harness.state.db.pool())
        .await
        .expect("project version")
}

fn approve_and_activate_url(baseline_id: &str, revision_id: &str) -> String {
    format!(
        "/api/v1/projects/{PROJECT_ID}/execution-baseline/{baseline_id}/revisions/{revision_id}/approve-and-activate"
    )
}

fn approve_and_activate_body(
    revision_id: &str,
    baseline_version: i64,
    project_version: i64,
    content_digest: &str,
    render_digest: &str,
    idempotency_key: &str,
) -> Value {
    json!({
        "mutation": {
            "expected_version": project_version,
            "idempotency_key": idempotency_key,
            "authorization": user_authorization(
                "project.execution_baseline.approve_and_activate",
                &format!("{idempotency_key}-event"),
            ),
        },
        "revision_id": revision_id,
        "content_digest": content_digest,
        "render_digest": render_digest,
        "expected_baseline_version": baseline_version,
    })
}

#[tokio::test]
async fn approve_and_activate_commits_and_replays_exact_on_lost_response() {
    let workspace = common::TestDir::new("baseline-approve-and-activate-replay");
    let harness = common::test_app(workspace.path(), "baseline-approve-and-activate-replay").await;
    seed_charter_backed_project(&harness.state.db).await;
    let (baseline_id, revision_id, baseline_version, content_digest, render_digest) =
        propose_fresh_baseline(&harness, "replay").await;
    let version = project_version(&harness).await;
    let body = approve_and_activate_body(
        &revision_id,
        baseline_version,
        version,
        &content_digest,
        &render_digest,
        "replay-approve-and-activate",
    );

    let first: ApproveAndActivateExecutionBaselineResponse = common::json_request(
        &harness.app,
        Method::POST,
        &approve_and_activate_url(&baseline_id, &revision_id),
        body.clone(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(first.baseline_id, baseline_id);
    assert_eq!(first.revision_id, revision_id);
    assert!(!first.approval_id.is_empty());
    assert!(!first.refresh_required);
    let projection = first.projection.clone().expect("projection present");
    assert_eq!(
        projection.baseline.lifecycle,
        ExecutionBaselineLifecycle::Active
    );
    assert_eq!(
        projection
            .current_revision
            .as_ref()
            .map(|revision| revision.id.clone()),
        Some(revision_id.clone())
    );

    // The client never saw the response (or the same click is retried) and
    // resubmits the exact same command under the exact same idempotency key.
    // This must replay the frozen committed success, not surface a failure
    // (F13's core defect).
    let replay: ApproveAndActivateExecutionBaselineResponse = common::json_request(
        &harness.app,
        Method::POST,
        &approve_and_activate_url(&baseline_id, &revision_id),
        body,
        StatusCode::OK,
    )
    .await;
    assert_eq!(replay, first);

    let approval_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_execution_baseline_approval WHERE baseline_id = ?",
    )
    .bind(&baseline_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("approval count");
    assert_eq!(
        approval_count, 1,
        "replay must not create a second approval"
    );
}

#[tokio::test]
async fn approve_and_activate_renders_success_when_the_exact_revision_is_already_active() {
    let workspace = common::TestDir::new("baseline-approve-and-activate-already-active");
    let harness = common::test_app(
        workspace.path(),
        "baseline-approve-and-activate-already-active",
    )
    .await;
    seed_charter_backed_project(&harness.state.db).await;
    let (baseline_id, revision_id, baseline_version, content_digest, render_digest) =
        propose_fresh_baseline(&harness, "double-submit").await;
    let version = project_version(&harness).await;

    let first: ApproveAndActivateExecutionBaselineResponse = common::json_request(
        &harness.app,
        Method::POST,
        &approve_and_activate_url(&baseline_id, &revision_id),
        approve_and_activate_body(
            &revision_id,
            baseline_version,
            version,
            &content_digest,
            &render_digest,
            "double-submit-first",
        ),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        first.projection.expect("projection").baseline.lifecycle,
        ExecutionBaselineLifecycle::Active
    );

    // A second click (or a UI bug that does not reuse the idempotency key)
    // resubmits the exact same target with a *different* idempotency key and
    // the stale versions it originally observed. The command itself now
    // fails closed (the baseline is no longer `proposed`), but because the
    // exact requested revision is already the Project's active baseline with
    // matching digests, the route must render success rather than the
    // stale-baseline failure F13 reported.
    let second: ApproveAndActivateExecutionBaselineResponse = common::json_request(
        &harness.app,
        Method::POST,
        &approve_and_activate_url(&baseline_id, &revision_id),
        approve_and_activate_body(
            &revision_id,
            baseline_version,
            version,
            &content_digest,
            &render_digest,
            "double-submit-second",
        ),
        StatusCode::OK,
    )
    .await;
    assert_eq!(second.baseline_id, baseline_id);
    assert_eq!(second.revision_id, revision_id);
    assert_eq!(second.approval_id, first.approval_id);
    assert!(!second.refresh_required);
    assert_eq!(
        second.projection.expect("projection").baseline.lifecycle,
        ExecutionBaselineLifecycle::Active
    );

    let approval_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_execution_baseline_approval WHERE baseline_id = ?",
    )
    .bind(&baseline_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("approval count");
    assert_eq!(
        approval_count, 1,
        "the already-active shortcut must not mint a second approval"
    );
}

#[tokio::test]
async fn approve_and_activate_returns_receipt_outcome_when_projection_assembly_fails_after_commit()
{
    let workspace = common::TestDir::new("baseline-approve-and-activate-refresh");
    let harness = common::test_app(workspace.path(), "baseline-approve-and-activate-refresh").await;
    seed_charter_backed_project(&harness.state.db).await;
    let (baseline_id, revision_id, baseline_version, content_digest, render_digest) =
        propose_fresh_baseline(&harness, "refresh").await;
    let version = project_version(&harness).await;
    let body = approve_and_activate_body(
        &revision_id,
        baseline_version,
        version,
        &content_digest,
        &render_digest,
        "refresh-approve-and-activate",
    );

    let first: ApproveAndActivateExecutionBaselineResponse = common::json_request(
        &harness.app,
        Method::POST,
        &approve_and_activate_url(&baseline_id, &revision_id),
        body.clone(),
        StatusCode::OK,
    )
    .await;
    assert!(!first.refresh_required);
    assert!(first.projection.is_some());

    // Corrupt the persisted revision's rendered view *after* the command
    // already committed. This models a post-commit read failure: the domain
    // effect is durable and correct, but reconstructing the full projection
    // now fails its digest-reproduction check. The revision table's own
    // immutability trigger has to be lifted first -- proof this is corruption
    // happening strictly after the real commit, not a value the command path
    // itself could ever have written.
    sqlx::query("DROP TRIGGER project_execution_baseline_revision_immutable_update")
        .execute(harness.state.db.pool())
        .await
        .expect("lift revision immutability guard");
    sqlx::query(
        "UPDATE project_execution_baseline_revision SET rendered_view = 'corrupted' WHERE id = ?",
    )
    .bind(&revision_id)
    .execute(harness.state.db.pool())
    .await
    .expect("corrupt persisted revision");

    // Replaying the exact same committed command must still report the
    // identity of the committed outcome -- never a command failure -- with a
    // bounded refresh signal instead of a full projection (D18/8.3.2).
    let replay: ApproveAndActivateExecutionBaselineResponse = common::json_request(
        &harness.app,
        Method::POST,
        &approve_and_activate_url(&baseline_id, &revision_id),
        body,
        StatusCode::OK,
    )
    .await;
    assert_eq!(replay.baseline_id, baseline_id);
    assert_eq!(replay.revision_id, revision_id);
    assert_eq!(replay.approval_id, first.approval_id);
    assert_eq!(replay.content_digest, content_digest);
    assert_eq!(replay.render_digest, render_digest);
    assert!(
        replay.projection.is_none(),
        "a corrupted projection read must not be silently substituted"
    );
    assert!(
        replay.refresh_required,
        "a post-commit projection failure must ask the caller to refresh, not report failure"
    );
}

#[tokio::test]
async fn approve_and_activate_rejects_a_genuine_project_version_race() {
    let workspace = common::TestDir::new("baseline-approve-and-activate-version-race");
    let harness = common::test_app(
        workspace.path(),
        "baseline-approve-and-activate-version-race",
    )
    .await;
    seed_charter_backed_project(&harness.state.db).await;
    let (baseline_id, revision_id, baseline_version, content_digest, render_digest) =
        propose_fresh_baseline(&harness, "race").await;
    let version = project_version(&harness).await;

    // A stale Project version that was never actually committed: the exact
    // requested revision is not (and never becomes) active, so this must
    // fail closed as a real conflict rather than being swallowed by the
    // already-active shortcut.
    let response = common::raw_json_request(
        &harness.app,
        Method::POST,
        &approve_and_activate_url(&baseline_id, &revision_id),
        approve_and_activate_body(
            &revision_id,
            baseline_version,
            version + 1,
            &content_digest,
            &render_digest,
            "race-approve-and-activate",
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let projection: ExecutionBaselineResponse = common::json_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{PROJECT_ID}/execution-baseline"),
        Value::Null,
        StatusCode::OK,
    )
    .await;
    assert_ne!(
        projection.baseline.lifecycle,
        ExecutionBaselineLifecycle::Active
    );
}
