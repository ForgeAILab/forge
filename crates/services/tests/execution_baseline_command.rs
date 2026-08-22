//! Focused acceptance coverage for the transport-neutral execution-baseline
//! command service.  The REST/native adapters are intentionally not used:
//! task 2.8 performs that rewiring after this boundary is authoritative.

use std::sync::Arc;

use api_types::{
    AdaptiveEnvelope, ArtifactRef, ExecutionBaselineContent, ExecutionBaselineReleasePolicy,
    PrincipalKind, PrincipalRef, RevisionProvenance,
};
use db::{create_sqlite_pool, run_migrations, ProjectRepo, SqliteDb};
use serde_json::Value;
use services::{
    release_policy_digest, render_execution_baseline, ActivateExecutionBaselineCommand,
    ApproveExecutionBaselineCommand, ExecutionBaselineCommandService, ProjectCommandAuthorization,
    ProposeExecutionBaselineForApprovalCommand, SaveExecutionBaselineDraftCommand,
    EXECUTION_BASELINE_ACTIVATE_COMMAND, EXECUTION_BASELINE_APPROVE_COMMAND,
    EXECUTION_BASELINE_PROPOSE_COMMAND, EXECUTION_BASELINE_RELEASE_POLICY_SCHEMA,
    EXECUTION_BASELINE_SAVE_DRAFT_COMMAND,
};

const USER_ID: &str = "baseline-command-user";
const PROJECT_ID: &str = "baseline-command-project";
const CHARTER_ID: &str = "baseline-command-charter";
const CHARTER_REVISION_ID: &str = "baseline-command-charter-revision";
const MILESTONE_ID: &str = "baseline-command-milestone";
const MILESTONE_REVISION_ID: &str = "baseline-command-milestone-revision";
const NOW: &str = "2026-08-20T00:00:00.000Z";

type DomainEventRow = (
    String,
    String,
    String,
    String,
    Option<String>,
    String,
    String,
    String,
    Option<String>,
    String,
);

async fn fixture() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    let db = Arc::new(SqliteDb::new(pool));
    sqlx::query(
        "INSERT INTO user (id, email, password_hash, created_at, updated_at)
         VALUES (?, ?, 'test', ?, ?)",
    )
    .bind(USER_ID)
    .bind("baseline-command@example.test")
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("user");
    ProjectRepo::create(
        &*db,
        db::CreateProject {
            id: PROJECT_ID.to_owned(),
            name: "Baseline command project".to_owned(),
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
                 'Outcome', '[]', '[]', ?, '[]', '[]', '[]', '[]', '[]',
                 '[]', '[]', 'Initial definition', 'milestone@1',
                 'milestone-render@1', '# Milestone', 'milestone-content',
                 'milestone-rendered', 'user', ?, '[]', ?)",
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
    db
}

fn authorization(operation: &str, key: &str) -> ProjectCommandAuthorization {
    ProjectCommandAuthorization {
        principal_type: "user".to_owned(),
        principal_id: USER_ID.to_owned(),
        policy_result: "allowed".to_owned(),
        policy_revision: Some("policy@1".to_owned()),
        policy_digest: Some("policy-digest".to_owned()),
        requested_permission: Some(operation.to_owned()),
        correlation_id: format!("correlation-{key}"),
        causation_id: None,
        causation_depth: 0,
        authorization_event_id: format!("authorization-{key}"),
        authorization_basis: "explicit user authorization".to_owned(),
        authorization_action: operation.to_owned(),
        authorization_occurred_at: db::now_rfc3339(),
        authorization_json: serde_json::json!({"operation": operation, "key": key}).to_string(),
    }
}

fn provenance() -> RevisionProvenance {
    RevisionProvenance {
        author: PrincipalRef {
            kind: PrincipalKind::User,
            id: USER_ID.to_owned(),
            display_name: Some("Baseline command user".to_owned()),
        },
        profile_revision: None,
        operating_skill_revision: None,
        source_refs: Vec::new(),
        change_summary: "baseline command test".to_owned(),
        material_diff: None,
    }
}

fn release_policy() -> ExecutionBaselineReleasePolicy {
    ExecutionBaselineReleasePolicy {
        schema_version: EXECUTION_BASELINE_RELEASE_POLICY_SCHEMA.to_owned(),
        revision: "policy-1".to_owned(),
        required_check_definition_revisions: vec!["check-1".to_owned()],
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
            release_policy_digest(&policy).expect("policy digest")
        } else {
            String::new()
        },
        release_policy: if complete {
            policy
        } else {
            ExecutionBaselineReleasePolicy::default()
        },
        acceptance_evidence_matrix: Vec::new(),
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
            allowed_task_operations: vec!["split".to_owned()],
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

#[tokio::test]
async fn draft_is_incomplete_and_replay_exact_after_response_loss() {
    let db = fixture().await;
    let service = ExecutionBaselineCommandService::new(db.clone());
    let draft_content = content(false);
    let rendered = render_execution_baseline(&draft_content).expect("render");
    let command = SaveExecutionBaselineDraftCommand {
        project_id: PROJECT_ID.to_owned(),
        baseline_id: None,
        base_revision_id: None,
        expected_baseline_version: None,
        content: draft_content,
        rendered_view: rendered.rendered_view,
        render_version: services::EXECUTION_BASELINE_RENDER_VERSION.to_owned(),
        content_digest: rendered.content_digest,
        render_digest: rendered.render_digest,
        provenance: provenance(),
        idempotency_key: "draft-replay".to_owned(),
        authorization: authorization(EXECUTION_BASELINE_SAVE_DRAFT_COMMAND, "draft-replay"),
        action: None,
    };
    let first = service
        .save_draft(command.clone())
        .await
        .expect("draft saves");
    assert_eq!(first.lifecycle, "draft");
    assert!(!first.requires_user_authorization);
    assert!(first.approval_target.is_none());
    let revision_id = first.revision_id.clone().expect("draft revision");
    let (baseline_lifecycle, current_revision_id): (String, Option<String>) = sqlx::query_as(
        "SELECT lifecycle, current_revision_id
         FROM project_execution_baseline WHERE id = ?",
    )
    .bind(&first.baseline_id)
    .fetch_one(db.pool())
    .await
    .expect("draft baseline state");
    assert_eq!(baseline_lifecycle, "draft");
    assert_eq!(current_revision_id.as_deref(), Some(revision_id.as_str()));
    let (revision_lifecycle, revision_number): (String, i64) = sqlx::query_as(
        "SELECT lifecycle, revision
         FROM project_execution_baseline_revision WHERE id = ?",
    )
    .bind(&revision_id)
    .fetch_one(db.pool())
    .await
    .expect("draft revision state");
    assert_eq!(revision_lifecycle, "draft");
    assert_eq!(revision_number, 1);

    let receipt_row: (
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
    ) = sqlx::query_as(
        "SELECT r.id, r.principal_type, r.principal_id, r.scope_type,
                r.scope_id, r.operation, r.idempotency_key, r.input_digest,
                r.event_id, r.outcome_json
         FROM command_receipt r
         WHERE r.operation = ? AND r.idempotency_key = ?",
    )
    .bind(EXECUTION_BASELINE_SAVE_DRAFT_COMMAND)
    .bind("draft-replay")
    .fetch_one(db.pool())
    .await
    .expect("draft command receipt");
    assert_eq!(receipt_row.0, first.receipt_id.clone().expect("receipt id"));
    assert_eq!(receipt_row.1, "user");
    assert_eq!(receipt_row.2, USER_ID);
    assert_eq!(receipt_row.3, "project");
    assert_eq!(receipt_row.4, PROJECT_ID);
    assert_eq!(receipt_row.5, EXECUTION_BASELINE_SAVE_DRAFT_COMMAND);
    assert_eq!(receipt_row.6, "draft-replay");
    assert!(!receipt_row.7.trim().is_empty());
    let receipt_outcome: Value =
        serde_json::from_str(&receipt_row.9).expect("receipt outcome JSON");
    assert_eq!(receipt_outcome["operation"], "save_draft");
    assert_eq!(receipt_outcome["project_id"], PROJECT_ID);
    assert_eq!(receipt_outcome["baseline_id"], first.baseline_id);
    assert_eq!(receipt_outcome["revision_id"], revision_id);
    assert_eq!(receipt_outcome["revision"], 1);
    assert_eq!(receipt_outcome["lifecycle"], "draft");
    assert_eq!(receipt_outcome["baseline_version"], first.baseline_version);
    assert_eq!(receipt_outcome["receipt_id"], receipt_row.0);
    assert_eq!(receipt_outcome["domain_committed"], true);

    let event_row: DomainEventRow = sqlx::query_as::<_, DomainEventRow>(
        "SELECT e.id, e.event_type, e.entity_type, e.entity_id,
                e.actor_id, e.scope_type, e.scope_id, e.correlation_id,
                e.dedupe_key, e.payload_json
         FROM domain_event e WHERE e.id = ?",
    )
    .bind(&receipt_row.8)
    .fetch_one(db.pool())
    .await
    .expect("draft event");
    assert_eq!(event_row.0, receipt_row.8);
    assert_eq!(event_row.1, "project.execution_baseline.draft_saved");
    assert_eq!(event_row.2, "execution_baseline");
    assert_eq!(event_row.3, first.baseline_id);
    assert_eq!(event_row.4.as_deref(), Some(USER_ID));
    assert_eq!(event_row.5, "project");
    assert_eq!(event_row.6, PROJECT_ID);
    assert_eq!(event_row.7, "correlation-draft-replay");
    assert_eq!(
        event_row.8.as_deref(),
        Some("execution-baseline:project.execution_baseline.save_draft:draft-replay")
    );
    let event_payload: Value =
        serde_json::from_str(&event_row.9).expect("draft event payload JSON");
    assert_eq!(event_payload["operation"], "save_draft");
    assert_eq!(event_payload["result"], receipt_outcome);

    let before_replay: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
             (SELECT COUNT(*) FROM project_execution_baseline WHERE project_id = ?),
             (SELECT COUNT(*) FROM project_execution_baseline_revision r
              JOIN project_execution_baseline b ON b.id = r.baseline_id
              WHERE b.project_id = ?),
             (SELECT COUNT(*) FROM command_receipt WHERE scope_type = 'project' AND scope_id = ?),
             (SELECT COUNT(*) FROM domain_event WHERE scope_type = 'project' AND scope_id = ?)",
    )
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("draft counts before replay");
    let replay = service.save_draft(command).await.expect("draft replays");
    assert_eq!(first, replay);
    let after_replay: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
             (SELECT COUNT(*) FROM project_execution_baseline WHERE project_id = ?),
             (SELECT COUNT(*) FROM project_execution_baseline_revision r
              JOIN project_execution_baseline b ON b.id = r.baseline_id
              WHERE b.project_id = ?),
             (SELECT COUNT(*) FROM command_receipt WHERE scope_type = 'project' AND scope_id = ?),
             (SELECT COUNT(*) FROM domain_event WHERE scope_type = 'project' AND scope_id = ?)",
    )
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("draft counts after replay");
    assert_eq!(after_replay, before_replay);
}

#[tokio::test]
async fn incomplete_candidate_is_rejected_without_a_proposed_revision() {
    let db = fixture().await;
    let service = ExecutionBaselineCommandService::new(db.clone());
    let draft_content = content(false);
    let draft_rendered = render_execution_baseline(&draft_content).expect("draft render");
    let draft = service
        .save_draft(SaveExecutionBaselineDraftCommand {
            project_id: PROJECT_ID.to_owned(),
            baseline_id: None,
            base_revision_id: None,
            expected_baseline_version: None,
            content: draft_content.clone(),
            rendered_view: draft_rendered.rendered_view,
            render_version: services::EXECUTION_BASELINE_RENDER_VERSION.to_owned(),
            content_digest: draft_rendered.content_digest,
            render_digest: draft_rendered.render_digest,
            provenance: provenance(),
            idempotency_key: "incomplete-draft".to_owned(),
            authorization: authorization(EXECUTION_BASELINE_SAVE_DRAFT_COMMAND, "incomplete-draft"),
            action: None,
        })
        .await
        .expect("draft");
    let baseline_before: (String, i64, Option<String>) = sqlx::query_as(
        "SELECT lifecycle, version, current_revision_id
         FROM project_execution_baseline WHERE id = ?",
    )
    .bind(&draft.baseline_id)
    .fetch_one(db.pool())
    .await
    .expect("baseline before incomplete proposal");
    let revisions_before: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_execution_baseline_revision WHERE baseline_id = ?",
    )
    .bind(&draft.baseline_id)
    .fetch_one(db.pool())
    .await
    .expect("revision count before incomplete proposal");

    let rendered = render_execution_baseline(&draft_content).expect("incomplete render");
    let error = service
        .propose_for_approval(ProposeExecutionBaselineForApprovalCommand {
            project_id: PROJECT_ID.to_owned(),
            baseline_id: draft.baseline_id.clone(),
            base_revision_id: draft.revision_id.clone(),
            expected_baseline_version: draft.baseline_version,
            content: draft_content,
            rendered_view: rendered.rendered_view,
            render_version: services::EXECUTION_BASELINE_RENDER_VERSION.to_owned(),
            content_digest: rendered.content_digest,
            render_digest: rendered.render_digest,
            provenance: provenance(),
            idempotency_key: "incomplete-proposal".to_owned(),
            authorization: authorization(EXECUTION_BASELINE_PROPOSE_COMMAND, "incomplete-proposal"),
            action: None,
        })
        .await
        .expect_err("approval-incomplete candidate must be rejected");
    assert!(matches!(
        error,
        services::ServiceError::InvalidOperation { .. } | services::ServiceError::Conflict(_)
    ));

    let baseline_after: (String, i64, Option<String>) = sqlx::query_as(
        "SELECT lifecycle, version, current_revision_id
         FROM project_execution_baseline WHERE id = ?",
    )
    .bind(&draft.baseline_id)
    .fetch_one(db.pool())
    .await
    .expect("baseline after incomplete proposal");
    assert_eq!(baseline_after, baseline_before);
    let revisions_after: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_execution_baseline_revision WHERE baseline_id = ?",
    )
    .bind(&draft.baseline_id)
    .fetch_one(db.pool())
    .await
    .expect("revision count after incomplete proposal");
    assert_eq!(revisions_after, revisions_before);
    let proposed_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_execution_baseline_revision
         WHERE baseline_id = ? AND lifecycle = 'proposed'",
    )
    .bind(&draft.baseline_id)
    .fetch_one(db.pool())
    .await
    .expect("proposed revision count");
    assert_eq!(proposed_count, 0);
    let proposal_receipts: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM command_receipt
         WHERE scope_type = 'project' AND scope_id = ?
           AND operation = ? AND idempotency_key = ?",
    )
    .bind(PROJECT_ID)
    .bind(EXECUTION_BASELINE_PROPOSE_COMMAND)
    .bind("incomplete-proposal")
    .fetch_one(db.pool())
    .await
    .expect("proposal receipt count");
    assert_eq!(proposal_receipts, 0);
}

#[tokio::test]
async fn proposal_target_is_frozen_through_approval_and_activation() {
    let db = fixture().await;
    let service = ExecutionBaselineCommandService::new(db.clone());
    let draft_content = content(false);
    let draft_rendered = render_execution_baseline(&draft_content).expect("draft render");
    let draft = service
        .save_draft(SaveExecutionBaselineDraftCommand {
            project_id: PROJECT_ID.to_owned(),
            baseline_id: None,
            base_revision_id: None,
            expected_baseline_version: None,
            content: draft_content,
            rendered_view: draft_rendered.rendered_view,
            render_version: services::EXECUTION_BASELINE_RENDER_VERSION.to_owned(),
            content_digest: draft_rendered.content_digest,
            render_digest: draft_rendered.render_digest,
            provenance: provenance(),
            idempotency_key: "draft-before-proposal".to_owned(),
            authorization: authorization(
                EXECUTION_BASELINE_SAVE_DRAFT_COMMAND,
                "draft-before-proposal",
            ),
            action: None,
        })
        .await
        .expect("draft");
    let proposal_content = content(true);
    let proposal_rendered = render_execution_baseline(&proposal_content).expect("proposal render");
    let proposal_command = ProposeExecutionBaselineForApprovalCommand {
        project_id: PROJECT_ID.to_owned(),
        baseline_id: draft.baseline_id.clone(),
        base_revision_id: draft.revision_id.clone(),
        expected_baseline_version: draft.baseline_version,
        content: proposal_content,
        rendered_view: proposal_rendered.rendered_view,
        render_version: services::EXECUTION_BASELINE_RENDER_VERSION.to_owned(),
        content_digest: proposal_rendered.content_digest,
        render_digest: proposal_rendered.render_digest,
        provenance: provenance(),
        idempotency_key: "proposal-replay".to_owned(),
        authorization: authorization(EXECUTION_BASELINE_PROPOSE_COMMAND, "proposal-replay"),
        action: None,
    };
    let proposed = service
        .propose_for_approval(proposal_command.clone())
        .await
        .expect("proposal");
    let target = proposed.approval_target.clone().expect("approval target");
    assert_eq!(proposed.lifecycle, "proposed");
    assert!(proposed.requires_user_authorization);
    assert_eq!(target.baseline_id, proposed.baseline_id);
    assert_eq!(
        target.revision_id,
        proposed.revision_id.clone().expect("proposal revision")
    );
    assert_eq!(target.revision, 2);
    assert_eq!(target.content, proposal_command.content);
    assert_eq!(target.rendered_view, proposal_command.rendered_view);
    assert_eq!(target.render_version, proposal_command.render_version);
    assert_eq!(target.content_digest, proposal_command.content_digest);
    assert_eq!(target.render_digest, proposal_command.render_digest);
    assert_eq!(target.provenance, proposal_command.provenance);
    assert!(target.requires_user_authorization);
    let project_version: i64 = sqlx::query_scalar("SELECT version FROM project WHERE id = ?")
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("project version");
    let approved = service
        .approve(ApproveExecutionBaselineCommand {
            project_id: PROJECT_ID.to_owned(),
            baseline_id: proposed.baseline_id.clone(),
            revision_id: proposed.revision_id.clone().expect("revision"),
            expected_baseline_version: proposed.baseline_version,
            expected_project_version: project_version,
            content_digest: proposed.content_digest.clone().expect("digest"),
            render_digest: proposed.render_digest.clone().expect("render digest"),
            idempotency_key: "approval-replay".to_owned(),
            authorization: authorization(EXECUTION_BASELINE_APPROVE_COMMAND, "approval-replay"),
            action: None,
        })
        .await
        .expect("approval");
    assert_eq!(approved.lifecycle, "approved");
    assert!(!approved.requires_user_authorization);
    let (approved_baseline_lifecycle, approved_revision_lifecycle): (String, String) =
        sqlx::query_as(
            "SELECT b.lifecycle, r.lifecycle
             FROM project_execution_baseline b
             JOIN project_execution_baseline_revision r
               ON r.id = b.current_revision_id
             WHERE b.id = ?",
        )
        .bind(&proposed.baseline_id)
        .fetch_one(db.pool())
        .await
        .expect("approved baseline state");
    assert_eq!(approved_baseline_lifecycle, "approved");
    assert_eq!(approved_revision_lifecycle, "approved");
    let activated = service
        .activate(ActivateExecutionBaselineCommand {
            project_id: PROJECT_ID.to_owned(),
            baseline_id: proposed.baseline_id.clone(),
            revision_id: proposed.revision_id.clone().expect("revision"),
            approval_id: approved.approval_id.clone().expect("approval id"),
            expected_baseline_version: approved.baseline_version,
            expected_project_version: project_version,
            content_digest: proposed.content_digest.clone().expect("digest"),
            render_digest: proposed.render_digest.clone().expect("render digest"),
            idempotency_key: "activation-replay".to_owned(),
            authorization: authorization(EXECUTION_BASELINE_ACTIVATE_COMMAND, "activation-replay"),
            action: None,
        })
        .await
        .expect("activation");
    assert_eq!(activated.lifecycle, "active");
    assert!(!activated.requires_user_authorization);
    let (active_lifecycle, active_revision_id, active_version): (String, Option<String>, i64) =
        sqlx::query_as(
            "SELECT lifecycle, current_revision_id, version
             FROM project_execution_baseline WHERE id = ?",
        )
        .bind(&proposed.baseline_id)
        .fetch_one(db.pool())
        .await
        .expect("active baseline state");
    assert_eq!(active_lifecycle, "active");
    assert_eq!(active_revision_id, proposed.revision_id);
    assert_eq!(active_version, activated.baseline_version);
    let project_version_after_activation: i64 =
        sqlx::query_scalar("SELECT version FROM project WHERE id = ?")
            .bind(PROJECT_ID)
            .fetch_one(db.pool())
            .await
            .expect("project version after activation");
    assert_eq!(project_version_after_activation, project_version + 1);
    let milestone_lifecycle: String =
        sqlx::query_scalar("SELECT lifecycle FROM project_milestone WHERE id = ?")
            .bind(MILESTONE_ID)
            .fetch_one(db.pool())
            .await
            .expect("milestone activation state");
    assert_eq!(milestone_lifecycle, "active");
    let approval_lifecycle: String = sqlx::query_scalar(
        "SELECT lifecycle FROM project_execution_baseline_approval WHERE id = ?",
    )
    .bind(approved.approval_id.clone().expect("approval id"))
    .fetch_one(db.pool())
    .await
    .expect("approval lifecycle after activation");
    assert_eq!(approval_lifecycle, "consumed");
    let activation_event: (String, String, String, String) = sqlx::query_as(
        "SELECT e.event_type, e.entity_type, e.entity_id, e.payload_json
         FROM command_receipt r
         JOIN domain_event e ON e.id = r.event_id
         WHERE r.operation = ? AND r.idempotency_key = ?",
    )
    .bind(EXECUTION_BASELINE_ACTIVATE_COMMAND)
    .bind("activation-replay")
    .fetch_one(db.pool())
    .await
    .expect("activation receipt/event");
    assert_eq!(activation_event.0, "project.execution_baseline.activated");
    assert_eq!(activation_event.1, "execution_baseline");
    assert_eq!(activation_event.2, proposed.baseline_id);
    let activation_payload: Value =
        serde_json::from_str(&activation_event.3).expect("activation event payload JSON");
    assert_eq!(activation_payload["operation"], "activate");
    assert_eq!(activation_payload["baseline_id"], proposed.baseline_id);
    assert_eq!(
        activation_payload["revision_id"],
        proposed.revision_id.clone().unwrap()
    );
    assert_eq!(
        activation_payload["approval_id"],
        approved.approval_id.clone().unwrap()
    );
    let replayed = service
        .propose_for_approval(proposal_command)
        .await
        .expect("proposal replay");
    assert_eq!(replayed, proposed);
    assert_eq!(replayed.approval_target, Some(target));
}

#[tokio::test]
async fn changed_input_with_same_key_is_an_idempotency_conflict() {
    let db = fixture().await;
    let service = ExecutionBaselineCommandService::new(db);
    let draft_content = content(false);
    let rendered = render_execution_baseline(&draft_content).expect("render");
    let mut command = SaveExecutionBaselineDraftCommand {
        project_id: PROJECT_ID.to_owned(),
        baseline_id: None,
        base_revision_id: None,
        expected_baseline_version: None,
        content: draft_content,
        rendered_view: rendered.rendered_view,
        render_version: services::EXECUTION_BASELINE_RENDER_VERSION.to_owned(),
        content_digest: rendered.content_digest,
        render_digest: rendered.render_digest,
        provenance: provenance(),
        idempotency_key: "changed-input".to_owned(),
        authorization: authorization(EXECUTION_BASELINE_SAVE_DRAFT_COMMAND, "changed-input"),
        action: None,
    };
    service
        .save_draft(command.clone())
        .await
        .expect("first draft");
    command.content.plan_item_ids.push("changed".to_owned());
    let rendered = render_execution_baseline(&command.content).expect("changed render");
    command.rendered_view = rendered.rendered_view;
    command.content_digest = rendered.content_digest;
    command.render_digest = rendered.render_digest;
    let error = service
        .save_draft(command)
        .await
        .expect_err("changed key must conflict");
    assert!(matches!(
        error,
        services::ServiceError::Db(db::DbError::IdempotencyConflict)
    ));
}

type ProjectSnapshot = (
    String,
    i64,
    Option<String>,
    String,
    i64,
    Option<String>,
    Option<String>,
    i64,
);
type BaselineSnapshot = (String, String, Option<String>, String, i64);
type BaselineRevisionSnapshot = (
    String,
    String,
    i64,
    i64,
    Option<String>,
    String,
    String,
    String,
    String,
);
type BaselineApprovalSnapshot = (
    String,
    String,
    String,
    i64,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
);
type MilestoneSnapshot = (String, String, Option<String>, i64);
type GovernanceSnapshot = (String, String, Option<String>, Option<String>, i64);
type DomainEventSnapshot = (
    String,
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
);
type CommandReceiptSnapshot = (
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    Option<String>,
);
type ActionExecutionSnapshot = (String, String, i64, String, String);

#[derive(Debug, PartialEq, Eq)]
struct ExecutionBaselineStateSnapshot {
    project: ProjectSnapshot,
    baselines: Vec<BaselineSnapshot>,
    revisions: Vec<BaselineRevisionSnapshot>,
    approvals: Vec<BaselineApprovalSnapshot>,
    milestones: Vec<MilestoneSnapshot>,
    governance: Vec<GovernanceSnapshot>,
    events: Vec<DomainEventSnapshot>,
    receipts: Vec<CommandReceiptSnapshot>,
    action_executions: Vec<ActionExecutionSnapshot>,
}

async fn execution_baseline_state_snapshot(db: &SqliteDb) -> ExecutionBaselineStateSnapshot {
    let project = sqlx::query_as::<_, ProjectSnapshot>(
        "SELECT id, version, primary_milestone_id, charter_status,
                charter_setup_required, current_charter_id,
                current_charter_revision_id, current_charter_version
         FROM project WHERE id = ?",
    )
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("project snapshot");
    let baselines = sqlx::query_as::<_, BaselineSnapshot>(
        "SELECT id, lifecycle, current_revision_id, project_id, version
         FROM project_execution_baseline WHERE project_id = ? ORDER BY id",
    )
    .bind(PROJECT_ID)
    .fetch_all(db.pool())
    .await
    .expect("baseline snapshot");
    let revisions = sqlx::query_as::<_, BaselineRevisionSnapshot>(
        "SELECT r.id, r.baseline_id, r.revision, r.base_revision,
                r.base_revision_id, r.lifecycle, r.content_digest,
                r.rendered_digest, r.rendered_view
         FROM project_execution_baseline_revision r
         JOIN project_execution_baseline b ON b.id = r.baseline_id
         WHERE b.project_id = ? ORDER BY r.id",
    )
    .bind(PROJECT_ID)
    .fetch_all(db.pool())
    .await
    .expect("baseline revision snapshot");
    let approvals = sqlx::query_as::<_, BaselineApprovalSnapshot>(
        "SELECT a.id, a.baseline_id, a.revision_id,
                a.expected_project_version, a.principal_type, a.principal_id,
                a.authorization_action, a.authorization_occurred_at,
                a.explicit_event, a.content_digest, a.rendered_digest,
                a.lifecycle
         FROM project_execution_baseline_approval a
         JOIN project_execution_baseline b ON b.id = a.baseline_id
         WHERE b.project_id = ? ORDER BY a.id",
    )
    .bind(PROJECT_ID)
    .fetch_all(db.pool())
    .await
    .expect("baseline approval snapshot");
    let milestones = sqlx::query_as::<_, MilestoneSnapshot>(
        "SELECT id, lifecycle, current_definition_revision_id, version
         FROM project_milestone WHERE project_id = ? ORDER BY id",
    )
    .bind(PROJECT_ID)
    .fetch_all(db.pool())
    .await
    .expect("milestone snapshot");
    let governance = sqlx::query_as::<_, GovernanceSnapshot>(
        "SELECT task_id, project_id, baseline_id, baseline_revision_id, version
         FROM project_task_governance WHERE project_id = ? ORDER BY task_id",
    )
    .bind(PROJECT_ID)
    .fetch_all(db.pool())
    .await
    .expect("governance snapshot");
    let events = sqlx::query_as::<_, DomainEventSnapshot>(
        "SELECT id, event_type, entity_type, entity_id, scope_id,
                dedupe_key, correlation_id
         FROM domain_event WHERE scope_type = 'project' AND scope_id = ?
         ORDER BY id",
    )
    .bind(PROJECT_ID)
    .fetch_all(db.pool())
    .await
    .expect("domain event snapshot");
    let receipts = sqlx::query_as::<_, CommandReceiptSnapshot>(
        "SELECT id, operation, idempotency_key, input_digest,
                event_id, scope_id, outcome_json, agent_action_execution_id
         FROM command_receipt WHERE scope_type = 'project' AND scope_id = ?
         ORDER BY id",
    )
    .bind(PROJECT_ID)
    .fetch_all(db.pool())
    .await
    .expect("command receipt snapshot");
    let action_executions = sqlx::query_as::<_, ActionExecutionSnapshot>(
        "SELECT id, action_id, attempt, status, idempotency_key
         FROM agent_action_execution ORDER BY id",
    )
    .fetch_all(db.pool())
    .await
    .expect("action execution snapshot");

    ExecutionBaselineStateSnapshot {
        project,
        baselines,
        revisions,
        approvals,
        milestones,
        governance,
        events,
        receipts,
        action_executions,
    }
}

async fn install_execution_baseline_receipt_failpoint(db: &SqliteDb, operation: &str) {
    sqlx::query(
        "CREATE TEMP TRIGGER execution_baseline_command_receipt_failpoint
         BEFORE INSERT ON command_receipt
         BEGIN SELECT RAISE(ABORT, 'execution baseline command receipt failpoint'); END",
    )
    .execute(db.pool())
    .await
    .unwrap_or_else(|error| panic!("install receipt failpoint for {operation}: {error}"));
}

async fn drop_execution_baseline_receipt_failpoint(db: &SqliteDb) {
    sqlx::query("DROP TRIGGER execution_baseline_command_receipt_failpoint")
        .execute(db.pool())
        .await
        .expect("drop execution-baseline receipt failpoint");
}

async fn assert_frozen_execution_baseline_receipt(
    db: &SqliteDb,
    operation: &str,
    idempotency_key: &str,
    outcome: &services::ExecutionBaselineCommandOutcome,
) {
    let (receipt_id, event_id, input_digest, outcome_json): (String, String, String, String) =
        sqlx::query_as(
            "SELECT id, event_id, input_digest, outcome_json
             FROM command_receipt
             WHERE operation = ? AND idempotency_key = ?",
        )
        .bind(operation)
        .bind(idempotency_key)
        .fetch_one(db.pool())
        .await
        .expect("frozen command receipt");
    assert_eq!(outcome.receipt_id.as_deref(), Some(receipt_id.as_str()));
    let persisted: Value = serde_json::from_str(&outcome_json).expect("receipt outcome JSON");
    assert_eq!(persisted["receipt_id"], receipt_id);
    let outcome_operation = match operation {
        EXECUTION_BASELINE_SAVE_DRAFT_COMMAND => "save_draft",
        EXECUTION_BASELINE_PROPOSE_COMMAND => "propose_for_approval",
        EXECUTION_BASELINE_APPROVE_COMMAND => "approve",
        EXECUTION_BASELINE_ACTIVATE_COMMAND => "activate",
        other => panic!("unexpected execution-baseline operation: {other}"),
    };
    assert_eq!(persisted["operation"], outcome_operation);
    assert_eq!(persisted["project_id"], PROJECT_ID);
    assert_eq!(
        persisted["baseline_id"],
        serde_json::json!(outcome.baseline_id)
    );
    assert_eq!(
        persisted["revision_id"],
        serde_json::json!(outcome.revision_id)
    );
    if let Some(approval_id) = &outcome.approval_id {
        assert_eq!(persisted["approval_id"], serde_json::json!(approval_id));
    }
    assert!(persisted["domain_committed"].as_bool().unwrap_or(false));
    assert!(!event_id.trim().is_empty());
    assert!(!input_digest.trim().is_empty());
}

async fn assert_replay_is_exact_without_duplicates(
    db: &SqliteDb,
    before_replay: ExecutionBaselineStateSnapshot,
    expected: &services::ExecutionBaselineCommandOutcome,
    replay: &services::ExecutionBaselineCommandOutcome,
) {
    assert_eq!(replay, expected);
    let after_replay = execution_baseline_state_snapshot(db).await;
    assert_eq!(
        after_replay, before_replay,
        "receipt replay must not duplicate baseline-domain rows, pointers, governance, events, receipts, or action executions"
    );
}

#[tokio::test]
async fn every_execution_baseline_command_is_atomic_at_receipt_finalization_and_replay_exact() {
    let db = fixture().await;

    // save_draft: a receipt-write failure must roll back the first baseline,
    // revision, pointer, event, receipt, and optional action execution.
    let draft_content = content(false);
    let draft_rendered = render_execution_baseline(&draft_content).expect("draft render");
    let draft_command = SaveExecutionBaselineDraftCommand {
        project_id: PROJECT_ID.to_owned(),
        baseline_id: None,
        base_revision_id: None,
        expected_baseline_version: None,
        content: draft_content,
        rendered_view: draft_rendered.rendered_view,
        render_version: services::EXECUTION_BASELINE_RENDER_VERSION.to_owned(),
        content_digest: draft_rendered.content_digest,
        render_digest: draft_rendered.render_digest,
        provenance: provenance(),
        idempotency_key: "receipt-fail-save-draft".to_owned(),
        authorization: authorization(
            EXECUTION_BASELINE_SAVE_DRAFT_COMMAND,
            "receipt-fail-save-draft",
        ),
        action: None,
    };
    let before_save = execution_baseline_state_snapshot(&db).await;
    install_execution_baseline_receipt_failpoint(&db, EXECUTION_BASELINE_SAVE_DRAFT_COMMAND).await;
    let failed_save = ExecutionBaselineCommandService::new(db.clone())
        .save_draft(draft_command.clone())
        .await
        .expect_err("save_draft receipt failpoint must abort the command");
    assert!(failed_save
        .to_string()
        .contains("execution baseline command receipt failpoint"));
    drop_execution_baseline_receipt_failpoint(&db).await;
    assert_eq!(
        execution_baseline_state_snapshot(&db).await,
        before_save,
        "save_draft receipt failure must leave no baseline, revision, pointer, governance, event, receipt, or action residue and must preserve versions"
    );
    let saved = ExecutionBaselineCommandService::new(db.clone())
        .save_draft(draft_command.clone())
        .await
        .expect("save_draft retry");
    let saved_expected = saved.clone();
    assert_frozen_execution_baseline_receipt(
        &db,
        EXECUTION_BASELINE_SAVE_DRAFT_COMMAND,
        "receipt-fail-save-draft",
        &saved_expected,
    )
    .await;
    let before_save_replay = execution_baseline_state_snapshot(&db).await;
    drop(saved);
    let save_replay = ExecutionBaselineCommandService::new(db.clone())
        .save_draft(draft_command)
        .await
        .expect("save_draft replay after response loss");
    assert_replay_is_exact_without_duplicates(
        &db,
        before_save_replay,
        &saved_expected,
        &save_replay,
    )
    .await;

    // propose_for_approval: the existing draft must survive the failed
    // finalization unchanged, including its baseline version and pointer.
    let proposal_content = content(true);
    let proposal_rendered = render_execution_baseline(&proposal_content).expect("proposal render");
    let proposal_command = ProposeExecutionBaselineForApprovalCommand {
        project_id: PROJECT_ID.to_owned(),
        baseline_id: saved_expected.baseline_id.clone(),
        base_revision_id: saved_expected.revision_id.clone(),
        expected_baseline_version: saved_expected.baseline_version,
        content: proposal_content,
        rendered_view: proposal_rendered.rendered_view,
        render_version: services::EXECUTION_BASELINE_RENDER_VERSION.to_owned(),
        content_digest: proposal_rendered.content_digest,
        render_digest: proposal_rendered.render_digest,
        provenance: provenance(),
        idempotency_key: "receipt-fail-propose".to_owned(),
        authorization: authorization(EXECUTION_BASELINE_PROPOSE_COMMAND, "receipt-fail-propose"),
        action: None,
    };
    let before_proposal = execution_baseline_state_snapshot(&db).await;
    install_execution_baseline_receipt_failpoint(&db, EXECUTION_BASELINE_PROPOSE_COMMAND).await;
    let failed_proposal = ExecutionBaselineCommandService::new(db.clone())
        .propose_for_approval(proposal_command.clone())
        .await
        .expect_err("propose_for_approval receipt failpoint must abort the command");
    assert!(failed_proposal
        .to_string()
        .contains("execution baseline command receipt failpoint"));
    drop_execution_baseline_receipt_failpoint(&db).await;
    assert_eq!(
        execution_baseline_state_snapshot(&db).await,
        before_proposal,
        "propose_for_approval receipt failure must preserve the draft, all pointers, versions, and leave no event/receipt/action residue"
    );
    let proposed = ExecutionBaselineCommandService::new(db.clone())
        .propose_for_approval(proposal_command.clone())
        .await
        .expect("propose_for_approval retry");
    let proposed_expected = proposed.clone();
    assert_frozen_execution_baseline_receipt(
        &db,
        EXECUTION_BASELINE_PROPOSE_COMMAND,
        "receipt-fail-propose",
        &proposed_expected,
    )
    .await;
    let before_proposal_replay = execution_baseline_state_snapshot(&db).await;
    drop(proposed);
    let proposal_replay = ExecutionBaselineCommandService::new(db.clone())
        .propose_for_approval(proposal_command)
        .await
        .expect("propose_for_approval replay after response loss");
    assert_replay_is_exact_without_duplicates(
        &db,
        before_proposal_replay,
        &proposed_expected,
        &proposal_replay,
    )
    .await;

    // approve: approval, revision lifecycle, baseline lifecycle/version, and
    // event/receipt must all roll back as one unit.
    let project_version: i64 = sqlx::query_scalar("SELECT version FROM project WHERE id = ?")
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("project version before approval");
    let approve_command = ApproveExecutionBaselineCommand {
        project_id: PROJECT_ID.to_owned(),
        baseline_id: proposed_expected.baseline_id.clone(),
        revision_id: proposed_expected
            .revision_id
            .clone()
            .expect("proposal revision"),
        expected_baseline_version: proposed_expected.baseline_version,
        expected_project_version: project_version,
        content_digest: proposed_expected
            .content_digest
            .clone()
            .expect("proposal digest"),
        render_digest: proposed_expected
            .render_digest
            .clone()
            .expect("proposal render digest"),
        idempotency_key: "receipt-fail-approve".to_owned(),
        authorization: authorization(EXECUTION_BASELINE_APPROVE_COMMAND, "receipt-fail-approve"),
        action: None,
    };
    let before_approval = execution_baseline_state_snapshot(&db).await;
    install_execution_baseline_receipt_failpoint(&db, EXECUTION_BASELINE_APPROVE_COMMAND).await;
    let failed_approval = ExecutionBaselineCommandService::new(db.clone())
        .approve(approve_command.clone())
        .await
        .expect_err("approve receipt failpoint must abort the command");
    assert!(failed_approval
        .to_string()
        .contains("execution baseline command receipt failpoint"));
    drop_execution_baseline_receipt_failpoint(&db).await;
    assert_eq!(
        execution_baseline_state_snapshot(&db).await,
        before_approval,
        "approve receipt failure must leave no approval, lifecycle/pointer/version mutation, event, receipt, or action residue"
    );
    let approved = ExecutionBaselineCommandService::new(db.clone())
        .approve(approve_command.clone())
        .await
        .expect("approve retry");
    let approved_expected = approved.clone();
    assert_frozen_execution_baseline_receipt(
        &db,
        EXECUTION_BASELINE_APPROVE_COMMAND,
        "receipt-fail-approve",
        &approved_expected,
    )
    .await;
    let before_approval_replay = execution_baseline_state_snapshot(&db).await;
    drop(approved);
    let approval_replay = ExecutionBaselineCommandService::new(db.clone())
        .approve(approve_command)
        .await
        .expect("approve replay after response loss");
    assert_replay_is_exact_without_duplicates(
        &db,
        before_approval_replay,
        &approved_expected,
        &approval_replay,
    )
    .await;

    // activate: baseline/milestone/project pointers, governance promotion,
    // approval consumption, event, receipt, and action execution are one
    // transaction.  The trigger fires only at receipt finalization.
    let activate_command = ActivateExecutionBaselineCommand {
        project_id: PROJECT_ID.to_owned(),
        baseline_id: approved_expected.baseline_id.clone(),
        revision_id: approved_expected
            .revision_id
            .clone()
            .expect("approved revision"),
        approval_id: approved_expected.approval_id.clone().expect("approval id"),
        expected_baseline_version: approved_expected.baseline_version,
        expected_project_version: project_version,
        content_digest: proposed_expected
            .content_digest
            .clone()
            .expect("approved digest"),
        render_digest: proposed_expected
            .render_digest
            .clone()
            .expect("approved render digest"),
        idempotency_key: "receipt-fail-activate".to_owned(),
        authorization: authorization(EXECUTION_BASELINE_ACTIVATE_COMMAND, "receipt-fail-activate"),
        action: None,
    };
    let before_activation = execution_baseline_state_snapshot(&db).await;
    install_execution_baseline_receipt_failpoint(&db, EXECUTION_BASELINE_ACTIVATE_COMMAND).await;
    let failed_activation = ExecutionBaselineCommandService::new(db.clone())
        .activate(activate_command.clone())
        .await
        .expect_err("activate receipt failpoint must abort the command");
    assert!(failed_activation
        .to_string()
        .contains("execution baseline command receipt failpoint"));
    drop_execution_baseline_receipt_failpoint(&db).await;
    assert_eq!(
        execution_baseline_state_snapshot(&db).await,
        before_activation,
        "activate receipt failure must leave no baseline/milestone/project pointer, governance, approval, event, receipt, or action residue and must preserve versions"
    );
    let activated = ExecutionBaselineCommandService::new(db.clone())
        .activate(activate_command.clone())
        .await
        .expect("activate retry");
    let activated_expected = activated.clone();
    assert_frozen_execution_baseline_receipt(
        &db,
        EXECUTION_BASELINE_ACTIVATE_COMMAND,
        "receipt-fail-activate",
        &activated_expected,
    )
    .await;
    let before_activation_replay = execution_baseline_state_snapshot(&db).await;
    drop(activated);
    let activation_replay = ExecutionBaselineCommandService::new(db.clone())
        .activate(activate_command)
        .await
        .expect("activate replay after response loss");
    assert_replay_is_exact_without_duplicates(
        &db,
        before_activation_replay,
        &activated_expected,
        &activation_replay,
    )
    .await;
}
