//! Gate A acceptance coverage for the Project milestone/readiness/release
//! command composites.
//!
//! The Project Agent materializer is still a legacy adapter for these three
//! operation families.  These tests therefore enter at the shared repository
//! command boundary, where the command contracts are already available, and
//! keep the transport gap explicit rather than duplicating the adapter's raw
//! SQL in another test.

use std::sync::Arc;

use api_types::{
    AcceptanceCheckSourceKind, AcceptanceEvidenceRequirement, ExecutionBaselineReleasePolicy,
    MilestoneAcceptanceCheck, MilestoneDefinitionContent, MilestoneDefinitionLifecycle,
    PrincipalKind, PrincipalRef, RevisionProvenance,
};
use db::{
    create_sqlite_pool, run_migrations, AgentActionExecutionStatus, AgentActionPolicyResult,
    AgentActionRepo, AgentActionStatus, AgentRepo, CommandReceiptRepo, CreateAgentAction,
    CreateAgentActionExecution, CreateAgentIdentity, CreateAgentProfile, CreateCommandReceipt,
    CreateDomainEvent, CreateProject, CreateProjectMilestone, CreateProjectMilestoneCommand,
    CreateProjectMilestoneRevision, CreateProjectReadinessSnapshot,
    CreateProjectReadinessSnapshotCommand, CreateProjectReleaseRequest,
    CreateProjectReleaseRequestCommand, DbError, DomainEventRepo, ProjectOrchestrationRepo,
    ProjectReadinessSnapshotRecord, ProjectRepo, SqliteDb, User, UserRepo,
};
use forge_agent_host::{
    PROJECT_MILESTONE_OPERATION, PROJECT_READINESS_OPERATION, PROJECT_RELEASE_OPERATION,
};
use serde_json::{json, Value};
use services::execution_baseline::{
    release_policy_digest, EXECUTION_BASELINE_RELEASE_POLICY_SCHEMA,
};
use services::{
    MilestoneRuntime, ProjectCommandAuthorization, ProjectMilestoneCommandService,
    ProjectMilestoneDefinitionCommand, ProjectPrimaryMilestoneCommand,
    ProjectReadinessRequestCommand, ProjectReleaseRequestCommand, ServiceError,
};

const USER_ID: &str = "milestone-command-user";
const AGENT_ID: &str = "milestone-command-agent";
const PROFILE_ID: &str = "milestone-command-profile";
const PROJECT_ID: &str = "milestone-command-project";
const MILESTONE_ID: &str = "milestone-command-milestone";
const MILESTONE_REVISION_ID: &str = "milestone-command-revision";
const BASELINE_ID: &str = "milestone-command-baseline";
const BASELINE_REVISION_ID: &str = "milestone-command-baseline-revision";
const CHARTER_ID: &str = "milestone-command-charter";
const CHARTER_REVISION_ID: &str = "milestone-command-charter-revision";
const NOW: &str = "2026-08-20T00:00:00.000Z";

fn baseline_release_policy() -> (String, String) {
    let policy = ExecutionBaselineReleasePolicy {
        schema_version: EXECUTION_BASELINE_RELEASE_POLICY_SCHEMA.to_owned(),
        revision: "policy@1".to_owned(),
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
    };
    let digest = release_policy_digest(&policy).expect("baseline policy digest");
    let envelope = json!({
        "revision": policy.revision.clone(),
        "digest": digest.clone(),
        "policy": policy,
    })
    .to_string();
    (envelope, digest)
}

async fn database() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("SQLite pool creates");
    run_migrations(&pool).await.expect("migrations run");
    Arc::new(SqliteDb::new(pool))
}

async fn fixture() -> Arc<SqliteDb> {
    fixture_with_acceptance_matrix("[]").await
}

async fn fixture_with_acceptance_matrix(acceptance_matrix_json: &str) -> Arc<SqliteDb> {
    let db = database().await;
    UserRepo::create_user(
        &*db,
        &User {
            id: USER_ID.to_owned(),
            email: "milestone-command@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: Some("Milestone command user".to_owned()),
            is_admin: false,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("user creates");
    AgentRepo::create_identity_with_profile(
        &*db,
        CreateAgentIdentity {
            id: AGENT_ID.to_owned(),
            name: "Milestone command agent".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: db::AgentStatus::Idle,
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
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("agent identity creates");
    ProjectRepo::create_with_agent_binding(
        &*db,
        CreateProject {
            id: PROJECT_ID.to_owned(),
            name: "Milestone command project".to_owned(),
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
    .expect("Project creates");

    // The acceptance commands consume an already active definition and an
    // approved active baseline.  These are immutable fixture rows, not an
    // alternate materializer implementation.
    sqlx::query(
        "INSERT INTO project_charter
            (id, account_id, genesis_session_id, project_id,
             current_draft_revision_id, current_approved_revision_id,
             project_mode, maturity, lifecycle, version, created_at, updated_at)
         VALUES (?, ?, NULL, ?, NULL, NULL, 'compact', 'mvp', 'attached', 1, ?, ?)",
    )
    .bind(CHARTER_ID)
    .bind(USER_ID)
    .bind(PROJECT_ID)
    .bind(CHARTER_REVISION_ID)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("Charter creates");
    sqlx::query(
        "INSERT INTO project_charter_revision
            (id, charter_id, revision, base_revision, base_revision_id,
             lifecycle, schema_version, render_version, content_json,
             rendered_view, change_summary, author_type, author_id,
             source_message_id, source_turn_job_id, source_refs_json,
             content_digest, rendered_digest, created_at)
         VALUES (?, ?, 1, 0, NULL, 'approved', 'charter@1', 'render@1', '{}',
                 '# Charter', 'fixture', 'user', ?, NULL, NULL, '[]',
                 'charter-content', 'charter-rendered', ?)",
    )
    .bind(CHARTER_REVISION_ID)
    .bind(CHARTER_ID)
    .bind(USER_ID)
    .bind(PROJECT_ID)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("Charter revision creates");
    sqlx::query("UPDATE project_charter SET current_approved_revision_id = ? WHERE id = ?")
        .bind(CHARTER_REVISION_ID)
        .bind(CHARTER_ID)
        .execute(db.pool())
        .await
        .expect("Charter pointer updates");
    sqlx::query(
        "INSERT INTO project_milestone
            (id, project_id, milestone_sequence, milestone_key, display_label,
             lifecycle, blocker_reason_json, stale_reason_json,
             reconciliation_reason_json, version, created_at, updated_at)
         VALUES (?, ?, 1, 'M001', 'Release milestone', 'active', '[]', '[]',
                 '[]', 1, ?, ?)",
    )
    .bind(MILESTONE_ID)
    .bind(PROJECT_ID)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("milestone creates");
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
         VALUES (?, ?, 1, 0, NULL, 'approved', 'Release milestone',
                 'The release outcome is delivered', '[]', '[]', ?, '[]',
                 '[]', '[]', '[]', '[]', '[]', '[]', 'fixture',
                 'milestone@1', 'milestone-render@1', '# Milestone',
                 'milestone-content', 'milestone-rendered', 'user', ?, '[]', ?)",
    )
    .bind(MILESTONE_REVISION_ID)
    .bind(MILESTONE_ID)
    .bind(CHARTER_REVISION_ID)
    .bind(USER_ID)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("milestone revision creates");
    sqlx::query(
        "UPDATE project_milestone
         SET current_definition_revision_id = ? WHERE id = ?",
    )
    .bind(MILESTONE_REVISION_ID)
    .bind(MILESTONE_ID)
    .execute(db.pool())
    .await
    .expect("milestone pointer updates");
    sqlx::query(
        "INSERT INTO project_execution_baseline
            (id, project_id, current_revision_id, lifecycle, version,
             created_at, updated_at)
         VALUES (?, ?, ?, 'active', 1, ?, ?)",
    )
    .bind(BASELINE_ID)
    .bind(PROJECT_ID)
    .bind(BASELINE_REVISION_ID)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("baseline creates");
    let (release_policy_json, release_policy_digest) = baseline_release_policy();
    sqlx::query(
        "INSERT INTO project_execution_baseline_revision
            (id, baseline_id, revision, base_revision, base_revision_id,
             lifecycle, charter_revision_id, document_revisions_json,
             plan_items_json, milestone_id, milestone_ids_json,
             milestone_definition_revision_ids_json, primary_milestone_id,
             release_policy_json, release_policy_revision,
             release_policy_digest, acceptance_matrix_json,
             capability_classes_json, risk_classes_json, adaptive_envelope_json,
             elevated_operations_json, exclusions_json, rollback_recovery_json,
             schema_version, render_version, rendered_view, content_digest,
             rendered_digest, source_refs_json, created_at)
         VALUES (?, ?, 1, 0, NULL, 'approved', ?, '[]', '[]', ?, ?, ?, NULL,
                 ?, 'policy@1', ?, ?, '[]', '[]', '{}',
                 '[]', '[]', '{}', 'baseline@1', 'baseline-render@1',
                 '# Baseline', 'baseline-digest-1', 'baseline-rendered-1',
                 '[]', ?)",
    )
    .bind(BASELINE_REVISION_ID)
    .bind(BASELINE_ID)
    .bind(CHARTER_REVISION_ID)
    .bind(MILESTONE_ID)
    .bind(format!("[\"{MILESTONE_ID}\"]"))
    .bind(format!("[\"{MILESTONE_REVISION_ID}\"]"))
    .bind(release_policy_json)
    .bind(release_policy_digest)
    .bind(acceptance_matrix_json)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("baseline revision creates");
    db
}

#[tokio::test]
async fn readiness_persists_reconciliation_when_baseline_matrix_is_absent_from_current_definition()
{
    let matrix = json!([{
        "id": "missing-check",
        "description": "A check the approved baseline requires",
        "required": true,
        "evidence_kind": "test-report",
        "check_definition_revision": "check-1",
    }]);
    let db = fixture_with_acceptance_matrix(&matrix.to_string()).await;
    let snapshot = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .request_readiness(readiness_command("inconsistent-baseline"), None)
        .await
        .expect("an inconsistent baseline must produce a canonical non-ready snapshot");

    assert_eq!(snapshot.outcome, "blocked");
    assert!(snapshot
        .blocking_reasons_json
        .contains("reconciliation_required"));
    assert!(snapshot.blocking_reasons_json.contains("missing-check"));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_readiness_snapshot WHERE project_id = ?",
        )
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("readiness snapshot count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'milestone.readiness.evaluated'",
        )
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("readiness event count"),
        1
    );
    let lifecycle: String =
        sqlx::query_scalar("SELECT lifecycle FROM project_milestone WHERE id = ?")
            .bind(MILESTONE_ID)
            .fetch_one(db.pool())
            .await
            .expect("milestone lifecycle");
    assert_eq!(lifecycle, "active");
    let reconciliation: String =
        sqlx::query_scalar("SELECT reconciliation_reason_json FROM project_milestone WHERE id = ?")
            .bind(MILESTONE_ID)
            .fetch_one(db.pool())
            .await
            .expect("milestone reconciliation projection");
    assert!(reconciliation.contains("reconciliation_required"));
    assert!(reconciliation.contains("missing-check"));

    let error = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .request_release(
            release_command("blocked-release-candidate", &snapshot),
            None,
        )
        .await
        .expect_err("a blocked readiness result cannot become a release candidate");
    let message = error.to_string();
    assert!(message.contains("readiness is blocked"));
    assert!(message.contains("reconciliation_required"));
    assert!(message.contains("do not claim Known Issues: None"));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE operation = ? AND idempotency_key = 'blocked-release-candidate'",
        )
        .bind(PROJECT_RELEASE_OPERATION)
        .fetch_one(db.pool())
        .await
        .expect("blocked release candidate receipt count"),
        0
    );
}

struct ReceiptInput<'a> {
    operation: &'a str,
    principal_type: &'a str,
    principal_id: &'a str,
    key: &'a str,
    digest: &'a str,
    correlation_id: &'a str,
    outcome: &'a Value,
    execution_id: Option<&'a str>,
}

fn receipt(input: ReceiptInput<'_>) -> CreateCommandReceipt {
    let ReceiptInput {
        operation,
        principal_type,
        principal_id,
        key,
        digest,
        correlation_id,
        outcome,
        execution_id,
    } = input;
    CreateCommandReceipt {
        id: format!("receipt-{key}"),
        principal_type: principal_type.to_owned(),
        principal_id: principal_id.to_owned(),
        scope_type: "project".to_owned(),
        scope_id: PROJECT_ID.to_owned(),
        operation: operation.to_owned(),
        idempotency_key: key.to_owned(),
        input_digest: digest.to_owned(),
        policy_result: "allowed".to_owned(),
        correlation_id: correlation_id.to_owned(),
        causation_id: None,
        causation_depth: 0,
        event_id: String::new(),
        agent_action_execution_id: execution_id.map(str::to_owned),
        outcome_json: outcome.to_string(),
        committed_at: NOW.to_owned(),
    }
}

fn user_authorization(action: &str, key: &str) -> ProjectCommandAuthorization {
    ProjectCommandAuthorization {
        principal_type: "user".to_owned(),
        principal_id: USER_ID.to_owned(),
        policy_result: "allowed".to_owned(),
        policy_revision: None,
        policy_digest: None,
        requested_permission: Some(action.to_owned()),
        correlation_id: format!("correlation-{key}"),
        causation_id: None,
        causation_depth: 0,
        authorization_event_id: format!("authorization-{key}"),
        authorization_basis: "explicit user authorization".to_owned(),
        authorization_action: action.to_owned(),
        authorization_occurred_at: db::now_rfc3339(),
        authorization_json: json!({"action": action, "key": key}).to_string(),
    }
}

fn agent_authorization(action: &str, key: &str) -> ProjectCommandAuthorization {
    ProjectCommandAuthorization {
        principal_type: "agent".to_owned(),
        principal_id: AGENT_ID.to_owned(),
        policy_result: "allowed".to_owned(),
        policy_revision: Some("agent-policy@1".to_owned()),
        policy_digest: Some("agent-policy-digest".to_owned()),
        requested_permission: Some(action.to_owned()),
        correlation_id: format!("correlation-{key}"),
        causation_id: None,
        causation_depth: 0,
        authorization_event_id: format!("authorization-{key}"),
        authorization_basis: "bound Project Agent authorization".to_owned(),
        authorization_action: action.to_owned(),
        authorization_occurred_at: db::now_rfc3339(),
        authorization_json: json!({
            "principal": {"kind": "agent", "id": AGENT_ID},
            "action": action,
            "event_id": format!("authorization-{key}"),
        })
        .to_string(),
    }
}

async fn arm_receipt_failpoint(db: &SqliteDb, trigger_name: &str, message: &str) {
    sqlx::query(&format!(
        "CREATE TEMP TRIGGER {trigger_name}
         BEFORE INSERT ON command_receipt
         BEGIN SELECT RAISE(ABORT, '{message}'); END"
    ))
    .execute(db.pool())
    .await
    .expect("command receipt failpoint creates");
}

async fn drop_receipt_failpoint(db: &SqliteDb, trigger_name: &str) {
    sqlx::query(&format!("DROP TRIGGER {trigger_name}"))
        .execute(db.pool())
        .await
        .expect("command receipt failpoint drops");
}

fn definition_content(name: &str, check_id: Option<&str>) -> MilestoneDefinitionContent {
    MilestoneDefinitionContent {
        name: name.to_owned(),
        outcome: format!("{name} outcome is delivered"),
        included_scope: vec!["implementation".to_owned()],
        excluded_scope: Vec::new(),
        charter_revision: None,
        document_revisions: Vec::new(),
        task_ids: Vec::new(),
        dependencies: Vec::new(),
        risks: Vec::new(),
        acceptance_checks: check_id
            .map(|id| {
                vec![MilestoneAcceptanceCheck {
                    id: id.to_owned(),
                    description: format!("Verify {name}"),
                    required: true,
                    source_kind: AcceptanceCheckSourceKind::Manual,
                    expected_result: "pass".to_owned(),
                    latest_result: None,
                    latest_result_id: None,
                    latest_result_digest: None,
                }]
            })
            .unwrap_or_default(),
        evidence_requirements: check_id
            .map(|id| {
                vec![AcceptanceEvidenceRequirement {
                    id: id.to_owned(),
                    description: format!("Evidence for {name}"),
                    required: true,
                    evidence_kind: None,
                    check_definition_revision: None,
                }]
            })
            .unwrap_or_default(),
        known_issues: Vec::new(),
        target_date: None,
    }
}

struct DefinitionCommandInput<'a> {
    project_version: i64,
    milestone_id: Option<&'a str>,
    milestone_version: i64,
    base_revision_id: Option<&'a str>,
    lifecycle: MilestoneDefinitionLifecycle,
    key: &'a str,
    name: &'a str,
    check_id: Option<&'a str>,
}

fn definition_command(input: DefinitionCommandInput<'_>) -> ProjectMilestoneDefinitionCommand {
    let DefinitionCommandInput {
        project_version,
        milestone_id,
        milestone_version,
        base_revision_id,
        lifecycle,
        key,
        name,
        check_id,
    } = input;
    let content = definition_content(name, check_id);
    ProjectMilestoneDefinitionCommand {
        project_id: PROJECT_ID.to_owned(),
        milestone_id: milestone_id.map(str::to_owned),
        display_label: Some(name.to_owned()),
        lifecycle,
        rendered_view: api_types::canonical_json(&content).expect("canonical milestone content"),
        render_version: "forge.milestone-definition-render/v1".to_owned(),
        change_summary: format!("{name} command"),
        provenance: RevisionProvenance {
            author: PrincipalRef {
                kind: PrincipalKind::User,
                id: USER_ID.to_owned(),
                display_name: None,
            },
            profile_revision: None,
            operating_skill_revision: None,
            source_refs: Vec::new(),
            change_summary: format!("{name} command"),
            material_diff: None,
        },
        content,
        base_revision_id: base_revision_id.map(str::to_owned),
        expected_project_version: project_version,
        expected_milestone_version: milestone_version,
        idempotency_key: key.to_owned(),
        authorization: user_authorization(
            if milestone_id.is_some() {
                "project.milestone.revision.save"
            } else {
                "project.milestone.create"
            },
            key,
        ),
    }
}

async fn action(
    db: &SqliteDb,
    action_id: &str,
    operation: &str,
    correlation_id: &str,
) -> db::AgentAction {
    AgentActionRepo::create_action(
        db,
        CreateAgentAction {
            id: action_id.to_owned(),
            actor_identity_id: AGENT_ID.to_owned(),
            scope_type: "project".to_owned(),
            scope_id: PROJECT_ID.to_owned(),
            operation: operation.to_owned(),
            payload_json: "{}".to_owned(),
            payload_hash: format!("hash-{action_id}"),
            dedupe_key: format!("dedupe-{action_id}"),
            correlation_id: correlation_id.to_owned(),
            causation_id: None,
            causation_depth: 0,
            requested_permission: format!("{operation}:execute"),
            policy_result: AgentActionPolicyResult::Allowed,
            policy_reason: None,
            status: AgentActionStatus::Proposed,
            target_type: Some("project".to_owned()),
            target_id: Some(PROJECT_ID.to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("native action creates")
}

fn action_execution(
    action: &db::AgentAction,
    execution_id: &str,
    key: &str,
    outcome: &Value,
) -> CreateAgentActionExecution {
    CreateAgentActionExecution {
        id: execution_id.to_owned(),
        action_id: action.id.clone(),
        expected_action_version: action.version,
        attempt: 1,
        status: AgentActionExecutionStatus::Succeeded,
        result_json: Some(outcome.to_string()),
        error: None,
        executed_by_type: "agent".to_owned(),
        executed_by_id: AGENT_ID.to_owned(),
        idempotency_key: key.to_owned(),
        action_status: AgentActionStatus::Executed,
        action_outcome_json: Some(outcome.to_string()),
        created_at: NOW.to_owned(),
        completed_at: Some(NOW.to_owned()),
        updated_at: NOW.to_owned(),
    }
}

fn milestone_revision(
    id: &str,
    milestone_id: &str,
    expected_version: i64,
    base_revision: i64,
    base_revision_id: Option<&str>,
    lifecycle: &str,
) -> CreateProjectMilestoneRevision {
    CreateProjectMilestoneRevision {
        id: id.to_owned(),
        milestone_id: milestone_id.to_owned(),
        expected_milestone_version: expected_version,
        base_revision,
        base_revision_id: base_revision_id.map(str::to_owned),
        lifecycle: lifecycle.to_owned(),
        display_label: Some("Release milestone".to_owned()),
        outcome: "The release outcome is delivered".to_owned(),
        included_scope_json: "[]".to_owned(),
        excluded_scope_json: "[]".to_owned(),
        charter_revision_id: None,
        document_revisions_json: "[]".to_owned(),
        task_selection_json: "[]".to_owned(),
        dependencies_json: "[]".to_owned(),
        risks_json: "[]".to_owned(),
        acceptance_checks_json: "[]".to_owned(),
        evidence_requirements_json: "[]".to_owned(),
        known_issues_json: "[]".to_owned(),
        change_summary: "command acceptance".to_owned(),
        schema_version: "milestone@1".to_owned(),
        render_version: "milestone-render@1".to_owned(),
        rendered_view: "# Release milestone".to_owned(),
        content_digest: format!("content-{id}"),
        rendered_digest: format!("rendered-{id}"),
        author_type: "user".to_owned(),
        author_id: Some(USER_ID.to_owned()),
        source_refs_json: "[]".to_owned(),
        created_at: NOW.to_owned(),
    }
}

fn readiness_snapshot(id: &str, key: &str) -> CreateProjectReadinessSnapshotCommand {
    CreateProjectReadinessSnapshotCommand {
        snapshot: CreateProjectReadinessSnapshot {
            id: id.to_owned(),
            project_id: PROJECT_ID.to_owned(),
            milestone_id: MILESTONE_ID.to_owned(),
            definition_revision_id: MILESTONE_REVISION_ID.to_owned(),
            baseline_id: BASELINE_ID.to_owned(),
            baseline_revision_id: BASELINE_REVISION_ID.to_owned(),
            baseline_digest: "baseline-digest-1".to_owned(),
            release_policy_revision: "policy@1".to_owned(),
            release_policy_digest: baseline_release_policy().1,
            input_manifest_json: "[]".to_owned(),
            event_watermark: "watermark-1".to_owned(),
            outcome: "ready".to_owned(),
            blocking_reasons_json: "[]".to_owned(),
            blocker_projection_json: "[]".to_owned(),
            stale_projection_json: "[]".to_owned(),
            reconciliation_projection_json: "[]".to_owned(),
            check_results_json: "[]".to_owned(),
            waiver_manifest_json: "[]".to_owned(),
            evidence_manifest_json: "[]".to_owned(),
            commit_context_json: "{}".to_owned(),
            computing_policy_revision: "forge.readiness.compute/v1".to_owned(),
            readiness_digest: "readiness-digest-1".to_owned(),
            principal_type: "user".to_owned(),
            principal_id: USER_ID.to_owned(),
            authorization_basis: "explicit user authorization".to_owned(),
            authorization_action: "project.milestone.readiness".to_owned(),
            authorization_occurred_at: db::now_rfc3339(),
            expected_milestone_version: 1,
            explicit_event: "readiness-evaluated".to_owned(),
            idempotency_key: key.to_owned(),
            created_at: NOW.to_owned(),
        },
        command_receipt: None,
        action_execution: None,
    }
}

fn readiness_command(key: &str) -> ProjectReadinessRequestCommand {
    ProjectReadinessRequestCommand {
        project_id: PROJECT_ID.to_owned(),
        milestone_id: MILESTONE_ID.to_owned(),
        expected_milestone_version: 1,
        baseline_id: BASELINE_ID.to_owned(),
        baseline_revision_id: BASELINE_REVISION_ID.to_owned(),
        release_policy_revision: "policy@1".to_owned(),
        idempotency_key: key.to_owned(),
        authenticated_user_id: Some(USER_ID.to_owned()),
        authorization: user_authorization("project.milestone.readiness", key),
    }
}

fn release_command(
    key: &str,
    snapshot: &ProjectReadinessSnapshotRecord,
) -> ProjectReleaseRequestCommand {
    ProjectReleaseRequestCommand {
        project_id: PROJECT_ID.to_owned(),
        milestone_id: MILESTONE_ID.to_owned(),
        expected_milestone_version: 2,
        readiness_snapshot_id: snapshot.id.clone(),
        readiness_digest: snapshot.readiness_digest.clone(),
        status: "pending_user_release_approval".to_owned(),
        idempotency_key: key.to_owned(),
        authorization: agent_authorization("project.milestone.release.request", key),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn milestone_service_is_race_safe_materializes_checks_and_replays_frozen_state() {
    let db = fixture().await;
    let service = ProjectMilestoneCommandService::new(Arc::clone(&db));
    let define = definition_command(DefinitionCommandInput {
        project_version: 1,
        milestone_id: None,
        milestone_version: 1,
        base_revision_id: None,
        lifecycle: MilestoneDefinitionLifecycle::Proposed,
        key: "service-define-race",
        name: "Service milestone",
        check_id: Some("service-check"),
    });
    let service_a = service.clone();
    let service_b = service.clone();
    let (first, second) = tokio::join!(
        service_a.define_milestone(define.clone(), None),
        service_b.define_milestone(define.clone(), None),
    );
    let first = first.expect("first service define");
    let second = second.expect("racing service define replays");
    assert_eq!(second, first);
    let milestone_key: String = sqlx::query_scalar(
        "SELECT milestone_key FROM project_milestone WHERE id = ? AND project_id = ?",
    )
    .bind(&first.milestone_id)
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("allocated milestone key");
    assert_eq!(milestone_key, "M002");
    let materialized_check: (String, String, i64) = sqlx::query_as(
        "SELECT definition_revision_id, source_kind, version
         FROM project_milestone_check WHERE id = 'service-check'",
    )
    .fetch_one(db.pool())
    .await
    .expect("proposed acceptance check materializes");
    assert_eq!(materialized_check.0, first.id);
    assert_eq!(materialized_check.1, "manual");
    assert_eq!(materialized_check.2, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE scope_id = ? AND operation = ? AND idempotency_key = ?",
        )
        .bind(PROJECT_ID)
        .bind(PROJECT_MILESTONE_OPERATION)
        .bind("service-define-race")
        .fetch_one(db.pool())
        .await
        .expect("define receipt count"),
        1
    );

    let primary = ProjectPrimaryMilestoneCommand {
        project_id: PROJECT_ID.to_owned(),
        primary_milestone_id: Some(first.milestone_id.clone()),
        expected_project_version: 2,
        idempotency_key: "service-primary".to_owned(),
        authorization: user_authorization("project.milestone.primary.set", "service-primary"),
    };
    let frozen = service
        .set_primary_milestone(primary.clone(), None)
        .await
        .expect("primary milestone command");
    sqlx::query("UPDATE project SET name = 'Later name', version = version + 1 WHERE id = ?")
        .bind(PROJECT_ID)
        .execute(db.pool())
        .await
        .expect("later Project mutation");
    let replay = service
        .set_primary_milestone(primary, None)
        .await
        .expect("primary milestone frozen replay");
    assert_eq!(replay, frozen);
    assert_ne!(
        ProjectRepo::get_by_id(&*db, PROJECT_ID)
            .await
            .expect("Project lookup")
            .expect("Project")
            .name,
        frozen.name
    );
    assert_eq!(
        service
            .define_milestone(define.clone(), None)
            .await
            .expect("define replay after mutable Project changes"),
        first
    );
    let mut changed = define;
    changed.content.outcome = "changed outcome".to_owned();
    changed.rendered_view =
        api_types::canonical_json(&changed.content).expect("changed canonical content");
    assert!(matches!(
        service.define_milestone(changed, None).await,
        Err(ServiceError::Db(DbError::IdempotencyConflict))
    ));

    let project_version: i64 = sqlx::query_scalar("SELECT version FROM project WHERE id = ?")
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("current Project version");
    let draft = service
        .define_milestone(
            definition_command(DefinitionCommandInput {
                project_version,
                milestone_id: None,
                milestone_version: 1,
                base_revision_id: None,
                lifecycle: MilestoneDefinitionLifecycle::Draft,
                key: "service-draft",
                name: "Draft milestone",
                check_id: Some("draft-check"),
            }),
            None,
        )
        .await
        .expect("draft define");
    assert_eq!(draft.lifecycle, "draft");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_milestone_check WHERE id = 'draft-check'",
        )
        .fetch_one(db.pool())
        .await
        .expect("draft check count"),
        0
    );
    let proposed = service
        .revise_milestone(
            definition_command(DefinitionCommandInput {
                project_version: 0,
                milestone_id: Some(&draft.milestone_id),
                milestone_version: 2,
                base_revision_id: Some(&draft.id),
                lifecycle: MilestoneDefinitionLifecycle::Proposed,
                key: "service-draft-propose",
                name: "Draft milestone proposed",
                check_id: Some("draft-check"),
            }),
            None,
        )
        .await
        .expect("draft-first milestone can be proposed");
    assert_eq!(
        proposed.base_revision_id.as_deref(),
        Some(draft.id.as_str())
    );
    assert_eq!(proposed.lifecycle, "proposed");
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT definition_revision_id FROM project_milestone_check
             WHERE id = 'draft-check'",
        )
        .fetch_one(db.pool())
        .await
        .expect("proposed draft check materializes"),
        proposed.id
    );
}

#[tokio::test]
async fn milestone_define_revise_and_primary_share_receipts_and_replay_contract() {
    let db = fixture().await;

    let define_outcome = json!({
        "operation": PROJECT_MILESTONE_OPERATION,
        "project_id": PROJECT_ID,
        "milestone_id": "direct-milestone",
        "revision_id": "direct-milestone-revision",
    });
    let define_receipt = receipt(ReceiptInput {
        operation: PROJECT_MILESTONE_OPERATION,
        principal_type: "user",
        principal_id: USER_ID,
        key: "direct-define",
        digest: "digest-direct-define",
        correlation_id: "direct-define-correlation",
        outcome: &define_outcome,
        execution_id: None,
    });
    let define = CreateProjectMilestoneCommand {
        milestone: CreateProjectMilestone {
            id: "direct-milestone".to_owned(),
            project_id: PROJECT_ID.to_owned(),
            expected_project_version: 1,
            milestone_sequence: 2,
            milestone_key: "M002".to_owned(),
            display_label: Some("Direct milestone".to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
        revision: milestone_revision(
            "direct-milestone-revision",
            "direct-milestone",
            1,
            0,
            None,
            "proposed",
        ),
        allocate_project_sequence: false,
        check_definitions: Vec::new(),
        command_receipt: Some(define_receipt),
        action_execution: None,
    };
    let created = ProjectOrchestrationRepo::create_project_milestone_command(&*db, define)
        .await
        .expect("direct milestone define");
    assert_eq!(created.revision, 1);

    let revise_outcome = json!({
        "operation": PROJECT_MILESTONE_OPERATION,
        "project_id": PROJECT_ID,
        "milestone_id": "direct-milestone",
        "revision_id": "direct-milestone-revision-2",
    });
    let revise = db::AppendProjectMilestoneRevisionCommand {
        revision: milestone_revision(
            "direct-milestone-revision-2",
            "direct-milestone",
            2,
            1,
            Some("direct-milestone-revision"),
            "proposed",
        ),
        check_definitions: Vec::new(),
        command_receipt: Some(receipt(ReceiptInput {
            operation: PROJECT_MILESTONE_OPERATION,
            principal_type: "user",
            principal_id: USER_ID,
            key: "direct-revise",
            digest: "digest-direct-revise",
            correlation_id: "direct-revise-correlation",
            outcome: &revise_outcome,
            execution_id: None,
        })),
        action_execution: None,
    };
    let revised = ProjectOrchestrationRepo::append_project_milestone_revision_command(&*db, revise)
        .await
        .expect("direct milestone revise");
    assert_eq!(revised.revision, 2);

    let primary_outcome = json!({
        "operation": PROJECT_MILESTONE_OPERATION,
        "project_id": PROJECT_ID,
        "primary_milestone_id": "direct-milestone",
    });
    let primary = db::SetPrimaryProjectMilestoneCommand {
        project_id: PROJECT_ID.to_owned(),
        primary_milestone_id: Some("direct-milestone".to_owned()),
        expected_project_version: 2,
        principal_type: "user".to_owned(),
        principal_id: USER_ID.to_owned(),
        authorization_basis: "explicit user authorization".to_owned(),
        authorization_action: "project.milestone.primary.set".to_owned(),
        authorization_occurred_at: db::now_rfc3339(),
        explicit_event: "primary-set".to_owned(),
        idempotency_key: "direct-primary".to_owned(),
        updated_at: NOW.to_owned(),
        command_receipt: Some(receipt(ReceiptInput {
            operation: PROJECT_MILESTONE_OPERATION,
            principal_type: "user",
            principal_id: USER_ID,
            key: "direct-primary",
            digest: "digest-direct-primary",
            correlation_id: "direct-primary-correlation",
            outcome: &primary_outcome,
            execution_id: None,
        })),
        action_execution: None,
    };
    let project =
        ProjectOrchestrationRepo::set_primary_project_milestone_command(&*db, primary.clone())
            .await
            .expect("direct set primary");
    assert_eq!(
        project.primary_milestone_id.as_deref(),
        Some("direct-milestone")
    );
    let replay = ProjectOrchestrationRepo::set_primary_project_milestone_command(&*db, primary)
        .await
        .expect("direct set primary replay");
    assert_eq!(replay, project);

    let stored = CommandReceiptRepo::get_command_receipt(
        &*db,
        "user",
        USER_ID,
        "project",
        PROJECT_ID,
        PROJECT_MILESTONE_OPERATION,
        "direct-primary",
        "digest-direct-primary",
    )
    .await
    .expect("primary receipt lookup")
    .expect("primary receipt");
    let event = DomainEventRepo::get_event(&*db, &stored.event_id)
        .await
        .expect("primary event lookup")
        .expect("primary event");
    assert_eq!(event.actor_type, "user");
    assert_eq!(event.actor_id.as_deref(), Some(USER_ID));
    assert_eq!(event.correlation_id, stored.correlation_id);
    assert_eq!(event.causation_id, stored.causation_id);
    assert_eq!(event.causation_depth, stored.causation_depth);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt WHERE scope_id = ? AND operation = ?",
        )
        .bind(PROJECT_ID)
        .bind(PROJECT_MILESTONE_OPERATION)
        .fetch_one(db.pool())
        .await
        .expect("milestone receipt count"),
        3
    );
}

#[tokio::test]
async fn native_milestone_and_readiness_release_commands_replay_after_state_changes() {
    let db = fixture().await;

    let milestone_action = action(
        &db,
        "native-milestone-action",
        PROJECT_MILESTONE_OPERATION,
        "native-milestone-correlation",
    )
    .await;
    let define_outcome = json!({
        "operation": PROJECT_MILESTONE_OPERATION,
        "project_id": PROJECT_ID,
        "milestone_id": "native-milestone",
        "revision_id": "native-milestone-revision",
    });
    let define_key = "native-milestone-define";
    let define_execution_id = "native-milestone-execution";
    let define_receipt = receipt(ReceiptInput {
        operation: PROJECT_MILESTONE_OPERATION,
        principal_type: "agent",
        principal_id: AGENT_ID,
        key: define_key,
        digest: "digest-native-milestone",
        correlation_id: "native-milestone-correlation",
        outcome: &define_outcome,
        execution_id: Some(define_execution_id),
    });
    let define_execution = action_execution(
        &milestone_action,
        define_execution_id,
        define_key,
        &define_outcome,
    );
    let define_input = CreateProjectMilestoneCommand {
        milestone: CreateProjectMilestone {
            id: "native-milestone".to_owned(),
            project_id: PROJECT_ID.to_owned(),
            expected_project_version: 1,
            milestone_sequence: 2,
            milestone_key: "M002".to_owned(),
            display_label: Some("Native milestone".to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
        revision: milestone_revision(
            "native-milestone-revision",
            "native-milestone",
            1,
            0,
            None,
            "proposed",
        ),
        allocate_project_sequence: false,
        check_definitions: Vec::new(),
        command_receipt: Some(define_receipt.clone()),
        action_execution: Some(define_execution.clone()),
    };
    let defined =
        ProjectOrchestrationRepo::create_project_milestone_command(&*db, define_input.clone())
            .await
            .expect("native milestone define");
    let replay = ProjectOrchestrationRepo::create_project_milestone_command(&*db, define_input)
        .await
        .expect("native milestone replay");
    assert_eq!(replay, defined);

    let stored = CommandReceiptRepo::get_command_receipt(
        &*db,
        "agent",
        AGENT_ID,
        "project",
        PROJECT_ID,
        PROJECT_MILESTONE_OPERATION,
        define_key,
        "digest-native-milestone",
    )
    .await
    .expect("native milestone receipt lookup")
    .expect("native milestone receipt");
    assert_eq!(
        stored.agent_action_execution_id.as_deref(),
        Some(define_execution_id)
    );
    let event = DomainEventRepo::get_event(&*db, &stored.event_id)
        .await
        .expect("native milestone event lookup")
        .expect("native milestone event");
    assert_eq!(event.actor_type, "agent");
    assert_eq!(event.actor_id.as_deref(), Some(AGENT_ID));
    assert_eq!(event.correlation_id, "native-milestone-correlation");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_action_execution WHERE id = ?",)
            .bind(define_execution_id)
            .fetch_one(db.pool())
            .await
            .expect("native execution count"),
        1
    );

    sqlx::query("UPDATE project_milestone SET version = 99 WHERE id = ?")
        .bind("native-milestone")
        .execute(db.pool())
        .await
        .expect("mutable milestone state changes");
    let mut changed_digest = define_input_for_conflict(&defined, &stored);
    changed_digest.input_digest = "different-digest".to_owned();
    let conflict = ProjectOrchestrationRepo::create_project_milestone_command(
        &*db,
        changed_digest.into_command(),
    )
    .await;
    assert!(matches!(conflict, Err(DbError::IdempotencyConflict)));

    let mut changed_principal = define_input_for_conflict(&defined, &stored);
    changed_principal.principal_id_for_test = Some("other-agent".to_owned());
    let conflict = ProjectOrchestrationRepo::create_project_milestone_command(
        &*db,
        changed_principal.into_command(),
    )
    .await;
    assert!(matches!(conflict, Err(DbError::IdempotencyConflict)));

    // A direct user readiness command is the existing user-side materializer
    // contract.  The native release request below proves the optional action
    // execution linkage on the request/event materialization boundary.
    let readiness_key = "direct-readiness";
    let readiness_outcome = json!({
        "operation": PROJECT_READINESS_OPERATION,
        "project_id": PROJECT_ID,
        "milestone_id": MILESTONE_ID,
        "readiness_snapshot_id": "readiness-command-snapshot",
    });
    let mut readiness = readiness_snapshot("readiness-command-snapshot", readiness_key);
    readiness.snapshot.event_watermark = sqlx::query_scalar(
        "SELECT COALESCE(
            (SELECT id FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type != 'milestone.readiness.evaluated'
             ORDER BY sequence DESC LIMIT 1),
            'none')",
    )
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("readiness source watermark");
    readiness.command_receipt = Some(receipt(ReceiptInput {
        operation: PROJECT_READINESS_OPERATION,
        principal_type: "user",
        principal_id: USER_ID,
        key: readiness_key,
        digest: "digest-direct-readiness",
        correlation_id: "direct-readiness-correlation",
        outcome: &readiness_outcome,
        execution_id: None,
    }));
    let snapshot = ProjectOrchestrationRepo::create_project_readiness_snapshot_command(
        &*db,
        readiness.clone(),
    )
    .await
    .expect("direct readiness command");
    assert_eq!(snapshot.outcome, "ready");
    sqlx::query("UPDATE project_milestone SET version = 77 WHERE id = ?")
        .bind(MILESTONE_ID)
        .execute(db.pool())
        .await
        .expect("readiness mutable state changes");
    let snapshot_replay =
        ProjectOrchestrationRepo::create_project_readiness_snapshot_command(&*db, readiness)
            .await
            .expect("direct readiness replay");
    assert_eq!(snapshot_replay, snapshot);

    let release_action = action(
        &db,
        "native-release-action",
        PROJECT_RELEASE_OPERATION,
        "native-release-correlation",
    )
    .await;
    let release_key = "native-release-request";
    let release_event_id = "native-release-event";
    let release_outcome = json!({
        "operation": PROJECT_RELEASE_OPERATION,
        "project_id": PROJECT_ID,
        "milestone_id": MILESTONE_ID,
        "readiness_snapshot_id": "readiness-command-snapshot",
        "candidate_event_id": release_event_id,
        "status": "pending_user_release_approval",
    });
    // Restore only the expected version needed by the release request.  The
    // request contract still verifies the ready lifecycle and snapshot digest.
    sqlx::query(
        "UPDATE project_milestone SET version = 2, lifecycle = 'ready_for_release'
         WHERE id = ?",
    )
    .bind(MILESTONE_ID)
    .execute(db.pool())
    .await
    .expect("ready milestone state");
    let release_receipt = receipt(ReceiptInput {
        operation: PROJECT_RELEASE_OPERATION,
        principal_type: "agent",
        principal_id: AGENT_ID,
        key: release_key,
        digest: "digest-native-release",
        correlation_id: "native-release-correlation",
        outcome: &release_outcome,
        execution_id: Some("native-release-execution"),
    });
    let release_execution = action_execution(
        &release_action,
        "native-release-execution",
        release_key,
        &release_outcome,
    );
    let release = CreateProjectReleaseRequestCommand {
        request: CreateProjectReleaseRequest {
            event_id: release_event_id.to_owned(),
            project_id: PROJECT_ID.to_owned(),
            milestone_id: MILESTONE_ID.to_owned(),
            expected_milestone_version: 2,
            readiness_snapshot_id: "readiness-command-snapshot".to_owned(),
            readiness_digest: "readiness-digest-1".to_owned(),
            status: "pending_user_release_approval".to_owned(),
            idempotency_key: release_key.to_owned(),
            created_at: NOW.to_owned(),
        },
        command_receipt: Some(release_receipt.clone()),
        action_execution: Some(release_execution.clone()),
    };
    let request =
        ProjectOrchestrationRepo::create_project_release_request_command(&*db, release.clone())
            .await
            .expect("native release request");
    assert_eq!(request.status, "pending_user_release_approval");
    let request_replay =
        ProjectOrchestrationRepo::create_project_release_request_command(&*db, release)
            .await
            .expect("native release request replay");
    assert_eq!(request_replay, request);
    let stored_release = CommandReceiptRepo::get_command_receipt(
        &*db,
        "agent",
        AGENT_ID,
        "project",
        PROJECT_ID,
        PROJECT_RELEASE_OPERATION,
        release_key,
        "digest-native-release",
    )
    .await
    .expect("release receipt lookup")
    .expect("release receipt");
    assert_eq!(
        stored_release.agent_action_execution_id.as_deref(),
        Some("native-release-execution")
    );
    let release_event = DomainEventRepo::get_event(&*db, &stored_release.event_id)
        .await
        .expect("release event lookup")
        .expect("release event");
    assert_eq!(
        release_event.event_type,
        "project_release.candidate_requested"
    );
    assert_eq!(release_event.actor_type, "agent");
    assert_eq!(release_event.actor_id.as_deref(), Some(AGENT_ID));
    assert_eq!(release_event.correlation_id, "native-release-correlation");

    sqlx::query("UPDATE project_milestone SET version = 100 WHERE id = ?")
        .bind(MILESTONE_ID)
        .execute(db.pool())
        .await
        .expect("release mutable state changes");
    let mut changed_release = release_input_for_conflict(&request, &stored_release);
    changed_release.input_digest = "different-release-digest".to_owned();
    let conflict = ProjectOrchestrationRepo::create_project_release_request_command(
        &*db,
        changed_release.into_command(),
    )
    .await;
    assert!(matches!(conflict, Err(DbError::IdempotencyConflict)));
}

#[tokio::test]
async fn milestone_define_receipt_failpoint_rolls_back_checks_pointer_and_version() {
    let db = fixture().await;
    let command = definition_command(DefinitionCommandInput {
        project_version: 1,
        milestone_id: None,
        milestone_version: 1,
        base_revision_id: None,
        lifecycle: MilestoneDefinitionLifecycle::Proposed,
        key: "failpoint-milestone-define",
        name: "Failpoint milestone",
        check_id: Some("failpoint-define-check"),
    });
    let service = ProjectMilestoneCommandService::new(Arc::clone(&db));
    let before: (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT version FROM project WHERE id = ?),
            (SELECT COUNT(*) FROM project_milestone WHERE project_id = ?),
            (SELECT COUNT(*) FROM project_milestone_revision WHERE milestone_id = ?),
            (SELECT COUNT(*) FROM project_milestone_check WHERE project_id = ?),
            (SELECT COUNT(*) FROM command_receipt WHERE operation = ? AND scope_id = ?)",
    )
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .bind(MILESTONE_ID)
    .bind(PROJECT_ID)
    .bind(PROJECT_MILESTONE_OPERATION)
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("define pre-state");
    assert_eq!(before, (1, 1, 1, 0, 0));

    arm_receipt_failpoint(
        &db,
        "milestone_define_receipt_failpoint",
        "milestone define receipt failpoint",
    )
    .await;
    let failed = service
        .define_milestone(command.clone(), None)
        .await
        .expect_err("receipt failpoint aborts milestone define");
    assert!(failed.to_string().contains("failpoint"));
    drop_receipt_failpoint(&db, "milestone_define_receipt_failpoint").await;

    let after_failure: (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT version FROM project WHERE id = ?),
            (SELECT COUNT(*) FROM project_milestone WHERE project_id = ?),
            (SELECT COUNT(*) FROM project_milestone_revision WHERE milestone_id = ?),
            (SELECT COUNT(*) FROM project_milestone_check WHERE project_id = ?),
            (SELECT COUNT(*) FROM command_receipt WHERE operation = ? AND scope_id = ?)",
    )
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .bind(MILESTONE_ID)
    .bind(PROJECT_ID)
    .bind(PROJECT_MILESTONE_OPERATION)
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("define post-failure state");
    assert_eq!(after_failure, before);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'milestone.definition.created'",
        )
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("define event absence"),
        0
    );

    let first = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .define_milestone(command.clone(), None)
        .await
        .expect("define retry after receipt failpoint");
    assert_eq!(first.revision, 1);
    assert_eq!(first.lifecycle, "proposed");
    let pointer: (String, i64) = sqlx::query_as(
        "SELECT current_definition_revision_id, version
         FROM project_milestone WHERE id = ?",
    )
    .bind(&first.milestone_id)
    .fetch_one(db.pool())
    .await
    .expect("defined milestone pointer");
    assert_eq!(pointer, (first.id.clone(), 2));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_milestone_check
             WHERE project_id = ? AND definition_revision_id = ?",
        )
        .bind(PROJECT_ID)
        .bind(&first.id)
        .fetch_one(db.pool())
        .await
        .expect("defined acceptance check"),
        1
    );

    // Simulate a lost response and a restarted service. The receipt is the
    // only source of the returned immutable revision, so no second shell,
    // revision, check, pointer update, or event may appear.
    let replay = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .define_milestone(command, None)
        .await
        .expect("define replay after service recreation");
    assert_eq!(replay, first);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_milestone
             WHERE project_id = ?",
        )
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("define milestone count"),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE scope_id = ? AND operation = ? AND idempotency_key = ?",
        )
        .bind(PROJECT_ID)
        .bind(PROJECT_MILESTONE_OPERATION)
        .bind("failpoint-milestone-define")
        .fetch_one(db.pool())
        .await
        .expect("define receipt count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'milestone.definition.created'",
        )
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("define event count"),
        1
    );
}

#[tokio::test]
async fn milestone_revise_receipt_failpoint_rolls_back_checks_pointer_and_version() {
    let db = fixture().await;
    let command = definition_command(DefinitionCommandInput {
        project_version: 0,
        milestone_id: Some(MILESTONE_ID),
        milestone_version: 1,
        base_revision_id: Some(MILESTONE_REVISION_ID),
        lifecycle: MilestoneDefinitionLifecycle::Proposed,
        key: "failpoint-milestone-revise",
        name: "Failpoint revision",
        check_id: Some("failpoint-revise-check"),
    });
    let before: (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT version FROM project_milestone WHERE id = ?),
            (SELECT COUNT(*) FROM project_milestone_revision WHERE milestone_id = ?),
            (SELECT COUNT(*) FROM project_milestone_check WHERE project_id = ?),
            (SELECT COUNT(*) FROM command_receipt WHERE operation = ? AND scope_id = ?),
            (SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'milestone.definition.revised')",
    )
    .bind(MILESTONE_ID)
    .bind(MILESTONE_ID)
    .bind(PROJECT_ID)
    .bind(PROJECT_MILESTONE_OPERATION)
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("revise pre-state");
    assert_eq!(before, (1, 1, 0, 0, 0));

    arm_receipt_failpoint(
        &db,
        "milestone_revise_receipt_failpoint",
        "milestone revise receipt failpoint",
    )
    .await;
    let failed = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .revise_milestone(command.clone(), None)
        .await
        .expect_err("receipt failpoint aborts milestone revise");
    assert!(failed.to_string().contains("failpoint"));
    drop_receipt_failpoint(&db, "milestone_revise_receipt_failpoint").await;

    let after_failure: (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT version FROM project_milestone WHERE id = ?),
            (SELECT COUNT(*) FROM project_milestone_revision WHERE milestone_id = ?),
            (SELECT COUNT(*) FROM project_milestone_check WHERE project_id = ?),
            (SELECT COUNT(*) FROM command_receipt WHERE operation = ? AND scope_id = ?),
            (SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'milestone.definition.revised')",
    )
    .bind(MILESTONE_ID)
    .bind(MILESTONE_ID)
    .bind(PROJECT_ID)
    .bind(PROJECT_MILESTONE_OPERATION)
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("revise post-failure state");
    assert_eq!(after_failure, before);

    let first = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .revise_milestone(command.clone(), None)
        .await
        .expect("revise retry after receipt failpoint");
    assert_eq!(first.revision, 2);
    let pointer: (String, i64) = sqlx::query_as(
        "SELECT current_definition_revision_id, version
         FROM project_milestone WHERE id = ?",
    )
    .bind(MILESTONE_ID)
    .fetch_one(db.pool())
    .await
    .expect("revised milestone pointer");
    assert_eq!(pointer, (first.id.clone(), 2));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_milestone_check
             WHERE project_id = ? AND definition_revision_id = ?",
        )
        .bind(PROJECT_ID)
        .bind(&first.id)
        .fetch_one(db.pool())
        .await
        .expect("revised acceptance check"),
        1
    );

    sqlx::query("UPDATE project_milestone SET version = 9 WHERE id = ?")
        .bind(MILESTONE_ID)
        .execute(db.pool())
        .await
        .expect("mutate milestone after revise");
    let replay = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .revise_milestone(command, None)
        .await
        .expect("revise replay after service recreation");
    assert_eq!(replay, first);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_milestone_revision WHERE milestone_id = ?",
        )
        .bind(MILESTONE_ID)
        .fetch_one(db.pool())
        .await
        .expect("revise revision count"),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE scope_id = ? AND operation = ? AND idempotency_key = ?",
        )
        .bind(PROJECT_ID)
        .bind(PROJECT_MILESTONE_OPERATION)
        .bind("failpoint-milestone-revise")
        .fetch_one(db.pool())
        .await
        .expect("revise receipt count"),
        1
    );
}

#[tokio::test]
async fn milestone_primary_receipt_failpoint_rolls_back_pointer_and_version() {
    let db = fixture().await;
    let command = ProjectPrimaryMilestoneCommand {
        project_id: PROJECT_ID.to_owned(),
        primary_milestone_id: Some(MILESTONE_ID.to_owned()),
        expected_project_version: 1,
        idempotency_key: "failpoint-milestone-primary".to_owned(),
        authorization: user_authorization(
            "project.milestone.primary.set",
            "failpoint-milestone-primary",
        ),
    };
    let before: (i64, Option<String>, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT version FROM project WHERE id = ?),
            (SELECT primary_milestone_id FROM project WHERE id = ?),
            (SELECT COUNT(*) FROM command_receipt WHERE operation = ? AND scope_id = ?),
            (SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'milestone.primary.set')",
    )
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .bind(PROJECT_MILESTONE_OPERATION)
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("primary pre-state");
    assert_eq!(before, (1, None, 0, 0));

    arm_receipt_failpoint(
        &db,
        "milestone_primary_receipt_failpoint",
        "milestone primary receipt failpoint",
    )
    .await;
    let failed = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .set_primary_milestone(command.clone(), None)
        .await
        .expect_err("receipt failpoint aborts primary milestone set");
    assert!(failed.to_string().contains("failpoint"));
    drop_receipt_failpoint(&db, "milestone_primary_receipt_failpoint").await;

    let after_failure: (i64, Option<String>, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT version FROM project WHERE id = ?),
            (SELECT primary_milestone_id FROM project WHERE id = ?),
            (SELECT COUNT(*) FROM command_receipt WHERE operation = ? AND scope_id = ?),
            (SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'milestone.primary.set')",
    )
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .bind(PROJECT_MILESTONE_OPERATION)
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("primary post-failure state");
    assert_eq!(after_failure, before);

    let first = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .set_primary_milestone(command.clone(), None)
        .await
        .expect("primary retry after receipt failpoint");
    assert_eq!(first.version, 2);
    assert_eq!(first.primary_milestone_id.as_deref(), Some(MILESTONE_ID));

    sqlx::query("UPDATE project SET name = 'Mutated after primary' WHERE id = ?")
        .bind(PROJECT_ID)
        .execute(db.pool())
        .await
        .expect("mutate primary project after commit");
    let replay = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .set_primary_milestone(command, None)
        .await
        .expect("primary replay after service recreation");
    assert_eq!(replay, first);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE scope_id = ? AND operation = ?
               AND idempotency_key = 'failpoint-milestone-primary'",
        )
        .bind(PROJECT_ID)
        .bind(PROJECT_MILESTONE_OPERATION)
        .fetch_one(db.pool())
        .await
        .expect("primary receipt count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'milestone.primary.set'",
        )
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("primary event count"),
        1
    );
}

#[tokio::test]
async fn readiness_freshness_rechecks_inputs_not_project_event_watermarks() {
    let db = fixture().await;
    let mut readiness_request = readiness_command("freshness-recheck");
    readiness_request.authorization.authorization_occurred_at = db::now_rfc3339();
    let readiness = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .request_readiness(readiness_request, None)
        .await
        .expect("readiness candidate persists");
    assert_eq!(readiness.outcome, "ready");

    let runtime = MilestoneRuntime::new(Arc::clone(&db));
    let fresh = runtime
        .readiness_freshness(PROJECT_ID, MILESTONE_ID, &readiness.id)
        .await
        .expect("fresh candidate rechecks");
    assert_eq!(
        fresh.status,
        api_types::ReadinessFreshnessStatus::Current,
        "the one-step readiness transition is accepted"
    );

    // Both a Project Agent release recommendation and an unrelated Project
    // event are attention/diagnostic records. Neither changes the governed
    // definition, baseline, policy, checks, evidence, waivers, Tasks,
    // Documents, or repository context.
    for (event_id, event_type) in [
        (
            "freshness-candidate-event",
            "project_release.candidate_requested",
        ),
        ("freshness-unrelated-event", "project.note.recorded"),
    ] {
        DomainEventRepo::append_event(
            &*db,
            CreateDomainEvent {
                id: event_id.to_owned(),
                event_type: event_type.to_owned(),
                entity_type: "project".to_owned(),
                entity_id: PROJECT_ID.to_owned(),
                actor_type: if event_type == "project_release.candidate_requested" {
                    "agent".to_owned()
                } else {
                    "user".to_owned()
                },
                actor_id: Some(if event_type == "project_release.candidate_requested" {
                    AGENT_ID.to_owned()
                } else {
                    USER_ID.to_owned()
                }),
                scope_type: "project".to_owned(),
                scope_id: PROJECT_ID.to_owned(),
                correlation_id: format!("correlation-{event_id}"),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: Some(format!("dedupe-{event_id}")),
                payload_json: "{}".to_owned(),
                created_at: NOW.to_owned(),
            },
        )
        .await
        .expect("diagnostic event appends");
    }
    let fresh_after_diagnostic_events = runtime
        .readiness_freshness(PROJECT_ID, MILESTONE_ID, &readiness.id)
        .await
        .expect("diagnostic events do not stale candidate");
    assert_eq!(
        fresh_after_diagnostic_events.status,
        api_types::ReadinessFreshnessStatus::Current
    );
    assert_ne!(
        fresh_after_diagnostic_events.snapshot_source_event_watermark,
        fresh_after_diagnostic_events.current_source_event_watermark,
        "watermarks remain diagnostic and may move independently"
    );

    // A second mutable milestone transition is an exact governed input
    // change and must invalidate the overlay even though the immutable
    // readiness row remains. (Definition/baseline revisions are immutable by
    // contract; the live milestone version is the canonical freshness CAS.)
    sqlx::query(
        "UPDATE project_milestone
         SET version = version + 1, updated_at = ?
         WHERE id = ?",
    )
    .bind(db::now_rfc3339())
    .bind(MILESTONE_ID)
    .execute(db.pool())
    .await
    .expect("mutate governed baseline input");
    let stale = runtime
        .readiness_freshness(PROJECT_ID, MILESTONE_ID, &readiness.id)
        .await
        .expect("changed candidate rechecks");
    assert_eq!(stale.status, api_types::ReadinessFreshnessStatus::Stale);
}

#[tokio::test]
async fn released_correction_freshness_accepts_terminal_version_without_increment() {
    let db = fixture().await;
    let mut initial_request = readiness_command("released-correction-initial");
    initial_request.authorization.authorization_occurred_at = db::now_rfc3339();
    ProjectMilestoneCommandService::new(Arc::clone(&db))
        .request_readiness(initial_request, None)
        .await
        .expect("initial readiness candidate persists");
    sqlx::query(
        "UPDATE project_milestone
         SET lifecycle = 'released'
         WHERE id = ? AND version = 2",
    )
    .bind(MILESTONE_ID)
    .execute(db.pool())
    .await
    .expect("promote fixture milestone to terminal release");

    let mut correction_request = readiness_command("released-correction");
    correction_request.expected_milestone_version = 2;
    correction_request.authorization.authorization_occurred_at = db::now_rfc3339();
    let correction = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .request_readiness(correction_request, None)
        .await
        .expect("released correction persists observational readiness");
    let version: i64 = sqlx::query_scalar(
        "SELECT version FROM project_milestone WHERE id = ? AND lifecycle = 'released'",
    )
    .bind(MILESTONE_ID)
    .fetch_one(db.pool())
    .await
    .expect("released milestone version");
    assert_eq!(version, correction.expected_milestone_version);

    let freshness = MilestoneRuntime::new(Arc::clone(&db))
        .readiness_freshness(PROJECT_ID, MILESTONE_ID, &correction.id)
        .await
        .expect("released correction freshness");
    assert_eq!(
        freshness.status,
        api_types::ReadinessFreshnessStatus::Current
    );
}

#[tokio::test]
async fn readiness_receipt_failpoint_rolls_back_snapshot_lifecycle_and_version() {
    let db = fixture().await;
    let command = readiness_command("failpoint-readiness");
    let before: (i64, String, i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT version FROM project_milestone WHERE id = ?),
            (SELECT lifecycle FROM project_milestone WHERE id = ?),
            (SELECT COUNT(*) FROM project_readiness_snapshot WHERE project_id = ?),
            (SELECT COUNT(*) FROM command_receipt WHERE operation = ? AND scope_id = ?),
            (SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'milestone.readiness.evaluated')",
    )
    .bind(MILESTONE_ID)
    .bind(MILESTONE_ID)
    .bind(PROJECT_ID)
    .bind(PROJECT_READINESS_OPERATION)
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("readiness pre-state");
    assert_eq!(before, (1, "active".to_owned(), 0, 0, 0));

    arm_receipt_failpoint(
        &db,
        "readiness_receipt_failpoint",
        "readiness receipt failpoint",
    )
    .await;
    let failed = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .request_readiness(command.clone(), None)
        .await
        .expect_err("receipt failpoint aborts readiness request");
    assert!(failed.to_string().contains("failpoint"));
    drop_receipt_failpoint(&db, "readiness_receipt_failpoint").await;

    let after_failure: (i64, String, i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT version FROM project_milestone WHERE id = ?),
            (SELECT lifecycle FROM project_milestone WHERE id = ?),
            (SELECT COUNT(*) FROM project_readiness_snapshot WHERE project_id = ?),
            (SELECT COUNT(*) FROM command_receipt WHERE operation = ? AND scope_id = ?),
            (SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'milestone.readiness.evaluated')",
    )
    .bind(MILESTONE_ID)
    .bind(MILESTONE_ID)
    .bind(PROJECT_ID)
    .bind(PROJECT_READINESS_OPERATION)
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("readiness post-failure state");
    assert_eq!(after_failure, before);

    let first = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .request_readiness(command.clone(), None)
        .await
        .expect("readiness retry after receipt failpoint");
    assert_eq!(first.outcome, "ready");
    let state: (i64, String) =
        sqlx::query_as("SELECT version, lifecycle FROM project_milestone WHERE id = ?")
            .bind(MILESTONE_ID)
            .fetch_one(db.pool())
            .await
            .expect("readiness committed lifecycle");
    assert_eq!(state, (2, "ready_for_release".to_owned()));

    sqlx::query("UPDATE project_milestone SET version = 8, lifecycle = 'active' WHERE id = ?")
        .bind(MILESTONE_ID)
        .execute(db.pool())
        .await
        .expect("mutate readiness milestone after commit");
    let replay = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .request_readiness(command, None)
        .await
        .expect("readiness replay after service recreation");
    assert_eq!(replay, first);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_readiness_snapshot WHERE project_id = ?",
        )
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("readiness snapshot count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE scope_id = ? AND operation = ? AND idempotency_key = 'failpoint-readiness'",
        )
        .bind(PROJECT_ID)
        .bind(PROJECT_READINESS_OPERATION)
        .fetch_one(db.pool())
        .await
        .expect("readiness receipt count"),
        1
    );
}

#[tokio::test]
async fn release_request_receipt_failpoint_rolls_back_event_without_advancing_milestone() {
    let db = fixture().await;
    let readiness = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .request_readiness(readiness_command("release-setup-readiness"), None)
        .await
        .expect("release readiness setup");
    assert_eq!(readiness.outcome, "ready");
    let command = release_command("failpoint-release-request", &readiness);
    let before: (i64, String, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT version FROM project_milestone WHERE id = ?),
            (SELECT lifecycle FROM project_milestone WHERE id = ?),
            (SELECT COUNT(*) FROM command_receipt WHERE operation = ? AND scope_id = ?),
            (SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'project_release.candidate_requested')",
    )
    .bind(MILESTONE_ID)
    .bind(MILESTONE_ID)
    .bind(PROJECT_RELEASE_OPERATION)
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("release pre-state");
    assert_eq!(before, (2, "ready_for_release".to_owned(), 0, 0));

    arm_receipt_failpoint(
        &db,
        "release_request_receipt_failpoint",
        "release request receipt failpoint",
    )
    .await;
    let failed = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .request_release(command.clone(), None)
        .await
        .expect_err("receipt failpoint aborts release request");
    assert!(failed.to_string().contains("failpoint"));
    drop_receipt_failpoint(&db, "release_request_receipt_failpoint").await;

    let after_failure: (i64, String, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT version FROM project_milestone WHERE id = ?),
            (SELECT lifecycle FROM project_milestone WHERE id = ?),
            (SELECT COUNT(*) FROM command_receipt WHERE operation = ? AND scope_id = ?),
            (SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'project_release.candidate_requested')",
    )
    .bind(MILESTONE_ID)
    .bind(MILESTONE_ID)
    .bind(PROJECT_RELEASE_OPERATION)
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("release post-failure state");
    assert_eq!(after_failure, before);

    let first = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .request_release(command.clone(), None)
        .await
        .expect("release retry after receipt failpoint");
    assert_eq!(first.status, "pending_user_release_approval");
    assert_eq!(first.milestone_id, MILESTONE_ID);
    sqlx::query("UPDATE project_milestone SET version = 7 WHERE id = ?")
        .bind(MILESTONE_ID)
        .execute(db.pool())
        .await
        .expect("mutate release milestone after commit");
    let replay = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .request_release(command, None)
        .await
        .expect("release replay after service recreation");
    assert_eq!(replay, first);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE scope_id = ? AND operation = ?
               AND idempotency_key = 'failpoint-release-request'",
        )
        .bind(PROJECT_ID)
        .bind(PROJECT_RELEASE_OPERATION)
        .fetch_one(db.pool())
        .await
        .expect("release receipt count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'project_release.candidate_requested'",
        )
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("release event count"),
        1
    );
}

// These small conflict wrappers keep the assertions above focused on the
// durable receipt identity while preserving the command's full typed input.
struct MilestoneConflictInput {
    input_digest: String,
    principal_id_for_test: Option<String>,
    command: CreateProjectMilestoneCommand,
}

impl MilestoneConflictInput {
    fn into_command(mut self) -> CreateProjectMilestoneCommand {
        if let Some(principal_id) = self.principal_id_for_test {
            if let Some(receipt) = self.command.command_receipt.as_mut() {
                receipt.principal_id = principal_id;
            }
        }
        if let Some(receipt) = self.command.command_receipt.as_mut() {
            receipt.input_digest = self.input_digest;
        }
        self.command
    }
}

fn define_input_for_conflict(
    _defined: &db::ProjectMilestoneRevisionRecord,
    stored: &db::CommandReceipt,
) -> MilestoneConflictInput {
    let outcome: Value = serde_json::from_str(&stored.outcome_json).expect("stored outcome");
    MilestoneConflictInput {
        input_digest: stored.input_digest.clone(),
        principal_id_for_test: None,
        command: CreateProjectMilestoneCommand {
            milestone: CreateProjectMilestone {
                id: "native-milestone".to_owned(),
                project_id: PROJECT_ID.to_owned(),
                expected_project_version: 1,
                milestone_sequence: 2,
                milestone_key: "M002".to_owned(),
                display_label: Some("Native milestone".to_owned()),
                created_at: NOW.to_owned(),
                updated_at: NOW.to_owned(),
            },
            revision: milestone_revision(
                "native-milestone-revision",
                "native-milestone",
                1,
                0,
                None,
                "proposed",
            ),
            allocate_project_sequence: false,
            check_definitions: Vec::new(),
            command_receipt: Some(receipt(ReceiptInput {
                operation: PROJECT_MILESTONE_OPERATION,
                principal_type: "agent",
                principal_id: AGENT_ID,
                key: "native-milestone-define",
                digest: &stored.input_digest,
                correlation_id: "native-milestone-correlation",
                outcome: &outcome,
                execution_id: Some("native-milestone-execution"),
            })),
            action_execution: Some(action_execution_placeholder(
                "native-milestone-action",
                "native-milestone-execution",
                "native-milestone-define",
                &outcome,
            )),
        },
    }
}

fn action_execution_placeholder(
    action_id: &str,
    execution_id: &str,
    key: &str,
    outcome: &Value,
) -> CreateAgentActionExecution {
    CreateAgentActionExecution {
        id: execution_id.to_owned(),
        action_id: action_id.to_owned(),
        expected_action_version: 1,
        attempt: 1,
        status: AgentActionExecutionStatus::Succeeded,
        result_json: Some(outcome.to_string()),
        error: None,
        executed_by_type: "agent".to_owned(),
        executed_by_id: AGENT_ID.to_owned(),
        idempotency_key: key.to_owned(),
        action_status: AgentActionStatus::Executed,
        action_outcome_json: Some(outcome.to_string()),
        created_at: NOW.to_owned(),
        completed_at: Some(NOW.to_owned()),
        updated_at: NOW.to_owned(),
    }
}

struct ReleaseConflictInput {
    input_digest: String,
    command: CreateProjectReleaseRequestCommand,
}

impl ReleaseConflictInput {
    fn into_command(mut self) -> CreateProjectReleaseRequestCommand {
        if let Some(receipt) = self.command.command_receipt.as_mut() {
            receipt.input_digest = self.input_digest;
        }
        self.command
    }
}

fn release_input_for_conflict(
    request: &db::ProjectReleaseRequestRecord,
    stored: &db::CommandReceipt,
) -> ReleaseConflictInput {
    let outcome: Value =
        serde_json::from_str(&stored.outcome_json).expect("stored release outcome");
    ReleaseConflictInput {
        input_digest: stored.input_digest.clone(),
        command: CreateProjectReleaseRequestCommand {
            request: CreateProjectReleaseRequest {
                event_id: request.event_id.clone(),
                project_id: PROJECT_ID.to_owned(),
                milestone_id: MILESTONE_ID.to_owned(),
                expected_milestone_version: request.expected_milestone_version,
                readiness_snapshot_id: request.readiness_snapshot_id.clone(),
                readiness_digest: request.readiness_digest.clone(),
                status: request.status.clone(),
                idempotency_key: request.idempotency_key.clone(),
                created_at: NOW.to_owned(),
            },
            command_receipt: Some(receipt(ReceiptInput {
                operation: PROJECT_RELEASE_OPERATION,
                principal_type: "agent",
                principal_id: AGENT_ID,
                key: request.idempotency_key.as_str(),
                digest: &stored.input_digest,
                correlation_id: "native-release-correlation",
                outcome: &outcome,
                execution_id: stored.agent_action_execution_id.as_deref(),
            })),
            action_execution: Some(action_execution_placeholder(
                "native-release-action",
                "native-release-execution",
                request.idempotency_key.as_str(),
                &outcome,
            )),
        },
    }
}
