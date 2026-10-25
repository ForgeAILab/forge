//! Project artifact validation observations and receipt manifest tests.

use super::{
    validation_outcome, validation_source_manifest, ProjectArtifactCommandService,
    ProjectCommandAuthorization, ProjectValidationCommand, ValidationManifestInputs,
    PROJECT_VALIDATION_COMMAND,
};
use crate::command_boundary::{
    CommandContext, CommandPrincipal, CommandScope, CommandScopeType, ExpectedCommandState,
    NewCommandContext,
};
use crate::{ProjectMilestoneCommandService, ProjectMilestoneDefinitionCommand, ServiceError};
use api_types::{
    AcceptanceCheckSourceKind, AcceptanceEvidenceRequirement, MilestoneAcceptanceCheck,
    MilestoneDefinitionContent, MilestoneDefinitionLifecycle, PrincipalKind, PrincipalRef,
    RevisionProvenance,
};
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentRepo, AgentStatus,
    CreateAgentIdentity, CreateAgentProfile, CreateProject, ProjectRepo, SqliteDb, User, UserRepo,
};
use serde_json::json;
use std::sync::Arc;

const USER_ID: &str = "verify-user";
const AGENT_ID: &str = "verify-agent";
const PROFILE_ID: &str = "verify-profile";
const PROJECT_ID: &str = "verify-project";
const CHECK_ID: &str = "verify-check";
const NOW: &str = "2026-09-02T00:00:00Z";

struct Verified {
    db: Arc<SqliteDb>,
    milestone_id: String,
    milestone_version: i64,
    definition_revision_id: String,
}

/// A Project with an active Agent binding, an approved Charter, and one
/// proposed milestone whose single check is `task_validation`.
async fn verified_project() -> Verified {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("memory pool");
    run_migrations(&pool).await.expect("migrations");
    let db = Arc::new(SqliteDb::new(pool));
    UserRepo::create_user(
        &*db,
        &User {
            id: USER_ID.to_owned(),
            email: "verify@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("user");
    AgentRepo::create_identity_with_profile(
        &*db,
        CreateAgentIdentity {
            id: AGENT_ID.to_owned(),
            name: "Verifier".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
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
    .expect("agent");
    ProjectRepo::create_with_agent_binding(
        &*db,
        CreateProject {
            id: PROJECT_ID.to_owned(),
            name: "Verified project".to_owned(),
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
    .expect("project");
    sqlx::query(
        "INSERT INTO project_charter
                (id, account_id, genesis_session_id, project_id, current_draft_revision_id,
                 current_approved_revision_id, project_mode, maturity, lifecycle, version,
                 created_at, updated_at)
             VALUES ('verify-charter', ?, NULL, ?, NULL, NULL, 'compact', 'mvp',
                     'attached', 1, ?, ?)",
    )
    .bind(USER_ID)
    .bind(PROJECT_ID)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("charter");
    sqlx::query(
        "INSERT INTO project_charter_revision
                (id, charter_id, revision, base_revision, base_revision_id, lifecycle,
                 schema_version, render_version, content_json, rendered_view, change_summary,
                 author_type, author_id, source_message_id, source_turn_job_id, source_refs_json,
                 content_digest, rendered_digest, created_at)
             VALUES ('verify-charter-rev', 'verify-charter', 1, 0, NULL, 'approved', 'charter@1',
                     'render@1', '{}', '# Charter', 'fixture', 'user', ?, NULL, NULL, '[]',
                     'charter-content', 'charter-rendered', ?)",
    )
    .bind(USER_ID)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("charter revision");
    sqlx::query(
        "UPDATE project_charter SET current_approved_revision_id = 'verify-charter-rev'
             WHERE id = 'verify-charter'",
    )
    .execute(db.pool())
    .await
    .expect("charter pointer");
    sqlx::query(
        "UPDATE project SET current_charter_id = 'verify-charter',
                 current_charter_revision_id = 'verify-charter-rev' WHERE id = ?",
    )
    .bind(PROJECT_ID)
    .execute(db.pool())
    .await
    .expect("charter pointer");

    let content = MilestoneDefinitionContent {
        name: "Verified outcome".to_owned(),
        outcome: "The CLI behaves as chartered".to_owned(),
        included_scope: vec!["implementation".to_owned()],
        excluded_scope: Vec::new(),
        charter_revision: None,
        document_revisions: Vec::new(),
        task_ids: Vec::new(),
        dependencies: Vec::new(),
        risks: Vec::new(),
        acceptance_checks: vec![MilestoneAcceptanceCheck {
            id: CHECK_ID.to_owned(),
            description: "Adding an item persists it".to_owned(),
            required: true,
            source_kind: AcceptanceCheckSourceKind::TaskValidation,
            expected_result: "the item is listed after a restart".to_owned(),
            latest_result: None,
            latest_result_id: None,
            latest_result_digest: None,
        }],
        evidence_requirements: vec![AcceptanceEvidenceRequirement {
            id: CHECK_ID.to_owned(),
            description: "Command log".to_owned(),
            required: true,
            evidence_kind: Some("log".to_owned()),
        }],
        known_issues: Vec::new(),
        target_date: None,
    };
    let revision = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .define_milestone(
            ProjectMilestoneDefinitionCommand {
                project_id: PROJECT_ID.to_owned(),
                milestone_id: None,
                display_label: Some("Verified outcome".to_owned()),
                lifecycle: MilestoneDefinitionLifecycle::Proposed,
                rendered_view: api_types::canonical_json(&content).expect("canonical"),
                render_version: "forge.milestone-definition-render/v1".to_owned(),
                change_summary: "define".to_owned(),
                provenance: RevisionProvenance {
                    author: PrincipalRef {
                        kind: PrincipalKind::User,
                        id: USER_ID.to_owned(),
                        display_name: None,
                    },
                    profile_revision: None,
                    operating_skill_revision: None,
                    source_refs: Vec::new(),
                    change_summary: "define".to_owned(),
                    material_diff: None,
                },
                content,
                base_revision_id: None,
                expected_project_version: 1,
                expected_milestone_version: 1,
                idempotency_key: "verify-define".to_owned(),
                authorization: authorization("user", USER_ID, "project.milestone.create"),
            },
            None,
        )
        .await
        .expect("milestone defines");
    let milestone_version: i64 =
        sqlx::query_scalar("SELECT version FROM project_milestone WHERE id = ?")
            .bind(&revision.milestone_id)
            .fetch_one(db.pool())
            .await
            .expect("milestone version");
    Verified {
        db,
        milestone_id: revision.milestone_id,
        milestone_version,
        definition_revision_id: revision.id,
    }
}

fn authorization(kind: &str, id: &str, action: &str) -> ProjectCommandAuthorization {
    ProjectCommandAuthorization {
        principal_type: kind.to_owned(),
        principal_id: id.to_owned(),
        policy_result: "allowed".to_owned(),
        policy_revision: None,
        policy_digest: None,
        requested_permission: Some(action.to_owned()),
        correlation_id: format!("correlation-{action}"),
        causation_id: None,
        causation_depth: 0,
        authorization_event_id: new_uuid_v4(),
        authorization_basis: "project_agent_binding_policy".to_owned(),
        authorization_action: action.to_owned(),
        authorization_occurred_at: now_rfc3339(),
        authorization_json: json!({"action": action}).to_string(),
    }
}

async fn observation(db: &SqliteDb, created_at: &str) -> String {
    let id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO project_command_observation (
                id, project_id, actor_identity_id, scope_type, scope_id, session_id, turn_id,
                program, args_json, exit_code, success, output_digest, stdout_excerpt,
                stderr_excerpt, created_at
             ) VALUES (?, ?, ?, 'agent_chat', 'verify-chat', 'session-1', NULL, 'cargo',
                       '[\"test\"]', 0, 1, 'digest', 'ok', '', ?)",
    )
    .bind(&id)
    .bind(PROJECT_ID)
    .bind(AGENT_ID)
    .bind(created_at)
    .execute(db.pool())
    .await
    .expect("observation");
    id
}

async fn record(
    fixture: &Verified,
    status: &str,
    observed_command_ids: Vec<String>,
    key: &str,
) -> crate::Result<db::ProjectMilestoneCheckResultRecord> {
    let command = ProjectValidationCommand {
        project_id: PROJECT_ID.to_owned(),
        milestone_id: fixture.milestone_id.clone(),
        check_id: CHECK_ID.to_owned(),
        definition_revision_id: fixture.definition_revision_id.clone(),
        status: status.to_owned(),
        result: "Ran the delivered CLI in checkout/".to_owned(),
        input_digest: format!("input-{key}"),
        observed_task_id: None,
        observed_command_ids,
        evidence_asset_id: None,
        expected_milestone_version: fixture.milestone_version,
        idempotency_key: key.to_owned(),
        authorization: authorization("agent", AGENT_ID, "project.validation.record"),
    };
    let context = CommandContext::from_authorized_input(
        NewCommandContext {
            principal: CommandPrincipal {
                principal_type: "agent".to_owned(),
                principal_id: AGENT_ID.to_owned(),
            },
            canonical_scope: CommandScope {
                scope_type: CommandScopeType::Project,
                scope_id: PROJECT_ID.to_owned(),
            },
            operation: PROJECT_VALIDATION_COMMAND.to_owned(),
            idempotency_key: key.to_owned(),
            expected_state: ExpectedCommandState::default(),
            authorization_provenance: None,
            action_provenance: None,
            correlation_id: format!("correlation-{key}"),
            causation_id: None,
            causation_depth: 0,
        },
        &json!({"key": key}),
    )
    .expect("command context");
    ProjectArtifactCommandService::new(Arc::clone(&fixture.db))
        .record_validation_with_context(command, context)
        .await
}

fn invalid_reason(error: ServiceError) -> String {
    match error {
        ServiceError::InvalidOperation { message } => message,
        other => panic!("expected an invalid_operation rejection, got {other:?}"),
    }
}

#[tokio::test]
async fn a_task_validation_pass_stands_on_the_agents_own_command_observations() {
    let fixture = verified_project().await;

    // A pass with nothing behind it is the rubber stamp this closes.
    let reason = invalid_reason(
        record(&fixture, "pass", Vec::new(), "no-observation")
            .await
            .expect_err("a pass without observations is refused"),
    );
    assert!(reason.contains("observed_command_ids"), "{reason}");

    // An observation older than the delivered work proves nothing about it.
    let stale = observation(&fixture.db, "2020-01-01T00:00:00Z").await;
    let reason = invalid_reason(
        record(&fixture, "fail", vec![stale], "stale-observation")
            .await
            .expect_err("a stale observation is refused"),
    );
    assert!(reason.contains("predates"), "{reason}");

    // Someone else's command is not this Agent's observation.
    let foreign = new_uuid_v4();
    let reason = invalid_reason(
        record(&fixture, "pass", vec![foreign], "foreign-observation")
            .await
            .expect_err("an unknown observation is refused"),
    );
    assert!(reason.contains("not a command this Agent ran"), "{reason}");

    // A result that claims no observation carries none and is admitted.
    record(&fixture, "unavailable", Vec::new(), "unavailable")
        .await
        .expect("an unavailable result claims nothing");

    // The Agent's own fresh run settles the check, and the manifest keeps it.
    let fresh = observation(&fixture.db, "2099-01-01T00:00:00Z").await;
    let recorded = record(&fixture, "pass", vec![fresh.clone()], "fresh-observation")
        .await
        .expect("a pass backed by the Agent's own run is recorded");
    assert_eq!(recorded.outcome, "passed");
    let manifest: serde_json::Value =
        serde_json::from_str(&recorded.source_manifest_json).expect("manifest json");
    assert_eq!(
        manifest["observed_by"]["command_observations"][0]["id"],
        json!(fresh)
    );
    assert_eq!(
        manifest["observed_by"]["command_observations"][0]["program"],
        json!("cargo")
    );
}

/// The persisted `outcome` column admits exactly this vocabulary
/// (`project_milestone_check_result`'s CHECK constraint). An Agent status
/// that reaches the insert untranslated aborts the write instead of
/// settling the check, so the mapping is the contract worth pinning.
#[test]
fn every_recordable_status_maps_into_the_persisted_outcome_vocabulary() {
    const PERSISTED: [&str; 5] = ["passed", "failed", "missing", "stale", "waived"];
    for status in ["pass", "fail", "blocked", "stale", "unavailable"] {
        let outcome = validation_outcome(status)
            .unwrap_or_else(|| panic!("{status} is an admitted validation status"));
        assert!(
            PERSISTED.contains(&outcome),
            "{status} maps to {outcome}, which the outcome CHECK constraint rejects"
        );
    }
    assert_eq!(validation_outcome("pass"), Some("passed"));
    assert_eq!(validation_outcome("fail"), Some("failed"));
    // A waiver is the user's alone, and `pending` is the absence of an
    // observation rather than one an Agent can report.
    assert_eq!(validation_outcome("waived"), None);
    assert_eq!(validation_outcome("pending"), None);
    assert_eq!(validation_outcome("passed"), None);
}

/// Readiness rejects any validation result whose manifest does not carry
/// the governing Charter revision and check definition revision
/// (`milestone_runtime`'s "stale for the active authority" gate). The
/// Agent record path and the user attestation route must therefore write
/// the same manifest shape -- a field present in one and absent in the
/// other makes every result from that path unusable.
#[test]
fn recorded_manifest_carries_every_field_readiness_reads_back() {
    let manifest = validation_source_manifest(ValidationManifestInputs {
        result: "Exercised the delivered CLI end to end.",
        governing_charter_revision_id: "charter-revision-1",
        check_definition_revision_id: "definition-revision-1",
        principal_type: "agent",
        principal_id: "project-agent-1",
        observed_task_id: Some("task-1"),
        command_observations: &[],
        evidence_asset_id: Some("asset-1"),
    });
    // These keys are read back verbatim by `milestone_runtime`'s readiness
    // gate; a missing one is not a lost detail, it makes the result
    // permanently "stale for the active authority".
    assert_eq!(
        manifest["check_definition_revision_id"],
        "definition-revision-1"
    );
    assert_eq!(
        manifest["governing_revision_ids"],
        serde_json::json!(["charter-revision-1"]),
        "the approved Charter is the whole governing authority"
    );
    assert_eq!(manifest["observed_by"]["kind"], "agent");
    // Provenance travels with the observation: which run produced it, and
    // which captured artifact backs it.
    assert_eq!(manifest["observed_by"]["task_id"], "task-1");
    assert_eq!(manifest["observed_by"]["evidence_asset_id"], "asset-1");
}
