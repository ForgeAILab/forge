//! Gate A acceptance coverage for the shared Project Decision command service.
//!
//! Native Project-Agent execution and authenticated user commands should both
//! enter the same transport-neutral service boundary.  The native adapter
//! tests below remain as coverage for action provenance; the user tests cover
//! service-level replay, approval, and rejection behavior.

use std::sync::Arc;

use api_types::{DecisionCandidateContext, DecisionClass};
use db::{
    create_sqlite_pool, run_migrations, AgentActionPolicyResult, AgentActionRepo,
    AgentActionStatus, AgentRepo, AgentStatus, CreateAgentAction, CreateAgentIdentity,
    CreateAgentProfile, CreateProject, ProjectRepo, SqliteDb, User, UserRepo,
};
use forge_agent_host::PROJECT_DECISION_OPERATION;
use serde_json::{json, Value};
use services::{
    AgentActionProvenance, ExecuteProjectOrchestrationActionInput, ProjectCommandAuthorization,
    ProjectDecisionApprovalCommand, ProjectDecisionCandidateCommand, ProjectDecisionCommandService,
    ProjectDecisionEffectiveCommand, ProjectDecisionRejectionCommand,
    ProjectOrchestrationActionService, ServiceError, PROJECT_DECISION_CANDIDATE_APPROVE_COMMAND,
    PROJECT_DECISION_CANDIDATE_CREATE_COMMAND, PROJECT_DECISION_CANDIDATE_REJECT_COMMAND,
    PROJECT_DECISION_EFFECTIVE_COMMAND,
};
use sqlx::Row;

const ACCOUNT_ID: &str = "decision-command-account";
const AGENT_ID: &str = "decision-command-agent";
const PROFILE_ID: &str = "decision-command-profile";
const PROJECT_ID: &str = "decision-command-project";
const CHARTER_ID: &str = "decision-command-charter";
const CHARTER_REVISION_ID: &str = "decision-command-charter-revision";
const BASELINE_ID: &str = "decision-command-baseline";
const BASELINE_REVISION_ID: &str = "decision-command-baseline-revision";
const NOW: &str = "2026-08-20T00:00:00.000Z";

struct Fixture {
    db: Arc<SqliteDb>,
    project_version: i64,
}

async fn database() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("SQLite pool creates");
    run_migrations(&pool).await.expect("migrations run");
    Arc::new(SqliteDb::new(pool))
}

async fn fixture() -> Fixture {
    let db = database().await;
    UserRepo::create_user(
        &*db,
        &User {
            id: ACCOUNT_ID.to_owned(),
            email: "decision-command@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: Some("Decision Command User".to_owned()),
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
            name: "Decision Project Agent".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some(ACCOUNT_ID.to_owned()),
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
    .expect("Project Agent identity creates");
    ProjectRepo::create_with_agent_binding(
        &*db,
        CreateProject {
            id: PROJECT_ID.to_owned(),
            name: "Decision Command Project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(ACCOUNT_ID.to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
        Some(AGENT_ID.to_owned()),
        Some(PROFILE_ID.to_owned()),
    )
    .await
    .expect("Project creates with active Project Agent binding");

    // The native Decision command requires an exact approved Charter revision
    // and an active approved baseline.  Keep their content deliberately small;
    // the command under test only needs the immutable references and envelope.
    sqlx::query(
        "INSERT INTO project_charter
            (id, account_id, project_id, project_mode, maturity, lifecycle,
             version, created_at, updated_at)
         VALUES (?, ?, ?, 'standard', 'mvp', 'attached', 1, ?, ?)",
    )
    .bind(CHARTER_ID)
    .bind(ACCOUNT_ID)
    .bind(PROJECT_ID)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("Charter creates");
    sqlx::query(
        "INSERT INTO project_charter_revision
            (id, charter_id, revision, base_revision, lifecycle, schema_version,
             render_version, content_json, rendered_view, change_summary,
             author_type, author_id, source_refs_json, content_digest,
             rendered_digest, created_at)
         VALUES (?, ?, 1, 0, 'approved', 'forge.project-charter/v1',
                 'forge.project-charter-render/v1', '{}', '# Decision Charter',
                 'fixture', 'user', ?, '[]', 'charter-content-digest',
                 'charter-render-digest', ?)",
    )
    .bind(CHARTER_REVISION_ID)
    .bind(CHARTER_ID)
    .bind(ACCOUNT_ID)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("Charter revision creates");
    sqlx::query(
        "UPDATE project_charter
         SET current_approved_revision_id = ? WHERE id = ?",
    )
    .bind(CHARTER_REVISION_ID)
    .bind(CHARTER_ID)
    .execute(db.pool())
    .await
    .expect("Charter approval pointer sets");
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
    .expect("Project Charter pointer sets");

    sqlx::query(
        "INSERT INTO project_execution_baseline
            (id, project_id, current_revision_id, lifecycle, version,
             created_at, updated_at)
         VALUES (?, ?, ?, 'active', 2, ?, ?)",
    )
    .bind(BASELINE_ID)
    .bind(PROJECT_ID)
    .bind(BASELINE_REVISION_ID)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("baseline creates");
    sqlx::query(
        "INSERT INTO project_execution_baseline_revision
            (id, baseline_id, revision, base_revision, lifecycle,
             charter_revision_id, document_revisions_json, plan_items_json,
             milestone_ids_json, milestone_definition_revision_ids_json,
             release_policy_json, release_policy_revision, release_policy_digest,
             acceptance_matrix_json, capability_classes_json, risk_classes_json,
             adaptive_envelope_json, elevated_operations_json, exclusions_json,
             rollback_recovery_json, schema_version, render_version, rendered_view,
             content_digest, rendered_digest, source_refs_json, created_at)
         VALUES (?, ?, 1, 0, 'approved', ?, '[]', '[]', '[]', '[]', '{}',
                 'policy-1', 'policy-digest', '[]', '[]', '[]', ?, '[]', '[]',
                 '{}', 'forge.execution-baseline/v1',
                 'forge.execution-baseline-render/v1', '# Decision Baseline',
                 'baseline-content-digest', 'baseline-render-digest', '[]', ?)",
    )
    .bind(BASELINE_REVISION_ID)
    .bind(BASELINE_ID)
    .bind(CHARTER_REVISION_ID)
    .bind(
        json!({
            "allowed_task_operations": [],
            "fixed_outcomes": [],
            "fixed_acceptance": [],
            "fixed_risk_classes": [],
            "forbidden_side_effects": [],
            "elevated_operations": [],
        })
        .to_string(),
    )
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("baseline revision creates");

    Fixture {
        db,
        project_version: 1,
    }
}

fn action_payload(action: &str, expected_project_version: i64, decision_id: Option<&str>) -> Value {
    let mut payload = json!({
        "action": action,
        "decision_class": "project_implementation",
        "baseline_id": BASELINE_ID,
        "baseline_revision_id": BASELINE_REVISION_ID,
        "expected_project_version": expected_project_version,
        "question": "Which implementation choice should the Project use?",
        "options": ["option-a", "option-b"],
        "selected_outcome": "option-a",
        "rationale": "The bounded implementation choice fits the approved envelope.",
        "affected_artifact_refs": [],
        "affected_task_ids": [],
        "affected_milestone_ids": [],
    });
    if let Some(decision_id) = decision_id {
        payload["decision_id"] = Value::String(decision_id.to_owned());
    }
    payload
}

async fn create_action(
    fixture: &Fixture,
    action_id: &str,
    payload: Value,
    correlation_id: &str,
) -> db::AgentAction {
    create_action_with_operation(
        fixture,
        action_id,
        PROJECT_DECISION_OPERATION,
        payload,
        correlation_id,
    )
    .await
}

async fn create_action_with_operation(
    fixture: &Fixture,
    action_id: &str,
    operation: &str,
    payload: Value,
    correlation_id: &str,
) -> db::AgentAction {
    AgentActionRepo::create_action(
        &*fixture.db,
        CreateAgentAction {
            id: action_id.to_owned(),
            actor_identity_id: AGENT_ID.to_owned(),
            scope_type: "project".to_owned(),
            scope_id: PROJECT_ID.to_owned(),
            operation: operation.to_owned(),
            payload_json: payload.to_string(),
            payload_hash: format!("{action_id}-payload"),
            dedupe_key: format!("{action_id}-dedupe"),
            correlation_id: correlation_id.to_owned(),
            causation_id: None,
            causation_depth: 0,
            requested_permission: "propose_decision".to_owned(),
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
    .expect("Decision action creates")
}

fn execute_input(
    action_id: &str,
    version: i64,
    key: &str,
    principal_id: &str,
) -> ExecuteProjectOrchestrationActionInput {
    ExecuteProjectOrchestrationActionInput {
        action_id: action_id.to_owned(),
        expected_version: version,
        executed_by_type: "agent".to_owned(),
        executed_by_id: principal_id.to_owned(),
        idempotency_key: key.to_owned(),
    }
}

fn user_authorization(action: &str, key: &str) -> ProjectCommandAuthorization {
    ProjectCommandAuthorization {
        principal_type: "user".to_owned(),
        principal_id: ACCOUNT_ID.to_owned(),
        policy_result: "allowed".to_owned(),
        policy_revision: Some("decision-policy@1".to_owned()),
        policy_digest: Some("decision-policy-digest".to_owned()),
        requested_permission: Some(action.to_owned()),
        correlation_id: format!("decision-service-correlation-{key}"),
        causation_id: None,
        causation_depth: 0,
        authorization_event_id: format!("decision-service-authorization-{key}"),
        authorization_basis: "explicit authenticated user authorization".to_owned(),
        authorization_action: action.to_owned(),
        authorization_occurred_at: db::now_rfc3339(),
        authorization_json: json!({
            "principal": {"type": "user", "id": ACCOUNT_ID},
            "action": action,
            "event_id": format!("decision-service-authorization-{key}"),
        })
        .to_string(),
    }
}

fn agent_authorization(
    authorization_action: &str,
    correlation_id: &str,
    authorization_event_id: &str,
) -> ProjectCommandAuthorization {
    ProjectCommandAuthorization {
        principal_type: "agent".to_owned(),
        principal_id: AGENT_ID.to_owned(),
        policy_result: "allowed".to_owned(),
        policy_revision: Some("decision-policy@1".to_owned()),
        policy_digest: Some("decision-policy-digest".to_owned()),
        requested_permission: Some("propose_decision".to_owned()),
        correlation_id: correlation_id.to_owned(),
        causation_id: None,
        causation_depth: 0,
        authorization_event_id: authorization_event_id.to_owned(),
        authorization_basis: "project_agent_binding_policy".to_owned(),
        authorization_action: authorization_action.to_owned(),
        authorization_occurred_at: db::now_rfc3339(),
        authorization_json: json!({
            "principal": {"type": "agent", "id": AGENT_ID},
            "action": authorization_action,
            "event_id": authorization_event_id,
            "correlation_id": correlation_id,
        })
        .to_string(),
    }
}

fn action_provenance(action: &db::AgentAction, execution_key: &str) -> AgentActionProvenance {
    AgentActionProvenance::new(
        action.id.clone(),
        action.version,
        1,
        execution_key.to_owned(),
        "agent".to_owned(),
        AGENT_ID.to_owned(),
    )
}

fn agent_candidate_command(
    key: &str,
    expected_project_version: i64,
) -> ProjectDecisionCandidateCommand {
    let mut command = user_candidate_command(
        key,
        "Which implementation choice should the Project use after a receipt failpoint?",
        expected_project_version,
    );
    command.authorization = agent_authorization(
        "project.decision.record_candidate",
        &format!("decision-failpoint-correlation-{key}"),
        &format!("decision-failpoint-authorization-{key}"),
    );
    command
}

fn agent_effective_command(
    key: &str,
    expected_project_version: i64,
) -> ProjectDecisionEffectiveCommand {
    let correlation_id = format!("decision-failpoint-correlation-{key}");
    let authorization_event_id = format!("decision-failpoint-authorization-{key}");
    ProjectDecisionEffectiveCommand {
        project_id: PROJECT_ID.to_owned(),
        decision_id: format!("decision-failpoint-effective-{key}"),
        question: "Which implementation choice should the Project use after a receipt failpoint?"
            .to_owned(),
        context: DecisionCandidateContext {
            summary: Some("A bounded implementation choice.".to_owned()),
            governing_baseline_revision_id: Some(BASELINE_REVISION_ID.to_owned()),
            ..DecisionCandidateContext::default()
        },
        options: vec!["option-a".to_owned(), "option-b".to_owned()],
        selected_outcome: "option-a".to_owned(),
        rationale: "The bounded implementation choice fits the approved envelope.".to_owned(),
        decision_class: DecisionClass::ProjectImplementation,
        authority_basis: "active_execution_baseline_adaptive_envelope".to_owned(),
        charter_revision_id: Some(CHARTER_REVISION_ID.to_owned()),
        baseline_revision_id: Some(BASELINE_REVISION_ID.to_owned()),
        source_refs: Vec::new(),
        supersedes_decision_id: None,
        state: "active".to_owned(),
        expected_project_version,
        idempotency_key: key.to_owned(),
        authorization: agent_authorization(
            "project.decision.record_effective",
            &correlation_id,
            &authorization_event_id,
        ),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DecisionMutationCounts {
    candidates: i64,
    decisions: i64,
    events: i64,
    receipts: i64,
    action_executions: i64,
    project_version: i64,
}

async fn mutation_counts(db: &SqliteDb) -> DecisionMutationCounts {
    DecisionMutationCounts {
        candidates: sqlx::query_scalar("SELECT COUNT(*) FROM project_decision_candidate")
            .fetch_one(db.pool())
            .await
            .expect("candidate count"),
        decisions: sqlx::query_scalar("SELECT COUNT(*) FROM project_decision")
            .fetch_one(db.pool())
            .await
            .expect("Decision count"),
        events: sqlx::query_scalar("SELECT COUNT(*) FROM domain_event")
            .fetch_one(db.pool())
            .await
            .expect("event count"),
        receipts: sqlx::query_scalar("SELECT COUNT(*) FROM command_receipt")
            .fetch_one(db.pool())
            .await
            .expect("receipt count"),
        action_executions: sqlx::query_scalar("SELECT COUNT(*) FROM agent_action_execution")
            .fetch_one(db.pool())
            .await
            .expect("Action execution count"),
        project_version: sqlx::query_scalar("SELECT version FROM project WHERE id = ?")
            .bind(PROJECT_ID)
            .fetch_one(db.pool())
            .await
            .expect("Project version"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReceiptIdentity {
    id: String,
    event_id: String,
    action_execution_id: Option<String>,
    outcome_json: String,
}

async fn receipt_identity(db: &SqliteDb, operation: &str, key: &str) -> ReceiptIdentity {
    let row = sqlx::query(
        "SELECT id, event_id, agent_action_execution_id, outcome_json
         FROM command_receipt WHERE operation = ? AND idempotency_key = ?",
    )
    .bind(operation)
    .bind(key)
    .fetch_one(db.pool())
    .await
    .expect("command receipt identity");
    ReceiptIdentity {
        id: row.get("id"),
        event_id: row.get("event_id"),
        action_execution_id: row.get("agent_action_execution_id"),
        outcome_json: row.get("outcome_json"),
    }
}

async fn candidate_state(
    db: &SqliteDb,
    candidate_id: &str,
) -> Option<(String, String, i64, Option<String>)> {
    sqlx::query(
        "SELECT lifecycle, context_json, version, effective_decision_id
         FROM project_decision_candidate WHERE id = ?",
    )
    .bind(candidate_id)
    .fetch_optional(db.pool())
    .await
    .expect("candidate state")
    .map(|row| {
        (
            row.get("lifecycle"),
            row.get("context_json"),
            row.get("version"),
            row.get("effective_decision_id"),
        )
    })
}

async fn arm_receipt_failpoint(db: &SqliteDb, trigger_name: &str, error_message: &str) {
    let statement = format!(
        "CREATE TEMP TRIGGER {trigger_name}
         BEFORE INSERT ON command_receipt
         BEGIN SELECT RAISE(ABORT, '{error_message}'); END"
    );
    sqlx::query(&statement)
        .execute(db.pool())
        .await
        .expect("receipt failpoint creates");
}

async fn remove_receipt_failpoint(db: &SqliteDb, trigger_name: &str) {
    sqlx::query(&format!("DROP TRIGGER {trigger_name}"))
        .execute(db.pool())
        .await
        .expect("receipt failpoint removes");
}

fn user_candidate_command(
    key: &str,
    question: &str,
    expected_project_version: i64,
) -> ProjectDecisionCandidateCommand {
    ProjectDecisionCandidateCommand {
        project_id: PROJECT_ID.to_owned(),
        question: question.to_owned(),
        context: DecisionCandidateContext {
            summary: Some("A bounded implementation choice.".to_owned()),
            governing_baseline_revision_id: Some(BASELINE_REVISION_ID.to_owned()),
            ..DecisionCandidateContext::default()
        },
        options: vec!["option-a".to_owned(), "option-b".to_owned()],
        selected_outcome: Some("option-a".to_owned()),
        rationale: Some("The bounded implementation choice fits the approved envelope.".to_owned()),
        decision_class: DecisionClass::ProjectImplementation,
        source_refs: Vec::new(),
        expected_project_version,
        reconciliation_reason: None,
        idempotency_key: key.to_owned(),
        authorization: user_authorization(PROJECT_DECISION_CANDIDATE_CREATE_COMMAND, key),
    }
}

#[tokio::test]
async fn native_candidate_decision_is_project_scoped_and_replay_safe() {
    let fixture = fixture().await;
    let action = create_action(
        &fixture,
        "decision-candidate-action",
        action_payload("record_candidate", fixture.project_version, None),
        "decision-candidate-correlation",
    )
    .await;
    let input = execute_input(
        &action.id,
        action.version,
        "decision-candidate-command",
        AGENT_ID,
    );
    let service = ProjectOrchestrationActionService::new(Arc::clone(&fixture.db));
    let first = service
        .execute(input.clone())
        .await
        .expect("native candidate command");
    let receipt = sqlx::query(
        "SELECT principal_type, principal_id, scope_type, scope_id, operation,
                event_id, agent_action_execution_id, outcome_json
         FROM command_receipt WHERE idempotency_key = ?",
    )
    .bind("decision-candidate-command")
    .fetch_one(fixture.db.pool())
    .await
    .expect("candidate command receipt");
    assert_eq!(receipt.get::<String, _>("principal_type"), "agent");
    assert_eq!(receipt.get::<String, _>("principal_id"), AGENT_ID);
    assert_eq!(receipt.get::<String, _>("scope_type"), "project");
    assert_eq!(receipt.get::<String, _>("scope_id"), PROJECT_ID);
    assert_eq!(
        receipt.get::<String, _>("operation"),
        PROJECT_DECISION_OPERATION
    );
    assert_eq!(
        receipt.get::<Option<String>, _>("agent_action_execution_id"),
        Some(first.id.clone())
    );
    let outcome: Value = serde_json::from_str(&receipt.get::<String, _>("outcome_json"))
        .expect("candidate receipt outcome JSON");
    assert_eq!(outcome["operation"], PROJECT_DECISION_OPERATION);
    assert_eq!(outcome["project_id"], PROJECT_ID);
    assert_eq!(outcome["lifecycle"], "proposed");
    assert_eq!(outcome["domain_committed"], true);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_decision_candidate WHERE project_id = ?",
        )
        .bind(PROJECT_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("candidate count"),
        1
    );

    // Replay lookup precedes mutable Project admission.  A changed version
    // must not create a second candidate or require the old version again.
    sqlx::query("UPDATE project SET version = 9, updated_at = ? WHERE id = ?")
        .bind(NOW)
        .bind(PROJECT_ID)
        .execute(fixture.db.pool())
        .await
        .expect("Project advances");
    let replay = service.execute(input).await.expect("candidate replay");
    assert_eq!(replay.id, first.id);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_decision_candidate WHERE project_id = ?",
        )
        .bind(PROJECT_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("candidate replay count"),
        1
    );

    let changed_principal = service
        .execute(execute_input(
            &action.id,
            action.version,
            "decision-candidate-command",
            "different-agent",
        ))
        .await;
    assert!(
        matches!(
            changed_principal,
            Err(ServiceError::Db(db::DbError::IdempotencyConflict))
        ),
        "changed principal must fail closed: {changed_principal:?}"
    );
}

#[tokio::test]
async fn native_effective_decision_links_execution_and_replays_after_project_change() {
    let fixture = fixture().await;
    let action = create_action(
        &fixture,
        "decision-effective-action",
        action_payload(
            "record_effective",
            fixture.project_version,
            Some("decision-1"),
        ),
        "decision-effective-correlation",
    )
    .await;
    let input = execute_input(
        &action.id,
        action.version,
        "decision-effective-command",
        AGENT_ID,
    );
    let service = ProjectOrchestrationActionService::new(Arc::clone(&fixture.db));
    let first = service
        .execute(input.clone())
        .await
        .expect("native effective Decision command");
    let receipt = sqlx::query(
        "SELECT principal_type, principal_id, scope_type, scope_id, operation,
                event_id, agent_action_execution_id, outcome_json
         FROM command_receipt WHERE idempotency_key = ?",
    )
    .bind("decision-effective-command")
    .fetch_one(fixture.db.pool())
    .await
    .expect("effective command receipt");
    assert_eq!(receipt.get::<String, _>("principal_type"), "agent");
    assert_eq!(receipt.get::<String, _>("principal_id"), AGENT_ID);
    assert_eq!(receipt.get::<String, _>("scope_type"), "project");
    assert_eq!(receipt.get::<String, _>("scope_id"), PROJECT_ID);
    assert_eq!(
        receipt.get::<String, _>("operation"),
        PROJECT_DECISION_OPERATION
    );
    assert_eq!(
        receipt.get::<Option<String>, _>("agent_action_execution_id"),
        Some(first.id.clone())
    );
    let outcome: Value = serde_json::from_str(&receipt.get::<String, _>("outcome_json"))
        .expect("effective receipt outcome JSON");
    assert_eq!(outcome["decision_id"], "decision-1");
    assert_eq!(outcome["state"], "active");
    assert_eq!(outcome["domain_committed"], true);
    let event = sqlx::query(
        "SELECT event_type, actor_type, actor_id, scope_type, scope_id, correlation_id
         FROM domain_event WHERE id = ?",
    )
    .bind(receipt.get::<String, _>("event_id"))
    .fetch_one(fixture.db.pool())
    .await
    .expect("effective Decision event");
    assert_eq!(
        event.get::<String, _>("event_type"),
        "project.decision.created"
    );
    assert_eq!(event.get::<String, _>("actor_type"), "agent");
    assert_eq!(event.get::<String, _>("actor_id"), AGENT_ID);
    assert_eq!(event.get::<String, _>("scope_type"), "project");
    assert_eq!(event.get::<String, _>("scope_id"), PROJECT_ID);
    assert_eq!(
        event.get::<String, _>("correlation_id"),
        "decision-effective-correlation"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project_decision WHERE project_id = ?")
            .bind(PROJECT_ID)
            .fetch_one(fixture.db.pool())
            .await
            .expect("effective Decision count"),
        1
    );

    sqlx::query("UPDATE project SET version = 11, updated_at = ? WHERE id = ?")
        .bind(NOW)
        .bind(PROJECT_ID)
        .execute(fixture.db.pool())
        .await
        .expect("Project advances after effective Decision");
    let replay = service
        .execute(input)
        .await
        .expect("effective Decision replay");
    assert_eq!(replay.id, first.id);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project_decision WHERE project_id = ?")
            .bind(PROJECT_ID)
            .fetch_one(fixture.db.pool())
            .await
            .expect("effective replay count"),
        1
    );
}

#[tokio::test]
async fn user_candidate_service_is_exactly_replay_safe_and_rejects_changed_input() {
    let fixture = fixture().await;
    let service = ProjectDecisionCommandService::new(Arc::clone(&fixture.db));
    let command = user_candidate_command(
        "decision-service-create",
        "Which implementation choice should the Project use?",
        fixture.project_version,
    );
    let (record, concurrent_replay) = tokio::join!(
        service.create_candidate(command.clone(), None),
        service.create_candidate(command.clone(), None),
    );
    let record = record.expect("user Decision candidate command");
    let concurrent_replay = concurrent_replay.expect("concurrent candidate replay");
    assert_eq!(concurrent_replay, record);
    assert_eq!(record.project_id, PROJECT_ID);
    assert_eq!(record.lifecycle, "proposed");
    assert_eq!(record.principal_type.as_deref(), Some("user"));
    assert_eq!(record.principal_id.as_deref(), Some(ACCOUNT_ID));

    // Replay lookup precedes mutable Project admission.  A changed Project
    // version must still return the exact candidate created by the first call.
    sqlx::query("UPDATE project SET version = 9, updated_at = ? WHERE id = ?")
        .bind(NOW)
        .bind(PROJECT_ID)
        .execute(fixture.db.pool())
        .await
        .expect("Project advances");
    let replay = service
        .create_candidate(command.clone(), None)
        .await
        .expect("user Decision candidate replay");
    assert_eq!(replay, record);
    let mut changed_input = command;
    changed_input.question =
        "Which changed implementation choice should the Project use?".to_owned();
    let digest_conflict = service.create_candidate(changed_input, None).await;
    assert!(
        matches!(
            digest_conflict,
            Err(ServiceError::Db(db::DbError::IdempotencyConflict))
        ),
        "changed user command input must conflict: {digest_conflict:?}"
    );
    let receipt = sqlx::query(
        "SELECT principal_type, principal_id, scope_type, scope_id, operation,
                agent_action_execution_id, outcome_json
         FROM command_receipt WHERE idempotency_key = ?",
    )
    .bind("decision-service-create")
    .fetch_one(fixture.db.pool())
    .await
    .expect("user Decision receipt");
    assert_eq!(receipt.get::<String, _>("principal_type"), "user");
    assert_eq!(receipt.get::<String, _>("principal_id"), ACCOUNT_ID);
    assert_eq!(receipt.get::<String, _>("scope_type"), "project");
    assert_eq!(receipt.get::<String, _>("scope_id"), PROJECT_ID);
    assert_eq!(
        receipt.get::<String, _>("operation"),
        PROJECT_DECISION_CANDIDATE_CREATE_COMMAND
    );
    assert!(receipt
        .get::<Option<String>, _>("agent_action_execution_id")
        .is_none());
    let outcome: Value =
        serde_json::from_str(&receipt.get::<String, _>("outcome_json")).expect("receipt outcome");
    assert_eq!(outcome["candidate_id"], record.id);
    assert_eq!(outcome["lifecycle"], "proposed");
    assert_eq!(outcome["domain_committed"], true);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project_decision_candidate")
            .fetch_one(fixture.db.pool())
            .await
            .expect("user candidate count"),
        1
    );
}

#[tokio::test]
async fn user_candidate_service_approves_and_replays_effective_decision() {
    let fixture = fixture().await;
    let service = ProjectDecisionCommandService::new(Arc::clone(&fixture.db));
    let create_command = user_candidate_command(
        "decision-service-approve-create",
        "Which implementation choice should be approved?",
        fixture.project_version,
    );
    let candidate = service
        .create_candidate(create_command.clone(), None)
        .await
        .expect("candidate for approval");
    let approval = ProjectDecisionApprovalCommand {
        project_id: PROJECT_ID.to_owned(),
        candidate_id: candidate.id.clone(),
        expected_project_version: fixture.project_version + 1,
        idempotency_key: "decision-service-approve".to_owned(),
        authorization: user_authorization(
            PROJECT_DECISION_CANDIDATE_APPROVE_COMMAND,
            "decision-service-approve",
        ),
    };
    let decision = service
        .approve_candidate(approval.clone(), None)
        .await
        .expect("user Decision candidate approval");
    assert_eq!(decision.project_id, PROJECT_ID);
    assert_eq!(decision.state, "active");
    assert_eq!(decision.decision_class, "project_implementation");
    assert_eq!(decision.principal_type, "user");
    assert_eq!(decision.principal_id, ACCOUNT_ID);

    let candidate_row = sqlx::query(
        "SELECT lifecycle, effective_decision_id, version
         FROM project_decision_candidate WHERE id = ?",
    )
    .bind(&candidate.id)
    .fetch_one(fixture.db.pool())
    .await
    .expect("approved candidate");
    assert_eq!(candidate_row.get::<String, _>("lifecycle"), "approved");
    assert_eq!(
        candidate_row.get::<Option<String>, _>("effective_decision_id"),
        Some(decision.id.clone())
    );
    assert_eq!(candidate_row.get::<i64, _>("version"), 2);

    let create_replay = service
        .create_candidate(create_command, None)
        .await
        .expect("candidate creation replay after approval");
    assert_eq!(create_replay, candidate);
    assert_eq!(create_replay.lifecycle, "proposed");
    assert_eq!(create_replay.version, 1);
    assert!(create_replay.effective_decision_id.is_none());

    let replay = service
        .approve_candidate(approval, None)
        .await
        .expect("user Decision approval replay");
    assert_eq!(replay.id, decision.id);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project_decision")
            .fetch_one(fixture.db.pool())
            .await
            .expect("effective Decision count"),
        1
    );
}

#[tokio::test]
async fn user_candidate_service_rejects_and_replays_candidate() {
    let fixture = fixture().await;
    let service = ProjectDecisionCommandService::new(Arc::clone(&fixture.db));
    let candidate = service
        .create_candidate(
            user_candidate_command(
                "decision-service-reject-create",
                "Which implementation choice should be rejected?",
                fixture.project_version,
            ),
            None,
        )
        .await
        .expect("candidate for rejection");
    let rejection = ProjectDecisionRejectionCommand {
        project_id: PROJECT_ID.to_owned(),
        candidate_id: candidate.id.clone(),
        reason: "The option exceeds the approved implementation envelope.".to_owned(),
        expected_project_version: fixture.project_version + 1,
        idempotency_key: "decision-service-reject".to_owned(),
        authorization: user_authorization(
            PROJECT_DECISION_CANDIDATE_REJECT_COMMAND,
            "decision-service-reject",
        ),
    };
    let rejected = service
        .reject_candidate(rejection.clone(), None)
        .await
        .expect("user Decision candidate rejection");
    assert_eq!(rejected.id, candidate.id);
    assert_eq!(rejected.lifecycle, "rejected");
    let rejected_context: Value =
        serde_json::from_str(&rejected.context_json).expect("rejected candidate context");
    assert_eq!(
        rejected_context["rejection_reason"],
        "The option exceeds the approved implementation envelope."
    );

    let replay = service
        .reject_candidate(rejection, None)
        .await
        .expect("user Decision rejection replay");
    assert_eq!(replay.id, rejected.id);
    assert_eq!(replay.lifecycle, "rejected");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project_decision_candidate")
            .fetch_one(fixture.db.pool())
            .await
            .expect("rejected candidate count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project_decision")
            .fetch_one(fixture.db.pool())
            .await
            .expect("rejected effective Decision count"),
        0
    );
}

#[tokio::test]
async fn candidate_receipt_failpoint_rolls_back_action_bundle_and_replays_exactly() {
    let fixture = fixture().await;
    let key = "decision-failpoint-candidate";
    let action = create_action_with_operation(
        &fixture,
        "decision-failpoint-candidate-action",
        PROJECT_DECISION_CANDIDATE_CREATE_COMMAND,
        action_payload("record_candidate", fixture.project_version, None),
        &format!("decision-failpoint-correlation-{key}"),
    )
    .await;
    let command = agent_candidate_command(key, fixture.project_version);
    let provenance = action_provenance(&action, key);
    let service = ProjectDecisionCommandService::new(Arc::clone(&fixture.db));
    let before = mutation_counts(&fixture.db).await;

    arm_receipt_failpoint(
        &fixture.db,
        "decision_candidate_receipt_failpoint",
        "decision candidate receipt failpoint",
    )
    .await;
    let failed = service
        .create_candidate(command.clone(), Some(provenance.clone()))
        .await
        .expect_err("candidate receipt failpoint must abort the command");
    assert!(failed
        .to_string()
        .contains("decision candidate receipt failpoint"));
    assert_eq!(mutation_counts(&fixture.db).await, before);
    assert_eq!(
        candidate_state(&fixture.db, "never-created-candidate").await,
        None
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM agent_action WHERE id = ?")
            .bind(&action.id)
            .fetch_one(fixture.db.pool())
            .await
            .expect("source Action status after rollback"),
        "proposed"
    );
    remove_receipt_failpoint(&fixture.db, "decision_candidate_receipt_failpoint").await;

    let first = service
        .create_candidate(command.clone(), Some(provenance.clone()))
        .await
        .expect("candidate retry after receipt rollback");
    let after = mutation_counts(&fixture.db).await;
    assert_eq!(after.candidates, before.candidates + 1);
    assert_eq!(after.decisions, before.decisions);
    assert_eq!(after.events, before.events + 1);
    assert_eq!(after.receipts, before.receipts + 1);
    assert_eq!(after.action_executions, before.action_executions + 1);
    assert_eq!(after.project_version, before.project_version + 1);
    let receipt =
        receipt_identity(&fixture.db, PROJECT_DECISION_CANDIDATE_CREATE_COMMAND, key).await;
    assert!(receipt.action_execution_id.is_some());
    let candidate_id = first.id.clone();
    assert_eq!(
        candidate_state(&fixture.db, &candidate_id).await.unwrap().0,
        "proposed"
    );

    drop(service);
    let replay_service = ProjectDecisionCommandService::new(Arc::clone(&fixture.db));
    let replay = replay_service
        .create_candidate(command, Some(provenance))
        .await
        .expect("candidate replay after service recreation");
    assert_eq!(replay, first, "replay must return the frozen candidate");
    assert_eq!(
        receipt_identity(&fixture.db, PROJECT_DECISION_CANDIDATE_CREATE_COMMAND, key).await,
        receipt,
        "replay must preserve receipt, event, execution, and outcome IDs"
    );
    assert_eq!(mutation_counts(&fixture.db).await, after);
    assert_eq!(
        candidate_state(&fixture.db, &candidate_id).await.unwrap().0,
        "proposed"
    );
}

#[tokio::test]
async fn effective_decision_receipt_failpoint_rolls_back_action_bundle_and_replays_exactly() {
    let fixture = fixture().await;
    let key = "decision-failpoint-effective";
    let action = create_action_with_operation(
        &fixture,
        "decision-failpoint-effective-action",
        PROJECT_DECISION_EFFECTIVE_COMMAND,
        action_payload("record_effective", fixture.project_version, Some("unused")),
        &format!("decision-failpoint-correlation-{key}"),
    )
    .await;
    let command = agent_effective_command(key, fixture.project_version);
    let provenance = action_provenance(&action, key);
    let service = ProjectDecisionCommandService::new(Arc::clone(&fixture.db));
    let before = mutation_counts(&fixture.db).await;

    arm_receipt_failpoint(
        &fixture.db,
        "decision_effective_receipt_failpoint",
        "decision effective receipt failpoint",
    )
    .await;
    let failed = service
        .append_effective(command.clone(), Some(provenance.clone()))
        .await
        .expect_err("effective Decision receipt failpoint must abort the command");
    assert!(failed
        .to_string()
        .contains("decision effective receipt failpoint"));
    assert_eq!(mutation_counts(&fixture.db).await, before);
    remove_receipt_failpoint(&fixture.db, "decision_effective_receipt_failpoint").await;

    let first = service
        .append_effective(command.clone(), Some(provenance.clone()))
        .await
        .expect("effective Decision retry after receipt rollback");
    let after = mutation_counts(&fixture.db).await;
    assert_eq!(after.candidates, before.candidates);
    assert_eq!(after.decisions, before.decisions + 1);
    assert_eq!(after.events, before.events + 1);
    assert_eq!(after.receipts, before.receipts + 1);
    assert_eq!(after.action_executions, before.action_executions + 1);
    assert_eq!(after.project_version, before.project_version + 1);
    let receipt = receipt_identity(&fixture.db, PROJECT_DECISION_EFFECTIVE_COMMAND, key).await;
    assert!(receipt.action_execution_id.is_some());
    let outcome: Value = serde_json::from_str(&receipt.outcome_json).expect("effective outcome");
    assert_eq!(outcome["decision_id"], first.id);

    drop(service);
    let replay_service = ProjectDecisionCommandService::new(Arc::clone(&fixture.db));
    let replay = replay_service
        .append_effective(command, Some(provenance))
        .await
        .expect("effective Decision replay after service recreation");
    assert_eq!(
        replay, first,
        "replay must return the frozen effective Decision"
    );
    assert_eq!(
        receipt_identity(&fixture.db, PROJECT_DECISION_EFFECTIVE_COMMAND, key).await,
        receipt,
        "replay must preserve receipt, event, execution, and outcome IDs"
    );
    assert_eq!(mutation_counts(&fixture.db).await, after);
}

#[tokio::test]
async fn approval_receipt_failpoint_rolls_back_candidate_promotion_and_replays_exactly() {
    let fixture = fixture().await;
    let service = ProjectDecisionCommandService::new(Arc::clone(&fixture.db));
    let create_command = user_candidate_command(
        "decision-failpoint-approval-create",
        "Which implementation choice should be approved after a receipt failpoint?",
        fixture.project_version,
    );
    let candidate = service
        .create_candidate(create_command, None)
        .await
        .expect("candidate for approval failpoint");
    let approval_key = "decision-failpoint-approval";
    let approval = ProjectDecisionApprovalCommand {
        project_id: PROJECT_ID.to_owned(),
        candidate_id: candidate.id.clone(),
        expected_project_version: fixture.project_version + 1,
        idempotency_key: approval_key.to_owned(),
        authorization: user_authorization(PROJECT_DECISION_CANDIDATE_APPROVE_COMMAND, approval_key),
    };
    let before = mutation_counts(&fixture.db).await;
    let candidate_before = candidate_state(&fixture.db, &candidate.id).await;

    arm_receipt_failpoint(
        &fixture.db,
        "decision_approval_receipt_failpoint",
        "decision approval receipt failpoint",
    )
    .await;
    let failed = service
        .approve_candidate(approval.clone(), None)
        .await
        .expect_err("approval receipt failpoint must abort the command");
    assert!(failed
        .to_string()
        .contains("decision approval receipt failpoint"));
    assert_eq!(mutation_counts(&fixture.db).await, before);
    assert_eq!(
        candidate_state(&fixture.db, &candidate.id).await,
        candidate_before
    );
    remove_receipt_failpoint(&fixture.db, "decision_approval_receipt_failpoint").await;

    let first = service
        .approve_candidate(approval.clone(), None)
        .await
        .expect("approval retry after receipt rollback");
    let after = mutation_counts(&fixture.db).await;
    assert_eq!(after.candidates, before.candidates);
    assert_eq!(after.decisions, before.decisions + 1);
    assert_eq!(after.events, before.events + 1);
    assert_eq!(after.receipts, before.receipts + 1);
    assert_eq!(after.action_executions, before.action_executions);
    assert_eq!(after.project_version, before.project_version + 1);
    let receipt = receipt_identity(
        &fixture.db,
        PROJECT_DECISION_CANDIDATE_APPROVE_COMMAND,
        approval_key,
    )
    .await;
    assert!(receipt.action_execution_id.is_none());
    assert_eq!(
        candidate_state(&fixture.db, &candidate.id).await.unwrap().0,
        "approved"
    );
    assert_eq!(
        candidate_state(&fixture.db, &candidate.id).await.unwrap().3,
        Some(first.id.clone())
    );

    drop(service);
    let replay_service = ProjectDecisionCommandService::new(Arc::clone(&fixture.db));
    let replay = replay_service
        .approve_candidate(approval, None)
        .await
        .expect("approval replay after service recreation");
    assert_eq!(
        replay, first,
        "replay must return the frozen approved Decision"
    );
    assert_eq!(
        receipt_identity(
            &fixture.db,
            PROJECT_DECISION_CANDIDATE_APPROVE_COMMAND,
            approval_key,
        )
        .await,
        receipt,
        "replay must preserve receipt and event IDs"
    );
    assert_eq!(mutation_counts(&fixture.db).await, after);
}

#[tokio::test]
async fn rejection_receipt_failpoint_rolls_back_candidate_rejection_and_replays_exactly() {
    let fixture = fixture().await;
    let service = ProjectDecisionCommandService::new(Arc::clone(&fixture.db));
    let candidate = service
        .create_candidate(
            user_candidate_command(
                "decision-failpoint-rejection-create",
                "Which implementation choice should be rejected after a receipt failpoint?",
                fixture.project_version,
            ),
            None,
        )
        .await
        .expect("candidate for rejection failpoint");
    let rejection_key = "decision-failpoint-rejection";
    let rejection = ProjectDecisionRejectionCommand {
        project_id: PROJECT_ID.to_owned(),
        candidate_id: candidate.id.clone(),
        reason: "The option exceeds the approved implementation envelope.".to_owned(),
        expected_project_version: fixture.project_version + 1,
        idempotency_key: rejection_key.to_owned(),
        authorization: user_authorization(PROJECT_DECISION_CANDIDATE_REJECT_COMMAND, rejection_key),
    };
    let before = mutation_counts(&fixture.db).await;
    let candidate_before = candidate_state(&fixture.db, &candidate.id).await;

    arm_receipt_failpoint(
        &fixture.db,
        "decision_rejection_receipt_failpoint",
        "decision rejection receipt failpoint",
    )
    .await;
    let failed = service
        .reject_candidate(rejection.clone(), None)
        .await
        .expect_err("rejection receipt failpoint must abort the command");
    assert!(failed
        .to_string()
        .contains("decision rejection receipt failpoint"));
    assert_eq!(mutation_counts(&fixture.db).await, before);
    assert_eq!(
        candidate_state(&fixture.db, &candidate.id).await,
        candidate_before
    );
    remove_receipt_failpoint(&fixture.db, "decision_rejection_receipt_failpoint").await;

    let first = service
        .reject_candidate(rejection.clone(), None)
        .await
        .expect("rejection retry after receipt rollback");
    let after = mutation_counts(&fixture.db).await;
    assert_eq!(after.candidates, before.candidates);
    assert_eq!(after.decisions, before.decisions);
    assert_eq!(after.events, before.events + 1);
    assert_eq!(after.receipts, before.receipts + 1);
    assert_eq!(after.action_executions, before.action_executions);
    assert_eq!(after.project_version, before.project_version + 1);
    let receipt = receipt_identity(
        &fixture.db,
        PROJECT_DECISION_CANDIDATE_REJECT_COMMAND,
        rejection_key,
    )
    .await;
    assert!(receipt.action_execution_id.is_none());
    let rejected_state = candidate_state(&fixture.db, &candidate.id)
        .await
        .expect("rejected candidate state");
    assert_eq!(rejected_state.0, "rejected");
    assert_eq!(rejected_state.2, 2);
    let rejected_context: Value =
        serde_json::from_str(&rejected_state.1).expect("rejected candidate context");
    assert_eq!(
        rejected_context["rejection_reason"],
        "The option exceeds the approved implementation envelope."
    );

    drop(service);
    let replay_service = ProjectDecisionCommandService::new(Arc::clone(&fixture.db));
    let replay = replay_service
        .reject_candidate(rejection, None)
        .await
        .expect("rejection replay after service recreation");
    assert_eq!(
        replay, first,
        "replay must return the frozen rejected candidate"
    );
    assert_eq!(
        receipt_identity(
            &fixture.db,
            PROJECT_DECISION_CANDIDATE_REJECT_COMMAND,
            rejection_key,
        )
        .await,
        receipt,
        "replay must preserve receipt and event IDs"
    );
    assert_eq!(mutation_counts(&fixture.db).await, after);
}
