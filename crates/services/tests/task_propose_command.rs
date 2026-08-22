//! Gate A acceptance coverage for the atomic `task.propose` command.
//!
//! These tests intentionally enter the public coordination command surface
//! (`AgentActionService::execute_task_proposal`) rather than calling the
//! legacy `TaskService::create_task_with_governance` helper.  The former is
//! the transport-neutral seam used by the native Project Agent adapter; the
//! command implementation must make the Task, governance link, domain event,
//! command receipt, and optional action execution one replayable transaction.

use std::sync::Arc;

use db::{
    create_sqlite_pool, run_migrations, AgentAction, AgentActionPolicyResult, AgentActionRepo,
    AgentActionStatus, AgentProfileRepo, AgentRepo, AgentStatus, CreateAgentAction,
    CreateAgentIdentity, CreateAgentProfile, CreateProject, CreateRepo, ProjectRepo, RepoRepo,
    SqliteDb, UpdateProject,
};
use events::EventBus;
use serde_json::{json, Value};
use services::{
    AgentActionService, DirectTaskProposalInput, ExecuteTaskProposalInput, ServiceError,
    TaskService,
};
use sha2::{Digest, Sha256};

const USER_ID: &str = "task-command-user";
const AGENT_ID: &str = "task-command-project-agent";
const PROFILE_ID: &str = "task-command-project-agent-profile";
const OTHER_AGENT_ID: &str = "task-command-other-agent";
const OTHER_PROFILE_ID: &str = "task-command-other-agent-profile";
const BASELINE_PROJECT_ID: &str = "task-command-baseline-project";
const BASELINE_REPO_ID: &str = "task-command-baseline-repo";
const PREBASELINE_PROJECT_ID: &str = "task-command-prebaseline-project";
const PREBASELINE_REPO_ID: &str = "task-command-prebaseline-repo";
const NOW: &str = "2026-08-21T00:00:00.000Z";
const CHARTER_ID: &str = "task-command-charter";
const CHARTER_REVISION_ID: &str = "task-command-charter-revision";
const MILESTONE_ID: &str = "task-command-milestone";
const MILESTONE_REVISION_ID: &str = "task-command-milestone-revision";
const BASELINE_ID: &str = "task-command-baseline";
const BASELINE_REVISION_ID: &str = "task-command-baseline-revision";
const PREBASELINE_CHARTER_ID: &str = "task-command-prebaseline-charter";
const PREBASELINE_CHARTER_REVISION_ID: &str = "task-command-prebaseline-charter-revision";
const PREBASELINE_MILESTONE_ID: &str = "task-command-prebaseline-milestone";
const PREBASELINE_MILESTONE_REVISION_ID: &str = "task-command-prebaseline-milestone-revision";
const OPERATION: &str = "task.propose";

#[derive(Clone)]
struct Fixture {
    db: Arc<SqliteDb>,
    task_service: TaskService,
    action_service: AgentActionService,
}

async fn database() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    Arc::new(SqliteDb::new(pool))
}

async fn seed_identity(db: &SqliteDb, identity_id: &str, profile_id: &str) {
    AgentRepo::create_identity_with_profile(
        db,
        CreateAgentIdentity {
            id: identity_id.to_owned(),
            name: identity_id.to_owned(),
            description: None,
            max_concurrent_tasks: 4,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some(USER_ID.to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: r#"{"permissions":["read_project","propose_task"]}"#
                .to_owned(),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
        CreateAgentProfile {
            id: profile_id.to_owned(),
            identity_id: identity_id.to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("test-model".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: r#"{"permissions":["read_project","propose_task"]}"#.to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("identity/profile");
}

async fn seed_project(
    db: &SqliteDb,
    project_id: &str,
    repo_id: &str,
    identity_id: &str,
    profile_id: &str,
) {
    ProjectRepo::create_with_agent_binding(
        db,
        CreateProject {
            id: project_id.to_owned(),
            name: project_id.to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(USER_ID.to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
        Some(identity_id.to_owned()),
        Some(profile_id.to_owned()),
    )
    .await
    .expect("project and active Project Agent binding");
    RepoRepo::create(
        db,
        CreateRepo {
            id: repo_id.to_owned(),
            project_id: project_id.to_owned(),
            name: format!("{project_id}-repo"),
            remote_url: format!("file:///tmp/{repo_id}"),
            local_path: None,
            work_mode: db::WorkMode::DirectMerge,
            default_branch: "main".to_owned(),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("repository");
    ProjectRepo::update(
        db,
        UpdateProject {
            id: project_id.to_owned(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.to_owned())),
            paused_at: None,
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("primary repository");
}

async fn seed_charter_and_milestone(
    db: &SqliteDb,
    project_id: &str,
    charter_id: &str,
    charter_revision_id: &str,
    milestone_id: &str,
    milestone_revision_id: &str,
) {
    sqlx::query(
        "INSERT INTO project_charter
         (id, account_id, project_id, project_mode, maturity, lifecycle,
          current_approved_revision_id, created_at, updated_at)
         VALUES (?, ?, ?, 'standard', 'mvp', 'attached', NULL, ?, ?)",
    )
    .bind(charter_id)
    .bind(USER_ID)
    .bind(project_id)
    .bind(charter_revision_id)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("Charter");
    sqlx::query(
        "INSERT INTO project_charter_revision
         (id, charter_id, revision, lifecycle, schema_version, render_version,
          content_json, rendered_view, change_summary, author_type, author_id,
          source_refs_json, content_digest, rendered_digest, created_at)
         VALUES (?, ?, 1, 'approved', 'charter@1', 'render@1', '{}',
                 '# Charter', 'task command fixture', 'user', ?, '[]',
                 'charter-content', 'charter-rendered', ?)",
    )
    .bind(charter_revision_id)
    .bind(charter_id)
    .bind(USER_ID)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("Charter revision");
    sqlx::query("UPDATE project_charter SET current_approved_revision_id = ? WHERE id = ?")
        .bind(charter_revision_id)
        .bind(charter_id)
        .execute(db.pool())
        .await
        .expect("Charter approved pointer");
    sqlx::query(
        "UPDATE project
         SET charter_status = 'charter_backed', charter_setup_required = 0,
             current_charter_id = ?, current_charter_revision_id = ?,
             current_charter_version = 1
         WHERE id = ?",
    )
    .bind(charter_id)
    .bind(charter_revision_id)
    .bind(project_id)
    .execute(db.pool())
    .await
    .expect("Project Charter pointer");
    sqlx::query(
        "INSERT INTO project_milestone
         (id, project_id, milestone_sequence, milestone_key, display_label,
          lifecycle, blocker_reason_json, stale_reason_json,
          reconciliation_reason_json, version, created_at, updated_at)
         VALUES (?, ?, 1, 'M001', 'Task command milestone', 'planned',
                 '[]', '[]', '[]', 1, ?, ?)",
    )
    .bind(milestone_id)
    .bind(project_id)
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
          acceptance_checks_json, evidence_requirements_json, known_issues_json,
          change_summary, schema_version, render_version, rendered_view,
          content_digest, rendered_digest, author_type, author_id,
          source_refs_json, created_at)
         VALUES (?, ?, 1, 0, NULL, 'approved', 'Task command milestone',
                 'Task command outcome', '[]', '[]', ?, '[]', '[]', '[]',
                 '[]', '[]', '[]', '[]', 'fixture', 'milestone@1',
                 'milestone-render@1', '# Milestone', 'milestone-content',
                 'milestone-rendered', 'user', ?, '[]', ?)",
    )
    .bind(milestone_revision_id)
    .bind(milestone_id)
    .bind(charter_revision_id)
    .bind(USER_ID)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("milestone revision");
    sqlx::query("UPDATE project_milestone SET current_definition_revision_id = ? WHERE id = ?")
        .bind(milestone_revision_id)
        .bind(milestone_id)
        .execute(db.pool())
        .await
        .expect("milestone pointer");
}

async fn seed_active_baseline(db: &SqliteDb, project_id: &str) {
    sqlx::query(
        "INSERT INTO project_execution_baseline
         (id, project_id, current_revision_id, lifecycle, version, created_at, updated_at)
         VALUES (?, ?, ?, 'active', 1, ?, ?)",
    )
    .bind(BASELINE_ID)
    .bind(project_id)
    .bind(BASELINE_REVISION_ID)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("active baseline");
    let revision_sql = format!(
        "INSERT INTO project_execution_baseline_revision
         (id, baseline_id, revision, base_revision, base_revision_id, lifecycle,
          charter_revision_id, document_revisions_json, plan_items_json,
          milestone_id, milestone_ids_json, milestone_definition_revision_ids_json,
          primary_milestone_id, release_policy_json, release_policy_revision,
          release_policy_digest, acceptance_matrix_json, capability_classes_json,
          risk_classes_json, adaptive_envelope_json, elevated_operations_json,
          exclusions_json, rollback_recovery_json, schema_version, render_version,
          rendered_view, content_digest, rendered_digest, source_refs_json, created_at)
        VALUES (?, ?, 1, 0, NULL, 'approved', ?, '[]', '[\"plan-1\"]',
                 ?, '[\"{}\"]', '[\"{}\"]', ?, '{{}}', 'policy-1',
                 'policy-digest', '[]', '[\"repository_write\"]', '[\"low\"]',
                 '{{\"allowed_task_operations\":[\"split\",\"sequence\",\"replace\"],\"fixed_outcomes\":[],\"fixed_acceptance\":[],\"fixed_risk_classes\":[\"low\"],\"forbidden_side_effects\":[],\"elevated_operations\":[]}}', '[]', '[]', '{{}}', 'baseline@1', 'baseline-render@1',
                 '# Baseline', 'baseline-content', 'baseline-rendered', '[]', ?)",
        MILESTONE_ID, MILESTONE_REVISION_ID,
    );
    sqlx::query(&revision_sql)
        .bind(BASELINE_REVISION_ID)
        .bind(BASELINE_ID)
        .bind(CHARTER_REVISION_ID)
        .bind(MILESTONE_ID)
        .bind(MILESTONE_ID)
        .bind(NOW)
        .execute(db.pool())
        .await
        .expect("baseline revision");
    sqlx::query(
        "INSERT INTO project_execution_baseline_approval
         (id, baseline_id, revision_id, expected_project_version, principal_type,
          principal_id, authorization_basis, authorization_action, explicit_event,
          authorization_occurred_at, content_digest, rendered_digest, lifecycle,
          idempotency_key, created_at, updated_at)
         VALUES ('task-command-approval', ?, ?, 1, 'user', ?,
                 'explicit baseline approval', 'project.execution_baseline.approve',
                 'task-command-approval-event', ?, 'baseline-content',
                 'baseline-rendered', 'active', 'task-command-approval-key', ?, ?)",
    )
    .bind(BASELINE_ID)
    .bind(BASELINE_REVISION_ID)
    .bind(USER_ID)
    .bind(NOW)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("baseline approval");
}

async fn fixture() -> Fixture {
    let db = database().await;
    sqlx::query(
        "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
         VALUES (?, ?, 'test', 'Task command user', ?, ?)",
    )
    .bind(USER_ID)
    .bind("task-command@example.test")
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("user");
    seed_identity(&db, AGENT_ID, PROFILE_ID).await;
    seed_identity(&db, OTHER_AGENT_ID, OTHER_PROFILE_ID).await;
    seed_project(
        &db,
        BASELINE_PROJECT_ID,
        BASELINE_REPO_ID,
        AGENT_ID,
        PROFILE_ID,
    )
    .await;
    seed_project(
        &db,
        PREBASELINE_PROJECT_ID,
        PREBASELINE_REPO_ID,
        AGENT_ID,
        PROFILE_ID,
    )
    .await;
    seed_charter_and_milestone(
        &db,
        BASELINE_PROJECT_ID,
        CHARTER_ID,
        CHARTER_REVISION_ID,
        MILESTONE_ID,
        MILESTONE_REVISION_ID,
    )
    .await;
    seed_active_baseline(&db, BASELINE_PROJECT_ID).await;
    seed_charter_and_milestone(
        &db,
        PREBASELINE_PROJECT_ID,
        PREBASELINE_CHARTER_ID,
        PREBASELINE_CHARTER_REVISION_ID,
        PREBASELINE_MILESTONE_ID,
        PREBASELINE_MILESTONE_REVISION_ID,
    )
    .await;
    Fixture {
        task_service: TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(32))),
        action_service: AgentActionService::new(Arc::clone(&db)),
        db,
    }
}

fn payload(_project_id: &str, title: &str, task_type: &str, baseline_governed: bool) -> Value {
    let mut value = json!({
        "title": title,
        "description": "A task command acceptance task",
        "task_type": task_type,
        "priority": 3,
        "merge_config": null,
        "role_assignments": null,
        "governance": null,
    });
    if baseline_governed {
        value["plan_item_id"] = Value::String("plan-1".to_owned());
        value["milestone_id"] = Value::String(MILESTONE_ID.to_owned());
        value["capability_class"] = Value::String("repository_write".to_owned());
        value["risk_class"] = Value::String("low".to_owned());
    }
    value
}

fn payload_hash(payload: &Value) -> String {
    let mut digest = Sha256::new();
    digest.update(payload.to_string().as_bytes());
    hex::encode(digest.finalize())
}

async fn action(
    db: &SqliteDb,
    id: &str,
    actor_identity_id: &str,
    project_id: &str,
    payload: &Value,
) -> AgentAction {
    AgentActionRepo::create_action(
        db,
        CreateAgentAction {
            id: id.to_owned(),
            actor_identity_id: actor_identity_id.to_owned(),
            scope_type: "project".to_owned(),
            scope_id: project_id.to_owned(),
            operation: OPERATION.to_owned(),
            payload_json: payload.to_string(),
            payload_hash: payload_hash(payload),
            dedupe_key: format!("dedupe-{id}"),
            correlation_id: format!("correlation-{id}"),
            causation_id: None,
            causation_depth: 0,
            requested_permission: "propose_task".to_owned(),
            policy_result: AgentActionPolicyResult::Allowed,
            policy_reason: None,
            status: AgentActionStatus::Proposed,
            target_type: Some("project".to_owned()),
            target_id: Some(project_id.to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("agent action");
    AgentActionRepo::get_action(db, id)
        .await
        .expect("action read")
        .expect("action exists")
}

async fn propose(
    fixture: &Fixture,
    action_id: String,
    expected_version: i64,
    idempotency_key: &str,
) -> Result<services::ExecutedTaskProposal, ServiceError> {
    fixture
        .action_service
        .execute_task_proposal(
            &fixture.task_service,
            ExecuteTaskProposalInput {
                action_id,
                expected_version,
                executed_by_type: "agent".to_owned(),
                executed_by_id: AGENT_ID.to_owned(),
                idempotency_key: idempotency_key.to_owned(),
            },
        )
        .await
}

async fn count(db: &SqliteDb, query: &str) -> i64 {
    sqlx::query_scalar(query)
        .fetch_one(db.pool())
        .await
        .expect("count")
}

async fn role_ids(db: &SqliteDb, task_id: &str) -> Vec<String> {
    sqlx::query_scalar("SELECT id FROM task_role_assignment WHERE task_id = ? ORDER BY id")
        .bind(task_id)
        .fetch_all(db.pool())
        .await
        .expect("role assignment ids")
}

async fn project_work_epoch(db: &SqliteDb, project_id: &str) -> i64 {
    sqlx::query_scalar("SELECT project_work_epoch FROM project WHERE id = ?")
        .bind(project_id)
        .fetch_one(db.pool())
        .await
        .expect("project work epoch")
}

#[tokio::test]
async fn task_proposal_commits_one_atomic_bundle_and_replays_frozen_task() {
    let fixture = fixture().await;
    let task_payload = payload(
        BASELINE_PROJECT_ID,
        "Baseline governed implementation",
        "task",
        true,
    );
    let task_action = action(
        &fixture.db,
        "task-command-atomic-action",
        AGENT_ID,
        BASELINE_PROJECT_ID,
        &task_payload,
    )
    .await;

    let first = propose(
        &fixture,
        task_action.id.clone(),
        task_action.version,
        "task-command-atomic-key",
    )
    .await
    .expect("task proposal");
    let task_id = first.task.id.clone();
    assert_eq!(
        first.execution.status,
        db::AgentActionExecutionStatus::Succeeded
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM task WHERE id = 'task-command-atomic-action'",
        )
        .await,
        0,
        "the server mints the Task id; it is not the source action id"
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM task WHERE project_id = 'task-command-baseline-project'"
        )
        .await,
        1
    );
    let governance: (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        i64,
    ) = sqlx::query_as(
        "SELECT baseline_id, baseline_revision_id, plan_item_id, milestone_id, runnable
             FROM project_task_governance WHERE task_id = ?",
    )
    .bind(&task_id)
    .fetch_one(fixture.db.pool())
    .await
    .expect("governance row");
    assert_eq!(governance.0.as_deref(), Some(BASELINE_ID));
    assert_eq!(governance.1.as_deref(), Some(BASELINE_REVISION_ID));
    assert_eq!(governance.2.as_deref(), Some("plan-1"));
    assert_eq!(governance.3.as_deref(), Some(MILESTONE_ID));
    assert_eq!(
        governance.4, 1,
        "active approved baseline makes implementation runnable"
    );

    let bundle: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
             (SELECT COUNT(*) FROM domain_event WHERE entity_type = 'task' AND entity_id = ?),
             (SELECT COUNT(*) FROM command_receipt
              WHERE operation = 'task.propose' AND idempotency_key = ?),
             (SELECT COUNT(*) FROM agent_action_execution WHERE action_id = ?),
             (SELECT COUNT(*) FROM project_task_governance WHERE task_id = ?)",
    )
    .bind(&task_id)
    .bind("task-command-atomic-key")
    .bind(&task_action.id)
    .bind(&task_id)
    .fetch_one(fixture.db.pool())
    .await
    .expect("atomic bundle counts");
    assert_eq!(bundle, (1, 1, 1, 1));

    let outcome_json: String = sqlx::query_scalar(
        "SELECT outcome_json FROM command_receipt
         WHERE operation = 'task.propose' AND idempotency_key = ?",
    )
    .bind("task-command-atomic-key")
    .fetch_one(fixture.db.pool())
    .await
    .expect("frozen command outcome");
    let outcome: Value = serde_json::from_str(&outcome_json).expect("frozen outcome JSON");
    let execution_result: Value = serde_json::from_str(
        &first
            .execution
            .result_json
            .clone()
            .expect("action execution result"),
    )
    .expect("action result JSON");
    assert_eq!(execution_result["task_id"], task_id);
    assert_eq!(outcome["task_id"], task_id);
    assert!(outcome["domain_committed"].as_bool().unwrap_or(false));

    // A response-loss replay must resolve the frozen receipt before mutable
    // binding/authorization state.  This also proves that server-minted IDs
    // are not regenerated from the same source action.
    sqlx::query("UPDATE task SET title = 'Live row changed after commit' WHERE id = ?")
        .bind(&task_id)
        .execute(fixture.db.pool())
        .await
        .expect("mutate live Task after commit");
    sqlx::query(
        "UPDATE project_agent_binding SET state = 'paused'
         WHERE project_id = ? AND identity_id = ? AND state = 'active'",
    )
    .bind(BASELINE_PROJECT_ID)
    .bind(AGENT_ID)
    .execute(fixture.db.pool())
    .await
    .expect("pause binding after commit");
    let replay = propose(
        &fixture,
        task_action.id,
        task_action.version,
        "task-command-atomic-key",
    )
    .await
    .expect("replay frozen task after response loss");
    assert_eq!(replay.task.id, task_id);
    assert_eq!(replay.task.title, first.task.title);
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM task WHERE project_id = 'task-command-baseline-project'"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM command_receipt WHERE operation = 'task.propose'"
        )
        .await,
        1
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM agent_action_execution").await,
        1
    );
}

#[tokio::test]
async fn task_proposal_changed_payload_or_principal_on_same_key_conflicts() {
    let fixture = fixture().await;
    let original_payload = payload(BASELINE_PROJECT_ID, "Original task command", "task", true);
    let task_action = action(
        &fixture.db,
        "task-command-conflict-action",
        AGENT_ID,
        BASELINE_PROJECT_ID,
        &original_payload,
    )
    .await;
    propose(
        &fixture,
        task_action.id.clone(),
        task_action.version,
        "task-command-conflict-key",
    )
    .await
    .expect("original proposal");

    // Mutating the source action simulates a caller reusing the command key
    // after changing its payload.  The command receipt digest, not a
    // read-before-create check, is the conflict boundary.
    let changed_payload = payload(BASELINE_PROJECT_ID, "Changed task command", "task", true);
    sqlx::query("UPDATE agent_action SET payload_json = ?, payload_hash = ? WHERE id = ?")
        .bind(changed_payload.to_string())
        .bind(payload_hash(&changed_payload))
        .bind(&task_action.id)
        .execute(fixture.db.pool())
        .await
        .expect("simulate changed source action");
    let changed_payload_result = propose(
        &fixture,
        task_action.id.clone(),
        task_action.version,
        "task-command-conflict-key",
    )
    .await
    .expect_err("changed payload must conflict");
    assert!(matches!(
        changed_payload_result,
        ServiceError::Db(db::DbError::IdempotencyConflict)
            | ServiceError::Conflict(_)
            | ServiceError::Db(db::DbError::VersionConflict)
    ));

    // A changed principal on the same key is a receipt conflict too.  Keep
    // the alternate principal actively bound so mutable authorization cannot
    // mask the canonical command-identity check.
    let principal_payload = payload(
        BASELINE_PROJECT_ID,
        "Principal-change planning task",
        "planning_task",
        false,
    );
    let principal_action = action(
        &fixture.db,
        "task-command-principal-conflict-action",
        AGENT_ID,
        BASELINE_PROJECT_ID,
        &principal_payload,
    )
    .await;
    propose(
        &fixture,
        principal_action.id.clone(),
        principal_action.version,
        "task-command-principal-conflict-key",
    )
    .await
    .expect("principal-conflict proposal");
    sqlx::query("UPDATE agent_action SET actor_identity_id = ? WHERE id = ?")
        .bind(OTHER_AGENT_ID)
        .bind(&principal_action.id)
        .execute(fixture.db.pool())
        .await
        .expect("simulate changed principal action");
    sqlx::query(
        "UPDATE project_agent_binding
         SET identity_id = ?, profile_id = ?, updated_at = ?, version = version + 1
         WHERE project_id = ? AND state = 'active'",
    )
    .bind(OTHER_AGENT_ID)
    .bind(OTHER_PROFILE_ID)
    .bind(NOW)
    .bind(BASELINE_PROJECT_ID)
    .execute(fixture.db.pool())
    .await
    .expect("simulate changed principal binding");
    let changed_principal_result = propose(
        &fixture,
        principal_action.id,
        principal_action.version,
        "task-command-principal-conflict-key",
    )
    .await
    .expect_err("changed principal must conflict");
    assert!(matches!(
        changed_principal_result,
        ServiceError::Db(db::DbError::IdempotencyConflict) | ServiceError::Conflict(_)
    ));
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM task WHERE project_id = 'task-command-baseline-project'"
        )
        .await,
        2
    );
}

#[tokio::test]
async fn task_proposal_changed_executor_on_same_key_conflicts_before_auth() {
    let fixture = fixture().await;
    let task_payload = payload(BASELINE_PROJECT_ID, "Executor conflict task", "task", true);
    let task_action = action(
        &fixture.db,
        "task-command-executor-conflict-action",
        AGENT_ID,
        BASELINE_PROJECT_ID,
        &task_payload,
    )
    .await;
    propose(
        &fixture,
        task_action.id.clone(),
        task_action.version,
        "task-command-executor-conflict-key",
    )
    .await
    .expect("original agent execution");

    // The user is a valid Project owner, but changing the executor principal
    // with the same canonical scope/operation/key must be a receipt conflict,
    // never a second execution or an authorization-dependent replay.
    let changed_executor = fixture
        .action_service
        .execute_task_proposal(
            &fixture.task_service,
            ExecuteTaskProposalInput {
                action_id: task_action.id,
                expected_version: task_action.version,
                executed_by_type: "user".to_owned(),
                executed_by_id: USER_ID.to_owned(),
                idempotency_key: "task-command-executor-conflict-key".to_owned(),
            },
        )
        .await
        .expect_err("changed executor must conflict before current auth");
    assert!(matches!(
        changed_executor,
        ServiceError::Db(db::DbError::IdempotencyConflict) | ServiceError::Conflict(_)
    ));
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM task WHERE project_id = 'task-command-baseline-project'",
        )
        .await,
        1
    );
}

#[tokio::test]
async fn concurrent_same_source_action_creates_one_task_and_one_bundle() {
    let fixture = fixture().await;
    let task_payload = payload(BASELINE_PROJECT_ID, "Concurrent task command", "task", true);
    let task_action = action(
        &fixture.db,
        "task-command-concurrent-action",
        AGENT_ID,
        BASELINE_PROJECT_ID,
        &task_payload,
    )
    .await;
    let input = ExecuteTaskProposalInput {
        action_id: task_action.id.clone(),
        expected_version: task_action.version,
        executed_by_type: "agent".to_owned(),
        executed_by_id: AGENT_ID.to_owned(),
        idempotency_key: "task-command-concurrent-key".to_owned(),
    };
    let left = fixture.clone();
    let right = fixture.clone();
    let left_input = input.clone();
    let right_input = input;
    let (first, second) = tokio::join!(
        async move {
            left.action_service
                .execute_task_proposal(&left.task_service, left_input)
                .await
        },
        async move {
            right
                .action_service
                .execute_task_proposal(&right.task_service, right_input)
                .await
        },
    );
    let first = first.expect("first concurrent proposal");
    let second = second.expect("second concurrent proposal replays");
    assert_eq!(first.task.id, second.task.id);
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM task WHERE project_id = 'task-command-baseline-project'"
        )
        .await,
        1
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM project_task_governance").await,
        1
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM command_receipt WHERE operation = 'task.propose'"
        )
        .await,
        1
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM agent_action_execution").await,
        1
    );
}

#[tokio::test]
async fn receipt_failure_rolls_back_task_governance_event_and_action_execution() {
    let fixture = fixture().await;
    let mut task_payload = payload(BASELINE_PROJECT_ID, "Rollback task command", "task", true);
    task_payload["role_assignments"] = json!([{
        "role_name": "reviewer",
        "assignee_type": "agent",
        "assignee_id": AGENT_ID,
    }]);
    let task_action = action(
        &fixture.db,
        "task-command-rollback-action",
        AGENT_ID,
        BASELINE_PROJECT_ID,
        &task_payload,
    )
    .await;

    // This database trigger is a deterministic receipt-write failpoint.  A
    // correct command composite rolls back every preceding insert, including
    // the Task and governance row; a two-transaction adapter strands them.
    sqlx::query(
        "CREATE TEMP TRIGGER task_command_receipt_failpoint
         BEFORE INSERT ON command_receipt
         BEGIN SELECT RAISE(ABORT, 'task command receipt failpoint'); END",
    )
    .execute(fixture.db.pool())
    .await
    .expect("receipt failpoint");
    let failed = propose(
        &fixture,
        task_action.id.clone(),
        task_action.version,
        "task-command-rollback-key",
    )
    .await
    .expect_err("receipt failpoint must abort the command");
    assert!(failed.to_string().contains("failpoint"));
    assert_eq!(count(&fixture.db, "SELECT COUNT(*) FROM task").await, 0);
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM project_task_governance").await,
        0
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM domain_event WHERE entity_type = 'task'"
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM command_receipt WHERE operation = 'task.propose'"
        )
        .await,
        0
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM agent_action_execution").await,
        0
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM task_role_assignment").await,
        0,
        "receipt failure must roll back explicit role assignments too"
    );
    sqlx::query("DROP TRIGGER task_command_receipt_failpoint")
        .execute(fixture.db.pool())
        .await
        .expect("drop receipt failpoint");

    let retry = propose(
        &fixture,
        task_action.id.clone(),
        task_action.version,
        "task-command-rollback-key",
    )
    .await
    .expect("retry after rollback");
    assert!(!retry.task.id.is_empty());
    let frozen_task = retry.task.clone();
    let frozen_execution_id = retry.execution.id.clone();
    let (frozen_receipt_id, frozen_event_id, frozen_receipt_execution_id): (
        String,
        String,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT id, event_id, agent_action_execution_id
         FROM command_receipt
         WHERE operation = 'task.propose' AND idempotency_key = ?",
    )
    .bind("task-command-rollback-key")
    .fetch_one(fixture.db.pool())
    .await
    .expect("frozen action-backed receipt");
    assert_eq!(
        frozen_receipt_execution_id.as_deref(),
        Some(frozen_execution_id.as_str())
    );
    let frozen_role_ids = role_ids(&fixture.db, &frozen_task.id).await;
    assert_eq!(frozen_role_ids.len(), 1);
    let frozen_work_epoch = project_work_epoch(&fixture.db, BASELINE_PROJECT_ID).await;
    assert_eq!(frozen_work_epoch, 1);
    let role: (String, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT role_name, assignee_type, assignee_id
         FROM task_role_assignment WHERE task_id = ?",
    )
    .bind(&frozen_task.id)
    .fetch_one(fixture.db.pool())
    .await
    .expect("explicit role assignment after retry");
    assert_eq!(
        role,
        (
            "reviewer".to_owned(),
            Some("agent".to_owned()),
            Some(AGENT_ID.to_owned()),
        )
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM task_role_assignment").await,
        1,
        "successful retry must persist exactly one explicit role assignment"
    );
    assert_eq!(count(&fixture.db, "SELECT COUNT(*) FROM task").await, 1);
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM project_task_governance").await,
        1
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM command_receipt WHERE operation = 'task.propose'"
        )
        .await,
        1
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM agent_action_execution").await,
        1
    );

    // Discard the successful response and recreate the command owner before
    // replaying.  The live Task may have changed after the original commit,
    // but the receipt remains authoritative and must return the frozen
    // Task/event/action-execution bundle without allocating anything else.
    drop(retry);
    sqlx::query("UPDATE task SET title = 'live action-backed title' WHERE id = ?")
        .bind(&frozen_task.id)
        .execute(fixture.db.pool())
        .await
        .expect("mutate live action-backed Task");
    let restarted_fixture = Fixture {
        db: Arc::clone(&fixture.db),
        task_service: TaskService::new(Arc::clone(&fixture.db), Arc::new(EventBus::new(32))),
        action_service: fixture.action_service.clone(),
    };
    let replay = propose(
        &restarted_fixture,
        task_action.id.clone(),
        task_action.version,
        "task-command-rollback-key",
    )
    .await
    .expect("replay after process loss and successful retry");
    assert_eq!(replay.task, frozen_task);
    assert_eq!(replay.execution.id, frozen_execution_id);
    let replay_receipt: (String, String, Option<String>) = sqlx::query_as(
        "SELECT id, event_id, agent_action_execution_id
         FROM command_receipt
         WHERE operation = 'task.propose' AND idempotency_key = ?",
    )
    .bind("task-command-rollback-key")
    .fetch_one(fixture.db.pool())
    .await
    .expect("replayed action-backed receipt");
    assert_eq!(replay_receipt.0, frozen_receipt_id);
    assert_eq!(replay_receipt.1, frozen_event_id);
    assert_eq!(
        replay_receipt.2.as_deref(),
        Some(frozen_execution_id.as_str())
    );
    assert_eq!(
        role_ids(&fixture.db, &frozen_task.id).await,
        frozen_role_ids
    );
    assert_eq!(
        project_work_epoch(&fixture.db, BASELINE_PROJECT_ID).await,
        frozen_work_epoch
    );
    let role_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_role_assignment WHERE task_id = ? AND role_name = 'reviewer'",
    )
    .bind(&frozen_task.id)
    .fetch_one(fixture.db.pool())
    .await
    .expect("role count after replay");
    assert_eq!(role_count, 1, "replay must not duplicate explicit roles");
    assert_eq!(count(&fixture.db, "SELECT COUNT(*) FROM task").await, 1);
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM project_task_governance").await,
        1
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM domain_event WHERE entity_type = 'task'",
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM command_receipt WHERE operation = 'task.propose'",
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM agent_action_execution WHERE action_id = 'task-command-rollback-action'",
        )
        .await,
        1
    );
}

#[tokio::test]
async fn prebaseline_planning_and_discovery_remain_read_only_non_runnable() {
    let fixture = fixture().await;
    for (suffix, task_type) in [("planning", "planning_task"), ("discovery", "discovery")] {
        let task_payload = payload(
            PREBASELINE_PROJECT_ID,
            &format!("Pre-baseline {task_type}"),
            task_type,
            false,
        );
        let task_action = action(
            &fixture.db,
            &format!("task-command-prebaseline-{suffix}"),
            AGENT_ID,
            PREBASELINE_PROJECT_ID,
            &task_payload,
        )
        .await;
        let proposal = propose(
            &fixture,
            task_action.id,
            task_action.version,
            &format!("task-command-prebaseline-{suffix}-key"),
        )
        .await
        .expect("pre-baseline planning/discovery proposal");
        let governance: (
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            i64,
        ) = sqlx::query_as(
            "SELECT baseline_id, baseline_revision_id, capability_class,
                    risk_class, runnable
             FROM project_task_governance WHERE task_id = ?",
        )
        .bind(&proposal.task.id)
        .fetch_one(fixture.db.pool())
        .await
        .expect("pre-baseline governance");
        assert!(governance.0.is_none());
        assert!(governance.1.is_none());
        assert_eq!(governance.2.as_deref(), Some("repository_read"));
        assert_eq!(governance.3.as_deref(), Some("low"));
        assert_eq!(governance.4, 0);
    }
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM task WHERE project_id = 'task-command-prebaseline-project'",
        )
        .await,
        2
    );
}

#[tokio::test]
async fn direct_task_proposal_commits_one_receipt_bundle_without_action_rows() {
    let fixture = fixture().await;
    let payload_value = payload(BASELINE_PROJECT_ID, "Direct task command", "task", true);
    let direct_payload: services::TaskProposalPayload =
        serde_json::from_value(payload_value).expect("task proposal payload");
    let input = DirectTaskProposalInput {
        actor_identity_id: AGENT_ID.to_owned(),
        executor_type: "agent".to_owned(),
        executor_id: AGENT_ID.to_owned(),
        source_scope_type: "project".to_owned(),
        source_scope_id: BASELINE_PROJECT_ID.to_owned(),
        project_id: BASELINE_PROJECT_ID.to_owned(),
        payload: direct_payload,
        idempotency_key: "task-command-direct-key".to_owned(),
        correlation_id: "task-command-direct-correlation".to_owned(),
        causation_id: Some("task-command-direct-cause".to_owned()),
        causation_depth: 1,
        policy_result: "allowed".to_owned(),
        preflight_policy_result: None,
        preflight_policy_reason: None,
        policy_revision: None,
        policy_digest: None,
        requested_permission: "propose_task".to_owned(),
    };

    let first = fixture
        .task_service
        .execute_task_proposal_direct(input.clone())
        .await
        .expect("direct task proposal");
    assert!(!first.replayed);
    assert_eq!(first.receipt.operation, OPERATION);
    assert_eq!(first.receipt.principal_type, "agent");
    assert_eq!(first.receipt.principal_id, AGENT_ID);
    assert!(first.receipt.agent_action_execution_id.is_none());
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM agent_action").await,
        0
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM agent_action_execution").await,
        0
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM command_receipt WHERE operation = 'task.propose'",
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM domain_event WHERE entity_type = 'task'",
        )
        .await,
        1
    );

    sqlx::query("UPDATE task SET title = 'live direct title' WHERE id = ?")
        .bind(&first.task.id)
        .execute(fixture.db.pool())
        .await
        .expect("mutate live Task");
    let replay = fixture
        .task_service
        .execute_task_proposal_direct(input.clone())
        .await
        .expect("direct replay");
    assert!(replay.replayed);
    assert_eq!(replay.task.id, first.task.id);
    assert_eq!(replay.task.title, first.task.title);
    assert_eq!(replay.receipt.id, first.receipt.id);
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM agent_action").await,
        0
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM agent_action_execution").await,
        0
    );

    // A fresh command must observe a selected-profile revocation while the
    // writer lock is held, even though an exact replay remains valid.
    let restricted_profile_id = "task-command-project-agent-restricted-profile";
    AgentProfileRepo::create_profile(
        fixture.db.as_ref(),
        CreateAgentProfile {
            id: restricted_profile_id.to_owned(),
            identity_id: AGENT_ID.to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("test-model".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: r#"{"permissions":["read_project"]}"#.to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: "2026-08-21T00:00:01.000Z".to_owned(),
            updated_at: "2026-08-21T00:00:01.000Z".to_owned(),
        },
    )
    .await
    .expect("restricted profile");
    sqlx::query("UPDATE agent_identity SET selected_profile_id = ? WHERE id = ?")
        .bind(restricted_profile_id)
        .bind(AGENT_ID)
        .execute(fixture.db.pool())
        .await
        .expect("select restricted profile");
    let mut revoked = input.clone();
    revoked.idempotency_key = "task-command-direct-revoked-profile".to_owned();
    let revoked_error = fixture
        .task_service
        .execute_task_proposal_direct(revoked)
        .await
        .expect_err("revoked profile must not create a fresh Task");
    assert!(matches!(
        revoked_error,
        ServiceError::Db(db::DbError::InvalidTransition)
            | ServiceError::AuthorizationDenied { .. }
            | ServiceError::InvalidOperation { .. }
    ));

    let mut changed = input.clone();
    changed.payload.title = "changed direct title".to_owned();
    let conflict = fixture
        .task_service
        .execute_task_proposal_direct(changed)
        .await
        .expect_err("changed direct input must conflict");
    assert!(matches!(
        conflict,
        ServiceError::Db(db::DbError::IdempotencyConflict) | ServiceError::Conflict(_)
    ));
    let mut changed_principal = input;
    changed_principal.actor_identity_id = OTHER_AGENT_ID.to_owned();
    let principal_conflict = fixture
        .task_service
        .execute_task_proposal_direct(changed_principal)
        .await
        .expect_err("changed direct principal must conflict");
    assert!(matches!(
        principal_conflict,
        ServiceError::Db(db::DbError::IdempotencyConflict) | ServiceError::Conflict(_)
    ));
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM task WHERE project_id = 'task-command-baseline-project'",
        )
        .await,
        1
    );
}

#[tokio::test]
async fn direct_task_proposal_receipt_failure_rolls_back_everything() {
    let fixture = fixture().await;
    let mut payload_value = payload(BASELINE_PROJECT_ID, "Direct rollback task", "task", true);
    payload_value["role_assignments"] = json!([{
        "role_name": "reviewer",
        "assignee_type": "agent",
        "assignee_id": AGENT_ID,
    }]);
    let direct_payload: services::TaskProposalPayload =
        serde_json::from_value(payload_value).expect("task proposal payload");
    let input = DirectTaskProposalInput {
        actor_identity_id: AGENT_ID.to_owned(),
        executor_type: "agent".to_owned(),
        executor_id: AGENT_ID.to_owned(),
        source_scope_type: "project".to_owned(),
        source_scope_id: BASELINE_PROJECT_ID.to_owned(),
        project_id: BASELINE_PROJECT_ID.to_owned(),
        payload: direct_payload,
        idempotency_key: "task-command-direct-rollback-key".to_owned(),
        correlation_id: "task-command-direct-rollback-correlation".to_owned(),
        causation_id: None,
        causation_depth: 0,
        policy_result: "allowed".to_owned(),
        preflight_policy_result: None,
        preflight_policy_reason: None,
        policy_revision: None,
        policy_digest: None,
        requested_permission: "propose_task".to_owned(),
    };
    sqlx::query(
        "CREATE TEMP TRIGGER direct_task_command_receipt_failpoint
         BEFORE INSERT ON command_receipt
         BEGIN SELECT RAISE(ABORT, 'direct task receipt failpoint'); END",
    )
    .execute(fixture.db.pool())
    .await
    .expect("receipt failpoint");
    let error = fixture
        .task_service
        .execute_task_proposal_direct(input.clone())
        .await
        .expect_err("receipt failpoint must abort direct command");
    assert!(error.to_string().contains("failpoint"));
    assert_eq!(count(&fixture.db, "SELECT COUNT(*) FROM task").await, 0);
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM project_task_governance").await,
        0
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM domain_event WHERE entity_type = 'task'",
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM command_receipt WHERE operation = 'task.propose'",
        )
        .await,
        0
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM agent_action").await,
        0
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM agent_action_execution").await,
        0
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM task_role_assignment").await,
        0,
        "receipt failure must roll back direct explicit role assignments too"
    );
    sqlx::query("DROP TRIGGER direct_task_command_receipt_failpoint")
        .execute(fixture.db.pool())
        .await
        .expect("drop receipt failpoint");

    let retry = fixture
        .task_service
        .execute_task_proposal_direct(input.clone())
        .await
        .expect("direct retry after rollback");
    assert!(!retry.replayed);
    assert!(retry.receipt.agent_action_execution_id.is_none());
    let frozen_task = retry.task.clone();
    let frozen_receipt = retry.receipt.clone();
    let frozen_role_ids = role_ids(&fixture.db, &frozen_task.id).await;
    assert_eq!(frozen_role_ids.len(), 1);
    let frozen_work_epoch = project_work_epoch(&fixture.db, BASELINE_PROJECT_ID).await;
    assert_eq!(frozen_work_epoch, 1);
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM task").await,
        1,
        "direct retry must persist exactly one Task"
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM project_task_governance").await,
        1
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM domain_event WHERE entity_type = 'task'",
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM command_receipt WHERE operation = 'task.propose'",
        )
        .await,
        1
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM agent_action_execution").await,
        0
    );

    // Simulate process loss after the retry has committed.  A new TaskService
    // must return the exact frozen receipt and Task rather than re-running the
    // domain writes or advancing the Project work epoch again.
    drop(retry);
    sqlx::query("UPDATE task SET title = 'live direct rollback title' WHERE id = ?")
        .bind(&frozen_task.id)
        .execute(fixture.db.pool())
        .await
        .expect("mutate live direct Task");
    let restarted_task_service =
        TaskService::new(Arc::clone(&fixture.db), Arc::new(EventBus::new(32)));
    let replay = restarted_task_service
        .execute_task_proposal_direct(input)
        .await
        .expect("direct replay after process loss");
    assert!(replay.replayed);
    assert_eq!(replay.task, frozen_task);
    assert_eq!(replay.receipt, frozen_receipt);
    assert_eq!(
        role_ids(&fixture.db, &frozen_task.id).await,
        frozen_role_ids
    );
    assert_eq!(
        project_work_epoch(&fixture.db, BASELINE_PROJECT_ID).await,
        frozen_work_epoch
    );
    assert_eq!(count(&fixture.db, "SELECT COUNT(*) FROM task").await, 1);
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM project_task_governance").await,
        1
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM domain_event WHERE entity_type = 'task'",
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM command_receipt WHERE operation = 'task.propose'",
        )
        .await,
        1
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM agent_action_execution").await,
        0
    );
}
