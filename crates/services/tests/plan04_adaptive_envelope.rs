//! PLAN-04 acceptance coverage for bounded Task reshaping.
//!
//! All operations enter TaskService, the shared service boundary used by REST
//! and MCP.  The fixture binds one exact approved baseline and verifies that
//! split, sequence, and replace preserve its governance/provenance while an
//! envelope or fixed-boundary crossing is durable reconciliation truth.

use std::sync::Arc;

use db::{
    create_sqlite_pool, run_migrations, AgentRepo, AgentStatus, CreateAgentIdentity,
    CreateAgentProfile, CreateProject, CreateRepo, CreateTask, ProjectRepo, RepoRepo, SqliteDb,
    TaskRepo, UpdateProject, WorkMode,
};
use events::EventBus;
use forge_agent_host::{
    AgentHostError, CanonicalScope, CanonicalScopeType, ForgeToolProvider, WorkspaceAccess,
};
use serde_json::{json, Value};
use services::{CoordinationToolProvider, ServiceError, TaskService};

const PROJECT_ID: &str = "plan04-project";
const REPO_ID: &str = "plan04-repo";
const CHARTER_ID: &str = "plan04-charter";
const CHARTER_REVISION_ID: &str = "plan04-charter-revision";
const BASELINE_ID: &str = "plan04-baseline";
const BASELINE_REVISION_ID: &str = "plan04-baseline-revision";
const MILESTONE_ID: &str = "plan04-milestone";
const MILESTONE_REVISION_ID: &str = "plan04-milestone-revision";
const ROOT_TASK_ID: &str = "plan04-root-task";
const AGENT_ID: &str = "plan04-project-agent";
const PROFILE_ID: &str = "plan04-project-agent-profile";
const NOW: &str = "2026-08-21T00:00:00.000Z";

const TASK_ADAPTIVE_OPERATION: &str = "task.adaptive";

async fn database() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    Arc::new(SqliteDb::new(pool))
}

async fn fixture() -> (Arc<SqliteDb>, TaskService) {
    fixture_with_allowed_operations(&["split", "sequence", "replace"]).await
}

/// Seed the same Project with an exact adaptive grant. Baseline revisions are
/// immutable by trigger — correctly so — which means a narrowed envelope has
/// to be authored up front rather than patched into an approved revision.
async fn fixture_with_allowed_operations(
    allowed_task_operations: &[&str],
) -> (Arc<SqliteDb>, TaskService) {
    let db = database().await;
    sqlx::query(
        "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
         VALUES ('plan04-user', 'plan04@example.test', 'test', 'PLAN-04', ?, ?)",
    )
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("user");
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: PROJECT_ID.to_owned(),
            name: "PLAN-04".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some("plan04-user".to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("project");
    RepoRepo::create(
        &*db,
        CreateRepo {
            id: REPO_ID.to_owned(),
            project_id: PROJECT_ID.to_owned(),
            name: "plan04".to_owned(),
            remote_url: "file:///tmp/plan04".to_owned(),
            local_path: None,
            work_mode: WorkMode::DirectMerge,
            default_branch: "main".to_owned(),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("repo");
    ProjectRepo::update(
        &*db,
        UpdateProject {
            id: PROJECT_ID.to_owned(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(REPO_ID.to_owned())),
            paused_at: None,
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("primary repo");

    sqlx::query(
        "INSERT INTO project_charter
         (id, account_id, project_id, project_mode, maturity, lifecycle,
          current_approved_revision_id, version, created_at, updated_at)
         VALUES (?, 'plan04-user', ?, 'compact', 'mvp', 'attached', NULL, 1, ?, ?)",
    )
    .bind(CHARTER_ID)
    .bind(PROJECT_ID)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("charter");
    sqlx::query(
        "INSERT INTO project_charter_revision
         (id, charter_id, revision, base_revision, lifecycle, schema_version,
          render_version, content_json, rendered_view, change_summary,
          author_type, author_id, source_refs_json, content_digest,
          rendered_digest, created_at)
         VALUES (?, ?, 1, 0, 'approved', 'charter@1', 'charter-render@1',
                 '{}', '# PLAN-04', 'fixture', 'user', 'plan04-user',
                 '[]', 'charter-content', 'charter-rendered', ?)",
    )
    .bind(CHARTER_REVISION_ID)
    .bind(CHARTER_ID)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("charter revision");
    sqlx::query("UPDATE project_charter SET current_approved_revision_id = ? WHERE id = ?")
        .bind(CHARTER_REVISION_ID)
        .bind(CHARTER_ID)
        .execute(db.pool())
        .await
        .expect("charter approval pointer");
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
    .expect("charter pointer");
    sqlx::query(
        "INSERT INTO project_milestone
         (id, project_id, milestone_sequence, milestone_key, display_label,
          lifecycle, blocker_reason_json, stale_reason_json,
          reconciliation_reason_json, version, created_at, updated_at)
         VALUES (?, ?, 1, 'M001', 'PLAN-04', 'active', '[]', '[]', '[]', 1, ?, ?)",
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
         (id, milestone_id, revision, base_revision, lifecycle, display_label,
          outcome, included_scope_json, excluded_scope_json, charter_revision_id,
          document_revisions_json, task_selection_json, dependencies_json,
          risks_json, acceptance_checks_json, evidence_requirements_json,
          known_issues_json, change_summary, schema_version, render_version,
          rendered_view, content_digest, rendered_digest, author_type, author_id,
          source_refs_json, created_at)
         VALUES (?, ?, 1, 0, 'approved', 'PLAN-04', 'ship-the-approved-outcome',
                 '[]', '[]', ?, '[]', '[]', '[]', '[]', '[]', '[]', '[]',
                 'fixture', 'milestone@1', 'milestone-render@1', '# PLAN-04',
                 'milestone-content', 'milestone-rendered', 'user',
                 'plan04-user', '[]', ?)",
    )
    .bind(MILESTONE_REVISION_ID)
    .bind(MILESTONE_ID)
    .bind(CHARTER_REVISION_ID)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("milestone revision");

    let envelope = json!({
        "allowed_task_operations": allowed_task_operations,
        "fixed_outcomes": ["ship-the-approved-outcome"],
        "fixed_acceptance": ["acceptance-r1"],
        "fixed_risk_classes": ["low"],
        "forbidden_side_effects": ["publish", "deploy"],
        "elevated_operations": ["none"],
    });
    sqlx::query(
        "INSERT INTO project_execution_baseline
         (id, project_id, current_revision_id, lifecycle, version, created_at, updated_at)
         VALUES (?, ?, ?, 'active', 1, ?, ?)",
    )
    .bind(BASELINE_ID)
    .bind(PROJECT_ID)
    .bind(BASELINE_REVISION_ID)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("baseline");
    sqlx::query(
        "INSERT INTO project_execution_baseline_revision
         (id, baseline_id, revision, base_revision, lifecycle,
          charter_revision_id, document_revisions_json, plan_items_json,
          milestone_id, milestone_ids_json, milestone_definition_revision_ids_json,
          primary_milestone_id, release_policy_json, release_policy_revision,
          release_policy_digest, acceptance_matrix_json, capability_classes_json,
          risk_classes_json, adaptive_envelope_json, elevated_operations_json,
          exclusions_json, rollback_recovery_json, schema_version, render_version,
          rendered_view, content_digest, rendered_digest, source_refs_json, created_at)
         VALUES (?, ?, 1, 0, 'approved', ?, '[]', '[\"plan-1\"]', ?,
                 '[\"plan04-milestone\"]', '[\"plan04-milestone-revision\"]', ?, '{}', 'release-r1', 'release-digest',
                 '[{\"id\":\"acceptance-r1\",\"description\":\"acceptance\",\"required\":true}]',
                 '[\"repository_write\"]', '[\"low\"]', ?, '[\"none\"]',
                 '[\"publish\",\"deploy\"]', '{}', 'baseline@1',
                 'baseline-render@1', '# PLAN-04 baseline', 'baseline-content',
                 'baseline-rendered', '[]', ?)",
    )
    .bind(BASELINE_REVISION_ID)
    .bind(BASELINE_ID)
    .bind(CHARTER_REVISION_ID)
    .bind(MILESTONE_ID)
    .bind(MILESTONE_ID)
    .bind(envelope.to_string())
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("baseline revision");
    sqlx::query(
        "INSERT INTO project_execution_baseline_approval
         (id, baseline_id, revision_id, expected_project_version,
          principal_type, principal_id, authorization_basis,
          authorization_action, explicit_event, authorization_occurred_at,
          content_digest, rendered_digest, lifecycle, idempotency_key,
          created_at, updated_at)
         VALUES ('plan04-approval', ?, ?, 3, 'user', 'plan04-user',
                 'explicit approval', 'project.execution_baseline.approve',
                 'plan04-approval-event', ?, 'baseline-content',
                 'baseline-rendered', 'active', 'plan04-approval-key', ?, ?)",
    )
    .bind(BASELINE_ID)
    .bind(BASELINE_REVISION_ID)
    .bind(NOW)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("baseline approval");

    TaskRepo::create(
        &*db,
        CreateTask {
            id: ROOT_TASK_ID.to_owned(),
            project_id: PROJECT_ID.to_owned(),
            repo_id: Some(REPO_ID.to_owned()),
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "Approved outcome".to_owned(),
            description: Some("acceptance-r1".to_owned()),
            task_type: "task".to_owned(),
            status: "todo".to_owned(),
            is_automation: false,
            priority: 1,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("root Task");
    sqlx::query(
        "INSERT INTO project_task_governance
         (task_id, project_id, charter_revision_id, baseline_id,
          baseline_revision_id, plan_item_id, milestone_id,
          document_revisions_json, capability_class, risk_class, runnable,
          replacement_of_task_id, provenance_json, version, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, 'plan-1', ?, '[]', 'repository_write', 'low',
                 1, NULL, ?, 1, ?, ?)",
    )
    .bind(ROOT_TASK_ID)
    .bind(PROJECT_ID)
    .bind(CHARTER_REVISION_ID)
    .bind(BASELINE_ID)
    .bind(BASELINE_REVISION_ID)
    .bind(MILESTONE_ID)
    .bind(
        json!({
            "schema": "forge.task-governance/v1",
            "origin_plan_item_id": "plan-1",
            "governing_baseline_id": BASELINE_ID,
            "governing_baseline_revision_id": BASELINE_REVISION_ID,
            "governing_baseline_content_digest": "baseline-content",
            "governing_baseline_rendered_digest": "baseline-rendered",
            "fixed_outcomes": ["ship-the-approved-outcome"],
            "fixed_acceptance": ["acceptance-r1"],
            "fixed_risk_classes": ["low"],
            "forbidden_side_effects": ["publish", "deploy"],
            "elevated_operations": ["none"]
        })
        .to_string(),
    )
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("root governance");

    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(32)));
    (db, service)
}

/// The native Project-Agent adapter is deliberately a second fixture layer.
/// The two original tests below exercise the TaskService seam directly; the
/// adapter tests must prove that the host-derived Project scope and active
/// binding reach the same service without an alternate persistence path.
struct AdapterFixture {
    db: Arc<SqliteDb>,
    service: TaskService,
    provider: CoordinationToolProvider,
    scope: CanonicalScope,
}

async fn adapter_fixture() -> AdapterFixture {
    let (db, service) = fixture().await;
    AgentRepo::create_identity_with_profile(
        &*db,
        CreateAgentIdentity {
            id: AGENT_ID.to_owned(),
            name: "PLAN-04 Project Agent".to_owned(),
            description: None,
            max_concurrent_tasks: 4,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some("plan04-user".to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling:
                r#"{"permissions":["read_project","propose_task","task_write"]}"#.to_owned(),
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
            tool_policy_json: r#"{"permissions":["read_project","propose_task","task_write"]}"#
                .to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("Project Agent identity/profile");
    // Project creation installs one setup-required binding with no identity.
    // Promote that canonical row in place so the fixture does not violate the
    // one-active-binding invariant by inserting a second row.
    sqlx::query(
        "UPDATE project_agent_binding
         SET identity_id = ?, profile_id = ?, state = 'active',
             autonomy_policy_json = '{}',
             permission_ceiling_json = ?, subscriptions_json = '{}',
             wake_budget = 10, updated_at = ?
         WHERE project_id = ? AND state = 'agent_setup_required'",
    )
    .bind(AGENT_ID)
    .bind(PROFILE_ID)
    .bind(r#"{"permissions":["read_project","propose_task","task_write"]}"#)
    .bind(NOW)
    .bind(PROJECT_ID)
    .execute(db.pool())
    .await
    .expect("Project Agent binding");

    let provider = CoordinationToolProvider::new(Arc::clone(&db));
    provider.set_task_service(Arc::new(service.clone()));
    AdapterFixture {
        db,
        service,
        provider,
        scope: CanonicalScope {
            scope_type: CanonicalScopeType::Project,
            scope_id: PROJECT_ID.to_owned(),
            workspace_access: WorkspaceAccess::Deny,
        },
    }
}

async fn adapter_propose(
    fixture: &AdapterFixture,
    operation: &str,
    payload: Value,
    dedupe_key: &str,
) -> Result<Value, AgentHostError> {
    ForgeToolProvider::propose(
        &fixture.provider,
        AGENT_ID,
        &fixture.scope,
        operation,
        json!({
            "payload": payload,
            "dedupe_key": dedupe_key,
            "correlation_id": format!("correlation-{dedupe_key}"),
        }),
    )
    .await
}

fn structured_error_code(error: AgentHostError) -> String {
    match error {
        AgentHostError::StructuredOutcome(outcome) => outcome.code.as_str().to_owned(),
        other => panic!("expected a structured orchestration error, got {other:?}"),
    }
}

fn assert_native_success(outcome: &Value, operation: &str) {
    assert_eq!(outcome["operation"], operation, "outcome: {outcome}");
    assert_eq!(outcome["code"], "ok", "outcome: {outcome}");
    assert_eq!(outcome["status"], "succeeded", "outcome: {outcome}");
    assert!(
        outcome["receipt_id"].as_str().is_some(),
        "outcome: {outcome}"
    );
    assert!(outcome["event_id"].as_str().is_some(), "outcome: {outcome}");
}

async fn governance_row(db: &SqliteDb, task_id: &str) -> (String, String, String, String, String) {
    sqlx::query_as(
        "SELECT baseline_id, baseline_revision_id, plan_item_id,
                risk_class, provenance_json
         FROM project_task_governance WHERE task_id = ?",
    )
    .bind(task_id)
    .fetch_one(db.pool())
    .await
    .expect("governance row")
}

#[tokio::test]
async fn plan04_split_sequence_replace_preserve_the_approved_boundaries() {
    let (db, service) = fixture().await;
    let children = service
        .create_subtasks(
            ROOT_TASK_ID.to_owned(),
            vec![
                services::NewSubtaskInput {
                    title: "First bounded slice".to_owned(),
                    description: Some("same acceptance".to_owned()),
                    assignee_id: None,
                },
                services::NewSubtaskInput {
                    title: "Second bounded slice".to_owned(),
                    description: Some("same acceptance".to_owned()),
                    assignee_id: None,
                },
            ],
        )
        .await
        .expect("split is inside the adaptive envelope");
    assert_eq!(children.len(), 2);
    for child in &children {
        let (baseline, revision, plan_item, risk, provenance) =
            governance_row(&db, &child.id).await;
        assert_eq!(baseline, BASELINE_ID);
        assert_eq!(revision, BASELINE_REVISION_ID);
        assert_eq!(plan_item, "plan-1");
        assert_eq!(risk, "low");
        let provenance: Value = serde_json::from_str(&provenance).expect("provenance JSON");
        assert_eq!(provenance["origin_task_id"], ROOT_TASK_ID);
        assert_eq!(provenance["replacement_of_task_id"], ROOT_TASK_ID);
        assert_eq!(provenance["adaptive_operation"], "split");
        assert_eq!(provenance["fixed_acceptance"], json!(["acceptance-r1"]));
        assert_eq!(
            provenance["forbidden_side_effects"],
            json!(["publish", "deploy"])
        );
        assert_eq!(provenance["elevated_operations"], json!(["none"]));
    }

    service
        .reorder_subtasks(
            ROOT_TASK_ID.to_owned(),
            children.iter().rev().map(|task| task.id.clone()).collect(),
        )
        .await
        .expect("sequence is inside the adaptive envelope");
    let ordered = TaskRepo::list_subtasks_ordered(&*db, ROOT_TASK_ID)
        .await
        .expect("ordered children");
    assert_eq!(ordered[0].id, children[1].id);
    assert_eq!(ordered[1].id, children[0].id);

    let replacement = service
        .replace_task(
            ROOT_TASK_ID,
            "Approved outcome replacement",
            Some("same acceptance".to_owned()),
        )
        .await
        .expect("replace is inside the adaptive envelope");
    let (baseline, revision, plan_item, risk, provenance) =
        governance_row(&db, &replacement.id).await;
    assert_eq!(
        (baseline, revision, plan_item, risk),
        (
            BASELINE_ID.to_owned(),
            BASELINE_REVISION_ID.to_owned(),
            "plan-1".to_owned(),
            "low".to_owned(),
        )
    );
    let provenance: Value = serde_json::from_str(&provenance).expect("replacement provenance");
    assert_eq!(provenance["origin_task_id"], ROOT_TASK_ID);
    assert_eq!(provenance["replacement_of_task_id"], ROOT_TASK_ID);
    assert_eq!(provenance["adaptive_operation"], "replace");
    let replacement_of: String = sqlx::query_scalar(
        "SELECT replacement_of_task_id FROM project_task_governance WHERE task_id = ?",
    )
    .bind(&replacement.id)
    .fetch_one(db.pool())
    .await
    .expect("replacement link");
    assert_eq!(replacement_of, ROOT_TASK_ID);
}

#[tokio::test]
async fn plan04_crossing_a_fixed_boundary_records_reconciliation_and_blocks() {
    let (db, service) = fixture().await;
    let governance = api_types::TaskGovernanceRequest {
        charter_revision_id: Some(CHARTER_REVISION_ID.to_owned()),
        baseline_id: Some(BASELINE_ID.to_owned()),
        baseline_revision_id: Some(BASELINE_REVISION_ID.to_owned()),
        plan_item_id: Some("plan-1".to_owned()),
        milestone_id: Some(MILESTONE_ID.to_owned()),
        document_revision_ids: Vec::new(),
        capability_class: Some("repository_write".to_owned()),
        risk_class: Some("low".to_owned()),
        provenance: Some(json!({"fixed_acceptance": ["changed-acceptance"]})),
    };
    let result = service
        .create_task_with_governance(
            PROJECT_ID,
            "Crosses approved risk/acceptance",
            None,
            Some(ROOT_TASK_ID.to_owned()),
            None,
            Some("sub_task".to_owned()),
            None,
            None,
            None,
            Some(governance.clone()),
        )
        .await
        .expect_err("fixed boundary crossing must be blocked");
    assert!(
        matches!(result, ServiceError::Conflict(message) if message.contains("reconciliation_required"))
    );
    let reconciliation: (String, String, String) = sqlx::query_as(
        "SELECT state, record_type, record_id
         FROM project_reconciliation_record
         WHERE project_id = ?",
    )
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("reconciliation record");
    assert_eq!(
        reconciliation,
        (
            "required".to_owned(),
            "task".to_owned(),
            ROOT_TASK_ID.to_owned()
        )
    );
    let task_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task WHERE project_id = ?")
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("task count");
    assert_eq!(task_count, 1, "blocked split creates no replacement Task");

    let blocked_again = service
        .create_subtasks(
            ROOT_TASK_ID.to_owned(),
            vec![services::NewSubtaskInput {
                title: "Blocked after reconciliation".to_owned(),
                description: None,
                assignee_id: None,
            }],
        )
        .await
        .expect_err("existing reconciliation remains a hard gate");
    assert!(
        matches!(blocked_again, ServiceError::Conflict(message) if message.contains("reconciliation_required"))
    );
}

#[tokio::test]
async fn plan04_project_agent_adapter_invokes_split_sequence_replace_and_replays_exactly() {
    let fixture = adapter_fixture().await;
    let board_revision: i64 = sqlx::query_scalar("SELECT board_revision FROM project WHERE id = ?")
        .bind(PROJECT_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("initial board revision");
    let split_payload = json!({
        "action": "split",
        "source_task_id": ROOT_TASK_ID,
        "expected_task_version": 1,
        "expected_board_revision": board_revision,
        "rationale": "keep the approved outcome executable",
        "items": [
            {"title": "First bounded slice", "description": "same acceptance"},
            {"title": "Second bounded slice", "description": "same acceptance"}
        ]
    });
    let first = adapter_propose(
        &fixture,
        TASK_ADAPTIVE_OPERATION,
        split_payload.clone(),
        "plan04-adapter-split",
    )
    .await
    .expect("Project-Agent split adapter invocation");
    assert_native_success(&first, TASK_ADAPTIVE_OPERATION);
    assert_eq!(first["replayed"], false);
    let first_receipt = first["receipt_id"].clone();
    let first_event = first["event_id"].clone();

    let children: Vec<(String, i64)> = sqlx::query_as(
        "SELECT id, subtask_order FROM task
         WHERE parent_task_id = ? AND deleted_at IS NULL
         ORDER BY subtask_order ASC, id ASC",
    )
    .bind(ROOT_TASK_ID)
    .fetch_all(fixture.db.pool())
    .await
    .expect("split children");
    assert_eq!(children.len(), 2);

    // The receipt is the replay boundary, not the current Task projection.
    // Mutate a live child between attempts: the exact retry must return the
    // frozen receipt snapshot rather than reconstructing the current row.
    sqlx::query("UPDATE task SET title = 'Live projection changed' WHERE id = ?")
        .bind(&children[0].0)
        .execute(fixture.db.pool())
        .await
        .expect("mutate live child projection");
    let replay = adapter_propose(
        &fixture,
        TASK_ADAPTIVE_OPERATION,
        split_payload.clone(),
        "plan04-adapter-split",
    )
    .await
    .expect("exact split replay");
    assert_native_success(&replay, TASK_ADAPTIVE_OPERATION);
    assert_eq!(replay["replayed"], true);
    assert_eq!(replay["receipt_id"], first_receipt);
    assert_eq!(replay["event_id"], first_event);
    assert_eq!(replay["result"], first["result"]);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task WHERE parent_task_id = ? AND deleted_at IS NULL",
        )
        .bind(ROOT_TASK_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("split count after replay"),
        2
    );

    // Reusing the receipt key with a changed title is an input conflict,
    // never another child or a best-effort replay of the old payload.
    let changed_payload = json!({
        "action": "split",
        "source_task_id": ROOT_TASK_ID,
        "expected_task_version": 1,
        "expected_board_revision": board_revision,
        "rationale": "keep the approved outcome executable",
        "items": [
            {"title": "Changed bounded slice", "description": "same acceptance"},
            {"title": "Second bounded slice", "description": "same acceptance"}
        ]
    });
    let changed = adapter_propose(
        &fixture,
        TASK_ADAPTIVE_OPERATION,
        changed_payload,
        "plan04-adapter-split",
    )
    .await
    .expect_err("changed split input must conflict");
    assert_eq!(
        structured_error_code(changed),
        "idempotency_conflict",
        "same key with changed title must not be treated as a replay"
    );

    let changed_description = json!({
        "action": "split",
        "source_task_id": ROOT_TASK_ID,
        "expected_task_version": 1,
        "expected_board_revision": board_revision,
        "rationale": "keep the approved outcome executable",
        "items": [
            {"title": "First bounded slice", "description": "changed acceptance"},
            {"title": "Second bounded slice", "description": "same acceptance"}
        ]
    });
    let changed = adapter_propose(
        &fixture,
        TASK_ADAPTIVE_OPERATION,
        changed_description,
        "plan04-adapter-split",
    )
    .await
    .expect_err("changed split description must conflict");
    assert_eq!(
        structured_error_code(changed),
        "idempotency_conflict",
        "same key with changed description/acceptance must not be a replay"
    );

    let ordered_ids = vec![children[1].0.clone(), children[0].0.clone()];
    let root_version: i64 = sqlx::query_scalar("SELECT version FROM task WHERE id = ?")
        .bind(ROOT_TASK_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("root version before sequence");
    let board_revision: i64 = sqlx::query_scalar("SELECT board_revision FROM project WHERE id = ?")
        .bind(PROJECT_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("board revision before sequence");
    let sequence = adapter_propose(
        &fixture,
        TASK_ADAPTIVE_OPERATION,
        json!({
            "action": "sequence",
            "source_task_id": ROOT_TASK_ID,
            "ordered_task_ids": ordered_ids,
            "expected_task_version": root_version,
            "expected_board_revision": board_revision,
            "rationale": "preserve the approved delivery order"
        }),
        "plan04-adapter-sequence",
    )
    .await
    .expect("Project-Agent sequence adapter invocation");
    assert_native_success(&sequence, TASK_ADAPTIVE_OPERATION);
    let ordered: Vec<String> = TaskRepo::list_subtasks_ordered(&*fixture.db, ROOT_TASK_ID)
        .await
        .expect("ordered children")
        .into_iter()
        .map(|task| task.id)
        .collect();
    assert_eq!(ordered, vec![children[1].0.clone(), children[0].0.clone()]);

    let root_version: i64 = sqlx::query_scalar("SELECT version FROM task WHERE id = ?")
        .bind(ROOT_TASK_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("root version before replacement");
    let board_revision: i64 = sqlx::query_scalar("SELECT board_revision FROM project WHERE id = ?")
        .bind(PROJECT_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("board revision before replacement");
    let replacement = adapter_propose(
        &fixture,
        TASK_ADAPTIVE_OPERATION,
        json!({
            "action": "replace",
            "source_task_id": ROOT_TASK_ID,
            "title": "Approved outcome replacement",
            "description": "same acceptance",
            "expected_task_version": root_version,
            "expected_board_revision": board_revision,
            "rationale": "replace the wedged implementation without changing scope"
        }),
        "plan04-adapter-replace",
    )
    .await
    .expect("Project-Agent replace adapter invocation");
    assert_native_success(&replacement, TASK_ADAPTIVE_OPERATION);
    let replacement_id: String = sqlx::query_scalar(
        "SELECT task_id FROM project_task_governance
         WHERE replacement_of_task_id = ? ORDER BY created_at DESC LIMIT 1",
    )
    .bind(ROOT_TASK_ID)
    .fetch_one(fixture.db.pool())
    .await
    .expect("replace adapter must persist a replacement Task");
    assert_ne!(replacement_id, ROOT_TASK_ID);
}

#[tokio::test]
async fn plan04_adaptive_adapter_replay_is_frozen_before_mutable_gate_recheck() {
    let fixture = adapter_fixture().await;
    let split_payload = adaptive_split_payload(board_revision(&fixture.db).await);
    let first = adapter_propose(
        &fixture,
        TASK_ADAPTIVE_OPERATION,
        split_payload.clone(),
        "plan04-replay-before-gate",
    )
    .await
    .expect("initial adaptive command");
    assert_native_success(&first, TASK_ADAPTIVE_OPERATION);

    // A response-loss retry must resolve the immutable receipt before it
    // consults mutable baseline/approval state.  This is intentionally a
    // stale live gate, not a stale command input: the exact command already
    // committed and must replay its frozen snapshots without a new child or
    // reconciliation record.
    sqlx::query("UPDATE project_execution_baseline SET lifecycle = 'superseded' WHERE id = ?")
        .bind(BASELINE_ID)
        .execute(fixture.db.pool())
        .await
        .expect("mutate live baseline gate");
    let replay = adapter_propose(
        &fixture,
        TASK_ADAPTIVE_OPERATION,
        split_payload,
        "plan04-replay-before-gate",
    )
    .await
    .expect("exact replay must precede mutable gate validation");
    assert_native_success(&replay, TASK_ADAPTIVE_OPERATION);
    assert_eq!(replay["replayed"], true);
    assert_eq!(replay["receipt_id"], first["receipt_id"]);
    assert_eq!(replay["event_id"], first["event_id"]);
    assert_eq!(replay["result"], first["result"]);
    assert_eq!(adaptive_children_count(&fixture.db).await, 2);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_reconciliation_record
             WHERE project_id = ? AND state = 'required'",
        )
        .bind(PROJECT_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("replay reconciliation count"),
        0
    );
}

#[tokio::test]
async fn plan04_adaptive_adapter_expected_versions_are_stale_and_cas_safe() {
    let fixture = adapter_fixture().await;
    let board_revision: i64 = sqlx::query_scalar("SELECT board_revision FROM project WHERE id = ?")
        .bind(PROJECT_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("initial board revision");
    let payload = json!({
        "action": "split",
        "source_task_id": ROOT_TASK_ID,
        "expected_task_version": 1,
        "expected_board_revision": board_revision,
        "rationale": "one CAS winner",
        "items": [
            {"title": "CAS first", "description": "same acceptance"},
            {"title": "CAS second", "description": "same acceptance"}
        ]
    });
    let (left, right) = tokio::join!(
        adapter_propose(
            &fixture,
            TASK_ADAPTIVE_OPERATION,
            payload.clone(),
            "plan04-cas-left",
        ),
        adapter_propose(
            &fixture,
            TASK_ADAPTIVE_OPERATION,
            payload.clone(),
            "plan04-cas-right",
        )
    );
    assert_eq!(left.is_ok() as u8 + right.is_ok() as u8, 1);
    let stale = if left.is_err() { left } else { right };
    let stale = stale.expect_err("one concurrent CAS caller must lose");
    assert_eq!(structured_error_code(stale), "version_conflict");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task WHERE parent_task_id = ? AND deleted_at IS NULL",
        )
        .bind(ROOT_TASK_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("CAS child count"),
        2,
        "the losing writer must leave no duplicate children"
    );

    // A later fresh command carrying the old Task/board versions is stale,
    // even though it has a new receipt key.
    let stale_retry = adapter_propose(
        &fixture,
        TASK_ADAPTIVE_OPERATION,
        payload,
        "plan04-cas-stale-retry",
    )
    .await
    .expect_err("old expected versions must be rejected");
    assert_eq!(structured_error_code(stale_retry), "version_conflict");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task WHERE parent_task_id = ? AND deleted_at IS NULL",
        )
        .bind(ROOT_TASK_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("stale child count"),
        2
    );
}

fn adaptive_split_payload(board_revision: i64) -> Value {
    json!({
        "action": "split",
        "source_task_id": ROOT_TASK_ID,
        "expected_task_version": 1,
        "expected_board_revision": board_revision,
        "rationale": "preserve the approved outcome",
        "items": [
            {"title": "bounded slice one", "description": "same acceptance"},
            {"title": "bounded slice two", "description": "same acceptance"}
        ]
    })
}

async fn board_revision(db: &SqliteDb) -> i64 {
    sqlx::query_scalar("SELECT board_revision FROM project WHERE id = ?")
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("board revision")
}

async fn adaptive_children_count(db: &SqliteDb) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM task
         WHERE parent_task_id = ? AND deleted_at IS NULL",
    )
    .bind(ROOT_TASK_ID)
    .fetch_one(db.pool())
    .await
    .expect("adaptive child count")
}

#[tokio::test]
async fn plan04_adaptive_adapter_rejects_stale_baseline_approval_and_reconciliation() {
    // A superseded current baseline is not an executable adaptive envelope.
    let stale_baseline = adapter_fixture().await;
    sqlx::query("UPDATE project_execution_baseline SET lifecycle = 'superseded' WHERE id = ?")
        .bind(BASELINE_ID)
        .execute(stale_baseline.db.pool())
        .await
        .expect("supersede baseline");
    let result = adapter_propose(
        &stale_baseline,
        TASK_ADAPTIVE_OPERATION,
        adaptive_split_payload(board_revision(&stale_baseline.db).await),
        "plan04-stale-baseline",
    )
    .await
    .expect_err("stale baseline must reject adaptive split");
    assert_eq!(structured_error_code(result), "validation_error");
    assert_eq!(adaptive_children_count(&stale_baseline.db).await, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_reconciliation_record
             WHERE project_id = ? AND state = 'required'",
        )
        .bind(PROJECT_ID)
        .fetch_one(stale_baseline.db.pool())
        .await
        .expect("stale baseline reconciliation"),
        1
    );

    // A baseline approval that was authoritatively revoked is equally stale,
    // even when the baseline itself remains active. Approval receipts are
    // immutable apart from their active -> revoked/consumed lifecycle, so use
    // that allowed state transition instead of forging a digest in place.
    let stale_approval = adapter_fixture().await;
    sqlx::query(
        "UPDATE project_execution_baseline_approval
         SET lifecycle = 'revoked', updated_at = '2026-08-21T00:01:00.000Z'
         WHERE id = 'plan04-approval'",
    )
    .execute(stale_approval.db.pool())
    .await
    .expect("revoke approval receipt");
    let result = adapter_propose(
        &stale_approval,
        TASK_ADAPTIVE_OPERATION,
        adaptive_split_payload(board_revision(&stale_approval.db).await),
        "plan04-stale-approval",
    )
    .await
    .expect_err("stale approval must reject adaptive split");
    assert_eq!(structured_error_code(result), "validation_error");
    assert_eq!(adaptive_children_count(&stale_approval.db).await, 0);

    // Reconciliation is a durable Project gate. Seed it through the native
    // TaskService seam, then prove the Project-Agent adapter cannot bypass it
    // with an otherwise valid split payload.
    let reconciled = adapter_fixture().await;
    let changed_governance = api_types::TaskGovernanceRequest {
        charter_revision_id: Some(CHARTER_REVISION_ID.to_owned()),
        baseline_id: Some(BASELINE_ID.to_owned()),
        baseline_revision_id: Some(BASELINE_REVISION_ID.to_owned()),
        plan_item_id: Some("plan-1".to_owned()),
        milestone_id: Some(MILESTONE_ID.to_owned()),
        document_revision_ids: Vec::new(),
        capability_class: Some("repository_write".to_owned()),
        risk_class: Some("low".to_owned()),
        provenance: Some(json!({"fixed_acceptance": ["changed"]})),
    };
    let result = reconciled
        .service
        .create_task_with_governance(
            PROJECT_ID,
            "reconciliation seed",
            Some("changed acceptance".to_owned()),
            Some(ROOT_TASK_ID.to_owned()),
            None,
            Some("sub_task".to_owned()),
            None,
            None,
            None,
            Some(changed_governance),
        )
        .await
        .expect_err("changed fixed boundary must seed reconciliation");
    assert!(matches!(
        result,
        ServiceError::Conflict(message) if message.contains("reconciliation_required")
    ));
    let result = adapter_propose(
        &reconciled,
        TASK_ADAPTIVE_OPERATION,
        adaptive_split_payload(board_revision(&reconciled.db).await),
        "plan04-existing-reconciliation",
    )
    .await
    .expect_err("existing reconciliation must remain a hard adapter gate");
    assert_eq!(structured_error_code(result), "validation_error");
    assert_eq!(adaptive_children_count(&reconciled.db).await, 0);
}

#[tokio::test]
async fn plan04_adaptive_adapter_rejects_changed_acceptance_and_fixed_boundaries() {
    let fixture = adapter_fixture().await;
    let mut changed_acceptance = adaptive_split_payload(board_revision(&fixture.db).await);
    changed_acceptance["items"][0]["description"] =
        Value::String("an outcome outside acceptance-r1".to_owned());
    changed_acceptance["fixed_acceptance"] = json!(["changed-acceptance"]);
    changed_acceptance["fixed_risk_classes"] = json!(["high"]);
    changed_acceptance["forbidden_side_effects"] = json!(["publish"]);
    changed_acceptance["elevated_operations"] = json!(["deploy"]);
    let result = adapter_propose(
        &fixture,
        TASK_ADAPTIVE_OPERATION,
        changed_acceptance,
        "plan04-changed-fixed-boundaries",
    )
    .await
    .expect_err("altered acceptance/fixed boundaries must be rejected");
    assert_eq!(structured_error_code(result), "validation_error");
    assert_eq!(adaptive_children_count(&fixture.db).await, 0);
    // The forged boundary fields are rejected by the strict adapter contract
    // before the shared command reaches persistence. No reconciliation is
    // recorded for an input that was never admitted, and no receipt/event is
    // left behind. The in-boundary reconciliation path remains covered by
    // `plan04_crossing_a_fixed_boundary_records_reconciliation_and_blocks`.
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_reconciliation_record
             WHERE project_id = ? AND state = 'required'",
        )
        .bind(PROJECT_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("fixed-boundary reconciliation"),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE operation = ? AND idempotency_key = ?",
        )
        .bind(TASK_ADAPTIVE_OPERATION)
        .bind("plan04-changed-fixed-boundaries")
        .fetch_one(fixture.db.pool())
        .await
        .expect("strict rejection receipt count"),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM domain_event WHERE event_type = ?",)
            .bind(TASK_ADAPTIVE_OPERATION)
            .fetch_one(fixture.db.pool())
            .await
            .expect("strict rejection event count"),
        0
    );
}

#[tokio::test]
async fn plan04_adaptive_adapter_failure_is_atomic_and_leaves_no_residue() {
    let fixture = adapter_fixture().await;
    sqlx::query(
        "CREATE TRIGGER plan04_fail_adaptive_governance
         BEFORE INSERT ON project_task_governance
         WHEN NEW.task_id <> 'plan04-root-task'
         BEGIN
             SELECT RAISE(ABORT, 'PLAN-04 deterministic atomic failure');
         END",
    )
    .execute(fixture.db.pool())
    .await
    .expect("install deterministic atomic-failure trigger");

    let result = adapter_propose(
        &fixture,
        TASK_ADAPTIVE_OPERATION,
        adaptive_split_payload(board_revision(&fixture.db).await),
        "plan04-atomic-failure",
    )
    .await
    .expect_err("governance failure must abort the adaptive command");
    assert_eq!(structured_error_code(result), "internal_failure");
    assert_eq!(adaptive_children_count(&fixture.db).await, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project_task_governance")
            .fetch_one(fixture.db.pool())
            .await
            .expect("governance rows after rollback"),
        1,
        "the source governance row must be the only surviving row"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE operation = ? AND idempotency_key = ?",
        )
        .bind(TASK_ADAPTIVE_OPERATION)
        .bind("plan04-atomic-failure")
        .fetch_one(fixture.db.pool())
        .await
        .expect("receipt rows after rollback"),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE entity_type = 'task' AND entity_id <> ?",
        )
        .bind(ROOT_TASK_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("event rows after rollback"),
        0
    );
}

#[tokio::test]
async fn plan04_generic_task_propose_cannot_bypass_adaptive_parent_governance() {
    let fixture = adapter_fixture().await;
    let result = adapter_propose(
        &fixture,
        "task.propose",
        json!({
            "title": "unreviewed child through generic proposal",
            "description": "This must not bypass the approved adaptive envelope.",
            "parent_task_id": ROOT_TASK_ID,
            "task_type": "planning_task",
            "priority": 1,
            "merge_config": null,
            "role_assignments": null,
            "governance": null
        }),
        "plan04-generic-parent-bypass",
    )
    .await
    .expect_err("generic task.propose must not reshape a governed root Task");
    let code = structured_error_code(result);
    assert!(
        matches!(code.as_str(), "validation_error" | "policy_denied"),
        "generic parent bypass returned unexpected code: {code}"
    );
    assert_eq!(adaptive_children_count(&fixture.db).await, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task WHERE project_id = ?")
            .bind(PROJECT_ID)
            .fetch_one(fixture.db.pool())
            .await
            .expect("task count after generic bypass"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE operation = 'task.propose' AND idempotency_key = ?",
        )
        .bind("plan04-generic-parent-bypass")
        .fetch_one(fixture.db.pool())
        .await
        .expect("generic bypass receipt count"),
        0,
        "a rejected generic bypass must not leave a receipt"
    );
}

/// F8/8.1.1: the closed vocabulary must have exactly one source of truth.
/// A hand-written diagnostic literal is the same drift that let
/// `task.propose`/`task.adaptive` reach an approved baseline, so the message
/// a caller reads is derived from the enum itself.
#[test]
fn adaptive_vocabulary_diagnostic_derives_from_the_closed_enum() {
    use api_types::AdaptiveTaskOperation;

    assert_eq!(
        AdaptiveTaskOperation::ALL.len(),
        3,
        "the adaptive vocabulary is exactly split/sequence/replace"
    );
    assert_eq!(
        services::adaptive_task_operation_supported_values(),
        AdaptiveTaskOperation::ALL
            .iter()
            .map(|operation| operation.as_str())
            .collect::<Vec<_>>()
            .join(", "),
        "the diagnostic must be derived from the enum, never a parallel literal"
    );
    for operation in AdaptiveTaskOperation::ALL {
        assert_eq!(
            AdaptiveTaskOperation::parse(operation.as_str()),
            Some(operation)
        );
    }
    for rejected in ["task.propose", "task.adaptive", "", "Split", "split "] {
        assert!(
            AdaptiveTaskOperation::parse(rejected).is_none(),
            "'{rejected}' must never parse as an adaptive verb"
        );
    }
}

/// 8.1.2: an envelope granting the same verb twice is malformed authority.
/// The closed type cannot express an unsupported verb, so the duplicate is
/// the remaining representable defect and must be named with the exact field
/// path and the allowed verbs.
#[test]
fn duplicate_adaptive_operation_is_rejected_with_the_exact_field_path() {
    use api_types::AdaptiveTaskOperation;

    let error = services::validate_adaptive_task_operations(&[
        AdaptiveTaskOperation::Split,
        AdaptiveTaskOperation::Split,
    ])
    .expect_err("a duplicate grant must be refused");
    assert!(
        error.contains("adaptive_envelope.allowed_task_operations"),
        "the diagnostic must name the exact field path, got: {error}"
    );
    assert!(
        error.contains("split") && error.contains("sequence") && error.contains("replace"),
        "the diagnostic must list the allowed verbs, got: {error}"
    );

    services::validate_adaptive_task_operations(&[
        AdaptiveTaskOperation::Split,
        AdaptiveTaskOperation::Replace,
    ])
    .expect("distinct grants remain valid");
}

/// A persisted legacy envelope carrying the pre-closure command names must
/// fail at active-baseline load naming the exact field, never silently admit
/// an unrecognized verb (F8).
#[test]
fn persisted_legacy_adaptive_envelope_fails_naming_the_field() {
    let legacy = r#"{"allowed_task_operations":["task.propose","task.adaptive"],
        "fixed_outcomes":[],"fixed_acceptance":[],"fixed_risk_classes":[],
        "forbidden_side_effects":[],"elevated_operations":[]}"#;
    let error = services::parse_persisted_adaptive_envelope(legacy)
        .expect_err("a legacy command-name envelope must not load");
    assert!(
        error.contains("adaptive_envelope.allowed_task_operations"),
        "the diagnostic must name the exact field path, got: {error}"
    );
    assert!(
        error.contains("split") && error.contains("sequence") && error.contains("replace"),
        "the diagnostic must list the allowed verbs, got: {error}"
    );
}

/// F9: a *denied no-op* must not create durable conflict truth.
///
/// The preserved failed run narrowed an envelope to exclude `replace`, called
/// it, and the miss recorded a canonical conflict plus a Task-scoped
/// reconciliation row — which `execution_gate()` then projected as a
/// Project-wide `ReconciliationRequired`. One rejected command that committed
/// no mutation stopped every unrelated Task in the Project. A denial is a
/// policy outcome; only a proven divergence between authoritative records is
/// reconciliation truth (D14).
#[tokio::test]
async fn plan04_denied_adaptive_no_op_creates_no_conflict_or_reconciliation() {
    // `replace` is valid vocabulary this baseline simply never granted — the
    // exact live shape.
    let (db, service) = fixture_with_allowed_operations(&["split"]).await;

    let error = service
        .replace_task(
            ROOT_TASK_ID.to_owned(),
            "replacement outside the granted envelope",
            Some("same acceptance".to_owned()),
        )
        .await
        .expect_err("replace is not granted by the narrowed envelope");

    // A denial, not a conflict: the message must name the allowed verbs so the
    // caller can propose a successor baseline instead of guessing.
    let rendered = error.to_string();
    assert!(
        !rendered.contains("reconciliation_required"),
        "a denied no-op must not be reported as reconciliation truth: {rendered}"
    );
    assert!(
        rendered.contains("split"),
        "the denial must name the allowed operations: {rendered}"
    );

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_canonical_conflict WHERE project_id = ?"
        )
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("conflict count"),
        0,
        "a rejected no-op must create no canonical conflict"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_reconciliation_record WHERE project_id = ?"
        )
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("reconciliation count"),
        0,
        "a rejected no-op must create no reconciliation row"
    );

    // The Project's execution authority is untouched: the still-granted
    // operation continues to work.
    service
        .create_subtasks(
            ROOT_TASK_ID.to_owned(),
            vec![services::NewSubtaskInput {
                title: "still-granted split".to_owned(),
                description: Some("same acceptance".to_owned()),
                assignee_id: None,
            }],
        )
        .await
        .expect("a denied no-op must not reduce unrelated execution authority");
}
