//! Task governance, capability admission, reconciliation, and workspace lease tests.

use super::*;
use events::EventBus;
use std::sync::Arc;

#[test]
fn workspace_lease_roles_preserve_reviewer_and_bound_custom_workers() {
    assert_eq!(
        canonical_workspace_lease_role("reviewer").expect("reviewer role"),
        "reviewer"
    );
    assert_eq!(
        canonical_workspace_lease_role("implementer").expect("custom worker role"),
        "worker"
    );
    assert_eq!(
        canonical_workspace_lease_role("orchestrator").expect("workflow worker role"),
        "worker"
    );
}
fn charter_backed_project() -> db::Project {
    db::Project {
        id: "project-1".to_owned(),
        name: "Project".to_owned(),
        settings: "{}".to_owned(),
        workflow_definition: "{}".to_owned(),
        workflow_template_name: None,
        primary_repo_id: Some("repo-1".to_owned()),
        paused_at: None,
        system_pause_reason: None,
        environment_pause_json: None,
        owner_id: None,
        project_hooks_json: "[]".to_owned(),
        project_work_epoch: 0,
        charter_status: "charter_backed".to_owned(),
        charter_setup_required: false,
        current_charter_id: Some("charter-1".to_owned()),
        current_charter_revision_id: Some("charter-revision-1".to_owned()),
        current_charter_version: 1,
        primary_milestone_id: Some("milestone-1".to_owned()),
        version: 1,
        created_at: "2026-08-13T00:00:00Z".to_owned(),
        updated_at: "2026-08-13T00:00:00Z".to_owned(),
    }
}

#[tokio::test]
async fn charter_backed_task_governance_is_runnable_before_repository_attachment() {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    let service = TaskService::new(
        Arc::new(db::SqliteDb::new(pool)),
        Arc::new(EventBus::new(4)),
    );
    let mut project = charter_backed_project();
    project.primary_repo_id = None;
    let governance = service
        .prepare_task_governance(&project, "task", None)
        .await
        .expect("implementation task can be recorded before the baseline")
        .expect("repository task receives a governance row");
    assert!(governance.runnable);
    assert_eq!(
        governance.charter_revision_id.as_deref(),
        Some("charter-revision-1")
    );
    assert!(governance.capability_class.is_none());
    assert!(governance.risk_class.is_none());
    assert!(governance.provenance_json.contains("charter_authority"));
}

fn minimal_task(id: &str, project_id: &str) -> db::Task {
    db::Task {
        condition: Default::default(),
        id: id.to_owned(),
        project_id: project_id.to_owned(),
        parent_task_id: None,
        assignee_type: None,
        assignee_id: None,
        title: "Task".to_owned(),
        description: None,
        task_type: "task".to_owned(),
        status: "todo".to_owned(),
        is_automation: false,
        priority: 1,
        board_position: 0.0,
        subtask_order: None,
        task_state_config: None,
        merge_config: None,
        metadata_json: None,
        plan: None,
        error_annotation: None,
        blocked_json: None,
        failed_json: None,
        entry_barrier_json: None,
        review_passed_at: None,
        archived_at: None,
        deleted_at: None,
        version: 1,
        created_at: "2026-08-13T00:00:00Z".to_owned(),
        updated_at: "2026-08-13T00:00:00Z".to_owned(),
    }
}

/// D14/8.1.4 characterization: a malformed or unknown adaptive operation
/// name (for example a command name such as `task.propose` mistakenly
/// passed as an operation) is a typed `validation_error`, never durable
/// conflict/reconciliation truth. Parsing happens before any governance
/// lookup, so this holds even for a Task/Project that do not exist.
#[tokio::test]
async fn authorize_adaptive_task_operation_rejects_malformed_verb_as_validation_error() {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations");
    let db = Arc::new(db::SqliteDb::new(pool));
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(4)));
    let task = minimal_task("task-does-not-exist", "project-does-not-exist");

    let error = service
        .authorize_adaptive_task_operation(&task, "task.propose")
        .await
        .expect_err("a command name is not an adaptive verb");
    match error {
        ServiceError::InvalidOperation { message } => {
            assert!(message.contains("task.propose"));
            assert!(message.contains("split"));
            assert!(message.contains("sequence"));
            assert!(message.contains("replace"));
            assert!(!message.contains("reconciliation_required"));
        }
        other => panic!("expected InvalidOperation (validation_error), got {other:?}"),
    }
    let reconciliation_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM project_reconciliation_record")
            .fetch_one(db.pool())
            .await
            .expect("reconciliation count");
    let conflict_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM project_canonical_conflict")
        .fetch_one(db.pool())
        .await
        .expect("conflict count");
    assert_eq!(reconciliation_count, 0);
    assert_eq!(conflict_count, 0);
}

#[tokio::test]
async fn charter_backed_repository_planning_task_is_runnable_read_only_work() {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    let service = TaskService::new(
        Arc::new(db::SqliteDb::new(pool)),
        Arc::new(EventBus::new(4)),
    );
    let governance = service
        .prepare_task_governance(&charter_backed_project(), "discovery", None)
        .await
        .expect("discovery plan can be recorded before baseline")
        .expect("repository discovery receives a governance row");
    assert!(governance.runnable);
    assert_eq!(
        governance.capability_class.as_deref(),
        Some("repository_read")
    );
    assert_eq!(governance.risk_class.as_deref(), Some("low"));
    assert!(governance.provenance_json.contains("charter_authority"));
}

#[tokio::test]
async fn pre_baseline_repository_planning_cannot_claim_write_capability() {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    let service = TaskService::new(
        Arc::new(db::SqliteDb::new(pool)),
        Arc::new(EventBus::new(4)),
    );
    let error = service
        .prepare_task_governance(
            &charter_backed_project(),
            "planning_task",
            Some(TaskGovernanceRequest {
                charter_revision_id: Some("charter-revision-1".to_owned()),
                plan_item_id: None,
                milestone_id: None,
                document_revision_ids: Vec::new(),
                capability_class: Some("repository_write".to_owned()),
                risk_class: Some("low".to_owned()),
                provenance: None,
            }),
        )
        .await
        .expect_err("pre-baseline planning must be read-only");
    assert!(error.to_string().contains("read-only"));
}

#[test]
fn capability_class_must_be_server_approved() {
    assert!(require_server_approved_capability_class(None).is_ok());
    assert!(require_server_approved_capability_class(Some("")).is_ok());
    for approved in SUPPORTED_CAPABILITY_PROFILES {
        assert!(require_server_approved_capability_class(Some(approved)).is_ok());
    }
    let error = require_server_approved_capability_class(Some("implementation"))
        .expect_err("baseline-authored classes outside the server set are rejected");
    let message = error.to_string();
    assert!(message.contains("'implementation'"));
    for approved in SUPPORTED_CAPABILITY_PROFILES {
        assert!(
            message.contains(approved),
            "error must enumerate allowed value {approved}: {message}"
        );
    }
}

#[tokio::test]
async fn unapproved_capability_class_is_rejected_at_task_creation() {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    let service = TaskService::new(
        Arc::new(db::SqliteDb::new(pool)),
        Arc::new(EventBus::new(4)),
    );
    let error = service
        .prepare_task_governance(
            &charter_backed_project(),
            "task",
            Some(TaskGovernanceRequest {
                charter_revision_id: Some("charter-revision-1".to_owned()),
                plan_item_id: None,
                milestone_id: None,
                document_revision_ids: Vec::new(),
                capability_class: Some("implementation".to_owned()),
                risk_class: None,
                provenance: None,
            }),
        )
        .await
        .expect_err("a class the lease issuer would refuse fails at creation");
    let message = error.to_string();
    assert!(message.contains("not server-approved"));
    assert!(message.contains("repository_write"));
}

// -- Capability-aware review (D16/D17, 8.2.5) --
//
// Repository mutation and independent read-only review are evaluated
// separately against the exact same recorded conflicts. Review of an
// already-committed result may continue only while every currently
// applicable conflict is acceptance/evidence/risk/reviewer/release
// neutral (`review_neutral`); repository mutation stays fully gated by
// any outstanding conflict regardless of neutrality.

fn reconciliation_conflict_row(
    conflict_code: &str,
    affected_paths: &[&str],
) -> ReconciliationConflictRow {
    ReconciliationConflictRow {
        reconciliation_id: "reconciliation-1".to_owned(),
        conflict_code: conflict_code.to_owned(),
        description: format!("{conflict_code} for task-1"),
        record_type: "task".to_owned(),
        record_id: "task-1".to_owned(),
        governing_record_type: "execution_baseline".to_owned(),
        governing_record_id: "baseline-1".to_owned(),
        affected_paths: affected_paths
            .iter()
            .map(|path| (*path).to_owned())
            .collect(),
        updated_at: "2026-08-13T00:00:00Z".to_owned(),
    }
}

#[test]
fn review_neutral_admits_plan_paths_but_blocks_every_fixed_boundary_field() {
    assert!(review_neutral(&reconciliation_conflict_row(
        "adaptive_task_governance_stale",
        &["plan_items", "milestone_id"],
    )));

    for field in FIXED_BOUNDARY_FIELDS {
        let blocking = reconciliation_conflict_row("adaptive_task_governance_stale", &[field]);
        assert!(
            !review_neutral(&blocking),
            "a conflict touching '{field}' must not be review-neutral"
        );
    }
}

/// Insert one Task-scoped canonical conflict + required reconciliation
/// record directly (mirroring what `record_adaptive_boundary_reconciliation`
/// persists), so `ensure_capability_permits_execution` reads it back
/// through the same `required_reconciliation_conflicts` query the rest of
/// D16/D17 relies on.
async fn insert_task_scoped_conflict(
    db: &db::SqliteDb,
    project_id: &str,
    task_id: &str,
    conflict_code: &str,
    affected_paths: &[&str],
) {
    let now = now_rfc3339();
    let conflict_id = format!("{task_id}-conflict");
    sqlx::query(
        "INSERT INTO project_canonical_conflict (
                 id, project_id, domain, governing_record_type, governing_record_id,
                 governing_record_revision, governing_record_digest,
                 conflicting_record_type, conflicting_record_id, conflicting_record_revision,
                 conflicting_record_digest, affected_paths_json, conflict_code, description,
                 detected_by_type, detected_by_id, authorization_basis, authorization_action,
                 explicit_event, authorization_occurred_at, idempotency_key, created_at
             ) VALUES (?, ?, 'execution', 'execution_baseline', 'baseline-1', '1',
                       'test-baseline-content', 'task', ?, '1', 'task-digest', ?, ?, ?, 'system',
                       'test-fixture', 'adaptive_task_boundary', 'task.adaptive.reject',
                       'task.adaptive.split.rejected', ?, ?, ?)",
    )
    .bind(&conflict_id)
    .bind(project_id)
    .bind(task_id)
    .bind(serde_json::to_string(affected_paths).expect("affected paths encode"))
    .bind(conflict_code)
    .bind(format!("{conflict_code} for {task_id}"))
    .bind(&now)
    .bind(format!("{task_id}-idempotency"))
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("canonical conflict inserts");
    sqlx::query(
        "INSERT INTO project_reconciliation_record (
                 id, project_id, conflict_id, record_type, record_id, record_revision,
                 record_digest, governing_record_type, governing_record_id,
                 governing_record_revision, governing_record_digest, state,
                 current_resolution_id, version, created_at, updated_at
             ) VALUES (?, ?, ?, 'task', ?, '1', 'task-digest', 'execution_baseline', 'baseline-1',
                       '1', 'test-baseline-content', 'required', NULL, 1, ?, ?)",
    )
    .bind(format!("{task_id}-reconciliation"))
    .bind(project_id)
    .bind(&conflict_id)
    .bind(task_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("reconciliation record inserts");
}

async fn capability_review_fixture(
    project_id: &str,
    task_id: &str,
) -> (Arc<db::SqliteDb>, TaskService, db::Task) {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations");
    let db = Arc::new(db::SqliteDb::new(pool));
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(4)));
    let now = now_rfc3339();
    db::ProjectRepo::create(
        &*db,
        db::CreateProject {
            id: project_id.to_owned(),
            name: "Capability review project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("project creates");
    let task = minimal_task(task_id, project_id);
    (db, service, task)
}

#[tokio::test]
async fn read_only_review_continues_past_a_review_neutral_conflict_that_blocks_mutation() {
    let (db, service, task) =
        capability_review_fixture("capability-review-neutral", "task-1").await;
    insert_task_scoped_conflict(
        &db,
        &task.project_id,
        &task.id,
        "adaptive_task_governance_stale",
        &["plan_items"],
    )
    .await;

    // Repository mutation stays blocked by the recorded conflict even
    // though the Project gate itself reports `Active`.
    let mutation_error = service
        .ensure_capability_permits_execution(
            &task,
            ExecutionGateCapability::RepositoryMutation,
            ExecutionGate::Active,
        )
        .await
        .expect_err("repository mutation stays blocked by the recorded conflict");
    assert!(matches!(mutation_error, ServiceError::Conflict(_)));

    // Independent read-only review of the already-committed result
    // continues: the conflict never touches an acceptance/evidence/
    // risk/reviewer/release boundary field (D16/8.2.5, F11).
    service
        .ensure_capability_permits_execution(
            &task,
            ExecutionGateCapability::ReadOnlyReview,
            ExecutionGate::Active,
        )
        .await
        .expect("a review-neutral conflict must never block independent review");
}

#[tokio::test]
async fn read_only_review_is_blocked_when_the_conflict_affects_a_fixed_boundary_field() {
    let (db, service, task) =
        capability_review_fixture("capability-review-blocking", "task-1").await;
    insert_task_scoped_conflict(
        &db,
        &task.project_id,
        &task.id,
        "adaptive_task_governance_stale",
        &["fixed_acceptance"],
    )
    .await;

    let review_error = service
        .ensure_capability_permits_execution(
            &task,
            ExecutionGateCapability::ReadOnlyReview,
            ExecutionGate::Active,
        )
        .await
        .expect_err("a conflict touching acceptance must block independent review too");
    match review_error {
        ServiceError::Conflict(message) => {
            assert!(message.contains("review_blocked"), "message: {message}")
        }
        other => panic!("expected a review_blocked Conflict, got {other:?}"),
    }
}

#[tokio::test]
async fn read_only_review_does_not_require_a_baseline() {
    let (_db, service, task) =
        capability_review_fixture("capability-review-ungoverned", "task-1").await;

    service
        .ensure_capability_permits_execution(
            &task,
            ExecutionGateCapability::ReadOnlyReview,
            ExecutionGate::PreBaselineReadOnly,
        )
        .await
        .expect("Charter-governed work is reviewable without a baseline");
}
