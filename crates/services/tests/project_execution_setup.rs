use std::sync::Arc;

use api_types::{
    canonical_digest_with_schema, AttachPrimaryRepositoryRequest, ExecutionSetupState,
    RetryProvisioningRequest, SelectExecutionPrincipalRequest,
};
use chrono::{Duration, Utc};
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentConnectionHealthRepo,
    AgentRepo, AgentStatus, ApplyProjectExecutionSetupCommand, CommandReceiptRepo,
    CreateAgentIdentity, CreateAgentProfile, CreateCommandReceipt, CreateProject, CreateRepo,
    CreateTask, DbError, Project, ProjectExecutionSetupCommandRepo, ProjectProvisioningRepo,
    ProjectRepo, RepoRepo, ScheduleProjectProvisioningRetry, SqliteDb, TaskRepo,
    UpsertAgentConnectionHealth, WorkMode,
};
use serde_json::json;
use services::{
    Assignee, ExecutionPrincipalRole, ProjectExecutionSetupService, ServiceError, TaskService,
};

const RECEIPT_SCHEMA: &str = "forge.project-execution-setup/v1";
const OWNER_ID: &str = "setup-owner";

async fn database() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    Arc::new(SqliteDb::new(pool))
}

async fn project(db: &SqliteDb, name: &str) -> Project {
    let now = now_rfc3339();
    ProjectRepo::create(
        db,
        CreateProject {
            id: new_uuid_v4(),
            name: name.to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(OWNER_ID.to_owned()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("project creates")
}

async fn native_agent(db: &SqliteDb, name: &str) -> (String, String) {
    let identity_id = new_uuid_v4();
    let profile_id = new_uuid_v4();
    let now = now_rfc3339();
    AgentRepo::create_identity_with_profile(
        db,
        CreateAgentIdentity {
            id: identity_id.clone(),
            name: name.to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: profile_id.clone(),
            identity_id: identity_id.clone(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("same-provider".to_owned()),
            model: Some("same-model".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("agent creates");
    AgentConnectionHealthRepo::upsert_connection_health(
        db,
        UpsertAgentConnectionHealth {
            profile_id: profile_id.clone(),
            status: "healthy".to_owned(),
            capability_status_json: "{}".to_owned(),
            checked_at: Some(now.clone()),
            error_code: None,
            updated_at: now,
        },
    )
    .await
    .expect("agent health creates");
    (identity_id, profile_id)
}

async fn operation(db: &SqliteDb, project_id: &str) -> db::ProjectProvisioningOperation {
    ProjectProvisioningRepo::get_provisioning_operation(db, project_id)
        .await
        .expect("provisioning operation loads")
        .expect("Project creation creates provisioning operation")
}

async fn repo(db: &SqliteDb, project_id: &str) -> String {
    repo_with_local_path(db, project_id, None).await
}

async fn repo_with_local_path(db: &SqliteDb, project_id: &str, local_path: Option<&str>) -> String {
    let now = now_rfc3339();
    RepoRepo::create(
        db,
        CreateRepo {
            id: new_uuid_v4(),
            project_id: project_id.to_owned(),
            name: "setup-repo".to_owned(),
            remote_url: "https://example.invalid/setup-repo".to_owned(),
            local_path: local_path.map(str::to_owned),
            work_mode: WorkMode::DirectMerge,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("repository creates")
    .id
}

#[tokio::test]
async fn setup_actions_replay_exactly_and_reconcile_ready_metadata() {
    let db = database().await;
    let project = project(&db, "setup action replay").await;
    let operation = operation(&db, &project.id).await;
    let (worker_id, _) = native_agent(&db, "Worker").await;
    let (reviewer_id, _) = native_agent(&db, "Reviewer").await;
    let service = ProjectExecutionSetupService::new(Arc::clone(&db));

    let selected_worker = service
        .select_execution_principal(
            &project.id,
            ExecutionPrincipalRole::Worker,
            &SelectExecutionPrincipalRequest {
                identity_id: worker_id.clone(),
                expected_project_version: project.version,
                idempotency_key: "select-worker-1".to_owned(),
            },
            OWNER_ID,
        )
        .await
        .expect("worker selection commits");
    assert_eq!(
        selected_worker
            .worker
            .as_ref()
            .map(|agent| &agent.identity_id),
        Some(&worker_id)
    );
    assert_eq!(
        selected_worker
            .provisioning
            .as_ref()
            .map(|operation| operation.status.as_str()),
        Some("setup_required")
    );

    let replayed_worker = service
        .select_execution_principal(
            &project.id,
            ExecutionPrincipalRole::Worker,
            &SelectExecutionPrincipalRequest {
                identity_id: worker_id.clone(),
                expected_project_version: 1,
                idempotency_key: "select-worker-1".to_owned(),
            },
            OWNER_ID,
        )
        .await
        .expect("worker command replays");
    assert_eq!(replayed_worker.worker, selected_worker.worker);
    assert_eq!(
        replayed_worker.project_version,
        selected_worker.project_version
    );

    let changed_input = service
        .select_execution_principal(
            &project.id,
            ExecutionPrincipalRole::Worker,
            &SelectExecutionPrincipalRequest {
                identity_id: reviewer_id.clone(),
                expected_project_version: 1,
                idempotency_key: "select-worker-1".to_owned(),
            },
            OWNER_ID,
        )
        .await;
    assert!(matches!(
        changed_input,
        Err(ServiceError::Db(DbError::IdempotencyConflict))
    ));

    let rejected_same_identity = service
        .select_execution_principal(
            &project.id,
            ExecutionPrincipalRole::IndependentReviewer,
            &SelectExecutionPrincipalRequest {
                identity_id: worker_id.clone(),
                expected_project_version: selected_worker.project_version,
                idempotency_key: "select-reviewer-same".to_owned(),
            },
            OWNER_ID,
        )
        .await;
    assert!(matches!(
        rejected_same_identity,
        Err(ServiceError::Conflict(_))
    ));

    let selected_reviewer = service
        .select_execution_principal(
            &project.id,
            ExecutionPrincipalRole::IndependentReviewer,
            &SelectExecutionPrincipalRequest {
                identity_id: reviewer_id.clone(),
                expected_project_version: selected_worker.project_version,
                idempotency_key: "select-reviewer-1".to_owned(),
            },
            OWNER_ID,
        )
        .await
        .expect("independent reviewer selection commits");
    assert_eq!(
        selected_reviewer
            .independent_reviewer
            .as_ref()
            .map(|agent| &agent.identity_id),
        Some(&reviewer_id)
    );
    assert_eq!(
        selected_reviewer
            .worker
            .as_ref()
            .and_then(|agent| agent.provider.as_deref()),
        Some("same-provider")
    );
    assert_eq!(
        selected_reviewer
            .independent_reviewer
            .as_ref()
            .and_then(|agent| agent.provider.as_deref()),
        Some("same-provider")
    );

    let attached = service
        .attach_primary_repository(
            &project.id,
            &AttachPrimaryRepositoryRequest {
                repo_id: repo(&db, &project.id).await,
                expected_project_version: selected_reviewer.project_version,
                idempotency_key: "attach-repo-1".to_owned(),
            },
            OWNER_ID,
        )
        .await
        .expect("repository attachment commits");
    assert_eq!(attached.execution_setup_state, ExecutionSetupState::Ready);
    assert_eq!(
        attached
            .provisioning
            .as_ref()
            .map(|operation| operation.status.as_str()),
        Some("ready")
    );
    let stored_operation = ProjectProvisioningRepo::get_provisioning_operation(&*db, &project.id)
        .await
        .expect("operation reloads")
        .expect("operation remains present");
    assert_eq!(stored_operation.status, "ready");
    assert_eq!(stored_operation.current_checkpoint, "completed");
    assert!(stored_operation.completed_at.is_some());
    assert_eq!(stored_operation.version, operation.version + 1);
}

#[tokio::test]
async fn expired_retry_receipt_replays_the_same_lease_and_returns_fresh_state() {
    let db = database().await;
    let project = project(&db, "setup retry recovery").await;
    let operation = operation(&db, &project.id).await;
    native_agent(&db, "Recovery worker").await;
    native_agent(&db, "Recovery reviewer").await;

    let idempotency_key = "retry-after-crash";
    let input = json!({
        "expected_operation_version": operation.version,
    });
    let digest = canonical_digest_with_schema(RECEIPT_SCHEMA, &input).expect("digest");
    let now = now_rfc3339();
    let expired = (Utc::now() - Duration::seconds(60)).to_rfc3339();
    let lease_owner = "execution-setup-retry:crash-owner";
    let mut receipt = CreateCommandReceipt {
        id: new_uuid_v4(),
        principal_type: "user".to_owned(),
        principal_id: OWNER_ID.to_owned(),
        scope_type: "project".to_owned(),
        scope_id: project.id.clone(),
        operation: "project.execution_setup.retry_provisioning".to_owned(),
        idempotency_key: idempotency_key.to_owned(),
        input_digest: digest,
        policy_result: "allowed".to_owned(),
        correlation_id: new_uuid_v4(),
        causation_id: None,
        causation_depth: 0,
        event_id: new_uuid_v4(),
        agent_action_execution_id: None,
        outcome_json: json!({
            "accepted": true,
            "operation": "project.execution_setup.retry_provisioning",
            "project_id": project.id,
        })
        .to_string(),
        committed_at: now.clone(),
    };
    let applied = ProjectExecutionSetupCommandRepo::apply_project_execution_setup_command(
        &*db,
        ApplyProjectExecutionSetupCommand {
            project_id: project.id.clone(),
            expected_project_version: None,
            settings: None,
            primary_repo_id: None,
            bump_project_version: false,
            provisioning_retry: Some(ScheduleProjectProvisioningRetry {
                operation_id: operation.id.clone(),
                expected_version: operation.version,
                lease_owner: lease_owner.to_owned(),
                lease_expires_at: expired,
                updated_at: now,
            }),
            provisioning_metadata: None,
            receipt: receipt.clone(),
        },
    )
    .await
    .expect("crash seam commits receipt and lease");
    assert!(!applied.replayed);
    let leased = ProjectProvisioningRepo::get_provisioning_operation(&*db, &project.id)
        .await
        .expect("operation reloads")
        .expect("operation remains present");
    assert_eq!(leased.attempt_count, operation.attempt_count + 1);
    assert_eq!(leased.lease_owner.as_deref(), Some(lease_owner));

    let response = ProjectExecutionSetupService::new(Arc::clone(&db))
        .retry_provisioning(
            &project.id,
            &RetryProvisioningRequest {
                expected_operation_version: operation.version,
                idempotency_key: idempotency_key.to_owned(),
            },
            OWNER_ID,
        )
        .await
        .expect("same receipt recovers expired lease");
    assert_ne!(
        response
            .provisioning
            .as_ref()
            .map(|operation| operation.status.as_str()),
        Some("provisioning")
    );
    assert_eq!(response.execution_setup_state, ExecutionSetupState::Ready);
    let recovered = ProjectProvisioningRepo::get_provisioning_operation(&*db, &project.id)
        .await
        .expect("recovered operation reloads")
        .expect("recovered operation remains present");
    assert_eq!(recovered.status, "ready");
    assert_eq!(recovered.attempt_count, leased.attempt_count);
    assert_eq!(recovered.lease_owner, None);

    // The same accepted command has one durable receipt.  Replaying it again
    // returns current projection without creating another operation attempt.
    let second = ProjectExecutionSetupService::new(Arc::clone(&db))
        .retry_provisioning(
            &project.id,
            &RetryProvisioningRequest {
                expected_operation_version: operation.version,
                idempotency_key: idempotency_key.to_owned(),
            },
            OWNER_ID,
        )
        .await
        .expect("completed receipt replays");
    assert_eq!(second.execution_setup_state, ExecutionSetupState::Ready);
    let final_operation = ProjectProvisioningRepo::get_provisioning_operation(&*db, &project.id)
        .await
        .expect("final operation reloads")
        .expect("final operation remains present");
    assert_eq!(final_operation.attempt_count, leased.attempt_count);
    let receipts = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM command_receipt WHERE scope_id = ? AND operation = ?",
    )
    .bind(&project.id)
    .bind("project.execution_setup.retry_provisioning")
    .fetch_one(db.pool())
    .await
    .expect("receipt count reads");
    assert_eq!(receipts, 1);

    // Keep the compiler honest if receipt fields evolve: the recovery test
    // intentionally verifies the exact persisted command identity.
    receipt.scope_id = project.id;
    assert!(CommandReceiptRepo::get_command_receipt(
        &*db,
        "user",
        OWNER_ID,
        "project",
        &receipt.scope_id,
        "project.execution_setup.retry_provisioning",
        idempotency_key,
        &receipt.input_digest,
    )
    .await
    .expect("receipt lookup succeeds")
    .is_some());
}

#[tokio::test]
async fn coordinator_identity_is_not_eligible_for_setup_actions() {
    let db = database().await;
    let (coordinator_id, coordinator_profile_id) = native_agent(&db, "Coordinator").await;
    let project = {
        let now = now_rfc3339();
        ProjectRepo::create_with_agent_binding(
            &*db,
            CreateProject {
                id: new_uuid_v4(),
                name: "coordinator exclusion".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: Some(OWNER_ID.to_owned()),
                created_at: now.clone(),
                updated_at: now,
            },
            Some(coordinator_id.clone()),
            Some(coordinator_profile_id),
        )
        .await
        .expect("project with coordinator creates")
    };
    native_agent(&db, "Other candidate").await;

    let result = ProjectExecutionSetupService::new(Arc::clone(&db))
        .select_execution_principal(
            &project.id,
            ExecutionPrincipalRole::Worker,
            &SelectExecutionPrincipalRequest {
                identity_id: coordinator_id,
                expected_project_version: project.version,
                idempotency_key: "coordinator-as-worker".to_owned(),
            },
            OWNER_ID,
        )
        .await;
    assert!(matches!(result, Err(ServiceError::Conflict(_))));
}

#[tokio::test]
async fn nonexistent_local_repository_does_not_claim_ready_setup() {
    let db = database().await;
    let project = project(&db, "unverified local repository").await;
    operation(&db, &project.id).await;
    let (worker_id, _) = native_agent(&db, "Local Worker").await;
    let (reviewer_id, _) = native_agent(&db, "Local Reviewer").await;
    let service = ProjectExecutionSetupService::new(Arc::clone(&db));

    let worker = service
        .select_execution_principal(
            &project.id,
            ExecutionPrincipalRole::Worker,
            &SelectExecutionPrincipalRequest {
                identity_id: worker_id,
                expected_project_version: project.version,
                idempotency_key: "local-worker".to_owned(),
            },
            OWNER_ID,
        )
        .await
        .expect("local worker selection commits");
    let reviewer = service
        .select_execution_principal(
            &project.id,
            ExecutionPrincipalRole::IndependentReviewer,
            &SelectExecutionPrincipalRequest {
                identity_id: reviewer_id,
                expected_project_version: worker.project_version,
                idempotency_key: "local-reviewer".to_owned(),
            },
            OWNER_ID,
        )
        .await
        .expect("local reviewer selection commits");
    let attached = service
        .attach_primary_repository(
            &project.id,
            &AttachPrimaryRepositoryRequest {
                repo_id: repo_with_local_path(
                    &db,
                    &project.id,
                    Some("/tmp/forge-setup-does-not-exist"),
                )
                .await,
                expected_project_version: reviewer.project_version,
                idempotency_key: "local-repo".to_owned(),
            },
            OWNER_ID,
        )
        .await
        .expect("local repository attachment commits");
    assert_eq!(
        attached.execution_setup_state,
        ExecutionSetupState::SetupRequired
    );
    assert_eq!(
        attached
            .provisioning
            .as_ref()
            .map(|operation| operation.status.as_str()),
        Some("setup_required")
    );
}

/// `PLAN-03` — an implementation Task cannot become runnable before its
/// Project has an active, user-approved execution baseline.
///
/// The Project below is deliberately taken all the way to
/// `ExecutionSetupState::Ready`: Worker, independent Reviewer, and primary
/// repository are all committed, and the Project is Charter-backed. The only
/// missing authority is the user's baseline approval. The Task is therefore
/// blocked for exactly one reason, and this test asserts the consequence the
/// governance gate exists to guarantee: no `WorkspaceLease` — and no
/// workspace or execution that would imply one — is ever created for it.
#[tokio::test]
async fn pre_baseline_implementation_task_creates_no_workspace_lease() {
    let db = database().await;
    let project = project(&db, "plan03 pre-baseline").await;
    let _operation = operation(&db, &project.id).await;
    let (worker_id, _) = native_agent(&db, "PLAN-03 Worker").await;
    let (reviewer_id, _) = native_agent(&db, "PLAN-03 Reviewer").await;
    let setup = ProjectExecutionSetupService::new(Arc::clone(&db));

    let selected_worker = setup
        .select_execution_principal(
            &project.id,
            ExecutionPrincipalRole::Worker,
            &SelectExecutionPrincipalRequest {
                identity_id: worker_id.clone(),
                expected_project_version: project.version,
                idempotency_key: "plan03-select-worker".to_owned(),
            },
            OWNER_ID,
        )
        .await
        .expect("worker selection commits");
    let selected_reviewer = setup
        .select_execution_principal(
            &project.id,
            ExecutionPrincipalRole::IndependentReviewer,
            &SelectExecutionPrincipalRequest {
                identity_id: reviewer_id.clone(),
                expected_project_version: selected_worker.project_version,
                idempotency_key: "plan03-select-reviewer".to_owned(),
            },
            OWNER_ID,
        )
        .await
        .expect("independent reviewer selection commits");
    let repo_id = repo(&db, &project.id).await;
    let attached = setup
        .attach_primary_repository(
            &project.id,
            &AttachPrimaryRepositoryRequest {
                repo_id: repo_id.clone(),
                expected_project_version: selected_reviewer.project_version,
                idempotency_key: "plan03-attach-repo".to_owned(),
            },
            OWNER_ID,
        )
        .await
        .expect("repository attachment commits");
    assert_eq!(
        attached.execution_setup_state,
        ExecutionSetupState::Ready,
        "execution setup must be complete so the baseline is the only remaining gate"
    );

    attach_approved_charter(&db, &project.id).await;
    let task_id = new_uuid_v4();
    let now = now_rfc3339();
    TaskRepo::create(
        &*db,
        CreateTask {
            id: task_id.clone(),
            project_id: project.id.clone(),
            repo_id: Some(repo_id.clone()),
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "implement the pre-baseline core loop".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "backlog".to_owned(),
            is_automation: false,
            priority: 0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("implementation Task creates");

    let tasks = TaskService::new(Arc::clone(&db), Arc::new(events::EventBus::new(16)));
    let error = tasks
        .claim_task(task_id.clone(), Assignee::Agent(worker_id.clone()), None)
        .await
        .expect_err("an implementation Task must not be claimable before baseline approval");
    let ServiceError::InvalidOperation { message } = &error else {
        panic!("expected a deterministic governance refusal, got {error:?}");
    };
    assert!(
        message.contains("active user-approved execution baseline is required"),
        "the refusal must name the missing baseline approval, got {message}"
    );

    for (table, label) in [
        ("workspace_lease", "WorkspaceLease"),
        ("workspace", "workspace"),
        ("execution", "execution"),
    ] {
        let count: i64 =
            sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table} WHERE task_id = ?"))
                .bind(&task_id)
                .fetch_one(db.pool())
                .await
                .unwrap_or_else(|error| panic!("{label} count: {error}"));
        assert_eq!(
            count, 0,
            "a refused pre-baseline claim must leave no {label} row"
        );
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM workspace_lease")
            .fetch_one(db.pool())
            .await
            .expect("global WorkspaceLease count"),
        0,
        "no WorkspaceLease exists anywhere in the Project before baseline approval"
    );
}

/// Attach an approved Charter revision so the Project is Charter-backed and
/// the governance gate applies. This mirrors the server-side approval shape
/// without granting an execution baseline.
async fn attach_approved_charter(db: &SqliteDb, project_id: &str) {
    let now = now_rfc3339();
    sqlx::query(
        "INSERT OR IGNORE INTO user (id, email, password_hash, display_name, created_at, updated_at)
         VALUES (?, ?, 'test', NULL, ?, ?)",
    )
    .bind(OWNER_ID)
    .bind(format!("{OWNER_ID}@example.test"))
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("owning account exists");
    let charter_id = format!("{project_id}-charter");
    let revision_id = format!("{charter_id}-revision-1");
    sqlx::query(
        "INSERT INTO project_charter (
             id, account_id, project_id, project_mode, maturity, lifecycle,
             version, created_at, updated_at
         ) VALUES (?, ?, ?, 'compact', 'prototype', 'attached', 1, ?, ?)",
    )
    .bind(&charter_id)
    .bind(OWNER_ID)
    .bind(project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("charter creates");
    sqlx::query(
        "INSERT INTO project_charter_revision (
             id, charter_id, revision, base_revision, lifecycle, schema_version,
             render_version, content_json, rendered_view, change_summary,
             author_type, author_id, source_refs_json, content_digest,
             rendered_digest, created_at
         ) VALUES (?, ?, 1, 0, 'approved', 'forge.project-charter/v1',
                   'forge.project-charter-render/v1', '{}', '# Project',
                   'test fixture approval', 'user', ?, '[]',
                   'plan03-charter-content-digest', 'plan03-charter-render-digest', ?)",
    )
    .bind(&revision_id)
    .bind(&charter_id)
    .bind(OWNER_ID)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("charter revision creates");
    sqlx::query(
        "UPDATE project_charter
         SET current_approved_revision_id = ?, current_draft_revision_id = ?, version = 2
         WHERE id = ?",
    )
    .bind(&revision_id)
    .bind(&revision_id)
    .bind(&charter_id)
    .execute(db.pool())
    .await
    .expect("charter approval attaches");
    sqlx::query(
        "UPDATE project
         SET current_charter_id = ?, current_charter_revision_id = ?,
             current_charter_version = 1, charter_status = 'charter_backed',
             charter_setup_required = 0, version = version + 1, updated_at = ?
         WHERE id = ?",
    )
    .bind(&charter_id)
    .bind(&revision_id)
    .bind(&now)
    .bind(project_id)
    .execute(db.pool())
    .await
    .expect("approved Charter attaches to Project");
}

/// A Worker at its concurrency limit is still the Project's Worker.
///
/// Dispatch re-checks role eligibility *after* the execution it is dispatching
/// is already running, so that execution consumes the identity's own capacity.
/// When `Busy` disqualified an identity, a `max_concurrent_tasks = 1` Worker
/// could never run anything: every dispatch failed with "repository execution
/// identity is not active and Project-eligible".
#[tokio::test]
async fn a_worker_at_capacity_is_still_eligible_for_its_own_role() {
    let db = database().await;
    let project = project(&db, "capacity eligibility").await;
    let (worker_id, _) = native_agent(&db, "Busy Worker").await;

    assert!(
        services::is_eligible_execution_identity(&db, &project.id, &worker_id)
            .await
            .expect("eligibility resolves"),
        "an idle healthy identity is eligible"
    );

    // Saturate the identity: one running execution against max_concurrent_tasks
    // of 1 is exactly the state dispatch observes for the execution it is about
    // to run.
    let task_id = new_uuid_v4();
    let now = now_rfc3339();
    TaskRepo::create(
        &*db,
        CreateTask {
            id: task_id.clone(),
            project_id: project.id.clone(),
            repo_id: None,
            parent_task_id: None,
            assignee_type: Some("agent".to_owned()),
            assignee_id: Some(worker_id.clone()),
            title: "saturating work".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "in_progress".to_owned(),
            is_automation: false,
            priority: 0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task creates");
    sqlx::query(
        "INSERT INTO execution (
             id, task_id, agent_id, role, status, created_at, updated_at
         ) VALUES (?, ?, ?, 'coder', 'running', ?, ?)",
    )
    .bind(new_uuid_v4())
    .bind(&task_id)
    .bind(&worker_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("running execution inserts");

    let agent = AgentRepo::get_by_id(&*db, &worker_id)
        .await
        .expect("agent lookup")
        .expect("agent exists");
    assert_eq!(agent.max_concurrent_tasks, 1);

    assert!(
        services::is_eligible_execution_identity(&db, &project.id, &worker_id)
            .await
            .expect("eligibility resolves"),
        "capacity is a scheduling fact enforced at claim time, not a reason to \
         strip an identity of its Project role"
    );
    services::ensure_execution_role_principal(&db, &project.id, "coder", &worker_id)
        .await
        .expect("a saturated Worker can still dispatch its own role");
}
