//! Characterization fixtures for Genesis Project creation/provisioning.
//!
//! These tests exercise the durable filesystem/SQLite seam. Each case keeps
//! the Project create successful while asserting the truthful setup operation
//! and checkpoint projection left behind by provisioning or interruption.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use chrono::{Duration, Utc};
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentConnectionHealthRepo,
    AgentRepo, AgentStatus, CreateAgentIdentity, CreateAgentProfile, CreateProject, CreateRepo,
    Project, ProjectRepo, RepoRepo, SqliteDb, UpsertAgentConnectionHealth, WorkMode,
};
use serde_json::Value;
use sqlx::Row;

const DEFAULT_BRANCH: &str = "main";

struct ProjectFixture {
    db: Arc<SqliteDb>,
    project: Project,
    repo_path: PathBuf,
    coordinator_identity_id: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
struct ProvisioningOperationSnapshot {
    id: String,
    status: String,
    current_checkpoint: String,
    attempt_count: i64,
    max_attempts: i64,
    retryable: bool,
    last_error_code: Option<String>,
}

async fn provisioning_operation(db: &SqliteDb, project_id: &str) -> ProvisioningOperationSnapshot {
    let row = sqlx::query(
        "SELECT id, status, current_checkpoint, attempt_count, max_attempts,
                retryable, last_error_code
         FROM project_provisioning_operation
         WHERE project_id = ?",
    )
    .bind(project_id)
    .fetch_one(db.pool())
    .await
    .expect("Project provisioning operation queries");
    ProvisioningOperationSnapshot {
        id: row.try_get("id").expect("operation id reads"),
        status: row.try_get("status").expect("operation status reads"),
        current_checkpoint: row
            .try_get("current_checkpoint")
            .expect("operation checkpoint reads"),
        attempt_count: row
            .try_get("attempt_count")
            .expect("operation attempts read"),
        max_attempts: row
            .try_get("max_attempts")
            .expect("operation max attempts read"),
        retryable: row
            .try_get::<i64, _>("retryable")
            .expect("operation retryable flag reads")
            != 0,
        last_error_code: row
            .try_get("last_error_code")
            .expect("operation error code reads"),
    }
}

async fn checkpoint_statuses(db: &SqliteDb, operation_id: &str) -> Vec<(String, String)> {
    sqlx::query(
        "SELECT checkpoint, status
         FROM project_provisioning_checkpoint
         WHERE operation_id = ?
         ORDER BY CASE checkpoint
             WHEN 'preflight' THEN 1
             WHEN 'repository_initialized' THEN 2
             WHEN 'repository_registered' THEN 3
             WHEN 'repository_linked' THEN 4
             WHEN 'roles_assigned' THEN 5
         END",
    )
    .bind(operation_id)
    .fetch_all(db.pool())
    .await
    .expect("Project provisioning checkpoints query")
    .into_iter()
    .map(|row| {
        (
            row.try_get("checkpoint").expect("checkpoint name reads"),
            row.try_get("status").expect("checkpoint status reads"),
        )
    })
    .collect()
}

async fn provisioning_error_codes(db: &SqliteDb, operation_id: &str) -> Vec<String> {
    sqlx::query(
        "SELECT code FROM project_provisioning_error
         WHERE operation_id = ? ORDER BY created_at, id",
    )
    .bind(operation_id)
    .fetch_all(db.pool())
    .await
    .expect("Project provisioning errors query")
    .into_iter()
    .map(|row| row.try_get("code").expect("provisioning error code reads"))
    .collect()
}

async fn database() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("SQLite pool creates");
    run_migrations(&pool).await.expect("migrations run");
    Arc::new(SqliteDb::new(pool))
}

fn workspace_root() -> PathBuf {
    std::env::var_os("FORGE_WORKSPACE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("forge").join("worktrees"))
}

fn project_slug(name: &str) -> String {
    let normalized = name
        .to_lowercase()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    let slug = normalized
        .split('-')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    if slug.is_empty() {
        "project".to_owned()
    } else {
        slug
    }
}

fn repo_path(project: &Project) -> PathBuf {
    let id_prefix = project.id.chars().take(8).collect::<String>();
    workspace_root()
        .join("repos")
        .join(format!("{}-{id_prefix}", project_slug(&project.name)))
}

async fn remove_path(path: &Path) {
    let Ok(metadata) = tokio::fs::symlink_metadata(path).await else {
        return;
    };
    if metadata.is_dir() {
        tokio::fs::remove_dir_all(path)
            .await
            .expect("test repository directory removes");
    } else {
        tokio::fs::remove_file(path)
            .await
            .expect("test repository interruption marker removes");
    }
}

async fn initialize_repository(path: &Path) -> String {
    tokio::fs::create_dir_all(path)
        .await
        .expect("repository directory creates");
    git::init(path).await.expect("repository initializes");
    tokio::fs::write(path.join("README.md"), "# Characterization\n")
        .await
        .expect("repository README writes");
    let sha = git::commit_all(path, "initial characterization commit")
        .await
        .expect("repository initial commit creates");
    if !git::branch_exists(path, DEFAULT_BRANCH)
        .await
        .expect("repository branch lookup succeeds")
    {
        git::rename_current_branch(path, DEFAULT_BRANCH)
            .await
            .expect("repository branch normalizes");
    }
    sha
}

async fn create_native_agent(db: &SqliteDb, name: &str) -> (String, String) {
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
            provider: Some("characterization".to_owned()),
            model: Some("characterization-model".to_owned()),
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
    .expect("native identity/profile creates");
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
    .expect("native identity health records");
    (identity_id, profile_id)
}

async fn create_project(with_coordinator: bool) -> ProjectFixture {
    let db = database().await;
    let project_id = new_uuid_v4();
    let id_prefix = project_id.chars().take(8).collect::<String>();
    let project_name = format!("Genesis Characterization {id_prefix}");
    let now = now_rfc3339();
    let coordinator = if with_coordinator {
        Some(create_native_agent(&db, "Project coordinator").await)
    } else {
        None
    };
    let input = CreateProject {
        id: project_id,
        name: project_name,
        settings: "{}".to_owned(),
        workflow_definition: "{}".to_owned(),
        primary_repo_id: None,
        owner_id: None,
        created_at: now.clone(),
        updated_at: now,
    };
    let project = match &coordinator {
        Some((identity_id, profile_id)) => ProjectRepo::create_with_agent_binding(
            &*db,
            input,
            Some(identity_id.clone()),
            Some(profile_id.clone()),
        )
        .await
        .expect("Project with coordinator creates"),
        None => ProjectRepo::create(&*db, input)
            .await
            .expect("Project creates"),
    };
    let repo_path = repo_path(&project);
    ProjectFixture {
        db,
        project,
        repo_path,
        coordinator_identity_id: coordinator.map(|(identity_id, _)| identity_id),
    }
}

async fn repo_count(db: &SqliteDb, project_id: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM repo WHERE project_id = ?")
        .bind(project_id)
        .fetch_one(db.pool())
        .await
        .expect("Project repository count queries")
}

async fn repo_ids(db: &SqliteDb, project_id: &str) -> Vec<String> {
    sqlx::query("SELECT id FROM repo WHERE project_id = ? ORDER BY created_at, id")
        .bind(project_id)
        .fetch_all(db.pool())
        .await
        .expect("Project repository rows query")
        .into_iter()
        .map(|row| row.try_get::<String, _>("id").expect("repository id reads"))
        .collect()
}

fn role_assignments(settings: &str) -> Vec<Value> {
    serde_json::from_str::<Value>(settings)
        .expect("Project settings are JSON")
        .get("default_role_assignments")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn assigned_id(assignments: &[Value], role: &str) -> String {
    assignments
        .iter()
        .find(|assignment| assignment.get("role_name").and_then(Value::as_str) == Some(role))
        .and_then(|assignment| assignment.get("assignee_id"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| panic!("{role} assignment is present"))
}

#[tokio::test]
async fn repository_success_creates_one_git_repo_link_and_distinct_roles() {
    let fixture = create_project(false).await;
    let (worker_a, _) = create_native_agent(&fixture.db, "Worker A").await;
    let (worker_b, _) = create_native_agent(&fixture.db, "Worker B").await;

    services::project_provisioning::provision_genesis_project(&fixture.db, &fixture.project.id)
        .await
        .expect("successful Genesis provisioning");

    let operation = provisioning_operation(&fixture.db, &fixture.project.id).await;
    assert_eq!(operation.status, "ready");
    assert_eq!(operation.current_checkpoint, "completed");
    assert_eq!(operation.attempt_count, 1);
    assert_eq!(operation.max_attempts, 3);
    assert!(!operation.retryable);
    assert_eq!(operation.last_error_code, None);
    assert_eq!(
        checkpoint_statuses(&fixture.db, &operation.id).await,
        vec![
            ("preflight".to_owned(), "completed".to_owned()),
            ("repository_initialized".to_owned(), "completed".to_owned()),
            ("repository_registered".to_owned(), "completed".to_owned()),
            ("repository_linked".to_owned(), "completed".to_owned()),
            ("roles_assigned".to_owned(), "completed".to_owned()),
        ]
    );
    assert!(provisioning_error_codes(&fixture.db, &operation.id)
        .await
        .is_empty());

    let project = ProjectRepo::get_by_id(&*fixture.db, &fixture.project.id)
        .await
        .expect("Project reloads")
        .expect("Project remains present");
    let repo_id = project
        .primary_repo_id
        .clone()
        .expect("successful provisioning links a primary repository");
    assert_eq!(repo_count(&fixture.db, &project.id).await, 1);
    let repo = RepoRepo::get_by_id(&*fixture.db, &repo_id)
        .await
        .expect("repository reloads")
        .expect("linked repository remains present");
    assert_eq!(repo.project_id, project.id);
    assert_eq!(
        repo.local_path.as_deref(),
        Some(fixture.repo_path.to_string_lossy().as_ref())
    );
    assert!(git::is_git_repo(&fixture.repo_path).await);
    assert!(git::branch_exists(&fixture.repo_path, DEFAULT_BRANCH)
        .await
        .expect("default branch lookup succeeds"));

    let assignments = role_assignments(&project.settings);
    assert_eq!(assignments.len(), 2);
    let coder = assigned_id(&assignments, "coder");
    let reviewer = assigned_id(&assignments, "reviewer");
    assert_ne!(coder, reviewer, "a valid two-worker setup is independent");
    assert!([worker_a.as_str(), worker_b.as_str()].contains(&coder.as_str()));
    assert!([worker_a.as_str(), worker_b.as_str()].contains(&reviewer.as_str()));

    remove_path(&fixture.repo_path).await;
}

#[tokio::test]
async fn filesystem_interruption_after_git_init_is_reconciled_without_a_second_repo() {
    let fixture = create_project(false).await;
    let interrupted_head = initialize_repository(&fixture.repo_path).await;
    create_native_agent(&fixture.db, "Interrupted Worker A").await;
    create_native_agent(&fixture.db, "Interrupted Worker B").await;

    // This directory/commit is the durable filesystem state left by a
    // process that stopped before the repository row and Project pointer.
    assert_eq!(repo_count(&fixture.db, &fixture.project.id).await, 0);
    let before = provisioning_operation(&fixture.db, &fixture.project.id).await;
    assert_eq!(before.status, "setup_required");
    assert_eq!(before.current_checkpoint, "preflight");
    services::project_provisioning::provision_genesis_project(&fixture.db, &fixture.project.id)
        .await
        .expect("provisioning resumes from initialized filesystem");

    let project = ProjectRepo::get_by_id(&*fixture.db, &fixture.project.id)
        .await
        .expect("Project reloads")
        .expect("Project remains present");
    assert!(project.primary_repo_id.is_some());
    assert_eq!(repo_count(&fixture.db, &project.id).await, 1);
    assert_eq!(
        git::get_current_sha(&fixture.repo_path).await.unwrap(),
        interrupted_head
    );
    let operation = provisioning_operation(&fixture.db, &fixture.project.id).await;
    assert_eq!(operation.status, "ready");
    assert_eq!(operation.current_checkpoint, "completed");
    assert_eq!(operation.attempt_count, 1);
    assert_eq!(repo_count(&fixture.db, &fixture.project.id).await, 1);
    remove_path(&fixture.repo_path).await;
}

#[tokio::test]
async fn repository_row_without_project_link_is_reused_without_a_duplicate_row() {
    let fixture = create_project(false).await;
    initialize_repository(&fixture.repo_path).await;
    create_native_agent(&fixture.db, "Orphan Worker A").await;
    create_native_agent(&fixture.db, "Orphan Worker B").await;
    let now = now_rfc3339();
    let orphan_repo = RepoRepo::create(
        &*fixture.db,
        CreateRepo {
            id: new_uuid_v4(),
            project_id: fixture.project.id.clone(),
            name: project_slug(&fixture.project.name),
            remote_url: fixture.repo_path.to_string_lossy().into_owned(),
            local_path: Some(fixture.repo_path.to_string_lossy().into_owned()),
            work_mode: WorkMode::DirectMerge,
            default_branch: DEFAULT_BRANCH.to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("interrupted repository row creates");

    services::project_provisioning::provision_genesis_project(&fixture.db, &fixture.project.id)
        .await
        .expect("current best-effort provisioning returns success");

    let project = ProjectRepo::get_by_id(&*fixture.db, &fixture.project.id)
        .await
        .expect("Project reloads")
        .expect("Project remains present");
    let linked_id = project
        .primary_repo_id
        .expect("reconciled provisioning links a repository");
    let ids = repo_ids(&fixture.db, &project.id).await;
    assert_eq!(ids.len(), 1, "reconciliation must not create a second row");
    assert!(ids.contains(&orphan_repo.id));
    assert_eq!(linked_id, orphan_repo.id);

    let operation = provisioning_operation(&fixture.db, &fixture.project.id).await;
    assert_eq!(operation.status, "ready");
    assert_eq!(operation.current_checkpoint, "completed");
    assert_eq!(operation.last_error_code, None);

    remove_path(&fixture.repo_path).await;
}

#[tokio::test]
async fn remote_repository_ready_backfill_is_verified_without_local_reprovisioning() {
    let fixture = create_project(false).await;
    let (worker_id, _) = create_native_agent(&fixture.db, "Remote Worker").await;
    let (reviewer_id, _) = create_native_agent(&fixture.db, "Remote Reviewer").await;
    let now = now_rfc3339();
    let repo = RepoRepo::create(
        &*fixture.db,
        CreateRepo {
            id: new_uuid_v4(),
            project_id: fixture.project.id.clone(),
            name: "remote-repository".to_owned(),
            remote_url: "https://example.invalid/remote-repository.git".to_owned(),
            local_path: None,
            work_mode: WorkMode::DirectMerge,
            default_branch: DEFAULT_BRANCH.to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("remote repository row creates");
    let settings = serde_json::json!({
        "default_role_assignments": [
            {"role_name": "coder", "assignee_type": "agent", "assignee_id": worker_id},
            {"role_name": "reviewer", "assignee_type": "agent", "assignee_id": reviewer_id},
        ]
    })
    .to_string();
    sqlx::query(
        "UPDATE project
         SET settings = ?, primary_repo_id = ?, version = version + 1, updated_at = ?
         WHERE id = ?",
    )
    .bind(settings)
    .bind(&repo.id)
    .bind(&now)
    .bind(&fixture.project.id)
    .execute(fixture.db.pool())
    .await
    .expect("remote repository link persists");

    let operation_id: String =
        sqlx::query_scalar("SELECT id FROM project_provisioning_operation WHERE project_id = ?")
            .bind(&fixture.project.id)
            .fetch_one(fixture.db.pool())
            .await
            .expect("remote operation reloads");
    sqlx::query(
        "UPDATE project_provisioning_operation
         SET status = 'ready', current_checkpoint = 'completed',
             retryable = 0, completed_at = ?, updated_at = ?, version = version + 1
         WHERE id = ? AND project_id = ?",
    )
    .bind(&now)
    .bind(&now)
    .bind(&operation_id)
    .bind(&fixture.project.id)
    .execute(fixture.db.pool())
    .await
    .expect("remote ready operation updates");
    for checkpoint in [
        ("preflight", "completed", "{}"),
        (
            "repository_initialized",
            "skipped",
            r#"{"filesystem_verified":false,"source":"V087_backfill"}"#,
        ),
        ("repository_registered", "completed", "{}"),
        ("repository_linked", "completed", "{}"),
        ("roles_assigned", "completed", "{}"),
    ] {
        sqlx::query(
            "UPDATE project_provisioning_checkpoint
             SET status = ?, details_json = ?, completed_at = ?,
                 updated_at = ?, version = version + 1
             WHERE operation_id = ? AND checkpoint = ?",
        )
        .bind(checkpoint.1)
        .bind(checkpoint.2)
        .bind(&now)
        .bind(&now)
        .bind(&operation_id)
        .bind(checkpoint.0)
        .execute(fixture.db.pool())
        .await
        .expect("remote checkpoint updates");
    }

    let operation =
        services::project_provisioning::provision_genesis_project(&fixture.db, &fixture.project.id)
            .await
            .expect("remote ready operation remains ready");
    assert_eq!(operation.status, "ready");
    assert_eq!(operation.attempt_count, 0);
    assert_eq!(operation.current_checkpoint, "completed");
    assert_eq!(repo_count(&fixture.db, &fixture.project.id).await, 1);
}

#[tokio::test]
async fn no_eligible_worker_returns_a_durable_setup_blocker() {
    let fixture = create_project(true).await;
    services::project_provisioning::provision_genesis_project(&fixture.db, &fixture.project.id)
        .await
        .expect("current best-effort provisioning returns success");

    let project = ProjectRepo::get_by_id(&*fixture.db, &fixture.project.id)
        .await
        .expect("Project reloads")
        .expect("Project remains present");
    assert!(project.primary_repo_id.is_some());
    assert_eq!(role_assignments(&project.settings), Vec::<Value>::new());
    let operation = provisioning_operation(&fixture.db, &fixture.project.id).await;
    assert_eq!(operation.status, "setup_required");
    assert_eq!(operation.current_checkpoint, "repository_linked");
    assert!(operation.retryable);
    assert_eq!(
        operation.last_error_code.as_deref(),
        Some("worker_roles_required")
    );
    assert_eq!(
        provisioning_error_codes(&fixture.db, &operation.id).await,
        vec!["worker_roles_required".to_owned()]
    );
    let coordinator_id = fixture
        .coordinator_identity_id
        .expect("coordinator fixture identity");
    let binding_identity: Option<String> = sqlx::query_scalar(
        "SELECT identity_id FROM project_agent_binding
         WHERE project_id = ? AND state = 'active'",
    )
    .bind(&project.id)
    .fetch_one(fixture.db.pool())
    .await
    .expect("active Project binding reads");
    assert_eq!(binding_identity.as_deref(), Some(coordinator_id.as_str()));
    remove_path(&fixture.repo_path).await;
}

#[tokio::test]
async fn one_worker_does_not_fall_back_to_self_review() {
    let fixture = create_project(true).await;
    let (worker_id, _) = create_native_agent(&fixture.db, "Only Worker").await;

    services::project_provisioning::provision_genesis_project(&fixture.db, &fixture.project.id)
        .await
        .expect("current best-effort provisioning returns success");

    let project = ProjectRepo::get_by_id(&*fixture.db, &fixture.project.id)
        .await
        .expect("Project reloads")
        .expect("Project remains present");
    let assignments = role_assignments(&project.settings);
    assert_eq!(
        assignments.len(),
        1,
        "the available Worker remains assigned"
    );
    assert_eq!(assigned_id(&assignments, "coder"), worker_id);
    assert!(assignments.iter().all(|assignment| {
        assignment.get("role_name").and_then(Value::as_str) != Some("reviewer")
    }));
    let operation = provisioning_operation(&fixture.db, &fixture.project.id).await;
    assert_eq!(operation.status, "setup_required");
    assert_eq!(operation.current_checkpoint, "repository_linked");
    assert!(operation.retryable);
    assert_eq!(
        operation.last_error_code.as_deref(),
        Some("independent_reviewer_required")
    );
    assert_eq!(
        provisioning_error_codes(&fixture.db, &operation.id).await,
        vec!["independent_reviewer_required".to_owned()]
    );
    assert_ne!(
        worker_id,
        fixture.coordinator_identity_id.unwrap_or_default()
    );

    remove_path(&fixture.repo_path).await;
}

#[tokio::test]
async fn replay_after_response_loss_reuses_the_project_repository_and_roles() {
    let fixture = create_project(false).await;
    create_native_agent(&fixture.db, "Replay Worker A").await;
    create_native_agent(&fixture.db, "Replay Worker B").await;

    // Model a committed Project-create response lost by the caller.  The
    // create path invokes this operation again when the request is replayed.
    services::project_provisioning::provision_genesis_project(&fixture.db, &fixture.project.id)
        .await
        .expect("first provisioning attempt");
    let first = ProjectRepo::get_by_id(&*fixture.db, &fixture.project.id)
        .await
        .expect("first Project reload")
        .expect("Project remains present");
    let first_repo_id = first
        .primary_repo_id
        .clone()
        .expect("first provisioning links repository");
    let first_settings = first.settings.clone();
    let first_operation = provisioning_operation(&fixture.db, &fixture.project.id).await;

    services::project_provisioning::provision_genesis_project(&fixture.db, &fixture.project.id)
        .await
        .expect("replayed provisioning attempt");
    let replay = ProjectRepo::get_by_id(&*fixture.db, &fixture.project.id)
        .await
        .expect("replayed Project reload")
        .expect("Project remains present after replay");
    assert_eq!(replay.id, first.id);
    assert_eq!(
        replay.primary_repo_id.as_deref(),
        Some(first_repo_id.as_str())
    );
    assert_eq!(replay.settings, first_settings);
    assert_eq!(repo_count(&fixture.db, &replay.id).await, 1);
    let replay_operation = provisioning_operation(&fixture.db, &fixture.project.id).await;
    assert_eq!(replay_operation.id, first_operation.id);
    assert_eq!(replay_operation.status, "ready");
    assert_eq!(replay_operation.current_checkpoint, "completed");
    assert_eq!(
        replay_operation.attempt_count, first_operation.attempt_count,
        "replaying a completed operation must not consume another attempt"
    );

    remove_path(&fixture.repo_path).await;
}

#[tokio::test]
async fn operation_row_repair_restores_missing_checkpoint_rows_before_replay() {
    let fixture = create_project(false).await;
    let operation_id: String =
        sqlx::query_scalar("SELECT id FROM project_provisioning_operation WHERE project_id = ?")
            .bind(&fixture.project.id)
            .fetch_one(fixture.db.pool())
            .await
            .expect("operation row reloads");
    sqlx::query("DELETE FROM project_provisioning_checkpoint WHERE operation_id = ?")
        .bind(&operation_id)
        .execute(fixture.db.pool())
        .await
        .expect("checkpoint rows remove for repair");

    create_native_agent(&fixture.db, "Repair Worker A").await;
    create_native_agent(&fixture.db, "Repair Worker B").await;
    let operation =
        services::project_provisioning::provision_genesis_project(&fixture.db, &fixture.project.id)
            .await
            .expect("replay repairs missing checkpoints");
    assert_eq!(operation.id, operation_id);
    assert_eq!(operation.status, "ready");
    assert_eq!(
        checkpoint_statuses(&fixture.db, &operation.id).await.len(),
        5
    );
    assert!(checkpoint_statuses(&fixture.db, &operation.id)
        .await
        .into_iter()
        .all(|(_, status)| status == "completed"));

    remove_path(&fixture.repo_path).await;
}

#[tokio::test]
async fn active_lease_is_not_stolen_but_expired_lease_is_reclaimed() {
    let fixture = create_project(false).await;
    // First pass creates all durable rows and leaves a typed setup blocker.
    services::project_provisioning::provision_genesis_project(&fixture.db, &fixture.project.id)
        .await
        .expect("initial setup blocker is durable");
    let active_expiry = (Utc::now() + Duration::minutes(5)).to_rfc3339();
    sqlx::query(
        "UPDATE project_provisioning_operation
         SET status = 'provisioning', attempt_count = 0,
             lease_owner = 'other-process', lease_expires_at = ?,
             retryable = 1, version = version + 1, updated_at = ?
         WHERE project_id = ?",
    )
    .bind(&active_expiry)
    .bind(now_rfc3339())
    .bind(&fixture.project.id)
    .execute(fixture.db.pool())
    .await
    .expect("active lease fixture updates");

    let held =
        services::project_provisioning::provision_genesis_project(&fixture.db, &fixture.project.id)
            .await
            .expect("active lease returns current durable operation");
    assert_eq!(held.status, "provisioning");
    assert_eq!(held.attempt_count, 0);
    assert_eq!(held.lease_owner.as_deref(), Some("other-process"));

    sqlx::query(
        "UPDATE project_provisioning_operation
         SET lease_expires_at = '2000-01-01T00:00:00Z',
             version = version + 1, updated_at = ?
         WHERE project_id = ?",
    )
    .bind(now_rfc3339())
    .bind(&fixture.project.id)
    .execute(fixture.db.pool())
    .await
    .expect("expired lease fixture updates");
    let reclaimed =
        services::project_provisioning::provision_genesis_project(&fixture.db, &fixture.project.id)
            .await
            .expect("expired lease is reclaimed");
    assert_eq!(reclaimed.attempt_count, 1);
    assert_ne!(reclaimed.lease_owner.as_deref(), Some("other-process"));

    remove_path(&fixture.repo_path).await;
}

#[tokio::test]
async fn renamed_project_reuses_repository_checkpoint_path() {
    let fixture = create_project(false).await;
    services::project_provisioning::provision_genesis_project(&fixture.db, &fixture.project.id)
        .await
        .expect("initial pass persists repository checkpoint path");
    let old_path = fixture.repo_path.clone();
    let project = ProjectRepo::get_by_id(&*fixture.db, &fixture.project.id)
        .await
        .expect("Project reloads")
        .expect("Project remains present");
    let renamed = ProjectRepo::update_at_version(
        &*fixture.db,
        db::UpdateProject {
            id: project.id.clone(),
            name: Some(format!("Renamed Genesis {}", project.id)),
            settings: None,
            primary_repo_id: None,
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        project.version,
        None,
    )
    .await
    .expect("Project rename commits with CAS");
    create_native_agent(&fixture.db, "Renamed Worker A").await;
    create_native_agent(&fixture.db, "Renamed Worker B").await;
    let operation =
        services::project_provisioning::provision_genesis_project(&fixture.db, &renamed.id)
            .await
            .expect("renamed Project retries durable provisioning");
    assert_eq!(operation.status, "ready");
    let linked = ProjectRepo::get_by_id(&*fixture.db, &renamed.id)
        .await
        .expect("renamed Project reloads")
        .expect("renamed Project remains present");
    let repo = RepoRepo::get_by_id(
        &*fixture.db,
        linked
            .primary_repo_id
            .as_deref()
            .expect("primary repo links"),
    )
    .await
    .expect("renamed repository reloads")
    .expect("renamed repository remains present");
    assert_eq!(
        repo.local_path.as_deref(),
        Some(old_path.to_string_lossy().as_ref())
    );
    assert!(!old_path
        .with_file_name(format!("renamed-genesis-{}", renamed.id))
        .exists());
    remove_path(&old_path).await;
}
