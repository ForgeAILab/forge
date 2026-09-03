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
    AgentRepo, AgentStatus, CreateAgentIdentity, CreateAgentProfile, CreateProject,
    CreateProjectCharter, CreateProjectCharterRevision, CreateProjectCharterRevisionAtomically,
    CreateRepo, Project, ProjectOrchestrationRepo, ProjectRepo, RepoRepo, SqliteDb,
    UpsertAgentConnectionHealth, User, UserRepo, WorkMode,
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
             WHEN 'repository_scaffolded' THEN 2
             WHEN 'repository_initialized' THEN 3
             WHEN 'repository_registered' THEN 4
             WHEN 'repository_linked' THEN 5
             WHEN 'roles_assigned' THEN 6
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

async fn provisioning_error_messages(db: &SqliteDb, operation_id: &str) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT message FROM project_provisioning_error
         WHERE operation_id = ? ORDER BY created_at, id",
    )
    .bind(operation_id)
    .fetch_all(db.pool())
    .await
    .expect("Project provisioning error messages query")
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
async fn repository_success_creates_one_git_repo_link_and_usable_role_defaults() {
    let fixture = create_project(false).await;
    let (worker_a, _) = create_native_agent(&fixture.db, "Worker A").await;
    let (worker_b, _) = create_native_agent(&fixture.db, "Worker B").await;

    services::project_provisioning::provision_genesis_project(&fixture.db, &fixture.project.id)
        .await
        .expect("successful Genesis provisioning");

    let operation = provisioning_operation(&fixture.db, &fixture.project.id).await;
    assert_eq!(
        operation.status,
        "ready",
        "provisioning did not finish: {:?} / {:?}",
        operation.last_error_code,
        provisioning_error_messages(&fixture.db, &operation.id).await
    );
    assert_eq!(operation.current_checkpoint, "completed");
    assert_eq!(operation.attempt_count, 1);
    assert_eq!(operation.max_attempts, 3);
    assert!(!operation.retryable);
    assert_eq!(operation.last_error_code, None);
    assert_eq!(
        checkpoint_statuses(&fixture.db, &operation.id).await,
        vec![
            ("preflight".to_owned(), "completed".to_owned()),
            ("repository_scaffolded".to_owned(), "skipped".to_owned()),
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
async fn project_agent_can_fill_default_execution_roles() {
    let fixture = create_project(true).await;
    services::project_provisioning::provision_genesis_project(&fixture.db, &fixture.project.id)
        .await
        .expect("current best-effort provisioning returns success");

    let project = ProjectRepo::get_by_id(&*fixture.db, &fixture.project.id)
        .await
        .expect("Project reloads")
        .expect("Project remains present");
    assert!(project.primary_repo_id.is_some());
    let assignments = role_assignments(&project.settings);
    assert_eq!(assignments.len(), 2);
    let operation = provisioning_operation(&fixture.db, &fixture.project.id).await;
    assert_eq!(operation.status, "ready");
    assert_eq!(operation.current_checkpoint, "completed");
    assert!(!operation.retryable);
    assert_eq!(operation.last_error_code, None);
    assert!(provisioning_error_codes(&fixture.db, &operation.id)
        .await
        .is_empty());
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
    assert_eq!(assigned_id(&assignments, "coder"), coordinator_id);
    assert_eq!(assigned_id(&assignments, "reviewer"), coordinator_id);
    remove_path(&fixture.repo_path).await;
}

#[tokio::test]
async fn configured_agents_may_fill_multiple_default_roles() {
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
    assert_eq!(assignments.len(), 2);
    let coordinator_id = fixture
        .coordinator_identity_id
        .as_deref()
        .expect("coordinator identity");
    for role in ["coder", "reviewer"] {
        let assigned = assigned_id(&assignments, role);
        assert!([worker_id.as_str(), coordinator_id].contains(&assigned.as_str()));
    }
    let operation = provisioning_operation(&fixture.db, &fixture.project.id).await;
    assert_eq!(operation.status, "ready");
    assert_eq!(operation.current_checkpoint, "completed");
    assert!(!operation.retryable);
    assert_eq!(operation.last_error_code, None);

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
        6
    );
    assert!(checkpoint_statuses(&fixture.db, &operation.id)
        .await
        .into_iter()
        .all(|(checkpoint, status)| {
            status == "completed" || (checkpoint == "repository_scaffolded" && status == "skipped")
        }));

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
             retryable = 1, completed_at = NULL,
             version = version + 1, updated_at = ?
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

// ---------------------------------------------------------------------------
// Charter scaffold: the `repository_scaffolded` checkpoint.
//
// The tests below set `FORGE_SCAFFOLD_COMMAND`, which is process-wide, so
// they serialize on one lock; the other cases in this file never read it
// because their Projects carry no Charter scaffold.
// ---------------------------------------------------------------------------

fn scaffold_env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

const SCAFFOLD_REVISION_VIEW: &str = "# Scaffolded Project\n\n- Working name: Scaffolded Project\n";

/// Attach an approved Charter whose content carries a scaffold block. Only
/// the block matters to provisioning, which reads it leniently.
async fn attach_scaffold_charter(
    fixture: &ProjectFixture,
    template: &str,
    packs: &[&str],
) -> String {
    let db = &fixture.db;
    let now = now_rfc3339();
    let account_id = new_uuid_v4();
    UserRepo::create_user(
        &**db,
        &User {
            id: account_id.clone(),
            email: format!("{account_id}@characterization.test"),
            password_hash: "test".to_owned(),
            display_name: Some("Characterization".to_owned()),
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("account creates");
    // The atomic Charter path only lets the Project's owner (or a privileged
    // member) attach a Charter, so the fixture Project adopts this account.
    sqlx::query("UPDATE project SET owner_id = ? WHERE id = ?")
        .bind(&account_id)
        .bind(&fixture.project.id)
        .execute(db.pool())
        .await
        .expect("Project owner sets");
    let charter_id = new_uuid_v4();
    let revision_id = new_uuid_v4();
    let content = serde_json::json!({
        "identity": {"working_name": "Scaffolded Project", "one_line_vision": "prove scaffolding", "maturity": "mvp"},
        "scaffold": {"template": template, "packs": packs},
    });
    ProjectOrchestrationRepo::create_project_charter_revision_atomically(
        &**db,
        CreateProjectCharterRevisionAtomically {
            project_id: Some(fixture.project.id.clone()),
            genesis_session_id: None,
            account_id: account_id.clone(),
            charter: CreateProjectCharter {
                id: charter_id.clone(),
                account_id: account_id.clone(),
                genesis_session_id: None,
                project_mode: "compact".to_owned(),
                maturity: "mvp".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            revision: CreateProjectCharterRevision {
                id: revision_id.clone(),
                charter_id: charter_id.clone(),
                expected_charter_version: 1,
                project_mode: "compact".to_owned(),
                maturity: "mvp".to_owned(),
                base_revision: 0,
                base_revision_id: None,
                lifecycle: "draft".to_owned(),
                schema_version: "forge.project-charter/v1".to_owned(),
                render_version: "forge.project-charter-render/v1".to_owned(),
                content_json: content.to_string(),
                rendered_view: SCAFFOLD_REVISION_VIEW.to_owned(),
                change_summary: "scaffolded Charter".to_owned(),
                author_type: "user".to_owned(),
                author_id: Some(account_id.clone()),
                source_message_id: None,
                source_turn_job_id: None,
                source_refs_json: "[]".to_owned(),
                content_digest: "scaffold-content-digest".to_owned(),
                rendered_digest: "scaffold-render-digest".to_owned(),
                created_at: now.clone(),
                command_receipt: None,
                action_execution: None,
            },
            command_receipt: None,
            action_execution: None,
        },
    )
    .await
    .expect("Charter revision creates");
    // The pointer trigger requires an approved revision; approval is the
    // user's exact-receipt flow, which this fixture stands in for.
    sqlx::query("UPDATE project_charter_revision SET lifecycle = 'approved' WHERE id = ?")
        .bind(&revision_id)
        .execute(db.pool())
        .await
        .expect("revision approves");
    sqlx::query("UPDATE project_charter SET current_approved_revision_id = ? WHERE id = ?")
        .bind(&revision_id)
        .bind(&charter_id)
        .execute(db.pool())
        .await
        .expect("approved pointer sets");
    revision_id
}

/// A stand-in for create-spark: creates the target directory with the files a
/// real scaffold carries, records its arguments, and honours the refusal that
/// matters here (an existing target directory).
async fn fake_create_spark(exit_code: i32) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("forge-fake-create-spark-{}", new_uuid_v4()));
    tokio::fs::create_dir_all(&dir)
        .await
        .expect("fake command directory creates");
    let script = dir.join("create-spark");
    let body = format!(
        "#!/bin/sh\n\
         target=\"$1\"\n\
         if [ -e \"$target\" ]; then echo \"Target directory already exists: $target\" >&2; exit 1; fi\n\
         mkdir -p \"$target/docs/spark\" \"$target/.claude/skills/worker-guidelines\"\n\
         printf '{{\"appName\":\"%s\",\"template\":\"%s\"}}\\n' \"$target\" \"$3\" > \"$target/spark.config.json\"\n\
         printf '# AGENTS.md\\n\\nOperating rules for %s.\\n' \"$target\" > \"$target/AGENTS.md\"\n\
         printf 'placeholder north star\\n' > \"$target/docs/spark/project.md\"\n\
         printf 'node_modules\\n' > \"$target/.gitignore\"\n\
         printf '# %s\\n\\nScaffolded by spark.\\n' \"$target\" > \"$target/README.md\"\n\
         printf 'lens\\n' > \"$target/.claude/skills/worker-guidelines/SKILL.md\"\n\
         echo \"$@\" > \"$target/ARGS\"\n\
         if [ {exit_code} -ne 0 ]; then echo 'Unknown pack \"nope\". Registered packs: db-sqlite' >&2; exit {exit_code}; fi\n\
         echo 'Created'\n"
    );
    tokio::fs::write(&script, body)
        .await
        .expect("fake command writes");
    let mut permissions = tokio::fs::metadata(&script)
        .await
        .expect("fake command metadata")
        .permissions();
    use std::os::unix::fs::PermissionsExt;
    permissions.set_mode(0o755);
    tokio::fs::set_permissions(&script, permissions)
        .await
        .expect("fake command becomes executable");
    script
}

async fn checkpoint_details(db: &SqliteDb, operation_id: &str, checkpoint: &str) -> Value {
    let details: String = sqlx::query_scalar(
        "SELECT details_json FROM project_provisioning_checkpoint
         WHERE operation_id = ? AND checkpoint = ?",
    )
    .bind(operation_id)
    .bind(checkpoint)
    .fetch_one(db.pool())
    .await
    .expect("checkpoint details query");
    serde_json::from_str(&details).expect("checkpoint details are JSON")
}

#[tokio::test]
async fn charter_scaffold_runs_the_command_and_commits_the_exported_charter() {
    let _guard = scaffold_env_lock().lock().await;
    let fixture = create_project(false).await;
    remove_path(&fixture.repo_path).await;
    create_native_agent(&fixture.db, "Scaffold Worker").await;
    let revision_id =
        attach_scaffold_charter(&fixture, "nextjs", &["db-sqlite", "ui-shadcn"]).await;
    let script = fake_create_spark(0).await;
    std::env::set_var("FORGE_SCAFFOLD_COMMAND", script.to_string_lossy().as_ref());

    services::project_provisioning::provision_genesis_project(&fixture.db, &fixture.project.id)
        .await
        .expect("scaffolded Genesis provisioning");
    std::env::remove_var("FORGE_SCAFFOLD_COMMAND");

    let operation = provisioning_operation(&fixture.db, &fixture.project.id).await;
    assert_eq!(operation.status, "ready");
    assert_eq!(operation.attempt_count, 1);
    assert_eq!(operation.last_error_code, None);
    assert_eq!(
        checkpoint_statuses(&fixture.db, &operation.id).await[..2],
        [
            ("preflight".to_owned(), "completed".to_owned()),
            ("repository_scaffolded".to_owned(), "completed".to_owned()),
        ]
    );
    let details = checkpoint_details(&fixture.db, &operation.id, "repository_scaffolded").await;
    assert_eq!(details["template"], "nextjs");
    assert_eq!(
        details["packs"],
        serde_json::json!(["db-sqlite", "ui-shadcn"])
    );
    assert_eq!(details["charter_revision_id"], revision_id);
    assert_eq!(
        details["path"],
        fixture.repo_path.to_string_lossy().as_ref()
    );
    assert!(details["command"]
        .as_str()
        .expect("command recorded")
        .ends_with("create-spark"));

    // create-spark received the deterministic directory name, the template,
    // and the pack list — non-interactively.
    let args = tokio::fs::read_to_string(fixture.repo_path.join("ARGS"))
        .await
        .expect("fake command recorded its arguments");
    assert_eq!(
        args.trim(),
        format!(
            "{} --template nextjs --yes --packs db-sqlite,ui-shadcn",
            fixture.repo_path.file_name().unwrap().to_string_lossy()
        )
    );

    // Forge's exports replaced the placeholder and joined the first commit.
    let project_md = tokio::fs::read_to_string(fixture.repo_path.join("docs/spark/project.md"))
        .await
        .expect("exported Charter exists");
    assert!(project_md.contains(&format!("revision `{revision_id}`")));
    assert!(project_md.contains("Forge is the source of truth"));
    assert!(project_md.ends_with(SCAFFOLD_REVISION_VIEW));
    let agents = tokio::fs::read_to_string(fixture.repo_path.join("AGENTS.md"))
        .await
        .expect("AGENTS.md exists");
    assert!(agents.starts_with("# AGENTS.md\n"));
    assert!(agents.contains("\n## Forge\n"));
    assert!(agents.contains("`worker-guidelines` lens is in force"));
    assert!(fixture.repo_path.join("spark.config.json").is_file());
    assert!(git::is_worktree_clean(&fixture.repo_path)
        .await
        .expect("worktree status reads"));
    let readme = tokio::fs::read_to_string(fixture.repo_path.join("README.md"))
        .await
        .expect("the scaffold's README survives");
    assert!(
        !readme.contains("Repository created by Forge Product Genesis"),
        "no README is fabricated over a scaffold: {readme}"
    );

    remove_path(&fixture.repo_path).await;
    remove_path(script.parent().unwrap()).await;
}

#[tokio::test]
async fn missing_scaffold_runtime_is_a_typed_retryable_failure_that_a_retry_clears() {
    let _guard = scaffold_env_lock().lock().await;
    let fixture = create_project(false).await;
    remove_path(&fixture.repo_path).await;
    create_native_agent(&fixture.db, "Runtime Worker").await;
    attach_scaffold_charter(&fixture, "vite-react", &[]).await;
    std::env::set_var(
        "FORGE_SCAFFOLD_COMMAND",
        "/nonexistent/forge-characterization/bunx @forgeailab/create-spark",
    );

    services::project_provisioning::provision_genesis_project(&fixture.db, &fixture.project.id)
        .await
        .expect("provisioning records the blocker instead of erroring");

    let operation = provisioning_operation(&fixture.db, &fixture.project.id).await;
    assert_eq!(operation.status, "setup_required");
    assert_eq!(operation.current_checkpoint, "preflight");
    assert_eq!(operation.attempt_count, 1);
    assert!(operation.retryable);
    assert_eq!(
        operation.last_error_code.as_deref(),
        Some("scaffold_runtime_unavailable")
    );
    assert_eq!(
        provisioning_error_codes(&fixture.db, &operation.id).await,
        vec!["scaffold_runtime_unavailable".to_owned()]
    );
    assert_eq!(
        checkpoint_statuses(&fixture.db, &operation.id).await[1],
        ("repository_scaffolded".to_owned(), "failed".to_owned())
    );
    assert_eq!(repo_count(&fixture.db, &fixture.project.id).await, 0);
    assert!(!fixture.repo_path.exists(), "nothing was created on disk");

    // The host installs the runtime and retries the same operation.
    let script = fake_create_spark(0).await;
    std::env::set_var("FORGE_SCAFFOLD_COMMAND", script.to_string_lossy().as_ref());
    services::project_provisioning::provision_genesis_project(&fixture.db, &fixture.project.id)
        .await
        .expect("retry provisions");
    std::env::remove_var("FORGE_SCAFFOLD_COMMAND");

    let operation = provisioning_operation(&fixture.db, &fixture.project.id).await;
    assert_eq!(operation.status, "ready");
    assert_eq!(operation.attempt_count, 2);
    let args = tokio::fs::read_to_string(fixture.repo_path.join("ARGS"))
        .await
        .expect("fake command recorded its arguments");
    assert!(args
        .trim()
        .ends_with("--template vite-react --yes --no-packs"));
    assert_eq!(repo_count(&fixture.db, &fixture.project.id).await, 1);

    remove_path(&fixture.repo_path).await;
    remove_path(script.parent().unwrap()).await;
}

#[tokio::test]
async fn refused_scaffold_removes_the_partial_directory_and_keeps_the_tool_output() {
    let _guard = scaffold_env_lock().lock().await;
    let fixture = create_project(false).await;
    remove_path(&fixture.repo_path).await;
    create_native_agent(&fixture.db, "Refusal Worker").await;
    attach_scaffold_charter(&fixture, "nextjs", &["nope"]).await;
    let script = fake_create_spark(3).await;
    std::env::set_var("FORGE_SCAFFOLD_COMMAND", script.to_string_lossy().as_ref());

    services::project_provisioning::provision_genesis_project(&fixture.db, &fixture.project.id)
        .await
        .expect("provisioning records the refusal");
    std::env::remove_var("FORGE_SCAFFOLD_COMMAND");

    let operation = provisioning_operation(&fixture.db, &fixture.project.id).await;
    assert_eq!(operation.status, "setup_required");
    assert!(operation.retryable);
    assert_eq!(
        operation.last_error_code.as_deref(),
        Some("repository_scaffold_failed")
    );
    let message: String = sqlx::query_scalar(
        "SELECT last_error_message FROM project_provisioning_operation WHERE id = ?",
    )
    .bind(&operation.id)
    .fetch_one(fixture.db.pool())
    .await
    .expect("error message reads");
    assert!(message.contains("Unknown pack \"nope\""), "{message}");
    let details = checkpoint_details(&fixture.db, &operation.id, "repository_scaffolded").await;
    assert!(details["output_tail"]
        .as_str()
        .expect("tool output kept")
        .contains("Registered packs: db-sqlite"));
    assert!(
        !fixture.repo_path.exists(),
        "the partial directory is removed so the next attempt starts clean"
    );
    assert_eq!(repo_count(&fixture.db, &fixture.project.id).await, 0);

    remove_path(script.parent().unwrap()).await;
}

#[tokio::test]
async fn retry_after_a_scaffold_failure_keeps_one_repository_path() {
    let _guard = scaffold_env_lock().lock().await;
    let fixture = create_project(false).await;
    remove_path(&fixture.repo_path).await;
    create_native_agent(&fixture.db, "Path Worker").await;
    attach_scaffold_charter(&fixture, "nextjs", &["db-sqlite"]).await;
    std::env::set_var(
        "FORGE_SCAFFOLD_COMMAND",
        "/nonexistent/forge-characterization/create-spark",
    );
    services::project_provisioning::provision_genesis_project(&fixture.db, &fixture.project.id)
        .await
        .expect("first attempt records the blocker");
    let operation = provisioning_operation(&fixture.db, &fixture.project.id).await;
    assert_eq!(
        operation.last_error_code.as_deref(),
        Some("scaffold_runtime_unavailable")
    );
    let first_path = checkpoint_details(&fixture.db, &operation.id, "repository_scaffolded").await
        ["path"]
        .as_str()
        .expect("first attempt persisted its target")
        .to_owned();
    assert_eq!(first_path, fixture.repo_path.to_string_lossy());

    // Between attempts the Project is renamed, so a fresh derivation would
    // point somewhere else. Both filesystem checkpoints must keep using the
    // directory the first attempt recorded.
    sqlx::query("UPDATE project SET name = ? WHERE id = ?")
        .bind("Renamed After Scaffold Failure")
        .bind(&fixture.project.id)
        .execute(fixture.db.pool())
        .await
        .expect("Project renames");
    let script = fake_create_spark(0).await;
    std::env::set_var("FORGE_SCAFFOLD_COMMAND", script.to_string_lossy().as_ref());
    services::project_provisioning::provision_genesis_project(&fixture.db, &fixture.project.id)
        .await
        .expect("retry provisions");
    std::env::remove_var("FORGE_SCAFFOLD_COMMAND");

    let operation = provisioning_operation(&fixture.db, &fixture.project.id).await;
    assert_eq!(operation.status, "ready");
    let scaffold_path = checkpoint_details(&fixture.db, &operation.id, "repository_scaffolded")
        .await["path"]
        .as_str()
        .expect("scaffold path")
        .to_owned();
    let init_path = checkpoint_details(&fixture.db, &operation.id, "repository_initialized").await
        ["path"]
        .as_str()
        .expect("init path")
        .to_owned();
    assert_eq!(scaffold_path, first_path);
    assert_eq!(
        init_path, first_path,
        "init must commit the directory the scaffold filled"
    );
    let project = ProjectRepo::get_by_id(&*fixture.db, &fixture.project.id)
        .await
        .expect("Project reloads")
        .expect("Project present");
    let repo = RepoRepo::get_by_id(
        &*fixture.db,
        project.primary_repo_id.as_deref().expect("primary repo"),
    )
    .await
    .expect("repo reloads")
    .expect("repo present");
    assert_eq!(repo.local_path.as_deref(), Some(first_path.as_str()));
    assert!(Path::new(&first_path).join("spark.config.json").is_file());
    assert!(git::is_worktree_clean(Path::new(&first_path))
        .await
        .expect("worktree status reads"));
    let renamed_derivation = repo_path(&project);
    assert_ne!(renamed_derivation, PathBuf::from(&first_path));
    assert!(
        !renamed_derivation.exists(),
        "no second directory is created"
    );

    remove_path(Path::new(&first_path)).await;
    remove_path(script.parent().unwrap()).await;
}
