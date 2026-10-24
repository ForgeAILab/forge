//! Shared test fixtures and topic modules.

use super::*;
use db::{create_sqlite_pool, run_migrations, CreateProject, CreateRepo, UpdateProject};
use tempfile::TempDir;

pub(crate) async fn sqlite_db() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    SqliteDb::new(pool)
}

async fn seed_project_repo(db: &SqliteDb) -> (String, String) {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "Forge".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");
    RepoRepo::create(
        db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "repo".to_owned(),
            remote_url: Some("/tmp/repo".to_owned()),
            local_path: Some("/tmp/repo".to_owned()),
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project primary repo updates");
    (project_id, repo_id)
}

pub(crate) async fn seed_task(
    db: &SqliteDb,
    project_id: &str,
    parent_task_id: Option<String>,
) -> Task {
    let now = now_rfc3339();
    TaskRepo::create(
        db,
        CreateTask {
            id: new_uuid_v4(),
            project_id: project_id.to_owned(),
            parent_task_id,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "task".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "todo".to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("task creates")
}

fn init_test_checkout(path: &std::path::Path, branch: &str) {
    let run = |args: &[&str]| {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(path)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .output()
            .expect("git command runs");
        assert!(
            output.status.success(),
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
    };
    run(&["init", "-b", branch]);
    run(&["config", "user.email", "test@forge.dev"]);
    run(&["config", "user.name", "Forge Test"]);
    std::fs::write(path.join("README.md"), "# Test\n").expect("write README");
    run(&["add", "-A"]);
    run(&["commit", "-m", "initial"]);
}

async fn seed_workspace(
    db: &SqliteDb,
    task: &Task,
    status: WorkspaceStatus,
    worktree_dir: &std::path::Path,
) -> Workspace {
    let worktree_path = worktree_dir.join(&task.id);
    std::fs::create_dir_all(&worktree_path).expect("worktree dir creates");
    let branch = ::workspace::task_branch_name(&task.id);
    init_test_checkout(&worktree_path, &branch);
    let now = now_rfc3339();
    let repo_id = ProjectRepo::get_by_id(db, &task.project_id)
        .await
        .expect("Project loads")
        .expect("Project exists")
        .primary_repo_id
        .expect("Project has a primary Repo");
    let repo = RepoRepo::get_by_id(db, &repo_id)
        .await
        .expect("Repo loads")
        .expect("Repo exists");
    persist_prepared_workspace(
        db,
        &repo,
        repo.local_path.as_deref().expect("fixture checkout"),
        CreateWorkspace {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            repo_id,
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch,
            status,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("workspace creates")
}

async fn execution_count(db: &SqliteDb, task_id: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM execution WHERE task_id = ?")
        .bind(task_id)
        .fetch_one(db.pool())
        .await
        .unwrap()
}

async fn seed_unpinned_claim_agent(db: &SqliteDb) -> Agent {
    AgentRepo::create(
        db,
        db::CreateAgent {
            id: new_uuid_v4(),
            name: "claim-agent".to_owned(),
            description: None,
            executor_type: "shell".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: db::AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap()
}

async fn assert_migrated_clone_claim(nonstandard: bool) {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    let migrations = Path::new(env!("CARGO_MANIFEST_DIR")).join("../db/migrations");
    let mut historical = std::fs::read_dir(migrations)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter_map(|path| {
            let name = path.file_name()?.to_str()?;
            let version: i64 = name.strip_prefix('V')?.split_once("__")?.0.parse().ok()?;
            // Placement arrives after the legacy rows are seeded; migrations
            // that read `workspace_placement` are applied with it below.
            (!matches!(
                version,
                202610010400
                    | 202610010530
                    | 202610020600
                    | 202610021500
                    | 202610060838
                    | 202610070507
                    | 202610100137
                    | 202610100820
                    | 202610101906
                    | 202610110650
                    | 202610111217
                    | 202610161644
            ))
            .then_some((version, path))
        })
        .collect::<Vec<_>>();
    historical.sort_by_key(|(version, _)| *version);
    for (_, path) in historical {
        sqlx::raw_sql(&std::fs::read_to_string(path).unwrap())
            .execute(&pool)
            .await
            .unwrap();
    }
    // Scheduler tables are needed by current writers. Its placement
    // triggers wait until the historical placement backfill below.
    let scheduler = include_str!("../../../../db/migrations/V202610060838__task_schedule.sql");
    sqlx::raw_sql(scheduler.split("\nCREATE TRIGGER").next().unwrap())
        .execute(&pool)
        .await
        .unwrap();
    // The reader migration moves bridge annotations into the park table
    // the scheduler migration creates, so it follows it here as it does
    // in version order.
    sqlx::raw_sql(include_str!(
        "../../../../db/migrations/V202610070507__task_condition_readers.sql"
    ))
    .execute(&pool)
    .await
    .unwrap();
    // The check slot-release triggers write the scheduler's tables.
    sqlx::raw_sql(include_str!(
        "../../../../db/migrations/V202610101906__check_slot_release.sql"
    ))
    .execute(&pool)
    .await
    .unwrap();
    // So do the slot-wait age triggers.
    sqlx::raw_sql(include_str!(
        "../../../../db/migrations/V202610110650__slot_wait_age.sql"
    ))
    .execute(&pool)
    .await
    .unwrap();
    // The cutover rewrites stored workflows and retained step snapshots;
    // it is the newest migration and follows the rest, as in version
    // order.
    sqlx::raw_sql(include_str!(
        "../../../../db/migrations/V202610111217__integration_cutover.sql"
    ))
    .execute(&pool)
    .await
    .unwrap();
    // The verify purpose rebuild re-creates the slot-wait age trigger on
    // `check_run`, so it follows that migration, in version order.
    sqlx::raw_sql(include_str!(
        "../../../../db/migrations/V202610161644__review_verify_purpose.sql"
    ))
    .execute(&pool)
    .await
    .unwrap();
    let db = Arc::new(SqliteDb::new(pool));
    let remote = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let (project_id, repo_id) = seed_project_with_real_repo(&db, remote.path()).await;
    let task = seed_task(&db, &project_id, None).await;
    let agent = seed_unpinned_claim_agent(&db).await;
    let daemon = db::DaemonRepo::upsert_by_machine_id(
        &*db,
        db::UpsertDaemon {
            max_concurrent_runs: None,
            id: new_uuid_v4(),
            machine_id: crate::embedded_daemon::embedded_machine_id(),
            hostname: "server".into(),
            os: "linux".into(),
            arch: "x86_64".into(),
            agent_version: None,
            labels_json: "{}".into(),
            status: db::DaemonStatus::Online,
            registration_token_hash: None,
            owner_id: None,
            visibility: "global".into(),
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    db::DaemonRepo::update_report(
        &*db,
        db::UpdateDaemonReport {
            max_concurrent_runs: None,
            id: daemon.id,
            detected_clis_json: r#"[{"kind":"shell","availability":"authenticated"}]"#.into(),
            labels_json: None,
            status: db::DaemonStatus::Online,
            last_report_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();

    let base = git::get_current_sha(remote.path()).await.unwrap();
    // Legacy remote-only repository and a ready task worktree. Its clone
    // cache has disappeared, so first use must clone before selection.
    sqlx::query("UPDATE repo SET local_path = NULL WHERE id = ?")
        .bind(&repo_id)
        .execute(db.pool())
        .await
        .unwrap();
    let path = if nonstandard {
        let path = root.path().join("legacy-worktree");
        git::create_worktree(
            remote.path(),
            &::workspace::task_branch_name(&task.id),
            &path,
        )
        .await
        .unwrap();
        path
    } else {
        ::workspace::WorkspaceManager::new(root.path().to_path_buf())
            .create_worktree_named(remote.path().to_str().unwrap(), &task.id, "repo", "main")
            .await
            .unwrap()
    };
    WorkspaceRepo::create(
        &*db,
        CreateWorkspace {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            repo_id: repo_id.clone(),
            worktree_path: path.to_string_lossy().into_owned(),
            branch: ::workspace::task_branch_name(&task.id),
            status: WorkspaceStatus::Ready,
            before_sha: Some(base),
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    sqlx::raw_sql(include_str!(
        "../../../../db/migrations/V202610010400__daemon_owned_workspaces.sql"
    ))
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::raw_sql(include_str!(
        "../../../../db/migrations/V202610020600__project_machine_readiness.sql"
    ))
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::raw_sql(include_str!(
        "../../../../db/migrations/V202610021500__repo_provision_retry.sql"
    ))
    .execute(db.pool())
    .await
    .unwrap();
    let triggers = format!(
        "CREATE TRIGGER{}",
        scheduler.split_once("\nCREATE TRIGGER").unwrap().1
    );
    sqlx::raw_sql(&triggers).execute(db.pool()).await.unwrap();
    // Rebuilds `task_step` and re-creates the scheduler's step triggers,
    // so it follows them here as it does in version order.
    sqlx::raw_sql(include_str!(
        "../../../../db/migrations/V202610100137__integration_task_step.sql"
    ))
    .execute(db.pool())
    .await
    .unwrap();
    // Rebuilds `task_step` again (step status `suspended`).
    sqlx::raw_sql(include_str!(
        "../../../../db/migrations/V202610100820__task_step_suspended.sql"
    ))
    .execute(db.pool())
    .await
    .unwrap();
    let status: String = sqlx::query_scalar("SELECT status FROM repo_location WHERE repo_id = ?")
        .bind(&repo_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(status, "unverified");
    if nonstandard {
        sqlx::query("UPDATE repo_location SET status = 'invalid', last_error = 'managed_clone_is_worktree' WHERE repo_id = ?")
                .bind(&repo_id).execute(db.pool()).await.unwrap();
    }
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_root(root.path().to_path_buf());
    let claimed = service
        .claim_task(&task.id, Assignee::Agent(agent.id), None)
        .await
        .unwrap();
    assert_eq!(claimed.task.status, "in_progress");
    let location_path: String =
        sqlx::query_scalar("SELECT path FROM repo_location WHERE repo_id = ?")
            .bind(&repo_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(
        location_path,
        root.path().join(".repos").join(&repo_id).to_string_lossy()
    );
    let status: String = sqlx::query_scalar("SELECT status FROM repo_location WHERE repo_id = ?")
        .bind(&repo_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(status, "ready");
    assert!(root.path().join(".repos").join(repo_id).is_dir());
    assert!(path.is_dir());
}

pub(crate) async fn seed_project_with_real_repo(
    db: &SqliteDb,
    repo_path: &std::path::Path,
) -> (String, String) {
    init_test_checkout(repo_path, "main");

    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "Forge".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");
    RepoRepo::create(
        db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "repo".to_owned(),
            remote_url: Some(repo_path.to_string_lossy().into_owned()),
            local_path: Some(repo_path.to_string_lossy().into_owned()),
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project primary repo updates");
    (project_id, repo_id)
}

mod preparation;

mod repository_clone;

mod admission;

mod environment_capacity;

mod disk_pressure;

mod worktree_recovery;

mod owner_wait;
