//! Shared test fixtures and topic modules.

use super::*;
use db::{
    create_sqlite_pool, new_uuid_v4, run_migrations, CreateProject, CreateRepo, CreateTask,
    CreateWorkspace, ProjectRepo, RepoRepo, TaskRepo, UpdateProject, WorkspaceStatus,
};
use tempfile::TempDir;

async fn sqlite_db() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    Arc::new(SqliteDb::new(pool))
}

async fn seed_workspace(
    db: &SqliteDb,
    workspace_root: &std::path::Path,
    status: WorkspaceStatus,
) -> (String, PathBuf) {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let task_id = new_uuid_v4();
    let workspace_id = new_uuid_v4();
    let worktree_path = workspace_root.join(&task_id).join("repo");
    let branch = workspace::task_branch_name(&task_id);
    let repo_path = workspace_root.join(".repos").join(&repo_id);
    std::fs::create_dir_all(&repo_path).expect("repo creates");
    git::init(&repo_path).await.expect("git initializes");
    git::commit_all(&repo_path, "initial commit")
        .await
        .expect("initial commit");
    WorkspaceManager::new(workspace_root.to_path_buf())
        .create_worktree_named(
            repo_path.to_str().expect("repo path"),
            &task_id,
            "repo",
            "main",
        )
        .await
        .expect("worktree creates");
    // Undelivered work: a branch with no commit of its own is already
    // contained in `main` and would be reclaimed with the worktree.
    std::fs::write(worktree_path.join("task-change.txt"), "task change")
        .expect("task change writes");
    git::commit_all(&worktree_path, "task change")
        .await
        .expect("task change commits");

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
            remote_url: Some("https://example.com/repo.git".to_owned()),
            local_path: None,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
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
    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.clone(),
            project_id: project_id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Cleanup task".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "done".to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task creates");
    WorkspaceRepo::create(
        db,
        CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task_id.clone(),
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
    .expect("workspace creates");
    std::fs::create_dir_all(worktree_path.join("node_modules")).expect("build output creates");
    std::fs::write(
        worktree_path.join("node_modules/build-output"),
        "build output",
    )
    .expect("build output writes");
    let logs = workspace_root
        .join(".forge/logs")
        .join(project_id)
        .join(task_id);
    std::fs::create_dir_all(logs.join(".codex-managed-home/task-scratch"))
        .expect("managed home creates");
    std::fs::write(logs.join("execution.jsonl"), "retained log").expect("log writes");

    (workspace_id, worktree_path)
}

async fn fixture_task(db: &SqliteDb, workspace_id: &str) -> db::Task {
    let workspace = WorkspaceRepo::get_by_id(db, workspace_id)
        .await
        .expect("workspace loads")
        .expect("workspace exists");
    TaskRepo::get_by_id(db, &workspace.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists")
}

fn logs_dir(root: &Path, task: &db::Task) -> PathBuf {
    root.join(".forge/logs")
        .join(&task.project_id)
        .join(&task.id)
}

fn assert_retained_logs(root: &Path, task: &db::Task) {
    let logs = logs_dir(root, task);
    assert!(!logs.join(".codex-managed-home").exists());
    assert_eq!(
        std::fs::read_to_string(logs.join("execution.jsonl")).expect("log remains"),
        "retained log"
    );
}

async fn set_fixture_status(db: &SqliteDb, task: &db::Task, status: &str) -> db::Task {
    TaskRepo::update_status(
        db,
        db::UpdateTaskStatus {
            id: task.id.clone(),
            expected_version: task.version,
            status: status.to_owned(),
            assignee_id: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("fixture status updates")
}

async fn fixture_execution(
    db: &SqliteDb,
    task: &db::Task,
    workspace_id: &str,
    status: db::ExecutionStatus,
) -> db::Execution {
    let now = now_rfc3339();
    db::ExecutionRepo::create(
        db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: None,
            role: "coder".to_owned(),
            status,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace_id.to_owned()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("execution creates")
}

struct FailingTerminalCleanup;

#[async_trait]
impl WorkspaceCleanupObserver for FailingTerminalCleanup {
    async fn cleanup_workspace_terminals(&self, _workspace_id: &str) -> Result<()> {
        Err(ServiceError::invalid_operation("cleanup observer failed"))
    }
}

struct FailingWorkspaceCleanup {
    workspace_id: String,
}

#[async_trait]
impl WorkspaceCleanupObserver for FailingWorkspaceCleanup {
    async fn cleanup_workspace_terminals(&self, workspace_id: &str) -> Result<()> {
        if workspace_id == self.workspace_id {
            return Err(ServiceError::invalid_operation("cleanup observer failed"));
        }
        Ok(())
    }
}

async fn seed_log_only_task(
    db: &SqliteDb,
    root: &Path,
    task: &db::Task,
    id: String,
    parent_task_id: Option<String>,
    status: &str,
) -> db::Task {
    let now = now_rfc3339();
    let task = TaskRepo::create(
        db,
        CreateTask {
            id,
            project_id: task.project_id.clone(),
            parent_task_id,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Log cleanup".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: status.to_owned(),
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
    .expect("log-only task creates");
    let logs = logs_dir(root, &task);
    std::fs::create_dir_all(logs.join(".codex-managed-home")).expect("managed home creates");
    std::fs::write(logs.join("execution.jsonl"), "retained log").expect("log writes");
    task
}

async fn fixture_git(cwd: &Path, args: &[&str]) -> String {
    let output = git::command_output(cwd, args).await.expect("git runs");
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

async fn fixture_source(db: &SqliteDb, root: &Path, workspace_id: &str) -> (PathBuf, String) {
    let workspace = WorkspaceRepo::get_by_id(db, workspace_id)
        .await
        .unwrap()
        .unwrap();
    (
        root.join(".repos").join(&workspace.repo_id),
        workspace.branch,
    )
}

async fn clean_due(scheduler: &WorkspaceCleanupScheduler, workspace_id: &str) {
    scheduler
        .schedule(workspace_id, Duration::ZERO)
        .await
        .unwrap();
    scheduler.tick().await.unwrap();
}

async fn cleanup_attention(db: &SqliteDb, workspace_id: &str) -> Vec<(String, i64)> {
    sqlx::query_as::<_, (String, i64)>(
        "SELECT status, version FROM attention_projection WHERE dedupe_key = ?",
    )
    .bind(cleanup_attention_key(workspace_id))
    .fetch_all(db.pool())
    .await
    .unwrap()
}

/// A workspace whose source is the user's own checkout at
/// `<root>/vol/user-repo`, recorded on the Repo, with Forge holding no
/// clone. `vol` stands for the volume the checkout lives on.
async fn user_repo_fixture(db: &Arc<SqliteDb>, root: &Path) -> (String, PathBuf, PathBuf) {
    let (workspace_id, worktree_path) = seed_workspace(db, root, WorkspaceStatus::Ready).await;
    let (cache, _branch) = fixture_source(db, root, &workspace_id).await;
    let workspace = WorkspaceRepo::get_by_id(&**db, &workspace_id)
        .await
        .unwrap()
        .unwrap();
    let user_repo = root.join("vol").join("user-repo");
    std::fs::create_dir_all(user_repo.parent().unwrap()).unwrap();
    std::fs::rename(&cache, &user_repo).unwrap();
    fixture_git(
        &user_repo,
        &["worktree", "repair", &worktree_path.to_string_lossy()],
    )
    .await;
    sqlx::query("UPDATE repo SET local_path = ? WHERE id = ?")
        .bind(user_repo.to_string_lossy().as_ref())
        .bind(&workspace.repo_id)
        .execute(db.pool())
        .await
        .unwrap();
    (workspace_id, worktree_path, user_repo)
}

async fn worktree_registered(repo: &Path, worktree: &Path) -> bool {
    fixture_git(repo, &["worktree", "list", "--porcelain"])
        .await
        .contains(worktree.to_string_lossy().as_ref())
}

const GC_TEST_BUDGET: Duration = Duration::from_secs(30);
const HOUR: Duration = Duration::from_secs(60 * 60);

/// A scheduler on `root` after the start-up step a running server does:
/// the root is adopted for this database.
async fn gc_scheduler(db: &Arc<SqliteDb>, root: &Path) -> WorkspaceCleanupScheduler {
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(db),
        Arc::new(EventBus::new(16)),
        root.to_path_buf(),
    );
    scheduler.adopt_workspace_root().await.unwrap();
    scheduler
}

/// An unrecorded Task root that Forge made (it carries `.forge-task`).
fn forge_made_root(root: &Path) -> PathBuf {
    let task_root = root.join(new_uuid_v4());
    std::fs::create_dir_all(task_root.join(".forge-task")).unwrap();
    std::fs::create_dir_all(task_root.join("repo")).unwrap();
    std::fs::write(task_root.join("repo/file"), "content").unwrap();
    task_root
}

/// Make `path` and what is directly inside it look `days` old.
fn backdate(path: &Path, days: u64) {
    let then = std::time::SystemTime::now() - Duration::from_secs(days * 24 * 60 * 60);
    let mut paths = vec![path.to_path_buf()];
    paths.extend(
        std::fs::read_dir(path)
            .unwrap()
            .flatten()
            .map(|entry| entry.path()),
    );
    for path in paths.iter().rev() {
        std::fs::File::open(path)
            .unwrap()
            .set_modified(then)
            .unwrap();
    }
}

async fn gc_at(scheduler: &WorkspaceCleanupScheduler, after: Duration) -> executors::gc::GcReport {
    scheduler
        .gc_pass_at(
            &mut SweepCursor::default(),
            GC_TEST_BUDGET,
            std::time::SystemTime::now() + after,
        )
        .await
}

fn quarantined(root: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(root.join(executors::gc::GC_DIR)) else {
        return Vec::new();
    };
    let mut names: Vec<String> = entries
        .flatten()
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| uuid::Uuid::parse_str(name.get(..36).unwrap_or_default()).is_ok())
        .collect();
    names.sort();
    names
}

mod cleanup;

mod scheduling;

mod log_retention;

mod repository_loss;

mod delivery_branches;

mod garbage_collection;
