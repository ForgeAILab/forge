use super::*;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

pub(crate) async fn prepare_workspace(
    db: &SqliteDb,
    workspace_root: &std::path::Path,
    task: &Task,
    task_id: &str,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
) -> Result<Workspace> {
    Ok(
        prepare_workspace_owned(db, workspace_root, task, task_id, repo_cache_locks)
            .await?
            .0,
    )
}

/// Prepare a workspace and report whether this call won creation ownership.
/// The ownership bit is consumed by admission-failure cleanup; callers must
/// never infer it from a racy preflight existence query.
pub(crate) async fn prepare_workspace_owned(
    db: &SqliteDb,
    workspace_root: &std::path::Path,
    task: &Task,
    task_id: &str,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
) -> Result<(Workspace, bool)> {
    if let Some(parent_task_id) = task.parent_task_id.as_deref() {
        let Some(workspace) = WorkspaceRepo::get_by_task_id(db, parent_task_id).await? else {
            return Err(ServiceError::parent_workspace_required(parent_task_id));
        };
        if workspace.status == WorkspaceStatus::Ready {
            match worktree_readiness(Path::new(&workspace.worktree_path)).await {
                WorktreeReadiness::Ready => {
                    info!(
                        task_id = task_id,
                        parent_task_id,
                        workspace_id = %workspace.id,
                        worktree_path = %workspace.worktree_path,
                        "reusing parent workspace"
                    );
                    return Ok((workspace, false));
                }
                WorktreeReadiness::Missing | WorktreeReadiness::Invalid => {
                    let parent_task = TaskRepo::get_by_id(db, parent_task_id, false)
                        .await?
                        .ok_or_else(|| {
                            ServiceError::not_found("task", parent_task_id.to_owned())
                        })?;
                    return Ok((
                        recover_missing_worktree(
                            db,
                            workspace_root,
                            &parent_task,
                            parent_task_id,
                            workspace,
                            repo_cache_locks,
                        )
                        .await?,
                        false,
                    ));
                }
            }
        }
        return Err(ServiceError::parent_workspace_required(parent_task_id));
    }

    if let Some(workspace) = WorkspaceRepo::get_by_task_id(db, task_id).await? {
        if workspace.status == WorkspaceStatus::Ready {
            match worktree_readiness(Path::new(&workspace.worktree_path)).await {
                WorktreeReadiness::Ready => {
                    info!(
                        task_id = task_id,
                        workspace_id = %workspace.id,
                        worktree_path = %workspace.worktree_path,
                        "reusing existing workspace"
                    );
                    return Ok((workspace, false));
                }
                WorktreeReadiness::Missing | WorktreeReadiness::Invalid => {
                    return Ok((
                        recover_missing_worktree(
                            db,
                            workspace_root,
                            task,
                            task_id,
                            workspace,
                            repo_cache_locks,
                        )
                        .await?,
                        false,
                    ));
                }
            }
        }
        return Err(ServiceError::invalid_operation(format!(
            "workspace for task {task_id} is not ready"
        )));
    }

    Ok((
        create_fresh_workspace(db, workspace_root, task, task_id, repo_cache_locks).await?,
        true,
    ))
}

async fn recover_missing_worktree(
    db: &SqliteDb,
    workspace_root: &std::path::Path,
    task: &Task,
    task_id: &str,
    workspace: Workspace,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
) -> Result<Workspace> {
    let readiness = worktree_readiness(Path::new(&workspace.worktree_path)).await;
    warn!(
        task_id = task_id,
        workspace_id = %workspace.id,
        worktree_path = %workspace.worktree_path,
        readiness = ?readiness,
        "workspace worktree path missing or unusable, attempting recovery"
    );

    let repo_id = task
        .repo_id
        .as_deref()
        .ok_or_else(|| ServiceError::invalid_operation("task has no associated repo"))?;
    let repo = RepoRepo::get_by_id(db, repo_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("repo", repo_id.to_owned()))?;
    let repo_source = resolve_repo_source(&repo, workspace_root).await?;

    if !Path::new(&repo_source).exists() {
        return Err(ServiceError::invalid_operation(format!(
            "repo source path does not exist: {repo_source}"
        )));
    }

    if matches!(readiness, WorktreeReadiness::Invalid)
        && try_repair_worktree_gitdir(Path::new(&repo_source), Path::new(&workspace.worktree_path))
            .await
    {
        info!(
            task_id = task_id,
            workspace_id = %workspace.id,
            worktree_path = %workspace.worktree_path,
            "workspace gitdir repaired"
        );
        return Ok(workspace);
    }

    let branch = &workspace.branch;
    let branch_exists = git::branch_exists(Path::new(&repo_source), branch)
        .await
        .unwrap_or(false);

    if branch_exists {
        if !matches!(readiness, WorktreeReadiness::Missing) {
            move_unusable_worktree_aside(Path::new(&workspace.worktree_path)).await?;
        }
        let mut manager = WorkspaceManager::new(workspace_root.to_path_buf());
        if let Some(locks) = repo_cache_locks {
            manager = manager.with_repo_cache_locks(locks);
        }
        let worktree_path = manager
            .recover_worktree_named(&repo_source, task_id, &repo.name, branch)
            .await
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let before_sha = git::get_current_sha(&worktree_path).await.ok();
        let now = now_rfc3339();
        WorkspaceRepo::update_status(db, &workspace.id, WorkspaceStatus::Ready, None, &now).await?;

        info!(
            task_id = task_id,
            workspace_id = %workspace.id,
            branch = %branch,
            worktree_path = %worktree_path.display(),
            before_sha = ?before_sha,
            "workspace recovered from existing branch"
        );

        WorkspaceRepo::get_by_id(db, &workspace.id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", workspace.id))
    } else {
        WorkspaceRepo::delete(db, &workspace.id).await?;
        warn!(
            task_id = task_id,
            branch = %branch,
            "task branch no longer exists in repo, workspace reset required"
        );
        Err(ServiceError::WorkspaceResetRequired {
            task_id: task_id.to_owned(),
            reason: format!(
                "worktree and branch '{branch}' are both gone; workspace must be recreated from {}",
                repo.default_branch
            ),
        })
    }
}

#[derive(Debug)]
enum WorktreeReadiness {
    Ready,
    Missing,
    Invalid,
}

async fn worktree_readiness(worktree_path: &Path) -> WorktreeReadiness {
    if !worktree_path.exists() {
        return WorktreeReadiness::Missing;
    }
    if !worktree_path.join(".git").exists() {
        // Some unit tests seed lightweight workspace rows with plain directories.
        // Real Forge-created worktrees always have .git metadata, so only verify
        // git health when metadata is present.
        return WorktreeReadiness::Ready;
    }
    match git::get_current_sha(worktree_path).await {
        Ok(_) => WorktreeReadiness::Ready,
        Err(_) => WorktreeReadiness::Invalid,
    }
}

async fn try_repair_worktree_gitdir(repo_source: &Path, worktree_path: &Path) -> bool {
    let output = Command::new("git")
        .args(["worktree", "repair", &worktree_path.to_string_lossy()])
        .current_dir(repo_source)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .await;
    match output {
        Ok(output) if output.status.success() => git::get_current_sha(worktree_path).await.is_ok(),
        Ok(output) => {
            warn!(
                repo_source = %repo_source.display(),
                worktree_path = %worktree_path.display(),
                stderr = %String::from_utf8_lossy(&output.stderr).trim(),
                "git worktree repair did not repair workspace"
            );
            false
        }
        Err(error) => {
            warn!(
                repo_source = %repo_source.display(),
                worktree_path = %worktree_path.display(),
                %error,
                "failed to run git worktree repair"
            );
            false
        }
    }
}

async fn move_unusable_worktree_aside(worktree_path: &Path) -> Result<()> {
    if !worktree_path.exists() {
        return Ok(());
    }
    let parent = worktree_path.parent().ok_or_else(|| {
        ServiceError::invalid_operation(format!(
            "worktree path has no parent: {}",
            worktree_path.display()
        ))
    })?;
    let name = worktree_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("worktree");
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    let backup_path: PathBuf = parent.join(format!("{name}.broken-{millis}"));
    tokio::fs::rename(worktree_path, &backup_path)
        .await
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "failed to move unusable worktree aside: {error}"
            ))
        })?;
    warn!(
        worktree_path = %worktree_path.display(),
        backup_path = %backup_path.display(),
        "moved unusable worktree aside before recovery"
    );
    Ok(())
}

async fn create_fresh_workspace(
    db: &SqliteDb,
    workspace_root: &std::path::Path,
    task: &Task,
    task_id: &str,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
) -> Result<Workspace> {
    let repo_id = task
        .repo_id
        .as_deref()
        .ok_or_else(|| ServiceError::invalid_operation("task has no associated repo"))?;
    let repo = RepoRepo::get_by_id(db, repo_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("repo", repo_id.to_owned()))?;
    let now = now_rfc3339();
    let mut manager = WorkspaceManager::new(workspace_root.to_path_buf());
    if let Some(locks) = repo_cache_locks {
        manager = manager.with_repo_cache_locks(locks);
    }
    let worktree_source = resolve_repo_source(&repo, workspace_root).await?;
    let branch = ::workspace::task_branch_name(task_id);
    let branch_exists = git::branch_exists(Path::new(&worktree_source), &branch)
        .await
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    let worktree_path = if branch_exists {
        // A rejected admission may have removed its fresh workspace row and
        // directory after Git created the task branch. Recover that exact
        // task-scoped branch so a corrected retry remains possible and no
        // potentially useful work is discarded.
        manager
            .recover_worktree_named(&worktree_source, task_id, &repo.name, &branch)
            .await
    } else {
        manager
            .create_worktree_named(&worktree_source, task_id, &repo.name, &repo.default_branch)
            .await
    }
    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    let before_sha = git::get_current_sha(&worktree_path).await.ok();
    let workspace_id = new_uuid_v4();
    let workspace = match WorkspaceRepo::create(
        db,
        CreateWorkspace {
            id: workspace_id,
            task_id: task_id.to_owned(),
            repo_id: repo_id.to_owned(),
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch,
            status: WorkspaceStatus::Ready,
            before_sha,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    {
        Ok(workspace) => workspace,
        Err(error) => {
            // A concurrent creator may have won the Task-unique workspace
            // row after this call observed no row.  Never remove its
            // worktree when our INSERT loses that race.
            if WorkspaceRepo::get_by_task_id(db, task_id)
                .await
                .ok()
                .flatten()
                .is_none()
            {
                let _ = manager.cleanup_worktree(task_id).await;
            }
            return Err(error.into());
        }
    };

    info!(
        task_id = task_id,
        workspace_id = %workspace.id,
        repo_id = repo_id,
        workspace_root = %workspace_root.display(),
        worktree_path = %workspace.worktree_path,
        branch = %workspace.branch,
        "workspace created"
    );

    Ok(workspace)
}

/// Ensure the Project Agent's own workspace.
///
/// This is the Agent's working directory, not a delivery worktree. It holds
/// `forge/` -- the durable place it keeps notes, decisions, and helper scripts
/// that later runs and other agents pick up -- and `checkout/`, a disposable
/// copy of the repository it can read and run so it can exercise the delivered
/// software instead of reasoning about whether it works.
///
/// `checkout/` is a detached git worktree: a real working copy on no branch,
/// so it never occupies the task branch namespace and nothing accumulates on
/// it. The Agent has a shell there, so it can of course run `git` -- what it
/// does not have is a write tool for source, a branch anything merges, or a
/// push path. The guarantee is that its edits go nowhere, not that git is
/// disabled.
pub async fn ensure_project_agent_workspace(
    db: &SqliteDb,
    workspaces_root: &Path,
    project_id: &str,
) -> Result<Option<PathBuf>> {
    // `git worktree add` runs with the repository as its working directory, so
    // a relative workspaces root (a `--data-dir ./test` server) would resolve
    // the checkout *inside the repository*. Anchor it absolutely first.
    let workspaces_root = &std::fs::canonicalize(workspaces_root).unwrap_or_else(|_| {
        std::env::current_dir()
            .map(|cwd| cwd.join(workspaces_root))
            .unwrap_or_else(|_| workspaces_root.to_path_buf())
    });
    let workspace = workspaces_root.join(project_id);
    std::fs::create_dir_all(workspace.join(PROJECT_AGENT_DOCS_DIR)).map_err(|error| {
        ServiceError::invalid_operation(format!("Project Agent workspace is unavailable: {error}"))
    })?;
    let Some(repo_id) = ProjectRepo::get_by_id(db, project_id)
        .await?
        .and_then(|project| project.primary_repo_id)
    else {
        // No repository yet: the Agent still gets its durable docs directory.
        return Ok(Some(workspace));
    };
    let Some(repo) = RepoRepo::get_by_id(db, &repo_id).await? else {
        return Ok(Some(workspace));
    };
    if !workspace.join(PROJECT_AGENT_CHECKOUT_DIR).is_dir() {
        let source = resolve_repo_source(&repo, workspaces_root).await?;
        let manager = WorkspaceManager::new(workspaces_root.to_path_buf());
        manager
            .create_detached_worktree_named(
                &source,
                project_id,
                PROJECT_AGENT_CHECKOUT_DIR,
                &repo.default_branch,
            )
            .await
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    }
    Ok(Some(workspace))
}

/// The one directory a Project Agent may write to, inside its workspace.
pub const PROJECT_AGENT_DOCS_DIR: &str = "forge";
/// The read-and-run copy of the repository inside that workspace.
pub const PROJECT_AGENT_CHECKOUT_DIR: &str = "checkout";

async fn resolve_repo_source(repo: &db::Repo, workspace_root: &std::path::Path) -> Result<String> {
    if let Some(local_path) = repo
        .local_path
        .as_deref()
        .map(str::trim)
        .filter(|p| !p.is_empty())
    {
        if Path::new(local_path).exists() {
            return Ok(local_path.to_owned());
        }
    }
    let clone_path = workspace_root.join(".repos").join(&repo.id);
    if !clone_path.exists() {
        tokio::fs::create_dir_all(
            clone_path
                .parent()
                .ok_or_else(|| ServiceError::invalid_operation("repo cache path has no parent"))?,
        )
        .await
        .map_err(|error| {
            ServiceError::invalid_operation(format!("failed to create repo cache: {error}"))
        })?;
        let output = Command::new("git")
            .args(["clone", &repo.remote_url, &clone_path.to_string_lossy()])
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .output()
            .await
            .map_err(|error| {
                ServiceError::invalid_operation(format!("failed to clone repo: {error}"))
            })?;
        if !output.status.success() {
            return Err(ServiceError::invalid_operation(format!(
                "failed to clone repo from {}: {}",
                repo.remote_url,
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
    }
    Ok(clone_path.to_string_lossy().into_owned())
}

pub(super) async fn reset_workspace(
    db: &SqliteDb,
    workspace_root: &std::path::Path,
    task: &Task,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
) -> Result<Workspace> {
    if let Some(workspace) = WorkspaceRepo::get_by_task_id(db, &task.id).await? {
        let repo_id = task
            .repo_id
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("task has no associated repo"))?;
        let repo = RepoRepo::get_by_id(db, repo_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("repo", repo_id.to_owned()))?;
        let repo_source = resolve_repo_source(&repo, workspace_root).await?;
        let worktree_path = Path::new(&workspace.worktree_path);
        if worktree_path.exists() {
            let mut manager = WorkspaceManager::new(workspace_root.to_path_buf());
            if let Some(ref locks) = repo_cache_locks {
                manager = manager.with_repo_cache_locks(Arc::clone(locks));
            }
            let _ = manager.cleanup_worktree(&task.id).await;
        }
        // Prune stale git worktree references
        let _ = tokio::process::Command::new("git")
            .args(["worktree", "prune"])
            .current_dir(&repo_source)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .output()
            .await;
        WorkspaceRepo::delete(db, &workspace.id).await?;
        info!(
            task_id = %task.id,
            workspace_id = %workspace.id,
            "old workspace deleted for reset"
        );
    }

    // Clear error annotation
    db::TaskRepo::update(
        db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(None),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: db::now_rfc3339(),
        },
    )
    .await?;

    let refreshed = db::TaskRepo::get_by_id(db, &task.id, false)
        .await?
        .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;

    create_fresh_workspace(db, workspace_root, &refreshed, &task.id, repo_cache_locks).await
}

pub(crate) fn default_workspace_root() -> PathBuf {
    std::env::var("FORGE_WORKSPACE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir().join("forge").join("worktrees"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{create_sqlite_pool, run_migrations, CreateProject, CreateRepo, UpdateProject};
    use tempfile::TempDir;

    async fn sqlite_db() -> SqliteDb {
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
                remote_url: "/tmp/repo".to_owned(),
                local_path: Some("/tmp/repo".to_owned()),
                work_mode: db::WorkMode::DirectMerge,
                default_branch: "main".to_owned(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("repo creates");
        ProjectRepo::update(
            db,
            UpdateProject {
                id: project_id.clone(),
                name: None,
                settings: None,
                primary_repo_id: Some(Some(repo_id.clone())),
                paused_at: None,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .expect("project primary repo updates");
        (project_id, repo_id)
    }

    async fn seed_task(
        db: &SqliteDb,
        project_id: &str,
        repo_id: &str,
        parent_task_id: Option<String>,
    ) -> Task {
        let now = now_rfc3339();
        TaskRepo::create(
            db,
            CreateTask {
                id: new_uuid_v4(),
                project_id: project_id.to_owned(),
                repo_id: Some(repo_id.to_owned()),
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

    async fn seed_workspace(
        db: &SqliteDb,
        task: &Task,
        status: WorkspaceStatus,
        worktree_dir: &std::path::Path,
    ) -> Workspace {
        let worktree_path = worktree_dir.join(&task.id);
        std::fs::create_dir_all(&worktree_path).expect("worktree dir creates");
        let now = now_rfc3339();
        WorkspaceRepo::create(
            db,
            CreateWorkspace {
                id: new_uuid_v4(),
                task_id: task.id.clone(),
                repo_id: task.repo_id.clone().unwrap_or_default(),
                worktree_path: worktree_path.to_string_lossy().into_owned(),
                branch: ::workspace::task_branch_name(&task.id),
                status,
                before_sha: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("workspace creates")
    }

    #[tokio::test]
    async fn root_task_reuses_ready_task_workspace() {
        let db = sqlite_db().await;
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let root = seed_task(&db, &project_id, &repo_id, None).await;
        let worktree_dir = TempDir::new().expect("worktree dir creates");
        let workspace =
            seed_workspace(&db, &root, WorkspaceStatus::Ready, worktree_dir.path()).await;
        let temp = TempDir::new().expect("temp dir creates");

        let prepared = prepare_workspace(&db, temp.path(), &root, &root.id, None)
            .await
            .expect("root workspace prepares");

        assert_eq!(prepared.id, workspace.id);
        assert_eq!(prepared.task_id, root.id);
    }

    #[tokio::test]
    async fn subtask_reuses_ready_parent_workspace() {
        let db = sqlite_db().await;
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let root = seed_task(&db, &project_id, &repo_id, None).await;
        let subtask = seed_task(&db, &project_id, &repo_id, Some(root.id.clone())).await;
        let worktree_dir = TempDir::new().expect("worktree dir creates");
        let workspace =
            seed_workspace(&db, &root, WorkspaceStatus::Ready, worktree_dir.path()).await;
        let temp = TempDir::new().expect("temp dir creates");

        let prepared = prepare_workspace(&db, temp.path(), &subtask, &subtask.id, None)
            .await
            .expect("subtask workspace prepares");

        assert_eq!(prepared.id, workspace.id);
        assert_eq!(prepared.task_id, root.id);
    }

    #[tokio::test]
    async fn subtask_rejects_missing_or_not_ready_parent_workspace() {
        let db = sqlite_db().await;
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let root = seed_task(&db, &project_id, &repo_id, None).await;
        let subtask = seed_task(&db, &project_id, &repo_id, Some(root.id.clone())).await;
        let temp = TempDir::new().expect("temp dir creates");

        let missing = prepare_workspace(&db, temp.path(), &subtask, &subtask.id, None).await;
        assert!(matches!(
            missing,
            Err(ServiceError::ParentWorkspaceRequired { parent_task_id }) if parent_task_id == root.id
        ));

        let worktree_dir = TempDir::new().expect("worktree dir creates");
        seed_workspace(&db, &root, WorkspaceStatus::Creating, worktree_dir.path()).await;
        let not_ready = prepare_workspace(&db, temp.path(), &subtask, &subtask.id, None).await;
        assert!(matches!(
            not_ready,
            Err(ServiceError::ParentWorkspaceRequired { parent_task_id }) if parent_task_id == root.id
        ));
    }

    #[tokio::test]
    async fn subtask_without_parent_workspace_errors() {
        let db = sqlite_db().await;
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let root = seed_task(&db, &project_id, &repo_id, None).await;
        let subtask = seed_task(&db, &project_id, &repo_id, Some(root.id.clone())).await;
        let temp = TempDir::new().expect("temp dir creates");

        let result = prepare_workspace(&db, temp.path(), &subtask, &subtask.id, None).await;
        assert!(matches!(
            result,
            Err(ServiceError::ParentWorkspaceRequired { .. })
        ));
    }

    async fn seed_project_with_real_repo(
        db: &SqliteDb,
        repo_path: &std::path::Path,
    ) -> (String, String) {
        std::process::Command::new("git")
            .args(["init", "-b", "main"])
            .current_dir(repo_path)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .output()
            .expect("git init");
        std::process::Command::new("git")
            .args(["config", "user.email", "test@forge.dev"])
            .current_dir(repo_path)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .output()
            .expect("git config email");
        std::process::Command::new("git")
            .args(["config", "user.name", "Forge Test"])
            .current_dir(repo_path)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .output()
            .expect("git config name");
        std::fs::write(repo_path.join("README.md"), "# Test\n").expect("write README");
        std::process::Command::new("git")
            .args(["add", "-A"])
            .current_dir(repo_path)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .output()
            .expect("git add");
        std::process::Command::new("git")
            .args(["commit", "-m", "initial"])
            .current_dir(repo_path)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .output()
            .expect("git commit");

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
                remote_url: repo_path.to_string_lossy().into_owned(),
                local_path: Some(repo_path.to_string_lossy().into_owned()),
                work_mode: db::WorkMode::DirectMerge,
                default_branch: "main".to_owned(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("repo creates");
        ProjectRepo::update(
            db,
            UpdateProject {
                id: project_id.clone(),
                name: None,
                settings: None,
                primary_repo_id: Some(Some(repo_id.clone())),
                paused_at: None,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .expect("project primary repo updates");
        (project_id, repo_id)
    }

    #[tokio::test]
    async fn missing_worktree_with_branch_auto_recovers() {
        let db = sqlite_db().await;
        let repo_dir = TempDir::new().expect("repo dir creates");
        let (project_id, repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let workspace_root = TempDir::new().expect("workspace root creates");
        let task = seed_task(&db, &project_id, &repo_id, None).await;

        let fresh = prepare_workspace(&db, workspace_root.path(), &task, &task.id, None)
            .await
            .expect("first workspace creates");
        assert_eq!(fresh.status, WorkspaceStatus::Ready);
        let branch = fresh.branch.clone();

        // Delete the worktree directory to simulate a stale workspace
        std::fs::remove_dir_all(&fresh.worktree_path).expect("remove worktree dir");
        assert!(!std::path::Path::new(&fresh.worktree_path).exists());

        // Branch still exists in repo — recovery should recreate the worktree
        let recovered = prepare_workspace(&db, workspace_root.path(), &task, &task.id, None)
            .await
            .expect("workspace auto-recovers");
        assert_eq!(recovered.id, fresh.id);
        assert_eq!(recovered.branch, branch);
        assert!(std::path::Path::new(&recovered.worktree_path).exists());
    }

    #[tokio::test]
    async fn unusable_existing_worktree_with_branch_auto_recovers() {
        let db = sqlite_db().await;
        let repo_dir = TempDir::new().expect("repo dir creates");
        let (project_id, repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let workspace_root = TempDir::new().expect("workspace root creates");
        let task = seed_task(&db, &project_id, &repo_id, None).await;

        let fresh = prepare_workspace(&db, workspace_root.path(), &task, &task.id, None)
            .await
            .expect("first workspace creates");
        std::fs::write(
            std::path::Path::new(&fresh.worktree_path).join(".git"),
            "gitdir: /tmp/forge-missing-gitdir\n",
        )
        .expect("break gitdir reference");
        assert!(
            git::get_current_sha(std::path::Path::new(&fresh.worktree_path))
                .await
                .is_err()
        );

        let recovered = prepare_workspace(&db, workspace_root.path(), &task, &task.id, None)
            .await
            .expect("workspace auto-recovers");

        assert_eq!(recovered.id, fresh.id);
        assert_eq!(recovered.branch, fresh.branch);
        assert!(
            git::get_current_sha(std::path::Path::new(&recovered.worktree_path))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn existing_worktree_with_missing_repo_source_errors_before_reuse() {
        let db = sqlite_db().await;
        let repo_dir = TempDir::new().expect("repo dir creates");
        let (project_id, repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let workspace_root = TempDir::new().expect("workspace root creates");
        let task = seed_task(&db, &project_id, &repo_id, None).await;

        let fresh = prepare_workspace(&db, workspace_root.path(), &task, &task.id, None)
            .await
            .expect("first workspace creates");
        std::fs::remove_dir_all(repo_dir.path()).expect("remove repo dir");

        let result = prepare_workspace(&db, workspace_root.path(), &task, &task.id, None).await;
        assert!(
            matches!(&result, Err(ServiceError::InvalidOperation { message }) if message.contains("does not exist")),
            "expected InvalidOperation about missing repo, got: {result:?}"
        );
        assert!(
            std::path::Path::new(&fresh.worktree_path).exists(),
            "unrecoverable worktree should be left in place"
        );
    }

    #[tokio::test]
    async fn missing_worktree_and_branch_returns_reset_required() {
        let db = sqlite_db().await;
        let repo_dir = TempDir::new().expect("repo dir creates");
        let (project_id, repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let workspace_root = TempDir::new().expect("workspace root creates");
        let task = seed_task(&db, &project_id, &repo_id, None).await;

        let fresh = prepare_workspace(&db, workspace_root.path(), &task, &task.id, None)
            .await
            .expect("first workspace creates");
        let branch = fresh.branch.clone();

        // Delete worktree AND the branch
        std::fs::remove_dir_all(&fresh.worktree_path).expect("remove worktree dir");
        std::process::Command::new("git")
            .args(["worktree", "prune"])
            .current_dir(repo_dir.path())
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .output()
            .expect("git worktree prune");
        std::process::Command::new("git")
            .args(["branch", "-D", &branch])
            .current_dir(repo_dir.path())
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .output()
            .expect("delete branch");

        let result = prepare_workspace(&db, workspace_root.path(), &task, &task.id, None).await;
        assert!(
            matches!(&result, Err(ServiceError::WorkspaceResetRequired { task_id, .. }) if *task_id == task.id),
            "expected WorkspaceResetRequired, got: {result:?}"
        );

        // Workspace record should have been deleted
        let ws = WorkspaceRepo::get_by_task_id(&db, &task.id)
            .await
            .expect("db query ok");
        assert!(ws.is_none(), "workspace record should be deleted");
    }

    #[tokio::test]
    async fn missing_repo_source_returns_io_error() {
        let db = sqlite_db().await;
        let repo_dir = TempDir::new().expect("repo dir creates");
        let (project_id, repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let workspace_root = TempDir::new().expect("workspace root creates");
        let task = seed_task(&db, &project_id, &repo_id, None).await;

        let fresh = prepare_workspace(&db, workspace_root.path(), &task, &task.id, None)
            .await
            .expect("first workspace creates");

        // Delete worktree AND the entire repo directory
        std::fs::remove_dir_all(&fresh.worktree_path).expect("remove worktree dir");
        std::fs::remove_dir_all(repo_dir.path()).expect("remove repo dir");

        let result = prepare_workspace(&db, workspace_root.path(), &task, &task.id, None).await;
        assert!(
            matches!(&result, Err(ServiceError::InvalidOperation { message }) if message.contains("does not exist")),
            "expected InvalidOperation about missing repo, got: {result:?}"
        );
    }
}
