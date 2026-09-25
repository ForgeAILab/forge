use super::project_agent_workspace::{cleanup_repo_cache_if_authority_gone, resolve_repo_source};
use super::*;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

/// Preserve Git and filesystem failures as typed transient service errors.
/// Dispatch dispositions are for stable governance refusals; converting these
/// failures to `InvalidOperation` text would park a Task until an unrelated
/// version change even though the repository/process may recover on its own.
fn map_workspace_error(error: ::workspace::WorkspaceError) -> ServiceError {
    match error {
        ::workspace::WorkspaceError::Git(error) => ServiceError::Git(error),
        ::workspace::WorkspaceError::Io(error) => ServiceError::Git(git::GitError::Io(error)),
        error => ServiceError::invalid_operation(error.to_string()),
    }
}

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
    let authority = resolve_task_repository_authority(db, task).await?;
    if let Some(parent_task_id) = task.parent_task_id.as_deref() {
        let parent_task = TaskRepo::get_by_id(db, parent_task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", parent_task_id.to_owned()))?;
        if parent_task.project_id != authority.project.id {
            return Err(ServiceError::RepoMismatch {
                project_id: task.project_id.clone(),
            });
        }
        let Some(workspace) = WorkspaceRepo::get_by_task_id(db, parent_task_id).await? else {
            // The root is a coordination container, so its first runnable
            // child creates the shared root-owned worktree on demand. A child
            // admission failure must not clean up that shared workspace.
            let (workspace, _) = create_fresh_workspace(
                db,
                workspace_root,
                &authority.repo,
                parent_task_id,
                repo_cache_locks,
            )
            .await?;
            return Ok((workspace, false));
        };
        ensure_workspace_repository_current(db, task, &workspace, &authority.repo, parent_task_id)
            .await?;
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
                    return Ok((clear_workspace_cleanup_after(db, workspace).await?, false));
                }
                WorktreeReadiness::Missing | WorktreeReadiness::Invalid => {
                    return Ok((
                        clear_workspace_cleanup_after(
                            db,
                            recover_missing_worktree(
                                db,
                                workspace_root,
                                &authority.repo,
                                parent_task_id,
                                workspace,
                                repo_cache_locks,
                                false,
                            )
                            .await?,
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
        ensure_workspace_repository_current(db, task, &workspace, &authority.repo, task_id).await?;
        if workspace.status == WorkspaceStatus::Cleaned {
            // A deliberate teardown (a reassignment reset, cancellation
            // cleanup) removes the worktree but keeps the row, so the Task's
            // next execution rebuilds it here instead of failing admission
            // on a row that can never become ready by itself.
            let repo_source = resolve_repo_source(&authority.repo, workspace_root).await?;
            let branch_exists = git::branch_exists(Path::new(&repo_source), &workspace.branch)
                .await
                .unwrap_or(false);
            if !branch_exists {
                // Nothing left to recover: start over from the default
                // branch. Deleting the row only unlinks past executions
                // (`ON DELETE SET NULL`); their own records are kept.
                WorkspaceRepo::delete(db, &workspace.id).await?;
                return create_fresh_workspace(
                    db,
                    workspace_root,
                    &authority.repo,
                    task_id,
                    repo_cache_locks,
                )
                .await;
            }
            info!(
                task_id = task_id,
                workspace_id = %workspace.id,
                branch = %workspace.branch,
                "recreating cleaned workspace from its task branch"
            );
            return Ok((
                clear_workspace_cleanup_after(
                    db,
                    recover_missing_worktree(
                        db,
                        workspace_root,
                        &authority.repo,
                        task_id,
                        workspace,
                        repo_cache_locks,
                        true,
                    )
                    .await?,
                )
                .await?,
                false,
            ));
        }
        if workspace.status == WorkspaceStatus::Ready {
            match worktree_readiness(Path::new(&workspace.worktree_path)).await {
                WorktreeReadiness::Ready => {
                    info!(
                        task_id = task_id,
                        workspace_id = %workspace.id,
                        worktree_path = %workspace.worktree_path,
                        "reusing existing workspace"
                    );
                    return Ok((clear_workspace_cleanup_after(db, workspace).await?, false));
                }
                WorktreeReadiness::Missing | WorktreeReadiness::Invalid => {
                    return Ok((
                        clear_workspace_cleanup_after(
                            db,
                            recover_missing_worktree(
                                db,
                                workspace_root,
                                &authority.repo,
                                task_id,
                                workspace,
                                repo_cache_locks,
                                true,
                            )
                            .await?,
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

    create_fresh_workspace(
        db,
        workspace_root,
        &authority.repo,
        task_id,
        repo_cache_locks,
    )
    .await
}

async fn ensure_workspace_repository_current(
    db: &SqliteDb,
    task: &Task,
    workspace: &Workspace,
    current_repo: &db::Repo,
    reset_task_id: &str,
) -> Result<()> {
    if workspace.repo_id == current_repo.id {
        return Ok(());
    }

    // A lease tied to an old repository can never authorize work after the
    // Project selects a different primary Repo. Revoke only this Task's lease;
    // the Workspace itself remains intact until the normal guarded reset.
    if let Some(lease) = WorkspaceLeaseRepo::get_active_for_task(db, &task.id).await? {
        WorkspaceLeaseRepo::revoke(db, &lease.id, lease.version, &now_rfc3339()).await?;
    }
    Err(ServiceError::WorkspaceResetRequired {
        task_id: reset_task_id.to_owned(),
        reason: format!(
            "workspace repository {} differs from current Project repository {}; reset is required before a new execution",
            workspace.repo_id, current_repo.id
        ),
    })
}

async fn recover_missing_worktree(
    db: &SqliteDb,
    workspace_root: &std::path::Path,
    repo: &db::Repo,
    task_id: &str,
    workspace: Workspace,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
    delete_missing_workspace: bool,
) -> Result<Workspace> {
    let readiness = worktree_readiness(Path::new(&workspace.worktree_path)).await;
    warn!(
        task_id = task_id,
        workspace_id = %workspace.id,
        worktree_path = %workspace.worktree_path,
        readiness = ?readiness,
        "workspace worktree path missing or unusable, attempting recovery"
    );

    let repo_source = resolve_repo_source(repo, workspace_root).await?;

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
            .map_err(map_workspace_error)?;
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
        if delete_missing_workspace {
            WorkspaceRepo::delete(db, &workspace.id).await?;
        } else {
            // A child may discover that the root branch disappeared while
            // preparing the shared worktree. Keep the root-owned row so the
            // required reset remains an explicit root operation; a child
            // must never delete the delivery workspace as a side effect of
            // its own admission attempt.
            warn!(
                task_id = task_id,
                workspace_id = %workspace.id,
                "shared root branch is gone; preserving workspace row for root reset"
            );
        }
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
        .map_err(|error| ServiceError::Git(git::GitError::Io(error)))?;
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
    repo: &db::Repo,
    task_id: &str,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
) -> Result<(Workspace, bool)> {
    let repo_id = repo.id.as_str();
    let now = now_rfc3339();
    let lock_key = repo
        .local_path
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty() && Path::new(path).exists())
        .map(str::to_owned)
        .unwrap_or_else(|| {
            workspace_root
                .join(".repos")
                .join(repo_id)
                .to_string_lossy()
                .into_owned()
        });
    // The branch-existence check, Git worktree mutation, and unique Workspace
    // insert form one per-repository critical section. Without this outer
    // lock, two concurrent claims can both observe a missing branch and one
    // leaks Git's raw "branch already exists" failure instead of losing the
    // normal Task-version claim race.
    let _repo_cache_guard = if let Some(locks) = &repo_cache_locks {
        Some(locks.acquire(&lock_key).await)
    } else {
        None
    };

    if let Some(workspace) = WorkspaceRepo::get_by_task_id(db, task_id).await? {
        return Ok((clear_workspace_cleanup_after(db, workspace).await?, false));
    }

    // The outer guard already owns the same key WorkspaceManager would use.
    let manager = WorkspaceManager::new(workspace_root.to_path_buf());
    let repo_cache_path = workspace_root.join(".repos").join(repo_id);
    let worktree_source = match resolve_repo_source(repo, workspace_root).await {
        Ok(source) => source,
        Err(error) => {
            cleanup_repo_cache_if_authority_gone(db, &repo.project_id, repo_id, &repo_cache_path)
                .await;
            return Err(error);
        }
    };
    let branch = ::workspace::task_branch_name(task_id);
    let branch_exists = match git::branch_exists(Path::new(&worktree_source), &branch).await {
        Ok(branch_exists) => branch_exists,
        Err(error) => {
            cleanup_repo_cache_if_authority_gone(db, &repo.project_id, repo_id, &repo_cache_path)
                .await;
            return Err(ServiceError::from(error));
        }
    };
    let worktree_result = if branch_exists {
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
    };
    let worktree_path = match worktree_result.map_err(map_workspace_error) {
        Ok(worktree_path) => worktree_path,
        Err(error) => {
            if let Err(cleanup_error) = manager.cleanup_worktree(task_id).await {
                tracing::error!(
                    task_id,
                    error = %cleanup_error,
                    "workspace creation failed and its worktree cleanup also failed"
                );
            }
            cleanup_repo_cache_if_authority_gone(db, &repo.project_id, repo_id, &repo_cache_path)
                .await;
            return Err(error);
        }
    };
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
            // The final Project deletion transaction uses BEGIN IMMEDIATE. If
            // it acquired the writer lock first, this INSERT waits for the
            // task/project cascade and then fails its FK admission; no
            // Workspace row exists, so the worktree created above belongs to
            // this failed admission and must be cleaned. If another creator
            // committed first, the row lookup proves the worktree is theirs,
            // so never remove it. The deletion transaction collects rows that
            // win this race and performs a post-commit path cleanup for the
            // opposite ordering.
            if WorkspaceRepo::get_by_task_id(db, task_id)
                .await
                .ok()
                .flatten()
                .is_none()
            {
                if let Err(cleanup_error) = manager.cleanup_worktree(task_id).await {
                    tracing::error!(
                        task_id,
                        error = %cleanup_error,
                        "workspace admission failed and its worktree cleanup also failed"
                    );
                }
                cleanup_repo_cache_if_authority_gone(
                    db,
                    &repo.project_id,
                    repo_id,
                    &repo_cache_path,
                )
                .await;
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

    Ok((workspace, true))
}

/// Reusing a workspace revives its shared delivery branch. Any cleanup
/// deadline left by a previously terminal child is stale and must be cleared
/// before a new execution can rely on the worktree.
async fn clear_workspace_cleanup_after(db: &SqliteDb, workspace: Workspace) -> Result<Workspace> {
    if workspace.cleanup_after.is_none() {
        return Ok(workspace);
    }
    Ok(WorkspaceRepo::set_cleanup_after(db, &workspace.id, None, &now_rfc3339()).await?)
}

pub(super) async fn reset_workspace(
    db: &SqliteDb,
    workspace_root: &std::path::Path,
    task: &Task,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
) -> Result<Workspace> {
    if task.parent_task_id.is_some() {
        return Err(ServiceError::invalid_operation(
            "subtask workspaces are shared with the coordination root; reset the root workspace instead",
        ));
    }
    let authority = resolve_task_repository_authority(db, task).await?;
    if let Some(workspace) = WorkspaceRepo::get_by_task_id(db, &task.id).await? {
        // Cleanup follows the attempt-pinned Workspace repository. The fresh
        // replacement below follows the Project's current primary Repo.
        let repo = RepoRepo::get_by_id(db, &workspace.repo_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("repo", workspace.repo_id.clone()))?;
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

    let (workspace, _) = create_fresh_workspace(
        db,
        workspace_root,
        &authority.repo,
        &refreshed.id,
        repo_cache_locks,
    )
    .await?;
    Ok(workspace)
}

pub(crate) fn default_workspace_root() -> PathBuf {
    std::env::var("FORGE_WORKSPACE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir().join("forge").join("worktrees"))
}

/// The account-owned scratch root, holding one directory per Main Agent
/// account. It sits beside the Project Agent workspaces rather than inside
/// any of them: nothing here belongs to a Project.
pub const MAIN_AGENT_WORKSPACES_DIR: &str = "main-agents";
/// The Main Agent's own durable notes, inside its scratch directory.
pub const MAIN_AGENT_NOTES_DIR: &str = "notes";
/// The parent of every ephemeral inquiry's private directory. It lives
/// inside the Main Agent's scratch root on purpose: the dispatcher can read
/// a sub-agent's findings file without the sub-agent's own directory ever
/// being writable by anything else.
pub const MAIN_AGENT_INQUIRIES_DIR: &str = "inquiries";

/// Ensure the Main Agent's scratch directory.
///
/// This is deliberately not a repository: no clone, no worktree, no remote,
/// nothing to push. It exists so the Main Agent and the ephemeral inquiries
/// it dispatches can keep findings and working notes on disk instead of
/// carrying them in a conversation that has to survive all day. Because no
/// repository is ever placed here, "cannot write to a repository" holds
/// because there is none, not because a tool was withheld.
pub async fn ensure_main_agent_workspace(
    workspaces_root: &Path,
    account_id: &str,
) -> Result<PathBuf> {
    let workspaces_root = std::fs::canonicalize(workspaces_root).unwrap_or_else(|_| {
        std::env::current_dir()
            .map(|cwd| cwd.join(workspaces_root))
            .unwrap_or_else(|_| workspaces_root.to_path_buf())
    });
    let workspace = workspaces_root
        .join(MAIN_AGENT_WORKSPACES_DIR)
        .join(account_id);
    std::fs::create_dir_all(workspace.join(MAIN_AGENT_NOTES_DIR)).map_err(|error| {
        ServiceError::invalid_operation(format!("Main Agent workspace is unavailable: {error}"))
    })?;
    std::fs::create_dir_all(workspace.join(MAIN_AGENT_INQUIRIES_DIR)).map_err(|error| {
        ServiceError::invalid_operation(format!("Main Agent workspace is unavailable: {error}"))
    })?;
    write_scratch_workspace_boundary(&workspace)?;
    Ok(workspace)
}

/// Ensure one inquiry's private directory inside the dispatching account's
/// scratch root. Each inquiry gets its own so that concurrent sub-agents
/// cannot overwrite each other's findings.
pub async fn ensure_inquiry_workspace(
    workspaces_root: &Path,
    account_id: &str,
    inquiry_id: &str,
) -> Result<PathBuf> {
    let workspace = ensure_main_agent_workspace(workspaces_root, account_id).await?;
    let inquiry = workspace.join(MAIN_AGENT_INQUIRIES_DIR).join(inquiry_id);
    std::fs::create_dir_all(&inquiry).map_err(|error| {
        ServiceError::invalid_operation(format!("inquiry workspace is unavailable: {error}"))
    })?;
    Ok(inquiry)
}

/// The same upward-search guard the verification workspace uses. A scratch
/// directory lives inside the Forge data directory, which on a development
/// server sits inside Forge's own repository, so a `cargo` invocation here
/// would otherwise climb into Forge's workspace and be refused.
fn write_scratch_workspace_boundary(workspace: &Path) -> Result<()> {
    let manifest = workspace.join("Cargo.toml");
    if manifest.exists() {
        return Ok(());
    }
    std::fs::write(
        &manifest,
        "# Forge Main Agent scratch boundary.\n\
         # Cargo searches upward for a workspace root; this manifest is that root, so\n\
         # a build here never resolves against whatever repository holds this data\n\
         # directory. Nothing in this tree is a repository.\n\
         [workspace]\n\
         members = []\n\
         resolver = \"2\"\n",
    )
    .map_err(|error| {
        ServiceError::invalid_operation(format!(
            "Main Agent workspace boundary could not be written: {error}"
        ))
    })
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

    async fn seed_task(db: &SqliteDb, project_id: &str, parent_task_id: Option<String>) -> Task {
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

    async fn seed_workspace(
        db: &SqliteDb,
        task: &Task,
        status: WorkspaceStatus,
        worktree_dir: &std::path::Path,
    ) -> Workspace {
        let worktree_path = worktree_dir.join(&task.id);
        std::fs::create_dir_all(&worktree_path).expect("worktree dir creates");
        let now = now_rfc3339();
        let repo_id = ProjectRepo::get_by_id(db, &task.project_id)
            .await
            .expect("Project loads")
            .expect("Project exists")
            .primary_repo_id
            .expect("Project has a primary Repo");
        WorkspaceRepo::create(
            db,
            CreateWorkspace {
                id: new_uuid_v4(),
                task_id: task.id.clone(),
                repo_id,
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
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let root = seed_task(&db, &project_id, None).await;
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
    async fn existing_workspace_for_old_primary_is_not_reused() {
        let db = sqlite_db().await;
        let (project_id, old_repo_id) = seed_project_repo(&db).await;
        let task = seed_task(&db, &project_id, None).await;
        let worktree_dir = TempDir::new().expect("worktree dir creates");
        let old_workspace =
            seed_workspace(&db, &task, WorkspaceStatus::Ready, worktree_dir.path()).await;
        assert_eq!(old_workspace.repo_id, old_repo_id);

        let replacement_repo_id = new_uuid_v4();
        let now = now_rfc3339();
        RepoRepo::create(
            &db,
            CreateRepo {
                id: replacement_repo_id.clone(),
                project_id: project_id.clone(),
                name: "replacement".to_owned(),
                remote_url: "/tmp/replacement-repo".to_owned(),
                local_path: Some("/tmp/replacement-repo".to_owned()),
                work_mode: db::WorkMode::DirectMerge,
                default_branch: "main".to_owned(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("replacement Repo creates");
        let project = ProjectRepo::get_by_id(&db, &project_id)
            .await
            .expect("Project loads")
            .expect("Project exists");
        ProjectRepo::update_at_version(
            &db,
            UpdateProject {
                id: project_id,
                name: None,
                settings: None,
                primary_repo_id: Some(Some(replacement_repo_id.clone())),
                paused_at: None,
                updated_at: now_rfc3339(),
            },
            project.version,
            None,
        )
        .await
        .expect("Project primary Repo changes");

        let result = prepare_workspace(&db, worktree_dir.path(), &task, &task.id, None).await;
        assert!(
            matches!(
                &result,
                Err(ServiceError::WorkspaceResetRequired { task_id, reason })
                    if task_id == &task.id
                        && reason.contains(&old_repo_id)
                        && reason.contains(&replacement_repo_id)
            ),
            "expected an explicit reset boundary, got: {result:?}"
        );
        let preserved = WorkspaceRepo::get_by_task_id(&db, &task.id)
            .await
            .expect("Workspace reload succeeds")
            .expect("historical Workspace remains until guarded reset");
        assert_eq!(preserved.id, old_workspace.id);
        assert_eq!(preserved.repo_id, old_repo_id);
    }

    #[tokio::test]
    async fn subtask_reuses_ready_parent_workspace() {
        let db = sqlite_db().await;
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let root = seed_task(&db, &project_id, None).await;
        let subtask = seed_task(&db, &project_id, Some(root.id.clone())).await;
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
    async fn subtask_rejects_not_ready_parent_workspace() {
        let db = sqlite_db().await;
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let root = seed_task(&db, &project_id, None).await;
        let subtask = seed_task(&db, &project_id, Some(root.id.clone())).await;
        let temp = TempDir::new().expect("temp dir creates");
        let worktree_dir = TempDir::new().expect("worktree dir creates");
        seed_workspace(&db, &root, WorkspaceStatus::Creating, worktree_dir.path()).await;
        let not_ready = prepare_workspace(&db, temp.path(), &subtask, &subtask.id, None).await;
        assert!(matches!(
            not_ready,
            Err(ServiceError::ParentWorkspaceRequired { parent_task_id }) if parent_task_id == root.id
        ));
    }

    #[tokio::test]
    async fn subtask_without_parent_workspace_creates_root_owned_workspace() {
        let db = sqlite_db().await;
        let repo_dir = TempDir::new().expect("repo dir creates");
        let (project_id, repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let root = seed_task(&db, &project_id, None).await;
        let subtask = seed_task(&db, &project_id, Some(root.id.clone())).await;
        let workspace_root = TempDir::new().expect("workspace root creates");

        let workspace = prepare_workspace(&db, workspace_root.path(), &subtask, &subtask.id, None)
            .await
            .expect("subtask creates the shared root workspace");

        assert_eq!(workspace.task_id, root.id);
        assert_eq!(workspace.repo_id, repo_id);
        assert_eq!(workspace.status, WorkspaceStatus::Ready);
        assert!(std::path::Path::new(&workspace.worktree_path).exists());
        assert!(
            WorkspaceRepo::get_by_task_id(&db, &subtask.id)
                .await
                .expect("subtask Workspace lookup succeeds")
                .is_none(),
            "the shared Workspace must remain owned by the coordination root"
        );
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

    #[tokio::test]
    async fn missing_worktree_with_branch_auto_recovers() {
        let db = sqlite_db().await;
        let repo_dir = TempDir::new().expect("repo dir creates");
        let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let workspace_root = TempDir::new().expect("workspace root creates");
        let task = seed_task(&db, &project_id, None).await;

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
    async fn cleaned_workspace_is_rebuilt_from_its_task_branch() {
        let db = sqlite_db().await;
        let repo_dir = TempDir::new().expect("repo dir creates");
        let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let workspace_root = TempDir::new().expect("workspace root creates");
        let task = seed_task(&db, &project_id, None).await;

        let fresh = prepare_workspace(&db, workspace_root.path(), &task, &task.id, None)
            .await
            .expect("first workspace creates");
        // What a reassignment reset does: the cleanup scheduler removes the
        // worktree and marks the row cleaned, but the task branch survives.
        ::workspace::WorkspaceManager::new(workspace_root.path().to_path_buf())
            .cleanup_worktree(&task.id)
            .await
            .expect("worktree cleans");
        WorkspaceRepo::mark_cleaned(&db, &fresh.id, &now_rfc3339())
            .await
            .expect("workspace marks cleaned");

        let rebuilt = prepare_workspace(&db, workspace_root.path(), &task, &task.id, None)
            .await
            .expect("cleaned workspace is rebuilt");
        assert_eq!(rebuilt.id, fresh.id);
        assert_eq!(rebuilt.branch, fresh.branch);
        assert_eq!(rebuilt.status, WorkspaceStatus::Ready);
        assert!(std::path::Path::new(&rebuilt.worktree_path).exists());
    }

    #[tokio::test]
    async fn cleaned_workspace_without_branch_starts_fresh() {
        let db = sqlite_db().await;
        let repo_dir = TempDir::new().expect("repo dir creates");
        let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let workspace_root = TempDir::new().expect("workspace root creates");
        let task = seed_task(&db, &project_id, None).await;

        let fresh = prepare_workspace(&db, workspace_root.path(), &task, &task.id, None)
            .await
            .expect("first workspace creates");
        ::workspace::WorkspaceManager::new(workspace_root.path().to_path_buf())
            .cleanup_worktree(&task.id)
            .await
            .expect("worktree cleans");
        WorkspaceRepo::mark_cleaned(&db, &fresh.id, &now_rfc3339())
            .await
            .expect("workspace marks cleaned");
        for args in [
            vec!["worktree", "prune"],
            vec!["branch", "-D", fresh.branch.as_str()],
        ] {
            std::process::Command::new("git")
                .args(&args)
                .current_dir(repo_dir.path())
                .env_remove("GIT_DIR")
                .env_remove("GIT_WORK_TREE")
                .env_remove("GIT_INDEX_FILE")
                .output()
                .expect("git runs");
        }

        let rebuilt = prepare_workspace(&db, workspace_root.path(), &task, &task.id, None)
            .await
            .expect("cleaned workspace starts fresh");
        assert_ne!(rebuilt.id, fresh.id);
        assert_eq!(rebuilt.status, WorkspaceStatus::Ready);
        assert!(std::path::Path::new(&rebuilt.worktree_path).exists());
    }

    #[tokio::test]
    async fn unusable_existing_worktree_with_branch_auto_recovers() {
        let db = sqlite_db().await;
        let repo_dir = TempDir::new().expect("repo dir creates");
        let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let workspace_root = TempDir::new().expect("workspace root creates");
        let task = seed_task(&db, &project_id, None).await;

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
        let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let workspace_root = TempDir::new().expect("workspace root creates");
        let task = seed_task(&db, &project_id, None).await;

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
        let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let workspace_root = TempDir::new().expect("workspace root creates");
        let task = seed_task(&db, &project_id, None).await;

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
        let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let workspace_root = TempDir::new().expect("workspace root creates");
        let task = seed_task(&db, &project_id, None).await;

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
