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
    // Capture an immutable generation/repository snapshot before touching the
    // filesystem. A Project ID can be explicitly reused after deletion; the
    // operation ID distinguishes those generations even when both rows start
    // at Project.version = 1.
    let Some((authority, repo)) = capture_project_agent_workspace_authority(db, project_id).await?
    else {
        return Ok(None);
    };

    // `git worktree add` runs with the repository as its working directory, so
    // a relative workspaces root (a `--data-dir ./test` server) would resolve
    // the checkout *inside the repository*. Anchor it absolutely first.
    let workspaces_root = &std::fs::canonicalize(workspaces_root).unwrap_or_else(|_| {
        std::env::current_dir()
            .map(|cwd| cwd.join(workspaces_root))
            .unwrap_or_else(|_| workspaces_root.to_path_buf())
    });
    let workspace = workspaces_root.join(project_id);
    // Reserve the path and stamp its generation before creating notes or a
    // checkout. If a replacement Project already claimed the same ID, the
    // old marker is atomically quarantined under the DB writer lock and never
    // becomes part of the replacement workspace.
    if !reserve_project_agent_workspace_path(db, &authority, &workspace).await? {
        return Ok(None);
    }
    std::fs::create_dir_all(workspace.join(PROJECT_AGENT_DOCS_DIR)).map_err(|error| {
        ServiceError::invalid_operation(format!("Project Agent workspace is unavailable: {error}"))
    })?;
    write_verification_workspace_boundary(&workspace)?;

    let Some(repo) = repo else {
        // No repository yet: the Agent still gets its durable docs directory.
        return retain_project_agent_workspace_if_current(db, &authority, workspace, None).await;
    };
    let checkout = workspace.join(PROJECT_AGENT_CHECKOUT_DIR);
    let repo_cache = workspaces_root.join(".repos").join(&repo.id);
    if checkout.is_dir() {
        // The checkout is a disposable read-and-run copy: re-point it at the
        // current default-branch tip every time it is handed out, so the
        // Agent verifies what Tasks actually delivered rather than the tree
        // as of Project creation.
        let manager = WorkspaceManager::new(workspaces_root.to_path_buf());
        manager
            .refresh_detached_worktree(&checkout, &repo.default_branch)
            .await
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    } else {
        let source = match resolve_repo_source(&repo, workspaces_root).await {
            Ok(source) => source,
            Err(error) => {
                if let Err(cleanup_error) = retain_project_agent_workspace_if_current(
                    db,
                    &authority,
                    workspace.clone(),
                    Some(repo_cache.clone()),
                )
                .await
                {
                    tracing::error!(
                        project_id,
                        error = %cleanup_error,
                        "Project Agent workspace cleanup after repository-source failure failed"
                    );
                }
                return Err(error);
            }
        };
        let manager = WorkspaceManager::new(workspaces_root.to_path_buf());
        if let Err(error) = manager
            .create_detached_worktree_named(
                &source,
                project_id,
                PROJECT_AGENT_CHECKOUT_DIR,
                &repo.default_branch,
            )
            .await
        {
            if let Err(cleanup_error) = retain_project_agent_workspace_if_current(
                db,
                &authority,
                workspace.clone(),
                Some(repo_cache.clone()),
            )
            .await
            {
                tracing::error!(
                    project_id,
                    error = %cleanup_error,
                    "Project Agent workspace cleanup after checkout creation failure failed"
                );
            }
            return Err(ServiceError::invalid_operation(error.to_string()));
        }
    }
    retain_project_agent_workspace_if_current(db, &authority, workspace, Some(repo_cache)).await
}

async fn retain_project_agent_workspace_if_current(
    db: &SqliteDb,
    authority: &ProjectAgentWorkspaceAuthority,
    workspace: PathBuf,
    repo_cache: Option<PathBuf>,
) -> Result<Option<PathBuf>> {
    let mut transaction = db::begin_immediate(db.pool()).await?;
    let current =
        query_project_agent_workspace_authority(&mut *transaction, &authority.project_id).await?;
    if current
        .as_ref()
        .is_some_and(|current| authority.same_generation_and_repository(current))
    {
        transaction.commit().await?;
        return Ok(Some(workspace));
    }

    // Only quarantine a workspace carrying this exact old generation's
    // marker. A replacement Project may already have claimed the same path;
    // in that case its marker makes the cleanup a no-op. A same-generation
    // repository change preserves notes and discards only the checkout.
    let stale_workspace = if current
        .as_ref()
        .is_some_and(|current| authority.same_generation(current))
    {
        quarantine_project_agent_checkout_if_repository_matches(
            &workspace,
            &authority.repository_marker_contents(),
        )
        .await?
    } else {
        quarantine_project_agent_workspace_if_marker_matches(
            &workspace,
            &authority.generation_marker_contents(),
        )
        .await?
    };

    // A cache is keyed by repository identity. Keep it whenever a live Repo
    // row with that identity exists, including a replacement Project that
    // reused the same repository ID; otherwise the captured generation owns
    // the cache and it can be quarantined with this cleanup.
    let stale_cache = if let (Some(repo_cache), Some(repository_identity)) = (
        repo_cache.as_deref(),
        authority.repository_identity.as_ref(),
    ) {
        let repo_live = sqlx::query_scalar::<_, i64>("SELECT 1 FROM repo WHERE id = ? LIMIT 1")
            .bind(&repository_identity.0)
            .fetch_optional(&mut *transaction)
            .await?
            .is_some();
        if repo_live {
            None
        } else {
            quarantine_project_agent_cache(repo_cache).await?
        }
    } else {
        None
    };
    transaction.commit().await?;

    for path in [stale_workspace, stale_cache].into_iter().flatten() {
        remove_quarantined_project_agent_path(&path).await?;
    }
    Ok(None)
}

/// A repository clone can finish after Project deletion's DB transaction has
/// captured its repository rows. If the following Workspace admission then
/// loses the Project FK race, the worktree cleanup above is not enough: this
/// cache was created after the DB snapshot and would otherwise survive forever.
/// Remove it only after both authorities are gone, so a normal unique-row
/// admission failure cannot destroy a cache still owned by a live Project.
async fn cleanup_repo_cache_if_authority_gone(
    db: &SqliteDb,
    project_id: &str,
    repo_id: &str,
    repo_cache: &Path,
) {
    // Serialize the authority check with Project/Repo admission. A plain
    // read followed by remove could delete a replacement repository cache
    // claimed in the gap between those operations.
    let mut transaction = match db::begin_immediate(db.pool()).await {
        Ok(transaction) => transaction,
        Err(error) => {
            tracing::error!(
                project_id,
                repo_id,
                error = %error,
                "could not acquire database guard before repository-cache cleanup"
            );
            return;
        }
    };
    let project_exists =
        match sqlx::query_scalar::<_, i64>("SELECT 1 FROM project WHERE id = ? LIMIT 1")
            .bind(project_id)
            .fetch_optional(&mut *transaction)
            .await
        {
            Ok(row) => row.is_some(),
            Err(error) => {
                tracing::error!(
                    project_id,
                    repo_id,
                    error = %error,
                    "could not verify Project authority before repository-cache cleanup"
                );
                return;
            }
        };
    let repo_exists = match sqlx::query_scalar::<_, i64>("SELECT 1 FROM repo WHERE id = ? LIMIT 1")
        .bind(repo_id)
        .fetch_optional(&mut *transaction)
        .await
    {
        Ok(row) => row.is_some(),
        Err(error) => {
            tracing::error!(
                project_id,
                repo_id,
                error = %error,
                "could not verify repository authority before repository-cache cleanup"
            );
            return;
        }
    };
    if project_exists && repo_exists {
        if let Err(error) = transaction.commit().await {
            tracing::error!(
                project_id,
                repo_id,
                error = %error,
                "could not release database guard after repository-cache cleanup check"
            );
        }
        return;
    }

    let quarantine = match quarantine_project_agent_cache(repo_cache).await {
        Ok(quarantine) => quarantine,
        Err(error) => {
            tracing::error!(
                project_id,
                repo_id,
                path = %repo_cache.display(),
                error = %error,
                "repository-cache quarantine after Project deletion failed"
            );
            return;
        }
    };
    if let Err(error) = transaction.commit().await {
        if let Some(quarantine) = quarantine.as_deref() {
            if let Err(restore_error) = tokio::fs::rename(quarantine, repo_cache).await {
                tracing::error!(
                    project_id,
                    repo_id,
                    path = %repo_cache.display(),
                    error = %restore_error,
                    "repository-cache restore after database-guard failure also failed"
                );
            }
        }
        tracing::error!(
            project_id,
            repo_id,
            error = %error,
            "could not commit database guard after repository-cache quarantine"
        );
        return;
    }
    if let Some(quarantine) = quarantine {
        if let Err(error) = remove_quarantined_project_agent_path(&quarantine).await {
            tracing::error!(
                project_id,
                repo_id,
                path = %quarantine.display(),
                error = %error,
                "repository-cache removal after Project deletion failed"
            );
        }
    }
}

/// The manifest that stops cargo's upward workspace search at the
/// verification workspace root.
///
/// Cargo resolves a package's workspace by climbing from its manifest to the
/// nearest parent `Cargo.toml` that declares `[workspace]`. A Project Agent's
/// checkout lives inside the Forge data directory, and a development server
/// keeps that directory inside Forge's own repository — so a build in
/// `checkout/` climbed into Forge's workspace and was refused as an unlisted
/// member ("current package believes it's in a workspace when it's not").
/// This root manifest is the nearest workspace instead, with the checkout as
/// its only member. A checkout that declares its own `[workspace]` never
/// reaches it; a checkout without a `Cargo.toml` never consults it.
pub const VERIFICATION_WORKSPACE_BOUNDARY_MANIFEST: &str =
    "# Forge verification workspace boundary.
# Cargo searches upward from checkout/ for a workspace root; this manifest is
# that root, so a build there never resolves against whatever repository
# holds this data directory. Do not add packages here.
[workspace]
members = [\"checkout\"]
resolver = \"2\"
";

/// Write the workspace-boundary manifest at `workspace` unless one exists.
pub(crate) fn write_verification_workspace_boundary(workspace: &Path) -> Result<()> {
    let manifest = workspace.join("Cargo.toml");
    if manifest.exists() {
        return Ok(());
    }
    std::fs::write(&manifest, VERIFICATION_WORKSPACE_BOUNDARY_MANIFEST).map_err(|error| {
        ServiceError::invalid_operation(format!(
            "Project Agent workspace boundary could not be written: {error}"
        ))
    })
}

/// The one directory a Project Agent may write to, inside its workspace.
pub const PROJECT_AGENT_DOCS_DIR: &str = "forge";
/// The read-and-run copy of the repository inside that workspace. The
/// runtime runs the Agent's verification commands inside this directory.
pub const PROJECT_AGENT_CHECKOUT_DIR: &str = forge_agent_host::PROJECT_VERIFICATION_CHECKOUT_DIR;

/// Stored inside a Project Agent workspace before any notes or checkout files
/// are created.  The directory name is intentionally still the public
/// Project ID, but the marker makes that path an ownership claim for one
/// immutable Project generation rather than for every future row with the
/// same ID.
const PROJECT_AGENT_GENERATION_MARKER: &str = ".forge-project-agent-generation";
/// Stores only a non-secret digest of the repository snapshot. Raw remote
/// URLs and local paths never enter the Agent-visible workspace.
const PROJECT_AGENT_REPOSITORY_MARKER: &str = ".forge-project-agent-repository";

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProjectAgentWorkspaceAuthority {
    project_id: String,
    project_created_at: String,
    provisioning_operation_id: Option<String>,
    primary_repo_id: Option<String>,
    /// `(id, remote_url, local_path, default_branch)` for the exact primary
    /// repository snapshot used to build the checkout.
    repository_identity: Option<(String, String, Option<String>, String)>,
}

impl ProjectAgentWorkspaceAuthority {
    /// This marker is deliberately generation-only. Project.version and the
    /// selected repository can change inside one Project generation without
    /// invalidating the durable `forge/` notes.
    fn generation_marker_contents(&self) -> String {
        serde_json::to_string(&(
            &self.project_id,
            &self.project_created_at,
            &self.provisioning_operation_id,
        ))
        .expect("Project Agent workspace authority is serializable")
    }

    fn repository_marker_contents(&self) -> String {
        let serialized = serde_json::to_string(&self.repository_identity)
            .expect("Project Agent repository authority is serializable");
        let mut digest = Sha256::new();
        digest.update(serialized.as_bytes());
        hex::encode(digest.finalize())
    }

    fn same_generation(&self, other: &Self) -> bool {
        self.project_id == other.project_id
            && self.project_created_at == other.project_created_at
            && match (
                self.provisioning_operation_id.as_deref(),
                other.provisioning_operation_id.as_deref(),
            ) {
                (Some(left), Some(right)) => left == right,
                // Legacy rows without a provisioning operation have no
                // durable generation token; created_at is their immutable
                // generation fallback. Do not fold mutable Project.version
                // into the workspace marker.
                (None, None) => true,
                _ => false,
            }
    }

    fn same_repository(&self, other: &Self) -> bool {
        self.primary_repo_id == other.primary_repo_id
            && self.repository_identity == other.repository_identity
    }

    fn same_generation_and_repository(&self, other: &Self) -> bool {
        self.same_generation(other) && self.same_repository(other)
    }
}

type ProjectAgentWorkspaceAuthorityRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

async fn query_project_agent_workspace_authority<'e, E>(
    executor: E,
    project_id: &str,
) -> Result<Option<ProjectAgentWorkspaceAuthority>>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let row = sqlx::query_as::<_, ProjectAgentWorkspaceAuthorityRow>(
        "SELECT p.id, p.created_at, p.primary_repo_id,
                operation.id,
                repo.id, repo.remote_url, repo.local_path, repo.default_branch
         FROM project p
         LEFT JOIN project_provisioning_operation operation
           ON operation.project_id = p.id
         LEFT JOIN repo
           ON repo.id = p.primary_repo_id AND repo.project_id = p.id
         WHERE p.id = ?",
    )
    .bind(project_id)
    .fetch_optional(executor)
    .await?;

    Ok(row.map(
        |(
            project_id,
            project_created_at,
            primary_repo_id,
            provisioning_operation_id,
            repository_id,
            remote_url,
            local_path,
            default_branch,
        )| ProjectAgentWorkspaceAuthority {
            project_id,
            project_created_at,
            provisioning_operation_id,
            primary_repo_id,
            repository_identity: repository_id.zip(remote_url).zip(default_branch).map(
                |((repository_id, remote_url), default_branch)| {
                    (repository_id, remote_url, local_path, default_branch)
                },
            ),
        },
    ))
}

async fn capture_project_agent_workspace_authority(
    db: &SqliteDb,
    project_id: &str,
) -> Result<Option<(ProjectAgentWorkspaceAuthority, Option<db::Repo>)>> {
    let Some(authority) = query_project_agent_workspace_authority(db.pool(), project_id).await?
    else {
        return Ok(None);
    };
    let repo = match authority.primary_repo_id.as_deref() {
        Some(repo_id) => match RepoRepo::get_by_id(db, repo_id).await? {
            Some(repo) if repo.project_id == authority.project_id => Some(repo),
            Some(_) => {
                return Err(ServiceError::RepoMismatch {
                    project_id: authority.project_id.clone(),
                });
            }
            None => None,
        },
        None => None,
    };
    Ok(Some((authority, repo)))
}

async fn quarantine_project_agent_workspace_if_marker_differs(
    workspace: &Path,
    expected_marker: &str,
) -> Result<Option<PathBuf>> {
    let metadata = match tokio::fs::symlink_metadata(workspace).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(ServiceError::invalid_operation(format!(
                "inspect Project Agent workspace {}: {error}",
                workspace.display()
            )));
        }
    };
    if metadata.file_type().is_symlink() {
        return Err(ServiceError::invalid_operation(format!(
            "Project Agent workspace is a symlink: {}",
            workspace.display()
        )));
    }

    let marker_path = workspace.join(PROJECT_AGENT_GENERATION_MARKER);
    let marker_matches = if metadata.is_dir() {
        match tokio::fs::read_to_string(&marker_path).await {
            Ok(marker) => marker == expected_marker,
            // A markerless workspace predates generation stamping. The
            // current authority owns the path at this point, so adopt it and
            // stamp it rather than deleting durable Project Agent notes.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(ServiceError::invalid_operation(format!(
                    "read Project Agent workspace generation {}: {error}",
                    marker_path.display()
                )));
            }
        }
    } else {
        false
    };
    if marker_matches {
        return Ok(None);
    }

    let parent = workspace.parent().ok_or_else(|| {
        ServiceError::invalid_operation(format!(
            "Project Agent workspace has no parent: {}",
            workspace.display()
        ))
    })?;
    let name = workspace
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("project-agent");
    let quarantine = parent.join(format!(
        ".{name}.forge-project-agent-stale-{}",
        new_uuid_v4()
    ));
    tokio::fs::rename(workspace, &quarantine)
        .await
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "quarantine stale Project Agent workspace {}: {error}",
                workspace.display()
            ))
        })?;
    Ok(Some(quarantine))
}

async fn quarantine_project_agent_workspace_if_marker_matches(
    workspace: &Path,
    expected_marker: &str,
) -> Result<Option<PathBuf>> {
    let metadata = match tokio::fs::symlink_metadata(workspace).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(ServiceError::invalid_operation(format!(
                "inspect Project Agent workspace {}: {error}",
                workspace.display()
            )));
        }
    };
    if metadata.file_type().is_symlink() {
        return Err(ServiceError::invalid_operation(format!(
            "Project Agent workspace is a symlink: {}",
            workspace.display()
        )));
    }
    if !metadata.is_dir() {
        return Ok(None);
    }
    let marker =
        match tokio::fs::read_to_string(workspace.join(PROJECT_AGENT_GENERATION_MARKER)).await {
            Ok(marker) => marker,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(ServiceError::invalid_operation(format!(
                    "read Project Agent workspace authority {}: {error}",
                    workspace.display()
                )));
            }
        };
    if marker != expected_marker {
        return Ok(None);
    }
    let parent = workspace.parent().ok_or_else(|| {
        ServiceError::invalid_operation(format!(
            "Project Agent workspace has no parent: {}",
            workspace.display()
        ))
    })?;
    let name = workspace
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("project-agent");
    let quarantine = parent.join(format!(
        ".{name}.forge-project-agent-stale-{}",
        new_uuid_v4()
    ));
    tokio::fs::rename(workspace, &quarantine)
        .await
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "quarantine stale Project Agent workspace {}: {error}",
                workspace.display()
            ))
        })?;
    Ok(Some(quarantine))
}

/// Quarantine an unowned repository cache after the database check has proved
/// that no live Repo row still owns its exact repository ID. Caches do not
/// carry a Project-Agent generation marker, so this helper is intentionally
/// separate from marker-aware workspace adoption.
async fn quarantine_project_agent_cache(path: &Path) -> Result<Option<PathBuf>> {
    let metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(ServiceError::invalid_operation(format!(
                "inspect repository cache {}: {error}",
                path.display()
            )));
        }
    };
    if metadata.file_type().is_symlink() {
        return Ok(None);
    }
    let parent = path.parent().ok_or_else(|| {
        ServiceError::invalid_operation(format!(
            "repository cache has no parent: {}",
            path.display()
        ))
    })?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("repo-cache");
    let quarantine = parent.join(format!(
        ".{name}.forge-project-agent-stale-{}",
        new_uuid_v4()
    ));
    tokio::fs::rename(path, &quarantine)
        .await
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "quarantine repository cache {}: {error}",
                path.display()
            ))
        })?;
    Ok(Some(quarantine))
}

/// A primary repository can change within one Project generation. Preserve
/// the durable `forge/` notes, but discard only a checkout whose stamped
/// repository snapshot is no longer the current one. A markerless checkout
/// is disposable by definition: its repository origin cannot be proven, so
/// it is quarantined and rebuilt for the current repository.
async fn quarantine_project_agent_checkout_if_repository_differs(
    workspace: &Path,
    expected_repository_marker: &str,
) -> Result<Option<PathBuf>> {
    let checkout = workspace.join(PROJECT_AGENT_CHECKOUT_DIR);
    let metadata = match tokio::fs::symlink_metadata(&checkout).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(ServiceError::invalid_operation(format!(
                "inspect Project Agent checkout {}: {error}",
                checkout.display()
            )));
        }
    };
    if metadata.file_type().is_symlink() {
        return Err(ServiceError::invalid_operation(format!(
            "Project Agent checkout is a symlink: {}",
            checkout.display()
        )));
    }
    if !metadata.is_dir() {
        return Ok(None);
    }
    let repository_marker =
        match tokio::fs::read_to_string(workspace.join(PROJECT_AGENT_REPOSITORY_MARKER)).await {
            Ok(marker) => marker,
            // The workspace marker may be adopted from a legacy markerless
            // directory, but its checkout is disposable and unproven. Let
            // the mismatch path below quarantine it for a clean rebuild.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => {
                return Err(ServiceError::invalid_operation(format!(
                    "read Project Agent repository authority {}: {error}",
                    workspace.join(PROJECT_AGENT_REPOSITORY_MARKER).display()
                )));
            }
        };
    if repository_marker == expected_repository_marker {
        return Ok(None);
    }
    let parent = checkout.parent().ok_or_else(|| {
        ServiceError::invalid_operation(format!(
            "Project Agent checkout has no parent: {}",
            checkout.display()
        ))
    })?;
    let name = checkout
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("checkout");
    let quarantine = parent.join(format!(
        ".{name}.forge-project-agent-stale-{}",
        new_uuid_v4()
    ));
    tokio::fs::rename(&checkout, &quarantine)
        .await
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "quarantine stale Project Agent checkout {}: {error}",
                checkout.display()
            ))
        })?;
    Ok(Some(quarantine))
}

/// A stale creator may finish after a same-generation repository change has
/// already reserved and rebuilt the checkout. In that ordering it must only
/// quarantine a checkout still stamped with its own repository snapshot; a
/// differing marker belongs to the newer repository admission and is left
/// untouched.
async fn quarantine_project_agent_checkout_if_repository_matches(
    workspace: &Path,
    expected_repository_marker: &str,
) -> Result<Option<PathBuf>> {
    let checkout = workspace.join(PROJECT_AGENT_CHECKOUT_DIR);
    let metadata = match tokio::fs::symlink_metadata(&checkout).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(ServiceError::invalid_operation(format!(
                "inspect Project Agent checkout {}: {error}",
                checkout.display()
            )));
        }
    };
    if metadata.file_type().is_symlink() {
        return Err(ServiceError::invalid_operation(format!(
            "Project Agent checkout is a symlink: {}",
            checkout.display()
        )));
    }
    if !metadata.is_dir() {
        return Ok(None);
    }
    let repository_marker =
        match tokio::fs::read_to_string(workspace.join(PROJECT_AGENT_REPOSITORY_MARKER)).await {
            Ok(marker) => marker,
            // Markerless legacy checkouts have no provenance that permits a
            // destructive cleanup, so preserve them conservatively.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(ServiceError::invalid_operation(format!(
                    "read Project Agent repository authority {}: {error}",
                    workspace.join(PROJECT_AGENT_REPOSITORY_MARKER).display()
                )));
            }
        };
    if repository_marker != expected_repository_marker {
        return Ok(None);
    }
    let parent = checkout.parent().ok_or_else(|| {
        ServiceError::invalid_operation(format!(
            "Project Agent checkout has no parent: {}",
            checkout.display()
        ))
    })?;
    let name = checkout
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("checkout");
    let quarantine = parent.join(format!(
        ".{name}.forge-project-agent-stale-{}",
        new_uuid_v4()
    ));
    tokio::fs::rename(&checkout, &quarantine)
        .await
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "quarantine stale Project Agent checkout {}: {error}",
                checkout.display()
            ))
        })?;
    Ok(Some(quarantine))
}

async fn remove_quarantined_project_agent_path(path: &Path) -> Result<()> {
    let metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(ServiceError::invalid_operation(format!(
                "inspect Project Agent quarantine {}: {error}",
                path.display()
            )));
        }
    };
    let result = if metadata.is_dir() && !metadata.file_type().is_symlink() {
        tokio::fs::remove_dir_all(path).await
    } else {
        tokio::fs::remove_file(path).await
    };
    result.map_err(|error| {
        ServiceError::invalid_operation(format!(
            "remove Project Agent quarantine {}: {error}",
            path.display()
        ))
    })
}

/// Restore a path moved aside by a reservation when the DB admission cannot
/// commit. The original name is restored only while it is still free; a
/// concurrent creator's occupied path is never overwritten, and the unique
/// quarantine remains available for recovery.
async fn restore_project_agent_quarantine_if_path_free(
    original: &Path,
    quarantine: Option<PathBuf>,
) {
    let Some(quarantine) = quarantine else {
        return;
    };
    let original_is_free = match tokio::fs::symlink_metadata(original).await {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Ok(_) => false,
        Err(error) => {
            tracing::error!(
                original = %original.display(),
                quarantine = %quarantine.display(),
                error = %error,
                "failed to inspect Project Agent path after reservation rollback"
            );
            false
        }
    };
    if original_is_free {
        if let Err(error) = tokio::fs::rename(&quarantine, original).await {
            tracing::error!(
                original = %original.display(),
                quarantine = %quarantine.display(),
                error = %error,
                "failed to restore Project Agent path after reservation rollback"
            );
        }
    } else {
        tracing::error!(
            original = %original.display(),
            quarantine = %quarantine.display(),
            "preserving Project Agent quarantine because the original path is occupied"
        );
    }
}

/// Move a replacement workspace created after a whole-workspace quarantine
/// aside before restoring the original tree. The generation marker proves it
/// is the replacement this reservation attempted to stamp; a markerless
/// partial is also safe to move because the caller has already reserved the
/// original path under the DB writer lock.
async fn quarantine_partial_project_agent_workspace(
    workspace: &Path,
    expected_marker: &str,
) -> Result<Option<PathBuf>> {
    let metadata = match tokio::fs::symlink_metadata(workspace).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(ServiceError::invalid_operation(format!(
                "inspect partial Project Agent workspace {}: {error}",
                workspace.display()
            )));
        }
    };
    if metadata.file_type().is_symlink() {
        return Ok(None);
    }
    if !metadata.is_dir() {
        return Ok(None);
    }
    match tokio::fs::read_to_string(workspace.join(PROJECT_AGENT_GENERATION_MARKER)).await {
        Ok(marker) if marker != expected_marker => return Ok(None),
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(ServiceError::invalid_operation(format!(
                "read partial Project Agent workspace generation {}: {error}",
                workspace.join(PROJECT_AGENT_GENERATION_MARKER).display()
            )));
        }
    }
    let parent = workspace.parent().ok_or_else(|| {
        ServiceError::invalid_operation(format!(
            "partial Project Agent workspace has no parent: {}",
            workspace.display()
        ))
    })?;
    let name = workspace
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("project-agent");
    let quarantine = parent.join(format!(
        ".{name}.forge-project-agent-reservation-failed-{}",
        new_uuid_v4()
    ));
    tokio::fs::rename(workspace, &quarantine)
        .await
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "quarantine partial Project Agent workspace {}: {error}",
                workspace.display()
            ))
        })?;
    Ok(Some(quarantine))
}

async fn rollback_project_agent_reservation(
    workspace: &Path,
    expected_marker: &str,
    stale_workspace: Option<PathBuf>,
    stale_checkout: Option<PathBuf>,
) {
    let partial_workspace = if stale_workspace.is_some() {
        match quarantine_partial_project_agent_workspace(workspace, expected_marker).await {
            Ok(partial_workspace) => partial_workspace,
            Err(error) => {
                tracing::error!(
                    workspace = %workspace.display(),
                    error = %error,
                    "failed to quarantine partial Project Agent replacement during rollback"
                );
                None
            }
        }
    } else {
        None
    };
    restore_project_agent_quarantine_if_path_free(workspace, stale_workspace).await;
    restore_project_agent_quarantine_if_path_free(
        &workspace.join(PROJECT_AGENT_CHECKOUT_DIR),
        stale_checkout,
    )
    .await;
    if let Some(partial_workspace) = partial_workspace {
        // The reservation has not admitted this replacement. Keep the
        // quarantine for recovery rather than deleting bytes on a rollback.
        tracing::warn!(
            workspace = %workspace.display(),
            quarantine = %partial_workspace.display(),
            "preserved partial Project Agent replacement quarantine after reservation rollback"
        );
    }
}

async fn reserve_project_agent_workspace_path(
    db: &SqliteDb,
    authority: &ProjectAgentWorkspaceAuthority,
    workspace: &Path,
) -> Result<bool> {
    let mut transaction = db::begin_immediate(db.pool()).await?;
    let Some(current) =
        query_project_agent_workspace_authority(&mut *transaction, &authority.project_id).await?
    else {
        transaction.commit().await?;
        return Ok(false);
    };
    if !authority.same_generation_and_repository(&current) {
        transaction.commit().await?;
        return Ok(false);
    }

    let expected_generation_marker = authority.generation_marker_contents();
    let stale_workspace = quarantine_project_agent_workspace_if_marker_differs(
        workspace,
        &expected_generation_marker,
    )
    .await?;
    let stale_checkout = if stale_workspace.is_none() {
        quarantine_project_agent_checkout_if_repository_differs(
            workspace,
            &authority.repository_marker_contents(),
        )
        .await?
    } else {
        None
    };
    if let Err(error) = tokio::fs::create_dir_all(workspace).await {
        drop(transaction);
        rollback_project_agent_reservation(
            workspace,
            &expected_generation_marker,
            stale_workspace,
            stale_checkout,
        )
        .await;
        return Err(ServiceError::Git(git::GitError::Io(error)));
    }
    if let Err(error) = tokio::fs::write(
        workspace.join(PROJECT_AGENT_GENERATION_MARKER),
        &expected_generation_marker,
    )
    .await
    {
        drop(transaction);
        rollback_project_agent_reservation(
            workspace,
            &expected_generation_marker,
            stale_workspace,
            stale_checkout,
        )
        .await;
        return Err(ServiceError::Git(git::GitError::Io(error)));
    }
    if let Err(error) = tokio::fs::write(
        workspace.join(PROJECT_AGENT_REPOSITORY_MARKER),
        authority.repository_marker_contents(),
    )
    .await
    {
        drop(transaction);
        rollback_project_agent_reservation(
            workspace,
            &expected_generation_marker,
            stale_workspace,
            stale_checkout,
        )
        .await;
        return Err(ServiceError::Git(git::GitError::Io(error)));
    }
    if let Err(error) = transaction.commit().await {
        rollback_project_agent_reservation(
            workspace,
            &expected_generation_marker,
            stale_workspace,
            stale_checkout,
        )
        .await;
        return Err(error.into());
    }

    for stale in [stale_workspace, stale_checkout].into_iter().flatten() {
        remove_quarantined_project_agent_path(&stale).await?;
    }
    Ok(true)
}

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
        .map_err(|error| ServiceError::Git(git::GitError::Io(error)))?;
        let output = Command::new("git")
            .args(["clone", &repo.remote_url, &clone_path.to_string_lossy()])
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .output()
            .await
            .map_err(|error| ServiceError::Git(git::GitError::Io(error)))?;
        if !output.status.success() {
            return Err(ServiceError::Git(git::GitError::CommandFailed {
                command: format!("git clone {} {}", repo.remote_url, clone_path.display()),
                stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            }));
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
    #[test]
    fn verification_workspace_boundary_is_written_once_and_names_the_checkout() {
        let root = std::env::temp_dir().join(format!(
            "forge-verify-boundary-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_nanos())
                .unwrap_or_default()
        ));
        std::fs::create_dir_all(&root).expect("workspace root");
        super::write_verification_workspace_boundary(&root).expect("boundary written");
        let manifest = std::fs::read_to_string(root.join("Cargo.toml")).expect("manifest");
        assert!(manifest.contains("[workspace]"));
        assert!(manifest.contains("members = [\"checkout\"]"));
        // An existing manifest is the operator's; it is never overwritten.
        std::fs::write(root.join("Cargo.toml"), "[workspace]\nmembers = []\n").expect("rewrite");
        super::write_verification_workspace_boundary(&root).expect("idempotent");
        assert_eq!(
            std::fs::read_to_string(root.join("Cargo.toml")).expect("manifest"),
            "[workspace]\nmembers = []\n"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

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

    #[tokio::test]
    async fn project_agent_generation_reuse_quarantines_old_workspace_without_touching_replacement()
    {
        let db = sqlite_db().await;
        let root = TempDir::new().expect("workspace root creates");
        let project_id = new_uuid_v4();
        let old_created_at = now_rfc3339();
        ProjectRepo::create(
            &db,
            CreateProject {
                id: project_id.clone(),
                name: "old generation".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: old_created_at.clone(),
                updated_at: old_created_at,
            },
        )
        .await
        .expect("old Project creates");
        let (old_authority, _) = capture_project_agent_workspace_authority(&db, &project_id)
            .await
            .expect("old authority query")
            .expect("old authority exists");
        let workspace = root.path().join(&project_id);
        assert!(
            reserve_project_agent_workspace_path(&db, &old_authority, &workspace)
                .await
                .expect("old workspace reserves")
        );
        std::fs::create_dir_all(workspace.join(PROJECT_AGENT_DOCS_DIR))
            .expect("old notes directory creates");
        std::fs::create_dir_all(workspace.join(PROJECT_AGENT_CHECKOUT_DIR))
            .expect("old checkout directory creates");
        std::fs::write(
            workspace.join(PROJECT_AGENT_DOCS_DIR).join("old-note"),
            "old generation",
        )
        .expect("old note writes");
        std::fs::write(
            workspace.join(PROJECT_AGENT_CHECKOUT_DIR).join("old-file"),
            "old generation",
        )
        .expect("old checkout writes");

        ProjectRepo::delete(&db, &project_id)
            .await
            .expect("old Project deletes");
        // Project IDs are normally generated afresh. This low-level fixture
        // deliberately models an explicit ID-reuse operation after the old
        // creation event has been archived by the caller.
        sqlx::query("DELETE FROM domain_event WHERE dedupe_key = ?")
            .bind(format!("project-created:{project_id}"))
            .execute(db.pool())
            .await
            .expect("old creation event archives");
        let replacement_created_at = now_rfc3339();
        ProjectRepo::create(
            &db,
            CreateProject {
                id: project_id.clone(),
                name: "replacement generation".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: replacement_created_at,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .expect("replacement Project creates with reused ID");
        let (replacement_authority, _) =
            capture_project_agent_workspace_authority(&db, &project_id)
                .await
                .expect("replacement authority query")
                .expect("replacement authority exists");
        assert_ne!(
            old_authority.provisioning_operation_id,
            replacement_authority.provisioning_operation_id,
            "Project ID reuse must create a new immutable generation"
        );

        // This is the late-admission ordering: the replacement reserves the
        // shared Project-ID path before the stale creator gets its final
        // retention check. Reservation quarantines the old tree, stamps the
        // replacement marker, and only then permits replacement data.
        assert!(
            reserve_project_agent_workspace_path(&db, &replacement_authority, &workspace)
                .await
                .expect("replacement workspace reserves")
        );
        std::fs::create_dir_all(workspace.join(PROJECT_AGENT_DOCS_DIR))
            .expect("replacement notes directory creates");
        std::fs::create_dir_all(workspace.join(PROJECT_AGENT_CHECKOUT_DIR))
            .expect("replacement checkout directory creates");
        std::fs::write(
            workspace
                .join(PROJECT_AGENT_DOCS_DIR)
                .join("replacement-note"),
            "replacement generation",
        )
        .expect("replacement note writes");
        std::fs::write(
            workspace
                .join(PROJECT_AGENT_CHECKOUT_DIR)
                .join("replacement-file"),
            "replacement generation",
        )
        .expect("replacement checkout writes");

        assert!(retain_project_agent_workspace_if_current(
            &db,
            &old_authority,
            workspace.clone(),
            None,
        )
        .await
        .expect("stale retention check succeeds")
        .is_none());
        assert!(!workspace
            .join(PROJECT_AGENT_DOCS_DIR)
            .join("old-note")
            .exists());
        assert!(!workspace
            .join(PROJECT_AGENT_CHECKOUT_DIR)
            .join("old-file")
            .exists());
        assert_eq!(
            std::fs::read_to_string(
                workspace
                    .join(PROJECT_AGENT_DOCS_DIR)
                    .join("replacement-note")
            )
            .expect("replacement note remains"),
            "replacement generation"
        );
        assert_eq!(
            std::fs::read_to_string(
                workspace
                    .join(PROJECT_AGENT_CHECKOUT_DIR)
                    .join("replacement-file")
            )
            .expect("replacement checkout remains"),
            "replacement generation"
        );
        assert_eq!(
            std::fs::read_to_string(workspace.join(PROJECT_AGENT_GENERATION_MARKER))
                .expect("replacement marker remains"),
            replacement_authority.generation_marker_contents()
        );
    }

    #[tokio::test]
    async fn project_agent_adopts_markerless_workspace_without_losing_legacy_notes() {
        let db = sqlite_db().await;
        let root = TempDir::new().expect("workspace root creates");
        let project_id = new_uuid_v4();
        let now = now_rfc3339();
        ProjectRepo::create(
            &db,
            CreateProject {
                id: project_id.clone(),
                name: "legacy workspace".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("Project creates");
        let (authority, _) = capture_project_agent_workspace_authority(&db, &project_id)
            .await
            .expect("authority query")
            .expect("authority exists");
        let workspace = root.path().join(&project_id);
        std::fs::create_dir_all(workspace.join(PROJECT_AGENT_DOCS_DIR))
            .expect("legacy notes directory creates");
        std::fs::write(
            workspace.join(PROJECT_AGENT_DOCS_DIR).join("legacy-note"),
            "must survive upgrade",
        )
        .expect("legacy note writes");
        let legacy_checkout = workspace.join(PROJECT_AGENT_CHECKOUT_DIR);
        std::fs::create_dir_all(&legacy_checkout).expect("legacy checkout directory creates");
        std::fs::write(legacy_checkout.join("legacy-file"), "must be rebuilt")
            .expect("legacy checkout writes");

        assert!(
            reserve_project_agent_workspace_path(&db, &authority, &workspace)
                .await
                .expect("legacy workspace adopts")
        );
        assert_eq!(
            std::fs::read_to_string(workspace.join(PROJECT_AGENT_DOCS_DIR).join("legacy-note"))
                .expect("legacy note remains"),
            "must survive upgrade"
        );
        assert_eq!(
            std::fs::read_to_string(workspace.join(PROJECT_AGENT_GENERATION_MARKER))
                .expect("generation marker stamped"),
            authority.generation_marker_contents()
        );
        assert!(
            !legacy_checkout.exists(),
            "markerless disposable checkout is not adopted with durable notes"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn project_agent_reservation_failure_restores_quarantined_checkout() {
        use std::os::unix::fs::PermissionsExt;

        let db = sqlite_db().await;
        let root = TempDir::new().expect("workspace root creates");
        let project_id = new_uuid_v4();
        let now = now_rfc3339();
        ProjectRepo::create(
            &db,
            CreateProject {
                id: project_id.clone(),
                name: "reservation rollback".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("Project creates");
        let (authority, _) = capture_project_agent_workspace_authority(&db, &project_id)
            .await
            .expect("authority query")
            .expect("authority exists");
        let workspace = root.path().join(&project_id);
        assert!(
            reserve_project_agent_workspace_path(&db, &authority, &workspace)
                .await
                .expect("initial workspace reserves")
        );
        let checkout = workspace.join(PROJECT_AGENT_CHECKOUT_DIR);
        std::fs::create_dir_all(&checkout).expect("checkout directory creates");
        std::fs::write(checkout.join("old-file"), "old checkout").expect("old checkout writes");
        std::fs::write(
            workspace.join(PROJECT_AGENT_REPOSITORY_MARKER),
            "old-repository-marker",
        )
        .expect("old repository marker writes");
        let marker = workspace.join(PROJECT_AGENT_REPOSITORY_MARKER);
        let mut permissions = std::fs::metadata(&marker)
            .expect("repository marker metadata")
            .permissions();
        permissions.set_mode(0o444);
        std::fs::set_permissions(&marker, permissions).expect("repository marker locks");

        let error = reserve_project_agent_workspace_path(&db, &authority, &workspace)
            .await
            .expect_err("locked repository marker rejects reservation");
        assert!(error.to_string().contains("Permission denied"));
        assert_eq!(
            std::fs::read_to_string(checkout.join("old-file"))
                .expect("old checkout restores after failure"),
            "old checkout"
        );

        let mut permissions = std::fs::metadata(&marker)
            .expect("repository marker metadata after failure")
            .permissions();
        permissions.set_mode(0o644);
        std::fs::set_permissions(&marker, permissions).expect("repository marker unlocks");
    }

    #[tokio::test]
    async fn project_agent_whole_workspace_rollback_restores_old_tree() {
        let root = TempDir::new().expect("workspace root creates");
        let workspace = root.path().join("project");
        let old_marker = "old-generation";
        let new_marker = "new-generation";
        std::fs::create_dir_all(workspace.join(PROJECT_AGENT_DOCS_DIR))
            .expect("old notes directory creates");
        std::fs::write(workspace.join(PROJECT_AGENT_GENERATION_MARKER), old_marker)
            .expect("old generation marker writes");
        std::fs::write(
            workspace.join(PROJECT_AGENT_DOCS_DIR).join("old-note"),
            "old durable note",
        )
        .expect("old note writes");

        let stale_workspace =
            quarantine_project_agent_workspace_if_marker_differs(&workspace, new_marker)
                .await
                .expect("old workspace quarantines")
                .expect("old workspace quarantine exists");
        std::fs::create_dir_all(workspace.join(PROJECT_AGENT_DOCS_DIR))
            .expect("partial notes directory creates");
        std::fs::write(workspace.join(PROJECT_AGENT_GENERATION_MARKER), new_marker)
            .expect("partial generation marker writes");
        std::fs::write(
            workspace.join(PROJECT_AGENT_DOCS_DIR).join("partial-note"),
            "partial replacement",
        )
        .expect("partial note writes");

        rollback_project_agent_reservation(&workspace, new_marker, Some(stale_workspace), None)
            .await;
        assert_eq!(
            std::fs::read_to_string(workspace.join(PROJECT_AGENT_DOCS_DIR).join("old-note"))
                .expect("old note restores"),
            "old durable note"
        );
        assert!(!workspace
            .join(PROJECT_AGENT_DOCS_DIR)
            .join("partial-note")
            .exists());
        assert_eq!(
            std::fs::read_to_string(workspace.join(PROJECT_AGENT_GENERATION_MARKER))
                .expect("old marker restores"),
            old_marker
        );
        assert!(
            std::fs::read_dir(root.path())
                .expect("workspace parent reads")
                .filter_map(|entry| entry.ok())
                .any(|entry| entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".project.forge-project-agent-reservation-failed-")),
            "partial replacement remains quarantined for recovery"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn project_agent_rejects_symlink_checkout_without_following() {
        use std::os::unix::fs::symlink;

        let db = sqlite_db().await;
        let root = TempDir::new().expect("workspace root creates");
        let project_id = new_uuid_v4();
        let now = now_rfc3339();
        ProjectRepo::create(
            &db,
            CreateProject {
                id: project_id.clone(),
                name: "symlink checkout".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("Project creates");
        let (authority, _) = capture_project_agent_workspace_authority(&db, &project_id)
            .await
            .expect("authority query")
            .expect("authority exists");
        let workspace = root.path().join(&project_id);
        assert!(
            reserve_project_agent_workspace_path(&db, &authority, &workspace)
                .await
                .expect("initial workspace reserves")
        );
        let external = root.path().join("external-checkout");
        std::fs::create_dir_all(&external).expect("external directory creates");
        std::fs::write(external.join("sentinel"), "must remain").expect("external sentinel writes");
        symlink(&external, workspace.join(PROJECT_AGENT_CHECKOUT_DIR))
            .expect("checkout symlink creates");

        let error = reserve_project_agent_workspace_path(&db, &authority, &workspace)
            .await
            .expect_err("symlink checkout rejects reservation");
        assert!(error.to_string().contains("checkout is a symlink"));
        assert_eq!(
            std::fs::read_to_string(external.join("sentinel"))
                .expect("external target remains untouched"),
            "must remain"
        );
    }

    #[tokio::test]
    async fn project_agent_version_and_repo_changes_preserve_notes_but_replace_checkout() {
        let db = sqlite_db().await;
        let root = TempDir::new().expect("workspace root creates");
        let project_id = new_uuid_v4();
        let repo_one_id = new_uuid_v4();
        let repo_two_id = new_uuid_v4();
        let now = now_rfc3339();
        ProjectRepo::create(
            &db,
            CreateProject {
                id: project_id.clone(),
                name: "mutable generation".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("Project creates");
        RepoRepo::create(
            &db,
            CreateRepo {
                id: repo_one_id.clone(),
                project_id: project_id.clone(),
                name: "first repository".to_owned(),
                remote_url: "https://example.invalid/first.git".to_owned(),
                local_path: None,
                work_mode: db::WorkMode::DirectMerge,
                default_branch: "main".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("first repository creates");
        ProjectRepo::update_at_version(
            &db,
            UpdateProject {
                id: project_id.clone(),
                name: None,
                settings: None,
                primary_repo_id: Some(Some(repo_one_id.clone())),
                paused_at: None,
                updated_at: now_rfc3339(),
            },
            ProjectRepo::get_by_id(&db, &project_id)
                .await
                .expect("Project lookup")
                .expect("Project exists")
                .version,
            None,
        )
        .await
        .expect("first repository binds");
        let (authority_one, _) = capture_project_agent_workspace_authority(&db, &project_id)
            .await
            .expect("first authority query")
            .expect("first authority exists");
        let workspace = root.path().join(&project_id);
        assert!(
            reserve_project_agent_workspace_path(&db, &authority_one, &workspace)
                .await
                .expect("first workspace reserves")
        );
        let first_repository_marker =
            std::fs::read_to_string(workspace.join(PROJECT_AGENT_REPOSITORY_MARKER))
                .expect("first repository marker is stamped");
        assert_eq!(first_repository_marker.len(), 64);
        assert!(!first_repository_marker.contains("example.invalid"));
        assert!(!first_repository_marker.contains("first.git"));
        assert!(!first_repository_marker.contains(&repo_one_id));
        assert_eq!(
            std::fs::read_to_string(workspace.join(PROJECT_AGENT_GENERATION_MARKER))
                .expect("first generation marker is stamped"),
            authority_one.generation_marker_contents()
        );
        std::fs::create_dir_all(workspace.join(PROJECT_AGENT_DOCS_DIR))
            .expect("notes directory creates");
        std::fs::create_dir_all(workspace.join(PROJECT_AGENT_CHECKOUT_DIR))
            .expect("checkout directory creates");
        std::fs::write(
            workspace.join(PROJECT_AGENT_DOCS_DIR).join("note"),
            "keep this",
        )
        .expect("note writes");
        std::fs::write(
            workspace.join(PROJECT_AGENT_CHECKOUT_DIR).join("old-tree"),
            "replace this",
        )
        .expect("old checkout writes");

        let version_before_edit = ProjectRepo::get_by_id(&db, &project_id)
            .await
            .expect("Project lookup")
            .expect("Project exists")
            .version;
        ProjectRepo::update_at_version(
            &db,
            UpdateProject {
                id: project_id.clone(),
                name: Some("renamed generation".to_owned()),
                settings: None,
                primary_repo_id: None,
                paused_at: None,
                updated_at: now_rfc3339(),
            },
            version_before_edit,
            None,
        )
        .await
        .expect("Project version bumps");
        let (authority_after_version, _) =
            capture_project_agent_workspace_authority(&db, &project_id)
                .await
                .expect("version authority query")
                .expect("version authority exists");
        assert_eq!(
            authority_one.provisioning_operation_id,
            authority_after_version.provisioning_operation_id
        );
        assert!(
            reserve_project_agent_workspace_path(&db, &authority_after_version, &workspace)
                .await
                .expect("version-bumped workspace reserves")
        );
        assert_eq!(
            std::fs::read_to_string(workspace.join(PROJECT_AGENT_GENERATION_MARKER))
                .expect("generation marker survives version bump"),
            authority_one.generation_marker_contents()
        );
        assert!(workspace.join(PROJECT_AGENT_DOCS_DIR).join("note").exists());
        assert!(workspace
            .join(PROJECT_AGENT_CHECKOUT_DIR)
            .join("old-tree")
            .exists());

        RepoRepo::create(
            &db,
            CreateRepo {
                id: repo_two_id.clone(),
                project_id: project_id.clone(),
                name: "second repository".to_owned(),
                remote_url: "https://example.invalid/second.git".to_owned(),
                local_path: None,
                work_mode: db::WorkMode::DirectMerge,
                default_branch: "main".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("second repository creates");
        let version_before_repo_change = ProjectRepo::get_by_id(&db, &project_id)
            .await
            .expect("Project lookup")
            .expect("Project exists")
            .version;
        ProjectRepo::update_at_version(
            &db,
            UpdateProject {
                id: project_id.clone(),
                name: None,
                settings: None,
                primary_repo_id: Some(Some(repo_two_id.clone())),
                paused_at: None,
                updated_at: now_rfc3339(),
            },
            version_before_repo_change,
            None,
        )
        .await
        .expect("second repository binds");
        let (authority_two, _) = capture_project_agent_workspace_authority(&db, &project_id)
            .await
            .expect("second authority query")
            .expect("second authority exists");
        assert!(
            reserve_project_agent_workspace_path(&db, &authority_two, &workspace)
                .await
                .expect("repository-changed workspace reserves")
        );
        let second_repository_marker =
            std::fs::read_to_string(workspace.join(PROJECT_AGENT_REPOSITORY_MARKER))
                .expect("second repository marker is stamped");
        assert_ne!(first_repository_marker, second_repository_marker);
        assert_eq!(second_repository_marker.len(), 64);
        assert!(!second_repository_marker.contains("example.invalid"));
        assert!(!second_repository_marker.contains("second.git"));
        assert!(!second_repository_marker.contains(&repo_two_id));
        assert!(workspace.join(PROJECT_AGENT_DOCS_DIR).join("note").exists());
        assert!(
            !workspace
                .join(PROJECT_AGENT_CHECKOUT_DIR)
                .join("old-tree")
                .exists(),
            "repository changes replace only the disposable checkout"
        );
        // The newer admission may finish building its replacement checkout
        // before an older creator gets its final retention check. The stale
        // finalizer must not mistake the newer repository marker for its own
        // checkout and remove the replacement.
        std::fs::create_dir_all(workspace.join(PROJECT_AGENT_CHECKOUT_DIR))
            .expect("replacement checkout directory creates");
        std::fs::write(
            workspace
                .join(PROJECT_AGENT_CHECKOUT_DIR)
                .join("replacement-tree"),
            "new repository",
        )
        .expect("replacement checkout writes");
        assert!(retain_project_agent_workspace_if_current(
            &db,
            &authority_one,
            workspace.clone(),
            None,
        )
        .await
        .expect("stale repository retention check succeeds")
        .is_none());
        assert_eq!(
            std::fs::read_to_string(
                workspace
                    .join(PROJECT_AGENT_CHECKOUT_DIR)
                    .join("replacement-tree")
            )
            .expect("replacement checkout remains"),
            "new repository"
        );
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
