//! Worktree readiness probes and missing-worktree repair.

use super::*;

/// Recreate the worktree from its surviving Task branch. `foreign` is the
/// workspace manager's finding that a usable Git directory at the recorded
/// path belongs to another repository: it is moved aside, never relinked.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn recover_missing_worktree(
    db: &SqliteDb,
    workspace_root: &std::path::Path,
    repo: &db::Repo,
    task_id: &str,
    workspace: Workspace,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
    delete_missing_workspace: bool,
    router: &crate::workspace_backend::WorkspaceBackendRouter,
    foreign: bool,
    disk_pressure: Option<api_types::DiskPressureKind>,
) -> Result<Workspace> {
    let resolved = resolve_workspace_backend(db, workspace_root, &workspace, router).await?;
    let existing_path = resolved.embedded_path()?;
    let readiness = match worktree_readiness(&existing_path).await? {
        WorktreeReadiness::Ready if foreign => WorktreeReadiness::Invalid,
        WorktreeReadiness::Ready => return Ok(workspace),
        readiness => readiness,
    };
    warn!(
        task_id = task_id,
        workspace_id = %workspace.id,
        worktree_path = %existing_path.display(),
        readiness = ?readiness,
        "workspace worktree path missing or unusable, attempting recovery"
    );

    let repo_source = resolve_repo_source(repo, workspace_root).await?;

    if !Path::new(&repo_source).exists() {
        return Err(ServiceError::invalid_operation(format!(
            "repo source path does not exist: {repo_source}"
        )));
    }

    // `git worktree repair` writes the directory's `.git` link even when it
    // then fails, which turns a directory that is not a worktree into one
    // Git accepts (every tracked file "deleted"). That is harmless when the
    // directory is moved aside right after; a refusal below must leave the
    // directory as it was found.
    let dot_git = existing_path.join(".git");
    let had_dot_git = tokio::fs::symlink_metadata(&dot_git).await.is_ok();
    if matches!(readiness, WorktreeReadiness::Invalid)
        && !foreign
        && try_repair_worktree_gitdir(Path::new(&repo_source), &existing_path).await
    {
        info!(
            task_id = task_id,
            workspace_id = %workspace.id,
            worktree_path = %existing_path.display(),
            "workspace gitdir repaired"
        );
        return Ok(workspace);
    }

    let branch = &workspace.branch;
    let branch_exists = recovery_branch_exists(Path::new(&repo_source), branch).await?;

    if branch_exists {
        // From here the tree is checked out again (an unusable one is moved
        // aside first, not deleted): a whole checkout of new disk. Under the
        // floor nothing is touched and the caller waits for disk, as it
        // does for a new worktree. The relink above writes two small files
        // and is never refused.
        if disk_pressure.is_some() {
            if !had_dot_git
                && tokio::fs::symlink_metadata(&dot_git)
                    .await
                    .is_ok_and(|metadata| metadata.file_type().is_file())
            {
                let _ = tokio::fs::remove_file(&dot_git).await;
            }
            return Err(recreation_refused_for_disk(db, task_id, &workspace).await?);
        }
        if !matches!(readiness, WorktreeReadiness::Missing) {
            move_unusable_worktree_aside(&existing_path).await?;
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
        persist_recovered_workspace(db, repo, &repo_source, &workspace, &worktree_path, &now)
            .await?;

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
            // Delivery and child admission checks keep the persisted row so
            // the required reset remains an explicit owner operation.
            warn!(
                task_id = task_id,
                workspace_id = %workspace.id,
                "task branch is gone; preserving workspace row for explicit reset"
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

pub(crate) async fn worktree_readiness(worktree_path: &Path) -> Result<WorktreeReadiness> {
    if !tokio::fs::try_exists(worktree_path)
        .await
        .map_err(git::GitError::Io)?
    {
        return Ok(WorktreeReadiness::Missing);
    }
    if !tokio::fs::try_exists(worktree_path.join(".git"))
        .await
        .map_err(git::GitError::Io)?
    {
        return Ok(WorktreeReadiness::Invalid);
    }
    match probe_worktree_head(worktree_path).await {
        Ok(_) => Ok(WorktreeReadiness::Ready),
        Err(error) if git_error_reports_unusable_worktree(&error) => Ok(WorktreeReadiness::Invalid),
        Err(error) => Err(error.into()),
    }
}

fn git_error_reports_unusable_worktree(error: &git::GitError) -> bool {
    let git::GitError::CommandFailed { stdout, stderr, .. } = error else {
        return false;
    };
    [stdout, stderr].iter().any(|output| {
        let output = output.to_ascii_lowercase();
        output.contains("not a git repository") || output.contains("invalid gitfile format")
    })
}

async fn probe_worktree_head(worktree_path: &Path) -> git::Result<String> {
    #[cfg(test)]
    if let Some(failure) = take_injected_worktree_probe_failure(worktree_path) {
        return Err(match failure {
            InjectedWorktreeProbeFailure::Io => {
                git::GitError::Io(std::io::Error::other("injected worktree probe failure"))
            }
            InjectedWorktreeProbeFailure::NotRepository => git::GitError::CommandFailed {
                command: "git rev-parse HEAD".to_owned(),
                stdout: String::new(),
                stderr: "fatal: not a git repository".to_owned(),
            },
        });
    }
    git::get_current_sha(worktree_path).await
}

pub(super) async fn recovery_branch_exists(repo_source: &Path, branch: &str) -> git::Result<bool> {
    #[cfg(test)]
    if take_injected_branch_lookup_failure(repo_source) {
        return Err(git::GitError::Io(std::io::Error::other(
            "injected branch lookup failure",
        )));
    }
    git::branch_exists(repo_source, branch).await
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
