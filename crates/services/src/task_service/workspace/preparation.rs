//! Task workspace creation and persistence.

use super::*;

pub(crate) async fn prepare_workspace(
    db: &SqliteDb,
    workspace_root: &Path,
    task: &Task,
    task_id: &str,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
    router: &crate::workspace_backend::WorkspaceBackendRouter,
) -> Result<Workspace> {
    prepare_workspace_for(
        db,
        workspace_root,
        task,
        task_id,
        repo_cache_locks,
        router,
        Purpose::Execute,
    )
    .await
}

/// Create or validate the Task's workspace for `purpose`. Creation is one of
/// the repairs; an existing row is checked by the workspace manager.
pub(crate) async fn prepare_workspace_for(
    db: &SqliteDb,
    workspace_root: &Path,
    task: &Task,
    task_id: &str,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
    router: &crate::workspace_backend::WorkspaceBackendRouter,
    purpose: Purpose,
) -> Result<Workspace> {
    Ok(prepare_workspace_owned(
        db,
        workspace_root,
        task,
        task_id,
        repo_cache_locks,
        router,
        purpose,
    )
    .await?
    .0)
}

/// The validated workspace for an integration caller (delivery, rebase).
///
/// Every check and repair lives in
/// [`crate::workspace_manager::WorkspaceManager::ensure_valid`]; this entry
/// point remains for the merge and rebase owners, which pass whether the row
/// is forgotten when its Task branch is gone.
pub(crate) async fn ensure_valid(
    db: &SqliteDb,
    workspace_root: &Path,
    task: &Task,
    workspace: Workspace,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
    delete_missing_workspace: bool,
    router: &crate::workspace_backend::WorkspaceBackendRouter,
) -> Result<Workspace> {
    ensure_valid_for(
        db,
        workspace_root,
        task,
        workspace,
        repo_cache_locks,
        delete_missing_workspace,
        router,
        Purpose::Integrate,
    )
    .await
}

/// Whether the callers that prepare a workspace for `purpose` turn a
/// worktree that cannot be made again under the disk floor into the Task's
/// disk capacity wait: a claim (`Execute`), a before-work hook and the
/// review entry's check request (`Check`, whose hooks step suspends on the
/// wait) do. So does admission to the integration queue (`Integrate`): the
/// `run_merge` hook suspends on the wait like any other hook, and the
/// reconciler, the importer's continuation and the owner's Retry state the
/// Task's disk capacity wait and ask again at its recheck. No worktree is
/// recreated under the floor.
pub(super) fn waits_for_disk(purpose: Purpose) -> bool {
    matches!(
        purpose,
        Purpose::Execute | Purpose::Hook | Purpose::Check | Purpose::Integrate
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn ensure_valid_for(
    db: &SqliteDb,
    workspace_root: &Path,
    task: &Task,
    workspace: Workspace,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
    delete_missing_workspace: bool,
    router: &crate::workspace_backend::WorkspaceBackendRouter,
    purpose: Purpose,
) -> Result<Workspace> {
    let manager = crate::workspace_manager::WorkspaceManager::new(
        db,
        workspace_root,
        repo_cache_locks,
        router,
    );
    let manager = if waits_for_disk(purpose) {
        manager.waiting_for_disk()
    } else {
        manager
    };
    let valid = if delete_missing_workspace {
        manager
            .ensure_valid_or_forget(task, workspace, purpose)
            .await
    } else {
        manager.ensure_valid(task, workspace, purpose).await
    };
    let valid = valid?;
    if valid.repair() != crate::workspace_manager::Repair::None {
        info!(
            task_id = %task.id,
            workspace_id = %valid.workspace().id,
            placement_id = %valid.resolved().placement.id,
            purpose = ?purpose,
            repair = ?valid.repair(),
            rescued_ref = valid.rescued_ref().unwrap_or(""),
            "workspace repaired before use"
        );
    }
    Ok(valid.into_workspace())
}

pub(super) async fn ensure_workspace_repository_current(
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

pub(super) async fn create_fresh_workspace(
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
            if let Err(cleanup_error) = manager
                .cleanup_worktree(
                    task_id,
                    Path::new(&worktree_source),
                    &workspace_root.join(task_id).join(&repo.name),
                )
                .await
            {
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
    let workspace = match persist_prepared_workspace(
        db,
        repo,
        &worktree_source,
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
                if let Err(cleanup_error) = manager
                    .cleanup_worktree(
                        task_id,
                        Path::new(&worktree_source),
                        &workspace_root.join(task_id).join(&repo.name),
                    )
                    .await
                {
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
            return Err(error);
        }
    };

    info!(
        task_id = task_id,
        workspace_id = %workspace.id,
        repo_id = repo_id,
        workspace_root = %workspace_root.display(),
        worktree_path = %worktree_path.display(),
        branch = %workspace.branch,
        "workspace created"
    );

    Ok((workspace, true))
}

pub(super) async fn server_repo_location_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    repo: &db::Repo,
    source: &str,
    now: &str,
) -> db::Result<String> {
    let kind = if repo.local_path.as_deref().map(str::trim) == Some(source) {
        RepoLocationKind::PrimaryCheckout
    } else {
        RepoLocationKind::ManagedClone
    };
    let existing = db::SqliteDb::prepared_server_repo_location_in_tx(
        transaction,
        &repo.id,
        kind.to_string(),
        source,
    )
    .await?;
    if let Some(row) = existing {
        let id: String = row.id?;
        let status: String = row.status?;
        if status != "ready" {
            let version: i64 = row.version?;
            let result = db::SqliteDb::mark_prepared_server_repo_location_ready_in_tx(
                transaction,
                now,
                now,
                &id,
                version,
            )
            .await?;
            if result == 0 {
                return Err(DbError::VersionConflict);
            }
        }
        return Ok(id);
    }

    let id = new_uuid_v4();
    db::SqliteDb::insert_prepared_server_repo_location_in_tx(
        transaction,
        db::InsertPreparedServerRepoLocationInTxInput {
            id: &id,
            repo_id: &repo.id,
            source,
            kind: kind.to_string(),
            is_default: kind == RepoLocationKind::PrimaryCheckout,
            last_verified_at: now,
            created_at: now,
            updated_at: now,
        },
    )
    .await?;
    Ok(id)
}

fn server_placement(
    workspace: &CreateWorkspace,
    repo_location_id: String,
) -> CreateWorkspacePlacement {
    CreateWorkspacePlacement {
        id: new_uuid_v4(),
        workspace_id: workspace.id.clone(),
        task_id: workspace.task_id.clone(),
        agent_id: None,
        owner_kind: PlacementOwnerKind::Server,
        daemon_id: None,
        runtime_id: None,
        repo_location_id,
        execution_daemon_id: None,
        workspace_handle: Some(workspace.worktree_path.clone()),
        generation: 1,
        state: match workspace.status {
            WorkspaceStatus::Creating => PlacementState::Preparing,
            WorkspaceStatus::Ready => PlacementState::Ready,
            WorkspaceStatus::Error => PlacementState::Failed,
            WorkspaceStatus::Cleaning => PlacementState::Cleaning,
            WorkspaceStatus::Cleaned => PlacementState::Cleaned,
        },
        selected_by: PlacementSelectedBy::Scheduler,
        selection_reason: json!({ "rule": "server_default" }).to_string(),
        reserved_until: None,
        disconnected_at: None,
        failure_cause: None,
        created_at: workspace.created_at.clone(),
        updated_at: workspace.updated_at.clone(),
    }
}

/// The workspace and its server placement become visible together. The writer
/// lock also serializes first-use managed-clone location registration.
pub(super) async fn persist_prepared_workspace(
    db: &SqliteDb,
    repo: &db::Repo,
    source: &str,
    input: CreateWorkspace,
) -> Result<Workspace> {
    let mut transaction = db::begin_immediate(db.pool()).await?;
    db.insert_prepared_server_workspace_in_tx(
        &mut transaction,
        db::InsertPreparedServerWorkspaceInTxInput {
            id: &input.id,
            task_id: &input.task_id,
            repo_id: &input.repo_id,
            worktree_path: &input.worktree_path,
            branch: &input.branch,
            status: input.status.to_string(),
            before_sha: input.before_sha.as_deref(),
            created_at: &input.created_at,
            updated_at: &input.updated_at,
        },
    )
    .await?;
    let location_id =
        server_repo_location_in_tx(&mut transaction, repo, source, &input.updated_at).await?;
    WorkspacePlacementRepo::create_in_tx(
        db,
        &mut transaction,
        server_placement(&input, location_id),
    )
    .await?;
    transaction.commit().await?;
    WorkspaceRepo::get_by_id(db, &input.id)
        .await?
        .ok_or_else(|| ServiceError::not_found("workspace", input.id))
}

pub(super) async fn persist_recovered_workspace(
    db: &SqliteDb,
    repo: &db::Repo,
    source: &str,
    workspace: &Workspace,
    worktree_path: &Path,
    now: &str,
) -> Result<()> {
    let mut transaction = db::begin_immediate(db.pool()).await?;
    let placement =
        WorkspacePlacementRepo::get_by_workspace_id_in_tx(db, &mut transaction, &workspace.id)
            .await?;
    if let Some(placement) = placement {
        if placement.owner_kind != PlacementOwnerKind::Server {
            return Err(
                crate::workspace_backend::WorkspaceBackendError::OwnerUnsupported {
                    owner_kind: placement.owner_kind,
                }
                .into(),
            );
        }
        WorkspacePlacementRepo::update_in_tx(
            db,
            &mut transaction,
            UpdateWorkspacePlacement {
                id: placement.id,
                expected_version: placement.version,
                agent_id: None,
                owner_kind: None,
                daemon_id: None,
                runtime_id: None,
                repo_location_id: None,
                execution_daemon_id: None,
                workspace_handle: Some(Some(worktree_path.to_string_lossy().into_owned())),
                generation: Some(placement.generation + 1),
                state: Some(PlacementState::Ready),
                selected_by: None,
                selection_reason: None,
                reserved_until: Some(None),
                disconnected_at: Some(None),
                failure_cause: Some(None),
                updated_at: now.to_owned(),
            },
        )
        .await?;
    } else {
        // Migration skips already-cleaned workspaces. Rebuilding one restores
        // its placement in the same transaction as the ready Workspace row.
        let location_id = server_repo_location_in_tx(&mut transaction, repo, source, now).await?;
        let input = CreateWorkspace {
            id: workspace.id.clone(),
            task_id: workspace.task_id.clone(),
            repo_id: workspace.repo_id.clone(),
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch: workspace.branch.clone(),
            status: WorkspaceStatus::Ready,
            before_sha: workspace.before_sha.clone(),
            created_at: workspace.created_at.clone(),
            updated_at: now.to_owned(),
        };
        WorkspacePlacementRepo::create_in_tx(
            db,
            &mut transaction,
            server_placement(&input, location_id),
        )
        .await?;
    }
    let result = db
        .mark_server_workspace_ready_in_tx(
            &mut transaction,
            worktree_path.to_string_lossy().as_ref(),
            now,
            &workspace.id,
        )
        .await?;
    if result == 0 {
        return Err(DbError::NotFound.into());
    }
    transaction.commit().await?;
    Ok(())
}
