//! Daemon and server workspace resets.

use super::*;

pub(in crate::task_service) async fn reset_daemon_workspace(
    db: &SqliteDb,
    workspace: &Workspace,
    resolved: &crate::workspace_backend::ResolvedWorkspace,
) -> Result<Workspace> {
    use crate::workspace_backend::{PreparedWorkspace, ResetSpec, WorkspaceBackendError};
    let placement = &resolved.placement;
    let running: bool = db
        .owner_workspace_has_running_execution(&workspace.id)
        .await?;
    if running {
        return Err(ServiceError::invalid_operation(
            "cannot reset an owner workspace with a running execution",
        ));
    }
    let base_sha = workspace
        .before_sha
        .as_deref()
        .ok_or_else(|| ServiceError::invalid_operation("workspace has no recorded reset base"))?;
    let generation = placement
        .generation
        .checked_add(1)
        .ok_or_else(|| ServiceError::invalid_operation("placement generation exhausted"))?;
    // A receipt can survive the owner reset while the placement CAS did
    // not. Apply that result before issuing any read with the old fence.
    let retained = db
        .retained_owner_workspace_reset_outcome(&placement.id, generation)
        .await?;
    let prepared = if let Some(retained) = retained {
        let retained: serde_json::Value = serde_json::from_str(&retained).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid reset receipt: {error}"))
        })?;
        let result: api_types::WorkspaceResetResult =
            serde_json::from_value(retained["owner_result"].clone()).map_err(|error| {
                ServiceError::invalid_operation(format!("invalid reset result: {error}"))
            })?;
        if i64::try_from(result.workspace.generation).ok() != Some(generation) {
            return Err(WorkspaceBackendError::StaleGeneration {
                placement_id: placement.id.clone(),
                expected: generation,
                actual: i64::try_from(result.workspace.generation).unwrap_or(i64::MAX),
            }
            .into());
        }
        PreparedWorkspace {
            handle: result.workspace.workspace_handle,
            base_sha: result.workspace.base_sha,
            branch: result.workspace.branch,
        }
    } else {
        let state = resolved.backend.describe(placement).await?;
        let head_sha = if state.exists {
            state
                .head_sha
                .filter(|sha| !sha.is_empty())
                .ok_or_else(|| ServiceError::invalid_operation("owner returned no reset HEAD"))?
        } else {
            base_sha.to_owned()
        };
        let mut next = placement.clone();
        next.generation = generation;
        resolved
            .backend
            .reset(
                &next,
                &ResetSpec {
                    expected_head_sha: head_sha,
                    base_ref: base_sha.to_owned(),
                },
            )
            .await?
    };
    if prepared.handle.is_empty() || prepared.base_sha.is_empty() || prepared.branch.is_empty() {
        return Err(ServiceError::invalid_operation(
            "owner returned an incomplete reset workspace",
        ));
    }
    let mut transaction = db::begin_immediate(db.pool()).await?;
    let running: bool = db
        .owner_workspace_has_running_execution_in_tx(&mut transaction, &workspace.id)
        .await?;
    if running {
        return Err(db::DbError::VersionConflict.into());
    }
    let mut update = crate::placement::admission::placement_update(placement);
    update.generation = Some(generation);
    update.workspace_handle = Some(Some(prepared.handle));
    update.state = Some(db::PlacementState::Ready);
    update.disconnected_at = Some(None);
    update.failure_cause = Some(None);
    db::WorkspacePlacementRepo::update_in_tx(db, &mut transaction, update).await?;
    db.record_owner_workspace_reset_in_tx(
        &mut transaction,
        prepared.base_sha,
        prepared.branch,
        now_rfc3339(),
        &workspace.id,
    )
    .await?;
    crate::placement::admission::resolve_workspace_attention_in_tx(
        db,
        &mut transaction,
        &placement.task_id,
    )
    .await?;
    transaction.commit().await?;
    WorkspaceRepo::get_by_id(db, &workspace.id)
        .await?
        .ok_or_else(|| ServiceError::not_found("workspace", &workspace.id))
}

pub(in crate::task_service) async fn reset_workspace(
    db: &SqliteDb,
    workspace_root: &std::path::Path,
    task: &Task,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
    router: &crate::workspace_backend::WorkspaceBackendRouter,
) -> Result<Workspace> {
    if task.parent_task_id.is_some() {
        return Err(ServiceError::invalid_operation(
            "subtask workspaces are shared with the coordination root; reset the root workspace instead",
        ));
    }
    let authority = resolve_task_repository_authority(db, task).await?;
    let mut daemon_reset = None;
    if let Some(workspace) = WorkspaceRepo::get_by_task_id(db, &task.id).await? {
        let resolved = resolve_workspace_backend(db, workspace_root, &workspace, router).await?;
        let owner_removed = match resolved.placement.daemon_id.as_deref() {
            Some(daemon_id) if resolved.placement.owner_kind == PlacementOwnerKind::Daemon => {
                db.daemon_removed(daemon_id).await?
            }
            _ => false,
        };
        if owner_removed {
            // Already released by machine removal: the next admission selects
            // another machine and prepares the replacement there.
            daemon_reset = Some(workspace.clone());
        } else if resolved.placement.owner_kind == PlacementOwnerKind::Daemon {
            if workspace.repo_id != authority.repo.id {
                return Err(ServiceError::invalid_operation(
                    "cannot reset a daemon placement onto a different repository",
                ));
            }
            daemon_reset = Some(reset_daemon_workspace(db, &workspace, &resolved).await?);
        } else {
            // Cleanup follows the attempt-pinned Workspace repository. The fresh
            // replacement below follows the Project's current primary Repo.
            let repo = RepoRepo::get_by_id(db, &workspace.repo_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("repo", workspace.repo_id.clone()))?;
            let worktree_path = resolved.embedded_path()?;
            let repo_source = resolve_repo_source(&repo, workspace_root).await?;
            if worktree_path.exists() {
                let _ = resolved.backend.cleanup(&resolved.placement).await;
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

    crate::placement::admission::resolve_workspace_attention(db, &task.id).await?;
    if let Some(workspace) = daemon_reset {
        return Ok(workspace);
    }

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
