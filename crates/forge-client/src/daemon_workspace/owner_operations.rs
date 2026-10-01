use super::*;

impl DaemonWorkspaceBackend {
    pub(super) async fn owner_operation(
        &self,
        params: WorkspaceOwnerOperationParams,
        active_ids: &[String],
    ) -> CommandResult<WorkspaceOwnerOperationResult> {
        let mut owned =
            self.workspace(&reference(&params.fence, &params.workspace_handle), false)?;
        ensure_inactive(&owned, active_ids)?;
        if !matches!(
            &params.operation,
            WorkspaceOwnerOperation::ReleaseReviewCheckout
        ) {
            owned = self
                .live_workspace(&reference(&params.fence, &params.workspace_handle))
                .await?;
        }
        if !owned.cleaned {
            self.check_expected(&owned, &params.fence.expected).await?;
        }
        let outcome = match params.operation {
            WorkspaceOwnerOperation::MaterializeAssets { environment } => {
                self.materialize_assets(&owned.path, &environment).await?;
                WorkspaceOwnerOperationOutcome::Applied
            }
            WorkspaceOwnerOperation::ReviewCheckout {
                commit_sha,
                environment,
                prepare,
            } => {
                if prepare {
                    let handle = self
                        .review_checkout(
                            &params.fence,
                            &params.workspace_handle,
                            &owned,
                            &commit_sha,
                            &environment,
                        )
                        .await?;
                    WorkspaceOwnerOperationOutcome::ReviewCheckout {
                        workspace_handle: handle,
                    }
                } else {
                    WorkspaceOwnerOperationOutcome::ReviewCheckout {
                        workspace_handle: params.workspace_handle.clone(),
                    }
                }
            }
            WorkspaceOwnerOperation::ReleaseReviewCheckout => {
                if owned.review_parent.is_none() {
                    return Err(error(
                        INVALID_INPUT,
                        "only a detached review checkout may be released",
                    ));
                }
                self.reclaim_review_checkouts(&owned, active_ids).await?;
                self.remove_owned_workspace(&params.workspace_handle, &owned)
                    .await?;
                return Ok(WorkspaceOwnerOperationResult {
                    entry_id: operation_entry_id(&params.fence.operation_id),
                    operation_id: params.fence.operation_id,
                    outcome: WorkspaceOwnerOperationOutcome::Applied,
                });
            }
            WorkspaceOwnerOperation::RestoreCandidate { commit_sha } => {
                let sha = resolve_commit(&owned.path, &commit_sha).await?;
                git::restore_worktree(&owned.path, &sha)
                    .await
                    .map_err(git_error)?;
                WorkspaceOwnerOperationOutcome::Applied
            }
            WorkspaceOwnerOperation::WriteKnowledge {
                files,
                create_new,
                commit_task_id,
                repository,
            } => {
                let root = if repository {
                    self.location_path(&owned.repo_location_id).await?.1
                } else {
                    owned.path.clone()
                };
                let mut writes = Vec::new();
                for file in files {
                    let relative = Path::new(&file.path);
                    if !(relative.starts_with("docs/knowledge")
                        || relative == Path::new(".forge/knowledge-context.md"))
                    {
                        return Err(error(INVALID_INPUT, "invalid knowledge file path"));
                    }
                    let path = self.relative_path(&root, &file.path)?;
                    writes.push((path, file.bytes));
                }
                for (path, bytes) in writes {
                    if create_new && path.exists() {
                        continue;
                    }
                    if let Some(parent) = path.parent() {
                        tokio::fs::create_dir_all(parent).await.map_err(io_error)?;
                    }
                    if create_new {
                        use tokio::io::AsyncWriteExt;
                        match tokio::fs::OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .open(path)
                            .await
                        {
                            Ok(mut file) => file.write_all(&bytes).await.map_err(io_error)?,
                            Err(failure) if failure.kind() == std::io::ErrorKind::AlreadyExists => {
                            }
                            Err(failure) => return Err(io_error(failure)),
                        }
                    } else {
                        tokio::fs::write(path, bytes).await.map_err(io_error)?;
                    }
                }
                if let Some(task_id) = commit_task_id {
                    // Capture's commit is best effort, as on the server owner.
                    if local_git(&root, &["add", "--", "docs/knowledge/"])
                        .await
                        .is_ok()
                    {
                        let _ = local_git(
                            &root,
                            &[
                                "commit",
                                "-m",
                                &format!("docs: capture knowledge from task {task_id}"),
                            ],
                        )
                        .await;
                    }
                }
                WorkspaceOwnerOperationOutcome::Applied
            }
            WorkspaceOwnerOperation::RebaseTarget {
                target_branch,
                handoff_conflicts,
            } => {
                validate_branch(&owned.path, &target_branch).await?;
                let target =
                    resolve_commit(&owned.path, &format!("refs/heads/{target_branch}")).await?;
                if !git::is_worktree_clean(&owned.path)
                    .await
                    .map_err(git_error)?
                {
                    WorkspaceOwnerOperationOutcome::Dirty {
                        files: git::status_porcelain(&owned.path)
                            .await
                            .map_err(git_error)?,
                    }
                } else {
                    match git::rebase(&owned.path, &target).await {
                        Ok(()) => WorkspaceOwnerOperationOutcome::Rebased,
                        Err(git::GitError::MergeConflict { stderr, .. }) => {
                            if !handoff_conflicts {
                                git::abort_rebase(&owned.path).await.map_err(git_error)?;
                                WorkspaceOwnerOperationOutcome::Conflict {
                                    details: stderr,
                                    conflict_paths: Vec::new(),
                                }
                            } else {
                                match git::continue_rebase_keeping_conflicts(&owned.path).await {
                                    Ok(conflict_paths) => {
                                        WorkspaceOwnerOperationOutcome::Conflict {
                                            details: stderr,
                                            conflict_paths,
                                        }
                                    }
                                    Err(git::GitError::UnsupportedRebaseConflict { details }) => {
                                        WorkspaceOwnerOperationOutcome::UnsupportedConflict {
                                            details,
                                        }
                                    }
                                    Err(failure) => return Err(git_error(failure)),
                                }
                            }
                        }
                        Err(failure) => return Err(git_error(failure)),
                    }
                }
            }
        };
        owned.version += 1;
        self.save_workspace(&params.workspace_handle, owned)?;
        Ok(WorkspaceOwnerOperationResult {
            entry_id: operation_entry_id(&params.fence.operation_id),
            operation_id: params.fence.operation_id,
            outcome,
        })
    }

    async fn materialize_assets(
        &self,
        path: &Path,
        environment: &ProjectEnvironment,
    ) -> CommandResult<()> {
        executors::environment::validate_project_environment(environment)
            .map_err(|failure| error(INVALID_INPUT, failure))?;
        for asset in &environment.assets {
            self.confined_path(Path::new(&asset.source))?;
            self.relative_path(path, &asset.target)?;
        }
        executors::environment::materialize_assets(path, &environment.assets)
            .await
            .map_err(|failure| error(WORKSPACE_ERROR, failure))
    }

    async fn review_checkout(
        &self,
        fence: &WorkspaceMutationFence,
        parent_handle: &str,
        parent: &OwnedWorkspace,
        commit_sha: &str,
        environment: &ProjectEnvironment,
    ) -> CommandResult<String> {
        let sha = resolve_commit(&parent.path, commit_sha).await?;
        let existing = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .handles
            .iter()
            .find(|(_, owned)| {
                owned.review_parent.as_deref() == Some(parent_handle)
                    && owned.review_operation_id.as_deref() == Some(&fence.operation_id)
            })
            .map(|(handle, owned)| (handle.clone(), owned.clone()));
        let (handle, mut owned) = match existing {
            Some((handle, owned)) => (handle, owned),
            None => {
                let handle = format!("workspace-{}", uuid::Uuid::new_v4());
                let owned = OwnedWorkspace {
                    daemon_id: parent.daemon_id.clone(),
                    runtime_id: parent.runtime_id.clone(),
                    placement_id: parent.placement_id.clone(),
                    repo_location_id: parent.repo_location_id.clone(),
                    path: self.path_for_handle(&handle)?,
                    branch: String::new(),
                    base_sha: sha.clone(),
                    generation: parent.generation,
                    version: 0,
                    prepared: false,
                    cleaned: false,
                    execution_ids: Vec::new(),
                    review_parent: Some(parent_handle.into()),
                    review_operation_id: Some(fence.operation_id.clone()),
                };
                self.save_workspace(&handle, owned.clone())?;
                (handle, owned)
            }
        };
        self.workspace(&reference(fence, &handle), false)?;
        if owned.cleaned || owned.base_sha != sha {
            return Err(error(
                INVALID_INPUT,
                "review checkout no longer belongs to this intent",
            ));
        }
        if !owned.path.exists() {
            let (_, repo) = self.location_path(&parent.repo_location_id).await?;
            self.manager
                .create_detached_worktree_named(
                    repo.to_str()
                        .ok_or_else(|| error(INVALID_INPUT, "repository path is not UTF-8"))?,
                    &handle,
                    "repo",
                    &sha,
                )
                .await
                .map_err(workspace_error)?;
        }
        verify_git_dir(&owned.path, &self.workspace_root).await?;
        if resolve_commit(&owned.path, "HEAD").await? != sha {
            return Err(error(VERSION_CONFLICT, "detached review checkout changed"));
        }
        self.materialize_assets(&owned.path, environment).await?;
        owned.prepared = true;
        owned.version += 1;
        self.save_workspace(&handle, owned)?;
        Ok(handle)
    }

    pub(super) async fn reclaim_review_checkouts(
        &self,
        parent: &OwnedWorkspace,
        active_ids: &[String],
    ) -> CommandResult<()> {
        // A root cleanup also reclaims review worktrees left by an interrupted
        // assessment, including detached checkouts created from other scratches.
        if parent.review_parent.is_some() {
            return Ok(());
        }
        let mut children: Vec<_> = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .handles
            .iter()
            .filter(|(_, owned)| {
                owned.placement_id == parent.placement_id
                    && owned.review_parent.is_some()
                    && !owned.cleaned
            })
            .map(|(handle, owned)| (handle.clone(), owned.clone()))
            .collect();
        children.sort_by(|left, right| left.0.cmp(&right.0));
        for (handle, child) in &children {
            self.workspace(
                &WorkspaceHandleReference {
                    daemon_id: child.daemon_id.clone(),
                    runtime_id: child.runtime_id.clone(),
                    placement_id: child.placement_id.clone(),
                    workspace_handle: handle.clone(),
                    generation: child.generation,
                },
                false,
            )?;
            ensure_inactive(child, active_ids)?;
        }
        for (handle, child) in children {
            self.remove_owned_workspace(&handle, &child).await?;
        }
        Ok(())
    }

    async fn remove_owned_workspace(
        &self,
        handle: &str,
        owned: &OwnedWorkspace,
    ) -> CommandResult<()> {
        if owned.cleaned {
            return Ok(());
        }
        let (_, repo) = self.location_path(&owned.repo_location_id).await?;
        self.confined_path(&owned.path)?;
        if owned.path.exists() {
            verify_git_dir(&owned.path, &self.workspace_root).await?;
            git::remove_worktree(&repo, &owned.path)
                .await
                .map_err(git_error)?;
        }
        match self
            .manager
            .cleanup_worktree(handle, &repo, &owned.path)
            .await
        {
            Ok(()) | Err(workspace::WorkspaceError::NotFound) => {}
            Err(failure) => return Err(workspace_error(failure)),
        }
        let mut owned = owned.clone();
        owned.cleaned = true;
        owned.version += 1;
        self.save_workspace(handle, owned)
    }
}
