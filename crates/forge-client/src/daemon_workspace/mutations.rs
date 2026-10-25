//! Reviewed and transferred workspace merges, reset, and cleanup operations.

use super::*;

impl DaemonWorkspaceBackend {
    pub(super) async fn merge(
        &self,
        reviewed: WorkspaceReviewedMergeParams,
    ) -> CommandResult<WorkspaceMergeResult> {
        let params = reviewed.merge;
        let mut owned = self
            .live_workspace(&reference(&params.fence, &params.workspace_handle))
            .await?;
        let (location, target) = self.location_path(&params.repo_location_id).await?;
        if location.kind != DaemonRepoLocationKind::PrimaryCheckout {
            return Err(error(
                INVALID_INPUT,
                "workspace.merge requires a verified primary_checkout",
            ));
        }
        let (_, source) = self.location_path(&owned.repo_location_id).await?;
        if git_common_dir(&source).await? != git_common_dir(&target).await? {
            return Err(error(
                INVALID_INPUT,
                "merge target is a different repository",
            ));
        }
        validate_branch(&target, &params.target_branch).await?;
        let outcome = if !git::is_worktree_clean(&owned.path)
            .await
            .map_err(git_error)?
        {
            WorkspaceMergeOutcome::Dirty {
                files: git::status_porcelain(&owned.path)
                    .await
                    .map_err(git_error)?,
            }
        } else if !git::is_worktree_clean(&target).await.map_err(git_error)? {
            WorkspaceMergeOutcome::TargetDirty {
                files: git::status_porcelain(&target).await.map_err(git_error)?,
            }
        } else {
            self.merge_clean(
                &owned,
                &target,
                &params,
                reviewed.reviewed_commit_sha.as_deref(),
            )
            .await?
        };
        let diffstat = if let WorkspaceMergeOutcome::Done {
            before_sha,
            after_sha,
            ..
        } = &outcome
        {
            Some(merge_diffstat(&target, before_sha, after_sha).await?)
        } else {
            None
        };
        owned.version += 1;
        self.save_workspace(&params.workspace_handle, owned)?;
        Ok(WorkspaceMergeResult {
            entry_id: operation_entry_id(&params.fence.operation_id),
            operation_id: params.fence.operation_id,
            outcome,
            diffstat,
        })
    }

    /// The fast-forward of the default checkout to a candidate held by no
    /// worktree of this owner. Admission and `verify_integration_objects`
    /// bound the candidate to this claim generation's outbound transfer ref
    /// and the target to the tip the claim read; both are read again here,
    /// under the checkout lock.
    pub(super) async fn merge_transferred(
        &self,
        reviewed: WorkspaceReviewedMergeParams,
    ) -> CommandResult<WorkspaceMergeResult> {
        let params = reviewed.merge;
        let (location, target) = self.location_path(&params.repo_location_id).await?;
        if location.kind != DaemonRepoLocationKind::PrimaryCheckout {
            return Err(error(
                INVALID_INPUT,
                "workspace.merge requires a verified primary_checkout",
            ));
        }
        validate_branch(&target, &params.target_branch).await?;
        let (WorkspaceOperationExpected::BaseSha { sha: head }, Some(key)) =
            (&params.fence.expected, &reviewed.candidate_transfer_key)
        else {
            return Err(error(
                INVALID_INPUT,
                "a transferred fast-forward names its candidate commit and transfer key",
            ));
        };
        let transferred = git::integration::transfer_ref(key)
            .map_err(|_| error(INVALID_INPUT, "invalid candidate transfer key"))?;
        let outcome = if reviewed.reviewed_commit_sha.as_deref() != Some(head.as_str())
            || !resolve_commit(&target, &transferred)
                .await
                .is_ok_and(|bound| &bound == head)
        {
            WorkspaceMergeOutcome::ReviewRequired {
                reason: "the transferred candidate is not the reviewed commit".into(),
            }
        } else if !git::is_worktree_clean(&target).await.map_err(git_error)? {
            WorkspaceMergeOutcome::TargetDirty {
                files: git::status_porcelain(&target).await.map_err(git_error)?,
            }
        } else {
            let target_sha =
                resolve_commit(&target, &format!("refs/heads/{}", params.target_branch)).await?;
            if local_git(&target, &["merge-base", "--is-ancestor", head, &target_sha])
                .await
                .is_ok()
            {
                WorkspaceMergeOutcome::Done {
                    before_sha: params.expected_target_sha.clone(),
                    after_sha: head.clone(),
                    branch: params.target_branch.clone(),
                }
            } else if target_sha != params.expected_target_sha {
                WorkspaceMergeOutcome::TargetMoved {
                    reason: "integration target changed since the rebase".into(),
                    target_branch: params.target_branch.clone(),
                }
            } else {
                // A compare-and-swap of the branch from the witnessed tip
                // to the candidate, which must contain that tip.
                match git::integration::fast_forward_exact(
                    &target,
                    &params.target_branch,
                    head,
                    &params.expected_target_sha,
                    git::integration::FastForwardLimits {
                        deadline: Duration::from_secs(30),
                        output_bytes: MAX_DIFF_BYTES,
                    },
                )
                .await
                .map_err(git_error)?
                {
                    git::integration::MergeApplyOutcome::Applied => WorkspaceMergeOutcome::Done {
                        before_sha: params.expected_target_sha.clone(),
                        after_sha: git::get_current_sha(&target).await.map_err(git_error)?,
                        branch: params.target_branch.clone(),
                    },
                    git::integration::MergeApplyOutcome::ReviewRequired { reason } => {
                        WorkspaceMergeOutcome::ReviewRequired { reason }
                    }
                    git::integration::MergeApplyOutcome::TargetMoved
                    | git::integration::MergeApplyOutcome::ExactObjectMismatch => {
                        WorkspaceMergeOutcome::TargetMoved {
                            reason: "integration target changed during the fast-forward".into(),
                            target_branch: params.target_branch.clone(),
                        }
                    }
                    git::integration::MergeApplyOutcome::ManualFailed(error) => {
                        return Err(git_error(error))
                    }
                }
            }
        };
        let diffstat = if let WorkspaceMergeOutcome::Done {
            before_sha,
            after_sha,
            ..
        } = &outcome
        {
            Some(merge_diffstat(&target, before_sha, after_sha).await?)
        } else {
            None
        };
        Ok(WorkspaceMergeResult {
            entry_id: operation_entry_id(&params.fence.operation_id),
            operation_id: params.fence.operation_id,
            outcome,
            diffstat,
        })
    }

    async fn merge_clean(
        &self,
        owned: &OwnedWorkspace,
        target: &Path,
        params: &WorkspaceMergeParams,
        reviewed_commit_sha: Option<&str>,
    ) -> CommandResult<WorkspaceMergeOutcome> {
        let head = git::get_current_sha(&owned.path).await.map_err(git_error)?;
        if let Some(reviewed) = reviewed_commit_sha {
            if reviewed != head
                || !matches!(&params.fence.expected, WorkspaceOperationExpected::BaseSha { sha } if sha == reviewed)
            {
                return Ok(WorkspaceMergeOutcome::ReviewRequired {
                    reason: "reviewed commit changed since review; fresh review required".into(),
                });
            }
        }
        match &params.fence.expected {
            WorkspaceOperationExpected::BaseSha { sha } if sha != &head => {
                return Ok(WorkspaceMergeOutcome::ReviewRequired {
                    reason: "reviewed commit changed since review; fresh review required".into(),
                })
            }
            _ => self.check_expected(owned, &params.fence.expected).await?,
        }
        let target_sha =
            resolve_commit(target, &format!("refs/heads/{}", params.target_branch)).await?;
        if local_git(target, &["merge-base", "--is-ancestor", &head, &target_sha])
            .await
            .is_ok()
        {
            return Ok(WorkspaceMergeOutcome::Done {
                before_sha: params.expected_target_sha.clone(),
                after_sha: head,
                branch: params.target_branch.clone(),
            });
        }
        if target_sha != params.expected_target_sha {
            return Ok(WorkspaceMergeOutcome::TargetMoved {
                reason: "integration target changed since review".into(),
                target_branch: params.target_branch.clone(),
            });
        }
        if !params.handed_off_paths.is_empty() {
            let paths =
                git::paths_adding_conflict_markers(&owned.path, &params.target_branch, "HEAD")
                    .await
                    .map_err(git_error)?
                    .into_iter()
                    .filter(|path| params.handed_off_paths.contains(path))
                    .collect::<Vec<_>>();
            if !paths.is_empty() {
                return Ok(WorkspaceMergeOutcome::UnresolvedConflictMarkers { paths });
            }
        }
        let before_sha = params.expected_target_sha.clone();
        match git::integration::apply_merge(target, &params.target_branch, &head, reviewed_commit_sha.is_some(), false, Some(&params.expected_target_sha), git::integration::FastForwardLimits { deadline: Duration::from_secs(30), output_bytes: MAX_DIFF_BYTES }).await.map_err(git_error)? {
            git::integration::MergeApplyOutcome::Applied => Ok(WorkspaceMergeOutcome::Done {
                before_sha, after_sha: git::get_current_sha(target).await.map_err(git_error)?, branch: params.target_branch.clone(),
            }),
            git::integration::MergeApplyOutcome::ReviewRequired { reason } => {
                let reason = if let Some(stderr) = reason.strip_prefix("git evidence unavailable: ") { format!("git merge --ff-only {head} failed: {stderr}") } else { reason };
                Ok(WorkspaceMergeOutcome::ReviewRequired { reason })
            },
            git::integration::MergeApplyOutcome::TargetMoved => Ok(WorkspaceMergeOutcome::TargetMoved { reason: "integration target changed during checkout".into(), target_branch: params.target_branch.clone() }),
            git::integration::MergeApplyOutcome::ExactObjectMismatch => Ok(WorkspaceMergeOutcome::TargetMoved { reason: "integration target changed during merge; reviewed content was not integrated".into(), target_branch: params.target_branch.clone() }),
            git::integration::MergeApplyOutcome::ManualFailed(git::GitError::MergeConflict { stderr, .. }) => {
                let conflict_paths = git::conflict_paths(target).await.map_err(git_error)?;
                git::abort_merge(target).await.map_err(git_error)?;
                Ok(WorkspaceMergeOutcome::Conflict { details: stderr, conflict_paths })
            }
            git::integration::MergeApplyOutcome::ManualFailed(error) => Err(git_error(error)),
        }
    }

    pub(super) async fn reset(
        &self,
        params: WorkspaceResetParams,
        active_ids: &[String],
        resuming: bool,
    ) -> CommandResult<WorkspaceResetResult> {
        let mut owned =
            self.workspace(&reference(&params.fence, &params.workspace_handle), true)?;
        ensure_inactive(&owned, active_ids)?;
        if owned.review_parent.is_some() {
            return Err(error(
                INVALID_INPUT,
                "detached review checkouts cannot advance a placement generation",
            ));
        }
        let (_, repo_path) = self.location_path(&owned.repo_location_id).await?;
        let base_sha = resolve_commit(&repo_path, &params.base_ref).await?;
        validate_branch(&repo_path, &params.branch).await?;
        if resuming
            && owned.generation == params.fence.generation
            && owned.base_sha == base_sha
            && owned.branch == params.branch
            && owned.prepared
            && !owned.cleaned
        {
            // Finishing a reset intent whose registry was already persisted.
        } else {
            if params.fence.generation <= owned.generation {
                return Err(error(
                    STALE_GENERATION,
                    "reset must advance the placement generation",
                ));
            }
            self.check_expected(&owned, &params.fence.expected).await?;
            self.reclaim_review_checkouts(&owned, active_ids, &params.fence.operation_id)
                .await?;
            if owned.path.exists() {
                verify_git_dir(&owned.path, &self.workspace_root).await?;
                self.manager
                    .reset_worktree(&params.workspace_handle, "repo")
                    .await
                    .map_err(workspace_error)?;
            } else {
                self.manager
                    .create_detached_worktree_named(
                        repo_path
                            .to_str()
                            .ok_or_else(|| error(INVALID_INPUT, "repository path is not UTF-8"))?,
                        &params.workspace_handle,
                        "repo",
                        &base_sha,
                    )
                    .await
                    .map_err(workspace_error)?;
            }
            confined_existing(&owned.path, &self.workspace_root)?;
            local_git(&owned.path, &["checkout", "-B", &params.branch, &base_sha]).await?;
            git::restore_worktree(&owned.path, &base_sha)
                .await
                .map_err(git_error)?;
            owned.base_sha = base_sha;
            owned.branch = params.branch;
            owned.generation = params.fence.generation;
            owned.prepared = true;
            owned.cleaned = false;
            owned.retired_by_operation_id = None;
            owned.version += 1;
            self.save_workspace(&params.workspace_handle, owned.clone())?;
        }
        Ok(WorkspaceResetResult {
            entry_id: operation_entry_id(&params.fence.operation_id),
            operation_id: params.fence.operation_id,
            workspace: prepared_state(&params.workspace_handle, &owned),
        })
    }

    pub(super) async fn cleanup(
        &self,
        params: WorkspaceCleanupParams,
        active_ids: &[String],
    ) -> CommandResult<WorkspaceCleanupResult> {
        let mut owned =
            self.workspace(&reference(&params.fence, &params.workspace_handle), false)?;
        ensure_inactive(&owned, active_ids)?;
        let (_, repo_path) = self.location_path(&owned.repo_location_id).await?;
        if !owned.cleaned {
            self.check_expected(&owned, &params.fence.expected).await?;
            self.reclaim_review_checkouts(&owned, active_ids, &params.fence.operation_id)
                .await?;
            if owned.path.exists() {
                verify_git_dir(&owned.path, &self.workspace_root).await?;
                git::remove_worktree(&repo_path, &owned.path)
                    .await
                    .map_err(git_error)?;
            }
            self.confined_path(
                owned
                    .path
                    .parent()
                    .ok_or_else(|| error(OUTSIDE_WORKSPACE_ROOT, "workspace has no parent"))?,
            )?;
            match self
                .manager
                .cleanup_worktree(&params.workspace_handle, &repo_path, &owned.path)
                .await
            {
                Ok(()) | Err(workspace::WorkspaceError::NotFound) => {}
                Err(error) => return Err(workspace_error(error)),
            }
            owned.cleaned = true;
            owned.retired_by_operation_id = Some(params.fence.operation_id.clone());
            owned.version += 1;
            self.save_workspace(&params.workspace_handle, owned.clone())?;
        }
        Ok(WorkspaceCleanupResult {
            entry_id: operation_entry_id(&params.fence.operation_id),
            operation_id: params.fence.operation_id,
            workspace_handle: params.workspace_handle,
            generation: owned.generation,
            cleaned: true,
        })
    }
}
