use super::*;

impl DaemonWorkspaceBackend {
    pub(super) async fn reconcile(
        &self,
        params: WorkspaceReconcileParams,
    ) -> CommandResult<WorkspaceReconcileResult> {
        validate_id(&params.operation_id)?;
        // A durable result is historical owner evidence. Cleanup or a newer
        // placement generation cannot make that receipt unreadable.
        if let Some(operation) = self
            .journal
            .operation(&params.operation_id)
            .map_err(storage_error)?
        {
            if let Some(outcome) = operation.outcome {
                if operation.fence.daemon_id != params.workspace.daemon_id
                    || operation.fence.runtime_id != params.workspace.runtime_id
                    || operation.fence.placement_id != params.workspace.placement_id
                    || operation.fence.generation != params.workspace.generation
                    || operation.workspace_handle.as_deref()
                        != Some(&params.workspace.workspace_handle)
                {
                    return Err(error(WRONG_OWNER, "receipt belongs to another workspace"));
                }
                return Ok(WorkspaceReconcileResult {
                    entry_id: operation.entry_id,
                    operation_id: params.operation_id,
                    outcome: match outcome {
                        Ok(result) => WorkspaceReconcileOutcome::Result { result },
                        Err(error) => WorkspaceReconcileOutcome::Error { error },
                    },
                });
            }
        }
        let owned = self.workspace(&params.workspace, false)?;
        let mut operation = match self
            .journal
            .operation(&params.operation_id)
            .map_err(storage_error)?
        {
            Some(operation) => operation,
            None => {
                let (WorkspaceIntegrationBinding::Attempt { request }
                | WorkspaceIntegrationBinding::TaskStepEffect { request }) = &params.integration
                else {
                    return Err(error(
                        DAEMON_UNAVAILABLE,
                        "operation journal entry is missing; its outcome is unknown",
                    ));
                };
                if request.fence.target_owner["daemon_id"].as_str()
                    != Some(&params.workspace.daemon_id)
                    || request.fence.target_owner["runtime_id"].as_str()
                        != Some(&params.workspace.runtime_id)
                    || request.witness["workspace"]["placement_id"].as_str()
                        != Some(&params.workspace.placement_id)
                    || request.witness["workspace"]["handle"].as_str()
                        != Some(&params.workspace.workspace_handle)
                {
                    return Err(error(
                        WRONG_OWNER,
                        "attempt lookup belongs to another workspace",
                    ));
                }
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                let mut updated = state.clone();
                // An absent intent proves no effect only after fencing a
                // delayed request with the same identity on this owner.
                updated.record_cancel_tombstone(&params.operation_id, unix_now());
                self.journal
                    .save_workspace_state(&updated)
                    .map_err(storage_error)?;
                *state = updated;
                drop(state);
                let fence = WorkspaceMutationFence {
                    integration: params.integration.clone(),
                    daemon_id: params.workspace.daemon_id.clone(),
                    runtime_id: params.workspace.runtime_id.clone(),
                    placement_id: params.workspace.placement_id.clone(),
                    operation_id: params.operation_id.clone(),
                    generation: params.workspace.generation,
                    expected: WorkspaceOperationExpected::BaseSha {
                        sha: request.witness["expected_head_sha"]
                            .as_str()
                            .unwrap_or_default()
                            .into(),
                    },
                };
                let operation = JournalOperation {
                    entry_id: operation_entry_id(&params.operation_id),
                    fence,
                    workspace_handle: Some(params.workspace.workspace_handle.clone()),
                    method: match request.kind {
                        WorkspaceIntegrationKind::Merge | WorkspaceIntegrationKind::FastForward => {
                            METHOD_WORKSPACE_MERGE
                        }
                        WorkspaceIntegrationKind::Rebase => METHOD_WORKSPACE_RESET,
                        WorkspaceIntegrationKind::Check => METHOD_WORKSPACE_RUN,
                    }
                    .into(),
                    request: serde_json::json!({"integration":params.integration}),
                    outcome: None,
                    acknowledged: false,
                    effect_started: false,
                };
                self.journal
                    .retain_entry(&JournalEntry::Operation {
                        operation: operation.clone(),
                    })
                    .map_err(storage_error)?;
                operation
            }
        };
        let fence = &operation.fence;
        if fence.daemon_id != params.workspace.daemon_id
            || fence.runtime_id != params.workspace.runtime_id
            || fence.placement_id != params.workspace.placement_id
            || operation.workspace_handle.as_deref() != Some(&params.workspace.workspace_handle)
        {
            return Err(error(WRONG_OWNER, "operation belongs to another workspace"));
        }
        self.check_generation(fence.generation, owned.generation, false)?;
        let integration = matches!(
            operation.fence.integration,
            WorkspaceIntegrationBinding::Attempt { .. }
                | WorkspaceIntegrationBinding::TaskStepEffect { .. }
        );
        if !integration
            && !matches!(
                operation.method.as_str(),
                METHOD_WORKSPACE_RUN | METHOD_WORKSPACE_MERGE
            )
        {
            return Err(error(
                INVALID_INPUT,
                "only run and merge intents can be reconciled",
            ));
        }
        if operation.outcome.is_none() {
            let outcome = if integration && !operation.effect_started {
                Err(error(
                    WORKSPACE_ERROR,
                    "integration intent was not performed",
                ))
            } else if operation.method == METHOD_WORKSPACE_MERGE {
                match self
                    .recover_merge(&owned, decode(operation.request.clone())?)
                    .await
                {
                    Ok(result) => encode(result),
                    Err(failure) => {
                        let mut interrupted = interrupted_error(&params.operation_id);
                        interrupted.message = format!(
                            "interrupted merge has no exact success proof: {}",
                            failure.message
                        );
                        Err(interrupted)
                    }
                }
            } else {
                // A process exit code cannot be reconstructed from files or
                // HEAD. Never rerun the configured command to guess a result.
                Err(interrupted_error(&params.operation_id))
            };
            operation.outcome = Some(outcome);
            let uncertain = operation.effect_started
                && operation
                    .outcome
                    .as_ref()
                    .is_some_and(|result| result.is_err());
            self.attach_integration_receipt(&mut operation, None, uncertain)
                .await;
            self.journal
                .finish_operation(&operation)
                .map_err(storage_error)?;
        }
        let outcome = match operation
            .outcome
            .expect("reconciliation settled the intent")
        {
            Ok(result) => WorkspaceReconcileOutcome::Result { result },
            Err(error) => WorkspaceReconcileOutcome::Error { error },
        };
        Ok(WorkspaceReconcileResult {
            entry_id: operation.entry_id,
            operation_id: params.operation_id,
            outcome,
        })
    }

    async fn recover_merge(
        &self,
        owned: &OwnedWorkspace,
        reviewed: WorkspaceReviewedMergeParams,
    ) -> CommandResult<WorkspaceMergeResult> {
        let params = reviewed.merge;
        let WorkspaceOperationExpected::BaseSha { sha: candidate } = &params.fence.expected else {
            return Err(error(
                DAEMON_UNAVAILABLE,
                "merge intent has no frozen candidate SHA",
            ));
        };
        if reviewed
            .reviewed_commit_sha
            .as_deref()
            .is_some_and(|sha| sha != candidate)
        {
            return Err(error(
                DAEMON_UNAVAILABLE,
                "reviewed object does not match the frozen candidate",
            ));
        }
        let (location, target) = self.location_path(&params.repo_location_id).await?;
        if location.kind != DaemonRepoLocationKind::PrimaryCheckout {
            return Err(error(
                INVALID_INPUT,
                "merge target is not a verified primary checkout",
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
        let target_ref = format!("refs/heads/{}", params.target_branch);
        if resolve_commit(&target, &target_ref).await? != *candidate
            || resolve_commit(&target, "HEAD").await? != *candidate
            || local_git(&target, &["symbolic-ref", "HEAD"]).await? != target_ref
            || resolve_commit(&target, candidate).await? != *candidate
            || resolve_commit(&target, &params.expected_target_sha).await?
                != params.expected_target_sha
            || local_git(
                &target,
                &["merge-base", &params.expected_target_sha, candidate],
            )
            .await?
                != params.expected_target_sha
            || !git::is_worktree_clean(&target).await.map_err(git_error)?
        {
            return Err(error(
                DAEMON_UNAVAILABLE,
                "target does not prove integration of the exact candidate",
            ));
        }
        if !params.handed_off_paths.is_empty() {
            let markers =
                git::paths_adding_conflict_markers(&target, &params.expected_target_sha, candidate)
                    .await
                    .map_err(git_error)?;
            if markers
                .iter()
                .any(|path| params.handed_off_paths.contains(path))
            {
                return Err(error(
                    DAEMON_UNAVAILABLE,
                    "candidate still contains handed-off conflict markers",
                ));
            }
        }
        let diffstat = merge_diffstat(&target, &params.expected_target_sha, candidate).await?;
        Ok(WorkspaceMergeResult {
            entry_id: operation_entry_id(&params.fence.operation_id),
            operation_id: params.fence.operation_id,
            outcome: WorkspaceMergeOutcome::Done {
                before_sha: params.expected_target_sha,
                after_sha: candidate.clone(),
                branch: params.target_branch,
            },
            diffstat: Some(diffstat),
        })
    }
}
