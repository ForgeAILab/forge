//! Workspace preparation, bounded command execution, file reads, and diffs.

use super::*;

impl DaemonWorkspaceBackend {
    pub(super) async fn prepare(
        &self,
        params: WorkspacePrepareParams,
    ) -> CommandResult<WorkspacePrepareResult> {
        let (location, repo_path) = self.location_path(&params.repo_location_id).await?;
        if location.daemon_id != params.fence.daemon_id
            || location.runtime_id != params.fence.runtime_id
        {
            return Err(error(
                WRONG_OWNER,
                "repository location belongs to another runtime",
            ));
        }
        let base_sha = resolve_commit(&repo_path, &params.base_ref).await?;
        validate_branch(&repo_path, &params.branch).await?;
        let (handle, mut owned) = match self.workspace_for_placement(&params.fence.placement_id) {
            Some((handle, owned)) => {
                if owned.repo_location_id != params.repo_location_id
                    || owned.branch != params.branch
                    || owned.cleaned
                {
                    return Err(error(
                        INVALID_INPUT,
                        "placement already has a different or cleaned workspace",
                    ));
                }
                (handle, owned)
            }
            None => {
                if git::branch_exists(&repo_path, &params.branch)
                    .await
                    .map_err(git_error)?
                {
                    return Err(error(
                        INVALID_INPUT,
                        "prepare branch already exists outside this placement",
                    ));
                }
                if let WorkspaceOperationExpected::BaseSha { sha } = &params.fence.expected {
                    if sha != &base_sha {
                        return Err(error(VERSION_CONFLICT, "prepare base SHA changed"));
                    }
                }
                let handle = format!("workspace-{}", uuid::Uuid::new_v4());
                let owned = OwnedWorkspace {
                    daemon_id: params.fence.daemon_id.clone(),
                    runtime_id: params.fence.runtime_id.clone(),
                    placement_id: params.fence.placement_id.clone(),
                    repo_location_id: params.repo_location_id.clone(),
                    path: self.path_for_handle(&handle)?,
                    branch: params.branch.clone(),
                    base_sha,
                    generation: params.fence.generation,
                    version: match params.fence.expected {
                        WorkspaceOperationExpected::Version { version } => version,
                        _ => 0,
                    },
                    prepared: false,
                    cleaned: false,
                    retired_by_operation_id: None,
                    execution_ids: Vec::new(),
                    review_parent: None,
                    review_operation_id: None,
                };
                self.save_workspace(&handle, owned.clone())?;
                (handle, owned)
            }
        };
        let reference = reference(&params.fence, &handle);
        self.workspace(&reference, false)?;
        if owned.path.exists() {
            verify_git_dir(&owned.path, &self.workspace_root).await?;
        }
        self.check_expected(&owned, &params.fence.expected).await?;
        if !owned.path.exists() {
            if owned.prepared {
                if !git::branch_exists(&repo_path, &owned.branch)
                    .await
                    .map_err(git_error)?
                {
                    return Err(error(
                        INVALID_INPUT,
                        "workspace branch is missing; reset_required",
                    ));
                }
                self.manager
                    .recover_worktree_named(
                        repo_path
                            .to_str()
                            .ok_or_else(|| error(INVALID_INPUT, "repository path is not UTF-8"))?,
                        &handle,
                        "repo",
                        &owned.branch,
                    )
                    .await
                    .map_err(workspace_error)?;
            } else {
                self.manager
                    .create_detached_worktree_named(
                        repo_path
                            .to_str()
                            .ok_or_else(|| error(INVALID_INPUT, "repository path is not UTF-8"))?,
                        &handle,
                        "repo",
                        &owned.base_sha,
                    )
                    .await
                    .map_err(workspace_error)?;
            }
        }
        let path = confined_existing(&owned.path, &self.workspace_root)?;
        verify_git_dir(&path, &self.workspace_root).await?;
        // `<root>/.forge/workspaces/<handle>/` is this daemon's Task root:
        // reserve `.forge-task` beside the worktree for run temp directories,
        // managed homes and build output.
        if let Some(task_root) = path.parent() {
            executors::sandbox::TaskRoot::reserve(task_root).map_err(io_error)?;
        }
        if !owned.prepared {
            if git::branch_exists(&repo_path, &owned.branch)
                .await
                .map_err(git_error)?
            {
                git::checkout_branch(&path, &owned.branch)
                    .await
                    .map_err(git_error)?;
            } else {
                local_git(&path, &["checkout", "-b", &owned.branch]).await?;
            }
            owned.prepared = true;
            owned.version += 1;
            self.save_workspace(&handle, owned.clone())?;
        }
        Ok(WorkspacePrepareResult {
            entry_id: operation_entry_id(&params.fence.operation_id),
            operation_id: params.fence.operation_id,
            workspace: prepared_state(&handle, &owned),
        })
    }

    pub(super) async fn run(
        &self,
        params: WorkspaceRunParams,
    ) -> CommandResult<WorkspaceRunResult> {
        if matches!(
            params.purpose,
            WorkspaceRunPurpose::EnvironmentProbe | WorkspaceRunPurpose::RepoProvision
        ) {
            return Err(error(
                INVALID_INPUT,
                "machine operations require their dedicated RPC",
            ));
        }
        if !self.policy.allowed_purposes.contains(&params.purpose) {
            return Err(error(
                PURPOSE_DENIED,
                "workspace.run purpose is denied by local daemon configuration",
            ));
        }
        if params.max_output_bytes == 0
            || (params.timeout_secs == 0 && params.purpose != WorkspaceRunPurpose::CiStep)
            || (params.max_output_bytes == u64::MAX
                && params.purpose != WorkspaceRunPurpose::CiStep)
        {
            return Err(error(
                INVALID_INPUT,
                "workspace.run requires positive time and output limits",
            ));
        }
        let mut owned = self
            .live_workspace(&reference(&params.fence, &params.workspace_handle))
            .await?;
        self.check_expected(&owned, &params.fence.expected).await?;
        let mut command = Command::new("bash");
        command
            .arg("-lc")
            .arg(&params.command)
            .envs(params.env.iter().cloned())
            .env("PWD", &owned.path)
            .current_dir(&owned.path);
        command
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE");
        executors::sandbox::start_compiler_cache(&owned.path).await;
        // The command's temp directory, removed when this call returns.
        let run_scope = executors::sandbox::SandboxEnv::for_command(
            &owned.path,
            executors::sandbox::RunPurpose::Check,
        );
        executors::run_process::apply_sandboxed(
            &mut command,
            &params.env.iter().cloned().collect(),
            run_scope.env(),
        );
        // Persist the version before launching; journal pressure after the
        // command exits must never discard its exit result.
        owned.version += 1;
        let run_path = owned.path.clone();
        let owner_runtime = owned.runtime_id.clone();
        self.save_workspace(&params.workspace_handle, owned)?;
        let start = Instant::now();
        let output_cap = usize::try_from(params.max_output_bytes)
            .unwrap_or(usize::MAX)
            .min(crate::daemon_persistence::MAX_CI_LOG_BYTES);
        if params.purpose == WorkspaceRunPurpose::CiStep && params.max_output_bytes == u64::MAX {
            let environment = params.env.iter().cloned().collect::<BTreeMap<_, _>>();
            let receipt = check_executor::execute(check_executor::CheckExecution {
                operation_id: &params.fence.operation_id,
                spec: &check_executor::legacy_ci_spec(
                    &params.command,
                    &environment,
                    params.timeout_secs,
                    true,
                ),
                target: check_executor::CheckoutTarget::Workspace(&run_path),
                owner: CheckOwnerIdentity {
                    owner_kind: "daemon".into(),
                    machine_id: Some(self.daemon_id.clone()),
                    runtime_id: owner_runtime,
                },
                input_revisions: None,
                environment: &environment,
                deadline: None,
                cancel: &tokio_util::sync::CancellationToken::new(),
                permit: &check_executor::CheckPermit::already_admitted(),
                cleanup: check_executor::CleanupPlan {
                    commands: &[],
                    timeout: check_executor::CLEANUP_TIMEOUT,
                },
                output_limit: output_cap,
            })
            .await;
            if let Some(message) = receipt.infrastructure_message {
                return Err(error(WORKSPACE_ERROR, message));
            }
            let output = receipt.commands.into_iter().next();
            return Ok(WorkspaceRunResult {
                entry_id: operation_entry_id(&params.fence.operation_id),
                operation_id: params.fence.operation_id,
                exit_code: output.as_ref().map_or(Some(0), |o| o.exit_code),
                stdout: output
                    .as_ref()
                    .map_or_else(String::new, |o| o.stdout_tail.clone()),
                stderr: output
                    .as_ref()
                    .map_or_else(String::new, |o| o.stderr_tail.clone()),
                duration_ms: start.elapsed().as_millis().min(u64::MAX as u128) as u64,
                timed_out: output
                    .as_ref()
                    .is_some_and(|o| o.outcome == CheckExecutionOutcome::TimedOut),
                stdout_truncated: output.as_ref().is_some_and(|o| o.stdout_truncated),
                stderr_truncated: output.as_ref().is_some_and(|o| o.stderr_truncated),
                stdout_drain_incomplete: output.as_ref().is_some_and(|o| o.stdout_drain_incomplete),
                stderr_drain_incomplete: output.as_ref().is_some_and(|o| o.stderr_drain_incomplete),
            });
        }
        let output = bounded_command(command, params.timeout_secs, output_cap, true).await?;
        let environment: BTreeMap<String, String> = params.env.into_iter().collect();
        let (stdout, stdout_truncated) = redacted_tail(&output.stdout, &environment, output_cap);
        let (stderr, stderr_truncated) = redacted_tail(&output.stderr, &environment, output_cap);
        Ok(WorkspaceRunResult {
            entry_id: operation_entry_id(&params.fence.operation_id),
            operation_id: params.fence.operation_id,
            exit_code: output.exit_code,
            stdout,
            stderr,
            duration_ms: start.elapsed().as_millis().min(u64::MAX as u128) as u64,
            timed_out: output.timed_out,
            stdout_truncated: output.stdout_truncated || stdout_truncated,
            stderr_truncated: output.stderr_truncated || stderr_truncated,
            stdout_drain_incomplete: output.stdout_drain_incomplete,
            stderr_drain_incomplete: output.stderr_drain_incomplete,
        })
    }

    pub(super) async fn read(
        &self,
        params: WorkspaceReadParams,
    ) -> CommandResult<WorkspaceReadResult> {
        let owned = self.live_workspace(&params.workspace).await?;
        let path = self.workspace_read_path(&owned.path, &params.path)?;
        let limit = params.limit.min(MAX_READ_BYTES);
        let mut bytes = read_bounded_file(&path, limit.saturating_add(1)).await?;
        let truncated = bytes.len() as u64 > limit;
        bytes.truncate(limit as usize);
        Ok(WorkspaceReadResult {
            path: params.path,
            bytes,
            truncated,
        })
    }

    pub(super) async fn diff(
        &self,
        params: WorkspaceDiffParams,
    ) -> CommandResult<WorkspaceDiffResult> {
        let owned = self.live_workspace(&params.workspace).await?;
        let base_sha = resolve_commit(&owned.path, &params.base_ref).await?;
        let head_sha =
            resolve_commit(&owned.path, params.head_ref.as_deref().unwrap_or("HEAD")).await?;
        let mut revisions = vec![base_sha.as_str()];
        if params.head_ref.is_some() {
            revisions.push(&head_sha);
        }
        let args = |flag: &str| {
            let mut args = vec![
                "diff".to_owned(),
                flag.to_owned(),
                "--find-renames".to_owned(),
            ];
            args.extend(revisions.iter().map(|s| s.to_string()));
            args.push("--".into());
            args
        };
        let statuses = git_output(&owned.path, &args("--name-status"), MAX_DIFF_BYTES).await?;
        let counts = git_output(&owned.path, &args("--numstat"), MAX_DIFF_BYTES).await?;
        let mut files = BTreeMap::<String, WorkspaceDiffFile>::new();
        for line in String::from_utf8_lossy(&statuses.stdout).lines() {
            let fields: Vec<_> = line.split('\t').collect();
            if fields.len() < 2 {
                continue;
            }
            let status = match fields[0].chars().next() {
                Some('A') => WorkspaceDiffFileStatus::Added,
                Some('D') => WorkspaceDiffFileStatus::Deleted,
                Some('R') => WorkspaceDiffFileStatus::Renamed,
                _ => WorkspaceDiffFileStatus::Modified,
            };
            let path = fields.last().unwrap().to_string();
            files.insert(
                path.clone(),
                WorkspaceDiffFile {
                    path,
                    status,
                    additions: 0,
                    deletions: 0,
                },
            );
        }
        for line in String::from_utf8_lossy(&counts.stdout).lines() {
            let fields: Vec<_> = line.split('\t').collect();
            if fields.len() < 3 {
                continue;
            }
            let path = if fields.len() >= 4 {
                fields[3]
            } else {
                fields[2]
            };
            let file = files.entry(path.to_owned()).or_insert(WorkspaceDiffFile {
                path: path.to_owned(),
                status: WorkspaceDiffFileStatus::Modified,
                additions: 0,
                deletions: 0,
            });
            file.additions = fields[0].parse().unwrap_or(0);
            file.deletions = fields[1].parse().unwrap_or(0);
        }
        let files: Vec<_> = files.into_values().collect();
        let stats = WorkspaceDiffStats {
            files_changed: files.len() as u64,
            total_additions: files.iter().map(|f| f.additions).sum(),
            total_deletions: files.iter().map(|f| f.deletions).sum(),
        };
        let diff = git_output(
            &owned.path,
            &args("--patch"),
            params.max_bytes.min(MAX_DIFF_BYTES as u64) as usize,
        )
        .await?;
        Ok(WorkspaceDiffResult {
            base_ref: params.base_ref,
            head_ref: params.head_ref.unwrap_or(owned.branch),
            base_sha,
            head_sha,
            files,
            stats,
            diff: String::from_utf8_lossy(&diff.stdout).into_owned(),
            truncated: diff.stdout_truncated
                || statuses.stdout_truncated
                || counts.stdout_truncated,
        })
    }
}
