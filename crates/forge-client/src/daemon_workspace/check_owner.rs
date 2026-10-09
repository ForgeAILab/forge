//! Fence-free, idempotent owner check operations. No Task/Review authority.
use super::*;
use crate::daemon_persistence::JournalCheckOperation;
use tokio_util::sync::CancellationToken;

impl DaemonWorkspaceBackend {
    pub(super) async fn check_lookup(
        &self,
        params: DaemonCheckOperationParams,
    ) -> CommandResult<DaemonCheckResult> {
        if params.daemon_id != self.daemon_id {
            return Err(error(WRONG_OWNER, "check lookup belongs to another owner"));
        }
        validate_id(&params.operation_id)?;
        let Some(operation) = self
            .journal
            .check_operation(&params.operation_id)
            .map_err(storage_error)?
        else {
            return Ok(DaemonCheckResult::Unknown {
                operation_id: params.operation_id,
            });
        };
        if let Some(receipt) = operation.receipt {
            return Ok(DaemonCheckResult::Completed {
                receipt: Box::new(receipt),
            });
        }
        if self
            .running_commands
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains_key(&params.operation_id)
        {
            Ok(DaemonCheckResult::Running {
                operation_id: params.operation_id,
            })
        } else {
            Ok(DaemonCheckResult::Interrupted {
                operation_id: params.operation_id,
            })
        }
    }
    pub(super) async fn check_cancel(
        &self,
        params: DaemonCheckOperationParams,
    ) -> CommandResult<DaemonCheckResult> {
        if params.daemon_id != self.daemon_id {
            return Err(error(WRONG_OWNER, "check cancel belongs to another owner"));
        }
        self.cancel_command(WorkspaceCancelParams {
            operation_id: params.operation_id.clone(),
        })
        .await?;
        self.check_lookup(params).await
    }
    pub(super) async fn check_run(
        &self,
        params: DaemonCheckRunParams,
    ) -> CommandResult<DaemonCheckResult> {
        validate_id(&params.operation_id)?;
        params
            .spec
            .validate()
            .map_err(|e| error(INVALID_INPUT, e))?;
        if params.spec.commands.len() > 64
            || params.cleanup_commands.len() > 32
            || params.cleanup_timeout_ms == 0
            || params.cleanup_timeout_ms > 30_000
            || serde_json::to_vec(&params)
                .map_err(|e| error(INVALID_INPUT, e.to_string()))?
                .len()
                > 128 * 1024
        {
            return Err(error(
                INVALID_INPUT,
                "check request exceeds owner command, cleanup or manifest bounds",
            ));
        }
        let deadline = chrono::DateTime::parse_from_rfc3339(&params.deadline)
            .map_err(|_| error(INVALID_INPUT, "check deadline must be RFC3339"))?;
        let remaining = (deadline.with_timezone(&chrono::Utc) - chrono::Utc::now())
            .to_std()
            .unwrap_or_default();
        let deadline = Instant::now().checked_add(remaining).ok_or_else(|| {
            error(
                INVALID_INPUT,
                "check deadline is outside the owner clock range",
            )
        })?;
        let raw = serde_json::to_value(&params).map_err(|e| error(INVALID_INPUT, e.to_string()))?;
        let request = journal_request(&raw);
        // Key lock covers receipt lookup, intent retention and registration only.
        // A duplicate never queues behind the process or acquires its checkout.
        let admission = self.owner_lock(&format!("check-operation:{}", params.operation_id));
        let admitted = admission.lock().await;
        if let Some(operation) = self
            .journal
            .check_operation(&params.operation_id)
            .map_err(storage_error)?
        {
            if operation.request != request {
                return Err(error(
                    TERMINAL_REPORT_CONFLICT,
                    "check operation key reused for a different request",
                ));
            }
            let result = self
                .check_lookup(DaemonCheckOperationParams {
                    daemon_id: self.daemon_id.clone(),
                    operation_id: params.operation_id,
                })
                .await;
            return result;
        }
        if !self.policy.allowed_purposes.contains(&params.purpose)
            || matches!(
                params.purpose,
                WorkspaceRunPurpose::RepoProvision | WorkspaceRunPurpose::EnvironmentProbe
            )
        {
            return Err(error(
                PURPOSE_DENIED,
                "check purpose is denied by local daemon configuration",
            ));
        }
        if self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .cancel_tombstones
            .contains_key(&params.operation_id)
        {
            return Err(error(
                WORKSPACE_ERROR,
                "check operation was cancelled before arrival",
            ));
        }
        let (path, owner_runtime, key, exact) = match &params.target {
            DaemonCheckTarget::Workspace { workspace } => {
                let owned = self.workspace(workspace, false)?;
                (
                    owned.path,
                    owned.runtime_id,
                    format!("workspace:{}", workspace.workspace_handle),
                    None,
                )
            }
            DaemonCheckTarget::ExactCommit {
                daemon_id,
                runtime_id,
                repo_location_id,
                commit_sha,
            } => {
                self.check_owner(daemon_id, runtime_id)?;
                let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                let location = state
                    .locations
                    .get(repo_location_id)
                    .ok_or_else(|| error(INVALID_INPUT, "unknown check repository location"))?;
                if location.runtime_id != *runtime_id || location.daemon_id != *daemon_id {
                    return Err(error(WRONG_OWNER, "check location owner mismatch"));
                }
                (
                    location.path.clone(),
                    runtime_id.clone(),
                    format!("location:{repo_location_id}"),
                    Some(commit_sha.clone()),
                )
            }
        };
        let mut operation = JournalCheckOperation {
            entry_id: operation_entry_id(&params.operation_id),
            operation_id: params.operation_id.clone(),
            request: raw,
            receipt: None,
        };
        self.journal
            .retain_entry(&JournalEntry::Check {
                operation: operation.clone(),
            })
            .map_err(storage_error)?;
        let (signal, mut cancelled) = tokio::sync::watch::channel(false);
        let (finished, completed) = tokio::sync::watch::channel(None);
        self.running_commands
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(
                params.operation_id.clone(),
                RunningWorkspaceCommand {
                    cancel: signal,
                    finished: completed,
                },
            );
        let mut guard = WorkspaceCommandGuard {
            running: &self.running_commands,
            id: params.operation_id.clone(),
            finished,
            state: WorkspaceCancelState::Killed,
        };
        drop(admitted);
        let token = CancellationToken::new();
        let checkout_lock = self.owner_lock(&key);
        // Cancellation while waiting still reaches the primitive, so declared
        // cleanup gets its independent settlement phase.
        // A cancelled request cannot clean beneath another live checkout
        // owner. Wait for the mutation guard, then let the primitive settle
        // without launching any run command if its deadline/token expired.
        let checkout = checkout_lock.lock().await;
        if *cancelled.borrow_and_update() {
            token.cancel();
        }
        // Revalidate workspace ownership/generation after the checkout lock.
        if let DaemonCheckTarget::Workspace { workspace } = &params.target {
            self.live_workspace(workspace).await?;
        }
        let build = self.confined_path(&self.workspace_root.join(".forge/build/checks"))?;
        let environment = params.env.iter().cloned().collect::<BTreeMap<_, _>>();
        let input = check_executor::CheckExecution {
            operation_id: &params.operation_id,
            spec: &params.spec,
            target: match &exact {
                Some(commit) => check_executor::CheckoutTarget::ExactCommit {
                    repository: &path,
                    build_area: &build,
                    commit,
                },
                None => check_executor::CheckoutTarget::Workspace(&path),
            },
            owner: CheckOwnerIdentity {
                owner_kind: "daemon".into(),
                machine_id: Some(self.daemon_id.clone()),
                runtime_id: owner_runtime,
            },
            input_revisions: None,
            environment: &environment,
            deadline: Some(deadline),
            cancel: &token,
            permit: &check_executor::CheckPermit::already_admitted(),
            cleanup: check_executor::CleanupPlan {
                commands: &params.cleanup_commands,
                timeout: Duration::from_millis(params.cleanup_timeout_ms),
            },
            output_limit: check_executor::OUTPUT_TAIL_BYTES,
        };
        let execution = check_executor::execute(input);
        tokio::pin!(execution);
        let receipt = tokio::select! {
            receipt=&mut execution => receipt,
            _=cancelled.changed() => {token.cancel();execution.await},
        };
        operation.receipt = Some(receipt);
        let retained = self
            .journal
            .finish_check(&operation)
            .map_err(storage_error)?;
        guard.state = WorkspaceCancelState::AlreadyFinished;
        drop(checkout);
        drop(guard);
        Ok(DaemonCheckResult::Completed {
            receipt: Box::new(retained.receipt.expect("retained receipt")),
        })
    }
}
