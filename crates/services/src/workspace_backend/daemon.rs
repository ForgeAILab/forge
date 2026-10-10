use std::sync::Arc;

use api_types::{
    WorkspaceCleanupParams, WorkspaceDescribeParams, WorkspaceDiffFileStatus, WorkspaceDiffParams,
    WorkspaceHandleReference, WorkspaceMergeParams, WorkspaceMutationFence,
    WorkspaceOperationExpected, WorkspacePrepareParams, WorkspacePreparedState,
    WorkspaceReadParams, WorkspaceResetParams, WorkspaceReviewedMergeParams, WorkspaceRunParams,
};
use async_trait::async_trait;
use db::{PlacementOwnerKind, SqliteDb, Workspace, WorkspacePlacement, WorkspaceRepo};

use super::{
    workspace_handle, CleanupAck, Diff, DiffSpec, MergeOutcome, MergeSpec, OutboxHarvest,
    PrepareSpec, PreparedWorkspace, ResetSpec, Result, RunResult, RunSpec, WorkspaceBackend,
    WorkspaceBackendError, WorkspaceRunPurpose, WorkspaceState,
};
use crate::{
    daemon_transport::{
        workspace_client::{DaemonWorkspaceClient, WorkspaceClientError},
        DaemonConnectionRegistry,
    },
    integration_effects::rpc::validate_merge_result,
    ServiceError,
};

struct MergeReplyRecord<'a> {
    request: &'a serde_json::Value,
    result: &'a api_types::WorkspaceMergeResult,
    execution_id: &'a str,
    attempt: Option<&'a db::TaskStep>,
    operation_id: &'a str,
}

pub struct DaemonWorkspaceBackend {
    db: Arc<SqliteDb>,
    client: DaemonWorkspaceClient,
    integration_locks: workspace::RepoCacheLockManager,
}

impl DaemonWorkspaceBackend {
    pub fn new(db: Arc<SqliteDb>, registry: Arc<DaemonConnectionRegistry>) -> Self {
        Self {
            db: Arc::clone(&db),
            client: DaemonWorkspaceClient::new(registry).with_receipts(Arc::clone(&db)),
            integration_locks: workspace::RepoCacheLockManager::new(),
        }
    }

    /// Consume validated owner facts with today's receipt transaction, then
    /// finish the Task operation and acknowledge the retained owner journal.
    async fn record_merge_reply(
        &self,
        placement: &WorkspacePlacement,
        daemon_id: &str,
        record: MergeReplyRecord<'_>,
    ) -> Result<()> {
        validate_merge_result(record.request, record.result)
            .map_err(|error| Self::error(placement, None, error))?;
        let value = serde_json::to_value(record.result).expect("merge result serializes");
        self.client
            .retain_merge_result(daemon_id, record.request, &value, record.execution_id)
            .await
            .map_err(|error| Self::error(placement, None, error))?;
        if let Some(step) = record.attempt {
            self.db
                .finish_remote_task_operation(step, record.operation_id)
                .await?;
        }
        self.client.acknowledge_recorded(daemon_id, &value).await;
        Ok(())
    }

    fn owner<'a>(&self, placement: &'a WorkspacePlacement) -> Result<(&'a str, &'a str)> {
        if placement.owner_kind != PlacementOwnerKind::Daemon {
            return Err(WorkspaceBackendError::WrongOwner {
                placement_id: placement.id.clone(),
            });
        }
        match (
            placement.daemon_id.as_deref(),
            placement.runtime_id.as_deref(),
        ) {
            (Some(daemon_id), Some(runtime_id))
                if !daemon_id.is_empty() && !runtime_id.is_empty() =>
            {
                Ok((daemon_id, runtime_id))
            }
            _ => Err(WorkspaceBackendError::WrongOwner {
                placement_id: placement.id.clone(),
            }),
        }
    }

    fn generation(&self, placement: &WorkspacePlacement) -> Result<u64> {
        if placement.generation < 1 {
            return Err(ServiceError::invalid_operation(
                "workspace placement generation must be positive",
            )
            .into());
        }
        u64::try_from(placement.generation).map_err(|_| {
            ServiceError::invalid_operation("workspace placement generation is invalid").into()
        })
    }

    fn reference(&self, placement: &WorkspacePlacement) -> Result<WorkspaceHandleReference> {
        let (daemon_id, runtime_id) = self.owner(placement)?;
        Ok(WorkspaceHandleReference {
            daemon_id: daemon_id.to_owned(),
            runtime_id: runtime_id.to_owned(),
            placement_id: placement.id.clone(),
            workspace_handle: workspace_handle(placement)?.to_owned(),
            generation: self.generation(placement)?,
        })
    }

    fn fence(
        &self,
        placement: &WorkspacePlacement,
        operation_id: String,
        expected: WorkspaceOperationExpected,
    ) -> Result<WorkspaceMutationFence> {
        let (daemon_id, runtime_id) = self.owner(placement)?;
        Ok(WorkspaceMutationFence {
            integration: api_types::WorkspaceIntegrationBinding::TaskStep,
            daemon_id: daemon_id.to_owned(),
            runtime_id: runtime_id.to_owned(),
            placement_id: placement.id.clone(),
            operation_id,
            generation: self.generation(placement)?,
            expected,
        })
    }

    async fn workspace(&self, placement: &WorkspacePlacement) -> Result<Workspace> {
        self.owner(placement)?;
        if !self
            .db
            .pending_remote_cancels(None, Some(&placement.workspace_id))
            .await?
            .is_empty()
        {
            return Err(ServiceError::invalid_operation(
                "pending_remote_cancel: workspace is fenced until owner acknowledgement",
            )
            .into());
        }

        WorkspaceRepo::get_by_id(&*self.db, &placement.workspace_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", &placement.workspace_id).into())
    }

    fn prepared(
        &self,
        placement: &WorkspacePlacement,
        workspace: WorkspacePreparedState,
    ) -> Result<PreparedWorkspace> {
        self.check_generation(placement, workspace.generation)?;
        if workspace.workspace_handle.is_empty() || workspace.base_sha.is_empty() {
            return Err(ServiceError::invalid_operation(
                "daemon returned an incomplete prepared workspace",
            )
            .into());
        }
        Ok(PreparedWorkspace {
            handle: workspace.workspace_handle,
            base_sha: workspace.base_sha,
            branch: workspace.branch,
        })
    }

    fn check_generation(&self, placement: &WorkspacePlacement, generation: u64) -> Result<()> {
        if self.generation(placement)? != generation {
            return Err(WorkspaceBackendError::StaleGeneration {
                placement_id: placement.id.clone(),
                expected: placement.generation,
                actual: i64::try_from(generation).unwrap_or(i64::MAX),
            });
        }
        Ok(())
    }

    fn check_operation(&self, expected: &str, actual: &str) -> Result<()> {
        if actual != expected {
            return Err(ServiceError::invalid_operation(
                "daemon returned a different workspace operation id",
            )
            .into());
        }
        Ok(())
    }

    async fn current_expected(
        &self,
        placement: &WorkspacePlacement,
    ) -> Result<WorkspaceOperationExpected> {
        let state = self.describe(placement).await?;
        let sha = state
            .head_sha
            .filter(|sha| !sha.is_empty())
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "workspace has no HEAD for the mutation precondition",
                )
            })?;
        // The owner's journal version and the placement's lifecycle version are distinct.
        Ok(WorkspaceOperationExpected::BaseSha { sha })
    }

    pub(super) fn error(
        placement: &WorkspacePlacement,
        purpose: Option<WorkspaceRunPurpose>,
        error: WorkspaceClientError,
    ) -> WorkspaceBackendError {
        let daemon_id = placement.daemon_id.clone().unwrap_or_default();
        match error {
            WorkspaceClientError::Transport(ServiceError::DaemonUnavailable { .. }) => {
                WorkspaceBackendError::OwnerUnreachable { daemon_id }
            }
            WorkspaceClientError::Transport(error) => error.into(),
            WorkspaceClientError::Daemon(error) => match error.code.as_str() {
                api_types::STALE_GENERATION => WorkspaceBackendError::StaleGeneration {
                    placement_id: placement.id.clone(),
                    expected: placement.generation,
                    actual: reported_generation(&error).unwrap_or(placement.generation),
                },
                api_types::WRONG_OWNER => WorkspaceBackendError::WrongOwner {
                    placement_id: placement.id.clone(),
                },
                api_types::PURPOSE_DENIED if purpose.is_some() => {
                    WorkspaceBackendError::PurposeDenied {
                        purpose: purpose.expect("run purpose checked"),
                    }
                }
                api_types::DAEMON_UNAVAILABLE | "disconnected"
                    if !error
                        .details
                        .as_ref()
                        .is_some_and(|details| details["interrupted"] == true) =>
                {
                    WorkspaceBackendError::OwnerUnreachable { daemon_id }
                }
                api_types::DISK_PRESSURE => WorkspaceBackendError::DiskPressure {
                    task_id: placement.task_id.clone(),
                    repo_location_id: placement.repo_location_id.clone(),
                    daemon_id,
                    runtime_id: placement.runtime_id.clone(),
                },
                "version_conflict" => db::DbError::VersionConflict.into(),
                // The shared backend error contract represents path refusals as InvalidOperation.
                _ => ServiceError::invalid_operation(format!("{}: {}", error.code, error.message))
                    .into(),
            },
        }
    }
}

fn operation_id(placement: &WorkspacePlacement, method: &str) -> String {
    uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_URL,
        format!("forge:{method}:{}:{}", placement.id, placement.generation).as_bytes(),
    )
    .to_string()
}

fn reported_generation(error: &api_types::DaemonErrorPayload) -> Option<i64> {
    error
        .details
        .as_ref()
        .and_then(|details| {
            [
                "actual",
                "actual_generation",
                "current_generation",
                "generation",
            ]
            .iter()
            .find_map(|key| details.get(key).and_then(serde_json::Value::as_i64))
        })
        .or_else(|| {
            error
                .message
                .rsplit_once("current generation ")
                .and_then(|(_, generation)| generation.split_whitespace().next())
                .and_then(|generation| generation.parse().ok())
        })
}

#[async_trait]
impl WorkspaceBackend for DaemonWorkspaceBackend {
    fn daemon_client(&self) -> Option<&DaemonWorkspaceClient> {
        Some(&self.client)
    }

    async fn prepare(
        &self,
        placement: &WorkspacePlacement,
        base: &PrepareSpec,
    ) -> Result<PreparedWorkspace> {
        let workspace = self.workspace(placement).await?;
        // Initial owner-local version is stable across placement lifecycle CAS updates.
        let recovering = placement.state == db::PlacementState::Ready;
        let expected = if recovering {
            WorkspaceOperationExpected::BaseSha {
                sha: base.base_ref.clone(),
            }
        } else {
            WorkspaceOperationExpected::Version { version: 0 }
        };
        let operation_id = if recovering {
            // A prior preparation receipt must not hide a deleted directory.
            db::new_uuid_v4()
        } else {
            operation_id(placement, api_types::METHOD_WORKSPACE_PREPARE)
        };
        let result = self
            .client
            .prepare(
                self.owner(placement)?.0,
                WorkspacePrepareParams {
                    fence: self.fence(placement, operation_id.clone(), expected)?,
                    repo_location_id: placement.repo_location_id.clone(),
                    workspace_id: placement.workspace_id.clone(),
                    task_id: placement.task_id.clone(),
                    base_ref: base.base_ref.clone(),
                    branch: workspace.branch,
                },
            )
            .await
            .map_err(|error| match &error {
                WorkspaceClientError::Daemon(error)
                    if recovering
                        && matches!(
                            error.code.as_str(),
                            api_types::INVALID_INPUT | "version_conflict"
                        ) =>
                {
                    ServiceError::WorkspaceResetRequired {
                        task_id: placement.task_id.clone(),
                        reason: error.message.clone(),
                    }
                    .into()
                }
                _ => Self::error(placement, None, error),
            })?;
        self.check_operation(&operation_id, &result.operation_id)?;
        self.prepared(placement, result.workspace)
    }

    async fn describe(&self, placement: &WorkspacePlacement) -> Result<WorkspaceState> {
        let result = self
            .client
            .describe(
                self.owner(placement)?.0,
                WorkspaceDescribeParams {
                    workspace: self.reference(placement)?,
                },
            )
            .await
            .map_err(|error| match error {
                WorkspaceClientError::Daemon(error) if error.code == "workspace_error" => {
                    ServiceError::Git(git::GitError::CommandFailed {
                        command: "workspace.describe".into(),
                        stdout: String::new(),
                        stderr: error.message,
                    })
                    .into()
                }
                error => Self::error(placement, None, error),
            })?;
        self.check_generation(placement, result.generation)?;
        if result.workspace_handle != workspace_handle(placement)? {
            return Err(WorkspaceBackendError::WrongOwner {
                placement_id: placement.id.clone(),
            });
        }
        Ok(WorkspaceState {
            exists: result.exists,
            head_sha: result.head_sha,
            dirty: result.dirty,
            branch: result.branch,
            locked: result.locked,
            active_execution_ids: result.active_execution_ids,
            journaled_execution_ids: result.journaled_execution_ids,
        })
    }

    async fn run(&self, placement: &WorkspacePlacement, spec: &RunSpec) -> Result<RunResult> {
        if !self
            .db
            .pending_remote_cancels(None, Some(&placement.workspace_id))
            .await?
            .is_empty()
        {
            return Err(ServiceError::invalid_operation(
                "pending_remote_cancel: workspace is fenced until owner acknowledgement",
            )
            .into());
        }
        if spec.max_output_bytes == 0
            || (spec.timeout_secs == 0 && spec.purpose != WorkspaceRunPurpose::CiStep)
        {
            return Err(ServiceError::invalid_operation(
                "workspace run requires an output budget and non-CI commands require a timeout",
            )
            .into());
        }
        let expected = self
            .current_expected(placement)
            .await
            .map_err(|error| match error {
                WorkspaceBackendError::Other(ref service)
                    if matches!(**service, ServiceError::DaemonTimeout { .. }) =>
                {
                    WorkspaceBackendError::RpcTimeoutBeforeStart {
                        daemon_id: placement.daemon_id.clone().unwrap_or_default(),
                        method: api_types::METHOD_WORKSPACE_DESCRIBE.to_owned(),
                    }
                }
                error => error,
            })?;
        let operation_id = db::new_uuid_v4();
        let attempt = crate::workflow::engine::durable::current_hook(&placement.task_id)
            .map(|attempt| attempt.step)
            .or_else(db::task_writer::current_task_step);
        if let Some(attempt) = &attempt {
            self.db
                .register_remote_task_operation(attempt, placement, &operation_id)
                .await?;
        }
        let result = self
            .client
            .run(
                self.owner(placement)?.0,
                WorkspaceRunParams {
                    fence: self.fence(placement, operation_id.clone(), expected)?,
                    workspace_handle: workspace_handle(placement)?.to_owned(),
                    purpose: spec.purpose,
                    command: spec.command.clone(),
                    env: spec
                        .env
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect(),
                    timeout_secs: spec.timeout_secs,
                    max_output_bytes: if spec.max_output_bytes == usize::MAX {
                        u64::MAX
                    } else {
                        spec.max_output_bytes as u64
                    },
                },
            )
            .await
            .map_err(|error| Self::error(placement, Some(spec.purpose), error))?;
        if let Some(attempt) = &attempt {
            self.db
                .finish_remote_task_operation(attempt, &operation_id)
                .await?;
        }
        if result.timed_out {
            return Err(ServiceError::invalid_operation("review command timed out").into());
        }
        if spec.purpose == WorkspaceRunPurpose::CiStep
            && spec.max_output_bytes != usize::MAX
            && (result.stdout_truncated || result.stderr_truncated)
        {
            return Err(ServiceError::invalid_operation(
                "review command output exceeds size budget",
            )
            .into());
        }
        let mut stdout = result.stdout;
        let mut stderr = result.stderr;
        if spec.purpose == WorkspaceRunPurpose::CiStep {
            stdout = executors::environment::redact_environment_values(&stdout, &spec.env);
            stderr = executors::environment::redact_environment_values(&stderr, &spec.env);
        }
        Ok(RunResult {
            exit_code: result.exit_code.unwrap_or(-1),
            stdout_tail: stdout,
            stderr_tail: stderr,
            duration_ms: result.duration_ms,
        })
    }

    async fn diff(&self, placement: &WorkspacePlacement, spec: &DiffSpec) -> Result<Diff> {
        let result = self
            .client
            .diff(
                self.owner(placement)?.0,
                WorkspaceDiffParams {
                    workspace: self.reference(placement)?,
                    base_ref: spec.base_ref.clone(),
                    head_ref: spec.head_ref.clone(),
                    max_bytes: spec.max_bytes as u64,
                },
            )
            .await
            .map_err(|error| Self::error(placement, None, error))?;
        Ok(Diff {
            response: api_types::DiffResponse {
                base_ref: result.base_ref,
                head_ref: result.head_ref,
                base_sha: result.base_sha,
                head_sha: result.head_sha,
                files: result
                    .files
                    .into_iter()
                    .map(|file| api_types::FileDiffSummary {
                        path: file.path,
                        status: match file.status {
                            WorkspaceDiffFileStatus::Added => api_types::DiffFileStatus::Added,
                            WorkspaceDiffFileStatus::Modified => {
                                api_types::DiffFileStatus::Modified
                            }
                            WorkspaceDiffFileStatus::Deleted => api_types::DiffFileStatus::Deleted,
                            WorkspaceDiffFileStatus::Renamed => api_types::DiffFileStatus::Renamed,
                        },
                        additions: file.additions,
                        deletions: file.deletions,
                    })
                    .collect(),
                stats: api_types::DiffStats {
                    files_changed: result.stats.files_changed,
                    total_additions: result.stats.total_additions,
                    total_deletions: result.stats.total_deletions,
                },
                diff: result.diff,
            },
            truncated: result.truncated,
        })
    }

    async fn read(
        &self,
        placement: &WorkspacePlacement,
        rel_path: &str,
        limit: u64,
    ) -> Result<Vec<u8>> {
        let result = self
            .client
            .read(
                self.owner(placement)?.0,
                WorkspaceReadParams {
                    workspace: self.reference(placement)?,
                    path: rel_path.to_owned(),
                    limit,
                },
            )
            .await
            .map_err(|error| match error {
                WorkspaceClientError::Daemon(error)
                    if error.code == api_types::WORKSPACE_FILE_NOT_FOUND =>
                {
                    ServiceError::invalid_operation("plan artifact not found").into()
                }
                error => Self::error(placement, None, error),
            })?;
        Ok(result.bytes)
    }

    async fn merge(
        &self,
        placement: &WorkspacePlacement,
        spec: &MergeSpec,
    ) -> Result<MergeOutcome> {
        use db::{RepoRepo, ReviewConformanceRepo, TaskRepo};

        let _integration_lock = self
            .integration_locks
            .acquire(&placement.repo_location_id)
            .await;
        let workspace = self.workspace(placement).await?;
        RepoRepo::get_by_id(&*self.db, &workspace.repo_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("repo", &workspace.repo_id))?;
        let task = TaskRepo::get_by_id(&*self.db, &placement.task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", &placement.task_id))?;
        if task.parent_task_id.is_some() {
            return Err(ServiceError::invalid_operation("subtasks do not merge").into());
        }
        let execution = crate::task_service::latest_executor_execution_for_task(&self.db, &task)
            .await?
            .ok_or_else(|| {
                ServiceError::invalid_operation("merge has no implementation execution")
            })?;
        if execution.workspace_id.as_deref() != Some(&workspace.id) {
            return Err(
                ServiceError::conflict("merge differs from the implementation workspace").into(),
            );
        }
        let expected = self.current_expected(placement).await?;
        self.db.protect_step_integration().await?;
        let guard = match self.db.lock_review_integration(&placement.task_id).await {
            Ok(guard) => guard,
            Err(db::DbError::Check(reason)) => return Ok(MergeOutcome::ReviewRequired { reason }),
            Err(error) => return Err(error.into()),
        };
        let contract = guard.contract.clone();
        if let Some(contract) = &contract {
            if expected
                != (WorkspaceOperationExpected::BaseSha {
                    sha: contract.commit_sha.clone(),
                })
            {
                return Ok(MergeOutcome::ReviewRequired {
                    reason: "reviewed commit changed since review; fresh review required".into(),
                });
            }
        }
        let target_sha = contract.as_ref().map_or_else(
            || spec.expected_target_sha.clone(),
            |contract| contract.base_sha.clone(),
        );
        if target_sha.is_empty() {
            return Err(ServiceError::invalid_operation(
                "remote merge requires an expected target SHA",
            )
            .into());
        }
        // Persist the intent without holding the authority transaction, then
        // reacquire and compare the frozen authority before sending the RPC.
        guard.release().await?;
        let params = WorkspaceReviewedMergeParams {
            merge: WorkspaceMergeParams {
                fence: self.fence(placement, db::new_uuid_v4(), expected)?,
                workspace_handle: workspace_handle(placement)?.to_owned(),
                repo_location_id: placement.repo_location_id.clone(),
                target_branch: spec.target_branch.clone(),
                expected_target_sha: target_sha,
                handed_off_paths: spec.handed_off_paths.clone(),
            },
            reviewed_commit_sha: contract
                .as_ref()
                .map(|contract| contract.commit_sha.clone()),
        };
        let daemon_id = self.owner(placement)?.0;

        let request = serde_json::to_value(&params).expect("merge serializes");
        let retained = self
            .client
            .retained_merge_result(&request, &execution.id)
            .await
            .map_err(|error| Self::error(placement, None, error))?;
        let (request, retained_result) = match retained {
            Some((request, result)) => (request, Some(result)),
            None => (
                self.client
                    .remember_mutation(
                        daemon_id,
                        api_types::METHOD_WORKSPACE_MERGE,
                        request,
                        Some(&execution.id),
                    )
                    .await
                    .map_err(|error| Self::error(placement, None, error))?,
                None,
            ),
        };
        let params: WorkspaceReviewedMergeParams = serde_json::from_value(request.clone())
            .map_err(|error| {
                ServiceError::invalid_operation(format!("invalid retained merge: {error}"))
            })?;
        self.db.protect_step_integration().await?;
        let guard = match self.db.lock_review_integration(&placement.task_id).await {
            Ok(guard) => guard,
            Err(db::DbError::Check(reason)) => return Ok(MergeOutcome::ReviewRequired { reason }),
            Err(error) => return Err(error.into()),
        };
        if guard.contract != contract {
            return Ok(MergeOutcome::ReviewRequired {
                reason: "review authority changed before integration".into(),
            });
        }
        // The socket reader persists heartbeats before reading RPC replies.
        // Keep the frozen authority check outside the transport exchange so
        // its SQLite write lock cannot block the reply behind a heartbeat.
        guard.release().await?;
        let attempt = db::task_writer::current_task_step();
        if let Some(step) = &attempt {
            self.db
                .register_remote_task_operation(step, placement, &params.merge.fence.operation_id)
                .await?;
        }
        let result = match retained_result {
            Some(result) => Ok(result),
            None => self.client.merge(daemon_id, params.clone()).await,
        };
        let result = match result {
            Ok(result) => result,
            Err(WorkspaceClientError::Daemon(error)) => {
                self.client
                    .retain_error(
                        daemon_id,
                        api_types::METHOD_WORKSPACE_MERGE,
                        &request,
                        &error,
                    )
                    .await
                    .map_err(|error| Self::error(placement, None, error))?;
                return Err(Self::error(
                    placement,
                    None,
                    WorkspaceClientError::Daemon(error),
                ));
            }
            Err(error) => return Err(Self::error(placement, None, error)),
        };
        self.record_merge_reply(
            placement,
            daemon_id,
            MergeReplyRecord {
                request: &request,
                result: &result,
                execution_id: &execution.id,
                attempt: attempt.as_ref(),
                operation_id: &params.merge.fence.operation_id,
            },
        )
        .await?;
        Ok(crate::integration_effects::merge::owner_merge_outcome(
            result.outcome,
            &spec.target_branch,
        ))
    }

    async fn reset(
        &self,
        placement: &WorkspacePlacement,
        spec: &ResetSpec,
    ) -> Result<PreparedWorkspace> {
        // The caller supplies the next generation and persists it after recreation succeeds.
        let workspace = self.workspace(placement).await?;
        let daemon_id = self.owner(placement)?.0;
        self.client
            .reconcile_pending_operations(daemon_id)
            .await
            .map_err(|error| Self::error(placement, None, error))?;
        self.client
            .retry_acknowledgements(daemon_id)
            .await
            .map_err(|error| Self::error(placement, None, error))?;
        let operation_id = operation_id(placement, api_types::METHOD_WORKSPACE_RESET);
        let result = self
            .client
            .reset(
                self.owner(placement)?.0,
                WorkspaceResetParams {
                    fence: self.fence(
                        placement,
                        operation_id.clone(),
                        WorkspaceOperationExpected::BaseSha {
                            sha: spec.expected_head_sha.clone(),
                        },
                    )?,
                    workspace_handle: workspace_handle(placement)?.to_owned(),
                    base_ref: spec.base_ref.clone(),
                    branch: workspace.branch,
                },
            )
            .await
            .map_err(|error| Self::error(placement, None, error))?;
        self.check_operation(&operation_id, &result.operation_id)?;
        self.prepared(placement, result.workspace)
    }

    async fn cleanup(&self, placement: &WorkspacePlacement) -> Result<CleanupAck> {
        if self
            .db
            .daemon_removed(self.owner(placement)?.0)
            .await
            .map_err(ServiceError::from)?
        {
            // Owner removal abandons its physical files. The normal cleanup
            // path can retire the server's bookkeeping without an owner RPC.
            return Ok(CleanupAck { removed: false });
        }
        // Cleaning is the durable cleanup intent. A replayed, acknowledged
        // cleanup may already have retired the owner's handle.
        let unknown_handle = |error: &WorkspaceBackendError| {
            matches!(
                placement.state,
                db::PlacementState::Cleaning | db::PlacementState::Cleaned
            ) && super::is_unknown_workspace_handle(error)
        };
        let state = match self.describe(placement).await {
            Ok(state) => state,
            Err(error) if unknown_handle(&error) => return Ok(CleanupAck { removed: false }),
            Err(error) => return Err(error),
        };
        let sha = match state.head_sha {
            Some(sha) => Some(sha),
            None => self.workspace(placement).await?.before_sha,
        };
        let expected = match sha {
            Some(sha) => WorkspaceOperationExpected::BaseSha { sha },
            None => WorkspaceOperationExpected::Version {
                version: placement.version,
            },
        };
        let operation_id = db::new_uuid_v4();
        let result = match self
            .client
            .cleanup(
                self.owner(placement)?.0,
                WorkspaceCleanupParams {
                    fence: self.fence(placement, operation_id.clone(), expected)?,
                    workspace_handle: workspace_handle(placement)?.to_owned(),
                },
            )
            .await
            .map_err(|error| Self::error(placement, None, error))
        {
            Ok(result) => result,
            Err(error) if unknown_handle(&error) => return Ok(CleanupAck { removed: false }),
            Err(error) => return Err(error),
        };
        self.check_generation(placement, result.generation)?;
        if result.workspace_handle != workspace_handle(placement)? {
            return Err(WorkspaceBackendError::WrongOwner {
                placement_id: placement.id.clone(),
            });
        }
        if !result.cleaned {
            return Err(ServiceError::invalid_operation(
                "daemon has not acknowledged workspace cleanup",
            )
            .into());
        }
        Ok(CleanupAck { removed: true })
    }

    async fn harvest_outbox(
        &self,
        placement: &WorkspacePlacement,
        _execution_id: &str,
    ) -> Result<OutboxHarvest> {
        self.owner(placement)?;
        // Daemon outbox entries are carried by its retained execution.terminal report.
        Ok(OutboxHarvest::default())
    }

    async fn consume_outbox(
        &self,
        placement: &WorkspacePlacement,
        _execution_id: &str,
    ) -> Result<()> {
        self.owner(placement)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use db::{PlacementSelectedBy, PlacementState};
    use serde_json::json;

    use super::*;
    use crate::daemon_transport::workspace_client::tests::{
        rejection, Reply, ScriptedDaemon, DAEMON_ID,
    };

    fn placement() -> WorkspacePlacement {
        WorkspacePlacement {
            id: "placement-test".to_owned(),
            workspace_id: "workspace-test".to_owned(),
            task_id: "task-test".to_owned(),
            agent_id: None,
            owner_kind: PlacementOwnerKind::Daemon,
            daemon_id: Some(DAEMON_ID.to_owned()),
            runtime_id: Some("runtime-test".to_owned()),
            repo_location_id: "location-test".to_owned(),
            execution_daemon_id: Some(DAEMON_ID.to_owned()),
            workspace_handle: Some("workspace-handle".to_owned()),
            generation: 1,
            state: PlacementState::Ready,
            selected_by: PlacementSelectedBy::Scheduler,
            selection_reason: "{}".to_owned(),
            reserved_until: None,
            disconnected_at: None,
            failure_cause: None,
            version: 3,
            created_at: "now".to_owned(),
            updated_at: "now".to_owned(),
        }
    }

    async fn backend(daemon: &ScriptedDaemon) -> DaemonWorkspaceBackend {
        let pool = db::create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        db::run_migrations(&pool)
            .await
            .expect("workspace cancellation tables exist");
        DaemonWorkspaceBackend {
            db: Arc::new(SqliteDb::new(pool)),
            client: DaemonWorkspaceClient::new(Arc::clone(&daemon.registry)),
            integration_locks: workspace::RepoCacheLockManager::new(),
        }
    }

    #[tokio::test]
    async fn merge_reply_is_not_blocked_by_heartbeat_persistence() {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = Arc::new(SqliteDb::new(pool));
        sqlx::raw_sql(
            "INSERT INTO project (id, name, created_at, updated_at)
             VALUES ('project-test', 'merge', '2026-09-30T00:00:00Z', '2026-09-30T00:00:00Z');
             INSERT INTO repo (id, project_id, name, remote_url, default_branch, created_at, updated_at)
             VALUES ('repo-test', 'project-test', 'repo', '', 'main', '2026-09-30T00:00:00Z', '2026-09-30T00:00:00Z');
             UPDATE project SET primary_repo_id = 'repo-test' WHERE id = 'project-test';
             INSERT INTO task (id, project_id, title, status, created_at, updated_at)
             VALUES ('task-test', 'project-test', 'merge', 'merging', '2026-09-30T00:00:00Z', '2026-09-30T00:00:00Z');
             INSERT INTO workspace (id, task_id, repo_id, worktree_path, branch, status, created_at, updated_at)
             VALUES ('workspace-test', 'task-test', 'repo-test', '', 'task/test', 'ready', '2026-09-30T00:00:00Z', '2026-09-30T00:00:00Z');
             INSERT INTO execution (id, task_id, role, status, workspace_id, after_sha, created_at, updated_at)
             VALUES ('execution-test', 'task-test', 'coder', 'completed', 'workspace-test', 'candidate', '2026-09-30T00:00:00Z', '2026-09-30T00:00:00Z');
             INSERT INTO daemon (id, machine_id, hostname, os, arch, status, created_at, updated_at)
             VALUES ('daemon-test', 'machine-test', 'host', 'linux', 'x86_64', 'online', '2026-09-30T00:00:00Z', '2026-09-30T00:00:00Z');
             INSERT INTO runtime (id, daemon_id, kind, workspace_root, status, created_at, updated_at)
             VALUES ('runtime-test', 'daemon-test', 'local', '/owner', 'ready', '2026-09-30T00:00:00Z', '2026-09-30T00:00:00Z');
             INSERT INTO repo_location (id, repo_id, owner_kind, daemon_id, runtime_id, path, kind, status, created_at, updated_at)
             VALUES ('location-test', 'repo-test', 'daemon', 'daemon-test', 'runtime-test', '/owner/repo', 'primary_checkout', 'ready', '2026-09-30T00:00:00Z', '2026-09-30T00:00:00Z');
             INSERT INTO workspace_placement (id, workspace_id, task_id, owner_kind, daemon_id, runtime_id, repo_location_id,
                execution_daemon_id, workspace_handle, generation, state, selected_by, selection_reason, created_at, updated_at)
             VALUES ('placement-test', 'workspace-test', 'task-test', 'daemon', 'daemon-test', 'runtime-test', 'location-test',
                'daemon-test', 'workspace-handle', 1, 'ready', 'scheduler', '{}', '2026-09-30T00:00:00Z', '2026-09-30T00:00:00Z');",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
        let (connection, mut outbound) =
            crate::daemon_transport::DaemonConnection::new(DAEMON_ID.to_owned());
        registry.register(DAEMON_ID.to_owned(), connection);
        let capabilities: Vec<_> = api_types::DAEMON_REQUIRED_CAPABILITIES
            .iter()
            .copied()
            .chain(std::iter::once(api_types::DAEMON_CAPABILITY_WORKSPACE))
            .collect();
        registry.dispatch_incoming(DAEMON_ID, api_types::DaemonFrame::Notification {
            method: api_types::METHOD_DAEMON_HANDSHAKE.to_owned(),
            params: json!({"protocol_revision": api_types::DAEMON_PROTOCOL_REVISION, "capabilities": capabilities}),
        });
        let replies = Arc::clone(&registry);
        let heartbeat_db = Arc::clone(&db);
        let peer = tokio::spawn(async move {
            while let Some(api_types::DaemonFrame::Request { id, method, params }) =
                outbound.recv().await
            {
                let result = match method.as_str() {
                    api_types::METHOD_WORKSPACE_DESCRIBE => {
                        json!({"workspace_handle": "workspace-handle", "exists": true, "head_sha": "candidate", "dirty": false,
                        "branch": "task/test", "locked": false, "generation": 1, "active_execution_ids": [], "journaled_execution_ids": []})
                    }
                    api_types::METHOD_WORKSPACE_MERGE => {
                        // The real socket reader touches the daemon before
                        // dispatching a response queued behind a heartbeat.
                        let mut heartbeat = db::begin_immediate(heartbeat_db.pool()).await.unwrap();
                        sqlx::query("UPDATE daemon SET last_report_at = ? WHERE id = ?")
                            .bind(db::now_rfc3339())
                            .bind(DAEMON_ID)
                            .execute(&mut *heartbeat)
                            .await
                            .unwrap();
                        heartbeat.commit().await.unwrap();
                        json!({"entry_id": "merge-entry", "operation_id": params["operation_id"],
                            "outcome": {"kind": "done", "before_sha": "target", "after_sha": "candidate", "branch": "main"},
                            "diffstat": {"files_changed": 1, "total_additions": 1, "total_deletions": 0}})
                    }
                    api_types::METHOD_JOURNAL_ACK => {
                        json!({"entry_id": params["entry_id"], "acknowledged": true})
                    }
                    other => panic!("unexpected workspace request: {other}"),
                };
                replies
                    .dispatch_incoming(DAEMON_ID, api_types::DaemonFrame::Response { id, result });
            }
        });
        let backend = DaemonWorkspaceBackend::new(Arc::clone(&db), registry);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            backend.merge(
                &placement(),
                &MergeSpec {
                    target_branch: "main".to_owned(),
                    expected_target_sha: "target".to_owned(),
                    handed_off_paths: Vec::new(),
                },
            ),
        )
        .await;
        peer.abort();
        assert_eq!(
            result
                .expect("heartbeat persistence must not block merge replies")
                .unwrap(),
            MergeOutcome::Done {
                before_sha: "target".to_owned(),
                after_sha: "candidate".to_owned(),
                branch: "main".to_owned(),
            }
        );
    }

    #[test]
    fn prepare_identity_is_stable_across_lifecycle_versions() {
        let placement = placement();
        let mut retry = placement.clone();
        retry.version += 1;
        assert_eq!(
            operation_id(&placement, api_types::METHOD_WORKSPACE_PREPARE),
            operation_id(&retry, api_types::METHOD_WORKSPACE_PREPARE)
        );
        retry.generation += 1;
        assert_ne!(
            operation_id(&placement, api_types::METHOD_WORKSPACE_PREPARE),
            operation_id(&retry, api_types::METHOD_WORKSPACE_PREPARE)
        );
    }

    #[tokio::test]
    async fn missing_workspace_file_maps_to_embedded_not_found_result() {
        let daemon = ScriptedDaemon::new(vec![rejection(
            api_types::WORKSPACE_FILE_NOT_FOUND,
            "workspace file not found",
            None,
        )]);
        let backend = backend(&daemon).await;
        assert!(
            matches!(backend.read(&placement(), "../plan.md", 1024).await,
            Err(WorkspaceBackendError::Other(error))
                if matches!(&*error, ServiceError::InvalidOperation { message }
                    if message == "plan artifact not found"))
        );
        assert_eq!(daemon.requests().len(), 1);
        daemon.finish().await;
    }

    #[tokio::test]
    async fn stale_generation_maps_to_backend_error() {
        let daemon = ScriptedDaemon::new(vec![rejection(
            api_types::STALE_GENERATION,
            "generation is stale",
            Some(json!({ "current_generation": 2 })),
        )]);
        let backend = backend(&daemon).await;
        assert!(matches!(backend.describe(&placement()).await,
            Err(WorkspaceBackendError::StaleGeneration { expected: 1, actual: 2, placement_id })
                if placement_id == "placement-test"));
        assert_eq!(daemon.requests().len(), 1);
        daemon.finish().await;
    }

    #[tokio::test]
    async fn purpose_denied_maps_without_retrying_run() {
        let daemon = ScriptedDaemon::new(vec![
            Reply::Describe,
            rejection(
                api_types::PURPOSE_DENIED,
                "hooks disabled by local policy",
                None,
            ),
        ]);
        let backend = backend(&daemon).await;
        let spec = RunSpec {
            purpose: WorkspaceRunPurpose::Hook,
            command: "configured-hook".to_owned(),
            env: BTreeMap::new(),
            timeout_secs: 1,
            max_output_bytes: 1024,
        };
        assert!(matches!(
            backend.run(&placement(), &spec).await,
            Err(WorkspaceBackendError::PurposeDenied {
                purpose: WorkspaceRunPurpose::Hook
            })
        ));
        let requests = daemon.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].method, api_types::METHOD_WORKSPACE_RUN);
        assert_eq!(requests[1].params["purpose"], "hook");
        daemon.finish().await;
    }

    #[tokio::test]
    async fn wrong_owner_maps_to_backend_error() {
        let daemon = ScriptedDaemon::new(vec![rejection(
            api_types::WRONG_OWNER,
            "wrong daemon",
            None,
        )]);
        let backend = backend(&daemon).await;
        assert!(matches!(backend.describe(&placement()).await,
            Err(WorkspaceBackendError::WrongOwner { placement_id }) if placement_id == "placement-test"));
        daemon.finish().await;
    }

    #[tokio::test]
    async fn outside_workspace_root_keeps_the_guardrail_code() {
        let daemon = ScriptedDaemon::new(vec![rejection(
            api_types::OUTSIDE_WORKSPACE_ROOT,
            "escaped handle",
            None,
        )]);
        let backend = backend(&daemon).await;
        let error = backend
            .describe(&placement())
            .await
            .expect_err("escaped handle rejected");
        let WorkspaceBackendError::Other(error) = error else {
            panic!("expected mapped service error")
        };
        assert!(
            matches!(*error, ServiceError::InvalidOperation { message } if message.starts_with(api_types::OUTSIDE_WORKSPACE_ROOT))
        );
        daemon.finish().await;
    }

    #[tokio::test]
    async fn cleanup_acknowledged_handle_is_already_cleaned_only_with_intent() {
        for state in [
            db::PlacementState::Cleaning,
            db::PlacementState::Cleaned,
            db::PlacementState::Ready,
        ] {
            let daemon = ScriptedDaemon::new(vec![rejection(
                "invalid_input",
                "unknown workspace_handle",
                None,
            )]);
            let backend = backend(&daemon).await;
            let mut placement = placement();
            placement.state = state;
            let result = backend.cleanup(&placement).await;
            if placement.state != db::PlacementState::Ready {
                assert!(!result.unwrap().removed);
            } else {
                assert!(result.is_err());
            }
            daemon.finish().await;
        }
    }

    #[tokio::test]
    async fn successive_runs_get_fresh_fenced_operation_ids() {
        let daemon = ScriptedDaemon::new(vec![
            Reply::Describe,
            Reply::Run,
            Reply::Describe,
            Reply::Run,
        ]);
        let backend = backend(&daemon).await;
        let spec = RunSpec {
            purpose: WorkspaceRunPurpose::CiStep,
            command: "configured-check".to_owned(),
            env: BTreeMap::from([("CONFIG".to_owned(), "value".to_owned())]),
            timeout_secs: 1,
            max_output_bytes: 1024,
        };
        for _ in 0..2 {
            let result = backend
                .run(&placement(), &spec)
                .await
                .expect("run succeeds");
            assert_eq!(result.exit_code, 0);
            assert_eq!(result.stdout_tail, "ok");
        }
        let requests = daemon.requests();
        assert_eq!(requests.len(), 4);
        assert_ne!(
            requests[1].params["operation_id"],
            requests[3].params["operation_id"]
        );
        assert_eq!(
            requests[1].params["expected"],
            json!({ "kind": "base_sha", "sha": "head-sha" })
        );
        assert_eq!(requests[1].params["daemon_id"], DAEMON_ID);
        assert_eq!(requests[1].params["runtime_id"], "runtime-test");
        assert_eq!(requests[1].params["placement_id"], "placement-test");
        assert_eq!(requests[1].params["workspace_handle"], "workspace-handle");
        assert_eq!(requests[1].params["generation"], 1);
        daemon.finish().await;
    }

    #[tokio::test]
    async fn describe_preserves_reconciliation_execution_ids() {
        let daemon = ScriptedDaemon::new(vec![Reply::Value(json!({
            "workspace_handle": "workspace-handle", "generation": 1, "exists": true,
            "head_sha": "head-sha", "dirty": false, "branch": "task/test", "locked": true,
            "active_execution_ids": ["active"], "journaled_execution_ids": ["finished"],
        }))]);
        let state = backend(&daemon)
            .await
            .describe(&placement())
            .await
            .expect("describe succeeds");
        assert!(state.locked);
        assert_eq!(state.active_execution_ids, vec!["active"]);
        assert_eq!(state.journaled_execution_ids, vec!["finished"]);
        daemon.finish().await;
    }
    #[tokio::test]
    async fn daemon_transport_review_command_limits_are_permanent_failures() {
        for (timed_out, truncated, expected) in [
            (true, false, "review command timed out"),
            (false, true, "review command output exceeds size budget"),
        ] {
            let daemon = ScriptedDaemon::new(vec![
                Reply::Describe,
                Reply::RunLimits {
                    timed_out,
                    truncated,
                },
            ]);
            let spec = RunSpec {
                purpose: WorkspaceRunPurpose::CiStep,
                command: "ci".into(),
                env: BTreeMap::new(),
                timeout_secs: 1,
                max_output_bytes: 1024,
            };
            let error = backend(&daemon)
                .await
                .run(&placement(), &spec)
                .await
                .unwrap_err();
            assert!(matches!(error, WorkspaceBackendError::Other(ref service)
                if matches!(**service, ServiceError::InvalidOperation { ref message } if message == expected)));
            assert_eq!(daemon.requests().len(), 2);
            daemon.finish().await;
        }
    }

    #[test]
    fn daemon_transport_review_interrupted_command_is_not_owner_unreachable() {
        let error = DaemonWorkspaceBackend::error(
            &placement(),
            Some(WorkspaceRunPurpose::CiStep),
            WorkspaceClientError::Daemon(api_types::DaemonErrorPayload {
                code: api_types::DAEMON_UNAVAILABLE.into(),
                message: "command interrupted".into(),
                details: Some(json!({"interrupted": true})),
            }),
        );
        assert!(matches!(error, WorkspaceBackendError::Other(_)));
    }
}
