//! Review and filesystem I/O supplied by the persisted workspace owner.

use super::{ResolvedWorkspace, Result, RunSpec, WorkspaceBackendError, WorkspaceRunPurpose};
use crate::{daemon_transport::workspace_client::DaemonWorkspaceClient, ServiceError};
use ::review::{CommandLimits, CommandOutput, ReviewWorkspace};
use api_types::*;
use std::{collections::BTreeMap, path::PathBuf};

impl ResolvedWorkspace {
    /// A Forge-driven rebase is authoritative HEAD evidence, independent of
    /// the worker's earlier terminal SHA. Fence it to this physical workspace.
    async fn read_rebase_head_facts(
        &self,
        db: &db::SqliteDb,
    ) -> Result<Option<crate::integration_effects::rebase::RebaseHeadFacts>> {
        if self.placement.owner_kind == db::PlacementOwnerKind::Server {
            let Some(daemon_id) = self.placement.execution_daemon_id.as_deref() else {
                return Ok(None);
            };
            if db::DaemonRepo::get_by_id(db, daemon_id)
                .await?
                .is_none_or(|daemon| {
                    crate::embedded_daemon::is_embedded_daemon_machine(&daemon.machine_id)
                })
            {
                return Ok(None);
            }
        }
        Ok(Some(crate::integration_effects::rebase::rebase_head_facts(
            self.backend.describe(&self.placement).await?,
        )?))
    }

    /// Project the supplied owner facts without issuing another Git/RPC read.
    pub(crate) async fn record_rebase_head(
        &self,
        db: &db::SqliteDb,
        facts: crate::integration_effects::rebase::RebaseHeadFacts,
    ) -> Result<()> {
        let changed = sqlx::query(
            "INSERT INTO workspace_expected_head (placement_id, generation, head_sha, recorded_at)
             SELECT id, generation, ?, ? FROM workspace_placement
             WHERE id = ? AND generation = ? AND version = ?
             ON CONFLICT(placement_id) DO UPDATE SET generation = excluded.generation,
                 head_sha = excluded.head_sha, recorded_at = excluded.recorded_at",
        )
        .bind(facts.head_sha)
        .bind(db::now_rfc3339())
        .bind(&self.placement.id)
        .bind(self.placement.generation)
        .bind(self.placement.version)
        .execute(db.pool())
        .await
        .map_err(ServiceError::from)?;
        if changed.rows_affected() != 1 {
            return Err(db::DbError::VersionConflict.into());
        }
        Ok(())
    }

    pub(crate) async fn record_head_best_effort(&self, db: &db::SqliteDb) {
        let recorded = async {
            if let Some(facts) = self.read_rebase_head_facts(db).await? {
                self.record_rebase_head(db, facts).await?;
            }
            Ok::<_, WorkspaceBackendError>(())
        }
        .await;
        if let Err(error) = recorded {
            tracing::warn!(placement_id = %self.placement.id,
                daemon_id = ?self.placement.daemon_id.as_ref().or(self.placement.execution_daemon_id.as_ref()),
                %error, "could not record Forge-established workspace HEAD");
        }
    }

    pub(crate) fn owner_client(&self) -> Result<&DaemonWorkspaceClient> {
        self.backend
            .daemon_client()
            .ok_or_else(|| WorkspaceBackendError::OwnerUnsupported {
                owner_kind: self.placement.owner_kind.clone(),
            })
    }

    pub(crate) fn owner_reference(&self) -> Result<WorkspaceHandleReference> {
        let placement = &self.placement;
        if placement.owner_kind != db::PlacementOwnerKind::Daemon {
            return Err(WorkspaceBackendError::WrongOwner {
                placement_id: placement.id.clone(),
            });
        }
        Ok(WorkspaceHandleReference {
            daemon_id: placement
                .daemon_id
                .clone()
                .ok_or_else(|| ServiceError::invalid_operation("placement has no daemon"))?,
            runtime_id: placement
                .runtime_id
                .clone()
                .ok_or_else(|| ServiceError::invalid_operation("placement has no runtime"))?,
            placement_id: placement.id.clone(),
            workspace_handle: self.handle()?.to_owned(),
            generation: u64::try_from(placement.generation)
                .ok()
                .filter(|generation| *generation > 0)
                .ok_or_else(|| {
                    ServiceError::invalid_operation("placement generation is invalid")
                })?,
        })
    }

    pub(crate) fn owner_error(
        &self,
        error: crate::daemon_transport::workspace_client::WorkspaceClientError,
    ) -> WorkspaceBackendError {
        super::daemon::DaemonWorkspaceBackend::error(&self.placement, None, error)
    }

    pub async fn owner_paths(&self) -> Result<(String, String)> {
        if self.placement.owner_kind == db::PlacementOwnerKind::Server {
            let path = self.embedded_path()?.to_string_lossy().into_owned();
            return Ok((path.clone(), path));
        }
        let reference = self.owner_reference()?;
        match self
            .owner_client()?
            .inspect(
                &reference.daemon_id,
                WorkspaceInspectParams::Paths {
                    workspace: reference.clone(),
                },
            )
            .await
            .map_err(|error| self.owner_error(error))?
        {
            WorkspaceInspectResult::Paths {
                workspace_path,
                repo_path,
            } if !workspace_path.is_empty() && !repo_path.is_empty() => {
                Ok((workspace_path, repo_path))
            }
            _ => Err(
                ServiceError::invalid_operation("owner returned invalid workspace paths").into(),
            ),
        }
    }

    /// Used by recovery and supervisor guards as well as review evidence.
    pub async fn git_query(
        &self,
        query: WorkspaceGitQuery,
        optional: bool,
    ) -> Result<Option<String>> {
        if self.placement.owner_kind == db::PlacementOwnerKind::Server {
            let path = self.embedded_path()?;
            if query == WorkspaceGitQuery::RebaseInProgress {
                return Ok(Some(
                    git::detect_rebase_in_progress(&path).await?.to_string(),
                ));
            }
            if let WorkspaceGitQuery::MarkerPaths { base, head } = &query {
                return git::paths_adding_conflict_markers(&path, base, head)
                    .await
                    .map(|paths| Some(paths.join("\n")))
                    .map_err(Into::into);
            }
            let args = git_query_args(&query);
            return path
                .git_read(
                    &args.iter().map(String::as_str).collect::<Vec<_>>(),
                    optional,
                )
                .await
                .map_err(|error| ServiceError::invalid_operation(error).into());
        }
        let reference = self.owner_reference()?;
        match self
            .owner_client()?
            .inspect(
                &reference.daemon_id,
                WorkspaceInspectParams::Git {
                    workspace: reference.clone(),
                    query,
                    optional,
                    limit: 1024 * 1024,
                },
            )
            .await
            .map_err(|error| self.owner_error(error))?
        {
            WorkspaceInspectResult::Git { output } => Ok(output),
            _ => Err(ServiceError::invalid_operation("owner returned invalid Git evidence").into()),
        }
    }

    pub async fn materialize_assets(
        &self,
        environment: &api_types::ProjectEnvironment,
    ) -> Result<()> {
        if self.placement.owner_kind == db::PlacementOwnerKind::Server {
            executors::environment::materialize_assets(&self.embedded_path()?, &environment.assets)
                .await
                .map_err(|error| ServiceError::invalid_operation(error).into())
        } else {
            self.apply_owner_change(WorkspaceOwnerOperation::MaterializeAssets {
                environment: environment.clone(),
            })
            .await?;
            Ok(())
        }
    }

    pub async fn read_files(
        &self,
        relative: &str,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<WorkspaceFileContent>> {
        if self.placement.owner_kind == db::PlacementOwnerKind::Daemon {
            return self
                .read_owner_files(relative, max_entries, max_bytes)
                .await;
        }
        let root = self.embedded_path()?.canonicalize()?;
        let directory = root.join(relative);
        if !tokio::fs::try_exists(&directory).await? {
            return Ok(Vec::new());
        }
        if !directory.canonicalize()?.starts_with(&root) {
            return Err(
                ServiceError::invalid_operation("workspace file read escapes its owner").into(),
            );
        }
        let mut pending = vec![directory];
        let mut files = Vec::new();
        let mut bytes: usize = 0;
        while let Some(directory) = pending.pop() {
            let mut entries = tokio::fs::read_dir(directory).await?;
            while let Some(entry) = entries.next_entry().await? {
                let kind = entry.file_type().await?;
                if kind.is_dir() {
                    pending.push(entry.path());
                }
                if !kind.is_file() {
                    continue;
                }
                let path = entry
                    .path()
                    .strip_prefix(&root)
                    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?
                    .to_string_lossy()
                    .into_owned();
                let content = self
                    .backend
                    .read(
                        &self.placement,
                        &path,
                        max_bytes.saturating_sub(bytes) as u64,
                    )
                    .await?;
                bytes += content.len();
                files.push(WorkspaceFileContent {
                    path,
                    bytes: content,
                });
                if files.len() > max_entries || bytes > max_bytes {
                    return Err(ServiceError::invalid_operation(
                        "workspace file read exceeds size budget",
                    )
                    .into());
                }
            }
        }
        files.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(files)
    }

    async fn read_owner_files(
        &self,
        relative: &str,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<WorkspaceFileContent>> {
        let reference = self.owner_reference()?;
        match self
            .owner_client()?
            .inspect(
                &reference.daemon_id,
                WorkspaceInspectParams::Files {
                    workspace: reference.clone(),
                    path: relative.into(),
                    repository: false,
                    max_entries: max_entries as u64,
                    max_bytes: max_bytes as u64,
                },
            )
            .await
            .map_err(|error| self.owner_error(error))?
        {
            WorkspaceInspectResult::Files { files }
                if files.len() <= max_entries
                    && files.iter().map(|file| file.bytes.len()).sum::<usize>() <= max_bytes =>
            {
                Ok(files)
            }
            _ => Err(
                ServiceError::invalid_operation("owner returned invalid workspace files").into(),
            ),
        }
    }

    async fn apply_owner_change(&self, operation: WorkspaceOwnerOperation) -> Result<()> {
        match self.apply_owner_operation(operation).await? {
            WorkspaceOwnerOperationOutcome::Applied => Ok(()),
            _ => Err(ServiceError::invalid_operation(
                "owner returned an invalid workspace change result",
            )
            .into()),
        }
    }

    pub async fn rebase_target(
        &self,
        target_branch: &str,
        handoff_conflicts: bool,
    ) -> Result<WorkspaceOwnerOperationOutcome> {
        if self.placement.owner_kind == db::PlacementOwnerKind::Daemon {
            return self
                .apply_owner_operation(WorkspaceOwnerOperation::RebaseTarget {
                    target_branch: target_branch.into(),
                    handoff_conflicts,
                })
                .await;
        }
        let path = self.embedded_path()?;
        let workspace = super::effect_workspace(&self.placement);
        Ok(crate::integration_effects::rebase::rebase(
            &crate::integration_effects::rebase::RebaseEffectInput {
                workspace: &workspace,
                worktree_path: &path,
                target_branch,
                handoff_conflicts,
                expected_head_sha: None,
                expected_target_sha: None,
                deadline: None,
            },
        )
        .await?)
    }

    pub(crate) async fn apply_plan_operation(
        &self,
        operation: WorkspaceOwnerOperation,
    ) -> Result<WorkspaceOwnerOperationOutcome> {
        let discard = matches!(&operation, WorkspaceOwnerOperation::DiscardPlan { .. });
        if discard && self.placement.state == db::PlacementState::Cleaned {
            return Ok(WorkspaceOwnerOperationOutcome::Applied);
        }
        let reference = self.owner_reference()?;
        let state = self
            .owner_client()?
            .describe_for_plan(
                &reference.daemon_id,
                api_types::WorkspaceDescribeParams {
                    workspace: reference.clone(),
                },
            )
            .await
            .map_err(|error| self.owner_error(error));
        let head = match state {
            Ok(state) if discard && !state.exists => {
                return Ok(WorkspaceOwnerOperationOutcome::Applied)
            }
            Ok(state) => state.head_sha.unwrap_or_default(),
            Err(error)
                if discard && crate::workspace_backend::is_unknown_workspace_handle(&error) =>
            {
                return Ok(WorkspaceOwnerOperationOutcome::Applied)
            }
            Err(error) => return Err(error),
        };
        let result = self
            .owner_client()?
            .owner_operation(
                &reference.daemon_id,
                WorkspaceOwnerOperationParams {
                    fence: WorkspaceMutationFence {
                        daemon_id: reference.daemon_id.clone(),
                        runtime_id: reference.runtime_id,
                        placement_id: reference.placement_id,
                        operation_id: db::new_uuid_v4(),
                        generation: reference.generation,
                        expected: WorkspaceOperationExpected::BaseSha { sha: head },
                    },
                    workspace_handle: reference.workspace_handle,
                    operation,
                },
            )
            .await
            .map_err(|error| self.owner_error(error))?;
        Ok(result.outcome)
    }

    pub(crate) async fn apply_owner_operation(
        &self,
        operation: WorkspaceOwnerOperation,
    ) -> Result<WorkspaceOwnerOperationOutcome> {
        let reference = self.owner_reference()?;
        let head = self
            .backend
            .describe(&self.placement)
            .await?
            .head_sha
            .ok_or_else(|| {
                ServiceError::invalid_operation("workspace has no HEAD for owner operation")
            })?;
        let result = self
            .owner_client()?
            .owner_operation(
                &reference.daemon_id,
                WorkspaceOwnerOperationParams {
                    fence: WorkspaceMutationFence {
                        daemon_id: reference.daemon_id.clone(),
                        runtime_id: reference.runtime_id,
                        placement_id: reference.placement_id,
                        operation_id: db::new_uuid_v4(),
                        generation: reference.generation,
                        expected: WorkspaceOperationExpected::BaseSha { sha: head },
                    },
                    workspace_handle: reference.workspace_handle,
                    operation,
                },
            )
            .await
            .map_err(|error| self.owner_error(error))?;
        Ok(result.outcome)
    }
}

#[async_trait::async_trait]
impl ReviewWorkspace for ResolvedWorkspace {
    async fn run(
        &self,
        command: &str,
        env: &BTreeMap<String, String>,
        limits: Option<CommandLimits>,
    ) -> std::result::Result<CommandOutput, ::review::ReviewError> {
        let output = self
            .backend
            .run(
                &self.placement,
                &RunSpec {
                    purpose: WorkspaceRunPurpose::CiStep,
                    command: command.to_owned(),
                    env: env.clone(),
                    timeout_secs: limits.map_or(0, |limits| limits.timeout_secs),
                    max_output_bytes: limits.map_or(usize::MAX, |limits| limits.max_output_bytes),
                },
            )
            .await
            .map_err(review_workspace_error)?;
        Ok(CommandOutput {
            exit_code: (output.exit_code >= 0).then_some(output.exit_code),
            stdout: output.stdout_tail,
            stderr: output.stderr_tail,
        })
    }
    async fn git_read(
        &self,
        args: &[&str],
        optional: bool,
    ) -> std::result::Result<Option<String>, String> {
        if self.placement.owner_kind == db::PlacementOwnerKind::Server {
            return self
                .embedded_path()
                .map_err(|error| error.to_string())?
                .git_read(args, optional)
                .await;
        }
        self.git_query(review_git_query(args)?, optional)
            .await
            .map_err(|error| error.to_string())
    }
    async fn diff(
        &self,
        default_branch: &str,
    ) -> std::result::Result<String, ::review::ReviewError> {
        if self.placement.owner_kind == db::PlacementOwnerKind::Server {
            return self
                .embedded_path()
                .map_err(review_workspace_error)?
                .diff(default_branch)
                .await;
        }
        let reference = self.owner_reference().map_err(review_workspace_error)?;
        self.owner_client()
            .map_err(review_workspace_error)?
            .review_diff(
                &reference.daemon_id,
                WorkspaceReviewDiffParams {
                    workspace: reference.clone(),
                    operation: WorkspaceReviewDiffOperation::Review,
                    default_branch: default_branch.into(),
                    max_bytes: 64 * 1024,
                },
            )
            .await
            .map(|result| result.diff)
            .map_err(|error| review_workspace_error(self.owner_error(error)))
    }
    async fn clean_checkout(
        &self,
        commit_sha: &str,
        environment: &ProjectEnvironment,
        prepare: bool,
    ) -> std::result::Result<Box<dyn ReviewWorkspace>, String> {
        if self.placement.owner_kind == db::PlacementOwnerKind::Server {
            return self
                .embedded_path()
                .map_err(|error| error.to_string())?
                .clean_checkout(commit_sha, environment, prepare)
                .await;
        }
        if !prepare {
            return Ok(Box::new(self.clone()));
        }
        let outcome = self
            .apply_owner_operation(WorkspaceOwnerOperation::ReviewCheckout {
                commit_sha: commit_sha.into(),
                environment: environment.clone(),
                prepare,
            })
            .await
            .map_err(|error| error.to_string())?;
        let WorkspaceOwnerOperationOutcome::ReviewCheckout { workspace_handle } = outcome else {
            return Err("owner returned no detached review checkout".into());
        };
        if workspace_handle.is_empty()
            || workspace_handle == self.handle().map_err(|error| error.to_string())?
        {
            return Err("owner returned the candidate workspace as its clean checkout".into());
        }
        let mut workspace = self.clone();
        workspace.placement.workspace_handle = Some(workspace_handle);
        Ok(Box::new(DaemonReviewCheckout { workspace }))
    }
    async fn materialize_assets(
        &self,
        environment: &ProjectEnvironment,
    ) -> std::result::Result<(), String> {
        ResolvedWorkspace::materialize_assets(self, environment)
            .await
            .map_err(|error| error.to_string())
    }
    async fn restore(&self, commit_sha: &str) -> std::result::Result<(), String> {
        if self.placement.owner_kind == db::PlacementOwnerKind::Server {
            return self
                .embedded_path()
                .map_err(|error| error.to_string())?
                .restore(commit_sha)
                .await;
        }
        self.apply_owner_change(WorkspaceOwnerOperation::RestoreCandidate {
            commit_sha: commit_sha.into(),
        })
        .await
        .map_err(|error| error.to_string())
    }
    async fn execution_path(&self) -> std::result::Result<String, String> {
        self.owner_paths()
            .await
            .map(|(path, _)| path)
            .map_err(|error| error.to_string())
    }
    fn placement_id(&self) -> Option<&str> {
        Some(&self.placement.id)
    }
    fn infrastructure_error(&self, reason: &str) -> Option<::review::ReviewError> {
        let daemon_id = self.placement.daemon_id.as_deref()?;
        if reason.contains(&format!("owner_unreachable: daemon {daemon_id}"))
            || reason.contains(&format!("workspace owner unavailable: daemon {daemon_id}"))
        {
            Some(::review::ReviewError::OwnerUnavailable {
                daemon_id: daemon_id.into(),
            })
        } else if [
            "stale_generation:",
            "wrong_owner:",
            "purpose_denied:",
            "owner_unsupported:",
        ]
        .iter()
        .any(|cause| reason.contains(cause))
        {
            Some(::review::ReviewError::WorkspaceInfrastructure(
                reason.into(),
            ))
        } else {
            None
        }
    }
    fn embedded_path(&self) -> std::result::Result<PathBuf, String> {
        self.embedded_path().map_err(|error| error.to_string())
    }
}

struct DaemonReviewCheckout {
    workspace: ResolvedWorkspace,
}

#[async_trait::async_trait]
impl ReviewWorkspace for DaemonReviewCheckout {
    async fn run(
        &self,
        command: &str,
        env: &BTreeMap<String, String>,
        limits: Option<CommandLimits>,
    ) -> std::result::Result<CommandOutput, ::review::ReviewError> {
        ReviewWorkspace::run(&self.workspace, command, env, limits).await
    }
    async fn git_read(
        &self,
        args: &[&str],
        optional: bool,
    ) -> std::result::Result<Option<String>, String> {
        self.workspace.git_read(args, optional).await
    }
    async fn diff(&self, branch: &str) -> std::result::Result<String, ::review::ReviewError> {
        self.workspace.diff(branch).await
    }
    async fn clean_checkout(
        &self,
        commit: &str,
        environment: &ProjectEnvironment,
        prepare: bool,
    ) -> std::result::Result<Box<dyn ReviewWorkspace>, String> {
        self.workspace
            .clean_checkout(commit, environment, prepare)
            .await
    }
    async fn restore(&self, commit: &str) -> std::result::Result<(), String> {
        self.workspace.restore(commit).await
    }
    async fn close(&self) -> std::result::Result<(), String> {
        self.workspace
            .apply_owner_change(WorkspaceOwnerOperation::ReleaseReviewCheckout)
            .await
            .map_err(|error| error.to_string())
    }
    fn infrastructure_error(&self, reason: &str) -> Option<::review::ReviewError> {
        self.workspace.infrastructure_error(reason)
    }
    fn embedded_path(&self) -> std::result::Result<PathBuf, String> {
        Err("detached review checkout belongs to a daemon".into())
    }
}

fn review_git_query(args: &[&str]) -> std::result::Result<WorkspaceGitQuery, String> {
    match args {
        ["rev-parse", "HEAD"] => Ok(WorkspaceGitQuery::Head),
        ["rev-parse", "--verify", reference] => Ok(WorkspaceGitQuery::ResolveRef {
            reference: (*reference).into(),
        }),
        ["merge-base", "--is-ancestor", base, head] => Ok(WorkspaceGitQuery::IsAncestor {
            base: (*base).into(),
            head: (*head).into(),
        }),
        ["merge-base", base, head] => Ok(WorkspaceGitQuery::MergeBase {
            base_ref: (*base).into(),
            head_ref: (*head).into(),
        }),
        ["diff", "--name-only", "HEAD"] => Ok(WorkspaceGitQuery::TrackedChanges),
        ["diff", "--name-only", "-z", "--diff-filter=ACDMRTUXB", range, "--"] => {
            let (base_sha, commit_sha) = range.split_once("..").ok_or("invalid candidate range")?;
            Ok(WorkspaceGitQuery::CandidatePaths {
                base_sha: base_sha.into(),
                commit_sha: commit_sha.into(),
            })
        }
        _ => Err("unsupported owner Git evidence query".into()),
    }
}

fn git_query_args(query: &WorkspaceGitQuery) -> Vec<String> {
    match query {
        WorkspaceGitQuery::MarkerPaths { .. } => unreachable!("marker paths use the git helper"),
        WorkspaceGitQuery::IsAncestor { base, head } => vec![
            "merge-base".into(),
            "--is-ancestor".into(),
            base.clone(),
            head.clone(),
        ],
        WorkspaceGitQuery::Head => vec!["rev-parse".into(), "HEAD".into()],
        WorkspaceGitQuery::ResolveRef { reference } => {
            vec!["rev-parse".into(), "--verify".into(), reference.clone()]
        }
        WorkspaceGitQuery::MergeBase { base_ref, head_ref } => {
            vec!["merge-base".into(), base_ref.clone(), head_ref.clone()]
        }
        WorkspaceGitQuery::CandidatePaths {
            base_sha,
            commit_sha,
        } => vec![
            "diff".into(),
            "--name-only".into(),
            "-z".into(),
            "--diff-filter=ACDMRTUXB".into(),
            format!("{base_sha}..{commit_sha}"),
            "--".into(),
        ],
        WorkspaceGitQuery::TrackedChanges => {
            vec!["diff".into(), "--name-only".into(), "HEAD".into()]
        }
        WorkspaceGitQuery::StatusPorcelain => vec!["status".into(), "--porcelain".into()],
        WorkspaceGitQuery::BranchExists { branch } | WorkspaceGitQuery::TargetHead { branch } => {
            vec![
                "rev-parse".into(),
                "--verify".into(),
                format!("refs/heads/{branch}"),
            ]
        }
        WorkspaceGitQuery::RebaseInProgress => unreachable!("rebase state is read through git"),
    }
}

fn review_workspace_error(error: WorkspaceBackendError) -> ::review::ReviewError {
    match error {
        WorkspaceBackendError::OwnerUnreachable { daemon_id } => {
            ::review::ReviewError::OwnerUnavailable { daemon_id }
        }
        WorkspaceBackendError::Other(error) => match *error {
            ServiceError::Db(error) => ::review::ReviewError::Db(error),
            ServiceError::Git(error) => ::review::ReviewError::Git(error),
            ServiceError::InvalidOperation { message } => ::review::ReviewError::Workspace(message),
            error => ::review::ReviewError::Workspace(error.to_string()),
        },
        error => ::review::ReviewError::WorkspaceInfrastructure(error.to_string()),
    }
}

#[async_trait::async_trait]
impl crate::integration_effects::rebase::GitFacts for ResolvedWorkspace {
    async fn query(
        &self,
        query: api_types::WorkspaceGitQuery,
        optional: bool,
    ) -> crate::Result<Option<String>> {
        Ok(self.git_query(query, optional).await?)
    }
}
