use std::{
    collections::{BTreeMap, HashMap},
    path::{Component, Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use api_types::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
};
use workspace::WorkspaceManager;

use crate::{
    daemon_fs,
    daemon_persistence::{
        journal_request, operation_entry_id, DaemonJournal, JournalEntry, JournalOperation,
    },
};

const WORKTREE_DIRECTORY: &str = ".forge/workspaces";
const WORKSPACE_ERROR: &str = "workspace_error";
const VERSION_CONFLICT: &str = "version_conflict";
const MAX_READ_BYTES: u64 = 1024 * 1024;

struct RemoveDirectory(PathBuf);
impl Drop for RemoveDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
const MAX_DIFF_BYTES: usize = 512 * 1024;

type CommandResult<T> = std::result::Result<T, DaemonErrorPayload>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct WorkspaceRegistry {
    locations: HashMap<String, VerifiedLocation>,
    handles: HashMap<String, OwnedWorkspace>,
    /// Cancellation tombstones prevent a delayed request from starting after
    /// an `unknown` acknowledgment, including after an owner restart. Each
    /// maps the operation id to its acknowledgment time (Unix seconds) and is
    /// pruned [`CANCEL_TOMBSTONE_RETENTION_SECS`] after it.
    #[serde(default)]
    cancel_tombstones: HashMap<String, u64>,
}

/// A delayed request for a cancelled operation cannot arrive a week later:
/// server RPCs and their retries are bounded far below this.
const CANCEL_TOMBSTONE_RETENTION_SECS: u64 = 7 * 24 * 60 * 60;

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or_default()
}

impl WorkspaceRegistry {
    /// Record an acknowledged cancellation and drop tombstones past retention.
    fn record_cancel_tombstone(&mut self, operation_id: &str, now: u64) {
        self.cancel_tombstones
            .retain(|_, acked_at| now.saturating_sub(*acked_at) < CANCEL_TOMBSTONE_RETENTION_SECS);
        self.cancel_tombstones.insert(operation_id.to_owned(), now);
    }
}

#[derive(Clone)]
struct RunningWorkspaceCommand {
    cancel: tokio::sync::watch::Sender<bool>,
    finished: tokio::sync::watch::Receiver<Option<WorkspaceCancelState>>,
}

struct WorkspaceCommandGuard<'a> {
    running: &'a Mutex<HashMap<String, RunningWorkspaceCommand>>,
    id: String,
    finished: tokio::sync::watch::Sender<Option<WorkspaceCancelState>>,
    state: WorkspaceCancelState,
}

impl Drop for WorkspaceCommandGuard<'_> {
    fn drop(&mut self) {
        self.finished.send_replace(Some(self.state));
        self.running
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&self.id);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VerifiedLocation {
    daemon_id: String,
    runtime_id: String,
    path: PathBuf,
    kind: DaemonRepoLocationKind,
    version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OwnedWorkspace {
    daemon_id: String,
    runtime_id: String,
    placement_id: String,
    repo_location_id: String,
    path: PathBuf,
    branch: String,
    base_sha: String,
    generation: u64,
    version: i64,
    prepared: bool,
    cleaned: bool,
    execution_ids: Vec<String>,
    #[serde(default)]
    retired_by_operation_id: Option<String>,
    #[serde(default)]
    review_parent: Option<String>,
    #[serde(default)]
    review_operation_id: Option<String>,
}

/// Handles are issued here and persisted in the shared journal directory.
/// Requests cannot supply a worktree path or manufacture a handle mapping.
pub struct DaemonWorkspaceBackend {
    workspace_root: PathBuf,
    daemon_id: String,
    policy: WorkspaceRunPolicy,
    journal: Arc<DaemonJournal>,
    manager: WorkspaceManager,
    state: Mutex<WorkspaceRegistry>,
    operation_lock: tokio::sync::Mutex<()>,
    provision_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    running_commands: Mutex<HashMap<String, RunningWorkspaceCommand>>,
}

impl DaemonWorkspaceBackend {
    pub fn new(
        workspace_root: PathBuf,
        daemon_id: String,
        policy: WorkspaceRunPolicy,
        journal: Arc<DaemonJournal>,
    ) -> anyhow::Result<Self> {
        let workspace_root = workspace_root.canonicalize()?;
        let probes = workspace_root.join(".forge/probes");
        if probes.exists() {
            if probes.canonicalize()? != probes {
                anyhow::bail!("probe directory contains a symlink");
            }
            for entry in std::fs::read_dir(&probes)? {
                let entry = entry?;
                if uuid::Uuid::parse_str(&entry.file_name().to_string_lossy()).is_ok() {
                    if entry.file_type()?.is_dir() {
                        std::fs::remove_dir_all(entry.path())?;
                    } else {
                        std::fs::remove_file(entry.path())?;
                    }
                }
            }
        }
        let state = journal.load_workspace_state()?;
        Ok(Self {
            manager: WorkspaceManager::new(workspace_root.join(WORKTREE_DIRECTORY)),
            workspace_root,
            daemon_id,
            policy,
            journal,
            state: Mutex::new(state),
            operation_lock: tokio::sync::Mutex::new(()),
            provision_locks: Mutex::new(HashMap::new()),
            running_commands: Mutex::new(HashMap::new()),
        })
    }

    pub fn run_policy(&self) -> &WorkspaceRunPolicy {
        &self.policy
    }

    pub fn supports(method: &str) -> bool {
        matches!(
            method,
            METHOD_REPO_LOCATION_VERIFY
                | METHOD_MACHINE_PROBE
                | METHOD_REPO_LOCATION_PROVISION
                | METHOD_WORKSPACE_PREPARE
                | METHOD_WORKSPACE_DESCRIBE
                | METHOD_WORKSPACE_RUN
                | METHOD_WORKSPACE_CANCEL
                | METHOD_WORKSPACE_DIFF
                | METHOD_WORKSPACE_READ
                | METHOD_WORKSPACE_MERGE
                | METHOD_WORKSPACE_RESET
                | METHOD_WORKSPACE_CLEANUP
        )
    }

    pub async fn handle(
        &self,
        method: &str,
        params: Value,
        active_ids: impl FnOnce() -> Vec<String>,
    ) -> CommandResult<Value> {
        if method == METHOD_WORKSPACE_CANCEL {
            return encode(self.cancel_command(decode(params)?).await?);
        }
        if !matches!(
            method,
            METHOD_WORKSPACE_PREPARE
                | METHOD_WORKSPACE_RUN
                | METHOD_WORKSPACE_MERGE
                | METHOD_WORKSPACE_RESET
                | METHOD_WORKSPACE_CLEANUP
        ) {
            return self.handle_inner(method, params, active_ids).await;
        }
        let fence: WorkspaceMutationFence = decode(params.clone())?;
        let id = fence.operation_id;
        validate_id(&id)?;
        let (cancel, mut cancelled) = tokio::sync::watch::channel(false);
        let (finished, completed) = tokio::sync::watch::channel(None);
        {
            // Register before waiting for the workspace mutation lock. Cancel
            // must also stop a command that has not yet spawned its child.
            let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.cancel_tombstones.contains_key(&id) {
                return Err(error(WORKSPACE_ERROR, "workspace operation was cancelled"));
            }
            let mut running = self
                .running_commands
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if running.contains_key(&id) {
                return Err(interrupted_error(&id));
            }
            running.insert(
                id.clone(),
                RunningWorkspaceCommand {
                    cancel,
                    finished: completed,
                },
            );
        }
        let mut guard = WorkspaceCommandGuard {
            running: &self.running_commands,
            id,
            finished,
            state: WorkspaceCancelState::Killed,
        };
        let result = if method == METHOD_WORKSPACE_MERGE {
            // Integration finishes before a cancel acknowledgement. The
            // server fences an unreachable owner until this handler settles.
            let result = self.handle_inner(method, params, active_ids).await;
            guard.state = WorkspaceCancelState::AlreadyFinished;
            result
        } else {
            tokio::select! {
                biased;
                _ = cancelled.changed() => Err(error(WORKSPACE_ERROR, "workspace operation was cancelled")),
                result = self.handle_inner(method, params, active_ids) => {
                    guard.state = WorkspaceCancelState::AlreadyFinished;
                    result
                }
            }
        };
        // The selected future has been dropped here. Its ProcessGroupGuard
        // kills descendants before the completion acknowledgment is visible.
        drop(guard);
        result
    }

    async fn cancel_command(
        &self,
        request: WorkspaceCancelParams,
    ) -> CommandResult<WorkspaceCancelResult> {
        validate_id(&request.operation_id)?;
        let running = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            let mut updated = state.clone();
            updated.record_cancel_tombstone(&request.operation_id, unix_now());
            self.journal
                .save_workspace_state(&updated)
                .map_err(storage_error)?;
            *state = updated;
            self.running_commands
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(&request.operation_id)
                .cloned()
        };
        let state = if let Some(mut running) = running {
            running.cancel.send_replace(true);
            loop {
                if let Some(state) = *running.finished.borrow_and_update() {
                    break state;
                }
                if running.finished.changed().await.is_err() {
                    break WorkspaceCancelState::Killed;
                }
            }
        } else if self
            .journal
            .operation(&request.operation_id)
            .map_err(storage_error)?
            .is_some()
        {
            WorkspaceCancelState::AlreadyFinished
        } else {
            WorkspaceCancelState::Unknown
        };
        Ok(WorkspaceCancelResult {
            operation_id: request.operation_id,
            state,
        })
    }

    async fn handle_inner(
        &self,
        method: &str,
        params: Value,
        active_ids: impl FnOnce() -> Vec<String>,
    ) -> CommandResult<Value> {
        // Probes neither serialize behind a workspace lock nor use its journal.
        if method == METHOD_MACHINE_PROBE {
            return encode(self.machine_probe(decode(params)?).await?);
        }
        if method == METHOD_REPO_LOCATION_PROVISION {
            let params: RepoLocationProvisionParams = decode(params)?;
            validate_id(&params.repo_id)?;
            let lock = self
                .provision_locks
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .entry(params.repo_id.clone())
                .or_default()
                .clone();
            let _guard = lock.lock().await;
            let remote = params.remote_url.clone();
            return encode(self.provision_location(params).await.map_err(|mut error| {
                error.message = git::redact_remote_credentials(&error.message, &remote);
                if let Some(details) = error.details {
                    error.details = serde_json::from_str(&git::redact_remote_credentials(
                        &details.to_string(),
                        &remote,
                    ))
                    .ok();
                }
                error
            })?);
        }
        let _guard = self.operation_lock.lock().await;
        // Capture activity after acquiring the mutation lock. An execution
        // may have started while this request was waiting for another RPC.
        let active_ids = active_ids();
        if !Self::supports(method) {
            return Err(error(UNSUPPORTED_METHOD, "unsupported workspace method"));
        }
        match method {
            METHOD_REPO_LOCATION_VERIFY => {
                let request: RepoLocationVerifyParams = decode(params)?;
                let remote = request.remote_url.clone().unwrap_or_default();
                return encode(self.verify(request).await.map_err(|mut error| {
                    error.message = git::redact_remote_credentials(&error.message, &remote);
                    error.details = error.details.and_then(|details| {
                        serde_json::from_str(&git::redact_remote_credentials(
                            &details.to_string(),
                            &remote,
                        ))
                        .ok()
                    });
                    error
                })?);
            }
            METHOD_WORKSPACE_DESCRIBE => {
                if params.get("operation").is_some() {
                    return encode(self.reconcile(decode(params)?).await?);
                }
                return encode(self.describe(decode(params)?, &active_ids).await?);
            }
            METHOD_WORKSPACE_DIFF => {
                if params.get("operation").is_some() {
                    return encode(self.review_diff(decode(params)?).await?);
                }
                return encode(self.diff(decode(params)?).await?);
            }
            METHOD_WORKSPACE_READ => {
                if params.get("operation").is_some() {
                    return encode(self.inspect(decode(params)?).await?);
                }
                return encode(self.read(decode(params)?).await?);
            }
            _ => {}
        }
        let fence: WorkspaceMutationFence = decode(params.clone())?;
        let owner_operation = method == METHOD_WORKSPACE_RESET && params.get("operation").is_some();
        let recreating = method == METHOD_WORKSPACE_RESET && !owner_operation;
        let discard_plan = owner_operation
            && params.pointer("/operation/kind").and_then(Value::as_str) == Some("discard_plan");
        match method {
            METHOD_WORKSPACE_PREPARE => {
                decode::<WorkspacePrepareParams>(params.clone()).map(|_| ())?
            }
            METHOD_WORKSPACE_RUN => decode::<WorkspaceRunParams>(params.clone()).map(|_| ())?,
            METHOD_WORKSPACE_MERGE => {
                decode::<WorkspaceReviewedMergeParams>(params.clone()).map(|_| ())?
            }
            METHOD_WORKSPACE_RESET if owner_operation => {
                decode::<WorkspaceOwnerOperationParams>(params.clone()).map(|_| ())?
            }
            METHOD_WORKSPACE_RESET => decode::<WorkspaceResetParams>(params.clone()).map(|_| ())?,
            METHOD_WORKSPACE_CLEANUP => {
                decode::<WorkspaceCleanupParams>(params.clone()).map(|_| ())?
            }
            _ => unreachable!("workspace read methods returned above"),
        }
        validate_id(&fence.operation_id)?;
        let existing = self.workspace_for_placement(&fence.placement_id);
        let handle = if method == METHOD_WORKSPACE_PREPARE {
            existing.as_ref().map(|(handle, _)| handle.as_str())
        } else {
            params.get("workspace_handle").and_then(Value::as_str)
        };
        let resuming = if let Some(operation) = self
            .journal
            .operation(&fence.operation_id)
            .map_err(storage_error)?
        {
            if operation.method != method || operation.request != journal_request(&params) {
                return Err(error(
                    TERMINAL_REPORT_CONFLICT,
                    "operation_id was reused for a different request",
                ));
            }
            if let Some(outcome) = operation.outcome {
                return outcome;
            }
            // Prepare, reset and cleanup can finish a durable intent after a
            // process restart. Shell and merge intents require inspection.
            if !matches!(
                method,
                METHOD_WORKSPACE_PREPARE | METHOD_WORKSPACE_RESET | METHOD_WORKSPACE_CLEANUP
            ) {
                return Err(interrupted_error(&fence.operation_id));
            }
            true
        } else {
            self.check_owner(&fence.daemon_id, &fence.runtime_id)?;
            if let Some((_, workspace)) = &existing {
                self.check_generation(fence.generation, workspace.generation, recreating)?;
            }
            if let Some(handle) = handle {
                if !discard_plan || existing.is_some() {
                    self.workspace(&reference(&fence, handle), recreating)?;
                }
            }
            let operation = JournalOperation {
                entry_id: operation_entry_id(&fence.operation_id),
                fence: fence.clone(),
                workspace_handle: params
                    .get("workspace_handle")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                method: method.to_owned(),
                request: params.clone(),
                outcome: None,
                acknowledged: false,
            };
            self.journal
                .retain_entry(&JournalEntry::Operation { operation })
                .map_err(storage_error)?;
            false
        };
        if let Some(handle) = handle {
            if !discard_plan || existing.is_some() {
                self.workspace(&reference(&fence, handle), recreating)?;
            }
        }
        let mut outcome = if discard_plan && existing.is_none() {
            encode(WorkspaceOwnerOperationResult {
                entry_id: operation_entry_id(&fence.operation_id),
                operation_id: fence.operation_id.clone(),
                outcome: WorkspaceOwnerOperationOutcome::Applied,
            })
        } else {
            match method {
                METHOD_WORKSPACE_PREPARE => {
                    self.prepare(decode(params.clone())?).await.and_then(encode)
                }
                METHOD_WORKSPACE_RUN => self.run(decode(params.clone())?).await.and_then(encode),
                METHOD_WORKSPACE_MERGE => {
                    self.merge(decode(params.clone())?).await.and_then(encode)
                }
                METHOD_WORKSPACE_RESET if owner_operation => self
                    .owner_operation(decode(params.clone())?, &active_ids)
                    .await
                    .and_then(encode),
                METHOD_WORKSPACE_RESET => self
                    .reset(decode(params.clone())?, &active_ids, resuming)
                    .await
                    .and_then(encode),
                METHOD_WORKSPACE_CLEANUP => self
                    .cleanup(decode(params.clone())?, &active_ids)
                    .await
                    .and_then(encode),
                _ => Err(error(
                    UNSUPPORTED_METHOD,
                    format!("unsupported workspace method: {method}"),
                )),
            }
        };
        if let Err(error) = &mut outcome {
            error.details = Some(serde_json::json!({
                "entry_id": operation_entry_id(&fence.operation_id),
                "operation_id": &fence.operation_id,
            }));
        }
        let mut operation = self
            .journal
            .operation(&fence.operation_id)
            .map_err(storage_error)?
            .ok_or_else(|| error(WORKSPACE_ERROR, "operation intent is missing"))?;
        if operation.workspace_handle.is_none() {
            operation.workspace_handle = outcome
                .as_ref()
                .ok()
                .and_then(|value| value.get("workspace_handle"))
                .and_then(Value::as_str)
                .map(str::to_owned);
        }
        operation.request = params;
        operation.outcome = Some(outcome.clone());
        self.journal
            .finish_operation(&operation)
            .map_err(storage_error)?
            .outcome
            .ok_or_else(|| error(WORKSPACE_ERROR, "operation result is missing"))?
    }

    pub async fn acknowledge_journal(
        &self,
        params: &JournalAckParams,
    ) -> CommandResult<JournalAckResult> {
        let _guard = self.operation_lock.lock().await;
        if let Some(JournalEntry::Operation { operation }) = self
            .journal
            .entry(&params.entry_id)
            .map_err(storage_error)?
        {
            let retires_handles = operation.method == METHOD_WORKSPACE_CLEANUP
                || (operation.method == METHOD_WORKSPACE_RESET
                    && (operation.request.get("operation").is_none()
                        || operation
                            .request
                            .pointer("/operation/kind")
                            .and_then(Value::as_str)
                            == Some("release_review_checkout")));
            if retires_handles && matches!(operation.outcome, Some(Ok(_))) {
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                let mut updated = state.clone();
                updated.handles.retain(|_, owned| {
                    !(owned.cleaned
                        && owned.retired_by_operation_id.as_deref()
                            == Some(operation.fence.operation_id.as_str()))
                });
                self.journal
                    .save_workspace_state(&updated)
                    .map_err(storage_error)?;
                *state = updated;
            }
        }
        self.journal.acknowledge(params).map_err(storage_error)
    }

    fn check_owner(&self, daemon_id: &str, runtime_id: &str) -> CommandResult<()> {
        if daemon_id != self.daemon_id || runtime_id.trim().is_empty() {
            return Err(error(
                WRONG_OWNER,
                "request does not belong to this daemon runtime",
            ));
        }
        Ok(())
    }

    fn check_generation(
        &self,
        requested: u64,
        current: u64,
        allow_next: bool,
    ) -> CommandResult<()> {
        if requested < current
            || (requested > current && (!allow_next || requested != current.saturating_add(1)))
        {
            return Err(error(
                STALE_GENERATION,
                format!("generation {requested} does not match current generation {current}"),
            ));
        }
        Ok(())
    }

    fn workspace_for_placement(&self, placement_id: &str) -> Option<(String, OwnedWorkspace)> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .handles
            .iter()
            .find(|(_, value)| value.placement_id == placement_id && value.review_parent.is_none())
            .map(|(handle, value)| (handle.clone(), value.clone()))
    }

    fn workspace(
        &self,
        reference: &WorkspaceHandleReference,
        allow_next: bool,
    ) -> CommandResult<OwnedWorkspace> {
        self.check_owner(&reference.daemon_id, &reference.runtime_id)?;
        let owned = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .handles
            .get(&reference.workspace_handle)
            .cloned()
            .ok_or_else(|| error(INVALID_INPUT, "unknown workspace_handle"))?;
        if owned.daemon_id != reference.daemon_id
            || owned.runtime_id != reference.runtime_id
            || owned.placement_id != reference.placement_id
        {
            return Err(error(
                WRONG_OWNER,
                "handle belongs to a different owner or placement",
            ));
        }
        self.check_generation(reference.generation, owned.generation, allow_next)?;
        if owned.review_parent.is_some() {
            if let Some((_, parent)) = self.workspace_for_placement(&owned.placement_id) {
                self.check_generation(reference.generation, parent.generation, false)?;
            }
        }
        let expected_path = self.path_for_handle(&reference.workspace_handle)?;
        if owned.path != expected_path {
            return Err(error(
                OUTSIDE_WORKSPACE_ROOT,
                "handle mapping is not a daemon-created path",
            ));
        }
        self.confined_path(&owned.path)?;
        Ok(owned)
    }

    fn path_for_handle(&self, handle: &str) -> CommandResult<PathBuf> {
        if !handle.starts_with("workspace-")
            || uuid::Uuid::parse_str(&handle["workspace-".len()..]).is_err()
        {
            return Err(error(INVALID_INPUT, "invalid workspace_handle"));
        }
        self.confined_path(
            &self
                .workspace_root
                .join(WORKTREE_DIRECTORY)
                .join(handle)
                .join("repo"),
        )
    }

    fn confined_path(&self, path: &Path) -> CommandResult<PathBuf> {
        if path
            .components()
            .any(|part| matches!(part, Component::ParentDir))
        {
            return Err(error(
                OUTSIDE_WORKSPACE_ROOT,
                "path contains parent traversal",
            ));
        }
        let absolute = if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.workspace_root.join(path)
        };
        let mut ancestor = absolute.as_path();
        while !ancestor.exists() {
            if std::fs::symlink_metadata(ancestor).is_ok() {
                return Err(error(
                    OUTSIDE_WORKSPACE_ROOT,
                    "path contains a dangling symlink",
                ));
            }
            ancestor = ancestor
                .parent()
                .ok_or_else(|| error(OUTSIDE_WORKSPACE_ROOT, "path has no confined parent"))?;
        }
        let canonical = ancestor.canonicalize().map_err(io_error)?;
        if !canonical.starts_with(&self.workspace_root) {
            return Err(error(
                OUTSIDE_WORKSPACE_ROOT,
                "path resolves outside workspace_root",
            ));
        }
        let suffix = absolute
            .strip_prefix(ancestor)
            .map_err(|_| error(OUTSIDE_WORKSPACE_ROOT, "path has no confined suffix"))?;
        if suffix.as_os_str().is_empty() {
            Ok(canonical)
        } else {
            Ok(canonical.join(suffix))
        }
    }

    fn save_workspace(&self, handle: &str, workspace: OwnedWorkspace) -> CommandResult<()> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let mut updated = state.clone();
        updated.handles.insert(handle.to_owned(), workspace);
        self.journal
            .save_workspace_state(&updated)
            .map_err(storage_error)?;
        *state = updated;
        Ok(())
    }

    async fn location_path(&self, id: &str) -> CommandResult<(VerifiedLocation, PathBuf)> {
        let location = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .locations
            .get(id)
            .cloned()
            .ok_or_else(|| error(INVALID_INPUT, "repository location has not been verified"))?;
        self.check_owner(&location.daemon_id, &location.runtime_id)?;
        self.confined_path(&location.path)?;
        let path = confined_existing(&location.path, &self.workspace_root)?;
        verify_git_dir(&path, &self.workspace_root).await?;
        Ok((location, path))
    }

    async fn verify(
        &self,
        params: RepoLocationVerifyParams,
    ) -> CommandResult<RepoLocationVerifyResult> {
        self.check_owner(&params.daemon_id, &params.runtime_id)?;
        validate_id(&params.repo_location_id)?;
        let requested = self.confined_path(Path::new(params.path.trim()))?;
        let path = confined_existing(&requested, &self.workspace_root)?;
        if !path.is_dir()
            || local_git(&path, &["rev-parse", "--is-inside-work-tree"]).await? != "true"
        {
            return Err(error(
                INVALID_INPUT,
                "repository location must be a git work tree",
            ));
        }
        verify_git_dir(&path, &self.workspace_root).await?;
        let top = PathBuf::from(local_git(&path, &["rev-parse", "--show-toplevel"]).await?);
        let path = confined_existing(&top, &self.workspace_root)?;
        let default_branch_sha = resolve_commit(&path, &params.default_branch).await?;
        let branches = git::list_branches(&path).await.map_err(git_error)?;
        if let (Some(origin), Some(expected)) = (&branches.origin_url, params.remote_url.as_deref())
        {
            if git::normalize_remote_url(expected) != git::normalize_remote_url(origin) {
                return Err(error(
                    INVALID_INPUT,
                    "repository origin remote does not match",
                ));
            }
        }
        let probe_content = match params.probe {
            Some(probe) => {
                let probe_path =
                    daemon_fs::validate_within_root(Path::new(&probe.path), &self.workspace_root)
                        .map_err(|_| {
                        error(OUTSIDE_WORKSPACE_ROOT, "probe path escapes workspace_root")
                    })?;
                let bytes = read_bounded_file(&probe_path, 4096).await?;
                let text = String::from_utf8(bytes)
                    .map_err(|_| error(INVALID_INPUT, "probe is not UTF-8"))?;
                if text != probe.content {
                    return Err(error(INVALID_INPUT, "shared-mount probe does not match"));
                }
                Some(text)
            }
            None if params.kind == DaemonRepoLocationKind::SharedMount => {
                return Err(error(INVALID_INPUT, "shared_mount requires a probe"))
            }
            None => None,
        };
        if path == self.workspace_root {
            self.exclude_runtime_metadata(&path).await?;
        }
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(existing) = state.locations.get(&params.repo_location_id) {
            if params.expected_version < existing.version {
                return Err(error(
                    VERSION_CONFLICT,
                    "repository location version is stale",
                ));
            }
            if existing.daemon_id != params.daemon_id || existing.runtime_id != params.runtime_id {
                return Err(error(
                    WRONG_OWNER,
                    "repository location belongs to another owner",
                ));
            }
        }
        let mut updated = state.clone();
        updated.locations.insert(
            params.repo_location_id.clone(),
            VerifiedLocation {
                daemon_id: params.daemon_id,
                runtime_id: params.runtime_id,
                path: path.clone(),
                kind: params.kind,
                version: params.expected_version,
            },
        );
        self.journal
            .save_workspace_state(&updated)
            .map_err(storage_error)?;
        *state = updated;
        Ok(RepoLocationVerifyResult {
            repo_location_id: params.repo_location_id,
            path: path.to_string_lossy().into_owned(),
            default_branch_sha,
            origin_url: branches
                .origin_url
                .map(|origin| git::redact_remote_credentials(&origin, &origin)),
            probe_content,
        })
    }

    async fn machine_probe(&self, params: MachineProbeParams) -> CommandResult<MachineProbeResult> {
        self.check_owner(&params.daemon_id, &params.runtime_id)?;
        if !self
            .policy
            .allowed_purposes
            .contains(&WorkspaceRunPurpose::EnvironmentProbe)
        {
            return Err(error(
                "run_purpose_denied",
                "environment_probe is denied by local daemon configuration",
            ));
        }
        if params.commands.is_empty()
            || params.commands.len() > 64
            || params.commands.iter().any(|command| {
                command.name.trim().is_empty()
                    || command.command.trim().is_empty()
                    || !(1..=300).contains(&command.timeout_seconds)
            })
        {
            return Err(error(
                INVALID_INPUT,
                "machine.probe requires 1–64 named commands with 1–300 second timeouts",
            ));
        }
        let checkout = match params.repo_location_id.as_deref() {
            Some(id) => Some(self.location_path(id).await?.1),
            None => None,
        };
        let mut results = Vec::new();
        for check in params.commands {
            let scratch = if checkout.is_none() {
                let path = self.confined_path(
                    &self
                        .workspace_root
                        .join(".forge/probes")
                        .join(uuid::Uuid::new_v4().to_string()),
                )?;
                std::fs::create_dir_all(&path).map_err(io_error)?;
                Some(RemoveDirectory(path))
            } else {
                None
            };
            let path = checkout
                .as_deref()
                .unwrap_or_else(|| &scratch.as_ref().expect("scratch").0);
            let mut command = Command::new("bash");
            command
                .args(["-lc", &check.command])
                .envs(&params.env)
                .env("PWD", path)
                .current_dir(path)
                .env_remove("GIT_DIR")
                .env_remove("GIT_WORK_TREE")
                .env_remove("GIT_INDEX_FILE");
            executors::run_process::apply(&mut command, &params.env);
            let output =
                bounded_command_inner(command, check.timeout_seconds, 4096, true, true).await?;
            let raw = [output.stdout, output.stderr].concat();
            let (output_tail, _) = redacted_tail(&raw, &params.env, 4096);
            results.push(MachineProbeCommandResult {
                name: check.name,
                exit_code: output.exit_code,
                timed_out: output.timed_out,
                output_tail,
            });
        }
        Ok(MachineProbeResult { results })
    }

    async fn provision_location(
        &self,
        params: RepoLocationProvisionParams,
    ) -> CommandResult<RepoLocationProvisionResult> {
        self.check_owner(&params.daemon_id, &params.runtime_id)?;
        validate_id(&params.repo_id)?;
        if !self
            .policy
            .allowed_purposes
            .contains(&WorkspaceRunPurpose::RepoProvision)
        {
            return Err(error(
                "run_purpose_denied",
                "repo_provision is denied by local daemon configuration",
            ));
        }
        if !(1..=86400).contains(&params.timeout_seconds) {
            return Err(error(
                INVALID_INPUT,
                "provision timeout must be between 1 and 86400 seconds",
            ));
        }
        let deadline = Instant::now() + Duration::from_secs(params.timeout_seconds);
        if params.default_branch.is_empty()
            || params.default_branch.starts_with('-')
            || local_git(
                &self.workspace_root,
                &[
                    "check-ref-format",
                    &format!("refs/heads/{}", params.default_branch),
                ],
            )
            .await
            .is_err()
        {
            return Err(error(INVALID_INPUT, "invalid repository branch"));
        }
        if params.remote_url.trim().is_empty() || params.remote_url.starts_with('-') {
            return Err(error(INVALID_INPUT, "repository remote is required"));
        }
        let requested_path = self.workspace_root.join("repos").join(&params.repo_id);
        let path = self.confined_path(&requested_path)?;
        if path != requested_path {
            return Err(error(
                "path_conflict",
                "managed clone path contains a symlink",
            ));
        }
        if path.exists() {
            let origin = local_git(&path, &["remote", "get-url", "origin"]).await;
            let top = local_git(&path, &["rev-parse", "--show-toplevel"]).await;
            if origin.is_err()
                || top.is_err()
                || Path::new(top.as_ref().expect("checked")) != path
                || git::normalize_remote_url(&params.remote_url)
                    != git::normalize_remote_url(origin.as_ref().expect("checked"))
            {
                return Err(error(
                    "path_conflict",
                    "managed clone path holds another repository or non-repository content",
                ));
            }
            // The identity is unchanged, but owner-configured credentials may
            // have rotated. Fetch uses this request's URL, never a stale origin.
            local_git(&path, &["remote", "set-url", "origin", &params.remote_url]).await?;
        } else {
            let requested_staging = self
                .workspace_root
                .join(".forge/provision")
                .join(&params.repo_id);
            let staging = self.confined_path(&requested_staging)?;
            if staging != requested_staging {
                return Err(error(
                    "path_conflict",
                    "private staging path contains a symlink",
                ));
            }
            // A crash can leave only our private staging directory. Retry removes
            // it before cloning; the final path is published atomically.
            if staging.exists() {
                std::fs::remove_dir_all(&staging).map_err(io_error)?;
            }
            std::fs::create_dir_all(staging.parent().expect("staging parent")).map_err(io_error)?;
            let cleanup = RemoveDirectory(staging.clone());
            let mut command = Command::new("git");
            command
                .arg("clone")
                .arg("--branch")
                .arg(&params.default_branch)
                .arg("--")
                .arg(&params.remote_url)
                .arg(&staging)
                .env("GIT_TERMINAL_PROMPT", "0")
                .current_dir(&self.workspace_root)
                .env_remove("GIT_DIR")
                .env_remove("GIT_WORK_TREE")
                .env_remove("GIT_INDEX_FILE");
            let output = bounded_command(command, params.timeout_seconds, 4096, true).await?;
            if output.timed_out || output.exit_code != Some(0) {
                let (message, _) = redacted_tail(&output.stderr, &BTreeMap::new(), 4096);
                return Err(error(
                    "clone_failed",
                    if output.timed_out {
                        "repository clone timed out".to_owned()
                    } else if message.is_empty() {
                        "repository clone failed".to_owned()
                    } else {
                        message
                    },
                ));
            }
            std::fs::create_dir_all(path.parent().expect("clone parent")).map_err(io_error)?;
            std::fs::rename(&staging, &path).map_err(io_error)?;
            drop(cleanup);
        }
        verify_git_dir(&path, &self.workspace_root).await?;
        if resolve_commit(&path, &format!("refs/heads/{}", params.default_branch))
            .await
            .is_err()
        {
            let mut fetch = Command::new("git");
            fetch
                .args([
                    "fetch",
                    "--",
                    "origin",
                    &format!(
                        "refs/heads/{0}:refs/remotes/origin/{0}",
                        params.default_branch
                    ),
                ])
                .current_dir(&path)
                .env("GIT_TERMINAL_PROMPT", "0")
                .env_remove("GIT_DIR")
                .env_remove("GIT_WORK_TREE")
                .env_remove("GIT_INDEX_FILE");
            let remaining = deadline.saturating_duration_since(Instant::now()).as_secs();
            if remaining == 0 {
                return Err(error("clone_failed", "repository fetch timed out"));
            }
            let fetched = bounded_command(fetch, remaining, 4096, true).await?;
            if fetched.timed_out || fetched.exit_code != Some(0) {
                return Err(error(
                    "clone_failed",
                    String::from_utf8_lossy(&fetched.stderr),
                ));
            }
            local_git(
                &path,
                &[
                    "branch",
                    "--no-track",
                    &params.default_branch,
                    &format!("refs/remotes/origin/{}", params.default_branch),
                ],
            )
            .await?;
        }
        let default_branch = params.default_branch;
        Ok(RepoLocationProvisionResult {
            workspace_root: self.workspace_root.to_string_lossy().into_owned(),
            path: path.to_string_lossy().into_owned(),
            default_branch,
        })
    }

    async fn exclude_runtime_metadata(&self, repo_path: &Path) -> CommandResult<()> {
        // When the advertised root is itself a checkout, Forge's own journal
        // and worktrees must not make the user's integration target dirty.
        let git_path = PathBuf::from(
            local_git(repo_path, &["rev-parse", "--git-path", "info/exclude"]).await?,
        );
        let path = self.confined_path(&if git_path.is_absolute() {
            git_path
        } else {
            repo_path.join(git_path)
        })?;
        let text = match tokio::fs::File::open(&path).await {
            Ok(mut file) => {
                use tokio::io::AsyncSeekExt;
                let metadata = file.metadata().await.map_err(io_error)?;
                if !metadata.is_file() {
                    return Err(error(INVALID_INPUT, "git exclude is not a regular file"));
                }
                file.seek(std::io::SeekFrom::Start(
                    metadata.len().saturating_sub(64 * 1024),
                ))
                .await
                .map_err(io_error)?;
                let mut tail = Vec::new();
                file.take(64 * 1024)
                    .read_to_end(&mut tail)
                    .await
                    .map_err(io_error)?;
                String::from_utf8_lossy(&tail).into_owned()
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => return Err(io_error(error)),
        };
        let mut extra = String::new();
        for pattern in ["/.forge/", "/.forge-daemon/"] {
            if !text.lines().any(|line| line == pattern) {
                extra.push('\n');
                extra.push_str(pattern);
                extra.push('\n');
            }
        }
        if !extra.is_empty() {
            use tokio::io::AsyncWriteExt;
            let parent = path
                .parent()
                .ok_or_else(|| error(OUTSIDE_WORKSPACE_ROOT, "exclude file has no parent"))?;
            self.confined_path(parent)?;
            tokio::fs::create_dir_all(parent).await.map_err(io_error)?;
            let mut file = tokio::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(path)
                .await
                .map_err(io_error)?;
            file.write_all(extra.as_bytes()).await.map_err(io_error)?;
            file.sync_all().await.map_err(io_error)?;
        }
        Ok(())
    }

    async fn prepare(
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

    async fn describe(
        &self,
        params: WorkspaceDescribeParams,
        active_ids: &[String],
    ) -> CommandResult<WorkspaceDescribeResult> {
        let owned = self.workspace(&params.workspace, false)?;
        let exists = owned.path.is_dir() && owned.prepared && !owned.cleaned;
        let (head_sha, dirty, branch) = if exists {
            verify_git_dir(&owned.path, &self.workspace_root).await?;
            (
                Some(git::get_current_sha(&owned.path).await.map_err(git_error)?),
                !git::is_worktree_clean(&owned.path)
                    .await
                    .map_err(git_error)?,
                git::list_branches(&owned.path)
                    .await
                    .map_err(git_error)?
                    .default_branch,
            )
        } else {
            (None, false, None)
        };
        let mut journaled_execution_ids = Vec::new();
        for entry in self.journal.pending().map_err(storage_error)? {
            if let JournalEntry::Terminal { report } = entry {
                if owned.execution_ids.contains(&report.execution_id) {
                    journaled_execution_ids.push(report.execution_id);
                }
            }
        }
        journaled_execution_ids.sort();
        journaled_execution_ids.dedup();
        Ok(WorkspaceDescribeResult {
            workspace_handle: params.workspace.workspace_handle,
            generation: owned.generation,
            exists,
            head_sha,
            dirty,
            branch,
            locked: owned
                .path
                .parent()
                .is_some_and(|p| p.join(".forge.lock").exists()),
            active_execution_ids: owned
                .execution_ids
                .into_iter()
                .filter(|id| active_ids.contains(id))
                .collect(),
            journaled_execution_ids,
        })
    }

    async fn check_expected(
        &self,
        owned: &OwnedWorkspace,
        expected: &WorkspaceOperationExpected,
    ) -> CommandResult<()> {
        match expected {
            WorkspaceOperationExpected::Version { version } if *version != owned.version => {
                return Err(error(VERSION_CONFLICT, "workspace version is stale"))
            }
            WorkspaceOperationExpected::BaseSha { sha } => {
                let current = if owned.path.exists() {
                    git::get_current_sha(&owned.path).await.map_err(git_error)?
                } else {
                    owned.base_sha.clone()
                };
                if sha != &current {
                    return Err(error(VERSION_CONFLICT, "workspace HEAD changed"));
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn live_workspace(
        &self,
        reference: &WorkspaceHandleReference,
    ) -> CommandResult<OwnedWorkspace> {
        let owned = self.workspace(reference, false)?;
        if !owned.prepared || owned.cleaned || !owned.path.is_dir() {
            return Err(error(INVALID_INPUT, "workspace is not prepared"));
        }
        verify_git_dir(&owned.path, &self.workspace_root).await?;
        Ok(owned)
    }

    pub async fn register_execution(&self, execution_id: &str, path: &Path) -> CommandResult<()> {
        let _guard = self.operation_lock.lock().await;
        let found = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .handles
            .iter()
            .find(|(_, owned)| owned.path == path)
            .map(|(handle, owned)| (handle.clone(), owned.clone()));
        if let Some((handle, mut owned)) = found {
            self.live_workspace(&WorkspaceHandleReference {
                daemon_id: owned.daemon_id.clone(),
                runtime_id: owned.runtime_id.clone(),
                placement_id: owned.placement_id.clone(),
                workspace_handle: handle.clone(),
                generation: owned.generation,
            })
            .await?;
            if !owned.execution_ids.iter().any(|id| id == execution_id) {
                owned.execution_ids.push(execution_id.to_owned());
                self.save_workspace(&handle, owned)?;
            }
        } else if path.starts_with(self.workspace_root.join(WORKTREE_DIRECTORY)) {
            return Err(error(
                INVALID_INPUT,
                "execution references an unknown daemon worktree",
            ));
        }
        Ok(())
    }

    async fn run(&self, params: WorkspaceRunParams) -> CommandResult<WorkspaceRunResult> {
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
        executors::run_process::apply(&mut command, &params.env.iter().cloned().collect());
        // Persist the version before launching; journal pressure after the
        // command exits must never discard its exit result.
        owned.version += 1;
        self.save_workspace(&params.workspace_handle, owned)?;
        let start = Instant::now();
        let output_cap = usize::try_from(params.max_output_bytes)
            .unwrap_or(usize::MAX)
            .min(crate::daemon_persistence::MAX_CI_LOG_BYTES);
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

    async fn read(&self, params: WorkspaceReadParams) -> CommandResult<WorkspaceReadResult> {
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

    async fn diff(&self, params: WorkspaceDiffParams) -> CommandResult<WorkspaceDiffResult> {
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

    async fn merge(
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
        git::checkout_branch(target, &params.target_branch)
            .await
            .map_err(git_error)?;
        if git::get_current_sha(target).await.map_err(git_error)? != params.expected_target_sha {
            return Ok(WorkspaceMergeOutcome::TargetMoved {
                reason: "integration target changed during checkout".into(),
                target_branch: params.target_branch.clone(),
            });
        }
        let before_sha = params.expected_target_sha.clone();
        let merged = if reviewed_commit_sha.is_some() {
            match local_git(target, &["merge", "--ff-only", &head]).await {
                Ok(_) => Ok(()),
                Err(error) => {
                    return Ok(WorkspaceMergeOutcome::ReviewRequired {
                        reason: error.message,
                    })
                }
            }
        } else {
            git::merge_branch_into(target, &head).await
        };
        match merged {
            Ok(()) => {
                let after_sha = git::get_current_sha(target).await.map_err(git_error)?;
                if reviewed_commit_sha.is_some() && after_sha != head {
                    return Ok(WorkspaceMergeOutcome::TargetMoved {
                        reason: "integration target changed during merge; reviewed content was not integrated".into(),
                        target_branch: params.target_branch.clone(),
                    });
                }
                Ok(WorkspaceMergeOutcome::Done {
                    before_sha,
                    after_sha,
                    branch: params.target_branch.clone(),
                })
            }
            Err(git::GitError::MergeConflict { stderr, .. }) => {
                let conflict_paths = git::conflict_paths(target).await.map_err(git_error)?;
                git::abort_merge(target).await.map_err(git_error)?;
                Ok(WorkspaceMergeOutcome::Conflict {
                    details: stderr,
                    conflict_paths,
                })
            }
            Err(error) => Err(git_error(error)),
        }
    }

    async fn reset(
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

    async fn cleanup(
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

fn reference(fence: &WorkspaceMutationFence, handle: &str) -> WorkspaceHandleReference {
    WorkspaceHandleReference {
        daemon_id: fence.daemon_id.clone(),
        runtime_id: fence.runtime_id.clone(),
        placement_id: fence.placement_id.clone(),
        workspace_handle: handle.to_owned(),
        generation: fence.generation,
    }
}

fn prepared_state(handle: &str, owned: &OwnedWorkspace) -> WorkspacePreparedState {
    WorkspacePreparedState {
        workspace_handle: handle.to_owned(),
        workspace_path: owned.path.to_string_lossy().into_owned(),
        base_sha: owned.base_sha.clone(),
        branch: owned.branch.clone(),
        generation: owned.generation,
    }
}

fn ensure_inactive(owned: &OwnedWorkspace, active_ids: &[String]) -> CommandResult<()> {
    if owned.execution_ids.iter().any(|id| active_ids.contains(id)) {
        return Err(error(
            INVALID_INPUT,
            "workspace still has an active execution",
        ));
    }
    Ok(())
}

fn confined_existing(path: &Path, root: &Path) -> CommandResult<PathBuf> {
    daemon_fs::validate_within_root(path, root).map_err(|error| DaemonErrorPayload {
        code: OUTSIDE_WORKSPACE_ROOT.into(),
        ..error
    })
}

async fn git_common_dir(path: &Path) -> CommandResult<PathBuf> {
    let git_dir = PathBuf::from(local_git(path, &["rev-parse", "--git-common-dir"]).await?);
    let resolved = if git_dir.is_absolute() {
        git_dir
    } else {
        path.join(git_dir)
    };
    resolved.canonicalize().map_err(io_error)
}

async fn verify_git_dir(path: &Path, root: &Path) -> CommandResult<()> {
    let common = git_common_dir(path).await?;
    if !common.starts_with(root) {
        return Err(error(
            OUTSIDE_WORKSPACE_ROOT,
            "repository git directory escapes workspace_root",
        ));
    }
    Ok(())
}

async fn resolve_commit(path: &Path, revision: &str) -> CommandResult<String> {
    if revision.trim().is_empty() {
        return Err(error(INVALID_INPUT, "git revision must not be empty"));
    }
    local_git(
        path,
        &[
            "rev-parse",
            "--verify",
            "--end-of-options",
            &format!("{revision}^{{commit}}"),
        ],
    )
    .await
}

async fn validate_branch(path: &Path, branch: &str) -> CommandResult<()> {
    if branch.starts_with('-') {
        return Err(error(INVALID_INPUT, "invalid branch name"));
    }
    if local_git(path, &["check-ref-format", "--branch", branch]).await? != branch {
        return Err(error(INVALID_INPUT, "branch aliases are not accepted"));
    }
    Ok(())
}

fn validate_id(id: &str) -> CommandResult<()> {
    if id.trim().is_empty() || id.len() > 200 {
        return Err(error(
            INVALID_INPUT,
            "identifier must be non-empty and at most 200 bytes",
        ));
    }
    Ok(())
}

async fn read_bounded_file(path: &Path, limit: u64) -> CommandResult<Vec<u8>> {
    let file = tokio::fs::File::open(path).await.map_err(|failure| {
        if failure.kind() == std::io::ErrorKind::NotFound {
            error(WORKSPACE_FILE_NOT_FOUND, "workspace file not found")
        } else {
            io_error(failure)
        }
    })?;
    if !file.metadata().await.map_err(io_error)?.is_file() {
        return Err(error(INVALID_INPUT, "read target is not a regular file"));
    }
    let mut bytes = Vec::new();
    file.take(limit)
        .read_to_end(&mut bytes)
        .await
        .map_err(io_error)?;
    Ok(bytes)
}

struct BoundedOutput {
    exit_code: Option<i32>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    timed_out: bool,
    stdout_truncated: bool,
    stderr_truncated: bool,
    stdout_drain_incomplete: bool,
    stderr_drain_incomplete: bool,
}

struct ProcessGroupGuard(Option<u32>);
impl ProcessGroupGuard {
    fn kill(&mut self) {
        #[cfg(unix)]
        if let Some(id) = self.0.take() {
            let _ = std::process::Command::new("kill")
                .args(["-KILL", "--", &format!("-{id}")])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        #[cfg(windows)]
        if let Some(id) = self.0.take() {
            let _ = std::process::Command::new("taskkill")
                .args(["/F", "/T", "/PID", &id.to_string()])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}
impl Drop for ProcessGroupGuard {
    fn drop(&mut self) {
        self.kill();
    }
}

async fn bounded_command(
    command: Command,
    seconds: u64,
    cap: usize,
    keep_tail: bool,
) -> CommandResult<BoundedOutput> {
    bounded_command_inner(command, seconds, cap, keep_tail, false).await
}

async fn bounded_command_inner(
    mut command: Command,
    seconds: u64,
    cap: usize,
    keep_tail: bool,
    kill_on_success: bool,
) -> CommandResult<BoundedOutput> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command.spawn().map_err(io_error)?;
    let mut group = ProcessGroupGuard(child.id());
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| error(WORKSPACE_ERROR, "child stdout is unavailable"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| error(WORKSPACE_ERROR, "child stderr is unavailable"))?;
    let stdout_capture = Arc::new(Mutex::new(StreamCapture::default()));
    let stderr_capture = Arc::new(Mutex::new(StreamCapture::default()));
    let mut stdout_task = tokio::spawn(read_bounded_stream(
        stdout,
        cap,
        keep_tail,
        stdout_capture.clone(),
    ));
    let mut stderr_task = tokio::spawn(read_bounded_stream(
        stderr,
        cap,
        keep_tail,
        stderr_capture.clone(),
    ));
    let wait = if seconds == 0 {
        Ok(child.wait().await)
    } else {
        tokio::time::timeout(Duration::from_secs(seconds), child.wait()).await
    };
    let (exit_code, timed_out) = match wait {
        Ok(status) => (status.map_err(io_error)?.code(), false),
        Err(_) => {
            group.kill();
            let _ = child.kill().await;
            let _ = child.wait().await;
            (None, true)
        }
    };
    if kill_on_success {
        group.kill();
    } else {
        group.0 = None;
    }
    let collect = async {
        (&mut stdout_task)
            .await
            .map_err(|e| error(WORKSPACE_ERROR, e.to_string()))?
            .map_err(io_error)?;
        (&mut stderr_task)
            .await
            .map_err(|e| error(WORKSPACE_ERROR, e.to_string()))?
            .map_err(io_error)?;
        Ok::<_, DaemonErrorPayload>(())
    };
    match tokio::time::timeout(Duration::from_secs(2), collect).await {
        Ok(result) => {
            result?;
        }
        Err(_) => {
            stdout_task.abort();
            stderr_task.abort();
        }
    }
    let stdout = stdout_capture.lock().unwrap_or_else(|p| p.into_inner());
    let stderr = stderr_capture.lock().unwrap_or_else(|p| p.into_inner());
    Ok(BoundedOutput {
        exit_code,
        stdout: stdout.bytes.clone(),
        stderr: stderr.bytes.clone(),
        timed_out,
        stdout_truncated: stdout.seen > cap as u64,
        stderr_truncated: stderr.seen > cap as u64,
        stdout_drain_incomplete: !stdout.eof,
        stderr_drain_incomplete: !stderr.eof,
    })
}

#[derive(Default)]
struct StreamCapture {
    bytes: Vec<u8>,
    seen: u64,
    eof: bool,
}

async fn read_bounded_stream(
    mut stream: impl AsyncRead + Unpin,
    cap: usize,
    keep_tail: bool,
    capture: Arc<Mutex<StreamCapture>>,
) -> std::io::Result<()> {
    let mut chunk = [0_u8; 8192];
    loop {
        let count = stream.read(&mut chunk).await?;
        let mut capture = capture.lock().unwrap_or_else(|p| p.into_inner());
        if count == 0 {
            capture.eof = true;
            break;
        }
        capture.seen += count as u64;
        let tail = &mut capture.bytes;
        if !keep_tail {
            let remaining = cap.saturating_sub(tail.len());
            tail.extend_from_slice(&chunk[..remaining.min(count)]);
        } else if count >= cap {
            tail.clear();
            tail.extend_from_slice(&chunk[count - cap..count]);
        } else {
            let remove = tail.len().saturating_add(count).saturating_sub(cap);
            tail.drain(..remove);
            tail.extend_from_slice(&chunk[..count]);
        }
    }
    Ok(())
}

fn redacted_tail(
    bytes: &[u8],
    environment: &BTreeMap<String, String>,
    cap: usize,
) -> (String, bool) {
    let text = executors::environment::redact_environment_values(
        &String::from_utf8_lossy(bytes),
        environment,
    );
    let mut start = text.len().saturating_sub(cap);
    while !text.is_char_boundary(start) {
        start += 1;
    }
    (text[start..].to_owned(), start > 0)
}

async fn git_output(path: &Path, args: &[String], cap: usize) -> CommandResult<BoundedOutput> {
    let mut command = Command::new("git");
    command
        .args(args)
        .current_dir(path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE");
    let output = bounded_command(command, 30, cap, false).await?;
    if output.exit_code != Some(0) {
        return Err(error(
            WORKSPACE_ERROR,
            format!(
                "git {} failed: {}",
                args.join(" "),
                String::from_utf8_lossy(&output.stderr)
            ),
        ));
    }
    Ok(output)
}

async fn local_git(path: &Path, args: &[&str]) -> CommandResult<String> {
    let output = git_output(
        path,
        &args.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        MAX_DIFF_BYTES,
    )
    .await?;
    if output.stdout_truncated {
        return Err(error(WORKSPACE_ERROR, "git output exceeds bounded size"));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn decode<T: serde::de::DeserializeOwned>(params: Value) -> CommandResult<T> {
    serde_json::from_value(params).map_err(|e| error(INVALID_FRAME, e.to_string()))
}
fn encode<T: Serialize>(value: T) -> CommandResult<Value> {
    serde_json::to_value(value).map_err(|e| error(INVALID_FRAME, e.to_string()))
}
fn error(code: &str, message: impl Into<String>) -> DaemonErrorPayload {
    DaemonErrorPayload {
        code: code.into(),
        message: message.into(),
        details: None,
    }
}
fn io_error(error_value: std::io::Error) -> DaemonErrorPayload {
    error(WORKSPACE_ERROR, error_value.to_string())
}
fn git_error(error_value: git::GitError) -> DaemonErrorPayload {
    error(WORKSPACE_ERROR, error_value.to_string())
}
fn workspace_error(error_value: workspace::WorkspaceError) -> DaemonErrorPayload {
    error(WORKSPACE_ERROR, error_value.to_string())
}
fn storage_error(error_value: anyhow::Error) -> DaemonErrorPayload {
    error(WORKSPACE_ERROR, error_value.to_string())
}

fn interrupted_error(operation_id: &str) -> DaemonErrorPayload {
    DaemonErrorPayload {
        details: Some(serde_json::json!({
            "entry_id": operation_entry_id(operation_id),
            "operation_id": operation_id,
            "interrupted": true,
        })),
        ..error(
            DAEMON_UNAVAILABLE,
            "operation was interrupted; its outcome requires reconciliation",
        )
    }
}

async fn merge_diffstat(
    target: &Path,
    before: &str,
    after: &str,
) -> CommandResult<WorkspaceDiffStats> {
    let counts = local_git(target, &["diff", "--numstat", before, after, "--"]).await?;
    let mut stats = WorkspaceDiffStats {
        files_changed: 0,
        total_additions: 0,
        total_deletions: 0,
    };
    for line in counts.lines() {
        let fields: Vec<_> = line.split('\t').collect();
        if fields.len() >= 3 {
            stats.files_changed += 1;
            stats.total_additions += fields[0].parse::<u64>().unwrap_or(0);
            stats.total_deletions += fields[1].parse::<u64>().unwrap_or(0);
        }
    }
    Ok(stats)
}

mod inspection;
mod owner_operations;
mod reconciliation;

#[cfg(test)]
mod tests;
