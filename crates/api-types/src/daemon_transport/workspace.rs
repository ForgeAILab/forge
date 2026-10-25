//! Daemon workspace handles, owner fences, probes, and operation payloads.

use super::*;

/// Owner identity and generation accompany handle reads as well as mutations.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspaceHandleReference {
    pub daemon_id: String,
    pub runtime_id: String,
    pub placement_id: String,
    pub workspace_handle: String,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkspaceOperationExpected {
    BaseSha { sha: String },
    Version { version: i64 },
}

/// Frozen queue owner identity; deliberately contains no owner-local path.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct IntegrationOwnerFence {
    pub queue_id: String,
    pub attempt_id: String,
    pub generation: i64,
    pub lease_owner: String,
    #[ts(type = "unknown")]
    pub target_owner: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum WorkspaceIntegrationKind {
    Merge,
    FastForward,
    Rebase,
    Check,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct WorkspaceIntegrationRequest {
    pub fence: IntegrationOwnerFence,
    pub kind: WorkspaceIntegrationKind,
    /// Placement, expected objects and exact effect input, frozen by recorder.
    #[ts(type = "unknown")]
    pub witness: serde_json::Value,
}

impl WorkspaceIntegrationRequest {
    /// The owner that performs this effect. The fence names the owner of the
    /// queue's default checkout; a rebase of a Task whose checkout is on
    /// another machine carries its own owner as `witness.effect_owner`
    /// (`location_id`, `owner_kind`, `daemon_id`, `runtime_id`).
    pub fn effect_owner(&self) -> &serde_json::Value {
        self.witness
            .get("effect_owner")
            .filter(|owner| owner.is_object())
            .unwrap_or(&self.fence.target_owner)
    }

    /// The effect is performed by another owner than the fence's target owner.
    pub fn off_target_owner(&self) -> bool {
        self.witness
            .get("effect_owner")
            .is_some_and(serde_json::Value::is_object)
    }

    /// Stable attempt/effect/fence identity in the existing bounded journal.
    pub fn operation_id(&self) -> String {
        let kind = match self.kind {
            WorkspaceIntegrationKind::Merge => "merge",
            WorkspaceIntegrationKind::FastForward => "fast_forward",
            WorkspaceIntegrationKind::Rebase => "rebase",
            WorkspaceIntegrationKind::Check => "check",
        };
        let step = self
            .fence
            .lease_owner
            .strip_prefix("task-step:")
            .map(|id| format!(":{id}"))
            .unwrap_or_default();
        format!(
            "integration:{}:{}:{kind}{step}",
            self.fence.attempt_id, self.fence.generation
        )
    }
}

/// Task-step authority remains distinct from the passive queue's lease. Both
/// use the same owner effects. Since revision 4 every request carries this tagged field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[ts(export)]
pub enum WorkspaceIntegrationBinding {
    TaskStep,
    TaskStepEffect {
        request: WorkspaceIntegrationRequest,
    },
    Attempt {
        request: WorkspaceIntegrationRequest,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum IntegrationOwnerRefusal {
    StaleFence,
    ForeignOwner,
    WitnessMismatch,
    ReconciliationRequired,
    RequestConflict,
}

/// Prepare uses the placement id before its owner has issued a handle.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct WorkspaceMutationFence {
    pub integration: WorkspaceIntegrationBinding,
    pub daemon_id: String,
    pub runtime_id: String,
    pub placement_id: String,
    pub operation_id: String,
    pub generation: u64,
    pub expected: WorkspaceOperationExpected,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspacePrepareParams {
    #[serde(flatten)]
    pub fence: WorkspaceMutationFence,
    pub repo_location_id: String,
    pub workspace_id: String,
    pub task_id: String,
    pub base_ref: String,
    pub branch: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspacePreparedState {
    pub workspace_handle: String,
    // The owner's path is for execution.start on that owner only.
    pub workspace_path: String,
    pub base_sha: String,
    pub branch: String,
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspacePrepareResult {
    pub entry_id: String,
    pub operation_id: String,
    #[serde(flatten)]
    pub workspace: WorkspacePreparedState,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspaceDescribeParams {
    #[serde(flatten)]
    pub workspace: WorkspaceHandleReference,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspaceDescribeResult {
    pub workspace_handle: String,
    pub generation: u64,
    pub exists: bool,
    pub head_sha: Option<String>,
    pub dirty: bool,
    pub branch: Option<String>,
    pub locked: bool,
    pub active_execution_ids: Vec<String>,
    pub journaled_execution_ids: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceRunPurpose {
    EnvironmentSetup,
    EnvironmentProbe,
    RepoProvision,
    Hook,
    CiStep,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct MachineProbeCommand {
    pub name: String,
    pub command: String,
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct MachineProbeParams {
    pub daemon_id: String,
    pub runtime_id: String,
    pub repo_location_id: Option<String>,
    pub commands: Vec<MachineProbeCommand>,
    pub env: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct MachineProbeCommandResult {
    pub name: String,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub output_tail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct MachineProbeResult {
    pub results: Vec<MachineProbeCommandResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct RepoLocationProvisionParams {
    pub default_branch: String,
    pub timeout_seconds: u64,
    pub daemon_id: String,
    pub runtime_id: String,
    pub repo_id: String,
    pub remote_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct RepoLocationProvisionResult {
    pub workspace_root: String,
    pub path: String,
    pub default_branch: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct WorkspaceRunPolicy {
    // Missing policy information grants no purposes; the daemon's local
    // configuration supplies its effective allow list.
    #[serde(default)]
    pub allowed_purposes: Vec<WorkspaceRunPurpose>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspaceRunParams {
    #[serde(flatten)]
    pub fence: WorkspaceMutationFence,
    pub workspace_handle: String,
    pub purpose: WorkspaceRunPurpose,
    pub command: String,
    #[serde(default)]
    pub env: Vec<(String, String)>,
    /// Zero preserves the CI runner's unbounded duration (ci_step only).
    pub timeout_secs: u64,
    /// u64::MAX accepts bounded CI output tails; other values are strict per-stream budgets.
    pub max_output_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct WorkspaceCancelParams {
    pub operation_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum WorkspaceCancelState {
    Killed,
    AlreadyFinished,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct WorkspaceCancelResult {
    pub operation_id: String,
    pub state: WorkspaceCancelState,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspaceRunResult {
    pub entry_id: String,
    pub operation_id: String,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub duration_ms: u64,
    pub timed_out: bool,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    /// The shell exited while descendants still held the output pipe open.
    #[serde(default)]
    pub stdout_drain_incomplete: bool,
    #[serde(default)]
    pub stderr_drain_incomplete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspaceDiffParams {
    #[serde(flatten)]
    pub workspace: WorkspaceHandleReference,
    pub base_ref: String,
    // Absent means include the current worktree, as the embedded diff does.
    pub head_ref: Option<String>,
    pub max_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceDiffFileStatus {
    Added,
    Modified,
    Deleted,
    Renamed,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspaceDiffFile {
    pub path: String,
    pub status: WorkspaceDiffFileStatus,
    pub additions: u64,
    pub deletions: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspaceDiffStats {
    pub files_changed: u64,
    pub total_additions: u64,
    pub total_deletions: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspaceDiffResult {
    pub base_ref: String,
    pub head_ref: String,
    pub base_sha: String,
    pub head_sha: String,
    pub files: Vec<WorkspaceDiffFile>,
    pub stats: WorkspaceDiffStats,
    pub diff: String,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspaceReadParams {
    #[serde(flatten)]
    pub workspace: WorkspaceHandleReference,
    pub path: String,
    pub limit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspaceReadResult {
    pub path: String,
    pub bytes: Vec<u8>,
    pub truncated: bool,
}

/// Fixed Git queries, never an arbitrary Git argv or a shell command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkspaceGitQuery {
    Head,
    ResolveRef {
        reference: String,
    },
    MergeBase {
        base_ref: String,
        head_ref: String,
    },
    CandidatePaths {
        base_sha: String,
        commit_sha: String,
    },
    MarkerPaths {
        base: String,
        head: String,
    },
    IsAncestor {
        base: String,
        head: String,
    },
    TrackedChanges,
    StatusPorcelain,
    BranchExists {
        branch: String,
    },
    RebaseInProgress,
    TargetHead {
        branch: String,
    },
    /// `git status --porcelain` of the checkout the workspace's repo location
    /// names (the default checkout when the Task is placed in it), not of the
    /// Task's own worktree.
    TargetStatusPorcelain,
}

/// Additional structured workspace.read inputs used by owner-local services.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum WorkspaceInspectParams {
    Git {
        #[serde(flatten)]
        workspace: WorkspaceHandleReference,
        query: WorkspaceGitQuery,
        optional: bool,
        limit: u64,
    },
    Paths {
        #[serde(flatten)]
        workspace: WorkspaceHandleReference,
    },
    Files {
        #[serde(flatten)]
        workspace: WorkspaceHandleReference,
        path: String,
        /// Read the verified repository location instead of the worktree.
        repository: bool,
        max_entries: u64,
        max_bytes: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspaceFileContent {
    pub path: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkspaceInspectResult {
    Git {
        output: Option<String>,
    },
    Paths {
        workspace_path: String,
        repo_path: String,
    },
    Files {
        files: Vec<WorkspaceFileContent>,
    },
}

/// Review's three-dot diff and fallback, with its existing truncation marker.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspaceReviewDiffParams {
    #[serde(flatten)]
    pub workspace: WorkspaceHandleReference,
    pub operation: WorkspaceReviewDiffOperation,
    pub default_branch: String,
    pub max_bytes: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceReviewDiffOperation {
    Review,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspaceReviewDiffResult {
    pub diff: String,
}

/// Owner-local operations carried by workspace.reset. These retain the current
/// generation; only the ordinary recreation request advances it.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[ts(export)]
pub enum WorkspaceOwnerOperation {
    PublishPlan {
        execution_id: String,
        content: String,
    },
    RestorePlan {
        execution_id: String,
    },
    DiscardPlan {
        execution_id: String,
    },
    MaterializeAssets {
        environment: crate::ProjectEnvironment,
    },
    ReviewCheckout {
        commit_sha: String,
        environment: crate::ProjectEnvironment,
        prepare: bool,
    },
    ReleaseReviewCheckout,
    RestoreCandidate {
        commit_sha: String,
    },
    RebaseTarget {
        target_branch: String,
        handoff_conflicts: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspaceOwnerOperationParams {
    #[serde(flatten)]
    pub fence: WorkspaceMutationFence,
    pub workspace_handle: String,
    pub operation: WorkspaceOwnerOperation,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkspaceOwnerOperationOutcome {
    Applied,
    ReviewCheckout {
        workspace_handle: String,
    },
    Rebased,
    Conflict {
        details: String,
        conflict_paths: Vec<String>,
    },
    UnsupportedConflict {
        details: String,
    },
    Dirty {
        files: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspaceOwnerOperationResult {
    pub entry_id: String,
    pub operation_id: String,
    pub outcome: WorkspaceOwnerOperationOutcome,
}

/// Integrate the frozen reviewed object with --ff-only when review evidence is
/// present. The mutation fence binds HEAD, including for a no-agent review.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspaceReviewedMergeParams {
    #[serde(flatten)]
    pub merge: WorkspaceMergeParams,
    pub reviewed_commit_sha: Option<String>,
    /// Set by a queue claim whose Task checkout is not a worktree of this
    /// default checkout (another machine, or another clone): the candidate
    /// is the commit this claim generation's outbound object transfer bound
    /// (`refs/forge/integration/<key>`), not the HEAD of a worktree this
    /// owner holds. `workspace_handle` and `placement_id` are then empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub candidate_transfer_key: Option<String>,
}

/// Finish an interrupted run/merge intent without executing it again. A merge
/// can succeed only when the verified target is exactly the frozen candidate;
/// an interrupted run has no recoverable exit code and finishes as an error.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct WorkspaceReconcileParams {
    pub integration: WorkspaceIntegrationBinding,
    #[serde(flatten)]
    pub workspace: WorkspaceHandleReference,
    pub operation: WorkspaceReconcileOperation,
    pub operation_id: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceReconcileOperation {
    Reconcile,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkspaceReconcileOutcome {
    Result {
        #[ts(type = "unknown")]
        result: serde_json::Value,
    },
    Error {
        error: DaemonErrorPayload,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspaceReconcileResult {
    pub entry_id: String,
    pub operation_id: String,
    pub outcome: WorkspaceReconcileOutcome,
    /// Present for a queue attempt lookup: what this owner knew about the
    /// attempt's queue and intent before it answered.
    pub owner_fence: Option<IntegrationFenceAnnouncement>,
}

/// What the owner's own records said about a looked-up attempt intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum IntegrationIntentRecord {
    /// The journal held the intent; the receipt is the owner's evidence.
    Retained,
    /// The owner already knew this claim generation and holds no intent for
    /// the operation: it never ran here, and it is now fenced off.
    NotPerformed,
    /// The owner had never been told of this claim generation, so its empty
    /// journal proves nothing. The server announces a generation before it
    /// marks any effect of it started, so this means lost or replaced owner
    /// state. Never a permission to repeat the effect.
    Unknown,
}

/// The owner's fence for a queue as it was before a lookup, and what its
/// journal said. Lets the server tell "not performed" from "unknown".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct IntegrationFenceAnnouncement {
    pub queue_id: String,
    /// Highest claim generation recorded for the queue; `None` when the owner
    /// had no fence for it.
    pub generation: Option<i64>,
    pub attempt_id: Option<String>,
    pub intent: IntegrationIntentRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspaceMergeParams {
    #[serde(flatten)]
    pub fence: WorkspaceMutationFence,
    pub workspace_handle: String,
    pub repo_location_id: String,
    pub target_branch: String,
    pub expected_target_sha: String,
    pub handed_off_paths: Vec<String>,
}

/// Direct-merge outcomes match the embedded backend's outcomes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkspaceMergeOutcome {
    ReviewRequired {
        reason: String,
    },
    TargetMoved {
        reason: String,
        target_branch: String,
    },
    Done {
        before_sha: String,
        after_sha: String,
        branch: String,
    },
    Conflict {
        details: String,
        conflict_paths: Vec<String>,
    },
    Dirty {
        files: Vec<String>,
    },
    TargetDirty {
        files: Vec<String>,
    },
    UnresolvedConflictMarkers {
        paths: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspaceMergeResult {
    pub entry_id: String,
    pub operation_id: String,
    pub outcome: WorkspaceMergeOutcome,
    pub diffstat: Option<WorkspaceDiffStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspaceResetParams {
    #[serde(flatten)]
    pub fence: WorkspaceMutationFence,
    pub workspace_handle: String,
    pub base_ref: String,
    pub branch: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspaceResetResult {
    pub entry_id: String,
    pub operation_id: String,
    #[serde(flatten)]
    pub workspace: WorkspacePreparedState,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspaceCleanupParams {
    #[serde(flatten)]
    pub fence: WorkspaceMutationFence,
    pub workspace_handle: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspaceCleanupResult {
    pub entry_id: String,
    pub operation_id: String,
    pub workspace_handle: String,
    pub generation: u64,
    pub cleaned: bool,
}
