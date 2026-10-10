use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::{FsEntry, UsageTelemetryState};

pub const METHOD_FS_LIST: &str = "fs.list";
pub const METHOD_FS_BRANCHES: &str = "fs.branches";
pub const METHOD_EXECUTION_START: &str = "execution.start";
pub const METHOD_EXECUTION_CANCEL: &str = "execution.cancel";
pub const METHOD_EXECUTION_LOG: &str = "execution.log";
pub const METHOD_EXECUTION_TERMINAL: &str = "execution.terminal";
pub const METHOD_JOURNAL_ACK: &str = "journal.ack";
pub const METHOD_DAEMON_HANDSHAKE: &str = "daemon.handshake";
pub const METHOD_REPO_LOCATION_VERIFY: &str = "repo_location.verify";
pub const METHOD_MACHINE_PROBE: &str = "machine.probe";
pub const METHOD_REPO_LOCATION_PROVISION: &str = "repo_location.provision";
pub const METHOD_WORKSPACE_PREPARE: &str = "workspace.prepare";
pub const METHOD_WORKSPACE_DESCRIBE: &str = "workspace.describe";
pub const METHOD_WORKSPACE_RUN: &str = "workspace.run";
pub const METHOD_WORKSPACE_CANCEL: &str = "workspace.cancel";
pub const METHOD_WORKSPACE_DIFF: &str = "workspace.diff";
pub const METHOD_WORKSPACE_READ: &str = "workspace.read";
pub const METHOD_WORKSPACE_MERGE: &str = "workspace.merge";
pub const METHOD_WORKSPACE_RESET: &str = "workspace.reset";
pub const METHOD_WORKSPACE_CLEANUP: &str = "workspace.cleanup";
pub const METHOD_TERMINAL_START: &str = "terminal.start";
pub const METHOD_TERMINAL_INPUT: &str = "terminal.input";
pub const METHOD_TERMINAL_RESIZE: &str = "terminal.resize";
pub const METHOD_TERMINAL_TERMINATE: &str = "terminal.terminate";
pub const METHOD_TERMINAL_OUTPUT: &str = "terminal.output";
pub const METHOD_TERMINAL_EXITED: &str = "terminal.exited";

pub const DAEMON_UNAVAILABLE: &str = "daemon_unavailable";
pub const DAEMON_UPGRADE_REQUIRED: &str = "daemon_upgrade_required";
pub const DAEMON_TIMEOUT: &str = "daemon_timeout";
pub const UNSUPPORTED_METHOD: &str = "unsupported_method";
pub const INVALID_FRAME: &str = "invalid_frame";
pub const INVALID_INPUT: &str = "invalid_input";
pub const PATH_GUARDRAIL: &str = "path_guardrail";
pub const EXECUTION_NOT_FOUND: &str = "execution_not_found";
pub const DAEMON_PROTOCOL_INCOMPATIBLE: &str = "daemon_protocol_incompatible";
pub const TERMINAL_REPORT_CONFLICT: &str = "terminal_report_conflict";
pub const STALE_GENERATION: &str = "stale_generation";
pub const WRONG_OWNER: &str = "wrong_owner";
pub const PURPOSE_DENIED: &str = "purpose_denied";
pub const OUTSIDE_WORKSPACE_ROOT: &str = "outside_workspace_root";
pub const WORKSPACE_FILE_NOT_FOUND: &str = "workspace_file_not_found";

/// Revision 6 added the integration owner wire: the fence announcement on
/// lookup, `integration.announce`, and Git object export / import. Revision 7
/// (one unreleased revision, two changes) lets `integration.release_objects`
/// delete the refs an attempt imported, and adds the disk facts of the
/// workspace root to every daemon report and the free-space floor to its
/// reply: a daemon that does not send them could fill its disk with work the
/// server keeps placing there.
pub const DAEMON_PROTOCOL_REVISION: u32 = 7;
/// Every command RPC requires revision 7.
pub const DAEMON_MIN_PROTOCOL_REVISION: u32 = 7;
pub const DAEMON_UPGRADE_REQUIRED_MESSAGE: &str = "upgrade the daemon to protocol revision 7 or newer by installing forge-ctl from the server's release, then restart it with the same --workspace-root; upgrade the server first, then every daemon";
pub const DAEMON_CAPABILITY_USAGE_REPORTS: &str = "execution.terminal.usage_reports";
pub const DAEMON_CAPABILITY_JOURNAL_ACK: &str = "journal.ack";
pub const DAEMON_CAPABILITY_PLAN_TRANSPORT: &str = "execution.plan_transport";
/// Remote plans reserve room for JSON escaping and the rest of the terminal report.
pub const MAX_EXECUTION_PLAN_BYTES: u64 = 128 * 1024;
pub const DAEMON_CAPABILITY_WORKSPACE: &str = "workspace.v1";
pub const DAEMON_CAPABILITY_MACHINE_PROBE: &str = "machine_probe.v1";
pub const DAEMON_CAPABILITY_REPO_PROVISION: &str = "repo_provision.v1";
pub const DAEMON_REQUIRED_CAPABILITIES: &[&str] = &[
    DAEMON_CAPABILITY_USAGE_REPORTS,
    DAEMON_CAPABILITY_JOURNAL_ACK,
];

/// Whether a daemon handshake advertises the minimum terminal accounting
/// contract. Newer revisions remain wire-compatible as long as they retain
/// the required capabilities.
pub fn daemon_protocol_is_compatible(revision: u32, capabilities: &[String]) -> bool {
    if revision < DAEMON_MIN_PROTOCOL_REVISION {
        return false;
    }
    DAEMON_REQUIRED_CAPABILITIES
        .iter()
        .all(|required| capabilities.iter().any(|capability| capability == required))
}

pub const DEFAULT_DAEMON_COMMAND_TIMEOUT_SECS: u64 = 30;
pub const DAEMON_HEARTBEAT_INTERVAL_SECS: u64 = 20;

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum DaemonFrame {
    Request {
        id: String,
        method: String,
        #[ts(type = "unknown")]
        params: serde_json::Value,
    },
    Response {
        id: String,
        #[ts(type = "unknown")]
        result: serde_json::Value,
    },
    Error {
        id: Option<String>,
        error: DaemonErrorPayload,
    },
    Notification {
        method: String,
        #[ts(type = "unknown")]
        params: serde_json::Value,
    },
    Heartbeat {
        seq: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct FsListParams {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct FsListResult {
    pub path: String,
    pub entries: Vec<FsEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct FsBranchesParams {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct FsBranchesResult {
    pub branches: Vec<String>,
    pub default_branch: Option<String>,
    pub origin_url: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum DaemonRepoLocationKind {
    PrimaryCheckout,
    ManagedClone,
    SharedMount,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct RepoLocationVerifyParams {
    pub repo_location_id: String,
    pub daemon_id: String,
    pub runtime_id: String,
    pub path: String,
    pub kind: DaemonRepoLocationKind,
    pub default_branch: String,
    pub remote_url: Option<String>,
    /// The location version this verification is stored as on the server.
    /// The owner keeps it: a verification older than the one it holds is
    /// refused, and a merge-queue claim must name exactly this version.
    pub expected_version: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe: Option<RepoLocationProbe>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct RepoLocationProbe {
    pub path: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct RepoLocationVerifyResult {
    pub repo_location_id: String,
    pub path: String,
    pub default_branch_sha: String,
    pub origin_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe_content: Option<String>,
}

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

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ExecutionStartParams {
    pub task_id: String,
    pub execution_id: String,
    pub workspace_path: String,
    pub executor_type: String,
    #[ts(type = "unknown")]
    pub executor_config: serde_json::Value,
    #[ts(type = "unknown")]
    pub prompt: serde_json::Value,
    pub max_turns: Option<u32>,
    pub plan_text: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct ExecutionStartResult {
    pub execution_id: String,
    pub accepted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct ExecutionCancelParams {
    pub execution_id: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct ExecutionCancelResult {
    pub execution_id: String,
    pub cancelled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct ExecutionLogNotification {
    pub execution_id: String,
    pub seq: u64,
    pub stream: String,
    pub line: String,
    pub ts: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_stream: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(type = "unknown")]
    pub payload: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub truncated: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct ExecutionTerminalNotification {
    // Stable identity for the complete terminal result. The daemon persists
    // this record and retains it until the authenticated server acknowledges
    // this exact report.
    pub terminal_report_id: String,
    pub execution_id: String,
    pub exit_code: Option<i32>,
    pub signal: Option<String>,
    pub error: Option<String>,
    pub ts: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_sha: Option<String>,
    // One report for every provider request/candidate attempt. This is
    // intentionally a vector: the server must not flatten fallback hops or
    // infer provider identity from executor family.
    pub usage_reports: Vec<RemoteUsageReport>,
    // Harvested by the owner, retained and acknowledged with this report.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outbox_entries: Vec<ExecutionOutboxEntry>,
    /// Exact bounded checklist candidate, retained with the terminal identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_text: Option<String>,
    // Structured failure disposition. Absent on older daemons — the server
    // then falls back to generic executor-failed handling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_class: Option<RemoteExecutionFailureClass>,
    // RFC3339 time when an unavailable executor route is worth retrying.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_candidate: Option<RemoteResolvedCandidate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_attempts: Option<Vec<RemoteRouteAttempt>>,
}

/// The same bounds as the CLI's worklog.jsonl and evidence.jsonl outbox.
pub const MAX_EXECUTION_OUTBOX_ENTRIES_PER_KIND: usize = 200;
pub const MAX_EXECUTION_OUTBOX_FILE_BYTES: u64 = 1024 * 1024;
pub const MAX_EXECUTION_OUTBOX_EVIDENCE_BYTES: u64 = 25 * 1024 * 1024;
pub const MAX_EXECUTION_OUTBOX_SUMMARY_CHARS: usize = 4_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionOutboxWorklogKind {
    Progress,
    Decision,
    Validation,
    Blocker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionOutboxEvidenceKind {
    Screenshot,
    WalkthroughVideo,
    Log,
    Report,
    Other,
}

/// Line numbers are one-based in the original file, preserving replay keys.
/// Execution, Task, Agent and role provenance comes from the execution row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExecutionOutboxEntry {
    Worklog {
        position: String,
        kind: ExecutionOutboxWorklogKind,
        summary: String,
    },
    Evidence {
        position: String,
        kind: ExecutionOutboxEvidenceKind,
        caption: String,
        // Mirrors the harness's path-or-content input. A path is provenance
        // only on the server; the owner supplies its captured bytes below.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        artifact: Option<ExecutionOutboxArtifact>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ExecutionOutboxArtifact {
    pub filename: String,
    pub content_type: String,
    pub bytes: Vec<u8>,
}

/// Missing facts are unsupported, including an executor absent from the map.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(default)]
pub struct ExecutorAdapterCapabilityFacts {
    pub structured_events: bool,
    pub usage: bool,
    pub resume: bool,
    pub cancel_ack: bool,
    pub terminal_observed: bool,
}

/// The command-stream handshake sent by a daemon immediately after an
/// authenticated connection is established. A server may dispatch work only
/// after the advertised revision and required capabilities are accepted.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct DaemonHandshakeNotification {
    pub protocol_revision: u32,
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub executor_capabilities: BTreeMap<String, ExecutorAdapterCapabilityFacts>,
    #[serde(default)]
    pub workspace_run_policy: WorkspaceRunPolicy,
}

/// A server acknowledgement for any durable daemon journal entry. The
/// acknowledgement is a request because the daemon must return a response so
/// transport failures cannot be mistaken for a durable delete.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
pub struct JournalAckParams {
    pub entry_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct JournalAckResult {
    pub entry_id: String,
    pub acknowledged: bool,
}

/// Structured failure class carried across the daemon protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum RemoteExecutionFailureClass {
    TaskFailed,
    ExecutorUnavailable,
}

/// The executor candidate that actually ran a remote execution.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
pub struct RemoteResolvedCandidate {
    pub candidate_key: String,
    pub executor_type: String,
    #[ts(type = "Record<string, unknown>")]
    pub config: serde_json::Value,
}

/// One candidate attempt outcome from a remote execution's fallback route.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
pub struct RemoteRouteAttempt {
    pub candidate_key: String,
    pub outcome: String,
}

/// Per-attempt usage transported over the daemon command stream.
///
/// Counters are nullable independently: a reported-money-only event, a
/// partially observed stream, and a producer with no telemetry are all
/// materially different from an explicit metered zero. `reported_cost_usd`
/// remains exact decimal text on the wire; clients must not parse it through a
/// binary floating-point number.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
pub struct RemoteUsageReport {
    pub report_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default)]
    pub report_sequence: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_key: Option<String>,
    #[serde(default)]
    pub attempt_ordinal: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    #[serde(default)]
    pub input_tokens: Option<u64>,
    #[serde(default)]
    pub output_tokens: Option<u64>,
    #[serde(default)]
    pub cache_read_tokens: Option<u64>,
    #[serde(default)]
    pub cache_write_tokens: Option<u64>,
    pub telemetry_state: UsageTelemetryState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_tier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reported_cost_usd: Option<String>,
    #[serde(default)]
    pub partial: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct TerminalStartParams {
    pub session_id: String,
    pub workspace_path: String,
    pub rows: u16,
    pub cols: u16,
    pub shell: Option<String>,
    pub env: Option<Vec<(String, String)>>,
    pub idle_timeout_secs: u64,
    pub max_lifetime_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct TerminalStartResult {
    pub session_id: String,
    pub pid: Option<u32>,
    pub started_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct TerminalInputParams {
    pub session_id: String,
    pub data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct TerminalInputResult {
    pub session_id: String,
    pub accepted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct TerminalResizeParams {
    pub session_id: String,
    pub rows: u16,
    pub cols: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct TerminalResizeResult {
    pub session_id: String,
    pub applied: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct TerminalTerminateParams {
    pub session_id: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct TerminalTerminateResult {
    pub session_id: String,
    pub terminated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct TerminalOutputNotification {
    pub session_id: String,
    pub data: String,
    pub ts: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct TerminalExitedNotification {
    pub session_id: String,
    pub exit_code: Option<i32>,
    pub signal: Option<String>,
    pub reason: Option<String>,
    pub ts: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct DaemonErrorPayload {
    pub code: String,
    pub message: String,
    #[ts(type = "unknown")]
    pub details: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    #[test]
    fn machine_capabilities_are_optional_at_current_revision() {
        let old: Vec<String> = super::DAEMON_REQUIRED_CAPABILITIES
            .iter()
            .map(|fact| (*fact).into())
            .collect();
        assert!(super::daemon_protocol_is_compatible(
            super::DAEMON_PROTOCOL_REVISION,
            &old
        ));
        assert_eq!(super::DAEMON_PROTOCOL_REVISION, 7);
        let probe: super::MachineProbeParams = serde_json::from_value(serde_json::json!({"daemon_id":"d","runtime_id":"r","repo_location_id":null,"commands":[{"name":"cargo","command":"cargo --version","timeout_seconds":10}],"env":{}})).unwrap();
        assert_eq!(probe.commands[0].name, "cargo");
        let provision: super::RepoLocationProvisionParams = serde_json::from_value(serde_json::json!({"daemon_id":"d","runtime_id":"r","repo_id":"repo","remote_url":"file:///repository","default_branch":"main","timeout_seconds":1800})).unwrap();
        assert!(serde_json::to_value(provision)
            .unwrap()
            .get("credentials")
            .is_none());
        for purpose in ["environment_probe", "repo_provision"] {
            assert!(
                serde_json::from_value::<super::WorkspaceRunPurpose>(serde_json::json!(purpose))
                    .is_ok()
            );
        }
    }

    use super::*;
    use crate::UsageTelemetryState;
    use serde::de::DeserializeOwned;
    use serde_json::{json, Value};

    fn assert_round_trip<T: Serialize + DeserializeOwned>(value: Value) -> T {
        let decoded: T = serde_json::from_value(value.clone()).expect("deserialize wire payload");
        assert_eq!(
            serde_json::to_value(&decoded).expect("serialize wire payload"),
            value
        );
        decoded
    }

    fn mutation_params(fields: Value) -> Value {
        let mut value = json!({
            "integration": {"kind":"task_step"},
            "daemon_id": "daemon-1",
            "runtime_id": "runtime-1",
            "placement_id": "placement-1",
            "operation_id": "operation-1",
            "generation": 2,
            "expected": { "kind": "base_sha", "sha": "base-sha" }
        });
        value
            .as_object_mut()
            .expect("mutation object")
            .extend(fields.as_object().expect("mutation fields").clone());
        value
    }

    fn workspace_params(fields: Value) -> Value {
        let mut value = json!({
            "daemon_id": "daemon-1",
            "runtime_id": "runtime-1",
            "placement_id": "placement-1",
            "workspace_handle": "opaque-handle-1",
            "generation": 2
        });
        value
            .as_object_mut()
            .expect("workspace object")
            .extend(fields.as_object().expect("workspace fields").clone());
        value
    }

    fn prepared_result() -> Value {
        json!({
            "entry_id": "journal-1",
            "operation_id": "operation-1",
            "workspace_handle": "opaque-handle-1",
            "workspace_path": "/daemon/workspaces/task-1/worktree",
            "base_sha": "base-sha",
            "branch": "forge/task-1",
            "generation": 2
        })
    }

    #[test]
    fn request_frame_round_trips() {
        let frame = DaemonFrame::Request {
            id: "req-1".to_owned(),
            method: "fs.list".to_owned(),
            params: serde_json::json!({ "path": "/tmp" }),
        };

        let json = serde_json::to_value(&frame).expect("serialize request frame");
        assert_eq!(json["type"], "request");
        assert!(json.get("id").is_some());
        assert!(json.get("method").is_some());
        assert!(json.get("params").is_some());

        let decoded: DaemonFrame = serde_json::from_value(json).expect("deserialize request frame");
        assert!(matches!(decoded, DaemonFrame::Request { .. }));
    }

    #[test]
    fn response_frame_round_trips() {
        let frame = DaemonFrame::Response {
            id: "req-1".to_owned(),
            result: serde_json::json!({ "ok": true }),
        };

        let json = serde_json::to_value(&frame).expect("serialize response frame");
        assert_eq!(json["type"], "response");

        let decoded: DaemonFrame =
            serde_json::from_value(json).expect("deserialize response frame");
        assert!(matches!(decoded, DaemonFrame::Response { .. }));
    }

    #[test]
    fn error_frame_round_trips() {
        let frame = DaemonFrame::Error {
            id: Some("req-1".to_owned()),
            error: DaemonErrorPayload {
                code: "daemon_timeout".to_owned(),
                message: "daemon timed out".to_owned(),
                details: None,
            },
        };

        let json = serde_json::to_value(&frame).expect("serialize error frame");
        assert_eq!(json["type"], "error");

        let decoded: DaemonFrame = serde_json::from_value(json).expect("deserialize error frame");
        assert!(matches!(decoded, DaemonFrame::Error { .. }));
    }

    #[test]
    fn notification_frame_round_trips() {
        let frame = DaemonFrame::Notification {
            method: "execution.log".to_owned(),
            params: serde_json::json!({
                "execution_id": "exec-1",
                "seq": 1,
                "stream": "stdout",
                "line": "started",
                "ts": "2026-05-14T00:00:00Z"
            }),
        };

        let json = serde_json::to_value(&frame).expect("serialize notification frame");
        assert_eq!(json["type"], "notification");

        let decoded: DaemonFrame =
            serde_json::from_value(json).expect("deserialize notification frame");
        assert!(matches!(decoded, DaemonFrame::Notification { .. }));
    }

    #[test]
    fn terminal_output_notification_round_trips() {
        let notification = TerminalOutputNotification {
            session_id: "term-1".to_owned(),
            data: "hello\r\n".to_owned(),
            ts: "2026-05-20T00:00:00Z".to_owned(),
        };

        let json = serde_json::to_value(&notification).expect("serialize terminal output");
        assert_eq!(json["session_id"], "term-1");
        assert_eq!(json["data"], "hello\r\n");

        let decoded: TerminalOutputNotification =
            serde_json::from_value(json).expect("deserialize terminal output");
        assert_eq!(decoded.session_id, "term-1");
        assert_eq!(decoded.data, "hello\r\n");
        assert_eq!(decoded.ts, "2026-05-20T00:00:00Z");
    }

    #[test]
    fn terminal_usage_reports_preserve_nullable_counters_and_decimal_cost() {
        let notification = ExecutionTerminalNotification {
            terminal_report_id: "terminal-report-1".to_owned(),
            execution_id: "execution-1".to_owned(),
            exit_code: Some(0),
            signal: None,
            error: None,
            ts: "2026-05-20T00:00:00Z".to_owned(),
            status: Some("completed".to_owned()),
            agent_session_id: None,
            summary: None,
            after_sha: None,
            outbox_entries: vec![],
            plan_text: None,
            usage_reports: vec![RemoteUsageReport {
                report_id: "report-1".to_owned(),
                request_id: Some("request-1".to_owned()),
                report_sequence: 0,
                candidate_key: Some("candidate-a".to_owned()),
                attempt_ordinal: 1,
                provider_id: Some("openai".to_owned()),
                model_id: Some("gpt-5".to_owned()),
                input_tokens: None,
                output_tokens: Some(12),
                cache_read_tokens: Some(0),
                cache_write_tokens: None,
                telemetry_state: UsageTelemetryState::Metered,
                context_tokens: Some(20),
                selected_tier: Some("short".to_owned()),
                reported_cost_usd: Some("0.000000001".to_owned()),
                partial: true,
            }],
            failure_class: None,
            retry_at: None,
            resolved_candidate: None,
            route_attempts: None,
        };

        let value = serde_json::to_value(&notification).expect("terminal serializes");
        assert_eq!(value["terminal_report_id"], "terminal-report-1");
        assert_eq!(
            value["usage_reports"][0]["input_tokens"],
            serde_json::Value::Null
        );
        assert_eq!(
            value["usage_reports"][0]["reported_cost_usd"],
            "0.000000001"
        );

        let decoded: ExecutionTerminalNotification =
            serde_json::from_value(value).expect("terminal deserializes");
        assert_eq!(decoded.usage_reports, notification.usage_reports);
    }

    #[test]
    fn terminal_notification_without_report_vector_is_rejected() {
        let value = serde_json::json!({
            "terminal_report_id": "terminal-report-1",
            "execution_id": "execution-1",
            "exit_code": 0,
            "signal": null,
            "error": null,
            "ts": "2026-05-20T00:00:00Z"
        });
        assert!(serde_json::from_value::<ExecutionTerminalNotification>(value).is_err());
    }

    #[test]
    fn daemon_protocol_gate_requires_revision_and_capabilities() {
        let capabilities = DAEMON_REQUIRED_CAPABILITIES
            .iter()
            .map(|capability| (*capability).to_owned())
            .collect::<Vec<_>>();
        assert!(daemon_protocol_is_compatible(
            DAEMON_PROTOCOL_REVISION,
            &capabilities
        ));
        assert!(!daemon_protocol_is_compatible(
            DAEMON_MIN_PROTOCOL_REVISION - 1,
            &capabilities
        ));
        assert!(!daemon_protocol_is_compatible(
            DAEMON_PROTOCOL_REVISION,
            &capabilities[..1]
        ));
        assert!(daemon_protocol_is_compatible(
            DAEMON_PROTOCOL_REVISION + 1,
            &capabilities
        ));
        let revision_2_capabilities = vec![
            DAEMON_CAPABILITY_USAGE_REPORTS.to_owned(),
            "execution.terminal.ack".to_owned(),
        ];
        assert!(!daemon_protocol_is_compatible(2, &revision_2_capabilities));
        assert!(!daemon_protocol_is_compatible(
            DAEMON_MIN_PROTOCOL_REVISION,
            &revision_2_capabilities[..1]
        ));
        assert!(!daemon_protocol_is_compatible(
            DAEMON_PROTOCOL_REVISION,
            &revision_2_capabilities
        ));
        assert_eq!(DAEMON_PROTOCOL_REVISION, 7);
        assert_eq!(DAEMON_MIN_PROTOCOL_REVISION, 7);
    }

    #[test]
    fn workspace_handshake_round_trips_adapter_facts_and_run_policy() {
        let handshake = assert_round_trip::<DaemonHandshakeNotification>(json!({
            "protocol_revision": DAEMON_PROTOCOL_REVISION,
            "capabilities": [
                DAEMON_CAPABILITY_USAGE_REPORTS,
                DAEMON_CAPABILITY_JOURNAL_ACK,
                DAEMON_CAPABILITY_WORKSPACE
            ],
            "executor_capabilities": {
                "codex": {
                    "structured_events": true,
                    "usage": true,
                    "resume": false,
                    "cancel_ack": true,
                    "terminal_observed": true
                }
            },
            "workspace_run_policy": {
                "allowed_purposes": ["ci_step", "hook", "environment_setup"]
            }
        }));
        assert_eq!(DAEMON_CAPABILITY_WORKSPACE, "workspace.v1");
        assert!(handshake.executor_capabilities["codex"].structured_events);
        assert!(!handshake.executor_capabilities["codex"].resume);
        assert_eq!(handshake.workspace_run_policy.allowed_purposes.len(), 3);
    }

    #[test]
    fn missing_adapter_facts_and_run_policy_are_unsupported() {
        let handshake: DaemonHandshakeNotification = serde_json::from_value(json!({
            "protocol_revision": 2,
            "capabilities": ["execution.terminal.usage_reports", "execution.terminal.ack"]
        }))
        .expect("revision 2 handshake remains readable");
        assert!(handshake.executor_capabilities.is_empty());
        assert!(handshake.workspace_run_policy.allowed_purposes.is_empty());

        let facts: ExecutorAdapterCapabilityFacts =
            serde_json::from_value(json!({ "structured_events": true }))
                .expect("partial capability facts");
        assert!(facts.structured_events);
        assert!(!facts.usage);
        assert!(!facts.resume);
        assert!(!facts.cancel_ack);
        assert!(!facts.terminal_observed);
    }

    #[test]
    fn repo_location_verify_round_trips() {
        assert_eq!(METHOD_REPO_LOCATION_VERIFY, "repo_location.verify");
        assert_round_trip::<RepoLocationVerifyParams>(json!({
            "repo_location_id": "location-1",
            "daemon_id": "daemon-1",
            "runtime_id": "runtime-1",
            "path": "/daemon/repos/project",
            "kind": "shared_mount",
            "default_branch": "main",
            "remote_url": "https://example.com/project.git",
            "expected_version": 4,
            "probe": { "path": "/daemon/repos/project/probe", "content": "probe-1" }
        }));
        assert_round_trip::<RepoLocationVerifyResult>(json!({
            "repo_location_id": "location-1",
            "path": "/daemon/repos/project",
            "default_branch_sha": "base-sha",
            "origin_url": null,
            "probe_content": "probe-1"
        }));
    }

    #[test]
    fn workspace_prepare_round_trips() {
        assert_eq!(METHOD_WORKSPACE_PREPARE, "workspace.prepare");
        let params = assert_round_trip::<WorkspacePrepareParams>(mutation_params(json!({
            "repo_location_id": "location-1",
            "workspace_id": "workspace-1",
            "task_id": "task-1",
            "base_ref": "main",
            "branch": "forge/task-1"
        })));
        assert_eq!(params.fence.operation_id, "operation-1");
        assert_eq!(params.fence.generation, 2);
        assert_round_trip::<WorkspacePrepareResult>(prepared_result());
    }

    #[test]
    fn workspace_describe_round_trips_execution_reconciliation_ids() {
        assert_eq!(METHOD_WORKSPACE_DESCRIBE, "workspace.describe");
        assert_round_trip::<WorkspaceDescribeParams>(workspace_params(json!({})));
        let result = assert_round_trip::<WorkspaceDescribeResult>(json!({
            "workspace_handle": "opaque-handle-1",
            "generation": 2,
            "exists": true,
            "head_sha": "head-sha",
            "dirty": false,
            "branch": "forge/task-1",
            "locked": true,
            "active_execution_ids": ["execution-active"],
            "journaled_execution_ids": ["execution-finished"]
        }));
        assert_eq!(result.active_execution_ids, ["execution-active"]);
        assert_eq!(result.journaled_execution_ids, ["execution-finished"]);
    }

    #[test]
    fn workspace_run_round_trips_purpose_and_output_bounds() {
        assert_eq!(METHOD_WORKSPACE_RUN, "workspace.run");
        assert_round_trip::<WorkspaceRunParams>(mutation_params(json!({
            "workspace_handle": "opaque-handle-1",
            "purpose": "ci_step",
            "command": "cargo test -p api-types daemon_transport::tests",
            "env": [["CI", "true"]],
            "timeout_secs": 120,
            "max_output_bytes": 4096
        })));
        assert_round_trip::<WorkspaceRunResult>(json!({
            "entry_id": "journal-1",
            "operation_id": "operation-1",
            "exit_code": 1,
            "stdout": "output tail\n",
            "stderr": "error tail\n",
            "duration_ms": 1234,
            "timed_out": false,
            "stdout_truncated": true,
            "stderr_truncated": false,
            "stdout_drain_incomplete": false,
            "stderr_drain_incomplete": false
        }));
        assert!(serde_json::from_value::<WorkspaceRunPurpose>(json!("shell")).is_err());
        assert_round_trip::<WorkspaceRunPolicy>(json!({ "allowed_purposes": ["ci_step"] }));
    }

    #[test]
    fn workspace_diff_round_trips() {
        assert_eq!(METHOD_WORKSPACE_DIFF, "workspace.diff");
        assert_round_trip::<WorkspaceDiffParams>(workspace_params(json!({
            "base_ref": "base-sha",
            "head_ref": null,
            "max_bytes": 4096
        })));
        assert_round_trip::<WorkspaceDiffResult>(json!({
            "base_ref": "main",
            "head_ref": "forge/task-1",
            "base_sha": "base-sha",
            "head_sha": "head-sha",
            "files": [{ "path": "README.md", "status": "modified", "additions": 2, "deletions": 1 }],
            "stats": { "files_changed": 1, "total_additions": 2, "total_deletions": 1 },
            "diff": "diff --git a/README.md b/README.md\n",
            "truncated": false
        }));
    }

    #[test]
    fn workspace_read_round_trips_binary_content() {
        assert_eq!(METHOD_WORKSPACE_READ, "workspace.read");
        assert_round_trip::<WorkspaceReadParams>(workspace_params(json!({
            "path": "artifacts/screenshot.png",
            "limit": 1024
        })));
        let result = assert_round_trip::<WorkspaceReadResult>(json!({
            "path": "artifacts/screenshot.png",
            "bytes": [0, 137, 255],
            "truncated": true
        }));
        assert_eq!(result.bytes, [0, 137, 255]);
    }

    #[test]
    fn workspace_merge_round_trips_embedded_outcomes_and_evidence() {
        assert_eq!(METHOD_WORKSPACE_MERGE, "workspace.merge");
        assert_round_trip::<WorkspaceMergeParams>(mutation_params(json!({
            "workspace_handle": "opaque-handle-1",
            "repo_location_id": "location-1",
            "target_branch": "main",
            "expected_target_sha": "target-sha",
            "handed_off_paths": ["src/main.rs"]
        })));
        for outcome in [
            json!({ "kind": "done", "before_sha": "target-sha", "after_sha": "merged-sha", "branch": "main" }),
            json!({ "kind": "conflict", "details": "conflict", "conflict_paths": ["src/main.rs"] }),
            json!({ "kind": "dirty", "files": ["src/main.rs"] }),
            json!({ "kind": "target_dirty", "files": ["README.md"] }),
            json!({ "kind": "review_required", "reason": "reviewed commit changed" }),
            json!({ "kind": "target_moved", "reason": "target changed", "target_branch": "main" }),
            json!({ "kind": "unresolved_conflict_markers", "paths": ["src/main.rs"] }),
        ] {
            assert_round_trip::<WorkspaceMergeResult>(json!({
                "entry_id": "journal-1",
                "operation_id": "operation-1",
                "outcome": outcome,
                "diffstat": { "files_changed": 1, "total_additions": 2, "total_deletions": 1 }
            }));
        }
    }

    #[test]
    fn workspace_reset_round_trips() {
        assert_eq!(METHOD_WORKSPACE_RESET, "workspace.reset");
        assert_round_trip::<WorkspaceResetParams>(mutation_params(json!({
            "workspace_handle": "opaque-handle-1",
            "base_ref": "main",
            "branch": "forge/task-1"
        })));
        let mut result = prepared_result();
        result["generation"] = json!(3);
        assert_round_trip::<WorkspaceResetResult>(result);
    }

    #[test]
    fn workspace_cleanup_round_trips_version_fence_and_journal_identity() {
        assert_eq!(METHOD_WORKSPACE_CLEANUP, "workspace.cleanup");
        let mut params = mutation_params(json!({ "workspace_handle": "opaque-handle-1" }));
        params["expected"] = json!({ "kind": "version", "version": 4 });
        let params = assert_round_trip::<WorkspaceCleanupParams>(params);
        assert_eq!(
            params.fence.expected,
            WorkspaceOperationExpected::Version { version: 4 }
        );
        assert_round_trip::<WorkspaceCleanupResult>(json!({
            "entry_id": "journal-cleanup-1",
            "operation_id": "operation-1",
            "workspace_handle": "opaque-handle-1",
            "generation": 2,
            "cleaned": true
        }));
    }

    #[test]
    fn journal_ack_round_trips_without_execution_specific_fields() {
        assert_eq!(METHOD_JOURNAL_ACK, "journal.ack");
        assert_round_trip::<JournalAckParams>(json!({ "entry_id": "journal-1" }));
        assert_round_trip::<JournalAckResult>(json!({
            "entry_id": "journal-1",
            "acknowledged": true
        }));
        assert!(serde_json::from_value::<JournalAckParams>(json!({
            "terminal_report_id": "terminal-1",
            "execution_id": "execution-1"
        }))
        .is_err());
    }

    #[test]
    fn absent_plan_preserves_retained_terminal_payload() {
        let value = json!({"terminal_report_id":"retained", "execution_id":"execution", "exit_code":0,
            "signal":null, "error":null, "ts":"now", "usage_reports":[]});
        let report: ExecutionTerminalNotification = serde_json::from_value(value.clone()).unwrap();
        assert!(report.plan_text.is_none());
        assert_eq!(serde_json::to_value(report).unwrap(), value);
    }

    #[test]
    fn execution_terminal_outbox_round_trips_plan_worklog_and_evidence() {
        let value = json!({
            "terminal_report_id": "terminal-1",
            "execution_id": "execution-1",
            "exit_code": 0,
            "signal": null,
            "error": null,
            "ts": "2026-09-29T00:00:00Z",
            "usage_reports": [],
            "plan_text": "- [ ] transported plan\n",
            "outbox_entries": [
                { "type": "worklog", "position": "3", "kind": "validation", "summary": "20 tests passed" },
                { "type": "evidence", "position": "1", "kind": "log", "caption": "test log", "content": "20 tests OK\n" },
                {
                    "type": "evidence", "position": "4", "kind": "screenshot",
                    "caption": "captured result", "path": "artifacts/shot.png",
                    "artifact": { "filename": "shot.png", "content_type": "image/png", "bytes": [137, 80, 78, 71, 0, 255] }
                }
            ]
        });
        let terminal = assert_round_trip::<ExecutionTerminalNotification>(value.clone());
        assert_eq!(terminal.outbox_entries.len(), 3);
        assert_eq!(
            terminal.plan_text.as_deref(),
            Some("- [ ] transported plan\n")
        );
        assert!(matches!(
            &terminal.outbox_entries[0],
            ExecutionOutboxEntry::Worklog {
                position,
                ..
            } if position == "3"
        ));
        let mut older = value;
        older
            .as_object_mut()
            .expect("terminal object")
            .remove("outbox_entries");
        let terminal: ExecutionTerminalNotification =
            serde_json::from_value(older).expect("revision 2 terminal notification");
        assert!(terminal.outbox_entries.is_empty());
    }

    #[test]
    fn workspace_review_owner_operations_round_trip() {
        let workspace = json!({"daemon_id":"daemon", "runtime_id":"runtime", "placement_id":"placement",
            "workspace_handle":"opaque", "generation":1});
        let mut request = workspace.clone();
        request["operation"] = json!("git");
        request["query"] =
            json!({"kind":"candidate_paths", "base_sha":"base", "commit_sha":"candidate"});
        request["optional"] = json!(false);
        request["limit"] = json!(1024);
        assert_round_trip::<WorkspaceInspectParams>(request);
        let mut request = workspace.clone();
        request["operation"] = json!("files");
        request["path"] = json!("docs");
        request["repository"] = json!(true);
        request["max_entries"] = json!(5);
        request["max_bytes"] = json!(8192);
        assert_round_trip::<WorkspaceInspectParams>(request);
        let mut request = workspace.clone();
        request["operation_id"] = json!("operation");
        request["integration"] = json!({"kind":"task_step"});
        request["expected"] = json!({"kind":"base_sha", "sha":"candidate"});
        request["operation"] = json!({"kind":"review_checkout", "commit_sha":"candidate",
            "environment":{"env":{}, "assets":[], "checks":[],
                "recheck_interval_seconds":600}, "prepare":true});
        assert_round_trip::<WorkspaceOwnerOperationParams>(request);
        let mut request = workspace;
        request["integration"] = json!({"kind":"task_step"});
        request["operation"] = json!("reconcile");
        request["operation_id"] = json!("operation");
        assert_round_trip::<WorkspaceReconcileParams>(request);
        assert_round_trip::<WorkspaceReconcileResult>(
            json!({"entry_id":"entry", "operation_id":"operation",
            "outcome":{"kind":"error", "error":{"code":DAEMON_UNAVAILABLE,"message":"interrupted","details":null}},
            "owner_fence":null}),
        );
        let lookup = assert_round_trip::<WorkspaceReconcileResult>(
            json!({"entry_id":"entry", "operation_id":"operation",
            "outcome":{"kind":"result", "result":{}},
            "owner_fence":{"queue_id":"queue","generation":null,"attempt_id":null,"intent":"unknown"}}),
        );
        assert_eq!(
            lookup.owner_fence.unwrap().intent,
            IntegrationIntentRecord::Unknown
        );
        assert_round_trip::<IntegrationAnnounceParams>(
            json!({"daemon_id":"d","runtime_id":"r","fence":{"queue_id":"queue","attempt_id":"attempt","generation":2,"lease_owner":"worker","target_owner":{}},"live_queue_ids":["queue"]}),
        );
        assert_round_trip::<ImportObjectsResult>(
            json!({"kind":"imported","receipt":{"key":"a-1-out","tip_sha":"abc","ref_name":"refs/forge/integration/a-1-out","replayed":true}}),
        );
        assert_round_trip::<ObjectTransferRefusal>(
            json!({"kind":"too_large","bytes":3,"max_bytes":2}),
        );
        assert_eq!(
            object_transfer_key("attempt", 7, ObjectTransferDirection::Inbound),
            "attempt-7-in"
        );
        assert_eq!(MAX_OBJECT_TRANSFER_BYTES, 256 * 1024 * 1024);
    }

    #[test]
    fn workspace_error_codes_round_trip() {
        for (code, expected) in [
            (STALE_GENERATION, "stale_generation"),
            (WRONG_OWNER, "wrong_owner"),
            (PURPOSE_DENIED, "purpose_denied"),
            (OUTSIDE_WORKSPACE_ROOT, "outside_workspace_root"),
        ] {
            assert_eq!(code, expected);
            assert_round_trip::<DaemonErrorPayload>(json!({
                "code": code,
                "message": "operation refused",
                "details": { "placement_id": "placement-1" }
            }));
        }
    }
}

pub const METHOD_CHECK_RUN: &str = "check.run";
pub const METHOD_CHECK_LOOKUP: &str = "check.lookup";
pub const METHOD_CHECK_CANCEL: &str = "check.cancel";

/// Paths are resolved by the physical owner from existing handles/locations.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DaemonCheckTarget {
    Workspace {
        workspace: WorkspaceHandleReference,
    },
    ExactCommit {
        daemon_id: String,
        runtime_id: String,
        repo_location_id: String,
        commit_sha: String,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
pub struct DaemonCheckRunParams {
    pub operation_id: String,
    pub target: DaemonCheckTarget,
    pub purpose: WorkspaceRunPurpose,
    pub spec: crate::CheckSpec,
    pub env: Vec<(String, String)>,
    /// Owner cleanup policy, bound by spec.execution_policy, with its own limit.
    pub cleanup_commands: Vec<crate::CheckCommandSpec>,
    pub cleanup_timeout_ms: u64,
    /// Absolute UTC wall deadline; retries cannot extend it.
    pub deadline: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
pub struct DaemonCheckOperationParams {
    pub daemon_id: String,
    pub operation_id: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DaemonCheckResult {
    Running {
        operation_id: String,
    },
    Completed {
        receipt: Box<crate::CheckReceipt>,
    },
    /// No terminal receipt after an owner restart/handler interruption.
    Interrupted {
        operation_id: String,
    },
    Unknown {
        operation_id: String,
    },
}

pub const METHOD_INTEGRATION_ANNOUNCE: &str = "integration.announce";
pub const METHOD_INTEGRATION_EXPORT_OBJECTS: &str = "integration.export_objects";
pub const METHOD_INTEGRATION_IMPORT_OBJECTS: &str = "integration.import_objects";
pub const METHOD_INTEGRATION_RELEASE_OBJECTS: &str = "integration.release_objects";
/// Error code of a typed object-transfer refusal; `details.refusal` is an
/// [`ObjectTransferRefusal`].
pub const OBJECT_TRANSFER_REFUSED: &str = "object_transfer_refused";
/// Largest Git object transfer between two checkouts.
pub const MAX_OBJECT_TRANSFER_BYTES: u64 = 256 * 1024 * 1024;
/// Largest decoded chunk in one export / import frame.
pub const MAX_OBJECT_TRANSFER_CHUNK_BYTES: u64 = 1024 * 1024;

/// The first message of a queue claim generation. The owner records the fence
/// as its high-water mark for the queue (an older generation is refused
/// `stale_fence`) and prunes what the new mark makes unreachable.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct IntegrationAnnounceParams {
    pub daemon_id: String,
    pub runtime_id: String,
    pub fence: IntegrationOwnerFence,
    /// Every queue that still targets this owner. When present, fences of
    /// other queues are dropped.
    pub live_queue_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct IntegrationAnnounceResult {
    pub queue_id: String,
    /// The fence held for the queue before this announcement.
    pub previous: Option<IntegrationOwnerFence>,
    pub generation: i64,
    pub pruned_fences: u32,
    pub pruned_entries: u32,
}

/// Why an object transfer was refused. No refusal changes a repository.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[ts(export)]
pub enum ObjectTransferRefusal {
    TooLarge {
        #[ts(type = "number")]
        bytes: u64,
        #[ts(type = "number")]
        max_bytes: u64,
    },
    Invalid {
        reason: String,
    },
    MissingObject {
        sha: String,
    },
    KeyConflict {
        existing_sha: String,
    },
    StaleFence,
}

/// Read one chunk of the bundle holding `want` minus `have`. The first call
/// for a key writes the bundle to owner-local staging; later calls (and
/// retries) read the same file until it is released.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ExportObjectsParams {
    pub daemon_id: String,
    pub runtime_id: String,
    pub fence: IntegrationOwnerFence,
    /// Idempotency key, `[A-Za-z0-9._-]{1,128}`; see `object_transfer_key`.
    pub key: String,
    pub repo_location_id: String,
    pub have: Vec<String>,
    pub want: String,
    #[ts(type = "number")]
    pub max_bytes: u64,
    #[ts(type = "number")]
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ObjectExportReceipt {
    pub key: String,
    pub tip_sha: String,
    /// Zero when the receiver's `have` commits already contain the tip.
    #[ts(type = "number")]
    pub total_bytes: u64,
    /// Lower-case hex SHA-256 of the whole bundle.
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ExportObjectsResult {
    pub receipt: ObjectExportReceipt,
    #[ts(type = "number")]
    pub offset: u64,
    /// Standard base64 of at most [`MAX_OBJECT_TRANSFER_CHUNK_BYTES`].
    pub data: String,
    pub eof: bool,
}

/// One piece of a bundle being sent to an owner. Chunks arrive in order:
/// `offset` must equal the bytes already stored (a repeat of bytes already
/// stored is accepted and ignored; a gap is refused and drops the staging).
/// `offset` 0 always starts the key over. Nothing past `total_bytes`, and
/// nothing past the 256 MiB cap, is written.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ObjectChunk {
    #[ts(type = "number")]
    pub total_bytes: u64,
    pub sha256: String,
    #[ts(type = "number")]
    pub offset: u64,
    pub data: String,
    /// The owner verifies and imports once the last chunk is stored.
    pub last: bool,
}

/// Without a chunk this only asks whether the key was already imported.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ImportObjectsParams {
    pub daemon_id: String,
    pub runtime_id: String,
    pub fence: IntegrationOwnerFence,
    pub key: String,
    pub repo_location_id: String,
    pub expected_tip_sha: String,
    #[ts(type = "number")]
    pub max_bytes: u64,
    pub chunk: Option<ObjectChunk>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ObjectImportReceipt {
    pub key: String,
    pub tip_sha: String,
    /// Always `refs/forge/integration/<key>`.
    pub ref_name: String,
    /// The key was already imported; nothing was transferred this time.
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[ts(export)]
pub enum ImportObjectsResult {
    Absent,
    Receiving {
        #[ts(type = "number")]
        received_bytes: u64,
    },
    Imported {
        receipt: ObjectImportReceipt,
    },
}

/// Drop a key's owner-local staging (export bundle, partial import). With
/// `attempt`, also delete every ref the attempt imported in one checkout
/// (`refs/forge/integration/<attempt>-*`) and the attempt's other staging.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ReleaseObjectsParams {
    pub daemon_id: String,
    pub runtime_id: String,
    pub key: String,
    #[serde(default)]
    pub attempt: Option<ReleaseAttemptRefs>,
}

/// The attempt whose imported refs a release deletes, and where.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ReleaseAttemptRefs {
    pub attempt_id: String,
    pub repo_location_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ReleaseObjectsResult {
    pub key: String,
    pub removed: bool,
    /// Imported refs deleted for `attempt`.
    #[serde(default)]
    pub removed_refs: u32,
}

/// `inbound` brings the target tip to the Task's checkout before a rebase;
/// `outbound` sends the candidate to the default checkout before the
/// fast-forward.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ObjectTransferDirection {
    Inbound,
    Outbound,
}

/// One transfer per attempt, claim generation and direction.
pub fn object_transfer_key(
    attempt_id: &str,
    generation: i64,
    direction: ObjectTransferDirection,
) -> String {
    let direction = match direction {
        ObjectTransferDirection::Inbound => "in",
        ObjectTransferDirection::Outbound => "out",
    };
    format!("{attempt_id}-{generation}-{direction}")
}
