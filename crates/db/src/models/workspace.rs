//! Workspaces, placements, runtime resources, terminal sessions, agent status, skills, and notifications.

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Workspace {
    pub id: String,
    pub task_id: String,
    pub repo_id: String,
    pub(crate) worktree_path: String,
    pub branch: String,
    pub status: WorkspaceStatus,
    pub before_sha: Option<String>,
    pub cleanup_after: Option<String>,
    pub cleanup_attempts: i64,
    pub last_cleanup_error: Option<String>,
    pub error: Option<String>,
    /// Disk bytes of the Task root as last measured by the garbage-collection
    /// sweep; `None` until a walk of it has finished.
    pub disk_bytes: Option<i64>,
    pub disk_measured_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl Workspace {
    /// Backend-only access to the legacy server path. Consumers must resolve
    /// the placement through `WorkspaceBackendRouter` instead.
    pub fn embedded_worktree_path_for_backend(&self) -> &str {
        &self.worktree_path
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum WorkspaceStatus {
    Creating,
    Ready,
    Error,
    Cleaning,
    Cleaned,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlacementOwnerKind {
    Server,
    Daemon,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlacementState {
    Reserved,
    Preparing,
    Ready,
    Disconnected,
    Cleaning,
    Cleaned,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlacementSelectedBy {
    Scheduler,
    Pin,
    Inherited,
    Backfill,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlacementFailureCause {
    PlacementUnavailable,
    PrepareFailed,
    OwnerDisconnected,
    OwnerDisconnectedTimeout,
    OwnerLostExecution,
    StaleGeneration,
    WrongOwner,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspacePlacement {
    pub id: String,
    pub workspace_id: String,
    pub task_id: String,
    pub agent_id: Option<String>,
    pub owner_kind: PlacementOwnerKind,
    pub daemon_id: Option<String>,
    pub runtime_id: Option<String>,
    pub repo_location_id: String,
    pub execution_daemon_id: Option<String>,
    pub workspace_handle: Option<String>,
    pub generation: i64,
    pub state: PlacementState,
    pub selected_by: PlacementSelectedBy,
    pub selection_reason: String,
    pub reserved_until: Option<String>,
    pub disconnected_at: Option<String>,
    pub failure_cause: Option<PlacementFailureCause>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

/// Scheduler-issued authority for one assigned Task/repository operation.
/// This row is internal: callers receive neither its capability payload nor
/// any filesystem path or bearer token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceLease {
    pub id: String,
    pub project_id: String,
    pub task_id: String,
    /// Exact Task revision admitted for this execution attempt.
    pub task_version: i64,
    /// Execution identity for the attempt; a lease cannot be replayed by a
    /// later execution of the same Task.
    pub execution_id: String,
    /// Stable operation key for replaying one scheduler lease issue.
    pub operation_idempotency_key: String,
    pub repository_binding_id: String,
    pub base_ref: String,
    pub role: String,
    pub capabilities_json: String,
    pub assigned_principal_type: String,
    pub assigned_principal_id: String,
    pub capability_profile_revision: String,
    pub capability_profile_digest: String,
    pub issuing_principal_type: String,
    pub issuing_principal_id: String,
    pub status: String,
    pub issued_at: String,
    pub expires_at: String,
    pub revoked_at: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Daemon {
    pub run_limit: Option<u32>,
    pub max_concurrent_runs: Option<i64>,
    /// The disk facts of its workspace root from its last report, as JSON
    /// ([`api_types::MachineDiskFacts`]). `None` until it reports one.
    pub disk_json: Option<String>,
    pub id: String,
    pub machine_id: String,
    pub hostname: String,
    pub os: String,
    pub arch: String,
    pub agent_version: Option<String>,
    pub labels_json: String,
    pub status: DaemonStatus,
    pub last_report_at: Option<String>,
    pub registration_token_hash: Option<String>,
    pub detected_clis_json: String,
    pub owner_id: Option<String>,
    pub visibility: String,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DaemonStatus {
    Online,
    Offline,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Runtime {
    pub id: String,
    pub daemon_id: String,
    pub kind: String,
    pub workspace_root: String,
    pub status: RuntimeStatus,
    pub labels_json: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeStatus {
    Ready,
    Degraded,
    Offline,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentStatus {
    Idle,
    Busy,
    Error,
    Offline,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skill {
    pub id: String,
    pub project_id: String,
    pub name: String,
    pub content: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notification {
    pub id: String,
    pub project_id: String,
    pub task_id: Option<String>,
    pub event_type: String,
    pub title: String,
    pub body: Option<String>,
    pub read: bool,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateNotification {
    pub id: String,
    pub project_id: String,
    pub task_id: Option<String>,
    pub event_type: String,
    pub title: String,
    pub body: Option<String>,
    pub read: bool,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalSessionStatus {
    Starting,
    Running,
    Exited,
    Terminated,
    TimedOut,
    Orphaned,
    CleanupTerminated,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalSession {
    pub id: String,
    pub task_id: String,
    pub workspace_id: String,
    pub daemon_id: Option<String>,
    pub status: TerminalSessionStatus,
    pub rows: i64,
    pub cols: i64,
    pub pid: Option<i64>,
    pub exit_code: Option<i64>,
    pub exit_signal: Option<String>,
    pub exit_reason: Option<String>,
    pub created_by_user_id: String,
    pub created_at: String,
    pub started_at: Option<String>,
    pub last_activity_at: Option<String>,
    pub ended_at: Option<String>,
    pub version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTerminalSession {
    pub id: String,
    pub task_id: String,
    pub workspace_id: String,
    pub daemon_id: Option<String>,
    pub created_by_user_id: String,
    pub rows: i64,
    pub cols: i64,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateTerminalSessionStatus {
    pub status: TerminalSessionStatus,
    pub started_at: Option<String>,
    pub last_activity_at: Option<String>,
    pub ended_at: Option<String>,
    pub pid: Option<i64>,
    pub exit_code: Option<i64>,
    pub exit_signal: Option<String>,
    pub exit_reason: Option<String>,
}
