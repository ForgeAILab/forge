mod workspace;

pub use workspace::*;

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
/// `workspace.prepare` (a new or recreated worktree) and an exact-commit
/// `check.run` refused because the daemon's own disk reading is under the
/// free-space floor. Nothing was created; the same request is admitted once
/// the machine has room.
pub const DISK_PRESSURE: &str = "disk_pressure";
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
mod tests;

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
pub const METHOD_INTEGRATION_TARGET_FACTS: &str = "integration.target_facts";
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

/// Read-only facts about a default checkout this owner holds, for a queue
/// head whose Task is placed in another checkout. Nothing is written.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct IntegrationTargetFactsParams {
    pub daemon_id: String,
    pub runtime_id: String,
    pub repo_location_id: String,
    pub target_branch: String,
    /// The head's candidate commit. It may be unknown to this checkout.
    pub candidate_sha: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct IntegrationTargetFactsResult {
    /// Tip of `target_branch` in the checkout.
    pub target_tip: String,
    /// The candidate is known here and is an ancestor of the tip.
    pub candidate_in_target: bool,
    /// The checkout has uncommitted changes.
    pub dirty: bool,
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
