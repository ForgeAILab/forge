//! Executions, leases, progress, and terminal outcomes.

use super::*;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Execution {
    pub id: String,
    pub task_id: String,
    pub agent_id: Option<String>,
    pub role: String,
    pub status: ExecutionStatus,
    pub stop_reason: Option<StopReason>,
    pub stopped_by: Option<String>,
    pub resume_policy: Option<ResumePolicy>,
    pub stopped_at: Option<String>,
    pub parent_execution_id: Option<String>,
    pub agent_session_id: Option<String>,
    pub agent_message_id: Option<String>,
    pub last_activity_at: Option<String>,
    pub prompt: Option<String>,
    pub summary: Option<String>,
    pub logs_path: Option<String>,
    pub before_sha: Option<String>,
    pub after_sha: Option<String>,
    pub error: Option<String>,
    pub executor_config_snapshot_json: Option<String>,
    pub workspace_id: Option<String>,
    /// Monotonic compare-and-swap version for the owner lease and terminal
    /// lifecycle.  This is deliberately separate from Task/version state.
    pub execution_version: i64,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<String>,
    pub hard_deadline_at: Option<String>,
    pub last_heartbeat_at: Option<String>,
    pub last_progress_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

pub type ExecutionRole = String;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ExecutionStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum StopReason {
    UserCancelled,
    TaskCancelled,
    RoleReassigned,
    GracefulShutdown,
    CrashRecovery,
    AgentTimeout,
    ExecutionStalled,
    DaemonDisconnected,
    ExecutorCancelled,
    ExecutorFailed,
    LegacyUnknown,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ResumePolicy {
    Auto,
    Manual,
    None,
}

/// Scheduler-owned lease claim. A claim is valid only for the expected
/// execution version and either an unowned/expired lease. When policy supplies
/// a hard deadline, the first claim establishes that immutable deadline for
/// the attempt; `None` leaves the execution bounded only by its renewable
/// owner lease and explicit cancellation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClaimExecutionLease {
    pub execution_id: String,
    pub expected_version: i64,
    pub owner: String,
    pub lease_expires_at: String,
    pub hard_deadline_at: Option<String>,
    pub now: String,
}

/// Fixed-cadence owner heartbeat.  Renewal changes only liveness metadata and
/// never extends the profile/capability hard deadline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenewExecutionLease {
    pub execution_id: String,
    pub expected_version: i64,
    pub owner: String,
    pub lease_expires_at: String,
    pub now: String,
}

/// Semantic progress is intentionally not a heartbeat.  The owner/version
/// compare-and-swap prevents a late runner from mutating current execution
/// truth after losing its lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordExecutionProgress {
    pub execution_id: String,
    pub expected_version: i64,
    pub owner: String,
    pub progress_at: String,
    pub now: String,
}

/// Atomically claim the right to publish one stale-progress warning.  The
/// expected progress value is part of the CAS so a semantic event that arrives
/// concurrently wins over the monitor and suppresses a stale warning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordExecutionProgressWarning {
    pub execution_id: String,
    pub expected_version: i64,
    pub owner: String,
    pub expected_last_progress_at: Option<String>,
    pub stale_before: String,
    pub now: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionLeaseDisposition {
    Revoke,
    Expire,
}

/// One terminal transition input shared by runner, cancellation, and monitor
/// callers.  The optional owner is a compare-and-swap predicate when supplied;
/// monitor and runner callers should always provide it, while an authorized
/// user cancellation may intentionally rely on the execution version alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalizeExecution {
    pub execution_id: String,
    pub expected_version: i64,
    pub lease_owner: Option<String>,
    pub status: ExecutionStatus,
    pub stop_reason: Option<Option<StopReason>>,
    pub stopped_by: Option<Option<String>>,
    pub stopped_at: Option<Option<String>>,
    pub resume_policy: Option<Option<ResumePolicy>>,
    pub agent_session_id: Option<Option<String>>,
    pub agent_message_id: Option<Option<String>>,
    pub last_activity_at: Option<Option<String>>,
    pub last_progress_at: Option<Option<String>>,
    pub summary: Option<Option<String>>,
    pub logs_path: Option<Option<String>>,
    pub before_sha: Option<Option<String>>,
    pub after_sha: Option<Option<String>>,
    pub error: Option<Option<String>>,
    pub executor_config_snapshot_json: Option<Option<String>>,
    pub updated_at: String,
    pub actor_type: String,
    pub actor_id: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
    pub causation_depth: i64,
    pub lease_disposition: ExecutionLeaseDisposition,
}

/// A fully materialized usage settlement that is committed with an execution
/// terminal CAS. Event payloads are built before entering this boundary so a
/// persistence retry can reuse the exact same ledger identity and values
/// without rerunning the provider.
#[derive(Debug, Clone, PartialEq)]
pub struct UsageLedgerSettlement {
    pub invocation_id: String,
    pub expected_version: i64,
    pub telemetry_state: UsageTelemetryState,
    pub terminal_reason: Option<String>,
    pub settled_at: String,
    pub updated_at: String,
    pub events: Vec<CreateUsageEvent>,
}

/// Private artifact content committed only by the winning terminal CAS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportedExecutionPlan {
    pub content: Option<String>,
    pub error: Option<String>,
    pub size: Option<i64>,
}

/// Composite terminalization input for Task execution. The optional receipt
/// identity is used by remote daemon delivery: it is persisted in the
/// terminal domain event and lets a post-restart duplicate be acknowledged
/// without mutating the execution or appending usage a second time.
#[derive(Debug, Clone, PartialEq)]
pub struct TerminalizeExecutionWithLedger {
    pub plan: Option<TransportedExecutionPlan>,
    pub terminal: TerminalizeExecution,
    pub settlements: Vec<UsageLedgerSettlement>,
    pub terminal_report_id: Option<String>,
    pub terminal_report_digest: Option<String>,
    /// Recovery may prove that a local provider result cannot be replayed
    /// after this terminal CAS. In that case pending invocations are marked
    /// `unsettled` in the same transaction instead of leaving a crash window
    /// between terminalization and a follow-up sweep. Remote daemon-owned
    /// calls leave this false so their retained terminal report can drain.
    pub mark_unreplayable_pending_unsettled: bool,
    /// A remote terminal report can arrive after another owner already won
    /// the execution terminal CAS (most notably a locally recovered
    /// cancellation). Such a report may still settle pending invocations and
    /// must receive a durable receipt, but it must never change the terminal
    /// execution outcome.
    pub allow_late_settlement: bool,
    /// Recovery of a daemon-owned execution may still receive a replayable
    /// terminal report after this domain transition. Keep started/pending
    /// invocations pending instead of closing them as an unmetered result.
    pub preserve_pending_settlement: bool,
    /// True only for a remote owner reporting its own terminal outcome. Such a
    /// caller must prove a live lease and hard deadline at the same CAS
    /// boundary, so a daemon cannot terminalize an execution it no longer owns.
    ///
    /// Local callers — a user cancel, the recovery reaper, the in-process
    /// runner — are the authority that revokes a lease rather than a holder
    /// proving one, and an expired lease is the state they exist to clear.
    /// They must not be held to the owner's liveness proof.
    pub require_live_owner_lease: bool,
}

/// Durable identity for a remote terminal report. The report id is globally
/// unique, while the digest binds the id to the complete immutable payload.
/// Receipts are retained independently of the in-memory daemon transport so a
/// replay after a server restart can be acknowledged exactly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionTerminalReceipt {
    pub terminal_report_id: String,
    pub execution_id: String,
    pub payload_digest: String,
    pub event_id: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionLeaseMutation {
    Updated(Execution),
    Concurrent { current: Option<Execution> },
    HardDeadline { current: Option<Execution> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionProgressWarningOutcome {
    Committed {
        execution: Execution,
        event: DomainEvent,
    },
    Replayed {
        execution: Execution,
        event: DomainEvent,
    },
    Concurrent {
        current: Option<Execution>,
    },
}

/// The terminal CAS result is deliberately not represented by
/// `DbError::VersionConflict`: callers must be able to distinguish a winning
/// committed identity from a concurrent loser without performing a stale
/// read.  Only `Committed` is allowed to cascade Task state or publish a
/// terminal event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionTerminalOutcome {
    Committed {
        execution: Execution,
        event: Box<DomainEvent>,
        workspace_lease_id: Option<String>,
        workspace_lease_status: Option<String>,
        /// True when this outcome came from an exact durable terminal-report
        /// receipt replay. Transport callers must acknowledge it without
        /// rerunning post-commit cascades or publishing duplicate events.
        replayed: bool,
    },
    Concurrent {
        current: Option<Execution>,
    },
}
