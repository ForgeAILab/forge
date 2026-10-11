//! Public reads of the per-target merge queue: where a Task is in line and
//! why it waits. Every type here is a projection; none is a command.
//!
//! The enums are open. They are stable snake_case strings and the queue will
//! grow more of them (states, wait codes, hints), so a client must tolerate a
//! value it does not know: show it neutrally, never fail the read. Rust
//! clients get that from the `unknown` variant every enum here ends with
//! (`#[serde(other)]`); the server never sends the literal `unknown`.
//! Timestamps are RFC 3339 strings. No lease owner, runtime id or machine
//! path is part of any type here; `machine_id` is the public id of
//! `GET /api/v1/daemons`.
use crate::IntegrationPhase;
use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// The state of one target branch's queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum IntegrationQueueState {
    /// Members merge one at a time.
    Open,
    /// The target is not ready (no default checkout, or its machine is
    /// gone): nothing merges until it is.
    Suspended,
    /// A merge into the target has an unknown result: nothing merges until
    /// its machine answers or is removed.
    Quarantined,
    Closed,
    /// A value this build does not know (a newer server). Never sent.
    #[serde(other)]
    Unknown,
}

/// Why a queue is not `open`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct IntegrationQueueStateReason {
    /// Stable cause code (`target_unconfigured`, `target_unavailable`, ...).
    pub code: String,
    pub message: Option<String>,
}

/// Where one Task stands in the merge of its change. The same vocabulary as
/// the Task condition's integration reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum IntegrationMemberState {
    /// In line, or stopped in line by its owner or its Project (see `wait`).
    Waiting,
    /// At the head: Forge is merging it (see `phase`).
    Owned,
    /// Sent back to its coder: a conflict or a dirty worktree to repair.
    Repair,
    /// Sent back for a fresh review.
    ReviewRequired,
    /// The check of the rebased change failed.
    CandidateCheckFailed,
    /// The merge stopped and waits (see `wait`).
    Deferred,
    /// The change is on the target; the Task is being closed.
    Applied,
    /// A value this build does not know (a newer server). Never sent.
    #[serde(other)]
    Unknown,
}

/// The typed cause of a wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum IntegrationWaitCode {
    /// Other members are ahead.
    Queue,
    /// The queue's target is not ready.
    QueueSuspended,
    /// The queue waits for the unknown result of another merge.
    QueueQuarantined,
    /// The owner's Hold on the Task.
    TaskHeld,
    TaskBlocked,
    TaskFailed,
    TaskEntryBlocked,
    TaskAwaitingHuman,
    ProjectPaused,
    /// The machine that owns the checkout is not connected (see `machine_id`).
    OwnerOffline,
    /// The target checkout has uncommitted changes.
    TargetDirty,
    /// A failure Forge retries by itself.
    Infrastructure,
    /// The automatic retries are spent.
    BudgetExhausted,
    /// Only the owner can continue the merge.
    OwnerRequired,
    /// The result of a merge is unknown on its machine.
    UnresolvedResult,
    /// A cancel was requested and is being applied.
    CancelRequested,
    /// A value this build does not know (a newer server). Never sent.
    #[serde(other)]
    Unknown,
}

/// Why a Task's merge is not moving.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct IntegrationWait {
    pub code: IntegrationWaitCode,
    pub message: Option<String>,
    /// The machine the wait is on, when it is one.
    pub machine_id: Option<String>,
    pub machine_name: Option<String>,
    /// When Forge tries again by itself, when it will.
    pub retry_at: Option<String>,
}

/// The action that moves a waiting merge, as a hint: the Task's
/// `available_actions` is the authority on what the caller may do. `retry`
/// is hinted exactly when the action system offers `integration_retry` for
/// the Task (the hint is computed by the function the offer is built from).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum IntegrationActionHint {
    /// Task action `retry` (offered as `integration_retry`).
    Retry,
    /// Task action `release`.
    Release,
    /// Resume the Project (`POST /api/v1/projects/{id}/resume`): a Project
    /// action, so it is never in a Task's `available_actions`.
    ResumeProject,
    /// A value this build does not know (a newer server). Never sent.
    #[serde(other)]
    Unknown,
}

/// Times of one attempt. Durations are cumulative over its rounds and absent
/// until the phase has run.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct IntegrationTimings {
    pub enqueued_at: String,
    /// When the attempt took the head of its queue; `null` while it waits.
    pub head_since: Option<String>,
    /// Times the attempt started at the head (a lost race starts another).
    #[ts(type = "number")]
    pub rounds: i64,
    #[ts(type = "number | null")]
    pub queued_ms: Option<i64>,
    #[ts(type = "number | null")]
    pub rebase_ms: Option<i64>,
    #[ts(type = "number | null")]
    pub check_ms: Option<i64>,
    #[ts(type = "number | null")]
    pub fast_forward_ms: Option<i64>,
}

/// A Task's place in the merge queue. `null` on a Task that has no attempt in
/// progress (never queued, merged, or cancelled).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TaskIntegrationSummary {
    pub attempt_id: String,
    pub queue_id: Option<String>,
    pub repo_id: Option<String>,
    /// The branch the Task merges into.
    pub target_branch: Option<String>,
    pub queue_state: Option<IntegrationQueueState>,
    pub state: IntegrationMemberState,
    /// Set while `state` is `owned`.
    pub phase: Option<IntegrationPhase>,
    /// Members ahead: 0 at the head, `null` when the Task is not in line.
    pub position: Option<u32>,
    pub wait: Option<IntegrationWait>,
    pub action_hint: Option<IntegrationActionHint>,
    pub timings: IntegrationTimings,
}

/// One member of a queue, in queue order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct IntegrationQueueMemberResponse {
    pub task_id: String,
    /// `null` when the Task was deleted.
    pub task_title: Option<String>,
    pub task_status: Option<String>,
    pub integration: TaskIntegrationSummary,
}

/// One target branch's merge queue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct IntegrationQueueResponse {
    pub id: String,
    pub repo_id: String,
    pub target_branch: String,
    pub state: IntegrationQueueState,
    /// Set when `state` is not `open`.
    pub state_reason: Option<IntegrationQueueStateReason>,
    pub head_task_id: Option<String>,
    pub head_attempt_id: Option<String>,
    pub head_since: Option<String>,
    /// Members with an attempt in progress, including those sent back.
    pub member_count: u32,
    /// The head first, then by queue order; at most 100.
    pub members: Vec<IntegrationQueueMemberResponse>,
    pub members_truncated: bool,
}

/// `GET /api/v1/repos/{id}/integration-queues`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct IntegrationQueuesResponse {
    pub items: Vec<IntegrationQueueResponse>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A client built today reads a summary a later server sends: values it
    /// does not know become `unknown`, and the read does not fail.
    #[test]
    fn unknown_enum_values_are_read_not_refused() {
        let summary: TaskIntegrationSummary = serde_json::from_value(serde_json::json!({
            "attempt_id": "a", "queue_id": "q", "repo_id": "r", "target_branch": "main",
            "queue_state": "draining", "state": "speculating", "phase": null, "position": null,
            "wait": {"code": "merge_train", "message": null, "machine_id": null, "machine_name": null, "retry_at": null},
            "action_hint": "reorder",
            "timings": {"enqueued_at": "2026-10-01T00:00:00+00:00", "head_since": null, "rounds": 0, "queued_ms": null, "rebase_ms": null, "check_ms": null, "fast_forward_ms": null}
        }))
        .unwrap();
        assert_eq!(summary.queue_state, Some(IntegrationQueueState::Unknown));
        assert_eq!(summary.state, IntegrationMemberState::Unknown);
        assert_eq!(summary.wait.unwrap().code, IntegrationWaitCode::Unknown);
        assert_eq!(summary.action_hint, Some(IntegrationActionHint::Unknown));
        // Known values are the stable snake_case strings.
        assert_eq!(
            serde_json::to_value((
                IntegrationQueueState::Quarantined,
                IntegrationMemberState::CandidateCheckFailed,
                IntegrationWaitCode::TaskAwaitingHuman,
                IntegrationActionHint::ResumeProject,
            ))
            .unwrap(),
            serde_json::json!([
                "quarantined",
                "candidate_check_failed",
                "task_awaiting_human",
                "resume_project"
            ])
        );
    }
}
