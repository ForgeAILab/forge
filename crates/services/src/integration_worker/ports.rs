//! The worker's seams. It consumes three ports (Task steps, read-only facts,
//! object transfer) and provides two (enqueue notification, snapshot).
use crate::{integration_effects::EffectWorkspace, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use db::{
    IntegrationAttempt, IntegrationAttemptState, IntegrationCheckTiming, IntegrationFailureKind,
    IntegrationOwnerFence, IntegrationPhaseTimings, IntegrationQueue, IntegrationQueueState,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// The actions of the `integration` Task-step kind the worker asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationStepAction {
    RequestCheck,
    Settle,
    Result,
    SendBack,
    Park,
    Clear,
}
impl IntegrationStepAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RequestCheck => "request_check",
            Self::Settle => "settle",
            Self::Result => "result",
            Self::SendBack => "send_back",
            Self::Park => "park",
            Self::Clear => "clear",
        }
    }
}

/// Payload of one `integration` Task step. `(task_id, causation_key)` makes
/// the enqueue idempotent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrationStepRequest {
    pub task_id: String,
    pub queue_id: String,
    pub attempt_id: String,
    pub expected_epoch: i64,
    /// The fence generation (slot generation) the worker holds.
    pub generation: i64,
    pub effect_seq: i64,
    pub action: IntegrationStepAction,
}
impl IntegrationStepRequest {
    pub fn causation_key(&self) -> String {
        format!(
            "integration:{}:{}:{}",
            self.attempt_id,
            self.effect_seq,
            self.action.as_str()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationStepOutcome {
    /// `settle`: `permit_json` was written in the same transaction.
    Permit,
    CandidateCheckFailed,
    NeedsReview,
    /// The Task is no longer `merging` at the admitted epoch.
    TaskLeft,
    /// No verdict (check infrastructure exhausted, step could not decide).
    Infrastructure,
    /// `result`: the Task was advanced.
    Done,
}

/// What a Task step writes to `effect_ack_json` (with `acknowledged_at`) in
/// the transaction of its Task write. The worker only reads it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrationStepAck {
    pub effect_seq: i64,
    pub generation: i64,
    pub action: IntegrationStepAction,
    pub outcome: IntegrationStepOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub check: Option<IntegrationCheckTiming>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}
impl IntegrationStepAck {
    /// The acknowledgment of `action` for the attempt as it is now. A settle
    /// permit binds the slot generation; a result does not (the fast-forward
    /// it reports is done, whoever holds the lease now).
    pub fn current(attempt: &IntegrationAttempt, action: IntegrationStepAction) -> Option<Self> {
        attempt.acknowledged_at.as_ref()?;
        let ack: Self = serde_json::from_value(attempt.effect_ack_json.clone()?).ok()?;
        (ack.effect_seq == attempt.effect_seq
            && ack.action == action
            && (action != IntegrationStepAction::Settle
                || ack.generation == attempt.slot_generation))
            .then_some(ack)
    }
}

/// Task-step handshake. The worker never writes Task state: it enqueues a
/// step and reads the acknowledgment the step leaves on the attempt.
///
/// Contract for the step side: a step is enqueued *before* the attempt
/// transition it belongs to, so a step whose attempt is not yet in the state
/// its action names retries; one whose attempt moved past `effect_seq` or
/// `generation` finishes without a write.
#[async_trait]
pub trait IntegrationStepPort: Send + Sync {
    async fn enqueue_step(&self, request: &IntegrationStepRequest) -> Result<()>;
    /// Make the protected `result` step of this attempt runnable now (it was
    /// pre-enqueued by `settle` with the permit deadline). Idempotent; a
    /// missing step is not an error.
    async fn ready_result_step(&self, attempt_id: &str, effect_seq: i64) -> Result<()>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskGate {
    /// Still `merging` at the attempt's `expected_epoch`.
    Live,
    /// Left that status entry (cancelled, moved, done, deleted).
    Left,
    ProjectPaused,
}

/// Read-only facts about the head, taken in one call.
#[derive(Debug, Clone)]
pub struct HeadFacts {
    pub gate: TaskGate,
    pub workspace: EffectWorkspace,
    pub task_branch: String,
    /// HEAD of the Task's checkout.
    pub candidate_head: String,
    /// Tip of the target branch in the default checkout.
    pub target_tip: String,
    /// The target tip is an ancestor of the candidate: nothing to rebase.
    pub target_in_candidate: bool,
    /// The candidate is an ancestor of the target tip: it has landed.
    pub candidate_in_target: bool,
    pub worktree_dirty: bool,
    pub target_dirty: bool,
    pub rebase_in_progress: bool,
    /// The Task's checkout shares the default checkout's object store.
    pub shared_object_store: bool,
}

/// No Task, Review, event or Git-write capability.
#[async_trait]
pub trait IntegrationFactsPort: Send + Sync {
    async fn head_facts(
        &self,
        attempt: &IntegrationAttempt,
        queue: &IntegrationQueue,
    ) -> Result<HeadFacts>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectTransferDirection {
    /// Target tip to the Task's checkout, before the rebase.
    Inbound,
    /// Candidate to the default checkout, before the fast-forward.
    Outbound,
}
#[derive(Debug, Clone)]
pub struct ObjectTransferRequest {
    pub fence: IntegrationOwnerFence,
    pub workspace: EffectWorkspace,
    pub target_branch: String,
    pub direction: ObjectTransferDirection,
    pub have: String,
    pub want: String,
    pub max_bytes: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectTransferOutcome {
    Transferred { bytes: u64 },
    TooLarge { bytes: u64 },
}
/// Moves commits between a non-default checkout and the default checkout.
/// Idempotent by `(fence.attempt_id, direction, want)`.
#[async_trait]
pub trait ObjectTransferPort: Send + Sync {
    async fn transfer(&self, request: ObjectTransferRequest) -> Result<ObjectTransferOutcome>;
}

/// Provided by the worker. The step that admits an attempt calls it after
/// its commit; a lost call is covered by the periodic sweep.
pub trait IntegrationEnqueuePort: Send + Sync {
    fn notify_enqueued(&self, queue_id: &str);
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct IntegrationMemberSnapshot {
    pub attempt_id: String,
    pub task_id: String,
    pub queue_seq: i64,
    pub attempt_number: i64,
    /// Members ahead of this one (0 for the head); `None` when it is not
    /// waiting in line (ejected, parked, ...).
    pub position: Option<u32>,
    pub state: IntegrationAttemptState,
    pub cancel_requested: bool,
    pub failure_kind: Option<IntegrationFailureKind>,
    pub failure_message: Option<String>,
    pub available_at: Option<String>,
    pub timings: Option<IntegrationPhaseTimings>,
}
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct IntegrationQueueSnapshot {
    pub queue_id: String,
    pub repo_id: String,
    pub target_branch: String,
    pub state: IntegrationQueueState,
    pub head_attempt_id: Option<String>,
    /// A driver of this process holds the head right now.
    pub driven_here: bool,
    pub lease_until: Option<String>,
    pub last_error_kind: Option<IntegrationFailureKind>,
    pub last_error: Option<String>,
    pub members: Vec<IntegrationMemberSnapshot>,
}
/// Provided by the worker: the read the public queue surface is built on.
#[async_trait]
pub trait IntegrationSnapshotPort: Send + Sync {
    async fn integration_queue_snapshot(
        &self,
        queue_id: &str,
        member_limit: u32,
    ) -> Result<Option<IntegrationQueueSnapshot>>;
}

#[async_trait]
pub trait WorkerClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
    async fn sleep(&self, duration: Duration);
}
pub struct SystemClock;
#[async_trait]
impl WorkerClock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}
