use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// Opaque identity; queue order and worker leases belong to stage-B storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(transparent)]
#[ts(export)]
pub struct IntegrationAttemptId(String);
impl IntegrationAttemptId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum IntegrationPhase {
    Validating,
    Rebasing,
    Checking,
    AwaitingCarry,
    AwaitingAuthorization,
    FastForwarding,
    Reconciling,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum IntegrationDeferralCause {
    Infrastructure,
    OwnerOffline,
    TargetDirty,
    BudgetExhausted,
    OwnerRequired,
    UnresolvedResult,
}
impl IntegrationDeferralCause {
    pub fn requires_intervention(&self) -> bool {
        matches!(
            self,
            Self::TargetDirty | Self::BudgetExhausted | Self::OwnerRequired
        )
    }
}

/// A bounded sample of a path set: how many paths there are and at most
/// [`INTEGRATION_PATH_SAMPLE`] of them. The whole set is the attempt's record.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct IntegrationPaths {
    /// Paths in the whole set.
    pub count: u32,
    /// The first paths of the set, at most [`INTEGRATION_PATH_SAMPLE`].
    pub paths: Vec<String>,
    /// Whether `paths` leaves some of the set out.
    pub truncated: bool,
}
/// Paths one integration reason carries per path set.
pub const INTEGRATION_PATH_SAMPLE: usize = 32;
impl IntegrationPaths {
    /// The bounded sample of `all`, in the order given.
    pub fn bounded(all: impl IntoIterator<Item = String>) -> Self {
        let mut paths = Vec::new();
        let mut count = 0_u32;
        for path in all {
            count = count.saturating_add(1);
            if paths.len() < INTEGRATION_PATH_SAMPLE {
                paths.push(path);
            }
        }
        Self {
            count,
            truncated: count as usize > paths.len(),
            paths,
        }
    }
}

/// Integration owns these statements until a Task step consumes or replaces
/// them. Repair/review lineage survives actual coder/reviewer execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[ts(tag = "kind", rename_all = "snake_case")]
#[ts(export)]
pub enum IntegrationReason {
    /// Queued behind other attempts. Queue order is the queue's own state and
    /// is never copied here: a queue advance rewrites no waiter.
    Waiting {
        attempt_id: IntegrationAttemptId,
    },
    Owned {
        attempt_id: IntegrationAttemptId,
        phase: IntegrationPhase,
    },
    /// Lineage is bounded: the full path sets belong to the attempt record.
    /// `conflict_paths` is `None` when the conflicting paths are not known.
    Repair {
        attempt_id: IntegrationAttemptId,
        predecessor_attempt_id: Option<IntegrationAttemptId>,
        conflict_paths: Option<IntegrationPaths>,
        repair_paths: IntegrationPaths,
    },
    ReviewRequired {
        attempt_id: IntegrationAttemptId,
        authority_reason: String,
    },
    CandidateCheckFailed {
        attempt_id: IntegrationAttemptId,
        check: String,
        message: String,
    },
    Deferred {
        attempt_id: IntegrationAttemptId,
        cause: IntegrationDeferralCause,
        owner_id: Option<String>,
        message: String,
    },
    Applied {
        attempt_id: IntegrationAttemptId,
    },
}
impl IntegrationReason {
    pub fn attempt_id(&self) -> &IntegrationAttemptId {
        match self {
            Self::Waiting { attempt_id, .. }
            | Self::Owned { attempt_id, .. }
            | Self::Repair { attempt_id, .. }
            | Self::ReviewRequired { attempt_id, .. }
            | Self::CandidateCheckFailed { attempt_id, .. }
            | Self::Deferred { attempt_id, .. }
            | Self::Applied { attempt_id } => attempt_id,
        }
    }
    pub fn requires_intervention(&self) -> bool {
        matches!(self, Self::Deferred { cause, .. } if cause.requires_intervention())
    }
    pub fn hands_off(&self) -> bool {
        matches!(
            self,
            Self::Repair { .. } | Self::ReviewRequired { .. } | Self::CandidateCheckFailed { .. }
        )
    }
}

/// Where a Task's requested check stands. A slot wait and a result wait are
/// owned work that ends on its own; exhausted infrastructure retries are not a
/// verdict on the candidate and wait for `retry` or `cancel`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum CheckWaitPhase {
    /// The check is admitted (or about to be) and no result exists yet.
    Result,
    /// The checkout's machine has no free run slot for the check.
    Slot,
    /// The check produced no verdict after its automatic retries.
    InfrastructureExhausted,
}
/// The check a Task waits on, named by the consumer that asked for it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct CheckWait {
    pub phase: CheckWaitPhase,
    /// Opaque identity of the Task's request for this check.
    pub consumer_id: String,
    /// The family that asked: `entry`, `integration`, ...
    pub origin: String,
}
impl CheckWait {
    pub fn requires_intervention(&self) -> bool {
        self.phase == CheckWaitPhase::InfrastructureExhausted
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[ts(tag = "kind", rename_all = "snake_case")]
#[ts(export)]
pub enum TaskCondition {
    Clear {
        details: ConditionDetails,
    },
    Entering {
        state: String,
        epoch: i64,
        step_id: String,
        phase: String,
        since: String,
        details: ConditionDetails,
    },
    Running {
        execution_id: String,
        role: String,
        epoch: i64,
        since: String,
        details: ConditionDetails,
    },
    Deferred {
        until: Option<String>,
        reason: RetryCause,
        resume: ConditionContinuation,
        details: ConditionDetails,
    },
    Parked {
        primary: ConditionReason,
        additional: Vec<ConditionReason>,
        resume: ConditionContinuation,
        since: Option<String>,
        details: ConditionDetails,
    },
    Failed {
        failure: ConditionReason,
        additional: Vec<ConditionReason>,
        resume: ConditionContinuation,
        since: Option<String>,
        details: ConditionDetails,
    },
    Settled {
        outcome: TerminalOutcome,
        details: ConditionDetails,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum HumanBoundary {
    PlanReview,
    ExternalMerge,
    Legacy,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ConditionCapacityScope {
    Agent,
    Machine,
    /// Every usable machine's workspace filesystem is under its free-space
    /// floor. Clears by itself when a reading recovers.
    Disk,
    Project,
    OwnerBackpressure,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ConditionEnvironmentKind {
    EnvironmentProbePending,
    EnvironmentNotReady,
    EnvironmentUnverified,
    ProvisionFailed,
    Unknown,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[ts(tag = "kind", rename_all = "snake_case")]
#[ts(export)]
pub enum ConditionReason {
    Integration {
        reason: IntegrationReason,
    },
    Held {
        actor: String,
    },
    Failure {
        failure_kind: crate::FailureKind,
    },
    /// Legacy `agent_timeout`: a blocking annotation kind that is not an
    /// `crate::FailureKind`.
    AgentTimeout {},
    BudgetExhausted {
        budget_kind: Option<String>,
    },
    EntryBlocked {
        state: Option<String>,
    },
    HumanDecision {
        boundary: HumanBoundary,
    },
    Capacity {
        scope: ConditionCapacityScope,
    },
    DispatchRefusal {
        capability: Option<String>,
        blocker_digest: Option<String>,
    },
    ProjectPaused {
        state: Option<String>,
    },
    OwnerOffline {
        daemon_id: Option<String>,
        started_at: Option<String>,
    },
    Environment {
        wait_kind: ConditionEnvironmentKind,
    },
    PlacementDenied {},
    DaemonUpgradeRequired {},
    RemoteCancelPending {},
    /// `dependency_ids` names the Tasks waited for (at most 16).
    Dependencies {
        cancelled: bool,
        #[serde(default)]
        dependency_ids: Vec<String>,
    },
    Children {
        root_id: String,
        remaining: Vec<String>,
    },
    /// A subtask waits for its parent Task. `cause` is `held`, `blocked` or
    /// `not_coordinating`; the exit is on the parent.
    Parent {
        parent_id: String,
        cause: String,
    },
    /// The Agent that would run the Task cannot take work; `status` is its
    /// effective status (`paused`, `daemon_offline`, ...).
    Agent {
        agent_id: String,
        status: String,
    },
    PlanSettlementWait {
        execution_id: Option<String>,
    },
    UnknownCondition {
        problem: UnknownConditionProblem,
    },
    WorkflowInvalid {
        state: String,
        definition_digest: String,
        cause: String,
    },
    // --- durable check runner (plan 3.3 stage D) ---
    /// The Task waits on a durable check run: for its result, for a slot on
    /// the checkout's machine, or parked after the automatic infrastructure
    /// retries were used up.
    Check {
        wait: CheckWait,
    },
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum RetryCause {
    ExecutionFailure,
    WorkflowGuard,
    ReviewCiInfrastructure,
    PlanTransport,
    Environment,
    ChildrenReady,
    Legacy,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[ts(tag = "kind", rename_all = "snake_case")]
#[ts(export)]
pub enum ConditionContinuation {
    Integration { attempt_id: IntegrationAttemptId },
    Reconcile,
    AdvanceAggregateReview { child_ids: Vec<String> },
    Dispatch { target_state: String },
    RetryEntry { state: Option<String> },
    Integrate { state: Option<String> },
    SettlePlan { execution_id: Option<String> },
    ResumeQueuedCommand { intent_id: Option<String> },
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum TerminalOutcome {
    Completed,
    Cancelled,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ConditionDetails {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub execution_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub owner: Option<ConditionOwner>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub recovery: Option<ConditionRecovery>,
    pub failure_kind: Option<crate::FailureKind>,
    pub diagnostic: Option<crate::TaskBlockingAnnotation>,
    /// The Task's failure record when one is stored, otherwise its blocked
    /// record. `failed` and `blocked` say which records are stored.
    pub interruption: Option<crate::InterruptionMetadata>,
    /// A failure record is stored; `interruption` is that record.
    pub failed: bool,
    /// A blocked record is stored; it is `interruption` unless `failed`.
    pub blocked: bool,
    pub human_wait: bool,
    pub entry_wait: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum UnknownConditionProblem {
    MalformedJson,
    NonObject,
    UnknownKind,
    InvalidShape,
    UnownedEntry,
    /// A BLOB or invalid UTF-8 value in a legacy TEXT column.
    NonText,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ConditionOwner {
    IntegrationWorker,
    User,
    ProjectAgent,
    Worker,
    Workflow,
    Machine,
    Scheduler,
    // --- durable check runner (plan 3.3 stage D) ---
    CheckRunner,
}

/// Recovery guidance for an owner park, separate from authorized Task offers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ConditionRecovery {
    WaitForIntegration,
    RepairIntegration,
    EditWorkflow,
    ReconcileEntry,
    // --- durable check runner (plan 3.3 stage D) ---
    WaitForCheck,
    RetryCheck,
}

impl TaskCondition {
    pub fn details(&self) -> &ConditionDetails {
        match self {
            Self::Clear { details }
            | Self::Entering { details, .. }
            | Self::Running { details, .. }
            | Self::Deferred { details, .. }
            | Self::Parked { details, .. }
            | Self::Failed { details, .. }
            | Self::Settled { details, .. } => details,
        }
    }
}
