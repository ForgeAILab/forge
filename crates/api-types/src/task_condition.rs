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

/// Integration owns these statements until a Task step consumes or replaces
/// them. Repair/review lineage survives actual coder/reviewer execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[ts(tag = "kind", rename_all = "snake_case")]
#[ts(export)]
pub enum IntegrationReason {
    Waiting {
        attempt_id: IntegrationAttemptId,
        blocked_by: Vec<IntegrationAttemptId>,
    },
    Owned {
        attempt_id: IntegrationAttemptId,
        phase: IntegrationPhase,
    },
    Repair {
        attempt_id: IntegrationAttemptId,
        conflict_paths: Option<Vec<String>>,
        repair_paths: Vec<String>,
        predecessor_attempt_id: Option<IntegrationAttemptId>,
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
        retry_at: Option<String>,
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
    Dependencies {
        cancelled: bool,
    },
    Children {
        root_id: String,
        remaining: Vec<String>,
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
