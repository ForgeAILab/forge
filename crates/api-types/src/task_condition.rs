use serde::{Deserialize, Serialize};
use ts_rs::TS;

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
    pub interruption: Option<crate::InterruptionMetadata>,
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
