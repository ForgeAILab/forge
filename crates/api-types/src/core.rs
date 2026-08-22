use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;

pub type TaskStatus = String;
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum TaskType {
    Task,
    PlanningTask,
    SubTask,
    Discovery,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum WorkMode {
    #[default]
    DirectMerge,
    PullRequest,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum AgentStatus {
    Idle,
    Busy,
    Error,
    Offline,
}

pub type ExecutionRole = String;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ExecutionStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ResumePolicy {
    Auto,
    Manual,
    None,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum RecoveryAction {
    ResumeSession,
    Reexecute,
    ResetToInitial,
    CancelTask,
    MarkReviewed,
    RetryHook,
    ResumeProcess,
    UpdateWorkspaceAndRetryHook,
    SkipHookOnce,
    ResetRetryWindow,
    ProceedOnce,
    OpenInteractive,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ExecutionBehaviorKind {
    ManualLaunch,
    SessionFollowUp,
    WorkflowResume,
    ReExecute,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub struct ExecutionBehavior {
    pub kind: ExecutionBehaviorKind,
    pub propagates: bool,
    pub cascade_role: Option<String>,
    pub cascade_state: Option<String>,
    pub description: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ExecutionActionKind {
    ManualLaunch,
    SessionFollowUp,
    WorkflowResume,
    ReExecute,
    StopExecution,
    CancelTask,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct ExecutionAction {
    pub action: ExecutionActionKind,
    pub label: String,
    pub enabled: bool,
    pub propagates: bool,
    pub requires_session: bool,
    pub disabled_reason: Option<String>,
    pub target_execution_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub struct BlockingArtifact {
    pub kind: String,
    pub id: Option<String>,
    pub log_path: Option<String>,
}

/// Closed vocabulary for task interruption kinds. This is the only
/// classification signal for blocked/failed metadata and blocking
/// annotations; human-readable reason text carries no classification weight.
///
/// `Unknown` is a deserialize-only fallback for legacy database rows and MUST
/// NOT be constructed by producers; it classifies as info-only (no recovery
/// actions offered).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum FailureKind {
    MergeConflict,
    TargetRepoDirty,
    DirtyWorktree,
    CiFailed,
    ReviewGateFailed,
    ReviewBudgetExhausted,
    RetryExhausted,
    MergeFixBudgetExhausted,
    WorkflowGuardRejected,
    InternalCommandFailed,
    PrClosedWithoutMerge,
    ExecutorFailed,
    WorkspaceFailed,
    WorkspaceResetRequired,
    WorkspaceError,
    BeforeWorkHookTimeout,
    BeforeWorkHookFailed,
    MaxTurnsExceeded,
    ManualStop,
    RecoveryRequired,
    /// No executor candidate could run the task (usage exhausted, missing
    /// CLI, or failed authentication across the whole fallback route).
    /// Bypasses the execution retry budget.
    ExecutorUnavailable,
    #[serde(other)]
    Unknown,
}

impl FailureKind {
    /// Retry-budget exhaustion as recorded in blocked metadata
    /// (`InterruptionMetadata.kind`).
    pub fn is_retry_exhausted_metadata(self) -> bool {
        matches!(
            self,
            Self::ReviewGateFailed | Self::RetryExhausted | Self::MergeFixBudgetExhausted
        )
    }

    /// Retry-budget exhaustion as recorded on blocking annotations
    /// (`TaskBlockingAnnotation.annotation_type`).
    pub fn is_budget_exhausted_annotation(self) -> bool {
        matches!(
            self,
            Self::ReviewBudgetExhausted | Self::RetryExhausted | Self::MergeFixBudgetExhausted
        )
    }

    /// Merge interruptions recoverable by retrying the merge or merge fix.
    pub fn is_merge_recoverable(self) -> bool {
        matches!(
            self,
            Self::MergeConflict | Self::TargetRepoDirty | Self::DirtyWorktree
        )
    }

    /// Workspace-level failures that require a workspace reset or repair.
    pub fn is_workspace_failure(self) -> bool {
        matches!(
            self,
            Self::WorkspaceFailed | Self::WorkspaceResetRequired | Self::WorkspaceError
        )
    }
}

impl std::fmt::Display for FailureKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = serde_json::to_value(self).map_err(|_| std::fmt::Error)?;
        f.write_str(value.as_str().ok_or(std::fmt::Error)?)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub struct TaskBlockingAnnotation {
    #[serde(rename = "type")]
    pub annotation_type: FailureKind,
    #[serde(default)]
    pub blocking_reason: String,
    pub blocked_by: Option<String>,
    pub blocked_at: Option<String>,
    pub blocked_execution_id: Option<String>,
    pub artifact: Option<BlockingArtifact>,
    pub message: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(type = "Record<string, unknown> | null")]
    #[ts(optional)]
    pub hook: Option<serde_json::Value>,
    #[serde(default)]
    pub recovery_actions: Vec<RecoveryAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub struct WorkflowHealthSummary {
    pub kind: WorkflowHealthKind,
    pub label: String,
    pub severity: HealthSeverity,
    pub message: Option<String>,
    pub state: Option<String>,
    pub role: Option<String>,
    pub execution_id: Option<String>,
    pub review_id: Option<String>,
    pub since: Option<String>,
    pub stale_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum WorkflowHealthKind {
    Idle,
    WaitingForAgent,
    Running,
    AwaitingHuman,
    Blocked,
    Failed,
    Stuck,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum HealthSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub struct WorkflowExceptionSummary {
    #[serde(rename = "type")]
    pub exception_type: String,
    pub message: String,
    pub review_id: Option<String>,
    pub execution_id: Option<String>,
    pub state: Option<String>,
    pub role: Option<String>,
    pub target_state: Option<String>,
    pub target_role: Option<String>,
    pub failing_step: Option<FailingStepSummary>,
    #[serde(default)]
    pub related_evidence: Vec<RelatedEvidence>,
    #[serde(default)]
    pub actions: Vec<WorkflowExceptionAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub struct FailingStepSummary {
    pub index: usize,
    pub command: Option<String>,
    pub exit_code: Option<i32>,
    pub output_tail: Option<String>,
    pub stderr_tail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub struct RelatedEvidence {
    pub kind: String,
    pub id: Option<String>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub struct WorkflowExceptionAction {
    pub kind: RecoveryAction,
    pub label: String,
    pub enabled: bool,
    pub disabled_reason: Option<String>,
    pub requires_reason: bool,
    pub requires_guidance: bool,
    pub propagates: bool,
    pub target_state: Option<String>,
    pub target_role: Option<String>,
    pub target_execution_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct InterruptionMetadata {
    pub reason: String,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional = nullable)]
    pub kind: Option<FailureKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional = nullable)]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional = nullable)]
    pub execution_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(type = "Record<string, unknown> | null")]
    #[ts(optional)]
    pub details: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[allow(clippy::large_enum_variant)]
#[serde(untagged)]
#[ts(export)]
pub enum TaskAnnotation {
    Blocking(TaskBlockingAnnotation),
    #[ts(type = "unknown")]
    Legacy(Value),
}

impl std::fmt::Display for TaskAnnotation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match serde_json::to_string(self) {
            Ok(s) => f.write_str(&s),
            Err(_) => write!(f, "{:?}", self),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReviewStatus {
    Running,
    AwaitingHuman,
    Passed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum AuthorType {
    User,
    Agent,
    System,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HumanDecision {
    Approved,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub struct ReviewConfig {
    #[serde(default)]
    pub ci_steps: Vec<String>,
    #[serde(default)]
    pub review_prompt: Option<String>,
}

#[cfg(test)]
mod failure_kind_tests {
    use super::FailureKind;

    #[test]
    fn round_trips_known_values() {
        for (kind, wire) in [
            (FailureKind::MergeConflict, "\"merge_conflict\""),
            (FailureKind::TargetRepoDirty, "\"target_repo_dirty\""),
            (FailureKind::CiFailed, "\"ci_failed\""),
            (FailureKind::ReviewGateFailed, "\"review_gate_failed\""),
            (FailureKind::RetryExhausted, "\"retry_exhausted\""),
            (
                FailureKind::MergeFixBudgetExhausted,
                "\"merge_fix_budget_exhausted\"",
            ),
            (FailureKind::RecoveryRequired, "\"recovery_required\""),
            (FailureKind::ManualStop, "\"manual_stop\""),
        ] {
            assert_eq!(serde_json::to_string(&kind).unwrap(), wire);
            assert_eq!(serde_json::from_str::<FailureKind>(wire).unwrap(), kind);
            assert_eq!(format!("\"{kind}\""), wire);
        }
    }

    #[test]
    fn unknown_strings_deserialize_to_unknown() {
        let kind: FailureKind = serde_json::from_str("\"some_future_kind\"").unwrap();
        assert_eq!(kind, FailureKind::Unknown);
    }

    #[test]
    fn classification_predicates() {
        assert!(FailureKind::ReviewGateFailed.is_retry_exhausted_metadata());
        assert!(FailureKind::RetryExhausted.is_retry_exhausted_metadata());
        assert!(FailureKind::MergeFixBudgetExhausted.is_retry_exhausted_metadata());
        assert!(!FailureKind::CiFailed.is_retry_exhausted_metadata());
        assert!(!FailureKind::Unknown.is_retry_exhausted_metadata());

        assert!(FailureKind::ReviewBudgetExhausted.is_budget_exhausted_annotation());
        assert!(!FailureKind::ReviewGateFailed.is_budget_exhausted_annotation());

        assert!(FailureKind::MergeConflict.is_merge_recoverable());
        assert!(FailureKind::TargetRepoDirty.is_merge_recoverable());
        assert!(FailureKind::DirtyWorktree.is_merge_recoverable());
        assert!(!FailureKind::Unknown.is_merge_recoverable());

        assert!(FailureKind::WorkspaceFailed.is_workspace_failure());
        assert!(!FailureKind::ExecutorFailed.is_workspace_failure());
    }
}
