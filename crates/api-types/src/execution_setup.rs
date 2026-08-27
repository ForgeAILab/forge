use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::{ExecutionBlockerProjection, RepoResponse, RetryAction, SetupRequirement};

/// Whether the singular Project Agent chat can admit a turn. This is kept
/// independent from repository and Task-role setup.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum CoordinationState {
    SetupRequired,
    Ready,
    Unavailable,
}

/// Whether a Project has the repository and the required execution principals
/// needed for repository-backed work.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ExecutionSetupState {
    SetupRequired,
    Provisioning,
    Ready,
    Failed,
    /// The authoritative provisioning/eligibility source could not be read.
    /// Callers must inspect `ProjectExecutionSetupResponse::availability` and
    /// retry rather than treating this as setup success or failure.
    Unavailable,
}

/// Legacy baseline/reconciliation projection retained for traceability UI.
/// Charter-backed Task execution reports `Active`; baseline approval is not
/// an execution gate.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ExecutionGate {
    PreBaselineReadOnly,
    BaselineApprovalRequired,
    Active,
    ReconciliationRequired,
    /// The authoritative baseline/reconciliation source could not be read.
    Unavailable,
}

/// Freshness of one readiness dimension. `stale` and `unavailable` are
/// explicit because a normal state enum alone cannot distinguish a verified
/// `setup_required` result from a failed projection read.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ProjectionAvailability {
    Current,
    Stale,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ProjectionStatus {
    pub availability: ProjectionAvailability,
    pub retry: Option<RetryAction>,
    pub error_code: Option<String>,
}

impl ProjectionStatus {
    #[must_use]
    pub const fn current() -> Self {
        Self {
            availability: ProjectionAvailability::Current,
            retry: None,
            error_code: None,
        }
    }

    #[must_use]
    pub fn unavailable() -> Self {
        Self {
            availability: ProjectionAvailability::Unavailable,
            retry: Some(RetryAction::RefreshAndRetry),
            error_code: Some("projection_source_unavailable".to_owned()),
        }
    }

    #[must_use]
    pub fn stale() -> Self {
        Self {
            availability: ProjectionAvailability::Stale,
            retry: Some(RetryAction::RefreshAndRetry),
            error_code: Some("projection_stale".to_owned()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ProjectExecutionSetupAvailability {
    pub coordination: ProjectionStatus,
    pub execution_setup: ProjectionStatus,
    pub execution_gate: ProjectionStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ExecutionPrincipalResponse {
    pub identity_id: String,
    pub name: String,
    pub profile_id: String,
    pub executor_type: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub status: String,
    pub paused: bool,
    pub version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ProvisioningOperationResponse {
    pub id: String,
    pub status: String,
    pub current_checkpoint: String,
    pub attempt_count: i64,
    pub max_attempts: i64,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<String>,
    pub next_retry_at: Option<String>,
    pub retryable: bool,
    pub last_error_code: Option<String>,
    pub last_error_message: Option<String>,
    pub version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ProjectExecutionSetupResponse {
    pub project_id: String,
    pub project_version: i64,
    pub coordination_state: CoordinationState,
    pub execution_setup_state: ExecutionSetupState,
    pub execution_gate: ExecutionGate,
    pub availability: ProjectExecutionSetupAvailability,
    pub primary_repo: Option<RepoResponse>,
    pub worker: Option<ExecutionPrincipalResponse>,
    pub independent_reviewer: Option<ExecutionPrincipalResponse>,
    pub eligible_workers: Vec<ExecutionPrincipalResponse>,
    pub eligible_reviewers: Vec<ExecutionPrincipalResponse>,
    pub setup_requirements: Vec<SetupRequirement>,
    pub next_action: Option<RetryAction>,
    pub provisioning: Option<ProvisioningOperationResponse>,
    /// The one canonical Project-wide execution blocker (D17), or `None`
    /// when the Project has no outstanding blocker. Consumers render this
    /// projection's copy instead of deriving their own from the raw
    /// `coordination_state`/`execution_setup_state`/`execution_gate` enums.
    pub execution_blocker: Option<ExecutionBlockerProjection>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct SelectExecutionPrincipalRequest {
    pub identity_id: String,
    pub expected_project_version: i64,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct AttachPrimaryRepositoryRequest {
    pub repo_id: String,
    pub expected_project_version: i64,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct RetryProvisioningRequest {
    pub expected_operation_version: i64,
    pub idempotency_key: String,
}
