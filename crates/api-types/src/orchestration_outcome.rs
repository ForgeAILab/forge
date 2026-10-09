use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;

/// Stable machine-readable categories for native and MCP orchestration
/// outcomes.  The model-facing adapters must branch on this value instead of
/// parsing `safe_message`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum OutcomeCode {
    TaskBusy,
    Ok,
    ApprovalRequired,
    SetupRequired,
    ActiveSessionConflict,
    VersionConflict,
    DigestConflict,
    IdempotencyConflict,
    PolicyDenied,
    NotFound,
    TransientFailure,
    InternalFailure,
    ValidationError,
    ActionUnavailable,
}

impl OutcomeCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TaskBusy => "task_busy",
            Self::Ok => "ok",
            Self::ApprovalRequired => "approval_required",
            Self::SetupRequired => "setup_required",
            Self::ActiveSessionConflict => "active_session_conflict",
            Self::VersionConflict => "version_conflict",
            Self::DigestConflict => "digest_conflict",
            Self::IdempotencyConflict => "idempotency_conflict",
            Self::PolicyDenied => "policy_denied",
            Self::NotFound => "not_found",
            Self::TransientFailure => "transient_failure",
            Self::InternalFailure => "internal_failure",
            Self::ValidationError => "validation_error",
            Self::ActionUnavailable => "action_unavailable",
        }
    }
}

/// Stable lifecycle state for an orchestration result.  Replay is deliberately
/// represented by [`OrchestrationOutcome::replayed`], never by another status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum OutcomeStatus {
    Succeeded,
    ApprovalRequired,
    SetupRequired,
    Failed,
}

impl OutcomeStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::ApprovalRequired => "approval_required",
            Self::SetupRequired => "setup_required",
            Self::Failed => "failed",
        }
    }
}

/// The scope identity bound to a command receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum OutcomeScopeType {
    Account,
    Project,
    AgentChat,
    Task,
}

impl OutcomeScopeType {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::Project => "project",
            Self::AgentChat => "agent_chat",
            Self::Task => "task",
        }
    }
}

/// Canonical scope reference included in every outcome, including failures.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct CanonicalScopeRef {
    pub scope_type: OutcomeScopeType,
    pub scope_id: String,
}

impl CanonicalScopeRef {
    #[must_use]
    pub fn new(scope_type: OutcomeScopeType, scope_id: impl Into<String>) -> Self {
        Self {
            scope_type,
            scope_id: scope_id.into(),
        }
    }
}

/// Typed identity and concurrency information for an approval proposal.
/// Command-specific immutable payloads belong in `result`, not in an
/// unbounded field on this envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ApprovalTarget {
    pub target_type: String,
    pub target_id: String,
    pub operation: Option<String>,
    pub version: Option<i64>,
    pub revision_id: Option<String>,
    pub revision: Option<i64>,
    pub content_digest: Option<String>,
    pub rendered_digest: Option<String>,
    pub requires_user_authorization: bool,
}

impl ApprovalTarget {
    #[must_use]
    pub fn new(target_type: impl Into<String>, target_id: impl Into<String>) -> Self {
        Self {
            target_type: target_type.into(),
            target_id: target_id.into(),
            operation: None,
            version: None,
            revision_id: None,
            revision: None,
            content_digest: None,
            rendered_digest: None,
            requires_user_authorization: true,
        }
    }
}

/// A bounded, typed setup blocker.  `action` tells the caller which safe
/// remediation may be attempted; it does not authorize that remediation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct SetupRequirement {
    pub requirement_type: String,
    pub resource_type: Option<String>,
    pub resource_id: Option<String>,
    pub role: Option<String>,
    pub capability: Option<String>,
    pub action: Option<RetryAction>,
}

impl SetupRequirement {
    #[must_use]
    pub fn new(requirement_type: impl Into<String>) -> Self {
        Self {
            requirement_type: requirement_type.into(),
            resource_type: None,
            resource_id: None,
            role: None,
            capability: None,
            action: None,
        }
    }
}

/// Authorized current state returned for a version or digest correction.
/// Fields other than the resource identity are optional because not every
/// command has both a mutable version and an immutable revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct CurrentVersionOrRevision {
    pub resource_type: String,
    pub resource_id: String,
    pub version: Option<i64>,
    pub revision_id: Option<String>,
    pub revision: Option<i64>,
    pub content_digest: Option<String>,
    pub rendered_digest: Option<String>,
}

impl CurrentVersionOrRevision {
    #[must_use]
    pub fn new(resource_type: impl Into<String>, resource_id: impl Into<String>) -> Self {
        Self {
            resource_type: resource_type.into(),
            resource_id: resource_id.into(),
            version: None,
            revision_id: None,
            revision: None,
            content_digest: None,
            rendered_digest: None,
        }
    }
}

/// A typed next action.  Arbitrary command-specific parameters belong in the
/// separately named `arguments` map and are only populated by a command that
/// has validated those parameters for the caller's scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum RetryAction {
    None,
    RefreshAndRetry,
    UseNewIdempotencyKey,
    Repropose,
    Reauthorize,
    CompleteSetup,
    RetryAfter,
    CorrectInput,
    SelectWorker,
    SelectIndependentReviewer,
    AttachRepository,
    RetryProvisioning,
    /// Review and resolve a recorded canonical conflict through the
    /// reconciliation surface. This is the one permitted next action for an
    /// `ExecutionBlockerProjection` coded `reconciliation_required` or
    /// `invalid_active_baseline` (D15/D17); it never doubles as a mere
    /// refresh once a genuine conflict is recorded.
    ResolveReconciliation,
}

impl RetryAction {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::RefreshAndRetry => "refresh_and_retry",
            Self::UseNewIdempotencyKey => "use_new_idempotency_key",
            Self::Repropose => "repropose",
            Self::Reauthorize => "reauthorize",
            Self::CompleteSetup => "complete_setup",
            Self::RetryAfter => "retry_after",
            Self::CorrectInput => "correct_input",
            Self::SelectWorker => "select_worker",
            Self::SelectIndependentReviewer => "select_independent_reviewer",
            Self::AttachRepository => "attach_repository",
            Self::RetryProvisioning => "retry_provisioning",
            Self::ResolveReconciliation => "resolve_reconciliation",
        }
    }
}

/// Lifetime of a terminal native denial. Session denials are also final for
/// the current turn; the session lifetime controls state-card reminders.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum RetryScope {
    Turn,
    Session,
}

/// Safe, server-owned cause of a native policy refusal. Only capability-wide
/// causes withdraw an operation; request-specific refusals remain uncached.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(try_from = "String", into = "String")]
#[ts(
    export,
    type = "\"authority_revoked\" | `permission_missing(${string})` | `project_paused(${string})` | \"target_agent_paused\" | \"identity_paused\" | \"charter_not_adopted\" | \"operation_not_in_scope\" | \"profile_not_selected\" | \"task_terminal\" | \"reviewer_read_only\" | \"independent_approval_required\" | \"user_request_required\" | \"leased_turn_required\" | \"charter_adoption_not_applicable\" | \"read_boundary_required\" | \"direct_command_not_admitted\" | \"review_assignment_required\" | \"placement_unavailable\" | \"daemon_upgrade_required\" | \"workspace_reset_required\" | \"unspecified\""
)]
pub enum DeniedBy {
    AuthorityRevoked,
    PermissionMissing(String),
    IdentityPaused,
    TargetAgentPaused,
    CharterNotAdopted,
    ProjectPaused(String),
    OperationNotInScope,
    ProfileNotSelected,
    TaskTerminal,
    ReviewerReadOnly,
    IndependentApprovalRequired,
    UserRequestRequired,
    LeasedTurnRequired,
    CharterAdoptionNotApplicable,
    ReadBoundaryRequired,
    DirectCommandNotAdmitted,
    ReviewAssignmentRequired,
    PlacementUnavailable,
    DaemonUpgradeRequired,
    WorkspaceResetRequired,
    Unspecified,
}

impl DeniedBy {
    /// Session reminders describe capability-wide refusals, including pauses
    /// that remain effective only while their live cause holds.
    #[must_use]
    pub const fn scope(&self) -> RetryScope {
        match self {
            Self::AuthorityRevoked
            | Self::PermissionMissing(_)
            | Self::IdentityPaused
            | Self::CharterNotAdopted
            | Self::ProjectPaused(_)
            | Self::OperationNotInScope => RetryScope::Session,
            _ => RetryScope::Turn,
        }
    }

    /// Whether a session reminder must be invalidated from current authority.
    #[must_use]
    pub const fn clears(&self) -> bool {
        matches!(
            self,
            Self::AuthorityRevoked
                | Self::PermissionMissing(_)
                | Self::IdentityPaused
                | Self::CharterNotAdopted
                | Self::ProjectPaused(_)
        )
    }

    /// Explicit allowlist for operation-wide cache and record admission.
    #[must_use]
    pub const fn withdraws_operation(&self) -> bool {
        matches!(
            self,
            Self::AuthorityRevoked
                | Self::PermissionMissing(_)
                | Self::IdentityPaused
                | Self::CharterNotAdopted
                | Self::ProjectPaused(_)
                | Self::OperationNotInScope
        )
    }
}

impl std::fmt::Display for DeniedBy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::PermissionMissing(permission) => {
                return write!(f, "permission_missing({permission})")
            }
            Self::ProjectPaused(detail) => return write!(f, "project_paused({detail})"),
            Self::AuthorityRevoked => "authority_revoked",
            Self::IdentityPaused => "identity_paused",
            Self::TargetAgentPaused => "target_agent_paused",
            Self::CharterNotAdopted => "charter_not_adopted",
            Self::OperationNotInScope => "operation_not_in_scope",
            Self::ProfileNotSelected => "profile_not_selected",
            Self::TaskTerminal => "task_terminal",
            Self::ReviewerReadOnly => "reviewer_read_only",
            Self::IndependentApprovalRequired => "independent_approval_required",
            Self::UserRequestRequired => "user_request_required",
            Self::LeasedTurnRequired => "leased_turn_required",
            Self::CharterAdoptionNotApplicable => "charter_adoption_not_applicable",
            Self::ReadBoundaryRequired => "read_boundary_required",
            Self::DirectCommandNotAdmitted => "direct_command_not_admitted",
            Self::ReviewAssignmentRequired => "review_assignment_required",
            Self::PlacementUnavailable => "placement_unavailable",
            Self::DaemonUpgradeRequired => "daemon_upgrade_required",
            Self::WorkspaceResetRequired => "workspace_reset_required",
            Self::Unspecified => "unspecified",
        };
        f.write_str(name)
    }
}

impl std::str::FromStr for DeniedBy {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if let Some(permission) = value
            .strip_prefix("permission_missing(")
            .and_then(|s| s.strip_suffix(')'))
        {
            if !permission.is_empty()
                && permission
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b == b'_')
            {
                return Ok(Self::PermissionMissing(permission.to_owned()));
            }
            return Err("invalid permission name");
        }
        if let Some(detail) = value
            .strip_prefix("project_paused(")
            .and_then(|s| s.strip_suffix(')'))
        {
            return Ok(Self::ProjectPaused(detail.to_owned()));
        }
        Ok(match value {
            "authority_revoked" => Self::AuthorityRevoked,
            "identity_paused" => Self::IdentityPaused,
            "target_agent_paused" => Self::TargetAgentPaused,
            "charter_not_adopted" => Self::CharterNotAdopted,
            "operation_not_in_scope" => Self::OperationNotInScope,
            "profile_not_selected" => Self::ProfileNotSelected,
            "task_terminal" => Self::TaskTerminal,
            "reviewer_read_only" => Self::ReviewerReadOnly,
            "independent_approval_required" => Self::IndependentApprovalRequired,
            "user_request_required" => Self::UserRequestRequired,
            "leased_turn_required" => Self::LeasedTurnRequired,
            "charter_adoption_not_applicable" => Self::CharterAdoptionNotApplicable,
            "read_boundary_required" => Self::ReadBoundaryRequired,
            "direct_command_not_admitted" => Self::DirectCommandNotAdmitted,
            "review_assignment_required" => Self::ReviewAssignmentRequired,
            "placement_unavailable" => Self::PlacementUnavailable,
            "daemon_upgrade_required" => Self::DaemonUpgradeRequired,
            "workspace_reset_required" => Self::WorkspaceResetRequired,
            "unspecified" => Self::Unspecified,
            _ => return Err("unknown denial cause"),
        })
    }
}

impl TryFrom<String> for DeniedBy {
    type Error = &'static str;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<DeniedBy> for String {
    fn from(value: DeniedBy) -> Self {
        value.to_string()
    }
}

/// Bounded corrective information for an outcome.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct RetryInstruction {
    pub action: RetryAction,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<RetryScope>,
    pub after_seconds: Option<u64>,
    #[ts(type = "Record<string, unknown>")]
    pub arguments: BTreeMap<String, Value>,
}

impl RetryInstruction {
    #[must_use]
    pub fn new(action: RetryAction, retryable: bool) -> Self {
        Self {
            action,
            retryable,
            scope: None,
            after_seconds: None,
            arguments: BTreeMap::new(),
        }
    }
}

/// Shared native/MCP model-facing orchestration result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct OrchestrationOutcome {
    pub code: OutcomeCode,
    pub status: OutcomeStatus,
    pub operation: String,
    pub scope: CanonicalScopeRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(type = "unknown | null")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_target: Option<ApprovalTarget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup_requirements: Option<Vec<SetupRequirement>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_version_or_revision: Option<CurrentVersionOrRevision>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryInstruction>,
    /// Optional typed, redacted discriminator-specific details. MCP known
    /// tools use this for safe conflict targets such as an execution id;
    /// arbitrary internal error payloads never belong here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(type = "unknown | null")]
    pub details: Option<Value>,
    /// Safe cause of a native policy denial, never cross-scope detail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub denied_by: Option<DeniedBy>,
    /// Sensible operations held by this caller, when one is available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alternatives: Option<Vec<String>>,
    pub safe_message: String,
    pub correlation_id: String,
    pub replayed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
}

impl OrchestrationOutcome {
    #[must_use]
    pub fn new(
        code: OutcomeCode,
        status: OutcomeStatus,
        operation: impl Into<String>,
        scope: CanonicalScopeRef,
        correlation_id: impl Into<String>,
    ) -> Self {
        Self {
            code,
            status,
            operation: operation.into(),
            scope,
            result: None,
            approval_target: None,
            setup_requirements: None,
            current_version_or_revision: None,
            retry: None,
            denied_by: None,
            alternatives: None,
            details: None,
            safe_message: String::new(),
            correlation_id: correlation_id.into(),
            replayed: false,
            receipt_id: None,
            event_id: None,
        }
    }

    /// Construct a terminal denial from an already-redacted, server-owned cause.
    #[must_use]
    pub fn terminal_denial(
        operation: impl Into<String>,
        scope: CanonicalScopeRef,
        correlation_id: impl Into<String>,
        denied_by: DeniedBy,
    ) -> Self {
        let operation = operation.into();
        // Recovery actions have different pause gates: cancellation and retry
        // window reset remain available while re-execution is blocked.
        let lifetime =
            if operation == "task.action" && matches!(denied_by, DeniedBy::ProjectPaused(_)) {
                RetryScope::Turn
            } else {
                denied_by.scope()
            };
        let generic = denied_by == DeniedBy::Unspecified;
        let message = if generic {
            "Refused for this request. Repeating the identical call will be refused again."
                .to_owned()
        } else {
            let duration = match lifetime {
                RetryScope::Turn => "for this request in this turn",
                RetryScope::Session => "in this session while this cause holds",
            };
            format!("Operation refused: {denied_by}. Do not retry: repeating this call will be refused again {duration}.")
        };
        let mut outcome = Self::failed(
            OutcomeCode::PolicyDenied,
            operation,
            scope,
            correlation_id,
            message,
        );
        let mut retry = RetryInstruction::new(RetryAction::None, false);
        retry.scope = Some(lifetime);
        outcome.retry = Some(retry);
        outcome.denied_by = Some(denied_by);
        outcome
    }

    #[must_use]
    pub fn succeeded(
        operation: impl Into<String>,
        scope: CanonicalScopeRef,
        correlation_id: impl Into<String>,
        result: Option<Value>,
    ) -> Self {
        let mut outcome = Self::new(
            OutcomeCode::Ok,
            OutcomeStatus::Succeeded,
            operation,
            scope,
            correlation_id,
        );
        outcome.safe_message = "command completed".to_owned();
        outcome.result = result;
        outcome
    }

    #[must_use]
    pub fn failed(
        code: OutcomeCode,
        operation: impl Into<String>,
        scope: CanonicalScopeRef,
        correlation_id: impl Into<String>,
        safe_message: impl Into<String>,
    ) -> Self {
        let mut outcome = Self::new(
            code,
            OutcomeStatus::Failed,
            operation,
            scope,
            correlation_id,
        );
        outcome.safe_message = safe_message.into();
        outcome
    }

    #[must_use]
    pub fn status_for_code(code: OutcomeCode) -> OutcomeStatus {
        match code {
            OutcomeCode::Ok => OutcomeStatus::Succeeded,
            OutcomeCode::ApprovalRequired => OutcomeStatus::ApprovalRequired,
            OutcomeCode::SetupRequired => OutcomeStatus::SetupRequired,
            OutcomeCode::TaskBusy
            | OutcomeCode::VersionConflict
            | OutcomeCode::ActiveSessionConflict
            | OutcomeCode::DigestConflict
            | OutcomeCode::IdempotencyConflict
            | OutcomeCode::PolicyDenied
            | OutcomeCode::NotFound
            | OutcomeCode::TransientFailure
            | OutcomeCode::InternalFailure
            | OutcomeCode::ValidationError
            | OutcomeCode::ActionUnavailable => OutcomeStatus::Failed,
        }
    }
}

/// A bounded, redaction-safe summary of one tool call's outcome.
///
/// This is the one shape carried across the runtime-event boundary
/// (`TurnEventSink::tool_call_finished`), the durable execution/chat log, the
/// model-visible tool result, and the UI tool card, so all four surfaces
/// agree on the same code, message, retryability, and correlation id for a
/// completed tool call. Only fields already vetted safe for a caller belong
/// here: raw tool arguments, raw payloads, credentials, protected internal
/// causes, and unredacted internal error text must never be added to this
/// type or assigned into `safe_message`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ToolResultSummary {
    pub status: OutcomeStatus,
    pub code: OutcomeCode,
    pub safe_message: String,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_action: Option<RetryAction>,
    pub correlation_id: String,
    /// The typed Forge operation the call executed (for example
    /// `task.propose` or `skill.section`), when the tool returned an
    /// `OrchestrationOutcome`. Typed Forge tools multiplex many operations
    /// behind one tool name, so this is what tells a reader which one ran.
    /// Absent for raw workspace/runtime results.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
}

impl ToolResultSummary {
    #[must_use]
    pub fn new(
        status: OutcomeStatus,
        code: OutcomeCode,
        safe_message: impl Into<String>,
        correlation_id: impl Into<String>,
    ) -> Self {
        Self {
            status,
            code,
            safe_message: safe_message.into(),
            retryable: false,
            recovery_action: None,
            correlation_id: correlation_id.into(),
            operation: None,
        }
    }

    /// Derives the bounded summary from a full orchestration outcome.
    ///
    /// Every other `OrchestrationOutcome` field (`result`, `approval_target`,
    /// `setup_requirements`, `current_version_or_revision`, `receipt_id`,
    /// `details`, `receipt_id`, `event_id`) is deliberately dropped: those carry command-specific
    /// payload data that a tool-result summary never needs and must not
    /// widen into.
    #[must_use]
    pub fn from_orchestration_outcome(outcome: &OrchestrationOutcome) -> Self {
        let mut summary = Self::new(
            outcome.status,
            outcome.code,
            outcome.safe_message.clone(),
            outcome.correlation_id.clone(),
        );
        if let Some(retry) = &outcome.retry {
            summary.retryable = retry.retryable;
            summary.recovery_action = Some(retry.action);
        }
        summary.operation = Some(outcome.operation.clone());
        summary
    }

    /// A fixed, generic summary for a tool result that carried no typed
    /// orchestration outcome (for example a worktree read/write/command, or
    /// a raw runtime failure). Its content is not vetted safe to echo
    /// verbatim, so the message is a constant and never derived from the
    /// underlying result. `correlation_id` should be the tool call id: there
    /// is no command-domain correlation to report, and the id is already
    /// visible next to this entry everywhere it is logged.
    #[must_use]
    pub fn unclassified(is_error: bool, correlation_id: impl Into<String>) -> Self {
        if is_error {
            Self::new(
                OutcomeStatus::Failed,
                OutcomeCode::InternalFailure,
                "the tool call did not complete successfully",
                correlation_id,
            )
        } else {
            Self::new(
                OutcomeStatus::Succeeded,
                OutcomeCode::Ok,
                "the tool call completed successfully",
                correlation_id,
            )
        }
    }
}

/// Permission JSON cannot select precedence between conflicting keys.
pub const CONFLICTING_PERMISSION_DOCUMENT: &str = "conflicting_permission_document";
pub const INVALID_PERMISSION_DOCUMENT: &str = "invalid_permission_document";

#[cfg(test)]
mod tool_result_summary_tests {
    use super::*;

    #[test]
    fn denial_lifetimes_and_messages_follow_typed_causes() {
        for cause in [
            DeniedBy::AuthorityRevoked,
            DeniedBy::PermissionMissing("propose_task".to_owned()),
            DeniedBy::IdentityPaused,
            DeniedBy::ProjectPaused("environment_not_ready".to_owned()),
            DeniedBy::CharterNotAdopted,
            DeniedBy::OperationNotInScope,
            DeniedBy::TaskTerminal,
            DeniedBy::ReviewerReadOnly,
            DeniedBy::IndependentApprovalRequired,
            DeniedBy::UserRequestRequired,
            DeniedBy::LeasedTurnRequired,
            DeniedBy::CharterAdoptionNotApplicable,
            DeniedBy::ReadBoundaryRequired,
            DeniedBy::DirectCommandNotAdmitted,
            DeniedBy::ReviewAssignmentRequired,
            DeniedBy::ProfileNotSelected,
            DeniedBy::Unspecified,
            DeniedBy::TargetAgentPaused,
        ] {
            let encoded = serde_json::to_value(&cause).unwrap();
            assert_eq!(serde_json::from_value::<DeniedBy>(encoded).unwrap(), cause);
            assert_eq!(
                cause.withdraws_operation(),
                cause.scope() == RetryScope::Session
            );
            let outcome = OrchestrationOutcome::terminal_denial(
                "operation",
                CanonicalScopeRef::new(OutcomeScopeType::Task, "task"),
                "corr",
                cause.clone(),
            );
            assert_eq!(outcome.retry.as_ref().unwrap().scope, Some(cause.scope()));
            assert!(!outcome.safe_message.contains("Escalate"));
            if cause == DeniedBy::Unspecified {
                assert!(!outcome.safe_message.contains("Do not retry"));
                assert_eq!(outcome.retry.unwrap().action, RetryAction::None);
                assert!(!outcome.safe_message.contains("corrected input"));
            } else {
                assert!(outcome.safe_message.contains(match cause.scope() {
                    RetryScope::Session => "in this session while this cause holds",
                    RetryScope::Turn => "for this request in this turn",
                }));
            }
        }
    }

    #[test]
    fn derives_only_the_bounded_fields_from_a_structured_outcome() {
        let mut outcome = OrchestrationOutcome::failed(
            OutcomeCode::VersionConflict,
            "task.propose",
            CanonicalScopeRef::new(OutcomeScopeType::Task, "task-1"),
            "corr-1",
            "the authorized resource changed; refresh current state and retry",
        );
        outcome.retry = Some(RetryInstruction::new(RetryAction::RefreshAndRetry, true));
        // A field that a tool-result summary must never widen into, even
        // when the full outcome legitimately carries it for the model.
        outcome.result = Some(serde_json::json!({
            "internal_cause": "db error: password=hunter2-secret-token",
        }));

        let summary = ToolResultSummary::from_orchestration_outcome(&outcome);

        assert_eq!(summary.status, OutcomeStatus::Failed);
        assert_eq!(summary.code, OutcomeCode::VersionConflict);
        assert_eq!(
            summary.safe_message,
            "the authorized resource changed; refresh current state and retry"
        );
        assert!(summary.retryable);
        assert_eq!(summary.recovery_action, Some(RetryAction::RefreshAndRetry));
        assert_eq!(summary.correlation_id, "corr-1");
        assert_eq!(summary.operation.as_deref(), Some("task.propose"));

        let serialized = serde_json::to_string(&summary).expect("summary serializes");
        assert!(!serialized.contains("hunter2-secret-token"));
        assert!(!serialized.contains("internal_cause"));
    }

    #[test]
    fn unclassified_summaries_never_echo_dynamic_content() {
        let failed = ToolResultSummary::unclassified(true, "call-1");
        assert_eq!(failed.status, OutcomeStatus::Failed);
        assert_eq!(failed.code, OutcomeCode::InternalFailure);
        assert!(!failed.retryable);
        assert_eq!(failed.recovery_action, None);
        assert_eq!(failed.correlation_id, "call-1");
        assert_eq!(failed.operation, None);

        let ok = ToolResultSummary::unclassified(false, "call-2");
        assert_eq!(ok.status, OutcomeStatus::Succeeded);
        assert_eq!(ok.code, OutcomeCode::Ok);
    }
}
