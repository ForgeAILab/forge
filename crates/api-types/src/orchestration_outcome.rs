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
}

impl OutcomeCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
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

/// Bounded corrective information for an outcome.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct RetryInstruction {
    pub action: RetryAction,
    pub retryable: bool,
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
            safe_message: String::new(),
            correlation_id: correlation_id.into(),
            replayed: false,
            receipt_id: None,
            event_id: None,
        }
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
            OutcomeCode::VersionConflict
            | OutcomeCode::ActiveSessionConflict
            | OutcomeCode::DigestConflict
            | OutcomeCode::IdempotencyConflict
            | OutcomeCode::PolicyDenied
            | OutcomeCode::NotFound
            | OutcomeCode::TransientFailure
            | OutcomeCode::InternalFailure
            | OutcomeCode::ValidationError => OutcomeStatus::Failed,
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
    /// `event_id`) is deliberately dropped: those carry command-specific
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

#[cfg(test)]
mod tool_result_summary_tests {
    use super::*;

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
