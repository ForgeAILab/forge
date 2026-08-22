use api_types::{
    CanonicalScopeRef, CurrentVersionOrRevision, OrchestrationOutcome, OutcomeCode,
    OutcomeScopeType, RetryAction, RetryInstruction, SetupRequirement,
};
use db::DbError;
use serde_json::{json, Value};
use services::ServiceError;
use uuid::Uuid;

use api_types::{
    TERMINAL_ACTIVE_EXECUTION, TERMINAL_ATTACH_TOKEN_INVALID, TERMINAL_DAEMON_UNAVAILABLE,
    TERMINAL_DISABLED, TERMINAL_INVALID_INPUT, TERMINAL_NOT_FOUND, TERMINAL_PATH_GUARDRAIL,
    TERMINAL_SESSION_LIMIT, TERMINAL_USER_LIMIT, TERMINAL_WORKSPACE_NOT_READY,
};

use crate::protocol::{error_response, success_response, McpResponse};

/// Stable scope for the unauthenticated MCP transport. It identifies the
/// server-owned transport principal without pretending that a payload field
/// supplied an account identity. Authenticated calls always replace it with
/// the project or account from `McpContext`.
const MCP_TRANSPORT_SCOPE_ID: &str = "mcp";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum McpErrorKind {
    /// A JSON-RPC transport/protocol error. These remain top-level JSON-RPC
    /// errors and are never presented to the model as a tool outcome.
    Protocol,
    /// A failure while invoking a known Forge tool. These are represented as
    /// an in-band MCP tool result by the HTTP adapter.
    Domain,
}

#[derive(Debug, Clone, Copy)]
enum ConflictActual {
    Version(i64),
    Revision(i64),
}

#[derive(Debug)]
pub(crate) struct McpToolError {
    pub(crate) code: i64,
    details: Box<McpToolErrorDetails>,
}

#[derive(Debug)]
struct McpToolErrorDetails {
    message: String,
    data: Option<Value>,
    kind: McpErrorKind,
    operation: Option<String>,
    scope: Option<CanonicalScopeRef>,
    current_version_or_revision: Option<Box<CurrentVersionOrRevision>>,
    retry_arguments: Option<Value>,
    conflict_actual: Option<ConflictActual>,
    correlation_id: String,
}

impl McpToolError {
    pub(crate) fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            details: Box::new(McpToolErrorDetails {
                message: message.into(),
                data: None,
                kind: McpErrorKind::Domain,
                operation: None,
                scope: None,
                current_version_or_revision: None,
                retry_arguments: None,
                conflict_actual: None,
                correlation_id: Uuid::new_v4().to_string(),
            }),
        }
    }

    pub(crate) fn protocol(code: i64, message: impl Into<String>) -> Self {
        let mut error = Self::new(code, message);
        error.details.kind = McpErrorKind::Protocol;
        error
    }

    pub(crate) fn with_data(mut self, data: Value) -> Self {
        self.details.data = Some(data);
        self
    }

    pub(crate) fn with_call_context(
        mut self,
        operation: impl Into<String>,
        project_id: Option<&str>,
        user_id: Option<&str>,
    ) -> Self {
        self.details.operation = Some(operation.into());
        self.details.scope = Some(match project_id.filter(|value| !value.trim().is_empty()) {
            Some(project_id) => CanonicalScopeRef::new(OutcomeScopeType::Project, project_id),
            None => CanonicalScopeRef::new(
                OutcomeScopeType::Account,
                user_id
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or(MCP_TRANSPORT_SCOPE_ID),
            ),
        });
        self
    }

    pub(crate) fn is_protocol(&self) -> bool {
        self.details.kind == McpErrorKind::Protocol
    }

    pub(crate) fn is_version_conflict(&self) -> bool {
        self.code == -32009
            && self
                .details
                .data
                .as_ref()
                .and_then(|data| data.get("code"))
                .and_then(Value::as_str)
                != Some("idempotency_conflict")
    }

    pub(crate) fn with_authorized_current_target(
        mut self,
        resource_type: impl Into<String>,
        resource_id: impl Into<String>,
        retry_field: impl Into<String>,
        current_version: i64,
    ) -> Self {
        if !self.is_version_conflict() {
            return self;
        }
        let actual = self
            .details
            .conflict_actual
            .unwrap_or(ConflictActual::Version(current_version));
        let mut current = CurrentVersionOrRevision::new(resource_type, resource_id);
        let value = match actual {
            ConflictActual::Version(version) => {
                current.version = Some(version);
                version
            }
            ConflictActual::Revision(revision) => {
                current.revision = Some(revision);
                revision
            }
        };
        self.details.current_version_or_revision = Some(Box::new(current));
        let mut retry_arguments = serde_json::Map::new();
        retry_arguments.insert(retry_field.into(), json!(value));
        self.details.retry_arguments = Some(Value::Object(retry_arguments));
        self
    }

    pub(crate) fn not_found(entity: &'static str, id: String) -> Self {
        Self::new(-32004, format!("{entity} not found: {id}"))
    }

    pub(crate) fn into_response(self, id: Value) -> McpResponse {
        error_response(id, self.code, self.details.message, self.details.data)
    }

    /// Render a known-tool failure as an MCP in-band tool result. The error's
    /// raw message and data are deliberately not forwarded: model-facing
    /// clients receive only stable codes and safe, bounded guidance.
    pub(crate) fn into_tool_response(self, id: Value) -> McpResponse {
        let outcome = self.into_outcome();
        let text = serde_json::to_string(&outcome).unwrap_or_else(|_| {
            // The envelope is composed only of JSON values, so this is a
            // defensive fallback for a future serialization change.
            "{\"code\":\"internal_failure\",\"status\":\"failed\",\"operation\":\"tools/call\",\"scope\":{\"scope_type\":\"account\",\"scope_id\":\"mcp\"},\"safe_message\":\"operation failed\",\"correlation_id\":\"unknown\",\"replayed\":false}".to_owned()
        });
        success_response(
            id,
            json!({
                "isError": true,
                "structuredContent": outcome,
                "content": [{
                    "type": "text",
                    "text": text,
                }],
            }),
        )
    }

    fn into_outcome(self) -> Value {
        let (outcome_code, safe_message, retry_action, retryable) = self.outcome_classification();
        let mut outcome = OrchestrationOutcome::new(
            outcome_code,
            OrchestrationOutcome::status_for_code(outcome_code),
            self.details
                .operation
                .unwrap_or_else(|| "tools/call".to_owned()),
            self.details.scope.unwrap_or_else(|| {
                CanonicalScopeRef::new(OutcomeScopeType::Account, MCP_TRANSPORT_SCOPE_ID)
            }),
            self.details.correlation_id,
        );
        outcome.safe_message = safe_message.to_owned();
        outcome.current_version_or_revision = self
            .details
            .current_version_or_revision
            .map(|current| *current);
        if outcome_code == OutcomeCode::SetupRequired {
            outcome.setup_requirements = Some(vec![SetupRequirement::new("operation_setup")]);
        }
        if retry_action.is_some() || retryable || self.details.retry_arguments.is_some() {
            let mut retry = RetryInstruction::new(
                retry_action.unwrap_or(RetryAction::RefreshAndRetry),
                retryable,
            );
            if let Some(Value::Object(arguments)) = self.details.retry_arguments {
                retry.arguments = arguments.into_iter().collect();
            }
            outcome.retry = Some(retry);
        }
        serde_json::to_value(outcome).unwrap_or_else(|_| {
            json!({
                "code": "internal_failure",
                "status": "failed",
                "operation": "tools/call",
                "scope": { "scope_type": "account", "scope_id": MCP_TRANSPORT_SCOPE_ID },
                "safe_message": "the operation failed",
                "correlation_id": Uuid::new_v4().to_string(),
                "replayed": false,
            })
        })
    }

    fn outcome_classification(&self) -> (OutcomeCode, &'static str, Option<RetryAction>, bool) {
        let detail_code = self
            .details
            .data
            .as_ref()
            .and_then(|data| data.get("code"))
            .and_then(Value::as_str);
        if matches!(detail_code, Some("idempotency_conflict")) {
            return (
                OutcomeCode::IdempotencyConflict,
                "the idempotency key is already bound to a different request",
                Some(RetryAction::UseNewIdempotencyKey),
                false,
            );
        }
        if matches!(
            detail_code,
            Some(
                "agent_setup_required"
                    | "daemon_unavailable"
                    | "setup_required"
                    | TERMINAL_DISABLED
                    | TERMINAL_DAEMON_UNAVAILABLE
                    | TERMINAL_WORKSPACE_NOT_READY
            )
        ) {
            return (
                OutcomeCode::SetupRequired,
                "additional setup is required before this operation can run",
                Some(RetryAction::CompleteSetup),
                false,
            );
        }
        if matches!(detail_code, Some("rate_limited")) {
            return (
                OutcomeCode::TransientFailure,
                "the operation is temporarily unavailable; retry later",
                Some(RetryAction::RetryAfter),
                true,
            );
        }

        match self.code {
            -32001 | -32003 => (
                OutcomeCode::PolicyDenied,
                "the caller is not authorized for this operation",
                None,
                false,
            ),
            -32004 => (
                OutcomeCode::NotFound,
                "the requested resource was not found",
                None,
                false,
            ),
            -32009 => (
                OutcomeCode::VersionConflict,
                "the authorized resource version changed; refresh before retrying",
                Some(RetryAction::RefreshAndRetry),
                true,
            ),
            -32010 => (
                OutcomeCode::PolicyDenied,
                "the requested state change is not allowed",
                None,
                false,
            ),
            -32029 => (
                OutcomeCode::TransientFailure,
                "the operation is temporarily unavailable; retry later",
                Some(RetryAction::RetryAfter),
                true,
            ),
            -32602 => (
                OutcomeCode::ValidationError,
                "the tool arguments are invalid",
                Some(RetryAction::CorrectInput),
                false,
            ),
            -32603 => (
                OutcomeCode::InternalFailure,
                "the operation failed",
                None,
                false,
            ),
            _ => (
                OutcomeCode::InternalFailure,
                "the operation failed",
                None,
                false,
            ),
        }
    }
}

impl From<ServiceError> for McpToolError {
    fn from(error: ServiceError) -> Self {
        let protected_cause = error.to_string();
        let mapped = match error {
            ServiceError::ExecutionSetupRequired {
                message,
                requirements,
            } => Self::new(-32029, "execution setup required").with_data(json!({
                "code": "setup_required",
                "message": message,
                "setup_requirements": requirements,
            })),
            ServiceError::DependencyGate => {
                Self::new(-32029, "dependency gate").with_data(json!({ "code": "setup_required" }))
            }
            ServiceError::NotFound { entity, id } => Self::not_found(entity, id),
            ServiceError::InvalidOperation { message } => Self::new(-32602, message),
            ServiceError::AuthorizationDenied { message } => {
                Self::new(-32003, message).with_data(json!({ "code": "authorization.invalid" }))
            }
            ServiceError::RateLimited {
                retry_after_seconds,
            } => Self::new(-32029, "rate limit exceeded").with_data(json!({
                "code": "rate_limited",
                "retry_after_seconds": retry_after_seconds,
            })),
            ServiceError::TaskActionUnavailable {
                available_actions,
                reason,
            } => Self::new(-32029, reason.clone()).with_data(json!({
                "available_actions": available_actions,
                "reason": reason,
            })),
            // A generic service conflict does not identify a versioned target;
            // do not infer a version conflict by parsing its prose.
            ServiceError::Conflict(_message) => Self::new(-32602, "operation conflict"),
            ServiceError::ProductGenesisActiveSession { .. } => {
                Self::new(-32602, "Product Genesis session already active")
            }
            ServiceError::MissingPrimaryRepo { .. }
            | ServiceError::RepoMismatch { .. }
            | ServiceError::PrProviderMissing { .. }
            | ServiceError::PrProviderTokenMissing { .. } => {
                Self::new(-32029, "required repository setup is missing")
                    .with_data(json!({ "code": "setup_required" }))
            }
            ServiceError::PrSyncFailure { .. } => Self::new(-32603, "PR sync failed"),
            ServiceError::NestedSubtaskUnsupported => {
                Self::new(-32602, "nested subtasks are unsupported").with_data(json!({
                    "code": "NESTED_SUBTASK_UNSUPPORTED"
                }))
            }
            ServiceError::SubtaskAssigneeUnsupported {
                root_coder_id,
                attempted,
            } => Self::new(-32602, "subtask assignee unsupported").with_data(json!({
                "code": "SUBTASK_ASSIGNEE_UNSUPPORTED",
                "root_coder_id": root_coder_id,
                "attempted": attempted
            })),
            ServiceError::SubtaskSequenceStarted { task_id } => Self::new(
                -32602,
                format!("subtask sequence already started for task {task_id}"),
            )
            .with_data(json!({
                "code": "SUBTASK_SEQUENCE_STARTED",
                "task_id": task_id
            })),
            ServiceError::SubtaskManagedByRoot {
                task_id,
                root_task_id,
            } => Self::new(
                -32029,
                format!("subtask {task_id} is managed by root {root_task_id}"),
            )
            .with_data(json!({
                "code": "SUBTASK_MANAGED_BY_ROOT",
                "task_id": task_id,
                "root_task_id": root_task_id
            })),
            ServiceError::ParentWorkspaceRequired { parent_task_id } => Self::new(
                -32602,
                format!("parent workspace required for task {parent_task_id}"),
            )
            .with_data(json!({
                "code": "PARENT_WORKSPACE_REQUIRED",
                "parent_task_id": parent_task_id
            })),
            ServiceError::WorkspaceResetRequired { task_id, reason } => Self::new(
                -32602,
                format!("workspace reset required for task {task_id}: {reason}"),
            )
            .with_data(json!({
                "code": "WORKSPACE_RESET_REQUIRED",
                "task_id": task_id,
                "reason": reason
            })),
            ServiceError::TaskSequenceAlreadyStarted { task_id } => Self::new(
                -32602,
                format!("task sequence already started for task {task_id}"),
            )
            .with_data(json!({
                "code": "TASK_SEQUENCE_ALREADY_STARTED",
                "task_id": task_id
            })),
            ServiceError::TerminalDisabled => Self::new(-32029, "terminal access is disabled")
                .with_data(json!({
                    "code": TERMINAL_DISABLED
                })),
            ServiceError::TerminalWorkspaceNotReady => {
                Self::new(-32029, "task workspace is not ready for terminal access").with_data(
                    json!({
                        "code": TERMINAL_WORKSPACE_NOT_READY
                    }),
                )
            }
            ServiceError::TerminalSessionLimit { scope } => {
                let code = if scope == "user" {
                    TERMINAL_USER_LIMIT
                } else {
                    TERMINAL_SESSION_LIMIT
                };
                Self::new(
                    -32029,
                    format!("terminal session limit reached for {scope}"),
                )
                .with_data(json!({
                    "code": code,
                    "scope": scope
                }))
            }
            ServiceError::TerminalDaemonUnavailable { daemon_id } => {
                Self::new(-32029, format!("terminal daemon {daemon_id} unavailable")).with_data(
                    json!({
                        "code": TERMINAL_DAEMON_UNAVAILABLE,
                        "daemon_id": daemon_id
                    }),
                )
            }
            ServiceError::TerminalActiveExecution { workspace_id } => Self::new(
                -32029,
                format!("workspace {workspace_id} has active terminal or execution work"),
            )
            .with_data(json!({
                "code": TERMINAL_ACTIVE_EXECUTION,
                "workspace_id": workspace_id
            })),
            ServiceError::TerminalAttachTokenInvalid => {
                Self::new(-32602, "terminal attach token is invalid").with_data(json!({
                    "code": TERMINAL_ATTACH_TOKEN_INVALID
                }))
            }
            ServiceError::TerminalPathGuardrail => Self::new(
                -32602,
                "terminal workspace path failed guardrail validation",
            )
            .with_data(json!({
                "code": TERMINAL_PATH_GUARDRAIL
            })),
            ServiceError::TerminalNotFound => Self::new(-32004, "terminal session not found")
                .with_data(json!({
                    "code": TERMINAL_NOT_FOUND
                })),
            ServiceError::TerminalInvalidInput { message } => {
                Self::new(-32602, message).with_data(json!({
                    "code": TERMINAL_INVALID_INPUT
                }))
            }
            ServiceError::DaemonUnavailable { daemon_id } => {
                Self::new(-32029, format!("daemon {daemon_id} unavailable"))
                    .with_data(json!({"code": "daemon_unavailable", "daemon_id": daemon_id}))
            }
            ServiceError::DaemonTimeout { daemon_id, method } => {
                Self::new(-32029, format!("daemon {daemon_id} timed out on {method}")).with_data(
                    json!({"code": "daemon_timeout", "daemon_id": daemon_id, "method": method}),
                )
            }
            ServiceError::Domain(message) => Self::new(-32602, message),
            ServiceError::Db(error) => error.into(),
            // Keep provider/repository internals out of both the structured
            // outcome and the legacy JSON-RPC error path. The protected cause
            // remains available to the server-side caller for logging.
            ServiceError::Git(_error) => Self::new(-32603, "git error"),
            ServiceError::Review(_error) => Self::new(-32603, "review error"),
            ServiceError::GuardRejection { guard, reason } => {
                if reason.starts_with("SUBTASK_SEQUENCE_NOT_COMPLETE") {
                    Self::new(-32029, "subtask sequence is not complete").with_data(json!({
                        "code": "SUBTASK_SEQUENCE_NOT_COMPLETE",
                        "guard": guard,
                        "reason": reason,
                    }))
                } else {
                    Self::new(-32029, format!("guard rejected: {guard}: {reason}"))
                }
            }
            ServiceError::AgentPaused { agent_id } => {
                Self::new(-32029, format!("agent {agent_id} is paused")).with_data(json!({
                    "code": "agent_paused",
                    "agent_id": agent_id
                }))
            }
            ServiceError::ProjectPaused { project_id } => {
                Self::new(-32029, format!("project {project_id} is paused")).with_data(json!({
                    "code": "project_paused",
                    "project_id": project_id
                }))
            }
        };
        tracing::debug!(
            correlation_id = %mapped.details.correlation_id,
            error = %protected_cause,
            "MCP service failure mapped to a safe tool outcome"
        );
        mapped
    }
}

impl From<DbError> for McpToolError {
    fn from(error: DbError) -> Self {
        let protected_cause = error.to_string();
        let mapped = match error {
            DbError::NotFound => Self::new(-32004, "not found"),
            DbError::VersionConflict => Self::new(-32009, "version conflict"),
            DbError::TaskVersionConflict {
                expected: _,
                actual,
            } => {
                let mut result = Self::new(-32009, "version conflict");
                result.details.conflict_actual = Some(ConflictActual::Version(actual));
                result
            }
            DbError::BoardRevisionConflict {
                expected: _,
                actual,
            } => {
                let mut result = Self::new(-32009, "revision conflict");
                result.details.conflict_actual = Some(ConflictActual::Revision(actual));
                result
            }
            DbError::IdempotencyConflict => Self::new(-32009, "idempotency conflict")
                .with_data(json!({ "code": "idempotency_conflict" })),
            DbError::InvalidTransition => Self::new(-32010, "invalid transition"),
            DbError::InvalidSoftDelete => Self::new(-32010, "invalid soft delete"),
            DbError::AgentAtCapacity => Self::new(-32029, "agent at capacity"),
            DbError::InvalidCursor => Self::new(-32602, "invalid cursor"),
            DbError::DependencyGate => Self::new(-32029, "dependency gate").with_data(json!({
                "code": "setup_required"
            })),
            _ => Self::new(-32603, "internal error"),
        };
        tracing::debug!(
            correlation_id = %mapped.details.correlation_id,
            error = %protected_cause,
            "MCP database failure mapped to a safe tool outcome"
        );
        mapped
    }
}
