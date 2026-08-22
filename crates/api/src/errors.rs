use api_types::{
    ErrorResponse, TERMINAL_ACTIVE_EXECUTION, TERMINAL_ATTACH_TOKEN_INVALID,
    TERMINAL_DAEMON_UNAVAILABLE, TERMINAL_DISABLED, TERMINAL_INVALID_INPUT, TERMINAL_NOT_FOUND,
    TERMINAL_PATH_GUARDRAIL, TERMINAL_SESSION_LIMIT, TERMINAL_USER_LIMIT,
    TERMINAL_WORKSPACE_NOT_READY,
};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use db::DbError;
use serde_json::json;
use services::ServiceError;
use uuid::Uuid;

use crate::middleware;

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
    details: Option<serde_json::Value>,
}

pub type ApiResult<T> = Result<T, ApiError>;

impl ApiError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "bad_request",
            message: message.into(),
            details: None,
        }
    }

    pub fn bad_request_with_code(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code,
            message: message.into(),
            details: None,
        }
    }

    pub fn too_many_requests_with_code(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            code,
            message: message.into(),
            details: None,
        }
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "unauthorized",
            message: message.into(),
            details: None,
        }
    }

    pub fn method_not_allowed(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::METHOD_NOT_ALLOWED,
            code,
            message: message.into(),
            details: None,
        }
    }

    pub fn not_found(entity: &'static str, id: impl Into<String>) -> Self {
        let id = id.into();
        Self {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!("{entity} not found: {id}"),
            details: Some(json!({ "entity": entity, "id": id })),
        }
    }

    pub fn not_found_with_code(
        code: &'static str,
        entity: &'static str,
        id: impl Into<String>,
    ) -> Self {
        let id = id.into();
        Self {
            status: StatusCode::NOT_FOUND,
            code,
            message: format!("{entity} not found: {id}"),
            details: Some(json!({ "entity": entity, "id": id })),
        }
    }

    pub fn execution_logs_unavailable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "execution.logs_unavailable",
            message: message.into(),
            details: None,
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "internal_error",
            message: message.into(),
            details: None,
        }
    }

    fn invalid_operation(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "validation_error",
            message: message.into(),
            details: None,
        }
    }

    pub fn invalid_operation_conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: "validation_error",
            message: message.into(),
            details: None,
        }
    }

    pub fn conflict_with_code(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code,
            message: message.into(),
            details: None,
        }
    }

    pub fn conflict_with_code_and_details(
        code: &'static str,
        message: impl Into<String>,
        details: serde_json::Value,
    ) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code,
            message: message.into(),
            details: Some(details),
        }
    }

    pub fn unauthorized_with_code(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code,
            message: message.into(),
            details: None,
        }
    }

    pub fn unprocessable(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            code,
            message: message.into(),
            details: None,
        }
    }

    pub fn conflict(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code,
            message: message.into(),
            details: None,
        }
    }

    pub fn forbidden_with_code(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            code,
            message: message.into(),
            details: None,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let request_id =
            middleware::current_request_id().unwrap_or_else(|| Uuid::new_v4().to_string());
        if self.status.is_server_error() {
            tracing::error!(
                status = %self.status,
                code = self.code,
                message = %self.message,
                details = ?self.details,
                request_id = %request_id,
                "api request failed"
            );
        }
        let body = ErrorResponse {
            code: self.code.to_owned(),
            message: self.message,
            details: self.details,
            request_id,
        };
        (self.status, Json(body)).into_response()
    }
}

impl From<ServiceError> for ApiError {
    fn from(error: ServiceError) -> Self {
        match error {
            ServiceError::DependencyGate => Self {
                status: StatusCode::CONFLICT,
                code: "dependency_gate",
                message: "task dependencies are not satisfied".to_owned(),
                details: None,
            },
            ServiceError::ExecutionSetupRequired {
                message,
                requirements,
            } => Self::conflict_with_code_and_details(
                "execution_setup_required",
                message,
                json!({ "setup_requirements": requirements }),
            ),
            ServiceError::Db(error) => error.into(),
            ServiceError::Git(error) => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "git_error",
                message: "git error".to_owned(),
                details: Some(json!({ "details": error.to_string() })),
            },
            ServiceError::Review(error) => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "review_error",
                message: "review error".to_owned(),
                details: Some(json!({ "details": error.to_string() })),
            },
            ServiceError::NotFound { entity, id } => Self::not_found(entity, id),
            ServiceError::InvalidOperation { message } => Self::invalid_operation(message),
            ServiceError::AuthorizationDenied { message } => {
                Self::forbidden_with_code("authorization.invalid", message)
            }
            ServiceError::RateLimited {
                retry_after_seconds,
            } => Self::too_many_requests_with_code(
                "provider_authorization.rate_limited",
                format!("retry provider authorization in {retry_after_seconds} seconds"),
            ),
            ServiceError::TaskActionUnavailable {
                available_actions,
                reason,
            } => Self::conflict_with_code_and_details(
                "task_action.unavailable",
                reason.clone(),
                json!({
                    "available_actions": available_actions,
                    "reason": reason,
                }),
            ),
            ServiceError::Conflict(message) => Self::conflict_with_code("conflict", message),
            ServiceError::ProductGenesisActiveSession { session_id } => {
                Self::conflict_with_code_and_details(
                    "product_genesis.active_session_conflict",
                    "a Product Genesis session is already active",
                    json!({ "session_id": session_id }),
                )
            }
            ServiceError::DaemonUnavailable { daemon_id } => Self {
                status: StatusCode::SERVICE_UNAVAILABLE,
                code: "daemon_unavailable",
                message: format!("daemon {daemon_id} is unavailable"),
                details: Some(json!({ "daemon_id": daemon_id })),
            },
            ServiceError::DaemonTimeout { daemon_id, method } => Self {
                status: StatusCode::GATEWAY_TIMEOUT,
                code: "daemon_timeout",
                message: format!("daemon {daemon_id} timed out handling {method}"),
                details: Some(json!({ "daemon_id": daemon_id, "method": method })),
            },
            ServiceError::Domain(message) => Self::bad_request_with_code("domain_error", message),
            ServiceError::MissingPrimaryRepo { project_id } => Self::conflict_with_code(
                "missing_primary_repo",
                format!("project {project_id} has no primary repo"),
            ),
            ServiceError::RepoMismatch { project_id } => Self::conflict_with_code(
                "repo_mismatch",
                format!("repo does not match primary repo for project {project_id}"),
            ),
            ServiceError::PrProviderMissing { repo_id } => Self::conflict_with_code(
                "pr_provider_missing",
                format!("PR provider missing for repo {repo_id}"),
            ),
            ServiceError::PrProviderTokenMissing { repo_id } => Self::conflict_with_code(
                "pr_provider_token_missing",
                format!("PR provider token missing for repo {repo_id}"),
            ),
            ServiceError::PrSyncFailure { task_id, details } => Self {
                status: StatusCode::BAD_GATEWAY,
                code: "pr_sync_failure",
                message: format!("PR sync failure for task {task_id}: {details}"),
                details: Some(json!({ "task_id": task_id, "details": details })),
            },
            ServiceError::AgentPaused { agent_id } => Self::conflict_with_code(
                "agent_paused",
                format!("agent {agent_id} is paused and cannot accept new work"),
            ),
            ServiceError::ProjectPaused { project_id } => Self::conflict_with_code(
                "project_paused",
                format!("project {project_id} is paused"),
            ),
            ServiceError::GuardRejection { guard, reason } => {
                if reason.starts_with("SUBTASK_SEQUENCE_NOT_COMPLETE") {
                    Self {
                        status: StatusCode::PRECONDITION_FAILED,
                        code: "SUBTASK_SEQUENCE_NOT_COMPLETE",
                        message: "subtask sequence is not complete".to_owned(),
                        details: Some(json!({ "guard": guard, "reason": reason })),
                    }
                } else {
                    Self {
                        status: StatusCode::PRECONDITION_FAILED,
                        code: "guard_rejected",
                        message: format!("guard rejected: {guard}: {reason}"),
                        details: Some(json!({ "guard": guard, "reason": reason })),
                    }
                }
            }
            ServiceError::NestedSubtaskUnsupported => Self {
                status: StatusCode::BAD_REQUEST,
                code: "NESTED_SUBTASK_UNSUPPORTED",
                message: "nested subtasks are unsupported".to_string(),
                details: None,
            },
            ServiceError::SubtaskAssigneeUnsupported {
                root_coder_id,
                attempted,
            } => Self {
                status: StatusCode::CONFLICT,
                code: "SUBTASK_ASSIGNEE_UNSUPPORTED",
                message: "subtask assignee unsupported".to_string(),
                details: Some(json!({ "root_coder_id": root_coder_id, "attempted": attempted })),
            },
            ServiceError::SubtaskSequenceStarted { task_id } => Self {
                status: StatusCode::CONFLICT,
                code: "SUBTASK_SEQUENCE_STARTED",
                message: format!("subtask sequence already started for task {task_id}"),
                details: Some(json!({ "task_id": task_id })),
            },
            ServiceError::SubtaskManagedByRoot {
                task_id,
                root_task_id,
            } => Self {
                status: StatusCode::CONFLICT,
                code: "SUBTASK_MANAGED_BY_ROOT",
                message: format!("subtask {task_id} is managed by root {root_task_id}"),
                details: Some(json!({ "task_id": task_id, "root_task_id": root_task_id })),
            },
            ServiceError::ParentWorkspaceRequired { parent_task_id } => Self {
                status: StatusCode::CONFLICT,
                code: "PARENT_WORKSPACE_REQUIRED",
                message: format!("parent workspace required for task {parent_task_id}"),
                details: Some(json!({ "parent_task_id": parent_task_id })),
            },
            ServiceError::WorkspaceResetRequired { task_id, reason } => Self {
                status: StatusCode::CONFLICT,
                code: "WORKSPACE_RESET_REQUIRED",
                message: format!("workspace reset required for task {task_id}: {reason}"),
                details: Some(json!({ "task_id": task_id, "reason": reason })),
            },
            ServiceError::TaskSequenceAlreadyStarted { task_id } => Self {
                status: StatusCode::CONFLICT,
                code: "TASK_SEQUENCE_ALREADY_STARTED",
                message: format!("task sequence already started for task {task_id}"),
                details: Some(json!({ "task_id": task_id })),
            },
            ServiceError::TerminalDisabled => Self {
                status: StatusCode::FORBIDDEN,
                code: TERMINAL_DISABLED,
                message: "terminal access is disabled".to_owned(),
                details: None,
            },
            ServiceError::TerminalWorkspaceNotReady => Self {
                status: StatusCode::CONFLICT,
                code: TERMINAL_WORKSPACE_NOT_READY,
                message: "task workspace is not ready for terminal access".to_owned(),
                details: None,
            },
            ServiceError::TerminalSessionLimit { scope } => {
                let code = if scope == "user" {
                    TERMINAL_USER_LIMIT
                } else {
                    TERMINAL_SESSION_LIMIT
                };
                Self {
                    status: StatusCode::CONFLICT,
                    code,
                    message: format!("terminal session limit reached for {scope}"),
                    details: Some(json!({ "scope": scope })),
                }
            }
            ServiceError::TerminalDaemonUnavailable { daemon_id } => Self {
                status: StatusCode::CONFLICT,
                code: TERMINAL_DAEMON_UNAVAILABLE,
                message: format!("terminal daemon {daemon_id} is unavailable"),
                details: Some(json!({ "daemon_id": daemon_id })),
            },
            ServiceError::TerminalActiveExecution { workspace_id } => Self {
                status: StatusCode::CONFLICT,
                code: TERMINAL_ACTIVE_EXECUTION,
                message: format!("workspace {workspace_id} has active terminal or execution work"),
                details: Some(json!({ "workspace_id": workspace_id })),
            },
            ServiceError::TerminalAttachTokenInvalid => Self {
                status: StatusCode::FORBIDDEN,
                code: TERMINAL_ATTACH_TOKEN_INVALID,
                message: "terminal attach token is invalid".to_owned(),
                details: None,
            },
            ServiceError::TerminalPathGuardrail => Self {
                status: StatusCode::BAD_REQUEST,
                code: TERMINAL_PATH_GUARDRAIL,
                message: "terminal workspace path failed guardrail validation".to_owned(),
                details: None,
            },
            ServiceError::TerminalNotFound => Self {
                status: StatusCode::NOT_FOUND,
                code: TERMINAL_NOT_FOUND,
                message: "terminal session not found".to_owned(),
                details: None,
            },
            ServiceError::TerminalInvalidInput { message } => {
                Self::bad_request_with_code(TERMINAL_INVALID_INPUT, message)
            }
        }
    }
}

impl From<DbError> for ApiError {
    fn from(error: DbError) -> Self {
        match error {
            DbError::NotFound => Self {
                status: StatusCode::NOT_FOUND,
                code: "not_found",
                message: "resource not found".to_owned(),
                details: None,
            },
            DbError::VersionConflict => Self {
                status: StatusCode::CONFLICT,
                code: "version_conflict",
                message: "resource version conflict".to_owned(),
                details: None,
            },
            DbError::IdempotencyConflict => Self {
                status: StatusCode::CONFLICT,
                code: "idempotency_conflict",
                message: "idempotency key was already used for a different mutation".to_owned(),
                details: None,
            },
            DbError::TaskVersionConflict { expected, actual } => Self {
                status: StatusCode::CONFLICT,
                code: "version_conflict",
                message: "task version changed before the move committed".to_owned(),
                details: Some(json!({
                    "expected_task_version": expected,
                    "actual_task_version": actual,
                })),
            },
            DbError::BoardRevisionConflict { expected, actual } => Self {
                status: StatusCode::CONFLICT,
                code: "board_revision_conflict",
                message: "board changed before the move committed".to_owned(),
                details: Some(json!({
                    "expected_board_revision": expected,
                    "actual_board_revision": actual,
                })),
            },
            DbError::MoveOperationConflict { operation_id } => Self {
                status: StatusCode::CONFLICT,
                code: "operation_conflict",
                message: "operation ID was already used for a different move".to_owned(),
                details: Some(json!({ "operation_id": operation_id })),
            },
            DbError::MoveOperationIncomplete { operation_id } => Self {
                status: StatusCode::CONFLICT,
                code: "operation_incomplete",
                message: "move committed but its workflow result is incomplete; reconcile from board truth"
                    .to_owned(),
                details: Some(json!({ "operation_id": operation_id })),
            },
            DbError::InvalidTaskMove(message) => Self {
                status: StatusCode::UNPROCESSABLE_ENTITY,
                code: "invalid_task_move",
                message,
                details: None,
            },
            DbError::InvalidTransition => Self {
                status: StatusCode::UNPROCESSABLE_ENTITY,
                code: "invalid_transition",
                message: "invalid status transition".to_owned(),
                details: None,
            },
            DbError::AgentAtCapacity => Self {
                status: StatusCode::CONFLICT,
                code: "agent_at_capacity",
                message: "agent is at capacity".to_owned(),
                details: None,
            },
            DbError::CycleDetected => Self {
                status: StatusCode::UNPROCESSABLE_ENTITY,
                code: "cycle_detected",
                message: "dependency cycle detected".to_owned(),
                details: None,
            },
            DbError::InvalidCursor => Self {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_cursor",
                message: "invalid pagination cursor".to_owned(),
                details: None,
            },
            DbError::Check(message) => Self {
                status: StatusCode::BAD_REQUEST,
                code: "check_constraint",
                message,
                details: None,
            },
            DbError::InvalidSoftDelete => Self {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_soft_delete",
                message: "resource cannot be deleted in its current state".to_owned(),
                details: None,
            },
            other => Self::internal(other.to_string()),
        }
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(error: serde_json::Error) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_json",
            message: error.to_string(),
            details: None,
        }
    }
}

impl From<std::io::Error> for ApiError {
    fn from(error: std::io::Error) -> Self {
        Self::internal(error.to_string())
    }
}
