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

    /// A malformed or contract-violating request body. Every route's `Json`
    /// extractor (`crate::json::Json`) maps a deserialization rejection to
    /// this so a closed-vocabulary violation (for example an
    /// `AdaptiveTaskOperation` outside `split`/`sequence`/`replace`) is one
    /// `400 validation_error`, never axum's default bare `422`.
    pub fn validation(message: impl Into<String>) -> Self {
        Self::bad_request_with_code("validation_error", message)
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
            ServiceError::TaskBusy {pending_steps,retry_after_ms} => Self::conflict_with_code_and_details(api_types::TASK_BUSY,
                "Task has pending steps; accepted work remains queued",serde_json::to_value(api_types::TaskBusyDetails {
                    pending_steps,retry_after_ms,retry_hint:"Retry after pending steps settle; refetch the Task version first".to_owned(),
                }).expect("Task busy details serialize")),
            ServiceError::TaskConditionQuarantined { task_id } => Self::conflict_with_code_and_details(
                api_types::TASK_CONDITION_QUARANTINED,
                "This Task's stored condition was written by a newer Forge build and is quarantined. Nothing was changed. Run a build that understands it",
                json!({ "task_id": task_id }),
            ),
            ServiceError::PlacementUnavailable(error) => Self::conflict_with_code_and_details(
                if error.needs_daemon_upgrade() { api_types::DAEMON_UPGRADE_REQUIRED } else { "placement_unavailable" },
                error.to_string(),
                json!({
                    "needs_human": error.needs_daemon_upgrade(),
                    "task_id": error.task_id,
                    "repo_id": error.repo_id,
                    "rejected_candidates": error.rejected_candidates,
                }),
            ),
            ServiceError::PrepareFailed {
                placement_id,
                message,
            } => Self::conflict_with_code_and_details(
                "prepare_failed",
                message,
                json!({ "placement_id": placement_id, "failure_cause": "prepare_failed" }),
            ),
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
            ServiceError::ExecutionAlreadyRunning {
                scope,
                execution_id,
            } => Self::conflict_with_code_and_details(
                "execution.already_running",
                format!("{scope} execution already running: {execution_id}"),
                json!({ "scope": scope, "execution_id": execution_id }),
            ),
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
                wait_cause,
            } => Self::conflict_with_code_and_details(
                "action_unavailable",
                reason.clone(),
                json!({
                    "available_actions": available_actions,
                    "reason": reason,
                    "denied_by": wait_cause.as_ref().map(ToString::to_string),
                    "retry": wait_cause.map(|_| json!({"action":"none", "scope":"turn", "retryable":false})),
                }),
            ),
            ServiceError::TurnFailure { error, .. } => Self::from(*error),
            ServiceError::Conflict(message) => Self::conflict_with_code("conflict", message),
            ServiceError::ProductGenesisActiveSession { session_id } => {
                Self::conflict_with_code_and_details(
                    "product_genesis.active_session_conflict",
                    "a Product Genesis session is already active",
                    json!({ "session_id": session_id }),
                )
            }
            ServiceError::DaemonNotReady { daemon_id } => Self {
                status: StatusCode::SERVICE_UNAVAILABLE,
                code: "daemon_not_ready",
                message: format!("daemon {daemon_id} has not sent its command handshake; wait for it to become ready"),
                details: Some(json!({ "daemon_id": daemon_id })),
            },
            ServiceError::DaemonUpgradeRequired { daemon_id } => Self {
                status: StatusCode::CONFLICT,
                code: api_types::DAEMON_UPGRADE_REQUIRED,
                message: api_types::DAEMON_UPGRADE_REQUIRED_MESSAGE.to_owned(),
                details: Some(json!({ "daemon_id": daemon_id, "needs_human": true })),
            },
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
            ServiceError::PrimaryRepoNotFound {
                project_id,
                repo_id,
            } => Self::conflict_with_code(
                "primary_repo_not_found",
                format!("project {project_id} primary repo was not found: {repo_id}"),
            ),
            ServiceError::RepoMismatch { project_id } => Self::conflict_with_code(
                "repo_mismatch",
                format!("repo does not match primary repo for project {project_id}"),
            ),
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
            DbError::PermissionDocument(error) => Self::bad_request_with_code(
                error.code(), error.to_string()),
            DbError::TaskBusy {pending_steps,retry_after_ms} => ServiceError::TaskBusy {pending_steps,retry_after_ms}.into(),
            DbError::TaskConditionQuarantined { task_id } => ServiceError::TaskConditionQuarantined { task_id }.into(),
            DbError::NotFound => Self {
                status: StatusCode::NOT_FOUND,
                code: "not_found",
                message: "resource not found".to_owned(),
                details: None,
            },
            DbError::TurnNotRetryable => Self::conflict_with_code("turn_not_retryable", "Agent Chat turn is not retryable"),
            DbError::ChatTurnLive => Self::conflict_with_code("another_turn_live", "another Agent Chat turn is live"),
            DbError::DeadLetterNotReplayable => Self::conflict_with_code("dead_letter_not_replayable", "Only whole-event dead letters can be replayed; dismiss this item instead"),
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
            DbError::MachineConnected => Self::conflict_with_code("machine_connected", "Stop the daemon before removing this connected machine"),
            DbError::LocalMachine => Self::conflict_with_code("local_machine", "The embedded server machine cannot be removed"),
            DbError::MachineOwnedByAnotherUser => Self::conflict_with_code("machine_owned", "This machine is registered to another user"),
            DbError::MachineAtCapacity => Self::conflict_with_code("machine_capacity", "Machine has no available run capacity"),
            DbError::AgentAtCapacity => Self {
                status: StatusCode::CONFLICT,
                code: "agent_at_capacity",
                message: "agent is at capacity".to_owned(),
                details: None,
            },
            DbError::AgentPaused { agent_id } => Self::conflict_with_code_and_details(
                "agent_paused",
                format!("agent {agent_id} is paused and cannot accept new work"),
                json!({ "agent_id": agent_id }),
            ),
            DbError::ExecutionAlreadyRunning {
                scope,
                execution_id,
            } => Self::conflict_with_code_and_details(
                "execution.already_running",
                format!("{scope} execution already running: {execution_id}"),
                json!({ "scope": scope, "execution_id": execution_id }),
            ),
            DbError::ProjectPaused { project_id } => Self::conflict_with_code_and_details(
                "project_paused",
                format!("project {project_id} is paused"),
                json!({ "project_id": project_id }),
            ),
            DbError::RepoInUse { repo_id } => Self::conflict_with_code_and_details(
                "repo_in_use",
                format!("repo {repo_id} has active executions or workspace leases"),
                json!({ "repo_id": repo_id }),
            ),
            DbError::ProjectInUse {
                project_id,
                running_executions,
                active_leases,
            } => Self::conflict_with_code_and_details(
                "project_in_use",
                format!(
                    "project {project_id} has {running_executions} running execution(s) and \
                     {active_leases} active workspace lease(s); retry with ?force=true to \
                     request cancellation before deletion"
                ),
                json!({
                    "project_id": project_id,
                    "running_executions": running_executions,
                    "active_leases": active_leases,
                }),
            ),
            DbError::ResourceInUse { resource, reason } => Self::conflict_with_code_and_details(
                "resource_in_use",
                format!("{resource} is in use: {reason}"),
                json!({ "resource": resource, "reason": reason }),
            ),
            error if error.is_foreign_key_violation() => Self::conflict_with_code_and_details(
                "resource_in_use",
                "the resource is still referenced by other records".to_owned(),
                json!({ "resource": null, "reason": "foreign_key" }),
            ),
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
            DbError::ReviewDetailsCorrupt { review_id, .. } => Self::internal(format!(
                "persisted review {review_id} has invalid step_results_json"
            )),
            DbError::TransitionBridgeCorrupt {
                transition_log_id, ..
            } => Self::internal(format!(
                "persisted transition {transition_log_id} has an unreadable bridge"
            )),
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

#[cfg(test)]
mod placement_tests {
    use super::*;

    #[test]
    fn daemon_upgrade_maps_to_human_action_and_readiness_stays_transient() {
        let upgrade = ApiError::from(ServiceError::DaemonUpgradeRequired {
            daemon_id: "old".into(),
        });
        assert_eq!(upgrade.status, StatusCode::CONFLICT);
        assert_eq!(upgrade.code, api_types::DAEMON_UPGRADE_REQUIRED);
        assert!(upgrade.message.contains("upgrade the daemon"));
        assert_eq!(upgrade.details.unwrap()["needs_human"], true);
        let ready = ApiError::from(ServiceError::DaemonNotReady {
            daemon_id: "new".into(),
        });
        assert_eq!(ready.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(ready.code, "daemon_not_ready");
        assert!(!ready.message.contains("upgrade"));
        let placement = ApiError::from(ServiceError::PlacementUnavailable(
            services::placement::PlacementUnavailable {
                task_id: "task".into(),
                repo_id: "repo".into(),
                rejected_candidates: vec![services::placement::CandidateRejection {
                    failing_checks: Vec::new(),
                    repo_location_id: "location".into(),
                    owner_kind: "daemon".into(),
                    daemon_id: Some("old".into()),
                    runtime_id: Some("runtime".into()),
                    filter_codes: vec![
                        services::placement::PlacementFilterCode::DaemonUpgradeRequired,
                    ],
                }],
            },
        ));
        assert_eq!(placement.code, api_types::DAEMON_UPGRADE_REQUIRED);
        assert_eq!(placement.details.unwrap()["needs_human"], true);
    }

    #[test]
    fn placement_unavailable_preserves_candidate_rejections_in_409() {
        let error = ApiError::from(ServiceError::PlacementUnavailable(
            services::placement::PlacementUnavailable {
                task_id: "task".to_owned(),
                repo_id: "repo".to_owned(),
                rejected_candidates: vec![services::placement::CandidateRejection {
                    failing_checks: Vec::new(),
                    repo_location_id: "location".to_owned(),
                    owner_kind: "daemon".to_owned(),
                    daemon_id: Some("daemon".to_owned()),
                    runtime_id: Some("runtime".to_owned()),
                    filter_codes: vec![
                        services::placement::PlacementFilterCode::ExecutorUnavailable,
                    ],
                }],
            },
        ));
        assert_eq!(error.status, StatusCode::CONFLICT);
        assert_eq!(error.code, "placement_unavailable");
        let details = error.details.unwrap();
        assert_eq!(details["task_id"], "task");
        assert_eq!(details["rejected_candidates"][0]["daemon_id"], "daemon");
        assert_eq!(
            details["rejected_candidates"][0]["filter_codes"][0],
            "executor_unavailable"
        );
    }

    #[test]
    fn prepare_failed_exposes_placement_failure_cause() {
        let error = ApiError::from(ServiceError::PrepareFailed {
            placement_id: "placement".to_owned(),
            message: "owner refused preparation".to_owned(),
        });
        assert_eq!(error.status, StatusCode::CONFLICT);
        assert_eq!(error.code, "prepare_failed");
        assert_eq!(error.details.unwrap()["failure_cause"], "prepare_failed");
    }
    #[tokio::test]
    async fn foreign_key_refusal_is_a_typed_conflict_not_a_500() {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        sqlx::raw_sql("CREATE TABLE parent(id TEXT PRIMARY KEY); CREATE TABLE child(id TEXT PRIMARY KEY, parent_id TEXT REFERENCES parent(id) ON DELETE RESTRICT); INSERT INTO parent VALUES('p'); INSERT INTO child VALUES('c','p');")
            .execute(&pool)
            .await
            .unwrap();
        let refused = sqlx::query("DELETE FROM parent WHERE id='p'")
            .execute(&pool)
            .await
            .unwrap_err();
        let error = ApiError::from(ServiceError::from(DbError::from(refused)));
        assert_eq!(error.status, StatusCode::CONFLICT, "{}", error.message);
        assert_eq!(error.code, "resource_in_use");
        let typed = ApiError::from(DbError::ResourceInUse {
            resource: "workspace w".into(),
            reason: "a remote workspace operation is still running".into(),
        });
        assert_eq!(typed.status, StatusCode::CONFLICT);
        assert_eq!(typed.code, "resource_in_use");
        assert_eq!(typed.details.unwrap()["resource"], "workspace w");
    }
    #[test]
    fn task_busy_preserves_retry_details_across_error_boundaries() {
        let error = ApiError::from(ServiceError::from(DbError::TaskBusy {
            pending_steps: 3,
            retry_after_ms: 250,
        }));
        assert_eq!(error.status, StatusCode::CONFLICT);
        assert_eq!(error.code, api_types::TASK_BUSY);
        let details = error.details.unwrap();
        assert_eq!(details["pending_steps"], 3);
        assert_eq!(details["retry_after_ms"], 250);
        assert!(details["retry_hint"].as_str().unwrap().contains("refetch"));
    }
}
