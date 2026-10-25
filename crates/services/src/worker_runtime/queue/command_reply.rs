//! Durable command replies use the service's typed error, just as the live
//! reply channel does. DB mutation replies remain the DB-owned envelope.
use crate::{placement, LeadRefusal, Result, ServiceError};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize)]
pub(super) struct CommandReply {
    pub result: Option<Value>,
    pub error: Option<StoredServiceError>,
}

impl CommandReply {
    pub fn encode(result: &Result<Value>) -> Result<String> {
        Self::encode_parts(result.as_ref().ok(), result.as_ref().err())
    }
    pub fn encode_error(error: &ServiceError) -> Result<String> {
        Self::encode_parts(None, Some(error))
    }
    fn encode_parts(result: Option<&Value>, error: Option<&ServiceError>) -> Result<String> {
        #[derive(Serialize)]
        struct Reply<'a> {
            result: Option<&'a Value>,
            error: Option<&'a ServiceError>,
        }
        serde_json::to_string(&Reply { result, error })
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))
    }

    pub fn decode<T: serde::de::DeserializeOwned>(self) -> Result<T> {
        match self.error {
            Some(error) => Err(error.into()),
            None => serde_json::from_value(self.result.unwrap_or(Value::Null))
                .map_err(|error| ServiceError::invalid_operation(error.to_string())),
        }
    }
}

/// Resource names in service errors originate in static service declarations.
/// Intern each distinct name once so re-reading replies does not allocate a new
/// static name for every failure. This preserves the existing error API.
fn intern_entity(name: String) -> &'static str {
    use std::{
        collections::HashSet,
        sync::{Mutex, OnceLock},
    };
    static NAMES: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();
    let mut names = NAMES
        .get_or_init(Default::default)
        .lock()
        .expect("resource names");
    if let Some(name) = names.get(name.as_str()) {
        return name;
    }
    let name = Box::leak(name.into_boxed_str());
    names.insert(name);
    name
}

pub(crate) mod database {
    use super::*;
    pub fn serialize<S: serde::Serializer>(
        error: &db::DbError,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        db::task_writer::TaskMutationReply::error_json(error).serialize(serializer)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<db::DbError, D::Error> {
        let reply = db::task_writer::TaskMutationReply {
            result: None,
            error: Some(Value::deserialize(deserializer)?),
        };
        match reply.decode::<Value>() {
            Err(error) => Ok(error),
            Ok(_) => Err(serde::de::Error::custom("stored DB error has no error")),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct IoError {
    os_code: Option<i32>,
    message: String,
}
impl From<&std::io::Error> for IoError {
    fn from(error: &std::io::Error) -> Self {
        Self {
            os_code: error.raw_os_error(),
            message: error.to_string(),
        }
    }
}
impl From<IoError> for std::io::Error {
    fn from(error: IoError) -> Self {
        match error.os_code {
            Some(code) => Self::from_raw_os_error(code),
            None => Self::other(error.message),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum GitError {
    CommandFailed {
        command: String,
        stdout: String,
        stderr: String,
    },
    MergeConflict {
        path: String,
        stderr: String,
    },
    UnsupportedRebaseConflict {
        details: String,
    },
    NoRebaseInProgress,
    Io(IoError),
}
impl From<&git::GitError> for GitError {
    fn from(error: &git::GitError) -> Self {
        match error {
            git::GitError::CommandFailed {
                command,
                stdout,
                stderr,
            } => Self::CommandFailed {
                command: command.clone(),
                stdout: stdout.clone(),
                stderr: stderr.clone(),
            },
            git::GitError::MergeConflict { path, stderr } => Self::MergeConflict {
                path: path.clone(),
                stderr: stderr.clone(),
            },
            git::GitError::UnsupportedRebaseConflict { details } => {
                Self::UnsupportedRebaseConflict {
                    details: details.clone(),
                }
            }
            git::GitError::NoRebaseInProgress => Self::NoRebaseInProgress,
            git::GitError::Io(error) => Self::Io(error.into()),
        }
    }
}
impl From<GitError> for git::GitError {
    fn from(error: GitError) -> Self {
        match error {
            GitError::CommandFailed {
                command,
                stdout,
                stderr,
            } => Self::CommandFailed {
                command,
                stdout,
                stderr,
            },
            GitError::MergeConflict { path, stderr } => Self::MergeConflict { path, stderr },
            GitError::UnsupportedRebaseConflict { details } => {
                Self::UnsupportedRebaseConflict { details }
            }
            GitError::NoRebaseInProgress => Self::NoRebaseInProgress,
            GitError::Io(error) => Self::Io(error.into()),
        }
    }
}

pub(crate) mod git_error {
    use super::*;
    pub fn serialize<S: serde::Serializer>(
        error: &git::GitError,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        GitError::from(error).serialize(serializer)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<git::GitError, D::Error> {
        GitError::deserialize(deserializer).map(Into::into)
    }
}

#[derive(Serialize, Deserialize)]
enum ExecutorError {
    Io(IoError),
    UsageExhausted {
        retry_after: Option<std::time::Duration>,
        usage_reports: Vec<executors::UsageReport>,
    },
    Unavailable {
        reason: String,
        usage_reports: Vec<executors::UsageReport>,
    },
    Other(String),
}
impl From<&executors::ExecutorError> for ExecutorError {
    fn from(error: &executors::ExecutorError) -> Self {
        match error {
            executors::ExecutorError::Io(error) => Self::Io(error.into()),
            executors::ExecutorError::UsageExhausted {
                retry_after,
                usage_reports,
            } => Self::UsageExhausted {
                retry_after: *retry_after,
                usage_reports: usage_reports.clone(),
            },
            executors::ExecutorError::Unavailable {
                reason,
                usage_reports,
            } => Self::Unavailable {
                reason: reason.clone(),
                usage_reports: usage_reports.clone(),
            },
            executors::ExecutorError::Other(message)
            | executors::ExecutorError::UncommittedWorktree(message) => {
                Self::Other(message.clone())
            }
        }
    }
}
impl From<ExecutorError> for executors::ExecutorError {
    fn from(error: ExecutorError) -> Self {
        match error {
            ExecutorError::Io(error) => Self::Io(error.into()),
            ExecutorError::UsageExhausted {
                retry_after,
                usage_reports,
            } => Self::UsageExhausted {
                retry_after,
                usage_reports,
            },
            ExecutorError::Unavailable {
                reason,
                usage_reports,
            } => Self::Unavailable {
                reason,
                usage_reports,
            },
            ExecutorError::Other(message) => Self::Other(message),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum ReviewError {
    CheckEnvironmentUnavailable,
    OwnerUnavailable { daemon_id: String },
    WorkspaceInfrastructure(String),
    Workspace(String),
    Db(#[serde(with = "database")] db::DbError),
    Io(IoError),
    Serde(String),
    Executor(ExecutorError),
    Git(GitError),
}
pub(crate) mod review_error {
    use super::*;
    pub fn serialize<S: serde::Serializer>(
        error: &::review::ReviewError,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use ::review::ReviewError as E;
        // The DB wrapper can be borrowed rather than rebuilding a DbError.
        if let E::Db(error) = error {
            return serde_json::json!({"Db":db::task_writer::TaskMutationReply::error_json(error)})
                .serialize(serializer);
        }
        let stored = match error {
            E::CheckEnvironmentUnavailable => ReviewError::CheckEnvironmentUnavailable,
            E::OwnerUnavailable { daemon_id } => ReviewError::OwnerUnavailable {
                daemon_id: daemon_id.clone(),
            },
            E::WorkspaceInfrastructure(message) => {
                ReviewError::WorkspaceInfrastructure(message.clone())
            }
            E::Workspace(message) => ReviewError::Workspace(message.clone()),
            E::Io(error) => ReviewError::Io(error.into()),
            E::Serde(error) => ReviewError::Serde(error.to_string()),
            E::Executor(error) => ReviewError::Executor(error.into()),
            E::Git(error) => ReviewError::Git(error.into()),
            E::Db(_) => unreachable!(),
        };
        stored.serialize(serializer)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<::review::ReviewError, D::Error> {
        use ::review::ReviewError as E;
        Ok(match ReviewError::deserialize(deserializer)? {
            ReviewError::CheckEnvironmentUnavailable => E::CheckEnvironmentUnavailable,
            ReviewError::OwnerUnavailable { daemon_id } => E::OwnerUnavailable { daemon_id },
            ReviewError::WorkspaceInfrastructure(message) => E::WorkspaceInfrastructure(message),
            ReviewError::Workspace(message) => E::Workspace(message),
            ReviewError::Db(error) => E::Db(error),
            ReviewError::Io(error) => E::Io(error.into()),
            ReviewError::Serde(message) => {
                E::Serde(serde_json::Error::io(std::io::Error::other(message)))
            }
            ReviewError::Executor(error) => E::Executor(error.into()),
            ReviewError::Git(error) => E::Git(error.into()),
        })
    }
}

#[derive(Deserialize)]
pub(super) enum StoredServiceError {
    ServerCommandsDisabled,
    ServerAgentsDisabled,
    ServerLocationRefused {
        message: String,
    },
    TaskBusy {
        pending_steps: i64,
        retry_after_ms: u64,
    },
    TaskConditionQuarantined {
        task_id: String,
    },
    PlacementUnavailable(placement::PlacementUnavailable),
    PrepareFailed {
        placement_id: String,
        message: String,
    },

    DependencyGate,
    ExecutionSetupRequired {
        message: String,
        requirements: Vec<api_types::SetupRequirement>,
    },
    Db(#[serde(with = "crate::worker_runtime::queue::command_reply::database")] db::DbError),
    Git(#[serde(with = "crate::worker_runtime::queue::command_reply::git_error")] git::GitError),
    Review(
        #[serde(with = "crate::worker_runtime::queue::command_reply::review_error")]
        review::ReviewError,
    ),
    NotFound {
        entity: String,
        id: String,
    },
    TurnFailure {
        failure: api_types::TurnFailure,
        error: Box<StoredServiceError>,
    },
    InvalidOperation {
        message: String,
    },
    ExecutionAlreadyRunning {
        scope: String,
        execution_id: String,
    },
    AuthorizationDenied {
        message: String,
    },
    RateLimited {
        retry_after_seconds: u64,
    },
    TaskActionUnavailable {
        available_actions: Vec<api_types::Offer>,
        reason: String,
        wait_cause: Option<api_types::DeniedBy>,
    },
    SessionRetired,
    Conflict(String),
    ProductGenesisActiveSession {
        session_id: String,
    },
    DaemonUnavailable {
        daemon_id: String,
    },
    DaemonNotReady {
        daemon_id: String,
    },
    DaemonUpgradeRequired {
        daemon_id: String,
    },
    DaemonTimeout {
        daemon_id: String,
        method: String,
    },
    Domain(String),
    MissingPrimaryRepo {
        project_id: String,
    },
    PrimaryRepoNotFound {
        project_id: String,
        repo_id: String,
    },
    RepoMismatch {
        project_id: String,
    },
    AgentPaused {
        agent_id: String,
    },
    ProjectPaused {
        project_id: String,
    },
    GuardRejection {
        guard: String,
        reason: String,
    },

    NestedSubtaskUnsupported,
    ParentWorkspaceRequired {
        parent_task_id: String,
    },
    SubtaskParentClosed {
        parent_task_id: String,
        state: String,
    },
    LeadRequiresNativeProfile {
        task_id: String,
        role: String,
    },
    LeadRoleNotSupported {
        task_id: String,
        role: String,
    },
    StepCheckRoleHeldByRoot {
        task_id: String,
        root_task_id: String,
        role: String,
    },
    LeadOperationRefused {
        refusal: LeadRefusal,
        message: String,
    },
    WorkspaceResetRequired {
        task_id: String,
        reason: String,
    },

    TerminalDisabled,

    TerminalWorkspaceNotReady,
    TerminalSessionLimit {
        scope: String,
    },
    TerminalDaemonUnavailable {
        daemon_id: String,
    },
    TerminalActiveExecution {
        workspace_id: String,
    },

    TerminalAttachTokenInvalid,

    TerminalPathGuardrail,

    TerminalNotFound,
    TerminalInvalidInput {
        message: String,
    },
}

impl From<StoredServiceError> for ServiceError {
    fn from(error: StoredServiceError) -> Self {
        match error {
            StoredServiceError::TaskBusy {
                pending_steps,
                retry_after_ms,
            } => Self::TaskBusy {
                pending_steps,
                retry_after_ms,
            },
            StoredServiceError::TaskConditionQuarantined { task_id } => {
                Self::TaskConditionQuarantined { task_id }
            }
            StoredServiceError::PlacementUnavailable(value) => Self::PlacementUnavailable(value),
            StoredServiceError::PrepareFailed {
                placement_id,
                message,
            } => Self::PrepareFailed {
                placement_id,
                message,
            },
            StoredServiceError::DependencyGate => Self::DependencyGate,
            StoredServiceError::ExecutionSetupRequired {
                message,
                requirements,
            } => Self::ExecutionSetupRequired {
                message,
                requirements,
            },
            StoredServiceError::Db(value) => Self::Db(value),
            StoredServiceError::Git(value) => Self::Git(value),
            StoredServiceError::Review(value) => Self::Review(value),
            StoredServiceError::NotFound { entity, id } => Self::NotFound {
                entity: intern_entity(entity),
                id,
            },
            StoredServiceError::TurnFailure { failure, error } => Self::TurnFailure {
                failure,
                error: Box::new((*error).into()),
            },
            StoredServiceError::ServerCommandsDisabled => Self::ServerCommandsDisabled,
            StoredServiceError::ServerAgentsDisabled => Self::ServerAgentsDisabled,
            StoredServiceError::ServerLocationRefused { message } => {
                Self::ServerLocationRefused { message }
            }
            StoredServiceError::InvalidOperation { message } => Self::InvalidOperation { message },
            StoredServiceError::ExecutionAlreadyRunning {
                scope,
                execution_id,
            } => Self::ExecutionAlreadyRunning {
                scope,
                execution_id,
            },
            StoredServiceError::AuthorizationDenied { message } => {
                Self::AuthorizationDenied { message }
            }
            StoredServiceError::RateLimited {
                retry_after_seconds,
            } => Self::RateLimited {
                retry_after_seconds,
            },
            StoredServiceError::TaskActionUnavailable {
                available_actions,
                reason,
                wait_cause,
            } => Self::TaskActionUnavailable {
                available_actions,
                reason,
                wait_cause,
            },
            StoredServiceError::SessionRetired => Self::SessionRetired,
            StoredServiceError::Conflict(value) => Self::Conflict(value),
            StoredServiceError::ProductGenesisActiveSession { session_id } => {
                Self::ProductGenesisActiveSession { session_id }
            }
            StoredServiceError::DaemonUnavailable { daemon_id } => {
                Self::DaemonUnavailable { daemon_id }
            }
            StoredServiceError::DaemonNotReady { daemon_id } => Self::DaemonNotReady { daemon_id },
            StoredServiceError::DaemonUpgradeRequired { daemon_id } => {
                Self::DaemonUpgradeRequired { daemon_id }
            }
            StoredServiceError::DaemonTimeout { daemon_id, method } => {
                Self::DaemonTimeout { daemon_id, method }
            }
            StoredServiceError::Domain(value) => Self::Domain(value),
            StoredServiceError::MissingPrimaryRepo { project_id } => {
                Self::MissingPrimaryRepo { project_id }
            }
            StoredServiceError::PrimaryRepoNotFound {
                project_id,
                repo_id,
            } => Self::PrimaryRepoNotFound {
                project_id,
                repo_id,
            },
            StoredServiceError::RepoMismatch { project_id } => Self::RepoMismatch { project_id },
            StoredServiceError::AgentPaused { agent_id } => Self::AgentPaused { agent_id },
            StoredServiceError::ProjectPaused { project_id } => Self::ProjectPaused { project_id },
            StoredServiceError::GuardRejection { guard, reason } => {
                Self::GuardRejection { guard, reason }
            }
            StoredServiceError::NestedSubtaskUnsupported => Self::NestedSubtaskUnsupported,
            StoredServiceError::ParentWorkspaceRequired { parent_task_id } => {
                Self::ParentWorkspaceRequired { parent_task_id }
            }
            StoredServiceError::SubtaskParentClosed {
                parent_task_id,
                state,
            } => Self::SubtaskParentClosed {
                parent_task_id,
                state,
            },
            StoredServiceError::LeadRequiresNativeProfile { task_id, role } => {
                Self::LeadRequiresNativeProfile { task_id, role }
            }
            StoredServiceError::LeadRoleNotSupported { task_id, role } => {
                Self::LeadRoleNotSupported { task_id, role }
            }
            StoredServiceError::StepCheckRoleHeldByRoot {
                task_id,
                root_task_id,
                role,
            } => Self::StepCheckRoleHeldByRoot {
                task_id,
                root_task_id,
                role,
            },
            StoredServiceError::LeadOperationRefused { refusal, message } => {
                Self::LeadOperationRefused { refusal, message }
            }
            StoredServiceError::WorkspaceResetRequired { task_id, reason } => {
                Self::WorkspaceResetRequired { task_id, reason }
            }
            StoredServiceError::TerminalDisabled => Self::TerminalDisabled,
            StoredServiceError::TerminalWorkspaceNotReady => Self::TerminalWorkspaceNotReady,
            StoredServiceError::TerminalSessionLimit { scope } => {
                Self::TerminalSessionLimit { scope }
            }
            StoredServiceError::TerminalDaemonUnavailable { daemon_id } => {
                Self::TerminalDaemonUnavailable { daemon_id }
            }
            StoredServiceError::TerminalActiveExecution { workspace_id } => {
                Self::TerminalActiveExecution { workspace_id }
            }
            StoredServiceError::TerminalAttachTokenInvalid => Self::TerminalAttachTokenInvalid,
            StoredServiceError::TerminalPathGuardrail => Self::TerminalPathGuardrail,
            StoredServiceError::TerminalNotFound => Self::TerminalNotFound,
            StoredServiceError::TerminalInvalidInput { message } => {
                Self::TerminalInvalidInput { message }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivery_failure_retains_the_existing_stored_error_shape() {
        let error = executors::ExecutorError::UncommittedWorktree("delivery diagnostic".to_owned());
        assert_eq!(
            serde_json::to_value(ExecutorError::from(&error)).unwrap(),
            serde_json::json!({"Other": "delivery diagnostic"})
        );
    }
}
