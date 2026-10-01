use std::fmt;

use serde::{Deserialize, Serialize};

use crate::RecoveryAction;

/// The typed actor responsible for a task transition.
///
/// Forge is currently a local-first, single-user product. `user_id` is
/// therefore intentionally `None` until the multi-user story is implemented;
/// the action source remains available for audit and policy decisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Actor {
    User {
        user_id: Option<String>,
        source: UserActionSource,
    },
    Agent {
        agent_id: String,
        execution_id: Option<String>,
    },
    System {
        component: SystemComponent,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UserActionSource {
    Api,
    BoardDrag,
    Board,
    Override(Box<UserActionSource>),
    Recovery(RecoveryAction),
    Reassignment,
    RoleReassignment,
    ManualAdvance,
    Transition,
    RetryHook,
    SkipHookOnce,
    Test,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SystemComponent {
    #[default]
    General,
    TaskDispatcher,
    CancelTask,
    Workflow,
    Executor,
    LifecycleHook,
    Dispatch,
    Mcp,
    Test,
    CrashRecovery,
    HeartbeatMonitor,
    DaemonReport,
    Daemon,
    GracefulShutdown,
}

impl Actor {
    pub fn user(source: UserActionSource) -> Self {
        Self::User {
            user_id: None,
            source,
        }
    }

    pub fn agent(agent_id: impl Into<String>) -> Self {
        Self::Agent {
            agent_id: agent_id.into(),
            execution_id: None,
        }
    }

    pub fn system(component: SystemComponent) -> Self {
        Self::System { component }
    }

    pub fn is_user(&self) -> bool {
        matches!(self, Self::User { .. })
    }

    pub fn is_agent(&self) -> bool {
        matches!(self, Self::Agent { .. })
    }

    pub fn is_system(&self) -> bool {
        matches!(self, Self::System { .. })
    }

    pub fn display(&self) -> String {
        self.to_string()
    }

    /// Mark a user transition as an explicit workflow override.
    ///
    /// Existing overrides are preserved so repeated workflow resolution cannot
    /// produce nested `user:override:user:override:...` audit values.
    pub fn into_override(self) -> Self {
        match self {
            Self::User { user_id, source } => Self::User {
                user_id,
                source: match source {
                    UserActionSource::Override(_) => source,
                    source => UserActionSource::Override(Box::new(source)),
                },
            },
            actor => actor,
        }
    }
}

impl fmt::Display for Actor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::User { source, .. } => write!(f, "user:{source}"),
            Self::Agent { agent_id, .. } => write!(f, "agent:{agent_id}"),
            Self::System { component } => match component {
                SystemComponent::General => f.write_str("system"),
                component => write!(f, "system:{component}"),
            },
        }
    }
}

impl fmt::Display for UserActionSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Api => f.write_str("api"),
            Self::BoardDrag => f.write_str("board_drag"),
            Self::Board => f.write_str("board"),
            Self::Override(source) => write!(f, "override:{source}"),
            Self::Recovery(action) => write!(f, "recovery:{action}"),
            Self::Reassignment => f.write_str("reassignment"),
            Self::RoleReassignment => f.write_str("role_reassignment"),
            Self::ManualAdvance => f.write_str("manual_advance"),
            Self::Transition => f.write_str("transition"),
            Self::RetryHook => f.write_str("retry_hook"),
            Self::SkipHookOnce => f.write_str("skip_hook_once"),
            Self::Test => f.write_str("test"),
        }
    }
}

impl fmt::Display for SystemComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::General => "general",
            Self::TaskDispatcher => "task_dispatcher",
            Self::CancelTask => "cancel_task",
            Self::Workflow => "workflow",
            Self::Executor => "executor",
            Self::LifecycleHook => "lifecycle_hook",
            Self::Dispatch => "dispatch",
            Self::Mcp => "mcp",
            Self::Test => "test",
            Self::CrashRecovery => "crash_recovery",
            Self::HeartbeatMonitor => "heartbeat_monitor",
            Self::DaemonReport => "daemon_report",
            Self::Daemon => "daemon",
            Self::GracefulShutdown => "graceful_shutdown",
        };
        f.write_str(value)
    }
}

impl fmt::Display for RecoveryAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::ResumeSession => "resume_session",
            Self::Reexecute => "reexecute",
            Self::ResetToInitial => "reset_to_initial",
            Self::CancelTask => "cancel_task",
            Self::MarkReviewed => "mark_reviewed",
            Self::DeferToFollowUp => "defer_to_follow_up",
            Self::RetryHook => "retry_hook",
            Self::ResumeProcess => "resume_process",
            Self::UpdateWorkspaceAndRetryHook => "update_workspace_and_retry_hook",
            Self::SkipHookOnce => "skip_hook_once",
            Self::ResetRetryWindow => "reset_retry_window",
            Self::ProceedOnce => "proceed_once",
            Self::OpenInteractive => "open_interactive",
        };
        f.write_str(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_preserves_audit_formats() {
        assert_eq!(Actor::user(UserActionSource::Api).display(), "user:api");
        assert_eq!(Actor::agent("worker").display(), "agent:worker");
        assert_eq!(Actor::system(SystemComponent::General).display(), "system");
        assert_eq!(
            Actor::system(SystemComponent::TaskDispatcher).display(),
            "system:task_dispatcher"
        );
        assert_eq!(
            Actor::user(UserActionSource::Recovery(RecoveryAction::ResumeProcess)).display(),
            "user:recovery:resume_process"
        );
        assert_eq!(
            Actor::user(UserActionSource::Api).into_override().display(),
            "user:override:api"
        );
    }

    #[test]
    fn override_is_not_double_wrapped() {
        let actor = Actor::user(UserActionSource::Api).into_override();
        assert_eq!(actor.clone().into_override(), actor);
    }
}
