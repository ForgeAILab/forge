use std::fmt;

use serde::{Deserialize, Serialize};

use crate::TaskAction;

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
        /// The request authenticated with a credential an agent may also hold
        /// (MCP: PATs, sessions and OAuth tokens all resolve to the user, with
        /// no agent-scoped kind). It keeps the user's action authority but not
        /// the owner's budget exemption: see [`Actor::is_owner`].
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        delegated: bool,
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
    Action(TaskAction),
    Reassignment,
    RoleReassignment,
    Transition,
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
            delegated: false,
        }
    }

    /// A user acting through a credential that cannot be told apart from an
    /// agent's (MCP).
    pub fn delegated_user(user_id: impl Into<String>, source: UserActionSource) -> Self {
        Self::User {
            user_id: Some(user_id.into()),
            source,
            delegated: true,
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

    /// Authenticated as the human owner (REST, web and `forge-ctl` sessions,
    /// escalation answers). Only the owner's actions spend no retry budget.
    pub fn is_owner(&self) -> bool {
        matches!(
            self,
            Self::User {
                delegated: false,
                ..
            }
        )
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
            Self::User {
                user_id,
                source,
                delegated,
            } => Self::User {
                user_id,
                source: match source {
                    UserActionSource::Override(_) => source,
                    source => UserActionSource::Override(Box::new(source)),
                },
                delegated,
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
            Self::Action(action) => write!(f, "action:{action}"),
            Self::Reassignment => f.write_str("reassignment"),
            Self::RoleReassignment => f.write_str("role_reassignment"),
            Self::Transition => f.write_str("transition"),
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
            Actor::user(UserActionSource::Action(TaskAction::retry())).display(),
            "user:action:retry"
        );
        assert_eq!(
            Actor::user(UserActionSource::Api).into_override().display(),
            "user:override:api"
        );
    }

    #[test]
    fn delegated_user_keeps_audit_format_but_not_owner_authority() {
        let delegated = Actor::delegated_user("u", UserActionSource::Api);
        assert_eq!(delegated.display(), "user:api");
        assert!(delegated.is_user());
        assert!(!delegated.is_owner());
        assert!(!delegated.clone().into_override().is_owner());
        assert!(Actor::user(UserActionSource::Api).is_owner());
        assert!(!Actor::agent("a").is_owner());
        let json = serde_json::to_value(Actor::user(UserActionSource::Api)).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"User":{"user_id":null,"source":"Api"}})
        );
        let round: Actor =
            serde_json::from_value(serde_json::to_value(&delegated).unwrap()).unwrap();
        assert_eq!(round, delegated);
    }

    #[test]
    fn override_is_not_double_wrapped() {
        let actor = Actor::user(UserActionSource::Api).into_override();
        assert_eq!(actor.clone().into_override(), actor);
    }
}
