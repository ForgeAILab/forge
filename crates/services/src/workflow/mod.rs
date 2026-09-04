use std::{path::PathBuf, sync::Arc};

use api_types::{Actor, GateConfig, StateDefinition, StateKind, WorkflowDefinition};
use async_trait::async_trait;
use serde_json::Value;

use crate::{
    merge_service::MergeService, terminal_service::TerminalActivityTracker,
    workspace_cleanup::WorkspaceCleanupScheduler,
    workspace_execution_lock::WorkspaceExecutionLockManager,
};
use executors::TaskExecutor;
use workspace::RepoCacheLockManager;

#[async_trait]
pub trait HookAction: Send + Sync {
    async fn execute(&self, ctx: &HookContext) -> HookResult;
}

#[derive(Clone)]
pub struct HookContext {
    pub task_id: String,
    pub project_id: String,
    pub from_state: String,
    pub to_state: String,
    pub db: Arc<db::SqliteDb>,
    pub event_bus: Arc<events::EventBus>,
    pub gate_config: Option<GateConfig>,
    pub workflow: Arc<WorkflowDefinition>,
    pub triggered_by: Actor,
    pub review_runner: Option<Arc<review::ReviewRunner>>,
    pub merge_service: Option<Arc<MergeService>>,
    pub cleanup_scheduler: Option<Arc<WorkspaceCleanupScheduler>>,
    pub task_executor: Option<Arc<dyn TaskExecutor>>,
    pub daemon_connections: Option<Arc<crate::daemon_transport::DaemonConnectionRegistry>>,
    pub workspace_exec_locks: Option<Arc<WorkspaceExecutionLockManager>>,
    pub terminal_activity: Option<Arc<TerminalActivityTracker>>,
    pub workspace_root: PathBuf,
    pub repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
    pub workspace_id: Option<String>,
    pub agent_id: Option<String>,
    pub execution_id: Option<String>,
    pub state_config: Value,
}

#[derive(Debug, Clone)]
pub enum HookResult {
    Ok,
    Skipped { reason: String },
    Failed { reason: String },
    Cascade { to: String, reason: String },
}

pub mod actions;
pub mod default_autonomous_workflow;
pub mod default_roles;
pub mod default_states;
pub mod default_workflow;
pub mod dispatch;
pub mod engine;
pub mod inherited_subtask_workflow;
pub mod registry;
pub mod template_service;
pub mod transition_event;
pub mod validation;

pub use dispatch::{AgentDispatchContext, AgentPrompt, PromptBuilder};
pub use inherited_subtask_workflow::inherited_subtask_workflow;

pub fn effective_role(state: &StateDefinition) -> Option<&str> {
    if let Some(role) = state.role.as_deref() {
        return Some(role);
    }
    if state.kind == StateKind::Active {
        return Some(default_roles::ASSIGNEE);
    }
    None
}

#[cfg(test)]
mod tests {
    use api_types::{CanonicalPhase, StateDefinition, StateHooks, StateKind};
    use serde_json::json;

    use super::effective_role;

    fn state(kind: StateKind, role: Option<&str>) -> StateDefinition {
        StateDefinition {
            name: "state".to_owned(),
            kind,
            column: "state".to_owned(),
            display_name: "State".to_owned(),
            role: role.map(str::to_owned),
            hooks: StateHooks::default(),
            cleanup: None,
            canonical_phase: Some(match kind {
                StateKind::Backlog => CanonicalPhase::Backlog,
                StateKind::Initial => CanonicalPhase::Ready,
                StateKind::Active => CanonicalPhase::Working,
                StateKind::Gate => CanonicalPhase::Working,
                StateKind::Terminal => CanonicalPhase::Done,
                StateKind::Custom => CanonicalPhase::Working,
            }),
            gate_config: None,
            dispatch: None,
            triggers: std::collections::BTreeMap::new(),
            config: json!({}),
        }
    }

    #[test]
    fn effective_role_uses_assignee_for_active_without_role() {
        assert_eq!(
            effective_role(&state(StateKind::Active, None)),
            Some("assignee")
        );
    }

    #[test]
    fn effective_role_keeps_gate_without_role_empty() {
        assert_eq!(effective_role(&state(StateKind::Gate, None)), None);
    }

    #[test]
    fn effective_role_prefers_explicit_role_for_any_kind() {
        assert_eq!(
            effective_role(&state(StateKind::Gate, Some("coder"))),
            Some("coder")
        );
    }
}
