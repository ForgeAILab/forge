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
    /// Exact Project authority used to resolve this hook's workflow. Hook
    /// actions that perform nested Task transitions must carry it through to
    /// the final DB CAS instead of re-reading an unrelated newer workflow.
    pub project_version: Option<i64>,
    pub project_workflow_definition: Option<String>,
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

/// Marks a mechanical integration-contention bounce whose only purpose is to
/// obtain a fresh review. These transitions must never consume a merge-fix
/// budget or dispatch an implementation agent.
pub(crate) const REVIEW_REFRESH_MARKER: &str = "[review-refresh]";

/// Additional marker used to bound repeated automatic target rebases.
pub(crate) const TARGET_MOVED_MARKER: &str = "[target-moved-rebase]";

pub(crate) async fn review_refresh_transition_pending(
    db: &db::SqliteDb,
    task_id: &str,
    current_state: &str,
) -> db::Result<bool> {
    let latest = sqlx::query_as::<_, (String, String, String, String)>(
        "SELECT from_state, to_state, trigger_reason, triggered_by
         FROM transition_log
         WHERE task_id = ?
         ORDER BY created_at DESC, rowid DESC
         LIMIT 1",
    )
    .bind(task_id)
    .fetch_optional(db.pool())
    .await?;

    Ok(
        latest.is_some_and(|(from_state, to_state, reason, triggered_by)| {
            from_state == default_states::MERGING
                && to_state == current_state
                && reason.contains(REVIEW_REFRESH_MARKER)
                && triggered_by
                    == api_types::Actor::system(api_types::SystemComponent::Workflow).display()
        }),
    )
}

pub(crate) fn review_refresh_target(
    workflow: &WorkflowDefinition,
    current_state: &str,
) -> Option<String> {
    workflow
        .outgoing_trigger_targets(current_state)
        .find_map(|(_, target)| {
            workflow.states.iter().find_map(|state| {
                (state.name == target
                    && state.canonical_phase == Some(api_types::CanonicalPhase::Review))
                .then(|| state.name.clone())
            })
        })
}

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
