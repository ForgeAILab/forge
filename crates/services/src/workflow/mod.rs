use std::{path::PathBuf, sync::Arc};

use api_types::{Actor, GateConfig, StateDefinition, StateKind, WorkflowDefinition};
use async_trait::async_trait;
use serde_json::Value;

use crate::{
    merge_service::MergeService, terminal_service::TerminalActivityTracker,
    workspace_cleanup::WorkspaceCleanupScheduler,
    workspace_execution_lock::WorkspaceExecutionLockManager,
};
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
    /// Originating service clone for nested dispatch; preserves the provider,
    /// outbox and workspace dependencies of the dispatch authority.
    pub task_service: crate::TaskService,
    pub daemon_connections: Option<Arc<crate::daemon_transport::DaemonConnectionRegistry>>,
    pub workspace_exec_locks: Option<Arc<WorkspaceExecutionLockManager>>,
    pub terminal_activity: Option<Arc<TerminalActivityTracker>>,
    pub workspace_root: PathBuf,
    pub repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
    pub workspace_backend_router: Arc<crate::workspace_backend::WorkspaceBackendRouter>,
    pub workspace_id: Option<String>,
    pub agent_id: Option<String>,
    pub execution_id: Option<String>,
    pub state_config: Value,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum HookResult {
    Ok,
    Skipped {
        reason: String,
    },
    Failed {
        reason: String,
    },
    Cascade {
        to: String,
        reason: String,
        #[serde(flatten)]
        bridge: api_types::TransitionBridge,
    },
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

/// Paths recorded in typed conflict handoffs in the current retry window.
pub(crate) fn handed_off_conflict_paths(entries: &[db::TransitionLog]) -> Vec<String> {
    let workflow_actor = api_types::Actor::system(api_types::SystemComponent::Workflow).display();
    let mut paths = Vec::new();
    for entry in crate::task_diagnostics::entries_since_retry_window_boundary(entries, None) {
        if entry.from_state != default_states::MERGING
            || entry.to_state != default_states::MERGE_FAILED
            || entry.triggered_by != workflow_actor
            || entry.bridge.bridge_kind != Some(api_types::TransitionBridgeKind::ConflictHandoff)
        {
            continue;
        }
        if let Some(handoff_paths) = entry.bridge.conflict_paths() {
            for path in handoff_paths {
                if !paths.contains(&path) {
                    paths.push(path);
                }
            }
        }
    }
    paths
}

/// Why the Task is entering `review`, when the reason is a purely mechanical
/// integration step whose predecessor's review may still apply.
///
/// The transition log must end with `merging -> merge_failed` bridge followed
/// directly by the current `merge_failed -> review` entry, so a failed check
/// bounce, a user move, or any other intervening transition disqualifies the
/// entry. Only Forge's own bridges qualify: a clean rebase classified as TargetMovedRebase followed by ReviewRefresh,
/// or a ConflictHandoff.
pub(crate) fn review_carry_entry_kind(
    entries: &[db::TransitionLog],
) -> Option<db::ReviewCarryKind> {
    let workflow_actor = api_types::Actor::system(api_types::SystemComponent::Workflow).display();
    let [.., bridge, current] = entries else {
        return None;
    };
    if current.from_state != default_states::MERGE_FAILED
        || current.to_state != default_states::REVIEW
        || current.rejection
        || bridge.from_state != default_states::MERGING
        || bridge.to_state != default_states::MERGE_FAILED
        || bridge.triggered_by != workflow_actor
    {
        return None;
    }
    if bridge.bridge.bridge_kind == Some(api_types::TransitionBridgeKind::ConflictHandoff) {
        Some(db::ReviewCarryKind::ConflictRepair)
    } else if bridge.bridge.bridge_kind == Some(api_types::TransitionBridgeKind::TargetMovedRebase)
        && current.bridge.is_review_refresh()
    {
        Some(db::ReviewCarryKind::CleanRebase)
    } else {
        None
    }
}

pub(crate) async fn review_refresh_transition_pending(
    db: &db::SqliteDb,
    task_id: &str,
    current_state: &str,
) -> db::Result<bool> {
    let latest = sqlx::query_as::<
        _,
        (
            String,
            String,
            String,
            Option<String>,
            Option<String>,
            String,
        ),
    >(
        "SELECT id, from_state, to_state, bridge_kind, bridge_payload, triggered_by
         FROM transition_log
         WHERE task_id = ?
         ORDER BY created_at DESC, rowid DESC
         LIMIT 1",
    )
    .bind(task_id)
    .fetch_optional(db.pool())
    .await?;
    let Some((id, from_state, to_state, kind, payload, triggered_by)) = latest else {
        return Ok(false);
    };
    let bridge = db::decode_transition_bridge(&id, kind.as_deref(), payload.as_deref())?;
    Ok(from_state == default_states::MERGING
        && to_state == current_state
        && bridge.is_review_refresh()
        && triggered_by == api_types::Actor::system(api_types::SystemComponent::Workflow).display())
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

    use super::{
        effective_role, handed_off_conflict_paths, review_carry_entry_kind,
        review_refresh_transition_pending,
    };

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
    fn bridge_log(from: &str, to: &str, bridge: api_types::TransitionBridge) -> db::TransitionLog {
        db::TransitionLog {
            id: db::new_uuid_v4(),
            task_id: "task".into(),
            from_state: from.into(),
            to_state: to.into(),
            trigger_name: None,
            triggered_by: api_types::Actor::system(api_types::SystemComponent::Workflow).display(),
            bridge,
            trigger_reason: "Human text is independent of classification".into(),
            hook_results_json: None,
            rejection: false,
            created_at: db::now_rfc3339(),
        }
    }

    #[test]
    fn bridge_readers_ignore_prose_and_use_only_typed_history() {
        use api_types::{TransitionBridge as Bridge, TransitionBridgeKind as Kind};
        let paths = vec!["src/a,b.rs".into(), "日本語.rs".into()];
        let handoff = bridge_log("merging", "merge_failed", Bridge::conflict_handoff(&paths));
        assert_eq!(
            handed_off_conflict_paths(std::slice::from_ref(&handoff)),
            paths
        );
        let current = bridge_log("merge_failed", "review", Bridge::new(Kind::ReviewRefresh));
        assert_eq!(
            review_carry_entry_kind(&[handoff.clone(), current.clone()]),
            Some(db::ReviewCarryKind::ConflictRepair)
        );
        let rebase = bridge_log(
            "merging",
            "merge_failed",
            Bridge::new(Kind::TargetMovedRebase),
        );
        assert_eq!(
            review_carry_entry_kind(&[rebase.clone(), current.clone()]),
            Some(db::ReviewCarryKind::CleanRebase)
        );
        let mut ordinary = handoff;
        ordinary.bridge = Bridge::default();
        ordinary.trigger_reason =
            "[conflict-handoff] [review-refresh] [target-moved-rebase]; paths_json=[\"fake.rs\"]"
                .into();
        assert!(handed_off_conflict_paths(&[ordinary.clone()]).is_empty());
        assert_eq!(review_carry_entry_kind(&[ordinary, current]), None);
        let mut rejected = rebase;
        rejected.rejection = true;
        assert_eq!(
            crate::task_diagnostics::audit_gate_rejections_since_boundary(&[rejected], "merging"),
            0
        );
    }

    #[tokio::test]
    async fn pending_refresh_reads_typed_latest_row_even_with_unrelated_reason() {
        use db::TransitionLogRepo;
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = db::SqliteDb::new(pool);
        sqlx::query(
            "INSERT INTO project(id,name,created_at,updated_at) VALUES('p','Project','now','now')",
        )
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('task','p','Task','merge_failed','now','now')").execute(db.pool()).await.unwrap();
        let log = bridge_log(
            "merging",
            "merge_failed",
            api_types::TransitionBridge::new(api_types::TransitionBridgeKind::TargetMovedRebase),
        );
        TransitionLogRepo::insert(
            &db,
            db::CreateTransitionLog {
                id: log.id,
                task_id: log.task_id,
                from_state: log.from_state,
                to_state: log.to_state,
                trigger_name: log.trigger_name,
                triggered_by: log.triggered_by,
                bridge: log.bridge,
                trigger_reason: log.trigger_reason,
                hook_results_json: None,
                rejection: false,
                created_at: log.created_at,
            },
        )
        .await
        .unwrap();
        assert!(
            review_refresh_transition_pending(&db, "task", "merge_failed")
                .await
                .unwrap()
        );
        sqlx::query("UPDATE transition_log SET bridge_kind=NULL,trigger_reason='[review-refresh]' WHERE task_id='task'").execute(db.pool()).await.unwrap();
        assert!(
            !review_refresh_transition_pending(&db, "task", "merge_failed")
                .await
                .unwrap()
        );
        // The column is open; a kind this build does not know is a typed
        // error, never "not a refresh" and never a panic.
        sqlx::query("UPDATE transition_log SET bridge_kind='future_kind' WHERE task_id='task'")
            .execute(db.pool())
            .await
            .unwrap();
        assert!(matches!(
            review_refresh_transition_pending(&db, "task", "merge_failed").await,
            Err(db::DbError::TransitionBridgeCorrupt { .. })
        ));
    }
}
