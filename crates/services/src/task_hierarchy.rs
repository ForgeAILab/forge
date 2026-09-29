//! Root-Task and ordered-subtask classification and policy.

use api_types::{CanonicalPhase, StateKind, WorkflowDefinition};
use db::{ProjectRepo, SqliteDb, Task, TaskRepo};
use sqlx::SqliteConnection;

use crate::{
    workflow::{default_states, effective_role, engine::WorkflowEngine},
    Result, ServiceError,
};

/// Workflow roles that may exist on a coordination root.
pub(crate) struct RootRolePolicy<'a> {
    workflow: &'a WorkflowDefinition,
    implementation_role: Option<&'a str>,
}

impl<'a> RootRolePolicy<'a> {
    /// Derive the root implementation and aggregate-review roles exactly once.
    pub(crate) fn for_workflow(workflow: &'a WorkflowDefinition) -> Self {
        let implementation_role = workflow
            .states
            .iter()
            .find(|state| state.name == default_states::IN_PROGRESS)
            .and_then(effective_role)
            .or_else(|| {
                workflow
                    .states
                    .iter()
                    .find(|state| state.kind == StateKind::Active)
                    .and_then(effective_role)
            });
        Self {
            workflow,
            implementation_role,
        }
    }

    /// Return the workflow's implementation role for root-policy decisions.
    pub(crate) fn implementation_role(&self) -> Option<&'a str> {
        self.implementation_role
    }

    /// Return whether `role` is declared by an aggregate review gate.
    pub(crate) fn is_aggregate_review_role(&self, role: &str) -> bool {
        self.workflow.states.iter().any(|state| {
            state.role.as_deref() == Some(role)
                && state.kind == StateKind::Gate
                && state.canonical_phase == Some(CanonicalPhase::Review)
        })
    }

    /// Return whether `role` may be assigned to a coordination root.
    pub(crate) fn allows_assignment(&self, role: &str) -> bool {
        self.implementation_role() != Some(role) && self.is_aggregate_review_role(role)
    }

    /// Return the only role that may execute on a coordination root in
    /// `state_name`: the state's effective role while it is the aggregate
    /// review phase, and none otherwise.
    pub(crate) fn execution_role_for_state(&self, state_name: &str) -> Option<&'a str> {
        if self.workflow.canonical_phase_for_state(state_name) != CanonicalPhase::Review {
            return None;
        }
        self.workflow
            .states
            .iter()
            .find(|state| state.name == state_name)
            .and_then(effective_role)
    }

    /// Return whether `role` may execute on a coordination root in `state_name`.
    pub(crate) fn allows_execution(&self, state_name: &str, role: &str) -> bool {
        self.execution_role_for_state(state_name) == Some(role)
    }
}

/// Error returned when a role may not be assigned to a coordination root.
pub(crate) fn root_assignment_denied() -> ServiceError {
    ServiceError::invalid_operation(
        "root tasks with subtasks are coordination containers; assign implementation agents to the subtasks",
    )
}

/// Return whether any root-blocking or recovery field is present.
pub(crate) fn root_blocked(task: &Task) -> bool {
    task.blocked_json.is_some()
        || task.failed_json.is_some()
        || task.error_annotation.is_some()
        || task.entry_barrier_json.is_some()
}

/// Return whether a child is terminal in either inherited or Project workflow.
pub(crate) fn subtask_is_terminal(task: &Task, project_workflow: &WorkflowDefinition) -> bool {
    let inherited = WorkflowEngine::resolve_subtask_workflow();
    inherited.state_kind(&task.status) == Some(StateKind::Terminal)
        || project_workflow.state_kind(&task.status) == Some(StateKind::Terminal)
}

/// Return the first ordered child that has not reached a terminal state.
pub(crate) fn next_incomplete_child<'a>(
    children: &'a [Task],
    workflow: &WorkflowDefinition,
) -> Option<&'a Task> {
    children
        .iter()
        .find(|child| !subtask_is_terminal(child, workflow))
}

/// Return every incomplete child ID in its existing order.
pub(crate) fn incomplete_child_ids(
    children: &[Task],
    workflow: &WorkflowDefinition,
) -> Vec<String> {
    children
        .iter()
        .filter(|child| !subtask_is_terminal(child, workflow))
        .map(|child| child.id.clone())
        .collect()
}

/// Return whether a non-empty ordered child sequence is complete.
pub(crate) fn child_sequence_complete(children: &[Task], workflow: &WorkflowDefinition) -> bool {
    !children.is_empty() && next_incomplete_child(children, workflow).is_none()
}

/// Select child IDs for a coordination root, or the Task's own ID otherwise.
pub(crate) fn child_ids_or_self(task: &Task, children: &[Task]) -> Vec<String> {
    if task.parent_task_id.is_none() && !children.is_empty() {
        children.iter().map(|child| child.id.clone()).collect()
    } else {
        vec![task.id.clone()]
    }
}

/// Load the visible direct children in their persisted execution order.
pub(crate) async fn ordered_children(db: &SqliteDb, parent_task_id: &str) -> Result<Vec<Task>> {
    Ok(TaskRepo::list_subtasks_ordered(db, parent_task_id).await?)
}

/// Load direct children only when the supplied Task is a root.
pub(crate) async fn children_if_root(db: &SqliteDb, task: &Task) -> Result<Vec<Task>> {
    if task.parent_task_id.is_some() {
        return Ok(Vec::new());
    }
    ordered_children(db, &task.id).await
}

/// Return whether the supplied visible Task is a root with visible children.
///
/// `TaskRepo::list_subtasks_ordered` excludes soft-deleted children. The
/// supplied root is trusted as the caller's current visible Task snapshot.
pub(crate) async fn coordination_root_has_subtasks(db: &SqliteDb, task: &Task) -> Result<bool> {
    if task.parent_task_id.is_some() {
        return Ok(false);
    }
    Ok(!ordered_children(db, &task.id).await?.is_empty())
}

/// Check coordination-root status inside an existing write transaction.
///
/// This preserves the transactional caller's SQL semantics: both root and
/// child must be non-deleted, the root must have no parent, and at least one
/// direct non-deleted child must exist. Like the pool-scoped check above,
/// soft-deleted children do not make a coordination root.
pub(crate) async fn is_coordination_root_tx(
    connection: &mut SqliteConnection,
    task_id: &str,
) -> Result<bool> {
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT EXISTS(
             SELECT 1
             FROM task AS root
             JOIN task AS child ON child.parent_task_id = root.id
             WHERE root.id = ?
               AND root.parent_task_id IS NULL
               AND root.deleted_at IS NULL
               AND child.deleted_at IS NULL
         )",
    )
    .bind(task_id)
    .fetch_one(connection)
    .await?
        != 0)
}

/// Return whether a child is currently first in the runnable ordered sequence.
pub(crate) async fn subtask_dispatch_ready(db: &SqliteDb, task: &Task) -> Result<bool> {
    let Some(parent_task_id) = task.parent_task_id.as_deref() else {
        return Ok(true);
    };
    let (parent, workflow) = coordination_root_context(db, parent_task_id).await?;
    if !coordination_root_allows_child_dispatch(&parent, &workflow) {
        return Ok(false);
    }
    let children = ordered_children(db, parent_task_id).await?;
    Ok(
        next_incomplete_child(&children, &workflow)
            .is_some_and(|candidate| candidate.id == task.id),
    )
}

/// Enforce ordered dispatch for a child while preserving caller error text.
pub(crate) async fn ensure_subtask_dispatch_order(db: &SqliteDb, task: &Task) -> Result<()> {
    let Some(parent_task_id) = task.parent_task_id.as_deref() else {
        return Ok(());
    };
    let (parent, workflow) = coordination_root_context(db, parent_task_id).await?;
    if !coordination_root_allows_child_dispatch(&parent, &workflow) {
        return Err(ServiceError::invalid_operation(format!(
            "subtask {} cannot start while coordination root {} is blocked, terminal, or in aggregate review",
            task.id, parent_task_id
        )));
    }
    let children = ordered_children(db, parent_task_id).await?;
    let next = next_incomplete_child(&children, &workflow);
    if next.is_some_and(|candidate| candidate.id == task.id) {
        return Ok(());
    }
    let waiting_on = next
        .map(|candidate| candidate.id.as_str())
        .unwrap_or("the completed sequence");
    Err(ServiceError::invalid_operation(format!(
        "subtask {} cannot start yet; ordered execution is waiting on {}",
        task.id, waiting_on
    )))
}

/// Load a coordination root and the Project workflow used for child policy.
pub(crate) async fn coordination_root_context(
    db: &SqliteDb,
    parent_task_id: &str,
) -> Result<(Task, WorkflowDefinition)> {
    let parent = TaskRepo::get_by_id(db, parent_task_id, false)
        .await?
        .ok_or_else(|| ServiceError::not_found("task", parent_task_id.to_owned()))?;
    let project = ProjectRepo::get_by_id(db, &parent.project_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("project", parent.project_id.clone()))?;
    let workflow = WorkflowEngine::resolve_workflow_for_task(
        &parent,
        &project.workflow_definition,
        &api_types::Actor::system(api_types::SystemComponent::TaskDispatcher),
    );
    Ok((parent, workflow))
}

/// Return whether a root state permits its next child to dispatch.
pub(crate) fn coordination_root_allows_child_dispatch(
    parent: &Task,
    workflow: &WorkflowDefinition,
) -> bool {
    if root_blocked(parent) {
        return false;
    }
    let Some(state) = workflow
        .states
        .iter()
        .find(|state| state.name == parent.status)
    else {
        return false;
    };
    if matches!(state.kind, StateKind::Backlog | StateKind::Terminal) {
        return false;
    }
    !(state.kind == StateKind::Gate && state.canonical_phase == Some(CanonicalPhase::Review))
}

/// Return whether a coordination root's non-empty child sequence is complete.
pub(crate) async fn coordination_root_sequence_complete(
    db: &SqliteDb,
    task: &Task,
    workflow: &WorkflowDefinition,
) -> Result<bool> {
    let children = children_if_root(db, task).await?;
    Ok(child_sequence_complete(&children, workflow))
}

/// Require every child to finish before aggregate review or terminal entry.
pub(crate) async fn ensure_coordination_root_target_ready(
    db: &SqliteDb,
    task: &Task,
    workflow: &WorkflowDefinition,
    target: &str,
) -> Result<()> {
    if task.parent_task_id.is_some()
        || workflow.cancellation_state.as_deref() == Some(target)
        || (!matches!(workflow.state_kind(target), Some(StateKind::Terminal))
            && workflow.canonical_phase_for_state(target) != CanonicalPhase::Review)
    {
        return Ok(());
    }
    let children = ordered_children(db, &task.id).await?;
    if let Some(incomplete) = next_incomplete_child(&children, workflow) {
        return Err(ServiceError::invalid_operation(format!(
            "coordination root {} cannot enter aggregate review or a terminal state while subtask {} is incomplete",
            task.id, incomplete.id
        )));
    }
    Ok(())
}

/// Require a coordination root to be free of blocking and recovery fields.
pub(crate) fn ensure_coordination_root_unblocked(parent: &Task) -> Result<()> {
    if root_blocked(parent) {
        return Err(ServiceError::invalid_operation(format!(
            "coordination root {} cannot enter aggregate review while blocked or awaiting recovery",
            parent.id
        )));
    }
    Ok(())
}

/// Return whether the Task identified by `task_id` has no parent.
pub async fn is_root_task(db: &SqliteDb, task_id: &str) -> Result<bool> {
    let task = TaskRepo::get_by_id(db, task_id, false)
        .await?
        .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
    Ok(task.parent_task_id.is_none())
}

/// Return whether the Task identified by `task_id` has a parent.
pub async fn is_subtask(db: &SqliteDb, task_id: &str) -> Result<bool> {
    Ok(!is_root_task(db, task_id).await?)
}

/// Resolve a Task to itself when root, or to its direct parent when a child.
pub async fn root_for(db: &SqliteDb, task_id: &str) -> Result<Task> {
    let task = TaskRepo::get_by_id(db, task_id, false)
        .await?
        .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
    let Some(parent_task_id) = task.parent_task_id.as_deref() else {
        return Ok(task);
    };
    TaskRepo::get_by_id(db, parent_task_id, false)
        .await?
        .ok_or_else(|| ServiceError::not_found("task", parent_task_id.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(id: &str, status: &str, parent_task_id: Option<&str>) -> Task {
        Task {
            id: id.to_owned(),
            project_id: "project".to_owned(),
            parent_task_id: parent_task_id.map(str::to_owned),
            assignee_type: None,
            assignee_id: None,
            title: id.to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: status.to_owned(),
            is_automation: false,
            priority: 0,
            board_position: 0.0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            metadata_json: None,
            plan: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            entry_barrier_json: None,
            review_passed_at: None,
            archived_at: None,
            deleted_at: None,
            version: 0,
            created_at: "2026-09-28T00:00:00Z".to_owned(),
            updated_at: "2026-09-28T00:00:00Z".to_owned(),
        }
    }

    #[test]
    fn task_hierarchy_selects_the_first_incomplete_child() {
        let workflow = crate::workflow::default_workflow::default_workflow();
        let children = vec![
            task("done", default_states::DONE, Some("root")),
            task("next", default_states::TODO, Some("root")),
            task("later", default_states::TODO, Some("root")),
        ];

        assert_eq!(
            next_incomplete_child(&children, &workflow).map(|child| child.id.as_str()),
            Some("next")
        );
        assert_eq!(
            incomplete_child_ids(&children, &workflow),
            vec!["next".to_owned(), "later".to_owned()]
        );
        assert!(!child_sequence_complete(&children, &workflow));
        assert!(!child_sequence_complete(&[], &workflow));
    }

    #[test]
    fn task_hierarchy_treats_inherited_terminal_children_as_complete() {
        let workflow = crate::workflow::default_workflow::default_workflow();
        let children = vec![
            task("done", default_states::DONE, Some("root")),
            task("cancelled", default_states::CANCELLED, Some("root")),
        ];

        assert!(child_sequence_complete(&children, &workflow));
        assert!(children
            .iter()
            .all(|child| subtask_is_terminal(child, &workflow)));
    }

    #[test]
    fn task_hierarchy_child_ids_fall_back_to_self() {
        let root = task("root", default_states::TODO, None);
        let child = task("child", default_states::TODO, Some("root"));
        let children = vec![task("other-child", default_states::TODO, Some("root"))];

        assert_eq!(child_ids_or_self(&root, &children), vec!["other-child"]);
        assert_eq!(child_ids_or_self(&root, &[]), vec!["root"]);
        assert_eq!(child_ids_or_self(&child, &children), vec!["child"]);
    }

    #[test]
    fn task_hierarchy_root_blocked_covers_all_recovery_fields() {
        let mut root = task("root", default_states::IN_PROGRESS, None);
        assert!(!root_blocked(&root));

        root.blocked_json = Some("{}".to_owned());
        assert!(root_blocked(&root));
        root.blocked_json = None;
        root.failed_json = Some("{}".to_owned());
        assert!(root_blocked(&root));
        root.failed_json = None;
        root.error_annotation = Some("{}".to_owned());
        assert!(root_blocked(&root));
        root.error_annotation = None;
        root.entry_barrier_json = Some("{}".to_owned());
        assert!(root_blocked(&root));
    }

    #[test]
    fn task_hierarchy_root_role_policy_allows_only_aggregate_review() {
        let mut workflow = crate::workflow::default_workflow::default_workflow();
        let policy = RootRolePolicy::for_workflow(&workflow);

        assert_eq!(policy.implementation_role(), Some("coder"));
        assert!(policy.is_aggregate_review_role("reviewer"));
        assert!(policy.allows_assignment("reviewer"));
        assert!(!policy.allows_assignment("coder"));
        assert!(!policy.allows_assignment("planner"));

        workflow
            .states
            .iter_mut()
            .find(|state| state.name == default_states::REVIEW)
            .expect("default review state exists")
            .role = Some("coder".to_owned());
        let policy = RootRolePolicy::for_workflow(&workflow);
        assert!(policy.is_aggregate_review_role("coder"));
        assert!(!policy.allows_assignment("coder"));
    }

    #[test]
    fn task_hierarchy_root_execution_follows_the_review_state_role() {
        let mut workflow = crate::workflow::default_workflow::default_workflow();
        let policy = RootRolePolicy::for_workflow(&workflow);

        assert!(policy.allows_execution(default_states::REVIEW, "reviewer"));
        assert!(!policy.allows_execution(default_states::REVIEW, "coder"));
        assert!(!policy.allows_execution(default_states::IN_PROGRESS, "coder"));
        assert!(!policy.allows_execution(default_states::IN_PROGRESS, "reviewer"));

        // A custom workflow that renames the aggregate reviewer must keep
        // assignment and execution in agreement, not fall back to "reviewer".
        workflow
            .states
            .iter_mut()
            .find(|state| state.name == default_states::REVIEW)
            .expect("default review state exists")
            .role = Some("auditor".to_owned());
        let policy = RootRolePolicy::for_workflow(&workflow);
        assert!(policy.allows_assignment("auditor"));
        assert!(policy.allows_execution(default_states::REVIEW, "auditor"));
        assert!(!policy.allows_assignment("reviewer"));
        assert!(!policy.allows_execution(default_states::REVIEW, "reviewer"));
    }

    #[test]
    fn task_hierarchy_child_dispatch_requires_a_live_non_review_root() {
        let workflow = crate::workflow::default_workflow::default_workflow();
        let mut root = task("root", default_states::IN_PROGRESS, None);
        assert!(coordination_root_allows_child_dispatch(&root, &workflow));

        root.status = default_states::REVIEW.to_owned();
        assert!(!coordination_root_allows_child_dispatch(&root, &workflow));
        root.status = default_states::DONE.to_owned();
        assert!(!coordination_root_allows_child_dispatch(&root, &workflow));
        root.status = "unknown".to_owned();
        assert!(!coordination_root_allows_child_dispatch(&root, &workflow));
        root.status = default_states::IN_PROGRESS.to_owned();
        root.error_annotation = Some("{}".to_owned());
        assert!(!coordination_root_allows_child_dispatch(&root, &workflow));
    }
}
