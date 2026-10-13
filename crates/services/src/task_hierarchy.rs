//! Root-Task and ordered-subtask classification and policy.

#[cfg(test)]
use std::collections::HashSet;

use api_types::{
    CanonicalPhase, EffectiveCoderSource, StateDefinition, StateKind, WorkflowDefinition,
};
use db::{ProjectRepo, SqliteDb, Task, TaskRepo, TaskRoleAssignment, TaskRoleAssignmentRepo};
use sqlx::SqliteConnection;

use crate::{
    workflow::{default_states, effective_role, engine::WorkflowEngine},
    Result, ServiceError,
};

/// The workflow's planning state: the first gate with a role, outside the
/// review phase, on the path a Task takes from an initial state to the first
/// implementation (`Active`) state. A coordination root runs its lead there.
pub(crate) fn planning_state(workflow: &WorkflowDefinition) -> Option<&StateDefinition> {
    for initial in workflow
        .states
        .iter()
        .filter(|state| state.kind == StateKind::Initial)
    {
        let mut cursor = initial.name.as_str();
        for _ in 0..workflow.states.len() {
            let Some(next) = workflow
                .outgoing_trigger_targets(cursor)
                .filter(|(trigger, _)| !trigger.system_only())
                .find_map(|(_, target)| {
                    workflow.states.iter().find(|state| {
                        state.name == target
                            && matches!(state.kind, StateKind::Active | StateKind::Gate)
                    })
                })
            else {
                break;
            };
            if next.kind == StateKind::Active {
                break;
            }
            if next.canonical_phase != Some(CanonicalPhase::Review)
                && effective_role(next).is_some()
            {
                return Some(next);
            }
            cursor = next.name.as_str();
        }
    }
    None
}

/// Whether the native host can run `role` as a root's lead. It maps Task
/// roles by literal name, so v1 supports only the built-in planner.
pub(crate) fn lead_role_supported(role: &str) -> bool {
    role == crate::workflow::default_roles::PLANNER
}

/// Workflow roles that may exist on a coordination root.
pub(crate) struct RootRolePolicy<'a> {
    workflow: &'a WorkflowDefinition,
    implementation_role: Option<&'a str>,
    planning: Option<&'a StateDefinition>,
}

impl<'a> RootRolePolicy<'a> {
    /// Derive the root implementation, planning and aggregate-review roles
    /// exactly once.
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
        // The implementation role is never executed on a root, so a planning
        // gate that names it has no lead.
        let planning = planning_state(workflow).filter(|state| {
            effective_role(state).is_some_and(|role| {
                role != crate::workflow::default_roles::CODER && implementation_role != Some(role)
            })
        });
        Self {
            workflow,
            implementation_role,
            planning,
        }
    }

    /// Return the workflow's implementation role for root-policy decisions.
    pub(crate) fn implementation_role(&self) -> Option<&'a str> {
        self.implementation_role
    }

    /// The state in which a coordination root runs its lead.
    pub(crate) fn planning_state(&self) -> Option<&'a str> {
        self.planning.map(|state| state.name.as_str())
    }

    /// The role a coordination root's lead holds: the planning state's role.
    pub(crate) fn planning_role(&self) -> Option<&'a str> {
        self.planning.and_then(effective_role)
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
        role == crate::workflow::default_roles::CODER
            || self.planning_role() == Some(role)
            || (self.implementation_role() != Some(role) && self.is_aggregate_review_role(role))
    }

    /// Return the only role that may execute on a coordination root in
    /// `state_name`: the lead in the planning state, the state's effective
    /// role while it is the aggregate review phase, and none otherwise.
    pub(crate) fn execution_role_for_state(&self, state_name: &str) -> Option<&'a str> {
        if self.planning_state() == Some(state_name) {
            return self.planning_role();
        }
        if self.workflow.canonical_phase_for_state(state_name) != CanonicalPhase::Review {
            return None;
        }
        self.workflow
            .states
            .iter()
            .find(|state| state.name == state_name)
            .and_then(effective_role)
    }

    /// Return whether `role` may execute on a coordination root in
    /// `state_name` by the workflow alone. Readers ask [`Self::runs_role`],
    /// which also requires the lead in the planning state.
    fn allows_execution(&self, state_name: &str, role: &str) -> bool {
        role != crate::workflow::default_roles::CODER
            && self.implementation_role() != Some(role)
            && self.execution_role_for_state(state_name) == Some(role)
    }

    /// Whether `role` runs on coordination root `root_id` in `state_name`
    /// with the rows it has now: [`Self::allows_execution`], and in the
    /// planning state only while a lead is assigned. A planning root whose
    /// lead was removed runs nothing; the root advance moves it on.
    pub(crate) fn runs_role(
        &self,
        root_id: &str,
        state_name: &str,
        role: &str,
        assignments: &[TaskRoleAssignment],
    ) -> bool {
        self.allows_execution(state_name, role)
            && (self.planning_state() != Some(state_name)
                || self.lead(root_id, assignments).is_some())
    }

    /// The lead of a coordination root: the Agent assigned to the planning
    /// role on the root's own row. THE resolver for "does this root have a
    /// lead"; every reader goes through it (or its database form,
    /// [`root_lead`]), over the rows it already loaded.
    pub(crate) fn lead<'r>(
        &self,
        root_id: &str,
        assignments: &'r [TaskRoleAssignment],
    ) -> Option<&'r TaskRoleAssignment> {
        let role = self.planning_role()?;
        assignments.iter().find(|assignment| {
            assignment.task_id == root_id
                && assignment.role_name == role
                && assignment.assignee_type == Some(db::AssigneeKind::Agent)
                && assignment.assignee_id.is_some()
        })
    }

    /// Whether a coordination root rests in `state_name` for its lead: the
    /// planning state, with a lead assigned. The root advance never moves a
    /// root out of such a state; the lead's execution does.
    pub(crate) fn lead_holds_state(
        &self,
        root_id: &str,
        state_name: &str,
        assignments: &[TaskRoleAssignment],
    ) -> bool {
        self.planning_state() == Some(state_name) && self.lead(root_id, assignments).is_some()
    }

    /// Whether a planning-role execution is running on a root that sits in
    /// its planning state. The root rests there until that run ends, lead
    /// assigned or not: its settlement moves the root on.
    pub(crate) fn planning_run_live<'e>(
        &self,
        root_status: &str,
        running_roles: impl IntoIterator<Item = &'e str>,
    ) -> bool {
        self.planning_state() == Some(root_status)
            && self
                .planning_role()
                .is_some_and(|role| running_roles.into_iter().any(|running| running == role))
    }

    /// Whether the level-triggered root advance has work for this root:
    /// kickoff (a root that has not reached implementation and has a subtask
    /// left to run), or completion (every subtask is terminal and the root
    /// has not reached aggregate review, or an advance is flagged pending).
    /// Never while the lead holds the root's state, whatever flag an earlier
    /// advance left behind, and never while a planning run is live on the
    /// root (`planning_run_live`, see [`Self::planning_run_live`]): a lead
    /// removed mid-run must not release the subtasks beside its planner.
    pub(crate) fn root_advance_due(
        &self,
        root: &Task,
        children: &[Task],
        assignments: &[TaskRoleAssignment],
        review_pending: bool,
        planning_run_live: bool,
    ) -> bool {
        if children.is_empty()
            || planning_run_live
            || self.lead_holds_state(&root.id, &root.status, assignments)
        {
            return false;
        }
        let Some(state) = self
            .workflow
            .states
            .iter()
            .find(|state| state.name == root.status)
        else {
            return false;
        };
        let review_phase = state.canonical_phase == Some(CanonicalPhase::Review);
        match state.kind {
            StateKind::Initial | StateKind::Gate | StateKind::Active => {}
            _ => return false,
        }
        if child_sequence_complete(children, self.workflow) {
            return review_pending || !review_phase;
        }
        // Kickoff: only from a state before implementation. A completion
        // flag left on a root that has since been given another subtask
        // describes nothing: that subtask runs first.
        state.kind != StateKind::Active && !review_phase
    }

    /// The next state of a root advance out of `from`, or none when the
    /// workflow has no forward route. A root with a lead enters the planning
    /// state when it is next; otherwise the implementation state is preferred
    /// over a gate, as before.
    pub(crate) fn advance_target(&self, from: &str, lead_assigned: bool) -> Option<String> {
        let candidates: Vec<&StateDefinition> = self
            .workflow
            .outgoing_trigger_targets(from)
            .filter_map(|(_, target)| {
                self.workflow
                    .states
                    .iter()
                    .find(|state| state.name == target)
                    .filter(|state| {
                        !matches!(state.kind, StateKind::Backlog | StateKind::Terminal)
                            && state.canonical_phase != Some(CanonicalPhase::Review)
                    })
            })
            .collect();
        if lead_assigned {
            if let Some(planning) = candidates
                .iter()
                .find(|state| self.planning_state() == Some(state.name.as_str()))
            {
                return Some(planning.name.clone());
            }
        }
        candidates
            .into_iter()
            .max_by_key(|state| state.kind == StateKind::Active)
            .map(|state| state.name.clone())
    }
}

/// Database form of [`RootRolePolicy::planning_run_live`].
pub(crate) async fn root_planning_run_live(
    db: &SqliteDb,
    root: &Task,
    policy: &RootRolePolicy<'_>,
) -> Result<bool> {
    if policy.planning_state() != Some(root.status.as_str()) {
        return Ok(false);
    }
    let running = db::ExecutionRepo::list_running_by_task(db, &root.id).await?;
    Ok(policy.planning_run_live(
        &root.status,
        running.iter().map(|execution| execution.role.as_str()),
    ))
}

/// Database form of [`RootRolePolicy::runs_role`] for `task` in its present
/// state: true for anything that is not a coordination root, and for a root
/// only when `role` runs on it now (its lead in the planning state, its
/// aggregate review role in review).
pub(crate) async fn root_runs_role(
    db: &SqliteDb,
    task: &Task,
    workflow: &WorkflowDefinition,
    role: &str,
) -> Result<bool> {
    if !coordination_root_has_subtasks(db, task).await? {
        return Ok(true);
    }
    let assignments = TaskRoleAssignmentRepo::list_by_task(db, &task.id).await?;
    Ok(
        RootRolePolicy::for_workflow(workflow).runs_role(
            &task.id,
            &task.status,
            role,
            &assignments,
        ),
    )
}

/// Database form of [`RootRolePolicy::lead`] for a coordination root.
pub async fn root_lead(
    db: &SqliteDb,
    root: &Task,
    workflow: &WorkflowDefinition,
) -> Result<Option<TaskRoleAssignment>> {
    if root.parent_task_id.is_some() {
        return Ok(None);
    }
    let policy = RootRolePolicy::for_workflow(workflow);
    let Some(role) = policy.planning_role() else {
        return Ok(None);
    };
    let row = TaskRoleAssignmentRepo::get_by_task_and_role(db, &root.id, role).await?;
    Ok(row.filter(|row| policy.lead(&root.id, std::slice::from_ref(row)).is_some()))
}

/// The lead a Task response reports: a coordination root's own lead, and for
/// a subtask its root's. `own_rows` and `task_workflow` are what the caller
/// already loaded for `task`; nothing more is read unless a lead may exist.
pub async fn effective_lead(
    db: &SqliteDb,
    task: &Task,
    own_rows: &[TaskRoleAssignment],
    task_workflow: &WorkflowDefinition,
) -> Result<Option<EffectiveCoderAssignment>> {
    let Some(parent_task_id) = task.parent_task_id.as_deref() else {
        // A root resolves under the Project workflow, which is its own.
        let policy = RootRolePolicy::for_workflow(task_workflow);
        let Some(lead) = policy.lead(&task.id, own_rows) else {
            return Ok(None);
        };
        if !coordination_root_has_subtasks(db, task).await? {
            return Ok(None);
        }
        return Ok(Some(EffectiveCoderAssignment {
            assignment: lead.clone(),
            source: EffectiveCoderSource::Own,
        }));
    };
    let root_rows = TaskRoleAssignmentRepo::list_by_task(db, parent_task_id).await?;
    if !root_rows.iter().any(|row| {
        row.role_name != crate::workflow::default_roles::CODER && row.assignee_id.is_some()
    }) {
        return Ok(None);
    }
    let (_, workflow) = coordination_root_context(db, parent_task_id).await?;
    Ok(RootRolePolicy::for_workflow(&workflow)
        .lead(parent_task_id, &root_rows)
        .map(|lead| EffectiveCoderAssignment {
            assignment: lead.clone(),
            source: EffectiveCoderSource::InheritedFromRoot,
        }))
}

/// Admit an assignee for the lead (the planning role) of coordination root
/// `task_id`. `backend_kind` is the assigned Agent's, and `None` for a
/// person. An empty assignment is always admitted: it removes the lead.
pub(crate) fn ensure_lead_assignable(
    task_id: &str,
    role: &str,
    assignee_type: Option<&db::AssigneeKind>,
    backend_kind: Option<&str>,
) -> Result<()> {
    if assignee_type.is_none() {
        return Ok(());
    }
    if !lead_role_supported(role) {
        return Err(ServiceError::LeadRoleNotSupported {
            task_id: task_id.to_owned(),
            role: role.to_owned(),
        });
    }
    if assignee_type != Some(&db::AssigneeKind::Agent) || backend_kind != Some("native") {
        return Err(ServiceError::LeadRequiresNativeProfile {
            task_id: task_id.to_owned(),
            role: role.to_owned(),
        });
    }
    Ok(())
}

/// Whether an existing assignment row stays on a Task that becomes a
/// coordination root: the roles the policy admits, and the planning role only
/// with an assignee that may lead (conversion never fails; it strips).
pub(crate) fn root_keeps_assignment(
    policy: &RootRolePolicy<'_>,
    task_id: &str,
    role: &str,
    assignee_type: Option<&db::AssigneeKind>,
    backend_kind: Option<&str>,
) -> bool {
    policy.allows_assignment(role)
        && (policy.planning_role() != Some(role)
            || ensure_lead_assignable(task_id, role, assignee_type, backend_kind).is_ok())
}

/// Error returned when a role may not be assigned to a coordination root.
pub(crate) fn root_assignment_denied() -> ServiceError {
    ServiceError::invalid_operation(
        "coordination roots accept only the coder default worker, the planning role (the lead) and aggregate review roles",
    )
}

/// A Task's resolved coder assignment and the row that supplied it.
#[derive(Debug, Clone)]
pub struct EffectiveCoderAssignment {
    pub assignment: TaskRoleAssignment,
    pub source: EffectiveCoderSource,
}

/// Resolve coder authority without materializing inherited rows on children.
///
/// A child's own row wins even when its assignee is empty. Only the absence of
/// an own row falls back to the direct coordination root.
pub async fn effective_coder_assignment(
    db: &SqliteDb,
    task: &Task,
) -> Result<Option<EffectiveCoderAssignment>> {
    if let Some(assignment) = TaskRoleAssignmentRepo::get_by_task_and_role(
        db,
        &task.id,
        crate::workflow::default_roles::CODER,
    )
    .await?
    {
        return Ok(Some(EffectiveCoderAssignment {
            assignment,
            source: EffectiveCoderSource::Own,
        }));
    }

    let Some(parent_task_id) = task.parent_task_id.as_deref() else {
        return Ok(None);
    };
    Ok(TaskRoleAssignmentRepo::get_by_task_and_role(
        db,
        parent_task_id,
        crate::workflow::default_roles::CODER,
    )
    .await?
    .map(|assignment| EffectiveCoderAssignment {
        assignment,
        source: EffectiveCoderSource::InheritedFromRoot,
    }))
}

/// The root whose lead holds `role` on `task`: `task` is a subtask governed
/// by the inherited subtask workflow and `role` is that workflow's step-check
/// role. Such a role is never read from the subtask's own rows: with no lead
/// on the root nobody holds it, whatever row the subtask carries.
pub(crate) fn step_check_root<'t>(task: &'t Task, role: &str) -> Option<&'t str> {
    let parent_task_id = task.parent_task_id.as_deref()?;
    (role == crate::workflow::inherited_subtask_workflow::STEP_CHECK_ROLE
        && crate::workflow::inherited_subtask_workflow::is_inherited_state(&task.status))
    .then_some(parent_task_id)
}

/// In-memory form of the step-check resolution: the lead of `root_id` among
/// `rows` (which hold the root's rows), when the Project workflow's planning
/// role is the step-check role.
pub(crate) fn step_check_lead<'r>(
    project_workflow: &WorkflowDefinition,
    root_id: &str,
    rows: &'r [TaskRoleAssignment],
) -> Option<&'r TaskRoleAssignment> {
    let policy = RootRolePolicy::for_workflow(project_workflow);
    (policy.planning_role() == Some(crate::workflow::inherited_subtask_workflow::STEP_CHECK_ROLE))
        .then(|| policy.lead(root_id, rows))
        .flatten()
}

/// Database form of [`step_check_lead`] for the root `root_id`.
async fn step_check_assignment(
    db: &SqliteDb,
    root_id: &str,
) -> Result<Option<EffectiveCoderAssignment>> {
    let rows = TaskRoleAssignmentRepo::list_by_task(db, root_id).await?;
    if !rows.iter().any(|row| {
        row.role_name == crate::workflow::inherited_subtask_workflow::STEP_CHECK_ROLE
            && row.assignee_id.is_some()
    }) {
        return Ok(None);
    }
    let (_, workflow) = coordination_root_context(db, root_id).await?;
    Ok(
        step_check_lead(&workflow, root_id, &rows).map(|lead| EffectiveCoderAssignment {
            assignment: lead.clone(),
            source: EffectiveCoderSource::InheritedFromRoot,
        }),
    )
}

/// Resolve an execution role. THE resolver for "who holds this role on this
/// Task": `coder` falls back from the subtask's row to its root's (the default
/// worker), a subtask's step-check role is its root's lead and nothing else
/// ([`step_check_root`]), and every other role is the Task's own row.
pub async fn effective_role_assignment(
    db: &SqliteDb,
    task: &Task,
    role: &str,
) -> Result<Option<EffectiveCoderAssignment>> {
    let canonical_role = if role == "executor" {
        crate::workflow::default_roles::CODER
    } else {
        role
    };
    if canonical_role == crate::workflow::default_roles::CODER {
        return effective_coder_assignment(db, task).await;
    }
    if let Some(root_id) = step_check_root(task, canonical_role) {
        return step_check_assignment(db, root_id).await;
    }
    Ok(
        TaskRoleAssignmentRepo::get_by_task_and_role(db, &task.id, canonical_role)
            .await?
            .map(|assignment| EffectiveCoderAssignment {
                assignment,
                source: EffectiveCoderSource::Own,
            }),
    )
}

/// Return whether any root-blocking or recovery field is present.
pub(crate) fn root_blocked(task: &Task) -> bool {
    task.condition.read().interruption_present
        || task.condition.read().hard_failure
        || task.condition.read().diagnostic_present
        || task.condition.read().entry_recorded
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

/// Project-scoped batch form of `coordination_root_has_subtasks`, with the
/// same visible-child semantics, for slot counting without a query per root.
#[cfg(test)]
pub(crate) async fn coordination_root_ids_with_subtasks(
    db: &SqliteDb,
    project_id: &str,
) -> Result<HashSet<String>> {
    Ok(sqlx::query_scalar::<_, String>(
        "SELECT root.id FROM task AS root
         WHERE root.project_id = ? AND root.parent_task_id IS NULL
           AND root.deleted_at IS NULL
           AND EXISTS (SELECT 1 FROM task AS child
                       WHERE child.parent_task_id = root.id AND child.deleted_at IS NULL)",
    )
    .bind(project_id)
    .fetch_all(db.pool())
    .await?
    .into_iter()
    .collect())
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
    Ok(db::coordination_root_visible_children_in_connection(connection, task_id).await? != 0)
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

/// The three refusals a subtask meets while its hierarchy does not admit it
/// (the root is blocked, the root is not implementing, an earlier sibling is
/// unfinished). Each is a wait, not a rejection of the caller's input: the
/// hierarchy's own progress ends it. They stay `InvalidOperation` so every
/// public mapping of them is unchanged; the replay of an accepted action
/// recognises them through [`is_subtask_wait_refusal`], which reads the same
/// constants these messages are built from.
pub(crate) const ROOT_BLOCKED_REFUSAL: &str = "cannot execute while coordination root";
pub(crate) const ROOT_NOT_IMPLEMENTING_REFUSAL: &str = "cannot start while coordination root";
pub(crate) const SIBLING_ORDER_REFUSAL: &str = "cannot start yet; ordered execution is waiting on";

pub(crate) fn subtask_wait_refusal(message: String) -> ServiceError {
    debug_assert!(is_subtask_wait_message(&message), "{message}");
    ServiceError::invalid_operation(message)
}

fn is_subtask_wait_message(message: &str) -> bool {
    message.strip_prefix("subtask ").is_some_and(|rest| {
        rest.split_once(' ').is_some_and(|(_, refusal)| {
            [
                ROOT_BLOCKED_REFUSAL,
                ROOT_NOT_IMPLEMENTING_REFUSAL,
                SIBLING_ORDER_REFUSAL,
            ]
            .iter()
            .any(|known| refusal.starts_with(known))
        })
    })
}

/// Whether an admission refusal is the hierarchy making a subtask wait.
pub(crate) fn is_subtask_wait_refusal(error: &ServiceError) -> bool {
    matches!(error, ServiceError::InvalidOperation { message } if is_subtask_wait_message(message))
}

/// Enforce ordered dispatch for a child while preserving caller error text.
pub(crate) async fn ensure_subtask_dispatch_order(db: &SqliteDb, task: &Task) -> Result<()> {
    let Some(parent_task_id) = task.parent_task_id.as_deref() else {
        return Ok(());
    };
    let (parent, workflow) = coordination_root_context(db, parent_task_id).await?;
    if !coordination_root_allows_child_dispatch(&parent, &workflow) {
        return Err(subtask_wait_refusal(format!(
            "subtask {} {ROOT_NOT_IMPLEMENTING_REFUSAL} {} is blocked or not in its implementation state",
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
    Err(subtask_wait_refusal(format!(
        "subtask {} {SIBLING_ORDER_REFUSAL} {}",
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

/// Refuse a new subtask under a parent whose state never dispatches a child
/// (the state rule of [`coordination_root_allows_child_dispatch`]): a
/// terminal parent stays terminal, and a parent in a review-phase gate
/// (review, merging) is judged on the work it already has. A working state
/// in the review phase (`merge_failed`) does dispatch children, so a
/// corrective subtask is accepted there.
pub(crate) fn ensure_parent_accepts_subtasks(
    parent: &Task,
    workflow: &WorkflowDefinition,
) -> Result<()> {
    let kind = workflow.state_kind(&parent.status);
    let closed = kind == Some(StateKind::Terminal)
        || (kind == Some(StateKind::Gate)
            && workflow.canonical_phase_for_state(&parent.status) == CanonicalPhase::Review);
    if closed {
        return Err(ServiceError::SubtaskParentClosed {
            parent_task_id: parent.id.clone(),
            state: parent.status.clone(),
        });
    }
    Ok(())
}

/// Return whether a root state permits its next child to dispatch: an
/// unblocked root in an implementation (`Active`) state. A root that plans,
/// or has not started, holds its children; `merge_failed` is `Active` and
/// runs a corrective subtask.
pub(crate) fn coordination_root_allows_child_dispatch(
    parent: &Task,
    workflow: &WorkflowDefinition,
) -> bool {
    !root_blocked(parent) && workflow.state_kind(&parent.status) == Some(StateKind::Active)
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

// ---------------------------------------------------------------------------
// Re-plan (root lead, stage 2). A coordination root with a lead goes back to
// its planning state when its aggregate review is rejected and when its
// current subtask parks needing a decision. Everything here is a pure read of
// rows the caller already loaded, so the scheduling fact and the command that
// acts on it cannot disagree.
// ---------------------------------------------------------------------------

/// The subtree predicate of every lead operation: `target_id` is `root` or
/// one of its direct subtasks, in the same Project. Called once, by the lead
/// command boundary (`TaskService::bind_lead`), before any handler runs. A
/// Task of another root, of another Project, a deleted one and an unknown id
/// all answer the same typed refusal: the lead learns nothing about Tasks
/// outside its subtree.
pub(crate) async fn ensure_in_lead_subtree(
    db: &SqliteDb,
    root: &Task,
    target_id: &str,
) -> Result<Task> {
    let target = if target_id == root.id {
        Some(root.clone())
    } else {
        TaskRepo::get_by_id(db, target_id, false)
            .await?
            .filter(|task| {
                task.project_id == root.project_id
                    && task.parent_task_id.as_deref() == Some(root.id.as_str())
            })
    };
    target.ok_or_else(|| ServiceError::LeadOperationRefused {
        refusal: crate::LeadRefusal::OutsideSubtree,
        message: format!(
            "task {target_id} is not coordination root {} or one of its subtasks; a lead acts on its own root's subtasks only",
            root.id
        ),
    })
}

/// The lead of `task`'s root and what it may do to `task` now through the
/// Task action system: the recovery verbs while `task` is parked on something
/// the lead decides ([`subtask_decision_park`]), cancel while `task` never had
/// an implementation run. `None` for every Task that is not a subtask of a
/// root with a lead; nothing is read for a root.
pub(crate) async fn lead_verbs(
    db: &SqliteDb,
    task: &Task,
) -> Result<Option<(String, Vec<&'static str>)>> {
    let Some(parent_task_id) = task.parent_task_id.as_deref() else {
        return Ok(None);
    };
    let (root, workflow) = match coordination_root_context(db, parent_task_id).await {
        Ok(context) => context,
        Err(ServiceError::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error),
    };
    let Some(lead) = root_lead(db, &root, &workflow)
        .await?
        .and_then(|lead| lead.assignee_id)
    else {
        return Ok(None);
    };
    let mut verbs = Vec::new();
    if subtask_decision_park(task).is_some() {
        verbs.extend(crate::task_actions::LEAD_RECOVERY_VERBS);
    } else if !crate::task_service::lead::subtask_started(db, &task.id).await? {
        verbs.extend(crate::task_actions::LEAD_UNSTARTED_VERBS);
    }
    Ok(Some((lead, verbs)))
}

/// Database form of "this Task is a coordination root and has a lead":
/// [`root_lead`], and only for a root that has subtasks.
pub(crate) async fn coordination_root_lead(
    db: &SqliteDb,
    task: &Task,
    workflow: &WorkflowDefinition,
) -> Result<Option<TaskRoleAssignment>> {
    let Some(lead) = root_lead(db, task, workflow).await? else {
        return Ok(None);
    };
    Ok(coordination_root_has_subtasks(db, task)
        .await?
        .then_some(lead))
}

/// Where a rejected gate sends a Task. THE reader of a gate's
/// `reject_target` for a rejection: the configured target (`in_progress`
/// when the gate names none), except that a coordination root with a lead
/// never goes back to its implementation state from a review-phase gate. No
/// role runs for it there; its lead turns the findings into work, so it goes
/// to its planning state. `lead_assigned` is false for every Task that is
/// not a coordination root with a lead, and then nothing changes.
pub(crate) fn effective_reject_target(
    workflow: &WorkflowDefinition,
    gate_state: &str,
    lead_assigned: bool,
) -> String {
    let gate = workflow
        .states
        .iter()
        .find(|state| state.name == gate_state);
    let configured = gate
        .and_then(|state| state.gate_config.as_ref())
        .and_then(|gate| gate.reject_target.clone())
        .unwrap_or_else(|| default_states::IN_PROGRESS.to_owned());
    if !lead_assigned
        || gate.is_none_or(|state| {
            state.kind != StateKind::Gate || state.canonical_phase != Some(CanonicalPhase::Review)
        })
    {
        return configured;
    }
    let to_implementation = workflow.states.iter().any(|state| {
        state.name == configured
            && state.kind == StateKind::Active
            && state.canonical_phase != Some(CanonicalPhase::Review)
    });
    match RootRolePolicy::for_workflow(workflow).planning_state() {
        Some(planning) if to_implementation => planning.to_owned(),
        _ => configured,
    }
}

/// Whether `bridge` is a re-plan entry into `target`: the one transition a
/// workflow needs no trigger for. Only a system actor may make it, and only
/// into the workflow's planning state.
pub(crate) fn replan_edge(
    workflow: &WorkflowDefinition,
    bridge: &api_types::TransitionBridge,
    target: &str,
) -> bool {
    bridge.bridge_kind == Some(api_types::TransitionBridgeKind::Replan)
        && RootRolePolicy::for_workflow(workflow).planning_state() == Some(target)
}

/// The receipt of the re-plan for one park of one subtask. It is both the
/// budget charge identity and the identity the planning entry carries.
pub(crate) fn replan_receipt_for_subtask(child_id: &str, park: &str) -> String {
    format!("replan:{child_id}:{park}")
}

/// The receipt of the re-plan for one rejected aggregate review.
pub(crate) fn replan_receipt_for_review(review_id: &str) -> String {
    format!("replan:review:{review_id}")
}

/// The bridge of a re-plan entry. `trigger` is `aggregate_review` or
/// `subtask_parked`; `subject` is the Review or the subtask.
pub(crate) fn replan_bridge(
    trigger: &str,
    subject: &str,
    receipt: &str,
    reason: &str,
) -> api_types::TransitionBridge {
    api_types::TransitionBridge {
        bridge_kind: Some(api_types::TransitionBridgeKind::Replan),
        bridge_payload: Some(serde_json::json!({
            "trigger": trigger,
            "subject_id": subject,
            "receipt": receipt,
            "reason": reason,
        })),
    }
}

/// Whether the root already entered its planning state for `receipt`.
pub(crate) fn replan_entered(transitions: &[db::TransitionLog], receipt: &str) -> bool {
    transitions.iter().any(|entry| {
        entry.bridge.bridge_kind == Some(api_types::TransitionBridgeKind::Replan)
            && entry
                .bridge
                .bridge_payload
                .as_ref()
                .and_then(|payload| payload.get("receipt"))
                .and_then(serde_json::Value::as_str)
                == Some(receipt)
    })
}

/// The condition a subtask's park is judged on. A stored copy that only
/// shadows the legacy fields may lag them; the fields decide, as they do for
/// the Task itself in the dispatcher's snapshot.
fn decision_condition(child: &Task) -> db::TaskCondition {
    if child.condition.evidence().stated {
        child.condition.clone()
    } else {
        child
            .condition
            .restate_legacy(&db::LegacyConditionInput::from(child))
    }
}

/// Whether a subtask's failure of this kind is the lead's to decide. The
/// lead decides about the work: retry it, hand it to another Agent, split or
/// drop it. So the list is the kinds that say "the work did not get done":
/// its attempts ran out, its run failed or ran out of turns, its checks or
/// its own gate rejected it, a dependency was cancelled. Every other kind is
/// the host's, the repository's, the workspace's or the platform's (a hook, a
/// dirty or conflicting tree, a missing executor, a failed dispatch, an
/// internal error), or the owner's own stop: no plan fixes those, so they
/// spend no re-plan and stay the Project Agent's and the owner's. A kind this
/// build does not know is not the lead's either.
fn lead_decides_failure(kind: api_types::FailureKind) -> bool {
    use api_types::FailureKind as Kind;
    matches!(
        kind,
        Kind::RetryExhausted
            | Kind::ExecutorFailed
            | Kind::MaxTurnsExceeded
            | Kind::CiFailed
            | Kind::ReviewGateFailed
            | Kind::ReviewBudgetExhausted
            | Kind::WorkflowGuardRejected
            | Kind::DependencyCancelled
    )
}

/// A subtask parked needing a decision, and the identity of that park:
/// it failed or was interrupted, and nothing clears that by itself. Never a
/// hold or a failure the lead cannot fix ([`lead_decides_failure`]: a host
/// environment that is not ready, the workspace, the repository, a missing
/// executor), a queued owner action, or any wait that ends on its
/// own (capacity, an Agent, a machine, the environment, an unfinished
/// dependency, a human decision, checks, its parent, a retry deadline, a run
/// in flight). A cancelled dependency does need a decision.
///
/// The identity is the time the park was recorded, which the stored blocker
/// carries and no restatement changes; a later park of the same subtask has
/// a later one. A park that records no time is not re-planned.
pub(crate) fn subtask_decision_park(child: &Task) -> Option<String> {
    let condition = decision_condition(child);
    // The park's own reason decides, not a blocker recorded beside a wait
    // that ends by itself or belongs to the owner.
    let (reason, since) = match &condition {
        db::TaskCondition::Parked { primary, since, .. } => (primary, since.clone()),
        db::TaskCondition::Failed { failure, since, .. } => (failure, since.clone()),
        _ => return None,
    };
    let decides = match reason {
        db::ParkReason::Failure { failure_kind } => lead_decides_failure(*failure_kind),
        db::ParkReason::AgentTimeout { .. }
        | db::ParkReason::EntryBlocked { .. }
        | db::ParkReason::Dependencies {
            cancelled: true, ..
        } => true,
        // The subtask's attempts or its own gate ran out. A merge repair, a
        // review-infrastructure or a re-plan budget is not a subtask's work.
        db::ParkReason::BudgetExhausted { budget_kind, .. } => !matches!(
            budget_kind,
            Some(
                db::budget::Kind::MergeFix
                    | db::budget::Kind::ReviewCiInfrastructure
                    | db::budget::Kind::Replan
            )
        ),
        _ => false,
    };
    if !decides {
        return None;
    }
    let read = condition.read();
    // The kind shown beside the park also has to be the lead's. A kind this
    // build cannot name (`Unknown`, which is also how a timeout reads) says
    // nothing either way; the reason above already decided.
    if !(read.hard_failure || read.interruption_present)
        || read.failure_kind.is_some_and(|kind| {
            kind != api_types::FailureKind::Unknown && !lead_decides_failure(kind)
        })
        || read.queued_command
    {
        return None;
    }
    // A park with no recorded time has no identity: one receipt would stand
    // for every later park of the subtask. It is not re-planned; it is the
    // Project Agent's.
    since
        .or_else(|| {
            read.interruption
                .map(|interruption| interruption.created_at)
        })
        .or_else(|| read.diagnostic.and_then(|diagnostic| diagnostic.blocked_at))
        .filter(|at| !at.trim().is_empty())
}

impl RootRolePolicy<'_> {
    /// The level-triggered re-plan fact (T3): the subtask that parked and
    /// the identity of its park, when the root must go back to planning for
    /// it. True only for an unblocked root with a lead, in its working
    /// implementation state (never `merge_failed`), whose first incomplete
    /// subtask is parked needing a decision, and that has not entered
    /// planning for this park yet. One re-plan per park: after it, the same
    /// park is the Project Agent's.
    pub(crate) fn replan_due(
        &self,
        root: &Task,
        children: &[Task],
        assignments: &[TaskRoleAssignment],
        transitions: &[db::TransitionLog],
    ) -> Option<(String, String)> {
        if root.parent_task_id.is_some() || root_blocked(root) {
            return None;
        }
        let state = self
            .workflow
            .states
            .iter()
            .find(|state| state.name == root.status)?;
        if state.kind != StateKind::Active || state.canonical_phase == Some(CanonicalPhase::Review)
        {
            return None;
        }
        self.planning_state()?;
        self.lead(&root.id, assignments)?;
        let child = next_incomplete_child(children, self.workflow)?;
        let park = subtask_decision_park(child)?;
        (!replan_entered(transitions, &replan_receipt_for_subtask(&child.id, &park)))
            .then(|| (child.id.clone(), park))
    }
}

/// Database form of [`RootRolePolicy::replan_due`], for the two places that
/// act on it (the dispatcher's fact and the command): the re-plan that is
/// due and may start now. A re-plan never starts beside running work: while
/// the parked subtask still has a live run (a stop that has not landed), the
/// root stays where it is, because its planner would share the root's
/// workspace with that run. The pure fact stays true, so the park is still
/// the lead's ([`lead_handles_subtask_park`]) and the root re-plans when
/// the run has ended.
pub(crate) async fn replan_due_now(
    db: &SqliteDb,
    policy: &RootRolePolicy<'_>,
    root: &Task,
    children: &[Task],
    assignments: &[TaskRoleAssignment],
    transitions: &[db::TransitionLog],
) -> Result<Option<(String, String)>> {
    let Some((child_id, park)) = policy.replan_due(root, children, assignments, transitions) else {
        return Ok(None);
    };
    if !db::ExecutionRepo::list_running_by_task(db, &child_id)
        .await?
        .is_empty()
    {
        return Ok(None);
    }
    Ok(Some((child_id, park)))
}

/// Whether the lead of `task_id`'s coordination root handles that subtask's
/// park, so the Project Agent is not woken for it: the root is about to
/// re-plan for this park ([`RootRolePolicy::replan_due`]), or its lead is
/// planning now. False for every other Task, and for a park the lead already
/// re-planned for and left in place, a root that is held or blocked (its
/// re-plan budget spent included), and a root that lost its lead: those are
/// the Project Agent's.
pub(crate) async fn lead_handles_subtask_park(db: &SqliteDb, task_id: &str) -> Result<bool> {
    let Some(task) = TaskRepo::get_by_id(db, task_id, false).await? else {
        return Ok(false);
    };
    let Some(parent_task_id) = task.parent_task_id.as_deref() else {
        return Ok(false);
    };
    let (root, workflow) = coordination_root_context(db, parent_task_id).await?;
    let policy = RootRolePolicy::for_workflow(&workflow);
    let assignments = TaskRoleAssignmentRepo::list_by_task(db, parent_task_id).await?;
    if policy.lead(&root.id, &assignments).is_none() || root_blocked(&root) {
        return Ok(false);
    }
    if policy.planning_state() == Some(root.status.as_str()) {
        return Ok(true);
    }
    let children = ordered_children(db, parent_task_id).await?;
    let transitions = db::TransitionLogRepo::list_by_task(db, parent_task_id).await?;
    Ok(policy
        .replan_due(&root, &children, &assignments, &transitions)
        .is_some_and(|(child_id, _)| child_id == task.id))
}

/// THE rule for who answers a subtask's park, for every caller that would
/// wake the Project Agent for it: true while the lead of its root handles it
/// ([`lead_handles_subtask_park`]). It never fails. A subtask or root that
/// is gone has no lead to handle anything, so the park is the Project
/// Agent's. Any other read error says nothing about the lead: the caller
/// holds this one round and asks again on its next sweep, because waking the
/// Project Agent on a guess would set it and the lead on the same park.
pub(crate) async fn subtask_park_is_the_leads(db: &SqliteDb, task_id: &str) -> bool {
    match lead_handles_subtask_park(db, task_id).await {
        Ok(handled) => handled,
        Err(ServiceError::NotFound { .. } | ServiceError::Db(db::DbError::NotFound)) => false,
        Err(error) => {
            tracing::warn!(%task_id, %error, "lead filter could not read a subtask's root; holding its wake for one sweep");
            true
        }
    }
}

#[cfg(test)]
mod replan_tests {
    use super::*;

    fn task(id: &str, status: &str, parent: Option<&str>) -> Task {
        Task {
            condition: Default::default(),
            id: id.to_owned(),
            project_id: "project".to_owned(),
            parent_task_id: parent.map(str::to_owned),
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
            created_at: "2026-10-12T00:00:00Z".to_owned(),
            updated_at: "2026-10-12T00:00:00Z".to_owned(),
        }
    }

    fn parked(id: &str, at: &str, kind: &str) -> Task {
        let mut child = task(id, default_states::IN_PROGRESS, Some("root"));
        child.blocked_json = Some(
            serde_json::json!({"reason": "retry budget exhausted", "created_at": at, "kind": kind})
                .to_string(),
        );
        child.error_annotation = Some(
            serde_json::json!({"type": kind, "blocking_reason": "retry budget exhausted", "message": "three attempts failed", "blocked_at": at})
                .to_string(),
        );
        // The stored copy lags on purpose: the fields decide.
        child
    }

    fn lead_row() -> Vec<TaskRoleAssignment> {
        vec![TaskRoleAssignment {
            id: "lead".to_owned(),
            task_id: "root".to_owned(),
            role_name: "planner".to_owned(),
            assignee_type: Some(db::AssigneeKind::Agent),
            assignee_id: Some("lead".to_owned()),
            created_at: "2026-10-12T00:00:00Z".to_owned(),
            updated_at: "2026-10-12T00:00:00Z".to_owned(),
        }]
    }

    fn replan_entry(receipt: &str) -> db::TransitionLog {
        db::TransitionLog {
            id: "entry".to_owned(),
            task_id: "root".to_owned(),
            from_state: default_states::IN_PROGRESS.to_owned(),
            to_state: default_states::PLANNING.to_owned(),
            trigger_name: None,
            triggered_by: "system:task_dispatcher".to_owned(),
            bridge: replan_bridge("subtask_parked", "child", receipt, "parked"),
            trigger_reason: "parked".to_owned(),
            hook_results_json: None,
            rejection: false,
            created_at: "2026-10-12T00:00:01Z".to_owned(),
        }
    }

    #[test]
    fn a_lead_root_rejects_from_review_into_planning_and_nothing_else_changes() {
        let workflow = crate::workflow::default_workflow::default_workflow();
        assert_eq!(
            effective_reject_target(&workflow, "review", false),
            "in_progress"
        );
        assert_eq!(
            effective_reject_target(&workflow, "review", true),
            "planning"
        );
        // The merge gate rejects into a repair state, never into planning,
        // and the planning gate keeps rejecting into itself.
        assert_eq!(
            effective_reject_target(&workflow, "merging", true),
            "merge_failed"
        );
        assert_eq!(
            effective_reject_target(&workflow, "planning", true),
            "planning"
        );
        // A gate that names no target still defaults to `in_progress`.
        let mut bare = workflow.clone();
        for state in &mut bare.states {
            if state.name == "review" {
                state
                    .gate_config
                    .as_mut()
                    .expect("review gate")
                    .reject_target = None;
            }
        }
        assert_eq!(
            effective_reject_target(&bare, "review", false),
            "in_progress"
        );
        assert_eq!(effective_reject_target(&bare, "review", true), "planning");
        // A workflow without a planning state has no re-plan.
        let mut flat = workflow.clone();
        for state in &mut flat.states {
            if state.name == "planning" {
                state.role = Some("coder".to_owned());
            }
        }
        assert_eq!(
            effective_reject_target(&flat, "review", true),
            "in_progress"
        );

        let replan = replan_bridge(
            "aggregate_review",
            "review-1",
            "replan:review:review-1",
            "x",
        );
        assert!(replan_edge(&workflow, &replan, "planning"));
        assert!(!replan_edge(&workflow, &replan, "in_progress"));
        assert!(!replan_edge(&workflow, &Default::default(), "planning"));
        assert!(!replan_edge(&flat, &replan, "planning"));
    }

    #[test]
    fn a_subtask_needs_a_decision_only_when_nothing_clears_its_park_by_itself() {
        let at = "2026-10-12T01:00:00Z";
        assert_eq!(
            subtask_decision_park(&parked("child", at, "retry_exhausted")).as_deref(),
            Some(at)
        );
        assert_eq!(
            subtask_decision_park(&parked("child", at, "executor_failed")).as_deref(),
            Some(at)
        );
        // A park that records no time has no identity and is not re-planned.
        let mut undated = task("child", default_states::IN_PROGRESS, Some("root"));
        undated.error_annotation = Some(
            serde_json::json!({"type": "executor_failed", "blocking_reason": "failed", "message": "failed"})
                .to_string(),
        );
        assert_eq!(subtask_decision_park(&undated), None);
        // The host's environment is the owner's to fix, not the lead's.
        assert_eq!(
            subtask_decision_park(&parked("child", at, "environment_not_ready")),
            None
        );
        // The owner's hold is the owner's; a clear, running or waiting
        // subtask needs nothing.
        assert_eq!(
            subtask_decision_park(&parked("child", at, "manual_stop")),
            None
        );
        assert_eq!(
            subtask_decision_park(&task("child", default_states::IN_PROGRESS, Some("root"))),
            None
        );
        assert_eq!(
            subtask_decision_park(&task("child", default_states::TODO, Some("root"))),
            None
        );
        // A dependency that is merely unfinished ends by itself; a cancelled
        // one needs a decision.
        let mut waiting = task("child", default_states::IN_PROGRESS, Some("root"));
        waiting.condition = db::TaskCondition::Parked {
            primary: db::ParkReason::Capacity {
                scope: db::ConditionCapacityScope::Agent,
            },
            additional: Vec::new(),
            resume: db::ConditionContinuation::Reconcile,
            since: Some(at.to_owned()),
            evidence: db::ConditionEvidence {
                stated: true,
                presentation: Some(db::ConditionRead {
                    interruption_present: true,
                    ..Default::default()
                }),
                ..Default::default()
            },
        };
        assert_eq!(subtask_decision_park(&waiting), None);
        // The identity survives a restatement of the stored condition and
        // changes with the next park.
        let mut restated = parked("child", at, "retry_exhausted");
        restated.condition = db::map_legacy_condition(&db::LegacyConditionInput::from(&restated));
        assert_eq!(subtask_decision_park(&restated).as_deref(), Some(at));
        assert_ne!(
            subtask_decision_park(&parked("child", "2026-10-12T02:00:00Z", "retry_exhausted")),
            subtask_decision_park(&restated)
        );
    }

    /// The lead decides about work: what to retry, reassign, split or drop.
    /// A park whose cause is the host, the repository, the workspace or the
    /// platform is not its to fix, and must not spend a re-plan.
    #[test]
    fn a_park_the_lead_cannot_fix_is_not_replanned() {
        let at = "2026-10-12T01:00:00Z";
        for kind in [
            "retry_exhausted",
            "executor_failed",
            "max_turns_exceeded",
            "agent_timeout",
            "ci_failed",
            "workflow_guard_rejected",
        ] {
            assert_eq!(
                subtask_decision_park(&parked("child", at, kind)).as_deref(),
                Some(at),
                "{kind}"
            );
        }
        for kind in [
            "workspace_failed",
            "workspace_reset_required",
            "workspace_error",
            "executor_unavailable",
            "before_work_hook_failed",
            "before_work_hook_timeout",
            "dispatch_failed",
            "merge_conflict",
            "target_repo_dirty",
            "dirty_worktree",
            "internal_command_failed",
            "recovery_required",
            "review_needs_owner",
            "review_blocked",
            "cascade_failed",
            "workflow_loop",
            "environment_not_ready",
            "manual_stop",
            "merge_fix_budget_exhausted",
            "some_kind_a_later_release_adds",
        ] {
            assert_eq!(
                subtask_decision_park(&parked("child", at, kind)),
                None,
                "{kind}"
            );
        }
    }

    #[test]
    fn replan_is_due_once_per_park_for_a_working_lead_root() {
        let workflow = crate::workflow::default_workflow::default_workflow();
        let policy = RootRolePolicy::for_workflow(&workflow);
        let at = "2026-10-12T01:00:00Z";
        let children = vec![
            task("done", default_states::DONE, Some("root")),
            parked("child", at, "retry_exhausted"),
            task("later", default_states::TODO, Some("root")),
        ];
        let root = |status: &str| task("root", status, None);
        let due = |status: &str, rows: &[TaskRoleAssignment], log: &[db::TransitionLog]| {
            policy.replan_due(&root(status), &children, rows, log)
        };
        assert_eq!(
            due("in_progress", &lead_row(), &[]),
            Some(("child".to_owned(), at.to_owned()))
        );
        // No lead, no re-plan: the root waits on its subtasks as before.
        assert_eq!(due("in_progress", &[], &[]), None);
        // Only from the working implementation state.
        for status in [
            "todo",
            "planning",
            "review",
            "merging",
            "merge_failed",
            "done",
        ] {
            assert_eq!(due(status, &lead_row(), &[]), None, "{status}");
        }
        // One re-plan per park; the next park of the same subtask is new.
        let receipt = replan_receipt_for_subtask("child", at);
        assert_eq!(
            due("in_progress", &lead_row(), &[replan_entry(&receipt)]),
            None
        );
        assert!(due(
            "in_progress",
            &lead_row(),
            &[replan_entry(&replan_receipt_for_subtask(
                "child",
                "2026-10-11T00:00:00Z"
            ))]
        )
        .is_some());
        // A blocked or held root never re-plans.
        let mut blocked = root("in_progress");
        blocked.blocked_json = Some("{}".to_owned());
        blocked.condition = db::map_legacy_condition(&db::LegacyConditionInput::from(&blocked));
        assert_eq!(
            policy.replan_due(&blocked, &children, &lead_row(), &[]),
            None
        );
        // Only the first incomplete subtask counts: a parked later one waits
        // its turn behind a running one.
        let running_first = vec![
            task("first", default_states::IN_PROGRESS, Some("root")),
            parked("child", at, "retry_exhausted"),
        ];
        assert_eq!(
            policy.replan_due(&root("in_progress"), &running_first, &lead_row(), &[]),
            None
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The hierarchy's three refusals are recognised as waits and are still
    /// `InvalidOperation` for every caller; nothing else is taken for one.
    #[test]
    fn the_hierarchys_refusals_are_waits_and_stay_invalid_operations() {
        for refusal in [
            format!("subtask a {ROOT_BLOCKED_REFUSAL} b is blocked or has an error"),
            format!(
                "subtask a {ROOT_NOT_IMPLEMENTING_REFUSAL} b is blocked or not in its implementation state"
            ),
            format!("subtask a {SIBLING_ORDER_REFUSAL} b"),
        ] {
            let error = subtask_wait_refusal(refusal);
            assert!(matches!(error, ServiceError::InvalidOperation { .. }));
            assert!(is_subtask_wait_refusal(&error), "{error}");
        }
        for other in [
            ServiceError::invalid_operation("stale queued Task action"),
            ServiceError::invalid_operation("queued action's role ownership changed"),
            ServiceError::invalid_operation(
                "root tasks with subtasks are coordination containers; assign and run their subtasks",
            ),
            ServiceError::DependencyGate,
        ] {
            assert!(!is_subtask_wait_refusal(&other), "{other}");
        }
    }

    fn task(id: &str, status: &str, parent_task_id: Option<&str>) -> Task {
        Task {
            condition: Default::default(),
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

    /// A subtask is refused exactly where the parent's state never
    /// dispatches a child, so a corrective subtask under `merge_failed`
    /// (a working state in the review phase) is accepted.
    #[test]
    fn parent_refuses_subtasks_only_in_states_that_never_dispatch_a_child() {
        let workflow = crate::workflow::default_workflow::default_workflow();
        for (status, accepts) in [
            ("backlog", true),
            ("todo", true),
            ("planning", true),
            ("in_progress", true),
            ("merge_failed", true),
            ("review", false),
            ("merging", false),
            ("done", false),
            ("cancelled", false),
        ] {
            let parent = task("root", status, None);
            let refusal = ensure_parent_accepts_subtasks(&parent, &workflow);
            assert_eq!(refusal.is_ok(), accepts, "{status}");
            if !accepts {
                assert!(
                    !coordination_root_allows_child_dispatch(&parent, &workflow),
                    "{status}: refused although its children would be dispatched"
                );
                assert!(matches!(
                    refusal,
                    Err(ServiceError::SubtaskParentClosed { state, .. }) if state == status
                ));
            }
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
        root.condition = db::map_legacy_condition(&db::LegacyConditionInput::from(&root));
        assert!(root_blocked(&root));
        root.blocked_json = None;
        root.condition = db::map_legacy_condition(&db::LegacyConditionInput::from(&root));
        root.failed_json = Some("{}".to_owned());
        root.condition = db::map_legacy_condition(&db::LegacyConditionInput::from(&root));
        assert!(root_blocked(&root));
        root.failed_json = None;
        root.condition = db::map_legacy_condition(&db::LegacyConditionInput::from(&root));
        root.error_annotation = Some("{}".to_owned());
        root.condition = db::map_legacy_condition(&db::LegacyConditionInput::from(&root));
        assert!(root_blocked(&root));
        root.error_annotation = None;
        root.condition = db::map_legacy_condition(&db::LegacyConditionInput::from(&root));
        root.entry_barrier_json = Some("{}".to_owned());
        root.condition = db::map_legacy_condition(&db::LegacyConditionInput::from(&root));
        assert!(root_blocked(&root));
    }

    #[test]
    fn task_hierarchy_root_role_policy_allows_default_worker_and_aggregate_review() {
        let mut workflow = crate::workflow::default_workflow::default_workflow();
        let policy = RootRolePolicy::for_workflow(&workflow);

        assert_eq!(policy.implementation_role(), Some("coder"));
        assert!(policy.is_aggregate_review_role("reviewer"));
        assert!(policy.allows_assignment("reviewer"));
        assert!(policy.allows_assignment("coder"));
        // The planning role is the root's lead.
        assert_eq!(policy.planning_state(), Some(default_states::PLANNING));
        assert_eq!(policy.planning_role(), Some("planner"));
        assert!(policy.allows_assignment("planner"));
        assert!(!policy.allows_assignment("auditor"));

        workflow
            .states
            .iter_mut()
            .find(|state| state.name == default_states::REVIEW)
            .expect("default review state exists")
            .role = Some("coder".to_owned());
        let policy = RootRolePolicy::for_workflow(&workflow);
        assert!(policy.is_aggregate_review_role("coder"));
        assert!(policy.allows_assignment("coder"));
        assert!(!policy.allows_execution(default_states::REVIEW, "coder"));
    }

    #[test]
    fn task_hierarchy_root_execution_follows_the_review_state_role() {
        let mut workflow = crate::workflow::default_workflow::default_workflow();
        let policy = RootRolePolicy::for_workflow(&workflow);

        assert!(policy.allows_execution(default_states::REVIEW, "reviewer"));
        assert!(!policy.allows_execution(default_states::REVIEW, "coder"));
        assert!(!policy.allows_execution(default_states::IN_PROGRESS, "coder"));
        assert!(!policy.allows_execution(default_states::IN_PROGRESS, "reviewer"));
        // The lead runs in the planning state and nowhere else.
        assert!(policy.allows_execution(default_states::PLANNING, "planner"));
        assert!(!policy.allows_execution(default_states::IN_PROGRESS, "planner"));
        assert!(!policy.allows_execution(default_states::REVIEW, "planner"));
        assert!(!policy.allows_execution(default_states::PLANNING, "coder"));
        assert!(!policy.allows_execution(default_states::PLANNING, "reviewer"));

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

    fn assignment(task_id: &str, role: &str, agent: Option<&str>) -> TaskRoleAssignment {
        TaskRoleAssignment {
            id: format!("{task_id}-{role}"),
            task_id: task_id.to_owned(),
            role_name: role.to_owned(),
            assignee_type: agent.map(|_| db::AssigneeKind::Agent),
            assignee_id: agent.map(str::to_owned),
            created_at: "2026-09-28T00:00:00Z".to_owned(),
            updated_at: "2026-09-28T00:00:00Z".to_owned(),
        }
    }

    /// The planning state is found by position, not by name, and a planning
    /// gate that names the implementation role has no lead.
    #[test]
    fn task_hierarchy_planning_state_is_the_first_role_gate_before_implementation() {
        let mut workflow = crate::workflow::default_workflow::default_workflow();
        assert_eq!(
            planning_state(&workflow).map(|state| state.name.as_str()),
            Some(default_states::PLANNING)
        );
        for state in &mut workflow.states {
            if state.name == default_states::PLANNING {
                state.name = "design".to_owned();
                state.role = Some("architect".to_owned());
            }
            for trigger in state.triggers.values_mut() {
                if trigger.to == default_states::PLANNING {
                    trigger.to = "design".to_owned();
                }
            }
        }
        let policy = RootRolePolicy::for_workflow(&workflow);
        assert_eq!(policy.planning_state(), Some("design"));
        assert_eq!(policy.planning_role(), Some("architect"));
        assert!(policy.allows_assignment("architect"));
        assert!(policy.allows_execution("design", "architect"));
        assert!(!lead_role_supported("architect"));
        assert!(lead_role_supported("planner"));

        for state in &mut workflow.states {
            if state.name == "design" {
                state.role = Some("coder".to_owned());
            }
        }
        let policy = RootRolePolicy::for_workflow(&workflow);
        assert_eq!(policy.planning_state(), None);
        assert!(!policy.allows_execution("design", "coder"));
    }

    #[test]
    fn task_hierarchy_root_lead_is_the_agent_on_the_roots_own_planning_row() {
        let workflow = crate::workflow::default_workflow::default_workflow();
        let policy = RootRolePolicy::for_workflow(&workflow);
        let rows = vec![
            assignment("root", "coder", Some("worker")),
            assignment("other", "planner", Some("someone")),
        ];
        assert!(policy.lead("root", &rows).is_none());
        assert!(!policy.lead_holds_state("root", default_states::PLANNING, &rows));
        let unassigned = vec![assignment("root", "planner", None)];
        assert!(policy.lead("root", &unassigned).is_none());
        let rows = vec![assignment("root", "planner", Some("lead"))];
        assert_eq!(
            policy
                .lead("root", &rows)
                .and_then(|row| row.assignee_id.as_deref()),
            Some("lead")
        );
        assert!(policy.lead_holds_state("root", default_states::PLANNING, &rows));
        assert!(!policy.lead_holds_state("root", default_states::IN_PROGRESS, &rows));
    }

    /// Kickoff and completion are one level-triggered fact, and the lead's
    /// state is never advanced past, whatever flag is left on the root.
    #[test]
    fn task_hierarchy_root_advance_covers_kickoff_and_completion_and_never_the_leads_state() {
        let workflow = crate::workflow::default_workflow::default_workflow();
        let policy = RootRolePolicy::for_workflow(&workflow);
        let open = vec![task("child", default_states::TODO, Some("root"))];
        let closed = vec![task("child", default_states::DONE, Some("root"))];
        let lead = vec![assignment("root", "planner", Some("lead"))];
        let due = |status: &str, children: &[Task], rows: &[TaskRoleAssignment], pending| {
            policy.root_advance_due(&task("root", status, None), children, rows, pending, false)
        };
        // A planning run still live on a root whose lead was removed holds
        // the root exactly as the lead did.
        assert!(policy.planning_run_live("planning", ["planner"]));
        assert!(!policy.planning_run_live("planning", ["reviewer"]));
        assert!(!policy.planning_run_live("in_progress", ["planner"]));
        assert!(!policy.root_advance_due(&task("root", "planning", None), &open, &[], false, true));
        // Kickoff, with and without a lead.
        assert!(due("todo", &open, &[], false));
        assert!(due("todo", &open, &lead, false));
        assert!(due("planning", &open, &[], false));
        assert!(!due("planning", &open, &lead, false));
        assert!(!due("in_progress", &open, &lead, false));
        assert!(!due("backlog", &open, &[], false));
        assert!(!due("review", &open, &[], false));
        assert!(!due("todo", &[], &[], false));
        // Completion.
        assert!(due("in_progress", &closed, &lead, false));
        assert!(due("planning", &closed, &[], false));
        assert!(!due("review", &closed, &[], false));
        assert!(!due("done", &closed, &[], false));
        // The last subtask was cancelled while the lead plans: the flag an
        // earlier advance set does not starve the planner.
        assert!(!due("planning", &closed, &lead, false));
        assert!(!due("planning", &closed, &lead, true));
        assert!(due("planning", &closed, &[], true));
        assert!(due("review", &closed, &[], true));
        // A stale completion flag on a root with a subtask left is not work.
        assert!(!due("in_progress", &open, &[], true));

        assert_eq!(
            policy.advance_target("todo", true).as_deref(),
            Some("planning")
        );
        assert_eq!(
            policy.advance_target("todo", false).as_deref(),
            Some("in_progress")
        );
        assert_eq!(
            policy.advance_target("planning", false).as_deref(),
            Some("in_progress")
        );
    }

    #[test]
    fn task_hierarchy_child_dispatch_requires_a_live_implementing_root() {
        let workflow = crate::workflow::default_workflow::default_workflow();
        let mut root = task("root", default_states::IN_PROGRESS, None);
        assert!(coordination_root_allows_child_dispatch(&root, &workflow));
        // A root that has not started, or plans, holds its children; a root
        // repairing a failed merge runs its corrective subtask.
        for (status, allowed) in [
            ("backlog", false),
            ("todo", false),
            ("planning", false),
            ("merge_failed", true),
            ("merging", false),
        ] {
            root.status = status.to_owned();
            assert_eq!(
                coordination_root_allows_child_dispatch(&root, &workflow),
                allowed,
                "{status}"
            );
        }
        root.status = default_states::IN_PROGRESS.to_owned();

        root.status = default_states::REVIEW.to_owned();
        assert!(!coordination_root_allows_child_dispatch(&root, &workflow));
        root.status = default_states::DONE.to_owned();
        assert!(!coordination_root_allows_child_dispatch(&root, &workflow));
        root.status = "unknown".to_owned();
        assert!(!coordination_root_allows_child_dispatch(&root, &workflow));
        root.status = default_states::IN_PROGRESS.to_owned();
        root.error_annotation = Some("{}".to_owned());
        root.condition = db::map_legacy_condition(&db::LegacyConditionInput::from(&root));
        assert!(!coordination_root_allows_child_dispatch(&root, &workflow));
    }
}
