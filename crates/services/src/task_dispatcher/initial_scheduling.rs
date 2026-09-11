use std::collections::HashSet;

use api_types::{Actor, StateKind, SystemComponent, WorkflowDefinition};
use db::{AgentRepo, DbError, Project, Task, TaskRoleAssignmentRepo};

use crate::{
    agent_service::{compute_effective_status, EffectiveStatus},
    deferred_dispatch,
    task_service::TransitionOptions,
    workflow::engine::WorkflowEngine,
    Result, ServiceError,
};

use super::{helpers, TaskDispatcher};

/// Dispatch-disposition capability for the coordination-root aggregate review
/// advance. A root never takes an ordinary role dispatch — both scans `continue`
/// out of the root branch — so it owns the single disposition slot outright and
/// cannot collide with a role capability.
pub(super) const COORDINATION_ROOT_CAPABILITY: &str = "coordination_root_advance";

#[derive(Debug)]
pub(super) struct InitialScheduleTarget {
    pub(super) transition_to: String,
    pub(super) role: String,
    pub(super) agent_id: String,
}

impl TaskDispatcher {
    /// Attempt one coordination-root aggregate-review advance, quiescing on a
    /// deterministic refusal.
    ///
    /// Ordinary role dispatch already parks a Task whose deterministic blocker
    /// has not changed, so the identical denial is not re-derived and re-logged
    /// on every scan. The coordination-root branch had no equivalent guard: a
    /// root that could not enter aggregate review re-attempted the advance and
    /// re-logged the same warning every 10s indefinitely — observed running for
    /// 19 consecutive scans until a user recovered the Task by hand.
    ///
    /// Parking is safe here because every event that can change the answer also
    /// wakes the root: `advance_subtask_sequence` calls `wake_task_dispatch` on
    /// the parent before advancing it, and any recovery action that clears the
    /// root's blocker bumps its `version`. Either invalidates the disposition.
    ///
    /// Returns the number of advances that actually committed, so callers can
    /// fold it straight into their dispatched count.
    pub(super) async fn advance_coordination_root_once(&self, task: &Task) -> Result<u64> {
        if deferred_dispatch::dispatch_disposition_is_current(task, COORDINATION_ROOT_CAPABILITY) {
            return Ok(0);
        }
        match self.task_service.advance_coordination_root(&task.id).await {
            Ok(()) => {
                deferred_dispatch::clear_dispatch_disposition(&self.db, task).await?;
                Ok(1)
            }
            Err(ServiceError::Db(DbError::VersionConflict)) => {
                tracing::debug!(task_id = %task.id, "coordination-root review advance lost version race");
                Ok(0)
            }
            Err(error) if helpers::is_deterministic_dispatch_refusal(&error) => {
                deferred_dispatch::record_dispatch_disposition(
                    &self.db,
                    task,
                    COORDINATION_ROOT_CAPABILITY,
                    &error.to_string(),
                )
                .await?;
                tracing::warn!(
                    task_id = %task.id,
                    %error,
                    "coordination-root aggregate review advance parked until Task state changes or an explicit wake"
                );
                Ok(0)
            }
            Err(error) => {
                // Potentially transient: no disposition, so the next scan
                // retries instead of stalling on a momentary failure.
                tracing::warn!(task_id = %task.id, %error, "coordination-root aggregate review advance remains pending");
                Ok(0)
            }
        }
    }

    pub(super) async fn dispatch_initial_tasks(
        &self,
        project: &Project,
        workflow: &WorkflowDefinition,
    ) -> Result<u64> {
        let mut initial_states: Vec<String> = workflow
            .states
            .iter()
            .filter(|state| state.kind == StateKind::Initial)
            .map(|state| state.name.clone())
            .collect();
        for state in WorkflowEngine::resolve_subtask_workflow()
            .states
            .iter()
            .filter(|state| state.kind == StateKind::Initial)
        {
            if !initial_states.contains(&state.name) {
                initial_states.push(state.name.clone());
            }
        }
        if initial_states.is_empty() {
            return Ok(0);
        }

        let mut tasks = self.list_tasks(&project.id, initial_states).await?;
        tasks.sort_by(|left, right| {
            right
                .priority
                .cmp(&left.priority)
                .then_with(|| left.created_at.cmp(&right.created_at))
                .then_with(|| left.id.cmp(&right.id))
        });

        let mut dispatched = 0;
        for task in tasks {
            if self.is_stopped() {
                break;
            }
            if !crate::task_service::subtask_dispatch_ready(&self.db, &task).await? {
                continue;
            }
            if crate::task_service::coordination_root_has_subtasks(&self.db, &task).await? {
                // The Project Agent coordinates this root through its child
                // records. Only children receive implementation dispatches.
                let sequence_complete = crate::task_service::coordination_root_sequence_complete(
                    &self.db, &task, workflow,
                )
                .await?;
                let needs_recovery_advance = sequence_complete
                    && workflow.canonical_phase_for_state(&task.status)
                        != api_types::CanonicalPhase::Review
                    && workflow.state_kind(&task.status) != Some(StateKind::Terminal);
                if crate::task_service::coordination_review_pending(&task) || needs_recovery_advance
                {
                    dispatched += self.advance_coordination_root_once(&task).await?;
                }
                continue;
            }
            let task_workflow = WorkflowEngine::resolve_workflow_for_task(
                &task,
                &project.workflow_definition,
                &Actor::system(SystemComponent::TaskDispatcher),
            );
            if task_workflow.state_kind(&task.status) != Some(StateKind::Initial) {
                continue;
            }
            // Creation gives a Task the Project's default assignees, so a Task
            // with no role assignment at all was proposed before those
            // defaults existed in Project settings (e.g. while the Project
            // was still paused for having no repository). Apply them now
            // rather than skipping the Task on every scan with nothing in
            // the log.
            if TaskRoleAssignmentRepo::list_by_task(&*self.db, &task.id)
                .await?
                .is_empty()
            {
                if let Err(error) = self.task_service.assign_project_default_roles(&task).await {
                    tracing::warn!(task_id = %task.id, %error, "Task has no role assignments and the Project defaults could not be applied");
                }
            }
            let Some(target) = self
                .resolve_initial_schedule_target(&task_workflow, &task)
                .await?
            else {
                continue;
            };
            if deferred_dispatch::dispatch_disposition_is_current(&task, &target.role) {
                // An unchanged deterministic blocker was already observed for
                // this exact Task version and capability. Skip the attempt —
                // and its warning — entirely rather than re-deriving and
                // re-logging the identical denial every scan (F11). Nothing
                // schedules this Task again until its version changes or an
                // explicit `wake_task_dispatch` clears the disposition.
                continue;
            }
            match self.dispatch_initial_task(&task, &target).await {
                Ok(true) => {
                    deferred_dispatch::clear_dispatch_disposition(&self.db, &task).await?;
                    dispatched += 1;
                }
                Ok(false) => {}
                Err(ServiceError::Db(DbError::VersionConflict)) => {
                    tracing::debug!(task_id = %task.id, "task dispatcher initial transition lost version race");
                }
                Err(error) if helpers::is_deterministic_dispatch_refusal(&error) => {
                    deferred_dispatch::record_dispatch_disposition(
                        &self.db,
                        &task,
                        &target.role,
                        &error.to_string(),
                    )
                    .await?;
                    tracing::warn!(
                        task_id = %task.id,
                        %error,
                        "task dispatch blocked; parked until Task/governance state changes or an explicit wake"
                    );
                }
                Err(error) => {
                    // Potentially transient: no disposition, so the next scan
                    // retries instead of stalling on a momentary failure.
                    tracing::warn!(task_id = %task.id, %error, "task dispatcher initial dispatch failed");
                }
            }
        }
        Ok(dispatched)
    }

    pub(super) async fn dispatch_initial_task(
        &self,
        task: &Task,
        target: &InitialScheduleTarget,
    ) -> Result<bool> {
        if self.is_stopped() {
            return Ok(false);
        }
        if helpers::has_blocking_annotation(task) {
            return Ok(false);
        }
        // Keep dispatch admission on the same centralized Task gate used by
        // claim/launch/lease issuance. It loads persisted capability/risk,
        // canonical setup projection, and the exact baseline rather than
        // reconstructing authority from Task kind or repository presence.
        if target.role == crate::workflow::default_roles::REVIEWER {
            self.task_service.ensure_task_reviewable(task).await?;
        } else {
            self.task_service.ensure_task_runnable(task).await?;
        }
        let agent = AgentRepo::get_by_id(&*self.db, &target.agent_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent", target.agent_id.clone()))?;
        if compute_effective_status(&self.db, &agent).await? != EffectiveStatus::Active {
            return Ok(false);
        }
        crate::ensure_execution_role_principal(
            &self.db,
            &task.project_id,
            &target.role,
            &target.agent_id,
        )
        .await?;

        self.task_service
            .transition(
                task.id.clone(),
                target.transition_to.clone(),
                TransitionOptions {
                    version: task.version,
                    reason: Some("scheduled by task dispatcher".to_owned()),
                    triggered_by: Actor::system(SystemComponent::TaskDispatcher),
                    rejection: false,
                    defer_dispatch_seconds: None,
                },
            )
            .await?;
        Ok(true)
    }

    pub(super) async fn resolve_initial_schedule_target(
        &self,
        workflow: &WorkflowDefinition,
        task: &Task,
    ) -> Result<Option<InitialScheduleTarget>> {
        let mut cursor_state = task.status.clone();
        let mut target_kinds = vec![StateKind::Active, StateKind::Gate];
        let mut visited = HashSet::new();
        let mut first_hop: Option<String> = None;

        loop {
            if !visited.insert(cursor_state.clone()) {
                return Ok(None);
            }
            let Some(target_state) =
                helpers::first_transition_to_kind(workflow, &cursor_state, &target_kinds)
            else {
                return Ok(None);
            };
            let transition_to = first_hop
                .get_or_insert_with(|| target_state.name.clone())
                .clone();
            let Some(role_name) = crate::workflow::effective_role(target_state) else {
                return Ok(None);
            };
            let assignment =
                TaskRoleAssignmentRepo::get_by_task_and_role(&*self.db, &task.id, role_name)
                    .await?;
            match assignment {
                Some(assignment)
                    if assignment.assignee_type == Some(db::AssigneeKind::Agent)
                        && assignment.assignee_id.is_some() =>
                {
                    return Ok(Some(InitialScheduleTarget {
                        transition_to,
                        role: role_name.to_owned(),
                        agent_id: assignment.assignee_id.expect("checked by match guard"),
                    }));
                }
                Some(assignment) if assignment.assignee_type == Some(db::AssigneeKind::User) => {
                    return Ok(None);
                }
                Some(assignment)
                    if helpers::role_assignment_unassigned(Some(&assignment))
                        && helpers::auto_cascades_on_unassigned_role(target_state) =>
                {
                    cursor_state = target_state.name.clone();
                    target_kinds = vec![StateKind::Active];
                }
                Some(_) => return Ok(None),
                None if helpers::auto_cascades_on_unassigned_role(target_state) => {
                    cursor_state = target_state.name.clone();
                    target_kinds = vec![StateKind::Active];
                }
                None => return Ok(None),
            }
        }
    }
}
