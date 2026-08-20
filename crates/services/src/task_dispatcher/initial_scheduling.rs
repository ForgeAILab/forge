use std::collections::HashSet;

use api_types::{Actor, StateKind, SystemComponent, WorkflowDefinition};
use db::{AgentRepo, DbError, Project, Task, TaskRoleAssignmentRepo};

use crate::{
    agent_service::{compute_effective_status, EffectiveStatus},
    task_service::TransitionOptions,
    Result, ServiceError,
};

use super::{helpers, TaskDispatcher};

#[derive(Debug)]
pub(super) struct InitialScheduleTarget {
    pub(super) transition_to: String,
    pub(super) agent_id: String,
}

impl TaskDispatcher {
    pub(super) async fn dispatch_initial_tasks(
        &self,
        project: &Project,
        workflow: &WorkflowDefinition,
    ) -> Result<u64> {
        let initial_states: Vec<String> = workflow
            .states
            .iter()
            .filter(|state| state.kind == StateKind::Initial)
            .map(|state| state.name.clone())
            .collect();
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
            let Some(target) = self
                .resolve_initial_schedule_target(workflow, &task)
                .await?
            else {
                continue;
            };
            match self.dispatch_initial_task(&task, &target).await {
                Ok(true) => dispatched += 1,
                Ok(false) => {}
                Err(ServiceError::Db(DbError::VersionConflict)) => {
                    tracing::debug!(task_id = %task.id, "task dispatcher initial transition lost version race");
                }
                Err(error) if helpers::is_not_runnable_error(&error) => {
                    tracing::warn!(
                        task_id = %task.id,
                        %error,
                        "task is not runnable under governance; parking until a user restarts it"
                    );
                    if let Err(annotate_error) = crate::workflow::engine::annotate_dispatch_failure(
                        &self.db,
                        &task.id,
                        task.status.as_str(),
                        &error.to_string(),
                    )
                    .await
                    {
                        tracing::warn!(task_id = %task.id, %annotate_error, "failed to annotate not-runnable task");
                    }
                }
                Err(error) => {
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
        if task.repo_id.is_none() {
            return Ok(false);
        }
        let agent = AgentRepo::get_by_id(&*self.db, &target.agent_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent", target.agent_id.clone()))?;
        if compute_effective_status(&self.db, &agent).await? != EffectiveStatus::Active {
            return Ok(false);
        }

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
