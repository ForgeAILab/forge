use std::sync::Arc;

use api_types::{StateKind, WorkflowDefinition};
use db::{AgentRepo, DbError, Project, Task, TaskRoleAssignmentRepo};

use crate::{
    agent_capacity::has_running_execution_capacity,
    agent_service::{compute_effective_status, EffectiveStatus},
    deferred_dispatch,
    workflow::{
        dispatch::{
            build_effective_prompt, dispatch_intent_from_workflow_dispatch,
            effective_prompt_selection, loader::load_agent_dispatch_context,
        },
        effective_role,
    },
    Result, ServiceError,
};

use super::{helpers, TaskDispatcher};

impl TaskDispatcher {
    pub(super) async fn recover_active_tasks(
        &self,
        project: &Project,
        workflow: &WorkflowDefinition,
    ) -> Result<u64> {
        let active_states: Vec<String> = workflow
            .states
            .iter()
            .filter(|state| matches!(state.kind, StateKind::Active | StateKind::Gate))
            .map(|state| state.name.clone())
            .collect();
        if active_states.is_empty() {
            return Ok(0);
        }

        let tasks = self.list_tasks(&project.id, active_states).await?;
        let mut dispatched = 0;
        for task in tasks {
            if self.is_stopped() {
                break;
            }
            match self.recover_active_task(project, workflow, &task).await {
                Ok(true) => dispatched += 1,
                Ok(false) => {}
                Err(ServiceError::Db(DbError::VersionConflict)) => {
                    tracing::debug!(task_id = %task.id, "task dispatcher recovery lost version race");
                }
                Err(ref error @ ServiceError::WorkspaceResetRequired { .. }) => {
                    tracing::warn!(task_id = %task.id, %error, "task branch lost, blocking for user reset");
                    if let Err(block_error) =
                        self.block_task_for_workspace_reset(&task, error).await
                    {
                        tracing::warn!(task_id = %task.id, %block_error, "failed to block task for workspace reset");
                    }
                }
                Err(error) if helpers::is_io_or_workspace_error(&error) => {
                    tracing::error!(task_id = %task.id, %error, "task dispatcher recovery blocked task due to workspace error");
                    if let Err(block_error) =
                        self.block_task_on_workspace_error(&task, &error).await
                    {
                        tracing::warn!(task_id = %task.id, %block_error, "failed to block task after workspace error");
                    }
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
                    tracing::warn!(task_id = %task.id, %error, "task dispatcher recovery failed");
                }
            }
        }
        Ok(dispatched)
    }

    async fn recover_active_task(
        &self,
        project: &Project,
        workflow: &WorkflowDefinition,
        task: &Task,
    ) -> Result<bool> {
        let Some(state) = workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
        else {
            return Ok(false);
        };
        if !matches!(state.kind, StateKind::Active | StateKind::Gate) {
            return Ok(false);
        }
        if self.is_stopped() {
            return Ok(false);
        }
        if !state
            .hooks
            .on_enter
            .iter()
            .any(|hook| hook.action == "dispatch_role_agent")
        {
            return Ok(false);
        }
        if helpers::has_blocking_annotation(task) {
            return Ok(false);
        }
        if task.repo_id.is_none() {
            return Ok(false);
        }
        if deferred_dispatch::is_pending(task, chrono::Utc::now()) {
            return Ok(false);
        }
        let Some(role_name) = effective_role(state) else {
            return Ok(false);
        };
        if state.kind == StateKind::Gate && helpers::auto_cascades_on_unassigned_role(state) {
            let assignment =
                TaskRoleAssignmentRepo::get_by_task_and_role(&*self.db, &task.id, role_name)
                    .await?;
            if helpers::role_assignment_unassigned(assignment.as_ref()) {
                let Some(target) = self.resolve_initial_schedule_target(workflow, task).await?
                else {
                    return Ok(false);
                };
                return self.dispatch_initial_task(task, &target).await;
            }
        }
        if helpers::latest_stopped_execution_blocks_dispatch(&self.db, &task.id, role_name).await? {
            return Ok(false);
        }
        let Some(assignment) =
            TaskRoleAssignmentRepo::get_by_task_and_role(&*self.db, &task.id, role_name).await?
        else {
            return Ok(false);
        };
        if assignment.assignee_type != Some(db::AssigneeKind::Agent) {
            return Ok(false);
        }
        let Some(agent_id) = assignment.assignee_id.as_deref() else {
            return Ok(false);
        };
        if helpers::has_running_execution_for_roles(
            &self.db,
            &task.id,
            &helpers::execution_guard_roles(role_name),
        )
        .await?
        {
            return Ok(false);
        }

        let state_config =
            helpers::merged_state_config(state, project, task.task_state_config.as_deref());
        if task.entry_barrier_json.is_some() {
            return Ok(false);
        }
        if role_name == crate::workflow::default_roles::REVIEWER
            && !helpers::reviewer_dispatch_ready(&self.db, &task.id, &state_config).await?
        {
            return Ok(false);
        }

        let agent = AgentRepo::get_by_id(&*self.db, agent_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent", agent_id.to_owned()))?;
        match compute_effective_status(&self.db, &agent).await? {
            EffectiveStatus::Error
            | EffectiveStatus::Paused
            | EffectiveStatus::DaemonOffline
            | EffectiveStatus::DaemonUnavailable
            | EffectiveStatus::ConnectionDegraded
            | EffectiveStatus::ConnectionUnavailable
            | EffectiveStatus::Deactivated => return Ok(false),
            EffectiveStatus::Active | EffectiveStatus::Busy => {}
        }
        if !has_running_execution_capacity(&self.db, &agent).await? {
            return Ok(false);
        }
        if deferred_dispatch::pending_until(task).is_some() {
            deferred_dispatch::clear(&self.db, task).await?;
        }

        let state_dispatch = dispatch_intent_from_workflow_dispatch(state.dispatch.as_ref());
        let selection = effective_prompt_selection(role_name, None, state_dispatch.as_ref());
        let dispatch_ctx = load_agent_dispatch_context(
            Arc::clone(&self.db),
            &task.id,
            role_name,
            &state.name,
            state_config,
            Some(selection.execution_policy.as_str()),
            workflow,
        )
        .await?;
        let (prompt, _selection) =
            build_effective_prompt(&dispatch_ctx, None, state_dispatch.as_ref());
        let dispatch_metadata = serde_json::json!({
            "target_role": role_name,
            "builder_id": selection.builder_id,
            "execution_policy": selection.execution_policy,
        });
        if self.is_stopped() {
            return Ok(false);
        }
        self.task_service
            .dispatch_initial_role_execution_with_metadata(
                &task.id,
                &agent.id,
                role_name,
                prompt.user,
                Some(dispatch_metadata),
            )
            .await?;
        Ok(true)
    }
}
