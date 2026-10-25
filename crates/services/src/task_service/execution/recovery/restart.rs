//! Task condition restart and daemon owner workspace reset.

use super::*;

impl TaskService {
    pub(crate) async fn restart_task_condition(
        &self,
        task: Task,
        annotation: &api_types::TaskBlockingAnnotation,
        reason: Option<String>,
    ) -> Result<Task> {
        let reason = optional_recovery_reason(reason, "restart");
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &TaskService::task_action_actor(api_types::Actor::user(
                api_types::UserActionSource::Action(api_types::TaskAction::Restart {
                    reason: None,
                }),
            )),
        );
        let initial_state = workflow_initial_state(&workflow)?;
        self.reset_daemon_owner_workspace(&task).await?;
        let assignee_id = should_clear_assignments_for_reset(annotation).then_some(None);
        let retry_gate_state = match self.current_gate_retry_budget(&task).await {
            Ok((gate_state, _, _)) => Some(gate_state),
            // A reset from a normal (non-gate) state does not need a retry
            // boundary.  Do not turn unrelated workflow/project/database
            // failures into that case: without this narrow match a status
            // reset could commit while silently losing its marker.
            Err(ServiceError::Conflict(message))
                if message.contains(" is not a retry-budget gate and no gate rejects into it") =>
            {
                None
            }
            Err(error) => return Err(error),
        };
        let actor = TaskService::task_action_actor(api_types::Actor::user(
            api_types::UserActionSource::Action(api_types::TaskAction::Restart { reason: None }),
        ));
        // Reset is the base handler's atomic condition/status/budget boundary;
        // it does not execute state-entry hooks or wait for dispatch admission.
        let reset_input = UpdateTaskStatus {
            id: task.id.clone(),
            expected_version: task.version,
            status: initial_state,
            assignee_id,
            error_annotation: Some(None),
            blocked_json: Some(None),
            failed_json: Some(None),
            updated_at: now_rfc3339(),
        };
        let (recovered, retry_boundary_id) = if let Some(gate_state) = retry_gate_state.as_deref() {
            let marker = recovery_marker(&task.id, gate_state, "restart", &actor, &reason);
            let id = marker.id.clone();
            (
                TaskRepo::update_status_with_recovery_marker(&*self.db, reset_input, marker)
                    .await?,
                Some(id),
            )
        } else {
            (TaskRepo::update_status(&*self.db, reset_input).await?, None)
        };
        crate::placement::admission::resolve_workspace_attention(&self.db, &recovered.id).await?;
        if let Some(boundary_id) = retry_boundary_id.as_deref() {
            self.publish_recovery_applied(
                &recovered,
                "restart",
                retry_gate_state.as_deref(),
                Some(boundary_id),
            );
        }

        super::clear_execution_retry_metadata(&self.db, &recovered).await?;
        if task.condition.read().interruption_present {
            self.publish(ForgeEvent {
                event_type: "task.unblocked".to_owned(),
                entity_id: recovered.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::TaskUnblocked {
                    project_id: recovered.project_id.clone(),
                    previous_reason: task.condition.read().interruption.map(|i| i.reason),
                },
            });
        }
        if task.condition.read().hard_failure {
            self.publish(ForgeEvent {
                event_type: "task.restarted".to_owned(),
                entity_id: recovered.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::TaskRestarted {
                    project_id: recovered.project_id.clone(),
                    previous_reason: task.condition.read().interruption.map(|i| i.reason),
                    new_execution_id: None,
                },
            });
        }
        self.publish(ForgeEvent {
            event_type: "task.recovery_action".to_owned(),
            entity_id: recovered.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskRecovered {
                project_id: recovered.project_id.clone(),
                reason,
            },
        });
        Ok(recovered)
    }

    pub(crate) async fn reset_daemon_owner_workspace(&self, task: &Task) -> Result<()> {
        let Some(placement) = db::WorkspacePlacementRepo::get_for_task(&*self.db, &task.id).await?
        else {
            return Ok(());
        };
        if placement.owner_kind != db::PlacementOwnerKind::Daemon {
            return Ok(());
        }
        // A removed owner's workspace is already released; the next admission
        // prepares a fresh one on another machine, so there is nothing to reset.
        if let Some(daemon_id) = placement.daemon_id.as_deref() {
            if self.db.daemon_removed(daemon_id).await? {
                return Ok(());
            }
        }
        if placement.task_id != task.id {
            return Err(ServiceError::invalid_operation(
                "reset the root's shared workspace instead",
            ));
        }
        let workspace = WorkspaceRepo::get_by_id(&*self.db, &placement.workspace_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", &placement.workspace_id))?;
        let resolved = self
            .workspace_backend_router
            .resolve(&self.db, &workspace)
            .await?;
        super::super::workspace::reset_daemon_workspace(&self.db, &workspace, &resolved).await?;
        Ok(())
    }
}
