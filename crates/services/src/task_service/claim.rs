use super::workspace::prepare_workspace_owned;
use super::*;
use crate::workflow::{actions::DispatchRoleAgent, HookAction, HookContext};
use api_types::{Actor, StateKind, SystemComponent, WorkflowDefinition};
use sqlx::Row;

impl TaskService {
    pub async fn claim_task(
        &self,
        task_id: impl Into<String>,
        assignee: Assignee,
        overrides: Option<ExecutionOverrides>,
    ) -> Result<ClaimedTask> {
        let task_id = task_id.into();
        validate_required("task_id", &task_id)?;

        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        if task.parent_task_id.is_some() {
            return Err(ServiceError::subtask_managed_by_root(
                task_id.clone(),
                task.parent_task_id.clone().unwrap_or_default(),
            ));
        }
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        // Do this before workspace preparation so a blocked Charter-backed
        // implementation Task can never receive a workspace/lease as a side
        // effect of an attempted claim.
        self.ensure_task_runnable(&task).await?;
        if matches!(&assignee, Assignee::Agent(_)) && project.paused_at.is_some() {
            return Err(ServiceError::ProjectPaused {
                project_id: project.id,
            });
        }
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &Actor::system(SystemComponent::General),
        );
        WorkflowEngine::validate_claimable(&workflow, &task.status)?;
        let target_status = resolve_claim_target(&workflow, &task.status)?;
        let target_role = workflow
            .states
            .iter()
            .find(|state| state.name == target_status)
            .and_then(crate::workflow::effective_role)
            .map(str::to_owned);
        let capacity_statuses = workflow_capacity_statuses(&workflow);
        let (assignee_type, agent, assignee_id, max_concurrent_tasks, event_assignee_id) =
            match assignee {
                Assignee::Agent(agent_id) => {
                    validate_required("agent_id", &agent_id)?;
                    let agent = AgentRepo::get_by_id(&*self.db, &agent_id)
                        .await?
                        .ok_or_else(|| ServiceError::not_found("agent", agent_id.clone()))?;
                    if agent.paused {
                        return Err(ServiceError::AgentPaused {
                            agent_id: agent.id.clone(),
                        });
                    }
                    let max_concurrent_tasks = agent.max_concurrent_tasks;
                    (
                        "agent".to_owned(),
                        Some(agent),
                        Some(agent_id.clone()),
                        max_concurrent_tasks,
                        agent_id,
                    )
                }
                Assignee::User(user_handle) => {
                    validate_required("user_handle", &user_handle)?;
                    (
                        "user".to_owned(),
                        None,
                        Some(user_handle.clone()),
                        i64::MAX,
                        user_handle,
                    )
                }
            };
        if let Some(claiming_agent) = agent.as_ref() {
            self.ensure_repository_worker_identity(&task.project_id, &claiming_agent.id)
                .await?;
            if let Some(role_name) = target_role.as_deref() {
                crate::ensure_execution_role_principal(
                    &self.db,
                    &task.project_id,
                    role_name,
                    &claiming_agent.id,
                )
                .await?;
            }
        }
        if let Some(claiming_agent_id) = agent.as_ref().map(|agent| agent.id.as_str()) {
            if let Some(role_name) = target_role.as_deref() {
                self.ensure_claim_role_available(&task.id, role_name, claiming_agent_id)
                    .await?;
                // A claim seeds the claiming agent into the role of the state it
                // enters. That is right for a work role, but the reviewer role
                // must stay independent of the Worker: without this guard a
                // Worker claiming its way into the review gate silently became
                // its own reviewer. Checked here, before a workspace or
                // transaction exists, so a refused claim costs nothing.
                self.ensure_review_role_is_independent(
                    &project,
                    &task.id,
                    role_name,
                    claiming_agent_id,
                )
                .await?;
            }
        }
        let previous_status = task.status.clone();
        let (workspace, workspace_created_by_attempt) = prepare_workspace_owned(
            &self.db,
            &self.workspace_root,
            &task,
            &task_id,
            self.repo_cache_locks.clone(),
        )
        .await?;
        let now = now_rfc3339();
        let execution_id = new_uuid_v4();
        let executor_config_snapshot_json = match agent.as_ref() {
            Some(agent) => {
                match build_executor_config_snapshot(&self.db, &task, agent, overrides).await {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        create_failed_execution_record(
                            &self.db,
                            &task_id,
                            agent,
                            &workspace,
                            &execution_id,
                            error.to_string(),
                        )
                        .await?;
                        return Err(error);
                    }
                }
            }
            None => None,
        };
        let agent_id = agent.as_ref().map(|agent| agent.id.clone());
        let execution = CreateExecution {
            id: execution_id.clone(),
            task_id: task_id.clone(),
            agent_id: agent_id.clone(),
            role: target_role.clone().unwrap_or_else(|| "executor".to_owned()),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json,
            workspace_id: Some(workspace.id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        };
        let execution_lease = match self.initial_execution_lease(&execution).await {
            Ok(lease) => lease,
            Err(error) => {
                if workspace_created_by_attempt {
                    self.cleanup_fresh_execution_workspace(&task, &workspace)
                        .await;
                }
                return Err(error);
            }
        };
        let mut transaction = db::begin_immediate(self.db.pool())
            .await
            .map_err(DbError::from)?;
        let claimed = TaskRepo::claim(
            &*self.db,
            &mut transaction,
            ClaimTask {
                task_id: task_id.clone(),
                assignee_type,
                assignee_id,
                expected_version: task.version,
                source_status: task.status.clone(),
                target_status: target_status.clone(),
                capacity_statuses,
                execution,
                execution_lease,
                max_concurrent_tasks,
                claimed_at: now,
            },
        )
        .await;
        let claimed = match claimed {
            Ok(claimed) => claimed,
            Err(error) => {
                drop(transaction);
                if workspace_created_by_attempt {
                    self.cleanup_fresh_execution_workspace(&task, &workspace)
                        .await;
                }
                return Err(error.into());
            }
        };
        if let (Some(role_name), Some(claiming_agent_id)) =
            (target_role.as_deref(), agent_id.as_deref())
        {
            let role_now = now_rfc3339();
            sqlx::query(
                "INSERT INTO task_role_assignment
                    (id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at)
                 VALUES (?, ?, ?, 'agent', ?, ?, ?)
                 ON CONFLICT(task_id, role_name) DO NOTHING",
            )
            .bind(new_uuid_v4())
            .bind(&claimed.task.id)
            .bind(role_name)
            .bind(claiming_agent_id)
            .bind(&role_now)
            .bind(&role_now)
            .execute(&mut *transaction)
            .await
            .map_err(DbError::from)?;
            let assignment = sqlx::query(
                "SELECT assignee_type, assignee_id
                 FROM task_role_assignment WHERE task_id = ? AND role_name = ?",
            )
            .bind(&claimed.task.id)
            .bind(role_name)
            .fetch_one(&mut *transaction)
            .await
            .map_err(DbError::from)?;
            if assignment
                .get::<Option<String>, _>("assignee_type")
                .as_deref()
                != Some("agent")
                || assignment
                    .get::<Option<String>, _>("assignee_id")
                    .as_deref()
                    != Some(claiming_agent_id)
            {
                return Err(ServiceError::conflict(format!(
                    "role '{}' is assigned to a different agent",
                    role_name
                )));
            }
        }
        // Human claims remain user-managed work and do not mint repository
        // authority. Only a scheduler-dispatched Agent Worker/reviewer gets
        // an execution-scoped WorkspaceLease.
        let lease = if agent_id.is_some() {
            match self
                .issue_workspace_lease_in_tx(
                    &mut transaction,
                    &claimed.task,
                    &workspace,
                    target_role.as_deref().unwrap_or("executor"),
                    agent_id.as_deref(),
                    &execution_id,
                )
                .await
            {
                Ok(lease) => Some(lease),
                Err(error) => {
                    drop(transaction);
                    if workspace_created_by_attempt {
                        self.cleanup_fresh_execution_workspace(&task, &workspace)
                            .await;
                    }
                    return Err(error);
                }
            }
        } else {
            None
        };
        if let Err(error) = transaction.commit().await.map_err(DbError::from) {
            // The commit may have succeeded at SQLite despite a transport
            // error; revoke the lease idempotently so a crashed claimant can
            // never retain repository authority.
            if let Some(lease) = lease.as_ref() {
                self.revoke_workspace_lease(lease).await;
            }
            if workspace_created_by_attempt {
                self.cleanup_fresh_execution_workspace(&task, &workspace)
                    .await;
            }
            return Err(error.into());
        }

        self.publish(ForgeEvent {
            event_type: "task.assigned".to_owned(),
            entity_id: claimed.task.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskAssigned {
                project_id: claimed.task.project_id.clone(),
                agent_id: event_assignee_id,
                execution_id: execution_id.clone(),
            },
        });
        // Claims move work into a workflow-resolved active state, so subscribers need the same status event as manual transitions.
        self.publish(ForgeEvent {
            event_type: "task.status_changed".to_owned(),
            entity_id: claimed.task.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskStatusChanged {
                project_id: claimed.task.project_id.clone(),
                old_status: task.status.to_string(),
                new_status: claimed.task.status.to_string(),
            },
        });

        if agent_id.is_some() {
            if claimed.task.parent_task_id.is_none() {
                if let Err(error) = super::execution::subtasks::begin_next_turn(
                    &self.db,
                    &self.event_bus,
                    &self.workspace_root,
                    &claimed.task.id,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %claimed.task.id,
                        %error,
                        "failed to begin subtask sequence, dispatching normally"
                    );
                }
            }
            self.dispatch_claim_state_role_agent(
                &claimed.task,
                &previous_status,
                &execution_id,
                claimed.execution.workspace_id.clone(),
            )
            .await;
        }

        Ok(claimed)
    }

    /// Refuse a claim that would make a Task's Worker its own reviewer.
    ///
    /// Only the Project's canonical independent-reviewer role is guarded; work
    /// roles are still seeded from the claiming agent as before.
    async fn ensure_review_role_is_independent(
        &self,
        project: &db::Project,
        task_id: &str,
        role_name: &str,
        claiming_agent_id: &str,
    ) -> Result<()> {
        let roles = crate::resolve_project_execution_roles(&self.db, project).await?;
        let (Some(worker_role), Some(reviewer_role)) =
            (roles.worker_role.as_deref(), roles.reviewer_role.as_deref())
        else {
            return Ok(());
        };
        if !review_role_needs_independence(worker_role, reviewer_role, role_name) {
            return Ok(());
        }
        let worker_assignee =
            TaskRoleAssignmentRepo::get_by_task_and_role(&*self.db, task_id, worker_role)
                .await?
                .and_then(|assignment| assignment.assignee_id);
        if worker_assignee.as_deref() == Some(claiming_agent_id) {
            return Err(ServiceError::conflict(format!(
                "role '{role_name}' requires an identity independent of the Worker"
            )));
        }
        Ok(())
    }

    async fn dispatch_claim_state_role_agent(
        &self,
        task: &Task,
        previous_status: &str,
        execution_id: &str,
        workspace_id: Option<String>,
    ) {
        let Ok(Some(project)) = ProjectRepo::get_by_id(&*self.db, &task.project_id).await else {
            return;
        };
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &Actor::system(SystemComponent::General),
        );
        let Some(state) = workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
        else {
            return;
        };
        if !state
            .hooks
            .on_enter
            .iter()
            .any(|hook| hook.action == "dispatch_role_agent")
        {
            return;
        }
        let gate_config = state.gate_config.clone();
        let state_config = state.config.clone();

        let ctx = HookContext {
            task_id: task.id.clone(),
            project_id: task.project_id.clone(),
            from_state: previous_status.to_owned(),
            to_state: task.status.clone(),
            db: Arc::clone(&self.db),
            event_bus: Arc::clone(&self.event_bus),
            gate_config,
            workflow: Arc::new(workflow),
            triggered_by: Actor::Agent {
                agent_id: task.assignee_id.clone().unwrap_or_default(),
                execution_id: Some(execution_id.to_owned()),
            },
            review_runner: self.review_runner.clone(),
            merge_service: self.merge_service.clone(),
            cleanup_scheduler: self.cleanup_scheduler.clone(),
            task_executor: self.task_executor.clone(),
            daemon_connections: self.daemon_connections.clone(),
            workspace_exec_locks: self.workspace_exec_locks.clone(),
            terminal_activity: self.terminal_activity.clone(),
            workspace_root: self.workspace_root.clone(),
            repo_cache_locks: self.repo_cache_locks.clone(),
            workspace_id,
            agent_id: task.assignee_id.clone(),
            execution_id: Some(execution_id.to_owned()),
            state_config,
        };
        let _ = DispatchRoleAgent.execute(&ctx).await;
    }

    pub(super) async fn role_for_state(
        &self,
        task: &Task,
        state_name: &str,
    ) -> Result<Option<String>> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &Actor::system(SystemComponent::General),
        );
        Ok(workflow
            .states
            .iter()
            .find(|state| state.name == state_name)
            .and_then(crate::workflow::effective_role)
            .map(str::to_owned))
    }

    async fn ensure_claim_role_available(
        &self,
        task_id: &str,
        role_name: &str,
        claiming_agent_id: &str,
    ) -> Result<()> {
        let existing =
            TaskRoleAssignmentRepo::get_by_task_and_role(&*self.db, task_id, role_name).await?;
        if existing.as_ref().is_some_and(|assignment| {
            assignment.assignee_type != Some(AssigneeKind::Agent)
                || assignment.assignee_id.as_deref() != Some(claiming_agent_id)
        }) {
            return Err(ServiceError::conflict(format!(
                "role '{role_name}' is assigned to a different agent"
            )));
        }
        Ok(())
    }
}

fn resolve_claim_target(workflow: &WorkflowDefinition, current_status: &str) -> Result<String> {
    let source_kind = workflow.state_kind(current_status);
    let targets = workflow
        .outgoing_trigger_targets(current_status)
        .filter(|(trigger, _)| {
            !trigger.system_only()
                || matches!(source_kind, Some(StateKind::Initial | StateKind::Custom))
        })
        .filter_map(|(_, target_name)| {
            workflow
                .states
                .iter()
                .find(|target| target.name == target_name)
                .map(|target| (target.name.clone(), target.kind))
        })
        .collect::<Vec<_>>();
    let target = targets
        .iter()
        .find(|(_, kind)| *kind == StateKind::Active)
        .or_else(|| targets.iter().find(|(_, kind)| *kind == StateKind::Gate))
        .map(|(name, _)| name.clone());

    target.ok_or_else(|| {
        ServiceError::invalid_operation(format!(
            "task in state '{current_status}' has no claimable active transition"
        ))
    })
}

fn workflow_capacity_statuses(workflow: &WorkflowDefinition) -> Vec<String> {
    workflow
        .states
        .iter()
        .filter(|state| matches!(state.kind, StateKind::Active | StateKind::Gate))
        .map(|state| state.name.clone())
        .collect()
}

/// Does claiming `role_name` need to be independent of the Task's Worker?
///
/// Only when the workflow genuinely separates the two roles. Plenty of
/// workflows use a single role to both implement and validate; there the
/// Worker legitimately claims its own role, and treating that as self-review
/// refuses the Worker from doing any work at all.
fn review_role_needs_independence(worker_role: &str, reviewer_role: &str, role_name: &str) -> bool {
    worker_role != reviewer_role && reviewer_role == role_name
}

#[cfg(test)]
mod claim_independence_tests {
    use super::review_role_needs_independence;

    #[test]
    fn a_separate_reviewer_role_needs_independence() {
        assert!(review_role_needs_independence(
            "coder", "reviewer", "reviewer"
        ));
    }

    #[test]
    fn claiming_the_work_role_never_needs_independence() {
        assert!(!review_role_needs_independence(
            "coder", "reviewer", "coder"
        ));
    }

    #[test]
    fn a_shared_role_never_needs_independence() {
        // The regression that refused a Worker its own work role: when one
        // role both implements and validates, every claim looked like a
        // self-review.
        assert!(!review_role_needs_independence(
            "worker", "worker", "worker"
        ));
    }

    #[test]
    fn an_unrelated_role_is_untouched() {
        assert!(!review_role_needs_independence(
            "coder", "reviewer", "planner"
        ));
    }
}
