use super::*;
use crate::workflow::{actions::DispatchRoleAgent, HookAction, HookContext};
use api_types::{Actor, SystemComponent, WorkflowDefinition};

impl TaskService {
    /// The accepted claim owns startup even when its HTTP waiter times out.
    pub async fn claim_and_start_task(
        &self,
        task_id: impl Into<String>,
        assignee: Assignee,
        overrides: Option<ExecutionOverrides>,
    ) -> Result<ClaimedTask> {
        let task_id = task_id.into();
        if !db::task_writer::owns_task(&task_id) {
            return self
                .request_task_command(
                    &task_id,
                    "claim_and_start_task",
                    serde_json::json!([task_id, assignee, overrides]),
                    false,
                )
                .await;
        }
        let mut claimed = self.claim_task(&task_id, assignee, overrides).await?;
        self.start_execution(&claimed.execution.id).await?;
        claimed.task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id))?;
        Ok(claimed)
    }
    pub async fn claim_task(
        &self,
        task_id: impl Into<String>,
        assignee: Assignee,
        overrides: Option<ExecutionOverrides>,
    ) -> Result<ClaimedTask> {
        let task_id: String = task_id.into();
        if !db::task_writer::owns_task(&task_id) {
            return self
                .request_task_command(
                    &task_id,
                    "claim_task",
                    serde_json::json!([task_id, assignee, overrides]),
                    false,
                )
                .await;
        }

        validate_required("task_id", &task_id)?;

        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        super::execution::ensure_plan_publication_transition_authority(&task, None)?;
        if crate::task_hierarchy::coordination_root_has_subtasks(&self.db, &task).await? {
            return Err(ServiceError::invalid_operation(
                "root tasks with subtasks are coordination containers; assign and run their subtasks",
            ));
        }
        crate::task_hierarchy::ensure_subtask_dispatch_order(&self.db, &task).await?;
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
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
        // Do this before workspace preparation so a blocked Charter-backed
        // Task can never receive a workspace/lease as a side effect of an
        // attempted claim. Independent reviewer work uses its read-only gate.
        if target_role.as_deref() == Some(crate::workflow::default_roles::REVIEWER) {
            self.ensure_task_reviewable(&task).await?;
        } else {
            self.ensure_task_runnable(&task).await?;
        }
        let (assignee_type, agent, assignee_id, event_assignee_id) = match assignee {
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
                (
                    "agent".to_owned(),
                    Some(agent),
                    Some(agent_id.clone()),
                    agent_id,
                )
            }
            Assignee::User(user_handle) => {
                validate_required("user_handle", &user_handle)?;
                (
                    "user".to_owned(),
                    None,
                    Some(user_handle.clone()),
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
                self.ensure_claim_role_available(&task, role_name, claiming_agent_id)
                    .await?;
            }
        }
        let claim_execution_role = target_role
            .clone()
            .unwrap_or_else(|| crate::workflow::default_roles::CODER.to_owned());
        let claim_assignment = if agent.is_some()
            && claim_execution_role != crate::workflow::default_roles::INTERACTIVE
        {
            let assignment_role = if claim_execution_role == "executor" {
                crate::workflow::default_roles::CODER
            } else {
                claim_execution_role.as_str()
            };
            crate::task_hierarchy::effective_role_assignment(&self.db, &task, assignment_role)
                .await?
                .map(|resolved| resolved.assignment)
        } else {
            None
        };
        let inherited_target_workflow = task.parent_task_id.is_some()
            && WorkflowEngine::resolve_subtask_workflow()
                .states
                .iter()
                .any(|state| state.name == target_status);
        let reviewer_snapshot = if claim_execution_role == crate::workflow::default_roles::REVIEWER
        {
            ReviewRepo::list_by_task(&*self.db, &task.id)
                .await?
                .into_iter()
                .max_by_key(|review| (review.attempt_number, review.id.clone()))
        } else {
            None
        };
        let mut execution_admission = agent.as_ref().map(|claiming_agent| {
            let mut admission = db::ExecutionAdmission {
                purpose: None,
                expected_queued_recovery_id: None,
                expected_project_version: Some(project.version),
                expected_task_version: task.version,
                expected_task_status: task.status.clone(),
                expected_effective_role: (claim_execution_role
                    != crate::workflow::default_roles::INTERACTIVE)
                    .then(|| claim_execution_role.clone()),
                expected_agent_version: Some(claiming_agent.version),
                expected_agent_max_concurrent_tasks: Some(claiming_agent.max_concurrent_tasks),
                expected_reviewer_parent_execution_id: None,
                expected_latest_review_candidate_execution_id: None,
                expected_reviewer_id: None,
                expected_reviewer_attempt_number: None,
                expected_reviewer_status: None,
                expected_reviewer_updated_at: None,
                expected_reviewer_execution_id: None,
                expected_auditor_execution_id: None,
                expected_assignment_id: claim_assignment
                    .as_ref()
                    .map(|assignment| assignment.id.clone()),
                expected_assignment_updated_at: claim_assignment
                    .as_ref()
                    .map(|assignment| assignment.updated_at.clone()),
                expected_workflow_definition: (claim_execution_role
                    != crate::workflow::default_roles::INTERACTIVE
                    && !inherited_target_workflow)
                    .then(|| project.workflow_definition.clone()),
            };
            if let Some(review) = reviewer_snapshot.as_ref() {
                admission.expected_reviewer_parent_execution_id = Some(review.execution_id.clone());
                admission.expected_latest_review_candidate_execution_id =
                    Some(review.execution_id.clone());
                admission.expected_reviewer_id = Some(review.id.clone());
                admission.expected_reviewer_attempt_number = Some(review.attempt_number);
                admission.expected_reviewer_status = Some(review.status.to_string());
                admission.expected_reviewer_updated_at = Some(review.updated_at.clone());
                admission.expected_reviewer_execution_id = review.reviewer_execution_id.clone();
                admission.expected_auditor_execution_id = review.auditor_execution_id.clone();
            }
            admission
        });
        let previous_status = task.status.clone();
        let workspace_admission = self
            .reserve_workspace_admission(
                &task,
                agent.as_ref(),
                &claim_execution_role,
                crate::placement::selection::EnvironmentAdmission::LaunchPreflight,
            )
            .await?;
        let workspace_admission = self.prepare_claim_workspace(workspace_admission).await?;
        let workspace = &workspace_admission.workspace;
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
                            workspace,
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
            parent_execution_id: reviewer_snapshot
                .as_ref()
                .map(|review| review.execution_id.clone()),
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
        let execution_lease = self.initial_execution_lease(&execution).await?;
        let mut transaction = db::begin_immediate(self.db.pool())
            .await
            .map_err(DbError::from)?;
        self.check_claim_placement_in_tx(&mut transaction, &task, &workspace_admission)
            .await?;
        self.check_placement_lease_owner(&workspace_admission.placement, &execution_lease)?;
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
                execution,
                execution_admission: execution_admission.take(),
                expected_project_version: Some(project.version),
                expected_workflow_definition: Some(project.workflow_definition.clone()),
                execution_lease,
                claimed_at: now,
            },
        )
        .await;
        let claimed = match claimed {
            Ok(claimed) => claimed,
            Err(error) => {
                drop(transaction);
                return Err(error.into());
            }
        };
        // `TaskRepo::claim` verifies (and, only when truly absent, creates)
        // the effective assignment inside this same write transaction. Do not
        // materialize a root default on the child after that guarded insert.
        // Human claims remain user-managed work and do not mint repository
        // authority. Only a scheduler-dispatched Agent Worker/reviewer gets
        // an execution-scoped WorkspaceLease.
        let lease = if agent_id.is_some() {
            match self
                .issue_workspace_lease_in_tx(
                    &mut transaction,
                    &claimed.task,
                    workspace,
                    target_role.as_deref().unwrap_or("executor"),
                    agent_id.as_deref(),
                    &execution_id,
                )
                .await
            {
                Ok(lease) => Some(lease),
                Err(error) => {
                    drop(transaction);
                    return Err(error);
                }
            }
        } else {
            None
        };
        if let (Some(agent), Some(snapshot)) = (
            agent.as_ref(),
            claimed.execution.executor_config_snapshot_json.as_deref(),
        ) {
            let snapshot = match serde_json::from_str::<Value>(snapshot) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    drop(transaction);
                    return Err(ServiceError::invalid_operation(format!(
                        "invalid executor config snapshot for usage admission: {error}"
                    )));
                }
            };
            if let Err(error) = super::execution::ledger::admit_task_execution_in_tx_with_db(
                &self.db,
                &mut transaction,
                &claimed.task,
                &claimed.execution,
                agent,
                &snapshot,
            )
            .await
            {
                drop(transaction);
                return Err(error);
            }
        }
        crate::placement::admission::resolve_workspace_attention_in_tx(
            &self.db,
            &mut transaction,
            &task_id,
        )
        .await?;
        if let Err(error) = transaction.commit().await.map_err(DbError::from) {
            // The commit may have succeeded at SQLite despite a transport
            // error; revoke the lease idempotently so a crashed claimant can
            // never retain repository authority.
            if let Some(lease) = lease.as_ref() {
                self.revoke_workspace_lease(lease).await;
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
            project_version: Some(project.version),
            project_workflow_definition: Some(project.workflow_definition.clone()),
            triggered_by: Actor::Agent {
                agent_id: task.assignee_id.clone().unwrap_or_default(),
                execution_id: Some(execution_id.to_owned()),
            },
            review_runner: self.review_runner.clone(),
            merge_service: self.merge_service.clone(),
            cleanup_scheduler: self.cleanup_scheduler.clone(),
            task_service: self.clone(),
            daemon_connections: self.daemon_connections.clone(),
            workspace_exec_locks: self.workspace_exec_locks.clone(),
            terminal_activity: self.terminal_activity.clone(),
            workspace_root: self.workspace_root.clone(),
            repo_cache_locks: self.repo_cache_locks.clone(),
            workspace_backend_router: Arc::clone(&self.workspace_backend_router),
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
        task: &Task,
        role_name: &str,
        claiming_agent_id: &str,
    ) -> Result<()> {
        let existing = crate::task_hierarchy::effective_role_assignment(&self.db, task, role_name)
            .await?
            .map(|resolved| resolved.assignment);
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
    let target = crate::workflow::claim_target_state(workflow, current_status)
        .map(|state| state.name.clone());

    target.ok_or_else(|| {
        ServiceError::invalid_operation(format!(
            "task in state '{current_status}' has no claimable active transition"
        ))
    })
}
