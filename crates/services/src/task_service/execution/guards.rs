use super::*;

impl TaskService {
    /// Admit an execution attempt against the coordination hierarchy before a
    /// caller transitions a Task or prepares a workspace.  A root with
    /// children is a container: only its aggregate reviewer may run after
    /// every child is terminal.  A child must be the first incomplete sibling,
    /// and its root must still be live and unblocked.
    pub(super) async fn ensure_ordered_execution_admission(
        &self,
        task: &Task,
        role: &str,
    ) -> Result<()> {
        if task.parent_task_id.is_none() {
            if !super::super::subtask::coordination_root_has_subtasks(&self.db, task).await? {
                return Ok(());
            }

            if task.blocked_json.is_some()
                || task.failed_json.is_some()
                || task.error_annotation.is_some()
                || task.entry_barrier_json.is_some()
            {
                return Err(ServiceError::invalid_operation(format!(
                    "coordination root {} is blocked or has an error",
                    task.id
                )));
            }

            let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
            let workflow = WorkflowEngine::resolve_workflow_for_task(
                task,
                &project.workflow_definition,
                &api_types::Actor::system(api_types::SystemComponent::Executor),
            );
            if workflow.canonical_phase_for_state(&task.status) != api_types::CanonicalPhase::Review
            {
                return Err(ServiceError::invalid_operation(format!(
                    "coordination root {} is not in its aggregate review state",
                    task.id
                )));
            }
            let expected_role = workflow
                .states
                .iter()
                .find(|state| state.name == task.status)
                .and_then(crate::workflow::effective_role);
            if expected_role != Some(role) {
                return Err(ServiceError::invalid_operation(
                    "root tasks with subtasks are coordination containers; only their aggregate review role may execute",
                ));
            }

            let subtasks = TaskRepo::list_subtasks_ordered(&*self.db, &task.id).await?;
            if let Some(next) = subtasks
                .iter()
                .find(|candidate| !super::super::subtask::subtask_is_terminal(candidate, &workflow))
            {
                return Err(ServiceError::invalid_operation(format!(
                    "coordination-root review is not ready; subtask {} is still incomplete",
                    next.id
                )));
            }
            return Ok(());
        }

        let parent_task_id = task
            .parent_task_id
            .as_deref()
            .expect("parent_task_id was checked above");
        let parent = TaskRepo::get_by_id(&*self.db, parent_task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", parent_task_id.to_owned()))?;
        if parent.blocked_json.is_some()
            || parent.failed_json.is_some()
            || parent.error_annotation.is_some()
            || parent.entry_barrier_json.is_some()
        {
            return Err(ServiceError::invalid_operation(format!(
                "subtask {} cannot execute while coordination root {} is blocked or has an error",
                task.id, parent.id
            )));
        }

        let project = ProjectRepo::get_by_id(&*self.db, &parent.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", parent.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &parent,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Executor),
        );
        if matches!(
            workflow.state_kind(&parent.status),
            Some(api_types::StateKind::Backlog | api_types::StateKind::Terminal)
        ) || workflow.canonical_phase_for_state(&parent.status)
            == api_types::CanonicalPhase::Review
        {
            return Err(ServiceError::invalid_operation(format!(
                "subtask {} cannot execute while coordination root {} is in {}",
                task.id, parent.id, parent.status
            )));
        }

        super::super::subtask::ensure_subtask_dispatch_order(&self.db, task).await
    }

    pub(super) async fn wait_for_agent_active_before_dispatch(
        &self,
        execution: &Execution,
    ) -> Result<Option<db::Execution>> {
        let Some(agent_id) = execution.agent_id.as_deref() else {
            return Ok(None);
        };
        let deadline = tokio::time::Instant::now() + DISPATCH_STATUS_WAIT_CEILING;

        loop {
            let current_execution = ExecutionRepo::get_by_id(&*self.db, &execution.id)
                .await?
                .ok_or_else(|| ServiceError::not_found("execution", execution.id.clone()))?;
            if current_execution.status != ExecutionStatus::Running {
                tracing::info!(
                    execution_id = %execution.id,
                    status = %current_execution.status,
                    "execution dispatch stopped while waiting for agent"
                );
                return Ok(Some(current_execution));
            }

            let agent = AgentRepo::get_by_id(&*self.db, agent_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("agent", agent_id.to_owned()))?;
            let status = compute_effective_status(&self.db, &agent).await?;
            if status == EffectiveStatus::Active
                || self
                    .busy_only_because_current_execution(&status, &agent, execution)
                    .await?
            {
                return Ok(None);
            }

            if tokio::time::Instant::now() >= deadline {
                let message = format!(
                    "agent {agent_id} did not become active within 600s before dispatch; last effective_status={status}"
                );
                return self
                    .fail_execution_before_dispatch(&execution.id, message)
                    .await
                    .map(Some);
            }

            tracing::debug!(
                execution_id = %execution.id,
                %agent_id,
                effective_status = %status,
                "waiting to dispatch execution"
            );
            // Queue waiting is not semantic execution progress and must not
            // repurpose the legacy activity timestamp as a heartbeat.  The
            // owner-bound lease is renewed by the executor after admission;
            // an unowned pre-dispatch row remains eligible for deterministic
            // recovery if this process disappears.
            tokio::time::sleep(DISPATCH_STATUS_POLL_INTERVAL).await;
        }
    }

    pub(super) async fn busy_only_because_current_execution(
        &self,
        status: &EffectiveStatus,
        agent: &Agent,
        execution: &Execution,
    ) -> Result<bool> {
        if *status != EffectiveStatus::Busy {
            return Ok(false);
        }
        let running_count = count_running_executions(&self.db, &agent.id).await?;
        if running_count <= agent.max_concurrent_tasks {
            return Ok(true);
        }
        let task = TaskRepo::get_by_id(&*self.db, &execution.task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", execution.task_id.clone()))?;
        let role_assignment = match execution.role.as_str() {
            "executor" | crate::workflow::default_roles::CODER => {
                self.coder_assignment(&task.id).await?
            }
            role => TaskRoleAssignmentRepo::get_by_task_and_role(&*self.db, &task.id, role).await?,
        };
        if !matches!(
            role_assignment.as_ref(),
            Some(assignment)
                if assignment.assignee_type == Some(AssigneeKind::Agent)
                    && assignment.assignee_id.as_deref() == Some(&agent.id)
        ) {
            return Ok(false);
        }
        Ok(running_count <= agent.max_concurrent_tasks)
    }

    pub(super) async fn ensure_no_running_interactive_execution(
        &self,
        task_id: &str,
    ) -> Result<()> {
        if let Some(running) = ExecutionRepo::list_running_by_task(&*self.db, task_id)
            .await?
            .into_iter()
            .find(|execution| execution.role == "interactive")
        {
            return Err(ServiceError::execution_already_running(
                "interactive",
                running.id,
            ));
        }
        Ok(())
    }

    /// A repository workspace admits only one running execution, including a
    /// root workspace shared by ordered subtasks. This preflight avoids
    /// preparing side effects for a losing launch; the execution INSERT
    /// repeats the check transactionally to close races and cover remote
    /// daemon execution.
    pub(super) async fn ensure_no_running_repository_execution(&self, task: &Task) -> Result<()> {
        let workspace_task_id = task.parent_task_id.as_deref().unwrap_or(&task.id);
        let workspace = WorkspaceRepo::get_by_task_id(&*self.db, workspace_task_id).await?;
        let running_execution_id = if let Some(workspace) = workspace {
            sqlx::query_scalar::<_, String>(
                "SELECT id FROM execution
                 WHERE workspace_id = ? AND status = 'running'
                 ORDER BY created_at DESC, id DESC LIMIT 1",
            )
            .bind(&workspace.id)
            .fetch_optional(self.db.pool())
            .await?
        } else {
            None
        };
        if let Some(running_execution_id) = running_execution_id {
            return Err(ServiceError::execution_already_running(
                "repository",
                running_execution_id,
            ));
        }
        Ok(())
    }

    pub(super) async fn check_dependency_gate(&self, task: &Task, agent_id: &str) -> Result<()> {
        let unsatisfied_dependencies =
            TaskDependencyRepo::unsatisfied_dependencies(&*self.db, &task.id).await?;
        if unsatisfied_dependencies.is_empty() {
            return Ok(());
        }

        let cancelled = self
            .cancelled_dependency_ids(task, &unsatisfied_dependencies)
            .await?;
        if !cancelled.is_empty() {
            self.block_cancelled_dependencies(task, &cancelled).await?;
            return Err(ServiceError::DependencyGate);
        }

        for depends_on_id in &unsatisfied_dependencies {
            if ExecutionRepo::has_execution_by_task_role_and_agent(
                &*self.db,
                depends_on_id,
                "executor",
                agent_id,
            )
            .await?
            {
                return Ok(());
            }
        }

        Err(ServiceError::DependencyGate)
    }

    pub async fn fail_execution_before_dispatch(
        &self,
        execution_id: &str,
        error: String,
    ) -> Result<db::Execution> {
        // A WorkspaceLease or optimistic-version mismatch here is the
        // in-transition dispatch race: the lease was pinned to the Task
        // version observed inside the transition and the commit moved it.
        // The dispatcher's recovery pass re-dispatches outside a transition,
        // so these stay auto-resumable; anything else waits for a human.
        let transient_authority_race =
            error.contains("WorkspaceLease") || error.contains("version conflict");
        let resume_policy = if transient_authority_race {
            db::ResumePolicy::Auto
        } else {
            db::ResumePolicy::Manual
        };
        let current = ExecutionRepo::get_by_id(&*self.db, execution_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("execution", execution_id.to_owned()))?;
        if current.status != ExecutionStatus::Running {
            return Ok(current);
        }
        let mut terminal_candidate = current;
        let mut outcome = None;
        for _ in 0..3 {
            let terminalized_at = now_rfc3339();
            let attempt = ExecutionRepo::terminalize_with_ledger(
                &*self.db,
                super::ledger::terminal_with_ledger(
                    TerminalizeExecution {
                        execution_id: terminal_candidate.id.clone(),
                        expected_version: terminal_candidate.execution_version,
                        // Pre-dispatch failures are authorized by the freshly
                        // read execution version. If a scheduler has already
                        // claimed an owner, include it as an additional
                        // predicate; legacy rows with no owner intentionally use
                        // version-only cancellation.
                        lease_owner: terminal_candidate.lease_owner.clone(),
                        status: ExecutionStatus::Failed,
                        stop_reason: Some(Some(db::StopReason::ExecutorFailed)),
                        stopped_by: Some(Some(
                            api_types::Actor::system(api_types::SystemComponent::Dispatch)
                                .display(),
                        )),
                        stopped_at: Some(Some(terminalized_at.clone())),
                        resume_policy: Some(Some(resume_policy.clone())),
                        agent_session_id: None,
                        agent_message_id: None,
                        last_activity_at: None,
                        last_progress_at: None,
                        summary: None,
                        logs_path: None,
                        before_sha: None,
                        after_sha: None,
                        error: Some(Some(error.clone())),
                        executor_config_snapshot_json: None,
                        updated_at: terminalized_at,
                        actor_type: "system".to_owned(),
                        actor_id: Some("dispatch".to_owned()),
                        correlation_id: Some(terminal_candidate.id.clone()),
                        causation_id: None,
                        causation_depth: 0,
                        lease_disposition: ExecutionLeaseDisposition::Revoke,
                    },
                    Vec::new(),
                    None,
                    None,
                ),
            )
            .await
            .map_err(ServiceError::from)?;
            match attempt {
                ExecutionTerminalOutcome::Concurrent {
                    current: Some(current),
                } if current.status == ExecutionStatus::Running => {
                    terminal_candidate = current;
                }
                other => {
                    outcome = Some(other);
                    break;
                }
            }
        }
        let outcome = outcome.unwrap_or(ExecutionTerminalOutcome::Concurrent {
            current: Some(terminal_candidate),
        });
        match outcome {
            ExecutionTerminalOutcome::Committed { execution, .. } => {
                super::publish_terminal_execution_event(self, &execution);
                if should_block_task_for_failed_execution(&execution) {
                    if let Err(error) = self.annotate_dispatch_failure_block(&execution).await {
                        tracing::warn!(
                            execution_id = %execution.id,
                            task_id = %execution.task_id,
                            %error,
                            "failed to block task after dispatch failure"
                        );
                    }
                }
                Ok(execution)
            }
            ExecutionTerminalOutcome::Concurrent { current } => {
                current.ok_or_else(|| ServiceError::not_found("execution", execution_id.to_owned()))
            }
        }
    }
}
