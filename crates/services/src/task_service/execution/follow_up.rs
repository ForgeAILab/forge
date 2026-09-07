use super::*;

impl TaskService {
    pub async fn dispatch_follow_up(
        &self,
        task_id: &str,
        review_outcome: ::review::ReviewOutcome,
        parent_execution_id: String,
    ) -> Result<Execution> {
        validate_required("task_id", task_id)?;
        validate_required("parent_execution_id", &parent_execution_id)?;

        let (trigger, prompt) = match &review_outcome {
            ::review::ReviewOutcome::Passed => {
                return Err(ServiceError::invalid_operation(
                    "cannot dispatch follow-up for a passed review",
                ));
            }
            ::review::ReviewOutcome::PassedCiOnly => {
                return Err(ServiceError::invalid_operation(
                    "cannot dispatch follow-up for a passed CI-only review",
                ));
            }
            ::review::ReviewOutcome::AwaitingHuman => {
                return Err(ServiceError::invalid_operation(
                    "cannot dispatch follow-up while review awaits a human decision",
                ));
            }
            ::review::ReviewOutcome::AuditorFailed { reason } => {
                let diff = self.best_effort_git_diff(task_id).await;
                (
                    "review_failed",
                    ::review::follow_up::render_review_fail_prompt(reason, &diff),
                )
            }
            ::review::ReviewOutcome::CiFailed { failing_steps } => (
                "ci_failed",
                ::review::follow_up::render_ci_fail_prompt(failing_steps),
            ),
            ::review::ReviewOutcome::MergeConflict {
                conflict_paths,
                conflict_summary,
            } => (
                "merge_failed",
                ::review::follow_up::render_merge_conflict_prompt(conflict_paths, conflict_summary),
            ),
        };
        dispatch_role_follow_up_impl(
            self.clone(),
            task_id.to_owned(),
            crate::workflow::default_roles::CODER.to_owned(),
            parent_execution_id,
            prompt,
            trigger.to_owned(),
            None,
        )
        .await
    }

    pub async fn dispatch_role_follow_up(
        &self,
        task_id: &str,
        role: &str,
        parent_execution_id: String,
        prompt: String,
        trigger: &str,
    ) -> Result<Execution> {
        dispatch_role_follow_up_impl(
            self.clone(),
            task_id.to_owned(),
            role.to_owned(),
            parent_execution_id,
            prompt,
            trigger.to_owned(),
            None,
        )
        .await
    }

    pub async fn dispatch_role_follow_up_with_agent(
        &self,
        task_id: &str,
        role: &str,
        parent_execution_id: String,
        agent_id: String,
        prompt: String,
        trigger: &str,
    ) -> Result<Execution> {
        dispatch_role_follow_up_impl(
            self.clone(),
            task_id.to_owned(),
            role.to_owned(),
            parent_execution_id,
            prompt,
            trigger.to_owned(),
            Some(agent_id),
        )
        .await
    }

    async fn active_state_for_role(&self, task: &Task, role: &str) -> Result<String> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = crate::workflow::engine::WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Dispatch),
        );
        if workflow
            .states
            .iter()
            .any(|state| state.name == task.status && state.role.as_deref() == Some(role))
        {
            return Ok(task.status.clone());
        }
        Ok(workflow
            .states
            .iter()
            .find(|state| {
                state.kind == api_types::StateKind::Active && state.role.as_deref() == Some(role)
            })
            .or_else(|| {
                workflow
                    .states
                    .iter()
                    .find(|state| state.role.as_deref() == Some(role))
            })
            .map(|state| state.name.clone())
            .unwrap_or_else(|| crate::workflow::default_states::IN_PROGRESS.to_owned()))
    }
}

fn dispatch_role_follow_up_impl(
    service: TaskService,
    task_id: String,
    role: String,
    parent_execution_id: String,
    prompt: String,
    trigger: String,
    agent_override: Option<String>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Execution>> + Send>> {
    Box::pin(async move {
        validate_required("task_id", &task_id)?;
        validate_required("role", &role)?;
        validate_required("parent_execution_id", &parent_execution_id)?;

        let supplied_parent_execution =
            ExecutionRepo::get_by_id(&*service.db, &parent_execution_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("execution", parent_execution_id.clone()))?;
        let task = TaskRepo::get_by_id(&*service.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        let role_parent = if execution_role_matches(&supplied_parent_execution, &role) {
            Some(supplied_parent_execution.clone())
        } else {
            latest_terminal_execution_for_follow_up_role(&service.db, &task_id, &role).await?
        };
        let lineage_parent = role_parent.as_ref().unwrap_or(&supplied_parent_execution);
        let agent_id = agent_override
            .or(assigned_agent_for_follow_up(&service, &task_id, &role).await?)
            .or_else(|| lineage_parent.agent_id.clone())
            .or_else(|| supplied_parent_execution.agent_id.clone())
            .ok_or_else(|| {
                ServiceError::invalid_operation(format!(
                    "no assigned agent available for follow-up role {role}"
                ))
            })?;
        let agent = AgentRepo::get_by_id(&*service.db, &agent_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent", agent_id.clone()))?;
        let executor_config_snapshot_json = if role_parent.is_some()
            && lineage_parent.agent_id.as_deref() == Some(agent_id.as_str())
            && lineage_parent.agent_session_id.is_some()
        {
            let agent_session_id = lineage_parent
                .agent_session_id
                .as_deref()
                .expect("checked agent_session_id exists");
            let snapshot_json = lineage_parent
                .executor_config_snapshot_json
                .as_deref()
                .ok_or_else(|| {
                    ServiceError::invalid_operation(format!(
                        "parent execution {} missing executor config snapshot",
                        lineage_parent.id
                    ))
                })?;
            Some(executor_snapshot_with_resume_thread(
                snapshot_json,
                agent_session_id,
            )?)
        } else {
            build_executor_config_snapshot(&service.db, &task, &agent, None).await?
        };
        let execution_id = new_uuid_v4();
        let logs_path = execution_logs_path(
            &service.workspace_root,
            &task.project_id,
            &task_id,
            &execution_id,
        );
        // Establish the final Task state/version before minting the
        // execution-scoped WorkspaceLease. A transition after issuance would
        // immediately make the exact-version authority stale.
        let active_state = service.active_state_for_role(&task, &role).await?;
        let task = if task.status != active_state {
            service
                .transition(task_id.clone(), active_state, task.version)
                .await?
                .task
        } else if task.error_annotation.is_some() {
            match TaskRepo::update(
                &*service.db,
                UpdateTask {
                    id: task.id.clone(),
                    expected_version: task.version,
                    error_annotation: Some(None),
                    updated_at: now_rfc3339(),
                    title: None,
                    description: None,
                    priority: None,
                    merge_config: None,
                    plan: None,
                    blocked_json: None,
                    failed_json: None,
                    task_state_config: None,
                    parent_task_id: None,
                },
            )
            .await
            {
                Ok(updated) => updated,
                Err(error) => {
                    tracing::warn!(%error, task_id = %task_id, "failed to clear error annotation before follow-up dispatch");
                    TaskRepo::get_by_id(&*service.db, &task_id, false)
                        .await?
                        .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?
                }
            }
        } else {
            task
        };
        service.ensure_task_runnable(&task).await?;
        let now = now_rfc3339();
        let execution = service
            .create_running_execution(
                CreateExecution {
                    id: execution_id.clone(),
                    task_id: task_id.clone(),
                    agent_id: Some(agent_id),
                    role: role.clone(),
                    status: ExecutionStatus::Running,
                    stop_reason: None,
                    stopped_by: None,
                    resume_policy: None,
                    stopped_at: None,
                    parent_execution_id: Some(lineage_parent.id.clone()),
                    agent_session_id: None,
                    agent_message_id: None,
                    last_activity_at: None,
                    summary: Some(prompt),
                    logs_path: Some(logs_path),
                    before_sha: None,
                    after_sha: None,
                    error: None,
                    executor_config_snapshot_json,
                    workspace_id: lineage_parent.workspace_id.clone(),
                    created_at: now.clone(),
                    updated_at: now,
                },
                false,
            )
            .await?;

        tracing::info!(
            task_id = %task_id,
            role = %role,
            execution_id = %execution.id,
            parent_execution_id = %lineage_parent.id,
            trigger = %trigger,
            "role follow-up dispatched"
        );

        service.publish(ForgeEvent {
            event_type: "follow_up.dispatched".to_owned(),
            entity_id: task_id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::FollowUpDispatched {
                task_id: task_id.clone(),
                parent_execution_id: lineage_parent.id.clone(),
                execution_id: execution.id.clone(),
                trigger: trigger.clone(),
            },
        });

        service.start_execution(execution.id.clone()).await?;

        Ok(execution)
    })
}

async fn assigned_agent_for_follow_up(
    service: &TaskService,
    task_id: &str,
    role: &str,
) -> Result<Option<String>> {
    let assignment =
        TaskRoleAssignmentRepo::get_by_task_and_role(&*service.db, task_id, role).await?;
    Ok(assignment.and_then(|assignment| {
        (assignment.assignee_type == Some(AssigneeKind::Agent))
            .then_some(assignment.assignee_id)
            .flatten()
    }))
}

async fn latest_terminal_execution_for_follow_up_role(
    db: &SqliteDb,
    task_id: &str,
    role: &str,
) -> Result<Option<Execution>> {
    if let Some(execution) = latest_terminal_execution_for_exact_role(db, task_id, role).await? {
        return Ok(Some(execution));
    }
    if role == crate::workflow::default_roles::CODER {
        return latest_terminal_execution_for_exact_role(db, task_id, "executor").await;
    }
    Ok(None)
}

async fn latest_terminal_execution_for_exact_role(
    db: &SqliteDb,
    task_id: &str,
    role: &str,
) -> Result<Option<Execution>> {
    let page = ExecutionRepo::list_by_task_and_role(
        db,
        task_id,
        role,
        PageRequest {
            cursor: None,
            limit: 20,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await?;
    Ok(page
        .items
        .into_iter()
        .find(|execution| execution.status != ExecutionStatus::Running))
}

fn execution_role_matches(execution: &Execution, role: &str) -> bool {
    execution.role == role
        || (role == crate::workflow::default_roles::CODER && execution.role == "executor")
}
