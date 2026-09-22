use super::*;

impl TaskService {
    pub async fn dispatch_initial_role_execution(
        &self,
        task_id: &str,
        agent_id: &str,
        role: &str,
        prompt: String,
    ) -> Result<Execution> {
        self.dispatch_initial_role_execution_with_metadata(task_id, agent_id, role, prompt, None)
            .await
    }

    pub async fn dispatch_initial_role_execution_with_metadata(
        &self,
        task_id: &str,
        agent_id: &str,
        role: &str,
        prompt: String,
        dispatch_metadata: Option<Value>,
    ) -> Result<Execution> {
        self.dispatch_initial_role_execution_with_optional_admission(
            task_id,
            agent_id,
            role,
            prompt,
            dispatch_metadata,
            None,
        )
        .await
    }

    pub(crate) async fn dispatch_initial_role_execution_with_metadata_and_admission(
        &self,
        task_id: &str,
        agent_id: &str,
        role: &str,
        prompt: String,
        dispatch_metadata: Option<Value>,
        admission: db::ExecutionAdmission,
    ) -> Result<Execution> {
        self.dispatch_initial_role_execution_with_optional_admission(
            task_id,
            agent_id,
            role,
            prompt,
            dispatch_metadata,
            Some(admission),
        )
        .await
    }

    async fn dispatch_initial_role_execution_with_optional_admission(
        &self,
        task_id: &str,
        agent_id: &str,
        role: &str,
        prompt: String,
        dispatch_metadata: Option<Value>,
        admission: Option<db::ExecutionAdmission>,
    ) -> Result<Execution> {
        validate_required("task_id", task_id)?;
        validate_required("agent_id", agent_id)?;
        validate_required("role", role)?;

        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        let agent = AgentRepo::get_by_id(&*self.db, agent_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent", agent_id.to_owned()))?;
        // Capture the admission facts from the same snapshot that selects the
        // role, prompt, and executor config.  Deriving these after workspace
        // preparation would allow a Task edit with the same status/role to
        // mint an execution carrying stale prompt/config data.
        let admission = match admission {
            Some(admission) => admission,
            None if role == crate::workflow::default_roles::REVIEWER => {
                return Err(ServiceError::conflict(
                    "reviewer dispatch requires a review-bound execution admission",
                ));
            }
            None => {
                let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
                let admission = crate::task_service::execution_admission_for_task(
                    &self.db,
                    &task,
                    &project.workflow_definition,
                    role,
                    Some(&agent),
                    project.version,
                )
                .await?;
                admission
            }
        };
        let reviewer_parent_execution_id = admission.expected_reviewer_parent_execution_id.clone();
        let coordination_root =
            super::super::subtask::coordination_root_has_subtasks(&self.db, &task).await?;
        self.ensure_ordered_execution_admission(&task, role).await?;
        if coordination_root || role == crate::workflow::default_roles::REVIEWER {
            self.ensure_task_reviewable(&task).await?;
        } else {
            self.ensure_task_runnable(&task).await?;
        }
        self.ensure_no_running_repository_execution(&task).await?;
        self.check_dependency_gate(&task, agent_id).await?;
        let (workspace, workspace_created_by_attempt) =
            super::super::workspace::prepare_workspace_owned(
                &self.db,
                &self.workspace_root,
                &task,
                &task.id,
                self.repo_cache_locks.clone(),
            )
            .await?;
        let executor_config_snapshot_json = with_dispatch_metadata(
            build_executor_config_snapshot(&self.db, &task, &agent, None).await?,
            dispatch_metadata,
        )?;
        let now = now_rfc3339();
        let create_input = CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent.id.clone()),
            role: role.to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: reviewer_parent_execution_id,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some(prompt),
            logs_path: None,
            before_sha: workspace.before_sha.clone(),
            after_sha: None,
            error: None,
            executor_config_snapshot_json,
            workspace_id: Some(workspace.id.clone()),
            created_at: now.clone(),
            updated_at: now,
        };
        let execution = self
            .create_running_execution_with_admission(
                create_input,
                workspace_created_by_attempt,
                Some(admission),
            )
            .await?;

        tracing::info!(
            task_id = %task.id,
            agent_id = %agent.id,
            role = %role,
            execution_id = %execution.id,
            "initial role execution dispatched"
        );

        self.publish(ForgeEvent {
            event_type: "task.execution_launched".to_owned(),
            entity_id: task.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskAssigned {
                project_id: task.project_id.clone(),
                agent_id: agent.id.clone(),
                execution_id: execution.id.clone(),
            },
        });

        self.start_execution(execution.id.clone()).await?;

        Ok(execution)
    }

    pub async fn launch_execution(
        &self,
        task_id: impl Into<String>,
        agent_id: impl Into<String>,
        summary: Option<String>,
        overrides: Option<ExecutionOverrides>,
    ) -> Result<LaunchExecutionResult> {
        let task_id = task_id.into();
        let agent_id = agent_id.into();
        validate_required("task_id", &task_id)?;
        validate_required("agent_id", &agent_id)?;

        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        if super::super::subtask::coordination_root_has_subtasks(&self.db, &task).await? {
            return Err(ServiceError::invalid_operation(
                "root tasks with subtasks are coordination containers; launch a subtask instead",
            ));
        }
        self.ensure_ordered_execution_admission(&task, "interactive")
            .await?;
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Executor),
        );
        if workflow.state_kind(&task.status) == Some(api_types::StateKind::Terminal) {
            return Err(ServiceError::invalid_operation(format!(
                "cannot launch execution for task {} in terminal status {}",
                task.id, task.status
            )));
        }
        self.ensure_no_running_repository_execution(&task).await?;
        let task = match workflow.state_kind(&task.status) {
            Some(api_types::StateKind::Initial | api_types::StateKind::Custom) => {
                if let Some(target) = first_launch_target(&workflow, &task.status) {
                    self.transition(task.id.clone(), target, task.version)
                        .await?
                        .task
                } else {
                    task
                }
            }
            _ => task,
        };
        self.ensure_task_runnable(&task).await?;
        self.check_dependency_gate(&task, &agent_id).await?;
        self.ensure_no_running_interactive_execution(&task.id)
            .await?;
        let agent = AgentRepo::get_by_id(&*self.db, &agent_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent", agent_id.clone()))?;
        let admission = crate::task_service::execution_admission_for_task(
            &self.db,
            &task,
            "",
            crate::workflow::default_roles::INTERACTIVE,
            Some(&agent),
            project.version,
        )
        .await?;
        let (workspace, workspace_created_by_attempt) =
            super::super::workspace::prepare_workspace_owned(
                &self.db,
                &self.workspace_root,
                &task,
                &task_id,
                self.repo_cache_locks.clone(),
            )
            .await?;
        self.run_blocking_before_work_preflight(&task, &project, &workspace, Some(&agent_id), None)
            .await?;
        let executor_config_snapshot_json =
            build_executor_config_snapshot(&self.db, &task, &agent, overrides).await?;
        let now = now_rfc3339();
        let execution = self
            .create_running_execution_with_admission(
                CreateExecution {
                    id: new_uuid_v4(),
                    task_id: task.id.clone(),
                    agent_id: Some(agent.id.clone()),
                    role: "interactive".to_owned(),
                    status: ExecutionStatus::Running,
                    stop_reason: None,
                    stopped_by: None,
                    resume_policy: None,
                    stopped_at: None,
                    parent_execution_id: None,
                    agent_session_id: None,
                    agent_message_id: None,
                    last_activity_at: None,
                    summary,
                    logs_path: None,
                    before_sha: workspace.before_sha.clone(),
                    after_sha: None,
                    error: None,
                    executor_config_snapshot_json,
                    workspace_id: Some(workspace.id.clone()),
                    created_at: now.clone(),
                    updated_at: now,
                },
                workspace_created_by_attempt,
                Some(admission),
            )
            .await?;

        tracing::info!(
            task_id = %task.id,
            agent_id = %agent.id,
            execution_id = %execution.id,
            role = "interactive",
            "execution launched"
        );

        self.publish(ForgeEvent {
            event_type: "task.execution_launched".to_owned(),
            entity_id: task.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskAssigned {
                project_id: task.project_id.clone(),
                agent_id,
                execution_id: execution.id.clone(),
            },
        });

        Ok(LaunchExecutionResult {
            task,
            execution,
            workspace,
        })
    }

    pub async fn follow_up_execution(
        &self,
        parent_execution_id: impl Into<String>,
        message: String,
        agent_id: Option<String>,
        overrides: Option<ExecutionOverrides>,
    ) -> Result<LaunchExecutionResult> {
        self.follow_up_execution_with_role(parent_execution_id, message, agent_id, overrides, None)
            .await
    }

    /// Launch a genuinely interactive session follow-up. Public transport
    /// callers use this explicit form; workflow recovery uses
    /// [`Self::follow_up_execution`] so a coder/reviewer role is preserved.
    pub async fn follow_up_interactive_execution(
        &self,
        parent_execution_id: impl Into<String>,
        message: String,
        agent_id: Option<String>,
        overrides: Option<ExecutionOverrides>,
    ) -> Result<LaunchExecutionResult> {
        self.follow_up_execution_with_role(
            parent_execution_id,
            message,
            agent_id,
            overrides,
            Some(crate::workflow::default_roles::INTERACTIVE),
        )
        .await
    }

    async fn follow_up_execution_with_role(
        &self,
        parent_execution_id: impl Into<String>,
        message: String,
        agent_id: Option<String>,
        overrides: Option<ExecutionOverrides>,
        requested_role: Option<&str>,
    ) -> Result<LaunchExecutionResult> {
        let parent_execution_id = parent_execution_id.into();
        validate_required("parent_execution_id", &parent_execution_id)?;

        let parent_execution = ExecutionRepo::get_by_id(&*self.db, &parent_execution_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("execution", parent_execution_id.clone()))?;
        if !matches!(
            parent_execution.status,
            ExecutionStatus::Completed | ExecutionStatus::Failed | ExecutionStatus::Cancelled
        ) {
            return Err(ServiceError::invalid_operation(format!(
                "follow-up requires a completed, failed, or cancelled execution, got {}",
                parent_execution.status
            )));
        }
        let parent_agent_session_id =
            parent_execution.agent_session_id.clone().ok_or_else(|| {
                ServiceError::invalid_operation(
                    "parent execution has no resumable session (agent_session_id is null)",
                )
            })?;

        let task = TaskRepo::get_by_id(&*self.db, &parent_execution.task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", parent_execution.task_id.clone()))?;
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Executor),
        );
        // `interactive` and the historical `executor` transport role are
        // genuine user follow-ups. A workflow role (for example `coder` or
        // `reviewer`) must remain that role so resuming it can satisfy the
        // current state and participate in the normal cascade.
        let follow_up_role = requested_role.map(str::to_owned).unwrap_or_else(|| {
            match parent_execution.role.as_str() {
                "interactive" | "executor" => "interactive".to_owned(),
                role => role.to_owned(),
            }
        });
        self.ensure_ordered_execution_admission(&task, &follow_up_role)
            .await?;
        if workflow.state_kind(&task.status) == Some(api_types::StateKind::Terminal) {
            return Err(ServiceError::invalid_operation(format!(
                "cannot follow up on a task in terminal status {}",
                task.status
            )));
        }
        self.ensure_no_running_repository_execution(&task).await?;
        let task = match workflow.state_kind(&task.status) {
            Some(api_types::StateKind::Initial | api_types::StateKind::Custom) => {
                if let Some(target) = first_launch_target(&workflow, &task.status) {
                    self.transition(task.id.clone(), target, task.version)
                        .await?
                        .task
                } else {
                    task
                }
            }
            _ => task,
        };

        let resolved_agent_id = agent_id
            .or_else(|| parent_execution.agent_id.clone())
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "follow-up requires agent_id either in request or parent execution",
                )
            })?;
        let agent = AgentRepo::get_by_id(&*self.db, &resolved_agent_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent", resolved_agent_id.clone()))?;

        let parent_executor_type = parent_execution
            .executor_config_snapshot_json
            .as_deref()
            .ok_or_else(|| {
                ServiceError::invalid_operation("parent execution missing executor config snapshot")
            })
            .and_then(|snapshot_json| {
                serde_json::from_str::<Value>(snapshot_json).map_err(|error| {
                    ServiceError::invalid_operation(format!(
                        "invalid parent executor config snapshot: {error}"
                    ))
                })
            })?
            .get("executor_type")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ServiceError::invalid_operation("parent execution snapshot missing executor_type")
            })?
            .to_owned();
        if parent_executor_type != agent.executor_type {
            // A routed agent may legitimately have run its parent execution
            // on a cross-CLI fallback candidate; accept any executor family
            // present in the agent's configured route.
            let parent_family_routed = serde_json::from_str::<Value>(&agent.config_json)
                .ok()
                .and_then(|config| config.get(executors::FALLBACKS_CONFIG_KEY).cloned())
                .and_then(|fallbacks| fallbacks.as_array().cloned())
                .is_some_and(|entries| {
                    entries.iter().any(|entry| {
                        entry.get("executor_type").and_then(Value::as_str)
                            == Some(parent_executor_type.as_str())
                    })
                });
            if !parent_family_routed {
                return Err(ServiceError::invalid_operation(format!(
                    "follow-up requires same executor type: parent used '{}' but agent '{}' uses '{}'",
                    parent_executor_type, agent.id, agent.executor_type
                )));
            }
        }

        if follow_up_role == crate::workflow::default_roles::REVIEWER {
            self.ensure_task_reviewable(&task).await?;
        } else {
            self.ensure_task_runnable(&task).await?;
        }
        if follow_up_role == crate::workflow::default_roles::INTERACTIVE {
            self.ensure_no_running_interactive_execution(&task.id)
                .await?;
        }
        let reviewer_snapshot = if follow_up_role == crate::workflow::default_roles::REVIEWER {
            Some(
                ReviewRepo::list_by_task(&*self.db, &task.id)
                    .await?
                    .into_iter()
                    .max_by_key(|review| (review.attempt_number, review.id.clone()))
                    .ok_or_else(|| {
                        ServiceError::conflict(
                            "reviewer follow-up requires a current review candidate",
                        )
                    })?,
            )
        } else {
            None
        };
        let mut admission = crate::task_service::execution_admission_for_task(
            &self.db,
            &task,
            &project.workflow_definition,
            &follow_up_role,
            Some(&agent),
            project.version,
        )
        .await?;
        let durable_parent_execution_id = if let Some(review) = reviewer_snapshot.as_ref() {
            admission.expected_reviewer_parent_execution_id = Some(review.execution_id.clone());
            admission.expected_latest_review_candidate_execution_id =
                Some(review.execution_id.clone());
            admission.expected_reviewer_id = Some(review.id.clone());
            admission.expected_reviewer_attempt_number = Some(review.attempt_number);
            admission.expected_reviewer_status = Some(review.status.to_string());
            admission.expected_reviewer_updated_at = Some(review.updated_at.clone());
            admission.expected_reviewer_execution_id = review.reviewer_execution_id.clone();
            admission.expected_auditor_execution_id = review.auditor_execution_id.clone();
            review.execution_id.clone()
        } else {
            parent_execution_id.clone()
        };
        let (workspace, workspace_created_by_attempt) =
            super::super::workspace::prepare_workspace_owned(
                &self.db,
                &self.workspace_root,
                &task,
                &task.id,
                self.repo_cache_locks.clone(),
            )
            .await?;
        let mut executor_config_snapshot_json =
            build_executor_config_snapshot(&self.db, &task, &agent, overrides).await?;
        if let (Some(snapshot_json), Some(parent_snapshot_json)) = (
            executor_config_snapshot_json.as_deref(),
            parent_execution.executor_config_snapshot_json.as_deref(),
        ) {
            // Resume is candidate-identity-aware: the parent's winning
            // candidate is promoted when still routed; a candidate switch
            // starts a fresh session instead of replaying another
            // account's session id.
            executor_config_snapshot_json = Some(
                crate::task_service::config::executor_snapshot_with_sticky_resume(
                    snapshot_json,
                    parent_snapshot_json,
                    &parent_agent_session_id,
                )?,
            );
        }

        let now = now_rfc3339();
        let execution = self
            .create_running_execution_with_admission(
                CreateExecution {
                    id: new_uuid_v4(),
                    task_id: task.id.clone(),
                    agent_id: Some(resolved_agent_id.clone()),
                    role: follow_up_role.clone(),
                    status: ExecutionStatus::Running,
                    stop_reason: None,
                    stopped_by: None,
                    resume_policy: None,
                    stopped_at: None,
                    parent_execution_id: Some(durable_parent_execution_id.clone()),
                    agent_session_id: None,
                    agent_message_id: None,
                    last_activity_at: None,
                    summary: Some(message),
                    logs_path: None,
                    before_sha: workspace.before_sha.clone(),
                    after_sha: None,
                    error: None,
                    executor_config_snapshot_json,
                    workspace_id: Some(workspace.id.clone()),
                    created_at: now.clone(),
                    updated_at: now,
                },
                workspace_created_by_attempt,
                Some(admission),
            )
            .await?;

        tracing::info!(
            task_id = %task.id,
            agent_id = %resolved_agent_id,
            execution_id = %execution.id,
            parent_execution_id = %durable_parent_execution_id,
            role = %follow_up_role,
            "follow-up execution launched"
        );

        self.publish(ForgeEvent {
            event_type: "task.execution_launched".to_owned(),
            entity_id: task.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskAssigned {
                project_id: task.project_id.clone(),
                agent_id: resolved_agent_id,
                execution_id: execution.id.clone(),
            },
        });

        Ok(LaunchExecutionResult {
            task,
            execution,
            workspace,
        })
    }

    pub async fn re_execute_execution(
        &self,
        parent_execution_id: impl Into<String>,
    ) -> Result<LaunchExecutionResult> {
        self.re_execute_execution_with_context(parent_execution_id, None)
            .await
    }

    pub async fn re_execute_execution_with_context(
        &self,
        parent_execution_id: impl Into<String>,
        context: Option<String>,
    ) -> Result<LaunchExecutionResult> {
        self.re_execute_execution_with_context_inner(parent_execution_id, context, false)
            .await
    }

    pub(super) async fn re_execute_execution_for_recovery(
        &self,
        parent_execution_id: impl Into<String>,
        context: Option<String>,
    ) -> Result<LaunchExecutionResult> {
        self.re_execute_execution_with_context_inner(parent_execution_id, context, true)
            .await
    }

    async fn re_execute_execution_with_context_inner(
        &self,
        parent_execution_id: impl Into<String>,
        context: Option<String>,
        clear_recovery_metadata: bool,
    ) -> Result<LaunchExecutionResult> {
        let parent_execution_id = parent_execution_id.into();
        validate_required("parent_execution_id", &parent_execution_id)?;

        let parent_execution = ExecutionRepo::get_by_id(&*self.db, &parent_execution_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("execution", parent_execution_id.clone()))?;
        if !matches!(
            parent_execution.status,
            ExecutionStatus::Completed | ExecutionStatus::Failed | ExecutionStatus::Cancelled
        ) {
            return Err(ServiceError::invalid_operation(format!(
                "re-execute requires a completed, failed, or cancelled execution, got {}",
                parent_execution.status
            )));
        }

        let task = TaskRepo::get_by_id(&*self.db, &parent_execution.task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", parent_execution.task_id.clone()))?;
        // The role assignment owns the execution principal: the INSERT
        // transaction compares the launched Agent against that row. Carrying
        // the parent execution's Agent over made re-execute fail as a bare
        // version conflict once the role had been reassigned -- exactly the
        // case recovery exists for. An `auditor` runs under a separately
        // selected Agent beneath the reviewer's assignment and an
        // `interactive` execution owes no assignment, so both keep the parent
        // principal.
        let assignment_role = match parent_execution.role.as_str() {
            crate::workflow::default_roles::INTERACTIVE | "auditor" => None,
            "executor" => Some(crate::workflow::default_roles::CODER),
            role => Some(role),
        };
        let assigned_agent_id = match assignment_role {
            Some(role) => {
                super::follow_up::assigned_agent_for_role(self, &parent_execution.task_id, role)
                    .await?
            }
            None => None,
        };
        let agent_id = assigned_agent_id
            .or_else(|| parent_execution.agent_id.clone())
            .ok_or_else(|| {
                ServiceError::invalid_operation(format!(
                    "parent execution {} missing agent_id",
                    parent_execution.id
                ))
            })?;
        let agent = AgentRepo::get_by_id(&*self.db, &agent_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent", agent_id.clone()))?;

        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Executor),
        );
        self.ensure_ordered_execution_admission(&task, &parent_execution.role)
            .await?;
        // Verify the execution role matches the current state effective role for cascade eligibility
        let current_state = workflow.states.iter().find(|s| s.name == task.status);
        let effective_role = current_state.and_then(|s| {
            s.role.clone().or_else(|| {
                if s.kind == api_types::StateKind::Active {
                    Some("assignee".to_owned())
                } else {
                    None
                }
            })
        });
        if effective_role.as_deref() != Some(&parent_execution.role)
            && parent_execution.role != "interactive"
        {
            tracing::info!(
                task_id = %task.id,
                execution_role = %parent_execution.role,
                effective_role = ?effective_role,
                "re-execute role does not match current state effective role; execution will not cascade"
            );
        }
        if workflow.state_kind(&task.status) == Some(api_types::StateKind::Terminal) {
            return Err(ServiceError::invalid_operation(format!(
                "cannot re-execute a task in terminal status {}",
                task.status
            )));
        }

        let task = match workflow.state_kind(&task.status) {
            Some(api_types::StateKind::Initial | api_types::StateKind::Custom) => {
                if let Some(target) = first_launch_target(&workflow, &task.status) {
                    self.transition(task.id.clone(), target, task.version)
                        .await?
                        .task
                } else {
                    task
                }
            }
            _ => task,
        };
        if parent_execution.role == crate::workflow::default_roles::REVIEWER {
            self.ensure_task_reviewable(&task).await?;
        } else {
            self.ensure_task_runnable(&task).await?;
        }

        let original_recovery_task = clear_recovery_metadata.then(|| task.clone());
        // Recovery metadata is part of the Task revision used for this
        // launch. Clear it before deriving admission and before building the
        // prompt/config so every downstream artifact comes from one final
        // snapshot rather than a stale pre-clear read.
        let task = if clear_recovery_metadata {
            self.clear_recovery_metadata_at_version(&task).await?
        } else {
            task
        };
        // A reviewer execution binds the current Review attempt, which only
        // accepts an attempt still Running. Re-executing happens inside
        // `review`, where the transition hooks that open an attempt never
        // run, so a settled attempt has to be replaced here or the execution
        // INSERT rejects the launch as a bare version conflict. This must
        // precede the dispatch context, which snapshots the attempt.
        if parent_execution.role == crate::workflow::default_roles::REVIEWER {
            if let Err(error) = self.ensure_review_attempt_for_recovery(&task, &project).await {
                if let Some(original) = original_recovery_task.as_ref() {
                    self.restore_recovery_metadata_after_failed_resume(
                        &task,
                        original,
                        None,
                        &parent_execution.role,
                    )
                    .await;
                }
                return Err(error);
            }
        }
        let mut admission = match crate::task_service::execution_admission_for_task(
            &self.db,
            &task,
            &project.workflow_definition,
            &parent_execution.role,
            Some(&agent),
            project.version,
        )
        .await
        {
            Ok(admission) => admission,
            Err(error) => {
                if let Some(original) = original_recovery_task.as_ref() {
                    self.restore_recovery_metadata_after_failed_resume(
                        &task,
                        original,
                        None,
                        &parent_execution.role,
                    )
                    .await;
                }
                return Err(error);
            }
        };
        let (workspace, workspace_created_by_attempt) =
            match super::super::workspace::prepare_workspace_owned(
                &self.db,
                &self.workspace_root,
                &task,
                &task.id,
                self.repo_cache_locks.clone(),
            )
            .await
            {
                Ok(workspace) => workspace,
                Err(error) => {
                    if let Some(original) = original_recovery_task.as_ref() {
                        self.restore_recovery_metadata_after_failed_resume(
                            &task,
                            original,
                            None,
                            &parent_execution.role,
                        )
                        .await;
                    }
                    return Err(error);
                }
            };
        let executor_config_snapshot_json =
            match build_executor_config_snapshot(&self.db, &task, &agent, None).await {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    if let Some(original) = original_recovery_task.as_ref() {
                        self.restore_recovery_metadata_after_failed_resume(
                            &task,
                            original,
                            None,
                            &parent_execution.role,
                        )
                        .await;
                    }
                    return Err(error);
                }
            };
        let role_name = &parent_execution.role;
        let state = workflow
            .states
            .iter()
            .find(|state| state.name == task.status);
        let state_config = state
            .map(|state| state.config.clone())
            .unwrap_or_else(|| serde_json::Value::Object(Default::default()));
        let state_dispatch =
            dispatch_intent_from_workflow_dispatch(state.and_then(|state| state.dispatch.as_ref()));
        let selection = effective_prompt_selection(role_name, None, state_dispatch.as_ref());
        let dispatch_ctx = match load_agent_dispatch_context(
            Arc::clone(&self.db),
            &task.id,
            role_name,
            &task.status,
            state_config,
            Some(selection.execution_policy.as_str()),
            &workflow,
        )
        .await
        {
            Ok(dispatch_ctx) => dispatch_ctx,
            Err(error) => {
                if let Some(original) = original_recovery_task.as_ref() {
                    self.restore_recovery_metadata_after_failed_resume(
                        &task,
                        original,
                        None,
                        &parent_execution.role,
                    )
                    .await;
                }
                return Err(error);
            }
        };
        let reviewer_snapshot = if parent_execution.role == crate::workflow::default_roles::REVIEWER
        {
            dispatch_ctx
                .prior_reviews
                .iter()
                .max_by_key(|review| (review.attempt_number, review.id.clone()))
                .map(|review| {
                    (
                        review.id.clone(),
                        review.execution_id.clone(),
                        review.attempt_number,
                        review.status.to_string(),
                        review.updated_at.clone(),
                        review.reviewer_execution_id.clone(),
                        review.auditor_execution_id.clone(),
                    )
                })
        } else {
            None
        };
        if let Some((
            id,
            execution_id,
            attempt_number,
            status,
            updated_at,
            reviewer_execution_id,
            auditor_execution_id,
        )) = reviewer_snapshot
        {
            admission.expected_reviewer_parent_execution_id = Some(execution_id.clone());
            admission.expected_latest_review_candidate_execution_id = Some(execution_id);
            admission.expected_reviewer_id = Some(id);
            admission.expected_reviewer_attempt_number = Some(attempt_number);
            admission.expected_reviewer_status = Some(status);
            admission.expected_reviewer_updated_at = Some(updated_at);
            admission.expected_reviewer_execution_id = reviewer_execution_id;
            admission.expected_auditor_execution_id = auditor_execution_id;
        }
        let (prompt, _selection) =
            build_effective_prompt(&dispatch_ctx, None, state_dispatch.as_ref());
        let summary = prompt.execution_input(context.as_deref());
        let reviewer_parent_execution_id = admission.expected_reviewer_parent_execution_id.clone();
        let now = now_rfc3339();
        let execution = match self
            .create_running_execution_with_admission(
                CreateExecution {
                    id: new_uuid_v4(),
                    task_id: task.id.clone(),
                    agent_id: Some(agent.id.clone()),
                    role: parent_execution.role.clone(),
                    status: ExecutionStatus::Running,
                    stop_reason: None,
                    stopped_by: None,
                    resume_policy: None,
                    stopped_at: None,
                    parent_execution_id: reviewer_parent_execution_id,
                    agent_session_id: None,
                    agent_message_id: None,
                    last_activity_at: None,
                    summary: Some(summary),
                    logs_path: None,
                    before_sha: workspace.before_sha.clone(),
                    after_sha: None,
                    error: None,
                    executor_config_snapshot_json,
                    workspace_id: Some(workspace.id.clone()),
                    created_at: now.clone(),
                    updated_at: now,
                },
                workspace_created_by_attempt,
                Some(admission),
            )
            .await
        {
            Ok(execution) => execution,
            Err(error) => {
                if let Some(original) = original_recovery_task.as_ref() {
                    self.restore_recovery_metadata_after_failed_resume(
                        &task,
                        original,
                        None,
                        &parent_execution.role,
                    )
                    .await;
                }
                return Err(error);
            }
        };

        if clear_recovery_metadata {
            if let Err(error) = super::clear_execution_retry_metadata(&self.db, &task).await {
                tracing::warn!(
                    task_id = %task.id,
                    %error,
                    "failed to clear execution retry metadata after re-execute"
                );
            }
        }

        tracing::info!(
            task_id = %task.id,
            agent_id = %agent.id,
            execution_id = %execution.id,
            parent_execution_id = %parent_execution.id,
            role = %execution.role,
            "re-execute execution launched"
        );

        self.publish(ForgeEvent {
            event_type: "task.execution_launched".to_owned(),
            entity_id: task.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskAssigned {
                project_id: task.project_id.clone(),
                agent_id,
                execution_id: execution.id.clone(),
            },
        });

        Ok(LaunchExecutionResult {
            task,
            execution,
            workspace,
        })
    }

    pub async fn cancel_execution(
        &self,
        execution_id: impl Into<String>,
        reason: String,
    ) -> Result<Execution> {
        self.stop_execution_with_actor(
            execution_id,
            reason,
            api_types::Actor::user(api_types::UserActionSource::Api),
            "user_cancelled",
            "Execution stopped by user",
        )
        .await
    }

    pub async fn pause_execution(
        &self,
        execution_id: impl Into<String>,
        reason: String,
    ) -> Result<Execution> {
        self.stop_execution_with_actor(
            execution_id,
            reason,
            api_types::Actor::user(api_types::UserActionSource::Api),
            "user_paused",
            "Task paused by user",
        )
        .await
    }

    async fn stop_execution_with_actor(
        &self,
        execution_id: impl Into<String>,
        reason: String,
        actor: api_types::Actor,
        blocking_reason: &str,
        annotation_message: &str,
    ) -> Result<Execution> {
        let execution_id = execution_id.into();
        let execution = ExecutionRepo::get_by_id(&*self.db, &execution_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("execution", execution_id.clone()))?;
        if execution.status != ExecutionStatus::Running {
            return Err(ServiceError::invalid_operation(format!(
                "can only cancel a running execution, got {}",
                execution.status
            )));
        }
        // Keep the state/role snapshot from before terminalization. A Task
        // transition may race the execution CAS; in that case a late manual
        // stop must not attach its recovery annotation to the next workflow
        // state or role.
        let task_at_stop_request = TaskRepo::get_by_id(&*self.db, &execution.task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", execution.task_id.clone()))?;
        let expected_stop_status = task_at_stop_request.status.clone();
        let expected_stop_state_entry_token = self
            .state_entry_token_for_stop_snapshot(&task_at_stop_request)
            .await?;
        let (expected_stop_role, expected_workflow_definition) = self
            .effective_role_for_stop_snapshot(&task_at_stop_request)
            .await?;
        // Role assignments are a separate authority domain from Tasks. Keep
        // the exact assignment that selected this execution so a same-role
        // reassignment cannot inherit this stop's manual blocker while the
        // execution is being terminalized.
        let expected_stop_assignment =
            if execution.role == crate::workflow::default_roles::INTERACTIVE {
                None
            } else {
                self.role_assignment_for_stop_snapshot(
                    &task_at_stop_request,
                    expected_stop_role.as_deref(),
                )
                .await?
            };
        let now = now_rfc3339();
        let cancellation_committed = self
            .cancel_active_execution(
                &execution,
                &reason,
                db::StopReason::UserCancelled,
                &actor,
                db::ResumePolicy::Manual,
            )
            .await?;
        // A concurrent terminal winner already owns the durable execution
        // event, lease disposition, and any Task-side cascade.  Do not add a
        // second manual-stop annotation or recovery transition from this
        // stale cancellation request.
        if !cancellation_committed {
            let current = ExecutionRepo::get_by_id(&*self.db, &execution_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("execution", execution_id))?;
            if current.status == ExecutionStatus::Running {
                // Nothing won the terminal CAS, so this stop never reached the
                // row. Returning it as an ordinary concurrent loss would answer
                // a stop request with 200 and a still-running execution; the
                // caller retries against the version it reads back instead.
                return Err(ServiceError::Db(DbError::VersionConflict));
            }
            return Ok(current);
        }
        let task = TaskRepo::get_by_id(&*self.db, &execution.task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", execution.task_id.clone()))?;
        let mut recovery_actions = vec![
            api_types::RecoveryAction::Reexecute,
            api_types::RecoveryAction::ResetToInitial,
            api_types::RecoveryAction::CancelTask,
        ];
        if self
            .resume_session_recovery_available(&task, &execution)
            .await?
        {
            recovery_actions.insert(0, api_types::RecoveryAction::ResumeSession);
        }
        let annotation = api_types::TaskBlockingAnnotation {
            annotation_type: api_types::FailureKind::ManualStop,
            blocking_reason: blocking_reason.to_owned(),
            blocked_by: Some(actor.display()),
            blocked_at: Some(now.clone()),
            blocked_execution_id: Some(execution.id.clone()),
            artifact: Some(api_types::BlockingArtifact {
                kind: "execution".to_owned(),
                id: Some(execution.id.clone()),
                log_path: None,
            }),
            message: Some(annotation_message.to_owned()),
            hook: None,
            recovery_actions,
        };
        let annotation = serde_json::to_string(&annotation).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "failed to serialize manual-stop annotation: {error}"
            ))
        })?;
        self.persist_manual_stop_annotation(
            &execution,
            &task_at_stop_request,
            &expected_stop_status,
            expected_stop_state_entry_token,
            expected_stop_role.as_deref(),
            &expected_workflow_definition,
            expected_stop_assignment,
            annotation,
            now,
        )
        .await?;
        ExecutionRepo::get_by_id(&*self.db, &execution_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("execution", execution_id))
    }

    /// Return the exact transition-log token for the current state entry.
    /// This uses the same `(created_at, insertion order)` ordering as action
    /// authority and the Task CAS. No matching log is the explicit initial/no-log epoch
    /// rather than an unknown value.
    async fn state_entry_token_for_stop_snapshot(&self, task: &Task) -> Result<Option<String>> {
        Ok(
            crate::task_service::action_resolver::latest_state_entry_authority(
                &self.db,
                &task.id,
                &task.status,
            )
            .await?
            .map(|entry| entry.id),
        )
    }

    async fn effective_role_for_stop_snapshot(
        &self,
        task: &Task,
    ) -> Result<(Option<String>, String)> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &api_types::Actor::user(api_types::UserActionSource::Api),
        );
        Ok((
            workflow
                .states
                .iter()
                .find(|state| state.name == task.status)
                .and_then(crate::workflow::effective_role)
                .map(str::to_owned),
            project.workflow_definition,
        ))
    }

    async fn role_assignment_for_stop_snapshot(
        &self,
        task: &Task,
        role: Option<&str>,
    ) -> Result<Option<TaskRoleAssignment>> {
        let Some(role) = role else {
            return Ok(None);
        };
        TaskRoleAssignmentRepo::get_by_task_and_role(&*self.db, &task.id, role)
            .await
            .map_err(Into::into)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn persist_manual_stop_annotation(
        &self,
        execution: &Execution,
        initial_task: &Task,
        expected_status: &str,
        expected_state_entry_token: Option<String>,
        expected_role: Option<&str>,
        expected_workflow_definition: &str,
        expected_assignment: Option<TaskRoleAssignment>,
        annotation: String,
        initial_updated_at: String,
    ) -> Result<()> {
        let initial_error_annotation = initial_task.error_annotation.clone();
        let mut candidate = initial_task.clone();
        let mut updated_at = initial_updated_at;
        for _ in 0..4 {
            if candidate.status != expected_status {
                tracing::debug!(
                    task_id = %candidate.id,
                    execution_id = %execution.id,
                    expected_status,
                    current_status = %candidate.status,
                    "skipping stale manual-stop annotation after Task transition"
                );
                return Ok(());
            }
            if candidate.error_annotation != initial_error_annotation {
                tracing::debug!(
                    task_id = %candidate.id,
                    execution_id = %execution.id,
                    "skipping stale manual-stop annotation after newer annotation"
                );
                return Ok(());
            }
            let (current_role, current_workflow_definition) =
                self.effective_role_for_stop_snapshot(&candidate).await?;
            let current_state_entry_token =
                self.state_entry_token_for_stop_snapshot(&candidate).await?;
            if current_role.as_deref() != expected_role
                || current_workflow_definition != expected_workflow_definition
                || current_state_entry_token != expected_state_entry_token
                || !stop_execution_role_matches(execution.role.as_str(), current_role.as_deref())
            {
                tracing::debug!(
                    task_id = %candidate.id,
                    execution_id = %execution.id,
                    execution_role = %execution.role,
                    expected_role = ?expected_role,
                    current_role = ?current_role,
                    "skipping stale manual-stop annotation after Task role change"
                );
                return Ok(());
            }
            let overlapping_roles = overlapping_execution_roles(execution.role.as_str());
            let expected_assignment_role = (execution.role
                != crate::workflow::default_roles::INTERACTIVE)
                .then_some(expected_role)
                .flatten();
            let current_assignment = self
                .role_assignment_for_stop_snapshot(&candidate, expected_assignment_role)
                .await?;
            if !role_assignment_snapshots_match(
                expected_assignment.as_ref(),
                current_assignment.as_ref(),
            ) {
                tracing::debug!(
                    task_id = %candidate.id,
                    execution_id = %execution.id,
                    execution_role = %execution.role,
                    expected_role = ?expected_role,
                    "skipping stale manual-stop annotation after role reassignment"
                );
                return Ok(());
            }
            match TaskRepo::set_error_annotation_if_no_running_execution(
                &*self.db,
                &candidate.id,
                candidate.version,
                expected_status,
                expected_state_entry_token.as_deref(),
                expected_workflow_definition,
                expected_assignment_role,
                expected_assignment.clone(),
                &annotation,
                &updated_at,
                &execution.id,
                execution.workspace_id.as_deref(),
                overlapping_roles,
            )
            .await
            {
                Ok(_) => return Ok(()),
                Err(db::DbError::ExecutionAlreadyRunning { .. }) => return Ok(()),
                Err(db::DbError::VersionConflict) => {
                    candidate = TaskRepo::get_by_id(&*self.db, &candidate.id, false)
                        .await?
                        .ok_or_else(|| ServiceError::not_found("task", candidate.id.clone()))?;
                    updated_at = now_rfc3339();
                }
                Err(error) => return Err(ServiceError::Db(error)),
            }
        }
        Err(ServiceError::Db(db::DbError::VersionConflict))
    }
}

fn stop_execution_role_matches(execution_role: &str, current_role: Option<&str>) -> bool {
    // Interactive executions are deliberately outside the workflow role
    // contract, but still belong to the current Task state. Their manual-stop
    // annotation must survive a concurrent Task write just like a role run.
    if execution_role == crate::workflow::default_roles::INTERACTIVE {
        return current_role.is_some();
    }
    current_role.is_some_and(|role| {
        execution_role == role
            || (role == crate::workflow::default_roles::CODER && execution_role == "executor")
    })
}

fn role_assignment_snapshots_match(
    expected: Option<&TaskRoleAssignment>,
    actual: Option<&TaskRoleAssignment>,
) -> bool {
    match (expected, actual) {
        (None, None) => true,
        (Some(expected), Some(actual)) => {
            expected.id == actual.id
                && expected.task_id == actual.task_id
                && expected.role_name == actual.role_name
                && expected.assignee_type == actual.assignee_type
                && expected.assignee_id == actual.assignee_id
                && expected.created_at == actual.created_at
                && expected.updated_at == actual.updated_at
        }
        _ => false,
    }
}

fn overlapping_execution_roles(role: &str) -> Vec<String> {
    // Manual-stop annotation is a Task/workspace boundary. A replacement in
    // any workflow role must win over a late stop, so the DB helper's empty
    // list deliberately means “all roles”. Keep the argument for the call
    // sites' role-specific intent/documentation.
    let _ = role;
    Vec::new()
}

fn with_dispatch_metadata(
    snapshot_json: Option<String>,
    dispatch_metadata: Option<Value>,
) -> Result<Option<String>> {
    let Some(snapshot_json) = snapshot_json else {
        return Ok(None);
    };
    let Some(dispatch_metadata) = dispatch_metadata else {
        return Ok(Some(snapshot_json));
    };
    let mut snapshot = serde_json::from_str::<Value>(&snapshot_json).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid executor config snapshot: {error}"))
    })?;
    let Some(snapshot_obj) = snapshot.as_object_mut() else {
        return Ok(Some(snapshot_json));
    };
    snapshot_obj.insert("dispatch".to_string(), dispatch_metadata);
    serde_json::to_string(&snapshot)
        .map(Some)
        .map_err(|error| ServiceError::invalid_operation(format!("invalid JSON snapshot: {error}")))
}

fn first_launch_target(workflow: &api_types::WorkflowDefinition, from: &str) -> Option<String> {
    let source_kind = workflow.state_kind(from);
    let targets = workflow
        .outgoing_trigger_targets(from)
        .filter(|(trigger, _)| {
            !trigger.system_only()
                || matches!(
                    source_kind,
                    Some(api_types::StateKind::Initial | api_types::StateKind::Custom)
                )
        })
        .filter_map(|(_, target)| workflow.state_kind(&target).map(|kind| (target, kind)))
        .collect::<Vec<_>>();

    targets
        .iter()
        .find(|(_, kind)| *kind == api_types::StateKind::Active)
        .or_else(|| {
            targets
                .iter()
                .find(|(_, kind)| *kind == api_types::StateKind::Gate)
        })
        .map(|(target, _)| target.clone())
}
