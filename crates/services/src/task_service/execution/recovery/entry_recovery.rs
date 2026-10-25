//! Entry check retries, overrides, and manual integration repair.

use super::*;

impl TaskService {
    pub(crate) async fn retry_entry_checks(
        &self,
        task: Task,
        reason: Option<String>,
    ) -> Result<Task> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let actor = if self
            .parse_blocking_annotation(&task)
            .is_some_and(|annotation| annotation.blocking_reason == "review_ci_infrastructure")
        {
            api_types::Actor::system(api_types::SystemComponent::TaskDispatcher)
        } else {
            TaskService::task_action_actor(api_types::Actor::user(
                api_types::UserActionSource::Action(api_types::TaskAction::retry()),
            ))
        };
        let workflow =
            WorkflowEngine::resolve_workflow_for_task(&task, &project.workflow_definition, &actor);
        let engine = self.workflow_execution();
        let recovered = engine
            .retry_entry_barrier_with_authority(
                &task.id,
                task.version,
                &workflow,
                &actor,
                reason.as_deref().unwrap_or("retry"),
                crate::workflow::engine::WorkflowAuthority {
                    project_version: project.version,
                    workflow_definition: project.workflow_definition.clone(),
                    clear_review_passed_at_on_commit: false,
                },
            )
            .await?
            .task;
        self.publish(ForgeEvent {
            event_type: "task.recovery_action".to_owned(),
            entity_id: recovered.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskRecovered {
                project_id: recovered.project_id.clone(),
                reason: reason.unwrap_or_else(|| "retry".to_owned()),
            },
        });
        if recovered
            .entry_barrier_json
            .as_deref()
            .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
            .is_none_or(|barrier| barrier["status"] != "blocked")
        {
            crate::placement::admission::resolve_review_ci_attention(&self.db, &recovered.id)
                .await?;
        }
        Ok(recovered)
    }

    pub(crate) async fn recover_retry_current_state_hooks(
        &self,
        task: Task,
        reason: Option<String>,
    ) -> Result<Task> {
        let reason = optional_recovery_reason(reason, "retry");
        super::ensure_plan_publication_transition_authority(&task, None)?;
        let cleared = self.clear_blocking_metadata(&task.id).await?;
        let project = ProjectRepo::get_by_id(&*self.db, &cleared.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", cleared.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &cleared,
            &project.workflow_definition,
            &TaskService::task_action_actor(api_types::Actor::user(
                api_types::UserActionSource::Action(api_types::TaskAction::retry()),
            )),
        );
        let engine = self.workflow_execution();
        let recovered = engine
            .manual_override_transition_with_authority(
                &cleared.id,
                &cleared.status,
                cleared.version,
                &workflow,
                TaskService::task_action_actor(api_types::Actor::user(
                    api_types::UserActionSource::Action(api_types::TaskAction::retry()),
                )),
                &reason,
                false,
                Some(crate::workflow::engine::WorkflowAuthority {
                    project_version: project.version,
                    workflow_definition: project.workflow_definition.clone(),
                    clear_review_passed_at_on_commit: false,
                }),
            )
            .await?
            .task;
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

    pub(crate) async fn recover_pull_request_merge_wait(
        &self,
        task: Task,
        reason: Option<String>,
    ) -> Result<Task> {
        let cleared = TaskRepo::mutate_metadata_and_bump_version(
            &*self.db,
            &task.id,
            task.version,
            vec![TaskMetadataMutation::CompareAndMutate {
                key: "awaiting_human_reason".to_owned(),
                expected: Value::String("pull_request_merge".to_owned()),
                mutations: vec![
                    TaskMetadataMutation::Remove {
                        key: "awaiting_human".to_owned(),
                    },
                    TaskMetadataMutation::Remove {
                        key: "awaiting_human_reason".to_owned(),
                    },
                    TaskMetadataMutation::Remove {
                        key: "awaiting_human_marker_id".to_owned(),
                    },
                ],
            }],
            &now_rfc3339(),
        )
        .await?;
        self.recover_retry_current_state_hooks(cleared, reason)
            .await
    }

    /// The owner, rather than a merge-fix execution, completed the repair or
    /// reopened a coordination root for corrective subtasks. Retain attempt
    /// lineage but give scheduling ownership back to the workflow.
    pub(crate) async fn handoff_manual_integration_repair(&self, task: &Task) -> Result<()> {
        if let Some(api_types::IntegrationReason::Repair { attempt_id, .. }) =
            task.condition.integration_reason()
        {
            if !task.condition.integration_handoff_ready() {
                let mut tx = db::begin_immediate(self.db.pool()).await?;
                self.db
                    .state_integration_condition_in_tx(
                        &mut tx,
                        &task.id,
                        &db::ConditionStatement::IntegrationHandedOff {
                            attempt_id: attempt_id.clone(),
                        },
                    )
                    .await?;
                tx.commit().await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn recover_manual_merge_repair_for_review(
        &self,
        task: Task,
        reason: Option<String>,
    ) -> Result<Task> {
        let recovery_reason = optional_recovery_reason(reason, "manual merge repair completed");
        // Validate the complete route before mutating the Task. The transition
        // clears blocked_json atomically with the status change, and the normal
        // transition cleanup clears the transient annotation and stale review
        // authority. A custom/invalid workflow therefore keeps the original
        // manual-repair blocker instead of losing its only recovery evidence.
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let actor = api_types::Actor::system(api_types::SystemComponent::Workflow);
        let workflow =
            WorkflowEngine::resolve_workflow_for_task(&task, &project.workflow_definition, &actor);
        let review_state = workflow
            .states
            .iter()
            .find(|state| state.canonical_phase == Some(api_types::CanonicalPhase::Review))
            .map(|state| state.name.clone())
            .ok_or_else(|| {
                ServiceError::invalid_operation("manual merge repair has no review state")
            })?;
        let reject_target = workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
            .and_then(|state| state.gate_config.as_ref())
            .and_then(|gate| gate.reject_target.clone())
            .ok_or_else(|| {
                ServiceError::invalid_operation("manual merge repair has no reject route")
            })?;
        self.handoff_manual_integration_repair(&task).await?;
        let recovered = self
            .transition_recovery_rejection(
                &task,
                reject_target,
                format!("{recovery_reason}; fresh review required"),
                &actor,
                api_types::TransitionBridge::new(api_types::TransitionBridgeKind::ReviewRefresh),
            )
            .await?;
        // The command defers execution hooks, including the repair bridge's
        // worker dispatch. Complete its mechanical move to fresh review here.
        let recovered = if recovered.status != review_state {
            self.transition(
                recovered.id.clone(),
                review_state,
                TransitionOptions {
                    bridge: api_types::TransitionBridge::new(
                        api_types::TransitionBridgeKind::ReviewRefresh,
                    ),
                    version: recovered.version,
                    reason: Some("Fresh review after manual repair".to_owned()),
                    triggered_by: actor.clone(),
                    rejection: false,
                    defer_dispatch_seconds: None,
                },
            )
            .await?
            .task
        } else {
            recovered
        };
        self.publish(ForgeEvent {
            event_type: "task.recovery_action".to_owned(),
            entity_id: recovered.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskRecovered {
                project_id: recovered.project_id.clone(),
                reason: recovery_reason,
            },
        });
        Ok(recovered)
    }

    pub(crate) async fn retry_review_entry(
        &self,
        task: Task,
        reason: Option<String>,
    ) -> Result<Task> {
        self.recover_retry_current_state_hooks(task, reason).await
    }

    pub(crate) async fn refresh_workspace_and_retry_entry(
        &self,
        task: Task,
        reason: Option<String>,
    ) -> Result<Task> {
        let workspace = super::super::workspace::prepare_workspace(
            &self.db,
            &self.workspace_root,
            &task,
            &task.id,
            self.repo_cache_locks.clone(),
            &self.workspace_backend_router,
        )
        .await?;
        let repo = RepoRepo::get_by_id(&*self.db, &workspace.repo_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("repo", workspace.repo_id.clone()))?;
        let target_branch = default_target_branch(&repo.default_branch);
        let resolved = self
            .workspace_backend_router
            .resolve(&self.db, &workspace)
            .await?;
        let status = resolved
            .git_query(api_types::WorkspaceGitQuery::StatusPorcelain, false)
            .await?
            .ok_or_else(|| ServiceError::invalid_operation("owner returned no workspace status"))?;
        if !status.trim().is_empty() {
            let files = status.lines().collect::<Vec<_>>().join(", ");
            return Err(ServiceError::invalid_operation(format!(
                "cannot update workspace before retrying hook because the worktree is dirty: {files}"
            )));
        }

        match resolved.rebase_target(&target_branch, false).await? {
            api_types::WorkspaceOwnerOperationOutcome::Rebased => {
                resolved.record_head_best_effort(&self.db).await;
                tracing::info!(
                    task_id = %task.id,
                    workspace_id = %workspace.id,
                    target_branch = %target_branch,
                    "workspace updated before retrying hook"
                );
            }
            api_types::WorkspaceOwnerOperationOutcome::Conflict { details, .. }
            | api_types::WorkspaceOwnerOperationOutcome::UnsupportedConflict { details } => {
                return Err(ServiceError::invalid_operation(format!(
                    "cannot update workspace before retrying hook because rebase onto {target_branch} conflicted: {details}"
                )));
            }
            api_types::WorkspaceOwnerOperationOutcome::Dirty { files } => {
                return Err(ServiceError::invalid_operation(format!(
                    "cannot update workspace before retrying hook because the worktree is dirty: {}",
                    files.join(", ")
                )));
            }
            _ => {
                return Err(ServiceError::invalid_operation(
                    "owner returned an invalid rebase result",
                ))
            }
        }

        if task.condition.read().entry_recorded {
            self.retry_entry_checks(task, reason).await
        } else {
            self.recover_retry_current_state_hooks(task, reason).await
        }
    }

    pub(crate) async fn override_entry_checks_once(
        &self,
        task: Task,
        reason: Option<String>,
        recovery_marker: Option<db::CreateTransitionLog>,
    ) -> Result<Task> {
        tracing::info!(
            task_id = %task.id,
            reason = %reason.clone().unwrap_or_else(|| "approve".to_owned()),
            "entry-check override recorded"
        );
        let recovered = if task.condition.read().entry_recorded {
            let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
            let workflow = WorkflowEngine::resolve_workflow_for_task(
                &task,
                &project.workflow_definition,
                &TaskService::task_action_actor(api_types::Actor::user(
                    api_types::UserActionSource::Action(api_types::TaskAction::Approve {
                        reason: None,
                        override_checks: Some(true),
                    }),
                )),
            );
            let barrier_state = task
                .entry_barrier_json
                .as_deref()
                .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
                .and_then(|barrier| {
                    barrier
                        .get("state")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                })
                .unwrap_or_else(|| task.status.clone());
            let skip_config = serde_json::json!({
                barrier_state.clone(): {
                    "skip_before_work_hook_once": true
                }
            });
            let update = UpdateTask {
                id: task.id.clone(),
                expected_version: task.version,
                title: None,
                description: None,
                priority: None,
                merge_config: None,
                plan: None,
                error_annotation: None,
                blocked_json: None,
                failed_json: None,
                task_state_config: Some(Some(skip_config.to_string())),
                parent_task_id: None,
                updated_at: now_rfc3339(),
            };
            let updated = if let Some(marker) = recovery_marker.as_ref() {
                TaskRepo::update_with_workflow_authority_and_recovery_marker(
                    &*self.db,
                    update,
                    marker.clone(),
                    project.version,
                    project.workflow_definition.clone(),
                )
                .await?
            } else {
                TaskRepo::update_with_workflow_authority(
                    &*self.db,
                    update,
                    project.version,
                    project.workflow_definition.clone(),
                )
                .await?
            };
            let engine = self.workflow_execution();
            let recovered = engine
                .retry_entry_barrier_with_authority(
                    &updated.id,
                    updated.version,
                    &workflow,
                    &TaskService::task_action_actor(api_types::Actor::user(
                        api_types::UserActionSource::Action(api_types::TaskAction::Approve {
                            reason: None,
                            override_checks: Some(true),
                        }),
                    )),
                    reason.as_deref().unwrap_or("approve"),
                    crate::workflow::engine::WorkflowAuthority {
                        project_version: project.version,
                        workflow_definition: project.workflow_definition.clone(),
                        clear_review_passed_at_on_commit: false,
                    },
                )
                .await?
                .task;
            clear_skip_before_work_hook_once_override(&self.db, &recovered, &barrier_state).await?
        } else {
            let mut config = task
                .task_state_config
                .as_deref()
                .map(serde_json::from_str::<Value>)
                .transpose()
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?
                .unwrap_or_else(|| json!({}));
            config[&task.status]["skip_before_work_hook_once"] = json!(true);
            let updated = TaskRepo::update(
                &*self.db,
                UpdateTask {
                    id: task.id.clone(),
                    expected_version: task.version,
                    title: None,
                    description: None,
                    priority: None,
                    merge_config: None,
                    plan: None,
                    error_annotation: Some(None),
                    blocked_json: Some(None),
                    failed_json: None,
                    task_state_config: Some(Some(config.to_string())),
                    parent_task_id: None,
                    updated_at: now_rfc3339(),
                },
            )
            .await?;
            let recovered = self
                .recover_retry_current_state_hooks(updated, reason.clone())
                .await?;
            clear_skip_before_work_hook_once_override(&self.db, &recovered, &task.status).await?
        };
        self.publish(ForgeEvent {
            event_type: "task.recovery_action".to_owned(),
            entity_id: recovered.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskRecovered {
                project_id: recovered.project_id.clone(),
                reason: reason.unwrap_or_else(|| "approve".to_owned()),
            },
        });
        Ok(recovered)
    }
}
