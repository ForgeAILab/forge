//! Execution provider selection and cancellation, owner launch parameters,
//! outbox ingestion, turn limits, and log paths.

use super::*;

impl TaskService {
    /// Apply entries supplied by the owner. Embedded turns harvest them from
    /// their backend; daemon turns supply the terminal report's entries.
    pub(crate) async fn ingest_execution_outbox_entries(
        &self,
        input: &crate::task_service::execution::ExecutionOutboxInput<'_>,
        entries: Vec<api_types::ExecutionOutboxEntry>,
    ) -> crate::task_service::execution::ExecutionOutboxReport {
        match self.credential_env.as_ref() {
            Some(embedded) => {
                embedded
                    .ingest_execution_outbox_entries(input, entries)
                    .await
            }
            None => crate::task_service::execution::ExecutionOutboxReport::default(),
        }
    }

    pub(in crate::task_service) async fn cancel_execution_with_provider(
        &self,
        execution: &Execution,
        reason: &str,
    ) -> Result<()> {
        let provider = self.execution_provider_for_execution(execution).await?;
        provider
            .cancel(api_types::ExecutionCancelParams {
                execution_id: execution.id.clone(),
                reason: Some(reason.to_owned()),
            })
            .await?;
        Ok(())
    }

    pub(in crate::task_service) async fn execution_provider_for_execution(
        &self,
        execution: &Execution,
    ) -> Result<Arc<dyn crate::daemon_transport::ExecutionProvider>> {
        let snapshot = execution
            .executor_config_snapshot_json
            .as_deref()
            .map(|snapshot| parse_json_value("executor config snapshot", snapshot))
            .transpose()?;
        let placement = match execution.workspace_id.as_deref() {
            Some(workspace_id) => Some(
                db::WorkspacePlacementRepo::get_by_workspace_id(&*self.db, workspace_id)
                    .await?
                    .ok_or_else(|| {
                        ServiceError::not_found("workspace placement", workspace_id.to_owned())
                    })?,
            ),
            None => None,
        };
        let fallback_agent = if execution.workspace_id.is_none() {
            match execution.agent_id.as_deref() {
                Some(id) => AgentRepo::get_by_id(&*self.db, id).await?,
                None => None,
            }
        } else {
            None
        };
        let daemon_id = match placement.as_ref() {
            Some(placement) => placement
                .execution_daemon_id
                .as_deref()
                .or(placement.daemon_id.as_deref()),
            None => snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.get("daemon_id"))
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .or_else(|| {
                    fallback_agent
                        .as_ref()
                        .and_then(|agent| agent.daemon_id.as_deref())
                }),
        };
        if placement
            .as_ref()
            .is_some_and(|placement| placement.state == db::PlacementState::Disconnected)
        {
            return Err(ServiceError::DaemonUnavailable {
                daemon_id: daemon_id.unwrap_or_default().to_owned(),
            });
        }
        if let Some(registry) = self.daemon_connections.as_ref() {
            return crate::daemon_transport::select_execution_provider(
                daemon_id, &self.db, registry,
            )
            .await
            .inspect_err(|error| {
                if let ServiceError::DaemonUnavailable { daemon_id } = error {
                    tracing::warn!(
                        execution_id = %execution.id,
                        daemon_id = %daemon_id,
                        agent_id = ?execution.agent_id,
                        "remote daemon unavailable for execution dispatch"
                    );
                }
            });
        }

        if let Some(daemon_id) = daemon_id {
            let daemon = db::DaemonRepo::get_by_id(&*self.db, daemon_id).await?;
            if !daemon.is_some_and(|daemon| {
                crate::embedded_daemon::is_embedded_daemon_machine(&daemon.machine_id)
            }) {
                return Err(ServiceError::DaemonUnavailable {
                    daemon_id: daemon_id.to_owned(),
                });
            }
        }
        let task_executor = self.task_executor.clone().ok_or_else(|| {
            ServiceError::invalid_operation(
                "task executor is not configured for execution dispatch",
            )
        })?;
        Ok(Arc::new(
            crate::daemon_transport::EmbeddedExecutionProvider::new(
                Arc::new(self.clone()),
                task_executor,
            ),
        ))
    }

    /// The provider's workspace argument at the placement boundary. A server
    /// worktree is checked by the workspace manager at this launch; a daemon
    /// path comes from the owner's retained preparation result.
    async fn owner_execution_path(
        &self,
        task: &Task,
        workspace: &Workspace,
        resolved: &ResolvedWorkspace,
    ) -> Result<String> {
        if resolved.placement.owner_kind == db::PlacementOwnerKind::Server {
            let valid = crate::workspace_manager::WorkspaceManager::new(
                &self.db,
                &self.workspace_root,
                self.repo_cache_locks.clone(),
                &self.workspace_backend_router,
            )
            // A worktree that vanished since the claim is made again here,
            // unless the disk is under its floor: that is the claim's disk
            // wait (the launch's caller records it), not a failed run.
            .waiting_for_disk()
            .ensure_valid(
                task,
                workspace.clone(),
                crate::workspace_manager::Purpose::Execute,
            )
            .await?;
            return valid
                .path()
                .map(|path| path.to_string_lossy().into_owned())
                .ok_or_else(|| ServiceError::invalid_operation("workspace has no server path"));
        }
        let placement = &resolved.placement;
        let daemon_id = placement
            .daemon_id
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("daemon placement has no owner"))?;
        // The backend durably retains the owner's prepare/reset result before
        // acknowledging it. Resolve the path from that result, fenced to this
        // handle and generation, without opening or inferring any server path.
        // Plan operations also use workspace.reset, but do not prepare a path.
        let receipt = self
            .db
            .daemon_workspace_preparation_receipt(
                &placement.task_id,
                &placement.id,
                placement.generation,
                daemon_id,
            )
            .await?
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "daemon placement has no retained preparation result",
                )
            })?;
        let receipt = parse_json_value("workspace preparation receipt", &receipt)?;
        prepared_owner_execution_path(placement, &receipt["owner_result"])
    }

    pub(super) async fn execution_start_params(
        &self,
        execution: &Execution,
    ) -> Result<api_types::ExecutionStartParams> {
        let task = TaskRepo::get_by_id(&*self.db, &execution.task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", execution.task_id.clone()))?;
        let workspace_id = execution
            .workspace_id
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("execution missing workspace_id"))?;
        let workspace = WorkspaceRepo::get_by_id(&*self.db, workspace_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", workspace_id.to_owned()))?;
        let resolved = self.resolve_task_workspace(&workspace).await?;
        let workspace_path = self
            .owner_execution_path(&task, &workspace, &resolved)
            .await?;
        let snapshot = execution
            .executor_config_snapshot_json
            .as_deref()
            .ok_or_else(|| {
                ServiceError::invalid_operation("execution missing executor config snapshot")
            })?;
        let mut executor_config = parse_json_value("executor config snapshot", snapshot)?;
        if let Some(config) = executor_config.as_object_mut() {
            config.remove("terminal_plan_text");
        }
        executor_config["_forge_plan_transport"] =
            json!(resolved.placement.owner_kind == db::PlacementOwnerKind::Daemon);
        if is_read_only_task_role(&execution.role)
            || matches!(task.task_type.as_str(), "planning_task" | "discovery")
        {
            executors::mark_worktree_read_only(&mut executor_config);
        }
        executors::mark_task_role(&mut executor_config, &execution.role);
        // A remote daemon receives the Project environment variables. Asset
        // staging and preflight checks run through the workspace owner before
        // launch (see `prepare_execution_environment`).
        let environment = self.project_environment(&task.project_id).await?;
        executors::environment::mark_task_environment(&mut executor_config, &environment.env);
        let executor_type = executor_config
            .get("executor_type")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ServiceError::invalid_operation("executor config snapshot missing executor_type")
            })?
            .to_owned();
        let description = ::review::contract::prepare_prompt(
            &self.db,
            &execution.id,
            &task.id,
            &resolved,
            matches!(execution.role.as_str(), "reviewer" | "auditor"),
            executor_type == "shell",
            execution_description(
                &execution.role,
                execution.summary.as_deref(),
                &task,
                &executor_config,
            ),
        )
        .await
        .map_err(ServiceError::invalid_operation)?;
        let max_turns = self.resolve_max_turns(&task).await?;

        let canonical = if resolved.placement.owner_kind == db::PlacementOwnerKind::Daemon
            && matches!(execution.role.as_str(), "worker" | "coder" | "executor")
        {
            crate::plan_artifact::read_plan_text_with_router(
                &self.db,
                &self.workspace_backend_router,
                workspace_id,
            )
            .await
            .map_err(|error| error.into_service_error("canonical plan artifact is unreadable"))?
        } else {
            None
        };
        let plan_text = executors::execution_plan_seed(
            Some(&execution.role),
            canonical.as_deref(),
            task.plan.as_deref(),
        )
        .map(str::to_owned);
        if resolved.placement.owner_kind == db::PlacementOwnerKind::Daemon {
            crate::plan_artifact::transport::remember_seed(
                &self.db,
                &execution.id,
                plan_text.as_deref(),
                &environment.env,
            )
            .await?;
        }
        Ok(api_types::ExecutionStartParams {
            plan_text: (resolved.placement.owner_kind == db::PlacementOwnerKind::Daemon)
                .then_some(plan_text)
                .flatten(),
            task_id: task.id.clone(),
            execution_id: execution.id.clone(),
            workspace_path,
            executor_type,
            executor_config,
            prompt: json!({ "description": description }),
            max_turns,
        })
    }
}

impl TaskService {
    pub(super) async fn annotate_max_turns_exceeded_block(
        &self,
        execution: &Execution,
        max_turns: Option<u32>,
    ) -> Result<()> {
        let task = TaskRepo::get_by_id(&*self.db, &execution.task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", execution.task_id.clone()))?;
        let message = match max_turns {
            Some(limit) => format!("Execution stopped after reaching max_turns={limit}"),
            None => "Execution stopped after reaching max_turns".to_owned(),
        };
        let annotation = api_types::TaskBlockingAnnotation {
            annotation_type: api_types::FailureKind::MaxTurnsExceeded,
            blocking_reason: "max_turns_exceeded".to_owned(),
            blocked_by: Some(
                api_types::Actor::system(api_types::SystemComponent::Executor).display(),
            ),
            blocked_at: Some(now_rfc3339()),
            blocked_execution_id: Some(execution.id.clone()),
            artifact: Some(api_types::BlockingArtifact {
                kind: "execution".to_owned(),
                id: Some(execution.id.clone()),
                log_path: execution.logs_path.clone(),
            }),
            message: Some(message.clone()),
            hook: None,
        };
        let blocked_meta = json!({
            "reason": message,
            "created_at": now_rfc3339(),
            "kind": "max_turns_exceeded",
            "execution_id": execution.id,
        });
        let updated = TaskRepo::update_status(
            &*self.db,
            db::UpdateTaskStatus {
                id: task.id.clone(),
                expected_version: task.version,
                status: task.status,
                assignee_id: None,
                error_annotation: Some(Some(serde_json::to_string(&annotation).map_err(
                    |error| {
                        ServiceError::invalid_operation(format!(
                            "failed to serialize max-turns annotation: {error}"
                        ))
                    },
                )?)),
                blocked_json: Some(Some(blocked_meta.to_string())),
                failed_json: Some(None),
                updated_at: now_rfc3339(),
            },
        )
        .await?;

        self.publish(ForgeEvent {
            event_type: "task.blocked".to_owned(),
            entity_id: updated.id,
            timestamp: event_timestamp(),
            context: EventContext::TaskBlocked {
                project_id: updated.project_id,
                reason: "max_turns_exceeded".to_owned(),
                kind: Some(api_types::FailureKind::MaxTurnsExceeded),
                source: None,
                execution_id: Some(execution.id.clone()),
            },
        });
        Ok(())
    }

    pub(super) async fn resolve_max_turns(&self, task: &Task) -> Result<Option<u32>> {
        if let Some(value) = task
            .task_state_config
            .as_deref()
            .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
            .and_then(|value| {
                max_turns_from_value(&value)
                    .or_else(|| value.get(&task.status).and_then(max_turns_from_value))
            })
        {
            return Ok(Some(value));
        }

        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Executor),
        );
        if let Some(value) = workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
            .and_then(|state| max_turns_from_value(&state.config))
        {
            return Ok(Some(value));
        }

        Ok(serde_json::from_str::<Value>(&project.settings)
            .ok()
            .and_then(|value| max_turns_from_value(&value)))
    }

    pub(super) async fn resolve_execution_logs_path(
        &self,
        execution: &Execution,
        task: &Task,
        workspace: &Workspace,
        execution_id: &str,
    ) -> Result<String> {
        let durable_path = execution_logs_path(
            &self.workspace_root,
            &task.project_id,
            &workspace.task_id,
            execution_id,
        );
        let Some(stored_path) = execution.logs_path.as_deref() else {
            return Ok(durable_path);
        };
        if stored_path == durable_path {
            return Ok(durable_path);
        }

        let stored = std::path::Path::new(stored_path);
        if !stored.exists() {
            return Ok(durable_path);
        }

        let durable = std::path::Path::new(&durable_path);
        if let Some(parent) = durable.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                ServiceError::invalid_operation(format!("failed to create log directory: {error}"))
            })?;
        }
        if !durable.exists() {
            std::fs::rename(stored, durable)
                .or_else(|_| {
                    std::fs::copy(stored, durable)?;
                    std::fs::remove_file(stored)
                })
                .map_err(|error| {
                    ServiceError::invalid_operation(format!(
                        "failed to move execution log: {error}"
                    ))
                })?;
        }

        Ok(durable_path)
    }
}
