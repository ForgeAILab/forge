//! Scoped CLI chat execution and result conversion.

use super::*;

/// Narrow legacy CLI adapter for migrated Agent Chats. It deliberately uses a
/// disposable empty directory and advertises denied workspace authority; a
/// Task execution path is not routed through this type.
#[derive(Clone)]
pub struct CliAgentChatSessionBackend {
    executor: Arc<dyn TaskExecutor>,
}

impl fmt::Debug for CliAgentChatSessionBackend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CliAgentChatSessionBackend")
            .finish_non_exhaustive()
    }
}

impl CliAgentChatSessionBackend {
    pub fn new(executor: Arc<dyn TaskExecutor>) -> Self {
        Self { executor }
    }

    pub fn capabilities() -> BackendCapabilities {
        BackendCapabilities {
            native_runtime: false,
            persistent_session: false,
            protected_checkpoints: false,
            lcm: false,
            cancel: true,
            steer: false,
            workspace: WorkspaceAccess::Deny,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn run_turn(
        &self,
        scope: &CanonicalScope,
        job_id: &str,
        chat_id: &str,
        executor_type: &str,
        agent_config: Value,
        prompt: String,
        logs_path: PathBuf,
        cancellation: CancellationToken,
    ) -> Result<(ExecutionResult, i64)> {
        if scope.scope_type != CanonicalScopeType::AgentChat
            || scope.scope_id != chat_id
            || scope.workspace_access != WorkspaceAccess::Deny
        {
            return Err(ServiceError::invalid_operation(
                "CLI Agent Chat backend requires a denied-filesystem Agent Chat scope",
            ));
        }
        let kind = executor_type
            .parse::<ExecutorKind>()
            .map_err(ServiceError::invalid_operation)?;
        let executor_type = kind.to_string();
        if matches!(kind, ExecutorKind::Shell | ExecutorKind::Embedded) {
            return Err(ServiceError::invalid_operation(
                "selected executor cannot run a legacy CLI Agent Chat turn",
            ));
        }
        let executor_snapshot = cli_executor_snapshot(&executor_type, agent_config);

        let sandbox = chat_sandbox_path(job_id);
        if sandbox.exists() {
            std::fs::remove_dir_all(&sandbox).map_err(|_| {
                ServiceError::invalid_operation("stale Agent Chat sandbox could not be removed")
            })?;
        }
        std::fs::create_dir_all(&sandbox).map_err(|_| {
            ServiceError::invalid_operation("Agent Chat sandbox could not be created")
        })?;
        if let Some(parent) = logs_path.parent() {
            std::fs::create_dir_all(parent).map_err(|_| {
                ServiceError::invalid_operation("Agent Chat log directory could not be created")
            })?;
        }

        let started = std::time::Instant::now();
        let execution = self.executor.execute(ExecutionContext {
            task_id: chat_id.to_owned(),
            execution_id: job_id.to_owned(),
            worktree_path: sandbox.to_string_lossy().into_owned(),
            description: prompt,
            agent_config: executor_snapshot,
            logs_path: logs_path.to_string_lossy().into_owned(),
            heartbeat_interval_seconds: 30,
            max_turns: None,
            log_sender: None,
        });
        tokio::pin!(execution);
        let result = tokio::select! {
            result = &mut execution => result,
            _ = cancellation.cancelled() => {
                let _ = self.executor.cancel(job_id).await;
                let _ = std::fs::remove_dir_all(&sandbox);
                return Err(ServiceError::invalid_operation("Agent Chat CLI turn was cancelled"));
            }
        };
        let result = match result {
            Ok(result) => result,
            Err(error) if !error.usage_reports().is_empty() => ExecutionResult {
                status: ExecutionOutcome::Failed,
                error: Some(error.to_string()),
                usage_reports: error.usage_reports().to_owned(),
                ..ExecutionResult::default()
            },
            Err(error) => return Err(error.into()),
        };
        let _ = std::fs::remove_dir_all(&sandbox);
        let duration_ms = checked_duration_ms(started.elapsed())?;
        Ok((result, duration_ms))
    }
}

struct ScopedCliChatTools {
    composition: forge_agent_host::ScopeToolComposition,
    session_id: String,
    turn_id: String,
}

#[async_trait]
impl cli_adapters::codex::client::ChatToolHandler for ScopedCliChatTools {
    fn specs(&self) -> Vec<cli_adapters::codex::protocol::DynamicToolSpec> {
        self.composition
            .tools()
            .iter()
            .map(|tool| {
                let spec = tool.spec();
                cli_adapters::codex::protocol::DynamicToolSpec {
                    name: spec.name,
                    description: spec.description,
                    input_schema: spec.input_schema,
                }
            })
            .collect()
    }

    async fn call(
        &self,
        name: &str,
        call_id: &str,
        arguments: Value,
    ) -> std::result::Result<Value, String> {
        self.composition
            .invoke_denied_chat_tool(&self.session_id, &self.turn_id, call_id, name, arguments)
            .await
            .map_err(|error| error.to_string())
    }
}

impl FederatedAgentChatTurnRunner {
    pub(super) async fn run_cli(
        &self,
        job: &AgentChatTurnJob,
        turn: LoadedAgentChatTurn,
        cancellation: CancellationToken,
    ) -> std::result::Result<CompletedAgentChatTurn, TurnRunError> {
        let LoadedAgentChatTurn {
            agent,
            profile,
            session,
            input,
            history,
            operating_instruction,
            state_card,
            operating_context_sources,
        } = turn;
        let prompt = build_cli_prompt(
            profile.prompt_template.as_deref(),
            operating_instruction.as_deref(),
            &history,
            &input.content,
            Some(&state_card),
        );
        let config = cli_profile_execution_config(&profile).map_err(|error| TurnRunError {
            failure: TurnFailure::Configuration,
            error,
        })?;
        // The CLI adapter streams its own events into the turn log; only the
        // attempt divider comes from Forge, so a retry reads the same way on
        // both backends.
        drop(self.turn_log_sink(job).await);
        let scope = CanonicalScope {
            scope_type: CanonicalScopeType::AgentChat,
            scope_id: job.chat_id.clone(),
            workspace_access: WorkspaceAccess::Deny,
        };
        let scoped_backend = if profile.executor_type == "codex" {
            let composition = self
                .embedded_agents
                .cli_chat_tools(&session.id, &agent.id, &scope)
                .await?;
            let handler = Arc::new(ScopedCliChatTools {
                composition,
                session_id: session.id.clone(),
                turn_id: job.id.clone(),
            });
            let mut registry = executors::AdapterRegistry::new();
            registry.register(Box::new(
                cli_adapters::CodexAdapter::new().with_chat_tools(handler),
            ));
            Some(CliAgentChatSessionBackend::new(Arc::new(
                executors::AdapterExecutor::new(Arc::new(registry)),
            )))
        } else {
            None
        };
        let (mut result, duration_ms) = scoped_backend
            .as_ref()
            .unwrap_or(&self.cli_backend)
            .run_turn(
                &scope,
                &job.id,
                &job.chat_id,
                &profile.executor_type,
                config,
                prompt,
                self.turn_logs.path_for(&job.id),
                cancellation,
            )
            .await?;
        let attempt_ordinal = job
            .invocation_count
            .checked_sub(1)
            .ok_or_else(|| ServiceError::invalid_operation("chat attempt count is invalid"))?;
        for report in &mut result.usage_reports {
            report.candidate_key = Some("chat".to_owned());
            report.attempt_ordinal = u32::try_from(attempt_ordinal)
                .map_err(|_| ServiceError::invalid_operation("chat attempt ordinal overflows"))?;
        }
        self.remember_usage(&job.id, result.usage_reports.clone())
            .await;
        let failure = cli_turn_failure(&result);
        let content =
            cli_result_content(result).map_err(|error| TurnRunError { failure, error })?;
        guard_agent_chat_content(&content)?;
        let context_manifest_id = if operating_context_sources.is_empty() {
            None
        } else {
            Some(
                self.persist_server_context_manifest(
                    job,
                    &agent,
                    &profile,
                    &session,
                    profile.model.as_deref(),
                    &operating_context_sources,
                )
                .await?,
            )
        };
        Ok(CompletedAgentChatTurn {
            identity_id: agent.id,
            profile_id: profile.id,
            session_id: session.id,
            model: profile.model,
            content,
            token_usage_json: None,
            duration_ms,
            context_manifest_id,
            pending_interaction_id: None,
        })
    }
}

pub(super) fn cli_turn_failure(result: &ExecutionResult) -> TurnFailure {
    if result
        .route_attempts
        .iter()
        .any(|attempt| attempt.outcome == executors::RouteAttemptOutcome::UsageExhausted)
    {
        return TurnFailure::UsageLimit {
            resets_at: result
                .retry_after
                .and_then(|delay| chrono::Duration::from_std(delay).ok())
                .map(|delay| Utc::now() + delay)
                .and_then(|at| u64::try_from(at.timestamp_millis()).ok()),
        };
    }
    if result.failure_class == Some(executors::ExecutionFailureClass::ExecutorUnavailable)
        && result.retry_after.is_some()
    {
        return TurnFailure::Transient {
            retry_after: result
                .retry_after
                .map(|delay| u64::try_from(delay.as_millis()).unwrap_or(u64::MAX)),
        };
    }
    if result.status == ExecutionOutcome::Completed
        && result
            .assistant_output
            .as_ref()
            .or(result.summary.as_ref())
            .is_none_or(|text| text.trim().is_empty())
    {
        return TurnFailure::EmptyResponse;
    }
    TurnFailure::Unclassified
}

pub(super) fn cli_result_content(result: ExecutionResult) -> Result<String> {
    let content = match result.status {
        ExecutionOutcome::Completed => result
            .assistant_output
            .or(result.summary)
            .filter(|summary| !summary.trim().is_empty())
            .ok_or_else(|| ServiceError::invalid_operation("Agent Chat CLI returned no content")),
        ExecutionOutcome::Cancelled => Err(ServiceError::invalid_operation(
            "Agent Chat CLI turn was cancelled",
        )),
        // The adapter already knows why it failed. Reporting only "turn
        // failed" left the real reason -- a missing binary, an unauthenticated
        // CLI, a bad model id -- in a dropped field, so the chat surfaced an
        // error nobody could act on and the server log carried nothing either.
        ExecutionOutcome::Failed => Err(ServiceError::invalid_operation(
            match result
                .error
                .as_deref()
                .map(str::trim)
                .filter(|error| !error.is_empty())
            {
                Some(error) => format!("Agent Chat CLI turn failed: {error}"),
                None => "Agent Chat CLI turn failed without a reported reason".to_owned(),
            },
        )),
    }?;
    Ok(content)
}

pub(super) fn cli_profile_execution_config(profile: &AgentProfile) -> Result<Value> {
    let mut config: Value = serde_json::from_str(&profile.config_json)
        .map_err(|_| ServiceError::invalid_operation("Agent profile config is invalid"))?;
    merge_overrides(
        &mut config,
        &ExecutionOverrides {
            model_id: profile.model.clone(),
            reasoning_effort: profile.reasoning_effort.clone(),
            permission_policy: profile.permission_policy.clone(),
            hard_deadline_seconds: None,
        },
    )?;

    if profile.executor_type == ExecutorKind::Smith.to_string() {
        if let Some(provider) = &profile.provider {
            config
                .as_object_mut()
                .expect("merge_overrides accepted an object")
                .insert("provider".to_owned(), Value::String(provider.clone()));
        }
    }

    // Agent Chat runs in a disposable, non-repository sandbox.  The CLI
    // adapter must never run Task git finalization (including a status probe)
    // after a successful chat reply; the chat service persists the reply and
    // any typed Forge proposals separately.
    config
        .as_object_mut()
        .expect("merge_overrides accepted an object")
        .insert("auto_commit".to_owned(), Value::Bool(false));

    Ok(config)
}

pub(super) fn cli_executor_snapshot(executor_type: &str, config: Value) -> Value {
    serde_json::json!({
        "executor_type": executor_type,
        "config": config,
    })
}
