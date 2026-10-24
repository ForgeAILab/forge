//! Native runtime preparation, LCM resolution, and working-set budgets.

use super::*;

pub(super) struct PreparedNativeRuntime {
    pub(super) runtime: agent_runtime::runtime::Runtime,
    pub(super) context_mode: TurnContextMode,
    pub(super) saved: bool,
    pub(super) lcm_link: Option<(String, String)>,
    pub(super) tool_result_summaries: Arc<Mutex<HashMap<String, ToolResultSummary>>>,
    pub(super) needs_adoption: bool,
    pub(super) summary_cap: u32,
    pub(super) topic_reads: Option<Arc<TopicReadFilter>>,
}

#[derive(Debug)]
struct TopicTimelineResolver {
    source: agent_runtime::harness::LcmTimelineBinding,
    successor: Option<agent_runtime::harness::LcmTimelineBinding>,
}
impl agent_runtime::harness::LcmTimelineResolver for TopicTimelineResolver {
    fn resolve(
        &self,
        session: &SessionId,
    ) -> Result<agent_runtime::harness::LcmTimelineBinding, RuntimeError> {
        let binding = if self.source.session == *session {
            &self.source
        } else {
            self.successor
                .as_ref()
                .ok_or_else(|| RuntimeError::not_found("topic timeline binding unavailable"))?
        };
        if binding.session != *session {
            return Err(RuntimeError::conflict("topic session binding differs"));
        }
        Ok(binding.clone())
    }
    fn new_timeline(
        &self,
        session: &SessionId,
        previous: &agent_runtime::harness::LcmTimelineBinding,
    ) -> Result<agent_runtime::harness::LcmTimelineBinding, RuntimeError> {
        if previous != &self.source {
            return Err(RuntimeError::conflict("topic fork source binding differs"));
        }
        let binding = self
            .successor
            .as_ref()
            .ok_or_else(|| RuntimeError::conflict("topic successor was not durably reserved"))?;
        if binding.session != *session {
            return Err(RuntimeError::conflict("topic session binding differs"));
        }
        Ok(binding.clone())
    }
}

/// The fixed, non-conversation part of a chat request (system prompt, state
/// card and tool schemas), sized with the runtime's default request sizer,
/// the same one the planner uses.
fn estimated_fixed_overhead(
    system_prompt: Option<&str>,
    state_card: Option<&str>,
    tools: &[Arc<dyn agent_runtime::core::tool::Tool>],
) -> u32 {
    use agent_runtime::context::RequestSizer;
    let sizer = agent_runtime::context::CharRatioSizer::default();
    let mut tokens = 0u32;
    if let Some(prompt) = system_prompt {
        tokens = tokens.saturating_add(sizer.size_message(&Message::text(Role::System, prompt)));
    }
    if let Some(card) = state_card {
        tokens = tokens.saturating_add(sizer.size_message(&Message::text(Role::User, card)));
    }
    tools.iter().fold(tokens, |tokens, tool| {
        tokens.saturating_add(sizer.size_tool_schema(&tool.spec().to_schema()))
    })
}

/// The working set for a turn whose session has no measured overhead yet.
///
/// LCM pressure budgets the conversation as `target - measured overhead`,
/// but the runtime measures that overhead only after a successful plan. A
/// cold session (new, adopted from pre-working-set state, or one whose every
/// plan so far failed) therefore budgets history against the whole target.
/// When the gap between target and hard cap cannot hold the system prompt,
/// tools and card (a model window at or below the surface target clamps both
/// to the same value), history stays under hard pressure while the planner
/// refuses the request, and the failed plan never records an overhead to
/// recover with. Lowering the target so the estimated overhead fits under the
/// hard cap lets LCM compact first. It changes nothing when the gap already
/// holds the overhead, and is never applied once an overhead is measured.
fn cold_start_working_set(policy: WorkingSetPolicy, estimated_overhead: u32) -> WorkingSetPolicy {
    WorkingSetPolicy {
        target_tokens: policy
            .target_tokens
            .min(policy.hard_tokens.saturating_sub(estimated_overhead))
            .max(policy.target_tokens / 4)
            .max(1),
        hard_tokens: policy.hard_tokens,
    }
}

impl NativeAgentRuntimeBackend {
    /// The overhead probe checks whether the session's persisted LCM state
    /// has no measured request overhead yet (see [`cold_start_working_set`]).
    pub(super) async fn prepare_runtime(
        &self,
        request: &AgentTurnRequest,
        successor: Option<&str>,
        probe_overhead: bool,
    ) -> Result<PreparedNativeRuntime, AgentHostError> {
        request.scope.validate()?;
        let binding = self
            .protected_store
            .runtime_scope_binding(
                &request.forge_session_id,
                &request.runtime_session_id,
                request.workspace_path.as_deref(),
            )
            .await?;
        if binding.scope != request.scope {
            return Err(AgentHostError::Authority(
                "native turn scope does not match the server-issued session binding".to_owned(),
            ));
        }
        if binding.workspace_path.as_deref() != request.workspace_path.as_deref() {
            return Err(AgentHostError::Authority(
                "native turn workspace does not match the server-issued Task workspace".to_owned(),
            ));
        }
        let context_mode = turn_context_mode(&binding.scope, binding.task_role.as_deref());
        let id = SessionId::new(&request.runtime_session_id);
        let flags = self
            .protected_store
            .database()
            .session_persistence_flags(&request.forge_session_id)
            .await
            .map_err(|_| AgentHostError::ProtectedPersistence)?;
        if flags.retired {
            return Err(AgentHostError::SessionRetired);
        }
        let flags = if context_mode.persistent_session {
            flags
        } else {
            db::SessionPersistenceFlags::default()
        };
        let saved = flags.saved;
        // Fork/abort plan no provider request and never need the cold-start set.
        let overhead_measured = if probe_overhead && context_mode.lcm && saved {
            self.protected_store
                .lcm_overhead_measured(&id, flags)
                .await
                .map_err(host_runtime_error)?
        } else {
            !probe_overhead
        };
        if successor.is_none() && request.scope.scope_type == CanonicalScopeType::AgentChat {
            let db = self.protected_store.database();
            sqlx::query("INSERT INTO agent_chat_topic (id, chat_id, sequence, label, summary, starting_message_id, starting_message_sequence, principal_type, principal_id, created_at, runtime_session_id) SELECT ?, ?, 0, 'Original conversation', NULL, NULL, 0, 'system', NULL, ?, ? WHERE NOT EXISTS (SELECT 1 FROM agent_chat_topic WHERE chat_id = ?)")
                .bind(db::new_uuid_v4()).bind(&request.scope.scope_id).bind(db::now_rfc3339()).bind(&request.runtime_session_id).bind(&request.scope.scope_id)
                .execute(db.pool()).await.map_err(|_| AgentHostError::ProtectedPersistence)?;
        }
        let summary_cap = if binding.scope.scope_type == CanonicalScopeType::AgentChat {
            let budgets = *self.working_sets.read().expect("working set lock poisoned");
            if binding.agent_chat_project_id.is_some() {
                budgets.1.hard_tokens
            } else {
                budgets.0.hard_tokens
            }
        } else {
            request.provider.max_input_tokens
        }
        .min(request.provider.max_input_tokens);
        let workspace = workspace_for_scope(&binding.scope, binding.workspace_path.as_deref())?;
        // A Task session and a Project Agent verification session both compose
        // against a real root; every other scope composes against none.
        let composed_workspace_root = match binding.scope.scope_type {
            CanonicalScopeType::Task => Some(workspace.root().to_owned()),
            CanonicalScopeType::AgentChat
                if binding.scope.workspace_access == WorkspaceAccess::ProjectVerify =>
            {
                Some(workspace.root().to_owned())
            }
            // The Main Agent and the ephemeral inquiry sub-agents it
            // dispatches compose against an account scratch directory. No
            // repository is present in it, so there is nothing to write back.
            CanonicalScopeType::Account | CanonicalScopeType::AgentChat
                if binding.scope.workspace_access == WorkspaceAccess::AccountScratch =>
            {
                Some(workspace.root().to_owned())
            }
            CanonicalScopeType::Account
            | CanonicalScopeType::Project
            | CanonicalScopeType::AgentChat => None,
        };
        let composition = ScopeToolComposition::for_scope_with_permissions_and_project_context(
            binding.identity_id.clone(),
            binding.scope.clone(),
            binding.task_role.as_deref(),
            composed_workspace_root.as_deref(),
            &binding.allowed_permissions,
            ProjectChatToolContext {
                is_project_agent_chat: binding.agent_chat_project_id.is_some(),
                charter_setup_required: binding.project_charter_setup_required,
            },
            self.forge_tool_provider.clone().map(|provider| {
                crate::authority_bound_provider(self.protected_store.database(), &binding, provider)
            }),
            ScopeToolRuntime {
                command_allowlist: request.command_allowlist.clone(),
                environment: request.environment.clone(),
                fetch_transport: Some(Arc::clone(&self.fetch_transport)),
            },
        )?;
        let topic_reads = if binding.scope.scope_type == CanonicalScopeType::AgentChat {
            let failed = self
                .failed_topic_reads
                .lock()
                .map_err(|_| AgentHostError::ProtectedPersistence)?
                .remove(&request.runtime_session_id);
            if failed {
                clear_topic_reads(
                    &self.protected_store.database(),
                    &request.runtime_session_id,
                )
                .await?;
            }
            Some(Arc::new(TopicReadFilter {
                db: self.protected_store.database(),
                runtime_session_id: request.runtime_session_id.clone(),
                turn: Mutex::new(TopicReadTurn::default()),
            }))
        } else {
            None
        };
        let composition = match &topic_reads {
            Some(filter) => composition.filter_results(filter.clone()),
            None => composition,
        };
        // `RuntimeEvent::ToolCallCompleted` only carries `is_error`; observe
        // each tool's exact result here, keyed by call id, so the bounded
        // `ToolResultSummary` a structured Forge command already produced
        // survives to `TurnEventSink::tool_call_finished` instead of being
        // discarded at that boundary (F14/D18).
        let tool_result_summaries: Arc<Mutex<HashMap<String, ToolResultSummary>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let observed_summaries = Arc::clone(&tool_result_summaries);
        let composition = composition.observe_results(Arc::new(
            move |call_id: &ToolCallId, result: &Result<ToolOutcome, RuntimeError>| {
                let summary = tool_result_summary(call_id.as_str(), result);
                if let Ok(mut summaries) = observed_summaries.lock() {
                    summaries.insert(call_id.as_str().to_owned(), summary);
                }
            },
        ));
        let provider = self.provider(request)?;
        let model_id = ModelId::new(&request.provider.model);

        let mut lcm_link = None;
        let mut needs_adoption = false;
        let lcm = if context_mode.lcm {
            let lcm_store = self
                .protected_store
                .lcm_store_for_runtime_session(
                    &request.runtime_session_id,
                    scope_type_name(request.scope.scope_type),
                    &request.scope.scope_id,
                )
                .await?;
            lcm_link = Some((
                lcm_store.timeline_id().to_owned(),
                lcm_store.authorization_revision().to_owned(),
            ));
            needs_adoption = lcm_store
                .needs_adoption(&request.runtime_session_id)
                .await?;
            let lcm_binding =
                lcm_store.runtime_binding(SessionId::new(&request.runtime_session_id))?;
            let mut resolver = TopicTimelineResolver {
                source: lcm_binding,
                successor: None,
            };
            let mut lcm_store = lcm_store;
            if let Some(successor) = successor {
                let next = self
                    .protected_store
                    .lcm_store_for_successor_runtime_session(
                        successor,
                        scope_type_name(request.scope.scope_type),
                        &request.scope.scope_id,
                    )
                    .await?;
                resolver.successor = Some(next.runtime_binding(SessionId::new(successor))?);
                lcm_store = lcm_store.with_alternate(next);
            }
            let lcm = LcmCoordinator::new(
                Arc::new(lcm_store),
                Arc::new(self.summary_model(request, summary_cap)?),
                Arc::new(resolver),
                LcmCoordinatorPolicy {
                    input_budget_tokens: u64::from(request.provider.max_input_tokens),
                    ..LcmCoordinatorPolicy::default()
                },
            )
            .map_err(|error| AgentHostError::Configuration(error.to_string()))?;
            Some(lcm)
        } else {
            None
        };
        let mut builder = RuntimeBuilder::new(model_id.clone())
            .provider_name(request.provider.provider.clone())
            .provider(if request.server_state_card.is_some() {
                Arc::new(ServerStateCardProvider {
                    inner: provider,
                    marker: self.state_card_marker.clone(),
                }) as Arc<dyn Provider>
            } else {
                provider
            })
            .model_profile(ResolvedModelProfile::explicit(
                request.provider.provider.clone(),
                model_id,
                ModelLimits::new(
                    request.provider.context_tokens,
                    request.provider.max_input_tokens,
                    request.provider.max_output_tokens,
                ),
            ))
            .workspace(workspace)
            .interaction_broker(Arc::new(self.interaction_broker.clone()))
            .security_subject(SecuritySubject::new(binding.identity_id))
            // Forge reduces raw arguments to a bounded, credential-masked
            // preview (`tool_preview::build_tool_argument_preview`) before
            // they are ever persisted or rendered; without this opt-in the
            // runtime withholds argument values by default.
            .emit_raw_tool_arguments(true);
        if context_mode.persistent_session {
            builder = builder
                .session_store(self.protected_store.clone())
                .checkpoint_store(self.protected_store.clone());
        }
        if let Some(lcm) = lcm {
            builder = builder.lcm(Arc::new(lcm));
            if binding.scope.scope_type == CanonicalScopeType::AgentChat {
                let budgets = *self.working_sets.read().expect("working set lock poisoned");
                let policy = if binding.agent_chat_project_id.is_some() {
                    budgets.1
                } else {
                    budgets.0
                };
                let window = request.provider.max_input_tokens.min(
                    request
                        .provider
                        .context_tokens
                        .saturating_sub(request.provider.max_output_tokens),
                );
                let mut policy = WorkingSetPolicy {
                    target_tokens: policy.target_tokens.min(window),
                    hard_tokens: policy.hard_tokens.min(window),
                };
                if !overhead_measured {
                    let card = request
                        .server_state_card
                        .as_ref()
                        .map(|card| format!("{}{card}", self.state_card_marker));
                    policy = cold_start_working_set(
                        policy,
                        estimated_fixed_overhead(
                            request.system_prompt.as_deref(),
                            card.as_deref(),
                            &composition.tools(),
                        ),
                    );
                }
                builder = builder.working_set_policy(policy);
            }
        }
        if context_mode.structural_compaction {
            builder =
                builder.compactor(task_structural_compactor(request.provider.max_input_tokens));
        }
        builder = composition.apply(builder);
        if let Some(prompt) = request.system_prompt.as_deref() {
            builder = builder.system_prompt(prompt);
        }
        if let Some(card) = request.server_state_card.clone() {
            builder = builder.context_contributor(Arc::new(ServerStateCard {
                card: format!("{}{card}", self.state_card_marker),
            }));
        }
        if let Some(effort) = request.provider.reasoning_effort.as_deref() {
            builder = builder.reasoning(ReasoningConfig {
                effort: Some(effort.to_owned()),
                max_tokens: None,
            });
        }
        let runtime = builder
            .build()
            .map_err(|error| AgentHostError::Configuration(error.to_string()))?;
        Ok(PreparedNativeRuntime {
            runtime,
            context_mode,
            saved,
            lcm_link,
            tool_result_summaries,
            needs_adoption,
            summary_cap,
            topic_reads,
        })
    }
}
