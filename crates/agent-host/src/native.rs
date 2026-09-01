use std::{
    collections::HashMap,
    fmt,
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
};

use agent_runtime::{
    core::{
        cancel::CancelReason,
        catalog::{ModelLimits, ResolvedModelProfile},
        content::{ContentPart, Role, UserInput},
        error::RuntimeError,
        event::{RuntimeEvent, TurnFinish},
        ids::{SessionId, ToolCallId},
        provider::{ModelId, Provider},
        provider_credential::ProviderCredentialTarget,
        security::SecuritySubject,
        tool::ToolOutcome,
        usage::CounterKind,
        workspace::DenyAllWorkspace,
    },
    harness::{LcmCoordinator, LcmCoordinatorPolicy, StaticLcmTimelineResolver},
    provider::{
        gemini::{GeminiInteractionsConfig, GeminiInteractionsProvider},
        openai::{OpenAiConfig, OpenAiProvider},
        responses::{ResponsesConfig, ResponsesProvider},
    },
    runtime::{RuntimeBuilder, SessionHandle, StartSession},
};
use api_types::{OrchestrationOutcome, ToolResultSummary};
use async_trait::async_trait;
use futures_util::StreamExt;

use crate::{
    AgentHostError, AgentSessionBackend, AgentTurnOutput, AgentTurnRequest, BackendCapabilities,
    CanonicalScope, CanonicalScopeType, DeterministicLcmSummaryModel, FORGE_LCM_STORE_REVISION,
    ForgeToolProvider, InteractionBrokerHandle, ProjectChatToolContext, RuntimeContextManifestLink,
    ScopeToolComposition, TurnEventSink, WorkspaceAccess,
    protected_store::SqliteProtectedRuntimeStore, transport::ReqwestTransport,
};

#[derive(Clone)]
pub struct NativeAgentRuntimeBackend {
    protected_store: Arc<SqliteProtectedRuntimeStore>,
    interaction_broker: InteractionBrokerHandle,
    active: Arc<Mutex<HashMap<String, SessionHandle>>>,
    forge_tool_provider: Option<Arc<dyn ForgeToolProvider>>,
    provider_override: Option<Arc<dyn Provider>>,
}

impl NativeAgentRuntimeBackend {
    pub fn new(protected_store: Arc<SqliteProtectedRuntimeStore>) -> Self {
        Self {
            interaction_broker: InteractionBrokerHandle::new(Arc::clone(&protected_store)),
            protected_store,
            active: Arc::new(Mutex::new(HashMap::new())),
            forge_tool_provider: None,
            provider_override: None,
        }
    }

    /// Replaces outbound provider construction with an in-process runtime
    /// provider. The transport's SSRF policy correctly rejects loopback
    /// endpoints, so integration tests that exercise the full native turn
    /// path (scope binding, LCM wiring, manifest linkage) inject a scripted
    /// provider here instead of a mock HTTP server.
    #[doc(hidden)]
    pub fn with_provider_override(mut self, provider: Arc<dyn Provider>) -> Self {
        self.provider_override = Some(provider);
        self
    }

    /// Installs the Forge domain provider used by scope-derived read/proposal
    /// tools.  The provider receives identity/scope values resolved from the
    /// persisted session, never from model arguments.
    pub fn with_forge_tool_provider(mut self, provider: Arc<dyn ForgeToolProvider>) -> Self {
        self.forge_tool_provider = Some(provider);
        self
    }

    /// Returns the shared protected broker used by native turns.  API
    /// handlers may answer through another clone; the durable row is the
    /// synchronization boundary rather than an in-process channel.
    pub fn interaction_broker(&self) -> InteractionBrokerHandle {
        self.interaction_broker.clone()
    }

    fn provider(&self, request: &AgentTurnRequest) -> Result<Arc<dyn Provider>, AgentHostError> {
        if let Some(provider) = &self.provider_override {
            return Ok(Arc::clone(provider));
        }
        let transport = ReqwestTransport::new()
            .map_err(|error| AgentHostError::Configuration(error.message))?;
        let target = ProviderCredentialTarget::new(request.provider.credential_handle_id.clone())
            .map_err(|error| AgentHostError::Configuration(error.to_string()))?;
        let source = self.protected_store.credential_source(
            request.provider.owner_user_id.clone(),
            request.provider.credential_handle_id.clone(),
        );
        match request.provider.provider.as_str() {
            "xai" => {
                let config = ResponsesConfig::new(
                    request.provider.base_url.clone(),
                    request.provider.model.clone(),
                );
                let provider =
                    ResponsesProvider::with_credential_source(transport, config, target, source)
                        .map_err(|error| AgentHostError::Configuration(error.to_string()))?;
                Ok(Arc::new(provider))
            }
            "gemini" => {
                let config = GeminiInteractionsConfig::new(
                    request.provider.base_url.clone(),
                    request.provider.model.clone(),
                );
                let provider = GeminiInteractionsProvider::with_credential_source(
                    transport, config, target, source,
                )
                .map_err(|error| AgentHostError::Configuration(error.to_string()))?;
                Ok(Arc::new(provider))
            }
            "openai"
                if request
                    .provider
                    .base_url
                    .contains("chatgpt.com/backend-api/codex") =>
            {
                let mut config = ResponsesConfig::chatgpt(request.provider.model.clone());
                // Preserve the stored endpoint so proxied deployments keep
                // working; the preset's canonical URL is only a default.
                config.base_url = request.provider.base_url.clone();
                if let Some(account_id) = request.provider.provider_account_id.as_deref() {
                    config = config.with_chatgpt_account(account_id);
                }
                let provider =
                    ResponsesProvider::with_credential_source(transport, config, target, source)
                        .map_err(|error| AgentHostError::Configuration(error.to_string()))?;
                Ok(Arc::new(provider))
            }
            "openai" | "openai_compatible" | "openrouter" => {
                let config = OpenAiConfig::new(
                    request.provider.base_url.clone(),
                    request.provider.model.clone(),
                );
                let provider =
                    OpenAiProvider::with_credential_source(transport, config, target, source)
                        .map_err(|error| AgentHostError::Configuration(error.to_string()))?;
                Ok(Arc::new(provider))
            }
            provider => Err(AgentHostError::Unsupported(format!(
                "native provider `{provider}` is not configured"
            ))),
        }
    }
}

/// Sizing constants mirroring the LCM `CharRatioSizer` default so the
/// host-side overhead estimate and the coordinator's timeline accounting
/// stay on one scale.
const LCM_CHARS_PER_TOKEN: u64 = 4;
/// Absorbs framing, cache-control, and estimator drift the host cannot
/// measure exactly at composition time.
const LCM_PLANNER_MARGIN_TOKENS: u64 = 1024;
/// Keeps the pressure policy valid (a zero budget is rejected) when the
/// non-conversation content alone exceeds the provider window; the turn then
/// fails with the planner's precise required-content diagnostic instead of a
/// policy configuration error.
const LCM_MIN_CONVERSATION_BUDGET_TOKENS: u64 = 1024;

/// The LCM pressure model counts only timeline (conversation) tokens, while
/// the context planner must fit the system prompt, activated tool schemas,
/// and the new user input inside the same provider window. Handing the
/// coordinator the full `max_input_tokens` leaves a dead zone: history alone
/// stays below hard pressure while the planned total already exceeds the
/// budget, so the turn fails planner-side (`budget_exceeded`) without LCM
/// ever compacting. Deduct the measurable non-conversation content so
/// pressure trips while compaction can still help.
fn conversation_budget_tokens(
    request: &AgentTurnRequest,
    composition: &ScopeToolComposition,
) -> u64 {
    let mut chars = request.system_prompt.as_deref().map_or(0, str::len) as u64;
    chars += request.input.len() as u64;
    for tool in composition.tools() {
        let spec = tool.spec();
        chars += (spec.name.len() + spec.description.len()) as u64;
        chars += serde_json::to_string(&spec.input_schema).map_or(0, |schema| schema.len() as u64);
    }
    let overhead = chars.div_ceil(LCM_CHARS_PER_TOKEN) + LCM_PLANNER_MARGIN_TOKENS;
    u64::from(request.provider.max_input_tokens)
        .saturating_sub(overhead)
        .max(LCM_MIN_CONVERSATION_BUDGET_TOKENS)
}

/// Forge's pressure policy, adjusted on two axes the stock defaults get
/// wrong for real chats:
///
/// - `leaf_target_tokens` must exceed one full canonical turn. Leaf planning
///   only commits a span ending at a user boundary; when a single
///   `[user, assistant]` pair outgrows the target (assistant replies are
///   bounded by `max_output_tokens`, far above the stock 2048), selection
///   stops mid-turn, the planner backs up to the previous user boundary —
///   eventually index zero — and returns no plan, so every attempt ends in
///   "LCM context cannot fit after bounded hard compaction" with the
///   frontier permanently stuck in front of the oversized turn. Scale the
///   target with the profile's output cap so one leaf can always swallow a
///   full turn pair.
/// - `max_rounds`: 3 rounds cannot walk a chat back under budget once it has
///   drifted deep past hard pressure. Sixteen rounds cover a full
///   provider-window overrun in one attempt; each round is a cheap local
///   deterministic summary, so the widened bound costs nothing when pressure
///   is caught early.
fn forge_lcm_pressure_policy(request: &AgentTurnRequest) -> agent_runtime::lcm::LcmPressurePolicy {
    agent_runtime::lcm::LcmPressurePolicy {
        revision: agent_runtime::registry::RegistryRevision::from_content("forge-lcm-pressure-2"),
        leaf_target_tokens: u64::from(request.provider.max_output_tokens).saturating_add(4096),
        max_rounds: 16,
        ..agent_runtime::lcm::LcmPressurePolicy::default()
    }
}

impl fmt::Debug for NativeAgentRuntimeBackend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeAgentRuntimeBackend")
            .field(
                "active_sessions",
                &self.active.lock().map(|map| map.len()).unwrap_or(0),
            )
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl AgentSessionBackend for NativeAgentRuntimeBackend {
    fn capabilities(&self, scope: &CanonicalScope) -> BackendCapabilities {
        BackendCapabilities {
            native_runtime: true,
            persistent_session: true,
            protected_checkpoints: true,
            lcm: true,
            cancel: true,
            steer: true,
            workspace: scope.workspace_access,
        }
    }

    async fn run_turn(
        &self,
        request: AgentTurnRequest,
        sink: Arc<dyn TurnEventSink>,
    ) -> Result<AgentTurnOutput, AgentHostError> {
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
            self.forge_tool_provider.clone(),
        )?;
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
        let provider = self.provider(&request)?;
        let model_id = ModelId::new(&request.provider.model);
        let lcm_store = self
            .protected_store
            .lcm_store_for_runtime_session(
                &request.runtime_session_id,
                scope_type_name(request.scope.scope_type),
                &request.scope.scope_id,
            )
            .await?;
        let lcm_timeline_id = lcm_store.timeline_id().to_owned();
        let lcm_binding_revision = lcm_store.authorization_revision().to_owned();
        let lcm_binding = lcm_store.runtime_binding(SessionId::new(&request.runtime_session_id))?;
        let lcm = LcmCoordinator::new(
            Arc::new(lcm_store),
            Arc::new(DeterministicLcmSummaryModel::default()),
            Arc::new(StaticLcmTimelineResolver::new(lcm_binding)),
            LcmCoordinatorPolicy {
                input_budget_tokens: conversation_budget_tokens(&request, &composition),
                pressure: forge_lcm_pressure_policy(&request),
                ..LcmCoordinatorPolicy::default()
            },
        )
        .map_err(|error| AgentHostError::Configuration(error.to_string()))?;
        let mut builder = RuntimeBuilder::new(model_id.clone())
            .provider_name(request.provider.provider.clone())
            .provider(provider)
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
            .session_store(self.protected_store.clone())
            .checkpoint_store(self.protected_store.clone())
            .interaction_broker(Arc::new(self.interaction_broker.clone()))
            .security_subject(SecuritySubject::new(binding.identity_id))
            .lcm(Arc::new(lcm));
        builder = composition.apply(builder);
        if let Some(prompt) = request.system_prompt.as_deref() {
            builder = builder.system_prompt(prompt);
        }
        let runtime = builder
            .build()
            .map_err(|error| AgentHostError::Configuration(error.to_string()))?;
        let session = runtime
            .start_session(
                StartSession::new()
                    .with_id(SessionId::new(&request.runtime_session_id))
                    .with_history(request.history),
            )
            .await
            .map_err(|error| AgentHostError::Runtime(error.to_string()))?;
        let mut events = session.subscribe();
        let turn = session
            .send(UserInput::text(request.input))
            .map_err(|error| AgentHostError::Runtime(error.to_string()))?;
        self.active
            .lock()
            .map_err(|_| AgentHostError::Runtime("active session registry failed".to_owned()))?
            .insert(request.runtime_session_id.clone(), session.clone());
        let turn_id = turn.id().clone();
        let mut last_turn_error: Option<String> = None;
        let finish_result = loop {
            tokio::select! {
                _ = request.cancellation.cancelled() => {
                    turn.interrupt(CancelReason::UserRequested);
                    break Ok(TurnFinish::Cancelled { reason: CancelReason::UserRequested });
                }
                event = events.next() => {
                    let Some(event) = event else {
                        break Err(AgentHostError::Runtime(
                            "runtime event stream ended before completion".to_owned(),
                        ));
                    };
                    if event.turn.as_ref() != Some(&turn_id) {
                        continue;
                    }
                    match &event.payload {
                        RuntimeEvent::TextDelta { text, .. } => sink.text_delta(text).await,
                        RuntimeEvent::ReasoningDelta { text, redacted, .. } => {
                            sink.reasoning_delta(text, *redacted).await;
                        }
                        RuntimeEvent::ToolCallRequested {
                            call,
                            name,
                            argument_keys,
                            ..
                        } => {
                            sink.tool_call_started(call.as_str(), name, argument_keys)
                                .await;
                        }
                        RuntimeEvent::ToolCallCompleted {
                            call,
                            name,
                            is_error,
                        } => {
                            let summary = tool_result_summaries
                                .lock()
                                .ok()
                                .and_then(|mut summaries| summaries.remove(call.as_str()))
                                .unwrap_or_else(|| {
                                    ToolResultSummary::unclassified(*is_error, call.as_str())
                                });
                            sink.tool_call_finished(call.as_str(), name, *is_error, &summary)
                                .await;
                        }
                        RuntimeEvent::Error { error } => {
                            last_turn_error = Some(error.to_string());
                        }
                        RuntimeEvent::TurnCompleted { finish, .. } => break Ok(finish.clone()),
                        _ => {}
                    }
                }
            }
        };
        let persist_result = session
            .persist()
            .await
            .map_err(|error| AgentHostError::Runtime(error.to_string()));
        self.active
            .lock()
            .map_err(|_| AgentHostError::Runtime("active session registry failed".to_owned()))?
            .remove(&request.runtime_session_id);
        let finish = finish_result?;
        persist_result?;

        let pending_interaction_id = match finish {
            TurnFinish::Completed => None,
            TurnFinish::NeedsInput { request } => Some(request.to_string()),
            TurnFinish::Cancelled { .. } => {
                return Err(AgentHostError::Runtime("turn cancelled".to_owned()));
            }
            TurnFinish::LimitReached { limit } => {
                return Err(AgentHostError::TurnLimitReached {
                    limit: limit.into(),
                });
            }
            TurnFinish::Failed => {
                return Err(AgentHostError::Runtime(match last_turn_error {
                    Some(detail) => format!("turn failed: {detail}"),
                    None => "turn failed".to_owned(),
                }));
            }
        };
        let history = session.history();
        let text = history
            .iter()
            .rev()
            .find(|message| message.role == Role::Assistant)
            .map(|message| {
                message
                    .content
                    .iter()
                    .filter_map(ContentPart::as_text)
                    .collect::<Vec<_>>()
                    .join("")
            })
            .unwrap_or_default();
        let snapshot = session.snapshot();
        let context_manifest =
            RuntimeContextManifestLink::from_snapshot(&snapshot).map(|manifest| {
                manifest.with_lcm_binding(
                    lcm_timeline_id,
                    lcm_binding_revision,
                    FORGE_LCM_STORE_REVISION,
                )
            });
        let usage = snapshot.usage.total();
        Ok(AgentTurnOutput {
            runtime_session_id: request.runtime_session_id,
            text,
            input_tokens: usage.input_tokens(),
            output_tokens: usage
                .get(CounterKind::Output)
                .saturating_add(usage.get(CounterKind::Reasoning)),
            context_manifest,
            pending_interaction_id,
        })
    }

    async fn cancel(&self, runtime_session_id: &str) -> Result<(), AgentHostError> {
        let session = self
            .active
            .lock()
            .map_err(|_| AgentHostError::Runtime("active session registry failed".to_owned()))?
            .get(runtime_session_id)
            .cloned()
            .ok_or(AgentHostError::SessionNotFound)?;
        session
            .interrupt_current_turn(CancelReason::UserRequested)
            .map_err(|error| AgentHostError::Runtime(error.to_string()))?;
        Ok(())
    }

    async fn steer(&self, runtime_session_id: &str, content: String) -> Result<(), AgentHostError> {
        let session = self
            .active
            .lock()
            .map_err(|_| AgentHostError::Runtime("active session registry failed".to_owned()))?
            .get(runtime_session_id)
            .cloned()
            .ok_or(AgentHostError::SessionNotFound)?;
        session
            .steer_current_turn(None, UserInput::text(content))
            .map_err(|error| AgentHostError::Runtime(error.to_string()))?;
        Ok(())
    }
}

/// Builds the bounded `ToolResultSummary` this turn attaches to a completed
/// tool call.
///
/// A native Forge command result already carries a full
/// `OrchestrationOutcome` serialized as the tool's JSON value (see
/// `typed_tools::provider_result_to_tool_outcome`); when the value round-trips
/// through that exact shape, its already-redacted fields are reused
/// unchanged. Any other tool result — a worktree read/write/command, public
/// search, or a raw runtime failure — is not vetted safe to echo verbatim, so
/// it receives a fixed, generic summary instead of a message built from its
/// content.
fn tool_result_summary(
    call_id: &str,
    result: &Result<ToolOutcome, RuntimeError>,
) -> ToolResultSummary {
    match result {
        Ok(outcome) => serde_json::from_value::<OrchestrationOutcome>(outcome.value.clone())
            .map(|outcome| ToolResultSummary::from_orchestration_outcome(&outcome))
            .unwrap_or_else(|_| ToolResultSummary::unclassified(outcome.is_error, call_id)),
        Err(_error) => ToolResultSummary::unclassified(true, call_id),
    }
}

fn scope_type_name(scope: CanonicalScopeType) -> &'static str {
    match scope {
        CanonicalScopeType::Account => "account",
        CanonicalScopeType::Project => "project",
        CanonicalScopeType::AgentChat => "agent_chat",
        CanonicalScopeType::Task => "task",
    }
}

/// Build the fail-closed workspace boundary for one server-authorized scope.
///
/// The runtime's workspace contract deliberately answers only whether a path
/// is inside a boundary.  Forge keeps the higher-level read/write distinction
/// in the canonical scope/tool policy and in the existing Task reviewer
/// worktree restoration path; this adapter makes sure only Task scopes receive
/// a repository root at all.
fn workspace_for_scope(
    scope: &CanonicalScope,
    workspace_path: Option<&str>,
) -> Result<Arc<dyn agent_runtime::core::workspace::Workspace>, AgentHostError> {
    match scope.scope_type {
        CanonicalScopeType::Task => {
            let path = workspace_path
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    AgentHostError::Authority(
                        "Task scope requires a host-issued workspace path".to_owned(),
                    )
                })?;
            let canonical = std::fs::canonicalize(path).map_err(|_| {
                AgentHostError::Authority(
                    "Task scope workspace path is not an existing directory".to_owned(),
                )
            })?;
            if !canonical.is_dir() {
                return Err(AgentHostError::Authority(
                    "Task scope workspace path is not a directory".to_owned(),
                ));
            }
            Ok(Arc::new(TaskWorkspace::new(canonical)))
        }
        CanonicalScopeType::AgentChat
            if scope.workspace_access == WorkspaceAccess::ProjectVerify =>
        {
            let path = workspace_path
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    AgentHostError::Authority(
                        "Project verification scope requires a host-issued checkout".to_owned(),
                    )
                })?;
            let canonical = std::fs::canonicalize(path).map_err(|_| {
                AgentHostError::Authority(
                    "Project verification checkout is not an existing directory".to_owned(),
                )
            })?;
            if !canonical.is_dir() {
                return Err(AgentHostError::Authority(
                    "Project verification checkout is not a directory".to_owned(),
                ));
            }
            Ok(Arc::new(TaskWorkspace::new(canonical)))
        }
        CanonicalScopeType::Account
        | CanonicalScopeType::Project
        | CanonicalScopeType::AgentChat => {
            if workspace_path.is_some() {
                return Err(AgentHostError::Authority(
                    "non-Task scope cannot receive a workspace path".to_owned(),
                ));
            }
            Ok(Arc::new(DenyAllWorkspace))
        }
    }
}

/// A filesystem-aware, fail-closed Task workspace boundary.
///
/// Existing paths are canonicalized before the component-aware boundary check.
/// For a not-yet-created path, the nearest existing ancestor is canonicalized;
/// this prevents a symlinked directory from redirecting a later write outside
/// the admitted root while still allowing tools to create new files.
#[derive(Debug, Clone)]
struct TaskWorkspace {
    root: PathBuf,
}

impl TaskWorkspace {
    fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        Self {
            root: std::fs::canonicalize(&root).unwrap_or(root),
        }
    }
}

impl agent_runtime::core::workspace::Workspace for TaskWorkspace {
    fn root(&self) -> &str {
        self.root.to_str().unwrap_or("<invalid-task-workspace>")
    }

    fn contains(&self, path: &str) -> bool {
        if path.is_empty()
            || Path::new(path)
                .components()
                .any(|component| component == Component::ParentDir)
        {
            return false;
        }
        let root = &self.root;
        let candidate = Path::new(path);
        let candidate = if candidate.is_absolute() {
            candidate.to_path_buf()
        } else {
            root.join(candidate)
        };
        let Ok(relative) = candidate.strip_prefix(root) else {
            return false;
        };
        let mut current = root.clone();
        for component in relative.components() {
            let Component::Normal(component) = component else {
                continue;
            };
            current.push(component);
            match std::fs::symlink_metadata(&current) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    // Do not follow symlinks even when their current target is
                    // inside the root.  This closes both existing escapes and
                    // broken-link write escapes where `Path::exists()` would
                    // otherwise skip the link and accept its parent.
                    return false;
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    // New descendants are allowed after the last existing,
                    // non-symlink parent.  The typed write tool rechecks after
                    // creating parents to close the create-then-follow race.
                    return true;
                }
                Err(_) => return false,
            }
        }
        std::fs::canonicalize(&candidate)
            .map(|canonical| canonical.as_path() == root.as_path() || canonical.starts_with(root))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod workspace_tests {
    use super::*;
    use crate::WorkspaceAccess;
    use agent_runtime::core::workspace::Workspace;

    #[test]
    fn task_workspace_is_component_bounded() {
        let root =
            std::env::temp_dir().join(format!("forge-task-workspace-{}", std::process::id()));
        std::fs::create_dir_all(root.join("src")).expect("workspace creates");
        let workspace = TaskWorkspace::new(&root);
        let canonical_root = PathBuf::from(workspace.root());
        assert!(workspace.contains(workspace.root()));
        assert!(workspace.contains(canonical_root.join("src/main.rs").to_str().unwrap()));
        assert!(
            !workspace.contains(
                canonical_root
                    .parent()
                    .unwrap()
                    .join("forge-task-workspace-sibling/src/main.rs")
                    .to_str()
                    .unwrap()
            )
        );
        assert!(
            !workspace.contains(
                canonical_root
                    .join("../forge-task-workspace-sibling")
                    .to_str()
                    .unwrap()
            )
        );
        std::fs::remove_dir_all(root).expect("workspace cleans");
    }

    #[cfg(unix)]
    #[test]
    fn task_workspace_rejects_symlinked_read_and_write_paths() {
        use std::os::unix::fs::symlink;

        let root = std::env::temp_dir().join(format!(
            "forge-task-workspace-symlink-{}",
            std::process::id()
        ));
        let outside = std::env::temp_dir().join(format!(
            "forge-task-workspace-outside-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).expect("workspace creates");
        std::fs::create_dir_all(&outside).expect("outside creates");
        std::fs::write(outside.join("secret.txt"), "outside").expect("outside file writes");
        symlink(&outside, root.join("linked")).expect("symlink creates");
        symlink(outside.join("does-not-exist"), root.join("broken-link"))
            .expect("broken symlink creates");

        let workspace = TaskWorkspace::new(&root);
        assert!(!workspace.contains(root.join("linked/secret.txt").to_str().unwrap()));
        assert!(!workspace.contains(root.join("linked/new.txt").to_str().unwrap()));
        assert!(!workspace.contains(root.join("broken-link/new.txt").to_str().unwrap()));

        std::fs::remove_dir_all(root).expect("workspace cleans");
        std::fs::remove_dir_all(outside).expect("outside cleans");
    }

    #[test]
    fn non_task_scopes_cannot_supply_workspace() {
        for scope_type in [
            CanonicalScopeType::Account,
            CanonicalScopeType::Project,
            CanonicalScopeType::AgentChat,
        ] {
            let scope = CanonicalScope {
                scope_type,
                scope_id: "scope-1".to_owned(),
                workspace_access: WorkspaceAccess::Deny,
            };
            let error = workspace_for_scope(&scope, Some("/tmp/repo")).unwrap_err();
            assert!(matches!(error, AgentHostError::Authority(_)));
            let deny_all = workspace_for_scope(&scope, None).expect("deny-all workspace");
            assert!(!deny_all.contains("/tmp/repo/file.rs"));
        }
    }

    #[test]
    fn task_scope_requires_a_host_issued_workspace() {
        for access in [WorkspaceAccess::TaskRead, WorkspaceAccess::TaskWrite] {
            let scope = CanonicalScope {
                scope_type: CanonicalScopeType::Task,
                scope_id: "task-1".to_owned(),
                workspace_access: access,
            };
            let error = workspace_for_scope(&scope, None).unwrap_err();
            assert!(matches!(error, AgentHostError::Authority(_)));
        }
    }

    #[test]
    fn canonical_scope_only_grants_task_read_or_write() {
        for scope_type in [
            CanonicalScopeType::Account,
            CanonicalScopeType::Project,
            CanonicalScopeType::AgentChat,
        ] {
            assert!(
                CanonicalScope {
                    scope_type,
                    scope_id: "scope-1".to_owned(),
                    workspace_access: WorkspaceAccess::Deny,
                }
                .validate()
                .is_ok()
            );
            for access in [WorkspaceAccess::TaskRead, WorkspaceAccess::TaskWrite] {
                assert!(
                    CanonicalScope {
                        scope_type,
                        scope_id: "scope-1".to_owned(),
                        workspace_access: access,
                    }
                    .validate()
                    .is_err()
                );
            }
        }
        for access in [WorkspaceAccess::TaskRead, WorkspaceAccess::TaskWrite] {
            assert!(
                CanonicalScope {
                    scope_type: CanonicalScopeType::Task,
                    scope_id: "task-1".to_owned(),
                    workspace_access: access,
                }
                .validate()
                .is_ok()
            );
        }
        assert!(
            CanonicalScope {
                scope_type: CanonicalScopeType::Task,
                scope_id: "task-1".to_owned(),
                workspace_access: WorkspaceAccess::Deny,
            }
            .validate()
            .is_err()
        );
    }
}

#[cfg(test)]
mod tool_result_summary_tests {
    use super::*;
    use api_types::{CanonicalScopeRef, OutcomeCode, OutcomeScopeType, OutcomeStatus, RetryAction};

    #[test]
    fn reunites_a_structured_forge_outcome_with_the_runtime_event_boundary() {
        // Reproduces F14: the runtime event only carries `is_error`, so a
        // native Forge command's typed outcome must be recovered from the
        // tool's JSON value rather than lost at this boundary.
        let outcome = OrchestrationOutcome::failed(
            OutcomeCode::PolicyDenied,
            "task.adaptive",
            CanonicalScopeRef::new(OutcomeScopeType::Task, "task-1"),
            "corr-task-1",
            "the operation is not admitted for the current Forge scope",
        );
        let tool_outcome = ToolOutcome {
            value: serde_json::to_value(&outcome).expect("outcome serializes"),
            content: Default::default(),
            is_error: true,
        };

        let summary = tool_result_summary("call-1", &Ok(tool_outcome));

        assert_eq!(summary.status, OutcomeStatus::Failed);
        assert_eq!(summary.code, OutcomeCode::PolicyDenied);
        assert_eq!(
            summary.safe_message,
            "the operation is not admitted for the current Forge scope"
        );
        assert_eq!(summary.correlation_id, "corr-task-1");
    }

    #[test]
    fn preserves_retry_and_recovery_from_a_structured_outcome() {
        let mut outcome = OrchestrationOutcome::failed(
            OutcomeCode::VersionConflict,
            "project.execution_baseline",
            CanonicalScopeRef::new(OutcomeScopeType::Project, "project-1"),
            "corr-baseline-1",
            "the authorized resource changed; refresh current state and retry",
        );
        outcome.retry = Some(api_types::RetryInstruction::new(
            RetryAction::RefreshAndRetry,
            true,
        ));
        let tool_outcome = ToolOutcome {
            value: serde_json::to_value(&outcome).expect("outcome serializes"),
            content: Default::default(),
            is_error: true,
        };

        let summary = tool_result_summary("call-2", &Ok(tool_outcome));

        assert!(summary.retryable);
        assert_eq!(summary.recovery_action, Some(RetryAction::RefreshAndRetry));
    }

    #[test]
    fn falls_back_to_a_generic_bounded_summary_for_worktree_results() {
        // A Task worktree command (e.g. `forge_task_command` running `git
        // commit`) never carries an `OrchestrationOutcome` — its JSON value
        // is arbitrary process stdout/stderr, which must not be echoed as a
        // safe message.
        let tool_outcome = ToolOutcome {
            value: serde_json::json!({
                "program": "git",
                "args": ["commit"],
                "status": 1,
                "success": false,
                "stdout": "",
                "stderr": "fatal: uncommitted worktree changes with SECRET_TOKEN=abc123",
            }),
            content: Default::default(),
            is_error: false,
        };

        let summary = tool_result_summary("call-3", &Ok(tool_outcome));

        assert_eq!(summary.status, OutcomeStatus::Succeeded);
        assert_eq!(summary.code, OutcomeCode::Ok);
        assert_eq!(summary.correlation_id, "call-3");
        let serialized = serde_json::to_string(&summary).expect("summary serializes");
        assert!(!serialized.contains("SECRET_TOKEN"));
    }

    #[test]
    fn falls_back_to_a_generic_bounded_summary_for_a_raw_runtime_failure() {
        let error = RuntimeError::tool("Task command failed: SECRET_TOKEN=abc123 leaked in stderr");

        let summary = tool_result_summary("call-4", &Err(error));

        assert_eq!(summary.status, OutcomeStatus::Failed);
        assert_eq!(summary.code, OutcomeCode::InternalFailure);
        assert_eq!(summary.correlation_id, "call-4");
        let serialized = serde_json::to_string(&summary).expect("summary serializes");
        assert!(!serialized.contains("SECRET_TOKEN"));
    }
}
