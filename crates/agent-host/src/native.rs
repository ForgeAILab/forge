use std::{
    collections::{HashMap, HashSet},
    fmt,
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
};

use agent_runtime::{
    context::{
        CacheClass, CompactionPolicy, ContextFragment, ContextLane, ContextPosition,
        FragmentContent, FragmentKind, FragmentSource, StructuralCompactor,
    },
    core::{
        cancel::CancelReason,
        catalog::{ModelLimits, ResolvedModelProfile},
        content::{ContentPart, Message, Role, UserInput},
        error::RuntimeError,
        event::{BudgetCategory, RuntimeEvent, TurnFinish},
        ids::{SessionId, ToolCallId},
        provider::{ModelId, Provider, ReasoningConfig},
        provider_credential::ProviderCredentialTarget,
        security::SecuritySubject,
        tool::ToolOutcome,
        usage::{CounterKind, UsageSource},
        workspace::DenyAllWorkspace,
    },
    harness::{
        ComponentDescriptor, ContextContributor, ContextPatch, ContextView, LcmCoordinator,
        LcmCoordinatorPolicy,
    },
    provider::{
        gemini::{GeminiInteractionsConfig, GeminiInteractionsProvider},
        openai::{OpenAiConfig, OpenAiProvider},
        responses::{ResponsesConfig, ResponsesProvider},
    },
    runtime::{RuntimeBuilder, SessionHandle, StartSession, WorkingSetPolicy},
};
use api_types::{OrchestrationOutcome, ToolResultSummary};
use async_trait::async_trait;
use futures_util::StreamExt;

use crate::{
    AgentHostError, AgentSessionBackend, AgentTurnLimit, AgentTurnOutput, AgentTurnRequest,
    AgentTurnTelemetryState, AgentTurnUsageReport, BackendCapabilities, CanonicalScope,
    CanonicalScopeType, FORGE_LCM_STORE_REVISION, ForgeToolProvider, InteractionBrokerHandle,
    ProjectChatToolContext, RuntimeContextManifestLink, ScopeToolComposition, ScopeToolRuntime,
    TurnEventSink, WorkspaceAccess, protected_store::SqliteProtectedRuntimeStore,
    transport::ReqwestTransport,
};

#[derive(Clone)]
pub struct NativeAgentRuntimeBackend {
    protected_store: Arc<SqliteProtectedRuntimeStore>,
    interaction_broker: InteractionBrokerHandle,
    active: Arc<Mutex<HashMap<String, ActiveNativeSession>>>,
    forge_tool_provider: Option<Arc<dyn ForgeToolProvider>>,
    /// The outbound transport behind the runtime's web fetch tool. One client
    /// factory for every turn: the tool is stateless and the transport pins a
    /// freshly resolved address per request.
    fetch_transport: Arc<dyn agent_runtime::harness::FetchTransport>,
    provider_override: Option<Arc<dyn Provider>>,
    working_sets: Arc<std::sync::RwLock<(WorkingSetPolicy, WorkingSetPolicy)>>,
    /// Runtime sessions whose last turn did not complete; their unchanged-
    /// read references are cleared before their next turn.
    failed_topic_reads: Arc<Mutex<HashSet<String>>>,
    /// Random per-backend line that identifies the contributed state card
    /// on the wire (see [`ServerStateCardProvider`]). Never sent.
    state_card_marker: String,
    #[cfg(feature = "test-support")]
    open_barrier: Arc<Mutex<Option<Arc<tokio::sync::Barrier>>>>,
    #[cfg(feature = "test-support")]
    failure_barrier: Arc<Mutex<Option<Arc<tokio::sync::Barrier>>>>,
}

mod failures;
mod runtime_context;
mod runtime_setup;
mod topic_fork;
mod turn_execution;
mod workspace;

pub(crate) use failures::provider_turn_failure;
use failures::{host_runtime_error, native_turn_cleanup, runtime_turn_failure};
use runtime_context::{
    ServerStateCard, ServerStateCardProvider, TopicReadFilter, TopicReadTurn, TopicReadTurnGuard,
    TurnContextMode, clear_topic_reads, retry_aware_input, task_structural_compactor,
    turn_context_mode,
};
use runtime_setup::PreparedNativeRuntime;
pub use topic_fork::{TOPIC_SUMMARY_TIMEOUT, topic_summary_usage_id};
use turn_execution::{ActiveNativeSession, tool_result_summary};
use workspace::workspace_for_scope;

impl NativeAgentRuntimeBackend {
    pub fn new(protected_store: Arc<SqliteProtectedRuntimeStore>) -> Self {
        Self {
            interaction_broker: InteractionBrokerHandle::new(Arc::clone(&protected_store)),
            protected_store,
            active: Arc::new(Mutex::new(HashMap::new())),
            forge_tool_provider: None,
            fetch_transport: Arc::new(crate::ForgeFetchTransport::new()),
            provider_override: None,
            failed_topic_reads: Arc::new(Mutex::new(HashSet::new())),
            state_card_marker: format!("[forge-state-card:{}]\n", db::new_uuid_v4()),
            #[cfg(feature = "test-support")]
            open_barrier: Arc::new(Mutex::new(None)),
            #[cfg(feature = "test-support")]
            failure_barrier: Arc::new(Mutex::new(None)),
            working_sets: Arc::new(std::sync::RwLock::new((
                WorkingSetPolicy {
                    target_tokens: 48_000,
                    hard_tokens: 64_000,
                },
                WorkingSetPolicy {
                    target_tokens: 96_000,
                    hard_tokens: 128_000,
                },
            ))),
        }
    }

    pub fn set_working_sets(
        &self,
        main_target: u32,
        main_hard: u32,
        project_target: u32,
        project_hard: u32,
    ) {
        *self
            .working_sets
            .write()
            .expect("working set lock poisoned") = (
            WorkingSetPolicy {
                target_tokens: main_target,
                hard_tokens: main_hard,
            },
            WorkingSetPolicy {
                target_tokens: project_target,
                hard_tokens: project_hard,
            },
        );
    }

    #[cfg(feature = "test-support")]
    pub fn set_open_barrier_for_test(&self, barrier: Arc<tokio::sync::Barrier>) {
        *self.open_barrier.lock().expect("open barrier lock") = Some(barrier);
    }

    #[cfg(feature = "test-support")]
    pub fn failed_topic_read_count_for_test(&self) -> usize {
        self.failed_topic_reads
            .lock()
            .expect("topic read lock")
            .len()
    }

    #[cfg(feature = "test-support")]
    pub fn set_failure_barrier_for_test(&self, barrier: Arc<tokio::sync::Barrier>) {
        *self.failure_barrier.lock().expect("failure barrier lock") = Some(barrier);
    }

    async fn retired_open_error(
        &self,
        forge_session_id: &str,
        original: AgentHostError,
    ) -> AgentHostError {
        match self
            .protected_store
            .database()
            .session_persistence_flags(forge_session_id)
            .await
        {
            Ok(flags) if flags.retired => AgentHostError::SessionRetired,
            _ => original,
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

fn scope_type_name(scope: CanonicalScopeType) -> &'static str {
    match scope {
        CanonicalScopeType::Account => "account",
        CanonicalScopeType::Project => "project",
        CanonicalScopeType::AgentChat => "agent_chat",
        CanonicalScopeType::Task => "task",
    }
}
