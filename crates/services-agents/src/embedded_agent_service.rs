use operation_registry::authority::permission_set;
use services::provider_credentials::{
    default_api_key_base_url, entry_base_url, provider_egress_refusal, provider_url_addresses,
    validate_provider_url_shape,
};
use std::{collections::BTreeSet, sync::Arc, time::Duration};

use cli_adapters::codex::codex_reasoning_efforts_for_model;
#[cfg(test)]
use cli_adapters::codex::CODEX_REASONING_EFFORTS;
#[cfg(test)]
use config::is_restricted_public_address;
use config::PublicSearchConfig;
use db::{
    new_uuid_v4, now_rfc3339, AccountMainAgentBindingRepo, Agent, AgentChatRepo,
    AgentConnectionHealth, AgentConnectionHealthRepo, AgentContextScopeRepo, AgentProfile,
    AgentProfileRepo, AgentRepo, AgentSession, AgentSessionRepo, AgentStatus, AssigneeKind,
    CreateAgentContextScope, CreateAgentIdentity, CreateAgentProfile, CreateAgentSession,
    CredentialHandle, CredentialHandleRepo, ExecutionRepo, ProjectAgentBindingRepo,
    ProjectMemberRepo, ProjectRepo, RotateAgentSession, SelectAgentProfile, SqliteDb, TaskRepo,
    TaskRoleAssignmentRepo, UpdateAgentSession, UpsertAgentConnectionHealth,
};
#[cfg(test)]
use db::{PageRequest, SortBy, SortOrder};
#[cfg(test)]
use forge_agent_host::outbound_http::PublicHttpError;
use forge_agent_host::outbound_http::{
    public_http_client_builder, ProxyEnvironment, PublicAddressPolicy, UNUSABLE_PROXY_MESSAGE,
};
use forge_agent_host::{
    AgentSessionBackend, BackendCapabilities, CanonicalScope, CanonicalScopeType, CommandAllowlist,
    CreateOAuthCredential, InteractionBrokerHandle, NativeAgentRuntimeBackend,
    OAuthCredentialBundle, Secret, SqliteProtectedRuntimeStore, WorkspaceAccess,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
#[cfg(test)]
use std::net::IpAddr;

use crate::{
    agent_chat_policy::guard_runtime_content,
    native_tools::CoordinationToolProvider,
    workflow::{default_roles, effective_role, engine::WorkflowEngine},
    Result, ServiceError,
};

const NATIVE_EXECUTOR_TYPE: &str = "embedded";
const DEFAULT_CONTEXT_TOKENS: u32 = 128_000;
const DEFAULT_MAX_INPUT_TOKENS: u32 = 96_000;
const DEFAULT_MAX_OUTPUT_TOKENS: u32 = 16_000;
/// The Codex CLI's usage API — `GET /wham/usage` beside the `backend-api`
/// Responses endpoint used by native turns — the only way to read an
/// account's ChatGPT rate-limit consumption without spending a model
/// request.
const CHATGPT_USAGE_ENDPOINT: &str = "https://chatgpt.com/backend-api/wham/usage";

#[derive(Clone)]
pub struct EmbeddedAgentService {
    db: Arc<SqliteDb>,
    protected_store: Arc<SqliteProtectedRuntimeStore>,
    native_backend: Arc<NativeAgentRuntimeBackend>,
    tool_provider: Arc<CoordinationToolProvider>,
    /// Root under which a Project Agent's disposable verification checkout is
    /// provisioned. Absent until the server wires it, in which case a Project
    /// Agent keeps its filesystem-denied scope.
    workspace_root: Arc<std::sync::RwLock<Option<(std::path::PathBuf, std::path::PathBuf)>>>,
    /// The owner-configured command allowlist every workspace turn starts
    /// from, before a Project layers its own settings over it.
    command_allowlist: Arc<std::sync::RwLock<Arc<CommandAllowlist>>>,
}

/// Create a direct (embedded-runtime) agent referencing an existing provider
/// entry. The entry owns the credential; this input never carries one.
#[derive(Clone)]
pub struct CreateEmbeddedAgent {
    pub owner_user_id: String,
    pub name: String,
    pub description: Option<String>,
    pub credential_id: String,
    pub model: String,
    pub reasoning_effort: Option<String>,
    pub system_prompt: Option<String>,
    pub account_permission_ceiling: Value,
    pub tool_policy: Value,
    pub context_tokens: Option<u32>,
    pub max_input_tokens: Option<u32>,
    pub max_output_tokens: Option<u32>,
}

#[derive(Clone)]
pub struct ConnectEmbeddedProfile {
    pub owner_user_id: String,
    pub identity_id: String,
    pub expected_identity_version: i64,
    pub credential_id: String,
    pub model: String,
    pub reasoning_effort: Option<String>,
    pub system_prompt: Option<String>,
    pub permission_policy: Option<String>,
    pub tool_policy: Value,
    pub context_tokens: Option<u32>,
    pub max_input_tokens: Option<u32>,
    pub max_output_tokens: Option<u32>,
}

/// Create an API-key provider entry. Publishing an entry never creates an
/// agent.
#[derive(Clone)]
pub struct ConnectApiKeyCredential {
    pub owner_user_id: String,
    pub provider: String,
    pub label: String,
    pub credential: Secret,
    pub base_url: Option<String>,
}

/// Store a completed OAuth authorization as a provider entry. Publishing an
/// entry never creates an agent.
#[derive(Clone)]
pub struct ConnectOAuthCredential {
    pub owner_user_id: String,
    pub provider: String,
    pub base_url: String,
    pub credential_label: String,
    pub credential: OAuthCredentialBundle,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectedEmbeddedAgent {
    pub agent: Agent,
    pub credential_handle: CredentialHandle,
    pub profile: AgentProfile,
    pub health: AgentConnectionHealth,
    pub session: AgentSession,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RequestedCanonicalScope {
    Account,
    Project { project_id: String },
    AgentChat { chat_id: String },
    Task { task_id: String, role: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateScopedSession {
    pub actor_user_id: String,
    pub identity_id: String,
    pub profile_id: Option<String>,
    pub scope: RequestedCanonicalScope,
}

/// Internal session input used by the Agent Chat turn runner after admission
/// has persisted a complete responder snapshot.  Unlike the interactive
/// session path, this input names the historical binding and Profile
/// revision directly; the runner must never resolve an active replacement for
/// an already-admitted turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateFrozenAgentChatSession {
    pub actor_user_id: String,
    pub identity_id: String,
    pub profile_id: String,
    pub profile_version: i64,
    pub binding_id: String,
    pub binding_version: i64,
    pub policy_revision: String,
    pub policy_digest: String,
    pub permission_policy_digest: String,
    pub tool_policy_digest: String,
    pub scope: RequestedCanonicalScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectivePermissions {
    pub allowed: BTreeSet<String>,
    pub denied: BTreeSet<String>,
    pub requires_approval: BTreeSet<String>,
}

/// Redacted result of a live provider connectivity test.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderEntryTestOutcome {
    pub ok: bool,
    pub latency_ms: u64,
    pub message: Option<String>,
    pub checked_at: String,
}

impl ProviderEntryTestOutcome {
    fn ok(latency_ms: u64, message: Option<String>) -> Self {
        Self {
            ok: true,
            latency_ms,
            message,
            checked_at: now_rfc3339(),
        }
    }

    fn failed(latency_ms: u64, message: String) -> Self {
        Self {
            ok: false,
            latency_ms,
            message: Some(message),
            checked_at: now_rfc3339(),
        }
    }
}

/// One provider-reported rate-limit window, already normalized to the API
/// shape (`window_minutes`, RFC3339 `resets_at`).
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderUsageWindowOutcome {
    pub id: String,
    pub used_percent: f64,
    pub window_minutes: Option<i64>,
    pub resets_at: Option<String>,
}

/// Redacted result of a live provider account-usage probe. `probed` is
/// `false` — with empty `windows` and a `detail` message — whenever the
/// provider isn't probeable or the probe failed. Usage is never fabricated
/// as 0%.
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderUsageOutcome {
    pub provider: String,
    pub probed: bool,
    pub plan_type: Option<String>,
    pub windows: Vec<ProviderUsageWindowOutcome>,
    pub fetched_at: String,
    pub detail: Option<String>,
}

impl ProviderUsageOutcome {
    fn unsupported(provider: String, detail: impl Into<String>) -> Self {
        Self {
            provider,
            probed: false,
            plan_type: None,
            windows: Vec::new(),
            fetched_at: now_rfc3339(),
            detail: Some(detail.into()),
        }
    }
}

/// The string members of a JSON array, ignoring anything else in it.
fn string_list(values: &[Value]) -> Vec<String> {
    values
        .iter()
        .filter_map(|value| value.as_str().map(str::to_owned))
        .collect()
}

impl EmbeddedAgentService {
    pub fn new(db: Arc<SqliteDb>, protected_key_material: &[u8]) -> Self {
        let digest = Sha256::digest(protected_key_material);
        let mut master_key = [0_u8; 32];
        master_key.copy_from_slice(&digest);
        let protected_store = Arc::new(SqliteProtectedRuntimeStore::new(
            Arc::clone(&db),
            master_key,
            1,
        ));
        let tool_provider = Arc::new(CoordinationToolProvider::new(Arc::clone(&db)));
        let native_backend = Arc::new(
            NativeAgentRuntimeBackend::new(Arc::clone(&protected_store))
                .with_forge_tool_provider(tool_provider.clone()),
        );
        Self {
            db,
            protected_store,
            native_backend,
            workspace_root: Arc::new(std::sync::RwLock::new(None)),
            command_allowlist: Arc::new(std::sync::RwLock::new(Arc::new(
                CommandAllowlist::builtin(),
            ))),
            tool_provider,
        }
    }

    /// Apply the owner's configured command policy. Entries that are not bare
    /// program names are dropped and logged here, once at startup, rather
    /// than silently narrowing nothing at every turn.
    pub fn set_command_policy(&self, policy: &config::CommandPolicyConfig) {
        let (allowlist, rejected) =
            CommandAllowlist::resolve(policy.only.as_deref(), &policy.allow);
        if !rejected.is_empty() {
            tracing::warn!(
                rejected = ?rejected,
                "ignored command allowlist entries that are not bare program names"
            );
        }
        tracing::info!(
            programs = allowlist.len(),
            replaced_builtin = policy.only.is_some(),
            "resolved the workspace command allowlist"
        );
        if let Ok(mut slot) = self.command_allowlist.write() {
            *slot = Arc::new(allowlist);
        }
    }

    /// Apply the server's optional public-search configuration to the shared
    /// provider used by all subsequently composed native sessions.
    /// The Task service the server wired in, when there is one.
    pub fn task_service_handle(&self) -> Option<Arc<crate::TaskService>> {
        self.tool_provider.task_service_handle()
    }

    pub fn set_public_search_config(&self, config: Option<PublicSearchConfig>) {
        self.tool_provider.set_public_search_config(config);
    }

    /// Attach the shared TaskService so admitted agent `task.propose`
    /// actions execute inline through the normal Task creation path.
    pub fn set_task_service(&self, task_service: Arc<crate::TaskService>) {
        self.tool_provider.set_task_service(task_service);
    }

    /// CLI transports use the same persisted scope and domain provider as
    /// native turns. Neither a model nor a CLI profile can choose authority.
    pub(crate) async fn cli_chat_tools(
        &self,
        session_id: &str,
        expected_identity: &str,
        expected_scope: &CanonicalScope,
    ) -> Result<forge_agent_host::ScopeToolComposition> {
        let binding = self
            .protected_store
            .cli_chat_scope_binding(session_id)
            .await
            .map_err(redacted_host_error)?;
        if binding.identity_id != expected_identity || &binding.scope != expected_scope {
            return Err(ServiceError::invalid_operation(
                "CLI Chat session authority does not match the admitted turn",
            ));
        }
        let provider = forge_agent_host::authority_bound_provider(
            self.db.clone(),
            &binding,
            self.tool_provider.clone(),
        );
        forge_agent_host::ScopeToolComposition::for_scope_with_permissions_and_project_context(
            binding.identity_id,
            binding.scope,
            None,
            None,
            &binding.allowed_permissions,
            forge_agent_host::ProjectChatToolContext {
                is_project_agent_chat: binding.agent_chat_project_id.is_some(),
                charter_setup_required: binding.project_charter_setup_required,
            },
            Some(provider),
            // Inspection must report the catalog a turn would actually get,
            // so it carries the same transport the backend composes with.
            forge_agent_host::ScopeToolRuntime {
                command_allowlist: None,
                environment: Default::default(),
                fetch_transport: Some(Arc::new(forge_agent_host::ForgeFetchTransport::new())),
            },
        )
        .map_err(redacted_host_error)
    }

    pub async fn chat_session_denials(
        &self,
        identity_id: &str,
        profile_id: &str,
        chat_id: &str,
        session_id: Option<&str>,
    ) -> Vec<(String, api_types::DeniedBy)> {
        self.tool_provider
            .chat_session_denials(identity_id, profile_id, chat_id, session_id)
            .await
    }

    /// Attach the inquiry runner so a Main Chat can dispatch ephemeral
    /// research sub-agents. Wired after construction because the runner needs
    /// a handle back to this service.
    pub fn set_inquiry_runner(&self, runner: Arc<dyn crate::agent_inquiry_runner::InquiryRunner>) {
        self.tool_provider.set_inquiry_runner(runner);
    }

    /// Ingest the worklog and evidence a CLI harness left in its execution
    /// outbox; see [`CoordinationToolProvider::ingest_execution_outbox`].
    pub async fn ingest_execution_outbox(
        &self,
        input: &crate::task_service::execution::ExecutionOutboxInput<'_>,
    ) -> crate::task_service::execution::ExecutionOutboxReport {
        self.tool_provider.ingest_execution_outbox(input).await
    }

    pub async fn ingest_execution_outbox_entries(
        &self,
        input: &crate::task_service::execution::ExecutionOutboxInput<'_>,
        entries: Vec<api_types::ExecutionOutboxEntry>,
    ) -> crate::task_service::execution::ExecutionOutboxReport {
        self.tool_provider
            .ingest_execution_outbox_entries(input, entries)
            .await
    }

    /// Inject an embedded backend while retaining the service's authority graph.
    #[cfg(any(test, feature = "test-support"))]
    pub fn with_native_backend(mut self, backend: Arc<NativeAgentRuntimeBackend>) -> Self {
        self.native_backend = backend;
        self
    }

    pub fn native_backend(&self) -> Arc<NativeAgentRuntimeBackend> {
        Arc::clone(&self.native_backend)
    }

    async fn native_session_retired(&self, session: &AgentSession) -> Result<bool> {
        Ok(session.backend_kind == "native"
            && self
                .db
                .session_persistence_flags(&session.id)
                .await?
                .retired)
    }

    async fn rotate_retired_session(&self, previous: &AgentSession) -> Result<AgentSession> {
        let profile = AgentProfileRepo::get_profile(&*self.db, &previous.profile_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_profile", &previous.profile_id))?;
        match self.rotate_authorized_session(previous, &profile).await {
            Ok(session) => Ok(session),
            Err(error) => {
                let winner = self
                    .activation_winner(error, &previous.identity_id, &previous.context_scope_id)
                    .await?;
                if winner.profile_id != previous.profile_id
                    || self.native_session_retired(&winner).await?
                {
                    return Err(ServiceError::Db(db::DbError::VersionConflict));
                }
                Ok(winner)
            }
        }
    }

    /// Retirement may win after selection but before native writer open. Rotate
    /// the host identity once, then let any second retirement fail the turn.
    pub async fn run_native_turn(
        &self,
        request: &mut forge_agent_host::AgentTurnRequest,
        sink: Arc<dyn forge_agent_host::TurnEventSink>,
    ) -> std::result::Result<forge_agent_host::AgentTurnOutput, forge_agent_host::AgentHostError>
    {
        let first = self
            .native_backend
            .run_turn(request.clone(), sink.clone())
            .await;
        if !matches!(first, Err(forge_agent_host::AgentHostError::SessionRetired)) {
            return first;
        }
        let previous = self
            .db
            .get_agent_session(&request.forge_session_id)
            .await
            .map_err(|_| forge_agent_host::AgentHostError::ProtectedPersistence)?
            .ok_or(forge_agent_host::AgentHostError::SessionNotFound)?;
        if previous.runtime_session_id.as_deref() != Some(&request.runtime_session_id) {
            return Err(forge_agent_host::AgentHostError::VersionConflict);
        }
        let fresh = self
            .rotate_retired_session(&previous)
            .await
            .map_err(|_| forge_agent_host::AgentHostError::ProtectedPersistence)?;
        request.forge_session_id = fresh.id;
        request.runtime_session_id = fresh
            .runtime_session_id
            .ok_or(forge_agent_host::AgentHostError::SessionNotFound)?;
        self.native_backend.run_turn(request.clone(), sink).await
    }

    pub fn protected_store(&self) -> Arc<SqliteProtectedRuntimeStore> {
        Arc::clone(&self.protected_store)
    }

    /// Return the broker composed by the native backend over the same
    /// protected store used for provider credentials and runtime state.
    pub fn interaction_broker(&self) -> InteractionBrokerHandle {
        self.native_backend.interaction_broker()
    }
}

/// Default profile capability ceiling for direct (embedded-runtime) agents.
/// A profile is a ceiling, not a scope grant: account ceiling, membership,
/// workflow admission, and the canonical scope still intersect server-side
/// and therefore remain decisive.
#[must_use]
pub fn default_profile_tool_policy() -> Value {
    serde_json::json!({
        "allowed": [
            "read_account",
            "read_project",
            "read_agent_chat",
            "read_task",
            "read_memory",
            "propose_task",
            "propose_discovery",
            "propose_project",
            "propose_handoff",
            "propose_message",
            "propose_review",
            "propose_commitment",
            "propose_memory",
            "propose_decision",
            "propose_session",
            "task_read",
            "task_write"
        ]
    })
}

/// The provider-side account id recorded on an entry during OAuth login.
/// The ChatGPT Codex backend requires it as the `chatgpt-account-id` header.
pub fn entry_provider_account_id(entry: &CredentialHandle) -> Option<String> {
    serde_json::from_str::<Value>(&entry.metadata_json)
        .ok()
        .and_then(|metadata| {
            metadata
                .get("provider_account_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
}

fn validate_public_runtime_text(value: Option<&str>) -> Result<Option<String>> {
    value
        .map(|text| guard_runtime_content(text).map(|guarded| guarded.content))
        .transpose()
}

fn validate_public_runtime_json(value: &Value) -> Result<()> {
    match value {
        Value::String(text) => {
            guard_runtime_content(text)?;
        }
        Value::Array(values) => {
            for value in values {
                validate_public_runtime_json(value)?;
            }
        }
        Value::Object(fields) => {
            for (key, value) in fields {
                let key = key.to_ascii_lowercase();
                if is_protected_runtime_field(&key) && !value.is_null() {
                    return Err(ServiceError::invalid_operation(
                        "protected values cannot be stored in ordinary runtime policy",
                    ));
                }
                validate_public_runtime_json(value)?;
            }
        }
        Value::Bool(_) | Value::Null | Value::Number(_) => {}
    }
    Ok(())
}

fn is_protected_runtime_field(key: &str) -> bool {
    let compact: String = key
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect();
    [
        "api_key",
        "apikey",
        "authorization",
        "credential",
        "password",
        "private_key",
        "secret",
    ]
    .iter()
    .any(|marker| {
        key == *marker || key.contains(marker) || compact.contains(&marker.replace('_', ""))
    }) || key == "token"
        || key.ends_with("_token")
        || compact == "token"
        || compact.ends_with("token")
}

/// Input budget a context window leaves once its reserved output is set
/// aside.  A model's input budget is not an independent quantity -- it is
/// whatever the window holds minus the response it must still fit -- so
/// deriving it keeps the two from drifting apart as either is tuned.
fn input_budget_for(context_tokens: u32, max_output_tokens: u32) -> u32 {
    context_tokens.saturating_sub(max_output_tokens)
}

/// Provider-aware native model limits `(context, max_input, max_output)`.
/// Every current Gemini API model serves a 1M-token context window.
///
/// The generic arm previously carried a flat 96k input budget that was
/// unrelated to its own 128k context window, stranding 32k of the window and
/// failing `budget_exceeded` before the model was called -- a Project Agent
/// turn died at 97,747 of 96,000 once accumulated `tool_result` context
/// crossed the line, with 128k actually available. The budget is derived from
/// the window now, so only Gemini still needs a bespoke arm.
fn native_model_limits(provider: &str) -> (u32, u32, u32) {
    match provider {
        "gemini" => (1_048_576, 800_000, 64_000),
        _ => (
            DEFAULT_CONTEXT_TOKENS,
            input_budget_for(DEFAULT_CONTEXT_TOKENS, DEFAULT_MAX_OUTPUT_TOKENS),
            DEFAULT_MAX_OUTPUT_TOKENS,
        ),
    }
}

/// Effective native model limits at load time. Profiles are immutable
/// (schema-enforced), so rows created before provider-aware defaults carry
/// the old generic triple baked into their config. That exact triple can only
/// be a baked default — never a deliberate user choice of all three values —
/// so it resolves to the provider defaults instead of capping the model.
pub fn effective_native_limits(
    provider: &str,
    context_tokens: u32,
    max_input_tokens: u32,
    max_output_tokens: u32,
) -> (u32, u32, u32) {
    if (context_tokens, max_input_tokens, max_output_tokens)
        == (
            DEFAULT_CONTEXT_TOKENS,
            DEFAULT_MAX_INPUT_TOKENS,
            DEFAULT_MAX_OUTPUT_TOKENS,
        )
    {
        native_model_limits(provider)
    } else {
        (context_tokens, max_input_tokens, max_output_tokens)
    }
}

fn native_config_json(
    base_url: &str,
    provider: &str,
    context_tokens: Option<u32>,
    max_input_tokens: Option<u32>,
    max_output_tokens: Option<u32>,
) -> Value {
    let (default_context, default_input, default_output) = native_model_limits(provider);
    json!({
        "base_url": base_url.trim_end_matches('/'),
        "context_tokens": context_tokens.unwrap_or(default_context),
        "max_input_tokens": max_input_tokens.unwrap_or(default_input),
        "max_output_tokens": max_output_tokens.unwrap_or(default_output),
        "runtime_revision": forge_agent_host::AGENT_RUNTIME_REVISION,
    })
}

fn native_capabilities() -> BackendCapabilities {
    BackendCapabilities {
        native_runtime: true,
        persistent_session: true,
        protected_checkpoints: true,
        lcm: true,
        cancel: true,
        steer: true,
        workspace: WorkspaceAccess::Deny,
    }
}

fn capabilities_for_profile(
    profile: &AgentProfile,
    canonical: &CanonicalScope,
) -> BackendCapabilities {
    if profile.backend_kind == "native" {
        let persistent = !canonical.is_ephemeral_inquiry();
        BackendCapabilities {
            workspace: canonical.workspace_access,
            persistent_session: persistent,
            protected_checkpoints: persistent,
            lcm: persistent && canonical.scope_type != CanonicalScopeType::Task,
            ..native_capabilities()
        }
    } else {
        BackendCapabilities {
            native_runtime: false,
            persistent_session: false,
            protected_checkpoints: false,
            lcm: false,
            cancel: true,
            steer: false,
            workspace: canonical.workspace_access,
        }
    }
}

fn canonical_scope_name(scope: CanonicalScopeType) -> &'static str {
    match scope {
        CanonicalScopeType::Account => "account",
        CanonicalScopeType::Project => "project",
        CanonicalScopeType::AgentChat => "agent_chat",
        CanonicalScopeType::Task => "task",
    }
}

fn parse_canonical_scope_type(value: &str) -> Result<CanonicalScopeType> {
    match value {
        "account" => Ok(CanonicalScopeType::Account),
        "project" => Ok(CanonicalScopeType::Project),
        "agent_chat" => Ok(CanonicalScopeType::AgentChat),
        "task" => Ok(CanonicalScopeType::Task),
        _ => Err(ServiceError::invalid_operation(
            "stored canonical scope type is invalid",
        )),
    }
}

fn workspace_access_name(access: WorkspaceAccess) -> &'static str {
    match access {
        WorkspaceAccess::Deny => "deny",
        WorkspaceAccess::TaskRead => "task_read",
        WorkspaceAccess::TaskWrite => "task_write",
        WorkspaceAccess::ProjectVerify => "project_verify",
        WorkspaceAccess::AccountScratch => "account_scratch",
    }
}

fn parse_workspace_access(value: &str) -> Result<WorkspaceAccess> {
    match value {
        "deny" => Ok(WorkspaceAccess::Deny),
        "task_read" => Ok(WorkspaceAccess::TaskRead),
        "task_write" => Ok(WorkspaceAccess::TaskWrite),
        "project_verify" => Ok(WorkspaceAccess::ProjectVerify),
        "account_scratch" => Ok(WorkspaceAccess::AccountScratch),
        _ => Err(ServiceError::invalid_operation(
            "stored workspace access is invalid",
        )),
    }
}

fn canonical_task_id(scope: &RequestedCanonicalScope) -> Option<String> {
    match scope {
        RequestedCanonicalScope::Task { task_id, .. } => Some(task_id.clone()),
        _ => None,
    }
}

fn canonical_task_role(scope: &RequestedCanonicalScope) -> Option<String> {
    match scope {
        RequestedCanonicalScope::Task { role, .. } => Some(role.clone()),
        _ => None,
    }
}

fn scope_authority_json(user_id: &str, scope: &RequestedCanonicalScope) -> Value {
    json!({
        "issued_to_user_id": user_id,
        "scope": scope,
        "revision": 1,
    })
}

fn validate_frozen_binding_state(
    state: &str,
    current_version: i64,
    admitted_version: i64,
) -> Result<()> {
    let version_matches = current_version == admitted_version
        || (state == "replaced" && current_version == admitted_version + 1);
    if !version_matches {
        return Err(ServiceError::invalid_operation(
            "frozen Agent Chat binding revision is no longer available",
        ));
    }
    if !matches!(state, "active" | "replaced") {
        return Err(ServiceError::invalid_operation(
            "frozen Agent Chat binding is no longer executable",
        ));
    }
    Ok(())
}

fn scope_permission_set(
    scope: &CanonicalScope,
    project_agent_chat: bool,
    project_charter_setup_required: bool,
) -> BTreeSet<String> {
    operation_registry::authority::scope_permissions(
        canonical_scope_name(scope.scope_type),
        match scope.workspace_access {
            WorkspaceAccess::Deny => "deny",
            WorkspaceAccess::TaskRead => "task_read",
            WorkspaceAccess::TaskWrite => "task_write",
            WorkspaceAccess::ProjectVerify => "project_verify",
            WorkspaceAccess::AccountScratch => "account_scratch",
        },
        project_agent_chat,
        project_charter_setup_required,
    )
}

fn known_permissions() -> BTreeSet<String> {
    [
        "read_account",
        "read_project",
        "read_agent_chat",
        "read_task",
        "read_memory",
        "propose_task",
        "propose_discovery",
        "propose_project",
        "propose_handoff",
        "propose_message",
        "propose_review",
        "propose_commitment",
        "propose_memory",
        "propose_decision",
        "propose_session",
        "task_read",
        "task_write",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn intersect_non_empty_layers(layers: &[BTreeSet<String>]) -> BTreeSet<String> {
    let Some(first) = layers.first() else {
        return BTreeSet::new();
    };
    // Every layer is a required ceiling.  Malformed JSON is represented by
    // an empty set and must deny all permissions instead of disappearing
    // from the intersection (which would be fail-open).
    layers.iter().skip(1).fold(first.clone(), |allowed, layer| {
        allowed.intersection(layer).cloned().collect()
    })
}

fn redacted_host_error(error: forge_agent_host::AgentHostError) -> ServiceError {
    match error {
        forge_agent_host::AgentHostError::SessionRetired => ServiceError::SessionRetired,
        forge_agent_host::AgentHostError::CredentialNotFound
        | forge_agent_host::AgentHostError::SessionNotFound
        | forge_agent_host::AgentHostError::VersionConflict => {
            ServiceError::not_found("protected_runtime_resource", "unavailable")
        }
        forge_agent_host::AgentHostError::Authority(message)
        | forge_agent_host::AgentHostError::Configuration(message)
        | forge_agent_host::AgentHostError::Unsupported(message) => {
            ServiceError::invalid_operation(message)
        }
        forge_agent_host::AgentHostError::AgentPaused { agent_id } => {
            ServiceError::AgentPaused { agent_id }
        }
        forge_agent_host::AgentHostError::ProjectPaused { project_id } => {
            ServiceError::ProjectPaused { project_id }
        }
        forge_agent_host::AgentHostError::Runtime(_) => {
            ServiceError::Domain("embedded runtime failed".to_owned())
        }
        forge_agent_host::AgentHostError::RuntimeWithUsage { .. } => {
            ServiceError::Domain("embedded runtime failed".to_owned())
        }
        forge_agent_host::AgentHostError::TurnLimitReached { limit } => {
            ServiceError::Domain(format!("embedded runtime {limit} limit reached"))
        }
        // Structured outcomes are consumed in-band by Forge's typed native
        // tools. If one escapes to the session boundary, keep its model-facing
        // payload out of this generic service error path.
        forge_agent_host::AgentHostError::StructuredOutcome(_) => {
            ServiceError::Domain("embedded runtime returned a command outcome".to_owned())
        }
        forge_agent_host::AgentHostError::ProtectedPersistence => {
            ServiceError::Domain("protected runtime persistence failed".to_owned())
        }
    }
}

/// Whether an execution row can back a native session claiming `task_role`.
///
/// The reviewer role covers both the review runner's own `reviewer` execution
/// and the `auditor` child execution it spawns for the conformance pass.
fn execution_role_serves_task_role(execution_role: &str, task_role: &str) -> bool {
    match task_role {
        "reviewer" => matches!(
            execution_role,
            default_roles::REVIEWER | default_roles::AUDITOR
        ),
        "planner" => execution_role == default_roles::PLANNER,
        _ => matches!(execution_role, default_roles::WORKER | default_roles::CODER),
    }
}

fn task_role_admitted_by_workflow(active_role: Option<&str>, requested_role: &str) -> bool {
    match requested_role {
        "reviewer" => active_role == Some(default_roles::REVIEWER),
        "planner" => active_role == Some(default_roles::PLANNER),
        "worker" => matches!(
            active_role,
            Some(default_roles::WORKER | default_roles::CODER)
        ),
        _ => false,
    }
}

#[async_trait::async_trait]
impl crate::task_service::execution::ExecutionSupport for EmbeddedAgentService {
    async fn capture_verify_evidence(
        &self,
        task_id: &str,
        media_id: &str,
        filename: &str,
        kind: &str,
        caption: &str,
        bytes: &[u8],
    ) -> Result<String> {
        self.tool_provider
            .capture_verify_evidence(task_id, media_id, filename, kind, caption, bytes)
            .await
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))
    }

    async fn inject_provider_env(&self, agent_config: &mut Value) -> Result<()> {
        EmbeddedAgentService::inject_provider_env(self, agent_config).await
    }

    async fn ingest_execution_outbox(
        &self,
        input: &crate::task_service::execution::ExecutionOutboxInput<'_>,
    ) -> crate::task_service::execution::ExecutionOutboxReport {
        EmbeddedAgentService::ingest_execution_outbox(self, input).await
    }

    async fn ingest_execution_outbox_entries(
        &self,
        input: &crate::task_service::execution::ExecutionOutboxInput<'_>,
        entries: Vec<api_types::ExecutionOutboxEntry>,
    ) -> crate::task_service::execution::ExecutionOutboxReport {
        EmbeddedAgentService::ingest_execution_outbox_entries(self, input, entries).await
    }
}

mod authorization;
mod providers;
mod sessions;
mod workspaces;

#[cfg(test)]
use providers::{
    provider_http_client, rfc3339_from_unix_seconds, usage_snapshot_from_wham_json,
    validate_native_reasoning_effort,
};

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "embedded_agent_service_scoped_cli_chat_tests.rs"]
mod scoped_cli_chat_tests;

#[cfg(test)]
#[path = "embedded_agent_provider_security_tests.rs"]
mod provider_security_tests;
