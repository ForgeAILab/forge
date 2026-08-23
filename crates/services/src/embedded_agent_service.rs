use std::{
    collections::BTreeSet,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use config::PublicSearchConfig;
use db::{
    new_uuid_v4, now_rfc3339, AccountMainAgentBindingRepo, Agent, AgentChatRepo,
    AgentConnectionHealth, AgentConnectionHealthRepo, AgentContextScopeRepo, AgentProfile,
    AgentProfileRepo, AgentRepo, AgentSession, AgentSessionRepo, AgentStatus, AssigneeKind,
    CreateAgentContextScope, CreateAgentIdentity, CreateAgentProfile, CreateAgentSession,
    CredentialHandle, CredentialHandleRepo, ExecutionRepo, ExecutionStatus, PageRequest,
    ProjectAgentBindingRepo, ProjectMemberRepo, ProjectRepo, RotateAgentSession,
    SelectAgentProfile, SortBy, SortOrder, SqliteDb, TaskRepo, TaskRoleAssignmentRepo,
    UpdateAgentSession, UpsertAgentConnectionHealth,
};
use forge_agent_host::{
    AgentSessionBackend, BackendCapabilities, CanonicalScope, CanonicalScopeType,
    CreateOAuthCredential, InteractionBrokerHandle, NativeAgentRuntimeBackend,
    OAuthCredentialBundle, Secret, SqliteProtectedRuntimeStore, WorkspaceAccess,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::Row;

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
            tool_provider,
        }
    }

    /// Apply the server's optional public-search configuration to the shared
    /// provider used by all subsequently composed native sessions.
    pub fn set_public_search_config(&self, config: Option<PublicSearchConfig>) {
        self.tool_provider.set_public_search_config(config);
    }

    /// Attach the shared TaskService so admitted agent `task.propose`
    /// actions execute inline through the normal Task creation path.
    pub fn set_task_service(&self, task_service: Arc<crate::TaskService>) {
        self.tool_provider.set_task_service(task_service);
    }

    pub fn native_backend(&self) -> Arc<NativeAgentRuntimeBackend> {
        Arc::clone(&self.native_backend)
    }

    pub fn protected_store(&self) -> Arc<SqliteProtectedRuntimeStore> {
        Arc::clone(&self.protected_store)
    }

    /// Return the broker composed by the native backend over the same
    /// protected store used for provider credentials and runtime state.
    pub fn interaction_broker(&self) -> InteractionBrokerHandle {
        self.native_backend.interaction_broker()
    }

    /// Create an API-key provider entry after validating the provider,
    /// endpoint, and key shape. No agent, profile, or binding changes.
    pub async fn connect_api_key_credential(
        &self,
        input: ConnectApiKeyCredential,
    ) -> Result<CredentialHandle> {
        if input.label.trim().is_empty() || input.credential.expose().trim().is_empty() {
            return Err(ServiceError::invalid_operation(
                "label and credential are required",
            ));
        }
        if !matches!(
            input.provider.as_str(),
            "openai" | "xai" | "gemini" | "openai_compatible" | "openrouter"
        ) {
            return Err(ServiceError::invalid_operation(
                "provider is not supported by the embedded runtime",
            ));
        }
        let base_url = match input.base_url.as_deref().map(str::trim) {
            Some(value) if !value.is_empty() => value.to_owned(),
            _ => default_api_key_base_url(&input.provider)
                .ok_or_else(|| {
                    ServiceError::invalid_operation("base_url is required for this provider")
                })?
                .to_owned(),
        };
        provider_url_addresses(&base_url)
            .await
            .map_err(ServiceError::invalid_operation)?;
        let now = now_rfc3339();
        let credential_id = new_uuid_v4();
        let handle = self
            .protected_store
            .create_credential(
                &credential_id,
                &input.owner_user_id,
                &input.provider,
                &input.label,
                input.credential.clone(),
                &now,
            )
            .await
            .map_err(redacted_host_error)?;
        self.record_entry_base_url(&credential_id, &base_url).await;
        Ok(CredentialHandle {
            metadata_json: json!({ "base_url": base_url }).to_string(),
            ..handle
        })
    }

    /// Store a completed OAuth authorization as a provider entry. No agent,
    /// profile, or binding changes.
    pub async fn connect_oauth_credential(
        &self,
        input: ConnectOAuthCredential,
    ) -> Result<CredentialHandle> {
        if input.provider.trim().is_empty()
            || input.credential.access_token.trim().is_empty()
            || input.credential.refresh_token.trim().is_empty()
        {
            return Err(ServiceError::invalid_operation(
                "OAuth connection is missing required provider or token data",
            ));
        }
        if !matches!(input.provider.as_str(), "openai" | "xai" | "gemini") {
            return Err(ServiceError::invalid_operation(
                "provider does not support Forge-managed OAuth",
            ));
        }
        provider_url_addresses(&input.base_url)
            .await
            .map_err(ServiceError::invalid_operation)?;
        let now = now_rfc3339();
        let credential_id = new_uuid_v4();
        let metadata_json = json!({
            "base_url": input.base_url.trim_end_matches('/'),
            "scopes": input.credential.scopes.clone(),
            "provider_account_id": input.credential.provider_account_id.clone(),
        })
        .to_string();
        self.protected_store
            .create_oauth_credential(CreateOAuthCredential {
                id: &credential_id,
                owner_user_id: &input.owner_user_id,
                provider: &input.provider,
                label: &input.credential_label,
                bundle: &input.credential,
                metadata_json: &metadata_json,
                now: &now,
            })
            .await
            .map_err(redacted_host_error)
    }

    /// Resolve a provider entry owned by the caller and still usable.
    pub async fn require_owned_entry(
        &self,
        owner_user_id: &str,
        credential_id: &str,
    ) -> Result<CredentialHandle> {
        let handle = CredentialHandleRepo::get_credential_handle(&*self.db, credential_id)
            .await?
            .filter(|handle| handle.owner_user_id == owner_user_id)
            .ok_or_else(|| {
                ServiceError::not_found("credential_handle", credential_id.to_owned())
            })?;
        if handle.status != "configured" {
            return Err(ServiceError::invalid_operation(
                "provider entry is disconnected",
            ));
        }
        Ok(handle)
    }

    /// Make one minimal authenticated request against the entry's API to
    /// check the provider is responding and the credential is accepted.
    /// The outcome message is always redacted: HTTP status classes and
    /// transport error kinds only, never credential material or bodies.
    pub async fn test_provider_entry(
        &self,
        owner_user_id: &str,
        credential_id: &str,
    ) -> Result<ProviderEntryTestOutcome> {
        let entry = self
            .require_owned_entry(owner_user_id, credential_id)
            .await?;
        let base_url = entry_base_url(&entry)?;
        provider_url_addresses(&base_url)
            .await
            .map_err(ServiceError::invalid_operation)?;
        let secret = self
            .protected_store
            .acquire_provider_credential(owner_user_id, credential_id, 60_000)
            .await
            .map_err(redacted_host_error)?;

        let base = base_url.trim_end_matches('/');
        let is_oauth = entry.credential_method == "oauth_bundle";
        // ChatGPT OAuth has no cheap read endpoint; probing the base URL still
        // exercises DNS, TLS, and the authorization layer.
        let openai_oauth = is_oauth && entry.provider == "openai";
        let probe_url = match entry.provider.as_str() {
            "openrouter" => format!("{base}/key"),
            "openai" if openai_oauth => base.to_owned(),
            _ => format!("{base}/models"),
        };
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| ServiceError::invalid_operation("provider test client unavailable"))?;
        let mut request = client.get(&probe_url);
        request = if entry.provider == "gemini" && !is_oauth {
            request.header("x-goog-api-key", secret.expose())
        } else {
            request.bearer_auth(secret.expose())
        };

        let started = std::time::Instant::now();
        let outcome = match request.send().await {
            Ok(response) => {
                let latency_ms = started.elapsed().as_millis() as u64;
                let status = response.status();
                if status.is_success() {
                    ProviderEntryTestOutcome::ok(latency_ms, None)
                } else if status.as_u16() == 401 || status.as_u16() == 403 {
                    ProviderEntryTestOutcome::failed(
                        latency_ms,
                        format!(
                            "provider rejected the credential (HTTP {})",
                            status.as_u16()
                        ),
                    )
                } else if openai_oauth {
                    // Auth middleware answers before routing: a non-401/403
                    // response means the token was accepted.
                    ProviderEntryTestOutcome::ok(
                        latency_ms,
                        Some("endpoint reachable; authorization accepted".to_owned()),
                    )
                } else {
                    ProviderEntryTestOutcome::failed(
                        latency_ms,
                        format!("provider returned HTTP {}", status.as_u16()),
                    )
                }
            }
            Err(error) => {
                let latency_ms = started.elapsed().as_millis() as u64;
                let reason = if error.is_timeout() {
                    "provider did not respond within 10 seconds"
                } else if error.is_connect() {
                    "provider could not be reached"
                } else {
                    "provider request failed before a response arrived"
                };
                ProviderEntryTestOutcome::failed(latency_ms, reason.to_owned())
            }
        };
        Ok(outcome)
    }

    /// Fetch the entry's provider-side account usage (rate-limit windows)
    /// when the provider supports a usage probe. Only ChatGPT-OAuth (Codex
    /// backend) entries are probeable today; every other entry — and any
    /// probe failure — reports `probed: false` with a redacted `detail`.
    /// Usage is never fabricated as 0%.
    pub async fn usage_provider_entry(
        &self,
        owner_user_id: &str,
        credential_id: &str,
    ) -> Result<ProviderUsageOutcome> {
        let entry = self
            .require_owned_entry(owner_user_id, credential_id)
            .await?;
        let base_url = entry_base_url(&entry)?;
        let is_codex_oauth = entry.credential_method == "oauth_bundle"
            && entry.provider == "openai"
            && is_codex_backend(&base_url);
        if !is_codex_oauth {
            return Ok(ProviderUsageOutcome::unsupported(
                entry.provider,
                "usage probe not supported for this provider",
            ));
        }

        let secret = match self
            .protected_store
            .acquire_provider_credential(owner_user_id, credential_id, 60_000)
            .await
        {
            Ok(secret) => secret,
            Err(error) => {
                let message = match redacted_host_error(error) {
                    ServiceError::NotFound { .. } => {
                        "provider credential is unavailable".to_owned()
                    }
                    _ => "provider credential could not be refreshed".to_owned(),
                };
                return Ok(ProviderUsageOutcome::unsupported(entry.provider, message));
            }
        };

        let account_id = entry_provider_account_id(&entry);
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
        {
            Ok(client) => client,
            Err(_) => {
                return Ok(ProviderUsageOutcome::unsupported(
                    entry.provider,
                    "usage probe client unavailable",
                ));
            }
        };
        let mut request = client
            .get(CHATGPT_USAGE_ENDPOINT)
            .header("originator", "codex_cli_rs")
            .bearer_auth(secret.expose());
        if let Some(account_id) = account_id.as_deref() {
            request = request.header("chatgpt-account-id", account_id);
        }

        Ok(match request.send().await {
            Ok(response) if response.status().is_success() => match response.text().await {
                Ok(body) => match usage_snapshot_from_wham_json(&body, now_unix_seconds()) {
                    Ok((plan_type, windows)) => ProviderUsageOutcome {
                        provider: entry.provider,
                        probed: true,
                        plan_type,
                        windows,
                        fetched_at: now_rfc3339(),
                        detail: None,
                    },
                    Err(_) => ProviderUsageOutcome::unsupported(
                        entry.provider,
                        "usage probe returned an unreadable response",
                    ),
                },
                Err(_) => ProviderUsageOutcome::unsupported(
                    entry.provider,
                    "usage probe response could not be read",
                ),
            },
            Ok(response) => ProviderUsageOutcome::unsupported(
                entry.provider,
                format!("usage probe returned HTTP {}", response.status().as_u16()),
            ),
            Err(error) => {
                let reason = if error.is_timeout() {
                    "usage probe did not respond within 10 seconds"
                } else if error.is_connect() {
                    "usage probe could not be reached"
                } else {
                    "usage probe request failed before a response arrived"
                };
                ProviderUsageOutcome::unsupported(entry.provider, reason)
            }
        })
    }

    pub async fn create_agent_from_entry(
        &self,
        input: CreateEmbeddedAgent,
    ) -> Result<ConnectedEmbeddedAgent> {
        if input.name.trim().is_empty() || input.model.trim().is_empty() {
            return Err(ServiceError::invalid_operation(
                "name and model are required",
            ));
        }
        let entry = self
            .require_owned_entry(&input.owner_user_id, &input.credential_id)
            .await?;
        let base_url = entry_base_url(&entry)?;
        let system_prompt = validate_public_runtime_text(input.system_prompt.as_deref())?;
        validate_public_runtime_json(&input.account_permission_ceiling)?;
        validate_public_runtime_json(&input.tool_policy)?;
        let now = now_rfc3339();
        let identity_id = new_uuid_v4();
        let profile_id = new_uuid_v4();
        let capabilities = native_capabilities();
        let profile = CreateAgentProfile {
            id: profile_id.clone(),
            identity_id: identity_id.clone(),
            backend_kind: "native".to_owned(),
            executor_type: NATIVE_EXECUTOR_TYPE.to_owned(),
            provider: Some(entry.provider.clone()),
            model: Some(input.model.clone()),
            reasoning_effort: None,
            permission_policy: Some("scoped_proposals".to_owned()),
            prompt_template: system_prompt,
            capabilities_json: serde_json::to_string(&capabilities).unwrap_or_else(|_| "{}".into()),
            tool_policy_json: input.tool_policy.to_string(),
            config_json: native_config_json(
                &base_url,
                &entry.provider,
                input.context_tokens,
                input.max_input_tokens,
                input.max_output_tokens,
            )
            .to_string(),
            credential_ref: Some(entry.id.clone()),
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        };
        let agent = AgentRepo::create_identity_with_profile(
            &*self.db,
            CreateAgentIdentity {
                id: identity_id.clone(),
                name: input.name,
                description: input.description,
                max_concurrent_tasks: 1,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status: AgentStatus::Idle,
                last_heartbeat_at: Some(now.clone()),
                is_default: false,
                paused: false,
                owner_id: Some(input.owner_user_id.clone()),
                visibility: "account".to_owned(),
                account_permission_ceiling: input.account_permission_ceiling.to_string(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            profile,
        )
        .await?;
        let profile = AgentProfileRepo::get_profile(&*self.db, &profile_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_profile", profile_id.clone()))?;
        let health = self
            .check_and_record_connection(&profile, &input.owner_user_id)
            .await?;
        let session = self
            .create_or_resume_session(CreateScopedSession {
                actor_user_id: input.owner_user_id,
                identity_id,
                profile_id: Some(profile_id),
                scope: RequestedCanonicalScope::Account,
            })
            .await?;
        Ok(ConnectedEmbeddedAgent {
            agent,
            credential_handle: entry,
            profile,
            health,
            session,
        })
    }

    pub async fn connect_profile(
        &self,
        input: ConnectEmbeddedProfile,
    ) -> Result<(Agent, CredentialHandle, AgentProfile, AgentConnectionHealth)> {
        let identity = self
            .require_owned_identity(&input.identity_id, &input.owner_user_id)
            .await?;
        if identity.version != input.expected_identity_version {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }
        if input.model.trim().is_empty() {
            return Err(ServiceError::invalid_operation("model is required"));
        }
        let entry = self
            .require_owned_entry(&input.owner_user_id, &input.credential_id)
            .await?;
        let base_url = entry_base_url(&entry)?;
        let system_prompt = validate_public_runtime_text(input.system_prompt.as_deref())?;
        let account_permission_ceiling = sqlx::query_scalar::<_, String>(
            "SELECT account_permission_ceiling FROM agent_identity WHERE id = ?",
        )
        .bind(&identity.id)
        .fetch_one(self.db.pool())
        .await?;
        let account_permission_ceiling =
            serde_json::from_str(&account_permission_ceiling).unwrap_or(Value::Null);
        validate_public_runtime_json(&account_permission_ceiling)?;
        validate_public_runtime_json(&input.tool_policy)?;
        let permission_policy = validate_public_runtime_text(input.permission_policy.as_deref())?;
        let now = now_rfc3339();
        let profile_id = new_uuid_v4();
        let (profile, agent) = AgentProfileRepo::create_and_select_profile(
            &*self.db,
            CreateAgentProfile {
                id: profile_id.clone(),
                identity_id: input.identity_id.clone(),
                backend_kind: "native".to_owned(),
                executor_type: NATIVE_EXECUTOR_TYPE.to_owned(),
                provider: Some(entry.provider.clone()),
                model: Some(input.model),
                reasoning_effort: None,
                permission_policy: permission_policy
                    .or_else(|| Some("scoped_proposals".to_owned())),
                prompt_template: system_prompt,
                capabilities_json: serde_json::to_string(&native_capabilities())
                    .unwrap_or_else(|_| "{}".into()),
                tool_policy_json: input.tool_policy.to_string(),
                config_json: native_config_json(
                    &base_url,
                    &entry.provider,
                    input.context_tokens,
                    input.max_input_tokens,
                    input.max_output_tokens,
                )
                .to_string(),
                credential_ref: Some(entry.id.clone()),
                daemon_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            SelectAgentProfile {
                identity_id: input.identity_id,
                profile_id: profile_id.clone(),
                expected_version: input.expected_identity_version,
                updated_at: now.clone(),
            },
        )
        .await?;
        let health = self
            .check_and_record_connection(&profile, &input.owner_user_id)
            .await?;
        Ok((agent, entry, profile, health))
    }

    /// Inject the referenced provider entry's API key into an in-memory
    /// executor config snapshot as the provider's environment variable
    /// (`auth_source: forge_provider` dispatch). The mutated value is handed
    /// to the spawned executor only — it is never written back to the
    /// database, events, or logs.
    pub async fn inject_provider_env(&self, agent_config: &mut Value) -> Result<()> {
        let executor_type = agent_config
            .get("executor_type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        if executor_type == NATIVE_EXECUTOR_TYPE {
            return Ok(());
        }
        let Some(agent_id) = agent_config
            .get("agent_id")
            .and_then(Value::as_str)
            .map(str::to_owned)
        else {
            return Ok(());
        };
        let Some(agent) = AgentRepo::get_by_id(&*self.db, &agent_id).await? else {
            return Ok(());
        };
        let Some(credential_ref) = agent.credential_ref.as_deref() else {
            return Ok(());
        };
        let handle = CredentialHandleRepo::get_credential_handle(&*self.db, credential_ref)
            .await?
            .ok_or_else(|| {
                ServiceError::invalid_operation("referenced provider entry is unavailable")
            })?;
        if handle.status != "configured" {
            return Err(ServiceError::invalid_operation(
                "referenced provider entry is disconnected",
            ));
        }
        if handle.credential_method != "api_key" {
            return Err(ServiceError::invalid_operation(
                "referenced provider entry cannot drive a CLI harness",
            ));
        }
        let variable = provider_env_variable(&handle.provider).ok_or_else(|| {
            ServiceError::invalid_operation("provider has no harness environment contract")
        })?;
        let secret = self
            .protected_store
            .acquire_provider_credential(&handle.owner_user_id, &handle.id, 60_000)
            .await
            .map_err(redacted_host_error)?;
        // `CommandOverrides` is `#[serde(flatten)]`ed into every CLI executor
        // config, so the env map lives directly at `config.env` — a nested
        // `command_overrides` object would be silently ignored on
        // deserialization.
        let config = agent_config
            .as_object_mut()
            .and_then(|snapshot| snapshot.get_mut("config"))
            .and_then(Value::as_object_mut)
            .ok_or_else(|| {
                ServiceError::invalid_operation("executor config snapshot is not injectable")
            })?;
        let entry = config.entry("env").or_insert_with(|| json!({}));
        if !entry.is_object() {
            *entry = json!({});
        }
        let env = entry.as_object_mut().ok_or_else(|| {
            ServiceError::invalid_operation("executor config snapshot is not injectable")
        })?;
        env.insert(
            variable.to_owned(),
            Value::String(secret.expose().to_owned()),
        );
        Ok(())
    }

    /// Best-effort persistence of the resolved base URL in entry metadata so
    /// later agent creation does not need to re-derive it.
    async fn record_entry_base_url(&self, credential_id: &str, base_url: &str) {
        let _ = sqlx::query(
            "UPDATE credential_handle
             SET metadata_json = json_set(metadata_json, '$.base_url', ?)
             WHERE id = ?",
        )
        .bind(base_url.trim_end_matches('/'))
        .bind(credential_id)
        .execute(self.db.pool())
        .await;
    }

    pub async fn create_or_resume_session(
        &self,
        input: CreateScopedSession,
    ) -> Result<AgentSession> {
        let identity = self
            .require_owned_identity(&input.identity_id, &input.actor_user_id)
            .await?;
        if identity.paused {
            return Err(ServiceError::AgentPaused {
                agent_id: identity.id,
            });
        }
        let profile_id = input
            .profile_id
            .unwrap_or_else(|| identity.profile_id.clone());
        let profile = AgentProfileRepo::get_profile(&*self.db, &profile_id)
            .await?
            .filter(|profile| profile.identity_id == identity.id)
            .ok_or_else(|| ServiceError::not_found("agent_profile", profile_id.clone()))?;
        let canonical = self
            .authorize_scope(&input.actor_user_id, &identity, &input.scope)
            .await?;
        let project_id = self.scope_project_id(&input.scope).await?;
        self.persist_authorized_session(
            identity,
            profile,
            canonical,
            project_id,
            canonical_task_id(&input.scope),
            canonical_task_role(&input.scope),
            scope_authority_json(&input.actor_user_id, &input.scope),
        )
        .await
    }

    /// Create or resume the session for an already-admitted Agent Chat turn.
    ///
    /// Interactive callers must continue through [`Self::authorize_scope`],
    /// which resolves the current binding.  A queued turn is different: its
    /// binding and Profile were selected at admission, so resolving the
    /// active replacement here would mix authority revisions.  This narrow
    /// internal path validates the retained historical row instead and only
    /// permits a replacement whose version transition is the one produced by
    /// the binding repository.  Runtime command/tool authorization remains
    /// current and therefore can still reject a withdrawn Project ceiling.
    pub(crate) async fn create_or_resume_frozen_chat_session(
        &self,
        input: CreateFrozenAgentChatSession,
    ) -> Result<AgentSession> {
        let RequestedCanonicalScope::AgentChat { chat_id } = &input.scope else {
            return Err(ServiceError::invalid_operation(
                "frozen Agent Chat sessions require an Agent Chat scope",
            ));
        };
        let identity = self
            .require_owned_identity(&input.identity_id, &input.actor_user_id)
            .await?;
        if identity.paused {
            return Err(ServiceError::AgentPaused {
                agent_id: identity.id,
            });
        }
        let profile = AgentProfileRepo::get_profile(&*self.db, &input.profile_id)
            .await?
            .filter(|profile| profile.identity_id == identity.id)
            .ok_or_else(|| ServiceError::not_found("agent_profile", input.profile_id.clone()))?;
        if profile.version != input.profile_version {
            return Err(ServiceError::invalid_operation(
                "frozen Agent Chat Profile revision is no longer available",
            ));
        }
        if input.permission_policy_digest.trim().is_empty()
            || input.tool_policy_digest.trim().is_empty()
        {
            return Err(ServiceError::invalid_operation(
                "frozen Agent Chat policy provenance is incomplete",
            ));
        }

        let chat = AgentChatRepo::get_agent_chat(&*self.db, chat_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_chat", chat_id.clone()))?;
        let project_id = match chat.kind.as_str() {
            "account_main" => {
                if chat.account_id.as_deref() != Some(input.actor_user_id.as_str())
                    || chat.project_id.is_some()
                {
                    return Err(ServiceError::not_found("agent_chat", chat_id.clone()));
                }
                let binding =
                    AccountMainAgentBindingRepo::get_main_binding(&*self.db, &input.binding_id)
                        .await?
                        .filter(|binding| {
                            binding.account_id == input.actor_user_id
                                && binding.identity_id == identity.id
                        })
                        .ok_or_else(|| {
                            ServiceError::invalid_operation(
                                "frozen Main Agent binding history is unavailable",
                            )
                        })?;
                validate_frozen_binding_state(
                    &binding.state,
                    binding.version,
                    input.binding_version,
                )?;
                if binding.tool_policy_revision != input.policy_revision {
                    return Err(ServiceError::invalid_operation(
                        "frozen Main Agent policy revision changed after admission",
                    ));
                }
                if crate::agent_turn_admission::policy_digest(&binding.autonomy_policy_json)?
                    != input.policy_digest
                {
                    return Err(ServiceError::invalid_operation(
                        "frozen Main Agent policy changed after admission",
                    ));
                }
                None
            }
            "project" => {
                if chat.account_id.is_some() {
                    return Err(ServiceError::not_found("agent_chat", chat_id.clone()));
                }
                let project_id = chat.project_id.clone().ok_or_else(|| {
                    ServiceError::invalid_operation("Project Agent Chat has no Project scope")
                })?;
                ProjectMemberRepo::get_member(&*self.db, &project_id, &input.actor_user_id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("project", project_id.clone()))?;
                let binding =
                    ProjectAgentBindingRepo::get_project_binding(&*self.db, &input.binding_id)
                        .await?
                        .filter(|binding| {
                            binding.project_id == project_id
                                && binding.identity_id.as_deref() == Some(identity.id.as_str())
                        })
                        .ok_or_else(|| {
                            ServiceError::invalid_operation(
                                "frozen Project Agent binding history is unavailable",
                            )
                        })?;
                validate_frozen_binding_state(
                    &binding.state,
                    binding.version,
                    input.binding_version,
                )?;
                let policy_row = sqlx::query(
                    "SELECT policy_revision, policy_digest
                     FROM project_agent_binding WHERE id = ? AND project_id = ?",
                )
                .bind(&input.binding_id)
                .bind(&project_id)
                .fetch_one(self.db.pool())
                .await?;
                let policy_revision: String = policy_row.try_get("policy_revision")?;
                let stored_policy_digest: String = policy_row.try_get("policy_digest")?;
                if policy_revision != input.policy_revision
                    || stored_policy_digest != input.policy_digest
                {
                    return Err(ServiceError::invalid_operation(
                        "frozen Project Agent policy changed after admission",
                    ));
                }
                if crate::agent_turn_admission::policy_digest(&binding.permission_ceiling_json)?
                    != input.permission_policy_digest
                {
                    return Err(ServiceError::invalid_operation(
                        "frozen Project Agent permission policy changed after admission",
                    ));
                }
                Some(project_id)
            }
            _ => {
                return Err(ServiceError::invalid_operation(
                    "Agent Chat has an unsupported canonical kind",
                ));
            }
        };
        if crate::agent_turn_admission::policy_digest(&profile.tool_policy_json)?
            != input.tool_policy_digest
        {
            return Err(ServiceError::invalid_operation(
                "frozen Agent Chat Profile policy changed after admission",
            ));
        }

        let canonical = CanonicalScope {
            scope_type: CanonicalScopeType::AgentChat,
            scope_id: chat_id.clone(),
            workspace_access: WorkspaceAccess::Deny,
        };
        canonical
            .validate()
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let mut authority = scope_authority_json(&input.actor_user_id, &input.scope);
        if let Some(object) = authority.as_object_mut() {
            object.insert("frozen_binding_id".to_owned(), json!(input.binding_id));
            object.insert(
                "frozen_binding_version".to_owned(),
                json!(input.binding_version),
            );
            object.insert("frozen_profile_id".to_owned(), json!(input.profile_id));
            object.insert(
                "frozen_profile_version".to_owned(),
                json!(input.profile_version),
            );
            object.insert(
                "frozen_policy_revision".to_owned(),
                json!(input.policy_revision),
            );
            object.insert(
                "frozen_policy_digest".to_owned(),
                json!(input.policy_digest),
            );
            object.insert(
                "frozen_permission_policy_digest".to_owned(),
                json!(input.permission_policy_digest),
            );
            object.insert(
                "frozen_tool_policy_digest".to_owned(),
                json!(input.tool_policy_digest),
            );
        }
        self.persist_authorized_session(
            identity, profile, canonical, project_id, None, None, authority,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn persist_authorized_session(
        &self,
        identity: Agent,
        profile: AgentProfile,
        canonical: CanonicalScope,
        project_id: Option<String>,
        task_id: Option<String>,
        task_role: Option<String>,
        authority_json: Value,
    ) -> Result<AgentSession> {
        let now = now_rfc3339();
        let scope = AgentContextScopeRepo::create_context_scope(
            &*self.db,
            CreateAgentContextScope {
                id: new_uuid_v4(),
                identity_id: identity.id.clone(),
                scope_type: canonical_scope_name(canonical.scope_type).to_owned(),
                scope_id: canonical.scope_id.clone(),
                project_id,
                task_id,
                task_role,
                workspace_access: workspace_access_name(canonical.workspace_access).to_owned(),
                authority_json: authority_json.to_string(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await?;
        if let Some(active) =
            AgentSessionRepo::get_active_agent_session(&*self.db, &identity.id, &scope.id).await?
        {
            if active.profile_id == profile.id {
                return Ok(active);
            }
            return self.rotate_authorized_session(&active, &profile).await;
        }
        let capabilities = capabilities_for_profile(&profile, &canonical);
        let (status, connection_status) = self.initial_session_status(&profile).await?;
        AgentSessionRepo::create_agent_session(
            &*self.db,
            CreateAgentSession {
                id: new_uuid_v4(),
                identity_id: identity.id,
                profile_id: profile.id,
                context_scope_id: scope.id,
                backend_kind: profile.backend_kind.clone(),
                runtime_session_id: (profile.backend_kind == "native").then(new_uuid_v4),
                status,
                capabilities_json: serde_json::to_string(&capabilities)
                    .unwrap_or_else(|_| "{}".to_owned()),
                connection_status,
                predecessor_session_id: None,
                last_activity_at: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .map_err(Into::into)
    }

    pub async fn rotate_session(
        &self,
        actor_user_id: &str,
        session_id: &str,
        expected_version: i64,
    ) -> Result<AgentSession> {
        let previous = AgentSessionRepo::get_agent_session(&*self.db, session_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_session", session_id.to_owned()))?;
        self.require_owned_identity(&previous.identity_id, actor_user_id)
            .await?;
        if previous.version != expected_version {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }
        let profile = AgentProfileRepo::get_profile(&*self.db, &previous.profile_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_profile", previous.profile_id.clone()))?;
        self.rotate_session_for_profile(actor_user_id, &previous, &profile)
            .await
    }

    pub async fn set_session_status(
        &self,
        actor_user_id: &str,
        session_id: &str,
        expected_version: i64,
        status: &str,
    ) -> Result<AgentSession> {
        if !matches!(status, "ready" | "suspended" | "cancelled") {
            return Err(ServiceError::invalid_operation(
                "session status must be ready, suspended, or cancelled",
            ));
        }
        let session = AgentSessionRepo::get_agent_session(&*self.db, session_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_session", session_id.to_owned()))?;
        self.require_owned_identity(&session.identity_id, actor_user_id)
            .await?;
        AgentSessionRepo::update_agent_session(
            &*self.db,
            UpdateAgentSession {
                id: session.id,
                expected_version,
                runtime_session_id: None,
                status: Some(status.to_owned()),
                connection_status: None,
                last_activity_at: None,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .map_err(Into::into)
    }

    pub async fn cancel_session_turn(&self, actor_user_id: &str, session_id: &str) -> Result<()> {
        let session = AgentSessionRepo::get_agent_session(&*self.db, session_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_session", session_id.to_owned()))?;
        self.require_owned_identity(&session.identity_id, actor_user_id)
            .await?;
        if session.backend_kind != "native" {
            return Err(ServiceError::invalid_operation(
                "selected backend does not support native cancellation",
            ));
        }
        let runtime_session_id = session.runtime_session_id.ok_or_else(|| {
            ServiceError::invalid_operation("session has no native runtime identifier")
        })?;
        self.native_backend
            .cancel(&runtime_session_id)
            .await
            .map_err(redacted_host_error)
    }

    pub async fn steer_session_turn(
        &self,
        actor_user_id: &str,
        session_id: &str,
        content: String,
    ) -> Result<()> {
        if content.trim().is_empty() {
            return Err(ServiceError::invalid_operation(
                "steer content cannot be empty",
            ));
        }
        let session = AgentSessionRepo::get_agent_session(&*self.db, session_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_session", session_id.to_owned()))?;
        self.require_owned_identity(&session.identity_id, actor_user_id)
            .await?;
        if session.backend_kind != "native" {
            return Err(ServiceError::invalid_operation(
                "selected backend does not support steering",
            ));
        }
        let runtime_session_id = session.runtime_session_id.ok_or_else(|| {
            ServiceError::invalid_operation("session has no native runtime identifier")
        })?;
        self.native_backend
            .steer(&runtime_session_id, content)
            .await
            .map_err(redacted_host_error)
    }

    pub async fn list_sessions(
        &self,
        actor_user_id: &str,
        identity_id: &str,
    ) -> Result<Vec<AgentSession>> {
        self.require_owned_identity(identity_id, actor_user_id)
            .await?;
        AgentSessionRepo::list_agent_sessions(&*self.db, identity_id)
            .await
            .map_err(Into::into)
    }

    pub async fn effective_permissions(
        &self,
        actor_user_id: &str,
        identity_id: &str,
        scope_request: &RequestedCanonicalScope,
    ) -> Result<EffectivePermissions> {
        let identity = self
            .require_owned_identity(identity_id, actor_user_id)
            .await?;
        let profile = AgentProfileRepo::get_profile(&*self.db, &identity.profile_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_profile", identity.profile_id.clone()))?;
        let canonical = self
            .authorize_scope(actor_user_id, &identity, scope_request)
            .await?;
        let account_json: String = sqlx::query_scalar(
            "SELECT account_permission_ceiling FROM agent_identity WHERE id = ?",
        )
        .bind(identity_id)
        .fetch_one(self.db.pool())
        .await?;
        let mut layers = vec![
            permission_set(&account_json),
            permission_set(&profile.tool_policy_json),
        ];
        let project_id = self.scope_project_id(scope_request).await?;
        let project_charter_setup_required = if let Some(project_id) = project_id.as_deref() {
            sqlx::query_scalar::<_, i64>(
                "SELECT charter_setup_required FROM project WHERE id = ? LIMIT 1",
            )
            .bind(project_id)
            .fetch_optional(self.db.pool())
            .await?
            .is_some_and(|value| value != 0)
        } else {
            false
        };
        if let Some(project_id) = project_id.as_deref() {
            // Project/Agent Chat authorization above already requires the
            // singular Project Agent binding. A Task assignment follows the
            // existing Task role path and does not inherit Project binding
            // authority.
            match scope_request {
                RequestedCanonicalScope::AgentChat { .. }
                | RequestedCanonicalScope::Project { .. } => {
                    let binding =
                        ProjectAgentBindingRepo::get_active_project_binding(&*self.db, project_id)
                            .await?
                            .filter(|binding| {
                                binding.state == "active"
                                    && binding.identity_id.as_deref() == Some(identity_id)
                            })
                            .ok_or_else(|| {
                                ServiceError::not_found("project_agent_binding", identity_id)
                            })?;
                    layers.push(permission_set(&binding.permission_ceiling_json));
                }
                RequestedCanonicalScope::Task { .. } => {}
                RequestedCanonicalScope::Account => unreachable!("Account has no Project id"),
            }
        }
        let project_agent_chat = matches!(scope_request, RequestedCanonicalScope::AgentChat { .. })
            && canonical.scope_type == CanonicalScopeType::AgentChat
            && project_id.is_some();
        layers.push(scope_permission_set(
            &canonical,
            project_agent_chat,
            project_charter_setup_required,
        ));
        let allowed = intersect_non_empty_layers(&layers);
        let known = known_permissions();
        let denied = known.difference(&allowed).cloned().collect();
        let requires_approval = allowed
            .iter()
            .filter(|permission| permission.starts_with("propose_") || *permission == "task_write")
            .cloned()
            .collect();
        Ok(EffectivePermissions {
            allowed,
            denied,
            requires_approval,
        })
    }

    async fn rotate_session_for_profile(
        &self,
        actor_user_id: &str,
        previous: &AgentSession,
        profile: &AgentProfile,
    ) -> Result<AgentSession> {
        self.require_owned_identity(&previous.identity_id, actor_user_id)
            .await?;
        self.rotate_authorized_session(previous, profile).await
    }

    async fn rotate_authorized_session(
        &self,
        previous: &AgentSession,
        profile: &AgentProfile,
    ) -> Result<AgentSession> {
        if profile.identity_id != previous.identity_id {
            return Err(ServiceError::invalid_operation(
                "replacement profile belongs to another identity",
            ));
        }
        let now = now_rfc3339();
        let replacement_id = new_uuid_v4();
        let (status, connection_status) = self.initial_session_status(profile).await?;
        let scope = AgentContextScopeRepo::get_context_scope(&*self.db, &previous.context_scope_id)
            .await?
            .ok_or_else(|| {
                ServiceError::not_found("agent_context_scope", previous.context_scope_id.clone())
            })?;
        let canonical = CanonicalScope {
            scope_type: parse_canonical_scope_type(&scope.scope_type)?,
            scope_id: scope.scope_id,
            workspace_access: parse_workspace_access(&scope.workspace_access)?,
        };
        let capabilities = capabilities_for_profile(profile, &canonical);
        AgentSessionRepo::rotate_agent_session(
            &*self.db,
            RotateAgentSession {
                previous_session_id: previous.id.clone(),
                expected_version: previous.version,
                replacement: CreateAgentSession {
                    id: replacement_id,
                    identity_id: previous.identity_id.clone(),
                    profile_id: profile.id.clone(),
                    context_scope_id: previous.context_scope_id.clone(),
                    backend_kind: profile.backend_kind.clone(),
                    runtime_session_id: (profile.backend_kind == "native").then(new_uuid_v4),
                    status,
                    capabilities_json: serde_json::to_string(&capabilities)
                        .unwrap_or_else(|_| "{}".to_owned()),
                    connection_status,
                    predecessor_session_id: Some(previous.id.clone()),
                    last_activity_at: None,
                    created_at: now.clone(),
                    updated_at: now,
                },
            },
        )
        .await
        .map_err(Into::into)
    }

    async fn initial_session_status(&self, profile: &AgentProfile) -> Result<(String, String)> {
        if profile.backend_kind != "native" {
            return Ok(("ready".to_owned(), "unknown".to_owned()));
        }
        let connection_status =
            AgentConnectionHealthRepo::get_connection_health(&*self.db, &profile.id)
                .await?
                .map(|health| health.status)
                .unwrap_or_else(|| "unknown".to_owned());
        let status = if connection_status == "healthy" {
            "ready"
        } else {
            "degraded"
        };
        Ok((status.to_owned(), connection_status))
    }

    async fn require_owned_identity(&self, identity_id: &str, user_id: &str) -> Result<Agent> {
        AgentRepo::get_by_id(&*self.db, identity_id)
            .await?
            .filter(|agent| agent.owner_id.as_deref() == Some(user_id))
            .ok_or_else(|| ServiceError::not_found("agent", identity_id.to_owned()))
    }

    async fn authorize_scope(
        &self,
        actor_user_id: &str,
        identity: &Agent,
        requested: &RequestedCanonicalScope,
    ) -> Result<CanonicalScope> {
        match requested {
            RequestedCanonicalScope::Account => Ok(CanonicalScope {
                scope_type: CanonicalScopeType::Account,
                scope_id: actor_user_id.to_owned(),
                workspace_access: WorkspaceAccess::Deny,
            }),
            RequestedCanonicalScope::Project { project_id } => {
                require_project_authority(&self.db, actor_user_id, &identity.id, project_id)
                    .await?;
                Ok(CanonicalScope {
                    scope_type: CanonicalScopeType::Project,
                    scope_id: project_id.clone(),
                    workspace_access: WorkspaceAccess::Deny,
                })
            }
            RequestedCanonicalScope::AgentChat { chat_id } => {
                let chat = AgentChatRepo::get_agent_chat(&*self.db, chat_id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("agent_chat", chat_id.clone()))?;
                match chat.kind.as_str() {
                    "account_main" => {
                        if chat.account_id.as_deref() != Some(actor_user_id) {
                            return Err(ServiceError::not_found("agent_chat", chat_id.clone()));
                        }
                        AccountMainAgentBindingRepo::get_active_main_binding(
                            &*self.db,
                            actor_user_id,
                        )
                        .await?
                        .filter(|binding| {
                            binding.state == "active" && binding.identity_id == identity.id
                        })
                        .ok_or_else(|| {
                            ServiceError::not_found("main_agent_binding", identity.id.clone())
                        })?;
                    }
                    "project" => {
                        let project_id = chat.project_id.as_deref().ok_or_else(|| {
                            ServiceError::not_found("agent_chat", chat_id.clone())
                        })?;
                        ProjectMemberRepo::get_member(&*self.db, project_id, actor_user_id)
                            .await?
                            .ok_or_else(|| {
                                ServiceError::not_found("project", project_id.to_owned())
                            })?;
                        ProjectAgentBindingRepo::get_active_project_binding(&*self.db, project_id)
                            .await?
                            .filter(|binding| {
                                binding.state == "active"
                                    && binding.identity_id.as_deref() == Some(identity.id.as_str())
                            })
                            .ok_or_else(|| {
                                ServiceError::not_found(
                                    "project_agent_binding",
                                    identity.id.clone(),
                                )
                            })?;
                    }
                    _ => {
                        return Err(ServiceError::not_found("agent_chat", chat_id.clone()));
                    }
                }
                Ok(CanonicalScope {
                    scope_type: CanonicalScopeType::AgentChat,
                    scope_id: chat_id.clone(),
                    workspace_access: WorkspaceAccess::Deny,
                })
            }
            RequestedCanonicalScope::Task { task_id, role } => {
                if !matches!(role.as_str(), "worker" | "reviewer" | "planner") {
                    return Err(ServiceError::invalid_operation(
                        "embedded Task sessions support only worker, reviewer, or planner roles",
                    ));
                }
                let task = TaskRepo::get_by_id(&*self.db, task_id, false)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
                ProjectMemberRepo::get_member(&*self.db, &task.project_id, actor_user_id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
                let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
                let workflow = WorkflowEngine::resolve_workflow_for_task(
                    &task,
                    &project.workflow_definition,
                    &api_types::Actor::system(api_types::SystemComponent::Executor),
                );
                let active_role = workflow
                    .states
                    .iter()
                    .find(|state| state.name == task.status)
                    .and_then(effective_role);
                if !task_role_admitted_by_workflow(active_role, role) {
                    return Err(ServiceError::invalid_operation(
                        "embedded Task scope is not admitted by the current workflow state",
                    ));
                }
                // The default Forge workflow calls its implementation role
                // `coder`, while the embedded authority contract calls the
                // same write-capable scope a `worker`. Resolve the durable
                // assignment here without granting anything to an unassigned
                // identity; reviewers remain an exact role match.
                let assignment = TaskRoleAssignmentRepo::list_by_task(&*self.db, task_id)
                    .await?
                    .into_iter()
                    .find(|assignment| {
                        assignment.assignee_type == Some(AssigneeKind::Agent)
                            && assignment.assignee_id.as_deref() == Some(identity.id.as_str())
                            && (assignment.role_name == role.as_str()
                                || (role == "worker"
                                    && matches!(assignment.role_name.as_str(), "worker" | "coder")))
                    })
                    .ok_or_else(|| {
                        ServiceError::not_found("task_role_assignment", task_id.clone())
                    })?;
                // Reviewer and planner sessions never receive worktree write
                // authority; planners author plans through Task metadata and
                // native tools only. Planning/discovery Tasks are read-only
                // for every role — the execution snapshot marks their
                // worktree read-only, so the session binding must match or a
                // coder-role dispatch on a planning record binds TaskWrite
                // while the turn requests TaskRead and authority validation
                // rejects the turn.
                let access = if matches!(role.as_str(), "reviewer" | "planner")
                    || matches!(task.task_type.as_str(), "planning_task" | "discovery")
                {
                    WorkspaceAccess::TaskRead
                } else {
                    WorkspaceAccess::TaskWrite
                };
                // A role-table preassignment is not itself a workspace grant.
                // The workflow must have admitted the task and a live
                // execution must exist for this identity.  This prevents a
                // caller from creating a durable write-capable Task session
                // before the normal claim/dispatch transaction runs.
                let active_execution = ExecutionRepo::list_by_task(
                    &*self.db,
                    task_id,
                    PageRequest {
                        cursor: None,
                        limit: 100,
                        include_total: false,
                        sort_by: SortBy::CreatedAt,
                        sort_order: SortOrder::Desc,
                    },
                )
                .await?
                .items
                .into_iter()
                .any(|execution| {
                    execution.status == ExecutionStatus::Running
                        && execution.agent_id.as_deref() == Some(identity.id.as_str())
                        && match role.as_str() {
                            "reviewer" => execution.role == default_roles::REVIEWER,
                            "planner" => execution.role == default_roles::PLANNER,
                            _ => matches!(
                                execution.role.as_str(),
                                default_roles::WORKER | default_roles::CODER
                            ),
                        }
                });
                // The durable claim marker is either the Task-level assignee
                // (interactive claim path) or the role-table assignment the
                // claim/dispatch transaction wrote (dispatcher-initiated
                // initial role dispatch never sets the Task assignee). Both
                // are server-written records; requiring the Task assignee
                // alone made every dispatcher-initiated embedded worker
                // dispatch fail authorization.
                let claim_admitted = role != "worker"
                    || (task.assignee_type.as_deref() == Some("agent")
                        && task.assignee_id.as_deref() == Some(identity.id.as_str()))
                    || assignment.assignee_id.as_deref() == Some(identity.id.as_str());
                if !claim_admitted || !active_execution {
                    // The Running execution row is re-queried here instead of
                    // being threaded from the caller, so a stop/terminalize
                    // racing this authorization makes the row disappear
                    // between the runner's admission check and this lookup.
                    // Name the race so the failure is diagnosable; recovery
                    // stays with the dispatcher's normal retry pass.
                    return Err(ServiceError::invalid_operation(format!(
                        "no Running {role} execution for this identity on task {task_id}; \
                         the execution likely stopped or was superseded between dispatch \
                         admission and session authorization"
                    )));
                }
                Ok(CanonicalScope {
                    scope_type: CanonicalScopeType::Task,
                    scope_id: task_id.clone(),
                    workspace_access: access,
                })
            }
        }
    }

    async fn scope_project_id(
        &self,
        requested: &RequestedCanonicalScope,
    ) -> Result<Option<String>> {
        match requested {
            RequestedCanonicalScope::Account => Ok(None),
            RequestedCanonicalScope::Project { project_id } => Ok(Some(project_id.clone())),
            RequestedCanonicalScope::AgentChat { chat_id } => {
                Ok(AgentChatRepo::get_agent_chat(&*self.db, chat_id)
                    .await?
                    .and_then(|chat| chat.project_id))
            }
            RequestedCanonicalScope::Task { task_id, .. } => Ok(Some(
                TaskRepo::get_by_id(&*self.db, task_id, false)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?
                    .project_id,
            )),
        }
    }

    /// The provider-side account id stored on one of the caller's provider
    /// entries during OAuth login, if any.
    pub async fn credential_provider_account_id(
        &self,
        credential_id: &str,
    ) -> Result<Option<String>> {
        Ok(
            CredentialHandleRepo::get_credential_handle(&*self.db, credential_id)
                .await?
                .as_ref()
                .and_then(entry_provider_account_id),
        )
    }

    async fn check_and_record_connection(
        &self,
        profile: &AgentProfile,
        owner_user_id: &str,
    ) -> Result<AgentConnectionHealth> {
        let now = now_rfc3339();
        let (status, error_code) = match self.probe_provider(profile, owner_user_id).await {
            Ok(()) => ("healthy".to_owned(), None),
            Err(code) => ("unavailable".to_owned(), Some(code)),
        };
        AgentConnectionHealthRepo::upsert_connection_health(
            &*self.db,
            UpsertAgentConnectionHealth {
                profile_id: profile.id.clone(),
                status,
                capability_status_json: profile.capabilities_json.clone(),
                checked_at: Some(now.clone()),
                error_code,
                updated_at: now,
            },
        )
        .await
        .map_err(Into::into)
    }

    async fn probe_provider(
        &self,
        profile: &AgentProfile,
        owner_user_id: &str,
    ) -> std::result::Result<(), String> {
        let handle = profile
            .credential_ref
            .as_deref()
            .ok_or_else(|| "credential_missing".to_owned())?;
        let credential = self
            .protected_store
            .acquire_provider_credential(owner_user_id, handle, 30_000)
            .await
            .map_err(|_| "credential_unavailable".to_owned())?;
        let config: Value = serde_json::from_str(&profile.config_json)
            .map_err(|_| "profile_config_invalid".to_owned())?;
        let base_url = config
            .get("base_url")
            .and_then(Value::as_str)
            .ok_or_else(|| "base_url_missing".to_owned())?;
        let (parsed, addresses) = provider_url_addresses(base_url)
            .await
            .map_err(|_| "provider_endpoint_invalid".to_owned())?;
        let endpoint = format!("{}/models", base_url.trim_end_matches('/'));
        let host = parsed
            .host_str()
            .ok_or_else(|| "provider_endpoint_invalid".to_owned())?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(8))
            .redirect(reqwest::redirect::Policy::none())
            .resolve_to_addrs(host, &addresses)
            .build()
            .map_err(|_| "provider_endpoint_invalid".to_owned())?;
        // Gemini's AI Studio API rejects API keys sent as Bearer tokens; it
        // authenticates via the `x-goog-api-key` header. Every other backend
        // Forge probes takes standard Bearer auth.
        let request = if profile.provider.as_deref() == Some("gemini") {
            client
                .get(endpoint)
                .header("x-goog-api-key", credential.expose())
        } else {
            client.get(endpoint).bearer_auth(credential.expose())
        };
        let response = request.send().await.map_err(|error| {
            if error.is_timeout() {
                "provider_timeout".to_owned()
            } else {
                "provider_unreachable".to_owned()
            }
        })?;
        let status = response.status().as_u16();
        if !response.status().is_success() {
            tracing::warn!(status, profile_id = %profile.id, "provider health probe rejected");
        }
        if response.status().is_success() {
            Ok(())
        } else if status == 401 || status == 403 {
            Err("provider_authentication_failed".to_owned())
        } else if is_codex_backend(base_url) {
            // The ChatGPT Codex backend has no `GET /models` route (it
            // answers 400) but authenticates every request before routing,
            // so any non-401/403 answer proves the credential is accepted.
            // Without this an OAuth-backed OpenAI agent is permanently
            // reported degraded even though turns work.
            Ok(())
        } else {
            Err("provider_health_failed".to_owned())
        }
    }
}

/// Environment variable a CLI harness reads for each provider's API key.
fn provider_env_variable(provider: &str) -> Option<&'static str> {
    match provider {
        "openai" | "openai_compatible" => Some("OPENAI_API_KEY"),
        "gemini" => Some("GEMINI_API_KEY"),
        "xai" => Some("XAI_API_KEY"),
        "openrouter" => Some("OPENROUTER_API_KEY"),
        _ => None,
    }
}

/// Default API endpoint for API-key entries. `openai_compatible` has no
/// default and requires an explicit base URL.
pub fn default_api_key_base_url(provider: &str) -> Option<&'static str> {
    match provider {
        "openai" => Some("https://api.openai.com/v1"),
        "xai" => Some("https://api.x.ai/v1"),
        "gemini" => Some("https://generativelanguage.googleapis.com/v1beta"),
        "openrouter" => Some("https://openrouter.ai/api/v1"),
        _ => None,
    }
}

/// Whether a base URL points at the ChatGPT Codex backend, which speaks the
/// Responses API only and has no OpenAI-compatible `GET /models` route.
fn is_codex_backend(base_url: &str) -> bool {
    base_url.contains("chatgpt.com/backend-api/codex")
}

fn default_oauth_base_url(provider: &str) -> Option<&'static str> {
    match provider {
        "openai" => Some("https://chatgpt.com/backend-api/codex"),
        "xai" => Some("https://api.x.ai/v1"),
        "gemini" => Some("https://generativelanguage.googleapis.com/v1beta"),
        _ => None,
    }
}

/// The API endpoint an agent built on this entry talks to: stored entry
/// metadata first, then the provider/method default.
pub fn entry_base_url(entry: &CredentialHandle) -> Result<String> {
    let stored = serde_json::from_str::<Value>(&entry.metadata_json)
        .ok()
        .and_then(|metadata| {
            metadata
                .get("base_url")
                .and_then(Value::as_str)
                .map(str::to_owned)
        });
    if let Some(base_url) = stored {
        return Ok(base_url);
    }
    let fallback = if entry.credential_method == "oauth_bundle" {
        default_oauth_base_url(&entry.provider)
    } else {
        default_api_key_base_url(&entry.provider)
    };
    fallback
        .map(str::to_owned)
        .ok_or_else(|| ServiceError::invalid_operation("provider entry has no usable API endpoint"))
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

/// The subset of a `GET /wham/usage` response this probe reads. Everything
/// else the endpoint reports — credits, spend controls — is account
/// commerce, not limit state, and stays unread.
#[derive(Debug, Deserialize)]
struct WhamUsageWindowWire {
    used_percent: Option<f64>,
    limit_window_seconds: Option<i64>,
    reset_after_seconds: Option<i64>,
    reset_at: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct WhamUsageRateLimitWire {
    primary_window: Option<WhamUsageWindowWire>,
    secondary_window: Option<WhamUsageWindowWire>,
}

#[derive(Debug, Deserialize)]
struct WhamUsagePayloadWire {
    plan_type: Option<String>,
    rate_limit: Option<WhamUsageRateLimitWire>,
}

/// Converts a `/wham/usage` response body into the plan type and normalized
/// usage windows the API reports. Pure (given `now_unix_seconds`) so the
/// `reset_at`/`reset_after_seconds`/`window_minutes` edge cases are
/// unit-testable without a live probe.
fn usage_snapshot_from_wham_json(
    body: &str,
    now_unix_seconds: i64,
) -> std::result::Result<(Option<String>, Vec<ProviderUsageWindowOutcome>), serde_json::Error> {
    let payload: WhamUsagePayloadWire = serde_json::from_str(body)?;
    let mut windows = Vec::new();
    if let Some(rate_limit) = payload.rate_limit {
        for (id, window) in [
            ("primary", rate_limit.primary_window),
            ("secondary", rate_limit.secondary_window),
        ] {
            let Some(window) = window else { continue };
            let Some(used_percent) = window.used_percent.filter(|percent| percent.is_finite())
            else {
                continue;
            };
            // The absolute reset time speaks when present; a `0` delay
            // beside it is filler, not "resets now". Only fall back to the
            // relative delay when no absolute time was given.
            let resets_at = window
                .reset_at
                .and_then(rfc3339_from_unix_seconds)
                .or_else(|| {
                    window
                        .reset_after_seconds
                        .filter(|seconds| *seconds > 0)
                        .and_then(|seconds| {
                            rfc3339_from_unix_seconds(now_unix_seconds.saturating_add(seconds))
                        })
                });
            windows.push(ProviderUsageWindowOutcome {
                id: id.to_owned(),
                used_percent,
                window_minutes: window.limit_window_seconds.map(|seconds| seconds / 60),
                resets_at,
            });
        }
    }
    Ok((payload.plan_type, windows))
}

fn rfc3339_from_unix_seconds(seconds: i64) -> Option<String> {
    chrono::DateTime::from_timestamp(seconds, 0).map(|instant| instant.to_rfc3339())
}

fn now_unix_seconds() -> i64 {
    chrono::Utc::now().timestamp()
}

async fn provider_url_addresses(
    base_url: &str,
) -> std::result::Result<(url::Url, Vec<SocketAddr>), &'static str> {
    let parsed = url::Url::parse(base_url).map_err(|_| "base_url must be an absolute URL")?;
    if parsed.username() != "" || parsed.password().is_some() || parsed.fragment().is_some() {
        return Err("base_url must not contain userinfo or a fragment");
    }
    if parsed.scheme() != "https" {
        return Err("base_url must use https");
    }
    let host = parsed.host().ok_or("base_url must include a host")?;
    let port = parsed
        .port_or_known_default()
        .ok_or("base_url has no known port")?;
    let addresses = match host {
        url::Host::Ipv4(address) => vec![SocketAddr::new(IpAddr::V4(address), port)],
        url::Host::Ipv6(address) => vec![SocketAddr::new(IpAddr::V6(address), port)],
        url::Host::Domain(domain) => {
            if restricted_provider_hostname(domain) {
                return Err("base_url must not target a local or private hostname");
            }
            tokio::net::lookup_host((domain, port))
                .await
                .map_err(|_| "base_url hostname could not be resolved")?
                .collect::<Vec<_>>()
        }
    };
    if addresses.is_empty()
        || addresses
            .iter()
            .any(|address| restricted_provider_ip(address.ip()))
    {
        return Err("base_url must not target a private or local address");
    }
    Ok((parsed, addresses))
}

fn restricted_provider_hostname(host: &str) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    host == "localhost"
        || host.ends_with(".localhost")
        || host.ends_with(".local")
        || host.ends_with(".internal")
        || host.ends_with(".home.arpa")
}

fn restricted_provider_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(address) => {
            let octets = address.octets();
            address.is_private()
                || address.is_loopback()
                || address.is_link_local()
                || address.is_unspecified()
                || address.is_broadcast()
                || (octets[0] == 100 && (64..=127).contains(&octets[1]))
                || (octets[0] == 192 && octets[1] == 0 && octets[2] == 0)
                || (octets[0] == 192 && octets[1] == 0 && octets[2] == 2)
                || (octets[0] == 198 && (18..=19).contains(&octets[1]))
                || (octets[0] == 198 && octets[1] == 51 && octets[2] == 100)
                || (octets[0] == 203 && octets[1] == 0 && octets[2] == 113)
                || octets[0] >= 224
        }
        IpAddr::V6(address) => {
            let segments = address.segments();
            address.is_loopback()
                || address.is_unspecified()
                || (segments[0] & 0xffc0) == 0xfe80
                || (segments[0] & 0xffc0) == 0xfec0
                || (segments[0] & 0xfe00) == 0xfc00
                || segments[0] >= 0xff00
                || address
                    .to_ipv4()
                    .is_some_and(|mapped| restricted_provider_ip(IpAddr::V4(mapped)))
        }
    }
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

/// Provider-aware native model limits `(context, max_input, max_output)`.
/// The generic defaults are conservative; every current Gemini API model
/// serves a 1M-token context window, and the generic 96k input budget made
/// large Task context manifests fail with `budget_exceeded` before the model
/// was even called.
fn native_model_limits(provider: &str) -> (u32, u32, u32) {
    match provider {
        "gemini" => (1_048_576, 800_000, 64_000),
        _ => (
            DEFAULT_CONTEXT_TOKENS,
            DEFAULT_MAX_INPUT_TOKENS,
            DEFAULT_MAX_OUTPUT_TOKENS,
        ),
    }
}

/// Effective native model limits at load time. Profiles are immutable
/// (schema-enforced), so rows created before provider-aware defaults carry
/// the old generic triple baked into their config. That exact triple can only
/// be a baked default — never a deliberate user choice of all three values —
/// so it resolves to the provider defaults instead of capping the model.
pub(crate) fn effective_native_limits(
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
        BackendCapabilities {
            workspace: canonical.workspace_access,
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

async fn require_project_authority(
    db: &SqliteDb,
    user_id: &str,
    identity_id: &str,
    project_id: &str,
) -> Result<()> {
    ProjectMemberRepo::get_member(db, project_id, user_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;
    ProjectAgentBindingRepo::get_active_project_binding(db, project_id)
        .await?
        .filter(|binding| {
            binding.state == "active" && binding.identity_id.as_deref() == Some(identity_id)
        })
        .ok_or_else(|| ServiceError::not_found("project_agent_binding", identity_id.to_owned()))?;
    Ok(())
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
    }
}

fn parse_workspace_access(value: &str) -> Result<WorkspaceAccess> {
    match value {
        "deny" => Ok(WorkspaceAccess::Deny),
        "task_read" => Ok(WorkspaceAccess::TaskRead),
        "task_write" => Ok(WorkspaceAccess::TaskWrite),
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

fn permission_set(value: &str) -> BTreeSet<String> {
    let Ok(value) = serde_json::from_str::<Value>(value) else {
        return BTreeSet::new();
    };
    match value {
        Value::Array(values) => values
            .into_iter()
            .filter_map(|value| value.as_str().map(str::to_owned))
            .collect(),
        Value::Object(map) => map
            .get("permissions")
            .or_else(|| map.get("allowed"))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|value| value.as_str().map(str::to_owned))
            .collect(),
        _ => BTreeSet::new(),
    }
}

fn scope_permission_set(
    scope: &CanonicalScope,
    project_agent_chat: bool,
    project_charter_setup_required: bool,
) -> BTreeSet<String> {
    let mut values: Vec<&str> = match scope.scope_type {
        CanonicalScopeType::Account => vec![
            "read_account",
            "propose_discovery",
            "propose_project",
            "propose_handoff",
        ],
        CanonicalScopeType::Project => {
            let mut project = vec![
                "read_project",
                "read_memory",
                "propose_project",
                "propose_message",
            ];
            if !project_charter_setup_required {
                project.extend([
                    "propose_task",
                    "propose_commitment",
                    "propose_memory",
                    "propose_review",
                    "propose_decision",
                    "propose_session",
                ]);
            }
            project
        }
        CanonicalScopeType::AgentChat => {
            let mut chat = vec!["read_agent_chat", "read_memory", "propose_message"];
            if project_agent_chat && !project_charter_setup_required {
                chat.extend(["propose_commitment", "propose_memory", "propose_session"]);
            }
            chat
        }
        CanonicalScopeType::Task => match scope.workspace_access {
            WorkspaceAccess::TaskRead => {
                vec!["read_task", "read_memory", "task_read", "propose_review"]
            }
            WorkspaceAccess::TaskWrite => {
                vec!["read_task", "read_memory", "task_read", "task_write"]
            }
            WorkspaceAccess::Deny => Vec::new(),
        },
    };
    if scope.scope_type == CanonicalScopeType::AgentChat && project_agent_chat {
        values.push("propose_project");
        if !project_charter_setup_required {
            values.push("propose_task");
        }
    } else if scope.scope_type == CanonicalScopeType::AgentChat {
        values.extend(["propose_discovery", "propose_project", "propose_handoff"]);
    }
    values.into_iter().map(str::to_owned).collect()
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
        forge_agent_host::AgentHostError::Runtime(_) => {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frozen_binding_accepts_the_retained_replacement_version_only() {
        assert!(validate_frozen_binding_state("active", 3, 3).is_ok());
        assert!(validate_frozen_binding_state("replaced", 4, 3).is_ok());
        assert!(validate_frozen_binding_state("replaced", 3, 3).is_ok());
        assert!(validate_frozen_binding_state("replaced", 5, 3).is_err());
        assert!(validate_frozen_binding_state("active", 4, 3).is_err());
    }

    #[test]
    fn frozen_binding_rejects_withdrawn_authority_even_with_matching_version() {
        for state in ["paused", "revoked", "agent_setup_required"] {
            assert!(validate_frozen_binding_state(state, 3, 3).is_err());
        }
    }

    #[test]
    fn permission_intersection_never_expands_authority() {
        let account = permission_set(r#"["read_project","read_memory","propose_task"]"#);
        let profile = permission_set(r#"{"allowed":["read_project","propose_task"]}"#);
        let scope: BTreeSet<String> = ["read_project", "read_memory"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        let effective = intersect_non_empty_layers(&[account, profile, scope]);
        assert_eq!(effective, BTreeSet::from(["read_project".to_owned()]));
    }

    #[test]
    fn task_is_the_only_scope_that_can_admit_workspace_access() {
        for scope_type in [
            CanonicalScopeType::Account,
            CanonicalScopeType::Project,
            CanonicalScopeType::AgentChat,
        ] {
            let scope = CanonicalScope {
                scope_type,
                scope_id: "scope".to_owned(),
                workspace_access: WorkspaceAccess::TaskRead,
            };
            assert!(scope.validate().is_err());
        }
    }

    #[test]
    fn non_task_scope_tool_permissions_never_include_task_workspace_authority() {
        for scope_type in [
            CanonicalScopeType::Account,
            CanonicalScopeType::Project,
            CanonicalScopeType::AgentChat,
        ] {
            let permissions = scope_permission_set(
                &CanonicalScope {
                    scope_type,
                    scope_id: "scope".to_owned(),
                    workspace_access: WorkspaceAccess::Deny,
                },
                false,
                false,
            );
            assert!(!permissions.contains("task_read"));
            assert!(!permissions.contains("task_write"));
            assert_eq!(
                permissions.contains("propose_review"),
                scope_type == CanonicalScopeType::Project,
                "a review request is a Project-scoped proposal, not Task workspace authority"
            );
        }
    }

    #[test]
    fn malformed_or_empty_required_layer_denies_all_permissions() {
        let account = permission_set(r#"["read_project","read_memory"]"#);
        let malformed = permission_set("not-json");
        let scope = BTreeSet::from(["read_project".to_owned()]);
        assert!(intersect_non_empty_layers(&[account.clone(), malformed, scope]).is_empty());
        assert!(intersect_non_empty_layers(&[account, BTreeSet::new()]).is_empty());
    }

    #[test]
    fn task_role_requires_the_current_workflow_state() {
        assert!(task_role_admitted_by_workflow(
            Some(default_roles::CODER),
            "worker"
        ));
        assert!(task_role_admitted_by_workflow(
            Some(default_roles::WORKER),
            "worker"
        ));
        assert!(task_role_admitted_by_workflow(
            Some(default_roles::REVIEWER),
            "reviewer"
        ));
        assert!(task_role_admitted_by_workflow(
            Some(default_roles::PLANNER),
            "planner"
        ));
        assert!(!task_role_admitted_by_workflow(
            Some(default_roles::CODER),
            "planner"
        ));
        assert!(!task_role_admitted_by_workflow(None, "worker"));
        assert!(!task_role_admitted_by_workflow(
            Some(default_roles::PLANNER),
            "worker"
        ));
        assert!(!task_role_admitted_by_workflow(
            Some(default_roles::CODER),
            "reviewer"
        ));
    }

    #[test]
    fn entry_base_url_prefers_metadata_then_method_default() {
        let handle = |method: &str, metadata: &str| CredentialHandle {
            id: "entry-1".to_owned(),
            owner_user_id: "user-1".to_owned(),
            provider: "openai".to_owned(),
            label: "entry".to_owned(),
            status: "configured".to_owned(),
            credential_method: method.to_owned(),
            metadata_json: metadata.to_owned(),
            version: 1,
            created_at: "2026-08-15T00:00:00Z".to_owned(),
            updated_at: "2026-08-15T00:00:00Z".to_owned(),
        };
        assert_eq!(
            entry_base_url(&handle(
                "api_key",
                r#"{"base_url":"https://proxy.example/v1"}"#
            ))
            .expect("metadata wins"),
            "https://proxy.example/v1"
        );
        assert_eq!(
            entry_base_url(&handle("api_key", "{}")).expect("api-key default"),
            "https://api.openai.com/v1"
        );
        assert_eq!(
            entry_base_url(&handle("oauth_bundle", "{}")).expect("oauth default"),
            "https://chatgpt.com/backend-api/codex"
        );
        let mut compatible = handle("api_key", "{}");
        compatible.provider = "openai_compatible".to_owned();
        assert!(entry_base_url(&compatible).is_err());
    }

    #[test]
    fn usage_payload_maps_windows_and_prefers_absolute_reset_over_a_zero_filler_delay() {
        let now = 1_700_000_000;
        let body = serde_json::json!({
            "plan_type": "pro",
            "rate_limit": {
                "primary_window": {
                    "used_percent": 42,
                    "limit_window_seconds": 300,
                    "reset_after_seconds": 0,
                    "reset_at": 1_704_069_000,
                },
                "secondary_window": {
                    "used_percent": 84.5,
                    "limit_window_seconds": 3600,
                    "reset_after_seconds": 1800,
                },
            },
        })
        .to_string();
        let (plan_type, windows) =
            usage_snapshot_from_wham_json(&body, now).expect("payload parses");
        assert_eq!(plan_type.as_deref(), Some("pro"));
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].id, "primary");
        assert_eq!(windows[0].used_percent, 42.0);
        assert_eq!(windows[0].window_minutes, Some(5));
        // The zero delay beside an absolute reset is filler, not "resets now".
        assert_eq!(
            windows[0].resets_at,
            rfc3339_from_unix_seconds(1_704_069_000)
        );
        assert_eq!(windows[1].id, "secondary");
        assert_eq!(windows[1].used_percent, 84.5);
        assert_eq!(windows[1].window_minutes, Some(60));
        assert_eq!(windows[1].resets_at, rfc3339_from_unix_seconds(now + 1800));
    }

    #[test]
    fn usage_payload_without_rate_limit_reports_no_windows_not_zeroes() {
        let body = serde_json::json!({ "plan_type": "plus" }).to_string();
        let (plan_type, windows) =
            usage_snapshot_from_wham_json(&body, 1_700_000_000).expect("payload parses");
        assert_eq!(plan_type.as_deref(), Some("plus"));
        assert!(windows.is_empty());
    }

    #[test]
    fn usage_window_with_no_reset_signal_reports_no_resets_at() {
        let body = serde_json::json!({
            "rate_limit": {
                "primary_window": {
                    "used_percent": 10,
                    "limit_window_seconds": 300,
                },
            },
        })
        .to_string();
        let (_, windows) = usage_snapshot_from_wham_json(&body, 1_700_000_000).expect("parses");
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].resets_at, None);
    }

    #[test]
    fn usage_window_missing_used_percent_is_skipped() {
        let body = serde_json::json!({
            "rate_limit": {
                "primary_window": { "limit_window_seconds": 300 },
                "secondary_window": { "used_percent": 12.5, "limit_window_seconds": 60 },
            },
        })
        .to_string();
        let (_, windows) = usage_snapshot_from_wham_json(&body, 1_700_000_000).expect("parses");
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].id, "secondary");
    }

    #[test]
    fn usage_payload_malformed_json_is_an_error() {
        assert!(usage_snapshot_from_wham_json("not json", 1_700_000_000).is_err());
    }

    async fn test_service_with_owner() -> (EmbeddedAgentService, String) {
        let pool = db::create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        db::run_migrations(&pool).await.expect("migrations run");
        let db = Arc::new(SqliteDb::new(pool));
        let owner = new_uuid_v4();
        let now = now_rfc3339();
        db::UserRepo::create_user(
            &*db,
            &db::User {
                id: owner.clone(),
                email: "owner@example.com".to_owned(),
                password_hash: "test".to_owned(),
                display_name: Some("Owner".to_owned()),
                is_admin: true,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("owner creates");
        (
            EmbeddedAgentService::new(db, b"entry-split-test-key"),
            owner,
        )
    }

    #[tokio::test]
    async fn api_key_entry_publishes_no_agent_and_stores_base_url() {
        let (service, owner) = test_service_with_owner().await;
        let entry = service
            .connect_api_key_credential(ConnectApiKeyCredential {
                owner_user_id: owner.clone(),
                provider: "openai_compatible".to_owned(),
                label: "work key".to_owned(),
                credential: Secret::new("provider-secret-value".to_owned()),
                base_url: Some("https://8.8.8.8".to_owned()),
            })
            .await
            .expect("entry creates");
        assert_eq!(entry.status, "configured");
        assert_eq!(entry.credential_method, "api_key");
        assert_eq!(
            entry_base_url(&entry).expect("stored endpoint"),
            "https://8.8.8.8"
        );
        let agents = AgentRepo::list(
            &*service.db,
            db::AgentListQuery {
                status: None,
                executor_type: None,
                capabilities: Vec::new(),
                page: PageRequest {
                    cursor: None,
                    limit: 10,
                    include_total: false,
                    sort_by: SortBy::CreatedAt,
                    sort_order: SortOrder::Desc,
                },
            },
        )
        .await
        .expect("agents list");
        assert!(agents.items.is_empty(), "connecting must not create agents");
        assert!(!entry.metadata_json.contains("provider-secret-value"));
    }

    #[tokio::test]
    async fn provider_env_injection_is_in_memory_only_and_refuses_oauth() {
        let (service, owner) = test_service_with_owner().await;
        let entry = service
            .connect_api_key_credential(ConnectApiKeyCredential {
                owner_user_id: owner.clone(),
                provider: "openai_compatible".to_owned(),
                label: "harness key".to_owned(),
                credential: Secret::new("injected-secret-value".to_owned()),
                base_url: Some("https://8.8.8.8".to_owned()),
            })
            .await
            .expect("entry creates");
        let now = now_rfc3339();
        let agent = AgentRepo::create(
            &*service.db,
            db::CreateAgent {
                id: new_uuid_v4(),
                name: "codex-worker".to_owned(),
                description: None,
                executor_type: "codex".to_owned(),
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: "[]".to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: Some(entry.id.clone()),
                daemon_id: None,
                max_concurrent_tasks: 1,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status: AgentStatus::Idle,
                last_heartbeat_at: Some(now.clone()),
                is_default: false,
                paused: false,
                owner_id: Some(owner.clone()),
                visibility: "account".to_owned(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("harness agent creates");

        let mut snapshot = json!({
            "executor_type": "codex",
            "agent_id": agent.id,
            "profile_id": agent.profile_id,
            "config": {}
        });
        service
            .inject_provider_env(&mut snapshot)
            .await
            .expect("injection succeeds");
        // `CommandOverrides` is flattened into executor configs, so the env
        // map must land at `config.env` for deserialization to see it.
        assert_eq!(
            snapshot["config"]["env"]["OPENAI_API_KEY"],
            Value::String("injected-secret-value".to_owned())
        );
        // The stored agent row never gains the secret: injection mutates only
        // the in-memory dispatch snapshot.
        let stored = AgentRepo::get_by_id(&*service.db, &agent.id)
            .await
            .expect("agent reads")
            .expect("agent exists");
        assert!(!stored.config_json.contains("injected-secret-value"));

        // Embedded agents resolve credentials inside the native runtime; the
        // injector must leave their snapshots untouched.
        let mut embedded_snapshot = json!({
            "executor_type": "embedded",
            "agent_id": agent.id,
            "config": {}
        });
        service
            .inject_provider_env(&mut embedded_snapshot)
            .await
            .expect("embedded snapshots pass through");
        assert_eq!(embedded_snapshot["config"], json!({}));

        // An OAuth-backed entry cannot drive a CLI harness.
        let oauth_entry = service
            .connect_oauth_credential(ConnectOAuthCredential {
                owner_user_id: owner.clone(),
                provider: "openai".to_owned(),
                base_url: "https://8.8.8.8".to_owned(),
                credential_label: "chatgpt login".to_owned(),
                credential: OAuthCredentialBundle {
                    schema_version: 1,
                    access_token: "oauth-access-secret".to_owned(),
                    refresh_token: "oauth-refresh-secret".to_owned(),
                    expires_at_ms: u64::MAX,
                    token_endpoint: "https://auth.openai.com/oauth/token".to_owned(),
                    client_id: "client".to_owned(),
                    client_secret: None,
                    scopes: vec!["openid".to_owned()],
                    provider_account_id: Some("acct".to_owned()),
                },
            })
            .await
            .expect("oauth entry creates");
        let now = now_rfc3339();
        let oauth_agent = AgentRepo::create(
            &*service.db,
            db::CreateAgent {
                id: new_uuid_v4(),
                name: "oauth-harness".to_owned(),
                description: None,
                executor_type: "codex".to_owned(),
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: "[]".to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: Some(oauth_entry.id.clone()),
                daemon_id: None,
                max_concurrent_tasks: 1,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status: AgentStatus::Idle,
                last_heartbeat_at: Some(now.clone()),
                is_default: false,
                paused: false,
                owner_id: Some(owner.clone()),
                visibility: "account".to_owned(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("oauth harness agent creates");
        let mut oauth_snapshot = json!({
            "executor_type": "codex",
            "agent_id": oauth_agent.id,
            "config": {}
        });
        let error = service
            .inject_provider_env(&mut oauth_snapshot)
            .await
            .expect_err("oauth entries cannot drive a harness");
        assert!(!error.to_string().contains("oauth-access-secret"));
    }
}
