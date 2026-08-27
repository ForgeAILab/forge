use serde::{Deserialize, Serialize};
use ts_rs::TS;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum AgentProviderId {
    #[serde(rename = "openai")]
    #[ts(rename = "openai")]
    OpenAi,
    #[serde(rename = "xai")]
    #[ts(rename = "xai")]
    XAi,
    Gemini,
    #[serde(rename = "openrouter")]
    #[ts(rename = "openrouter")]
    OpenRouter,
    #[serde(rename = "openai_compatible")]
    #[ts(rename = "openai_compatible")]
    OpenAiCompatible,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ProviderCredentialMethod {
    ApiKey,
    BrowserOauth,
    DeviceOauth,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ProviderSupportLevel {
    Stable,
    Experimental,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct ProviderRuntimeCapability {
    /// `direct` for the embedded native adapter, or a harness executor type
    /// such as `codex` or `gemini`.
    pub runtime: String,
    pub support_level: ProviderSupportLevel,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct ProviderCredentialCapability {
    pub method: ProviderCredentialMethod,
    pub action_label: String,
    pub support_level: ProviderSupportLevel,
    pub configured: bool,
    pub setup_guidance: Option<String>,
    pub boundary_note: Option<String>,
    pub runtimes: Vec<ProviderRuntimeCapability>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct AgentProviderCapability {
    pub provider: AgentProviderId,
    pub display_name: String,
    pub default_base_url: Option<String>,
    pub default_model: Option<String>,
    pub model_discovery: bool,
    pub credential_methods: Vec<ProviderCredentialCapability>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct AgentProviderCapabilitiesResponse {
    pub items: Vec<AgentProviderCapability>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ProviderAuthorizationState {
    Starting,
    AwaitingBrowser,
    AwaitingDevice,
    Polling,
    Exchanging,
    Verifying,
    Publishing,
    Succeeded,
    Denied,
    Expired,
    Cancelled,
    Failed,
}

impl ProviderAuthorizationState {
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Denied | Self::Expired | Self::Cancelled | Self::Failed
        )
    }
}

/// Who binds the loopback socket that receives a browser OAuth callback.
///
/// Providers whose OAuth client only whitelists a localhost callback (OpenAI's
/// Codex client) require a listener on the *browser's* machine. That is the
/// Forge server itself when the browser is local, and `forge-ctl` when the
/// server is remote.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum LoopbackOwner {
    /// Forge binds the callback port. Only valid when the browser runs on the
    /// server's machine.
    #[default]
    Server,
    /// The caller bound the port itself and relays the authorization code back
    /// to `/api/v1/provider-authorizations/{provider}/callback`.
    Client,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct StartProviderAuthorizationRequest {
    pub provider: AgentProviderId,
    pub method: ProviderCredentialMethod,
    pub redirect_origin: String,
    pub credential_label: String,
    /// Browser OAuth only; ignored by API-key and device flows.
    #[serde(default)]
    pub loopback_owner: LoopbackOwner,
    /// Required when `loopback_owner` is `client`: the already-bound port, which
    /// must be one the provider's OAuth client whitelists.
    #[serde(default)]
    pub loopback_port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct CancelProviderAuthorizationRequest {
    #[ts(type = "number")]
    pub expected_version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct ProviderAuthorizationOperationResponse {
    pub id: String,
    pub provider: AgentProviderId,
    pub method: ProviderCredentialMethod,
    pub state: ProviderAuthorizationState,
    pub authorization_url: Option<String>,
    pub user_code: Option<String>,
    pub expires_at: String,
    pub poll_interval_seconds: u32,
    pub credential_handle_id: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    #[ts(type = "number")]
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ProviderAuthorizationCallbackQuery {
    pub code: Option<String>,
    pub state: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct ProviderEntryAgentRef {
    pub agent_id: String,
    pub agent_name: String,
    /// `direct` for embedded agents, otherwise the harness executor type.
    pub runtime: String,
}

/// One configured provider entry: a credentialed connection a user added.
/// Multiple entries of the same provider type may coexist.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct ProviderEntryResponse {
    pub id: String,
    pub provider: String,
    pub label: String,
    pub credential_method: String,
    pub status: String,
    /// Reversible local policy. Disabled entries retain credentials and
    /// dependents but cannot admit new work.
    pub enabled: bool,
    pub base_url: Option<String>,
    pub provider_account_id: Option<String>,
    pub used_by: Vec<ProviderEntryAgentRef>,
    pub last_used_at: Option<String>,
    #[ts(type = "number")]
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

/// Result of a live connectivity test against a provider entry's API.
/// `message` is always redacted server-side; it never carries credential
/// material or provider response bodies.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct ProviderEntryTestResponse {
    /// `ok` when the provider answered and accepted the credential; `failed`
    /// otherwise (see `message` for the redacted reason).
    pub status: String,
    #[ts(type = "number")]
    pub latency_ms: u64,
    pub message: Option<String>,
    pub checked_at: String,
}

/// A CLI-managed runtime discovered on a connected daemon. Forge reads only
/// availability signals; it never imports the CLI's credential files.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct CliRuntimeEntryResponse {
    pub kind: String,
    pub daemon_id: String,
    pub daemon_hostname: Option<String>,
    pub daemon_status: String,
    pub availability: String,
    /// Reversible account policy for this exact daemon/runtime source.
    pub enabled: bool,
    /// Zero means no policy row exists yet; the first availability update
    /// creates version one.
    #[ts(type = "number")]
    pub policy_version: i64,
    pub version: Option<String>,
    pub login_hint: Option<String>,
    pub used_by: Vec<ProviderEntryAgentRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct SetCliRuntimeAvailabilityRequest {
    pub enabled: bool,
    #[ts(type = "number")]
    pub version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct ProviderEntriesResponse {
    pub items: Vec<ProviderEntryResponse>,
    pub cli_runtimes: Vec<CliRuntimeEntryResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct CreateProviderEntryRequest {
    pub provider: AgentProviderId,
    pub label: String,
    pub credential: String,
    /// Required for `openai_compatible`; defaults to the provider's
    /// documented endpoint otherwise.
    pub base_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct RenameProviderEntryRequest {
    pub label: String,
    #[ts(type = "number")]
    pub version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct SetProviderEntryAvailabilityRequest {
    pub enabled: bool,
    #[ts(type = "number")]
    pub version: i64,
}

/// One provider-reported rate-limit window (e.g. ChatGPT's rolling 5h/weekly
/// windows).
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct ProviderUsageWindow {
    pub id: String,
    pub used_percent: f64,
    pub window_minutes: Option<i64>,
    pub resets_at: Option<String>,
}

/// Live (or best-effort) account usage for a provider entry. `source` is
/// `probe` only when live data was actually fetched; `unknown` — with empty
/// `windows` and a `detail` message — whenever the provider isn't probeable
/// or the probe failed. Usage is never fabricated as 0%.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct ProviderUsageResponse {
    pub id: String,
    pub provider: String,
    pub source: String,
    pub plan_type: Option<String>,
    pub windows: Vec<ProviderUsageWindow>,
    pub fetched_at: String,
    pub detail: Option<String>,
}
