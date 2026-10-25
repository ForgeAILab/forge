use std::{net::Ipv4Addr, sync::Arc, time::Duration};

use api_types::{
    AgentProviderCapabilitiesResponse, AgentProviderCapability, AgentProviderId, LoopbackOwner,
    ProviderCredentialCapability, ProviderCredentialMethod, ProviderRuntimeCapability,
    ProviderSupportLevel, StartProviderAuthorizationRequest,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use db::{
    new_uuid_v4, now_rfc3339, CreateProviderAuthorizationOperation, ProviderAuthorizationOperation,
    ProviderAuthorizationRepo, SqliteDb, UpdateProviderAuthorizationOperation,
};
use forge_agent_host::{OAuthCredentialBundle, SqliteProtectedRuntimeStore};
use rand::{rngs::OsRng, RngCore};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::RwLock;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use url::Url;

use crate::embedded_agent_service::{ConnectOAuthCredential, EmbeddedAgentService};
use services::{Result, ServiceError};

mod flows;

const OPENAI_ISSUER: &str = "https://auth.openai.com";
const OPENAI_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const OPENAI_SCOPES: &str =
    "openid profile email offline_access api.connectors.read api.connectors.invoke";
const XAI_ISSUER: &str = "https://auth.x.ai";
const XAI_CLIENT_ID: &str = "b1a00492-073a-47ea-816f-4c329264a828";
const XAI_SCOPES: &str = "openid profile email offline_access api:access";
const GEMINI_SCOPES: &str =
    "openid email profile https://www.googleapis.com/auth/generative-language";
const MAX_AUTH_RESPONSE_BYTES: usize = 1024 * 1024;
/// Ports OpenAI's Codex OAuth client whitelists for its localhost callback.
/// Nothing else is accepted, so the browser must reach a listener on one of
/// them on its own machine.
const LOOPBACK_CALLBACK_PORTS: [u16; 2] = [1455, 1457];
const LOOPBACK_CALLBACK_PATH: &str = "/auth/callback";
const MAX_CALLBACK_BYTES: usize = 8 * 1024;
const MAX_CALLBACK_CODE_BYTES: usize = 2 * 1024;
const LOOPBACK_CALLBACK_TIMEOUT: Duration = Duration::from_secs(10 * 60);

#[derive(Clone)]
pub struct ProviderAuthorizationService {
    db: Arc<SqliteDb>,
    embedded_agents: Arc<EmbeddedAgentService>,
    protected_store: Arc<SqliteProtectedRuntimeStore>,
    trusted_origins: Arc<RwLock<Vec<String>>>,
    client: reqwest::Client,
}

impl std::fmt::Debug for ProviderAuthorizationService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProviderAuthorizationService")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProtectedAuthorizationState {
    state: Option<String>,
    code_verifier: Option<String>,
    redirect_uri: Option<String>,
    client_id: String,
    client_secret: Option<String>,
    token_endpoint: String,
    scope: String,
    device_code: Option<String>,
    device_auth_id: Option<String>,
    user_code: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OAuthTokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    expires_in: Option<u64>,
    scope: Option<String>,
    id_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DeviceAuthorizationResponse {
    device_code: String,
    user_code: String,
    verification_uri: Option<String>,
    verification_uri_complete: Option<String>,
    interval: Option<u64>,
    expires_in: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct OpenAiDeviceAuthorizationResponse {
    device_auth_id: String,
    #[serde(alias = "usercode")]
    user_code: String,
    interval: String,
}

#[derive(Debug, Deserialize)]
struct OpenAiDeviceCodeResponse {
    authorization_code: String,
    code_verifier: String,
}

#[derive(Debug, Deserialize)]
struct OidcDiscovery {
    device_authorization_endpoint: Option<String>,
    token_endpoint: Option<String>,
}

fn provider_uses_loopback_callback(provider: AgentProviderId) -> bool {
    // OpenAI's Codex client whitelists only its localhost callback.
    matches!(provider, AgentProviderId::OpenAi)
}

fn loopback_redirect_uri(port: u16) -> String {
    format!("http://localhost:{port}{LOOPBACK_CALLBACK_PATH}")
}

fn is_loopback_origin(origin: &str) -> bool {
    Url::parse(origin).is_ok_and(|url| match url.host() {
        Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    })
}

async fn bind_loopback_callback() -> Result<(TcpListener, u16)> {
    for port in LOOPBACK_CALLBACK_PORTS {
        match TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await {
            Ok(listener) => return Ok((listener, port)),
            Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {}
            Err(_) => {
                return Err(ServiceError::invalid_operation(
                    "Forge could not bind the localhost OAuth callback",
                ));
            }
        }
    }
    Err(ServiceError::invalid_operation(
        "browser login needs localhost port 1455 or 1457; both are already in use",
    ))
}

/// Extracts the query string of a well-formed `GET /auth/callback` request.
fn callback_query(head: &str) -> Option<String> {
    let mut parts = head.lines().next()?.split_whitespace();
    if parts.next() != Some("GET") {
        return None;
    }
    let target = parts.next()?;
    if parts.next() != Some("HTTP/1.1") || parts.next().is_some() {
        return None;
    }
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    (path == LOOPBACK_CALLBACK_PATH && !query.contains('#')).then(|| query.to_owned())
}

async fn read_request_head(stream: &mut TcpStream) -> std::result::Result<String, ()> {
    let mut bytes = Vec::new();
    loop {
        let mut chunk = [0_u8; 1_024];
        let count = stream.read(&mut chunk).await.map_err(|_| ())?;
        if count == 0 || bytes.len().saturating_add(count) > MAX_CALLBACK_BYTES {
            return Err(());
        }
        bytes.extend_from_slice(&chunk[..count]);
        if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8(bytes).map_err(|_| ())
}

async fn respond(stream: &mut TcpStream, status: u16, body: &str) {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        _ => "Error",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: text/plain; charset=utf-8\r\n\
         Content-Length: {}\r\nConnection: close\r\nCache-Control: no-store\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
}

async fn redirect(stream: &mut TcpStream, location: &str) {
    let response = format!(
        "HTTP/1.1 303 See Other\r\nLocation: {location}\r\nContent-Length: 0\r\n\
         Connection: close\r\nCache-Control: no-store\r\n\r\n"
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
}

enum DevicePoll {
    Pending,
    SlowDown,
    Denied,
    Expired,
    Failed,
    Token(OAuthTokenResponse),
}

fn device_poll_from_error(error: Option<&str>) -> DevicePoll {
    match error {
        Some("authorization_pending") => DevicePoll::Pending,
        Some("slow_down") => DevicePoll::SlowDown,
        Some("access_denied") => DevicePoll::Denied,
        Some("expired_token") => DevicePoll::Expired,
        _ => DevicePoll::Failed,
    }
}

fn trusted_oidc_endpoint(endpoint: &str, issuer: &str) -> Result<String> {
    let endpoint = Url::parse(endpoint)
        .map_err(|_| ServiceError::Domain("provider discovery was malformed".to_owned()))?;
    let issuer = Url::parse(issuer)
        .map_err(|_| ServiceError::Domain("provider issuer is invalid".to_owned()))?;
    if endpoint.scheme() != "https"
        || endpoint.origin() != issuer.origin()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(ServiceError::Domain(
            "provider discovery returned an untrusted endpoint".to_owned(),
        ));
    }
    Ok(endpoint.to_string())
}

fn capability(
    provider: AgentProviderId,
    display_name: &str,
    default_base_url: Option<&str>,
    default_model: Option<&str>,
    model_discovery: bool,
    credential_methods: Vec<ProviderCredentialCapability>,
) -> AgentProviderCapability {
    AgentProviderCapability {
        provider,
        display_name: display_name.to_owned(),
        default_base_url: default_base_url.map(str::to_owned),
        default_model: default_model.map(str::to_owned),
        model_discovery,
        credential_methods,
    }
}

fn method(
    provider: AgentProviderId,
    method: ProviderCredentialMethod,
    action_label: &str,
    support_level: ProviderSupportLevel,
    configured: bool,
    setup_guidance: Option<&str>,
    boundary_note: Option<&str>,
) -> ProviderCredentialCapability {
    ProviderCredentialCapability {
        method,
        action_label: action_label.to_owned(),
        support_level,
        configured,
        setup_guidance: setup_guidance.map(str::to_owned),
        boundary_note: boundary_note.map(str::to_owned),
        runtimes: runtime_matrix(provider, method),
    }
}

/// Which runtimes an entry with this provider/method can drive. The server is
/// authoritative; the web client renders (and disables) options from this
/// matrix and agent creation re-validates it.
fn runtime_matrix(
    provider: AgentProviderId,
    method: ProviderCredentialMethod,
) -> Vec<ProviderRuntimeCapability> {
    let mut runtimes = vec![ProviderRuntimeCapability {
        runtime: "direct".to_owned(),
        support_level: if method == ProviderCredentialMethod::ApiKey
            || provider == AgentProviderId::Gemini
        {
            ProviderSupportLevel::Stable
        } else {
            ProviderSupportLevel::Experimental
        },
        reason: None,
    }];
    match (provider, method) {
        (AgentProviderId::OpenAi, ProviderCredentialMethod::ApiKey) => {
            runtimes.push(ProviderRuntimeCapability {
                runtime: "codex".to_owned(),
                support_level: ProviderSupportLevel::Stable,
                reason: None,
            });
        }
        (AgentProviderId::OpenAi, _) => {
            runtimes.push(ProviderRuntimeCapability {
                runtime: "codex".to_owned(),
                support_level: ProviderSupportLevel::Unavailable,
                reason: Some(
                    "OAuth handoff into the Codex CLI is not supported; use the CLI's own login"
                        .to_owned(),
                ),
            });
        }
        (AgentProviderId::Gemini, ProviderCredentialMethod::ApiKey) => {
            runtimes.push(ProviderRuntimeCapability {
                runtime: "gemini".to_owned(),
                support_level: ProviderSupportLevel::Stable,
                reason: None,
            });
        }
        (AgentProviderId::Gemini, _) => {
            runtimes.push(ProviderRuntimeCapability {
                runtime: "gemini".to_owned(),
                support_level: ProviderSupportLevel::Unavailable,
                reason: Some(
                    "OAuth handoff into the Gemini CLI is not supported; use the CLI's own login"
                        .to_owned(),
                ),
            });
        }
        _ => {}
    }
    runtimes
}

/// Server-side gate for agent creation: may an entry with this provider and
/// credential method drive the requested runtime?
pub fn runtime_supported(
    provider: &str,
    credential_method: &str,
    runtime: &str,
) -> std::result::Result<(), String> {
    let provider = match provider {
        "openai" => AgentProviderId::OpenAi,
        "xai" => AgentProviderId::XAi,
        "gemini" => AgentProviderId::Gemini,
        "openrouter" => AgentProviderId::OpenRouter,
        "openai_compatible" => AgentProviderId::OpenAiCompatible,
        _ => return Err("provider is not in the capability catalog".to_owned()),
    };
    let method = match credential_method {
        "api_key" => ProviderCredentialMethod::ApiKey,
        "oauth_bundle" => ProviderCredentialMethod::BrowserOauth,
        _ => return Err("credential method is not in the capability catalog".to_owned()),
    };
    let matrix = runtime_matrix(provider, method);
    match matrix
        .iter()
        .find(|capability| capability.runtime == runtime)
    {
        Some(capability) if capability.support_level != ProviderSupportLevel::Unavailable => Ok(()),
        Some(capability) => Err(capability
            .reason
            .clone()
            .unwrap_or_else(|| "runtime is unavailable for this provider entry".to_owned())),
        None => Err(format!(
            "a {credential_method} {} entry cannot drive the {runtime} runtime",
            provider_name(provider)
        )),
    }
}

fn browser_authorization(
    provider: AgentProviderId,
    redirect_uri: &str,
    state: &str,
    verifier: &str,
    challenge: &str,
) -> Result<(String, ProtectedAuthorizationState)> {
    let (authorization_endpoint, token_endpoint, client_id, client_secret, scope) = match provider {
        AgentProviderId::OpenAi => (
            format!("{OPENAI_ISSUER}/oauth/authorize"),
            format!("{OPENAI_ISSUER}/oauth/token"),
            OPENAI_CLIENT_ID.to_owned(),
            None,
            OPENAI_SCOPES.to_owned(),
        ),
        AgentProviderId::Gemini => (
            "https://accounts.google.com/o/oauth2/v2/auth".to_owned(),
            "https://oauth2.googleapis.com/token".to_owned(),
            gemini_client_id()
                .ok_or_else(|| ServiceError::invalid_operation("Gemini OAuth is not configured"))?,
            std::env::var("FORGE_GEMINI_OAUTH_CLIENT_SECRET").ok(),
            GEMINI_SCOPES.to_owned(),
        ),
        _ => {
            return Err(ServiceError::invalid_operation(
                "provider does not support browser OAuth",
            ));
        }
    };
    let mut url = Url::parse(&authorization_endpoint)
        .map_err(|_| ServiceError::invalid_operation("provider authorization URL is invalid"))?;
    url.query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", &client_id)
        .append_pair("redirect_uri", redirect_uri)
        .append_pair("scope", &scope)
        .append_pair("state", state)
        .append_pair("code_challenge", challenge)
        .append_pair("code_challenge_method", "S256");
    match provider {
        // The ChatGPT flow reads the plan and account from the id token's
        // organization claims, and rejects Google's offline-access params.
        AgentProviderId::OpenAi => {
            url.query_pairs_mut()
                .append_pair("id_token_add_organizations", "true")
                .append_pair("codex_cli_simplified_flow", "true")
                .append_pair("originator", "forge");
        }
        // Google only returns a refresh token with both of these present.
        _ => {
            url.query_pairs_mut()
                .append_pair("access_type", "offline")
                .append_pair("prompt", "consent");
        }
    }
    Ok((
        url.to_string(),
        ProtectedAuthorizationState {
            state: Some(state.to_owned()),
            code_verifier: Some(verifier.to_owned()),
            redirect_uri: Some(redirect_uri.to_owned()),
            client_id,
            client_secret,
            token_endpoint,
            scope,
            device_code: None,
            device_auth_id: None,
            user_code: None,
        },
    ))
}

async fn decode_response<T: for<'de> Deserialize<'de>>(response: reqwest::Response) -> Result<T> {
    if !response.status().is_success() {
        return Err(ServiceError::Domain(
            "provider authorization request was rejected".to_owned(),
        ));
    }
    let value = bounded_bytes(response).await?;
    serde_json::from_slice(&value).map_err(|_| {
        ServiceError::Domain("provider returned an invalid authorization response".to_owned())
    })
}

async fn bounded_json(response: reqwest::Response) -> Result<Value> {
    let value = bounded_bytes(response).await?;
    serde_json::from_slice(&value).map_err(|_| {
        ServiceError::Domain("provider returned an invalid authorization response".to_owned())
    })
}

async fn bounded_bytes(response: reqwest::Response) -> Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_AUTH_RESPONSE_BYTES as u64)
    {
        return Err(ServiceError::Domain(
            "provider authorization response was too large".to_owned(),
        ));
    }
    let bytes = response.bytes().await.map_err(|_| {
        ServiceError::Domain("provider authorization response was unavailable".to_owned())
    })?;
    if bytes.len() > MAX_AUTH_RESPONSE_BYTES {
        return Err(ServiceError::Domain(
            "provider authorization response was too large".to_owned(),
        ));
    }
    Ok(bytes.to_vec())
}

fn random_url_token() -> String {
    let mut bytes = [0_u8; 32];
    OsRng.fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

fn hash_state(state: &str) -> String {
    hex::encode(Sha256::digest(state.as_bytes()))
}

fn provider_name(provider: AgentProviderId) -> &'static str {
    match provider {
        AgentProviderId::OpenAi => "openai",
        AgentProviderId::XAi => "xai",
        AgentProviderId::Gemini => "gemini",
        AgentProviderId::OpenRouter => "openrouter",
        AgentProviderId::OpenAiCompatible => "openai_compatible",
    }
}

fn provider_base_url(provider: &str) -> &'static str {
    match provider {
        "openai" => "https://chatgpt.com/backend-api/codex",
        "xai" => "https://api.x.ai/v1",
        "gemini" => "https://generativelanguage.googleapis.com/v1beta",
        _ => "",
    }
}

fn gemini_client_id() -> Option<String> {
    std::env::var("FORGE_GEMINI_OAUTH_CLIENT_ID")
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn xai_client_id() -> String {
    std::env::var("XAI_OAUTH_CLIENT_ID")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| XAI_CLIENT_ID.to_owned())
}

fn provider_account_id_from_jwt(token: &str) -> Option<String> {
    let payload = token.split('.').nth(1)?;
    let bytes = URL_SAFE_NO_PAD.decode(payload).ok()?;
    let claims: Value = serde_json::from_slice(&bytes).ok()?;
    claims
        .get("https://api.openai.com/auth")
        .and_then(|value| value.get("chatgpt_account_id"))
        .or_else(|| claims.get("chatgpt_account_id"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn now_ms() -> u64 {
    Utc::now().timestamp_millis().max(0) as u64
}

fn expired(expires_at: &str) -> bool {
    DateTime::parse_from_rfc3339(expires_at)
        .map(|value| value.with_timezone(&Utc) <= Utc::now())
        .unwrap_or(true)
}

fn terminal(status: &str) -> bool {
    matches!(
        status,
        "succeeded" | "denied" | "expired" | "cancelled" | "failed"
    )
}

fn bounded_error(message: &str) -> String {
    let safe = match message {
        value if value.contains("denied") => "Provider authorization was denied",
        value if value.contains("expired") => "Provider authorization expired",
        value if value.contains("unavailable") => "Provider authorization service is unavailable",
        _ => "Provider authorization failed",
    };
    safe.to_owned()
}

fn redacted_host_error(_: forge_agent_host::AgentHostError) -> ServiceError {
    ServiceError::invalid_operation("protected provider authorization persistence failed")
}

#[cfg(test)]
mod tests;
