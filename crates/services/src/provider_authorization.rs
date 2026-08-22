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

use crate::{embedded_agent_service::ConnectOAuthCredential, Result, ServiceError};

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
    embedded_agents: Arc<crate::embedded_agent_service::EmbeddedAgentService>,
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

impl ProviderAuthorizationService {
    pub fn new(
        db: Arc<SqliteDb>,
        embedded_agents: Arc<crate::embedded_agent_service::EmbeddedAgentService>,
        trusted_origins: Vec<String>,
    ) -> Self {
        Self {
            protected_store: embedded_agents.protected_store(),
            db,
            embedded_agents,
            trusted_origins: Arc::new(RwLock::new(trusted_origins)),
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("provider authorization client configuration is valid"),
        }
    }

    pub fn set_trusted_origins(&self, origins: Vec<String>) {
        *self
            .trusted_origins
            .write()
            .expect("provider authorization origin lock poisoned") = origins;
    }

    pub fn capabilities(&self) -> AgentProviderCapabilitiesResponse {
        let gemini_oauth = gemini_client_id().is_some();
        AgentProviderCapabilitiesResponse {
            items: vec![
                capability(
                    AgentProviderId::OpenAi,
                    "OpenAI",
                    Some("https://api.openai.com/v1"),
                    Some("gpt-5.2"),
                    true,
                    vec![
                        method(AgentProviderId::OpenAi, ProviderCredentialMethod::ApiKey, "Use OpenAI API key", ProviderSupportLevel::Stable, true, None, None),
                        method(AgentProviderId::OpenAi,
                            ProviderCredentialMethod::BrowserOauth,
                            "Continue with ChatGPT",
                            ProviderSupportLevel::Experimental,
                            true,
                            None,
                            Some("Signs in to ChatGPT through Forge's reviewed native OAuth client. Forge does not import Codex CLI credentials."),
                        ),
                        method(AgentProviderId::OpenAi,
                            ProviderCredentialMethod::DeviceOauth,
                            "Use ChatGPT device code",
                            ProviderSupportLevel::Experimental,
                            true,
                            None,
                            Some("Device authorization availability is controlled by OpenAI and may vary by account."),
                        ),
                    ],
                ),
                capability(
                    AgentProviderId::XAi,
                    "xAI",
                    Some("https://api.x.ai/v1"),
                    Some("grok-4"),
                    true,
                    vec![
                        method(AgentProviderId::XAi, ProviderCredentialMethod::ApiKey, "Use xAI API key", ProviderSupportLevel::Stable, true, None, None),
                        method(AgentProviderId::XAi,
                            ProviderCredentialMethod::DeviceOauth,
                            "Continue with xAI",
                            ProviderSupportLevel::Experimental,
                            true,
                            None,
                            Some("Uses xAI's OIDC discovery and RFC 8628 device flow."),
                        ),
                    ],
                ),
                capability(
                    AgentProviderId::Gemini,
                    "Google Gemini",
                    Some("https://generativelanguage.googleapis.com/v1beta"),
                    Some("gemini-2.5-pro"),
                    true,
                    vec![
                        method(AgentProviderId::Gemini, ProviderCredentialMethod::ApiKey, "Use Gemini API key", ProviderSupportLevel::Stable, true, None, None),
                        method(AgentProviderId::Gemini,
                            ProviderCredentialMethod::BrowserOauth,
                            "Continue with Google",
                            if gemini_oauth { ProviderSupportLevel::Stable } else { ProviderSupportLevel::Unavailable },
                            gemini_oauth,
                            (!gemini_oauth).then_some("Set FORGE_GEMINI_OAUTH_CLIENT_ID (and, when required, FORGE_GEMINI_OAUTH_CLIENT_SECRET)."),
                            Some("Uses Google's documented OAuth endpoints. Gemini CLI auth caches are never imported."),
                        ),
                    ],
                ),
                capability(
                    AgentProviderId::OpenRouter,
                    "OpenRouter",
                    Some("https://openrouter.ai/api/v1"),
                    None,
                    true,
                    vec![method(AgentProviderId::OpenRouter, ProviderCredentialMethod::ApiKey, "Use OpenRouter API key", ProviderSupportLevel::Stable, true, None, None)],
                ),
                capability(
                    AgentProviderId::OpenAiCompatible,
                    "OpenAI-compatible",
                    None,
                    None,
                    false,
                    vec![method(AgentProviderId::OpenAiCompatible, ProviderCredentialMethod::ApiKey, "Use API key", ProviderSupportLevel::Stable, true, None, None)],
                ),
            ],
        }
    }

    pub async fn start(
        &self,
        owner_user_id: String,
        request: StartProviderAuthorizationRequest,
        request_origin: Option<String>,
    ) -> Result<ProviderAuthorizationOperation> {
        let since = (Utc::now() - ChronoDuration::minutes(1)).to_rfc3339();
        let recent = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM provider_authorization_operation
             WHERE owner_user_id = ? AND created_at >= ?",
        )
        .bind(&owner_user_id)
        .bind(&since)
        .fetch_one(self.db.pool())
        .await?;
        if recent >= 5 {
            return Err(ServiceError::RateLimited {
                retry_after_seconds: 60,
            });
        }
        if request.method == ProviderCredentialMethod::ApiKey {
            return Err(ServiceError::invalid_operation(
                "API keys use the direct connection endpoint",
            ));
        }
        self.require_supported(request.provider, request.method)?;
        // Device flows never send a browser back here, so they only need a
        // well-formed origin to record. Only the browser ceremony's post-login
        // bounce has to land on a trusted origin.
        let origin = self
            .validate_redirect_origin(
                &request.redirect_origin,
                request.method == ProviderCredentialMethod::BrowserOauth,
                request_origin.as_deref(),
            )
            .await?;
        if request.credential_label.trim().is_empty() {
            return Err(ServiceError::invalid_operation(
                "credential_label is required",
            ));
        }
        let now = Utc::now();
        let id = new_uuid_v4();
        let request_json = serde_json::to_string(&request)
            .map_err(|_| ServiceError::invalid_operation("authorization request is invalid"))?;
        match request.method {
            ProviderCredentialMethod::BrowserOauth => {
                let state = random_url_token();
                let verifier = random_url_token();
                let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
                let (redirect_uri, listener) = self
                    .browser_callback_target(request.provider, &origin, &request)
                    .await?;
                let (authorization_url, secret) = browser_authorization(
                    request.provider,
                    &redirect_uri,
                    &state,
                    &verifier,
                    &challenge,
                )?;
                let created = ProviderAuthorizationRepo::create_provider_authorization(
                    &*self.db,
                    CreateProviderAuthorizationOperation {
                        id: id.clone(),
                        owner_user_id,
                        provider: provider_name(request.provider).to_owned(),
                        method: "browser_oauth".to_owned(),
                        status: "awaiting_browser".to_owned(),
                        authorization_url: Some(authorization_url),
                        user_code: None,
                        redirect_origin: origin,
                        callback_state_hash: Some(hash_state(&state)),
                        request_json,
                        poll_interval_seconds: 5,
                        expires_at: (now + ChronoDuration::minutes(10)).to_rfc3339(),
                        created_at: now.to_rfc3339(),
                        updated_at: now.to_rfc3339(),
                    },
                )
                .await?;
                self.protected_store
                    .seal_provider_authorization_state(
                        &id,
                        &serde_json::to_vec(&secret).map_err(|_| {
                            ServiceError::invalid_operation("authorization state is invalid")
                        })?,
                        &created.updated_at,
                    )
                    .await
                    .map_err(redacted_host_error)?;
                // The sealed state has to exist before the browser can come
                // back, so the listener only starts once the row is durable.
                if let Some(listener) = listener {
                    let service = self.clone();
                    let provider = provider_name(request.provider).to_owned();
                    let return_to = created.redirect_origin.clone();
                    tokio::spawn(async move {
                        service
                            .serve_loopback_callback(listener, provider, return_to)
                            .await;
                    });
                }
                Ok(created)
            }
            ProviderCredentialMethod::DeviceOauth => {
                let created = ProviderAuthorizationRepo::create_provider_authorization(
                    &*self.db,
                    CreateProviderAuthorizationOperation {
                        id: id.clone(),
                        owner_user_id: owner_user_id.clone(),
                        provider: provider_name(request.provider).to_owned(),
                        method: "device_oauth".to_owned(),
                        status: "starting".to_owned(),
                        authorization_url: None,
                        user_code: None,
                        redirect_origin: origin,
                        callback_state_hash: None,
                        request_json,
                        poll_interval_seconds: 5,
                        expires_at: (now + ChronoDuration::minutes(15)).to_rfc3339(),
                        created_at: now.to_rfc3339(),
                        updated_at: now.to_rfc3339(),
                    },
                )
                .await?;
                match self.start_device(request.provider).await {
                    Ok((authorization_url, user_code, interval, expires_in, secret)) => {
                        let expires_at = now + ChronoDuration::seconds(expires_in as i64);
                        sqlx::query(
                            "UPDATE provider_authorization_operation SET expires_at = ? WHERE id = ?",
                        )
                        .bind(expires_at.to_rfc3339())
                        .bind(&id)
                        .execute(self.db.pool())
                        .await?;
                        self.protected_store
                            .seal_provider_authorization_state(
                                &id,
                                &serde_json::to_vec(&secret).map_err(|_| {
                                    ServiceError::invalid_operation(
                                        "authorization state is invalid",
                                    )
                                })?,
                                &now_rfc3339(),
                            )
                            .await
                            .map_err(redacted_host_error)?;
                        let operation = ProviderAuthorizationRepo::update_provider_authorization(
                            &*self.db,
                            UpdateProviderAuthorizationOperation {
                                id: id.clone(),
                                expected_version: created.version,
                                status: "awaiting_device".to_owned(),
                                authorization_url: Some(authorization_url),
                                user_code: Some(user_code),
                                poll_interval_seconds: interval as i64,
                                profile_id: None,
                                credential_handle_id: None,
                                error_code: None,
                                error_message: None,
                                updated_at: now_rfc3339(),
                                completed_at: None,
                            },
                        )
                        .await?;
                        let service = self.clone();
                        tokio::spawn(async move {
                            service.poll_device(id, owner_user_id).await;
                        });
                        Ok(operation)
                    }
                    Err(error) => {
                        self.fail(&created, "provider_unavailable", &error.to_string())
                            .await
                    }
                }
            }
            ProviderCredentialMethod::ApiKey => unreachable!(),
        }
    }

    pub async fn get(
        &self,
        id: &str,
        owner_user_id: &str,
    ) -> Result<ProviderAuthorizationOperation> {
        ProviderAuthorizationRepo::get_provider_authorization(&*self.db, id, owner_user_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("provider_authorization", id.to_owned()))
    }

    pub async fn cancel(
        &self,
        id: &str,
        owner_user_id: &str,
        expected_version: i64,
    ) -> Result<ProviderAuthorizationOperation> {
        let operation = self.get(id, owner_user_id).await?;
        if terminal(&operation.status) {
            return Ok(operation);
        }
        let updated = ProviderAuthorizationRepo::update_provider_authorization(
            &*self.db,
            UpdateProviderAuthorizationOperation {
                id: operation.id.clone(),
                expected_version,
                status: "cancelled".to_owned(),
                authorization_url: operation.authorization_url.clone(),
                user_code: operation.user_code.clone(),
                poll_interval_seconds: operation.poll_interval_seconds,
                profile_id: None,
                credential_handle_id: None,
                error_code: None,
                error_message: None,
                updated_at: now_rfc3339(),
                completed_at: Some(now_rfc3339()),
            },
        )
        .await?;
        self.protected_store
            .delete_provider_authorization_state(id)
            .await
            .map_err(redacted_host_error)?;
        Ok(updated)
    }

    pub async fn callback(
        &self,
        provider: &str,
        state: &str,
        code: Option<&str>,
        error: Option<&str>,
    ) -> Result<ProviderAuthorizationOperation> {
        let state_hash = hash_state(state);
        let operation = ProviderAuthorizationRepo::get_provider_authorization_by_state_hash(
            &*self.db,
            &state_hash,
        )
        .await?
        .ok_or_else(|| ServiceError::not_found("provider_authorization", "state"))?;
        if operation.provider != provider || terminal(&operation.status) {
            return Err(ServiceError::invalid_operation(
                "provider authorization callback is stale or mismatched",
            ));
        }
        if expired(&operation.expires_at) {
            return self
                .fail(&operation, "expired", "Provider authorization expired")
                .await;
        }
        let secret = self.protected_state(&operation.id).await?;
        if secret.state.as_deref() != Some(state) {
            return Err(ServiceError::AuthorizationDenied {
                message: "provider authorization state did not match".to_owned(),
            });
        }
        if let Some(error) = error {
            return self.fail(&operation, "provider_denied", error).await;
        }
        let Some(code) = code else {
            return self
                .fail(
                    &operation,
                    "malformed_callback",
                    "Provider callback did not include an authorization code",
                )
                .await;
        };
        let exchanging = self.advance(&operation, "exchanging").await?;
        let token = self.exchange_browser(code, &secret).await;
        match token {
            Ok(token) => self.finalize_or_fail(exchanging, token).await,
            Err(error) => {
                self.fail(&exchanging, "token_exchange_failed", &error.to_string())
                    .await
            }
        }
    }

    async fn start_device(
        &self,
        provider: AgentProviderId,
    ) -> Result<(String, String, u64, u64, ProtectedAuthorizationState)> {
        match provider {
            AgentProviderId::OpenAi => {
                let response = self
                    .client
                    .post(format!("{OPENAI_ISSUER}/api/accounts/deviceauth/usercode"))
                    .json(&serde_json::json!({"client_id": OPENAI_CLIENT_ID}))
                    .send()
                    .await
                    .map_err(|_| {
                        ServiceError::Domain(
                            "OpenAI device authorization is unavailable".to_owned(),
                        )
                    })?;
                let body: OpenAiDeviceAuthorizationResponse = decode_response(response).await?;
                let interval = body.interval.parse::<u64>().unwrap_or(5).clamp(1, 60);
                Ok((
                    format!("{OPENAI_ISSUER}/codex/device"),
                    body.user_code.clone(),
                    interval,
                    15 * 60,
                    ProtectedAuthorizationState {
                        state: None,
                        code_verifier: None,
                        redirect_uri: None,
                        client_id: OPENAI_CLIENT_ID.to_owned(),
                        client_secret: None,
                        token_endpoint: format!("{OPENAI_ISSUER}/oauth/token"),
                        scope: OPENAI_SCOPES.to_owned(),
                        device_code: None,
                        device_auth_id: Some(body.device_auth_id),
                        user_code: Some(body.user_code),
                    },
                ))
            }
            AgentProviderId::XAi => {
                let discovery: OidcDiscovery = decode_response(
                    self.client
                        .get(format!("{XAI_ISSUER}/.well-known/openid-configuration"))
                        .send()
                        .await
                        .map_err(|_| {
                            ServiceError::Domain("xAI OAuth discovery is unavailable".to_owned())
                        })?,
                )
                .await?;
                let device_endpoint = trusted_oidc_endpoint(
                    &discovery.device_authorization_endpoint.ok_or_else(|| {
                        ServiceError::Domain(
                            "xAI does not advertise device authorization".to_owned(),
                        )
                    })?,
                    XAI_ISSUER,
                )?;
                let token_endpoint = trusted_oidc_endpoint(
                    &discovery.token_endpoint.ok_or_else(|| {
                        ServiceError::Domain("xAI does not advertise a token endpoint".to_owned())
                    })?,
                    XAI_ISSUER,
                )?;
                let client_id = xai_client_id();
                let body: DeviceAuthorizationResponse = decode_response(
                    self.client
                        .post(device_endpoint)
                        .form(&[("client_id", client_id.as_str()), ("scope", XAI_SCOPES)])
                        .send()
                        .await
                        .map_err(|_| {
                            ServiceError::Domain(
                                "xAI device authorization is unavailable".to_owned(),
                            )
                        })?,
                )
                .await?;
                let verification = body
                    .verification_uri_complete
                    .clone()
                    .or(body.verification_uri)
                    .ok_or_else(|| {
                        ServiceError::Domain("xAI returned no verification URL".to_owned())
                    })?;
                Ok((
                    verification,
                    body.user_code.clone(),
                    body.interval.unwrap_or(5).clamp(1, 60),
                    body.expires_in.unwrap_or(15 * 60).min(15 * 60),
                    ProtectedAuthorizationState {
                        state: None,
                        code_verifier: None,
                        redirect_uri: None,
                        client_id,
                        client_secret: None,
                        token_endpoint,
                        scope: XAI_SCOPES.to_owned(),
                        device_code: Some(body.device_code),
                        device_auth_id: None,
                        user_code: Some(body.user_code),
                    },
                ))
            }
            _ => Err(ServiceError::invalid_operation(
                "provider does not support device authorization",
            )),
        }
    }

    async fn poll_device(&self, id: String, owner_user_id: String) {
        loop {
            let Ok(operation) = self.get(&id, &owner_user_id).await else {
                return;
            };
            if terminal(&operation.status) {
                return;
            }
            if expired(&operation.expires_at) {
                let _ = self
                    .fail(&operation, "expired", "Provider authorization expired")
                    .await;
                return;
            }
            tokio::time::sleep(Duration::from_secs(
                operation.poll_interval_seconds.max(1) as u64
            ))
            .await;
            let Ok(secret) = self.protected_state(&id).await else {
                let _ = self
                    .fail(
                        &operation,
                        "state_unavailable",
                        "Protected authorization state is unavailable",
                    )
                    .await;
                return;
            };
            match self.poll_device_once(&operation.provider, &secret).await {
                DevicePoll::Pending => {}
                DevicePoll::SlowDown => {
                    let _ = ProviderAuthorizationRepo::update_provider_authorization(
                        &*self.db,
                        UpdateProviderAuthorizationOperation {
                            id: operation.id,
                            expected_version: operation.version,
                            status: "polling".to_owned(),
                            authorization_url: operation.authorization_url,
                            user_code: operation.user_code,
                            poll_interval_seconds: (operation.poll_interval_seconds + 5).min(60),
                            profile_id: None,
                            credential_handle_id: None,
                            error_code: None,
                            error_message: None,
                            updated_at: now_rfc3339(),
                            completed_at: None,
                        },
                    )
                    .await;
                }
                DevicePoll::Denied => {
                    let _ = self
                        .fail(
                            &operation,
                            "provider_denied",
                            "Provider authorization was denied",
                        )
                        .await;
                    return;
                }
                DevicePoll::Expired => {
                    let _ = self
                        .fail(&operation, "expired", "Provider authorization expired")
                        .await;
                    return;
                }
                DevicePoll::Failed => {
                    let _ = self
                        .fail(
                            &operation,
                            "provider_rejected",
                            "Provider authorization failed",
                        )
                        .await;
                    return;
                }
                DevicePoll::Token(token) => {
                    let current = match self.get(&id, &owner_user_id).await {
                        Ok(value) => value,
                        Err(_) => return,
                    };
                    if let Ok(exchanging) = self.advance(&current, "exchanging").await {
                        let _ = self.finalize_or_fail(exchanging, token).await;
                    }
                    return;
                }
            }
        }
    }

    async fn poll_device_once(
        &self,
        provider: &str,
        secret: &ProtectedAuthorizationState,
    ) -> DevicePoll {
        if provider == "openai" {
            let response = self
                .client
                .post(format!("{OPENAI_ISSUER}/api/accounts/deviceauth/token"))
                .json(&serde_json::json!({
                    "device_auth_id": secret.device_auth_id,
                    "user_code": secret.user_code,
                }))
                .send()
                .await;
            let Ok(response) = response else {
                return DevicePoll::Failed;
            };
            if matches!(
                response.status(),
                StatusCode::FORBIDDEN | StatusCode::NOT_FOUND
            ) {
                return DevicePoll::Pending;
            }
            let code: OpenAiDeviceCodeResponse = match decode_response(response).await {
                Ok(value) => value,
                Err(_) => return DevicePoll::Failed,
            };
            let mut exchange = secret.clone();
            exchange.code_verifier = Some(code.code_verifier);
            exchange.redirect_uri = Some(format!("{OPENAI_ISSUER}/deviceauth/callback"));
            return match self
                .exchange_browser(&code.authorization_code, &exchange)
                .await
            {
                Ok(token) => DevicePoll::Token(token),
                Err(_) => DevicePoll::Failed,
            };
        }
        let response = self
            .client
            .post(&secret.token_endpoint)
            .form(&[
                ("client_id", secret.client_id.as_str()),
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                (
                    "device_code",
                    secret.device_code.as_deref().unwrap_or_default(),
                ),
            ])
            .send()
            .await;
        let Ok(response) = response else {
            return DevicePoll::Failed;
        };
        if response.status().is_success() {
            return match decode_response(response).await {
                Ok(token) => DevicePoll::Token(token),
                Err(_) => DevicePoll::Failed,
            };
        }
        let error = bounded_json(response).await.ok().and_then(|value| {
            value
                .get("error")
                .and_then(Value::as_str)
                .map(str::to_owned)
        });
        device_poll_from_error(error.as_deref())
    }

    async fn exchange_browser(
        &self,
        code: &str,
        secret: &ProtectedAuthorizationState,
    ) -> Result<OAuthTokenResponse> {
        let mut form = vec![
            ("grant_type", "authorization_code".to_owned()),
            ("code", code.to_owned()),
            ("client_id", secret.client_id.clone()),
            (
                "redirect_uri",
                secret.redirect_uri.clone().unwrap_or_default(),
            ),
            (
                "code_verifier",
                secret.code_verifier.clone().unwrap_or_default(),
            ),
        ];
        if let Some(client_secret) = secret.client_secret.clone() {
            form.push(("client_secret", client_secret));
        }
        let response = self
            .client
            .post(&secret.token_endpoint)
            .form(&form)
            .send()
            .await
            .map_err(|_| {
                ServiceError::Domain("provider token endpoint is unavailable".to_owned())
            })?;
        decode_response(response).await
    }

    async fn finalize(
        &self,
        operation: ProviderAuthorizationOperation,
        token: OAuthTokenResponse,
    ) -> Result<ProviderAuthorizationOperation> {
        let verifying = self.advance(&operation, "verifying").await?;
        let refresh_token = token.refresh_token.ok_or_else(|| {
            ServiceError::Domain("provider did not return a renewable credential".to_owned())
        })?;
        let account = token
            .id_token
            .as_deref()
            .and_then(provider_account_id_from_jwt)
            .or_else(|| provider_account_id_from_jwt(&token.access_token));
        self.verify_provider(&verifying.provider, &token.access_token, account.as_deref())
            .await?;
        let publishing = self.advance(&verifying, "publishing").await?;
        let request: StartProviderAuthorizationRequest =
            serde_json::from_str(&publishing.request_json).map_err(|_| {
                ServiceError::invalid_operation("stored authorization request is invalid")
            })?;
        let secret = self.protected_state(&publishing.id).await?;
        let bundle = OAuthCredentialBundle {
            schema_version: 1,
            access_token: token.access_token,
            refresh_token,
            expires_at_ms: now_ms()
                .saturating_add(token.expires_in.unwrap_or(3600).saturating_mul(1000)),
            token_endpoint: secret.token_endpoint,
            client_id: secret.client_id,
            client_secret: secret.client_secret,
            scopes: token
                .scope
                .unwrap_or(secret.scope)
                .split_whitespace()
                .map(str::to_owned)
                .collect(),
            provider_account_id: account,
        };
        let credential = self
            .embedded_agents
            .connect_oauth_credential(ConnectOAuthCredential {
                owner_user_id: publishing.owner_user_id.clone(),
                provider: publishing.provider.clone(),
                base_url: provider_base_url(&publishing.provider).to_owned(),
                credential_label: request.credential_label,
                credential: bundle,
            })
            .await?;
        let completed = ProviderAuthorizationRepo::update_provider_authorization(
            &*self.db,
            UpdateProviderAuthorizationOperation {
                id: publishing.id.clone(),
                expected_version: publishing.version,
                status: "succeeded".to_owned(),
                authorization_url: publishing.authorization_url,
                user_code: publishing.user_code,
                poll_interval_seconds: publishing.poll_interval_seconds,
                profile_id: None,
                credential_handle_id: Some(credential.id),
                error_code: None,
                error_message: None,
                updated_at: now_rfc3339(),
                completed_at: Some(now_rfc3339()),
            },
        )
        .await?;
        self.protected_store
            .delete_provider_authorization_state(&completed.id)
            .await
            .map_err(redacted_host_error)?;
        Ok(completed)
    }

    async fn finalize_or_fail(
        &self,
        operation: ProviderAuthorizationOperation,
        token: OAuthTokenResponse,
    ) -> Result<ProviderAuthorizationOperation> {
        let operation_id = operation.id.clone();
        let owner_user_id = operation.owner_user_id.clone();
        match self.finalize(operation, token).await {
            Ok(completed) => Ok(completed),
            Err(error) => {
                let current = self.get(&operation_id, &owner_user_id).await?;
                if terminal(&current.status) {
                    return Ok(current);
                }
                self.fail(&current, "publication_failed", &error.to_string())
                    .await
            }
        }
    }

    async fn verify_provider(
        &self,
        provider: &str,
        access_token: &str,
        account: Option<&str>,
    ) -> Result<()> {
        let endpoint = match provider {
            "openai" => "https://chatgpt.com/backend-api/wham/usage",
            "xai" => "https://api.x.ai/v1/models",
            "gemini" => "https://generativelanguage.googleapis.com/v1beta/models",
            _ => {
                return Err(ServiceError::invalid_operation(
                    "unsupported OAuth provider",
                ));
            }
        };
        let mut request = self.client.get(endpoint).bearer_auth(access_token);
        if provider == "openai" {
            if let Some(account) = account {
                request = request.header("ChatGPT-Account-Id", account);
            }
        }
        let response = request
            .send()
            .await
            .map_err(|_| ServiceError::Domain("provider verification is unavailable".to_owned()))?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(ServiceError::Domain(
                "provider rejected the authorized credential".to_owned(),
            ))
        }
    }

    async fn protected_state(&self, id: &str) -> Result<ProtectedAuthorizationState> {
        let plaintext = self
            .protected_store
            .open_provider_authorization_state(id)
            .await
            .map_err(redacted_host_error)?;
        serde_json::from_slice(&plaintext).map_err(|_| {
            ServiceError::invalid_operation("protected authorization state is invalid")
        })
    }

    async fn advance(
        &self,
        operation: &ProviderAuthorizationOperation,
        status: &str,
    ) -> Result<ProviderAuthorizationOperation> {
        ProviderAuthorizationRepo::update_provider_authorization(
            &*self.db,
            UpdateProviderAuthorizationOperation {
                id: operation.id.clone(),
                expected_version: operation.version,
                status: status.to_owned(),
                authorization_url: operation.authorization_url.clone(),
                user_code: operation.user_code.clone(),
                poll_interval_seconds: operation.poll_interval_seconds,
                profile_id: operation.profile_id.clone(),
                credential_handle_id: operation.credential_handle_id.clone(),
                error_code: None,
                error_message: None,
                updated_at: now_rfc3339(),
                completed_at: None,
            },
        )
        .await
        .map_err(Into::into)
    }

    async fn fail(
        &self,
        operation: &ProviderAuthorizationOperation,
        code: &str,
        message: &str,
    ) -> Result<ProviderAuthorizationOperation> {
        let status = if code == "provider_denied" {
            "denied"
        } else if code == "expired" {
            "expired"
        } else {
            "failed"
        };
        let updated = ProviderAuthorizationRepo::update_provider_authorization(
            &*self.db,
            UpdateProviderAuthorizationOperation {
                id: operation.id.clone(),
                expected_version: operation.version,
                status: status.to_owned(),
                authorization_url: operation.authorization_url.clone(),
                user_code: operation.user_code.clone(),
                poll_interval_seconds: operation.poll_interval_seconds,
                profile_id: None,
                credential_handle_id: None,
                error_code: Some(code.to_owned()),
                error_message: Some(bounded_error(message)),
                updated_at: now_rfc3339(),
                completed_at: Some(now_rfc3339()),
            },
        )
        .await?;
        let _ = self
            .protected_store
            .delete_provider_authorization_state(&updated.id)
            .await;
        Ok(updated)
    }

    fn require_supported(
        &self,
        provider: AgentProviderId,
        method: ProviderCredentialMethod,
    ) -> Result<()> {
        let supported = matches!(
            (provider, method),
            (
                AgentProviderId::OpenAi,
                ProviderCredentialMethod::BrowserOauth
            ) | (
                AgentProviderId::OpenAi,
                ProviderCredentialMethod::DeviceOauth
            ) | (AgentProviderId::XAi, ProviderCredentialMethod::DeviceOauth)
        ) || (provider == AgentProviderId::Gemini
            && method == ProviderCredentialMethod::BrowserOauth
            && gemini_client_id().is_some());
        if supported {
            Ok(())
        } else {
            Err(ServiceError::invalid_operation(
                "provider does not support the requested credential method",
            ))
        }
    }

    async fn validate_redirect_origin(
        &self,
        origin: &str,
        require_trusted: bool,
        request_origin: Option<&str>,
    ) -> Result<String> {
        let parsed = Url::parse(origin).map_err(|_| {
            ServiceError::invalid_operation("redirect_origin must be an absolute URL")
        })?;
        if !matches!(parsed.scheme(), "http" | "https")
            || parsed.username() != ""
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || parsed.path() != "/"
        {
            return Err(ServiceError::invalid_operation(
                "redirect_origin must contain only an HTTP(S) origin",
            ));
        }
        let normalized = parsed.origin().ascii_serialization();
        if require_trusted && !self.is_trusted_redirect_origin(&normalized, request_origin) {
            return Err(ServiceError::AuthorizationDenied {
                message: "redirect_origin is not a configured trusted origin".to_owned(),
            });
        }
        Ok(normalized)
    }

    /// Besides operator-configured origins, the origin the authenticated
    /// request itself arrived from is trusted: bouncing the browser back to
    /// where it already is cannot send it anywhere new.
    fn is_trusted_redirect_origin(&self, normalized: &str, request_origin: Option<&str>) -> bool {
        if self
            .trusted_origins
            .read()
            .expect("provider authorization origin lock poisoned")
            .iter()
            .any(|value| value == normalized)
        {
            return true;
        }
        request_origin
            .and_then(|value| Url::parse(value).ok())
            .is_some_and(|url| url.origin().ascii_serialization() == normalized)
    }

    /// Picks the OAuth `redirect_uri` for a browser ceremony, and the listener
    /// that will receive it when Forge owns the socket.
    async fn browser_callback_target(
        &self,
        provider: AgentProviderId,
        origin: &str,
        request: &StartProviderAuthorizationRequest,
    ) -> Result<(String, Option<TcpListener>)> {
        if !provider_uses_loopback_callback(provider) {
            // Operator-registered clients point back at Forge's own callback
            // route, which works from any origin the operator registered.
            return Ok((
                format!(
                    "{origin}/api/v1/provider-authorizations/{}/callback",
                    provider_name(provider)
                ),
                None,
            ));
        }
        match request.loopback_owner {
            LoopbackOwner::Client => {
                let port = request.loopback_port.ok_or_else(|| {
                    ServiceError::invalid_operation(
                        "loopback_port is required when the client owns the callback socket",
                    )
                })?;
                if !LOOPBACK_CALLBACK_PORTS.contains(&port) {
                    return Err(ServiceError::invalid_operation(
                        "loopback_port is not a port this provider's OAuth client accepts",
                    ));
                }
                Ok((loopback_redirect_uri(port), None))
            }
            LoopbackOwner::Server => {
                if !is_loopback_origin(origin) {
                    return Err(ServiceError::invalid_operation(
                        "this provider's OAuth client only accepts a localhost callback, so \
                         browser login needs Forge on the same machine as the browser; use the \
                         device-code method or `forge-ctl embedded provider login`",
                    ));
                }
                let (listener, port) = bind_loopback_callback().await?;
                Ok((loopback_redirect_uri(port), Some(listener)))
            }
        }
    }

    /// Serves the one localhost callback the provider redirects to, then bounces
    /// the browser back into the Forge UI. This is the same relay `forge-ctl`
    /// performs when the server is remote.
    async fn serve_loopback_callback(
        &self,
        listener: TcpListener,
        provider: String,
        return_to: String,
    ) {
        let Ok(Ok((mut stream, peer))) =
            tokio::time::timeout(LOOPBACK_CALLBACK_TIMEOUT, listener.accept()).await
        else {
            return;
        };
        if !peer.ip().is_loopback() {
            respond(&mut stream, 403, "Callback rejected.").await;
            return;
        }
        let Ok(head) = read_request_head(&mut stream).await else {
            respond(&mut stream, 400, "Callback was malformed.").await;
            return;
        };
        let Some(query) = callback_query(&head) else {
            respond(&mut stream, 404, "Not found.").await;
            return;
        };
        let mut state = String::new();
        let mut code = String::new();
        let mut error = String::new();
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            match key.as_ref() {
                "state" => state.push_str(&value),
                "code" => code.push_str(&value),
                "error" => error.push_str(&value),
                _ => {}
            }
        }
        if state.is_empty() || code.len() > MAX_CALLBACK_CODE_BYTES {
            respond(&mut stream, 400, "Callback was malformed.").await;
            return;
        }
        match self
            .callback(
                &provider,
                &state,
                (!code.is_empty()).then_some(code.as_str()),
                (!error.is_empty()).then_some(error.as_str()),
            )
            .await
        {
            Ok(operation) => {
                let location = format!(
                    "{return_to}/agents?provider={}&status={}&authorization={}",
                    operation.provider, operation.status, operation.id
                );
                redirect(&mut stream, &location).await;
            }
            // The operation row already carries the redacted reason; never echo
            // one into a page the provider's redirect can read.
            Err(_) => {
                respond(
                    &mut stream,
                    400,
                    "Sign-in could not be completed. Return to Forge and retry.",
                )
                .await;
            }
        }
    }
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
mod tests {
    use super::*;
    use db::{User, UserRepo};

    async fn test_service() -> (ProviderAuthorizationService, String) {
        let pool = db::create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        db::run_migrations(&pool).await.expect("migrations run");
        let db = Arc::new(SqliteDb::new(pool));
        let owner = new_uuid_v4();
        let now = now_rfc3339();
        UserRepo::create_user(
            &*db,
            &User {
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
        let embedded = Arc::new(crate::embedded_agent_service::EmbeddedAgentService::new(
            Arc::clone(&db),
            b"provider-auth-test-key",
        ));
        (
            ProviderAuthorizationService::new(
                db,
                embedded,
                vec!["http://localhost:5173".to_owned()],
            ),
            owner,
        )
    }

    /// Client-owned loopback: the caller already holds the socket, so these
    /// tests never bind a real port.
    fn browser_request() -> StartProviderAuthorizationRequest {
        StartProviderAuthorizationRequest {
            provider: AgentProviderId::OpenAi,
            method: ProviderCredentialMethod::BrowserOauth,
            redirect_origin: "http://localhost:5173".to_owned(),
            credential_label: "Test login".to_owned(),
            loopback_owner: LoopbackOwner::Client,
            loopback_port: Some(LOOPBACK_CALLBACK_PORTS[0]),
        }
    }

    fn callback_state(operation: &ProviderAuthorizationOperation) -> String {
        Url::parse(
            operation
                .authorization_url
                .as_deref()
                .expect("browser URL exists"),
        )
        .expect("browser URL parses")
        .query_pairs()
        .find_map(|(key, value)| (key == "state").then(|| value.into_owned()))
        .expect("callback state exists")
    }

    #[tokio::test]
    async fn registry_exposes_reviewed_provider_boundaries() {
        let pool = db::create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        db::run_migrations(&pool).await.expect("migrations run");
        let db = Arc::new(SqliteDb::new(pool));
        let embedded = Arc::new(crate::embedded_agent_service::EmbeddedAgentService::new(
            Arc::clone(&db),
            b"provider-auth-test-key",
        ));
        let service = ProviderAuthorizationService::new(
            db,
            embedded,
            vec!["http://localhost:5173".to_owned()],
        );
        let registry = service.capabilities();
        let openai = registry
            .items
            .iter()
            .find(|item| item.provider == AgentProviderId::OpenAi)
            .expect("OpenAI capability exists");
        assert!(openai.credential_methods.iter().any(|method| {
            method.method == ProviderCredentialMethod::BrowserOauth
                && method.action_label == "Continue with ChatGPT"
                && method.support_level == ProviderSupportLevel::Experimental
        }));
        let xai = registry
            .items
            .iter()
            .find(|item| item.provider == AgentProviderId::XAi)
            .expect("xAI capability exists");
        assert!(xai.credential_methods.iter().any(|method| {
            method.method == ProviderCredentialMethod::DeviceOauth
                && method.support_level == ProviderSupportLevel::Experimental
        }));
        assert_eq!(
            service
                .validate_redirect_origin("http://localhost:5173", true, None)
                .await
                .expect("configured origin is accepted"),
            "http://localhost:5173"
        );
        assert!(service
            .validate_redirect_origin("https://attacker.example", true, None)
            .await
            .is_err());
        // The origin the request itself arrived from is trusted even when the
        // operator never configured it — the bounce lands where the browser
        // already is.
        assert_eq!(
            service
                .validate_redirect_origin(
                    "http://10.0.0.2:8080",
                    true,
                    Some("http://10.0.0.2:8080")
                )
                .await
                .expect("the request's own origin is accepted"),
            "http://10.0.0.2:8080"
        );
        assert!(service
            .validate_redirect_origin(
                "https://attacker.example",
                true,
                Some("http://10.0.0.2:8080")
            )
            .await
            .is_err());
        // Device flows record the origin without trusting it: nothing ever
        // redirects a browser there.
        assert_eq!(
            service
                .validate_redirect_origin("https://forge.example.com", false, None)
                .await
                .expect("device flows accept any well-formed origin"),
            "https://forge.example.com"
        );
        assert!(service
            .validate_redirect_origin("not-a-url", false, None)
            .await
            .is_err());
    }

    #[test]
    fn browser_oauth_uses_pkce_and_keeps_verifier_protected() {
        let (public_url, protected) = browser_authorization(
            AgentProviderId::OpenAi,
            &loopback_redirect_uri(LOOPBACK_CALLBACK_PORTS[0]),
            "public-state",
            "private-verifier",
            "public-challenge",
        )
        .expect("authorization builds");
        assert!(public_url.contains("code_challenge=public-challenge"));
        assert!(public_url.contains("state=public-state"));
        assert!(!public_url.contains("private-verifier"));
        assert_eq!(protected.code_verifier.as_deref(), Some("private-verifier"));
        // ChatGPT reads plan and account from the id token's org claims, and
        // must not receive Google's offline-access params.
        assert!(public_url.contains("id_token_add_organizations=true"));
        assert!(public_url.contains("codex_cli_simplified_flow=true"));
        assert!(!public_url.contains("access_type"));
        assert!(!public_url.contains("prompt=consent"));
        assert!(public_url.contains("redirect_uri=http%3A%2F%2Flocalhost%3A1455%2Fauth%2Fcallback"));
    }

    #[test]
    fn only_openai_uses_the_loopback_callback() {
        assert!(provider_uses_loopback_callback(AgentProviderId::OpenAi));
        // Gemini's client is operator-registered against Forge's own route.
        assert!(!provider_uses_loopback_callback(AgentProviderId::Gemini));
        assert!(is_loopback_origin("http://localhost:5173"));
        assert!(is_loopback_origin("http://127.0.0.1:8080"));
        assert!(!is_loopback_origin("https://forge.example.com"));
    }

    #[tokio::test]
    async fn client_owned_loopback_requires_a_whitelisted_port() {
        let (service, owner) = test_service().await;
        let mut request = browser_request();
        request.loopback_port = None;
        assert!(service.start(owner.clone(), request, None).await.is_err());

        let mut request = browser_request();
        request.loopback_port = Some(9999);
        assert!(service.start(owner.clone(), request, None).await.is_err());
    }

    #[tokio::test]
    async fn server_owned_loopback_needs_a_local_browser() {
        let (service, owner) = test_service().await;
        let mut request = browser_request();
        request.loopback_owner = LoopbackOwner::Server;
        request.loopback_port = None;
        request.redirect_origin = "https://forge.example.com".to_owned();
        service.set_trusted_origins(vec!["https://forge.example.com".to_owned()]);
        // Trusted, but the browser is not on this machine, so nothing could
        // ever answer the localhost callback.
        let error = service
            .start(owner, request, None)
            .await
            .expect_err("remote browser cannot own a localhost callback");
        assert!(error.to_string().contains("same machine"));
    }

    /// End to end over a real socket: Forge binds the callback, the "browser"
    /// hits it, and the operation lands in a terminal state.
    #[tokio::test]
    async fn server_owned_loopback_serves_the_browser_callback() {
        let (service, owner) = test_service().await;
        let mut request = browser_request();
        request.loopback_owner = LoopbackOwner::Server;
        request.loopback_port = None;
        let operation = service
            .start(owner.clone(), request, None)
            .await
            .expect("browser authorization starts");
        let authorization_url =
            Url::parse(operation.authorization_url.as_deref().expect("browser URL"))
                .expect("browser URL parses");
        let redirect_uri = authorization_url
            .query_pairs()
            .find_map(|(key, value)| (key == "redirect_uri").then(|| value.into_owned()))
            .expect("redirect_uri is present");
        let port = Url::parse(&redirect_uri)
            .expect("redirect_uri parses")
            .port()
            .expect("redirect_uri carries a port");
        assert!(LOOPBACK_CALLBACK_PORTS.contains(&port));
        let state = callback_state(&operation);

        // A denial keeps the exchange local: no provider token endpoint call.
        let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, port))
            .await
            .expect("callback listener is bound");
        let request_line = format!(
            "GET /auth/callback?state={state}&error=access_denied HTTP/1.1\r\n\
             Host: localhost\r\nConnection: close\r\n\r\n"
        );
        stream
            .write_all(request_line.as_bytes())
            .await
            .expect("callback request sends");
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .await
            .expect("callback responds");
        assert!(response.starts_with("HTTP/1.1 303 See Other"));
        assert!(response.contains("Location: http://localhost:5173/agents?provider=openai"));
        assert_eq!(
            service
                .get(&operation.id, &owner)
                .await
                .expect("owner can read operation")
                .status,
            "denied"
        );
    }

    #[tokio::test]
    async fn browser_denial_is_terminal_redacted_and_replay_safe() {
        let (service, owner) = test_service().await;
        let operation = service
            .start(owner.clone(), browser_request(), None)
            .await
            .expect("browser authorization starts");
        let state = callback_state(&operation);
        assert!(service
            .callback("openai", "mismatched-state", Some("code"), None)
            .await
            .is_err());
        assert_eq!(
            service
                .get(&operation.id, &owner)
                .await
                .expect("state mismatch leaves operation pending")
                .status,
            "awaiting_browser"
        );
        let denied = service
            .callback("openai", &state, None, Some("access_denied: secret detail"))
            .await
            .expect("denial is recorded");
        assert_eq!(denied.status, "denied");
        assert_eq!(denied.error_code.as_deref(), Some("provider_denied"));
        assert_eq!(
            denied.error_message.as_deref(),
            Some("Provider authorization was denied")
        );
        assert!(!denied.error_message.unwrap().contains("secret"));
        assert!(service
            .callback("openai", &state, Some("replayed-code"), None)
            .await
            .is_err());
        assert_eq!(
            service
                .get(&operation.id, &owner)
                .await
                .expect("owner can read operation")
                .status,
            "denied"
        );
    }

    #[tokio::test]
    async fn expired_callback_never_reaches_token_exchange() {
        let (service, owner) = test_service().await;
        let operation = service
            .start(owner.clone(), browser_request(), None)
            .await
            .expect("browser authorization starts");
        let state = callback_state(&operation);
        sqlx::query("UPDATE provider_authorization_operation SET expires_at = ? WHERE id = ?")
            .bind("2000-01-01T00:00:00Z")
            .bind(&operation.id)
            .execute(service.db.pool())
            .await
            .expect("operation expires");

        let expired = service
            .callback("openai", &state, Some("must-not-be-exchanged"), None)
            .await
            .expect("expiry is recorded");
        assert_eq!(expired.status, "expired");
        assert_eq!(expired.error_code.as_deref(), Some("expired"));
    }

    #[tokio::test]
    async fn failed_finalization_is_terminal_and_erases_transient_state() {
        let (service, owner) = test_service().await;
        let operation = service
            .start(owner.clone(), browser_request(), None)
            .await
            .expect("browser authorization starts");
        let exchanging = service
            .advance(&operation, "exchanging")
            .await
            .expect("operation advances");

        let failed = service
            .finalize_or_fail(
                exchanging,
                OAuthTokenResponse {
                    access_token: "access-secret".to_owned(),
                    refresh_token: None,
                    expires_in: Some(3600),
                    scope: None,
                    id_token: None,
                },
            )
            .await
            .expect("failure is recorded as an operation result");

        assert_eq!(failed.status, "failed");
        assert_eq!(failed.error_code.as_deref(), Some("publication_failed"));
        assert_eq!(
            failed.error_message.as_deref(),
            Some("Provider authorization failed")
        );
        assert!(!failed.error_message.unwrap().contains("access-secret"));
        assert!(service
            .protected_store
            .open_provider_authorization_state(&operation.id)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn malformed_callback_is_terminal_and_redacted() {
        let (service, owner) = test_service().await;
        let operation = service
            .start(owner, browser_request(), None)
            .await
            .expect("browser authorization starts");
        let state = callback_state(&operation);

        let failed = service
            .callback("openai", &state, None, None)
            .await
            .expect("malformed callback is recorded");

        assert_eq!(failed.status, "failed");
        assert_eq!(failed.error_code.as_deref(), Some("malformed_callback"));
        assert_eq!(
            failed.error_message.as_deref(),
            Some("Provider authorization failed")
        );
    }

    #[tokio::test]
    async fn authorization_start_is_account_rate_limited() {
        let (service, owner) = test_service().await;
        for _ in 0..5 {
            service
                .start(owner.clone(), browser_request(), None)
                .await
                .expect("start remains inside account budget");
        }
        assert!(matches!(
            service.start(owner, browser_request(), None).await,
            Err(ServiceError::RateLimited {
                retry_after_seconds: 60
            })
        ));
    }

    #[test]
    fn device_errors_and_discovery_boundaries_are_deterministic() {
        assert!(matches!(
            device_poll_from_error(Some("authorization_pending")),
            DevicePoll::Pending
        ));
        assert!(matches!(
            device_poll_from_error(Some("slow_down")),
            DevicePoll::SlowDown
        ));
        assert!(matches!(
            device_poll_from_error(Some("access_denied")),
            DevicePoll::Denied
        ));
        assert!(trusted_oidc_endpoint("https://auth.x.ai/oauth/token", XAI_ISSUER).is_ok());
        assert!(trusted_oidc_endpoint("http://auth.x.ai/oauth/token", XAI_ISSUER).is_err());
        assert!(trusted_oidc_endpoint("https://attacker.example/token", XAI_ISSUER).is_err());
        assert_eq!(
            bounded_error("upstream outage includes bearer secret"),
            "Provider authorization failed"
        );
    }
}
