//! The provider authorization service: constructor, capability registry, browser and device flows, and terminal state handling.

use super::*;

impl ProviderAuthorizationService {
    pub fn new(
        db: Arc<SqliteDb>,
        embedded_agents: Arc<EmbeddedAgentService>,
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
                    Some("gemini-3.1-pro-preview"),
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
        executors::server_commands::ensure_agents_allowed()
            .map_err(|_| ServiceError::ServerAgentsDisabled)?;
        let since = (Utc::now() - ChronoDuration::minutes(1)).to_rfc3339();
        let recent = self
            .db
            .recent_provider_authorization_count(&owner_user_id, &since)
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
                        self.db
                            .update_provider_device_authorization_expiry(
                                expires_at.to_rfc3339(),
                                &id,
                            )
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

    pub(super) async fn finalize_or_fail(
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

    pub(super) async fn advance(
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

    pub(super) async fn validate_redirect_origin(
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
