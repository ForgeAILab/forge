//! Embedded provider credentials, profiles, health probes, and usage.

use super::*;

impl EmbeddedAgentService {
    /// Create an API-key provider entry after validating the provider,
    /// endpoint, and key shape. No agent, profile, or binding changes.
    pub async fn connect_api_key_credential(
        &self,
        input: ConnectApiKeyCredential,
    ) -> Result<CredentialHandle> {
        executors::server_commands::ensure_agents_allowed()?;
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
        executors::server_commands::ensure_agents_allowed()?;
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
        let handle = CredentialHandleRepo::get_credential_handle_for_owner(
            &*self.db,
            credential_id,
            owner_user_id,
        )
        .await?
        .ok_or_else(|| ServiceError::not_found("credential_handle", credential_id.to_owned()))?;
        if handle.status != "configured" {
            return Err(ServiceError::invalid_operation(
                "provider entry is disconnected",
            ));
        }
        if !handle.enabled {
            return Err(ServiceError::invalid_operation(
                "provider entry is disabled; enable it before starting new work",
            ));
        }
        Ok(handle)
    }

    pub(super) async fn require_profile_source_enabled(
        &self,
        profile: &AgentProfile,
    ) -> Result<()> {
        let Some(credential_ref) = profile.credential_ref.as_deref() else {
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
        if !handle.enabled {
            return Err(ServiceError::invalid_operation(
                "referenced provider entry is disabled",
            ));
        }
        Ok(())
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
        let client = provider_http_client(
            &base_url,
            Duration::from_secs(10),
            &ProxyEnvironment::from_env(),
        )
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
                } else if status.as_u16() == 429 || status.is_server_error() {
                    ProviderEntryTestOutcome::failed(
                        latency_ms,
                        format!("provider returned HTTP {}", status.as_u16()),
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
        provider_url_addresses(&base_url)
            .await
            .map_err(ServiceError::invalid_operation)?;
        let reasoning_effort = validate_native_reasoning_effort(
            &entry.provider,
            &entry.credential_method,
            &base_url,
            &input.model,
            input.reasoning_effort.as_deref(),
        )?;
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
            reasoning_effort,
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
        provider_url_addresses(&base_url)
            .await
            .map_err(ServiceError::invalid_operation)?;
        let reasoning_effort = validate_native_reasoning_effort(
            &entry.provider,
            &entry.credential_method,
            &base_url,
            &input.model,
            input.reasoning_effort.as_deref(),
        )?;
        let system_prompt = validate_public_runtime_text(input.system_prompt.as_deref())?;
        let account_permission_ceiling = self
            .db
            .embedded_identity_account_permission_ceiling(&identity.id)
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
                reasoning_effort,
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
        if !handle.enabled {
            return Err(ServiceError::invalid_operation(
                "referenced provider entry is disabled",
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
        let _ = self
            .db
            .record_embedded_credential_base_url(base_url.trim_end_matches('/'), credential_id)
            .await;
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
        let health = AgentConnectionHealthRepo::upsert_connection_health(
            &*self.db,
            UpsertAgentConnectionHealth {
                profile_id: profile.id.clone(),
                status,
                capability_status_json: profile.capabilities_json.clone(),
                checked_at: Some(now.clone()),
                error_code: error_code.clone(),
                updated_at: now,
            },
        )
        .await
        .map_err(ServiceError::from)?;
        if let Some(credential_id) = profile.credential_ref.as_deref() {
            let outcome = match error_code.as_deref() {
                None => Some(Ok(())),
                Some("provider_authentication_failed") => Some(Err("provider returned HTTP 401")),
                Some("provider_timeout" | "provider_unreachable") => {
                    Some(Err("provider HTTP request failed"))
                }
                _ => None,
            };
            if let Some(outcome) = outcome {
                crate::provider_health::record_entry_outcome(&self.db, credential_id, outcome)
                    .await?;
            }
        }
        Ok(health)
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
        let client = provider_http_client(
            base_url,
            Duration::from_secs(8),
            &ProxyEnvironment::from_env(),
        )
        .await
        .map_err(|message| {
            if message == UNUSABLE_PROXY_MESSAGE {
                "outbound_proxy_unusable".to_owned()
            } else {
                "provider_endpoint_invalid".to_owned()
            }
        })?;
        let endpoint = format!("{}/models", base_url.trim_end_matches('/'));
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

/// Whether a base URL points at the ChatGPT Codex backend, which speaks the
/// Responses API only and has no OpenAI-compatible `GET /models` route.
fn is_codex_backend(base_url: &str) -> bool {
    base_url.contains("chatgpt.com/backend-api/codex")
}

pub(super) fn validate_native_reasoning_effort(
    provider: &str,
    credential_method: &str,
    base_url: &str,
    model: &str,
    reasoning_effort: Option<&str>,
) -> Result<Option<String>> {
    let Some(reasoning_effort) = reasoning_effort
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    if provider != "openai" || credential_method != "oauth_bundle" || !is_codex_backend(base_url) {
        return Err(ServiceError::invalid_operation(
            "reasoning effort is supported only for direct ChatGPT login profiles",
        ));
    }
    let supported_efforts = codex_reasoning_efforts_for_model(model);
    if !supported_efforts.contains(&reasoning_effort) {
        return Err(ServiceError::invalid_operation(format!(
            "direct ChatGPT model `{model}` supports reasoning efforts: {}",
            supported_efforts.join(", ")
        )));
    }
    Ok(Some(reasoning_effort.to_owned()))
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
pub(super) fn usage_snapshot_from_wham_json(
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

pub(super) fn rfc3339_from_unix_seconds(seconds: i64) -> Option<String> {
    chrono::DateTime::from_timestamp(seconds, 0).map(|instant| instant.to_rfc3339())
}

fn now_unix_seconds() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Provider tests and probes share native request/fetch/search egress policy.
pub(super) async fn provider_http_client(
    base_url: &str,
    timeout: Duration,
    proxies: &ProxyEnvironment,
) -> std::result::Result<reqwest::Client, &'static str> {
    let parsed = validate_provider_url_shape(base_url)?;
    public_http_client_builder(&parsed, PublicAddressPolicy::Provider, proxies)
        .await
        .map_err(provider_egress_refusal)?
        .timeout(timeout)
        .build()
        .map_err(|_| "provider client unavailable")
}
