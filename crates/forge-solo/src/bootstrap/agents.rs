//! Agent registration, provider discovery, and selection eligibility for Solo bootstrap.

use super::*;

impl SoloBootstrapService {
    /// Create or resume an owner-scoped local CLI Agent from a positive,
    /// structured harness discovery result. This is the convergent bridge for
    /// a new isolated store, where no Agent row exists yet. It deliberately
    /// does not claim an ownerless/global Agent: the current typed Agent
    /// boundary has no safe owner-CAS operation, and silently taking one would
    /// cross account scope. The returned candidate may still be ineligible if
    /// the daemon/source changed between registration and the health read.
    pub async fn ensure_authenticated_agent(
        &self,
        owner_id: &str,
        mut registration: SoloAuthenticatedAgentRegistration,
    ) -> Result<SoloAgentCandidate> {
        let owner_id = required("owner_id", owner_id)?;
        UserRepo::get_user_by_id(&*self.db, owner_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("user", owner_id.to_owned()))?;
        registration.registration_key = registration.registration_key.trim().to_owned();
        registration.name = registration.name.trim().to_owned();
        registration.executor_type = registration.executor_type.trim().to_ascii_lowercase();
        if registration.registration_key.is_empty() {
            return Err(ServiceError::invalid_operation(
                "registration_key must not be empty",
            ));
        }
        if registration.name.is_empty() {
            return Err(ServiceError::invalid_operation("name must not be empty"));
        }
        if !registration.availability.is_authenticated() {
            return Err(ServiceError::invalid_operation(
                "only authenticated harness discovery may register a Solo Agent",
            ));
        }
        if !allowed_cli_executor(&registration.executor_type) {
            return Err(ServiceError::invalid_operation(format!(
                "unsupported Solo CLI executor: {}",
                registration.executor_type
            )));
        }
        if registration
            .daemon_id
            .as_deref()
            .is_some_and(|daemon_id| daemon_id.trim().is_empty())
        {
            registration.daemon_id = None;
        }
        if registration
            .credential_ref
            .as_deref()
            .is_some_and(|credential_ref| credential_ref.trim().is_empty())
        {
            registration.credential_ref = None;
        }

        let agents = list_agents(&self.db).await?;
        let matching = agents
            .iter()
            .filter(|agent| {
                agent_registration_key(agent).as_deref()
                    == Some(registration.registration_key.as_str())
            })
            .collect::<Vec<_>>();
        if matching.len() > 1 {
            return Err(conflict(
                "more than one Agent is bound to the same Solo registration key",
            ));
        }

        let agent = if let Some(agent) = matching.into_iter().next() {
            if agent.owner_id.as_deref() != Some(owner_id) {
                return Err(conflict(
                    "Solo registration key is already bound to another owner scope",
                ));
            }
            if agent.executor_type != registration.executor_type
                || agent.daemon_id != registration.daemon_id
                || agent.credential_ref != registration.credential_ref
            {
                return Err(conflict(
                    "Solo Agent registration changed its executor or source binding",
                ));
            }
            agent.clone()
        } else {
            let now = db::now_rfc3339();
            let config_json = serde_json::to_string(&json!({
                SOLO_AGENT_REGISTRATION_KEY: registration.registration_key,
            }))
            .map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "Solo Agent registration metadata is not serializable: {error}"
                ))
            })?;
            AgentRepo::create(
                &*self.db,
                CreateAgent {
                    id: db::new_uuid_v4(),
                    name: registration.name.clone(),
                    description: Some("Forge Solo local CLI harness".to_owned()),
                    executor_type: registration.executor_type.clone(),
                    model: None,
                    reasoning_effort: None,
                    permission_policy: None,
                    prompt_template: None,
                    capabilities_json: "[]".to_owned(),
                    config_json,
                    credential_ref: registration.credential_ref.clone(),
                    daemon_id: registration.daemon_id.clone(),
                    max_concurrent_tasks: 1,
                    heartbeat_interval_seconds: 30,
                    max_missed_heartbeats: 3,
                    status: AgentStatus::Idle,
                    last_heartbeat_at: Some(now.clone()),
                    is_default: false,
                    paused: false,
                    owner_id: Some(owner_id.to_owned()),
                    visibility: "account".to_owned(),
                    created_at: now.clone(),
                    updated_at: now,
                },
            )
            .await?
        };

        let evaluation = self
            .evaluate_candidate(
                owner_id,
                SoloAgentCandidateInput {
                    identity_id: agent.id,
                    profile_id: Some(agent.profile_id),
                    executor_type: registration.executor_type,
                    availability: registration.availability,
                    display_name: Some(registration.name),
                },
            )
            .await?;
        Ok(evaluation.candidate)
    }

    pub(super) async fn candidate_evaluations(
        &self,
        request: &SoloBootstrapRequest,
        owner_id: &str,
        durable_metadata: Option<&SoloProjectMetadata>,
    ) -> Result<Vec<CandidateEvaluation>> {
        let mut inputs = if request.agent_candidates.is_empty() {
            self.discovered_candidate_inputs(owner_id).await?
        } else {
            request.agent_candidates.clone()
        };

        // A persisted selection must still be re-readable even when a caller
        // retries without passing the previous discovery response.
        for identity_id in [
            request.selected_project_agent_id.as_deref(),
            request.selected_worker_agent_id.as_deref(),
            durable_metadata.and_then(|metadata| metadata.selected_project_agent_id.as_deref()),
            durable_metadata.and_then(|metadata| metadata.selected_worker_agent_id.as_deref()),
        ]
        .into_iter()
        .flatten()
        {
            if !inputs.iter().any(|input| input.identity_id == identity_id) {
                if let Some(agent) = AgentRepo::get_by_id(&*self.db, identity_id).await? {
                    inputs.push(SoloAgentCandidateInput {
                        identity_id: agent.id,
                        profile_id: Some(agent.profile_id),
                        executor_type: agent.executor_type,
                        availability: SoloAgentAvailability::Authenticated,
                        display_name: Some(agent.name),
                    });
                }
            }
        }
        self.evaluate_candidate_inputs(owner_id, inputs).await
    }

    async fn discovered_candidate_inputs(
        &self,
        owner_id: &str,
    ) -> Result<Vec<SoloAgentCandidateInput>> {
        Ok(list_agents(&self.db)
            .await?
            .into_iter()
            .filter(|agent| agent.owner_id.as_deref() == Some(owner_id))
            .map(|agent| SoloAgentCandidateInput {
                identity_id: agent.id,
                profile_id: Some(agent.profile_id),
                executor_type: agent.executor_type,
                // The source check below is authoritative.  This value is a
                // discovery hint, never an eligibility grant.
                availability: SoloAgentAvailability::Authenticated,
                display_name: Some(agent.name),
            })
            .collect())
    }

    /// Config-declared direct Agents owned by the Solo owner. These were
    /// materialized by the bootstrap provider sync; startup appends them to
    /// the explicit CLI candidate list so the picker shows both families.
    pub async fn config_agent_candidates(
        &self,
        owner_id: &str,
    ) -> Result<Vec<SoloAgentCandidateInput>> {
        Ok(list_agents(&self.db)
            .await?
            .into_iter()
            .filter(|agent| {
                agent.owner_id.as_deref() == Some(owner_id)
                    && agent.backend_kind == "native"
                    && agent.executor_type == "embedded"
            })
            .map(|agent| SoloAgentCandidateInput {
                identity_id: agent.id,
                profile_id: Some(agent.profile_id),
                executor_type: "embedded".to_owned(),
                availability: SoloAgentAvailability::Authenticated,
                display_name: Some(agent.name),
            })
            .collect())
    }

    /// Materialize the config-declared providers as protected provider
    /// entries and direct embedded Agents owned by the Solo owner.  The sync
    /// is idempotent: entries are keyed by a `config:` label and Agents by a
    /// `provider/model` name, so repeated launches reuse the durable records.
    /// A provider that cannot be connected is skipped with a log line; it
    /// never blocks bootstrap or removes existing candidates.
    #[tracing::instrument(skip(self, owner_id), fields(providers = self.config_providers.entries.len()))]
    pub(super) async fn sync_config_provider_agents(&self, owner_id: &str) {
        let Some(embedded) = self.embedded_agents.as_ref() else {
            return;
        };
        if self.config_providers.entries.is_empty() {
            return;
        }
        let existing_handles =
            db::CredentialHandleRepo::list_credential_handles(&*self.db, owner_id)
                .await
                .unwrap_or_default();
        let existing_agents: Vec<Agent> = list_agents(&self.db)
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|agent| agent.owner_id.as_deref() == Some(owner_id))
            .collect();

        for (provider_name, declaration) in &self.config_providers.entries {
            let secret = match resolve_provider_secret(provider_name, declaration) {
                Ok(secret) => secret,
                Err(reason) => {
                    tracing::warn!(
                        provider = provider_name.as_str(),
                        reason,
                        "skipping config provider"
                    );
                    continue;
                }
            };
            let label = format!("config:{provider_name}");
            let credential_id = match existing_handles
                .iter()
                .find(|handle| handle.label == label && handle.enabled)
            {
                Some(existing) => existing.id.clone(),
                None => {
                    match embedded
                        .connect_api_key_credential(
                            services_agents::embedded_agent_service::ConnectApiKeyCredential {
                                owner_user_id: owner_id.to_owned(),
                                provider: declaration.kind.clone(),
                                label: label.clone(),
                                credential: secret,
                                base_url: declaration.base_url.clone(),
                            },
                        )
                        .await
                    {
                        Ok(handle) => handle.id,
                        Err(error) => {
                            tracing::warn!(
                                provider = provider_name.as_str(),
                                error = %error,
                                "config provider entry could not be created"
                            );
                            continue;
                        }
                    }
                }
            };
            for model in &declaration.models {
                let name = format!("{provider_name}/{}", model.model);
                if existing_agents
                    .iter()
                    .any(|agent| agent.name == name && agent.backend_kind == "native")
                {
                    continue;
                }
                if let Err(error) = embedded
                    .create_agent_from_entry(
                        services_agents::embedded_agent_service::CreateEmbeddedAgent {
                            owner_user_id: owner_id.to_owned(),
                            name,
                            description: Some(format!(
                                "Config-declared provider agent ({})",
                                declaration.kind
                            )),
                            credential_id: credential_id.clone(),
                            model: model.model.clone(),
                            reasoning_effort: model.reasoning_effort.clone(),
                            system_prompt: None,
                            account_permission_ceiling:
                                services_agents::embedded_agent_service::default_profile_tool_policy(),
                            tool_policy:
                                services_agents::embedded_agent_service::default_profile_tool_policy(),
                            context_tokens: model.context_tokens,
                            max_input_tokens: model.max_input_tokens,
                            max_output_tokens: model.max_output_tokens,
                        },
                    )
                    .await
                {
                    tracing::warn!(
                        provider = provider_name.as_str(),
                        model = model.model.as_str(),
                        error = %error,
                        "config provider agent could not be created"
                    );
                }
            }
        }
    }

    async fn evaluate_candidate_inputs(
        &self,
        owner_id: &str,
        inputs: Vec<SoloAgentCandidateInput>,
    ) -> Result<Vec<CandidateEvaluation>> {
        let mut seen = HashSet::new();
        let mut evaluations = Vec::with_capacity(inputs.len());
        for input in inputs {
            let identity_id = input.identity_id.trim().to_owned();
            if identity_id.is_empty() {
                continue;
            }
            if !seen.insert(identity_id.clone()) {
                return Err(conflict(format!(
                    "duplicate Solo Agent candidate {identity_id}"
                )));
            }
            evaluations.push(self.evaluate_candidate(owner_id, input).await?);
        }
        evaluations.sort_by(|left, right| {
            left.candidate
                .executor_type
                .cmp(&right.candidate.executor_type)
                .then_with(|| {
                    left.candidate
                        .display_name
                        .cmp(&right.candidate.display_name)
                })
                .then_with(|| left.candidate.identity_id.cmp(&right.candidate.identity_id))
        });
        Ok(evaluations)
    }

    async fn evaluate_candidate(
        &self,
        owner_id: &str,
        input: SoloAgentCandidateInput,
    ) -> Result<CandidateEvaluation> {
        let identity_id = input.identity_id.trim().to_owned();
        let executor_type = input.executor_type.trim().to_ascii_lowercase();
        let display_name = input
            .display_name
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(&executor_type)
            .to_owned();
        let mut candidate = SoloAgentCandidate {
            identity_id: identity_id.clone(),
            profile_id: input.profile_id.clone(),
            display_name,
            executor_type: executor_type.clone(),
            availability: input.availability,
            health: SoloAgentHealth::Unavailable,
            eligible: false,
            reason: None,
            next_step: None,
        };

        // Direct embedded Agents are config/provider-backed: they carry a
        // protected credential instead of a CLI login and are validated by the
        // native profile alignment check below.
        if !allowed_cli_executor(&executor_type) && executor_type != "embedded" {
            candidate.reason = Some("unsupported_cli_executor".to_owned());
            candidate.next_step = Some("choose a supported authenticated CLI harness".to_owned());
            return Ok(CandidateEvaluation {
                candidate,
                agent: None,
            });
        }
        if !input.availability.is_authenticated() {
            candidate.reason = Some(availability_reason(input.availability).to_owned());
            candidate.next_step = Some(availability_next_step(input.availability).to_owned());
            return Ok(CandidateEvaluation {
                candidate,
                agent: None,
            });
        }

        let Some(agent) = AgentRepo::get_by_id(&*self.db, &identity_id).await? else {
            candidate.availability = SoloAgentAvailability::Unknown;
            candidate.reason = Some("agent_identity_missing".to_owned());
            candidate.next_step = Some("refresh local Agent discovery".to_owned());
            return Ok(CandidateEvaluation {
                candidate,
                agent: None,
            });
        };
        if agent.owner_id.as_deref() != Some(owner_id) {
            // Do not disclose another scope's identity in a picker.
            candidate.availability = SoloAgentAvailability::Unknown;
            candidate.reason = Some("agent_not_owned_by_solo_store".to_owned());
            candidate.next_step = Some("choose an Agent owned by this Solo store".to_owned());
            return Ok(CandidateEvaluation {
                candidate,
                agent: None,
            });
        }
        if agent.executor_type != executor_type {
            candidate.reason = Some("executor_changed".to_owned());
            candidate.next_step = Some("refresh local Agent discovery".to_owned());
            return Ok(CandidateEvaluation {
                candidate,
                agent: Some(agent),
            });
        }
        if let Some(profile_id) = input.profile_id.as_deref() {
            if profile_id != agent.profile_id {
                candidate.reason = Some("profile_changed".to_owned());
                candidate.next_step = Some("refresh the Agent profile".to_owned());
                return Ok(CandidateEvaluation {
                    candidate,
                    agent: Some(agent),
                });
            }
        }
        candidate.profile_id = Some(agent.profile_id.clone());
        candidate.display_name = agent.name.clone();

        let Some(profile) = AgentProfileRepo::get_profile(&*self.db, &agent.profile_id)
            .await?
            .filter(|profile| profile.identity_id == agent.id)
        else {
            candidate.reason = Some("agent_profile_missing".to_owned());
            candidate.next_step = Some("refresh local Agent discovery".to_owned());
            return Ok(CandidateEvaluation {
                candidate,
                agent: Some(agent),
            });
        };
        if (profile.backend_kind != "cli"
            || profile.executor_type != executor_type
            || agent.backend_kind != "cli")
            && (profile.backend_kind != "native"
                || executor_type != "embedded"
                || agent.backend_kind != "native")
        {
            candidate.reason = Some("not_a_local_cli_profile".to_owned());
            candidate.next_step = Some("choose a supported local CLI harness".to_owned());
            return Ok(CandidateEvaluation {
                candidate,
                agent: Some(agent),
            });
        }

        if let Some(credential_ref) = profile.credential_ref.as_deref() {
            let configured = db::CredentialHandleRepo::get_credential_handle_for_owner(
                &*self.db,
                credential_ref,
                owner_id,
            )
            .await?
            .is_some_and(|handle| handle.status == "configured" && handle.enabled);
            if !configured {
                candidate.availability = SoloAgentAvailability::Disabled;
                candidate.reason = Some("credential_source_disabled".to_owned());
                candidate.next_step = Some("enable or authenticate the CLI credential".to_owned());
                return Ok(CandidateEvaluation {
                    candidate,
                    agent: Some(agent),
                });
            }
        }

        if !cli_runtime_source_enabled(&self.db, &agent).await? {
            candidate.availability = SoloAgentAvailability::Unauthenticated;
            candidate.reason = Some("cli_harness_not_authenticated".to_owned());
            candidate.next_step = Some("log in to the CLI and retry discovery".to_owned());
            return Ok(CandidateEvaluation {
                candidate,
                agent: Some(agent),
            });
        }

        match compute_effective_status(&self.db, &agent, None).await? {
            EffectiveStatus::Active => {
                candidate.health = SoloAgentHealth::Healthy;
                candidate.eligible = true;
            }
            EffectiveStatus::Busy => {
                // Capacity is enforced at Task claim.  Busy is still a valid
                // identity selection and is shown distinctly to the picker.
                candidate.health = SoloAgentHealth::Busy;
                candidate.eligible = true;
            }
            status => {
                candidate.availability = SoloAgentAvailability::Unknown;
                candidate.reason = Some(format!("agent_source_{}", status.as_str()));
                candidate.next_step = Some(status_next_step(&status).to_owned());
            }
        }
        Ok(CandidateEvaluation {
            candidate,
            agent: Some(agent),
        })
    }

    pub(super) async fn require_selected_candidates(
        &self,
        metadata: &SoloProjectMetadata,
        evaluations: &[CandidateEvaluation],
        final_check: bool,
    ) -> Result<()> {
        for (label, selected_id) in [
            (
                "Project Agent",
                metadata.selected_project_agent_id.as_deref(),
            ),
            ("Task Worker", metadata.selected_worker_agent_id.as_deref()),
        ] {
            let Some(selected_id) = selected_id else {
                continue;
            };
            let evaluation = evaluations
                .iter()
                .find(|evaluation| evaluation.candidate.identity_id == selected_id)
                .ok_or_else(|| {
                    conflict(format!(
                        "selected {label} {selected_id} is not in current local Agent discovery"
                    ))
                })?;
            if !evaluation.candidate.eligible {
                return Err(conflict(format!(
                    "selected {label} {selected_id} is unavailable: {}",
                    evaluation
                        .candidate
                        .reason
                        .as_deref()
                        .unwrap_or("structured eligibility check failed")
                )));
            }
            if final_check && evaluation.agent.is_none() {
                return Err(conflict(format!(
                    "selected {label} {selected_id} disappeared during bootstrap"
                )));
            }
        }
        if let Some(project_agent_id) = metadata.selected_project_agent_id.as_deref() {
            let agent = evaluations
                .iter()
                .find(|evaluation| evaluation.candidate.identity_id == project_agent_id)
                .and_then(|evaluation| evaluation.agent.as_ref())
                .ok_or_else(|| conflict("selected Project Agent disappeared"))?;
            let profile = AgentProfileRepo::get_profile(&*self.db, &agent.profile_id)
                .await?
                .ok_or_else(|| conflict("selected Project Agent profile disappeared"))?;
            if !policy_allows_project_chat(&profile.tool_policy_json) {
                return Err(conflict(
                    "selected Project Agent identity is not permitted for Project Chat scope",
                ));
            }
        }
        if let Some(worker_id) = metadata.selected_worker_agent_id.as_deref() {
            let agent = evaluations
                .iter()
                .find(|evaluation| evaluation.candidate.identity_id == worker_id)
                .and_then(|evaluation| evaluation.agent.as_ref())
                .ok_or_else(|| conflict("selected Task Worker disappeared"))?;
            let profile = AgentProfileRepo::get_profile(&*self.db, &agent.profile_id)
                .await?
                .ok_or_else(|| conflict("selected Task Worker profile disappeared"))?;
            if !policy_allows_task_write(&profile.tool_policy_json) {
                return Err(conflict(
                    "selected Task Worker identity is not permitted for Task scope",
                ));
            }
        }
        Ok(())
    }
}

fn policy_allows_task_write(tool_policy_json: &str) -> bool {
    policy_has_permission(tool_policy_json, "task_write")
}

fn policy_allows_project_chat(tool_policy_json: &str) -> bool {
    policy_has_permission(tool_policy_json, "read_agent_chat")
        && policy_has_permission(tool_policy_json, "propose_message")
}

fn policy_has_permission(tool_policy_json: &str, expected: &str) -> bool {
    operation_registry::authority::permission_set(tool_policy_json).contains(expected)
}

fn availability_reason(availability: SoloAgentAvailability) -> &'static str {
    match availability {
        SoloAgentAvailability::Authenticated => "authenticated",
        SoloAgentAvailability::Installed => "cli_not_authenticated",
        SoloAgentAvailability::NotFound => "cli_not_found",
        SoloAgentAvailability::Unauthenticated => "cli_not_authenticated",
        SoloAgentAvailability::Disabled => "cli_source_disabled",
        SoloAgentAvailability::Unknown => "cli_availability_unknown",
    }
}

fn availability_next_step(availability: SoloAgentAvailability) -> &'static str {
    match availability {
        SoloAgentAvailability::Authenticated => "",
        SoloAgentAvailability::Installed | SoloAgentAvailability::Unauthenticated => {
            "log in to the CLI and retry discovery"
        }
        SoloAgentAvailability::NotFound => "install the supported CLI and retry discovery",
        SoloAgentAvailability::Disabled => "enable the local CLI source and retry discovery",
        SoloAgentAvailability::Unknown => "refresh local Agent discovery",
    }
}

fn status_next_step(status: &EffectiveStatus) -> &'static str {
    match status {
        EffectiveStatus::DaemonUnavailable | EffectiveStatus::DaemonOffline => {
            "start the local Forge daemon and retry discovery"
        }
        EffectiveStatus::SourceDisabled | EffectiveStatus::ConnectionUnavailable => {
            "authenticate or enable the local CLI source and retry discovery"
        }
        EffectiveStatus::ConnectionDegraded => "repair the local CLI connection and retry",
        EffectiveStatus::Paused => "unpause the Agent and retry discovery",
        EffectiveStatus::Deactivated => "refresh the local CLI harness configuration",
        EffectiveStatus::Active | EffectiveStatus::Busy => "",
        EffectiveStatus::DaemonUpgradeRequired => {
            "upgrade the local Forge daemon and retry discovery"
        }
        EffectiveStatus::Error => "repair the Agent error and retry discovery",
    }
}

fn agent_registration_key(agent: &Agent) -> Option<String> {
    serde_json::from_str::<Value>(&agent.config_json)
        .ok()
        .and_then(|value| {
            value
                .get(SOLO_AGENT_REGISTRATION_KEY)
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
}

/// Resolve a config-declared provider's API key.  An inline key wins over the
/// environment reference; both being set was already rejected by config
/// validation, and neither being set is a config error surfaced at load.
fn resolve_provider_secret(
    provider_name: &str,
    declaration: &config::ProviderDeclaration,
) -> std::result::Result<forge_agent_host::Secret, String> {
    if let Some(api_key) = declaration
        .api_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(forge_agent_host::Secret::new(api_key.to_owned()));
    }
    let env_name = declaration
        .api_key_env
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "provider declares neither api_key nor api_key_env".to_owned())?;
    std::env::var(env_name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| forge_agent_host::Secret::new(value.trim().to_owned()))
        .ok_or_else(|| format!("environment variable {env_name} is not set"))
        .map_err(|reason| format!("provider `{provider_name}`: {reason}"))
}
