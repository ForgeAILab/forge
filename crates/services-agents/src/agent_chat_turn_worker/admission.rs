//! Turn loading, frozen authority validation, prompt preview, and the Agent Chat turn runner implementation.

use super::*;

impl FederatedAgentChatTurnRunner {
    /// Validate and load the authority snapshot captured at turn admission.
    ///
    /// V088 deliberately leaves the provenance columns nullable for rows
    /// created before the cutover.  Such rows take the legacy path and are
    /// still subject to the existing conservative authority checks.  A row
    /// created after the cutover must be complete; falling back to an active
    /// binding, the selected Profile, or the current skill would make a
    /// queued turn change meaning while it was waiting.
    pub(super) async fn load_frozen_authority(
        &self,
        chat: &db::AgentChat,
        job: &AgentChatTurnJob,
        agent: &Agent,
        profile: &AgentProfile,
    ) -> Result<Option<FrozenTurnAuthority>> {
        let provenance_fields = [
            job.responder_binding_id.is_some(),
            job.responder_binding_version.is_some(),
            job.responder_identity_version.is_some(),
            job.profile_version.is_some(),
            job.operating_skill_revision_id.is_some(),
            job.policy_revision.is_some(),
            job.policy_digest.is_some(),
            job.permission_policy_digest.is_some(),
            job.tool_policy_digest.is_some(),
            job.admission_digest.is_some(),
            job.canonical_scope_provenance_json.is_some(),
        ];
        if provenance_fields.iter().all(|present| !present) {
            return Ok(None);
        }
        if provenance_fields.iter().any(|present| !present) {
            return Err(ServiceError::invalid_operation(
                "Agent Chat turn has incomplete admission provenance",
            ));
        }
        if job.canonical_scope_type != "agent_chat" || job.canonical_scope_id != job.chat_id {
            return Err(ServiceError::invalid_operation(
                "Agent Chat turn has an invalid frozen canonical scope",
            ));
        }
        let binding_id = job.responder_binding_id.clone().ok_or_else(|| {
            ServiceError::invalid_operation("Agent Chat turn has no frozen responder binding")
        })?;
        let binding_version = job.responder_binding_version.ok_or_else(|| {
            ServiceError::invalid_operation("Agent Chat turn has no frozen binding version")
        })?;
        let profile_version = job.profile_version.ok_or_else(|| {
            ServiceError::invalid_operation("Agent Chat turn has no frozen Profile version")
        })?;
        let identity_version = job.responder_identity_version.ok_or_else(|| {
            ServiceError::invalid_operation("Agent Chat turn has no frozen identity version")
        })?;
        if profile.version != profile_version {
            return Err(ServiceError::invalid_operation(
                "Agent Chat turn Profile revision is no longer available",
            ));
        }
        if job.profile_id.as_deref() != Some(profile.id.as_str())
            || job.responder_identity_id.as_deref() != Some(agent.id.as_str())
        {
            return Err(ServiceError::invalid_operation(
                "Agent Chat turn frozen responder does not match its Profile",
            ));
        }

        // The provenance JSON is redaction-safe, but it is still checked as a
        // second representation of the admission receipt.  This catches a
        // partially migrated row or an accidental column mix before any
        // session/backend work occurs.
        let provenance =
            serde_json::from_str::<crate::agent_turn_admission::ResolvedAgentResponder>(
                job.canonical_scope_provenance_json
                    .as_deref()
                    .ok_or_else(|| {
                        ServiceError::invalid_operation(
                            "Agent Chat turn has no canonical scope provenance",
                        )
                    })?,
            )
            .map_err(|_| {
                ServiceError::invalid_operation("Agent Chat turn canonical provenance is invalid")
            })?;
        provenance.require_frozen()?;
        if provenance.chat_id != chat.id
            || provenance.canonical_scope_type != job.canonical_scope_type
            || provenance.canonical_scope_id != job.canonical_scope_id
            || provenance.binding_id.as_deref() != Some(binding_id.as_str())
            || provenance.binding_version != Some(binding_version)
            || provenance.identity_id.as_deref() != Some(agent.id.as_str())
            || provenance.identity_version != Some(identity_version)
            || provenance.profile_id.as_deref() != Some(profile.id.as_str())
            || provenance.profile_version != Some(profile_version)
            || provenance.operating_skill_revision.as_deref()
                != job.operating_skill_revision_id.as_deref()
            || provenance.policy_revision.as_deref() != job.policy_revision.as_deref()
            || provenance.policy_digest.as_deref() != job.policy_digest.as_deref()
            || provenance.permission_policy_digest.as_deref()
                != job.permission_policy_digest.as_deref()
            || provenance.tool_policy_digest.as_deref() != job.tool_policy_digest.as_deref()
        {
            return Err(ServiceError::invalid_operation(
                "Agent Chat turn frozen provenance representations disagree",
            ));
        }

        let (
            binding_identity_id,
            policy_json,
            permission_json,
            policy_revision,
            stored_policy_digest,
            binding_state,
            binding_row_version,
            expected_skill_key,
        ) = match chat.kind.as_str() {
            "account_main" => {
                if chat.project_id.is_some() {
                    return Err(ServiceError::invalid_operation(
                        "Main Agent Chat unexpectedly has a Project scope",
                    ));
                }
                let account_id = chat.account_id.as_deref().ok_or_else(|| {
                    ServiceError::invalid_operation("Main Agent Chat has no account scope")
                })?;
                let binding = AccountMainAgentBindingRepo::get_main_binding(&*self.db, &binding_id)
                    .await?
                    .filter(|binding| binding.account_id == account_id)
                    .ok_or_else(|| {
                        ServiceError::invalid_operation(
                            "Agent Chat frozen Main binding history is unavailable",
                        )
                    })?;
                (
                    binding.identity_id,
                    binding.autonomy_policy_json,
                    // Main account ceilings are mutable command-time policy.
                    // The admission digest is retained below for provenance,
                    // but it must not turn a later tightening into a reason
                    // to reject an otherwise frozen model turn.
                    String::new(),
                    binding.tool_policy_revision,
                    None,
                    binding.state,
                    binding.version,
                    main_skill_key_for_frozen_revision(
                        job.operating_skill_revision_id
                            .as_deref()
                            .unwrap_or_default(),
                    ),
                )
            }
            "project" => {
                if chat.account_id.is_some() {
                    return Err(ServiceError::invalid_operation(
                        "Project Agent Chat unexpectedly has an account scope",
                    ));
                }
                let project_id = chat.project_id.as_deref().ok_or_else(|| {
                    ServiceError::invalid_operation("Project Agent Chat has no Project scope")
                })?;
                let row = self
                    .db
                    .frozen_project_turn_binding(&binding_id, project_id)
                    .await?
                    .ok_or_else(|| {
                        ServiceError::invalid_operation(
                            "Agent Chat frozen Project binding history is unavailable",
                        )
                    })?;
                let identity_id: Option<String> = row.identity_id()?;
                let binding_row_version: i64 = row.version()?;
                let autonomy_policy_json: String = row.autonomy_policy_json()?;
                let permission_json: String = row.permission_ceiling_json()?;
                let policy_revision: String = row.policy_revision()?;
                let stored_policy_digest: String = row.policy_digest()?;
                (
                    identity_id.ok_or_else(|| {
                        ServiceError::invalid_operation(
                            "Agent Chat frozen Project binding has no identity",
                        )
                    })?,
                    autonomy_policy_json,
                    permission_json,
                    policy_revision,
                    Some(stored_policy_digest),
                    row.state()?,
                    binding_row_version,
                    PROJECT_OPERATING_SKILL_KEY,
                )
            }
            _ => {
                return Err(ServiceError::invalid_operation(
                    "Agent Chat has an unsupported canonical kind",
                ));
            }
        };

        let historical_binding_version_matches = binding_row_version == binding_version
            || (binding_state == "replaced" && binding_row_version == binding_version + 1);
        if binding_identity_id != agent.id || !historical_binding_version_matches {
            return Err(ServiceError::invalid_operation(
                "Agent Chat frozen binding does not match its responder",
            ));
        }
        // A replaced binding is valid history for its already-admitted job.
        // A paused/revoked/setup row is an explicit authority withdrawal and
        // must fail closed without selecting a newer active binding.
        if matches!(
            binding_state.as_str(),
            "paused" | "revoked" | "agent_setup_required"
        ) {
            return Err(ServiceError::invalid_operation(
                "Agent Chat frozen responder binding is no longer executable",
            ));
        }
        if let Some(expected) = job.policy_revision.as_deref() {
            if expected != policy_revision {
                return Err(ServiceError::invalid_operation(
                    "Agent Chat frozen policy revision is no longer available",
                ));
            }
        }
        let computed_policy_digest = crate::agent_turn_admission::policy_digest(&policy_json)?;
        let expected_policy_digest = job.policy_digest.as_deref().ok_or_else(|| {
            ServiceError::invalid_operation("Agent Chat turn has no frozen policy digest")
        })?;
        if chat.kind == "account_main" {
            if expected_policy_digest != computed_policy_digest {
                return Err(ServiceError::invalid_operation(
                    "Agent Chat frozen Main policy changed after admission",
                ));
            }
        } else if stored_policy_digest.as_deref() != Some(expected_policy_digest) {
            return Err(ServiceError::invalid_operation(
                "Agent Chat frozen Project policy changed after admission",
            ));
        }
        let expected_permission_policy_digest =
            job.permission_policy_digest.as_deref().ok_or_else(|| {
                ServiceError::invalid_operation("Agent Chat turn has no frozen permission digest")
            })?;
        if expected_permission_policy_digest.trim().is_empty() {
            return Err(ServiceError::invalid_operation(
                "Agent Chat turn has an empty frozen permission digest",
            ));
        }
        if chat.kind == "project"
            && crate::agent_turn_admission::policy_digest(&permission_json)?
                != expected_permission_policy_digest
        {
            return Err(ServiceError::invalid_operation(
                "Agent Chat frozen permission policy changed after admission",
            ));
        }
        let permission_policy_digest = expected_permission_policy_digest.to_owned();
        let tool_policy_digest =
            crate::agent_turn_admission::policy_digest(&profile.tool_policy_json)?;
        if job.tool_policy_digest.as_deref() != Some(tool_policy_digest.as_str()) {
            return Err(ServiceError::invalid_operation(
                "Agent Chat frozen Profile policy changed after admission",
            ));
        }

        let operating_skill_revision_id =
            job.operating_skill_revision_id.clone().ok_or_else(|| {
                ServiceError::invalid_operation("Agent Chat turn has no operating-skill provenance")
            })?;
        if expected_skill_key == MAIN_BASELINE_OPERATING_SKILL_KEY {
            let (operating_skill_body, operating_skill_content_digest) =
                canonical_main_baseline_operating_skill_body_for_revision(
                    &operating_skill_revision_id,
                )
                .ok_or_else(|| {
                    ServiceError::invalid_operation(
                        "Agent Chat frozen Main baseline skill does not match a supported Main context",
                    )
                })?;
            return Ok(Some(FrozenTurnAuthority {
                binding_id,
                binding_version,
                profile_version,
                operating_skill_revision_id,
                policy_revision,
                policy_digest: expected_policy_digest.to_owned(),
                permission_policy_digest,
                tool_policy_digest,
                operating_skill_key: MAIN_BASELINE_OPERATING_SKILL_KEY.to_owned(),
                operating_skill_body: operating_skill_body.to_owned(),
                operating_skill_content_digest: operating_skill_content_digest.to_owned(),
            }));
        }
        let skill_row = self
            .db
            .frozen_turn_operating_skill(&operating_skill_revision_id)
            .await?
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "Agent Chat frozen operating-skill revision is no longer available",
                )
            })?;
        let skill_id: String = skill_row.id()?;
        let skill_key: String = skill_row.skill_key()?;
        let skill_body: String = skill_row.canonical_body()?;
        let skill_content_digest: String = skill_row.content_digest()?;
        if skill_id != operating_skill_revision_id
            || skill_key != expected_skill_key
            || skill_body.trim().is_empty()
            || skill_content_digest.trim().is_empty()
        {
            return Err(ServiceError::invalid_operation(
                "Agent Chat frozen operating-skill revision is invalid",
            ));
        }

        Ok(Some(FrozenTurnAuthority {
            binding_id,
            binding_version,
            profile_version,
            operating_skill_revision_id,
            policy_revision,
            policy_digest: expected_policy_digest.to_owned(),
            permission_policy_digest,
            tool_policy_digest,
            operating_skill_key: skill_key,
            operating_skill_body: skill_body,
            operating_skill_content_digest: skill_content_digest,
        }))
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn preview_prompt(&self, job: &AgentChatTurnJob) -> Result<AgentChatPromptPreview> {
        let mut turn = self.load_turn_context(job).await?;
        self.append_session_denials(
            &mut turn.state_card,
            &turn.agent.id,
            &turn.profile.id,
            &job.chat_id,
            None,
        )
        .await?;
        Ok(AgentChatPromptPreview {
            system_prompt: compose_system_prompt(
                turn.profile.prompt_template.as_deref(),
                turn.operating_instruction.as_deref(),
            ),
            history: turn
                .history
                .iter()
                .map(|m| AgentChatPromptMessage {
                    role: match m.author_type {
                        AgentChatMessageAuthorType::Agent => "assistant",
                        AgentChatMessageAuthorType::System => "system",
                        AgentChatMessageAuthorType::User | AgentChatMessageAuthorType::Handoff => {
                            "user"
                        }
                    }
                    .to_owned(),
                    content: m.content.clone(),
                })
                .collect(),
            input_parts: vec![turn.input.content.clone(), turn.state_card.clone()],
            cli_prompt: (turn.profile.backend_kind == "cli").then(|| {
                build_cli_prompt(
                    turn.profile.prompt_template.as_deref(),
                    turn.operating_instruction.as_deref(),
                    &turn.history,
                    &turn.input.content,
                    Some(&turn.state_card),
                )
            }),
        })
    }

    /// Persist the real loader's sources through the production recorder,
    /// without executing a provider. Only available to test-support builds.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn record_prompt_manifest_for_test(&self, job: &AgentChatTurnJob) -> Result<String> {
        let turn = self.load_turn(job).await?;
        self.persist_server_context_manifest(
            job,
            &turn.agent,
            &turn.profile,
            &turn.session,
            turn.profile.model.as_deref(),
            &turn.operating_context_sources,
        )
        .await
    }

    pub(super) async fn load_turn(&self, job: &AgentChatTurnJob) -> Result<LoadedAgentChatTurn> {
        let LoadedAgentChatContext {
            agent,
            profile,
            input,
            history,
            operating_instruction,
            mut state_card,
            operating_context_sources,
            frozen_authority,
        } = self.load_turn_context(job).await?;
        let owner_user_id = agent
            .owner_id
            .clone()
            .ok_or_else(|| ServiceError::invalid_operation("Agent identity has no owner"))?;
        let scope = RequestedCanonicalScope::AgentChat {
            chat_id: job.chat_id.clone(),
        };
        let session = if let Some(frozen) = frozen_authority.as_ref() {
            self.embedded_agents
                .create_or_resume_frozen_chat_session(CreateFrozenAgentChatSession {
                    actor_user_id: owner_user_id,
                    identity_id: agent.id.clone(),
                    profile_id: profile.id.clone(),
                    profile_version: frozen.profile_version,
                    binding_id: frozen.binding_id.clone(),
                    binding_version: frozen.binding_version,
                    policy_revision: frozen.policy_revision.clone(),
                    policy_digest: frozen.policy_digest.clone(),
                    permission_policy_digest: frozen.permission_policy_digest.clone(),
                    tool_policy_digest: frozen.tool_policy_digest.clone(),
                    scope,
                })
                .await?
        } else {
            self.embedded_agents
                .create_or_resume_session(CreateScopedSession {
                    actor_user_id: owner_user_id,
                    identity_id: agent.id.clone(),
                    profile_id: Some(profile.id.clone()),
                    scope,
                })
                .await?
        };
        self.append_session_denials(
            &mut state_card,
            &agent.id,
            &profile.id,
            &job.chat_id,
            Some(&session.id),
        )
        .await?;
        Ok(LoadedAgentChatTurn {
            agent,
            profile,
            session,
            input,
            history,
            operating_instruction,
            state_card,
            operating_context_sources,
        })
    }

    async fn append_session_denials(
        &self,
        state_card: &mut String,
        identity_id: &str,
        profile_id: &str,
        chat_id: &str,
        session_id: Option<&str>,
    ) -> Result<()> {
        let unavailable = self
            .embedded_agents
            .chat_session_denials(identity_id, profile_id, chat_id, session_id)
            .await;
        if !unavailable.is_empty() {
            state_card.push_str("\n### Unavailable in this session\n");
            for (operation, cause) in unavailable {
                state_card.push_str(&format!("- {operation} ({cause})\n"));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl AgentChatTurnRunner for FederatedAgentChatTurnRunner {
    async fn rotate_pending_topic(
        &self,
        chat_id: &str,
        now: chrono::DateTime<Utc>,
    ) -> Result<bool> {
        crate::TopicRotator::rotate_pending_at(
            &crate::TopicRotationCoordinator::new(self.db.clone(), self.embedded_agents.clone()),
            chat_id,
            now,
        )
        .await
    }

    async fn validate_admission_authority(&self, job: &AgentChatTurnJob) -> Result<()> {
        if job.canonical_scope_type != "agent_chat" || job.canonical_scope_id != job.chat_id {
            return Err(ServiceError::invalid_operation(
                "Agent Chat turn has a mismatched canonical scope",
            ));
        }
        let chat = AgentChatRepo::get_agent_chat(&*self.db, &job.chat_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_chat", job.chat_id.clone()))?;
        let identity_id = job
            .responder_identity_id
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("Agent Chat job has no responder"))?;
        let agent = AgentRepo::get_by_id(&*self.db, identity_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_identity", identity_id.to_owned()))?;
        let profile_id = job
            .profile_id
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("Agent Chat job has no profile"))?;
        let profile = AgentProfileRepo::get_profile(&*self.db, profile_id)
            .await?
            .filter(|profile| profile.identity_id == agent.id)
            .ok_or_else(|| ServiceError::not_found("agent_profile", profile_id.to_owned()))?;
        self.load_frozen_authority(&chat, job, &agent, &profile)
            .await?;
        Ok(())
    }

    async fn validate_provider_availability(&self, job: &AgentChatTurnJob) -> Result<()> {
        let identity_id = job
            .responder_identity_id
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("Agent Chat job has no responder"))?;
        let agent = AgentRepo::get_by_id(&*self.db, identity_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_identity", identity_id.to_owned()))?;
        let profile_id = job
            .profile_id
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("Agent Chat job has no profile"))?;
        let profile = AgentProfileRepo::get_profile(&*self.db, profile_id)
            .await?
            .filter(|profile| profile.identity_id == agent.id)
            .ok_or_else(|| ServiceError::not_found("agent_profile", profile_id.to_owned()))?;
        let owner_user_id = agent.owner_id.as_deref().ok_or_else(|| {
            ServiceError::invalid_operation("Agent identity has no account owner")
        })?;
        crate::chat_usage::validate_provider_availability(&self.db, &profile, owner_user_id).await
    }

    async fn run_turn(
        &self,
        job: &AgentChatTurnJob,
        cancellation: CancellationToken,
    ) -> Result<CompletedAgentChatTurn> {
        self.run_typed_turn(job, cancellation)
            .await
            .map_err(|error| error.error)
    }

    async fn run_turn_with_usage(
        &self,
        job: &AgentChatTurnJob,
        cancellation: CancellationToken,
    ) -> AgentChatTurnRunOutcome {
        let result = self.run_typed_turn(job, cancellation).await;
        let usage_reports = self.take_usage(&job.id).await;
        match result {
            Ok(turn) => AgentChatTurnRunOutcome::Completed {
                turn,
                usage_reports,
            },
            Err(error) => AgentChatTurnRunOutcome::Failed {
                failure: error.failure,
                error: error.error,
                usage_reports,
            },
        }
    }
}
