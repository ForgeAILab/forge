//! Runtime and server context manifest persistence for chat turns.

use super::*;

impl FederatedAgentChatTurnRunner {
    /// Persist only the final runtime manifest's redaction-safe linkage. The
    /// runtime remains the owner of context ordering and bodies; Forge stores
    /// identifiers, revisions, counts and fingerprints and links the result
    /// to the canonical Agent Chat session before the response is admitted.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn persist_runtime_context_manifest(
        &self,
        job: &AgentChatTurnJob,
        agent: &Agent,
        profile: &AgentProfile,
        session: &AgentSession,
        model: Option<&str>,
        runtime_manifest: &RuntimeContextManifestLink,
        operating_context_sources: &[ContextSourceInput],
    ) -> Result<String> {
        let identity_id = uuid::Uuid::parse_str(&agent.id)
            .map_err(|_| ServiceError::invalid_operation("Agent identity id is invalid"))?;
        let context_scope_id = uuid::Uuid::parse_str(&session.context_scope_id).map_err(|_| {
            ServiceError::invalid_operation("Agent Chat context scope id is invalid")
        })?;
        let manifest_id = agent_chat_manifest_id(&agent.id, &session.id, runtime_manifest);
        let request_fingerprint = agent_chat_request_fingerprint(
            job,
            profile,
            session,
            model,
            runtime_manifest,
            operating_context_sources,
        )?;
        let runtime_sources = runtime_manifest_sources(runtime_manifest)?;
        let mut sources = operating_context_sources.to_vec();
        let source_offset = i64::try_from(sources.len()).map_err(|_| {
            ServiceError::invalid_operation(
                "Agent Chat context source count overflows the persisted range",
            )
        })?;
        for (offset, mut source) in runtime_sources.into_iter().enumerate() {
            let offset = i64::try_from(offset).map_err(|_| {
                ServiceError::invalid_operation(
                    "Agent Chat runtime context source count overflows the persisted range",
                )
            })?;
            source.ordinal = source_offset.checked_add(offset).ok_or_else(|| {
                ServiceError::invalid_operation(
                    "Agent Chat context source ordinal overflows the persisted range",
                )
            })?;
            sources.push(source);
        }
        let service = ContextManifestService::new(Arc::clone(&self.db));

        if let Some(existing) = service
            .get_authorized(manifest_id, identity_id, context_scope_id)
            .await?
        {
            if existing.runtime_manifest_fingerprint.as_deref()
                != Some(runtime_manifest.runtime_manifest_fingerprint.as_str())
                || existing.request_fingerprint != request_fingerprint
            {
                return Err(ServiceError::invalid_operation(
                    "Agent Chat runtime context manifest idempotency conflict",
                ));
            }
            let existing_sources = service
                .sources(manifest_id, identity_id, context_scope_id)
                .await?;
            for source in &sources {
                if existing_sources.iter().any(|stored| {
                    stored.ordinal == source.ordinal
                        && stored.source_id == source.source_id
                        && stored.source_revision == source.source_revision
                }) {
                    continue;
                }
                service
                    .append_source(manifest_id, identity_id, context_scope_id, source.clone())
                    .await?;
            }
            return Ok(manifest_id.to_string());
        }

        let created = service
            .create(
                ContextManifestInput {
                    id: manifest_id,
                    identity_id,
                    agent_session_id: Some(uuid::Uuid::parse_str(&session.id).map_err(|_| {
                        ServiceError::invalid_operation("Agent Chat session id is invalid")
                    })?),
                    context_scope_id,
                    scope_type: "agent_chat".to_owned(),
                    scope_id: job.chat_id.clone(),
                    policy_revision: "forge-agent-chat-context-policy-1".to_owned(),
                    domain_revision: "forge-agent-chat-runtime-link-1".to_owned(),
                    lcm_binding_revision: runtime_manifest.lcm_binding_revision.clone(),
                    runtime_manifest_id: Some(runtime_manifest.turn_id.clone()),
                    runtime_manifest_fingerprint: Some(
                        runtime_manifest.runtime_manifest_fingerprint.clone(),
                    ),
                    request_fingerprint,
                },
                &sources,
            )
            .await?;
        let created_id = uuid::Uuid::parse_str(&created.id).map_err(|_| {
            ServiceError::invalid_operation("persisted context manifest id is invalid")
        })?;
        for source in sources {
            service
                .append_source(created_id, identity_id, context_scope_id, source)
                .await?;
        }
        Ok(created.id)
    }

    /// Persist the server-owned context even when the selected backend does
    /// not expose an Agent Runtime manifest (for example, the CLI backend).
    ///
    /// Main/Project operating sources are already bounded, redaction-safe
    /// references produced by `load_turn`.  They must not disappear merely
    /// because the provider adapter has no runtime manifest to attach.  The
    /// durable turn job is the idempotency boundary here: a retry of the same
    /// job reuses the same immutable manifest, while a changed source/profile
    /// set is rejected as an idempotency conflict.
    pub(super) async fn persist_server_context_manifest(
        &self,
        job: &AgentChatTurnJob,
        agent: &Agent,
        profile: &AgentProfile,
        session: &AgentSession,
        model: Option<&str>,
        operating_context_sources: &[ContextSourceInput],
    ) -> Result<String> {
        let identity_id = uuid::Uuid::parse_str(&agent.id)
            .map_err(|_| ServiceError::invalid_operation("Agent identity id is invalid"))?;
        let session_id = uuid::Uuid::parse_str(&session.id)
            .map_err(|_| ServiceError::invalid_operation("Agent Chat session id is invalid"))?;
        let context_scope_id = uuid::Uuid::parse_str(&session.context_scope_id).map_err(|_| {
            ServiceError::invalid_operation("Agent Chat context scope id is invalid")
        })?;
        let manifest_id = agent_chat_server_manifest_id(&agent.id, &session.id, &job.id);
        let request_fingerprint = agent_chat_server_request_fingerprint(
            job,
            profile,
            session,
            model,
            operating_context_sources,
        )?;
        let service = ContextManifestService::new(Arc::clone(&self.db));

        if let Some(existing) = service
            .get_authorized(manifest_id, identity_id, context_scope_id)
            .await?
        {
            if existing.request_fingerprint != request_fingerprint {
                return Err(ServiceError::invalid_operation(
                    "Agent Chat server context manifest idempotency conflict",
                ));
            }
            let existing_sources = service
                .sources(manifest_id, identity_id, context_scope_id)
                .await?;
            for source in operating_context_sources {
                if existing_sources.iter().any(|stored| {
                    stored.ordinal == source.ordinal
                        && stored.source_id == source.source_id
                        && stored.source_revision == source.source_revision
                }) {
                    continue;
                }
                service
                    .append_source(manifest_id, identity_id, context_scope_id, source.clone())
                    .await?;
            }
            return Ok(manifest_id.to_string());
        }

        let created = service
            .create(
                ContextManifestInput {
                    id: manifest_id,
                    identity_id,
                    agent_session_id: Some(session_id),
                    context_scope_id,
                    scope_type: "agent_chat".to_owned(),
                    scope_id: job.chat_id.clone(),
                    policy_revision: "forge-agent-chat-context-policy-1".to_owned(),
                    domain_revision: "forge-agent-chat-server-context-1".to_owned(),
                    lcm_binding_revision: None,
                    runtime_manifest_id: None,
                    runtime_manifest_fingerprint: None,
                    request_fingerprint,
                },
                operating_context_sources,
            )
            .await?;
        let created_id = uuid::Uuid::parse_str(&created.id).map_err(|_| {
            ServiceError::invalid_operation("persisted context manifest id is invalid")
        })?;
        for source in operating_context_sources {
            service
                .append_source(created_id, identity_id, context_scope_id, source.clone())
                .await?;
        }
        Ok(created.id)
    }
}
