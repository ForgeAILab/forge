//! Topic rotation summaries, durable fork completion, and aborts.

use super::*;

type SealedTopicSeed = (Option<Vec<u8>>, Option<Vec<u8>>);

/// Bound on the provider topic-summary call made during a rotation.
pub const TOPIC_SUMMARY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const TOPIC_SUMMARY_EMPTY: &str = "No earlier conversation.";
const TOPIC_SUMMARY_FALLBACK: &str =
    "Earlier conversation continues from the previous topic; consult current state before acting.";

/// The deterministic seed used when the provider summary fails or times out.
async fn deterministic_topic_seed(request: &agent_runtime::lcm::LcmSummaryModelRequest) -> String {
    use agent_runtime::lcm::LcmSummaryModel;
    crate::DeterministicLcmSummaryModel::default()
        .summarize(request)
        .await
        .map(|response| response.text)
        .unwrap_or_else(|_| TOPIC_SUMMARY_FALLBACK.to_owned())
}

/// The usage-outbox row id for one rotation intent's summary call.
#[must_use]
pub fn topic_summary_usage_id(intent_id: &str) -> String {
    format!("topic-summary:{intent_id}")
}

impl NativeAgentRuntimeBackend {
    /// Complete the durable intent using its pre-reserved successor identity.
    pub async fn fork_topic(
        &self,
        request: AgentTurnRequest,
        intent_id: &str,
        successor: &str,
    ) -> Result<(), AgentHostError> {
        executors::server_commands::ensure_agents_allowed()
            .map_err(|error| AgentHostError::Authority(error.to_string()))?;
        use agent_runtime::lcm::LcmSummaryModel;
        let _conversion_admission = self
            .protected_store
            .foreground_session(&SessionId::new(&request.runtime_session_id))
            .await?;
        let db = self.protected_store.database();
        if self
            .active
            .lock()
            .map_err(|_| AgentHostError::ProtectedPersistence)?
            .contains_key(&request.runtime_session_id)
        {
            return Err(AgentHostError::VersionConflict);
        }
        // A fork plans no provider request, so the cold-start working set
        // never applies to it.
        let prepared = self
            .prepare_runtime(&request, Some(successor), false)
            .await?;
        let saved_seed: Option<SealedTopicSeed> = sqlx::query_as(
            "SELECT summary_ciphertext, summary_nonce FROM agent_chat_topic_rotation WHERE id = ?",
        )
        .bind(intent_id)
        .fetch_optional(db.pool())
        .await
        .map_err(|_| AgentHostError::ProtectedPersistence)?;
        let seed = if let Some((Some(bytes), Some(nonce))) = saved_seed {
            String::from_utf8(
                self.protected_store
                    .open_protected(&bytes, &nonce)
                    .map_err(host_runtime_error)?,
            )
            .map_err(|_| AgentHostError::ProtectedPersistence)?
        } else {
            let id = SessionId::new(&request.runtime_session_id);
            let saved = prepared.saved;
            let start = if saved {
                let start = StartSession::resume(id);
                if prepared.needs_adoption {
                    start.with_lcm_policy(agent_runtime::harness::LcmRecoveryPolicy::Adopt)
                } else {
                    start
                }
            } else {
                StartSession::create(id, request.history.clone())
            };
            let parent = prepared
                .runtime
                .start_session(start)
                .await
                .map_err(host_runtime_error)?;
            parent.persist().await.map_err(host_runtime_error)?;
            let messages = parent.history();
            let source = agent_runtime::lcm::Fingerprint::of(
                serde_json::to_vec(&messages).map_err(|_| AgentHostError::ProtectedPersistence)?,
            );
            let summary_request = agent_runtime::lcm::LcmSummaryModelRequest {
                purpose: "topic_summary".to_owned(),
                level: agent_runtime::lcm::EscalationLevel::PreserveDetails,
                target_tokens: 1024,
                source_range: agent_runtime::lcm::LcmRange::new(
                    agent_runtime::lcm::LcmSequence::new(0),
                    agent_runtime::lcm::LcmSequence::new(messages.len().saturating_sub(1) as u64),
                )
                .map_err(|e| AgentHostError::Configuration(e.to_string()))?,
                source_fingerprint: source,
                messages,
                operation_fingerprint: agent_runtime::lcm::LcmOperationFingerprint::new(
                    agent_runtime::lcm::Fingerprint::of(intent_id.as_bytes()),
                ),
                policy_revision: agent_runtime::registry::RegistryRevision::new(
                    "forge-topic-summary-1",
                ),
                sizer_revision: agent_runtime::context::RequestSizer::revision(
                    &agent_runtime::context::CharRatioSizer::default(),
                )
                .revision,
            };
            let model = self.summary_model(&request, prepared.summary_cap)?;
            // The provider summary is bounded and best-effort: a timeout or
            // a failure falls back to the deterministic seed, and never
            // fails the rotation on its own.
            let result = if summary_request.messages.is_empty() {
                None
            } else {
                Some(
                    tokio::time::timeout(TOPIC_SUMMARY_TIMEOUT, model.summarize(&summary_request))
                        .await,
                )
            };
            let (text, input, output, failed) = match result {
                Some(Ok(Ok(response))) => (
                    response.text,
                    response.input_tokens,
                    response.output_tokens,
                    false,
                ),
                Some(Ok(Err(error))) => {
                    tracing::warn!(intent_id, error = %error, "topic summary failed; using the deterministic seed");
                    let (input, output) = error.reported_usage().unwrap_or((0, 0));
                    (
                        deterministic_topic_seed(&summary_request).await,
                        input,
                        output,
                        true,
                    )
                }
                Some(Err(_elapsed)) => {
                    tracing::warn!(
                        intent_id,
                        timeout_secs = TOPIC_SUMMARY_TIMEOUT.as_secs(),
                        "topic summary timed out; using the deterministic seed"
                    );
                    (deterministic_topic_seed(&summary_request).await, 0, 0, true)
                }
                None => (TOPIC_SUMMARY_EMPTY.to_owned(), 0, 0, false),
            };
            let (ciphertext, nonce) = self
                .protected_store
                .seal_protected(text.as_bytes())
                .map_err(host_runtime_error)?;
            let mut tx = db::begin_immediate(db.pool())
                .await
                .map_err(|_| AgentHostError::ProtectedPersistence)?;
            sqlx::query("UPDATE agent_chat_topic_rotation SET summary_ciphertext = ?, summary_nonce = ? WHERE id = ? AND summary_ciphertext IS NULL")
                .bind(ciphertext).bind(nonce).bind(intent_id).execute(&mut *tx).await.map_err(|_| AgentHostError::ProtectedPersistence)?;
            if input != 0 || output != 0 {
                // Usage outbox, written with the sealed seed. The caller
                // settles it into the usage ledger once, keyed by this id.
                sqlx::query(
                    "INSERT OR IGNORE INTO agent_topic_summary_usage
                         (id, runtime_session_id, provider, model, input_tokens,
                          output_tokens, failed, purpose, chat_id)
                     VALUES (?, ?, ?, ?, ?, ?, ?, 'topic_summary', ?)",
                )
                .bind(topic_summary_usage_id(intent_id))
                .bind(successor)
                .bind(&request.provider.provider)
                .bind(&request.provider.model)
                .bind(i64::try_from(input).unwrap_or(i64::MAX))
                .bind(i64::try_from(output).unwrap_or(i64::MAX))
                .bind(failed)
                .bind(&request.scope.scope_id)
                .execute(&mut *tx)
                .await
                .map_err(|_| AgentHostError::ProtectedPersistence)?;
            }
            tx.commit()
                .await
                .map_err(|_| AgentHostError::ProtectedPersistence)?;
            text
        };
        let child = prepared
            .runtime
            .fork_session(agent_runtime::runtime::ForkSession {
                from: SessionId::new(&request.runtime_session_id),
                new_id: SessionId::new(successor),
                seed: agent_runtime::runtime::ForkSeed::Summary(seed),
                lcm: agent_runtime::runtime::ForkLcm::NewTimeline,
            })
            .await
            .map_err(host_runtime_error)?;
        child.persist().await.map_err(host_runtime_error)?;
        self.protected_store
            .journals()
            .post_terminal(&request.runtime_session_id)
            .await;
        self.protected_store
            .journals()
            .post_terminal(successor)
            .await;
        Ok(())
    }

    /// Clears a pending fork intent an abandoned topic rotation left on the
    /// source session, so the source keeps taking turns. A no-op when no
    /// fork is pending; refused by the runtime once the successor was saved.
    pub async fn abort_topic_fork(&self, request: AgentTurnRequest) -> Result<(), AgentHostError> {
        let _conversion_admission = self
            .protected_store
            .foreground_session(&SessionId::new(&request.runtime_session_id))
            .await?;
        let prepared = self.prepare_runtime(&request, None, false).await?;
        prepared
            .runtime
            .abort_fork(&SessionId::new(&request.runtime_session_id))
            .await
            .map_err(host_runtime_error)?;
        self.protected_store
            .journals()
            .post_terminal(&request.runtime_session_id)
            .await;
        Ok(())
    }

    pub(super) fn summary_model(
        &self,
        request: &AgentTurnRequest,
        summary_cap: u32,
    ) -> Result<agent_runtime::lcm::ProviderLcmSummaryModel<dyn Provider>, AgentHostError> {
        agent_runtime::lcm::ProviderLcmSummaryModel::new(
            Arc::new(LowReasoningProvider(
                self.provider(request)?,
                if request.provider.provider == "gemini" {
                    "minimal"
                } else {
                    "low"
                },
            )) as Arc<dyn Provider>,
            ResolvedModelProfile::explicit(
                request.provider.provider.clone(),
                ModelId::new(&request.provider.model),
                ModelLimits::new(
                    request.provider.context_tokens,
                    summary_cap,
                    request.provider.max_output_tokens,
                ),
            ),
            crate::lcm::SUMMARY_INSTRUCTIONS,
            Arc::new(agent_runtime::context::CharRatioSizer::default()),
        )
        .map_err(|e| AgentHostError::Configuration(e.to_string()))
    }
}

#[derive(Debug)]
struct LowReasoningProvider(Arc<dyn Provider>, &'static str);
#[async_trait]
impl Provider for LowReasoningProvider {
    fn describe(&self) -> Vec<agent_runtime::core::provider::ModelDescriptor> {
        self.0.describe()
    }
    fn capabilities(&self, model: &ModelId) -> Option<agent_runtime::core::provider::Capabilities> {
        self.0.capabilities(model)
    }
    async fn stream(
        &self,
        mut request: agent_runtime::core::provider::ProviderRequest,
        ctx: agent_runtime::core::provider::ProviderCallContext,
    ) -> Result<
        agent_runtime::core::provider::ProviderStream,
        agent_runtime::core::provider::ProviderError,
    > {
        request.reasoning = self.0.capabilities(&request.model).and_then(|caps| {
            (caps.reasoning == agent_runtime::core::provider::ReasoningSupport::Controllable).then(
                || ReasoningConfig {
                    effort: Some(self.1.to_owned()),
                    max_tokens: None,
                },
            )
        });
        self.0.stream(request, ctx).await
    }
}
