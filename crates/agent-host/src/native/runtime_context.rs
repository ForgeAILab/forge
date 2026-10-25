//! Topic read caching, retry input, state cards, and context policies.

use super::*;

/// Operations whose repeat reads, unchanged within one topic, return a
/// reference to the earlier full result instead of the body.
const UNCHANGED_READ_OPERATIONS: [&str; 2] = ["project.current_state", "project.charter"];

/// Unchanged-read references for one runtime session (= one topic).
///
/// A full result carries a unique `read_ref`; a later identical read returns
/// `{"unchanged_since_call": <read_ref>}` only while that full result is
/// provably in the model-visible history of this topic:
///
/// - results of the running turn are staged in memory and become durable,
///   with their canonical history index, only when the turn completes, so a
///   failed, cancelled or retried turn never leaves a reference behind (and
///   a failed turn also clears the session's durable references);
/// - a durable reference is honoured only while no LCM summary node covers
///   its history index (the LCM entry sequence is the history index);
/// - a staged reference is honoured only while no LCM node reaches into the
///   running turn;
/// - each topic is its own runtime session, and rotation deletes the
///   predecessor's references.
#[derive(Debug)]
pub(super) struct TopicReadFilter {
    pub(super) db: Arc<db::SqliteDb>,
    pub(super) runtime_session_id: String,
    pub(super) turn: Mutex<TopicReadTurn>,
}

#[derive(Debug, Default)]
pub(super) struct TopicReadTurn {
    /// Canonical history length when the running turn started.
    start_history_len: usize,
    /// operation -> (digest, read_ref) of full results this turn returned.
    staged: HashMap<String, (String, String)>,
}

impl TopicReadFilter {
    fn persistence_error() -> RuntimeError {
        RuntimeError::internal("topic read cache unavailable")
    }

    pub(super) fn begin_turn(&self, start_history_len: usize) {
        if let Ok(mut turn) = self.turn.lock() {
            *turn = TopicReadTurn {
                start_history_len,
                staged: HashMap::new(),
            };
        }
    }

    /// Highest canonical history index covered by an LCM summary node of
    /// this session's timeline, or -1.
    async fn compacted_through(&self) -> Result<i64, RuntimeError> {
        sqlx::query_scalar(
            "SELECT COALESCE(MAX(node.range_end), -1)
             FROM agent_lcm_node AS node
             JOIN agent_runtime_lcm_binding AS binding ON binding.timeline_id = node.timeline_id
             WHERE binding.runtime_session_id = ?",
        )
        .bind(&self.runtime_session_id)
        .fetch_one(self.db.pool())
        .await
        .map_err(|_| Self::persistence_error())
    }

    /// Makes this turn's references durable once the turn completed and its
    /// history is persisted. A reference whose result cannot be found in the
    /// history is dropped rather than trusted.
    pub(super) async fn commit(&self, history: &[Message]) -> Result<(), AgentHostError> {
        let staged = match self.turn.lock() {
            Ok(mut turn) => std::mem::take(&mut turn.staged),
            Err(_) => return Err(AgentHostError::ProtectedPersistence),
        };
        for (operation, (digest, reference)) in staged {
            let index = history.iter().rposition(|message| {
                message.role == Role::Tool
                    && serde_json::to_string(message)
                        .is_ok_and(|encoded| encoded.contains(&reference))
            });
            let query = match index {
                Some(index) => sqlx::query(
                    "INSERT INTO agent_topic_read_digest
                         (runtime_session_id, operation, digest, call_ref, history_index)
                     VALUES (?, ?, ?, ?, ?)
                     ON CONFLICT(runtime_session_id, operation) DO UPDATE SET
                         digest = excluded.digest, call_ref = excluded.call_ref,
                         history_index = excluded.history_index",
                )
                .bind(&self.runtime_session_id)
                .bind(&operation)
                .bind(digest)
                .bind(reference)
                .bind(i64::try_from(index).unwrap_or(i64::MAX)),
                None => sqlx::query(
                    "DELETE FROM agent_topic_read_digest
                     WHERE runtime_session_id = ? AND operation = ?",
                )
                .bind(&self.runtime_session_id)
                .bind(&operation),
            };
            query
                .execute(self.db.pool())
                .await
                .map_err(|_| AgentHostError::ProtectedPersistence)?;
        }
        Ok(())
    }
}

/// Clears a session's unchanged-read references at the start of its next
/// turn when the previous turn did not complete.
pub(super) async fn clear_topic_reads(
    db: &db::SqliteDb,
    runtime_session_id: &str,
) -> Result<(), AgentHostError> {
    sqlx::query("DELETE FROM agent_topic_read_digest WHERE runtime_session_id = ?")
        .bind(runtime_session_id)
        .execute(db.pool())
        .await
        .map_err(|_| AgentHostError::ProtectedPersistence)?;
    Ok(())
}

/// Marks a turn's unchanged-read references for clearing unless the turn
/// completes ([`Self::complete`]). Synchronous, so every early return and
/// cancellation is covered; the clearing runs before the session's next
/// turn plans anything.
pub(super) struct TopicReadTurnGuard {
    pub(super) failed: Arc<Mutex<HashSet<String>>>,
    pub(super) runtime_session_id: String,
    pub(super) completed: bool,
}

impl TopicReadTurnGuard {
    pub(super) fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for TopicReadTurnGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        if let Ok(mut failed) = self.failed.lock() {
            failed.insert(self.runtime_session_id.clone());
        }
    }
}

#[async_trait]
impl crate::typed_tools::ToolResultFilter for TopicReadFilter {
    async fn filter(
        &self,
        _call_id: &ToolCallId,
        arguments: &serde_json::Value,
        mut outcome: ToolOutcome,
    ) -> Result<ToolOutcome, RuntimeError> {
        let operation = arguments
            .get("operation")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if outcome.is_error
            || !UNCHANGED_READ_OPERATIONS.contains(&operation)
            || !outcome.content.is_empty()
            || !outcome.value.is_object()
        {
            return Ok(outcome);
        }
        use sha2::{Digest, Sha256};
        let digest = hex::encode(Sha256::digest(
            serde_json::to_vec(&outcome.value)
                .map_err(|_| RuntimeError::internal("topic read digest failed"))?,
        ));
        let compacted = self.compacted_through().await?;
        let (staged, start_history_len) = {
            let turn = self.turn.lock().map_err(|_| Self::persistence_error())?;
            (turn.staged.get(operation).cloned(), turn.start_history_len)
        };
        let start_history_len = i64::try_from(start_history_len).unwrap_or(i64::MAX);
        let visible = match staged {
            Some((staged_digest, reference)) => {
                (staged_digest == digest && compacted < start_history_len).then_some(reference)
            }
            None => {
                let previous: Option<(String, String, i64)> = sqlx::query_as(
                    "SELECT digest, call_ref, history_index FROM agent_topic_read_digest
                     WHERE runtime_session_id = ? AND operation = ?",
                )
                .bind(&self.runtime_session_id)
                .bind(operation)
                .fetch_optional(self.db.pool())
                .await
                .map_err(|_| Self::persistence_error())?;
                previous.and_then(|(previous_digest, reference, history_index)| {
                    (previous_digest == digest && history_index > compacted).then_some(reference)
                })
            }
        };
        if let Some(reference) = visible {
            outcome.value = serde_json::json!({"unchanged_since_call": reference});
            return Ok(outcome);
        }
        let reference = format!("read-{}", db::new_uuid_v4());
        if let serde_json::Value::Object(body) = &mut outcome.value {
            body.insert(
                "read_ref".to_owned(),
                serde_json::Value::String(reference.clone()),
            );
        }
        self.turn
            .lock()
            .map_err(|_| Self::persistence_error())?
            .staged
            .insert(operation.to_owned(), (digest, reference));
        Ok(outcome)
    }
}

/// Serialization of the host's identified data card. The pinned runtime renders
/// a contributed TailContext text fragment on the system role and rejects
/// user-role contributed messages, so this adapter narrows the card message to
/// the user role before the vendor adapter serializes it.
///
/// The card is identified by an explicit marker, never by matching text: the
/// contributor prefixes the card with this backend's random marker line, and
/// only the system-role message that starts with it is rewritten (marker
/// stripped). User text that repeats or edits the card is never touched.
/// Remove this adapter once the runtime admits user-role transient
/// contributors (upstream follow-up).
pub(super) struct ServerStateCardProvider {
    pub(super) inner: Arc<dyn Provider>,
    pub(super) marker: String,
}
impl fmt::Debug for ServerStateCardProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerStateCardProvider")
            .field("inner", &self.inner)
            .field("marker", &"[redacted]")
            .finish()
    }
}
#[async_trait]
impl Provider for ServerStateCardProvider {
    fn describe(&self) -> Vec<agent_runtime::core::provider::ModelDescriptor> {
        self.inner.describe()
    }
    fn capabilities(&self, model: &ModelId) -> Option<agent_runtime::core::provider::Capabilities> {
        self.inner.capabilities(model)
    }
    fn requires_nonempty_assistant_content(&self) -> bool {
        self.inner.requires_nonempty_assistant_content()
    }
    fn cache_resource_provider(
        &self,
    ) -> Option<&dyn agent_runtime::core::provider::CacheResourceProvider> {
        self.inner.cache_resource_provider()
    }
    async fn stream(
        &self,
        mut request: agent_runtime::core::provider::ProviderRequest,
        ctx: agent_runtime::core::provider::ProviderCallContext,
    ) -> Result<
        agent_runtime::core::provider::ProviderStream,
        agent_runtime::core::provider::ProviderError,
    > {
        unmark_state_card(&mut request.messages, &self.marker)?;
        self.inner.stream(request, ctx).await
    }
}

/// Rewrites the one marked state-card message to the user role and strips
/// its marker. A request without it is refused: the card is required.
fn unmark_state_card(
    messages: &mut [Message],
    marker: &str,
) -> Result<(), agent_runtime::core::provider::ProviderError> {
    let card = messages
        .iter_mut()
        .rev()
        .find(|message| {
            message.role == Role::System
                && matches!(
                    message.content.first(),
                    Some(ContentPart::Text { text }) if text.starts_with(marker)
                )
        })
        .ok_or_else(|| {
            agent_runtime::core::provider::ProviderError::new(
                agent_runtime::core::provider::ProviderErrorKind::BadRequest,
                "planned state card is missing",
            )
        })?;
    if let Some(ContentPart::Text { text }) = card.content.first_mut() {
        text.replace_range(..marker.len(), "");
    }
    card.role = Role::User;
    Ok(())
}

/// Sent in place of a retried message that is already in the durable
/// session history without a reply.
const RETRY_CONTINUATION_INPUT: &str = "Your previous attempt to answer the message above \
ended in an error before you replied. Continue from where you left off; do not repeat \
work that already succeeded.";

/// A failed turn keeps its user input (and any tool rounds it ran) in the
/// persistent session history, and every retry of the same chat message sends
/// that text again, so the model saw the identical request once per attempt.
/// When the most recent real user message in history is this input and no
/// final assistant reply follows it, send a short continuation instead.
pub(super) fn retry_aware_input(history: &[Message], input: String) -> String {
    for message in history.iter().rev() {
        match message.role {
            Role::Assistant
                if message.tool_calls().next().is_none()
                    && !message.joined_text().trim().is_empty() =>
            {
                return input;
            }
            Role::User => {
                // Only the first text part is user text. A session written
                // before the state card left the user message still holds a
                // card as a second part, which may differ between attempts.
                let text = message
                    .content
                    .first()
                    .and_then(ContentPart::as_text)
                    .unwrap_or_default();
                if text == RETRY_CONTINUATION_INPUT {
                    continue;
                }
                return if text == input {
                    RETRY_CONTINUATION_INPUT.to_owned()
                } else {
                    input
                };
            }
            _ => {}
        }
    }
    input
}

/// Harness component id of [`ServerStateCard`]. It owns no session state.
const STATE_CARD_COMPONENT_ID: &str = "forge.server_state_card";
const STATE_CARD_COMPONENT_REVISION: &str = "2";
const STATE_CARD_FRAGMENT_ID: &str = "forge:server-state-card";
/// Sorts the card after every other trailing fragment, so it is the last
/// block of the request.
const STATE_CARD_TAIL_SEQUENCE: u64 = 1_000_000;

/// The turn's server state card, contributed to each provider request of the
/// turn instead of being written into the user message.
///
/// A user message is durable history: a card attached to it would be sent
/// again on every later request, one more superseded card per turn without
/// bound. A contributed fragment is planned, budgeted and recorded in the
/// context manifest like any other, but it is never history, so a request
/// holds exactly one card, and every step of a tool loop still sees it.
///
/// The fragment trails the conversation, which keeps the system prompt, the
/// tool schemas and the whole history a byte-stable prefix across a state
/// change. The runtime renders a contributed text fragment on the system
/// role; [`ServerStateCardProvider`] sends it as the trailing user-role data
/// message (1B.1), identified by its marker line.
#[derive(Debug)]
pub(super) struct ServerStateCard {
    pub(super) card: String,
}

#[async_trait]
impl ContextContributor for ServerStateCard {
    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor::new(
            STATE_CARD_COMPONENT_ID,
            agent_runtime::registry::RegistryRevision::new(STATE_CARD_COMPONENT_REVISION),
        )
    }

    async fn contribute(&self, _view: &ContextView) -> Result<ContextPatch, RuntimeError> {
        // Required (the default), so compaction can never evict the only copy
        // of the current state.
        Ok(ContextPatch::new(vec![
            ContextFragment::new(
                STATE_CARD_FRAGMENT_ID,
                FragmentKind::Continuation,
                FragmentSource::Host,
                agent_runtime::registry::RegistryRevision::from_content(&self.card),
                FragmentContent::Text(self.card.clone()),
            )
            .with_position(ContextPosition::new(
                ContextLane::TailContext,
                STATE_CARD_TAIL_SEQUENCE,
            ))
            .with_cache_class(CacheClass::NoCache),
        ]))
    }
}

const TASK_COMPACTION_HIGH_PERCENT: u64 = 85;
const TASK_COMPACTION_LOW_PERCENT: u64 = 70;
const TASK_COMPACTION_POLICY_REVISION: &str = "forge-task-structural-compaction-1";

pub(super) fn task_structural_compactor(max_input_tokens: u32) -> StructuralCompactor {
    let max_input_tokens = u64::from(max_input_tokens).max(1);
    let watermark = |percent: u64| {
        u32::try_from(max_input_tokens.saturating_mul(percent) / 100)
            .unwrap_or(u32::MAX)
            .max(1)
    };
    StructuralCompactor::new(CompactionPolicy::new(
        agent_runtime::registry::RegistryRevision::from_content(TASK_COMPACTION_POLICY_REVISION),
        watermark(TASK_COMPACTION_HIGH_PERCENT),
        watermark(TASK_COMPACTION_LOW_PERCENT),
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TurnContextMode {
    pub(super) persistent_session: bool,
    pub(super) lcm: bool,
    pub(super) structural_compaction: bool,
}

pub(super) fn turn_context_mode(
    scope: &CanonicalScope,
    task_role: Option<&str>,
) -> TurnContextMode {
    if scope.is_ephemeral_inquiry() {
        return TurnContextMode {
            persistent_session: false,
            lcm: false,
            structural_compaction: false,
        };
    }
    if scope.scope_type == CanonicalScopeType::Task {
        return TurnContextMode {
            // Reviewer attempts are self-contained audits. Worker/planner
            // follow-ups retain their protected session history, but compact
            // that history structurally instead of admitting it to LCM.
            persistent_session: task_role != Some("reviewer"),
            lcm: false,
            structural_compaction: true,
        };
    }
    TurnContextMode {
        persistent_session: true,
        lcm: true,
        structural_compaction: false,
    }
}

#[cfg(test)]
mod context_mode_tests;
#[cfg(test)]
mod retry_input_tests;
#[cfg(test)]
mod state_card_provider_tests;
#[cfg(test)]
mod topic_read_tests;
