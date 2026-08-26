//! Repository boundary for the Main Chat topic boundary (design D21, live
//! finding F18).
//!
//! A topic is a durable, user-owned context epoch *inside* the one account
//! Main Chat -- never a second chat, binding, or authority scope. Rows are
//! immutable once written (V103 enforces this with the same
//! before-update/before-delete trigger pattern as every other Agent Chat
//! ledger table): the store only ever inserts the next topic in sequence.
//!
//! A topic's "messages" are never marked by mutating `agent_chat_message` or
//! `agent_chat_turn_job` rows -- doing so would touch ids/provenance the
//! migration and every later read must leave untouched. Instead each topic
//! records the `sequence` of the visible divider message that opens it
//! (`starting_message_sequence`); "this topic's messages" is
//! `sequence >= starting_message_sequence`, bounded above by the next
//! topic's `starting_message_sequence` when one exists. The V103 backfill
//! creates exactly one topic per existing Main Chat at `sequence = 0`,
//! `starting_message_sequence = 0`, so every historical message/turn is
//! covered without a single UPDATE to either table.

use async_trait::async_trait;

use crate::{AgentChatMessage, AgentChatMessageAuthorType, AgentChatMessageStatus, Result};

/// One durable Main Chat topic row (D21).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentChatTopic {
    pub id: String,
    pub chat_id: String,
    /// Immutable, zero-based, unique per `chat_id`.
    pub sequence: i64,
    pub label: String,
    pub summary: Option<String>,
    /// The visible divider message that opens this topic. `None` only for
    /// the V103-backfilled initial topic, which predates any divider
    /// message and simply starts at `starting_message_sequence = 0`.
    pub starting_message_id: Option<String>,
    pub starting_message_sequence: i64,
    /// `"user"` for an explicit reset, `"system"` for the migration backfill.
    pub principal_type: String,
    pub principal_id: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct CreateAgentChatTopic {
    /// Caller-minted id; also this call's idempotency key -- replaying the
    /// same id returns the already-committed topic/divider pair instead of
    /// rotating a second time.
    pub id: String,
    pub chat_id: String,
    pub label: String,
    pub summary: Option<String>,
    pub principal_type: String,
    pub principal_id: Option<String>,
    pub created_at: String,
}

/// One atomic topic rotation: the new topic row anchored to a freshly
/// appended, visible divider message. Both commit together, or neither does.
///
/// The transactional implementation is the sole authority for the D21/F18
/// denial rule: it must refuse to rotate while a Main turn is live (any
/// `agent_chat_turn_job` for this chat in `queued`/`leased`/`retry_wait`) or
/// while a Product Genesis session for this chat's account still needs an
/// explicit finish-or-cancel decision (`discovering`/`ready_for_project`).
/// Both checks happen inside the same immediate transaction that performs
/// the insert, so nothing can race between the check and the write.
#[derive(Debug, Clone)]
pub struct RotateAgentChatTopic {
    pub topic: CreateAgentChatTopic,
    /// The visible timeline divider. `sequence` is only a hint -- the store
    /// allocates the real value exactly like every other Agent Chat message
    /// append, and the persisted topic's `starting_message_sequence` is that
    /// allocated value.
    pub divider_message: crate::CreateAgentChatMessage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RotatedAgentChatTopic {
    pub topic: AgentChatTopic,
    pub divider_message: AgentChatMessage,
}

/// Denied because the D21/F18 rotation precondition was not met. Carried as
/// a normal `Ok` value rather than an error variant so the service layer can
/// render a specific, safe explanation instead of a generic conflict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentChatTopicDenialReason {
    MainTurnLive,
    GenesisDecisionPending,
}

#[async_trait]
pub trait AgentChatTopicRepo: Send + Sync {
    async fn get_agent_chat_topic(&self, id: &str) -> Result<Option<AgentChatTopic>>;
    /// The most recently started topic for this chat -- `None` only before
    /// the V103 backfill/first rotation has ever run for it.
    async fn get_current_agent_chat_topic(&self, chat_id: &str) -> Result<Option<AgentChatTopic>>;
    async fn list_agent_chat_topics(&self, chat_id: &str) -> Result<Vec<AgentChatTopic>>;
}

#[async_trait]
pub trait AgentChatTopicTransactionRepo: Send + Sync {
    /// Rotate to a new topic, or refuse per [`AgentChatTopicDenialReason`].
    async fn rotate_agent_chat_topic(
        &self,
        input: RotateAgentChatTopic,
    ) -> Result<std::result::Result<RotatedAgentChatTopic, AgentChatTopicDenialReason>>;
}

/// Build the visible system divider message body. Kept as one function so
/// the exact copy shown in the timeline can never drift between the service
/// that rotates a topic and any test that asserts on it.
#[must_use]
pub fn topic_divider_message_body(label: &str) -> String {
    format!("New topic started: {label}")
}

/// Construct the divider message input for a topic rotation. Shared so the
/// author/status/sensitivity shape used for every topic divider (a durable,
/// complete, internal-sensitivity system message) is defined exactly once.
#[must_use]
pub fn topic_divider_message(
    message_id: String,
    chat_id: String,
    label: &str,
    correlation_id: String,
    created_at: String,
) -> crate::CreateAgentChatMessage {
    crate::CreateAgentChatMessage {
        id: message_id,
        chat_id,
        // The store allocates the real sequence.
        sequence: 0,
        author_type: AgentChatMessageAuthorType::System,
        author_id: None,
        content: topic_divider_message_body(label),
        content_guard_json: "{}".to_owned(),
        sensitivity: "internal".to_owned(),
        status: AgentChatMessageStatus::Complete,
        outcome: Some("topic_started".to_owned()),
        model: None,
        profile_id: None,
        session_id: None,
        context_manifest_id: None,
        token_usage_json: None,
        duration_ms: None,
        error: None,
        correlation_id,
        causation_id: None,
        handoff_id: None,
        source_type: "native".to_owned(),
        source_id: None,
        source_message_id: None,
        source_room_id: None,
        source_conversation_id: None,
        source_sequence: None,
        source_metadata_json: "{}".to_owned(),
        created_at,
    }
}
