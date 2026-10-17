//! Idempotent system notices on durable Agent Chat timelines.

use crate::Result;
use db::{
    new_uuid_v4, now_rfc3339, AgentChatMessage, AgentChatMessageAuthorType, AgentChatMessageRepo,
    AgentChatMessageStatus, CreateAgentChatMessage,
};

/// Append one durable system message to an Agent Chat timeline, outside any
/// turn. The caller-chosen `message_id` is the idempotency key: replaying the
/// same id returns the stored message instead of appending a duplicate, so
/// lifecycle hooks can call this on every (re)execution.
pub async fn append_system_chat_message(
    db: &db::SqliteDb,
    chat_id: &str,
    message_id: &str,
    content: &str,
) -> Result<AgentChatMessage> {
    Ok(AgentChatMessageRepo::append_agent_chat_message(
        db,
        CreateAgentChatMessage {
            id: message_id.to_owned(),
            chat_id: chat_id.to_owned(),
            // The store allocates the real sequence.
            sequence: 0,
            author_type: AgentChatMessageAuthorType::System,
            author_id: None,
            content: content.to_owned(),
            content_guard_json: "{}".to_owned(),
            sensitivity: "internal".to_owned(),
            status: AgentChatMessageStatus::Complete,
            outcome: None,
            model: None,
            profile_id: None,
            session_id: None,
            context_manifest_id: None,
            token_usage_json: None,
            duration_ms: None,
            error: None,
            correlation_id: new_uuid_v4(),
            causation_id: None,
            handoff_id: None,
            source_type: "native".to_owned(),
            source_id: None,
            source_message_id: None,
            source_room_id: None,
            source_conversation_id: None,
            source_sequence: None,
            source_metadata_json: "{}".to_owned(),
            created_at: now_rfc3339(),
        },
    )
    .await?)
}
