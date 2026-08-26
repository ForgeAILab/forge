//! SQLite adapter for the Main Chat topic boundary (V103, D21, F18).
//!
//! Kept as its own file rather than folded into `agent_chat.rs` so this
//! change and any concurrent Agent Chat work never touch the same lines.
//! `map_agent_chat_message`/`map_agent_chat_topic` below are therefore local
//! duplicates of the row-mapping shape already used in `agent_chat.rs`
//! rather than shared private helpers -- those helpers are module-private to
//! `sqlite::agent_chat` and intentionally left untouched here.

use super::*;
use crate::{
    AgentChatTopic, AgentChatTopicDenialReason, AgentChatTopicRepo, AgentChatTopicTransactionRepo,
    RotateAgentChatTopic, RotatedAgentChatTopic,
};

#[async_trait]
impl AgentChatTopicRepo for SqliteDb {
    async fn get_agent_chat_topic(&self, id: &str) -> Result<Option<AgentChatTopic>> {
        sqlx::query("SELECT * FROM agent_chat_topic WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_agent_chat_topic)
            .transpose()
    }

    async fn get_current_agent_chat_topic(&self, chat_id: &str) -> Result<Option<AgentChatTopic>> {
        sqlx::query(
            "SELECT * FROM agent_chat_topic
             WHERE chat_id = ? ORDER BY sequence DESC LIMIT 1",
        )
        .bind(chat_id)
        .fetch_optional(&self.pool)
        .await?
        .map(map_agent_chat_topic)
        .transpose()
    }

    async fn list_agent_chat_topics(&self, chat_id: &str) -> Result<Vec<AgentChatTopic>> {
        sqlx::query("SELECT * FROM agent_chat_topic WHERE chat_id = ? ORDER BY sequence ASC")
            .bind(chat_id)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(map_agent_chat_topic)
            .collect()
    }
}

#[async_trait]
impl AgentChatTopicTransactionRepo for SqliteDb {
    async fn rotate_agent_chat_topic(
        &self,
        input: RotateAgentChatTopic,
    ) -> Result<std::result::Result<RotatedAgentChatTopic, AgentChatTopicDenialReason>> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;

        // Idempotent replay: a topic with this id already committed. Return
        // it and its divider message rather than rotating a second time.
        if let Some(existing) = sqlx::query("SELECT * FROM agent_chat_topic WHERE id = ?")
            .bind(&input.topic.id)
            .fetch_optional(&mut *transaction)
            .await?
        {
            let topic = map_agent_chat_topic(existing)?;
            let Some(divider_message_id) = topic.starting_message_id.clone() else {
                transaction.rollback().await?;
                return Err(DbError::Check(
                    "replayed Main Chat topic has no divider message".to_owned(),
                ));
            };
            let divider_row = sqlx::query("SELECT * FROM agent_chat_message WHERE id = ?")
                .bind(&divider_message_id)
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or(DbError::NotFound)?;
            let divider_message = map_agent_chat_message(divider_row)?;
            transaction.rollback().await?;
            return Ok(Ok(RotatedAgentChatTopic {
                topic,
                divider_message,
            }));
        }

        // Deny while a Main turn is live (D21/8.5.4): any non-terminal turn
        // job for this chat.
        let live_turn_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_chat_turn_job
             WHERE chat_id = ? AND status IN ('queued', 'leased', 'retry_wait')",
        )
        .bind(&input.topic.chat_id)
        .fetch_one(&mut *transaction)
        .await?;
        if live_turn_count > 0 {
            transaction.rollback().await?;
            return Ok(Err(AgentChatTopicDenialReason::MainTurnLive));
        }

        // Deny while a Product Genesis session for this account still needs
        // an explicit finish-or-cancel decision (D21/8.5.4). Only a Main
        // Chat carries an `account_id`; a Project Chat never reaches this
        // branch since only Main Chats are rotated today.
        let chat_row = sqlx::query("SELECT * FROM agent_chat WHERE id = ?")
            .bind(&input.topic.chat_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        let account_id: Option<String> = chat_row.try_get("account_id")?;
        if let Some(account_id) = account_id {
            let pending_genesis = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM product_genesis_session
                 WHERE account_id = ? AND lifecycle IN ('discovering', 'ready_for_project')",
            )
            .bind(&account_id)
            .fetch_one(&mut *transaction)
            .await?;
            if pending_genesis > 0 {
                transaction.rollback().await?;
                return Ok(Err(AgentChatTopicDenialReason::GenesisDecisionPending));
            }
        }

        let next_sequence: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(sequence), -1) + 1 FROM agent_chat_topic WHERE chat_id = ?",
        )
        .bind(&input.topic.chat_id)
        .fetch_one(&mut *transaction)
        .await?;

        // Append the visible divider message, allocating its sequence
        // exactly like every other Agent Chat message (message_count/version
        // bump on the parent chat row).
        let message_count = sqlx::query_scalar::<_, i64>(
            "UPDATE agent_chat
             SET message_count = message_count + 1,
                 last_message_at = CASE
                     WHEN last_message_at IS NULL OR last_message_at < ? THEN ?
                     ELSE last_message_at END,
                 version = version + 1, updated_at = ?
             WHERE id = ?
             RETURNING message_count",
        )
        .bind(&input.divider_message.created_at)
        .bind(&input.divider_message.created_at)
        .bind(&input.divider_message.created_at)
        .bind(&input.topic.chat_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
        let divider_sequence = message_count - 1;

        sqlx::query(
            "INSERT INTO agent_chat_message (
                id, chat_id, sequence, author_type, author_id, content,
                content_guard_json, sensitivity, status, outcome, model, profile_id,
                session_id, context_manifest_id, token_usage_json, duration_ms, error,
                correlation_id, causation_id, handoff_id, source_type, source_id,
                source_message_id, source_room_id, source_conversation_id,
                source_sequence, source_metadata_json, created_at
             ) VALUES (
                 ?, ?, ?, ?,
                 ?, ?, ?, ?,
                 ?, ?, ?, ?,
                 ?, ?, ?, ?,
                 ?, ?, ?, ?,
                 ?, ?, ?, ?,
                 ?, ?, ?, ?
             )",
        )
        .bind(&input.divider_message.id)
        .bind(&input.topic.chat_id)
        .bind(divider_sequence)
        .bind(input.divider_message.author_type.to_string())
        .bind(input.divider_message.author_id.as_deref())
        .bind(&input.divider_message.content)
        .bind(&input.divider_message.content_guard_json)
        .bind(&input.divider_message.sensitivity)
        .bind(input.divider_message.status.to_string())
        .bind(input.divider_message.outcome.as_deref())
        .bind(input.divider_message.model.as_deref())
        .bind(input.divider_message.profile_id.as_deref())
        .bind(input.divider_message.session_id.as_deref())
        .bind(input.divider_message.context_manifest_id.as_deref())
        .bind(input.divider_message.token_usage_json.as_deref())
        .bind(input.divider_message.duration_ms)
        .bind(input.divider_message.error.as_deref())
        .bind(&input.divider_message.correlation_id)
        .bind(input.divider_message.causation_id.as_deref())
        .bind(input.divider_message.handoff_id.as_deref())
        .bind(&input.divider_message.source_type)
        .bind(input.divider_message.source_id.as_deref())
        .bind(input.divider_message.source_message_id.as_deref())
        .bind(input.divider_message.source_room_id.as_deref())
        .bind(input.divider_message.source_conversation_id.as_deref())
        .bind(input.divider_message.source_sequence)
        .bind(&input.divider_message.source_metadata_json)
        .bind(&input.divider_message.created_at)
        .execute(&mut *transaction)
        .await?;

        sqlx::query(
            "INSERT INTO agent_chat_topic (
                id, chat_id, sequence, label, summary, starting_message_id,
                starting_message_sequence, principal_type, principal_id, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.topic.id)
        .bind(&input.topic.chat_id)
        .bind(next_sequence)
        .bind(&input.topic.label)
        .bind(input.topic.summary.as_deref())
        .bind(&input.divider_message.id)
        .bind(divider_sequence)
        .bind(&input.topic.principal_type)
        .bind(input.topic.principal_id.as_deref())
        .bind(&input.topic.created_at)
        .execute(&mut *transaction)
        .await?;

        let topic_row = sqlx::query("SELECT * FROM agent_chat_topic WHERE id = ?")
            .bind(&input.topic.id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(DbError::from)
            .and_then(map_agent_chat_topic)?;
        let divider_row = sqlx::query("SELECT * FROM agent_chat_message WHERE id = ?")
            .bind(&input.divider_message.id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(DbError::from)
            .and_then(map_agent_chat_message)?;

        transaction.commit().await?;
        Ok(Ok(RotatedAgentChatTopic {
            topic: topic_row,
            divider_message: divider_row,
        }))
    }
}

fn map_agent_chat_topic(row: SqliteRow) -> Result<AgentChatTopic> {
    Ok(AgentChatTopic {
        id: row.try_get("id")?,
        chat_id: row.try_get("chat_id")?,
        sequence: row.try_get("sequence")?,
        label: row.try_get("label")?,
        summary: row.try_get("summary")?,
        starting_message_id: row.try_get("starting_message_id")?,
        starting_message_sequence: row.try_get("starting_message_sequence")?,
        principal_type: row.try_get("principal_type")?,
        principal_id: row.try_get("principal_id")?,
        created_at: row.try_get("created_at")?,
    })
}

/// Local duplicate of `sqlite::agent_chat::map_agent_chat_message` (see the
/// module doc comment for why this file does not reuse that helper).
fn map_agent_chat_message(row: SqliteRow) -> Result<AgentChatMessage> {
    Ok(AgentChatMessage {
        id: row.try_get("id")?,
        chat_id: row.try_get("chat_id")?,
        sequence: row.try_get("sequence")?,
        author_type: parse_enum(row.try_get::<String, _>("author_type")?)?,
        author_id: row.try_get("author_id")?,
        content: row.try_get("content")?,
        content_guard_json: row.try_get("content_guard_json")?,
        sensitivity: row.try_get("sensitivity")?,
        status: parse_enum(row.try_get::<String, _>("status")?)?,
        outcome: row.try_get("outcome")?,
        model: row.try_get("model")?,
        profile_id: row.try_get("profile_id")?,
        session_id: row.try_get("session_id")?,
        context_manifest_id: row.try_get("context_manifest_id")?,
        token_usage_json: row.try_get("token_usage_json")?,
        duration_ms: row.try_get("duration_ms")?,
        error: row.try_get("error")?,
        correlation_id: row.try_get("correlation_id")?,
        causation_id: row.try_get("causation_id")?,
        handoff_id: row.try_get("handoff_id")?,
        source_type: row.try_get("source_type")?,
        source_id: row.try_get("source_id")?,
        source_message_id: row.try_get("source_message_id")?,
        source_room_id: row.try_get("source_room_id")?,
        source_conversation_id: row.try_get("source_conversation_id")?,
        source_sequence: row.try_get("source_sequence")?,
        source_metadata_json: row.try_get("source_metadata_json")?,
        created_at: row.try_get("created_at")?,
    })
}
