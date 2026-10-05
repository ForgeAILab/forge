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
    AbandonAgentChatTopicRotation, AgentChatTopic, AgentChatTopicDenialReason, AgentChatTopicRepo,
    AgentChatTopicTransactionRepo, CreateAgentChatMessage, RotateAgentChatTopic,
    RotatedAgentChatTopic,
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
    async fn request_agent_chat_topic(&self, input: RotateAgentChatTopic) -> Result<String> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        if pending_genesis_decision(&mut tx, &input.topic.chat_id).await? {
            tx.rollback().await?;
            return Err(DbError::AgentChatTopicDenied(
                AgentChatTopicDenialReason::GenesisDecisionPending,
            ));
        }
        // A pending intent keeps its id (its crash-recovery identity) and
        // takes the newest requested label and summary. Automatic triggers
        // stay INSERT OR IGNORE, so they never overwrite a user's request.
        sqlx::query(
            "INSERT INTO agent_chat_topic_rotation
                 (chat_id, id, label, requested_summary, cause, created_at)
             VALUES (?, ?, ?, ?, 'rest', ?)
             ON CONFLICT(chat_id) DO UPDATE SET
                 label = excluded.label,
                 requested_summary = excluded.requested_summary",
        )
        .bind(&input.topic.chat_id)
        .bind(&input.topic.id)
        .bind(&input.topic.label)
        .bind(&input.topic.summary)
        .bind(&input.topic.created_at)
        .execute(&mut *tx)
        .await?;
        let id = sqlx::query_scalar("SELECT id FROM agent_chat_topic_rotation WHERE chat_id = ?")
            .bind(&input.topic.chat_id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(id)
    }

    async fn pending_agent_chat_topic(&self, chat_id: &str) -> Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT id FROM agent_chat_topic_rotation WHERE chat_id = ?")
                .bind(chat_id)
                .fetch_optional(self.pool())
                .await?,
        )
    }

    async fn abandon_agent_chat_topic_rotation(
        &self,
        input: AbandonAgentChatTopicRotation,
    ) -> Result<bool> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let successor: Option<String> = sqlx::query_scalar(
            "SELECT successor_session_id FROM agent_chat_topic_rotation
             WHERE chat_id = ? AND id = ? AND owner_token = ?",
        )
        .bind(&input.chat_id)
        .bind(&input.intent_id)
        .bind(&input.owner_token)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(successor) = successor else {
            tx.rollback().await?;
            return Ok(false);
        };
        let now = crate::now_rfc3339();
        if input.hand_over {
            // The fork superseded the source: the successor takes over.
            sqlx::query(
                "DELETE FROM agent_topic_read_digest WHERE runtime_session_id =
                     (SELECT p.runtime_session_id FROM agent_session s
                      JOIN agent_session p ON p.id = s.predecessor_session_id
                      WHERE s.id = ?)",
            )
            .bind(&successor)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "UPDATE agent_session SET status = 'replaced', replaced_by_session_id = ?1,
                     version = version + 1, updated_at = ?2
                 WHERE id = (SELECT predecessor_session_id FROM agent_session WHERE id = ?1)
                   AND status IN ('starting', 'ready', 'running', 'degraded', 'suspended')",
            )
            .bind(&successor)
            .bind(&now)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "UPDATE agent_session SET status = 'ready', version = version + 1, updated_at = ?
                 WHERE id = ? AND status = 'suspended'",
            )
            .bind(&now)
            .bind(&successor)
            .execute(&mut *tx)
            .await?;
        } else {
            // A reserved successor that never took over must not be resumed
            // later as the chat's dormant session.
            sqlx::query(
                "UPDATE agent_session SET status = 'failed', version = version + 1, updated_at = ?
                 WHERE id = ? AND status = 'suspended'",
            )
            .bind(&now)
            .bind(&successor)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query("DELETE FROM agent_chat_topic_rotation WHERE chat_id = ? AND id = ?")
            .bind(&input.chat_id)
            .bind(&input.intent_id)
            .execute(&mut *tx)
            .await?;
        let notice = append_system_message_in_tx(self, &mut tx, &input.notice_message).await?;
        DomainEventRepo::append_event_in_tx(
            self,
            &mut tx,
            &CreateDomainEvent {
                id: new_uuid_v4(),
                event_type: "agent_chat.topic.rotation_failed".to_owned(),
                entity_type: "agent_chat_topic_rotation".to_owned(),
                entity_id: input.intent_id.clone(),
                actor_type: "system".to_owned(),
                actor_id: None,
                scope_type: "agent_chat".to_owned(),
                scope_id: input.chat_id.clone(),
                correlation_id: input.intent_id.clone(),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: Some(format!(
                    "chat-read:agent_chat.topic.rotation_failed:{}",
                    input.intent_id
                )),
                payload_json: serde_json::json!({
                    "chat_id": input.chat_id,
                    "id": input.intent_id,
                    "error_kind": input.error_kind,
                    "attempts": input.attempts,
                    "session_handed_over": input.hand_over,
                    "notice_message_id": notice,
                })
                .to_string(),
                created_at: now,
            },
        )
        .await?;
        tx.commit().await?;
        Ok(true)
    }

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
        let system_rotation = input.rotation_owner.is_some();
        let leased: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_chat_turn_job WHERE chat_id = ? AND status = 'leased')")
            .bind(&input.topic.chat_id).fetch_one(&mut *transaction).await?;
        if leased || (!system_rotation && live_turn_count > 0) {
            transaction.rollback().await?;
            return Ok(Err(AgentChatTopicDenialReason::MainTurnLive));
        }

        // Deny a direct rotation while a Product Genesis session for this
        // account still needs an explicit finish-or-cancel decision
        // (D21/8.5.4). An intent was admitted under that rule when it was
        // requested (`request_agent_chat_topic`) or raised by Genesis
        // itself, so its execution is not re-denied: that would leave the
        // intent pending and block the chat.
        if !system_rotation
            && pending_genesis_decision(&mut transaction, &input.topic.chat_id).await?
        {
            transaction.rollback().await?;
            return Ok(Err(AgentChatTopicDenialReason::GenesisDecisionPending));
        }

        let mut input = input;
        if let Some(owner) = &input.rotation_owner {
            // The intent row is the authority for label and summary: a REST
            // request may have updated them after this attempt leased it.
            let intent: Option<(String, Option<String>)> = sqlx::query_as(
                "SELECT label, requested_summary FROM agent_chat_topic_rotation
                 WHERE chat_id = ? AND id = ? AND owner_token = ?",
            )
            .bind(&input.topic.chat_id)
            .bind(&input.topic.id)
            .bind(owner)
            .fetch_optional(&mut *transaction)
            .await?;
            let Some((label, summary)) = intent else {
                return Err(DbError::VersionConflict);
            };
            input.divider_message.content = crate::topic_divider_message_body(&label);
            input.topic.label = label;
            input.topic.summary = summary;
        }
        let next_sequence: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(sequence), -1) + 1 FROM agent_chat_topic WHERE chat_id = ?",
        )
        .bind(&input.topic.chat_id)
        .fetch_one(&mut *transaction)
        .await?;

        // Append the visible divider message, allocating its sequence
        // exactly like every other Agent Chat message.
        let mut divider = input.divider_message.clone();
        divider.chat_id = input.topic.chat_id.clone();
        append_system_message_in_tx(self, &mut transaction, &divider).await?;
        let divider_sequence: i64 =
            sqlx::query_scalar("SELECT sequence FROM agent_chat_message WHERE id = ?")
                .bind(&input.divider_message.id)
                .fetch_one(&mut *transaction)
                .await?;

        sqlx::query(
            "INSERT INTO agent_chat_topic (
                id, chat_id, sequence, label, summary, starting_message_id,
                starting_message_sequence, principal_type, principal_id, created_at, runtime_session_id
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
        .bind(&input.runtime_session_id)
        .execute(&mut *transaction)
        .await?;

        if let Some(runtime_id) = &input.runtime_session_id {
            let successor: (String, Option<String>) = sqlx::query_as(
                "SELECT id, predecessor_session_id FROM agent_session WHERE runtime_session_id = ?",
            )
            .bind(runtime_id)
            .fetch_one(&mut *transaction)
            .await?;
            if let Some(parent) = &successor.1 {
                // The predecessor's unchanged-read digests refer to calls the
                // new topic cannot see.
                sqlx::query(
                    "DELETE FROM agent_topic_read_digest WHERE runtime_session_id =
                         (SELECT runtime_session_id FROM agent_session WHERE id = ?)",
                )
                .bind(parent)
                .execute(&mut *transaction)
                .await?;
                let version: i64 =
                    sqlx::query_scalar("SELECT version FROM agent_session WHERE id = ?")
                        .bind(parent)
                        .fetch_one(&mut *transaction)
                        .await?;
                sqlx::query("UPDATE agent_session SET status = 'replaced', replaced_by_session_id = ?, version = version + 1 WHERE id = ? AND version = ?")
                    .bind(&successor.0).bind(parent).bind(version).execute(&mut *transaction).await?;
            }
            let version: i64 = sqlx::query_scalar("SELECT version FROM agent_session WHERE id = ?")
                .bind(&successor.0)
                .fetch_one(&mut *transaction)
                .await?;
            sqlx::query("UPDATE agent_session SET status = 'ready', version = version + 1 WHERE id = ? AND version = ? AND status = 'suspended'")
                .bind(&successor.0).bind(version).execute(&mut *transaction).await?;
        }
        if let Some(owner) = &input.rotation_owner {
            sqlx::query("DELETE FROM agent_chat_topic_rotation WHERE chat_id = ? AND id = ? AND owner_token = ?")
                .bind(&input.topic.chat_id).bind(&input.topic.id).bind(owner).execute(&mut *transaction).await?;
        }
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

        super::chat_read_events::append(
            self,
            &mut transaction,
            super::chat_read_events::ChatReadEvent {
                event_type: "agent_chat.topic.started",
                entity_type: "agent_chat_topic",
                entity_id: &topic_row.id,
                chat_id: &topic_row.chat_id,
                status: None,
                dedupe_key: Some(format!(
                    "chat-read:agent_chat.topic.started:{}:{}",
                    topic_row.id, topic_row.sequence
                )),
                created_at: &topic_row.created_at,
            },
        )
        .await?;
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

/// Whether the chat's account has a Product Genesis session awaiting an
/// explicit finish-or-cancel decision (D21/8.5.4). Project Chats carry no
/// `account_id` and are never denied.
async fn pending_genesis_decision(
    transaction: &mut Transaction<'_, Sqlite>,
    chat_id: &str,
) -> Result<bool> {
    let chat_row = sqlx::query("SELECT account_id FROM agent_chat WHERE id = ?")
        .bind(chat_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(DbError::NotFound)?;
    let Some(account_id) = chat_row.try_get::<Option<String>, _>("account_id")? else {
        return Ok(false);
    };
    Ok(sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM product_genesis_session
         WHERE account_id = ? AND lifecycle IN ('discovering', 'ready_for_project'))",
    )
    .bind(&account_id)
    .fetch_one(&mut **transaction)
    .await?)
}

/// Appends one system-authored chat message (topic divider or rotation
/// notice), allocating its sequence exactly like every other Agent Chat
/// message append (message_count/version bump on the parent chat row), and
/// records its `agent_chat.message.appended` read event. Returns its id.
async fn append_system_message_in_tx(
    db: &SqliteDb,
    transaction: &mut Transaction<'_, Sqlite>,
    message: &CreateAgentChatMessage,
) -> Result<String> {
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
    .bind(&message.created_at)
    .bind(&message.created_at)
    .bind(&message.created_at)
    .bind(&message.chat_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DbError::NotFound)?;
    let sequence = message_count - 1;
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
    .bind(&message.id)
    .bind(&message.chat_id)
    .bind(sequence)
    .bind(message.author_type.to_string())
    .bind(message.author_id.as_deref())
    .bind(&message.content)
    .bind(&message.content_guard_json)
    .bind(&message.sensitivity)
    .bind(message.status.to_string())
    .bind(message.outcome.as_deref())
    .bind(message.model.as_deref())
    .bind(message.profile_id.as_deref())
    .bind(message.session_id.as_deref())
    .bind(message.context_manifest_id.as_deref())
    .bind(message.token_usage_json.as_deref())
    .bind(message.duration_ms)
    .bind(message.error.as_deref())
    .bind(&message.correlation_id)
    .bind(message.causation_id.as_deref())
    .bind(message.handoff_id.as_deref())
    .bind(&message.source_type)
    .bind(message.source_id.as_deref())
    .bind(message.source_message_id.as_deref())
    .bind(message.source_room_id.as_deref())
    .bind(message.source_conversation_id.as_deref())
    .bind(message.source_sequence)
    .bind(&message.source_metadata_json)
    .bind(&message.created_at)
    .execute(&mut **transaction)
    .await?;
    super::chat_read_events::append(
        db,
        transaction,
        super::chat_read_events::ChatReadEvent {
            event_type: "agent_chat.message.appended",
            entity_type: "agent_chat_message",
            entity_id: &message.id,
            chat_id: &message.chat_id,
            status: Some(message.status.to_string().as_str()),
            dedupe_key: Some(format!("chat-message:{}", message.id)),
            created_at: &crate::now_rfc3339(),
        },
    )
    .await?;
    Ok(message.id.clone())
}
