//! Minimal durable notifications for chat projections missing a lifecycle event.
use super::*;
use crate::now_rfc3339;

pub(super) struct ChatReadEvent<'a> {
    pub event_type: &'a str,
    pub entity_type: &'a str,
    pub entity_id: &'a str,
    pub chat_id: &'a str,
    pub status: Option<&'a str>,
    pub dedupe_key: Option<String>,
    pub created_at: &'a str,
}

pub(super) async fn append(
    db: &SqliteDb,
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    input: ChatReadEvent<'_>,
) -> Result<()> {
    DomainEventRepo::append_event_in_tx(db, transaction, &CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: input.event_type.to_owned(),
        entity_type: input.entity_type.to_owned(),
        entity_id: input.entity_id.to_owned(),
        actor_type: "system".to_owned(), actor_id: None,
        scope_type: "agent_chat".to_owned(), scope_id: input.chat_id.to_owned(),
        correlation_id: input.entity_id.to_owned(), causation_id: None, causation_depth: 0,
        dedupe_key: input.dedupe_key,
        payload_json: serde_json::json!({"chat_id": input.chat_id, "id": input.entity_id, "status": input.status}).to_string(),
        created_at: input.created_at.to_owned(),
    }).await?;
    Ok(())
}

impl SqliteDb {
    /// Append a minimal status notification in the same transaction as the write.
    /// Call only after a successful status change, never for lease renewals.
    pub async fn append_agent_chat_turn_status_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        turn_id: &str,
    ) -> Result<()> {
        let row = sqlx::query("SELECT chat_id, status FROM agent_chat_turn_job WHERE id = ?")
            .bind(turn_id)
            .fetch_one(&mut **transaction)
            .await?;
        append(
            self,
            transaction,
            ChatReadEvent {
                event_type: "agent_chat.turn.status_changed",
                entity_type: "agent_chat_turn_job",
                entity_id: turn_id,
                chat_id: row.get::<String, _>("chat_id").as_str(),
                status: Some(row.get::<String, _>("status").as_str()),
                dedupe_key: None,
                created_at: &now_rfc3339(),
            },
        )
        .await
    }

    /// Keep repository status writes and their read notifications atomic.
    pub(crate) async fn execute_agent_chat_turn_status_update<'q>(
        &self,
        turn_id: &str,
        query: sqlx::query::Query<'q, Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    ) -> Result<sqlx::sqlite::SqliteQueryResult> {
        let mut tx = crate::begin_immediate(&self.pool).await?;
        let before: Option<String> =
            sqlx::query_scalar("SELECT status FROM agent_chat_turn_job WHERE id = ?")
                .bind(turn_id)
                .fetch_optional(&mut *tx)
                .await?;
        let result = query.execute(&mut *tx).await?;
        if result.rows_affected() != 0 {
            let after: String =
                sqlx::query_scalar("SELECT status FROM agent_chat_turn_job WHERE id = ?")
                    .bind(turn_id)
                    .fetch_one(&mut *tx)
                    .await?;
            if before.as_deref() != Some(after.as_str()) {
                self.append_agent_chat_turn_status_in_tx(&mut tx, turn_id)
                    .await?;
            }
        }
        tx.commit().await?;
        Ok(result)
    }
}
