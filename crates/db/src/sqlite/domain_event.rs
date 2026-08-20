use super::*;

#[async_trait]
impl DomainEventRepo for SqliteDb {
    async fn append_event(&self, input: CreateDomainEvent) -> Result<DomainEvent> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let event = self.append_event_in_tx(&mut transaction, &input).await?;
        transaction.commit().await?;
        Ok(event)
    }

    async fn append_event_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: &CreateDomainEvent,
    ) -> Result<DomainEvent> {
        if input.payload_json.len() > 64 * 1024 {
            return Err(DbError::Check(
                "domain event payload exceeds the 64 KiB limit".to_owned(),
            ));
        }
        if serde_json::from_str::<serde_json::Value>(&input.payload_json).is_err() {
            return Err(DbError::Check(
                "domain event payload must be valid JSON".to_owned(),
            ));
        }
        if !(0..=16).contains(&input.causation_depth) {
            return Err(DbError::Check(
                "domain event causation depth must be between 0 and 16".to_owned(),
            ));
        }
        let result = sqlx::query(
            "INSERT INTO domain_event (
                id, event_type, entity_type, entity_id, actor_type, actor_id,
                scope_type, scope_id, correlation_id, causation_id,
                causation_depth, dedupe_key, payload_json, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.event_type)
        .bind(&input.entity_type)
        .bind(&input.entity_id)
        .bind(&input.actor_type)
        .bind(input.actor_id.as_deref())
        .bind(&input.scope_type)
        .bind(&input.scope_id)
        .bind(&input.correlation_id)
        .bind(input.causation_id.as_deref())
        .bind(input.causation_depth)
        .bind(input.dedupe_key.as_deref())
        .bind(&input.payload_json)
        .bind(&input.created_at)
        .execute(&mut **transaction)
        .await;

        match result {
            Ok(_) => {}
            Err(error) if input.dedupe_key.is_some() && error.to_string().contains("UNIQUE") => {
                let event = sqlx::query("SELECT * FROM domain_event WHERE dedupe_key = ?")
                    .bind(input.dedupe_key.as_deref())
                    .fetch_optional(&mut **transaction)
                    .await?
                    .map(map_domain_event)
                    .transpose()?;
                let Some(event) = event else {
                    return Err(error.into());
                };
                if !event_semantics_match(input, &event) {
                    return Err(DbError::Check(
                        "domain event dedupe key conflicts with a different event".to_owned(),
                    ));
                }
                return Ok(event);
            }
            Err(error) => return Err(error.into()),
        }

        sqlx::query("SELECT * FROM domain_event WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .and_then(map_domain_event)
            .map_err(DbError::from)
    }

    async fn get_event(&self, id: &str) -> Result<Option<DomainEvent>> {
        sqlx::query("SELECT * FROM domain_event WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(|row| map_domain_event(row).map_err(DbError::from))
            .transpose()
    }

    async fn get_event_by_dedupe(&self, dedupe_key: &str) -> Result<Option<DomainEvent>> {
        sqlx::query("SELECT * FROM domain_event WHERE dedupe_key = ?")
            .bind(dedupe_key)
            .fetch_optional(&self.pool)
            .await?
            .map(|row| map_domain_event(row).map_err(DbError::from))
            .transpose()
    }

    async fn list_events_after(&self, sequence: i64, limit: i64) -> Result<Vec<DomainEvent>> {
        sqlx::query(
            "SELECT * FROM domain_event
             WHERE sequence > ?
             ORDER BY sequence ASC
             LIMIT ?",
        )
        .bind(sequence.max(0))
        .bind(limit.clamp(1, 500))
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| map_domain_event(row).map_err(DbError::from))
        .collect()
    }

    async fn get_consumer_cursor(
        &self,
        consumer_name: &str,
    ) -> Result<Option<EventConsumerCursor>> {
        sqlx::query("SELECT * FROM event_consumer_cursor WHERE consumer_name = ?")
            .bind(consumer_name)
            .fetch_optional(&self.pool)
            .await?
            .map(|row| map_event_consumer_cursor(row).map_err(DbError::from))
            .transpose()
    }

    async fn claim_event_batch(&self, input: ClaimDomainEvents) -> Result<Vec<DomainEvent>> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        sqlx::query(
            "INSERT INTO event_consumer_cursor (
                consumer_name, last_sequence, version, updated_at
             ) VALUES (?, 0, 1, ?)
             ON CONFLICT(consumer_name) DO NOTHING",
        )
        .bind(&input.consumer_name)
        .bind(&input.now)
        .execute(&mut *transaction)
        .await?;

        // Repair a cursor that may lag behind a receipt after a crash in an
        // older consumer implementation.  Advance only over a contiguous
        // prefix of receipts; an unprocessed gap remains authoritative and is
        // still claimed in sequence order below.
        let mut last_sequence = sqlx::query_scalar::<_, i64>(
            "SELECT last_sequence FROM event_consumer_cursor WHERE consumer_name = ?",
        )
        .bind(&input.consumer_name)
        .fetch_one(&mut *transaction)
        .await?;
        loop {
            let Some((next_sequence, next_id)) = sqlx::query_as::<_, (i64, String)>(
                "SELECT sequence, id FROM domain_event WHERE sequence = ?",
            )
            .bind(last_sequence + 1)
            .fetch_optional(&mut *transaction)
            .await?
            else {
                break;
            };
            let has_receipt = sqlx::query_scalar::<_, i64>(
                "SELECT 1 FROM event_projection_receipt
                 WHERE consumer_name = ? AND event_id = ?",
            )
            .bind(&input.consumer_name)
            .bind(&next_id)
            .fetch_optional(&mut *transaction)
            .await?
            .is_some();
            if !has_receipt {
                break;
            }
            sqlx::query(
                "UPDATE event_consumer_cursor
                 SET last_sequence = ?, version = version + 1, updated_at = ?
                 WHERE consumer_name = ? AND last_sequence = ?",
            )
            .bind(next_sequence)
            .bind(&input.now)
            .bind(&input.consumer_name)
            .bind(last_sequence)
            .execute(&mut *transaction)
            .await?;
            last_sequence = next_sequence;
        }

        let rows = sqlx::query(
            "SELECT event.*
             FROM domain_event AS event
             JOIN event_consumer_cursor AS cursor
               ON cursor.consumer_name = ?
             LEFT JOIN event_projection_receipt AS receipt
               ON receipt.consumer_name = cursor.consumer_name
              AND receipt.event_id = event.id
             WHERE event.sequence > cursor.last_sequence
               AND receipt.event_id IS NULL
             ORDER BY event.sequence ASC
             LIMIT ?",
        )
        .bind(&input.consumer_name)
        .bind(input.limit.clamp(1, 100))
        .fetch_all(&mut *transaction)
        .await?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let event = map_domain_event(row)?;
            let result = sqlx::query(
                "INSERT INTO event_processing_lease (
                    consumer_name, event_sequence, lease_owner, leased_until,
                    attempts, updated_at
                 ) VALUES (?, ?, ?, ?, 1, ?)
                 ON CONFLICT(consumer_name, event_sequence) DO UPDATE SET
                    lease_owner = excluded.lease_owner,
                    leased_until = excluded.leased_until,
                    attempts = event_processing_lease.attempts + 1,
                    updated_at = excluded.updated_at
                 WHERE event_processing_lease.leased_until <= ?
                    OR event_processing_lease.lease_owner = excluded.lease_owner",
            )
            .bind(&input.consumer_name)
            .bind(event.sequence)
            .bind(&input.lease_owner)
            .bind(&input.leased_until)
            .bind(&input.now)
            .bind(&input.now)
            .execute(&mut *transaction)
            .await?;
            if result.rows_affected() == 1 {
                events.push(event);
            } else {
                // A live lease held by another worker at the head of this
                // consumer's sequence creates a hard ordering barrier. Do
                // not hand out later rows that could never be checkpointed.
                break;
            }
        }

        transaction.commit().await?;
        Ok(events)
    }

    async fn complete_claimed_event(&self, input: CompleteDomainEvent) -> Result<bool> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let cursor = sqlx::query_scalar::<_, i64>(
            "SELECT last_sequence FROM event_consumer_cursor WHERE consumer_name = ?",
        )
        .bind(&input.consumer_name)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;

        if input.event_sequence > cursor + 1 {
            return Err(DbError::Check(
                "domain events must be checkpointed in sequence order".to_owned(),
            ));
        }

        let event = sqlx::query_as::<_, (String, Option<String>)>(
            "SELECT id, dedupe_key FROM domain_event WHERE sequence = ?",
        )
        .bind(input.event_sequence)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
        if event.0 != input.event_id {
            return Err(DbError::Check(
                "event id does not match the claimed sequence".to_owned(),
            ));
        }
        let expected_dedupe = event.1.unwrap_or_else(|| event.0.clone());
        if expected_dedupe != input.dedupe_key {
            return Err(DbError::Check(
                "event dedupe key does not match the claimed event".to_owned(),
            ));
        }

        // A receipt may exist after a worker was interrupted between the
        // projection write and cursor checkpoint in an older implementation.
        // Such a receipt is safe to use for cursor repair.  Otherwise only the
        // current lease owner may complete the event; a stale worker must not
        // acknowledge work leased to another worker.
        let receipt_exists = sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM event_projection_receipt
             WHERE consumer_name = ? AND event_id = ?",
        )
        .bind(&input.consumer_name)
        .bind(&input.event_id)
        .fetch_optional(&mut *transaction)
        .await?
        .is_some();
        if !receipt_exists {
            let lease_owner = sqlx::query_scalar::<_, String>(
                "SELECT lease_owner FROM event_processing_lease
                 WHERE consumer_name = ? AND event_sequence = ?",
            )
            .bind(&input.consumer_name)
            .bind(input.event_sequence)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
            if lease_owner != input.lease_owner {
                return Err(DbError::VersionConflict);
            }
        }

        let inserted = sqlx::query(
            "INSERT INTO event_projection_receipt (
                consumer_name, event_id, dedupe_key, processed_at
             ) VALUES (?, ?, ?, ?)
             ON CONFLICT(consumer_name, event_id) DO NOTHING",
        )
        .bind(&input.consumer_name)
        .bind(&input.event_id)
        .bind(&input.dedupe_key)
        .bind(&input.completed_at)
        .execute(&mut *transaction)
        .await?
        .rows_affected()
            == 1;

        if input.event_sequence == cursor + 1 {
            sqlx::query(
                "UPDATE event_consumer_cursor
                 SET last_sequence = ?, version = version + 1, updated_at = ?
                 WHERE consumer_name = ? AND last_sequence = ?",
            )
            .bind(input.event_sequence)
            .bind(&input.completed_at)
            .bind(&input.consumer_name)
            .bind(cursor)
            .execute(&mut *transaction)
            .await?;
        }

        sqlx::query(
            "DELETE FROM event_processing_lease
             WHERE consumer_name = ? AND event_sequence = ? AND lease_owner = ?",
        )
        .bind(&input.consumer_name)
        .bind(input.event_sequence)
        .bind(&input.lease_owner)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(inserted)
    }
}

fn event_semantics_match(input: &CreateDomainEvent, existing: &DomainEvent) -> bool {
    input.event_type == existing.event_type
        && input.entity_type == existing.entity_type
        && input.entity_id == existing.entity_id
        && input.actor_type == existing.actor_type
        && input.actor_id == existing.actor_id
        && input.scope_type == existing.scope_type
        && input.scope_id == existing.scope_id
        && input.correlation_id == existing.correlation_id
        && input.causation_id == existing.causation_id
        && input.causation_depth == existing.causation_depth
        && input.dedupe_key == existing.dedupe_key
        && input.payload_json == existing.payload_json
}

fn map_domain_event(row: SqliteRow) -> std::result::Result<DomainEvent, sqlx::Error> {
    Ok(DomainEvent {
        sequence: row.try_get("sequence")?,
        id: row.try_get("id")?,
        event_type: row.try_get("event_type")?,
        entity_type: row.try_get("entity_type")?,
        entity_id: row.try_get("entity_id")?,
        actor_type: row.try_get("actor_type")?,
        actor_id: row.try_get("actor_id")?,
        scope_type: row.try_get("scope_type")?,
        scope_id: row.try_get("scope_id")?,
        correlation_id: row.try_get("correlation_id")?,
        causation_id: row.try_get("causation_id")?,
        causation_depth: row.try_get("causation_depth")?,
        dedupe_key: row.try_get("dedupe_key")?,
        payload_json: row.try_get("payload_json")?,
        created_at: row.try_get("created_at")?,
    })
}

fn map_event_consumer_cursor(
    row: SqliteRow,
) -> std::result::Result<EventConsumerCursor, sqlx::Error> {
    Ok(EventConsumerCursor {
        consumer_name: row.try_get("consumer_name")?,
        last_sequence: row.try_get("last_sequence")?,
        version: row.try_get("version")?,
        updated_at: row.try_get("updated_at")?,
    })
}
