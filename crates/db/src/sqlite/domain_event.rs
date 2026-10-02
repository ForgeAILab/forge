use super::*;
use crate::WorkerHealth;

/// Gap-tolerant legacy lookup; sequence numbers need not be contiguous.
pub(super) async fn next_existing_sequence(
    tx: &mut Transaction<'_, Sqlite>,
    cursor: i64,
) -> Result<Option<i64>> {
    next_subscribed_sequence(tx, cursor, &EventSubscription::All).await
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "values", rename_all = "snake_case")]
pub enum EventSubscription {
    Exact(Vec<String>),
    Prefix(Vec<String>),
    All,
}
impl EventSubscription {
    /// Prevent duplicate counts and redundant overlapping prefix scans.
    pub fn normalized(&self) -> Self {
        match self {
            Self::All => Self::All,
            Self::Exact(values) => {
                let mut values = values.clone();
                values.sort();
                values.dedup();
                Self::Exact(values)
            }
            Self::Prefix(values) => {
                let mut values = values.clone();
                values.sort();
                values.dedup();
                if values.iter().any(String::is_empty) {
                    return Self::All;
                }
                let mut prefixes = Vec::<String>::new();
                for value in values {
                    if !prefixes.iter().any(|p| value.starts_with(p)) {
                        prefixes.push(value);
                    }
                }
                Self::Prefix(prefixes)
            }
        }
    }
}

/// Binary UTF-8 range successor, including U+D7FF/U+E000 and U+10FFFF edges.
pub(super) fn prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut prefix = prefix.to_owned();
    while let Some(last) = prefix.pop() {
        let mut next = u32::from(last) + 1;
        if next == 0xD800 {
            next = 0xE000;
        }
        if let Some(next) = char::from_u32(next) {
            prefix.push(next);
            return Some(prefix);
        }
    }
    None
}
fn push_type_range<'a>(query: &mut sqlx::QueryBuilder<'a, Sqlite>, value: &'a str, prefix: bool) {
    query
        .push(if prefix {
            "event_type >= "
        } else {
            "event_type = "
        })
        .push_bind(value);
    if prefix {
        if let Some(upper) = prefix_upper_bound(value) {
            query.push(" AND event_type < ").push_bind(upper);
        }
    }
}
fn push_next_sequence<'a>(
    query: &mut sqlx::QueryBuilder<'a, Sqlite>,
    cursor: i64,
    subscription: &'a EventSubscription,
    head: Option<i64>,
) {
    match subscription {
        EventSubscription::All => {
            query
                .push("SELECT sequence FROM domain_event WHERE sequence > ")
                .push_bind(cursor);
            if let Some(head) = head {
                query.push(" AND sequence <= ").push_bind(head);
            }
            query.push(" ORDER BY sequence LIMIT 1");
        }
        EventSubscription::Exact(values) | EventSubscription::Prefix(values) => {
            if values.is_empty() {
                query.push("SELECT NULL");
                return;
            }
            let prefix = matches!(subscription, EventSubscription::Prefix(_));
            query.push("SELECT MIN(sequence) FROM (");
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    query.push(" UNION ALL ");
                }
                query.push("SELECT MIN(sequence) AS sequence FROM domain_event INDEXED BY idx_domain_event_type_sequence WHERE ");
                push_type_range(query, value, prefix);
                query.push(" AND sequence > ").push_bind(cursor);
                if let Some(head) = head {
                    query.push(" AND sequence <= ").push_bind(head);
                }
            }
            query.push(")");
        }
    }
}
async fn next_subscribed_sequence(
    tx: &mut Transaction<'_, Sqlite>,
    cursor: i64,
    subscription: &EventSubscription,
) -> Result<Option<i64>> {
    let mut query = sqlx::QueryBuilder::<Sqlite>::new("SELECT (");
    push_next_sequence(&mut query, cursor, subscription, None);
    query.push(")");
    Ok(query
        .build_query_scalar::<Option<i64>>()
        .fetch_one(&mut **tx)
        .await?)
}

impl SqliteDb {
    pub async fn domain_event_head(&self) -> Result<i64> {
        Ok(
            sqlx::query_scalar("SELECT COALESCE(MAX(sequence), 0) FROM domain_event")
                .fetch_one(&self.pool)
                .await?,
        )
    }
    pub async fn next_subscribed_domain_event(
        &self,
        cursor: i64,
        subscription: &EventSubscription,
    ) -> Result<Option<DomainEvent>> {
        self.subscribed_event_before(cursor, subscription, None)
            .await
    }
    async fn subscribed_event_before(
        &self,
        cursor: i64,
        subscription: &EventSubscription,
        head: Option<i64>,
    ) -> Result<Option<DomainEvent>> {
        let mut query =
            sqlx::QueryBuilder::<Sqlite>::new("SELECT * FROM domain_event WHERE sequence = (");
        push_next_sequence(&mut query, cursor, subscription, head);
        query.push(")");
        query
            .build()
            .fetch_optional(&self.pool)
            .await?
            .map(|row| map_domain_event(row).map_err(DbError::from))
            .transpose()
    }
    /// Read the head first, then search only up to that head. A later append
    /// cannot be accidentally included in a lazy ignored/skipped advance.
    pub async fn scan_subscribed_domain_event(
        &self,
        cursor: i64,
        subscription: &EventSubscription,
    ) -> Result<(Option<DomainEvent>, i64)> {
        let head = self.domain_event_head().await?;
        Ok((
            self.subscribed_event_before(cursor, subscription, Some(head))
                .await?,
            head,
        ))
    }
    pub async fn next_subscribed_domain_event_sequence_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        cursor: i64,
        subscription: &EventSubscription,
    ) -> Result<Option<i64>> {
        next_subscribed_sequence(tx, cursor, subscription).await
    }
    pub async fn ensure_domain_event_cursor_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        name: &str,
        now: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO event_consumer_cursor (consumer_name, last_sequence, version, updated_at)
            VALUES (?, 0, 1, ?) ON CONFLICT(consumer_name) DO NOTHING",
        )
        .bind(name)
        .bind(now)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
    pub async fn event_worker_is_initialized(
        &self,
        name: &str,
        subscription: &EventSubscription,
    ) -> Result<bool> {
        let json = serde_json::to_string(subscription)
            .map_err(|_| DbError::Check("invalid event subscription".into()))?;
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(SELECT 1 FROM worker_health AS health
            JOIN event_consumer_cursor AS cursor ON health.worker_name = cursor.consumer_name
            WHERE worker_name = ? AND subscription_json = ?)",
        )
        .bind(name)
        .bind(json)
        .fetch_one(&self.pool)
        .await?
            != 0)
    }
    pub async fn initialize_event_worker_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        health: &WorkerHealth,
        subscription: &EventSubscription,
    ) -> Result<()> {
        let json = serde_json::to_string(subscription)
            .map_err(|_| DbError::Check("invalid event subscription".into()))?;
        self.ensure_domain_event_cursor_in_tx(tx, health.name(), &crate::now_rfc3339())
            .await?;
        health.ensure_in_tx(tx).await?;
        sqlx::query("UPDATE worker_health SET subscription_json = ? WHERE worker_name = ?")
            .bind(json)
            .bind(health.name())
            .execute(&mut **tx)
            .await?;
        Ok(())
    }
    pub async fn validate_event_worker_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        name: &str,
        expected: i64,
        scanned: i64,
        event_sequence: i64,
        subscription: &EventSubscription,
    ) -> Result<()> {
        let cursor: i64 = sqlx::query_scalar(
            "SELECT last_sequence FROM event_consumer_cursor WHERE consumer_name = ?",
        )
        .bind(name)
        .fetch_one(&mut **tx)
        .await?;
        if cursor != expected
            || next_subscribed_sequence(tx, scanned.max(cursor), subscription).await?
                != Some(event_sequence)
        {
            return Err(DbError::VersionConflict);
        }
        Ok(())
    }
    pub async fn advance_domain_event_cursor_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        name: &str,
        expected: i64,
        sequence: i64,
        now: &str,
    ) -> Result<()> {
        let result = sqlx::query(
            "UPDATE event_consumer_cursor SET last_sequence = ?, version = version + 1,
            updated_at = ? WHERE consumer_name = ? AND last_sequence = ?",
        )
        .bind(sequence)
        .bind(now)
        .bind(name)
        .bind(expected)
        .execute(&mut **tx)
        .await?;
        if result.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        Ok(())
    }
    /// A lazy event checkpoint only completes pending state belonging to an
    /// event inside the classified/filtered prefix, not a later failing item.
    pub async fn complete_event_scan_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        health: &WorkerHealth,
        through: i64,
    ) -> Result<()> {
        let keys: (Option<String>, Option<String>) = sqlx::query_as(
            "SELECT retry_source_key, deferred_source_key FROM worker_health WHERE worker_name = ?",
        )
        .bind(health.name())
        .fetch_one(&mut **tx)
        .await?;
        for key in [keys.0, keys.1].into_iter().flatten() {
            if key.parse::<i64>().is_ok_and(|sequence| sequence <= through) {
                health.success_in_tx(tx, &key).await?;
            }
        }
        Ok(())
    }
    /// Counts only matching index ranges. No json_each or event payload read.
    pub(super) async fn subscribed_backlog(
        &self,
        cursor: i64,
        subscription: &EventSubscription,
    ) -> Result<(i64, Option<String>)> {
        let subscription = subscription.normalized();
        let mut lag = 0i64;
        let mut first = None::<i64>;
        match &subscription {
            EventSubscription::All => {
                lag = sqlx::query_scalar("SELECT COUNT(*) FROM domain_event WHERE sequence > ?")
                    .bind(cursor)
                    .fetch_one(&self.pool)
                    .await?;
                first =
                    sqlx::query_scalar("SELECT MIN(sequence) FROM domain_event WHERE sequence > ?")
                        .bind(cursor)
                        .fetch_one(&self.pool)
                        .await?;
            }
            EventSubscription::Exact(values) | EventSubscription::Prefix(values) => {
                let prefix = matches!(subscription, EventSubscription::Prefix(_));
                for value in values {
                    let mut query = sqlx::QueryBuilder::<Sqlite>::new("SELECT COUNT(*), MIN(sequence) FROM domain_event INDEXED BY idx_domain_event_type_sequence WHERE ");
                    push_type_range(&mut query, value, prefix);
                    query.push(" AND sequence > ").push_bind(cursor);
                    let (count, sequence): (i64, Option<i64>) =
                        query.build_query_as().fetch_one(&self.pool).await?;
                    lag += count;
                    if let Some(sequence) = sequence {
                        first = Some(first.map_or(sequence, |old| old.min(sequence)));
                    }
                }
            }
        }
        let created_at = if let Some(sequence) = first {
            sqlx::query_scalar("SELECT created_at FROM domain_event WHERE sequence = ?")
                .bind(sequence)
                .fetch_optional(&self.pool)
                .await?
        } else {
            None
        };
        Ok((lag, created_at))
    }
}

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

    async fn get_consumer_cutover(
        &self,
        consumer_name: &str,
    ) -> Result<Option<EventConsumerCutover>> {
        sqlx::query("SELECT * FROM event_consumer_cutover WHERE consumer_name = ?")
            .bind(consumer_name)
            .fetch_optional(&self.pool)
            .await?
            .map(|row| map_event_consumer_cutover(row).map_err(DbError::from))
            .transpose()
    }

    async fn claim_event_batch(&self, input: ClaimDomainEvents) -> Result<Vec<DomainEvent>> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        self.ensure_domain_event_cursor_in_tx(&mut transaction, &input.consumer_name, &input.now)
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
            let Some(next_sequence) =
                next_existing_sequence(&mut transaction, last_sequence).await?
            else {
                break;
            };
            let next_id =
                sqlx::query_scalar::<_, String>("SELECT id FROM domain_event WHERE sequence = ?")
                    .bind(next_sequence)
                    .fetch_one(&mut *transaction)
                    .await?;
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
            self.advance_domain_event_cursor_in_tx(
                &mut transaction,
                &input.consumer_name,
                last_sequence,
                next_sequence,
                &input.now,
            )
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

        if input.event_sequence
            > next_existing_sequence(&mut transaction, cursor)
                .await?
                .unwrap_or(i64::MAX)
        {
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

        if Some(input.event_sequence) == next_existing_sequence(&mut transaction, cursor).await? {
            self.advance_domain_event_cursor_in_tx(
                &mut transaction,
                &input.consumer_name,
                cursor,
                input.event_sequence,
                &input.completed_at,
            )
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

pub(super) fn map_domain_event(row: SqliteRow) -> std::result::Result<DomainEvent, sqlx::Error> {
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

fn map_event_consumer_cutover(
    row: SqliteRow,
) -> std::result::Result<EventConsumerCutover, sqlx::Error> {
    Ok(EventConsumerCutover {
        consumer_name: row.try_get("consumer_name")?,
        cutover_sequence: row.try_get("cutover_sequence")?,
        reason: row.try_get("reason")?,
        created_at: row.try_get("created_at")?,
    })
}
