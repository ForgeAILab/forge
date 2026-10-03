use super::domain_event::map_domain_event;
use super::*;
use sqlx::SqliteConnection;

#[derive(Debug, Clone)]
pub struct DeadLetter {
    pub id: String,
    pub worker_name: String,
    pub source_key: String,
    pub item_type: String,
    pub attempts: i64,
    pub last_error: String,
    pub error_kind: String,
    pub first_failed_at: String,
    pub last_failed_at: String,
    pub dead_lettered_at: String,
    pub version: i64,
    pub resolved_at: Option<String>,
    pub resolved_by: Option<String>,
    pub resolution: Option<String>,
    pub resolution_reason: Option<String>,
    pub event_created_at: Option<String>,
    pub events_since: i64,
}
impl DeadLetter {
    pub fn replayable(&self) -> bool {
        self.source_key
            .parse::<i64>()
            .is_ok_and(|sequence| sequence > 0 && sequence.to_string() == self.source_key)
    }
    pub fn event_sequence(&self) -> Option<i64> {
        self.source_key.parse().ok().or_else(|| {
            self.source_key
                .strip_prefix("event:")?
                .split(':')
                .next()?
                .parse()
                .ok()
        })
    }
}
pub struct DeadLetterPage {
    pub items: Vec<DeadLetter>,
    pub next_cursor: Option<String>,
}
pub struct DeadLetterAction<'a> {
    pub actor_id: &'a str,
    pub outcome: &'a str,
    pub reason: Option<&'a str>,
    pub error_kind: Option<&'a str>,
}
#[derive(Serialize, Deserialize)]
struct Cursor {
    time: String,
    id: String,
    consumer: Option<String>,
    resolved: bool,
}
pub(super) fn map_dead_letter(row: SqliteRow) -> Result<DeadLetter> {
    Ok(DeadLetter {
        id: row.try_get("id")?,
        worker_name: row.try_get("worker_name")?,
        source_key: row.try_get("source_key")?,
        item_type: row.try_get("item_type")?,
        attempts: row.try_get("attempts")?,
        last_error: row.try_get("last_error")?,
        error_kind: row.try_get("error_kind")?,
        first_failed_at: row.try_get("first_failed_at")?,
        last_failed_at: row.try_get("last_failed_at")?,
        dead_lettered_at: row.try_get("dead_lettered_at")?,
        version: row.try_get("version")?,
        resolved_at: row.try_get("resolved_at")?,
        resolved_by: row.try_get("resolved_by")?,
        resolution: row.try_get("resolution")?,
        resolution_reason: row.try_get("resolution_reason")?,
        event_created_at: None,
        events_since: 0,
    })
}
impl SqliteDb {
    pub async fn get_dead_letter(&self, id: &str) -> Result<DeadLetter> {
        let mut connection = self.pool.acquire().await?;
        Self::read_dead_letter(&mut connection, id).await
    }
    pub async fn get_dead_letter_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        id: &str,
    ) -> Result<DeadLetter> {
        Self::read_dead_letter(tx, id).await
    }
    async fn read_dead_letter(connection: &mut SqliteConnection, id: &str) -> Result<DeadLetter> {
        let row = sqlx::query("SELECT * FROM worker_dead_letter WHERE id = ?")
            .bind(id)
            .fetch_optional(&mut *connection)
            .await?
            .ok_or(DbError::NotFound)?;
        let mut row = map_dead_letter(row)?;
        Self::dead_letter_context(connection, &mut row).await?;
        Ok(row)
    }
    pub(super) async fn dead_letter_context(
        connection: &mut SqliteConnection,
        row: &mut DeadLetter,
    ) -> Result<()> {
        let Some(sequence) = row.event_sequence() else {
            return Ok(());
        };
        let (created_at, cursor, subscription): (Option<String>, Option<i64>, Option<String>) =
            sqlx::query_as(
                "SELECT e.created_at, c.last_sequence, h.subscription_json
            FROM (SELECT ? AS sequence, ? AS worker_name) source
            LEFT JOIN domain_event e ON e.sequence = source.sequence
            LEFT JOIN event_consumer_cursor c ON c.consumer_name = source.worker_name
            LEFT JOIN worker_health h ON h.worker_name = source.worker_name",
            )
            .bind(sequence)
            .bind(&row.worker_name)
            .fetch_one(&mut *connection)
            .await?;
        row.event_created_at = created_at;
        let cursor = cursor.unwrap_or(0);
        if cursor <= sequence {
            return Ok(());
        }
        let subscription: EventSubscription = subscription
            .map(|value| {
                serde_json::from_str(&value)
                    .map_err(|_| DbError::Check("invalid worker subscription".into()))
            })
            .transpose()?
            .unwrap_or(EventSubscription::All)
            .normalized();
        let mut query = sqlx::QueryBuilder::<Sqlite>::new(
            "SELECT COUNT(*) FROM domain_event WHERE sequence > ",
        );
        query
            .push_bind(sequence)
            .push(" AND sequence <= ")
            .push_bind(cursor);
        match &subscription {
            EventSubscription::All => {}
            EventSubscription::Exact(types) => {
                query.push(" AND event_type IN (");
                let mut values = query.separated(", ");
                for kind in types {
                    values.push_bind(kind);
                }
                values.push_unseparated(")");
            }
            EventSubscription::Prefix(prefixes) => {
                query.push(" AND (0");
                for prefix in prefixes {
                    query.push(" OR (event_type >= ").push_bind(prefix);
                    if let Some(upper) = super::domain_event::prefix_upper_bound(prefix) {
                        query.push(" AND event_type < ").push_bind(upper);
                    }
                    query.push(")");
                }
                query.push(")");
            }
        }
        row.events_since = query.build_query_scalar().fetch_one(connection).await?;
        Ok(())
    }
    pub async fn dead_letter_event(&self, sequence: i64) -> Result<Option<DomainEvent>> {
        sqlx::query("SELECT * FROM domain_event WHERE sequence = ?")
            .bind(sequence)
            .fetch_optional(&self.pool)
            .await?
            .map(|row| map_domain_event(row).map_err(DbError::from))
            .transpose()
    }
    pub async fn list_dead_letters(
        &self,
        consumer: Option<&str>,
        resolved: bool,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<DeadLetterPage> {
        let cursor: Option<Cursor> = cursor
            .map(|value| {
                let bytes = URL_SAFE_NO_PAD
                    .decode(value)
                    .map_err(|_| DbError::InvalidCursor)?;
                let cursor: Cursor =
                    serde_json::from_slice(&bytes).map_err(|_| DbError::InvalidCursor)?;
                if cursor.consumer.as_deref() != consumer || cursor.resolved != resolved {
                    return Err(DbError::InvalidCursor);
                }
                Ok(cursor)
            })
            .transpose()?;
        let limit = limit.clamp(1, 100);
        let time_column = if resolved {
            "resolved_at"
        } else {
            "dead_lettered_at"
        };
        let mut query = sqlx::QueryBuilder::<Sqlite>::new(
            "SELECT * FROM worker_dead_letter WHERE resolved_at IS ",
        );
        query.push(if resolved { "NOT NULL" } else { "NULL" });
        if let Some(consumer) = consumer {
            query.push(" AND worker_name = ").push_bind(consumer);
        }
        if let Some(cursor) = &cursor {
            query
                .push(" AND (")
                .push(time_column)
                .push(", id) < (")
                .push_bind(&cursor.time)
                .push(", ")
                .push_bind(&cursor.id)
                .push(")");
        }
        query
            .push(" ORDER BY ")
            .push(time_column)
            .push(" DESC, id DESC LIMIT ")
            .push_bind((limit + 1) as i64);
        let mut items = query
            .build()
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(map_dead_letter)
            .collect::<Result<Vec<_>>>()?;
        let has_more = items.len() > limit;
        items.truncate(limit);
        let mut connection = self.pool.acquire().await?;
        for row in &mut items {
            Self::dead_letter_context(&mut connection, row).await?;
        }
        let next_cursor = if has_more {
            items
                .last()
                .map(|row| {
                    serde_json::to_vec(&Cursor {
                        time: if resolved {
                            row.resolved_at
                                .clone()
                                .expect("resolved rows have a timestamp")
                        } else {
                            row.dead_lettered_at.clone()
                        },
                        id: row.id.clone(),
                        consumer: consumer.map(str::to_owned),
                        resolved,
                    })
                    .map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
                    .map_err(|_| DbError::InvalidCursor)
                })
                .transpose()?
        } else {
            None
        };
        Ok(DeadLetterPage { items, next_cursor })
    }
    /// Fence before calling the consumer's commit. The writer transaction keeps
    /// the row fenced until effects and the disposition commit together.
    pub async fn fence_dead_letter_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        row: &DeadLetter,
    ) -> Result<()> {
        let changed = sqlx::query("UPDATE worker_dead_letter SET version = version + 1 WHERE id = ? AND version = ? AND resolved_at IS NULL")
            .bind(&row.id).bind(row.version).execute(&mut **tx).await?.rows_affected();
        if changed != 1 {
            return Err(DbError::VersionConflict);
        }
        Ok(())
    }
    /// Caller has fenced the row in this transaction. Failure never creates
    /// worker retry state; only another explicit operator action can retry it.
    pub async fn finish_dead_letter_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        row: &DeadLetter,
        action: DeadLetterAction<'_>,
    ) -> Result<DeadLetter> {
        let now = crate::now_rfc3339();
        if action.outcome == "replay_failed" {
            sqlx::query("UPDATE worker_dead_letter SET attempts = attempts + 1, last_error = ?, error_kind = ?, last_failed_at = ? WHERE id = ?")
                .bind(action.reason.unwrap_or("replay failed")).bind(action.error_kind.unwrap_or("failure")).bind(&now).bind(&row.id).execute(&mut **tx).await?;
        } else {
            sqlx::query("UPDATE worker_dead_letter SET attempts = attempts + ?, resolved_at = ?, resolved_by = ?, resolution = ?, resolution_reason = ? WHERE id = ?")
                .bind(i64::from(action.outcome != "dismissed")).bind(&now).bind(action.actor_id).bind(action.outcome).bind(action.reason).bind(&row.id).execute(&mut **tx).await?;
        }
        sqlx::query("INSERT INTO worker_dead_letter_action (id, dead_letter_id, actor_id, occurred_at, outcome, reason, version) VALUES (?, ?, ?, ?, ?, ?, ?)")
            .bind(crate::new_uuid_v4()).bind(&row.id).bind(action.actor_id).bind(&now).bind(action.outcome).bind(action.reason).bind(row.version + 1).execute(&mut **tx).await?;
        self.get_dead_letter_in_tx(tx, &row.id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn context_preserves_progress_without_source_and_matches_current_subscription() {
        let pool = crate::create_sqlite_pool("sqlite::memory:").await.unwrap();
        crate::run_migrations(&pool).await.unwrap();
        let db = Arc::new(SqliteDb::new(pool));
        let mut events = Vec::new();
        for event_type in ["wanted", "ignored", "wanted", "wanted.later"] {
            events.push(
                db.append_event(CreateDomainEvent {
                    id: crate::new_uuid_v4(),
                    event_type: event_type.into(),
                    entity_type: "test".into(),
                    entity_id: "test".into(),
                    actor_type: "system".into(),
                    actor_id: None,
                    scope_type: "system".into(),
                    scope_id: "test".into(),
                    correlation_id: "test".into(),
                    causation_id: None,
                    causation_depth: 0,
                    dedupe_key: None,
                    payload_json: "{}".into(),
                    created_at: crate::now_rfc3339(),
                })
                .await
                .unwrap(),
            );
        }
        let health = crate::WorkerHealth::new(db.clone(), "metadata");
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        db.initialize_event_worker_in_tx(&mut tx, &health, &EventSubscription::All)
            .await
            .unwrap();
        health
            .dead_letter_in_tx(
                &mut tx,
                crate::WorkItem {
                    source_key: &events[0].sequence.to_string(),
                    item_type: "wanted",
                },
                crate::FailureState {
                    attempts: 8,
                    first_failed_at: &crate::now_rfc3339(),
                },
                "failure",
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
        sqlx::query(
            "UPDATE event_consumer_cursor SET last_sequence = ? WHERE consumer_name = 'metadata'",
        )
        .bind(events[3].sequence)
        .execute(db.pool())
        .await
        .unwrap();
        let id: String =
            sqlx::query_scalar("SELECT id FROM worker_dead_letter WHERE worker_name = 'metadata'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(db.get_dead_letter(&id).await.unwrap().events_since, 3);
        sqlx::query("DELETE FROM domain_event WHERE sequence = ?")
            .bind(events[0].sequence)
            .execute(db.pool())
            .await
            .unwrap();
        let missing = db.get_dead_letter(&id).await.unwrap();
        assert!(missing.event_created_at.is_none());
        assert_eq!(missing.events_since, 3);
        for (subscription, count) in [
            (EventSubscription::Exact(vec!["wanted".into()]), 1),
            (EventSubscription::Prefix(vec!["wanted".into()]), 2),
        ] {
            sqlx::query(
                "UPDATE worker_health SET subscription_json = ? WHERE worker_name = 'metadata'",
            )
            .bind(serde_json::to_string(&subscription).unwrap())
            .execute(db.pool())
            .await
            .unwrap();
            assert_eq!(
                db.list_dead_letters(Some("metadata"), false, None, 5)
                    .await
                    .unwrap()
                    .items[0]
                    .events_since,
                count
            );
        }
    }
}
