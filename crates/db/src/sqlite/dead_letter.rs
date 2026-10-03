use super::domain_event::map_domain_event;
use super::*;

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
}
impl DeadLetter {
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
    })
}
impl SqliteDb {
    pub async fn get_dead_letter(&self, id: &str) -> Result<DeadLetter> {
        let row = sqlx::query("SELECT * FROM worker_dead_letter WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(DbError::NotFound)?;
        map_dead_letter(row)
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
        let mut query = sqlx::QueryBuilder::<Sqlite>::new(
            "SELECT * FROM worker_dead_letter WHERE resolved_at IS ",
        );
        query.push(if resolved { "NOT NULL" } else { "NULL" });
        if let Some(consumer) = consumer {
            query.push(" AND worker_name = ").push_bind(consumer);
        }
        if let Some(cursor) = &cursor {
            query
                .push(" AND (dead_lettered_at, id) < (")
                .push_bind(&cursor.time)
                .push(", ")
                .push_bind(&cursor.id)
                .push(")");
        }
        query
            .push(" ORDER BY dead_lettered_at DESC, id DESC LIMIT ")
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
        let next_cursor = if has_more {
            items
                .last()
                .map(|row| {
                    serde_json::to_vec(&Cursor {
                        time: row.dead_lettered_at.clone(),
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
        let row = sqlx::query("SELECT * FROM worker_dead_letter WHERE id = ?")
            .bind(&row.id)
            .fetch_one(&mut **tx)
            .await?;
        map_dead_letter(row)
    }
}
