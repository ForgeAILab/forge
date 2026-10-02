//! Source-neutral health and poison persistence; no event table or cursor SQL.
use super::{policy::bounded_error, PoisonDecision, RetryPolicy};
use crate::Result;
use chrono::Utc;
use db::{now_rfc3339, SqliteDb};
use sqlx::{Sqlite, Transaction};
use std::{sync::Arc, time::Duration};

/// Identity provided by a work source (event sequence, step ID, etc.).
pub struct WorkItem<'a> {
    pub source_key: &'a str,
    pub item_type: &'a str,
}
/// A source with leases can supply its own per-item failure accounting.
pub struct FailureState<'a> {
    pub attempts: u32,
    pub first_failed_at: &'a str,
}

#[derive(Clone)]
pub struct WorkerHealth {
    db: Arc<SqliteDb>,
    name: String,
}
impl WorkerHealth {
    pub fn new(db: Arc<SqliteDb>, name: impl Into<String>) -> Self {
        Self {
            db,
            name: name.into(),
        }
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub async fn ensure_in_tx(&self, tx: &mut Transaction<'_, Sqlite>) -> Result<()> {
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO worker_health (worker_name, cursor_updated_at, created_at, updated_at)
            VALUES (?, ?, ?, ?) ON CONFLICT(worker_name) DO NOTHING",
        )
        .bind(&self.name)
        .bind(&now)
        .bind(&now)
        .bind(&now)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
    /// Must share the source's acknowledgement transaction. Returns a decision;
    /// the source acknowledges only DeadLettered, never Retry.
    pub async fn failure_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        item: WorkItem<'_>,
        policy: RetryPolicy,
        reason: &str,
        terminal: bool,
    ) -> Result<PoisonDecision> {
        self.ensure_in_tx(tx).await?;
        let now = now_rfc3339();
        let (key, previous, started): (Option<String>, i64, Option<String>) = sqlx::query_as(
            "SELECT retry_source_key, retry_attempts, retry_started_at FROM worker_health WHERE worker_name = ?")
            .bind(&self.name).fetch_one(&mut **tx).await?;
        let same = key.as_deref() == Some(item.source_key);
        let attempts = if same { previous } else { 0 } + i64::from(!terminal);
        let started = if same {
            started.unwrap_or_else(|| now.clone())
        } else {
            now.clone()
        };
        let reason = bounded_error(reason);
        if terminal
            || matches!(
                policy.decision(attempts as u32),
                PoisonDecision::DeadLettered
            )
        {
            self.dead_letter_in_tx(
                tx,
                item,
                FailureState {
                    attempts: attempts as u32,
                    first_failed_at: &started,
                },
                &reason,
            )
            .await?;
            self.clear_pending_in_tx(tx).await?;
            self.error_in_tx(tx, &reason).await?;
            return Ok(PoisonDecision::DeadLettered);
        }
        let delay = policy.delay(attempts as u32);
        sqlx::query("UPDATE worker_health SET retry_source_key = ?, retry_attempts = ?,
            retry_not_before = ?, retry_started_at = ?, last_error = ?, last_error_at = ?, updated_at = ?
            WHERE worker_name = ?")
            .bind(item.source_key).bind(attempts).bind(not_before(delay)).bind(started)
            .bind(reason).bind(&now).bind(&now).bind(&self.name).execute(&mut **tx).await?;
        Ok(PoisonDecision::Retry(delay))
    }
    /// Source-neutral quarantine persistence. The source owns acknowledgement
    /// and, when needed, lease attempts; callers share their transaction here.
    pub async fn dead_letter_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        item: WorkItem<'_>,
        failure: FailureState<'_>,
        reason: &str,
    ) -> Result<()> {
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO worker_dead_letter (worker_name, source_key, item_type, attempts,
            last_error, first_failed_at, last_failed_at, dead_lettered_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(worker_name, source_key) DO NOTHING",
        )
        .bind(&self.name)
        .bind(item.source_key)
        .bind(item.item_type)
        .bind(i64::from(failure.attempts))
        .bind(bounded_error(reason))
        .bind(failure.first_failed_at)
        .bind(&now)
        .bind(&now)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
    pub async fn defer_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        key: &str,
        after: Duration,
        reason: &str,
    ) -> Result<()> {
        self.ensure_in_tx(tx).await?;
        let now = now_rfc3339();
        sqlx::query(
            "UPDATE worker_health SET deferred_since = CASE WHEN deferred_source_key = ?
            THEN deferred_since ELSE ? END, deferred_source_key = ?, deferred_reason = ?,
            defer_not_before = ?, updated_at = ? WHERE worker_name = ?",
        )
        .bind(key)
        .bind(&now)
        .bind(key)
        .bind(bounded_error(reason))
        .bind(not_before(after))
        .bind(&now)
        .bind(&self.name)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
    pub async fn clear_pending_in_tx(&self, tx: &mut Transaction<'_, Sqlite>) -> Result<()> {
        sqlx::query("UPDATE worker_health SET retry_source_key = NULL, retry_attempts = 0,
            retry_not_before = NULL, retry_started_at = NULL, deferred_source_key = NULL,
            deferred_reason = NULL, deferred_since = NULL, defer_not_before = NULL WHERE worker_name = ?")
            .bind(&self.name).execute(&mut **tx).await?;
        Ok(())
    }
    pub async fn success_in_tx(&self, tx: &mut Transaction<'_, Sqlite>) -> Result<()> {
        let now = now_rfc3339();
        sqlx::query("UPDATE worker_health SET last_error = NULL, last_error_at = NULL,
            last_success_at = ?, updated_at = ?, retry_source_key = NULL, retry_attempts = 0,
            retry_not_before = NULL, retry_started_at = NULL, deferred_source_key = NULL,
            deferred_reason = NULL, deferred_since = NULL, defer_not_before = NULL WHERE worker_name = ?")
            .bind(&now).bind(&now).bind(&self.name).execute(&mut **tx).await?;
        Ok(())
    }
    pub async fn error_in_tx(&self, tx: &mut Transaction<'_, Sqlite>, reason: &str) -> Result<()> {
        let now = now_rfc3339();
        sqlx::query("UPDATE worker_health SET last_error = ?, last_error_at = ?, updated_at = ? WHERE worker_name = ?")
            .bind(bounded_error(reason)).bind(&now).bind(&now).bind(&self.name).execute(&mut **tx).await?;
        Ok(())
    }
    /// Recreates missing health independently of event-source initialization.
    pub async fn report_error(&self, reason: &str) -> Result<()> {
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        self.ensure_in_tx(&mut tx).await?;
        self.error_in_tx(&mut tx, reason).await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn record_restart(&self, reason: &str) -> Result<()> {
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        self.ensure_in_tx(&mut tx).await?;
        self.error_in_tx(&mut tx, reason).await?;
        sqlx::query(
            "UPDATE worker_health SET restart_count = restart_count + 1 WHERE worker_name = ?",
        )
        .bind(&self.name)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
}
fn not_before(delay: Duration) -> String {
    Utc::now()
        .checked_add_signed(chrono::Duration::from_std(delay).unwrap_or(chrono::Duration::MAX))
        .unwrap_or(chrono::DateTime::<Utc>::MAX_UTC)
        .to_rfc3339()
}
