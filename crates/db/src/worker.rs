//! Source-neutral health and poison persistence; no event table or cursor SQL.
use crate::now_rfc3339;
use crate::{Result, SqliteDb};
use chrono::Utc;
use sqlx::{Row, Sqlite, Transaction};
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
            "INSERT INTO worker_health (worker_name, created_at, updated_at)
            VALUES (?, ?, ?) ON CONFLICT(worker_name) DO NOTHING",
        )
        .bind(&self.name)
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
            let source_key = item.source_key;
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
            sqlx::query("UPDATE worker_dead_letter SET error_kind = ? WHERE worker_name = ? AND source_key = ?")
                .bind(if terminal { "terminal" } else { "failure" }).bind(&self.name).bind(source_key).execute(&mut **tx).await?;
            self.clear_pending_in_tx(tx).await?;
            self.clear_kind_in_tx(tx, HealthErrorKind::Item).await?;
            return Ok(PoisonDecision::DeadLettered);
        }
        let delay = policy.delay(attempts as u32);
        sqlx::query("UPDATE worker_health SET retry_source_key = ?, retry_attempts = ?,
            retry_not_before = ?, retry_started_at = ?, item_error = ?, item_error_at = ?, item_error_kind = 'failure', updated_at = ?,
            deferred_source_key = NULL, deferred_reason = NULL, deferred_since = NULL, defer_not_before = NULL
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
            "INSERT INTO worker_dead_letter (id, worker_name, source_key, item_type, attempts,
            last_error, first_failed_at, last_failed_at, dead_lettered_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(worker_name, source_key) DO NOTHING",
        )
        .bind(crate::new_uuid_v4())
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
            defer_not_before = ?, updated_at = ?, retry_not_before = NULL WHERE worker_name = ?",
        )
        .bind(key)
        .bind(&now)
        .bind(key)
        .bind(bounded_error(reason))
        .bind(not_before(clamp_worker_deferral(after)))
        .bind(&now)
        .bind(&self.name)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
    pub async fn clear_pending_in_tx(&self, tx: &mut Transaction<'_, Sqlite>) -> Result<()> {
        sqlx::query(
            "UPDATE worker_health SET retry_source_key = NULL, retry_attempts = 0,
            retry_not_before = NULL, retry_started_at = NULL, deferred_source_key = NULL,
            deferred_reason = NULL, deferred_since = NULL, defer_not_before = NULL,
            item_error = NULL, item_error_at = NULL, item_error_kind = NULL WHERE worker_name = ?",
        )
        .bind(&self.name)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
    /// Completion of another source/item cannot erase the active item's fault.
    pub async fn success_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        source_key: &str,
    ) -> Result<()> {
        let now = now_rfc3339();
        sqlx::query("UPDATE worker_health SET last_success_at = ?, updated_at = ?, after_commit_error = NULL, after_commit_error_at = NULL, after_commit_error_kind = NULL, item_error = CASE WHEN retry_source_key = ? THEN NULL ELSE item_error END, item_error_kind = CASE WHEN retry_source_key = ? THEN NULL ELSE item_error_kind END, item_error_at = CASE WHEN retry_source_key = ? THEN NULL ELSE item_error_at END, retry_attempts = CASE WHEN retry_source_key = ? THEN 0 ELSE retry_attempts END, retry_not_before = CASE WHEN retry_source_key = ? THEN NULL ELSE retry_not_before END, retry_started_at = CASE WHEN retry_source_key = ? THEN NULL ELSE retry_started_at END, retry_source_key = CASE WHEN retry_source_key = ? THEN NULL ELSE retry_source_key END, deferred_reason = CASE WHEN deferred_source_key = ? THEN NULL ELSE deferred_reason END, deferred_since = CASE WHEN deferred_source_key = ? THEN NULL ELSE deferred_since END, defer_not_before = CASE WHEN deferred_source_key = ? THEN NULL ELSE defer_not_before END, deferred_source_key = CASE WHEN deferred_source_key = ? THEN NULL ELSE deferred_source_key END WHERE worker_name = ?")
            .bind(&now).bind(&now)
            .bind(source_key)
            .bind(source_key)
            .bind(source_key)
            .bind(source_key)
            .bind(source_key)
            .bind(source_key)
            .bind(source_key)
            .bind(source_key)
            .bind(source_key)
            .bind(source_key)
            .bind(source_key)
            .bind(&self.name).execute(&mut **tx).await?;
        Ok(())
    }
    pub async fn error_in_tx(&self, tx: &mut Transaction<'_, Sqlite>, reason: &str) -> Result<()> {
        self.error_kind_in_tx(tx, HealthErrorKind::Runtime, reason)
            .await
    }
    pub async fn error_kind_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        kind: HealthErrorKind,
        reason: &str,
    ) -> Result<()> {
        self.classified_error_in_tx(
            tx,
            kind,
            if matches!(kind, HealthErrorKind::Runtime) {
                "transient"
            } else {
                "failure"
            },
            reason,
        )
        .await
    }
    async fn classified_error_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        kind: HealthErrorKind,
        class: &str,
        reason: &str,
    ) -> Result<()> {
        let (column, at) = kind.columns();
        let now = now_rfc3339();
        sqlx::query(&format!(
            "UPDATE worker_health SET {column} = ?, {at} = ?, {column}_kind = ?, updated_at = ? WHERE worker_name = ?"
        ))
        .bind(bounded_error(reason))
        .bind(&now)
        .bind(class)
        .bind(&now)
        .bind(&self.name)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
    pub async fn clear_kind_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        kind: HealthErrorKind,
    ) -> Result<()> {
        let (column, at) = kind.columns();
        sqlx::query(&format!("UPDATE worker_health SET {column} = NULL, {at} = NULL, {column}_kind = NULL WHERE worker_name = ? AND {column} IS NOT NULL"))
            .bind(&self.name).execute(&mut **tx).await?;
        Ok(())
    }
    /// Repeated identical infrastructure/tick errors do not write again.
    pub async fn report_error_kind(&self, kind: HealthErrorKind, reason: &str) -> Result<()> {
        self.report_classified_error(
            kind,
            if matches!(kind, HealthErrorKind::Runtime) {
                "transient"
            } else {
                "failure"
            },
            reason,
        )
        .await
    }
    pub async fn report_classified_error(
        &self,
        kind: HealthErrorKind,
        class: &str,
        reason: &str,
    ) -> Result<()> {
        let reason = bounded_error(reason);
        let (column, _) = kind.columns();
        let stored: Option<(Option<String>, Option<String>)> = sqlx::query_as(&format!(
            "SELECT {column}, {column}_kind FROM worker_health WHERE worker_name = ?"
        ))
        .bind(&self.name)
        .fetch_optional(self.db.pool())
        .await?;
        if stored.is_some_and(|(message, kind)| {
            message.as_deref() == Some(&reason) && kind.as_deref() == Some(class)
        }) {
            return Ok(());
        }
        let mut tx = crate::begin_immediate(self.db.pool()).await?;
        self.ensure_in_tx(&mut tx).await?;
        self.classified_error_in_tx(&mut tx, kind, class, &reason)
            .await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn report_error(&self, reason: &str) -> Result<()> {
        self.report_error_kind(HealthErrorKind::Runtime, reason)
            .await
    }
    /// One write only when that error is actually set; unrelated item errors
    /// survive runtime/tick recovery, including recovery on an empty poll.
    pub async fn clear_error_if_set(&self, kind: HealthErrorKind) -> Result<()> {
        let (column, _) = kind.columns();
        let stored: Option<Option<String>> = sqlx::query_scalar(&format!(
            "SELECT {column} FROM worker_health WHERE worker_name = ?"
        ))
        .bind(&self.name)
        .fetch_optional(self.db.pool())
        .await?;
        if stored.flatten().is_none() {
            return Ok(());
        }
        let mut tx = crate::begin_immediate(self.db.pool()).await?;
        self.clear_kind_in_tx(&mut tx, kind).await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn wait_state(&self) -> Result<WorkerWaitState> {
        let row = sqlx::query("SELECT retry_source_key, retry_not_before, deferred_source_key, defer_not_before FROM worker_health WHERE worker_name = ?")
            .bind(&self.name).fetch_one(self.db.pool()).await?;
        Ok(WorkerWaitState {
            retry_key: row.try_get("retry_source_key")?,
            retry_at: row.try_get("retry_not_before")?,
            defer_key: row.try_get("deferred_source_key")?,
            defer_at: row.try_get("defer_not_before")?,
        })
    }

    pub async fn isolated_item_failed(
        &self,
        item: WorkItem<'_>,
        policy: RetryPolicy,
        class: &str,
        reason: &str,
    ) -> Result<PoisonDecision> {
        let mut tx = crate::begin_immediate(self.db.pool()).await?;
        let decision = self
            .isolated_item_failed_in_tx(&mut tx, item, policy, class, reason)
            .await?;
        tx.commit().await?;
        Ok(decision)
    }
    /// Isolated item failure and any terminal domain record share the caller's transaction.
    pub async fn isolated_item_failed_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        item: WorkItem<'_>,
        policy: RetryPolicy,
        class: &str,
        reason: &str,
    ) -> Result<PoisonDecision> {
        let previous: Option<(i64, i64, String)> = sqlx::query_as("SELECT attempts, transient_attempts, first_failed_at FROM worker_item_failure WHERE worker_name = ? AND source_key = ?")
            .bind(&self.name).bind(item.source_key).fetch_optional(&mut **tx).await?;
        let (attempts, transient_attempts, started) = previous.unwrap_or((0, 0, now_rfc3339()));
        let attempts = attempts as u32 + u32::from(class == "failure");
        let transient_attempts = if class == "transient" {
            transient_attempts as u32 + 1
        } else {
            0
        };
        let decision = match class {
            "terminal" => PoisonDecision::DeadLettered,
            "transient" => PoisonDecision::Retry(policy.delay(transient_attempts)),
            _ => policy.decision(attempts),
        };
        if matches!(decision, PoisonDecision::DeadLettered) {
            let key = item.source_key;
            self.dead_letter_in_tx(
                tx,
                item,
                FailureState {
                    attempts,
                    first_failed_at: &started,
                },
                reason,
            )
            .await?;
            sqlx::query("UPDATE worker_dead_letter SET error_kind = ? WHERE worker_name = ? AND source_key = ?").bind(class).bind(&self.name).bind(key).execute(&mut **tx).await?;
            sqlx::query("DELETE FROM worker_item_failure WHERE worker_name = ? AND source_key = ?")
                .bind(&self.name)
                .bind(key)
                .execute(&mut **tx)
                .await?;
        } else {
            let delay = match decision {
                PoisonDecision::Retry(delay) => delay,
                _ => unreachable!(),
            };
            sqlx::query("INSERT INTO worker_item_failure (worker_name, source_key, attempts, transient_attempts, first_failed_at, last_error, error_kind, retry_not_before) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(worker_name, source_key) DO UPDATE SET attempts = excluded.attempts, transient_attempts = excluded.transient_attempts, last_error = excluded.last_error, error_kind = excluded.error_kind, retry_not_before = excluded.retry_not_before")
                .bind(&self.name).bind(item.source_key).bind(i64::from(attempts)).bind(i64::from(transient_attempts)).bind(started).bind(bounded_error(reason)).bind(class).bind(not_before(delay)).execute(&mut **tx).await?;
        }
        Ok(decision)
    }
    pub async fn isolated_item_succeeded(&self, key: &str) -> Result<()> {
        sqlx::query("DELETE FROM worker_item_failure WHERE worker_name = ? AND source_key = ?")
            .bind(&self.name)
            .bind(key)
            .execute(self.db.pool())
            .await?;
        Ok(())
    }
    pub async fn record_restart(&self, reason: &str) -> Result<()> {
        let mut tx = crate::begin_immediate(self.db.pool()).await?;
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
        .checked_add_signed(
            chrono::Duration::from_std(delay).unwrap_or_else(|_| chrono::Duration::seconds(300)),
        )
        .unwrap_or_else(Utc::now)
        .to_rfc3339()
}

/// Worker-selected poison policy. Attempt one waits `initial_backoff`, then
/// exponential waits, each bounded by `max_backoff`.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}
impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 8,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(300),
        }
    }
}
#[derive(Debug)]
pub enum PoisonDecision {
    Retry(Duration),
    DeadLettered,
}

impl RetryPolicy {
    /// A leased source may own per-item attempt accounting and reuse this
    /// decision without the serial event worker's health-row retry state.
    pub fn decision(&self, attempts: u32) -> PoisonDecision {
        if attempts >= self.max_attempts.max(1) {
            PoisonDecision::DeadLettered
        } else {
            PoisonDecision::Retry(self.delay(attempts))
        }
    }
    pub fn delay(&self, attempts: u32) -> Duration {
        let cap = safe_policy_cap(self.max_backoff);
        self.initial_backoff
            .checked_mul(1 << attempts.saturating_sub(1).min(31))
            .unwrap_or(cap)
            .clamp(Duration::from_secs(1), cap)
    }
}

/// A deferral is uncapped in attempts, but its individual readiness waits are
/// bounded so malformed durations cannot hot-loop or overflow a timestamp.
pub fn clamp_worker_deferral(delay: Duration) -> Duration {
    delay.clamp(Duration::from_secs(1), Duration::from_secs(3600))
}
fn safe_policy_cap(cap: Duration) -> Duration {
    let cap = cap.max(Duration::from_secs(1));
    let valid = std::time::Instant::now().checked_add(cap).is_some()
        && chrono::Duration::from_std(cap)
            .ok()
            .and_then(|d| Utc::now().checked_add_signed(d))
            .is_some_and(|date| chrono::Datelike::year(&date) <= 9999);
    if valid {
        cap
    } else {
        Duration::from_secs(300)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum HealthErrorKind {
    Runtime,
    Item,
    Tick,
    AfterCommit,
}
impl HealthErrorKind {
    fn columns(self) -> (&'static str, &'static str) {
        match self {
            Self::Runtime => ("runtime_error", "runtime_error_at"),
            Self::Item => ("item_error", "item_error_at"),
            Self::Tick => ("tick_error", "tick_error_at"),
            Self::AfterCommit => ("after_commit_error", "after_commit_error_at"),
        }
    }
}
pub struct WorkerWaitState {
    pub retry_key: Option<String>,
    pub retry_at: Option<String>,
    pub defer_key: Option<String>,
    pub defer_at: Option<String>,
}
fn bounded_error(value: &str) -> String {
    value
        .chars()
        .filter(|c| !c.is_control())
        .take(1024)
        .collect()
}
