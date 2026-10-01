//! Shared runtime for ordered, durable domain-event workers.
//!
//! A worker prepares slow or external work in [`Worker::handle`] before the
//! runtime opens a write transaction. [`Worker::commit`] may await database
//! operations only; its effect, the cursor, and health are committed together.

use std::{fmt, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use db::{now_rfc3339, DomainEvent, SqliteDb};
use sqlx::{Row, Sqlite, Transaction};
use tokio::{
    sync::{watch, Notify},
    task::JoinHandle,
};

use crate::{Result, ServiceError};

const DEFAULT_MAX_ATTEMPTS: u32 = 5;
const MIN_BACKOFF: Duration = Duration::from_millis(250);
const MAX_BACKOFF: Duration = Duration::from_secs(5);
const MAX_ERROR_CHARS: usize = 1_024;

/// Result of preparing one subscribed event.
#[derive(Debug)]
pub enum WorkerOutcome<T> {
    /// Apply the prepared database effect and advance the cursor atomically.
    Done(T),
    /// Leave the cursor on this event and try it again after the duration.
    RetryAfter(Duration),
    /// Quarantine this event immediately and advance to the next one.
    DeadLetter(String),
}

/// Bounded, operator-safe failure supplied by a worker. Implementations must
/// never include event payloads, credentials, or other secret material.
#[derive(Debug, Clone)]
pub struct WorkerError {
    message: String,
}

impl WorkerError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: bounded_error(&message.into()),
        }
    }

    fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for WorkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for WorkerError {}

/// Two-phase event worker. `handle` runs without a write transaction and may
/// perform slow work. `commit` runs inside the runtime's short SQLite write
/// transaction and must await database operations only.
#[async_trait]
pub trait Worker: Send + Sync + 'static {
    type Prepared: Send + Sync + 'static;

    fn name(&self) -> &str;
    fn event_types(&self) -> &'static [&'static str];

    async fn handle(
        &self,
        event: &DomainEvent,
    ) -> std::result::Result<WorkerOutcome<Self::Prepared>, WorkerError>;

    async fn commit(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        event: &DomainEvent,
        prepared: &Self::Prepared,
    ) -> std::result::Result<(), WorkerError>;
}

#[derive(Debug, Clone, Copy)]
struct WorkerRuntimeOptions {
    max_attempts: u32,
    min_backoff: Duration,
    max_backoff: Duration,
}

impl Default for WorkerRuntimeOptions {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            min_backoff: MIN_BACKOFF,
            max_backoff: MAX_BACKOFF,
        }
    }
}

pub struct WorkerRuntime<W: Worker> {
    db: Arc<SqliteDb>,
    worker: Arc<W>,
    notify: Arc<Notify>,
    options: WorkerRuntimeOptions,
    #[cfg(test)]
    write_transactions: std::sync::atomic::AtomicU64,
}

#[derive(Debug)]
enum PollResult {
    Progress,
    Retry(Duration),
    Idle,
}

#[derive(Debug)]
struct Backlog {
    head_sequence: i64,
    lag: i64,
    oldest_pending_at: Option<String>,
}

#[derive(Debug)]
enum FailureResult {
    Retry(Duration),
    DeadLettered,
}

impl<W: Worker> WorkerRuntime<W> {
    pub fn new(db: Arc<SqliteDb>, worker: Arc<W>) -> Self {
        let notify = db.domain_event_notify();
        Self {
            db,
            worker,
            notify,
            options: WorkerRuntimeOptions::default(),
            #[cfg(test)]
            write_transactions: std::sync::atomic::AtomicU64::new(0),
        }
    }

    #[cfg(test)]
    fn with_options(mut self, options: WorkerRuntimeOptions) -> Self {
        self.options = options;
        self
    }

    /// Start the worker under a restart loop. The returned task does not exit
    /// merely because the worker task returned or panicked; it exits only when
    /// shutdown is requested.
    pub fn start(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut restart_backoff = self.options.min_backoff;
            loop {
                if shutdown_requested(&mut shutdown) {
                    return;
                }

                let child_runtime = Arc::clone(&self);
                let child_shutdown = shutdown.clone();
                let mut child =
                    tokio::spawn(async move { child_runtime.run_loop(child_shutdown).await });
                let exit = tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow_and_update() {
                            child.abort();
                            let _ = child.await;
                            return;
                        }
                        continue;
                    }
                    result = &mut child => result,
                };

                if shutdown_requested(&mut shutdown) {
                    return;
                }
                let reason = match exit {
                    Ok(Ok(())) => "worker task exited unexpectedly",
                    Ok(Err(_)) => "worker task returned an error",
                    Err(error) if error.is_panic() => "worker task panicked",
                    Err(_) => "worker task was cancelled",
                };
                if let Err(error) = self.record_restart(reason).await {
                    tracing::warn!(worker = %self.worker.name(), %error, "failed to record worker restart");
                }

                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow_and_update() {
                            return;
                        }
                    }
                    _ = tokio::time::sleep(restart_backoff) => {}
                }
                restart_backoff = doubled(restart_backoff, self.options.max_backoff);
            }
        })
    }

    /// Run a bounded amount of work without sleeping. This is used by focused
    /// tests and one-shot callers; the supervised loop is the production path.
    pub async fn run_once(&self, limit: usize) -> Result<usize> {
        self.initialize().await?;
        let mut progressed = 0;
        for _ in 0..limit.clamp(1, 100) {
            match self.poll_once().await? {
                PollResult::Progress => progressed += 1,
                PollResult::Retry(_) | PollResult::Idle => break,
            }
        }
        Ok(progressed)
    }

    async fn run_loop(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        self.initialize().await?;
        let mut idle_backoff = self.options.min_backoff;
        loop {
            if shutdown_requested(&mut shutdown) {
                return Ok(());
            }

            // Register before polling so a commit racing the read cannot lose
            // its wakeup between the idle decision and this wait.
            let notified = self.notify.notified();
            tokio::pin!(notified);
            match self.poll_once().await {
                Ok(PollResult::Progress) => {
                    idle_backoff = self.options.min_backoff;
                }
                Ok(PollResult::Retry(delay)) => {
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow_and_update() {
                                return Ok(());
                            }
                        }
                        _ = tokio::time::sleep(delay) => {}
                    }
                }
                Ok(PollResult::Idle) => {
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow_and_update() {
                                return Ok(());
                            }
                        }
                        _ = &mut notified => {}
                        _ = tokio::time::sleep(idle_backoff) => {
                            idle_backoff = doubled(idle_backoff, self.options.max_backoff);
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(worker = %self.worker.name(), %error, "worker poll failed");
                    self.record_runtime_error("worker runtime database poll failed")
                        .await;
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow_and_update() {
                                return Ok(());
                            }
                        }
                        _ = tokio::time::sleep(idle_backoff) => {
                            idle_backoff = doubled(idle_backoff, self.options.max_backoff);
                        }
                    }
                }
            }
        }
    }

    async fn poll_once(&self) -> Result<PollResult> {
        let cursor = self.cursor().await?;
        let Some(event) = self.next_wanted_event(cursor).await? else {
            let head =
                sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(sequence) FROM domain_event")
                    .fetch_one(self.db.pool())
                    .await?
                    .unwrap_or(0);
            if head > cursor && self.advance_ignored(cursor).await? {
                return Ok(PollResult::Progress);
            }
            return Ok(PollResult::Idle);
        };

        if let Some(delay) = self.retry_delay_if_pending(event.sequence).await? {
            return Ok(PollResult::Retry(delay));
        }

        match self.worker.handle(&event).await {
            Ok(WorkerOutcome::Done(prepared)) => {
                if let Err(error) = self.commit_success(cursor, &event, &prepared).await {
                    return Ok(
                        match self
                            .record_failure(cursor, &event, error.message(), None, false)
                            .await?
                        {
                            FailureResult::Retry(delay) => PollResult::Retry(delay),
                            FailureResult::DeadLettered => PollResult::Progress,
                        },
                    );
                }
                Ok(PollResult::Progress)
            }
            Ok(WorkerOutcome::RetryAfter(delay)) => Ok(
                match self
                    .record_failure(
                        cursor,
                        &event,
                        "handler requested retry",
                        Some(delay),
                        false,
                    )
                    .await?
                {
                    FailureResult::Retry(delay) => PollResult::Retry(delay),
                    FailureResult::DeadLettered => PollResult::Progress,
                },
            ),
            Ok(WorkerOutcome::DeadLetter(reason)) => {
                self.record_failure(cursor, &event, &reason, None, true)
                    .await?;
                Ok(PollResult::Progress)
            }
            Err(error) => Ok(
                match self
                    .record_failure(cursor, &event, error.message(), None, false)
                    .await?
                {
                    FailureResult::Retry(delay) => PollResult::Retry(delay),
                    FailureResult::DeadLettered => PollResult::Progress,
                },
            ),
        }
    }

    async fn initialize(&self) -> Result<()> {
        if self.worker.name().trim().is_empty() || self.worker.event_types().is_empty() {
            return Err(ServiceError::invalid_operation(
                "worker name and event subscriptions must be non-empty",
            ));
        }
        let now = now_rfc3339();
        let mut transaction = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO event_consumer_cursor (
                consumer_name, last_sequence, version, updated_at
             ) VALUES (?, 0, 1, ?)
             ON CONFLICT(consumer_name) DO NOTHING",
        )
        .bind(self.worker.name())
        .bind(&now)
        .execute(&mut *transaction)
        .await?;
        let (cursor, cursor_updated_at) = sqlx::query_as::<_, (i64, String)>(
            "SELECT last_sequence, updated_at FROM event_consumer_cursor
             WHERE consumer_name = ?",
        )
        .bind(self.worker.name())
        .fetch_one(&mut *transaction)
        .await?;
        let backlog = self.backlog_in_tx(&mut transaction, cursor).await?;
        sqlx::query(
            "INSERT INTO worker_health (
                worker_name, cursor_sequence, head_sequence, lag,
                oldest_pending_at, restart_count, cursor_updated_at,
                created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?)
             ON CONFLICT(worker_name) DO UPDATE SET
                cursor_sequence = excluded.cursor_sequence,
                head_sequence = excluded.head_sequence,
                lag = excluded.lag,
                oldest_pending_at = excluded.oldest_pending_at,
                cursor_updated_at = CASE
                    WHEN worker_health.cursor_sequence <> excluded.cursor_sequence
                    THEN excluded.cursor_updated_at
                    ELSE worker_health.cursor_updated_at
                END,
                updated_at = excluded.updated_at",
        )
        .bind(self.worker.name())
        .bind(cursor)
        .bind(backlog.head_sequence)
        .bind(backlog.lag)
        .bind(backlog.oldest_pending_at.as_deref())
        .bind(cursor_updated_at)
        .bind(&now)
        .bind(&now)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn cursor(&self) -> Result<i64> {
        sqlx::query_scalar(
            "SELECT last_sequence FROM event_consumer_cursor WHERE consumer_name = ?",
        )
        .bind(self.worker.name())
        .fetch_optional(self.db.pool())
        .await?
        .ok_or_else(|| ServiceError::Db(db::DbError::NotFound))
    }

    async fn next_wanted_event(&self, cursor: i64) -> Result<Option<DomainEvent>> {
        let mut query =
            sqlx::QueryBuilder::<Sqlite>::new("SELECT * FROM domain_event WHERE sequence > ");
        query.push_bind(cursor).push(" AND event_type IN (");
        let mut separated = query.separated(", ");
        for event_type in self.worker.event_types() {
            separated.push_bind(*event_type);
        }
        separated.push_unseparated(") ORDER BY sequence ASC LIMIT 1");
        query
            .build()
            .fetch_optional(self.db.pool())
            .await?
            .map(map_domain_event)
            .transpose()
            .map_err(ServiceError::from)
    }

    async fn retry_delay_if_pending(&self, event_sequence: i64) -> Result<Option<Duration>> {
        let retry = sqlx::query_as::<_, (Option<i64>, Option<String>)>(
            "SELECT retry_sequence, retry_not_before FROM worker_health
             WHERE worker_name = ?",
        )
        .bind(self.worker.name())
        .fetch_one(self.db.pool())
        .await?;
        if retry.0 != Some(event_sequence) {
            return Ok(None);
        }
        let Some(not_before) = retry.1 else {
            return Ok(None);
        };
        let not_before = DateTime::parse_from_rfc3339(&not_before)
            .map_err(|_| ServiceError::Db(db::DbError::InvalidTransition))?
            .with_timezone(&Utc);
        Ok((not_before > Utc::now())
            .then(|| (not_before - Utc::now()).to_std().unwrap_or(Duration::ZERO)))
    }

    async fn advance_ignored(&self, expected_cursor: i64) -> Result<bool> {
        let now = now_rfc3339();
        let mut transaction = self.begin_write().await?;
        let cursor = self.cursor_in_tx(&mut transaction).await?;
        if cursor != expected_cursor {
            transaction.rollback().await?;
            return Ok(false);
        }
        if self
            .db
            .next_existing_domain_event_sequence_in_tx(
                &mut transaction,
                cursor,
                self.worker.event_types(),
            )
            .await?
            .is_some()
        {
            transaction.rollback().await?;
            return Ok(false);
        }
        let head = sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(sequence) FROM domain_event")
            .fetch_one(&mut *transaction)
            .await?
            .unwrap_or(0);
        if head <= cursor {
            transaction.rollback().await?;
            return Ok(false);
        }
        self.advance_cursor_in_tx(&mut transaction, cursor, head, &now)
            .await?;
        sqlx::query(
            "UPDATE worker_health SET
                cursor_sequence = ?, head_sequence = ?, lag = 0,
                oldest_pending_at = NULL, cursor_updated_at = ?,
                retry_sequence = NULL, retry_attempts = 0,
                retry_not_before = NULL, retry_started_at = NULL,
                updated_at = ?
             WHERE worker_name = ?",
        )
        .bind(head)
        .bind(head)
        .bind(&now)
        .bind(&now)
        .bind(self.worker.name())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(true)
    }

    async fn commit_success(
        &self,
        expected_cursor: i64,
        event: &DomainEvent,
        prepared: &W::Prepared,
    ) -> std::result::Result<(), WorkerError> {
        let result: Result<()> = async {
            let now = now_rfc3339();
            let mut transaction = self.begin_write().await?;
            self.validate_next_in_tx(&mut transaction, expected_cursor, event.sequence)
                .await?;
            if let Err(error) = self.worker.commit(&mut transaction, event, prepared).await {
                transaction.rollback().await?;
                return Err(ServiceError::invalid_operation(error.to_string()));
            }
            self.advance_cursor_in_tx(&mut transaction, expected_cursor, event.sequence, &now)
                .await?;
            let backlog = self.backlog_in_tx(&mut transaction, event.sequence).await?;
            sqlx::query(
                "UPDATE worker_health SET
                    cursor_sequence = ?, head_sequence = ?, lag = ?,
                    oldest_pending_at = ?, last_error = NULL,
                    last_error_at = NULL, last_success_at = ?,
                    cursor_updated_at = ?, retry_sequence = NULL,
                    retry_attempts = 0, retry_not_before = NULL,
                    retry_started_at = NULL, updated_at = ?
                 WHERE worker_name = ?",
            )
            .bind(event.sequence)
            .bind(backlog.head_sequence)
            .bind(backlog.lag)
            .bind(backlog.oldest_pending_at.as_deref())
            .bind(&now)
            .bind(&now)
            .bind(&now)
            .bind(self.worker.name())
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
            Ok(())
        }
        .await;
        result.map_err(|error| WorkerError::new(format!("worker commit failed: {error}")))
    }

    async fn record_failure(
        &self,
        expected_cursor: i64,
        event: &DomainEvent,
        reason: &str,
        requested_delay: Option<Duration>,
        force_dead_letter: bool,
    ) -> Result<FailureResult> {
        let reason = bounded_error(reason);
        let now = now_rfc3339();
        let mut transaction = self.begin_write().await?;
        self.validate_next_in_tx(&mut transaction, expected_cursor, event.sequence)
            .await?;
        let retry = sqlx::query_as::<_, (Option<i64>, i64, Option<String>)>(
            "SELECT retry_sequence, retry_attempts, retry_started_at
             FROM worker_health WHERE worker_name = ?",
        )
        .bind(self.worker.name())
        .fetch_one(&mut *transaction)
        .await?;
        let same_event = retry.0 == Some(event.sequence);
        let attempts = if same_event {
            retry.1.saturating_add(1)
        } else {
            1
        };
        let first_failed_at = if same_event {
            retry.2.unwrap_or_else(|| now.clone())
        } else {
            now.clone()
        };

        if force_dead_letter || attempts >= i64::from(self.options.max_attempts) {
            sqlx::query(
                "INSERT INTO worker_dead_letter (
                    worker_name, event_sequence, event_type, attempts,
                    last_error, first_failed_at, last_failed_at, dead_lettered_at
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(worker_name, event_sequence) DO UPDATE SET
                    attempts = excluded.attempts,
                    last_error = excluded.last_error,
                    last_failed_at = excluded.last_failed_at,
                    dead_lettered_at = excluded.dead_lettered_at",
            )
            .bind(self.worker.name())
            .bind(event.sequence)
            .bind(&event.event_type)
            .bind(attempts)
            .bind(&reason)
            .bind(&first_failed_at)
            .bind(&now)
            .bind(&now)
            .execute(&mut *transaction)
            .await?;
            self.advance_cursor_in_tx(&mut transaction, expected_cursor, event.sequence, &now)
                .await?;
            let backlog = self.backlog_in_tx(&mut transaction, event.sequence).await?;
            sqlx::query(
                "UPDATE worker_health SET
                    cursor_sequence = ?, head_sequence = ?, lag = ?,
                    oldest_pending_at = ?, last_error = ?, last_error_at = ?,
                    cursor_updated_at = ?, retry_sequence = NULL,
                    retry_attempts = 0, retry_not_before = NULL,
                    retry_started_at = NULL, updated_at = ?
                 WHERE worker_name = ?",
            )
            .bind(event.sequence)
            .bind(backlog.head_sequence)
            .bind(backlog.lag)
            .bind(backlog.oldest_pending_at.as_deref())
            .bind(&reason)
            .bind(&now)
            .bind(&now)
            .bind(&now)
            .bind(self.worker.name())
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
            return Ok(FailureResult::DeadLettered);
        }

        let delay = requested_delay.unwrap_or_else(|| self.retry_backoff(attempts as u32));
        let retry_not_before = (Utc::now()
            + chrono::Duration::from_std(delay).unwrap_or_else(|_| chrono::Duration::seconds(5)))
        .to_rfc3339();
        let backlog = self
            .backlog_in_tx(&mut transaction, expected_cursor)
            .await?;
        sqlx::query(
            "UPDATE worker_health SET
                head_sequence = ?, lag = ?, oldest_pending_at = ?,
                last_error = ?, last_error_at = ?, retry_sequence = ?,
                retry_attempts = ?, retry_not_before = ?, retry_started_at = ?,
                updated_at = ?
             WHERE worker_name = ?",
        )
        .bind(backlog.head_sequence)
        .bind(backlog.lag)
        .bind(backlog.oldest_pending_at.as_deref())
        .bind(&reason)
        .bind(&now)
        .bind(event.sequence)
        .bind(attempts)
        .bind(&retry_not_before)
        .bind(&first_failed_at)
        .bind(&now)
        .bind(self.worker.name())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(FailureResult::Retry(delay))
    }

    async fn validate_next_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        expected_cursor: i64,
        event_sequence: i64,
    ) -> Result<()> {
        let cursor = self.cursor_in_tx(transaction).await?;
        if cursor != expected_cursor {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }
        let next = self
            .db
            .next_existing_domain_event_sequence_in_tx(
                transaction,
                cursor,
                self.worker.event_types(),
            )
            .await?;
        if next != Some(event_sequence) {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }
        Ok(())
    }

    async fn cursor_in_tx(&self, transaction: &mut Transaction<'_, Sqlite>) -> Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT last_sequence FROM event_consumer_cursor WHERE consumer_name = ?",
        )
        .bind(self.worker.name())
        .fetch_one(&mut **transaction)
        .await?)
    }

    async fn advance_cursor_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        expected_cursor: i64,
        sequence: i64,
        now: &str,
    ) -> Result<()> {
        let updated = sqlx::query(
            "UPDATE event_consumer_cursor
             SET last_sequence = ?, version = version + 1, updated_at = ?
             WHERE consumer_name = ? AND last_sequence = ?",
        )
        .bind(sequence)
        .bind(now)
        .bind(self.worker.name())
        .bind(expected_cursor)
        .execute(&mut **transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }
        Ok(())
    }

    async fn backlog_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        cursor: i64,
    ) -> Result<Backlog> {
        let head_sequence =
            sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(sequence) FROM domain_event")
                .fetch_one(&mut **transaction)
                .await?
                .unwrap_or(0);
        let mut query = sqlx::QueryBuilder::<Sqlite>::new(
            "SELECT COUNT(*), MIN(created_at) FROM domain_event WHERE sequence > ",
        );
        query.push_bind(cursor).push(" AND event_type IN (");
        let mut separated = query.separated(", ");
        for event_type in self.worker.event_types() {
            separated.push_bind(*event_type);
        }
        separated.push_unseparated(")");
        let (lag, oldest_pending_at) = query
            .build_query_as::<(i64, Option<String>)>()
            .fetch_one(&mut **transaction)
            .await?;
        Ok(Backlog {
            head_sequence,
            lag,
            oldest_pending_at,
        })
    }

    async fn record_restart(&self, reason: &str) -> Result<()> {
        self.initialize().await?;
        let now = now_rfc3339();
        sqlx::query(
            "UPDATE worker_health SET
                restart_count = restart_count + 1,
                last_error = ?, last_error_at = ?, updated_at = ?
             WHERE worker_name = ?",
        )
        .bind(bounded_error(reason))
        .bind(&now)
        .bind(&now)
        .bind(self.worker.name())
        .execute(self.db.pool())
        .await?;
        Ok(())
    }

    async fn record_runtime_error(&self, reason: &str) {
        let now = now_rfc3339();
        let _ = sqlx::query(
            "UPDATE worker_health SET last_error = ?, last_error_at = ?, updated_at = ?
             WHERE worker_name = ?",
        )
        .bind(bounded_error(reason))
        .bind(&now)
        .bind(&now)
        .bind(self.worker.name())
        .execute(self.db.pool())
        .await;
    }

    fn retry_backoff(&self, attempts: u32) -> Duration {
        let shift = attempts.saturating_sub(1).min(20);
        let factor = 1_u32 << shift;
        self.options
            .min_backoff
            .checked_mul(factor)
            .unwrap_or(self.options.max_backoff)
            .min(self.options.max_backoff)
    }

    async fn begin_write(&self) -> Result<Transaction<'_, Sqlite>> {
        #[cfg(test)]
        self.write_transactions
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(db::begin_immediate(self.db.pool()).await?)
    }

    #[cfg(test)]
    fn write_transaction_count(&self) -> u64 {
        self.write_transactions
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

fn map_domain_event(row: sqlx::sqlite::SqliteRow) -> std::result::Result<DomainEvent, sqlx::Error> {
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

fn shutdown_requested(shutdown: &mut watch::Receiver<bool>) -> bool {
    *shutdown.borrow_and_update()
}

fn doubled(value: Duration, maximum: Duration) -> Duration {
    value.checked_mul(2).unwrap_or(maximum).min(maximum)
}

fn bounded_error(value: &str) -> String {
    value
        .chars()
        .filter(|character| !character.is_control() || *character == ' ')
        .take(MAX_ERROR_CHARS)
        .collect()
}

#[cfg(test)]
mod tests;
