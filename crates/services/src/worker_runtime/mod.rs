//! Two-phase event execution, assembled from a durable source and reusable
//! source-neutral supervisor, health and poison policy.
mod event_source;
mod health;
mod policy;
mod supervisor;

pub use db::EventSubscription as Subscription;
pub use event_source::DurableEventSource;
pub use health::{FailureState, WorkItem, WorkerHealth};
pub use policy::{Outcome, PoisonDecision, RetryPolicy, WorkerError, WorkerErrorKind};
pub use supervisor::{SupervisorPolicy, WorkerSupervisor};

use crate::{Result, ServiceError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use db::{DomainEvent, SqliteDb};
use sqlx::{Sqlite, Transaction};
use std::{
    future::Future,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
    time::Duration,
};
use supervisor::shutdown_signal;
use tokio::{
    sync::{watch, Notify},
    task::JoinHandle,
    time::Instant,
};

const MIN_IDLE: Duration = Duration::from_millis(250);
const MAX_IDLE: Duration = Duration::from_secs(5);

/// Slow preparation is outside the transaction. `commit` must await only DB
/// work. Hooks must be idempotent; after_commit cannot undo acknowledgement.
#[async_trait]
pub trait Worker: Send + Sync + 'static {
    type Prepared: Send + Sync + 'static;
    fn name(&self) -> &str;
    fn subscription(&self) -> Subscription;
    fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy::default()
    }
    fn handle_timeout(&self) -> Duration {
        Duration::from_secs(300)
    }
    async fn tick(&self) -> std::result::Result<(), WorkerError> {
        Ok(())
    }
    async fn handle(
        &self,
        event: &DomainEvent,
    ) -> std::result::Result<Outcome<Self::Prepared>, WorkerError>;
    async fn commit(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        event: &DomainEvent,
        prepared: &Self::Prepared,
    ) -> std::result::Result<(), WorkerError>;
    async fn after_commit(
        &self,
        _event: &DomainEvent,
        _prepared: &Self::Prepared,
    ) -> std::result::Result<(), WorkerError> {
        Ok(())
    }
}

pub struct WorkerRuntime<W: Worker> {
    db: Arc<SqliteDb>,
    worker: Arc<W>,
    notify: Arc<Notify>,
    source: DurableEventSource,
    health: WorkerHealth,
    wait: Mutex<Option<(String, Instant)>>,
    #[cfg(test)]
    idle_entered: Notify,
    #[cfg(test)]
    write_transactions: std::sync::atomic::AtomicU64,
}
#[derive(Debug)]
enum PollResult {
    Progress,
    Retry(Duration),
    Idle,
}

impl<W: Worker> WorkerRuntime<W> {
    pub fn new(db: Arc<SqliteDb>, worker: Arc<W>) -> Self {
        Self {
            source: DurableEventSource::new(Arc::clone(&db), worker.name(), worker.subscription()),
            health: WorkerHealth::new(Arc::clone(&db), worker.name()),
            notify: db.domain_event_notify(),
            db,
            worker,
            wait: Mutex::new(None),
            #[cfg(test)]
            idle_entered: Notify::new(),
            #[cfg(test)]
            write_transactions: std::sync::atomic::AtomicU64::new(0),
        }
    }
    pub fn start(self: Arc<Self>, shutdown: watch::Receiver<bool>) -> JoinHandle<()> {
        WorkerSupervisor::new(self.health.clone(), SupervisorPolicy::default()).start(
            move |shutdown| {
                let runtime = Arc::clone(&self);
                async move { runtime.run_loop(shutdown).await }
            },
            shutdown,
        )
    }
    pub async fn run_once(&self, limit: usize) -> Result<usize> {
        let mut progressed = 0;
        for _ in 0..limit.clamp(1, 100) {
            match self.poll_once().await? {
                PollResult::Progress => progressed += 1,
                PollResult::Retry(_) | PollResult::Idle => break,
            }
        }
        Ok(progressed)
    }
    async fn initialize(&self) -> Result<()> {
        if self.worker.name().trim().is_empty() {
            return Err(ServiceError::invalid_operation(
                "worker name must be non-empty",
            ));
        }
        if !self.source.is_initialized().await? {
            let mut tx = self.begin_write().await?;
            self.source.initialize_in_tx(&mut tx, &self.health).await?;
            tx.commit().await?;
        }
        Ok(())
    }
    async fn run_loop(&self, shutdown: watch::Receiver<bool>) -> Result<()> {
        self.run_loop_with_idle(shutdown, MIN_IDLE).await
    }
    async fn run_loop_with_idle(
        &self,
        mut shutdown: watch::Receiver<bool>,
        initial_idle: Duration,
    ) -> Result<()> {
        self.initialize().await?;
        let mut idle = initial_idle;
        loop {
            if *shutdown.borrow_and_update() {
                return Ok(());
            }
            // enable registers with notify_waiters before the read, not merely
            // when this future is first polled by the select below.
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.poll_once().await {
                Ok(PollResult::Progress) => {
                    idle = MIN_IDLE;
                }
                Ok(PollResult::Retry(delay)) => {
                    tokio::select! { _ = shutdown_signal(&mut shutdown) => return Ok(()),
                    _ = tokio::time::sleep(delay) => {} }
                }
                Ok(PollResult::Idle) => {
                    #[cfg(test)]
                    self.idle_entered.notify_one();
                    tokio::select! { _ = shutdown_signal(&mut shutdown) => return Ok(()),
                    _ = &mut notified => {},
                    _ = tokio::time::sleep(idle) => { idle = idle.saturating_mul(2).min(MAX_IDLE); } }
                }
                Err(error) => {
                    tracing::warn!(worker = self.worker.name(), %error, "worker runtime poll failed");
                    // Error persistence cannot consume an event attempt.
                    let _ = self
                        .health
                        .report_error("worker runtime database poll failed")
                        .await;
                    tokio::select! { _ = shutdown_signal(&mut shutdown) => return Ok(()),
                    _ = tokio::time::sleep(idle) => {} }
                    idle = idle.saturating_mul(2).min(MAX_IDLE);
                }
            }
        }
    }
    async fn poll_once(&self) -> Result<PollResult> {
        self.initialize().await?;
        if let Err(error) =
            catch_worker(async { self.worker.tick().await }, "worker tick panicked").await
        {
            tracing::warn!(worker = self.worker.name(), %error, "worker tick failed");
            self.health.report_error(error.message()).await?;
            return Ok(PollResult::Retry(MIN_IDLE));
        }
        let cursor = self.source.cursor().await?;
        let Some(event) = self.source.next(cursor).await? else {
            if self.source.flush_due(cursor) {
                let mut tx = self.begin_write().await?;
                let advanced = self.source.flush_in_tx(&mut tx, cursor).await?;
                tx.commit().await?;
                self.source.flushed();
                if advanced {
                    return Ok(PollResult::Progress);
                }
            }
            return Ok(PollResult::Idle);
        };
        let key = event.sequence.to_string();
        if let Some(delay) = self.pending_delay(&key).await? {
            return Ok(PollResult::Retry(delay));
        }
        let outcome = catch_worker(
            async {
                tokio::time::timeout(self.worker.handle_timeout(), self.worker.handle(&event))
                    .await
                    .unwrap_or_else(|_| Err(WorkerError::new("worker handle timed out")))
            },
            "worker handle panicked",
        )
        .await;
        match outcome {
            Ok(Outcome::Done(prepared)) => {
                let mut tx = self.begin_write().await?;
                self.source
                    .validate_in_tx(&mut tx, cursor, event.sequence)
                    .await?;
                self.health.ensure_in_tx(&mut tx).await?;
                if let Err(error) = catch_worker(
                    async { self.worker.commit(&mut tx, &event, &prepared).await },
                    "worker commit panicked",
                )
                .await
                {
                    tx.rollback().await?;
                    return self.worker_failure(cursor, &event, error).await;
                }
                self.source
                    .acknowledge_in_tx(&mut tx, cursor, event.sequence)
                    .await?;
                self.health.success_in_tx(&mut tx).await?;
                tx.commit().await?;
                self.clear_wait();
                if let Err(error) = catch_worker(
                    async { self.worker.after_commit(&event, &prepared).await },
                    "worker after_commit panicked",
                )
                .await
                {
                    tracing::warn!(worker = self.worker.name(), %error, "worker after_commit failed");
                    // The event is already committed, even if reporting fails.
                    let _ = self.health.report_error(error.message()).await;
                }
                Ok(PollResult::Progress)
            }
            Ok(Outcome::Defer { after, reason }) => {
                let mut tx = self.begin_write().await?;
                self.source
                    .validate_in_tx(&mut tx, cursor, event.sequence)
                    .await?;
                self.health
                    .defer_in_tx(&mut tx, &key, after, &reason)
                    .await?;
                tx.commit().await?;
                self.set_wait(key, after);
                Ok(PollResult::Retry(after))
            }
            Ok(Outcome::DeadLetter { reason }) => self.failure(cursor, &event, &reason, true).await,
            Err(error) => self.worker_failure(cursor, &event, error).await,
        }
    }
    async fn worker_failure(
        &self,
        cursor: i64,
        event: &DomainEvent,
        error: WorkerError,
    ) -> Result<PollResult> {
        if error.kind == WorkerErrorKind::Transient {
            self.health.report_error(error.message()).await?;
            let delay = self.worker.retry_policy().delay(1);
            self.set_wait(event.sequence.to_string(), delay);
            return Ok(PollResult::Retry(delay));
        }
        self.failure(cursor, event, error.message(), false).await
    }
    async fn failure(
        &self,
        cursor: i64,
        event: &DomainEvent,
        reason: &str,
        terminal: bool,
    ) -> Result<PollResult> {
        let mut tx = self.begin_write().await?;
        self.source
            .validate_in_tx(&mut tx, cursor, event.sequence)
            .await?;
        let key = event.sequence.to_string();
        let decision = self
            .health
            .failure_in_tx(
                &mut tx,
                WorkItem {
                    source_key: &key,
                    item_type: &event.event_type,
                },
                self.worker.retry_policy(),
                reason,
                terminal,
            )
            .await?;
        if matches!(decision, PoisonDecision::DeadLettered) {
            self.source
                .acknowledge_in_tx(&mut tx, cursor, event.sequence)
                .await?;
        }
        tx.commit().await?;
        match decision {
            PoisonDecision::Retry(delay) => {
                self.set_wait(key, delay);
                Ok(PollResult::Retry(delay))
            }
            PoisonDecision::DeadLettered => {
                self.clear_wait();
                Ok(PollResult::Progress)
            }
        }
    }
    async fn pending_delay(&self, key: &str) -> Result<Option<Duration>> {
        {
            let wait = self.wait.lock().expect("worker wait lock");
            if let Some((waiting_key, deadline)) = wait.as_ref() {
                if waiting_key == key {
                    return Ok(deadline
                        .checked_duration_since(Instant::now())
                        .filter(|d| !d.is_zero()));
                }
            }
        }
        let (retry_key, retry_at, defer_key, defer_at): (
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        ) = sqlx::query_as(
            "SELECT retry_source_key, retry_not_before, deferred_source_key, defer_not_before
                FROM worker_health WHERE worker_name = ?",
        )
        .bind(self.worker.name())
        .fetch_one(self.db.pool())
        .await?;
        let mut delay = Duration::ZERO;
        for (pending_key, at) in [(retry_key, retry_at), (defer_key, defer_at)] {
            if pending_key.as_deref() != Some(key) {
                continue;
            }
            if let Some(at) = at {
                let time = DateTime::parse_from_rfc3339(&at)
                    .map_err(|_| db::DbError::Check("invalid worker retry timestamp".to_owned()))?;
                delay = delay.max(
                    (time.with_timezone(&Utc) - Utc::now())
                        .to_std()
                        .unwrap_or(Duration::ZERO),
                );
            }
        }
        if delay.is_zero() {
            return Ok(None);
        }
        self.set_wait(key.to_owned(), delay);
        Ok(Some(delay))
    }
    fn set_wait(&self, key: String, delay: Duration) {
        *self.wait.lock().expect("worker wait lock") = Some((key, Instant::now() + delay));
    }
    fn clear_wait(&self) {
        *self.wait.lock().expect("worker wait lock") = None;
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

// Catch only worker hooks. Runtime errors/panics retain their own loop/supervisor
// path. Polling inside catch_unwind also drops a timed-out or panicked future.
async fn catch_worker<F, T>(
    future: F,
    panic_message: &'static str,
) -> std::result::Result<T, WorkerError>
where
    F: Future<Output = std::result::Result<T, WorkerError>>,
{
    tokio::pin!(future);
    std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(poll) => poll,
            Err(_) => std::task::Poll::Ready(Err(WorkerError::new(panic_message))),
        }
    })
    .await
}
#[cfg(test)]
mod tests;
