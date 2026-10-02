//! Ordered event execution assembled from a durable source and reusable,
//! source-neutral DB health, poison policy and loop supervision.
mod event_source;
mod policy;
mod supervisor;

pub use db::EventSubscription as Subscription;
pub use db::{FailureState, HealthErrorKind, PoisonDecision, RetryPolicy, WorkItem, WorkerHealth};
pub use event_source::DurableEventSource;
pub use policy::{Outcome, WorkerError, WorkerErrorKind};
pub use supervisor::{SupervisorPolicy, WorkerSupervisor};

use crate::{Result, ServiceError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use db::{DomainEvent, SqliteDb};
use sqlx::{Sqlite, Transaction};
use std::{
    collections::HashSet,
    future::Future,
    marker::PhantomData,
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
const MIN_RETRY: Duration = Duration::from_secs(1);
const CYCLE_LIMIT: usize = 100;

/// Stable Rust does not support associated type defaults. The commit-result
/// parameter therefore defaults to (), preserving the no-result worker form.
/// Slow preparation precedes the transaction; commit may await only DB work.
#[async_trait]
pub trait Worker<C: Send + Sync + 'static = ()>: Send + Sync + 'static {
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
    ) -> std::result::Result<C, WorkerError>;
    async fn after_commit(
        &self,
        _event: &DomainEvent,
        _prepared: &Self::Prepared,
        _committed: &C,
    ) -> std::result::Result<(), WorkerError> {
        Ok(())
    }
}
struct Wait {
    key: String,
    deadline: Instant,
    infrastructure: bool,
}
#[derive(Default)]
struct Schedule {
    failures: u32,
    due: Option<Instant>,
}
impl Schedule {
    fn failed(&mut self) -> Duration {
        self.failures = self.failures.saturating_add(1);
        let delay = MIN_RETRY
            .checked_mul(1 << self.failures.saturating_sub(1).min(10))
            .unwrap_or(MAX_IDLE)
            .min(MAX_IDLE);
        self.due = Instant::now().checked_add(delay);
        delay
    }
    fn ready(&self) -> bool {
        self.due.is_none_or(|due| due <= Instant::now())
    }
    fn reset(&mut self) {
        *self = Self::default();
    }
}
pub struct WorkerRuntime<W: Worker<C>, C: Send + Sync + 'static = ()> {
    db: Arc<SqliteDb>,
    worker: Arc<W>,
    notify: Arc<Notify>,
    source: DurableEventSource,
    health: WorkerHealth,
    wait: Mutex<Option<Wait>>,
    tick_schedule: Mutex<Schedule>,
    transient_schedule: Mutex<Schedule>,
    bad_timestamps: Mutex<HashSet<&'static str>>,
    committed_type: PhantomData<fn() -> C>,
    #[cfg(test)]
    idle_entered: Notify,
    #[cfg(test)]
    cycle_finished: Notify,
    #[cfg(test)]
    loop_starts: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    write_transactions: std::sync::atomic::AtomicU64,
}
#[derive(Debug)]
enum PollResult {
    Progress,
    Scanned,
    Retry(Duration),
    InfrastructureRetry(Duration),
    Idle,
}

impl<W: Worker<C>, C: Send + Sync + 'static> WorkerRuntime<W, C> {
    pub fn new(db: Arc<SqliteDb>, worker: Arc<W>) -> Self {
        Self {
            source: DurableEventSource::new(Arc::clone(&db), worker.name(), worker.subscription()),
            health: WorkerHealth::new(Arc::clone(&db), worker.name()),
            notify: db.domain_event_notify(),
            db,
            worker,
            wait: Mutex::new(None),
            tick_schedule: Mutex::new(Schedule::default()),
            transient_schedule: Mutex::new(Schedule::default()),
            bad_timestamps: Mutex::new(HashSet::new()),
            committed_type: PhantomData,
            #[cfg(test)]
            idle_entered: Notify::new(),
            #[cfg(test)]
            cycle_finished: Notify::new(),
            #[cfg(test)]
            loop_starts: std::sync::atomic::AtomicUsize::new(0),
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
        match self.poll_cycle(limit.clamp(1, CYCLE_LIMIT)).await {
            Ok((count, _)) => Ok(count),
            Err(error) => {
                let _ = self.health.report_error(&error.to_string()).await;
                Err(error)
            }
        }
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
    async fn poll_cycle(&self, limit: usize) -> Result<(usize, PollResult)> {
        self.initialize().await?;
        self.tick_if_due().await;
        let mut count = 0;
        let mut result = PollResult::Idle;
        for _ in 0..limit {
            result = self.poll_event().await?;
            match result {
                PollResult::Progress => count += 1,
                PollResult::Scanned => {}
                _ => break,
            }
        }
        if matches!(result, PollResult::Scanned) {
            let cursor = self.source.cursor().await?;
            if self.source.flush_due(cursor) {
                let mut tx = self.begin_write().await?;
                self.source.flush_in_tx(&mut tx, cursor).await?;
                tx.commit().await?;
                self.source.flushed();
            }
        }
        if !matches!(result, PollResult::InfrastructureRetry(_)) {
            self.health
                .clear_error_if_set(HealthErrorKind::Runtime)
                .await?;
        }
        #[cfg(test)]
        self.cycle_finished.notify_one();
        Ok((count, result))
    }
    async fn tick_if_due(&self) {
        if !self
            .tick_schedule
            .lock()
            .expect("tick schedule lock")
            .ready()
        {
            return;
        }
        match catch_worker(async { self.worker.tick().await }, "worker tick panicked").await {
            Ok(()) => {
                self.tick_schedule
                    .lock()
                    .expect("tick schedule lock")
                    .reset();
                if let Err(error) = self.health.clear_error_if_set(HealthErrorKind::Tick).await {
                    tracing::warn!(worker = self.worker.name(), %error, "failed to clear tick error");
                }
            }
            Err(error) => {
                self.tick_schedule
                    .lock()
                    .expect("tick schedule lock")
                    .failed();
                tracing::warn!(worker = self.worker.name(), %error, "worker tick failed");
                if let Err(report) = self
                    .health
                    .report_error_kind(HealthErrorKind::Tick, error.message())
                    .await
                {
                    tracing::warn!(worker = self.worker.name(), %report, "failed to report tick error");
                }
            }
        }
    }
    async fn run_loop(&self, shutdown: watch::Receiver<bool>) -> Result<()> {
        self.run_loop_with_idle(shutdown, MIN_IDLE).await
    }
    async fn run_loop_with_idle(
        &self,
        mut shutdown: watch::Receiver<bool>,
        initial_idle: Duration,
    ) -> Result<()> {
        #[cfg(test)]
        self.loop_starts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.initialize().await?;
        let mut idle = initial_idle;
        loop {
            if *shutdown.borrow_and_update() {
                return Ok(());
            }
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let delay = match self.poll_cycle(CYCLE_LIMIT).await {
                Ok((_, PollResult::Progress | PollResult::Scanned)) => {
                    idle = MIN_IDLE;
                    continue;
                }
                Ok((_, PollResult::Retry(delay) | PollResult::InfrastructureRetry(delay))) => {
                    delay.min(MAX_IDLE)
                }
                Ok((_, PollResult::Idle)) => {
                    #[cfg(test)]
                    self.idle_entered.notify_one();
                    let delay = idle;
                    idle = idle.saturating_mul(2).min(MAX_IDLE);
                    delay
                }
                Err(error) => {
                    tracing::warn!(worker = self.worker.name(), %error, "worker runtime poll failed");
                    let _ = self.health.report_error(&error.to_string()).await;
                    let delay = idle;
                    idle = idle.saturating_mul(2).min(MAX_IDLE);
                    delay
                }
            };
            let tick_due = self.tick_schedule.lock().expect("tick schedule lock").due;
            let delay = tick_due
                .and_then(|due| due.checked_duration_since(Instant::now()))
                .map_or(delay, |tick| delay.min(tick));
            tokio::select! { _ = shutdown_signal(&mut shutdown) => return Ok(()),
            _ = &mut notified => {}, _ = tokio::time::sleep(delay.min(MAX_IDLE)) => {} }
        }
    }
    #[cfg(test)]
    async fn poll_once(&self) -> Result<PollResult> {
        Ok(self.poll_cycle(1).await?.1)
    }
    async fn poll_event(&self) -> Result<PollResult> {
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
        if let Some((delay, infrastructure)) = self.pending_delay(&key).await? {
            return Ok(if infrastructure {
                PollResult::InfrastructureRetry(delay)
            } else {
                PollResult::Retry(delay)
            });
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
                let committed = match catch_worker(
                    async { self.worker.commit(&mut tx, &event, &prepared).await },
                    "worker commit panicked",
                )
                .await
                {
                    Ok(committed) => committed,
                    Err(error) => {
                        tx.rollback().await?;
                        return self.worker_failure(cursor, &event, error).await;
                    }
                };
                self.source
                    .acknowledge_in_tx(&mut tx, cursor, event.sequence)
                    .await?;
                self.health.success_in_tx(&mut tx, &key).await?;
                tx.commit().await?;
                self.clear_wait();
                self.source.flushed();
                if let Err(error) = catch_worker(
                    async {
                        self.worker
                            .after_commit(&event, &prepared, &committed)
                            .await
                    },
                    "worker after_commit panicked",
                )
                .await
                {
                    tracing::warn!(worker = self.worker.name(), %error, "worker after_commit failed");
                    let _ = self
                        .health
                        .report_error_kind(HealthErrorKind::AfterCommit, error.message())
                        .await;
                }
                Ok(PollResult::Progress)
            }
            Ok(Outcome::Skip) => {
                self.source.skip(event.sequence);
                self.clear_wait();
                Ok(PollResult::Scanned)
            }
            Ok(Outcome::Defer { after, reason }) => {
                self.transient_schedule
                    .lock()
                    .expect("transient schedule lock")
                    .reset();
                let after = db::clamp_worker_deferral(after);
                let mut tx = self.begin_write().await?;
                self.source
                    .validate_in_tx(&mut tx, cursor, event.sequence)
                    .await?;
                self.health
                    .defer_in_tx(&mut tx, &key, after, &reason)
                    .await?;
                tx.commit().await?;
                self.set_wait(key, after, false);
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
            let delay = self
                .transient_schedule
                .lock()
                .expect("transient schedule lock")
                .failed();
            self.set_wait(event.sequence.to_string(), delay, true);
            return Ok(PollResult::InfrastructureRetry(delay));
        }
        self.transient_schedule
            .lock()
            .expect("transient schedule lock")
            .reset();
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
                self.set_wait(key, delay, false);
                Ok(PollResult::Retry(delay))
            }
            PoisonDecision::DeadLettered => {
                self.clear_wait();
                self.source.flushed();
                Ok(PollResult::Progress)
            }
        }
    }
    async fn pending_delay(&self, key: &str) -> Result<Option<(Duration, bool)>> {
        {
            let wait = self.wait.lock().expect("worker wait lock");
            if let Some(wait) = wait.as_ref() {
                if wait.key == key {
                    return Ok(wait
                        .deadline
                        .checked_duration_since(Instant::now())
                        .filter(|d| !d.is_zero())
                        .map(|delay| (delay, wait.infrastructure)));
                }
            }
        }
        let state = self.health.wait_state().await?;
        let mut delay = Duration::ZERO;
        for (field, pending_key, at, cap) in [
            (
                "retry",
                state.retry_key,
                state.retry_at,
                self.worker.retry_policy().delay(u32::MAX),
            ),
            (
                "defer",
                state.defer_key,
                state.defer_at,
                Duration::from_secs(3600),
            ),
        ] {
            if pending_key.as_deref() != Some(key) {
                continue;
            }
            if let Some(at) = at {
                if let Ok(time) = DateTime::parse_from_rfc3339(&at) {
                    delay = delay.max(
                        (time.with_timezone(&Utc) - Utc::now())
                            .to_std()
                            .unwrap_or(Duration::ZERO)
                            .min(cap),
                    );
                } else if self
                    .bad_timestamps
                    .lock()
                    .expect("timestamp warning lock")
                    .insert(field)
                {
                    tracing::warn!(
                        worker = self.worker.name(),
                        field,
                        "invalid worker wait timestamp; treating as due"
                    );
                }
            }
        }
        if delay.is_zero() {
            return Ok(None);
        }
        self.set_wait(key.to_owned(), delay, false);
        Ok(Some((delay, false)))
    }
    fn set_wait(&self, key: String, delay: Duration, infrastructure: bool) {
        let deadline = Instant::now()
            .checked_add(delay)
            .unwrap_or_else(Instant::now);
        *self.wait.lock().expect("worker wait lock") = Some(Wait {
            key,
            deadline,
            infrastructure,
        });
    }
    fn clear_wait(&self) {
        *self.wait.lock().expect("worker wait lock") = None;
        self.transient_schedule
            .lock()
            .expect("transient schedule lock")
            .reset();
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
