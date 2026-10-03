//! Source-free ticks. Domain workers own their timers and wake signals; the
//! shared supervisor owns restart, while this context bounds and reports work.
use super::{HealthErrorKind, SupervisorPolicy, WorkerHealth, WorkerSupervisor};
use crate::{Result, ServiceError};
use api_types::PeriodicWorkerStatus;
use db::SqliteDb;
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle};

#[derive(Default)]
struct State {
    running: bool,
    last_tick_at: Option<String>,
}

#[cfg(test)]
mod tests;

/// One registry per runtime, including server-only workers. Stopped entries
/// remain visible; another process's persisted health is never called running.
pub struct PeriodicWorkers {
    db: Arc<SqliteDb>,
    entries: Mutex<BTreeMap<String, Arc<Mutex<State>>>>,
}

impl PeriodicWorkers {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self {
            db,
            entries: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn worker(&self, name: &str) -> PeriodicWorker {
        let state = self
            .entries
            .lock()
            .expect("periodic worker registry")
            .entry(name.to_owned())
            .or_default()
            .clone();
        PeriodicWorker {
            health: WorkerHealth::new(Arc::clone(&self.db), name),
            state,
            timeout: Duration::from_secs(300),
        }
    }

    pub async fn status(&self) -> Result<Vec<PeriodicWorkerStatus>> {
        let entries: Vec<_> = self
            .entries
            .lock()
            .expect("periodic worker registry")
            .iter()
            .map(|(name, state)| (name.clone(), Arc::clone(state)))
            .collect();
        let mut statuses = Vec::with_capacity(entries.len());
        for (worker_name, state) in entries {
            let stored: Option<(Option<String>, Option<String>, i64)> = sqlx::query_as(
                "SELECT last_error, last_error_at, restart_count FROM worker_health WHERE worker_name = ?")
                .bind(&worker_name).fetch_optional(self.db.pool()).await?;
            let (last_error, last_error_at, restart_count) = stored.unwrap_or_default();
            let state = state.lock().expect("periodic worker state");
            statuses.push(PeriodicWorkerStatus {
                worker_name,
                running: state.running,
                last_tick_at: state.last_tick_at.clone(),
                last_error,
                last_error_at,
                restart_count,
            });
        }
        Ok(statuses)
    }
}

#[derive(Clone)]
pub struct PeriodicWorker {
    health: WorkerHealth,
    state: Arc<Mutex<State>>,
    timeout: Duration,
}

struct Running(Arc<Mutex<State>>);
impl Drop for Running {
    fn drop(&mut self) {
        self.0.lock().expect("periodic worker state").running = false;
    }
}

impl PeriodicWorker {
    pub fn with_tick_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn start<F, Fut>(
        self,
        shutdown: watch::Receiver<bool>,
        stopped: impl Fn() -> bool + Send + Sync + 'static,
        run: F,
    ) -> JoinHandle<()>
    where
        F: Fn(Self, watch::Receiver<bool>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        WorkerSupervisor::new(self.health.clone(), SupervisorPolicy::default())
            .with_stop(stopped)
            // Preserve completion of in-flight ticks on a normal shutdown.
            // RuntimeSupervisor already imposes the same ten-second deadline.
            .with_shutdown_grace(Duration::from_secs(10))
            .start(
                move |shutdown| {
                    let worker = self.clone();
                    worker.state.lock().expect("periodic worker state").running = true;
                    let running = Running(Arc::clone(&worker.state));
                    let future = run(worker, shutdown);
                    async move {
                        let _running = running;
                        future.await
                    }
                },
                shutdown,
            )
    }

    /// Workers with their own atomic stop flag/Notify retain that lifecycle;
    /// an intentional stop must not be treated as an unexpected loop return.
    pub fn start_stoppable<F, Fut>(
        self,
        stopped: impl Fn() -> bool + Send + Sync + 'static,
        run: F,
    ) -> JoinHandle<()>
    where
        F: Fn(Self) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let (sender, receiver) = watch::channel(false);
        self.start(receiver, stopped, move |worker, _| {
            let _keep_open = &sender;
            run(worker)
        })
    }

    /// A panic escapes to WorkerSupervisor, which records and restarts the
    /// session. Timeout drops the tick future and retains the original cadence.
    pub async fn tick<T>(&self, tick: impl Future<Output = Result<T>>) -> Result<T> {
        self.state
            .lock()
            .expect("periodic worker state")
            .last_tick_at = Some(db::now_rfc3339());
        let result = tokio::time::timeout(self.timeout, tick)
            .await
            .unwrap_or_else(|_| {
                Err(ServiceError::invalid_operation(
                    "periodic worker tick timed out",
                ))
            });
        match &result {
            Ok(_) => {
                for kind in [HealthErrorKind::Tick, HealthErrorKind::Runtime] {
                    if let Err(error) = self.health.clear_error_if_set(kind).await {
                        tracing::warn!(worker = self.health.name(), %error, "failed to clear periodic worker error");
                    }
                }
            }
            Err(error) => {
                tracing::warn!(worker = self.health.name(), %error, "periodic worker tick failed");
                if let Err(report) = self
                    .health
                    .report_error_kind(HealthErrorKind::Tick, &error.to_string())
                    .await
                {
                    tracing::warn!(worker = self.health.name(), %report, "failed to report periodic worker error");
                }
            }
        }
        result
    }

    /// Original immediate-pass / wait-after-pass schedule used by dispatch,
    /// heartbeat, daemon reporting and external sync. Waits are NOT tick work.
    pub async fn run<T, Tick, Wait, TickFuture, WaitFuture>(
        &self,
        stopped: impl Fn() -> bool,
        tick: Tick,
        wait: Wait,
    ) -> Result<()>
    where
        Tick: Fn() -> TickFuture,
        Wait: Fn() -> WaitFuture,
        TickFuture: Future<Output = Result<T>>,
        WaitFuture: Future<Output = ()>,
    {
        while !stopped() {
            let _ = self.tick(tick()).await;
            wait().await;
        }
        Ok(())
    }
}
