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

const LAST_TICK_SAMPLE_INTERVAL: Duration = Duration::from_secs(30);

struct State {
    running: bool,
    last_tick_at: Option<String>,
    last_tick_sample: Option<tokio::time::Instant>,
    needs_error_clear: bool,
    reported_error: Option<String>,
    #[cfg(test)]
    health_clear_checks: usize,
}
impl Default for State {
    fn default() -> Self {
        Self {
            running: false,
            last_tick_at: None,
            last_tick_sample: None,
            needs_error_clear: true,
            reported_error: None,
            #[cfg(test)]
            health_clear_checks: 0,
        }
    }
}

#[derive(Clone, Copy)]
enum BudgetMode {
    Cancel,
    Warn,
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
            budget: Duration::from_secs(300),
            budget_mode: BudgetMode::Cancel,
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
    budget: Duration,
    budget_mode: BudgetMode,
}

struct Running(Arc<Mutex<State>>);
impl Drop for Running {
    fn drop(&mut self) {
        self.0.lock().expect("periodic worker state").running = false;
    }
}

impl PeriodicWorker {
    pub fn with_tick_timeout(mut self, timeout: Duration) -> Self {
        self.budget = timeout;
        self.budget_mode = BudgetMode::Cancel;
        self
    }

    /// Workflow transitions and committed admission batches must not be dropped
    /// by an aggregate deadline. Warn once and await the same work to completion.
    pub fn with_stall_budget(mut self, budget: Duration) -> Self {
        self.budget = budget;
        self.budget_mode = BudgetMode::Warn;
        self
    }

    pub fn name(&self) -> &str {
        self.health.name()
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
                    {
                        let mut state = worker.state.lock().expect("periodic worker state");
                        state.running = true;
                        // The supervisor may have recorded a runtime panic since
                        // the previous child. Clear it on this child's first success.
                        state.needs_error_clear = true;
                        state.reported_error = None;
                    }
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

    /// Only cancel-safe work uses a cancelling timeout. A warning budget keeps
    /// polling the SAME future, including while health persistence is pending.
    pub async fn tick<T>(&self, tick: impl Future<Output = Result<T>>) -> Result<T> {
        {
            let mut state = self.state.lock().expect("periodic worker state");
            if state
                .last_tick_sample
                .is_none_or(|last| last.elapsed() >= LAST_TICK_SAMPLE_INTERVAL)
            {
                state.last_tick_at = Some(db::now_rfc3339());
                state.last_tick_sample = Some(tokio::time::Instant::now());
            }
        }
        tokio::pin!(tick);
        let result = match self.budget_mode {
            BudgetMode::Cancel => tokio::time::timeout(self.budget, &mut tick)
                .await
                .unwrap_or_else(|_| {
                    Err(ServiceError::invalid_operation(format!(
                        "periodic worker tick timed out after {:?}",
                        self.budget
                    )))
                }),
            BudgetMode::Warn => tokio::select! {
                biased;
                result = &mut tick => result,
                _ = tokio::time::sleep(self.budget) => {
                    let reason = format!("tick running longer than {:?}", self.budget);
                    tracing::warn!(worker = self.name(), "{reason}");
                    // A tick may hold a DB writer transaction. Poll it alongside
                    // the report so the writer can finish and release its lock.
                    let (_, result) = tokio::join!(self.report_tick_error(&reason), &mut tick);
                    result
                }
            },
        };
        match &result {
            Ok(_) => self.clear_errors_after_success().await,
            Err(error) => self.report_tick_error(&error.to_string()).await,
        }
        result
    }

    async fn report_tick_error(&self, reason: &str) {
        let reason = super::policy::bounded_error(reason);
        {
            let mut state = self.state.lock().expect("periodic worker state");
            state.needs_error_clear = true;
            if state.reported_error.as_deref() == Some(reason.as_str()) {
                return;
            }
        }
        match self
            .health
            .report_error_kind(HealthErrorKind::Tick, &reason)
            .await
        {
            Ok(()) => {
                self.state
                    .lock()
                    .expect("periodic worker state")
                    .reported_error = Some(reason)
            }
            Err(error) => {
                tracing::warn!(worker = self.name(), %error, "failed to report periodic worker error")
            }
        }
    }

    async fn clear_errors_after_success(&self) {
        if !self
            .state
            .lock()
            .expect("periodic worker state")
            .needs_error_clear
        {
            return;
        }
        let mut cleared = true;
        for kind in [HealthErrorKind::Tick, HealthErrorKind::Runtime] {
            #[cfg(test)]
            {
                self.state
                    .lock()
                    .expect("periodic worker state")
                    .health_clear_checks += 1;
            }
            if let Err(error) = self.health.clear_error_if_set(kind).await {
                cleared = false;
                tracing::warn!(worker = self.name(), %error, "failed to clear periodic worker error");
            }
        }
        if cleared {
            let mut state = self.state.lock().expect("periodic worker state");
            state.needs_error_clear = false;
            state.reported_error = None;
        }
    }

    /// Original immediate-pass / wait-after-pass schedule used by dispatch,
    /// heartbeat, daemon reporting and external sync. Waits are NOT tick work.
    pub async fn run<T, Tick, Wait, TickFuture, WaitFuture>(
        &self,
        stopped: impl Fn() -> bool,
        failure_message: &'static str,
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
            if let Err(error) = self.tick(tick()).await {
                tracing::warn!(worker = self.name(), %error, "{failure_message}");
            }
            wait().await;
        }
        Ok(())
    }
}
