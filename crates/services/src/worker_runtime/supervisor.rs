//! Supervision independent of any work source or ordering protocol.
use super::WorkerHealth;
use crate::Result;
use std::{future::Future, sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinHandle, time::Instant};

#[derive(Debug, Clone, Copy)]
pub struct SupervisorPolicy {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub healthy_period: Duration,
}
impl Default for SupervisorPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_millis(250),
            max_backoff: Duration::from_secs(5),
            healthy_period: Duration::from_secs(60),
        }
    }
}
pub struct WorkerSupervisor {
    health: WorkerHealth,
    policy: SupervisorPolicy,
    #[cfg(test)]
    child_abort: Option<Arc<std::sync::Mutex<Option<tokio::task::AbortHandle>>>>,
    #[cfg(test)]
    backoff_entered: Option<Arc<tokio::sync::Notify>>,
}
impl WorkerSupervisor {
    pub fn new(health: WorkerHealth, policy: SupervisorPolicy) -> Self {
        Self {
            health,
            policy,
            #[cfg(test)]
            child_abort: None,
            #[cfg(test)]
            backoff_entered: None,
        }
    }
    #[cfg(test)]
    pub(super) fn with_backoff_signal(mut self, signal: Arc<tokio::sync::Notify>) -> Self {
        self.backoff_entered = Some(signal);
        self
    }
    #[cfg(test)]
    pub(crate) fn with_child_abort(
        mut self,
        slot: Arc<std::sync::Mutex<Option<tokio::task::AbortHandle>>>,
    ) -> Self {
        self.child_abort = Some(slot);
        self
    }
    pub fn start<F, Fut>(self, run: F, mut shutdown: watch::Receiver<bool>) -> JoinHandle<()>
    where
        F: Fn(watch::Receiver<bool>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let run = Arc::new(run);
        tokio::spawn(async move {
            let mut backoff = self.policy.initial_backoff;
            loop {
                if *shutdown.borrow_and_update() {
                    return;
                }
                let started = Instant::now();
                let child_run = Arc::clone(&run);
                let child_shutdown = shutdown.clone();
                let mut child = tokio::spawn(async move { child_run(child_shutdown).await });
                #[cfg(test)]
                if let Some(slot) = &self.child_abort {
                    *slot.lock().unwrap() = Some(child.abort_handle());
                }

                // A false watch change stays in this select with the SAME child.
                let exit = tokio::select! {
                        _ = shutdown_signal(&mut shutdown) => {
                            child.abort(); let _ = child.await; return;
                        }
                        result = &mut child => result,
                };
                if *shutdown.borrow_and_update() {
                    return;
                }
                if started.elapsed() >= self.policy.healthy_period {
                    backoff = self.policy.initial_backoff;
                }
                let reason = match exit {
                    Ok(Ok(())) => "worker loop exited unexpectedly",
                    Ok(Err(_)) => "worker loop returned an error",
                    Err(error) if error.is_panic() => "worker runtime panicked",
                    Err(_) => "worker loop cancelled",
                };
                tokio::select! {
                    _ = shutdown_signal(&mut shutdown) => return,
                    result = self.health.record_restart(reason) => {
                        if let Err(error) = result {
                            tracing::warn!(worker = self.health.name(), %error, "failed to persist worker restart");
                        }
                    }
                }
                #[cfg(test)]
                if let Some(signal) = &self.backoff_entered {
                    signal.notify_one();
                }
                tokio::select! {
                    _ = shutdown_signal(&mut shutdown) => return,
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = backoff.saturating_mul(2).min(self.policy.max_backoff);
            }
        })
    }
}
pub(super) async fn shutdown_signal(shutdown: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow_and_update() || shutdown.changed().await.is_err() {
            return;
        }
    }
}
