use std::{fmt, time::Duration};

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
        self.initial_backoff
            .checked_mul(1 << attempts.saturating_sub(1).min(31))
            .unwrap_or(self.max_backoff)
            .min(self.max_backoff)
    }
}

#[derive(Debug)]
pub enum Outcome<T> {
    Done(T),
    /// Readiness wait, not a strike. The first deferral time survives retries.
    Defer {
        after: Duration,
        reason: String,
    },
    DeadLetter {
        reason: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerErrorKind {
    Failure,
    Transient,
}

/// Messages must exclude payloads, credentials and other secret material.
#[derive(Debug, Clone)]
pub struct WorkerError {
    pub kind: WorkerErrorKind,
    message: String,
}
impl WorkerError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            kind: WorkerErrorKind::Failure,
            message: bounded_error(&message.into()),
        }
    }
    pub fn transient(message: impl Into<String>) -> Self {
        Self {
            kind: WorkerErrorKind::Transient,
            message: bounded_error(&message.into()),
        }
    }
    pub fn message(&self) -> &str {
        &self.message
    }
}
impl fmt::Display for WorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for WorkerError {}

pub(super) fn bounded_error(value: &str) -> String {
    value
        .chars()
        .filter(|c| !c.is_control())
        .take(1_024)
        .collect()
}
