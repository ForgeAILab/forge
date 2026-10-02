use std::{fmt, time::Duration};

#[derive(Debug)]
pub enum Outcome<T> {
    Done(T),
    /// Classification rejected this event. No effect; lazy checkpoint.
    Skip,
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
    pub fn database(context: &str, error: db::DbError) -> Self {
        let message = format!("{context}: {error}");
        if error.is_transient() {
            Self::transient(message)
        } else {
            Self::new(message)
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
