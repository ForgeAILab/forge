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
    /// Deterministic rejection discovered at the transaction boundary.
    Terminal,
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
    pub fn terminal(message: impl Into<String>) -> Self {
        Self {
            kind: WorkerErrorKind::Terminal,
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

impl WorkerErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Failure => "failure",
            Self::Transient => "transient",
            Self::Terminal => "terminal",
        }
    }
}
/// Classification shared by the durable projections. Domain refusals cannot
/// become valid by repeating the same event; availability and CAS races can.
pub fn consumer_error(error: crate::ServiceError) -> WorkerError {
    use crate::ServiceError as S;
    use db::DbError as D;
    let message = error.to_string();
    match &error {
        S::Db(e) if e.is_transient() => WorkerError::transient(message),
        S::Db(
            D::NotFound
            | D::Check(_)
            | D::IdempotencyConflict
            | D::InvalidTransition
            | D::InvalidTaskMove(_)
            | D::TurnNotRetryable
            | D::InvalidCursor,
        )
        | S::InvalidOperation { .. }
        | S::NotFound { .. }
        | S::AuthorizationDenied { .. } => WorkerError::terminal(message),
        S::ExecutionAlreadyRunning { .. }
        | S::RateLimited { .. }
        | S::DaemonUnavailable { .. }
        | S::DaemonNotReady { .. }
        | S::DaemonTimeout { .. } => WorkerError::transient(message),
        _ => WorkerError::new(message),
    }
}
