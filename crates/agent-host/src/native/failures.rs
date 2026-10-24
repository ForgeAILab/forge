//! Typed provider and runtime failure mapping and terminal cleanup.

use super::*;

// Prefer the original attempt evidence when it is available.
pub(crate) fn provider_turn_failure(
    error: &agent_runtime::core::provider::ProviderError,
) -> api_types::TurnFailure {
    use agent_runtime::core::provider::ProviderErrorKind;
    use api_types::TurnFailure;
    match error.kind {
        // A provider can mark a rejection retryable (e.g. a transient 4xx);
        // only a non-retryable rejection is a deterministic schema failure.
        ProviderErrorKind::BadRequest | ProviderErrorKind::Unsupported if error.retryable => {
            TurnFailure::ProviderRejected {
                retryable: true,
                retry_after: error.retry_after_ms,
            }
        }
        ProviderErrorKind::BadRequest | ProviderErrorKind::Unsupported => {
            TurnFailure::ProviderSchema
        }
        ProviderErrorKind::Auth => TurnFailure::ProviderAuth,
        ProviderErrorKind::LimitExhausted => TurnFailure::UsageLimit {
            resets_at: error.limit_resets_at_ms,
        },
        ProviderErrorKind::Timeout
        | ProviderErrorKind::RateLimited
        | ProviderErrorKind::Network
        | ProviderErrorKind::Server => TurnFailure::Transient {
            retry_after: error.retry_after_ms,
        },
        _ if error.retryable => TurnFailure::Transient {
            retry_after: error.retry_after_ms,
        },
        _ => TurnFailure::Unclassified,
    }
}

pub(super) fn runtime_turn_failure(error: &RuntimeError) -> api_types::TurnFailure {
    use agent_runtime::core::error::{ErrorKind, FailureClass, FailureStage};
    use api_types::TurnFailure;
    match &error.class {
        FailureClass::PolicyDenied { .. } => return TurnFailure::Authority,
        FailureClass::ContextOverflow { .. } => return TurnFailure::ContextOverflow,
        FailureClass::Auth { .. } => return TurnFailure::ProviderAuth,
        FailureClass::QuotaExhausted { .. } => {
            return TurnFailure::UsageLimit {
                resets_at: error.limit_resets_at_ms,
            };
        }
        FailureClass::RequestRejected {
            stage: FailureStage::Provider,
        } => {
            return if error.retryable {
                TurnFailure::ProviderRejected {
                    retryable: true,
                    retry_after: error.retry_after_ms,
                }
            } else {
                TurnFailure::ProviderSchema
            };
        }
        FailureClass::TurnLimit { limit, .. } => {
            return AgentHostError::TurnLimitReached {
                limit: (*limit).into(),
            }
            .turn_failure();
        }
        // Classification alone cannot admit retries. In particular, a local
        // Config/nonretryable error may retain Transient provider evidence.
        FailureClass::Transient { .. } | FailureClass::RateLimited { .. } => {}
        // Forge has no dedicated projection for component/state failures or
        // cancellation here. Keep the coarse kind/retryability policy below;
        // a conflict must not be promoted into a transient provider failure.
        FailureClass::RequestRejected { .. }
        | FailureClass::StateConflict { .. }
        | FailureClass::HostComponent { .. }
        | FailureClass::Cancelled { .. }
        | FailureClass::Internal { .. }
        | FailureClass::Unclassified => {}
        // Future classes retain the existing conservative coarse projection.
        _ => {}
    }
    match error.kind {
        ErrorKind::Config => TurnFailure::Unclassified,
        ErrorKind::Approval | ErrorKind::Workspace => TurnFailure::Authority,
        _ if error.retryable => TurnFailure::Transient {
            retry_after: error.retry_after_ms,
        },
        _ => TurnFailure::Unclassified,
    }
}

/// Cleanup cannot replace a terminal failure or discard its usage envelope.
pub(super) fn native_turn_cleanup(
    primary_failed: bool,
    cleanup: Vec<(&'static str, Result<(), RuntimeError>)>,
) -> Result<(), AgentHostError> {
    let mut primary_cleanup_error = None;
    for (operation, result) in cleanup {
        if let Err(error) = result {
            if primary_failed && error.kind == agent_runtime::core::error::ErrorKind::Conflict {
                tracing::debug!(target: "forge_session_journal", operation,
                    "terminal turn cleanup remains fenced until reopen");
            } else if primary_failed || primary_cleanup_error.is_some() {
                tracing::warn!(target: "forge_session_journal", operation, kind=?error.kind,
                    "secondary native turn cleanup failed");
            } else {
                primary_cleanup_error = Some(host_runtime_error(error));
            }
        }
    }
    match primary_cleanup_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

pub(super) fn host_runtime_error(error: RuntimeError) -> AgentHostError {
    AgentHostError::Runtime(error.to_string())
}

#[cfg(test)]
mod tests;
