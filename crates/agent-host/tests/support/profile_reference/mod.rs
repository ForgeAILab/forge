//! Unpublished reference packed SQLite adapter and physical-disk harness.
//! Sealing models nonce/tag lengths; this is not an encrypted production store.
mod codec;
mod metadata;
mod packed;
use agent_runtime::core::error::{
    ErrorKind, FailureClass, FailureComponent, FailureStage, RuntimeError,
};
pub use packed::PackedSqliteJournalStorage;
fn conflict() -> RuntimeError {
    RuntimeError::conflict("packed journal conflict").with_class(FailureClass::StateConflict {
        stage: FailureStage::PreProvider,
        component: FailureComponent::Checkpoint,
    })
}
fn failure<T>(_: T) -> RuntimeError {
    RuntimeError::internal("packed journal storage failure").with_class(
        FailureClass::HostComponent {
            stage: FailureStage::PreProvider,
            component: FailureComponent::Checkpoint,
        },
    )
}
fn invalid_data() -> RuntimeError {
    RuntimeError::new(ErrorKind::Serialization, "invalid packed journal state").with_class(
        FailureClass::HostComponent {
            stage: FailureStage::PreProvider,
            component: FailureComponent::Checkpoint,
        },
    )
}
fn sqlite_failure(error: rusqlite::Error) -> RuntimeError {
    let retryable = matches!(
        &error,
        rusqlite::Error::SqliteFailure(code, _)
            if matches!(code.code, rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
    );
    let error = failure(error);
    if retryable { error.retryable() } else { error }
}
fn commit_failure(error: rusqlite::Error) -> RuntimeError {
    // A WAL commit frame may survive an I/O error reported while syncing it.
    // Other failures that prove the transaction was not written stay terminal.
    let ambiguous = matches!(
        &error,
        rusqlite::Error::SqliteFailure(code, _) if code.code == rusqlite::ErrorCode::SystemIoFailure
    );
    let error = sqlite_failure(error);
    if ambiguous { error.retryable() } else { error }
}
