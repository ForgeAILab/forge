//! Forge's encrypted persistence boundary for the runtime-owned journal engine.
//! JournalEngine owns semantic validation and collection for the native stores.
mod apply;
#[cfg(feature = "test-support")]
mod audit;
mod cache;
mod codec;
mod diagnostics;
pub use diagnostics::JournalByteStats;
mod maintenance;
mod operations;
mod release;
mod rows;
mod seal;
mod storage;
mod wire;
pub use storage::ForgeJournalStorage;
#[cfg(feature = "test-support")]
pub use storage::JournalCacheStats;

/// A sealed unit that `ForgeJournalStorage::reseal_unit_for_test` rewrites.
#[cfg(feature = "test-support")]
#[derive(Clone, Copy, Debug)]
pub enum TestSealedUnit {
    /// A pack's prefix/offset directory, by pack id.
    PackIndex(i64),
    /// A pack's compressed body frame, by pack id.
    PackBody(i64),
    /// A pack's decompressed canonical bodies, recompressed after mutation.
    PackBodyCanonical(i64),
    /// An operation pack's decompressed records, by tier.
    Operations(i64),
    /// An operation pack's compressed frame, by tier.
    OperationsFrame(i64),
}

use agent_runtime::core::error::{
    ErrorKind, FailureClass, FailureComponent, FailureStage, RuntimeError,
};

fn corrupt() -> RuntimeError {
    RuntimeError::new(
        ErrorKind::Serialization,
        "invalid authenticated journal storage",
    )
    .with_class(FailureClass::HostComponent {
        stage: FailureStage::PreProvider,
        component: FailureComponent::Checkpoint,
    })
}

fn conflict() -> RuntimeError {
    RuntimeError::conflict("journal storage fence or immutable body conflict").with_class(
        FailureClass::StateConflict {
            stage: FailureStage::PreProvider,
            component: FailureComponent::Checkpoint,
        },
    )
}

#[cfg(feature = "test-support")]
pub mod profile;
