//! Forge's encrypted persistence boundary for the runtime-owned journal engine.
//! This module is not yet connected to the native runtime stores.
mod apply;
mod cache;
mod codec;
mod rows;
mod seal;
mod storage;
mod wire;
pub use storage::ForgeJournalStorage;
#[cfg(feature = "test-support")]
pub use storage::JournalCacheStats;

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
