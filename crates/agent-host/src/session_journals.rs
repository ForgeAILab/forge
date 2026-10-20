//! Long-lived host ownership of the runtime's two journal domains.
use std::sync::Arc;

use agent_runtime::core::{
    error::RuntimeError,
    journal::{
        JournalEngine, JournalEngineConfig, JournalProjectionPolicy, JournalStorage,
        JournalStorageId,
    },
    store::{SessionSnapshot, SessionStateSensitivity},
};

use crate::session_journal::ForgeJournalStorage;

use db::{SESSION_JOURNAL_ORDINARY_STORAGE_ID, SESSION_JOURNAL_PROTECTED_STORAGE_ID};
/// Estimated retained-root charge, independently for each engine; not an RSS cap.
pub const JOURNAL_MEMO_BYTES: usize = 128 * 1024 * 1024;

/// Only this host-owned namespace family declares optional diagnostics. Unknown
/// extensions (including LCM), conversation, manifests and runtime cache remain
/// exact: any may be required to restore an ordinary-only session. Diagnostics
/// must never carry recovery state. Sensitivity alone does not imply optionality.
#[derive(Debug)]
pub struct ForgeJournalProjection;
impl JournalProjectionPolicy for ForgeJournalProjection {
    fn project(&self, mut snapshot: SessionSnapshot) -> Result<SessionSnapshot, RuntimeError> {
        snapshot.extension_state.retain(|namespace, state| {
            !(namespace.starts_with("forge.diagnostics.")
                && state.sensitivity == SessionStateSensitivity::Sensitive)
        });
        Ok(snapshot)
    }
}

/// Construct once with the owning protected store. Cloning this owner or either
/// engine shares its gate, memo and lease registry with runtimes and collectors.
#[derive(Debug)]
pub struct SessionJournals<S: JournalStorage = ForgeJournalStorage> {
    storage: Arc<S>,
    ordinary: JournalEngine<S>,
    protected: JournalEngine<S>,
}
impl SessionJournals {
    pub(crate) fn new(storage: Arc<ForgeJournalStorage>) -> Self {
        Self::with_budget(storage, JOURNAL_MEMO_BYTES)
    }
}
impl<S: JournalStorage> SessionJournals<S> {
    fn with_budget(storage: Arc<S>, memo_bytes: usize) -> Self {
        let config = JournalEngineConfig {
            collection_enabled: true,
            validated_root_memo_bytes: memo_bytes,
        };
        Self {
            ordinary: JournalEngine::ordinary(
                storage.clone(),
                JournalStorageId::new(SESSION_JOURNAL_ORDINARY_STORAGE_ID),
                Arc::new(ForgeJournalProjection),
                config.clone(),
            ),
            protected: JournalEngine::protected(
                storage.clone(),
                JournalStorageId::new(SESSION_JOURNAL_PROTECTED_STORAGE_ID),
                config,
            ),
            storage,
        }
    }

    pub fn ordinary(&self) -> JournalEngine<S> {
        self.ordinary.clone()
    }

    pub fn protected(&self) -> JournalEngine<S> {
        self.protected.clone()
    }

    /// Shared storage for trait-based maintenance and diagnostics.
    pub fn storage(&self) -> Arc<S> {
        self.storage.clone()
    }

    #[cfg(feature = "test-support")]
    pub fn with_test_memo_budget(storage: Arc<S>, memo_bytes: usize) -> Self {
        Self::with_budget(storage, memo_bytes)
    }
}

impl<S: JournalStorage> Clone for SessionJournals<S> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            ordinary: self.ordinary.clone(),
            protected: self.protected.clone(),
        }
    }
}

impl<S: JournalStorage> SessionJournals<S> {
    /// Read one opaque extension under an engine-issued lease. Acquisition may
    /// validate a cold root; the reader operation itself never walks history or
    /// checkpoints. A warm root reuses the same owner memo as runtime opens.
    pub async fn extension(
        &self,
        session: &agent_runtime::core::ids::SessionId,
        flags: db::SessionPersistenceFlags,
        namespace: &str,
    ) -> Result<Option<agent_runtime::core::store::VersionedSessionState>, RuntimeError> {
        if flags.retired {
            return Err(RuntimeError::conflict("session journal is retired"));
        }
        for (published, journal) in [
            (flags.ordinary_published, &self.ordinary),
            (flags.protected_published, &self.protected),
        ] {
            if !published {
                continue;
            }
            let value = read_extension(journal, session, namespace).await?;
            if value.is_some() {
                return Ok(value);
            }
        }
        Ok(None)
    }
}

async fn read_extension<S: JournalStorage>(
    journal: &JournalEngine<S>,
    session: &agent_runtime::core::ids::SessionId,
    namespace: &str,
) -> Result<Option<agent_runtime::core::store::VersionedSessionState>, RuntimeError> {
    use agent_runtime::core::journal::*;
    let pin = db::new_uuid_v4();
    let lease = journal
        .open(
            session,
            JournalOpen {
                root: JournalRootSelection::Latest,
                expected_revision: None,
                operation: JournalOperationId::new(format!("forge-extension:{pin}")),
                pin: JournalPinId::new(pin),
                intent: JournalOpenIntent::ReadOnly,
            },
        )
        .await?;
    let mut guard = ExtensionLease {
        journal: journal.clone(),
        lease: Some(lease),
    };
    let result = async {
        let mut reader = JournalReader::new(
            journal,
            guard.lease.as_ref().expect("live probe lease"),
            JournalReadLimits {
                objects: 128,
                encoded_bytes: 8 * 1024 * 1024,
                entries: 100_000,
            },
        )?;
        reader.extension(namespace).await
    }
    .await;
    let closed = journal
        .close(guard.lease.take().expect("live probe lease"))
        .await;
    let value = result?;
    closed?;
    Ok(value)
}

// Reader cancellation must not leave a process pin behind. Engine close is
// I/O-free; an async cleanup on the same executor avoids polling it in Drop.
struct ExtensionLease<S: JournalStorage> {
    journal: JournalEngine<S>,
    lease: Option<agent_runtime::core::journal::JournalLease>,
}
impl<S: JournalStorage> Drop for ExtensionLease<S> {
    fn drop(&mut self) {
        use agent_runtime::core::journal::SessionJournal;
        if let Some(lease) = self.lease.take() {
            let journal = self.journal.clone();
            tokio::spawn(async move {
                let _ = journal.close(lease).await;
            });
        }
    }
}
