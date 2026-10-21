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

/// Partial progress is durable: a later retry skips an already retired domain.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RetirementProgress {
    pub retired_domains: usize,
    pub conflicts: usize,
    pub unreadable_session: bool,
    pub deferred_domains: usize,
}

impl<S: JournalStorage> SessionJournals<S> {
    /// Only published native roots participate. Never opens a writer or invokes
    /// the legacy importer. All controls share the runtime's gates and leases.
    pub async fn retire_published(
        &self,
        runtime_session_id: &str,
    ) -> Result<RetirementProgress, RuntimeError> {
        use agent_runtime::core::{
            clock::{Clock, SystemClock},
            error::ErrorKind,
            ids::SessionId,
            journal::{JournalDomain, JournalRole, SessionJournal},
        };
        let session = SessionId::new(runtime_session_id);
        let mut progress = RetirementProgress::default();
        // Authenticate BOTH domains and obtain both revision-bound grants
        // before retiring either. A bad ordinary record must not leave a
        // healthy protected domain retired merely because it was visited first.
        let mut prepared = Vec::new();
        for (engine, role) in [
            (&self.protected, JournalRole::Protected),
            (&self.ordinary, JournalRole::Ordinary),
        ] {
            let loaded = self
                .storage
                .load(
                    &JournalDomain {
                        storage_id: engine.storage_id(),
                        role,
                    },
                    &session,
                )
                .await;
            let record = match loaded {
                Ok(Some(record)) => record,
                Ok(None) => continue,
                Err(error) if error.kind == ErrorKind::Serialization && !error.retryable => {
                    progress.unreadable_session = true;
                    return Ok(progress);
                }
                Err(error) => return Err(error),
            };
            if record.head.is_some() && record.retirement.is_none() {
                prepared.push((engine, record.revision));
            }
        }
        for (engine, revision) in &prepared {
            match engine
                .authorize_retirement(&session, *revision, SystemClock.now())
                .await
            {
                Ok(()) => {}
                Err(error) if error.kind == ErrorKind::Conflict => {
                    progress.conflicts += 1;
                    return Ok(progress);
                }
                Err(error) if !error.retryable && !journal_lock_poisoned(&error) => {
                    progress.deferred_domains += 1;
                    return Ok(progress);
                }
                Err(error) => return Err(error),
            }
        }
        // Domain order is durable; restart skips an already retired domain.
        for (engine, revision) in prepared {
            match engine.retire(&session, revision).await {
                Ok(_) => progress.retired_domains += 1,
                Err(error) if error.kind == ErrorKind::Conflict => {
                    progress.conflicts += 1;
                    break;
                }
                // Retirement scans authoritative dependency candidates. A bad
                // OTHER session may protect this one: defer, never misidentify
                // this candidate as unreadable, and preserve partial progress.
                Err(error) if !error.retryable && !journal_lock_poisoned(&error) => {
                    progress.deferred_domains += 1;
                    break;
                }
                Err(error) => return Err(error),
            }
        }
        tracing::debug!(target: "forge_session_journal", retired_domains=progress.retired_domains, conflicts=progress.conflicts, deferred_domains=progress.deferred_domains, "retirement attempt");
        Ok(progress)
    }
}

impl SessionJournals {
    /// Physical S2 follows an acknowledged terminal publication. It must never
    /// replace that turn's outcome. A durable dirty marker also survives a
    /// lost nudge; the worker uses these same engine owners for collection.
    pub async fn post_terminal(&self, runtime_session_id: &str) {
        use agent_runtime::core::{
            ids::SessionId,
            journal::{JournalDomain, JournalRole, SessionJournal},
        };
        let session = SessionId::new(runtime_session_id);
        for (engine, role) in [
            (&self.protected, JournalRole::Protected),
            (&self.ordinary, JournalRole::Ordinary),
        ] {
            if let Err(error) = self
                .storage
                .post_terminal(
                    &JournalDomain {
                        storage_id: engine.storage_id(),
                        role,
                    },
                    &session,
                )
                .await
            {
                if error.kind == agent_runtime::core::error::ErrorKind::Serialization {
                    ForgeJournalStorage::note_unreadable_session(&session);
                } else {
                    tracing::warn!(target: "forge_session_journal", "terminal journal maintenance deferred");
                }
            }
        }
        self.storage.nudge_retention();
    }
}

fn journal_lock_poisoned(error: &RuntimeError) -> bool {
    error.metadata.get("journal_lock_poisoned")
        == Some(&agent_runtime::core::metadata::MetaValue::Bool(true))
}
