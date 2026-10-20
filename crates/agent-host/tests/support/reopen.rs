//! Reopen the real file/pool and discard storage caches on every load, as in
//! v0.8.0's packed reference. All subsequent hydration reads use that storage.
use std::sync::{Arc, Mutex};

use agent_runtime::core::{error::RuntimeError, ids::SessionId, journal::*};
use async_trait::async_trait;
use forge_agent_host::{
    session_journal::ForgeJournalStorage,
    test_support::{FileDb, MappingStorage},
};

use super::support::{self, Storage};

#[derive(Debug)]
pub struct Reopen {
    fixture: FileDb,
    current: Mutex<Arc<Storage>>,
}

impl Reopen {
    pub async fn new() -> Self {
        let storage = support::storage().await;
        Self {
            fixture: storage.fixture.clone(),
            current: Mutex::new(storage),
        }
    }

    fn storage(&self) -> Arc<Storage> {
        self.current.lock().unwrap().clone()
    }
}

#[async_trait]
impl JournalStorage for Reopen {
    fn sharing(&self) -> JournalBodySharing {
        self.storage().sharing()
    }

    async fn retained_bytes(&self, d: &JournalDomain) -> Result<Option<u64>, RuntimeError> {
        self.storage().retained_bytes(d).await
    }

    async fn load(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Option<JournalSessionRecord>, RuntimeError> {
        let db = self.fixture.reopen().await.expect("independent file pool");
        let raw = ForgeJournalStorage::new(db, [7; 32], 1)?;
        assert_eq!(raw.cache_stats()?.index_decodes, 0);
        assert_eq!(raw.cache_stats()?.body_decodes, 0);
        let storage = Arc::new(MappingStorage::new(Arc::new(raw), self.fixture.clone()));
        *self.current.lock().unwrap() = storage.clone();
        storage.load(d, s).await
    }

    async fn objects(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        ids: &[JournalObjectId],
    ) -> Result<Vec<Option<Arc<[u8]>>>, RuntimeError> {
        self.storage().objects(d, s, ids).await
    }

    async fn members(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Vec<JournalObjectId>, RuntimeError> {
        self.storage().members(d, s).await
    }

    async fn operation(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        o: &JournalOperationId,
    ) -> Result<Option<JournalOperationRecord>, RuntimeError> {
        self.storage().operation(d, s, o).await
    }

    async fn pins(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Vec<JournalPinRecord>, RuntimeError> {
        self.storage().pins(d, s).await
    }

    async fn catalog(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        h: &JournalObjectId,
    ) -> Result<Option<JournalCatalogEntry>, RuntimeError> {
        self.storage().catalog(d, s, h).await
    }

    async fn dirty_sessions(
        &self,
        d: &JournalDomain,
        a: Option<&SessionId>,
        n: usize,
    ) -> Result<Vec<SessionId>, RuntimeError> {
        self.storage().dirty_sessions(d, a, n).await
    }

    async fn apply(&self, w: JournalWrite) -> Result<JournalApplyReceipt, RuntimeError> {
        self.storage().apply(w).await
    }

    async fn compact(&self, d: &JournalDomain) -> Result<(), RuntimeError> {
        self.storage().compact(d).await
    }
}
