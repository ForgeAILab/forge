use agent_runtime::core::{error::RuntimeError, ids::SessionId, journal::*};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

/// Meter trait calls without looking into Forge storage or runtime metadata.
#[derive(Debug)]
pub struct Metered<S> {
    pub inner: Arc<S>,
    pub objects: Mutex<Vec<JournalObjectId>>,
    pub apply_entered: tokio::sync::Notify,
}
impl<S> Metered<S> {
    pub fn new(inner: Arc<S>) -> Self {
        Self {
            inner,
            objects: Mutex::new(vec![]),
            apply_entered: tokio::sync::Notify::new(),
        }
    }
    pub fn take(&self) -> Vec<JournalObjectId> {
        std::mem::take(&mut *self.objects.lock().unwrap())
    }
}
#[async_trait]
impl<S: JournalStorage> JournalStorage for Metered<S> {
    fn sharing(&self) -> JournalBodySharing {
        self.inner.sharing()
    }
    async fn load(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Option<JournalSessionRecord>, RuntimeError> {
        self.inner.load(d, s).await
    }
    async fn objects(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        ids: &[JournalObjectId],
    ) -> Result<Vec<Option<Arc<[u8]>>>, RuntimeError> {
        self.objects.lock().unwrap().extend_from_slice(ids);
        self.inner.objects(d, s, ids).await
    }
    async fn members(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Vec<JournalObjectId>, RuntimeError> {
        self.inner.members(d, s).await
    }
    async fn operation(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        op: &JournalOperationId,
    ) -> Result<Option<JournalOperationRecord>, RuntimeError> {
        self.inner.operation(d, s, op).await
    }
    async fn pins(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Vec<JournalPinRecord>, RuntimeError> {
        self.inner.pins(d, s).await
    }
    async fn catalog(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        h: &JournalObjectId,
    ) -> Result<Option<JournalCatalogEntry>, RuntimeError> {
        self.inner.catalog(d, s, h).await
    }
    async fn dirty_sessions(
        &self,
        d: &JournalDomain,
        after: Option<&SessionId>,
        limit: usize,
    ) -> Result<Vec<SessionId>, RuntimeError> {
        self.inner.dirty_sessions(d, after, limit).await
    }
    async fn apply(&self, w: JournalWrite) -> Result<JournalApplyReceipt, RuntimeError> {
        self.apply_entered.notify_one();
        self.inner.apply(w).await
    }
    async fn retained_bytes(&self, d: &JournalDomain) -> Result<Option<u64>, RuntimeError> {
        self.inner.retained_bytes(d).await
    }
    async fn compact(&self, d: &JournalDomain) -> Result<(), RuntimeError> {
        self.inner.compact(d).await
    }
}

/// Deliberately uses the ordinary per-object transport so memo hits are metered
/// too. The engine still authorizes every read of this live lease.
#[derive(Debug)]
pub struct ReadMeter<'a> {
    pub inner: &'a dyn SessionJournal,
    pub kinds: Mutex<Vec<JournalObjectKind>>,
}
#[async_trait]
impl SessionJournal for ReadMeter<'_> {
    fn storage_id(&self) -> JournalStorageId {
        self.inner.storage_id()
    }
    async fn open(&self, s: &SessionId, r: JournalOpen) -> Result<JournalLease, RuntimeError> {
        self.inner.open(s, r).await
    }
    async fn read(&self, l: &JournalLease, id: &JournalObjectId) -> Result<Vec<u8>, RuntimeError> {
        let bytes = self.inner.read(l, id).await?;
        let kind = JournalObject {
            id: id.clone(),
            bytes: bytes.clone(),
        }
        .kind()?;
        self.kinds.lock().unwrap().push(kind);
        Ok(bytes)
    }
    async fn commit(
        &self,
        l: &JournalLease,
        c: JournalCommit,
    ) -> Result<JournalHead, RuntimeError> {
        self.inner.commit(l, c).await
    }
    async fn renew(&self, l: &JournalLease) -> Result<JournalLease, RuntimeError> {
        self.inner.renew(l).await
    }
    async fn close(&self, l: JournalLease) -> Result<(), RuntimeError> {
        self.inner.close(l).await
    }
    async fn retire(
        &self,
        s: &SessionId,
        r: JournalRevision,
    ) -> Result<JournalRetirement, RuntimeError> {
        self.inner.retire(s, r).await
    }
    async fn collect(&self, r: JournalGcRequest) -> Result<JournalGcReport, RuntimeError> {
        self.inner.collect(r).await
    }
}
