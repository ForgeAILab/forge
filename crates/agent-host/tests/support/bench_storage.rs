use agent_runtime::core::{error::RuntimeError, ids::SessionId, journal::*};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

/// Meter trait calls without looking into Forge storage or runtime metadata.
#[derive(Debug)]
pub struct Metered<S> {
    pub inner: Arc<S>,
    pub object_reads: std::sync::atomic::AtomicU64,
    pub operations: Mutex<Vec<(JournalDomain, SessionId, JournalOperationRecord)>>,
    pub receipt: Mutex<JournalApplyReceipt>,
    pub admitted_objects: std::sync::atomic::AtomicU64,
    pub prepared_commits: std::sync::atomic::AtomicU64,
    pub unprepared_commits: std::sync::atomic::AtomicU64,
    pub admitted_bytes: std::sync::atomic::AtomicU64,
}
impl<S> Metered<S> {
    pub fn new(inner: Arc<S>) -> Self {
        Self {
            inner,
            object_reads: std::sync::atomic::AtomicU64::new(0),
            operations: Mutex::new(vec![]),
            receipt: Mutex::new(JournalApplyReceipt::default()),
            admitted_objects: std::sync::atomic::AtomicU64::new(0),
            prepared_commits: std::sync::atomic::AtomicU64::new(0),
            unprepared_commits: std::sync::atomic::AtomicU64::new(0),
            admitted_bytes: std::sync::atomic::AtomicU64::new(0),
        }
    }
    pub fn take(&self) -> u64 {
        self.object_reads
            .swap(0, std::sync::atomic::Ordering::Relaxed)
    }
    pub fn reads(&self) -> u64 {
        self.object_reads.load(std::sync::atomic::Ordering::Relaxed)
    }
}
#[async_trait]
impl<S: JournalStorage> JournalStorage for Metered<S> {
    fn collection_capability(&self) -> JournalCollectionCapability {
        self.inner.collection_capability()
    }
    async fn collection_page(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        request: JournalCollectionPageRequest,
    ) -> Result<JournalCollectionPage, RuntimeError> {
        self.inner.collection_page(d, s, request).await
    }
    async fn prepare_collection_apply(
        &self,
        plan: &JournalCollectionPlan,
    ) -> Result<JournalCollectionPreflight, RuntimeError> {
        self.inner.prepare_collection_apply(plan).await
    }
    async fn apply_collection(
        &self,
        plan: JournalCollectionPlan,
    ) -> Result<JournalCollectionReceipt, RuntimeError> {
        let result = self.inner.apply_collection(plan).await?;
        let mut total = self.receipt.lock().unwrap();
        total.reclaimed_objects += result.receipt.reclaimed_objects;
        total.reclaimed_bytes += result.receipt.reclaimed_bytes;
        Ok(result)
    }
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
        self.object_reads
            .fetch_add(ids.len() as u64, std::sync::atomic::Ordering::Relaxed);
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
        let op = if crate::profile::clean() {
            None
        } else {
            w.operation
                .clone()
                .map(|op| (w.domain.clone(), w.session.clone(), op))
        };
        let count = w.objects.objects.len() as u64;
        let bytes = w
            .objects
            .objects
            .iter()
            .map(|o| o.bytes.len() as u64)
            .sum::<u64>();
        let receipt = self.inner.apply(w).await?;
        if let Some(op) = op.filter(|_| !crate::profile::clean()) {
            self.operations.lock().unwrap().push(op);
        }
        self.admitted_objects
            .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
        self.admitted_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        {
            let mut total = self.receipt.lock().unwrap();
            total.reclaimed_objects += receipt.reclaimed_objects;
            total.reclaimed_bytes += receipt.reclaimed_bytes;
        }
        Ok(receipt)
    }
    async fn retained_bytes(&self, d: &JournalDomain) -> Result<Option<u64>, RuntimeError> {
        self.inner.retained_bytes(d).await
    }
    async fn compact(&self, d: &JournalDomain) -> Result<(), RuntimeError> {
        self.inner.compact(d).await
    }
}
