use std::sync::Arc;

use agent_runtime::core::{error::RuntimeError, ids::SessionId, journal::*};
use async_trait::async_trait;

use super::FileDb;

/// Test-only mapping decorator. Reads never create a mapping; the first apply
/// for any runtime id creates one before delegating to the storage under test.
#[derive(Debug)]
pub struct MappingStorage<S> {
    pub inner: Arc<S>,
    pub fixture: FileDb,
}

impl<S> MappingStorage<S> {
    pub fn new(inner: Arc<S>, fixture: FileDb) -> Self {
        Self { inner, fixture }
    }
}

#[async_trait]
impl<S: JournalStorage> JournalStorage for MappingStorage<S> {
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
        self.inner.apply_collection(plan).await
    }
    fn sharing(&self) -> JournalBodySharing {
        self.inner.sharing()
    }

    async fn retained_bytes(&self, domain: &JournalDomain) -> Result<Option<u64>, RuntimeError> {
        self.inner.retained_bytes(domain).await
    }

    async fn load(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
    ) -> Result<Option<JournalSessionRecord>, RuntimeError> {
        self.inner.load(domain, session).await
    }

    async fn objects(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
        ids: &[JournalObjectId],
    ) -> Result<Vec<Option<Arc<[u8]>>>, RuntimeError> {
        self.inner.objects(domain, session, ids).await
    }

    async fn members(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
    ) -> Result<Vec<JournalObjectId>, RuntimeError> {
        self.inner.members(domain, session).await
    }

    async fn operation(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
        operation: &JournalOperationId,
    ) -> Result<Option<JournalOperationRecord>, RuntimeError> {
        self.inner.operation(domain, session, operation).await
    }

    async fn pins(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
    ) -> Result<Vec<JournalPinRecord>, RuntimeError> {
        self.inner.pins(domain, session).await
    }

    async fn catalog(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
        head: &JournalObjectId,
    ) -> Result<Option<JournalCatalogEntry>, RuntimeError> {
        self.inner.catalog(domain, session, head).await
    }

    async fn dirty_sessions(
        &self,
        domain: &JournalDomain,
        after: Option<&SessionId>,
        limit: usize,
    ) -> Result<Vec<SessionId>, RuntimeError> {
        self.inner.dirty_sessions(domain, after, limit).await
    }

    async fn apply(&self, write: JournalWrite) -> Result<JournalApplyReceipt, RuntimeError> {
        self.fixture.ensure_session(&write.session).await?;
        self.inner.apply(write).await
    }

    async fn compact(&self, domain: &JournalDomain) -> Result<(), RuntimeError> {
        self.inner.compact(domain).await
    }
}
