//! Test-target-only adaptations of v0.9.0's private journal_storage_broken
//! fault shapes. Every non-faulted operation reaches real Forge SQL/AEAD storage.
use std::{
    collections::{BTreeMap, BTreeSet},
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
};

use agent_runtime::core::{error::RuntimeError, ids::SessionId, journal::*};
use agent_runtime_testkit::conformance::journal_storage::assert_journal_storage_conformance;
use async_trait::async_trait;
use futures_util::FutureExt;

use super::support::{self, Storage};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Defect {
    TornTransaction,
    LostWrite,
    NonRetryableLostAck,
    ReorderedCommit,
    StaleEpoch,
    StaleGeneration,
    SharedBodyDeleted,
    DeadBodyServed,
}

#[derive(Debug)]
struct Broken {
    inner: Mutex<Arc<Storage>>,
    // Prepare the reorder destination outside async_trait's Send apply future:
    // the migration fixture constructor itself does not promise a Send future.
    replacement: Mutex<Option<Arc<Storage>>>,
    // Models a bad global unlink on release: even an independently held copy
    // becomes unreadable, while its real membership remains in SQL. Merely
    // reclaiming the releasing scope's own copy is valid and is not a fault.
    unlinked: Mutex<BTreeSet<JournalObjectId>>,
    dead: Mutex<BTreeMap<JournalObjectId, Arc<[u8]>>>,
    defect: Defect,
}

impl Broken {
    async fn new(defect: Defect) -> Self {
        Self {
            inner: Mutex::new(support::storage().await),
            replacement: Mutex::new(if defect == Defect::ReorderedCommit {
                Some(support::storage().await)
            } else {
                None
            }),
            unlinked: Mutex::new(BTreeSet::new()),
            dead: Mutex::new(BTreeMap::new()),
            defect,
        }
    }

    fn storage(&self) -> Arc<Storage> {
        self.inner.lock().unwrap().clone()
    }
}

#[async_trait]
impl JournalStorage for Broken {
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
        self.storage().load(d, s).await
    }

    async fn objects(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        ids: &[JournalObjectId],
    ) -> Result<Vec<Option<Arc<[u8]>>>, RuntimeError> {
        let mut bodies = self.storage().objects(d, s, ids).await?;
        let unlinked = self.unlinked.lock().unwrap();
        for (id, body) in ids.iter().zip(&mut bodies) {
            if unlinked.contains(id) {
                *body = None;
            }
            if self.defect == Defect::DeadBodyServed && body.is_none() {
                *body = self.dead.lock().unwrap().get(id).cloned();
            }
        }
        Ok(bodies)
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

    async fn compact(&self, d: &JournalDomain) -> Result<(), RuntimeError> {
        self.storage().compact(d).await
    }

    async fn apply(&self, mut write: JournalWrite) -> Result<JournalApplyReceipt, RuntimeError> {
        match self.defect {
            Defect::LostWrite if write.kind == JournalWriteKind::Commit => {
                return Ok(JournalApplyReceipt::default());
            }
            Defect::TornTransaction
                if write.inject.is_some_and(|point| !point.is_after_commit()) =>
            {
                // Commit new bodies while retaining the previous publication
                // and omitting its operation/pin/catalog, then report failure.
                write.inject = None;
                write.record = self
                    .storage()
                    .load(&write.domain, &write.session)
                    .await?
                    .unwrap();
                write.record.generation += 1;
                write.operation = None;
                write.pins.clear();
                write.catalog.clear();
                self.storage().apply(write).await?;
                return Err(RuntimeError::internal("injected torn storage transaction"));
            }
            Defect::ReorderedCommit
                if write.kind == JournalWriteKind::Commit && write.record.revision.0 > 1 =>
            {
                // Publish n+1 into another real file that never published n.
                // Preserve reused immutable leaves so current hydration works;
                // the missing predecessor publication is the intended failure.
                let current = self.storage();
                let ids = current.members(&write.domain, &write.session).await?;
                let bodies = current.objects(&write.domain, &write.session, &ids).await?;
                let mut objects: BTreeMap<_, _> = write
                    .objects
                    .objects
                    .into_iter()
                    .map(|object| (object.id.clone(), object))
                    .collect();
                for (id, bytes) in ids.into_iter().zip(bodies) {
                    let bytes = bytes.unwrap();
                    let kind = JournalObject {
                        id: id.clone(),
                        bytes: bytes.to_vec(),
                    }
                    .kind()?;
                    if !matches!(
                        kind,
                        JournalObjectKind::Head
                            | JournalObjectKind::Batch
                            | JournalObjectKind::Checkpoint
                    ) {
                        objects.entry(id.clone()).or_insert(JournalStoredObject {
                            id,
                            kind,
                            bytes,
                        });
                    }
                }
                write.objects.objects = objects.into_values().collect();
                let replacement = self.replacement.lock().unwrap().take().unwrap();
                let mut reservation = write.clone();
                reservation.kind = JournalWriteKind::OpenWriter;
                reservation.expect = JournalFence::default();
                let record = &mut reservation.record;
                record.generation = 1;
                record.revision = JournalRevision(0);
                record.head = None;
                record.has_checkpoint = false;
                reservation.objects.objects.clear();
                reservation.operation = None;
                reservation.pins.clear();
                reservation.catalog.clear();
                reservation.release.clear();
                reservation.inject = None;
                replacement.apply(reservation).await?;
                write.expect.revision = JournalRevision(0);
                write.expect.generation = 1;
                write.record.generation = 2;
                let receipt = replacement.apply(write).await?;
                *self.inner.lock().unwrap() = replacement;
                return Ok(receipt);
            }
            Defect::StaleGeneration => {
                if let Some(record) = self.storage().load(&write.domain, &write.session).await? {
                    write.expect.generation = record.generation;
                    write.record.generation = record.generation + 1;
                }
            }
            Defect::StaleEpoch => {
                // Keep revision and generation fenced; ignore only epoch.
                if let Some(record) = self.storage().load(&write.domain, &write.session).await? {
                    write.expect.writer_epoch = record.writer_epoch;
                }
            }
            _ => {}
        }
        let release = write.release.clone();
        if self.defect == Defect::DeadBodyServed {
            let bodies = self
                .storage()
                .objects(&write.domain, &write.session, &release)
                .await?;
            self.dead.lock().unwrap().extend(
                release
                    .iter()
                    .cloned()
                    .zip(bodies)
                    .filter_map(|(id, body)| body.map(|body| (id, body))),
            );
        }
        let lost_ack = self.defect == Defect::NonRetryableLostAck
            && write.inject == Some(JournalCommitFailurePoint::AfterCommit);
        let result = self.storage().apply(write).await;
        if lost_ack {
            let mut error = result.unwrap_err();
            error.retryable = false;
            return Err(error);
        }
        let receipt = result?;
        if self.defect == Defect::SharedBodyDeleted {
            self.unlinked.lock().unwrap().extend(release);
        }
        Ok(receipt)
    }
}

pub async fn rejects(defect: Defect, case: &str) -> String {
    let result = AssertUnwindSafe(Box::pin(assert_journal_storage_conformance(|| async {
        Arc::new(Broken::new(defect).await)
    })))
    .catch_unwind()
    .await;
    let payload = result.expect_err("broken Forge storage passed the full conformance suite");
    let message = payload
        .downcast_ref::<String>()
        .expect("named conformance failure");
    assert!(
        message.starts_with(&format!("journal_storage::{case}:")),
        "wrong failing case for {defect:?}: {message}"
    );
    eprintln!("BROKEN FORGE STORAGE {defect:?} -> journal_storage::{case}");
    message.clone()
}
