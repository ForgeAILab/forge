//! Fenced transactions and the runtime's ordered publication cuts.
use super::{
    cache::{Scope, ScopeDirectory},
    codec, conflict,
    seal::role,
    storage::{ForgeJournalStorage, PackView, ambiguous_database_error, database_error},
    wire,
};
use agent_runtime::core::{
    error::{FailureClass, FailureComponent, FailureStage, RuntimeError},
    journal::*,
};
use sqlx::SqliteConnection;
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

fn fail(write: &JournalWrite, cut: JournalCommitFailurePoint) -> Result<(), RuntimeError> {
    if write.inject == Some(cut) {
        Err(RuntimeError::internal("injected journal commit failure")
            .with_class(FailureClass::HostComponent {
                stage: FailureStage::PreProvider,
                component: FailureComponent::Checkpoint,
            })
            .retryable())
    } else {
        Ok(())
    }
}
fn directory(views: &[PackView]) -> Arc<ScopeDirectory> {
    Arc::new(ScopeDirectory {
        packs: views
            .iter()
            .map(|v| (v.index.key.id, v.index.clone()))
            .collect(),
    })
}

// SQLx also rolls back a dropped transaction. Drop invalidation covers a
// cancelled apply whose COMMIT acknowledgement may never reach this future.
struct InvalidateOnDrop {
    cache: Arc<Mutex<super::cache::Cache>>,
    scope: Scope,
    armed: bool,
}
impl Drop for InvalidateOnDrop {
    fn drop(&mut self) {
        if self.armed {
            if let Ok(mut cache) = self.cache.lock() {
                cache.invalidate(&self.scope);
            }
        }
    }
}

impl ForgeJournalStorage {
    pub(super) async fn apply_write(
        &self,
        write: JournalWrite,
    ) -> Result<JournalApplyReceipt, RuntimeError> {
        let start = Instant::now();
        let scope = (write.domain.clone(), write.session.clone());
        let mut invalidation = InvalidateOnDrop {
            cache: self.cache.clone(),
            scope: scope.clone(),
            armed: true,
        };
        let mut hold_us = 0u64;
        let mut receipt = JournalApplyReceipt::default();
        let mut committed = false;
        let result = async {
            if write.expect.generation.checked_add(1) != Some(write.record.generation) {
                return Err(conflict());
            }
            // Validate candidates outside the write lock; no runtime-private
            // engine metadata or closure calculation belongs in this adapter.
            let objects = write.objects.objects.clone();
            let record = write.record.clone();
            self.work(move || {
                if objects.windows(2).any(|p| p[0].id >= p[1].id) {
                    return Err(conflict());
                }
                for object in &objects {
                    codec::validate(object)?;
                }
                if record.has_checkpoint && (record.head.is_none() || record.retirement.is_some()) {
                    return Err(conflict());
                }
                if let Some(head) = &record.head {
                    if head.kind()? != JournalObjectKind::Head {
                        return Err(conflict());
                    }
                }
                wire::record_bytes(&record)?;
                Ok(())
            })
            .await?;
            let mut tx = db::begin_immediate(self.db.pool())
                .await
                .map_err(database_error)?;
            let locked = Instant::now();
            let staged = self.apply_on(&mut tx, &scope, &write).await;
            match staged {
                Ok((next, applied_receipt)) => {
                    let commit = tx.commit().await;
                    hold_us = locked.elapsed().as_micros().min(u64::MAX as u128) as u64;
                    match commit {
                        Ok(()) => {
                            committed = true;
                            // Publish only after COMMIT. Existing immutable bodies
                            // remain useful; new directories replace the old scope.
                            receipt = applied_receipt;
                            self.cache_lock()
                                .map(|mut cache| cache.put_directory(scope.clone(), next))
                                .and_then(|_| fail(&write, JournalCommitFailurePoint::AfterCommit))
                                // Nothing after COMMIT may claim no durable effect.
                                .map_err(RuntimeError::retryable)
                                .map(|_| receipt)
                        }
                        Err(e) => Err(ambiguous_database_error(e)),
                    }
                }
                Err(e) => {
                    let rollback = tx.rollback().await;
                    hold_us = locked.elapsed().as_micros().min(u64::MAX as u128) as u64;
                    match rollback {
                        Ok(()) => Err(e),
                        Err(e) => Err(ambiguous_database_error(e)),
                    }
                }
            }
        }
        .await;
        #[cfg(feature = "test-support")]
        if result.is_err() && write.inject.is_some() && write.domain.role == JournalRole::Protected
        {
            let _ = self.failed_commit_observation.compare_exchange(
                1,
                2,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            );
        }
        invalidation.armed = result.is_err();
        drop(invalidation);
        tracing::debug!(target:"forge_session_journal",kind=?write.kind,
            duration_us=start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            lock_hold_us=hold_us,committed,success=result.is_ok(),
            reclaimed_objects=receipt.reclaimed_objects,reclaimed_bytes=receipt.reclaimed_bytes,
            "journal apply");
        result
    }

    async fn apply_on(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        write: &JournalWrite,
    ) -> Result<(Arc<ScopeDirectory>, JournalApplyReceipt), RuntimeError> {
        // Full fence comparison is SQL, including the all-zero absent-record
        // case. BEGIN IMMEDIATE prevents a writer racing this absence guard.
        let matches:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_session_journal_session WHERE storage_id=? AND role=? AND runtime_session_id=? AND revision=? AND writer_epoch=? AND generation=?) OR (?=zeroblob(8) AND ?=zeroblob(8) AND ?=zeroblob(8) AND NOT EXISTS(SELECT 1 FROM agent_session_journal_session WHERE storage_id=? AND role=? AND runtime_session_id=?))")
            .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str())
            .bind(write.expect.revision.0.to_be_bytes().as_slice()).bind(write.expect.writer_epoch.to_be_bytes().as_slice()).bind(write.expect.generation.to_be_bytes().as_slice())
            .bind(write.expect.revision.0.to_be_bytes().as_slice()).bind(write.expect.writer_epoch.to_be_bytes().as_slice()).bind(write.expect.generation.to_be_bytes().as_slice())
            .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str())
            .fetch_one(&mut *conn).await.map_err(database_error)?;
        if !matches {
            return Err(conflict());
        }
        let owner = self.mapping(conn, scope).await?;
        if self.load_on(conn, &scope.0, &scope.1).await?.is_none() {
            // FK anchor for staged membership/pins. Its zero fence is replaced
            // at the Head step; no reservation is visible before COMMIT.
            self.insert_record(
                conn,
                scope,
                &owner,
                &JournalSessionRecord {
                    generation: 0,
                    revision: JournalRevision(0),
                    writer_epoch: 0,
                    head: None,
                    has_checkpoint: false,
                    retirement: None,
                    collection_dirty: false,
                    engine: JournalEngineState {
                        version: 0,
                        bytes: vec![],
                    },
                },
            )
            .await?;
        }
        let mut views = self.directory(conn, scope, false).await?;
        if views.iter().any(|v| v.index.key.owner != owner) {
            return Err(RuntimeError::internal("journal pack owner mapping changed"));
        }
        // Admissions (including re-admitted deferred slots) follow the cut
        // order; genuinely new bodies collect here for one final S1 pack.
        let mut pending = Vec::new();
        for (step, cut) in [
            JournalCommitFailurePoint::AfterObjects,
            JournalCommitFailurePoint::AfterCheckpoint,
            JournalCommitFailurePoint::AfterBatch,
        ]
        .into_iter()
        .enumerate()
        {
            let group: Vec<_> = write
                .objects
                .objects
                .iter()
                .filter(|o| match o.kind {
                    JournalObjectKind::Checkpoint => step == 1,
                    JournalObjectKind::Batch => step == 2,
                    JournalObjectKind::Head => false,
                    _ => step == 0,
                })
                .cloned()
                .collect();
            pending.extend(self.admit(conn, scope, &owner, &mut views, &group).await?);
            fail(write, cut)?;
        }
        self.pin_changes(conn, scope, &owner, &write.pins).await?;
        self.catalog_changes(conn, scope, &owner, &write.catalog)
            .await?;
        fail(write, JournalCommitFailurePoint::AfterPin)?;
        let heads: Vec<_> = write
            .objects
            .objects
            .iter()
            .filter(|o| o.kind == JournalObjectKind::Head)
            .cloned()
            .collect();
        pending.extend(self.admit(conn, scope, &owner, &mut views, &heads).await?);
        // Every pre-commit cut rolls back the whole transaction, so packing
        // per cut would only rewrite unobservable intermediate rows. Encode,
        // seal and insert the new bodies once (capped segments when large).
        if !pending.is_empty() {
            pending.sort_by(|a, b| a.id.cmp(&b.id));
            for pack in self.work(move || codec::split(pending)).await? {
                self.insert_pack(conn, scope, &owner, pack).await?;
            }
        }
        self.replace_record(conn, scope, &owner, &write.record, write.expect)
            .await?;
        fail(write, JournalCommitFailurePoint::AfterHead)?;
        if let Some(operation) = &write.operation {
            self.append_operation(conn, scope, &owner, operation)
                .await?;
        }
        if let Some(floor) = write.prune_operations_before {
            self.prune_operations(conn, scope, &owner, floor, write.record.writer_epoch)
                .await?;
        }
        fail(write, JournalCommitFailurePoint::AfterIdempotency)?;
        // Reuse every previously read index; only the final new pack indexes
        // need decoding. There is no database-wide invalidation counter.
        let mut final_views = self
            .directory_from(conn, scope, false, Some(directory(&views)))
            .await?;
        let (receipt, purged) = self
            .release_objects(
                conn,
                scope,
                &owner,
                &mut final_views,
                &write.release,
                matches!(write.kind, JournalWriteKind::Collect)
                    && write.record.retirement.is_some(),
            )
            .await?;
        let collect = matches!(write.kind, JournalWriteKind::Collect);
        if collect {
            self.collection_repack(conn, scope, &purged).await?;
        }
        // Released bits live only in SQL. Without a purge (every removed pack
        // counts at least one reclaimed copy) or a repack, the indexes are
        // exactly the ones already read in this transaction.
        if !collect && receipt.reclaimed_objects == 0 {
            return Ok((directory(&final_views), receipt));
        }
        let final_views = self
            .directory_from(conn, scope, false, Some(directory(&final_views)))
            .await?;
        Ok((directory(&final_views), receipt))
    }
}
