//! Membership release and mandatory physical-copy receipt accounting.
use super::{
    cache::Scope,
    codec, corrupt,
    seal::{Unit, role},
    storage::{ForgeJournalStorage, PackView, database_error},
};
use agent_runtime::core::{error::RuntimeError, journal::*};
use sqlx::SqliteConnection;
use std::collections::BTreeSet;

impl ForgeJournalStorage {
    pub(super) async fn delete_pack(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        id: i64,
    ) -> Result<(), RuntimeError> {
        { #[cfg(feature = "test-support")] let _sql_profile = crate::session_journal::profile::timer("sql.DELETE", 0); sqlx::query("DELETE FROM agent_session_journal_pack WHERE pack_id=? AND storage_id=? AND role=? AND runtime_session_id=?")
            .bind(id).bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).execute(&mut *conn).await }.map_err(database_error)?;
        Ok(())
    }
    /// A purge keeps its monotonic identity and advances the pack generation.
    /// All output slots are live; their sorted-id order defines the new bitmap.
    async fn rewrite_live_pack(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        view: &PackView,
        objects: Vec<JournalStoredObject>,
    ) -> Result<(), RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("release.rewrite_live_pack", 0);
        let encoded = self.work(move || codec::encode(&objects)).await?;
        let key = &view.index.key;
        let generation = key.generation.checked_add(1).ok_or_else(corrupt)?;
        let index = self
            .seal_row(
                scope,
                &key.owner,
                Unit::PackIndex,
                &key.id.to_be_bytes(),
                generation,
                encoded.index,
            )
            .await?;
        let body = self
            .seal_row(
                scope,
                &key.owner,
                Unit::PackBody,
                &key.id.to_be_bytes(),
                generation,
                encoded.body,
            )
            .await?;
        { #[cfg(feature = "test-support")] let _sql_profile = crate::session_journal::profile::timer("sql.UPDATE", 0); sqlx::query("UPDATE agent_session_journal_pack SET generation=?,object_count=?,retained_canonical_bytes=?,index_nonce=?,index_payload=?,body_nonce=?,body_payload=? WHERE pack_id=? AND storage_id=? AND role=? AND runtime_session_id=?")
            .bind(generation.to_be_bytes().as_slice()).bind(encoded.count as i64).bind(encoded.canonical_bytes as i64).bind(index.nonce).bind(index.payload).bind(body.nonce).bind(body.payload).bind(key.id).bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).execute(&mut *conn).await }.map_err(database_error)?;
        let mut bits = vec![255; encoded.count.div_ceil(8)];
        if encoded.count % 8 != 0 {
            *bits.last_mut().ok_or_else(corrupt)? = (1 << (encoded.count % 8)) - 1;
        }
        self.put_bits(conn, scope, &key.owner, key.id, &bits).await
    }
    pub(super) async fn release_objects(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        owner: &str,
        views: &mut [PackView],
        release: &[JournalObjectId],
        retired_collection: bool,
    ) -> Result<(JournalApplyReceipt, BTreeSet<i64>), RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("release.release_objects", 0);
        let ids: BTreeSet<_> = release.iter().collect();
        let wanted: BTreeSet<_> = views
            .iter()
            .filter(|v| {
                ids.iter()
                    .any(|id| v.index.directory.candidates(id).any(|s| v.held(s)))
            })
            .map(|v| v.index.key.id)
            .collect();
        let bodies = self.bodies(conn, views, &wanted, false).await?;
        let mut touched = BTreeSet::new();
        for view in &mut *views {
            if let Some(objects) = bodies.get(&view.index.key.id) {
                for (slot, object) in objects.iter().enumerate() {
                    if view.held(slot) && ids.contains(&object.id) {
                        view.bitmap[slot / 8] &= !(1 << (slot % 8));
                        touched.insert(view.index.key.id);
                    }
                }
                if touched.contains(&view.index.key.id) {
                    self.put_bits(conn, scope, owner, view.index.key.id, &view.bitmap)
                        .await?;
                }
            }
        }
        let empty = views.iter().all(|v| v.bitmap.iter().all(|b| *b == 0));
        let mut purge = BTreeSet::new();
        for view in &*views {
            if !retired_collection && !empty && !touched.contains(&view.index.key.id) {
                continue;
            }
            let dead: usize = view
                .index
                .directory
                .slots
                .iter()
                .enumerate()
                .filter(|(s, _)| !view.held(*s))
                .map(|(_, s)| s.len)
                .sum();
            if dead != 0
                && (retired_collection
                    || empty
                    || (touched.contains(&view.index.key.id)
                        && dead * 4 >= view.index.key.retained))
            {
                purge.insert(view.index.key.id);
            }
        }
        // Decode/authenticate even all-dead packs before physical removal. Only
        // the separate post-deletion orphan cleanup may bypass body decryption.
        let missing = purge.difference(&wanted).copied().collect();
        let mut bodies = bodies;
        bodies.extend(self.bodies(conn, views, &missing, false).await?);
        let mut receipt = JournalApplyReceipt::default();
        let mut survivors = BTreeSet::new();
        for view in views.iter().filter(|v| purge.contains(&v.index.key.id)) {
            let objects = bodies.get(&view.index.key.id).ok_or_else(corrupt)?;
            let mut live = Vec::new();
            for (slot, object) in objects.iter().enumerate() {
                if view.held(slot) {
                    live.push(object.clone());
                } else {
                    receipt.reclaimed_objects = receipt
                        .reclaimed_objects
                        .checked_add(1)
                        .ok_or_else(corrupt)?;
                    receipt.reclaimed_bytes = receipt
                        .reclaimed_bytes
                        .checked_add(object.bytes.len() as u64)
                        .ok_or_else(corrupt)?;
                }
            }
            if live.is_empty() {
                self.delete_pack(conn, scope, view.index.key.id).await?;
            } else {
                self.rewrite_live_pack(conn, scope, view, live).await?;
                survivors.insert(view.index.key.id);
            }
        }
        Ok((receipt, survivors))
    }
}

#[cfg(feature = "test-support")]
impl ForgeJournalStorage {
    /// Construct a real, authenticated below-threshold deferred slot without
    /// changing any engine object, head or existing membership. Lifecycle tests
    /// use this to prove retired-domain collection purges already-dead bytes.
    pub async fn add_deferred_slot_for_test(
        &self,
        domain: &JournalDomain,
        session: &agent_runtime::core::ids::SessionId,
        object: JournalStoredObject,
    ) -> Result<u64, RuntimeError> {
        use super::storage::ambiguous_database_error;
        let scope = (domain.clone(), session.clone());
        codec::validate(&object)?;
        let bytes = object.bytes.len();
        let mut tx = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.BEGIN_IMMEDIATE", 0);
            db::begin_immediate(self.db.pool()).await
        }
        .map_err(database_error)?;
        let views = self.directory(&mut tx, &scope, false).await?;
        let view = views
            .iter()
            .find(|v| {
                (0..v.index.key.count).all(|slot| v.held(slot))
                    && v.index.key.retained > bytes * 4
                    && v.index.key.retained + bytes <= codec::PACK_CAP
            })
            .ok_or_else(corrupt)?;
        let bodies = self
            .bodies(&mut tx, &views, &BTreeSet::from([view.index.key.id]), false)
            .await?;
        let mut objects = bodies
            .get(&view.index.key.id)
            .ok_or_else(corrupt)?
            .as_ref()
            .clone();
        if objects.iter().any(|o| o.id == object.id) {
            return Err(corrupt());
        }
        let dead = object.id.clone();
        objects.push(object);
        objects.sort_by(|a, b| a.id.cmp(&b.id));
        let slot = objects
            .iter()
            .position(|o| o.id == dead)
            .ok_or_else(corrupt)?;
        let count = objects.len();
        self.rewrite_live_pack(&mut tx, &scope, view, objects)
            .await?;
        let mut bitmap = vec![255; count.div_ceil(8)];
        if count % 8 != 0 {
            *bitmap.last_mut().ok_or_else(corrupt)? = (1 << (count % 8)) - 1;
        }
        bitmap[slot / 8] &= !(1 << (slot % 8));
        self.put_bits(
            &mut tx,
            &scope,
            &view.index.key.owner,
            view.index.key.id,
            &bitmap,
        )
        .await?;
        {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.COMMIT", 0);
            tx.commit().await
        }
        .map_err(ambiguous_database_error)?;
        self.cache_lock()
            .map_err(RuntimeError::retryable)?
            .invalidate(&scope);
        Ok(bytes as u64)
    }
}
