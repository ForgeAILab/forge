//! Membership release and mandatory physical-copy receipt accounting.
use super::{
    cache::{PackKey, Scope},
    codec, corrupt,
    seal::Unit,
    storage::{ForgeJournalStorage, PackView, Staged, bit, database_error, prefix},
};
use agent_runtime::core::{error::RuntimeError, ids::SessionId, journal::*};
use sqlx::SqliteConnection;
use std::{collections::BTreeSet, sync::Arc};

/// Each holder domain's bits for the kept slots, from its own previous bits.
/// A rewrite never grants a domain a slot it did not hold.
pub(super) fn remap(view: &PackView, kept: &[usize]) -> Vec<(JournalDomain, Vec<u8>)> {
    std::iter::once((&view.domain, &view.bitmap))
        .chain(view.others.iter().map(|(d, bits)| (d, bits)))
        .map(|(domain, bits)| {
            let mut next = vec![0; kept.len().div_ceil(8)];
            for (slot, &old) in kept.iter().enumerate() {
                if bit(bits, old) {
                    next[slot / 8] |= 1 << (slot % 8);
                }
            }
            (domain.clone(), next)
        })
        .collect()
}

impl ForgeJournalStorage {
    pub(super) async fn delete_pack(
        &self,
        conn: &mut SqliteConnection,
        session: &SessionId,
        id: i64,
        staged: &mut Staged,
    ) -> Result<(), RuntimeError> {
        // Membership rows of every domain cascade with the pack row.
        {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.DELETE", 0);
            sqlx::query(
                "DELETE FROM agent_session_journal_pack WHERE pack_id=? AND runtime_session_id=?",
            )
            .bind(id)
            .bind(session.as_str())
            .execute(&mut *conn)
            .await
        }
        .map_err(database_error)?;
        staged.removed.insert(id);
        Ok(())
    }
    /// A purge keeps its monotonic identity and advances the pack generation.
    /// Output slots are the kept (live) slots in sorted-id order; every holder
    /// domain's bitmap is remapped from its own bits.
    async fn rewrite_live_pack(
        &self,
        conn: &mut SqliteConnection,
        session: &SessionId,
        view: &PackView,
        objects: Vec<JournalStoredObject>,
        holders: Vec<(JournalDomain, Vec<u8>)>,
        staged: &mut Staged,
    ) -> Result<(), RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("release.rewrite_live_pack", 0);
        let objects = Arc::new(objects);
        let encode = objects.clone();
        let encoded = self.work(move || codec::encode_validated(&encode)).await?;
        let key = &view.index.key;
        let generation = key.generation.checked_add(1).ok_or_else(corrupt)?;
        let index = self
            .seal_unit(
                None,
                session,
                &key.owner,
                Unit::PackIndex,
                &key.id.to_be_bytes(),
                generation,
                encoded.index,
            )
            .await?;
        let body = self
            .seal_unit(
                None,
                session,
                &key.owner,
                Unit::PackBody,
                &key.id.to_be_bytes(),
                generation,
                encoded.body,
            )
            .await?;
        { #[cfg(feature = "test-support")] let _sql_profile = crate::session_journal::profile::timer("sql.UPDATE", 0); sqlx::query("UPDATE agent_session_journal_pack SET generation=?,object_count=?,retained_canonical_bytes=?,index_nonce=?,index_payload=?,body_nonce=?,body_payload=? WHERE pack_id=? AND runtime_session_id=?")
            .bind(generation.to_be_bytes().as_slice()).bind(encoded.count as i64).bind(encoded.canonical_bytes as i64).bind(index.nonce).bind(index.payload).bind(body.nonce).bind(body.payload).bind(key.id).bind(session.as_str()).execute(&mut *conn).await }.map_err(database_error)?;
        for (domain, bits) in holders {
            super::storage::validate_bitmap(&bits, encoded.count)?;
            self.put_bits(conn, &(domain, session.clone()), &key.owner, key.id, &bits)
                .await?;
        }
        staged.removed.insert(key.id);
        staged.written.push((
            PackKey {
                session: session.clone(),
                id: key.id,
                generation,
                count: encoded.count,
                retained: encoded.canonical_bytes,
                owner: key.owner.clone(),
            },
            objects,
        ));
        Ok(())
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn release_objects(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        owner: &str,
        views: &mut [PackView],
        release: &[JournalObjectId],
        retired_collection: bool,
        staged: &mut Staged,
    ) -> Result<(JournalApplyReceipt, BTreeSet<i64>), RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("release.release_objects", 0);
        let ids: BTreeSet<_> = release.iter().collect();
        let prefixes: BTreeSet<_> = ids.iter().map(|id| prefix(id)).collect();
        let wanted: BTreeSet<_> = views
            .iter()
            .filter(|v| {
                prefixes
                    .iter()
                    .any(|p| v.index.directory.prefix_candidates(p).any(|s| v.held(s)))
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
        // The writing domain holds nothing left in this session: purge every
        // slot no domain holds. Slots another domain holds stay untouched.
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
                .filter(|(s, _)| !view.live(*s))
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
            let mut kept = Vec::new();
            for (slot, object) in objects.iter().enumerate() {
                if view.live(slot) {
                    live.push(object.clone());
                    kept.push(slot);
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
                self.delete_pack(conn, &scope.1, view.index.key.id, staged)
                    .await?;
            } else {
                let holders = remap(view, &kept);
                self.rewrite_live_pack(conn, &scope.1, view, live, holders, staged)
                    .await?;
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
        let old_ids: Vec<_> = objects.iter().map(|o| o.id.clone()).collect();
        objects.push(object);
        objects.sort_by(|a, b| a.id.cmp(&b.id));
        // Every holder keeps exactly its previous slots; the new slot is dead.
        let holders = std::iter::once((view.domain.clone(), view.bitmap.clone()))
            .chain(view.others.iter().cloned())
            .map(|(domain, bits)| {
                let mut next = vec![0; objects.len().div_ceil(8)];
                for (old, id) in old_ids.iter().enumerate() {
                    if super::storage::bit(&bits, old) {
                        let slot = objects.iter().position(|o| &o.id == id).unwrap_or(0);
                        next[slot / 8] |= 1 << (slot % 8);
                    }
                }
                (domain, next)
            })
            .collect();
        let mut staged = Staged::default();
        self.rewrite_live_pack(&mut tx, &scope.1, view, objects, holders, &mut staged)
            .await?;
        {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.COMMIT", 0);
            tx.commit().await
        }
        .map_err(ambiguous_database_error)?;
        self.cache_lock()
            .map_err(RuntimeError::retryable)?
            .invalidate(&scope.1);
        Ok(bytes as u64)
    }
}
