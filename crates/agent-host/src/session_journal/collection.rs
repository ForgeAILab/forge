//! Fence-bound SQL pages and read-only physical collection planning.
use super::{
    cache::Scope,
    codec, conflict, corrupt,
    seal::role,
    storage::{ForgeJournalStorage, database_error, prefix},
    wire,
};
use agent_runtime::core::{error::RuntimeError, ids::SessionId, journal::*};
use sqlx::{Row, SqliteConnection};
use std::collections::BTreeSet;

pub(super) const LIMITS: JournalCollectionLimits = JournalCollectionLimits {
    max_page_items: 256,
    max_unit_bytes: codec::PACK_CAP as u64,
    max_apply_units: 32,
};

pub(super) struct CollectionWork {
    pub packs: BTreeSet<i64>,
    pub units: u32,
    pub operation_units: u32,
    pub more: bool,
}

impl ForgeJournalStorage {
    // Backfill only on a writer path. Ranked pins need no decryption unless
    // compared with a new id; sparse ranks normally avoid updating other rows.
    pub(super) async fn rank_pins(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
    ) -> Result<(), RuntimeError> {
        let missing: Vec<Vec<u8>> = sqlx::query_scalar("SELECT pin_key FROM agent_session_journal_pin WHERE storage_id=? AND role=? AND runtime_session_id=? AND pin_order IS NULL")
            .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str())
            .fetch_all(&mut *conn).await.map_err(database_error)?;
        if missing.is_empty() {
            return Ok(());
        }
        let mut ranked: Vec<(Vec<u8>, i64)> = sqlx::query_as("SELECT pin_key,pin_order FROM agent_session_journal_pin WHERE storage_id=? AND role=? AND runtime_session_id=? AND pin_order IS NOT NULL ORDER BY pin_order")
            .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str())
            .fetch_all(&mut *conn).await.map_err(database_error)?;
        if ranked.first().is_some_and(|p| p.1 < 0) || ranked.windows(2).any(|p| p[0].1 >= p[1].1) {
            return Err(corrupt());
        }
        let mut decoded = std::collections::HashMap::new();
        const STEP: i64 = 1 << 32;
        for key in missing {
            let pin = self.pin_id_for_rank(conn, scope, &key).await?;
            let (mut lo, mut hi) = (0, ranked.len());
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                let other_key = &ranked[mid].0;
                if let std::collections::hash_map::Entry::Vacant(entry) =
                    decoded.entry(other_key.clone())
                {
                    entry.insert(self.pin_id_for_rank(conn, scope, other_key).await?);
                }
                match decoded[other_key].cmp(&pin) {
                    std::cmp::Ordering::Less => lo = mid + 1,
                    std::cmp::Ordering::Greater => hi = mid,
                    std::cmp::Ordering::Equal => return Err(corrupt()),
                }
            }
            let rank_between = |ranked: &[(Vec<u8>, i64)]| {
                let left = lo.checked_sub(1).map_or(0, |i| ranked[i].1);
                let right = ranked
                    .get(lo)
                    .map_or_else(|| left.checked_add(STEP), |p| Some(p.1))?;
                (right - left > 1).then(|| left + (right - left) / 2)
            };
            if rank_between(&ranked).is_none() {
                for (index, (key, old)) in ranked.iter_mut().enumerate() {
                    let rank = i64::try_from(index + 1)
                        .ok()
                        .and_then(|n| n.checked_mul(STEP))
                        .ok_or_else(corrupt)?;
                    if *old != rank {
                        self.set_pin_rank(conn, scope, key, rank).await?;
                        *old = rank;
                    }
                }
            }
            let rank = rank_between(&ranked).ok_or_else(corrupt)?;
            self.set_pin_rank(conn, scope, &key, rank).await?;
            decoded.insert(key.clone(), pin);
            ranked.insert(lo, (key, rank));
        }
        Ok(())
    }

    async fn pin_id_for_rank(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        key: &[u8],
    ) -> Result<JournalPinId, RuntimeError> {
        #[cfg(feature = "test-support")]
        self.pin_rank_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let row = sqlx::query("SELECT * FROM agent_session_journal_pin WHERE storage_id=? AND role=? AND runtime_session_id=? AND pin_key=?")
            .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).bind(key)
            .fetch_one(&mut *conn).await.map_err(database_error)?;
        Ok(self.pin_row(&scope.0, &scope.1, row).await?.pin)
    }

    async fn set_pin_rank(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        key: &[u8],
        rank: i64,
    ) -> Result<(), RuntimeError> {
        sqlx::query("UPDATE agent_session_journal_pin SET pin_order=? WHERE storage_id=? AND role=? AND runtime_session_id=? AND pin_key=?")
            .bind(rank).bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).bind(key)
            .execute(conn).await.map_err(database_error)?;
        Ok(())
    }

    pub(super) async fn bounded_page(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
        request: JournalCollectionPageRequest,
    ) -> Result<JournalCollectionPage, RuntimeError> {
        let mut tx = self.db.pool().begin().await.map_err(database_error)?;
        let fence = self
            .load_on(&mut tx, domain, session)
            .await?
            .map_or(JournalFence::default(), |r| r.fence());
        if fence != request.fence {
            return Err(conflict());
        }
        let layout: i64 = sqlx::query_scalar("SELECT coalesce((SELECT layout_generation FROM agent_session_journal_session WHERE storage_id=? AND role=? AND runtime_session_id=?),0)")
            .bind(domain.storage_id.as_str()).bind(role(domain)).bind(session.as_str())
            .fetch_one(&mut *tx).await.map_err(database_error)?;
        let position = request
            .after
            .as_ref()
            .map(|a| a.position(domain, session, fence, request.kind))
            .transpose()?;
        let limit = request.max_items.min(LIMITS.max_page_items) as usize;
        if limit == 0 {
            return Err(conflict());
        }
        let (items, bytes, next) = match request.kind {
            JournalCollectionPageKind::Members => {
                let (first, mut slot) = match position {
                    None => (0, 0),
                    Some(p) => {
                        let p: [u8; 24] = p.try_into().map_err(|_| conflict())?;
                        if i64::from_be_bytes(p[..8].try_into().map_err(|_| conflict())?) != layout
                        {
                            return Err(conflict());
                        }
                        (
                            i64::from_be_bytes(p[8..16].try_into().map_err(|_| conflict())?),
                            usize::try_from(u64::from_be_bytes(
                                p[16..].try_into().map_err(|_| conflict())?,
                            ))
                            .map_err(|_| conflict())?,
                        )
                    }
                };
                // One capped pack per page, even if this scope holds no slots
                // in it. Empty pages carry a strictly advancing continuation.
                let scope = (domain.clone(), session.clone());
                let views = self
                    .directory_range(&mut tx, &scope, false, None, first, 1)
                    .await?;
                let mut ids = Vec::new();
                let mut next = None;
                if let Some(view) = views.first() {
                    let id = view.index.key.id;
                    if id != first {
                        slot = 0;
                    }
                    if view.bitmap.iter().any(|b| *b != 0) {
                        let bodies = self
                            .bodies(&mut tx, &views, &BTreeSet::from([id]), true)
                            .await?;
                        let objects = bodies.get(&id).ok_or_else(corrupt)?;
                        while slot < objects.len() && ids.len() < limit {
                            if view.held(slot) {
                                ids.push(objects[slot].id.clone());
                            }
                            slot += 1;
                        }
                    } else {
                        slot = view.index.key.count;
                    }
                    let count = view.index.key.count;
                    let (next_pack, next_slot) = if slot < count {
                        (id, slot)
                    } else {
                        (id.checked_add(1).ok_or_else(conflict)?, 0)
                    };
                    let more: bool = slot < count || sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_session_journal_pack WHERE runtime_session_id=? AND pack_id>?)")
                        .bind(session.as_str()).bind(id).fetch_one(&mut *tx).await.map_err(database_error)?;
                    if more {
                        let mut p = layout.to_be_bytes().to_vec();
                        p.extend(next_pack.to_be_bytes());
                        p.extend((next_slot as u64).to_be_bytes());
                        next = Some(p);
                    }
                }
                let bytes = ids.len() as u64 * 32;
                (JournalCollectionItems::Members(ids), bytes, next)
            }
            JournalCollectionPageKind::Pins => {
                let scope = (domain.clone(), session.clone());
                let (after, mut previous) = if let Some(p) = position {
                    if p.len() != 40 {
                        return Err(conflict());
                    }
                    let rank = i64::from_be_bytes(p[..8].try_into().map_err(|_| conflict())?);
                    let row = sqlx::query("SELECT * FROM agent_session_journal_pin WHERE storage_id=?1 AND role=?2 AND runtime_session_id=?3 AND pin_key=?4 AND pin_order=?5 AND 1=(SELECT count(*) FROM agent_session_journal_pin WHERE storage_id=?1 AND role=?2 AND runtime_session_id=?3 AND pin_order=?5)")
                        .bind(domain.storage_id.as_str()).bind(role(domain)).bind(session.as_str()).bind(&p[8..]).bind(rank)
                        .fetch_optional(&mut *tx).await.map_err(database_error)?.ok_or_else(corrupt)?;
                    (rank, Some(self.pin_row(domain, session, row).await?.pin))
                } else {
                    (-1, None)
                };
                let missing: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_session_journal_pin WHERE storage_id=? AND role=? AND runtime_session_id=? AND (pin_order IS NULL OR pin_order<0))")
                    .bind(domain.storage_id.as_str()).bind(role(domain)).bind(session.as_str()).fetch_one(&mut *tx).await.map_err(database_error)?;
                if missing {
                    return Err(corrupt());
                }
                let rows = sqlx::query("SELECT * FROM agent_session_journal_pin WHERE storage_id=? AND role=? AND runtime_session_id=? AND pin_order>? ORDER BY pin_order LIMIT ?")
                    .bind(domain.storage_id.as_str()).bind(role(domain)).bind(session.as_str()).bind(after).bind((limit+1) as i64)
                    .fetch_all(&mut *tx).await.map_err(database_error)?;
                let more = rows.len() > limit;
                let mut pins = Vec::new();
                let mut bytes = 0;
                let mut last = after;
                let mut next = None;
                // Validate the lookahead too: a duplicated boundary rank must
                // fail before a continuation could skip the second pin.
                for row in rows {
                    let rank: i64 = row.try_get("pin_order").map_err(|_| corrupt())?;
                    let key: Vec<u8> = row.try_get("pin_key").map_err(|_| corrupt())?;
                    let pin = self.pin_row(&scope.0, &scope.1, row).await?;
                    if rank <= last || previous.as_ref().is_some_and(|p| p >= &pin.pin) {
                        return Err(corrupt());
                    }
                    last = rank;
                    previous = Some(pin.pin.clone());
                    if pins.len() < limit {
                        bytes += wire::pin_bytes(&pin)?.len() as u64;
                        pins.push(pin);
                        let mut p = rank.to_be_bytes().to_vec();
                        p.extend(key);
                        next = Some(p);
                    }
                }
                (
                    JournalCollectionItems::Pins(pins),
                    bytes,
                    if more { next } else { None },
                )
            }
            _ => {
                return Err(RuntimeError::internal(
                    "invalid journal collection page kind",
                ));
            }
        };
        tx.commit().await.map_err(database_error)?;
        Ok(JournalCollectionPage {
            fence,
            items,
            bytes,
            next: next.map(|p| {
                JournalCollectionContinuation::new(domain, session, fence, request.kind, p)
            }),
        })
    }

    pub(super) async fn collection_work(
        &self,
        conn: &mut SqliteConnection,
        plan: &JournalCollectionPlan,
    ) -> Result<Option<CollectionWork>, RuntimeError> {
        let w = &plan.write;
        if w.kind != JournalWriteKind::Collect
            || !w.objects.objects.is_empty()
            || w.operation.is_some()
            || w.pins.iter().any(|p| matches!(p, JournalPinChange::Put(_)))
            || w.expect.generation.checked_add(1) != Some(w.record.generation)
        {
            return Err(conflict());
        }
        if self
            .load_on(conn, &w.domain, &w.session)
            .await?
            .map_or(JournalFence::default(), |r| r.fence())
            != w.expect
        {
            return Err(conflict());
        }
        let scope = (w.domain.clone(), w.session.clone());
        let mut views = self.directory(conn, &scope, false).await?;
        let ids: BTreeSet<_> = w.release.iter().collect();
        let prefixes: BTreeSet<_> = ids.iter().map(|id| prefix(id)).collect();
        let wanted = views
            .iter()
            .filter(|v| {
                prefixes
                    .iter()
                    .any(|p| v.index.directory.prefix_candidates(p).any(|s| v.held(s)))
            })
            .map(|v| v.index.key.id)
            .collect();
        let bodies = self.bodies(conn, &views, &wanted, false).await?;
        let mut packs = BTreeSet::new();
        for view in &mut views {
            if let Some(objects) = bodies.get(&view.index.key.id) {
                for (slot, object) in objects.iter().enumerate() {
                    if view.held(slot) && ids.contains(&object.id) {
                        view.bitmap[slot / 8] &= !(1 << (slot % 8));
                        packs.insert(view.index.key.id);
                    }
                }
            }
        }
        let operation_units = self
            .prune_operation_units(
                conn,
                &scope,
                w.prune_operations_before,
                w.record.writer_epoch,
            )
            .await?;
        let cap = plan.max_units.min(LIMITS.max_apply_units) as usize;
        if packs.len() + operation_units as usize > cap {
            return Ok(None);
        }
        let mut room = cap - packs.len() - operation_units as usize;
        let forced =
            w.record.retirement.is_some() || views.iter().all(|v| v.bitmap.iter().all(|b| *b == 0));
        let mut more = false;
        if forced {
            for view in &views {
                if !view.fully_live() && !packs.contains(&view.index.key.id) {
                    if room == 0 {
                        more = true;
                    } else {
                        packs.insert(view.index.key.id);
                        room -= 1;
                    }
                }
            }
        }
        if self.repacking() {
            for view in &views {
                if room == 0 {
                    break;
                }
                if view.fully_live()
                    && !view.others_pack()
                    && view.index.key.retained < 64 * 1024
                    && packs.insert(view.index.key.id)
                {
                    room -= 1;
                }
            }
        }
        Ok(Some(CollectionWork {
            units: (packs.len() + operation_units as usize) as u32,
            packs,
            operation_units,
            more,
        }))
    }
}
