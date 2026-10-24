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
    // Pin ids stay sealed. Persist only their relative ranks, at pin mutation or
    // once when hydrating a pre-upgrade scope, never in a collector page.
    pub(super) async fn rank_pins(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
    ) -> Result<(), RuntimeError> {
        let rows = sqlx::query("SELECT * FROM agent_session_journal_pin WHERE storage_id=? AND role=? AND runtime_session_id=?")
            .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str())
            .fetch_all(&mut *conn).await.map_err(database_error)?;
        let mut pins = Vec::with_capacity(rows.len());
        for row in rows {
            let key: Vec<u8> = row.try_get("pin_key").map_err(|_| corrupt())?;
            pins.push((self.pin_row(&scope.0, &scope.1, row).await?.pin, key));
        }
        pins.sort_by(|a, b| a.0.cmp(&b.0));
        for (rank, (_, key)) in pins.into_iter().enumerate() {
            sqlx::query("UPDATE agent_session_journal_pin SET pin_order=? WHERE storage_id=? AND role=? AND runtime_session_id=? AND pin_key=?")
                .bind(i64::try_from(rank).map_err(|_| corrupt())?).bind(scope.0.storage_id.as_str())
                .bind(role(&scope.0)).bind(scope.1.as_str()).bind(key)
                .execute(&mut *conn).await.map_err(database_error)?;
        }
        Ok(())
    }

    pub(super) async fn upgrade_pin_ranks(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
    ) -> Result<(), RuntimeError> {
        let missing: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_session_journal_pin WHERE storage_id=? AND role=? AND runtime_session_id=? AND pin_order IS NULL)")
            .bind(domain.storage_id.as_str()).bind(role(domain)).bind(session.as_str())
            .fetch_one(self.db.pool()).await.map_err(database_error)?;
        if missing {
            let mut tx = db::begin_immediate(self.db.pool())
                .await
                .map_err(database_error)?;
            self.rank_pins(&mut tx, &(domain.clone(), session.clone()))
                .await?;
            tx.commit()
                .await
                .map_err(super::storage::ambiguous_database_error)?;
        }
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
                let after = position
                    .map(|p| {
                        <[u8; 8]>::try_from(p)
                            .map(i64::from_be_bytes)
                            .map_err(|_| conflict())
                    })
                    .transpose()?
                    .unwrap_or(-1);
                let missing: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_session_journal_pin WHERE storage_id=? AND role=? AND runtime_session_id=? AND pin_order IS NULL)")
                    .bind(domain.storage_id.as_str()).bind(role(domain)).bind(session.as_str()).fetch_one(&mut *tx).await.map_err(database_error)?;
                if missing {
                    return Err(conflict());
                }
                let rows = sqlx::query("SELECT * FROM agent_session_journal_pin WHERE storage_id=? AND role=? AND runtime_session_id=? AND pin_order>? ORDER BY pin_order LIMIT ?")
                    .bind(domain.storage_id.as_str()).bind(role(domain)).bind(session.as_str()).bind(after).bind((limit+1) as i64)
                    .fetch_all(&mut *tx).await.map_err(database_error)?;
                let more = rows.len() > limit;
                let mut pins = Vec::new();
                let mut bytes = 0;
                let mut last = after;
                for row in rows.into_iter().take(limit) {
                    last = row.try_get("pin_order").map_err(|_| corrupt())?;
                    let pin = self.pin_row(domain, session, row).await?;
                    bytes += wire::pin_bytes(&pin)?.len() as u64;
                    pins.push(pin);
                }
                (
                    JournalCollectionItems::Pins(pins),
                    bytes,
                    more.then(|| last.to_be_bytes().to_vec()),
                )
            }
            _ => return Err(conflict()),
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
