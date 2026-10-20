//! Physical deletion after ownership/membership cascades, without opening AEAD.
use super::SqliteDb;
use sqlx::Row;
use std::time::Instant;

pub const JOURNAL_CLEANUP_PAGE: i64 = 5_000;
pub const JOURNAL_CLEANUP_PACKS: u64 = 32;
pub const JOURNAL_CLEANUP_BYTES: u64 = 8 * 1024 * 1024;

/// Separate from engine apply receipts: these are deleted-owner/orphan copies.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct JournalCleanupBatch {
    pub after_pack_id: i64,
    pub candidates: usize,
    pub deleted_packs: u64,
    pub deleted_objects: u64,
    pub deleted_canonical_bytes: u64,
    pub has_more: bool,
}

impl SqliteDb {
    /// Post-commit activity wakeup shared with domain-event consumers. The
    /// durable orphan index is authoritative; unrelated wakes are harmless.
    /// Task 21 will attach its worker to this signal and retry from zero on
    /// startup/ticks, so cancellation and missed wakes cannot lose cleanup.
    pub fn session_journal_cleanup_notify(&self) -> std::sync::Arc<tokio::sync::Notify> {
        self.domain_event_notify()
    }

    /// One candidate page and one write transaction. A sweep advances `after`;
    /// restart/a new sweep begins at zero to revisit races and earlier orphans.
    pub async fn drain_session_journal_orphans(
        &self,
        after: i64,
    ) -> crate::Result<JournalCleanupBatch> {
        let started = Instant::now();
        // The partial index avoids scanning retained packs or decrypting any
        // index/body unit. Owner labels and membership pack ids are indexed too.
        let candidates: Vec<(i64, String)> = sqlx::query_as(
            "SELECT pack_id, forge_session_id FROM agent_session_journal_pack
             WHERE orphaned=1 AND pack_id>? ORDER BY pack_id LIMIT ?",
        )
        .bind(after)
        .bind(JOURNAL_CLEANUP_PAGE)
        .fetch_all(self.pool())
        .await?;
        let mut report = JournalCleanupBatch {
            after_pack_id: after,
            candidates: candidates.len(),
            ..Default::default()
        };
        if candidates.is_empty() {
            return Ok(report);
        }
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let locked = Instant::now();
        for (id, candidate_owner) in candidates {
            // Re-read under the write lock. A writer can re-admit an orphan or
            // an owner label can change after discovery; neither is deletion.
            let row = sqlx::query(
                "SELECT p.forge_session_id, p.orphaned, p.object_count, p.retained_canonical_bytes,
                    EXISTS(SELECT 1 FROM agent_session s WHERE s.id=p.forge_session_id) AS owner_present,
                    EXISTS(SELECT 1 FROM agent_session_journal_membership m WHERE m.pack_id=p.pack_id) AS held
                 FROM agent_session_journal_pack p WHERE p.pack_id=?",
            ).bind(id).fetch_optional(&mut *tx).await?;
            let Some(row) = row else {
                report.after_pack_id = id;
                continue;
            };
            let owner: String = row.try_get("forge_session_id")?;
            let held: bool = row.try_get("held")?;
            let owner_present: bool = row.try_get("owner_present")?;
            let orphaned: bool = row.try_get("orphaned")?;
            if owner != candidate_owner || held || (owner_present && !orphaned) {
                report.after_pack_id = id;
                continue;
            }
            let bytes = u64::try_from(row.try_get::<i64, _>("retained_canonical_bytes")?)
                .map_err(|_| crate::DbError::Check("invalid journal retained bytes".into()))?;
            let objects = u64::try_from(row.try_get::<i64, _>("object_count")?)
                .map_err(|_| crate::DbError::Check("invalid journal object count".into()))?;
            if report.deleted_packs == JOURNAL_CLEANUP_PACKS
                || report.deleted_canonical_bytes + bytes > JOURNAL_CLEANUP_BYTES
            {
                break;
            }
            // Repeat the ownership/membership predicates at the mutation boundary.
            let removed = sqlx::query(
                "DELETE FROM agent_session_journal_pack
                 WHERE pack_id=? AND forge_session_id=?
                   AND NOT EXISTS(SELECT 1 FROM agent_session_journal_membership WHERE pack_id=?)
                   AND (orphaned=1 OR NOT EXISTS(SELECT 1 FROM agent_session WHERE id=?))",
            )
            .bind(id)
            .bind(&owner)
            .bind(id)
            .bind(&owner)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            report.after_pack_id = id;
            if removed == 1 {
                report.deleted_packs += 1;
                report.deleted_objects += objects;
                report.deleted_canonical_bytes += bytes;
            }
        }
        // All counts describe this committed transaction, never a fabricated
        // JournalApplyReceipt. Existing membership checks invalidate stale caches.
        report.has_more = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM agent_session_journal_pack WHERE orphaned=1 AND pack_id>?)",
        ).bind(report.after_pack_id).fetch_one(&mut *tx).await?;
        tx.commit().await?;
        tracing::debug!(target: "forge_session_journal", candidates=report.candidates, deleted_packs=report.deleted_packs, deleted_objects=report.deleted_objects, deleted_canonical_bytes=report.deleted_canonical_bytes, duration_us=started.elapsed().as_micros() as u64, lock_hold_us=locked.elapsed().as_micros() as u64, "deletion cleanup");
        Ok(report)
    }
}
