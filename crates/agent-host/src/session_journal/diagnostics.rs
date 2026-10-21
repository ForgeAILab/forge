//! Partial, authenticated byte diagnostics. Never used to decide liveness.
use super::{
    codec, corrupt,
    seal::{Binding, Unit, role},
    storage::{ForgeJournalStorage, counter, database_error, validate_bitmap},
};
use agent_runtime::core::{
    error::{ErrorKind, RuntimeError},
    ids::SessionId,
    journal::*,
};
use sqlx::{Row, SqliteConnection};
use std::{
    collections::HashSet,
    sync::{Mutex, OnceLock},
};

/// Counts cover readable sessions only; skipped sessions have unknown sizes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct JournalByteStats {
    pub retained_bytes: u64,
    pub live_bytes: u64,
    pub deferred_bytes: u64,
    pub skipped_sessions: usize,
}

pub(super) fn unreadable(error: &RuntimeError) -> bool {
    error.kind == ErrorKind::Serialization && !error.retryable
}

impl ForgeJournalStorage {
    /// One neutral warning per runtime session for the entire process, shared
    /// by diagnostics, compaction and the retention worker. Never logs the id.
    pub fn note_unreadable_session(session: &SessionId) {
        static WARNED: OnceLock<Mutex<HashSet<SessionId>>> = OnceLock::new();
        let first = WARNED
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(session.clone());
        if first {
            tracing::warn!(target: "forge_session_journal", skipped_sessions=1,
                "unreadable session journal preserved; maintenance skipped");
        }
    }

    /// Includes orphan and deferred copies, but omits an entire session when
    /// its record or any index cannot authenticate. The omission is explicit
    /// in skipped_sessions, not a claim that unreadable bytes disappeared.
    pub async fn byte_stats(
        &self,
        domain: &JournalDomain,
    ) -> Result<JournalByteStats, RuntimeError> {
        let mut tx = self.db.pool().begin().await.map_err(database_error)?;
        let mut after: Option<String> = None;
        let mut total = JournalByteStats::default();
        loop {
            let sessions: Vec<String> = sqlx::query_scalar(
                "SELECT DISTINCT runtime_session_id FROM agent_session_journal_pack
                 WHERE storage_id=? AND role=? AND (? IS NULL OR runtime_session_id>?)
                 ORDER BY runtime_session_id LIMIT 128",
            )
            .bind(domain.storage_id.as_str())
            .bind(role(domain))
            .bind(&after)
            .bind(&after)
            .fetch_all(&mut *tx)
            .await
            .map_err(database_error)?;
            if sessions.is_empty() {
                break;
            }
            for session in sessions {
                let id = SessionId::new(&session);
                match self.session_byte_stats(&mut tx, domain, &id).await {
                    Ok(stats) => {
                        total.retained_bytes = total
                            .retained_bytes
                            .checked_add(stats.retained_bytes)
                            .ok_or_else(corrupt)?;
                        total.live_bytes = total
                            .live_bytes
                            .checked_add(stats.live_bytes)
                            .ok_or_else(corrupt)?;
                    }
                    Err(error) if unreadable(&error) => {
                        Self::note_unreadable_session(&id);
                        total.skipped_sessions += 1;
                    }
                    Err(error) => return Err(error),
                }
                after = Some(session);
            }
        }
        total.deferred_bytes = total
            .retained_bytes
            .checked_sub(total.live_bytes)
            .ok_or_else(corrupt)?;
        tx.commit().await.map_err(database_error)?;
        Ok(total)
    }

    #[cfg(feature = "test-support")]
    pub async fn byte_stats_for_session_for_test(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
    ) -> Result<JournalByteStats, RuntimeError> {
        let mut tx = self.db.pool().begin().await.map_err(database_error)?;
        let mut stats = self.session_byte_stats(&mut tx, domain, session).await?;
        stats.deferred_bytes = stats
            .retained_bytes
            .checked_sub(stats.live_bytes)
            .ok_or_else(corrupt)?;
        tx.commit().await.map_err(database_error)?;
        Ok(stats)
    }

    async fn session_byte_stats(
        &self,
        conn: &mut SqliteConnection,
        domain: &JournalDomain,
        session: &SessionId,
    ) -> Result<JournalByteStats, RuntimeError> {
        // Missing records are allowed for orphan packs; a bad existing record
        // is never treated as absent. Use the same WAL snapshot for every unit.
        self.load_on(conn, domain, session).await?;
        let mut after = 0i64;
        let mut stats = JournalByteStats::default();
        loop {
            let rows = sqlx::query(
                "SELECT p.pack_id,p.forge_session_id,p.generation,p.object_count,
                        p.retained_canonical_bytes,p.key_revision,p.format,p.index_nonce,p.index_payload,m.bitmap
                 FROM agent_session_journal_pack p
                 LEFT JOIN agent_session_journal_membership m ON m.pack_id=p.pack_id
                 WHERE p.storage_id=? AND p.role=? AND p.runtime_session_id=? AND p.pack_id>?
                 ORDER BY p.pack_id LIMIT 400",
            )
            .bind(domain.storage_id.as_str())
            .bind(role(domain))
            .bind(session.as_str())
            .bind(after)
            .fetch_all(&mut *conn)
            .await
            .map_err(database_error)?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                self.revision(row)?;
            }
            let decodes = rows.len() as u64;
            let keys = self.keys.clone();
            let domain = domain.clone();
            let session = session.clone();
            let (last, retained, live) = self
                .work(move || {
                    let (mut last, mut retained_total, mut live_total) = (0, 0u64, 0u64);
                    for row in rows {
                        let id: i64 = row.try_get("pack_id").map_err(|_| corrupt())?;
                        let owner: String =
                            row.try_get("forge_session_id").map_err(|_| corrupt())?;
                        let generation = counter(&row, "generation")?;
                        let count = usize::try_from(
                            row.try_get::<i64, _>("object_count")
                                .map_err(|_| corrupt())?,
                        )
                        .map_err(|_| corrupt())?;
                        let retained = usize::try_from(
                            row.try_get::<i64, _>("retained_canonical_bytes")
                                .map_err(|_| corrupt())?,
                        )
                        .map_err(|_| corrupt())?;
                        let raw = keys.open(
                            &Binding {
                                unit: Unit::PackIndex,
                                domain: &domain,
                                session: &session,
                                owner: &owner,
                                row: &id.to_be_bytes(),
                                generation,
                            },
                            &row.try_get::<Vec<u8>, _>("index_nonce")
                                .map_err(|_| corrupt())?,
                            &row.try_get::<Vec<u8>, _>("index_payload")
                                .map_err(|_| corrupt())?,
                            codec::MAX_INDEX_BYTES,
                        )?;
                        let directory = codec::decode_index(&raw, count, retained)?;
                        let bitmap: Option<Vec<u8>> =
                            row.try_get("bitmap").map_err(|_| corrupt())?;
                        if let Some(bitmap) = bitmap {
                            validate_bitmap(&bitmap, count)?;
                            for (slot, entry) in directory.slots.iter().enumerate() {
                                if bitmap[slot / 8] & (1 << (slot % 8)) != 0 {
                                    live_total = live_total
                                        .checked_add(entry.len as u64)
                                        .ok_or_else(corrupt)?;
                                }
                            }
                        }
                        retained_total = retained_total
                            .checked_add(retained as u64)
                            .ok_or_else(corrupt)?;
                        last = id;
                    }
                    Ok((last, retained_total, live_total))
                })
                .await?;
            self.cache_lock()?.index_decodes += decodes;
            stats.retained_bytes = stats
                .retained_bytes
                .checked_add(retained)
                .ok_or_else(corrupt)?;
            stats.live_bytes = stats.live_bytes.checked_add(live).ok_or_else(corrupt)?;
            after = last;
        }
        Ok(stats)
    }
}
