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
/// Packs are shared by a session's domains: retained bytes cover every pack
/// of each session the domain participates in, live bytes the slots some
/// domain holds and deferred bytes the slots no domain holds. The protected
/// and ordinary values of one session therefore overlap and are not additive.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct JournalByteStats {
    pub retained_bytes: u64,
    pub live_bytes: u64,
    pub deferred_bytes: u64,
    pub skipped_sessions: usize,
}

fn decode_hex(hex: &str) -> Result<Vec<u8>, RuntimeError> {
    if hex.len() % 2 != 0 {
        return Err(corrupt());
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(hex.get(i..i + 2).ok_or_else(corrupt)?, 16).map_err(|_| corrupt())
        })
        .collect()
}

pub(super) fn unreadable(error: &RuntimeError) -> bool {
    error.kind == ErrorKind::Serialization && !error.retryable
}

const WARNING_SESSION_CAP: usize = 4_096;

struct WarningTracker {
    sessions: HashSet<SessionId>,
    capacity: usize,
    observations: u64,
}

#[derive(Debug, PartialEq, Eq)]
struct WarningDecision {
    session: bool,
    overflow: bool,
}

impl WarningTracker {
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            sessions: HashSet::new(),
            capacity,
            observations: 0,
        }
    }

    fn observe(&mut self, session: &SessionId) -> WarningDecision {
        self.observations = self.observations.saturating_add(1);
        if self.sessions.len() == self.capacity || self.sessions.contains(session) {
            return WarningDecision {
                session: false,
                overflow: false,
            };
        }
        self.sessions.insert(session.clone());
        WarningDecision {
            session: true,
            overflow: self.sessions.len() == self.capacity,
        }
    }
}

fn warning_tracker() -> &'static Mutex<WarningTracker> {
    static WARNED: OnceLock<Mutex<WarningTracker>> = OnceLock::new();
    WARNED.get_or_init(|| Mutex::new(WarningTracker::new(WARNING_SESSION_CAP)))
}

impl ForgeJournalStorage {
    /// At most 4,096 once-per-session warnings per process, followed by one
    /// capacity warning. Further observations only increment the aggregate.
    pub fn note_unreadable_session(session: &SessionId) {
        let decision = warning_tracker()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .observe(session);
        if decision.session {
            tracing::warn!(target: "forge_session_journal", skipped_sessions=1,
                "unreadable session journal preserved; maintenance skipped");
        }
        if decision.overflow {
            tracing::warn!(target: "forge_session_journal", warning_session_cap=WARNING_SESSION_CAP,
                "unreadable session warning capacity reached; further session warnings suppressed");
        }
    }

    pub fn unreadable_session_observations() -> u64 {
        warning_tracker()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .observations
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
            // The trait's sharing scopes: sessions with this domain's record
            // or membership. Orphaned packs (deleted owner or no holder) belong
            // to no domain any more; they stay visible here until the deletion
            // drain removes them, so no physical byte silently disappears.
            let sessions: Vec<String> = sqlx::query_scalar(
                "SELECT runtime_session_id FROM agent_session_journal_session
                 WHERE storage_id=?1 AND role=?2 AND (?3 IS NULL OR runtime_session_id>?3)
                 UNION SELECT runtime_session_id FROM agent_session_journal_membership
                 WHERE storage_id=?1 AND role=?2 AND (?3 IS NULL OR runtime_session_id>?3)
                 UNION SELECT runtime_session_id FROM agent_session_journal_pack
                 WHERE orphaned=1 AND (?3 IS NULL OR runtime_session_id>?3)
                 ORDER BY runtime_session_id LIMIT 128",
            )
            .bind(domain.storage_id.as_str())
            .bind(role(domain))
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
            // Union of every domain's bitmap: a slot is live if any domain
            // of the session holds it.
            let rows = sqlx::query(
                "SELECT p.pack_id,p.forge_session_id,p.generation,p.object_count,
                        p.retained_canonical_bytes,p.key_revision,p.format,p.index_nonce,p.index_payload,
                        (SELECT group_concat(hex(m.bitmap)) FROM agent_session_journal_membership m
                         WHERE m.pack_id=p.pack_id) AS bitmaps
                 FROM agent_session_journal_pack p
                 WHERE p.runtime_session_id=? AND p.pack_id>?
                 ORDER BY p.pack_id LIMIT 400",
            )
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
                                domain: None,
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
                        let bitmaps: Option<String> =
                            row.try_get("bitmaps").map_err(|_| corrupt())?;
                        let mut union = vec![0u8; count.div_ceil(8)];
                        for hex in bitmaps.iter().flat_map(|b| b.split(',')) {
                            let bitmap = decode_hex(hex)?;
                            validate_bitmap(&bitmap, count)?;
                            for (out, bits) in union.iter_mut().zip(&bitmap) {
                                *out |= bits;
                            }
                        }
                        for (slot, entry) in directory.slots.iter().enumerate() {
                            if union[slot / 8] & (1 << (slot % 8)) != 0 {
                                live_total = live_total
                                    .checked_add(entry.len as u64)
                                    .ok_or_else(corrupt)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unreadable_warning_tracker_caps_ids_without_eviction_and_warns_overflow_once() {
        let mut tracker = WarningTracker::new(3);
        let mut session_warnings = 0;
        let mut overflow_warnings = 0;
        for index in 0..100 {
            let decision = tracker.observe(&SessionId::new(format!("unreadable-{index}")));
            session_warnings += usize::from(decision.session);
            overflow_warnings += usize::from(decision.overflow);
            assert!(tracker.sessions.len() <= 3);
        }
        assert_eq!(session_warnings, 3);
        assert_eq!(overflow_warnings, 1);
        assert_eq!(tracker.sessions.len(), 3);
        assert_eq!(tracker.observations, 100);
        assert!(tracker.sessions.contains(&SessionId::new("unreadable-0")));
        for _ in 0..10 {
            assert_eq!(
                tracker.observe(&SessionId::new("unreadable-0")),
                WarningDecision {
                    session: false,
                    overflow: false
                }
            );
        }
        assert_eq!(
            tracker.observations, 110,
            "suppression does not suppress counting"
        );
    }
}
