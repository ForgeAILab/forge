//! Scalar host continuity probes. No encrypted payload or journal pack is read.
use super::SqliteDb;
use sqlx::Row;

pub const SESSION_JOURNAL_ORDINARY_STORAGE_ID: &str = "forge.session-journal.v1.ordinary";
pub const SESSION_JOURNAL_PROTECTED_STORAGE_ID: &str = "forge.session-journal.v1.protected";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionPersistenceFlags {
    pub ordinary_published: bool,
    pub protected_published: bool,
    pub retired: bool,
    pub saved: bool,
    pub checkpoint_present: bool,
    /// Legacy decoding is permitted only before either native domain publishes.
    /// Revision-zero reservations do not take authority away from legacy state.
    pub legacy_eligible: bool,
}

impl SqliteDb {
    pub async fn session_persistence_flags(
        &self,
        forge_session_id: &str,
    ) -> crate::Result<SessionPersistenceFlags> {
        let row = sqlx::query(
            "WITH native AS (
                SELECT COALESCE(MAX(CASE WHEN role='ordinary' THEN published ELSE 0 END),0) AS ordinary_published,
                       COALESCE(MAX(CASE WHEN role='protected' THEN published ELSE 0 END),0) AS protected_published,
                       COALESCE(MAX(retired),0) AS retired,
                       COALESCE(MAX(CASE WHEN role='protected' AND published=1 AND retired=0 THEN has_checkpoint ELSE 0 END),0) AS checkpoint_present
                FROM agent_session_journal_session
                WHERE forge_session_id=?
                  AND runtime_session_id=(SELECT runtime_session_id FROM agent_session WHERE id=?)
                  AND ((storage_id=? AND role='ordinary') OR (storage_id=? AND role='protected'))
             )
             SELECT native.*,
                EXISTS(SELECT 1 FROM protected_agent_session_state WHERE session_id=?
                    AND ((snapshot_ciphertext IS NOT NULL AND snapshot_nonce IS NOT NULL)
                      OR (checkpoint_ciphertext IS NOT NULL AND checkpoint_nonce IS NOT NULL))) AS legacy_saved,
                EXISTS(SELECT 1 FROM protected_agent_session_state WHERE session_id=?
                    AND checkpoint_ciphertext IS NOT NULL AND checkpoint_nonce IS NOT NULL) AS legacy_checkpoint
             FROM native",
        )
        .bind(forge_session_id).bind(forge_session_id)
        .bind(SESSION_JOURNAL_ORDINARY_STORAGE_ID).bind(SESSION_JOURNAL_PROTECTED_STORAGE_ID)
        .bind(forge_session_id).bind(forge_session_id)
        .fetch_one(self.pool()).await?;
        let ordinary_published: bool = row.try_get("ordinary_published")?;
        let protected_published: bool = row.try_get("protected_published")?;
        let retired: bool = row.try_get("retired")?;
        let native = ordinary_published || protected_published;
        let legacy_eligible = !native && !retired;
        Ok(SessionPersistenceFlags {
            ordinary_published,
            protected_published,
            retired,
            legacy_eligible,
            saved: !retired
                && (native || (legacy_eligible && row.try_get::<bool, _>("legacy_saved")?)),
            checkpoint_present: !retired
                && (row.try_get::<bool, _>("checkpoint_present")?
                    || (legacy_eligible && row.try_get::<bool, _>("legacy_checkpoint")?)),
        })
    }
}

impl SqliteDb {
    /// Keyset page of host rows eligible for the retention policy to examine.
    /// An EXISTS over indexed owner labels excludes legacy-only rows without
    /// importing them. The policy re-reads the host row before issuing controls.
    pub async fn native_journal_retirement_page(
        &self,
        after: &str,
        limit: u32,
    ) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar(
            "SELECT s.id FROM agent_session s
             WHERE s.id > ? AND s.backend_kind='native'
               AND (s.status IN ('replaced','suspended') OR s.replaced_by_session_id IS NOT NULL)
               AND EXISTS (SELECT 1 FROM agent_session_journal_session j
                   WHERE j.forge_session_id=s.id AND j.runtime_session_id=s.runtime_session_id
                     AND j.published=1 AND j.retired=0
                     AND ((j.storage_id=? AND j.role='protected') OR (j.storage_id=? AND j.role='ordinary')))
             ORDER BY s.id LIMIT ?",
        )
        .bind(after).bind(SESSION_JOURNAL_PROTECTED_STORAGE_ID).bind(SESSION_JOURNAL_ORDINARY_STORAGE_ID).bind(limit)
        .fetch_all(self.pool()).await?)
    }
}

/// Must run in the host activation transaction, before publishing a ready row.
pub(super) async fn require_unretired_session(
    connection: &mut sqlx::SqliteConnection,
    forge_session_id: &str,
) -> crate::Result<()> {
    let retired: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM agent_session_journal_session j
         JOIN agent_session s ON s.id=j.forge_session_id AND s.runtime_session_id=j.runtime_session_id
         WHERE s.id=? AND s.backend_kind='native' AND j.retired=1
           AND ((j.storage_id=? AND j.role='ordinary') OR (j.storage_id=? AND j.role='protected')))",
    )
    .bind(forge_session_id)
    .bind(SESSION_JOURNAL_ORDINARY_STORAGE_ID)
    .bind(SESSION_JOURNAL_PROTECTED_STORAGE_ID)
    .fetch_one(connection).await?;
    if retired {
        return Err(crate::DbError::SessionRetired);
    }
    Ok(())
}

impl SqliteDb {
    /// An open intent owns its reserved successor and protects the predecessor
    /// even after startup suspends it and the successor becomes newest dormant.
    pub async fn native_chat_session_pending_predecessor(
        &self,
        session_id: &str,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM agent_chat_topic_rotation r
             JOIN agent_session successor ON successor.id=r.successor_session_id
             WHERE successor.predecessor_session_id=?)",
        )
        .bind(session_id)
        .fetch_one(self.pool())
        .await?)
    }

    /// Publication is the durable conversion cursor. Reservations and exact-only
    /// partial imports remain eligible; tombstones never permit legacy fallback.
    pub async fn legacy_journal_conversion_page(
        &self,
        after: &str,
        limit: u32,
        excluded: &[String],
    ) -> crate::Result<Vec<(String, String)>> {
        Ok(sqlx::query_as(
            "SELECT s.id, s.runtime_session_id FROM agent_session s
             JOIN protected_agent_session_state l ON l.session_id=s.id
             WHERE s.id > ? AND s.backend_kind='native' AND s.runtime_session_id IS NOT NULL
               AND (l.snapshot_ciphertext IS NOT NULL OR l.checkpoint_ciphertext IS NOT NULL)
               AND s.id NOT IN (SELECT value FROM json_each(?))
               AND NOT EXISTS (SELECT 1 FROM agent_session_journal_session j
                   WHERE j.forge_session_id=s.id AND j.runtime_session_id=s.runtime_session_id
                     AND (j.retired=1 OR (j.role='ordinary' AND j.storage_id=? AND j.published=1)))
             ORDER BY s.id LIMIT ?",
        )
        .bind(after)
        .bind(serde_json::json!(excluded).to_string())
        .bind(SESSION_JOURNAL_ORDINARY_STORAGE_ID)
        .bind(limit)
        .fetch_all(self.pool())
        .await?)
    }

    pub async fn legacy_journal_conversion_remaining(
        &self,
        excluded: &[String],
    ) -> crate::Result<u64> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM agent_session s
             JOIN protected_agent_session_state l ON l.session_id=s.id
             WHERE s.backend_kind='native' AND s.runtime_session_id IS NOT NULL
               AND (l.snapshot_ciphertext IS NOT NULL OR l.checkpoint_ciphertext IS NOT NULL)
               AND s.id NOT IN (SELECT value FROM json_each(?))
               AND NOT EXISTS (SELECT 1 FROM agent_session_journal_session j
                   WHERE j.forge_session_id=s.id AND j.runtime_session_id=s.runtime_session_id
                     AND (j.retired=1 OR (j.role='ordinary' AND j.storage_id=? AND j.published=1)))",
        ).bind(serde_json::json!(excluded).to_string()).bind(SESSION_JOURNAL_ORDINARY_STORAGE_ID).fetch_one(self.pool()).await?;
        Ok(count as u64)
    }
}
