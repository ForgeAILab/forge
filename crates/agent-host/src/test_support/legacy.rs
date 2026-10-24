//! Helpers use the production encryption/envelope and checkpoint JSON decoder.
use super::*;

impl SqliteProtectedRuntimeStore {
    /// Decode with Forge's real persisted decoder and the runtime's schema /
    /// transition gate. No test-specific schema acceptance path is introduced.
    pub fn decode_legacy_checkpoint(bytes: &[u8]) -> Result<TurnCheckpoint, RuntimeError> {
        let checkpoint = decode_checkpoint(&SessionId::new("schema-guard"), bytes)?;
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    pub fn decode_legacy_snapshot(bytes: &[u8]) -> Result<SessionSnapshot, RuntimeError> {
        Ok(serde_json::from_slice(bytes)?)
    }

    /// Seed raw materialized bytes without calling production save/CAS paths.
    /// Both payloads are retained independently, matching the legacy two-blob
    /// layout. `enveloped = false` exercises the pre-envelope reader too.
    pub async fn seed_legacy_bytes(
        &self,
        session: &SessionId,
        snapshot: Option<&[u8]>,
        checkpoint: Option<&[u8]>,
        enveloped: bool,
    ) -> Result<(), RuntimeError> {
        let forge_id = self.forge_session_id(session).await?;
        let mut sealed = Vec::new();
        for bytes in [snapshot, checkpoint] {
            sealed.push(match bytes {
                Some(bytes) => {
                    let (ciphertext, nonce) = if enveloped {
                        self.seal_session_state(bytes.to_vec()).await?
                    } else {
                        self.seal(bytes)?
                    };
                    (Some(ciphertext), Some(nonce))
                }
                None => (None, None),
            });
        }
        sqlx::query(
            "INSERT INTO protected_agent_session_state
             (session_id, snapshot_ciphertext, snapshot_nonce, checkpoint_ciphertext,
              checkpoint_nonce, key_revision, lcm_policy_revision,
              checkpoint_lcm_policy_revision, state_revision, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
             ON CONFLICT(session_id) DO UPDATE SET
              snapshot_ciphertext = excluded.snapshot_ciphertext,
              snapshot_nonce = excluded.snapshot_nonce,
              checkpoint_ciphertext = excluded.checkpoint_ciphertext,
              checkpoint_nonce = excluded.checkpoint_nonce",
        )
        .bind(forge_id)
        .bind(&sealed[0].0)
        .bind(&sealed[0].1)
        .bind(&sealed[1].0)
        .bind(&sealed[1].1)
        .bind(self.key_revision)
        .bind(crate::FORGE_LCM_POLICY_REVISION)
        .bind(crate::FORGE_LCM_POLICY_REVISION)
        .bind(db::now_rfc3339())
        .execute(self.db.pool())
        .await
        .map_err(|_| RuntimeError::internal("legacy fixture insert failed"))?;
        Ok(())
    }
}
