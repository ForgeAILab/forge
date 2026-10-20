//! Frozen legacy L2 measurement support. Never compiled into production.
//! This is the previous sequential save/digest/order behavior, using the real
//! codec and cipher. It is deliberately separate from both read-only traits.
use super::*;

impl SqliteProtectedRuntimeStore {
    pub fn legacy_snapshot_digest(
        &self,
        snapshot: &SessionSnapshot,
    ) -> Result<String, RuntimeError> {
        // Runtime snapshots stamp the clock on every call, even without a state change.
        // Diagnostic manifests are a bounded recent window and are omitted from
        // new protected checkpoints; neither belongs to canonical state identity.
        let bytes = serde_json::to_vec(&(
            &snapshot.id,
            &snapshot.history,
            &snapshot.usage,
            &snapshot.identity,
            &snapshot.extension_state,
        ))?;
        let mut mac = self.legacy_snapshot_mac.clone();
        mac.update(&bytes);
        Ok(hex::encode(mac.sign().as_ref()))
    }

    pub async fn save_legacy_snapshot_baseline(
        &self,
        snapshot: &SessionSnapshot,
    ) -> Result<(), RuntimeError> {
        let forge_session_id = self.forge_session_id(&snapshot.id).await?;
        let digest = self.legacy_snapshot_digest(snapshot)?;
        let row = sqlx::query(
            "SELECT state_revision, snapshot_digest, key_revision, lcm_policy_revision
             FROM protected_agent_session_state WHERE session_id = ?",
        )
        .bind(&forge_session_id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(|_| RuntimeError::internal("protected session load failed"))?;
        let expected_revision = row
            .as_ref()
            .map(|row| row.get::<i64, _>("state_revision"))
            .unwrap_or(0);
        if let Some(row) = &row {
            let stored_digest: Option<String> = row.get("snapshot_digest");
            let current_format = row.get::<i64, _>("key_revision") == self.key_revision
                && row
                    .get::<Option<String>, _>("lcm_policy_revision")
                    .as_deref()
                    == Some(crate::FORGE_LCM_POLICY_REVISION);
            if current_format {
                let unchanged = match stored_digest {
                    Some(stored) => stored == digest,
                    None => SessionStore::load(self, &snapshot.id)
                        .await?
                        .map(|stored| self.legacy_snapshot_digest(&stored))
                        .transpose()?
                        .is_some_and(|stored| stored == digest),
                };
                if unchanged {
                    return Ok(());
                }
            }
        }
        let bytes = serde_json::to_vec(snapshot)?;
        let (ciphertext, nonce) = self.seal_session_state(bytes).await?;
        let result = sqlx::query(
            "INSERT INTO protected_agent_session_state (
                session_id, snapshot_ciphertext, snapshot_nonce, snapshot_digest,
                key_revision, lcm_policy_revision, state_revision, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, 1, ?)
             ON CONFLICT(session_id) DO UPDATE SET
                snapshot_ciphertext = excluded.snapshot_ciphertext,
                snapshot_nonce = excluded.snapshot_nonce,
                snapshot_digest = excluded.snapshot_digest,
                key_revision = excluded.key_revision,
                checkpoint_lcm_policy_revision = CASE
                    WHEN protected_agent_session_state.checkpoint_ciphertext IS NOT NULL THEN
                        COALESCE(protected_agent_session_state.checkpoint_lcm_policy_revision,
                                 protected_agent_session_state.lcm_policy_revision, '')
                    ELSE protected_agent_session_state.checkpoint_lcm_policy_revision END,
                lcm_policy_revision = excluded.lcm_policy_revision,
                state_revision = protected_agent_session_state.state_revision + 1,
                updated_at = excluded.updated_at
             WHERE protected_agent_session_state.state_revision = ?",
        )
        .bind(forge_session_id)
        .bind(ciphertext)
        .bind(nonce)
        .bind(digest)
        .bind(self.key_revision)
        .bind(crate::FORGE_LCM_POLICY_REVISION)
        .bind(db::now_rfc3339())
        .bind(expected_revision)
        .execute(self.db.pool())
        .await
        .map_err(|_| RuntimeError::internal("protected session save failed"))?;
        if result.rows_affected() == 0 {
            // All SessionStore callers, including runtime lifecycle and turn
            // saves, treat a lost CAS as superseded. Preserve the winner.
            tracing::info!(session_id = %snapshot.id.as_str(),
                "protected session save superseded by competing state");
        }
        Ok(())
    }

    pub async fn save_legacy_checkpoint_baseline(
        &self,
        checkpoint: &TurnCheckpoint,
    ) -> Result<(), RuntimeError> {
        // Runtime callers propagate save errors into the turn. A competing save
        // is ordinary progress: reload once, then keep the winning state.
        for retry in [false, true] {
            match self.save_legacy_checkpoint_once(checkpoint, retry).await {
                Err(error) if error.kind == ErrorKind::Conflict => {
                    if !retry {
                        // A new turn that won the CAS is authoritative. Do not
                        // retry an older turn over it just because its turn ID
                        // differs (which ordinarily permits starting a turn).
                        let winner =
                            CheckpointStore::load_latest(self, &checkpoint.session).await?;
                        if winner.is_some_and(|winner| {
                            winner.turn != checkpoint.turn
                                || winner.state_revision > checkpoint.state_revision
                                || (winner.state_revision == checkpoint.state_revision
                                    && winner.operation_fingerprint
                                        != checkpoint.operation_fingerprint)
                        }) {
                            tracing::info!(session_id = %checkpoint.session.as_str(),
                                turn_id = %checkpoint.turn.as_str(),
                                "protected checkpoint save superseded by competing state");
                            return Ok(());
                        }
                    }
                    tracing::info!(session_id = %checkpoint.session.as_str(),
                        turn_id = %checkpoint.turn.as_str(), superseded = retry,
                        "protected checkpoint save conflicted; retrying once or keeping the winner");
                }
                result => return result,
            }
        }
        Ok(())
    }

    async fn save_legacy_checkpoint_once(
        &self,
        checkpoint: &TurnCheckpoint,
        retry: bool,
    ) -> Result<(), RuntimeError> {
        let forge_session_id = self.forge_session_id(&checkpoint.session).await?;
        let fingerprint = checkpoint.operation_fingerprint.to_string();
        let revision = i64::try_from(checkpoint.state_revision).unwrap_or(i64::MAX);
        let row = sqlx::query(
            "SELECT state_revision, checkpoint_turn_id, checkpoint_revision, checkpoint_fingerprint
             FROM protected_agent_session_state WHERE session_id = ?",
        )
        .bind(&forge_session_id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(|_| RuntimeError::internal("protected checkpoint load failed"))?;
        let expected_revision = row
            .as_ref()
            .map(|row| row.get::<i64, _>("state_revision"))
            .unwrap_or(0);
        if let Some(row) = &row {
            if retry
                && row
                    .get::<Option<String>, _>("checkpoint_turn_id")
                    .is_some_and(|turn| turn != checkpoint.turn.as_str())
            {
                return Err(RuntimeError::conflict(
                    "protected checkpoint turn changed during retry",
                ));
            }
            if row
                .get::<Option<String>, _>("checkpoint_turn_id")
                .as_deref()
                == Some(checkpoint.turn.as_str())
            {
                if let Some(stored_revision) = row.get::<Option<i64>, _>("checkpoint_revision") {
                    if revision < stored_revision
                        || (revision == stored_revision
                            && row
                                .get::<Option<String>, _>("checkpoint_fingerprint")
                                .as_deref()
                                != Some(fingerprint.as_str()))
                    {
                        return Err(RuntimeError::conflict(
                            "protected checkpoint revision moved backwards or changed fingerprint",
                        ));
                    }
                    if revision == stored_revision {
                        return Ok(());
                    }
                }
            }
        }
        let digest = self.legacy_snapshot_digest(&checkpoint.snapshot)?;
        let bytes = serde_json::to_vec(checkpoint)?;
        let (ciphertext, nonce) = self.seal_session_state(bytes).await?;
        let result = sqlx::query(
            "INSERT INTO protected_agent_session_state (
                session_id, checkpoint_ciphertext, checkpoint_nonce,
                checkpoint_turn_id, checkpoint_revision, checkpoint_fingerprint,
                snapshot_digest,
                key_revision, lcm_policy_revision, checkpoint_lcm_policy_revision, state_revision, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
             ON CONFLICT(session_id) DO UPDATE SET
                checkpoint_ciphertext = excluded.checkpoint_ciphertext,
                checkpoint_nonce = excluded.checkpoint_nonce,
                checkpoint_turn_id = excluded.checkpoint_turn_id,
                checkpoint_revision = excluded.checkpoint_revision,
                checkpoint_fingerprint = excluded.checkpoint_fingerprint,
                snapshot_ciphertext = NULL,
                snapshot_nonce = NULL,
                snapshot_digest = excluded.snapshot_digest,
                key_revision = excluded.key_revision,
                lcm_policy_revision = excluded.lcm_policy_revision,
                checkpoint_lcm_policy_revision = excluded.checkpoint_lcm_policy_revision,
                state_revision = protected_agent_session_state.state_revision + 1,
                updated_at = excluded.updated_at
             WHERE protected_agent_session_state.state_revision = ? AND (
                protected_agent_session_state.checkpoint_revision IS NULL
                OR excluded.checkpoint_turn_id != protected_agent_session_state.checkpoint_turn_id
                OR excluded.checkpoint_revision > protected_agent_session_state.checkpoint_revision
                OR (
                    excluded.checkpoint_revision = protected_agent_session_state.checkpoint_revision
                    AND excluded.checkpoint_fingerprint = protected_agent_session_state.checkpoint_fingerprint
                ))",
        )
        .bind(forge_session_id)
        .bind(ciphertext)
        .bind(nonce)
        .bind(checkpoint.turn.as_str())
        .bind(revision)
        .bind(fingerprint)
        .bind(digest)
        .bind(self.key_revision)
        .bind(crate::FORGE_LCM_POLICY_REVISION)
        .bind(crate::FORGE_LCM_POLICY_REVISION)
        .bind(db::now_rfc3339())
        .bind(expected_revision)
        .execute(self.db.pool())
        .await
        .map_err(|_| RuntimeError::internal("protected checkpoint save failed"))?;
        if result.rows_affected() == 0 {
            return Err(RuntimeError::conflict(
                "protected checkpoint revision moved backwards or changed fingerprint",
            ));
        }
        Ok(())
    }
}
