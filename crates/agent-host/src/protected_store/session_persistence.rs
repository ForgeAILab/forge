//! Legacy session and checkpoint reads through the protected store.

use super::*;

#[async_trait]
impl SessionStore for SqliteProtectedRuntimeStore {
    fn journal(&self) -> Option<Arc<dyn agent_runtime::core::journal::SessionJournal>> {
        Some(Arc::new(self.journals.ordinary()))
    }

    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("legacy.load", 0);
        let forge_session_id = match self.forge_session_id(id).await {
            Ok(value) => value,
            Err(error) if error.kind == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let row = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.SELECT", 0);
            sqlx::query(
                "SELECT snapshot_ciphertext, snapshot_nonce
             FROM protected_agent_session_state WHERE session_id = ?",
            )
            .bind(&forge_session_id)
            .fetch_optional(self.db.pool())
            .await
        }
        .map_err(protected_database_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let ciphertext: Option<Vec<u8>> = row
            .try_get("snapshot_ciphertext")
            .map_err(|_| RuntimeError::internal("protected session row is invalid"))?;
        let nonce: Option<Vec<u8>> = row
            .try_get("snapshot_nonce")
            .map_err(|_| RuntimeError::internal("protected session row is invalid"))?;
        match (ciphertext, nonce) {
            (Some(ciphertext), Some(nonce)) => {
                let bytes = self.open_session_state(ciphertext, nonce).await?;
                let mut snapshot: SessionSnapshot = serde_json::from_slice(&bytes)?;
                self.omit_relinquished_legacy_lcm(&forge_session_id, &mut snapshot)
                    .await?;
                Ok(Some(snapshot))
            }
            // A checkpoint already owns the exact canonical snapshot. NULL
            // snapshot columns reference that copy rather than storing it twice.
            _ => Ok(CheckpointStore::load_latest(self, id)
                .await?
                .map(|checkpoint| checkpoint.snapshot)),
        }
    }

    async fn save(&self, _snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        Err(RuntimeError::conflict("legacy session store is read-only"))
    }
}

#[async_trait]
impl CheckpointStore for SqliteProtectedRuntimeStore {
    fn journal(&self) -> Option<Arc<dyn agent_runtime::core::journal::SessionJournal>> {
        Some(Arc::new(self.journals.protected()))
    }

    async fn load_latest(
        &self,
        session: &SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("legacy.load_latest", 0);
        let forge_session_id = match self.forge_session_id(session).await {
            Ok(value) => value,
            Err(error) if error.kind == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let row = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.SELECT", 0);
            sqlx::query(
                "SELECT checkpoint_ciphertext, checkpoint_nonce
             FROM protected_agent_session_state WHERE session_id = ?",
            )
            .bind(&forge_session_id)
            .fetch_optional(self.db.pool())
            .await
        }
        .map_err(protected_database_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let ciphertext: Option<Vec<u8>> = row
            .try_get("checkpoint_ciphertext")
            .map_err(|_| RuntimeError::internal("protected checkpoint row is invalid"))?;
        let nonce: Option<Vec<u8>> = row
            .try_get("checkpoint_nonce")
            .map_err(|_| RuntimeError::internal("protected checkpoint row is invalid"))?;
        match (ciphertext, nonce) {
            (Some(ciphertext), Some(nonce)) => {
                let bytes = self
                    .open_session_state(ciphertext, nonce)
                    .await
                    .map_err(|error| {
                        RuntimeError::new(
                            error.kind,
                            format!(
                                "protected checkpoint for session {} could not be decoded: {}",
                                session.as_str(),
                                error
                            ),
                        )
                    })?;
                let mut checkpoint = decode_checkpoint(session, &bytes)?;
                self.omit_relinquished_legacy_lcm(&forge_session_id, &mut checkpoint.snapshot)
                    .await?;
                Ok(Some(checkpoint))
            }
            (None, None) => Ok(None),
            _ => Err(RuntimeError::new(
                ErrorKind::Serialization,
                format!(
                    "protected checkpoint for session {} has incomplete encrypted payload",
                    session.as_str()
                ),
            )),
        }
    }

    async fn save(&self, _checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        Err(RuntimeError::conflict("legacy session store is read-only"))
    }
}
