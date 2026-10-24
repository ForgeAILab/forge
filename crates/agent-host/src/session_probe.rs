//! Native-first host probes; legacy decoding remains the pre-import fallback.
use super::*;

impl SqliteProtectedRuntimeStore {
    pub async fn persistence_flags(
        &self,
        session: &SessionId,
    ) -> Result<db::SessionPersistenceFlags, RuntimeError> {
        let forge_id = match self.forge_session_id(session).await {
            Ok(id) => id,
            Err(error) if error.kind == ErrorKind::NotFound => return Ok(Default::default()),
            Err(error) => return Err(error),
        };
        let flags = self
            .db
            .session_persistence_flags(&forge_id)
            .await
            .map_err(protected_database_error)?;
        if flags.retired {
            return Err(RuntimeError::conflict("session journal is retired"));
        }
        Ok(flags)
    }

    pub async fn lcm_overhead_measured(
        &self,
        session: &SessionId,
        flags: db::SessionPersistenceFlags,
    ) -> Result<bool, RuntimeError> {
        let state = self
            .persisted_extension(session, flags, agent_runtime::harness::LCM_COMPONENT_ID)
            .await?;
        Ok(state
            .and_then(|state| {
                state
                    .value
                    .get("fixed_overhead_tokens")
                    .and_then(serde_json::Value::as_u64)
            })
            .is_some_and(|tokens| tokens > 0))
    }
    pub async fn persisted_extension(
        &self,
        session: &SessionId,
        flags: db::SessionPersistenceFlags,
        namespace: &str,
    ) -> Result<Option<agent_runtime::core::store::VersionedSessionState>, RuntimeError> {
        if flags.retired {
            return Err(RuntimeError::conflict("session journal is retired"));
        }
        let value = if flags.ordinary_published || flags.protected_published {
            self.journals.extension(session, flags, namespace).await?
        } else if flags.legacy_eligible && flags.saved {
            SessionStore::load(self, session)
                .await?
                .and_then(|mut snapshot| snapshot.extension_state.remove(namespace))
        } else {
            None
        };
        Ok(value)
    }
}
