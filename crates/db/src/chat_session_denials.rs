//! Session-local reminders of capability-wide native operation denials.

use api_types::{DeniedBy, RetryScope};
use async_trait::async_trait;
use sqlx::Row;

use crate::{Result, SqliteDb};

#[async_trait]
pub trait ChatSessionDenialRepo: Send + Sync {
    /// Resolve a native runtime id (or CLI Forge session id) inside the
    /// authenticated chat scope. A token from another scope records nothing.
    async fn record_chat_session_denial(
        &self,
        identity_id: &str,
        chat_id: &str,
        session_token: &str,
        operation: &str,
        denied_by: &DeniedBy,
    ) -> Result<()>;

    /// Load rows for the resolved session; previews may inspect active sessions.
    /// Current policy is evaluated by services, outside this repository.
    async fn chat_session_denials(
        &self,
        identity_id: &str,
        profile_id: &str,
        chat_id: &str,
        session_id: Option<&str>,
    ) -> Result<Vec<ChatSessionDenial>>;
    async fn delete_chat_session_denial(&self, denial: &ChatSessionDenial) -> Result<()>;
}

/// A short, single-line detail from the caller's own Project pause state.
pub fn project_pause_denial(reason: Option<&str>) -> DeniedBy {
    let detail: String = reason
        .unwrap_or("paused by user")
        .chars()
        .filter(|c| !c.is_control())
        .take(160)
        .collect();
    DeniedBy::ProjectPaused(detail)
}

#[derive(Debug, Clone)]
pub struct ChatSessionDenial {
    pub session_id: String,
    pub runtime_session_id: Option<String>,
    pub operation: String,
    pub denied_by: String,
}

#[async_trait]
impl ChatSessionDenialRepo for SqliteDb {
    async fn record_chat_session_denial(
        &self,
        identity_id: &str,
        chat_id: &str,
        session_token: &str,
        operation: &str,
        denied_by: &DeniedBy,
    ) -> Result<()> {
        if !denied_by.withdraws_operation()
            || denied_by.scope() != RetryScope::Session
            || matches!(denied_by, DeniedBy::PermissionMissing(name) if name == "unknown")
        {
            return Ok(());
        }
        sqlx::query(
            "INSERT OR IGNORE INTO chat_session_denied_operation (session_id, operation, denied_by, created_at)
             SELECT s.id, ?, ?, ? FROM agent_session s
             JOIN agent_context_scope c ON c.id = s.context_scope_id
             WHERE s.identity_id = ? AND c.scope_type = 'agent_chat' AND c.scope_id = ?
               AND (s.id = ? OR s.runtime_session_id = ?)",
        ).bind(operation).bind(denied_by.to_string()).bind(crate::now_rfc3339())
            .bind(identity_id).bind(chat_id).bind(session_token).bind(session_token)
            .execute(self.pool()).await?;
        Ok(())
    }

    async fn chat_session_denials(
        &self,
        identity_id: &str,
        profile_id: &str,
        chat_id: &str,
        session_id: Option<&str>,
    ) -> Result<Vec<ChatSessionDenial>> {
        let rows = sqlx::query(
            "SELECT d.session_id, s.runtime_session_id, d.operation, d.denied_by
             FROM chat_session_denied_operation d
             JOIN agent_session s ON s.id = d.session_id
             JOIN agent_context_scope c ON c.id = s.context_scope_id
             WHERE s.identity_id = ? AND s.profile_id = ?
               AND c.scope_type = 'agent_chat' AND c.scope_id = ?
               AND (? IS NULL OR s.id = ?)
               AND s.status IN ('starting', 'ready', 'running', 'degraded')
             ORDER BY d.operation, d.denied_by",
        )
        .bind(identity_id)
        .bind(profile_id)
        .bind(chat_id)
        .bind(session_id)
        .bind(session_id)
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(ChatSessionDenial {
                    session_id: row.try_get("session_id")?,
                    runtime_session_id: row.try_get("runtime_session_id")?,
                    operation: row.try_get("operation")?,
                    denied_by: row.try_get("denied_by")?,
                })
            })
            .collect()
    }

    async fn delete_chat_session_denial(&self, denial: &ChatSessionDenial) -> Result<()> {
        sqlx::query("DELETE FROM chat_session_denied_operation WHERE session_id = ? AND operation = ? AND denied_by = ?")
            .bind(&denial.session_id).bind(&denial.operation).bind(&denial.denied_by)
            .execute(self.pool()).await?;
        Ok(())
    }
}
