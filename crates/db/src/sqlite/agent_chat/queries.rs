//! Agent Chat scope, worker, Genesis, and orchestration queries.

use super::*;

impl SqliteDb {
    pub async fn get_agent_chat_turn_job_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        id: &str,
    ) -> Result<Option<AgentChatTurnJob>> {
        sqlx::query(AGENT_CHAT_TURN_WITH_RETRY_STATE)
            .bind(id)
            .fetch_optional(&mut **tx)
            .await?
            .map(map_agent_chat_turn_job)
            .transpose()
    }
}

/// Typed fields retain decoding failures for the caller's original conditional/error handling.
#[derive(Debug)]
pub struct WakeChatCanonicalScopeRow {
    pub project_id: crate::Result<Option<String>>,
    pub account_id: crate::Result<Option<String>>,
}

impl SqliteDb {
    pub async fn wake_chat_canonical_scope_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        chat_id: &str,
    ) -> crate::Result<Option<WakeChatCanonicalScopeRow>> {
        let rows = sqlx::query("SELECT project_id,account_id FROM agent_chat WHERE id=?")
            .bind(chat_id)
            .fetch_optional(connection)
            .await?;
        rows.map(|row| {
            Ok(WakeChatCanonicalScopeRow {
                project_id: row.try_get("project_id").map_err(crate::DbError::from),
                account_id: row.try_get("account_id").map_err(crate::DbError::from),
            })
        })
        .transpose()
    }
}

impl SqliteDb {
    pub async fn wake_budget_chat_project_id_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        chat_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT project_id FROM agent_chat WHERE id = ?",
        )
        .bind(chat_id)
        .fetch_optional(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn wake_budget_main_chat_account_id_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        chat_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT account_id FROM agent_chat
                 WHERE id = ? AND kind = 'account_main'",
        )
        .bind(chat_id)
        .fetch_optional(connection)
        .await?)
    }
}

/// Typed fields retain decoding failures for the caller's original conditional/error handling.
#[derive(Debug)]
pub struct AttentionChatScopeRow {
    pub kind: crate::Result<String>,
    pub account_id: crate::Result<Option<String>>,
    pub project_id: crate::Result<Option<String>>,
}

impl SqliteDb {
    pub async fn attention_chat_scope(
        &self,
        chat_id: &str,
    ) -> crate::Result<Option<AttentionChatScopeRow>> {
        let rows = sqlx::query("SELECT kind, account_id, project_id FROM agent_chat WHERE id = ?")
            .bind(chat_id)
            .fetch_optional(self.pool())
            .await?;
        rows.map(|row| {
            Ok(AttentionChatScopeRow {
                kind: row.try_get("kind").map_err(crate::DbError::from),
                account_id: row.try_get("account_id").map_err(crate::DbError::from),
                project_id: row.try_get("project_id").map_err(crate::DbError::from),
            })
        })
        .transpose()
    }
}

impl SqliteDb {
    pub async fn attention_scope_chat_count(&self, chat_id: &str) -> crate::Result<i64> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_chat WHERE id = ?")
                .bind(chat_id)
                .fetch_one(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn attention_responder_chat_scope(
        &self,
        chat_id: &str,
    ) -> crate::Result<Option<AttentionChatScopeRow>> {
        let rows = sqlx::query("SELECT kind, account_id, project_id FROM agent_chat WHERE id = ?")
            .bind(chat_id)
            .fetch_optional(self.pool())
            .await?;
        rows.map(|row| {
            Ok(AttentionChatScopeRow {
                kind: row.try_get("kind").map_err(crate::DbError::from),
                account_id: row.try_get("account_id").map_err(crate::DbError::from),
                project_id: row.try_get("project_id").map_err(crate::DbError::from),
            })
        })
        .transpose()
    }
}

impl SqliteDb {
    pub async fn attention_configured_chat_scope(
        &self,
        chat_id: &str,
    ) -> crate::Result<Option<AttentionChatScopeRow>> {
        let rows = sqlx::query("SELECT kind, account_id, project_id FROM agent_chat WHERE id = ?")
            .bind(chat_id)
            .fetch_optional(self.pool())
            .await?;
        rows.map(|row| {
            Ok(AttentionChatScopeRow {
                kind: row.try_get("kind").map_err(crate::DbError::from),
                account_id: row.try_get("account_id").map_err(crate::DbError::from),
                project_id: row.try_get("project_id").map_err(crate::DbError::from),
            })
        })
        .transpose()
    }
}

impl SqliteDb {
    pub async fn attention_chat_identity_access_count(
        &self,
        chat_id: &str,
        main_identity_id: &str,
        project_identity_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_chat c
                 WHERE c.id = ? AND (
                    (c.kind = 'account_main' AND EXISTS (
                        SELECT 1 FROM account_main_agent_binding b
                        WHERE b.account_id = c.account_id
                          AND b.identity_id = ? AND b.state = 'active'
                    )) OR (c.kind = 'project' AND EXISTS (
                        SELECT 1 FROM project_agent_binding b
                        WHERE b.project_id = c.project_id
                          AND b.identity_id = ? AND b.state = 'active'
                    ))
                 )",
        )
        .bind(chat_id)
        .bind(main_identity_id)
        .bind(project_identity_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_chat_account_project_scope(
        &self,
        chat_id: &str,
    ) -> crate::Result<Option<(Option<String>, Option<String>)>> {
        Ok(sqlx::query_as::<_, (Option<String>, Option<String>)>(
            "SELECT account_id, project_id FROM agent_chat WHERE id = ?",
        )
        .bind(chat_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct ActionApproverChatScopeRow {
    row: sqlx::sqlite::SqliteRow,
}

impl ActionApproverChatScopeRow {
    pub fn kind(&self) -> crate::Result<String> {
        Ok(self.row.try_get("kind")?)
    }
    pub fn account_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("account_id")?)
    }
    pub fn project_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("project_id")?)
    }
}

impl SqliteDb {
    pub async fn action_approver_chat_scope(
        &self,
        chat_id: &str,
    ) -> crate::Result<Option<ActionApproverChatScopeRow>> {
        let rows = sqlx::query("SELECT kind, account_id, project_id FROM agent_chat WHERE id = ?")
            .bind(chat_id)
            .fetch_optional(self.pool())
            .await?;
        Ok(rows.map(|row| ActionApproverChatScopeRow { row }))
    }
}

impl SqliteDb {
    pub async fn action_orchestration_chat_project(
        &self,
        chat_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT project_id FROM agent_chat
                 WHERE id = ? AND kind = 'project' LIMIT 1",
        )
        .bind(chat_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn escalation_latest_leased_project_turn_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
        identity_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>("SELECT j.id FROM agent_chat_turn_job j JOIN agent_chat c ON c.id=j.chat_id WHERE c.project_id=? AND j.responder_identity_id=? AND j.status='leased' ORDER BY j.created_at DESC LIMIT 1")
            .bind(project_id)
            .bind(identity_id)
            .fetch_optional(connection)
            .await?)
    }
}

impl SqliteDb {
    pub async fn escalation_turn_chat_project(
        &self,
        chat_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(
            sqlx::query_scalar::<_, Option<String>>("SELECT project_id FROM agent_chat WHERE id=?")
                .bind(chat_id)
                .fetch_optional(self.pool())
                .await?,
        )
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct ChatTurnProtectedInteractionRow {
    row: sqlx::sqlite::SqliteRow,
}
impl ChatTurnProtectedInteractionRow {
    pub fn status(&self) -> crate::Result<String> {
        Ok(self.row.try_get("status")?)
    }
    pub fn expires_at(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("expires_at")?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct ParkedChatTurnInteractionRow {
    row: sqlx::sqlite::SqliteRow,
}
impl ParkedChatTurnInteractionRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn pending_interaction_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("pending_interaction_id")?)
    }
    pub fn version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("version")?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct ExpiredChatTurnLeaseRow {
    row: sqlx::sqlite::SqliteRow,
}
impl ExpiredChatTurnLeaseRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn attempt_count(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("attempt_count")?)
    }
    pub fn max_attempts(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("max_attempts")?)
    }
    pub fn lease_refund_count(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("lease_refund_count")?)
    }
    pub fn version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("version")?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct ProjectTurnAdmissionHandoffRow {
    row: sqlx::sqlite::SqliteRow,
}
impl ProjectTurnAdmissionHandoffRow {
    pub fn content(&self) -> crate::Result<String> {
        Ok(self.row.try_get("content")?)
    }
    pub fn source_revisions_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("source_revisions_json")?)
    }
    pub fn status(&self) -> crate::Result<String> {
        Ok(self.row.try_get("status")?)
    }
    pub fn target_chat_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("target_chat_id")?)
    }
    pub fn target_project_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("target_project_id")?)
    }
}

impl SqliteDb {
    pub async fn project_turn_admission_handoff(
        &self,
        handoff_id: &str,
    ) -> crate::Result<Option<ProjectTurnAdmissionHandoffRow>> {
        let rows = sqlx::query(
            "SELECT h.content, h.source_revisions_json, h.status,
                                h.target_chat_id, target.project_id AS target_project_id
                         FROM agent_handoff h
                         JOIN agent_chat target ON target.id = h.target_chat_id
                         WHERE h.id = ? AND target.kind = 'project'",
        )
        .bind(handoff_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| ProjectTurnAdmissionHandoffRow { row }))
    }
}

pub struct ClaimFifoChatTurnInTxInput<'a> {
    pub available_at: &'a str,
    pub backoff_at: &'a str,
    pub lease_owner: &'a str,
    pub leased_until: &'a str,
    pub updated_at: &'a str,
}

impl SqliteDb {
    pub async fn claim_fifo_chat_turn_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: ClaimFifoChatTurnInTxInput<'_>,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>("WITH candidate AS (
                 SELECT job.id
                 FROM agent_chat_turn_job AS job
                 WHERE job.status IN ('queued', 'retry_wait')
                   AND NOT EXISTS (SELECT 1 FROM agent_chat_topic_rotation r WHERE r.chat_id = job.chat_id AND r.origin_turn_id IS NOT job.id)
                   AND job.attempt_count < job.max_attempts
                   AND (job.next_attempt_at IS NULL OR job.next_attempt_at <= ?)
                   -- A responder whose provider entry is backing off waits
                   -- without spending an attempt; the first claim after the
                   -- window lapses is the trial call.
                   AND NOT EXISTS (
                       SELECT 1 FROM agent_identity AS responder
                       JOIN agent_profile AS profile ON profile.id = responder.selected_profile_id
                       JOIN provider_entry_health AS health
                         ON health.credential_id = profile.credential_ref
                       WHERE responder.id = job.responder_identity_id
                         AND (health.last_error_kind = 'auth'
                              OR julianday(health.backoff_until) > julianday(?))
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM agent_chat_turn_job AS prior
                       WHERE prior.id <> job.id
                         AND prior.responder_identity_id = job.responder_identity_id
                         AND prior.canonical_scope_id = job.canonical_scope_id
                         AND prior.status IN ('queued', 'leased', 'retry_wait', 'awaiting_input')
                         AND (prior.created_at < job.created_at
                              OR (prior.created_at = job.created_at AND prior.id < job.id))
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM agent_chat_turn_job AS active
                       WHERE active.chat_id = job.chat_id AND active.status = 'leased'
                   )
                 ORDER BY job.created_at ASC, job.id ASC
                 LIMIT 1
             )
             UPDATE agent_chat_turn_job
             SET status = 'leased', lease_owner = ?, leased_until = ?,
                 attempt_count = attempt_count + 1, invocation_count = invocation_count + 1, next_attempt_at = NULL,
                 version = version + 1, updated_at = ?
             WHERE id = (SELECT id FROM candidate)
               AND status IN ('queued', 'retry_wait')
             RETURNING id",)
.bind(input.available_at)
.bind(input.backoff_at)
.bind(input.lease_owner)
.bind(input.leased_until)
.bind(input.updated_at)
.fetch_optional(connection).await?)
    }
}

impl SqliteDb {
    pub async fn expired_chat_turn_leases(
        &self,
        now: &str,
    ) -> crate::Result<Vec<ExpiredChatTurnLeaseRow>> {
        let rows = sqlx::query(
            "SELECT id, attempt_count, max_attempts, lease_refund_count, version
             FROM agent_chat_turn_job
             WHERE status = 'leased' AND leased_until IS NOT NULL AND leased_until <= ?
             ORDER BY created_at ASC, id ASC",
        )
        .bind(now)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| ExpiredChatTurnLeaseRow { row })
            .collect())
    }
}

pub struct RecoverExpiredChatTurnLeaseInTxInput<'a> {
    pub status: &'a str,
    pub next_attempt_at: Option<String>,
    pub error_message: &'a str,
    pub failure_json: &'a str,
    pub retry_decision: &'a str,
    pub attempt_count: i64,
    pub refund_increment: i64,
    pub updated_at: &'a str,
    pub turn_id: &'a str,
    pub version: i64,
    pub expired_at: &'a str,
}

impl SqliteDb {
    pub async fn recover_expired_chat_turn_lease_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: RecoverExpiredChatTurnLeaseInTxInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE agent_chat_turn_job
                 SET status = ?, lease_owner = NULL, leased_until = NULL,
                     next_attempt_at = ?, error_code = 'lease_expired',
                     error_message = ?, failure_class_json = ?, retry_decision = ?, attempt_count = ?,
                     lease_refund_count = lease_refund_count + ?,
                     version = version + 1, updated_at = ?
                 WHERE id = ? AND version = ? AND status = 'leased' AND leased_until IS NOT NULL
                   AND leased_until <= ?",)
.bind(input.status)
.bind(input.next_attempt_at)
.bind(input.error_message)
.bind(input.failure_json)
.bind(input.retry_decision)
.bind(input.attempt_count)
.bind(input.refund_increment)
.bind(input.updated_at)
.bind(input.turn_id)
.bind(input.version)
.bind(input.expired_at)
.execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn parked_chat_turn_interactions(
        &self,
    ) -> crate::Result<Vec<ParkedChatTurnInteractionRow>> {
        let rows = sqlx::query(
            "SELECT id, pending_interaction_id, version
             FROM agent_chat_turn_job
             WHERE status = 'awaiting_input'
             ORDER BY created_at ASC, id ASC",
        )
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| ParkedChatTurnInteractionRow { row })
            .collect())
    }
}

pub struct FailChatTurnWithoutInteractionInTxInput<'a> {
    pub failure_json: &'a str,
    pub retry_decision: &'a str,
    pub updated_at: &'a str,
    pub turn_id: &'a str,
    pub version: i64,
}

impl SqliteDb {
    pub async fn fail_chat_turn_without_interaction_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: FailChatTurnWithoutInteractionInTxInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "UPDATE agent_chat_turn_job
                     SET status = 'failed', error_code = 'interaction_orphaned',
                         error_message = 'turn parked without interaction reference',
                         failure_class_json = ?, retry_decision = ?,
                         version = version + 1, updated_at = ?
                     WHERE id = ? AND version = ? AND status = 'awaiting_input'",
        )
        .bind(input.failure_json)
        .bind(input.retry_decision)
        .bind(input.updated_at)
        .bind(input.turn_id)
        .bind(input.version)
        .execute(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn chat_turn_protected_interaction(
        &self,
        interaction_id: &str,
    ) -> crate::Result<Option<ChatTurnProtectedInteractionRow>> {
        let rows = sqlx::query("SELECT status, expires_at FROM protected_interaction WHERE id = ?")
            .bind(interaction_id)
            .fetch_optional(self.pool())
            .await?;
        Ok(rows.map(|row| ChatTurnProtectedInteractionRow { row }))
    }
}

pub struct FailChatTurnMissingInteractionInTxInput<'a> {
    pub failure_json: &'a str,
    pub retry_decision: &'a str,
    pub updated_at: &'a str,
    pub turn_id: &'a str,
    pub version: i64,
}

impl SqliteDb {
    pub async fn fail_chat_turn_missing_interaction_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: FailChatTurnMissingInteractionInTxInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "UPDATE agent_chat_turn_job
                         SET status = 'failed', error_code = 'interaction_orphaned',
                             error_message = 'pending interaction not found',
                             failure_class_json = ?, retry_decision = ?,
                             version = version + 1, updated_at = ?
                         WHERE id = ? AND version = ? AND status = 'awaiting_input'",
        )
        .bind(input.failure_json)
        .bind(input.retry_decision)
        .bind(input.updated_at)
        .bind(input.turn_id)
        .bind(input.version)
        .execute(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn resume_chat_turn_answered_interaction_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        updated_at: &str,
        turn_id: &str,
        version: i64,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "UPDATE agent_chat_turn_job
                             SET status = 'queued', pending_interaction_id = NULL,
                                 next_attempt_at = NULL, version = version + 1, updated_at = ?
                             WHERE id = ? AND version = ? AND status = 'awaiting_input'",
        )
        .bind(updated_at)
        .bind(turn_id)
        .bind(version)
        .execute(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn cancel_chat_turn_cancelled_interaction_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        updated_at: &str,
        turn_id: &str,
        version: i64,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE agent_chat_turn_job
                             SET status = 'cancelled', pending_interaction_id = NULL,
                                 error_code = 'interaction_cancelled', failure_class_json = NULL, retry_decision = NULL,
                                 error_message = 'pending interaction was cancelled',
                                 version = version + 1, updated_at = ?
                             WHERE id = ? AND version = ? AND status = 'awaiting_input'",)
.bind(updated_at)
.bind(turn_id)
.bind(version)
.execute(connection).await?)
    }
}

pub struct FailChatTurnExpiredInteractionInTxInput<'a> {
    pub failure_json: &'a str,
    pub retry_decision: &'a str,
    pub updated_at: &'a str,
    pub turn_id: &'a str,
    pub version: i64,
}

impl SqliteDb {
    pub async fn fail_chat_turn_expired_interaction_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: FailChatTurnExpiredInteractionInTxInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "UPDATE agent_chat_turn_job
                             SET status = 'failed', pending_interaction_id = NULL,
                                 error_code = 'interaction_expired',
                                 error_message = 'pending interaction expired',
                                 failure_class_json = ?, retry_decision = ?,
                                 version = version + 1, updated_at = ?
                             WHERE id = ? AND version = ? AND status = 'awaiting_input'",
        )
        .bind(input.failure_json)
        .bind(input.retry_decision)
        .bind(input.updated_at)
        .bind(input.turn_id)
        .bind(input.version)
        .execute(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn release_shutdown_chat_turn_lease_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        updated_at: String,
        turn_id: &str,
        version: i64,
        lease_owner: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE agent_chat_turn_job SET status = 'queued',
            lease_owner = NULL, leased_until = NULL, next_attempt_at = NULL,
            attempt_count = MAX(0, attempt_count - 1), failure_class_json = NULL, retry_decision = NULL,
            error_code = NULL, error_message = NULL, version = version + 1, updated_at = ?
            WHERE id = ? AND version = ? AND status = 'leased' AND lease_owner = ?",)
.bind(updated_at)
.bind(turn_id)
.bind(version)
.bind(lease_owner)
.execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn delivery_followup_source_turn(
        &self,
        turn_id: Option<&str>,
        message_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM agent_chat_turn_job WHERE id = ? AND triggering_message_id = ?",
        )
        .bind(turn_id)
        .bind(message_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn delivery_followup_canonical_project_chat(
        &self,
        chat_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM agent_chat
             WHERE id = ? AND kind = 'project' AND project_id = ?",
        )
        .bind(chat_id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn renew_chat_turn_worker_lease(
        &self,
        leased_until: String,
        updated_at: &str,
        turn_id: &str,
        lease_owner: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "UPDATE agent_chat_turn_job
                             SET leased_until = ?, version = version + 1, updated_at = ?
                             WHERE id = ? AND status = 'leased' AND lease_owner = ?",
        )
        .bind(leased_until)
        .bind(updated_at)
        .bind(turn_id)
        .bind(lease_owner)
        .execute(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn native_chat_scope_summary(
        &self,
        id: &str,
    ) -> crate::Result<Option<NativeScopeSummaryRow>> {
        let rows = sqlx::query("SELECT id, kind, status, kind AS scope_type, id AS scope_id FROM agent_chat WHERE id = ?").bind(id).fetch_optional(self.pool()).await?;
        Ok(rows.map(|row| NativeScopeSummaryRow { row }))
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct TopicRotationHistoryMessageRow {
    row: sqlx::sqlite::SqliteRow,
}
impl TopicRotationHistoryMessageRow {}

impl SqliteDb {
    pub async fn topic_rotation_history_messages(
        &self,
        chat_id: &str,
        topic_chat_id: &str,
    ) -> crate::Result<Vec<TopicRotationHistoryMessageRow>> {
        let rows = sqlx::query("SELECT author_type, content FROM agent_chat_message WHERE chat_id = ? AND status = 'complete' AND sequence >= COALESCE((SELECT MAX(starting_message_sequence) FROM agent_chat_topic WHERE chat_id = ?), 0) ORDER BY sequence ASC",)
.bind(chat_id)
.bind(topic_chat_id)
.fetch_all(self.pool()).await?;
        Ok(rows
            .into_iter()
            .map(|row| TopicRotationHistoryMessageRow { row })
            .collect())
    }
}

impl TopicRotationHistoryMessageRow {
    pub fn author_type_value(&self) -> String {
        self.row.get("author_type")
    }
    pub fn content_value(&self) -> String {
        self.row.get("content")
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct GenesisDraftMainChatScopeRow {
    row: sqlx::sqlite::SqliteRow,
}
impl GenesisDraftMainChatScopeRow {
    pub fn kind(&self) -> crate::Result<String> {
        Ok(self.row.try_get("kind")?)
    }
    pub fn account_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("account_id")?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct GenesisStartLeasedMainTurnRow {
    row: sqlx::sqlite::SqliteRow,
}
impl GenesisStartLeasedMainTurnRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn triggering_message_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("triggering_message_id")?)
    }
    pub fn correlation_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("correlation_id")?)
    }
    pub fn causation_depth(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("causation_depth")?)
    }
}

impl SqliteDb {
    pub async fn genesis_start_live_source_turn_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        turn_id: &str,
        chat_id: &str,
        message_id: &str,
        responder_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM agent_chat_turn_job
                 WHERE id = ? AND chat_id = ? AND triggering_message_id = ?
                   AND responder_identity_id = ? AND status = 'leased'",
        )
        .bind(turn_id)
        .bind(chat_id)
        .bind(message_id)
        .bind(responder_id)
        .fetch_optional(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn genesis_next_chat_instruction_revision_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        chat_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COALESCE(MAX(revision), 0) + 1
             FROM agent_chat_instruction_revision WHERE chat_id = ?",
        )
        .bind(chat_id)
        .fetch_one(connection)
        .await?)
    }
}

pub struct InsertGenesisChatInstructionInTxInput<'a> {
    pub id: String,
    pub chat_id: &'a str,
    pub source_id: &'a str,
    pub revision: i64,
    pub body: &'a str,
    pub account_id: &'a str,
    pub created_at: &'a str,
}

impl SqliteDb {
    pub async fn insert_genesis_chat_instruction_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: InsertGenesisChatInstructionInTxInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "INSERT INTO agent_chat_instruction_revision (
                id, chat_id, source_type, source_id, revision, body,
                content_guard_json, sensitivity, created_by_type, created_by_id,
                created_at
             ) VALUES (?, ?, 'native', ?, ?, ?, '{}', 'internal',
                       'product_genesis', ?, ?)",
        )
        .bind(input.id)
        .bind(input.chat_id)
        .bind(input.source_id)
        .bind(input.revision)
        .bind(input.body)
        .bind(input.account_id)
        .bind(input.created_at)
        .execute(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn set_main_chat_genesis_instruction_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        revision: i64,
        updated_at: &str,
        chat_id: &str,
        account_id: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "UPDATE agent_chat
             SET instruction_revision = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND kind = 'account_main' AND account_id = ? AND status = 'ready'",
        )
        .bind(revision)
        .bind(updated_at)
        .bind(chat_id)
        .bind(account_id)
        .execute(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn genesis_start_ready_main_chat_id(
        &self,
        account_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT chat.id
             FROM agent_chat AS chat
             JOIN account_main_agent_binding AS binding
               ON binding.account_id = chat.account_id
              AND binding.state = 'active'
             WHERE chat.account_id = ? AND chat.kind = 'account_main'
               AND chat.status = 'ready'
             ORDER BY chat.created_at ASC, chat.id ASC LIMIT 1",
        )
        .bind(account_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn genesis_start_leased_main_turn(
        &self,
        chat_id: &str,
        identity_id: &str,
    ) -> crate::Result<Option<GenesisStartLeasedMainTurnRow>> {
        let rows = sqlx::query(
            "SELECT id, triggering_message_id, correlation_id, causation_depth
                     FROM agent_chat_turn_job
                     WHERE chat_id = ? AND responder_identity_id = ? AND status = 'leased'
                     ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(chat_id)
        .bind(identity_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| GenesisStartLeasedMainTurnRow { row }))
    }
}

impl SqliteDb {
    pub async fn genesis_draft_main_chat_scope(
        &self,
        chat_id: &str,
    ) -> crate::Result<Option<GenesisDraftMainChatScopeRow>> {
        let rows = sqlx::query("SELECT kind, account_id FROM agent_chat WHERE id = ?")
            .bind(chat_id)
            .fetch_optional(self.pool())
            .await?;
        Ok(rows.map(|row| GenesisDraftMainChatScopeRow { row }))
    }
}

impl SqliteDb {
    pub async fn product_genesis_ready_main_binding_count(
        &self,
        chat_id: &str,
        account_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_chat
             JOIN account_main_agent_binding AS binding
               ON binding.account_id = agent_chat.account_id
              AND binding.state = 'active'
             WHERE agent_chat.id = ?
               AND agent_chat.account_id = ?
               AND agent_chat.kind = 'account_main'
               AND agent_chat.status = 'ready'",
        )
        .bind(chat_id)
        .bind(account_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn product_genesis_next_instruction_revision_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        chat_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COALESCE(MAX(revision), 0) + 1
             FROM agent_chat_instruction_revision
             WHERE chat_id = ?",
        )
        .bind(chat_id)
        .fetch_one(connection)
        .await?)
    }
}

pub struct InsertProductGenesisInstructionInTxInput<'a> {
    pub id: String,
    pub chat_id: &'a str,
    pub source_id: &'a str,
    pub revision: i64,
    pub body: &'a str,
    pub account_id: &'a str,
    pub created_at: &'a str,
}

impl SqliteDb {
    pub async fn insert_product_genesis_instruction_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: InsertProductGenesisInstructionInTxInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "INSERT INTO agent_chat_instruction_revision (
                id, chat_id, source_type, source_id, revision, body,
                content_guard_json, sensitivity, created_by_type, created_by_id,
                created_at
             ) VALUES (?, ?, 'native', ?, ?, ?, '{}', 'internal',
                       'product_genesis', ?, ?)",
        )
        .bind(input.id)
        .bind(input.chat_id)
        .bind(input.source_id)
        .bind(input.revision)
        .bind(input.body)
        .bind(input.account_id)
        .bind(input.created_at)
        .execute(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn set_product_genesis_chat_instruction_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        revision: i64,
        updated_at: &str,
        chat_id: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "UPDATE agent_chat
             SET instruction_revision = ?, version = version + 1, updated_at = ?
             WHERE id = ?",
        )
        .bind(revision)
        .bind(updated_at)
        .bind(chat_id)
        .execute(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn chat_usage_admitted_turn_guard_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        turn_id: &str,
        version: i64,
        lease_owner: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM agent_chat_turn_job
                 WHERE id = ? AND version = ? AND status = 'leased'
                   AND lease_owner = ?",
        )
        .bind(turn_id)
        .bind(version)
        .bind(lease_owner)
        .fetch_optional(connection)
        .await?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct DirectProjectChatScopeRow {
    row: sqlx::sqlite::SqliteRow,
}
impl DirectProjectChatScopeRow {
    pub fn kind(&self) -> crate::Result<String> {
        Ok(self.row.try_get("kind")?)
    }
    pub fn project_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("project_id")?)
    }
}

impl SqliteDb {
    pub async fn direct_project_command_chat_scope(
        &self,
        chat_id: &str,
    ) -> crate::Result<Option<DirectProjectChatScopeRow>> {
        let rows = sqlx::query("SELECT kind, project_id FROM agent_chat WHERE id = ? LIMIT 1")
            .bind(chat_id)
            .fetch_optional(self.pool())
            .await?;
        Ok(rows.map(|row| DirectProjectChatScopeRow { row }))
    }
}

impl SqliteDb {
    pub async fn project_action_chat_project_id(
        &self,
        chat_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT project_id FROM agent_chat WHERE id = ? AND kind = 'project' LIMIT 1",
        )
        .bind(chat_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn task_outcome_project_chat_scope(
        &self,
        chat_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT project_id FROM agent_chat
                 WHERE id = ? AND kind = 'project'",
        )
        .bind(chat_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

pub struct TaskProposalChatScopeRow {
    pub kind: String,
    pub project_id: Option<String>,
}

impl SqliteDb {
    pub async fn task_proposal_chat_scope(
        &self,
        chat_id: &str,
    ) -> crate::Result<Option<TaskProposalChatScopeRow>> {
        let row = sqlx::query("SELECT kind, project_id FROM agent_chat WHERE id = ? LIMIT 1")
            .bind(chat_id)
            .fetch_optional(self.pool())
            .await?;
        row.map(|row| {
            Ok(TaskProposalChatScopeRow {
                kind: row.try_get("kind")?,
                project_id: row.try_get("project_id")?,
            })
        })
        .transpose()
    }
}

impl SqliteDb {
    pub async fn charter_command_project_chat_id(
        &self,
        project_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar(
            "SELECT id FROM agent_chat WHERE kind = 'project' AND project_id = ? LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn orchestration_main_chat_account_target(
        &self,
        chat_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT account_id FROM agent_chat WHERE id = ? AND kind = 'account_main'",
        )
        .bind(chat_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn orchestration_direct_chat_project_target(
        &self,
        chat_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT project_id
                     FROM agent_chat
                     WHERE id = ? AND kind = 'project'
                     LIMIT 1",
        )
        .bind(chat_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

pub struct OrchestrationMainChatScopeRow {
    row: SqliteRow,
}
impl OrchestrationMainChatScopeRow {
    pub fn kind(&self) -> crate::Result<String> {
        Ok(self.row.try_get("kind")?)
    }
    pub fn account_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("account_id")?)
    }
}

impl SqliteDb {
    pub async fn orchestration_main_chat_scope(
        &self,
        chat_id: &str,
    ) -> crate::Result<Option<OrchestrationMainChatScopeRow>> {
        let rows = sqlx::query("SELECT kind, account_id FROM agent_chat WHERE id = ? LIMIT 1")
            .bind(chat_id)
            .fetch_optional(self.pool())
            .await?;
        Ok(rows.map(|row| OrchestrationMainChatScopeRow { row }))
    }
}

pub struct OrchestrationBoundProjectChatScopeRow {
    row: SqliteRow,
}
impl OrchestrationBoundProjectChatScopeRow {
    pub fn kind(&self) -> crate::Result<String> {
        Ok(self.row.try_get("kind")?)
    }
    pub fn project_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("project_id")?)
    }
}

impl SqliteDb {
    pub async fn orchestration_bound_project_chat_scope(
        &self,
        identity_id: &str,
        chat_id: &str,
    ) -> crate::Result<Option<OrchestrationBoundProjectChatScopeRow>> {
        let rows = sqlx::query(
            "SELECT chat.kind, chat.project_id, binding.identity_id
                     FROM agent_chat AS chat
                     JOIN project_agent_binding AS binding
                       ON binding.project_id = chat.project_id
                      AND binding.identity_id = ?
                      AND binding.state = 'active'
                     WHERE chat.id = ? AND chat.kind = 'project'
                     LIMIT 1",
        )
        .bind(identity_id)
        .bind(chat_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| OrchestrationBoundProjectChatScopeRow { row }))
    }
}

impl SqliteDb {
    pub async fn project_creation_genesis_source_turn(
        &self,
        chat_id: &str,
        message_id: &str,
    ) -> crate::Result<Option<(String, String, String)>> {
        Ok(sqlx::query_as(
            "SELECT id, responder_identity_id, profile_id
             FROM agent_chat_turn_job
             WHERE chat_id = ? AND triggering_message_id = ?
               AND responder_identity_id IS NOT NULL AND profile_id IS NOT NULL
             ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(chat_id)
        .bind(message_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn project_creation_genesis_instruction_revision(
        &self,
        chat_id: &str,
        genesis_id: &str,
    ) -> crate::Result<Option<(String, i64)>> {
        Ok(sqlx::query_as(
            "SELECT id, revision FROM agent_chat_instruction_revision
             WHERE chat_id = ? AND source_type = 'native' AND source_id = ?
             ORDER BY revision DESC, id DESC LIMIT 1",
        )
        .bind(chat_id)
        .bind(genesis_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

pub struct MainActionChatAccountRow {
    row: SqliteRow,
}
impl MainActionChatAccountRow {
    pub fn account_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("account_id")?)
    }
}

impl SqliteDb {
    pub async fn main_action_chat_account_scope(
        &self,
        chat_id: &str,
    ) -> crate::Result<Option<MainActionChatAccountRow>> {
        let rows = sqlx::query(
            "SELECT kind, account_id FROM agent_chat WHERE id = ? AND kind = 'account_main'",
        )
        .bind(chat_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| MainActionChatAccountRow { row }))
    }
}
