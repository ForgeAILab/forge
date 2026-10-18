use super::*;
/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct NativeApprovedProjectCharterRow {
    row: sqlx::sqlite::SqliteRow,
}
impl NativeApprovedProjectCharterRow {
    pub fn revision_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("revision_id")?)
    }
    pub fn content_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("content_json")?)
    }
    pub fn charter_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("charter_id")?)
    }
    pub fn revision(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("revision")?)
    }
    pub fn charter_version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("charter_version")?)
    }
    pub fn project_mode(&self) -> crate::Result<String> {
        Ok(self.row.try_get("project_mode")?)
    }
    pub fn content_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("content_digest")?)
    }
    pub fn rendered_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("rendered_digest")?)
    }
    pub fn rendered_view(&self) -> crate::Result<String> {
        Ok(self.row.try_get("rendered_view")?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct NativeDiscoverySessionRow {
    row: sqlx::sqlite::SqliteRow,
}
impl NativeDiscoverySessionRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn maturity(&self) -> crate::Result<String> {
        Ok(self.row.try_get("maturity")?)
    }
    pub fn lifecycle(&self) -> crate::Result<String> {
        Ok(self.row.try_get("lifecycle")?)
    }
    pub fn project_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("project_id")?)
    }
    pub fn handoff_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("handoff_id")?)
    }
    pub fn version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("version")?)
    }
    pub fn created_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("created_at")?)
    }
    pub fn updated_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("updated_at")?)
    }
}

impl SqliteDb {
    pub async fn native_discovery_sessions(
        &self,
        account_id: &str,
        limit: i64,
    ) -> crate::Result<Vec<NativeDiscoverySessionRow>> {
        let rows = sqlx::query(
            "SELECT id, maturity, lifecycle, project_id, handoff_id, version,
                    created_at, updated_at
             FROM product_genesis_session
             WHERE account_id = ?
             ORDER BY updated_at DESC, id DESC LIMIT ?",
        )
        .bind(account_id)
        .bind(limit)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| NativeDiscoverySessionRow { row })
            .collect())
    }
}

impl SqliteDb {
    pub async fn native_approved_project_charter(
        &self,
        project_id: &str,
    ) -> crate::Result<Option<NativeApprovedProjectCharterRow>> {
        let rows = sqlx::query(
            "SELECT c.id AS charter_id, c.project_mode, c.version AS charter_version,
                    r.id AS revision_id, r.revision, r.content_digest, r.rendered_digest,
                    r.rendered_view, r.content_json
             FROM project_charter AS c
             JOIN project_charter_revision AS r
               ON r.id = c.current_approved_revision_id AND r.lifecycle = 'approved'
             WHERE c.project_id = ?",
        )
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| NativeApprovedProjectCharterRow { row }))
    }
}

pub struct InsertNativeProjectCommandObservationInput<'a> {
    pub id: &'a str,
    pub project_id: &'a str,
    pub actor_identity_id: &'a str,
    pub scope_type: &'a str,
    pub scope_id: &'a str,
    pub session_id: &'a str,
    pub turn_id: Option<&'a str>,
    pub program: &'a str,
    pub args_json: String,
    pub exit_code: Option<i32>,
    pub success: i64,
    pub output_digest: &'a str,
    pub stdout_excerpt: &'a str,
    pub stderr_excerpt: &'a str,
    pub created_at: &'a str,
}

impl SqliteDb {
    pub async fn insert_native_project_command_observation(
        &self,
        input: InsertNativeProjectCommandObservationInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "INSERT INTO project_command_observation (
                id, project_id, actor_identity_id, scope_type, scope_id, session_id, turn_id,
                program, args_json, exit_code, success, output_digest, stdout_excerpt,
                stderr_excerpt, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(input.id)
        .bind(input.project_id)
        .bind(input.actor_identity_id)
        .bind(input.scope_type)
        .bind(input.scope_id)
        .bind(input.session_id)
        .bind(input.turn_id)
        .bind(input.program)
        .bind(input.args_json)
        .bind(input.exit_code)
        .bind(input.success)
        .bind(input.output_digest)
        .bind(input.stdout_excerpt)
        .bind(input.stderr_excerpt)
        .bind(input.created_at)
        .execute(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn genesis_start_active_session_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        account_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT id FROM product_genesis_session
             WHERE account_id = ? AND lifecycle IN ('discovering', 'ready_for_project')
             ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(account_id)
        .fetch_optional(connection)
        .await?)
    }
}

pub struct InsertMainGenesisSessionInTxInput<'a> {
    pub session_id: &'a str,
    pub account_id: &'a str,
    pub chat_id: &'a str,
    pub prompt_revision: &'a str,
    pub prompt_body: &'a str,
    pub maturity: &'a str,
    pub initial_idea: &'a str,
    pub source_message_ids_json: &'a str,
    pub preferred_identity_id: Option<&'a str>,
    pub created_at: &'a str,
    pub updated_at: &'a str,
}

impl SqliteDb {
    pub async fn insert_main_genesis_session_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: InsertMainGenesisSessionInTxInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "INSERT INTO product_genesis_session (
                id, account_id, main_chat_id, prompt_revision, prompt_body, maturity,
                initial_idea, lifecycle, source_message_ids_json,
                preferred_project_agent_identity_id, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, 'discovering', ?, ?, 1, ?, ?)",
        )
        .bind(input.session_id)
        .bind(input.account_id)
        .bind(input.chat_id)
        .bind(input.prompt_revision)
        .bind(input.prompt_body)
        .bind(input.maturity)
        .bind(input.initial_idea)
        .bind(input.source_message_ids_json)
        .bind(input.preferred_identity_id)
        .bind(input.created_at)
        .bind(input.updated_at)
        .execute(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn genesis_active_main_skill_revision_id(
        &self,
        skill_key: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT current_revision_id FROM operating_skill
             WHERE id = ? AND lifecycle = 'active'",
        )
        .bind(skill_key)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn genesis_discovering_session_id(
        &self,
        account_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT id FROM product_genesis_session
                 WHERE account_id = ? AND lifecycle = 'discovering'
                 ORDER BY updated_at DESC, id DESC LIMIT 1",
        )
        .bind(account_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

pub struct SetGenesisPreferredProjectIdentityInTxInput<'a> {
    pub identity_id: &'a str,
    pub updated_at: &'a str,
    pub session_id: &'a str,
    pub account_id: &'a str,
    pub version: i64,
}

impl SqliteDb {
    pub async fn set_genesis_preferred_project_identity_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: SetGenesisPreferredProjectIdentityInTxInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "UPDATE product_genesis_session
             SET preferred_project_agent_identity_id = ?, version = version + 1,
                 updated_at = ?
             WHERE id = ? AND account_id = ? AND lifecycle = 'discovering'
               AND version = ?",
        )
        .bind(input.identity_id)
        .bind(input.updated_at)
        .bind(input.session_id)
        .bind(input.account_id)
        .bind(input.version)
        .execute(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn genesis_current_active_session_id(
        &self,
        account_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT id FROM product_genesis_session
                 WHERE account_id = ? AND lifecycle IN ('discovering', 'ready_for_project')
                 ORDER BY updated_at DESC, id DESC LIMIT 1",
        )
        .bind(account_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct ProductGenesisSourceMessagesRow {
    row: sqlx::sqlite::SqliteRow,
}
impl ProductGenesisSourceMessagesRow {
    pub fn version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("version")?)
    }
    pub fn source_message_ids_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("source_message_ids_json")?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct ProductGenesisSessionSourceRow {
    row: sqlx::sqlite::SqliteRow,
}
impl ProductGenesisSessionSourceRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn account_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("account_id")?)
    }
    pub fn main_chat_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("main_chat_id")?)
    }
    pub fn prompt_revision(&self) -> crate::Result<String> {
        Ok(self.row.try_get("prompt_revision")?)
    }
    pub fn initial_idea(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("initial_idea")?)
    }
    pub fn preferred_project_agent_identity_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("preferred_project_agent_identity_id")?)
    }
    pub fn charter_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("charter_id")?)
    }
    pub fn charter_revision_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("charter_revision_id")?)
    }
    pub fn charter_approval_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("charter_approval_id")?)
    }
    pub fn charter_version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("charter_version")?)
    }
    pub fn project_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("project_id")?)
    }
    pub fn handoff_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("handoff_id")?)
    }
    pub fn failure_reason(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("failure_reason")?)
    }
    pub fn version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("version")?)
    }
    pub fn created_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("created_at")?)
    }
    pub fn updated_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("updated_at")?)
    }
    pub fn maturity(&self) -> crate::Result<String> {
        Ok(self.row.try_get("maturity")?)
    }
    pub fn lifecycle(&self) -> crate::Result<String> {
        Ok(self.row.try_get("lifecycle")?)
    }
    pub fn source_message_ids_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("source_message_ids_json")?)
    }
}

impl SqliteDb {
    pub async fn product_genesis_session_source(
        &self,
        session_id: &str,
    ) -> crate::Result<Option<ProductGenesisSessionSourceRow>> {
        let rows = sqlx::query(
            "SELECT id, account_id, main_chat_id, prompt_revision, prompt_body, maturity,
                    initial_idea, lifecycle, source_message_ids_json,
                    preferred_project_agent_identity_id, charter_id,
                    charter_revision_id, charter_approval_id, charter_version,
                    project_id, handoff_id,
                    failure_reason, version, created_at, updated_at
             FROM product_genesis_session WHERE id = ?",
        )
        .bind(session_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| ProductGenesisSessionSourceRow { row }))
    }
}

impl SqliteDb {
    pub async fn product_genesis_active_session_source(
        &self,
        account_id: &str,
    ) -> crate::Result<Option<ProductGenesisSessionSourceRow>> {
        let rows = sqlx::query(
            "SELECT id, account_id, main_chat_id, prompt_revision, prompt_body, maturity,
                    initial_idea, lifecycle, source_message_ids_json,
                    preferred_project_agent_identity_id, charter_id,
                    charter_revision_id, charter_approval_id, charter_version,
                    project_id, handoff_id,
                    failure_reason, version, created_at, updated_at
             FROM product_genesis_session
             WHERE account_id = ? AND lifecycle IN ('discovering', 'ready_for_project')
             ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(account_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| ProductGenesisSessionSourceRow { row }))
    }
}

pub struct InsertProductGenesisSessionInTxInput<'a> {
    pub id: &'a str,
    pub account_id: &'a str,
    pub chat_id: &'a str,
    pub prompt_revision: &'a str,
    pub prompt_body: &'a str,
    pub maturity: &'a str,
    pub initial_idea: Option<&'a str>,
    pub preferred_identity_id: Option<&'a str>,
    pub created_at: &'a str,
    pub updated_at: &'a str,
}

impl SqliteDb {
    pub async fn insert_product_genesis_session_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: InsertProductGenesisSessionInTxInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "INSERT INTO product_genesis_session (
                id, account_id, main_chat_id, prompt_revision, prompt_body, maturity,
                initial_idea, lifecycle, source_message_ids_json,
                preferred_project_agent_identity_id, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, 'discovering', '[]', ?, 1, ?, ?)",
        )
        .bind(input.id)
        .bind(input.account_id)
        .bind(input.chat_id)
        .bind(input.prompt_revision)
        .bind(input.prompt_body)
        .bind(input.maturity)
        .bind(input.initial_idea)
        .bind(input.preferred_identity_id)
        .bind(input.created_at)
        .bind(input.updated_at)
        .execute(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn cancel_active_product_genesis(
        &self,
        reason: Option<&str>,
        updated_at: String,
        session_id: &str,
        version: i64,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "UPDATE product_genesis_session
             SET lifecycle = 'cancelled', failure_reason = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?
               AND lifecycle IN ('discovering', 'ready_for_project')",
        )
        .bind(reason)
        .bind(updated_at)
        .bind(session_id)
        .bind(version)
        .execute(self.pool())
        .await?)
    }
}

pub struct TransitionProductGenesisSessionInput<'a> {
    pub lifecycle: &'a str,
    pub project_id: Option<&'a str>,
    pub handoff_id: Option<&'a str>,
    pub failure_reason: Option<&'a str>,
    pub updated_at: String,
    pub session_id: &'a str,
    pub version: i64,
    pub discovering_target: &'a str,
    pub ready_target: &'a str,
}

impl SqliteDb {
    pub async fn transition_product_genesis_session(
        &self,
        input: TransitionProductGenesisSessionInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "UPDATE product_genesis_session
             SET lifecycle = ?, project_id = COALESCE(?, project_id),
                 handoff_id = COALESCE(?, handoff_id), failure_reason = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?
               AND ((lifecycle = 'discovering' AND ? IN ('ready_for_project', 'cancelled'))
                 OR (lifecycle = 'ready_for_project' AND ? IN ('handed_off', 'cancelled'))) ",
        )
        .bind(input.lifecycle)
        .bind(input.project_id)
        .bind(input.handoff_id)
        .bind(input.failure_reason)
        .bind(input.updated_at)
        .bind(input.session_id)
        .bind(input.version)
        .bind(input.discovering_target)
        .bind(input.ready_target)
        .execute(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn product_genesis_source_messages_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        session_id: &str,
    ) -> crate::Result<Option<ProductGenesisSourceMessagesRow>> {
        let rows = sqlx::query(
            "SELECT version, source_message_ids_json
             FROM product_genesis_session WHERE id = ?",
        )
        .bind(session_id)
        .fetch_optional(connection)
        .await?;
        Ok(rows.map(|row| ProductGenesisSourceMessagesRow { row }))
    }
}

impl SqliteDb {
    pub async fn record_product_genesis_source_messages_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        source_ids_json: String,
        updated_at: String,
        session_id: &str,
        version: i64,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "UPDATE product_genesis_session
             SET source_message_ids_json = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?
               AND lifecycle IN ('discovering', 'ready_for_project')",
        )
        .bind(source_ids_json)
        .bind(updated_at)
        .bind(session_id)
        .bind(version)
        .execute(connection)
        .await?)
    }
}

pub struct ApplyProductGenesisSetupInput<'a> {
    pub maturity: Option<&'a str>,
    pub preferred_identity_id: Option<&'a str>,
    pub setup_applied_at: &'a str,
    pub updated_at: &'a str,
    pub session_id: &'a str,
    pub version: i64,
}

impl SqliteDb {
    pub async fn apply_product_genesis_setup(
        &self,
        input: ApplyProductGenesisSetupInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE product_genesis_session
             SET maturity = COALESCE(?, maturity),
                 preferred_project_agent_identity_id = COALESCE(?, preferred_project_agent_identity_id),
                 setup_applied_at = ?,
                 version = version + 1,
                 updated_at = ?
             WHERE id = ? AND version = ? AND lifecycle = 'discovering' AND setup_applied_at IS NULL",)
.bind(input.maturity)
.bind(input.preferred_identity_id)
.bind(input.setup_applied_at)
.bind(input.updated_at)
.bind(input.session_id)
.bind(input.version)
.execute(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn product_genesis_current_version(
        &self,
        session_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(
            sqlx::query_scalar::<_, i64>(
                "SELECT version FROM product_genesis_session WHERE id = ?",
            )
            .bind(session_id)
            .fetch_optional(self.pool())
            .await?,
        )
    }
}

impl SqliteDb {
    pub async fn chat_usage_active_genesis_exists(
        &self,
        chat_id: &str,
        account_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(
                   SELECT 1 FROM product_genesis_session
                   WHERE main_chat_id = ?
                     AND account_id = ?
                     AND lifecycle IN ('discovering', 'ready_for_project')
                )",
        )
        .bind(chat_id)
        .bind(account_id)
        .fetch_one(self.pool())
        .await?)
    }
}

pub struct ChatUsageHandedOffProjectIdInput<'a> {
    pub chat_id: &'a str,
    pub account_id: &'a str,
    pub turn_id: &'a str,
    pub source_message_id: &'a str,
    pub cited_message_id: &'a str,
}

impl SqliteDb {
    pub async fn chat_usage_handed_off_project_id(
        &self,
        input: ChatUsageHandedOffProjectIdInput<'_>,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT g.project_id
                 FROM product_genesis_session g
                 JOIN agent_handoff h ON h.id = g.handoff_id
                 JOIN project p
                   ON p.id = g.project_id
                  AND p.owner_id = g.account_id
                 JOIN project_admission_receipt receipt
                   ON receipt.project_id = g.project_id
                  AND receipt.source_kind = 'genesis_handoff'
                  AND receipt.handoff_id = g.handoff_id
                 JOIN agent_chat target ON target.id = h.target_chat_id
                 JOIN agent_handoff_delivery delivery
                   ON delivery.handoff_id = h.id
                  AND delivery.delivery_sequence = 1
                  AND delivery.status = 'delivered'
                  AND delivery.target_message_id IS h.target_message_id
                  AND delivery.target_turn_job_id IS h.target_turn_job_id
                 WHERE g.main_chat_id = ? AND g.lifecycle = 'handed_off'
                   AND g.project_id IS NOT NULL AND h.target_chat_id = target.id
                   AND g.account_id = ? AND h.source_chat_id = g.main_chat_id
                   AND h.status = 'delivered'
                   AND target.kind = 'project' AND target.project_id = g.project_id
                   AND json_valid(h.source_revisions_json)
                   AND json_extract(
                       h.source_revisions_json,
                       '$.request.source_revisions_digest'
                   ) = receipt.payload_digest
                   AND (h.source_turn_job_id = ? OR h.source_message_id = ?
                        OR EXISTS (
                            SELECT 1
                            FROM json_each(
                                json_extract(h.source_revisions_json, '$.source.message_ids')
                            )
                            WHERE json_each.value = ?
                        ))
                 ORDER BY h.created_at DESC, h.id DESC LIMIT 1",
        )
        .bind(input.chat_id)
        .bind(input.account_id)
        .bind(input.turn_id)
        .bind(input.source_message_id)
        .bind(input.cited_message_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn chat_has_active_product_genesis(&self, chat_id: &str) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(
                     SELECT 1 FROM product_genesis_session
                     WHERE main_chat_id = ? AND lifecycle IN ('discovering', 'ready_for_project')
                 )",
        )
        .bind(chat_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn main_active_genesis_session_id(
        &self,
        account_id: &str,
        charter_id: Option<&str>,
    ) -> crate::Result<Option<String>> {
        let query = if charter_id.is_some() {
            "SELECT id FROM product_genesis_session
                     WHERE account_id = ? AND lifecycle IN ('discovering', 'ready_for_project')
                     ORDER BY CASE WHEN charter_id = ? THEN 0
                                   WHEN charter_id IS NULL THEN 1
                                   ELSE 2 END,
                              updated_at DESC, id DESC LIMIT 1"
        } else {
            "SELECT id FROM product_genesis_session
                     WHERE account_id = ? AND lifecycle IN ('discovering', 'ready_for_project')
                     ORDER BY updated_at DESC, id DESC LIMIT 1"
        };
        let mut request = sqlx::query_scalar::<_, String>(query).bind(account_id);
        if let Some(charter_id) = charter_id {
            request = request.bind(charter_id);
        }
        Ok(request.fetch_optional(self.pool()).await?)
    }
}
