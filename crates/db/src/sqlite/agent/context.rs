//! Agent scope summaries, eligibility, and orchestration context queries.

use super::*;

pub struct NativeScopeSummaryRow {
    pub(crate) row: sqlx::sqlite::SqliteRow,
}
impl NativeScopeSummaryRow {
    pub fn text(&self, field: NativeScopeSummaryField) -> crate::Result<String> {
        Ok(self.row.try_get(field.as_str())?)
    }
    pub fn integer(&self, field: NativeScopeSummaryField) -> crate::Result<i64> {
        Ok(self.row.try_get(field.as_str())?)
    }
}
#[derive(Clone, Copy)]
pub enum NativeScopeSummaryField {
    Id,
    Name,
    Title,
    Status,
    Paused,
    Visibility,
    ScopeType,
    ScopeId,
    ProjectId,
    Priority,
}
impl NativeScopeSummaryField {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Id => "id",
            Self::Name => "name",
            Self::Title => "title",
            Self::Status => "status",
            Self::Paused => "paused",
            Self::Visibility => "visibility",
            Self::ScopeType => "scope_type",
            Self::ScopeId => "scope_id",
            Self::ProjectId => "project_id",
            Self::Priority => "priority",
        }
    }
}

impl SqliteDb {
    pub async fn native_account_scope_summary(
        &self,
        id: &str,
    ) -> crate::Result<Option<NativeScopeSummaryRow>> {
        let rows = sqlx::query(
            "SELECT id, name, status, paused, visibility FROM agent_identity WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| NativeScopeSummaryRow { row }))
    }
}

impl SqliteDb {
    pub async fn topic_rotation_main_binding(
        &self,
        account_id: &str,
    ) -> crate::Result<Option<(String, String)>> {
        Ok(sqlx::query_as::<_, (String, String)>("SELECT identity_id, profile_id FROM account_main_agent_binding WHERE account_id = ? AND state = 'active'",)
.bind(account_id)
.fetch_optional(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn topic_rotation_project_binding(
        &self,
        project_id: &Option<String>,
    ) -> crate::Result<Option<(String, String)>> {
        Ok(sqlx::query_as::<_, (String, String)>("SELECT identity_id, profile_id FROM project_agent_binding WHERE project_id = ? AND state = 'active'",)
.bind(project_id)
.fetch_optional(self.pool()).await?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct GenesisDraftIdentityOwnerRow {
    row: sqlx::sqlite::SqliteRow,
}
impl GenesisDraftIdentityOwnerRow {
    pub fn owner_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("owner_id")?)
    }
}

impl SqliteDb {
    pub async fn genesis_draft_identity_owner(
        &self,
        identity_id: &str,
    ) -> crate::Result<Option<GenesisDraftIdentityOwnerRow>> {
        let rows = sqlx::query(
            "SELECT owner_id, paused, archived_at
                     FROM agent_identity WHERE id = ?",
        )
        .bind(identity_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| GenesisDraftIdentityOwnerRow { row }))
    }
}

impl SqliteDb {
    pub async fn genesis_current_main_principal(
        &self,
        identity_id: &str,
        account_id: &str,
        scope_type: &str,
        scope_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT 1
             FROM agent_identity AS identity
             JOIN account_main_agent_binding AS binding
               ON binding.identity_id = identity.id
              AND binding.account_id = identity.owner_id
              AND binding.state = 'active'
             JOIN agent_chat AS chat
               ON chat.account_id = binding.account_id
              AND chat.kind = 'account_main'
             WHERE identity.id = ?
               AND identity.owner_id = ?
               AND identity.paused = 0
               AND identity.archived_at IS NULL
               AND (? <> 'agent_chat' OR chat.id = ?)
             LIMIT 1",
        )
        .bind(identity_id)
        .bind(account_id)
        .bind(scope_type)
        .bind(scope_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn artifact_authorized_project_identity(
        &self,
        project_id: &str,
        identity_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT identity_id FROM project_agent_binding
                 WHERE project_id = ? AND identity_id = ? AND state = 'active' LIMIT 1",
        )
        .bind(project_id)
        .bind(identity_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn milestone_authorized_project_identity(
        &self,
        project_id: &str,
        identity_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT identity_id FROM project_agent_binding
                 WHERE project_id = ? AND identity_id = ? AND state = 'active' LIMIT 1",
        )
        .bind(project_id)
        .bind(identity_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn decision_authorized_project_identity(
        &self,
        project_id: &str,
        identity_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM project_agent_binding
                 WHERE project_id = ? AND identity_id = ? AND state = 'active' LIMIT 1",
        )
        .bind(project_id)
        .bind(identity_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct DirectProjectActorStatusRow {
    row: sqlx::sqlite::SqliteRow,
}
impl DirectProjectActorStatusRow {
    pub fn paused(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("paused")?)
    }
    pub fn archived_at(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("archived_at")?)
    }
}

impl SqliteDb {
    pub async fn direct_project_actor_status(
        &self,
        identity_id: &str,
    ) -> crate::Result<Option<DirectProjectActorStatusRow>> {
        let rows =
            sqlx::query("SELECT paused, archived_at FROM agent_identity WHERE id = ? LIMIT 1")
                .bind(identity_id)
                .fetch_optional(self.pool())
                .await?;
        Ok(rows.map(|row| DirectProjectActorStatusRow { row }))
    }
}

impl SqliteDb {
    pub async fn direct_project_command_bound_identity(
        &self,
        project_id: &str,
        identity_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT identity_id FROM project_agent_binding
             WHERE project_id = ? AND identity_id = ? AND state = 'active' LIMIT 1",
        )
        .bind(project_id)
        .bind(identity_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn project_action_active_project_binding(
        &self,
        project_id: &str,
        identity_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT project_id FROM project_agent_binding
                 WHERE project_id = ? AND identity_id = ? AND state = 'active' LIMIT 1",
        )
        .bind(project_id)
        .bind(identity_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn placement_eligibility_selected_profiles(
        &self,
    ) -> crate::Result<Vec<(String, i64, String)>> {
        Ok(sqlx::query_as(
            "SELECT id,version,COALESCE(selected_profile_id,'') FROM agent_identity ORDER BY id",
        )
        .fetch_all(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn placement_eligibility_profile_versions(
        &self,
    ) -> crate::Result<Vec<(String, i64)>> {
        Ok(
            sqlx::query_as("SELECT id,version FROM agent_profile ORDER BY id")
                .fetch_all(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn placement_eligibility_credentials(
        &self,
    ) -> crate::Result<Vec<(String, String, bool)>> {
        Ok(
            sqlx::query_as("SELECT id,updated_at,enabled FROM credential_handle ORDER BY id")
                .fetch_all(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn task_proposal_executor_identity(
        &self,
        identity_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(
            sqlx::query_scalar("SELECT 1 FROM agent_identity WHERE id = ? LIMIT 1")
                .bind(identity_id)
                .fetch_optional(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn charter_command_agent_account_owner(
        &self,
        identity_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT owner_id FROM agent_identity WHERE id = ? LIMIT 1",
        )
        .bind(identity_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn charter_command_agent_eligible(
        &self,
        identity_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar(
                    "SELECT 1 FROM agent_identity WHERE id = ? AND paused = 0 AND archived_at IS NULL LIMIT 1",
                ).bind(identity_id).fetch_optional(self.pool()).await?)
    }
}

#[derive(Debug)]
pub struct CharterSelectedAgentEligibilityRow {
    pub tool_policy_json: String,
    pub owner_id: Option<String>,
    pub selected_profile_id: Option<String>,
    pub skill_revision_id: String,
    pub skill_key: String,
    pub lifecycle: String,
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for CharterSelectedAgentEligibilityRow {
    fn from_row(row: &'r SqliteRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            tool_policy_json: row.try_get(0)?,
            owner_id: row.try_get(1)?,
            selected_profile_id: row.try_get(2)?,
            skill_revision_id: row.try_get(3)?,
            skill_key: row.try_get(4)?,
            lifecycle: row.try_get(5)?,
        })
    }
}

impl SqliteDb {
    pub async fn charter_selected_agent_eligibility(
        &self,
        skill_revision_id: &str,
        profile_id: &str,
        identity_id: &str,
        account_id: &str,
    ) -> crate::Result<Option<CharterSelectedAgentEligibilityRow>> {
        Ok(sqlx::query_as(
            "SELECT p.tool_policy_json, i.owner_id, i.selected_profile_id,
                sr.id, sr.skill_key, os.lifecycle
         FROM agent_profile p
         JOIN agent_identity i ON i.id = p.identity_id
         JOIN operating_skill_revision sr ON sr.id = ?
         JOIN operating_skill os ON os.id = sr.operating_skill_id
         WHERE p.id = ? AND p.identity_id = ? AND i.owner_id = ?
           AND i.paused = 0 AND i.archived_at IS NULL
         LIMIT 1",
        )
        .bind(skill_revision_id)
        .bind(profile_id)
        .bind(identity_id)
        .bind(account_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

pub struct AgentRunnableProfileHealthRow {
    row: SqliteRow,
}
impl AgentRunnableProfileHealthRow {
    pub fn profile_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("profile_id")?)
    }
    pub fn status(&self) -> crate::Result<String> {
        Ok(self.row.try_get("status")?)
    }
}

impl SqliteDb {
    pub async fn agent_runnable_profile_health_sources(
        &self,
        profile_ids_json: String,
    ) -> crate::Result<Vec<AgentRunnableProfileHealthRow>> {
        let rows = sqlx::query("SELECT profile_id,status FROM agent_connection_health WHERE profile_id IN (SELECT value FROM json_each(?))").bind(profile_ids_json).fetch_all(self.pool()).await?;
        Ok(rows
            .into_iter()
            .map(|row| AgentRunnableProfileHealthRow { row })
            .collect())
    }
}

impl SqliteDb {
    pub async fn embedded_identity_account_permission_ceiling(
        &self,
        identity_id: &str,
    ) -> crate::Result<String> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT account_permission_ceiling FROM agent_identity WHERE id = ?",
        )
        .bind(identity_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn record_embedded_credential_base_url(
        &self,
        base_url: &str,
        credential_id: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "UPDATE credential_handle
             SET metadata_json = json_set(metadata_json, '$.base_url', ?)
             WHERE id = ?",
        )
        .bind(base_url)
        .bind(credential_id)
        .execute(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn dormant_native_chat_session_id(
        &self,
        identity_id: &str,
        context_scope_id: &str,
        profile_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar(
            "SELECT s.id FROM agent_session s
             WHERE s.identity_id = ? AND s.context_scope_id = ? AND s.profile_id = ?
               AND s.backend_kind = 'native' AND s.status = 'suspended'
               AND s.replaced_by_session_id IS NULL
             ORDER BY s.created_at DESC LIMIT 1",
        )
        .bind(identity_id)
        .bind(context_scope_id)
        .bind(profile_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn orchestration_actor_account_owner(
        &self,
        identity_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT owner_id FROM agent_identity WHERE id = ?",
        )
        .bind(identity_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn admission_account_permission_ceiling(
        &self,
        identity_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT account_permission_ceiling FROM agent_identity WHERE id = ?",
        )
        .bind(identity_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn daemon_owned_executor_agent_exists(
        &self,
        daemon_id: &str,
        executor_type: &str,
        owner_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(
                SELECT 1
                FROM agent_current
                WHERE daemon_id = ?
                  AND executor_type = ?
                  AND owner_id = ?
                LIMIT 1
            )",
        )
        .bind(daemon_id)
        .bind(executor_type)
        .bind(owner_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn genesis_eligible_project_agent_ids(
        &self,
        account_id: &str,
    ) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT identity.id
         FROM agent_identity AS identity
         JOIN agent_profile AS profile
           ON profile.id = identity.selected_profile_id
          AND profile.identity_id = identity.id
         WHERE identity.owner_id = ?
           AND identity.paused = 0
           AND identity.archived_at IS NULL
           AND (profile.credential_ref IS NULL OR EXISTS (
               SELECT 1 FROM credential_handle AS credential
               WHERE credential.id = profile.credential_ref
                 AND credential.status = 'configured'
                 AND credential.enabled = 1
           ))
         ORDER BY identity.created_at ASC, identity.id ASC",
        )
        .bind(account_id)
        .fetch_all(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn project_agent_auto_pick_candidate(
        &self,
        account_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT identity.id
         FROM agent_identity AS identity
         JOIN agent_profile AS profile
           ON profile.id = identity.selected_profile_id
          AND profile.identity_id = identity.id
         WHERE identity.owner_id = ?
           AND identity.paused = 0
           AND identity.archived_at IS NULL
           AND (identity.is_default = 0 OR profile.credential_ref IS NOT NULL)
           AND (profile.credential_ref IS NULL OR EXISTS (
               SELECT 1 FROM credential_handle AS credential
               WHERE credential.id = profile.credential_ref
                 AND credential.status = 'configured'
                 AND credential.enabled = 1
           ))
           AND profile.model IS NOT NULL
           AND trim(profile.model) <> ''
         ORDER BY EXISTS (
               SELECT 1 FROM project_agent_binding AS project_binding
               WHERE project_binding.identity_id = identity.id
                 AND project_binding.state = 'active'
           ) ASC,
           identity.created_at ASC,
           identity.id ASC
         LIMIT 1",
        )
        .bind(account_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn main_action_actor_account_owner(
        &self,
        identity_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT owner_id FROM agent_identity WHERE id = ?",
        )
        .bind(identity_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

pub struct WakeBlockerProjectResponderRow {
    row: SqliteRow,
}
impl WakeBlockerProjectResponderRow {
    pub fn identity_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("identity_id")?)
    }
    pub fn selected_profile_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("selected_profile_id")?)
    }
    pub fn profile_version(&self) -> crate::Result<Option<i64>> {
        Ok(self.row.try_get("profile_version")?)
    }
}

impl SqliteDb {
    pub async fn wake_blocker_project_responder_source(
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
    ) -> crate::Result<Option<WakeBlockerProjectResponderRow>> {
        let rows = sqlx::query(
            "SELECT b.identity_id, i.selected_profile_id, p.version AS profile_version
         FROM project_agent_binding b
         JOIN agent_identity i ON i.id = b.identity_id
         LEFT JOIN agent_profile p ON p.id = i.selected_profile_id
         WHERE b.project_id = ? AND b.state = 'active' AND b.identity_id IS NOT NULL
         ORDER BY b.version DESC LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(connection)
        .await?;
        Ok(rows.map(|row| WakeBlockerProjectResponderRow { row }))
    }
}
