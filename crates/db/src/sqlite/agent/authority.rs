//! Agent action authority and Project turn binding queries.

use super::*;

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct ActionApproverIdentityRow {
    row: sqlx::sqlite::SqliteRow,
}

impl ActionApproverIdentityRow {
    pub fn owner_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("owner_id")?)
    }
    pub fn paused(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("paused")?)
    }
    pub fn archived_at(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("archived_at")?)
    }
    pub fn account_permission_ceiling(&self) -> crate::Result<String> {
        Ok(self.row.try_get("account_permission_ceiling")?)
    }
    pub fn selected_profile_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("selected_profile_id")?)
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct ActionApproverProjectBindingRow {
    row: sqlx::sqlite::SqliteRow,
}

impl ActionApproverProjectBindingRow {
    pub fn permission_ceiling_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("permission_ceiling_json")?)
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct TaskActionActorPolicyRow {
    row: sqlx::sqlite::SqliteRow,
}

impl TaskActionActorPolicyRow {
    pub fn paused(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("paused")?)
    }
    pub fn archived_at(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("archived_at")?)
    }
    pub fn account_permission_ceiling(&self) -> crate::Result<String> {
        Ok(self.row.try_get("account_permission_ceiling")?)
    }
    pub fn selected_profile_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("selected_profile_id")?)
    }
}

impl SqliteDb {
    pub async fn action_approver_identity(
        &self,
        identity_id: &str,
    ) -> crate::Result<Option<ActionApproverIdentityRow>> {
        let rows = sqlx::query(
            "SELECT owner_id, paused, archived_at, account_permission_ceiling,
                selected_profile_id
         FROM agent_identity WHERE id = ?",
        )
        .bind(identity_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| ActionApproverIdentityRow { row }))
    }
}

impl SqliteDb {
    pub async fn action_approver_profile_policy(
        &self,
        profile_id: &str,
        identity_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT tool_policy_json FROM agent_profile
             WHERE id = ? AND identity_id = ?",
        )
        .bind(profile_id)
        .bind(identity_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn action_approver_project_binding(
        &self,
        project_id: &str,
        identity_id: &str,
    ) -> crate::Result<Option<ActionApproverProjectBindingRow>> {
        let rows = sqlx::query(
            "SELECT permission_ceiling_json FROM project_agent_binding
                 WHERE project_id = ? AND identity_id = ? AND state = 'active'",
        )
        .bind(project_id)
        .bind(identity_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| ActionApproverProjectBindingRow { row }))
    }
}

impl SqliteDb {
    pub async fn action_approver_chat_project_permission(
        &self,
        project_id: &str,
        identity_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT permission_ceiling_json FROM project_agent_binding
                             WHERE project_id = ? AND identity_id = ? AND state = 'active'",
        )
        .bind(project_id)
        .bind(identity_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn action_scope_agent_owner(
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
    pub async fn task_action_actor_policy(
        &self,
        identity_id: &str,
    ) -> crate::Result<Option<TaskActionActorPolicyRow>> {
        let rows = sqlx::query(
            "SELECT paused, archived_at, account_permission_ceiling, selected_profile_id
         FROM agent_identity WHERE id = ?",
        )
        .bind(identity_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| TaskActionActorPolicyRow { row }))
    }
}

impl SqliteDb {
    pub async fn task_action_actor_profile_policy(
        &self,
        profile_id: &str,
        identity_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT tool_policy_json FROM agent_profile WHERE id = ? AND identity_id = ?",
        )
        .bind(profile_id)
        .bind(identity_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn action_actor_paused_or_archived(&self, identity_id: &str) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT paused OR archived_at IS NOT NULL FROM agent_identity WHERE id=?",
        )
        .bind(identity_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn escalation_active_project_binding_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
        identity_id: &str,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM project_agent_binding WHERE project_id=? AND identity_id=? AND state='active')")
            .bind(project_id)
            .bind(identity_id)
            .fetch_one(connection)
            .await?)
    }
}

impl SqliteDb {
    pub async fn bootstrap_unowned_agent_identities_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        user_id: &str,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "UPDATE agent_identity
             SET owner_id = ?, visibility = 'account'
             WHERE owner_id IS NULL",
        )
        .bind(user_id)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct ProjectTurnBindingAuthorityRow {
    row: sqlx::sqlite::SqliteRow,
}
impl ProjectTurnBindingAuthorityRow {
    pub fn state(&self) -> crate::Result<String> {
        Ok(self.row.try_get("state")?)
    }
    pub fn version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("version")?)
    }
    pub fn project_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("project_id")?)
    }
    pub fn identity_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("identity_id")?)
    }
    pub fn operating_skill_revision_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("operating_skill_revision_id")?)
    }
    pub fn policy_revision(&self) -> crate::Result<String> {
        Ok(self.row.try_get("policy_revision")?)
    }
    pub fn policy_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("policy_digest")?)
    }
    pub fn charter_id_optional(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("charter_id")?)
    }
    pub fn charter_revision_id_optional(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("charter_revision_id")?)
    }
    pub fn charter_setup_required(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("charter_setup_required")?)
    }
    pub fn admission_receipt_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("admission_receipt_id")?)
    }
    pub fn charter_approval_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("charter_approval_id")?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct FrozenProjectTurnBindingRow {
    row: sqlx::sqlite::SqliteRow,
}
impl FrozenProjectTurnBindingRow {
    pub fn identity_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("identity_id")?)
    }
    pub fn version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("version")?)
    }
    pub fn autonomy_policy_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("autonomy_policy_json")?)
    }
    pub fn permission_ceiling_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("permission_ceiling_json")?)
    }
    pub fn policy_revision(&self) -> crate::Result<String> {
        Ok(self.row.try_get("policy_revision")?)
    }
    pub fn policy_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("policy_digest")?)
    }
    pub fn state(&self) -> crate::Result<String> {
        Ok(self.row.try_get("state")?)
    }
}

impl SqliteDb {
    pub async fn frozen_project_turn_binding(
        &self,
        binding_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<FrozenProjectTurnBindingRow>> {
        let rows = sqlx::query(
            "SELECT identity_id, profile_id, state, version, autonomy_policy_json,
                            permission_ceiling_json,
                            operating_skill_revision_id, policy_revision, policy_digest
                     FROM project_agent_binding
                     WHERE id = ? AND project_id = ?",
        )
        .bind(binding_id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| FrozenProjectTurnBindingRow { row }))
    }
}

impl SqliteDb {
    pub async fn main_turn_account_permission_ceiling(
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
    pub async fn project_turn_binding_authority(
        &self,
        binding_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<ProjectTurnBindingAuthorityRow>> {
        let rows = sqlx::query(
            "SELECT id, project_id, state, version, identity_id,
                    operating_skill_revision_id, policy_revision, policy_digest,
                    charter_id, charter_revision_id, charter_setup_required,
                    admission_receipt_id, charter_approval_id
             FROM project_agent_binding
             WHERE id = ? AND project_id = ?",
        )
        .bind(binding_id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| ProjectTurnBindingAuthorityRow { row }))
    }
}

impl SqliteDb {
    pub async fn native_project_verify_workspace_path(
        &self,
        identity_id: &str,
        scope_type: &str,
        scope_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT workspace_path FROM agent_context_scope
             WHERE identity_id = ? AND scope_type = ? AND scope_id = ?
               AND workspace_access = 'project_verify'
             ORDER BY updated_at DESC LIMIT 1",
        )
        .bind(identity_id)
        .bind(scope_type)
        .bind(scope_id)
        .fetch_optional(self.pool())
        .await?)
    }
}
