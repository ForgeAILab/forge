//! Project authority, settings, ownership and typed projection queries, plus primary-milestone writes and owner bootstrap.

use super::*;

impl SqliteDb {
    pub async fn lock_primary_milestone_project_version_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
        expected_project_version: i64,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "UPDATE project SET version = version
             WHERE id = ? AND version = ?",
        )
        .bind(project_id)
        .bind(expected_project_version)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn set_primary_milestone_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        primary_milestone_id: Option<&str>,
        now: &str,
        project_id: &str,
        expected_project_version: i64,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "UPDATE project SET primary_milestone_id = ?, version = version + 1,
                 updated_at = ? WHERE id = ? AND version = ?",
        )
        .bind(primary_milestone_id)
        .bind(now)
        .bind(project_id)
        .bind(expected_project_version)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn primary_milestone_id_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
    ) -> crate::Result<Option<String>> {
        let result = sqlx::query_scalar("SELECT primary_milestone_id FROM project WHERE id = ?")
            .bind(project_id)
            .fetch_one(connection)
            .await?;
        Ok(result)
    }
}

impl SqliteDb {
    pub async fn repair_primary_milestone_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        replacement: Option<String>,
        now: &str,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "UPDATE project SET primary_milestone_id = ?, version = version + 1,
             updated_at = ? WHERE id = ? AND primary_milestone_id = ?",
        )
        .bind(replacement)
        .bind(now)
        .bind(project_id)
        .bind(milestone_id)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn workspace_admission_project_settings(
        &self,
        project_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT settings FROM project WHERE id=?")
                .bind(project_id)
                .fetch_optional(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn workspace_admission_project_version_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
    ) -> crate::Result<i64> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT version FROM project WHERE id = ?")
                .bind(project_id)
                .fetch_one(connection)
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn orphan_attempt_workflow_definition_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT workflow_definition FROM project WHERE id = ?")
                .bind(project_id)
                .fetch_optional(connection)
                .await?,
        )
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct ActionOrchestrationCharterStateRow {
    row: sqlx::sqlite::SqliteRow,
}

impl ActionOrchestrationCharterStateRow {
    pub fn charter_status(&self) -> crate::Result<String> {
        Ok(self.row.try_get("charter_status")?)
    }
    pub fn charter_setup_required(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("charter_setup_required")?)
    }
    pub fn current_charter_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("current_charter_id")?)
    }
    pub fn current_charter_revision_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("current_charter_revision_id")?)
    }
}

impl SqliteDb {
    pub async fn action_orchestration_charter_state(
        &self,
        project_id: &str,
    ) -> crate::Result<Option<ActionOrchestrationCharterStateRow>> {
        let rows = sqlx::query(
            "SELECT charter_status, charter_setup_required,
                    current_charter_id, current_charter_revision_id
             FROM project WHERE id = ? LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| ActionOrchestrationCharterStateRow { row }))
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct EffectiveProjectIdentitySourceRow {
    row: sqlx::sqlite::SqliteRow,
}

impl EffectiveProjectIdentitySourceRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn name(&self) -> crate::Result<String> {
        Ok(self.row.try_get("name")?)
    }
    pub fn settings(&self) -> crate::Result<String> {
        Ok(self.row.try_get("settings")?)
    }
    pub fn paused_at(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("paused_at")?)
    }
    pub fn charter_status(&self) -> crate::Result<String> {
        Ok(self.row.try_get("charter_status")?)
    }
    pub fn charter_setup_required(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("charter_setup_required")?)
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
    pub fn project_work_epoch(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("project_work_epoch")?)
    }
    pub fn primary_milestone_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("primary_milestone_id")?)
    }
}

impl SqliteDb {
    pub async fn effective_project_identity_source(
        &self,
        project_id: &str,
    ) -> crate::Result<Option<EffectiveProjectIdentitySourceRow>> {
        let rows = sqlx::query(
            "SELECT id, name, settings, paused_at, charter_status, charter_setup_required,
                version, created_at, updated_at, project_work_epoch,
                primary_milestone_id
         FROM project WHERE id = ?",
        )
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| EffectiveProjectIdentitySourceRow { row }))
    }
}

pub async fn escalation_project_owner_on_connection(
    connection: &mut sqlx::SqliteConnection,
    project_id: &str,
) -> crate::Result<Option<Option<String>>> {
    Ok(
        sqlx::query_scalar::<_, Option<String>>("SELECT owner_id FROM project WHERE id=?")
            .bind(project_id)
            .fetch_optional(connection)
            .await?,
    )
}

impl SqliteDb {
    pub async fn environment_probe_project_settings_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
    ) -> crate::Result<String> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT settings FROM project WHERE id=?")
                .bind(project_id)
                .fetch_one(connection)
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn environment_provision_repo_project_settings(
        &self,
        repo_id: &str,
    ) -> crate::Result<String> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT p.settings FROM project p JOIN repo r ON r.project_id=p.id WHERE r.id=?",
        )
        .bind(repo_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn environment_wait_project_state_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
    ) -> crate::Result<(Option<String>, String)> {
        Ok(sqlx::query_as::<_, (Option<String>, String)>(
            "SELECT paused_at, settings FROM project WHERE id = ?",
        )
        .bind(project_id)
        .fetch_one(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn bootstrap_unowned_project_ids_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
    ) -> crate::Result<Vec<(String,)>> {
        Ok(
            sqlx::query_as::<_, (String,)>("SELECT id FROM project WHERE owner_id IS NULL")
                .fetch_all(connection)
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn bootstrap_unowned_project_owner_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        user_id: &str,
        project_id: &str,
    ) -> crate::Result<u64> {
        let result =
            sqlx::query("UPDATE project SET owner_id = ? WHERE id = ? AND owner_id IS NULL")
                .bind(user_id)
                .bind(project_id)
                .execute(connection)
                .await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn adaptive_task_current_charter_revision(
        &self,
        project_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT current_charter_revision_id FROM project
             WHERE id = ? AND charter_status = 'charter_backed'
               AND charter_setup_required = 0",
        )
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn repository_worker_project_charter_backed(
        &self,
        project_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS (
                 SELECT 1 FROM project
                 WHERE id = ? AND charter_status = 'charter_backed'
                   AND charter_setup_required = 0
             )",
        )
        .bind(project_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn workspace_assignment_project_charter_backed(
        &self,
        project_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS (
                 SELECT 1 FROM project
                 WHERE id = ? AND charter_status = 'charter_backed'
                   AND charter_setup_required = 0
             )",
        )
        .bind(project_id)
        .fetch_one(self.pool())
        .await?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct MainTurnPortfolioProjectRow {
    row: sqlx::sqlite::SqliteRow,
}
impl MainTurnPortfolioProjectRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("version")?)
    }
    pub fn updated_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("updated_at")?)
    }
    pub fn paused_at(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("paused_at")?)
    }
    pub fn charter_status(&self) -> crate::Result<String> {
        Ok(self.row.try_get("charter_status")?)
    }
    pub fn current_charter_revision_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("current_charter_revision_id")?)
    }
}

impl SqliteDb {
    pub async fn main_turn_portfolio_projects(
        &self,
        account_id: &str,
    ) -> crate::Result<Vec<MainTurnPortfolioProjectRow>> {
        let rows = sqlx::query(
            "SELECT id, version, updated_at, paused_at, charter_status,
                    current_charter_revision_id
             FROM project
             WHERE owner_id = ?
             ORDER BY updated_at DESC, id DESC
             LIMIT 20",
        )
        .bind(account_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| MainTurnPortfolioProjectRow { row })
            .collect())
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct NativeOwnedProjectSummaryRow {
    row: sqlx::sqlite::SqliteRow,
}
impl NativeOwnedProjectSummaryRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn name(&self) -> crate::Result<String> {
        Ok(self.row.try_get("name")?)
    }
    pub fn paused_at(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("paused_at")?)
    }
    pub fn task_count(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("task_count")?)
    }
    pub fn created_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("created_at")?)
    }
    pub fn updated_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("updated_at")?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct NativePortfolioProjectRow {
    row: sqlx::sqlite::SqliteRow,
}
impl NativePortfolioProjectRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn name(&self) -> crate::Result<String> {
        Ok(self.row.try_get("name")?)
    }
    pub fn paused_at(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("paused_at")?)
    }
    pub fn created_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("created_at")?)
    }
    pub fn updated_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("updated_at")?)
    }
}

impl SqliteDb {
    pub async fn native_paused_project_reason(
        &self,
        project_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT system_pause_reason FROM project WHERE id = ? AND paused_at IS NOT NULL",
        )
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn native_portfolio_projects(
        &self,
        account_id: &str,
        limit: i64,
    ) -> crate::Result<Vec<NativePortfolioProjectRow>> {
        let rows = sqlx::query(
            "SELECT id, name, paused_at, created_at, updated_at
             FROM project WHERE owner_id = ? ORDER BY updated_at DESC, id DESC LIMIT ?",
        )
        .bind(account_id)
        .bind(limit)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| NativePortfolioProjectRow { row })
            .collect())
    }
}

impl SqliteDb {
    pub async fn native_project_version(&self, project_id: &str) -> crate::Result<Option<i64>> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT version FROM project WHERE id = ?")
                .bind(project_id)
                .fetch_optional(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn native_owned_project_summary(
        &self,
        project_id: &str,
        account_id: &str,
    ) -> crate::Result<Option<NativeOwnedProjectSummaryRow>> {
        let rows = sqlx::query(
            "SELECT p.id, p.name, p.paused_at, p.created_at, p.updated_at,
                    COUNT(t.id) AS task_count
             FROM project AS p
             LEFT JOIN task AS t ON t.project_id = p.id AND t.deleted_at IS NULL
             WHERE p.id = ? AND p.owner_id = ?
             GROUP BY p.id",
        )
        .bind(project_id)
        .bind(account_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| NativeOwnedProjectSummaryRow { row }))
    }
}

impl SqliteDb {
    pub async fn native_project_pause_reason(
        &self,
        project_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT system_pause_reason FROM project WHERE id = ?",
        )
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn native_project_scope_summary(
        &self,
        id: &str,
    ) -> crate::Result<Option<NativeScopeSummaryRow>> {
        let rows = sqlx::query("SELECT id, name FROM project WHERE id = ?")
            .bind(id)
            .fetch_optional(self.pool())
            .await?;
        Ok(rows.map(|row| NativeScopeSummaryRow { row }))
    }
}

impl SqliteDb {
    pub async fn workspace_gc_root_project_exists(&self, project_id: &str) -> crate::Result<i64> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT EXISTS (SELECT 1 FROM project WHERE id = ?)")
                .bind(project_id)
                .fetch_one(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn provisioning_current_project_settings_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
    ) -> crate::Result<String> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT settings FROM project WHERE id=?")
                .bind(project_id)
                .fetch_one(connection)
                .await?,
        )
    }
}

pub async fn provisioning_project_settings_on_connection(
    connection: &mut sqlx::SqliteConnection,
    project_id: &str,
) -> crate::Result<Option<String>> {
    Ok(
        sqlx::query_scalar::<_, String>("SELECT settings FROM project WHERE id=?")
            .bind(project_id)
            .fetch_optional(connection)
            .await?,
    )
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct ValidationProjectCharterSourceRow {
    row: sqlx::sqlite::SqliteRow,
}
impl ValidationProjectCharterSourceRow {
    pub fn current_charter_revision_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("current_charter_revision_id")?)
    }
}

impl SqliteDb {
    pub async fn validation_project_charter_source(
        &self,
        project_id: &str,
    ) -> crate::Result<Option<ValidationProjectCharterSourceRow>> {
        let rows = sqlx::query(
            "SELECT p.current_charter_revision_id FROM project p WHERE p.id = ? LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| ValidationProjectCharterSourceRow { row }))
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct ChatUsageProjectScopeRow {
    row: sqlx::sqlite::SqliteRow,
}
impl ChatUsageProjectScopeRow {
    pub fn owner_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("owner_id")?)
    }
    pub fn name(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("name")?)
    }
}

impl SqliteDb {
    pub async fn chat_usage_project_scope(
        &self,
        project_id: &str,
    ) -> crate::Result<Option<ChatUsageProjectScopeRow>> {
        let rows = sqlx::query("SELECT owner_id, name FROM project WHERE id = ?")
            .bind(project_id)
            .fetch_optional(self.pool())
            .await?;
        Ok(rows.map(|row| ChatUsageProjectScopeRow { row }))
    }
}

impl SqliteDb {
    pub async fn milestone_command_project_version(
        &self,
        project_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT version FROM project WHERE id = ?")
                .bind(project_id)
                .fetch_optional(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn direct_project_command_project_id(
        &self,
        project_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT id FROM project WHERE id = ? LIMIT 1")
                .bind(project_id)
                .fetch_optional(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn project_action_target_project_id(
        &self,
        project_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT id FROM project WHERE id = ? LIMIT 1")
                .bind(project_id)
                .fetch_optional(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn project_action_project_owner(
        &self,
        project_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT owner_id FROM project WHERE id = ? LIMIT 1")
                .bind(project_id)
                .fetch_optional(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn placement_eligibility_project_version(
        &self,
        project_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar("SELECT version FROM project WHERE id=?")
            .bind(project_id)
            .fetch_one(self.pool())
            .await?)
    }
}

impl SqliteDb {
    pub async fn task_outcome_project_owner(
        &self,
        project_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(
            sqlx::query_scalar::<_, Option<String>>("SELECT owner_id FROM project WHERE id = ?")
                .bind(project_id)
                .fetch_optional(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn task_proposal_actor_permission_ceiling(
        &self,
        project_id: &str,
        identity_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar(
            "SELECT permission_ceiling_json FROM project_agent_binding
             WHERE project_id = ? AND identity_id = ? AND state = 'active' LIMIT 1",
        )
        .bind(project_id)
        .bind(identity_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn task_proposal_project_owner(
        &self,
        project_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT owner_id FROM project WHERE id = ? LIMIT 1")
                .bind(project_id)
                .fetch_optional(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn task_proposal_current_charter_revision(
        &self,
        project_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar(
            "SELECT current_charter_revision_id FROM project
             WHERE id = ? AND charter_status = 'charter_backed'
               AND charter_setup_required = 0 LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn task_proposal_primary_milestone(
        &self,
        project_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(
            sqlx::query_scalar("SELECT primary_milestone_id FROM project WHERE id = ?")
                .bind(project_id)
                .fetch_optional(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn charter_command_active_agent_binding(
        &self,
        project_id: &str,
        identity_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar(
                    "SELECT identity_id FROM project_agent_binding WHERE project_id = ? AND identity_id = ? AND state = 'active' LIMIT 1",
                ).bind(project_id).bind(identity_id).fetch_optional(self.pool()).await?)
    }
}

pub struct FrozenProjectBindingPolicyRow {
    row: SqliteRow,
}
impl FrozenProjectBindingPolicyRow {
    pub fn policy_revision(&self) -> crate::Result<String> {
        Ok(self.row.try_get("policy_revision")?)
    }
    pub fn policy_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("policy_digest")?)
    }
}

impl SqliteDb {
    pub async fn frozen_project_agent_binding_policy(
        &self,
        binding_id: &str,
        project_id: &str,
    ) -> crate::Result<FrozenProjectBindingPolicyRow> {
        let rows = sqlx::query(
            "SELECT policy_revision, policy_digest
                     FROM project_agent_binding WHERE id = ? AND project_id = ?",
        )
        .bind(binding_id)
        .bind(project_id)
        .fetch_one(self.pool())
        .await?;
        Ok(FrozenProjectBindingPolicyRow { row: rows })
    }
}

impl SqliteDb {
    pub async fn orchestration_bound_project_target(
        &self,
        identity_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT p.id
                     FROM project AS p
                     JOIN project_agent_binding AS binding
                       ON binding.project_id = p.id
                      AND binding.identity_id = ?
                      AND binding.state = 'active'
                     WHERE p.id = ?
                     LIMIT 1",
        )
        .bind(identity_id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn integration_task_project_paused(&self, task_id: &str) -> crate::Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM task t JOIN project p ON p.id=t.project_id WHERE t.id=? AND p.paused_at IS NOT NULL)",
        ).bind(task_id).fetch_one(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn task_creation_project_version_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar("SELECT version FROM project WHERE id=?")
            .bind(project_id)
            .fetch_one(connection)
            .await?)
    }
}

pub async fn execution_admission_project_owner_in_tx(
    connection: &mut sqlx::SqliteConnection,
    project_id: &str,
) -> crate::Result<Option<Option<String>>> {
    Ok(
        sqlx::query_scalar("SELECT owner_id FROM project WHERE id = ?")
            .bind(project_id)
            .fetch_optional(connection)
            .await?,
    )
}

impl SqliteDb {
    pub async fn review_ci_project_authority_valid_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
        version: i64,
        workflow_definition: Option<&str>,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM project WHERE id = ? AND version = ? AND workflow_definition IS ?)").bind(project_id).bind(version).bind(workflow_definition).fetch_one(connection).await?)
    }
}

impl SqliteDb {
    pub async fn project_agent_workspace_project_live_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT 1 FROM project WHERE id = ? LIMIT 1")
                .bind(project_id)
                .fetch_optional(connection)
                .await?,
        )
    }
}

#[derive(Debug)]
pub struct ProjectAgentWorkspaceAuthoritySourceRow {
    pub project_id: String,
    pub project_created_at: String,
    pub primary_repo_id: Option<String>,
    pub provisioning_operation_id: Option<String>,
    pub repository_id: Option<String>,
    pub remote_url: Option<String>,
    pub local_path: Option<String>,
    pub default_branch: Option<String>,
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for ProjectAgentWorkspaceAuthoritySourceRow {
    fn from_row(row: &'r SqliteRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            project_id: row.try_get(0)?,
            project_created_at: row.try_get(1)?,
            primary_repo_id: row.try_get(2)?,
            provisioning_operation_id: row.try_get(3)?,
            repository_id: row.try_get(4)?,
            remote_url: row.try_get(5)?,
            local_path: row.try_get(6)?,
            default_branch: row.try_get(7)?,
        })
    }
}

pub async fn project_agent_workspace_authority_source<'e>(
    executor: impl sqlx::Executor<'e, Database = sqlx::Sqlite>,
    project_id: &str,
) -> crate::Result<Option<ProjectAgentWorkspaceAuthoritySourceRow>> {
    Ok(
        sqlx::query_as::<_, ProjectAgentWorkspaceAuthoritySourceRow>(
            "SELECT p.id, p.created_at, p.primary_repo_id,
                operation.id,
                repo.id, CASE WHEN trim(repo.remote_url) = '' THEN NULL ELSE repo.remote_url END,
                repo.local_path, repo.default_branch
         FROM project p
         LEFT JOIN project_provisioning_operation operation
           ON operation.project_id = p.id
         LEFT JOIN repo
           ON repo.id = p.primary_repo_id AND repo.project_id = p.id
         WHERE p.id = ?",
        )
        .bind(project_id)
        .fetch_optional(executor)
        .await?,
    )
}

impl SqliteDb {
    pub async fn project_hook_current_version_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(
            sqlx::query_scalar("SELECT version FROM project WHERE id = ?")
                .bind(project_id)
                .fetch_optional(connection)
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn integration_gate_project_paused(&self, project_id: &str) -> crate::Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM project WHERE id=? AND paused_at IS NOT NULL)",
        )
        .bind(project_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn notification_project_exists(&self, project_id: &str) -> crate::Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM project WHERE id = ?)")
                .bind(project_id)
                .fetch_one(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn workflow_transition_project_authority_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
    ) -> crate::Result<Option<WorkflowProjectAuthorityRow>> {
        Ok(
            sqlx::query("SELECT version, workflow_definition FROM project WHERE id = ?")
                .bind(project_id)
                .fetch_optional(connection)
                .await?
                .map(|row| WorkflowProjectAuthorityRow { row }),
        )
    }
}
