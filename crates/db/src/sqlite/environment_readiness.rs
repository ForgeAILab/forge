use crate::{
    DbError, EnvironmentMachine, EnvironmentReadinessStatus, ProjectMachineReadiness,
    ProjectMachineReadinessRepo, Result, SqliteDb,
};
use async_trait::async_trait;
use sqlx::{sqlite::SqliteRow, Row};

pub(crate) fn map_readiness(row: SqliteRow) -> Result<ProjectMachineReadiness> {
    let kind: String = row.try_get("owner_kind")?;
    let machine = if kind == "server" {
        EnvironmentMachine::Server
    } else {
        EnvironmentMachine::Daemon {
            daemon_id: row.try_get("daemon_id")?,
            runtime_id: row.try_get("runtime_id")?,
        }
    };
    let status: String = row.try_get("status")?;
    Ok(ProjectMachineReadiness {
        project_id: row.try_get("project_id")?,
        machine,
        status: match status.as_str() {
            "ready" => EnvironmentReadinessStatus::Ready,
            "not_ready" => EnvironmentReadinessStatus::NotReady,
            _ => EnvironmentReadinessStatus::Unknown,
        },
        checks_digest: row.try_get("checks_digest")?,
        failing_checks: serde_json::from_str(&row.try_get::<String, _>("failing_checks_json")?)
            .map_err(|error| DbError::Check(error.to_string()))?,
        scope_covered: row.try_get("scope_covered")?,
        role: row.try_get("role")?,
        workspace_id: row.try_get("workspace_id")?,
        checked_at: row.try_get("checked_at")?,
        next_check_at: row.try_get("next_check_at")?,
        version: row.try_get("version")?,
    })
}

#[async_trait]
impl ProjectMachineReadinessRepo for SqliteDb {
    async fn list_readiness_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        project_id: &str,
    ) -> Result<Vec<ProjectMachineReadiness>> {
        sqlx::query("SELECT * FROM project_machine_readiness WHERE project_id = ?")
            .bind(project_id)
            .fetch_all(&mut **tx)
            .await?
            .into_iter()
            .map(map_readiness)
            .collect()
    }
    async fn get_readiness(
        &self,
        project_id: &str,
        machine: &EnvironmentMachine,
    ) -> Result<Option<ProjectMachineReadiness>> {
        let (kind, daemon, runtime) = machine.columns();
        sqlx::query("SELECT * FROM project_machine_readiness WHERE project_id = ? AND owner_kind = ? AND daemon_id = ? AND runtime_id = ?")
            .bind(project_id).bind(kind).bind(daemon).bind(runtime).fetch_optional(self.pool()).await?
            .map(map_readiness).transpose()
    }
    async fn list_readiness(&self, project_id: &str) -> Result<Vec<ProjectMachineReadiness>> {
        sqlx::query("SELECT * FROM project_machine_readiness WHERE project_id = ? ORDER BY owner_kind, daemon_id, runtime_id")
            .bind(project_id).fetch_all(self.pool()).await?.into_iter().map(map_readiness).collect()
    }
    async fn put_readiness(
        &self,
        row: ProjectMachineReadiness,
        expected_version: Option<i64>,
    ) -> Result<ProjectMachineReadiness> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let settings: String = sqlx::query_scalar("SELECT settings FROM project WHERE id = ?")
            .bind(&row.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::NotFound)?;
        let environment = crate::environment_readiness::settings_environment(&settings)?;
        if environment.checks.is_empty()
            || crate::environment_checks_digest(&environment) != row.checks_digest
        {
            return Err(DbError::VersionConflict);
        }
        let (kind, daemon, runtime) = row.machine.columns();
        let status = match row.status {
            EnvironmentReadinessStatus::Ready => "ready",
            EnvironmentReadinessStatus::NotReady => "not_ready",
            EnvironmentReadinessStatus::Unknown => "unknown",
        };
        let failures = serde_json::to_string(&row.failing_checks)
            .map_err(|error| DbError::Check(error.to_string()))?;
        let updated = if let Some(version) = expected_version {
            sqlx::query("UPDATE project_machine_readiness SET status = ?, checks_digest = ?, failing_checks_json = ?, scope_covered = ?, role = ?, workspace_id = ?, checked_at = ?, next_check_at = ?, version = version + 1 WHERE project_id = ? AND owner_kind = ? AND daemon_id = ? AND runtime_id = ? AND version = ? RETURNING *")
                .bind(status).bind(&row.checks_digest).bind(&failures).bind(&row.scope_covered).bind(&row.role).bind(&row.workspace_id).bind(&row.checked_at).bind(&row.next_check_at)
                .bind(&row.project_id).bind(kind).bind(daemon).bind(runtime).bind(version).fetch_optional(&mut *tx).await?
        } else {
            sqlx::query("INSERT INTO project_machine_readiness (project_id, owner_kind, daemon_id, runtime_id, status, checks_digest, failing_checks_json, scope_covered, role, workspace_id, checked_at, next_check_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING RETURNING *")
                .bind(&row.project_id).bind(kind).bind(daemon).bind(runtime).bind(status).bind(&row.checks_digest).bind(&failures).bind(&row.scope_covered).bind(&row.role).bind(&row.workspace_id).bind(&row.checked_at).bind(&row.next_check_at).fetch_optional(&mut *tx).await?
        }.ok_or(DbError::VersionConflict)?;
        let saved = map_readiness(updated)?;
        if saved.status != EnvironmentReadinessStatus::Unknown {
            // Readiness wakes do not spend another Task version on a queued
            // probe deferral. Authority edits retain the existing version fence.
            sqlx::query("UPDATE task SET metadata_json = json_remove(metadata_json, '$.deferred_dispatch') WHERE project_id = ? AND json_valid(metadata_json) AND json_extract(metadata_json, '$.deferred_dispatch.reason') LIKE '%environment_%'")
                .bind(&saved.project_id).execute(&mut *tx).await?;
        }
        if saved.status == EnvironmentReadinessStatus::Ready {
            let machine = serde_json::to_value(&saved.machine)
                .map_err(|error| DbError::Check(error.to_string()))?
                .to_string();
            sqlx::query("UPDATE attention_projection SET status = 'resolved', resolved_at = ?, updated_at = ?, version = version + 1 WHERE status <> 'resolved' AND dedupe_key IN (SELECT 'task-environment-wait:' || id FROM task WHERE project_id = ? AND json_valid(metadata_json) AND json_extract(metadata_json, '$.environment_wait.machine') = json(?))")
                .bind(crate::now_rfc3339()).bind(crate::now_rfc3339()).bind(&saved.project_id).bind(&machine).execute(&mut *tx).await?;
            sqlx::query("UPDATE task SET metadata_json = json_remove(metadata_json, '$.environment_wait', '$.deferred_dispatch') WHERE project_id = ? AND json_valid(metadata_json) AND json_extract(metadata_json, '$.environment_wait.machine') = json(?)")
                .bind(&saved.project_id).bind(&machine).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(saved)
    }
}

pub(crate) async fn fill_migrated_digests(pool: &sqlx::SqlitePool) -> Result<()> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name = 'project_machine_readiness')",
    )
    .fetch_one(pool)
    .await?;
    if !exists {
        return Ok(());
    }
    let rows: Vec<(String, String)> = sqlx::query_as("SELECT DISTINCT p.id, p.settings FROM project p JOIN project_machine_readiness r ON r.project_id = p.id WHERE r.checks_digest = ''").fetch_all(pool).await?;
    for (id, settings) in rows {
        let digest = crate::environment_checks_digest(
            &crate::environment_readiness::settings_environment(&settings)?,
        );
        sqlx::query("UPDATE project_machine_readiness SET checks_digest = ? WHERE project_id = ? AND checks_digest = ''").bind(digest).bind(id).execute(pool).await?;
    }
    Ok(())
}
