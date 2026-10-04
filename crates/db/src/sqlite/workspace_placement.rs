use super::{check_error, parse_enum, SqliteDb};
use crate::{
    CreateWorkspacePlacement, DbError, Execution, ExecutionLeaseMutation, PlacementFailureCause,
    PlacementOwnerKind, PlacementState, RenewExecutionLease, Result, UpdateWorkspacePlacement,
    WorkspaceLease, WorkspaceLeaseRepo, WorkspacePlacement, WorkspacePlacementRepo,
};
use async_trait::async_trait;
use sqlx::{sqlite::SqliteRow, Row, Sqlite, Transaction};

#[async_trait]
impl WorkspacePlacementRepo for SqliteDb {
    async fn create(&self, input: CreateWorkspacePlacement) -> Result<WorkspacePlacement> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let placement = WorkspacePlacementRepo::create_in_tx(self, &mut transaction, input).await?;
        transaction.commit().await?;
        Ok(placement)
    }

    async fn create_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateWorkspacePlacement,
    ) -> Result<WorkspacePlacement> {
        let row = sqlx::query(
            "INSERT INTO workspace_placement (
                id, workspace_id, task_id, agent_id, owner_kind, daemon_id, runtime_id,
                repo_location_id, execution_daemon_id, workspace_handle, generation,
                state, selected_by, selection_reason, reserved_until, disconnected_at,
                failure_cause, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING *",
        )
        .bind(&input.id)
        .bind(&input.workspace_id)
        .bind(&input.task_id)
        .bind(input.agent_id.as_deref())
        .bind(input.owner_kind.to_string())
        .bind(input.daemon_id.as_deref())
        .bind(input.runtime_id.as_deref())
        .bind(&input.repo_location_id)
        .bind(input.execution_daemon_id.as_deref())
        .bind(input.workspace_handle.as_deref())
        .bind(input.generation)
        .bind(input.state.to_string())
        .bind(input.selected_by.to_string())
        .bind(&input.selection_reason)
        .bind(input.reserved_until.as_deref())
        .bind(input.disconnected_at.as_deref())
        .bind(input.failure_cause.as_ref().map(ToString::to_string))
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .fetch_one(&mut **transaction)
        .await
        .map_err(check_error)?;
        map_workspace_placement(row)
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<WorkspacePlacement>> {
        sqlx::query("SELECT * FROM workspace_placement WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_workspace_placement)
            .transpose()
    }

    async fn get_by_id_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        id: &str,
    ) -> Result<Option<WorkspacePlacement>> {
        sqlx::query("SELECT * FROM workspace_placement WHERE id = ?")
            .bind(id)
            .fetch_optional(&mut **transaction)
            .await?
            .map(map_workspace_placement)
            .transpose()
    }

    async fn get_by_workspace_id(&self, workspace_id: &str) -> Result<Option<WorkspacePlacement>> {
        sqlx::query("SELECT * FROM workspace_placement WHERE workspace_id = ?")
            .bind(workspace_id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_workspace_placement)
            .transpose()
    }

    async fn get_by_workspace_id_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        workspace_id: &str,
    ) -> Result<Option<WorkspacePlacement>> {
        sqlx::query("SELECT * FROM workspace_placement WHERE workspace_id = ?")
            .bind(workspace_id)
            .fetch_optional(&mut **transaction)
            .await?
            .map(map_workspace_placement)
            .transpose()
    }

    async fn update(&self, input: UpdateWorkspacePlacement) -> Result<WorkspacePlacement> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let placement = WorkspacePlacementRepo::update_in_tx(self, &mut transaction, input).await?;
        transaction.commit().await?;
        Ok(placement)
    }

    async fn update_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: UpdateWorkspacePlacement,
    ) -> Result<WorkspacePlacement> {
        let mut query = sqlx::QueryBuilder::<Sqlite>::new(
            "UPDATE workspace_placement SET version = version + 1, updated_at = ",
        );
        query.push_bind(&input.updated_at);
        if let Some(agent_id) = input.agent_id {
            query.push(", agent_id = ").push_bind(agent_id);
        }
        if let Some(owner_kind) = input.owner_kind {
            query
                .push(", owner_kind = ")
                .push_bind(owner_kind.to_string());
        }
        if let Some(daemon_id) = input.daemon_id {
            query.push(", daemon_id = ").push_bind(daemon_id);
        }
        if let Some(runtime_id) = input.runtime_id {
            query.push(", runtime_id = ").push_bind(runtime_id);
        }
        if let Some(repo_location_id) = input.repo_location_id {
            query
                .push(", repo_location_id = ")
                .push_bind(repo_location_id);
        }
        if let Some(execution_daemon_id) = input.execution_daemon_id {
            query
                .push(", execution_daemon_id = ")
                .push_bind(execution_daemon_id);
        }
        if let Some(workspace_handle) = input.workspace_handle {
            query
                .push(", workspace_handle = ")
                .push_bind(workspace_handle);
        }
        if let Some(generation) = input.generation {
            query.push(", generation = ").push_bind(generation);
        }
        if let Some(state) = input.state {
            query.push(", state = ").push_bind(state.to_string());
        }
        if let Some(selected_by) = input.selected_by {
            query
                .push(", selected_by = ")
                .push_bind(selected_by.to_string());
        }
        if let Some(selection_reason) = input.selection_reason {
            query
                .push(", selection_reason = ")
                .push_bind(selection_reason);
        }
        if let Some(reserved_until) = input.reserved_until {
            query.push(", reserved_until = ").push_bind(reserved_until);
        }
        if let Some(disconnected_at) = input.disconnected_at {
            query
                .push(", disconnected_at = ")
                .push_bind(disconnected_at);
        }
        if let Some(failure_cause) = input.failure_cause {
            query
                .push(", failure_cause = ")
                .push_bind(failure_cause.map(|cause| cause.to_string()));
        }
        query
            .push(" WHERE id = ")
            .push_bind(&input.id)
            .push(" AND version = ")
            .push_bind(input.expected_version)
            .push(" RETURNING *");
        let row = query
            .build()
            .fetch_optional(&mut **transaction)
            .await
            .map_err(check_error)?
            .ok_or(DbError::VersionConflict)?;
        map_workspace_placement(row)
    }

    async fn list_by_daemon_and_state(
        &self,
        daemon_id: &str,
        state: PlacementState,
    ) -> Result<Vec<WorkspacePlacement>> {
        let rows = sqlx::query(
            "SELECT * FROM workspace_placement WHERE daemon_id = ? AND state = ?
             ORDER BY created_at ASC, id ASC",
        )
        .bind(daemon_id)
        .bind(state.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(map_workspace_placement).collect()
    }

    async fn list_by_state(&self, state: PlacementState) -> Result<Vec<WorkspacePlacement>> {
        let rows = sqlx::query(
            "SELECT * FROM workspace_placement WHERE state = ? ORDER BY created_at ASC, id ASC",
        )
        .bind(state.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(map_workspace_placement).collect()
    }

    async fn get_for_task(&self, task_id: &str) -> Result<Option<WorkspacePlacement>> {
        sqlx::query(
            "SELECT p.* FROM workspace_placement p
             JOIN workspace w ON w.id = p.workspace_id
             JOIN task t ON t.id = ?
             WHERE w.task_id = COALESCE(t.parent_task_id, t.id)
             ORDER BY p.created_at DESC, p.id DESC LIMIT 1",
        )
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await?
        .map(map_workspace_placement)
        .transpose()
    }

    async fn suspend_expired_execution_lease(
        &self,
        placement: &WorkspacePlacement,
        execution: &Execution,
        now: &str,
    ) -> Result<Option<WorkspacePlacement>> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let row = sqlx::query(
            "UPDATE workspace_placement SET state = 'disconnected', disconnected_at = ?,
                 failure_cause = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND generation = ? AND state = 'ready'
               AND (owner_kind = 'daemon' OR execution_daemon_id IS NOT NULL)
               AND EXISTS (SELECT 1 FROM execution e
                   WHERE e.id = ? AND e.workspace_id = workspace_placement.workspace_id
                     AND e.execution_version = ? AND e.lease_owner IS ? AND e.status = 'running'
                     AND (e.lease_expires_at IS NULL OR e.lease_expires_at <= ?)
                     AND (e.hard_deadline_at IS NULL OR e.hard_deadline_at > ?))
             RETURNING *",
        )
        .bind(now)
        .bind(PlacementFailureCause::OwnerDisconnected.to_string())
        .bind(now)
        .bind(&placement.id)
        .bind(placement.version)
        .bind(placement.generation)
        .bind(&execution.id)
        .bind(execution.execution_version)
        .bind(execution.lease_owner.as_deref())
        .bind(now)
        .bind(now)
        .fetch_optional(&mut *transaction)
        .await?
        .map(map_workspace_placement)
        .transpose()?;
        transaction.commit().await?;
        Ok(row)
    }

    async fn resume_disconnected_execution_lease(
        &self,
        placement: &WorkspacePlacement,
        execution: &Execution,
        input: RenewExecutionLease,
    ) -> Result<ExecutionLeaseMutation> {
        let daemon_id = match placement.owner_kind {
            PlacementOwnerKind::Daemon => placement.daemon_id.as_deref(),
            PlacementOwnerKind::Server => placement.execution_daemon_id.as_deref(),
        }
        .ok_or_else(|| DbError::Check("suspended execution has no daemon owner".to_owned()))?;
        let owner_prefix = format!("daemon:{daemon_id}:connection:");
        if placement.state != PlacementState::Disconnected
            || execution.workspace_id.as_deref() != Some(placement.workspace_id.as_str())
            || input.execution_id != execution.id
            || input.expected_version != execution.execution_version
            || !input.owner.starts_with(&owner_prefix)
            || execution
                .lease_owner
                .as_deref()
                .is_none_or(|owner| !owner.starts_with(&owner_prefix))
            || input.lease_expires_at <= input.now
        {
            return Err(DbError::Check(
                "invalid suspended execution owner claim".to_owned(),
            ));
        }
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let result = sqlx::query(
            "UPDATE execution SET lease_owner = ?,
                 lease_expires_at = MIN(?, COALESCE(hard_deadline_at, ?)), last_heartbeat_at = ?,
                 execution_version = execution_version + 1, updated_at = ?
             WHERE id = ? AND execution_version = ? AND lease_owner = ?
               AND status = 'running' AND (hard_deadline_at IS NULL OR hard_deadline_at > ?)
               AND EXISTS (SELECT 1 FROM workspace_placement p
                   WHERE p.id = ? AND p.workspace_id = execution.workspace_id
                     AND p.version = ? AND p.generation = ?
                     AND CASE p.owner_kind WHEN 'daemon' THEN p.daemon_id
                         ELSE p.execution_daemon_id END = ? AND p.state = 'disconnected')",
        )
        .bind(&input.owner)
        .bind(&input.lease_expires_at)
        .bind(&input.lease_expires_at)
        .bind(&input.now)
        .bind(&input.now)
        .bind(&execution.id)
        .bind(execution.execution_version)
        .bind(execution.lease_owner.as_deref())
        .bind(&input.now)
        .bind(&placement.id)
        .bind(placement.version)
        .bind(placement.generation)
        .bind(daemon_id)
        .execute(&mut *transaction)
        .await?;
        let current = sqlx::query("SELECT * FROM execution WHERE id = ?")
            .bind(&execution.id)
            .fetch_optional(&mut *transaction)
            .await?
            .map(super::map_execution)
            .transpose()?;
        if result.rows_affected() == 0 {
            transaction.rollback().await?;
            return Ok(
                if current.as_ref().is_some_and(|current| {
                    current
                        .hard_deadline_at
                        .as_deref()
                        .is_some_and(|deadline| deadline <= input.now.as_str())
                }) {
                    ExecutionLeaseMutation::HardDeadline { current }
                } else {
                    ExecutionLeaseMutation::Concurrent { current }
                },
            );
        }
        let resumed = current.ok_or(DbError::NotFound)?;
        let resumed_expiry = resumed
            .lease_expires_at
            .as_deref()
            .ok_or(DbError::NotFound)?;
        // The scheduler grant is suspended too. Extend only the exact active
        // grant, never a successor Task lease or a revoked authority grant.
        let grants = sqlx::query(
            "SELECT id, version FROM workspace_lease
            WHERE execution_id = ? AND status = 'active' AND expires_at < ?",
        )
        .bind(&execution.id)
        .bind(resumed_expiry)
        .fetch_all(&mut *transaction)
        .await?;
        for grant in grants {
            sqlx::query(
                "UPDATE workspace_lease SET expires_at = ?, version = version + 1, updated_at = ?
                WHERE id = ? AND version = ? AND status = 'active'",
            )
            .bind(resumed_expiry)
            .bind(&input.now)
            .bind(grant.try_get::<String, _>("id")?)
            .bind(grant.try_get::<i64, _>("version")?)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(ExecutionLeaseMutation::Updated(resumed))
    }

    async fn expire_unsuspended_workspace_leases(
        &self,
        now: &str,
        limit: i64,
    ) -> Result<Vec<WorkspaceLease>> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let rows = sqlx::query(
            "SELECT wl.id, wl.version FROM workspace_lease wl
             WHERE wl.status = 'active' AND wl.expires_at <= ?
               AND NOT EXISTS (SELECT 1 FROM execution e JOIN workspace_placement p
                   ON p.workspace_id = e.workspace_id
                   WHERE e.id = wl.execution_id AND e.status = 'running'
                     AND p.state = 'disconnected' AND (e.hard_deadline_at IS NULL OR e.hard_deadline_at > ?))
             ORDER BY wl.expires_at, wl.id LIMIT ?",
        )
        .bind(now)
        .bind(now)
        .bind(limit.clamp(1, 500))
        .fetch_all(&mut *transaction)
        .await?;
        let mut ids = Vec::new();
        for row in rows {
            let id: String = row.try_get("id")?;
            let version: i64 = row.try_get("version")?;
            let updated = sqlx::query(
                "UPDATE workspace_lease
                SET status = 'expired', revoked_at = ?, version = version + 1, updated_at = ?
                WHERE id = ? AND version = ? AND status = 'active' AND expires_at <= ?",
            )
            .bind(now)
            .bind(now)
            .bind(&id)
            .bind(version)
            .bind(now)
            .execute(&mut *transaction)
            .await?;
            if updated.rows_affected() == 1 {
                ids.push(id);
            }
        }
        transaction.commit().await?;
        let mut expired = Vec::new();
        for id in ids {
            if let Some(lease) = WorkspaceLeaseRepo::get_by_id(self, &id).await? {
                expired.push(lease);
            }
        }
        Ok(expired)
    }
}

pub(super) fn map_workspace_placement(row: SqliteRow) -> Result<WorkspacePlacement> {
    Ok(WorkspacePlacement {
        id: row.try_get("id")?,
        workspace_id: row.try_get("workspace_id")?,
        task_id: row.try_get("task_id")?,
        agent_id: row.try_get("agent_id")?,
        owner_kind: parse_enum(row.try_get::<String, _>("owner_kind")?)?,
        daemon_id: row.try_get("daemon_id")?,
        runtime_id: row.try_get("runtime_id")?,
        repo_location_id: row.try_get("repo_location_id")?,
        execution_daemon_id: row.try_get("execution_daemon_id")?,
        workspace_handle: row.try_get("workspace_handle")?,
        generation: row.try_get("generation")?,
        state: parse_enum(row.try_get::<String, _>("state")?)?,
        selected_by: parse_enum(row.try_get::<String, _>("selected_by")?)?,
        selection_reason: row.try_get("selection_reason")?,
        reserved_until: row.try_get("reserved_until")?,
        disconnected_at: row.try_get("disconnected_at")?,
        failure_cause: row
            .try_get::<Option<String>, _>("failure_cause")?
            .map(parse_enum)
            .transpose()?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}
