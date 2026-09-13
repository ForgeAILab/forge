use super::*;

fn workspace_lease_write_error(error: sqlx::Error) -> DbError {
    if let sqlx::Error::Database(database_error) = &error {
        let message = database_error.message().to_ascii_lowercase();
        if message.contains("operation_idempotency_key") {
            return DbError::IdempotencyConflict;
        }
        if message.contains("unique constraint") || message.contains("constraint failed") {
            return DbError::VersionConflict;
        }
    }
    check_error(error)
}

fn is_stale_workspace_lease_renewal(error: &sqlx::Error) -> bool {
    let sqlx::Error::Database(database_error) = error else {
        return false;
    };
    matches!(
        database_error.message(),
        "Workspace lease renewal authority is stale"
            | "Orchestration agents cannot receive Workspace leases"
    )
}

fn map_workspace_lease(row: SqliteRow) -> Result<WorkspaceLease> {
    Ok(WorkspaceLease {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        task_id: row.try_get("task_id")?,
        task_version: row.try_get("task_version")?,
        execution_id: row.try_get("execution_id")?,
        operation_idempotency_key: row.try_get("operation_idempotency_key")?,
        repository_binding_id: row.try_get("repository_binding_id")?,
        base_ref: row.try_get("base_ref")?,
        role: row.try_get("role")?,
        capabilities_json: row.try_get("capabilities_json")?,
        assigned_principal_type: row.try_get("assigned_principal_type")?,
        assigned_principal_id: row.try_get("assigned_principal_id")?,
        capability_profile_revision: row.try_get("capability_profile_revision")?,
        capability_profile_digest: row.try_get("capability_profile_digest")?,
        issuing_principal_type: row.try_get("issuing_principal_type")?,
        issuing_principal_id: row.try_get("issuing_principal_id")?,
        status: row.try_get("status")?,
        issued_at: row.try_get("issued_at")?,
        expires_at: row.try_get("expires_at")?,
        revoked_at: row.try_get("revoked_at")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

const WORKSPACE_LEASE_COLUMNS: &str = "id, project_id, task_id, task_version, execution_id, operation_idempotency_key, repository_binding_id, base_ref, role, capabilities_json, assigned_principal_type, assigned_principal_id, capability_profile_revision, capability_profile_digest, issuing_principal_type, issuing_principal_id, status, issued_at, expires_at, revoked_at, version, created_at, updated_at";

impl SqliteDb {
    async fn issue_inner(
        &self,
        input: CreateWorkspaceLease,
        expected_project_version: Option<i64>,
        expected_workflow_definition: Option<&str>,
        expected_task_status: Option<&str>,
        expected_effective_role: Option<&str>,
        replacing_lease: Option<(&str, i64)>,
    ) -> Result<WorkspaceLease> {
        let mut tx = crate::begin_immediate(&self.pool).await?;
        let mut current_workflow_definition = None;
        if let Some(expected_project_version) = expected_project_version {
            let row = sqlx::query(
                "SELECT version, workflow_definition
                 FROM project WHERE id = ?",
            )
            .bind(&input.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::NotFound)?;
            let actual_project_version: i64 = row.try_get("version")?;
            let actual_workflow_definition: String = row.try_get("workflow_definition")?;
            if actual_project_version != expected_project_version
                || expected_workflow_definition != Some(actual_workflow_definition.as_str())
            {
                return Err(DbError::VersionConflict);
            }
            current_workflow_definition = Some(actual_workflow_definition);
        } else if expected_workflow_definition.is_some() {
            return Err(DbError::VersionConflict);
        }
        match (expected_task_status, expected_effective_role) {
            (Some(expected_task_status), Some(expected_effective_role)) => {
                let task = sqlx::query(
                    "SELECT version, status, parent_task_id
                     FROM task
                     WHERE id = ? AND project_id = ? AND deleted_at IS NULL",
                )
                .bind(&input.task_id)
                .bind(&input.project_id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(DbError::NotFound)?;
                let task_version: i64 = task.try_get("version")?;
                let task_status: String = task.try_get("status")?;
                if task_version != input.task_version || task_status != expected_task_status {
                    return Err(DbError::VersionConflict);
                }
                let parent_task_id: Option<String> = task.try_get("parent_task_id")?;
                let workflow_is_inherited =
                    parent_task_id.is_some() && inherited_subtask_workflow_state(&task_status);
                let workflow_definition = current_workflow_definition
                    .as_deref()
                    .ok_or(DbError::VersionConflict)?;
                let raw = workflow_definition.trim();
                let parsed_workflow = if raw.is_empty() || raw == "{}" {
                    None
                } else {
                    Some(
                        serde_json::from_str::<api_types::WorkflowDefinition>(raw)
                            .map_err(|_| DbError::VersionConflict)?,
                    )
                };
                let actual_role = if workflow_is_inherited {
                    inherited_subtask_workflow_role(&task_status).map(str::to_owned)
                } else if let Some(workflow) = parsed_workflow.as_ref() {
                    workflow
                        .states
                        .iter()
                        .find(|state| state.name == task_status)
                        .and_then(|state| {
                            state.role.as_deref().or_else(|| {
                                (state.kind == api_types::StateKind::Active).then_some("assignee")
                            })
                        })
                        .map(str::to_owned)
                } else {
                    default_workflow_role(&task_status).map(str::to_owned)
                };
                if canonical_execution_role(actual_role.as_deref())
                    != canonical_execution_role(Some(expected_effective_role))
                {
                    return Err(DbError::VersionConflict);
                }
            }
            (None, None) => {}
            _ => return Err(DbError::VersionConflict),
        }
        if let Some(row) = sqlx::query(&format!(
            "SELECT {WORKSPACE_LEASE_COLUMNS} FROM workspace_lease WHERE id = ?"
        ))
        .bind(&input.id)
        .fetch_optional(&mut *tx)
        .await?
        {
            let existing = map_workspace_lease(row)?;
            let same = existing.project_id == input.project_id
                && existing.task_id == input.task_id
                && existing.task_version == input.task_version
                && existing.execution_id == input.execution_id
                && existing.operation_idempotency_key == input.operation_idempotency_key
                && existing.repository_binding_id == input.repository_binding_id
                && existing.base_ref == input.base_ref
                && existing.role == input.role
                && existing.capabilities_json == input.capabilities_json
                && existing.assigned_principal_type == input.assigned_principal_type
                && existing.assigned_principal_id == input.assigned_principal_id
                && existing.capability_profile_revision == input.capability_profile_revision
                && existing.capability_profile_digest == input.capability_profile_digest
                && existing.issuing_principal_type == input.issuing_principal_type
                && existing.issuing_principal_id == input.issuing_principal_id
                && existing.issued_at == input.issued_at
                && existing.expires_at == input.expires_at;
            if !same {
                return Err(DbError::IdempotencyConflict);
            }
            tx.commit().await?;
            return Ok(existing);
        }
        // The operation key is the scheduler's durable replay identity.  A
        // retry may allocate a different transport row ID, but it must
        // return the original lease instead of falling through to SQLite's
        // generic UNIQUE error (or silently minting a second grant).
        if let Some(row) = sqlx::query(&format!(
            "SELECT {WORKSPACE_LEASE_COLUMNS} FROM workspace_lease
             WHERE operation_idempotency_key = ?"
        ))
        .bind(&input.operation_idempotency_key)
        .fetch_optional(&mut *tx)
        .await?
        {
            let existing = map_workspace_lease(row)?;
            let same = existing.project_id == input.project_id
                && existing.task_id == input.task_id
                && existing.task_version == input.task_version
                && existing.execution_id == input.execution_id
                && existing.repository_binding_id == input.repository_binding_id
                && existing.base_ref == input.base_ref
                && existing.role == input.role
                && existing.capabilities_json == input.capabilities_json
                && existing.assigned_principal_type == input.assigned_principal_type
                && existing.assigned_principal_id == input.assigned_principal_id
                && existing.capability_profile_revision == input.capability_profile_revision
                && existing.capability_profile_digest == input.capability_profile_digest
                && existing.issuing_principal_type == input.issuing_principal_type
                && existing.issuing_principal_id == input.issuing_principal_id;
            if !same {
                return Err(DbError::IdempotencyConflict);
            }
            tx.commit().await?;
            return Ok(existing);
        }
        if let Some((replacing_lease_id, replacing_lease_version)) = replacing_lease {
            let old = sqlx::query(
                "SELECT project_id, task_id, execution_id
                 FROM workspace_lease
                 WHERE id = ? AND status = 'active' AND version = ?",
            )
            .bind(replacing_lease_id)
            .bind(replacing_lease_version)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::VersionConflict)?;
            let old_project_id: String = old.try_get("project_id")?;
            let old_task_id: String = old.try_get("task_id")?;
            let old_execution_id: String = old.try_get("execution_id")?;
            if old_project_id != input.project_id
                || old_task_id != input.task_id
                || old_execution_id != input.execution_id
            {
                return Err(DbError::VersionConflict);
            }
            let revoked_at = input.updated_at.clone();
            let result = sqlx::query(
                "UPDATE workspace_lease
                 SET status = 'revoked', revoked_at = ?, version = version + 1,
                     updated_at = ?
                 WHERE id = ? AND status = 'active' AND version = ?",
            )
            .bind(&revoked_at)
            .bind(&revoked_at)
            .bind(replacing_lease_id)
            .bind(replacing_lease_version)
            .execute(&mut *tx)
            .await
            .map_err(workspace_lease_write_error)?;
            if result.rows_affected() != 1 {
                return Err(DbError::VersionConflict);
            }
        }
        sqlx::query(
            "INSERT INTO workspace_lease (
                id, project_id, task_id, task_version, execution_id,
                operation_idempotency_key,
                repository_binding_id, base_ref, role, capabilities_json,
                assigned_principal_type, assigned_principal_id,
                capability_profile_revision, capability_profile_digest,
                issuing_principal_type, issuing_principal_id, status, issued_at,
                expires_at, revoked_at, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, NULL, 1, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.project_id)
        .bind(&input.task_id)
        .bind(input.task_version)
        .bind(&input.execution_id)
        .bind(&input.operation_idempotency_key)
        .bind(&input.repository_binding_id)
        .bind(&input.base_ref)
        .bind(&input.role)
        .bind(&input.capabilities_json)
        .bind(&input.assigned_principal_type)
        .bind(&input.assigned_principal_id)
        .bind(&input.capability_profile_revision)
        .bind(&input.capability_profile_digest)
        .bind(&input.issuing_principal_type)
        .bind(&input.issuing_principal_id)
        .bind(&input.issued_at)
        .bind(&input.expires_at)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(workspace_lease_write_error)?;
        let row = sqlx::query(&format!(
            "SELECT {WORKSPACE_LEASE_COLUMNS} FROM workspace_lease WHERE id = ?"
        ))
        .bind(&input.id)
        .fetch_one(&mut *tx)
        .await?;
        let lease = map_workspace_lease(row)?;
        tx.commit().await?;
        Ok(lease)
    }
}

#[async_trait]
impl WorkspaceLeaseRepo for SqliteDb {
    async fn issue(&self, input: CreateWorkspaceLease) -> Result<WorkspaceLease> {
        self.issue_inner(input, None, None, None, None, None).await
    }

    async fn issue_with_project_authority(
        &self,
        input: CreateWorkspaceLease,
        expected_project_version: i64,
        expected_workflow_definition: &str,
        expected_task_status: &str,
        expected_effective_role: &str,
    ) -> Result<WorkspaceLease> {
        self.issue_inner(
            input,
            Some(expected_project_version),
            Some(expected_workflow_definition),
            Some(expected_task_status),
            Some(expected_effective_role),
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn replace_with_project_authority(
        &self,
        input: CreateWorkspaceLease,
        expected_project_version: i64,
        expected_workflow_definition: &str,
        expected_task_status: &str,
        expected_effective_role: &str,
        replacing_lease_id: &str,
        replacing_lease_version: i64,
    ) -> Result<WorkspaceLease> {
        self.issue_inner(
            input,
            Some(expected_project_version),
            Some(expected_workflow_definition),
            Some(expected_task_status),
            Some(expected_effective_role),
            Some((replacing_lease_id, replacing_lease_version)),
        )
        .await
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<WorkspaceLease>> {
        sqlx::query(&format!(
            "SELECT {WORKSPACE_LEASE_COLUMNS} FROM workspace_lease WHERE id = ?"
        ))
        .bind(id)
        .fetch_optional(self.pool())
        .await?
        .map(map_workspace_lease)
        .transpose()
    }

    async fn get_active_for_task(&self, task_id: &str) -> Result<Option<WorkspaceLease>> {
        sqlx::query(&format!(
            "SELECT {WORKSPACE_LEASE_COLUMNS} FROM workspace_lease
             WHERE task_id = ? AND status = 'active'
             ORDER BY issued_at DESC, id DESC LIMIT 1"
        ))
        .bind(task_id)
        .fetch_optional(self.pool())
        .await?
        .map(map_workspace_lease)
        .transpose()
    }

    async fn list_active_for_project(&self, project_id: &str) -> Result<Vec<WorkspaceLease>> {
        sqlx::query(&format!(
            "SELECT {WORKSPACE_LEASE_COLUMNS} FROM workspace_lease
             WHERE project_id = ? AND status = 'active'
             ORDER BY issued_at ASC, id ASC"
        ))
        .bind(project_id)
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(map_workspace_lease)
        .collect()
    }

    async fn revoke(
        &self,
        id: &str,
        expected_version: i64,
        revoked_at: &str,
    ) -> Result<WorkspaceLease> {
        let result = sqlx::query(
            "UPDATE workspace_lease
             SET status = 'revoked', revoked_at = ?, version = version + 1,
                 updated_at = ?
             WHERE id = ? AND status = 'active' AND version = ?",
        )
        .bind(revoked_at)
        .bind(revoked_at)
        .bind(id)
        .bind(expected_version)
        .execute(self.pool())
        .await
        .map_err(workspace_lease_write_error)?;
        if result.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        WorkspaceLeaseRepo::get_by_id(self, id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn renew_active(
        &self,
        now: &str,
        renew_before: &str,
        expires_at: &str,
        limit: i64,
    ) -> Result<Vec<WorkspaceLease>> {
        let rows = sqlx::query(&format!(
            "SELECT {WORKSPACE_LEASE_COLUMNS} FROM workspace_lease wl
             WHERE wl.status = 'active'
               AND wl.expires_at > ? AND wl.expires_at <= ?
               AND EXISTS (
                   SELECT 1 FROM execution e
                   WHERE e.id = wl.execution_id AND e.task_id = wl.task_id
                     AND e.status = 'running'
               )
             ORDER BY wl.expires_at ASC, wl.id ASC LIMIT ?"
        ))
        .bind(now)
        .bind(renew_before)
        .bind(limit.clamp(1, 500))
        .fetch_all(self.pool())
        .await?;
        let mut renewed = Vec::with_capacity(rows.len());
        for row in rows {
            let lease = map_workspace_lease(row)?;
            let result = sqlx::query(
                "UPDATE workspace_lease
                 SET expires_at = ?, version = version + 1, updated_at = ?
                 WHERE id = ? AND status = 'active' AND version = ?
                   AND expires_at > ? AND expires_at <= ?",
            )
            .bind(expires_at)
            .bind(now)
            .bind(&lease.id)
            .bind(lease.version)
            .bind(now)
            .bind(renew_before)
            .execute(self.pool())
            .await;
            match result {
                Ok(result) if result.rows_affected() == 1 => {
                    let row = sqlx::query(&format!(
                        "SELECT {WORKSPACE_LEASE_COLUMNS} FROM workspace_lease WHERE id = ?"
                    ))
                    .bind(&lease.id)
                    .fetch_one(self.pool())
                    .await?;
                    renewed.push(map_workspace_lease(row)?);
                }
                Ok(_) => {
                    // A stopped execution or stale governance binding must
                    // simply age out. The database renewal trigger performs
                    // the authoritative recheck for every candidate.
                }
                Err(error) if is_stale_workspace_lease_renewal(&error) => {}
                Err(error) => return Err(workspace_lease_write_error(error)),
            }
        }
        Ok(renewed)
    }

    async fn expire(&self, now: &str, limit: i64) -> Result<Vec<WorkspaceLease>> {
        let mut tx = crate::begin_immediate(&self.pool).await?;
        let rows = sqlx::query(&format!(
            "SELECT {WORKSPACE_LEASE_COLUMNS} FROM workspace_lease
             WHERE status = 'active' AND expires_at <= ?
             ORDER BY expires_at ASC, id ASC LIMIT ?"
        ))
        .bind(now)
        .bind(limit.clamp(1, 500))
        .fetch_all(&mut *tx)
        .await?;
        let mut expired = Vec::with_capacity(rows.len());
        for row in rows {
            let lease = map_workspace_lease(row)?;
            let result = sqlx::query(
                "UPDATE workspace_lease
                 SET status = 'expired', revoked_at = ?, version = version + 1,
                     updated_at = ?
                 WHERE id = ? AND status = 'active' AND version = ?
                   AND expires_at <= ?",
            )
            .bind(now)
            .bind(now)
            .bind(&lease.id)
            .bind(lease.version)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(workspace_lease_write_error)?;
            if result.rows_affected() == 1 {
                let row = sqlx::query(&format!(
                    "SELECT {WORKSPACE_LEASE_COLUMNS} FROM workspace_lease WHERE id = ?"
                ))
                .bind(&lease.id)
                .fetch_one(&mut *tx)
                .await?;
                expired.push(map_workspace_lease(row)?);
            }
        }
        tx.commit().await?;
        Ok(expired)
    }
}
