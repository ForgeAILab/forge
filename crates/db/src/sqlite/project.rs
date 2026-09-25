use super::*;
use crate::{new_uuid_v4, now_rfc3339};

const DEFAULT_PROJECT_AGENT_PERMISSION_CEILING: &str = r#"{"allowed":["read_project","read_agent_chat","read_task","read_memory","propose_task","propose_project","propose_message","propose_review","propose_commitment","propose_memory","propose_decision","propose_session"]}"#;

const PROJECT_VISIBLE_TO_USER: &str = "(owner_id IS NULL OR owner_id = ? OR EXISTS (
    SELECT 1 FROM project_member WHERE project_member.project_id = project.id
      AND project_member.user_id = ?
))";

async fn project_in_use_counts(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    project_id: &str,
) -> Result<(i64, i64)> {
    sqlx::query_as(
        "SELECT
            (SELECT COUNT(*) FROM execution e
               JOIN task t ON t.id = e.task_id
              WHERE t.project_id = ? AND e.status = 'running'),
            (SELECT COUNT(*) FROM workspace_lease wl
              WHERE wl.project_id = ? AND wl.status = 'active')",
    )
    .bind(project_id)
    .bind(project_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(Into::into)
}

#[async_trait]
impl ProjectRepo for SqliteDb {
    async fn create(&self, input: CreateProject) -> Result<Project> {
        self.create_with_agent_binding(input, None, None).await
    }

    async fn create_with_agent_binding(
        &self,
        input: CreateProject,
        identity_id: Option<String>,
        profile_id: Option<String>,
    ) -> Result<Project> {
        if identity_id.is_some() != profile_id.is_some() {
            return Err(DbError::Check(
                "Project Agent identity and profile must be selected together".to_owned(),
            ));
        }
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        sqlx::query("INSERT INTO project (id, name, settings, workflow_definition, workflow_template_name, primary_repo_id, owner_id, project_hooks_json, project_work_epoch, created_at, updated_at) VALUES (?, ?, ?, ?, NULL, ?, ?, '[]', 0, ?, ?)")
            .bind(&input.id)
            .bind(&input.name)
            .bind(&input.settings)
            .bind(&input.workflow_definition)
            .bind(&input.primary_repo_id)
            .bind(&input.owner_id)
            .bind(&input.created_at)
            .bind(&input.updated_at)
            .execute(&mut *transaction)
            .await?;

        // Project-member authorization is the stricter boundary used by
        // Project-local chat, artifact, media, and administration routes.
        // Materialize an authenticated creator's owner membership in the
        // same transaction as the Project so every public creation surface
        // returns a Project the creator can immediately manage. Test/system
        // fixtures may still use an owner label that has no `user` row; the
        // INSERT ... SELECT intentionally leaves those legacy fixtures alone.
        if let Some(owner_id) = input.owner_id.as_deref() {
            sqlx::query(
                "INSERT INTO project_member (
                    id, project_id, user_id, role, created_at, updated_at
                 )
                 SELECT ?, ?, id, 'owner', ?, ? FROM user WHERE id = ?",
            )
            .bind(new_uuid_v4())
            .bind(&input.id)
            .bind(&input.created_at)
            .bind(&input.updated_at)
            .bind(owner_id)
            .execute(&mut *transaction)
            .await?;
        }

        // V087 makes execution setup a durable Project concern. Keep the
        // operation and its finite checkpoint set in the same transaction as
        // Project creation so a fresh Project cannot become visible without
        // a recoverable operation.
        let operation_id = new_uuid_v4();
        sqlx::query(
            "INSERT INTO project_provisioning_operation (
                id, project_id, idempotency_key, status, current_checkpoint,
                attempt_count, max_attempts, retryable, created_at, updated_at, version
             ) VALUES (?, ?, ?, 'setup_required', 'preflight', 0, 3, 1, ?, ?, 1)",
        )
        .bind(&operation_id)
        .bind(&input.id)
        .bind(format!("project-provisioning:{}", input.id))
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&mut *transaction)
        .await?;
        for checkpoint in [
            "preflight",
            "repository_scaffolded",
            "repository_initialized",
            "repository_registered",
            "repository_linked",
            "roles_assigned",
        ] {
            sqlx::query(
                "INSERT INTO project_provisioning_checkpoint (
                    id, operation_id, checkpoint, status, attempt_count,
                    details_json, created_at, updated_at, version
                 ) VALUES (?, ?, ?, 'pending', 0, '{}', ?, ?, 1)",
            )
            .bind(new_uuid_v4())
            .bind(&operation_id)
            .bind(checkpoint)
            .bind(&input.created_at)
            .bind(&input.updated_at)
            .execute(&mut *transaction)
            .await?;
        }

        if let (Some(identity_id), Some(profile_id)) = (identity_id, profile_id) {
            let current: (String, i64) = sqlx::query_as(
                "UPDATE project_agent_binding
                 SET state = 'replaced', replaced_by_binding_id = NULL,
                     replacement_reason = 'project creation binding selection',
                     version = version + 1, updated_at = ?
                 WHERE project_id = ? AND state = 'agent_setup_required' AND version = 1
                 RETURNING id, version",
            )
            .bind(&input.updated_at)
            .bind(&input.id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| DbError::Check("project setup binding was not created".to_owned()))?;
            let binding_id = new_uuid_v4();
            sqlx::query(
                "INSERT INTO project_agent_binding (
                    id, project_id, identity_id, profile_id, state,
                    autonomy_policy_json, permission_ceiling_json, subscriptions_json,
                    wake_budget, version, created_at, updated_at
                 ) VALUES (?, ?, ?, ?, 'active', '{}', ?, '[]', ?, ?, ?, ?)",
            )
            .bind(&binding_id)
            .bind(&input.id)
            .bind(&identity_id)
            .bind(&profile_id)
            .bind(DEFAULT_PROJECT_AGENT_PERMISSION_CEILING)
            .bind(crate::DEFAULT_PROJECT_AGENT_WAKE_BUDGET)
            .bind(current.1)
            .bind(&input.created_at)
            .bind(&input.updated_at)
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                "UPDATE project_agent_binding
                 SET replaced_by_binding_id = ?
                 WHERE id = ? AND state = 'replaced'",
            )
            .bind(&binding_id)
            .bind(&current.0)
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                "UPDATE agent_chat
                 SET status = 'ready', version = version + 1, updated_at = ?
                 WHERE kind = 'project' AND project_id = ? AND status = 'agent_setup_required'",
            )
            .bind(&input.updated_at)
            .bind(&input.id)
            .execute(&mut *transaction)
            .await?;
        }

        // V071 triggers create the canonical Project Chat and setup binding
        // in this transaction.  Record all three durable facts before the
        // commit as well, so a rollback can never leave a ledger that claims
        // a Project exists without its singular chat/binding state.
        let (chat_id, chat_status): (String, String) = sqlx::query_as(
            "SELECT id, status FROM agent_chat
             WHERE kind = 'project' AND project_id = ?",
        )
        .bind(&input.id)
        .fetch_one(&mut *transaction)
        .await?;
        let (binding_id, binding_state, binding_identity_id, binding_profile_id, binding_version): (
            String,
            String,
            Option<String>,
            Option<String>,
            i64,
        ) = sqlx::query_as(
            "SELECT id, state, identity_id, profile_id, version
             FROM project_agent_binding
             WHERE project_id = ? AND state IN ('active', 'agent_setup_required')
             ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(&input.id)
        .fetch_one(&mut *transaction)
        .await?;
        let correlation_id = new_uuid_v4();
        let actor_type = if input.owner_id.is_some() {
            "user"
        } else {
            "system"
        }
        .to_owned();
        let actor_id = input.owner_id.clone();
        let events = vec![
            CreateDomainEvent {
                id: new_uuid_v4(),
                event_type: "project.created".to_owned(),
                entity_type: "project".to_owned(),
                entity_id: input.id.clone(),
                actor_type: actor_type.clone(),
                actor_id: actor_id.clone(),
                scope_type: "project".to_owned(),
                scope_id: input.id.clone(),
                correlation_id: correlation_id.clone(),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: Some(format!("project-created:{}", input.id)),
                payload_json: serde_json::json!({
                    "project_id": input.id,
                    "name": input.name,
                    "agent_chat_id": chat_id,
                    "project_agent_binding_id": binding_id,
                })
                .to_string(),
                created_at: input.created_at.clone(),
            },
            CreateDomainEvent {
                id: new_uuid_v4(),
                event_type: "project_agent_binding.created".to_owned(),
                entity_type: "project_agent_binding".to_owned(),
                entity_id: binding_id.clone(),
                actor_type: actor_type.clone(),
                actor_id: actor_id.clone(),
                scope_type: "project".to_owned(),
                scope_id: input.id.clone(),
                correlation_id: correlation_id.clone(),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: Some(format!("project-agent-binding-created:{}", binding_id)),
                payload_json: serde_json::json!({
                    "project_id": input.id,
                    "binding_id": binding_id,
                    "state": binding_state,
                    "identity_id": binding_identity_id,
                    "profile_id": binding_profile_id,
                    "version": binding_version,
                })
                .to_string(),
                created_at: input.created_at.clone(),
            },
            CreateDomainEvent {
                id: new_uuid_v4(),
                event_type: "agent_chat.created".to_owned(),
                entity_type: "agent_chat".to_owned(),
                entity_id: chat_id.clone(),
                actor_type,
                actor_id,
                scope_type: "agent_chat".to_owned(),
                scope_id: chat_id.clone(),
                correlation_id,
                causation_id: None,
                causation_depth: 0,
                dedupe_key: Some(format!("agent-chat-created:{}", chat_id)),
                payload_json: serde_json::json!({
                    "chat_id": chat_id,
                    "project_id": input.id,
                    "kind": "project",
                    "status": chat_status,
                })
                .to_string(),
                created_at: input.created_at.clone(),
            },
        ];
        for event in events {
            DomainEventRepo::append_event_in_tx(self, &mut transaction, &event).await?;
        }
        transaction.commit().await?;
        ProjectRepo::get_by_id(self, &input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<Project>> {
        sqlx::query(&format!(
            "SELECT {PROJECT_COLUMNS} FROM project WHERE id = ?"
        ))
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        .map(map_project)
        .transpose()
    }

    async fn get_visible_by_id(&self, id: &str, user_id: &str) -> Result<Option<Project>> {
        sqlx::query(&format!(
            "SELECT {PROJECT_COLUMNS} FROM project WHERE id = ? AND {PROJECT_VISIBLE_TO_USER}"
        ))
        .bind(id)
        .bind(user_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?
        .map(map_project)
        .transpose()
    }

    async fn list_visible(&self, user_id: &str, page: PageRequest) -> Result<Page<Project>> {
        let offset = decode_offset(&page.cursor)?;
        let sql = format!(
            "SELECT {PROJECT_COLUMNS} FROM project WHERE {PROJECT_VISIBLE_TO_USER}
             ORDER BY {} LIMIT ? OFFSET ?",
            order_clause_without_priority(&page)
        );
        let rows = sqlx::query(&sql)
            .bind(user_id)
            .bind(user_id)
            .bind(limit(&page) + 1)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?;
        let items = rows
            .into_iter()
            .map(map_project)
            .collect::<Result<Vec<_>>>()?;
        let total = if page.include_total {
            Some(
                sqlx::query_scalar::<_, i64>(&format!(
                    "SELECT COUNT(*) FROM project WHERE {PROJECT_VISIBLE_TO_USER}"
                ))
                .bind(user_id)
                .bind(user_id)
                .fetch_one(&self.pool)
                .await?,
            )
        } else {
            None
        };
        page_from_items(items, &page, offset, total)
    }

    async fn list(&self, page: PageRequest) -> Result<Page<Project>> {
        let offset = decode_offset(&page.cursor)?;
        let sql = format!(
            "SELECT {PROJECT_COLUMNS} FROM project ORDER BY {} LIMIT ? OFFSET ?",
            order_clause_without_priority(&page)
        );
        let rows = sqlx::query(&sql)
            .bind(limit(&page) + 1)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?;
        let items = rows
            .into_iter()
            .map(map_project)
            .collect::<Result<Vec<_>>>()?;
        let total = if page.include_total {
            total_count(&self.pool, "SELECT COUNT(*) FROM project").await?
        } else {
            None
        };
        page_from_items(items, &page, offset, total)
    }

    async fn update_at_version(
        &self,
        input: UpdateProject,
        expected_version: i64,
        project_hooks_json: Option<String>,
    ) -> Result<Project> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let project_row = sqlx::query(&format!(
            "SELECT {PROJECT_COLUMNS} FROM project WHERE id = ?"
        ))
        .bind(&input.id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
        let mut project = map_project(project_row)?;
        if project.version != expected_version {
            return Err(DbError::VersionConflict);
        }
        let original_settings = project.settings.clone();
        let original_primary_repo_id = project.primary_repo_id.clone();
        let original_paused_at = project.paused_at.clone();
        let original_system_pause_reason = project.system_pause_reason.clone();
        if let Some(name) = input.name {
            project.name = name;
        }
        if let Some(settings) = input.settings {
            project.settings = settings;
        }
        if let Some(primary_repo_id) = input.primary_repo_id {
            project.primary_repo_id = primary_repo_id;
        }
        if let Some(paused_at) = input.paused_at {
            project.paused_at = paused_at;
            project.system_pause_reason = None;
        }
        if let Some(project_hooks_json) = project_hooks_json {
            project.project_hooks_json = project_hooks_json;
        }
        project.updated_at = input.updated_at;
        let result = sqlx::query(
            "UPDATE project
             SET name = ?, settings = ?, primary_repo_id = ?, paused_at = ?,
                 system_pause_reason = ?, project_hooks_json = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(&project.name)
        .bind(&project.settings)
        .bind(project.primary_repo_id.as_deref())
        .bind(project.paused_at.as_deref())
        .bind(project.system_pause_reason.as_deref())
        .bind(&project.project_hooks_json)
        .bind(&project.updated_at)
        .bind(&project.id)
        .bind(expected_version)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        let project_authority_changed = original_settings != project.settings
            || original_primary_repo_id != project.primary_repo_id
            || original_paused_at != project.paused_at
            || original_system_pause_reason != project.system_pause_reason;
        if project_authority_changed {
            wake_dispatch_for_project_in_tx(&mut transaction, &project.id, &project.updated_at)
                .await?;
        }
        let updated_row = sqlx::query(&format!(
            "SELECT {PROJECT_COLUMNS} FROM project WHERE id = ?"
        ))
        .bind(&project.id)
        .fetch_one(&mut *transaction)
        .await?;
        let updated_project = map_project(updated_row)?;
        transaction.commit().await?;
        Ok(updated_project)
    }

    async fn update_workflow(
        &self,
        id: &str,
        workflow_definition: &str,
        workflow_template_name: Option<&str>,
        expected_version: i64,
        updated_at: &str,
    ) -> Result<()> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let current_version: i64 = sqlx::query_scalar("SELECT version FROM project WHERE id = ?")
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        if current_version != expected_version {
            return Err(DbError::VersionConflict);
        }
        let result = sqlx::query(
            "UPDATE project
             SET workflow_definition = ?, workflow_template_name = ?, version = version + 1,
                 updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(workflow_definition)
        .bind(workflow_template_name)
        .bind(updated_at)
        .bind(id)
        .bind(expected_version)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        wake_dispatch_for_project_in_tx(&mut transaction, id, updated_at).await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn set_project_hooks_json(
        &self,
        id: &str,
        project_hooks_json: &str,
        updated_at: &str,
    ) -> Result<()> {
        let result =
            sqlx::query("UPDATE project SET project_hooks_json = ?, updated_at = ? WHERE id = ?")
                .bind(project_hooks_json)
                .bind(updated_at)
                .bind(id)
                .execute(&self.pool)
                .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        Ok(())
    }

    async fn increment_project_work_epoch(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        by: i64,
    ) -> Result<i64> {
        let epoch = sqlx::query_scalar::<_, i64>(
            "UPDATE project SET project_work_epoch = project_work_epoch + ? WHERE id = ? RETURNING project_work_epoch",
        )
        .bind(by)
        .bind(project_id)
        .fetch_optional(&mut **tx)
        .await?;
        epoch.ok_or(DbError::NotFound)
    }

    async fn set_paused_at(&self, id: &str, paused_at: Option<String>) -> Result<()> {
        // Both callers (pause_project, resume_project) are an explicit,
        // external pause/resume, so this always clears any system reason —
        // pausing manually never carries one, and resuming (manual or the
        // dispatcher's own) makes any prior reason moot.
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let updated_at = now_rfc3339();
        let result = sqlx::query(
            "UPDATE project
             SET paused_at = ?, system_pause_reason = NULL, version = version + 1,
                 updated_at = ?
             WHERE id = ?",
        )
        .bind(paused_at.as_deref())
        .bind(&updated_at)
        .bind(id)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        wake_dispatch_for_project_in_tx(&mut transaction, id, &updated_at).await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn set_system_pause_reason_if_unchanged(
        &self,
        id: &str,
        expected_version: i64,
        expected_primary_repo_id: Option<&str>,
        repository_linked: bool,
        paused_at: &str,
        reason: &str,
    ) -> Result<bool> {
        // Guarded on the complete dispatcher snapshot: only the same active
        // Project/repository state may be auto-paused. A concurrent pause,
        // repository attachment/deletion, or another Project mutation makes
        // this a benign no-op rather than an error.
        let linkage = if repository_linked {
            "EXISTS"
        } else {
            "NOT EXISTS"
        };
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let updated_at = now_rfc3339();
        let result = sqlx::query(&format!(
            "UPDATE project
             SET paused_at = ?, system_pause_reason = ?, version = version + 1,
                 updated_at = ?
             WHERE id = ? AND version = ? AND paused_at IS NULL
               AND system_pause_reason IS NULL
               AND primary_repo_id IS ?
               AND {linkage} (
                   SELECT 1 FROM repo
                   WHERE repo.id = project.primary_repo_id
                     AND repo.project_id = project.id
               )"
        ))
        .bind(paused_at)
        .bind(reason)
        .bind(&updated_at)
        .bind(id)
        .bind(expected_version)
        .bind(expected_primary_repo_id)
        .execute(&mut *transaction)
        .await?;
        let paused = result.rows_affected() > 0;
        if paused {
            wake_dispatch_for_project_in_tx(&mut transaction, id, &updated_at).await?;
        }
        transaction.commit().await?;
        Ok(paused)
    }

    async fn clear_system_pause_if_unchanged(
        &self,
        id: &str,
        expected_version: i64,
        expected_primary_repo_id: &str,
        expected_paused_at: &str,
        expected_reason: &str,
    ) -> Result<bool> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let updated_at = now_rfc3339();
        let result = sqlx::query(
            "UPDATE project
             SET paused_at = NULL, system_pause_reason = NULL, version = version + 1,
                 updated_at = ?
             WHERE id = ? AND version = ? AND primary_repo_id = ?
               AND paused_at = ? AND system_pause_reason = ?
               AND EXISTS (
                   SELECT 1 FROM repo
                   WHERE repo.id = project.primary_repo_id
                     AND repo.project_id = project.id
               )",
        )
        .bind(&updated_at)
        .bind(id)
        .bind(expected_version)
        .bind(expected_primary_repo_id)
        .bind(expected_paused_at)
        .bind(expected_reason)
        .execute(&mut *transaction)
        .await?;
        let cleared = result.rows_affected() > 0;
        if cleared {
            wake_dispatch_for_project_in_tx(&mut transaction, id, &updated_at).await?;
        }
        transaction.commit().await?;
        Ok(cleared)
    }

    async fn ensure_deletable(&self, id: &str) -> Result<()> {
        let mut tx = crate::begin_immediate(&self.pool).await?;
        let exists = sqlx::query_scalar::<_, i64>("SELECT 1 FROM project WHERE id = ?")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?
            .is_some();
        if !exists {
            return Err(DbError::NotFound);
        }

        let (running_executions, active_leases) = project_in_use_counts(&mut tx, id).await?;
        if running_executions > 0 || active_leases > 0 {
            return Err(DbError::ProjectInUse {
                project_id: id.to_owned(),
                running_executions,
                active_leases,
            });
        }
        tx.commit().await?;
        Ok(())
    }

    async fn delete(&self, id: &str) -> Result<()> {
        self.delete_with_workspace_paths(id).await.map(|_| ())
    }

    async fn delete_with_workspace_paths(&self, id: &str) -> Result<ProjectDeletionPaths> {
        let mut tx = crate::begin_immediate(&self.pool).await?;
        let exists = sqlx::query_scalar::<_, i64>("SELECT 1 FROM project WHERE id = ?")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?
            .is_some();
        if !exists {
            return Err(DbError::NotFound);
        }

        // Deleting a Project destroys in-flight agent work. Force callers
        // must obtain provider acknowledgement, terminalize executions, and
        // revoke leases before reaching this transaction, but the database
        // boundary remains guarded as the final admission check. Keeping the
        // guard here prevents any lower-level caller from bypassing the
        // stop protocol accidentally.
        let (running_executions, active_leases) = project_in_use_counts(&mut tx, id).await?;
        if running_executions > 0 || active_leases > 0 {
            return Err(DbError::ProjectInUse {
                project_id: id.to_owned(),
                running_executions,
                active_leases,
            });
        }

        // These snapshots are taken after BEGIN IMMEDIATE, so Task,
        // Workspace, and repository rows admitted before the teardown lock is
        // acquired are included. Task IDs also cover an orphan worktree with
        // no Workspace row. A creator that loses the lock must wait for the
        // Project/task/repository cascade and then clean its just-created
        // worktree or cache after its FK failure.
        let task_ids = sqlx::query_scalar::<_, String>("SELECT id FROM task WHERE project_id = ?")
            .bind(id)
            .fetch_all(&mut *tx)
            .await?;
        let workspace_paths = sqlx::query_scalar::<_, String>(
            "SELECT w.worktree_path
             FROM workspace w
             JOIN task t ON t.id = w.task_id
             WHERE t.project_id = ?",
        )
        .bind(id)
        .fetch_all(&mut *tx)
        .await?;
        let repository_paths = sqlx::query_as::<_, (String, Option<String>)>(
            "SELECT id, local_path FROM repo WHERE project_id = ?",
        )
        .bind(id)
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(|(id, local_path)| ProjectDeletionRepositoryPath { id, local_path })
        .collect();

        // Immutable orchestration rows remain protected from individual
        // deletion. Project deletion is the one bounded teardown operation:
        // the transaction installs a Project-scoped guard, removes immutable
        // leaves in dependency order, then lets the existing cascades remove
        // mutable projections. Deferring FKs closes self-referential and
        // cross-artifact RESTRICT edges until the whole Project is gone.
        sqlx::query("PRAGMA defer_foreign_keys = ON")
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO project_deletion_guard (project_id, created_at) VALUES (?, ?)")
            .bind(id)
            .bind(now_rfc3339())
            .execute(&mut *tx)
            .await?;

        for statement in [
            "DELETE FROM agent_lcm_node WHERE timeline_id IN (
                 SELECT l.id FROM agent_lcm_timeline l
                 JOIN project_deletion_guard g ON g.project_id = ?
                 WHERE (l.scope_type = 'project' AND l.scope_id = g.project_id)
                    OR (l.scope_type = 'task' AND EXISTS (
                        SELECT 1 FROM task t WHERE t.id = l.scope_id
                          AND t.project_id = g.project_id))
                    OR (l.scope_type = 'agent_chat' AND EXISTS (
                        SELECT 1 FROM agent_chat c WHERE c.id = l.scope_id
                          AND c.project_id = g.project_id)))",
            "DELETE FROM agent_lcm_entry WHERE timeline_id IN (
                 SELECT l.id FROM agent_lcm_timeline l
                 JOIN project_deletion_guard g ON g.project_id = ?
                 WHERE (l.scope_type = 'project' AND l.scope_id = g.project_id)
                    OR (l.scope_type = 'task' AND EXISTS (
                        SELECT 1 FROM task t WHERE t.id = l.scope_id
                          AND t.project_id = g.project_id))
                    OR (l.scope_type = 'agent_chat' AND EXISTS (
                        SELECT 1 FROM agent_chat c WHERE c.id = l.scope_id
                          AND c.project_id = g.project_id)))",
            "DELETE FROM agent_lcm_operation WHERE timeline_id IN (
                 SELECT l.id FROM agent_lcm_timeline l
                 JOIN project_deletion_guard g ON g.project_id = ?
                 WHERE (l.scope_type = 'project' AND l.scope_id = g.project_id)
                    OR (l.scope_type = 'task' AND EXISTS (
                        SELECT 1 FROM task t WHERE t.id = l.scope_id
                          AND t.project_id = g.project_id))
                    OR (l.scope_type = 'agent_chat' AND EXISTS (
                        SELECT 1 FROM agent_chat c WHERE c.id = l.scope_id
                          AND c.project_id = g.project_id)))",
            "DELETE FROM agent_lcm_timeline WHERE id IN (
                 SELECT l.id FROM agent_lcm_timeline l
                 JOIN project_deletion_guard g ON g.project_id = ?
                 WHERE (l.scope_type = 'project' AND l.scope_id = g.project_id)
                    OR (l.scope_type = 'task' AND EXISTS (
                        SELECT 1 FROM task t WHERE t.id = l.scope_id
                          AND t.project_id = g.project_id))
                    OR (l.scope_type = 'agent_chat' AND EXISTS (
                        SELECT 1 FROM agent_chat c WHERE c.id = l.scope_id
                          AND c.project_id = g.project_id)))",
            "DELETE FROM media_asset_tombstone WHERE asset_id IN
                 (SELECT id FROM media_asset WHERE project_id = ?)",
            // Attachments hold the assets with RESTRICT, so the Project's own
            // cascade cannot be trusted to reach them before the assets.
            "DELETE FROM project_media_attachment WHERE project_id = ?",
            "DELETE FROM project_release_media_pin WHERE project_id = ?",
            "DELETE FROM project_release_reference WHERE release_id IN
                 (SELECT id FROM project_release WHERE project_id = ?)",
            "DELETE FROM project_release WHERE project_id = ?",
            "DELETE FROM project_readiness_input WHERE readiness_snapshot_id IN
                 (SELECT id FROM project_readiness_snapshot WHERE project_id = ?)",
            "DELETE FROM project_readiness_snapshot WHERE project_id = ?",
            "DELETE FROM project_milestone_check_result WHERE project_id = ?",
            "DELETE FROM project_milestone_revision WHERE milestone_id IN
                 (SELECT id FROM project_milestone WHERE project_id = ?)",
            "DELETE FROM project_document_approval WHERE document_id IN
                 (SELECT id FROM project_document WHERE project_id = ?)",
            "DELETE FROM project_document_revision WHERE document_id IN
                 (SELECT id FROM project_document WHERE project_id = ?)",
            "DELETE FROM project_decision WHERE project_id = ?",
            "DELETE FROM project_reconciliation_resolution WHERE reconciliation_id IN
                 (SELECT id FROM project_reconciliation_record WHERE project_id = ?)",
            "DELETE FROM project_reconciliation_record WHERE project_id = ?",
            "DELETE FROM project_canonical_conflict WHERE project_id = ?",
            "DELETE FROM project_charter_approval_event WHERE approval_id IN
                 (SELECT a.id FROM project_charter_approval a
                  JOIN project_charter c ON c.id = a.charter_id
                  WHERE c.project_id = ?)",
            "DELETE FROM project_charter_approval WHERE charter_id IN
                 (SELECT id FROM project_charter WHERE project_id = ?)",
            "DELETE FROM project_charter_revision WHERE charter_id IN
                 (SELECT id FROM project_charter WHERE project_id = ?)",
            "DELETE FROM workspace_lease WHERE project_id = ?",
            "DELETE FROM project_charter WHERE project_id = ?",
            // The Project's Agent Chat is removed by cascade, but the rows that
            // hang off it are immutable and RESTRICT-referenced, so they have to
            // go first, while their parents still exist for the guard to match.
            "DELETE FROM agent_wake_disposition_current WHERE disposition_id IN
                 (SELECT d.id FROM agent_wake_disposition d
                  JOIN agent_chat_turn_job j ON j.id = d.turn_job_id
                  JOIN agent_chat c ON c.id = j.chat_id
                  WHERE c.project_id = ?)",
            "DELETE FROM agent_wake_disposition WHERE turn_job_id IN
                 (SELECT j.id FROM agent_chat_turn_job j
                  JOIN agent_chat c ON c.id = j.chat_id
                  WHERE c.project_id = ?)",
            "DELETE FROM agent_handoff_delivery WHERE handoff_id IN
                 (SELECT h.id FROM agent_handoff h
                  JOIN agent_chat c ON c.id = h.source_chat_id OR c.id = h.target_chat_id
                  WHERE c.project_id = ?)",
            "DELETE FROM agent_chat_message WHERE chat_id IN
                 (SELECT id FROM agent_chat WHERE project_id = ?)",
            // Topics only ever cascade in from `agent_chat`, never explicitly
            // deleted otherwise -- by the time that cascade reaches them the
            // parent Chat row is already gone, so their own guard (which
            // joins back to `agent_chat`) can no longer match. Clearing them
            // here, while the Chat still exists, keeps the guard meaningful.
            "DELETE FROM agent_chat_topic WHERE chat_id IN
                 (SELECT id FROM agent_chat WHERE project_id = ?)",
            // A handed-off Genesis session exists only as this Project's origin
            // record, and its CHECK forbids the NULL the Project's removal would
            // otherwise write. It also has to precede the handoff delete for the
            // same reason. Charters are already gone, so nothing re-parents.
            "DELETE FROM product_genesis_session WHERE project_id = ?",
            "DELETE FROM agent_handoff WHERE EXISTS
                 (SELECT 1 FROM agent_chat c
                  WHERE c.project_id = ?
                    AND (c.id = agent_handoff.source_chat_id
                         OR c.id = agent_handoff.target_chat_id))",
            "DELETE FROM agent_chat_instruction_revision WHERE chat_id IN
                 (SELECT id FROM agent_chat WHERE project_id = ?)",
        ] {
            sqlx::query(statement).bind(id).execute(&mut *tx).await?;
        }

        let result = sqlx::query("DELETE FROM project WHERE id = ?")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        sqlx::query("DELETE FROM project_deletion_guard WHERE project_id = ?")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(ProjectDeletionPaths {
            task_ids,
            workspace_paths,
            repository_paths,
        })
    }
}
