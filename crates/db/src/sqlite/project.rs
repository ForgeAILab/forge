use super::*;
use crate::{new_uuid_v4, now_rfc3339};

const DEFAULT_PROJECT_AGENT_PERMISSION_CEILING: &str = r#"{"allowed":["read_project","read_agent_chat","read_task","read_memory","propose_task","propose_project","propose_message","propose_review","propose_commitment","propose_memory","propose_decision","propose_session"]}"#;

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

    async fn update(&self, input: UpdateProject) -> Result<Project> {
        let mut project = ProjectRepo::get_by_id(self, &input.id)
            .await?
            .ok_or(DbError::NotFound)?;
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
        }
        project.updated_at = input.updated_at;
        sqlx::query(
            "UPDATE project SET name = ?, settings = ?, primary_repo_id = ?, paused_at = ?, project_hooks_json = ?, project_work_epoch = ?, updated_at = ? WHERE id = ?",
        )
        .bind(&project.name)
        .bind(&project.settings)
        .bind(project.primary_repo_id.as_deref())
        .bind(project.paused_at.as_deref())
        .bind(&project.project_hooks_json)
        .bind(project.project_work_epoch)
        .bind(&project.updated_at)
        .bind(&project.id)
        .execute(&self.pool)
        .await?;
        Ok(project)
    }

    async fn update_at_version(
        &self,
        input: UpdateProject,
        expected_version: i64,
        project_hooks_json: Option<String>,
    ) -> Result<Project> {
        let mut project = ProjectRepo::get_by_id(self, &input.id)
            .await?
            .ok_or(DbError::NotFound)?;
        if project.version != expected_version {
            return Err(DbError::VersionConflict);
        }
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
        }
        if let Some(project_hooks_json) = project_hooks_json {
            project.project_hooks_json = project_hooks_json;
        }
        project.updated_at = input.updated_at;
        let result = sqlx::query(
            "UPDATE project
             SET name = ?, settings = ?, primary_repo_id = ?, paused_at = ?,
                 project_hooks_json = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(&project.name)
        .bind(&project.settings)
        .bind(project.primary_repo_id.as_deref())
        .bind(project.paused_at.as_deref())
        .bind(&project.project_hooks_json)
        .bind(&project.updated_at)
        .bind(&project.id)
        .bind(expected_version)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        ProjectRepo::get_by_id(self, &project.id)
            .await?
            .ok_or(DbError::NotFound)
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
        let result = sqlx::query("UPDATE project SET paused_at = ?, updated_at = ? WHERE id = ?")
            .bind(paused_at.as_deref())
            .bind(now_rfc3339())
            .bind(id)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        Ok(())
    }

    async fn delete(&self, id: &str) -> Result<()> {
        let mut tx = crate::begin_immediate(&self.pool).await?;
        let exists = sqlx::query_scalar::<_, i64>("SELECT 1 FROM project WHERE id = ?")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?
            .is_some();
        if !exists {
            return Err(DbError::NotFound);
        }

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
            "DELETE FROM media_asset_tombstone WHERE asset_id IN
                 (SELECT id FROM media_asset WHERE project_id = ?)",
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
            "DELETE FROM project_execution_baseline_approval WHERE baseline_id IN
                 (SELECT id FROM project_execution_baseline WHERE project_id = ?)",
            "DELETE FROM project_execution_baseline_revision WHERE baseline_id IN
                 (SELECT id FROM project_execution_baseline WHERE project_id = ?)",
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
        Ok(())
    }
}
