use super::*;
use crate::now_rfc3339;

// CLI identities still receive authority from the server-issued canonical
// scope. This ceiling only makes the scope catalog usable; it cannot grant a
// Chat filesystem access or let Main cross into Project/Task mutations.
const DEFAULT_CLI_SCOPE_PERMISSIONS: &str = r#"{"permissions":["read_account","read_project","read_agent_chat","read_task","read_memory","propose_task","propose_discovery","propose_project","propose_handoff","propose_message","propose_review","propose_commitment","propose_memory","propose_decision","propose_session","task_read","task_write"]}"#;

#[async_trait]
impl AgentRepo for SqliteDb {
    async fn create(&self, input: CreateAgent) -> Result<Agent> {
        let profile_id = crate::new_uuid_v4();
        let mut transaction = crate::begin_immediate(&self.pool).await?;

        if input.is_default {
            clear_default_for_executor(&mut transaction, &input.executor_type, Some(&input.id))
                .await?;
        }

        sqlx::query(
            "INSERT INTO agent_identity (
                id, name, description, selected_profile_id,
                max_concurrent_tasks, heartbeat_interval_seconds,
                max_missed_heartbeats, status, last_heartbeat_at,
                is_default, paused, owner_id, visibility,
                account_permission_ceiling, created_at, updated_at
             ) VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.name)
        .bind(input.description.as_deref())
        .bind(input.max_concurrent_tasks)
        .bind(input.heartbeat_interval_seconds)
        .bind(input.max_missed_heartbeats)
        .bind(input.status.to_string())
        .bind(input.last_heartbeat_at.as_deref())
        .bind(if input.is_default { 1 } else { 0 })
        .bind(if input.paused { 1 } else { 0 })
        .bind(input.owner_id.as_deref())
        .bind(&input.visibility)
        .bind(DEFAULT_CLI_SCOPE_PERMISSIONS)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&mut *transaction)
        .await?;

        insert_profile(
            &mut transaction,
            &CreateAgentProfile {
                id: profile_id.clone(),
                identity_id: input.id.clone(),
                backend_kind: "cli".to_string(),
                executor_type: input.executor_type,
                provider: None,
                model: input.model,
                reasoning_effort: input.reasoning_effort,
                permission_policy: input.permission_policy,
                prompt_template: input.prompt_template,
                capabilities_json: input.capabilities_json,
                tool_policy_json: DEFAULT_CLI_SCOPE_PERMISSIONS.to_string(),
                config_json: input.config_json,
                credential_ref: input.credential_ref,
                daemon_id: input.daemon_id,
                created_at: input.created_at,
                updated_at: input.updated_at.clone(),
            },
        )
        .await?;

        sqlx::query(
            "UPDATE agent_identity
             SET selected_profile_id = ?
             WHERE id = ?",
        )
        .bind(&profile_id)
        .bind(&input.id)
        .execute(&mut *transaction)
        .await?;

        transaction.commit().await?;
        AgentRepo::get_by_id(self, &input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn create_identity_with_profile(
        &self,
        identity: CreateAgentIdentity,
        profile: CreateAgentProfile,
    ) -> Result<Agent> {
        if profile.identity_id != identity.id {
            return Err(DbError::Check(
                "profile identity must match the new identity".to_owned(),
            ));
        }
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        if identity.is_default {
            clear_default_for_executor(
                &mut transaction,
                &profile.executor_type,
                Some(&identity.id),
            )
            .await?;
        }
        sqlx::query(
            "INSERT INTO agent_identity (
                id, name, description, selected_profile_id,
                max_concurrent_tasks, heartbeat_interval_seconds,
                max_missed_heartbeats, status, last_heartbeat_at,
                is_default, paused, owner_id, visibility,
                account_permission_ceiling, created_at, updated_at
             ) VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&identity.id)
        .bind(&identity.name)
        .bind(identity.description.as_deref())
        .bind(identity.max_concurrent_tasks)
        .bind(identity.heartbeat_interval_seconds)
        .bind(identity.max_missed_heartbeats)
        .bind(identity.status.to_string())
        .bind(identity.last_heartbeat_at.as_deref())
        .bind(if identity.is_default { 1 } else { 0 })
        .bind(if identity.paused { 1 } else { 0 })
        .bind(identity.owner_id.as_deref())
        .bind(&identity.visibility)
        .bind(&identity.account_permission_ceiling)
        .bind(&identity.created_at)
        .bind(&identity.updated_at)
        .execute(&mut *transaction)
        .await?;
        insert_profile(&mut transaction, &profile).await?;
        sqlx::query("UPDATE agent_identity SET selected_profile_id = ? WHERE id = ?")
            .bind(&profile.id)
            .bind(&identity.id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        AgentRepo::get_by_id(self, &identity.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<Agent>> {
        sqlx::query("SELECT * FROM agent_current WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_agent)
            .transpose()
    }

    async fn list(&self, query: AgentListQuery) -> Result<Page<Agent>> {
        let offset = decode_offset(&query.page.cursor)?;
        let mut where_parts = Vec::new();
        if query.status.is_some() {
            where_parts.push("agent.status = ?");
        }
        if query.executor_type.is_some() {
            where_parts.push("agent.executor_type = ?");
        }
        where_parts.extend(std::iter::repeat_n(
            "agent.capabilities_json LIKE ?",
            query.capabilities.len(),
        ));
        let where_sql = if where_parts.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", where_parts.join(" AND "))
        };
        let order_sql = match (&query.page.sort_by, &query.page.sort_order) {
            (SortBy::CreatedAt, SortOrder::Asc) => "agent.created_at ASC, agent.id ASC",
            (SortBy::CreatedAt, SortOrder::Desc) => "agent.created_at DESC, agent.id DESC",
            (SortBy::UpdatedAt, SortOrder::Asc) => "agent.updated_at ASC, agent.id ASC",
            (SortBy::UpdatedAt, SortOrder::Desc) => "agent.updated_at DESC, agent.id DESC",
            (SortBy::Id, SortOrder::Asc) => "agent.id ASC",
            (SortBy::Id, SortOrder::Desc) => "agent.id DESC",
            (SortBy::Priority, SortOrder::Asc) => "agent.created_at ASC, agent.id ASC",
            (SortBy::Priority, SortOrder::Desc) => "agent.created_at DESC, agent.id DESC",
            (SortBy::BoardPosition, SortOrder::Asc) => "agent.created_at ASC, agent.id ASC",
            (SortBy::BoardPosition, SortOrder::Desc) => "agent.created_at DESC, agent.id DESC",
            (SortBy::Title, SortOrder::Asc) => "agent.name ASC, agent.id ASC",
            (SortBy::Title, SortOrder::Desc) => "agent.name DESC, agent.id DESC",
            (SortBy::Status, SortOrder::Asc) => "agent.status ASC, agent.id ASC",
            (SortBy::Status, SortOrder::Desc) => "agent.status DESC, agent.id DESC",
            (SortBy::Agent, SortOrder::Asc) | (SortBy::TaskType, SortOrder::Asc) => {
                "agent.created_at ASC, agent.id ASC"
            }
            (SortBy::Agent, SortOrder::Desc) | (SortBy::TaskType, SortOrder::Desc) => {
                "agent.created_at DESC, agent.id DESC"
            }
        };
        let sql = format!(
            "SELECT agent.* FROM agent_current AS agent{} ORDER BY {} LIMIT ? OFFSET ?",
            where_sql, order_sql
        );
        let mut q = sqlx::query(&sql);
        if let Some(status) = &query.status {
            q = q.bind(status.to_string());
        }
        if let Some(executor_type) = &query.executor_type {
            q = q.bind(executor_type);
        }
        for capability in &query.capabilities {
            q = q.bind(format!("%\"{capability}\"%"));
        }
        let rows = q
            .bind(limit(&query.page) + 1)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?;
        let items = rows
            .into_iter()
            .map(map_agent)
            .collect::<Result<Vec<_>>>()?;
        let total = if query.page.include_total {
            let count_sql = format!("SELECT COUNT(*) FROM agent_current AS agent{}", where_sql);
            let mut q = sqlx::query_scalar::<_, i64>(&count_sql);
            if let Some(status) = &query.status {
                q = q.bind(status.to_string());
            }
            if let Some(executor_type) = &query.executor_type {
                q = q.bind(executor_type);
            }
            for capability in &query.capabilities {
                q = q.bind(format!("%\"{capability}\"%"));
            }
            Some(q.fetch_one(&self.pool).await?)
        } else {
            None
        };
        page_from_items(items, &query.page, offset, total)
    }

    async fn update(&self, input: UpdateAgent) -> Result<Agent> {
        let mut agent = AgentRepo::get_by_id(self, &input.id)
            .await?
            .ok_or(DbError::NotFound)?;
        if agent.version != input.expected_version {
            return Err(DbError::VersionConflict);
        }

        let profile_changed = input.model.is_some()
            || input.reasoning_effort.is_some()
            || input.permission_policy.is_some()
            || input.prompt_template.is_some()
            || input.capabilities_json.is_some()
            || input.config_json.is_some()
            || input.daemon_id.is_some();

        if let Some(name) = input.name {
            agent.name = name;
        }
        if let Some(description) = input.description {
            agent.description = description;
        }
        if let Some(model) = input.model {
            agent.model = model;
        }
        if let Some(reasoning_effort) = input.reasoning_effort {
            agent.reasoning_effort = reasoning_effort;
        }
        if let Some(permission_policy) = input.permission_policy {
            agent.permission_policy = permission_policy;
        }
        if let Some(prompt_template) = input.prompt_template {
            agent.prompt_template = prompt_template;
        }
        if let Some(capabilities_json) = input.capabilities_json {
            agent.capabilities_json = capabilities_json;
        }
        if let Some(config_json) = input.config_json {
            agent.config_json = config_json;
        }
        if let Some(daemon_id) = input.daemon_id {
            agent.daemon_id = daemon_id;
        }
        if let Some(max_concurrent_tasks) = input.max_concurrent_tasks {
            agent.max_concurrent_tasks = max_concurrent_tasks;
        }
        if let Some(heartbeat_interval_seconds) = input.heartbeat_interval_seconds {
            agent.heartbeat_interval_seconds = heartbeat_interval_seconds;
        }
        if let Some(max_missed_heartbeats) = input.max_missed_heartbeats {
            agent.max_missed_heartbeats = max_missed_heartbeats;
        }
        if let Some(status) = input.status {
            agent.status = status;
        }
        if let Some(last_heartbeat_at) = input.last_heartbeat_at {
            agent.last_heartbeat_at = last_heartbeat_at;
        }
        if let Some(is_default) = input.is_default {
            agent.is_default = is_default;
        }
        if let Some(paused) = input.paused {
            agent.paused = paused;
        }

        let mut transaction = crate::begin_immediate(&self.pool).await?;
        if agent.is_default {
            clear_default_for_executor(&mut transaction, &agent.executor_type, Some(&agent.id))
                .await?;
        }

        let selected_profile_id = if profile_changed {
            let profile_id = crate::new_uuid_v4();
            insert_profile(
                &mut transaction,
                &CreateAgentProfile {
                    id: profile_id.clone(),
                    identity_id: agent.id.clone(),
                    backend_kind: agent.backend_kind.clone(),
                    executor_type: agent.executor_type.clone(),
                    provider: agent.provider.clone(),
                    model: agent.model.clone(),
                    reasoning_effort: agent.reasoning_effort.clone(),
                    permission_policy: agent.permission_policy.clone(),
                    prompt_template: agent.prompt_template.clone(),
                    capabilities_json: agent.capabilities_json.clone(),
                    tool_policy_json: agent.tool_policy_json.clone(),
                    config_json: agent.config_json.clone(),
                    credential_ref: agent.credential_ref.clone(),
                    daemon_id: agent.daemon_id.clone(),
                    created_at: input.updated_at.clone(),
                    updated_at: input.updated_at.clone(),
                },
            )
            .await?;
            profile_id
        } else {
            agent.profile_id.clone()
        };

        let result = sqlx::query(
            "UPDATE agent_identity
             SET name = ?, description = ?, selected_profile_id = ?,
                 max_concurrent_tasks = ?, heartbeat_interval_seconds = ?,
                 max_missed_heartbeats = ?, status = ?, last_heartbeat_at = ?,
                 is_default = ?, paused = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(&agent.name)
        .bind(agent.description.as_deref())
        .bind(&selected_profile_id)
        .bind(agent.max_concurrent_tasks)
        .bind(agent.heartbeat_interval_seconds)
        .bind(agent.max_missed_heartbeats)
        .bind(agent.status.to_string())
        .bind(agent.last_heartbeat_at.as_deref())
        .bind(if agent.is_default { 1 } else { 0 })
        .bind(if agent.paused { 1 } else { 0 })
        .bind(&input.updated_at)
        .bind(&agent.id)
        .bind(input.expected_version)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }

        transaction.commit().await?;
        AgentRepo::get_by_id(self, &agent.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn set_paused(&self, id: &str, paused: bool) -> Result<()> {
        let result = sqlx::query(
            "UPDATE agent_identity
             SET paused = ?, version = version + 1, updated_at = ?
             WHERE id = ?",
        )
        .bind(if paused { 1 } else { 0 })
        .bind(now_rfc3339())
        .bind(id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        Ok(())
    }

    async fn duplicate_agent(
        &self,
        source_id: &str,
        new_id: String,
        new_name: String,
        now: String,
    ) -> Result<Agent> {
        let source = AgentRepo::get_by_id(self, source_id)
            .await?
            .ok_or(DbError::NotFound)?;
        AgentRepo::create(
            self,
            CreateAgent {
                id: new_id.clone(),
                name: new_name,
                description: source.description,
                executor_type: source.executor_type,
                model: source.model,
                reasoning_effort: source.reasoning_effort,
                permission_policy: source.permission_policy,
                prompt_template: source.prompt_template,
                capabilities_json: source.capabilities_json,
                config_json: source.config_json,
                credential_ref: source.credential_ref,
                daemon_id: source.daemon_id,
                max_concurrent_tasks: source.max_concurrent_tasks,
                heartbeat_interval_seconds: source.heartbeat_interval_seconds,
                max_missed_heartbeats: source.max_missed_heartbeats,
                status: AgentStatus::Idle,
                last_heartbeat_at: None,
                is_default: false,
                paused: false,
                owner_id: source.owner_id,
                visibility: source.visibility,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
    }

    async fn archive(&self, id: &str, archived_at: &str) -> Result<()> {
        let result = sqlx::query(
            "UPDATE agent_identity
             SET archived_at = ?, paused = 1, is_default = 0, status = 'offline',
                 version = version + 1, updated_at = ?
             WHERE id = ? AND archived_at IS NULL",
        )
        .bind(archived_at)
        .bind(archived_at)
        .bind(id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        Ok(())
    }

    async fn count_active_tasks(&self, agent_id: &str) -> Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT
                (
                    SELECT COUNT(DISTINCT task.id)
                    FROM task
                    JOIN task_role_assignment ON task_role_assignment.task_id = task.id
                    JOIN project ON project.id = task.project_id
                    WHERE task_role_assignment.assignee_type = 'agent'
                      AND task_role_assignment.assignee_id = ?
                      AND task.deleted_at IS NULL
                      AND (
                          EXISTS (
                              SELECT 1
                              FROM json_each(
                                  CASE
                                      WHEN json_valid(project.workflow_definition)
                                      THEN project.workflow_definition
                                      ELSE '{\"states\":[]}'
                                  END,
                                  '$.states'
                              ) AS workflow_state
                              WHERE json_extract(workflow_state.value, '$.name') = task.status
                                AND json_extract(workflow_state.value, '$.kind') IN ('active', 'gate')
                          )
                          OR (
                              task.status IN ('in_progress', 'review', 'merging')
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM json_each(
                                      CASE
                                          WHEN json_valid(project.workflow_definition)
                                          THEN project.workflow_definition
                                          ELSE '{\"states\":[]}'
                                      END,
                                      '$.states'
                                  ) AS workflow_state
                                  WHERE json_extract(workflow_state.value, '$.name') = task.status
                              )
                          )
                      )
                ) +
                (
                    SELECT COUNT(*)
                    FROM agent_chat_turn_job
                    WHERE responder_identity_id = ?
                      AND status IN ('leased', 'running')
                )",
        )
        .bind(agent_id)
        .bind(agent_id)
        .fetch_one(&self.pool)
        .await?)
    }
}

#[async_trait]
impl AgentProfileRepo for SqliteDb {
    async fn create_profile(&self, input: CreateAgentProfile) -> Result<AgentProfile> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        insert_profile(&mut transaction, &input).await?;
        transaction.commit().await?;
        AgentProfileRepo::get_profile(self, &input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn create_and_select_profile(
        &self,
        profile: CreateAgentProfile,
        selection: SelectAgentProfile,
    ) -> Result<(AgentProfile, Agent)> {
        if profile.id != selection.profile_id || profile.identity_id != selection.identity_id {
            return Err(DbError::VersionConflict);
        }
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        insert_profile(&mut transaction, &profile).await?;
        let selected = sqlx::query(
            "UPDATE agent_identity
             SET selected_profile_id = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(&selection.profile_id)
        .bind(&selection.updated_at)
        .bind(&selection.identity_id)
        .bind(selection.expected_version)
        .execute(&mut *transaction)
        .await?;
        if selected.rows_affected() == 0 {
            transaction.rollback().await?;
            let identity_exists =
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_identity WHERE id = ?")
                    .bind(&selection.identity_id)
                    .fetch_one(&self.pool)
                    .await?
                    > 0;
            return Err(if identity_exists {
                DbError::VersionConflict
            } else {
                DbError::NotFound
            });
        }
        transaction.commit().await?;
        let created = AgentProfileRepo::get_profile(self, &profile.id)
            .await?
            .ok_or(DbError::NotFound)?;
        let agent = AgentRepo::get_by_id(self, &selection.identity_id)
            .await?
            .ok_or(DbError::NotFound)?;
        Ok((created, agent))
    }

    async fn get_profile(&self, id: &str) -> Result<Option<AgentProfile>> {
        sqlx::query("SELECT * FROM agent_profile WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_agent_profile)
            .transpose()
    }

    async fn list_profiles(&self, identity_id: &str) -> Result<Vec<AgentProfile>> {
        sqlx::query(
            "SELECT * FROM agent_profile
             WHERE identity_id = ?
             ORDER BY version DESC, created_at DESC, id DESC",
        )
        .bind(identity_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_agent_profile)
        .collect()
    }

    async fn select_profile(&self, input: SelectAgentProfile) -> Result<Agent> {
        let result = sqlx::query(
            "UPDATE agent_identity
             SET selected_profile_id = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?
               AND EXISTS (
                   SELECT 1 FROM agent_profile
                   WHERE agent_profile.id = ?
                     AND agent_profile.identity_id = agent_identity.id
               )",
        )
        .bind(&input.profile_id)
        .bind(&input.updated_at)
        .bind(&input.identity_id)
        .bind(input.expected_version)
        .bind(&input.profile_id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            let identity_exists =
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_identity WHERE id = ?")
                    .bind(&input.identity_id)
                    .fetch_one(&self.pool)
                    .await?
                    > 0;
            return Err(if identity_exists {
                DbError::VersionConflict
            } else {
                DbError::NotFound
            });
        }
        AgentRepo::get_by_id(self, &input.identity_id)
            .await?
            .ok_or(DbError::NotFound)
    }
}

impl SqliteDb {
    pub async fn list_agents_usable_in_project(
        &self,
        project_id: &str,
        user_id: &str,
    ) -> Result<Vec<Agent>> {
        let rows = sqlx::query(
            "SELECT DISTINCT agent.*
             FROM agent_current AS agent
             LEFT JOIN project_agent_binding AS binding
               ON binding.identity_id = agent.id
              AND binding.state = 'active'
             WHERE agent.owner_id = ?
                OR agent.visibility = 'global'
                OR (binding.project_id = ?)
             ORDER BY agent.created_at ASC, agent.id ASC",
        )
        .bind(user_id)
        .bind(project_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(map_agent).collect()
    }
}

async fn insert_profile(
    transaction: &mut Transaction<'_, Sqlite>,
    input: &CreateAgentProfile,
) -> Result<()> {
    let revision = sqlx::query_scalar::<_, i64>(
        "SELECT COALESCE(MAX(version), 0) + 1
         FROM agent_profile
         WHERE identity_id = ?",
    )
    .bind(&input.identity_id)
    .fetch_one(&mut **transaction)
    .await?;

    sqlx::query(
        "INSERT INTO agent_profile (
            id, identity_id, backend_kind, executor_type, provider, model,
            reasoning_effort, permission_policy, prompt_template,
            capabilities_json, tool_policy_json, config_json, credential_ref,
            daemon_id, version, created_at, updated_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&input.id)
    .bind(&input.identity_id)
    .bind(&input.backend_kind)
    .bind(&input.executor_type)
    .bind(input.provider.as_deref())
    .bind(input.model.as_deref())
    .bind(input.reasoning_effort.as_deref())
    .bind(input.permission_policy.as_deref())
    .bind(input.prompt_template.as_deref())
    .bind(&input.capabilities_json)
    .bind(&input.tool_policy_json)
    .bind(&input.config_json)
    .bind(input.credential_ref.as_deref())
    .bind(input.daemon_id.as_deref())
    .bind(revision)
    .bind(&input.created_at)
    .bind(&input.updated_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn clear_default_for_executor(
    transaction: &mut Transaction<'_, Sqlite>,
    executor_type: &str,
    except_identity_id: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "UPDATE agent_identity
         SET is_default = 0
         WHERE selected_profile_id IN (
             SELECT id FROM agent_profile WHERE executor_type = ?
         )
           AND (? IS NULL OR id != ?)",
    )
    .bind(executor_type)
    .bind(except_identity_id)
    .bind(except_identity_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}
