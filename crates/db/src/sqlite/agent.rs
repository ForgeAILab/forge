use super::*;
use crate::now_rfc3339;

// CLI identities still receive authority from the server-issued canonical
// scope. This ceiling only makes the scope catalog usable; it cannot grant a
// Chat filesystem access or let Main cross into Project/Task mutations.
const DEFAULT_CLI_SCOPE_PERMISSIONS: &str = r#"{"permissions":["read_account","read_project","read_agent_chat","read_task","read_memory","propose_task","propose_discovery","propose_project","propose_handoff","propose_message","propose_review","propose_commitment","propose_memory","propose_decision","propose_session","task_read","task_write"]}"#;

async fn list_agents_filtered(
    db: &SqliteDb,
    query: AgentListQuery,
    user_id: Option<&str>,
) -> Result<Page<Agent>> {
    let offset = decode_offset(&query.page.cursor)?;
    let mut where_parts = Vec::new();
    if user_id.is_some() {
        // Apply visibility in SQL before pagination and counting. The REST
        // and MCP surfaces use this same predicate, so neither can leak
        // private identities through an offset or total-count mismatch.
        where_parts.push("(agent.visibility = 'global' OR agent.owner_id = ?)");
    }
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
    if let Some(user_id) = user_id {
        q = q.bind(user_id);
    }
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
        .fetch_all(&db.pool)
        .await?;
    let items = rows
        .into_iter()
        .map(map_agent)
        .collect::<Result<Vec<_>>>()?;
    let total = if query.page.include_total {
        let count_sql = format!("SELECT COUNT(*) FROM agent_current AS agent{}", where_sql);
        let mut q = sqlx::query_scalar::<_, i64>(&count_sql);
        if let Some(user_id) = user_id {
            q = q.bind(user_id);
        }
        if let Some(status) = &query.status {
            q = q.bind(status.to_string());
        }
        if let Some(executor_type) = &query.executor_type {
            q = q.bind(executor_type);
        }
        for capability in &query.capabilities {
            q = q.bind(format!("%\"{capability}\"%"));
        }
        Some(q.fetch_one(&db.pool).await?)
    } else {
        None
    };
    page_from_items(items, &query.page, offset, total)
}

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
        .bind({
            operation_registry::authority::parse_permissions(&identity.account_permission_ceiling)?;
            &identity.account_permission_ceiling
        })
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
        list_agents_filtered(self, query, None).await
    }

    async fn list_visible(&self, user_id: &str, query: AgentListQuery) -> Result<Page<Agent>> {
        list_agents_filtered(self, query, Some(user_id)).await
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

    async fn count_active_assigned_tasks(&self, agent_id: &str) -> Result<i64> {
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

    async fn count_running_executions(&self, agent_id: &str) -> Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution WHERE agent_id = ? AND status = 'running'",
        )
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
    operation_registry::authority::parse_permissions(&input.tool_policy_json)?;
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

impl SqliteDb {
    /// Live assignment/turn counts; terminal execution statistics are supplied
    /// by the shared read index on collection routes.
    pub async fn agent_list_active_assignments(
        &self,
        agent_ids: &[String],
    ) -> Result<std::collections::HashMap<String, i64>> {
        let ids_json = serde_json::to_string(agent_ids).map_err(|_| DbError::InvalidTransition)?;
        let rows = sqlx::query(
            "WITH requested AS (SELECT DISTINCT value AS agent_id FROM json_each(?)),
             assignments AS MATERIALIZED (
                 SELECT DISTINCT assignee_id, task_id FROM task_role_assignment
                 WHERE assignee_type = 'agent' AND assignee_id IN requested
             ), workflow_states AS MATERIALIZED (
                 SELECT project.id AS project_id,
                        json_extract(workflow_state.value, '$.name') AS state_name,
                        MAX(CASE WHEN json_extract(workflow_state.value, '$.kind') IN ('active', 'gate')
                            THEN 1 ELSE 0 END) AS active
                 FROM project, json_each(
                     CASE WHEN json_valid(project.workflow_definition)
                         THEN project.workflow_definition ELSE '{\"states\":[]}' END,
                     '$.states') AS workflow_state
                 WHERE project.id IN (SELECT project_id FROM task WHERE id IN (SELECT task_id FROM assignments))
                 GROUP BY project.id, state_name
             ), active_tasks AS MATERIALIZED (
                 SELECT task.id FROM task JOIN project ON project.id = task.project_id
                 LEFT JOIN workflow_states ON workflow_states.project_id = task.project_id
                     AND workflow_states.state_name = task.status
                 WHERE task.deleted_at IS NULL AND task.id IN (SELECT task_id FROM assignments)
                   AND CASE WHEN workflow_states.state_name IS NOT NULL THEN workflow_states.active
                       ELSE task.status IN ('in_progress', 'review', 'merging') END
             ), task_counts AS (
                 SELECT assignee_id AS agent_id, COUNT(*) AS active_tasks
                 FROM assignments WHERE task_id IN active_tasks GROUP BY assignee_id
             ), turn_counts AS (
                 SELECT responder_identity_id AS agent_id, COUNT(*) AS active_turns
                 FROM agent_chat_turn_job WHERE responder_identity_id IN requested
                   AND status IN ('leased', 'running') GROUP BY responder_identity_id
             )
             SELECT requested.agent_id,
                    COALESCE(task_counts.active_tasks, 0) + COALESCE(turn_counts.active_turns, 0) AS active_assigned_task_count
             FROM requested LEFT JOIN task_counts USING(agent_id)
               LEFT JOIN turn_counts USING(agent_id)",
        )
        .bind(ids_json)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                Ok((
                    row.try_get("agent_id")?,
                    row.try_get("active_assigned_task_count")?,
                ))
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct AgentOperatorPressureRow {
    pub agent_id: String,
    pub agent_name: String,
    pub daemon_id: Option<String>,
    pub max_concurrent_tasks: i64,
    pub running_executions: i64,
}

impl SqliteDb {
    pub async fn agent_operator_pressure_rows(
        &self,
    ) -> crate::Result<Vec<AgentOperatorPressureRow>> {
        let rows = sqlx::query("SELECT
                a.id AS agent_id,
                a.name AS agent_name,
                a.daemon_id,
                a.max_concurrent_tasks,
                (
                    SELECT COUNT(*)
                    FROM execution e
                    WHERE e.agent_id = a.id AND e.status = 'running'
                ) + (
                    SELECT COUNT(*) FROM workspace_placement p
                    WHERE p.agent_id = a.id AND p.state IN ('reserved', 'preparing')
                       AND julianday(COALESCE(p.reserved_until, datetime(p.updated_at, '+10 minutes'))) > julianday('now')
                      AND NOT EXISTS (SELECT 1 FROM execution e
                          WHERE e.workspace_id = p.workspace_id AND e.status = 'running')
                ) AS running_executions
             FROM agent_current a
             ORDER BY a.name ASC, a.id ASC")
.fetch_all(self.pool()).await?;
        rows.into_iter()
            .map(|row| {
                Ok(AgentOperatorPressureRow {
                    agent_id: row.try_get("agent_id")?,
                    agent_name: row.try_get("agent_name")?,
                    daemon_id: row.try_get("daemon_id")?,
                    max_concurrent_tasks: row.try_get("max_concurrent_tasks")?,
                    running_executions: row.try_get("running_executions")?,
                })
            })
            .collect()
    }
}

impl SqliteDb {
    pub async fn retired_machine_task_agent_ids(
        &self,
        daemon_id: &str,
        task_id: &str,
        role_task_id: &str,
    ) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar("SELECT i.id FROM agent_identity i WHERE i.archived_at IS NOT NULL
         AND i.selected_profile_id IN (SELECT id FROM agent_profile WHERE daemon_id=?)
         AND (EXISTS(SELECT 1 FROM task t WHERE t.id=? AND t.assignee_type='agent' AND t.assignee_id=i.id)
           OR EXISTS(SELECT 1 FROM task_role_assignment r WHERE r.task_id=? AND r.assignee_type='agent' AND r.assignee_id=i.id))")
.bind(daemon_id)
.bind(task_id)
.bind(role_task_id)
.fetch_all(self.pool())
.await?)
    }
}

impl SqliteDb {
    pub async fn workspace_admission_agent_version_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        agent_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT version FROM agent_current WHERE id = ?")
                .bind(agent_id)
                .fetch_optional(connection)
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn workspace_claim_agent_version_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        agent_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT version FROM agent_current WHERE id = ?")
                .bind(agent_id)
                .fetch_optional(connection)
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn active_project_identity_wake_budget_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        identity_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT wake_budget
                 FROM project_agent_binding
                 WHERE identity_id = ? AND project_id = ? AND state = 'active'",
        )
        .bind(identity_id)
        .bind(project_id)
        .fetch_optional(connection)
        .await?)
    }
}

#[derive(Debug)]
pub struct VisibleAttentionAgentBindingsInput<'a> {
    pub identity_id: &'a str,
    pub account_id: &'a str,
    pub member_id: &'a str,
    pub project_identity_id: &'a str,
    pub owner_id: &'a str,
    pub limit: i64,
}
/// Typed fields retain decoding failures for the caller's original conditional/error handling.
#[derive(Debug)]
pub struct AttentionAgentBindingRow {
    pub binding_id: crate::Result<String>,
    pub binding_type: crate::Result<String>,
    pub project_id: crate::Result<Option<String>>,
    pub chat_id: crate::Result<String>,
    pub state: crate::Result<String>,
    pub subscriptions_json: crate::Result<String>,
    pub wake_budget: crate::Result<i64>,
}

impl SqliteDb {
    pub async fn visible_attention_agent_bindings(
        &self,
        input: VisibleAttentionAgentBindingsInput<'_>,
    ) -> crate::Result<Vec<AttentionAgentBindingRow>> {
        let rows = sqlx::query(
            "SELECT binding_id, binding_type, project_id, chat_id, state,
                    subscriptions_json, wake_budget
             FROM (
                 SELECT b.id AS binding_id,
                        'main' AS binding_type,
                        NULL AS project_id,
                        c.id AS chat_id,
                        b.state,
                        '[]' AS subscriptions_json,
                        0 AS wake_budget
                 FROM account_main_agent_binding b
                 JOIN agent_chat c
                   ON c.kind = 'account_main' AND c.account_id = b.account_id
                 WHERE b.identity_id = ? AND b.account_id = ?
                   AND b.state <> 'revoked'
                 UNION ALL
                 SELECT b.id AS binding_id,
                        'project' AS binding_type,
                        b.project_id,
                        c.id AS chat_id,
                        b.state,
                        b.subscriptions_json,
                        b.wake_budget
                 FROM project_agent_binding b
                 JOIN agent_chat c
                   ON c.kind = 'project' AND c.project_id = b.project_id
                 JOIN project p ON p.id = b.project_id
                 LEFT JOIN project_member pm
                   ON pm.project_id = b.project_id AND pm.user_id = ?
                 WHERE b.identity_id = ? AND b.state <> 'revoked'
                   AND (p.owner_id IS NULL OR p.owner_id = ? OR pm.user_id IS NOT NULL)
             )
             ORDER BY project_id ASC, binding_type ASC, binding_id ASC LIMIT ?",
        )
        .bind(input.identity_id)
        .bind(input.account_id)
        .bind(input.member_id)
        .bind(input.project_identity_id)
        .bind(input.owner_id)
        .bind(input.limit)
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(AttentionAgentBindingRow {
                    binding_id: row.try_get("binding_id").map_err(crate::DbError::from),
                    binding_type: row.try_get("binding_type").map_err(crate::DbError::from),
                    project_id: row.try_get("project_id").map_err(crate::DbError::from),
                    chat_id: row.try_get("chat_id").map_err(crate::DbError::from),
                    state: row.try_get("state").map_err(crate::DbError::from),
                    subscriptions_json: row
                        .try_get("subscriptions_json")
                        .map_err(crate::DbError::from),
                    wake_budget: row.try_get("wake_budget").map_err(crate::DbError::from),
                })
            })
            .collect()
    }
}

/// Typed fields retain decoding failures for the caller's original conditional/error handling.
#[derive(Debug)]
pub struct AttentionAgentSessionRow {
    pub session_id: crate::Result<String>,
    pub scope_type: crate::Result<String>,
    pub scope_id: crate::Result<String>,
    pub backend_kind: crate::Result<String>,
    pub status: crate::Result<String>,
    pub connection_status: crate::Result<String>,
    pub last_activity_at: crate::Result<Option<String>>,
    pub updated_at: crate::Result<String>,
}

impl SqliteDb {
    pub async fn attention_agent_sessions(
        &self,
        identity_id: &str,
    ) -> crate::Result<Vec<AttentionAgentSessionRow>> {
        let rows = sqlx::query(
            "SELECT s.id AS session_id, c.scope_type, c.scope_id,
                    s.backend_kind, s.status, s.connection_status,
                    s.last_activity_at, s.updated_at
             FROM agent_session s
             JOIN agent_context_scope c ON c.id = s.context_scope_id
             WHERE s.identity_id = ?
             ORDER BY s.updated_at DESC, s.id DESC",
        )
        .bind(identity_id)
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(AttentionAgentSessionRow {
                    session_id: row.try_get("session_id").map_err(crate::DbError::from),
                    scope_type: row.try_get("scope_type").map_err(crate::DbError::from),
                    scope_id: row.try_get("scope_id").map_err(crate::DbError::from),
                    backend_kind: row.try_get("backend_kind").map_err(crate::DbError::from),
                    status: row.try_get("status").map_err(crate::DbError::from),
                    connection_status: row
                        .try_get("connection_status")
                        .map_err(crate::DbError::from),
                    last_activity_at: row
                        .try_get("last_activity_at")
                        .map_err(crate::DbError::from),
                    updated_at: row.try_get("updated_at").map_err(crate::DbError::from),
                })
            })
            .collect()
    }
}

impl SqliteDb {
    pub async fn attention_session_checkpoint_count(&self, session_id: &str) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM protected_agent_session_state
                 WHERE session_id = ? AND checkpoint_turn_id IS NOT NULL",
        )
        .bind(session_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_actor_identity_count(&self, identity_id: &str) -> crate::Result<i64> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_identity WHERE id = ?")
                .bind(identity_id)
                .fetch_one(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn attention_project_responder_identity(
        &self,
        project_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT identity_id FROM project_agent_binding
                     WHERE project_id = ? AND state = 'active' AND identity_id IS NOT NULL",
        )
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_main_responder_identity(
        &self,
        account_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT identity_id FROM account_main_agent_binding
                     WHERE account_id = ? AND state = 'active'",
        )
        .bind(account_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_chat_main_responder_identity(
        &self,
        account_id: Option<String>,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT identity_id FROM account_main_agent_binding
                             WHERE account_id = ? AND state = 'active'",
        )
        .bind(account_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_chat_project_responder_identity(
        &self,
        project_id: Option<String>,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT identity_id FROM project_agent_binding
                             WHERE project_id = ? AND state = 'active' AND identity_id IS NOT NULL",
        )
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_requested_identity_count(
        &self,
        identity_id: &str,
    ) -> crate::Result<i64> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_identity WHERE id = ?")
                .bind(identity_id)
                .fetch_one(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn attention_main_identity_binding_count(
        &self,
        account_id: &str,
        identity_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM account_main_agent_binding
                         WHERE account_id = ? AND identity_id = ? AND state = 'active'",
        )
        .bind(account_id)
        .bind(identity_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_main_binding_count(&self, account_id: &str) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM account_main_agent_binding
                         WHERE account_id = ? AND state = 'active'",
        )
        .bind(account_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_project_identity_binding_count(
        &self,
        project_id: &str,
        identity_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_agent_binding
                         WHERE project_id = ? AND identity_id = ? AND state = 'active'",
        )
        .bind(project_id)
        .bind(identity_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_project_binding_count(&self, project_id: &str) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_agent_binding
                         WHERE project_id = ? AND state = 'active' AND identity_id IS NOT NULL",
        )
        .bind(project_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_chat_main_identity_binding_count(
        &self,
        account_id: String,
        identity_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM account_main_agent_binding
                                 WHERE account_id = ? AND identity_id = ? AND state = 'active'",
        )
        .bind(account_id)
        .bind(identity_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_chat_main_binding_count(
        &self,
        account_id: String,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM account_main_agent_binding
                                 WHERE account_id = ? AND state = 'active'",
        )
        .bind(account_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_chat_project_identity_binding_count(
        &self,
        project_id: String,
        identity_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_agent_binding
                                 WHERE project_id = ? AND identity_id = ? AND state = 'active'",
        )
        .bind(project_id)
        .bind(identity_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_chat_project_binding_count(
        &self,
        project_id: String,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_agent_binding
                                 WHERE project_id = ? AND state = 'active'
                                   AND identity_id IS NOT NULL",
        )
        .bind(project_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_account_identity_access_count(
        &self,
        identity_id: &str,
        account_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_identity
                 WHERE id = ? AND (owner_id IS NULL OR owner_id = ?)",
        )
        .bind(identity_id)
        .bind(account_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_project_identity_access_count(
        &self,
        identity_id: &str,
        project_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_agent_binding
                 WHERE identity_id = ? AND project_id = ? AND state = 'active'",
        )
        .bind(identity_id)
        .bind(project_id)
        .fetch_one(self.pool())
        .await?)
    }
}

/// Typed fields retain decoding failures for the caller's original conditional/error handling.
#[derive(Debug)]
pub struct AttentionIdentityOwnerRow {
    pub owner_id: crate::Result<Option<String>>,
}

impl SqliteDb {
    pub async fn attention_identity_owner(
        &self,
        identity_id: &str,
    ) -> crate::Result<Option<AttentionIdentityOwnerRow>> {
        let rows = sqlx::query("SELECT owner_id FROM agent_identity WHERE id = ?")
            .bind(identity_id)
            .fetch_optional(self.pool())
            .await?;
        rows.map(|row| {
            Ok(AttentionIdentityOwnerRow {
                owner_id: row.try_get("owner_id").map_err(crate::DbError::from),
            })
        })
        .transpose()
    }
}

impl SqliteDb {
    pub async fn attention_identity_visible_project_count(
        &self,
        member_id: &str,
        identity_id: &str,
        owner_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM project_agent_binding m
             JOIN project p ON p.id = m.project_id
             LEFT JOIN project_member pm ON pm.project_id = m.project_id AND pm.user_id = ?
             WHERE m.identity_id = ?
               AND m.state = 'active'
               AND (p.owner_id IS NULL OR p.owner_id = ? OR pm.user_id IS NOT NULL)",
        )
        .bind(member_id)
        .bind(identity_id)
        .bind(owner_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub(crate) fn attention_agent_visibility_predicate(
        user_id: &str,
        project_id: Option<&str>,
    ) -> (String, Vec<String>) {
        match project_id {
            Some(project_id) => (
                "EXISTS (
                    SELECT 1 FROM project_agent_binding pm
                    WHERE pm.identity_id = a.id AND pm.project_id = ? AND pm.state = 'active'
                )"
                .to_owned(),
                vec![project_id.to_owned()],
            ),
            None => (
                "(a.owner_id IS NULL OR a.owner_id = ? OR EXISTS (
                    SELECT 1 FROM project_agent_binding pam
                    JOIN project p ON p.id = pam.project_id
                    LEFT JOIN project_member pm ON pm.project_id = p.id AND pm.user_id = ?
                    WHERE pam.identity_id = a.id AND pam.state = 'active'
                      AND (p.owner_id IS NULL OR p.owner_id = ? OR pm.user_id IS NOT NULL)
                ))"
                .to_owned(),
                vec![user_id.to_owned(), user_id.to_owned(), user_id.to_owned()],
            ),
        }
    }
}

/// Typed fields retain decoding failures for the caller's original conditional/error handling.
#[derive(Debug)]
pub struct AttentionVisibleAgentHealthRow {
    pub id: crate::Result<String>,
    pub name: crate::Result<String>,
    pub backend_kind: crate::Result<Option<String>>,
    pub provider: crate::Result<Option<String>>,
    pub model: crate::Result<Option<String>>,
    pub status: crate::Result<String>,
    pub paused: crate::Result<i64>,
    pub last_heartbeat_at: crate::Result<Option<String>>,
    pub connection_status: crate::Result<Option<String>>,
    pub active_session_count: crate::Result<i64>,
    pub project_count: crate::Result<i64>,
}

impl SqliteDb {
    pub async fn attention_visible_agent_health(
        &self,
        user_id: &str,
        project_id: Option<&str>,
        limit: i64,
    ) -> crate::Result<Vec<AttentionVisibleAgentHealthRow>> {
        let (predicate, values) = Self::attention_agent_visibility_predicate(user_id, project_id);
        let (project_count, count_values) = if let Some(project_id) = project_id {
            (
                "COUNT(DISTINCT CASE WHEN m.project_id = ? THEN m.project_id END)".to_owned(),
                vec![project_id.to_owned()],
            )
        } else {
            (
                "COUNT(DISTINCT CASE
                    WHEN p2.owner_id IS NULL OR p2.owner_id = ? OR pm2.user_id IS NOT NULL
                    THEN m.project_id END)"
                    .to_owned(),
                vec![user_id.to_owned()],
            )
        };
        let (active_session_count, session_count_values) = match project_id {
            Some(project_id) => (
                "COUNT(DISTINCT CASE WHEN session_scope.project_id = ? THEN s.id END)".to_owned(),
                vec![project_id.to_owned()],
            ),
            None => ("COUNT(DISTINCT s.id)".to_owned(), Vec::new()),
        };
        let sql = format!(
            "SELECT a.id, a.name, a.backend_kind, a.provider, a.model,
                    a.status, a.paused, a.last_heartbeat_at,
                    h.status AS connection_status,
                    {active_session_count} AS active_session_count,
                    {project_count} AS project_count
             FROM agent_current a
             LEFT JOIN agent_connection_health h ON h.profile_id = a.profile_id
             LEFT JOIN agent_session s ON s.identity_id = a.id
                 AND s.status IN ('starting', 'ready', 'running', 'degraded')
             LEFT JOIN agent_context_scope session_scope
                 ON session_scope.id = s.context_scope_id
             LEFT JOIN project_agent_binding m ON m.identity_id = a.id AND m.state = 'active'
             LEFT JOIN project p2 ON p2.id = m.project_id
             LEFT JOIN project_member pm2 ON pm2.project_id = m.project_id AND pm2.user_id = ?
             WHERE {predicate}
             GROUP BY a.id, a.name, a.backend_kind, a.provider, a.model,
                      a.status, a.paused, a.last_heartbeat_at, h.status
             ORDER BY a.name ASC, a.id ASC LIMIT ?"
        );
        let mut query = sqlx::query(&sql);
        for value in session_count_values {
            query = query.bind(value);
        }
        for value in count_values {
            query = query.bind(value);
        }
        query = query.bind(user_id);
        for value in values {
            query = query.bind(value);
        }
        let rows = query.bind(limit).fetch_all(self.pool()).await?;
        Ok(rows
            .into_iter()
            .map(|row| AttentionVisibleAgentHealthRow {
                id: row.try_get("id").map_err(crate::DbError::from),
                name: row.try_get("name").map_err(crate::DbError::from),
                backend_kind: row.try_get("backend_kind").map_err(crate::DbError::from),
                provider: row.try_get("provider").map_err(crate::DbError::from),
                model: row.try_get("model").map_err(crate::DbError::from),
                status: row.try_get("status").map_err(crate::DbError::from),
                paused: row.try_get("paused").map_err(crate::DbError::from),
                last_heartbeat_at: row
                    .try_get("last_heartbeat_at")
                    .map_err(crate::DbError::from),
                connection_status: row
                    .try_get("connection_status")
                    .map_err(crate::DbError::from),
                active_session_count: row
                    .try_get("active_session_count")
                    .map_err(crate::DbError::from),
                project_count: row.try_get("project_count").map_err(crate::DbError::from),
            })
            .collect())
    }
}

impl SqliteDb {
    pub async fn attention_visible_active_session_count(
        &self,
        user_id: &str,
        project_id: Option<&str>,
    ) -> crate::Result<i64> {
        let (agent_predicate, agent_values) =
            Self::attention_agent_visibility_predicate(user_id, project_id);
        let session_sql = format!(
            "SELECT COUNT(*) FROM agent_session s
             JOIN agent_current a ON a.id = s.identity_id
             WHERE s.status IN ('starting', 'ready', 'running', 'degraded')
               AND {agent_predicate}"
        );
        let mut session_query = sqlx::query_scalar::<_, i64>(&session_sql);
        for value in agent_values {
            session_query = session_query.bind(value);
        }
        let active_sessions = session_query.fetch_one(self.pool()).await?;
        Ok(active_sessions)
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct AgentStatusHealthRow {
    row: sqlx::sqlite::SqliteRow,
}

impl AgentStatusHealthRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn connection_status(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("connection_status")?)
    }
    pub fn credential_status(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("credential_status")?)
    }
    pub fn credential_enabled(&self) -> crate::Result<Option<bool>> {
        Ok(self.row.try_get("credential_enabled")?)
    }
    pub fn provider_status(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("provider_status")?)
    }
    pub fn last_error_kind(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("last_error_kind")?)
    }
    pub fn backoff_until(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("backoff_until")?)
    }
    pub fn reservations(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("reservations")?)
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct CliRuntimePolicyStatusRow {
    row: sqlx::sqlite::SqliteRow,
}

impl CliRuntimePolicyStatusRow {
    pub fn owner_user_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("owner_user_id")?)
    }
    pub fn daemon_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("daemon_id")?)
    }
    pub fn executor_type(&self) -> crate::Result<String> {
        Ok(self.row.try_get("executor_type")?)
    }
    pub fn enabled(&self) -> crate::Result<bool> {
        Ok(self.row.try_get("enabled")?)
    }
}

impl SqliteDb {
    pub async fn agent_status_health_for_requested_agents(
        &self,
        requested_json: &str,
    ) -> crate::Result<Vec<AgentStatusHealthRow>> {
        let rows = sqlx::query("WITH requested AS (
            SELECT json_extract(value,'$.id') AS id,
                   json_extract(value,'$.profile') AS profile,
                   json_extract(value,'$.credential') AS credential FROM json_each(?)
         ), reservations AS (
            SELECT p.agent_id, COUNT(*) AS n FROM workspace_placement p
            WHERE p.agent_id IN (SELECT id FROM requested)
              AND p.state IN ('reserved','preparing')
              AND julianday(COALESCE(p.reserved_until,datetime(p.updated_at,'+10 minutes'))) > julianday('now')
              AND NOT EXISTS (SELECT 1 FROM execution e WHERE e.workspace_id=p.workspace_id AND e.status='running')
            GROUP BY p.agent_id
         )
         SELECT requested.id, h.status AS connection_status,
                c.status AS credential_status, c.enabled AS credential_enabled,
                p.status AS provider_status, p.last_error_kind, p.backoff_until,
                COALESCE(reservations.n,0) AS reservations
         FROM requested LEFT JOIN agent_connection_health h ON h.profile_id=requested.profile
         LEFT JOIN credential_handle c ON c.id=requested.credential
         LEFT JOIN provider_entry_health p ON p.credential_id=requested.credential
         LEFT JOIN reservations ON reservations.agent_id=requested.id")
            .bind(requested_json)
            .fetch_all(self.pool())
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| AgentStatusHealthRow { row })
            .collect())
    }
}

impl SqliteDb {
    pub async fn cli_runtime_policies_for_executor_types(
        &self,
        executor_types_json: &str,
    ) -> crate::Result<Vec<CliRuntimePolicyStatusRow>> {
        let rows = sqlx::query("SELECT owner_user_id,daemon_id,executor_type,enabled FROM cli_runtime_policy WHERE executor_type IN (SELECT value FROM json_each(?))")
            .bind(executor_types_json)
            .fetch_all(self.pool())
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| CliRuntimePolicyStatusRow { row })
            .collect())
    }
}

impl SqliteDb {
    pub async fn owner_cli_runtime_enabled(
        &self,
        owner_user_id: &str,
        daemon_id: &str,
        executor_type: &str,
    ) -> crate::Result<Option<bool>> {
        Ok(sqlx::query_scalar::<_, bool>(
            "SELECT enabled FROM cli_runtime_policy
             WHERE owner_user_id = ? AND daemon_id = ? AND executor_type = ?",
        )
        .bind(owner_user_id)
        .bind(daemon_id)
        .bind(executor_type)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn global_cli_runtime_enabled(
        &self,
        daemon_id: &str,
        executor_type: &str,
    ) -> crate::Result<Option<bool>> {
        Ok(sqlx::query_scalar::<_, Option<bool>>(
            "SELECT MIN(enabled) FROM cli_runtime_policy
         WHERE daemon_id = ? AND executor_type = ?",
        )
        .bind(daemon_id)
        .bind(executor_type)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn archive_agent_identity_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        archived_at: &str,
        updated_at: &str,
        agent_id: &str,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "UPDATE agent_identity
             SET archived_at = ?, paused = 1, is_default = 0, status = 'offline',
                 version = version + 1, updated_at = ?
             WHERE id = ? AND archived_at IS NULL",
        )
        .bind(archived_at)
        .bind(updated_at)
        .bind(agent_id)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

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
