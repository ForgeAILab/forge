use super::*;
use std::collections::HashSet;

#[async_trait]
impl TaskRepo for SqliteDb {
    async fn create(&self, input: CreateTask) -> Result<Task> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let task = TaskRepo::create_in_tx(self, &mut transaction, input).await?;
        transaction.commit().await?;
        Ok(task)
    }

    async fn create_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateTask,
    ) -> Result<Task> {
        sqlx::query("INSERT INTO task (id, project_id, repo_id, parent_task_id, assignee_type, assignee_id, title, description, task_type, status, is_automation, priority, board_position, subtask_order, task_state_config, merge_config, metadata_json, plan, created_at, updated_at) SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(MAX(board_position), 0.0) + 1.0, ?, ?, ?, ?, ?, ?, ? FROM task WHERE project_id = ?")
            .bind(&input.id)
            .bind(&input.project_id)
            .bind(input.repo_id.as_deref())
            .bind(input.parent_task_id.as_deref())
            .bind(input.assignee_type.as_deref())
            .bind(input.assignee_id.as_deref())
            .bind(&input.title)
            .bind(input.description.as_deref())
            .bind(&input.task_type)
            .bind(&input.status)
            .bind(if input.is_automation { 1 } else { 0 })
            .bind(input.priority)
            .bind(input.subtask_order)
            .bind(input.task_state_config.as_deref())
            .bind(input.merge_config.as_deref())
            .bind(Option::<&str>::None)
            .bind(input.plan.as_deref())
            .bind(&input.created_at)
            .bind(&input.updated_at)
            .bind(&input.project_id)
            .execute(&mut **transaction)
            .await?;
        let row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await?;
        map_task(row)
    }

    async fn get_by_id(&self, id: &str, include_deleted: bool) -> Result<Option<Task>> {
        let sql = if include_deleted {
            format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?")
        } else {
            format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ? AND deleted_at IS NULL")
        };
        sqlx::query(&sql)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_task)
            .transpose()
    }

    async fn list(&self, query: TaskListQuery) -> Result<Page<Task>> {
        let offset = decode_offset(&query.page.cursor)?;
        let mut where_parts = vec!["project_id = ?"];
        if !query.include_deleted {
            where_parts.push("deleted_at IS NULL");
        }
        if !query.include_archived {
            where_parts.push("archived_at IS NULL");
        }
        if !query.include_cancelled && !query.statuses.iter().any(|status| status == "cancelled") {
            where_parts.push("status != 'cancelled'");
        }
        if !query.statuses.is_empty() {
            where_parts.push("status IN (__STATUSES__)");
        }
        if !query.agent_ids.is_empty() {
            where_parts.push("id IN (SELECT task_id FROM task_role_assignment WHERE assignee_type = 'agent' AND assignee_id IN (__AGENTS__))");
        }
        if !query.assignee_types.is_empty() || !query.assignee_ids.is_empty() {
            where_parts.push("id IN (SELECT task_id FROM task_role_assignment WHERE (__ASSIGNEE_TYPE_FILTER__) AND (__ASSIGNEE_ID_FILTER__))");
        }
        if query.priority.is_some() {
            where_parts.push("priority = ?");
        }
        let search_pattern = query
            .q
            .as_deref()
            .map(str::trim)
            .filter(|term| !term.is_empty())
            .map(search_like_pattern);
        if search_pattern.is_some() {
            where_parts.push("(LOWER(title) LIKE ? ESCAPE '\\' OR LOWER(COALESCE(description, '')) LIKE ? ESCAPE '\\')");
        }
        let status_placeholders = vec!["?"; query.statuses.len()].join(", ");
        let agent_placeholders = vec!["?"; query.agent_ids.len()].join(", ");
        let assignee_type_placeholders = vec!["?"; query.assignee_types.len()].join(", ");
        let assignee_id_placeholders = vec!["?"; query.assignee_ids.len()].join(", ");
        let assignee_type_filter = if query.assignee_types.is_empty() {
            "1 = 1".to_owned()
        } else {
            format!("assignee_type IN ({assignee_type_placeholders})")
        };
        let assignee_id_filter = if query.assignee_ids.is_empty() {
            "1 = 1".to_owned()
        } else {
            format!("assignee_id IN ({assignee_id_placeholders})")
        };
        let where_sql = where_parts
            .join(" AND ")
            .replace("__STATUSES__", &status_placeholders)
            .replace("__AGENTS__", &agent_placeholders)
            .replace("__ASSIGNEE_TYPE_FILTER__", &assignee_type_filter)
            .replace("__ASSIGNEE_ID_FILTER__", &assignee_id_filter);
        let sql = format!(
            "SELECT {TASK_COLUMNS} FROM task WHERE {} ORDER BY {} LIMIT ? OFFSET ?",
            where_sql,
            order_clause(&query.page)
        );
        let mut q = sqlx::query(&sql).bind(&query.project_id);
        for status in &query.statuses {
            q = q.bind(status);
        }
        for agent_id in &query.agent_ids {
            q = q.bind(agent_id);
        }
        for assignee_type in &query.assignee_types {
            q = q.bind(assignee_type);
        }
        for assignee_id in &query.assignee_ids {
            q = q.bind(assignee_id);
        }
        if let Some(priority) = query.priority {
            q = q.bind(priority);
        }
        if let Some(search_pattern) = search_pattern.as_ref() {
            q = q.bind(search_pattern).bind(search_pattern);
        }
        let rows = q
            .bind(limit(&query.page) + 1)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?;
        let items = rows.into_iter().map(map_task).collect::<Result<Vec<_>>>()?;
        let total = if query.page.include_total {
            let count_sql = format!("SELECT COUNT(*) FROM task WHERE {}", where_sql);
            let mut q = sqlx::query_scalar::<_, i64>(&count_sql).bind(&query.project_id);
            for status in &query.statuses {
                q = q.bind(status);
            }
            for agent_id in &query.agent_ids {
                q = q.bind(agent_id);
            }
            for assignee_type in &query.assignee_types {
                q = q.bind(assignee_type);
            }
            for assignee_id in &query.assignee_ids {
                q = q.bind(assignee_id);
            }
            if let Some(priority) = query.priority {
                q = q.bind(priority);
            }
            if let Some(search_pattern) = search_pattern.as_ref() {
                q = q.bind(search_pattern).bind(search_pattern);
            }
            Some(q.fetch_one(&self.pool).await?)
        } else {
            None
        };
        page_from_items(items, &query.page, offset, total)
    }

    async fn list_by_executing_agent(&self, query: AgentTaskListQuery) -> Result<Page<Task>> {
        let offset = decode_offset(&query.page.cursor)?;
        let mut where_parts =
            vec!["id IN (SELECT DISTINCT task_id FROM execution WHERE agent_id = ?)".to_owned()];
        if !query.include_deleted {
            where_parts.push("deleted_at IS NULL".to_owned());
        }
        if !query.include_archived {
            where_parts.push("archived_at IS NULL".to_owned());
        }
        if !query.include_cancelled {
            where_parts.push("status != 'cancelled'".to_owned());
        }
        let where_sql = where_parts.join(" AND ");
        let sql = format!(
            "SELECT {TASK_COLUMNS} FROM task WHERE {} ORDER BY {} LIMIT ? OFFSET ?",
            where_sql,
            order_clause(&query.page)
        );
        let rows = sqlx::query(&sql)
            .bind(&query.agent_id)
            .bind(limit(&query.page) + 1)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?;
        let items = rows.into_iter().map(map_task).collect::<Result<Vec<_>>>()?;
        let total = if query.page.include_total {
            let count_sql = format!("SELECT COUNT(*) FROM task WHERE {where_sql}");
            Some(
                sqlx::query_scalar::<_, i64>(&count_sql)
                    .bind(&query.agent_id)
                    .fetch_one(&self.pool)
                    .await?,
            )
        } else {
            None
        };
        page_from_items(items, &query.page, offset, total)
    }

    async fn list_subtasks_ordered(&self, parent_task_id: &str) -> Result<Vec<Task>> {
        let sql = format!(
            "SELECT {TASK_COLUMNS} FROM task WHERE parent_task_id = ? AND deleted_at IS NULL ORDER BY subtask_order ASC, created_at ASC, id ASC"
        );
        let rows = sqlx::query(&sql)
            .bind(parent_task_id)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(map_task).collect()
    }

    async fn next_subtask_order(&self, parent_task_id: &str) -> Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COALESCE(MAX(subtask_order) + 1, 0) FROM task WHERE parent_task_id = ? AND deleted_at IS NULL",
        )
        .bind(parent_task_id)
        .fetch_one(&self.pool)
        .await?)
    }

    async fn reorder_subtasks(
        &self,
        parent_task_id: &str,
        ordered_ids: &[String],
        updated_at: &str,
    ) -> Result<()> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        if ordered_ids.iter().collect::<HashSet<_>>().len() != ordered_ids.len() {
            return Err(DbError::InvalidTransition);
        }

        for task_id in ordered_ids {
            let belongs_to_parent = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM task WHERE id = ? AND parent_task_id = ? AND deleted_at IS NULL",
            )
            .bind(task_id)
            .bind(parent_task_id)
            .fetch_one(&mut *transaction)
            .await?;
            if belongs_to_parent == 0 {
                return Err(DbError::NotFound);
            }
        }

        let sibling_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task WHERE parent_task_id = ? AND deleted_at IS NULL",
        )
        .bind(parent_task_id)
        .fetch_one(&mut *transaction)
        .await?;
        if sibling_count != ordered_ids.len() as i64 {
            return Err(DbError::InvalidTransition);
        }

        for (subtask_order, task_id) in ordered_ids.iter().enumerate() {
            let result =
                sqlx::query("UPDATE task SET subtask_order = ?, updated_at = ? WHERE id = ?")
                    .bind(subtask_order as i64)
                    .bind(updated_at)
                    .bind(task_id)
                    .execute(&mut *transaction)
                    .await?;
            if result.rows_affected() == 0 {
                return Err(DbError::NotFound);
            }
        }

        transaction.commit().await?;
        Ok(())
    }

    async fn update(&self, input: UpdateTask) -> Result<Task> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let task_row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
            .bind(&input.id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        let mut task = map_task(task_row)?;
        if task.deleted_at.is_some() {
            return Err(DbError::InvalidSoftDelete);
        }
        if task.version != input.expected_version {
            return Err(DbError::VersionConflict);
        }
        let previous_error_annotation = task.error_annotation.clone();
        let previous_blocked_json = task.blocked_json.clone();
        let previous_failed_json = task.failed_json.clone();
        if let Some(title) = input.title {
            task.title = title;
        }
        if let Some(description) = input.description {
            task.description = description;
        }
        if let Some(priority) = input.priority {
            task.priority = priority;
        }
        if let Some(merge_config) = input.merge_config {
            task.merge_config = merge_config;
        }
        if let Some(plan) = input.plan {
            task.plan = plan;
        }
        if let Some(error_annotation) = input.error_annotation {
            task.error_annotation = error_annotation;
        }
        let blocked_json_update = input.blocked_json;
        let failed_json_update = input.failed_json;
        let set_blocked_json = blocked_json_update.is_some();
        let set_failed_json = failed_json_update.is_some();
        let clear_failed_json = matches!(blocked_json_update, Some(Some(_)));
        let clear_blocked_json = matches!(failed_json_update, Some(Some(_)));
        if let Some(blocked_json) = blocked_json_update {
            task.blocked_json = blocked_json;
            if task.blocked_json.is_some() {
                task.failed_json = None;
            }
        }
        if let Some(failed_json) = failed_json_update {
            task.failed_json = failed_json;
            if task.failed_json.is_some() {
                task.blocked_json = None;
            }
        }
        if let Some(task_state_config) = input.task_state_config {
            task.task_state_config = task_state_config;
        }
        if let Some(parent_task_id) = input.parent_task_id {
            task.parent_task_id = parent_task_id;
        }
        task.updated_at = input.updated_at;
        task.version += 1;
        let mut query = sqlx::QueryBuilder::<Sqlite>::new("UPDATE task SET title = ");
        query
            .push_bind(&task.title)
            .push(", description = ")
            .push_bind(task.description.as_deref())
            .push(", priority = ")
            .push_bind(task.priority)
            .push(", merge_config = ")
            .push_bind(task.merge_config.as_deref())
            .push(", plan = ")
            .push_bind(task.plan.as_deref())
            .push(", error_annotation = ")
            .push_bind(task.error_annotation.as_deref());
        if set_blocked_json || clear_blocked_json {
            query
                .push(", blocked_json = ")
                .push_bind(task.blocked_json.as_deref());
        }
        if set_failed_json || clear_failed_json {
            query
                .push(", failed_json = ")
                .push_bind(task.failed_json.as_deref());
        }
        query
            .push(", task_state_config = ")
            .push_bind(task.task_state_config.as_deref())
            .push(", parent_task_id = ")
            .push_bind(task.parent_task_id.as_deref())
            .push(", version = version + 1, updated_at = ")
            .push_bind(&task.updated_at)
            .push(" WHERE id = ")
            .push_bind(&task.id)
            .push(" AND version = ")
            .push_bind(input.expected_version)
            .push(" AND deleted_at IS NULL");
        let result = query.build().execute(&mut *transaction).await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        if interruption_fields_changed(
            &previous_error_annotation,
            &previous_blocked_json,
            &previous_failed_json,
            &task,
        ) {
            append_task_interruption_event(self, &mut transaction, &task).await?;
        }
        transaction.commit().await?;
        Ok(task)
    }

    async fn set_review_passed_at(
        &self,
        id: &str,
        review_passed_at: Option<String>,
        updated_at: &str,
    ) -> Result<Task> {
        let result = sqlx::query(
            "UPDATE task SET review_passed_at = ?, updated_at = ? WHERE id = ? AND deleted_at IS NULL",
        )
        .bind(review_passed_at.as_deref())
        .bind(updated_at)
        .bind(id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        TaskRepo::get_by_id(self, id, true)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn archive(&self, input: crate::ArchiveTask) -> Result<Task> {
        let mut task = self.get_task_required(&input.id, true).await?;
        if task.deleted_at.is_some() {
            return Err(DbError::InvalidSoftDelete);
        }
        if task.version != input.expected_version {
            return Err(DbError::VersionConflict);
        }
        task.archived_at = Some(input.archived_at);
        task.updated_at = input.updated_at;
        task.version += 1;
        let result = sqlx::query("UPDATE task SET archived_at = ?, version = version + 1, updated_at = ? WHERE id = ? AND version = ? AND deleted_at IS NULL")
            .bind(task.archived_at.as_deref())
            .bind(&task.updated_at)
            .bind(&task.id)
            .bind(input.expected_version)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        Ok(task)
    }

    async fn set_metadata_json(
        &self,
        id: &str,
        metadata_json: Option<String>,
        updated_at: &str,
    ) -> Result<()> {
        let result = sqlx::query(
            "UPDATE task SET metadata_json = ?, updated_at = ? WHERE id = ? AND deleted_at IS NULL",
        )
        .bind(metadata_json.as_deref())
        .bind(updated_at)
        .bind(id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        Ok(())
    }

    async fn set_entry_barrier(
        &self,
        id: &str,
        expected_version: i64,
        entry_barrier_json: Option<String>,
        updated_at: &str,
    ) -> Result<Task> {
        let result = sqlx::query(
            "UPDATE task SET entry_barrier_json = ?, version = version + 1, updated_at = ? WHERE id = ? AND version = ? AND deleted_at IS NULL",
        )
        .bind(entry_barrier_json.as_deref())
        .bind(updated_at)
        .bind(id)
        .bind(expected_version)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        TaskRepo::get_by_id(self, id, true)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn soft_delete(&self, input: crate::SoftDeleteTask) -> Result<Task> {
        let mut task = self.get_task_required(&input.id, true).await?;
        if task.deleted_at.is_some()
            || matches!(task.status.as_str(), "in_progress" | "review" | "merging")
        {
            return Err(DbError::InvalidSoftDelete);
        }
        if task.version != input.expected_version {
            return Err(DbError::VersionConflict);
        }
        task.deleted_at = Some(input.deleted_at);
        task.updated_at = input.updated_at;
        task.version += 1;
        let result = sqlx::query("UPDATE task SET deleted_at = ?, version = version + 1, updated_at = ? WHERE id = ? AND version = ? AND deleted_at IS NULL")
            .bind(task.deleted_at.as_deref())
            .bind(&task.updated_at)
            .bind(&task.id)
            .bind(input.expected_version)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        Ok(task)
    }

    async fn claim(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: ClaimTask,
    ) -> Result<ClaimedTask> {
        let sql = format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ? AND deleted_at IS NULL");
        let row = sqlx::query(&sql)
            .bind(&input.task_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        let mut task = map_task(row)?;
        if task.version != input.expected_version {
            return Err(DbError::VersionConflict);
        }
        if task.status != input.source_status {
            return Err(DbError::InvalidTransition);
        }
        if task.entry_barrier_json.is_some() {
            return Err(DbError::InvalidTransition);
        }
        if input.execution.status != ExecutionStatus::Running {
            return Err(DbError::InvalidTransition);
        }
        let initial_lease = &input.execution_lease;
        if initial_lease.execution_id != input.execution.id
            || initial_lease.expected_version != 1
            || initial_lease.owner.trim().is_empty()
            || initial_lease.lease_expires_at > initial_lease.hard_deadline_at
            || initial_lease.hard_deadline_at <= initial_lease.now
        {
            return Err(DbError::Check(
                "task claim requires a valid bounded initial execution lease".to_owned(),
            ));
        }
        // Claim and its Running execution are one transaction. Re-check the
        // Charter-backed execution admission here before mutating Task
        // assignment/status; the service's earlier read gate only avoids
        // unnecessary workspace side effects.
        if input.execution.status == ExecutionStatus::Running
            && input.execution.workspace_id.is_some()
        {
            Self::ensure_execution_admission_in_tx(transaction, &input.task_id).await?;
        }

        let assignee_agent_id = match input.assignee_type.as_str() {
            "agent" => {
                let agent_id = input
                    .execution
                    .agent_id
                    .as_deref()
                    .ok_or(DbError::InvalidTransition)?;
                Some(agent_id)
            }
            "user" => {
                if input.assignee_id.is_none() {
                    return Err(DbError::InvalidTransition);
                }
                None
            }
            _ => return Err(DbError::InvalidTransition),
        };

        let unsatisfied_dependencies =
            Self::unsatisfied_dependencies_in_tx(transaction, &input.task_id).await?;
        if !unsatisfied_dependencies.is_empty() && input.assignee_type == "agent" {
            let agent_id = assignee_agent_id.ok_or(DbError::InvalidTransition)?;
            let mut context_holder_match = false;
            for depends_on_id in &unsatisfied_dependencies {
                let context_holder = sqlx::query_scalar::<_, Option<String>>(
                    "SELECT agent_id FROM execution WHERE task_id = ? AND role = 'executor' ORDER BY created_at DESC LIMIT 1",
                )
                .bind(depends_on_id)
                .fetch_optional(&mut **transaction)
                .await?
                .flatten();
                if context_holder.as_deref() == Some(agent_id) {
                    context_holder_match = true;
                    break;
                }
            }
            if !context_holder_match {
                return Err(DbError::DependencyGate);
            }
        }

        if let Some(agent_id) = assignee_agent_id {
            let active_count = if input.capacity_statuses.is_empty() {
                0
            } else {
                let placeholders = vec!["?"; input.capacity_statuses.len()].join(", ");
                let sql = format!(
                    "SELECT
                        (
                            SELECT COUNT(DISTINCT task.id) FROM task
                            JOIN task_role_assignment ON task_role_assignment.task_id = task.id
                            WHERE task_role_assignment.assignee_type = 'agent'
                              AND task_role_assignment.assignee_id = ?
                              AND task.id != ?
                              AND task.deleted_at IS NULL
                              AND task.status IN ({placeholders})
                        ) +
                        (
                            SELECT COUNT(*) FROM agent_chat_turn_job
                            WHERE responder_identity_id = ?
                              AND status IN ('leased', 'running')
                        )"
                );
                let mut query = sqlx::query_scalar::<_, i64>(&sql)
                    .bind(agent_id)
                    .bind(&input.task_id);
                for status in &input.capacity_statuses {
                    query = query.bind(status);
                }
                query = query.bind(agent_id);
                query.fetch_one(&mut **transaction).await?
            };
            if active_count >= input.max_concurrent_tasks {
                return Err(DbError::AgentAtCapacity);
            }
        }

        let result = sqlx::query("UPDATE task SET assignee_type = ?, assignee_id = ?, status = ?, review_passed_at = NULL, entry_barrier_json = NULL, version = version + 1, updated_at = ? WHERE id = ? AND version = ? AND deleted_at IS NULL")
            .bind(&input.assignee_type)
            .bind(input.assignee_id.as_deref())
            .bind(&input.target_status)
            .bind(&input.claimed_at)
            .bind(&input.task_id)
            .bind(input.expected_version)
            .execute(&mut **transaction)
            .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        task.assignee_type = Some(input.assignee_type);
        task.assignee_id = input.assignee_id;
        task.status = input.target_status;
        task.review_passed_at = None;
        task.entry_barrier_json = None;
        task.version += 1;
        task.updated_at = input.claimed_at;
        let mut execution = Self::create_execution_in_tx(transaction, &input.execution).await?;
        let lease_result = sqlx::query(
            "UPDATE execution
             SET lease_owner = ?,
                 lease_expires_at = MIN(?, ?),
                 hard_deadline_at = ?,
                 last_heartbeat_at = ?,
                 execution_version = execution_version + 1,
                 updated_at = ?
             WHERE id = ? AND status = 'running'
               AND execution_version = ? AND lease_owner IS NULL",
        )
        .bind(&initial_lease.owner)
        .bind(&initial_lease.lease_expires_at)
        .bind(&initial_lease.hard_deadline_at)
        .bind(&initial_lease.hard_deadline_at)
        .bind(&initial_lease.now)
        .bind(&initial_lease.now)
        .bind(&initial_lease.execution_id)
        .bind(initial_lease.expected_version)
        .execute(&mut **transaction)
        .await?;
        if lease_result.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        execution.execution_version += 1;
        execution.lease_owner = Some(initial_lease.owner.clone());
        execution.lease_expires_at = Some(initial_lease.lease_expires_at.clone());
        execution.hard_deadline_at = Some(initial_lease.hard_deadline_at.clone());
        execution.last_heartbeat_at = Some(initial_lease.now.clone());
        execution.updated_at = initial_lease.now.clone();
        Ok(ClaimedTask { task, execution })
    }

    async fn update_status(&self, input: UpdateTaskStatus) -> Result<Task> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let task_row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
            .bind(&input.id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        let mut task = map_task(task_row)?;
        if task.deleted_at.is_some() {
            return Err(DbError::InvalidSoftDelete);
        }
        if task.version != input.expected_version {
            return Err(DbError::VersionConflict);
        }
        let previous_status = task.status.clone();
        let previous_error_annotation = task.error_annotation.clone();
        let previous_blocked_json = task.blocked_json.clone();
        let previous_failed_json = task.failed_json.clone();
        let target_status = input.status.clone();
        task.status = target_status;
        if input.assignee_id.is_some() {
            task.assignee_type = None;
            task.assignee_id = None;
        }
        if let Some(error_annotation) = input.error_annotation {
            task.error_annotation = error_annotation;
        }
        let blocked_json_update = input.blocked_json;
        let failed_json_update = input.failed_json;
        let set_blocked_json = blocked_json_update.is_some();
        let set_failed_json = failed_json_update.is_some();
        let clear_failed_json = matches!(blocked_json_update, Some(Some(_)));
        let clear_blocked_json = matches!(failed_json_update, Some(Some(_)));
        if let Some(blocked_json) = blocked_json_update {
            task.blocked_json = blocked_json;
            if task.blocked_json.is_some() {
                task.failed_json = None;
            }
        }
        if let Some(failed_json) = failed_json_update {
            task.failed_json = failed_json;
            if task.failed_json.is_some() {
                task.blocked_json = None;
            }
        }
        task.updated_at = input.updated_at;
        task.entry_barrier_json = None;
        task.version += 1;
        let mut query = sqlx::QueryBuilder::<Sqlite>::new("UPDATE task SET status = ");
        query
            .push_bind(&task.status)
            .push(", assignee_type = ")
            .push_bind(task.assignee_type.as_deref())
            .push(", assignee_id = ")
            .push_bind(task.assignee_id.as_deref())
            .push(", error_annotation = ")
            .push_bind(task.error_annotation.as_deref());
        if set_blocked_json || clear_blocked_json {
            query
                .push(", blocked_json = ")
                .push_bind(task.blocked_json.as_deref());
        }
        if set_failed_json || clear_failed_json {
            query
                .push(", failed_json = ")
                .push_bind(task.failed_json.as_deref());
        }
        query
            .push(", entry_barrier_json = NULL")
            .push(", version = version + 1, updated_at = ")
            .push_bind(&task.updated_at)
            .push(" WHERE id = ")
            .push_bind(&task.id)
            .push(" AND version = ")
            .push_bind(input.expected_version)
            .push(" AND deleted_at IS NULL");
        let result = query.build().execute(&mut *transaction).await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }

        if interruption_fields_changed(
            &previous_error_annotation,
            &previous_blocked_json,
            &previous_failed_json,
            &task,
        ) {
            append_task_interruption_event(self, &mut transaction, &task).await?;
        }

        let event_id = new_uuid_v4();
        let event = CreateDomainEvent {
            id: event_id.clone(),
            event_type: "task.status_changed".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: task.id.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "task".to_owned(),
            scope_id: task.id.clone(),
            correlation_id: event_id.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!("task-status-update:{}:{}", task.id, task.version)),
            payload_json: serde_json::json!({
                "from_status": previous_status,
                "to_status": task.status,
                "task_version": task.version,
            })
            .to_string(),
            created_at: task.updated_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut transaction, &event).await?;
        transaction.commit().await?;
        Ok(task)
    }
}

fn interruption_fields_changed(
    previous_error_annotation: &Option<String>,
    previous_blocked_json: &Option<String>,
    previous_failed_json: &Option<String>,
    task: &Task,
) -> bool {
    previous_error_annotation != &task.error_annotation
        || previous_blocked_json != &task.blocked_json
        || previous_failed_json != &task.failed_json
}

async fn append_task_interruption_event(
    db: &SqliteDb,
    transaction: &mut Transaction<'_, Sqlite>,
    task: &Task,
) -> Result<()> {
    let event = CreateDomainEvent::task_interruption_changed(task);
    DomainEventRepo::append_event_in_tx(db, transaction, &event).await?;
    Ok(())
}

fn search_like_pattern(term: &str) -> String {
    let mut pattern = String::with_capacity(term.len() + 2);
    pattern.push('%');
    for ch in term.to_lowercase().chars() {
        if matches!(ch, '%' | '_' | '\\') {
            pattern.push('\\');
        }
        pattern.push(ch);
    }
    pattern.push('%');
    pattern
}
