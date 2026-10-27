//! Task repository operations.

use super::*;

#[async_trait]
impl TaskRepo for SqliteDb {
    async fn count_project_slots(
        &self,
        project_id: &str,
        project_states_json: &str,
        subtask_states_json: &str,
    ) -> Result<(i64, i64, i64)> {
        // Every term is 0 or 1, so each CASE equals the boolean it wraps. The
        // CASE is for evaluation order: SQLite stops a CASE condition at the
        // first deciding operand, but as a plain result value it evaluates
        // every operand, running the review, child and execution probes even
        // for rows the earlier operands already decide.
        Ok(sqlx::query_as(
            "WITH project_states AS (
                SELECT key AS name, json_extract(value, '$.kind') AS kind,
                       json_extract(value, '$.owns_work') AS owns_work FROM json_each(?)
             ), subtask_states AS (
                SELECT key AS name, json_extract(value, '$.kind') AS kind,
                       json_extract(value, '$.owns_work') AS owns_work FROM json_each(?)
             ), visible AS MATERIALIZED (
                SELECT t.id, t.parent_task_id,
                       COALESCE(s.kind, p.kind) AS kind,
                       COALESCE(s.owns_work, p.owns_work, 0) AS owns_work,
                       CASE WHEN COALESCE(s.kind, p.kind) IN ('active', 'gate') AND
                       (json_extract(t.condition_json,'$.evidence.presentation.interruption_present') = 1
                        OR json_extract(t.condition_json,'$.evidence.presentation.hard_failure') = 1
                        OR (json_extract(t.condition_json,'$.evidence.presentation.refusal.capability') IN ('machine_capacity', 'project_capacity')
                            AND NOT EXISTS (SELECT 1 FROM execution e WHERE e.task_id = t.id AND e.status = 'running'))
                        OR json_extract(t.condition_json,'$.evidence.presentation.environment_recorded')=1
                        OR COALESCE(json_extract(t.condition_json,'$.evidence.presentation.slot_blocker'),0)
                        OR COALESCE(json_extract(t.condition_json,'$.evidence.presentation.review_wait'),0))
                       THEN 1 ELSE 0 END AS parked
                FROM task t
                LEFT JOIN subtask_states s ON t.parent_task_id IS NOT NULL AND s.name = t.status
                LEFT JOIN project_states p ON p.name = t.status
                WHERE t.project_id = ? AND t.archived_at IS NULL AND t.deleted_at IS NULL
                  AND COALESCE(s.kind, p.kind) IN ('initial', 'active', 'gate')
             )
             SELECT
               COALESCE(SUM(CASE WHEN kind IN ('active', 'gate') AND NOT parked AND (
                   parent_task_id IS NOT NULL
                   OR NOT EXISTS (SELECT 1 FROM task child WHERE child.parent_task_id = visible.id AND child.deleted_at IS NULL)
                   OR ((owns_work OR EXISTS (SELECT 1 FROM execution e WHERE e.task_id = visible.id AND e.status = 'running'))
                       AND NOT EXISTS (SELECT 1 FROM visible child WHERE child.parent_task_id = visible.id
                                       AND child.kind IN ('active', 'gate') AND NOT child.parked))
               ) THEN 1 ELSE 0 END), 0),
               COALESCE(SUM(kind IN ('active', 'gate') AND parked), 0),
               COALESCE(SUM(kind = 'initial'), 0)
             FROM visible",
        )
        .bind(project_states_json).bind(subtask_states_json).bind(project_id)
        .fetch_one(self.pool()).await?)
    }

    async fn count_projects_slots(
        &self,
        project_states_json: &str,
        subtask_states_json: &str,
    ) -> Result<Vec<ProjectSlotCounts>> {
        let rows = sqlx::query(
            "WITH projects AS MATERIALIZED (
                SELECT key AS id, value AS states FROM json_each(?)
             ), project_states AS (
                SELECT projects.id AS project_id, states.key AS name, json_extract(states.value, '$.kind') AS kind,
                       json_extract(states.value, '$.owns_work') AS owns_work FROM projects, json_each(projects.states) states
             ), subtask_states AS (
                SELECT key AS name, json_extract(value, '$.kind') AS kind,
                       json_extract(value, '$.owns_work') AS owns_work FROM json_each(?)
             ), visible AS MATERIALIZED (
                SELECT t.id, t.project_id, t.parent_task_id,
                       COALESCE(s.kind, p.kind) AS kind,
                       COALESCE(s.owns_work, p.owns_work, 0) AS owns_work,
                       CASE WHEN COALESCE(s.kind, p.kind) IN ('active', 'gate') AND
                       (json_extract(t.condition_json,'$.evidence.presentation.interruption_present') = 1
                        OR json_extract(t.condition_json,'$.evidence.presentation.hard_failure') = 1
                        OR (json_extract(t.condition_json,'$.evidence.presentation.refusal.capability') IN ('machine_capacity', 'project_capacity')
                            AND NOT EXISTS (SELECT 1 FROM execution e WHERE e.task_id = t.id AND e.status = 'running'))
                        OR json_extract(t.condition_json,'$.evidence.presentation.environment_recorded')=1
                        OR COALESCE(json_extract(t.condition_json,'$.evidence.presentation.slot_blocker'),0)
                        OR COALESCE(json_extract(t.condition_json,'$.evidence.presentation.review_wait'),0))
                       THEN 1 ELSE 0 END AS parked
                FROM projects JOIN task t ON t.project_id = projects.id
                LEFT JOIN subtask_states s ON t.parent_task_id IS NOT NULL AND s.name = t.status
                LEFT JOIN project_states p ON p.project_id = t.project_id AND p.name = t.status
                WHERE t.archived_at IS NULL AND t.deleted_at IS NULL
                  AND COALESCE(s.kind, p.kind) IN ('initial', 'active', 'gate')
             )
             SELECT project.id AS project_id, project.version AS project_version, project.list_revision,
               COALESCE(SUM(CASE WHEN kind IN ('active', 'gate') AND NOT parked AND (
                   parent_task_id IS NOT NULL
                   OR NOT EXISTS (SELECT 1 FROM task child WHERE child.parent_task_id = visible.id AND child.deleted_at IS NULL)
                   OR ((owns_work OR EXISTS (SELECT 1 FROM execution e WHERE e.task_id = visible.id AND e.status = 'running'))
                       AND NOT EXISTS (SELECT 1 FROM visible child WHERE child.parent_task_id = visible.id
                                       AND child.project_id = visible.project_id
                                       AND child.kind IN ('active', 'gate') AND NOT child.parked))
               ) THEN 1 ELSE 0 END), 0) AS active,
               COALESCE(SUM(kind IN ('active', 'gate') AND parked), 0) AS parked,
               COALESCE(SUM(kind = 'initial'), 0) AS queued
             FROM projects JOIN project ON project.id = projects.id
             LEFT JOIN visible ON visible.project_id = projects.id
             GROUP BY project.id",
        )
        .bind(project_states_json)
        .bind(subtask_states_json)
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(ProjectSlotCounts {
                    project_id: row.try_get("project_id")?,
                    project_version: row.try_get("project_version")?,
                    list_revision: row.try_get("list_revision")?,
                    active: row.try_get("active")?,
                    parked: row.try_get("parked")?,
                    queued: row.try_get("queued")?,
                })
            })
            .collect()
    }

    async fn create(&self, input: CreateTask) -> Result<Task> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        self.fence_current_step_in_tx(&mut transaction).await?;
        let task = TaskRepo::create_in_tx(self, &mut transaction, input).await?;
        self.record_mutation_reply_in_tx(&mut transaction, &task)
            .await?;

        transaction.commit().await?;
        Ok(task)
    }

    async fn create_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateTask,
    ) -> Result<Task> {
        sqlx::query("INSERT INTO task (id, project_id, parent_task_id, assignee_type, assignee_id, title, description, task_type, status, is_automation, priority, board_position, subtask_order, task_state_config, merge_config, metadata_json, plan, created_at, updated_at) SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(MAX(board_position), 0.0) + 1.0, ?, ?, ?, ?, ?, ?, ? FROM task WHERE project_id = ?")
            .bind(&input.id)
            .bind(&input.project_id)
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
        let mut task = map_task(row)?;
        // A new row carries the column default: state every family once.
        crate::task_condition::produce(transaction, &task.id, crate::ConditionChange::Full).await?;
        crate::task_condition::refresh_task_condition(transaction, &mut task).await?;
        if let Some(parent) = &task.parent_task_id {
            crate::task_condition::produce_children(transaction, parent).await?;
        }
        Ok(task)
    }

    async fn get_by_id(&self, id: &str, include_deleted: bool) -> Result<Option<Task>> {
        load_task(&self.pool, id, include_deleted).await
    }

    async fn get_by_id_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        id: &str,
        include_deleted: bool,
    ) -> Result<Option<Task>> {
        load_task(&mut **transaction, id, include_deleted).await
    }

    async fn list(&self, query: TaskListQuery) -> Result<Page<Task>> {
        let mut connection = self.pool.acquire().await?;
        list_page(&mut connection, query).await
    }

    async fn list_by_project_with_metadata_key(
        &self,
        project_id: &str,
        key: &str,
    ) -> Result<Vec<Task>> {
        let path = format!("$.{key}");
        let sql = format!(
            "SELECT {TASK_COLUMNS} FROM task
             WHERE project_id = ? AND deleted_at IS NULL
               AND json_valid(metadata_json)
               AND json_type(metadata_json, ?) IS NOT NULL
             ORDER BY created_at ASC, id ASC"
        );
        let rows = sqlx::query(&sql)
            .bind(project_id)
            .bind(path)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(map_task).collect()
    }

    async fn list_by_executing_agent(&self, query: AgentTaskListQuery) -> Result<Page<Task>> {
        let offset = decode_offset(&query.page.cursor)?;
        let mut where_parts =
            vec!["id IN (SELECT DISTINCT task_id FROM execution WHERE agent_id = ?)".to_owned()];
        where_parts.push("EXISTS (SELECT 1 FROM project WHERE project.id = task.project_id AND (project.owner_id IS NULL OR project.owner_id = ? OR EXISTS (SELECT 1 FROM project_member WHERE project_member.project_id = project.id AND project_member.user_id = ?)))".to_owned());
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
            .bind(&query.user_id)
            .bind(&query.user_id)
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
                    .bind(&query.user_id)
                    .bind(&query.user_id)
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
        self.fence_current_step_in_tx(&mut transaction).await?;
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
        // A new order cannot make an earlier child depend (even transitively)
        // on one that the same order requires to wait for it. Roll back the
        // whole order if the implicit waits close a prerequisite cycle.
        let ids: Vec<&str> = ordered_ids.iter().map(String::as_str).collect();
        if crate::sqlite::task_dependency::dependency_cycle_any_in_tx(&mut transaction, &ids, None)
            .await?
        {
            return Err(DbError::CycleDetected);
        }
        // Child witnesses follow the sequence order.
        crate::task_condition::produce_children(&mut transaction, parent_task_id).await?;

        transaction.commit().await?;
        Ok(())
    }

    async fn update(&self, input: UpdateTask) -> Result<Task> {
        if !crate::task_writer::owns_task(&input.id)
            && (input.error_annotation.is_some()
                || input.blocked_json.is_some()
                || input.failed_json.is_some()
                || input.parent_task_id.is_some())
        {
            return self
                .run_task_mutation(
                    &input.id,
                    crate::TaskMutation::TaskUpdate {
                        input: input.clone(),
                    },
                )
                .await;
        }

        update_task_inner(self, input, None, None).await
    }

    async fn update_with_recovery_marker(
        &self,
        input: UpdateTask,
        marker: CreateTransitionLog,
    ) -> Result<Task> {
        if !crate::task_writer::owns_task(&input.id) {
            return self
                .run_task_mutation(
                    &input.id,
                    crate::TaskMutation::TaskUpdateWithRecoveryMarker {
                        input: input.clone(),
                        marker: marker.clone(),
                    },
                )
                .await;
        }

        update_task_inner(self, input, Some(&marker), None).await
    }

    async fn update_with_workflow_authority(
        &self,
        input: UpdateTask,
        expected_project_version: i64,
        expected_workflow_definition: String,
    ) -> Result<Task> {
        if !crate::task_writer::owns_task(&input.id) {
            return self
                .run_task_mutation(
                    &input.id,
                    crate::TaskMutation::TaskUpdateWithWorkflowAuthority {
                        input: input.clone(),
                        expected_project_version,
                        expected_workflow_definition: expected_workflow_definition.clone(),
                    },
                )
                .await;
        }

        update_task_inner(
            self,
            input,
            None,
            Some((expected_project_version, expected_workflow_definition)),
        )
        .await
    }

    async fn update_with_workflow_authority_and_recovery_marker(
        &self,
        input: UpdateTask,
        marker: CreateTransitionLog,
        expected_project_version: i64,
        expected_workflow_definition: String,
    ) -> Result<Task> {
        if !crate::task_writer::owns_task(&input.id) {
            return self
                .run_task_mutation(
                    &input.id,
                    crate::TaskMutation::TaskUpdateWithWorkflowAuthorityAndRecoveryMarker {
                        input: input.clone(),
                        marker: marker.clone(),
                        expected_project_version,
                        expected_workflow_definition: expected_workflow_definition.clone(),
                    },
                )
                .await;
        }

        update_task_inner(
            self,
            input,
            Some(&marker),
            Some((expected_project_version, expected_workflow_definition)),
        )
        .await
    }

    async fn set_error_annotation_if_no_running_execution(
        &self,
        id: &str,
        expected_version: i64,
        expected_status: &str,
        expected_state_entry_token: Option<&str>,
        expected_workflow_definition: &str,
        expected_assignment_role: Option<&str>,
        expected_assignment: Option<TaskRoleAssignment>,
        annotation: &str,
        updated_at: &str,
        stopped_execution_id: &str,
        workspace_id: Option<&str>,
        overlapping_roles: Vec<String>,
    ) -> Result<Task> {
        if !crate::task_writer::owns_task(id) {
            return self
                .run_task_mutation(
                    id,
                    crate::TaskMutation::TaskSetErrorAnnotationIfNoRunningExecution {
                        id: id.to_owned(),
                        expected_version,
                        expected_status: expected_status.to_owned(),
                        expected_state_entry_token: expected_state_entry_token.map(str::to_owned),
                        expected_workflow_definition: expected_workflow_definition.to_owned(),
                        expected_assignment_role: expected_assignment_role.map(str::to_owned),
                        expected_assignment: expected_assignment.clone(),
                        annotation: annotation.to_owned(),
                        updated_at: updated_at.to_owned(),
                        stopped_execution_id: stopped_execution_id.to_owned(),
                        workspace_id: workspace_id.map(str::to_owned),
                        overlapping_roles: overlapping_roles.clone(),
                    },
                )
                .await;
        }

        set_error_annotation_if_no_running_execution_inner(
            self,
            id,
            expected_version,
            expected_status,
            expected_state_entry_token,
            expected_workflow_definition,
            expected_assignment_role,
            expected_assignment.as_ref(),
            annotation,
            updated_at,
            stopped_execution_id,
            workspace_id,
            overlapping_roles,
        )
        .await
    }

    async fn update_recovery_metadata_if_no_running_execution(
        &self,
        id: &str,
        expected_version: i64,
        error_annotation: Option<String>,
        blocked_json: Option<String>,
        failed_json: Option<String>,
        updated_at: &str,
        workspace_id: Option<&str>,
        overlapping_roles: Vec<String>,
        metadata_mutations: Vec<TaskMetadataMutation>,
        condition: Option<crate::ConditionStatement>,
    ) -> Result<Task> {
        if !crate::task_writer::owns_task(id) {
            return self
                .run_task_mutation(
                    id,
                    crate::TaskMutation::TaskUpdateRecoveryMetadataIfNoRunningExecution {
                        condition: condition.clone(),
                        id: id.to_owned(),
                        expected_version,
                        error_annotation: error_annotation.clone(),
                        blocked_json: blocked_json.clone(),
                        failed_json: failed_json.clone(),
                        updated_at: updated_at.to_owned(),
                        workspace_id: workspace_id.map(str::to_owned),
                        overlapping_roles: overlapping_roles.clone(),
                        metadata_mutations: metadata_mutations.clone(),
                    },
                )
                .await;
        }

        update_recovery_metadata_inner(
            self,
            id,
            expected_version,
            error_annotation,
            blocked_json,
            failed_json,
            updated_at,
            workspace_id,
            overlapping_roles,
            metadata_mutations,
            None,
            condition,
        )
        .await
    }

    async fn restore_queued_recovery(&self, input: RestoreQueuedRecovery) -> Result<Task> {
        if !crate::task_writer::owns_task(&input.task_id) {
            return self
                .run_task_mutation(
                    &input.task_id,
                    crate::TaskMutation::TaskRestoreQueuedRecovery {
                        input: input.clone(),
                    },
                )
                .await;
        }

        update_recovery_metadata_inner(
            self,
            &input.task_id,
            input.expected_version,
            input.error_annotation,
            input.blocked_json,
            input.failed_json,
            &input.updated_at,
            None,
            Vec::new(),
            vec![
                TaskMetadataMutation::Remove {
                    key: "queued_recovery".to_owned(),
                },
                TaskMetadataMutation::Remove {
                    key: "deferred_dispatch".to_owned(),
                },
            ],
            Some(&input.queued_recovery_id),
            None,
        )
        .await
    }

    async fn set_review_passed_at(
        &self,
        id: &str,
        review_passed_at: Option<String>,
        updated_at: &str,
    ) -> Result<Task> {
        if !crate::task_writer::owns_task(id) {
            return self
                .run_task_mutation(
                    id,
                    crate::TaskMutation::TaskSetReviewPassedAt {
                        id: id.to_owned(),
                        review_passed_at: review_passed_at.clone(),
                        updated_at: updated_at.to_owned(),
                    },
                )
                .await;
        }

        set_review_passed_at_inner(self, id, None, review_passed_at, None, updated_at).await
    }

    async fn set_review_passed_at_cas(
        &self,
        id: &str,
        expected_version: i64,
        review_passed_at: Option<String>,
        updated_at: &str,
    ) -> Result<Task> {
        if !crate::task_writer::owns_task(id) {
            return self
                .run_task_mutation(
                    id,
                    crate::TaskMutation::TaskSetReviewPassedAtCas {
                        id: id.to_owned(),
                        expected_version,
                        review_passed_at: review_passed_at.clone(),
                        updated_at: updated_at.to_owned(),
                    },
                )
                .await;
        }

        let expected_review_updated_at = review_passed_at.as_ref().map(|_| updated_at);
        set_review_passed_at_inner(
            self,
            id,
            Some(expected_version),
            review_passed_at,
            expected_review_updated_at,
            updated_at,
        )
        .await
    }

    async fn set_review_passed_at_cas_for_review(
        &self,
        id: &str,
        expected_version: i64,
        review_passed_at: Option<String>,
        expected_review_updated_at: &str,
        updated_at: &str,
    ) -> Result<Task> {
        if !crate::task_writer::owns_task(id) {
            return self
                .run_task_mutation(
                    id,
                    crate::TaskMutation::TaskSetReviewPassedAtCasForReview {
                        id: id.to_owned(),
                        expected_version,
                        review_passed_at: review_passed_at.clone(),
                        expected_review_updated_at: expected_review_updated_at.to_owned(),
                        updated_at: updated_at.to_owned(),
                    },
                )
                .await;
        }

        set_review_passed_at_inner(
            self,
            id,
            Some(expected_version),
            review_passed_at,
            Some(expected_review_updated_at),
            updated_at,
        )
        .await
    }

    async fn mutate_metadata(
        &self,
        id: &str,
        expected_version: Option<i64>,
        mutations: Vec<TaskMetadataMutation>,
        updated_at: &str,
    ) -> Result<Task> {
        if !crate::task_writer::owns_task(id) {
            return self
                .run_task_mutation(
                    id,
                    crate::TaskMutation::TaskMutateMetadata {
                        id: id.to_owned(),
                        expected_version,
                        mutations: mutations.clone(),
                        updated_at: updated_at.to_owned(),
                    },
                )
                .await;
        }

        let (task, _) = self
            .mutate_metadata_with_change(id, expected_version, mutations, updated_at)
            .await?;
        Ok(task)
    }

    async fn mutate_metadata_with_change(
        &self,
        id: &str,
        expected_version: Option<i64>,
        mutations: Vec<TaskMetadataMutation>,
        updated_at: &str,
    ) -> Result<(Task, bool)> {
        if !crate::task_writer::owns_task(id) {
            return self
                .run_task_mutation(
                    id,
                    crate::TaskMutation::TaskMutateMetadataWithChange {
                        id: id.to_owned(),
                        expected_version,
                        mutations: mutations.clone(),
                        updated_at: updated_at.to_owned(),
                    },
                )
                .await;
        }

        let mut transaction = crate::begin_immediate(&self.pool).await?;
        self.fence_current_step_in_tx(&mut transaction).await?;
        let task_row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        let mut task = map_task(task_row)?;
        if task.deleted_at.is_some() {
            return Err(DbError::NotFound);
        }
        if !crate::task_writer::owns_task(id)
            && expected_version.is_some_and(|expected| task.version != expected)
        {
            return Err(DbError::VersionConflict);
        }

        let mut metadata = TaskMetadata::parse(task.metadata_json.as_deref())
            .map_err(|error| DbError::Check(format!("invalid task metadata: {error}")))?;
        let condition_changed = crate::task_condition::metadata_changes_condition(&mutations);
        let mut changed = false;
        for mutation in mutations {
            changed |=
                apply_task_mutation(&mut transaction, &task.id, &mut metadata, mutation).await?;
        }
        if !changed {
            transaction.commit().await?;
            return Ok((task, false));
        }
        let metadata_json = metadata.to_json();
        let condition = if condition_changed {
            crate::task_condition::metadata_condition(
                &mut transaction,
                id,
                metadata_json.as_deref(),
            )
            .await?
        } else {
            None
        };
        sqlx::query(
            "UPDATE task
             SET metadata_json = ?, condition_json = CASE WHEN ? THEN ? ELSE condition_json END, condition_digest = CASE WHEN ? THEN ? ELSE condition_digest END, updated_at = ?
             WHERE id = ? AND deleted_at IS NULL",
        )
        .bind(metadata_json.as_deref())
        .bind(condition.is_some())
        .bind(condition.as_ref().map(|(body, _)| body))
        .bind(condition.is_some())
        .bind(condition.as_ref().map(|(_, digest)| digest))
        .bind(updated_at)
        .bind(id)
        .execute(&mut *transaction)
        .await?;
        if let Some((body, digest)) = condition {
            task.condition = crate::task_condition::decode(&body)?;
            task.condition_digest = Some(digest);
        }
        task.metadata_json = metadata_json;
        task.updated_at = updated_at.to_owned();
        self.record_mutation_reply_in_tx(&mut transaction, &(&task, true))
            .await?;

        transaction.commit().await?;
        Ok((task, true))
    }

    async fn mutate_metadata_and_bump_version(
        &self,
        id: &str,
        expected_version: i64,
        mutations: Vec<TaskMetadataMutation>,
        updated_at: &str,
    ) -> Result<Task> {
        if !crate::task_writer::owns_task(id) {
            return self
                .run_task_mutation(
                    id,
                    crate::TaskMutation::TaskMutateMetadataAndBumpVersion {
                        id: id.to_owned(),
                        expected_version,
                        mutations: mutations.clone(),
                        updated_at: updated_at.to_owned(),
                    },
                )
                .await;
        }

        let mut transaction = crate::begin_immediate(&self.pool).await?;
        self.fence_current_step_in_tx(&mut transaction).await?;
        let task_row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        let mut task = map_task(task_row)?;
        if task.deleted_at.is_some() {
            return Err(DbError::NotFound);
        }
        let expected_version = if crate::task_writer::owns_task(&task.id) {
            task.version
        } else {
            expected_version
        };
        if task.version != expected_version {
            return Err(DbError::VersionConflict);
        }

        let mut metadata = TaskMetadata::parse(task.metadata_json.as_deref())
            .map_err(|error| DbError::Check(format!("invalid task metadata: {error}")))?;
        let condition_changed = crate::task_condition::metadata_changes_condition(&mutations);
        let mut changed = false;
        for mutation in mutations {
            changed |=
                apply_task_mutation(&mut transaction, &task.id, &mut metadata, mutation).await?;
        }
        if !changed {
            transaction.commit().await?;
            return Ok(task);
        }

        let metadata_json = metadata.to_json();
        let condition = if condition_changed {
            crate::task_condition::metadata_condition(
                &mut transaction,
                id,
                metadata_json.as_deref(),
            )
            .await?
        } else {
            None
        };
        let result = sqlx::query(
            "UPDATE task
             SET metadata_json = ?, condition_json = CASE WHEN ? THEN ? ELSE condition_json END, condition_digest = CASE WHEN ? THEN ? ELSE condition_digest END, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND deleted_at IS NULL",
        )
        .bind(metadata_json.as_deref())
        .bind(condition.is_some())
        .bind(condition.as_ref().map(|(body, _)| body))
        .bind(condition.is_some())
        .bind(condition.as_ref().map(|(_, digest)| digest))
        .bind(updated_at)
        .bind(id)
        .bind(expected_version)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        if let Some((body, digest)) = condition {
            task.condition = crate::task_condition::decode(&body)?;
            task.condition_digest = Some(digest);
        }
        task.metadata_json = metadata_json;
        task.version += 1;
        task.updated_at = updated_at.to_owned();
        self.record_mutation_reply_in_tx(&mut transaction, &task)
            .await?;

        transaction.commit().await?;
        Ok(task)
    }

    async fn mutate_metadata_and_bump_version_with_project_authority(
        &self,
        id: &str,
        expected_version: i64,
        expected_project_version: i64,
        mutations: Vec<TaskMetadataMutation>,
        updated_at: &str,
    ) -> Result<Task> {
        if !crate::task_writer::owns_task(id) {
            return self
                .run_task_mutation(
                    id,
                    crate::TaskMutation::TaskMutateMetadataAndBumpVersionWithProjectAuthority {
                        id: id.to_owned(),
                        expected_version,
                        expected_project_version,
                        mutations: mutations.clone(),
                        updated_at: updated_at.to_owned(),
                    },
                )
                .await;
        }

        let mut transaction = crate::begin_immediate(&self.pool).await?;
        self.fence_current_step_in_tx(&mut transaction).await?;
        let task_row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        let mut task = map_task(task_row)?;
        if task.deleted_at.is_some() {
            return Err(DbError::NotFound);
        }
        let expected_version = if crate::task_writer::owns_task(&task.id) {
            task.version
        } else {
            expected_version
        };
        if task.version != expected_version {
            return Err(DbError::VersionConflict);
        }
        let project_version =
            sqlx::query_scalar::<_, i64>("SELECT version FROM project WHERE id = ?")
                .bind(&task.project_id)
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or(DbError::NotFound)?;
        if project_version != expected_project_version {
            return Err(DbError::VersionConflict);
        }

        let mut metadata = TaskMetadata::parse(task.metadata_json.as_deref())
            .map_err(|error| DbError::Check(format!("invalid task metadata: {error}")))?;
        let condition_changed = crate::task_condition::metadata_changes_condition(&mutations);
        let mut changed = false;
        for mutation in mutations {
            changed |=
                apply_task_mutation(&mut transaction, &task.id, &mut metadata, mutation).await?;
        }
        if !changed {
            transaction.commit().await?;
            return Ok(task);
        }

        let metadata_json = metadata.to_json();
        let condition = if condition_changed {
            crate::task_condition::metadata_condition(
                &mut transaction,
                id,
                metadata_json.as_deref(),
            )
            .await?
        } else {
            None
        };
        let result = sqlx::query(
            "UPDATE task
             SET metadata_json = ?, condition_json = CASE WHEN ? THEN ? ELSE condition_json END, condition_digest = CASE WHEN ? THEN ? ELSE condition_digest END, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND deleted_at IS NULL",
        )
        .bind(metadata_json.as_deref())
        .bind(condition.is_some())
        .bind(condition.as_ref().map(|(body, _)| body))
        .bind(condition.is_some())
        .bind(condition.as_ref().map(|(_, digest)| digest))
        .bind(updated_at)
        .bind(id)
        .bind(expected_version)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        if let Some((body, digest)) = condition {
            task.condition = crate::task_condition::decode(&body)?;
            task.condition_digest = Some(digest);
        }
        task.metadata_json = metadata_json;
        task.version += 1;
        task.updated_at = updated_at.to_owned();
        self.record_mutation_reply_in_tx(&mut transaction, &task)
            .await?;

        transaction.commit().await?;
        Ok(task)
    }

    async fn mutate_metadata_and_bump_version_for_latest_execution(
        &self,
        id: &str,
        expected_version: i64,
        authority: LatestExecutionAuthority,
        mutations: Vec<TaskMetadataMutation>,
        updated_at: &str,
    ) -> Result<Option<Task>> {
        if !crate::task_writer::owns_task(id) {
            return self
                .run_task_mutation(
                    id,
                    crate::TaskMutation::TaskMutateMetadataAndBumpVersionForLatestExecution {
                        id: id.to_owned(),
                        expected_version,
                        authority: authority.clone(),
                        mutations: mutations.clone(),
                        updated_at: updated_at.to_owned(),
                    },
                )
                .await;
        }

        let mut transaction = crate::begin_immediate(&self.pool).await?;
        self.fence_current_step_in_tx(&mut transaction).await?;
        let task_row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        let mut task = map_task(task_row)?;
        if task.deleted_at.is_some() {
            return Err(DbError::NotFound);
        }
        let expected_version = if crate::task_writer::owns_task(&task.id) {
            task.version
        } else {
            expected_version
        };
        if task.version != expected_version {
            return Err(DbError::VersionConflict);
        }
        if !latest_execution_authority_matches_in_tx(&mut transaction, &task, &authority).await? {
            transaction.commit().await?;
            return Ok(None);
        }

        let mut metadata = TaskMetadata::parse(task.metadata_json.as_deref())
            .map_err(|error| DbError::Check(format!("invalid task metadata: {error}")))?;
        let condition_changed = crate::task_condition::metadata_changes_condition(&mutations);
        let mut changed = false;
        for mutation in mutations {
            changed |=
                apply_task_mutation(&mut transaction, &task.id, &mut metadata, mutation).await?;
        }
        // Retry admission and its resume policy are one durable effect. A
        // process crash must not leave a deferred Task behind a manual stop.
        changed |= restore_recorded_retry_policy_in_tx(
            &mut transaction,
            &task,
            &metadata,
            &authority,
            updated_at,
        )
        .await?;
        if !changed {
            transaction.commit().await?;
            return Ok(Some(task));
        }
        let metadata_json = metadata.to_json();
        let condition = if condition_changed {
            crate::task_condition::metadata_condition(
                &mut transaction,
                id,
                metadata_json.as_deref(),
            )
            .await?
        } else {
            None
        };
        let result = sqlx::query(
            "UPDATE task
             SET metadata_json = ?, condition_json = CASE WHEN ? THEN ? ELSE condition_json END, condition_digest = CASE WHEN ? THEN ? ELSE condition_digest END, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND deleted_at IS NULL",
        )
        .bind(metadata_json.as_deref())
        .bind(condition.is_some())
        .bind(condition.as_ref().map(|(body, _)| body))
        .bind(condition.is_some())
        .bind(condition.as_ref().map(|(_, digest)| digest))
        .bind(updated_at)
        .bind(id)
        .bind(expected_version)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        if let Some((body, digest)) = condition {
            task.condition = crate::task_condition::decode(&body)?;
            task.condition_digest = Some(digest);
        }
        task.metadata_json = metadata_json;
        task.version += 1;
        task.updated_at = updated_at.to_owned();
        transaction.commit().await?;
        Ok(Some(task))
    }

    async fn claim_metadata_for_latest_execution(
        &self,
        mut input: LatestExecutionMetadataClaim,
    ) -> Result<Task> {
        if !crate::task_writer::owns_task(&input.task_id) {
            return self
                .run_task_mutation(
                    &input.task_id,
                    crate::TaskMutation::TaskClaimMetadataForLatestExecution {
                        input: input.clone(),
                    },
                )
                .await;
        }

        let mut transaction = crate::begin_immediate(&self.pool).await?;
        self.fence_current_step_in_tx(&mut transaction).await?;
        let task_row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
            .bind(&input.task_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        let mut task = map_task(task_row)?;
        if task.deleted_at.is_some() {
            return Err(DbError::NotFound);
        }
        input.expected_task_version = task.version;
        if task.version != input.expected_task_version {
            return Err(DbError::VersionConflict);
        }
        if !latest_execution_authority_matches_in_tx(&mut transaction, &task, &input.authority)
            .await?
        {
            return Err(DbError::VersionConflict);
        }

        let mut metadata = TaskMetadata::parse(task.metadata_json.as_deref())
            .map_err(|error| DbError::Check(format!("invalid task metadata: {error}")))?;
        if metadata.extra.contains_key(&input.key) {
            return Err(DbError::VersionConflict);
        }
        metadata.extra.insert(input.key, input.value);
        let metadata_json = metadata.to_json();
        let condition = crate::task_condition::metadata_condition(
            &mut transaction,
            &input.task_id,
            metadata_json.as_deref(),
        )
        .await?;
        let result = sqlx::query(
            "UPDATE task
             SET metadata_json = ?, condition_json = CASE WHEN ? THEN ? ELSE condition_json END, condition_digest = CASE WHEN ? THEN ? ELSE condition_digest END, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND deleted_at IS NULL",
        )
        .bind(metadata_json.as_deref())
        .bind(condition.is_some())
        .bind(condition.as_ref().map(|(body, _)| body))
        .bind(condition.is_some())
        .bind(condition.as_ref().map(|(_, digest)| digest))
        .bind(&input.updated_at)
        .bind(&input.task_id)
        .bind(input.expected_task_version)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        if let Some((body, digest)) = condition {
            task.condition = crate::task_condition::decode(&body)?;
            task.condition_digest = Some(digest);
        }
        task.metadata_json = metadata_json;
        task.version += 1;
        task.updated_at = input.updated_at;
        self.record_mutation_reply_in_tx(&mut transaction, &task)
            .await?;

        transaction.commit().await?;
        Ok(task)
    }

    async fn wake_dispatch_for_task(&self, id: &str, updated_at: &str) -> Result<Task> {
        if !crate::task_writer::owns_task(id) {
            return self
                .run_task_mutation(
                    id,
                    crate::TaskMutation::TaskWakeDispatchForTask {
                        id: id.to_owned(),
                        updated_at: updated_at.to_owned(),
                    },
                )
                .await;
        }

        let mut transaction = crate::begin_immediate(&self.pool).await?;
        self.fence_current_step_in_tx(&mut transaction).await?;
        let task_row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        let task = map_task(task_row)?;
        if task.deleted_at.is_some() {
            return Err(DbError::NotFound);
        }

        let mut metadata = TaskMetadata::parse(task.metadata_json.as_deref())
            .map_err(|error| DbError::Check(format!("invalid task metadata: {error}")))?;
        let disposition_changed = apply_metadata_mutation(
            &mut metadata,
            TaskMetadataMutation::Remove {
                key: "dispatch_disposition".to_owned(),
            },
        )?;
        let deferred_changed = apply_metadata_mutation(
            &mut metadata,
            TaskMetadataMutation::Remove {
                key: "deferred_dispatch".to_owned(),
            },
        )?;
        let changed = disposition_changed || deferred_changed;
        if !changed {
            transaction.commit().await?;
            return Ok(task);
        }

        let metadata_json = metadata.to_json();
        let condition = crate::task_condition::metadata_condition(
            &mut transaction,
            id,
            metadata_json.as_deref(),
        )
        .await?;
        let result = sqlx::query(
            "UPDATE task
             SET metadata_json = ?, condition_json = CASE WHEN ? THEN ? ELSE condition_json END, condition_digest = CASE WHEN ? THEN ? ELSE condition_digest END, updated_at = ?, version = version + 1
             WHERE id = ? AND deleted_at IS NULL",
        )
        .bind(metadata_json.as_deref())
        .bind(condition.is_some())
        .bind(condition.as_ref().map(|(body, _)| body))
        .bind(condition.is_some())
        .bind(condition.as_ref().map(|(_, digest)| digest))
        .bind(updated_at)
        .bind(id)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        let updated_row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
            .bind(id)
            .fetch_one(&mut *transaction)
            .await?;
        let updated_task = map_task(updated_row)?;
        transaction.commit().await?;
        Ok(updated_task)
    }

    async fn wake_dispatch_for_project(&self, project_id: &str, updated_at: &str) -> Result<u64> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        self.fence_current_step_in_tx(&mut transaction).await?;
        let changed =
            wake_dispatch_for_project_in_tx(self, &mut transaction, project_id, updated_at).await?;
        transaction.commit().await?;
        Ok(changed)
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

    async fn set_entry_barrier(
        &self,
        id: &str,
        expected_version: i64,
        entry_barrier_json: Option<String>,
        updated_at: &str,
    ) -> Result<Task> {
        if !crate::task_writer::owns_task(id) {
            return self
                .run_task_mutation(
                    id,
                    crate::TaskMutation::TaskSetEntryBarrier {
                        id: id.to_owned(),
                        expected_version,
                        entry_barrier_json: entry_barrier_json.clone(),
                        updated_at: updated_at.to_owned(),
                    },
                )
                .await;
        }

        let mut transaction = crate::begin_immediate(&self.pool).await?;
        self.fence_current_step_in_tx(&mut transaction).await?;
        if entry_barrier_json.is_none() {
            crate::budget::reset(
                &mut transaction,
                id,
                crate::budget::Kind::ReviewCiInfrastructure.key(),
                &format!("ci-success:{updated_at}"),
            )
            .await?;
        }
        let result = sqlx::query(
            "UPDATE task SET entry_barrier_json = ?, version = version + 1, updated_at = ? WHERE id = ? AND version = ? AND deleted_at IS NULL",
        )
        .bind(entry_barrier_json.as_deref())
        .bind(updated_at)
        .bind(id)
        .bind(expected_version)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        let mut task = self
            .get_task_in_tx(&mut transaction, id)
            .await?
            .ok_or(DbError::NotFound)?;
        crate::task_condition::produce(&mut transaction, &task.id, crate::ConditionChange::Legacy)
            .await?;
        crate::task_condition::refresh_task_condition(&mut transaction, &mut task).await?;
        transaction.commit().await?;
        Ok(task)
    }

    async fn set_entry_barrier_with_workflow_authority(
        &self,
        id: &str,
        expected_version: i64,
        entry_barrier_json: Option<String>,
        updated_at: &str,
        expected_project_version: i64,
        expected_workflow_definition: String,
    ) -> Result<Task> {
        if !crate::task_writer::owns_task(id) {
            return self
                .run_task_mutation(
                    id,
                    crate::TaskMutation::TaskSetEntryBarrierWithWorkflowAuthority {
                        id: id.to_owned(),
                        expected_version,
                        entry_barrier_json: entry_barrier_json.clone(),
                        updated_at: updated_at.to_owned(),
                        expected_project_version,
                        expected_workflow_definition: expected_workflow_definition.clone(),
                    },
                )
                .await;
        }

        let mut transaction = crate::begin_immediate(&self.pool).await?;
        self.fence_current_step_in_tx(&mut transaction).await?;
        check_project_workflow_authority_in_tx(
            &mut transaction,
            id,
            expected_project_version,
            &expected_workflow_definition,
        )
        .await?;
        if entry_barrier_json.is_none() {
            crate::budget::reset(
                &mut transaction,
                id,
                crate::budget::Kind::ReviewCiInfrastructure.key(),
                &format!("ci-success:{updated_at}"),
            )
            .await?;
        }
        let result = sqlx::query(
            "UPDATE task SET entry_barrier_json = ?, version = version + 1, updated_at = ? WHERE id = ? AND version = ? AND deleted_at IS NULL",
        )
        .bind(entry_barrier_json.as_deref())
        .bind(updated_at)
        .bind(id)
        .bind(expected_version)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        let row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
            .bind(id)
            .fetch_one(&mut *transaction)
            .await?;
        let mut task = map_task(row)?;
        crate::task_condition::produce(&mut transaction, &task.id, crate::ConditionChange::Legacy)
            .await?;
        crate::task_condition::refresh_task_condition(&mut transaction, &mut task).await?;
        self.record_mutation_reply_in_tx(&mut transaction, &task)
            .await?;

        transaction.commit().await?;
        Ok(task)
    }

    async fn soft_delete(&self, input: crate::SoftDeleteTask) -> Result<Task> {
        let mut task = self.get_task_required(&input.id, true).await?;
        let workflow: String =
            sqlx::query_scalar("SELECT workflow_definition FROM project WHERE id = ?")
                .bind(&task.project_id)
                .fetch_one(&self.pool)
                .await?;
        if task.deleted_at.is_some()
            || matches!(task.status.as_str(), "in_progress" | "merging")
            || crate::budget::review_states_from_json(&workflow).contains(&task.status)
        {
            return Err(DbError::InvalidSoftDelete);
        }
        if task.version != input.expected_version {
            return Err(DbError::VersionConflict);
        }
        task.deleted_at = Some(input.deleted_at);
        task.updated_at = input.updated_at;
        task.version += 1;
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let result = sqlx::query("UPDATE task SET deleted_at = ?, version = version + 1, updated_at = ? WHERE id = ? AND version = ? AND deleted_at IS NULL")
            .bind(task.deleted_at.as_deref())
            .bind(&task.updated_at)
            .bind(&task.id)
            .bind(input.expected_version)
            .execute(&mut *transaction)
            .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        // The row's own facts are unchanged; its parent lost a visible child.
        if let Some(parent) = &task.parent_task_id {
            crate::task_condition::produce_children(&mut transaction, parent).await?;
        }
        transaction.commit().await?;
        Ok(task)
    }

    async fn claim(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        mut input: ClaimTask,
    ) -> Result<ClaimedTask> {
        if !crate::task_writer::owns_task(&input.task_id) {
            return Err(DbError::Check(
                "Task claim requires its claimed step".to_owned(),
            ));
        }
        self.fence_current_step_in_tx(transaction).await?;
        let sql = format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ? AND deleted_at IS NULL");
        let row = sqlx::query(&sql)
            .bind(&input.task_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        let mut task = map_task(row)?;
        if crate::task_writer::owns_task(&input.task_id) {
            input.expected_version = task.version;
        }
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
        // Read the Project authority inside the writer transaction even for
        // low-level callers that omit an expected revision. The execution
        // snapshot is stamped from this exact row below; a pre-transaction
        // Task/Project read must never become completion authority.
        let project_authority = sqlx::query_as::<_, (i64, String)>(
            "SELECT version, workflow_definition FROM project WHERE id = ?",
        )
        .bind(&task.project_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(DbError::NotFound)?;
        if (input.expected_project_version.is_some()
            || input.expected_workflow_definition.is_some())
            && (input.expected_project_version != Some(project_authority.0)
                || input.expected_workflow_definition.as_deref()
                    != Some(project_authority.1.as_str()))
        {
            return Err(DbError::VersionConflict);
        }
        let initial_lease = &input.execution_lease;
        let invalid_deadline = initial_lease
            .hard_deadline_at
            .as_deref()
            .is_some_and(|deadline| {
                initial_lease.lease_expires_at.as_str() > deadline
                    || deadline <= initial_lease.now.as_str()
            });
        if initial_lease.execution_id != input.execution.id
            || initial_lease.expected_version != 1
            || initial_lease.owner.trim().is_empty()
            || invalid_deadline
        {
            return Err(DbError::Check(
                "task claim requires a valid initial execution lease".to_owned(),
            ));
        }
        // Claim and its Running execution are one transaction. Re-check the
        // Charter-backed execution admission here; the service's earlier
        // read gate only avoids unnecessary workspace side effects.
        if input.execution.status == ExecutionStatus::Running {
            if let Some(workspace_id) = input.execution.workspace_id.as_deref() {
                Self::ensure_execution_admission_in_tx(transaction, &input.task_id, workspace_id)
                    .await?;
            }
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
        let claim_triggered_by = match input.assignee_type.as_str() {
            "agent" => format!(
                "agent:{}",
                input
                    .execution
                    .agent_id
                    .as_deref()
                    .ok_or(DbError::InvalidTransition)?
            ),
            "user" => format!(
                "user:{}",
                input
                    .assignee_id
                    .as_deref()
                    .ok_or(DbError::InvalidTransition)?
            ),
            _ => return Err(DbError::InvalidTransition),
        };

        // A claim is itself the state-entry boundary for the execution it
        // creates. Mint the token and stamp the immutable execution snapshot
        // under the same SQLite writer transaction as the source -> target
        // Task CAS. This prevents a direct claim from retaining the source
        // state/token captured while its executor configuration was built.
        let state_entry_token = new_uuid_v4();
        if let Some(raw_snapshot) = input.execution.executor_config_snapshot_json.as_deref() {
            let mut snapshot: serde_json::Value = serde_json::from_str(raw_snapshot)
                .map_err(|error| DbError::Check(format!("invalid execution snapshot: {error}")))?;
            let snapshot = snapshot.as_object_mut().ok_or_else(|| {
                DbError::Check("execution snapshot must be a JSON object".to_owned())
            })?;
            snapshot.insert(
                "task_state".to_owned(),
                serde_json::Value::String(input.target_status.clone()),
            );
            snapshot.insert(
                "state_entry_token".to_owned(),
                serde_json::Value::String(state_entry_token.clone()),
            );
            snapshot.insert(
                "project_version".to_owned(),
                serde_json::Value::Number(project_authority.0.into()),
            );
            input.execution.executor_config_snapshot_json =
                Some(serde_json::to_string(snapshot).map_err(|error| {
                    DbError::Check(format!("invalid execution snapshot: {error}"))
                })?);
        }

        let unsatisfied_dependencies =
            Self::unsatisfied_dependencies_in_tx(transaction, &input.task_id).await?;
        if !unsatisfied_dependencies.is_empty() && input.assignee_type == "agent" {
            let agent_id = assignee_agent_id.ok_or(DbError::InvalidTransition)?;
            let mut context_holder_match = false;
            for depends_on_id in &unsatisfied_dependencies {
                // A dependency may have been retried by a different agent
                // after this agent supplied its context.  Checking only the
                // newest executor row would hide that older context-holder
                // execution and incorrectly reject the dispatch.
                let context_holder_exists = sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(
                         SELECT 1 FROM execution
                         WHERE task_id = ? AND role = 'executor' AND agent_id = ?
                     )",
                )
                .bind(depends_on_id)
                .bind(agent_id)
                .fetch_one(&mut **transaction)
                .await?;
                if context_holder_exists {
                    context_holder_match = true;
                    break;
                }
            }
            if !context_holder_match {
                return Err(DbError::DependencyGate);
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

        insert_recovery_marker_in_tx(
            transaction,
            &CreateTransitionLog {
                id: state_entry_token,
                task_id: task.id.clone(),
                from_state: input.source_status.clone(),
                to_state: task.status.clone(),
                trigger_name: None,
                triggered_by: claim_triggered_by,
                bridge: Default::default(),
                trigger_reason: "task claimed".to_owned(),
                hook_results_json: None,
                rejection: false,
                created_at: task.updated_at.clone(),
            },
        )
        .await?;

        // The Task status changes from the source state to the claim target
        // in this transaction. Validate the execution admission against that
        // committed-in-transaction target state, not the stale source state
        // that selected the claim.
        let mut execution_admission = input.execution_admission.clone();
        if let Some(admission) = execution_admission.as_mut() {
            admission.expected_task_version = task.version;
            admission.expected_task_status = task.status.clone();
        }
        if let Some(admission) = execution_admission.as_mut() {
            if admission.expected_effective_role.is_some() && input.execution.role != "interactive"
            {
                let assignment_role = if input.execution.role == "executor" {
                    "coder"
                } else {
                    input.execution.role.as_str()
                };
                let assignment = effective_execution_assignment_in_tx(
                    transaction,
                    &input.task_id,
                    assignment_role,
                )
                .await?;
                match (
                    admission.expected_assignment_id.as_deref(),
                    admission.expected_assignment_updated_at.as_deref(),
                    assignment,
                ) {
                    (Some(expected_id), Some(expected_updated_at), Some(row)) => {
                        let actual_id: String = row.try_get("id")?;
                        let actual_updated_at: String = row.try_get("updated_at")?;
                        let actual_type: Option<String> = row.try_get("assignee_type")?;
                        let actual_agent: Option<String> = row.try_get("assignee_id")?;
                        if actual_id != expected_id
                            || actual_updated_at != expected_updated_at
                            || actual_type.as_deref() != Some("agent")
                            || actual_agent.as_deref() != input.execution.agent_id.as_deref()
                        {
                            return Err(DbError::VersionConflict);
                        }
                    }
                    (None, None, None) => {
                        let assignment_id = new_uuid_v4();
                        sqlx::query(
                            "INSERT INTO task_role_assignment
                                (id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at)
                             VALUES (?, ?, ?, 'agent', ?, ?, ?)",
                        )
                        .bind(&assignment_id)
                        .bind(&input.task_id)
                        .bind(assignment_role)
                        .bind(input.execution.agent_id.as_deref())
                        .bind(&task.updated_at)
                        .bind(&task.updated_at)
                        .execute(&mut **transaction)
                        .await?;
                        admission.expected_assignment_id = Some(assignment_id);
                        admission.expected_assignment_updated_at = Some(task.updated_at.clone());
                    }
                    _ => return Err(DbError::VersionConflict),
                }
            }
            Self::ensure_task_execution_admission_in_tx(transaction, &input.execution, admission)
                .await?;
        }
        let mut execution = self
            .create_execution_in_tx(transaction, &input.execution, execution_admission.as_ref())
            .await?;
        // A reviewer/auditor claim owns the selected Review attempt in the
        // same transaction as the Task mutation, Running execution, and
        // initial lease. This keeps claim admission from bypassing the
        // durable attempt binding used by ordinary dispatch.
        Self::bind_role_execution_to_review_in_tx(
            transaction,
            &input.execution,
            execution_admission.as_ref(),
        )
        .await?;
        let lease_result = sqlx::query(
            "UPDATE execution
             SET lease_owner = ?,
                 lease_expires_at = MIN(?, COALESCE(?, ?)),
                 hard_deadline_at = ?,
                 last_heartbeat_at = ?,
                 execution_version = execution_version + 1,
                 updated_at = ?
             WHERE id = ? AND status = 'running'
               AND execution_version = ? AND lease_owner IS NULL",
        )
        .bind(&initial_lease.owner)
        .bind(&initial_lease.lease_expires_at)
        .bind(initial_lease.hard_deadline_at.as_deref())
        .bind(&initial_lease.lease_expires_at)
        .bind(initial_lease.hard_deadline_at.as_deref())
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
        execution.hard_deadline_at = initial_lease.hard_deadline_at.clone();
        execution.last_heartbeat_at = Some(initial_lease.now.clone());
        execution.updated_at = initial_lease.now.clone();
        Ok(ClaimedTask { task, execution })
    }

    async fn update_status(&self, input: UpdateTaskStatus) -> Result<Task> {
        if !crate::task_writer::owns_task(&input.id) {
            return self
                .run_task_mutation(
                    &input.id,
                    crate::TaskMutation::TaskUpdateStatus {
                        input: input.clone(),
                    },
                )
                .await;
        }

        update_task_status_inner(self, input, None, None)
            .await?
            .ok_or(DbError::VersionConflict)
    }

    async fn update_status_for_latest_execution(
        &self,
        input: UpdateTaskStatus,
        authority: LatestExecutionAuthority,
    ) -> Result<Option<Task>> {
        if !crate::task_writer::owns_task(&input.id) {
            return self
                .run_task_mutation(
                    &input.id,
                    crate::TaskMutation::TaskUpdateStatusForLatestExecution {
                        input: input.clone(),
                        authority: authority.clone(),
                    },
                )
                .await;
        }

        update_task_status_inner(self, input, None, Some(&authority)).await
    }

    async fn update_status_with_recovery_marker(
        &self,
        input: UpdateTaskStatus,
        marker: CreateTransitionLog,
    ) -> Result<Task> {
        if !crate::task_writer::owns_task(&input.id) {
            return self
                .run_task_mutation(
                    &input.id,
                    crate::TaskMutation::TaskUpdateStatusWithRecoveryMarker {
                        input: input.clone(),
                        marker: marker.clone(),
                    },
                )
                .await;
        }

        update_task_status_inner(self, input, Some(&marker), None)
            .await?
            .ok_or(DbError::VersionConflict)
    }
}
