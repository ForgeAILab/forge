use super::*;
use crate::{AssigneeKind, CreateTransitionLog, TaskRoleAssignment};
use std::collections::HashSet;

async fn load_task<'e, E>(executor: E, id: &str, include_deleted: bool) -> Result<Option<Task>>
where
    E: sqlx::Executor<'e, Database = Sqlite>,
{
    let sql = if include_deleted {
        format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?")
    } else {
        format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ? AND deleted_at IS NULL")
    };
    sqlx::query(&sql)
        .bind(id)
        .fetch_optional(executor)
        .await?
        .map(map_task)
        .transpose()
}

async fn check_project_workflow_authority_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    task_id: &str,
    expected_project_version: i64,
    expected_workflow_definition: &str,
) -> Result<()> {
    let authority = sqlx::query_as::<_, (i64, String)>(
        "SELECT p.version, p.workflow_definition
         FROM task AS t
         JOIN project AS p ON p.id = t.project_id
         WHERE t.id = ?",
    )
    .bind(task_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DbError::NotFound)?;
    if authority.0 != expected_project_version || authority.1 != expected_workflow_definition {
        return Err(DbError::VersionConflict);
    }
    Ok(())
}

async fn set_review_passed_at_inner(
    db: &SqliteDb,
    id: &str,
    expected_version: Option<i64>,
    review_passed_at: Option<String>,
    expected_review_updated_at: Option<&str>,
    updated_at: &str,
) -> Result<Task> {
    let mut transaction = crate::begin_immediate(&db.pool).await?;
    let task_row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
        .bind(id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
    let task = map_task(task_row)?;
    if task.deleted_at.is_some() {
        return Err(DbError::NotFound);
    }
    if let Some(expected_version) = expected_version {
        if task.version != expected_version {
            return Err(DbError::VersionConflict);
        }
    } else if task.updated_at.as_str() > updated_at {
        // The legacy, snapshot-free boundary cannot carry the caller's Task
        // version.  Retain a monotonic timestamp guard until its call sites
        // migrate to the strict CAS method below.
        return Err(DbError::VersionConflict);
    }

    // A passed projection is only meaningful for the newest review.  An old
    // review completion arriving after a new attempt has started must not turn
    // the Task back into an apparently passed review.  Fixtures and legacy
    // callers that set the projection before any review row exists remain
    // valid; once a review exists, its latest status is the authority for a
    // new pass projection. Strict callers also bind the projection to the
    // exact review `updated_at` write that produced the pass.
    if review_passed_at.is_some() && expected_version.is_some() {
        let expected_review_updated_at = expected_review_updated_at.ok_or_else(|| {
            DbError::Check("review pass projection is missing its Review timestamp".to_owned())
        })?;
        let latest_review = sqlx::query(
            "SELECT status, updated_at
             FROM review
             WHERE task_id = ?
             ORDER BY attempt_number DESC, created_at DESC, id DESC
             LIMIT 1",
        )
        .bind(id)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(latest_review) = latest_review {
            let status: String = latest_review.try_get("status")?;
            let review_updated_at: String = latest_review.try_get("updated_at")?;
            // Bind the projection to the exact review write that produced it.
            // A stale completion that happens to observe a newer Task version
            // must not re-project an old passed row after authority changed.
            if status != "passed" || review_updated_at != expected_review_updated_at {
                return Err(DbError::VersionConflict);
            }
        }
    }

    let (freshness_predicate, version_predicate, version_bump) = if expected_version.is_some() {
        ("", " AND version = ?", ", version = version + 1")
    } else {
        (" AND updated_at <= ?", "", "")
    };
    let query = format!(
        "UPDATE task
         SET review_passed_at = ?, updated_at = ?{version_bump}
         WHERE id = ? AND deleted_at IS NULL{freshness_predicate}{version_predicate}"
    );
    let mut update = sqlx::query(&query)
        .bind(review_passed_at.as_deref())
        .bind(updated_at)
        .bind(id);
    if expected_version.is_none() {
        update = update.bind(updated_at);
    }
    if let Some(expected_version) = expected_version {
        update = update.bind(expected_version);
    }
    let result = update.execute(&mut *transaction).await?;
    if result.rows_affected() == 0 {
        return Err(DbError::VersionConflict);
    }
    let updated_row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
        .bind(id)
        .fetch_one(&mut *transaction)
        .await?;
    let updated = map_task(updated_row)?;
    transaction.commit().await?;
    Ok(updated)
}

fn apply_metadata_mutation(
    metadata: &mut TaskMetadata,
    mutation: TaskMetadataMutation,
) -> Result<bool> {
    match mutation {
        TaskMetadataMutation::Set { key, value } => {
            if metadata.extra.get(&key) == Some(&value) {
                return Ok(false);
            }
            metadata.extra.insert(key, value);
            Ok(true)
        }
        TaskMetadataMutation::SetIf {
            key,
            expected,
            value,
        } => {
            if metadata.extra.get(&key) != Some(&expected)
                || metadata.extra.get(&key) == Some(&value)
            {
                return Ok(false);
            }
            metadata.extra.insert(key, value);
            Ok(true)
        }
        TaskMetadataMutation::SetIfAbsent { key, value } => {
            if metadata.extra.contains_key(&key) {
                return Ok(false);
            }
            metadata.extra.insert(key, value);
            Ok(true)
        }
        TaskMetadataMutation::Increment { key, by } => {
            let current = metadata.extra.get(&key).map_or(Ok(0_i64), |value| {
                value.as_i64().ok_or_else(|| {
                    DbError::Check(format!("metadata counter {key} is not an integer"))
                })
            })?;
            let next = current
                .checked_add(by)
                .ok_or_else(|| DbError::Check(format!("metadata counter {key} overflowed")))?;
            let value = serde_json::Value::Number(serde_json::Number::from(next));
            if metadata.extra.get(&key) == Some(&value) {
                return Ok(false);
            }
            metadata.extra.insert(key, value);
            Ok(true)
        }
        TaskMetadataMutation::Remove { key } => Ok(metadata.extra.remove(&key).is_some()),
        TaskMetadataMutation::RemoveIf { key, expected } => {
            if metadata.extra.get(&key) != Some(&expected) {
                return Ok(false);
            }
            metadata.extra.remove(&key);
            Ok(true)
        }
        TaskMetadataMutation::CompareAndMutate {
            key,
            expected,
            mutations,
        } => {
            if metadata.extra.get(&key) != Some(&expected) {
                return Ok(false);
            }
            let mut changed = false;
            for mutation in mutations {
                changed |= apply_metadata_mutation(metadata, mutation)?;
            }
            Ok(changed)
        }
    }
}

async fn insert_recovery_marker_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    marker: &CreateTransitionLog,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO transition_log (id, task_id, from_state, to_state, trigger_name, triggered_by, trigger_reason, hook_results_json, rejection, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&marker.id)
    .bind(&marker.task_id)
    .bind(&marker.from_state)
    .bind(&marker.to_state)
    .bind(marker.trigger_name.as_deref())
    .bind(&marker.triggered_by)
    .bind(&marker.trigger_reason)
    .bind(marker.hook_results_json.as_deref())
    .bind(if marker.rejection { 1_i64 } else { 0_i64 })
    .bind(&marker.created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn update_task_inner(
    db: &SqliteDb,
    input: UpdateTask,
    recovery_marker: Option<&CreateTransitionLog>,
    workflow_authority: Option<(i64, String)>,
) -> Result<Task> {
    if recovery_marker.is_some_and(|marker| marker.task_id != input.id) {
        return Err(DbError::InvalidTransition);
    }
    let mut transaction = crate::begin_immediate(&db.pool).await?;
    let task_row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
        .bind(&input.id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
    let mut task = map_task(task_row)?;
    if task.deleted_at.is_some() {
        return Err(DbError::InvalidSoftDelete);
    }
    if let Some((expected_project_version, expected_workflow_definition)) =
        workflow_authority.as_ref()
    {
        check_project_workflow_authority_in_tx(
            &mut transaction,
            &task.id,
            *expected_project_version,
            expected_workflow_definition,
        )
        .await?;
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
        append_task_interruption_event(db, &mut transaction, &task).await?;
    }
    if let Some(marker) = recovery_marker {
        insert_recovery_marker_in_tx(&mut transaction, marker).await?;
    }
    transaction.commit().await?;
    Ok(task)
}

async fn ensure_no_running_execution_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    task_id: &str,
    excluded_execution_id: Option<&str>,
    workspace_id: Option<&str>,
    overlapping_roles: Vec<String>,
) -> Result<()> {
    let mut running = sqlx::QueryBuilder::<Sqlite>::new(
        "SELECT role, id FROM execution WHERE status = 'running' AND (task_id = ",
    );
    running.push_bind(task_id);
    if let Some(workspace_id) = workspace_id {
        // A workspace reset can admit the same Task in a new workspace. A
        // shared-root workspace can also carry a sibling Task's execution;
        // either must block a late Task-level annotation/restore.
        running.push(" OR workspace_id = ").push_bind(workspace_id);
    }
    running.push(")");
    if let Some(excluded_execution_id) = excluded_execution_id {
        running.push(" AND id <> ").push_bind(excluded_execution_id);
    }
    // An empty role list is the fail-closed wildcard used by manual-stop and
    // recovery restoration. Those boundaries must not miss a cross-role
    // replacement admitted for the same Task or workspace.
    if !overlapping_roles.is_empty() {
        running.push(" AND role IN (");
        {
            let mut separated = running.separated(", ");
            for role in overlapping_roles {
                separated.push_bind(role);
            }
        }
        running.push(")");
    }
    running.push(" ORDER BY created_at DESC, id DESC LIMIT 1");
    let replacement: Option<(String, String)> = running
        .build_query_as()
        .fetch_optional(&mut **transaction)
        .await?;
    if let Some((role, execution_id)) = replacement {
        // The public conflict contract names the resource slot, not the
        // workflow role that happens to occupy it. Repository-capable roles
        // share the repository/workspace slot; interactive keeps its own
        // canonical slot name.
        let scope = if role == "interactive" {
            "interactive"
        } else {
            "repository"
        };
        return Err(DbError::ExecutionAlreadyRunning {
            scope: scope.to_owned(),
            execution_id,
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn set_error_annotation_if_no_running_execution_inner(
    db: &SqliteDb,
    id: &str,
    expected_version: i64,
    expected_status: &str,
    expected_state_entry_token: Option<&str>,
    expected_workflow_definition: &str,
    expected_assignment_role: Option<&str>,
    expected_assignment: Option<&TaskRoleAssignment>,
    annotation: &str,
    updated_at: &str,
    stopped_execution_id: &str,
    workspace_id: Option<&str>,
    overlapping_roles: Vec<String>,
) -> Result<Task> {
    let mut transaction = crate::begin_immediate(&db.pool).await?;
    let task_row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
        .bind(id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
    let mut task = map_task(task_row)?;
    if task.deleted_at.is_some() {
        return Err(DbError::InvalidSoftDelete);
    }
    if task.version != expected_version {
        return Err(DbError::VersionConflict);
    }

    // Project workflow edits are a separate version domain from Task edits.
    // Bind the annotation to the exact status/workflow snapshot used before
    // execution terminalization, otherwise a workflow role change at the same
    // Task status could make this stale stop annotate the next role window.
    let actual_workflow_definition: String = sqlx::query_scalar(
        "SELECT workflow_definition FROM project
         WHERE id = (SELECT project_id FROM task WHERE id = ?)",
    )
    .bind(id)
    .fetch_one(&mut *transaction)
    .await?;
    if task.status != expected_status || actual_workflow_definition != expected_workflow_definition
    {
        return Err(DbError::VersionConflict);
    }

    // Task.version alone does not identify a state entry: a Task can cycle
    // X -> Y -> X while retaining the same role and assignment.  Bind the
    // manual-stop annotation to the exact latest transition-log row entering
    // the expected state.  An absent row is meaningful for the initial state,
    // so `None` must match only another no-log snapshot.
    let actual_state_entry_token: Option<String> = sqlx::query_scalar(
        "SELECT id
         FROM transition_log
         WHERE task_id = ? AND to_state = ?
         ORDER BY created_at DESC, rowid DESC
         LIMIT 1",
    )
    .bind(id)
    .bind(expected_status)
    .fetch_optional(&mut *transaction)
    .await?;
    if actual_state_entry_token.as_deref() != expected_state_entry_token {
        return Err(DbError::VersionConflict);
    }

    // Role assignments are a separate version domain from Tasks. Bind a
    // manual-stop annotation to the exact assignment selected for the
    // stopped execution so a same-role reassignment cannot inherit the old
    // execution's blocker. Interactive stops intentionally have no workflow
    // assignment dependency and pass `None` here.
    if let Some(role_name) = expected_assignment_role {
        let actual_assignment = sqlx::query(
            "SELECT id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at
             FROM task_role_assignment WHERE task_id = ? AND role_name = ?",
        )
        .bind(id)
        .bind(role_name)
        .fetch_optional(&mut *transaction)
        .await?
        .map(|row| -> Result<TaskRoleAssignment> {
            let assignee_type = row
                .try_get::<Option<String>, _>("assignee_type")?
                .map(|value| {
                    value
                        .parse::<AssigneeKind>()
                        .map_err(|_| DbError::InvalidTransition)
                })
                .transpose()?;
            Ok::<TaskRoleAssignment, DbError>(TaskRoleAssignment {
                id: row.try_get("id")?,
                task_id: row.try_get("task_id")?,
                role_name: row.try_get("role_name")?,
                assignee_type,
                assignee_id: row.try_get("assignee_id")?,
                created_at: row.try_get("created_at")?,
                updated_at: row.try_get("updated_at")?,
            })
        })
        .transpose()?;
        let assignment_matches = match (actual_assignment, expected_assignment) {
            (None, None) => true,
            (Some(actual), Some(expected)) => {
                actual.id == expected.id
                    && actual.task_id == expected.task_id
                    && actual.role_name == expected.role_name
                    && actual.assignee_type == expected.assignee_type
                    && actual.assignee_id == expected.assignee_id
                    && actual.created_at == expected.created_at
                    && actual.updated_at == expected.updated_at
            }
            _ => false,
        };
        if !assignment_matches {
            return Err(DbError::VersionConflict);
        }
    }

    // Hold the same immediate write transaction as the Task CAS. A replacement
    // admitted before this transaction starts is visible and blocks the
    // annotation; one admitted after the commit observes the new annotation
    // through its own admission check instead of being retroactively blocked.
    ensure_no_running_execution_in_tx(
        &mut transaction,
        id,
        Some(stopped_execution_id),
        workspace_id,
        overlapping_roles,
    )
    .await?;

    let previous_error_annotation = task.error_annotation.clone();
    task.error_annotation = Some(annotation.to_owned());
    task.updated_at = updated_at.to_owned();
    task.version += 1;
    let result = sqlx::query(
        "UPDATE task
         SET error_annotation = ?, version = version + 1, updated_at = ?
         WHERE id = ? AND version = ? AND deleted_at IS NULL",
    )
    .bind(task.error_annotation.as_deref())
    .bind(&task.updated_at)
    .bind(&task.id)
    .bind(expected_version)
    .execute(&mut *transaction)
    .await?;
    if result.rows_affected() == 0 {
        return Err(DbError::VersionConflict);
    }
    if previous_error_annotation != task.error_annotation {
        append_task_interruption_event(db, &mut transaction, &task).await?;
    }
    transaction.commit().await?;
    Ok(task)
}

#[allow(clippy::too_many_arguments)]
async fn restore_recovery_metadata_if_no_running_execution_inner(
    db: &SqliteDb,
    id: &str,
    expected_version: i64,
    error_annotation: Option<String>,
    blocked_json: Option<String>,
    failed_json: Option<String>,
    updated_at: &str,
    workspace_id: Option<&str>,
    overlapping_roles: Vec<String>,
) -> Result<Task> {
    let mut transaction = crate::begin_immediate(&db.pool).await?;
    let task_row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
        .bind(id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
    let mut task = map_task(task_row)?;
    if task.deleted_at.is_some() {
        return Err(DbError::InvalidSoftDelete);
    }
    if task.version != expected_version {
        return Err(DbError::VersionConflict);
    }
    ensure_no_running_execution_in_tx(&mut transaction, id, None, workspace_id, overlapping_roles)
        .await?;

    let previous_error_annotation = task.error_annotation.clone();
    let previous_blocked_json = task.blocked_json.clone();
    let previous_failed_json = task.failed_json.clone();
    task.error_annotation = error_annotation;
    task.blocked_json = blocked_json;
    task.failed_json = failed_json;
    task.updated_at = updated_at.to_owned();
    task.version += 1;
    let result = sqlx::query(
        "UPDATE task
         SET error_annotation = ?, blocked_json = ?, failed_json = ?,
             version = version + 1, updated_at = ?
         WHERE id = ? AND version = ? AND deleted_at IS NULL",
    )
    .bind(task.error_annotation.as_deref())
    .bind(task.blocked_json.as_deref())
    .bind(task.failed_json.as_deref())
    .bind(&task.updated_at)
    .bind(&task.id)
    .bind(expected_version)
    .execute(&mut *transaction)
    .await?;
    if result.rows_affected() == 0 {
        return Err(DbError::VersionConflict);
    }
    if interruption_fields_changed(
        &previous_error_annotation,
        &previous_blocked_json,
        &previous_failed_json,
        &task,
    ) {
        append_task_interruption_event(db, &mut transaction, &task).await?;
    }
    transaction.commit().await?;
    Ok(task)
}

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
        map_task(row)
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
        update_task_inner(self, input, None, None).await
    }

    async fn update_with_recovery_marker(
        &self,
        input: UpdateTask,
        marker: CreateTransitionLog,
    ) -> Result<Task> {
        update_task_inner(self, input, Some(&marker), None).await
    }

    async fn update_with_workflow_authority(
        &self,
        input: UpdateTask,
        expected_project_version: i64,
        expected_workflow_definition: String,
    ) -> Result<Task> {
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

    async fn restore_recovery_metadata_if_no_running_execution(
        &self,
        id: &str,
        expected_version: i64,
        error_annotation: Option<String>,
        blocked_json: Option<String>,
        failed_json: Option<String>,
        updated_at: &str,
        workspace_id: Option<&str>,
        overlapping_roles: Vec<String>,
    ) -> Result<Task> {
        restore_recovery_metadata_if_no_running_execution_inner(
            self,
            id,
            expected_version,
            error_annotation,
            blocked_json,
            failed_json,
            updated_at,
            workspace_id,
            overlapping_roles,
        )
        .await
    }

    async fn set_review_passed_at(
        &self,
        id: &str,
        review_passed_at: Option<String>,
        updated_at: &str,
    ) -> Result<Task> {
        set_review_passed_at_inner(self, id, None, review_passed_at, None, updated_at).await
    }

    async fn set_review_passed_at_cas(
        &self,
        id: &str,
        expected_version: i64,
        review_passed_at: Option<String>,
        updated_at: &str,
    ) -> Result<Task> {
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
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let task_row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        let mut task = map_task(task_row)?;
        if task.deleted_at.is_some() {
            return Err(DbError::NotFound);
        }
        if expected_version.is_some_and(|expected| task.version != expected) {
            return Err(DbError::VersionConflict);
        }

        let mut metadata = TaskMetadata::parse(task.metadata_json.as_deref())
            .map_err(|error| DbError::Check(format!("invalid task metadata: {error}")))?;
        let mut changed = false;
        for mutation in mutations {
            changed |= apply_metadata_mutation(&mut metadata, mutation)?;
        }
        if !changed {
            transaction.commit().await?;
            return Ok(task);
        }
        let metadata_json = metadata.to_json();
        sqlx::query(
            "UPDATE task
             SET metadata_json = ?, updated_at = ?
             WHERE id = ? AND deleted_at IS NULL",
        )
        .bind(metadata_json.as_deref())
        .bind(updated_at)
        .bind(id)
        .execute(&mut *transaction)
        .await?;
        task.metadata_json = metadata_json;
        task.updated_at = updated_at.to_owned();
        transaction.commit().await?;
        Ok(task)
    }

    async fn wake_dispatch_for_task(&self, id: &str, updated_at: &str) -> Result<Task> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
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
        let result = sqlx::query(
            "UPDATE task
             SET metadata_json = ?, updated_at = ?, version = version + 1
             WHERE id = ? AND deleted_at IS NULL",
        )
        .bind(metadata_json.as_deref())
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
        let changed =
            wake_dispatch_for_project_in_tx(&mut transaction, project_id, updated_at).await?;
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

    async fn set_entry_barrier_with_workflow_authority(
        &self,
        id: &str,
        expected_version: i64,
        entry_barrier_json: Option<String>,
        updated_at: &str,
        expected_project_version: i64,
        expected_workflow_definition: String,
    ) -> Result<Task> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        check_project_workflow_authority_in_tx(
            &mut transaction,
            id,
            expected_project_version,
            &expected_workflow_definition,
        )
        .await?;
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
        let task = map_task(row)?;
        transaction.commit().await?;
        Ok(task)
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
        if input.expected_project_version.is_some() || input.expected_workflow_definition.is_some()
        {
            let project_authority = sqlx::query_as::<_, (i64, String)>(
                "SELECT version, workflow_definition FROM project WHERE id = ?",
            )
            .bind(&task.project_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(DbError::NotFound)?;
            if input.expected_project_version != Some(project_authority.0)
                || input.expected_workflow_definition.as_deref()
                    != Some(project_authority.1.as_str())
            {
                return Err(DbError::VersionConflict);
            }
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
                let assignment = sqlx::query(
                    "SELECT id, assignee_type, assignee_id, updated_at
                     FROM task_role_assignment
                     WHERE task_id = ? AND role_name = ?",
                )
                .bind(&input.task_id)
                .bind(assignment_role)
                .fetch_optional(&mut **transaction)
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
        let mut execution = Self::create_execution_in_tx(
            transaction,
            &input.execution,
            execution_admission.as_ref(),
        )
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
        update_task_status_inner(self, input, None).await
    }

    async fn update_status_with_recovery_marker(
        &self,
        input: UpdateTaskStatus,
        marker: CreateTransitionLog,
    ) -> Result<Task> {
        update_task_status_inner(self, input, Some(&marker)).await
    }
}

async fn update_task_status_inner(
    db: &SqliteDb,
    input: UpdateTaskStatus,
    recovery_marker: Option<&CreateTransitionLog>,
) -> Result<Task> {
    if recovery_marker.is_some_and(|marker| marker.task_id != input.id) {
        return Err(DbError::InvalidTransition);
    }
    let mut transaction = crate::begin_immediate(&db.pool).await?;
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
        append_task_interruption_event(db, &mut transaction, &task).await?;
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
    DomainEventRepo::append_event_in_tx(db, &mut transaction, &event).await?;
    if let Some(marker) = recovery_marker {
        insert_recovery_marker_in_tx(&mut transaction, marker).await?;
    }
    transaction.commit().await?;
    Ok(task)
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
