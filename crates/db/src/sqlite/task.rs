use super::*;
use crate::ProjectSlotCounts;
use crate::{
    AssigneeKind, CreateTransitionLog, LatestExecutionMetadataClaim, RestoreQueuedRecovery,
    TaskRoleAssignment,
};
use std::collections::HashSet;

// Existing Project and Task/state/time indexes bound the seek before parsing.
// One query supplies every eligible path; UTC RFC3339 bounds match DB writers.
pub const CONFLICT_HANDOFF_QUERY: &str =
    "SELECT tl.id, tl.task_id, tl.bridge_kind, tl.bridge_payload, tl.created_at
    FROM task t JOIN transition_log tl ON tl.task_id = t.id
    WHERE t.project_id = ? AND tl.from_state = 'merging' AND tl.to_state = 'merge_failed'
      AND tl.triggered_by = ? AND tl.created_at > ? AND tl.created_at <= ?
    ORDER BY tl.created_at DESC, tl.rowid DESC";

/// Authoritative handoff fields, decoded only when the consumer needs them.
pub struct ConflictHandoffSourceRow {
    row: SqliteRow,
}

impl ConflictHandoffSourceRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn from_state(&self) -> crate::Result<String> {
        Ok(self.row.try_get("from_state")?)
    }
    pub fn to_state(&self) -> crate::Result<String> {
        Ok(self.row.try_get("to_state")?)
    }
    pub fn triggered_by(&self) -> crate::Result<String> {
        Ok(self.row.try_get("triggered_by")?)
    }
    pub fn bridge_kind(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("bridge_kind")?)
    }
    pub fn bridge_payload(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("bridge_payload")?)
    }
    pub fn created_at(&self) -> String {
        self.row.get("created_at")
    }
}

/// Historical handoffs decoded in the consumer's original per-row order.
pub struct ConflictHandoffHistoryRow {
    row: SqliteRow,
}

impl ConflictHandoffHistoryRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn task_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("task_id")?)
    }
    pub fn bridge_kind(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("bridge_kind")?)
    }
    pub fn bridge_payload(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("bridge_payload")?)
    }
    pub fn created_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("created_at")?)
    }
}

/// The Task whose own row decides who holds `role_name` on `task_id`: the
/// Task itself, except for the step-check role of a subtask in an inherited
/// workflow state, which its root holds
/// ([`crate::inherited_subtask::step_check_holder`], the one rule).
pub(super) async fn assignment_holder_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    task_id: &str,
    role_name: &str,
) -> Result<String> {
    let row: Option<(Option<String>, String)> =
        sqlx::query_as("SELECT parent_task_id, status FROM task WHERE id = ?")
            .bind(task_id)
            .fetch_optional(&mut **transaction)
            .await?;
    Ok(row
        .and_then(|(parent_task_id, status)| {
            crate::inherited_subtask::step_check_holder(
                parent_task_id.as_deref(),
                &status,
                role_name,
            )
            .map(str::to_owned)
        })
        .unwrap_or_else(|| task_id.to_owned()))
}

/// Load the assignment that authorizes an execution while holding the same
/// writer transaction that will create it. Coder rows resolve child first,
/// then direct parent. The step-check role of a subtask in an inherited
/// workflow state is its root's row and never its own
/// ([`assignment_holder_in_tx`]). Every other role remains Task-local.
pub(super) async fn effective_execution_assignment_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    task_id: &str,
    role_name: &str,
) -> Result<Option<SqliteRow>> {
    let holder = assignment_holder_in_tx(transaction, task_id, role_name).await?;
    if holder != task_id {
        return Ok(sqlx::query(
            "SELECT id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at
             FROM task_role_assignment WHERE task_id = ? AND role_name = ?",
        )
        .bind(&holder)
        .bind(role_name)
        .fetch_optional(&mut **transaction)
        .await?);
    }
    Ok(sqlx::query(
        "SELECT assignment.id, assignment.task_id, assignment.role_name, assignment.created_at,
                assignment.assignee_type,
                assignment.assignee_id,
                assignment.updated_at
         FROM task
         JOIN task_role_assignment AS assignment
           ON assignment.role_name = ?
          AND (assignment.task_id = task.id
               OR (? = 'coder'
                   AND task.parent_task_id IS NOT NULL
                   AND assignment.task_id = task.parent_task_id))
         WHERE task.id = ?
         ORDER BY CASE WHEN assignment.task_id = task.id THEN 0 ELSE 1 END
         LIMIT 1",
    )
    .bind(role_name)
    .bind(role_name)
    .bind(task_id)
    .fetch_optional(&mut **transaction)
    .await?)
}

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

async fn latest_execution_authority_matches_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    task: &Task,
    authority: &LatestExecutionAuthority,
) -> Result<bool> {
    let project_version = sqlx::query_scalar::<_, i64>("SELECT version FROM project WHERE id = ?")
        .bind(&task.project_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(DbError::NotFound)?;
    if project_version != authority.expected_project_version {
        return Ok(false);
    }

    let latest_execution_id = if matches!(authority.role.as_str(), "coder" | "executor") {
        sqlx::query_scalar::<_, String>(
            "SELECT id FROM execution
             WHERE task_id = ? AND role IN ('coder', 'executor')
             ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(&task.id)
        .fetch_optional(&mut **transaction)
        .await?
    } else if matches!(authority.role.as_str(), "reviewer" | "auditor") {
        sqlx::query_scalar::<_, String>(
            "SELECT id FROM execution
             WHERE task_id = ? AND role IN ('reviewer', 'auditor')
             ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(&task.id)
        .fetch_optional(&mut **transaction)
        .await?
    } else {
        sqlx::query_scalar::<_, String>(
            "SELECT id FROM execution
             WHERE task_id = ? AND role = ?
             ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(&task.id)
        .bind(&authority.role)
        .fetch_optional(&mut **transaction)
        .await?
    };
    if latest_execution_id.as_deref() != Some(authority.execution_id.as_str()) {
        return Ok(false);
    }

    if authority.role != "interactive" {
        let assignment_role = match authority.role.as_str() {
            "executor" => "coder",
            "auditor" => "reviewer",
            role => role,
        };
        // The step-check role of a subtask in an inherited workflow state is
        // held by its root's row and never its own.
        let assignment_task_id = crate::inherited_subtask::step_check_holder(
            task.parent_task_id.as_deref(),
            &task.status,
            assignment_role,
        )
        .unwrap_or(task.id.as_str());
        let assignment = sqlx::query_as::<_, (Option<String>, Option<String>, String)>(
            "SELECT assignee_type, assignee_id, updated_at
             FROM task_role_assignment
             WHERE task_id = ? AND role_name = ?",
        )
        .bind(assignment_task_id)
        .bind(assignment_role)
        .fetch_optional(&mut **transaction)
        .await?;
        if let Some((assignee_type, assignee_id, assignment_updated_at)) = assignment {
            let assignment_supersedes_execution =
                chrono::DateTime::parse_from_rfc3339(&assignment_updated_at)
                    .ok()
                    .zip(chrono::DateTime::parse_from_rfc3339(&authority.execution_updated_at).ok())
                    .is_some_and(|(assignment, execution)| assignment > execution);
            if assignee_type.as_deref() != Some("agent")
                || assignee_id.as_deref() != authority.agent_id.as_deref()
                || assignment_supersedes_execution
            {
                return Ok(false);
            }
        } else if assignment_role == "coder" && task.parent_task_id.is_some() {
            // Mirrors `TaskService::execution_owns_current_role_attempt`: an
            // inherited root default worker is admission authority, not live
            // ownership, so a started child with no coder of its own keeps
            // its current attempt through completion.
        } else if authority.agent_id.is_some() {
            return Ok(false);
        }
    }
    Ok(true)
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
    db.fence_current_step_in_tx(&mut transaction).await?;
    let task_row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
        .bind(id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
    let task = map_task(task_row)?;
    if task.deleted_at.is_some() {
        return Err(DbError::NotFound);
    }
    let expected_version = if crate::task_writer::owns_task(&task.id) && expected_version.is_some()
    {
        Some(task.version)
    } else {
        expected_version
    };
    if let Some(expected_version) = expected_version {
        let expected_version = if crate::task_writer::owns_task(&task.id) {
            task.version
        } else {
            expected_version
        };
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

async fn apply_task_mutation(
    transaction: &mut Transaction<'_, Sqlite>,
    task: &str,
    metadata: &mut TaskMetadata,
    mutation: TaskMetadataMutation,
) -> Result<bool> {
    match mutation {
        TaskMetadataMutation::Budget(effect) => {
            crate::budget::apply(transaction, task, effect).await
        }
        TaskMetadataMutation::BudgetIfSpent {
            key,
            expected,
            window_id,
            mutations,
        } => {
            if !crate::budget::matches_spent(transaction, task, &key, expected, &window_id).await? {
                return Ok(false);
            }
            let mut changed = false;
            for mutation in mutations {
                changed |=
                    Box::pin(apply_task_mutation(transaction, task, metadata, mutation)).await?;
            }
            Ok(changed)
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
                changed |=
                    Box::pin(apply_task_mutation(transaction, task, metadata, mutation)).await?;
            }
            Ok(changed)
        }
        mutation => apply_metadata_mutation(metadata, mutation),
    }
}

fn apply_metadata_mutation(
    metadata: &mut TaskMetadata,
    mutation: TaskMetadataMutation,
) -> Result<bool> {
    match mutation {
        TaskMetadataMutation::Budget(_) | TaskMetadataMutation::BudgetIfSpent { .. } => Err(
            DbError::Check("budget effects require a Task transaction".into()),
        ),
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
    if marker.bridge.bridge_kind == Some(api_types::TransitionBridgeKind::RetryWindowReset) {
        crate::budget::reset_all(transaction, &marker.task_id, &marker.id).await?;
    }
    sqlx::query(
        "INSERT INTO transition_log (id, task_id, from_state, to_state, trigger_name, triggered_by, trigger_reason, hook_results_json, rejection, created_at, bridge_kind, bridge_payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
    .bind(marker.bridge.bridge_kind.map(api_types::TransitionBridgeKind::as_str))
    .bind(marker.bridge.bridge_payload.as_ref().map(ToString::to_string))
    .execute(&mut **transaction)
    .await?;
    // A pre-epoch Task reads its entry from unstamped receipts.
    crate::task_condition::produce(transaction, &marker.task_id, crate::ConditionChange::Entry)
        .await?;
    Ok(())
}

async fn update_task_inner(
    db: &SqliteDb,
    mut input: UpdateTask,
    recovery_marker: Option<&CreateTransitionLog>,
    workflow_authority: Option<(i64, String)>,
) -> Result<Task> {
    if recovery_marker.is_some_and(|marker| marker.task_id != input.id) {
        return Err(DbError::InvalidTransition);
    }
    let mut transaction = crate::begin_immediate(&db.pool).await?;
    db.fence_current_step_in_tx(&mut transaction).await?;
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
    if crate::task_writer::owns_task(&input.id) {
        input.expected_version = task.version;
    }
    if task.version != input.expected_version {
        return Err(DbError::VersionConflict);
    }
    let previous_parent = task.parent_task_id.clone();
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
    if previous_parent != task.parent_task_id {
        // A subtask's terminal states and both parents' child witnesses.
        crate::task_condition::produce(&mut transaction, &task.id, crate::ConditionChange::Entry)
            .await?;
        task.condition = crate::task_condition::decode_or_unknown(
            &sqlx::query_scalar::<_, String>("SELECT condition_json FROM task WHERE id=?")
                .bind(&task.id)
                .fetch_one(&mut *transaction)
                .await?,
        );
        for parent in [&previous_parent, &task.parent_task_id]
            .into_iter()
            .flatten()
        {
            crate::task_condition::produce_children(&mut transaction, parent).await?;
        }
    } else {
        crate::task_condition::produce(&mut transaction, &task.id, crate::ConditionChange::Legacy)
            .await?;
        task.condition = crate::task_condition::decode_or_unknown(
            &sqlx::query_scalar::<_, String>("SELECT condition_json FROM task WHERE id=?")
                .bind(&task.id)
                .fetch_one(&mut *transaction)
                .await?,
        );
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
    db.record_mutation_reply_in_tx(&mut transaction, &task)
        .await?;

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
    db.fence_task_lease_in_tx(
        &mut transaction,
        id,
        "set_error_annotation_if_no_running_execution",
    )
    .await?;
    let task_row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
        .bind(id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
    let mut task = map_task(task_row)?;
    if task.deleted_at.is_some() {
        return Err(DbError::InvalidSoftDelete);
    }
    let expected_version = if crate::task_writer::owns_task(&task.id) {
        task.version
    } else {
        expected_version
    };
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
        // Stop and start must fence the same effective assignment, including
        // a child's inherited coder and its root-owned step-check lead.
        let actual_assignment =
            effective_execution_assignment_in_tx(&mut transaction, id, role_name)
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
    crate::task_condition::produce(&mut transaction, &task.id, crate::ConditionChange::Legacy)
        .await?;
    task.condition = crate::task_condition::decode_or_unknown(
        &sqlx::query_scalar::<_, String>("SELECT condition_json FROM task WHERE id=?")
            .bind(&task.id)
            .fetch_one(&mut *transaction)
            .await?,
    );
    if previous_error_annotation != task.error_annotation {
        append_task_interruption_event(db, &mut transaction, &task).await?;
    }
    db.record_mutation_reply_in_tx(&mut transaction, &task)
        .await?;

    transaction.commit().await?;
    Ok(task)
}

#[allow(clippy::too_many_arguments)]
async fn update_recovery_metadata_inner(
    db: &SqliteDb,
    id: &str,
    expected_version: i64,
    error_annotation: Option<String>,
    blocked_json: Option<String>,
    failed_json: Option<String>,
    updated_at: &str,
    workspace_id: Option<&str>,
    overlapping_roles: Vec<String>,
    metadata_mutations: Vec<TaskMetadataMutation>,
    expected_queued_recovery_id: Option<&str>,
    condition: Option<crate::ConditionStatement>,
) -> Result<Task> {
    let mut transaction = crate::begin_immediate(&db.pool).await?;
    db.fence_task_lease_in_tx(&mut transaction, id, "update_recovery_metadata")
        .await?;
    let task_row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
        .bind(id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
    let mut task = map_task(task_row)?;
    if task.deleted_at.is_some() {
        return Err(DbError::InvalidSoftDelete);
    }
    let expected_version = if crate::task_writer::owns_task(&task.id) {
        task.version
    } else {
        expected_version
    };
    if task.version != expected_version {
        return Err(DbError::VersionConflict);
    }
    if let Some(expected_id) = expected_queued_recovery_id {
        let metadata = TaskMetadata::parse(task.metadata_json.as_deref())
            .map_err(|error| DbError::Check(format!("invalid task metadata: {error}")))?;
        if metadata
            .extra
            .get("queued_recovery")
            .and_then(|queued| queued.get("id"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            != expected_id
        {
            return Err(DbError::VersionConflict);
        }
    } else {
        ensure_no_running_execution_in_tx(
            &mut transaction,
            id,
            None,
            workspace_id,
            overlapping_roles,
        )
        .await?;
    }

    let previous_error_annotation = task.error_annotation.clone();
    let previous_blocked_json = task.blocked_json.clone();
    let previous_failed_json = task.failed_json.clone();
    task.error_annotation = error_annotation;
    task.blocked_json = blocked_json;
    task.failed_json = failed_json;
    if !metadata_mutations.is_empty() {
        let mut metadata = TaskMetadata::parse(task.metadata_json.as_deref())
            .map_err(|error| DbError::Check(format!("invalid task metadata: {error}")))?;
        for mutation in metadata_mutations {
            apply_task_mutation(&mut transaction, &task.id, &mut metadata, mutation).await?;
        }
        task.metadata_json = metadata.to_json();
    }
    task.updated_at = updated_at.to_owned();
    task.version += 1;
    let result = sqlx::query(
        "UPDATE task
         SET error_annotation = ?, blocked_json = ?, failed_json = ?, metadata_json = ?,
             version = version + 1, updated_at = ?
         WHERE id = ? AND version = ? AND deleted_at IS NULL",
    )
    .bind(task.error_annotation.as_deref())
    .bind(task.blocked_json.as_deref())
    .bind(task.failed_json.as_deref())
    .bind(task.metadata_json.as_deref())
    .bind(&task.updated_at)
    .bind(&task.id)
    .bind(expected_version)
    .execute(&mut *transaction)
    .await?;
    if result.rows_affected() == 0 {
        return Err(DbError::VersionConflict);
    }
    match &condition {
        // The writer said what this write means: the condition is stated,
        // not mapped back from the fields just written.
        Some(statement) => {
            crate::task_condition::state(&mut transaction, &task.id, statement).await?
        }
        None => {
            crate::task_condition::produce(
                &mut transaction,
                &task.id,
                crate::ConditionChange::Legacy,
            )
            .await?
        }
    }
    task.condition = crate::task_condition::decode_or_unknown(
        &sqlx::query_scalar::<_, String>("SELECT condition_json FROM task WHERE id=?")
            .bind(&task.id)
            .fetch_one(&mut *transaction)
            .await?,
    );
    if interruption_fields_changed(
        &previous_error_annotation,
        &previous_blocked_json,
        &previous_failed_json,
        &task,
    ) {
        append_task_interruption_event(db, &mut transaction, &task).await?;
    }
    db.record_mutation_reply_in_tx(&mut transaction, &task)
        .await?;

    transaction.commit().await?;
    Ok(task)
}

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
        task.condition = crate::task_condition::decode_or_unknown(
            &sqlx::query_scalar::<_, String>("SELECT condition_json FROM task WHERE id=?")
                .bind(&task.id)
                .fetch_one(&mut **transaction)
                .await?,
        );
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
             SET metadata_json = ?, condition_json = CASE WHEN ? THEN ? ELSE condition_json END, updated_at = ?
             WHERE id = ? AND deleted_at IS NULL",
        )
        .bind(metadata_json.as_deref())
        .bind(condition.is_some())
        .bind(condition)
        .bind(updated_at)
        .bind(id)
        .execute(&mut *transaction)
        .await?;
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
             SET metadata_json = ?, condition_json = CASE WHEN ? THEN ? ELSE condition_json END, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND deleted_at IS NULL",
        )
        .bind(metadata_json.as_deref())
        .bind(condition.is_some())
        .bind(condition)
        .bind(updated_at)
        .bind(id)
        .bind(expected_version)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
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
             SET metadata_json = ?, condition_json = CASE WHEN ? THEN ? ELSE condition_json END, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND deleted_at IS NULL",
        )
        .bind(metadata_json.as_deref())
        .bind(condition.is_some())
        .bind(condition)
        .bind(updated_at)
        .bind(id)
        .bind(expected_version)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
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
             SET metadata_json = ?, condition_json = CASE WHEN ? THEN ? ELSE condition_json END, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND deleted_at IS NULL",
        )
        .bind(metadata_json.as_deref())
        .bind(condition.is_some())
        .bind(condition)
        .bind(updated_at)
        .bind(id)
        .bind(expected_version)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
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
             SET metadata_json = ?, condition_json = CASE WHEN ? THEN ? ELSE condition_json END, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND deleted_at IS NULL",
        )
        .bind(metadata_json.as_deref())
        .bind(condition.is_some())
        .bind(condition)
        .bind(&input.updated_at)
        .bind(&input.task_id)
        .bind(input.expected_task_version)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
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
             SET metadata_json = ?, condition_json = CASE WHEN ? THEN ? ELSE condition_json END, updated_at = ?, version = version + 1
             WHERE id = ? AND deleted_at IS NULL",
        )
        .bind(metadata_json.as_deref())
        .bind(condition.is_some())
        .bind(condition)
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
        task.condition = crate::task_condition::decode_or_unknown(
            &sqlx::query_scalar::<_, String>("SELECT condition_json FROM task WHERE id=?")
                .bind(&task.id)
                .fetch_one(&mut *transaction)
                .await?,
        );
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
        task.condition = crate::task_condition::decode_or_unknown(
            &sqlx::query_scalar::<_, String>("SELECT condition_json FROM task WHERE id=?")
                .bind(&task.id)
                .fetch_one(&mut *transaction)
                .await?,
        );
        self.record_mutation_reply_in_tx(&mut transaction, &task)
            .await?;

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

async fn update_task_status_inner(
    db: &SqliteDb,
    mut input: UpdateTaskStatus,
    recovery_marker: Option<&CreateTransitionLog>,
    latest_execution_authority: Option<&LatestExecutionAuthority>,
) -> Result<Option<Task>> {
    if recovery_marker.is_some_and(|marker| marker.task_id != input.id) {
        return Err(DbError::InvalidTransition);
    }
    let mut transaction = crate::begin_immediate(&db.pool).await?;
    db.fence_task_lease_in_tx(&mut transaction, &input.id, "update_task_status")
        .await?;
    let task_row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
        .bind(&input.id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
    let mut task = map_task(task_row)?;
    if task.deleted_at.is_some() {
        return Err(DbError::InvalidSoftDelete);
    }
    if crate::task_writer::owns_task(&input.id) {
        input.expected_version = task.version;
    }
    if task.version != input.expected_version {
        return Err(DbError::VersionConflict);
    }
    if let Some(authority) = latest_execution_authority {
        if !latest_execution_authority_matches_in_tx(&mut transaction, &task, authority).await? {
            transaction.commit().await?;
            return Ok(None);
        }
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

    crate::task_condition::produce(&mut transaction, &task.id, crate::ConditionChange::Entry)
        .await?;
    task.condition = crate::task_condition::decode_or_unknown(
        &sqlx::query_scalar::<_, String>("SELECT condition_json FROM task WHERE id=?")
            .bind(&task.id)
            .fetch_one(&mut *transaction)
            .await?,
    );
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
    Ok(Some(task))
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
    let current = db
        .get_task_in_tx(transaction, &task.id)
        .await?
        .ok_or(DbError::NotFound)?;
    let event = CreateDomainEvent::task_interruption_changed(&current);
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

pub(super) async fn list_page(
    connection: &mut sqlx::SqliteConnection,
    query: TaskListQuery,
) -> Result<Page<Task>> {
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
        .fetch_all(&mut *connection)
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
        Some(q.fetch_one(&mut *connection).await?)
    } else {
        None
    };
    page_from_items(items, &query.page, offset, total)
}

impl SqliteDb {
    pub async fn workspace_cleanup_task_ids_after(
        &self,
        after: &str,
        limit: i64,
    ) -> crate::Result<Vec<String>> {
        let result =
            sqlx::query_scalar::<_, String>("SELECT id FROM task WHERE id > ? ORDER BY id LIMIT ?")
                .bind(after)
                .bind(limit)
                .fetch_all(self.pool())
                .await?;
        Ok(result)
    }
}

#[derive(Debug, Clone)]
pub struct TaskOperatorConditionRow {
    pub id: String,
    pub title: String,
    pub condition_json: String,
    pub updated_at: String,
}

impl SqliteDb {
    pub async fn tasks_with_exhausted_infrastructure_checks(
        &self,
    ) -> crate::Result<Vec<TaskOperatorConditionRow>> {
        let rows = sqlx::query(
            "SELECT id, title, condition_json, updated_at FROM task
             WHERE condition_json LIKE '%infrastructure_exhausted%'
               AND deleted_at IS NULL AND archived_at IS NULL
               AND status NOT IN ('done', 'cancelled')
             ORDER BY updated_at DESC, id ASC LIMIT 50",
        )
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(TaskOperatorConditionRow {
                    id: row.try_get("id")?,
                    title: row.try_get("title")?,
                    condition_json: row.try_get("condition_json")?,
                    updated_at: row.try_get("updated_at")?,
                })
            })
            .collect()
    }
}

impl SqliteDb {
    pub async fn open_tasks_with_retired_auditor(&self) -> crate::Result<Vec<String>> {
        let result = sqlx::query_scalar("SELECT id FROM task WHERE deleted_at IS NULL AND status NOT IN ('done','cancelled') AND task_state_config IS NOT NULL AND json_valid(task_state_config) AND COALESCE(json_extract(task_state_config,'$.review.auditor_agent_id'), json_extract(task_state_config,'$.auditor_agent_id')) IS NOT NULL ORDER BY id LIMIT 11")
            .fetch_all(self.pool()).await?;
        Ok(result)
    }
}

#[derive(Debug, Clone)]
pub struct BlockedTaskOperatorRow {
    pub id: String,
    pub title: String,
    pub condition_json: String,
    pub updated_at: String,
}

impl SqliteDb {
    pub async fn blocked_task_operator_rows(&self) -> crate::Result<Vec<BlockedTaskOperatorRow>> {
        let rows = sqlx::query("SELECT id, title, condition_json, updated_at
             FROM task
             WHERE (status = 'blocked' OR json_extract(condition_json,'$.evidence.presentation.interruption_present') = 1)
               AND deleted_at IS NULL
               AND archived_at IS NULL
               AND status NOT IN ('done', 'cancelled')
             ORDER BY updated_at DESC, id ASC")
.fetch_all(self.pool()).await?;
        rows.into_iter()
            .map(|row| {
                Ok(BlockedTaskOperatorRow {
                    id: row.try_get("id")?,
                    title: row.try_get("title")?,
                    condition_json: row.try_get("condition_json")?,
                    updated_at: row.try_get("updated_at")?,
                })
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct TaskOperatorRetryRow {
    pub task_id: String,
    pub title: String,
    pub status: String,
    pub condition_json: String,
    pub attempt_count: i64,
    pub execution_spent: i64,
    pub last_error: Option<String>,
}

impl SqliteDb {
    pub async fn task_operator_retry_rows(&self) -> crate::Result<Vec<TaskOperatorRetryRow>> {
        let rows = sqlx::query("SELECT
                t.id AS task_id,
                t.title,
                t.status,
                t.condition_json,
                COUNT(tl.id) AS attempt_count,
                COALESCE((
                    SELECT b.spent FROM task_budget b
                    WHERE b.task_id = t.id AND b.kind = 'execution'
                ), 0) AS execution_spent,
                (
                    SELECT e.error
                    FROM execution e
                    WHERE e.task_id = t.id AND e.status = 'failed'
                    ORDER BY COALESCE(e.stopped_at, e.updated_at) DESC, e.id DESC
                    LIMIT 1
                ) AS last_error
             FROM task t
             LEFT JOIN transition_log tl ON tl.task_id = t.id AND tl.rejection = 1
             WHERE t.deleted_at IS NULL
               AND t.status NOT IN ('done', 'cancelled')
             GROUP BY t.id, t.title, t.status, t.condition_json
             HAVING COUNT(tl.id) >= 1 OR json_extract(t.condition_json,'$.evidence.presentation.retry_display.reason') IS NOT NULL OR execution_spent > 0
             ORDER BY attempt_count DESC, t.updated_at DESC, t.id ASC")
.fetch_all(self.pool()).await?;
        rows.into_iter()
            .map(|row| {
                Ok(TaskOperatorRetryRow {
                    task_id: row.try_get("task_id")?,
                    title: row.try_get("title")?,
                    status: row.try_get("status")?,
                    condition_json: row.try_get("condition_json")?,
                    attempt_count: row.try_get("attempt_count")?,
                    execution_spent: row.try_get("execution_spent")?,
                    last_error: row.try_get("last_error")?,
                })
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct MilestoneTaskSourceRow {
    pub version: i64,
    pub task_type: String,
    pub status: String,
    pub deleted_at: Option<String>,
    pub updated_at: String,
}

impl SqliteDb {
    pub async fn milestone_task_source_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<MilestoneTaskSourceRow>> {
        let row = sqlx::query(
            "SELECT version, task_type, status, deleted_at, updated_at
                 FROM task WHERE id = ? AND project_id = ?",
        )
        .bind(task_id)
        .bind(project_id)
        .fetch_optional(connection)
        .await?;
        row.map(|row| {
            Ok(MilestoneTaskSourceRow {
                version: row.try_get("version")?,
                task_type: row.try_get("task_type")?,
                status: row.try_get("status")?,
                deleted_at: row.try_get("deleted_at")?,
                updated_at: row.try_get("updated_at")?,
            })
        })
        .transpose()
    }
}

impl SqliteDb {
    pub async fn pending_recovery_command_exists(
        &self,
        task_id: &str,
        prefix: &str,
        matching_prefix: &str,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_step WHERE task_id=? AND kind='command' AND status IN ('pending','claimed') AND substr(causation_key,1,length(?))=?)")
.bind(task_id)
.bind(prefix)
.bind(matching_prefix)
.fetch_one(self.pool())
.await?)
    }
}

impl SqliteDb {
    pub async fn pending_retired_review_settlement_exists(
        &self,
        task_id: &str,
        prefix: &str,
        matching_prefix: &str,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_step WHERE task_id=? AND kind='command' AND status IN ('pending','claimed') AND substr(causation_key,1,length(?))=?)")
.bind(task_id)
.bind(prefix)
.bind(matching_prefix)
.fetch_one(self.pool())
.await?)
    }
}

#[derive(Debug)]
pub struct AnnotateOwnerRecoveryInTxInput<'a> {
    pub annotation: String,
    pub updated_at: String,
    pub task_id: &'a str,
    pub expected_version: i64,
    pub terminal_states_json: String,
}

impl SqliteDb {
    pub async fn annotate_owner_recovery_in_tx(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        target_id: &str,
        input: AnnotateOwnerRecoveryInTxInput<'_>,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self, target_id, "UPDATE task SET error_annotation = ?, updated_at = ?, version = version + 1 WHERE id = ? AND version = ?
            AND deleted_at IS NULL AND failed_json IS NULL AND status NOT IN (SELECT value FROM json_each(?))
            AND (error_annotation IS NULL OR (json_valid(error_annotation) AND json_extract(error_annotation, '$.blocking_reason') IN ('owner_disconnected_timeout', 'owner_lost_execution')))")
.bind(input.annotation)
.bind(input.updated_at)
.bind(input.task_id)
.bind(input.expected_version)
.bind(input.terminal_states_json)
.identity_fenced()
.execute_in_tx(transaction)
.await
    }
}

impl SqliteDb {
    pub async fn clear_reconciled_owner_waits_in_tx(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        updated_at: String,
        task_id: &str,
        parent_task_id: &str,
    ) -> crate::Result<crate::task_writer::BulkTaskQueryResult> {
        crate::task_writer::BulkTaskQuery::new(self, "UPDATE task SET metadata_json = json_remove(COALESCE(metadata_json, '{}'),
        '$.dispatch_disposition', '$.deferred_dispatch', '$.owner_wait'), version = version + 1, updated_at = ?
        WHERE (id = ? OR parent_task_id = ?) AND deleted_at IS NULL
        AND (json_type(metadata_json, '$.dispatch_disposition') IS NOT NULL
             OR json_type(metadata_json, '$.deferred_dispatch') IS NOT NULL
             OR json_type(metadata_json, '$.owner_wait') IS NOT NULL)")
.bind(updated_at)
.bind(task_id)
.bind(parent_task_id)
.execute_in_tx(transaction)
.await
    }
}

impl SqliteDb {
    pub async fn owner_reconnect_exhausted_review_task_ids_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        parent_task_id: &str,
    ) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar("SELECT id FROM task WHERE (id=? OR parent_task_id=?) AND deleted_at IS NULL AND entry_barrier_json IS NOT NULL AND json_extract(error_annotation,'$.blocking_reason')='review_ci_infrastructure_exhausted'")
.bind(task_id)
.bind(parent_task_id)
.fetch_all(connection)
.await?)
    }
}

impl SqliteDb {
    pub async fn restart_owner_reconnect_review_barrier_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        window: &str,
        task_id: &str,
    ) -> crate::Result<u64> {
        let result = sqlx::query("UPDATE task SET entry_barrier_json=json_set(entry_barrier_json,'$.started_at',?) WHERE id=?")
.bind(window)
.bind(task_id)
.execute(connection)
.await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn restore_owner_reconnect_review_retry_in_tx(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        updated_at: String,
        task_id: &str,
        parent_task_id: &str,
    ) -> crate::Result<crate::task_writer::BulkTaskQueryResult> {
        crate::task_writer::BulkTaskQuery::new(self, "UPDATE task SET blocked_json = NULL,
        entry_barrier_json = entry_barrier_json,
        error_annotation = json_set(error_annotation, '$.blocking_reason', 'review_ci_infrastructure'),
        version = version + 1, updated_at = ? WHERE (id = ? OR parent_task_id = ?) AND deleted_at IS NULL
        AND entry_barrier_json IS NOT NULL AND json_extract(error_annotation, '$.blocking_reason') = 'review_ci_infrastructure_exhausted'")
.bind(updated_at)
.bind(task_id)
.bind(parent_task_id)
.execute_in_tx(transaction)
.await
    }
}

#[derive(Debug)]
pub struct ClearRemovedMachineCancelAnnotationInput<'a> {
    pub updated_at: String,
    pub task_id: &'a str,
    pub workspace_task_id: &'a str,
    pub step_task_id: &'a str,
    pub execution_task_id: &'a str,
}

impl SqliteDb {
    pub async fn clear_removed_machine_cancel_annotation(
        &self,
        target_id: &str,
        input: ClearRemovedMachineCancelAnnotationInput<'_>,
    ) -> crate::Result<u64> {
        crate::task_writer::TaskQuery::new(self, target_id, "UPDATE task SET error_annotation=NULL, metadata_json=CASE WHEN json_valid(metadata_json) THEN json_remove(metadata_json,'$.dispatch_disposition','$.deferred_dispatch') ELSE metadata_json END, version=version+1,updated_at=? WHERE id=? AND deleted_at IS NULL AND json_valid(error_annotation) AND json_extract(error_annotation,'$.blocking_reason')='pending_remote_cancel' AND NOT EXISTS(SELECT 1 FROM pending_remote_cancel c LEFT JOIN workspace w ON w.id=c.workspace_id LEFT JOIN task_step s ON s.id=c.step_id WHERE w.task_id=? OR s.task_id=? OR EXISTS(SELECT 1 FROM execution e WHERE e.task_id=? AND e.workspace_id=c.workspace_id))")
.bind(input.updated_at)
.bind(input.task_id)
.bind(input.workspace_task_id)
.bind(input.step_task_id)
.bind(input.execution_task_id)
.identity_fenced()
.execute(self.pool())
.await
    }
}

impl SqliteDb {
    pub async fn refresh_remaining_machine_cancel_annotation(
        &self,
        target_id: &str,
        annotation: String,
        updated_at: String,
        task_id: &str,
        previous_annotation: &str,
    ) -> crate::Result<u64> {
        crate::task_writer::TaskQuery::new(self, target_id, "UPDATE task SET error_annotation=?,version=version+1,updated_at=? WHERE id=? AND error_annotation=?")
.bind(annotation)
.bind(updated_at)
.bind(task_id)
.bind(previous_annotation)
.identity_fenced()
.execute(self.pool())
.await
    }
}

impl SqliteDb {
    pub async fn clear_removed_machine_metadata_wait(
        &self,
        target_id: &str,
        key: &str,
        owner: &str,
        task_id: &str,
        daemon_id: &str,
    ) -> crate::Result<u64> {
        crate::task_writer::TaskQuery::new(self, target_id, format!("UPDATE task SET metadata_json=json_remove(metadata_json,'$.{key}','$.deferred_dispatch','$.dispatch_disposition') WHERE id=? AND json_valid(metadata_json) AND json_extract(metadata_json,'$.{owner}')=?"))
.bind(task_id)
.bind(daemon_id)
.identity_fenced()
.execute(self.pool())
.await
    }
}

impl SqliteDb {
    pub async fn clear_removed_machine_queued_recovery(
        &self,
        target_id: &str,
        metadata_path: String,
        updated_at: String,
        task_id: &str,
    ) -> crate::Result<u64> {
        crate::task_writer::TaskQuery::new(self, target_id, "UPDATE task SET metadata_json=json_remove(metadata_json,?,'$.deferred_dispatch','$.dispatch_disposition'),version=version+1,updated_at=? WHERE id=? AND json_valid(metadata_json)")
.bind(metadata_path)
.bind(updated_at)
.bind(task_id)
.identity_fenced()
.execute(self.pool())
.await
    }
}

#[derive(Debug)]
pub struct AnnotateRemovedMachineTaskInput<'a> {
    pub annotation: String,
    pub updated_at: String,
    pub task_id: &'a str,
    pub terminal_states_json: String,
    pub owner_loss_reasons_json: String,
}

impl SqliteDb {
    pub async fn annotate_removed_machine_task(
        &self,
        target_id: &str,
        input: AnnotateRemovedMachineTaskInput<'_>,
    ) -> crate::Result<u64> {
        crate::task_writer::TaskQuery::new(self, target_id, "UPDATE task SET error_annotation=?,
            blocked_json=CASE WHEN json_valid(error_annotation) AND json_extract(error_annotation,'$.blocked_by')='system:workflow' THEN NULL ELSE blocked_json END,
            updated_at=?, version=version+1
         WHERE id=? AND deleted_at IS NULL AND failed_json IS NULL
           AND status NOT IN (SELECT value FROM json_each(?))
           AND (error_annotation IS NULL OR (json_valid(error_annotation)
                AND json_extract(error_annotation,'$.blocking_reason') IN (SELECT value FROM json_each(?))))")
.bind(input.annotation)
.bind(input.updated_at)
.bind(input.task_id)
.bind(input.terminal_states_json)
.bind(input.owner_loss_reasons_json)
.identity_fenced()
.execute(self.pool())
.await
    }
}

#[derive(Debug)]
pub struct RecordPlacementRefusalInTxInput<'a> {
    pub deferral: &'a Option<String>,
    pub deferral_value: &'a Option<String>,
    pub annotation: &'a Option<String>,
    pub blocked: &'a Option<String>,
    pub updated_at: String,
    pub version_increment: i64,
    pub task_id: &'a str,
    pub expected_version: i64,
    pub expected_status: &'a str,
}

impl SqliteDb {
    pub async fn record_placement_refusal_in_tx(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        target_id: &str,
        input: RecordPlacementRefusalInTxInput<'_>,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self, target_id, "UPDATE task SET metadata_json = CASE WHEN ? IS NULL THEN
            json_remove(COALESCE(metadata_json, '{}'), '$.deferred_dispatch') ELSE
            json_set(COALESCE(metadata_json, '{}'), '$.deferred_dispatch', json(?)) END,
            error_annotation = COALESCE(?, error_annotation),
            blocked_json = COALESCE(?, blocked_json), updated_at = ?, version = version + ? WHERE id = ? AND version = ? AND status = ?")
.bind(input.deferral)
.bind(input.deferral_value)
.bind(input.annotation)
.bind(input.blocked)
.bind(input.updated_at)
.bind(input.version_increment)
.bind(input.task_id)
.bind(input.expected_version)
.bind(input.expected_status)
.identity_fenced()
.execute_in_tx(transaction)
.await
    }
}

impl SqliteDb {
    pub async fn record_placement_owner_wait_in_tx(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        target_id: &str,
        owner_wait: String,
        task_id: &str,
        expected_status: &str,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self, target_id, "UPDATE task SET metadata_json = json_set(COALESCE(metadata_json, '{}'), '$.owner_wait', json(?)) WHERE id = ? AND status = ?")
.bind(owner_wait)
.bind(task_id)
.bind(expected_status)
.identity_fenced()
.execute_in_tx(transaction)
.await
    }
}

impl SqliteDb {
    pub async fn clear_expired_placement_wait_in_tx(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        target_id: &str,
        task_id: &str,
        expected_version: i64,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self, target_id, "UPDATE task SET metadata_json = json_remove(metadata_json, '$.owner_wait', '$.deferred_dispatch') WHERE id = ? AND version = ?")
.bind(task_id)
.bind(expected_version)
.execute_in_tx(transaction)
.await
    }
}

impl SqliteDb {
    pub async fn clear_claim_placement_waits_in_tx(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        target_id: &str,
        task_id: &str,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self, target_id, "UPDATE task SET metadata_json = json_remove(metadata_json, '$.owner_wait', '$.environment_wait', '$.deferred_dispatch')
            WHERE id = ? AND (json_type(metadata_json, '$.owner_wait') IS NOT NULL
            OR json_type(metadata_json, '$.environment_wait') IS NOT NULL)")
.bind(task_id)
.execute_in_tx(transaction)
.await
    }
}

impl SqliteDb {
    pub async fn wake_unsettled_task_condition_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>("SELECT condition_json FROM task WHERE id=? AND deleted_at IS NULL AND json_extract(condition_json,'$.kind') != 'settled'")
.bind(task_id)
.fetch_optional(connection)
.await?)
    }
}

impl SqliteDb {
    pub async fn wake_live_task_project_id_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT project_id FROM task WHERE id=? AND deleted_at IS NULL",
        )
        .bind(task_id)
        .fetch_optional(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn wake_budget_task_project_id_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT project_id FROM task WHERE id = ?")
                .bind(task_id)
                .fetch_optional(connection)
                .await?,
        )
    }
}

/// Typed fields retain decoding failures for the caller's original conditional/error handling.
#[derive(Debug)]
pub struct AttentionTaskContextRow {
    pub title: crate::Result<String>,
    pub status: crate::Result<String>,
    pub version: crate::Result<i64>,
}

impl SqliteDb {
    pub async fn attention_task_context(
        &self,
        task_id: &str,
    ) -> crate::Result<Option<AttentionTaskContextRow>> {
        let rows = sqlx::query("SELECT title, status, version FROM task WHERE id = ?")
            .bind(task_id)
            .fetch_optional(self.pool())
            .await?;
        rows.map(|row| {
            Ok(AttentionTaskContextRow {
                title: row.try_get("title").map_err(crate::DbError::from),
                status: row.try_get("status").map_err(crate::DbError::from),
                version: row.try_get("version").map_err(crate::DbError::from),
            })
        })
        .transpose()
    }
}

impl SqliteDb {
    pub async fn attention_event_task_project_id(
        &self,
        task_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT project_id FROM task WHERE id = ?")
                .bind(task_id)
                .fetch_optional(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn attention_task_assignee_identity(
        &self,
        task_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT assignee_id FROM task
                     WHERE id = ? AND assignee_type = 'agent' AND assignee_id IS NOT NULL",
        )
        .bind(task_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_task_identity_assignment_count(
        &self,
        task_id: &str,
        identity_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task
                         WHERE id = ? AND assignee_type = 'agent' AND assignee_id = ?",
        )
        .bind(task_id)
        .bind(identity_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_task_agent_assignment_count(&self, task_id: &str) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task
                         WHERE id = ? AND assignee_type = 'agent' AND assignee_id IS NOT NULL",
        )
        .bind(task_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_task_identity_access_count(
        &self,
        task_id: &str,
        identity_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task
                 WHERE id = ? AND assignee_type = 'agent' AND assignee_id = ?",
        )
        .bind(task_id)
        .bind(identity_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_scope_task_project_id(
        &self,
        task_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT project_id FROM task WHERE id = ?")
                .bind(task_id)
                .fetch_optional(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn attention_agent_focus_task_id(
        &self,
        assignee_id: &str,
        commitment_owner_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT task_id
             FROM (
                 SELECT t.id AS task_id, t.updated_at
                 FROM task t
                 WHERE t.assignee_id = ?
                   AND t.status = 'in_progress'
                   AND t.deleted_at IS NULL
                 UNION
                 SELECT t.id AS task_id, t.updated_at
                 FROM task t
                 JOIN agent_commitment c ON c.originating_task_id = t.id
                 WHERE c.owner_identity_id = ?
                   AND c.status IN ('proposed', 'open', 'accepted', 'in_progress', 'blocked')
                   AND t.deleted_at IS NULL
             )
             ORDER BY updated_at DESC, task_id DESC
             LIMIT 1",
        )
        .bind(assignee_id)
        .bind(commitment_owner_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub(crate) fn attention_project_visibility_predicate(
        user_id: &str,
        project_id: Option<&str>,
    ) -> (String, Vec<String>) {
        match project_id {
            Some(project_id) => ("t.project_id = ?".to_owned(), vec![project_id.to_owned()]),
            None => (
                "(p.owner_id IS NULL OR p.owner_id = ? OR EXISTS (
                    SELECT 1 FROM project_member pm
                    WHERE pm.project_id = p.id AND pm.user_id = ?
                ))"
                .to_owned(),
                vec![user_id.to_owned(), user_id.to_owned()],
            ),
        }
    }
}

/// Typed fields retain decoding failures for the caller's original conditional/error handling.
#[derive(Debug)]
pub struct AttentionVisibleWorkItemsRow {
    pub id: crate::Result<String>,
    pub project_id: crate::Result<String>,
    pub title: crate::Result<String>,
    pub status: crate::Result<String>,
    pub priority: crate::Result<i64>,
    pub updated_at: crate::Result<String>,
}

impl SqliteDb {
    pub async fn attention_visible_work_items(
        &self,
        user_id: &str,
        project_id: Option<&str>,
        statuses: &[&str],
        limit: i64,
    ) -> crate::Result<Vec<AttentionVisibleWorkItemsRow>> {
        let status_placeholders = vec!["?"; statuses.len()].join(", ");
        let status_values = statuses.to_vec();
        let (project_predicate, project_values) =
            Self::attention_project_visibility_predicate(user_id, project_id);
        let sql = format!(
            "SELECT t.id, t.project_id, t.title, t.status, t.priority, t.updated_at
             FROM task t JOIN project p ON p.id = t.project_id
             WHERE t.deleted_at IS NULL AND t.status IN ({status_placeholders})
               AND {project_predicate}
             ORDER BY t.priority DESC, t.updated_at ASC, t.id ASC LIMIT ?"
        );
        let mut query = sqlx::query(&sql);
        for status in status_values {
            query = query.bind(status);
        }
        for value in project_values {
            query = query.bind(value);
        }
        let rows = query.bind(limit).fetch_all(self.pool()).await?;
        Ok(rows
            .into_iter()
            .map(|row| AttentionVisibleWorkItemsRow {
                id: row.try_get("id").map_err(crate::DbError::from),
                project_id: row.try_get("project_id").map_err(crate::DbError::from),
                title: row.try_get("title").map_err(crate::DbError::from),
                status: row.try_get("status").map_err(crate::DbError::from),
                priority: row.try_get("priority").map_err(crate::DbError::from),
                updated_at: row.try_get("updated_at").map_err(crate::DbError::from),
            })
            .collect())
    }
}

/// Typed fields retain decoding failures for the caller's original conditional/error handling.
#[derive(Debug)]
pub struct AttentionVisibleRecentOutcomesRow {
    pub id: crate::Result<String>,
    pub project_id: crate::Result<String>,
    pub title: crate::Result<String>,
    pub status: crate::Result<String>,
    pub updated_at: crate::Result<String>,
}

impl SqliteDb {
    pub async fn attention_visible_recent_outcomes(
        &self,
        user_id: &str,
        project_id: Option<&str>,
        limit: i64,
    ) -> crate::Result<Vec<AttentionVisibleRecentOutcomesRow>> {
        let (predicate, values) = Self::attention_project_visibility_predicate(user_id, project_id);
        let sql = format!(
            "SELECT t.id, t.project_id, t.title, t.status, t.updated_at
             FROM task t JOIN project p ON p.id = t.project_id
             WHERE t.deleted_at IS NULL AND t.status IN ('done', 'cancelled', 'blocked')
               AND {predicate}
             ORDER BY t.updated_at DESC, t.id DESC LIMIT ?"
        );
        let mut query = sqlx::query(&sql);
        for value in values {
            query = query.bind(value);
        }
        let rows = query.bind(limit).fetch_all(self.pool()).await?;
        Ok(rows
            .into_iter()
            .map(|row| AttentionVisibleRecentOutcomesRow {
                id: row.try_get("id").map_err(crate::DbError::from),
                project_id: row.try_get("project_id").map_err(crate::DbError::from),
                title: row.try_get("title").map_err(crate::DbError::from),
                status: row.try_get("status").map_err(crate::DbError::from),
                updated_at: row.try_get("updated_at").map_err(crate::DbError::from),
            })
            .collect())
    }
}

/// Typed fields retain decoding failures for the caller's original conditional/error handling.
#[derive(Debug)]
pub struct AttentionVisibleQueuedTaskCountRow {
    pub count: crate::Result<i64>,
}

impl SqliteDb {
    pub async fn attention_visible_queued_task_count(
        &self,
        user_id: &str,
        project_id: Option<&str>,
    ) -> crate::Result<AttentionVisibleQueuedTaskCountRow> {
        let (predicate, values) = Self::attention_project_visibility_predicate(user_id, project_id);
        let queued_sql = format!(
            "SELECT COUNT(*) AS count FROM task t JOIN project p ON p.id = t.project_id
             WHERE t.deleted_at IS NULL AND t.status IN ('todo', 'backlog') AND {predicate}"
        );
        let mut query = sqlx::query(&queued_sql);
        for value in &values {
            query = query.bind(value);
        }
        let queued = query.fetch_one(self.pool()).await?;
        Ok(AttentionVisibleQueuedTaskCountRow {
            count: queued.try_get("count").map_err(crate::DbError::from),
        })
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct TaskActionScopeAssignmentRow {
    row: sqlx::sqlite::SqliteRow,
}

impl TaskActionScopeAssignmentRow {
    pub fn status(&self) -> crate::Result<String> {
        Ok(self.row.try_get("status")?)
    }
    pub fn assignee_type(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("assignee_type")?)
    }
    pub fn assignee_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("assignee_id")?)
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct TaskActionScopeRoleRow {
    row: sqlx::sqlite::SqliteRow,
}

impl TaskActionScopeRoleRow {
    pub fn role_name(&self) -> crate::Result<String> {
        Ok(self.row.try_get("role_name")?)
    }
}

impl SqliteDb {
    pub async fn action_scope_visible_task_count(&self, task_id: &str) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task WHERE id = ? AND deleted_at IS NULL",
        )
        .bind(task_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn task_action_scope_assignment(
        &self,
        task_id: &str,
    ) -> crate::Result<TaskActionScopeAssignmentRow> {
        let rows = sqlx::query("SELECT status, assignee_type, assignee_id FROM task WHERE id = ?")
            .bind(task_id)
            .fetch_one(self.pool())
            .await?;
        Ok(TaskActionScopeAssignmentRow { row: rows })
    }
}

impl SqliteDb {
    pub async fn task_action_scope_roles(
        &self,
        task_id: &str,
        identity_id: &str,
    ) -> crate::Result<Vec<TaskActionScopeRoleRow>> {
        let rows = sqlx::query(
            "SELECT role_name FROM task_role_assignment
                 WHERE task_id = ? AND assignee_type = 'agent' AND assignee_id = ?",
        )
        .bind(task_id)
        .bind(identity_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| TaskActionScopeRoleRow { row })
            .collect())
    }
}

impl SqliteDb {
    pub async fn action_approver_task_assignment_exists(
        &self,
        task_id: &str,
        identity_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT (
                    EXISTS (
                        SELECT 1 FROM task
                        WHERE id = ? AND assignee_type = 'agent' AND assignee_id = ?
                    )
                    OR EXISTS (
                        SELECT 1 FROM task_role_assignment
                        WHERE task_id = ? AND assignee_type = 'agent' AND assignee_id = ?
                    )
                )",
        )
        .bind(task_id)
        .bind(identity_id)
        .bind(task_id)
        .bind(identity_id)
        .fetch_one(self.pool())
        .await?)
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct EffectiveProjectTaskStatusCountRow {
    row: sqlx::sqlite::SqliteRow,
}

impl EffectiveProjectTaskStatusCountRow {
    pub fn key(&self) -> crate::Result<String> {
        Ok(self.row.try_get("key")?)
    }
    pub fn count(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("count")?)
    }
}

impl SqliteDb {
    pub async fn effective_project_task_status_counts(
        &self,
        project_id: &str,
    ) -> crate::Result<Vec<EffectiveProjectTaskStatusCountRow>> {
        let rows = sqlx::query(
            "SELECT status AS key, COUNT(*) AS count
         FROM task WHERE project_id = ? AND deleted_at IS NULL
         GROUP BY status ORDER BY status ASC",
        )
        .bind(project_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| EffectiveProjectTaskStatusCountRow { row })
            .collect())
    }
}

impl SqliteDb {
    pub async fn escalation_visible_project_task_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        project_id: &str,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM task WHERE id=? AND project_id=? AND deleted_at IS NULL)",
        )
        .bind(task_id)
        .bind(project_id)
        .fetch_one(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn escalation_unsettled_task_condition_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>("SELECT condition_json FROM task WHERE id=? AND project_id=? AND deleted_at IS NULL AND json_extract(condition_json,'$.kind') != 'settled'")
            .bind(task_id)
            .bind(project_id)
            .fetch_optional(connection)
            .await?)
    }
}

impl SqliteDb {
    pub async fn environment_probe_task_metadata(
        &self,
        task_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(
            sqlx::query_scalar::<_, Option<String>>("SELECT metadata_json FROM task WHERE id=?")
                .bind(task_id)
                .fetch_optional(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn environment_wait_task_metadata_at_version_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        expected_version: i64,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT metadata_json FROM task WHERE id = ? AND version = ?",
        )
        .bind(task_id)
        .bind(expected_version)
        .fetch_optional(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn record_environment_probe_error_for_task_waits_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        error: &str,
        project_id: &str,
        machine_json: String,
    ) -> crate::Result<crate::task_writer::BulkTaskQueryResult> {
        crate::task_writer::BulkTaskQuery::new(self,"UPDATE task SET metadata_json=json_set(metadata_json,'$.environment_wait.last_error',?,'$.deferred_dispatch.reason',COALESCE(json_extract(metadata_json,'$.deferred_dispatch.reason'),'environment_probe_pending') || '; last failure: ' || ?) WHERE project_id=? AND json_valid(metadata_json) AND json_extract(metadata_json,'$.environment_wait.machine')=json(?) AND json_extract(metadata_json,'$.environment_wait.kind')='environment_probe_pending'").bind(error).bind(error).bind(project_id).bind(machine_json).execute_in_tx(tx).await
    }
}

impl SqliteDb {
    pub async fn persist_environment_probe_pending_wait_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        task_id: &str,
        marker_json: String,
        wait_json: String,
        expected_version: i64,
        expected_status: &str,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self,task_id,"UPDATE task SET metadata_json = json_set(COALESCE(metadata_json, '{}'), '$.deferred_dispatch', json(?), '$.environment_wait', json(?)) WHERE id = ? AND version = ? AND status = ?").bind(marker_json).bind(wait_json).bind(task_id).bind(expected_version).bind(expected_status).identity_fenced().execute_in_tx(tx).await
    }
}

impl SqliteDb {
    pub async fn persist_environment_machine_wait_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        task_id: &str,
        marker_json: String,
        wait_json: String,
        expected_version: i64,
        expected_status: &str,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self,task_id,"UPDATE task SET metadata_json = json_set(json_remove(COALESCE(metadata_json, '{}'), '$.owner_wait'), '$.environment_wait', json(?), '$.deferred_dispatch', json(?)) WHERE id = ? AND version = ? AND status = ?").bind(marker_json).bind(wait_json).bind(task_id).bind(expected_version).bind(expected_status).identity_fenced().execute_in_tx(tx).await
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct WorkspaceLeaseTaskAssignmentRow {
    row: sqlx::sqlite::SqliteRow,
}

impl WorkspaceLeaseTaskAssignmentRow {
    pub fn assignee_type_unchecked(&self) -> Option<String> {
        self.row.get("assignee_type")
    }
    pub fn assignee_id_unchecked(&self) -> Option<String> {
        self.row.get("assignee_id")
    }
    pub fn task_type_unchecked(&self) -> String {
        self.row.get("task_type")
    }
    pub fn charter_status_unchecked(&self) -> String {
        self.row.get("charter_status")
    }
    pub fn charter_setup_required_unchecked(&self) -> i64 {
        self.row.get("charter_setup_required")
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct WorkspaceLeaseRoleAssignmentRow {
    row: sqlx::sqlite::SqliteRow,
}

impl WorkspaceLeaseRoleAssignmentRow {
    pub fn assignee_type_unchecked(&self) -> Option<String> {
        self.row.get("assignee_type")
    }
    pub fn assignee_id_unchecked(&self) -> Option<String> {
        self.row.get("assignee_id")
    }
}

impl SqliteDb {
    pub async fn task_governance_replacement_project(
        &self,
        task_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT project_id FROM task WHERE id = ? AND deleted_at IS NULL",
        )
        .bind(task_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn workspace_lease_task_assignment_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<Option<WorkspaceLeaseTaskAssignmentRow>> {
        let rows = sqlx::query(
            "SELECT t.project_id, t.assignee_type, t.assignee_id,
                    t.task_type, p.charter_status, p.charter_setup_required
             FROM task t
             JOIN project p ON p.id = t.project_id
             WHERE t.id = ? AND t.deleted_at IS NULL",
        )
        .bind(task_id)
        .fetch_optional(connection)
        .await?;
        Ok(rows.map(|row| WorkspaceLeaseTaskAssignmentRow { row }))
    }
}

impl SqliteDb {
    pub async fn workspace_lease_role_assignment_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        role_name: &str,
    ) -> crate::Result<Option<WorkspaceLeaseRoleAssignmentRow>> {
        let rows = sqlx::query(
            "SELECT assignee_type, assignee_id
             FROM task_role_assignment WHERE task_id = ? AND role_name = ?",
        )
        .bind(task_id)
        .bind(role_name)
        .fetch_optional(connection)
        .await?;
        Ok(rows.map(|row| WorkspaceLeaseRoleAssignmentRow { row }))
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct NativeWorkTaskRow {
    row: sqlx::sqlite::SqliteRow,
}
impl NativeWorkTaskRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn condition_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("condition_json")?)
    }
    pub fn parent_task_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("parent_task_id")?)
    }
    pub fn subtask_order(&self) -> crate::Result<Option<i64>> {
        Ok(self.row.try_get("subtask_order")?)
    }
    pub fn version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("version")?)
    }
    pub fn title(&self) -> crate::Result<String> {
        Ok(self.row.try_get("title")?)
    }
    pub fn status(&self) -> crate::Result<String> {
        Ok(self.row.try_get("status")?)
    }
    pub fn priority(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("priority")?)
    }
    pub fn assignee_type(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("assignee_type")?)
    }
    pub fn assignee_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("assignee_id")?)
    }
}

impl SqliteDb {
    pub async fn native_project_work_tasks(
        &self,
        project_id: &str,
        limit: i64,
    ) -> crate::Result<Vec<NativeWorkTaskRow>> {
        let rows = sqlx::query(
            "SELECT id, parent_task_id, subtask_order, version,
                            title, status, priority, assignee_type, assignee_id,
                            condition_json
                     FROM task WHERE project_id = ? AND deleted_at IS NULL
                     ORDER BY updated_at DESC, id DESC LIMIT ?",
        )
        .bind(project_id)
        .bind(limit)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| NativeWorkTaskRow { row })
            .collect())
    }
}

impl SqliteDb {
    pub async fn native_scope_work_task(
        &self,
        task_id: &str,
    ) -> crate::Result<Vec<NativeWorkTaskRow>> {
        let rows = sqlx::query(
            "SELECT id, parent_task_id, subtask_order, version,
                        title, status, priority, assignee_type, assignee_id,
                        condition_json
                     FROM task WHERE id = ? AND deleted_at IS NULL LIMIT 1",
        )
        .bind(task_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| NativeWorkTaskRow { row })
            .collect())
    }
}

impl SqliteDb {
    pub async fn native_task_project_id(&self, task_id: &str) -> crate::Result<Option<String>> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT project_id FROM task WHERE id = ?")
                .bind(task_id)
                .fetch_optional(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn native_task_scope_summary(
        &self,
        id: &str,
    ) -> crate::Result<Option<NativeScopeSummaryRow>> {
        let rows =
            sqlx::query("SELECT id, project_id, title, status, priority FROM task WHERE id = ?")
                .bind(id)
                .fetch_optional(self.pool())
                .await?;
        Ok(rows.map(|row| NativeScopeSummaryRow { row }))
    }
}

impl SqliteDb {
    pub async fn workspace_gc_task_log_references(&self, task_id: &str) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS (
                SELECT 1 FROM review
                WHERE task_id = ?1 AND status IN ('running', 'awaiting_human')
             ) OR EXISTS (
                SELECT 1 FROM attention_projection
                WHERE status <> 'resolved'
                  AND ((scope_type = 'task' AND scope_id = ?1)
                       OR json_extract(details_json, '$.task_id') = ?1
                       OR json_extract(details_json, '$.task.id') = ?1)
             )",
        )
        .bind(task_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn workspace_gc_legacy_log_task_exists(&self, task_id: &str) -> crate::Result<i64> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT EXISTS (SELECT 1 FROM task WHERE id = ?)")
                .bind(task_id)
                .fetch_one(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn workspace_gc_root_task_exists(&self, task_id: &str) -> crate::Result<i64> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT EXISTS (SELECT 1 FROM task WHERE id = ?)")
                .bind(task_id)
                .fetch_one(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn workspace_gc_task_active_uses(&self, task_id: &str) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>("SELECT EXISTS (
                SELECT 1 FROM execution
                WHERE (task_id = ?1 OR workspace_id IN (SELECT id FROM workspace WHERE task_id = ?1))
                  AND status = 'running'
             ) OR EXISTS (
                SELECT 1 FROM workspace_lease wl
                LEFT JOIN execution e ON e.id = wl.execution_id
                WHERE (wl.task_id = ?1
                       OR e.workspace_id IN (SELECT id FROM workspace WHERE task_id = ?1))
                  AND wl.status = 'active'
             ) OR EXISTS (
                SELECT 1 FROM task child JOIN execution e ON e.task_id = child.id
                WHERE child.parent_task_id = ?1 AND e.status = 'running'
             )",)
.bind(task_id)
.fetch_one(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn repository_provision_environment_wait_tasks(
        &self,
        project_id: &str,
        machine_json: &str,
    ) -> crate::Result<Vec<(String, i64)>> {
        Ok(sqlx::query_as::<_, (String, i64)>("SELECT id,version FROM task WHERE project_id=? AND json_valid(metadata_json) AND json_extract(metadata_json,'$.environment_wait.machine')=json(?)",)
.bind(project_id)
.bind(machine_json)
.fetch_all(self.pool()).await?)
    }
}

pub struct RepositoryProvisionTaskWaitInput<'a> {
    pub task_id: &'a str,
    pub reason: &'a str,
    pub version: i64,
    pub machine_json: &'a str,
    pub project_id: &'a str,
    pub daemon_id: &'a Option<String>,
    pub runtime_id: &'a Option<String>,
    pub readiness_version: i64,
}
impl SqliteDb {
    pub async fn record_repository_provision_task_wait_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        input: RepositoryProvisionTaskWaitInput<'_>,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self, input.task_id, "UPDATE task SET metadata_json=json_set(metadata_json,'$.deferred_dispatch.reason',?) WHERE id=? AND version=? AND json_extract(metadata_json,'$.environment_wait.machine')=json(?) AND COALESCE((SELECT version FROM project_machine_readiness WHERE project_id=? AND owner_kind='daemon' AND daemon_id=? AND runtime_id=?),-1)=?")
.bind(input.reason).bind(input.task_id).bind(input.version).bind(input.machine_json).bind(input.project_id).bind(input.daemon_id).bind(input.runtime_id).bind(input.readiness_version).identity_fenced().execute_in_tx(tx).await
    }
}

impl SqliteDb {
    pub async fn validation_observed_task_status(
        &self,
        task_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT status FROM task
                 WHERE id = ? AND project_id = ? AND deleted_at IS NULL",
        )
        .bind(task_id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn milestone_definition_task_reference(
        &self,
        task_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM task WHERE id = ? AND project_id = ? LIMIT 1",
        )
        .bind(task_id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn decision_governed_task_reference(
        &self,
        task_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT 1
                 FROM task AS t
                 JOIN project_task_governance AS g ON g.task_id = t.id
                 WHERE t.id = ? AND t.project_id = ?
                 LIMIT 1",
        )
        .bind(task_id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn decision_task_reference(
        &self,
        id: &str,
        project_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM task WHERE id = ? AND project_id = ? LIMIT 1",
        )
        .bind(id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

#[derive(Debug)]
pub struct PlacementEligibilityAssignmentRow {
    pub role_name: String,
    pub assignee_type: Option<String>,
    pub assignee_id: Option<String>,
    pub updated_at: String,
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for PlacementEligibilityAssignmentRow {
    fn from_row(row: &'r SqliteRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            role_name: row.try_get(0)?,
            assignee_type: row.try_get(1)?,
            assignee_id: row.try_get(2)?,
            updated_at: row.try_get(3)?,
        })
    }
}

impl SqliteDb {
    pub async fn placement_eligibility_assignments(
        &self,
        task_id: &str,
        parent_task_id: &str,
    ) -> crate::Result<Vec<PlacementEligibilityAssignmentRow>> {
        Ok(sqlx::query_as("SELECT role_name,assignee_type,assignee_id,updated_at FROM task_role_assignment WHERE task_id=? OR task_id=? ORDER BY task_id,role_name").bind(task_id).bind(parent_task_id).fetch_all(self.pool()).await?)
    }
}

pub struct ClearPlacementDispatchRefusalInput<'a> {
    pub annotation: String,
    pub updated_at: String,
    pub task_id: &'a str,
    pub version: i64,
    pub metadata_json: &'a Option<String>,
}
impl SqliteDb {
    pub async fn clear_placement_dispatch_refusal(
        &self,
        input: ClearPlacementDispatchRefusalInput<'_>,
    ) -> crate::Result<u64> {
        crate::task_writer::TaskQuery::new(self, input.task_id, "UPDATE task SET error_annotation=CASE WHEN error_annotation=? THEN NULL ELSE error_annotation END,
            metadata_json=CASE WHEN json_extract(metadata_json,'$.environment_wait.kind') IN ('environment_unverified','provision_failed') THEN json_remove(metadata_json,'$.placement_refusal','$.dispatch_disposition','$.environment_wait') ELSE json_remove(metadata_json,'$.placement_refusal','$.dispatch_disposition') END,version=version+1,updated_at=? WHERE id=? AND version=? AND metadata_json IS ?").bind(input.annotation).bind(input.updated_at).bind(input.task_id).bind(input.version).bind(input.metadata_json).execute(self.pool()).await
    }
}

pub struct RecordPlacementDispatchRefusalInput<'a> {
    pub annotation: String,
    pub environment_wait: bool,
    pub marker: String,
    pub wait_json: String,
    pub marker_copy: String,
    pub updated_at: String,
    pub task_id: &'a str,
    pub version: i64,
}
impl SqliteDb {
    pub async fn record_placement_dispatch_refusal(
        &self,
        input: RecordPlacementDispatchRefusalInput<'_>,
    ) -> crate::Result<u64> {
        crate::task_writer::TaskQuery::new(self, input.task_id, "UPDATE task SET error_annotation=CASE WHEN error_annotation IS NULL OR json_extract(error_annotation,'$.type')='dispatch_failed' THEN ? ELSE error_annotation END,
            metadata_json=CASE WHEN ? THEN json_set(COALESCE(metadata_json,'{}'),'$.placement_refusal',json(?),'$.environment_wait',json(?)) ELSE json_set(COALESCE(metadata_json,'{}'),'$.placement_refusal',json(?)) END,version=version+1,updated_at=? WHERE id=? AND version=?").bind(input.annotation).bind(input.environment_wait).bind(input.marker).bind(input.wait_json).bind(input.marker_copy).bind(input.updated_at).bind(input.task_id).bind(input.version).execute(self.pool()).await
    }
}

impl SqliteDb {
    pub async fn execution_plan_wait_task_version(
        &self,
        execution_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar("SELECT version FROM task WHERE json_extract(metadata_json, '$.plan_settlement_wait.execution_id')=?").bind(execution_id).fetch_optional(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn clear_execution_plan_task_wait(
        &self,
        task_id: &str,
        execution_id: &str,
        updated_at: String,
        version: Option<i64>,
    ) -> crate::Result<u64> {
        crate::task_writer::TaskQuery::new(self, task_id, "UPDATE task SET error_annotation=CASE WHEN json_extract(error_annotation, '$.type')='plan_settlement_wait' AND json_extract(error_annotation, '$.execution_id')=? THEN NULL ELSE error_annotation END,
        metadata_json=CASE WHEN json_extract(metadata_json, '$.owner_wait.plan_execution_id')=?
          THEN json_remove(metadata_json, '$.plan_settlement_wait', '$.deferred_dispatch', '$.owner_wait')
          ELSE json_remove(metadata_json, '$.plan_settlement_wait', '$.deferred_dispatch') END,
        version=version+1,updated_at=? WHERE json_extract(metadata_json, '$.plan_settlement_wait.execution_id')=? AND version=?").bind(execution_id).bind(execution_id).bind(updated_at).bind(execution_id).bind(version).execute(self.pool()).await
    }
}

impl SqliteDb {
    pub async fn record_execution_plan_owner_wait_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        task_id: &str,
        owner_wait_json: String,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self, task_id, "UPDATE task SET metadata_json=json_set(metadata_json, '$.owner_wait', json(?)) WHERE id=?").bind(owner_wait_json).bind(task_id).identity_fenced().execute_in_tx(tx).await
    }
}

pub struct RecordExecutionPlanTaskWaitInTxInput<'a> {
    pub task_id: &'a str,
    pub marker: String,
    pub disposition: String,
    pub annotation: String,
    pub updated_at: String,
    pub version: i64,
}

impl SqliteDb {
    pub async fn record_execution_plan_task_wait_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        input: RecordExecutionPlanTaskWaitInTxInput<'_>,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self, input.task_id, "UPDATE task SET metadata_json=json_set(COALESCE(metadata_json, '{}'), '$.plan_settlement_wait', json(?), '$.deferred_dispatch', json(?)),
        error_annotation=CASE WHEN error_annotation IS NULL OR json_extract(error_annotation, '$.type')='plan_settlement_wait' THEN ? ELSE error_annotation END,
        version=version+1, updated_at=? WHERE id=? AND version=?").bind(input.marker).bind(input.disposition).bind(input.annotation).bind(input.updated_at).bind(input.task_id).bind(input.version).identity_fenced().execute_in_tx(tx).await
    }
}

impl SqliteDb {
    pub async fn task_outcome_task_exists_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task WHERE id = ?)")
                .bind(task_id)
                .fetch_one(connection)
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn link_task_hook_execution_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        execution_id: &str,
        step_id: &str,
        hook_index: i64,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE task_hook_checkpoint SET execution_id=? WHERE step_id=? AND hook_index=? AND execution_id IS NULL").bind(execution_id).bind(step_id).bind(hook_index).execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn lead_budget_charge_recorded_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        budget: &str,
        key: &str,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM task_budget_charge c JOIN task_budget b USING(task_id,kind) WHERE c.task_id=? AND c.kind=? AND c.window_id=b.window_id AND c.step_id=?)",
            ).bind(task_id).bind(budget).bind(key).fetch_one(connection).await?)
    }
}

pub struct TaskRoleAssignmentSourceRow {
    pub id: crate::Result<String>,
    pub task_id: crate::Result<String>,
    pub role_name: crate::Result<String>,
    pub assignee_type: crate::Result<Option<String>>,
    pub assignee_id: crate::Result<Option<String>>,
    pub created_at: crate::Result<String>,
    pub updated_at: crate::Result<String>,
}

impl SqliteDb {
    pub async fn agent_role_sweep_assignments_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        agent_id: &str,
    ) -> crate::Result<Vec<TaskRoleAssignmentSourceRow>> {
        let rows = sqlx::query(
            "SELECT id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at FROM task_role_assignment WHERE assignee_type = 'agent' AND assignee_id = ? ORDER BY task_id, role_name",
        ).bind(agent_id).fetch_all(connection).await?;
        Ok(rows
            .into_iter()
            .map(|row| TaskRoleAssignmentSourceRow {
                id: row.try_get("id").map_err(crate::DbError::from),
                task_id: row.try_get("task_id").map_err(crate::DbError::from),
                role_name: row.try_get("role_name").map_err(crate::DbError::from),
                assignee_type: row.try_get("assignee_type").map_err(crate::DbError::from),
                assignee_id: row.try_get("assignee_id").map_err(crate::DbError::from),
                created_at: row.try_get("created_at").map_err(crate::DbError::from),
                updated_at: row.try_get("updated_at").map_err(crate::DbError::from),
            })
            .collect())
    }
}

impl SqliteDb {
    pub async fn previous_task_role_assignment_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        role_name: &str,
    ) -> crate::Result<Option<TaskRoleAssignmentSourceRow>> {
        let rows = sqlx::query(
            "SELECT id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at
             FROM task_role_assignment WHERE task_id = ? AND role_name = ?",
        )
        .bind(task_id)
        .bind(role_name)
        .fetch_optional(connection)
        .await?;
        Ok(rows.map(|row| TaskRoleAssignmentSourceRow {
            id: row.try_get("id").map_err(crate::DbError::from),
            task_id: row.try_get("task_id").map_err(crate::DbError::from),
            role_name: row.try_get("role_name").map_err(crate::DbError::from),
            assignee_type: row.try_get("assignee_type").map_err(crate::DbError::from),
            assignee_id: row.try_get("assignee_id").map_err(crate::DbError::from),
            created_at: row.try_get("created_at").map_err(crate::DbError::from),
            updated_at: row.try_get("updated_at").map_err(crate::DbError::from),
        }))
    }
}

impl SqliteDb {
    pub async fn agent_role_sweep_task_ids_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        agent_id: &str,
    ) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar("SELECT id FROM task WHERE deleted_at IS NULL AND (assignee_id=? OR id IN (SELECT task_id FROM task_role_assignment WHERE assignee_id=?))").bind(agent_id).bind(agent_id).fetch_all(connection).await?)
    }
}

impl SqliteDb {
    pub async fn refresh_task_role_assignment_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        updated_at: &str,
        task_id: &str,
        role_name: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "UPDATE task_role_assignment SET updated_at = ? WHERE task_id = ? AND role_name = ?",
        )
        .bind(updated_at)
        .bind(task_id)
        .bind(role_name)
        .execute(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn upsert_idle_task_role_assignment_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: &crate::CreateTaskRoleAssignment,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "INSERT INTO task_role_assignment
                (id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(task_id, role_name) DO UPDATE SET
                assignee_type = excluded.assignee_type,
                assignee_id = excluded.assignee_id,
                updated_at = excluded.updated_at",
        )
        .bind(&input.id)
        .bind(&input.task_id)
        .bind(&input.role_name)
        .bind(input.assignee_type.as_ref().map(ToString::to_string))
        .bind(input.assignee_id.as_deref())
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn invalidate_idle_task_assignment_review_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        updated_at: &str,
        task_id: &str,
        version: i64,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "UPDATE task
             SET review_passed_at = NULL, updated_at = ?, version = version + 1
             WHERE id = ? AND deleted_at IS NULL AND version = ?",
        )
        .bind(updated_at)
        .bind(task_id)
        .bind(version)
        .execute(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn invalidate_deleted_agent_task_authority_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        task_id: &str,
        updated_at: String,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self, task_id, "UPDATE task
                 SET review_passed_at = NULL,
                     version = version + CASE WHEN review_passed_at IS NOT NULL THEN 1 ELSE 0 END,
                     metadata_json = CASE
                         WHEN json_valid(COALESCE(metadata_json, '{}'))
                         THEN NULLIF(json_remove(COALESCE(metadata_json, '{}'), '$.dispatch_disposition', '$.deferred_dispatch'), '{}')
                         ELSE metadata_json
                     END,
                     updated_at = CASE
                         WHEN review_passed_at IS NOT NULL
                              OR (json_valid(COALESCE(metadata_json, '{}'))
                                  AND (json_type(metadata_json, '$.dispatch_disposition') IS NOT NULL
                                       OR json_type(metadata_json, '$.deferred_dispatch') IS NOT NULL))
                         THEN ? ELSE updated_at END
                 WHERE id = ? AND deleted_at IS NULL
                   AND (review_passed_at IS NOT NULL
                        OR (json_valid(COALESCE(metadata_json, '{}'))
                            AND (json_type(metadata_json, '$.dispatch_disposition') IS NOT NULL
                                 OR json_type(metadata_json, '$.deferred_dispatch') IS NOT NULL)))").bind(updated_at).bind(task_id).identity_fenced().execute_in_tx(tx).await
    }
}

impl SqliteDb {
    pub async fn clear_deleted_agent_task_assignee_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        task_id: &str,
        updated_at: String,
        agent_id: &str,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self, task_id, "UPDATE task SET assignee_id=NULL,updated_at=? WHERE id=? AND assignee_type='agent' AND assignee_id=?").bind(updated_at).bind(task_id).bind(agent_id).identity_fenced().execute_in_tx(tx).await
    }
}

impl SqliteDb {
    pub async fn clear_deleted_agent_role_assignments_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        task_id: &str,
        updated_at: String,
        agent_id: &str,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self, task_id, "UPDATE task_role_assignment SET assignee_id=NULL,updated_at=? WHERE task_id=? AND assignee_type='agent' AND assignee_id=?").bind(updated_at).bind(task_id).bind(agent_id).identity_fenced().execute_in_tx(tx).await
    }
}

impl SqliteDb {
    pub async fn clear_removed_lead_role_assignment(
        &self,
        task_id: &str,
        updated_at: String,
        role_name: &str,
        agent_id: &str,
    ) -> crate::Result<u64> {
        crate::task_writer::TaskQuery::new(self, task_id, "UPDATE task_role_assignment SET assignee_id=NULL,updated_at=? WHERE task_id=? AND role_name=? AND assignee_type='agent' AND assignee_id=?").bind(updated_at).bind(task_id).bind(role_name).bind(agent_id).identity_fenced().execute(self.pool()).await
    }
}

impl SqliteDb {
    pub async fn integration_task_waited_owner(
        &self,
        task_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar(
            "SELECT CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json,'$.owner_wait.daemon_id') END FROM task WHERE id=?",
        ).bind(task_id).fetch_optional(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn consume_applied_task_integration_pause_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        updated_at: String,
        task_id: &str,
        version: i64,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE task SET metadata_json=json_set(json_remove(COALESCE(metadata_json,'{}'),'$.paused_integration'),'$.paused_integration_generation',COALESCE(json_extract(metadata_json,'$.paused_integration_generation'),0)+1),updated_at=? WHERE id=? AND version=?").bind(updated_at).bind(task_id).bind(version).execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn clear_workspace_admission_environment_wait_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        task_id: &str,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self, task_id, "UPDATE task SET metadata_json = json_remove(CASE WHEN json_extract(metadata_json, '$.deferred_dispatch.kind') = 'environment_not_ready' THEN json_remove(metadata_json, '$.deferred_dispatch') ELSE metadata_json END, '$.environment_wait') WHERE id = ? AND json_valid(metadata_json) AND json_type(metadata_json, '$.environment_wait') IS NOT NULL").bind(task_id).identity_fenced().execute_in_tx(tx).await
    }
}

#[derive(Debug)]
pub struct TaskCreationParentRoleRow {
    pub role_name: String,
    pub assignee_type: Option<String>,
    pub assignee_id: Option<String>,
    pub backend_kind: Option<String>,
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for TaskCreationParentRoleRow {
    fn from_row(row: &'r SqliteRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            role_name: row.try_get(0)?,
            assignee_type: row.try_get(1)?,
            assignee_id: row.try_get(2)?,
            backend_kind: row.try_get(3)?,
        })
    }
}

impl SqliteDb {
    pub async fn task_creation_parent_roles_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        parent_task_id: &str,
    ) -> crate::Result<Vec<TaskCreationParentRoleRow>> {
        Ok(sqlx::query_as::<_, TaskCreationParentRoleRow>(
            "SELECT a.role_name, a.assignee_type, a.assignee_id,
                        (SELECT backend_kind FROM agent_current WHERE id = a.assignee_id)
                 FROM task_role_assignment a WHERE a.task_id = ?",
        )
        .bind(parent_task_id)
        .fetch_all(connection)
        .await?)
    }
}

pub struct InsertInitialExplicitTaskRoleInTxInput<'a> {
    pub id: String,
    pub task_id: &'a str,
    pub role_name: String,
    pub assignee_type: String,
    pub assignee_id: String,
    pub created_at: &'a str,
    pub updated_at: &'a str,
}

impl SqliteDb {
    pub async fn insert_initial_explicit_task_role_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: InsertInitialExplicitTaskRoleInTxInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("INSERT INTO task_role_assignment(id,task_id,role_name,assignee_type,assignee_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?)").bind(input.id).bind(input.task_id).bind(input.role_name).bind(input.assignee_type).bind(input.assignee_id).bind(input.created_at).bind(input.updated_at).execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn insert_initial_default_task_role_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        assignment: crate::CreateTaskRoleAssignment,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("INSERT INTO task_role_assignment(id,task_id,role_name,assignee_type,assignee_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?)").bind(assignment.id).bind(task_id).bind(assignment.role_name).bind(assignment.assignee_type.map(|kind|kind.to_string())).bind(assignment.assignee_id).bind(assignment.created_at).bind(assignment.updated_at).execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn insert_project_hook_task_role_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        assignment: &crate::CreateTaskRoleAssignment,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("INSERT INTO task_role_assignment (id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)").bind(&assignment.id).bind(task_id).bind(&assignment.role_name).bind(assignment.assignee_type.as_ref().map(|kind|kind.to_string())).bind(&assignment.assignee_id).bind(&assignment.created_at).bind(&assignment.updated_at).execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn advance_project_hook_task_role_version_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        updated_at: String,
        task_id: &str,
        version: i64,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE task SET version = version + 1, updated_at = ? WHERE id = ? AND deleted_at IS NULL AND version = ?").bind(updated_at).bind(task_id).bind(version).execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn tasks_naming_retired_auditor(&self) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar(
        "SELECT id FROM task WHERE deleted_at IS NULL AND status NOT IN ('done','cancelled') AND task_state_config IS NOT NULL AND json_valid(task_state_config) AND COALESCE(json_extract(task_state_config,'$.review.auditor_agent_id'), json_extract(task_state_config,'$.auditor_agent_id')) IS NOT NULL ORDER BY id",
    ).fetch_all(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn review_ci_infrastructure_budget_spent_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar(
                "SELECT COALESCE((SELECT spent FROM task_budget WHERE task_id=? AND kind='review_ci_infrastructure'),0)",
            ).bind(task_id).fetch_one(connection).await?)
    }
}

impl SqliteDb {
    pub async fn integration_step_live_task_version(
        &self,
        task_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(
            sqlx::query_scalar("SELECT version FROM task WHERE id=? AND deleted_at IS NULL")
                .bind(task_id)
                .fetch_optional(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn execution_setup_task_capability_class(
        &self,
        task_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT capability_class FROM project_task_governance
         WHERE task_id = ? AND project_id = ?",
        )
        .bind(task_id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn integration_admission_task_status_epoch(
        &self,
        task_id: &str,
    ) -> crate::Result<i64> {
        Ok(
            sqlx::query_scalar("SELECT status_epoch FROM task WHERE id=?")
                .bind(task_id)
                .fetch_one(self.pool())
                .await?,
        )
    }
}

pub struct ProjectWorkCompletionSourceRow {
    row: SqliteRow,
}
impl ProjectWorkCompletionSourceRow {
    pub fn status(&self) -> crate::Result<String> {
        Ok(self.row.try_get("status")?)
    }
    pub fn parent_task_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("parent_task_id")?)
    }
}

impl SqliteDb {
    pub async fn project_work_completion_sources(
        &self,
        project_id: &str,
    ) -> crate::Result<Vec<ProjectWorkCompletionSourceRow>> {
        let rows = sqlx::query(
            "SELECT id, status, parent_task_id \
             FROM task \
             WHERE project_id = ? \
               AND is_automation = 0 \
               AND archived_at IS NULL \
               AND deleted_at IS NULL",
        )
        .bind(project_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| ProjectWorkCompletionSourceRow { row })
            .collect())
    }
}

pub async fn project_work_completion_sources_in_tx(
    connection: &mut sqlx::SqliteConnection,
    project_id: &str,
) -> crate::Result<Vec<ProjectWorkCompletionSourceRow>> {
    let rows = sqlx::query("SELECT id, status, parent_task_id FROM task WHERE project_id = ? AND is_automation = 0 AND archived_at IS NULL AND deleted_at IS NULL").bind(project_id).fetch_all(connection).await?;
    Ok(rows
        .into_iter()
        .map(|row| ProjectWorkCompletionSourceRow { row })
        .collect())
}

pub struct ShutdownTaskSourceRow {
    row: SqliteRow,
}
impl ShutdownTaskSourceRow {
    pub fn id(&self) -> String {
        self.row.get("id")
    }
    pub fn project_id(&self) -> String {
        self.row.get("project_id")
    }
}

impl SqliteDb {
    pub async fn shutdown_in_progress_task_sources(
        &self,
    ) -> crate::Result<Vec<ShutdownTaskSourceRow>> {
        let rows = sqlx::query(
            "SELECT id, project_id FROM task WHERE status = 'in_progress' AND deleted_at IS NULL",
        )
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| ShutdownTaskSourceRow { row })
            .collect())
    }
}

pub async fn coordination_root_visible_children_in_connection(
    connection: &mut sqlx::SqliteConnection,
    task_id: &str,
) -> crate::Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT EXISTS(
             SELECT 1
             FROM task AS root
             JOIN task AS child ON child.parent_task_id = root.id
             WHERE root.id = ?
               AND root.parent_task_id IS NULL
               AND root.deleted_at IS NULL
               AND child.deleted_at IS NULL
         )",
    )
    .bind(task_id)
    .fetch_one(connection)
    .await?)
}

pub struct TaskRepositoryAuthoritySourceRow {
    row: SqliteRow,
}
impl TaskRepositoryAuthoritySourceRow {
    pub fn project_id(&self) -> String {
        self.row.get("project_id")
    }
    pub fn task_version(&self) -> i64 {
        self.row.get("task_version")
    }
    pub fn primary_repo_id(&self) -> Option<String> {
        self.row.get("primary_repo_id")
    }
    pub fn resolved_repo_id(&self) -> Option<String> {
        self.row.get("resolved_repo_id")
    }
    pub fn repo_project_id(&self) -> String {
        self.row.get("repo_project_id")
    }
    pub fn default_branch(&self) -> String {
        self.row.get("default_branch")
    }
}

impl SqliteDb {
    pub async fn task_repository_authority_source_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<Option<TaskRepositoryAuthoritySourceRow>> {
        Ok(sqlx::query(
            "SELECT t.project_id, t.version AS task_version,
                    p.primary_repo_id,
                    r.id AS resolved_repo_id,
                    r.project_id AS repo_project_id,
                    r.default_branch
             FROM task t
             JOIN project p ON p.id = t.project_id
             LEFT JOIN repo r ON r.id = p.primary_repo_id
             WHERE t.id = ? AND t.deleted_at IS NULL",
        )
        .bind(task_id)
        .fetch_optional(connection)
        .await?
        .map(|row| TaskRepositoryAuthoritySourceRow { row }))
    }
}

pub struct LatestTaskStateEntrySourceRow {
    row: SqliteRow,
}
impl LatestTaskStateEntrySourceRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn created_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("created_at")?)
    }
}

impl SqliteDb {
    pub async fn latest_task_state_entry_source(
        &self,
        task_id: &str,
        to_state: &str,
    ) -> crate::Result<Option<LatestTaskStateEntrySourceRow>> {
        let rows = sqlx::query(
        "SELECT id, created_at FROM transition_log WHERE task_id = ? AND to_state = ? ORDER BY created_at DESC, rowid DESC LIMIT 1",
    ).bind(task_id).bind(to_state).fetch_optional(self.pool()).await?;
        Ok(rows.map(|row| LatestTaskStateEntrySourceRow { row }))
    }
}

impl SqliteDb {
    pub async fn consume_paused_integration_after_merge_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        updated_at: String,
        task_id: &str,
        version: i64,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE task SET metadata_json=json_set(json_remove(COALESCE(metadata_json,'{}'),'$.paused_integration'),'$.paused_integration_generation',COALESCE(json_extract(metadata_json,'$.paused_integration_generation'),0)+1),updated_at=? WHERE id=? AND version=?")
            .bind(updated_at)
            .bind(task_id)
            .bind(version)
            .execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn task_step_settlement_epoch_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<i64> {
        Ok(
            sqlx::query_scalar("SELECT status_epoch FROM task WHERE id=?")
                .bind(task_id)
                .fetch_one(connection)
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn annotate_task_step_failure_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        annotation_json: String,
        blocked_json: &Option<String>,
        updated_at: &str,
        task_id: &str,
        version: i64,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE task SET error_annotation=?,blocked_json=COALESCE(?,blocked_json),version=version+1,updated_at=? WHERE id=? AND version=? AND deleted_at IS NULL")
            .bind(annotation_json)
            .bind(blocked_json)
            .bind(updated_at)
            .bind(task_id)
            .bind(version)
            .execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn conflict_handoff_source(
        &self,
        transition_log_id: &str,
        task_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<ConflictHandoffSourceRow>> {
        Ok(sqlx::query("SELECT tl.id, tl.from_state, tl.to_state, tl.triggered_by, tl.bridge_kind, tl.bridge_payload, tl.created_at
            FROM transition_log tl JOIN task t ON t.id = tl.task_id
            WHERE tl.id = ? AND t.id = ? AND t.project_id = ?")
            .bind(transition_log_id)
            .bind(task_id)
            .bind(project_id)
            .fetch_optional(self.pool()).await?.map(|row| ConflictHandoffSourceRow { row }))
    }
}

impl SqliteDb {
    pub async fn project_conflict_handoffs_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
        triggered_by: String,
        after: String,
        through: String,
    ) -> crate::Result<Vec<ConflictHandoffHistoryRow>> {
        Ok(sqlx::query(CONFLICT_HANDOFF_QUERY)
            .bind(project_id)
            .bind(triggered_by)
            .bind(after)
            .bind(through)
            .fetch_all(connection)
            .await?
            .into_iter()
            .map(|row| ConflictHandoffHistoryRow { row })
            .collect())
    }
}

impl SqliteDb {
    pub async fn daemon_upgrade_refused_task_ids(
        &self,
        refusal_code: &str,
    ) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar::<_, String>("SELECT id FROM task WHERE deleted_at IS NULL AND CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.daemon_upgrade_refusal.code') END = ?")
            .bind(refusal_code)
            .fetch_all(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn task_current_entry_first_transition_bridge(
        &self,
        task_id: &str,
        to_state: &str,
    ) -> crate::Result<Option<(String, Option<String>, Option<String>)>> {
        Ok(sqlx::query_as("SELECT id, bridge_kind, bridge_payload FROM transition_log WHERE task_id = ? AND to_state = ? AND status_epoch = (SELECT status_epoch FROM task WHERE id = ?) ORDER BY created_at, rowid LIMIT 1")
            .bind(task_id)
            .bind(to_state)
            .bind(task_id)
            .fetch_optional(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn workflow_transition_task_version_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar("SELECT version FROM task WHERE id=?")
            .bind(task_id)
            .fetch_one(connection)
            .await?)
    }
}

pub struct WriteWorkflowTransitionStateInput<'a> {
    pub target_state: &'a str,
    pub updated_at: &'a str,
    pub entry_barrier_json: Option<&'a str>,
    pub clear_review_passed_at: bool,
    pub task_id: &'a str,
    pub has_step: bool,
    pub version: i64,
    pub expected_status: Option<&'a str>,
    pub expected_epoch: Option<i64>,
}

impl SqliteDb {
    pub async fn write_workflow_transition_state_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: WriteWorkflowTransitionStateInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE task\n                 SET status = ?, version = version + 1, updated_at = ?, blocked_json = NULL, entry_barrier_json = ?,\n                     review_passed_at = CASE WHEN ? THEN NULL ELSE review_passed_at END,\n                     status_epoch = status_epoch + (status = ?)\n                 WHERE id = ? AND deleted_at IS NULL AND ((? = 0 AND version = ?) OR (? = 1 AND status = ? AND status_epoch = ?))")
            .bind(input.target_state)
            .bind(input.updated_at)
            .bind(input.entry_barrier_json)
            .bind(input.clear_review_passed_at)
            .bind(input.target_state)
            .bind(input.task_id)
            .bind(input.has_step)
            .bind(input.version)
            .bind(input.has_step)
            .bind(input.expected_status)
            .bind(input.expected_epoch)
            .execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn set_workflow_deferred_dispatch_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        marker_json: &str,
        task_id: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE task SET metadata_json=json_set(COALESCE(metadata_json,'{}'),'$.deferred_dispatch',json(?)) WHERE id=?")
            .bind(marker_json)
            .bind(task_id)
            .execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn clear_workflow_deferred_dispatch_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE task SET metadata_json=CASE WHEN json_valid(metadata_json) THEN json_remove(metadata_json,'$.deferred_dispatch') ELSE metadata_json END WHERE id=?")
            .bind(task_id)
            .execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn carry_paused_integration_state_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        target_state: &str,
        task_id: &str,
        current_status: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE task SET metadata_json=json_set(metadata_json,'$.paused_integration.state',?) WHERE id=? AND CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json,'$.paused_integration.state') END=?")
            .bind(target_state)
            .bind(task_id)
            .bind(current_status)
            .execute(connection).await?)
    }
}

pub struct InsertWorkflowTransitionLogInput<'a> {
    pub id: &'a str,
    pub task_id: &'a str,
    pub from_state: &'a str,
    pub to_state: &'a str,
    pub trigger_name: Option<&'a str>,
    pub triggered_by: String,
    pub trigger_reason: &'a str,
    pub hook_results_json: String,
    pub rejection: i64,
    pub created_at: &'a str,
    pub bridge_kind: Option<&'a str>,
    pub bridge_payload: Option<String>,
}

impl SqliteDb {
    pub async fn insert_workflow_transition_log_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: InsertWorkflowTransitionLogInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("INSERT INTO transition_log (
                            id, task_id, from_state, to_state, trigger_name, triggered_by,
                            trigger_reason, hook_results_json, rejection, created_at, bridge_kind, bridge_payload, status_epoch
                         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, (SELECT status_epoch FROM task WHERE id = ?))")
            .bind(input.id)
            .bind(input.task_id)
            .bind(input.from_state)
            .bind(input.to_state)
            .bind(input.trigger_name)
            .bind(input.triggered_by)
            .bind(input.trigger_reason)
            .bind(input.hook_results_json)
            .bind(input.rejection)
            .bind(input.created_at)
            .bind(input.bridge_kind)
            .bind(input.bridge_payload)
            .bind(input.task_id)
            .execute(connection).await?)
    }
}
