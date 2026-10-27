use super::*;
use crate::ProjectSlotCounts;
use crate::{
    AssigneeKind, CreateTransitionLog, LatestExecutionMetadataClaim, RestoreQueuedRecovery,
    TaskRoleAssignment,
};
use std::collections::HashSet;

mod action_scope;
mod attention;
mod dispatch;
mod operator;
mod repository;
mod roles;
mod workflow;

pub use action_scope::*;
pub use attention::*;
pub use dispatch::*;
pub use operator::*;
pub use roles::*;
pub use workflow::*;

async fn restore_recorded_retry_policy_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    task: &Task,
    metadata: &TaskMetadata,
    authority: &LatestExecutionAuthority,
    updated_at: &str,
) -> Result<bool> {
    if metadata
        .extra
        .get("last_execution_failure_execution_id")
        .and_then(serde_json::Value::as_str)
        != Some(authority.execution_id.as_str())
        || metadata
            .extra
            .get("deferred_dispatch")
            .and_then(|retry| retry.get("target_state"))
            .and_then(serde_json::Value::as_str)
            != Some(task.status.as_str())
    {
        return Ok(false);
    }
    Ok(sqlx::query("UPDATE execution SET resume_policy='auto', updated_at=? WHERE id=? AND task_id=? AND status='failed' AND (resume_policy IS NULL OR resume_policy='manual')")
        .bind(updated_at).bind(&authority.execution_id).bind(&task.id)
        .execute(&mut **tx).await?.rows_affected() > 0)
}

impl SqliteDb {
    /// Heal a pre-atomic retry admission left by a crash. The exact retry,
    /// current execution and current assignment/Project authority must agree.
    pub async fn restore_recorded_execution_retry(
        &self,
        task_id: &str,
        authority: LatestExecutionAuthority,
    ) -> Result<bool> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        self.fence_current_step_in_tx(&mut tx).await?;
        let task = self
            .get_task_in_tx(&mut tx, task_id)
            .await?
            .ok_or(DbError::NotFound)?;
        if !latest_execution_authority_matches_in_tx(&mut tx, &task, &authority).await? {
            return Ok(false);
        }
        let metadata = TaskMetadata::parse(task.metadata_json.as_deref())
            .map_err(|error| DbError::Check(format!("invalid task metadata: {error}")))?;
        let changed = restore_recorded_retry_policy_in_tx(
            &mut tx,
            &task,
            &metadata,
            &authority,
            &crate::now_rfc3339(),
        )
        .await?;
        tx.commit().await?;
        Ok(changed)
    }
}

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
        crate::task_condition::refresh_task_condition(&mut transaction, &mut task).await?;
        for parent in [&previous_parent, &task.parent_task_id]
            .into_iter()
            .flatten()
        {
            crate::task_condition::produce_children(&mut transaction, parent).await?;
        }
    } else {
        crate::task_condition::produce(&mut transaction, &task.id, crate::ConditionChange::Legacy)
            .await?;
        crate::task_condition::refresh_task_condition(&mut transaction, &mut task).await?;
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
        crate::task_condition::refresh_task_condition(&mut transaction, &mut task).await?;
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
    crate::task_condition::refresh_task_condition(&mut transaction, &mut task).await?;
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
    crate::task_condition::refresh_task_condition(&mut transaction, &mut task).await?;
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
    crate::task_condition::refresh_task_condition(&mut transaction, &mut task).await?;
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
        crate::task_condition::refresh_task_condition(&mut transaction, &mut task).await?;
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
