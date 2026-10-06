use super::*;
use crate::{
    new_uuid_v4, now_rfc3339, AssigneeKind, CreateTaskRoleAssignment, CreateTransitionLog,
    TaskRoleAssignment, TaskRoleAssignmentRepo, TransitionLog, TransitionLogRepo,
};
use std::str::FromStr;

fn map_task_role_assignment_row(
    row: SqliteRow,
) -> std::result::Result<TaskRoleAssignment, DbError> {
    let assignee_type = row
        .get::<Option<String>, _>(3)
        .map(|value| AssigneeKind::from_str(&value).map_err(|_| DbError::InvalidTransition))
        .transpose()?;
    Ok(TaskRoleAssignment {
        id: row.get(0),
        task_id: row.get(1),
        role_name: row.get(2),
        assignee_type,
        assignee_id: row.get(4),
        created_at: row.get(5),
        updated_at: row.get(6),
    })
}

fn assignment_snapshot_matches(
    current: &TaskRoleAssignment,
    expected: &TaskRoleAssignment,
) -> bool {
    current.id == expected.id
        && current.task_id == expected.task_id
        && current.role_name == expected.role_name
        && current.assignee_type == expected.assignee_type
        && current.assignee_id == expected.assignee_id
        && current.created_at == expected.created_at
        && current.updated_at == expected.updated_at
}

fn map_transition_log_row(row: SqliteRow) -> Result<TransitionLog> {
    let id: String = row.get(0);
    let bridge = crate::decode_transition_bridge(
        &id,
        row.get::<Option<String>, _>("bridge_kind").as_deref(),
        row.get::<Option<String>, _>("bridge_payload").as_deref(),
    )?;
    Ok(TransitionLog {
        id,
        task_id: row.get(1),
        from_state: row.get(2),
        to_state: row.get(3),
        trigger_name: row.get(4),
        triggered_by: row.get(5),
        bridge,
        trigger_reason: row.get(6),
        hook_results_json: row.get(7),
        rejection: row.get::<i64, _>(8) != 0,
        created_at: row.get(9),
    })
}

fn map_workflow_sqlx_error(error: sqlx::Error) -> DbError {
    match error {
        sqlx::Error::RowNotFound => DbError::NotFound,
        other => DbError::Sqlx(other),
    }
}

#[async_trait]
impl TaskRoleAssignmentRepo for SqliteDb {
    async fn assign(
        &self,
        input: CreateTaskRoleAssignment,
    ) -> std::result::Result<TaskRoleAssignment, DbError> {
        if !crate::task_writer::owns_task(&input.task_id) {
            return self
                .run_task_mutation(
                    &input.task_id,
                    crate::TaskMutation::TaskRoleAssignmentAssign {
                        input: input.clone(),
                    },
                )
                .await;
        }

        let mut tx = crate::begin_immediate(self.pool()).await?;
        self.fence_current_step_in_tx(&mut tx).await?;
        sqlx::query(
            "INSERT INTO task_role_assignment (id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(task_id, role_name) DO UPDATE SET assignee_type = excluded.assignee_type, assignee_id = excluded.assignee_id, updated_at = excluded.updated_at",
        )
        .bind(&input.id)
        .bind(&input.task_id)
        .bind(&input.role_name)
        .bind(input.assignee_type.as_ref().map(ToString::to_string))
        .bind(input.assignee_id.as_deref())
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(map_workflow_sqlx_error)?;

        let row = sqlx::query(
            "SELECT id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at FROM task_role_assignment WHERE task_id = ? AND role_name = ?",
        )
        .bind(&input.task_id)
        .bind(&input.role_name)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_workflow_sqlx_error)?;

        let assignment = map_task_role_assignment_row(row)?;
        self.record_mutation_reply_in_tx(&mut tx, &assignment)
            .await?;
        tx.commit().await?;
        Ok(assignment)
    }

    async fn assign_if_unchanged(
        &self,
        input: CreateTaskRoleAssignment,
        expected_previous: Option<&TaskRoleAssignment>,
    ) -> std::result::Result<TaskRoleAssignment, DbError> {
        if !crate::task_writer::owns_task(&input.task_id) {
            return self
                .run_task_mutation(
                    &input.task_id,
                    crate::TaskMutation::TaskRoleAssignmentAssignIfUnchanged {
                        input: input.clone(),
                        expected_previous: expected_previous.cloned(),
                    },
                )
                .await;
        }

        let mut transaction = crate::begin_immediate(&self.pool).await?;
        self.fence_current_step_in_tx(&mut transaction).await?;
        let current_row = sqlx::query(
            "SELECT id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at
             FROM task_role_assignment WHERE task_id = ? AND role_name = ?",
        )
        .bind(&input.task_id)
        .bind(&input.role_name)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_workflow_sqlx_error)?;
        let current = current_row.map(map_task_role_assignment_row).transpose()?;
        let expected_matches = match expected_previous {
            Some(expected) => current
                .as_ref()
                .is_some_and(|current| assignment_snapshot_matches(current, expected)),
            None => current.is_none(),
        };
        if !expected_matches {
            return Err(DbError::VersionConflict);
        }

        if let Some(expected_previous) = expected_previous {
            let result = sqlx::query(
                "UPDATE task_role_assignment
                 SET assignee_type = ?, assignee_id = ?, updated_at = ?
                 WHERE task_id = ? AND role_name = ? AND id = ? AND updated_at = ?",
            )
            .bind(input.assignee_type.as_ref().map(ToString::to_string))
            .bind(input.assignee_id.as_deref())
            .bind(&input.updated_at)
            .bind(&input.task_id)
            .bind(&input.role_name)
            .bind(&expected_previous.id)
            .bind(&expected_previous.updated_at)
            .execute(&mut *transaction)
            .await
            .map_err(map_workflow_sqlx_error)?;
            if result.rows_affected() == 0 {
                return Err(DbError::VersionConflict);
            }
        } else {
            sqlx::query(
                "INSERT INTO task_role_assignment
                    (id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&input.id)
            .bind(&input.task_id)
            .bind(&input.role_name)
            .bind(input.assignee_type.as_ref().map(ToString::to_string))
            .bind(input.assignee_id.as_deref())
            .bind(&input.created_at)
            .bind(&input.updated_at)
            .execute(&mut *transaction)
            .await
            .map_err(map_workflow_sqlx_error)?;
        }

        let row = sqlx::query(
            "SELECT id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at
             FROM task_role_assignment WHERE task_id = ? AND role_name = ?",
        )
        .bind(&input.task_id)
        .bind(&input.role_name)
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_workflow_sqlx_error)?;
        let assignment = map_task_role_assignment_row(row)?;
        transaction.commit().await?;
        Ok(assignment)
    }

    async fn get_by_task_and_role(
        &self,
        task_id: &str,
        role_name: &str,
    ) -> std::result::Result<Option<TaskRoleAssignment>, DbError> {
        match sqlx::query(
            "SELECT id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at FROM task_role_assignment WHERE task_id = ? AND role_name = ?",
        )
        .bind(task_id)
        .bind(role_name)
        .fetch_one(&self.pool)
        .await
        {
            Ok(row) => map_task_role_assignment_row(row).map(Some),
            Err(sqlx::Error::RowNotFound) => Ok(None),
            Err(error) => Err(map_workflow_sqlx_error(error)),
        }
    }

    async fn list_by_task(
        &self,
        task_id: &str,
    ) -> std::result::Result<Vec<TaskRoleAssignment>, DbError> {
        let rows = sqlx::query(
            "SELECT id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at FROM task_role_assignment WHERE task_id = ? ORDER BY role_name",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await
        .map_err(map_workflow_sqlx_error)?;

        rows.into_iter().map(map_task_role_assignment_row).collect()
    }

    async fn list_by_tasks(&self, task_ids: &[&str]) -> Result<Vec<TaskRoleAssignment>> {
        let mut connection = self.pool.acquire().await?;
        roles_for_tasks(&mut connection, task_ids).await
    }

    async fn remove(&self, task_id: &str, role_name: &str) -> std::result::Result<(), DbError> {
        if !crate::task_writer::owns_task(task_id) {
            return self
                .run_task_mutation(
                    task_id,
                    crate::TaskMutation::TaskRoleAssignmentRemove {
                        task_id: task_id.to_owned(),
                        role_name: role_name.to_owned(),
                    },
                )
                .await;
        }

        let mut tx = crate::begin_immediate(self.pool()).await?;
        self.fence_current_step_in_tx(&mut tx).await?;
        sqlx::query("DELETE FROM task_role_assignment WHERE task_id = ? AND role_name = ?")
            .bind(task_id)
            .bind(role_name)
            .execute(&mut *tx)
            .await
            .map_err(map_workflow_sqlx_error)?;
        self.record_mutation_reply_in_tx(&mut tx, &()).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn assign_and_clear_review_authority(
        &self,
        input: CreateTaskRoleAssignment,
        expected_previous: Option<&TaskRoleAssignment>,
        expected_task_version: i64,
        updated_at: &str,
    ) -> std::result::Result<(TaskRoleAssignment, Task), DbError> {
        if !crate::task_writer::owns_task(&input.task_id) {
            return self
                .run_task_mutation(
                    &input.task_id,
                    crate::TaskMutation::TaskRoleAssignmentAssignAndClearReviewAuthority {
                        input: input.clone(),
                        expected_previous: expected_previous.cloned(),
                        expected_task_version,
                        updated_at: updated_at.to_owned(),
                    },
                )
                .await;
        }

        let mut transaction = crate::begin_immediate(&self.pool).await?;
        self.fence_current_step_in_tx(&mut transaction).await?;
        let current_row = sqlx::query(
            "SELECT id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at
             FROM task_role_assignment WHERE task_id = ? AND role_name = ?",
        )
        .bind(&input.task_id)
        .bind(&input.role_name)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_workflow_sqlx_error)?;
        let current = current_row.map(map_task_role_assignment_row).transpose()?;
        let expected_matches = match expected_previous {
            Some(expected) => current
                .as_ref()
                .is_some_and(|current| assignment_snapshot_matches(current, expected)),
            None => current.is_none(),
        };
        if !expected_matches {
            return Err(DbError::VersionConflict);
        }
        sqlx::query(
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
        .execute(&mut *transaction)
        .await
        .map_err(map_workflow_sqlx_error)?;

        // A passed review is authority over the implementation it reviewed,
        // so only a change to who owns that implementation invalidates it.
        // Swapping the reviewer leaves the reviewed code exactly as it was,
        // and clearing the marker there silently sends a Task back through a
        // review it had already earned. The Task version advances either
        // way: the assignment snapshot admission compares against changed.
        let invalidates_review = matches!(input.role_name.as_str(), "coder" | "executor");
        let result = sqlx::query(
            "UPDATE task
             SET review_passed_at = CASE WHEN ? THEN NULL ELSE review_passed_at END,
                 updated_at = ?,
                 version = version + 1
             WHERE id = ? AND deleted_at IS NULL AND version = ?",
        )
        .bind(invalidates_review)
        .bind(updated_at)
        .bind(&input.task_id)
        .bind(expected_task_version)
        .execute(&mut *transaction)
        .await
        .map_err(map_workflow_sqlx_error)?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }

        let assignment = sqlx::query(
            "SELECT id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at
             FROM task_role_assignment WHERE task_id = ? AND role_name = ?",
        )
        .bind(&input.task_id)
        .bind(&input.role_name)
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_workflow_sqlx_error)
        .and_then(map_task_role_assignment_row)?;
        let task = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
            .bind(&input.task_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(map_workflow_sqlx_error)
            .and_then(map_task)?;
        transaction.commit().await?;
        Ok((assignment, task))
    }

    async fn remove_and_clear_review_authority(
        &self,
        expected_assignment: &TaskRoleAssignment,
        expected_task_version: i64,
        updated_at: &str,
    ) -> std::result::Result<Task, DbError> {
        if !crate::task_writer::owns_task(&expected_assignment.task_id) {
            return self
                .run_task_mutation(
                    &expected_assignment.task_id,
                    crate::TaskMutation::TaskRoleAssignmentRemoveAndClearReviewAuthority {
                        expected_assignment: expected_assignment.clone(),
                        expected_task_version,
                        updated_at: updated_at.to_owned(),
                    },
                )
                .await;
        }

        let mut transaction = crate::begin_immediate(&self.pool).await?;
        self.fence_current_step_in_tx(&mut transaction).await?;
        let current_row = sqlx::query(
            "SELECT id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at
             FROM task_role_assignment WHERE task_id = ? AND role_name = ?",
        )
        .bind(&expected_assignment.task_id)
        .bind(&expected_assignment.role_name)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_workflow_sqlx_error)?;
        let current = current_row.map(map_task_role_assignment_row).transpose()?;
        let Some(current) = current else {
            return Err(DbError::VersionConflict);
        };
        if !assignment_snapshot_matches(&current, expected_assignment) {
            return Err(DbError::VersionConflict);
        }
        let result = sqlx::query(
            "DELETE FROM task_role_assignment
             WHERE task_id = ? AND role_name = ? AND id = ? AND updated_at = ?",
        )
        .bind(&expected_assignment.task_id)
        .bind(&expected_assignment.role_name)
        .bind(&expected_assignment.id)
        .bind(&expected_assignment.updated_at)
        .execute(&mut *transaction)
        .await
        .map_err(map_workflow_sqlx_error)?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }

        // Removing the coder assignment removes the authority behind the
        // implementation a passed review approved, so that marker goes with
        // it. Removing any other role leaves the reviewed code untouched and
        // must not force it through review again. The Task version advances
        // either way, because the assignment snapshot admission compares
        // against has changed.
        let invalidates_review =
            matches!(expected_assignment.role_name.as_str(), "coder" | "executor");
        let result = sqlx::query(
            "UPDATE task
             SET review_passed_at = CASE WHEN ? THEN NULL ELSE review_passed_at END,
                 updated_at = ?,
                 version = version + 1
             WHERE id = ? AND deleted_at IS NULL AND version = ?",
        )
        .bind(invalidates_review)
        .bind(updated_at)
        .bind(&expected_assignment.task_id)
        .bind(expected_task_version)
        .execute(&mut *transaction)
        .await
        .map_err(map_workflow_sqlx_error)?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }

        let task = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
            .bind(&expected_assignment.task_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(map_workflow_sqlx_error)
            .and_then(map_task)?;
        transaction.commit().await?;
        Ok(task)
    }
}

#[async_trait]
impl TransitionLogRepo for SqliteDb {
    async fn insert(
        &self,
        input: CreateTransitionLog,
    ) -> std::result::Result<TransitionLog, DbError> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let task = self
            .get_task_in_tx(&mut transaction, &input.task_id)
            .await?
            .ok_or(DbError::NotFound)?;
        // A logged-only transition (a recovery marker) carries no typed actor
        // and so no owner authority. Production markers never consume a budget;
        // a RetryWindowReset marker resets every kind whoever records it.
        crate::budget::transition(
            &mut transaction,
            &task.id,
            &input.from_state,
            false,
            &input.bridge,
            input.rejection,
            &input.id,
            None,
        )
        .await?;
        sqlx::query(
            "INSERT INTO transition_log (id, task_id, from_state, to_state, trigger_name, triggered_by, trigger_reason, hook_results_json, rejection, created_at, bridge_kind, bridge_payload, status_epoch) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, (SELECT status_epoch FROM task WHERE id=?))",
        )
        .bind(&input.id)
        .bind(&input.task_id)
        .bind(&input.from_state)
        .bind(&input.to_state)
        .bind(input.trigger_name.as_deref())
        .bind(&input.triggered_by)
        .bind(&input.trigger_reason)
        .bind(input.hook_results_json.as_deref())
        .bind(if input.rejection { 1_i64 } else { 0_i64 })
        .bind(&input.created_at)
        .bind(input.bridge.bridge_kind.map(api_types::TransitionBridgeKind::as_str))
        .bind(input.bridge.bridge_payload.as_ref().map(ToString::to_string))
        .bind(&input.task_id)
        .execute(&mut *transaction)
        .await
        .map_err(map_workflow_sqlx_error)?;

        let row = sqlx::query(
            "SELECT id, task_id, from_state, to_state, trigger_name, triggered_by, trigger_reason, hook_results_json, rejection, created_at, bridge_kind, bridge_payload FROM transition_log WHERE id = ?",
        )
        .bind(&input.id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_workflow_sqlx_error)?;

        let result = map_transition_log_row(row)?;
        crate::task_condition::sync_condition(&mut transaction, &input.task_id).await?;
        transaction.commit().await?;
        Ok(result)
    }

    async fn insert_recovery_marker(
        &self,
        task_id: &str,
        current_state: &str,
        action_kind: &str,
        triggered_by: &str,
        reason: &str,
    ) -> std::result::Result<TransitionLog, DbError> {
        self.insert(CreateTransitionLog {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            from_state: current_state.to_owned(),
            to_state: current_state.to_owned(),
            trigger_name: Some(action_kind.to_owned()),
            triggered_by: triggered_by.to_owned(),
            bridge: api_types::TransitionBridge::recovery(
                action_kind,
                matches!(
                    action_kind,
                    "restart" | "reset_to_initial" | "reset_retry_window"
                ),
            ),
            trigger_reason: reason.to_owned(),
            hook_results_json: None,
            rejection: false,
            created_at: now_rfc3339(),
        })
        .await
    }

    async fn list_by_task(
        &self,
        task_id: &str,
    ) -> std::result::Result<Vec<TransitionLog>, DbError> {
        let rows = sqlx::query(
            "SELECT id, task_id, from_state, to_state, trigger_name, triggered_by, trigger_reason, hook_results_json, rejection, created_at, bridge_kind, bridge_payload FROM transition_log WHERE task_id = ? ORDER BY created_at, rowid",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await
        .map_err(map_workflow_sqlx_error)?;

        rows.into_iter().map(map_transition_log_row).collect()
    }

    async fn list_by_tasks(&self, task_ids: &[&str]) -> Result<Vec<TransitionLog>> {
        let mut connection = self.pool.acquire().await?;
        transitions_for_tasks(&mut connection, task_ids).await
    }

    async fn count_to_state_since(
        &self,
        task_id: &str,
        to_state: &str,
        since: Option<&str>,
    ) -> std::result::Result<i64, DbError> {
        let mut query =
            sqlx::QueryBuilder::new("SELECT COUNT(*) FROM transition_log WHERE task_id = ");
        query
            .push_bind(task_id)
            .push(" AND to_state = ")
            .push_bind(to_state);
        if let Some(since) = since {
            query.push(" AND created_at >= ").push_bind(since);
        }
        query
            .build_query_scalar::<i64>()
            .fetch_one(&self.pool)
            .await
            .map_err(map_workflow_sqlx_error)
    }

    async fn update_hook_results(
        &self,
        id: &str,
        hook_results_json: &str,
    ) -> std::result::Result<(), DbError> {
        let result = sqlx::query("UPDATE transition_log SET hook_results_json = ? WHERE id = ?")
            .bind(hook_results_json)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(map_workflow_sqlx_error)?;

        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }

        Ok(())
    }
}

pub(super) async fn roles_for_tasks(
    connection: &mut sqlx::SqliteConnection,
    task_ids: &[&str],
) -> Result<Vec<TaskRoleAssignment>> {
    if task_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = sqlx::QueryBuilder::<Sqlite>::new(
        "SELECT id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at
             FROM task_role_assignment WHERE task_id IN (",
    );
    let mut ids = query.separated(", ");
    for task_id in task_ids {
        ids.push_bind(*task_id);
    }
    ids.push_unseparated(") ORDER BY task_id, role_name");
    let rows = query.build().fetch_all(&mut *connection).await?;
    rows.into_iter().map(map_task_role_assignment_row).collect()
}

pub(super) async fn transitions_for_tasks(
    connection: &mut sqlx::SqliteConnection,
    task_ids: &[&str],
) -> Result<Vec<TransitionLog>> {
    if task_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = sqlx::QueryBuilder::<Sqlite>::new(
        "SELECT id, task_id, from_state, to_state, trigger_name, triggered_by,
                    trigger_reason, hook_results_json, rejection, created_at, bridge_kind, bridge_payload
             FROM transition_log WHERE task_id IN (",
    );
    let mut ids = query.separated(", ");
    for task_id in task_ids {
        ids.push_bind(*task_id);
    }
    ids.push_unseparated(") ORDER BY task_id, created_at, rowid");
    let rows = query.build().fetch_all(&mut *connection).await?;
    rows.into_iter().map(map_transition_log_row).collect()
}
