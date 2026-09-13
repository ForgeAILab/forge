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

fn map_transition_log_row(row: SqliteRow) -> TransitionLog {
    TransitionLog {
        id: row.get(0),
        task_id: row.get(1),
        from_state: row.get(2),
        to_state: row.get(3),
        trigger_name: row.get(4),
        triggered_by: row.get(5),
        trigger_reason: row.get(6),
        hook_results_json: row.get(7),
        rejection: row.get::<i64, _>(8) != 0,
        created_at: row.get(9),
    }
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
        .execute(&self.pool)
        .await
        .map_err(map_workflow_sqlx_error)?;

        let row = sqlx::query(
            "SELECT id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at FROM task_role_assignment WHERE task_id = ? AND role_name = ?",
        )
        .bind(&input.task_id)
        .bind(&input.role_name)
        .fetch_one(&self.pool)
        .await
        .map_err(map_workflow_sqlx_error)?;

        map_task_role_assignment_row(row)
    }

    async fn assign_if_unchanged(
        &self,
        input: CreateTaskRoleAssignment,
        expected_previous: Option<&TaskRoleAssignment>,
    ) -> std::result::Result<TaskRoleAssignment, DbError> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
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

    async fn remove(&self, task_id: &str, role_name: &str) -> std::result::Result<(), DbError> {
        sqlx::query("DELETE FROM task_role_assignment WHERE task_id = ? AND role_name = ?")
            .bind(task_id)
            .bind(role_name)
            .execute(&self.pool)
            .await
            .map_err(map_workflow_sqlx_error)?;
        Ok(())
    }

    async fn assign_and_clear_review_authority(
        &self,
        input: CreateTaskRoleAssignment,
        expected_previous: Option<&TaskRoleAssignment>,
        expected_task_version: i64,
        updated_at: &str,
    ) -> std::result::Result<(TaskRoleAssignment, Task), DbError> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
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

        let result = sqlx::query(
            "UPDATE task
             SET review_passed_at = NULL, updated_at = ?, version = version + 1
             WHERE id = ? AND deleted_at IS NULL AND version = ?",
        )
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
        let mut transaction = crate::begin_immediate(&self.pool).await?;
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

        let result = sqlx::query(
            "UPDATE task
             SET review_passed_at = NULL, updated_at = ?, version = version + 1
             WHERE id = ? AND deleted_at IS NULL AND version = ?",
        )
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
        sqlx::query(
            "INSERT INTO transition_log (id, task_id, from_state, to_state, trigger_name, triggered_by, trigger_reason, hook_results_json, rejection, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
        .execute(&self.pool)
        .await
        .map_err(map_workflow_sqlx_error)?;

        let row = sqlx::query(
            "SELECT id, task_id, from_state, to_state, trigger_name, triggered_by, trigger_reason, hook_results_json, rejection, created_at FROM transition_log WHERE id = ?",
        )
        .bind(&input.id)
        .fetch_one(&self.pool)
        .await
        .map_err(map_workflow_sqlx_error)?;

        Ok(map_transition_log_row(row))
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
            "SELECT id, task_id, from_state, to_state, trigger_name, triggered_by, trigger_reason, hook_results_json, rejection, created_at FROM transition_log WHERE task_id = ? ORDER BY created_at, rowid",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await
        .map_err(map_workflow_sqlx_error)?;

        Ok(rows.into_iter().map(map_transition_log_row).collect())
    }

    async fn count_gate_rejections(
        &self,
        task_id: &str,
        gate_state: &str,
    ) -> std::result::Result<i64, DbError> {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM transition_log AS rejection
             WHERE rejection.task_id = ?
               AND rejection.from_state = ?
               AND rejection.rejection = 1
               AND NOT EXISTS (
                   SELECT 1
                   FROM transition_log AS boundary
                   WHERE boundary.task_id = rejection.task_id
                     AND boundary.from_state = rejection.from_state
                     AND boundary.rejection = 0
                     AND boundary.trigger_name IN ('reset_retry_window', 'reset_to_initial')
                     AND (
                         boundary.created_at > rejection.created_at
                         OR (
                             boundary.created_at = rejection.created_at
                             AND boundary.rowid > rejection.rowid
                         )
                     )
               )",
        )
        .bind(task_id)
        .bind(gate_state)
        .fetch_one(&self.pool)
        .await
        .map_err(map_workflow_sqlx_error)
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
