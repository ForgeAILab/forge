use super::*;
use crate::{
    CompareAndMoveTask, CreateTransitionLog, MoveTaskIdentity, MoveTaskPersistence, MoveTaskResult,
    TaskBoardRepo, TransitionLog,
};
use sqlx::{QueryBuilder, Sqlite, Transaction};

const MIN_BOARD_POSITION_GAP: f64 = 1e-9;

#[async_trait]
impl TaskBoardRepo for SqliteDb {
    async fn board_revision(&self, project_id: &str) -> Result<i64> {
        sqlx::query_scalar("SELECT board_revision FROM project WHERE id = ?")
            .bind(project_id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn replay_move_task(
        &self,
        operation_id: &str,
        identity: &MoveTaskIdentity,
    ) -> Result<Option<MoveTaskResult>> {
        let Some((request_hash, state, result_json)) =
            sqlx::query_as::<_, (String, String, Option<String>)>(
                "SELECT request_hash, state, result_json FROM task_move_operation WHERE operation_id = ?",
            )
            .bind(operation_id)
            .fetch_optional(&self.pool)
            .await?
        else {
            return Ok(None);
        };
        if request_hash != normalized_identity(identity)? {
            return Err(DbError::MoveOperationConflict {
                operation_id: operation_id.to_owned(),
            });
        }
        if state != "completed" {
            return Err(DbError::MoveOperationIncomplete {
                operation_id: operation_id.to_owned(),
            });
        }
        let raw = result_json.ok_or_else(|| {
            DbError::InvalidTaskMove("completed move has no stored result".to_owned())
        })?;
        serde_json::from_str(&raw).map(Some).map_err(|error| {
            DbError::InvalidTaskMove(format!("invalid stored move result: {error}"))
        })
    }

    async fn compare_and_move_task(
        &self,
        input: CompareAndMoveTask,
    ) -> Result<MoveTaskPersistence> {
        validate_move_input(&input)?;
        let request_hash = normalized_request(&input)?;
        let mut tx = crate::begin_immediate(&self.pool).await?;

        let reserved = sqlx::query(
            "INSERT INTO task_move_operation (operation_id, project_id, task_id, request_hash, state, created_at, updated_at) VALUES (?, ?, ?, ?, 'processing', ?, ?) ON CONFLICT(operation_id) DO NOTHING",
        )
        .bind(&input.operation_id)
        .bind(&input.project_id)
        .bind(&input.task_id)
        .bind(&request_hash)
        .bind(&input.updated_at)
        .bind(&input.updated_at)
        .execute(&mut *tx)
        .await?;

        if reserved.rows_affected() == 0 {
            let existing = sqlx::query_as::<_, (String, String, Option<String>)>(
                "SELECT request_hash, state, result_json FROM task_move_operation WHERE operation_id = ?",
            )
            .bind(&input.operation_id)
            .fetch_one(&mut *tx)
            .await?;
            if existing.0 != request_hash {
                return Err(DbError::MoveOperationConflict {
                    operation_id: input.operation_id,
                });
            }
            if existing.1 == "completed" {
                let raw = existing.2.ok_or_else(|| {
                    DbError::InvalidTaskMove("completed move has no stored result".to_owned())
                })?;
                let result = serde_json::from_str(&raw).map_err(|error| {
                    DbError::InvalidTaskMove(format!("invalid stored move result: {error}"))
                })?;
                tx.commit().await?;
                return Ok(MoveTaskPersistence::Replayed(Box::new(result)));
            }
            return Err(DbError::MoveOperationIncomplete {
                operation_id: input.operation_id,
            });
        }

        let actual_board_revision =
            sqlx::query_scalar::<_, i64>("SELECT board_revision FROM project WHERE id = ?")
                .bind(&input.project_id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(DbError::NotFound)?;
        if actual_board_revision != input.board_revision {
            return Err(DbError::BoardRevisionConflict {
                expected: input.board_revision,
                actual: actual_board_revision,
            });
        }

        let task_row = sqlx::query(&format!(
            "SELECT {TASK_COLUMNS} FROM task WHERE id = ? AND deleted_at IS NULL AND archived_at IS NULL",
        ))
        .bind(&input.task_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
        let task = map_task(task_row)?;
        if task.project_id != input.project_id {
            return Err(DbError::InvalidTaskMove(
                "task belongs to a different project".to_owned(),
            ));
        }
        if task.version != input.task_version {
            return Err(DbError::TaskVersionConflict {
                expected: input.task_version,
                actual: task.version,
            });
        }

        let mut before = load_neighbor(&mut tx, &input, input.before_id.as_deref()).await?;
        let mut after = load_neighbor(&mut tx, &input, input.after_id.as_deref()).await?;
        validate_placement(&mut tx, &input, before.as_ref(), after.as_ref()).await?;

        if before
            .as_ref()
            .zip(after.as_ref())
            .is_some_and(|(before, after)| {
                after.board_position - before.board_position < MIN_BOARD_POSITION_GAP
            })
        {
            renormalize_positions(&mut tx, &input.project_id, &input.updated_at).await?;
            before = load_neighbor(&mut tx, &input, input.before_id.as_deref()).await?;
            after = load_neighbor(&mut tx, &input, input.after_id.as_deref()).await?;
        }

        let new_position = match (&before, &after) {
            (Some(before), Some(after)) => {
                (before.board_position + after.board_position) / 2.0
            }
            (Some(before), None) => before.board_position + 1.0,
            (None, Some(after)) => after.board_position - 1.0,
            (None, None) => {
                sqlx::query_scalar::<_, f64>(
                    "SELECT COALESCE(MAX(board_position), 0.0) + 1.0 FROM task WHERE project_id = ? AND deleted_at IS NULL AND archived_at IS NULL AND id != ?",
                )
                .bind(&input.project_id)
                .bind(&input.task_id)
                .fetch_one(&mut *tx)
                .await?
            }
        };
        if !new_position.is_finite() {
            return Err(DbError::InvalidTaskMove(
                "calculated board position is not finite".to_owned(),
            ));
        }

        let updated = sqlx::query(
            "UPDATE task SET status = ?, board_position = ?, version = version + 1, updated_at = ?, blocked_json = NULL, entry_barrier_json = ? WHERE id = ? AND project_id = ? AND version = ? AND deleted_at IS NULL AND archived_at IS NULL",
        )
        .bind(&input.target_status)
        .bind(new_position)
        .bind(&input.updated_at)
        .bind(input.entry_barrier_json.as_deref())
        .bind(&input.task_id)
        .bind(&input.project_id)
        .bind(input.task_version)
        .execute(&mut *tx)
        .await?;
        if updated.rows_affected() != 1 {
            let actual = sqlx::query_scalar::<_, i64>("SELECT version FROM task WHERE id = ?")
                .bind(&input.task_id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(DbError::NotFound)?;
            return Err(DbError::TaskVersionConflict {
                expected: input.task_version,
                actual,
            });
        }

        let updated_task_row =
            sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
                .bind(&input.task_id)
                .fetch_one(&mut *tx)
                .await?;
        let updated_task = map_task(updated_task_row)?;
        if task.blocked_json != updated_task.blocked_json {
            let interruption_event = CreateDomainEvent::task_interruption_changed(&updated_task);
            DomainEventRepo::append_event_in_tx(self, &mut tx, &interruption_event).await?;
        }

        let transition_input = CreateTransitionLog {
            id: input.transition_log_id.clone(),
            task_id: input.task_id.clone(),
            from_state: task.status.clone(),
            to_state: input.target_status.clone(),
            trigger_name: input.trigger_name.clone(),
            triggered_by: input.triggered_by.clone(),
            trigger_reason: input.trigger_reason.clone(),
            hook_results_json: None,
            rejection: input.rejection,
            created_at: input.updated_at.clone(),
        };
        insert_transition_log(&mut tx, &transition_input).await?;
        let transition_event = CreateDomainEvent::task_transition(
            transition_input.id.clone(),
            transition_input.task_id.clone(),
            input.project_id.clone(),
            &transition_input.from_state,
            &transition_input.to_state,
            transition_input.trigger_name.as_deref(),
            &transition_input.triggered_by,
            &transition_input.trigger_reason,
            transition_input.rejection,
            transition_input.created_at.clone(),
            input.workflow_snapshot,
        );
        DomainEventRepo::append_event_in_tx(self, &mut tx, &transition_event).await?;

        let board_revision =
            sqlx::query_scalar::<_, i64>("SELECT board_revision FROM project WHERE id = ?")
                .bind(&input.project_id)
                .fetch_one(&mut *tx)
                .await?;
        let result = MoveTaskResult {
            task: updated_task,
            board_revision,
            operation_id: input.operation_id.clone(),
            old_status: task.status,
            old_board_position: task.board_position,
            before_id: input.before_id.clone(),
            after_id: input.after_id.clone(),
        };
        let direct_result_json = serde_json::to_string(&result).map_err(|error| {
            DbError::InvalidTaskMove(format!("failed to serialize move result: {error}"))
        })?;
        sqlx::query(
            "UPDATE task_move_operation SET state = 'committed', direct_result_json = ?, updated_at = ? WHERE operation_id = ? AND state = 'processing'",
        )
        .bind(direct_result_json)
        .bind(&input.updated_at)
        .bind(&input.operation_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(MoveTaskPersistence::Committed {
            result: Box::new(result),
            transition_log: Box::new(TransitionLog {
                id: transition_input.id,
                task_id: transition_input.task_id,
                from_state: transition_input.from_state,
                to_state: transition_input.to_state,
                trigger_name: transition_input.trigger_name,
                triggered_by: transition_input.triggered_by,
                trigger_reason: transition_input.trigger_reason,
                hook_results_json: None,
                rejection: transition_input.rejection,
                created_at: transition_input.created_at,
            }),
        })
    }

    async fn complete_move_operation(
        &self,
        operation_id: &str,
        result: &MoveTaskResult,
        updated_at: &str,
    ) -> Result<()> {
        let result_json = serde_json::to_string(result).map_err(|error| {
            DbError::InvalidTaskMove(format!("failed to serialize final move result: {error}"))
        })?;
        let updated = sqlx::query(
            "UPDATE task_move_operation SET state = 'completed', result_json = ?, updated_at = ? WHERE operation_id = ? AND state = 'committed'",
        )
        .bind(result_json)
        .bind(updated_at)
        .bind(operation_id)
        .execute(&self.pool)
        .await?;
        if updated.rows_affected() == 1 {
            return Ok(());
        }
        let state = sqlx::query_scalar::<_, String>(
            "SELECT state FROM task_move_operation WHERE operation_id = ?",
        )
        .bind(operation_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(DbError::NotFound)?;
        if state == "completed" {
            Ok(())
        } else {
            Err(DbError::MoveOperationIncomplete {
                operation_id: operation_id.to_owned(),
            })
        }
    }
}

fn validate_move_input(input: &CompareAndMoveTask) -> Result<()> {
    if input.operation_id.is_empty() || input.project_id.is_empty() || input.task_id.is_empty() {
        return Err(DbError::InvalidTaskMove(
            "operation_id, project_id, and task_id are required".to_owned(),
        ));
    }
    if input.target_column_statuses.is_empty()
        || !input
            .target_column_statuses
            .iter()
            .any(|status| status == &input.target_status)
    {
        return Err(DbError::InvalidTaskMove(
            "target status is not in the destination column group".to_owned(),
        ));
    }
    if input.before_id == input.after_id && input.before_id.is_some() {
        return Err(DbError::InvalidTaskMove(
            "before_id and after_id must differ".to_owned(),
        ));
    }
    if input.before_id.as_deref() == Some(input.task_id.as_str())
        || input.after_id.as_deref() == Some(input.task_id.as_str())
    {
        return Err(DbError::InvalidTaskMove(
            "a neighbor cannot be the moved task".to_owned(),
        ));
    }
    Ok(())
}

fn normalized_request(input: &CompareAndMoveTask) -> Result<String> {
    normalized_identity(&input.identity())
}

fn normalized_identity(identity: &MoveTaskIdentity) -> Result<String> {
    serde_json::to_string(&(
        &identity.project_id,
        &identity.task_id,
        identity.task_version,
        identity.board_revision,
        &identity.target_status,
        &identity.before_id,
        &identity.after_id,
    ))
    .map_err(|error| DbError::InvalidTaskMove(format!("invalid move request: {error}")))
}

async fn load_neighbor(
    tx: &mut Transaction<'_, Sqlite>,
    input: &CompareAndMoveTask,
    neighbor_id: Option<&str>,
) -> Result<Option<Task>> {
    let Some(neighbor_id) = neighbor_id else {
        return Ok(None);
    };
    let row = sqlx::query(&format!(
        "SELECT {TASK_COLUMNS} FROM task WHERE id = ? AND deleted_at IS NULL AND archived_at IS NULL",
    ))
    .bind(neighbor_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| DbError::InvalidTaskMove(format!("neighbor task {neighbor_id} was not found")))?;
    let task = map_task(row)?;
    if task.project_id != input.project_id {
        return Err(DbError::InvalidTaskMove(format!(
            "neighbor task {neighbor_id} belongs to another project"
        )));
    }
    if !input
        .target_column_statuses
        .iter()
        .any(|status| status == &task.status)
    {
        return Err(DbError::InvalidTaskMove(format!(
            "neighbor task {neighbor_id} is outside the destination column"
        )));
    }
    Ok(Some(task))
}

async fn validate_placement(
    tx: &mut Transaction<'_, Sqlite>,
    input: &CompareAndMoveTask,
    before: Option<&Task>,
    after: Option<&Task>,
) -> Result<()> {
    if let (Some(before), Some(after)) = (before, after) {
        if before.board_position >= after.board_position {
            return Err(DbError::InvalidTaskMove(
                "neighbors are ordered inconsistently".to_owned(),
            ));
        }
    }

    let mut query = QueryBuilder::<Sqlite>::new("SELECT COUNT(*) FROM task WHERE project_id = ");
    query
        .push_bind(&input.project_id)
        .push(" AND id != ")
        .push_bind(&input.task_id)
        .push(" AND deleted_at IS NULL AND archived_at IS NULL AND status IN (");
    let mut statuses = query.separated(", ");
    for status in &input.target_column_statuses {
        statuses.push_bind(status);
    }
    statuses.push_unseparated(")");
    match (before, after) {
        (None, None) => {}
        (Some(before), Some(after)) => {
            query
                .push(" AND board_position > ")
                .push_bind(before.board_position)
                .push(" AND board_position < ")
                .push_bind(after.board_position);
        }
        (Some(before), None) => {
            query
                .push(" AND board_position > ")
                .push_bind(before.board_position);
        }
        (None, Some(after)) => {
            query
                .push(" AND board_position < ")
                .push_bind(after.board_position);
        }
    }
    let count = query
        .build_query_scalar::<i64>()
        .fetch_one(&mut **tx)
        .await?;
    if count != 0 {
        let message = match (before, after) {
            (None, None) => "both neighbors may be null only for an empty destination column",
            _ => "neighbors do not describe an adjacent destination placement",
        };
        return Err(DbError::InvalidTaskMove(message.to_owned()));
    }
    Ok(())
}

async fn renormalize_positions(
    tx: &mut Transaction<'_, Sqlite>,
    project_id: &str,
    updated_at: &str,
) -> Result<()> {
    let task_ids = sqlx::query_scalar::<_, String>(
        "SELECT id FROM task WHERE project_id = ? AND deleted_at IS NULL AND archived_at IS NULL ORDER BY board_position ASC, created_at ASC, id ASC",
    )
    .bind(project_id)
    .fetch_all(&mut **tx)
    .await?;
    for (index, task_id) in task_ids.into_iter().enumerate() {
        sqlx::query("UPDATE task SET board_position = ?, updated_at = ? WHERE id = ?")
            .bind(index as f64 + 1.0)
            .bind(updated_at)
            .bind(task_id)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

async fn insert_transition_log(
    tx: &mut Transaction<'_, Sqlite>,
    input: &CreateTransitionLog,
) -> Result<()> {
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
    .execute(&mut **tx)
    .await?;
    Ok(())
}
