//! Repair no change budget accounting operations.
use super::*;

/// The latest settled Review in the current window, only when it failed, and the commit
/// it judged. A later pass must not resurrect an earlier failure. Frozen reviewer
/// contracts take precedence over entry CI evidence, then delivery evidence.
pub async fn failed_review_for_no_change(
    db: &SqliteDb,
    task: &str,
) -> Result<Option<(Review, Option<String>)>> {
    let row: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT r.id, COALESCE(
            CASE WHEN json_valid(r.step_results_json) THEN json_extract(r.step_results_json,'$.conformance.contract.commit_sha') END,
            (SELECT cr.commit_sha FROM check_consumer c JOIN check_run cr ON cr.id=c.run_id
             WHERE c.task_id=r.task_id AND c.origin='entry'
               AND c.request_key='task-step/1|entry|'||r.task_id||'|'||c.status_epoch||'|'||r.id
             ORDER BY c.created_at DESC,c.id DESC LIMIT 1),
            e.after_sha)
         FROM review r JOIN execution e ON e.id=r.execution_id
         LEFT JOIN task_budget b ON b.task_id=r.task_id AND b.kind='review'
         LEFT JOIN transition_log boundary ON boundary.id=b.window_id AND boundary.task_id=r.task_id
         WHERE r.task_id=? AND r.status IN ('passed','failed')
           AND (boundary.id IS NULL OR r.created_at >= boundary.created_at)
         ORDER BY r.attempt_number DESC,r.id DESC LIMIT 1",
    ).bind(task).fetch_optional(db.pool()).await?;
    match row {
        Some((id, head)) => Ok(crate::ReviewRepo::get_by_id(db, &id)
            .await?
            .filter(|review| review.status == ReviewStatus::Failed)
            .map(|review| (review, head))),
        None => Ok(None),
    }
}

/// Start the guard, retaining the decision of an entry already in progress when
/// the guard shipped. Exit hooks precede the guard, so only entry checkpoints
/// prove that it is too late to evaluate it.
pub async fn start_repair_no_change(
    db: &SqliteDb,
    step: &crate::TaskStep,
    first_entry_index: i64,
) -> Result<Option<RepairNoChangeDecision>> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    db.fence_hook_in_tx(&mut tx, step).await?;
    let recorded: Option<String> = sqlx::query_scalar(
        "SELECT result_json FROM task_hook_checkpoint WHERE step_id=? AND hook_index=?",
    )
    .bind(&step.id)
    .bind(REPAIR_NO_CHANGE_CHECKPOINT)
    .fetch_optional(&mut *tx)
    .await?
    .flatten();
    let recorded = match recorded {
        Some(recorded) => Some(recorded),
        None => {
            // Every hook, also one that suspended for a check or for disk,
            // opened its checkpoint first. A recorded wait alone is no proof:
            // a waking step keeps `awaited_consumer_id`, including after an
            // exit hook's wait that precedes the guard.
            let started: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM task_hook_checkpoint WHERE step_id=? AND hook_index>=?)",
            )
            .bind(&step.id)
            .bind(first_entry_index)
            .fetch_one(&mut *tx)
            .await?;
            started.then(|| serde_json::to_string(&RepairNoChangeDecision::Continue).unwrap())
        }
    };
    sqlx::query("INSERT INTO task_hook_checkpoint(step_id,hook_index,started_at,result_json) VALUES(?,?,?,?) ON CONFLICT(step_id,hook_index) DO UPDATE SET result_json=excluded.result_json")
        .bind(&step.id).bind(REPAIR_NO_CHANGE_CHECKPOINT).bind(crate::now_rfc3339())
        .bind(&recorded).execute(&mut *tx).await?;
    tx.commit().await?;
    recorded
        .map(|raw| serde_json::from_str(&raw).map_err(|e| DbError::Check(e.to_string())))
        .transpose()
}

/// Save the guard's decision and its one allowed reminder charge together.
/// A replay returns that decision before checking mutable Git state again.
pub async fn settle_repair_no_change(
    db: &SqliteDb,
    task: &Task,
    step: &crate::TaskStep,
    unchanged_attempt: Option<i64>,
) -> Result<RepairNoChangeDecision> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    db.fence_hook_in_tx(&mut tx, step).await?;
    let current: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task WHERE id=? AND version=? AND status=? AND deleted_at IS NULL)")
        .bind(&task.id).bind(task.version).bind(&task.status).fetch_one(&mut *tx).await?;
    if !current {
        return Err(DbError::VersionConflict);
    }
    let recorded: Option<String> = sqlx::query_scalar(
        "SELECT result_json FROM task_hook_checkpoint WHERE step_id=? AND hook_index=?",
    )
    .bind(&step.id)
    .bind(REPAIR_NO_CHANGE_CHECKPOINT)
    .fetch_optional(&mut *tx)
    .await?
    .flatten();
    if let Some(recorded) = recorded {
        return serde_json::from_str(&recorded).map_err(|error| DbError::Check(error.to_string()));
    }
    let decision = match unchanged_attempt {
        None => RepairNoChangeDecision::Continue,
        Some(attempt) => {
            let window: String = sqlx::query_scalar("SELECT COALESCE((SELECT window_id FROM task_budget WHERE task_id=? AND kind='review'),'initial')")
                .bind(&task.id).fetch_one(&mut *tx).await?;
            reset(&mut tx, &task.id, Kind::RepairNoChange.key(), &window).await?;
            let charge = charge(
                &mut tx,
                &task.id,
                Kind::RepairNoChange.key(),
                i64::from(Kind::RepairNoChange.default_limit()),
                &format!("entry:{}", step.id),
            )
            .await?;
            if charge.charged {
                RepairNoChangeDecision::SendBack { attempt }
            } else {
                RepairNoChangeDecision::Park
            }
        }
    };
    sqlx::query("UPDATE task_hook_checkpoint SET result_json=? WHERE step_id=? AND hook_index=?")
        .bind(serde_json::to_string(&decision).map_err(|error| DbError::Check(error.to_string()))?)
        .bind(&step.id)
        .bind(REPAIR_NO_CHANGE_CHECKPOINT)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(decision)
}
