//! One typed fixture setter, shared by db and dependent crate tests.
use super::*;
use crate::{EnqueueTaskStep, TaskRepo, TaskStepRepo};

pub async fn set_task_condition(
    db: &SqliteDb,
    task_id: &str,
    statement: ConditionStatement,
) -> Result<TaskCondition> {
    async fn set(db: &SqliteDb, task_id: &str, statement: &ConditionStatement) -> Result<()> {
        let mut tx = crate::begin_immediate(db.pool()).await?;
        db.state_task_condition_in_tx(&mut tx, task_id, ConditionChange::Full, statement)
            .await?;
        tx.commit().await?;
        Ok(())
    }
    if crate::task_writer::owns_task(task_id) {
        set(db, task_id, &statement).await?;
    } else {
        let task = TaskRepo::get_by_id(db, task_id, false)
            .await?
            .ok_or(DbError::NotFound)?;
        let id = crate::new_uuid_v4();
        db.enqueue_step(&EnqueueTaskStep {
            id: id.clone(),
            task_id: task_id.to_owned(),
            kind: "command".into(),
            payload_json: "{}".into(),
            causation_step_id: None,
            causation_key: id.clone(),
            chain_id: id,
            chain_position: 1,
            expected_status: task.status,
            expected_version: task.version,
            expected_epoch: None,
            lane: "fast".into(),
            available_at: crate::now_rfc3339(),
        })
        .await?;
        let step = db
            .claim_step(
                "typed-condition-fixture",
                Some(task_id),
                &crate::task_writer::lease_deadline(),
            )
            .await?
            .ok_or(DbError::VersionConflict)?;
        crate::task_writer::in_task_step(step.clone(), async {
            let result = set(db, task_id, &statement).await;
            let mut tx = crate::begin_immediate(db.pool()).await?;
            with_statement(
                task_id,
                ConditionStatement::Preserve,
                db.finish_step_in_tx(&mut tx, &step, "done", None),
            )
            .await?;
            tx.commit().await?;
            db.release_step(
                &step.id,
                step.claimed_by.as_deref().expect("fixture step is claimed"),
            )
            .await?;
            result
        })
        .await?;
    }
    db.task_condition(task_id).await
}
