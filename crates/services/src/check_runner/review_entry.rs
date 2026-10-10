//! The review-entry consumer family: `run_ci_steps` asks from its hooks step,
//! which then waits suspended; the delivered result wakes that step and the
//! hook settles the Review from the result, under the step's own authority.
use super::consumer::{CheckApplication, CheckConsumerFamily, CheckVerdict};
use crate::Result;
use async_trait::async_trait;
use db::{CheckDeliveryRepo, ReviewRepo, ReviewStatus, SqliteDb};
use std::sync::Arc;

pub struct ReviewEntryChecks {
    db: Arc<SqliteDb>,
}
impl ReviewEntryChecks {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }
}

#[async_trait]
impl CheckConsumerFamily for ReviewEntryChecks {
    /// The review attempt that still waits for its CI result: the Task's
    /// newest Review, running, with an empty `ci_steps` list (as created). A delivery
    /// for an earlier attempt, or for a Task whose entry already settled, is
    /// stale. The Task leaving the status entry is fenced by its epoch
    /// before this is asked.
    async fn current_authority(&self, task_id: &str, _status_epoch: i64) -> Result<Option<String>> {
        let newest = ReviewRepo::list_by_task(&*self.db, task_id)
            .await?
            .into_iter()
            .max_by_key(|review| review.attempt_number);
        Ok(newest.filter(awaits_ci).map(|review| review.id))
    }

    /// Wake the suspended hooks step. It reads the same result and writes
    /// the Review, so a redelivery only wakes a step that is already done.
    /// Exhausted infrastructure is no verdict: the Task stays parked on its
    /// typed check condition and the step keeps waiting.
    async fn apply(&self, application: &CheckApplication) -> Result<()> {
        if matches!(application.verdict, CheckVerdict::Result(_)) {
            self.db
                .wake_suspended_hooks(&application.task_id, &application.consumer_id)
                .await?;
        }
        Ok(())
    }
}

/// A review attempt still waits for CI: running, with the empty `ci_steps`
/// list it was created with.
fn awaits_ci(review: &db::Review) -> bool {
    review.status == ReviewStatus::Running
        && serde_json::from_str::<serde_json::Value>(&review.step_results_json)
            .ok()
            .is_none_or(|details| {
                details["ci_steps"]
                    .as_array()
                    .is_none_or(|steps| steps.is_empty())
            })
}

/// Hooks steps of this Task that were superseded while they waited for
/// review-entry CI (the Task was cancelled, held or left `review`) never read
/// the result. For each one:
/// - a run still executing on a daemon is cancelled there when the daemon is
///   reachable, and otherwise fences the workspace with a
///   `pending_remote_cancel` marker until the daemon confirms, exactly as a
///   preempted remote command does;
/// - the consumer is cancelled, so a late delivery is stale and the check
///   worker stops a run nobody waits for and frees its slot;
/// - the review attempt the step opened is cancelled, so no attempt stays
///   `running`.
///
/// Idempotent: a cleaned wait is forgotten.
pub async fn abandon_superseded_waits(
    db: &Arc<SqliteDb>,
    daemons: Option<Arc<crate::daemon_transport::DaemonConnectionRegistry>>,
    task_id: &str,
) -> Result<()> {
    for (step_id, consumer_id) in db.abandoned_check_waits(task_id).await? {
        let state = CheckDeliveryRepo::check_consumer_delivery(&**db, &consumer_id)
            .await?
            .filter(|state| state.consumer.origin == db::CheckConsumerOrigin::Entry);
        if let Some(state) = &state {
            fence_remote_run(db, daemons.clone(), &step_id, &state.consumer).await?;
            db.cancel_abandoned_check_consumer(&consumer_id).await?;
            // The wait is over: the Task no longer states that it waits for
            // this check. Only the Task's own step may write its condition.
            if db::task_writer::owns_task(task_id) {
                CheckDeliveryRepo::state_check_condition(
                    &**db,
                    task_id,
                    &db::ConditionStatement::CheckCleared {
                        consumer_id: consumer_id.clone(),
                    },
                )
                .await?;
            }
        }
        let review_id = state.and_then(|state| {
            state
                .consumer
                .request_key
                .rsplit('|')
                .next()
                .map(str::to_owned)
        });
        let review = match review_id {
            Some(id) => ReviewRepo::get_by_id(&**db, &id).await?,
            None => None,
        };
        if let Some(review) = review.filter(awaits_ci) {
            let now = db::now_rfc3339();
            let mut details: serde_json::Value = serde_json::from_str(&review.step_results_json)
                .unwrap_or_else(|_| serde_json::json!({ "ci_steps": [] }));
            details["execution_retry"] = serde_json::json!({
                "status": "cancelled_authority_lost",
                "reason": "the Task left review while its CI ran",
                "cancelled_at": now,
            });
            ReviewRepo::cancel_if_unchanged(
                &**db,
                &review.id,
                review.status.clone(),
                &review.updated_at,
                details.to_string(),
                &now,
                &now,
            )
            .await?;
        }
        db.forget_check_wait(&step_id).await?;
    }
    Ok(())
}

/// The run `consumer` waits for may still execute on a daemon. When this
/// consumer is the only one waiting, its operation is put under the remote
/// cancellation fence: stopped now if the daemon answers, or marked so the
/// workspace stays excluded until the daemon reconnects and confirms.
async fn fence_remote_run(
    db: &Arc<SqliteDb>,
    daemons: Option<Arc<crate::daemon_transport::DaemonConnectionRegistry>>,
    step_id: &str,
    consumer: &db::CheckConsumer,
) -> Result<()> {
    use db::{CheckDispatchTarget, CheckWorkerRepo};
    let Some(run_id) = consumer.run_id.as_deref() else {
        return Ok(());
    };
    if consumer.result_id.is_some() {
        return Ok(());
    }
    let record = match CheckWorkerRepo::check_worker_record(&**db, run_id).await {
        Ok(record) => record,
        Err(db::DbError::NotFound) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if record.run.state.terminal() || record.receipt.is_some() {
        return Ok(());
    }
    let Some(db::CheckDispatchIntent {
        target: CheckDispatchTarget::Daemon { workspace },
        ..
    }) = &record.dispatch
    else {
        return Ok(());
    };
    let Some(workspace_id) = record.run.workspace_id.as_deref() else {
        return Ok(());
    };
    // A run other consumers still wait for keeps running for them.
    let others = CheckWorkerRepo::active_check_consumers(&**db, run_id, &db::now_rfc3339())
        .await?
        .into_iter()
        .any(|other| other.id != consumer.id);
    if others {
        return Ok(());
    }
    let Ok(generation) = i64::try_from(workspace.generation) else {
        return Ok(());
    };
    let Some(operation) = db
        .register_abandoned_check_operation(
            step_id,
            consumer.status_epoch,
            &record.run.operation_id,
            workspace_id,
            &workspace.placement_id,
            &workspace.daemon_id,
            &workspace.runtime_id,
            generation,
        )
        .await?
    else {
        return Ok(());
    };
    crate::remote_cancel::cancel_operations(db, daemons, &[operation]).await?;
    Ok(())
}
