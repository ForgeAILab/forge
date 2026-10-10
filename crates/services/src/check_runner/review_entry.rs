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
/// the result. Cancel the review attempt each one opened, so no attempt
/// stays `running`; the consumer itself is fenced by its authority (and
/// cancelled by the check worker once the Task's status entry has moved).
/// Idempotent: a cleaned wait is forgotten.
pub async fn abandon_superseded_waits(db: &Arc<SqliteDb>, task_id: &str) -> Result<()> {
    for (step_id, consumer_id) in db.abandoned_check_waits(task_id).await? {
        let review_id = CheckDeliveryRepo::check_consumer_delivery(&**db, &consumer_id)
            .await?
            .filter(|state| state.consumer.origin == db::CheckConsumerOrigin::Entry)
            .and_then(|state| {
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
