//! The worker's Task-step port over the real step queue.
use super::INTEGRATION_STEP_KIND;
use crate::{
    integration_worker::{
        IntegrationStepAction, IntegrationStepPort, IntegrationStepRequest, IntegrationStepState,
    },
    Result, ServiceError,
};
use async_trait::async_trait;
use db::{IntegrationQueueRepo, SqliteDb, TaskStepRepo};
use std::sync::Arc;

/// Enqueues one `integration` Task step per worker request. Idempotent on
/// `(task_id, causation_key)`. It writes `task_step` rows only.
///
/// Fencing per action:
/// - `request_check`, `settle`, `send_back`, `park`: entry-fenced on the
///   attempt's admitted status entry, so a preempting Cancel / Hold drops a
///   pending one at once.
/// - `clear`: identity-fenced (the Task has left that entry by the time it
///   runs) and not protected.
/// - `result`: protected (identity-fenced and marked as started integration)
///   exactly as the one `settle` pre-enqueues; when `settle` already enqueued
///   it this is the same row.
pub struct TaskStepIntegrationPort {
    db: Arc<SqliteDb>,
}

impl TaskStepIntegrationPort {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }

    /// The step row for a request. `expected_status` is the status the
    /// attempt was admitted in; `available_at` is when it may first run.
    pub fn step_input(
        request: &IntegrationStepRequest,
        expected_status: &str,
        expected_version: i64,
        causation_step_id: Option<String>,
        available_at: String,
    ) -> Result<db::EnqueueTaskStep> {
        let key = request.causation_key();
        Ok(db::EnqueueTaskStep {
            kind: INTEGRATION_STEP_KIND.into(),
            id: db::new_uuid_v4(),
            task_id: request.task_id.clone(),
            payload_json: serde_json::to_string(request)
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
            causation_step_id,
            causation_key: key.clone(),
            chain_id: key,
            chain_position: 1,
            expected_status: expected_status.to_owned(),
            expected_version,
            expected_epoch: Some(request.expected_epoch),
            lane: "fast".into(),
            available_at,
        })
    }
}

#[async_trait]
impl IntegrationStepPort for TaskStepIntegrationPort {
    async fn enqueue_step(&self, request: &IntegrationStepRequest) -> Result<()> {
        let attempt = self
            .db
            .integration_attempt(&request.attempt_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("integration attempt", &request.attempt_id))?;
        if attempt.task_ref != request.task_id {
            return Err(ServiceError::invalid_operation(
                "integration step request names another Task",
            ));
        }
        let version: Option<i64> =
            sqlx::query_scalar("SELECT version FROM task WHERE id=? AND deleted_at IS NULL")
                .bind(&request.task_id)
                .fetch_optional(self.db.pool())
                .await?;
        let Some(version) = version else {
            // A deleted Task has no step queue; its attempt ends without one.
            return Ok(());
        };
        let input = Self::step_input(
            request,
            &attempt.expected_status,
            version,
            None,
            db::now_rfc3339(),
        )?;
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        match request.action {
            IntegrationStepAction::Result => {
                self.db
                    .enqueue_protected_integration_step_in_tx(&mut tx, &input)
                    .await?;
            }
            IntegrationStepAction::Clear => {
                let id = self.db.enqueue_step_in_tx(&mut tx, &input).await?;
                self.db
                    .mark_step_identity_fenced_in_tx(&mut tx, &id)
                    .await?;
            }
            _ => {
                self.db.enqueue_step_in_tx(&mut tx, &input).await?;
            }
        }
        tx.commit().await?;
        self.db.domain_event_notify().notify_waiters();
        Ok(())
    }

    async fn ready_result_step(&self, attempt_id: &str, effect_seq: i64) -> Result<()> {
        let Some(attempt) = self.db.integration_attempt(attempt_id).await? else {
            return Ok(());
        };
        let key = format!(
            "integration:{attempt_id}:{effect_seq}:{}",
            IntegrationStepAction::Result.as_str()
        );
        self.db
            .ready_integration_step(&attempt.task_ref, &key)
            .await?;
        Ok(())
    }

    async fn step_state(&self, request: &IntegrationStepRequest) -> Result<IntegrationStepState> {
        let status: Option<String> =
            sqlx::query_scalar("SELECT status FROM task_step WHERE task_id=? AND causation_key=?")
                .bind(&request.task_id)
                .bind(request.causation_key())
                .fetch_optional(self.db.pool())
                .await?;
        Ok(match status.as_deref() {
            None => IntegrationStepState::Missing,
            // `suspended` waits for a check and is as live as a queued step.
            Some("pending" | "claimed" | "suspended" | "parked") => IntegrationStepState::Live,
            Some("done") => IntegrationStepState::Done,
            Some(_) => {
                let left: bool = sqlx::query_scalar(
                    "SELECT NOT EXISTS(SELECT 1 FROM integration_attempt a JOIN task t ON t.id=a.task_ref WHERE a.id=? AND t.status=a.expected_status AND t.status_epoch=a.expected_epoch AND t.deleted_at IS NULL)",
                )
                .bind(&request.attempt_id)
                .fetch_one(self.db.pool())
                .await?;
                if left {
                    IntegrationStepState::TaskLeft
                } else {
                    IntegrationStepState::Dead
                }
            }
        })
    }
}
