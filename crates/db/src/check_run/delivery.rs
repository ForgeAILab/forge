//! Check-result outbox. Only check rows and enqueue-only Task-step writes.
use super::*;
use crate::{EnqueueTaskStep, TaskStepRepo};

/// An identity envelope, not permission to apply a verdict. The consuming
/// Task step compares the current epoch, candidate, definition and authority.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckResultDelivery {
    pub consumer_id: String,
    pub result_id: String,
    pub run_id: String,
    pub identity_key: String,
    pub spec_digest: String,
    pub commit_sha: String,
    pub request_key: String,
    pub origin: CheckConsumerOrigin,
}

#[async_trait]
pub trait CheckDeliveryRepo: Send + Sync {
    /// The durable marker and enqueue share a transaction. A crash on either
    /// side of this call cannot lose a consumer or enqueue it twice.
    async fn enqueue_check_result_steps(&self, limit: i64) -> Result<u64>;
}
#[async_trait]
impl CheckDeliveryRepo for SqliteDb {
    async fn enqueue_check_result_steps(&self, limit: i64) -> Result<u64> {
        if !(1..=1000).contains(&limit) {
            return Err(DbError::Check("invalid check delivery batch limit".into()));
        }
        let mut tx = begin_immediate(self.pool()).await?;
        let rows = sqlx::query("SELECT c.*, r.spec_digest, r.commit_sha, t.status AS task_status, t.version AS task_version FROM check_consumer c JOIN check_run r ON r.id=c.run_id JOIN task t ON t.id=c.task_id JOIN check_result result ON result.id=c.result_id WHERE c.result_id IS NOT NULL AND c.delivery_step_id IS NULL AND c.cancelled_at IS NULL AND (result.outcome<>'infrastructure_failed' OR c.infrastructure_retries>=2) AND r.state IN ('succeeded','failed','cancelled') ORDER BY c.created_at,c.id LIMIT ?")
            .bind(limit).fetch_all(&mut *tx).await?;
        let count = rows.len() as u64;
        for row in rows {
            let status: String = row.try_get("task_status")?;
            let version: i64 = row.try_get("task_version")?;
            let spec_digest: String = row.try_get("spec_digest")?;
            let commit_sha: String = row.try_get("commit_sha")?;
            let consumer = map_consumer(row)?;
            let payload = CheckResultDelivery {
                consumer_id: consumer.id.clone(),
                result_id: consumer.result_id.ok_or(DbError::NotFound)?,
                run_id: consumer.run_id.ok_or(DbError::NotFound)?,
                identity_key: consumer.identity_key,
                spec_digest,
                commit_sha,
                request_key: consumer.request_key,
                origin: consumer.origin,
            };
            let step = EnqueueTaskStep {
                id: new_uuid_v4(),
                task_id: consumer.task_id.ok_or(DbError::NotFound)?,
                kind: "command".into(),
                payload_json: json(
                    &serde_json::json!({"operation":"apply_check_result","arguments":payload,"preempt":false}),
                    CHECK_INPUT_BYTES,
                )?,
                causation_step_id: None,
                causation_key: format!("check-result:{}", consumer.id),
                chain_id: format!("check-result:{}", consumer.id),
                chain_position: 1,
                expected_status: status,
                expected_version: version,
                expected_epoch: Some(consumer.status_epoch),
                lane: "fast".into(),
                available_at: now_rfc3339(),
            };
            let id = self.enqueue_step_in_tx(&mut tx, &step).await?;
            // Commands are claimed before their handler checks its authority.
            // Supersede an already stale delivery here too, so no unsupported
            // or future consumer handler can accidentally apply an old epoch.
            let now = now_rfc3339();
            sqlx::query("UPDATE task_step SET status='superseded',last_error='stale check consumer epoch',updated_at=?,completed_at=? WHERE id=? AND status='pending' AND NOT EXISTS(SELECT 1 FROM task WHERE id=? AND status_epoch=? AND deleted_at IS NULL)")
                .bind(&now).bind(&now).bind(&id).bind(&step.task_id).bind(consumer.status_epoch).execute(&mut *tx).await?;
            sqlx::query("UPDATE check_consumer SET delivery_step_id=? WHERE id=? AND delivery_step_id IS NULL")
                .bind(id).bind(&consumer.id).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        if count != 0 {
            self.domain_event_notify().notify_waiters();
        }
        Ok(count)
    }
}
