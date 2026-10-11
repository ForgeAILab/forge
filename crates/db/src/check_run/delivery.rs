//! Check-result outbox. Only check rows and enqueue-only Task-step writes.
use super::*;
use crate::{EnqueueTaskStep, TaskStepRepo};
use sqlx::{Sqlite, Transaction};

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

/// What the worker tells a waiting consumer's Task between request and
/// result. Information only: the consuming step restates its own condition.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CheckProgress {
    /// The checkout's machine has no free run slot.
    SlotWait,
    /// The slot freed and the run was admitted.
    Admitted,
}
/// One progress note for one consumer of one run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckProgressDelivery {
    pub consumer_id: String,
    pub run_id: String,
    pub request_key: String,
    pub origin: CheckConsumerOrigin,
    pub progress: CheckProgress,
}

/// Enqueue one `apply_check_progress` step per live consumer of `run_id`, in
/// the caller's transaction. Idempotent per (consumer, run, progress): a run
/// waits for a slot at most once, so each consumer hears of it once.
pub(super) async fn enqueue_check_progress_in_tx(
    db: &SqliteDb,
    tx: &mut Transaction<'_, Sqlite>,
    run_id: &str,
    progress: CheckProgress,
) -> Result<u64> {
    let rows = sqlx::query("SELECT c.*, t.status AS task_status, t.version AS task_version FROM check_consumer c JOIN task t ON t.id=c.task_id AND t.status_epoch=c.status_epoch AND t.deleted_at IS NULL WHERE c.run_id=? AND c.cancelled_at IS NULL AND c.delivery_step_id IS NULL ORDER BY c.created_at,c.id")
        .bind(run_id).fetch_all(&mut **tx).await?;
    let count = rows.len() as u64;
    for row in rows {
        let status: String = row.try_get("task_status")?;
        let version: i64 = row.try_get("task_version")?;
        let consumer = map_consumer(row)?;
        let name = match progress {
            CheckProgress::SlotWait => "check-slot-wait",
            CheckProgress::Admitted => "check-slot-freed",
        };
        let key = format!("{name}:{}:{run_id}", consumer.id);
        let payload = CheckProgressDelivery {
            consumer_id: consumer.id.clone(),
            run_id: run_id.to_owned(),
            request_key: consumer.request_key,
            origin: consumer.origin,
            progress,
        };
        let step = EnqueueTaskStep {
            id: new_uuid_v4(),
            task_id: consumer.task_id.ok_or(DbError::NotFound)?,
            kind: "command".into(),
            payload_json: json(
                &serde_json::json!({"operation":"apply_check_progress","arguments":payload,"preempt":false}),
                CHECK_INPUT_BYTES,
            )?,
            causation_step_id: None,
            causation_key: key.clone(),
            chain_id: key,
            chain_position: 1,
            expected_status: status,
            expected_version: version,
            expected_epoch: Some(consumer.status_epoch),
            lane: "fast".into(),
            available_at: now_rfc3339(),
        };
        db.enqueue_step_in_tx(tx, &step).await?;
    }
    Ok(count)
}

/// A consumer as its delivery step must see it before applying.
#[derive(Debug, Clone)]
pub struct CheckConsumerDelivery {
    pub consumer: CheckConsumer,
    pub delivery_step_id: Option<String>,
    pub cancelled_at: Option<String>,
    pub applied_at: Option<String>,
    /// The consumer's Task's current status entry; None when it is gone.
    pub live_task_epoch: Option<i64>,
}

#[async_trait]
pub trait CheckDeliveryRepo: Send + Sync {
    async fn live_task_epoch(&self, task_id: &str) -> Result<Option<i64>>;
    async fn check_consumer_delivery(
        &self,
        consumer_id: &str,
    ) -> Result<Option<CheckConsumerDelivery>>;
    /// Only the recorded delivery step may mark its consumer applied, once.
    async fn mark_check_result_applied(
        &self,
        consumer_id: &str,
        step_id: &str,
        now: &str,
    ) -> Result<bool>;
    /// The durable marker and enqueue share a transaction. A crash on either
    /// side of this call cannot lose a consumer or enqueue it twice.
    async fn enqueue_check_result_steps(&self, limit: i64) -> Result<u64>;
    /// The owner's `retry` of a consumer whose delivery said "no verdict
    /// after the automatic retries": the same consumer asks again with a
    /// fresh infrastructure budget. Its exhausted answer must have been
    /// applied, its Task must still be in the status entry that asked, and a
    /// result whose cleanup is unproven is never retried from here. `false`
    /// when the consumer is not in that state (nothing changes).
    async fn rearm_exhausted_check_consumer(&self, consumer_id: &str) -> Result<bool>;
    /// Whether the run is queued behind a full machine right now.
    async fn check_run_waits_for_slot(&self, run_id: &str) -> Result<bool>;
    /// State (or clear) the Task's check wait under its claimed step. The
    /// only Task write a check consumer makes outside its family.
    async fn state_check_condition(
        &self,
        task_id: &str,
        statement: &crate::ConditionStatement,
    ) -> Result<()>;
}
#[async_trait]
impl CheckDeliveryRepo for SqliteDb {
    async fn live_task_epoch(&self, task_id: &str) -> Result<Option<i64>> {
        Ok(
            sqlx::query_scalar("SELECT status_epoch FROM task WHERE id=? AND deleted_at IS NULL")
                .bind(task_id)
                .fetch_optional(self.pool())
                .await?,
        )
    }
    async fn check_consumer_delivery(
        &self,
        consumer_id: &str,
    ) -> Result<Option<CheckConsumerDelivery>> {
        let Some(row) = sqlx::query("SELECT c.*,(SELECT t.status_epoch FROM task t WHERE t.id=c.task_id AND t.deleted_at IS NULL) AS live_task_epoch FROM check_consumer c WHERE c.id=?")
            .bind(consumer_id)
            .fetch_optional(self.pool())
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(CheckConsumerDelivery {
            delivery_step_id: row.try_get("delivery_step_id")?,
            cancelled_at: row.try_get("cancelled_at")?,
            applied_at: row.try_get("applied_at")?,
            live_task_epoch: row.try_get("live_task_epoch")?,
            consumer: map_consumer(row)?,
        }))
    }
    async fn mark_check_result_applied(
        &self,
        consumer_id: &str,
        step_id: &str,
        now: &str,
    ) -> Result<bool> {
        Ok(sqlx::query("UPDATE check_consumer SET applied_at=? WHERE id=? AND delivery_step_id=? AND applied_at IS NULL")
            .bind(now)
            .bind(consumer_id)
            .bind(step_id)
            .execute(self.pool())
            .await?
            .rows_affected()
            == 1)
    }
    async fn check_run_waits_for_slot(&self, run_id: &str) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM check_run WHERE id=? AND state='queued' AND capacity_wait_since IS NOT NULL)")
            .bind(run_id)
            .fetch_one(self.pool())
            .await?)
    }
    async fn state_check_condition(
        &self,
        task_id: &str,
        statement: &crate::ConditionStatement,
    ) -> Result<()> {
        if !matches!(
            statement,
            crate::ConditionStatement::Check { .. }
                | crate::ConditionStatement::CheckCleared { .. }
        ) {
            return Err(DbError::Check("expected a check statement".into()));
        }
        let mut tx = begin_immediate(self.pool()).await?;
        self.state_integration_condition_in_tx(&mut tx, task_id, statement)
            .await?;
        tx.commit().await?;
        Ok(())
    }
    async fn rearm_exhausted_check_consumer(&self, consumer_id: &str) -> Result<bool> {
        // The sweep's infrastructure retry takes it from here: it moves the
        // consumer to a fresh run (or a live or certified one of the same
        // identity) exactly as it does for an automatic retry.
        let changed = sqlx::query("UPDATE check_consumer SET infrastructure_retries=0,delivery_step_id=NULL,applied_at=NULL WHERE id=? AND cancelled_at IS NULL AND applied_at IS NOT NULL AND delivery_step_id IS NOT NULL AND EXISTS(SELECT 1 FROM check_result result WHERE result.id=check_consumer.result_id AND result.outcome='infrastructure_failed' AND result.cleanup<>'uncertain') AND EXISTS(SELECT 1 FROM check_run r WHERE r.id=check_consumer.run_id AND r.state='failed') AND EXISTS(SELECT 1 FROM task t WHERE t.id=check_consumer.task_id AND t.status_epoch=check_consumer.status_epoch AND t.deleted_at IS NULL)")
            .bind(consumer_id)
            .execute(self.pool())
            .await?
            .rows_affected();
        if changed == 1 {
            self.domain_event_notify().notify_waiters();
        }
        Ok(changed == 1)
    }
    async fn enqueue_check_result_steps(&self, limit: i64) -> Result<u64> {
        if !(1..=1000).contains(&limit) {
            return Err(DbError::Check("invalid check delivery batch limit".into()));
        }
        // An idle sweep must not take the write lock: probe the partial
        // index first and open the transaction only when there is work.
        let pending: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM check_consumer c WHERE c.result_id IS NOT NULL AND c.delivery_step_id IS NULL AND c.cancelled_at IS NULL)")
            .fetch_one(self.pool())
            .await?;
        if !pending {
            return Ok(0);
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
            let result_id = consumer.result_id.ok_or(DbError::NotFound)?;
            // One delivery per (consumer, result): a consumer re-armed by the
            // owner's retry is answered again, by its next result.
            let key = format!("check-result:{}:{result_id}", consumer.id);
            let payload = CheckResultDelivery {
                consumer_id: consumer.id.clone(),
                result_id,
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
                causation_key: key.clone(),
                chain_id: key,
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
