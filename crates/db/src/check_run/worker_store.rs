//! Leased orchestration storage; this port cannot mutate a Task or Review.
use super::*;
use api_types::{CheckOwnerIdentity, CheckReceipt, WorkspaceHandleReference};

pub const CHECK_AUTOMATIC_RETRIES: i64 = 2;
/// A refused or unanswered owner acknowledgement is retried no sooner.
pub const CHECK_ACK_RETRY_SECONDS: i64 = 60;
/// One hour past the owner's own 24-hour retention of a check entry.
pub const CHECK_ACK_GIVE_UP_SECONDS: i64 = 25 * 3600;
/// An uncertain run the owner still reports as running is looked up again
/// no sooner; the lease is kept so no other worker re-claims it meanwhile.
pub const CHECK_RECONCILE_BACKOFF_SECONDS: i64 = 15;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "owner", rename_all = "snake_case")]
pub enum CheckDispatchTarget {
    Server {
        path: String,
        workspace_id: String,
        placement_id: String,
        generation: i64,
    },
    Daemon {
        workspace: WorkspaceHandleReference,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckDispatchIntent {
    pub target: CheckDispatchTarget,
    pub owner: CheckOwnerIdentity,
    /// The requesting Task whose Project environment is resolved transiently.
    pub environment_task_id: String,
}
#[derive(Debug, Clone)]
pub struct CheckWorkerRecord {
    pub run: StoredCheckRun,
    pub dispatch: Option<CheckDispatchIntent>,
    pub receipt: Option<CheckReceipt>,
    pub admitted_at: Option<String>,
    pub deadline_at: Option<String>,
    pub infrastructure_attempt: i64,
    pub acknowledged_at: Option<String>,
}
fn map_worker(row: SqliteRow) -> Result<CheckWorkerRecord> {
    let dispatch = row
        .try_get::<Option<String>, _>("dispatch_json")?
        .map(|s| parse(&s))
        .transpose()?;
    let receipt = row
        .try_get::<Option<String>, _>("owner_receipt_json")?
        .map(|s| parse(&s))
        .transpose()?;
    Ok(CheckWorkerRecord {
        dispatch,
        receipt,
        admitted_at: row.try_get("admitted_at")?,
        deadline_at: row.try_get("deadline_at")?,
        infrastructure_attempt: row.try_get("infrastructure_attempt")?,
        acknowledged_at: row.try_get("acknowledged_at")?,
        run: map_run(row)?,
    })
}

#[derive(Debug)]
pub enum CheckAdmission {
    Admitted(Box<StoredCheckRun>),
    Waiting,
}

#[async_trait]
pub trait CheckWorkerRepo: CheckRunRepo + CheckDeliveryRepo {
    async fn expired_queued_checks(&self, now: &str, limit: i64) -> Result<Vec<StoredCheckRun>>;
    async fn defer_check_reconciliation(&self, fence: &CheckRunFence, now: &str) -> Result<()>;
    async fn admit_check_run(
        &self,
        run: &StoredCheckRun,
        owner: &str,
        now: &str,
        until: &str,
    ) -> Result<CheckAdmission>;
    async fn runnable_check_runs(&self, now: &str, limit: i64) -> Result<Vec<StoredCheckRun>>;
    async fn check_worker_record(&self, id: &str) -> Result<CheckWorkerRecord>;
    /// Drops stale consumers in check storage, without changing their Tasks.
    async fn active_check_consumers(&self, id: &str, now: &str) -> Result<Vec<CheckConsumer>>;
    /// Persists the pinned owner, operation identity and deadline before dispatch.
    async fn record_check_dispatch(
        &self,
        fence: &CheckRunFence,
        intent: &CheckDispatchIntent,
        now: &str,
        deadline: &str,
    ) -> Result<()>;
    /// An operation-keyed, immutable owner receipt can outlive the worker lease.
    async fn record_check_owner_receipt(
        &self,
        id: &str,
        operation_id: &str,
        receipt: &CheckReceipt,
    ) -> Result<()>;
    /// Dispatched terminal runs whose owner entry is not yet acknowledged and
    /// whose last attempt is older than [`CHECK_ACK_RETRY_SECONDS`].
    async fn unacknowledged_checks(&self, now: &str, limit: i64) -> Result<Vec<CheckWorkerRecord>>;
    async fn acknowledge_check(&self, id: &str, operation_id: &str, now: &str) -> Result<()>;
    async fn defer_check_ack(&self, id: &str, operation_id: &str, now: &str) -> Result<()>;
    /// Only infrastructure outcomes get automatic retries, after reconciliation
    /// proved the previous operation stopped. The original consumers are moved
    /// atomically; the failed attempt/result stay append-only history.
    async fn retry_infrastructure_check(
        &self,
        id: &str,
        now: &str,
    ) -> Result<Option<StoredCheckRun>>;
    async fn retryable_check_runs(&self, limit: i64) -> Result<Vec<String>>;
    /// The run's consumers get its next infrastructure failure as their
    /// answer: no automatic retry may follow an operation whose owner never
    /// confirmed it stopped.
    async fn exhaust_check_retries(&self, id: &str) -> Result<()>;
}
#[async_trait]
impl CheckWorkerRepo for SqliteDb {
    async fn expired_queued_checks(&self, now: &str, limit: i64) -> Result<Vec<StoredCheckRun>> {
        sqlx::query("SELECT * FROM check_run WHERE state='queued' AND julianday(created_at)<=julianday(?)-1800.0/86400 ORDER BY created_at,id LIMIT ?")
            .bind(now).bind(limit.clamp(1,1000)).fetch_all(self.pool()).await?.into_iter().map(map_run).collect()
    }
    async fn defer_check_reconciliation(&self, fence: &CheckRunFence, now: &str) -> Result<()> {
        // The lease CHECK ties owner and expiry together, so the backoff is a
        // short lease: nobody claims the run until it expires, then any
        // worker (this one included) takes it over and looks it up again.
        let until = (chrono::DateTime::parse_from_rfc3339(now)
            .map_err(|e| DbError::Check(e.to_string()))?
            + chrono::Duration::seconds(CHECK_RECONCILE_BACKOFF_SECONDS))
        .to_rfc3339();
        let changed=sqlx::query("UPDATE check_run SET lease_until=?,version=version+1,updated_at=? WHERE id=? AND version=? AND lease_generation=? AND lease_owner=? AND state='uncertain' AND julianday(lease_until)>julianday(?)")
            .bind(until).bind(now).bind(&fence.run_id).bind(fence.version).bind(fence.lease_generation).bind(&fence.lease_owner).bind(now).execute(self.pool()).await?;
        if changed.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        Ok(())
    }
    async fn admit_check_run(
        &self,
        run: &StoredCheckRun,
        owner: &str,
        now: &str,
        until: &str,
    ) -> Result<CheckAdmission> {
        validate_times(now, until)?;
        if run.state != CheckRunState::Queued {
            return self
                .claim_check_run(&run.id, run.version, owner, now, until)
                .await
                .map(|run| CheckAdmission::Admitted(Box::new(run)));
        }
        let mut tx = begin_immediate(self.pool()).await?;
        let current = map_run(
            sqlx::query("SELECT * FROM check_run WHERE id=?")
                .bind(&run.id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(DbError::NotFound)?,
        )?;
        if current.version != run.version || current.state != CheckRunState::Queued {
            return Err(DbError::VersionConflict);
        }
        // Whether this run already told its consumers it waits for a slot.
        let waited: bool =
            sqlx::query_scalar("SELECT capacity_wait_since IS NOT NULL FROM check_run WHERE id=?")
                .bind(&run.id)
                .fetch_one(&mut *tx)
                .await?;
        let embedded = self.server_run_cap.embedded_machine_id();
        let (machine, cap, removed) = if let Some(id) = run.machine_id.as_deref() {
            let row: Option<(String, Option<i64>, Option<u32>, Option<String>)> = sqlx::query_as(
                "SELECT machine_id,max_concurrent_runs,run_limit,removed_at FROM daemon WHERE id=?",
            )
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?;
            match row {
                Some((physical, reported, admin, removed)) if physical != embedded => (
                    Some(id),
                    crate::machine_capacity::effective_machine_cap(reported, admin),
                    removed.is_some(),
                ),
                Some((_, _, _, removed)) => (
                    None,
                    crate::machine_capacity::server_machine_cap(self, &mut tx).await?,
                    removed.is_some(),
                ),
                None => (Some(id), None, true),
            }
        } else {
            (
                None,
                crate::machine_capacity::server_machine_cap(self, &mut tx).await?,
                false,
            )
        };
        let clock =
            chrono::DateTime::parse_from_rfc3339(now).map_err(|e| DbError::Check(e.to_string()))?;
        let created = chrono::DateTime::parse_from_rfc3339(&run.created_at)
            .map_err(|e| DbError::Check(e.to_string()))?;
        let expired = (clock - created).num_seconds() >= 1800
            || !matches!(run.applied_timeout_seconds, Some(1..=86_400));
        if !removed && !expired {
            let capacity =
                crate::machine_capacity::count_machine_capacity(&mut tx, machine, cap, &embedded)
                    .await?;
            // A slot of its own needs a free one that the checks queued ahead
            // of this run leave it, and room in the checks' share while a
            // Task waits for a run slot here. Borrowing needs none of it.
            let own_slot = capacity.has_capacity()
                && capacity.admits_check(
                    crate::machine_capacity::checks_queued_ahead(&mut tx, &run.id, &embedded)
                        .await?,
                    crate::machine_capacity::run_slot_waiters(&mut tx, machine, &embedded).await?,
                );
            if !own_slot
                && !crate::machine_capacity::check_borrows_machine_slot(
                    &mut tx, &run.id, machine, &embedded,
                )
                .await?
            {
                // The first refusal of this run starts its one slot wait: its
                // consumers' Tasks are told once, not on every sweep.
                sqlx::query("UPDATE check_run SET capacity_wait_since=COALESCE(capacity_wait_since,?),version=version+1,updated_at=? WHERE id=? AND version=? AND state='queued'").bind(now).bind(now).bind(&run.id).bind(run.version).execute(&mut *tx).await?;
                let told = if !waited {
                    enqueue_check_progress_in_tx(self, &mut tx, &run.id, CheckProgress::SlotWait)
                        .await?
                } else {
                    0
                };
                tx.commit().await?;
                if told != 0 {
                    self.domain_event_notify().notify_waiters();
                }
                return Ok(CheckAdmission::Waiting);
            }
        }
        let deadline = (clock
            + chrono::Duration::seconds(run.applied_timeout_seconds.unwrap_or(1800).min(86_400)))
        .to_rfc3339();
        let row=sqlx::query("UPDATE check_run SET state='running',lease_owner=?,lease_until=?,lease_generation=lease_generation+1,version=version+1,updated_at=?,admitted_at=?,deadline_at=?,capacity_wait_since=NULL WHERE id=? AND version=? AND state='queued' RETURNING *")
            .bind(owner).bind(until).bind(now).bind((!removed && !expired).then_some(now)).bind(deadline).bind(&run.id).bind(run.version).fetch_optional(&mut *tx).await?.ok_or(DbError::VersionConflict)?;
        // A run that waited for its slot tells its consumers the wait ended.
        let run = map_run(row)?;
        let told = if waited && !removed && !expired {
            enqueue_check_progress_in_tx(self, &mut tx, &run.id, CheckProgress::Admitted).await?
        } else {
            0
        };
        tx.commit().await?;
        if told != 0 {
            self.domain_event_notify().notify_waiters();
        }
        Ok(CheckAdmission::Admitted(Box::new(run)))
    }
    async fn runnable_check_runs(&self, now: &str, limit: i64) -> Result<Vec<StoredCheckRun>> {
        sqlx::query("SELECT * FROM check_run WHERE state IN ('queued','running','cancelling','cleaning','uncertain') AND (lease_until IS NULL OR julianday(lease_until)<=julianday(?)) ORDER BY updated_at,created_at,id LIMIT ?")
            .bind(now).bind(limit.clamp(1,1000)).fetch_all(self.pool()).await?.into_iter().map(map_run).collect()
    }
    async fn check_worker_record(&self, id: &str) -> Result<CheckWorkerRecord> {
        map_worker(
            sqlx::query("SELECT * FROM check_run WHERE id=?")
                .bind(id)
                .fetch_optional(self.pool())
                .await?
                .ok_or(DbError::NotFound)?,
        )
    }
    async fn active_check_consumers(&self, id: &str, now: &str) -> Result<Vec<CheckConsumer>> {
        let mut tx = begin_immediate(self.pool()).await?;
        sqlx::query("UPDATE check_consumer SET cancelled_at=? WHERE run_id=? AND cancelled_at IS NULL AND NOT EXISTS(SELECT 1 FROM task t WHERE t.id=check_consumer.task_id AND t.status_epoch=check_consumer.status_epoch AND t.deleted_at IS NULL)")
            .bind(now).bind(id).execute(&mut *tx).await?;
        let rows = sqlx::query("SELECT * FROM check_consumer WHERE run_id=? AND cancelled_at IS NULL ORDER BY created_at,id").bind(id).fetch_all(&mut *tx).await?;
        tx.commit().await?;
        rows.into_iter().map(map_consumer).collect()
    }
    async fn record_check_dispatch(
        &self,
        fence: &CheckRunFence,
        intent: &CheckDispatchIntent,
        now: &str,
        deadline: &str,
    ) -> Result<()> {
        validate_times(now, deadline)?;
        let intent = json(intent, CHECK_INPUT_BYTES)?;
        let changed = sqlx::query("UPDATE check_run SET dispatch_json=?,admitted_at=COALESCE(admitted_at,?),deadline_at=COALESCE(deadline_at,?) WHERE id=? AND version=? AND lease_generation=? AND lease_owner=? AND julianday(lease_until)>julianday(?) AND state='running' AND dispatch_json IS NULL")
            .bind(intent).bind(now).bind(deadline).bind(&fence.run_id).bind(fence.version).bind(fence.lease_generation).bind(&fence.lease_owner).bind(now).execute(self.pool()).await?;
        if changed.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        Ok(())
    }
    async fn record_check_owner_receipt(
        &self,
        id: &str,
        operation_id: &str,
        receipt: &CheckReceipt,
    ) -> Result<()> {
        if receipt.operation_id != operation_id {
            return Err(DbError::Check("foreign owner receipt".into()));
        }
        let text = json(receipt, 4_194_304)?;
        let mut tx = begin_immediate(self.pool()).await?;
        let stored: (String, Option<String>, Option<String>) = sqlx::query_as(
            "SELECT operation_id,dispatch_json,owner_receipt_json FROM check_run WHERE id=?",
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
        if stored.0 != operation_id || stored.1.is_none() {
            return Err(DbError::Check(
                "receipt has no matching dispatch intent".into(),
            ));
        }
        if let Some(previous) = stored.2 {
            if previous != text {
                return Err(DbError::IdempotencyConflict);
            }
        } else {
            sqlx::query("UPDATE check_run SET owner_receipt_json=? WHERE id=? AND operation_id=? AND owner_receipt_json IS NULL").bind(text).bind(id).bind(operation_id).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }
    async fn unacknowledged_checks(&self, now: &str, limit: i64) -> Result<Vec<CheckWorkerRecord>> {
        // `acknowledge_attempted_at` is NULL until the first attempt, so a
        // fresh result is acknowledged at once and a refused one backs off.
        sqlx::query("SELECT * FROM check_run WHERE dispatch_json IS NOT NULL AND acknowledged_at IS NULL AND finished_at IS NOT NULL AND (acknowledge_attempted_at IS NULL OR julianday(acknowledge_attempted_at)<=julianday(?)-?/86400.0) ORDER BY updated_at LIMIT ?")
            .bind(now).bind(CHECK_ACK_RETRY_SECONDS).bind(limit.clamp(1,1000)).fetch_all(self.pool()).await?.into_iter().map(map_worker).collect()
    }
    async fn acknowledge_check(&self, id: &str, operation_id: &str, now: &str) -> Result<()> {
        sqlx::query("UPDATE check_run SET acknowledged_at=? WHERE id=? AND operation_id=? AND state IN ('succeeded','failed','cancelled') AND acknowledged_at IS NULL").bind(now).bind(id).bind(operation_id).execute(self.pool()).await?;
        Ok(())
    }
    async fn defer_check_ack(&self, id: &str, operation_id: &str, now: &str) -> Result<()> {
        // The owner keeps a check entry for 24 hours at most. Past that there
        // is nothing left to acknowledge, so the retry ends.
        sqlx::query("UPDATE check_run SET acknowledge_attempted_at=?,updated_at=?,acknowledged_at=CASE WHEN julianday(finished_at)<=julianday(?)-?/86400.0 THEN ? END WHERE id=? AND operation_id=? AND state IN ('succeeded','failed','cancelled') AND acknowledged_at IS NULL")
            .bind(now).bind(now).bind(now).bind(CHECK_ACK_GIVE_UP_SECONDS).bind(now).bind(id).bind(operation_id).execute(self.pool()).await?;
        Ok(())
    }
    async fn retry_infrastructure_check(
        &self,
        id: &str,
        now: &str,
    ) -> Result<Option<StoredCheckRun>> {
        let mut tx = begin_immediate(self.pool()).await?;
        let record = map_worker(
            sqlx::query("SELECT * FROM check_run WHERE id=?")
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(DbError::NotFound)?,
        )?;
        if record.run.state != CheckRunState::Failed {
            return Ok(None);
        }
        let infrastructure: bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM check_result WHERE run_id=? AND outcome='infrastructure_failed' AND cleanup<>'uncertain')").bind(id).fetch_one(&mut *tx).await?;
        if !infrastructure {
            return Ok(None);
        }
        let retryable: bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM check_consumer WHERE run_id=? AND cancelled_at IS NULL AND delivery_step_id IS NULL AND infrastructure_retries<2)").bind(id).fetch_one(&mut *tx).await?;
        if !retryable {
            return Ok(None);
        }
        // A different consumer may have certified this key between the old
        // failure and recovery. Semantic recovery reuses that evidence; it
        // must never schedule a second run of the certified key.
        if record.run.cacheable {
            if let Some(row) = sqlx::query(REUSABLE)
                .bind(&record.run.identity_key)
                .fetch_optional(&mut *tx)
                .await?
            {
                let result = map_result(row)?;
                sqlx::query("UPDATE check_consumer SET run_id=?,result_id=? WHERE run_id=? AND cancelled_at IS NULL AND delivery_step_id IS NULL AND infrastructure_retries<2")
                    .bind(&result.run_id).bind(&result.id).bind(id).execute(&mut *tx).await?;
                let run = map_run(
                    sqlx::query("SELECT * FROM check_run WHERE id=?")
                        .bind(&result.run_id)
                        .fetch_one(&mut *tx)
                        .await?,
                )?;
                tx.commit().await?;
                return Ok(Some(run));
            }
        }
        // Another request may have scheduled a successor after settlement.
        // Join it rather than fighting the partial unique identity index.
        let next = if let Some(row) = sqlx::query(LIVE)
            .bind(&record.run.identity_key)
            .fetch_optional(&mut *tx)
            .await?
        {
            map_run(row)?
        } else {
            let next_id = new_uuid_v4();
            sqlx::query("INSERT INTO check_run(id,project_id,repo_id,commit_sha,spec_digest,identity_key,input_json,cacheable,state,operation_id,workspace_id,machine_id,applied_timeout_seconds,infrastructure_attempt,created_at,updated_at) SELECT ?,project_id,repo_id,commit_sha,spec_digest,identity_key,input_json,cacheable,'queued',?,workspace_id,machine_id,applied_timeout_seconds,?, ?,? FROM check_run WHERE id=?")
                .bind(&next_id).bind(new_uuid_v4()).bind((record.infrastructure_attempt+1).min(CHECK_AUTOMATIC_RETRIES)).bind(now).bind(now).bind(id).execute(&mut *tx).await?;
            map_run(
                sqlx::query("SELECT * FROM check_run WHERE id=?")
                    .bind(next_id)
                    .fetch_one(&mut *tx)
                    .await?,
            )?
        };
        sqlx::query("UPDATE check_consumer SET run_id=?,result_id=NULL,infrastructure_retries=infrastructure_retries+1 WHERE run_id=? AND cancelled_at IS NULL AND delivery_step_id IS NULL AND infrastructure_retries<2")
            .bind(&next.id).bind(id).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(Some(next))
    }
    async fn exhaust_check_retries(&self, id: &str) -> Result<()> {
        sqlx::query("UPDATE check_consumer SET infrastructure_retries=? WHERE run_id=? AND cancelled_at IS NULL AND delivery_step_id IS NULL")
            .bind(CHECK_AUTOMATIC_RETRIES).bind(id).execute(self.pool()).await?;
        Ok(())
    }
    async fn retryable_check_runs(&self, limit: i64) -> Result<Vec<String>> {
        // Driven from the undelivered-consumer partial index, never from the
        // history of failed runs.
        Ok(sqlx::query_scalar("SELECT DISTINCT c.run_id FROM check_consumer c JOIN check_result result ON result.id=c.result_id JOIN check_run r ON r.id=c.run_id WHERE c.result_id IS NOT NULL AND c.delivery_step_id IS NULL AND c.cancelled_at IS NULL AND c.infrastructure_retries<2 AND result.outcome='infrastructure_failed' AND result.cleanup<>'uncertain' AND r.state='failed' LIMIT ?")
            .bind(limit.clamp(1,1000)).fetch_all(self.pool()).await?)
    }
}
