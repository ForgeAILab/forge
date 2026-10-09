//! A check-table writer with an enqueue-only outbox. No domain projection.
use super::*;
use api_types::{CheckReceipt, DaemonCheckResult};
use async_trait::async_trait;
use db::{
    CheckAdmission, CheckCleanup, CheckDispatchIntent, CheckResultEvidence, CheckResultOutcome,
    CheckRunFence, CheckRunState, CheckWorkerRecord, CheckWorkerRepo,
};
use std::time::Duration;
use tokio::{
    sync::watch,
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;

pub const CHECK_LEASE_SECONDS: i64 = 60;
pub const CHECK_SWEEP_INTERVAL: Duration = Duration::from_secs(1);
pub const CHECK_SLOT_WAIT_SECONDS: i64 = 1800;
const OWNER_RPC_BOUND: Duration = Duration::from_secs(30);
const SETTLEMENT_GRACE_SECONDS: i64 = 65;

/// This effect port supplies owner-local evidence, never Task authority.
#[async_trait]
pub trait CheckOwnerPort: Send + Sync {
    async fn prepare(&self, run: &StoredCheckRun) -> Result<CheckDispatchIntent>;
    async fn run(
        &self,
        record: &CheckWorkerRecord,
        cancel: &CancellationToken,
    ) -> Result<DaemonCheckResult>;
    async fn lookup(&self, record: &CheckWorkerRecord) -> Result<DaemonCheckResult>;
    async fn cancel(&self, record: &CheckWorkerRecord) -> Result<DaemonCheckResult>;
    /// True only after machine removal or the existing disconnected-owner bound.
    async fn owner_gone(&self, record: &CheckWorkerRecord) -> Result<bool>;
    async fn acknowledge(&self, record: &CheckWorkerRecord) -> Result<()>;
}

pub struct CheckRunWorker {
    store: Arc<dyn CheckWorkerRepo>,
    owners: Arc<dyn CheckOwnerPort>,
    instance: String,
}
impl CheckRunWorker {
    pub fn new(store: Arc<dyn CheckWorkerRepo>, owners: Arc<dyn CheckOwnerPort>) -> Self {
        Self {
            store,
            owners,
            instance: format!("check-worker:{}", db::new_uuid_v4()),
        }
    }
    /// Shared supervision owns restart/health; this worker owns its leases and
    /// bounded effects. Startup and reconnect need no separate execution path:
    /// the immediate scan and one-second sweep reconcile every expired lease.
    pub fn start(
        self: Arc<Self>,
        workers: &crate::worker_runtime::PeriodicWorkers,
        shutdown: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        workers.worker("check-runs").start(shutdown, || false, move |worker, mut shutdown| {
            let this=self.clone();
            async move {
                let mut jobs=JoinSet::new();
                let mut interval=tokio::time::interval(CHECK_SWEEP_INTERVAL);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _=async { if !*shutdown.borrow_and_update() { while shutdown.changed().await.is_ok() && !*shutdown.borrow_and_update() {} } }=> { jobs.abort_all(); while jobs.join_next().await.is_some() {} return Ok(()); },
                        Some(result)=jobs.join_next(), if !jobs.is_empty()=> { if let Err(error)=result { tracing::warn!(%error,"check worker job stopped; lease recovery will reconcile"); } },
                        _=interval.tick()=> {
                            worker.tick(this.sweep(&mut jobs)).await?;
                        }
                    }
                }
            }
        })
    }
    pub async fn sweep(self: &Arc<Self>, jobs: &mut JoinSet<Result<()>>) -> Result<()> {
        // Expiry settlement needs no effect slot and must progress even when
        // all admitted owner jobs are busy. Queue time never becomes wall time.
        let now = db::now_rfc3339();
        for run in self.store.expired_queued_checks(&now, 32).await? {
            match self
                .store
                .admit_check_run(&run, &self.instance, &now, &lease_until())
                .await
            {
                Ok(CheckAdmission::Admitted(run)) => self.drive(*run).await?,
                Ok(CheckAdmission::Waiting) | Err(db::DbError::VersionConflict) => {}
                Err(error) => return Err(error.into()),
            }
        }
        // Resume the result->retry boundary before exposing terminal evidence.
        for id in self.store.retryable_check_runs(100).await? {
            self.store
                .retry_infrastructure_check(&id, &db::now_rfc3339())
                .await?;
        }
        self.store.enqueue_check_result_steps(100).await?;
        // A disconnected machine's ACK must not serially delay every other
        // machine. Every retained result remains eligible on later sweeps.
        let mut acknowledgments = JoinSet::new();
        for record in self.store.unacknowledged_checks(100).await? {
            let (store, owners) = (self.store.clone(), self.owners.clone());
            acknowledgments.spawn(async move {
                if tokio::time::timeout(OWNER_RPC_BOUND, owners.acknowledge(&record))
                    .await
                    .is_ok_and(|r| r.is_ok())
                {
                    store
                        .acknowledge_check(
                            &record.run.id,
                            &record.run.operation_id,
                            &db::now_rfc3339(),
                        )
                        .await?;
                } else {
                    store
                        .defer_check_ack(
                            &record.run.id,
                            &record.run.operation_id,
                            &db::now_rfc3339(),
                        )
                        .await?;
                }
                Ok::<_, ServiceError>(())
            });
        }
        while let Some(result) = acknowledgments.join_next().await {
            result.map_err(|_| {
                ServiceError::invalid_operation("check acknowledgment job stopped")
            })??;
        }
        let now = db::now_rfc3339();
        for run in self
            .store
            .runnable_check_runs(&now, 32_i64.saturating_sub(jobs.len() as i64))
            .await?
        {
            if jobs.len() >= 32 {
                break;
            }
            let until = lease_until();
            match self
                .store
                .admit_check_run(&run, &self.instance, &now, &until)
                .await
            {
                Ok(CheckAdmission::Admitted(run)) => {
                    let this = self.clone();
                    jobs.spawn(async move { this.drive(*run).await });
                }
                Ok(CheckAdmission::Waiting) | Err(db::DbError::VersionConflict) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
    pub async fn drive(&self, mut run: StoredCheckRun) -> Result<()> {
        let mut record = self.store.check_worker_record(&run.id).await?;
        if let Some(receipt) = record.receipt.clone() {
            return self.settle_receipt(run, &record, &receipt).await;
        }
        let consumers = self
            .store
            .active_check_consumers(&run.id, &db::now_rfc3339())
            .await?;
        if consumers.is_empty() && record.dispatch.is_none() {
            return self
                .settle_without_receipt(
                    run,
                    CheckResultOutcome::Cancelled,
                    CheckCleanup::NotPerformed,
                )
                .await;
        }
        if record.admitted_at.is_none() && record.dispatch.is_none() {
            return self
                .settle_without_receipt(
                    run,
                    CheckResultOutcome::InfrastructureFailed,
                    CheckCleanup::NotPerformed,
                )
                .await;
        }
        if record.dispatch.is_none() {
            // A taken-over intent-less run provably spawned nothing.
            if run.state == CheckRunState::Uncertain {
                return self
                    .settle_without_receipt(
                        run,
                        CheckResultOutcome::InfrastructureFailed,
                        CheckCleanup::NotPerformed,
                    )
                    .await;
            }
            let prepared = tokio::time::timeout(OWNER_RPC_BOUND, self.owners.prepare(&run)).await;
            let intent = match prepared {
                Ok(Ok(intent)) => intent,
                _ => {
                    return self
                        .settle_without_receipt(
                            run,
                            CheckResultOutcome::InfrastructureFailed,
                            CheckCleanup::NotPerformed,
                        )
                        .await
                }
            };
            let now = chrono::Utc::now();
            let timeout = run.applied_timeout_seconds.unwrap_or(1800).min(86_400);
            let deadline = record
                .deadline_at
                .clone()
                .unwrap_or_else(|| (now + chrono::Duration::seconds(timeout)).to_rfc3339());
            self.store
                .record_check_dispatch(&fence(&run), &intent, &now.to_rfc3339(), &deadline)
                .await?;
            record = self.store.check_worker_record(&run.id).await?;
        }
        let cancel = CancellationToken::new();
        let dispatch = run.state == CheckRunState::Running;
        // Dispatch exactly once. A lost reply changes to lookup, not run again.
        let response = if dispatch {
            let deadline = settlement_deadline(&record)?;
            let operation = self.owners.run(&record, &cancel);
            tokio::pin!(operation);
            let mut renew = tokio::time::interval(Duration::from_secs(15));
            renew.tick().await;
            loop {
                tokio::select! {
                    result=&mut operation=>break result,
                    _=tokio::time::sleep_until(deadline)=>break Err(ServiceError::invalid_operation("check dispatch reply uncertain")),
                    _=renew.tick()=> {
                        run=self.store.renew_check_run(&fence(&run),&db::now_rfc3339(),&lease_until()).await?;
                        if self.store.active_check_consumers(&run.id,&db::now_rfc3339()).await?.is_empty() { cancel.cancel(); break self.bounded_cancel(&mut run,&record).await; }
                    }
                }
            }
        } else {
            self.bounded_lookup(&mut run, &record).await
        };
        if let Ok(DaemonCheckResult::Completed { receipt }) = response {
            return self.settle_receipt(run, &record, &receipt).await;
        }
        if run.state != CheckRunState::Uncertain {
            run = self
                .store
                .mark_check_run_uncertain(&fence(&run), &db::now_rfc3339())
                .await?;
        }
        if self.owners.owner_gone(&record).await? {
            return self
                .settle_without_receipt(
                    run,
                    CheckResultOutcome::InfrastructureFailed,
                    CheckCleanup::Failed,
                )
                .await;
        }
        // Owner supervision already bounds the effect; request cancel when
        // its reply's settlement grace expires. Unknown/Interrupted from
        // that owner prove no operation remains and permit the retry.
        let expired = tokio::time::Instant::now() >= settlement_deadline(&record)?;
        let gone = self
            .store
            .active_check_consumers(&run.id, &db::now_rfc3339())
            .await?
            .is_empty();
        let reply = if expired || gone {
            cancel.cancel();
            self.bounded_cancel(&mut run, &record).await
        } else {
            self.bounded_lookup(&mut run, &record).await
        };
        match reply {
            Ok(DaemonCheckResult::Completed { receipt }) => {
                return self.settle_receipt(run, &record, &receipt).await
            }
            Ok(DaemonCheckResult::Interrupted { .. } | DaemonCheckResult::Unknown { .. }) => {
                // Unknown cannot fence a delayed run RPC. The owner must
                // first retain its persistent cancellation tombstone.
                match self.bounded_cancel(&mut run, &record).await {
                    Ok(DaemonCheckResult::Completed { receipt }) => {
                        return self.settle_receipt(run, &record, &receipt).await
                    }
                    Ok(
                        DaemonCheckResult::Interrupted { .. } | DaemonCheckResult::Unknown { .. },
                    ) => {
                        return self
                            .settle_without_receipt(
                                run,
                                if gone {
                                    CheckResultOutcome::Cancelled
                                } else {
                                    CheckResultOutcome::InfrastructureFailed
                                },
                                CheckCleanup::NotPerformed,
                            )
                            .await
                    }
                    _ => {}
                }
            }
            _ => {}
        }
        // A reachable owner must not report Running forever past the
        // effect's own bound. Keep its identity fenced until it confirms
        // stop or reaches the configured disconnected-owner timeout.
        // Retain machine occupancy and the single-flight identity, but
        // release this worker job while awaiting the next reconciliation.
        self.store
            .defer_check_reconciliation(&fence(&run), &db::now_rfc3339())
            .await?;
        Ok(())
    }
    async fn owner_call<F>(
        &self,
        run: &mut StoredCheckRun,
        operation: F,
        bound: Duration,
    ) -> Result<DaemonCheckResult>
    where
        F: std::future::Future<Output = Result<DaemonCheckResult>>,
    {
        tokio::pin!(operation);
        let timeout = tokio::time::sleep(bound);
        tokio::pin!(timeout);
        let mut renew = tokio::time::interval(Duration::from_secs(15));
        renew.tick().await;
        loop {
            tokio::select! {
                result=&mut operation=>return result,
                _=&mut timeout=>return Err(ServiceError::invalid_operation("check owner reply uncertain")),
                _=renew.tick()=> { *run=self.store.renew_check_run(&fence(run),&db::now_rfc3339(),&lease_until()).await?; }
            }
        }
    }
    async fn bounded_lookup(
        &self,
        run: &mut StoredCheckRun,
        record: &CheckWorkerRecord,
    ) -> Result<DaemonCheckResult> {
        self.owner_call(run, self.owners.lookup(record), OWNER_RPC_BOUND)
            .await
    }
    async fn bounded_cancel(
        &self,
        run: &mut StoredCheckRun,
        record: &CheckWorkerRecord,
    ) -> Result<DaemonCheckResult> {
        self.owner_call(run, self.owners.cancel(record), Duration::from_secs(61))
            .await
    }
    async fn settle_receipt(
        &self,
        mut run: StoredCheckRun,
        record: &CheckWorkerRecord,
        receipt: &CheckReceipt,
    ) -> Result<()> {
        let evidence = match super::receipt::validate_receipt(
            &run,
            record
                .dispatch
                .as_ref()
                .ok_or_else(|| ServiceError::invalid_operation("receipt without dispatch"))?,
            receipt,
        ) {
            Ok(evidence) => evidence,
            // A terminal owner reply proves the handler stopped, but malformed
            // evidence certifies nothing and is a typed infrastructure result.
            Err(_) => {
                return self
                    .settle_without_receipt(
                        run,
                        CheckResultOutcome::InfrastructureFailed,
                        CheckCleanup::Failed,
                    )
                    .await
            }
        };
        self.store
            .record_check_owner_receipt(&run.id, &run.operation_id, receipt)
            .await?;
        if run.state != CheckRunState::Cleaning {
            run = self
                .store
                .transition_check_run(&fence(&run), CheckRunState::Cleaning, &db::now_rfc3339())
                .await?;
        }
        self.store
            .finish_check_run(&fence(&run), evidence, &db::now_rfc3339())
            .await?;
        self.store
            .retry_infrastructure_check(&run.id, &db::now_rfc3339())
            .await?;
        self.store.enqueue_check_result_steps(100).await?;
        Ok(())
    }
    async fn settle_without_receipt(
        &self,
        mut run: StoredCheckRun,
        outcome: CheckResultOutcome,
        cleanup: CheckCleanup,
    ) -> Result<()> {
        if run.state != CheckRunState::Cleaning {
            run = self
                .store
                .transition_check_run(&fence(&run), CheckRunState::Cleaning, &db::now_rfc3339())
                .await?;
        }
        self.store
            .finish_check_run(
                &fence(&run),
                CheckResultEvidence {
                    outcome,
                    cleanup,
                    commands: vec![],
                    output_truncated: false,
                    redaction_values: vec![],
                },
                &db::now_rfc3339(),
            )
            .await?;
        self.store
            .retry_infrastructure_check(&run.id, &db::now_rfc3339())
            .await?;
        self.store.enqueue_check_result_steps(100).await?;
        Ok(())
    }
}
pub(super) fn fence(run: &StoredCheckRun) -> CheckRunFence {
    CheckRunFence {
        run_id: run.id.clone(),
        version: run.version,
        lease_generation: run.lease_generation,
        lease_owner: run.lease_owner.clone(),
    }
}
fn lease_until() -> String {
    (chrono::Utc::now() + chrono::Duration::seconds(CHECK_LEASE_SECONDS)).to_rfc3339()
}
fn settlement_deadline(record: &CheckWorkerRecord) -> Result<tokio::time::Instant> {
    let deadline = chrono::DateTime::parse_from_rfc3339(
        record
            .deadline_at
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("check dispatch has no deadline"))?,
    )
    .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
    let remaining = (deadline.with_timezone(&chrono::Utc)
        + chrono::Duration::seconds(SETTLEMENT_GRACE_SECONDS)
        - chrono::Utc::now())
    .to_std()
    .unwrap_or_default();
    Ok(tokio::time::Instant::now() + remaining)
}
