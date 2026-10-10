//! Integration queue worker (3.2 stage D, part 1d). NOT started by the
//! runtime until D2: nothing constructs it outside its tests.
//!
//! One supervised loop sweeps the queues; one driver per claimed queue walks
//! its head through the table in `table.rs`. The worker writes queue and
//! attempt rows only. Git runs behind the fenced owner; Task state is written
//! by Task steps it asks for through [`IntegrationStepPort`].
mod driver;
mod ports;
mod table;
#[cfg(test)]
mod tests;

pub use driver::{HeadDriver, ParkReason, Pass};
pub use ports::*;
pub use table::{head_row, CancelRule, HeadAction, HeadStateRow, HeadTimeout, HEAD_TABLE};

use crate::{Result, ServiceError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use db::{
    IntegrationActivationRepo, IntegrationAttempt, IntegrationAttemptState, IntegrationQueue,
    IntegrationQueueReopenWitness, IntegrationQueueRepo, IntegrationQueueState, SqliteDb,
};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{watch, Notify},
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct IntegrationWorkerConfig {
    pub lease: Duration,
    pub renew_every: Duration,
    pub sweep_interval: Duration,
    /// How often a waiting driver re-reads its attempt.
    pub poll: Duration,
    pub conflict_backoff: Duration,
    /// Concurrent heads (one per queue).
    pub max_heads: usize,
    pub validate_timeout: Duration,
    pub rebase_deadline: Duration,
    /// Check timeout plus slack; the runner bounds the run itself.
    pub check_wait: Duration,
    pub step_wait: Duration,
    pub permit_lifetime: Duration,
    pub ff_owner_bound: Duration,
    pub reconcile_interval: Duration,
    /// Longest gap between retries of a wait that has no end of its own.
    pub soon_retry_max: Duration,
    /// Automatic retries of an infrastructure park, then the owner decides.
    pub infra_retry: Vec<Duration>,
    pub external_move_allowance: usize,
    pub max_rounds: i64,
    pub transfer_cap_bytes: u64,
    pub page: u32,
    /// While a head waits for a check verdict, ask for it again this often.
    /// The ask joins the running check, or applies a verdict whose delivery
    /// to the Task failed, so a lost delivery is not waited out.
    pub check_reask: Duration,
    /// Times one session asks again for a deciding step (`request_check`,
    /// `settle`) that settled without answering, before it parks the head.
    pub step_reasks: u32,
    /// The Task-step side's timers (`IntegrationSteps::with_timers`): how
    /// long the protected `result` step sleeps before it looks for itself,
    pub result_wake: Duration,
    /// its poll while a fast-forward is in flight or unknown,
    pub result_poll: Duration,
    /// and how long a `result` that could not be applied waits. The worker
    /// re-arms it on every sweep while the attempt is `applied`.
    pub result_park: Duration,
}
impl Default for IntegrationWorkerConfig {
    fn default() -> Self {
        Self {
            lease: Duration::from_secs(60),
            renew_every: Duration::from_secs(15),
            sweep_interval: Duration::from_secs(30),
            poll: Duration::from_secs(1),
            conflict_backoff: Duration::from_millis(250),
            max_heads: 16,
            validate_timeout: Duration::from_secs(30),
            rebase_deadline: Duration::from_secs(120),
            check_wait: Duration::from_secs(1800 + 300),
            step_wait: Duration::from_secs(300),
            permit_lifetime: Duration::from_secs(120),
            ff_owner_bound: Duration::from_secs(30),
            reconcile_interval: Duration::from_secs(60),
            soon_retry_max: Duration::from_secs(600),
            infra_retry: vec![
                Duration::from_secs(30),
                Duration::from_secs(120),
                Duration::from_secs(600),
            ],
            external_move_allowance: 5,
            max_rounds: 9,
            transfer_cap_bytes: 256 * 1024 * 1024,
            page: 100,
            check_reask: Duration::from_secs(60),
            step_reasks: 2,
            result_wake: Duration::from_secs(150),
            result_poll: Duration::from_secs(15),
            result_park: Duration::from_secs(600),
        }
    }
}

/// Where a test may stop the driver, as if the process died there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashPoint {
    AfterStepEnqueue,
    BeforeEffect,
    AfterEffectReceipt,
    BeforeTransition,
    AfterTransition,
}
#[cfg(test)]
type FaultHook = Arc<dyn Fn(IntegrationAttemptState, CrashPoint) -> bool + Send + Sync>;

#[derive(Default)]
struct Shared {
    active: HashSet<String>,
    /// Rotation point: the queue claimed last, so no queue starves when more
    /// are claimable than `max_heads`.
    cursor: Option<String>,
    reconcile_after: HashMap<String, DateTime<Utc>>,
}

/// Bound of the worker's own journal on an attempt (`operation_receipts_json`).
pub(crate) const JOURNAL_MAX: usize = 64;
pub(crate) fn journal_push(attempt: &mut IntegrationAttempt, entry: serde_json::Value) {
    let mut journal = attempt
        .operation_receipts_json
        .as_array()
        .cloned()
        .unwrap_or_default();
    journal.push(entry);
    let excess = journal.len().saturating_sub(JOURNAL_MAX);
    journal.drain(..excess);
    attempt.operation_receipts_json = journal.into();
}
/// The Task of this waiting member was told its queue is suspended, at the
/// member's present `effect_seq`. Stored on the attempt, so a restart neither
/// tells it twice nor forgets to tell it the wait is over.
fn told_suspended(attempt: &IntegrationAttempt) -> bool {
    attempt
        .operation_receipts_json
        .as_array()
        .and_then(|journal| journal.last())
        .is_some_and(|entry| {
            entry["kind"] == "worker_told"
                && entry["effect_seq"].as_i64() == Some(attempt.effect_seq)
        })
}

pub struct IntegrationQueueWorker {
    pub(crate) db: Arc<SqliteDb>,
    pub(crate) owner: Arc<dyn IntegrationOwnerPort>,
    pub(crate) steps: Arc<dyn IntegrationStepPort>,
    pub(crate) facts: Arc<dyn IntegrationFactsPort>,
    pub(crate) transfer: Arc<dyn ObjectTransferPort>,
    pub(crate) locations: Option<Arc<dyn IntegrationLocationPort>>,
    pub(crate) clock: Arc<dyn WorkerClock>,
    pub(crate) config: IntegrationWorkerConfig,
    pub(crate) instance: String,
    wake: Notify,
    shared: Mutex<Shared>,
    #[cfg(test)]
    fault: Mutex<Option<FaultHook>>,
}

pub(crate) fn stamp(at: DateTime<Utc>) -> String {
    at.to_rfc3339()
}
pub(crate) fn later(at: DateTime<Utc>, by: Duration) -> DateTime<Utc> {
    at + chrono::Duration::from_std(by).unwrap_or_else(|_| chrono::Duration::days(365))
}
pub(crate) fn parse_time(value: Option<&str>) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value?)
        .ok()
        .map(|at| at.with_timezone(&Utc))
}
pub(crate) fn is_conflict(error: &ServiceError) -> bool {
    matches!(error, ServiceError::Db(db::DbError::VersionConflict))
}

impl IntegrationQueueWorker {
    pub fn new(
        db: Arc<SqliteDb>,
        owner: Arc<dyn IntegrationOwnerPort>,
        steps: Arc<dyn IntegrationStepPort>,
        facts: Arc<dyn IntegrationFactsPort>,
        transfer: Arc<dyn ObjectTransferPort>,
        clock: Arc<dyn WorkerClock>,
        config: IntegrationWorkerConfig,
    ) -> Self {
        Self {
            db,
            owner,
            steps,
            facts,
            transfer,
            locations: None,
            clock,
            config,
            instance: format!("integration-worker:{}", db::new_uuid_v4()),
            wake: Notify::new(),
            shared: Mutex::new(Shared::default()),
            #[cfg(test)]
            fault: Mutex::new(None),
        }
    }

    /// The location verification the worker requests when an owner refuses a
    /// claim's target. Without it such a head parks and retries with backoff.
    pub fn with_locations(mut self, locations: Arc<dyn IntegrationLocationPort>) -> Self {
        self.locations = Some(locations);
        self
    }

    #[cfg(test)]
    pub(crate) fn set_fault(&self, hook: Option<FaultHook>) {
        *self.fault.lock().expect("fault hook") = hook;
    }
    pub(crate) fn fault(&self, state: IntegrationAttemptState, point: CrashPoint) -> Result<()> {
        #[cfg(test)]
        if let Some(hook) = self.fault.lock().expect("fault hook").as_ref() {
            if hook(state, point) {
                return Err(ServiceError::invalid_operation(format!(
                    "integration worker stopped at {state}/{point:?}"
                )));
            }
        }
        let _ = (state, point);
        Ok(())
    }

    /// Shared supervision owns restart and health; the worker owns its
    /// leases. Startup needs no separate path: the first sweep claims every
    /// expired head.
    pub fn start(
        self: Arc<Self>,
        workers: &crate::worker_runtime::PeriodicWorkers,
        shutdown: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        workers.worker("integration-queue").start(
            shutdown,
            || false,
            move |_, shutdown| Arc::clone(&self).run(shutdown),
        )
    }

    pub async fn run(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let stop = CancellationToken::new();
        let mut heads: JoinSet<()> = JoinSet::new();
        // No transfer of this process is running yet: leftovers of a crashed
        // one, and the refs of attempts that hold no slot, can go.
        if let Err(error) = self.transfer.sweep_at_start().await {
            tracing::warn!(target: "services::integration_worker", %error, "integration transfer sweep failed at start");
        }
        loop {
            if *shutdown.borrow_and_update() {
                break;
            }
            match self.sweep_once().await {
                Ok(drivers) => {
                    for driver in drivers {
                        heads.spawn(driver.run(stop.child_token()));
                    }
                }
                Err(error) => {
                    tracing::warn!(target: "services::integration_worker", %error, "integration sweep failed; the next sweep retries");
                }
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow_and_update() { break; }
                }
                Some(result) = heads.join_next(), if !heads.is_empty() => {
                    if let Err(error) = result {
                        tracing::warn!(target: "services::integration_worker", %error, "integration head driver stopped; lease recovery will take over");
                    }
                }
                _ = self.wake.notified() => {}
                _ = self.clock.sleep(self.config.sweep_interval) => {}
            }
        }
        // A driver stops at its next await; an in-flight fast-forward is
        // awaited to its receipt (it does not observe this token).
        stop.cancel();
        while heads.join_next().await.is_some() {}
        Ok(())
    }

    pub(crate) fn release_active(&self, queue_id: &str) {
        self.shared
            .lock()
            .expect("integration worker state")
            .active
            .remove(queue_id);
        self.wake.notify_one();
    }
    pub(crate) fn defer_reconcile(&self, queue_id: &str) {
        let until = later(self.clock.now(), self.config.reconcile_interval);
        self.shared
            .lock()
            .expect("integration worker state")
            .reconcile_after
            .insert(queue_id.to_owned(), until);
    }
    pub fn active_heads(&self) -> usize {
        self.shared
            .lock()
            .expect("integration worker state")
            .active
            .len()
    }

    pub(crate) async fn attempt(&self, id: &str) -> Result<IntegrationAttempt> {
        self.db
            .integration_attempt(id)
            .await?
            .ok_or(ServiceError::NotFound {
                entity: "integration_attempt",
                id: id.to_owned(),
            })
    }
    pub(crate) async fn queue(&self, id: &str) -> Result<IntegrationQueue> {
        self.db
            .integration_queue(id)
            .await?
            .ok_or(ServiceError::NotFound {
                entity: "integration_queue",
                id: id.to_owned(),
            })
    }

    /// The only attempt-state writer. Re-reads the row (a receipt, a timings
    /// write or a cancel request bumps `revision`), applies `change` to the
    /// fresh copy and refuses an edge the head table does not have. `change`
    /// returning `false` abandons the write.
    pub(crate) async fn advance_if(
        &self,
        attempt_id: &str,
        to: IntegrationAttemptState,
        change: impl FnOnce(&mut IntegrationAttempt) -> bool + Send,
    ) -> Result<Option<IntegrationAttempt>> {
        let mut attempt = self.attempt(attempt_id).await?;
        let from = attempt.state;
        if from != to && !head_row(from).allows(to) {
            return Err(ServiceError::invalid_operation(format!(
                "integration head table has no edge {from} -> {to}"
            )));
        }
        if !change(&mut attempt) {
            return Ok(None);
        }
        attempt.state = to;
        self.fault(from, CrashPoint::BeforeTransition)?;
        let attempt = self.db.transition_integration_attempt(attempt).await?;
        self.fault(from, CrashPoint::AfterTransition)?;
        Ok(Some(attempt))
    }
    pub(crate) async fn advance(
        &self,
        attempt_id: &str,
        to: IntegrationAttemptState,
        change: impl FnOnce(&mut IntegrationAttempt) + Send,
    ) -> Result<IntegrationAttempt> {
        Ok(self
            .advance_if(attempt_id, to, |attempt| {
                change(attempt);
                true
            })
            .await?
            .expect("unconditional change"))
    }

    pub(crate) fn step_request(
        attempt: &IntegrationAttempt,
        action: IntegrationStepAction,
    ) -> IntegrationStepRequest {
        IntegrationStepRequest {
            task_id: attempt.task_ref.clone(),
            queue_id: attempt.queue_id.clone().unwrap_or_default(),
            attempt_id: attempt.id.clone(),
            expected_epoch: attempt.expected_epoch,
            generation: attempt.slot_generation,
            effect_seq: attempt.effect_seq,
            action,
        }
    }

    /// One pass of every sweep. Returns the drivers of the queues it claimed;
    /// the supervised loop spawns them, a test steps them by hand. An idle
    /// install is read-only here.
    pub async fn sweep_once(self: &Arc<Self>) -> Result<Vec<HeadDriver>> {
        self.sweep_cancel_requests().await?;
        self.sweep_due_parked().await?;
        self.sweep_claims().await
    }

    /// Cancel requests on attempts that hold no slot. A flagged head is its
    /// driver's business; a flagged in-flight or unknown fast-forward stays
    /// listed until it is terminal and is skipped here (no hot loop).
    async fn sweep_cancel_requests(&self) -> Result<()> {
        let mut after: Option<String> = None;
        loop {
            let page = self
                .db
                .cancel_requested_integration_attempts(after.as_deref(), self.config.page)
                .await?;
            let Some(last) = page.last() else { break };
            after = Some(last.id.clone());
            let full = page.len() as u32 >= self.config.page;
            for attempt in page {
                if !head_row(attempt.state).allows(IntegrationAttemptState::Cancelled) {
                    continue;
                }
                let Some(queue_id) = attempt.queue_id.as_deref() else {
                    continue;
                };
                let queue = self.queue(queue_id).await?;
                if queue.head_attempt_id.as_deref() == Some(&attempt.id) {
                    continue;
                }
                if let Err(error) = self.cancel_released(&attempt).await {
                    if !is_conflict(&error) {
                        tracing::warn!(target: "services::integration_worker", attempt_id = %attempt.id, %error, "integration cancel sweep failed for one attempt");
                    }
                }
            }
            if !full {
                break;
            }
        }
        Ok(())
    }
    async fn cancel_released(&self, attempt: &IntegrationAttempt) -> Result<()> {
        self.steps
            .enqueue_step(&Self::step_request(attempt, IntegrationStepAction::Clear))
            .await?;
        self.advance_if(&attempt.id, IntegrationAttemptState::Cancelled, |attempt| {
            attempt.permit_json = None;
            attempt.available_at = None;
            attempt.cancel_requested_at.is_some() && attempt.effect_intent_json.is_none()
        })
        .await?;
        Ok(())
    }

    async fn sweep_due_parked(&self) -> Result<()> {
        let now = stamp(self.clock.now());
        let mut after: Option<String> = None;
        let mut requeued = false;
        loop {
            let page = self
                .db
                .due_parked_integration_attempts(&now, after.as_deref(), self.config.page)
                .await?;
            let Some(last) = page.last() else { break };
            after = Some(last.id.clone());
            let full = page.len() as u32 >= self.config.page;
            for attempt in page {
                if attempt.cancel_requested_at.is_some() {
                    continue;
                }
                // A counted retry (infrastructure, timeout) is over: one
                // `park` step restates `waiting` on the Task (the handler
                // reads the attempt as it is then). It gets a fresh
                // `effect_seq`; the park that began the wait used the present
                // one. Enqueued first: a step whose attempt is not re-queued
                // yet retries, and a repeat is the same key. A wait with no
                // end of its own (a paused Project, a target that is not
                // ready) restates nothing until it really ends: its Task was
                // told once when the wait began.
                let counted = attempt
                    .operation_receipts_json
                    .as_array()
                    .and_then(|journal| {
                        journal
                            .iter()
                            .rev()
                            .find(|entry| entry["kind"] == "worker_park")
                    })
                    .is_none_or(|entry| entry["counted"] == true);
                let seq = attempt.effect_seq + i64::from(counted);
                if counted {
                    let mut restate = Self::step_request(&attempt, IntegrationStepAction::Park);
                    restate.effect_seq = seq;
                    if let Err(error) = self.steps.enqueue_step(&restate).await {
                        tracing::warn!(target: "services::integration_worker", attempt_id = %attempt.id, %error, "integration re-queue step was not enqueued; the next sweep retries");
                        continue;
                    }
                }
                match self
                    .advance_if(&attempt.id, IntegrationAttemptState::Queued, |attempt| {
                        if attempt.state != IntegrationAttemptState::Parked
                            || attempt.effect_seq + i64::from(counted) != seq
                        {
                            return false;
                        }
                        attempt.available_at = None;
                        attempt.resume_state = None;
                        if counted {
                            attempt.effect_seq = seq;
                            attempt.effect_ack_json = None;
                            attempt.acknowledged_at = None;
                        }
                        true
                    })
                    .await
                {
                    Ok(Some(_)) => requeued = true,
                    Ok(None) => {}
                    Err(error) if is_conflict(&error) => {}
                    Err(error) => {
                        tracing::warn!(target: "services::integration_worker", attempt_id = %attempt.id, %error, "integration parked retry failed for one attempt");
                    }
                }
            }
            if !full {
                break;
            }
        }
        if requeued {
            self.wake.notify_one();
        }
        Ok(())
    }

    /// Expired heads (takeover) and unleased queues with work, in rotation.
    async fn sweep_claims(self: &Arc<Self>) -> Result<Vec<HeadDriver>> {
        let now = stamp(self.clock.now());
        let mut candidates: Vec<IntegrationQueue> = Vec::new();
        for expired in [true, false] {
            let mut after: Option<String> = None;
            loop {
                let page = if expired {
                    self.db
                        .expired_integration_heads(&now, after.as_deref(), self.config.page)
                        .await?
                } else {
                    self.db
                        .claimable_integration_queues(&now, after.as_deref(), self.config.page)
                        .await?
                };
                let Some(last) = page.last() else { break };
                after = Some(last.id.clone());
                let full = page.len() as u32 >= self.config.page;
                candidates.extend(page);
                if !full {
                    break;
                }
            }
        }
        candidates.sort_by(|a, b| a.id.cmp(&b.id));
        candidates.dedup_by(|a, b| a.id == b.id);
        let cursor = self
            .shared
            .lock()
            .expect("integration worker state")
            .cursor
            .clone();
        if let Some(cursor) = cursor {
            let split = candidates.partition_point(|queue| queue.id <= cursor);
            candidates.rotate_left(split);
        }
        let mut drivers = Vec::new();
        for queue in candidates {
            {
                let shared = self.shared.lock().expect("integration worker state");
                if shared.active.len() >= self.config.max_heads {
                    break;
                }
                if shared.active.contains(&queue.id) {
                    continue;
                }
            }
            match self.try_claim(&queue).await {
                Ok(Some(driver)) => {
                    let mut shared = self.shared.lock().expect("integration worker state");
                    shared.active.insert(queue.id.clone());
                    shared.cursor = Some(queue.id.clone());
                    shared.reconcile_after.remove(&queue.id);
                    drivers.push(driver);
                }
                Ok(None) => {}
                // One unreadable queue must not stop the sweep for the rest.
                Err(error) => {
                    tracing::warn!(target: "services::integration_worker", queue_id = %queue.id, %error, "integration claim failed for one queue");
                }
            }
        }
        Ok(drivers)
    }

    async fn try_claim(self: &Arc<Self>, listed: &IntegrationQueue) -> Result<Option<HeadDriver>> {
        let now = self.clock.now();
        let mut queue = self.queue(&listed.id).await?;
        if queue.state == IntegrationQueueState::Quarantined {
            let waiting = self
                .shared
                .lock()
                .expect("integration worker state")
                .reconcile_after
                .get(&queue.id)
                .is_some_and(|until| *until > now);
            if waiting {
                return Ok(None);
            }
            match self.db.integration_head(&queue.id).await? {
                // No head: an imported quarantine. Nothing was ever admitted
                // for the member that pins it, and storage re-verifies that,
                // so the queue re-opens for the members behind it. The pinned
                // attempt itself stays quarantined for its owner's Retry.
                None => match self.reopen_headless(&queue).await {
                    Ok(true) => {}
                    Ok(false) => {
                        self.defer_reconcile(&queue.id);
                        return Ok(None);
                    }
                    Err(error) => {
                        self.defer_reconcile(&queue.id);
                        return Err(error);
                    }
                },
                Some(head) => match head.state {
                    IntegrationAttemptState::Reconciling | IntegrationAttemptState::FfInflight => {}
                    IntegrationAttemptState::Quarantined => {
                        self.advance(&head.id, IntegrationAttemptState::Reconciling, |_| {})
                            .await?;
                    }
                    // The head resolved but the process died before the queue
                    // was re-opened: re-open with the stored witness.
                    _ => {
                        if let Err(error) = self.reopen(&queue, &head).await {
                            self.defer_reconcile(&queue.id);
                            return Err(error);
                        }
                    }
                },
            }
            queue = self.queue(&queue.id).await?;
        }
        match self
            .db
            .claim_integration_queue(
                &queue.id,
                queue.revision,
                &self.instance,
                &stamp(now),
                &stamp(later(now, self.config.lease)),
            )
            .await
        {
            Ok(claimed) => {
                // The queue is open (the claim re-opens a suspended one):
                // members that were told it is suspended wait in line again.
                if let Err(error) = self.restate_waiting(&claimed).await {
                    tracing::warn!(target: "services::integration_worker", queue_id = %claimed.id, %error, "integration members were not told their queue re-opened; the next claim retries");
                }
                Ok(Some(HeadDriver::new(Arc::clone(self), &claimed)))
            }
            Err(db::DbError::NotFound) => Ok(None),
            // Lost the race, or the target is not ready (the claim committed
            // `suspended`; the next sweep tries again).
            Err(db::DbError::VersionConflict) => {
                self.tell_suspended_members(&queue.id).await?;
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Each queued member of a suspended queue gets one `park` step, so its
    /// Task shows why it waits (the step reads the queue's `last_error_kind`).
    /// The key is the member's own `effect_seq`, so a repeat is a no-op.
    async fn tell_suspended_members(&self, queue_id: &str) -> Result<()> {
        let queue = self.queue(queue_id).await?;
        if queue.state != IntegrationQueueState::Suspended {
            return Ok(());
        }
        for member in self.db.integration_members(queue_id, 1000).await? {
            // The waiting members, and a head the target vanished under.
            let waits = member.state == IntegrationAttemptState::Queued
                || queue.head_attempt_id.as_deref() == Some(&member.id);
            if !waits || member.cancel_requested_at.is_some() {
                continue;
            }
            if told_suspended(&member) {
                continue;
            }
            self.steps
                .enqueue_step(&Self::step_request(&member, IntegrationStepAction::Park))
                .await?;
            let (seq, at) = (member.effect_seq, stamp(self.clock.now()));
            match self
                .advance_if(&member.id, member.state, move |member| {
                    if member.effect_seq != seq {
                        return false;
                    }
                    journal_push(
                        member,
                        serde_json::json!({"kind":"worker_told","reason":"queue_suspended","effect_seq":seq,"at":at}),
                    );
                    true
                })
                .await
            {
                Ok(_) => {}
                Err(error) if is_conflict(&error) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// Produce `park` for every waiting member whose Task was told the queue
    /// is suspended: the step now finds an open queue and restates `waiting`.
    /// Level-triggered on the stored mark, so it also runs for a queue that
    /// re-opened under a process that died before telling anyone.
    async fn restate_waiting(&self, claimed: &IntegrationQueue) -> Result<()> {
        for member in self.db.integration_members(&claimed.id, 1000).await? {
            if member.state != IntegrationAttemptState::Queued
                || claimed.head_attempt_id.as_deref() == Some(&member.id)
                || member.cancel_requested_at.is_some()
                || !told_suspended(&member)
            {
                continue;
            }
            let seq = member.effect_seq + 1;
            let mut restate = Self::step_request(&member, IntegrationStepAction::Park);
            restate.effect_seq = seq;
            self.steps.enqueue_step(&restate).await?;
            match self
                .advance_if(&member.id, IntegrationAttemptState::Queued, move |member| {
                    if member.effect_seq + 1 != seq {
                        return false;
                    }
                    member.effect_seq = seq;
                    member.effect_ack_json = None;
                    member.acknowledged_at = None;
                    true
                })
                .await
            {
                Ok(_) => {}
                Err(error) if is_conflict(&error) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// `Ok(false)`: no member of the queue can witness that nothing ran.
    async fn reopen_headless(&self, queue: &IntegrationQueue) -> Result<bool> {
        for member in self.db.integration_members(&queue.id, 1000).await? {
            let untouched = member.effect_intent_json.is_none()
                && member
                    .effect_receipts_json
                    .as_array()
                    .is_some_and(Vec::is_empty);
            if !untouched {
                continue;
            }
            let queue = self.queue(&queue.id).await?;
            return match self
                .db
                .reopen_integration_queue(
                    &queue.id,
                    queue.revision,
                    &IntegrationQueueReopenWitness::NoEffect {
                        attempt_id: member.id,
                    },
                )
                .await
            {
                Ok(_) => Ok(true),
                // Storage found an unsettled effect in the queue: it stays.
                Err(db::DbError::Check(_)) => Ok(false),
                Err(error) => Err(error.into()),
            };
        }
        Ok(false)
    }

    /// Leave `quarantined` with evidence the storage re-verifies: the head's
    /// newest settled receipt, or `NoEffect` for a head that never had one.
    pub(crate) async fn reopen(
        &self,
        queue: &IntegrationQueue,
        head: &IntegrationAttempt,
    ) -> Result<IntegrationQueue> {
        let receipts: Vec<db::IntegrationEffectReceipt> =
            serde_json::from_value(head.effect_receipts_json.clone()).unwrap_or_default();
        let witness = match receipts.iter().rev().find(|receipt| {
            matches!(
                receipt.operation_state,
                db::IntegrationOperationState::Succeeded | db::IntegrationOperationState::Failed
            )
        }) {
            Some(receipt) => IntegrationQueueReopenWitness::SettledEffect {
                attempt_id: head.id.clone(),
                generation: receipt.request.fence.generation,
                operation: receipt.request.kind,
            },
            None => IntegrationQueueReopenWitness::NoEffect {
                attempt_id: head.id.clone(),
            },
        };
        let queue = self.queue(&queue.id).await?;
        Ok(self
            .db
            .reopen_integration_queue(&queue.id, queue.revision, &witness)
            .await?)
    }

    /// `Ok(false)`: the lease is no longer this worker's at `generation`.
    pub(crate) async fn renew_if_due(&self, queue_id: &str, generation: i64) -> Result<bool> {
        for _ in 0..3 {
            let queue = self.queue(queue_id).await?;
            let now = self.clock.now();
            let Some(until) = parse_time(queue.lease_until.as_deref()) else {
                return Ok(false);
            };
            if queue.lease_owner.as_deref() != Some(&self.instance)
                || queue.fence_generation != generation
                || until <= now
            {
                return Ok(false);
            }
            let fresh_for = self.config.lease.saturating_sub(self.config.renew_every);
            if until > later(now, fresh_for) {
                return Ok(true);
            }
            // An admission bumps the queue revision between this read and
            // the write: read again rather than give the lease up.
            match self
                .db
                .renew_integration_queue(
                    queue_id,
                    queue.revision,
                    &self.instance,
                    generation,
                    &stamp(now),
                    &stamp(later(now, self.config.lease)),
                )
                .await
            {
                Ok(_) => return Ok(true),
                Err(db::DbError::VersionConflict) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(false)
    }
}

impl IntegrationEnqueuePort for IntegrationQueueWorker {
    fn notify_enqueued(&self, _queue_id: &str) {
        self.wake.notify_one();
    }
}

#[async_trait]
impl IntegrationSnapshotPort for IntegrationQueueWorker {
    async fn integration_queue_snapshot(
        &self,
        queue_id: &str,
        member_limit: u32,
    ) -> Result<Option<IntegrationQueueSnapshot>> {
        let Some(queue) = self.db.integration_queue(queue_id).await? else {
            return Ok(None);
        };
        let driven_here = self
            .shared
            .lock()
            .expect("integration worker state")
            .active
            .contains(queue_id);
        let has_head = u32::from(queue.head_attempt_id.is_some());
        let mut waiting = 0u32;
        let mut members = Vec::new();
        for attempt in self.db.integration_members(queue_id, member_limit).await? {
            let position = if queue.head_attempt_id.as_deref() == Some(&attempt.id) {
                Some(0)
            } else if attempt.state == IntegrationAttemptState::Queued {
                waiting += 1;
                Some(waiting - 1 + has_head)
            } else {
                None
            };
            members.push(IntegrationMemberSnapshot {
                task_id: attempt.task_ref,
                queue_seq: attempt.queue_seq,
                attempt_number: attempt.attempt_number,
                position,
                state: attempt.state,
                cancel_requested: attempt.cancel_requested_at.is_some(),
                failure_kind: attempt.failure_kind,
                failure_message: attempt.failure_message,
                available_at: attempt.available_at,
                timings: attempt.phase_timings,
                attempt_id: attempt.id,
            });
        }
        Ok(Some(IntegrationQueueSnapshot {
            queue_id: queue.id,
            repo_id: queue.repo_id,
            target_branch: queue.target_branch,
            state: queue.state,
            head_attempt_id: queue.head_attempt_id,
            driven_here,
            lease_until: queue.lease_until,
            last_error_kind: queue.last_error_kind,
            last_error: queue.last_error,
            members,
        }))
    }
}
