//! Per-queue head driver. One `pass` performs the action of the head's row
//! in the table and at most one state transition chain; every await point is
//! followed by a re-read, so a pass can stop anywhere and the next claim
//! continues from the stored rows.
use super::{
    is_conflict, later, parse_time, stamp, CrashPoint, HeadAction, HeadFacts, HeadTimeout,
    IntegrationQueueWorker, IntegrationStepAck, IntegrationStepAction, IntegrationStepOutcome,
    IntegrationStepState, ObjectTransferDirection, ObjectTransferEndpoint, ObjectTransferOutcome,
    ObjectTransferRelease, ObjectTransferRequest, OwnerFastForwardRequest, OwnerRebaseRequest,
    TaskGate,
};
use crate::{
    integration_effects::EffectOwner,
    integration_owner::{OwnerEffectRefusal, OwnerMergeReceipt, OwnerRebaseReceipt},
    MergeOutcome, Result, ServiceError,
};
use api_types::WorkspaceOwnerOperationOutcome;
use chrono::{DateTime, Utc};
use db::{
    IntegrationActivationRepo, IntegrationAttempt, IntegrationAttemptState as S,
    IntegrationCheckTiming, IntegrationCiSkipReason, IntegrationEffectReceipt,
    IntegrationEffectRequest, IntegrationFailureKind, IntegrationLostRace, IntegrationLostRaceKind,
    IntegrationOperationKind, IntegrationOperationState, IntegrationOwnerFence,
    IntegrationPhaseTimings, IntegrationQueue, IntegrationQueueState,
};
use serde_json::json;
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pass {
    /// A row changed; run the next pass at once.
    Progress,
    /// Nothing to do before this delay (or a wake).
    Wait(Duration),
    /// The head left the slot (terminal, ejected, parked, ...).
    Released,
    /// The lease is not this driver's any more, or it stopped on request.
    Lost,
}

/// Why the worker parked an attempt. `code` leads `failure_message`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParkReason {
    Infrastructure,
    Timeout,
    ProjectPaused,
    TargetNotReady,
    TargetDirty,
    WorktreeDirty,
    UnsupportedConflict,
    TransferTooLarge,
    ExternalMovesExhausted,
    RoundsExhausted,
    /// The Task's checkout and the default checkout have different owners.
    CrossOwner,
    /// A deciding Task step settled without answering, again after re-asks.
    StepFailed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Retry {
    /// Counted against `infra_retry`; then the owner decides.
    Counted,
    /// Re-queued by the next sweep interval, uncounted.
    Soon,
    /// No automatic retry.
    Owner,
}
impl ParkReason {
    pub fn code(self) -> &'static str {
        match self {
            Self::Infrastructure => "infrastructure",
            Self::Timeout => "timeout",
            Self::ProjectPaused => "project_paused",
            Self::TargetNotReady => "target_not_ready",
            Self::TargetDirty => "target_dirty",
            Self::WorktreeDirty => "worktree_dirty",
            Self::UnsupportedConflict => "unsupported_conflict",
            Self::TransferTooLarge => "transfer_too_large",
            Self::ExternalMovesExhausted => "external_target_moves_exhausted",
            Self::RoundsExhausted => "rounds_exhausted",
            Self::CrossOwner => "cross_owner_unsupported",
            Self::StepFailed => "task_step_failed",
        }
    }
    fn kind(self) -> IntegrationFailureKind {
        match self {
            Self::Infrastructure | Self::StepFailed => IntegrationFailureKind::Infrastructure,
            Self::Timeout => IntegrationFailureKind::Timeout,
            Self::TargetNotReady => IntegrationFailureKind::TargetUnavailable,
            _ => IntegrationFailureKind::OwnerRequired,
        }
    }
    fn retry(self) -> Retry {
        match self {
            Self::Infrastructure | Self::Timeout | Self::StepFailed => Retry::Counted,
            Self::ProjectPaused | Self::TargetNotReady => Retry::Soon,
            _ => Retry::Owner,
        }
    }
}

fn bounded(text: &str) -> String {
    let mut end = text.len().min(2048);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}
fn millis(from: DateTime<Utc>, to: DateTime<Utc>) -> i64 {
    (to - from).num_milliseconds().max(0)
}
fn add(slot: &mut Option<i64>, value: i64) {
    *slot = Some(slot.unwrap_or(0).saturating_add(value));
}
/// The owner a queue's target (or a fence's `target_owner`) names.
fn target_owner(owner: Option<&serde_json::Value>) -> Option<EffectOwner> {
    let owner = owner?;
    match (
        owner["owner_kind"].as_str()?,
        owner["daemon_id"].as_str(),
        owner["runtime_id"].as_str(),
    ) {
        ("server", _, _) => Some(EffectOwner::Server),
        ("daemon", Some(daemon_id), Some(runtime_id)) => Some(EffectOwner::Daemon {
            daemon_id: daemon_id.to_owned(),
            runtime_id: runtime_id.to_owned(),
        }),
        _ => None,
    }
}
fn receipts(attempt: &IntegrationAttempt) -> Vec<IntegrationEffectReceipt> {
    serde_json::from_value(attempt.effect_receipts_json.clone()).unwrap_or_default()
}

pub struct HeadDriver {
    w: Arc<IntegrationQueueWorker>,
    queue_id: String,
    generation: i64,
    /// Steps this session enqueued, by `(effect_seq, action)`. A new session
    /// (a new claim) asks again under a new `effect_seq`.
    enqueued: HashSet<(i64, IntegrationStepAction)>,
    /// When this session first saw the head in its present state.
    since: Option<(S, DateTime<Utc>)>,
    result_checked: Option<DateTime<Utc>>,
    /// The ends of an object transfer this session made for the head.
    transferred: Option<ObjectTransferRelease>,
    /// Permits this session could not use (they did not bind the head).
    refused_permits: u32,
    /// Deciding steps this session asked again because they settled without
    /// answering.
    reasked: u32,
    /// When this session last asked for the head's check.
    check_asked: Option<DateTime<Utc>>,
    stop: CancellationToken,
}

impl HeadDriver {
    pub(crate) fn new(w: Arc<IntegrationQueueWorker>, claimed: &IntegrationQueue) -> Self {
        Self {
            w,
            queue_id: claimed.id.clone(),
            generation: claimed.fence_generation,
            enqueued: HashSet::new(),
            since: None,
            result_checked: None,
            transferred: None,
            refused_permits: 0,
            reasked: 0,
            check_asked: None,
            stop: CancellationToken::new(),
        }
    }
    pub fn queue_id(&self) -> &str {
        &self.queue_id
    }
    pub fn generation(&self) -> i64 {
        self.generation
    }

    /// Drive the head until it leaves the slot, the lease is lost or `stop`
    /// fires. Repeated errors end the session; the lease then expires and
    /// the next claim takes over, so the backoff is bounded by the lease.
    pub async fn run(mut self, stop: CancellationToken) {
        self.stop = stop.clone();
        let mut errors = 0u32;
        loop {
            if stop.is_cancelled() {
                break;
            }
            let wait = match self.pass().await {
                Ok(Pass::Progress) => {
                    errors = 0;
                    continue;
                }
                Ok(Pass::Wait(wait)) => {
                    errors = 0;
                    wait
                }
                Ok(Pass::Released | Pass::Lost) => break,
                Err(error) => {
                    errors += 1;
                    tracing::warn!(target: "services::integration_worker", queue_id = %self.queue_id, %error, "integration head pass failed");
                    if errors > 5 {
                        break;
                    }
                    self.w.config.conflict_backoff * errors
                }
            };
            tokio::select! {
                _ = stop.cancelled() => break,
                _ = self.w.clock.sleep(wait) => {}
            }
        }
        self.w.release_active(&self.queue_id);
    }

    /// One pass. A version conflict is not an error: another writer (a Task
    /// step's acknowledgment, a cancel request) got in; the next pass re-reads.
    pub async fn pass(&mut self) -> Result<Pass> {
        match self.step().await {
            Err(error) if is_conflict(&error) => Ok(Pass::Wait(self.w.config.conflict_backoff)),
            Ok(pass @ (Pass::Released | Pass::Lost)) => {
                if pass == Pass::Released {
                    if let Some(release) = self.transferred.take() {
                        // Best effort: a leftover ref holds objects only; the
                        // transfer owner also sweeps ended attempts at start.
                        if let Err(error) = self.w.transfer.release(release).await {
                            tracing::warn!(target: "services::integration_worker", queue_id = %self.queue_id, %error, "integration transfer refs were not released");
                        }
                    }
                }
                self.w.release_active(&self.queue_id);
                Ok(pass)
            }
            other => other,
        }
    }

    async fn step(&mut self) -> Result<Pass> {
        let w = Arc::clone(&self.w);
        if !w.renew_if_due(&self.queue_id, self.generation).await? {
            return Ok(Pass::Lost);
        }
        let queue = w.queue(&self.queue_id).await?;
        let Some(head) = queue.head_attempt_id.clone() else {
            return Ok(Pass::Released);
        };
        let a = w.attempt(&head).await?;
        let now = w.clock.now();
        if self.since.map(|(state, _)| state) != Some(a.state) {
            self.since = Some((a.state, now));
        }
        match super::head_row(a.state).action {
            HeadAction::Start => self.start(a).await,
            HeadAction::Validate => self.validate(a, &queue).await,
            HeadAction::Rebase => self.rebase(a, &queue).await,
            HeadAction::AwaitCheck => self.await_check(a).await,
            HeadAction::AwaitPermit => self.await_permit(a).await,
            HeadAction::CommitFastForward => self.commit_fast_forward(a, &queue).await,
            HeadAction::FastForward => self.fast_forward(a, &queue).await,
            HeadAction::Reconcile => self.reconcile(a, &queue).await,
            HeadAction::AwaitResult => self.await_result(a, &queue).await,
            HeadAction::Requeue => {
                w.advance(&a.id, S::Reconciling, |_| {}).await?;
                Ok(Pass::Progress)
            }
            // A released or terminal attempt cannot hold the slot.
            HeadAction::Released | HeadAction::Terminal => Ok(Pass::Lost),
        }
    }

    fn timed_out(&self, timeout: HeadTimeout) -> bool {
        let limit = match timeout {
            HeadTimeout::None => return false,
            HeadTimeout::Validate => self.w.config.validate_timeout,
            HeadTimeout::Rebase => self.w.config.rebase_deadline,
            HeadTimeout::Check => self.w.config.check_wait,
            HeadTimeout::Step => self.w.config.step_wait,
            HeadTimeout::Permit => self.w.config.permit_lifetime,
            HeadTimeout::FastForward => self.w.config.ff_owner_bound,
        };
        self.since
            .is_some_and(|(_, since)| self.w.clock.now() >= later(since, limit))
    }

    async fn timings(
        &self,
        attempt_id: &str,
        change: impl FnOnce(&mut IntegrationPhaseTimings) + Send,
    ) -> Result<()> {
        let a = self.w.attempt(attempt_id).await?;
        let mut timings = a.phase_timings.clone().unwrap_or_default();
        change(&mut timings);
        if a.phase_timings.as_ref() != Some(&timings) {
            self.w
                .db
                .record_integration_head_timings(attempt_id, a.revision, &timings)
                .await?;
        }
        Ok(())
    }
    /// Total head time, written while the attempt still holds the slot.
    async fn close_timings(&self, a: &IntegrationAttempt) -> Result<()> {
        let now = self.w.clock.now();
        let started = parse_time(a.started_at.as_deref());
        self.timings(&a.id, |timings| {
            if let Some(started) = started {
                timings.head_total_ms = Some(millis(started, now));
            }
        })
        .await
    }

    async fn enqueue(
        &mut self,
        a: &IntegrationAttempt,
        action: IntegrationStepAction,
    ) -> Result<()> {
        self.w
            .steps
            .enqueue_step(&IntegrationQueueWorker::step_request(a, action))
            .await?;
        self.enqueued.insert((a.effect_seq, action));
        self.w.fault(a.state, CrashPoint::AfterStepEnqueue)
    }

    /// Ask for a deciding step (`request_check`, `settle`) under a fresh
    /// `effect_seq`. The causation key carries no generation, so an enqueue
    /// repeated after a takeover at the same `effect_seq` would be dropped as
    /// a duplicate while the stored step still names the old generation.
    async fn fresh_step(
        &mut self,
        a: &IntegrationAttempt,
        action: IntegrationStepAction,
    ) -> Result<IntegrationAttempt> {
        let a = self
            .w
            .advance(&a.id, a.state, |a| {
                a.effect_seq += 1;
                a.effect_ack_json = None;
                a.acknowledged_at = None;
                a.permit_json = None;
            })
            .await?;
        // A permit of an earlier ask pre-enqueued its protected `result`
        // step. It sleeps until its own deadline and holds back every later
        // step of the Task, this one included. That round is over: wake it,
        // so it reads the newer `effect_seq`, writes nothing and finishes.
        // (The ask before this one, and the one a takeover moved past.)
        for stale in [a.effect_seq - 1, a.effect_seq - 2] {
            if stale > 0 {
                self.w.steps.ready_result_step(&a.id, stale).await?;
            }
        }
        self.enqueue(&a, action).await?;
        Ok(a)
    }

    /// The step this state waits for, asked once per session.
    async fn ensure_step(
        &mut self,
        a: &IntegrationAttempt,
        action: IntegrationStepAction,
    ) -> Result<bool> {
        if self.enqueued.contains(&(a.effect_seq, action)) {
            return Ok(false);
        }
        self.fresh_step(a, action).await?;
        Ok(true)
    }

    /// The deciding step of this wait settled without answering (its handler
    /// failed for good, or a preempting command dropped it): the wait would
    /// only end at its timeout. Ask again under a new `effect_seq`, a bounded
    /// number of times per session, then park with a typed cause.
    /// `Ok(None)`: the step can still answer.
    async fn reask_dead_step(
        &mut self,
        a: &IntegrationAttempt,
        action: IntegrationStepAction,
    ) -> Result<Option<Pass>> {
        let state = self
            .w
            .steps
            .step_state(&IntegrationQueueWorker::step_request(a, action))
            .await?;
        if state != IntegrationStepState::Dead {
            return Ok(None);
        }
        if self.reasked >= self.w.config.step_reasks {
            return self
                .park(
                    a,
                    ParkReason::StepFailed,
                    &format!(
                        "the `{}` Task step failed after {} asks",
                        action.as_str(),
                        self.reasked + 1
                    ),
                )
                .await
                .map(Some);
        }
        self.reasked += 1;
        self.fresh_step(a, action).await?;
        Ok(Some(Pass::Progress))
    }

    // ----- releases -----------------------------------------------------

    async fn release_cancelled(&mut self, a: &IntegrationAttempt) -> Result<Pass> {
        if a.effect_intent_json.is_some() {
            self.w.advance(&a.id, S::Reconciling, |_| {}).await?;
            return Ok(Pass::Progress);
        }
        self.enqueue(a, IntegrationStepAction::Clear).await?;
        self.close_timings(a).await?;
        self.w
            .advance(&a.id, S::Cancelled, |a| {
                a.permit_json = None;
                a.deadline = None;
            })
            .await?;
        Ok(Pass::Released)
    }

    async fn park(
        &mut self,
        a: &IntegrationAttempt,
        reason: ParkReason,
        detail: &str,
    ) -> Result<Pass> {
        if a.effect_intent_json.is_some() {
            // An admitted effect is settled by its owner, never by a park.
            self.w.advance(&a.id, S::Reconciling, |_| {}).await?;
            return Ok(Pass::Progress);
        }
        let now = self.w.clock.now();
        let journal = a
            .operation_receipts_json
            .as_array()
            .cloned()
            .unwrap_or_default();
        let parks = |entry: &&serde_json::Value| entry["kind"] == "worker_park";
        let counted = journal
            .iter()
            .filter(parks)
            .filter(|entry| entry["counted"] == true)
            .count();
        // Parks for this same reason with nothing but the re-queue between
        // them (`start` adds one to `effect_seq`; any step adds more).
        // The re-queue of a parked attempt adds one too (its `park` step
        // restates `waiting`), and a head reconciled back to `queued` skips
        // that one: a gap of one or two.
        let mut repeats = 0u32;
        let mut seq = a.effect_seq;
        for entry in journal.iter().rev().filter(parks) {
            let Some(at) = entry["effect_seq"].as_i64() else {
                break;
            };
            if entry["reason"] != reason.code() || !(1..=2).contains(&(seq - at)) {
                break;
            }
            repeats += 1;
            seq = at;
        }
        let (kind, available_at, message) = match reason.retry() {
            Retry::Counted => match self.w.config.infra_retry.get(counted) {
                Some(delay) => (
                    reason.kind(),
                    Some(stamp(later(now, *delay))),
                    format!("{}: {detail}", reason.code()),
                ),
                None => (
                    IntegrationFailureKind::OwnerRequired,
                    None,
                    format!("retries_exhausted: {}: {detail}", reason.code()),
                ),
            },
            // A wait with no end of its own (a paused Project, a target that
            // is not ready): the retry backs off to `soon_retry_max`.
            Retry::Soon => (
                reason.kind(),
                Some(stamp(later(
                    now,
                    self.w
                        .config
                        .sweep_interval
                        .saturating_mul(1u32 << repeats.min(16))
                        .min(self.w.config.soon_retry_max),
                ))),
                format!("{}: {detail}", reason.code()),
            ),
            Retry::Owner => (reason.kind(), None, format!("{}: {detail}", reason.code())),
        };
        // The Task was told when this wait began. A wait with no end of its
        // own (`Soon`) is not told again on each retry, and its re-queue
        // restates nothing either; a counted retry was re-queued with
        // `waiting` restated, so its next park says so again.
        if repeats == 0 || reason.retry() != Retry::Soon {
            self.enqueue(a, IntegrationStepAction::Park).await?;
        }
        self.close_timings(a).await?;
        let entry = json!({"kind":"worker_park","reason":reason.code(),"counted":reason.retry()==Retry::Counted,"effect_seq":a.effect_seq,"at":stamp(now)});
        self.w
            .advance(&a.id, S::Parked, move |a| {
                super::journal_push(a, entry);
                a.failure_kind = Some(kind);
                a.failure_message = Some(bounded(&message));
                a.available_at = available_at;
                a.permit_json = None;
                a.deadline = None;
            })
            .await?;
        Ok(Pass::Released)
    }

    /// The owner does not recognise the claim's target: its record of the
    /// default checkout differs from the one the claim froze (the location
    /// was verified again or changed since, for example on a daemon
    /// reconnect). The refusal ran no Git. The head leaves the slot and
    /// retries with backoff; each retry is a new claim, which re-reads the
    /// target. Holding the lease and claiming again at once would only burn
    /// rounds until the owner and the server agree.
    async fn foreign_owner(&mut self, a: &IntegrationAttempt) -> Result<Pass> {
        self.park(
            a,
            ParkReason::TargetNotReady,
            "the owner of the default checkout does not recognise this claim's target yet",
        )
        .await
    }

    async fn send_back(
        &mut self,
        a: &IntegrationAttempt,
        to: S,
        change: impl FnOnce(&mut IntegrationAttempt) + Send,
    ) -> Result<Pass> {
        self.enqueue(a, IntegrationStepAction::SendBack).await?;
        self.close_timings(a).await?;
        self.w
            .advance(&a.id, to, |a| {
                a.permit_json = None;
                a.deadline = None;
                change(a);
            })
            .await?;
        Ok(Pass::Released)
    }

    // ----- rounds ---------------------------------------------------------

    /// Another fence generation for the same head. One generation admits one
    /// receipt per effect kind, so a second rebase or fast-forward needs it.
    /// `Ok(None)`: the head was parked or the lease is gone.
    async fn new_round(
        &mut self,
        a: &IntegrationAttempt,
        lost: Option<IntegrationLostRaceKind>,
        counted: bool,
    ) -> Result<Option<Pass>> {
        let now = self.w.clock.now();
        let mut rounds = 0;
        let mut external = 0;
        self.timings(&a.id, |timings| {
            if let Some(kind) = lost {
                timings.lost_races.push(IntegrationLostRace {
                    kind,
                    round: timings.rounds.max(1),
                    at: stamp(now),
                });
            }
            if counted {
                timings.rounds += 1;
            }
            rounds = timings.rounds;
            external = timings.external_target_moves();
        })
        .await?;
        let a = self.w.attempt(&a.id).await?;
        if external > self.w.config.external_move_allowance {
            return self
                .park(
                    &a,
                    ParkReason::ExternalMovesExhausted,
                    "the target branch keeps moving outside Forge",
                )
                .await
                .map(Some);
        }
        if rounds > self.w.config.max_rounds {
            return self
                .park(
                    &a,
                    ParkReason::RoundsExhausted,
                    "too many integration rounds",
                )
                .await
                .map(Some);
        }
        let queue = self.w.queue(&self.queue_id).await?;
        match self
            .w
            .db
            .start_integration_round(
                &queue.id,
                queue.revision,
                &self.w.instance,
                self.generation,
                &stamp(now),
                &stamp(later(now, self.w.config.lease)),
            )
            .await
        {
            Ok(queue) => {
                self.generation = queue.fence_generation;
                self.enqueued.clear();
                Ok(None)
            }
            Err(db::DbError::VersionConflict) => {
                let queue = self.w.queue(&self.queue_id).await?;
                if queue.state == IntegrationQueueState::Suspended
                    && queue.lease_owner.as_deref() == Some(&self.w.instance)
                {
                    // The target stopped being ready; the storage left the
                    // lease with us. Release the head instead of holding it
                    // until expiry.
                    let a = self.w.attempt(&a.id).await?;
                    return self
                        .park(
                            &a,
                            ParkReason::TargetNotReady,
                            "the default checkout is not ready",
                        )
                        .await
                        .map(Some);
                }
                Ok(Some(Pass::Lost))
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn facts(&self, a: &IntegrationAttempt, queue: &IntegrationQueue) -> Result<HeadFacts> {
        match tokio::time::timeout(
            self.w.config.validate_timeout,
            self.w.facts.head_facts(a, queue),
        )
        .await
        {
            Ok(facts) => facts,
            Err(_) => Err(ServiceError::invalid_operation(
                "integration head facts timed out",
            )),
        }
    }
    async fn fence(&self, a: &IntegrationAttempt) -> Result<IntegrationOwnerFence> {
        self.w
            .db
            .integration_owner_fence(&a.id)
            .await?
            .ok_or_else(|| ServiceError::invalid_operation("integration head has no owner fence"))
    }

    async fn transfer(
        &mut self,
        a: &IntegrationAttempt,
        queue: &IntegrationQueue,
        facts: &HeadFacts,
        direction: ObjectTransferDirection,
    ) -> Result<Option<Pass>> {
        if facts.shared_object_store {
            return Ok(None);
        }
        let started = self.w.clock.now();
        let (have, want) = match direction {
            ObjectTransferDirection::Inbound => {
                (facts.candidate_head.clone(), facts.target_tip.clone())
            }
            ObjectTransferDirection::Outbound => {
                (facts.target_tip.clone(), facts.candidate_head.clone())
            }
        };
        let fence = self.fence(a).await?;
        let target = ObjectTransferEndpoint {
            repo_location_id: fence.target_owner["location_id"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            owner: target_owner(Some(&fence.target_owner)).unwrap_or(EffectOwner::Server),
        };
        self.transferred = Some(ObjectTransferRelease {
            attempt_id: a.id.clone(),
            task: facts.task_location.clone(),
            target: target.clone(),
        });
        let outcome = self
            .w
            .transfer
            .transfer(ObjectTransferRequest {
                fence,
                direction,
                task: facts.task_location.clone(),
                target,
                target_branch: queue.target_branch.clone(),
                have: vec![have],
                want,
                max_bytes: self.w.config.transfer_cap_bytes,
            })
            .await;
        match outcome {
            Ok(ObjectTransferOutcome::Transferred { .. }) => {
                let elapsed = millis(started, self.w.clock.now());
                self.timings(&a.id, |timings| add(&mut timings.transfer_ms, elapsed))
                    .await?;
                Ok(None)
            }
            Ok(ObjectTransferOutcome::TooLarge { bytes }) => {
                let a = self.w.attempt(&a.id).await?;
                self.park(
                    &a,
                    ParkReason::TransferTooLarge,
                    &format!(
                        "change too large to move between machines ({bytes} bytes); place the Task on the target machine"
                    ),
                )
                .await
                .map(Some)
            }
            Err(error) => {
                let a = self.w.attempt(&a.id).await?;
                self.park(&a, ParkReason::Infrastructure, &error.to_string())
                    .await
                    .map(Some)
            }
        }
    }

    // ----- states ---------------------------------------------------------

    async fn start(&mut self, a: IntegrationAttempt) -> Result<Pass> {
        if a.cancel_requested_at.is_some() {
            return self.release_cancelled(&a).await;
        }
        let now = self.w.clock.now();
        let enqueued = parse_time(Some(&a.enqueued_at));
        let mut rounds = 0;
        self.timings(&a.id, |timings| {
            timings.rounds += 1;
            rounds = timings.rounds;
            if timings.queued_ms.is_none() {
                timings.queued_ms = enqueued.map(|enqueued| millis(enqueued, now));
            }
        })
        .await?;
        if rounds > self.w.config.max_rounds {
            let a = self.w.attempt(&a.id).await?;
            return self
                .park(
                    &a,
                    ParkReason::RoundsExhausted,
                    "too many integration rounds",
                )
                .await;
        }
        self.w
            .advance(&a.id, S::Validating, |a| {
                // Head age reads `started_at`; the claim does not set it.
                if a.started_at.is_none() {
                    a.started_at = Some(stamp(now));
                }
                a.effect_seq += 1;
                a.effect_ack_json = None;
                a.acknowledged_at = None;
                a.permit_json = None;
                a.deadline = None;
                a.available_at = None;
                a.failure_kind = None;
                a.failure_message = None;
            })
            .await?;
        Ok(Pass::Progress)
    }

    async fn validate(&mut self, a: IntegrationAttempt, queue: &IntegrationQueue) -> Result<Pass> {
        if a.cancel_requested_at.is_some() {
            return self.release_cancelled(&a).await;
        }
        let started = self.w.clock.now();
        let facts = match self.facts(&a, queue).await {
            Ok(facts) => facts,
            Err(error) if self.timed_out(HeadTimeout::Validate) => {
                return self.park(&a, ParkReason::Timeout, &error.to_string()).await
            }
            Err(error) => {
                return self
                    .park(&a, ParkReason::Infrastructure, &error.to_string())
                    .await
            }
        };
        match facts.gate {
            TaskGate::Live => {}
            TaskGate::Left => return self.release_cancelled(&a).await,
            TaskGate::ProjectPaused => {
                return self
                    .park(&a, ParkReason::ProjectPaused, "the Project is paused")
                    .await
            }
        }
        // One fenced owner performs the rebase (in the Task's checkout) and
        // the fast-forward (in the default checkout): the owner gate refuses
        // an effect whose workspace it does not hold.
        if Some(&facts.workspace.owner) != target_owner(queue.target_owner_json.as_ref()).as_ref() {
            return self
                .park(
                    &a,
                    ParkReason::CrossOwner,
                    "the Task's checkout and the default checkout are on different machines; place the Task on the machine that holds the default checkout",
                )
                .await;
        }
        let admitted = a
            .candidate_sha
            .clone()
            .or_else(|| a.original_candidate_sha.clone());
        if facts.candidate_in_target {
            // Landed witness: the candidate is already in the target.
            let head = facts.candidate_head.clone();
            let tip = facts.target_tip.clone();
            self.w
                .advance(&a.id, S::Applied, |a| {
                    a.candidate_sha = Some(head.clone());
                    a.target_tip_sha = Some(tip);
                    a.integrated_sha = Some(head);
                })
                .await?;
            return Ok(Pass::Progress);
        }
        if admitted
            .as_deref()
            .is_some_and(|sha| sha != facts.candidate_head)
        {
            return self
                .send_back(&a, S::NeedsReview, |a| {
                    a.failure_message =
                        Some("the candidate commit changed after it was admitted".into());
                })
                .await;
        }
        if facts.rebase_in_progress {
            return self
                .park(
                    &a,
                    ParkReason::Infrastructure,
                    "a rebase is stopped in the Task checkout",
                )
                .await;
        }
        if facts.worktree_dirty {
            return self
                .park(
                    &a,
                    ParkReason::WorktreeDirty,
                    "the Task checkout has uncommitted changes",
                )
                .await;
        }
        if facts.target_dirty {
            return self
                .park(
                    &a,
                    ParkReason::TargetDirty,
                    "the default checkout has uncommitted changes",
                )
                .await;
        }
        if let Some(pass) = self
            .transfer(&a, queue, &facts, ObjectTransferDirection::Inbound)
            .await?
        {
            return Ok(pass);
        }
        let rebased_by_queue = a
            .original_candidate_sha
            .as_deref()
            .is_some_and(|original| original != facts.candidate_head);
        let checked = a.checks_commit_sha.as_deref() == Some(&facts.candidate_head);
        let elapsed = millis(started, self.w.clock.now());
        let skip_check = facts.target_in_candidate && (!rebased_by_queue || checked);
        let lost = !facts.target_in_candidate;
        let now = self.w.clock.now();
        self.timings(&a.id, |timings| {
            add(&mut timings.validate_ms, elapsed);
            if skip_check && !rebased_by_queue {
                timings.check = Some(IntegrationCheckTiming::Skipped {
                    reason: IntegrationCiSkipReason::TargetUnchanged,
                });
            }
            if lost {
                // The slot was free while the target moved: earlier queue
                // members (free of charge). A move while the slot is held is
                // recorded as external where it is detected.
                timings.lost_races.push(IntegrationLostRace {
                    kind: IntegrationLostRaceKind::QueueMember,
                    round: timings.rounds.max(1),
                    at: stamp(now),
                });
            }
        })
        .await?;
        let head = facts.candidate_head.clone();
        let tip = facts.target_tip.clone();
        if skip_check {
            // Unchanged target: the reviewed commit goes to authorization as
            // it is; no new check.
            let a = self.w.attempt(&a.id).await?;
            let a = self.fresh_step(&a, IntegrationStepAction::Settle).await?;
            self.w
                .advance(&a.id, S::AwaitingTaskStep, |a| {
                    a.candidate_sha = Some(head);
                    a.target_tip_sha = Some(tip);
                })
                .await?;
        } else {
            self.w
                .advance(&a.id, S::Rebasing, |a| {
                    a.candidate_sha = Some(head);
                    a.target_tip_sha = Some(tip);
                })
                .await?;
        }
        Ok(Pass::Progress)
    }

    async fn rebase(&mut self, a: IntegrationAttempt, queue: &IntegrationQueue) -> Result<Pass> {
        if a.effect_intent_json.is_some() {
            self.w.advance(&a.id, S::Reconciling, |_| {}).await?;
            return Ok(Pass::Progress);
        }
        if a.cancel_requested_at.is_some() {
            return self.release_cancelled(&a).await;
        }
        let facts = match self.facts(&a, queue).await {
            Ok(facts) => facts,
            Err(error) => {
                return self
                    .park(&a, ParkReason::Infrastructure, &error.to_string())
                    .await
            }
        };
        // A rebase whose receipt was recorded but never applied (the process
        // stopped between the receipt and the transition), in this or an
        // earlier generation. Applying it changes `candidate_sha`, so it is
        // adopted once.
        let adopted = receipts(&a).into_iter().rev().find(|receipt| {
            receipt.request.kind == IntegrationOperationKind::Rebase
                && receipt.operation_state == IntegrationOperationState::Succeeded
                && receipt.request.witness["expected_head_sha"].as_str()
                    == a.candidate_sha.as_deref()
                && receipt.request.witness["expected_target_sha"].as_str()
                    == Some(&facts.target_tip)
                && (receipt.request.fence.generation == self.generation
                    || a.candidate_sha.as_deref() != Some(&facts.candidate_head))
        });
        if let Some(receipt) = adopted {
            let outcome: OwnerRebaseReceipt =
                serde_json::from_value(receipt.result).map_err(|e| {
                    ServiceError::invalid_operation(format!("invalid rebase receipt: {e}"))
                })?;
            return self
                .apply_rebase(&a, queue, outcome, 0, &facts.target_tip)
                .await;
        }
        if a.candidate_sha.as_deref() != Some(&facts.candidate_head) {
            return self
                .park(
                    &a,
                    ParkReason::Infrastructure,
                    "the candidate moved under the head",
                )
                .await;
        }
        if a.target_tip_sha.as_deref() != Some(&facts.target_tip) {
            // Moved while this head held the slot: nobody in the queue can
            // have done it.
            let tip = facts.target_tip.clone();
            self.w
                .advance(&a.id, S::Rebasing, |a| a.target_tip_sha = Some(tip))
                .await?;
            let a = self.w.attempt(&a.id).await?;
            return Ok(self
                .new_round(&a, Some(IntegrationLostRaceKind::External), true)
                .await?
                .unwrap_or(Pass::Progress));
        }
        let fence = self.fence(&a).await?;
        self.w.fault(S::Rebasing, CrashPoint::BeforeEffect)?;
        let started = self.w.clock.now();
        let cancel = self.stop.child_token();
        let outcome = {
            let effect = self.w.owner.rebase(OwnerRebaseRequest {
                fence,
                workspace: facts.workspace.clone(),
                target_branch: queue.target_branch.clone(),
                expected_head_sha: facts.candidate_head.clone(),
                expected_target_sha: facts.target_tip.clone(),
                handoff_conflicts: true,
                deadline: self.w.config.rebase_deadline,
                cancel: cancel.clone(),
            });
            tokio::pin!(effect);
            // The effect can outlive a lease period: keep the lease, and
            // stop the effect when the Task is cancelled meanwhile.
            let watch = async {
                loop {
                    self.w.clock.sleep(self.w.config.poll).await;
                    if !matches!(
                        self.w.renew_if_due(&self.queue_id, self.generation).await,
                        Ok(true)
                    ) {
                        cancel.cancel();
                    }
                    if self
                        .w
                        .attempt(&a.id)
                        .await
                        .is_ok_and(|a| a.cancel_requested_at.is_some())
                    {
                        cancel.cancel();
                    }
                }
            };
            tokio::select! {
                outcome = &mut effect => outcome,
                () = watch => unreachable!("the watch never returns"),
            }
        };
        let outcome = match outcome {
            Ok(outcome) => outcome,
            // The receipt may or may not exist; the rows decide on the next
            // pass. Returned as an error so that a rebase that keeps failing
            // before its intent is counted and ends the session (the lease
            // then paces the retries) instead of spinning at the backoff.
            Err(error) => return Err(error),
        };
        self.w.fault(S::Rebasing, CrashPoint::AfterEffectReceipt)?;
        let elapsed = millis(started, self.w.clock.now());
        self.apply_rebase(&a, queue, outcome, elapsed, &facts.target_tip)
            .await
    }

    async fn apply_rebase(
        &mut self,
        a: &IntegrationAttempt,
        queue: &IntegrationQueue,
        outcome: OwnerRebaseReceipt,
        elapsed_ms: i64,
        target_tip: &str,
    ) -> Result<Pass> {
        // The receipt bumped the revision: everything below re-reads.
        let a = self.w.attempt(&a.id).await?;
        self.timings(&a.id, |timings| add(&mut timings.rebase_ms, elapsed_ms))
            .await?;
        let a = self.w.attempt(&a.id).await?;
        match outcome {
            OwnerRebaseReceipt::Completed {
                outcome: WorkspaceOwnerOperationOutcome::Rebased,
            } => {
                // The receipt names no commit; read the rebased HEAD.
                let facts = self.facts(&a, queue).await?;
                let head = facts.candidate_head;
                let tip = target_tip.to_owned();
                let a = self
                    .fresh_step(&a, IntegrationStepAction::RequestCheck)
                    .await?;
                self.w
                    .advance(&a.id, S::Checking, |a| {
                        a.candidate_sha = Some(head);
                        a.target_tip_sha = Some(tip);
                        a.checks_json = None;
                        a.checks_commit_sha = None;
                    })
                    .await?;
                Ok(Pass::Progress)
            }
            OwnerRebaseReceipt::Completed {
                outcome:
                    WorkspaceOwnerOperationOutcome::Conflict {
                        details,
                        conflict_paths,
                    },
            } => {
                let paths = json!(conflict_paths);
                let paths = db::validate_integration_paths(&paths)
                    .is_ok()
                    .then_some(paths);
                self.send_back(&a, S::Ejected, |a| {
                    a.conflict_paths_json = paths;
                    a.failure_message = Some(bounded(&details));
                })
                .await
            }
            OwnerRebaseReceipt::Completed {
                outcome: WorkspaceOwnerOperationOutcome::UnsupportedConflict { details },
            } => {
                self.park(&a, ParkReason::UnsupportedConflict, &details)
                    .await
            }
            OwnerRebaseReceipt::Completed {
                outcome: WorkspaceOwnerOperationOutcome::Dirty { files },
            } => {
                self.park(&a, ParkReason::WorktreeDirty, &files.join(", "))
                    .await
            }
            OwnerRebaseReceipt::Completed { .. } => {
                self.park(&a, ParkReason::Infrastructure, "unexpected rebase outcome")
                    .await
            }
            OwnerRebaseReceipt::Infrastructure { .. } => {
                // Uncertain: the intent stays; only a receipt settles it.
                self.w.advance(&a.id, S::Reconciling, |_| {}).await?;
                Ok(Pass::Progress)
            }
            OwnerRebaseReceipt::TimedOut { .. } => {
                self.park(&a, ParkReason::Timeout, "the rebase ran out of time")
                    .await
            }
            OwnerRebaseReceipt::Cancelled { .. } => {
                if a.cancel_requested_at.is_some() {
                    self.release_cancelled(&a).await
                } else {
                    // Shutdown or a lost lease: stop; the next claim rebases.
                    Ok(Pass::Lost)
                }
            }
            OwnerRebaseReceipt::Refused {
                reason: OwnerEffectRefusal::StaleFence,
            } => Ok(Pass::Lost),
            OwnerRebaseReceipt::Refused {
                reason: OwnerEffectRefusal::ForeignOwner,
            } => self.foreign_owner(&a).await,
            OwnerRebaseReceipt::Refused { .. } | OwnerRebaseReceipt::NotPerformed {} => {
                // The witness no longer holds (the target moved while this
                // head held the slot) or this generation already has a rebase
                // receipt: read again in a new round.
                let facts = self.facts(&a, queue).await?;
                let lost = (a.target_tip_sha.as_deref() != Some(&facts.target_tip))
                    .then_some(IntegrationLostRaceKind::External);
                let tip = facts.target_tip;
                self.w
                    .advance(&a.id, S::Rebasing, |a| a.target_tip_sha = Some(tip))
                    .await?;
                let a = self.w.attempt(&a.id).await?;
                Ok(self
                    .new_round(&a, lost, true)
                    .await?
                    .unwrap_or(Pass::Progress))
            }
        }
    }

    async fn await_check(&mut self, a: IntegrationAttempt) -> Result<Pass> {
        if a.cancel_requested_at.is_some() {
            return self.release_cancelled(&a).await;
        }
        if self
            .ensure_step(&a, IntegrationStepAction::RequestCheck)
            .await?
        {
            self.check_asked = Some(self.w.clock.now());
            return Ok(Pass::Progress);
        }
        let Some(ack) = IntegrationStepAck::current(&a, IntegrationStepAction::Settle) else {
            if self.timed_out(HeadTimeout::Check) {
                return self
                    .park(&a, ParkReason::Timeout, "no check verdict arrived")
                    .await;
            }
            if let Some(pass) = self
                .reask_dead_step(&a, IntegrationStepAction::RequestCheck)
                .await?
            {
                self.check_asked = Some(self.w.clock.now());
                return Ok(pass);
            }
            // The verdict reaches the Task by a delivery step of the check
            // runner. One that failed is not delivered again, but the next
            // ask applies the stored verdict (and an ask for a check still
            // running only joins it): ask on a timer instead of waiting the
            // check timeout out.
            let now = self.w.clock.now();
            let asked = *self.check_asked.get_or_insert(now);
            if now >= later(asked, self.w.config.check_reask) {
                self.fresh_step(&a, IntegrationStepAction::RequestCheck)
                    .await?;
                self.check_asked = Some(now);
                return Ok(Pass::Progress);
            }
            return Ok(Pass::Wait(self.w.config.poll));
        };
        if let Some(check) = ack.check.clone() {
            self.timings(&a.id, |timings| timings.check = Some(check))
                .await?;
        }
        let a = self.w.attempt(&a.id).await?;
        match ack.outcome {
            IntegrationStepOutcome::CandidateCheckFailed => self.check_failed(&a, &ack).await,
            IntegrationStepOutcome::Infrastructure => {
                self.park(
                    &a,
                    ParkReason::Infrastructure,
                    ack.message.as_deref().unwrap_or("no check verdict"),
                )
                .await
            }
            IntegrationStepOutcome::TaskLeft => self.release_cancelled(&a).await,
            IntegrationStepOutcome::Permit
            | IntegrationStepOutcome::NeedsReview
            | IntegrationStepOutcome::Done => {
                // The same acknowledgment is the settle decision; the next
                // state consumes it.
                let passed = ack.outcome == IntegrationStepOutcome::Permit;
                let seq = a.effect_seq;
                self.w
                    .advance(&a.id, S::AwaitingTaskStep, |a| {
                        if passed {
                            a.checks_commit_sha = a.candidate_sha.clone();
                            a.checks_json = Some(json!({"result":"passed"}));
                        }
                    })
                    .await?;
                self.enqueued.insert((seq, IntegrationStepAction::Settle));
                Ok(Pass::Progress)
            }
        }
    }
    async fn check_failed(
        &mut self,
        a: &IntegrationAttempt,
        ack: &IntegrationStepAck,
    ) -> Result<Pass> {
        let message = ack.message.clone();
        self.send_back(a, S::Ejected, |a| {
            a.failure_kind = Some(IntegrationFailureKind::CandidateCheckFailed);
            a.failure_message = message.as_deref().map(bounded);
        })
        .await
    }

    async fn await_permit(&mut self, a: IntegrationAttempt) -> Result<Pass> {
        if a.cancel_requested_at.is_some() {
            // No fast-forward intent exists yet, so the cancel wins even over
            // a permit that was already written.
            return self.release_cancelled(&a).await;
        }
        if self.ensure_step(&a, IntegrationStepAction::Settle).await? {
            return Ok(Pass::Progress);
        }
        let Some(ack) = IntegrationStepAck::current(&a, IntegrationStepAction::Settle) else {
            if self.timed_out(HeadTimeout::Step) {
                return self
                    .park(&a, ParkReason::Timeout, "no Task-step decision arrived")
                    .await;
            }
            if let Some(pass) = self
                .reask_dead_step(&a, IntegrationStepAction::Settle)
                .await?
            {
                return Ok(pass);
            }
            return Ok(Pass::Wait(self.w.config.poll));
        };
        let waited = self
            .since
            .map(|(_, since)| millis(since, self.w.clock.now()))
            .unwrap_or(0);
        self.timings(&a.id, |timings| add(&mut timings.step_wait_ms, waited))
            .await?;
        let a = self.w.attempt(&a.id).await?;
        match ack.outcome {
            IntegrationStepOutcome::Permit => {
                let deadline = stamp(later(self.w.clock.now(), self.w.config.permit_lifetime));
                match self
                    .w
                    .advance(&a.id, S::ReadyFf, |a| a.deadline = Some(deadline))
                    .await
                {
                    Ok(_) => Ok(Pass::Progress),
                    // The permit does not bind this candidate, target, Task
                    // entry and slot: ask again, a bounded number of times
                    // (each ask is a Task step).
                    Err(ServiceError::Db(db::DbError::Check(detail))) => {
                        self.refused_permits += 1;
                        if self.refused_permits > 2 {
                            return self.park(&a, ParkReason::Infrastructure, &detail).await;
                        }
                        self.enqueued.clear();
                        Ok(Pass::Wait(self.w.config.poll))
                    }
                    Err(error) => Err(error),
                }
            }
            IntegrationStepOutcome::NeedsReview => {
                let message = ack.message.clone();
                self.send_back(&a, S::NeedsReview, |a| {
                    a.failure_message = message.as_deref().map(bounded);
                })
                .await
            }
            IntegrationStepOutcome::CandidateCheckFailed => self.check_failed(&a, &ack).await,
            IntegrationStepOutcome::TaskLeft => self.release_cancelled(&a).await,
            IntegrationStepOutcome::Infrastructure | IntegrationStepOutcome::Done => {
                self.park(
                    &a,
                    ParkReason::Infrastructure,
                    ack.message
                        .as_deref()
                        .unwrap_or("the Task step could not decide"),
                )
                .await
            }
        }
    }

    async fn commit_fast_forward(
        &mut self,
        a: IntegrationAttempt,
        queue: &IntegrationQueue,
    ) -> Result<Pass> {
        if a.cancel_requested_at.is_some() {
            return self.release_cancelled(&a).await;
        }
        let expired = parse_time(a.deadline.as_deref())
            .is_some_and(|deadline| deadline <= self.w.clock.now())
            || self.timed_out(HeadTimeout::Permit);
        if expired {
            self.w
                .advance(&a.id, S::AwaitingTaskStep, |a| {
                    a.permit_json = None;
                    a.effect_seq += 1;
                    a.effect_ack_json = None;
                    a.acknowledged_at = None;
                    a.deadline = None;
                })
                .await?;
            return Ok(Pass::Progress);
        }
        let facts = match self.facts(&a, queue).await {
            Ok(facts) => facts,
            Err(error) => {
                return self
                    .park(&a, ParkReason::Infrastructure, &error.to_string())
                    .await
            }
        };
        if let Some(pass) = self
            .transfer(&a, queue, &facts, ObjectTransferDirection::Outbound)
            .await?
        {
            return Ok(pass);
        }
        // Cancel-or-commit. The cancel request and this write are both a
        // compare-and-set on the attempt revision, and a cancel is refused
        // once `ff_inflight` is stored: exactly one of them wins.
        let committed = self
            .w
            .advance_if(&a.id, S::FfInflight, |a| a.cancel_requested_at.is_none())
            .await?;
        match committed {
            Some(_) => Ok(Pass::Progress),
            None => {
                let a = self.w.attempt(&a.id).await?;
                self.release_cancelled(&a).await
            }
        }
    }

    async fn fast_forward(
        &mut self,
        a: IntegrationAttempt,
        queue: &IntegrationQueue,
    ) -> Result<Pass> {
        let (Some(candidate), Some(target)) = (a.candidate_sha.clone(), a.target_tip_sha.clone())
        else {
            self.w.advance(&a.id, S::Reconciling, |_| {}).await?;
            return Ok(Pass::Progress);
        };
        if a.effect_intent_json.is_some() {
            self.w.advance(&a.id, S::Reconciling, |_| {}).await?;
            return Ok(Pass::Progress);
        }
        let facts = match self.facts(&a, queue).await {
            Ok(facts) => facts,
            Err(_) => {
                self.w.advance(&a.id, S::Reconciling, |_| {}).await?;
                return Ok(Pass::Progress);
            }
        };
        let fence = self.fence(&a).await?;
        self.w.fault(S::FfInflight, CrashPoint::BeforeEffect)?;
        let started = self.w.clock.now();
        // Never cancelled by shutdown: a started fast-forward runs to its
        // receipt or to the owner's own bound.
        let outcome = self
            .w
            .owner
            .fast_forward(OwnerFastForwardRequest {
                fence,
                workspace: facts.workspace.clone(),
                target_branch: queue.target_branch.clone(),
                task_branch: facts.task_branch.clone(),
                candidate_sha: candidate.clone(),
                target_sha: target.clone(),
                deadline: self.w.config.ff_owner_bound,
            })
            .await;
        self.w
            .fault(S::FfInflight, CrashPoint::AfterEffectReceipt)?;
        let elapsed = millis(started, self.w.clock.now());
        match outcome {
            Ok(OwnerMergeReceipt::Completed {
                outcome:
                    MergeOutcome::Done {
                        before_sha,
                        after_sha,
                        ..
                    },
            }) => {
                self.timings(&a.id, |timings| add(&mut timings.ff_ms, elapsed))
                    .await?;
                self.w
                    .advance(&a.id, S::Applied, |a| {
                        a.integrated_before_sha = Some(before_sha);
                        a.integrated_sha = Some(after_sha);
                        a.deadline = None;
                    })
                    .await?;
                Ok(Pass::Progress)
            }
            Ok(OwnerMergeReceipt::Refused {
                reason: OwnerEffectRefusal::StaleFence,
            }) => Ok(Pass::Lost),
            Ok(OwnerMergeReceipt::Refused {
                reason: OwnerEffectRefusal::ForeignOwner,
            }) => {
                let a = self.w.attempt(&a.id).await?;
                self.foreign_owner(&a).await
            }
            Ok(OwnerMergeReceipt::Completed {
                outcome: MergeOutcome::TargetMoved { .. },
            })
            | Ok(OwnerMergeReceipt::Refused {
                reason: OwnerEffectRefusal::WitnessMismatch,
            }) => {
                let a = self.w.attempt(&a.id).await?;
                let moved = self.facts(&a, queue).await.ok().filter(|facts| {
                    facts.target_tip != target && facts.candidate_head == candidate
                });
                let Some(facts) = moved else {
                    self.w.advance(&a.id, S::Reconciling, |_| {}).await?;
                    return Ok(Pass::Progress);
                };
                // The target moved under a head that held the slot: a writer
                // outside Forge. Rebase again in a new round.
                self.w
                    .advance(&a.id, S::Rebasing, |a| {
                        a.target_tip_sha = Some(facts.target_tip);
                        a.permit_json = None;
                        a.effect_seq += 1;
                        a.effect_ack_json = None;
                        a.acknowledged_at = None;
                        a.deadline = None;
                    })
                    .await?;
                let a = self.w.attempt(&a.id).await?;
                Ok(self
                    .new_round(&a, Some(IntegrationLostRaceKind::External), true)
                    .await?
                    .unwrap_or(Pass::Progress))
            }
            // Settled without a merge, uncertain, or no receipt at all:
            // reconciliation decides from the rows and a Git witness.
            Ok(_) | Err(_) => {
                self.w.advance(&a.id, S::Reconciling, |_| {}).await?;
                Ok(Pass::Progress)
            }
        }
    }

    async fn reconcile(&mut self, a: IntegrationAttempt, queue: &IntegrationQueue) -> Result<Pass> {
        let mut a = a;
        if let Some(intent) = a.effect_intent_json.as_ref() {
            // Only the owner settles an admitted effect, under its own
            // checkout lock: a receipt, an exact proof, or "the ref never
            // moved". The worker records nothing itself.
            let outcome =
                match serde_json::from_value::<IntegrationEffectRequest>(intent["request"].clone())
                {
                    Ok(request) => self.w.owner.reconcile_effect(&request).await,
                    Err(_) => self.w.owner.reconcile_outstanding().await,
                };
            if let Err(error) = outcome {
                tracing::warn!(target: "services::integration_worker", attempt_id = %a.id, %error, "integration owner reconciliation failed");
            }
            a = self.w.attempt(&a.id).await?;
        }
        // Read after the settlement, so the facts are never older than it.
        let facts = self.facts(&a, queue).await;
        if a.effect_intent_json.is_some() {
            // Unknown result. Nothing is guessed: the queue is quarantined
            // with a typed reason and the reconcile timer keeps asking.
            let message = "integration result unknown: the owner has no settled receipt";
            let queue = self.w.queue(&self.queue_id).await?;
            if queue.state == IntegrationQueueState::Open {
                self.w
                    .db
                    .quarantine_integration_queue(
                        &queue.id,
                        queue.revision,
                        &self.w.instance,
                        self.generation,
                        IntegrationFailureKind::NeedsFact,
                        message,
                    )
                    .await?;
            }
            self.enqueue(&a, IntegrationStepAction::Park).await?;
            self.w
                .advance(&a.id, S::Quarantined, |a| {
                    a.failure_kind = Some(IntegrationFailureKind::NeedsFact);
                    a.failure_message = Some(message.into());
                })
                .await?;
            self.w.defer_reconcile(&self.queue_id);
            return Ok(Pass::Lost);
        }
        // Settled. The newest receipt and a Git witness decide.
        let last = receipts(&a).into_iter().next_back();
        let merged = last.as_ref().and_then(|receipt| {
            (receipt.operation_state == IntegrationOperationState::Succeeded
                && matches!(
                    receipt.request.kind,
                    IntegrationOperationKind::FastForward | IntegrationOperationKind::Merge
                ))
            .then(|| serde_json::from_value::<OwnerMergeReceipt>(receipt.result.clone()).ok())
            .flatten()
        });
        if let Some(OwnerMergeReceipt::Completed {
            outcome:
                MergeOutcome::Done {
                    before_sha,
                    after_sha,
                    ..
                },
        }) = merged
        {
            self.w
                .advance(&a.id, S::Applied, |a| {
                    a.integrated_before_sha = Some(before_sha);
                    a.integrated_sha = Some(after_sha);
                    a.failure_kind = None;
                    a.failure_message = None;
                    a.deadline = None;
                })
                .await?;
            self.reopen_if_quarantined(&a.id).await?;
            return Ok(Pass::Progress);
        }
        let facts = match facts {
            Ok(facts) => facts,
            Err(error) => {
                return self
                    .park(&a, ParkReason::Infrastructure, &error.to_string())
                    .await
            }
        };
        if facts.candidate_in_target && a.candidate_sha.as_deref() == Some(&facts.candidate_head) {
            let head = facts.candidate_head.clone();
            self.w
                .advance(&a.id, S::Applied, |a| {
                    a.integrated_sha = Some(head);
                    a.failure_kind = None;
                    a.failure_message = None;
                    a.deadline = None;
                })
                .await?;
            self.reopen_if_quarantined(&a.id).await?;
            return Ok(Pass::Progress);
        }
        let rebased = last.as_ref().is_some_and(|receipt| {
            receipt.request.kind == IntegrationOperationKind::Rebase
                && receipt.operation_state == IntegrationOperationState::Succeeded
                && receipt.request.witness["expected_head_sha"].as_str()
                    == a.candidate_sha.as_deref()
        });
        // Proven not landed: back to validation (or to the rebase whose
        // receipt is still to be applied) at no cost to the Task.
        let to = if rebased { S::Rebasing } else { S::Queued };
        self.w
            .advance(&a.id, to, |a| {
                a.permit_json = None;
                a.deadline = None;
                a.failure_kind = None;
                a.failure_message = None;
            })
            .await?;
        self.reopen_if_quarantined(&a.id).await?;
        let a = self.w.attempt(&a.id).await?;
        // `start` counts the round of a re-queued head itself.
        Ok(self
            .new_round(&a, None, rebased)
            .await?
            .unwrap_or(Pass::Progress))
    }

    async fn reopen_if_quarantined(&self, attempt_id: &str) -> Result<()> {
        let queue = self.w.queue(&self.queue_id).await?;
        if queue.state == IntegrationQueueState::Quarantined {
            let head = self.w.attempt(attempt_id).await?;
            self.w.reopen(&queue, &head).await?;
        }
        Ok(())
    }

    async fn await_result(
        &mut self,
        a: IntegrationAttempt,
        queue: &IntegrationQueue,
    ) -> Result<Pass> {
        let action = IntegrationStepAction::Result;
        if !self.enqueued.contains(&(a.effect_seq, action)) {
            // Idempotent: `settle` pre-enqueued the same causation key.
            self.enqueue(&a, action).await?;
            self.w.steps.ready_result_step(&a.id, a.effect_seq).await?;
        }
        let now = self.w.clock.now();
        let mut done = IntegrationStepAck::current(&a, action).is_some();
        let due = self
            .result_checked
            .is_none_or(|at| now >= later(at, self.w.config.sweep_interval));
        if !done && due {
            // Re-armed on every sweep interval while the attempt is `applied`
            // without its acknowledgment: a `result` step that hit an error
            // parked itself for `result_park`, and the merge has landed.
            self.w.steps.ready_result_step(&a.id, a.effect_seq).await?;
            // The other exit: the Task has left `merging` (the result step
            // advanced it and died before its acknowledgment).
            if self.result_checked.is_some() {
                done = self
                    .facts(&a, queue)
                    .await
                    .is_ok_and(|facts| facts.gate == TaskGate::Left);
            }
            self.result_checked = Some(now);
        }
        if !done {
            return Ok(Pass::Wait(self.w.config.poll));
        }
        self.close_timings(&a).await?;
        self.w.advance(&a.id, S::Completed, |_| {}).await?;
        Ok(Pass::Released)
    }
}
