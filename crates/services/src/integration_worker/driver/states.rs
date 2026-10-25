//! Head validation, rebase, check, fast-forward, and reconciliation stages.

use super::*;

impl HeadDriver {
    // ----- states ---------------------------------------------------------

    pub(super) async fn start(&mut self, a: IntegrationAttempt) -> Result<Pass> {
        if a.cancel_requested_at.is_some() {
            return self.release_cancelled(&a).await;
        }
        // A member whose Task its owner stopped is skipped here: it leaves
        // the slot before a round is counted or anything is read from Git,
        // and the next member is claimed.
        if let Some(pass) = self.gated(&a).await? {
            return Ok(pass);
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

    pub(super) async fn validate(
        &mut self,
        a: IntegrationAttempt,
        queue: &IntegrationQueue,
    ) -> Result<Pass> {
        if a.cancel_requested_at.is_some() {
            return self.release_cancelled(&a).await;
        }
        // The Task gate first, from storage: a Task its owner stopped leaves
        // the slot before any Git is read or written, and spends no retry.
        if let Some(pass) = self.gated(&a).await? {
            return Ok(pass);
        }
        let started = self.w.clock.now();
        let facts = match self.facts(&a, queue).await {
            Ok(facts) => facts,
            Err(error) if self.timed_out(HeadTimeout::Validate) => {
                return self.park(&a, ParkReason::Timeout, &error.to_string()).await
            }
            Err(error) => {
                return self
                    .park(&a, ParkReason::unreached(&error), &error.to_string())
                    .await
            }
        };
        match facts.gate {
            TaskGate::Live => {}
            TaskGate::Left => return self.release_cancelled(&a).await,
            TaskGate::Gated(cause) => {
                return self
                    .park(&a, ParkReason::Gate(cause), cause.message())
                    .await
            }
        }
        let admitted = a
            .candidate_sha
            .clone()
            .or_else(|| a.original_candidate_sha.clone());
        if facts.rebase_in_progress {
            // A rebase is stopped in the Task's checkout (a merge the old
            // path started and never reported, or an owner that died in the
            // middle of one). Nobody else will finish it: the owner's rebase
            // does, by the interrupted-rebase rule (`git::integration::
            // rebase` continues it and hands a conflict to the Worker, or
            // reports the conflict it cannot hand off). HEAD is the commit
            // the stopped rebase stands on, which is what the owner's
            // witness names. This is read before the landed witness below: a
            // rebase that stopped on its first commit leaves HEAD on the
            // target tip, which is not "the candidate has landed".
            let head = facts.candidate_head.clone();
            let tip = facts.target_tip.clone();
            self.w
                .advance(&a.id, S::Rebasing, |a| {
                    a.candidate_sha = Some(head);
                    a.target_tip_sha = Some(tip);
                })
                .await?;
            return Ok(Pass::Progress);
        }
        if landed(&a, &facts) {
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
            && !rebased_and_not_applied(&a, admitted.as_deref(), &facts)
        {
            return self
                .send_back(&a, S::NeedsReview, |a| {
                    a.failure_message =
                        Some("the candidate commit changed after it was admitted".into());
                })
                .await;
        }
        if facts.worktree_dirty {
            return self
                .send_back(&a, S::Ejected, |a| {
                    a.failure_kind = Some(IntegrationFailureKind::OwnerRequired);
                    a.failure_message =
                        Some("worktree_dirty: the Task checkout has uncommitted changes".into());
                })
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
        // Every attempt of an entry that was imported as a missing-source
        // quarantine owes the head check, not only the attempt the owner's
        // Retry admitted: a successor of that attempt has no more proof of
        // its candidate than it had.
        let retried_incomplete_import = self.w.db.retried_quarantined_import(&a.task_ref).await?;
        let triaged_review = self.w.db.review_ci_passed_by_triage(&a.task_ref).await?;
        let skip_check = facts.target_in_candidate
            && (!triaged_review || checked)
            && (!rebased_by_queue || checked)
            && (!retried_incomplete_import || checked);
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

    pub(super) async fn rebase(
        &mut self,
        a: IntegrationAttempt,
        queue: &IntegrationQueue,
    ) -> Result<Pass> {
        if a.effect_intent_json.is_some() {
            self.w.advance(&a.id, S::Reconciling, |_| {}).await?;
            return Ok(Pass::Progress);
        }
        if a.cancel_requested_at.is_some() {
            return self.release_cancelled(&a).await;
        }
        // No intent exists: a gate that landed since validation stops the
        // rebase before it is asked for.
        if let Some(pass) = self.gated(&a).await? {
            return Ok(pass);
        }
        let facts = match self.facts(&a, queue).await {
            Ok(facts) => facts,
            Err(error) => {
                return self
                    .park(&a, ParkReason::unreached(&error), &error.to_string())
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
        if facts.target_in_candidate && !facts.rebase_in_progress {
            // The candidate is already on this tip: an earlier round of this
            // attempt rebased it and left the slot before its check came
            // back (the Task's owner stopped it meanwhile, or the check found
            // no verdict). There is nothing to rebase: no Git effect is asked
            // for, only the check of the commit that is there.
            if !facts.placement_ready {
                if self.timed_out(HeadTimeout::Step) {
                    return self
                        .park(
                            &a,
                            ParkReason::OwnerOffline,
                            "the Task's checkout is not ready on its machine",
                        )
                        .await;
                }
                return Ok(Pass::Wait(self.w.config.poll));
            }
            let a = self
                .fresh_step(&a, IntegrationStepAction::RequestCheck)
                .await?;
            self.w
                .advance(&a.id, S::Checking, |a| {
                    a.checks_json = None;
                    a.checks_commit_sha = None;
                })
                .await?;
            return Ok(Pass::Progress);
        }
        // In another clone the rebase witness is the ref this generation's
        // inbound transfer binds. Validation transferred under the generation
        // it ran in; a later round of the same head transfers again here
        // (a repeat of a key moves nothing).
        if self.inbound_generation != Some(self.generation) {
            if let Some(pass) = self
                .transfer(&a, queue, &facts, ObjectTransferDirection::Inbound)
                .await?
            {
                return Ok(pass);
            }
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
            Err(error) => {
                let now = self.w.attempt(&a.id).await?;
                let answered = now.effect_intent_json.is_some()
                    || receipts(&now).iter().any(|receipt| {
                        receipt.request.kind == IntegrationOperationKind::Rebase
                            && receipt.request.fence.generation == self.generation
                    });
                if answered {
                    // An intent or a receipt exists: the rows decide on the
                    // next pass (reconcile the unknown result, or adopt the
                    // receipt).
                    return Err(error);
                }
                // The owner was never asked: the request failed before its
                // intent (the owner could not be told the claim, or storage
                // refused the effect). Nothing ran. The head leaves the slot
                // with a typed, counted retry, so the members behind it go
                // on; left in `rebasing` it would be re-claimed at every
                // lease expiry for as long as the cause lasts.
                return self
                    .park(&now, ParkReason::unreached(&error), &error.to_string())
                    .await;
            }
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
                if !facts.placement_ready {
                    // The owner answered for its rebase (it is connected),
                    // but the Task's placement is not `ready` again yet: the
                    // owner reconciliation after a reconnect has not run.
                    // The check owner refuses such a placement for good, so
                    // the check is not asked for until it is ready. The
                    // receipt stays to be adopted on the next pass.
                    if self.timed_out(HeadTimeout::Step) {
                        return self
                            .park(
                                &a,
                                ParkReason::OwnerOffline,
                                "the Task's checkout is not ready on its machine",
                            )
                            .await;
                    }
                    return Ok(Pass::Wait(self.w.config.poll));
                }
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
            } => {
                if let Some(pass) = self.foreign_owner(&a).await? {
                    return Ok(pass);
                }
                let a = self.w.attempt(&a.id).await?;
                Ok(self
                    .new_round(&a, None, true)
                    .await?
                    .unwrap_or(Pass::Progress))
            }
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

    pub(super) async fn await_check(&mut self, a: IntegrationAttempt) -> Result<Pass> {
        if a.cancel_requested_at.is_some() {
            return self.release_cancelled(&a).await;
        }
        // A gate that lands while the check runs: the head leaves the slot
        // and stops waiting. The verdict, when it arrives, finds the attempt
        // parked and is applied to nothing; the check is asked again once the
        // gate has lifted.
        if let Some(pass) = self.gated(&a).await? {
            return Ok(pass);
        }
        if self
            .ensure_step(&a, IntegrationStepAction::RequestCheck)
            .await?
        {
            self.check_asked = Some(self.w.clock.now());
            return Ok(Pass::Progress);
        }
        let Some(ack) = IntegrationStepAck::current(&a, IntegrationStepAction::Settle) else {
            if let Some(authority) = crate::integration_steps::check_authority(&a) {
                if self
                    .w
                    .db
                    .integration_head_triage_pending(
                        &a.task_ref,
                        a.expected_epoch,
                        &authority,
                        &stamp(self.w.clock.now()),
                    )
                    .await?
                {
                    // Each triage stage has its own check deadline. Give its
                    // delivery a fresh head wait instead of timing out on the
                    // original candidate check's 35-minute allowance.
                    self.since = Some((a.state, self.w.clock.now()));
                }
            }
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
                // No verdict because the Task's owner stopped it meanwhile:
                // that is the gate's wait, not an infrastructure retry.
                if let Some(pass) = self.gated(&a).await? {
                    return Ok(pass);
                }
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
            | IntegrationStepOutcome::NotLanded
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
            a.failure_message = message
                .as_deref()
                .map(crate::integration_steps::bounded_ci_failure);
        })
        .await
    }

    pub(super) async fn await_permit(&mut self, a: IntegrationAttempt) -> Result<Pass> {
        if a.cancel_requested_at.is_some() {
            // No fast-forward intent exists yet, so the cancel wins even over
            // a permit that was already written.
            return self.release_cancelled(&a).await;
        }
        // Before the decision is asked for or while it is awaited. A permit
        // that is already written was decided before the gate landed: the
        // `settle` step reads the same gate in the transaction of the permit.
        let permitted = IntegrationStepAck::current(&a, IntegrationStepAction::Settle)
            .is_some_and(|ack| ack.outcome == IntegrationStepOutcome::Permit);
        if !permitted {
            if let Some(pass) = self.gated(&a).await? {
                return Ok(pass);
            }
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
            IntegrationStepOutcome::Infrastructure
            | IntegrationStepOutcome::NotLanded
            | IntegrationStepOutcome::Done => {
                // The step refused the permit under the Task gate.
                if let Some(pass) = self.gated(&a).await? {
                    return Ok(pass);
                }
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

    pub(super) async fn commit_fast_forward(
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
        // The Task's checkout no longer holds the candidate the permit was
        // written for. The permit is dropped and the Task step decides again
        // (it refuses a changed candidate); nothing is sent to the target.
        if !facts.shared_object_store
            && a.candidate_sha.as_deref() != Some(facts.candidate_head.as_str())
        {
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

    pub(super) async fn fast_forward(
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
                if let Some(pass) = self.foreign_owner(&a).await? {
                    return Ok(pass);
                }
                // The refusal is this generation's fast-forward receipt:
                // reconciliation reads it as "not landed" and re-queues the
                // head for a new round.
                self.w.advance(&a.id, S::Reconciling, |_| {}).await?;
                Ok(Pass::Progress)
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

    pub(super) async fn reconcile(
        &mut self,
        a: IntegrationAttempt,
        queue: &IntegrationQueue,
    ) -> Result<Pass> {
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
        if let Some(intent) = a.effect_intent_json.clone() {
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
            // The slot and the lease stay with this session. Leaving here
            // would make the lease expiry the retry clock.
            self.w.defer_reconcile(&self.queue_id);
            let now = self.w.clock.now();
            self.reconcile_at = Some(later(now, self.w.config.reconcile_interval));
            self.owner_link = self.w.owner.owner_connection(intent_owner(&intent));
            return Ok(Pass::Wait(
                self.w.config.poll.min(self.w.config.reconcile_interval),
            ));
        }
        // An unknown result the importer took over: a merge that was in
        // flight the old way when the server was upgraded. It has no fenced
        // intent, so no owner receipt will ever settle it, and storage lets
        // it leave only as `applied` or `quarantined` while the operation is
        // unknown. Git decides once the checkouts can be read: every owner
        // was restarted for the upgrade, so nothing started before it still
        // runs. Until then the head is quarantined with a typed reason and
        // asked again on the reconcile timer; it never loops on a refused
        // write.
        if matches!(
            a.current_operation_state,
            Some(IntegrationOperationState::Running | IntegrationOperationState::Uncertain)
        ) {
            match &facts {
                Ok(facts) => {
                    let landed = landed(&a, facts);
                    let head = facts.candidate_head.clone();
                    a = self
                        .w
                        .advance(&a.id, S::Reconciling, |a| {
                            a.current_operation_state =
                                landed.then_some(IntegrationOperationState::Succeeded);
                            if landed {
                                a.candidate_sha = Some(head);
                            }
                        })
                        .await?;
                }
                Err(error) => {
                    let message = format!(
                        "the result of the merge that was in progress before the upgrade is unknown: {error}"
                    );
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
                                &message,
                            )
                            .await?;
                    }
                    self.enqueue(&a, IntegrationStepAction::Park).await?;
                    self.w
                        .advance(&a.id, S::Quarantined, |a| {
                            a.failure_kind = Some(IntegrationFailureKind::NeedsFact);
                            a.failure_message = Some(bounded(&message));
                        })
                        .await?;
                    self.w.defer_reconcile(&self.queue_id);
                    self.reconcile_at =
                        Some(later(self.w.clock.now(), self.w.config.reconcile_interval));
                    return Ok(Pass::Wait(
                        self.w.config.poll.min(self.w.config.reconcile_interval),
                    ));
                }
            }
        }
        self.reconcile_at = None;
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
        if a.candidate_sha.is_some() && landed(&a, &facts) {
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

    pub(super) async fn await_result(
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
        let answer = IntegrationStepAck::current(&a, action);
        if let Some(answer) = answer
            .as_ref()
            .filter(|answer| answer.outcome == IntegrationStepOutcome::NotLanded)
        {
            // The Task was not reported merged. The recorded result is
            // dropped with the slot; the next validation reads the target.
            let detail = answer
                .message
                .clone()
                .unwrap_or_else(|| "the target does not contain the merged commit".into());
            let a = self
                .w
                .advance(&a.id, S::Applied, |a| {
                    a.integrated_sha = None;
                    a.integrated_before_sha = None;
                })
                .await?;
            return self.park(&a, ParkReason::NotLanded, &detail).await;
        }
        let mut done = answer.is_some();
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
