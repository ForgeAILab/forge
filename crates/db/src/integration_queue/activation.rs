//! Passive activation storage for the integration queue (3.2 stage D1a).
//! Nothing here is called until the queue worker exists (D2). Every write is a
//! compare-and-set on the row's `revision` and bumps it, so a caller holding an
//! older copy of the row must re-read before its next write.
use super::*;

stored_enum!(IntegrationLostRaceKind { QueueMember => "queue_member", External => "external" });
stored_enum!(IntegrationCiSkipReason { TargetUnchanged => "target_unchanged", NoChecksConfigured => "no_checks_configured" });

/// Queue states as data, like the attempt graph. `closed` is terminal.
pub const INTEGRATION_QUEUE_TRANSITIONS: &[(IntegrationQueueState, &[IntegrationQueueState])] = &[
    (
        IntegrationQueueState::Open,
        &[
            IntegrationQueueState::Suspended,
            IntegrationQueueState::Quarantined,
            IntegrationQueueState::Closed,
        ],
    ),
    (
        IntegrationQueueState::Suspended,
        &[IntegrationQueueState::Open, IntegrationQueueState::Closed],
    ),
    (
        IntegrationQueueState::Quarantined,
        &[
            IntegrationQueueState::Open,
            IntegrationQueueState::Suspended,
            IntegrationQueueState::Closed,
        ],
    ),
    (IntegrationQueueState::Closed, &[]),
];
impl IntegrationQueueState {
    pub fn exits(self) -> &'static [Self] {
        INTEGRATION_QUEUE_TRANSITIONS
            .iter()
            .find(|(state, _)| *state == self)
            .expect("total queue transition table")
            .1
    }
}
impl IntegrationAttemptState {
    /// A cancel request is legal exactly where the graph can reach `cancelled`.
    /// The request is only a flag: the worker still performs the transition,
    /// and never from an in-flight or unknown fast-forward.
    pub fn cancel_requestable(self) -> bool {
        self.exits().contains(&Self::Cancelled)
    }
}

/// How a head lost its target: to earlier queue members (free) or to a writer
/// outside Forge (spends the "target moved" allowance).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrationLostRace {
    pub kind: IntegrationLostRaceKind,
    /// The claim round (1-based) that lost.
    pub round: i64,
    pub at: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum IntegrationCheckTiming {
    Ran { slot_wait_ms: i64, run_ms: i64 },
    Skipped { reason: IntegrationCiSkipReason },
}
/// Cumulative over every round of one attempt. The worker is the only writer
/// and replaces the whole document.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrationPhaseTimings {
    #[serde(default)]
    pub rounds: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validate_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rebase_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub check: Option<IntegrationCheckTiming>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_wait_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ff_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_total_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lost_races: Vec<IntegrationLostRace>,
}
/// More lost races than this are folded by the caller; the row stays bounded.
pub const INTEGRATION_LOST_RACES_MAX: usize = 32;
impl IntegrationPhaseTimings {
    /// Lost races that spend the "target moved" allowance.
    pub fn external_target_moves(&self) -> usize {
        self.lost_races
            .iter()
            .filter(|race| race.kind == IntegrationLostRaceKind::External)
            .count()
    }
    fn validate(&self) -> Result<()> {
        let mut durations = vec![
            Some(self.rounds),
            self.queued_ms,
            self.validate_ms,
            self.transfer_ms,
            self.rebase_ms,
            self.step_wait_ms,
            self.ff_ms,
            self.head_total_ms,
        ];
        if let Some(IntegrationCheckTiming::Ran {
            slot_wait_ms,
            run_ms,
        }) = &self.check
        {
            durations.extend([Some(*slot_wait_ms), Some(*run_ms)]);
        }
        if durations.into_iter().flatten().any(|value| value < 0)
            || self.lost_races.len() > INTEGRATION_LOST_RACES_MAX
            || self.lost_races.iter().any(|race| race.round < 1)
        {
            return Err(DbError::Check("invalid integration timings".into()));
        }
        for race in &self.lost_races {
            integration_time(&race.at)?;
        }
        Ok(())
    }
}

/// Durable evidence that lets a queue leave `quarantined` or `suspended`.
/// It is re-verified against the stored rows; the caller's word is not trusted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum IntegrationQueueReopenWitness {
    /// A settled (succeeded or failed, never uncertain) receipt of this queue,
    /// recorded by the owner on reconnect + lookup or by a machine / location
    /// removal settlement. Leaves `quarantined`.
    SettledEffect {
        attempt_id: String,
        generation: i64,
        operation: IntegrationOperationKind,
    },
    /// The repo's one ready default checkout, as resolved now. Leaves `suspended`.
    TargetReady {
        location_id: String,
        generation: i64,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrationAge {
    pub count: i64,
    /// Whole seconds; `None` when `count` is 0.
    pub oldest_seconds: Option<i64>,
}
/// Operator status input. Queue-state counts stay in `IntegrationQueueCounts`:
/// a queue row has no "entered this state" time (a lease renewal rewrites
/// `updated_at`), so no age is claimed for suspended / quarantined queues.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrationQueueAges {
    /// Current `queued` attempts, aged from `enqueued_at`.
    pub queued: IntegrationAge,
    /// Head attempts, aged from `started_at` (else the attempt's last write).
    pub heads: IntegrationAge,
    /// Heads whose lease has run out, aged from `lease_until`.
    pub expired_heads: IntegrationAge,
    /// Current `parked` attempts, aged from their last write.
    pub parked: IntegrationAge,
    /// Current attempts with a cancel request not yet applied.
    pub cancel_requested: IntegrationAge,
    /// Current attempts whose effect intent has no settled receipt, aged from the last
    /// intent / receipt write.
    pub unsettled_effects: IntegrationAge,
}

pub(super) const CLAIMABLE_QUEUES_SQL: &str = "SELECT q.* FROM integration_queue q WHERE q.id>? AND q.lease_until IS NULL AND ((q.head_attempt_id IS NOT NULL AND q.state<>'closed') OR q.state='quarantined' OR (q.state IN ('open','suspended') AND EXISTS(SELECT 1 FROM integration_attempt a WHERE a.queue_id=q.id AND a.current=1 AND a.state='queued' AND a.cancel_requested_at IS NULL AND (a.available_at IS NULL OR julianday(a.available_at)<=julianday(?))))) ORDER BY q.id LIMIT ?";
pub(super) const EXPIRED_HEADS_SQL: &str = "SELECT * FROM integration_queue INDEXED BY integration_queue_expired_lease WHERE lease_until IS NOT NULL AND julianday(lease_until)<=julianday(?) AND id>? ORDER BY id LIMIT ?";
pub(super) const DUE_PARKED_SQL: &str = "SELECT * FROM integration_attempt INDEXED BY integration_attempt_timed WHERE current=1 AND available_at IS NOT NULL AND state='parked' AND id>? AND julianday(available_at)<=julianday(?) ORDER BY id LIMIT ?";
pub(super) const CANCEL_REQUESTED_SQL: &str = "SELECT * FROM integration_attempt INDEXED BY integration_attempt_cancel_requested WHERE current=1 AND cancel_requested_at IS NOT NULL AND id>? ORDER BY id LIMIT ?";
pub(super) const PRUNE_SQL: &str = "UPDATE integration_attempt SET effect_receipts_json='[]',operation_receipts_json='[]',observations_json='[]',revision=revision+1,updated_at=? WHERE id IN (SELECT a.id FROM integration_attempt a INDEXED BY integration_attempt_prunable WHERE a.completed_at IS NOT NULL AND (a.effect_receipts_json<>'[]' OR a.operation_receipts_json<>'[]' OR a.observations_json<>'[]') AND a.completed_at<? AND julianday(a.completed_at)<julianday(?) AND a.current=0 AND a.state IN ('completed','cancelled','superseded') AND a.effect_intent_json IS NULL AND COALESCE(a.current_operation_state,'') NOT IN ('pending','running','uncertain') AND NOT EXISTS(SELECT 1 FROM json_each(a.effect_receipts_json) r WHERE json_extract(r.value,'$.operation_state')='uncertain') AND NOT EXISTS(SELECT 1 FROM integration_attempt live WHERE live.task_ref=a.task_ref AND live.current=1) AND NOT EXISTS(SELECT 1 FROM integration_queue q WHERE q.id=a.queue_id AND (q.state='quarantined' OR q.head_attempt_id=a.id)) ORDER BY a.completed_at,a.id LIMIT ?)";
pub(super) const TIMING_SAMPLES_SQL: &str = "SELECT phase_timings_json FROM integration_attempt INDEXED BY integration_attempt_retention WHERE state='completed' AND completed_at>=? AND phase_timings_json IS NOT NULL ORDER BY completed_at DESC LIMIT ?";

fn page(limit: u32) -> u32 {
    limit.clamp(1, 500)
}

/// The Task step that cancels a Task calls this inside its own transaction,
/// next to its Task write. Idempotent: a second request keeps the first time
/// and does not bump the revision.
pub async fn request_integration_cancel_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    attempt_id: &str,
    expected_revision: i64,
    requested_at: &str,
) -> Result<IntegrationAttempt> {
    integration_time(requested_at)?;
    let mut a = attempt_in_tx(tx, attempt_id).await?;
    if a.revision != expected_revision {
        return Err(DbError::VersionConflict);
    }
    if !a.current || !a.state.cancel_requestable() {
        return Err(DbError::InvalidTransition);
    }
    if a.cancel_requested_at.is_some() {
        return Ok(a);
    }
    let n = sqlx::query("UPDATE integration_attempt SET cancel_requested_at=?,updated_at=?,revision=revision+1 WHERE id=? AND revision=?")
        .bind(requested_at)
        .bind(requested_at)
        .bind(attempt_id)
        .bind(expected_revision)
        .execute(&mut **tx)
        .await?
        .rows_affected();
    if n != 1 {
        return Err(DbError::VersionConflict);
    }
    a.cancel_requested_at = Some(requested_at.to_owned());
    a.updated_at = requested_at.to_owned();
    a.revision += 1;
    Ok(a)
}

fn age(row: &sqlx::sqlite::SqliteRow) -> Result<IntegrationAge> {
    let count: i64 = row.try_get("n")?;
    let oldest: Option<f64> = row.try_get("age")?;
    Ok(IntegrationAge {
        count,
        oldest_seconds: oldest.map(|seconds| seconds.max(0.0).round() as i64),
    })
}

#[async_trait]
pub trait IntegrationActivationRepo: Send + Sync {
    /// Flag a current attempt for cancellation. `InvalidTransition` when the
    /// attempt is not current or its state cannot reach `cancelled`
    /// (`ff_inflight`, `reconciling`, `applied`, `quarantined`, terminal).
    /// A queued attempt carrying the flag is never claimed as head.
    async fn request_integration_cancel(
        &self,
        attempt_id: &str,
        expected_revision: i64,
        requested_at: &str,
    ) -> Result<IntegrationAttempt>;
    /// Replace the head's timings document. `InvalidTransition` unless the
    /// attempt is the reserved head of its queue.
    async fn record_integration_head_timings(
        &self,
        attempt_id: &str,
        expected_revision: i64,
        timings: &IntegrationPhaseTimings,
    ) -> Result<IntegrationAttempt>;
    /// Unleased queues a worker must act on, in `id` order after
    /// `after_queue_id`: an open / suspended queue with a due queued member or
    /// a reserved head, and every quarantined queue (claim it when its head is
    /// `reconciling` / `ff_inflight`, otherwise re-open it with a witness).
    async fn claimable_integration_queues(
        &self,
        now: &str,
        after_queue_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<IntegrationQueue>>;
    /// Queues whose head lease ended at or before `now`, in `id` order.
    async fn expired_integration_heads(
        &self,
        now: &str,
        after_queue_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<IntegrationQueue>>;
    /// Current `parked` attempts whose `available_at` is due, in `id` order.
    /// A parked attempt with no `available_at` waits for its owner and is
    /// never returned.
    async fn due_parked_integration_attempts(
        &self,
        now: &str,
        after_attempt_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<IntegrationAttempt>>;
    /// Current attempts carrying a cancel request, in `id` order.
    async fn cancel_requested_integration_attempts(
        &self,
        after_attempt_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<IntegrationAttempt>>;
    /// The live lease holder starts another round for its head: a new fence
    /// generation and lease, with the same takeover rules as a claim (a ready
    /// permit is dropped, an in-flight effect must reconcile). One generation
    /// admits one receipt per effect kind, so a second rebase or fast-forward
    /// of the same head needs this. `VersionConflict` for a stale revision, a
    /// foreign or expired lease, a different fence, or a target that is not
    /// ready (the queue is then left suspended, as a claim leaves it).
    async fn start_integration_round(
        &self,
        queue_id: &str,
        expected_revision: i64,
        owner: &str,
        fence_generation: i64,
        now: &str,
        lease_until: &str,
    ) -> Result<IntegrationQueue>;
    /// The lease holder marks its queue `quarantined` because the head's
    /// result is unknown. `VersionConflict` for a stale revision, owner or
    /// fence; `InvalidTransition` unless the queue is `open` with a head that
    /// is `ff_inflight` / `reconciling` / `quarantined` or has an unsettled
    /// effect.
    async fn quarantine_integration_queue(
        &self,
        queue_id: &str,
        expected_revision: i64,
        owner: &str,
        fence_generation: i64,
        kind: IntegrationFailureKind,
        message: &str,
    ) -> Result<IntegrationQueue>;
    /// Leave `quarantined` (with `SettledEffect`) or `suspended` (with
    /// `TargetReady`). `InvalidTransition` for any other state / witness
    /// pairing, `DbError::Check` when the witness does not match the stored
    /// rows or an effect of the queue is still unsettled. A quarantined queue
    /// whose target is not ready becomes `suspended`, not `open`. Lease and
    /// head are left as they are.
    async fn reopen_integration_queue(
        &self,
        queue_id: &str,
        expected_revision: i64,
        witness: &IntegrationQueueReopenWitness,
    ) -> Result<IntegrationQueue>;
    /// Empty the receipts and observations of at most `limit` attempts that
    /// reached a terminal state before `completed_before`. Skips an attempt
    /// with an intent, a pending / running / uncertain operation, an uncertain
    /// receipt, a live successor of the same Task, or a quarantined queue.
    /// Returns the number pruned; call again while it equals `limit`.
    async fn prune_integration_evidence(&self, completed_before: &str, limit: u32) -> Result<u64>;
    async fn integration_queue_ages(&self, now: &str) -> Result<IntegrationQueueAges>;
    /// Timings of attempts completed at or after `completed_since`, newest first.
    async fn integration_timing_samples(
        &self,
        completed_since: &str,
        limit: u32,
    ) -> Result<Vec<IntegrationPhaseTimings>>;
}

#[async_trait]
impl IntegrationActivationRepo for SqliteDb {
    async fn request_integration_cancel(
        &self,
        attempt_id: &str,
        expected_revision: i64,
        requested_at: &str,
    ) -> Result<IntegrationAttempt> {
        let mut tx = begin_immediate(self.pool()).await?;
        let a =
            request_integration_cancel_in_tx(&mut tx, attempt_id, expected_revision, requested_at)
                .await?;
        tx.commit().await?;
        Ok(a)
    }
    async fn record_integration_head_timings(
        &self,
        attempt_id: &str,
        expected_revision: i64,
        timings: &IntegrationPhaseTimings,
    ) -> Result<IntegrationAttempt> {
        timings.validate()?;
        let json = serde_json::to_string(timings).map_err(|e| DbError::Check(e.to_string()))?;
        let mut tx = begin_immediate(self.pool()).await?;
        let mut a = attempt_in_tx(&mut tx, attempt_id).await?;
        if a.revision != expected_revision {
            return Err(DbError::VersionConflict);
        }
        let head: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM integration_queue WHERE id=? AND head_attempt_id=?)",
        )
        .bind(&a.queue_id)
        .bind(attempt_id)
        .fetch_one(&mut *tx)
        .await?;
        if !head || !a.current {
            return Err(DbError::InvalidTransition);
        }
        let now = now_rfc3339();
        let n = sqlx::query("UPDATE integration_attempt SET phase_timings_json=?,updated_at=?,revision=revision+1 WHERE id=? AND revision=?")
            .bind(json)
            .bind(&now)
            .bind(attempt_id)
            .bind(expected_revision)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        if n != 1 {
            return Err(DbError::VersionConflict);
        }
        tx.commit().await?;
        a.phase_timings = Some(timings.clone());
        a.updated_at = now;
        a.revision += 1;
        Ok(a)
    }
    async fn claimable_integration_queues(
        &self,
        now: &str,
        after_queue_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<IntegrationQueue>> {
        integration_time(now)?;
        sqlx::query(CLAIMABLE_QUEUES_SQL)
            .bind(after_queue_id.unwrap_or(""))
            .bind(now)
            .bind(page(limit))
            .fetch_all(self.pool())
            .await?
            .into_iter()
            .map(map_queue)
            .collect()
    }
    async fn expired_integration_heads(
        &self,
        now: &str,
        after_queue_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<IntegrationQueue>> {
        integration_time(now)?;
        sqlx::query(EXPIRED_HEADS_SQL)
            .bind(now)
            .bind(after_queue_id.unwrap_or(""))
            .bind(page(limit))
            .fetch_all(self.pool())
            .await?
            .into_iter()
            .map(map_queue)
            .collect()
    }
    async fn due_parked_integration_attempts(
        &self,
        now: &str,
        after_attempt_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<IntegrationAttempt>> {
        integration_time(now)?;
        sqlx::query(DUE_PARKED_SQL)
            .bind(after_attempt_id.unwrap_or(""))
            .bind(now)
            .bind(page(limit))
            .fetch_all(self.pool())
            .await?
            .into_iter()
            .map(map_attempt)
            .collect()
    }
    async fn cancel_requested_integration_attempts(
        &self,
        after_attempt_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<IntegrationAttempt>> {
        sqlx::query(CANCEL_REQUESTED_SQL)
            .bind(after_attempt_id.unwrap_or(""))
            .bind(page(limit))
            .fetch_all(self.pool())
            .await?
            .into_iter()
            .map(map_attempt)
            .collect()
    }
    async fn start_integration_round(
        &self,
        queue_id: &str,
        expected_revision: i64,
        owner: &str,
        fence_generation: i64,
        now: &str,
        lease_until: &str,
    ) -> Result<IntegrationQueue> {
        let mut tx = begin_immediate(self.pool()).await?;
        let round = claim_queue_in_tx(
            &mut tx,
            queue_id,
            expected_revision,
            owner,
            now,
            lease_until,
            Some(fence_generation),
        )
        .await?;
        tx.commit().await?;
        round.ok_or(DbError::VersionConflict)
    }
    async fn quarantine_integration_queue(
        &self,
        queue_id: &str,
        expected_revision: i64,
        owner: &str,
        fence_generation: i64,
        kind: IntegrationFailureKind,
        message: &str,
    ) -> Result<IntegrationQueue> {
        if message.len() > 4096 {
            return Err(DbError::Check(
                "integration diagnostic exceeds bound".into(),
            ));
        }
        let mut tx = begin_immediate(self.pool()).await?;
        let q = queue_in_tx(&mut tx, queue_id).await?;
        if q.revision != expected_revision
            || q.lease_owner.as_deref() != Some(owner)
            || q.fence_generation != fence_generation
        {
            return Err(DbError::VersionConflict);
        }
        let Some(head) = q.head_attempt_id.as_deref().filter(|_| {
            q.state != IntegrationQueueState::Quarantined
                && q.state
                    .exits()
                    .contains(&IntegrationQueueState::Quarantined)
        }) else {
            return Err(DbError::InvalidTransition);
        };
        let head = attempt_in_tx(&mut tx, head).await?;
        if !matches!(
            head.state,
            IntegrationAttemptState::FfInflight
                | IntegrationAttemptState::Reconciling
                | IntegrationAttemptState::Quarantined
        ) && head.effect_intent_json.is_none()
            && !matches!(
                head.current_operation_state,
                Some(IntegrationOperationState::Running | IntegrationOperationState::Uncertain)
            )
        {
            return Err(DbError::InvalidTransition);
        }
        let n = sqlx::query("UPDATE integration_queue SET state='quarantined',last_error_kind=?,last_error=?,revision=revision+1,updated_at=? WHERE id=? AND revision=?")
            .bind(kind.to_string())
            .bind(message)
            .bind(now_rfc3339())
            .bind(queue_id)
            .bind(expected_revision)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        if n != 1 {
            return Err(DbError::VersionConflict);
        }
        let q = queue_in_tx(&mut tx, queue_id).await?;
        tx.commit().await?;
        Ok(q)
    }
    async fn reopen_integration_queue(
        &self,
        queue_id: &str,
        expected_revision: i64,
        witness: &IntegrationQueueReopenWitness,
    ) -> Result<IntegrationQueue> {
        let mut tx = begin_immediate(self.pool()).await?;
        let q = queue_in_tx(&mut tx, queue_id).await?;
        if q.revision != expected_revision {
            return Err(DbError::VersionConflict);
        }
        let target = resolve_integration_target_in_tx(&mut tx, &q.repo_id).await?;
        match (q.state, witness) {
            (
                IntegrationQueueState::Quarantined,
                IntegrationQueueReopenWitness::SettledEffect {
                    attempt_id,
                    generation,
                    operation,
                },
            ) => {
                let raw: Option<String> = sqlx::query_scalar(
                    "SELECT effect_receipts_json FROM integration_attempt WHERE id=? AND queue_id=?",
                )
                .bind(attempt_id)
                .bind(queue_id)
                .fetch_optional(&mut *tx)
                .await?;
                let receipts: Vec<IntegrationEffectReceipt> = raw
                    .map(|raw| serde_json::from_str(&raw))
                    .transpose()
                    .map_err(|e| DbError::Check(format!("invalid effect receipts: {e}")))?
                    .unwrap_or_default();
                let settled = receipts.iter().any(|receipt| {
                    receipt.request.fence.queue_id == queue_id
                        && &receipt.request.fence.attempt_id == attempt_id
                        && receipt.request.fence.generation == *generation
                        && receipt.request.kind == *operation
                        && matches!(
                            receipt.operation_state,
                            IntegrationOperationState::Succeeded
                                | IntegrationOperationState::Failed
                        )
                });
                if !settled {
                    return Err(DbError::Check(
                        "integration reopen witness names no settled receipt of this queue".into(),
                    ));
                }
                let unsettled: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM integration_attempt WHERE queue_id=? AND current=1 AND (effect_intent_json IS NOT NULL OR current_operation_state IN ('running','uncertain')))")
                    .bind(queue_id)
                    .fetch_one(&mut *tx)
                    .await?;
                if unsettled {
                    return Err(DbError::Check(
                        "integration queue still has an unsettled effect".into(),
                    ));
                }
            }
            (
                IntegrationQueueState::Suspended,
                IntegrationQueueReopenWitness::TargetReady {
                    location_id,
                    generation,
                },
            ) => {
                let owner = target.owner.as_ref();
                if target.failure.is_some()
                    || target.location_id.as_deref() != Some(location_id)
                    || owner.and_then(|owner| owner["generation"].as_i64()) != Some(*generation)
                {
                    return Err(DbError::Check(
                        "integration reopen witness is not the repo's ready default checkout"
                            .into(),
                    ));
                }
            }
            _ => return Err(DbError::InvalidTransition),
        }
        let state = if target.failure.is_some() {
            IntegrationQueueState::Suspended
        } else {
            IntegrationQueueState::Open
        };
        if !q.state.exits().contains(&state) {
            return Err(DbError::InvalidTransition);
        }
        let n = sqlx::query("UPDATE integration_queue SET state=?,target_location_id=?,target_owner_json=?,last_error_kind=?,last_error=NULL,revision=revision+1,updated_at=? WHERE id=? AND revision=?")
            .bind(state.to_string())
            .bind(&target.location_id)
            .bind(target.owner.as_ref().map(ToString::to_string))
            .bind(target.failure.map(|failure| failure.to_string()))
            .bind(now_rfc3339())
            .bind(queue_id)
            .bind(expected_revision)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        if n != 1 {
            return Err(DbError::VersionConflict);
        }
        let q = queue_in_tx(&mut tx, queue_id).await?;
        tx.commit().await?;
        Ok(q)
    }
    async fn prune_integration_evidence(&self, completed_before: &str, limit: u32) -> Result<u64> {
        integration_time(completed_before)?;
        let mut tx = begin_immediate(self.pool()).await?;
        let pruned = sqlx::query(PRUNE_SQL)
            .bind(now_rfc3339())
            .bind(completed_before)
            .bind(completed_before)
            .bind(page(limit))
            .execute(&mut *tx)
            .await?
            .rows_affected();
        tx.commit().await?;
        Ok(pruned)
    }
    async fn integration_queue_ages(&self, now: &str) -> Result<IntegrationQueueAges> {
        integration_time(now)?;
        let mut ages = IntegrationQueueAges::default();
        // One statement, one snapshot; attempt arms read current rows only.
        for row in sqlx::query("SELECT 'queued' kind,COUNT(*) n,MAX((julianday(?1)-julianday(enqueued_at))*86400.0) age FROM integration_attempt WHERE current=1 AND state='queued' \
            UNION ALL SELECT 'heads',COUNT(*),MAX((julianday(?1)-julianday(COALESCE(a.started_at,a.updated_at)))*86400.0) FROM integration_queue q JOIN integration_attempt a ON a.id=q.head_attempt_id \
            UNION ALL SELECT 'expired_heads',COUNT(*),MAX((julianday(?1)-julianday(lease_until))*86400.0) FROM integration_queue WHERE lease_until IS NOT NULL AND julianday(lease_until)<=julianday(?1) \
            UNION ALL SELECT 'parked',COUNT(*),MAX((julianday(?1)-julianday(updated_at))*86400.0) FROM integration_attempt WHERE current=1 AND state='parked' \
            UNION ALL SELECT 'cancel_requested',COUNT(*),MAX((julianday(?1)-julianday(cancel_requested_at))*86400.0) FROM integration_attempt WHERE current=1 AND cancel_requested_at IS NOT NULL \
            UNION ALL SELECT 'unsettled_effects',COUNT(*),MAX((julianday(?1)-julianday(updated_at))*86400.0) FROM integration_attempt WHERE current=1 AND effect_intent_json IS NOT NULL")
            .bind(now)
            .fetch_all(self.pool())
            .await?
        {
            let kind: String = row.try_get("kind")?;
            let value = age(&row)?;
            match kind.as_str() {
                "queued" => ages.queued = value,
                "heads" => ages.heads = value,
                "expired_heads" => ages.expired_heads = value,
                "parked" => ages.parked = value,
                "cancel_requested" => ages.cancel_requested = value,
                _ => ages.unsettled_effects = value,
            }
        }
        Ok(ages)
    }
    async fn integration_timing_samples(
        &self,
        completed_since: &str,
        limit: u32,
    ) -> Result<Vec<IntegrationPhaseTimings>> {
        integration_time(completed_since)?;
        sqlx::query_scalar::<_, String>(TIMING_SAMPLES_SQL)
            .bind(completed_since)
            .bind(page(limit))
            .fetch_all(self.pool())
            .await?
            .into_iter()
            .map(|raw| {
                serde_json::from_str(&raw)
                    .map_err(|e| DbError::Check(format!("invalid integration timings: {e}")))
            })
            .collect()
    }
}
