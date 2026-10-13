//! What a Task step may write on its attempt: the acknowledgment, the permit
//! and the cancel flag, each inside the step's own Task-write transaction.
//! The queue worker only reads these fields.
use super::*;

/// A missing-execution/workspace import that owns no queue/effect and has no
/// successor. Wholly unreadable or contradictory evidence keeps its own
/// continuation policy; it is not a candidate-availability owner park.
/// `a` is the attempt alias. Match the Task entry as well: history must never
/// park a later entry, or compete with any admitted successor.
pub const UNOWNED_QUARANTINED_IMPORT: &str = "a.current=0 AND a.state='quarantined' AND a.failure_kind='corrupt_import' AND json_extract(a.import_source_json,'$.decision.reason')='missing execution or pinned workspace' AND a.effect_intent_json IS NULL AND a.operation_id IS NULL AND a.current_operation_state IS NULL AND NOT EXISTS(SELECT 1 FROM integration_attempt successor WHERE successor.task_ref=a.task_ref AND successor.id<>a.id) AND EXISTS(SELECT 1 FROM task entry WHERE entry.id=a.task_ref AND entry.status=a.expected_status AND entry.status_epoch=a.expected_epoch AND entry.deleted_at IS NULL)";

impl SqliteDb {
    pub async fn unowned_quarantined_import(
        &self,
        task_id: &str,
    ) -> Result<Option<IntegrationAttempt>> {
        sqlx::query(&format!("SELECT a.* FROM integration_attempt a WHERE a.task_ref=? AND ({UNOWNED_QUARANTINED_IMPORT})"))
            .bind(task_id).fetch_optional(self.pool()).await?.map(map_attempt).transpose()
    }
}

/// The Task's present merge entry was imported as a missing-source
/// quarantine and its owner has retried it since: an attempt of the same
/// entry names that quarantine as its predecessor (only the admission of an
/// unowned quarantine writes that link). `?1` is the Task id.
///
/// Every admission of that entry, not only the first Retry's, may use the
/// Task's present workspace, and every attempt of it owes the head check:
/// the legacy execution that would prove the candidate does not exist.
const RETRIED_QUARANTINED_IMPORT: &str = "SELECT EXISTS(SELECT 1 FROM task t JOIN integration_attempt q ON q.task_ref=t.id AND q.expected_status=t.status AND q.expected_epoch=t.status_epoch WHERE t.id=?1 AND t.deleted_at IS NULL AND q.current=0 AND q.state='quarantined' AND q.failure_kind='corrupt_import' AND json_extract(q.import_source_json,'$.decision.reason')='missing execution or pinned workspace' AND EXISTS(SELECT 1 FROM integration_attempt s WHERE s.task_ref=q.task_ref AND s.predecessor_attempt_id=q.id AND s.expected_status=q.expected_status AND s.expected_epoch=q.expected_epoch))";

impl SqliteDb {
    /// See [`RETRIED_QUARANTINED_IMPORT`].
    pub async fn retried_quarantined_import(&self, task_id: &str) -> Result<bool> {
        Ok(sqlx::query_scalar(RETRIED_QUARANTINED_IMPORT)
            .bind(task_id)
            .fetch_one(self.pool())
            .await?)
    }
}

/// One acknowledgment, written with the Task write it answers.
#[derive(Debug, Clone, PartialEq)]
pub struct IntegrationStepAckWrite {
    pub attempt_id: String,
    /// The fence generation the asking worker held.
    pub generation: i64,
    pub effect_seq: i64,
    /// False only for `result`: the fast-forward it reports is done, whoever
    /// holds the lease now.
    pub bind_generation: bool,
    /// States the attempt may be in for this answer to apply.
    pub states: Vec<IntegrationAttemptState>,
    /// The serialized acknowledgment; its `action` names what it answers.
    pub ack: Value,
    /// Written with a `settle` permit only.
    pub permit: Option<Value>,
    pub acknowledged_at: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IntegrationStepAckOutcome {
    Written(IntegrationAttempt),
    /// The same `(effect_seq, action)` was already answered: nothing is
    /// written (a redelivered step).
    AlreadyAcknowledged(IntegrationAttempt),
    /// The attempt moved past this `effect_seq` or generation, or is no
    /// longer current: the step finishes without a write.
    Stale(IntegrationAttempt),
    /// The attempt has not reached a state this answer applies to yet (a
    /// step is enqueued before its attempt transition): the step retries.
    NotYet(IntegrationAttempt),
}

/// The decision of [`acknowledge_integration_step_in_tx`] without the write,
/// so a step can read the attempt it is about to answer in its transaction.
pub fn integration_step_position(
    a: &IntegrationAttempt,
    generation: i64,
    effect_seq: i64,
    bind_generation: bool,
    action: &str,
    states: &[IntegrationAttemptState],
) -> IntegrationStepPosition {
    let answered = a.acknowledged_at.is_some()
        && a.effect_ack_json.as_ref().is_some_and(|ack| {
            ack["effect_seq"].as_i64() == Some(effect_seq) && ack["action"].as_str() == Some(action)
        });
    if answered && a.effect_seq == effect_seq {
        return IntegrationStepPosition::Answered;
    }
    if a.effect_seq != effect_seq
        || (bind_generation && a.slot_generation != generation)
        || a.state.terminal()
        || !a.current
    {
        return IntegrationStepPosition::Stale;
    }
    if !states.contains(&a.state) {
        return IntegrationStepPosition::NotYet;
    }
    IntegrationStepPosition::Due
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrationStepPosition {
    Due,
    Answered,
    Stale,
    NotYet,
}

fn permit_binds(a: &IntegrationAttempt, permit: &Value) -> bool {
    let text = |value: &Option<String>| value.as_deref().filter(|sha| !sha.is_empty()).is_some();
    permit.is_object()
        && text(&a.candidate_sha)
        && text(&a.target_tip_sha)
        && permit["candidate_sha"].as_str() == a.candidate_sha.as_deref()
        && permit["target_tip_sha"].as_str() == a.target_tip_sha.as_deref()
        && permit["task_ref"].as_str() == Some(&a.task_ref)
        && permit["expected_epoch"].as_i64() == Some(a.expected_epoch)
        && permit["slot_generation"].as_i64() == Some(a.slot_generation)
}

/// The attempt as this transaction sees it.
pub async fn integration_attempt_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    attempt_id: &str,
) -> Result<IntegrationAttempt> {
    attempt_in_tx(tx, attempt_id).await
}

/// The Task's current attempt as this transaction sees it.
pub async fn current_integration_attempt_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    task_id: &str,
) -> Result<Option<IntegrationAttempt>> {
    sqlx::query("SELECT * FROM integration_attempt WHERE task_ref=? AND current=1")
        .bind(task_id)
        .fetch_optional(&mut **tx)
        .await?
        .map(map_attempt)
        .transpose()
}

/// Write `effect_ack_json`, `acknowledged_at` and (for a permit) `permit_json`
/// in the caller's transaction. A compare-and-set on the attempt's revision,
/// `effect_seq` and, unless `bind_generation` is false, its slot generation.
///
/// `DbError::Check`: the acknowledgment is malformed, or the permit does not
/// bind this candidate, target tip, Task entry and slot, or the attempt is
/// flagged for cancellation (Cancel won). `VersionConflict`:
/// the row changed under this transaction (impossible under `BEGIN
/// IMMEDIATE`; kept as the fence).
pub async fn acknowledge_integration_step_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    write: &IntegrationStepAckWrite,
) -> Result<IntegrationStepAckOutcome> {
    integration_time(&write.acknowledged_at)?;
    let action = write.ack["action"]
        .as_str()
        .ok_or_else(|| DbError::Check("integration acknowledgment names no action".into()))?;
    if write.ack["effect_seq"].as_i64() != Some(write.effect_seq)
        || write.ack["generation"].as_i64() != Some(write.generation)
    {
        return Err(DbError::Check(
            "integration acknowledgment does not name its effect_seq and generation".into(),
        ));
    }
    let mut a = attempt_in_tx(tx, &write.attempt_id).await?;
    match integration_step_position(
        &a,
        write.generation,
        write.effect_seq,
        write.bind_generation,
        action,
        &write.states,
    ) {
        IntegrationStepPosition::Answered => {
            return Ok(IntegrationStepAckOutcome::AlreadyAcknowledged(a))
        }
        IntegrationStepPosition::Stale => return Ok(IntegrationStepAckOutcome::Stale(a)),
        IntegrationStepPosition::NotYet => return Ok(IntegrationStepAckOutcome::NotYet(a)),
        IntegrationStepPosition::Due => {}
    }
    if let Some(permit) = &write.permit {
        if a.cancel_requested_at.is_some() {
            return Err(DbError::Check(
                "integration permit refused: the Task asked to cancel this attempt".into(),
            ));
        }
        if !permit_binds(&a, permit) {
            return Err(DbError::Check(
                "integration permit does not bind this candidate, target, Task entry and slot"
                    .into(),
            ));
        }
    }
    let n = sqlx::query("UPDATE integration_attempt SET effect_ack_json=?,acknowledged_at=?,permit_json=COALESCE(?,permit_json),updated_at=?,revision=revision+1 WHERE id=? AND revision=? AND effect_seq=? AND current=1")
        .bind(write.ack.to_string())
        .bind(&write.acknowledged_at)
        .bind(write.permit.as_ref().map(ToString::to_string))
        .bind(&write.acknowledged_at)
        .bind(&a.id)
        .bind(a.revision)
        .bind(write.effect_seq)
        .execute(&mut **tx)
        .await?
        .rows_affected();
    if n != 1 {
        return Err(DbError::VersionConflict);
    }
    a.effect_ack_json = Some(write.ack.clone());
    a.acknowledged_at = Some(write.acknowledged_at.clone());
    if write.permit.is_some() {
        a.permit_json = write.permit.clone();
    }
    a.updated_at = write.acknowledged_at.clone();
    a.revision += 1;
    Ok(IntegrationStepAckOutcome::Written(a))
}

/// The protected `result` step woke at its own deadline and the permit was
/// never used: take the permit and its acknowledgment back so the waiting
/// owner command can run. A compare-and-set against the worker's
/// `ready_ff -> ff_inflight`: exactly one of the two commits. `false`: the
/// attempt is no longer waiting on that permit (nothing is written).
pub async fn revoke_integration_permit_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    attempt_id: &str,
    effect_seq: i64,
    revoked_at: &str,
) -> Result<bool> {
    integration_time(revoked_at)?;
    let n = sqlx::query("UPDATE integration_attempt SET permit_json=NULL,effect_ack_json=NULL,acknowledged_at=NULL,updated_at=?,revision=revision+1 WHERE id=? AND current=1 AND effect_seq=? AND state IN ('awaiting_task_step','ready_ff') AND permit_json IS NOT NULL")
        .bind(revoked_at)
        .bind(attempt_id)
        .bind(effect_seq)
        .execute(&mut **tx)
        .await?
        .rows_affected();
    Ok(n == 1)
}

/// What a Task-level Cancel found on the Task's current attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskIntegrationCancel {
    /// The Task has no current attempt.
    NoAttempt,
    /// The flag is set (or already was): the worker releases the attempt.
    Requested,
    /// `ff_inflight`, `reconciling`, `applied` or `quarantined` with a live
    /// protected `result` step: the Cancel waits behind that step.
    Protected,
    /// One of those four states with no protected step (a shadow or imported
    /// row nothing drives): the Cancel proceeds and nothing is written.
    Undriven,
    /// The attempt held no slot and no owner effect was outstanding: it is
    /// cancelled in the Cancel's own transaction.
    Cancelled,
}

/// The Task's cancel transition (and the board move into the cancellation
/// state). An attempt that holds no slot of its queue and has no owner effect
/// outstanding is ended here, in the transition's own transaction, instead of
/// at the worker's next sweep: a server start in between would find a live
/// attempt behind a terminal Task and import the old merge intent a second
/// time. It carries no cancel request, so the worker's release-step sweep
/// reads it and asks for the `clear` no worker asked for.
///
/// Everything else is [`request_task_integration_cancel_in_tx`]: an attempt
/// in the slot stays its head driver's (it is flagged), and so does one with
/// an admitted effect, which only its owner's receipt settles.
pub async fn cancel_task_integration_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    task_id: &str,
    requested_at: &str,
) -> Result<TaskIntegrationCancel> {
    integration_time(requested_at)?;
    if let Ok(Some(mut a)) = current_integration_attempt_in_tx(tx, task_id).await {
        let holds_slot: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM integration_queue WHERE head_attempt_id=?)",
        )
        .bind(&a.id)
        .fetch_one(&mut **tx)
        .await?;
        if !holds_slot
            && a.cancel_requested_at.is_none()
            && a.effect_intent_json.is_none()
            // The states of a member that waits its turn or its time. Every
            // other cancellable state is a head's, whatever the queue row
            // says at this instant.
            && matches!(
                a.state,
                IntegrationAttemptState::Queued | IntegrationAttemptState::Parked
            )
            && !matches!(
                a.current_operation_state,
                Some(IntegrationOperationState::Running | IntegrationOperationState::Uncertain)
            )
        {
            a.state = IntegrationAttemptState::Cancelled;
            a.permit_json = None;
            a.available_at = None;
            a.deadline = None;
            a.current = false;
            a.updated_at = requested_at.to_owned();
            a.completed_at = Some(requested_at.to_owned());
            super::update_attempt(tx, &a).await?;
            return Ok(TaskIntegrationCancel::Cancelled);
        }
    }
    request_task_integration_cancel_in_tx(tx, task_id, requested_at).await
}

/// Called by the Task's cancel transition inside its own transaction. Never
/// fails a Cancel for a reason of its own: only `Protected` asks it to wait.
pub async fn request_task_integration_cancel_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    task_id: &str,
    requested_at: &str,
) -> Result<TaskIntegrationCancel> {
    let a = match current_integration_attempt_in_tx(tx, task_id).await {
        Ok(Some(a)) => a,
        Ok(None) => return Ok(TaskIntegrationCancel::NoAttempt),
        // A row that cannot be read is a row nothing can drive (the worker
        // quarantines it): it never holds a Cancel back.
        Err(DbError::Check(_)) => return Ok(TaskIntegrationCancel::Undriven),
        Err(error) => return Err(error),
    };
    match request_integration_cancel_in_tx(tx, &a.id, a.revision, requested_at).await {
        // Not a reason to refuse the Cancel: nothing is written.
        Err(DbError::Check(_)) => Ok(TaskIntegrationCancel::Undriven),
        Ok(_) => Ok(TaskIntegrationCancel::Requested),
        Err(DbError::InvalidTransition) => {
            let protected: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_step WHERE task_id=? AND kind='integration' AND integration_started_at IS NOT NULL AND status IN ('pending','claimed'))")
                .bind(task_id)
                .fetch_one(&mut **tx)
                .await?;
            Ok(if protected {
                TaskIntegrationCancel::Protected
            } else {
                TaskIntegrationCancel::Undriven
            })
        }
        Err(error) => Err(error),
    }
}

impl SqliteDb {
    pub async fn integration_passed_review_ids(&self, task_id: &str) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar(
            "SELECT id FROM review WHERE task_id=? AND status='passed' ORDER BY attempt_number DESC,id DESC LIMIT 16",
        ).bind(task_id).fetch_all(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn integration_passed_review_entry_consumer(
        &self,
        task_id: &str,
        identity_key: &str,
        review_id: &str,
    ) -> crate::Result<Option<(String, i64)>> {
        Ok(sqlx::query_as(
                "SELECT c.id,c.status_epoch FROM check_consumer c JOIN check_result r ON r.id=c.result_id JOIN check_run run ON run.id=c.run_id WHERE c.task_id=?1 AND c.origin='entry' AND c.identity_key=?2 AND c.cancelled_at IS NULL AND r.outcome='pass' AND r.identity_key=?2 AND run.identity_key=?2 AND run.state='succeeded' AND substr(c.request_key,length(c.request_key)-length(?3))='|'||?3 ORDER BY c.created_at DESC,c.id DESC LIMIT 1",
            ).bind(task_id).bind(identity_key).bind(review_id).fetch_optional(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn integration_latest_review_details_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<Option<(String, String)>> {
        Ok(sqlx::query_as("SELECT id,step_results_json FROM review WHERE task_id=? ORDER BY attempt_number DESC,id DESC LIMIT 1").bind(task_id).fetch_optional(connection).await?)
    }
}

impl SqliteDb {
    pub async fn integration_check_consumer_origin_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        consumer_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT origin FROM check_consumer WHERE id=?")
                .bind(consumer_id)
                .fetch_optional(connection)
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn integration_first_check_consumer_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        run_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar(
            "SELECT id FROM check_consumer WHERE run_id=? ORDER BY created_at,rowid LIMIT 1",
        )
        .bind(run_id)
        .fetch_optional(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn annotate_integration_review_details_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        details_json: String,
        updated_at: String,
        review_id: String,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(
            sqlx::query("UPDATE review SET step_results_json=?,updated_at=? WHERE id=?")
                .bind(details_json)
                .bind(updated_at)
                .bind(review_id)
                .execute(connection)
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn integration_step_task_left_entry(&self, attempt_id: &str) -> crate::Result<bool> {
        Ok(sqlx::query_scalar(
                    "SELECT NOT EXISTS(SELECT 1 FROM integration_attempt a JOIN task t ON t.id=a.task_ref WHERE a.id=? AND t.status=a.expected_status AND t.status_epoch=a.expected_epoch AND t.deleted_at IS NULL)",
                ).bind(attempt_id).fetch_one(self.pool()).await?)
    }
}
