//! What a Task step may write on its attempt: the acknowledgment, the permit
//! and the cancel flag, each inside the step's own Task-write transaction.
//! The queue worker only reads these fields.
use super::*;

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
/// bind this candidate, target tip, Task entry and slot. `VersionConflict`:
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
}

/// Called by the Task's cancel transition inside its own transaction. Never
/// fails a Cancel for a reason of its own: only `Protected` asks it to wait.
pub async fn request_task_integration_cancel_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    task_id: &str,
    requested_at: &str,
) -> Result<TaskIntegrationCancel> {
    let Some(a) = current_integration_attempt_in_tx(tx, task_id).await? else {
        return Ok(TaskIntegrationCancel::NoAttempt);
    };
    match request_integration_cancel_in_tx(tx, &a.id, a.revision, requested_at).await {
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
