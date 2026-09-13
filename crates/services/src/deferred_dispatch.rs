use chrono::{DateTime, Utc};
use db::{now_rfc3339, Task, TaskMetadata, TaskMetadataMutation, TaskRepo};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::{Result, ServiceError};

const METADATA_KEY: &str = "deferred_dispatch";
const PAUSED_INTEGRATION_METADATA_KEY: &str = "paused_integration";
// Ordinary metadata mutations intentionally do not advance Task.version.
// Project-level dispatch wakes do advance it for affected rows, forming a
// causal fence against stale disposition writers. This separate generation
// is the paused-integration marker's own fence: clear increments it, and a
// stale producer/retainer carrying the prior generation becomes a no-op.
const PAUSED_INTEGRATION_GENERATION_KEY: &str = "paused_integration_generation";

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct DeferredDispatch {
    pub not_before: String,
    pub reason: String,
    pub target_state: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct PausedIntegration {
    pub state: String,
    pub deferred_at: String,
}

pub(crate) async fn set(
    db: &db::SqliteDb,
    task: &Task,
    target_state: &str,
    not_before: &str,
    reason: &str,
) -> Result<()> {
    TaskRepo::mutate_metadata(
        db,
        &task.id,
        Some(task.version),
        vec![TaskMetadataMutation::Set {
            key: METADATA_KEY.to_owned(),
            value: json!({
                "not_before": not_before,
                "reason": reason,
                "target_state": target_state,
            }),
        }],
        &now_rfc3339(),
    )
    .await?;
    Ok(())
}

pub(crate) async fn clear(db: &db::SqliteDb, task: &Task) -> Result<()> {
    let metadata = TaskMetadata::parse(task.metadata_json.as_deref()).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid task metadata for {}: {error}", task.id))
    })?;
    let Some(expected) = metadata.extra.get(METADATA_KEY).cloned() else {
        return Ok(());
    };
    TaskRepo::mutate_metadata(
        db,
        &task.id,
        None,
        vec![TaskMetadataMutation::RemoveIf {
            key: METADATA_KEY.to_owned(),
            expected,
        }],
        &now_rfc3339(),
    )
    .await?;
    Ok(())
}

pub(crate) fn pending_until(task: &Task) -> Option<DeferredDispatch> {
    let metadata = TaskMetadata::parse(task.metadata_json.as_deref()).ok()?;
    let value = metadata.extra.get(METADATA_KEY)?.clone();
    serde_json::from_value(value).ok()
}

pub(crate) fn is_pending(task: &Task, now: DateTime<Utc>) -> bool {
    let Some(deferred) = pending_until(task) else {
        return false;
    };
    let Ok(not_before) = DateTime::parse_from_rfc3339(&deferred.not_before) else {
        return false;
    };
    now < not_before.with_timezone(&Utc)
}

/// Record an integration transition that reached a Project pause boundary.
///
/// The Task may still be in `review` (a passed review was prevented from
/// entering integration) or already in `merging` (the pause won the final
/// database write race immediately before Git integration). The dispatcher
/// consumes this marker after Project resume and retries the exact state
/// capability without manufacturing another reviewer attempt.
pub(crate) async fn defer_integration_for_pause(db: &db::SqliteDb, task: &Task) -> Result<()> {
    let metadata = TaskMetadata::parse(task.metadata_json.as_deref()).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid task metadata for {}: {error}", task.id))
    })?;
    let generation = metadata
        .extra
        .get(PAUSED_INTEGRATION_GENERATION_KEY)
        .cloned()
        .unwrap_or_else(|| json!(0));
    if generation.as_i64().is_none() {
        return Err(ServiceError::invalid_operation(format!(
            "invalid paused-integration generation for {}",
            task.id
        )));
    }
    TaskRepo::mutate_metadata(
        db,
        &task.id,
        Some(task.version),
        vec![
            // Legacy rows may not have a generation yet. Establish zero in
            // the same transaction before the conditional marker write.
            TaskMetadataMutation::SetIfAbsent {
                key: PAUSED_INTEGRATION_GENERATION_KEY.to_owned(),
                value: generation.clone(),
            },
            TaskMetadataMutation::CompareAndMutate {
                key: PAUSED_INTEGRATION_GENERATION_KEY.to_owned(),
                expected: generation,
                mutations: vec![TaskMetadataMutation::SetIfAbsent {
                    key: PAUSED_INTEGRATION_METADATA_KEY.to_owned(),
                    value: json!({
                        "state": task.status.clone(),
                        "deferred_at": now_rfc3339(),
                    }),
                }],
            },
        ],
        &now_rfc3339(),
    )
    .await
    .map_err(|error| match error {
        db::DbError::NotFound => ServiceError::not_found("task", task.id.clone()),
        error => error.into(),
    })?;
    Ok(())
}

pub(crate) fn paused_integration(task: &Task) -> Option<PausedIntegration> {
    let metadata = TaskMetadata::parse(task.metadata_json.as_deref()).ok()?;
    serde_json::from_value(metadata.extra.get(PAUSED_INTEGRATION_METADATA_KEY)?.clone()).ok()
}

/// Refresh a paused-integration marker only if the exact marker observed in
/// `task` is still present. A recovery worker can otherwise read the marker,
/// lose a race to a successful worker's conditional clear, and then recreate
/// it with an unconditional metadata write.
pub(crate) async fn refresh_paused_integration_for_pause(
    db: &db::SqliteDb,
    task: &Task,
) -> Result<()> {
    let metadata = TaskMetadata::parse(task.metadata_json.as_deref()).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid task metadata for {}: {error}", task.id))
    })?;
    let Some(expected) = metadata.extra.get(PAUSED_INTEGRATION_METADATA_KEY).cloned() else {
        return Ok(());
    };
    let generation = metadata
        .extra
        .get(PAUSED_INTEGRATION_GENERATION_KEY)
        .cloned()
        .unwrap_or_else(|| json!(0));
    if generation.as_i64().is_none() {
        return Err(ServiceError::invalid_operation(format!(
            "invalid paused-integration generation for {}",
            task.id
        )));
    }
    if serde_json::from_value::<PausedIntegration>(expected.clone()).is_err() {
        return Ok(());
    }
    TaskRepo::mutate_metadata(
        db,
        &task.id,
        Some(task.version),
        vec![
            TaskMetadataMutation::SetIfAbsent {
                key: PAUSED_INTEGRATION_GENERATION_KEY.to_owned(),
                value: generation.clone(),
            },
            TaskMetadataMutation::CompareAndMutate {
                key: PAUSED_INTEGRATION_GENERATION_KEY.to_owned(),
                expected: generation,
                mutations: vec![TaskMetadataMutation::SetIf {
                    key: PAUSED_INTEGRATION_METADATA_KEY.to_owned(),
                    expected,
                    value: json!({
                        "state": task.status.clone(),
                        "deferred_at": now_rfc3339(),
                    }),
                }],
            },
        ],
        &now_rfc3339(),
    )
    .await
    .map_err(|error| match error {
        db::DbError::NotFound => ServiceError::not_found("task", task.id.clone()),
        error => error.into(),
    })?;
    Ok(())
}

pub(crate) async fn clear_paused_integration(
    db: &db::SqliteDb,
    task_id: &str,
    expected: &PausedIntegration,
) -> Result<()> {
    TaskRepo::mutate_metadata(
        db,
        task_id,
        None,
        vec![TaskMetadataMutation::CompareAndMutate {
            key: PAUSED_INTEGRATION_METADATA_KEY.to_owned(),
            expected: serde_json::to_value(expected)
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
            mutations: vec![
                TaskMetadataMutation::Remove {
                    key: PAUSED_INTEGRATION_METADATA_KEY.to_owned(),
                },
                TaskMetadataMutation::Increment {
                    key: PAUSED_INTEGRATION_GENERATION_KEY.to_owned(),
                    by: 1,
                },
            ],
        }],
        &now_rfc3339(),
    )
    .await
    .map_err(|error| match error {
        db::DbError::NotFound => ServiceError::not_found("task", task_id.to_owned()),
        error => error.into(),
    })?;
    Ok(())
}

// --- Dispatch disposition (F11: quiescent, capability-aware dispatch) ---
//
// `DeferredDispatch` above is a time-based "don't retry before X" cooldown
// used for backoff after a transient execution failure. It is unrelated to
// this section: a *deterministic* blocker — a governance denial from
// `TaskService::ensure_task_runnable`, an unresolved canonical conflict —
// does not get better with time, so retrying it on a fixed cooldown is
// exactly the infinite churn F11 describes. The ten-second scan re-attempts
// dispatch and re-logs the identical denial forever, and review never runs.
//
// A `DispatchDisposition` instead records the last attempt's outcome keyed by
// the Task's own `version`, the execution capability the scan was attempting,
// and a digest of what blocked it. While the Task's current
// `(version, capability)` still matches a stored disposition, nothing has
// changed since that observation and the scan skips the Task entirely — no
// repeat admission call, no repeat warning, no repeat annotation.
//
// `blocker_digest` is derived from the observed refusal purely so a later
// blocker projection can tell "same blocker" from "different blocker"; it is
// never parsed or matched against known strings, because the denial wording
// is owned by that projection and is expected to keep changing.
//
// A disposition self-invalidates as soon as anything writes the Task row
// through the normal `version`-incrementing path. It does *not* self-
// invalidate for governance state on other tables
// (`project_execution_baseline*`, `project_reconciliation_record`, ...);
// whatever commits one of those changes must call `wake_task_dispatch`.
const DISPOSITION_METADATA_KEY: &str = "dispatch_disposition";

/// The stored record of one dispatch attempt's deterministic refusal. See the
/// notes above for the invalidation contract.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub(crate) struct DispatchDisposition {
    pub task_version: i64,
    pub capability: String,
    pub blocker_digest: String,
    pub recorded_at: String,
    pub safe_message: String,
}

fn dispatch_disposition(task: &Task) -> Option<DispatchDisposition> {
    let metadata = TaskMetadata::parse(task.metadata_json.as_deref()).ok()?;
    serde_json::from_value(metadata.extra.get(DISPOSITION_METADATA_KEY)?.clone()).ok()
}

/// True when a disposition is already recorded for this exact Task version and
/// requested capability — the scan already attempted this and nothing has
/// changed since. Callers skip the repeat attempt and its warning entirely.
pub(crate) fn dispatch_disposition_is_current(task: &Task, capability: &str) -> bool {
    dispatch_disposition(task).is_some_and(|disposition| {
        disposition.task_version == task.version && disposition.capability == capability
    })
}

/// The disposition still in force for this Task, if any.
///
/// A recorded disposition means the dispatcher has stopped attempting this
/// capability and will not reconsider until the Task changes or something
/// wakes it — which is exactly the state a user needs to see, rather than the
/// Task reading as ordinary queueing.
pub(crate) fn current_dispatch_disposition(task: &Task) -> Option<DispatchDisposition> {
    dispatch_disposition(task).filter(|disposition| disposition.task_version == task.version)
}

/// Persist the disposition observed for a dispatch attempt that just failed
/// deterministically. Callers reach this only after
/// `dispatch_disposition_is_current` established there was nothing current to
/// skip, so every call is a genuinely new observation and is safe to log once.
pub(crate) async fn record_dispatch_disposition(
    db: &db::SqliteDb,
    task: &Task,
    capability: &str,
    safe_message: &str,
) -> Result<()> {
    TaskRepo::mutate_metadata(
        db,
        &task.id,
        Some(task.version),
        vec![TaskMetadataMutation::Set {
            key: DISPOSITION_METADATA_KEY.to_owned(),
            value: json!({
                "task_version": task.version,
                "capability": capability,
                "blocker_digest": dispatch_blocker_digest(safe_message),
                "recorded_at": now_rfc3339(),
                "safe_message": bounded_safe_message(safe_message),
            }),
        }],
        &now_rfc3339(),
    )
    .await?;
    Ok(())
}

/// Clear a stored disposition, e.g. once dispatch succeeds again.
pub(crate) async fn clear_dispatch_disposition(db: &db::SqliteDb, task: &Task) -> Result<()> {
    let metadata = TaskMetadata::parse(task.metadata_json.as_deref()).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid task metadata for {}: {error}", task.id))
    })?;
    let Some(expected) = metadata.extra.get(DISPOSITION_METADATA_KEY).cloned() else {
        return Ok(());
    };
    TaskRepo::mutate_metadata(
        db,
        &task.id,
        None,
        vec![TaskMetadataMutation::RemoveIf {
            key: DISPOSITION_METADATA_KEY.to_owned(),
            expected,
        }],
        &now_rfc3339(),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
pub(crate) fn dispatch_disposition_for_test(task: &Task) -> Option<DispatchDisposition> {
    dispatch_disposition(task)
}

fn dispatch_blocker_digest(description: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(description.as_bytes());
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

/// Long enough for a full governance refusal, short enough that a runaway
/// message cannot bloat every Task's metadata row.
const MAX_SAFE_MESSAGE_LEN: usize = 500;

fn bounded_safe_message(message: &str) -> String {
    if message.chars().count() <= MAX_SAFE_MESSAGE_LEN {
        return message.to_owned();
    }
    let truncated: String = message.chars().take(MAX_SAFE_MESSAGE_LEN).collect();
    format!("{truncated}…")
}

/// Wake exactly one Task's dispatch eligibility: clear its stored disposition
/// so the next scan re-attempts it instead of treating it as an unchanged
/// blocker. Also clears any time-based deferral for the same Task.
///
/// A Task's own `version` invalidates a disposition for free, but state living
/// outside the `task` row — reconciliation resolution or an authorized retry
/// — does not touch `version`. Whatever
/// commits one of those must call this afterward, or the previously observed
/// denial keeps the Task quiesced forever.
pub async fn wake_task_dispatch(db: &db::SqliteDb, task_id: &str, reason: &str) -> Result<()> {
    let result = TaskRepo::wake_dispatch_for_task(db, task_id, &now_rfc3339()).await;
    if let Err(db::DbError::NotFound) = &result {
        return Ok(());
    }
    result?;
    tracing::info!(task_id = %task_id, %reason, "task dispatch woken");
    Ok(())
}
