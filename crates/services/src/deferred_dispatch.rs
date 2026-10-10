use chrono::{DateTime, Utc};
use db::{now_rfc3339, Task, TaskMetadata, TaskMetadataMutation, TaskRepo};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::{Result, ServiceError};

const METADATA_KEY: &str = "deferred_dispatch";
pub(crate) const QUEUED_RECOVERY_KEY: &str = "queued_recovery";
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
pub(crate) struct QueuedRecovery {
    pub id: String,
    pub request: QueuedTaskAction,
    pub target_state: String,
    pub error_annotation: Option<String>,
    pub blocked_json: Option<String>,
    #[serde(default)]
    pub failed_json: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct QueuedTaskAction {
    pub action: api_types::TaskAction,
    pub offer: api_types::Offer,
    pub actor: api_types::Actor,
    pub agent_id: Option<String>,
    #[serde(default)]
    pub role_name: Option<String>,
    #[serde(default)]
    pub assignment_id: Option<String>,
}

pub(crate) fn queued_recovery(task: &Task) -> Option<QueuedRecovery> {
    let metadata = TaskMetadata::parse(task.metadata_json.as_deref()).ok()?;
    serde_json::from_value(metadata.extra.get(QUEUED_RECOVERY_KEY)?.clone()).ok()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct PausedIntegration {
    pub state: String,
    pub deferred_at: String,
}

#[cfg(test)]
pub(crate) async fn set(
    db: &db::SqliteDb,
    task: &Task,
    target_state: &str,
    not_before: &str,
    reason: &str,
) -> Result<()> {
    set_with_mutations(db, task, target_state, not_before, reason, Vec::new()).await
}

#[cfg(test)]
pub(crate) async fn set_with_mutations(
    db: &db::SqliteDb,
    task: &Task,
    target_state: &str,
    not_before: &str,
    reason: &str,
    mut mutations: Vec<TaskMetadataMutation>,
) -> Result<()> {
    mutations.push(TaskMetadataMutation::Set {
        key: METADATA_KEY.to_owned(),
        value: json!({
            "not_before": not_before,
            "reason": reason,
            "target_state": target_state,
        }),
    });
    TaskRepo::mutate_metadata(db, &task.id, Some(task.version), mutations, &now_rfc3339()).await?;
    Ok(())
}

pub(crate) async fn set_with_mutations_for_latest_execution(
    db: &db::SqliteDb,
    task: &Task,
    authority: db::LatestExecutionAuthority,
    target_state: &str,
    not_before: &str,
    reason: &str,
    mut mutations: Vec<TaskMetadataMutation>,
) -> Result<bool> {
    mutations.push(TaskMetadataMutation::Set {
        key: METADATA_KEY.to_owned(),
        value: json!({
            "not_before": not_before,
            "reason": reason,
            "target_state": target_state,
        }),
    });
    Ok(
        TaskRepo::mutate_metadata_and_bump_version_for_latest_execution(
            db,
            &task.id,
            task.version,
            authority,
            mutations,
            &now_rfc3339(),
        )
        .await?
        .is_some(),
    )
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
/// The capability of the disposition an accepted action leaves when an
/// unfinished dependency refuses its admission. It holds until the Task
/// changes or is woken (a dependency finished, a link was removed).
pub(crate) const DEPENDENCY_WAIT_CAPABILITY: &str = "dependency_wait";

/// The stored record of one dispatch attempt's deterministic refusal. See the
/// notes above for the invalidation contract.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub(crate) struct DispatchDisposition {
    pub task_version: i64,
    pub capability: String,
    pub blocker_digest: String,
    pub recorded_at: String,
    pub safe_message: String,
    /// What a `machine_capacity` wait is for when it is not a run slot:
    /// `disk`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity_scope: Option<String>,
}

pub(crate) fn dispatch_disposition(task: &Task) -> Option<DispatchDisposition> {
    let metadata = TaskMetadata::parse(task.metadata_json.as_deref()).ok()?;
    serde_json::from_value(metadata.extra.get(DISPOSITION_METADATA_KEY)?.clone()).ok()
}

/// True when a disposition is already recorded for this exact Task version and
/// requested capability — the scan already attempted this and nothing has
/// changed since. Callers skip the repeat attempt and its warning entirely.
pub(crate) fn dispatch_disposition_is_current(task: &Task, capability: &str) -> bool {
    dispatch_disposition(task).is_some_and(|disposition| {
        (disposition.task_version == task.version
            || matches!(
                disposition.capability.as_str(),
                "machine_capacity" | "project_capacity"
            ))
            && disposition.capability == capability
    })
}

/// The disposition still in force for this Task, if any.
///
/// Role dispositions wait for a Task change or explicit wake. The
/// `project_capacity` and `machine_capacity` capabilities explain temporary
/// queueing and are rechecked each tick, since another Task can free a slot.
pub(crate) fn current_dispatch_disposition(task: &Task) -> Option<DispatchDisposition> {
    dispatch_disposition(task).filter(|disposition| {
        disposition.task_version == task.version
            || matches!(
                disposition.capability.as_str(),
                "machine_capacity" | "project_capacity"
            )
    })
}

/// Persist the disposition observed for a dispatch attempt that just failed
/// deterministically. Return whether the stored disposition actually changed;
/// an identical observation preserves its timestamp and produces no refresh.
pub(crate) async fn record_dispatch_disposition(
    db: &db::SqliteDb,
    task: &Task,
    capability: &str,
    safe_message: &str,
) -> Result<bool> {
    record_dispatch_disposition_naming(db, task, capability, safe_message, &[]).await
}

/// The wait message of a Task no machine can take for lack of a run slot.
pub(crate) const MACHINE_CAPACITY_WAIT: &str = "machine_capacity: waiting for a machine run slot";

/// Record that the Task waits for a machine: a run slot, or free space on a
/// workspace filesystem. Either way it is the one machine-capacity wait the
/// dispatcher re-evaluates on every scan, so it clears by itself.
pub(crate) async fn record_capacity_wait(
    db: &db::SqliteDb,
    task: &Task,
    wait: crate::placement::CapacityWait,
) -> Result<bool> {
    match wait {
        crate::placement::CapacityWait::Machine => {
            record_dispatch_disposition(db, task, "machine_capacity", MACHINE_CAPACITY_WAIT).await
        }
        crate::placement::CapacityWait::Disk => {
            let message = disk_wait_message(db).await;
            record_disposition(
                db,
                task,
                "machine_capacity",
                &message,
                &[],
                Some(api_types::CAPACITY_SCOPE_DISK),
            )
            .await
        }
    }
}

/// Which machines are under their free-space floor, and whether anything
/// reclaims space on them. Stable while the facts are: a changing message
/// would rewrite the wait on every scan.
pub(crate) async fn disk_wait_message(db: &db::SqliteDb) -> String {
    let rows = db::machine_disk::list_machine_disks(db)
        .await
        .unwrap_or_default();
    let under: Vec<&db::machine_disk::MachineDiskRow> = rows
        .iter()
        .filter(|row| row.disk.pressure.is_some())
        .collect();
    let names = |rows: &[&db::machine_disk::MachineDiskRow]| {
        rows.iter()
            .map(|row| row.hostname.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    };
    let mut message = if under.is_empty() {
        "disk_pressure: waiting for free space on a workspace filesystem".to_owned()
    } else {
        format!(
            "disk_pressure: waiting for free space on the workspace filesystem of {}",
            names(&under)
        )
    };
    let unowned: Vec<&db::machine_disk::MachineDiskRow> = under
        .iter()
        .copied()
        .filter(|row| row.disk.facts.gc_state.as_deref() != Some(api_types::WORKSPACE_GC_OWNED))
        .collect();
    if !unowned.is_empty() {
        message.push_str(&format!(
            "; workspace garbage collection is not running on {} (its workspace root is not owned by that Forge), so nothing is reclaimed there until an operator frees space or fixes the ownership",
            names(&unowned)
        ));
    }
    message
}

/// [`record_dispatch_disposition`], naming the dependencies the Task waits
/// for (`dependency_ids`), which the typed condition presents.
pub(crate) async fn record_dispatch_disposition_naming(
    db: &db::SqliteDb,
    task: &Task,
    capability: &str,
    safe_message: &str,
    dependency_ids: &[String],
) -> Result<bool> {
    record_disposition(db, task, capability, safe_message, dependency_ids, None).await
}

async fn record_disposition(
    db: &db::SqliteDb,
    task: &Task,
    capability: &str,
    safe_message: &str,
    dependency_ids: &[String],
    capacity_scope: Option<&str>,
) -> Result<bool> {
    let metadata = TaskMetadata::parse(task.metadata_json.as_deref()).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid task metadata for {}: {error}", task.id))
    })?;
    let blocker_digest = dispatch_blocker_digest(safe_message);
    let safe_message = bounded_safe_message(safe_message);
    if !(capability == "machine_capacity" && metadata.extra.contains_key("environment_wait"))
        && dispatch_disposition(task).is_some_and(|disposition| {
            disposition.task_version == task.version
                && disposition.capability == capability
                && disposition.blocker_digest == blocker_digest
                && disposition.safe_message == safe_message
        })
    {
        return Ok(false);
    }
    let mut value = json!({
        "task_version": task.version,
        "capability": capability,
        "blocker_digest": blocker_digest,
        "recorded_at": now_rfc3339(),
        "safe_message": safe_message,
    });
    if !dependency_ids.is_empty() {
        value["dependency_ids"] = json!(dependency_ids);
    }
    if let Some(scope) = capacity_scope {
        value["capacity_scope"] = json!(scope);
    }
    let mutation = match metadata.extra.get(DISPOSITION_METADATA_KEY) {
        Some(expected) => TaskMetadataMutation::SetIf {
            key: DISPOSITION_METADATA_KEY.to_owned(),
            expected: expected.clone(),
            value,
        },
        None => TaskMetadataMutation::SetIfAbsent {
            key: DISPOSITION_METADATA_KEY.to_owned(),
            value,
        },
    };
    let mut mutations = vec![mutation];
    if capability == "machine_capacity" {
        if let Some(wait) = metadata.extra.get("environment_wait") {
            mutations.push(TaskMetadataMutation::RemoveIf {
                key: "environment_wait".to_owned(),
                expected: wait.clone(),
            });
        }
        if let Some(deferred) = metadata.extra.get("deferred_dispatch").filter(|d| {
            d["kind"]
                .as_str()
                .is_some_and(|k| k.starts_with("environment_"))
        }) {
            mutations.push(TaskMetadataMutation::RemoveIf {
                key: "deferred_dispatch".to_owned(),
                expected: deferred.clone(),
            });
        }
    }
    let (_, changed) = TaskRepo::mutate_metadata_with_change(
        db,
        &task.id,
        Some(task.version),
        mutations,
        &now_rfc3339(),
    )
    .await?;
    Ok(changed)
}

/// Clear a stored disposition, e.g. once dispatch succeeds again, returning
/// whether the conditional removal actually changed stored metadata.
pub(crate) async fn clear_dispatch_disposition(db: &db::SqliteDb, task: &Task) -> Result<bool> {
    let metadata = TaskMetadata::parse(task.metadata_json.as_deref()).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid task metadata for {}: {error}", task.id))
    })?;
    let Some(expected) = metadata.extra.get(DISPOSITION_METADATA_KEY).cloned() else {
        return Ok(false);
    };
    let (_, changed) = TaskRepo::mutate_metadata_with_change(
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
    Ok(changed)
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
    // The stored refusal is cleared below exactly as before. The kick makes the
    // reconciler look at the Task even when nothing was stored to clear.
    db.kick_schedule(task_id).await?;
    if !db::task_writer::owns_task(task_id) {
        if TaskRepo::get_by_id(db, task_id, false).await?.is_none() {
            return Ok(());
        }
        // A wake must never be dropped because the Task changed status first.
        db.enqueue_fenced_task_mutation(
            task_id,
            db::TaskMutation::TaskWakeDispatchForTask {
                id: task_id.to_owned(),
                updated_at: now_rfc3339(),
            },
            db::task_writer::EffectFence::Identity,
        )
        .await?;
        tracing::info!(task_id = %task_id, %reason, "task dispatch wake queued");
        return Ok(());
    }
    let result = TaskRepo::wake_dispatch_for_task(db, task_id, &now_rfc3339()).await;
    if let Err(db::DbError::NotFound) = &result {
        return Ok(());
    }
    result?;
    tracing::info!(task_id = %task_id, %reason, "task dispatch woken");
    Ok(())
}

/// Only real dispatch observations replace capacity waits. Ordinary edits keep
/// the parked projection until the dispatcher observes another outcome.
pub(crate) async fn clear_capacity_wait(db: &db::SqliteDb, task: &Task) -> Result<()> {
    if dispatch_disposition(task).is_some_and(|d| {
        matches!(
            d.capability.as_str(),
            "machine_capacity" | "project_capacity"
        )
    }) {
        clear_dispatch_disposition(db, task).await?;
    }
    Ok(())
}

pub(crate) async fn refresh_machine_wait(db: &db::SqliteDb, task: &mut Task) -> Result<()> {
    *task = TaskRepo::get_by_id(db, &task.id, false)
        .await?
        .ok_or_else(|| ServiceError::not_found("task", &task.id))?;
    Ok(())
}

/// A marker older than this transition was not a refusal by its dispatch.
/// A new refusal may be stamped at the final barrier version without trusting
/// an old wait as evidence of current capacity.
pub(crate) async fn finish_machine_wait(
    db: &db::SqliteDb,
    task: &mut Task,
    previous_version: i64,
) -> Result<()> {
    refresh_machine_wait(db, task).await?;
    if let Some(d) = dispatch_disposition(task).filter(|d| {
        matches!(
            d.capability.as_str(),
            "machine_capacity" | "project_capacity"
        )
    }) {
        if d.task_version <= previous_version {
            crate::placement::machine_precheck::retire_wait(db, task).await?;
        } else if d.task_version != task.version {
            record_disposition(
                db,
                task,
                &d.capability,
                &d.safe_message,
                &[],
                d.capacity_scope.as_deref(),
            )
            .await?;
        }
        refresh_machine_wait(db, task).await?;
    }
    Ok(())
}
