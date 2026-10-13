//! Bounded observations on an attempt (the repository form). The queue is
//! the authority: nothing observes the Task-step path into an attempt any
//! more, so no writer but the queue worker and its Task steps moves an
//! attempt's revision.
use super::*;

/// An attempt keeps its first observation and the most recent
/// `INTEGRATION_OBSERVATION_RECENT`; older ones are counted in
/// `observations_dropped`. A Task that loops through rebase or review cannot
/// grow the row without limit.
pub const INTEGRATION_OBSERVATION_RECENT: i64 = 15;
/// Path lists inside one observation are evidence, not the full set.
pub const INTEGRATION_OBSERVATION_PATHS: usize = 256;
/// Serialized bound of one observation; larger ones lose their path lists.
pub const INTEGRATION_OBSERVATION_BYTES: usize = 16 * 1024;

/// The append is ONE statement: no read of the row into Rust, no rewrite of
/// the other columns. Binds, in order: cap, observation JSON, cap.
const APPEND_SET: &str = "observations_json=json_insert(CASE WHEN json_array_length(observations_json)>=? THEN json_remove(observations_json,'$[1]') ELSE observations_json END,'$[#]',json(?)),observations_dropped=observations_dropped+(json_array_length(observations_json)>=?)";
/// Replay guard. Bind: observation identity.
const NOT_RECORDED: &str = "NOT EXISTS(SELECT 1 FROM json_each(integration_attempt.observations_json) WHERE json_extract(value,'$.identity')=?)";

/// Facts the existing Task-step owner already learned. Identity makes replay
/// idempotent; absent SHAs/path sets mean unobserved, never an inferred fact.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrationObservation {
    pub identity: String,
    pub step_id: Option<String>,
    pub kind: IntegrationOutcomeKind,
    pub candidate_sha: Option<String>,
    pub target_tip_sha: Option<String>,
    pub changed_paths: Option<Vec<String>>,
    pub conflict_paths: Option<Vec<String>>,
    /// A path list was cut to the observation bound; it is not the full set.
    #[serde(default)]
    pub paths_truncated: bool,
    pub recorded_at: String,
}
impl IntegrationObservation {
    pub fn new(identity: String, kind: IntegrationOutcomeKind) -> Self {
        Self {
            identity,
            kind,
            step_id: None,
            candidate_sha: None,
            target_tip_sha: None,
            changed_paths: None,
            conflict_paths: None,
            paths_truncated: false,
            recorded_at: now_rfc3339(),
        }
    }
    /// Validate and bound the observation; returns the JSON that is stored.
    fn stored(&self) -> Result<String> {
        if self.identity.is_empty() {
            return Err(DbError::Check("observation identity missing".into()));
        }
        let mut bounded = self.clone();
        for paths in [&mut bounded.changed_paths, &mut bounded.conflict_paths]
            .into_iter()
            .flatten()
        {
            if paths.len() > INTEGRATION_OBSERVATION_PATHS {
                paths.truncate(INTEGRATION_OBSERVATION_PATHS);
                bounded.paths_truncated = true;
            }
            validate_integration_paths(&serde_json::json!(paths))?;
        }
        let mut json = serde_json::to_string(&bounded).expect("observation serializes");
        if json.len() > INTEGRATION_OBSERVATION_BYTES {
            bounded.changed_paths = None;
            bounded.conflict_paths = None;
            bounded.paths_truncated = true;
            json = serde_json::to_string(&bounded).expect("observation serializes");
        }
        if json.len() > INTEGRATION_OBSERVATION_BYTES {
            return Err(DbError::Check("observation exceeds bound".into()));
        }
        Ok(json)
    }
}
/// Repository form, by attempt id. A replayed identity is a no-op when the
/// retained content matches and an `IdempotencyConflict` when it differs.
pub(super) async fn record_observation_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    attempt_id: &str,
    observation: &IntegrationObservation,
) -> Result<()> {
    let json = observation.stored()?;
    // Observations never grant authority, claim a slot, or drive state.
    let recorded = sqlx::query(&format!("UPDATE integration_attempt SET {APPEND_SET},updated_at=?,revision=revision+1 WHERE id=? AND {NOT_RECORDED}"))
        .bind(INTEGRATION_OBSERVATION_RECENT + 1)
        .bind(&json)
        .bind(INTEGRATION_OBSERVATION_RECENT + 1)
        .bind(&observation.recorded_at)
        .bind(attempt_id)
        .bind(&observation.identity)
        .execute(&mut **tx)
        .await?
        .rows_affected();
    if recorded == 1 {
        return Ok(());
    }
    let retained: String =
        sqlx::query_scalar("SELECT observations_json FROM integration_attempt WHERE id=?")
            .bind(attempt_id)
            .fetch_optional(&mut **tx)
            .await?
            .ok_or(DbError::NotFound)?;
    let mut next = parse_json(json)?;
    next.as_object_mut()
        .expect("typed observation")
        .remove("recorded_at");
    let same = parse_json(retained)?
        .as_array()
        .into_iter()
        .flatten()
        .filter(|o| o["identity"].as_str() == Some(&observation.identity))
        .any(|o| {
            let mut existing = o.clone();
            if let Some(object) = existing.as_object_mut() {
                object.remove("recorded_at");
            }
            existing == next
        });
    if same {
        Ok(())
    } else {
        Err(DbError::IdempotencyConflict)
    }
}

/// The passive-stage (`shadow:`) attempt that is still the *current* row of
/// a Task that was deleted (`task.deleted_at` is set, or the Task row is
/// gone; nothing in Forge undeletes a Task). Only a row at rest: one that
/// waits (`queued`, `path_wait`, `parked`, `quarantined`, `ejected`,
/// `needs_review`), is no queue's head and records no effect (no intent, no
/// operation that runs or is unknown). A row in any state in which an owner
/// may have been asked for an effect (`validating` to `applied`) is kept as
/// evidence, and so is every history row (`current=0`): a successor's
/// `predecessor_attempt_id` is therefore never nulled by this delete, and a
/// terminal row (which a Review's `integration_checks` or a Task's lineage
/// may name) is never touched. Walks the partial index of current rows
/// (`integration_attempt_current_task`), not the attempt history.
pub(super) const ORPHAN_SHADOW_ATTEMPTS: &str = "FROM integration_attempt INDEXED BY integration_attempt_current_task \
     WHERE current=1 AND admission_key LIKE 'shadow:%' \
       AND state IN ('queued','path_wait','parked','quarantined','ejected','needs_review') \
       AND effect_intent_json IS NULL \
       AND (current_operation_state IS NULL OR current_operation_state NOT IN ('running','uncertain')) \
       AND NOT EXISTS(SELECT 1 FROM task t WHERE t.id=integration_attempt.task_ref AND t.deleted_at IS NULL) \
       AND NOT EXISTS(SELECT 1 FROM integration_queue q WHERE q.head_attempt_id=integration_attempt.id)";

/// Current pre-queue attempts (imported rows and shadow rows) that are
/// `queued`, of Tasks that still exist, without their continuation step.
/// Read by the queue worker's sweep every 30 s: it seeks the `queued` rows
/// (`integration_attempt_retention`; the live members of the queues, not the
/// history) and probes the unique `(task_id, causation_key)` of the steps.
pub(super) const QUEUED_WITHOUT_CONTINUATION: &str =
    "SELECT a.id FROM integration_attempt a INDEXED BY integration_attempt_retention \
     WHERE a.state='queued' AND a.current=1 \
       AND (a.import_source_json IS NOT NULL OR a.admission_key LIKE 'shadow:%') \
       AND EXISTS(SELECT 1 FROM task t WHERE t.id=a.task_ref AND t.deleted_at IS NULL) \
       AND NOT EXISTS(SELECT 1 FROM task_step s WHERE s.task_id=a.task_ref \
                      AND s.causation_key='integration-import:'||a.id) \
     ORDER BY a.id";

impl crate::SqliteDb {
    /// Delete the current `shadow:` attempts of deleted Tasks that are at
    /// rest ([`ORPHAN_SHADOW_ATTEMPTS`]). Such a row gets no continuation
    /// step (the Task is gone), holds no slot and is never superseded:
    /// nothing else ends it. Returns the rows deleted.
    pub async fn delete_orphan_shadow_attempts(&self) -> crate::Result<u64> {
        Ok(sqlx::query(&format!("DELETE {ORPHAN_SHADOW_ATTEMPTS}"))
            .execute(self.pool())
            .await?
            .rows_affected())
    }
    /// The ids [`QUEUED_WITHOUT_CONTINUATION`] lists.
    pub async fn queued_imports_without_continuation(&self) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar(QUEUED_WITHOUT_CONTINUATION)
            .fetch_all(self.pool())
            .await?)
    }
}
