use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

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

// The latest target read stays in the current Task step's memory until that
// step's existing result transaction records it. Reading a target adds no
// database transaction, Git command, checkpoint, event, lease or queue action
// to the merge path. One slot: a step runs one hook at a time.
struct TargetRead {
    step_id: String,
    hook_index: i64,
    candidate_sha: Option<String>,
    target_tip_sha: String,
}
tokio::task_local! { static TARGET_READ: std::cell::RefCell<Option<TargetRead>>; }
pub(crate) async fn with_observation_buffer<T>(future: impl std::future::Future<Output = T>) -> T {
    TARGET_READ
        .scope(std::cell::RefCell::new(None), future)
        .await
}
/// Remember the target tip the merge path just read for `step_id`'s hook.
/// A no-op outside that step's own scope; never touches the database.
pub fn note_integration_target(
    step_id: &str,
    hook_index: i64,
    candidate_sha: Option<&str>,
    target_tip_sha: &str,
) {
    if !crate::task_writer::owns_step(step_id) {
        return;
    }
    let _ = TARGET_READ.try_with(|slot| {
        *slot.borrow_mut() = Some(TargetRead {
            step_id: step_id.to_owned(),
            hook_index,
            candidate_sha: candidate_sha.map(str::to_owned),
            target_tip_sha: target_tip_sha.to_owned(),
        });
    });
}
fn target_read(step_id: &str, hook_index: i64) -> Option<(Option<String>, String)> {
    TARGET_READ
        .try_with(|slot| {
            slot.borrow()
                .as_ref()
                .filter(|read| read.step_id == step_id && read.hook_index == hook_index)
                .map(|read| (read.candidate_sha.clone(), read.target_tip_sha.clone()))
        })
        .ok()
        .flatten()
}

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
/// Shadow form: one statement on the Task's current attempt, found through
/// the `integration_attempt_current_task` partial unique index. Returns
/// whether a row took the observation (false: no current attempt, or replay).
async fn append_for_task(
    tx: &mut Transaction<'_, Sqlite>,
    task_id: &str,
    observation: &IntegrationObservation,
    candidate_sha: Option<&str>,
) -> Result<bool> {
    let json = observation.stored()?;
    Ok(sqlx::query(&format!("UPDATE integration_attempt SET {APPEND_SET},candidate_sha=COALESCE(?,candidate_sha),updated_at=?,revision=revision+1 WHERE task_ref=? AND current=1 AND {NOT_RECORDED}"))
        .bind(INTEGRATION_OBSERVATION_RECENT + 1)
        .bind(&json)
        .bind(INTEGRATION_OBSERVATION_RECENT + 1)
        .bind(candidate_sha)
        .bind(&observation.recorded_at)
        .bind(task_id)
        .bind(&observation.identity)
        .execute(&mut **tx)
        .await?
        .rows_affected()
        == 1)
}

/// Every shadow write is optional. A site records with ONE statement, so a
/// constraint or statement failure undoes only that statement (SQLite's
/// statement atomicity) and the authoritative result still commits; only the
/// once-per-merge-entry admission needs several statements and a savepoint.
/// Each site has already written in its transaction, so it holds the write
/// lock and a recording statement cannot wait on another writer. There is no
/// runtime feature flag.
impl SqliteDb {
    pub(crate) async fn observe_integration_hook_best_effort(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        step: &crate::TaskStep,
        index: i64,
        key: &str,
        value: &str,
    ) {
        if !matches!(
            key,
            "merge_intent" | "merge_outcome" | "rebase_target" | "rebase_outcome"
        ) {
            return;
        }
        if let Err(error) = observe_hook_in_tx(tx, step, index, key, value).await {
            log_shadow_failure(key, &step.task_id, &error);
        }
    }
    /// Runs in every step settlement, so it is exactly one indexed statement
    /// that matches nothing unless the Task has a current attempt and is
    /// terminal. An uncertain effect, or a reserved queue head, stays pinned:
    /// a Task's status alone never proves a non-effect.
    pub(crate) async fn observe_integration_terminal_best_effort(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        step: &crate::TaskStep,
    ) {
        const RESOLVED: &str = "state NOT IN ('reconciling','ff_inflight','quarantined') AND COALESCE(current_operation_state,'') NOT IN ('running','uncertain') AND NOT EXISTS(SELECT 1 FROM integration_queue q WHERE q.head_attempt_id=integration_attempt.id)";
        let mut observation = IntegrationObservation::new(
            format!("{}:terminal", step.id),
            IntegrationOutcomeKind::Done,
        );
        observation.step_id = Some(step.id.clone());
        let result = async {
            let json = observation.stored()?;
            sqlx::query(&format!("UPDATE integration_attempt SET observations_json=json_insert(CASE WHEN json_array_length(observations_json)>=?1 THEN json_remove(observations_json,'$[1]') ELSE observations_json END,'$[#]',json_set(json(?2),'$.kind',(SELECT status FROM task WHERE id=?3))),observations_dropped=observations_dropped+(json_array_length(observations_json)>=?1),state=CASE WHEN {RESOLVED} THEN (SELECT CASE status WHEN 'done' THEN 'completed' ELSE 'cancelled' END FROM task WHERE id=?3) ELSE state END,current=CASE WHEN {RESOLVED} THEN 0 ELSE current END,completed_at=CASE WHEN {RESOLVED} THEN ?4 ELSE completed_at END,updated_at=?4,revision=revision+1 WHERE task_ref=?3 AND current=1 AND (SELECT status FROM task WHERE id=?3) IN ('done','cancelled') AND NOT EXISTS(SELECT 1 FROM json_each(integration_attempt.observations_json) WHERE json_extract(value,'$.identity')=?5)"))
                .bind(INTEGRATION_OBSERVATION_RECENT + 1)
                .bind(&json)
                .bind(&step.task_id)
                .bind(&observation.recorded_at)
                .bind(&observation.identity)
                .execute(&mut **tx)
                .await?;
            Ok(())
        }
        .await;
        if let Err(error) = result {
            log_shadow_failure("terminal", &step.task_id, &error);
        }
    }
    pub(crate) async fn observe_integration_review_best_effort(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        task_id: &str,
        review_id: &str,
        status: &crate::ReviewStatus,
        details: &Value,
        carry: Option<&crate::NewReviewAuthorityCarry>,
    ) {
        if !crate::task_writer::owns_task(task_id) {
            return;
        }
        // CI completion is independent of the later semantic or human
        // verdict: running/awaiting-human Reviews can already contain the
        // finished check result. No finished check, no statement.
        let Some(codes) = details
            .get("ci_steps")
            .and_then(Value::as_array)
            .filter(|checks| !checks.is_empty())
            .and_then(|checks| {
                checks
                    .iter()
                    .map(|check| check["exit_code"].as_i64())
                    .collect::<Option<Vec<_>>>()
            })
        else {
            return;
        };
        let kind = if codes.iter().any(|code| *code != 0) {
            IntegrationOutcomeKind::CiFailed
        } else {
            IntegrationOutcomeKind::CiPassed
        };
        let mut observation =
            IntegrationObservation::new(format!("review:{review_id}:{status}"), kind);
        observation.step_id = crate::task_writer::current_step_id();
        if let Some(carry) = carry {
            observation.candidate_sha = Some(carry.commit_sha.clone());
            observation.target_tip_sha = Some(carry.base_sha.clone());
            observation.changed_paths = Some(carry.changed_paths.clone());
        }
        if let Err(error) = append_for_task(tx, task_id, &observation, None).await {
            log_shadow_failure("review", task_id, &error);
        }
    }
}
/// "Logged once" without remembering identities: the first failure and then
/// every power of two is a warning, the rest are debug lines. One counter.
static SHADOW_FAILURES: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
pub(super) fn shadow_failure_count() -> u64 {
    SHADOW_FAILURES.load(Ordering::Relaxed)
}
fn log_shadow_failure(site: &str, task_id: &str, error: &DbError) {
    let failures = SHADOW_FAILURES.fetch_add(1, Ordering::Relaxed) + 1;
    if failures.is_power_of_two() {
        tracing::warn!(task_id, site, failures, %error, "integration shadow observation failed; legacy result retained");
    } else {
        tracing::debug!(task_id, site, failures, %error, "integration shadow observation failed; legacy result retained");
    }
}
fn observed_paths(paths: &Value) -> Result<Vec<String>> {
    serde_json::from_value(paths.clone())
        .map_err(|_| DbError::Check("unsupported path encoding".into()))
}
async fn observe_hook_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    step: &crate::TaskStep,
    index: i64,
    key: &str,
    value: &str,
) -> Result<()> {
    let value = parse_json(value.to_owned())?;
    let mut observation = IntegrationObservation::new(
        format!("{}:{index}:{key}", step.id),
        IntegrationOutcomeKind::Admission,
    );
    observation.step_id = Some(step.id.clone());
    // The target tip this hook's merge path read, if it read one. Done's
    // before_sha can be the checkout's HEAD or the candidate of a
    // reconstructed result, so it is never used as a target-tip witness.
    let read = target_read(&step.id, index);
    match key {
        "merge_intent" => {
            observation.candidate_sha = value["candidate_sha"].as_str().map(str::to_owned);
        }
        "rebase_target" => {
            observation.kind = IntegrationOutcomeKind::TargetTip;
            observation.target_tip_sha = value.as_str().map(str::to_owned);
        }
        "merge_outcome" => {
            let (kind, facts) = value
                .as_object()
                .and_then(|object| object.iter().next())
                .ok_or_else(|| DbError::Check("invalid merge outcome".into()))?;
            observation.kind = match kind.as_str() {
                "Done" => IntegrationOutcomeKind::Done,
                "ReviewRequired" => IntegrationOutcomeKind::ReviewRequired,
                "TargetMoved" => IntegrationOutcomeKind::TargetMoved,
                "Conflict" => IntegrationOutcomeKind::Conflict,
                "Dirty" => IntegrationOutcomeKind::Dirty,
                "TargetDirty" => IntegrationOutcomeKind::TargetDirty,
                "UnresolvedConflictMarkers" => IntegrationOutcomeKind::Markers,
                _ => return Err(DbError::Check("unknown merge outcome".into())),
            };
            if let Some(paths) = facts.get("conflict_paths").or_else(|| facts.get("paths")) {
                observation.conflict_paths = Some(observed_paths(paths)?);
            }
        }
        "rebase_outcome" => {
            observation.kind = match value["kind"].as_str() {
                Some("rebased") => IntegrationOutcomeKind::CleanRebase,
                Some("conflict") => IntegrationOutcomeKind::ConflictHandoff,
                Some("dirty") => IntegrationOutcomeKind::Dirty,
                Some("unsupported_conflict") => IntegrationOutcomeKind::UnsupportedConflict,
                _ => return Err(DbError::Check("unknown rebase outcome".into())),
            };
            if let Some(paths) = value.get("conflict_paths") {
                observation.conflict_paths = Some(observed_paths(paths)?);
            }
        }
        _ => unreachable!("filtered by the caller"),
    }
    if matches!(key, "merge_intent" | "merge_outcome") {
        if let Some((candidate, target)) = read {
            observation.candidate_sha = observation.candidate_sha.or(candidate);
            observation.target_tip_sha = Some(target);
        }
    }
    let intent_candidate = (key == "merge_intent")
        .then(|| observation.candidate_sha.clone())
        .flatten();
    if append_for_task(tx, &step.task_id, &observation, intent_candidate.as_deref()).await?
        || key != "merge_intent"
    {
        return Ok(());
    }
    // Admission, once per merge entry: no row took the intent.
    let replay: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM integration_attempt WHERE task_ref=? AND current=1)",
    )
    .bind(&step.task_id)
    .fetch_one(&mut **tx)
    .await?;
    if replay {
        return Ok(());
    }
    let task = sqlx::query(
        "SELECT project_id,status,status_epoch,version,parent_task_id FROM task WHERE id=?",
    )
    .bind(&step.task_id)
    .fetch_one(&mut **tx)
    .await?;
    // A subtask merges into its parent's branch: no orphan queue for it.
    if task
        .try_get::<Option<String>, _>("parent_task_id")?
        .is_some()
    {
        return Ok(());
    }
    let workspace_id = value["workspace_id"]
        .as_str()
        .ok_or_else(|| DbError::Check("merge intent has no workspace".into()))?;
    let branch = value["target_branch"]
        .as_str()
        .ok_or_else(|| DbError::Check("merge intent has no target".into()))?;
    let repo_id: String = sqlx::query_scalar("SELECT repo_id FROM workspace WHERE id=?")
        .bind(workspace_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(DbError::NotFound)?;
    let epoch: i64 = task.try_get("status_epoch")?;
    let mut a = IntegrationAttempt::new(
        None,
        step.task_id.clone(),
        task.try_get("project_id")?,
        format!("shadow:{}:{epoch}", step.task_id),
        task.try_get("status")?,
        epoch,
        task.try_get("version")?,
    );
    a.original_candidate_sha = observation.candidate_sha.clone();
    a.candidate_sha = a.original_candidate_sha.clone();
    a.execution_id = value["execution_id"].as_str().map(str::to_owned);
    a.execution_ref = a.execution_id.clone();
    a.workspace_id = Some(workspace_id.to_owned());
    a.workspace_ref = a.workspace_id.clone();
    a.observations_json = serde_json::json!([parse_json(observation.stored()?)?]);
    sqlx::query("SAVEPOINT integration_shadow")
        .execute(&mut **tx)
        .await?;
    let admitted = async {
        let queue = create_queue_in_tx(tx, &repo_id, branch).await?;
        a.queue_id = Some(queue.id);
        admit_in_tx(tx, a).await
    }
    .await;
    if admitted.is_err() {
        let _ = sqlx::query("ROLLBACK TO integration_shadow")
            .execute(&mut **tx)
            .await;
    }
    let _ = sqlx::query("RELEASE integration_shadow")
        .execute(&mut **tx)
        .await;
    admitted.map(|_| ())
}
