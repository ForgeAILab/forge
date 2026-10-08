use super::*;

// Facts stay in the current Task-step's memory until its existing result
// transaction records them. Reading a target adds no database transaction,
// Git command, checkpoint, event, lease or queue action to the merge path.
tokio::task_local! { static TARGET_OBSERVATIONS: std::cell::RefCell<Vec<(String,i64,IntegrationObservation)>>; }
pub(crate) async fn with_observation_buffer<T>(future: impl std::future::Future<Output = T>) -> T {
    TARGET_OBSERVATIONS
        .scope(std::cell::RefCell::new(Vec::new()), future)
        .await
}
pub fn note_integration_target(
    task_id: &str,
    index: i64,
    candidate_sha: Option<&str>,
    target_tip_sha: &str,
) {
    let Some(step) = crate::task_writer::current_task_step().filter(|step| step.task_id == task_id)
    else {
        return;
    };
    let mut observation = IntegrationObservation::new(
        format!("{}:{index}:merge_target", step.id),
        IntegrationOutcomeKind::TargetTip,
    );
    observation.step_id = Some(step.id.clone());
    observation.candidate_sha = candidate_sha.map(str::to_owned);
    observation.target_tip_sha = Some(target_tip_sha.to_owned());
    let _ = TARGET_OBSERVATIONS.try_with(|buffer| {
        let mut buffer = buffer.borrow_mut();
        buffer.retain(|(id, hook, _)| id != &step.id || *hook != index);
        buffer.push((step.id.clone(), index, observation));
    });
}
fn target_observation(step: &crate::TaskStep, index: i64) -> Option<IntegrationObservation> {
    TARGET_OBSERVATIONS
        .try_with(|buffer| {
            buffer
                .borrow()
                .iter()
                .find(|(id, hook, _)| id == &step.id && *hook == index)
                .map(|(_, _, o)| o.clone())
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
            recorded_at: now_rfc3339(),
        }
    }
}
pub(super) async fn record_observation_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    attempt_id: &str,
    observation: &IntegrationObservation,
) -> Result<()> {
    if observation.identity.is_empty() {
        return Err(DbError::Check("observation identity missing".into()));
    }
    for paths in [&observation.changed_paths, &observation.conflict_paths]
        .into_iter()
        .flatten()
    {
        validate_integration_paths(&serde_json::json!(paths))?;
    }
    let mut a = attempt_in_tx(tx, attempt_id).await?;
    let observations = a
        .observations_json
        .as_array_mut()
        .ok_or_else(|| DbError::Check("observations are not an array".into()))?;
    if let Some(existing) = observations
        .iter()
        .find(|v| v["identity"].as_str() == Some(&observation.identity))
    {
        let mut existing = existing.clone();
        let mut next = serde_json::to_value(observation).expect("observation serializes");
        existing
            .as_object_mut()
            .expect("typed observation")
            .remove("recorded_at");
        next.as_object_mut()
            .expect("typed observation")
            .remove("recorded_at");
        if existing != next {
            return Err(DbError::IdempotencyConflict);
        }
        return Ok(());
    }
    observations.push(serde_json::to_value(observation).expect("observation serializes"));
    a.updated_at = observation.recorded_at.clone();
    // Observations never grant authority, claim a slot, or drive state.
    update_attempt(tx, &a).await
}

/// Isolate every optional shadow write. SQLite constraint/statement failures
/// roll back this savepoint while the authoritative result still commits.
/// One invocation produces at most one warning; replay identities deduplicate
/// successful observations. There is no runtime feature flag.
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
        let result = async {
            sqlx::query("SAVEPOINT integration_shadow")
                .execute(&mut **tx)
                .await?;
            observe_hook_in_tx(tx, step, index, key, value).await
        }
        .await;
        finish_shadow(
            tx,
            &step.task_id,
            &format!("{}:{index}:{key}", step.id),
            result,
        )
        .await;
    }
    pub(crate) async fn observe_integration_terminal_best_effort(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        step: &crate::TaskStep,
    ) {
        let result = async {
            sqlx::query("SAVEPOINT integration_shadow").execute(&mut **tx).await?;
            let status: String = sqlx::query_scalar("SELECT status FROM task WHERE id=?").bind(&step.task_id).fetch_one(&mut **tx).await?;
            let kind = match status.as_str() { "cancelled" => IntegrationOutcomeKind::Cancelled, "done" => IntegrationOutcomeKind::Done, _ => return Ok(()) };
            let id: Option<String> = sqlx::query_scalar("SELECT id FROM integration_attempt WHERE task_ref=? AND current=1").bind(&step.task_id).fetch_optional(&mut **tx).await?;
            if let Some(id) = id {
                let mut observation = IntegrationObservation::new(format!("{}:terminal", step.id),kind);
                observation.step_id = Some(step.id.clone());
                record_observation_in_tx(tx, &id, &observation).await?;
                // An uncertain effect remains pinned even if a legacy Task
                // is terminal. Its status alone never proves a non-effect.
                let a=attempt_in_tx(tx,&id).await?;
                if matches!(a.state,IntegrationAttemptState::Reconciling|IntegrationAttemptState::FfInflight|IntegrationAttemptState::Quarantined) || matches!(a.current_operation_state,Some(IntegrationOperationState::Running|IntegrationOperationState::Uncertain)) {return Ok(());}
                // Only resolved terminal history follows the legacy result.
                sqlx::query("UPDATE integration_attempt SET state=?,current=0,completed_at=?,revision=revision+1 WHERE id=?")
                    .bind(if status == "done" { "completed" } else { "cancelled" }).bind(&observation.recorded_at).bind(&id).execute(&mut **tx).await?;
            }
            Ok(())
        }.await;
        finish_shadow(tx, &step.task_id, &format!("{}:terminal", step.id), result).await;
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
        let result = async {
            sqlx::query("SAVEPOINT integration_shadow")
                .execute(&mut **tx)
                .await?;
            let id: Option<String> = sqlx::query_scalar(
                "SELECT id FROM integration_attempt WHERE task_ref=? AND current=1",
            )
            .bind(task_id)
            .fetch_optional(&mut **tx)
            .await?;
            if let Some(id) = id {
                // CI completion is independent of the later semantic or
                // human verdict: running/awaiting-human Reviews can already
                // contain the finished check result.
                if details.get("ci_steps").is_some() {
                    let Some(checks) = details["ci_steps"]
                        .as_array()
                        .filter(|checks| !checks.is_empty())
                    else {
                        return Ok(());
                    };
                    let codes = checks
                        .iter()
                        .map(|check| check["exit_code"].as_i64())
                        .collect::<Option<Vec<_>>>();
                    let Some(codes) = codes else {
                        return Ok(());
                    };
                    let kind = if codes.iter().any(|code| *code != 0) {
                        IntegrationOutcomeKind::CiFailed
                    } else {
                        IntegrationOutcomeKind::CiPassed
                    };
                    let mut observation =
                        IntegrationObservation::new(format!("review:{review_id}:{status}"), kind);
                    observation.step_id = crate::task_writer::current_task_step().map(|s| s.id);
                    if let Some(carry) = carry {
                        observation.candidate_sha = Some(carry.commit_sha.clone());
                        observation.target_tip_sha = Some(carry.base_sha.clone());
                        observation.changed_paths = Some(carry.changed_paths.clone());
                    }
                    record_observation_in_tx(tx, &id, &observation).await?;
                }
            }
            Ok(())
        }
        .await;
        finish_shadow(tx, task_id, &format!("review:{review_id}:{status}"), result).await;
    }
}
async fn finish_shadow(
    tx: &mut Transaction<'_, Sqlite>,
    task_id: &str,
    identity: &str,
    result: Result<()>,
) {
    if let Err(error) = result {
        let _ = sqlx::query("ROLLBACK TO integration_shadow")
            .execute(&mut **tx)
            .await;
        log_shadow_failure(identity, task_id, &error);
    }
    let _ = sqlx::query("RELEASE integration_shadow")
        .execute(&mut **tx)
        .await;
}
fn log_shadow_failure(identity: &str, task_id: &str, error: &DbError) {
    static WARNED: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<String>>> =
        std::sync::OnceLock::new();
    if WARNED
        .get_or_init(Default::default)
        .lock()
        .expect("shadow warning identities")
        .insert(identity.to_owned())
    {
        tracing::warn!(task_id, observation_identity=identity, %error, "integration shadow observation failed; legacy result retained");
    }
}
async fn observe_hook_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    step: &crate::TaskStep,
    index: i64,
    key: &str,
    value: &str,
) -> Result<()> {
    let value = parse_json(value.to_owned())?;
    let mut id: Option<String> =
        sqlx::query_scalar("SELECT id FROM integration_attempt WHERE task_ref=? AND current=1")
            .bind(&step.task_id)
            .fetch_optional(&mut **tx)
            .await?;
    if key == "merge_intent" {
        let task = sqlx::query(
            "SELECT project_id,status,status_epoch,version,parent_task_id FROM task WHERE id=?",
        )
        .bind(&step.task_id)
        .fetch_one(&mut **tx)
        .await?;
        if task
            .try_get::<Option<String>, _>("parent_task_id")?
            .is_some()
        {
            return Ok(());
        }
        let workspace_id = value["workspace_id"]
            .as_str()
            .ok_or_else(|| DbError::Check("merge intent has no workspace".into()))?;
        let repo_id: String = sqlx::query_scalar("SELECT repo_id FROM workspace WHERE id=?")
            .bind(workspace_id)
            .fetch_one(&mut **tx)
            .await?;
        let branch = value["target_branch"]
            .as_str()
            .ok_or_else(|| DbError::Check("merge intent has no target".into()))?;
        let q = create_queue_in_tx(tx, &repo_id, branch).await?;
        if let Some(id) = &id {
            let mut a = attempt_in_tx(tx, id).await?;
            a.candidate_sha = value["candidate_sha"].as_str().map(str::to_owned);
            update_attempt(tx, &a).await?;
        }
        if id.is_none() {
            let mut a = IntegrationAttempt::new(
                Some(q.id),
                step.task_id.clone(),
                task.try_get("project_id")?,
                format!(
                    "shadow:{}:{}",
                    step.task_id,
                    task.try_get::<i64, _>("status_epoch")?
                ),
                task.try_get("status")?,
                task.try_get("status_epoch")?,
                task.try_get("version")?,
            );
            a.original_candidate_sha = value["candidate_sha"].as_str().map(str::to_owned);
            a.candidate_sha = a.original_candidate_sha.clone();
            a.execution_id = value["execution_id"].as_str().map(str::to_owned);
            a.execution_ref = a.execution_id.clone();
            a.workspace_id = Some(workspace_id.to_owned());
            a.workspace_ref = a.workspace_id.clone();
            let a = admit_in_tx(tx, a).await?;
            id = Some(a.id);
        }
    }
    let Some(id) = id else {
        return Ok(());
    };
    if let Some(target) = target_observation(step, index) {
        record_observation_in_tx(tx, &id, &target).await?;
    }

    let mut observation = IntegrationObservation::new(
        format!("{}:{index}:{key}", step.id),
        IntegrationOutcomeKind::Admission,
    );
    observation.step_id = Some(step.id.clone());
    match key {
        "merge_intent" => {
            observation.candidate_sha = value["candidate_sha"].as_str().map(str::to_owned)
        }
        "rebase_target" => {
            observation.kind = IntegrationOutcomeKind::TargetTip;
            observation.target_tip_sha = value.as_str().map(str::to_owned);
        }
        "merge_outcome" => {
            let object = value
                .as_object()
                .ok_or_else(|| DbError::Check("invalid merge outcome".into()))?;
            let (kind, facts) = object
                .iter()
                .next()
                .ok_or_else(|| DbError::Check("empty merge outcome".into()))?;
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
            // Done.before_sha can be the checkout's HEAD, or the candidate
            // in a reconstructed result. It is not a target-tip witness.
            let a = attempt_in_tx(tx, &id).await?;
            observation.target_tip_sha = a
                .observations_json
                .as_array()
                .and_then(|all| {
                    all.iter().rev().find(|o| {
                        o["kind"] == serde_json::json!("target_tip")
                            && o["step_id"].as_str() == Some(&step.id)
                    })
                })
                .and_then(|o| o["target_tip_sha"].as_str().map(str::to_owned));
            if let Some(paths) = facts.get("conflict_paths").or_else(|| facts.get("paths")) {
                observation.conflict_paths = Some(
                    serde_json::from_value(paths.clone())
                        .map_err(|_| DbError::Check("unsupported path encoding".into()))?,
                );
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
                observation.conflict_paths = Some(
                    serde_json::from_value(paths.clone())
                        .map_err(|_| DbError::Check("unsupported path encoding".into()))?,
                );
            }
        }
        _ => unreachable!(),
    }
    let a = attempt_in_tx(tx, &id).await?;
    if observation.candidate_sha.is_none() && key == "merge_outcome" {
        observation.candidate_sha = a.candidate_sha;
    }
    if observation.target_tip_sha.is_none() && key == "rebase_outcome" {
        let target: Option<String> = sqlx::query_scalar("SELECT json_extract(effects_json,'$.rebase_target') FROM task_hook_checkpoint WHERE step_id=? AND hook_index=?").bind(&step.id).bind(index).fetch_optional(&mut **tx).await?.flatten();
        observation.target_tip_sha = target
            .map(parse_json)
            .transpose()?
            .and_then(|v| v.as_str().map(str::to_owned));
    }
    record_observation_in_tx(tx, &id, &observation).await
}
