//! Stored encoding, quarantine and version-fenced condition persistence.
use super::*;

pub(super) fn json_error(error: serde_json::Error) -> DbError {
    DbError::Check(format!("Task condition encoding: {error}"))
}

pub fn decode(raw: &str) -> Result<TaskCondition> {
    let condition: TaskCondition = serde_json::from_str(raw)
        .map_err(|e| DbError::Check(format!("invalid Task condition: {e}")))?;
    integration::validate(&condition)?;
    Ok(condition)
}
/// How a stored condition reads to this build.
#[derive(Debug)]
pub(crate) enum Stored {
    Readable(Box<TaskCondition>),
    /// Corrupt or empty: invalid JSON, a scalar, `{}`, `[]`, a known tag with
    /// a broken body. The legacy columns and durable facts still hold what
    /// the row should say, so every producer, statement and check RESTATES
    /// it from them.
    Corrupt(UnknownConditionProblem),
    /// Recognisably a newer encoding: a well-formed tagged value naming a
    /// variant this build does not know, or any undecodable value in a
    /// database whose recorded mapping revision is higher than this build's.
    /// QUARANTINED: the bytes are never rewritten here. The only exit is a
    /// build that understands the encoding.
    Newer,
}

/// Whether `value` is a tagged value of `T` naming a variant `T` lacks.
fn unknown_tag<T: serde::de::DeserializeOwned>(value: Option<&serde_json::Value>) -> bool {
    let Some(kind) = value.and_then(|v| v.get("kind")).and_then(|k| k.as_str()) else {
        return false;
    };
    // serde reports an unknown tag before it reads any field.
    serde_json::from_value::<T>(serde_json::json!({ "kind": kind }))
        .is_err_and(|error| error.to_string().starts_with("unknown variant"))
}
/// A reason, and the integration reason nested in it, names an unknown variant.
fn unknown_reason(value: Option<&serde_json::Value>) -> bool {
    unknown_tag::<ParkReason>(value)
        || value.is_some_and(|v| {
            v.get("kind").and_then(|k| k.as_str()) == Some("integration")
                && unknown_tag::<IntegrationReason>(v.get("reason"))
        })
}
/// Whether an undecodable object is recognisably a newer encoding: its own
/// tag, or the tag of a value at a typed position inside it, is unknown.
fn newer_encoding(value: &serde_json::Value) -> bool {
    if unknown_tag::<TaskCondition>(Some(value)) {
        return true;
    }
    fn list(value: Option<&serde_json::Value>) -> &[serde_json::Value] {
        value
            .and_then(|v| v.as_array())
            .map(Vec::as_slice)
            .unwrap_or_default()
    }
    let evidence = value.get("evidence");
    unknown_reason(value.get("primary"))
        || unknown_reason(value.get("failure"))
        || unknown_tag::<ConditionContinuation>(value.get("resume"))
        || list(value.get("additional"))
            .iter()
            .chain(list(evidence.and_then(|e| e.get("observations"))))
            .any(|reason| unknown_reason(Some(reason)))
        || list(evidence.and_then(|e| e.get("witnesses")))
            .iter()
            .any(|witness| {
                unknown_tag::<ConditionWitness>(Some(witness))
                    || unknown_tag::<IntegrationReason>(witness.get("reason"))
                    || unknown_phase(witness.get("wait"))
            })
}
/// A check wait whose phase this build does not know.
fn unknown_phase(wait: Option<&serde_json::Value>) -> bool {
    wait.and_then(|wait| wait.get("phase"))
        .is_some_and(|phase| serde_json::from_value::<CheckWaitPhase>(phase.clone()).is_err())
}
/// Classify stored bytes by their own shape. A database-wide newer revision
/// is added by [`stored_condition`].
pub(crate) fn classify(stored: &[u8]) -> Stored {
    use UnknownConditionProblem as Problem;
    let Ok(raw) = std::str::from_utf8(stored) else {
        return Stored::Corrupt(Problem::NonText);
    };
    if let Ok(condition) = decode(raw) {
        return Stored::Readable(Box::new(condition));
    }
    match serde_json::from_str::<serde_json::Value>(raw) {
        Err(_) => Stored::Corrupt(Problem::MalformedJson),
        Ok(value) if !value.is_object() => Stored::Corrupt(Problem::NonObject),
        Ok(value) if newer_encoding(&value) => Stored::Newer,
        Ok(_) => Stored::Corrupt(Problem::InvalidShape),
    }
}
/// [`classify`], with every undecodable value of a database a newer build
/// has recorded its mapping revision in counted as that build's encoding.
pub(crate) async fn stored_condition(connection: &mut SqliteConnection, stored: &[u8]) -> Stored {
    match classify(stored) {
        Stored::Corrupt(_) if recorded_revision_is_newer(connection).await => Stored::Newer,
        classified => classified,
    }
}
/// Whether a newer build recorded its mapping revision here. A database with
/// no settings table (the migration that adds this column) records none.
async fn recorded_revision_is_newer(connection: &mut SqliteConnection) -> bool {
    sqlx::query_scalar::<_, String>("SELECT value FROM system_setting WHERE key=?")
        .bind(MAPPING_REVISION_KEY)
        .fetch_optional(connection)
        .await
        .ok()
        .flatten()
        .and_then(|recorded| recorded.parse::<i64>().ok())
        .is_some_and(|recorded| recorded > MAPPING_REVISION)
}
/// The ids a quarantine warning or report names.
pub const QUARANTINE_REPORT_LIMIT: usize = 20;

/// The stored condition as every row reader sees it: a value that cannot be
/// decoded is a typed unknown park, never a silent `Clear`. `unknown_kind`
/// is the quarantined class (a newer encoding); every other problem is
/// corruption the next producer or check restates.
pub fn decode_or_unknown(raw: &str) -> TaskCondition {
    let problem = match classify(raw.as_bytes()) {
        Stored::Readable(condition) => return *condition,
        Stored::Newer => UnknownConditionProblem::UnknownKind,
        Stored::Corrupt(problem) => problem,
    };
    TaskCondition::Parked {
        primary: ParkReason::UnknownCondition {
            source: ConditionSource {
                field: ConditionField::ConditionJson,
                key: None,
            },
            problem,
        },
        additional: Vec::new(),
        resume: ConditionContinuation::Reconcile,
        since: None,
        evidence: Default::default(),
    }
}
pub(super) fn encode(condition: &TaskCondition) -> String {
    serde_json::to_string(condition).expect("Task condition serializes")
}
/// Version-fenced shadow write shared by every producer seam: store `condition` unless the
/// row already holds exactly it. Touches only `condition_json`: no version,
/// timestamp, revision, event or Attention change.
pub(super) async fn set_condition(
    connection: &mut SqliteConnection,
    task_id: &str,
    condition: &str,
    expected_version: Option<i64>,
) -> Result<()> {
    debug_assert!(
        decode(condition).is_ok_and(|decoded| encode(&decoded) == condition),
        "Task condition must round-trip through its stored form"
    );
    let written =
        sqlx::query("UPDATE task SET condition_json=?1 WHERE id=?2 AND (?3 IS NULL OR version=?3) AND condition_json IS NOT ?1")
            .bind(condition)
            .bind(task_id)
            .bind(expected_version)
            .execute(connection)
            .await?
            .rows_affected();
    debug_assert!(written <= 1, "one Task owns one condition");
    Ok(())
}

/// The full recompute of one row: the mapping fallback, used by the backfill,
/// the invariant check and a writer with no narrower claim. Integration
/// witnesses of a readable row survive. A corrupt or empty stored value is
/// restated from the legacy columns and facts; a recognisably newer encoding
/// is quarantined and left byte for byte.
pub(crate) async fn sync_condition(connection: &mut SqliteConnection, task_id: &str) -> Result<()> {
    produce(connection, task_id, ConditionChange::Full).await
}

/// Migration post-step for `V202610060030__task_condition`: map every existing
/// Task with the same function the writers use, and record the mapping
/// revision it wrote. Runs inside the migration's transaction; on a Task only
/// `condition_json` changes, and only where it differs from the column default.
pub(crate) async fn backfill(connection: &mut SqliteConnection) -> Result<()> {
    let full_schema: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name IN ('project','execution','task_step')").fetch_one(&mut *connection).await?;
    let mut after: Option<String> = None;
    let (mut quarantined, mut quarantined_ids) = (0_u64, Vec::<String>::new());
    loop {
        let rows = sqlx::query(&format!(
            "SELECT {CHANNEL_SELECT},id FROM task WHERE ?1 IS NULL OR id>?1 ORDER BY id LIMIT 200"
        ))
        .bind(after.as_deref())
        .fetch_all(&mut *connection)
        .await?;
        let Some(last) = rows.last() else {
            break;
        };
        after = Some(last.try_get(6)?);
        for row in &rows {
            let stored: Vec<u8> = row.try_get(5)?;
            let id: String = row.try_get(6)?;
            // A newer encoding is left alone; corruption is restated below.
            if matches!(
                stored_condition(&mut *connection, &stored).await,
                Stored::Newer
            ) {
                quarantined += 1;
                if quarantined_ids.len() < QUARANTINE_REPORT_LIMIT {
                    quarantined_ids.push(id);
                }
                continue;
            }
            let input = ConditionInput::from_row(row)?;
            let mapped = compose_condition(&input);
            let condition = if full_schema == 3 {
                match ConditionFacts::load(connection, &id).await {
                    Ok(facts) => encode(&facts.condition(&input)),
                    Err(_) => encode(&mapped),
                }
            } else {
                encode(&mapped)
            };
            if row.try_get::<Vec<u8>, _>(5)? != condition.as_bytes() {
                let id: String = row.try_get(6)?;
                set_condition(&mut *connection, &id, &condition, None).await?;
            }
        }
    }
    if quarantined != 0 {
        tracing::warn!(
            quarantined,
            task_ids = ?quarantined_ids,
            "Task conditions written by a newer build were left quarantined by the backfill; run a build that understands them"
        );
    }
    // Every row this build can read or restate now holds this revision's
    // encoding: record it, so the first dispatcher tick after a clean upgrade
    // does not walk the table again.
    let settings: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name='system_setting'",
    )
    .fetch_one(&mut *connection)
    .await?;
    if full_schema == 3 && settings == 1 {
        sqlx::query("INSERT INTO system_setting(key,value,updated_at) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at")
            .bind(MAPPING_REVISION_KEY)
            .bind(MAPPING_REVISION.to_string())
            .bind(crate::now_rfc3339())
            .execute(&mut *connection)
            .await?;
    }
    Ok(())
}

impl SqliteDb {
    /// Stage-one diagnostic only; no REST/MCP Task fields or readers change.
    pub async fn task_condition(&self, task_id: &str) -> Result<TaskCondition> {
        let raw: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
            .bind(task_id)
            .fetch_optional(self.pool())
            .await?
            .ok_or(DbError::NotFound)?;
        decode(&raw)
    }

    /// State integration ownership under the live Task-step lease. No legacy
    /// field, version, event, budget or queue row is written by this seam.
    pub async fn state_integration_condition_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        task_id: &str,
        statement: &ConditionStatement,
    ) -> Result<()> {
        if !statement.owner_stated() {
            return Err(DbError::Check(
                "expected an integration or check statement".into(),
            ));
        }
        if !crate::task_writer::owns_task(task_id) {
            return Err(DbError::Check(
                "integration statement requires the Task step lease".into(),
            ));
        }
        // The generic hook fence permits completed command bookkeeping.
        // An integration statement needs a currently claimed Task step.
        let step = crate::task_writer::current_task_step().ok_or(DbError::VersionConflict)?;
        let claimed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_step WHERE id=? AND task_id=? AND status='claimed' AND claimed_by=? AND (lease_until>? OR ?))")
            .bind(&step.id).bind(task_id).bind(&step.claimed_by)
            .bind(crate::now_rfc3339()).bind(self.step_is_active(&step))
            .fetch_one(&mut **tx).await?;
        if !claimed {
            return Err(DbError::VersionConflict);
        }
        self.fence_task_lease_in_tx(tx, task_id, "integration statement")
            .await?;
        // A quarantined row refuses the statement inside `derive`.
        let produced =
            producers::derive(tx, task_id, ConditionChange::Legacy, None, Some(statement))
                .await?
                .ok_or(DbError::NotFound)?;
        self.set_condition(
            tx,
            task_id,
            produced.version,
            &produced.input,
            &produced.condition,
        )
        .await
    }

    /// Strict single-writer seam for a producer that states a condition. It
    /// requires the Task's claimed step and its live lease, never rebinds a
    /// stale version, and compares all five source values (metadata changes
    /// without advancing Task.version). Legacy-derived reasons must agree
    /// with their mapping; integration is validated from its typed witness.
    /// A condition is refused unless those owners agree under the
    /// witnesses it states itself. Those witnesses are the producer's claim:
    /// they are not re-read here (the invariant check does that). The only
    /// observer read is the epoch-fenced scheduler park, which the witnesses
    /// do not own; it is checked in this same transaction.
    /// The shadow write bumps no version and emits no event or Attention.
    pub async fn set_condition(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        task_id: &str,
        expected_version: i64,
        expected_input: &ConditionInput,
        condition: &TaskCondition,
    ) -> Result<()> {
        integration::validate(condition)?;
        check::validate(condition)?;
        if !crate::task_writer::owns_task(task_id) {
            return Err(DbError::Check(
                "set_condition requires the Task step lease".into(),
            ));
        }
        self.fence_task_lease_in_tx(tx, task_id, "set_condition")
            .await?;
        let row = sqlx::query(&format!(
            "SELECT {CHANNEL_SELECT},version,status,status_epoch FROM task WHERE id=?"
        ))
        .bind(task_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(DbError::NotFound)?;
        let stored: Vec<u8> = row.try_get(5)?;
        // A corrupt stored value states no integration ownership: the
        // condition written over it is the restatement.
        let stored_condition = match stored_condition(tx, &stored).await {
            Stored::Readable(stored) => Some(*stored),
            Stored::Corrupt(_) => None,
            Stored::Newer => {
                return Err(DbError::TaskConditionQuarantined {
                    task_id: task_id.to_owned(),
                })
            }
        };
        let owned = |c: &TaskCondition| {
            // Settlement keeps the attempt identity alone, so a settled
            // condition owns what its unsettled predecessor's attempt owned.
            match c {
                TaskCondition::Settled { .. } => (None, false, c.integration_attempt().cloned()),
                _ => (
                    c.integration_reason().cloned(),
                    c.integration_handoff_ready(),
                    c.integration_attempt().cloned(),
                ),
            }
        };
        let before = stored_condition.as_ref().map(owned).unwrap_or_default();
        let after = owned(condition);
        // A check wait belongs to the status entry that asked: only one this
        // entry still witnesses counts as owned.
        let epoch: i64 = row.try_get(8)?;
        let waits = |c: &TaskCondition| {
            c.check_witness()
                .filter(|(_, at)| *at == epoch)
                .map(|(wait, _)| wait.clone())
        };
        let unchanged = (if matches!(condition, TaskCondition::Settled { .. }) {
            before.2 == after.2
        } else {
            before == after
        }) && stored_condition.as_ref().and_then(waits) == waits(condition);
        if !unchanged {
            // Completed command bookkeeping may carry integration, never
            // introduce, replace or clear its independently owned statement.
            let step = crate::task_writer::current_task_step().ok_or(DbError::VersionConflict)?;
            let claimed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_step WHERE id=? AND task_id=? AND status='claimed')")
                .bind(&step.id).bind(task_id).fetch_one(&mut **tx).await?;
            if !claimed {
                // A check failure, not a version conflict: the caller that
                // answers a conflict by writing under the version fence alone
                // (`state_condition_in_tx`) must not write this one.
                return Err(DbError::Check(
                    "integration or check ownership cannot change under a completed Task step"
                        .into(),
                ));
            }
        }
        let mut bare = expected_input.clone();
        bare.facts = None;
        if row.try_get::<i64, _>(6)? != expected_version || ConditionInput::from_row(&row)? != bare
        {
            return Err(DbError::VersionConflict);
        }
        if !condition.channels_match(&bare) {
            return Err(DbError::Check(
                "Task condition digest differs from its channel inputs".into(),
            ));
        }
        // Scheduler parks are epoch-fenced observer facts, not owned
        // witnesses. Compare the statement under the actual observer record,
        // in this transaction, so a pause/parent/Agent wait cannot make an
        // otherwise valid check or integration statement disagree with itself.
        let owner_park: Option<String> = sqlx::query_scalar(
            // The pause record counts only while the Project is paused: the
            // same rule as the snapshot the producer reads (`Families::sql`).
            "SELECT reason_json FROM task_schedule_park WHERE task_id=?1 AND epoch=?2 AND NOT (json_valid(reason_json) AND json_extract(reason_json,'$.reason')='ProjectPaused' AND (SELECT p.paused_at FROM project p JOIN task t ON t.project_id=p.id WHERE t.id=?1) IS NULL)",
        )
        .bind(task_id)
        .bind(row.get::<i64, _>(8))
        .fetch_optional(&mut **tx)
        .await?;
        let owner_park = owner_park.and_then(|raw| serde_json::from_str(&raw).ok());
        let claim = condition.clone();
        let carried = ConditionFacts::recover(&claim);
        let stated = carried
            .filter(|facts| facts.task_id == task_id)
            .map(|mut facts| {
                facts.version = expected_version;
                facts.state = row.get(7);
                facts.children_pending = facts::children_pending(bare.metadata_json.as_deref());
                facts.owner_park = owner_park;
                facts
            })
            .filter(|facts| facts.epoch == row.get::<i64, _>(8));
        // The condition must be exactly what its witnesses and the legacy
        // fields state. One carrying an integration reason is the exception:
        // an integration statement edits the stored condition's typed form
        // (it writes no legacy field), so the result carries the `stated`
        // mark and none of the legacy copies the recomputation carries, and
        // the two can only be compared without them.
        let agrees = |facts: ConditionFacts| {
            let expected = facts.condition(&bare);
            if condition.integration_reason().is_some() || condition.check_witness().is_some() {
                expected.typed() == condition.typed()
            } else {
                expected == *condition
            }
        };
        if !stated.is_some_and(agrees) {
            return Err(DbError::Check(
                "Task condition differs from its legacy and owner witnesses".into(),
            ));
        }
        set_condition(
            tx,
            task_id,
            &serde_json::to_string(condition).map_err(json_error)?,
            Some(expected_version),
        )
        .await
    }

    /// The producer seam for a writer outside this crate that has fenced its
    /// step lease in this transaction (the engine's status CAS, the worker's
    /// step settlement): call it after the write, naming what the write
    /// changed. Under the Task's step the condition is stated through
    /// [`Self::set_condition`]; the same writer running with no step (an
    /// owner command applied inline), or after settling its step in this
    /// transaction, uses the version fence alone. A condition the strict
    /// seam refuses is logged and not written. It never refuses the legacy
    /// write it follows.
    /// It adds no version, timestamp, event or wake to the legacy write.
    pub async fn state_condition_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        task_id: &str,
        change: ConditionChange,
    ) -> Result<()> {
        let Some(produced) = producers::derive(tx, task_id, change, None, None).await? else {
            return Ok(());
        };
        if produced.changed {
            let stated = if crate::task_writer::owns_task(task_id) {
                self.set_condition(
                    tx,
                    task_id,
                    produced.version,
                    &produced.input,
                    &produced.condition,
                )
                .await
            } else {
                Err(DbError::VersionConflict)
            };
            match stated {
                Ok(()) => {}
                // No step, or a step this transaction has already settled:
                // the legacy write stands on its own fences and the
                // condition must still describe it, under the version fence.
                Err(DbError::VersionConflict) => {
                    set_condition(tx, task_id, &produced.encoded, Some(produced.version)).await?
                }
                // The strict seam refused the condition itself. Nothing is
                // written: the legacy write is authoritative and proceeds,
                // and the invariant check restates the row.
                Err(DbError::Check(reason)) => {
                    tracing::error!(task_id, ?change, %reason, "Task condition refused by set_condition; left to the invariant check")
                }
                Err(error) => return Err(error),
            }
        }
        if let Some(parent) = &produced.parent {
            produce_children(tx, parent).await?;
        }
        Ok(())
    }

    /// The producer seam for a direct SQL writer that holds no step lease
    /// (bulk clears, admission and reconnect writes): same transaction, after
    /// the write, naming what the write changed. It adds no rejection to the
    /// legacy write beyond its own read.
    pub async fn produce_condition_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        task_id: &str,
        change: ConditionChange,
    ) -> Result<()> {
        produce(tx, task_id, change).await
    }

    /// The mapping fallback for one row: recompute every fact family and
    /// store the result. Tests and repair paths use it; a writer that knows
    /// what it changed uses [`Self::produce_condition_in_tx`].
    pub async fn sync_condition_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        task_id: &str,
    ) -> Result<()> {
        sync_condition(tx, task_id).await
    }

    /// Invariant usable by writer tests: the cache agrees with the row's
    /// current channel digest and independently loaded durable facts.
    pub async fn check_task_condition_invariant(&self, task: &Task) -> Result<()> {
        let mut connection = self.pool().begin().await?;
        let row = sqlx::query(&format!("SELECT {CHANNEL_SELECT} FROM task WHERE id=?"))
            .bind(&task.id)
            .fetch_one(&mut *connection)
            .await?;
        let actual: String = row.try_get(5)?;
        decode(&actual)?;
        let facts = ConditionFacts::load(&mut connection, &task.id).await?;
        let expected = facts.condition(&ConditionInput::from_row(&row)?);
        if !TaskCondition::stored_agrees(actual.as_bytes(), &expected, &encode(&expected)) {
            return Err(DbError::Check(format!(
                "Task condition invariant failed for {}",
                task.id
            )));
        }
        Ok(())
    }

    /// Test-support sweep: ids of every Task whose shadow is not the mapping
    /// of its legacy fields and durable facts. Production uses bounded pages.
    pub async fn task_condition_violations(&self) -> Result<Vec<String>> {
        let mut connection = self.pool().begin().await?;
        let rows = sqlx::query(&format!("SELECT {CHANNEL_SELECT},id FROM task ORDER BY id"))
            .fetch_all(&mut *connection)
            .await?;
        let mut violations = Vec::new();
        for row in &rows {
            let id: String = row.try_get(6)?;
            let facts = ConditionFacts::load(&mut connection, &id).await?;
            let expected = facts.condition(&ConditionInput::from_row(row)?);
            if !TaskCondition::stored_agrees(
                &row.try_get::<Vec<u8>, _>(5)?,
                &expected,
                &encode(&expected),
            ) {
                violations.push(id);
            }
        }
        Ok(violations)
    }
}
