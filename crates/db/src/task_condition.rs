//! Stage-one shadow condition. Legacy columns still own runtime decisions.
//!
//! The marked SQL projection is the single deterministic mapping definition:
//! migration backfill, atomic raw-SQL adapters and snapshot mapping all use it.
//! No queue intent, publication claim or settlement receipt is moved here.
use crate::{DbError, Result, SqliteDb, Task};
use serde::{Deserialize, Serialize};
use sqlx::{Row, Sqlite, SqliteConnection, Transaction};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskCondition {
    Clear {
        evidence: ConditionEvidence,
    },
    Entering {
        state: String,
        epoch: i64,
        step_id: String,
        phase: String,
        since: String,
        evidence: ConditionEvidence,
    },
    Running {
        execution_id: String,
        role: String,
        epoch: i64,
        since: String,
        evidence: ConditionEvidence,
    },
    Deferred {
        until: String,
        reason: RetryCause,
        resume: ConditionContinuation,
        evidence: ConditionEvidence,
    },
    Parked {
        primary: ParkReason,
        additional: Vec<ParkReason>,
        resume: ConditionContinuation,
        since: Option<String>,
        evidence: ConditionEvidence,
    },
    Failed {
        failure: ParkReason,
        additional: Vec<ParkReason>,
        resume: ConditionContinuation,
        since: Option<String>,
        evidence: ConditionEvidence,
    },
    Settled {
        outcome: TerminalOutcome,
        evidence: ConditionEvidence,
    },
}

/// Exact legacy condition input, including malformed strings. Valid metadata
/// keeps condition/evidence keys only; unrelated keys and intent bodies remain
/// untouched in metadata_json. This evidence is private and non-authoritative.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionEvidence {
    pub error_annotation: Option<String>,
    pub blocked_json: Option<String>,
    pub failed_json: Option<String>,
    pub entry_barrier_json: Option<String>,
    /// Raw JSON fragments preserve large numbers, lone escapes and deeply
    /// nested legacy evidence without routing it through a Value decoder.
    pub metadata: BTreeMap<String, String>,
    pub unparsed_metadata: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyConditionField {
    ErrorAnnotation,
    BlockedJson,
    FailedJson,
    EntryBarrierJson,
    MetadataJson,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionSource {
    pub field: LegacyConditionField,
    pub key: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnknownConditionProblem {
    MalformedJson,
    NonObject,
    UnknownKind,
    InvalidShape,
    UnownedEntry,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HumanBoundary {
    PlanReview,
    ExternalMerge,
    Legacy,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConditionCapacityScope {
    Agent,
    Machine,
    Project,
    OwnerBackpressure,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConditionEnvironmentKind {
    EnvironmentProbePending,
    EnvironmentNotReady,
    EnvironmentUnverified,
    ProvisionFailed,
    Unknown,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ParkReason {
    Held {
        actor: String,
    },
    Failure {
        failure_kind: api_types::FailureKind,
    },
    BudgetExhausted {
        budget_kind: Option<crate::budget::Kind>,
        source: ConditionSource,
    },
    EntryBlocked {
        state: Option<String>,
        source: ConditionSource,
    },
    HumanDecision {
        boundary: HumanBoundary,
        source: ConditionSource,
    },
    Capacity {
        scope: ConditionCapacityScope,
    },
    DispatchRefusal {
        capability: Option<String>,
        blocker_digest: Option<String>,
    },
    ProjectPaused {
        state: Option<String>,
    },
    OwnerOffline {
        daemon_id: Option<String>,
        started_at: Option<String>,
    },
    Environment {
        wait_kind: ConditionEnvironmentKind,
        source: ConditionSource,
    },
    PlacementDenied {
        source: ConditionSource,
    },
    DaemonUpgradeRequired {
        source: ConditionSource,
    },
    RemoteCancelPending {
        source: ConditionSource,
    },
    Dependencies {
        cancelled: bool,
        source: ConditionSource,
    },
    PlanSettlementWait {
        execution_id: Option<String>,
    },
    UnknownCondition {
        source: ConditionSource,
        problem: UnknownConditionProblem,
    },
    WorkflowInvalid {
        state: String,
        definition_digest: String,
        cause: String,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryCause {
    ExecutionFailure,
    WorkflowGuard,
    ReviewCiInfrastructure,
    PlanTransport,
    Legacy,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConditionContinuation {
    Reconcile,
    Dispatch { target_state: String },
    RetryEntry { state: Option<String> },
    Integrate { state: Option<String> },
    SettlePlan { execution_id: Option<String> },
    ResumeQueuedCommand { intent_id: Option<String> },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalOutcome {
    Completed,
    Cancelled,
}

/// A snapshot fence must include metadata: several existing metadata writers
/// intentionally leave Task.version unchanged. No timestamp enters the map.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LegacyConditionInput {
    pub error_annotation: Option<String>,
    pub blocked_json: Option<String>,
    pub failed_json: Option<String>,
    pub entry_barrier_json: Option<String>,
    pub metadata_json: Option<String>,
}
impl From<&Task> for LegacyConditionInput {
    fn from(task: &Task) -> Self {
        Self {
            error_annotation: task.error_annotation.clone(),
            blocked_json: task.blocked_json.clone(),
            failed_json: task.failed_json.clone(),
            entry_barrier_json: task.entry_barrier_json.clone(),
            metadata_json: task.metadata_json.clone(),
        }
    }
}
impl LegacyConditionInput {
    fn from_row(row: &sqlx::sqlite::SqliteRow) -> Self {
        Self {
            error_annotation: row.get("error_annotation"),
            blocked_json: row.get("blocked_json"),
            failed_json: row.get("failed_json"),
            entry_barrier_json: row.get("entry_barrier_json"),
            metadata_json: row.get("metadata_json"),
        }
    }
}

fn projection_sql() -> &'static str {
    include_str!("../migrations/V202610060030__task_condition.sql")
        .split("-- condition-map-begin\n")
        .nth(1)
        .expect("condition migration has mapping start")
        .split("-- condition-map-end")
        .next()
        .expect("condition migration has mapping end")
        .trim()
}
fn decode(raw: &str) -> Result<TaskCondition> {
    serde_json::from_str(raw).map_err(|e| DbError::Check(format!("invalid Task condition: {e}")))
}

/// Map an immutable legacy snapshot, using SQLite's exact migration mapping.
/// This reads no stored Task or ownership table and has no side effects. A
/// pooled connection or an existing writer transaction can evaluate it.
/// Running/Entering/Settled require durable witnesses absent from these five
/// inputs; the stage-one import never fabricates them from state-name prose.
pub async fn map_legacy_condition(
    connection: &mut SqliteConnection,
    input: &LegacyConditionInput,
) -> Result<TaskCondition> {
    let query = format!(
        "WITH task AS (SELECT '' AS id, ? AS error_annotation, ? AS blocked_json, \
         ? AS failed_json, ? AS entry_barrier_json, ? AS metadata_json) \
         SELECT condition_json FROM ({})",
        projection_sql()
    );
    let raw: String = sqlx::query_scalar(&query)
        .bind(&input.error_annotation)
        .bind(&input.blocked_json)
        .bind(&input.failed_json)
        .bind(&input.entry_barrier_json)
        .bind(&input.metadata_json)
        .fetch_one(connection)
        .await?;
    decode(&raw)
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

    /// Strict single-writer seam. Never rebinds a stale version. Source-value
    /// comparison additionally fences version-neutral metadata mutations.
    /// Shadow writes do not bump legacy versions or emit events/Attention.
    pub async fn set_condition(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        task_id: &str,
        expected_version: i64,
        expected_legacy: &LegacyConditionInput,
        condition: &TaskCondition,
    ) -> Result<()> {
        if !crate::task_writer::owns_task(task_id) {
            return Err(DbError::Check(
                "set_condition requires the Task step lease".into(),
            ));
        }
        self.fence_task_lease_in_tx(tx, task_id, "set_condition")
            .await?;
        let row = sqlx::query("SELECT version,error_annotation,blocked_json,failed_json,entry_barrier_json,metadata_json,condition_json FROM task WHERE id=?")
            .bind(task_id).fetch_optional(&mut **tx).await?.ok_or(DbError::NotFound)?;
        if row.get::<i64, _>("version") != expected_version
            || LegacyConditionInput::from_row(&row) != *expected_legacy
        {
            return Err(DbError::VersionConflict);
        }
        // The insert/update adapters and condition guard enforce this equality
        // in SQLite. Avoid evaluating the large import projection again for an
        // already-canonical shadow: ordinary dual writes are a fenced no-op.
        if decode(row.get("condition_json")).ok().as_ref() == Some(condition) {
            return Ok(());
        }
        if map_legacy_condition(tx, expected_legacy).await? != *condition {
            return Err(DbError::Check(
                "Task condition differs from legacy fields".into(),
            ));
        }
        // Use the canonical SQL representation, not serde's object-key order.
        sqlx::query("UPDATE task SET condition_json=(SELECT condition_json FROM task_condition_legacy WHERE id=?) WHERE id=? AND version=? AND condition_json IS NOT (SELECT condition_json FROM task_condition_legacy WHERE id=?)")
            .bind(task_id).bind(task_id).bind(expected_version).bind(task_id)
            .execute(&mut **tx).await?;
        Ok(())
    }

    /// Adapter after an existing authorized legacy write. Atomic triggers have
    /// already covered direct SQL; this seam enforces the same explicit fence
    /// in repository/query adapters without altering returned legacy Tasks.
    pub(crate) async fn sync_condition_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        task_id: &str,
    ) -> Result<()> {
        let row = sqlx::query("SELECT version,error_annotation,blocked_json,failed_json,entry_barrier_json,metadata_json,condition_json FROM task WHERE id=?")
            .bind(task_id).fetch_optional(&mut **tx).await?;
        let Some(row) = row else {
            return Ok(());
        }; // Hard deletion has no shadow row.
        let input = LegacyConditionInput::from_row(&row);
        let condition = decode(row.get("condition_json"))?;
        self.set_condition(tx, task_id, row.get("version"), &input, &condition)
            .await
    }

    /// Invariant usable by writer tests. Map the fetched Task's source snapshot
    /// independently of its stored shadow; never recompute and return the
    /// shadow as if it were the persisted value.
    pub async fn check_task_condition_invariant(&self, task: &Task) -> Result<()> {
        let mut connection = self.pool().acquire().await?;
        let expected =
            map_legacy_condition(&mut connection, &LegacyConditionInput::from(task)).await?;
        let actual: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
            .bind(&task.id)
            .fetch_optional(&mut *connection)
            .await?
            .ok_or(DbError::NotFound)?;
        if decode(&actual)? != expected {
            return Err(DbError::Check(format!(
                "Task condition invariant failed for {}",
                task.id
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
