//! Passive integration queue storage. Nothing here dispatches a Task or runs Git.
use crate::{begin_immediate, new_uuid_v4, now_rfc3339, DbError, Result, SqliteDb};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{Row, Sqlite, Transaction};
use std::{fmt, str::FromStr};

macro_rules! stored_enum {
    ($name:ident { $($variant:ident => $text:literal),+ $(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
        pub enum $name { $(#[serde(rename=$text)] $variant),+ }
        impl $name { pub const ALL: &'static [Self] = &[$(Self::$variant),+]; }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(match self { $(Self::$variant => $text),+ })
            }
        }
        impl FromStr for $name {
            type Err = DbError;
            fn from_str(value: &str) -> Result<Self> {
                match value { $($text => Ok(Self::$variant),)+ _ => Err(DbError::Check(format!("unknown {}: {value}", stringify!($name)))) }
            }
        }
    };
}
stored_enum!(IntegrationQueueState { Open => "open", Suspended => "suspended", Quarantined => "quarantined", Closed => "closed" });
stored_enum!(IntegrationAttemptState { Queued => "queued", PathWait => "path_wait", Validating => "validating", Rebasing => "rebasing", Checking => "checking", AwaitingTaskStep => "awaiting_task_step", ReadyFf => "ready_ff", FfInflight => "ff_inflight", Reconciling => "reconciling", Applied => "applied", Ejected => "ejected", NeedsReview => "needs_review", Parked => "parked", Quarantined => "quarantined", Completed => "completed", Cancelled => "cancelled", Superseded => "superseded" });
stored_enum!(IntegrationOutcomeKind { Admission => "admission", Candidate => "candidate", TargetTip => "target_tip", CleanRebase => "clean_rebase", ConflictHandoff => "conflict_handoff", Conflict => "conflict", ReviewRequired => "review_required", TargetMoved => "target_moved", Dirty => "dirty", TargetDirty => "target_dirty", Markers => "markers", CiPassed => "ci_passed", CiFailed => "ci_failed", OwnerOffline => "owner_offline", WorkspaceLost => "workspace_lost", Done => "done", Cancelled => "cancelled", UnsupportedConflict => "unsupported_conflict" });
stored_enum!(IntegrationFailureKind { Infrastructure => "infrastructure", TargetUnconfigured => "target_unconfigured", TargetAmbiguous => "target_ambiguous", TargetUnavailable => "target_unavailable", OwnerRequired => "owner_required", UnsupportedPath => "unsupported_path", CorruptImport => "corrupt_import", ContradictoryProof => "contradictory_proof", NeedsFact => "needs_fact", Timeout => "timeout", WorkspaceLost => "workspace_lost", CandidateCheckFailed => "candidate_check_failed" });
stored_enum!(IntegrationOwnerKind { Server => "server", Daemon => "daemon" });
stored_enum!(IntegrationOperationKind { Merge => "merge", Rebase => "rebase", Check => "check", FastForward => "fast_forward", Reconcile => "reconcile" });
stored_enum!(IntegrationOperationState { Pending => "pending", Running => "running", Succeeded => "succeeded", Failed => "failed", Uncertain => "uncertain", Acknowledged => "acknowledged" });
stored_enum!(IntegrationImportDisposition { Classified => "classified", NeedsFact => "needs_fact", Quarantined => "quarantined", Obsolete => "obsolete", History => "history" });

#[cfg(test)]
mod audit_tests;
mod importer;
pub(crate) mod shadow;
#[cfg(test)]
mod tests;
pub use importer::*;
pub use shadow::*;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IntegrationQueue {
    pub id: String,
    pub repo_id: String,
    pub target_branch: String,
    pub target_location_id: Option<String>,
    pub target_owner_json: Option<Value>,
    pub next_seq: i64,
    pub head_attempt_id: Option<String>,
    pub lease_owner: Option<String>,
    pub lease_until: Option<String>,
    pub fence_generation: i64,
    pub state: IntegrationQueueState,
    pub available_at: Option<String>,
    pub updated_at: String,
    pub created_at: String,
    pub revision: i64,
    pub last_error_kind: Option<IntegrationFailureKind>,
    pub last_error: Option<String>,
}
fn map_queue(row: sqlx::sqlite::SqliteRow) -> Result<IntegrationQueue> {
    Ok(IntegrationQueue {
        id: row.try_get("id")?,
        repo_id: row.try_get("repo_id")?,
        target_branch: row.try_get("target_branch")?,
        target_location_id: row.try_get("target_location_id")?,
        target_owner_json: row
            .try_get::<Option<String>, _>("target_owner_json")?
            .map(parse_json)
            .transpose()?,
        next_seq: row.try_get("next_seq")?,
        head_attempt_id: row.try_get("head_attempt_id")?,
        lease_owner: row.try_get("lease_owner")?,
        lease_until: row.try_get("lease_until")?,
        fence_generation: row.try_get("fence_generation")?,
        state: row.try_get::<String, _>("state")?.parse()?,
        available_at: row.try_get("available_at")?,
        updated_at: row.try_get("updated_at")?,
        created_at: row.try_get("created_at")?,
        revision: row.try_get("revision")?,
        last_error_kind: row
            .try_get::<Option<String>, _>("last_error_kind")?
            .map(|s| s.parse())
            .transpose()?,
        last_error: row.try_get("last_error")?,
    })
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IntegrationAttempt {
    pub id: String,
    pub queue_id: Option<String>,
    pub task_id: Option<String>,
    pub task_ref: String,
    pub project_ref: String,
    pub queue_seq: i64,
    pub attempt_number: i64,
    pub predecessor_attempt_id: Option<String>,
    pub current: bool,
    pub admission_key: String,
    pub expected_status: String,
    pub expected_epoch: i64,
    pub observed_task_version: i64,
    pub workflow_ref_id: Option<String>,
    pub enqueued_at: String,
    pub execution_id: Option<String>,
    pub execution_ref: Option<String>,
    pub workspace_id: Option<String>,
    pub workspace_ref: Option<String>,
    pub placement_id: Option<String>,
    pub placement_ref: Option<String>,
    pub repo_location_id: Option<String>,
    pub repo_location_ref: Option<String>,
    pub owner_kind: Option<IntegrationOwnerKind>,
    pub daemon_id: Option<String>,
    pub runtime_id: Option<String>,
    pub placement_generation: Option<i64>,
    pub original_candidate_sha: Option<String>,
    pub candidate_sha: Option<String>,
    pub target_tip_sha: Option<String>,
    pub contract_execution_id: Option<String>,
    pub review_id: Option<String>,
    pub reviewed_paths_json: Option<Value>,
    pub changed_paths_json: Option<Value>,
    pub conflict_paths_json: Option<Value>,
    pub repair_paths_json: Option<Value>,
    pub guard_paths_json: Option<Value>,
    pub state: IntegrationAttemptState,
    pub resume_state: Option<IntegrationAttemptState>,
    pub failure_kind: Option<IntegrationFailureKind>,
    pub failure_message: Option<String>,
    pub slot_generation: i64,
    pub permit_json: Option<Value>,
    pub operation_kind: Option<IntegrationOperationKind>,
    pub operation_id: Option<String>,
    pub current_operation_state: Option<IntegrationOperationState>,
    pub operation_receipts_json: Value,
    pub checks_json: Option<Value>,
    pub checks_commit_sha: Option<String>,
    pub deadline: Option<String>,
    pub effect_seq: i64,
    pub effect_ack_json: Option<Value>,
    pub acknowledged_at: Option<String>,
    pub integrated_before_sha: Option<String>,
    pub integrated_sha: Option<String>,
    pub available_at: Option<String>,
    pub last_error_kind: Option<IntegrationFailureKind>,
    pub last_error: Option<String>,
    pub started_at: Option<String>,
    pub updated_at: String,
    pub created_at: String,
    pub completed_at: Option<String>,
    pub import_source_json: Option<Value>,
    pub observations_json: Value,
    pub observations_dropped: i64,
    pub revision: i64,
}
fn map_attempt(row: sqlx::sqlite::SqliteRow) -> Result<IntegrationAttempt> {
    Ok(IntegrationAttempt {
        id: row.try_get("id")?,
        queue_id: row.try_get("queue_id")?,
        task_id: row.try_get("task_id")?,
        task_ref: row.try_get("task_ref")?,
        project_ref: row.try_get("project_ref")?,
        queue_seq: row.try_get("queue_seq")?,
        attempt_number: row.try_get("attempt_number")?,
        predecessor_attempt_id: row.try_get("predecessor_attempt_id")?,
        current: row.try_get("current")?,
        admission_key: row.try_get("admission_key")?,
        expected_status: row.try_get("expected_status")?,
        expected_epoch: row.try_get("expected_epoch")?,
        observed_task_version: row.try_get("observed_task_version")?,
        workflow_ref_id: row.try_get("workflow_ref_id")?,
        enqueued_at: row.try_get("enqueued_at")?,
        execution_id: row.try_get("execution_id")?,
        execution_ref: row.try_get("execution_ref")?,
        workspace_id: row.try_get("workspace_id")?,
        workspace_ref: row.try_get("workspace_ref")?,
        placement_id: row.try_get("placement_id")?,
        placement_ref: row.try_get("placement_ref")?,
        repo_location_id: row.try_get("repo_location_id")?,
        repo_location_ref: row.try_get("repo_location_ref")?,
        owner_kind: row
            .try_get::<Option<String>, _>("owner_kind")?
            .map(|s| s.parse())
            .transpose()?,
        daemon_id: row.try_get("daemon_id")?,
        runtime_id: row.try_get("runtime_id")?,
        placement_generation: row.try_get("placement_generation")?,
        original_candidate_sha: row.try_get("original_candidate_sha")?,
        candidate_sha: row.try_get("candidate_sha")?,
        target_tip_sha: row.try_get("target_tip_sha")?,
        contract_execution_id: row.try_get("contract_execution_id")?,
        review_id: row.try_get("review_id")?,
        reviewed_paths_json: row
            .try_get::<Option<String>, _>("reviewed_paths_json")?
            .map(parse_json)
            .transpose()?,
        changed_paths_json: row
            .try_get::<Option<String>, _>("changed_paths_json")?
            .map(parse_json)
            .transpose()?,
        conflict_paths_json: row
            .try_get::<Option<String>, _>("conflict_paths_json")?
            .map(parse_json)
            .transpose()?,
        repair_paths_json: row
            .try_get::<Option<String>, _>("repair_paths_json")?
            .map(parse_json)
            .transpose()?,
        guard_paths_json: row
            .try_get::<Option<String>, _>("guard_paths_json")?
            .map(parse_json)
            .transpose()?,
        state: row.try_get::<String, _>("state")?.parse()?,
        resume_state: row
            .try_get::<Option<String>, _>("resume_state")?
            .map(|s| s.parse())
            .transpose()?,
        failure_kind: row
            .try_get::<Option<String>, _>("failure_kind")?
            .map(|s| s.parse())
            .transpose()?,
        failure_message: row.try_get("failure_message")?,
        slot_generation: row.try_get("slot_generation")?,
        permit_json: row
            .try_get::<Option<String>, _>("permit_json")?
            .map(parse_json)
            .transpose()?,
        operation_kind: row
            .try_get::<Option<String>, _>("operation_kind")?
            .map(|s| s.parse())
            .transpose()?,
        operation_id: row.try_get("operation_id")?,
        current_operation_state: row
            .try_get::<Option<String>, _>("current_operation_state")?
            .map(|s| s.parse())
            .transpose()?,
        operation_receipts_json: parse_json(row.try_get("operation_receipts_json")?)?,
        checks_json: row
            .try_get::<Option<String>, _>("checks_json")?
            .map(parse_json)
            .transpose()?,
        checks_commit_sha: row.try_get("checks_commit_sha")?,
        deadline: row.try_get("deadline")?,
        effect_seq: row.try_get("effect_seq")?,
        effect_ack_json: row
            .try_get::<Option<String>, _>("effect_ack_json")?
            .map(parse_json)
            .transpose()?,
        acknowledged_at: row.try_get("acknowledged_at")?,
        integrated_before_sha: row.try_get("integrated_before_sha")?,
        integrated_sha: row.try_get("integrated_sha")?,
        available_at: row.try_get("available_at")?,
        last_error_kind: row
            .try_get::<Option<String>, _>("last_error_kind")?
            .map(|s| s.parse())
            .transpose()?,
        last_error: row.try_get("last_error")?,
        started_at: row.try_get("started_at")?,
        updated_at: row.try_get("updated_at")?,
        created_at: row.try_get("created_at")?,
        completed_at: row.try_get("completed_at")?,
        import_source_json: row
            .try_get::<Option<String>, _>("import_source_json")?
            .map(parse_json)
            .transpose()?,
        observations_json: parse_json(row.try_get("observations_json")?)?,
        observations_dropped: row.try_get("observations_dropped")?,
        revision: row.try_get("revision")?,
    })
}
fn parse_json(s: String) -> Result<Value> {
    serde_json::from_str(&s).map_err(|e| DbError::Check(format!("invalid integration JSON: {e}")))
}
impl IntegrationAttempt {
    /// An exact admission snapshot; callers fill facts they actually possess.
    pub fn new(
        queue_id: Option<String>,
        task_ref: String,
        project_ref: String,
        admission_key: String,
        expected_status: String,
        expected_epoch: i64,
        observed_task_version: i64,
    ) -> Self {
        let now = now_rfc3339();
        Self {
            id: new_uuid_v4(),
            queue_id,
            task_id: Some(task_ref.clone()),
            task_ref,
            project_ref,
            queue_seq: 1,
            attempt_number: 1,
            predecessor_attempt_id: None,
            current: true,
            admission_key,
            expected_status,
            expected_epoch,
            observed_task_version,
            workflow_ref_id: None,
            enqueued_at: now.clone(),
            execution_id: None,
            execution_ref: None,
            workspace_id: None,
            workspace_ref: None,
            placement_id: None,
            placement_ref: None,
            repo_location_id: None,
            repo_location_ref: None,
            owner_kind: None,
            daemon_id: None,
            runtime_id: None,
            placement_generation: None,
            original_candidate_sha: None,
            candidate_sha: None,
            target_tip_sha: None,
            contract_execution_id: None,
            review_id: None,
            reviewed_paths_json: None,
            changed_paths_json: None,
            conflict_paths_json: None,
            repair_paths_json: None,
            guard_paths_json: None,
            state: IntegrationAttemptState::Queued,
            resume_state: None,
            failure_kind: None,
            failure_message: None,
            slot_generation: 0,
            permit_json: None,
            operation_kind: None,
            operation_id: None,
            current_operation_state: None,
            operation_receipts_json: serde_json::json!([]),
            checks_json: None,
            checks_commit_sha: None,
            deadline: None,
            effect_seq: 0,
            effect_ack_json: None,
            acknowledged_at: None,
            integrated_before_sha: None,
            integrated_sha: None,
            available_at: None,
            last_error_kind: None,
            last_error: None,
            started_at: None,
            updated_at: now.clone(),
            created_at: now.clone(),
            completed_at: None,
            import_source_json: None,
            observations_json: serde_json::json!([]),
            observations_dropped: 0,
            revision: 1,
        }
    }
}
async fn insert_attempt(tx: &mut Transaction<'_, Sqlite>, a: &IntegrationAttempt) -> Result<()> {
    validate_attempt(a)?;
    sqlx::query("INSERT INTO integration_attempt (id,queue_id,task_id,task_ref,project_ref,queue_seq,attempt_number,predecessor_attempt_id,current,admission_key,expected_status,expected_epoch,observed_task_version,workflow_ref_id,enqueued_at,execution_id,execution_ref,workspace_id,workspace_ref,placement_id,placement_ref,repo_location_id,repo_location_ref,owner_kind,daemon_id,runtime_id,placement_generation,original_candidate_sha,candidate_sha,target_tip_sha,contract_execution_id,review_id,reviewed_paths_json,changed_paths_json,conflict_paths_json,repair_paths_json,guard_paths_json,state,resume_state,failure_kind,failure_message,slot_generation,permit_json,operation_kind,operation_id,current_operation_state,operation_receipts_json,checks_json,checks_commit_sha,deadline,effect_seq,effect_ack_json,acknowledged_at,integrated_before_sha,integrated_sha,available_at,last_error_kind,last_error,started_at,updated_at,created_at,completed_at,import_source_json,observations_json,observations_dropped,revision) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        .bind(&a.id)
        .bind(&a.queue_id)
        .bind(&a.task_id)
        .bind(&a.task_ref)
        .bind(&a.project_ref)
        .bind(a.queue_seq)
        .bind(a.attempt_number)
        .bind(&a.predecessor_attempt_id)
        .bind(a.current)
        .bind(&a.admission_key)
        .bind(&a.expected_status)
        .bind(a.expected_epoch)
        .bind(a.observed_task_version)
        .bind(&a.workflow_ref_id)
        .bind(&a.enqueued_at)
        .bind(&a.execution_id)
        .bind(&a.execution_ref)
        .bind(&a.workspace_id)
        .bind(&a.workspace_ref)
        .bind(&a.placement_id)
        .bind(&a.placement_ref)
        .bind(&a.repo_location_id)
        .bind(&a.repo_location_ref)
        .bind(a.owner_kind.as_ref().map(ToString::to_string))
        .bind(&a.daemon_id)
        .bind(&a.runtime_id)
        .bind(a.placement_generation)
        .bind(&a.original_candidate_sha)
        .bind(&a.candidate_sha)
        .bind(&a.target_tip_sha)
        .bind(&a.contract_execution_id)
        .bind(&a.review_id)
        .bind(a.reviewed_paths_json.as_ref().map(ToString::to_string))
        .bind(a.changed_paths_json.as_ref().map(ToString::to_string))
        .bind(a.conflict_paths_json.as_ref().map(ToString::to_string))
        .bind(a.repair_paths_json.as_ref().map(ToString::to_string))
        .bind(a.guard_paths_json.as_ref().map(ToString::to_string))
        .bind(a.state.to_string())
        .bind(a.resume_state.as_ref().map(ToString::to_string))
        .bind(a.failure_kind.as_ref().map(ToString::to_string))
        .bind(&a.failure_message)
        .bind(a.slot_generation)
        .bind(a.permit_json.as_ref().map(ToString::to_string))
        .bind(a.operation_kind.as_ref().map(ToString::to_string))
        .bind(&a.operation_id)
        .bind(a.current_operation_state.as_ref().map(ToString::to_string))
        .bind(a.operation_receipts_json.to_string())
        .bind(a.checks_json.as_ref().map(ToString::to_string))
        .bind(&a.checks_commit_sha)
        .bind(&a.deadline)
        .bind(a.effect_seq)
        .bind(a.effect_ack_json.as_ref().map(ToString::to_string))
        .bind(&a.acknowledged_at)
        .bind(&a.integrated_before_sha)
        .bind(&a.integrated_sha)
        .bind(&a.available_at)
        .bind(a.last_error_kind.as_ref().map(ToString::to_string))
        .bind(&a.last_error)
        .bind(&a.started_at)
        .bind(&a.updated_at)
        .bind(&a.created_at)
        .bind(&a.completed_at)
        .bind(a.import_source_json.as_ref().map(ToString::to_string))
        .bind(a.observations_json.to_string())
        .bind(a.observations_dropped)
        .bind(a.revision)
        .execute(&mut **tx).await?;
    Ok(())
}
/// Never writes `observations_json` / `observations_dropped`: only the
/// single-statement observation append owns those two columns.
async fn update_attempt(tx: &mut Transaction<'_, Sqlite>, a: &IntegrationAttempt) -> Result<()> {
    validate_attempt(a)?;
    let n = sqlx::query("UPDATE integration_attempt SET current=?,execution_id=?,execution_ref=?,workspace_id=?,workspace_ref=?,placement_id=?,placement_ref=?,repo_location_id=?,repo_location_ref=?,owner_kind=?,daemon_id=?,runtime_id=?,placement_generation=?,original_candidate_sha=?,candidate_sha=?,target_tip_sha=?,contract_execution_id=?,review_id=?,reviewed_paths_json=?,changed_paths_json=?,conflict_paths_json=?,repair_paths_json=?,guard_paths_json=?,state=?,resume_state=?,failure_kind=?,failure_message=?,slot_generation=?,permit_json=?,operation_kind=?,operation_id=?,current_operation_state=?,operation_receipts_json=?,checks_json=?,checks_commit_sha=?,deadline=?,effect_seq=?,effect_ack_json=?,acknowledged_at=?,integrated_before_sha=?,integrated_sha=?,available_at=?,last_error_kind=?,last_error=?,started_at=?,updated_at=?,completed_at=?,import_source_json=?,revision=revision+1 WHERE id=? AND revision=?")
        .bind(a.current)
        .bind(&a.execution_id)
        .bind(&a.execution_ref)
        .bind(&a.workspace_id)
        .bind(&a.workspace_ref)
        .bind(&a.placement_id)
        .bind(&a.placement_ref)
        .bind(&a.repo_location_id)
        .bind(&a.repo_location_ref)
        .bind(a.owner_kind.as_ref().map(ToString::to_string))
        .bind(&a.daemon_id)
        .bind(&a.runtime_id)
        .bind(a.placement_generation)
        .bind(&a.original_candidate_sha)
        .bind(&a.candidate_sha)
        .bind(&a.target_tip_sha)
        .bind(&a.contract_execution_id)
        .bind(&a.review_id)
        .bind(a.reviewed_paths_json.as_ref().map(ToString::to_string))
        .bind(a.changed_paths_json.as_ref().map(ToString::to_string))
        .bind(a.conflict_paths_json.as_ref().map(ToString::to_string))
        .bind(a.repair_paths_json.as_ref().map(ToString::to_string))
        .bind(a.guard_paths_json.as_ref().map(ToString::to_string))
        .bind(a.state.to_string())
        .bind(a.resume_state.as_ref().map(ToString::to_string))
        .bind(a.failure_kind.as_ref().map(ToString::to_string))
        .bind(&a.failure_message)
        .bind(a.slot_generation)
        .bind(a.permit_json.as_ref().map(ToString::to_string))
        .bind(a.operation_kind.as_ref().map(ToString::to_string))
        .bind(&a.operation_id)
        .bind(a.current_operation_state.as_ref().map(ToString::to_string))
        .bind(a.operation_receipts_json.to_string())
        .bind(a.checks_json.as_ref().map(ToString::to_string))
        .bind(&a.checks_commit_sha)
        .bind(&a.deadline)
        .bind(a.effect_seq)
        .bind(a.effect_ack_json.as_ref().map(ToString::to_string))
        .bind(&a.acknowledged_at)
        .bind(&a.integrated_before_sha)
        .bind(&a.integrated_sha)
        .bind(&a.available_at)
        .bind(a.last_error_kind.as_ref().map(ToString::to_string))
        .bind(&a.last_error)
        .bind(&a.started_at)
        .bind(&a.updated_at)
        .bind(&a.completed_at)
        .bind(a.import_source_json.as_ref().map(ToString::to_string))
        .bind(&a.id).bind(a.revision).execute(&mut **tx).await?.rows_affected();
    if n != 1 {
        return Err(DbError::VersionConflict);
    }
    Ok(())
}

/// The passive attempt graph is data, shared by transitions and validation.
/// Critical/uncertain effects cannot be declared cancelled by a clock alone.
pub const INTEGRATION_TRANSITIONS: &[(IntegrationAttemptState, &[IntegrationAttemptState])] = &[
    (
        IntegrationAttemptState::Queued,
        &[
            IntegrationAttemptState::PathWait,
            IntegrationAttemptState::Validating,
            IntegrationAttemptState::Parked,
            IntegrationAttemptState::Cancelled,
            IntegrationAttemptState::Superseded,
        ],
    ),
    (
        IntegrationAttemptState::PathWait,
        &[
            IntegrationAttemptState::Queued,
            IntegrationAttemptState::Validating,
            IntegrationAttemptState::Parked,
            IntegrationAttemptState::Cancelled,
            IntegrationAttemptState::Superseded,
        ],
    ),
    (
        IntegrationAttemptState::Validating,
        &[
            IntegrationAttemptState::Applied,
            IntegrationAttemptState::NeedsReview,
            IntegrationAttemptState::Parked,
            IntegrationAttemptState::Rebasing,
            IntegrationAttemptState::Cancelled,
            IntegrationAttemptState::Superseded,
        ],
    ),
    (
        IntegrationAttemptState::Rebasing,
        &[
            IntegrationAttemptState::Checking,
            IntegrationAttemptState::Ejected,
            IntegrationAttemptState::Parked,
            IntegrationAttemptState::Reconciling,
            IntegrationAttemptState::Cancelled,
        ],
    ),
    (
        IntegrationAttemptState::Checking,
        &[
            IntegrationAttemptState::AwaitingTaskStep,
            IntegrationAttemptState::Ejected,
            IntegrationAttemptState::Parked,
            IntegrationAttemptState::Cancelled,
        ],
    ),
    (
        IntegrationAttemptState::AwaitingTaskStep,
        &[
            IntegrationAttemptState::ReadyFf,
            IntegrationAttemptState::NeedsReview,
            IntegrationAttemptState::Ejected,
            IntegrationAttemptState::Parked,
            IntegrationAttemptState::Cancelled,
        ],
    ),
    (
        IntegrationAttemptState::ReadyFf,
        &[
            IntegrationAttemptState::AwaitingTaskStep,
            IntegrationAttemptState::FfInflight,
            IntegrationAttemptState::NeedsReview,
            IntegrationAttemptState::Parked,
            IntegrationAttemptState::Cancelled,
        ],
    ),
    (
        IntegrationAttemptState::FfInflight,
        &[
            IntegrationAttemptState::Applied,
            IntegrationAttemptState::Rebasing,
            IntegrationAttemptState::Reconciling,
        ],
    ),
    (
        IntegrationAttemptState::Reconciling,
        &[
            IntegrationAttemptState::Applied,
            IntegrationAttemptState::Rebasing,
            IntegrationAttemptState::ReadyFf,
            IntegrationAttemptState::Parked,
            IntegrationAttemptState::Quarantined,
        ],
    ),
    (
        IntegrationAttemptState::Applied,
        &[IntegrationAttemptState::Completed],
    ),
    (
        IntegrationAttemptState::Ejected,
        &[
            IntegrationAttemptState::NeedsReview,
            IntegrationAttemptState::Parked,
            IntegrationAttemptState::Cancelled,
            IntegrationAttemptState::Superseded,
        ],
    ),
    (
        IntegrationAttemptState::NeedsReview,
        &[
            IntegrationAttemptState::Parked,
            IntegrationAttemptState::Cancelled,
            IntegrationAttemptState::Superseded,
        ],
    ),
    (
        IntegrationAttemptState::Parked,
        &[
            IntegrationAttemptState::Queued,
            IntegrationAttemptState::Rebasing,
            IntegrationAttemptState::Checking,
            IntegrationAttemptState::NeedsReview,
            IntegrationAttemptState::Ejected,
            IntegrationAttemptState::Cancelled,
            IntegrationAttemptState::Superseded,
        ],
    ),
    (
        IntegrationAttemptState::Quarantined,
        &[
            IntegrationAttemptState::Reconciling,
            IntegrationAttemptState::Parked,
            IntegrationAttemptState::Queued,
            IntegrationAttemptState::Superseded,
        ],
    ),
    (IntegrationAttemptState::Completed, &[]),
    (IntegrationAttemptState::Cancelled, &[]),
    (IntegrationAttemptState::Superseded, &[]),
];
impl IntegrationAttemptState {
    pub fn terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled | Self::Superseded)
    }
    pub fn exits(self) -> &'static [Self] {
        INTEGRATION_TRANSITIONS
            .iter()
            .find(|(state, _)| *state == self)
            .expect("total transition table")
            .1
    }
}

fn validate_branch(branch: &str) -> Result<String> {
    let branch = branch.strip_prefix("refs/heads/").unwrap_or(branch);
    if branch.is_empty()
        || branch == "@"
        || branch.starts_with('-')
        || branch.starts_with('/')
        || branch.ends_with('/')
        || branch.ends_with('.')
        || branch.contains("..")
        || branch.contains("@{")
        || branch.contains("//")
        || branch
            .chars()
            .any(|c| c.is_control() || " ~^:?*[\\".contains(c))
        || branch
            .split('/')
            .any(|p| p.starts_with('.') || p.ends_with(".lock"))
    {
        return Err(DbError::Check("invalid integration target branch".into()));
    }
    Ok(branch.to_owned())
}
/// Exact repo-relative UTF-8 identity: no case folding, Unicode normalization,
/// lossy conversion or rename-endpoint collapsing. Unknown sets are `None`.
pub fn validate_integration_paths(value: &Value) -> Result<()> {
    let paths = value
        .as_array()
        .ok_or_else(|| DbError::Check("integration paths must be an array".into()))?;
    for path in paths {
        let path = path
            .as_str()
            .ok_or_else(|| DbError::Check("integration path must be UTF-8 text".into()))?;
        if path.is_empty()
            || path.starts_with('/')
            || path.contains('\0')
            || path
                .split('/')
                .any(|p| p.is_empty() || p == "." || p == "..")
        {
            return Err(DbError::Check("unsupported integration path".into()));
        }
    }
    Ok(())
}
fn integration_time(value: &str) -> Result<chrono::DateTime<chrono::FixedOffset>> {
    chrono::DateTime::parse_from_rfc3339(value)
        .map_err(|_| DbError::Check("invalid integration timestamp".into()))
}
fn validate_attempt(a: &IntegrationAttempt) -> Result<()> {
    for set in [
        &a.reviewed_paths_json,
        &a.changed_paths_json,
        &a.conflict_paths_json,
        &a.repair_paths_json,
        &a.guard_paths_json,
    ]
    .into_iter()
    .flatten()
    {
        validate_integration_paths(set)?;
    }
    for at in [&a.available_at, &a.deadline, &a.started_at, &a.completed_at]
        .into_iter()
        .flatten()
    {
        integration_time(at)?;
    }
    if !a.operation_receipts_json.is_array() || !a.observations_json.is_array() {
        return Err(DbError::Check(
            "integration receipts and observations must be arrays".into(),
        ));
    }
    if matches!(
        a.state,
        IntegrationAttemptState::ReadyFf | IntegrationAttemptState::FfInflight
    ) {
        let candidate = a
            .candidate_sha
            .as_deref()
            .filter(|sha| !sha.is_empty())
            .ok_or_else(|| {
                DbError::Check("ready integration requires an exact candidate".into())
            })?;
        let target = a
            .target_tip_sha
            .as_deref()
            .filter(|sha| !sha.is_empty())
            .ok_or_else(|| {
                DbError::Check("ready integration requires an exact target tip".into())
            })?;
        let permit = a
            .permit_json
            .as_ref()
            .filter(|permit| permit.is_object())
            .ok_or_else(|| DbError::Check("ready integration requires a bound permit".into()))?;
        if permit["candidate_sha"].as_str() != Some(candidate)
            || permit["target_tip_sha"].as_str() != Some(target)
            || permit["task_ref"].as_str() != Some(&a.task_ref)
            || permit["expected_epoch"].as_i64() != Some(a.expected_epoch)
            || permit["slot_generation"].as_i64() != Some(a.slot_generation)
            || (a.checks_json.is_some() && a.checks_commit_sha.as_deref() != Some(candidate))
        {
            return Err(DbError::Check("integration permit/check facts do not bind this candidate, target, Task entry and slot".into()));
        }
    }
    if a.state.terminal() && a.current {
        return Err(DbError::Check(
            "terminal integration attempt is not current".into(),
        ));
    }
    for text in [&a.failure_message, &a.last_error].into_iter().flatten() {
        if text.len() > 4096 {
            return Err(DbError::Check(
                "integration diagnostic exceeds bound".into(),
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IntegrationTarget {
    pub location_id: Option<String>,
    pub owner: Option<Value>,
    pub failure: Option<IntegrationFailureKind>,
}
/// Single target-selection seam. Read only the repo's configured local_path
/// and explicitly-default primary_checkout locations. Neither the Task's
/// placement nor a daemon's mere presence selects an integration target.
async fn resolve_integration_target_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    repo_id: &str,
) -> Result<IntegrationTarget> {
    let local_path: Option<String> = sqlx::query_scalar("SELECT local_path FROM repo WHERE id=?")
        .bind(repo_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(DbError::NotFound)?;
    let locations = sqlx::query("SELECT id,owner_kind,daemon_id,runtime_id,path,status,version FROM repo_location WHERE repo_id=? AND is_default=1 AND kind='primary_checkout' ORDER BY id")
        .bind(repo_id).fetch_all(&mut **tx).await?;
    let failure = if locations.is_empty() {
        Some(IntegrationFailureKind::TargetUnconfigured)
    } else if locations.len() != 1 {
        Some(IntegrationFailureKind::TargetAmbiguous)
    } else {
        None
    };
    if failure.is_some() {
        return Ok(IntegrationTarget {
            location_id: None,
            owner: None,
            failure,
        });
    }
    let location = &locations[0];
    let owner_kind: String = location.try_get("owner_kind")?;
    let path: String = location.try_get("path")?;
    if local_path
        .as_ref()
        .is_some_and(|configured| owner_kind != "server" || configured != &path)
    {
        return Ok(IntegrationTarget {
            location_id: None,
            owner: None,
            failure: Some(IntegrationFailureKind::TargetAmbiguous),
        });
    }
    let id: String = location.try_get("id")?;
    let status: String = location.try_get("status")?;
    Ok(IntegrationTarget {
        location_id: Some(id.clone()),
        owner: Some(
            serde_json::json!({"location_id":id,"owner_kind":owner_kind,"daemon_id":location.try_get::<Option<String>, _>("daemon_id")?,"runtime_id":location.try_get::<Option<String>, _>("runtime_id")?,"generation":location.try_get::<i64, _>("version")?}),
        ),
        failure: (status != "ready").then_some(IntegrationFailureKind::TargetUnavailable),
    })
}

/// A queue whose target location is being deleted keeps its members and
/// history but names no location: `suspended` with `target_unconfigured`.
/// The foreign key alone would only null the column (`ON DELETE SET NULL`).
pub(crate) async fn suspend_queues_for_deleted_location(
    tx: &mut Transaction<'_, Sqlite>,
    location_id: &str,
) -> Result<u64> {
    Ok(sqlx::query("UPDATE integration_queue SET target_location_id=NULL,target_owner_json=NULL,state=CASE WHEN state IN ('open','suspended') THEN 'suspended' ELSE state END,last_error_kind='target_unconfigured',last_error='target repo location deleted',revision=revision+1,updated_at=? WHERE target_location_id=?")
        .bind(now_rfc3339())
        .bind(location_id)
        .execute(&mut **tx)
        .await?
        .rows_affected())
}

#[async_trait]
pub trait IntegrationQueueRepo: Send + Sync {
    async fn create_or_get_integration_queue(
        &self,
        repo_id: &str,
        target_branch: &str,
    ) -> Result<IntegrationQueue>;
    async fn integration_queue(&self, id: &str) -> Result<Option<IntegrationQueue>>;
    async fn admit_integration_attempt(
        &self,
        attempt: IntegrationAttempt,
    ) -> Result<IntegrationAttempt>;
    async fn integration_attempt(&self, id: &str) -> Result<Option<IntegrationAttempt>>;
    async fn current_integration_attempt(
        &self,
        task_ref: &str,
    ) -> Result<Option<IntegrationAttempt>>;
    async fn integration_members(
        &self,
        queue_id: &str,
        limit: u32,
    ) -> Result<Vec<IntegrationAttempt>>;
    async fn integration_head(&self, queue_id: &str) -> Result<Option<IntegrationAttempt>>;
    async fn claim_integration_queue(
        &self,
        queue_id: &str,
        expected_revision: i64,
        owner: &str,
        now: &str,
        lease_until: &str,
    ) -> Result<IntegrationQueue>;
    async fn renew_integration_queue(
        &self,
        queue_id: &str,
        expected_revision: i64,
        owner: &str,
        fence_generation: i64,
        now: &str,
        lease_until: &str,
    ) -> Result<IntegrationQueue>;
    async fn transition_integration_attempt(
        &self,
        attempt: IntegrationAttempt,
    ) -> Result<IntegrationAttempt>;
    async fn supersede_integration_attempt(
        &self,
        predecessor_id: &str,
        expected_revision: i64,
        successor: IntegrationAttempt,
    ) -> Result<IntegrationAttempt>;
    async fn record_integration_observation(
        &self,
        attempt_id: &str,
        observation: IntegrationObservation,
    ) -> Result<()>;
    async fn integration_queue_counts(&self) -> Result<IntegrationQueueCounts>;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IntegrationQueueCounts {
    pub queues_by_state: std::collections::BTreeMap<String, i64>,
    pub current_attempts_by_state: std::collections::BTreeMap<String, i64>,
    pub quarantined_imports: i64,
}
async fn queue_in_tx(tx: &mut Transaction<'_, Sqlite>, id: &str) -> Result<IntegrationQueue> {
    map_queue(
        sqlx::query("SELECT * FROM integration_queue WHERE id=?")
            .bind(id)
            .fetch_optional(&mut **tx)
            .await?
            .ok_or(DbError::NotFound)?,
    )
}
async fn attempt_in_tx(tx: &mut Transaction<'_, Sqlite>, id: &str) -> Result<IntegrationAttempt> {
    map_attempt(
        sqlx::query("SELECT * FROM integration_attempt WHERE id=?")
            .bind(id)
            .fetch_optional(&mut **tx)
            .await?
            .ok_or(DbError::NotFound)?,
    )
}
async fn create_queue_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    repo_id: &str,
    target_branch: &str,
) -> Result<IntegrationQueue> {
    let target_branch = validate_branch(target_branch)?;
    if let Some(row) =
        sqlx::query("SELECT * FROM integration_queue WHERE repo_id=? AND target_branch=?")
            .bind(repo_id)
            .bind(&target_branch)
            .fetch_optional(&mut **tx)
            .await?
    {
        return map_queue(row);
    }
    let target = resolve_integration_target_in_tx(tx, repo_id).await?;
    let now = now_rfc3339();
    let id = new_uuid_v4();
    sqlx::query("INSERT INTO integration_queue(id,repo_id,target_branch,target_location_id,target_owner_json,state,created_at,updated_at,last_error_kind) VALUES(?,?,?,?,?,?,?,?,?)")
        .bind(&id).bind(repo_id).bind(&target_branch).bind(target.location_id).bind(target.owner.map(|v| v.to_string()))
        .bind(if target.failure.is_some() { "suspended" } else { "open" }).bind(&now).bind(&now).bind(target.failure.map(|f| f.to_string())).execute(&mut **tx).await?;
    queue_in_tx(tx, &id).await
}
async fn admit_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    mut a: IntegrationAttempt,
) -> Result<IntegrationAttempt> {
    if let Some(row) =
        sqlx::query("SELECT * FROM integration_attempt WHERE queue_id IS ? AND admission_key=?")
            .bind(&a.queue_id)
            .bind(&a.admission_key)
            .fetch_optional(&mut **tx)
            .await?
    {
        let existing = map_attempt(row)?;
        if existing.task_ref != a.task_ref
            || existing.expected_epoch != a.expected_epoch
            || existing.original_candidate_sha != a.original_candidate_sha
        {
            return Err(DbError::IdempotencyConflict);
        }
        return Ok(existing);
    }
    if let Some(task_id) = &a.task_id {
        let project: String = sqlx::query_scalar("SELECT project_id FROM task WHERE id=?")
            .bind(task_id)
            .fetch_optional(&mut **tx)
            .await?
            .ok_or(DbError::NotFound)?;
        if task_id != &a.task_ref || project != a.project_ref {
            return Err(DbError::Check(
                "integration admission Task/Project identity differs".into(),
            ));
        }
        if let Some(queue_id) = &a.queue_id {
            let queue_project:String=sqlx::query_scalar("SELECT r.project_id FROM integration_queue q JOIN repo r ON r.id=q.repo_id WHERE q.id=?").bind(queue_id).fetch_one(&mut **tx).await?;
            if queue_project != project {
                return Err(DbError::Check(
                    "integration queue belongs to another Project".into(),
                ));
            }
        }
    }
    if a.current
        && sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM integration_attempt WHERE task_ref=? AND current=1)",
        )
        .bind(&a.task_ref)
        .fetch_one(&mut **tx)
        .await?
    {
        return Err(DbError::VersionConflict);
    }
    if let Some(queue_id) = &a.queue_id {
        let queue = queue_in_tx(tx, queue_id).await?;
        a.queue_seq = queue.next_seq;
        sqlx::query("UPDATE integration_queue SET next_seq=next_seq+1,revision=revision+1,updated_at=? WHERE id=? AND revision=?")
            .bind(&a.updated_at).bind(queue_id).bind(queue.revision).execute(&mut **tx).await?;
    } else {
        a.current = false;
    }
    if a.state.terminal() {
        a.current = false;
    }
    insert_attempt(tx, &a).await?;
    Ok(a)
}

#[async_trait]
impl IntegrationQueueRepo for SqliteDb {
    async fn create_or_get_integration_queue(
        &self,
        repo_id: &str,
        target_branch: &str,
    ) -> Result<IntegrationQueue> {
        let mut tx = begin_immediate(self.pool()).await?;
        let queue = create_queue_in_tx(&mut tx, repo_id, target_branch).await?;
        tx.commit().await?;
        Ok(queue)
    }
    async fn integration_queue(&self, id: &str) -> Result<Option<IntegrationQueue>> {
        sqlx::query("SELECT * FROM integration_queue WHERE id=?")
            .bind(id)
            .fetch_optional(self.pool())
            .await?
            .map(map_queue)
            .transpose()
    }
    async fn admit_integration_attempt(
        &self,
        attempt: IntegrationAttempt,
    ) -> Result<IntegrationAttempt> {
        let mut tx = begin_immediate(self.pool()).await?;
        let a = admit_in_tx(&mut tx, attempt).await?;
        tx.commit().await?;
        Ok(a)
    }
    async fn integration_attempt(&self, id: &str) -> Result<Option<IntegrationAttempt>> {
        sqlx::query("SELECT * FROM integration_attempt WHERE id=?")
            .bind(id)
            .fetch_optional(self.pool())
            .await?
            .map(map_attempt)
            .transpose()
    }
    async fn current_integration_attempt(
        &self,
        task_ref: &str,
    ) -> Result<Option<IntegrationAttempt>> {
        sqlx::query("SELECT * FROM integration_attempt WHERE task_ref=? AND current=1")
            .bind(task_ref)
            .fetch_optional(self.pool())
            .await?
            .map(map_attempt)
            .transpose()
    }
    async fn integration_members(
        &self,
        queue_id: &str,
        limit: u32,
    ) -> Result<Vec<IntegrationAttempt>> {
        sqlx::query("SELECT * FROM integration_attempt WHERE queue_id=? AND current=1 ORDER BY queue_seq,attempt_number,id LIMIT ?").bind(queue_id).bind(limit.clamp(1,1000)).fetch_all(self.pool()).await?.into_iter().map(map_attempt).collect()
    }
    async fn integration_head(&self, queue_id: &str) -> Result<Option<IntegrationAttempt>> {
        sqlx::query("SELECT a.* FROM integration_attempt a JOIN integration_queue q ON q.head_attempt_id=a.id WHERE q.id=?").bind(queue_id).fetch_optional(self.pool()).await?.map(map_attempt).transpose()
    }
    async fn claim_integration_queue(
        &self,
        queue_id: &str,
        expected_revision: i64,
        owner: &str,
        now: &str,
        lease_until: &str,
    ) -> Result<IntegrationQueue> {
        let now_time = integration_time(now)?;
        if owner.is_empty() || integration_time(lease_until)? <= now_time {
            return Err(DbError::Check("invalid integration lease".into()));
        }
        let mut tx = begin_immediate(self.pool()).await?;
        let mut q = queue_in_tx(&mut tx, queue_id).await?;
        if q.revision != expected_revision
            || !matches!(
                q.state,
                IntegrationQueueState::Open | IntegrationQueueState::Quarantined
            )
            || q.target_location_id.is_none()
            || (q.state == IntegrationQueueState::Quarantined && q.head_attempt_id.is_none())
            || q.lease_until
                .as_deref()
                .map(integration_time)
                .transpose()?
                .is_some_and(|until| until > now_time)
        {
            return Err(DbError::VersionConflict);
        }
        if let Some(head) = &q.head_attempt_id {
            let a = attempt_in_tx(&mut tx, head).await?;
            if a.queue_id.as_deref() != Some(queue_id) || !a.current || a.state.terminal() {
                return Err(DbError::Check("invalid reserved integration head".into()));
            }
        } else {
            q.head_attempt_id = sqlx::query_scalar("SELECT id FROM integration_attempt WHERE queue_id=? AND current=1 AND state='queued' AND (available_at IS NULL OR julianday(available_at)<=julianday(?)) ORDER BY queue_seq,id LIMIT 1").bind(queue_id).bind(now).fetch_optional(&mut *tx).await?;
            if q.head_attempt_id.is_none() {
                return Err(DbError::NotFound);
            }
        }
        let mut head = attempt_in_tx(
            &mut tx,
            q.head_attempt_id.as_deref().expect("reserved head"),
        )
        .await?;
        if q.state == IntegrationQueueState::Quarantined
            && !matches!(
                head.state,
                IntegrationAttemptState::Reconciling | IntegrationAttemptState::FfInflight
            )
        {
            return Err(DbError::VersionConflict);
        }
        // Takeover transfers observation ownership. A ready permit belongs
        // to the old fence and needs a new Task-step authorization; an
        // in-flight effect must first reconcile its original identity.
        if head.state == IntegrationAttemptState::ReadyFf {
            head.state = IntegrationAttemptState::AwaitingTaskStep;
            head.permit_json = None;
            head.effect_seq += 1;
            head.effect_ack_json = None;
            head.acknowledged_at = None;
        } else if head.state == IntegrationAttemptState::FfInflight {
            head.state = IntegrationAttemptState::Reconciling;
        }
        head.slot_generation = q.fence_generation + 1;
        head.updated_at = now.to_owned();
        update_attempt(&mut tx, &head).await?;
        let n = sqlx::query("UPDATE integration_queue SET head_attempt_id=?,lease_owner=?,lease_until=?,fence_generation=fence_generation+1,revision=revision+1,updated_at=? WHERE id=? AND revision=?")
            .bind(&q.head_attempt_id).bind(owner).bind(lease_until).bind(now).bind(queue_id).bind(expected_revision).execute(&mut *tx).await?.rows_affected();
        if n != 1 {
            return Err(DbError::VersionConflict);
        }
        let q = queue_in_tx(&mut tx, queue_id).await?;
        tx.commit().await?;
        Ok(q)
    }
    async fn renew_integration_queue(
        &self,
        queue_id: &str,
        expected_revision: i64,
        owner: &str,
        fence_generation: i64,
        now: &str,
        lease_until: &str,
    ) -> Result<IntegrationQueue> {
        if integration_time(lease_until)? <= integration_time(now)? {
            return Err(DbError::Check("invalid integration lease deadline".into()));
        }
        let mut tx = begin_immediate(self.pool()).await?;
        let n = sqlx::query("UPDATE integration_queue SET lease_until=?,revision=revision+1,updated_at=? WHERE id=? AND revision=? AND lease_owner=? AND fence_generation=? AND julianday(lease_until)>julianday(?) AND julianday(lease_until)<=julianday(?)")
            .bind(lease_until).bind(now).bind(queue_id).bind(expected_revision).bind(owner).bind(fence_generation).bind(now).bind(lease_until).execute(&mut *tx).await?.rows_affected();
        if n != 1 {
            return Err(DbError::VersionConflict);
        }
        let q = queue_in_tx(&mut tx, queue_id).await?;
        tx.commit().await?;
        Ok(q)
    }
    async fn transition_integration_attempt(
        &self,
        mut a: IntegrationAttempt,
    ) -> Result<IntegrationAttempt> {
        let mut tx = begin_immediate(self.pool()).await?;
        let old = attempt_in_tx(&mut tx, &a.id).await?;
        if old.revision != a.revision {
            return Err(DbError::VersionConflict);
        }
        if old.state.terminal() || (old.state != a.state && !old.state.exits().contains(&a.state)) {
            return Err(DbError::InvalidTransition);
        }
        if old.queue_id != a.queue_id
            || old.task_ref != a.task_ref
            || old.project_ref != a.project_ref
            || old.task_id != a.task_id
            || old.workflow_ref_id != a.workflow_ref_id
            || old.observed_task_version != a.observed_task_version
            || old.enqueued_at != a.enqueued_at
            || old.created_at != a.created_at
            || old.queue_seq != a.queue_seq
            || old.attempt_number != a.attempt_number
            || old.admission_key != a.admission_key
            || old.expected_epoch != a.expected_epoch
            || old.expected_status != a.expected_status
            || old.original_candidate_sha != a.original_candidate_sha
            || old.predecessor_attempt_id != a.predecessor_attempt_id
            || old.current != a.current
        {
            return Err(DbError::Check(
                "immutable integration admission changed".into(),
            ));
        }
        if let Some(queue_id) = &a.queue_id {
            let q = queue_in_tx(&mut tx, queue_id).await?;
            if q.head_attempt_id.as_deref() == Some(&a.id)
                && a.slot_generation != q.fence_generation
            {
                return Err(DbError::VersionConflict);
            }
        }
        a.updated_at = now_rfc3339();
        if a.state.terminal() {
            a.current = false;
            a.completed_at = Some(a.updated_at.clone());
        }
        update_attempt(&mut tx, &a).await?;
        // Release only resolved/ejected slots, never an uncertain operation.
        if matches!(
            a.state,
            IntegrationAttemptState::Completed
                | IntegrationAttemptState::Cancelled
                | IntegrationAttemptState::Superseded
                | IntegrationAttemptState::Ejected
                | IntegrationAttemptState::NeedsReview
                | IntegrationAttemptState::Parked
        ) {
            if matches!(
                a.current_operation_state,
                Some(IntegrationOperationState::Running | IntegrationOperationState::Uncertain)
            ) {
                return Err(DbError::InvalidTransition);
            }
            sqlx::query("UPDATE integration_queue SET head_attempt_id=NULL,lease_owner=NULL,lease_until=NULL,revision=revision+1,updated_at=? WHERE head_attempt_id=?")
                .bind(&a.updated_at).bind(&a.id).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        a.revision += 1;
        Ok(a)
    }
    async fn supersede_integration_attempt(
        &self,
        predecessor_id: &str,
        expected_revision: i64,
        mut successor: IntegrationAttempt,
    ) -> Result<IntegrationAttempt> {
        let mut tx = begin_immediate(self.pool()).await?;
        let mut old = attempt_in_tx(&mut tx, predecessor_id).await?;
        if old.revision != expected_revision || !old.current {
            return Err(DbError::VersionConflict);
        }
        if !old
            .state
            .exits()
            .contains(&IntegrationAttemptState::Superseded)
            || old.task_ref != successor.task_ref
            || old.queue_id != successor.queue_id
            || old.project_ref != successor.project_ref
            || matches!(
                old.current_operation_state,
                Some(IntegrationOperationState::Running | IntegrationOperationState::Uncertain)
            )
        {
            return Err(DbError::InvalidTransition);
        }
        if sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM integration_queue WHERE head_attempt_id=?)",
        )
        .bind(predecessor_id)
        .fetch_one(&mut *tx)
        .await?
        {
            return Err(DbError::InvalidTransition);
        }
        old.current = false;
        old.state = IntegrationAttemptState::Superseded;
        old.updated_at = now_rfc3339();
        old.completed_at = Some(old.updated_at.clone());
        update_attempt(&mut tx, &old).await?;
        successor.queue_seq = old.queue_seq;
        successor.attempt_number = old.attempt_number + 1;
        successor.predecessor_attempt_id = Some(old.id);
        successor.current = true;
        successor.revision = 1;
        insert_attempt(&mut tx, &successor).await?;
        tx.commit().await?;
        Ok(successor)
    }
    async fn record_integration_observation(
        &self,
        attempt_id: &str,
        observation: IntegrationObservation,
    ) -> Result<()> {
        let mut tx = begin_immediate(self.pool()).await?;
        shadow::record_observation_in_tx(&mut tx, attempt_id, &observation).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn integration_queue_counts(&self) -> Result<IntegrationQueueCounts> {
        let mut counts = IntegrationQueueCounts::default();
        for state in IntegrationQueueState::ALL {
            counts.queues_by_state.insert(state.to_string(), 0);
        }
        for state in IntegrationAttemptState::ALL {
            counts
                .current_attempts_by_state
                .insert(state.to_string(), 0);
        }
        // A single read statement gives the counts one SQLite snapshot.
        for row in sqlx::query("SELECT 'queue' kind,state,COUNT(*) n FROM integration_queue GROUP BY state UNION ALL SELECT 'attempt',state,COUNT(*) FROM integration_attempt WHERE current=1 GROUP BY state UNION ALL SELECT 'quarantine','quarantined',COUNT(*) FROM integration_attempt WHERE import_source_json IS NOT NULL AND json_extract(import_source_json,'$.disposition')='quarantined'").fetch_all(self.pool()).await? {
            let kind: String = row.try_get("kind")?;
            let state: String = row.try_get("state")?;
            let n: i64 = row.try_get("n")?;
            match kind.as_str() { "queue" => { counts.queues_by_state.insert(state,n); }, "attempt" => { counts.current_attempts_by_state.insert(state,n); }, _ => counts.quarantined_imports=n }
        }
        Ok(counts)
    }
}
