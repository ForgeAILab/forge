//! Passive check repositories. No Task/Review writes, scheduling or execution.
use crate::{begin_immediate, new_uuid_v4, now_rfc3339, DbError, Result, SqliteDb};
use api_types::{CheckCommandOutcome, CheckDigestInput, CheckPurpose, CheckScope};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqliteRow, Row};
use std::{collections::BTreeMap, fmt, str::FromStr};

mod delivery;
mod worker_store;
pub use delivery::*;
pub use worker_store::*;

pub const CHECK_INPUT_BYTES: usize = 131_072;
pub const CHECK_STEPS_BYTES: usize = 262_144;
pub const CHECK_OUTPUT_TAIL_BYTES: usize = 4096;

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
stored_enum!(CheckRunState { Queued => "queued", Running => "running", Cancelling => "cancelling", Cleaning => "cleaning", Uncertain => "uncertain", Succeeded => "succeeded", Failed => "failed", Cancelled => "cancelled" });
stored_enum!(CheckResultOutcome { Pass => "pass", Fail => "fail", TimedOut => "timed_out", Cancelled => "cancelled", InfrastructureFailed => "infrastructure_failed" });
stored_enum!(CheckCleanup { Success => "success", Failed => "failed", Uncertain => "uncertain", NotPerformed => "not_performed" });
stored_enum!(CheckConsumerOrigin { Entry => "entry", ManualReview => "manual_review", Conformance => "conformance", BeforeWork => "before_work", Lifecycle => "lifecycle", Environment => "environment", Integration => "integration" });

/// State-machine DATA; lease renewal is a fenced self transition.
/// Edges are performed by: the claim (queued to running; and, when it takes
/// over an expired lease, running, cancelling or cleaning to uncertain),
/// `finish_check_run` (cleaning to succeeded or failed, and any settlement
/// that carries evidence) or `transition_check_run`.
/// A queued run has never been dispatched, so it cannot become uncertain.
pub const CHECK_RUN_TRANSITIONS: &[(CheckRunState, CheckRunState)] = &[
    (CheckRunState::Queued, CheckRunState::Running),
    (CheckRunState::Queued, CheckRunState::Cancelled),
    (CheckRunState::Running, CheckRunState::Cancelling),
    (CheckRunState::Running, CheckRunState::Cleaning),
    (CheckRunState::Running, CheckRunState::Uncertain),
    (CheckRunState::Cancelling, CheckRunState::Cleaning),
    (CheckRunState::Cancelling, CheckRunState::Uncertain),
    (CheckRunState::Cleaning, CheckRunState::Succeeded),
    (CheckRunState::Cleaning, CheckRunState::Failed),
    (CheckRunState::Cleaning, CheckRunState::Cancelled),
    (CheckRunState::Cleaning, CheckRunState::Uncertain),
    // A reconciliation receipt, not lease expiry, permits these exits.
    (CheckRunState::Uncertain, CheckRunState::Cleaning),
    (CheckRunState::Uncertain, CheckRunState::Cancelled),
];
impl CheckRunState {
    pub fn terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckRunIdentity {
    pub project_id: String,
    pub repo_id: String,
    pub commit_sha: String,
    pub inputs: CheckDigestInput,
}
impl CheckRunIdentity {
    pub fn key(&self) -> Result<String> {
        if self.project_id.is_empty() || self.repo_id.is_empty() || self.commit_sha.is_empty() {
            return Err(DbError::Check(
                "check identity needs Project, repo and exact commit".into(),
            ));
        }
        if !matches!(self.commit_sha.len(), 40 | 64)
            || !self
                .commit_sha
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(DbError::Check(
                "check identity requires a full lowercase commit object ID, not a mutable ref"
                    .into(),
            ));
        }
        let digest = self.inputs.digest().map_err(DbError::Check)?;
        api_types::canonical_digest_with_schema(
            "forge.check-scope/1",
            &serde_json::json!({
                "project_id": self.project_id, "repo_id": self.repo_id,
                "commit_sha": self.commit_sha, "spec_digest": digest,
            }),
        )
        .map_err(|e| DbError::Check(e.to_string()))
    }
}
#[derive(Debug, Clone)]
pub struct CheckRunRequest {
    pub identity: CheckRunIdentity,
    pub request_key: String,
    pub task_id: Option<String>,
    pub status_epoch: i64,
    pub origin: CheckConsumerOrigin,
    /// Why this consumer asks. Recorded on the consumer row only: it never
    /// decides which run the consumer gets.
    pub purpose: CheckPurpose,
    pub workspace_id: Option<String>,
    pub machine_id: Option<String>,
    /// The whole-run wall limit in force now. It is recorded on a run this
    /// request schedules and is the limit that run executes under; it is not
    /// part of the identity and is ignored when the request joins or reuses.
    pub wall_timeout_seconds: u64,
}
#[derive(Debug, Clone)]
pub struct StoredCheckRun {
    pub id: String,
    pub identity: CheckRunIdentity,
    pub identity_key: String,
    pub spec_digest: String,
    pub cacheable: bool,
    pub state: CheckRunState,
    pub operation_id: String,
    pub workspace_id: Option<String>,
    pub machine_id: Option<String>,
    /// The wall limit this run executes under, fixed when it was scheduled.
    pub applied_timeout_seconds: Option<i64>,
    pub lease_owner: Option<String>,
    pub lease_generation: i64,
    pub lease_until: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
    pub finished_at: Option<String>,
}
#[derive(Debug, Clone)]
pub struct CheckConsumer {
    pub id: String,
    pub project_id: String,
    pub repo_id: String,
    pub task_id: Option<String>,
    pub status_epoch: i64,
    pub origin: CheckConsumerOrigin,
    pub purpose: Option<CheckPurpose>,
    pub request_key: String,
    pub identity_key: String,
    pub run_id: Option<String>,
    pub result_id: Option<String>,
    pub created_at: String,
}
#[derive(Debug, Clone)]
pub enum CheckRequestDisposition {
    Scheduled,
    Joined,
    Reused,
    Idempotent,
}
#[derive(Debug, Clone)]
pub struct RequestedCheckRun {
    pub consumer: CheckConsumer,
    pub disposition: CheckRequestDisposition,
}
#[derive(Debug, Clone)]
pub struct StoredCheckResult {
    pub id: String,
    pub run_id: String,
    pub identity_key: String,
    pub outcome: CheckResultOutcome,
    pub cleanup: CheckCleanup,
    pub certified: bool,
    pub cacheable: bool,
    pub commands: Vec<CheckCommandOutcome>,
    pub output_truncated: bool,
    pub created_at: String,
}
/// Same command outcomes as the existing CheckRun primitive, with settlement
/// evidence added outside its execution semantics.
pub struct CheckResultEvidence {
    pub outcome: CheckResultOutcome,
    pub cleanup: CheckCleanup,
    pub commands: Vec<CheckCommandOutcome>,
    pub output_truncated: bool,
    /// Actual environment secret values, used transiently for redaction only.
    /// Never serialized, stored, or included in an identity.
    pub redaction_values: Vec<String>,
}
/// The caller's last read of a run. `lease_owner` is None only for a queued
/// run nobody has claimed; that fence is version-only and can do nothing but
/// cancel it. Every other mutation needs the current, unexpired lease.
#[derive(Debug, Clone)]
pub struct CheckRunFence {
    pub run_id: String,
    pub version: i64,
    pub lease_generation: i64,
    pub lease_owner: Option<String>,
}
#[derive(Debug, Clone, Default)]
pub struct CheckRunCounts {
    pub by_state: BTreeMap<String, i64>,
    pub reusable_results: i64,
    pub admitted_runs: i64,
    pub waiting_for_capacity: i64,
    pub borrowed_runs: i64,
}

#[async_trait]
pub trait CheckRunRepo: Send + Sync {
    async fn request_check_run(&self, request: CheckRunRequest) -> Result<RequestedCheckRun>;
    async fn check_run(&self, id: &str) -> Result<Option<StoredCheckRun>>;
    async fn find_live_check_run(
        &self,
        identity: &CheckRunIdentity,
    ) -> Result<Option<StoredCheckRun>>;
    async fn claim_check_run(
        &self,
        id: &str,
        version: i64,
        owner: &str,
        now: &str,
        until: &str,
    ) -> Result<StoredCheckRun>;
    async fn renew_check_run(
        &self,
        fence: &CheckRunFence,
        now: &str,
        until: &str,
    ) -> Result<StoredCheckRun>;
    async fn transition_check_run(
        &self,
        fence: &CheckRunFence,
        state: CheckRunState,
        now: &str,
    ) -> Result<StoredCheckRun>;
    async fn mark_check_run_uncertain(
        &self,
        fence: &CheckRunFence,
        now: &str,
    ) -> Result<StoredCheckRun>;
    /// Atomic CAS settlement and immutable INSERT, never an UPDATE to a result.
    async fn finish_check_run(
        &self,
        fence: &CheckRunFence,
        evidence: CheckResultEvidence,
        now: &str,
    ) -> Result<StoredCheckResult>;
    async fn check_result(&self, id: &str) -> Result<Option<StoredCheckResult>>;
    async fn reusable_check_result(
        &self,
        identity: &CheckRunIdentity,
    ) -> Result<Option<StoredCheckResult>>;
    async fn check_run_counts(&self) -> Result<CheckRunCounts>;
}

fn json<T: Serialize>(value: &T, cap: usize) -> Result<String> {
    let text = serde_json::to_string(value).map_err(|e| DbError::Check(e.to_string()))?;
    if text.len() > cap {
        return Err(DbError::Check("check JSON exceeds storage budget".into()));
    }
    Ok(text)
}
fn parse<T: serde::de::DeserializeOwned>(text: &str) -> Result<T> {
    serde_json::from_str(text).map_err(|e| DbError::Check(e.to_string()))
}
fn map_run(row: SqliteRow) -> Result<StoredCheckRun> {
    let input: CheckDigestInput = parse(&row.try_get::<String, _>("input_json")?)?;
    Ok(StoredCheckRun {
        id: row.try_get("id")?,
        identity: CheckRunIdentity {
            project_id: row.try_get("project_id")?,
            repo_id: row.try_get("repo_id")?,
            commit_sha: row.try_get("commit_sha")?,
            inputs: input,
        },
        identity_key: row.try_get("identity_key")?,
        spec_digest: row.try_get("spec_digest")?,
        cacheable: row.try_get("cacheable")?,
        state: row.try_get::<String, _>("state")?.parse()?,
        operation_id: row.try_get("operation_id")?,
        workspace_id: row.try_get("workspace_id")?,
        machine_id: row.try_get("machine_id")?,
        applied_timeout_seconds: row.try_get("applied_timeout_seconds")?,
        lease_owner: row.try_get("lease_owner")?,
        lease_generation: row.try_get("lease_generation")?,
        lease_until: row.try_get("lease_until")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        finished_at: row.try_get("finished_at")?,
    })
}
fn map_consumer(row: SqliteRow) -> Result<CheckConsumer> {
    Ok(CheckConsumer {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        repo_id: row.try_get("repo_id")?,
        task_id: row.try_get("task_id")?,
        status_epoch: row.try_get("status_epoch")?,
        origin: row.try_get::<String, _>("origin")?.parse()?,
        purpose: row
            .try_get::<Option<String>, _>("purpose")?
            .map(|text| {
                CheckPurpose::ALL
                    .iter()
                    .copied()
                    .find(|purpose| purpose.as_str() == text)
                    .ok_or_else(|| DbError::Check(format!("unknown CheckPurpose: {text}")))
            })
            .transpose()?,
        request_key: row.try_get("request_key")?,
        identity_key: row.try_get("identity_key")?,
        run_id: row.try_get("run_id")?,
        result_id: row.try_get("result_id")?,
        created_at: row.try_get("created_at")?,
    })
}
fn map_result(row: SqliteRow) -> Result<StoredCheckResult> {
    Ok(StoredCheckResult {
        id: row.try_get("id")?,
        run_id: row.try_get("run_id")?,
        identity_key: row.try_get("identity_key")?,
        outcome: row.try_get::<String, _>("outcome")?.parse()?,
        cleanup: row.try_get::<String, _>("cleanup")?.parse()?,
        certified: row.try_get("certified")?,
        cacheable: row.try_get("cacheable")?,
        commands: parse(&row.try_get::<String, _>("steps_json")?)?,
        output_truncated: row.try_get("output_truncated")?,
        created_at: row.try_get("created_at")?,
    })
}
const LIVE: &str = "SELECT * FROM check_run WHERE identity_key=? AND state IN ('queued','running','cancelling','cleaning','uncertain')";
/// `certified` already holds the pass-and-cleanup rule (see `finish_check_run`).
const REUSABLE: &str = "SELECT result.* FROM check_result result JOIN check_run run ON run.id=result.run_id WHERE result.identity_key=? AND result.outcome='pass' AND result.certified=1 AND result.cacheable=1 AND run.cacheable=1 AND run.state='succeeded'";
/// Keep at most `budget` trailing bytes on a character boundary.
fn keep_tail(text: &mut String, budget: usize) -> bool {
    if text.len() <= budget {
        return false;
    }
    let mut offset = text.len() - budget;
    while !text.is_char_boundary(offset) {
        offset += 1;
    }
    text.drain(..offset);
    true
}
fn validate_times(now: &str, until: &str) -> Result<()> {
    let start =
        chrono::DateTime::parse_from_rfc3339(now).map_err(|e| DbError::Check(e.to_string()))?;
    let end =
        chrono::DateTime::parse_from_rfc3339(until).map_err(|e| DbError::Check(e.to_string()))?;
    if end <= start {
        return Err(DbError::Check(
            "check lease must expire in the future".into(),
        ));
    }
    Ok(())
}

#[async_trait]
impl CheckRunRepo for SqliteDb {
    async fn request_check_run(&self, request: CheckRunRequest) -> Result<RequestedCheckRun> {
        let key = request.identity.key()?;
        let input = json(&request.identity.inputs, CHECK_INPUT_BYTES)?;
        if request.request_key.is_empty() || request.status_epoch < 0 {
            return Err(DbError::Check("invalid check consumer identity".into()));
        }
        let wall_timeout = i64::try_from(request.wall_timeout_seconds)
            .ok()
            .filter(|seconds| *seconds > 0)
            .ok_or_else(|| DbError::Check("check wall timeout must be positive".into()))?;
        // A worktree- or Task-scoped identity is only this requester's to ask for.
        match &request.identity.inputs.spec.scope {
            CheckScope::Commit => {}
            CheckScope::Workspace { workspace_id, .. } => {
                if request.workspace_id.as_ref() != Some(workspace_id) {
                    return Err(DbError::Check(
                        "check scope names a different workspace than the request".into(),
                    ));
                }
            }
            CheckScope::Task { task_id } => {
                if request.task_id.as_ref() != Some(task_id) {
                    return Err(DbError::Check(
                        "check scope names a different Task than the request".into(),
                    ));
                }
            }
        }
        let mut tx = begin_immediate(self.pool()).await?;
        // Reject contradictory scope even though individual FK references exist.
        let project: String = sqlx::query_scalar("SELECT project_id FROM repo WHERE id=?")
            .bind(&request.identity.repo_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::NotFound)?;
        if project != request.identity.project_id {
            return Err(DbError::Check("check repo is outside Project".into()));
        }
        if let Some(task) = &request.task_id {
            let project: String = sqlx::query_scalar("SELECT project_id FROM task WHERE id=?")
                .bind(task)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(DbError::NotFound)?;
            if project != request.identity.project_id {
                return Err(DbError::Check("check Task is outside Project".into()));
            }
        }
        if let Some(workspace) = &request.workspace_id {
            let repo: String = sqlx::query_scalar("SELECT repo_id FROM workspace WHERE id=?")
                .bind(workspace)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(DbError::NotFound)?;
            if repo != request.identity.repo_id {
                return Err(DbError::Check("check workspace is outside repo".into()));
            }
        }
        if let Some(row) = sqlx::query("SELECT * FROM check_consumer WHERE request_key=?")
            .bind(&request.request_key)
            .fetch_optional(&mut *tx)
            .await?
        {
            let consumer = map_consumer(row)?;
            if consumer.identity_key != key
                || consumer.task_id != request.task_id
                || consumer.status_epoch != request.status_epoch
                || consumer.origin != request.origin
                || consumer.purpose != Some(request.purpose)
            {
                return Err(DbError::IdempotencyConflict);
            }
            tx.commit().await?;
            return Ok(RequestedCheckRun {
                consumer,
                disposition: CheckRequestDisposition::Idempotent,
            });
        }
        let now = now_rfc3339();
        let hit = if request.identity.inputs.reusable_inputs() {
            sqlx::query(REUSABLE)
                .bind(&key)
                .fetch_optional(&mut *tx)
                .await?
                .map(map_result)
                .transpose()?
        } else {
            None
        };
        let (run_id, result_id, disposition) = if let Some(result) = hit {
            (
                result.run_id,
                Some(result.id),
                CheckRequestDisposition::Reused,
            )
        } else if let Some(row) = sqlx::query(LIVE)
            .bind(&key)
            .fetch_optional(&mut *tx)
            .await?
        {
            (map_run(row)?.id, None, CheckRequestDisposition::Joined)
        } else {
            let id = new_uuid_v4();
            sqlx::query("INSERT INTO check_run(id,project_id,repo_id,commit_sha,spec_digest,identity_key,input_json,cacheable,state,operation_id,workspace_id,machine_id,applied_timeout_seconds,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,'queued',?,?,?,?,?,?)")
                .bind(&id).bind(&request.identity.project_id).bind(&request.identity.repo_id).bind(&request.identity.commit_sha)
                .bind(request.identity.inputs.digest().map_err(DbError::Check)?).bind(&key).bind(input).bind(request.identity.inputs.reusable_inputs())
                .bind(new_uuid_v4()).bind(&request.workspace_id).bind(&request.machine_id).bind(wall_timeout).bind(&now).bind(&now).execute(&mut *tx).await?;
            (id, None, CheckRequestDisposition::Scheduled)
        };
        let id = new_uuid_v4();
        sqlx::query("INSERT INTO check_consumer(id,project_id,repo_id,task_id,status_epoch,origin,purpose,request_key,identity_key,run_id,result_id,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(&id).bind(&request.identity.project_id).bind(&request.identity.repo_id).bind(&request.task_id).bind(request.status_epoch)
            .bind(request.origin.to_string()).bind(request.purpose.as_str()).bind(&request.request_key).bind(key).bind(run_id).bind(result_id).bind(now).execute(&mut *tx).await?;
        let consumer = map_consumer(
            sqlx::query("SELECT * FROM check_consumer WHERE id=?")
                .bind(id)
                .fetch_one(&mut *tx)
                .await?,
        )?;
        tx.commit().await?;
        Ok(RequestedCheckRun {
            consumer,
            disposition,
        })
    }
    async fn check_run(&self, id: &str) -> Result<Option<StoredCheckRun>> {
        sqlx::query("SELECT * FROM check_run WHERE id=?")
            .bind(id)
            .fetch_optional(self.pool())
            .await?
            .map(map_run)
            .transpose()
    }
    async fn find_live_check_run(
        &self,
        identity: &CheckRunIdentity,
    ) -> Result<Option<StoredCheckRun>> {
        sqlx::query(LIVE)
            .bind(identity.key()?)
            .fetch_optional(self.pool())
            .await?
            .map(map_run)
            .transpose()
    }
    async fn claim_check_run(
        &self,
        id: &str,
        version: i64,
        owner: &str,
        now: &str,
        until: &str,
    ) -> Result<StoredCheckRun> {
        validate_times(now, until)?;
        if owner.is_empty() {
            return Err(DbError::Check("check lease owner is empty".into()));
        }
        // Nobody started a queued run, so it is simply claimed. Taking over an
        // expired lease on a dispatched run is different: the previous owner
        // may still be executing, so the run becomes uncertain and can only
        // be reconciled or superseded, never continued or relaunched.
        let row = sqlx::query("UPDATE check_run SET state=CASE WHEN state='queued' THEN 'running' ELSE 'uncertain' END,lease_owner=?,lease_until=?,lease_generation=lease_generation+1,version=version+1,updated_at=? WHERE id=? AND version=? AND state IN ('queued','running','cancelling','cleaning','uncertain') AND (lease_until IS NULL OR julianday(lease_until)<=julianday(?)) RETURNING *")
            .bind(owner).bind(until).bind(now).bind(id).bind(version).bind(now).fetch_optional(self.pool()).await?.ok_or(DbError::VersionConflict)?;
        map_run(row)
    }
    async fn renew_check_run(
        &self,
        fence: &CheckRunFence,
        now: &str,
        until: &str,
    ) -> Result<StoredCheckRun> {
        validate_times(now, until)?;
        let row = sqlx::query("UPDATE check_run SET lease_until=?,version=version+1,updated_at=? WHERE id=? AND version=? AND lease_generation=? AND lease_owner=? AND julianday(lease_until)>julianday(?) AND state IN ('running','cancelling','cleaning','uncertain') RETURNING *")
            .bind(until).bind(now).bind(&fence.run_id).bind(fence.version).bind(fence.lease_generation).bind(&fence.lease_owner).bind(now)
            .fetch_optional(self.pool()).await?.ok_or(DbError::VersionConflict)?;
        map_run(row)
    }
    async fn transition_check_run(
        &self,
        fence: &CheckRunFence,
        state: CheckRunState,
        now: &str,
    ) -> Result<StoredCheckRun> {
        // Results can become terminal only through atomic evidence settlement;
        // cancelled queued/uncertain runs have no certified result.
        if state.terminal() && state != CheckRunState::Cancelled {
            return Err(DbError::InvalidTransition);
        }
        let mut tx = begin_immediate(self.pool()).await?;
        let run = map_run(
            sqlx::query("SELECT * FROM check_run WHERE id=?")
                .bind(&fence.run_id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(DbError::NotFound)?,
        )?;
        if run.version != fence.version
            || run.lease_generation != fence.lease_generation
            || run.lease_owner != fence.lease_owner
        {
            return Err(DbError::VersionConflict);
        }
        // The first claim is the only way out of queued into running.
        if !CHECK_RUN_TRANSITIONS.contains(&(run.state, state)) || state == CheckRunState::Running {
            return Err(DbError::InvalidTransition);
        }
        // An unclaimed queued run has no lease to expire; its fence is the
        // version alone. A leased run still needs an unexpired lease.
        let row = sqlx::query("UPDATE check_run SET state=?,version=version+1,updated_at=?,finished_at=CASE WHEN ?='cancelled' THEN ? ELSE NULL END WHERE id=? AND version=? AND lease_generation=? AND lease_owner IS ? AND (lease_until IS NULL OR julianday(lease_until)>julianday(?)) RETURNING *")
            .bind(state.to_string()).bind(now).bind(state.to_string()).bind(now).bind(&fence.run_id).bind(fence.version).bind(fence.lease_generation).bind(&fence.lease_owner).bind(now)
            .fetch_optional(&mut *tx).await?.ok_or(DbError::VersionConflict)?;
        let run = map_run(row)?;
        tx.commit().await?;
        Ok(run)
    }
    async fn mark_check_run_uncertain(
        &self,
        fence: &CheckRunFence,
        now: &str,
    ) -> Result<StoredCheckRun> {
        self.transition_check_run(fence, CheckRunState::Uncertain, now)
            .await
    }
    async fn finish_check_run(
        &self,
        fence: &CheckRunFence,
        mut evidence: CheckResultEvidence,
        now: &str,
    ) -> Result<StoredCheckResult> {
        let mut tx = begin_immediate(self.pool()).await?;
        let run = map_run(
            sqlx::query("SELECT * FROM check_run WHERE id=?")
                .bind(&fence.run_id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(DbError::NotFound)?,
        )?;
        if fence.lease_owner.is_none()
            || run.version != fence.version
            || run.lease_generation != fence.lease_generation
            || run.lease_owner != fence.lease_owner
        {
            return Err(DbError::VersionConflict);
        }
        if run.state != CheckRunState::Cleaning {
            return Err(DbError::InvalidTransition);
        }
        evidence
            .redaction_values
            .sort_by_key(|value| std::cmp::Reverse(value.len()));
        for (index, step) in evidence.commands.iter_mut().enumerate() {
            let expected = run
                .identity
                .inputs
                .spec
                .commands
                .get(index)
                .ok_or_else(|| DbError::Check("result has extra commands".into()))?;
            if step.index != index || step.command != expected.shell_text {
                return Err(DbError::Check("result commands differ from spec".into()));
            }
            for text in [&mut step.stderr_tail, &mut step.output_tail] {
                for secret in evidence.redaction_values.iter().filter(|s| !s.is_empty()) {
                    *text = text.replace(secret, "[REDACTED]");
                }
                evidence.output_truncated |= keep_tail(text, CHECK_OUTPUT_TAIL_BYTES);
            }
        }
        if evidence.outcome == CheckResultOutcome::Pass
            && (evidence.commands.len() != run.identity.inputs.spec.commands.len()
                || evidence.commands.iter().any(|c| c.exit_code != 0))
        {
            return Err(DbError::Check(
                "passing result requires every declared command to pass".into(),
            ));
        }
        // A bundle with no cleanup step has nothing to clean: `not_performed`
        // is then the complete, expected receipt. When the spec declares a
        // cleanup step, only a performed and successful cleanup certifies.
        // A timed-out, failed or cancelled run is never certified.
        let certified = evidence.outcome == CheckResultOutcome::Pass
            && (evidence.cleanup == CheckCleanup::Success
                || (evidence.cleanup == CheckCleanup::NotPerformed
                    && !run.identity.inputs.spec.declares_cleanup));
        let state = if evidence.cleanup == CheckCleanup::Uncertain {
            CheckRunState::Uncertain
        } else if certified {
            CheckRunState::Succeeded
        } else if evidence.outcome == CheckResultOutcome::Cancelled {
            CheckRunState::Cancelled
        } else {
            CheckRunState::Failed
        };
        // Uncertain cleanup cannot release the single-flight key. A result is
        // immutable evidence; reconciliation can append a later cleanup receipt.
        // A long bundle with full tails can exceed the row budget. Output is
        // evidence, not the verdict: halve every tail until the row fits, so a
        // finished run can always settle. Only command text alone can refuse.
        let mut budget = CHECK_OUTPUT_TAIL_BYTES;
        let steps = loop {
            match json(&evidence.commands, CHECK_STEPS_BYTES) {
                Ok(steps) => break steps,
                Err(error) if budget == 0 => return Err(error),
                Err(_) => {
                    budget /= 2;
                    for step in &mut evidence.commands {
                        for text in [&mut step.stderr_tail, &mut step.output_tail] {
                            evidence.output_truncated |= keep_tail(text, budget);
                        }
                    }
                }
            }
        };
        let updated = sqlx::query("UPDATE check_run SET state=?,version=version+1,updated_at=?,finished_at=CASE WHEN ?='uncertain' THEN NULL ELSE ? END WHERE id=? AND version=? AND lease_generation=? AND lease_owner=? AND julianday(lease_until)>julianday(?)")
            .bind(state.to_string()).bind(now).bind(state.to_string()).bind(now).bind(&fence.run_id).bind(fence.version).bind(fence.lease_generation).bind(&fence.lease_owner).bind(now)
            .execute(&mut *tx).await?;
        if updated.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let id = new_uuid_v4();
        sqlx::query("INSERT INTO check_result(id,run_id,identity_key,outcome,cleanup,certified,cacheable,steps_json,output_truncated,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)")
            .bind(&id).bind(&run.id).bind(&run.identity_key).bind(evidence.outcome.to_string()).bind(evidence.cleanup.to_string())
            .bind(certified).bind(run.cacheable).bind(steps).bind(evidence.output_truncated).bind(now).execute(&mut *tx).await?;
        sqlx::query("UPDATE check_consumer SET result_id=? WHERE run_id=?")
            .bind(&id)
            .bind(&run.id)
            .execute(&mut *tx)
            .await?;
        let result = map_result(
            sqlx::query("SELECT * FROM check_result WHERE id=?")
                .bind(id)
                .fetch_one(&mut *tx)
                .await?,
        )?;
        tx.commit().await?;
        Ok(result)
    }
    async fn check_result(&self, id: &str) -> Result<Option<StoredCheckResult>> {
        sqlx::query("SELECT * FROM check_result WHERE id=?")
            .bind(id)
            .fetch_optional(self.pool())
            .await?
            .map(map_result)
            .transpose()
    }
    async fn reusable_check_result(
        &self,
        identity: &CheckRunIdentity,
    ) -> Result<Option<StoredCheckResult>> {
        if !identity.inputs.reusable_inputs() {
            return Ok(None);
        }
        sqlx::query(REUSABLE)
            .bind(identity.key()?)
            .fetch_optional(self.pool())
            .await?
            .map(map_result)
            .transpose()
    }
    async fn check_run_counts(&self) -> Result<CheckRunCounts> {
        let mut counts = CheckRunCounts {
            by_state: CheckRunState::ALL
                .iter()
                .map(|s| (s.to_string(), 0))
                .collect(),
            reusable_results: 0,
            admitted_runs: 0,
            waiting_for_capacity: 0,
            borrowed_runs: 0,
        };
        for row in sqlx::query("SELECT state,COUNT(*) AS n FROM check_run GROUP BY state")
            .fetch_all(self.pool())
            .await?
        {
            counts
                .by_state
                .insert(row.try_get("state")?, row.try_get("n")?);
        }
        counts.reusable_results = sqlx::query_scalar("SELECT COUNT(*) FROM check_result result JOIN check_run run ON run.id=result.run_id WHERE result.outcome='pass' AND result.certified=1 AND result.cacheable=1 AND run.cacheable=1 AND run.state='succeeded'").fetch_one(self.pool()).await?;
        counts.admitted_runs=sqlx::query_scalar("SELECT COUNT(*) FROM check_run WHERE admitted_at IS NOT NULL AND state IN ('running','cancelling','cleaning','uncertain')").fetch_one(self.pool()).await?;
        counts.waiting_for_capacity=sqlx::query_scalar("SELECT COUNT(*) FROM check_run WHERE state='queued' AND capacity_wait_since IS NOT NULL").fetch_one(self.pool()).await?;
        let borrowed = format!(
            "{} SELECT COALESCE(SUM(borrowed_check_runs),0) FROM occupancy",
            include_str!("../machine_occupancy.sql")
        );
        counts.borrowed_runs = sqlx::query_scalar(&borrowed)
            .bind(self.server_run_cap.embedded_machine_id())
            .fetch_one(self.pool())
            .await?;
        Ok(counts)
    }
}

#[cfg(test)]
mod tests;
