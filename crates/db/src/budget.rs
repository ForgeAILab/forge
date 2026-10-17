//! Task budget policy and transactional accounting. Counts never come from audit prose.
use crate::{DbError, Result, Review, ReviewStatus, SqliteDb, Task};
use api_types::{CanonicalPhase, GateConfig, StateDefinition, StateKind, WorkflowDefinition};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{Row, Sqlite, Transaction};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Review,
    ReviewProgress,
    /// Inconclusive verify reruns, ledger-backed per Review invocation.
    ReviewVerifyRerun,
    /// One reminder per Review window when a coder returns the same clean HEAD.
    RepairNoChange,
    GateRejection,
    MergeFix,
    Execution,
    WorkflowGuard,
    TargetMovedRebase,
    ConflictHandoff,
    ReviewCarry,
    AutomaticReviewRecovery,
    ReportCorrection,
    ReviewCiInfrastructure,
    /// Clean-checkout review checks re-run after a timeout, per evaluation.
    ReviewCheckRerun,
    /// A coordination root's entries into its planning state after kickoff:
    /// one per rejected aggregate review and one per parked subtask. Charged
    /// only on a root that has a lead, so it is not in [`Kind::PERSISTED`]:
    /// no row exists, and nothing is projected, for any other Task. Its
    /// limit is fixed ([`Kind::default_limit`]): `retry_budgets` does not
    /// configure it, in any config, like the other system-owned kinds.
    Replan,
}
impl Kind {
    pub const PERSISTED: [Self; 12] = [
        Self::Review,
        Self::ReviewProgress,
        Self::ReviewVerifyRerun,
        Self::RepairNoChange,
        Self::MergeFix,
        Self::Execution,
        Self::WorkflowGuard,
        Self::TargetMovedRebase,
        Self::ConflictHandoff,
        Self::ReviewCarry,
        Self::AutomaticReviewRecovery,
        Self::ReviewCiInfrastructure,
    ];
    pub fn key(self) -> &'static str {
        match self {
            Self::Review => "review",
            Self::ReviewProgress => "review_progress",
            Self::ReviewVerifyRerun => "review_verify_rerun",
            Self::RepairNoChange => "repair_no_change",
            Self::GateRejection => "gate",
            Self::MergeFix => "merge_fix",
            Self::Execution => "execution",
            Self::WorkflowGuard => "workflow_guard",
            Self::TargetMovedRebase => "target_moved_rebase",
            Self::ConflictHandoff => "conflict_handoff",
            Self::ReviewCarry => "review_carry",
            Self::AutomaticReviewRecovery => "automatic_review_recovery",
            Self::ReportCorrection => "report_correction",
            Self::ReviewCiInfrastructure => "review_ci_infrastructure",
            Self::ReviewCheckRerun => "review_check_rerun",
            Self::Replan => "replan",
        }
    }
    pub const fn default_limit(self) -> i32 {
        match self {
            Self::Review | Self::Execution | Self::WorkflowGuard => 3,
            Self::MergeFix
            | Self::AutomaticReviewRecovery
            | Self::RepairNoChange
            | Self::ReviewVerifyRerun => 1,
            Self::GateRejection => i32::MAX,
            Self::TargetMovedRebase
            | Self::ConflictHandoff
            | Self::ReviewCarry
            | Self::ReviewCiInfrastructure
            | Self::Replan => 5,
            Self::ReportCorrection | Self::ReviewCheckRerun | Self::ReviewProgress => 2,
        }
    }
    /// Per-invocation counters live in memory and never reach the ledger.
    pub const fn is_volatile(self) -> bool {
        matches!(self, Self::ReportCorrection | Self::ReviewCheckRerun)
    }
}
/// Reserved checkpoint before configurable review-entry hooks. Negative keeps
/// every existing Project hook ordinal stable, including suspended entries.
pub const REPAIR_NO_CHANGE_CHECKPOINT: i64 = -1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairNoChangeDecision {
    Continue,
    SendBack { attempt: i64 },
    Park,
}

/// The latest settled Review in the current window, only when it failed, and the commit
/// it judged. A later pass must not resurrect an earlier failure. Frozen reviewer
/// contracts take precedence over entry CI evidence, then delivery evidence.
pub async fn failed_review_for_no_change(
    db: &SqliteDb,
    task: &str,
) -> Result<Option<(Review, Option<String>)>> {
    let row: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT r.id, COALESCE(
            CASE WHEN json_valid(r.step_results_json) THEN json_extract(r.step_results_json,'$.conformance.contract.commit_sha') END,
            (SELECT cr.commit_sha FROM check_consumer c JOIN check_run cr ON cr.id=c.run_id
             WHERE c.task_id=r.task_id AND c.origin='entry'
               AND c.request_key='task-step/1|entry|'||r.task_id||'|'||c.status_epoch||'|'||r.id
             ORDER BY c.created_at DESC,c.id DESC LIMIT 1),
            e.after_sha)
         FROM review r JOIN execution e ON e.id=r.execution_id
         LEFT JOIN task_budget b ON b.task_id=r.task_id AND b.kind='review'
         LEFT JOIN transition_log boundary ON boundary.id=b.window_id AND boundary.task_id=r.task_id
         WHERE r.task_id=? AND r.status IN ('passed','failed')
           AND (boundary.id IS NULL OR r.created_at >= boundary.created_at)
         ORDER BY r.attempt_number DESC,r.id DESC LIMIT 1",
    ).bind(task).fetch_optional(db.pool()).await?;
    match row {
        Some((id, head)) => Ok(crate::ReviewRepo::get_by_id(db, &id)
            .await?
            .filter(|review| review.status == ReviewStatus::Failed)
            .map(|review| (review, head))),
        None => Ok(None),
    }
}

/// Start the guard, retaining the decision of an entry already in progress when
/// the guard shipped. Exit hooks precede the guard, so only entry checkpoints
/// prove that it is too late to evaluate it.
pub async fn start_repair_no_change(
    db: &SqliteDb,
    step: &crate::TaskStep,
    first_entry_index: i64,
) -> Result<Option<RepairNoChangeDecision>> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    db.fence_hook_in_tx(&mut tx, step).await?;
    let recorded: Option<String> = sqlx::query_scalar(
        "SELECT result_json FROM task_hook_checkpoint WHERE step_id=? AND hook_index=?",
    )
    .bind(&step.id)
    .bind(REPAIR_NO_CHANGE_CHECKPOINT)
    .fetch_optional(&mut *tx)
    .await?
    .flatten();
    let recorded = match recorded {
        Some(recorded) => Some(recorded),
        None => {
            // Every hook, also one that suspended for a check or for disk,
            // opened its checkpoint first. A recorded wait alone is no proof:
            // a waking step keeps `awaited_consumer_id`, including after an
            // exit hook's wait that precedes the guard.
            let started: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM task_hook_checkpoint WHERE step_id=? AND hook_index>=?)",
            )
            .bind(&step.id)
            .bind(first_entry_index)
            .fetch_one(&mut *tx)
            .await?;
            started.then(|| serde_json::to_string(&RepairNoChangeDecision::Continue).unwrap())
        }
    };
    sqlx::query("INSERT INTO task_hook_checkpoint(step_id,hook_index,started_at,result_json) VALUES(?,?,?,?) ON CONFLICT(step_id,hook_index) DO UPDATE SET result_json=excluded.result_json")
        .bind(&step.id).bind(REPAIR_NO_CHANGE_CHECKPOINT).bind(crate::now_rfc3339())
        .bind(&recorded).execute(&mut *tx).await?;
    tx.commit().await?;
    recorded
        .map(|raw| serde_json::from_str(&raw).map_err(|e| DbError::Check(e.to_string())))
        .transpose()
}

/// Save the guard's decision and its one allowed reminder charge together.
/// A replay returns that decision before checking mutable Git state again.
pub async fn settle_repair_no_change(
    db: &SqliteDb,
    task: &Task,
    step: &crate::TaskStep,
    unchanged_attempt: Option<i64>,
) -> Result<RepairNoChangeDecision> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    db.fence_hook_in_tx(&mut tx, step).await?;
    let current: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task WHERE id=? AND version=? AND status=? AND deleted_at IS NULL)")
        .bind(&task.id).bind(task.version).bind(&task.status).fetch_one(&mut *tx).await?;
    if !current {
        return Err(DbError::VersionConflict);
    }
    let recorded: Option<String> = sqlx::query_scalar(
        "SELECT result_json FROM task_hook_checkpoint WHERE step_id=? AND hook_index=?",
    )
    .bind(&step.id)
    .bind(REPAIR_NO_CHANGE_CHECKPOINT)
    .fetch_optional(&mut *tx)
    .await?
    .flatten();
    if let Some(recorded) = recorded {
        return serde_json::from_str(&recorded).map_err(|error| DbError::Check(error.to_string()));
    }
    let decision = match unchanged_attempt {
        None => RepairNoChangeDecision::Continue,
        Some(attempt) => {
            let window: String = sqlx::query_scalar("SELECT COALESCE((SELECT window_id FROM task_budget WHERE task_id=? AND kind='review'),'initial')")
                .bind(&task.id).fetch_one(&mut *tx).await?;
            reset(&mut tx, &task.id, Kind::RepairNoChange.key(), &window).await?;
            let charge = charge(
                &mut tx,
                &task.id,
                Kind::RepairNoChange.key(),
                i64::from(Kind::RepairNoChange.default_limit()),
                &format!("entry:{}", step.id),
            )
            .await?;
            if charge.charged {
                RepairNoChangeDecision::SendBack { attempt }
            } else {
                RepairNoChangeDecision::Park
            }
        }
    };
    sqlx::query("UPDATE task_hook_checkpoint SET result_json=? WHERE step_id=? AND hook_index=?")
        .bind(serde_json::to_string(&decision).map_err(|error| DbError::Check(error.to_string()))?)
        .bind(&step.id)
        .bind(REPAIR_NO_CHANGE_CHECKPOINT)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(decision)
}

/// Bounds on what a coordination root's lead may repeat. The rows live on
/// the ROOT under these keys, in the root's budget window: only a Retry with
/// a new budget on the root (the owner's or the Project Agent's) opens a new
/// one, and nothing the lead can do to a subtask resets them. They are not
/// [`Kind`]s: no other Task ever has such a row and nothing projects them.
///
/// Subtasks the lead created under the root, per budget window.
pub const LEAD_SUBTASKS_KEY: &str = "lead_subtasks";
pub const LEAD_SUBTASKS_LIMIT: i64 = 24;
/// Subtasks one lead run (one planning run on the root) created.
pub const LEAD_SUBTASKS_PER_RUN_LIMIT: i64 = 8;
pub fn lead_subtasks_run_key(execution: &str) -> String {
    format!("lead_subtasks_run:{execution}")
}
/// Recovery actions the lead applied to one subtask, per budget window.
pub const LEAD_RECOVER_LIMIT: i64 = 2;
pub fn lead_recover_key(subtask: &str) -> String {
    format!("lead_recover:{subtask}")
}
/// Review-gate rejections, counted against the cancelled-review entry cap
/// (`max_rejections - 1`); the Review kind itself is keyed `review`.
pub const REVIEW_GATE_KEY: &str = "gate:review";
/// The built-in name keeps every budget rule it always had (also owner-only
/// review). A renamed review gate is the gate the engine runs Review for: the
/// `reviewer` role in the review phase. A reviewer-role gate in another phase
/// (a plan or design check) keeps its own gate budget and entry cap.
pub fn is_review_state(state: &StateDefinition) -> bool {
    state.name == "review"
        || (state.kind == StateKind::Gate
            && state.role.as_deref() == Some("reviewer")
            && state.canonical_phase == Some(CanonicalPhase::Review))
}

pub fn gate_budget_key(state: &StateDefinition) -> String {
    if is_review_state(state) {
        Kind::Review.key().into()
    } else {
        gate_key(&state.name)
    }
}

fn review_state_from_json(raw: &str, status: &str) -> bool {
    review_states_from_json(raw)
        .iter()
        .any(|name| name == status)
}

/// The review states of a Project `workflow_definition` text. An empty or
/// unreadable text is the built-in workflow, whose review state is `review`.
pub(crate) fn review_states_from_json(raw: &str) -> Vec<String> {
    let raw = raw.trim();
    if raw.is_empty() || raw == "{}" {
        return vec!["review".into()];
    }
    match serde_json::from_str::<WorkflowDefinition>(raw) {
        Ok(workflow) => workflow
            .states
            .iter()
            .filter(|state| is_review_state(state))
            .map(|state| state.name.clone())
            .collect(),
        Err(_) => vec!["review".into()],
    }
}

pub fn gate_key(state: &str) -> String {
    match state {
        "review" => "review".into(),
        _ => format!("gate:{state}"),
    }
}
fn configured(value: &Value, key: &str) -> Option<i32> {
    value
        .get("retry_budgets")?
        .get(key)?
        .as_i64()
        .and_then(|n| i32::try_from(n).ok())
        .filter(|n| *n >= 0)
}
/// Resolve a kind's limit in the order every consuming step has always used:
/// Task-wide `retry_budgets`, then the state config the step supplies, then the
/// gate's `max_rejections` (Review/MergeFix), then the kind default.
///
/// Task per-state overrides are not read here. Hook steps pass their merged
/// state config, which already carries `task_state_config[state]`; reviewer
/// completion, Execution retry and the workflow guard pass the workflow's own
/// state config and so never saw per-state values.
pub fn limit(
    task: &Task,
    kind: Kind,
    state_config: Option<&Value>,
    gate: Option<&GateConfig>,
) -> Result<i32> {
    if !matches!(
        kind,
        Kind::Review
            | Kind::ReviewProgress
            | Kind::ReviewVerifyRerun
            | Kind::MergeFix
            | Kind::Execution
            | Kind::WorkflowGuard
            | Kind::GateRejection
    ) {
        return Ok(kind.default_limit());
    }
    if kind == Kind::GateRejection {
        let n = gate
            .and_then(|g| g.max_rejections)
            .unwrap_or(i32::MAX)
            .max(0);
        return Ok(n);
    }
    if let Some(n) = configured_override(task, state_config, kind) {
        return Ok(n);
    }
    if matches!(kind, Kind::Review | Kind::MergeFix) {
        if let Some(n) = gate.and_then(|g| g.max_rejections) {
            return Ok(n.max(0));
        }
    }
    Ok(kind.default_limit())
}

/// The Task-wide value, else the supplied state config's value, for a kind.
fn configured_override(task: &Task, state_config: Option<&Value>, kind: Kind) -> Option<i32> {
    let key = if kind == Kind::WorkflowGuard {
        Kind::Execution.key()
    } else {
        kind.key()
    };
    task.task_state_config
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .and_then(|config| configured(&config, key))
        .or_else(|| state_config.and_then(|config| configured(config, key)))
}

/// The review-entry failure with no failed Review: base capped it at the gate's
/// `max_rejections` alone and never blocked without one. Review overrides now
/// apply here too; with neither an override nor a gate cap it stays unlimited.
pub fn review_entry_limit(task: &Task, state_config: &Value, gate: Option<&GateConfig>) -> i32 {
    configured_override(task, Some(state_config), Kind::Review)
        .or_else(|| gate.and_then(|g| g.max_rejections).map(|n| n.max(0)))
        .unwrap_or(i32::MAX)
}
pub fn remaining(limit: i64, spent: i64) -> i64 {
    limit.saturating_sub(spent).max(0)
}
pub fn allows_retry(limit: i64, spent: i64) -> bool {
    remaining(limit, spent) > 0
}
pub fn gate_entry_exhausted(is_review: bool, limit: i64, spent: i64) -> bool {
    !is_review && !allows_retry(limit, spent)
}
pub fn after_charge_exhausted(limit: i64, spent: i64) -> bool {
    spent > limit
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Mutation {
    Charge {
        key: String,
        limit: i64,
        step: String,
    },
    Reset {
        key: String,
        window: String,
    },
}
#[derive(Debug, Clone, Copy)]
pub struct Charge {
    pub spent: i64,
    pub remaining: i64,
    pub charged: bool,
}
pub async fn spent(pool: &sqlx::SqlitePool, task: &str, key: &str) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT spent FROM task_budget WHERE task_id = ? AND kind = ?")
            .bind(task)
            .bind(key)
            .fetch_optional(pool)
            .await?
            .unwrap_or(0),
    )
}
/// Whether `step` already holds a receipt for `key` in the Task's current
/// budget window: the charge it names was made and is not made again.
pub async fn receipt_recorded(
    pool: &sqlx::SqlitePool,
    task: &str,
    key: &str,
    step: &str,
) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM task_budget_charge c JOIN task_budget b USING(task_id,kind) WHERE c.task_id=? AND c.kind=? AND c.window_id=b.window_id AND c.step_id=?)",
    )
    .bind(task)
    .bind(key)
    .bind(step)
    .fetch_one(pool)
    .await?)
}
pub async fn load(pool: &sqlx::SqlitePool, task: &str) -> Result<HashMap<String, i64>> {
    let rows = sqlx::query("SELECT kind, spent FROM task_budget WHERE task_id = ?")
        .bind(task)
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|r| Ok((r.try_get("kind")?, r.try_get("spent")?)))
        .collect()
}
/// Spending for a page of Tasks in one query, keyed by Task id.
pub async fn load_many(
    pool: &sqlx::SqlitePool,
    tasks: &[&str],
) -> Result<HashMap<String, HashMap<String, i64>>> {
    let ids = serde_json::to_string(tasks).map_err(|e| DbError::Check(e.to_string()))?;
    let rows = sqlx::query(
        "SELECT task_id, kind, spent FROM task_budget WHERE task_id IN (SELECT value FROM json_each(?))",
    )
    .bind(ids)
    .fetch_all(pool)
    .await?;
    let mut spent = HashMap::<String, HashMap<String, i64>>::new();
    for row in rows {
        spent
            .entry(row.try_get("task_id")?)
            .or_default()
            .insert(row.try_get("kind")?, row.try_get("spent")?);
    }
    Ok(spent)
}
pub async fn reset(
    tx: &mut Transaction<'_, Sqlite>,
    task: &str,
    key: &str,
    window: &str,
) -> Result<()> {
    sqlx::query("INSERT INTO task_budget(task_id,kind,window_id,spent) VALUES(?,?,?,0) ON CONFLICT(task_id,kind) DO UPDATE SET window_id=excluded.window_id, spent=0 WHERE task_budget.window_id != excluded.window_id")
        .bind(task).bind(key).bind(window).execute(&mut **tx).await?;
    crate::task_condition::produce(tx, task, crate::ConditionChange::Budget).await?;
    Ok(())
}
pub async fn reset_all(tx: &mut Transaction<'_, Sqlite>, task: &str, window: &str) -> Result<()> {
    for kind in Kind::PERSISTED {
        reset(tx, task, kind.key(), window).await?;
    }
    sqlx::query("UPDATE task_budget SET window_id=?,spent=0 WHERE task_id=? AND window_id!=?")
        .bind(window)
        .bind(task)
        .bind(window)
        .execute(&mut **tx)
        .await?;
    crate::task_condition::produce(tx, task, crate::ConditionChange::Budget).await?;
    Ok(())
}
/// The receipt identity a failed review entry charges under. The cascade that
/// bounces that entry back reuses it, so one failed entry is charged once.
pub fn entry_charge_step(hook_step: &str) -> String {
    format!("entry:{hook_step}")
}
/// A fresh state entry (any status write that does not carry the previous entry
/// barrier) starts a new review-CI interruption episode. An entry retry keeps
/// its barrier and with it the episode's spending.
pub async fn fresh_entry(tx: &mut Transaction<'_, Sqlite>, task: &str, window: &str) -> Result<()> {
    reset(tx, task, Kind::ReviewCiInfrastructure.key(), window).await
}
/// Record an outcome the workflow already admitted (a logged rejection, an
/// exhausting verdict, a queued pre-upgrade increment): it always spends once
/// per receipt, whatever the limit.
pub async fn admit(
    tx: &mut Transaction<'_, Sqlite>,
    task: &str,
    key: &str,
    step: &str,
) -> Result<bool> {
    Ok(charge(tx, task, key, i64::MAX, step).await?.charged)
}
/// This is the only persisted charge point. Its receipt and consuming write share
/// the caller's authority-fenced transaction. Step identities include effect ordinals.
/// A charge that would exceed `limit` is refused (not recorded).
pub async fn charge(
    tx: &mut Transaction<'_, Sqlite>,
    task: &str,
    key: &str,
    limit: i64,
    step: &str,
) -> Result<Charge> {
    if [Kind::ReportCorrection, Kind::ReviewCheckRerun]
        .iter()
        .any(|kind| kind.key() == key)
    {
        return Err(DbError::Check(format!("{key} is invocation-scoped")));
    }
    sqlx::query("INSERT INTO task_budget(task_id,kind,window_id,spent) VALUES(?,?,COALESCE((SELECT window_id FROM task_budget WHERE task_id=? AND kind='execution'),'initial'),0) ON CONFLICT DO NOTHING")
        .bind(task).bind(key).bind(task).execute(&mut **tx).await?;
    let (window, old): (String, i64) =
        sqlx::query_as("SELECT window_id,spent FROM task_budget WHERE task_id=? AND kind=?")
            .bind(task)
            .bind(key)
            .fetch_one(&mut **tx)
            .await?;
    let recorded: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_budget_charge WHERE task_id=? AND kind=? AND window_id=? AND step_id=?)")
        .bind(task).bind(key).bind(&window).bind(step).fetch_one(&mut **tx).await?;
    if recorded || !allows_retry(limit, old) {
        return Ok(Charge {
            spent: old,
            remaining: remaining(limit, old),
            charged: false,
        });
    }
    sqlx::query("INSERT INTO task_budget_charge(task_id,kind,window_id,step_id) VALUES(?,?,?,?)")
        .bind(task)
        .bind(key)
        .bind(&window)
        .bind(step)
        .execute(&mut **tx)
        .await?;
    let spent = old.saturating_add(1);
    sqlx::query("UPDATE task_budget SET spent=? WHERE task_id=? AND kind=?")
        .bind(spent)
        .bind(task)
        .bind(key)
        .execute(&mut **tx)
        .await?;
    // The charge is authoritative; the shadow never refuses it.
    crate::task_condition::produce_best_effort(tx, task, crate::ConditionChange::Budget).await;
    Ok(Charge {
        spent,
        remaining: remaining(limit, spent),
        charged: true,
    })
}
pub async fn apply(tx: &mut Transaction<'_, Sqlite>, task: &str, effect: Mutation) -> Result<bool> {
    match effect {
        Mutation::Reset { key, window } => {
            let old: Option<(String, i64)> = sqlx::query_as(
                "SELECT window_id,spent FROM task_budget WHERE task_id=? AND kind=?",
            )
            .bind(task)
            .bind(&key)
            .fetch_optional(&mut **tx)
            .await?;
            reset(tx, task, &key, &window).await?;
            Ok(old.is_some_and(|(previous, spent)| previous != window && spent != 0))
        }
        Mutation::Charge { key, limit, step } => {
            let result = charge(tx, task, &key, limit, &step).await?;
            if !result.charged && !allows_retry(limit, result.spent) {
                let exists: bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_budget_charge c JOIN task_budget b USING(task_id,kind) WHERE c.task_id=? AND c.kind=? AND c.window_id=b.window_id AND c.step_id=?)")
                    .bind(task).bind(&key).bind(&step).fetch_one(&mut **tx).await?;
                if !exists {
                    return Err(DbError::Check(format!("{key} retry budget exhausted")));
                }
            }
            Ok(result.charged)
        }
    }
}

/// The invocation starts with its first request, including a verify that passes
/// without a rerun. Replaying the same Review never refunds an existing charge.
pub async fn initialize_verify_budget(
    db: &SqliteDb,
    task: &Task,
    workflow: &WorkflowDefinition,
    review_id: &str,
) -> Result<()> {
    let mut tx = verify_budget_transaction(db, task, workflow, review_id).await?;
    reset(&mut tx, &task.id, Kind::ReviewVerifyRerun.key(), review_id).await?;
    tx.commit().await?;
    Ok(())
}

async fn verify_budget_transaction(
    db: &SqliteDb,
    task: &Task,
    workflow: &WorkflowDefinition,
    review_id: &str,
) -> Result<Transaction<'static, Sqlite>> {
    if !workflow
        .states
        .iter()
        .any(|state| state.name == task.status && is_review_state(state))
    {
        return Err(DbError::InvalidTransition);
    }
    let mut tx = crate::begin_immediate(db.pool()).await?;
    if let Some(step) = crate::task_writer::current_task_step().filter(|s| s.task_id == task.id) {
        db.fence_hook_in_tx(&mut tx, &step).await?;
    }
    let current: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task t JOIN review r ON r.task_id=t.id WHERE t.id=? AND t.version=? AND t.status=? AND t.deleted_at IS NULL AND r.id=? AND r.status='running' AND NOT EXISTS(SELECT 1 FROM review newer WHERE newer.task_id=t.id AND newer.attempt_number>r.attempt_number))")
        .bind(&task.id).bind(task.version).bind(&task.status).bind(review_id).fetch_one(&mut *tx).await?;
    if !current {
        return Err(DbError::VersionConflict);
    }
    Ok(tx)
}

/// Admit one further verify evaluation. The Review id is the invocation window
/// and the evaluated result is the receipt: replay after a checkpoint gap must
/// return the same admission without charging twice.
pub async fn grant_verify_rerun(
    db: &SqliteDb,
    task: &Task,
    workflow: &WorkflowDefinition,
    state_config: &Value,
    review_id: &str,
    evaluation: u32,
) -> Result<bool> {
    let mut tx = verify_budget_transaction(db, task, workflow, review_id).await?;
    let key = Kind::ReviewVerifyRerun.key();
    let receipt = format!("verify:{review_id}:{evaluation}");
    reset(&mut tx, &task.id, key, review_id).await?;
    let paid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_budget_charge WHERE task_id=? AND kind=? AND window_id=? AND step_id=?)")
        .bind(&task.id).bind(key).bind(review_id).bind(&receipt).fetch_one(&mut *tx).await?;
    let admitted = paid
        || charge(
            &mut tx,
            &task.id,
            key,
            i64::from(limit(
                task,
                Kind::ReviewVerifyRerun,
                Some(state_config),
                None,
            )?),
            &receipt,
        )
        .await?
        .charged;
    tx.commit().await?;
    Ok(admitted)
}
/// Volatile kinds (native result-block corrections, review check re-runs) are
/// deliberately scoped to one invocation.
pub struct Invocation {
    kind: Kind,
    spent: i64,
}
impl Invocation {
    pub fn new(kind: Kind) -> Self {
        debug_assert!(kind.is_volatile());
        Self { kind, spent: 0 }
    }
    pub fn spent(&self) -> i64 {
        self.spent
    }
    pub fn consume(&mut self) -> bool {
        if !allows_retry(i64::from(self.kind.default_limit()), self.spent) {
            return false;
        }
        self.spent += 1;
        true
    }
    pub fn remaining(&self) -> i64 {
        remaining(i64::from(self.kind.default_limit()), self.spent)
    }
}
/// Authoritative remaining values, including arbitrary gate instances. Gate names
/// remain public keys; non-gate kinds use their stable names.
pub fn projection(
    task: &Task,
    workflow: &WorkflowDefinition,
    counts: &HashMap<String, i64>,
    recovery_limit: i64,
) -> Result<HashMap<String, i64>> {
    let mut out = HashMap::new();
    for state in &workflow.states {
        if state.kind != api_types::StateKind::Gate {
            continue;
        }
        if state
            .gate_config
            .as_ref()
            .and_then(|g| g.max_rejections)
            .is_none()
            && !is_review_state(state)
        {
            continue;
        }
        let kind = if is_review_state(state) {
            Kind::Review
        } else {
            Kind::GateRejection
        };
        let mut origin = task.clone();
        origin.status = state.name.clone();
        let n = limit(
            &origin,
            kind,
            Some(&state.config),
            state.gate_config.as_ref(),
        )?;
        if is_review_state(state) {
            let cap = limit(
                &origin,
                Kind::GateRejection,
                Some(&state.config),
                state.gate_config.as_ref(),
            )?;
            out.insert(
                "review_gate".into(),
                remaining(
                    i64::from(cap.saturating_sub(1).max(0)),
                    *counts.get(REVIEW_GATE_KEY).unwrap_or(&0),
                ),
            );
        }
        out.insert(
            state.name.clone(),
            remaining(
                i64::from(n),
                *counts.get(&gate_budget_key(state)).unwrap_or(&0),
            ),
        );
    }
    let current = workflow.states.iter().find(|s| s.name == task.status);
    for kind in Kind::PERSISTED
        .into_iter()
        .filter(|k| !matches!(k, Kind::Review))
    {
        let scoped = if matches!(kind, Kind::ReviewProgress | Kind::ReviewVerifyRerun) {
            current
                .filter(|state| is_review_state(state))
                .or_else(|| workflow.states.iter().find(|state| is_review_state(state)))
        } else if kind == Kind::MergeFix {
            workflow.states.iter().find(|s| s.name == "merging")
        } else {
            current
        };
        let n = if kind == Kind::AutomaticReviewRecovery {
            recovery_limit
        } else {
            i64::from(limit(
                task,
                kind,
                scoped.map(|s| &s.config),
                scoped.and_then(|s| s.gate_config.as_ref()),
            )?)
        };
        out.insert(
            kind.key().into(),
            remaining(n, *counts.get(kind.key()).unwrap_or(&0)),
        );
    }
    out.insert(
        Kind::ReportCorrection.key().into(),
        i64::from(Kind::ReportCorrection.default_limit()),
    );
    // Only a root that re-planned has this row; no other Task shows it.
    if let Some(spent) = counts.get(Kind::Replan.key()) {
        out.insert(
            Kind::Replan.key().into(),
            remaining(i64::from(Kind::Replan.default_limit()), *spent),
        );
    }
    Ok(out)
}

/// Preserve the reviewer finding policy at the charge point. A verdict was
/// already admitted, so it always spends; whether it exhausts the budget, and
/// so opens a recovery episode, is decided where the limit is resolved
/// (`review_failure_target`).
pub async fn review_verdict(
    tx: &mut Transaction<'_, Sqlite>,
    task: &Task,
    review: &Review,
    status: &ReviewStatus,
    details: &Value,
    owner: bool,
) -> Result<()> {
    if *status == ReviewStatus::Passed {
        if let Some(contract) = details
            .pointer("/conformance/contract/execution_id")
            .and_then(Value::as_str)
        {
            reset(tx, &task.id, Kind::ReviewCarry.key(), contract).await?;
        }
        return Ok(());
    }
    if *status != ReviewStatus::Failed
        || owner
        || owner_step(&task.id)
        || details.pointer("/verify/status").and_then(Value::as_str) == Some("inconclusive")
    {
        return Ok(());
    }
    let latest: Option<String> = sqlx::query_scalar(
        "SELECT id FROM review WHERE task_id=? ORDER BY attempt_number DESC,id DESC LIMIT 1",
    )
    .bind(&task.id)
    .fetch_optional(&mut **tx)
    .await?;
    if latest.as_deref() != Some(review.id.as_str()) || details.get("execution_failure").is_some() {
        return Ok(());
    }
    if owner_requested_review_entry(tx, &task.id, None, Some(&review.created_at)).await? {
        return Ok(());
    }
    if let Some(c) = details.get("conformance").filter(|v| !v.is_null()) {
        let conformance: api_types::ReviewConformance =
            serde_json::from_value(c.clone()).map_err(|e| DbError::Check(e.to_string()))?;
        if conformance.status == api_types::ConformanceStatus::Blocked {
            return Ok(());
        }
        let window_reviews = SqliteDb::review_window_in_tx(tx, &task.id).await?.reviews;
        let previous_reviews = sqlx::query("SELECT * FROM review WHERE task_id=? AND attempt_number<? ORDER BY attempt_number DESC,id DESC LIMIT 1")
            .bind(&task.id).bind(review.attempt_number).fetch_all(&mut **tx).await?
            .into_iter().map(crate::sqlite::map_review).collect::<Result<Vec<_>>>()?;
        if owner_review_failure_message(
            &conformance,
            review,
            &previous_reviews,
            &window_reviews,
            !coordination_root_in_tx(tx, task).await?,
        )
        .is_some()
        {
            return Ok(());
        }
    }
    admit(
        tx,
        &task.id,
        Kind::Review.key(),
        &format!("review:{}", review.id),
    )
    .await?;
    Ok(())
}

/// The review budget ran out on `review`: open that exhaustion's automatic
/// recovery episode. Idempotent per exhausting verdict.
pub async fn open_recovery_episode(db: &SqliteDb, task: &str, review: &str) -> Result<()> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    db.fence_task_lease_in_tx(&mut tx, task, "review recovery episode")
        .await?;
    open_recovery_episode_in_tx(&mut tx, task, review).await?;
    tx.commit().await?;
    Ok(())
}
async fn open_recovery_episode_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    task: &str,
    review: &str,
) -> Result<()> {
    reset(
        tx,
        task,
        Kind::AutomaticReviewRecovery.key(),
        &format!("exhaustion:{review}"),
    )
    .await
}

/// Charge the budget a committed transition consumed. Every charge here is an
/// outcome the workflow already admitted, so limits are not consulted.
///
/// `owner` is the transition actor's typed owner authority
/// ([`api_types::Actor::is_owner`]): an owner's move spends nothing.
/// `entry_step` is the hook step whose failed review entry produced this
/// cascade; the bounce shares that entry's receipt instead of charging twice.
#[allow(clippy::too_many_arguments)]
pub async fn transition(
    tx: &mut Transaction<'_, Sqlite>,
    task: &str,
    from: &str,
    owner: bool,
    bridge: &api_types::TransitionBridge,
    rejection: bool,
    step: &str,
    entry_step: Option<&str>,
) -> Result<()> {
    if bridge.bridge_kind == Some(api_types::TransitionBridgeKind::RetryWindowReset) {
        return reset_all(tx, task, step).await;
    }
    if owner {
        return Ok(());
    }
    let from_review = if rejection {
        let raw: String = sqlx::query_scalar(
            "SELECT p.workflow_definition FROM project p JOIN task t ON t.project_id=p.id WHERE t.id=?",
        ).bind(task).fetch_one(&mut **tx).await?;
        review_state_from_json(&raw, from)
    } else {
        false
    };
    if from_review {
        if owner_requested_review_entry(tx, task, Some(step), None).await? {
            return Ok(());
        }
        let latest:Option<(String,String)>=sqlx::query_as("SELECT id,status FROM review WHERE task_id=? ORDER BY attempt_number DESC,id DESC LIMIT 1").bind(task).fetch_optional(&mut **tx).await?;
        // Failed verdict settlement is the charge point for failures. This branch
        // retains only the already-existing gate-outcome debit (or a gate with no Review).
        if latest.as_ref().is_none_or(|(_, status)| status != "failed") {
            let receipt = entry_step.map(entry_charge_step);
            admit(
                tx,
                task,
                Kind::Review.key(),
                receipt.as_deref().unwrap_or(step),
            )
            .await?;
        }
        admit(tx, task, REVIEW_GATE_KEY, step).await?;
        return Ok(());
    }
    let key = match bridge.bridge_kind {
        Some(api_types::TransitionBridgeKind::TargetMovedRebase) => {
            Some(Kind::TargetMovedRebase.key().to_owned())
        }
        Some(api_types::TransitionBridgeKind::ConflictHandoff) => {
            Some(Kind::ConflictHandoff.key().to_owned())
        }
        _ if rejection && from == "merging" && !bridge.is_review_refresh() => {
            admit(tx, task, &gate_key(from), step).await?;
            Some(Kind::MergeFix.key().to_owned())
        }
        _ if rejection => Some(gate_key(from)),
        _ => None,
    };
    if let Some(key) = key {
        admit(tx, task, &key, step).await?;
    }
    Ok(())
}

pub fn recovery_limit(settings: &str) -> i64 {
    let settings = serde_json::from_str::<Value>(settings).unwrap_or(Value::Null);
    if !settings
        .pointer("/automatic_recovery/enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return 0;
    }
    settings
        .pointer("/automatic_recovery/max_attempts")
        .and_then(Value::as_i64)
        .unwrap_or(1)
        .max(1)
}

/// Keep Project defaults in this resolver too; top-level Project review/execution
/// retry fields were inert and remain so. Only the existing merging overlay applies.
pub fn state_config(state: &StateDefinition, settings: &str) -> Value {
    let mut config = state.config.clone();
    let settings = serde_json::from_str::<Value>(settings).unwrap_or(Value::Null);
    if state.name == "merging" {
        if let Some(value) = settings.pointer("/retry_budgets/merge_fix") {
            if !config.is_object() {
                config = serde_json::json!({});
            }
            if !config.get("retry_budgets").is_some_and(Value::is_object) {
                config["retry_budgets"] = serde_json::json!({});
            }
            config["retry_budgets"]["merge_fix"] = value.clone();
        }
    }
    if is_review_state(state) {
        if let Some(defaults) = settings
            .get("default_review_config")
            .and_then(Value::as_object)
        {
            if !config.is_object() {
                config = serde_json::json!({});
            }
            let object = config.as_object_mut().expect("initialized object");
            for (key, value) in defaults {
                object.entry(key.clone()).or_insert_with(|| value.clone());
            }
        }
        for key in ["review_progress", "review_verify_rerun"] {
            let pointer = format!("/retry_budgets/{key}");
            if let Some(value) = settings
                .pointer(&pointer)
                .filter(|_| config.pointer(&pointer).is_none())
            {
                if !config.is_object() {
                    config = serde_json::json!({});
                }
                if !config.get("retry_budgets").is_some_and(Value::is_object) {
                    config["retry_budgets"] = serde_json::json!({});
                }
                config["retry_budgets"][key] = value.clone();
            }
        }
    }
    config
}
pub fn with_project_defaults(workflow: &WorkflowDefinition, settings: &str) -> WorkflowDefinition {
    let mut workflow = workflow.clone();
    for state in &mut workflow.states {
        state.config = state_config(state, settings);
    }
    workflow
}
/// Whether the Task step running now acts for the owner: a hook, cascade or
/// request whose typed actor has owner authority.
fn owner_step(task: &str) -> bool {
    crate::task_writer::current_task_step()
        .filter(|s| s.task_id == task)
        .and_then(|s| serde_json::from_str::<Value>(&s.payload_json).ok())
        .is_some_and(|p| {
            ["/actor", "/request/actor"].into_iter().any(|pointer| {
                p.pointer(pointer).is_some_and(|actor| {
                    serde_json::from_value::<api_types::Actor>(actor.clone())
                        .is_ok_and(|actor| actor.is_owner())
                })
            })
        })
}

/// Whether the owner asked for the review entry the Task is in, or the one it
/// is leaving: a review re-run records who requested it on the entry's
/// transition row. Such an entry is routed as the workflow's own, but nothing
/// it causes (red checks, a failing reviewer, the bounce to remediation)
/// spends review retry budget. `leaving` is the transition row being written
/// by the move out of `review`; `since` limits the answer to a Review the
/// entry itself opened.
pub async fn owner_requested_review_entry(
    tx: &mut Transaction<'_, Sqlite>,
    task: &str,
    leaving: Option<&str>,
    since: Option<&str>,
) -> Result<bool> {
    Ok(review_rerun_entry(&mut **tx, task, leaving)
        .await?
        .is_some_and(|(requested_by, entered_at)| {
            requested_by.is_owner() && since.is_none_or(|opened| opened >= entered_at.as_str())
        }))
}

/// The latest entry of the Task into `review`, when it is a review re-run:
/// who asked for it and when it began. `leaving` excludes the transition row
/// a move out of `review` is writing. A Review created before that time is
/// not this entry's.
pub async fn review_rerun_entry<'e, E>(
    executor: E,
    task: &str,
    leaving: Option<&str>,
) -> Result<Option<(api_types::Actor, String)>>
where
    E: sqlx::Executor<'e, Database = Sqlite>,
{
    // Retain only the latest entry per state, then resolve the review state
    // from the Project workflow. Recovery markers have no entry epoch.
    let entries = sqlx::query(
        "WITH entries AS (
            SELECT id,bridge_kind,bridge_payload,created_at,to_state,status_epoch,
                   ROW_NUMBER() OVER (PARTITION BY to_state ORDER BY status_epoch DESC,created_at DESC,rowid DESC) AS ordinal
            FROM transition_log WHERE task_id=? AND status_epoch IS NOT NULL AND id IS NOT ?
         )
         SELECT e.*,p.workflow_definition FROM entries e
         JOIN task t ON t.id=? JOIN project p ON p.id=t.project_id
         WHERE e.ordinal=1 ORDER BY e.status_epoch DESC,e.created_at DESC",
    )
    .bind(task)
    .bind(leaving)
    .bind(task)
    .fetch_all(executor)
    .await?;
    for entry in entries {
        let workflow: String = entry.try_get("workflow_definition")?;
        let state: String = entry.try_get("to_state")?;
        if !review_state_from_json(&workflow, &state) {
            continue;
        }
        let id: String = entry.try_get("id")?;
        let kind: Option<String> = entry.try_get("bridge_kind")?;
        let payload: Option<String> = entry.try_get("bridge_payload")?;
        let entered_at: String = entry.try_get("created_at")?;
        return Ok(
            crate::decode_transition_bridge(&id, kind.as_deref(), payload.as_deref())
                .ok()
                .and_then(|bridge| bridge.review_rerun_requester())
                .map(|requested_by| (requested_by, entered_at)),
        );
    }
    Ok(None)
}

/// Honor routing only when the reviewer verdict caused the failure. Forge
/// replaces the assessment reason when setup, checks, or reproduction fail.
pub fn owner_review_failure_message(
    conformance: &api_types::ReviewConformance,
    review: &Review,
    reviews: &[Review],
    window_reviews: &[Review],
    derived_rules: bool,
) -> Option<String> {
    let assessment = conformance.assessment.as_ref()?;
    if conformance.status != api_types::ConformanceStatus::Failed
        || assessment.result != api_types::ReviewResult::Fail
    {
        return None;
    }
    // Owner routing and the self-reported repeat need the reviewer's verdict
    // to be the cause; the derived repeat is Forge's own rule over the ledger.
    let verdict_caused = conformance.checks.iter().all(|check| !check.is_blocking())
        && conformance.reason.as_deref().unwrap_or_default() == assessment.reason;
    let reason = if assessment.reason.is_empty() {
        "reviewer reported a blocking finding"
    } else {
        &assessment.reason
    };
    if verdict_caused && assessment.fixable_by == api_types::FixableBy::Owner {
        return Some(format!("fixable by owner: {reason}"));
    }
    let mut projected = review.clone();
    let mut details: Value = serde_json::from_str(&review.step_results_json).ok()?;
    if !details.is_object() {
        details = serde_json::json!({});
    }
    details["conformance"] = serde_json::to_value(conformance).ok()?;
    projected.step_results_json = details.to_string();
    if verdict_caused && !assessment.findings.is_empty() {
        let mut window: Vec<_> = window_reviews
            .iter()
            .filter(|row| row.attempt_number < review.attempt_number)
            .cloned()
            .collect();
        window.push(projected.clone());
        if let Ok(ledger) = crate::review_findings::finding_ledger(&window) {
            if !ledger.iter().any(|entry| {
                entry.status.is_open()
                    && entry.severity == api_types::ReviewFindingSeverity::Blocking
            }) {
                return Some("only non-blocking findings remain".into());
            }
        }
    }
    if derived_rules {
        if let Ok(policy) = crate::review_findings::round_policy(window_reviews, &projected) {
            if policy.derived {
                return policy.repeat;
            }
        }
    }
    if !verdict_caused {
        return None;
    }
    let previous_failed = reviews
        .iter()
        .filter(|previous| previous.attempt_number < review.attempt_number)
        .max_by_key(|previous| (previous.attempt_number, previous.id.as_str()))
        .is_some_and(|previous| {
            previous.status == ReviewStatus::Failed
                && serde_json::from_str::<Value>(&previous.step_results_json)
                    .ok()
                    .and_then(|details| {
                        serde_json::from_value::<api_types::ReviewConformance>(
                            details["conformance"].clone(),
                        )
                        .ok()
                    })
                    .is_some_and(|conformance| {
                        conformance.status == api_types::ConformanceStatus::Failed
                            && conformance.checks.iter().all(|check| !check.is_blocking())
                            && conformance.assessment.as_ref().is_some_and(|assessment| {
                                assessment.result == api_types::ReviewResult::Fail
                                    && conformance.reason.as_deref()
                                        == Some(assessment.reason.as_str())
                            })
                    })
        });
    (assessment.repeat && previous_failed).then(|| format!("repeated finding: {reason}"))
}

// Charging uses exactly the planning-role resolution used by services.
async fn coordination_root_in_tx(tx: &mut Transaction<'_, Sqlite>, task: &Task) -> Result<bool> {
    if task.parent_task_id.is_some() {
        return Ok(false);
    }
    let raw: String = sqlx::query_scalar("SELECT workflow_definition FROM project WHERE id=?")
        .bind(&task.project_id)
        .fetch_one(&mut **tx)
        .await?;
    let Some(role) = WorkflowDefinition::coordination_planning_role_from_json(&raw) else {
        return Ok(false);
    };
    Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_role_assignment a
        WHERE a.task_id=? AND a.role_name=? AND a.assignee_type='agent' AND a.assignee_id IS NOT NULL
        AND EXISTS(SELECT 1 FROM task child WHERE child.parent_task_id=a.task_id AND child.deleted_at IS NULL))")
        .bind(&task.id).bind(role).fetch_one(&mut **tx).await?)
}

/// Admit one progress round atomically under Task authority. A replay keeps its
/// earlier grant even after spending the last allowance. Owner re-runs are
/// free but stay within the limit.
pub async fn grant_review_progress(
    db: &SqliteDb,
    task: &Task,
    review: &Review,
    limit: i64,
) -> Result<bool> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    db.fence_task_lease_in_tx(&mut tx, &task.id, "review progress round")
        .await?;
    let valid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task t WHERE t.id=? AND t.version=? AND t.status=? AND t.deleted_at IS NULL AND ?=(SELECT r.id FROM review r WHERE r.task_id=t.id ORDER BY r.attempt_number DESC,r.id DESC LIMIT 1))")
        .bind(&task.id).bind(task.version).bind(&task.status).bind(&review.id).fetch_one(&mut *tx).await?;
    if !valid {
        return Err(DbError::VersionConflict);
    }
    let window: String = sqlx::query_scalar("SELECT COALESCE((SELECT window_id FROM task_budget WHERE task_id=? AND kind='review'),'initial')")
        .bind(&task.id).fetch_one(&mut *tx).await?;
    reset(&mut tx, &task.id, Kind::ReviewProgress.key(), &window).await?;
    if owner_requested_review_entry(&mut tx, &task.id, None, Some(&review.created_at)).await? {
        // An owner's re-run spends nothing, but it never exceeds the limit:
        // `review_progress = 0` stays disabled and a spent window stays spent.
        let spent: i64 =
            sqlx::query_scalar("SELECT spent FROM task_budget WHERE task_id=? AND kind=?")
                .bind(&task.id)
                .bind(Kind::ReviewProgress.key())
                .fetch_one(&mut *tx)
                .await?;
        tx.commit().await?;
        return Ok(allows_retry(limit, spent));
    }
    let receipt = format!("progress:{}", review.id);
    let paid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_budget_charge WHERE task_id=? AND kind=? AND window_id=? AND step_id=?)")
        .bind(&task.id).bind(Kind::ReviewProgress.key()).bind(&window).bind(&receipt).fetch_one(&mut *tx).await?;
    let granted = paid
        || charge(
            &mut tx,
            &task.id,
            Kind::ReviewProgress.key(),
            limit,
            &receipt,
        )
        .await?
        .charged;
    tx.commit().await?;
    Ok(granted)
}

/// A failed review-entry hook with no failed Review consumes one Review
/// attempt, owner-entered or not, exactly as base counted the bounce; only an
/// entry the owner asked for as a review re-run
/// ([`owner_requested_review_entry`]) consumes none. Its
/// receipt ([`entry_charge_step`]) and either the exhausted barrier or the
/// cleared barrier commit together; the bounce cascade reuses the receipt.
pub async fn failed_review_entry(
    db: &SqliteDb,
    task: &Task,
    limit: i64,
    hook_step: &str,
    started_at: &str,
) -> Result<(Task, bool)> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    db.fence_task_lease_in_tx(&mut tx, &task.id, "failed review entry budget")
        .await?;
    let current = db
        .get_task_in_tx(&mut tx, &task.id)
        .await?
        .ok_or(DbError::NotFound)?;
    if current.status != task.status {
        return Err(DbError::VersionConflict);
    }
    let spent_now = if owner_requested_review_entry(&mut tx, &task.id, None, None).await? {
        // The owner asked for this entry: it leaves the budget where it was.
        sqlx::query_scalar(
            "SELECT COALESCE((SELECT spent FROM task_budget WHERE task_id=? AND kind=?),0)",
        )
        .bind(&task.id)
        .bind(Kind::Review.key())
        .fetch_one(&mut *tx)
        .await?
    } else {
        charge(
            &mut tx,
            &task.id,
            Kind::Review.key(),
            limit,
            &entry_charge_step(hook_step),
        )
        .await?
        .spent
    };
    let allowed = allows_retry(limit, spent_now);
    let barrier = (!allowed).then(|| exhausted_review_barrier(&task.status, started_at));
    sqlx::query("UPDATE task SET entry_barrier_json=?,updated_at=?,version=version+1 WHERE id=?")
        .bind(barrier)
        .bind(crate::now_rfc3339())
        .bind(&task.id)
        .execute(&mut *tx)
        .await?;
    let updated = db
        .get_task_in_tx(&mut tx, &task.id)
        .await?
        .ok_or(DbError::NotFound)?;
    crate::task_condition::produce(&mut tx, &updated.id, crate::ConditionChange::Legacy).await?;
    tx.commit().await?;
    Ok((updated, allowed))
}

/// The entry barrier that parks a Task whose review entry budget ran out. The
/// reset-and-retry offers key on its `blocking_reason`.
pub fn exhausted_review_barrier(state: &str, started_at: &str) -> String {
    serde_json::json!({
        "state": state,
        "status": "blocked",
        "started_at": started_at,
        "updated_at": crate::now_rfc3339(),
        "blocking_reason": "review retry budget exhausted",
    })
    .to_string()
}

/// A pre-upgrade verdict, or a crash between verdict persistence and disposition,
/// is settled exactly once using the same charge point as a live verdict.
pub async fn reconcile_failed_review(db: &SqliteDb, task: &Task, review: &Review) -> Result<()> {
    if review.status != ReviewStatus::Failed {
        return Ok(());
    }
    let paid:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_budget_charge c JOIN task_budget b USING(task_id,kind) WHERE c.task_id=? AND c.kind='review' AND c.window_id=b.window_id AND c.step_id=?)").bind(&task.id).bind(format!("review:{}",review.id)).fetch_one(db.pool()).await?;
    if paid {
        return Ok(());
    }
    let mut details = serde_json::from_str::<Value>(&review.step_results_json)
        .unwrap_or_else(|_| serde_json::json!({}));
    if !details.is_object() {
        details = serde_json::json!({});
    }
    if details.get("conformance").is_some_and(|c| {
        !c.is_null() && serde_json::from_value::<api_types::ReviewConformance>(c.clone()).is_err()
    }) {
        details
            .as_object_mut()
            .expect("normalized object")
            .remove("conformance");
    }
    let mut tx = crate::begin_immediate(db.pool()).await?;
    db.fence_task_lease_in_tx(&mut tx, &task.id, "review budget reconciliation")
        .await?;
    let owner:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM domain_event WHERE event_type='review.status_changed' AND actor_type='user' AND json_valid(payload_json) AND json_extract(payload_json,'$.review_id')=? AND json_extract(payload_json,'$.status')='failed')").bind(&review.id).fetch_one(&mut *tx).await?;
    let current = db
        .get_task_in_tx(&mut tx, &task.id)
        .await?
        .ok_or(DbError::NotFound)?;
    if current.status != task.status {
        return Err(DbError::VersionConflict);
    }
    review_verdict(&mut tx, &current, review, &review.status, &details, owner).await?;
    tx.commit().await?;
    Ok(())
}

pub async fn matches_spent(
    tx: &mut Transaction<'_, Sqlite>,
    task: &str,
    key: &str,
    expected: i64,
    window: &str,
) -> Result<bool> {
    Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_budget WHERE task_id=? AND kind=? AND spent=? AND window_id=?)").bind(task).bind(key).bind(expected).bind(window).fetch_one(&mut **tx).await?)
}
/// Upgrade pending, typed DB effects atomically with their retained ledger.
/// Arbitrary metadata Set values and opaque command arguments are never traversed.
pub(crate) async fn migrate_pending_mutations(tx: &mut Transaction<'_, Sqlite>) -> Result<()> {
    let rows:Vec<(String,String,String)>=sqlx::query_as("SELECT id,task_id,payload_json FROM task_step WHERE kind='mutation' AND status IN ('pending','claimed','failed')").fetch_all(&mut **tx).await?;
    for (step, task_id, raw) in rows {
        let mut payload: Value =
            serde_json::from_str(&raw).map_err(|e| DbError::Check(e.to_string()))?;
        let Some(root) = payload.as_object_mut() else {
            continue;
        };
        let Some((name, args)) = root.iter_mut().next() else {
            continue;
        };
        if !name.starts_with("TaskMutateMetadata")
            && !name.starts_with("TaskUpdateRecoveryMetadata")
        {
            continue;
        }
        let Some(args) = args.as_object_mut() else {
            continue;
        };
        let field = if args.contains_key("mutations") {
            "mutations"
        } else {
            "metadata_mutations"
        };
        let Some(mutations) = args.get_mut(field) else {
            continue;
        };
        let original: Vec<crate::TaskMetadataMutation> =
            serde_json::from_value(mutations.clone()).map_err(|e| DbError::Check(e.to_string()))?;
        let mut changed = false;
        let mut converted = Vec::new();
        for (ordinal, mutation) in original.into_iter().enumerate() {
            let replacement = match mutation {
                crate::TaskMetadataMutation::Increment { key, by: 1 }
                    if key == "execution_retry_count" || key == "workflow_guard_retry_count" =>
                {
                    changed = true;
                    let kind = if key == "execution_retry_count" {
                        Kind::Execution
                    } else {
                        Kind::WorkflowGuard
                    };
                    // The old binary admitted this increment before queuing
                    // it and applied it unconditionally: it is not re-checked.
                    crate::TaskMetadataMutation::Budget(Mutation::Charge {
                        key: kind.key().into(),
                        limit: i64::MAX,
                        step: format!("upgrade:{step}:{ordinal}"),
                    })
                }
                crate::TaskMetadataMutation::CompareAndMutate {
                    key,
                    expected,
                    mut mutations,
                } if key == "execution_retry_count" || key == "workflow_guard_retry_count" => {
                    changed = true;
                    let kind = if key == "execution_retry_count" {
                        Kind::Execution
                    } else {
                        Kind::WorkflowGuard
                    };
                    let window: String = sqlx::query_scalar(
                        "SELECT window_id FROM task_budget WHERE task_id=? AND kind=?",
                    )
                    .bind(&task_id)
                    .bind(kind.key())
                    .fetch_one(&mut **tx)
                    .await?;
                    if kind == Kind::WorkflowGuard {
                        mutations.push(crate::TaskMetadataMutation::Budget(Mutation::Reset {
                            key: kind.key().into(),
                            window: format!("success:upgrade:{step}"),
                        }));
                    }
                    crate::TaskMetadataMutation::BudgetIfSpent {
                        key: kind.key().into(),
                        expected: expected
                            .as_u64()
                            .map(|n| n.min(i64::MAX as u64) as i64)
                            .unwrap_or(0),
                        window_id: window,
                        mutations,
                    }
                }
                mutation => mutation,
            };
            converted.push(replacement);
        }
        if changed {
            *mutations =
                serde_json::to_value(converted).map_err(|e| DbError::Check(e.to_string()))?;
            sqlx::query("UPDATE task_step SET payload_json=? WHERE id=?")
                .bind(payload.to_string())
                .bind(step)
                .execute(&mut **tx)
                .await?;
        }
    }
    Ok(())
}

pub async fn cancelled_review_entry_allows_retry(
    db: &SqliteDb,
    task: &Task,
    gate: Option<&GateConfig>,
) -> Result<bool> {
    let cap = limit(task, Kind::GateRejection, None, gate)?
        .saturating_sub(1)
        .max(0);
    Ok(allows_retry(
        i64::from(cap),
        spent(db.pool(), &task.id, REVIEW_GATE_KEY).await?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TaskRepo;
    use serde_json::json;

    async fn fixture() -> (SqliteDb, Task) {
        let pool = crate::create_sqlite_pool("sqlite::memory:").await.unwrap();
        crate::run_migrations(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO project(id,name,created_at,updated_at) VALUES('p','Project','now','now')",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('t','p','Task','review','now','now')").execute(&pool).await.unwrap();
        let db = SqliteDb::new(pool);
        let task = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
        (db, task)
    }

    /// The re-plan limit is fixed: no `retry_budgets` value, Task-wide or
    /// per state, changes it, and what is projected is counted against it.
    #[tokio::test]
    async fn the_replan_limit_is_fixed_and_projected_only_once_spent() {
        let (db, mut task) = fixture().await;
        task.task_state_config = Some(
            json!({"retry_budgets":{"replan":1},"in_progress":{"retry_budgets":{"replan":9}}})
                .to_string(),
        );
        let merged = json!({"retry_budgets":{"replan":9}});
        assert_eq!(limit(&task, Kind::Replan, None, None).unwrap(), 5);
        assert_eq!(limit(&task, Kind::Replan, Some(&merged), None).unwrap(), 5);
        assert!(!Kind::PERSISTED.contains(&Kind::Replan));
        let workflow = WorkflowDefinition::default();
        let before = projection(
            &task,
            &workflow,
            &load(db.pool(), &task.id).await.unwrap(),
            3,
        )
        .unwrap();
        assert!(!before.contains_key("replan"), "{before:?}");
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        assert!(
            charge(&mut tx, &task.id, "replan", 5, "replan:child:park")
                .await
                .unwrap()
                .charged
        );
        tx.commit().await.unwrap();
        assert!(
            receipt_recorded(db.pool(), &task.id, "replan", "replan:child:park")
                .await
                .unwrap()
        );
        assert!(
            !receipt_recorded(db.pool(), &task.id, "replan", "replan:child:other")
                .await
                .unwrap()
        );
        let after = projection(
            &task,
            &workflow,
            &load(db.pool(), &task.id).await.unwrap(),
            3,
        )
        .unwrap();
        assert_eq!(after.get("replan"), Some(&4), "{after:?}");
    }

    #[tokio::test]
    async fn every_persisted_kind_charges_blocks_replays_and_resets() {
        let (db, task) = fixture().await;
        let mut keys = Kind::PERSISTED
            .into_iter()
            .map(|k| k.key().to_owned())
            .collect::<Vec<_>>();
        keys.push(gate_key("planning"));
        for key in keys {
            let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
            let first = charge(&mut tx, &task.id, &key, 2, "first").await.unwrap();
            assert!(first.charged);
            assert_eq!(first.remaining, 1);
            let duplicate = charge(&mut tx, &task.id, &key, 2, "first").await.unwrap();
            assert!(!duplicate.charged);
            assert_eq!(duplicate.spent, 1);
            let second = charge(&mut tx, &task.id, &key, 2, "second").await.unwrap();
            assert!(second.charged);
            assert_eq!(second.remaining, 0);
            let blocked = charge(&mut tx, &task.id, &key, 2, "third").await.unwrap();
            assert!(!blocked.charged);
            assert_eq!(blocked.spent, 2);
            reset(&mut tx, &task.id, &key, &format!("natural-window:{key}"))
                .await
                .unwrap();
            assert!(
                charge(&mut tx, &task.id, &key, 2, "first")
                    .await
                    .unwrap()
                    .charged
            );
            // Replaying the reset must not refund a charge in that window.
            reset(&mut tx, &task.id, &key, &format!("natural-window:{key}"))
                .await
                .unwrap();
            tx.commit().await.unwrap();
            assert_eq!(spent(db.pool(), &task.id, &key).await.unwrap(), 1);
        }
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        reset_all(&mut tx, &task.id, "explicit-reset")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(load(db.pool(), &task.id)
            .await
            .unwrap()
            .values()
            .all(|n| *n == 0));
    }

    #[tokio::test]
    async fn consuming_write_rollback_refunds_charge_and_owner_sendback_is_free() {
        let (db, task) = fixture().await;
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        charge(&mut tx, &task.id, Kind::Execution.key(), 3, "failure")
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        assert_eq!(
            spent(db.pool(), &task.id, Kind::Execution.key())
                .await
                .unwrap(),
            0
        );
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        for state in ["review", "planning", "merging"] {
            transition(
                &mut tx,
                &task.id,
                state,
                true,
                &api_types::TransitionBridge::new(api_types::TransitionBridgeKind::GateRejected),
                true,
                state,
                None,
            )
            .await
            .unwrap();
        }
        tx.commit().await.unwrap();
        assert!(load(db.pool(), &task.id)
            .await
            .unwrap()
            .values()
            .all(|n| *n == 0));
    }

    #[tokio::test]
    async fn execution_spending_survives_review_lap_and_resets_only_explicitly() {
        let (db, task) = fixture().await;
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        charge(&mut tx, &task.id, Kind::Execution.key(), 3, "failure")
            .await
            .unwrap();
        sqlx::query("UPDATE task SET status='in_progress' WHERE id='t'")
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("UPDATE task SET status='review' WHERE id='t'")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(
            spent(db.pool(), &task.id, Kind::Execution.key())
                .await
                .unwrap(),
            1
        );
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        transition(
            &mut tx,
            &task.id,
            "review",
            true,
            &api_types::TransitionBridge::recovery("retry", true),
            false,
            "reset",
            None,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(
            spent(db.pool(), &task.id, Kind::Execution.key())
                .await
                .unwrap(),
            0
        );
    }

    async fn log_review_entry(
        db: &SqliteDb,
        id: &str,
        epoch: i64,
        at: &str,
        by: Option<api_types::Actor>,
    ) {
        let bridge = by
            .map(|actor| api_types::TransitionBridge::review_rerun(&actor))
            .unwrap_or_default();
        sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,created_at,bridge_kind,bridge_payload,status_epoch) VALUES(?,'t','review','review','system','entry',?,?,?,?)")
            .bind(id)
            .bind(at)
            .bind(bridge.bridge_kind.map(api_types::TransitionBridgeKind::as_str))
            .bind(bridge.bridge_payload.map(|payload| payload.to_string()))
            .bind(epoch)
            .execute(db.pool())
            .await
            .unwrap();
    }
    async fn review_spent(db: &SqliteDb) -> (i64, i64) {
        (
            spent(db.pool(), "t", Kind::Review.key()).await.unwrap(),
            spent(db.pool(), "t", REVIEW_GATE_KEY).await.unwrap(),
        )
    }

    // Every charge a review entry can cause is skipped when the owner asked
    // for that entry as a review re-run, and only then: a failing reviewer and
    // the bounce out of `review` here; the failed entry hook asks the same
    // question under its step lease (services `rerun_review` tests).
    #[tokio::test]
    async fn an_owner_requested_review_rerun_entry_spends_nothing_and_any_other_entry_spends() {
        let (db, task) = fixture().await;
        let owner = api_types::Actor::user(api_types::UserActionSource::Api);
        let workflow = api_types::Actor::system(api_types::SystemComponent::Workflow);
        sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES('e','t','coder','completed','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')").execute(db.pool()).await.unwrap();
        let fail = |id: &'static str, attempt: i64, at: &'static str| {
            let db = &db;
            let task = &task;
            async move {
                sqlx::query("INSERT INTO review(id,task_id,execution_id,attempt_number,status,step_results_json,started_at,created_at,updated_at) VALUES(?,'t','e',?,'running','{}',?,?,?)").bind(id).bind(attempt).bind(at).bind(at).bind(at).execute(db.pool()).await.unwrap();
                let review = crate::ReviewRepo::get_by_id(db, id).await.unwrap().unwrap();
                let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
                review_verdict(
                    &mut tx,
                    task,
                    &review,
                    &ReviewStatus::Failed,
                    &json!({}),
                    false,
                )
                .await
                .unwrap();
                sqlx::query("UPDATE review SET status='failed' WHERE id=?")
                    .bind(id)
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
            }
        };
        let bounce = |log: &'static str| {
            let db = &db;
            async move {
                let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
                transition(
                    &mut tx,
                    "t",
                    "review",
                    false,
                    &Default::default(),
                    true,
                    log,
                    None,
                )
                .await
                .unwrap();
                tx.commit().await.unwrap();
            }
        };

        // The owner's re-runs: three entries, each fails every way it can.
        for (n, (entry, review, at, out)) in [
            ("o1", "r1", "2026-01-01T00:00:01Z", "x1"),
            ("o2", "r2", "2026-01-01T00:00:02Z", "x2"),
            ("o3", "r3", "2026-01-01T00:00:03Z", "x3"),
        ]
        .into_iter()
        .enumerate()
        {
            log_review_entry(&db, entry, n as i64 + 1, at, Some(owner.clone())).await;
            fail(review, n as i64 + 1, at).await;
            bounce(out).await;
            assert_eq!(review_spent(&db).await, (0, 0), "owner re-run {entry}");
        }

        // A marker written while the Task sits in `review` is not an entry.
        sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,created_at,bridge_kind,bridge_payload) VALUES('m','t','review','review','user','note','2026-01-01T00:00:04Z','recovery','{\"verb\":\"retry\"}')").execute(db.pool()).await.unwrap();
        bounce("x4").await;
        assert_eq!(review_spent(&db).await, (0, 0));

        // A Review the entry did not open is not covered by it.
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        assert!(
            !owner_requested_review_entry(&mut tx, "t", None, Some("2026-01-01T00:00:00Z"))
                .await
                .unwrap()
        );
        assert!(
            owner_requested_review_entry(&mut tx, "t", None, Some("2026-01-01T00:00:03Z"))
                .await
                .unwrap()
        );
        drop(tx);

        // The same re-run asked for by the workflow itself spends, everywhere.
        log_review_entry(&db, "s1", 4, "2026-01-01T00:00:05Z", Some(workflow)).await;
        fail("r4", 4, "2026-01-01T00:00:05Z").await;
        assert_eq!(review_spent(&db).await, (1, 0), "the reviewer's verdict");
        bounce("x5").await;
        assert_eq!(review_spent(&db).await, (1, 1), "the bounce's gate debit");
        // And so does an ordinary entry.
        log_review_entry(&db, "a1", 5, "2026-01-01T00:00:06Z", None).await;
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        assert!(!owner_requested_review_entry(&mut tx, "t", None, None)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn review_two_failed_verdicts_allow_one_bounce_and_owner_finding_is_free() {
        let (db, task) = fixture().await;
        let workflow = json!({"roles":[],"states":[{"name":"review","kind":"gate","column":"Review","display_name":"Review","role":null,"hooks":{},"config":{},"gate_config":{"max_rejections":2}}]});
        sqlx::query("UPDATE project SET workflow_definition=? WHERE id='p'")
            .bind(workflow.to_string())
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES('e','t','coder','completed','now','now')").execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO review(id,task_id,execution_id,attempt_number,status,step_results_json,started_at,created_at,updated_at) VALUES('r','t','e',1,'running','{}','now','now','now')").execute(db.pool()).await.unwrap();
        let review = crate::ReviewRepo::get_by_id(&db, "r")
            .await
            .unwrap()
            .unwrap();
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        review_verdict(
            &mut tx,
            &task,
            &review,
            &ReviewStatus::Failed,
            &json!({}),
            true,
        )
        .await
        .unwrap();
        let conformance = api_types::ReviewConformance {
            status: api_types::ConformanceStatus::Failed,
            reason: Some("owner access".into()),
            assessment: Some(api_types::ReviewAssessment {
                result: api_types::ReviewResult::Fail,
                reason: "owner access".into(),
                fixable_by: api_types::FixableBy::Owner,
                repeat: false,
                findings: Vec::new(),
                resolved: Vec::new(),
                report: String::new(),
            }),
            ..Default::default()
        };
        review_verdict(
            &mut tx,
            &task,
            &review,
            &ReviewStatus::Failed,
            &json!({"conformance":conformance}),
            false,
        )
        .await
        .unwrap();
        let untouched:i64=sqlx::query_scalar("SELECT COALESCE((SELECT spent FROM task_budget WHERE task_id='t' AND kind='review'),0)").fetch_one(&mut *tx).await.unwrap();
        assert_eq!(untouched, 0);
        let mut review = review.clone();
        review.id = "r1".into();
        review.attempt_number = 2;
        sqlx::query("INSERT INTO review(id,task_id,execution_id,attempt_number,status,step_results_json,started_at,created_at,updated_at) VALUES('r1','t','e',2,'running','{}','now','now','now')").execute(&mut *tx).await.unwrap();
        review_verdict(
            &mut tx,
            &task,
            &review,
            &ReviewStatus::Failed,
            &json!({}),
            false,
        )
        .await
        .unwrap();
        let first: i64 =
            sqlx::query_scalar("SELECT spent FROM task_budget WHERE task_id='t' AND kind='review'")
                .fetch_one(&mut *tx)
                .await
                .unwrap();
        assert_eq!(remaining(2, first), 1);
        assert!(allows_retry(2, first));
        let mut second = review.clone();
        second.id = "r2".into();
        second.attempt_number = 3;
        sqlx::query("INSERT INTO review(id,task_id,execution_id,attempt_number,status,step_results_json,started_at,created_at,updated_at) VALUES('r2','t','e',3,'running','{}','now','now','now')").execute(&mut *tx).await.unwrap();
        review_verdict(
            &mut tx,
            &task,
            &second,
            &ReviewStatus::Failed,
            &json!({}),
            false,
        )
        .await
        .unwrap();
        let second_spent: i64 =
            sqlx::query_scalar("SELECT spent FROM task_budget WHERE task_id='t' AND kind='review'")
                .fetch_one(&mut *tx)
                .await
                .unwrap();
        assert_eq!(remaining(2, second_spent), 0);
        assert!(!allows_retry(2, second_spent));
        tx.commit().await.unwrap();
    }

    #[tokio::test]
    async fn exhausting_verdict_opens_one_recovery_episode() {
        let (db, task) = fixture().await;
        let key = Kind::AutomaticReviewRecovery.key();
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        let episode = "SELECT window_id,spent FROM task_budget WHERE task_id='t' AND kind='automatic_review_recovery'";
        assert!(
            charge(&mut tx, &task.id, key, 1, "attempt")
                .await
                .unwrap()
                .charged
        );
        open_recovery_episode_in_tx(&mut tx, &task.id, "r2")
            .await
            .unwrap();
        let opened: (String, i64) = sqlx::query_as(episode).fetch_one(&mut *tx).await.unwrap();
        assert_eq!(opened, ("exhaustion:r2".to_owned(), 0));
        // Re-resolving the same exhausted verdict keeps the episode's spending.
        assert!(
            charge(&mut tx, &task.id, key, 1, "recovery")
                .await
                .unwrap()
                .charged
        );
        open_recovery_episode_in_tx(&mut tx, &task.id, "r2")
            .await
            .unwrap();
        let kept: (String, i64) = sqlx::query_as(episode).fetch_one(&mut *tx).await.unwrap();
        assert_eq!(kept, ("exhaustion:r2".to_owned(), 1));
        tx.commit().await.unwrap();
    }

    #[tokio::test]
    async fn resolver_honors_task_wide_then_supplied_state_config_then_gate() {
        let (_db, mut task) = fixture().await;
        let gate = api_types::GateConfig {
            reject_target: None,
            max_rejections: Some(2),
            approve_label: None,
            reject_label: None,
            requires_user_approval: None,
            optional_when_unassigned: None,
        };
        assert_eq!(limit(&task, Kind::Review, None, Some(&gate)).unwrap(), 2);
        // A per-state value is only seen where the step supplies its merged config.
        task.task_state_config = Some(json!({"review":{"retry_budgets":{"review":4}}}).to_string());
        assert_eq!(limit(&task, Kind::Review, None, Some(&gate)).unwrap(), 2);
        let merged = json!({"retry_budgets":{"review":4}});
        assert_eq!(
            limit(&task, Kind::Review, Some(&merged), Some(&gate)).unwrap(),
            4
        );
        // The web editor's Task-wide save wins over every per-state value.
        task.task_state_config = Some(
            json!({"retry_budgets":{"review":1},"review":{"retry_budgets":{"review":4}}})
                .to_string(),
        );
        assert_eq!(
            limit(&task, Kind::Review, Some(&merged), Some(&gate)).unwrap(),
            1
        );
        assert_eq!(limit(&task, Kind::Review, None, Some(&gate)).unwrap(), 1);
    }

    #[test]
    fn ci_triage_has_no_budget_kind() {
        // The public triage mode remains a Project setting. It is never a
        // retry budget: durable check request keys bound replay instead.
        assert!(serde_json::from_str::<Kind>(r#""review_ci_triage""#).is_err());
    }

    #[test]
    fn correction_invocation_has_two_turns_and_new_invocation_restores_allowance() {
        let mut invocation = Invocation::new(Kind::ReportCorrection);
        assert_eq!(invocation.remaining(), 2);
        assert!(invocation.consume());
        assert_eq!(invocation.remaining(), 1);
        assert!(invocation.consume());
        assert!(!invocation.consume());
        assert_eq!(Invocation::new(Kind::ReportCorrection).remaining(), 2);
    }
    #[tokio::test]
    async fn budget_review_progress_resolves_and_projects_in_review_window_and_verify_is_persisted()
    {
        let (db, mut task) = fixture().await;
        let workflow = WorkflowDefinition { states: vec![api_types::StateDefinition {
            name:"review".into(), kind:api_types::StateKind::Gate, config:json!({"retry_budgets":{"review_progress":4}}),
            ..serde_json::from_value(json!({"name":"review","kind":"gate","column":"Review","display_name":"Review","hooks":{},"config":{}})).unwrap()
        }], ..Default::default() };
        let mut state = workflow.states[0].clone();
        state.config = json!({});
        let merged = state_config(&state, &json!({"retry_budgets":{"review_progress":2},"default_review_config":{"retry_budgets":{"review":7}}}).to_string());
        assert_eq!(merged["retry_budgets"]["review"], 7);
        assert_eq!(merged["retry_budgets"]["review_progress"], 2);
        let gate: GateConfig = serde_json::from_value(json!({"max_rejections":99})).unwrap();
        assert_eq!(
            limit(&task, Kind::ReviewProgress, None, Some(&gate)).unwrap(),
            2
        );
        assert_eq!(
            limit(
                &task,
                Kind::ReviewProgress,
                Some(&workflow.states[0].config),
                Some(&gate)
            )
            .unwrap(),
            4
        );
        task.task_state_config = Some(json!({"retry_budgets":{"review_progress":0}}).to_string());
        assert_eq!(
            limit(
                &task,
                Kind::ReviewProgress,
                Some(&workflow.states[0].config),
                None
            )
            .unwrap(),
            0
        );
        task.task_state_config = None;
        let projection = projection(
            &task,
            &workflow,
            &HashMap::from([("review_progress".into(), 1)]),
            0,
        )
        .unwrap();
        assert_eq!(projection.get("review_progress"), Some(&3));
        assert!(!Kind::ReviewVerifyRerun.is_volatile());
        assert!(Kind::PERSISTED.contains(&Kind::ReviewVerifyRerun));
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        assert!(
            charge(
                &mut tx,
                &task.id,
                Kind::ReviewVerifyRerun.key(),
                1,
                "verify"
            )
            .await
            .unwrap()
            .charged
        );
        assert!(
            !charge(
                &mut tx,
                &task.id,
                Kind::ReviewVerifyRerun.key(),
                1,
                "verify"
            )
            .await
            .unwrap()
            .charged
        );
        tx.commit().await.unwrap();
    }
    #[tokio::test]
    async fn budget_verify_rerun_receipts_replay_and_new_reviews_get_fresh_windows() {
        let (db, task) = fixture().await;
        let workflow: WorkflowDefinition = serde_json::from_value(json!({"roles":[],"states":[{"name":"review","kind":"gate","column":"Review","display_name":"Review","hooks":{},"config":{"retry_budgets":{"review_verify_rerun":3}}}]})).unwrap();
        sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES('e','t','coder','completed','now','now')").execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO review(id,task_id,execution_id,attempt_number,status,step_results_json,started_at,created_at,updated_at) VALUES('r1','t','e',1,'running','{}','now','now','now')").execute(db.pool()).await.unwrap();
        let config = json!({"retry_budgets":{"review_verify_rerun":1}});
        initialize_verify_budget(&db, &task, &workflow, "r1")
            .await
            .unwrap();
        assert!(grant_verify_rerun(&db, &task, &workflow, &config, "r1", 0)
            .await
            .unwrap());
        assert!(grant_verify_rerun(&db, &task, &workflow, &config, "r1", 0)
            .await
            .unwrap());
        assert_eq!(
            spent(db.pool(), &task.id, "review_verify_rerun")
                .await
                .unwrap(),
            1
        );
        initialize_verify_budget(&db, &task, &workflow, "r1")
            .await
            .unwrap();
        assert!(!grant_verify_rerun(&db, &task, &workflow, &config, "r1", 1)
            .await
            .unwrap());
        sqlx::query("UPDATE review SET status='cancelled' WHERE id='r1'")
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO review(id,task_id,execution_id,attempt_number,status,step_results_json,started_at,created_at,updated_at) VALUES('r2','t','e',2,'running','{}','now','now','now')").execute(db.pool()).await.unwrap();
        initialize_verify_budget(&db, &task, &workflow, "r2")
            .await
            .unwrap();
        assert_eq!(
            spent(db.pool(), &task.id, "review_verify_rerun")
                .await
                .unwrap(),
            0
        );
        assert!(grant_verify_rerun(&db, &task, &workflow, &config, "r2", 0)
            .await
            .unwrap());
        assert!(
            grant_verify_rerun(&db, &task, &workflow, &config, "r1", 0)
                .await
                .is_err(),
            "stale Review cannot restore its budget window"
        );
        assert_eq!(
            spent(db.pool(), &task.id, "review_verify_rerun")
                .await
                .unwrap(),
            1
        );
        let projected = projection(
            &task,
            &workflow,
            &HashMap::from([("review_verify_rerun".into(), 1)]),
            0,
        )
        .unwrap();
        assert_eq!(projected["review_verify_rerun"], 2);
    }

    #[tokio::test]
    async fn budget_verify_rerun_charges_custom_review_state_and_fences_status() {
        let (db, _) = fixture().await;
        let workflow: WorkflowDefinition = serde_json::from_value(json!({"roles":[],"states":[
            {"name":"quality_check","kind":"gate","role":"reviewer","canonical_phase":"review","column":"Review","display_name":"Quality","hooks":{},"config":{},"gate_config":{"max_rejections":2}},
            {"name":"working","kind":"active","column":"Working","display_name":"Working","hooks":{},"config":{}}
        ]})).unwrap();
        sqlx::query("UPDATE project SET workflow_definition=? WHERE id='p'")
            .bind(serde_json::to_string(&workflow).unwrap())
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE task SET status='quality_check' WHERE id='t'")
            .execute(db.pool())
            .await
            .unwrap();
        let task = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
        sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES('e','t','coder','completed','now','now')").execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO review(id,task_id,execution_id,attempt_number,status,step_results_json,started_at,created_at,updated_at) VALUES('r','t','e',1,'running','{}','now','now','now')").execute(db.pool()).await.unwrap();
        let settings =
            json!({"retry_budgets":{"review_verify_rerun":3,"review_progress":4}}).to_string();
        let workflow = with_project_defaults(&workflow, &settings);
        let config = &workflow.states[0].config;
        initialize_verify_budget(&db, &task, &workflow, "r")
            .await
            .unwrap();
        assert!(grant_verify_rerun(&db, &task, &workflow, config, "r", 0)
            .await
            .unwrap());
        assert!(grant_verify_rerun(&db, &task, &workflow, config, "r", 0)
            .await
            .unwrap());
        let counts = load(db.pool(), &task.id).await.unwrap();
        assert_eq!(counts[Kind::ReviewVerifyRerun.key()], 1);
        let projected = projection(&task, &workflow, &counts, 0).unwrap();
        assert_eq!(projected["review_verify_rerun"], 2);
        assert_eq!(projected["review_progress"], 4);
        assert_eq!(projected["quality_check"], 2);
        assert_eq!(projected["review_gate"], 1);
        assert!(!gate_entry_exhausted(
            is_review_state(&workflow.states[0]),
            0,
            0
        ));
        assert!(gate_entry_exhausted(false, 0, 0));
        let gate = workflow.states[0].gate_config.as_ref();
        assert!(cancelled_review_entry_allows_retry(&db, &task, gate)
            .await
            .unwrap());

        let owner = api_types::Actor::user(api_types::UserActionSource::Api);
        let bridge = api_types::TransitionBridge::review_rerun(&owner);
        sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,created_at,bridge_kind,bridge_payload,status_epoch) VALUES('owner-entry','t','quality_check','quality_check','system','entry','2026-01-01T00:00:00Z',?,?,1)")
            .bind(bridge.bridge_kind.map(api_types::TransitionBridgeKind::as_str))
            .bind(bridge.bridge_payload.map(|payload| payload.to_string()))
            .execute(db.pool()).await.unwrap();
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        assert!(owner_requested_review_entry(&mut tx, &task.id, None, None)
            .await
            .unwrap());
        transition(
            &mut tx,
            &task.id,
            &task.status,
            false,
            &api_types::TransitionBridge::default(),
            true,
            "owner-rejection",
            None,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(review_spent(&db).await, (0, 0));

        sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,created_at,status_epoch) VALUES('automatic-entry','t','working','quality_check','system','entry','2026-01-01T00:00:01Z',2)")
            .execute(db.pool()).await.unwrap();
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        assert!(!owner_requested_review_entry(&mut tx, &task.id, None, None)
            .await
            .unwrap());
        transition(
            &mut tx,
            &task.id,
            &task.status,
            false,
            &api_types::TransitionBridge::default(),
            true,
            "automatic-rejection",
            None,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(review_spent(&db).await, (1, 1));
        assert!(!cancelled_review_entry_allows_retry(&db, &task, gate)
            .await
            .unwrap());
        assert_eq!(gate_budget_key(&workflow.states[0]), Kind::Review.key());

        sqlx::query("UPDATE task SET status='working',version=version+1 WHERE id='t'")
            .execute(db.pool())
            .await
            .unwrap();
        assert!(matches!(
            grant_verify_rerun(&db, &task, &workflow, config, "r", 1).await,
            Err(DbError::VersionConflict)
        ));
        let task = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
        assert!(matches!(
            initialize_verify_budget(&db, &task, &workflow, "r").await,
            Err(DbError::InvalidTransition)
        ));
        assert_eq!(
            spent(db.pool(), &task.id, "review_verify_rerun")
                .await
                .unwrap(),
            1
        );
    }

    /// A reviewer-role gate outside the review phase (a design check) runs no
    /// Review, so nothing would ever park it at verdict time: it keeps its own
    /// gate budget, entry cap and configuration. The built-in name keeps the
    /// review rules whatever its kind.
    #[tokio::test]
    async fn budget_reviewer_gate_outside_the_review_phase_keeps_its_gate_budget() {
        let (db, _) = fixture().await;
        let workflow: WorkflowDefinition = serde_json::from_value(json!({"roles":[],"states":[
            {"name":"design_review","kind":"gate","role":"reviewer","canonical_phase":"working","column":"In Progress","display_name":"Design","hooks":{},"config":{},"gate_config":{"max_rejections":2}},
            {"name":"working","kind":"active","column":"Working","display_name":"Working","hooks":{},"config":{}}
        ]})).unwrap();
        sqlx::query("UPDATE project SET workflow_definition=? WHERE id='p'")
            .bind(serde_json::to_string(&workflow).unwrap())
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE task SET status='design_review' WHERE id='t'")
            .execute(db.pool())
            .await
            .unwrap();
        let task = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
        let gate = &workflow.states[0];
        assert!(!is_review_state(gate));
        assert_eq!(gate_budget_key(gate), "gate:design_review");
        assert!(gate_entry_exhausted(is_review_state(gate), 2, 2));
        let settings = json!({
            "retry_budgets":{"review_progress":4},
            "default_review_config":{"retry_budgets":{"review":7}}
        })
        .to_string();
        assert_eq!(state_config(gate, &settings), json!({}));

        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        transition(
            &mut tx,
            &task.id,
            &task.status,
            false,
            &api_types::TransitionBridge::default(),
            true,
            "design-rejection",
            None,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(review_spent(&db).await, (0, 0));
        let counts = load(db.pool(), &task.id).await.unwrap();
        assert_eq!(counts["gate:design_review"], 1);
        let projected = projection(&task, &workflow, &counts, 0).unwrap();
        assert_eq!(projected["design_review"], 1);
        assert!(!projected.contains_key("review_gate"));

        let mut review = gate.clone();
        review.name = "review".into();
        review.kind = StateKind::Active;
        assert!(is_review_state(&review));
        assert_eq!(
            state_config(&review, &settings)["retry_budgets"]["review"],
            7
        );
    }

    // D5 rule 1 is Forge's own rule: a reviewer finding on its third check
    // parks even when a required conformance check also failed (which
    // replaces the reported reason). Only owner routing (`fixable_by`) and the
    // self-reported `repeat` require the reviewer's verdict to be the cause.
    #[test]
    fn derived_repeat_parks_when_a_required_check_also_failed() {
        let row = |attempt: i64, details: Value| Review {
            id: format!("review-{attempt}"),
            task_id: "t".into(),
            execution_id: "e".into(),
            reviewer_execution_id: None,
            auditor_execution_id: None,
            attempt_number: attempt,
            status: ReviewStatus::Failed,
            step_results_json: details.to_string(),
            started_at: format!("2026-10-15T00:00:{attempt:02}Z"),
            finished_at: None,
            created_at: format!("2026-10-15T00:00:{attempt:02}Z"),
            updated_at: "now".into(),
        };
        let assessment = |findings: Value| json!({"result":"fail","reason":"problems","findings":findings,"repeat":true});
        let window = vec![
            row(
                1,
                json!({"conformance":{"status":"failed","checks":[],"reason":"problems",
                "assessment":assessment(json!([{"summary":"stuck"},{"summary":"other"}]))}}),
            ),
            row(
                2,
                json!({"conformance":{"status":"failed","checks":[],"reason":"problems",
                "assessment":assessment(json!([{"id":"F1","summary":"stuck"},{"id":"F2","summary":"other"}]))}}),
            ),
        ];
        let current = row(3, json!({}));
        for owner in [false, true] {
            let mut verdict = assessment(json!([{"id":"F1","summary":"stuck"}]));
            if owner {
                verdict["fixable_by"] = json!("owner");
            }
            let conformance: api_types::ReviewConformance = serde_json::from_value(json!({
                "status":"failed","reason":"required conformance check failed",
                "checks":[{"check_id":"lint","command":"lint","exit_code":1,"output":"bad"}],
                "assessment":verdict,
            }))
            .unwrap();
            assert_eq!(
                owner_review_failure_message(&conformance, &current, &window, &window, true)
                    .as_deref(),
                Some("repeated finding: F1 stuck"),
                "owner={owner}"
            );
            // Derived rules off: the reviewer did not cause this failure, so
            // neither its owner routing nor its self-reported repeat applies.
            assert_eq!(
                owner_review_failure_message(&conformance, &current, &window, &window, false),
                None
            );
        }
    }
    #[tokio::test]
    async fn budget_coordination_root_resolves_the_workflow_planning_role() {
        let (db, task) = fixture().await;
        let workflow: WorkflowDefinition = serde_json::from_value(json!({"states":[
            {"name":"todo","kind":"initial","column":"Todo","display_name":"Todo","hooks":{},"config":{},"triggers":{"accept":{"to":"design"}}},
            {"name":"design","kind":"gate","column":"Planning","display_name":"Planning","hooks":{},"config":{},"role":"architect","triggers":{"accept":{"to":"working"}}},
            {"name":"working","kind":"active","column":"Working","display_name":"Working","hooks":{},"config":{},"role":"builder"},
            {"name":"review","kind":"gate","column":"Review","display_name":"Review","hooks":{},"config":{},"role":"reviewer","canonical_phase":"review"}
        ], "roles":[]})).unwrap();
        assert_eq!(
            workflow
                .coordination_planning_state()
                .unwrap()
                .role
                .as_deref(),
            Some("architect")
        );
        sqlx::query("UPDATE project SET workflow_definition=? WHERE id='p'")
            .bind(serde_json::to_string(&workflow).unwrap())
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO task(id,project_id,title,status,parent_task_id,created_at,updated_at) VALUES('child','p','Child','done','t','now','now')").execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO task_role_assignment(id,task_id,role_name,assignee_type,assignee_id,created_at,updated_at) VALUES('lead','t','architect','agent','agent','now','now')").execute(db.pool()).await.unwrap();
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        assert!(coordination_root_in_tx(&mut tx, &task).await.unwrap());
        tx.rollback().await.unwrap();
        sqlx::query("UPDATE task_role_assignment SET role_name='planner' WHERE id='lead'")
            .execute(db.pool())
            .await
            .unwrap();
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        assert!(
            !coordination_root_in_tx(&mut tx, &task).await.unwrap(),
            "a planner outside the planning gate is not a lead"
        );
    }
    #[test]
    fn budget_anchoring_nonblocking_park_uses_triaged_conformance_outcome() {
        let conformance: api_types::ReviewConformance = serde_json::from_value(json!({
            "status":"failed", "checks":[{"check_id":"ci:0","command":"tests","exit_code":1,"output":"flake", "triage":{"mode":"full","base_status":"available","blocking":false,"class":"flaky","failures":[]}}],
            "reason":"separate scope", "assessment":{"result":"fail","reason":"separate scope","findings":[{"summary":"later work","kind":"out_of_scope"}]}
        })).unwrap();
        let current = Review {
            id: "r".into(),
            task_id: "t".into(),
            execution_id: "e".into(),
            reviewer_execution_id: None,
            auditor_execution_id: None,
            attempt_number: 1,
            status: ReviewStatus::Failed,
            step_results_json: json!({"conformance":conformance}).to_string(),
            started_at: "now".into(),
            finished_at: None,
            created_at: "now".into(),
            updated_at: "now".into(),
        };
        for derived_rules in [true, false] {
            assert_eq!(
                owner_review_failure_message(&conformance, &current, &[], &[], derived_rules)
                    .as_deref(),
                Some("only non-blocking findings remain")
            );
        }
    }
    #[tokio::test]
    async fn budget_coordination_root_legacy_default_agrees_with_services_planner() {
        let (db, task) = fixture().await;
        sqlx::query("INSERT INTO task(id,project_id,title,status,parent_task_id,created_at,updated_at) VALUES('child','p','Child','done','t','now','now')").execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO task_role_assignment(id,task_id,role_name,assignee_type,assignee_id,created_at,updated_at) VALUES('lead','t','planner','agent','agent','now','now')").execute(db.pool()).await.unwrap();
        for raw in ["{}", "", "not-json"] {
            sqlx::query("UPDATE project SET workflow_definition=? WHERE id='p'")
                .bind(raw)
                .execute(db.pool())
                .await
                .unwrap();
            let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
            assert!(
                coordination_root_in_tx(&mut tx, &task).await.unwrap(),
                "services resolves {raw:?} to its default planner"
            );
            tx.rollback().await.unwrap();
        }
    }
}
