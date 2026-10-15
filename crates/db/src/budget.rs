//! Task budget policy and transactional accounting. Counts never come from audit prose.
use crate::{DbError, Result, Review, ReviewStatus, SqliteDb, Task};
use api_types::{GateConfig, WorkflowDefinition};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{Row, Sqlite, Transaction};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Review,
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
    pub const PERSISTED: [Self; 10] = [
        Self::Review,
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
            Self::MergeFix | Self::AutomaticReviewRecovery | Self::RepairNoChange => 1,
            Self::GateRejection => i32::MAX,
            Self::TargetMovedRebase
            | Self::ConflictHandoff
            | Self::ReviewCarry
            | Self::ReviewCiInfrastructure
            | Self::Replan => 5,
            Self::ReportCorrection | Self::ReviewCheckRerun => 2,
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

/// The latest Review in the current window, only when it failed, and the commit
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
         WHERE r.task_id=?
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
/// (or a recorded check wait) prove that it is too late to evaluate it.
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
            let started: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM task_hook_checkpoint WHERE step_id=? AND hook_index>=?)
                 OR EXISTS(SELECT 1 FROM task_step WHERE id=? AND awaited_consumer_id IS NOT NULL)",
            )
            .bind(&step.id)
            .bind(first_entry_index)
            .bind(&step.id)
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
        Kind::Review | Kind::MergeFix | Kind::Execution | Kind::WorkflowGuard | Kind::GateRejection
    ) {
        return Ok(kind.default_limit());
    }
    if kind == Kind::GateRejection {
        let n = gate
            .and_then(|g| g.max_rejections)
            .unwrap_or(i32::MAX)
            .max(0);
        return Ok(if task.status == "review" {
            n.saturating_sub(1).max(0)
        } else {
            n
        });
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
pub fn gate_entry_exhausted(state: &str, limit: i64, spent: i64) -> bool {
    state != "review" && !allows_retry(limit, spent)
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
    if key == Kind::ReportCorrection.key() || key == Kind::ReviewCheckRerun.key() {
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
            && state.name != "review"
        {
            continue;
        }
        let kind = match state.name.as_str() {
            "review" => Kind::Review,
            _ => Kind::GateRejection,
        };
        let mut origin = task.clone();
        origin.status = state.name.clone();
        let n = limit(
            &origin,
            kind,
            Some(&state.config),
            state.gate_config.as_ref(),
        )?;
        if state.name == "review" {
            let cap = limit(
                &origin,
                Kind::GateRejection,
                Some(&state.config),
                state.gate_config.as_ref(),
            )?;
            out.insert(
                "review_gate".into(),
                remaining(i64::from(cap), *counts.get(REVIEW_GATE_KEY).unwrap_or(&0)),
            );
        }
        out.insert(
            state.name.clone(),
            remaining(
                i64::from(n),
                *counts.get(&gate_key(&state.name)).unwrap_or(&0),
            ),
        );
    }
    let current = workflow.states.iter().find(|s| s.name == task.status);
    for kind in Kind::PERSISTED
        .into_iter()
        .filter(|k| !matches!(k, Kind::Review))
    {
        let scoped = if kind == Kind::MergeFix {
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
    if *status != ReviewStatus::Failed || owner || owner_step(&task.id) {
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
        let previous_rows=sqlx::query("SELECT id,attempt_number,status,step_results_json FROM review WHERE task_id=? AND attempt_number<? ORDER BY attempt_number DESC,id DESC LIMIT 1").bind(&task.id).bind(review.attempt_number).fetch_all(&mut **tx).await?;
        let mut previous_reviews = Vec::new();
        for row in previous_rows {
            let mut previous = review.clone();
            previous.id = row.try_get("id")?;
            previous.attempt_number = row.try_get("attempt_number")?;
            previous.status = row
                .try_get::<String, _>("status")?
                .parse()
                .map_err(|e| DbError::Check(format!("invalid review status: {e}")))?;
            previous.step_results_json = row.try_get("step_results_json")?;
            previous_reviews.push(previous);
        }
        if owner_review_failure_message(&conformance, review, &previous_reviews).is_some() {
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
    if from == "review" && rejection {
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
        _ if rejection && from != "review" => Some(gate_key(from)),
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
pub fn state_config(config: &Value, state: &str, settings: &str) -> Value {
    let mut config = config.clone();
    let settings = serde_json::from_str::<Value>(settings).unwrap_or(Value::Null);
    if state == "merging" {
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
    if state == "review" {
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
    }
    config
}
pub fn with_project_defaults(workflow: &WorkflowDefinition, settings: &str) -> WorkflowDefinition {
    let mut workflow = workflow.clone();
    for state in &mut workflow.states {
        state.config = state_config(&state.config, &state.name, settings);
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
    // Only an entry logs its epoch; a recovery marker written while the Task
    // sits in `review` has none.
    let entry: Option<(String, Option<String>, Option<String>, String)> = sqlx::query_as(
        "SELECT id,bridge_kind,bridge_payload,created_at FROM transition_log
         WHERE task_id=? AND to_state='review' AND status_epoch IS NOT NULL AND id IS NOT ?
         ORDER BY status_epoch DESC,created_at DESC,rowid DESC LIMIT 1",
    )
    .bind(task)
    .bind(leaving)
    .fetch_optional(executor)
    .await?;
    Ok(entry.and_then(|(id, kind, payload, entered_at)| {
        crate::decode_transition_bridge(&id, kind.as_deref(), payload.as_deref())
            .ok()?
            .review_rerun_requester()
            .map(|requested_by| (requested_by, entered_at))
    }))
}

/// Honor routing only when the reviewer verdict caused the failure. Forge
/// replaces the assessment reason when setup, checks, or reproduction fail.
pub fn owner_review_failure_message(
    conformance: &api_types::ReviewConformance,
    review: &Review,
    reviews: &[Review],
) -> Option<String> {
    let assessment = conformance.assessment.as_ref()?;
    if conformance.status != api_types::ConformanceStatus::Failed
        || assessment.result != api_types::ReviewResult::Fail
        || conformance.checks.iter().any(|check| check.exit_code != 0)
        || conformance.reason.as_deref().unwrap_or_default() != assessment.reason
    {
        return None;
    }
    let reason = if assessment.reason.is_empty() {
        "reviewer reported a blocking finding"
    } else {
        &assessment.reason
    };
    if assessment.fixable_by == api_types::FixableBy::Owner {
        return Some(format!("fixable by owner: {reason}"));
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
                            && conformance.checks.iter().all(|check| check.exit_code == 0)
                            && conformance.assessment.as_ref().is_some_and(|assessment| {
                                assessment.result == api_types::ReviewResult::Fail
                                    && conformance.reason.as_deref()
                                        == Some(assessment.reason.as_str())
                            })
                    })
        });
    (assessment.repeat && previous_failed).then(|| format!("repeated finding: {reason}"))
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
    let cap = limit(task, Kind::GateRejection, None, gate)?;
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
    fn correction_invocation_has_two_turns_and_new_invocation_restores_allowance() {
        let mut invocation = Invocation::new(Kind::ReportCorrection);
        assert_eq!(invocation.remaining(), 2);
        assert!(invocation.consume());
        assert_eq!(invocation.remaining(), 1);
        assert!(invocation.consume());
        assert!(!invocation.consume());
        assert_eq!(Invocation::new(Kind::ReportCorrection).remaining(), 2);
    }
}
