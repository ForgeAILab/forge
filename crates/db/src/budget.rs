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
}
impl Kind {
    pub const PERSISTED: [Self; 9] = [
        Self::Review,
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
        }
    }
    pub const fn default_limit(self) -> i32 {
        match self {
            Self::Review | Self::Execution | Self::WorkflowGuard => 3,
            Self::MergeFix | Self::AutomaticReviewRecovery => 1,
            Self::GateRejection => i32::MAX,
            Self::TargetMovedRebase
            | Self::ConflictHandoff
            | Self::ReviewCarry
            | Self::ReviewCiInfrastructure => 5,
            Self::ReportCorrection => 2,
        }
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
/// Explicit Task per-state settings precede Task-wide settings; state configuration
/// is the resolved workflow/Project default supplied by the consuming step.
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
    let key = if kind == Kind::WorkflowGuard {
        "execution"
    } else {
        kind.key()
    };
    let state = match kind {
        Kind::Review => "review",
        Kind::MergeFix => {
            if task.status == "merge_failed" {
                "merge_failed"
            } else {
                "merging"
            }
        }
        _ => task.status.as_str(),
    };
    let config = task
        .task_state_config
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok());
    if let Some(n) = config
        .as_ref()
        .and_then(|c| c.get(state))
        .and_then(|c| configured(c, key))
    {
        return Ok(n);
    }
    if let Some(n) = config.as_ref().and_then(|c| configured(c, key)) {
        return Ok(n);
    }
    // Explicit state retry settings are existing overrides, including Project merging policy.
    if let Some(n) = state_config.and_then(|c| configured(c, key)) {
        return Ok(n);
    }
    if matches!(kind, Kind::Review | Kind::MergeFix | Kind::GateRejection) {
        if let Some(n) = gate.and_then(|g| g.max_rejections) {
            return Ok(n.max(0));
        }
    }
    Ok(kind.default_limit())
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
pub async fn load(pool: &sqlx::SqlitePool, task: &str) -> Result<HashMap<String, i64>> {
    let rows = sqlx::query("SELECT kind, spent FROM task_budget WHERE task_id = ?")
        .bind(task)
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|r| Ok((r.try_get("kind")?, r.try_get("spent")?)))
        .collect()
}
pub async fn reset(
    tx: &mut Transaction<'_, Sqlite>,
    task: &str,
    key: &str,
    window: &str,
) -> Result<()> {
    sqlx::query("INSERT INTO task_budget(task_id,kind,window_id,spent) VALUES(?,?,?,0) ON CONFLICT(task_id,kind) DO UPDATE SET window_id=excluded.window_id, spent=0 WHERE task_budget.window_id != excluded.window_id")
        .bind(task).bind(key).bind(window).execute(&mut **tx).await?;
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
    Ok(())
}
/// This is the only persisted charge point. Its receipt and consuming write share
/// the caller's authority-fenced transaction. Step identities include effect ordinals.
pub async fn charge(
    tx: &mut Transaction<'_, Sqlite>,
    task: &str,
    key: &str,
    limit: i64,
    step: &str,
) -> Result<Charge> {
    charge_consumption(tx, task, key, limit, step, false).await
}
async fn charge_consumption(
    tx: &mut Transaction<'_, Sqlite>,
    task: &str,
    key: &str,
    limit: i64,
    step: &str,
    admitted_outcome: bool,
) -> Result<Charge> {
    if key == Kind::ReportCorrection.key() {
        return Err(DbError::Check(
            "report corrections are invocation-scoped".into(),
        ));
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
    if recorded || (!admitted_outcome && !allows_retry(limit, old)) {
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
/// Native result-block corrections are deliberately scoped to one invocation.
pub struct Invocation {
    kind: Kind,
    spent: i64,
}
impl Invocation {
    pub fn new(kind: Kind) -> Self {
        Self { kind, spent: 0 }
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
                remaining(i64::from(cap), *counts.get("gate:review").unwrap_or(&0)),
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
    Ok(out)
}

/// Preserve the reviewer finding policy at the charge point, including recovery.
pub async fn review_verdict(
    tx: &mut Transaction<'_, Sqlite>,
    db: &SqliteDb,
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
    let (project, settings): (String, String) =
        sqlx::query_as("SELECT workflow_definition,settings FROM project WHERE id=?")
            .bind(&task.project_id)
            .fetch_one(&mut **tx)
            .await?;
    let workflow = serde_json::from_str::<WorkflowDefinition>(&project)
        .ok()
        .map(|w| with_project_defaults(&w, &settings));
    let state = workflow
        .as_ref()
        .and_then(|w| w.states.iter().find(|s| s.name == task.status));
    let n = limit(
        task,
        Kind::Review,
        state.map(|s| &s.config),
        state.and_then(|s| s.gate_config.as_ref()),
    )?;
    let result = charge_consumption(
        tx,
        &task.id,
        Kind::Review.key(),
        i64::from(n),
        &format!("review:{}", review.id),
        true,
    )
    .await?;
    if !allows_retry(i64::from(n), result.spent) {
        reset(
            tx,
            &task.id,
            Kind::AutomaticReviewRecovery.key(),
            &format!("exhaustion:{}", review.id),
        )
        .await?;
    }
    let _ = db;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn transition(
    tx: &mut Transaction<'_, Sqlite>,
    task: &Task,
    from: &str,
    actor: &str,
    bridge: &api_types::TransitionBridge,
    rejection: bool,
    step: &str,
) -> Result<()> {
    if bridge.bridge_kind == Some(api_types::TransitionBridgeKind::RetryWindowReset) {
        return reset_all(tx, &task.id, step).await;
    }
    if actor.starts_with("user") {
        return Ok(());
    }
    if from == "review" && rejection {
        let latest:Option<(String,String)>=sqlx::query_as("SELECT id,status FROM review WHERE task_id=? ORDER BY attempt_number DESC,id DESC LIMIT 1").bind(&task.id).fetch_optional(&mut **tx).await?;
        // Failed verdict settlement is the charge point for failures. This branch
        // retains only the already-existing gate-outcome debit (or a gate with no Review).
        if latest.as_ref().is_none_or(|(_, status)| status != "failed") {
            let (definition, settings): (String, String) =
                sqlx::query_as("SELECT workflow_definition,settings FROM project WHERE id=?")
                    .bind(&task.project_id)
                    .fetch_one(&mut **tx)
                    .await?;
            let workflow = serde_json::from_str::<WorkflowDefinition>(&definition)
                .ok()
                .map(|w| with_project_defaults(&w, &settings));
            let state = workflow
                .as_ref()
                .and_then(|w| w.states.iter().find(|s| s.name == from));
            let n = limit(
                task,
                Kind::Review,
                state.map(|s| &s.config),
                state.and_then(|s| s.gate_config.as_ref()),
            )?;
            charge_consumption(tx, &task.id, Kind::Review.key(), i64::from(n), step, true).await?;
        }
        let (definition, settings): (String, String) =
            sqlx::query_as("SELECT workflow_definition,settings FROM project WHERE id=?")
                .bind(&task.project_id)
                .fetch_one(&mut **tx)
                .await?;
        let workflow = serde_json::from_str::<WorkflowDefinition>(&definition)
            .ok()
            .map(|w| with_project_defaults(&w, &settings));
        let state = workflow
            .as_ref()
            .and_then(|w| w.states.iter().find(|s| s.name == "review"));
        let cap = limit(
            task,
            Kind::GateRejection,
            state.map(|s| &s.config),
            state.and_then(|s| s.gate_config.as_ref()),
        )?;
        charge_consumption(tx, &task.id, "gate:review", i64::from(cap), step, true).await?;
        return Ok(());
    }
    let kind = match bridge.bridge_kind {
        Some(api_types::TransitionBridgeKind::TargetMovedRebase) => Some(Kind::TargetMovedRebase),
        Some(api_types::TransitionBridgeKind::ConflictHandoff) => Some(Kind::ConflictHandoff),
        _ if rejection && from == "merging" && !bridge.is_review_refresh() => Some(Kind::MergeFix),
        _ if rejection && from != "review" => Some(Kind::GateRejection),
        _ => None,
    };
    if let Some(kind) = kind {
        let (project, settings): (String, String) =
            sqlx::query_as("SELECT workflow_definition,settings FROM project WHERE id=?")
                .bind(&task.project_id)
                .fetch_one(&mut **tx)
                .await?;
        let workflow = serde_json::from_str::<WorkflowDefinition>(&project)
            .ok()
            .map(|w| with_project_defaults(&w, &settings));
        let state = workflow
            .as_ref()
            .and_then(|w| w.states.iter().find(|s| s.name == from));
        let mut origin = task.clone();
        origin.status = from.into();
        let n = limit(
            &origin,
            kind,
            state.map(|s| &s.config),
            state.and_then(|s| s.gate_config.as_ref()),
        )?;
        let key = if kind == Kind::GateRejection {
            gate_key(from)
        } else {
            kind.key().into()
        };
        // The graph/status step already admitted this outcome. Keep the old
        // entry-hook ordering; retry admission checks occur before producing it.
        charge_consumption(tx, &task.id, &key, i64::from(n), step, true).await?;
        if kind == Kind::MergeFix {
            let gate_limit = state
                .and_then(|s| s.gate_config.as_ref())
                .and_then(|g| g.max_rejections)
                .map(i64::from)
                .unwrap_or(i64::from(i32::MAX));
            charge_consumption(tx, &task.id, &gate_key(from), gate_limit, step, true).await?;
        }
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
pub async fn task_limit(
    db: &SqliteDb,
    task: &Task,
    kind: Kind,
    config: Option<&Value>,
    gate: Option<&GateConfig>,
) -> Result<i32> {
    let settings: String = sqlx::query_scalar("SELECT settings FROM project WHERE id=?")
        .bind(&task.project_id)
        .fetch_one(db.pool())
        .await?;
    let state = match kind {
        Kind::Review => "review",
        Kind::MergeFix => {
            if task.status == "merge_failed" {
                "merge_failed"
            } else {
                "merging"
            }
        }
        _ => task.status.as_str(),
    };
    let config = state_config(config.unwrap_or(&Value::Null), state, &settings);
    limit(task, kind, Some(&config), gate)
}

fn owner_step(task: &str) -> bool {
    crate::task_writer::current_task_step()
        .filter(|s| s.task_id == task)
        .and_then(|s| serde_json::from_str::<Value>(&s.payload_json).ok())
        .is_some_and(|p| {
            p.get("actor").is_some_and(|a| a.get("User").is_some())
                || p.pointer("/request/actor")
                    .is_some_and(|a| a.get("User").is_some())
                || p.get("operation").and_then(Value::as_str) == Some("rerun_review")
        })
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

/// A failed review-entry hook with no Review verdict consumes the same failed
/// attempt. Its receipt and either barrier or retry disposition commit together.
pub async fn failed_review_entry(
    db: &SqliteDb,
    task: &Task,
    limit: i64,
    step: &str,
    started_at: &str,
    owner: bool,
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
    let allowed = if owner {
        true
    } else {
        let charged = charge(&mut tx, &task.id, Kind::Review.key(), limit, step).await?;
        allows_retry(limit, charged.spent)
    };
    let barrier=(!allowed).then(||serde_json::json!({"state":task.status,"status":"blocked","started_at":started_at,"updated_at":crate::now_rfc3339(),"blocking_reason":"review retry budget exhausted"}).to_string());
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
    tx.commit().await?;
    Ok((updated, allowed))
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
    review_verdict(
        &mut tx,
        db,
        &current,
        review,
        &review.status,
        &details,
        owner,
    )
    .await?;
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
pub(crate) async fn migrate_pending_mutations(
    tx: &mut Transaction<'_, Sqlite>,
    pool: &sqlx::SqlitePool,
) -> Result<()> {
    let db = SqliteDb::new(pool.clone());
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
        let task = db
            .get_task_in_tx(tx, &task_id)
            .await?
            .ok_or(DbError::NotFound)?;
        let (definition, settings): (String, String) =
            sqlx::query_as("SELECT workflow_definition,settings FROM project WHERE id=?")
                .bind(&task.project_id)
                .fetch_one(&mut **tx)
                .await?;
        let workflow = serde_json::from_str::<WorkflowDefinition>(&definition)
            .ok()
            .map(|w| with_project_defaults(&w, &settings));
        let state = workflow
            .as_ref()
            .and_then(|w| w.states.iter().find(|s| s.name == task.status));
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
                    crate::TaskMetadataMutation::Budget(Mutation::Charge {
                        key: kind.key().into(),
                        limit: i64::from(limit(
                            &task,
                            kind,
                            state.map(|s| &s.config),
                            state.and_then(|s| s.gate_config.as_ref()),
                        )?),
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
        spent(db.pool(), &task.id, "gate:review").await?,
    ))
}

/// Raising per-state precedence must not reduce a retained Task's allowance.
/// Normalize conflicting previously-shadowed cells to the old winning Task
/// value, retaining the complete original configuration as immutable provenance.
pub(crate) async fn normalize_retained_policy(tx: &mut Transaction<'_, Sqlite>) -> Result<()> {
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT id,task_state_config FROM task WHERE task_state_config IS NOT NULL")
            .fetch_all(&mut **tx)
            .await?;
    for (id, raw) in rows {
        let Ok(mut config) = serde_json::from_str::<Value>(&raw) else {
            continue;
        };
        let winners = configured_winners(&config);
        let Some(states) = config.as_object_mut() else {
            continue;
        };
        let mut changed = false;
        for (name, state) in states.iter_mut() {
            if name == "retry_budgets" {
                continue;
            }
            for (key, value) in &winners {
                if (*key == "review" && name != "review")
                    || (*key == "merge_fix" && !matches!(name.as_str(), "merging" | "merge_failed"))
                {
                    continue;
                }
                if configured(state, key).is_some_and(|current| current != *value) {
                    state["retry_budgets"][*key] = Value::from(*value);
                    changed = true;
                }
            }
        }
        if changed {
            sqlx::query(
                "INSERT INTO task_budget_policy_snapshot(task_id,task_state_config) VALUES(?,?)",
            )
            .bind(&id)
            .bind(&raw)
            .execute(&mut **tx)
            .await?;
            sqlx::query("UPDATE task SET task_state_config=? WHERE id=?")
                .bind(config.to_string())
                .bind(id)
                .execute(&mut **tx)
                .await?;
        }
    }
    Ok(())
}
fn configured_winners(config: &Value) -> Vec<(&'static str, i32)> {
    ["review", "merge_fix", "execution"]
        .into_iter()
        .filter_map(|key| configured(config, key).map(|n| (key, n)))
        .collect()
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
                &task,
                state,
                "user:send_back",
                &api_types::TransitionBridge::new(api_types::TransitionBridgeKind::GateRejected),
                true,
                state,
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
            &task,
            "review",
            "user:retry",
            &api_types::TransitionBridge::recovery("retry", true),
            false,
            "reset",
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
            &db,
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
                report: String::new(),
            }),
            ..Default::default()
        };
        review_verdict(
            &mut tx,
            &db,
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
            &db,
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
            &db,
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
        let episode:String=sqlx::query_scalar("SELECT window_id FROM task_budget WHERE task_id='t' AND kind='automatic_review_recovery'").fetch_one(&mut *tx).await.unwrap();
        assert_eq!(episode, "exhaustion:r2");
        tx.commit().await.unwrap();
    }

    #[tokio::test]
    async fn resolver_honors_state_then_task_then_workflow_and_gate() {
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
        task.task_state_config = Some(
            json!({"retry_budgets":{"review":5},"review":{"retry_budgets":{"review":4}}})
                .to_string(),
        );
        assert_eq!(limit(&task, Kind::Review, None, Some(&gate)).unwrap(), 4);
        task.task_state_config = Some(json!({"retry_budgets":{"review":5}}).to_string());
        assert_eq!(limit(&task, Kind::Review, None, Some(&gate)).unwrap(), 5);
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
