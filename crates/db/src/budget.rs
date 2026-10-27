//! Task budget policy and transactional accounting. Counts never come from audit prose.
use crate::task_condition::channels as channel;
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

mod repair_no_change;
pub use repair_no_change::{
    failed_review_for_no_change, settle_repair_no_change, start_repair_no_change,
};
mod ledger;
pub use ledger::{
    admit, apply, charge, entry_charge_step, fresh_entry, grant_verify_rerun,
    initialize_verify_budget, load, load_many, receipt_recorded, reset, reset_all, spent,
};
mod review;
pub(crate) use ledger::migrate_pending_mutations;
pub use review::{
    cancelled_review_entry_allows_retry, exhausted_review_barrier, failed_review_entry,
    grant_review_progress, matches_spent, open_recovery_episode, owner_requested_review_entry,
    reconcile_failed_review, review_rerun_entry, review_verdict, transition,
};

#[cfg(test)]
mod condition_tests;
#[cfg(test)]
mod tests;
