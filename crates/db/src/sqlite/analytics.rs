use super::*;
use api_types::{
    AccountUsageAnalyticsResponse, ActivityCounts, AgentUsageBreakdown, AnalyticsWindow,
    CostCoverage, CostCoverageReason, CostCoverageReasonCode as ApiReasonCode, CostKind,
    CostSourceFreshness, CostSourceKind, CostSourceRef, CostSummary, ModelUsageBreakdown,
    MoneyAmount, ProjectUsageBreakdown, SurfaceUsageBreakdown, TokenCounters as ApiTokenCounters,
    UsageAggregate, UsageAnalytics, UsageCostCoverage, UsageSurface as ApiUsageSurface,
};
use chrono::DateTime;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    hash::Hash,
    str::FromStr,
};

#[derive(Debug, Deserialize)]
struct StepResult {
    command: String,
    exit_code: i64,
    #[serde(default)]
    started_at: Option<String>,
    #[serde(default)]
    finished_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StepResultsObject {
    #[serde(default)]
    ci_steps: Vec<StepResult>,
}

#[derive(Debug, Default)]
struct StepAggregate {
    total_runs: i64,
    pass_count: i64,
    fail_count: i64,
    duration_ms: Vec<i64>,
    last_run_at: Option<String>,
}

fn parse_step_results(step_results_json: &str) -> Vec<StepResult> {
    if let Ok(steps) = serde_json::from_str::<Vec<StepResult>>(step_results_json) {
        return steps;
    }
    if let Ok(payload) = serde_json::from_str::<StepResultsObject>(step_results_json) {
        return payload.ci_steps;
    }
    Vec::new()
}

fn parse_duration_ms(started_at: &str, finished_at: &str) -> Option<i64> {
    let started = DateTime::parse_from_rfc3339(started_at).ok()?;
    let finished = DateTime::parse_from_rfc3339(finished_at).ok()?;
    let delta_ms = finished.signed_duration_since(started).num_milliseconds();
    if delta_ms < 0 {
        return None;
    }
    Some(delta_ms)
}

async fn list_review_step_results_json(
    db: &SqliteDb,
    project_id: &str,
    from: Option<&str>,
    to: Option<&str>,
) -> Result<Vec<String>> {
    let mut query = sqlx::QueryBuilder::<Sqlite>::new(
        "SELECT r.step_results_json \
         FROM review r \
         JOIN execution e ON r.execution_id = e.id \
         JOIN task t ON e.task_id = t.id \
         WHERE t.project_id = ",
    );
    query.push_bind(project_id);
    if let Some(from) = from {
        query
            .push(" AND julianday(r.started_at) >= julianday(")
            .push_bind(from)
            .push(")");
    }
    if let Some(to) = to {
        query
            .push(" AND julianday(r.started_at) < julianday(")
            .push_bind(to)
            .push(")");
    }

    let rows = query.build().fetch_all(db.pool()).await?;
    rows.into_iter()
        .map(|row| row.try_get("step_results_json").map_err(Into::into))
        .collect()
}

mod review;
mod usage_read;

// -------------------------------------------------------------------------
// Ledger-backed usage analytics
// -------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct LedgerInvocationRow {
    id: String,
    project_id: Option<String>,
    surface: UsageSurface,
    source_id: String,
    admitted_provider_id: Option<String>,
    admitted_model_id: Option<String>,
    agent_id: Option<String>,
    profile_id: Option<String>,
    agent_name_snapshot: Option<String>,
    project_name_snapshot: Option<String>,
    executor_type: Option<String>,
    lifecycle: UsageInvocationLifecycle,
    telemetry_state: UsageTelemetryState,
    admitted_at: String,
    settled_at: Option<String>,
}

#[derive(Debug, Clone)]
struct LedgerEventRow {
    invocation_id: String,
    project_id: Option<String>,
    provider_id: Option<String>,
    model_id: Option<String>,
    runtime_model: Option<String>,
    agent_id: Option<String>,
    profile_id: Option<String>,
    agent_name_snapshot: Option<String>,
    project_name_snapshot: Option<String>,
    executor_type: Option<String>,
    counters: [Option<i64>; 4],
    provider_reported_nano_usd: Option<i64>,
    legacy_reported_cost_text: Option<String>,
    estimated_nano_usd: Option<i64>,
    cost_kind: UsageCostKind,
    provenance_kind: UsageEventProvenanceKind,
    rate_revision_id: Option<String>,
    catalog_snapshot_id: Option<String>,
    formula_revision: Option<String>,
    retrospective: bool,
    coverage_reason_code: Option<CostCoverageReasonCode>,
    occurred_at: String,
    rate_source_kind: Option<String>,
    rate_effective_at: Option<String>,
    rate_input: Option<i64>,
    rate_output: Option<i64>,
    rate_cache_read: Option<i64>,
    rate_cache_write: Option<i64>,
    catalog_digest: Option<String>,
    catalog_fetched_at: Option<String>,
    catalog_freshness: Option<String>,
}

#[derive(Debug, Clone)]
struct LedgerInvocationView {
    row: LedgerInvocationRow,
    events: Vec<LedgerEventRow>,
}

#[derive(Debug, Clone)]
struct DomainRun {
    surface: UsageSurface,
    source_id: String,
    project_id: Option<String>,
}

/// Immutable proof that an assistant response belongs to a handed-off
/// Product Genesis session.  The source aliases cover the message id and the
/// durable turn/source ids used by the historical and runtime chat writers.
#[derive(Debug, Clone)]
struct GenesisAttribution {
    message_id: String,
    source_id: Option<String>,
    source_message_id: Option<String>,
    source_turn_job_id: Option<String>,
    project_id: String,
    occurred_at: String,
}

/// Immutable Genesis handoff boundary. A control-transfer turn can have no
/// assistant message at all, so message-only attribution cannot count its
/// unmetered invocation or map its NULL-at-admission Project ID. The durable
/// delivery receipt is the occurrence boundary; unlike a turn's mutable
/// `updated_at`, its `created_at` is immutable.
#[derive(Debug, Clone)]
struct GenesisBoundary {
    project_id: String,
    source_turn_job_id: Option<String>,
    source_turn_occurred_at: Option<String>,
    source_message_ids: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct UsageDataset {
    invocations: Vec<LedgerInvocationView>,
    domain_runs: Vec<DomainRun>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RunKey {
    surface: UsageSurface,
    source_id: String,
}

impl std::hash::Hash for RunKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        surface_order(self.surface).hash(state);
        self.source_id.hash(state);
    }
}

impl Ord for RunKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        surface_order(self.surface)
            .cmp(&surface_order(other.surface))
            .then_with(|| self.source_id.cmp(&other.source_id))
    }
}

impl PartialOrd for RunKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AgentGroupKey {
    agent_id: Option<String>,
    agent_name_snapshot: Option<String>,
    profile_id: Option<String>,
    executor_type: Option<String>,
}

impl Ord for AgentGroupKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.agent_id
            .cmp(&other.agent_id)
            .then_with(|| self.agent_name_snapshot.cmp(&other.agent_name_snapshot))
            .then_with(|| self.profile_id.cmp(&other.profile_id))
            .then_with(|| self.executor_type.cmp(&other.executor_type))
    }
}

impl PartialOrd for AgentGroupKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn surface_order(surface: UsageSurface) -> usize {
    match surface {
        UsageSurface::TaskExecution => 0,
        UsageSurface::ProjectChat => 1,
        UsageSurface::MainChat => 2,
        UsageSurface::GenesisChat => 3,
        UsageSurface::MainInquiry => 4,
    }
}

#[derive(Debug, Clone)]
enum GroupFilter {
    All,
    Surface(UsageSurface),
    Model(Option<String>, Option<String>),
    Agent(AgentGroupKey),
    Project(String),
}

#[derive(Debug, Clone, Default)]
struct AttemptSummary {
    costed: bool,
    metered: bool,
    pending: bool,
    unsettled: bool,
    reasons: Vec<ApiReasonCode>,
}

#[derive(Debug, Clone, Default)]
struct ReasonData {
    run_keys: std::collections::HashSet<RunKey>,
    invocation_ids: std::collections::HashSet<String>,
    tokens: [i64; 4],
}

#[derive(Debug, Clone, Default)]
struct CostAccumulator {
    provider_reported_nano_usd: Option<i128>,
    estimated_nano_usd: Option<i128>,
    tokens: [i64; 4],
    priced_tokens: [i64; 4],
    unpriced_tokens: [i64; 4],
    total_provider_attempts: i64,
    settled_provider_attempts: i64,
    pending_provider_attempts: i64,
    unsettled_provider_attempts: i64,
    metered_provider_attempts: i64,
    unmetered_provider_attempts: i64,
    costed_provider_attempts: i64,
    unpriced_provider_attempts: i64,
    costed_event_count: i64,
    unpriced_event_count: i64,
    sources: BTreeMap<String, CostSourceRef>,
    reasons: BTreeMap<String, ReasonData>,
}

impl CostAccumulator {
    fn add_amount(slot: &mut Option<i128>, amount: i128) -> Result<()> {
        if amount < 0 {
            return Err(DbError::InvalidTransition);
        }
        // `None` represents that no amount-bearing event has been seen yet;
        // using zero only as the additive identity preserves that presence
        // distinction because the slot is set to `Some` immediately.
        let current = (*slot).unwrap_or_default();
        *slot = Some(
            current
                .checked_add(amount)
                .ok_or(DbError::InvalidTransition)?,
        );
        Ok(())
    }

    fn add_counter(slot: &mut i64, value: Option<i64>) -> Result<()> {
        if let Some(value) = value {
            if value < 0 {
                return Err(DbError::InvalidTransition);
            }
            *slot = slot.checked_add(value).ok_or(DbError::InvalidTransition)?;
        }
        Ok(())
    }

    fn add_tokens(&mut self, counters: [Option<i64>; 4]) -> Result<()> {
        for (slot, value) in self.tokens.iter_mut().zip(counters) {
            Self::add_counter(slot, value)?;
        }
        Ok(())
    }

    fn add_reason(
        &mut self,
        code: ApiReasonCode,
        run_key: &RunKey,
        invocation_id: &str,
        tokens: [i64; 4],
    ) -> Result<()> {
        let key = reason_key(code);
        let reason = self.reasons.entry(key).or_default();
        reason.run_keys.insert(run_key.clone());
        reason.invocation_ids.insert(invocation_id.to_owned());
        for (slot, value) in reason.tokens.iter_mut().zip(tokens) {
            *slot = slot.checked_add(value).ok_or(DbError::InvalidTransition)?;
        }
        Ok(())
    }
}

fn reason_key(code: ApiReasonCode) -> String {
    match code {
        ApiReasonCode::Pending => "pending",
        ApiReasonCode::Unsettled => "unsettled",
        ApiReasonCode::Unmetered => "unmetered",
        ApiReasonCode::MissingProvider => "missing_provider",
        ApiReasonCode::MissingModel => "missing_model",
        ApiReasonCode::MissingBinding => "missing_binding",
        ApiReasonCode::MissingRate => "missing_rate",
        ApiReasonCode::UnresolvedTier => "unresolved_tier",
        ApiReasonCode::IdentityMismatch => "identity_mismatch",
        ApiReasonCode::InvalidLegacyUsage => "invalid_legacy_usage",
    }
    .to_owned()
}

fn reason_from_key(value: &str) -> Option<ApiReasonCode> {
    Some(match value {
        "pending" => ApiReasonCode::Pending,
        "unsettled" => ApiReasonCode::Unsettled,
        "unmetered" => ApiReasonCode::Unmetered,
        "missing_provider" => ApiReasonCode::MissingProvider,
        "missing_model" => ApiReasonCode::MissingModel,
        "missing_binding" => ApiReasonCode::MissingBinding,
        "missing_rate" => ApiReasonCode::MissingRate,
        "unresolved_tier" => ApiReasonCode::UnresolvedTier,
        "identity_mismatch" => ApiReasonCode::IdentityMismatch,
        "invalid_legacy_usage" => ApiReasonCode::InvalidLegacyUsage,
        _ => return None,
    })
}

fn api_reason_code(value: CostCoverageReasonCode) -> ApiReasonCode {
    match value {
        CostCoverageReasonCode::Pending => ApiReasonCode::Pending,
        CostCoverageReasonCode::Unsettled => ApiReasonCode::Unsettled,
        CostCoverageReasonCode::Unmetered => ApiReasonCode::Unmetered,
        CostCoverageReasonCode::MissingProvider => ApiReasonCode::MissingProvider,
        CostCoverageReasonCode::MissingModel => ApiReasonCode::MissingModel,
        CostCoverageReasonCode::MissingBinding => ApiReasonCode::MissingBinding,
        CostCoverageReasonCode::MissingRate => ApiReasonCode::MissingRate,
        CostCoverageReasonCode::UnresolvedTier => ApiReasonCode::UnresolvedTier,
        CostCoverageReasonCode::IdentityMismatch => ApiReasonCode::IdentityMismatch,
        CostCoverageReasonCode::InvalidLegacyUsage => ApiReasonCode::InvalidLegacyUsage,
    }
}

fn reason_order(code: ApiReasonCode) -> usize {
    match code {
        ApiReasonCode::Pending => 0,
        ApiReasonCode::Unsettled => 1,
        ApiReasonCode::Unmetered => 2,
        ApiReasonCode::MissingProvider => 3,
        ApiReasonCode::MissingModel => 4,
        ApiReasonCode::MissingBinding => 5,
        ApiReasonCode::MissingRate => 6,
        ApiReasonCode::UnresolvedTier => 7,
        ApiReasonCode::IdentityMismatch => 8,
        ApiReasonCode::InvalidLegacyUsage => 9,
    }
}

fn api_surface(surface: UsageSurface) -> ApiUsageSurface {
    match surface {
        UsageSurface::TaskExecution => ApiUsageSurface::TaskExecution,
        UsageSurface::ProjectChat => ApiUsageSurface::ProjectChat,
        UsageSurface::MainChat => ApiUsageSurface::MainChat,
        UsageSurface::GenesisChat => ApiUsageSurface::GenesisChat,
        UsageSurface::MainInquiry => ApiUsageSurface::MainInquiry,
    }
}

fn api_tokens(counters: [i64; 4]) -> ApiTokenCounters {
    ApiTokenCounters {
        input_tokens: counters[0],
        output_tokens: counters[1],
        cache_read_tokens: counters[2],
        cache_write_tokens: counters[3],
    }
}

fn run_key_for(invocation: &LedgerInvocationView) -> RunKey {
    RunKey {
        surface: invocation.row.surface,
        source_id: invocation.row.source_id.clone(),
    }
}

fn event_counters(event: &LedgerEventRow) -> Result<[i64; 4]> {
    // Aggregate wire counters are numeric totals.  Converting an absent
    // nullable bucket to zero here is only a summation convenience; telemetry
    // evidence and metered/unmetered coverage continue to use the invocation
    // state and never infer evidence from this value.
    let counters = event.counters.map(|value| value.unwrap_or(0));
    if counters.iter().any(|value| *value < 0) {
        return Err(DbError::InvalidTransition);
    }
    Ok(counters)
}

fn invocation_model(invocation: &LedgerInvocationView) -> (Option<String>, Option<String>) {
    invocation
        .events
        .first()
        .map(|event| {
            (
                event
                    .provider_id
                    .clone()
                    .or_else(|| invocation.row.admitted_provider_id.clone()),
                event
                    .model_id
                    .clone()
                    .or_else(|| event.runtime_model.clone())
                    .or_else(|| invocation.row.admitted_model_id.clone()),
            )
        })
        .unwrap_or_else(|| {
            (
                invocation.row.admitted_provider_id.clone(),
                invocation.row.admitted_model_id.clone(),
            )
        })
}

fn invocation_agent(invocation: &LedgerInvocationView) -> AgentGroupKey {
    invocation.events.first().map_or_else(
        || AgentGroupKey {
            agent_id: invocation.row.agent_id.clone(),
            agent_name_snapshot: invocation.row.agent_name_snapshot.clone(),
            profile_id: invocation.row.profile_id.clone(),
            executor_type: invocation.row.executor_type.clone(),
        },
        |event| AgentGroupKey {
            agent_id: event
                .agent_id
                .clone()
                .or_else(|| invocation.row.agent_id.clone()),
            agent_name_snapshot: event
                .agent_name_snapshot
                .clone()
                .or_else(|| invocation.row.agent_name_snapshot.clone()),
            profile_id: event
                .profile_id
                .clone()
                .or_else(|| invocation.row.profile_id.clone()),
            executor_type: event
                .executor_type
                .clone()
                .or_else(|| invocation.row.executor_type.clone()),
        },
    )
}

fn invocation_matches(invocation: &LedgerInvocationView, filter: &GroupFilter) -> bool {
    match filter {
        GroupFilter::All => true,
        GroupFilter::Surface(surface) => invocation.row.surface == *surface,
        GroupFilter::Model(provider, model) => {
            let (invocation_provider, invocation_model) = invocation_model(invocation);
            invocation_provider == *provider && invocation_model == *model
        }
        GroupFilter::Agent(agent) => invocation_agent(invocation) == *agent,
        GroupFilter::Project(project_id) => {
            invocation.row.project_id.as_deref() == Some(project_id)
        }
    }
}

fn parse_money_decimal(input: &str) -> Option<i128> {
    let input = input.trim().trim_matches('\'');
    if input.is_empty() || input.eq_ignore_ascii_case("null") || input.starts_with('-') {
        return None;
    }
    let (mantissa, exponent) = match input.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i32>().ok()?),
        None => (input, 0_i32),
    };
    let (whole, fraction) = mantissa
        .split_once('.')
        .map_or((mantissa, ""), |(w, f)| (w, f));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let digits = format!("{whole}{fraction}");
    let digits = digits.trim_start_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    let integer = digits.parse::<i128>().ok()?;
    let scale = i32::try_from(fraction.len()).ok()?.checked_sub(exponent)?;
    if scale <= 0 {
        let multiplier = 10_i128.checked_pow(scale.unsigned_abs())?;
        return integer.checked_mul(multiplier)?.checked_mul(1_000_000_000);
    }
    let denominator = 10_i128.checked_pow(u32::try_from(scale).ok()?)?;
    let numerator = integer.checked_mul(1_000_000_000)?;
    let quotient = numerator / denominator;
    let remainder = numerator % denominator;
    // Round half away from zero without multiplying the remainder: the
    // latter can overflow for otherwise valid, very large decimal inputs.
    let round_up =
        remainder > denominator / 2 || (denominator % 2 == 0 && remainder == denominator / 2);
    let rounded = quotient.checked_add(i128::from(round_up))?;
    Some(rounded)
}

fn event_reported_amount(event: &LedgerEventRow) -> Option<i128> {
    event
        .provider_reported_nano_usd
        .map(i128::from)
        .or_else(|| {
            event
                .legacy_reported_cost_text
                .as_deref()
                .and_then(parse_money_decimal)
        })
}

fn event_unpriced_reason(event: &LedgerEventRow) -> ApiReasonCode {
    if let Some(reason) = event.coverage_reason_code.map(api_reason_code) {
        return reason;
    }
    // A legacy REAL provider amount is authoritative, but its original
    // lexical representation may still be unconvertible to fixed-point. Do
    // not mislabel that preserved data as a missing rate.
    if event.cost_kind == UsageCostKind::ProviderReported
        && event_reported_amount(event).is_none()
        && !matches!(
            event.provenance_kind,
            UsageEventProvenanceKind::RuntimeReport
        )
    {
        ApiReasonCode::InvalidLegacyUsage
    } else {
        ApiReasonCode::MissingRate
    }
}

fn event_source(event: &LedgerEventRow) -> Option<(String, CostSourceRef)> {
    if event_reported_amount(event).is_some() {
        let legacy = !matches!(
            event.provenance_kind,
            UsageEventProvenanceKind::RuntimeReport
        );
        let source_kind = if legacy {
            CostSourceKind::LegacyProviderReported
        } else {
            CostSourceKind::ProviderReported
        };
        let source = CostSourceRef {
            source_kind,
            rate_revision_id: None,
            catalog_snapshot_id: None,
            catalog_digest: None,
            effective_at: None,
            fetched_at: None,
            freshness: CostSourceFreshness::NotApplicable,
            retrospective: false,
            formula_revision: None,
        };
        return Some((
            format!("reported:{}", reason_key_for_source(source_kind)),
            source,
        ));
    }
    event.estimated_nano_usd?;
    let source_kind = match event.rate_source_kind.as_deref() {
        Some("manual_override") => CostSourceKind::ManualOverride,
        Some("models_dev_catalog") => CostSourceKind::ModelsDevCatalog,
        _ => return None,
    };
    let source = CostSourceRef {
        source_kind,
        rate_revision_id: event.rate_revision_id.clone(),
        catalog_snapshot_id: event.catalog_snapshot_id.clone(),
        catalog_digest: event.catalog_digest.clone(),
        effective_at: event.rate_effective_at.clone(),
        fetched_at: event.catalog_fetched_at.clone(),
        freshness: if source_kind == CostSourceKind::ModelsDevCatalog {
            match event.catalog_freshness.as_deref() {
                Some("fresh") => CostSourceFreshness::Fresh,
                Some("stale") => CostSourceFreshness::Stale,
                Some("refresh_failed") => CostSourceFreshness::RefreshFailed,
                Some("not_applicable") => CostSourceFreshness::NotApplicable,
                // Historical rows with an absent/unknown freshness must not
                // be presented as fresh provenance.  Preserve the usable
                // estimate while conservatively marking its source as a
                // refresh failure.
                _ => CostSourceFreshness::RefreshFailed,
            }
        } else {
            CostSourceFreshness::NotApplicable
        },
        retrospective: event.retrospective,
        formula_revision: event.formula_revision.clone(),
    };
    let key = format!(
        "estimated:{}:{}:{}",
        reason_key_for_source(source_kind),
        event.rate_revision_id.as_deref().unwrap_or_default(),
        event.catalog_snapshot_id.as_deref().unwrap_or_default()
    );
    Some((key, source))
}

fn reason_key_for_source(source_kind: CostSourceKind) -> &'static str {
    match source_kind {
        CostSourceKind::ProviderReported => "provider_reported",
        CostSourceKind::LegacyProviderReported => "legacy_provider_reported",
        CostSourceKind::ModelsDevCatalog => "models_dev_catalog",
        CostSourceKind::ManualOverride => "manual_override",
    }
}

fn event_accounting(
    event: &LedgerEventRow,
    accumulator: &mut CostAccumulator,
    run_key: &RunKey,
    blocked_reason: Option<ApiReasonCode>,
) -> Result<bool> {
    accumulator.add_tokens(event.counters)?;
    let counters = event_counters(event)?;
    if let Some(reason) = blocked_reason {
        accumulator.unpriced_event_count = accumulator
            .unpriced_event_count
            .checked_add(1)
            .ok_or(DbError::InvalidTransition)?;
        for (slot, value) in accumulator.unpriced_tokens.iter_mut().zip(counters) {
            *slot = slot.checked_add(value).ok_or(DbError::InvalidTransition)?;
        }
        accumulator.add_reason(reason, run_key, &event.invocation_id, counters)?;
        return Ok(false);
    }

    // A provider-reported amount wins for its event.  An estimate attached to
    // the same row (for example by a later retrospective revision) must not be
    // added on top of that reported amount.  Applied retrospective revisions
    // are exposed by the query as an effective `estimated` cost kind.
    let reported = (event.cost_kind == UsageCostKind::ProviderReported)
        .then(|| event_reported_amount(event))
        .flatten();
    let estimated = (event.cost_kind == UsageCostKind::Estimated)
        .then(|| event.estimated_nano_usd.map(i128::from))
        .flatten();
    let costed = reported.is_some() || estimated.is_some();

    if let Some(amount) = reported {
        CostAccumulator::add_amount(&mut accumulator.provider_reported_nano_usd, amount)?;
    } else if let Some(amount) = estimated {
        CostAccumulator::add_amount(&mut accumulator.estimated_nano_usd, amount)?;
    }
    if costed {
        accumulator.costed_event_count = accumulator
            .costed_event_count
            .checked_add(1)
            .ok_or(DbError::InvalidTransition)?;
        for (slot, value) in accumulator.priced_tokens.iter_mut().zip(counters) {
            *slot = slot.checked_add(value).ok_or(DbError::InvalidTransition)?;
        }
        if let Some((key, source)) = event_source(event) {
            accumulator.sources.insert(key, source);
        }
        return Ok(true);
    }

    accumulator.unpriced_event_count = accumulator
        .unpriced_event_count
        .checked_add(1)
        .ok_or(DbError::InvalidTransition)?;
    let reason = event_unpriced_reason(event);
    let rates = [
        event.rate_input,
        event.rate_output,
        event.rate_cache_read,
        event.rate_cache_write,
    ];
    let mut reason_tokens = [0_i64; 4];
    let reason_blocks_all_buckets = matches!(
        reason,
        ApiReasonCode::Unmetered
            | ApiReasonCode::MissingProvider
            | ApiReasonCode::MissingModel
            | ApiReasonCode::MissingBinding
            | ApiReasonCode::IdentityMismatch
            | ApiReasonCode::InvalidLegacyUsage
    );
    for (index, value) in counters.into_iter().enumerate() {
        let bucket_unpriced = reason_blocks_all_buckets
            || reason == ApiReasonCode::UnresolvedTier
            || rates[index].is_none();
        let target = if bucket_unpriced {
            &mut accumulator.unpriced_tokens[index]
        } else {
            &mut accumulator.priced_tokens[index]
        };
        *target = target
            .checked_add(value)
            .ok_or(DbError::InvalidTransition)?;
        if bucket_unpriced {
            reason_tokens[index] = value;
        }
    }
    accumulator.add_reason(reason, run_key, &event.invocation_id, reason_tokens)?;
    Ok(false)
}

fn attempt_summary(
    invocation: &LedgerInvocationView,
    accumulator: &mut CostAccumulator,
    run_key: &RunKey,
) -> Result<AttemptSummary> {
    let pending = !matches!(
        invocation.row.lifecycle,
        UsageInvocationLifecycle::Settled | UsageInvocationLifecycle::Unsettled
    );
    let unsettled = invocation.row.lifecycle == UsageInvocationLifecycle::Unsettled;
    let metered = invocation.row.telemetry_state == UsageTelemetryState::Metered;
    let mut summary = AttemptSummary {
        costed: !invocation.events.is_empty(),
        metered,
        pending,
        unsettled,
        reasons: Vec::new(),
    };
    accumulator.total_provider_attempts = accumulator
        .total_provider_attempts
        .checked_add(1)
        .ok_or(DbError::InvalidTransition)?;
    if pending {
        accumulator.pending_provider_attempts = accumulator
            .pending_provider_attempts
            .checked_add(1)
            .ok_or(DbError::InvalidTransition)?;
        let reason = ApiReasonCode::Pending;
        summary.reasons.push(reason);
        accumulator.add_reason(reason, run_key, &invocation.row.id, [0; 4])?;
    } else if unsettled {
        accumulator.unsettled_provider_attempts = accumulator
            .unsettled_provider_attempts
            .checked_add(1)
            .ok_or(DbError::InvalidTransition)?;
        let reason = ApiReasonCode::Unsettled;
        summary.reasons.push(reason);
        accumulator.add_reason(reason, run_key, &invocation.row.id, [0; 4])?;
    } else {
        accumulator.settled_provider_attempts = accumulator
            .settled_provider_attempts
            .checked_add(1)
            .ok_or(DbError::InvalidTransition)?;
    }
    if metered {
        accumulator.metered_provider_attempts = accumulator
            .metered_provider_attempts
            .checked_add(1)
            .ok_or(DbError::InvalidTransition)?;
    } else if !pending && !unsettled {
        accumulator.unmetered_provider_attempts = accumulator
            .unmetered_provider_attempts
            .checked_add(1)
            .ok_or(DbError::InvalidTransition)?;
        // Unmetered is an independent telemetry dimension.  It must remain
        // visible even when the provider supplied a reportable money amount;
        // the amount can make the attempt costed without proving counters.
        let reason = ApiReasonCode::Unmetered;
        summary.reasons.push(reason);
        accumulator.add_reason(reason, run_key, &invocation.row.id, [0; 4])?;
    }

    if invocation.events.is_empty() {
        summary.costed = false;
        accumulator.unpriced_provider_attempts = accumulator
            .unpriced_provider_attempts
            .checked_add(1)
            .ok_or(DbError::InvalidTransition)?;
        return Ok(summary);
    }

    summary.costed = true;
    let blocked_reason = if pending {
        Some(ApiReasonCode::Pending)
    } else if unsettled {
        Some(ApiReasonCode::Unsettled)
    } else {
        None
    };
    for event in &invocation.events {
        if !event_accounting(event, accumulator, run_key, blocked_reason)? {
            summary.costed = false;
            if blocked_reason.is_none() {
                let reason = event_unpriced_reason(event);
                summary.reasons.push(reason);
            }
        }
    }
    if summary.costed {
        accumulator.costed_provider_attempts = accumulator
            .costed_provider_attempts
            .checked_add(1)
            .ok_or(DbError::InvalidTransition)?;
    } else {
        accumulator.unpriced_provider_attempts = accumulator
            .unpriced_provider_attempts
            .checked_add(1)
            .ok_or(DbError::InvalidTransition)?;
    }
    Ok(summary)
}

fn source_refs(accumulator: &CostAccumulator) -> Vec<CostSourceRef> {
    let mut sources: Vec<_> = accumulator.sources.values().cloned().collect();
    sources.sort_by(|left, right| {
        source_order(left.source_kind)
            .cmp(&source_order(right.source_kind))
            .then_with(|| left.rate_revision_id.cmp(&right.rate_revision_id))
            .then_with(|| left.catalog_snapshot_id.cmp(&right.catalog_snapshot_id))
            .then_with(|| left.effective_at.cmp(&right.effective_at))
    });
    sources
}

fn source_order(source_kind: CostSourceKind) -> usize {
    match source_kind {
        CostSourceKind::ProviderReported => 0,
        CostSourceKind::LegacyProviderReported => 1,
        CostSourceKind::ModelsDevCatalog => 2,
        CostSourceKind::ManualOverride => 3,
    }
}

fn money(nanos: Option<i128>) -> Option<MoneyAmount> {
    let nanos = nanos?;
    if nanos < 0 {
        return None;
    }
    let whole = nanos / 1_000_000_000;
    let fraction = nanos % 1_000_000_000;
    let decimal = if fraction == 0 {
        whole.to_string()
    } else {
        let mut fraction = format!("{fraction:09}");
        while fraction.ends_with('0') {
            fraction.pop();
        }
        format!("{whole}.{fraction}")
    };
    Some(MoneyAmount {
        currency: "USD".to_owned(),
        decimal,
    })
}

fn cost_summary(
    accumulator: CostAccumulator,
    run_states: &BTreeMap<RunKey, Vec<AttemptSummary>>,
) -> Result<CostSummary> {
    let mut pending_runs = 0_i64;
    let mut no_provider_call_runs = 0_i64;
    let mut fully_metered_runs = 0_i64;
    let mut fully_costed_runs = 0_i64;
    let mut partially_costed_runs = 0_i64;
    let mut unavailable_cost_runs = 0_i64;

    let mut reasons = accumulator.reasons.clone();
    for (run_key, attempts) in run_states {
        if attempts.is_empty() {
            no_provider_call_runs = no_provider_call_runs
                .checked_add(1)
                .ok_or(DbError::InvalidTransition)?;
            continue;
        }
        let has_pending = attempts.iter().any(|attempt| attempt.pending);
        let has_unsettled = attempts.iter().any(|attempt| attempt.unsettled);
        let costed = attempts.iter().filter(|attempt| attempt.costed).count();
        let all_metered = attempts.iter().all(|attempt| attempt.metered);
        if has_pending {
            pending_runs = pending_runs
                .checked_add(1)
                .ok_or(DbError::InvalidTransition)?;
        } else if !has_unsettled && all_metered {
            fully_metered_runs = fully_metered_runs
                .checked_add(1)
                .ok_or(DbError::InvalidTransition)?;
        }
        if has_pending {
            // Pending has precedence over every settled status.
        } else if costed == attempts.len() && !has_unsettled {
            fully_costed_runs = fully_costed_runs
                .checked_add(1)
                .ok_or(DbError::InvalidTransition)?;
        } else if costed > 0 {
            partially_costed_runs = partially_costed_runs
                .checked_add(1)
                .ok_or(DbError::InvalidTransition)?;
        } else {
            unavailable_cost_runs = unavailable_cost_runs
                .checked_add(1)
                .ok_or(DbError::InvalidTransition)?;
        }

        for attempt in attempts {
            for reason in &attempt.reasons {
                let reason = reasons.entry(reason_key(*reason)).or_default();
                reason.run_keys.insert(run_key.clone());
            }
        }
    }

    let coverage = if accumulator.total_provider_attempts == 0 {
        CostCoverage::NoUsage
    } else if accumulator.pending_provider_attempts > 0 {
        CostCoverage::Pending
    } else if accumulator.unsettled_provider_attempts > 0 {
        if accumulator.costed_provider_attempts > 0 {
            CostCoverage::Partial
        } else {
            CostCoverage::Unavailable
        }
    } else if accumulator.costed_provider_attempts == accumulator.total_provider_attempts {
        CostCoverage::Complete
    } else if accumulator.costed_provider_attempts > 0 {
        CostCoverage::Partial
    } else {
        CostCoverage::Unavailable
    };

    let kind = if accumulator.provider_reported_nano_usd.is_some()
        && accumulator.estimated_nano_usd.is_some()
    {
        CostKind::Mixed
    } else if accumulator.provider_reported_nano_usd.is_some() {
        CostKind::ProviderReported
    } else if accumulator.estimated_nano_usd.is_some() {
        CostKind::Estimated
    } else if accumulator.total_provider_attempts == 0
        || (coverage == CostCoverage::Pending && accumulator.costed_provider_attempts == 0)
    {
        CostKind::None
    } else {
        CostKind::Unknown
    };

    // `None` means no amount-bearing event exists; `Some(0)` is an explicit
    // known zero and must remain distinguishable from that absence.
    let known_total = match (
        accumulator.provider_reported_nano_usd,
        accumulator.estimated_nano_usd,
    ) {
        (Some(reported), Some(estimated)) => Some(
            reported
                .checked_add(estimated)
                .ok_or(DbError::InvalidTransition)?,
        ),
        (Some(reported), None) => Some(reported),
        (None, Some(estimated)) => Some(estimated),
        (None, None) => None,
    };
    let complete_total = (coverage == CostCoverage::Complete)
        .then_some(known_total)
        .flatten();
    let total_runs = i64::try_from(run_states.len()).map_err(|_| DbError::InvalidTransition)?;
    let mut reason_items: Vec<_> = reasons
        .into_iter()
        .map(|(key, reason)| -> Result<_> {
            let code = reason_from_key(&key).ok_or(DbError::InvalidTransition)?;
            Ok((
                reason_order(code),
                CostCoverageReason {
                    code,
                    run_or_turn_count: i64::try_from(reason.run_keys.len())
                        .map_err(|_| DbError::InvalidTransition)?,
                    provider_attempt_count: i64::try_from(reason.invocation_ids.len())
                        .map_err(|_| DbError::InvalidTransition)?,
                    tokens: api_tokens(reason.tokens),
                },
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    reason_items.sort_by_key(|(order, _)| *order);

    Ok(CostSummary {
        kind,
        coverage,
        provider_reported: money(accumulator.provider_reported_nano_usd),
        estimated: money(accumulator.estimated_nano_usd),
        known_subtotal: (accumulator.provider_reported_nano_usd.is_some()
            || accumulator.estimated_nano_usd.is_some())
        .then(|| money(known_total))
        .flatten(),
        complete_total: money(complete_total),
        usage_coverage: UsageCostCoverage {
            total_runs_or_turns: total_runs,
            pending_runs_or_turns: pending_runs,
            no_provider_call_runs_or_turns: no_provider_call_runs,
            fully_metered_runs_or_turns: fully_metered_runs,
            fully_costed_runs_or_turns: fully_costed_runs,
            partially_costed_runs_or_turns: partially_costed_runs,
            unavailable_cost_runs_or_turns: unavailable_cost_runs,
            total_provider_attempts: accumulator.total_provider_attempts,
            settled_provider_attempts: accumulator.settled_provider_attempts,
            pending_provider_attempts: accumulator.pending_provider_attempts,
            unsettled_provider_attempts: accumulator.unsettled_provider_attempts,
            metered_provider_attempts: accumulator.metered_provider_attempts,
            unmetered_provider_attempts: accumulator.unmetered_provider_attempts,
            costed_provider_attempts: accumulator.costed_provider_attempts,
            unpriced_provider_attempts: accumulator.unpriced_provider_attempts,
            priced_tokens: api_tokens(accumulator.priced_tokens),
            unpriced_tokens: api_tokens(accumulator.unpriced_tokens),
            reasons: reason_items.into_iter().map(|(_, reason)| reason).collect(),
        },
        sources: source_refs(&accumulator),
    })
}

fn group_has_invocation(filter: &GroupFilter, invocation: &LedgerInvocationView) -> bool {
    invocation_matches(invocation, filter)
}

fn aggregate_for(
    invocations: &[LedgerInvocationView],
    domain_runs: &[DomainRun],
    filter: &GroupFilter,
) -> Result<UsageAggregate> {
    let selected: Vec<&LedgerInvocationView> = invocations
        .iter()
        .filter(|invocation| group_has_invocation(filter, invocation))
        .collect();
    let selected_source_ids: std::collections::HashSet<RunKey> = selected
        .iter()
        .map(|invocation| run_key_for(invocation))
        .collect();
    let mut run_states: BTreeMap<RunKey, Vec<AttemptSummary>> = BTreeMap::new();
    let mut accumulator = CostAccumulator::default();
    let mut counts = ActivityCounts {
        task_execution_count: 0,
        chat_turn_count: 0,
        inquiry_count: 0,
        provider_attempt_count: 0,
    };
    let mut tokens = [0_i64; 4];

    for invocation in selected {
        let run_key = run_key_for(invocation);
        let summary = attempt_summary(invocation, &mut accumulator, &run_key)?;
        run_states.entry(run_key).or_default().push(summary);
        counts.provider_attempt_count = counts
            .provider_attempt_count
            .checked_add(1)
            .ok_or(DbError::InvalidTransition)?;
        for event in &invocation.events {
            for (slot, value) in tokens.iter_mut().zip(event_counters(event)?) {
                *slot = slot.checked_add(value).ok_or(DbError::InvalidTransition)?;
            }
        }
    }

    for domain_run in domain_runs {
        let run_key = RunKey {
            surface: domain_run.surface,
            source_id: domain_run.source_id.clone(),
        };
        let filter_matches = match filter {
            GroupFilter::All => true,
            GroupFilter::Surface(surface) => domain_run.surface == *surface,
            GroupFilter::Project(project_id) => {
                domain_run.project_id.as_deref() == Some(project_id)
            }
            GroupFilter::Model(_, _) | GroupFilter::Agent(_) => {
                selected_source_ids.contains(&run_key)
            }
        };
        if filter_matches {
            run_states.entry(run_key).or_default();
        }
    }

    for key in run_states.keys() {
        match key.surface {
            UsageSurface::TaskExecution => {
                counts.task_execution_count = counts
                    .task_execution_count
                    .checked_add(1)
                    .ok_or(DbError::InvalidTransition)?;
            }
            UsageSurface::ProjectChat | UsageSurface::MainChat | UsageSurface::GenesisChat => {
                counts.chat_turn_count = counts
                    .chat_turn_count
                    .checked_add(1)
                    .ok_or(DbError::InvalidTransition)?;
            }
            UsageSurface::MainInquiry => {
                counts.inquiry_count = counts
                    .inquiry_count
                    .checked_add(1)
                    .ok_or(DbError::InvalidTransition)?;
            }
        }
    }
    let cost = cost_summary(accumulator, &run_states)?;
    Ok(UsageAggregate {
        counts,
        tokens: api_tokens(tokens),
        cost,
    })
}

fn usage_analytics(dataset: UsageDataset) -> Result<UsageAnalytics> {
    let all = aggregate_for(
        &dataset.invocations,
        &dataset.domain_runs,
        &GroupFilter::All,
    )?;
    let mut surfaces = Vec::new();
    for surface in [
        UsageSurface::TaskExecution,
        UsageSurface::ProjectChat,
        UsageSurface::MainChat,
        UsageSurface::GenesisChat,
        UsageSurface::MainInquiry,
    ] {
        let aggregate = aggregate_for(
            &dataset.invocations,
            &dataset.domain_runs,
            &GroupFilter::Surface(surface),
        )?;
        if aggregate.counts.task_execution_count > 0
            || aggregate.counts.chat_turn_count > 0
            || aggregate.counts.inquiry_count > 0
            || aggregate.counts.provider_attempt_count > 0
        {
            surfaces.push(SurfaceUsageBreakdown {
                surface: api_surface(surface),
                counts: aggregate.counts,
                tokens: aggregate.tokens,
                cost: aggregate.cost,
            });
        }
    }

    let mut model_keys = BTreeMap::<(Option<String>, Option<String>), ()>::new();
    let mut agent_keys = BTreeMap::<AgentGroupKey, ()>::new();
    for invocation in &dataset.invocations {
        model_keys.insert(invocation_model(invocation), ());
        agent_keys.insert(invocation_agent(invocation), ());
    }
    let mut by_model = Vec::new();
    for ((provider_id, model_id), _) in model_keys {
        let aggregate = aggregate_for(
            &dataset.invocations,
            &dataset.domain_runs,
            &GroupFilter::Model(provider_id.clone(), model_id.clone()),
        )?;
        by_model.push(ModelUsageBreakdown {
            provider_id,
            model_id,
            counts: aggregate.counts,
            tokens: aggregate.tokens,
            cost: aggregate.cost,
        });
    }
    let mut by_agent = Vec::new();
    for agent in agent_keys.into_keys() {
        let aggregate = aggregate_for(
            &dataset.invocations,
            &dataset.domain_runs,
            &GroupFilter::Agent(agent.clone()),
        )?;
        by_agent.push(AgentUsageBreakdown {
            agent_id: agent.agent_id,
            agent_name_snapshot: agent.agent_name_snapshot,
            profile_id: agent.profile_id,
            executor_type: agent.executor_type,
            counts: aggregate.counts,
            tokens: aggregate.tokens,
            cost: aggregate.cost,
        });
    }

    Ok(UsageAnalytics {
        counts: all.counts,
        tokens: all.tokens,
        cost: all.cost,
        by_surface: surfaces,
        by_model,
        by_agent,
    })
}

fn account_usage_analytics(
    dataset: UsageDataset,
    from: Option<&str>,
    to: Option<&str>,
) -> Result<AccountUsageAnalyticsResponse> {
    let token_usage = usage_analytics(dataset.clone())?;
    // Account `by_project` follows immutable Project attribution, not the
    // presence of a usage event. A successful unmetered Genesis handoff has no
    // usage event by contract, but its handoff boundary still proves the
    // Project and must appear exactly once in this grouping. Domain runs are
    // included as well so a Project activity that stopped before any provider
    // admission remains visible in the grouping's no-provider denominator.
    let project_invocations: Vec<LedgerInvocationView> = dataset
        .invocations
        .iter()
        .filter(|invocation| invocation.row.project_id.is_some())
        .cloned()
        .collect();
    let mut projects = BTreeMap::<String, Option<String>>::new();
    for invocation in &project_invocations {
        let Some(project_id) = invocation.row.project_id.as_ref() else {
            continue;
        };
        let name = invocation.row.project_name_snapshot.clone().or_else(|| {
            invocation
                .events
                .iter()
                .find_map(|event| event.project_name_snapshot.clone())
        });
        projects
            .entry(project_id.clone())
            .and_modify(|current| {
                if current.is_none() {
                    *current = name.clone();
                }
            })
            .or_insert(name);
    }
    for run in &dataset.domain_runs {
        let Some(project_id) = run.project_id.as_ref() else {
            continue;
        };
        projects.entry(project_id.clone()).or_insert(None);
    }

    let mut by_project = Vec::with_capacity(projects.len());
    for (project_id, project_name_snapshot) in projects {
        let aggregate = aggregate_for(
            &project_invocations,
            &dataset.domain_runs,
            &GroupFilter::Project(project_id.clone()),
        )?;
        by_project.push(ProjectUsageBreakdown {
            project_id: Some(project_id),
            project_name_snapshot,
            counts: aggregate.counts,
            tokens: aggregate.tokens,
            cost: aggregate.cost,
        });
    }

    Ok(AccountUsageAnalyticsResponse {
        window: AnalyticsWindow {
            from: from.map(str::to_owned),
            to: to.map(str::to_owned),
        },
        token_usage,
        by_project,
    })
}

fn parse_timestamp(value: &str) -> Option<DateTime<chrono::FixedOffset>> {
    DateTime::parse_from_rfc3339(value).ok()
}

fn validate_window(from: Option<&str>, to: Option<&str>) -> Result<()> {
    let from = from
        .map(|value| parse_timestamp(value).ok_or(DbError::InvalidTransition))
        .transpose()?;
    let to = to
        .map(|value| parse_timestamp(value).ok_or(DbError::InvalidTransition))
        .transpose()?;
    if let (Some(from), Some(to)) = (from, to) {
        if from >= to {
            return Err(DbError::InvalidTransition);
        }
    }
    Ok(())
}

/// SQL callers widen each bound by one second before this exact check. SQLite
/// `julianday` is a floating-point prefilter and can otherwise round a
/// sub-millisecond RFC3339 value onto a boundary; the Rust comparison below
/// is the final half-open `[from, to)` authority.
fn timestamp_in_window(value: &str, from: Option<&str>, to: Option<&str>) -> bool {
    let Some(value) = parse_timestamp(value) else {
        // An invalid historical occurrence cannot be assigned to a bounded
        // instant window, but an open-ended report must still expose the
        // preserved legacy row so its `invalid_legacy_usage` coverage is not
        // silently erased by parsing.
        return from.is_none() && to.is_none();
    };
    if let Some(from) = from.and_then(parse_timestamp) {
        if value < from {
            return false;
        }
    }
    if let Some(to) = to.and_then(parse_timestamp) {
        if value >= to {
            return false;
        }
    }
    true
}

fn parse_db_enum<T: FromStr<Err = String>>(value: String) -> Result<T> {
    value.parse().map_err(|_| DbError::InvalidTransition)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod ledger_analytics_tests;
