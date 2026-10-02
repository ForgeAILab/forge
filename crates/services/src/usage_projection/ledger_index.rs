//! Rebuildable observation index. No ledger rows or accounting decisions live here.
//! Integer totals/reasons are additive; run classifications depend on attempt
//! counts; provenance is an ordered set (last event wins), not an additive sum.
use super::*;
use sqlx::Row;
use std::sync::Arc;

const REASONS: usize = 10;
const MAX_INDEX_BYTES: usize = 128 * 1024 * 1024;
const REASON_CODES: [CostCoverageReasonCode; REASONS] = [
    CostCoverageReasonCode::Pending,
    CostCoverageReasonCode::Unsettled,
    CostCoverageReasonCode::Unmetered,
    CostCoverageReasonCode::MissingProvider,
    CostCoverageReasonCode::MissingModel,
    CostCoverageReasonCode::MissingBinding,
    CostCoverageReasonCode::MissingRate,
    CostCoverageReasonCode::UnresolvedTier,
    CostCoverageReasonCode::IdentityMismatch,
    CostCoverageReasonCode::InvalidLegacyUsage,
];
type Scope = Option<Arc<str>>;
type ScopedRun = (Scope, Arc<RunKey>);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Watermarks {
    invocation_rowid: i64,
    event_rowid: i64,
    estimate_rowid: i64,
    execution_rowid: i64,
    invocation_revision: i64,
    execution_revision: i64,
    deletion_generation: i64,
}
impl Watermarks {
    async fn read(connection: &mut sqlx::SqliteConnection) -> Result<Self> {
        let r = sqlx::query("SELECT * FROM usage_ledger_revision WHERE id = 1")
            .fetch_one(connection)
            .await?;
        Ok(Self {
            invocation_rowid: r.try_get("invocation_rowid")?,
            event_rowid: r.try_get("event_rowid")?,
            estimate_rowid: r.try_get("estimate_rowid")?,
            execution_rowid: r.try_get("execution_rowid")?,
            invocation_revision: r.try_get("invocation_revision")?,
            execution_revision: r.try_get("execution_revision")?,
            deletion_generation: r.try_get("deletion_generation")?,
        })
    }
}

#[derive(Debug, Clone, Default)]
struct Amount {
    nanos: i128,
    present: i64,
}
impl Amount {
    fn change(&mut self, other: &Self, sign: i64) -> Result<()> {
        self.nanos = self
            .nanos
            .checked_add(
                other
                    .nanos
                    .checked_mul(i128::from(sign))
                    .ok_or_else(invalid_transition)?,
            )
            .ok_or_else(invalid_transition)?;
        change(&mut self.present, other.present, sign)?;
        if self.nanos < 0 {
            return Err(invalid_transition());
        }
        Ok(())
    }
    fn value(&self) -> Option<i128> {
        (self.present > 0).then_some(self.nanos)
    }
}
fn change(slot: &mut i64, amount: i64, sign: i64) -> Result<()> {
    *slot = slot
        .checked_add(amount.checked_mul(sign).ok_or_else(invalid_transition)?)
        .ok_or_else(invalid_transition)?;
    if *slot < 0 {
        return Err(invalid_transition());
    }
    Ok(())
}
fn array_change<const N: usize>(slots: &mut [i64; N], values: &[i64; N], sign: i64) -> Result<()> {
    for (s, v) in slots.iter_mut().zip(values) {
        change(s, *v, sign)?;
    }
    Ok(())
}
#[derive(Debug, Clone, Copy, Default)]
struct Reason {
    runs: i64,
    attempts: i64,
    tokens: [i64; 4],
}
#[derive(Debug, Clone, Default)]
struct Totals {
    counts: [i64; 4],
    tokens: [i64; 4],
    coverage: [i64; 23],
    active_domains: i64,
    reported: Amount,
    estimated: Amount,
    reasons: [Reason; REASONS],
}
impl Totals {
    fn change(&mut self, other: &Self, sign: i64) -> Result<()> {
        array_change(&mut self.counts, &other.counts, sign)?;
        array_change(&mut self.tokens, &other.tokens, sign)?;
        array_change(&mut self.coverage, &other.coverage, sign)?;
        change(&mut self.active_domains, other.active_domains, sign)?;
        self.reported.change(&other.reported, sign)?;
        self.estimated.change(&other.estimated, sign)?;
        for (s, v) in self.reasons.iter_mut().zip(other.reasons) {
            change(&mut s.runs, v.runs, sign)?;
            change(&mut s.attempts, v.attempts, sign)?;
            array_change(&mut s.tokens, &v.tokens, sign)?;
        }
        Ok(())
    }
    fn public(&self, sources: Vec<CostSourceRef>) -> Result<UsageAggregate> {
        let c = &self.coverage;
        let coverage = if self.active_domains > 0 && c[1] > 0 {
            CostCoverage::Pending
        } else if c[7] == 0 {
            CostCoverage::NoUsage
        } else if c[9] > 0 {
            CostCoverage::Pending
        } else if c[10] > 0 {
            if c[13] > 0 {
                CostCoverage::Partial
            } else {
                CostCoverage::Unavailable
            }
        } else if c[13] == c[7] {
            CostCoverage::Complete
        } else if c[13] > 0 {
            CostCoverage::Partial
        } else {
            CostCoverage::Unavailable
        };
        let reported = self.reported.value();
        let estimated = self.estimated.value();
        let known = reported
            .unwrap_or(0)
            .checked_add(estimated.unwrap_or(0))
            .ok_or_else(invalid_transition)?;
        Ok(UsageAggregate {
            counts: ActivityCounts {
                task_execution_count: self.counts[0],
                chat_turn_count: self.counts[1],
                inquiry_count: self.counts[2],
                provider_attempt_count: self.counts[3],
            },
            tokens: token_counters(self.tokens),
            cost: CostSummary {
                kind: cost_kind(
                    reported.is_some(),
                    estimated.is_some(),
                    c[7],
                    coverage,
                    c[13],
                ),
                coverage,
                provider_reported: money(reported),
                estimated: money(estimated),
                known_subtotal: money((reported.is_some() || estimated.is_some()).then_some(known)),
                complete_total: money((coverage == CostCoverage::Complete).then_some(known)),
                usage_coverage: UsageCostCoverage {
                    total_runs_or_turns: c[0],
                    pending_runs_or_turns: c[1],
                    no_provider_call_runs_or_turns: c[2],
                    fully_metered_runs_or_turns: c[3],
                    fully_costed_runs_or_turns: c[4],
                    partially_costed_runs_or_turns: c[5],
                    unavailable_cost_runs_or_turns: c[6],
                    total_provider_attempts: c[7],
                    settled_provider_attempts: c[8],
                    pending_provider_attempts: c[9],
                    unsettled_provider_attempts: c[10],
                    metered_provider_attempts: c[11],
                    unmetered_provider_attempts: c[12],
                    costed_provider_attempts: c[13],
                    unpriced_provider_attempts: c[14],
                    priced_tokens: token_counters(c[15..19].try_into().expect("four tokens")),
                    unpriced_tokens: token_counters(c[19..23].try_into().expect("four tokens")),
                    reasons: self
                        .reasons
                        .iter()
                        .enumerate()
                        .filter(|(_, r)| r.attempts > 0)
                        .map(|(i, r)| CostCoverageReason {
                            code: REASON_CODES[i],
                            run_or_turn_count: r.runs,
                            provider_attempt_count: r.attempts,
                            tokens: token_counters(r.tokens),
                        })
                        .collect(),
                },
                sources,
            },
        })
    }
}

#[derive(Debug, Clone, Default)]
struct Events {
    count: i64,
    known: i64,
    tokens: [i64; 4],
    priced: [i64; 4],
    reported: Amount,
    estimated: Amount,
    unpriced: Vec<(usize, Reason)>,
    all_reasons: Vec<(usize, Reason)>,
}
impl Events {
    fn change(&mut self, e: &EffectiveUsageEvent, sign: i64) -> Result<()> {
        let tokens = event_counter_array(&e.event)?;
        let (reported, estimated) = event_amounts(&e.event)?;
        change(&mut self.count, 1, sign)?;
        array_change(&mut self.tokens, &tokens, sign)?;
        self.reported.change(
            &Amount {
                nanos: reported.unwrap_or(0),
                present: i64::from(reported.is_some()),
            },
            sign,
        )?;
        self.estimated.change(
            &Amount {
                nanos: estimated.unwrap_or(0),
                present: i64::from(estimated.is_some()),
            },
            sign,
        )?;
        let known = reported.is_some() || estimated.is_some();
        change(&mut self.known, i64::from(known), sign)?;
        if known {
            array_change(&mut self.priced, &tokens, sign)?;
        }
        let reason = e.event.coverage_reason_code.map(api_reason).unwrap_or(
            if e.event.telemetry_state == DbUsageTelemetryState::Unmetered {
                CostCoverageReasonCode::Unmetered
            } else {
                CostCoverageReasonCode::MissingRate
            },
        );
        let n = reason_order(reason);
        event_reason_change(&mut self.all_reasons, n, tokens, sign)?;
        if !known {
            event_reason_change(&mut self.unpriced, n, tokens, sign)?;
        }
        Ok(())
    }
    fn heap_bytes(&self) -> usize {
        (self.unpriced.capacity() + self.all_reasons.capacity())
            * std::mem::size_of::<(usize, Reason)>()
            + 64
    }
    fn attempt(&self, life: UsageInvocationLifecycle, telemetry: DbUsageTelemetryState) -> Totals {
        let pending = !matches!(
            life,
            UsageInvocationLifecycle::Settled | UsageInvocationLifecycle::Unsettled
        );
        let unsettled = life == UsageInvocationLifecycle::Unsettled;
        let metered = telemetry == DbUsageTelemetryState::Metered;
        let costed = self.count > 0 && self.count == self.known && !unsettled;
        let mut result = Totals {
            reported: self.reported.clone(),
            estimated: self.estimated.clone(),
            tokens: self.tokens,
            ..Totals::default()
        };
        result.counts[3] = 1;
        result.coverage[7..15].copy_from_slice(&[
            1,
            i64::from(!pending && !unsettled),
            i64::from(pending),
            i64::from(unsettled),
            i64::from(metered),
            i64::from(!metered && !pending && !unsettled),
            i64::from(costed),
            i64::from(!costed),
        ]);
        let priced = if unsettled { [0; 4] } else { self.priced };
        result.coverage[15..19].copy_from_slice(&priced);
        for (slot, (total, priced)) in result.coverage[19..23]
            .iter_mut()
            .zip(self.tokens.iter().zip(priced))
        {
            *slot = total - priced;
        }
        let reasons = if unsettled {
            &self.all_reasons
        } else {
            &self.unpriced
        };
        for (n, r) in reasons {
            if r.attempts > 0 {
                result.reasons[*n] = Reason {
                    attempts: 1,
                    tokens: r.tokens,
                    ..Reason::default()
                };
            }
        }
        if pending {
            result.reasons[0].attempts = 1;
        } else if unsettled {
            result.reasons[1].attempts = 1;
        } else if self.count == 0 {
            result.reasons[2].attempts = 1;
        }
        result
    }
}
fn event_reason_change(
    reasons: &mut Vec<(usize, Reason)>,
    code: usize,
    tokens: [i64; 4],
    sign: i64,
) -> Result<()> {
    let n = if let Some(n) = reasons.iter().position(|(c, _)| *c == code) {
        n
    } else {
        reasons.reserve_exact(1);
        reasons.push((code, Reason::default()));
        reasons.len() - 1
    };
    change(&mut reasons[n].1.attempts, 1, sign)?;
    array_change(&mut reasons[n].1.tokens, &tokens, sign)
}

#[derive(Debug)]
struct Invocation {
    run: Arc<RunKey>,
    owner: Scope,
    ordinal: i64,
    lifecycle: UsageInvocationLifecycle,
    telemetry: DbUsageTelemetryState,
    events: Events,
    foreign: HashMap<Arc<str>, Events>,
}
impl Invocation {
    fn totals(&self, scope: &Scope) -> Option<Totals> {
        if scope.is_none() || scope == &self.owner {
            Some(self.events.attempt(self.lifecycle, self.telemetry))
        } else {
            scope
                .as_ref()
                .and_then(|id| self.foreign.get(id))
                .filter(|e| e.count > 0)
                .map(|e| e.attempt(self.lifecycle, self.telemetry))
        }
    }
}
#[derive(Debug, Default)]
struct Run {
    attempts: Totals,
    domain: Option<bool>,
}
impl Run {
    fn totals(&self, surface: DbUsageSurface) -> Totals {
        let mut t = self.attempts.clone();
        let c = &mut t.coverage;
        let exists = c[7] > 0 || self.domain.is_some();
        c[0] = i64::from(exists);
        if exists {
            t.counts[match surface {
                DbUsageSurface::TaskExecution => 0,
                DbUsageSurface::MainInquiry => 2,
                _ => 1,
            }] = 1;
        }
        t.active_domains = i64::from(self.domain == Some(true));
        if c[7] == 0 {
            if self.domain == Some(true) {
                c[1] = 1;
            } else if exists {
                c[2] = 1;
            }
        } else {
            if c[9] > 0 {
                c[1] = 1;
            } else if c[10] == 0 && c[11] == c[7] {
                c[3] = 1;
            }
            if c[9] == 0 {
                if c[13] == c[7] && c[10] == 0 {
                    c[4] = 1;
                } else if c[13] > 0 {
                    c[5] = 1;
                } else {
                    c[6] = 1;
                }
            }
        }
        for r in &mut t.reasons {
            r.runs = i64::from(r.attempts > 0);
        }
        t
    }
}

// Reported provenance is invariant for each key, so only its reference count is
// retained. Estimated provenance needs ordering citations: freshness/formula/
// retrospective fields can differ for the same key. No event payload is kept.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SourceOrder {
    source: Arc<str>,
    ordinal: i64,
    invocation: Arc<str>,
    occurred: Arc<str>,
    event: Arc<str>,
}
#[derive(Debug)]
enum Citations {
    Uniform(CostSourceRef, i64),
    Ordered(BTreeMap<SourceOrder, CostSourceRef>),
}
#[derive(Debug, Default)]
struct Rollup {
    totals: Totals,
    sources: BTreeMap<String, Citations>,
    cached: Option<UsageAggregate>,
}
impl Rollup {
    fn source(
        &mut self,
        key: &str,
        source: &CostSourceRef,
        order: SourceOrder,
        sign: i64,
    ) -> Result<()> {
        let book = self.sources.entry(key.to_owned()).or_insert_with(|| {
            if key.starts_with("reported:") {
                Citations::Uniform(source.clone(), 0)
            } else {
                Citations::Ordered(BTreeMap::new())
            }
        });
        let empty = match book {
            Citations::Uniform(_, count) => {
                change(count, 1, sign)?;
                *count == 0
            }
            Citations::Ordered(entries) => {
                if sign > 0 {
                    entries.insert(order, source.clone());
                } else {
                    entries.remove(&order).ok_or_else(invalid_transition)?;
                }
                entries.is_empty()
            }
        };
        if empty {
            self.sources.remove(key);
        }
        self.cached = None;
        Ok(())
    }
    fn public(&mut self) -> Result<UsageAggregate> {
        if let Some(value) = &self.cached {
            return Ok(value.clone());
        }
        let mut sources = self
            .sources
            .values()
            .map(|entries| match entries {
                Citations::Uniform(source, _) => source.clone(),
                Citations::Ordered(values) => {
                    values.last_key_value().expect("nonempty source").1.clone()
                }
            })
            .collect::<Vec<_>>();
        sources.sort_by(|a, b| {
            source_order(a.source_kind)
                .cmp(&source_order(b.source_kind))
                .then_with(|| a.rate_revision_id.cmp(&b.rate_revision_id))
                .then_with(|| a.catalog_snapshot_id.cmp(&b.catalog_snapshot_id))
        });
        let value = self.totals.public(sources)?;
        self.cached = Some(value.clone());
        Ok(value)
    }
}
// SQLite's duration expression is a dyadic f64. Keep its exact value in
// fixed-point ticks; an error interval certifies that SQLite's sequential AVG
// has the same rounded millisecond. Ambiguous round boundaries retain the
// original SQL AVG (cached until execution inputs change), rather than guessing.
const DURATION_SCALE: f64 = 4_294_967_296.0;
#[derive(Debug, Clone)]
struct Execution {
    run: Arc<RunKey>,
    owner: Scope,
    pending: bool,
    completed: bool,
    duration: Option<i128>,
    stable_date: bool,
}
#[derive(Debug, Default)]
struct ExecutionStats {
    count: i64,
    running: i64,
    completed: i64,
    duration_count: i64,
    duration: i128,
    absolute_duration: i128,
    unstable: i64,
    cached_average: Option<Option<i64>>,
}
impl ExecutionStats {
    fn change(&mut self, e: &Execution, sign: i64) -> Result<()> {
        change(&mut self.count, 1, sign)?;
        change(&mut self.running, i64::from(e.pending), sign)?;
        change(&mut self.completed, i64::from(e.completed), sign)?;
        change(&mut self.unstable, i64::from(!e.stable_date), sign)?;
        if let Some(duration) = e.duration {
            change(&mut self.duration_count, 1, sign)?;
            self.duration = self
                .duration
                .checked_add(
                    duration
                        .checked_mul(i128::from(sign))
                        .ok_or_else(invalid_transition)?,
                )
                .ok_or_else(invalid_transition)?;
            self.absolute_duration = self
                .absolute_duration
                .checked_add(
                    duration
                        .abs()
                        .checked_mul(i128::from(sign))
                        .ok_or_else(invalid_transition)?,
                )
                .ok_or_else(invalid_transition)?;
        }
        self.cached_average = None;
        Ok(())
    }
    fn average(&self) -> Option<Option<i64>> {
        if self.unstable > 0 {
            return None;
        }
        if let Some(value) = self.cached_average {
            return Some(value);
        }
        if self.duration_count == 0 {
            return Some(None);
        }
        let n = self.duration_count as f64;
        let mean = (self.duration as f64 / DURATION_SCALE) / n;
        let absolute = self.absolute_duration as f64 / DURATION_SCALE;
        let error =
            (absolute * f64::EPSILON * (n + 3.0) * 2.0) / n + mean.abs() * f64::EPSILON * 4.0;
        let low = (mean - error).round() as i64;
        let high = (mean + error).round() as i64;
        (low == high).then_some(Some(low))
    }
}

#[derive(Debug, Default)]
struct State {
    watermarks: Option<Watermarks>,
    overflow: Option<i64>,
    invocations: HashMap<Arc<str>, Invocation>,
    executions: HashMap<Arc<str>, Execution>,
    runs: HashMap<ScopedRun, Run>,
    scopes: HashMap<Scope, Rollup>,
    bytes: usize,
    execution_stats: HashMap<Arc<str>, ExecutionStats>,
    #[cfg(test)]
    audit: Audit,
}
#[cfg(test)]
#[derive(Debug, Default, Clone, Copy)]
struct Audit {
    payload_rows: usize,
    statements: usize,
    attempt_updates: usize,
}
impl State {
    fn run_change(
        &mut self,
        key: ScopedRun,
        before: Option<Totals>,
        after: Option<Totals>,
        domain: Option<Option<bool>>,
    ) -> Result<()> {
        let r = self.runs.entry(key.clone()).or_default();
        let old = r.totals(key.1.surface);
        if let Some(before) = before {
            r.attempts.change(&before, -1)?;
        }
        if let Some(after) = after {
            r.attempts.change(&after, 1)?;
        }
        if let Some(domain) = domain {
            r.domain = domain;
        }
        let new = r.totals(key.1.surface);
        let total = self.scopes.entry(key.0).or_default();
        total.totals.change(&old, -1)?;
        total.totals.change(&new, 1)?;
        total.cached = None;
        Ok(())
    }
    fn invocation(&mut self, row: UsageInvocation) -> Result<()> {
        let id: Arc<str> = Arc::from(row.id.as_str());
        if let Some(old) = self.invocations.get(&id) {
            if old.lifecycle == row.lifecycle && old.telemetry == row.telemetry_state {
                return Ok(());
            }
            // V135's event guard permits events only after settlement; lifecycle
            // and telemetry cannot change after settlement. Active attempts have
            // no events, so an in-place lifecycle delta never walks old events.
            if old.events.count != 0 {
                return Err(invalid_transition());
            }
            let run = old.run.clone();
            let owner = old.owner.clone();
            let before = old.events.attempt(old.lifecycle, old.telemetry);
            let after = old.events.attempt(row.lifecycle, row.telemetry_state);
            self.run_change(
                (None, run.clone()),
                Some(before.clone()),
                Some(after.clone()),
                None,
            )?;
            if owner.is_some() {
                self.run_change((owner, run), Some(before), Some(after), None)?;
            }
            let old = self.invocations.get_mut(&id).expect("indexed invocation");
            old.lifecycle = row.lifecycle;
            old.telemetry = row.telemetry_state;
        } else {
            let run = Arc::new(RunKey {
                surface: row.surface,
                source_id: row.source_id,
            });
            let owner = row.agent_id.as_deref().map(Arc::from);
            let item = Invocation {
                run: run.clone(),
                owner: owner.clone(),
                ordinal: row.attempt_ordinal,
                lifecycle: row.lifecycle,
                telemetry: row.telemetry_state,
                events: Events::default(),
                foreign: HashMap::new(),
            };
            let after = item.events.attempt(item.lifecycle, item.telemetry);
            self.bytes += std::mem::size_of::<Invocation>()
                + id.len()
                + run.source_id.len()
                + owner.as_ref().map_or(0, |s: &Arc<str>| s.len())
                + 256;
            self.run_change((None, run.clone()), None, Some(after.clone()), None)?;
            if owner.is_some() {
                self.run_change((owner, run), None, Some(after), None)?;
            }
            self.invocations.insert(id, item);
        }
        #[cfg(test)]
        {
            self.audit.attempt_updates += 1;
        }
        Ok(())
    }
    fn event(&mut self, e: EffectiveUsageEvent, sign: i64) -> Result<()> {
        let id: Arc<str> = Arc::from(e.event.invocation_id.as_str());
        let i = self.invocations.get(&id).ok_or_else(invalid_transition)?;
        let run = i.run.clone();
        let owner = i.owner.clone();
        let foreign = e
            .event
            .agent_id
            .as_deref()
            .map(Arc::<str>::from)
            .filter(|agent| Some(agent) != owner.as_ref());
        let mut scopes = vec![None];
        if owner.is_some() {
            scopes.push(owner.clone());
        }
        if let Some(agent) = &foreign {
            scopes.push(Some(agent.clone()));
        }
        let before = scopes
            .iter()
            .map(|scope| (scope.clone(), i.totals(scope)))
            .collect::<Vec<_>>();
        let order = SourceOrder {
            source: Arc::from(run.source_id.as_str()),
            ordinal: i.ordinal,
            invocation: id.clone(),
            occurred: Arc::from(e.event.occurred_at.as_str()),
            event: Arc::from(e.event.id.as_str()),
        };
        let source = if i.lifecycle != UsageInvocationLifecycle::Unsettled {
            e.source.as_ref()
        } else {
            None
        };
        for scope in &scopes {
            if let Some((key, value)) = source {
                self.scopes.entry(scope.clone()).or_default().source(
                    key,
                    value,
                    order.clone(),
                    sign,
                )?;
                if !key.starts_with("reported:") {
                    let size = std::mem::size_of::<SourceOrder>()
                        + std::mem::size_of::<CostSourceRef>()
                        + key.len()
                        + order.source.len()
                        + order.invocation.len()
                        + order.occurred.len()
                        + order.event.len()
                        + 512;
                    self.bytes = self
                        .bytes
                        .checked_add_signed(sign as isize * size as isize)
                        .ok_or_else(invalid_transition)?;
                }
            }
        }
        let previous_heap = {
            let i = self.invocations.get(&id).expect("indexed invocation");
            i.events.heap_bytes()
                + foreign
                    .as_ref()
                    .and_then(|a| i.foreign.get(a))
                    .map_or(0, Events::heap_bytes)
        };
        let i = self.invocations.get_mut(&id).expect("indexed invocation");
        i.events.change(&e, sign)?;
        if let Some(agent) = &foreign {
            if !i.foreign.contains_key(agent) {
                self.bytes += std::mem::size_of::<Events>() + agent.len() + 128;
            }
            i.foreign
                .entry(agent.clone())
                .or_default()
                .change(&e, sign)?;
        }
        let new_heap = i.events.heap_bytes()
            + foreign
                .as_ref()
                .and_then(|a| i.foreign.get(a))
                .map_or(0, Events::heap_bytes);
        self.bytes = self
            .bytes
            .checked_add(new_heap - previous_heap)
            .ok_or_else(invalid_transition)?;
        let after = scopes
            .iter()
            .map(|scope| (scope.clone(), i.totals(scope)))
            .collect::<Vec<_>>();
        for ((scope, old), (_, new)) in before.into_iter().zip(after) {
            self.run_change((scope, run.clone()), old, new, None)?;
        }
        Ok(())
    }
    fn execution(&mut self, row: &sqlx::sqlite::SqliteRow) -> Result<()> {
        let raw_id: String = row.try_get("id")?;
        let id: Arc<str> = Arc::from(raw_id);
        let status: String = row.try_get("status")?;
        let created: String = row.try_get("created_at")?;
        let updated: String = row.try_get("updated_at")?;
        let duration: Option<f64> = row.try_get("duration_ms")?;
        let new = Execution {
            run: Arc::new(RunKey {
                surface: DbUsageSurface::TaskExecution,
                source_id: id.to_string(),
            }),
            owner: row.try_get::<Option<String>, _>("agent_id")?.map(Arc::from),
            pending: status == "running",
            completed: status == "completed",
            duration: duration.map(|value| (value * DURATION_SCALE) as i128),
            stable_date: chrono::DateTime::parse_from_rfc3339(&created).is_ok()
                && chrono::DateTime::parse_from_rfc3339(&updated).is_ok(),
        };
        if let Some(old) = self.executions.remove(&id) {
            if let Some(owner) = &old.owner {
                self.execution_stats
                    .entry(owner.clone())
                    .or_default()
                    .change(&old, -1)?;
            }
            self.run_change((None, old.run.clone()), None, None, Some(None))?;
            if old.owner.is_some() {
                self.run_change((old.owner, old.run), None, None, Some(None))?;
            }
        } else {
            self.bytes += std::mem::size_of::<Execution>()
                + id.len() * 2
                + new.owner.as_ref().map_or(0, |s| s.len())
                + 128;
        }
        if let Some(owner) = &new.owner {
            self.execution_stats
                .entry(owner.clone())
                .or_default()
                .change(&new, 1)?;
        }
        self.run_change((None, new.run.clone()), None, None, Some(Some(new.pending)))?;
        if new.owner.is_some() {
            self.run_change(
                (new.owner.clone(), new.run.clone()),
                None,
                None,
                Some(Some(new.pending)),
            )?;
        }
        self.executions.insert(id, new);
        Ok(())
    }
    fn bound(&self) -> bool {
        self.bytes + self.runs.len() * (std::mem::size_of::<Run>() + 256) + self.scopes.len() * 2048
            > MAX_INDEX_BYTES
    }
}

/// One per database in the shared server/Solo graph. A warm read probes one
/// singleton row. Deltas replace compact attempt/run summaries and update
/// running totals; they never fold the historical run map. At the charged
/// 128 MiB bound the index is discarded and reads use the fresh reference until
/// a deletion generation permits a smaller rebuild. Correctness wins at the bound.
#[derive(Debug)]
pub struct UsageLedgerIndex {
    db: Arc<db::SqliteDb>,
    state: tokio::sync::Mutex<State>,
}
impl UsageLedgerIndex {
    pub fn new(db: Arc<db::SqliteDb>) -> Self {
        Self {
            db,
            state: tokio::sync::Mutex::new(State::default()),
        }
    }
    pub async fn operations(&self) -> Result<UsageAggregate> {
        let mut state = self.state.lock().await;
        self.sync(&mut state).await?;
        if state.overflow.is_some() {
            return usage_aggregate_for_operations(&self.db).await;
        }
        state.scopes.entry(None).or_default().public()
    }
    pub async fn agent(&self, id: &str) -> Result<UsageAggregate> {
        self.agents(&[id.to_owned()])
            .await?
            .remove(id)
            .ok_or_else(invalid_transition)
    }
    pub async fn agents(&self, ids: &[String]) -> Result<HashMap<String, UsageAggregate>> {
        let mut state = self.state.lock().await;
        self.sync(&mut state).await?;
        if state.overflow.is_some() {
            let mut tx = self.db.pool().begin().await?;
            let values = agent_usage_in_snapshot(&mut tx, ids).await?;
            tx.commit().await?;
            return Ok(values);
        }
        let mut result = HashMap::new();
        for id in ids {
            let key = Some(Arc::<str>::from(id.as_str()));
            let value = if let Some(scope) = state.scopes.get_mut(&key) {
                scope.public()?
            } else {
                Totals::default().public(Vec::new())?
            };
            result.insert(id.clone(), value);
        }
        Ok(result)
    }
    /// Page statistics share the execution deltas; active assignment counts
    /// remain a live repository query. No terminal history scan on ledger appends.
    pub async fn agent_execution_stats(
        &self,
        ids: &[String],
    ) -> Result<HashMap<String, (db::AgentExecutionStats, i64)>> {
        use db::{AgentRepo, ExecutionRepo};
        let mut state = self.state.lock().await;
        self.sync(&mut state).await?;
        let mut result = HashMap::new();
        for id in ids {
            let key: Arc<str> = Arc::from(id.as_str());
            if state.overflow.is_some() {
                result.insert(
                    id.clone(),
                    (
                        ExecutionRepo::stats_by_agent(&*self.db, id).await?,
                        AgentRepo::count_running_executions(&*self.db, id).await?,
                    ),
                );
                continue;
            }
            let Some(stats) = state.execution_stats.get_mut(&key) else {
                result.insert(
                    id.clone(),
                    (
                        db::AgentExecutionStats {
                            avg_duration_ms: None,
                            success_rate: None,
                        },
                        0,
                    ),
                );
                continue;
            };

            let average = if let Some(average) = stats.average() {
                average
            } else {
                let value = ExecutionRepo::stats_by_agent(&*self.db, id)
                    .await?
                    .avg_duration_ms;
                if stats.unstable == 0 {
                    stats.cached_average = Some(value);
                }
                value
            };
            result.insert(
                id.clone(),
                (
                    db::AgentExecutionStats {
                        avg_duration_ms: average,
                        success_rate: (stats.count > 0)
                            .then_some(stats.completed as f64 / stats.count as f64),
                    },
                    stats.running,
                ),
            );
        }
        Ok(result)
    }

    async fn sync(&self, state: &mut State) -> Result<()> {
        #[cfg(test)]
        {
            state.audit = Audit::default();
            state.audit.statements = 1;
        }
        let header = {
            let mut connection = self.db.pool().acquire().await?;
            Watermarks::read(&mut connection).await?
        };
        if state.watermarks == Some(header) || state.overflow == Some(header.deletion_generation) {
            return Ok(());
        }
        let mut tx = self.db.pool().begin().await?;
        let header = Watermarks::read(&mut tx).await?;
        if state
            .watermarks
            .is_none_or(|old| old.deletion_generation != header.deletion_generation)
            || state.overflow.is_some()
        {
            *state = State::default();
        }
        let old = state.watermarks.unwrap_or_default();
        let result = apply_delta(state, &mut tx, old, header).await;
        if let Err(error) = result {
            *state = State::default();
            return Err(error);
        }
        if let Err(error) = tx.commit().await {
            *state = State::default();
            return Err(error.into());
        }
        if state.bound() {
            *state = State {
                overflow: Some(header.deletion_generation),
                ..State::default()
            };
        } else {
            state.watermarks = Some(header);
        }
        Ok(())
    }
}

async fn effective_events(
    connection: &mut sqlx::SqliteConnection,
    events: Vec<UsageEvent>,
    through: i64,
) -> Result<Vec<EffectiveUsageEvent>> {
    let mut result = Vec::with_capacity(events.len());
    for chunk in events.chunks(150) {
        let ids = chunk.iter().map(|e| e.id.as_str()).collect::<Vec<_>>();
        let rows=sqlx::query("WITH requested AS (SELECT value AS id FROM json_each(?))
            SELECT e.id AS event_id, er.id AS applied_id, er.estimated_nano_usd AS applied_nanos,
              er.rate_revision_id AS applied_rate, er.catalog_snapshot_id AS applied_catalog,
              er.formula_revision AS applied_formula, er.retrospective AS applied_retrospective,
              ps.source_kind AS selection_source_kind, ps.catalog_freshness AS selection_catalog_freshness,
              ps.rate_revision_id AS selection_rate_revision_id, ps.catalog_snapshot_id AS selection_catalog_snapshot_id,
              r.source_kind AS rate_source_kind, r.catalog_snapshot_id AS rate_catalog_snapshot_id,
              r.effective_at AS rate_effective_at, c.revision_digest AS catalog_digest,
              c.fetched_at AS catalog_fetched_at, ep.catalog_freshness AS estimate_catalog_freshness
            FROM requested JOIN usage_event e ON e.id=requested.id
            JOIN usage_invocation i ON i.id=e.invocation_id
            LEFT JOIN cost_estimate_revision er ON er.id=(SELECT prior.id FROM cost_estimate_revision prior
              WHERE prior.usage_event_id=e.id AND prior.state='applied' AND prior.rowid<=?
              ORDER BY prior.revision DESC,prior.created_at DESC,prior.id DESC LIMIT 1)
            LEFT JOIN pricing_selection ps ON ps.id=i.pricing_selection_id
            LEFT JOIN cost_estimation_run erun ON erun.id=er.run_id
            LEFT JOIN cost_estimation_preview ep ON ep.id=erun.preview_id
            LEFT JOIN pricing_rate_revision r ON r.id=COALESCE(er.rate_revision_id,e.rate_revision_id,ps.rate_revision_id)
            LEFT JOIN pricing_catalog_snapshot c ON c.id=COALESCE(er.catalog_snapshot_id,e.catalog_snapshot_id,r.catalog_snapshot_id,ps.catalog_snapshot_id)")
            .bind(serde_json::to_string(&ids).map_err(|_|invalid_transition())?).bind(through).fetch_all(&mut *connection).await?;
        let mut rows = rows
            .into_iter()
            .map(|r| Ok((r.try_get::<String, _>("event_id")?, r)))
            .collect::<std::result::Result<HashMap<_, _>, sqlx::Error>>()?;
        for original in chunk {
            let row = rows.remove(&original.id).ok_or_else(invalid_transition)?;
            let mut event = original.clone();
            if row.try_get::<Option<String>, _>("applied_id")?.is_some() {
                if event.cost_kind == UsageCostKind::ProviderReported {
                    return Err(invalid_transition());
                }
                event.cost_kind = UsageCostKind::Estimated;
                event.estimated_nano_usd = Some(
                    row.try_get::<Option<i64>, _>("applied_nanos")?
                        .ok_or_else(invalid_transition)?,
                );
                event.rate_revision_id = row
                    .try_get::<Option<String>, _>("applied_rate")?
                    .or(event.rate_revision_id);
                event.catalog_snapshot_id = row
                    .try_get::<Option<String>, _>("applied_catalog")?
                    .or(event.catalog_snapshot_id);
                event.formula_revision = row
                    .try_get::<Option<String>, _>("applied_formula")?
                    .or(event.formula_revision);
                event.retrospective = row.try_get("applied_retrospective")?;
            }
            let metadata = event_source_metadata_from_row(&row, &event)?;
            result.push(EffectiveUsageEvent {
                source: event_source_with_metadata(&event, Some(&metadata))?,
                event,
            });
        }
    }
    Ok(result)
}

async fn apply_delta(
    state: &mut State,
    connection: &mut sqlx::SqliteConnection,
    old: Watermarks,
    new: Watermarks,
) -> Result<()> {
    let mut position = old.invocation_rowid;
    while position < new.invocation_rowid {
        let rows = db::SqliteDb::usage_invocation_slice(connection, position, new.invocation_rowid)
            .await?;
        #[cfg(test)]
        {
            state.audit.statements += 1;
            state.audit.payload_rows += rows.len();
        }
        if rows.is_empty() {
            break;
        }
        for (rowid, row) in rows {
            position = rowid;
            state.invocation(row)?;
        }
    }
    let mut position = old.invocation_revision;
    while position < new.invocation_revision {
        let changes=sqlx::query("SELECT id,revision FROM usage_changed_invocation WHERE revision>? AND revision<=? ORDER BY revision LIMIT 400")
            .bind(position).bind(new.invocation_revision).fetch_all(&mut *connection).await?;
        #[cfg(test)]
        {
            state.audit.statements += 1;
            state.audit.payload_rows += changes.len();
        }
        if changes.is_empty() {
            break;
        }
        let ids = changes
            .iter()
            .map(|r| r.try_get("id"))
            .collect::<std::result::Result<Vec<String>, _>>()?;
        position = changes.last().expect("changes").try_get("revision")?;
        let rows = db::SqliteDb::usage_invocations_by_ids(connection, &ids).await?;
        #[cfg(test)]
        {
            state.audit.statements += 1;
            state.audit.payload_rows += rows.len();
        }
        for row in rows {
            state.invocation(row)?;
        }
    }
    let mut position = old.event_rowid;
    while position < new.event_rowid {
        let rows = db::SqliteDb::usage_event_slice(connection, position, new.event_rowid).await?;
        #[cfg(test)]
        {
            state.audit.statements += 1;
            state.audit.payload_rows += rows.len();
        }
        if rows.is_empty() {
            break;
        }
        position = rows.last().expect("events").0;
        let events = rows.into_iter().map(|(_, e)| e).collect::<Vec<_>>();
        #[cfg(test)]
        {
            state.audit.statements += events.len().div_ceil(150);
            state.audit.payload_rows += events.len();
        }
        for event in effective_events(connection, events, new.estimate_rowid).await? {
            state.event(event, 1)?;
        }
    }
    // A new revision can replace the effective amount/provenance of an old
    // event. Read that event at the old and new revision watermarks, subtract
    // the old contribution and add the new one. No invocation history walk.
    let mut position = old.estimate_rowid;
    let mut revised = BTreeSet::new();
    while position < new.estimate_rowid {
        let rows=sqlx::query("SELECT r.rowid AS revision_rowid,e.id FROM cost_estimate_revision r JOIN usage_event e ON e.id=r.usage_event_id WHERE r.rowid>? AND r.rowid<=? AND e.rowid<=? ORDER BY r.rowid LIMIT 400")
            .bind(position).bind(new.estimate_rowid).bind(old.event_rowid).fetch_all(&mut *connection).await?;
        #[cfg(test)]
        {
            state.audit.statements += 1;
            state.audit.payload_rows += rows.len();
        }
        if rows.is_empty() {
            break;
        }
        for row in rows {
            position = row.try_get("revision_rowid")?;
            revised.insert(row.try_get::<String, _>("id")?);
        }
    }
    for chunk in revised.into_iter().collect::<Vec<_>>().chunks(400) {
        let events = db::SqliteDb::usage_events_by_ids(connection, chunk).await?;
        #[cfg(test)]
        {
            state.audit.statements += 1 + 2 * events.len().div_ceil(150);
            state.audit.payload_rows += 3 * events.len();
        }
        for event in effective_events(connection, events.clone(), old.estimate_rowid).await? {
            state.event(event, -1)?;
        }
        for event in effective_events(connection, events, new.estimate_rowid).await? {
            state.event(event, 1)?;
        }
    }
    let mut position = old.execution_rowid;
    while position < new.execution_rowid {
        let rows=sqlx::query("SELECT rowid AS execution_rowid,id,agent_id,status,created_at,updated_at,CASE WHEN status!='running' THEN (JULIANDAY(updated_at)-JULIANDAY(created_at))*86400000 END AS duration_ms FROM execution WHERE rowid>? AND rowid<=? ORDER BY rowid LIMIT 400")
            .bind(position).bind(new.execution_rowid).fetch_all(&mut *connection).await?;
        #[cfg(test)]
        {
            state.audit.statements += 1;
            state.audit.payload_rows += rows.len();
        }
        if rows.is_empty() {
            break;
        }
        for row in rows {
            position = row.try_get("execution_rowid")?;
            state.execution(&row)?;
        }
    }
    let mut position = old.execution_revision;
    let mut last_id = String::new();
    loop {
        let rows=sqlx::query("SELECT c.id,c.revision,e.id AS present_id,e.agent_id,e.status,e.created_at,e.updated_at,CASE WHEN e.status!='running' THEN (JULIANDAY(e.updated_at)-JULIANDAY(e.created_at))*86400000 END AS duration_ms FROM usage_changed_execution c LEFT JOIN execution e ON e.id=c.id WHERE (c.revision>? OR (c.revision=? AND c.id>?)) AND c.revision<=? ORDER BY c.revision,c.id LIMIT 400")
            .bind(position).bind(position).bind(&last_id).bind(new.execution_revision).fetch_all(&mut *connection).await?;
        #[cfg(test)]
        {
            state.audit.statements += 1;
            state.audit.payload_rows += rows.len();
        }
        if rows.is_empty() {
            break;
        }
        for row in rows {
            position = row.try_get("revision")?;
            last_id = row.try_get("id")?;
            if let Some(id) = row.try_get::<Option<String>, _>("present_id")? {
                let _ = id;
                state.execution(&row)?;
            } else if let Some(previous) = state.executions.remove(last_id.as_str()) {
                if let Some(owner) = &previous.owner {
                    state
                        .execution_stats
                        .entry(owner.clone())
                        .or_default()
                        .change(&previous, -1)?;
                }
                state.run_change((None, previous.run.clone()), None, None, Some(None))?;
                if previous.owner.is_some() {
                    state.run_change((previous.owner, previous.run), None, None, Some(None))?;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
