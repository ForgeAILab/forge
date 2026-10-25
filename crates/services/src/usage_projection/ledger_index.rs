//! Rebuildable observation index. No ledger rows or accounting decisions live here.
//! Integer totals/reasons are additive; run classifications depend on attempt
//! counts; provenance is an ordered set (last event wins), not an additive sum.
use super::*;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

const REASONS: usize = 10;
#[cfg(test)]
const MAX_INDEX_BYTES: usize = config::DEFAULT_USAGE_INDEX_BUDGET_MB as usize * 1024 * 1024;
const READ_MEMORY_RESERVE: usize = 8 * 1024 * 1024;
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
    rows: [i64; 4],
    owned_executions: i64,
}
impl Watermarks {
    async fn read(connection: &mut sqlx::SqliteConnection) -> Result<Self> {
        let r = db::usage_index_watermarks_on_connection(connection).await?;
        Ok(Self {
            invocation_rowid: r.invocation_rowid()?,
            event_rowid: r.event_rowid()?,
            estimate_rowid: r.estimate_rowid()?,
            execution_rowid: r.execution_rowid()?,
            invocation_revision: r.invocation_revision()?,
            execution_revision: r.execution_revision()?,
            deletion_generation: r.deletion_generation()?,
            owned_executions: r.owned_execution_count()?,
            rows: [
                r.invocation_count()?,
                r.event_count()?,
                r.estimate_count()?,
                r.execution_count()?,
            ],
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

// One count and maximum order per distinct reference, not per event. A reprice
// which removes a maximum is repaired from the same read snapshot before the
// delta is published. Frozen pricing ids keep references stable.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SourceOrder {
    source: Arc<str>,
    ordinal: i64,
    invocation: Arc<str>,
    occurred: Arc<str>,
    event: Arc<str>,
}
#[derive(Debug)]
struct Citation {
    reference: CostSourceRef,
    count: i64,
    winner: Option<SourceOrder>,
    needs_repair: bool,
}
#[derive(Debug, Default)]
struct Citations {
    variants: BTreeMap<String, Citation>,
    winners: BTreeMap<SourceOrder, String>,
    bytes: usize,
    pending: usize,
}
impl Citations {
    fn variant_key(reference: &CostSourceRef) -> Result<String> {
        serde_json::to_string(reference).map_err(|_| invalid_transition())
    }
    fn charge(key: &str, citation: &Citation) -> usize {
        // Inline BTree entries, allocator/node overhead, serialized identity,
        // provenance strings and its public-cache copy; winners retain one
        // order/key per variant. This is independent of citation count.
        let reference = &citation.reference;
        let strings = [
            &reference.rate_revision_id,
            &reference.catalog_snapshot_id,
            &reference.catalog_digest,
            &reference.effective_at,
            &reference.fetched_at,
            &reference.formula_revision,
        ]
        .iter()
        .filter_map(|s| s.as_ref())
        .map(|s| s.capacity() + 16)
        .sum::<usize>();
        std::mem::size_of::<Citation>()
            + std::mem::size_of::<CostSourceRef>()
            + key.len() * 2
            + strings * 2
            + 256
            + citation.winner.as_ref().map_or(0, |order| {
                std::mem::size_of::<SourceOrder>()
                    + order.source.len()
                    + order.invocation.len()
                    + order.occurred.len()
                    + order.event.len()
                    + 96
            })
    }
    fn heap_bytes(&self) -> usize {
        self.bytes
    }
    fn change(
        &mut self,
        reference: &CostSourceRef,
        order: SourceOrder,
        sign: i64,
        uniform: bool,
    ) -> Result<()> {
        let key = Self::variant_key(reference)?;
        let old = self.variants.remove(&key);
        let mut citation = if let Some(c) = old {
            self.bytes = self
                .bytes
                .checked_sub(Self::charge(&key, &c))
                .ok_or_else(invalid_transition)?;
            if c.needs_repair {
                self.pending -= 1;
            }
            if let Some(order) = &c.winner {
                self.winners.remove(order);
            }
            c
        } else {
            if sign < 0 {
                return Err(invalid_transition());
            }
            Citation {
                reference: reference.clone(),
                count: 0,
                winner: None,
                needs_repair: false,
            }
        };
        change(&mut citation.count, 1, sign)?;
        if sign > 0 {
            if citation.winner.as_ref().is_none_or(|old| order > *old) {
                citation.winner = Some(order);
            }
        } else if !uniform && citation.winner.as_ref() == Some(&order) {
            citation.winner = None;
            citation.needs_repair = citation.count > 0;
        }
        if citation.count > 0 {
            self.bytes += Self::charge(&key, &citation);
            if citation.needs_repair {
                self.pending += 1;
            }
            if let Some(order) = &citation.winner {
                self.winners.insert(order.clone(), key.clone());
            }
            self.variants.insert(key, citation);
        }
        Ok(())
    }
    fn repair(&mut self, reference: &CostSourceRef, winner: Option<SourceOrder>) -> Result<()> {
        let key = Self::variant_key(reference)?;
        if let Some(citation) = self.variants.get_mut(&key) {
            let before = Self::charge(&key, citation);
            if let Some(order) = &citation.winner {
                self.winners.remove(order);
            }
            if citation.needs_repair {
                self.pending -= 1;
            }
            // A known retained winner can be older than another event added
            // in this delta. Both are valid candidates; keep the greater order.
            citation.winner = citation.winner.take().max(winner);
            citation.needs_repair = false;
            if let Some(order) = &citation.winner {
                self.winners.insert(order.clone(), key.clone());
            }
            self.bytes = self
                .bytes
                .checked_add_signed(Self::charge(&key, citation) as isize - before as isize)
                .ok_or_else(invalid_transition)?;
        }
        Ok(())
    }
    fn contains_winner(&self, reference: &CostSourceRef, order: &SourceOrder) -> Result<bool> {
        Ok(self
            .variants
            .get(&Self::variant_key(reference)?)
            .is_some_and(|c| c.count > 1 && c.winner.as_ref() == Some(order)))
    }
    fn winner(&self) -> Result<CostSourceRef> {
        if self.pending > 0 {
            return Err(invalid_transition());
        }
        let (_, key) = self
            .winners
            .last_key_value()
            .ok_or_else(invalid_transition)?;
        Ok(self
            .variants
            .get(key)
            .ok_or_else(invalid_transition)?
            .reference
            .clone())
    }
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
        let book = self.sources.entry(key.to_owned()).or_default();
        book.change(source, order, sign, key.starts_with("reported:"))?;
        if book.variants.is_empty() {
            self.sources.remove(key);
        }
        self.cached = None;
        Ok(())
    }
    fn public(&mut self) -> Result<UsageAggregate> {
        if let Some(value) = &self.cached {
            return Ok(value.clone());
        }
        let mut sources = Vec::with_capacity(self.sources.len());
        for book in self.sources.values() {
            sources.push(book.winner()?);
        }
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
    overflow: Option<Overflow>,
    observed_header: Option<Watermarks>,
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
        if r.domain.is_none() && r.attempts.coverage[7] == 0 {
            self.runs.remove(&key);
        }
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
            self.bytes += id.len()
                + run.source_id.len()
                + owner.as_ref().map_or(0, |s: &Arc<str>| s.len())
                + std::mem::size_of::<RunKey>()
                + 128;
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
                let rollup = self.scopes.entry(scope.clone()).or_default();
                let before = rollup
                    .sources
                    .get(key)
                    .map_or(0, |book| book.heap_bytes() + key.len() + 96);
                rollup.source(key, value, order.clone(), sign)?;
                let after = rollup
                    .sources
                    .get(key)
                    .map_or(0, |book| book.heap_bytes() + key.len() + 96);
                self.bytes = self
                    .bytes
                    .checked_add_signed(after as isize - before as isize)
                    .ok_or_else(invalid_transition)?;
            }
        }
        let previous_heap = {
            let i = self.invocations.get(&id).expect("indexed invocation");
            i.events.heap_bytes()
                + i.foreign.capacity() * (std::mem::size_of::<(Arc<str>, Events)>() + 2) * 8 / 7
                + foreign
                    .as_ref()
                    .and_then(|a| i.foreign.get(a))
                    .map_or(0, Events::heap_bytes)
        };
        let i = self.invocations.get_mut(&id).expect("indexed invocation");
        i.events.change(&e, sign)?;
        if let Some(agent) = &foreign {
            if !i.foreign.contains_key(agent) {
                self.bytes += agent.len() + 32;
            }
            i.foreign
                .entry(agent.clone())
                .or_default()
                .change(&e, sign)?;
        }
        let new_heap = i.events.heap_bytes()
            + i.foreign.capacity() * (std::mem::size_of::<(Arc<str>, Events)>() + 2) * 8 / 7
            + foreign
                .as_ref()
                .and_then(|a| i.foreign.get(a))
                .map_or(0, Events::heap_bytes);
        self.bytes = self
            .bytes
            .checked_add_signed(new_heap as isize - previous_heap as isize)
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
    fn execution(&mut self, row: &db::UsageIndexExecutionRow) -> Result<()> {
        let raw_id: String = row.id()?;
        let id: Arc<str> = Arc::from(raw_id);
        let status: String = row.status()?;
        let created: String = row.created_at()?;
        let updated: String = row.updated_at()?;
        let duration: Option<f64> = row.duration_ms()?;
        let new = Execution {
            run: Arc::new(RunKey {
                surface: DbUsageSurface::TaskExecution,
                source_id: id.to_string(),
            }),
            owner: row.agent_id()?.map(Arc::from),
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
            self.bytes += id.len() * 2
                + new.owner.as_ref().map_or(0, |s| s.len())
                + std::mem::size_of::<RunKey>()
                + 96;
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
    fn charged_bytes(&self) -> usize {
        fn table<K, V>(capacity: usize) -> usize {
            capacity * (std::mem::size_of::<(K, V)>() + 2) * 8 / 7 + 64
        }
        let live = self.bytes
            + table::<Arc<str>,Invocation>(self.invocations.capacity())
            + table::<Arc<str>,Execution>(self.executions.capacity())
            + table::<ScopedRun,Run>(self.runs.capacity())
            + table::<Scope,Rollup>(self.scopes.capacity())
            + table::<Arc<str>,ExecutionStats>(self.execution_stats.capacity())
            // Rollup caches own a second copy of the small public vocabulary;
            // BTree nodes and keys are charged conservatively here.
            + self.scopes.len() * 4096;
        // Allocator arenas retain freed read batches and retired hash-table
        // buckets. Include a 50% reserve in the enforced charge;
        // RSS calibration is recorded with the release benchmark results.
        live.saturating_mul(3) / 2
            + if self.invocations.is_empty() && self.executions.is_empty() {
                0
            } else {
                READ_MEMORY_RESERVE
            }
    }
    fn bound(&self, budget: usize) -> bool {
        self.charged_bytes() > budget
    }
}

#[derive(Debug, Clone, Copy)]
struct Overflow {
    budget: usize,
    header: Watermarks,
    rows: i64,
}
#[derive(Debug, Default)]
struct Fallback {
    header: Option<Watermarks>,
    operations: Option<UsageAggregate>,
    agents: HashMap<String, UsageAggregate>,
    stats: HashMap<String, (db::AgentExecutionStats, i64)>,
    bytes: usize,
}
const MAX_FALLBACK_BYTES: usize = 32 * 1024 * 1024;
impl Fallback {
    fn charge(value: &UsageAggregate) -> usize {
        serde_json::to_string(value).map_or(MAX_FALLBACK_BYTES + 1, |s| s.len() * 2 + 1024)
    }
    fn room(&mut self, bytes: usize) {
        if self.bytes + bytes > MAX_FALLBACK_BYTES
            || self.agents.len() >= 256
            || self.stats.len() >= 256
        {
            self.operations = None;
            self.agents.clear();
            self.stats.clear();
            self.bytes = 0;
        }
    }
    fn store_operations(&mut self, value: &UsageAggregate) {
        let bytes = Self::charge(value);
        self.room(bytes);
        if bytes <= MAX_FALLBACK_BYTES {
            self.operations = Some(value.clone());
            self.bytes += bytes;
        }
    }
    fn store_agent(&mut self, id: &str, value: &UsageAggregate) {
        let bytes = Self::charge(value) + id.len();
        self.room(bytes);
        if bytes <= MAX_FALLBACK_BYTES {
            self.agents.insert(id.to_owned(), value.clone());
            self.bytes += bytes;
        }
    }
}

/// One shared, bounded observation index per database. Reads stage all deltas in
/// a snapshot, then publish them synchronously. Cancellation cannot publish a
/// partial delta. Oversized ledgers use separately memoized fresh aggregates.
#[derive(Debug)]
pub struct UsageLedgerIndex {
    // u64::MAX represents an unset setting; all u32 budgets remain valid.
    configured_budget_mb: AtomicU64,
    db: Arc<db::SqliteDb>,
    state: tokio::sync::Mutex<State>,
    fallback: tokio::sync::Mutex<Fallback>,
    operations_fill: tokio::sync::Mutex<()>,
    agents_fill: tokio::sync::Mutex<()>,
}

async fn overflow_usage_snapshot(
    connection: &mut sqlx::SqliteConnection,
    ids: Option<&[String]>,
) -> Result<HashMap<Scope, UsageAggregate>> {
    match bounded_usage_snapshot(connection, ids).await {
        Err(error) if matches!(error, ServiceError::Db(db::DbError::InvalidTransition)) => {
            tracing::error!(%error, "usage overflow fold invariant failed; using independent fresh reference");
            match ids {
                Some(ids) => Ok(agent_usage_in_snapshot(connection, ids)
                    .await?
                    .into_iter()
                    .map(|(id, value)| (Some(Arc::from(id)), value))
                    .collect()),
                None => Ok(HashMap::from([(
                    None,
                    operations_usage_in_snapshot(connection).await?,
                )])),
            }
        }
        result => result,
    }
}

/// A fresh fold with bounded source batches. Every logical run stays in one
/// batch, so run classifications/reason deduplication are summed exactly once.
/// The reference readers remain independent; only overflow reads use this path.
async fn bounded_usage_snapshot(
    connection: &mut sqlx::SqliteConnection,
    ids: Option<&[String]>,
) -> Result<HashMap<Scope, UsageAggregate>> {
    let requested = ids.map(|ids| {
        ids.iter()
            .map(|id| Some(Arc::<str>::from(id.as_str())))
            .collect::<HashSet<_>>()
    });
    let ids_json =
        serde_json::to_string(ids.unwrap_or_default()).map_err(|_| invalid_transition())?;
    let mut after = String::new();
    let mut first = true;
    let mut totals = HashMap::<Scope, Rollup>::new();
    loop {
        let sources = db::usage_index_source_batch_on_connection(
            &mut *connection,
            first,
            ids.is_none(),
            &ids_json,
            &after,
        )
        .await?;
        if sources.is_empty() {
            break;
        }
        after = sources.last().expect("source batch").clone();
        first = false;
        let invocations = db::SqliteDb::usage_invocations_for_sources(connection, &sources).await?;
        let mut events = effective_usage_events_for_invocations(connection, &invocations).await?;
        let mut batch = State::default();
        for invocation in invocations {
            let effective = events.remove(&invocation.id).unwrap_or_default();
            batch.invocation(invocation)?;
            for event in effective {
                batch.event(event, 1)?;
            }
        }
        for row in db::usage_index_execution_domains_on_connection(
            &mut *connection,
            serde_json::to_string(&sources).map_err(|_| invalid_transition())?,
        )
        .await?
        {
            let id: String = row.id()?;
            let owner = row.agent_id()?.map(Arc::<str>::from);
            let run = Arc::new(RunKey {
                surface: DbUsageSurface::TaskExecution,
                source_id: id,
            });
            let domain = Some(Some(row.status()? == "running"));
            batch.run_change((None, run.clone()), None, None, domain)?;
            if owner.is_some() {
                batch.run_change((owner, run), None, None, domain)?;
            }
        }
        for (scope, rollup) in batch.scopes {
            if requested.as_ref().is_some_and(|ids| !ids.contains(&scope))
                || (ids.is_none() && scope.is_some())
            {
                continue;
            }
            let target = totals.entry(scope).or_default();
            target.totals.change(&rollup.totals, 1)?;
            for (key, book) in rollup.sources {
                let dest = target.sources.entry(key.clone()).or_default();
                for citation in book.variants.into_values() {
                    dest.change(
                        &citation.reference,
                        citation.winner.ok_or_else(invalid_transition)?,
                        1,
                        key.starts_with("reported:"),
                    )?;
                    let entry = dest
                        .variants
                        .get_mut(&Citations::variant_key(&citation.reference)?)
                        .ok_or_else(invalid_transition)?;
                    change(&mut entry.count, citation.count - 1, 1)?;
                }
            }
        }
    }
    if let Some(ids) = ids {
        for id in ids {
            totals.entry(Some(Arc::from(id.as_str()))).or_default();
        }
    } else {
        totals.entry(None).or_default();
    }
    totals
        .into_iter()
        .map(|(scope, mut rollup)| Ok((scope, rollup.public()?)))
        .collect()
}

async fn effective_events(
    connection: &mut sqlx::SqliteConnection,
    events: Vec<UsageEvent>,
    through: i64,
) -> Result<Vec<EffectiveUsageEvent>> {
    let mut result = Vec::with_capacity(events.len());
    for chunk in events.chunks(150) {
        let ids = chunk.iter().map(|e| e.id.as_str()).collect::<Vec<_>>();
        let rows = db::usage_index_effective_event_sources_on_connection(
            &mut *connection,
            serde_json::to_string(&ids).map_err(|_| invalid_transition())?,
            through,
        )
        .await?;
        let mut rows = rows
            .into_iter()
            .map(|r| Ok((r.event_id()?, r)))
            .collect::<std::result::Result<HashMap<_, _>, db::DbError>>()?;
        for original in chunk {
            let row = rows.remove(&original.id).ok_or_else(invalid_transition)?;
            let mut event = original.clone();
            if row.applied_id()?.is_some() {
                if event.cost_kind == UsageCostKind::ProviderReported {
                    return Err(invalid_transition());
                }
                event.cost_kind = UsageCostKind::Estimated;
                event.estimated_nano_usd =
                    Some(row.applied_nanos()?.ok_or_else(invalid_transition)?);
                event.rate_revision_id = row.applied_rate()?.or(event.rate_revision_id);
                event.catalog_snapshot_id = row.applied_catalog()?.or(event.catalog_snapshot_id);
                event.formula_revision = row.applied_formula()?.or(event.formula_revision);
                event.retrospective = row.applied_retrospective()?;
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

type CitationRepair = (Scope, String, CostSourceRef, Option<SourceOrder>);
enum CitationRepairs {
    Ready(Vec<CitationRepair>, usize),
    Overflow(usize),
}
async fn citation_repairs(
    state: &State,
    staged: &Staged,
    connection: &mut sqlx::SqliteConnection,
    through: Watermarks,
    budget: usize,
) -> Result<CitationRepairs> {
    let Staged::Delta { changes, .. } = staged else {
        return Ok(CitationRepairs::Ready(Vec::new(), 0));
    };
    // A reprice may change only the amount. Its winning event then retains
    // the same provenance and order, so no historical lookup is necessary.
    let revised = changes
        .iter()
        .filter_map(|change| match change {
            Change::Event(event, -1) => Some(event.event.id.as_str()),
            _ => None,
        })
        .collect::<HashSet<_>>();
    if revised.is_empty() {
        return Ok(CitationRepairs::Ready(Vec::new(), 0));
    }
    let retained = changes
        .iter()
        .filter_map(|change| match change {
            Change::Event(event, 1) if revised.contains(event.event.id.as_str()) => event
                .source
                .as_ref()
                .map(|source| (event.event.id.as_str(), source)),
            _ => None,
        })
        .collect::<HashMap<_, _>>();
    let mut requests = Vec::<(Scope, String, CostSourceRef, Option<SourceOrder>)>::new();
    let mut requested = HashSet::new();
    let mut charge = staged.charged_bytes();
    for change in changes {
        let Change::Event(event, -1) = change else {
            continue;
        };
        let Some((key, reference)) = &event.source else {
            continue;
        };
        if key.starts_with("reported:") {
            continue;
        }
        let invocation = state
            .invocations
            .get(event.event.invocation_id.as_str())
            .ok_or_else(invalid_transition)?;
        let order = SourceOrder {
            source: Arc::from(invocation.run.source_id.as_str()),
            ordinal: invocation.ordinal,
            invocation: Arc::from(event.event.invocation_id.as_str()),
            occurred: Arc::from(event.event.occurred_at.as_str()),
            event: Arc::from(event.event.id.as_str()),
        };
        let retained_order = retained
            .get(event.event.id.as_str())
            .is_some_and(|(new_key, new_reference)| new_key == key && new_reference == reference)
            .then(|| order.clone());
        let mut scopes = vec![None];
        if invocation.owner.is_some() {
            scopes.push(invocation.owner.clone());
        }
        if let Some(agent) = event
            .event
            .agent_id
            .as_deref()
            .filter(|agent| invocation.owner.as_deref() != Some(*agent))
        {
            scopes.push(Some(Arc::from(agent)));
        }
        for scope in scopes {
            let Some(rollup) = state.scopes.get(&scope) else {
                continue;
            };
            if rollup
                .sources
                .get(key)
                .map(|book| book.contains_winner(reference, &order))
                .transpose()?
                .unwrap_or(false)
            {
                let variant = Citations::variant_key(reference)?;
                if requested.insert((scope.clone(), key.clone(), variant.clone())) {
                    // Requests and completed repairs coexist with the staged
                    // delta. Reserve their owned provenance/order buffers before
                    // retaining them; the query itself uses 150-row batches.
                    charge = charge.saturating_add(
                        4096 + Citations::charge(
                            &variant,
                            &Citation {
                                reference: reference.clone(),
                                count: 1,
                                winner: Some(order.clone()),
                                needs_repair: false,
                            },
                        ),
                    );
                    if charge > budget {
                        return Ok(CitationRepairs::Overflow(charge));
                    }
                    requests.push((
                        scope,
                        key.clone(),
                        reference.clone(),
                        retained_order.clone(),
                    ));
                }
            }
        }
    }
    let mut repairs = Vec::with_capacity(requests.len());
    for (scope, key, reference, retained_order) in requests {
        if let Some(order) = retained_order {
            repairs.push((scope, key, reference, Some(order)));
            continue;
        }
        let mut offset = 0i64;
        let winner = loop {
            // Narrow by immutable provenance ids and variant fields, then seek
            // in the exact source/attempt/invocation/occurrence/event order.
            // Only a removed winner needs this query; ordinary appends do not.
            let ids = db::usage_index_citation_event_ids_on_connection(
                &mut *connection,
                db::UsageIndexCitationEventIdsOnConnectionInput {
                    estimate_rowid: through.estimate_rowid,
                    event_rowid: through.event_rowid,
                    agent_id: scope.as_deref(),
                    rate_revision_id: &reference.rate_revision_id,
                    catalog_snapshot_id: &reference.catalog_snapshot_id,
                    formula_revision: &reference.formula_revision,
                    retrospective: reference.retrospective,
                    offset,
                },
            )
            .await?;
            if ids.is_empty() {
                break None;
            }
            offset += ids.len() as i64;
            let rows = db::SqliteDb::usage_events_by_ids(connection, &ids).await?;
            let mut effective = effective_events(connection, rows, through.estimate_rowid)
                .await?
                .into_iter()
                .map(|e| (e.event.id.clone(), e))
                .collect::<HashMap<_, _>>();
            let mut winner = None;
            for id in ids {
                let e = effective.remove(&id).ok_or_else(invalid_transition)?;
                if e.source.as_ref() == Some(&(key.clone(), reference.clone())) {
                    winner = Some(SourceOrder {
                        source: Arc::from(e.event.source_id.as_str()),
                        ordinal: e.event.attempt_ordinal,
                        invocation: Arc::from(e.event.invocation_id.as_str()),
                        occurred: Arc::from(e.event.occurred_at.as_str()),
                        event: Arc::from(e.event.id.as_str()),
                    });
                    break;
                }
            }
            if winner.is_some() {
                break winner;
            }
        };
        repairs.push((scope, key, reference, winner));
    }
    Ok(CitationRepairs::Ready(
        repairs,
        charge - staged.charged_bytes(),
    ))
}

enum Change {
    Invocation(Box<UsageInvocation>),
    Event(Box<EffectiveUsageEvent>, i64),
    Execution(db::UsageIndexExecutionRow),
    RemoveExecution(String),
}
impl Change {
    fn staging_bytes(&self) -> usize {
        let strings = match self {
            Self::Invocation(i) => {
                let mandatory = [
                    &i.id,
                    &i.source_id,
                    &i.domain_idempotency_key,
                    &i.pricing_selection_id,
                    &i.admitted_at,
                    &i.created_at,
                    &i.updated_at,
                ]
                .iter()
                .map(|s| s.capacity() + 16)
                .sum::<usize>();
                let optional = [
                    &i.owner_user_id,
                    &i.project_id,
                    &i.execution_id,
                    &i.task_id,
                    &i.candidate_key,
                    &i.admitted_provider_id,
                    &i.admitted_model_id,
                    &i.admitted_runtime_model,
                    &i.pricing_subject_id,
                    &i.pricing_subject_revision_id,
                    &i.subject_revision_digest,
                    &i.agent_id,
                    &i.profile_id,
                    &i.agent_name_snapshot,
                    &i.project_name_snapshot,
                    &i.executor_type,
                    &i.backend_kind,
                    &i.terminal_reason,
                    &i.started_at,
                    &i.settled_at,
                ]
                .iter()
                .filter_map(|s| s.as_ref())
                .map(|s| s.capacity() + 16)
                .sum::<usize>();
                mandatory + optional
            }
            Self::Event(e, _) => {
                let v = &e.event;
                let mandatory = [
                    &v.id,
                    &v.invocation_id,
                    &v.source_id,
                    &v.event_idempotency_key,
                    &v.source_report_id,
                    &v.legacy_counter_values_json,
                    &v.occurred_at,
                    &v.created_at,
                ]
                .iter()
                .map(|s| s.capacity() + 16)
                .sum::<usize>();
                let optional = [
                    &v.owner_user_id,
                    &v.project_id,
                    &v.execution_id,
                    &v.task_id,
                    &v.legacy_source_table,
                    &v.legacy_source_id,
                    &v.legacy_provider_raw,
                    &v.legacy_provider_sqlite_type,
                    &v.legacy_provider_sql_literal,
                    &v.legacy_model_raw,
                    &v.legacy_model_sqlite_type,
                    &v.legacy_model_sql_literal,
                    &v.legacy_cost_usd_raw,
                    &v.legacy_created_at_raw,
                    &v.legacy_project_owner_raw,
                    &v.provider_id,
                    &v.model_id,
                    &v.runtime_model,
                    &v.candidate_key,
                    &v.agent_id,
                    &v.profile_id,
                    &v.agent_name_snapshot,
                    &v.project_name_snapshot,
                    &v.executor_type,
                    &v.pricing_subject_revision_id,
                    &v.subject_revision_digest,
                    &v.selected_tier,
                    &v.rate_revision_id,
                    &v.catalog_snapshot_id,
                    &v.formula_revision,
                ]
                .iter()
                .filter_map(|s| s.as_ref())
                .map(|s| s.capacity() + 16)
                .sum::<usize>();
                mandatory
                    + optional
                    + e.source.as_ref().map_or(0, |(key, reference)| {
                        key.capacity()
                            + serde_json::to_string(reference)
                                .map_or(usize::MAX / 2, |s| s.len() * 2 + 256)
                    })
            }
            Self::Execution(row) => [
                db::UsageIndexExecutionTextField::Id,
                db::UsageIndexExecutionTextField::AgentId,
                db::UsageIndexExecutionTextField::Status,
                db::UsageIndexExecutionTextField::CreatedAt,
                db::UsageIndexExecutionTextField::UpdatedAt,
            ]
            .iter()
            .filter_map(|column| row.optional_text(*column).ok().flatten())
            .map(|s| s.len() + 16)
            .sum(),
            Self::RemoveExecution(id) => id.capacity(),
        };
        // Inline values, Vec capacity slack, SQL storage and allocator headers.
        // Variable-width buffers are charged in addition to the fixed allowance.
        4096 + strings
    }
    fn apply(self, state: &mut State) -> Result<()> {
        match self {
            Self::Invocation(row) => state.invocation(*row),
            Self::Event(event, sign) => state.event(*event, sign),
            Self::Execution(row) => state.execution(&row),
            Self::RemoveExecution(id) => {
                if let Some(previous) = state.executions.remove(id.as_str()) {
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
                Ok(())
            }
        }
    }
}
enum Staged {
    Cold {
        state: Box<State>,
        discard_charge: Option<usize>,
    },
    Delta {
        changes: Vec<Change>,
        bytes: usize,
        #[cfg(test)]
        audit: Audit,
    },
}
impl Staged {
    fn charged_bytes(&self) -> usize {
        match self {
            Self::Cold {
                state,
                discard_charge,
            } => discard_charge.unwrap_or_else(|| state.charged_bytes()),
            Self::Delta { bytes, .. } => *bytes,
        }
    }
    fn overflowed(&self, budget: usize) -> bool {
        self.charged_bytes() > budget
    }
    fn push(&mut self, change: Change, budget: usize) -> Result<()> {
        match self {
            Self::Cold {
                state,
                discard_charge,
            } => {
                if !state.fits_next(&change, budget) {
                    *discard_charge = Some(state.projected_charge(&change));
                    return Ok(());
                }
                change.apply(state)
            }
            Self::Delta { changes, bytes, .. } => {
                // A staged row includes strings/SQL row storage; its generous
                // allowance bounds even a burst of changes before publication.
                *bytes += change.staging_bytes();
                if *bytes <= budget {
                    changes.push(change);
                }
                Ok(())
            }
        }
    }
    #[cfg(test)]
    fn audit(&mut self) -> &mut Audit {
        match self {
            Self::Cold { state, .. } => &mut state.audit,
            Self::Delta { audit, .. } => audit,
        }
    }
}
impl State {
    fn fits_next(&self, change: &Change, budget: usize) -> bool {
        self.projected_charge(change) <= budget
    }
    fn projected_charge(&self, change: &Change) -> usize {
        fn growth<K, V>(len: usize, capacity: usize, add: usize) -> usize {
            if len + add > capacity {
                (capacity.max(3) + 1) * (std::mem::size_of::<(K, V)>() + 2) * 8 / 7
            } else {
                0
            }
        }
        let (inv, exec, runs) = match change {
            Change::Invocation(row) if !self.invocations.contains_key(row.id.as_str()) => (1, 0, 2),
            Change::Execution(row)
                if row
                    .id()
                    .is_ok_and(|id| !self.executions.contains_key(id.as_str())) =>
            {
                (0, 1, 2)
            }
            Change::Event(_, 1) => (0, 0, 1),
            _ => (0, 0, 0),
        };
        let growth =
            4096 + growth::<Arc<str>, Invocation>(
                self.invocations.len(),
                self.invocations.capacity(),
                inv,
            ) + growth::<Arc<str>, Execution>(
                self.executions.len(),
                self.executions.capacity(),
                exec,
            ) + growth::<ScopedRun, Run>(self.runs.len(), self.runs.capacity(), runs);
        self.charged_bytes()
            .saturating_add(growth.saturating_mul(3) / 2)
    }
}

async fn read_delta(
    staged: &mut Staged,
    connection: &mut sqlx::SqliteConnection,
    old: Watermarks,
    new: Watermarks,
    cold: bool,
    budget: usize,
) -> Result<()> {
    let mut position = old.invocation_rowid;
    while position < new.invocation_rowid {
        let rows = db::SqliteDb::usage_invocation_slice(connection, position, new.invocation_rowid)
            .await?;
        #[cfg(test)]
        {
            staged.audit().statements += 1;
            staged.audit().payload_rows += rows.len();
        }
        if rows.is_empty() {
            break;
        }
        for (rowid, row) in rows {
            position = rowid;
            staged.push(Change::Invocation(Box::new(row)), budget)?;
            if staged.overflowed(budget) {
                return Ok(());
            }
        }
    }
    let mut position = old.invocation_revision;
    while !cold && position < new.invocation_revision {
        let changes = db::usage_index_changed_invocations_on_connection(
            &mut *connection,
            position,
            new.invocation_revision,
            old.invocation_rowid,
        )
        .await?;
        #[cfg(test)]
        {
            staged.audit().statements += 1;
            staged.audit().payload_rows += changes.len();
        }
        if changes.is_empty() {
            break;
        }
        let ids = changes
            .iter()
            .map(|r| r.id())
            .collect::<std::result::Result<Vec<String>, _>>()?;
        position = changes.last().expect("changes").revision()?;
        let rows = db::SqliteDb::usage_invocations_by_ids(connection, &ids).await?;
        #[cfg(test)]
        {
            staged.audit().statements += 1;
            staged.audit().payload_rows += rows.len();
        }
        for row in rows {
            staged.push(Change::Invocation(Box::new(row)), budget)?;
            if staged.overflowed(budget) {
                return Ok(());
            }
        }
    }
    let mut position = old.event_rowid;
    while position < new.event_rowid {
        let rows = db::SqliteDb::usage_event_slice(connection, position, new.event_rowid).await?;
        #[cfg(test)]
        {
            staged.audit().statements += 1;
            staged.audit().payload_rows += rows.len();
        }
        if rows.is_empty() {
            break;
        }
        position = rows.last().expect("events").0;
        let events = rows.into_iter().map(|(_, e)| e).collect::<Vec<_>>();
        #[cfg(test)]
        {
            staged.audit().statements += events.len().div_ceil(150);
            staged.audit().payload_rows += events.len();
        }
        for event in effective_events(connection, events, new.estimate_rowid).await? {
            staged.push(Change::Event(Box::new(event), 1), budget)?;
            if staged.overflowed(budget) {
                return Ok(());
            }
        }
    }
    // A new revision can replace the effective amount/provenance of an old
    // event. Read that event at the old and new revision watermarks, subtract
    // the old contribution and add the new one. No invocation history walk.
    let mut position = old.estimate_rowid;
    let mut revised = BTreeSet::new();
    while !cold && position < new.estimate_rowid {
        let rows = db::usage_index_changed_estimates_on_connection(
            &mut *connection,
            position,
            new.estimate_rowid,
            old.event_rowid,
        )
        .await?;
        #[cfg(test)]
        {
            staged.audit().statements += 1;
            staged.audit().payload_rows += rows.len();
        }
        if rows.is_empty() {
            break;
        }
        for row in rows {
            position = row.revision_rowid()?;
            let id = row.id()?;
            let bytes = id.capacity() + 192;
            if revised.insert(id) {
                // The deduplicated revision set and its eventual Vec are
                // temporary delta storage too, not just decoded events.
                let overflowed = match staged {
                    Staged::Delta {
                        bytes: staged_bytes,
                        ..
                    } => {
                        *staged_bytes = staged_bytes.saturating_add(bytes);
                        *staged_bytes > budget
                    }
                    Staged::Cold { .. } => false,
                };
                if overflowed {
                    return Ok(());
                }
            }
        }
    }
    for chunk in revised.into_iter().collect::<Vec<_>>().chunks(400) {
        let events = db::SqliteDb::usage_events_by_ids(connection, chunk).await?;
        #[cfg(test)]
        {
            staged.audit().statements += 1 + 2 * events.len().div_ceil(150);
            staged.audit().payload_rows += 3 * events.len();
        }
        for event in effective_events(connection, events.clone(), old.estimate_rowid).await? {
            staged.push(Change::Event(Box::new(event), -1), budget)?;
            if staged.overflowed(budget) {
                return Ok(());
            }
        }
        for event in effective_events(connection, events, new.estimate_rowid).await? {
            staged.push(Change::Event(Box::new(event), 1), budget)?;
            if staged.overflowed(budget) {
                return Ok(());
            }
        }
    }
    let mut position = old.execution_rowid;
    while position < new.execution_rowid {
        let rows = db::usage_index_inserted_executions_on_connection(
            &mut *connection,
            position,
            new.execution_rowid,
        )
        .await?;
        #[cfg(test)]
        {
            staged.audit().statements += 1;
            staged.audit().payload_rows += rows.len();
        }
        if rows.is_empty() {
            break;
        }
        for row in rows {
            position = row.execution_rowid()?;
            staged.push(Change::Execution(row), budget)?;
            if staged.overflowed(budget) {
                return Ok(());
            }
        }
    }
    if old.execution_revision == new.execution_revision {
        return Ok(());
    }
    let mut position = old.execution_revision;
    let mut last_id = String::new();
    while !cold && position <= new.execution_revision {
        let rows = db::usage_index_changed_executions_on_connection(
            &mut *connection,
            db::UsageIndexChangedExecutionsOnConnectionInput {
                after_revision: position,
                cursor_revision: position,
                last_id: &last_id,
                through_revision: new.execution_revision,
                through_rowid: old.execution_rowid,
                old_revision: old.execution_revision,
            },
        )
        .await?;
        #[cfg(test)]
        {
            staged.audit().statements += 1;
            staged.audit().payload_rows += rows.len();
        }
        if rows.is_empty() {
            break;
        }
        for row in rows {
            position = row.revision()?;
            last_id = row.id()?;
            if let Some(id) = row.present_id()? {
                let _ = id;
                staged.push(Change::Execution(row), budget)?;
                if staged.overflowed(budget) {
                    return Ok(());
                }
            } else {
                staged.push(Change::RemoveExecution(last_id.clone()), budget)?;
            }
        }
    }
    Ok(())
}

mod queries;

#[cfg(test)]
mod tests;
