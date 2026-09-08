//! Shared public projections for usage-ledger observability surfaces.
//!
//! The analytics repository owns the broad account/Project aggregates.  The
//! execution, agent-detail, and Operations surfaces need the same vocabulary
//! at a smaller scope, however, so they use these deliberately narrow
//! ledger-backed helpers instead of falling back to a legacy aggregate table
//! or chat token JSON.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};

use api_types::{
    ActivityCounts, CostCoverage, CostCoverageReason, CostCoverageReasonCode, CostKind,
    CostSourceFreshness, CostSourceKind, CostSourceRef, CostSummary, MoneyAmount, TokenCounters,
    UsageAggregate, UsageAttribution, UsageBreakdown, UsageCostCoverage, UsageSurface,
    UsageTelemetryState,
};
use db::{
    CostCoverageReasonCode as DbCostCoverageReasonCode, CostEstimateRevisionState,
    RetrospectiveEstimateRepo, UsageCostKind, UsageEvent, UsageEventProvenanceKind,
    UsageInvocation, UsageInvocationLifecycle, UsageLedgerRepo, UsageSurface as DbUsageSurface,
    UsageTelemetryState as DbUsageTelemetryState,
};

use crate::{Result, ServiceError};

const NANOS_PER_USD: i128 = 1_000_000_000;

/// A domain run/turn that may have stopped before a provider invocation was
/// admitted.  Such rows are needed for truthful no-provider-call coverage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageDomainRun {
    pub surface: DbUsageSurface,
    pub source_id: String,
    /// A run which is active before its provider invocation is admitted still
    /// has pending cost coverage.  This keeps Operations from calling that
    /// state "no usage" merely because admission has not committed yet.
    pub pending: bool,
}

/// Stable internal key for a logical run/turn.  The database enum intentionally
/// does not expose ordering/hash semantics, so ordering is defined explicitly
/// here rather than coupling projections to the model's derive choices.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RunKey {
    surface: DbUsageSurface,
    source_id: String,
}

impl Hash for RunKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        surface_order(self.surface).hash(state);
        self.source_id.hash(state);
    }
}

impl Ord for RunKey {
    fn cmp(&self, other: &Self) -> Ordering {
        surface_order(self.surface)
            .cmp(&surface_order(other.surface))
            .then_with(|| self.source_id.cmp(&other.source_id))
    }
}

impl PartialOrd for RunKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone)]
struct EffectiveUsageEvent {
    event: UsageEvent,
    source: Option<(String, CostSourceRef)>,
}

/// Aggregate one ledger source (normally one execution).  The source route
/// performs its existence/visibility check before calling this helper.
pub async fn usage_aggregate_for_source(
    db: &db::SqliteDb,
    source_id: &str,
) -> Result<UsageAggregate> {
    usage_aggregate_for_source_state(db, source_id, false).await
}

/// Aggregate one source while preserving an externally known active state.
/// An execution can be running before its first invocation row is committed;
/// callers such as Operations pass `pending = true` for that narrow window.
pub async fn usage_aggregate_for_source_state(
    db: &db::SqliteDb,
    source_id: &str,
    pending: bool,
) -> Result<UsageAggregate> {
    let invocations = UsageLedgerRepo::list_usage_invocations_for_source(db, source_id).await?;
    let mut events_by_invocation = HashMap::new();
    for invocation in &invocations {
        events_by_invocation.insert(
            invocation.id.clone(),
            list_effective_usage_events(db, invocation).await?,
        );
    }
    let surface = invocations
        .first()
        .map(|invocation| invocation.surface)
        .unwrap_or(DbUsageSurface::TaskExecution);
    aggregate_usage_with_sources(
        &invocations,
        &events_by_invocation,
        &[UsageDomainRun {
            surface,
            source_id: source_id.to_owned(),
            pending,
        }],
    )
}

/// Return the ordered, per-event public projection for one source (an
/// execution, chat turn, or inquiry).  Invocations are ordered by the
/// repository's attempt order and events by occurrence/id.
pub async fn usage_breakdowns_for_source(
    db: &db::SqliteDb,
    source_id: &str,
) -> Result<Vec<UsageBreakdown>> {
    let invocations = UsageLedgerRepo::list_usage_invocations_for_source(db, source_id).await?;
    let mut rows = Vec::new();
    for invocation in invocations {
        let events = list_effective_usage_events(db, &invocation).await?;
        rows.extend(usage_breakdowns_for_effective_invocation(
            &invocation,
            &events,
        )?);
    }
    Ok(rows)
}

/// Overlay the latest applied retrospective estimate without mutating the
/// immutable usage event. The broad analytics repository performs this join
/// in its dataset query; these smaller public projections use the repository
/// list methods and apply the same effective-cost rule per event.
async fn list_effective_usage_events(
    db: &db::SqliteDb,
    invocation: &UsageInvocation,
) -> Result<Vec<EffectiveUsageEvent>> {
    let events = UsageLedgerRepo::list_usage_events_for_invocation(db, &invocation.id).await?;
    let mut effective = Vec::with_capacity(events.len());
    for event in events {
        let revisions =
            RetrospectiveEstimateRepo::list_cost_estimate_revisions_for_event(db, &event.id)
                .await?;
        if let Some(revision) = revisions
            .into_iter()
            .find(|revision| revision.state == CostEstimateRevisionState::Applied)
        {
            if event.cost_kind == UsageCostKind::ProviderReported {
                return Err(invalid_transition());
            }
            let Some(estimated_nano_usd) = revision.estimated_nano_usd else {
                return Err(invalid_transition());
            };
            let mut event = event;
            event.estimated_nano_usd = Some(estimated_nano_usd);
            event.cost_kind = UsageCostKind::Estimated;
            event.rate_revision_id = revision.rate_revision_id.or(event.rate_revision_id);
            event.catalog_snapshot_id = revision.catalog_snapshot_id.or(event.catalog_snapshot_id);
            event.formula_revision = revision.formula_revision.or(event.formula_revision);
            event.retrospective = revision.retrospective;
            let source_metadata =
                load_event_source_metadata(db, invocation, &event, Some(&revision.id)).await?;
            effective.push(EffectiveUsageEvent {
                source: event_source_with_metadata(&event, Some(&source_metadata))?,
                event,
            });
        } else {
            let source_metadata = load_event_source_metadata(db, invocation, &event, None).await?;
            effective.push(EffectiveUsageEvent {
                source: event_source_with_metadata(&event, Some(&source_metadata))?,
                event,
            });
        }
    }
    Ok(effective)
}

#[derive(Debug, Clone)]
struct EventSourceMetadata {
    source_kind: Option<CostSourceKind>,
    rate_revision_id: Option<String>,
    catalog_snapshot_id: Option<String>,
    catalog_digest: Option<String>,
    effective_at: Option<String>,
    fetched_at: Option<String>,
    freshness: Option<CostSourceFreshness>,
}

/// Read the immutable pricing provenance attached to an invocation/event.
/// Selection freshness is admission-time truth; retrospective estimates use
/// the immutable preview freshness instead. Rate/snapshot fields are joined by
/// their frozen IDs and never re-resolved from current mutable bindings.
async fn load_event_source_metadata(
    db: &db::SqliteDb,
    invocation: &UsageInvocation,
    event: &UsageEvent,
    applied_revision_id: Option<&str>,
) -> Result<EventSourceMetadata> {
    let row = sqlx::query(
        "SELECT
             ps.source_kind AS selection_source_kind,
             ps.catalog_freshness AS selection_catalog_freshness,
             ps.rate_revision_id AS selection_rate_revision_id,
             ps.catalog_snapshot_id AS selection_catalog_snapshot_id,
             r.source_kind AS rate_source_kind,
             r.catalog_snapshot_id AS rate_catalog_snapshot_id,
             r.effective_at AS rate_effective_at,
             c.revision_digest AS catalog_digest,
             c.fetched_at AS catalog_fetched_at,
             ep.catalog_freshness AS estimate_catalog_freshness
         FROM usage_invocation i
         LEFT JOIN pricing_selection ps ON ps.id = i.pricing_selection_id
         LEFT JOIN cost_estimate_revision er ON er.id = ?
         LEFT JOIN cost_estimation_run erun ON erun.id = er.run_id
         LEFT JOIN cost_estimation_preview ep ON ep.id = erun.preview_id
         LEFT JOIN pricing_rate_revision r
           ON r.id = COALESCE(?, ps.rate_revision_id)
         LEFT JOIN pricing_catalog_snapshot c
           ON c.id = COALESCE(?, r.catalog_snapshot_id, ps.catalog_snapshot_id)
         WHERE i.id = ?",
    )
    .bind(applied_revision_id)
    .bind(event.rate_revision_id.as_deref())
    .bind(event.catalog_snapshot_id.as_deref())
    .bind(&invocation.id)
    .fetch_one(db.pool())
    .await?;

    let rate_source_kind: Option<String> = sqlx::Row::try_get(&row, "rate_source_kind")?;
    let selection_source_kind: Option<String> = sqlx::Row::try_get(&row, "selection_source_kind")?;
    let source_kind = rate_source_kind
        .or(selection_source_kind)
        .map(|value| match value.as_str() {
            "models_dev_catalog" => Ok(CostSourceKind::ModelsDevCatalog),
            "manual_override" => Ok(CostSourceKind::ManualOverride),
            _ => Err(invalid_transition()),
        })
        .transpose()?;
    let selection_freshness: Option<String> =
        sqlx::Row::try_get(&row, "selection_catalog_freshness")?;
    let estimate_freshness: Option<String> =
        sqlx::Row::try_get(&row, "estimate_catalog_freshness")?;
    let freshness = estimate_freshness
        .or(selection_freshness)
        .map(|value| parse_source_freshness(&value))
        .transpose()?;
    let selection_rate_revision_id: Option<String> =
        sqlx::Row::try_get(&row, "selection_rate_revision_id")?;
    let selection_catalog_snapshot_id: Option<String> =
        sqlx::Row::try_get(&row, "selection_catalog_snapshot_id")?;
    let rate_catalog_snapshot_id: Option<String> =
        sqlx::Row::try_get(&row, "rate_catalog_snapshot_id")?;
    let rate_revision_id = event
        .rate_revision_id
        .clone()
        .or(selection_rate_revision_id);
    let catalog_snapshot_id = event
        .catalog_snapshot_id
        .clone()
        .or(rate_catalog_snapshot_id)
        .or(selection_catalog_snapshot_id);
    let catalog_digest: Option<String> = sqlx::Row::try_get(&row, "catalog_digest")?;
    let effective_at: Option<String> = sqlx::Row::try_get(&row, "rate_effective_at")?;
    let fetched_at: Option<String> = sqlx::Row::try_get(&row, "catalog_fetched_at")?;
    Ok(EventSourceMetadata {
        source_kind,
        rate_revision_id,
        catalog_snapshot_id,
        catalog_digest,
        effective_at,
        fetched_at,
        freshness,
    })
}

fn parse_source_freshness(value: &str) -> Result<CostSourceFreshness> {
    match value {
        "fresh" => Ok(CostSourceFreshness::Fresh),
        "stale" => Ok(CostSourceFreshness::Stale),
        "refresh_failed" => Ok(CostSourceFreshness::RefreshFailed),
        "not_applicable" => Ok(CostSourceFreshness::NotApplicable),
        // Unknown historical values are not evidence of freshness. Keep the
        // usable amount visible and mark the provenance conservatively.
        _ => Ok(CostSourceFreshness::RefreshFailed),
    }
}

/// Shape one invocation as one row per immutable usage event.  Invocations
/// without an event remain visible as a row with a null `usage_event_id` so
/// active/pending and terminal-unsettled attempts cannot disappear from
/// execution observability.
pub fn usage_breakdowns_for_invocation(
    invocation: &UsageInvocation,
    events: &[UsageEvent],
) -> Result<Vec<UsageBreakdown>> {
    let effective_events = events
        .iter()
        .cloned()
        .map(|event| {
            let source = event_source(&event)?;
            Ok(EffectiveUsageEvent { event, source })
        })
        .collect::<Result<Vec<_>>>()?;
    usage_breakdowns_for_effective_invocation(invocation, &effective_events)
}

fn usage_breakdowns_for_effective_invocation(
    invocation: &UsageInvocation,
    events: &[EffectiveUsageEvent],
) -> Result<Vec<UsageBreakdown>> {
    if events.is_empty() {
        return Ok(vec![UsageBreakdown {
            invocation_id: invocation.id.clone(),
            usage_event_id: None,
            surface: api_surface(invocation.surface),
            telemetry_state: api_telemetry_state(invocation_state(invocation, None)),
            attribution: invocation_attribution(invocation, None),
            counters: None,
            context_tokens: None,
            selected_tier: None,
            occurred_at: invocation
                .settled_at
                .clone()
                .or_else(|| invocation.started_at.clone())
                .unwrap_or_else(|| invocation.admitted_at.clone()),
            cost: invocation_cost_summary(invocation),
        }]);
    }

    events
        .iter()
        .map(|effective| {
            let event = &effective.event;
            Ok::<_, ServiceError>(UsageBreakdown {
                invocation_id: invocation.id.clone(),
                usage_event_id: Some(event.id.clone()),
                surface: api_surface(event.surface),
                telemetry_state: api_telemetry_state(invocation_state(invocation, Some(event))),
                attribution: invocation_attribution(invocation, Some(event)),
                counters: event_counters(event)?,
                context_tokens: event.context_tokens,
                selected_tier: event.selected_tier.clone(),
                occurred_at: event.occurred_at.clone(),
                cost: event_cost_summary(invocation, event, effective.source.as_ref())?,
            })
        })
        .collect()
}

/// Aggregate a set of ledger invocations while retaining the separate domain
/// run/turn denominator.  `domain_runs` may include runs with no invocation.
pub fn aggregate_usage(
    invocations: &[UsageInvocation],
    events_by_invocation: &HashMap<String, Vec<UsageEvent>>,
    domain_runs: &[UsageDomainRun],
) -> Result<UsageAggregate> {
    let effective_events_by_invocation = events_by_invocation
        .iter()
        .map(|(invocation_id, events)| {
            let events = events
                .iter()
                .cloned()
                .map(|event| {
                    let source = event_source(&event)?;
                    Ok(EffectiveUsageEvent { event, source })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((invocation_id.clone(), events))
        })
        .collect::<Result<HashMap<_, _>>>()?;
    aggregate_usage_with_sources(invocations, &effective_events_by_invocation, domain_runs)
}

fn aggregate_usage_with_sources(
    invocations: &[UsageInvocation],
    events_by_invocation: &HashMap<String, Vec<EffectiveUsageEvent>>,
    domain_runs: &[UsageDomainRun],
) -> Result<UsageAggregate> {
    let mut run_keys = BTreeSet::new();
    let mut pending_run_keys = BTreeSet::new();
    for run in domain_runs {
        let key = RunKey {
            surface: run.surface,
            source_id: run.source_id.clone(),
        };
        run_keys.insert(key.clone());
        if run.pending {
            pending_run_keys.insert(key);
        }
    }

    let mut counts = ActivityCounts {
        task_execution_count: 0,
        chat_turn_count: 0,
        inquiry_count: 0,
        provider_attempt_count: 0,
    };
    let mut tokens = [0_i64; 4];
    let mut priced_tokens = [0_i64; 4];
    let mut unpriced_tokens = [0_i64; 4];
    let mut provider_reported_nanos = None;
    let mut estimated_nanos = None;
    let mut total_provider_attempts = 0_i64;
    let mut settled_provider_attempts = 0_i64;
    let mut pending_provider_attempts = 0_i64;
    let mut unsettled_provider_attempts = 0_i64;
    let mut metered_provider_attempts = 0_i64;
    let mut unmetered_provider_attempts = 0_i64;
    let mut costed_provider_attempts = 0_i64;
    let mut unpriced_provider_attempts = 0_i64;
    let mut sources = BTreeMap::<String, CostSourceRef>::new();
    let mut reasons = BTreeMap::<usize, ReasonAccumulator>::new();
    let mut attempts_by_run = BTreeMap::<RunKey, Vec<AttemptCoverage>>::new();

    for invocation in invocations {
        run_keys.insert(RunKey {
            surface: invocation.surface,
            source_id: invocation.source_id.clone(),
        });
        counts.provider_attempt_count = counts
            .provider_attempt_count
            .checked_add(1)
            .ok_or_else(invalid_transition)?;
        total_provider_attempts = total_provider_attempts
            .checked_add(1)
            .ok_or_else(invalid_transition)?;

        let invocation_pending = !matches!(
            invocation.lifecycle,
            UsageInvocationLifecycle::Settled | UsageInvocationLifecycle::Unsettled
        );
        let invocation_unsettled = invocation.lifecycle == UsageInvocationLifecycle::Unsettled;
        if invocation_pending {
            pending_provider_attempts = pending_provider_attempts
                .checked_add(1)
                .ok_or_else(invalid_transition)?;
            add_reason(
                &mut reasons,
                CostCoverageReasonCode::Pending,
                invocation,
                [0; 4],
            )?;
        } else if invocation_unsettled {
            unsettled_provider_attempts = unsettled_provider_attempts
                .checked_add(1)
                .ok_or_else(invalid_transition)?;
            add_reason(
                &mut reasons,
                CostCoverageReasonCode::Unsettled,
                invocation,
                [0; 4],
            )?;
        } else {
            settled_provider_attempts = settled_provider_attempts
                .checked_add(1)
                .ok_or_else(invalid_transition)?;
        }

        let metered = invocation.telemetry_state == DbUsageTelemetryState::Metered;
        if metered {
            metered_provider_attempts = metered_provider_attempts
                .checked_add(1)
                .ok_or_else(invalid_transition)?;
        } else if !invocation_pending && !invocation_unsettled {
            unmetered_provider_attempts = unmetered_provider_attempts
                .checked_add(1)
                .ok_or_else(invalid_transition)?;
        }

        let events = events_by_invocation
            .get(&invocation.id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let mut attempt = AttemptCoverage {
            pending: invocation_pending,
            unsettled: invocation_unsettled,
            metered,
            costed: !events.is_empty() && !invocation_unsettled,
        };
        if events.is_empty() {
            attempt.costed = false;
            if !invocation_pending && !invocation_unsettled {
                add_reason(
                    &mut reasons,
                    CostCoverageReasonCode::Unmetered,
                    invocation,
                    [0; 4],
                )?;
            }
        }

        for effective in events {
            let event = &effective.event;
            let counters = event_counter_array(event)?;
            for (slot, value) in tokens.iter_mut().zip(counters) {
                *slot = slot.checked_add(value).ok_or_else(invalid_transition)?;
            }

            let (reported, estimated) = event_amounts(event)?;
            let event_costed = !invocation_unsettled
                && (reported.is_some() || estimated.is_some())
                && matches!(
                    event.cost_kind,
                    UsageCostKind::ProviderReported
                        | UsageCostKind::Estimated
                        | UsageCostKind::None
                );
            if let Some(amount) = reported {
                add_nanos(&mut provider_reported_nanos, amount)?;
            }
            if let Some(amount) = estimated {
                add_nanos(&mut estimated_nanos, amount)?;
            }
            if event_costed {
                for (slot, value) in priced_tokens.iter_mut().zip(counters) {
                    *slot = slot.checked_add(value).ok_or_else(invalid_transition)?;
                }
                if let Some((key, source)) = effective.source.as_ref() {
                    sources.insert(key.clone(), source.clone());
                }
            } else {
                attempt.costed = false;
                for (slot, value) in unpriced_tokens.iter_mut().zip(counters) {
                    *slot = slot.checked_add(value).ok_or_else(invalid_transition)?;
                }
                let reason = event
                    .coverage_reason_code
                    .map(api_reason)
                    .unwrap_or_else(|| {
                        if event.telemetry_state == DbUsageTelemetryState::Unmetered {
                            CostCoverageReasonCode::Unmetered
                        } else {
                            CostCoverageReasonCode::MissingRate
                        }
                    });
                add_reason(&mut reasons, reason, invocation, counters)?;
            }
        }

        if attempt.costed {
            costed_provider_attempts = costed_provider_attempts
                .checked_add(1)
                .ok_or_else(invalid_transition)?;
        } else {
            unpriced_provider_attempts = unpriced_provider_attempts
                .checked_add(1)
                .ok_or_else(invalid_transition)?;
        }
        attempts_by_run
            .entry(RunKey {
                surface: invocation.surface,
                source_id: invocation.source_id.clone(),
            })
            .or_default()
            .push(attempt);
    }

    for key in &run_keys {
        match key.surface {
            DbUsageSurface::TaskExecution => {
                counts.task_execution_count = counts
                    .task_execution_count
                    .checked_add(1)
                    .ok_or_else(invalid_transition)?;
            }
            DbUsageSurface::ProjectChat
            | DbUsageSurface::MainChat
            | DbUsageSurface::GenesisChat => {
                counts.chat_turn_count = counts
                    .chat_turn_count
                    .checked_add(1)
                    .ok_or_else(invalid_transition)?;
            }
            DbUsageSurface::MainInquiry => {
                counts.inquiry_count = counts
                    .inquiry_count
                    .checked_add(1)
                    .ok_or_else(invalid_transition)?;
            }
        }
    }

    let mut pending_runs = 0_i64;
    let mut no_provider_call_runs = 0_i64;
    let mut fully_metered_runs = 0_i64;
    let mut fully_costed_runs = 0_i64;
    let mut partially_costed_runs = 0_i64;
    let mut unavailable_cost_runs = 0_i64;
    for key in &run_keys {
        let attempts = attempts_by_run.get(key).map(Vec::as_slice).unwrap_or(&[]);
        if attempts.is_empty() {
            if pending_run_keys.contains(key) {
                pending_runs = pending_runs.checked_add(1).ok_or_else(invalid_transition)?;
            } else {
                no_provider_call_runs = no_provider_call_runs
                    .checked_add(1)
                    .ok_or_else(invalid_transition)?;
            }
            continue;
        }
        let has_pending = attempts.iter().any(|attempt| attempt.pending);
        let has_unsettled = attempts.iter().any(|attempt| attempt.unsettled);
        let costed = attempts.iter().filter(|attempt| attempt.costed).count();
        if has_pending {
            pending_runs = pending_runs.checked_add(1).ok_or_else(invalid_transition)?;
        } else if !has_unsettled && attempts.iter().all(|attempt| attempt.metered) {
            fully_metered_runs = fully_metered_runs
                .checked_add(1)
                .ok_or_else(invalid_transition)?;
        }
        if has_pending {
            // Pending takes precedence over every terminal category.
        } else if costed == attempts.len() && !has_unsettled {
            fully_costed_runs = fully_costed_runs
                .checked_add(1)
                .ok_or_else(invalid_transition)?;
        } else if costed > 0 {
            partially_costed_runs = partially_costed_runs
                .checked_add(1)
                .ok_or_else(invalid_transition)?;
        } else {
            unavailable_cost_runs = unavailable_cost_runs
                .checked_add(1)
                .ok_or_else(invalid_transition)?;
        }
    }

    let coverage = if !pending_run_keys.is_empty() && pending_runs > 0 {
        CostCoverage::Pending
    } else if total_provider_attempts == 0 {
        CostCoverage::NoUsage
    } else if pending_provider_attempts > 0 {
        CostCoverage::Pending
    } else if unsettled_provider_attempts > 0 {
        if costed_provider_attempts > 0 {
            CostCoverage::Partial
        } else {
            CostCoverage::Unavailable
        }
    } else if costed_provider_attempts == total_provider_attempts {
        CostCoverage::Complete
    } else if costed_provider_attempts > 0 {
        CostCoverage::Partial
    } else {
        CostCoverage::Unavailable
    };
    let kind = cost_kind(
        provider_reported_nanos.is_some(),
        estimated_nanos.is_some(),
        total_provider_attempts,
        coverage,
        costed_provider_attempts,
    );
    // The zero defaults are only for adding two optional accumulator slots.
    // Presence is retained separately below, so an all-null amount never
    // becomes a known zero in the public summary.
    let known_total = provider_reported_nanos
        .unwrap_or(0)
        .checked_add(estimated_nanos.unwrap_or(0))
        .ok_or_else(invalid_transition)?;

    let reason_items: Vec<_> = reasons
        .into_values()
        .map(|data| {
            Ok::<_, ServiceError>(CostCoverageReason {
                code: data.code,
                run_or_turn_count: count_i64(data.run_keys.len())?,
                provider_attempt_count: count_i64(data.invocation_ids.len())?,
                tokens: token_counters(data.tokens),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut source_items: Vec<_> = sources.into_values().collect();
    source_items.sort_by(|left, right| {
        source_order(left.source_kind)
            .cmp(&source_order(right.source_kind))
            .then_with(|| left.rate_revision_id.cmp(&right.rate_revision_id))
            .then_with(|| left.catalog_snapshot_id.cmp(&right.catalog_snapshot_id))
    });

    Ok(UsageAggregate {
        counts,
        tokens: token_counters(tokens),
        cost: CostSummary {
            kind,
            coverage,
            provider_reported: money(provider_reported_nanos),
            estimated: money(estimated_nanos),
            known_subtotal: (provider_reported_nanos.is_some() || estimated_nanos.is_some())
                .then(|| money(Some(known_total)))
                .flatten(),
            complete_total: money((coverage == CostCoverage::Complete).then_some(known_total)),
            usage_coverage: UsageCostCoverage {
                total_runs_or_turns: count_i64(run_keys.len())?,
                pending_runs_or_turns: pending_runs,
                no_provider_call_runs_or_turns: no_provider_call_runs,
                fully_metered_runs_or_turns: fully_metered_runs,
                fully_costed_runs_or_turns: fully_costed_runs,
                partially_costed_runs_or_turns: partially_costed_runs,
                unavailable_cost_runs_or_turns: unavailable_cost_runs,
                total_provider_attempts,
                settled_provider_attempts,
                pending_provider_attempts,
                unsettled_provider_attempts,
                metered_provider_attempts,
                unmetered_provider_attempts,
                costed_provider_attempts,
                unpriced_provider_attempts,
                priced_tokens: token_counters(priced_tokens),
                unpriced_tokens: token_counters(unpriced_tokens),
                reasons: reason_items,
            },
            sources: source_items,
        },
    })
}

/// Collect all ledger rows attributed to an Agent identity and build its
/// shared aggregate.  This is intentionally scoped to immutable invocation
/// and event attribution; mutable `agent_current`/chat token JSON are not
/// used as accounting authority.
pub async fn usage_aggregate_for_agent(
    db: &db::SqliteDb,
    identity_id: &str,
) -> Result<UsageAggregate> {
    let source_rows = sqlx::query(
        "SELECT DISTINCT source_id FROM usage_invocation WHERE agent_id = ?
         UNION SELECT DISTINCT source_id FROM usage_event WHERE agent_id = ?
         ORDER BY source_id ASC",
    )
    .bind(identity_id)
    .bind(identity_id)
    .fetch_all(db.pool())
    .await?;
    let mut invocations = Vec::new();
    let mut events_by_invocation = HashMap::new();
    for row in source_rows {
        let source_id: String = sqlx::Row::try_get(&row, "source_id")?;
        for invocation in UsageLedgerRepo::list_usage_invocations_for_source(db, &source_id).await?
        {
            let events = list_effective_usage_events(db, &invocation).await?;
            let invocation_matches = invocation.agent_id.as_deref() == Some(identity_id);
            let matching_events = events
                .iter()
                .any(|effective| effective.event.agent_id.as_deref() == Some(identity_id));
            if !invocation_matches && !matching_events {
                continue;
            }
            let events = if invocation_matches {
                events
            } else {
                events
                    .into_iter()
                    .filter(|effective| effective.event.agent_id.as_deref() == Some(identity_id))
                    .collect()
            };
            events_by_invocation.insert(invocation.id.clone(), events);
            invocations.push(invocation);
        }
    }
    let mut domain_runs = invocations
        .iter()
        .map(|invocation| UsageDomainRun {
            surface: invocation.surface,
            source_id: invocation.source_id.clone(),
            pending: false,
        })
        .collect::<Vec<_>>();
    let execution_rows = sqlx::query("SELECT id, status FROM execution WHERE agent_id = ?")
        .bind(identity_id)
        .fetch_all(db.pool())
        .await?;
    for row in execution_rows {
        let source_id: String = sqlx::Row::try_get(&row, "id")?;
        let status: String = sqlx::Row::try_get(&row, "status")?;
        domain_runs.push(UsageDomainRun {
            surface: DbUsageSurface::TaskExecution,
            source_id,
            pending: status == "running",
        });
    }
    aggregate_usage_with_sources(&invocations, &events_by_invocation, &domain_runs)
}

/// Collect the execution attempts belonging to one Task.  Runtime rows are
/// included as domain runs even when no invocation was admitted, preserving
/// no-provider and active/pending coverage in Task observability.
pub async fn usage_aggregate_for_task(db: &db::SqliteDb, task_id: &str) -> Result<UsageAggregate> {
    let execution_rows = sqlx::query("SELECT id, status FROM execution WHERE task_id = ?")
        .bind(task_id)
        .fetch_all(db.pool())
        .await?;
    let mut execution_ids = BTreeSet::new();
    let mut domain_runs = Vec::with_capacity(execution_rows.len());
    for row in execution_rows {
        let id: String = sqlx::Row::try_get(&row, "id")?;
        let status: String = sqlx::Row::try_get(&row, "status")?;
        execution_ids.insert(id.clone());
        domain_runs.push(UsageDomainRun {
            surface: DbUsageSurface::TaskExecution,
            source_id: id,
            pending: status == "running",
        });
    }

    let source_rows = sqlx::query(
        "SELECT DISTINCT source_id FROM usage_invocation
         WHERE task_id = ?
            OR execution_id IN (SELECT id FROM execution WHERE task_id = ?)
            OR source_id IN (SELECT id FROM execution WHERE task_id = ?)
         UNION SELECT DISTINCT source_id FROM usage_event
         WHERE task_id = ?
            OR execution_id IN (SELECT id FROM execution WHERE task_id = ?)
            OR source_id IN (SELECT id FROM execution WHERE task_id = ?)
         ORDER BY source_id ASC",
    )
    .bind(task_id)
    .bind(task_id)
    .bind(task_id)
    .bind(task_id)
    .bind(task_id)
    .bind(task_id)
    .fetch_all(db.pool())
    .await?;
    let mut invocations = Vec::new();
    let mut events_by_invocation = HashMap::new();
    for row in source_rows {
        let source_id: String = sqlx::Row::try_get(&row, "source_id")?;
        for invocation in UsageLedgerRepo::list_usage_invocations_for_source(db, &source_id).await?
        {
            let invocation_matches = invocation.task_id.as_deref() == Some(task_id)
                || execution_ids.contains(&invocation.source_id);
            let events = list_effective_usage_events(db, &invocation).await?;
            let events = if invocation_matches {
                events
            } else {
                events
                    .into_iter()
                    .filter(|effective| {
                        effective.event.task_id.as_deref() == Some(task_id)
                            || execution_ids.contains(&effective.event.source_id)
                    })
                    .collect()
            };
            if !invocation_matches && events.is_empty() {
                continue;
            }
            events_by_invocation.insert(invocation.id.clone(), events);
            invocations.push(invocation);
        }
    }
    aggregate_usage_with_sources(&invocations, &events_by_invocation, &domain_runs)
}

/// Collect all ledger rows for the operator's global Operations summary.
/// Operations is admin-only at the route boundary, so this deliberately
/// mirrors the whole-account projection rather than accepting an unscoped
/// caller-provided identity.
pub async fn usage_aggregate_for_operations(db: &db::SqliteDb) -> Result<UsageAggregate> {
    let source_rows = sqlx::query(
        "SELECT DISTINCT source_id FROM usage_invocation
         UNION SELECT DISTINCT source_id FROM usage_event
         ORDER BY source_id ASC",
    )
    .fetch_all(db.pool())
    .await?;
    let mut invocations = Vec::new();
    let mut events_by_invocation = HashMap::new();
    for row in source_rows {
        let source_id: String = sqlx::Row::try_get(&row, "source_id")?;
        for invocation in UsageLedgerRepo::list_usage_invocations_for_source(db, &source_id).await?
        {
            let events = list_effective_usage_events(db, &invocation).await?;
            events_by_invocation.insert(invocation.id.clone(), events);
            invocations.push(invocation);
        }
    }
    let mut domain_runs = invocations
        .iter()
        .map(|invocation| UsageDomainRun {
            surface: invocation.surface,
            source_id: invocation.source_id.clone(),
            pending: false,
        })
        .collect::<Vec<_>>();
    let execution_rows = sqlx::query("SELECT id, status FROM execution")
        .fetch_all(db.pool())
        .await?;
    for row in execution_rows {
        let source_id: String = sqlx::Row::try_get(&row, "id")?;
        let status: String = sqlx::Row::try_get(&row, "status")?;
        domain_runs.push(UsageDomainRun {
            surface: DbUsageSurface::TaskExecution,
            source_id,
            pending: status == "running",
        });
    }
    aggregate_usage_with_sources(&invocations, &events_by_invocation, &domain_runs)
}

fn event_cost_summary(
    invocation: &UsageInvocation,
    event: &UsageEvent,
    source: Option<&(String, CostSourceRef)>,
) -> Result<CostSummary> {
    let (reported, estimated) = event_amounts(event)?;
    let invocation_unsettled = invocation.lifecycle == UsageInvocationLifecycle::Unsettled;
    let pending = !matches!(
        invocation.lifecycle,
        UsageInvocationLifecycle::Settled | UsageInvocationLifecycle::Unsettled
    );
    let event_costed = !invocation_unsettled && (reported.is_some() || estimated.is_some());
    let coverage = if pending {
        CostCoverage::Pending
    } else if invocation_unsettled {
        CostCoverage::Unavailable
    } else if event_costed {
        CostCoverage::Complete
    } else if event.coverage_reason_code.is_some_and(|reason| {
        matches!(
            reason,
            db::CostCoverageReasonCode::MissingRate
                | db::CostCoverageReasonCode::UnresolvedTier
                | db::CostCoverageReasonCode::IdentityMismatch
        )
    }) && event_counter_array(event)?.iter().any(|value| *value > 0)
    {
        CostCoverage::Partial
    } else {
        CostCoverage::Unavailable
    };
    let kind = cost_kind(
        reported.is_some(),
        estimated.is_some(),
        1,
        coverage,
        i64::from(event_costed),
    );
    // As above, these defaults are local arithmetic identities only; the
    // `is_some` checks on `known_subtotal` preserve null versus explicit zero.
    let known_total = reported
        .unwrap_or(0)
        .checked_add(estimated.unwrap_or(0))
        .ok_or_else(invalid_transition)?;
    let mut reasons = Vec::new();
    if let Some(code) = event_reason(event, pending, invocation_unsettled) {
        reasons.push(CostCoverageReason {
            code,
            run_or_turn_count: 1,
            provider_attempt_count: 1,
            tokens: token_counters(event_counter_array(event)?),
        });
    }
    let sources = source
        .map(|(_, source)| vec![source.clone()])
        .unwrap_or_default();
    let counters = event_counter_array(event)?;
    Ok(CostSummary {
        kind,
        coverage,
        provider_reported: money(reported),
        estimated: money(estimated),
        known_subtotal: (reported.is_some() || estimated.is_some())
            .then(|| money(Some(known_total)))
            .flatten(),
        complete_total: money((coverage == CostCoverage::Complete).then_some(known_total)),
        usage_coverage: UsageCostCoverage {
            total_runs_or_turns: 1,
            pending_runs_or_turns: i64::from(pending),
            no_provider_call_runs_or_turns: 0,
            fully_metered_runs_or_turns: i64::from(
                !pending
                    && !invocation_unsettled
                    && event.telemetry_state == DbUsageTelemetryState::Metered,
            ),
            fully_costed_runs_or_turns: i64::from(event_costed && !pending),
            partially_costed_runs_or_turns: i64::from(coverage == CostCoverage::Partial),
            unavailable_cost_runs_or_turns: i64::from(coverage == CostCoverage::Unavailable),
            total_provider_attempts: 1,
            settled_provider_attempts: i64::from(!pending && !invocation_unsettled),
            pending_provider_attempts: i64::from(pending),
            unsettled_provider_attempts: i64::from(invocation_unsettled),
            metered_provider_attempts: i64::from(
                event.telemetry_state == DbUsageTelemetryState::Metered,
            ),
            unmetered_provider_attempts: i64::from(
                event.telemetry_state != DbUsageTelemetryState::Metered
                    && !pending
                    && !invocation_unsettled,
            ),
            costed_provider_attempts: i64::from(event_costed),
            unpriced_provider_attempts: i64::from(!event_costed),
            priced_tokens: token_counters(if event_costed { counters } else { [0; 4] }),
            unpriced_tokens: token_counters(if event_costed { [0; 4] } else { counters }),
            reasons,
        },
        sources,
    })
}

fn invocation_cost_summary(invocation: &UsageInvocation) -> CostSummary {
    let pending = !matches!(
        invocation.lifecycle,
        UsageInvocationLifecycle::Settled | UsageInvocationLifecycle::Unsettled
    );
    let unsettled = invocation.lifecycle == UsageInvocationLifecycle::Unsettled;
    let coverage = if pending {
        CostCoverage::Pending
    } else {
        CostCoverage::Unavailable
    };
    let reason = if pending {
        CostCoverageReasonCode::Pending
    } else if unsettled {
        CostCoverageReasonCode::Unsettled
    } else {
        CostCoverageReasonCode::Unmetered
    };
    CostSummary {
        kind: if pending || unsettled {
            if unsettled {
                CostKind::Unknown
            } else {
                CostKind::None
            }
        } else {
            CostKind::Unknown
        },
        coverage,
        provider_reported: None,
        estimated: None,
        known_subtotal: None,
        complete_total: None,
        usage_coverage: UsageCostCoverage {
            total_runs_or_turns: 1,
            pending_runs_or_turns: i64::from(pending),
            no_provider_call_runs_or_turns: 0,
            fully_metered_runs_or_turns: 0,
            fully_costed_runs_or_turns: 0,
            partially_costed_runs_or_turns: 0,
            unavailable_cost_runs_or_turns: i64::from(!pending),
            total_provider_attempts: 1,
            settled_provider_attempts: i64::from(!pending && !unsettled),
            pending_provider_attempts: i64::from(pending),
            unsettled_provider_attempts: i64::from(unsettled),
            metered_provider_attempts: i64::from(
                invocation.telemetry_state == DbUsageTelemetryState::Metered,
            ),
            unmetered_provider_attempts: i64::from(
                invocation.telemetry_state != DbUsageTelemetryState::Metered
                    && !pending
                    && !unsettled,
            ),
            costed_provider_attempts: 0,
            unpriced_provider_attempts: i64::from(!pending),
            priced_tokens: token_counters([0; 4]),
            unpriced_tokens: token_counters([0; 4]),
            reasons: vec![CostCoverageReason {
                code: reason,
                run_or_turn_count: 1,
                provider_attempt_count: 1,
                tokens: token_counters([0; 4]),
            }],
        },
        sources: Vec::new(),
    }
}

fn invocation_state(
    invocation: &UsageInvocation,
    event: Option<&UsageEvent>,
) -> DbUsageTelemetryState {
    if invocation.lifecycle == UsageInvocationLifecycle::Unsettled {
        DbUsageTelemetryState::Unsettled
    } else if !matches!(
        invocation.lifecycle,
        UsageInvocationLifecycle::Settled | UsageInvocationLifecycle::Unsettled
    ) {
        DbUsageTelemetryState::Pending
    } else {
        event
            .map(|event| event.telemetry_state)
            .unwrap_or(invocation.telemetry_state)
    }
}

fn invocation_attribution(
    invocation: &UsageInvocation,
    event: Option<&UsageEvent>,
) -> UsageAttribution {
    UsageAttribution {
        pricing_subject_revision: event
            .and_then(|event| event.pricing_subject_revision_id.clone())
            .or_else(|| invocation.pricing_subject_revision_id.clone()),
        agent_id: event
            .and_then(|event| event.agent_id.clone())
            .or_else(|| invocation.agent_id.clone()),
        profile_id: event
            .and_then(|event| event.profile_id.clone())
            .or_else(|| invocation.profile_id.clone()),
        executor_type: event
            .and_then(|event| event.executor_type.clone())
            .or_else(|| invocation.executor_type.clone()),
        provider_id: event
            .and_then(|event| event.provider_id.clone())
            .or_else(|| invocation.admitted_provider_id.clone()),
        model_id: event
            .and_then(|event| event.model_id.clone())
            .or_else(|| event.and_then(|event| event.runtime_model.clone()))
            .or_else(|| invocation.admitted_model_id.clone()),
        candidate_key: event
            .and_then(|event| event.candidate_key.clone())
            .or_else(|| invocation.candidate_key.clone()),
        attempt_ordinal: event
            .map(|event| event.attempt_ordinal)
            .unwrap_or(invocation.attempt_ordinal),
    }
}

fn event_counters(event: &UsageEvent) -> Result<Option<TokenCounters>> {
    let has_counters = event.input_tokens.is_some()
        || event.output_tokens.is_some()
        || event.cache_read_tokens.is_some()
        || event.cache_write_tokens.is_some();
    if !has_counters {
        return Ok(None);
    }
    Ok(Some(token_counters(event_counter_array(event)?)))
}

fn event_counter_array(event: &UsageEvent) -> Result<[i64; 4]> {
    let counters = [
        event.input_tokens.unwrap_or(0),
        event.output_tokens.unwrap_or(0),
        event.cache_read_tokens.unwrap_or(0),
        event.cache_write_tokens.unwrap_or(0),
    ];
    if counters.iter().any(|value| *value < 0) {
        return Err(invalid_transition());
    }
    Ok(counters)
}

fn event_reason(
    event: &UsageEvent,
    pending: bool,
    unsettled: bool,
) -> Option<CostCoverageReasonCode> {
    if pending {
        return Some(CostCoverageReasonCode::Pending);
    }
    if unsettled {
        return Some(CostCoverageReasonCode::Unsettled);
    }
    event.coverage_reason_code.map(api_reason)
}

fn api_reason(code: DbCostCoverageReasonCode) -> CostCoverageReasonCode {
    match code {
        DbCostCoverageReasonCode::Pending => CostCoverageReasonCode::Pending,
        DbCostCoverageReasonCode::Unsettled => CostCoverageReasonCode::Unsettled,
        DbCostCoverageReasonCode::Unmetered => CostCoverageReasonCode::Unmetered,
        DbCostCoverageReasonCode::MissingProvider => CostCoverageReasonCode::MissingProvider,
        DbCostCoverageReasonCode::MissingModel => CostCoverageReasonCode::MissingModel,
        DbCostCoverageReasonCode::MissingBinding => CostCoverageReasonCode::MissingBinding,
        DbCostCoverageReasonCode::MissingRate => CostCoverageReasonCode::MissingRate,
        DbCostCoverageReasonCode::UnresolvedTier => CostCoverageReasonCode::UnresolvedTier,
        DbCostCoverageReasonCode::IdentityMismatch => CostCoverageReasonCode::IdentityMismatch,
        DbCostCoverageReasonCode::InvalidLegacyUsage => CostCoverageReasonCode::InvalidLegacyUsage,
    }
}

fn event_amounts(event: &UsageEvent) -> Result<(Option<i128>, Option<i128>)> {
    let reported = event
        .provider_reported_nano_usd
        .map(|value| {
            if value < 0 {
                Err(invalid_transition())
            } else {
                Ok(i128::from(value))
            }
        })
        .transpose()?
        .or_else(|| {
            event
                .legacy_cost_usd_raw
                .as_deref()
                .and_then(parse_money_decimal)
        });
    let estimated = event
        .estimated_nano_usd
        .map(|value| {
            if value < 0 {
                Err(invalid_transition())
            } else {
                Ok(i128::from(value))
            }
        })
        .transpose()?;
    Ok(match event.cost_kind {
        UsageCostKind::ProviderReported => (reported, None),
        UsageCostKind::Estimated => (None, estimated),
        UsageCostKind::None => (None, None),
    })
}

fn event_source(event: &UsageEvent) -> Result<Option<(String, CostSourceRef)>> {
    event_source_with_metadata(event, None)
}

fn event_source_with_metadata(
    event: &UsageEvent,
    metadata: Option<&EventSourceMetadata>,
) -> Result<Option<(String, CostSourceRef)>> {
    let (reported, estimated) = event_amounts(event)?;
    if reported.is_some() {
        let source_kind = if matches!(
            event.provenance_kind,
            UsageEventProvenanceKind::RuntimeReport
        ) {
            CostSourceKind::ProviderReported
        } else {
            CostSourceKind::LegacyProviderReported
        };
        return Ok(Some((
            format!("reported:{}", source_kind_name(source_kind)),
            CostSourceRef {
                source_kind,
                rate_revision_id: None,
                catalog_snapshot_id: None,
                catalog_digest: None,
                effective_at: None,
                fetched_at: None,
                freshness: CostSourceFreshness::NotApplicable,
                retrospective: false,
                formula_revision: None,
            },
        )));
    }
    if estimated.is_none() {
        return Ok(None);
    }
    let (
        source_kind,
        rate_revision_id,
        catalog_snapshot_id,
        catalog_digest,
        effective_at,
        fetched_at,
        freshness,
    ) = if let Some(metadata) = metadata {
        let Some(source_kind) = metadata.source_kind else {
            return Ok(None);
        };
        (
            source_kind,
            metadata
                .rate_revision_id
                .clone()
                .or_else(|| event.rate_revision_id.clone()),
            metadata
                .catalog_snapshot_id
                .clone()
                .or_else(|| event.catalog_snapshot_id.clone()),
            metadata.catalog_digest.clone(),
            metadata.effective_at.clone(),
            metadata.fetched_at.clone(),
            metadata.freshness,
        )
    } else {
        // Estimated amounts are only publicly attributable when the caller
        // has joined the immutable selection/rate/snapshot metadata. Never
        // invent a catalog freshness or provenance value from event shape.
        return Ok(None);
    };
    let freshness = if source_kind == CostSourceKind::ModelsDevCatalog {
        // Historical selection rows can predate the freshness field. Preserve
        // their usable estimate but fail closed in provenance rather than
        // silently presenting an unknown source as fresh.
        freshness.unwrap_or(CostSourceFreshness::RefreshFailed)
    } else {
        CostSourceFreshness::NotApplicable
    };
    Ok(Some((
        format!(
            "estimated:{}:{}:{}",
            source_kind_name(source_kind),
            rate_revision_id.as_deref().unwrap_or_default(),
            catalog_snapshot_id.as_deref().unwrap_or_default()
        ),
        CostSourceRef {
            source_kind,
            rate_revision_id,
            catalog_snapshot_id,
            catalog_digest,
            effective_at,
            fetched_at,
            freshness,
            retrospective: event.retrospective,
            formula_revision: event.formula_revision.clone(),
        },
    )))
}

#[derive(Debug, Clone)]
struct AttemptCoverage {
    pending: bool,
    unsettled: bool,
    metered: bool,
    costed: bool,
}

#[derive(Debug, Clone)]
struct ReasonAccumulator {
    code: CostCoverageReasonCode,
    run_keys: HashSet<RunKey>,
    invocation_ids: HashSet<String>,
    tokens: [i64; 4],
}

fn add_reason(
    reasons: &mut BTreeMap<usize, ReasonAccumulator>,
    code: CostCoverageReasonCode,
    invocation: &UsageInvocation,
    tokens: [i64; 4],
) -> Result<()> {
    let key = RunKey {
        surface: invocation.surface,
        source_id: invocation.source_id.clone(),
    };
    let reason = reasons
        .entry(reason_order(code))
        .or_insert_with(|| ReasonAccumulator {
            code,
            run_keys: HashSet::new(),
            invocation_ids: HashSet::new(),
            tokens: [0; 4],
        });
    reason.run_keys.insert(key);
    reason.invocation_ids.insert(invocation.id.clone());
    for (slot, value) in reason.tokens.iter_mut().zip(tokens) {
        *slot = slot.checked_add(value).ok_or_else(invalid_transition)?;
    }
    Ok(())
}

fn add_nanos(slot: &mut Option<i128>, amount: i128) -> Result<()> {
    *slot = Some(
        slot.unwrap_or(0)
            .checked_add(amount)
            .ok_or_else(invalid_transition)?,
    );
    Ok(())
}

fn count_i64(value: usize) -> Result<i64> {
    i64::try_from(value).map_err(|_| invalid_transition())
}

fn invalid_transition() -> ServiceError {
    ServiceError::Db(db::DbError::InvalidTransition)
}

fn cost_kind(
    has_reported: bool,
    has_estimated: bool,
    attempts: i64,
    coverage: CostCoverage,
    costed_attempts: i64,
) -> CostKind {
    if has_reported && has_estimated {
        CostKind::Mixed
    } else if has_reported {
        CostKind::ProviderReported
    } else if has_estimated {
        CostKind::Estimated
    } else if attempts == 0 || (coverage == CostCoverage::Pending && costed_attempts == 0) {
        CostKind::None
    } else {
        CostKind::Unknown
    }
}

fn token_counters(values: [i64; 4]) -> TokenCounters {
    TokenCounters {
        input_tokens: values[0],
        output_tokens: values[1],
        cache_read_tokens: values[2],
        cache_write_tokens: values[3],
    }
}

fn money(nanos: Option<i128>) -> Option<MoneyAmount> {
    let nanos = nanos?;
    if nanos < 0 {
        return None;
    }
    let whole = nanos / NANOS_PER_USD;
    let fraction = nanos % NANOS_PER_USD;
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
        return integer
            .checked_mul(10_i128.checked_pow(scale.unsigned_abs())?)?
            .checked_mul(NANOS_PER_USD);
    }
    let denominator = 10_i128.checked_pow(u32::try_from(scale).ok()?)?;
    let numerator = integer.checked_mul(NANOS_PER_USD)?;
    let quotient = numerator / denominator;
    let remainder = numerator % denominator;
    let round_up =
        remainder > denominator / 2 || (denominator % 2 == 0 && remainder == denominator / 2);
    quotient.checked_add(i128::from(round_up))
}

fn api_surface(surface: DbUsageSurface) -> UsageSurface {
    match surface {
        DbUsageSurface::TaskExecution => UsageSurface::TaskExecution,
        DbUsageSurface::ProjectChat => UsageSurface::ProjectChat,
        DbUsageSurface::MainChat => UsageSurface::MainChat,
        DbUsageSurface::GenesisChat => UsageSurface::GenesisChat,
        DbUsageSurface::MainInquiry => UsageSurface::MainInquiry,
    }
}

fn api_telemetry_state(state: DbUsageTelemetryState) -> UsageTelemetryState {
    match state {
        DbUsageTelemetryState::Pending => UsageTelemetryState::Pending,
        DbUsageTelemetryState::Metered => UsageTelemetryState::Metered,
        DbUsageTelemetryState::Unmetered => UsageTelemetryState::Unmetered,
        DbUsageTelemetryState::Unsettled => UsageTelemetryState::Unsettled,
    }
}

fn surface_order(surface: DbUsageSurface) -> usize {
    match surface {
        DbUsageSurface::TaskExecution => 0,
        DbUsageSurface::ProjectChat => 1,
        DbUsageSurface::MainChat => 2,
        DbUsageSurface::GenesisChat => 3,
        DbUsageSurface::MainInquiry => 4,
    }
}

fn source_order(source_kind: CostSourceKind) -> usize {
    match source_kind {
        CostSourceKind::ProviderReported => 0,
        CostSourceKind::LegacyProviderReported => 1,
        CostSourceKind::ModelsDevCatalog => 2,
        CostSourceKind::ManualOverride => 3,
    }
}

fn source_kind_name(source_kind: CostSourceKind) -> &'static str {
    match source_kind {
        CostSourceKind::ProviderReported => "provider_reported",
        CostSourceKind::LegacyProviderReported => "legacy_provider_reported",
        CostSourceKind::ModelsDevCatalog => "models_dev_catalog",
        CostSourceKind::ManualOverride => "manual_override",
    }
}

fn reason_order(code: CostCoverageReasonCode) -> usize {
    match code {
        CostCoverageReasonCode::Pending => 0,
        CostCoverageReasonCode::Unsettled => 1,
        CostCoverageReasonCode::Unmetered => 2,
        CostCoverageReasonCode::MissingProvider => 3,
        CostCoverageReasonCode::MissingModel => 4,
        CostCoverageReasonCode::MissingBinding => 5,
        CostCoverageReasonCode::MissingRate => 6,
        CostCoverageReasonCode::UnresolvedTier => 7,
        CostCoverageReasonCode::IdentityMismatch => 8,
        CostCoverageReasonCode::InvalidLegacyUsage => 9,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn invocation(id: &str) -> UsageInvocation {
        UsageInvocation {
            id: id.to_owned(),
            owner_user_id: None,
            project_id: Some("project".to_owned()),
            domain_kind: db::PricingDomainKind::Execution,
            surface: DbUsageSurface::TaskExecution,
            source_id: format!("source-{id}"),
            execution_id: Some(format!("source-{id}")),
            task_id: Some("task".to_owned()),
            domain_idempotency_key: format!("key-{id}"),
            candidate_key: Some(format!("candidate-{id}")),
            attempt_ordinal: 0,
            pricing_selection_id: format!("selection-{id}"),
            admitted_provider_id: Some("provider".to_owned()),
            admitted_model_id: Some("model".to_owned()),
            admitted_runtime_model: None,
            pricing_subject_id: None,
            pricing_subject_revision_id: None,
            subject_revision_digest: None,
            agent_id: Some("agent".to_owned()),
            profile_id: Some("profile".to_owned()),
            agent_name_snapshot: None,
            project_name_snapshot: None,
            executor_type: Some("executor".to_owned()),
            backend_kind: Some("backend".to_owned()),
            provenance_kind: db::PricingAdmissionProvenanceKind::Runtime,
            lifecycle: UsageInvocationLifecycle::Settled,
            telemetry_state: DbUsageTelemetryState::Metered,
            terminal_reason: None,
            version: 1,
            admitted_at: "2026-01-01T00:00:00Z".to_owned(),
            started_at: Some("2026-01-01T00:00:01Z".to_owned()),
            settled_at: Some("2026-01-01T00:00:02Z".to_owned()),
            created_at: "2026-01-01T00:00:00Z".to_owned(),
            updated_at: "2026-01-01T00:00:02Z".to_owned(),
        }
    }

    fn event(id: &str, invocation_id: &str) -> UsageEvent {
        UsageEvent {
            id: id.to_owned(),
            invocation_id: invocation_id.to_owned(),
            owner_user_id: None,
            project_id: Some("project".to_owned()),
            surface: DbUsageSurface::TaskExecution,
            source_id: format!("source-{invocation_id}"),
            execution_id: Some(format!("source-{invocation_id}")),
            task_id: Some("task".to_owned()),
            event_idempotency_key: format!("event-key-{id}"),
            source_report_id: format!("report-{id}"),
            report_sequence: 0,
            report_mode: db::UsageEventReportMode::Delta,
            provenance_kind: UsageEventProvenanceKind::RuntimeReport,
            legacy_source_table: None,
            legacy_source_id: None,
            legacy_provider_raw: None,
            legacy_provider_sqlite_type: None,
            legacy_provider_sql_literal: None,
            legacy_model_raw: None,
            legacy_model_sqlite_type: None,
            legacy_model_sql_literal: None,
            legacy_counter_values_json: "{}".to_owned(),
            legacy_cost_usd_raw: None,
            legacy_created_at_raw: None,
            legacy_project_owner_raw: None,
            legacy_invalid_usage: false,
            provider_id: Some("provider".to_owned()),
            model_id: Some("model".to_owned()),
            runtime_model: None,
            candidate_key: Some("candidate".to_owned()),
            attempt_ordinal: 0,
            agent_id: Some("agent".to_owned()),
            profile_id: Some("profile".to_owned()),
            agent_name_snapshot: None,
            project_name_snapshot: None,
            executor_type: Some("executor".to_owned()),
            pricing_subject_revision_id: None,
            subject_revision_digest: None,
            telemetry_state: DbUsageTelemetryState::Metered,
            input_tokens: None,
            output_tokens: None,
            cache_read_tokens: None,
            cache_write_tokens: None,
            context_tokens: None,
            selected_tier: None,
            provider_reported_nano_usd: Some(0),
            legacy_reported_cost_usd: None,
            estimated_nano_usd: None,
            cost_kind: UsageCostKind::ProviderReported,
            rate_revision_id: None,
            catalog_snapshot_id: None,
            formula_revision: None,
            retrospective: false,
            coverage_reason_code: None,
            occurred_at: "2026-01-01T00:00:02Z".to_owned(),
            created_at: "2026-01-01T00:00:02Z".to_owned(),
        }
    }

    #[test]
    fn money_keeps_explicit_zero_and_sub_cent_values_decimal() {
        assert_eq!(money(Some(0)).expect("zero").decimal, "0");
        assert_eq!(money(Some(1)).expect("nano").decimal, "0.000000001");
        assert_eq!(parse_money_decimal("0.0000000005"), Some(1));
    }

    #[test]
    fn corrupt_negative_counter_is_rejected() {
        let invocation = invocation("invocation");
        let mut usage_event = event("event", &invocation.id);
        usage_event.input_tokens = Some(-1);

        let result = usage_breakdowns_for_invocation(&invocation, &[usage_event]);
        assert!(matches!(
            result,
            Err(ServiceError::Db(db::DbError::InvalidTransition))
        ));
    }

    #[test]
    fn counter_overflow_is_rejected_in_aggregate() {
        let first_invocation = invocation("first");
        let second_invocation = invocation("second");
        let mut first_event = event("first-event", &first_invocation.id);
        first_event.input_tokens = Some(i64::MAX);
        let mut second_event = event("second-event", &second_invocation.id);
        second_event.input_tokens = Some(1);

        let mut events_by_invocation = HashMap::new();
        events_by_invocation.insert(first_invocation.id.clone(), vec![first_event]);
        events_by_invocation.insert(second_invocation.id.clone(), vec![second_event]);
        let result = aggregate_usage(
            &[first_invocation, second_invocation],
            &events_by_invocation,
            &[],
        );

        assert!(matches!(
            result,
            Err(ServiceError::Db(db::DbError::InvalidTransition))
        ));
    }

    #[test]
    fn explicit_zero_provider_cost_is_complete_zero() {
        let invocation = invocation("invocation");
        let usage_event = event("event", &invocation.id);
        let mut events_by_invocation = HashMap::new();
        events_by_invocation.insert(invocation.id.clone(), vec![usage_event]);
        let aggregate = aggregate_usage(&[invocation], &events_by_invocation, &[]).unwrap();

        assert_eq!(aggregate.cost.coverage, CostCoverage::Complete);
        assert_eq!(
            aggregate
                .cost
                .complete_total
                .as_ref()
                .map(|value| value.decimal.as_str()),
            Some("0")
        );
    }

    #[test]
    fn pending_and_settled_unpriced_costs_are_not_zero() {
        let mut pending_invocation = invocation("pending");
        pending_invocation.lifecycle = UsageInvocationLifecycle::Started;
        let pending = usage_breakdowns_for_invocation(&pending_invocation, &[]).unwrap();
        assert_eq!(pending[0].cost.coverage, CostCoverage::Pending);
        assert_eq!(pending[0].cost.kind, CostKind::None);
        assert!(pending[0].cost.complete_total.is_none());

        let settled_invocation = invocation("settled");
        let mut unpriced_event = event("unpriced-event", &settled_invocation.id);
        unpriced_event.provider_reported_nano_usd = None;
        unpriced_event.cost_kind = UsageCostKind::None;
        unpriced_event.input_tokens = Some(1);
        let settled =
            usage_breakdowns_for_invocation(&settled_invocation, &[unpriced_event]).unwrap();
        assert_eq!(settled[0].cost.coverage, CostCoverage::Unavailable);
        assert_eq!(settled[0].cost.kind, CostKind::Unknown);
        assert!(settled[0].cost.complete_total.is_none());
    }

    #[test]
    fn estimated_source_preserves_frozen_catalog_freshness_and_provenance() {
        for freshness in [
            CostSourceFreshness::Stale,
            CostSourceFreshness::RefreshFailed,
        ] {
            let mut usage_event = event("event", "invocation");
            usage_event.provider_reported_nano_usd = None;
            usage_event.estimated_nano_usd = Some(1);
            usage_event.cost_kind = UsageCostKind::Estimated;
            usage_event.rate_revision_id = Some("rate-revision".to_owned());
            usage_event.catalog_snapshot_id = Some("catalog-snapshot".to_owned());
            usage_event.formula_revision = Some("formula-1".to_owned());
            usage_event.retrospective = true;
            let metadata = EventSourceMetadata {
                source_kind: Some(CostSourceKind::ModelsDevCatalog),
                rate_revision_id: Some("rate-revision".to_owned()),
                catalog_snapshot_id: Some("catalog-snapshot".to_owned()),
                catalog_digest: Some("catalog-digest".to_owned()),
                effective_at: Some("2026-01-01T00:00:00Z".to_owned()),
                fetched_at: Some("2026-01-01T00:00:00Z".to_owned()),
                freshness: Some(freshness),
            };

            let (_, source) = event_source_with_metadata(&usage_event, Some(&metadata))
                .unwrap()
                .unwrap();
            assert_eq!(source.freshness, freshness);
            assert_eq!(source.catalog_digest.as_deref(), Some("catalog-digest"));
            assert_eq!(source.effective_at.as_deref(), Some("2026-01-01T00:00:00Z"));
            assert_eq!(source.fetched_at.as_deref(), Some("2026-01-01T00:00:00Z"));
            assert!(source.retrospective);
        }
    }

    #[test]
    fn estimated_source_without_freshness_fails_closed() {
        let mut usage_event = event("event", "invocation");
        usage_event.provider_reported_nano_usd = None;
        usage_event.estimated_nano_usd = Some(1);
        usage_event.cost_kind = UsageCostKind::Estimated;
        let metadata = EventSourceMetadata {
            source_kind: Some(CostSourceKind::ModelsDevCatalog),
            rate_revision_id: Some("rate-revision".to_owned()),
            catalog_snapshot_id: Some("catalog-snapshot".to_owned()),
            catalog_digest: None,
            effective_at: None,
            fetched_at: None,
            freshness: None,
        };

        let (_, source) = event_source_with_metadata(&usage_event, Some(&metadata))
            .unwrap()
            .unwrap();
        assert_eq!(source.freshness, CostSourceFreshness::RefreshFailed);
    }
}
