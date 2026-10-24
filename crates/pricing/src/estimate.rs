//! Usage event estimates and context tier coverage.

use super::*;

// ---------------------------------------------------------------------------
// Event estimates and context-tier coverage
// ---------------------------------------------------------------------------

/// Event-level estimate outcome.  Provider-reported money is authoritative
/// for the event and never receives a second Forge estimate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventEstimate {
    /// The producer supplied a non-negative amount.
    ProviderReported {
        /// Reported amount retained independently from any selected rate.
        amount: NanoUsd,
    },
    /// Forge calculated a complete amount from a frozen rate revision.
    Estimated {
        /// One-event fixed-point estimate.
        amount: NanoUsd,
        /// Immutable pricing provenance.
        provenance: EstimateProvenance,
    },
    /// Some token evidence exists but no complete amount can be proven.
    Partial {
        /// Why the event could not be fully priced.
        reason: PriceSelectionReasonCode,
        /// Positive buckets that had usable rates.
        priced_counters: EventTokenCounts,
        /// Positive buckets that were unknown/unusable.
        unpriced_counters: EventTokenCounts,
        /// Frozen provenance, when a rate was selected.
        provenance: Option<EstimateProvenance>,
    },
    /// No amount is available for this event.
    Unpriced {
        /// Why no amount can be calculated.
        reason: PriceSelectionReasonCode,
    },
}

/// Input to event estimation. `None` counters mean no trustworthy telemetry,
/// not a metered all-zero result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageEventEstimateInput {
    /// Authoritative counters, if the producer supplied them.
    pub counters: Option<EventTokenCounts>,
    /// Provider-reported amount, if supplied.
    pub provider_reported_amount: Option<NanoUsd>,
    /// Frozen admission-time selection.
    pub selection: FrozenPriceSelection,
    /// Actual provider identity observed at execution time, if any.
    pub actual_provider_id: Option<String>,
    /// Actual model identity observed at execution time, if any.
    pub actual_model_id: Option<String>,
    /// Request-level context evidence needed for exact tiers.
    pub context_tokens: Option<u64>,
}

/// Immutable provenance stored alongside a complete Forge estimate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EstimateProvenance {
    /// Selected source kind.
    pub source_kind: PricingSourceKind,
    /// Immutable rate revision ID.
    pub rate_revision_id: String,
    /// Catalog snapshot ID, if catalog sourced.
    pub catalog_snapshot_id: Option<String>,
    /// Freshness frozen at admission/selection time.
    pub catalog_freshness: CatalogFreshness,
    /// Selected exact tier label, if any.
    pub selected_tier: Option<String>,
    /// Formula revision persisted with the estimate.
    pub formula_revision: String,
}

/// Selects a base or exact context-tier rate set.  Legacy threshold-unknown
/// pricing is intentionally rejected for positive usage.
pub fn select_context_rates(
    selection: &FrozenPriceSelection,
    counters: EventTokenCounts,
    context_tokens: Option<u64>,
) -> Result<(EventBucketRates, Option<String>), PriceSelectionReasonCode> {
    if selection.validate().is_err() {
        return Err(PriceSelectionReasonCode::MissingBinding);
    }
    if selection.status != PriceSelectionStatus::Priced {
        return Err(selection
            .reason
            .unwrap_or(PriceSelectionReasonCode::MissingBinding));
    }
    let Some(base_rates) = selection.rates else {
        return Err(selection
            .reason
            .unwrap_or(PriceSelectionReasonCode::MissingBinding));
    };
    if counters.as_array().iter().all(|counter| *counter == 0) {
        return Ok((base_rates, None));
    }
    if selection.legacy_context_over_200k.is_some() && selection.tiers.is_empty() {
        return Err(PriceSelectionReasonCode::UnresolvedTier);
    }
    if selection.tiers.is_empty() {
        return Ok((base_rates, None));
    }
    let Some(context_tokens) = context_tokens else {
        return Err(PriceSelectionReasonCode::UnresolvedTier);
    };
    // `tier.size` is the lower bound of the tier.  Below the first exact
    // lower bound the validated base rate remains the applicable band.
    let selected = selection
        .tiers
        .iter()
        .filter(|tier| tier.threshold_tokens <= context_tokens)
        .max_by_key(|tier| tier.threshold_tokens);
    Ok(match selected {
        Some(tier) => (
            tier.rates,
            Some(format!("context_{}", tier.threshold_tokens)),
        ),
        None => (base_rates, None),
    })
}

/// Calculates one usage event while preserving reported/estimated/unknown
/// semantics and disallowing identity mismatches from being priced.
pub fn estimate_usage_event(input: UsageEventEstimateInput) -> EventEstimate {
    if input.selection.validate().is_err() {
        return EventEstimate::Unpriced {
            reason: PriceSelectionReasonCode::MissingBinding,
        };
    }
    if let Some(amount) = input.provider_reported_amount {
        return EventEstimate::ProviderReported { amount };
    }
    let Some(counters) = input.counters else {
        return EventEstimate::Unpriced {
            reason: PriceSelectionReasonCode::Unmetered,
        };
    };
    if !input.selection.is_priced() {
        return EventEstimate::Unpriced {
            reason: input
                .selection
                .reason
                .unwrap_or(PriceSelectionReasonCode::MissingBinding),
        };
    }
    // CLI/runtime admissions may not carry an admitted provider, but an
    // exact catalog binding still does.  Once catalog provenance supplies the
    // expected provider, an execution report naming a different provider is
    // an identity mismatch just like an explicitly admitted provider.
    let expected_provider_id = input
        .selection
        .admitted_provider_id
        .as_deref()
        .or(input.selection.catalog_provider_id.as_deref());
    let provider_mismatch = match (expected_provider_id, input.actual_provider_id.as_deref()) {
        (Some(admitted), Some(actual)) => admitted != actual,
        _ => false,
    };
    let model_mismatch = match input.actual_model_id.as_deref() {
        Some(actual) => {
            let runtime_matches = input
                .selection
                .runtime_model
                .as_deref()
                .is_some_and(|admitted| admitted == actual);
            let catalog_matches = input
                .selection
                .catalog_model_id
                .as_deref()
                .is_some_and(|catalog| catalog == actual);
            !runtime_matches && !catalog_matches
        }
        None => false,
    };
    if provider_mismatch || model_mismatch {
        return EventEstimate::Unpriced {
            reason: PriceSelectionReasonCode::IdentityMismatch,
        };
    }
    let (rates, selected_tier) =
        match select_context_rates(&input.selection, counters, input.context_tokens) {
            Ok(rates) => rates,
            Err(reason) => {
                return EventEstimate::Partial {
                    reason,
                    priced_counters: EventTokenCounts::default(),
                    unpriced_counters: counters,
                    provenance: Some(estimate_provenance(&input.selection, None)),
                };
            }
        };
    let provenance = estimate_provenance(&input.selection, selected_tier);
    match calculate_event_cost(counters, rates) {
        Ok(EventCostEstimate::Complete(amount)) => EventEstimate::Estimated { amount, provenance },
        Ok(EventCostEstimate::Incomplete { missing_rates }) => {
            let (priced_counters, unpriced_counters) = split_priced_counters(counters, rates);
            let _ = missing_rates;
            EventEstimate::Partial {
                reason: PriceSelectionReasonCode::MissingRate,
                priced_counters,
                unpriced_counters,
                provenance: Some(provenance),
            }
        }
        Err(EventCostError::Overflow) => EventEstimate::Unpriced {
            reason: PriceSelectionReasonCode::MissingRate,
        },
    }
}

fn estimate_provenance(
    selection: &FrozenPriceSelection,
    selected_tier: Option<String>,
) -> EstimateProvenance {
    EstimateProvenance {
        source_kind: selection
            .source_kind
            .unwrap_or(PricingSourceKind::ManualOverride),
        rate_revision_id: selection.rate_revision_id.clone().unwrap_or_default(),
        catalog_snapshot_id: selection.catalog_snapshot_id.clone(),
        catalog_freshness: selection.catalog_freshness,
        selected_tier,
        formula_revision: COST_FORMULA_REVISION.to_owned(),
    }
}

fn split_priced_counters(
    counters: EventTokenCounts,
    rates: EventBucketRates,
) -> (EventTokenCounts, EventTokenCounts) {
    let counters = counters.as_array();
    let rates = rates.as_array();
    let mut priced = [0_u64; 4];
    let mut unpriced = [0_u64; 4];
    for index in 0..4 {
        if rates[index].is_some() {
            priced[index] = counters[index];
        } else {
            unpriced[index] = counters[index];
        }
    }
    (
        EventTokenCounts::new(priced[0], priced[1], priced[2], priced[3]),
        EventTokenCounts::new(unpriced[0], unpriced[1], unpriced[2], unpriced[3]),
    )
}
