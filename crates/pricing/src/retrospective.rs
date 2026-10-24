//! Retrospective estimate previews and commits.

use super::*;

// ---------------------------------------------------------------------------
// Retrospective preview/commit domain boundary
// ---------------------------------------------------------------------------

/// Minimal immutable usage-event projection required for retrospective
/// estimation.  The DB adapter supplies this from its append-only ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrospectiveUsageEvent {
    /// Stable usage-event ID.
    pub event_id: String,
    /// Actual provider identity from the ledger.
    pub provider_id: Option<String>,
    /// Actual runtime model identity from the ledger.
    pub model_id: Option<String>,
    /// Authoritative counters, if metered.
    pub counters: Option<EventTokenCounts>,
    /// Existing provider-reported money, which must never be overwritten.
    pub provider_reported_amount: Option<NanoUsd>,
    /// Request-level context evidence, if captured.
    pub context_tokens: Option<u64>,
    /// Original immutable occurrence time.
    pub occurred_at: SystemTime,
}

/// Exact retrospective candidate that can be committed as an estimate
/// revision without changing the original usage event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrospectiveEstimateCandidate {
    /// Usage event to which the immutable estimate revision points.
    pub event_id: String,
    /// Calculated amount.
    pub amount: NanoUsd,
    /// Exact catalog row/revision used.
    pub rate_revision_id: String,
    /// Snapshot selected by the caller.
    pub snapshot_id: String,
    /// Selected exact context tier, if any.
    pub selected_tier: Option<String>,
}

/// Event omitted from a retrospective preview and its bounded reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrospectiveUnmatchedEvent {
    /// Stable usage event ID.
    pub event_id: String,
    /// Why the event cannot be priced against the chosen snapshot.
    pub reason: PriceSelectionReasonCode,
}

/// Immutable preview over one exact usage set and catalog snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrospectivePreview {
    /// Deterministic preview ID.
    pub id: String,
    /// Project scope selected by the caller.
    pub project_id: String,
    /// Immutable catalog snapshot ID.
    pub snapshot_id: String,
    /// Catalog freshness frozen while this preview was constructed.  A
    /// historical preview remains tied to this state even after the mutable
    /// catalog status changes.
    pub catalog_freshness: CatalogFreshness,
    /// Digest of all source event identities/values in the preview.
    pub usage_set_digest: String,
    /// Exact usage-event IDs covered by this preview.  The persistence
    /// boundary uses these IDs to re-read the append-only ledger before a
    /// commit; candidate amounts are never trusted as the source of truth.
    pub source_event_ids: Vec<String>,
    /// Number of exact eligible events.
    pub eligible_event_count: usize,
    /// Number of non-reported unmatched events.
    pub unmatched_event_count: usize,
    /// Number of provider-reported events excluded from backfill.
    pub already_reported_event_count: usize,
    /// Sum of eligible whole-event estimates, including explicit zero.
    pub projected_cost: Option<NanoUsd>,
    /// Exact immutable candidate set to commit.
    pub eligible_events: Vec<RetrospectiveEstimateCandidate>,
    /// Unmatched coverage details.
    pub unmatched_events: Vec<RetrospectiveUnmatchedEvent>,
    /// Preview expiration boundary.
    pub expires_at: SystemTime,
}

/// Builds a retrospective preview against one chosen immutable snapshot.
///
/// When a mutable catalog status is available, prefer
/// [`preview_retrospective_estimates_with_freshness`] so the preview records
/// the latest successful check (including a 304) rather than only the
/// immutable snapshot's retrieval time.
pub fn preview_retrospective_estimates(
    project_id: impl Into<String>,
    snapshot: &CatalogSnapshot,
    events: &[RetrospectiveUsageEvent],
    created_at: SystemTime,
    expires_after: Duration,
) -> Result<RetrospectivePreview, RetrospectivePreviewError> {
    preview_retrospective_estimates_with_freshness(
        project_id,
        snapshot,
        events,
        created_at,
        expires_after,
        snapshot.freshness_at(created_at),
    )
}

/// Builds a retrospective preview while freezing the caller's current
/// catalog freshness.  Callers with repository status should pass
/// [`CatalogStatus::freshness_for_snapshot`] here so a successful 304 check
/// is reflected even though the immutable snapshot payload is unchanged.
pub fn preview_retrospective_estimates_with_freshness(
    project_id: impl Into<String>,
    snapshot: &CatalogSnapshot,
    events: &[RetrospectiveUsageEvent],
    created_at: SystemTime,
    expires_after: Duration,
    catalog_freshness: CatalogFreshness,
) -> Result<RetrospectivePreview, RetrospectivePreviewError> {
    let project_id = project_id.into();
    if project_id.trim().is_empty() {
        return Err(RetrospectivePreviewError::InvalidProject);
    }
    let usage_set_digest = retrospective_usage_set_digest(events);
    let mut eligible_events = Vec::new();
    let mut unmatched_events = Vec::new();
    let mut projected_cost: Option<NanoUsd> = None;
    let mut already_reported_event_count = 0;
    let mut event_ids = BTreeSet::new();
    for event in events {
        if !event_ids.insert(event.event_id.as_str()) {
            return Err(RetrospectivePreviewError::DuplicateEventId);
        }
        if event.provider_reported_amount.is_some() {
            already_reported_event_count += 1;
            continue;
        }
        let Some(provider_id) = event
            .provider_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        else {
            unmatched_events.push(RetrospectiveUnmatchedEvent {
                event_id: event.event_id.clone(),
                reason: PriceSelectionReasonCode::MissingProvider,
            });
            continue;
        };
        let Some(model_id) = event
            .model_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        else {
            unmatched_events.push(RetrospectiveUnmatchedEvent {
                event_id: event.event_id.clone(),
                reason: PriceSelectionReasonCode::MissingModel,
            });
            continue;
        };
        let Some(counters) = event.counters else {
            unmatched_events.push(RetrospectiveUnmatchedEvent {
                event_id: event.event_id.clone(),
                reason: PriceSelectionReasonCode::Unmetered,
            });
            continue;
        };
        let Some(model_rate) = snapshot.model_rate(provider_id, model_id) else {
            unmatched_events.push(RetrospectiveUnmatchedEvent {
                event_id: event.event_id.clone(),
                reason: PriceSelectionReasonCode::MissingBinding,
            });
            continue;
        };
        let selection = selection_from_snapshot(snapshot, model_rate, catalog_freshness);
        let (rates, selected_tier) =
            match select_context_rates(&selection, counters, event.context_tokens) {
                Ok(value) => value,
                Err(reason) => {
                    unmatched_events.push(RetrospectiveUnmatchedEvent {
                        event_id: event.event_id.clone(),
                        reason,
                    });
                    continue;
                }
            };
        let amount = match calculate_event_cost(counters, rates) {
            Ok(EventCostEstimate::Complete(amount)) => amount,
            Ok(EventCostEstimate::Incomplete { .. }) => {
                unmatched_events.push(RetrospectiveUnmatchedEvent {
                    event_id: event.event_id.clone(),
                    reason: PriceSelectionReasonCode::MissingRate,
                });
                continue;
            }
            Err(EventCostError::Overflow) => {
                return Err(RetrospectivePreviewError::ArithmeticOverflow);
            }
        };
        projected_cost = Some(match projected_cost {
            Some(existing) => existing
                .checked_add(amount)
                .ok_or(RetrospectivePreviewError::ArithmeticOverflow)?,
            None => amount,
        });
        eligible_events.push(RetrospectiveEstimateCandidate {
            event_id: event.event_id.clone(),
            amount,
            rate_revision_id: model_rate.rate_digest(&snapshot.id),
            snapshot_id: snapshot.id.clone(),
            selected_tier,
        });
    }
    let expires_at = created_at
        .checked_add(expires_after)
        .ok_or(RetrospectivePreviewError::ArithmeticOverflow)?;
    Ok(RetrospectivePreview {
        id: canonical_hash("retrospective-preview-v3", |encoded| {
            append_str(encoded, &project_id);
            append_str(encoded, &snapshot.id);
            append_str(encoded, &usage_set_digest);
            append_str(encoded, catalog_freshness.as_str());
        }),
        project_id,
        snapshot_id: snapshot.id.clone(),
        catalog_freshness,
        usage_set_digest,
        source_event_ids: {
            let mut ids = events
                .iter()
                .map(|event| event.event_id.clone())
                .collect::<Vec<_>>();
            ids.sort();
            ids
        },
        eligible_event_count: eligible_events.len(),
        unmatched_event_count: unmatched_events.len(),
        already_reported_event_count,
        projected_cost,
        eligible_events,
        unmatched_events,
        expires_at,
    })
}

/// Freshness-aware alias for [`preview_retrospective_estimates_with_freshness`].
pub fn preview_cost_estimation_with_freshness(
    project_id: impl Into<String>,
    snapshot: &CatalogSnapshot,
    events: &[RetrospectiveUsageEvent],
    created_at: SystemTime,
    expires_after: Duration,
    catalog_freshness: CatalogFreshness,
) -> Result<RetrospectivePreview, RetrospectivePreviewError> {
    preview_retrospective_estimates_with_freshness(
        project_id,
        snapshot,
        events,
        created_at,
        expires_after,
        catalog_freshness,
    )
}

/// Preview construction failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RetrospectivePreviewError {
    #[error("retrospective preview project is empty")]
    InvalidProject,
    #[error("retrospective preview arithmetic overflow")]
    ArithmeticOverflow,
    #[error("retrospective preview contains a duplicate usage event ID")]
    DuplicateEventId,
}

fn selection_from_snapshot(
    snapshot: &CatalogSnapshot,
    model: &CatalogModelRate,
    catalog_freshness: CatalogFreshness,
) -> FrozenPriceSelection {
    let has_cost = model.has_cost();
    let mut selection = FrozenPriceSelection {
        subject_id: None,
        subject_revision_digest: None,
        runtime_model: Some(model.model_id.clone()),
        candidate_key: None,
        attempt_ordinal: 0,
        admitted_provider_id: Some(model.provider_id.clone()),
        catalog_provider_id: Some(model.provider_id.clone()),
        catalog_model_id: Some(model.model_id.clone()),
        source_kind: Some(PricingSourceKind::ModelsDevCatalog),
        rate_revision_id: Some(model.rate_digest(&snapshot.id)),
        catalog_snapshot_id: Some(snapshot.id.clone()),
        rates: Some(model.rates),
        tiers: model.tiers.clone(),
        legacy_context_over_200k: model.legacy_context_over_200k.clone(),
        catalog_freshness,
        status: if has_cost {
            PriceSelectionStatus::Priced
        } else {
            PriceSelectionStatus::Unpriced
        },
        reason: (!has_cost).then_some(PriceSelectionReasonCode::MissingRate),
        selection_digest: String::new(),
    };
    selection.selection_digest = frozen_selection_digest(&selection);
    selection
}

/// Computes a deterministic digest over the exact source event set.
pub fn retrospective_usage_set_digest(events: &[RetrospectiveUsageEvent]) -> String {
    let mut rows = events
        .iter()
        .map(retrospective_usage_event_digest_row)
        .collect::<Vec<Vec<u8>>>();
    rows.sort();
    canonical_hash("retrospective-usage-set-v2", |encoded| {
        append_u64(
            encoded,
            u64::try_from(rows.len()).expect("in-memory usage-event count fits u64"),
        );
        for row in rows {
            append_len_prefixed(encoded, &row);
        }
    })
}

fn retrospective_usage_event_digest_row(event: &RetrospectiveUsageEvent) -> Vec<u8> {
    let mut encoded = Vec::new();
    append_str(&mut encoded, &event.event_id);
    append_option_str(&mut encoded, event.provider_id.as_deref());
    append_option_str(&mut encoded, event.model_id.as_deref());
    match event.counters {
        Some(counters) => {
            encoded.push(1);
            for counter in counters.as_array() {
                append_u64(&mut encoded, counter);
            }
        }
        None => encoded.push(0),
    }
    match event.provider_reported_amount {
        Some(amount) => {
            encoded.push(1);
            append_i64(&mut encoded, amount.as_nano_usd());
        }
        None => encoded.push(0),
    }
    match event.context_tokens {
        Some(value) => {
            encoded.push(1);
            append_u64(&mut encoded, value);
        }
        None => encoded.push(0),
    }
    append_system_time(&mut encoded, event.occurred_at);
    encoded
}

/// Request to commit one exact preview idempotently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrospectiveCommitRequest {
    /// Preview ID returned by the immutable preview.
    pub preview_id: String,
    /// Exact usage-set digest returned by the preview.
    pub usage_set_digest: String,
    /// Stable retry key.
    pub idempotency_key: String,
}

/// Immutable retrospective run result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrospectiveRun {
    /// Immutable run ID.
    pub id: String,
    /// Project scope.
    pub project_id: String,
    /// Preview and snapshot provenance.
    pub preview_id: String,
    /// Catalog snapshot used.
    pub snapshot_id: String,
    /// Usage-set digest committed.
    pub usage_set_digest: String,
    /// Number of estimate revisions inserted.
    pub applied_event_count: usize,
    /// Number of events left unmatched in the preview.
    pub unmatched_event_count: usize,
    /// Sum of newly applied estimates.
    pub cost: Option<NanoUsd>,
    /// Creation time.
    pub created_at: SystemTime,
}

/// Retrospective commit failure at the repository boundary.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RetrospectiveRepositoryError {
    #[error("retrospective preview is expired")]
    Expired,
    #[error("retrospective usage-set digest does not match the preview")]
    UsageSetConflict,
    #[error("retrospective preview or idempotency key conflicts")]
    Conflict,
    #[error("retrospective repository unavailable: {0}")]
    Unavailable(String),
}

/// Atomic retrospective-estimate persistence boundary.  Implementations must
/// insert immutable estimate revisions in one transaction, key retries by
/// preview/usage-set/idempotency identity, and never overwrite a provider
/// reported amount or mutate a prior estimate revision.
#[async_trait]
pub trait RetrospectiveEstimateRepository: Send + Sync {
    /// Commits a validated preview and returns the immutable run result.
    async fn commit_retrospective_preview(
        &self,
        preview: RetrospectivePreview,
        request: RetrospectiveCommitRequest,
        now: SystemTime,
    ) -> Result<RetrospectiveRun, RetrospectiveRepositoryError>;
}

/// Commits a preview through an abstract repository boundary.
pub async fn commit_retrospective_preview<R>(
    repository: &R,
    preview: RetrospectivePreview,
    request: RetrospectiveCommitRequest,
    now: SystemTime,
) -> Result<RetrospectiveRun, RetrospectiveRepositoryError>
where
    R: RetrospectiveEstimateRepository + ?Sized,
{
    if request.preview_id != preview.id || request.usage_set_digest != preview.usage_set_digest {
        return Err(RetrospectiveRepositoryError::UsageSetConflict);
    }
    if request.idempotency_key.trim().is_empty() {
        return Err(RetrospectiveRepositoryError::Conflict);
    }
    if now >= preview.expires_at {
        return Err(RetrospectiveRepositoryError::Expired);
    }
    repository
        .commit_retrospective_preview(preview, request, now)
        .await
}
