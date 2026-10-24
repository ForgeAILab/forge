//! Immutable price selections and persisted context rates.

use super::*;

// ---------------------------------------------------------------------------
// Exact bindings, immutable overrides, and admission-time selections
// ---------------------------------------------------------------------------

/// Source kind for a selected immutable price revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PricingSourceKind {
    /// A normalized row from one immutable models.dev snapshot.
    ModelsDevCatalog,
    /// A user-entered immutable rate revision.
    ManualOverride,
}

impl PricingSourceKind {
    /// Stable persistence/API spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ModelsDevCatalog => "models_dev_catalog",
            Self::ManualOverride => "manual_override",
        }
    }
}

fn rates_exceed_plausibility_bound(rates: EventBucketRates) -> bool {
    rates
        .as_array()
        .iter()
        .flatten()
        .any(|rate| rate.as_nano_usd_per_million() > MAX_MANUAL_RATE_NANO_USD_PER_MILLION)
}

/// A frozen admission-time decision for one candidate/attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrozenPriceSelection {
    /// Pricing subject ID, if the candidate has one.
    pub subject_id: Option<String>,
    /// Immutable subject revision digest.
    pub subject_revision_digest: Option<String>,
    /// Exact runtime model admitted for this candidate.
    pub runtime_model: Option<String>,
    /// Candidate route identity, retained for replay-safe attribution.
    pub candidate_key: Option<String>,
    /// Candidate attempt ordinal.
    pub attempt_ordinal: u32,
    /// Admitted provider identity, when known.
    pub admitted_provider_id: Option<String>,
    /// Exact catalog provider identity, when catalog-priced.
    pub catalog_provider_id: Option<String>,
    /// Exact catalog model identity, when catalog-priced.
    pub catalog_model_id: Option<String>,
    /// Frozen source kind and immutable rate revision.
    pub source_kind: Option<PricingSourceKind>,
    /// Immutable selected rate revision.
    pub rate_revision_id: Option<String>,
    /// Catalog snapshot revision, when selected from models.dev.
    pub catalog_snapshot_id: Option<String>,
    /// Fixed-point base rates.
    pub rates: Option<EventBucketRates>,
    /// Exact context bands retained with the selection.
    pub tiers: Vec<ContextTier>,
    /// Legacy threshold-unknown data is retained but never guessed.
    pub legacy_context_over_200k: Option<LegacyContextRate>,
    /// Catalog freshness frozen at admission.
    pub catalog_freshness: CatalogFreshness,
    /// Whether a usable exact price was selected.
    pub status: PriceSelectionStatus,
    /// Why this selection is unpriced, if applicable.
    pub reason: Option<PriceSelectionReasonCode>,
    /// Stable digest of this immutable decision.
    pub selection_digest: String,
}

/// Admission selection status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PriceSelectionStatus {
    /// One exact manual/catalog revision was selected.
    Priced,
    /// Candidate is admitted but has no usable exact price.
    Unpriced,
}

impl PriceSelectionStatus {
    /// Stable persistence/API spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Priced => "priced",
            Self::Unpriced => "unpriced",
        }
    }
}

/// Admission/estimate reason codes for unpriced selections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
pub enum PriceSelectionReasonCode {
    #[error("provider identity is missing")]
    MissingProvider,
    #[error("runtime model identity is missing")]
    MissingModel,
    #[error("no exact active pricing binding exists")]
    MissingBinding,
    #[error("pricing binding is retired")]
    RetiredBinding,
    #[error("actual provider/model identity differs from admitted identity")]
    IdentityMismatch,
    #[error("token telemetry is unavailable")]
    Unmetered,
    #[error("a positive token bucket has no rate")]
    MissingRate,
    #[error("context tier cannot be selected with available evidence")]
    UnresolvedTier,
}

/// Failure while authenticating an admission-time frozen selection before it
/// is used for estimation or persistence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FrozenPriceSelectionError {
    #[error("frozen pricing selection digest is missing")]
    MissingDigest,
    #[error("frozen pricing selection digest does not match canonical provenance")]
    DigestMismatch,
    #[error("frozen pricing selection violates a semantic invariant: {0}")]
    SemanticInvariantViolation(&'static str),
}

impl PriceSelectionReasonCode {
    /// Stable persistence/API spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MissingProvider => "missing_provider",
            Self::MissingModel => "missing_model",
            Self::MissingBinding => "missing_binding",
            Self::RetiredBinding => "retired_binding",
            Self::IdentityMismatch => "identity_mismatch",
            Self::Unmetered => "unmetered",
            Self::MissingRate => "missing_rate",
            Self::UnresolvedTier => "unresolved_tier",
        }
    }
}

impl FrozenPriceSelection {
    /// Buckets this frozen selection can charge for, in formula order
    /// (`input`, `output`, `cache_read`, `cache_write`).
    ///
    /// A bucket with no positive rate in the base band or any context tier
    /// cannot move the total (models.dev lists `cache_write: 0` for GLM), so a
    /// report that omits its counter has hidden no spend. Callers use this to tell an omitted-but-free bucket (safe to
    /// read as zero) from an omitted-but-priced one (a real gap).
    pub fn priced_buckets(&self) -> [bool; 4] {
        let mut priced = [false; 4];
        for rates in self
            .rates
            .iter()
            .chain(self.tiers.iter().map(|tier| &tier.rates))
        {
            for (slot, rate) in priced.iter_mut().zip(rates.as_array()) {
                *slot = *slot || rate.is_some_and(|rate| rate.as_nano_usd_per_million() > 0);
            }
        }
        priced
    }

    /// Constructs a frozen selection from trusted admission/persistence
    /// components and computes its canonical provenance digest.  Subsequent
    /// consumers still need to call [`Self::validate`] after crossing an
    /// untrusted boundary.
    #[allow(clippy::too_many_arguments)]
    pub fn from_parts(
        subject_id: Option<String>,
        subject_revision_digest: Option<String>,
        runtime_model: Option<String>,
        candidate_key: Option<String>,
        attempt_ordinal: u32,
        admitted_provider_id: Option<String>,
        catalog_provider_id: Option<String>,
        catalog_model_id: Option<String>,
        source_kind: Option<PricingSourceKind>,
        rate_revision_id: Option<String>,
        catalog_snapshot_id: Option<String>,
        rates: Option<EventBucketRates>,
        tiers: Vec<ContextTier>,
        legacy_context_over_200k: Option<LegacyContextRate>,
        catalog_freshness: CatalogFreshness,
        status: PriceSelectionStatus,
        reason: Option<PriceSelectionReasonCode>,
    ) -> Self {
        let mut selection = Self {
            subject_id,
            subject_revision_digest,
            runtime_model,
            candidate_key,
            attempt_ordinal,
            admitted_provider_id,
            catalog_provider_id,
            catalog_model_id,
            source_kind,
            rate_revision_id,
            catalog_snapshot_id,
            rates,
            tiers,
            legacy_context_over_200k,
            catalog_freshness,
            status,
            reason,
            selection_digest: String::new(),
        };
        selection.selection_digest = frozen_selection_digest(&selection);
        selection
    }

    /// Returns whether this admission selected an exact usable rate.
    pub fn is_priced(&self) -> bool {
        self.validate().is_ok() && matches!(self.status, PriceSelectionStatus::Priced)
    }

    /// Verifies that the immutable provenance has not been mutated after
    /// admission and that its status/source/freshness fields are semantically
    /// consistent. Callers must pass this check before persisting or using a
    /// selection for an estimate.
    pub fn validate(&self) -> Result<(), FrozenPriceSelectionError> {
        if self.selection_digest.is_empty() {
            return Err(FrozenPriceSelectionError::MissingDigest);
        }
        if self.selection_digest != frozen_selection_digest(self) {
            return Err(FrozenPriceSelectionError::DigestMismatch);
        }
        validate_frozen_selection_semantics(self)
    }

    /// Returns this selection only after its canonical provenance digest and
    /// semantic invariants have been verified. Persistence adapters should use
    /// this gate before reading fields for a durable pricing-selection row.
    pub fn validated(&self) -> Result<&Self, FrozenPriceSelectionError> {
        self.validate().map(|()| self)
    }

    /// Returns the canonical selection digest after digest verification.
    pub fn validated_selection_digest(&self) -> Result<&str, FrozenPriceSelectionError> {
        self.validate().map(|()| self.selection_digest.as_str())
    }
}

fn validate_frozen_selection_semantics(
    selection: &FrozenPriceSelection,
) -> Result<(), FrozenPriceSelectionError> {
    let invalid = |reason: &'static str| {
        Err(FrozenPriceSelectionError::SemanticInvariantViolation(
            reason,
        ))
    };
    let optional_fields = [
        selection.subject_id.as_deref(),
        selection.subject_revision_digest.as_deref(),
        selection.runtime_model.as_deref(),
        selection.candidate_key.as_deref(),
        selection.admitted_provider_id.as_deref(),
        selection.catalog_provider_id.as_deref(),
        selection.catalog_model_id.as_deref(),
        selection.rate_revision_id.as_deref(),
        selection.catalog_snapshot_id.as_deref(),
    ];
    if optional_fields
        .iter()
        .flatten()
        .any(|value| value.trim().is_empty())
    {
        return invalid("a provenance field is present but blank");
    }

    if selection.subject_id.is_some() != selection.subject_revision_digest.is_some() {
        return invalid("subject id and revision digest must be present together");
    }
    if selection.status == PriceSelectionStatus::Priced && selection.reason.is_some() {
        return invalid("a priced selection cannot carry an unpriced reason");
    }
    if selection.status == PriceSelectionStatus::Unpriced && selection.reason.is_none() {
        return invalid("an unpriced selection must carry a reason");
    }

    let catalog_provenance_present = selection.catalog_provider_id.is_some()
        || selection.catalog_model_id.is_some()
        || selection.catalog_snapshot_id.is_some();
    match selection.source_kind {
        None => {
            if selection.status == PriceSelectionStatus::Priced
                || selection.rate_revision_id.is_some()
                || catalog_provenance_present
                || selection.rates.is_some()
                || !selection.tiers.is_empty()
                || selection.legacy_context_over_200k.is_some()
                || selection.catalog_freshness != CatalogFreshness::NotApplicable
                || matches!(
                    selection.reason,
                    Some(
                        PriceSelectionReasonCode::MissingRate
                            | PriceSelectionReasonCode::UnresolvedTier
                    )
                )
            {
                return invalid("an unsourced selection cannot carry rate provenance");
            }
        }
        // A manual price may keep context bands: a discount scales every band
        // of the catalog row it came from (gpt-5.6's 272K tier, the 200K
        // tiers on Claude and Gemini Pro), not just the base rate.
        Some(PricingSourceKind::ManualOverride) => {
            if selection.subject_id.is_none()
                || selection.runtime_model.is_none()
                || selection.rate_revision_id.is_none()
                || selection.rates.is_none()
                || catalog_provenance_present
                || selection.catalog_freshness != CatalogFreshness::NotApplicable
                || selection.status != PriceSelectionStatus::Priced
            {
                return invalid(
                    "a manual override must carry its subject, model, rate and rates, and no catalog provenance",
                );
            }
        }
        Some(PricingSourceKind::ModelsDevCatalog) => {
            if selection.runtime_model.is_none()
                || selection.rate_revision_id.is_none()
                || selection.catalog_provider_id.is_none()
                || selection.catalog_model_id.is_none()
                || selection.catalog_snapshot_id.is_none()
                || selection.rates.is_none()
                || selection.catalog_freshness == CatalogFreshness::NotApplicable
            {
                return invalid("a catalog selection is missing its rate or catalog provenance");
            }
            // The admitted provider and the catalog provider are two
            // namespaces, not two copies of one fact: the first is the
            // runtime identity Forge dispatched (`gemini`), the second is
            // the models.dev row the rate came from (`google`). Requiring
            // them to match makes every provider whose labels differ
            // unpriceable, and admission deliberately does not compare them.
            // Settlement is where a provider claim is checked, against the
            // actual provider an execution reported.
            if selection.status == PriceSelectionStatus::Unpriced
                && !matches!(
                    selection.reason,
                    Some(
                        PriceSelectionReasonCode::MissingRate
                            | PriceSelectionReasonCode::UnresolvedTier
                    )
                )
            {
                return invalid("an unpriced catalog selection must name a rate or tier reason");
            }
        }
    }

    if selection.rates.is_some_and(rates_exceed_plausibility_bound)
        || selection
            .tiers
            .iter()
            .any(|tier| rates_exceed_plausibility_bound(tier.rates))
        || selection
            .legacy_context_over_200k
            .as_ref()
            .is_some_and(|legacy| rates_exceed_plausibility_bound(legacy.rates))
    {
        return invalid("a rate exceeds the plausibility bound");
    }

    let mut thresholds = BTreeSet::new();
    if selection
        .tiers
        .iter()
        .any(|tier| tier.threshold_tokens == 0 || !thresholds.insert(tier.threshold_tokens))
    {
        return invalid("context tiers must have distinct nonzero thresholds");
    }
    Ok(())
}

/// Rebuilds a frozen selection from the immutable values persisted by the DB
/// ledger. This is the single constructor used by settlement code so tier,
/// legacy-threshold, catalog freshness, and source provenance all participate
/// in the canonical digest instead of being silently dropped.
#[allow(clippy::too_many_arguments)]
pub fn freeze_persisted_price_selection(
    subject_id: Option<String>,
    subject_revision_digest: Option<String>,
    runtime_model: Option<String>,
    candidate_key: Option<String>,
    attempt_ordinal: u32,
    admitted_provider_id: Option<String>,
    catalog_provider_id: Option<String>,
    catalog_model_id: Option<String>,
    source_kind: Option<PricingSourceKind>,
    rate_revision_id: Option<String>,
    catalog_snapshot_id: Option<String>,
    rates: Option<EventBucketRates>,
    tiers: Vec<ContextTier>,
    legacy_context_over_200k: Option<LegacyContextRate>,
    catalog_freshness: CatalogFreshness,
    status: PriceSelectionStatus,
    reason: Option<PriceSelectionReasonCode>,
) -> FrozenPriceSelection {
    FrozenPriceSelection::from_parts(
        subject_id,
        subject_revision_digest,
        runtime_model,
        candidate_key,
        attempt_ordinal,
        admitted_provider_id,
        catalog_provider_id,
        catalog_model_id,
        source_kind,
        rate_revision_id,
        catalog_snapshot_id,
        rates,
        tiers,
        legacy_context_over_200k,
        catalog_freshness,
        status,
        reason,
    )
}

/// Parses the validated tier JSON retained on an immutable DB rate revision.
/// The parser is shared with catalog ingestion so settlement cannot invent a
/// different tier shape or silently coerce malformed fields.
pub fn parse_persisted_context_tiers(raw_json: &str) -> Result<Vec<ContextTier>, String> {
    validate_bounded_json_fragment(raw_json)?;
    let raw = serde_json::from_str::<Box<RawValue>>(raw_json).map_err(bound_json_error)?;
    parse_tiers(&raw, "$.tiers").map_err(|error| bounded_error_text(&error.to_string()))
}

/// Parses the validated legacy threshold-unknown context JSON retained on an
/// immutable DB rate revision.
pub fn parse_persisted_legacy_context_rate(raw_json: &str) -> Result<LegacyContextRate, String> {
    validate_bounded_json_fragment(raw_json)?;
    let raw = serde_json::from_str::<Box<RawValue>>(raw_json).map_err(bound_json_error)?;
    parse_legacy_context_rate(&raw, "$.context_over_200k")
        .map_err(|error| bounded_error_text(&error.to_string()))
}

fn validate_bounded_json_fragment(raw_json: &str) -> Result<(), String> {
    if raw_json.len() > MODELS_DEV_MAX_RESPONSE_BYTES {
        return Err(format!(
            "JSON fragment exceeds the {MODELS_DEV_MAX_RESPONSE_BYTES}-byte limit"
        ));
    }
    validate_json_nesting_depth(raw_json.as_bytes())
        .map_err(|error| bounded_error_text(&error.to_string()))
}

/// Computes the canonical digest for a frozen selection's provenance.
pub(super) fn frozen_selection_digest(selection: &FrozenPriceSelection) -> String {
    canonical_hash("frozen-selection-v2", |encoded| {
        append_option_str(encoded, selection.subject_id.as_deref());
        append_option_str(encoded, selection.subject_revision_digest.as_deref());
        append_option_str(encoded, selection.runtime_model.as_deref());
        append_option_str(encoded, selection.candidate_key.as_deref());
        append_u32(encoded, selection.attempt_ordinal);
        append_option_str(encoded, selection.admitted_provider_id.as_deref());
        append_option_enum_str(
            encoded,
            selection.source_kind.map(PricingSourceKind::as_str),
        );
        append_option_str(encoded, selection.rate_revision_id.as_deref());
        append_option_str(encoded, selection.catalog_snapshot_id.as_deref());
        append_option_str(encoded, selection.catalog_provider_id.as_deref());
        append_option_str(encoded, selection.catalog_model_id.as_deref());
        append_option_rates(encoded, selection.rates);
        append_tiers(encoded, &selection.tiers);
        append_option_legacy_context(encoded, selection.legacy_context_over_200k.as_ref());
        append_str(encoded, selection.catalog_freshness.as_str());
        append_str(encoded, selection.status.as_str());
        append_option_enum_str(
            encoded,
            selection.reason.map(PriceSelectionReasonCode::as_str),
        );
    })
}
