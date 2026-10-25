//! Catalog snapshots, source parsing, and repository contracts.

use super::*;

// ---------------------------------------------------------------------------
// models.dev catalog ingestion
// ---------------------------------------------------------------------------

/// Freshness of a catalog snapshot at the time it is used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CatalogFreshness {
    /// The last conditional check succeeded within the stale-age window.
    Fresh,
    /// The last successful check is older than [`MODELS_DEV_STALE_AFTER`].
    Stale,
    /// The last refresh failed, but an older active snapshot remains usable.
    RefreshFailed,
    /// The source is not a catalog (for example, a manual override).
    NotApplicable,
}

impl CatalogFreshness {
    /// Stable spelling used by canonical provenance encodings.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Stale => "stale",
            Self::RefreshFailed => "refresh_failed",
            Self::NotApplicable => "not_applicable",
        }
    }
}

/// Mutable catalog state held next to immutable snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CatalogState {
    /// No successful snapshot has ever been activated.
    Absent,
    /// An active snapshot was checked recently.
    Fresh,
    /// An active snapshot exists but has exceeded the stale-age window.
    Stale,
    /// The latest refresh failed.  The active snapshot, if any, is still LKG.
    RefreshFailed,
}

impl CatalogState {
    /// Returns the corresponding estimate provenance freshness.
    pub const fn freshness(self) -> CatalogFreshness {
        match self {
            Self::Absent => CatalogFreshness::NotApplicable,
            Self::Fresh => CatalogFreshness::Fresh,
            Self::Stale => CatalogFreshness::Stale,
            Self::RefreshFailed => CatalogFreshness::RefreshFailed,
        }
    }
}

/// A context-sensitive pricing band from the exact `cost.tiers` field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextTier {
    /// The context threshold supplied by models.dev.
    pub threshold_tokens: u64,
    /// Rates that apply at this threshold.
    pub rates: EventBucketRates,
    /// Original tier JSON, retained for audit and future parser revisions.
    pub raw_json: String,
}

/// Legacy `cost.context_over_200k` pricing.  Its threshold is intentionally
/// unknown: the field name is not evidence that a producer crossed exactly
/// 200,000 tokens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyContextRate {
    /// The optional legacy buckets, preserving absent versus explicit zero.
    pub rates: EventBucketRates,
    /// Original legacy JSON, retained as immutable provenance.
    pub raw_json: String,
}

/// Context-tier provenance for one catalog model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ContextTierState {
    /// No tier fields were supplied.
    None,
    /// One or more exact context bands were supplied and validated.
    Resolved,
    /// Only the legacy threshold-unknown field was supplied.
    ThresholdUnknown,
}

impl ContextTierState {
    /// Stable spelling used by canonical catalog-rate encodings.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Resolved => "resolved",
            Self::ThresholdUnknown => "threshold_unknown",
        }
    }
}

/// Four independently measurable catalog rates.  The alias is intentional:
/// callers use the same absent/zero semantics for catalog rows and events.
pub type RateBuckets = EventBucketRates;

/// One provider/model row normalized from models.dev `/api.json`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogModelRate {
    /// Top-level `/api.json` provider map key.
    pub provider_id: String,
    /// Provider-scoped nested model map key.
    pub model_id: String,
    /// The model key exactly as received from the source map.
    pub source_model_key: String,
    /// Optional source metadata; never used as a revision.
    pub source_last_updated: Option<String>,
    /// Base rates. `None` is unknown; `Some(ZERO)` is explicitly free.
    pub rates: EventBucketRates,
    /// Exact context bands, sorted by threshold ascending.
    pub tiers: Vec<ContextTier>,
    /// Raw legacy-only context pricing, if present.
    pub legacy_context_over_200k: Option<LegacyContextRate>,
    /// Whether context pricing is exact, unknown, or absent.
    pub context_tier_state: ContextTierState,
    /// Original `cost` object, including ignored source fields.
    pub received_rates_json: Option<String>,
}

impl CatalogModelRate {
    /// Returns whether the source included a `cost` object at all.
    pub fn has_cost(&self) -> bool {
        self.received_rates_json.is_some()
    }

    /// Looks up this exact provider/model row in a snapshot-independent list.
    pub fn exact_key(&self) -> (&str, &str) {
        (&self.provider_id, &self.model_id)
    }

    /// Serializes the validated exact tiers without converting source numbers
    /// through binary floating point.
    pub fn tiers_json(&self) -> String {
        let items = self
            .tiers
            .iter()
            .map(|tier| tier.raw_json.as_str())
            .collect::<Vec<_>>()
            .join(",");
        format!("[{items}]")
    }

    /// Returns the retained legacy context JSON, if supplied.
    pub fn legacy_context_over_200k_json(&self) -> Option<&str> {
        self.legacy_context_over_200k
            .as_ref()
            .map(|legacy| legacy.raw_json.as_str())
    }

    /// Returns the retained raw `cost` object, or an empty JSON object when
    /// the source model was valid but unpriced.
    pub fn received_rates_json(&self) -> &str {
        self.received_rates_json.as_deref().unwrap_or("{}")
    }

    /// Computes a deterministic normalized-row digest for immutable rate
    /// revision identity.  Source floating-point spellings remain in the raw
    /// provenance field while the normalized buckets drive this digest.
    pub fn rate_digest(&self, snapshot_id: &str) -> String {
        canonical_hash("catalog-rate-v2", |encoded| {
            append_str(encoded, snapshot_id);
            append_str(encoded, &self.provider_id);
            append_str(encoded, &self.model_id);
            append_str(encoded, &self.source_model_key);
            append_option_str(encoded, self.source_last_updated.as_deref());
            append_rates(encoded, self.rates);
            append_tiers(encoded, &self.tiers);
            append_option_legacy_context(encoded, self.legacy_context_over_200k.as_ref());
            append_str(encoded, self.context_tier_state.as_str());
            append_option_str(encoded, self.received_rates_json.as_deref());
        })
    }
}

/// A parsed and validated source payload before it is assigned a snapshot ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedModelsDevCatalog {
    /// The exact decoded bytes passed to the parser.
    pub raw_payload: Vec<u8>,
    /// SHA-256 of [`Self::raw_payload`], lowercase hexadecimal.
    pub payload_sha256: String,
    /// Parser revision that produced [`Self::models`].
    pub parser_revision: String,
    /// All normalized provider/model rows in deterministic key order.
    pub models: Vec<CatalogModelRate>,
}

impl ParsedModelsDevCatalog {
    /// Finds an exact provider/model row without aliases or fuzzy matching.
    pub fn model_rate(&self, provider_id: &str, model_id: &str) -> Option<&CatalogModelRate> {
        self.models
            .iter()
            .find(|model| model.provider_id == provider_id && model.model_id == model_id)
    }

    /// Materializes an immutable snapshot for atomic repository activation.
    pub fn into_snapshot(
        self,
        snapshot_id: impl Into<String>,
        etag: Option<String>,
        fetched_at: SystemTime,
        created_at: SystemTime,
    ) -> Result<CatalogSnapshot, CatalogSnapshotError> {
        CatalogSnapshot::from_parsed(self, snapshot_id.into(), etag, fetched_at, created_at)
    }
}

/// An immutable normalized catalog snapshot and its raw source payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogSnapshot {
    /// Application-generated immutable snapshot ID.
    pub id: String,
    /// Fixed source URL.
    pub source_url: String,
    /// Conditional HTTP validator, when supplied by models.dev.
    pub etag: Option<String>,
    /// Exact source-body digest.
    pub payload_sha256: String,
    /// Parser/schema revision used to normalize the source.
    pub parser_revision: String,
    /// Forge revision = SHA-256(payload digest + parser revision).
    pub revision_digest: String,
    /// Bounded raw JSON bytes retained for 304 parser-revision reparses.
    pub raw_payload: Vec<u8>,
    /// Normalized provider/model rows.
    pub models: Vec<CatalogModelRate>,
    /// Successful retrieval/check time represented by this snapshot.
    pub fetched_at: SystemTime,
    /// Immutable row creation time.
    pub created_at: SystemTime,
}

impl CatalogSnapshot {
    /// Builds and validates a snapshot from parser output.
    pub fn from_parsed(
        parsed: ParsedModelsDevCatalog,
        snapshot_id: String,
        etag: Option<String>,
        fetched_at: SystemTime,
        created_at: SystemTime,
    ) -> Result<Self, CatalogSnapshotError> {
        if snapshot_id.trim().is_empty() || snapshot_id.len() > MODELS_DEV_MAX_IDENTIFIER_BYTES {
            return Err(CatalogSnapshotError::InvalidSnapshotId);
        }
        if parsed.raw_payload.len() > MODELS_DEV_MAX_RESPONSE_BYTES {
            return Err(CatalogSnapshotError::ResponseTooLarge);
        }
        if etag.as_deref().is_some_and(|value| {
            value.len() > MODELS_DEV_MAX_OPTIONAL_STRING_BYTES
                || value.chars().any(char::is_control)
        }) {
            return Err(CatalogSnapshotError::InvalidEtag);
        }
        if parsed.models.is_empty() {
            return Err(CatalogSnapshotError::EmptyCatalog);
        }
        let expected_digest = sha256_hex(&parsed.raw_payload);
        if expected_digest.as_bytes() != parsed.payload_sha256.as_bytes() {
            return Err(CatalogSnapshotError::PayloadDigestMismatch);
        }
        let revision_digest =
            catalog_revision_digest(&parsed.payload_sha256, &parsed.parser_revision);
        Ok(Self {
            id: snapshot_id,
            source_url: MODELS_DEV_ENDPOINT.to_owned(),
            etag,
            payload_sha256: parsed.payload_sha256,
            parser_revision: parsed.parser_revision,
            revision_digest,
            raw_payload: parsed.raw_payload,
            models: parsed.models,
            fetched_at,
            created_at,
        })
    }

    /// Finds an exact provider/model row.  No provider prefixes are added.
    pub fn model_rate(&self, provider_id: &str, model_id: &str) -> Option<&CatalogModelRate> {
        self.models
            .iter()
            .find(|model| model.provider_id == provider_id && model.model_id == model_id)
    }

    /// Computes the snapshot's current freshness from a successful check time.
    pub fn freshness_at(&self, now: SystemTime) -> CatalogFreshness {
        if models_dev_catalog_is_stale(self.fetched_at, now) {
            CatalogFreshness::Stale
        } else {
            CatalogFreshness::Fresh
        }
    }
}

/// Errors while materializing immutable catalog snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CatalogSnapshotError {
    #[error("catalog snapshot ID is empty")]
    InvalidSnapshotId,
    #[error("catalog snapshot payload exceeds the configured size limit")]
    ResponseTooLarge,
    #[error("catalog snapshot ETag is invalid or exceeds the configured bound")]
    InvalidEtag,
    #[error("catalog snapshot contains no models")]
    EmptyCatalog,
    #[error("catalog snapshot payload digest does not match its bytes")]
    PayloadDigestMismatch,
}

/// Computes Forge's immutable catalog revision from payload digest and parser.
pub fn catalog_revision_digest(payload_sha256: &str, parser_revision: &str) -> String {
    canonical_hash("catalog-revision-v2", |encoded| {
        append_str(encoded, payload_sha256);
        append_str(encoded, parser_revision);
    })
}

/// Catalog parse/validation failure.  Error details contain only bounded
/// field/path metadata, never the raw response body.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CatalogParseError {
    #[error("models.dev catalog response exceeds the {limit}-byte limit")]
    ResponseTooLarge { limit: usize },
    #[error("models.dev catalog response is empty")]
    EmptyPayload,
    #[error("models.dev catalog response is invalid JSON: {0}")]
    InvalidJson(String),
    #[error("models.dev catalog exceeds the maximum JSON nesting depth of {limit}")]
    NestingTooDeep { limit: usize },
    #[error("models.dev catalog root must be a non-empty provider object")]
    InvalidRoot,
    #[error("models.dev catalog contains duplicate {path} key {key:?}")]
    DuplicateKey { path: String, key: String },
    #[error("models.dev catalog field {path} is invalid: {reason}")]
    InvalidField { path: String, reason: String },
    #[error("models.dev catalog field {path} is missing")]
    MissingField { path: String },
}

/// Parses a bounded models.dev `/api.json` payload with lexical rates.
pub fn parse_models_dev_catalog(body: &[u8]) -> Result<ParsedModelsDevCatalog, CatalogParseError> {
    if body.len() > MODELS_DEV_MAX_RESPONSE_BYTES {
        return Err(CatalogParseError::ResponseTooLarge {
            limit: MODELS_DEV_MAX_RESPONSE_BYTES,
        });
    }
    if body.is_empty() {
        return Err(CatalogParseError::EmptyPayload);
    }
    validate_json_nesting_depth(body)?;

    let root = serde_json::from_slice::<RawObject>(body).map_err(|error| {
        match duplicate_key_from_json_error(&error) {
            Some(key) => CatalogParseError::DuplicateKey {
                path: "root".to_owned(),
                key,
            },
            None => CatalogParseError::InvalidJson(bound_json_error(error)),
        }
    })?;
    if root.0.is_empty() {
        return Err(CatalogParseError::InvalidRoot);
    }

    let mut models = Vec::new();
    let mut seen_model_keys = BTreeSet::new();
    for (provider_key, provider_raw) in root.0 {
        validate_non_empty_identifier(&provider_key, "provider key")?;
        let provider_path = format!("provider[{}]", bounded_error_text(&provider_key));
        let provider = parse_raw_object(&provider_raw, &provider_path)?;
        let provider_id = parse_required_string(&provider, "id", &format!("{provider_path}.id"))?;
        if provider_id != provider_key {
            return Err(CatalogParseError::InvalidField {
                path: format!("{provider_path}.id"),
                reason: "must equal the top-level provider map key".to_owned(),
            });
        }
        // Names are not part of the normalized billing identity, but they are
        // bounded source metadata.  Validate them when present so an ignored
        // display field cannot consume the full response budget by itself.
        let _ = parse_optional_string(&provider, "name", &format!("{provider_path}.name"))?;
        let models_raw = required_raw(&provider, "models", &format!("{provider_path}.models"))?;
        let model_object = parse_raw_object(models_raw, &format!("{provider_path}.models"))?;
        if model_object.0.is_empty() {
            return Err(CatalogParseError::InvalidField {
                path: format!("{provider_path}.models"),
                reason: "must contain at least one model".to_owned(),
            });
        }
        for (model_key, model_raw) in model_object.0 {
            validate_non_empty_identifier(&model_key, "model key")?;
            if !seen_model_keys.insert((provider_key.clone(), model_key.clone())) {
                return Err(CatalogParseError::DuplicateKey {
                    path: format!("{provider_path}.models"),
                    key: bounded_error_text(&model_key),
                });
            }
            let model_path = format!("{provider_path}.models[{}]", bounded_error_text(&model_key));
            let model = parse_raw_object(&model_raw, &model_path)?;
            let model_id = parse_required_string(&model, "id", &format!("{model_path}.id"))?;
            if model_id != model_key {
                return Err(CatalogParseError::InvalidField {
                    path: format!("{model_path}.id"),
                    reason: "must equal the provider-scoped model map key".to_owned(),
                });
            }
            let _ = parse_optional_string(&model, "name", &format!("{model_path}.name"))?;
            let source_last_updated = parse_optional_string(
                &model,
                "last_updated",
                &format!("{model_path}.last_updated"),
            )?;
            let _ = parse_optional_string(
                &model,
                "release_date",
                &format!("{model_path}.release_date"),
            )?;
            let cost_raw = model
                .0
                .iter()
                .find(|(key, _)| key == "cost")
                .map(|(_, value)| value);
            let (rates, tiers, legacy_context_over_200k, context_tier_state, received_rates_json) =
                match cost_raw {
                    None => (
                        EventBucketRates::default(),
                        Vec::new(),
                        None,
                        ContextTierState::None,
                        None,
                    ),
                    Some(cost) => parse_cost(cost, &format!("{model_path}.cost"))?,
                };
            models.push(CatalogModelRate {
                provider_id: provider_key.clone(),
                model_id: model_key.clone(),
                source_model_key: model_key,
                source_last_updated,
                rates,
                tiers,
                legacy_context_over_200k,
                context_tier_state,
                received_rates_json,
            });
        }
    }
    if models.is_empty() {
        return Err(CatalogParseError::InvalidRoot);
    }
    models.sort_by(|left, right| left.exact_key().cmp(&right.exact_key()));
    Ok(ParsedModelsDevCatalog {
        payload_sha256: sha256_hex(body),
        raw_payload: body.to_vec(),
        parser_revision: MODELS_DEV_PARSER_REVISION.to_owned(),
        models,
    })
}

#[derive(Debug)]
struct RawObject(Vec<(String, Box<RawValue>)>);

impl<'de> Deserialize<'de> for RawObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct RawObjectVisitor;

        impl<'de> Visitor<'de> for RawObjectVisitor {
            type Value = RawObject;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON object with unique keys")
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut entries = Vec::new();
                let mut seen = BTreeSet::new();
                while let Some((key, value)) = map.next_entry::<String, Box<RawValue>>()? {
                    if !seen.insert(key.clone()) {
                        return Err(de::Error::custom(format!(
                            "duplicate object key {}",
                            bounded_error_text(&key)
                        )));
                    }
                    entries.push((key, value));
                }
                Ok(RawObject(entries))
            }
        }

        deserializer.deserialize_map(RawObjectVisitor)
    }
}

fn parse_raw_object(raw: &RawValue, path: &str) -> Result<RawObject, CatalogParseError> {
    serde_json::from_str::<RawObject>(raw.get()).map_err(|error| {
        if let Some(key) = duplicate_key_from_json_error(&error) {
            CatalogParseError::DuplicateKey {
                path: path.to_owned(),
                key,
            }
        } else {
            CatalogParseError::InvalidField {
                path: path.to_owned(),
                reason: bound_json_error(error),
            }
        }
    })
}

fn required_raw<'a>(
    object: &'a RawObject,
    field: &str,
    path: &str,
) -> Result<&'a RawValue, CatalogParseError> {
    object
        .0
        .iter()
        .find(|(key, _)| key == field)
        .map(|(_, value)| value.as_ref())
        .ok_or_else(|| CatalogParseError::MissingField {
            path: path.to_owned(),
        })
}

fn parse_required_string(
    object: &RawObject,
    field: &str,
    path: &str,
) -> Result<String, CatalogParseError> {
    let raw = required_raw(object, field, path)?;
    let value =
        serde_json::from_str::<String>(raw.get()).map_err(|_| CatalogParseError::InvalidField {
            path: path.to_owned(),
            reason: "must be a JSON string".to_owned(),
        })?;
    validate_non_empty_identifier(&value, path)?;
    Ok(value)
}

fn parse_optional_string(
    object: &RawObject,
    field: &str,
    path: &str,
) -> Result<Option<String>, CatalogParseError> {
    let Some((_, raw)) = object.0.iter().find(|(key, _)| key == field) else {
        return Ok(None);
    };
    let value =
        serde_json::from_str::<String>(raw.get()).map_err(|_| CatalogParseError::InvalidField {
            path: path.to_owned(),
            reason: "must be a JSON string when present".to_owned(),
        })?;
    if value.len() > MODELS_DEV_MAX_OPTIONAL_STRING_BYTES {
        return Err(CatalogParseError::InvalidField {
            path: path.to_owned(),
            reason: format!("must be at most {MODELS_DEV_MAX_OPTIONAL_STRING_BYTES} UTF-8 bytes"),
        });
    }
    Ok(Some(value))
}

fn validate_non_empty_identifier(value: &str, path: &str) -> Result<(), CatalogParseError> {
    if value.trim().is_empty() {
        return Err(CatalogParseError::InvalidField {
            path: path.to_owned(),
            reason: "must not be empty".to_owned(),
        });
    }
    if value.len() > MODELS_DEV_MAX_IDENTIFIER_BYTES {
        return Err(CatalogParseError::InvalidField {
            path: path.to_owned(),
            reason: format!("must be at most {MODELS_DEV_MAX_IDENTIFIER_BYTES} UTF-8 bytes"),
        });
    }
    Ok(())
}

pub(super) fn validate_json_nesting_depth(body: &[u8]) -> Result<(), CatalogParseError> {
    let mut depth = 0_usize;
    let mut in_string = false;
    let mut escaped = false;
    for &byte in body {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth = depth.saturating_add(1);
                if depth > MODELS_DEV_MAX_JSON_NESTING_DEPTH {
                    return Err(CatalogParseError::NestingTooDeep {
                        limit: MODELS_DEV_MAX_JSON_NESTING_DEPTH,
                    });
                }
            }
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    Ok(())
}

type ParsedCost = (
    EventBucketRates,
    Vec<ContextTier>,
    Option<LegacyContextRate>,
    ContextTierState,
    Option<String>,
);

fn parse_cost(raw: &RawValue, path: &str) -> Result<ParsedCost, CatalogParseError> {
    let cost = parse_raw_object(raw, path)?;
    let rates = EventBucketRates {
        input: Some(parse_required_rate(
            &cost,
            "input",
            &format!("{path}.input"),
        )?),
        output: Some(parse_required_rate(
            &cost,
            "output",
            &format!("{path}.output"),
        )?),
        cache_read: parse_optional_rate(&cost, "cache_read", &format!("{path}.cache_read"))?,
        cache_write: parse_optional_rate(&cost, "cache_write", &format!("{path}.cache_write"))?,
    };

    // These are known source fields but are not measurable in this revision.
    // Validate them lexically so malformed known data cannot be activated;
    // retain the complete raw cost object for future parser revisions.
    for field in ["reasoning", "input_audio", "output_audio"] {
        let _ = parse_optional_rate(&cost, field, &format!("{path}.{field}"))?;
    }

    let tiers = match cost.0.iter().find(|(key, _)| key == "tiers") {
        Some((_, raw_tiers)) => parse_tiers(raw_tiers, &format!("{path}.tiers"))?,
        None => Vec::new(),
    };
    let legacy = match cost.0.iter().find(|(key, _)| key == "context_over_200k") {
        Some((_, raw_legacy)) => Some(parse_legacy_context_rate(
            raw_legacy,
            &format!("{path}.context_over_200k"),
        )?),
        None => None,
    };
    let context_tier_state = if !tiers.is_empty() {
        ContextTierState::Resolved
    } else if legacy.is_some() {
        ContextTierState::ThresholdUnknown
    } else {
        ContextTierState::None
    };
    Ok((
        rates,
        tiers,
        legacy,
        context_tier_state,
        Some(raw.get().to_owned()),
    ))
}

fn parse_required_rate(
    object: &RawObject,
    field: &str,
    path: &str,
) -> Result<NanoUsdPerMillion, CatalogParseError> {
    let raw = required_raw(object, field, path)?;
    parse_rate_value(raw, path)
}

fn parse_optional_rate(
    object: &RawObject,
    field: &str,
    path: &str,
) -> Result<Option<NanoUsdPerMillion>, CatalogParseError> {
    object
        .0
        .iter()
        .find(|(key, _)| key == field)
        .map(|(_, raw)| parse_rate_value(raw, path).map(Some))
        .unwrap_or(Ok(None))
}

fn parse_rate_value(raw: &RawValue, path: &str) -> Result<NanoUsdPerMillion, CatalogParseError> {
    parse_models_dev_json_number(raw.get()).map_err(|error| CatalogParseError::InvalidField {
        path: path.to_owned(),
        reason: error.to_string(),
    })
}

pub(super) fn parse_tiers(
    raw: &RawValue,
    path: &str,
) -> Result<Vec<ContextTier>, CatalogParseError> {
    let tiers = serde_json::from_str::<Vec<Box<RawValue>>>(raw.get()).map_err(|error| {
        CatalogParseError::InvalidField {
            path: path.to_owned(),
            reason: bound_json_error(error),
        }
    })?;
    let mut parsed = Vec::with_capacity(tiers.len());
    let mut seen_thresholds = BTreeSet::new();
    for (index, tier_raw) in tiers.iter().enumerate() {
        let tier_path = format!("{path}[{index}]");
        let tier = parse_raw_object(tier_raw, &tier_path)?;
        let tier_meta = parse_raw_object(
            required_raw(&tier, "tier", &format!("{tier_path}.tier"))?,
            &format!("{tier_path}.tier"),
        )?;
        let tier_type =
            parse_required_string(&tier_meta, "type", &format!("{tier_path}.tier.type"))?;
        if tier_type != "context" {
            return Err(CatalogParseError::InvalidField {
                path: format!("{tier_path}.tier.type"),
                reason: "only context tiers are supported".to_owned(),
            });
        }
        let threshold = parse_required_u64(&tier_meta, "size", &format!("{tier_path}.tier.size"))?;
        if threshold == 0 {
            return Err(CatalogParseError::InvalidField {
                path: format!("{tier_path}.tier.size"),
                reason: "must be greater than zero".to_owned(),
            });
        }
        if !seen_thresholds.insert(threshold) {
            return Err(CatalogParseError::DuplicateKey {
                path: path.to_owned(),
                key: threshold.to_string(),
            });
        }
        let rates = EventBucketRates {
            input: Some(parse_required_rate(
                &tier,
                "input",
                &format!("{tier_path}.input"),
            )?),
            output: Some(parse_required_rate(
                &tier,
                "output",
                &format!("{tier_path}.output"),
            )?),
            cache_read: parse_optional_rate(
                &tier,
                "cache_read",
                &format!("{tier_path}.cache_read"),
            )?,
            cache_write: parse_optional_rate(
                &tier,
                "cache_write",
                &format!("{tier_path}.cache_write"),
            )?,
        };
        for field in ["reasoning", "input_audio", "output_audio"] {
            let _ = parse_optional_rate(&tier, field, &format!("{tier_path}.{field}"))?;
        }
        parsed.push(ContextTier {
            threshold_tokens: threshold,
            rates,
            raw_json: tier_raw.get().to_owned(),
        });
    }
    parsed.sort_by_key(|tier| tier.threshold_tokens);
    Ok(parsed)
}

pub(super) fn parse_legacy_context_rate(
    raw: &RawValue,
    path: &str,
) -> Result<LegacyContextRate, CatalogParseError> {
    let legacy = parse_raw_object(raw, path)?;
    let rates = EventBucketRates {
        input: parse_optional_rate(&legacy, "input", &format!("{path}.input"))?,
        output: parse_optional_rate(&legacy, "output", &format!("{path}.output"))?,
        cache_read: parse_optional_rate(&legacy, "cache_read", &format!("{path}.cache_read"))?,
        cache_write: parse_optional_rate(&legacy, "cache_write", &format!("{path}.cache_write"))?,
    };
    Ok(LegacyContextRate {
        rates,
        raw_json: raw.get().to_owned(),
    })
}

fn parse_required_u64(
    object: &RawObject,
    field: &str,
    path: &str,
) -> Result<u64, CatalogParseError> {
    let raw = required_raw(object, field, path)?;
    let value =
        serde_json::from_str::<u64>(raw.get()).map_err(|_| CatalogParseError::InvalidField {
            path: path.to_owned(),
            reason: "must be a non-negative JSON integer".to_owned(),
        })?;
    Ok(value)
}

pub(super) fn bound_json_error(error: serde_json::Error) -> String {
    // Serde's error is useful for tests and diagnostics, but cap it so a
    // hostile payload cannot turn a bounded response into an unbounded error.
    let mut text = error.to_string();
    if text.len() > 256 {
        text.truncate(256);
    }
    text
}

pub(super) fn bounded_error_text(value: &str) -> String {
    value.chars().take(128).collect()
}

fn duplicate_key_from_json_error(error: &serde_json::Error) -> Option<String> {
    let text = error.to_string();
    let key = text.strip_prefix("duplicate object key ")?;
    let key = key.split(" at line ").next().unwrap_or(key);
    Some(bounded_error_text(key))
}

/// A compact catalog status value returned by the repository boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogStatus {
    /// Derived state as of the repository's last check.
    pub state: CatalogState,
    /// Active last-known-good snapshot ID, if any.
    pub active_snapshot_id: Option<String>,
    /// Active snapshot revision digest, if any.
    pub revision: Option<String>,
    /// Current HTTP ETag, if any.
    pub etag: Option<String>,
    /// Last attempted check time.
    pub last_checked_at: Option<SystemTime>,
    /// Last successful 200 or 304 check time.
    pub last_successful_check_at: Option<SystemTime>,
    /// The configured stale boundary, if an active snapshot exists.
    pub stale_after: Option<SystemTime>,
    /// Redacted machine-readable failure code from the latest failed check.
    pub last_error_code: Option<String>,
    /// Last completed refresh idempotency key.
    pub last_idempotency_key: Option<String>,
    /// Repository version used by single-flight/concurrency checks.
    pub version: u64,
}

impl CatalogStatus {
    /// Returns an empty status before the first successful activation.
    pub fn absent() -> Self {
        Self {
            state: CatalogState::Absent,
            active_snapshot_id: None,
            revision: None,
            etag: None,
            last_checked_at: None,
            last_successful_check_at: None,
            stale_after: None,
            last_error_code: None,
            last_idempotency_key: None,
            version: 1,
        }
    }

    /// Recomputes freshness from the persisted stale boundary (falling back
    /// to the catalog's fixed policy for older rows that predate that field).
    pub fn state_at(&self, now: SystemTime) -> CatalogState {
        if self.last_error_code.is_some() {
            return CatalogState::RefreshFailed;
        }
        if self.active_snapshot_id.is_none() {
            return CatalogState::Absent;
        }
        let derived_stale_after = self
            .last_successful_check_at
            .and_then(|checked_at| checked_at.checked_add(MODELS_DEV_STALE_AFTER));
        // A conditional refresh advances last_successful_check_at.  If an
        // older row retained the previous boundary, it must not make the
        // freshly revalidated snapshot stale immediately.
        let stale_after = match (self.stale_after, derived_stale_after) {
            (Some(persisted), Some(derived)) => Some(persisted.max(derived)),
            (Some(persisted), None) => Some(persisted),
            (None, derived) => derived,
        };
        match stale_after {
            Some(stale_after) if now > stale_after => CatalogState::Stale,
            Some(_) if self.last_successful_check_at.is_some() => CatalogState::Fresh,
            _ => CatalogState::Stale,
        }
    }

    /// Returns a copy with state derived at `now`.
    pub fn at(&self, now: SystemTime) -> Self {
        let mut status = self.clone();
        status.state = self.state_at(now);
        if let Some(derived) = status
            .last_successful_check_at
            .and_then(|checked_at| checked_at.checked_add(MODELS_DEV_STALE_AFTER))
        {
            if status
                .stale_after
                .is_none_or(|persisted| persisted < derived)
            {
                status.stale_after = Some(derived);
            }
        }
        status
    }

    /// Returns the estimate freshness represented by this status at `now`.
    pub fn freshness_at(&self, now: SystemTime) -> CatalogFreshness {
        self.state_at(now).freshness()
    }

    /// Returns freshness for `snapshot`, using the latest successful check
    /// when this status still points at that active snapshot.  This preserves
    /// 304 freshness semantics while falling back safely to the immutable
    /// snapshot's own successful retrieval time for unrelated status values.
    pub fn freshness_for_snapshot(
        &self,
        snapshot: &CatalogSnapshot,
        now: SystemTime,
    ) -> CatalogFreshness {
        let refers_to_snapshot = self.active_snapshot_id.as_deref() == Some(snapshot.id.as_str())
            && self
                .revision
                .as_deref()
                .is_none_or(|revision| revision == snapshot.revision_digest.as_str());
        if refers_to_snapshot {
            if self.last_error_code.is_some() {
                return CatalogFreshness::RefreshFailed;
            }
            if self.last_successful_check_at.is_some() {
                return self.freshness_at(now);
            }
            // A legacy/incomplete status row may identify the active
            // snapshot without retaining its successful-check timestamp.
            // The immutable snapshot timestamp is still authoritative data
            // in that case; do not turn available freshness into a synthetic
            // stale result merely because the mutable status is incomplete.
        }
        snapshot.freshness_at(now)
    }
}

/// Errors from a future DB-backed catalog repository.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CatalogRepositoryError {
    /// An optimistic version or idempotency conflict.
    #[error("pricing catalog repository conflict: {0}")]
    Conflict(String),
    /// The repository could not complete its transaction.
    #[error("pricing catalog repository unavailable: {0}")]
    Unavailable(String),
}

/// Atomic catalog persistence boundary.  `activate_snapshot` must insert the
/// immutable payload, all normalized rates, and the active-state pointer in
/// one transaction; a failure must leave the prior active snapshot intact.
#[async_trait]
pub trait PricingCatalogRepository: Send + Sync {
    /// Reads the current mutable state without changing it.
    async fn catalog_status(&self) -> Result<CatalogStatus, CatalogRepositoryError>;

    /// Looks up a durable refresh outcome before any transport is attempted.
    ///
    /// Implementations that persist operation receipts should return the
    /// original outcome for the supplied key. The default keeps lightweight
    /// in-memory repositories source-compatible; concrete persistence
    /// adapters override it.
    async fn replay_catalog_refresh(
        &self,
        _idempotency_key: &str,
    ) -> Result<Option<CatalogRefreshOutcome>, CatalogRepositoryError> {
        Ok(None)
    }

    /// Durably records a serialized no-op result for a refresh that lost a
    /// single-flight race to another key. This prevents retrying that key
    /// from issuing a later network fetch with a different outcome.
    async fn record_catalog_refresh_already_refreshed(
        &self,
        _checked_at: SystemTime,
        _idempotency_key: &str,
    ) -> Result<CatalogStatus, CatalogRepositoryError> {
        self.catalog_status().await
    }

    /// Returns the active immutable payload for a 304 parser-revision reparse.
    async fn active_catalog_snapshot(
        &self,
    ) -> Result<Option<CatalogSnapshot>, CatalogRepositoryError>;

    /// Atomically stores and activates a complete immutable snapshot.
    async fn activate_catalog_snapshot(
        &self,
        snapshot: CatalogSnapshot,
        idempotency_key: &str,
    ) -> Result<CatalogStatus, CatalogRepositoryError>;

    /// Atomically records a successful 304 check without making a new source
    /// payload revision.
    async fn record_catalog_not_modified(
        &self,
        checked_at: SystemTime,
        etag: Option<String>,
        idempotency_key: &str,
    ) -> Result<CatalogStatus, CatalogRepositoryError>;

    /// Records a redacted failure while preserving the active LKG snapshot.
    async fn record_catalog_refresh_failure(
        &self,
        checked_at: SystemTime,
        code: CatalogRefreshErrorCode,
        idempotency_key: &str,
    ) -> Result<CatalogStatus, CatalogRepositoryError>;
}
