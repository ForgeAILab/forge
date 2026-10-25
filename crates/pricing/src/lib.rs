//! Provider pricing catalog, exact bindings, and fixed-point estimation.
//!
//! The module keeps network and persistence boundaries deliberately small:
//! [`ModelsDevClient`] owns bounded conditional fetching and parsing, while
//! [`PricingCatalogRepository`] is the atomic activation boundary that a DB
//! implementation can satisfy later.  The pure binding and estimation types
//! are also useful to producers that need admission-time price freezing
//! without depending on a concrete database.

use std::{
    collections::BTreeSet,
    fmt,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use reqwest::header::{CONTENT_TYPE, ETAG, IF_NONE_MATCH};
use serde::{de, de::MapAccess, de::Visitor, Deserialize};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

pub mod auto;
mod error;
pub mod sqlite;

#[cfg(test)]
#[path = "auto_tests.rs"]
mod auto_tests;

pub use error::{PricingError, PricingResult};

mod catalog;
mod client;
mod estimate;
mod retrospective;
mod selection;

pub use catalog::{
    catalog_revision_digest, parse_models_dev_catalog, CatalogFreshness, CatalogModelRate,
    CatalogParseError, CatalogRepositoryError, CatalogSnapshot, CatalogSnapshotError, CatalogState,
    CatalogStatus, ContextTier, ContextTierState, LegacyContextRate, ParsedModelsDevCatalog,
    PricingCatalogRepository, RateBuckets,
};
pub use client::{
    CatalogClientError, CatalogRefreshErrorCode, CatalogRefreshOutcome, CatalogRefreshRequest,
    ModelsDevClient, ModelsDevHttpResponse, ModelsDevTransport, ModelsDevTransportError,
    ReqwestModelsDevTransport,
};
pub use estimate::{
    estimate_usage_event, select_context_rates, EstimateProvenance, EventEstimate,
    UsageEventEstimateInput,
};
pub use retrospective::{
    commit_retrospective_preview, preview_cost_estimation_with_freshness,
    preview_retrospective_estimates, preview_retrospective_estimates_with_freshness,
    retrospective_usage_set_digest, RetrospectiveCommitRequest, RetrospectiveEstimateCandidate,
    RetrospectiveEstimateRepository, RetrospectivePreview, RetrospectivePreviewError,
    RetrospectiveRepositoryError, RetrospectiveRun, RetrospectiveUnmatchedEvent,
    RetrospectiveUsageEvent,
};
pub use selection::{
    freeze_persisted_price_selection, parse_persisted_context_tiers,
    parse_persisted_legacy_context_rate, FrozenPriceSelection, FrozenPriceSelectionError,
    PriceSelectionReasonCode, PriceSelectionStatus, PricingSourceKind,
};

use catalog::{
    bound_json_error, bounded_error_text, parse_legacy_context_rate, parse_tiers,
    validate_json_nesting_depth,
};
#[cfg(test)]
use client::response_content_length_exceeds_limit;
use selection::frozen_selection_digest;

/// The only remote catalog endpoint used by Forge.
pub const MODELS_DEV_ENDPOINT: &str = "https://models.dev/api.json";

/// Maximum decoded response-body size accepted from models.dev.
///
/// The live catalog is currently several megabytes; this leaves bounded
/// headroom without permitting an unbounded response.
pub const MODELS_DEV_MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

/// Maximum duration for one models.dev request.
pub const MODELS_DEV_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// A catalog is stale after more than seven days without a successful 200 or
/// 304 check. The active last-known-good snapshot remains usable when stale.
pub const MODELS_DEV_STALE_AFTER: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Revision of the models.dev parser contract represented by this module.
pub const MODELS_DEV_PARSER_REVISION: &str = "models-dev-api-v1";

/// Number of fractional decimal digits in Forge's fixed-point nano-USD type.
pub const NANO_USD_SCALE: u32 = 9;

/// Number of nano-USD in one USD.
pub const NANO_USD_PER_USD: i128 = 1_000_000_000;

/// Token denominator used by rates expressed per one million tokens.
pub const TOKENS_PER_MILLION: i128 = 1_000_000;

/// Maximum models.dev rate accepted by the fixed-point source parser.
///
/// This is deliberately much wider than current public model prices while
/// still rejecting values that are almost certainly a malformed or hostile
/// catalog entry before they reach storage or estimation arithmetic.
pub const MODELS_DEV_MAX_USD_PER_MILLION: i64 = 1_000_000;

/// Fixed-point form of [`MODELS_DEV_MAX_USD_PER_MILLION`].
pub const MODELS_DEV_MAX_RATE_NANO_USD_PER_MILLION: i64 =
    MODELS_DEV_MAX_USD_PER_MILLION * 1_000_000_000;

/// Maximum manually entered USD-per-million rate.  Manual overrides use the
/// same plausibility ceiling as models.dev values and DB validation.
pub const MAX_MANUAL_USD_PER_MILLION: i64 = MODELS_DEV_MAX_USD_PER_MILLION;

/// Fixed-point form of [`MAX_MANUAL_USD_PER_MILLION`].
pub const MAX_MANUAL_RATE_NANO_USD_PER_MILLION: i64 = MAX_MANUAL_USD_PER_MILLION * 1_000_000_000;

/// Maximum UTF-8 byte length for a provider/model map key or exact `id`.
///
/// The live models.dev catalog currently uses identifiers much shorter than
/// this bound.  Keeping the bound explicit prevents an otherwise valid,
/// bounded response from retaining arbitrarily large map keys in every
/// normalized row and digest input.
pub const MODELS_DEV_MAX_IDENTIFIER_BYTES: usize = 256;

/// Maximum UTF-8 byte length for an optional source string such as a provider
/// or model display `name`, `release_date`, or `last_updated`.
///
/// Optional strings are retained as provenance, so they need their own bound
/// even though the complete response body is already capped at 16 MiB.
pub const MODELS_DEV_MAX_OPTIONAL_STRING_BYTES: usize = 1_024;

/// Maximum JSON container nesting depth accepted from models.dev.
///
/// The limit counts every `{` and `[` container, including ignored source
/// fields.  It is intentionally pinned in this module so parser behavior is
/// deterministic and remains safe if the upstream schema grows deeper.
pub const MODELS_DEV_MAX_JSON_NESTING_DEPTH: usize = 64;

const NANO_USD_PER_USD_I64: i64 = 1_000_000_000;
const POW10_I64: [i64; 10] = [
    1,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
];

/// Revision of the persisted rounding behavior.
pub const COST_ROUNDING_REVISION: &str = "cost-rounding-v1";

/// Revision of the four-bucket token-cost formula.
pub const COST_FORMULA_REVISION: &str = "token-cost-v1";

/// Non-secret billable identity used to version a provider entry or CLI
/// runtime.  Labels and credentials are intentionally absent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PricingSubjectIdentity {
    /// `provider_entry` or `cli_runtime` subject kind.
    pub subject_kind: String,
    /// Stable provider/runtime kind (for example `openai` or `gemini`).
    pub provider_kind: String,
    /// Credential method class, never the credential itself.
    pub credential_method: String,
    /// Canonical endpoint class/host with no embedded credentials.
    pub endpoint_class: String,
    /// Runtime fingerprint for discovered CLIs; absent for provider entries.
    pub runtime_fingerprint: Option<String>,
    /// Parser/config schema revision.
    pub schema_revision: String,
}

/// Computes the immutable non-secret pricing-subject revision digest.
pub fn pricing_subject_revision_digest(identity: &PricingSubjectIdentity) -> String {
    canonical_hash("pricing-subject-revision-v2", |encoded| {
        append_str(encoded, &identity.subject_kind);
        append_str(encoded, &identity.provider_kind);
        append_str(encoded, &identity.credential_method);
        append_str(encoded, &identity.endpoint_class);
        append_option_str(encoded, identity.runtime_fingerprint.as_deref());
        append_str(encoded, &identity.schema_revision);
    })
}

/// A non-negative amount represented as integer nano-USD.
///
/// The value is intentionally stored as `i64`: it is large enough for the
/// supported USD display range while keeping database and wire conversions
/// checked and unambiguous. Provider-reported amounts and estimates remain
/// separate at their owning ledger boundaries; this type is only the fixed
/// point scalar.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NanoUsd(i64);

impl NanoUsd {
    /// The zero USD amount.
    pub const ZERO: Self = Self(0);

    /// Constructs a nano-USD amount, rejecting negative storage values.
    pub const fn from_nano_usd(value: i64) -> Option<Self> {
        if value < 0 {
            None
        } else {
            Some(Self(value))
        }
    }

    /// Returns the checked integer representation.
    pub const fn as_nano_usd(self) -> i64 {
        self.0
    }

    /// Adds aggregate amounts without floating-point conversion.
    pub fn checked_add(self, other: Self) -> Option<Self> {
        Self::from_nano_usd(self.0.checked_add(other.0)?)
    }

    /// Serializes the amount as canonical non-negative USD decimal text.
    pub fn to_usd_decimal(self) -> String {
        format_nano_usd(self.0)
    }
}

impl fmt::Display for NanoUsd {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_usd_decimal())
    }
}

/// A non-negative USD rate expressed per one million tokens.
///
/// This is a distinct type from [`NanoUsd`] so a per-million rate cannot be
/// accidentally used as an event amount without an explicit calculation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NanoUsdPerMillion(i64);

impl NanoUsdPerMillion {
    /// The zero USD-per-million rate.
    pub const ZERO: Self = Self(0);

    /// Constructs a non-negative USD-per-million rate from nano-USD storage.
    pub const fn from_nano_usd(value: i64) -> Option<Self> {
        if value < 0 {
            None
        } else {
            Some(Self(value))
        }
    }

    /// Returns the checked integer representation.
    pub const fn as_nano_usd_per_million(self) -> i64 {
        self.0
    }

    /// Parses a strict user-entered USD-per-million decimal.
    pub fn parse_manual_usd_per_million(input: &str) -> Result<Self, ManualUsdParseError> {
        parse_manual_usd_per_million(input)
    }

    /// Parses a lexical models.dev JSON number and rounds it once to nanos.
    pub fn parse_models_dev_json_number(input: &str) -> Result<Self, ModelsDevPriceParseError> {
        parse_models_dev_json_number(input)
    }

    /// Serializes the rate as canonical non-negative USD-per-million text.
    pub fn to_usd_decimal_per_million(self) -> String {
        format_nano_usd(self.0)
    }
}

impl fmt::Display for NanoUsdPerMillion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_usd_decimal_per_million())
    }
}

impl FromStr for NanoUsdPerMillion {
    type Err = ManualUsdParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Self::parse_manual_usd_per_million(input)
    }
}

/// Errors from strict manual USD decimal parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ManualUsdParseError {
    #[error("USD rate is empty")]
    Empty,
    #[error("USD rate cannot contain whitespace")]
    WhitespaceNotAllowed,
    #[error("USD rate cannot contain a sign")]
    SignNotAllowed,
    #[error("USD rate cannot contain an exponent")]
    ExponentNotAllowed,
    #[error("USD rate is not a decimal number")]
    InvalidSyntax,
    #[error("USD rate has more than nine fractional digits")]
    TooManyFractionalDigits,
    #[error("USD rate does not fit checked i64 nano-USD storage")]
    Overflow,
    #[error("USD rate exceeds the configured plausibility bound")]
    ImplausiblyLarge,
}

/// Errors from lexical models.dev JSON-number parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ModelsDevPriceParseError {
    #[error("models.dev price is empty")]
    Empty,
    #[error("models.dev price is negative")]
    Negative,
    #[error("models.dev price is non-finite")]
    NonFinite,
    #[error("models.dev price is not a valid JSON number")]
    InvalidSyntax,
    #[error("models.dev price exponent is out of range")]
    ExponentOverflow,
    #[error("models.dev price does not fit checked i64 nano-USD storage")]
    Overflow,
    #[error("models.dev price exceeds the configured plausibility bound")]
    ImplausiblyLarge,
}

/// The four independently measurable token buckets used by the estimate
/// formula.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CostBucket {
    Input,
    Output,
    CacheRead,
    CacheWrite,
}

impl CostBucket {
    /// Stable order used by the four-bucket formula and fixtures.
    pub const ALL: [Self; 4] = [Self::Input, Self::Output, Self::CacheRead, Self::CacheWrite];

    const fn bit(self) -> u8 {
        match self {
            Self::Input => 1 << 0,
            Self::Output => 1 << 1,
            Self::CacheRead => 1 << 2,
            Self::CacheWrite => 1 << 3,
        }
    }
}

/// The token counters supplied for one usage event.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EventTokenCounts {
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_write: u64,
}

impl EventTokenCounts {
    /// Creates the four counters in formula order.
    pub const fn new(input: u64, output: u64, cache_read: u64, cache_write: u64) -> Self {
        Self {
            input,
            output,
            cache_read,
            cache_write,
        }
    }

    const fn as_array(self) -> [u64; 4] {
        [self.input, self.output, self.cache_read, self.cache_write]
    }
}

/// The optional per-million rates for the four independently measurable
/// token buckets. `None` means unknown; `Some(ZERO)` means known free.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EventBucketRates {
    pub input: Option<NanoUsdPerMillion>,
    pub output: Option<NanoUsdPerMillion>,
    pub cache_read: Option<NanoUsdPerMillion>,
    pub cache_write: Option<NanoUsdPerMillion>,
}

impl EventBucketRates {
    /// Creates the four optional rates in formula order.
    pub const fn new(
        input: Option<NanoUsdPerMillion>,
        output: Option<NanoUsdPerMillion>,
        cache_read: Option<NanoUsdPerMillion>,
        cache_write: Option<NanoUsdPerMillion>,
    ) -> Self {
        Self {
            input,
            output,
            cache_read,
            cache_write,
        }
    }

    const fn as_array(self) -> [Option<NanoUsdPerMillion>; 4] {
        [self.input, self.output, self.cache_read, self.cache_write]
    }
}

/// The set of buckets whose positive token counters lacked a rate.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MissingRateBuckets(u8);

impl MissingRateBuckets {
    /// Returns whether no positive counter lacked a rate.
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Returns whether `bucket` was missing a rate for a positive counter.
    pub const fn contains(self, bucket: CostBucket) -> bool {
        self.0 & bucket.bit() != 0
    }

    const fn insert(self, bucket: CostBucket) -> Self {
        Self(self.0 | bucket.bit())
    }
}

/// A calculated event estimate, or an explicit incomplete result when a
/// positive counter has no corresponding rate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventCostEstimate {
    Complete(NanoUsd),
    Incomplete { missing_rates: MissingRateBuckets },
}

impl EventCostEstimate {
    /// Returns the complete amount, if every positive bucket was priced.
    pub const fn amount(self) -> Option<NanoUsd> {
        match self {
            Self::Complete(amount) => Some(amount),
            Self::Incomplete { .. } => None,
        }
    }

    /// Returns whether this event has a complete estimate.
    pub const fn is_complete(self) -> bool {
        matches!(self, Self::Complete(_))
    }
}

/// Arithmetic failure while calculating an otherwise eligible event estimate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EventCostError {
    #[error("event cost arithmetic overflow")]
    Overflow,
}

/// Returns whether a catalog snapshot is stale at `now`.
///
/// A clock that moves backwards does not make a snapshot stale. The boundary
/// is strict: exactly seven days is still fresh, while any later instant is
/// stale.
pub fn models_dev_catalog_is_stale(last_successful_check: SystemTime, now: SystemTime) -> bool {
    now.duration_since(last_successful_check)
        .is_ok_and(|age| age > MODELS_DEV_STALE_AFTER)
}

/// Rounds a rational integer value half away from zero.
///
/// The denominator may be negative, but may not be zero. `None` indicates a
/// zero denominator or an `i128` overflow while taking the absolute value or
/// producing the rounded result.
pub fn round_half_away_from_zero(numerator: i128, denominator: i128) -> Option<i128> {
    if denominator == 0 {
        return None;
    }

    let numerator_negative = numerator.is_negative();
    let denominator_negative = denominator.is_negative();
    let numerator_magnitude = numerator.checked_abs()? as u128;
    let denominator_magnitude = denominator.checked_abs()? as u128;
    let quotient = numerator_magnitude / denominator_magnitude;
    let remainder = numerator_magnitude % denominator_magnitude;
    let round_up = remainder > denominator_magnitude / 2
        || (denominator_magnitude.is_multiple_of(2) && remainder == denominator_magnitude / 2);
    let rounded_magnitude = quotient.checked_add(u128::from(round_up))?;
    let negative = numerator_negative != denominator_negative;

    if negative {
        if rounded_magnitude == (i128::MAX as u128) + 1 {
            Some(i128::MIN)
        } else {
            Some(-i128::try_from(rounded_magnitude).ok()?)
        }
    } else {
        i128::try_from(rounded_magnitude).ok()
    }
}

/// Parses a strict, non-negative, non-exponent USD-per-million decimal into
/// fixed nano-USD storage.
pub fn parse_manual_usd_per_million(input: &str) -> Result<NanoUsdPerMillion, ManualUsdParseError> {
    if input.is_empty() {
        return Err(ManualUsdParseError::Empty);
    }
    if input.chars().any(char::is_whitespace) {
        return Err(ManualUsdParseError::WhitespaceNotAllowed);
    }
    if input
        .as_bytes()
        .first()
        .is_some_and(|byte| matches!(byte, b'+' | b'-'))
    {
        return Err(ManualUsdParseError::SignNotAllowed);
    }
    if input.bytes().any(|byte| matches!(byte, b'e' | b'E')) {
        return Err(ManualUsdParseError::ExponentNotAllowed);
    }

    let (whole, fraction) = match input.split_once('.') {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (input, None),
    };
    if whole.is_empty()
        || whole.bytes().any(|byte| !byte.is_ascii_digit())
        || fraction.is_some_and(|value| {
            value.is_empty() || value.bytes().any(|byte| !byte.is_ascii_digit())
        })
    {
        return Err(ManualUsdParseError::InvalidSyntax);
    }

    let fraction = fraction.unwrap_or_default();
    if fraction.len() > NANO_USD_SCALE as usize {
        return Err(ManualUsdParseError::TooManyFractionalDigits);
    }

    let whole_nanos = parse_i64_digits(whole.as_bytes())
        .ok_or(ManualUsdParseError::Overflow)?
        .checked_mul(NANO_USD_PER_USD_I64)
        .ok_or(ManualUsdParseError::Overflow)?;
    let fraction_value = parse_i64_digits(fraction.as_bytes())
        .ok_or(ManualUsdParseError::Overflow)?
        .checked_mul(POW10_I64[NANO_USD_SCALE as usize - fraction.len()])
        .ok_or(ManualUsdParseError::Overflow)?;
    let nanos = whole_nanos
        .checked_add(fraction_value)
        .ok_or(ManualUsdParseError::Overflow)?;

    if nanos > MAX_MANUAL_RATE_NANO_USD_PER_MILLION {
        return Err(ManualUsdParseError::ImplausiblyLarge);
    }

    Ok(NanoUsdPerMillion(nanos))
}

/// Parses a models.dev JSON number from its original lexical representation.
///
/// Unlike manual input, the source feed may contain exponent notation and
/// long decimal tails caused by JavaScript floating-point serialization. The
/// value is normalized exactly from decimal digits and rounded once to the
/// fixed nano-USD scale.
pub fn parse_models_dev_json_number(
    input: &str,
) -> Result<NanoUsdPerMillion, ModelsDevPriceParseError> {
    if input.is_empty() {
        return Err(ModelsDevPriceParseError::Empty);
    }
    if input.chars().any(char::is_whitespace) {
        return Err(ModelsDevPriceParseError::InvalidSyntax);
    }
    if is_non_finite_literal(input) {
        return Err(ModelsDevPriceParseError::NonFinite);
    }

    let (digits, decimal_position) = parse_models_dev_decimal_parts(input)?;
    let nanos = normalize_models_dev_digits(&digits, decimal_position)?;
    Ok(NanoUsdPerMillion(nanos))
}

/// Calculates one event's four-bucket estimate using checked integer
/// arithmetic and exactly one division/rounding step after the bucket sum.
///
/// A missing rate is incomplete only when its corresponding counter is
/// positive. A present zero rate is known free, and a zero counter does not
/// require a rate. Provider-reported costs are intentionally not accepted by
/// this helper: callers must preserve reported-vs-estimated authority in the
/// owning usage ledger.
pub fn calculate_event_cost(
    counters: EventTokenCounts,
    rates: EventBucketRates,
) -> Result<EventCostEstimate, EventCostError> {
    calculate_four_bucket_cost(counters.as_array(), rates.as_array())
}

/// Array-shaped form of [`calculate_event_cost`] for callers that already
/// have the stable `[input, output, cache_read, cache_write]` order.
pub fn calculate_four_bucket_cost(
    counters: [u64; 4],
    rates: [Option<NanoUsdPerMillion>; 4],
) -> Result<EventCostEstimate, EventCostError> {
    let mut missing_rates = MissingRateBuckets::default();
    for (index, (&counter, rate)) in counters.iter().zip(rates.iter()).enumerate() {
        if counter > 0 && rate.is_none() {
            missing_rates = missing_rates.insert(CostBucket::ALL[index]);
        }
    }
    if !missing_rates.is_empty() {
        return Ok(EventCostEstimate::Incomplete { missing_rates });
    }

    let mut numerator = 0_i128;
    for (&counter, rate) in counters.iter().zip(rates.iter()) {
        let Some(rate) = rate else {
            continue;
        };
        let product = i128::from(counter)
            .checked_mul(i128::from(rate.as_nano_usd_per_million()))
            .ok_or(EventCostError::Overflow)?;
        numerator = numerator
            .checked_add(product)
            .ok_or(EventCostError::Overflow)?;
    }

    let rounded =
        round_half_away_from_zero(numerator, TOKENS_PER_MILLION).ok_or(EventCostError::Overflow)?;
    let amount =
        NanoUsd::from_nano_usd(i64::try_from(rounded).map_err(|_| EventCostError::Overflow)?)
            .ok_or(EventCostError::Overflow)?;
    Ok(EventCostEstimate::Complete(amount))
}

fn parse_i64_digits(digits: &[u8]) -> Option<i64> {
    let mut value = 0_i64;
    for &digit in digits {
        if !digit.is_ascii_digit() {
            return None;
        }
        value = value
            .checked_mul(10)?
            .checked_add(i64::from(digit - b'0'))?;
    }
    Some(value)
}

fn format_nano_usd(nanos: i64) -> String {
    debug_assert!(nanos >= 0);
    let whole = nanos / NANO_USD_PER_USD_I64;
    let fraction = nanos % NANO_USD_PER_USD_I64;
    if fraction == 0 {
        return whole.to_string();
    }

    let mut fraction_text = format!("{fraction:09}");
    while fraction_text.ends_with('0') {
        fraction_text.pop();
    }
    format!("{whole}.{fraction_text}")
}

fn is_non_finite_literal(input: &str) -> bool {
    input.eq_ignore_ascii_case("nan")
        || input.eq_ignore_ascii_case("infinity")
        || input.eq_ignore_ascii_case("inf")
        || input.eq_ignore_ascii_case("+infinity")
        || input.eq_ignore_ascii_case("-infinity")
        || input.eq_ignore_ascii_case("+inf")
        || input.eq_ignore_ascii_case("-inf")
}

fn parse_models_dev_decimal_parts(input: &str) -> Result<(Vec<u8>, i64), ModelsDevPriceParseError> {
    let bytes = input.as_bytes();
    let mut cursor = 0_usize;
    if bytes.first() == Some(&b'-') {
        return Err(ModelsDevPriceParseError::Negative);
    }
    if bytes.first() == Some(&b'+') {
        return Err(ModelsDevPriceParseError::InvalidSyntax);
    }

    let integer_start = cursor;
    match bytes.get(cursor) {
        Some(b'0') => {
            cursor += 1;
            if bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
                return Err(ModelsDevPriceParseError::InvalidSyntax);
            }
        }
        Some(byte @ b'1'..=b'9') => {
            let _ = byte;
            cursor += 1;
            while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
                cursor += 1;
            }
        }
        _ => return Err(ModelsDevPriceParseError::InvalidSyntax),
    }
    let integer_end = cursor;

    let fraction_start = if bytes.get(cursor) == Some(&b'.') {
        cursor += 1;
        let start = cursor;
        if !bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            return Err(ModelsDevPriceParseError::InvalidSyntax);
        }
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            cursor += 1;
        }
        Some(start)
    } else {
        None
    };
    let fraction_end = fraction_start.map_or(integer_end, |_| cursor);

    let exponent = if bytes
        .get(cursor)
        .is_some_and(|byte| matches!(byte, b'e' | b'E'))
    {
        cursor += 1;
        let negative = match bytes.get(cursor) {
            Some(b'-') => {
                cursor += 1;
                true
            }
            Some(b'+') => {
                cursor += 1;
                false
            }
            _ => false,
        };
        let mut magnitude = 0_u64;
        let exponent_start = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            magnitude = magnitude
                .checked_mul(10)
                .and_then(|value| value.checked_add(u64::from(bytes[cursor] - b'0')))
                .ok_or(ModelsDevPriceParseError::ExponentOverflow)?;
            cursor += 1;
        }
        if cursor == exponent_start {
            return Err(ModelsDevPriceParseError::InvalidSyntax);
        }
        if negative {
            if magnitude > (1_u64 << 63) {
                return Err(ModelsDevPriceParseError::ExponentOverflow);
            }
            if magnitude == (1_u64 << 63) {
                i64::MIN
            } else {
                -i64::try_from(magnitude).map_err(|_| ModelsDevPriceParseError::ExponentOverflow)?
            }
        } else {
            i64::try_from(magnitude).map_err(|_| ModelsDevPriceParseError::ExponentOverflow)?
        }
    } else {
        0
    };

    if cursor != bytes.len() {
        return Err(ModelsDevPriceParseError::InvalidSyntax);
    }

    let mut digits = Vec::with_capacity(bytes.len());
    digits.extend_from_slice(&bytes[integer_start..integer_end]);
    if let Some(start) = fraction_start {
        digits.extend_from_slice(&bytes[start..fraction_end]);
    }

    let leading_zeroes = digits.iter().take_while(|digit| **digit == b'0').count();
    if leading_zeroes == digits.len() {
        return Ok((Vec::new(), 0));
    }
    let mut significant = digits[leading_zeroes..].to_vec();
    while significant.last() == Some(&b'0') {
        significant.pop();
    }

    let integer_len = i64::try_from(integer_end - integer_start)
        .map_err(|_| ModelsDevPriceParseError::Overflow)?;
    let leading_zeroes =
        i64::try_from(leading_zeroes).map_err(|_| ModelsDevPriceParseError::Overflow)?;
    let decimal_position = integer_len
        .checked_add(exponent)
        .ok_or(ModelsDevPriceParseError::ExponentOverflow)?
        .checked_sub(leading_zeroes)
        .unwrap_or(i64::MIN);

    Ok((significant, decimal_position))
}

fn normalize_models_dev_digits(
    digits: &[u8],
    decimal_position: i64,
) -> Result<i64, ModelsDevPriceParseError> {
    if digits.is_empty() {
        return Ok(0);
    }

    let digit_count =
        i64::try_from(digits.len()).map_err(|_| ModelsDevPriceParseError::Overflow)?;
    let nano_decimal_position = match decimal_position.checked_add(i64::from(NANO_USD_SCALE)) {
        Some(position) => position,
        None => return Err(ModelsDevPriceParseError::ImplausiblyLarge),
    };
    let Some(shift) = nano_decimal_position.checked_sub(digit_count) else {
        return Ok(0);
    };

    let nanos = if shift >= 0 {
        let total_digits = digit_count
            .checked_add(shift)
            .ok_or(ModelsDevPriceParseError::ImplausiblyLarge)?;
        if total_digits > 19 {
            return Err(ModelsDevPriceParseError::ImplausiblyLarge);
        }
        let mut value = parse_i64_digits(digits).ok_or(ModelsDevPriceParseError::Overflow)?;
        let shift =
            usize::try_from(shift).map_err(|_| ModelsDevPriceParseError::ImplausiblyLarge)?;
        for _ in 0..shift {
            value = value
                .checked_mul(10)
                .ok_or(ModelsDevPriceParseError::Overflow)?;
        }
        value
    } else {
        let drop = match shift
            .checked_neg()
            .and_then(|value| usize::try_from(value).ok())
        {
            Some(value) => value,
            None => return Ok(0),
        };
        if drop > digits.len() {
            return Ok(0);
        }
        let keep = digits.len() - drop;
        let mut value =
            parse_i64_digits(&digits[..keep]).ok_or(ModelsDevPriceParseError::Overflow)?;
        if digits[keep] >= b'5' {
            value = value
                .checked_add(1)
                .ok_or(ModelsDevPriceParseError::Overflow)?;
        }
        value
    };

    if nanos > MODELS_DEV_MAX_RATE_NANO_USD_PER_MILLION {
        return Err(ModelsDevPriceParseError::ImplausiblyLarge);
    }
    Ok(nanos)
}

/// SHA-256 helper used for payloads, revisions, and deterministic idempotency.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Hashes a domain-separated, length-prefixed canonical value.
///
/// Every variable-length component is encoded with its byte length, and
/// optional values carry an explicit presence tag.  This keeps identity
/// digests injective even when source values contain delimiters, newlines, or
/// empty strings.
fn canonical_hash<F>(domain: &str, encode: F) -> String
where
    F: FnOnce(&mut Vec<u8>),
{
    let mut bytes = Vec::new();
    append_str(&mut bytes, domain);
    encode(&mut bytes);
    sha256_hex(&bytes)
}

fn append_len_prefixed(encoded: &mut Vec<u8>, value: &[u8]) {
    let length = u64::try_from(value.len()).expect("in-memory value length fits u64");
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(value);
}

fn append_str(encoded: &mut Vec<u8>, value: &str) {
    append_len_prefixed(encoded, value.as_bytes());
}

fn append_option_str(encoded: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            encoded.push(1);
            append_str(encoded, value);
        }
        None => encoded.push(0),
    }
}

fn append_u32(encoded: &mut Vec<u8>, value: u32) {
    encoded.extend_from_slice(&value.to_be_bytes());
}

fn append_u64(encoded: &mut Vec<u8>, value: u64) {
    encoded.extend_from_slice(&value.to_be_bytes());
}

fn append_i64(encoded: &mut Vec<u8>, value: i64) {
    encoded.extend_from_slice(&value.to_be_bytes());
}

fn append_option_rate(encoded: &mut Vec<u8>, value: Option<NanoUsdPerMillion>) {
    match value {
        Some(value) => {
            encoded.push(1);
            append_i64(encoded, value.as_nano_usd_per_million());
        }
        None => encoded.push(0),
    }
}

fn append_rates(encoded: &mut Vec<u8>, rates: EventBucketRates) {
    for rate in rates.as_array() {
        append_option_rate(encoded, rate);
    }
}

fn append_option_rates(encoded: &mut Vec<u8>, rates: Option<EventBucketRates>) {
    match rates {
        Some(rates) => {
            encoded.push(1);
            append_rates(encoded, rates);
        }
        None => encoded.push(0),
    }
}

fn append_tiers(encoded: &mut Vec<u8>, tiers: &[ContextTier]) {
    append_u64(
        encoded,
        u64::try_from(tiers.len()).expect("in-memory tier count fits u64"),
    );
    for tier in tiers {
        append_u64(encoded, tier.threshold_tokens);
        append_rates(encoded, tier.rates);
        append_str(encoded, &tier.raw_json);
    }
}

fn append_option_legacy_context(encoded: &mut Vec<u8>, legacy: Option<&LegacyContextRate>) {
    match legacy {
        Some(legacy) => {
            encoded.push(1);
            append_rates(encoded, legacy.rates);
            append_str(encoded, &legacy.raw_json);
        }
        None => encoded.push(0),
    }
}

fn append_option_enum_str(encoded: &mut Vec<u8>, value: Option<&'static str>) {
    append_option_str(encoded, value);
}

fn append_system_time(encoded: &mut Vec<u8>, time: SystemTime) {
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => {
            encoded.push(0);
            append_u64(encoded, duration.as_secs());
            append_u32(encoded, duration.subsec_nanos());
        }
        Err(error) => {
            let duration = error.duration();
            encoded.push(1);
            append_u64(encoded, duration.as_secs());
            append_u32(encoded, duration.subsec_nanos());
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod catalog_domain_tests;
