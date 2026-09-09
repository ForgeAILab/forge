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

/// Stable name of the rounding algorithm persisted with estimates.
pub const COST_ROUNDING_MODE: &str = "half_away_from_zero";

/// Revision of the persisted rounding behavior.
pub const COST_ROUNDING_REVISION: &str = "cost-rounding-v1";

/// Revision of the four-bucket token-cost formula.
pub const COST_FORMULA_REVISION: &str = "token-cost-v1";

/// Credential class used by a reviewed built-in provider alias.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuiltInProviderAuthClass {
    /// A direct API-key-backed public provider endpoint.
    ApiKey,
    /// The direct Gemini endpoint may use an API key or reviewed OAuth flow.
    ApiKeyOrOauth,
}

/// An exact, reviewed mapping from a Forge provider subject to models.dev.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuiltInProviderAlias {
    /// Stable Forge subject kind, not a mutable display label.
    pub forge_subject: &'static str,
    /// Canonical endpoint class required for this mapping. Scheme, host, and
    /// provider API path are all significant; custom paths do not alias.
    pub endpoint_host: &'static str,
    /// Credential class required for this mapping.
    pub auth_class: BuiltInProviderAuthClass,
    /// Top-level provider ID in models.dev `/api.json`.
    pub models_dev_provider_id: &'static str,
}

/// Direct OpenAI Platform API → models.dev `openai`.
pub const BUILT_IN_ALIAS_OPENAI: BuiltInProviderAlias = BuiltInProviderAlias {
    forge_subject: "direct_openai_platform_api",
    endpoint_host: "https://api.openai.com/v1",
    auth_class: BuiltInProviderAuthClass::ApiKey,
    models_dev_provider_id: "openai",
};

/// Direct xAI API → models.dev `xai`.
pub const BUILT_IN_ALIAS_XAI: BuiltInProviderAlias = BuiltInProviderAlias {
    forge_subject: "direct_xai_api",
    endpoint_host: "https://api.x.ai/v1",
    auth_class: BuiltInProviderAuthClass::ApiKey,
    models_dev_provider_id: "xai",
};

/// Direct Gemini API → models.dev `google`.
pub const BUILT_IN_ALIAS_GEMINI: BuiltInProviderAlias = BuiltInProviderAlias {
    forge_subject: "direct_gemini_api",
    endpoint_host: "https://generativelanguage.googleapis.com/v1beta",
    auth_class: BuiltInProviderAuthClass::ApiKeyOrOauth,
    models_dev_provider_id: "google",
};

/// Direct OpenRouter API → models.dev `openrouter`.
pub const BUILT_IN_ALIAS_OPENROUTER: BuiltInProviderAlias = BuiltInProviderAlias {
    forge_subject: "direct_openrouter_api",
    endpoint_host: "https://openrouter.ai/api/v1",
    auth_class: BuiltInProviderAuthClass::ApiKey,
    models_dev_provider_id: "openrouter",
};

/// The reviewed aliases, in stable order for deterministic fixtures and UI.
pub const BUILT_IN_PROVIDER_ALIASES: [BuiltInProviderAlias; 4] = [
    BUILT_IN_ALIAS_OPENAI,
    BUILT_IN_ALIAS_XAI,
    BUILT_IN_ALIAS_GEMINI,
    BUILT_IN_ALIAS_OPENROUTER,
];

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

/// Returns a reviewed built-in models.dev alias only for exact endpoint and
/// credential classes.  Custom URLs, OpenAI-compatible entries, subscriptions
/// and CLI runtimes intentionally return `None`.
pub fn reviewed_provider_alias(identity: &PricingSubjectIdentity) -> Option<BuiltInProviderAlias> {
    if identity.subject_kind != "provider_entry" {
        return None;
    }
    let expected = match identity.provider_kind.as_str() {
        "openai" => BUILT_IN_ALIAS_OPENAI,
        "xai" => BUILT_IN_ALIAS_XAI,
        "gemini" => BUILT_IN_ALIAS_GEMINI,
        "openrouter" => BUILT_IN_ALIAS_OPENROUTER,
        _ => return None,
    };
    if identity.endpoint_class != expected.endpoint_host {
        return None;
    }
    let auth_matches = match expected.auth_class {
        BuiltInProviderAuthClass::ApiKey => identity.credential_method == "api_key",
        BuiltInProviderAuthClass::ApiKeyOrOauth => {
            matches!(identity.credential_method.as_str(), "api_key" | "oauth")
        }
    };
    auth_matches.then_some(expected)
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

/// Returns whether a decoded models.dev response body is within the bound.
pub const fn models_dev_response_body_is_within_limit(body_len: usize) -> bool {
    body_len <= MODELS_DEV_MAX_RESPONSE_BYTES
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

/// Serializes a nano-USD amount as canonical USD decimal text.
pub fn serialize_usd_decimal(amount: NanoUsd) -> String {
    amount.to_usd_decimal()
}

/// Serializes a per-million rate as canonical USD decimal text.
pub fn serialize_usd_per_million_decimal(rate: NanoUsdPerMillion) -> String {
    rate.to_usd_decimal_per_million()
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

/// Adds two aggregate amounts with checked i64 storage and no floating-point
/// conversion.
pub fn checked_add_aggregate_nano_usd(left: NanoUsd, right: NanoUsd) -> Option<NanoUsd> {
    left.checked_add(right)
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

#[cfg(test)]
mod tests {
    use super::*;

    // Minimal provider/model-shaped fixture used only to keep the response
    // bound test tied to the source payload shape rather than an arbitrary
    // byte string. Full schema parsing belongs to the catalog-ingestion task.
    const MODELS_DEV_PROVIDER_FIXTURE: &[u8] = br#"{
      "openai": {
        "id": "openai",
        "env": ["OPENAI_API_KEY"],
        "npm": "@ai-sdk/openai",
        "name": "OpenAI",
        "doc": "https://platform.openai.com/docs/models",
        "models": {
          "gpt-5": {
            "id": "gpt-5",
            "name": "GPT-5",
            "description": "Fixture model",
            "attachment": true,
            "reasoning": true,
            "reasoning_options": [{"type":"effort","values":["low"]}],
            "tool_call": true,
            "release_date": "2025-08-07",
            "last_updated": "2025-08-07",
            "modalities": {"input":["text"],"output":["text"]},
            "open_weights": false,
            "limit": {"context": 400000, "output": 128000},
            "cost": {"input": 1.25, "output": 10.0}
          }
        }
      }
    }"#;

    #[test]
    fn constants_pin_catalog_fetch_contract() {
        assert_eq!(MODELS_DEV_ENDPOINT, "https://models.dev/api.json");
        assert_eq!(MODELS_DEV_MAX_RESPONSE_BYTES, 16 * 1024 * 1024);
        assert_eq!(MAX_MANUAL_USD_PER_MILLION, 1_000_000);
        assert_eq!(MAX_MANUAL_RATE_NANO_USD_PER_MILLION, 1_000_000_000_000_000);
        assert_eq!(MODELS_DEV_MAX_IDENTIFIER_BYTES, 256);
        assert_eq!(MODELS_DEV_MAX_OPTIONAL_STRING_BYTES, 1_024);
        assert_eq!(MODELS_DEV_MAX_JSON_NESTING_DEPTH, 64);
        assert_eq!(MODELS_DEV_REQUEST_TIMEOUT, Duration::from_secs(30));
        assert!(MODELS_DEV_MAX_RESPONSE_BYTES > MODELS_DEV_PROVIDER_FIXTURE.len());
        assert!(models_dev_response_body_is_within_limit(
            MODELS_DEV_PROVIDER_FIXTURE.len()
        ));
        assert!(!models_dev_response_body_is_within_limit(
            MODELS_DEV_MAX_RESPONSE_BYTES + 1
        ));
    }

    #[test]
    fn stale_policy_uses_successful_check_and_strict_seven_day_boundary() {
        let checked_at = SystemTime::UNIX_EPOCH;
        assert!(!models_dev_catalog_is_stale(
            checked_at,
            checked_at + MODELS_DEV_STALE_AFTER
        ));
        assert!(models_dev_catalog_is_stale(
            checked_at,
            checked_at + MODELS_DEV_STALE_AFTER + Duration::from_secs(1)
        ));
        assert!(!models_dev_catalog_is_stale(
            checked_at + Duration::from_secs(1),
            checked_at
        ));
    }

    #[test]
    fn fixed_point_and_rounding_revisions_are_stable() {
        assert_eq!(NANO_USD_SCALE, 9);
        assert_eq!(NANO_USD_PER_USD, 1_000_000_000);
        assert_eq!(TOKENS_PER_MILLION, 1_000_000);
        assert_eq!(COST_ROUNDING_MODE, "half_away_from_zero");
        assert_eq!(COST_ROUNDING_REVISION, "cost-rounding-v1");
        assert_eq!(COST_FORMULA_REVISION, "token-cost-v1");

        assert_eq!(round_half_away_from_zero(4, 2), Some(2));
        assert_eq!(round_half_away_from_zero(5, 2), Some(3));
        assert_eq!(round_half_away_from_zero(-5, 2), Some(-3));
        assert_eq!(round_half_away_from_zero(5, -2), Some(-3));
        assert_eq!(round_half_away_from_zero(1, 0), None);
    }

    #[test]
    fn reviewed_provider_aliases_are_exact_and_distinct() {
        assert_eq!(BUILT_IN_PROVIDER_ALIASES.len(), 4);
        assert_eq!(BUILT_IN_ALIAS_OPENAI.models_dev_provider_id, "openai");
        assert_eq!(
            BUILT_IN_ALIAS_OPENAI.endpoint_host,
            "https://api.openai.com/v1"
        );
        assert_eq!(BUILT_IN_ALIAS_XAI.models_dev_provider_id, "xai");
        assert_eq!(BUILT_IN_ALIAS_XAI.endpoint_host, "https://api.x.ai/v1");
        assert_eq!(BUILT_IN_ALIAS_GEMINI.models_dev_provider_id, "google");
        assert_eq!(
            BUILT_IN_ALIAS_GEMINI.endpoint_host,
            "https://generativelanguage.googleapis.com/v1beta"
        );
        assert_eq!(
            BUILT_IN_ALIAS_OPENROUTER.models_dev_provider_id,
            "openrouter"
        );
        assert_eq!(
            BUILT_IN_ALIAS_OPENROUTER.endpoint_host,
            "https://openrouter.ai/api/v1"
        );

        let provider_ids: std::collections::BTreeSet<_> = BUILT_IN_PROVIDER_ALIASES
            .iter()
            .map(|alias| alias.models_dev_provider_id)
            .collect();
        assert_eq!(provider_ids.len(), BUILT_IN_PROVIDER_ALIASES.len());

        let openai = PricingSubjectIdentity {
            subject_kind: "provider_entry".to_owned(),
            provider_kind: "openai".to_owned(),
            credential_method: "api_key".to_owned(),
            endpoint_class: "https://api.openai.com/v1".to_owned(),
            runtime_fingerprint: None,
            schema_revision: "provider-v1".to_owned(),
        };
        assert_eq!(
            reviewed_provider_alias(&openai).map(|alias| alias.models_dev_provider_id),
            Some("openai")
        );

        let mut custom_endpoint = openai.clone();
        custom_endpoint.endpoint_class = "https://api.openai.com/v2".to_owned();
        assert!(reviewed_provider_alias(&custom_endpoint).is_none());

        let mut subscription = openai.clone();
        subscription.subject_kind = "cli_runtime".to_owned();
        subscription.credential_method = "oauth_subscription".to_owned();
        assert!(reviewed_provider_alias(&subscription).is_none());

        let mut cli_runtime = openai;
        cli_runtime.subject_kind = "cli_runtime".to_owned();
        assert!(reviewed_provider_alias(&cli_runtime).is_none());
    }

    #[test]
    fn pricing_subject_revision_digest_is_injective_for_delimiter_values() {
        let left = PricingSubjectIdentity {
            subject_kind: "provider_entry|nested".to_owned(),
            provider_kind: "openai".to_owned(),
            credential_method: "api_key".to_owned(),
            endpoint_class: "api.openai.com".to_owned(),
            runtime_fingerprint: None,
            schema_revision: "provider-v1".to_owned(),
        };
        let mut right = left.clone();
        right.subject_kind = "provider_entry".to_owned();
        right.provider_kind = "nested|openai".to_owned();
        assert_ne!(
            pricing_subject_revision_digest(&left),
            pricing_subject_revision_digest(&right)
        );

        let mut option_left = left.clone();
        let mut option_right = left;
        option_left.runtime_fingerprint = None;
        option_right.runtime_fingerprint = Some(String::new());
        assert_ne!(
            pricing_subject_revision_digest(&option_left),
            pricing_subject_revision_digest(&option_right)
        );
    }

    #[test]
    fn binding_revision_digest_is_injective_for_delimiter_values() {
        let left = DesiredPricingBinding::manual("model|source", EventBucketRates::default());
        let right = DesiredPricingBinding::manual("model", EventBucketRates::default());
        assert_ne!(
            binding_revision_digest(&left),
            binding_revision_digest(&right)
        );
    }

    fn rate(nanos: i64) -> NanoUsdPerMillion {
        NanoUsdPerMillion::from_nano_usd(nanos).expect("test rate must be non-negative")
    }

    fn amount(nanos: i64) -> NanoUsd {
        NanoUsd::from_nano_usd(nanos).expect("test amount must be non-negative")
    }

    #[test]
    fn manual_rates_are_strict_and_checked() {
        let cases = [
            ("0", 0),
            ("00.000000000", 0),
            ("1", 1_000_000_000),
            ("1.230000000", 1_230_000_000),
            ("0.000000001", 1),
            ("1000000", MODELS_DEV_MAX_RATE_NANO_USD_PER_MILLION),
        ];
        for (input, expected) in cases {
            let parsed = parse_manual_usd_per_million(input).expect(input);
            assert_eq!(parsed.as_nano_usd_per_million(), expected, "{input}");
        }

        assert_eq!(
            NanoUsdPerMillion::from_str("1.25").expect("FromStr parses manual rates"),
            rate(1_250_000_000)
        );
        assert_eq!(rate(1_230_000_000).to_usd_decimal_per_million(), "1.23");

        let rejected = [
            ("", ManualUsdParseError::Empty),
            (" ", ManualUsdParseError::WhitespaceNotAllowed),
            ("1 2", ManualUsdParseError::WhitespaceNotAllowed),
            ("+1", ManualUsdParseError::SignNotAllowed),
            ("-0", ManualUsdParseError::SignNotAllowed),
            ("1e3", ManualUsdParseError::ExponentNotAllowed),
            ("1E-3", ManualUsdParseError::ExponentNotAllowed),
            ("1.1234567890", ManualUsdParseError::TooManyFractionalDigits),
            ("1.", ManualUsdParseError::InvalidSyntax),
            (".1", ManualUsdParseError::InvalidSyntax),
            ("1..0", ManualUsdParseError::InvalidSyntax),
            ("abc", ManualUsdParseError::InvalidSyntax),
            ("1000000.000000001", ManualUsdParseError::ImplausiblyLarge),
            ("1000001", ManualUsdParseError::ImplausiblyLarge),
            ("9223372036.854775808", ManualUsdParseError::Overflow),
            ("9223372037", ManualUsdParseError::Overflow),
        ];
        for (input, expected) in rejected {
            assert_eq!(
                parse_manual_usd_per_million(input),
                Err(expected),
                "{input}"
            );
        }
    }

    #[test]
    fn models_dev_numbers_parse_lexically_and_round_once() {
        let cases = [
            ("0", 0),
            ("0.000000000", 0),
            ("1e-9", 1),
            ("5e-10", 1),
            ("4.9e-10", 0),
            ("1e+3", 1_000_000_000_000),
            ("1.2345678914", 1_234_567_891),
            ("1.2345678915", 1_234_567_892),
            // This is the kind of floating-point artifact emitted by the
            // live catalog; lexical parsing rounds it to the intended nanos.
            ("0.049999999999999996", 50_000_000),
            ("1e-100", 0),
            ("1000000", MODELS_DEV_MAX_RATE_NANO_USD_PER_MILLION),
        ];
        for (input, expected) in cases {
            let parsed = parse_models_dev_json_number(input).expect(input);
            assert_eq!(parsed.as_nano_usd_per_million(), expected, "{input}");
        }

        let rejected = [
            ("-0", ModelsDevPriceParseError::Negative),
            ("-1", ModelsDevPriceParseError::Negative),
            ("NaN", ModelsDevPriceParseError::NonFinite),
            ("Infinity", ModelsDevPriceParseError::NonFinite),
            ("-Infinity", ModelsDevPriceParseError::NonFinite),
            ("inf", ModelsDevPriceParseError::NonFinite),
            ("01", ModelsDevPriceParseError::InvalidSyntax),
            ("1.", ModelsDevPriceParseError::InvalidSyntax),
            (".1", ModelsDevPriceParseError::InvalidSyntax),
            ("1e", ModelsDevPriceParseError::InvalidSyntax),
            ("1e+", ModelsDevPriceParseError::InvalidSyntax),
            ("1e-", ModelsDevPriceParseError::InvalidSyntax),
            ("1 2", ModelsDevPriceParseError::InvalidSyntax),
            (
                "1000000.000000001",
                ModelsDevPriceParseError::ImplausiblyLarge,
            ),
            ("1e100", ModelsDevPriceParseError::ImplausiblyLarge),
            (
                "1e999999999999999999999999",
                ModelsDevPriceParseError::ExponentOverflow,
            ),
            ("", ModelsDevPriceParseError::Empty),
        ];
        for (input, expected) in rejected {
            assert_eq!(
                parse_models_dev_json_number(input),
                Err(expected),
                "{input}"
            );
        }
    }

    #[test]
    fn canonical_serialization_has_no_exponent_or_unneeded_zeroes() {
        let cases = [
            (0, "0"),
            (1, "0.000000001"),
            (10, "0.00000001"),
            (100_000_000, "0.1"),
            (1_230_000_000, "1.23"),
            (i64::MAX, "9223372036.854775807"),
        ];
        for (nanos, expected) in cases {
            let value = amount(nanos);
            assert_eq!(value.to_usd_decimal(), expected);
            assert_eq!(serialize_usd_decimal(value), expected);
            assert!(!value.to_string().contains('e'));
            assert!(value
                .to_usd_decimal()
                .split('.')
                .nth(1)
                .is_none_or(|fraction| fraction.len() <= NANO_USD_SCALE as usize));
        }

        let rate = rate(1_250_000_000);
        assert_eq!(rate.to_string(), "1.25");
        assert_eq!(serialize_usd_per_million_decimal(rate), "1.25");
    }

    #[test]
    fn event_cost_prices_all_buckets_and_rounds_once() {
        let counters = EventTokenCounts::new(1_000_000, 2_000_000, 3_000_000, 4_000_000);
        let rates = EventBucketRates::new(
            Some(rate(1_000_000_000)),
            Some(rate(2_000_000_000)),
            Some(rate(3_000_000_000)),
            Some(rate(4_000_000_000)),
        );
        let result = calculate_event_cost(counters, rates).expect("arithmetic should fit");
        assert_eq!(result, EventCostEstimate::Complete(amount(30_000_000_000)));

        // Each bucket is half a nano-USD on its own. The contract sums first
        // and rounds once, so the total is one nano-USD rather than two.
        let result = calculate_four_bucket_cost(
            [1, 1, 0, 0],
            [Some(rate(500_000)), Some(rate(500_000)), None, None],
        )
        .expect("arithmetic should fit");
        assert_eq!(result, EventCostEstimate::Complete(amount(1)));
    }

    #[test]
    fn event_cost_preserves_unknown_vs_zero_rate_semantics() {
        let result = calculate_event_cost(
            EventTokenCounts::new(1, 0, 0, 0),
            EventBucketRates::new(None, None, None, None),
        )
        .expect("missing rate is an explicit result, not arithmetic failure");
        let EventCostEstimate::Incomplete { missing_rates } = result else {
            panic!("positive counter without a rate must be incomplete");
        };
        assert!(missing_rates.contains(CostBucket::Input));
        assert!(!missing_rates.contains(CostBucket::Output));

        let result = calculate_event_cost(
            EventTokenCounts::new(0, 0, 0, 0),
            EventBucketRates::default(),
        )
        .expect("zero counters do not require rates");
        assert_eq!(result, EventCostEstimate::Complete(NanoUsd::ZERO));

        let result = calculate_event_cost(
            EventTokenCounts::new(4, 2, 0, 0),
            EventBucketRates::new(Some(NanoUsdPerMillion::ZERO), None, None, None),
        )
        .expect("missing output rate is explicit");
        let EventCostEstimate::Incomplete { missing_rates } = result else {
            panic!("positive output counter without a rate must be incomplete");
        };
        assert!(missing_rates.contains(CostBucket::Output));
        assert!(!missing_rates.contains(CostBucket::Input));

        let result = calculate_event_cost(
            EventTokenCounts::new(4, 0, 3, 0),
            EventBucketRates::new(Some(NanoUsdPerMillion::ZERO), None, Some(rate(0)), None),
        )
        .expect("explicit zero rates are known free");
        assert_eq!(result, EventCostEstimate::Complete(NanoUsd::ZERO));
    }

    #[test]
    fn event_and_aggregate_arithmetic_overflow_is_checked() {
        let max_rate = rate(i64::MAX);
        let result = calculate_four_bucket_cost([u64::MAX; 4], [Some(max_rate); 4]);
        assert_eq!(result, Err(EventCostError::Overflow));

        let result =
            calculate_four_bucket_cost([1_000_001, 0, 0, 0], [Some(max_rate), None, None, None]);
        assert_eq!(result, Err(EventCostError::Overflow));

        let maximum = amount(i64::MAX);
        assert_eq!(
            checked_add_aggregate_nano_usd(maximum, NanoUsd::ZERO),
            Some(maximum)
        );
        assert_eq!(checked_add_aggregate_nano_usd(maximum, amount(1)), None);
        assert_eq!(maximum.checked_add(amount(1)), None);
        assert_eq!(NanoUsd::from_nano_usd(-1), None);
        assert_eq!(NanoUsdPerMillion::from_nano_usd(-1), None);
    }
}

// ---------------------------------------------------------------------------
// models.dev catalog ingestion
// ---------------------------------------------------------------------------

/// The exact parser revision used for normalized catalog rows.
pub const MODELS_DEV_SCHEMA_REVISION: &str = MODELS_DEV_PARSER_REVISION;

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

    /// Returns whether at least one positive counter needs context evidence.
    pub fn requires_context_evidence(&self, counters: EventTokenCounts) -> bool {
        if counters.as_array().iter().all(|counter| *counter == 0) {
            return false;
        }
        !self.tiers.is_empty() || self.legacy_context_over_200k.is_some()
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
    /// Number of normalized provider/model rows.
    pub fn model_count(&self) -> usize {
        self.models.len()
    }

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

    /// Computes freshness using repository status when it refers to this
    /// active snapshot.  In particular, a successful conditional 304 check
    /// advances `last_successful_check_at` without changing this immutable
    /// snapshot's original `fetched_at`.
    pub fn freshness_with_status_at(
        &self,
        status: &CatalogStatus,
        now: SystemTime,
    ) -> CatalogFreshness {
        status.freshness_for_snapshot(self, now)
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

/// Short alias for callers that refer to the source as a generic catalog.
pub fn parse_catalog_payload(body: &[u8]) -> Result<ParsedModelsDevCatalog, CatalogParseError> {
    parse_models_dev_catalog(body)
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

fn validate_json_nesting_depth(body: &[u8]) -> Result<(), CatalogParseError> {
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

fn parse_tiers(raw: &RawValue, path: &str) -> Result<Vec<ContextTier>, CatalogParseError> {
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

fn parse_legacy_context_rate(
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

fn bound_json_error(error: serde_json::Error) -> String {
    // Serde's error is useful for tests and diagnostics, but cap it so a
    // hostile payload cannot turn a bounded response into an unbounded error.
    let mut text = error.to_string();
    if text.len() > 256 {
        text.truncate(256);
    }
    text
}

fn bounded_error_text(value: &str) -> String {
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

/// Bounded, redacted failure code persisted for a catalog refresh attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CatalogRefreshErrorCode {
    /// DNS, TLS, timeout, or other transport failure.
    Transport,
    /// Non-200/non-304 HTTP response.
    HttpStatus,
    /// 200 response did not advertise JSON content.
    ContentType,
    /// Response body exceeded the hard bound.
    ResponseTooLarge,
    /// Empty, truncated, malformed, or semantically invalid payload.
    InvalidPayload,
    /// A 304 was received without an active retained payload.
    MissingSnapshot,
    /// Invalid request/idempotency data.
    InvalidRequest,
}

impl CatalogRefreshErrorCode {
    /// Stable persisted/API spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Transport => "transport",
            Self::HttpStatus => "http_status",
            Self::ContentType => "content_type",
            Self::ResponseTooLarge => "response_too_large",
            Self::InvalidPayload => "invalid_payload",
            Self::MissingSnapshot => "missing_snapshot",
            Self::InvalidRequest => "invalid_request",
        }
    }
}

/// A request to refresh the fixed models.dev catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogRefreshRequest {
    /// Stable retry key supplied by the authorized caller.
    pub idempotency_key: String,
    /// Injectable clock used for metadata and deterministic tests.
    pub requested_at: SystemTime,
}

impl CatalogRefreshRequest {
    /// Creates a request using the current wall clock.
    pub fn new(idempotency_key: impl Into<String>) -> Self {
        Self {
            idempotency_key: idempotency_key.into(),
            requested_at: SystemTime::now(),
        }
    }

    /// Creates a request with an explicit clock value.
    pub fn at(idempotency_key: impl Into<String>, requested_at: SystemTime) -> Self {
        Self {
            idempotency_key: idempotency_key.into(),
            requested_at,
        }
    }
}

/// HTTP result exposed by an injectable models.dev transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelsDevHttpResponse {
    /// HTTP status code.
    pub status: u16,
    /// ETag header, if valid and present.
    pub etag: Option<String>,
    /// Content-Type header value, if present.
    pub content_type: Option<String>,
    /// Bounded decoded response bytes.
    pub body: Vec<u8>,
}

impl ModelsDevHttpResponse {
    /// Constructs a successful JSON response for tests/transports.
    pub fn ok(body: impl Into<Vec<u8>>, etag: Option<String>) -> Self {
        Self {
            status: 200,
            etag,
            content_type: Some("application/json".to_owned()),
            body: body.into(),
        }
    }

    /// Constructs a conditional-not-modified response.
    pub fn not_modified(etag: Option<String>) -> Self {
        Self {
            status: 304,
            etag,
            content_type: None,
            body: Vec::new(),
        }
    }
}

/// Transport failure before a response can be validated.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ModelsDevTransportError {
    #[error("models.dev transport request failed: {0}")]
    Request(String),
    #[error("models.dev response exceeded the {limit}-byte limit")]
    ResponseTooLarge { limit: usize },
}

/// Injectable transport for deterministic catalog fixtures and local tests.
#[async_trait]
pub trait ModelsDevTransport: Send + Sync {
    /// Fetches the fixed endpoint with an optional conditional ETag.
    async fn fetch(
        &self,
        if_none_match: Option<&str>,
    ) -> Result<ModelsDevHttpResponse, ModelsDevTransportError>;
}

/// Production HTTPS transport for the fixed models.dev endpoint.
#[derive(Clone)]
pub struct ReqwestModelsDevTransport {
    client: reqwest::Client,
}

impl ReqwestModelsDevTransport {
    /// Builds a client with redirects disabled and the fixed 30-second bound.
    pub fn new() -> Result<Self, ModelsDevTransportError> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(MODELS_DEV_REQUEST_TIMEOUT)
            .build()
            .map_err(|error| ModelsDevTransportError::Request(error.to_string()))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl ModelsDevTransport for ReqwestModelsDevTransport {
    async fn fetch(
        &self,
        if_none_match: Option<&str>,
    ) -> Result<ModelsDevHttpResponse, ModelsDevTransportError> {
        let mut request = self.client.get(MODELS_DEV_ENDPOINT);
        if let Some(etag) = if_none_match {
            request = request.header(IF_NONE_MATCH, etag);
        }
        let mut response = request
            .send()
            .await
            .map_err(|error| ModelsDevTransportError::Request(error.to_string()))?;
        let status = response.status().as_u16();
        let etag = response
            .headers()
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        if status != 200 {
            return Ok(ModelsDevHttpResponse {
                status,
                etag,
                content_type,
                body: Vec::new(),
            });
        }

        // Reject a declared body larger than the hard bound before asking
        // reqwest to buffer or stream any bytes. The chunk-by-chunk check
        // below remains necessary because a server may omit or falsify this
        // header.
        if response_content_length_exceeds_limit(response.content_length()) {
            return Err(ModelsDevTransportError::ResponseTooLarge {
                limit: MODELS_DEV_MAX_RESPONSE_BYTES,
            });
        }

        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| ModelsDevTransportError::Request(error.to_string()))?
        {
            if body.len().saturating_add(chunk.len()) > MODELS_DEV_MAX_RESPONSE_BYTES {
                return Err(ModelsDevTransportError::ResponseTooLarge {
                    limit: MODELS_DEV_MAX_RESPONSE_BYTES,
                });
            }
            body.extend_from_slice(&chunk);
        }
        Ok(ModelsDevHttpResponse {
            status,
            etag,
            content_type,
            body,
        })
    }
}

/// Errors that prevent a refresh transaction from being attempted.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CatalogClientError {
    #[error("catalog refresh request is invalid: {0}")]
    InvalidRequest(String),
    #[error(transparent)]
    Repository(#[from] CatalogRepositoryError),
}

/// Result of one serialized conditional refresh.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogRefreshOutcome {
    /// A 200 payload was validated and atomically activated.
    Activated {
        snapshot: Box<CatalogSnapshot>,
        status: CatalogStatus,
    },
    /// A 304 updated successful-check metadata without a new payload revision.
    NotModified { status: CatalogStatus },
    /// A concurrent caller already completed a refresh while this call waited.
    AlreadyRefreshed { status: CatalogStatus },
    /// Refresh failed; the status contains the preserved LKG state.
    Failed {
        status: CatalogStatus,
        code: CatalogRefreshErrorCode,
    },
}

/// Bounded conditional catalog client.  Clones share a single-flight gate.
#[derive(Clone)]
pub struct ModelsDevClient {
    transport: Arc<dyn ModelsDevTransport>,
    repository: Arc<dyn PricingCatalogRepository>,
    refresh_gate: Arc<Mutex<()>>,
}

impl ModelsDevClient {
    /// Builds the production client with the fixed models.dev HTTPS transport.
    pub fn new(
        repository: Arc<dyn PricingCatalogRepository>,
    ) -> Result<Self, ModelsDevTransportError> {
        Ok(Self::with_transport(
            repository,
            Arc::new(ReqwestModelsDevTransport::new()?),
        ))
    }

    /// Builds a client with an injectable transport for fixtures/tests.
    pub fn with_transport(
        repository: Arc<dyn PricingCatalogRepository>,
        transport: Arc<dyn ModelsDevTransport>,
    ) -> Self {
        Self {
            transport,
            repository,
            refresh_gate: Arc::new(Mutex::new(())),
        }
    }

    /// Returns current status, deriving stale/fresh state at `now`.
    pub async fn status_at(&self, now: SystemTime) -> Result<CatalogStatus, CatalogClientError> {
        Ok(self.repository.catalog_status().await?.at(now))
    }

    /// Refreshes using the request's injectable clock.
    pub async fn refresh(
        &self,
        request: CatalogRefreshRequest,
    ) -> Result<CatalogRefreshOutcome, CatalogClientError> {
        if request.idempotency_key.trim().is_empty() {
            return Err(CatalogClientError::InvalidRequest(
                "idempotency key must not be empty".to_owned(),
            ));
        }
        if request.idempotency_key.len() > 256 {
            return Err(CatalogClientError::InvalidRequest(
                "idempotency key is too long".to_owned(),
            ));
        }

        if let Some(outcome) = self
            .repository
            .replay_catalog_refresh(&request.idempotency_key)
            .await?
        {
            return Ok(outcome);
        }
        let initial = self.repository.catalog_status().await?;
        let _gate = self.refresh_gate.lock().await;
        if let Some(outcome) = self
            .repository
            .replay_catalog_refresh(&request.idempotency_key)
            .await?
        {
            return Ok(outcome);
        }
        let current = self.repository.catalog_status().await?;
        if current.last_idempotency_key.as_deref() == Some(request.idempotency_key.as_str()) {
            let status = self
                .repository
                .record_catalog_refresh_already_refreshed(
                    request.requested_at,
                    &request.idempotency_key,
                )
                .await?;
            return Ok(CatalogRefreshOutcome::AlreadyRefreshed {
                status: status.at(request.requested_at),
            });
        }
        // A different concurrent caller may have completed while this call
        // waited.  Return its result without issuing a second network fetch.
        if current.version != initial.version || current.last_checked_at != initial.last_checked_at
        {
            let status = self
                .repository
                .record_catalog_refresh_already_refreshed(
                    request.requested_at,
                    &request.idempotency_key,
                )
                .await?;
            return Ok(CatalogRefreshOutcome::AlreadyRefreshed {
                status: status.at(request.requested_at),
            });
        }

        let response = match self.transport.fetch(current.etag.as_deref()).await {
            Ok(response) => response,
            Err(ModelsDevTransportError::ResponseTooLarge { .. }) => {
                return self
                    .failed(
                        current,
                        request.requested_at,
                        request.idempotency_key,
                        CatalogRefreshErrorCode::ResponseTooLarge,
                    )
                    .await;
            }
            Err(ModelsDevTransportError::Request(_)) => {
                return self
                    .failed(
                        current,
                        request.requested_at,
                        request.idempotency_key,
                        CatalogRefreshErrorCode::Transport,
                    )
                    .await;
            }
        };
        if let Some(etag) = response.etag.as_deref() {
            if etag.len() > MODELS_DEV_MAX_OPTIONAL_STRING_BYTES
                || etag.chars().any(char::is_control)
            {
                return self
                    .failed(
                        current,
                        request.requested_at,
                        request.idempotency_key,
                        CatalogRefreshErrorCode::InvalidPayload,
                    )
                    .await;
            }
        }
        match response.status {
            304 => self.handle_not_modified(current, response, request).await,
            200 => self.handle_ok(current, response, request).await,
            _ => {
                self.failed(
                    current,
                    request.requested_at,
                    request.idempotency_key,
                    CatalogRefreshErrorCode::HttpStatus,
                )
                .await
            }
        }
    }

    /// Convenience wrapper for the common current-time refresh path.
    pub async fn refresh_with_idempotency_key(
        &self,
        idempotency_key: impl Into<String>,
    ) -> Result<CatalogRefreshOutcome, CatalogClientError> {
        self.refresh(CatalogRefreshRequest::new(idempotency_key))
            .await
    }

    async fn handle_ok(
        &self,
        current: CatalogStatus,
        response: ModelsDevHttpResponse,
        request: CatalogRefreshRequest,
    ) -> Result<CatalogRefreshOutcome, CatalogClientError> {
        if response.body.len() > MODELS_DEV_MAX_RESPONSE_BYTES {
            return self
                .failed(
                    current,
                    request.requested_at,
                    request.idempotency_key,
                    CatalogRefreshErrorCode::ResponseTooLarge,
                )
                .await;
        }
        if !is_json_content_type(response.content_type.as_deref()) {
            let status = self
                .repository
                .record_catalog_refresh_failure(
                    request.requested_at,
                    CatalogRefreshErrorCode::ContentType,
                    &request.idempotency_key,
                )
                .await?;
            return Ok(CatalogRefreshOutcome::Failed {
                status: status.at(request.requested_at),
                code: CatalogRefreshErrorCode::ContentType,
            });
        }
        let parsed = match parse_models_dev_catalog(&response.body) {
            Ok(parsed) => parsed,
            Err(_) => {
                let current = self.repository.catalog_status().await?;
                return self
                    .failed(
                        current,
                        request.requested_at,
                        request.idempotency_key,
                        CatalogRefreshErrorCode::InvalidPayload,
                    )
                    .await;
            }
        };
        let snapshot = parsed
            .into_snapshot(
                uuid::Uuid::new_v4().to_string(),
                response.etag,
                request.requested_at,
                request.requested_at,
            )
            .map_err(|_| {
                CatalogClientError::InvalidRequest("snapshot materialization failed".to_owned())
            })?;
        let status = self
            .repository
            .activate_catalog_snapshot(snapshot.clone(), &request.idempotency_key)
            .await?;
        Ok(CatalogRefreshOutcome::Activated {
            snapshot: Box::new(snapshot),
            status: status.at(request.requested_at),
        })
    }

    async fn handle_not_modified(
        &self,
        current: CatalogStatus,
        response: ModelsDevHttpResponse,
        request: CatalogRefreshRequest,
    ) -> Result<CatalogRefreshOutcome, CatalogClientError> {
        let Some(active) = self.repository.active_catalog_snapshot().await? else {
            return self
                .failed(
                    current,
                    request.requested_at,
                    request.idempotency_key,
                    CatalogRefreshErrorCode::MissingSnapshot,
                )
                .await;
        };
        let etag = response.etag.or(active.etag.clone());
        if active.parser_revision == MODELS_DEV_PARSER_REVISION {
            let status = self
                .repository
                .record_catalog_not_modified(request.requested_at, etag, &request.idempotency_key)
                .await?;
            return Ok(CatalogRefreshOutcome::NotModified {
                status: status.at(request.requested_at),
            });
        }

        // A 304 with a changed parser revision is a local normalization
        // revision, not a new source payload revision.
        let parsed = match parse_models_dev_catalog(&active.raw_payload) {
            Ok(parsed) => parsed,
            Err(_) => {
                return self
                    .failed(
                        current,
                        request.requested_at,
                        request.idempotency_key,
                        CatalogRefreshErrorCode::InvalidPayload,
                    )
                    .await;
            }
        };
        let snapshot = parsed
            .into_snapshot(
                uuid::Uuid::new_v4().to_string(),
                etag,
                request.requested_at,
                request.requested_at,
            )
            .map_err(|_| {
                CatalogClientError::InvalidRequest("snapshot materialization failed".to_owned())
            })?;
        let status = self
            .repository
            .activate_catalog_snapshot(snapshot.clone(), &request.idempotency_key)
            .await?;
        Ok(CatalogRefreshOutcome::Activated {
            snapshot: Box::new(snapshot),
            status: status.at(request.requested_at),
        })
    }

    async fn failed(
        &self,
        _current: CatalogStatus,
        checked_at: SystemTime,
        idempotency_key: String,
        code: CatalogRefreshErrorCode,
    ) -> Result<CatalogRefreshOutcome, CatalogClientError> {
        let status = self
            .repository
            .record_catalog_refresh_failure(checked_at, code, &idempotency_key)
            .await?;
        Ok(CatalogRefreshOutcome::Failed {
            status: status.at(checked_at),
            code,
        })
    }
}

fn is_json_content_type(value: Option<&str>) -> bool {
    value
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
}

fn response_content_length_exceeds_limit(content_length: Option<u64>) -> bool {
    content_length.is_some_and(|length| length > MODELS_DEV_MAX_RESPONSE_BYTES as u64)
}

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

/// Lifecycle of an exact pricing binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PricingBindingState {
    /// Used for future admissions.
    Active,
    /// Retained for historical provenance but not future resolution.
    Retired,
}

/// An exact catalog rate reference attached to a runtime model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogRateBinding {
    /// Immutable binding row ID.
    pub id: String,
    /// Exact provider ID from `/api.json`.
    pub provider_id: String,
    /// Exact provider-scoped model ID from `/api.json`.
    pub model_id: String,
    /// Immutable normalized rate-revision ID.
    pub rate_revision_id: String,
    /// Immutable catalog snapshot ID.
    pub snapshot_id: String,
    /// Runtime model identifier; compared byte-for-byte.
    pub runtime_model: String,
    /// Normalized base rates.
    pub rates: EventBucketRates,
    /// Exact tiers retained from the source row.
    pub tiers: Vec<ContextTier>,
    /// Legacy threshold-unknown source values, if any.
    pub legacy_context_over_200k: Option<LegacyContextRate>,
    /// Catalog status frozen at selection time when known.
    pub catalog_freshness: CatalogFreshness,
    /// Binding lifecycle state.
    pub state: PricingBindingState,
}

impl CatalogRateBinding {
    /// Returns whether this binding is an exact runtime-model match.
    pub fn matches_runtime_model(&self, runtime_model: &str) -> bool {
        self.state == PricingBindingState::Active && self.runtime_model == runtime_model
    }
}

/// An immutable user-entered rate revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManualRateOverride {
    /// Immutable binding/revision ID.
    pub id: String,
    /// Immutable pricing-subject revision digest.
    pub subject_revision_digest: String,
    /// Runtime model identifier; compared byte-for-byte.
    pub runtime_model: String,
    /// Immutable manual rate revision ID.
    pub rate_revision_id: String,
    /// User-entered rates, preserving absent versus explicit zero.
    pub rates: EventBucketRates,
    /// Time from which this revision may be admitted.
    pub effective_at: SystemTime,
    /// Retirement time, if this revision is no longer used for admissions.
    pub retired_at: Option<SystemTime>,
}

impl ManualRateOverride {
    /// Returns whether this override is active for the exact subject/model.
    pub fn matches(&self, subject_revision_digest: &str, runtime_model: &str) -> bool {
        self.retired_at.is_none()
            && self.subject_revision_digest == subject_revision_digest
            && self.runtime_model == runtime_model
    }
}

/// One source of an exact binding.  Manual and catalog entries are kept as
/// distinct variants so resolution can enforce manual-over-catalog precedence
/// even when a caller is reconciling old and new configuration rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PricingBindingSource {
    /// Exact models.dev provider/model reference.
    ModelsDev(CatalogRateBinding),
    /// Exact user override.
    Manual(ManualRateOverride),
}

impl PricingBindingSource {
    /// Returns the source kind.
    pub const fn kind(&self) -> PricingSourceKind {
        match self {
            Self::ModelsDev(_) => PricingSourceKind::ModelsDevCatalog,
            Self::Manual(_) => PricingSourceKind::ManualOverride,
        }
    }

    /// Returns the exact runtime model.
    pub fn runtime_model(&self) -> &str {
        match self {
            Self::ModelsDev(binding) => &binding.runtime_model,
            Self::Manual(override_) => &override_.runtime_model,
        }
    }

    /// Returns whether the source is active for a subject/model.
    pub fn is_active_for(&self, subject_revision_digest: &str, runtime_model: &str) -> bool {
        match self {
            Self::ModelsDev(binding) => {
                binding.state == PricingBindingState::Active
                    && binding.runtime_model == runtime_model
            }
            Self::Manual(override_) => override_.matches(subject_revision_digest, runtime_model),
        }
    }
}

/// Exact binding/override configuration for one pricing subject revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PricingConfiguration {
    /// Stable provider-entry or CLI-runtime subject ID.
    pub subject_id: String,
    /// Immutable non-secret subject revision digest.
    pub subject_revision_digest: String,
    /// Optimistic configuration version.
    pub version: u64,
    /// Active and retired immutable source rows.
    pub bindings: Vec<PricingBindingSource>,
    /// Last completed mutation key, for idempotent retries.
    pub last_idempotency_key: Option<String>,
    /// Digest of the last desired binding set.
    pub last_update_digest: Option<String>,
}

impl PricingConfiguration {
    /// Creates an empty configuration for a subject revision.
    pub fn new(subject_id: impl Into<String>, subject_revision_digest: impl Into<String>) -> Self {
        Self {
            subject_id: subject_id.into(),
            subject_revision_digest: subject_revision_digest.into(),
            version: 1,
            bindings: Vec::new(),
            last_idempotency_key: None,
            last_update_digest: None,
        }
    }

    /// Resolves manual first, then catalog, using exact runtime-model equality.
    pub fn resolve(&self, runtime_model: &str) -> Option<&PricingBindingSource> {
        self.bindings
            .iter()
            .find(|binding| {
                matches!(binding, PricingBindingSource::Manual(override_)
                    if override_.matches(&self.subject_revision_digest, runtime_model))
            })
            .or_else(|| {
                self.bindings.iter().find(|binding| {
                    matches!(binding, PricingBindingSource::ModelsDev(binding)
                        if binding.matches_runtime_model(runtime_model))
                })
            })
    }

    /// Returns active sources in deterministic manual-then-catalog order.
    pub fn active_for(&self, runtime_model: &str) -> Vec<&PricingBindingSource> {
        let mut manual = Vec::new();
        let mut catalog = Vec::new();
        for binding in &self.bindings {
            if !binding.is_active_for(&self.subject_revision_digest, runtime_model) {
                continue;
            }
            match binding {
                PricingBindingSource::Manual(_) => manual.push(binding),
                PricingBindingSource::ModelsDev(_) => catalog.push(binding),
            }
        }
        manual.extend(catalog);
        manual
    }

    /// Replaces the complete desired exact binding set with optimistic CAS.
    /// Changed manual values produce a new immutable revision; rows omitted
    /// from the desired set are retired rather than deleted.
    pub fn replace(
        &mut self,
        request: ReplacePricingRequest,
        now: SystemTime,
    ) -> Result<BindingMutationResult, PricingBindingError> {
        validate_replace_request(self, &request)?;
        let desired_digest = desired_binding_set_digest(&request.bindings);
        if self.last_idempotency_key.as_deref() == Some(request.idempotency_key.as_str()) {
            if self.last_update_digest.as_deref() == Some(desired_digest.as_str()) {
                return Ok(BindingMutationResult {
                    configuration: self.clone(),
                    changed: false,
                });
            }
            return Err(PricingBindingError::IdempotencyConflict);
        }
        if request.expected_version != self.version {
            return Err(PricingBindingError::VersionConflict {
                expected: request.expected_version,
                actual: self.version,
            });
        }

        let mut changed = false;
        let mut desired_keys = BTreeSet::new();
        for desired in &request.bindings {
            let key = (desired.source_kind(), desired.runtime_model().to_owned());
            desired_keys.insert(key.clone());
            if let Some(existing) = self.bindings.iter().find(|binding| {
                binding_key(binding) == key
                    && binding_matches_desired(binding, desired, &self.subject_revision_digest)
            }) {
                if binding_is_active(existing) {
                    continue;
                }
            }
            // Retire any older row for the same source/model before creating
            // the new immutable revision.
            for binding in &mut self.bindings {
                if binding_key(binding) == key && binding_is_active(binding) {
                    retire_binding_source(binding, now);
                }
            }
            self.bindings.push(desired.to_source(
                &self.subject_id,
                &self.subject_revision_digest,
                now,
            ));
            changed = true;
        }
        for binding in &mut self.bindings {
            if binding_is_active(binding) && !desired_keys.contains(&binding_key(binding)) {
                retire_binding_source(binding, now);
                changed = true;
            }
        }
        if changed {
            self.version = self.version.saturating_add(1);
        }
        self.last_idempotency_key = Some(request.idempotency_key);
        self.last_update_digest = Some(desired_digest);
        Ok(BindingMutationResult {
            configuration: self.clone(),
            changed,
        })
    }

    /// Retires every active exact source for one runtime model.
    pub fn retire_binding(
        &mut self,
        runtime_model: &str,
        expected_version: u64,
        idempotency_key: impl Into<String>,
        now: SystemTime,
    ) -> Result<BindingMutationResult, PricingBindingError> {
        let request = ReplacePricingRequest {
            expected_version,
            idempotency_key: idempotency_key.into(),
            subject_revision_digest: self.subject_revision_digest.clone(),
            bindings: self
                .bindings
                .iter()
                .filter(|binding| {
                    binding_is_active(binding) && binding.runtime_model() != runtime_model
                })
                .map(DesiredPricingBinding::from_source)
                .collect(),
        };
        self.replace(request, now)
    }
}

/// A complete replacement request for one subject's exact bindings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplacePricingRequest {
    /// Optimistic version expected by the caller.
    pub expected_version: u64,
    /// Stable retry/idempotency key.
    pub idempotency_key: String,
    /// Exact subject revision the bindings belong to.
    pub subject_revision_digest: String,
    /// Complete desired set. Omitted active rows are retired.
    pub bindings: Vec<DesiredPricingBinding>,
}

/// One desired exact catalog or manual binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DesiredPricingBinding {
    /// Exact runtime model identifier.
    pub runtime_model: String,
    /// Source and immutable rate data.
    pub source: DesiredPricingSource,
}

impl DesiredPricingBinding {
    /// Creates a desired manual override.
    pub fn manual(runtime_model: impl Into<String>, rates: EventBucketRates) -> Self {
        Self {
            runtime_model: runtime_model.into(),
            source: DesiredPricingSource::Manual { rates },
        }
    }

    /// Creates a desired exact catalog binding.
    #[allow(clippy::too_many_arguments)]
    pub fn models_dev(
        runtime_model: impl Into<String>,
        provider_id: impl Into<String>,
        model_id: impl Into<String>,
        snapshot_id: impl Into<String>,
        rate_revision_id: impl Into<String>,
        rates: EventBucketRates,
        tiers: Vec<ContextTier>,
        legacy_context_over_200k: Option<LegacyContextRate>,
        catalog_freshness: CatalogFreshness,
    ) -> Self {
        Self {
            runtime_model: runtime_model.into(),
            source: DesiredPricingSource::ModelsDev {
                provider_id: provider_id.into(),
                model_id: model_id.into(),
                snapshot_id: snapshot_id.into(),
                rate_revision_id: rate_revision_id.into(),
                rates,
                tiers,
                legacy_context_over_200k,
                catalog_freshness,
            },
        }
    }

    fn runtime_model(&self) -> &str {
        &self.runtime_model
    }

    fn source_kind(&self) -> PricingSourceKind {
        match &self.source {
            DesiredPricingSource::ModelsDev { .. } => PricingSourceKind::ModelsDevCatalog,
            DesiredPricingSource::Manual { .. } => PricingSourceKind::ManualOverride,
        }
    }

    fn to_source(
        &self,
        subject_id: &str,
        subject_revision_digest: &str,
        now: SystemTime,
    ) -> PricingBindingSource {
        match &self.source {
            DesiredPricingSource::ModelsDev {
                provider_id,
                model_id,
                snapshot_id,
                rate_revision_id,
                rates,
                tiers,
                legacy_context_over_200k,
                catalog_freshness,
            } => PricingBindingSource::ModelsDev(CatalogRateBinding {
                id: binding_id(
                    subject_id,
                    subject_revision_digest,
                    &self.runtime_model,
                    &self.source_kind(),
                    rate_revision_id,
                    now,
                ),
                provider_id: provider_id.clone(),
                model_id: model_id.clone(),
                rate_revision_id: rate_revision_id.clone(),
                snapshot_id: snapshot_id.clone(),
                runtime_model: self.runtime_model.clone(),
                rates: *rates,
                tiers: tiers.clone(),
                legacy_context_over_200k: legacy_context_over_200k.clone(),
                catalog_freshness: *catalog_freshness,
                state: PricingBindingState::Active,
            }),
            DesiredPricingSource::Manual { rates } => {
                let rate_revision_id = manual_rate_revision_id(
                    subject_id,
                    subject_revision_digest,
                    &self.runtime_model,
                    *rates,
                    now,
                );
                PricingBindingSource::Manual(ManualRateOverride {
                    id: binding_id(
                        subject_id,
                        subject_revision_digest,
                        &self.runtime_model,
                        &self.source_kind(),
                        &rate_revision_id,
                        now,
                    ),
                    subject_revision_digest: subject_revision_digest.to_owned(),
                    runtime_model: self.runtime_model.clone(),
                    rate_revision_id,
                    rates: *rates,
                    effective_at: now,
                    retired_at: None,
                })
            }
        }
    }

    fn from_source(source: &PricingBindingSource) -> Self {
        match source {
            PricingBindingSource::ModelsDev(binding) => Self {
                runtime_model: binding.runtime_model.clone(),
                source: DesiredPricingSource::ModelsDev {
                    provider_id: binding.provider_id.clone(),
                    model_id: binding.model_id.clone(),
                    snapshot_id: binding.snapshot_id.clone(),
                    rate_revision_id: binding.rate_revision_id.clone(),
                    rates: binding.rates,
                    tiers: binding.tiers.clone(),
                    legacy_context_over_200k: binding.legacy_context_over_200k.clone(),
                    catalog_freshness: binding.catalog_freshness,
                },
            },
            PricingBindingSource::Manual(override_) => {
                Self::manual(override_.runtime_model.clone(), override_.rates)
            }
        }
    }
}

/// Source data in a replacement request.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DesiredPricingSource {
    /// Exact models.dev row and snapshot.
    ModelsDev {
        provider_id: String,
        model_id: String,
        snapshot_id: String,
        rate_revision_id: String,
        rates: EventBucketRates,
        tiers: Vec<ContextTier>,
        legacy_context_over_200k: Option<LegacyContextRate>,
        catalog_freshness: CatalogFreshness,
    },
    /// User-entered fixed-point rates.
    Manual { rates: EventBucketRates },
}

/// Computes the canonical digest for one desired immutable binding revision.
///
/// This is also useful to persistence adapters that need a stable identity
/// for one binding without reimplementing the domain encoding.
pub fn binding_revision_digest(binding: &DesiredPricingBinding) -> String {
    let key = desired_binding_digest_key(binding);
    canonical_hash("binding-revision-v2", |encoded| {
        append_len_prefixed(encoded, &key);
    })
}

/// Result of an optimistic binding mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingMutationResult {
    /// Current configuration after the mutation.
    pub configuration: PricingConfiguration,
    /// Whether any binding/revision/state changed.
    pub changed: bool,
}

/// Validation and optimistic-concurrency errors for pricing configuration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PricingBindingError {
    #[error("pricing subject revision digest is empty")]
    EmptySubjectRevision,
    #[error("pricing binding idempotency key is empty or too long")]
    InvalidIdempotencyKey,
    #[error("pricing binding request targets a different subject revision")]
    SubjectRevisionMismatch,
    #[error("pricing binding version conflict: expected {expected}, actual {actual}")]
    VersionConflict { expected: u64, actual: u64 },
    #[error("pricing binding idempotency key was reused with a different payload")]
    IdempotencyConflict,
    #[error("pricing binding runtime model is empty")]
    EmptyRuntimeModel,
    #[error("pricing binding contains a duplicate exact source/model")]
    DuplicateRuntimeModel,
    #[error("pricing catalog binding has an empty exact identifier")]
    InvalidCatalogReference,
    #[error("pricing binding contains a rate above the $1,000,000-per-million plausibility bound")]
    ImplausiblyLargeRate,
}

fn validate_replace_request(
    configuration: &PricingConfiguration,
    request: &ReplacePricingRequest,
) -> Result<(), PricingBindingError> {
    if configuration.subject_revision_digest.trim().is_empty() {
        return Err(PricingBindingError::EmptySubjectRevision);
    }
    if request.idempotency_key.trim().is_empty() || request.idempotency_key.len() > 256 {
        return Err(PricingBindingError::InvalidIdempotencyKey);
    }
    if request.subject_revision_digest != configuration.subject_revision_digest {
        return Err(PricingBindingError::SubjectRevisionMismatch);
    }
    let mut seen = BTreeSet::new();
    for binding in &request.bindings {
        if binding.runtime_model.is_empty() {
            return Err(PricingBindingError::EmptyRuntimeModel);
        }
        let key = (binding.source_kind(), binding.runtime_model.clone());
        if !seen.insert(key) {
            return Err(PricingBindingError::DuplicateRuntimeModel);
        }
        match &binding.source {
            DesiredPricingSource::ModelsDev {
                provider_id,
                model_id,
                snapshot_id,
                rate_revision_id,
                rates,
                tiers,
                legacy_context_over_200k,
                ..
            } => {
                if provider_id.trim().is_empty()
                    || model_id.trim().is_empty()
                    || snapshot_id.trim().is_empty()
                    || rate_revision_id.trim().is_empty()
                {
                    return Err(PricingBindingError::InvalidCatalogReference);
                }
                if rates_exceed_plausibility_bound(*rates)
                    || tiers
                        .iter()
                        .any(|tier| rates_exceed_plausibility_bound(tier.rates))
                    || legacy_context_over_200k
                        .as_ref()
                        .is_some_and(|legacy| rates_exceed_plausibility_bound(legacy.rates))
                {
                    return Err(PricingBindingError::ImplausiblyLargeRate);
                }
            }
            DesiredPricingSource::Manual { rates } => {
                if rates_exceed_plausibility_bound(*rates) {
                    return Err(PricingBindingError::ImplausiblyLargeRate);
                }
            }
        }
    }
    Ok(())
}

fn rates_exceed_plausibility_bound(rates: EventBucketRates) -> bool {
    rates
        .as_array()
        .iter()
        .flatten()
        .any(|rate| rate.as_nano_usd_per_million() > MAX_MANUAL_RATE_NANO_USD_PER_MILLION)
}

fn binding_key(source: &PricingBindingSource) -> (PricingSourceKind, String) {
    (source.kind(), source.runtime_model().to_owned())
}

fn binding_is_active(source: &PricingBindingSource) -> bool {
    match source {
        PricingBindingSource::ModelsDev(binding) => binding.state == PricingBindingState::Active,
        PricingBindingSource::Manual(override_) => override_.retired_at.is_none(),
    }
}

fn binding_matches_desired(
    source: &PricingBindingSource,
    desired: &DesiredPricingBinding,
    subject_revision_digest: &str,
) -> bool {
    match (source, &desired.source) {
        (
            PricingBindingSource::ModelsDev(existing),
            DesiredPricingSource::ModelsDev {
                provider_id,
                model_id,
                snapshot_id,
                rate_revision_id,
                rates,
                tiers,
                legacy_context_over_200k,
                catalog_freshness,
            },
        ) => {
            existing.runtime_model == desired.runtime_model
                && existing.provider_id == *provider_id
                && existing.model_id == *model_id
                && existing.snapshot_id == *snapshot_id
                && existing.rate_revision_id == *rate_revision_id
                && existing.rates == *rates
                && existing.tiers == *tiers
                && existing.legacy_context_over_200k == *legacy_context_over_200k
                && existing.catalog_freshness == *catalog_freshness
        }
        (PricingBindingSource::Manual(existing), DesiredPricingSource::Manual { rates }) => {
            existing.subject_revision_digest == subject_revision_digest
                && existing.runtime_model == desired.runtime_model
                && existing.rates == *rates
        }
        _ => false,
    }
}

fn retire_binding_source(source: &mut PricingBindingSource, now: SystemTime) {
    match source {
        PricingBindingSource::ModelsDev(binding) => {
            binding.state = PricingBindingState::Retired;
        }
        PricingBindingSource::Manual(override_) => {
            override_.retired_at = Some(now);
        }
    }
}

fn desired_binding_set_digest(bindings: &[DesiredPricingBinding]) -> String {
    let mut keys = bindings
        .iter()
        .map(desired_binding_digest_key)
        .collect::<Vec<Vec<u8>>>();
    keys.sort();
    canonical_hash("desired-binding-set-v2", |encoded| {
        append_u64(
            encoded,
            u64::try_from(keys.len()).expect("in-memory binding count fits u64"),
        );
        for key in keys {
            append_len_prefixed(encoded, &key);
        }
    })
}

fn desired_binding_digest_key(binding: &DesiredPricingBinding) -> Vec<u8> {
    let mut encoded = Vec::new();
    append_str(&mut encoded, &binding.runtime_model);
    match &binding.source {
        DesiredPricingSource::ModelsDev {
            provider_id,
            model_id,
            snapshot_id,
            rate_revision_id,
            rates,
            tiers,
            legacy_context_over_200k,
            catalog_freshness,
        } => {
            append_str(&mut encoded, PricingSourceKind::ModelsDevCatalog.as_str());
            append_str(&mut encoded, provider_id);
            append_str(&mut encoded, model_id);
            append_str(&mut encoded, snapshot_id);
            append_str(&mut encoded, rate_revision_id);
            append_rates(&mut encoded, *rates);
            append_tiers(&mut encoded, tiers);
            append_option_legacy_context(&mut encoded, legacy_context_over_200k.as_ref());
            append_str(&mut encoded, catalog_freshness.as_str());
        }
        DesiredPricingSource::Manual { rates } => {
            append_str(&mut encoded, PricingSourceKind::ManualOverride.as_str());
            append_rates(&mut encoded, *rates);
        }
    }
    encoded
}

fn binding_id(
    subject_id: &str,
    subject_revision_digest: &str,
    runtime_model: &str,
    source_kind: &PricingSourceKind,
    rate_revision_id: &str,
    effective_at: SystemTime,
) -> String {
    canonical_hash("binding-id-v2", |encoded| {
        append_str(encoded, subject_id);
        append_str(encoded, subject_revision_digest);
        append_str(encoded, runtime_model);
        append_str(encoded, source_kind.as_str());
        append_str(encoded, rate_revision_id);
        append_system_time(encoded, effective_at);
    })
}

fn manual_rate_revision_id(
    subject_id: &str,
    subject_revision_digest: &str,
    runtime_model: &str,
    rates: EventBucketRates,
    effective_at: SystemTime,
) -> String {
    canonical_hash("manual-rate-revision-v2", |encoded| {
        append_str(encoded, subject_id);
        append_str(encoded, subject_revision_digest);
        append_str(encoded, runtime_model);
        append_rates(encoded, rates);
        append_system_time(encoded, effective_at);
    })
}

/// Abstract persistence boundary for exact binding replacements.
#[async_trait]
pub trait PricingBindingRepository: Send + Sync {
    /// Reads one subject's current configuration.
    async fn pricing_configuration(
        &self,
        subject_id: &str,
    ) -> Result<PricingConfiguration, CatalogRepositoryError>;

    /// Atomically validates the expected version, creates immutable revisions,
    /// retires omitted rows, and updates the mutable binding pointer.
    async fn replace_pricing_configuration(
        &self,
        subject_id: &str,
        request: ReplacePricingRequest,
        now: SystemTime,
    ) -> Result<PricingConfiguration, CatalogRepositoryError>;
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
    #[error("frozen pricing selection violates semantic invariants")]
    SemanticInvariantViolation,
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
    /// A bucket with no rate in the base band and none in any context tier
    /// cannot move the total, so a report that omits its counter has hidden
    /// no spend. Callers use this to tell an omitted-but-free bucket (safe to
    /// read as zero) from an omitted-but-priced one (a real gap).
    pub fn priced_buckets(&self) -> [bool; 4] {
        let mut priced = [false; 4];
        for rates in self
            .rates
            .iter()
            .chain(self.tiers.iter().map(|tier| &tier.rates))
        {
            for (slot, rate) in priced.iter_mut().zip(rates.as_array()) {
                *slot = *slot || rate.is_some();
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

    /// Returns the frozen catalog freshness after digest verification.
    pub fn validated_catalog_freshness(
        &self,
    ) -> Result<CatalogFreshness, FrozenPriceSelectionError> {
        self.validate().map(|()| self.catalog_freshness)
    }

    /// Returns the canonical selection digest after digest verification.
    pub fn validated_selection_digest(&self) -> Result<&str, FrozenPriceSelectionError> {
        self.validate().map(|()| self.selection_digest.as_str())
    }
}

fn validate_frozen_selection_semantics(
    selection: &FrozenPriceSelection,
) -> Result<(), FrozenPriceSelectionError> {
    let invalid = || Err(FrozenPriceSelectionError::SemanticInvariantViolation);
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
        return invalid();
    }

    if selection.subject_id.is_some() != selection.subject_revision_digest.is_some() {
        return invalid();
    }
    if selection.status == PriceSelectionStatus::Priced && selection.reason.is_some() {
        return invalid();
    }
    if selection.status == PriceSelectionStatus::Unpriced && selection.reason.is_none() {
        return invalid();
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
                return invalid();
            }
        }
        Some(PricingSourceKind::ManualOverride) => {
            if selection.subject_id.is_none()
                || selection.runtime_model.is_none()
                || selection.rate_revision_id.is_none()
                || selection.rates.is_none()
                || catalog_provenance_present
                || !selection.tiers.is_empty()
                || selection.legacy_context_over_200k.is_some()
                || selection.catalog_freshness != CatalogFreshness::NotApplicable
                || selection.status != PriceSelectionStatus::Priced
            {
                return invalid();
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
                return invalid();
            }
            if selection.admitted_provider_id.is_some()
                && selection.admitted_provider_id != selection.catalog_provider_id
            {
                return invalid();
            }
            if selection.status == PriceSelectionStatus::Unpriced
                && !matches!(
                    selection.reason,
                    Some(
                        PriceSelectionReasonCode::MissingRate
                            | PriceSelectionReasonCode::UnresolvedTier
                    )
                )
            {
                return invalid();
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
        return invalid();
    }

    let mut thresholds = BTreeSet::new();
    if selection
        .tiers
        .iter()
        .any(|tier| tier.threshold_tokens == 0 || !thresholds.insert(tier.threshold_tokens))
    {
        return invalid();
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

/// Resolves and freezes one exact candidate price at admission.
///
/// When a mutable catalog status is available, prefer
/// [`resolve_and_freeze_price_selection_with_freshness`] so the selection
/// records the latest successful check (including a 304) rather than the
/// freshness retained on the binding itself.
pub fn resolve_and_freeze_price_selection(
    configuration: &PricingConfiguration,
    runtime_model: Option<&str>,
    candidate_key: Option<&str>,
    attempt_ordinal: u32,
    admitted_provider_id: Option<&str>,
) -> FrozenPriceSelection {
    resolve_and_freeze_price_selection_internal(
        configuration,
        runtime_model,
        candidate_key,
        attempt_ordinal,
        admitted_provider_id,
        None,
    )
}

/// Resolves and freezes one exact candidate price, overriding a catalog
/// binding's historical freshness with the caller's current status when one
/// is available.  Manual selections remain `NotApplicable`.
pub fn resolve_and_freeze_price_selection_with_freshness(
    configuration: &PricingConfiguration,
    runtime_model: Option<&str>,
    candidate_key: Option<&str>,
    attempt_ordinal: u32,
    admitted_provider_id: Option<&str>,
    current_catalog_freshness: CatalogFreshness,
) -> FrozenPriceSelection {
    resolve_and_freeze_price_selection_internal(
        configuration,
        runtime_model,
        candidate_key,
        attempt_ordinal,
        admitted_provider_id,
        Some(current_catalog_freshness),
    )
}

fn resolve_and_freeze_price_selection_internal(
    configuration: &PricingConfiguration,
    runtime_model: Option<&str>,
    candidate_key: Option<&str>,
    attempt_ordinal: u32,
    admitted_provider_id: Option<&str>,
    current_catalog_freshness: Option<CatalogFreshness>,
) -> FrozenPriceSelection {
    let runtime_model_owned = runtime_model
        .map(str::to_owned)
        .filter(|value| !value.trim().is_empty());
    let candidate_key_owned = candidate_key.map(str::to_owned);
    let mut selection = FrozenPriceSelection {
        subject_id: Some(configuration.subject_id.clone()),
        subject_revision_digest: Some(configuration.subject_revision_digest.clone()),
        runtime_model: runtime_model_owned,
        candidate_key: candidate_key_owned,
        attempt_ordinal,
        admitted_provider_id: admitted_provider_id.map(str::to_owned),
        catalog_provider_id: None,
        catalog_model_id: None,
        source_kind: None,
        rate_revision_id: None,
        catalog_snapshot_id: None,
        rates: None,
        tiers: Vec::new(),
        legacy_context_over_200k: None,
        catalog_freshness: CatalogFreshness::NotApplicable,
        status: PriceSelectionStatus::Unpriced,
        reason: None,
        selection_digest: String::new(),
    };
    let Some(runtime_model) = selection.runtime_model.as_deref() else {
        selection.reason = Some(PriceSelectionReasonCode::MissingModel);
        selection.selection_digest = frozen_selection_digest(&selection);
        return selection;
    };
    if runtime_model.is_empty() {
        selection.reason = Some(PriceSelectionReasonCode::MissingModel);
        selection.selection_digest = frozen_selection_digest(&selection);
        return selection;
    }
    let Some(binding) = configuration.resolve(runtime_model) else {
        let retired = configuration
            .bindings
            .iter()
            .any(|binding| binding.runtime_model() == runtime_model && !binding_is_active(binding));
        selection.reason = Some(if retired {
            PriceSelectionReasonCode::RetiredBinding
        } else {
            PriceSelectionReasonCode::MissingBinding
        });
        selection.selection_digest = frozen_selection_digest(&selection);
        return selection;
    };
    selection.status = PriceSelectionStatus::Priced;
    match binding {
        PricingBindingSource::ModelsDev(binding) => {
            selection.source_kind = Some(PricingSourceKind::ModelsDevCatalog);
            selection.rate_revision_id = Some(binding.rate_revision_id.clone());
            selection.catalog_snapshot_id = Some(binding.snapshot_id.clone());
            selection.catalog_provider_id = Some(binding.provider_id.clone());
            selection.catalog_model_id = Some(binding.model_id.clone());
            selection.rates = Some(binding.rates);
            selection.tiers = binding.tiers.clone();
            selection.legacy_context_over_200k = binding.legacy_context_over_200k.clone();
            selection.catalog_freshness =
                current_catalog_freshness.unwrap_or(binding.catalog_freshness);
        }
        PricingBindingSource::Manual(override_) => {
            selection.source_kind = Some(PricingSourceKind::ManualOverride);
            selection.rate_revision_id = Some(override_.rate_revision_id.clone());
            selection.rates = Some(override_.rates);
        }
    }
    selection.selection_digest = frozen_selection_digest(&selection);
    selection
}

/// Alias emphasizing that the function freezes all candidate provenance.
pub fn freeze_price_selection(
    configuration: &PricingConfiguration,
    runtime_model: Option<&str>,
    candidate_key: Option<&str>,
    attempt_ordinal: u32,
    admitted_provider_id: Option<&str>,
) -> FrozenPriceSelection {
    resolve_and_freeze_price_selection(
        configuration,
        runtime_model,
        candidate_key,
        attempt_ordinal,
        admitted_provider_id,
    )
}

/// Alias for [`resolve_and_freeze_price_selection_with_freshness`].
pub fn freeze_price_selection_with_freshness(
    configuration: &PricingConfiguration,
    runtime_model: Option<&str>,
    candidate_key: Option<&str>,
    attempt_ordinal: u32,
    admitted_provider_id: Option<&str>,
    current_catalog_freshness: CatalogFreshness,
) -> FrozenPriceSelection {
    resolve_and_freeze_price_selection_with_freshness(
        configuration,
        runtime_model,
        candidate_key,
        attempt_ordinal,
        admitted_provider_id,
        current_catalog_freshness,
    )
}

/// Computes the canonical digest for a frozen selection's provenance.
fn frozen_selection_digest(selection: &FrozenPriceSelection) -> String {
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

/// Alias for callers using the shorter estimate verb.
pub fn estimate_event(input: UsageEventEstimateInput) -> EventEstimate {
    estimate_usage_event(input)
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

/// Alias for callers using the cost-estimation naming in the REST contract.
pub fn preview_cost_estimation(
    project_id: impl Into<String>,
    snapshot: &CatalogSnapshot,
    events: &[RetrospectiveUsageEvent],
    created_at: SystemTime,
    expires_after: Duration,
) -> Result<RetrospectivePreview, RetrospectivePreviewError> {
    preview_retrospective_estimates(project_id, snapshot, events, created_at, expires_after)
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

#[cfg(test)]
mod catalog_domain_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const FIXTURE: &[u8] = br#"{
      "openai": {
        "id": "openai",
        "name": "OpenAI",
        "ignored_provider_field": {"kept": true},
        "models": {
          "gpt-5": {
            "id": "gpt-5",
            "last_updated": "2026-09-01",
            "cost": {
              "input": 0.049999999999999996,
              "output": 1.2345678915,
              "cache_read": 0,
              "context_over_200k": {"input": 0.1, "output": 2},
              "tiers": [
                {"input": 0.1, "output": 2, "cache_read": 0,
                 "tier": {"type": "context", "size": 200000}}
              ],
              "reasoning": 3,
              "input_audio": 4,
              "unknown_cost_field": "ignored"
            },
            "unknown_model_field": [1, 2, 3]
          },
          "free": {
            "id": "free",
            "name": "Free",
            "cost": {"input": 0, "output": 0}
          },
          "unpriced": {"id": "unpriced", "name": "No price"}
        }
      }
    }"#;

    fn at(seconds: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)
    }

    fn rate(nanos: i64) -> NanoUsdPerMillion {
        NanoUsdPerMillion::from_nano_usd(nanos).expect("test rate")
    }

    fn snapshot() -> CatalogSnapshot {
        parse_models_dev_catalog(FIXTURE)
            .expect("fixture parses")
            .into_snapshot("snapshot-1", Some("\"etag-1\"".to_owned()), at(10), at(10))
            .expect("snapshot builds")
    }

    #[test]
    fn parser_preserves_exact_keys_absence_zero_legacy_and_tiers() {
        let parsed = parse_models_dev_catalog(FIXTURE).expect("fixture parses");
        assert_eq!(parsed.model_count(), 3);
        assert_eq!(parsed.payload_sha256, sha256_hex(FIXTURE));
        let priced = parsed.model_rate("openai", "gpt-5").expect("priced row");
        assert_eq!(priced.source_model_key, "gpt-5");
        assert_eq!(priced.rates.input, Some(rate(50_000_000)));
        assert_eq!(priced.rates.output, Some(rate(1_234_567_892)));
        assert_eq!(priced.rates.cache_read, Some(NanoUsdPerMillion::ZERO));
        assert_eq!(priced.rates.cache_write, None);
        assert_eq!(priced.context_tier_state, ContextTierState::Resolved);
        assert_eq!(priced.tiers.len(), 1);
        assert_eq!(priced.tiers[0].threshold_tokens, 200_000);
        assert!(priced.legacy_context_over_200k.is_some());
        assert!(priced.received_rates_json.as_deref().is_some_and(|raw| {
            raw.contains("unknown_cost_field") && raw.contains("0.049999999999999996")
        }));
        assert_eq!(
            parsed.model_rate("openai", "free").expect("free row").rates,
            EventBucketRates::new(Some(rate(0)), Some(rate(0)), None, None)
        );
        assert_eq!(
            parsed
                .model_rate("openai", "unpriced")
                .expect("unpriced row")
                .rates,
            EventBucketRates::default()
        );
    }

    #[test]
    fn parser_rejects_empty_truncated_duplicate_mismatched_and_oversized_payloads() {
        for payload in [
            br"{}".as_slice(),
            br#"{"openai":{"id":"openai","models":{}}}"#.as_slice(),
            br#"{"wrong":{"id":"openai","models":{"gpt":{"id":"gpt","cost":{"input":1,"output":1}}}}}"#.as_slice(),
            br#"{"openai":{"id":"openai","models":{"wrong":{"id":"gpt","cost":{"input":1,"output":1}}}}}"#.as_slice(),
            br#"{"openai":{"id":"openai","models":{"gpt":{"id":"gpt","cost":{"input":1,"output":}}}}}"#.as_slice(),
            br#"{"openai":{"id":"openai","models":{"gpt":{"id":"gpt"}}"#.as_slice(),
        ] {
            assert!(parse_models_dev_catalog(payload).is_err(), "payload should reject");
        }
        let duplicate = br#"{"openai":{"id":"openai","models":{"gpt":{"id":"gpt","cost":{"input":1,"output":1}}}},"openai":{"id":"openai","models":{"gpt":{"id":"gpt","cost":{"input":1,"output":1}}}}}"#;
        assert!(parse_models_dev_catalog(duplicate).is_err());
        let oversized = vec![b' '; MODELS_DEV_MAX_RESPONSE_BYTES + 1];
        assert!(matches!(
            parse_models_dev_catalog(&oversized),
            Err(CatalogParseError::ResponseTooLarge { .. })
        ));
    }

    #[test]
    fn parser_pins_identifier_optional_string_and_nesting_boundaries() {
        let identifier = "p".repeat(MODELS_DEV_MAX_IDENTIFIER_BYTES);
        let payload = format!(
            r#"{{"{identifier}":{{"id":"{identifier}","models":{{"m":{{"id":"m","last_updated":"ok","cost":{{"input":1,"output":1}}}}}}}}}}"#
        );
        assert!(parse_models_dev_catalog(payload.as_bytes()).is_ok());

        let too_long_identifier = "p".repeat(MODELS_DEV_MAX_IDENTIFIER_BYTES + 1);
        let payload = format!(
            r#"{{"{too_long_identifier}":{{"id":"{too_long_identifier}","models":{{"m":{{"id":"m","cost":{{"input":1,"output":1}}}}}}}}}}"#
        );
        assert!(matches!(
            parse_models_dev_catalog(payload.as_bytes()),
            Err(CatalogParseError::InvalidField { .. })
        ));

        let optional = "x".repeat(MODELS_DEV_MAX_OPTIONAL_STRING_BYTES);
        let payload = format!(
            r#"{{"openai":{{"id":"openai","name":"{optional}","models":{{"m":{{"id":"m","name":"{optional}","release_date":"{optional}","last_updated":"{optional}","cost":{{"input":1,"output":1}}}}}}}}}}"#
        );
        assert!(parse_models_dev_catalog(payload.as_bytes()).is_ok());

        let too_long_optional = "x".repeat(MODELS_DEV_MAX_OPTIONAL_STRING_BYTES + 1);
        let payload = format!(
            r#"{{"openai":{{"id":"openai","models":{{"m":{{"id":"m","last_updated":"{too_long_optional}","cost":{{"input":1,"output":1}}}}}}}}}}"#
        );
        assert!(matches!(
            parse_models_dev_catalog(payload.as_bytes()),
            Err(CatalogParseError::InvalidField { .. })
        ));

        for field in ["name", "release_date", "last_updated"] {
            let payload = String::from(
                r#"{"openai":{"id":"openai","name":"ok","models":{"m":{"id":"m","name":"ok","release_date":"ok","last_updated":"ok","cost":{"input":1,"output":1}}}}}"#,
            );
            let payload = payload.replace(
                &format!("\"{field}\":\"ok\""),
                &format!("\"{field}\":\"{too_long_optional}\""),
            );
            assert!(matches!(
                parse_models_dev_catalog(payload.as_bytes()),
                Err(CatalogParseError::InvalidField { .. })
            ));
        }

        let nested_payload = nested_unknown_field_payload(MODELS_DEV_MAX_JSON_NESTING_DEPTH);
        assert!(parse_models_dev_catalog(&nested_payload).is_ok());
        let too_deep_payload = nested_unknown_field_payload(MODELS_DEV_MAX_JSON_NESTING_DEPTH + 1);
        assert!(matches!(
            parse_models_dev_catalog(&too_deep_payload),
            Err(CatalogParseError::NestingTooDeep { .. })
        ));

        let parsed = parse_models_dev_catalog(br#"{"openai":{"id":"openai","models":{"m":{"id":"m","cost":{"input":1,"output":1}}}}}"#)
            .expect("snapshot fixture parses");
        let max_etag = "e".repeat(MODELS_DEV_MAX_OPTIONAL_STRING_BYTES);
        assert!(parsed
            .clone()
            .into_snapshot("snapshot", Some(max_etag), at(1), at(1))
            .is_ok());
        let oversized_etag = "e".repeat(MODELS_DEV_MAX_OPTIONAL_STRING_BYTES + 1);
        assert_eq!(
            parsed.into_snapshot("snapshot", Some(oversized_etag), at(1), at(1)),
            Err(CatalogSnapshotError::InvalidEtag)
        );
    }

    fn nested_unknown_field_payload(depth: usize) -> Vec<u8> {
        // Root/provider/models/model account for four containers.  The cost
        // object is closed before the unknown array value, which supplies the
        // remaining requested depth and is still fully checked by the bounded
        // JSON parser.
        let array_depth = depth.saturating_sub(4);
        let mut payload = String::from(
            "{\"openai\":{\"id\":\"openai\",\"models\":{\"m\":{\"id\":\"m\",\"cost\":{\"input\":1,\"output\":1},\"ignored\":",
        );
        for _ in 0..array_depth {
            payload.push('[');
        }
        payload.push('0');
        for _ in 0..array_depth {
            payload.push(']');
        }
        payload.push_str("}}}}");
        payload.into_bytes()
    }

    #[test]
    fn legacy_only_context_rate_is_retained_with_unknown_threshold() {
        let payload = br#"{"openai":{"id":"openai","models":{"legacy":{"id":"legacy","cost":{"input":1,"output":2,"context_over_200k":{"input":3,"output":4}}}}}}"#;
        let parsed = parse_models_dev_catalog(payload).expect("legacy fixture parses");
        let model = parsed.model_rate("openai", "legacy").expect("legacy row");
        assert_eq!(model.context_tier_state, ContextTierState::ThresholdUnknown);
        assert!(model.tiers.is_empty());
        assert_eq!(
            model
                .legacy_context_over_200k
                .as_ref()
                .expect("legacy rates")
                .rates
                .input,
            Some(rate(3_000_000_000))
        );
    }

    #[test]
    fn lkg_status_transitions_from_fresh_to_stale_then_failed_without_losing_identity() {
        let mut status = CatalogStatus::absent();
        status.active_snapshot_id = Some("snapshot-1".to_owned());
        status.revision = Some("revision-1".to_owned());
        status.last_successful_check_at = Some(at(10));
        status.last_checked_at = Some(at(10));
        assert_eq!(
            status.at(at(10 + MODELS_DEV_STALE_AFTER.as_secs())).state,
            CatalogState::Fresh
        );
        assert_eq!(
            status
                .at(at(10 + MODELS_DEV_STALE_AFTER.as_secs() + 1))
                .state,
            CatalogState::Stale
        );
        status.last_error_code = Some(CatalogRefreshErrorCode::Transport.as_str().to_owned());
        assert_eq!(
            status
                .at(at(10 + MODELS_DEV_STALE_AFTER.as_secs() + 1))
                .state,
            CatalogState::RefreshFailed
        );
        assert_eq!(status.active_snapshot_id.as_deref(), Some("snapshot-1"));
    }

    #[test]
    fn freshness_uses_latest_successful_check_for_an_active_snapshot() {
        let snapshot = snapshot();
        let mut status = CatalogStatus::absent();
        status.active_snapshot_id = Some(snapshot.id.clone());
        status.revision = Some(snapshot.revision_digest.clone());
        status.last_checked_at = Some(at(20));
        status.last_successful_check_at = Some(at(20));
        // A stale persisted boundary from the immutable fetch must not
        // override the newer successful conditional check below.
        status.stale_after = at(10).checked_add(MODELS_DEV_STALE_AFTER);

        // The immutable payload was fetched at t=10, but a conditional 304 at
        // t=20 is the latest successful check and therefore keeps it fresh.
        let now = at(20 + MODELS_DEV_STALE_AFTER.as_secs());
        assert_eq!(snapshot.freshness_at(now), CatalogFreshness::Stale);
        assert_eq!(
            status.freshness_for_snapshot(&snapshot, now),
            CatalogFreshness::Fresh
        );
        assert_eq!(
            snapshot.freshness_with_status_at(&status, now),
            CatalogFreshness::Fresh
        );

        let mut incomplete_status = status;
        incomplete_status.last_successful_check_at = None;
        assert_eq!(
            incomplete_status.freshness_for_snapshot(&snapshot, at(20)),
            CatalogFreshness::Fresh
        );
    }

    #[tokio::test]
    async fn conditional_refresh_200_then_304_reuses_revision_and_etag() {
        let repository = Arc::new(FakeCatalogRepository::default());
        let transport = Arc::new(SequenceTransport::new(vec![
            Ok(ModelsDevHttpResponse::ok(
                FIXTURE,
                Some("etag-1".to_owned()),
            )),
            Ok(ModelsDevHttpResponse::not_modified(None)),
        ]));
        let client = ModelsDevClient::with_transport(repository.clone(), transport.clone());
        let first = client
            .refresh(CatalogRefreshRequest::at("first", at(10)))
            .await
            .expect("first refresh");
        let (first_id, first_revision) = match first {
            CatalogRefreshOutcome::Activated { snapshot, .. } => {
                (snapshot.id, snapshot.revision_digest)
            }
            other => panic!("expected activation, got {other:?}"),
        };
        let second = client
            .refresh(CatalogRefreshRequest::at("second", at(20)))
            .await
            .expect("304 refresh");
        match second {
            CatalogRefreshOutcome::NotModified { status } => {
                assert_eq!(
                    status.active_snapshot_id.as_deref(),
                    Some(first_id.as_str())
                );
                assert_eq!(status.revision.as_deref(), Some(first_revision.as_str()));
                assert_eq!(status.state, CatalogState::Fresh);
            }
            other => panic!("expected 304, got {other:?}"),
        }
        assert_eq!(transport.fetch_count.load(Ordering::Relaxed), 2);
        let seen_etags = transport.seen_etags().await;
        assert_eq!(seen_etags.get(1).and_then(Option::as_deref), Some("etag-1"));
        assert_eq!(
            repository
                .catalog_status()
                .await
                .expect("status")
                .last_idempotency_key
                .as_deref(),
            Some("second")
        );
    }

    #[tokio::test]
    async fn conditional_304_with_changed_parser_revision_reparses_retained_body() {
        let repository = Arc::new(FakeCatalogRepository::default());
        let mut old = snapshot();
        old.parser_revision = "models-dev-old-parser".to_owned();
        old.revision_digest = catalog_revision_digest(&old.payload_sha256, &old.parser_revision);
        repository.install_snapshot(old.clone()).await;
        let transport = Arc::new(SequenceTransport::new(vec![Ok(
            ModelsDevHttpResponse::not_modified(Some("etag-2".to_owned())),
        )]));
        let client = ModelsDevClient::with_transport(repository.clone(), transport);
        let result = client
            .refresh(CatalogRefreshRequest::at("reparse", at(20)))
            .await
            .expect("reparse refresh");
        let reparsed = match result {
            CatalogRefreshOutcome::Activated { snapshot, .. } => snapshot,
            other => panic!("expected reparse activation, got {other:?}"),
        };
        assert_ne!(reparsed.id, old.id);
        assert_eq!(reparsed.payload_sha256, old.payload_sha256);
        assert_eq!(reparsed.parser_revision, MODELS_DEV_PARSER_REVISION);
        assert_ne!(reparsed.revision_digest, old.revision_digest);
    }

    #[tokio::test]
    async fn failed_refresh_keeps_last_known_good_snapshot_and_marks_failed() {
        let repository = Arc::new(FakeCatalogRepository::default());
        let transport = Arc::new(SequenceTransport::new(vec![
            Ok(ModelsDevHttpResponse::ok(
                FIXTURE,
                Some("etag-1".to_owned()),
            )),
            Ok(ModelsDevHttpResponse {
                status: 200,
                etag: Some("etag-2".to_owned()),
                content_type: Some("application/json".to_owned()),
                body: b"{truncated".to_vec(),
            }),
        ]));
        let client = ModelsDevClient::with_transport(repository.clone(), transport);
        let first = client
            .refresh(CatalogRefreshRequest::at("good", at(10)))
            .await
            .expect("good refresh");
        let first_id = match first {
            CatalogRefreshOutcome::Activated { snapshot, .. } => snapshot.id,
            other => panic!("expected activation, got {other:?}"),
        };
        let failed = client
            .refresh(CatalogRefreshRequest::at("bad", at(20)))
            .await
            .expect("failure is represented as outcome");
        match failed {
            CatalogRefreshOutcome::Failed { status, code } => {
                assert_eq!(code, CatalogRefreshErrorCode::InvalidPayload);
                assert_eq!(status.state, CatalogState::RefreshFailed);
                assert_eq!(
                    status.active_snapshot_id.as_deref(),
                    Some(first_id.as_str())
                );
            }
            other => panic!("expected failed outcome, got {other:?}"),
        }
        assert_eq!(repository.snapshot().await.expect("LKG"), first_id);
    }

    #[test]
    fn declared_content_length_is_bounded_before_streaming() {
        assert!(!response_content_length_exceeds_limit(None));
        assert!(!response_content_length_exceeds_limit(Some(
            MODELS_DEV_MAX_RESPONSE_BYTES as u64,
        )));
        assert!(response_content_length_exceeds_limit(Some(
            MODELS_DEV_MAX_RESPONSE_BYTES as u64 + 1,
        )));
    }

    #[tokio::test]
    async fn refresh_rejects_redirects_bad_content_type_and_unbounded_body() {
        for status_code in [301, 302] {
            let repository = Arc::new(FakeCatalogRepository::default());
            let transport = Arc::new(SequenceTransport::new(vec![Ok(ModelsDevHttpResponse {
                status: status_code,
                etag: None,
                content_type: Some("text/html".to_owned()),
                body: Vec::new(),
            })]));
            let client = ModelsDevClient::with_transport(repository, transport.clone());
            let result = client
                .refresh(CatalogRefreshRequest::at(
                    format!("redirect-{status_code}"),
                    at(10),
                ))
                .await
                .expect("redirect is represented as a failed outcome");
            assert!(matches!(
                result,
                CatalogRefreshOutcome::Failed {
                    code: CatalogRefreshErrorCode::HttpStatus,
                    ..
                }
            ));
            // There is no second queued response: a redirect can never be
            // followed by the bounded client.
            assert_eq!(transport.fetch_count.load(Ordering::Relaxed), 1);
        }

        let repository = Arc::new(FakeCatalogRepository::default());
        let transport = Arc::new(SequenceTransport::new(vec![
            Ok(ModelsDevHttpResponse {
                status: 200,
                etag: None,
                content_type: Some("text/html".to_owned()),
                body: FIXTURE.to_vec(),
            }),
            Ok(ModelsDevHttpResponse {
                status: 200,
                etag: Some("etag-json".to_owned()),
                content_type: Some("application/json; charset=utf-8".to_owned()),
                body: FIXTURE.to_vec(),
            }),
        ]));
        let client = ModelsDevClient::with_transport(repository, transport.clone());
        let wrong_type = client
            .refresh(CatalogRefreshRequest::at("wrong-content-type", at(10)))
            .await
            .expect("content type is represented as a failed outcome");
        assert!(matches!(
            wrong_type,
            CatalogRefreshOutcome::Failed {
                code: CatalogRefreshErrorCode::ContentType,
                ..
            }
        ));
        let charset = client
            .refresh(CatalogRefreshRequest::at("json-with-charset", at(11)))
            .await
            .expect("JSON charset is accepted");
        assert!(matches!(charset, CatalogRefreshOutcome::Activated { .. }));

        let repository = Arc::new(FakeCatalogRepository::default());
        let transport = Arc::new(SequenceTransport::new(vec![Ok(ModelsDevHttpResponse::ok(
            vec![b'x'; MODELS_DEV_MAX_RESPONSE_BYTES + 1],
            None,
        ))]));
        let client = ModelsDevClient::with_transport(repository, transport.clone());
        let oversized = client
            .refresh(CatalogRefreshRequest::at("oversized-body", at(10)))
            .await
            .expect("oversized body is represented as a failed outcome");
        assert!(matches!(
            oversized,
            CatalogRefreshOutcome::Failed {
                code: CatalogRefreshErrorCode::ResponseTooLarge,
                ..
            }
        ));
        assert_eq!(transport.fetch_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn transport_failure_exposes_only_a_stable_redacted_code() {
        let repository = Arc::new(FakeCatalogRepository::default());
        let sensitive = "GET https://models.dev/api.json bearer=secret-token body=private-payload";
        let transport = Arc::new(SequenceTransport::new(vec![Err(
            ModelsDevTransportError::Request(sensitive.to_owned()),
        )]));
        let client = ModelsDevClient::with_transport(repository.clone(), transport);
        let result = client
            .refresh(CatalogRefreshRequest::at("transport-redaction", at(10)))
            .await
            .expect("transport failures are represented as a failed outcome");
        match result {
            CatalogRefreshOutcome::Failed { status, code } => {
                assert_eq!(code, CatalogRefreshErrorCode::Transport);
                assert_eq!(
                    status.last_error_code.as_deref(),
                    Some(CatalogRefreshErrorCode::Transport.as_str())
                );
                let rendered = format!("{status:?}");
                assert!(!rendered.contains("secret-token"));
                assert!(!rendered.contains("private-payload"));
                assert!(!rendered.contains("models.dev/api.json"));
            }
            other => panic!("expected redacted transport failure, got {other:?}"),
        }
    }

    #[test]
    fn exact_manual_precedence_retirement_and_optimistic_conflict() {
        let now = at(10);
        let mut configuration = PricingConfiguration::new("subject", "subject-rev-1");
        let catalog = DesiredPricingBinding::models_dev(
            "gpt-custom",
            "openai",
            "gpt-5",
            "snapshot-1",
            "catalog-rate-1",
            EventBucketRates::new(
                Some(rate(1_000_000_000)),
                Some(rate(2_000_000_000)),
                None,
                None,
            ),
            Vec::new(),
            None,
            CatalogFreshness::Fresh,
        );
        let manual = DesiredPricingBinding::manual(
            "gpt-custom",
            EventBucketRates::new(
                Some(rate(3_000_000_000)),
                Some(rate(4_000_000_000)),
                None,
                None,
            ),
        );
        let replaced = configuration
            .replace(
                ReplacePricingRequest {
                    expected_version: 1,
                    idempotency_key: "bind-1".to_owned(),
                    subject_revision_digest: "subject-rev-1".to_owned(),
                    bindings: vec![catalog, manual],
                },
                now,
            )
            .expect("binding replacement");
        assert!(replaced.changed);
        let resolved = configuration.resolve("gpt-custom").expect("manual wins");
        assert!(matches!(resolved, PricingBindingSource::Manual(_)));
        assert_eq!(
            match resolved {
                PricingBindingSource::Manual(override_) => override_.rates.input,
                PricingBindingSource::ModelsDev(_) => None,
            },
            Some(rate(3_000_000_000))
        );
        let conflict = configuration.replace(
            ReplacePricingRequest {
                expected_version: 1,
                idempotency_key: "stale".to_owned(),
                subject_revision_digest: "subject-rev-1".to_owned(),
                bindings: Vec::new(),
            },
            at(11),
        );
        assert!(matches!(
            conflict,
            Err(PricingBindingError::VersionConflict { .. })
        ));
        let retired = configuration
            .retire_binding("gpt-custom", configuration.version, "retire-1", at(12))
            .expect("retirement");
        assert!(retired.changed);
        assert!(configuration.resolve("gpt-custom").is_none());
        assert!(configuration
            .bindings
            .iter()
            .all(|binding| !binding_is_active(binding)));
    }

    #[test]
    fn manual_revision_and_binding_ids_are_scoped_to_the_subject() {
        let request = |key: &str| ReplacePricingRequest {
            expected_version: 1,
            idempotency_key: key.to_owned(),
            subject_revision_digest: "same-identity-revision".to_owned(),
            bindings: vec![DesiredPricingBinding::manual(
                "gpt",
                EventBucketRates::new(Some(rate(1_000_000_000)), None, None, None),
            )],
        };
        let mut first = PricingConfiguration::new("subject-a", "same-identity-revision");
        let mut second = PricingConfiguration::new("subject-b", "same-identity-revision");
        first
            .replace(request("first"), at(10))
            .expect("first replace");
        second
            .replace(request("second"), at(10))
            .expect("second replace");

        let first_manual = match first.bindings.first().expect("first binding") {
            PricingBindingSource::Manual(override_) => override_,
            PricingBindingSource::ModelsDev(_) => panic!("expected manual binding"),
        };
        let second_manual = match second.bindings.first().expect("second binding") {
            PricingBindingSource::Manual(override_) => override_,
            PricingBindingSource::ModelsDev(_) => panic!("expected manual binding"),
        };
        assert_ne!(
            first_manual.rate_revision_id,
            second_manual.rate_revision_id
        );
        assert_ne!(first_manual.id, second_manual.id);
    }

    #[test]
    fn admission_can_freeze_current_catalog_freshness_over_binding_metadata() {
        let now = at(10);
        let mut configuration = PricingConfiguration::new("subject", "subject-rev-1");
        configuration
            .replace(
                ReplacePricingRequest {
                    expected_version: 1,
                    idempotency_key: "catalog".to_owned(),
                    subject_revision_digest: "subject-rev-1".to_owned(),
                    bindings: vec![DesiredPricingBinding::models_dev(
                        "gpt",
                        "openai",
                        "gpt-5",
                        "snapshot-1",
                        "rate-1",
                        EventBucketRates::new(Some(rate(1)), Some(rate(2)), None, None),
                        Vec::new(),
                        None,
                        CatalogFreshness::Stale,
                    )],
                },
                now,
            )
            .expect("catalog binding");
        let selection = resolve_and_freeze_price_selection_with_freshness(
            &configuration,
            Some("gpt"),
            Some("candidate"),
            0,
            Some("openai"),
            CatalogFreshness::Fresh,
        );
        assert_eq!(selection.catalog_freshness, CatalogFreshness::Fresh);
        assert_eq!(selection.validate(), Ok(()));

        let historical = resolve_and_freeze_price_selection(
            &configuration,
            Some("gpt"),
            Some("candidate"),
            0,
            Some("openai"),
        );
        assert_eq!(historical.catalog_freshness, CatalogFreshness::Stale);
    }

    #[test]
    fn replacement_rejects_manual_rates_above_the_catalog_bound() {
        let mut configuration = PricingConfiguration::new("subject", "subject-rev-1");
        let result = configuration.replace(
            ReplacePricingRequest {
                expected_version: 1,
                idempotency_key: "too-large".to_owned(),
                subject_revision_digest: "subject-rev-1".to_owned(),
                bindings: vec![DesiredPricingBinding::manual(
                    "gpt",
                    EventBucketRates::new(
                        Some(rate(MODELS_DEV_MAX_RATE_NANO_USD_PER_MILLION + 1)),
                        None,
                        None,
                        None,
                    ),
                )],
            },
            at(10),
        );
        assert_eq!(result, Err(PricingBindingError::ImplausiblyLargeRate));
    }

    #[test]
    fn estimate_distinguishes_reported_unmetered_zero_missing_rate_tier_and_identity() {
        let now = at(10);
        let mut configuration = PricingConfiguration::new("subject", "subject-rev-1");
        configuration
            .replace(
                ReplacePricingRequest {
                    expected_version: 1,
                    idempotency_key: "manual".to_owned(),
                    subject_revision_digest: "subject-rev-1".to_owned(),
                    bindings: vec![DesiredPricingBinding::manual(
                        "gpt",
                        EventBucketRates::new(Some(rate(1_000_000_000)), None, None, None),
                    )],
                },
                now,
            )
            .expect("manual binding");
        let selection = resolve_and_freeze_price_selection(
            &configuration,
            Some("gpt"),
            Some("candidate-a"),
            0,
            Some("openai"),
        );
        let reported = estimate_usage_event(UsageEventEstimateInput {
            counters: None,
            provider_reported_amount: Some(NanoUsd::from_nano_usd(7).expect("amount")),
            selection: selection.clone(),
            actual_provider_id: None,
            actual_model_id: None,
            context_tokens: None,
        });
        assert_eq!(
            reported,
            EventEstimate::ProviderReported {
                amount: NanoUsd::from_nano_usd(7).expect("amount")
            }
        );
        let unmetered = estimate_usage_event(UsageEventEstimateInput {
            counters: None,
            provider_reported_amount: None,
            selection: selection.clone(),
            actual_provider_id: None,
            actual_model_id: None,
            context_tokens: None,
        });
        assert_eq!(
            unmetered,
            EventEstimate::Unpriced {
                reason: PriceSelectionReasonCode::Unmetered
            }
        );
        let partial = estimate_usage_event(UsageEventEstimateInput {
            counters: Some(EventTokenCounts::new(1, 1, 0, 0)),
            provider_reported_amount: None,
            selection: selection.clone(),
            actual_provider_id: None,
            actual_model_id: None,
            context_tokens: None,
        });
        assert!(matches!(
            partial,
            EventEstimate::Partial {
                reason: PriceSelectionReasonCode::MissingRate,
                ..
            }
        ));
        // A caller must not be able to mutate a frozen selection into a
        // different price while retaining the original digest.
        let mut tampered_selection = selection.clone();
        tampered_selection.rates = Some(EventBucketRates::new(
            Some(NanoUsdPerMillion::ZERO),
            Some(NanoUsdPerMillion::ZERO),
            None,
            None,
        ));
        assert_eq!(
            tampered_selection.validate(),
            Err(FrozenPriceSelectionError::DigestMismatch)
        );
        let rejected_mutation = estimate_usage_event(UsageEventEstimateInput {
            counters: Some(EventTokenCounts::new(1, 1, 0, 0)),
            provider_reported_amount: None,
            selection: tampered_selection,
            actual_provider_id: None,
            actual_model_id: None,
            context_tokens: None,
        });
        assert_eq!(
            rejected_mutation,
            EventEstimate::Unpriced {
                reason: PriceSelectionReasonCode::MissingBinding
            }
        );

        let mut tampered_reported_selection = selection.clone();
        tampered_reported_selection.catalog_freshness = CatalogFreshness::Fresh;
        let rejected_reported_mutation = estimate_usage_event(UsageEventEstimateInput {
            counters: None,
            provider_reported_amount: Some(NanoUsd::from_nano_usd(7).expect("amount")),
            selection: tampered_reported_selection,
            actual_provider_id: None,
            actual_model_id: None,
            context_tokens: None,
        });
        assert_eq!(
            rejected_reported_mutation,
            EventEstimate::Unpriced {
                reason: PriceSelectionReasonCode::MissingBinding
            }
        );

        let mut free_configuration = PricingConfiguration::new("subject", "subject-rev-1");
        free_configuration
            .replace(
                ReplacePricingRequest {
                    expected_version: 1,
                    idempotency_key: "free".to_owned(),
                    subject_revision_digest: "subject-rev-1".to_owned(),
                    bindings: vec![DesiredPricingBinding::manual(
                        "gpt",
                        EventBucketRates::new(
                            Some(NanoUsdPerMillion::ZERO),
                            Some(NanoUsdPerMillion::ZERO),
                            None,
                            None,
                        ),
                    )],
                },
                now,
            )
            .expect("free binding");
        let free_selection = resolve_and_freeze_price_selection(
            &free_configuration,
            Some("gpt"),
            Some("candidate-a"),
            0,
            Some("openai"),
        );
        let free = estimate_usage_event(UsageEventEstimateInput {
            counters: Some(EventTokenCounts::new(1, 1, 0, 0)),
            provider_reported_amount: None,
            selection: free_selection,
            actual_provider_id: None,
            actual_model_id: None,
            context_tokens: None,
        });
        assert!(matches!(free, EventEstimate::Estimated { amount, .. } if amount == NanoUsd::ZERO));
        let mismatch = estimate_usage_event(UsageEventEstimateInput {
            counters: Some(EventTokenCounts::new(1, 0, 0, 0)),
            provider_reported_amount: None,
            selection,
            actual_provider_id: Some("other-provider".to_owned()),
            actual_model_id: Some("other-model".to_owned()),
            context_tokens: None,
        });
        assert_eq!(
            mismatch,
            EventEstimate::Unpriced {
                reason: PriceSelectionReasonCode::IdentityMismatch
            }
        );
    }

    #[test]
    fn estimate_uses_catalog_provider_for_providerless_runtime_admissions() {
        let mut configuration = PricingConfiguration::new("subject", "subject-rev-1");
        configuration
            .replace(
                ReplacePricingRequest {
                    expected_version: 1,
                    idempotency_key: "catalog".to_owned(),
                    subject_revision_digest: "subject-rev-1".to_owned(),
                    bindings: vec![DesiredPricingBinding::models_dev(
                        "gpt",
                        "openai",
                        "gpt-5",
                        "snapshot-1",
                        "rate-1",
                        EventBucketRates::new(Some(rate(1_000_000_000)), None, None, None),
                        Vec::new(),
                        None,
                        CatalogFreshness::Fresh,
                    )],
                },
                at(10),
            )
            .expect("catalog binding");
        let selection = resolve_and_freeze_price_selection(
            &configuration,
            Some("gpt"),
            Some("candidate"),
            0,
            None,
        );
        assert_eq!(selection.admitted_provider_id, None);
        assert_eq!(selection.catalog_provider_id.as_deref(), Some("openai"));

        let matching = estimate_usage_event(UsageEventEstimateInput {
            counters: Some(EventTokenCounts::new(1, 0, 0, 0)),
            provider_reported_amount: None,
            selection: selection.clone(),
            actual_provider_id: Some("openai".to_owned()),
            actual_model_id: Some("gpt".to_owned()),
            context_tokens: None,
        });
        assert!(matches!(matching, EventEstimate::Estimated { .. }));

        let mismatching = estimate_usage_event(UsageEventEstimateInput {
            counters: Some(EventTokenCounts::new(1, 0, 0, 0)),
            provider_reported_amount: None,
            selection: selection.clone(),
            actual_provider_id: Some("other-provider".to_owned()),
            actual_model_id: Some("gpt".to_owned()),
            context_tokens: None,
        });
        assert_eq!(
            mismatching,
            EventEstimate::Unpriced {
                reason: PriceSelectionReasonCode::IdentityMismatch
            }
        );

        let absent_report = estimate_usage_event(UsageEventEstimateInput {
            counters: Some(EventTokenCounts::new(1, 0, 0, 0)),
            provider_reported_amount: None,
            selection,
            actual_provider_id: None,
            actual_model_id: Some("gpt".to_owned()),
            context_tokens: None,
        });
        assert!(matches!(absent_report, EventEstimate::Estimated { .. }));
    }

    #[test]
    fn frozen_selection_digest_covers_freshness_and_option_boundaries() {
        let mut configuration = PricingConfiguration::new("subject", "subject-rev-1");
        configuration
            .replace(
                ReplacePricingRequest {
                    expected_version: 1,
                    idempotency_key: "manual".to_owned(),
                    subject_revision_digest: "subject-rev-1".to_owned(),
                    bindings: vec![DesiredPricingBinding::manual(
                        "gpt",
                        EventBucketRates::new(Some(rate(1)), Some(rate(2)), None, None),
                    )],
                },
                at(10),
            )
            .expect("manual binding");
        let selection = resolve_and_freeze_price_selection(
            &configuration,
            Some("gpt"),
            Some("candidate"),
            0,
            None,
        );
        assert_eq!(selection.validate(), Ok(()));

        let mut freshness_mutation = selection.clone();
        freshness_mutation.catalog_freshness = CatalogFreshness::Fresh;
        assert_eq!(
            freshness_mutation.validate(),
            Err(FrozenPriceSelectionError::DigestMismatch)
        );

        let mut empty_option_mutation = selection.clone();
        empty_option_mutation.candidate_key = Some(String::new());
        assert_eq!(
            empty_option_mutation.validate(),
            Err(FrozenPriceSelectionError::DigestMismatch)
        );
    }

    #[test]
    fn frozen_selection_semantics_reject_incomplete_and_inconsistent_provenance() {
        let rates = EventBucketRates::new(Some(rate(1)), Some(rate(2)), None, None);
        let missing_priced_provenance = FrozenPriceSelection::from_parts(
            Some("subject".to_owned()),
            Some("subject-rev-1".to_owned()),
            Some("gpt".to_owned()),
            None,
            0,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Vec::new(),
            None,
            CatalogFreshness::NotApplicable,
            PriceSelectionStatus::Priced,
            None,
        );
        assert_eq!(
            missing_priced_provenance.validate(),
            Err(FrozenPriceSelectionError::SemanticInvariantViolation)
        );
        assert!(!missing_priced_provenance.is_priced());

        let missing_unpriced_reason = FrozenPriceSelection::from_parts(
            None,
            None,
            Some("gpt".to_owned()),
            None,
            0,
            Some("openai".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
            Vec::new(),
            None,
            CatalogFreshness::NotApplicable,
            PriceSelectionStatus::Unpriced,
            None,
        );
        assert_eq!(
            missing_unpriced_reason.validate(),
            Err(FrozenPriceSelectionError::SemanticInvariantViolation)
        );

        let manual_with_catalog_freshness = FrozenPriceSelection::from_parts(
            Some("subject".to_owned()),
            Some("subject-rev-1".to_owned()),
            Some("gpt".to_owned()),
            None,
            0,
            None,
            None,
            None,
            Some(PricingSourceKind::ManualOverride),
            Some("manual-rate-1".to_owned()),
            None,
            Some(rates),
            Vec::new(),
            None,
            CatalogFreshness::Fresh,
            PriceSelectionStatus::Priced,
            None,
        );
        assert_eq!(
            manual_with_catalog_freshness.validate(),
            Err(FrozenPriceSelectionError::SemanticInvariantViolation)
        );

        let catalog_providerless = FrozenPriceSelection::from_parts(
            None,
            None,
            Some("gpt".to_owned()),
            None,
            0,
            None,
            Some("openai".to_owned()),
            Some("gpt-5".to_owned()),
            Some(PricingSourceKind::ModelsDevCatalog),
            Some("catalog-rate-1".to_owned()),
            Some("catalog-snapshot-1".to_owned()),
            Some(rates),
            Vec::new(),
            None,
            CatalogFreshness::Fresh,
            PriceSelectionStatus::Priced,
            None,
        );
        assert_eq!(catalog_providerless.validate(), Ok(()));
        assert!(catalog_providerless.is_priced());

        let catalog_with_invalid_freshness = FrozenPriceSelection::from_parts(
            None,
            None,
            Some("gpt".to_owned()),
            None,
            0,
            None,
            Some("openai".to_owned()),
            Some("gpt-5".to_owned()),
            Some(PricingSourceKind::ModelsDevCatalog),
            Some("catalog-rate-1".to_owned()),
            Some("catalog-snapshot-1".to_owned()),
            Some(rates),
            Vec::new(),
            None,
            CatalogFreshness::NotApplicable,
            PriceSelectionStatus::Priced,
            None,
        );
        assert_eq!(
            catalog_with_invalid_freshness.validate(),
            Err(FrozenPriceSelectionError::SemanticInvariantViolation)
        );

        let catalog_with_conflicting_provider = FrozenPriceSelection::from_parts(
            None,
            None,
            Some("gpt".to_owned()),
            None,
            0,
            Some("different-provider".to_owned()),
            Some("openai".to_owned()),
            Some("gpt-5".to_owned()),
            Some(PricingSourceKind::ModelsDevCatalog),
            Some("catalog-rate-1".to_owned()),
            Some("catalog-snapshot-1".to_owned()),
            Some(rates),
            Vec::new(),
            None,
            CatalogFreshness::Fresh,
            PriceSelectionStatus::Priced,
            None,
        );
        assert_eq!(
            catalog_with_conflicting_provider.validate(),
            Err(FrozenPriceSelectionError::SemanticInvariantViolation)
        );
    }

    #[test]
    fn retrospective_preview_is_exact_idempotent_input_and_preserves_reported() {
        let snapshot = snapshot();
        let events = vec![
            RetrospectiveUsageEvent {
                event_id: "reported".to_owned(),
                provider_id: Some("openai".to_owned()),
                model_id: Some("gpt-5".to_owned()),
                counters: None,
                provider_reported_amount: Some(NanoUsd::from_nano_usd(11).expect("amount")),
                context_tokens: None,
                occurred_at: at(1),
            },
            RetrospectiveUsageEvent {
                event_id: "eligible".to_owned(),
                provider_id: Some("openai".to_owned()),
                model_id: Some("gpt-5".to_owned()),
                counters: Some(EventTokenCounts::new(1_000_000, 0, 0, 0)),
                provider_reported_amount: None,
                context_tokens: Some(1),
                occurred_at: at(2),
            },
            RetrospectiveUsageEvent {
                event_id: "missing".to_owned(),
                provider_id: Some("openai".to_owned()),
                model_id: Some("does-not-exist".to_owned()),
                counters: Some(EventTokenCounts::new(1, 0, 0, 0)),
                provider_reported_amount: None,
                context_tokens: None,
                occurred_at: at(3),
            },
        ];
        let preview = preview_retrospective_estimates(
            "project-1",
            &snapshot,
            &events,
            at(10),
            Duration::from_secs(60),
        )
        .expect("preview");
        assert_eq!(preview.eligible_event_count, 1);
        assert_eq!(preview.unmatched_event_count, 1);
        assert_eq!(preview.already_reported_event_count, 1);
        assert_eq!(
            preview.projected_cost,
            Some(NanoUsd::from_nano_usd(50_000_000).expect("cost"))
        );
        let changed_digest = retrospective_usage_set_digest(&[RetrospectiveUsageEvent {
            event_id: "eligible".to_owned(),
            provider_id: Some("openai".to_owned()),
            model_id: Some("gpt-5".to_owned()),
            counters: Some(EventTokenCounts::new(2_000_000, 0, 0, 0)),
            provider_reported_amount: None,
            context_tokens: Some(1),
            occurred_at: at(2),
        }]);
        assert_ne!(preview.usage_set_digest, changed_digest);
    }

    #[test]
    fn retrospective_and_preview_digests_reject_delimiter_collisions() {
        let left = RetrospectiveUsageEvent {
            event_id: "event|provider".to_owned(),
            provider_id: Some("model".to_owned()),
            model_id: None,
            counters: None,
            provider_reported_amount: None,
            context_tokens: None,
            occurred_at: at(1),
        };
        let right = RetrospectiveUsageEvent {
            event_id: "event".to_owned(),
            provider_id: Some("provider|model".to_owned()),
            ..left.clone()
        };
        assert_ne!(
            retrospective_usage_set_digest(&[left]),
            retrospective_usage_set_digest(&[right])
        );
        let newline_left = RetrospectiveUsageEvent {
            event_id: "event\nprovider".to_owned(),
            provider_id: Some("model".to_owned()),
            model_id: None,
            counters: None,
            provider_reported_amount: None,
            context_tokens: None,
            occurred_at: at(1),
        };
        let newline_right = RetrospectiveUsageEvent {
            event_id: "event".to_owned(),
            provider_id: Some("provider\nmodel".to_owned()),
            ..newline_left.clone()
        };
        assert_ne!(
            retrospective_usage_set_digest(&[newline_left]),
            retrospective_usage_set_digest(&[newline_right])
        );

        let snapshot = snapshot();
        let preview_left = preview_retrospective_estimates(
            "project|snapshot",
            &snapshot,
            &[],
            at(10),
            Duration::from_secs(60),
        )
        .expect("left preview");
        let mut alternate_snapshot = snapshot.clone();
        alternate_snapshot.id = "snapshot".to_owned();
        let preview_right = preview_retrospective_estimates(
            "project",
            &alternate_snapshot,
            &[],
            at(10),
            Duration::from_secs(60),
        )
        .expect("right preview");
        assert_ne!(preview_left.id, preview_right.id);
    }

    struct FakeCatalogRepository {
        status: Mutex<CatalogStatus>,
        snapshot: Mutex<Option<CatalogSnapshot>>,
    }

    impl Default for FakeCatalogRepository {
        fn default() -> Self {
            Self {
                status: Mutex::new(CatalogStatus::absent()),
                snapshot: Mutex::new(None),
            }
        }
    }

    impl FakeCatalogRepository {
        async fn install_snapshot(&self, snapshot: CatalogSnapshot) {
            let mut status = self.status.lock().await;
            status.state = CatalogState::Fresh;
            status.active_snapshot_id = Some(snapshot.id.clone());
            status.revision = Some(snapshot.revision_digest.clone());
            status.etag = snapshot.etag.clone();
            status.last_checked_at = Some(snapshot.fetched_at);
            status.last_successful_check_at = Some(snapshot.fetched_at);
            status.stale_after = snapshot.fetched_at.checked_add(MODELS_DEV_STALE_AFTER);
            status.last_error_code = None;
            status.version += 1;
            *self.snapshot.lock().await = Some(snapshot);
        }

        async fn snapshot(&self) -> Option<String> {
            self.snapshot
                .lock()
                .await
                .as_ref()
                .map(|snapshot| snapshot.id.clone())
        }
    }

    #[async_trait]
    impl PricingCatalogRepository for FakeCatalogRepository {
        async fn catalog_status(&self) -> Result<CatalogStatus, CatalogRepositoryError> {
            Ok(self.status.lock().await.clone())
        }

        async fn active_catalog_snapshot(
            &self,
        ) -> Result<Option<CatalogSnapshot>, CatalogRepositoryError> {
            Ok(self.snapshot.lock().await.clone())
        }

        async fn activate_catalog_snapshot(
            &self,
            snapshot: CatalogSnapshot,
            idempotency_key: &str,
        ) -> Result<CatalogStatus, CatalogRepositoryError> {
            *self.snapshot.lock().await = Some(snapshot.clone());
            let mut status = self.status.lock().await;
            status.state = CatalogState::Fresh;
            status.active_snapshot_id = Some(snapshot.id);
            status.revision = Some(snapshot.revision_digest);
            status.etag = snapshot.etag;
            status.last_checked_at = Some(snapshot.fetched_at);
            status.last_successful_check_at = Some(snapshot.fetched_at);
            status.stale_after = snapshot.fetched_at.checked_add(MODELS_DEV_STALE_AFTER);
            status.last_error_code = None;
            status.last_idempotency_key = Some(idempotency_key.to_owned());
            status.version += 1;
            Ok(status.clone())
        }

        async fn record_catalog_not_modified(
            &self,
            checked_at: SystemTime,
            etag: Option<String>,
            idempotency_key: &str,
        ) -> Result<CatalogStatus, CatalogRepositoryError> {
            let mut status = self.status.lock().await;
            status.state = CatalogState::Fresh;
            status.etag = etag.or_else(|| status.etag.clone());
            status.last_checked_at = Some(checked_at);
            status.last_successful_check_at = Some(checked_at);
            status.stale_after = checked_at.checked_add(MODELS_DEV_STALE_AFTER);
            status.last_error_code = None;
            status.last_idempotency_key = Some(idempotency_key.to_owned());
            status.version += 1;
            Ok(status.clone())
        }

        async fn record_catalog_refresh_failure(
            &self,
            checked_at: SystemTime,
            code: CatalogRefreshErrorCode,
            idempotency_key: &str,
        ) -> Result<CatalogStatus, CatalogRepositoryError> {
            let mut status = self.status.lock().await;
            status.state = CatalogState::RefreshFailed;
            status.last_checked_at = Some(checked_at);
            status.last_error_code = Some(code.as_str().to_owned());
            status.last_idempotency_key = Some(idempotency_key.to_owned());
            status.version += 1;
            Ok(status.clone())
        }
    }

    struct SequenceTransport {
        responses: Mutex<Vec<Result<ModelsDevHttpResponse, ModelsDevTransportError>>>,
        seen_etags: Mutex<Vec<Option<String>>>,
        fetch_count: AtomicUsize,
    }

    impl SequenceTransport {
        fn new(responses: Vec<Result<ModelsDevHttpResponse, ModelsDevTransportError>>) -> Self {
            Self {
                responses: Mutex::new(responses),
                seen_etags: Mutex::new(Vec::new()),
                fetch_count: AtomicUsize::new(0),
            }
        }

        async fn seen_etags(&self) -> Vec<Option<String>> {
            self.seen_etags.lock().await.clone()
        }
    }

    #[async_trait]
    impl ModelsDevTransport for SequenceTransport {
        async fn fetch(
            &self,
            if_none_match: Option<&str>,
        ) -> Result<ModelsDevHttpResponse, ModelsDevTransportError> {
            self.fetch_count.fetch_add(1, Ordering::Relaxed);
            self.seen_etags
                .lock()
                .await
                .push(if_none_match.map(str::to_owned));
            self.responses.lock().await.remove(0)
        }
    }
}
