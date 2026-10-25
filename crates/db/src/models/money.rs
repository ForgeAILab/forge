//! Fixed point money, rates, parsing, and event cost arithmetic.

use super::*;

// -------------------------------------------------------------------------
// Provider pricing and usage ledger (V135)
// -------------------------------------------------------------------------

/// Number of nano-USD in one USD.  Rates are stored per million tokens and
/// amounts are stored per event; neither path uses a binary floating point
/// value for new data.
pub const NANO_USD_PER_USD: i128 = 1_000_000_000;
pub const TOKENS_PER_MILLION: i128 = 1_000_000;
pub const PRICING_CATALOG_SOURCE_URL: &str = "https://models.dev/api.json";
pub const PRICING_PARSER_REVISION: &str = "models-dev-api-v1";
pub const COST_FORMULA_REVISION: &str = "token-cost-v1";
pub const COST_ROUNDING_REVISION: &str = "cost-rounding-v1";

/// A checked, non-negative nano-USD amount.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NanoUsd(i64);

impl NanoUsd {
    pub const ZERO: Self = Self(0);

    pub const fn from_nano_usd(value: i64) -> Option<Self> {
        if value < 0 {
            None
        } else {
            Some(Self(value))
        }
    }

    pub const fn as_nano_usd(self) -> i64 {
        self.0
    }

    pub fn checked_add(self, other: Self) -> Option<Self> {
        Self::from_nano_usd(self.0.checked_add(other.0)?)
    }

    pub fn to_usd_decimal(self) -> String {
        format_nano_usd(self.0)
    }
}

impl fmt::Display for NanoUsd {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_usd_decimal())
    }
}

/// A checked, non-negative USD-per-million rate.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NanoUsdPerMillion(i64);

impl NanoUsdPerMillion {
    pub const ZERO: Self = Self(0);

    pub const fn from_nano_usd(value: i64) -> Option<Self> {
        if value < 0 {
            None
        } else {
            Some(Self(value))
        }
    }

    pub const fn as_nano_usd_per_million(self) -> i64 {
        self.0
    }

    pub fn parse_manual_usd_per_million(input: &str) -> Result<Self, PricingParseError> {
        parse_manual_usd_per_million(input)
    }

    pub fn parse_models_dev_json_number(input: &str) -> Result<Self, PricingParseError> {
        parse_models_dev_json_number(input)
    }

    pub fn to_usd_decimal_per_million(self) -> String {
        format_nano_usd(self.0)
    }
}

impl fmt::Display for NanoUsdPerMillion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_usd_decimal_per_million())
    }
}

/// Parsing failures at the fixed-point boundary.  Manual values are strict
/// decimal strings; models.dev values additionally permit JSON exponent
/// notation and are rounded once to nano-USD.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PricingParseError {
    #[error("USD rate is empty")]
    Empty,
    #[error("USD rate cannot contain whitespace")]
    WhitespaceNotAllowed,
    #[error("USD rate cannot contain a sign")]
    SignNotAllowed,
    #[error("USD rate cannot contain an exponent")]
    ExponentNotAllowed,
    #[error("USD rate is negative")]
    Negative,
    #[error("USD rate is non-finite")]
    NonFinite,
    #[error("USD rate is not a valid JSON number")]
    InvalidSyntax,
    #[error("USD rate exponent is out of range")]
    ExponentOverflow,
    #[error("USD rate has more than nine fractional digits")]
    TooManyFractionalDigits,
    #[error("USD rate does not fit checked nano-USD storage")]
    Overflow,
    #[error("USD rate exceeds the configured plausibility bound")]
    ImplausiblyLarge,
}

/// Four disjoint token counters. `None` in a persisted row means that the
/// producer did not provide that counter; an explicit zero is represented by
/// `Some(0)`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenCounters {
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cache_read_tokens: Option<i64>,
    pub cache_write_tokens: Option<i64>,
}

impl TokenCounters {
    pub const fn new(
        input_tokens: Option<i64>,
        output_tokens: Option<i64>,
        cache_read_tokens: Option<i64>,
        cache_write_tokens: Option<i64>,
    ) -> Self {
        Self {
            input_tokens,
            output_tokens,
            cache_read_tokens,
            cache_write_tokens,
        }
    }

    pub fn all_present(self) -> bool {
        self.input_tokens.is_some()
            && self.output_tokens.is_some()
            && self.cache_read_tokens.is_some()
            && self.cache_write_tokens.is_some()
    }

    pub fn all_non_negative(self) -> bool {
        [
            self.input_tokens,
            self.output_tokens,
            self.cache_read_tokens,
            self.cache_write_tokens,
        ]
        .into_iter()
        .all(|value| value.is_none_or(|value| value >= 0))
    }
}

/// Optional per-million rates for the four token counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RateBuckets {
    pub input: Option<i64>,
    pub output: Option<i64>,
    pub cache_read: Option<i64>,
    pub cache_write: Option<i64>,
}

impl RateBuckets {
    pub const fn new(
        input: Option<i64>,
        output: Option<i64>,
        cache_read: Option<i64>,
        cache_write: Option<i64>,
    ) -> Self {
        Self {
            input,
            output,
            cache_read,
            cache_write,
        }
    }

    pub fn all_non_negative(self) -> bool {
        [self.input, self.output, self.cache_read, self.cache_write]
            .into_iter()
            .all(|value| value.is_none_or(|value| value >= 0))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CostBucket {
    Input,
    Output,
    CacheRead,
    CacheWrite,
}

impl CostBucket {
    pub const ALL: [Self; 4] = [Self::Input, Self::Output, Self::CacheRead, Self::CacheWrite];
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MissingRateBuckets(u8);

impl MissingRateBuckets {
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub const fn contains(self, bucket: CostBucket) -> bool {
        let bit = match bucket {
            CostBucket::Input => 1,
            CostBucket::Output => 2,
            CostBucket::CacheRead => 4,
            CostBucket::CacheWrite => 8,
        };
        self.0 & bit != 0
    }

    const fn insert(self, bucket: CostBucket) -> Self {
        let bit = match bucket {
            CostBucket::Input => 1,
            CostBucket::Output => 2,
            CostBucket::CacheRead => 4,
            CostBucket::CacheWrite => 8,
        };
        Self(self.0 | bit)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventCostEstimate {
    Complete(NanoUsd),
    Incomplete { missing_rates: MissingRateBuckets },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EventCostError {
    #[error("event cost arithmetic overflow")]
    Overflow,
}

impl EventCostEstimate {
    pub const fn amount(self) -> Option<NanoUsd> {
        match self {
            Self::Complete(amount) => Some(amount),
            Self::Incomplete { .. } => None,
        }
    }

    pub const fn is_complete(self) -> bool {
        matches!(self, Self::Complete(_))
    }
}

/// Estimate a single event after summing all four measurable buckets.  The
/// division/rounding occurs exactly once, after the checked i128 sum.
pub fn calculate_event_cost(
    counters: TokenCounters,
    rates: RateBuckets,
) -> Result<EventCostEstimate, EventCostError> {
    if !counters.all_non_negative() || !rates.all_non_negative() {
        return Err(EventCostError::Overflow);
    }
    let counters = [
        counters.input_tokens.unwrap_or(0),
        counters.output_tokens.unwrap_or(0),
        counters.cache_read_tokens.unwrap_or(0),
        counters.cache_write_tokens.unwrap_or(0),
    ];
    let rates = [
        rates.input,
        rates.output,
        rates.cache_read,
        rates.cache_write,
    ];
    let mut missing = MissingRateBuckets::default();
    for (index, (&counter, rate)) in counters.iter().zip(rates.iter()).enumerate() {
        if counter > 0 && rate.is_none() {
            missing = missing.insert(CostBucket::ALL[index]);
        }
    }
    if !missing.is_empty() {
        return Ok(EventCostEstimate::Incomplete {
            missing_rates: missing,
        });
    }

    let mut numerator = 0_i128;
    for (&counter, rate) in counters.iter().zip(rates.iter()) {
        if let Some(rate) = rate {
            numerator = numerator
                .checked_add(
                    i128::from(counter)
                        .checked_mul(i128::from(*rate))
                        .ok_or(EventCostError::Overflow)?,
                )
                .ok_or(EventCostError::Overflow)?;
        }
    }
    let rounded =
        round_half_away_from_zero(numerator, TOKENS_PER_MILLION).ok_or(EventCostError::Overflow)?;
    let rounded = i64::try_from(rounded).map_err(|_| EventCostError::Overflow)?;
    Ok(EventCostEstimate::Complete(
        NanoUsd::from_nano_usd(rounded).ok_or(EventCostError::Overflow)?,
    ))
}

pub fn round_half_away_from_zero(numerator: i128, denominator: i128) -> Option<i128> {
    if denominator == 0 {
        return None;
    }
    let negative = numerator.is_negative() != denominator.is_negative();
    let numerator = numerator.checked_abs()? as u128;
    let denominator = denominator.checked_abs()? as u128;
    let quotient = numerator / denominator;
    let remainder = numerator % denominator;
    let round_up = remainder > denominator / 2
        || (denominator.is_multiple_of(2) && remainder == denominator / 2);
    let rounded = quotient.checked_add(u128::from(round_up))?;
    if negative {
        if rounded == (i128::MAX as u128) + 1 {
            Some(i128::MIN)
        } else {
            Some(-i128::try_from(rounded).ok()?)
        }
    } else {
        i128::try_from(rounded).ok()
    }
}

fn parse_i64_digits(digits: &[u8]) -> Option<i64> {
    let mut value = 0_i64;
    for digit in digits {
        if !digit.is_ascii_digit() {
            return None;
        }
        value = value
            .checked_mul(10)?
            .checked_add(i64::from(*digit - b'0'))?;
    }
    Some(value)
}

fn format_nano_usd(nanos: i64) -> String {
    debug_assert!(nanos >= 0);
    let whole = nanos / NANO_USD_PER_USD as i64;
    let fraction = nanos % NANO_USD_PER_USD as i64;
    if fraction == 0 {
        return whole.to_string();
    }
    let mut fraction_text = format!("{fraction:09}");
    while fraction_text.ends_with('0') {
        fraction_text.pop();
    }
    format!("{whole}.{fraction_text}")
}

fn parse_manual_usd_per_million(input: &str) -> Result<NanoUsdPerMillion, PricingParseError> {
    if input.is_empty() {
        return Err(PricingParseError::Empty);
    }
    if input.chars().any(char::is_whitespace) {
        return Err(PricingParseError::WhitespaceNotAllowed);
    }
    if input.starts_with(['+', '-']) {
        return Err(if input.starts_with('-') {
            PricingParseError::Negative
        } else {
            PricingParseError::SignNotAllowed
        });
    }
    if input.bytes().any(|byte| matches!(byte, b'e' | b'E')) {
        return Err(PricingParseError::ExponentNotAllowed);
    }
    let (whole, fraction) = input.split_once('.').map_or((input, ""), |(w, f)| (w, f));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || (input.contains('.')
            && (fraction.is_empty() || !fraction.bytes().all(|byte| byte.is_ascii_digit())))
    {
        return Err(PricingParseError::InvalidSyntax);
    }
    if fraction.len() > 9 {
        return Err(PricingParseError::TooManyFractionalDigits);
    }
    let whole_nanos = parse_i64_digits(whole.as_bytes())
        .ok_or(PricingParseError::Overflow)?
        .checked_mul(NANO_USD_PER_USD as i64)
        .ok_or(PricingParseError::Overflow)?;
    let fraction_value = parse_i64_digits(fraction.as_bytes())
        .ok_or(PricingParseError::Overflow)?
        .checked_mul(10_i64.pow((9 - fraction.len()) as u32))
        .ok_or(PricingParseError::Overflow)?;
    let nanos = whole_nanos
        .checked_add(fraction_value)
        .ok_or(PricingParseError::Overflow)?;
    if nanos > 1_000_000_i64 * NANO_USD_PER_USD as i64 {
        return Err(PricingParseError::ImplausiblyLarge);
    }
    Ok(NanoUsdPerMillion(nanos))
}

/// Parse a models.dev JSON number from its lexical decimal representation.
/// This intentionally avoids `serde_json::Number::as_f64`, preserving source
/// precision before one deterministic nano-USD rounding step.
fn parse_models_dev_json_number(input: &str) -> Result<NanoUsdPerMillion, PricingParseError> {
    if input.is_empty() {
        return Err(PricingParseError::Empty);
    }
    if input.chars().any(char::is_whitespace) {
        return Err(PricingParseError::InvalidSyntax);
    }
    if input.eq_ignore_ascii_case("nan")
        || input.eq_ignore_ascii_case("inf")
        || input.eq_ignore_ascii_case("infinity")
        || input.eq_ignore_ascii_case("+inf")
        || input.eq_ignore_ascii_case("-inf")
        || input.eq_ignore_ascii_case("+infinity")
        || input.eq_ignore_ascii_case("-infinity")
    {
        return Err(PricingParseError::NonFinite);
    }
    let bytes = input.as_bytes();
    if bytes.first() == Some(&b'-') {
        return Err(PricingParseError::Negative);
    }
    if bytes.first() == Some(&b'+') {
        return Err(PricingParseError::InvalidSyntax);
    }
    let mut cursor;
    match bytes.first() {
        Some(b'0') => {
            cursor = 1;
            if bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
                return Err(PricingParseError::InvalidSyntax);
            }
        }
        Some(b'1'..=b'9') => {
            cursor = 1;
            while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
                cursor += 1;
            }
        }
        _ => return Err(PricingParseError::InvalidSyntax),
    }
    let integer_end = cursor;
    let fraction_start = if bytes.get(cursor) == Some(&b'.') {
        cursor += 1;
        let start = cursor;
        if !bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            return Err(PricingParseError::InvalidSyntax);
        }
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            cursor += 1;
        }
        Some(start)
    } else {
        None
    };
    let fraction_end = cursor;
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
        let start = cursor;
        let mut magnitude = 0_u64;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            magnitude = magnitude
                .checked_mul(10)
                .and_then(|value| value.checked_add(u64::from(bytes[cursor] - b'0')))
                .ok_or(PricingParseError::ExponentOverflow)?;
            cursor += 1;
        }
        if cursor == start {
            return Err(PricingParseError::InvalidSyntax);
        }
        if negative {
            const I64_MIN_MAGNITUDE: u64 = 1_u64 << 63;
            if magnitude > I64_MIN_MAGNITUDE {
                return Err(PricingParseError::ExponentOverflow);
            }
            if magnitude == I64_MIN_MAGNITUDE {
                i64::MIN
            } else {
                -i64::try_from(magnitude).map_err(|_| PricingParseError::ExponentOverflow)?
            }
        } else {
            i64::try_from(magnitude).map_err(|_| PricingParseError::ExponentOverflow)?
        }
    } else {
        0
    };
    if cursor != bytes.len() {
        return Err(PricingParseError::InvalidSyntax);
    }
    let mut digits = Vec::with_capacity(bytes.len());
    digits.extend_from_slice(&bytes[..integer_end]);
    if let Some(start) = fraction_start {
        digits.extend_from_slice(&bytes[start..fraction_end]);
    }
    let leading_zeroes = digits.iter().take_while(|digit| **digit == b'0').count();
    if leading_zeroes == digits.len() {
        return Ok(NanoUsdPerMillion::ZERO);
    }
    let mut significant = digits[leading_zeroes..].to_vec();
    while significant.last() == Some(&b'0') {
        significant.pop();
    }
    let integer_len = i64::try_from(integer_end).map_err(|_| PricingParseError::Overflow)?;
    let leading_zeroes = i64::try_from(leading_zeroes).map_err(|_| PricingParseError::Overflow)?;
    let decimal_position = integer_len
        .checked_add(exponent)
        .ok_or(PricingParseError::ExponentOverflow)?
        .checked_sub(leading_zeroes)
        .ok_or(PricingParseError::ExponentOverflow)?;
    normalize_models_dev_digits(&significant, decimal_position)
}

fn normalize_models_dev_digits(
    digits: &[u8],
    decimal_position: i64,
) -> Result<NanoUsdPerMillion, PricingParseError> {
    if digits.is_empty() {
        return Ok(NanoUsdPerMillion::ZERO);
    }
    let digit_count = i64::try_from(digits.len()).map_err(|_| PricingParseError::Overflow)?;
    let nano_decimal_position = decimal_position
        .checked_add(9)
        .ok_or(PricingParseError::ImplausiblyLarge)?;
    let Some(shift) = nano_decimal_position.checked_sub(digit_count) else {
        return Ok(NanoUsdPerMillion::ZERO);
    };
    let nanos = if shift >= 0 {
        let total_digits = digit_count
            .checked_add(shift)
            .ok_or(PricingParseError::ImplausiblyLarge)?;
        if total_digits > 19 {
            return Err(PricingParseError::ImplausiblyLarge);
        }
        let mut value = parse_i64_digits(digits).ok_or(PricingParseError::Overflow)?;
        let shift = usize::try_from(shift).map_err(|_| PricingParseError::ImplausiblyLarge)?;
        for _ in 0..shift {
            value = value.checked_mul(10).ok_or(PricingParseError::Overflow)?;
        }
        value
    } else {
        let drop = match shift
            .checked_neg()
            .and_then(|value| usize::try_from(value).ok())
        {
            Some(value) => value,
            None => return Ok(NanoUsdPerMillion::ZERO),
        };
        if drop > digits.len() {
            return Ok(NanoUsdPerMillion::ZERO);
        }
        let keep = digits.len() - drop;
        let mut value = parse_i64_digits(&digits[..keep]).ok_or(PricingParseError::Overflow)?;
        if keep < digits.len() && digits[keep] >= b'5' {
            value = value.checked_add(1).ok_or(PricingParseError::Overflow)?;
        }
        value
    };
    if nanos > 1_000_000_i64 * NANO_USD_PER_USD as i64 {
        return Err(PricingParseError::ImplausiblyLarge);
    }
    Ok(NanoUsdPerMillion(nanos))
}
