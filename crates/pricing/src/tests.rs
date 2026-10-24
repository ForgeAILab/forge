//! Fixed point parsing, rounding, and event cost checks.

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
    assert_eq!(COST_ROUNDING_REVISION, "cost-rounding-v1");
    assert_eq!(COST_FORMULA_REVISION, "token-cost-v1");

    assert_eq!(round_half_away_from_zero(4, 2), Some(2));
    assert_eq!(round_half_away_from_zero(5, 2), Some(3));
    assert_eq!(round_half_away_from_zero(-5, 2), Some(-3));
    assert_eq!(round_half_away_from_zero(5, -2), Some(-3));
    assert_eq!(round_half_away_from_zero(1, 0), None);
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
        assert!(!value.to_string().contains('e'));
        assert!(value
            .to_usd_decimal()
            .split('.')
            .nth(1)
            .is_none_or(|fraction| fraction.len() <= NANO_USD_SCALE as usize));
    }

    let rate = rate(1_250_000_000);
    assert_eq!(rate.to_string(), "1.25");
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
fn event_arithmetic_overflow_is_checked() {
    let max_rate = rate(i64::MAX);
    let result = calculate_four_bucket_cost([u64::MAX; 4], [Some(max_rate); 4]);
    assert_eq!(result, Err(EventCostError::Overflow));

    let result =
        calculate_four_bucket_cost([1_000_001, 0, 0, 0], [Some(max_rate), None, None, None]);
    assert_eq!(result, Err(EventCostError::Overflow));

    let maximum = amount(i64::MAX);
    assert_eq!(maximum.checked_add(amount(1)), None);
    assert_eq!(NanoUsd::from_nano_usd(-1), None);
    assert_eq!(NanoUsdPerMillion::from_nano_usd(-1), None);
}
