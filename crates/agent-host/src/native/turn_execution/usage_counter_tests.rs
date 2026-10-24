//! Per-attempt usage buckets and per-turn ledger slicing tests.

use super::*;
use agent_runtime::core::usage::UsageDelta;

#[test]
fn a_metered_attempt_reports_every_bucket_with_absent_ones_as_zero() {
    // Gemini with no cache hit: the adapter omits the zero cached bucket.
    let no_cache_hit = UsageDelta::new()
        .with(CounterKind::InputUncached, 3_847)
        .with(CounterKind::Output, 400)
        .with(CounterKind::Reasoning, 63);
    assert_eq!(
        record_counters(&no_cache_hit).expect("counters fit"),
        Some([3_847, 463, 0, 0])
    );

    // A full cache hit omits the zero uncached bucket instead.
    let full_hit = UsageDelta::new()
        .with(CounterKind::InputCached, 57_025)
        .with(CounterKind::Output, 12);
    assert_eq!(
        record_counters(&full_hit).expect("counters fit"),
        Some([0, 12, 57_025, 0])
    );

    assert_eq!(record_counters(&UsageDelta::new()).expect("empty"), None);
}

#[test]
fn prompt_size_is_the_three_input_buckets_of_one_attempt() {
    let report = AgentTurnUsageReport {
        report_id: "r".to_owned(),
        request_id: None,
        attempt_id: None,
        provider_id: None,
        model_id: None,
        input_tokens: Some(3_847),
        output_tokens: Some(463),
        cache_read_tokens: Some(270_000),
        cache_write_tokens: Some(0),
        telemetry_state: AgentTurnTelemetryState::Metered,
        failed: false,
    };
    assert_eq!(report.prompt_tokens(), Some(273_847));
    let unmetered = AgentTurnUsageReport {
        telemetry_state: AgentTurnTelemetryState::Unmetered,
        ..report
    };
    assert_eq!(unmetered.prompt_tokens(), None);
}

#[test]
fn a_turn_owns_only_the_records_appended_after_it_started() {
    let record = |input| agent_runtime::core::usage::UsageRecord {
        source: UsageSource::ProviderAttempt,
        provenance: Default::default(),
        delta: UsageDelta::new().with(CounterKind::InputUncached, input),
    };
    // Two earlier turns' calls restored with the session, then this one's.
    let ledger = [record(5_162), record(5_679), record(6_381)];
    let inputs = |baseline| {
        turn_usage_records(&ledger, baseline)
            .iter()
            .map(|record| record.delta.get(CounterKind::InputUncached))
            .collect::<Vec<_>>()
    };
    assert_eq!(inputs(2), [6_381]);
    assert_eq!(inputs(0), [5_162, 5_679, 6_381]);
    // A turn that reached no provider reports nothing, not the session.
    assert!(inputs(3).is_empty());
    assert!(inputs(4).is_empty());
}
