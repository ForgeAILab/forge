//! Catalog ingestion, refresh, selection, and retrospective checks.

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

    let parsed = parse_models_dev_catalog(
        br#"{"openai":{"id":"openai","models":{"m":{"id":"m","cost":{"input":1,"output":1}}}}}"#,
    )
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
fn admission_can_freeze_current_catalog_freshness_over_binding_metadata() {
    let selection = freeze_persisted_price_selection(
        Some("subject".to_owned()),
        Some("subject-rev-1".to_owned()),
        Some("gpt".to_owned()),
        Some("candidate".to_owned()),
        0,
        Some("openai".to_owned()),
        Some("openai".to_owned()),
        Some("gpt-5".to_owned()),
        Some(PricingSourceKind::ModelsDevCatalog),
        Some("rate-1".to_owned()),
        Some("snapshot-1".to_owned()),
        Some(EventBucketRates::new(
            Some(rate(1)),
            Some(rate(2)),
            None,
            None,
        )),
        Vec::new(),
        None,
        CatalogFreshness::Fresh,
        PriceSelectionStatus::Priced,
        None,
    );
    assert_eq!(selection.catalog_freshness, CatalogFreshness::Fresh);
    assert_eq!(selection.validate(), Ok(()));

    let historical = freeze_persisted_price_selection(
        Some("subject".to_owned()),
        Some("subject-rev-1".to_owned()),
        Some("gpt".to_owned()),
        Some("candidate".to_owned()),
        0,
        Some("openai".to_owned()),
        Some("openai".to_owned()),
        Some("gpt-5".to_owned()),
        Some(PricingSourceKind::ModelsDevCatalog),
        Some("rate-1".to_owned()),
        Some("snapshot-1".to_owned()),
        Some(EventBucketRates::new(
            Some(rate(1)),
            Some(rate(2)),
            None,
            None,
        )),
        Vec::new(),
        None,
        CatalogFreshness::Stale,
        PriceSelectionStatus::Priced,
        None,
    );
    assert_eq!(historical.catalog_freshness, CatalogFreshness::Stale);
}

#[test]
fn estimate_distinguishes_reported_unmetered_zero_missing_rate_tier_and_identity() {
    let selection = freeze_persisted_price_selection(
        Some("subject".to_owned()),
        Some("subject-rev-1".to_owned()),
        Some("gpt".to_owned()),
        Some("candidate-a".to_owned()),
        0,
        Some("openai".to_owned()),
        None,
        None,
        Some(PricingSourceKind::ManualOverride),
        Some("manual-rate-1".to_owned()),
        None,
        Some(EventBucketRates::new(
            Some(rate(1_000_000_000)),
            None,
            None,
            None,
        )),
        Vec::new(),
        None,
        CatalogFreshness::NotApplicable,
        PriceSelectionStatus::Priced,
        None,
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

    let free_selection = freeze_persisted_price_selection(
        Some("subject".to_owned()),
        Some("subject-rev-1".to_owned()),
        Some("gpt".to_owned()),
        Some("candidate-a".to_owned()),
        0,
        Some("openai".to_owned()),
        None,
        None,
        Some(PricingSourceKind::ManualOverride),
        Some("manual-free-rate".to_owned()),
        None,
        Some(EventBucketRates::new(
            Some(NanoUsdPerMillion::ZERO),
            Some(NanoUsdPerMillion::ZERO),
            None,
            None,
        )),
        Vec::new(),
        None,
        CatalogFreshness::NotApplicable,
        PriceSelectionStatus::Priced,
        None,
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
    let selection = freeze_persisted_price_selection(
        Some("subject".to_owned()),
        Some("subject-rev-1".to_owned()),
        Some("gpt".to_owned()),
        Some("candidate".to_owned()),
        0,
        None,
        Some("openai".to_owned()),
        Some("gpt-5".to_owned()),
        Some(PricingSourceKind::ModelsDevCatalog),
        Some("rate-1".to_owned()),
        Some("snapshot-1".to_owned()),
        Some(EventBucketRates::new(
            Some(rate(1_000_000_000)),
            None,
            None,
            None,
        )),
        Vec::new(),
        None,
        CatalogFreshness::Fresh,
        PriceSelectionStatus::Priced,
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
    let selection = freeze_persisted_price_selection(
        Some("subject".to_owned()),
        Some("subject-rev-1".to_owned()),
        Some("gpt".to_owned()),
        Some("candidate".to_owned()),
        0,
        None,
        None,
        None,
        Some(PricingSourceKind::ManualOverride),
        Some("manual-rate-1".to_owned()),
        None,
        Some(EventBucketRates::new(
            Some(rate(1)),
            Some(rate(2)),
            None,
            None,
        )),
        Vec::new(),
        None,
        CatalogFreshness::NotApplicable,
        PriceSelectionStatus::Priced,
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
    assert!(matches!(
        missing_priced_provenance.validate(),
        Err(FrozenPriceSelectionError::SemanticInvariantViolation(_))
    ));
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
    assert!(matches!(
        missing_unpriced_reason.validate(),
        Err(FrozenPriceSelectionError::SemanticInvariantViolation(_))
    ));

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
    assert!(matches!(
        manual_with_catalog_freshness.validate(),
        Err(FrozenPriceSelectionError::SemanticInvariantViolation(_))
    ));

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
    assert!(matches!(
        catalog_with_invalid_freshness.validate(),
        Err(FrozenPriceSelectionError::SemanticInvariantViolation(_))
    ));

    // A runtime provider label that differs from the catalog provider id
    // is the ordinary case, not a contradiction: Forge dispatches
    // `gemini` and models.dev prices the same model under `google`.
    // Whether the execution actually ran on the admitted provider is
    // settlement's question, answered against reported evidence.
    let catalog_provider_in_its_own_namespace = FrozenPriceSelection::from_parts(
        None,
        None,
        Some("gemini-3.8-flash".to_owned()),
        None,
        0,
        Some("gemini".to_owned()),
        Some("google".to_owned()),
        Some("gemini-3.8-flash".to_owned()),
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
    assert_eq!(catalog_provider_in_its_own_namespace.validate(), Ok(()));
    assert!(catalog_provider_in_its_own_namespace.is_priced());
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
