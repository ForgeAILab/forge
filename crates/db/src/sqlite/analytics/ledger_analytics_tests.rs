//! Ledger cost coverage and usage aggregation checks.

use super::*;

fn run_key(source_id: &str) -> RunKey {
    RunKey {
        surface: UsageSurface::TaskExecution,
        source_id: source_id.to_owned(),
    }
}

fn attempt(costed: bool, pending: bool, unsettled: bool) -> AttemptSummary {
    AttemptSummary {
        costed,
        metered: !pending && !unsettled,
        pending,
        unsettled,
        reasons: Vec::new(),
    }
}

fn invocation(
    lifecycle: UsageInvocationLifecycle,
    telemetry_state: UsageTelemetryState,
    events: Vec<LedgerEventRow>,
) -> LedgerInvocationView {
    LedgerInvocationView {
        row: LedgerInvocationRow {
            id: "invocation".to_owned(),
            project_id: Some("project".to_owned()),
            surface: UsageSurface::TaskExecution,
            source_id: "run".to_owned(),
            admitted_provider_id: Some("provider".to_owned()),
            admitted_model_id: Some("model".to_owned()),
            agent_id: None,
            profile_id: None,
            agent_name_snapshot: None,
            project_name_snapshot: None,
            executor_type: None,
            lifecycle,
            telemetry_state,
            admitted_at: "2026-09-08T00:00:00Z".to_owned(),
            settled_at: Some("2026-09-08T00:00:01Z".to_owned()),
        },
        events,
    }
}

fn reported_event(counters: [Option<i64>; 4]) -> LedgerEventRow {
    LedgerEventRow {
        invocation_id: "invocation".to_owned(),
        project_id: Some("project".to_owned()),
        provider_id: Some("provider".to_owned()),
        model_id: Some("model".to_owned()),
        runtime_model: None,
        agent_id: None,
        profile_id: None,
        agent_name_snapshot: None,
        project_name_snapshot: None,
        executor_type: None,
        counters,
        provider_reported_nano_usd: Some(0),
        legacy_reported_cost_text: None,
        estimated_nano_usd: None,
        cost_kind: UsageCostKind::ProviderReported,
        provenance_kind: UsageEventProvenanceKind::RuntimeReport,
        rate_revision_id: None,
        catalog_snapshot_id: None,
        formula_revision: None,
        retrospective: false,
        coverage_reason_code: None,
        occurred_at: "2026-09-08T00:00:01Z".to_owned(),
        rate_source_kind: None,
        rate_effective_at: None,
        rate_input: None,
        rate_output: None,
        rate_cache_read: None,
        rate_cache_write: None,
        catalog_digest: None,
        catalog_fetched_at: None,
        catalog_freshness: None,
    }
}

#[test]
fn decimal_money_is_fixed_point_and_preserves_explicit_zero() {
    assert_eq!(parse_money_decimal("0"), Some(0));
    assert_eq!(parse_money_decimal("0.000000001"), Some(1));
    assert_eq!(parse_money_decimal("1.2345678915"), Some(1_234_567_892));
    assert_eq!(parse_money_decimal("'0.25'"), Some(250_000_000));
    assert_eq!(parse_money_decimal("NULL"), None);
    assert_eq!(
        money(Some(0)),
        Some(MoneyAmount {
            currency: "USD".to_owned(),
            decimal: "0".to_owned(),
        })
    );
}

#[test]
fn timestamp_window_uses_exact_rfc3339_instants_at_subsecond_boundaries() {
    let from = "2026-09-08T00:00:00.000000001Z";
    let to = "2026-09-08T00:00:00.000000003Z";
    assert!(timestamp_in_window(
        "2026-09-08T00:00:00.000000001Z",
        Some(from),
        Some(to)
    ));
    assert!(timestamp_in_window(
        "2026-09-08T05:30:00.000000002+05:30",
        Some(from),
        Some(to)
    ));
    assert!(!timestamp_in_window(to, Some(from), Some(to)));
}

#[test]
fn coverage_is_no_usage_without_provider_attempts() {
    let summary = cost_summary(
        CostAccumulator::default(),
        &BTreeMap::from([(run_key("no-provider"), Vec::new())]),
    )
    .expect("coverage summary");
    assert_eq!(summary.kind, CostKind::None);
    assert_eq!(summary.coverage, CostCoverage::NoUsage);
    assert_eq!(summary.complete_total, None);
    assert_eq!(summary.usage_coverage.no_provider_call_runs_or_turns, 1);
}

#[test]
fn pending_precedes_partial_and_keeps_known_subtotal_separate() {
    let accumulator = CostAccumulator {
        provider_reported_nano_usd: Some(1_000_000_000),
        total_provider_attempts: 2,
        settled_provider_attempts: 1,
        pending_provider_attempts: 1,
        costed_provider_attempts: 1,
        ..CostAccumulator::default()
    };
    let summary = cost_summary(
        accumulator,
        &BTreeMap::from([(
            run_key("pending"),
            vec![attempt(true, false, false), attempt(false, true, false)],
        )]),
    )
    .expect("coverage summary");
    assert_eq!(summary.coverage, CostCoverage::Pending);
    assert_eq!(summary.kind, CostKind::ProviderReported);
    assert_eq!(summary.known_subtotal.unwrap().decimal, "1");
    assert_eq!(summary.complete_total, None);
    assert_eq!(summary.usage_coverage.pending_runs_or_turns, 1);
}

#[test]
fn terminal_unsettled_is_unavailable_without_another_costed_attempt() {
    let accumulator = CostAccumulator {
        total_provider_attempts: 1,
        unsettled_provider_attempts: 1,
        ..CostAccumulator::default()
    };
    let summary = cost_summary(
        accumulator,
        &BTreeMap::from([(run_key("unsettled"), vec![attempt(false, false, true)])]),
    )
    .expect("coverage summary");
    assert_eq!(summary.coverage, CostCoverage::Unavailable);
    assert_eq!(summary.kind, CostKind::Unknown);
    assert_eq!(summary.usage_coverage.unsettled_provider_attempts, 1);
}

#[test]
fn mixed_costed_and_unpriced_attempts_are_partial() {
    let accumulator = CostAccumulator {
        provider_reported_nano_usd: Some(2_000_000_000),
        total_provider_attempts: 2,
        settled_provider_attempts: 2,
        costed_provider_attempts: 1,
        unpriced_provider_attempts: 1,
        ..CostAccumulator::default()
    };
    let summary = cost_summary(
        accumulator,
        &BTreeMap::from([(
            run_key("partial"),
            vec![attempt(true, false, false), attempt(false, false, false)],
        )]),
    )
    .expect("coverage summary");
    assert_eq!(summary.coverage, CostCoverage::Partial);
    assert_eq!(summary.known_subtotal.unwrap().decimal, "2");
    assert_eq!(summary.complete_total, None);
    assert_eq!(summary.usage_coverage.partially_costed_runs_or_turns, 1);
}

#[test]
fn no_event_and_pending_attempts_stay_in_the_attempt_partition() {
    let no_event = invocation(
        UsageInvocationLifecycle::Settled,
        UsageTelemetryState::Unmetered,
        Vec::new(),
    );
    let no_event_key = run_key("no-event");
    let mut no_event_accumulator = CostAccumulator::default();
    let no_event_summary = attempt_summary(&no_event, &mut no_event_accumulator, &no_event_key)
        .expect("no-event attempt");
    assert!(!no_event_summary.costed);
    assert_eq!(no_event_accumulator.total_provider_attempts, 1);
    assert_eq!(no_event_accumulator.costed_provider_attempts, 0);
    assert_eq!(no_event_accumulator.unpriced_provider_attempts, 1);

    let pending = invocation(
        UsageInvocationLifecycle::PendingSettlement,
        UsageTelemetryState::Pending,
        Vec::new(),
    );
    let pending_key = run_key("pending-no-event");
    let mut pending_accumulator = CostAccumulator::default();
    let pending_summary =
        attempt_summary(&pending, &mut pending_accumulator, &pending_key).expect("pending attempt");
    assert!(!pending_summary.costed);
    assert_eq!(pending_accumulator.total_provider_attempts, 1);
    assert_eq!(pending_accumulator.costed_provider_attempts, 0);
    assert_eq!(pending_accumulator.unpriced_provider_attempts, 1);
    assert_eq!(pending_accumulator.pending_provider_attempts, 1);
}

#[test]
fn reported_money_without_counters_is_costed_but_unmetered() {
    let invocation = invocation(
        UsageInvocationLifecycle::Settled,
        UsageTelemetryState::Unmetered,
        vec![reported_event([None, None, None, None])],
    );
    let key = run_key("reported-only");
    let mut accumulator = CostAccumulator::default();
    let summary =
        attempt_summary(&invocation, &mut accumulator, &key).expect("reported-only attempt");
    assert!(summary.costed);
    assert_eq!(accumulator.total_provider_attempts, 1);
    assert_eq!(accumulator.costed_provider_attempts, 1);
    assert_eq!(accumulator.unpriced_provider_attempts, 0);
    assert_eq!(accumulator.unmetered_provider_attempts, 1);
    assert_eq!(accumulator.provider_reported_nano_usd, Some(0));
    assert_eq!(accumulator.tokens, [0; 4]);
}

#[test]
fn missing_catalog_freshness_fails_closed_in_estimate_provenance() {
    let mut event = reported_event([Some(1), None, None, None]);
    event.provider_reported_nano_usd = None;
    event.estimated_nano_usd = Some(1);
    event.cost_kind = UsageCostKind::Estimated;
    event.rate_source_kind = Some("models_dev_catalog".to_owned());
    event.rate_revision_id = Some("rate-revision".to_owned());
    let (_, source) = event_source(&event).expect("estimate source");
    assert_eq!(source.freshness, CostSourceFreshness::RefreshFailed);
}

#[test]
fn genesis_unmetered_invocation_keeps_immutable_project_attribution() {
    let project_id = "project-genesis".to_owned();
    let dataset = UsageDataset {
        invocations: vec![LedgerInvocationView {
            row: LedgerInvocationRow {
                id: "genesis-invocation".to_owned(),
                project_id: Some(project_id.clone()),
                surface: UsageSurface::GenesisChat,
                source_id: "genesis-source-turn".to_owned(),
                admitted_provider_id: Some("openai".to_owned()),
                admitted_model_id: Some("gpt-test".to_owned()),
                agent_id: Some("main-agent".to_owned()),
                profile_id: Some("main-profile".to_owned()),
                agent_name_snapshot: Some("Main".to_owned()),
                project_name_snapshot: Some("Genesis Project".to_owned()),
                executor_type: Some("native".to_owned()),
                lifecycle: UsageInvocationLifecycle::Settled,
                telemetry_state: UsageTelemetryState::Unmetered,
                admitted_at: "2026-09-08T00:00:00Z".to_owned(),
                settled_at: Some("2026-09-08T00:00:01Z".to_owned()),
            },
            events: Vec::new(),
        }],
        domain_runs: Vec::new(),
    };

    let analytics = account_usage_analytics(dataset, None, None)
        .expect("Genesis usage analytics should accept a no-event invocation");
    assert_eq!(analytics.token_usage.counts.chat_turn_count, 1);
    assert_eq!(analytics.token_usage.by_surface.len(), 1);
    let genesis_surface = analytics
        .token_usage
        .by_surface
        .iter()
        .find(|surface| surface.surface == ApiUsageSurface::GenesisChat)
        .expect("Genesis surface is present");
    assert_eq!(genesis_surface.counts.chat_turn_count, 1);
    assert_eq!(analytics.by_project.len(), 1);
    assert_eq!(
        analytics.by_project[0].project_id.as_deref(),
        Some(project_id.as_str())
    );
    assert_eq!(analytics.by_project[0].counts.chat_turn_count, 1);
    assert_eq!(analytics.by_project[0].counts.provider_attempt_count, 1);
}

#[test]
fn account_project_group_keeps_no_provider_domain_run() {
    let analytics = account_usage_analytics(
        UsageDataset {
            invocations: Vec::new(),
            domain_runs: vec![DomainRun {
                surface: UsageSurface::GenesisChat,
                source_id: "genesis-no-provider".to_owned(),
                project_id: Some("project-genesis".to_owned()),
            }],
        },
        None,
        None,
    )
    .expect("account analytics should retain domain-only Project activity");
    assert_eq!(analytics.by_project.len(), 1);
    assert_eq!(analytics.by_project[0].counts.chat_turn_count, 1);
    assert_eq!(analytics.by_project[0].counts.provider_attempt_count, 0);
    assert_eq!(analytics.by_project[0].cost.coverage, CostCoverage::NoUsage);
}

#[tokio::test]
async fn ledger_projection_queries_empty_project_and_account_scopes() {
    let pool = crate::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    crate::run_migrations(&pool).await.expect("migrations run");
    let db = SqliteDb::new(pool);
    let project_id = "missing-project";

    let project = UsageAnalyticsRepo::get_project_usage_analytics(&db, project_id, None, None)
        .await
        .expect("empty project projection");
    assert_eq!(project.cost.coverage, CostCoverage::NoUsage);
    assert_eq!(
        project.counts,
        ActivityCounts {
            task_execution_count: 0,
            chat_turn_count: 0,
            inquiry_count: 0,
            provider_attempt_count: 0,
        }
    );

    let account = UsageAnalyticsRepo::get_account_usage_analytics(&db, "missing-owner", None, None)
        .await
        .expect("empty account projection");
    assert_eq!(account.token_usage.cost.coverage, CostCoverage::NoUsage);
    assert!(account.by_project.is_empty());
}
