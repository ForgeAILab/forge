//! Pricing admission, frozen selections, and usage-report settlement tests.
use super::*;
use db::{
    create_sqlite_pool, run_migrations, PricingAdmissionProvenanceKind, PricingDomainKind,
    UsageEventProvenanceKind,
};

fn selection(reason: Option<CostCoverageReasonCode>) -> PricingSelection {
    let mut selection = PricingSelection {
        id: "selection-1".to_owned(),
        owner_user_id: None,
        project_id: None,
        domain_kind: PricingDomainKind::Execution,
        surface: UsageSurface::TaskExecution,
        source_id: "execution-1".to_owned(),
        execution_id: Some("execution-1".to_owned()),
        task_id: Some("task-1".to_owned()),
        candidate_key: Some("candidate-1".to_owned()),
        attempt_ordinal: 0,
        invocation_id: Some("invocation-1".to_owned()),
        subject_id: None,
        subject_revision_id: None,
        subject_revision_digest: None,
        binding_id: None,
        rate_revision_id: None,
        catalog_snapshot_id: None,
        catalog_freshness: None,
        runtime_model: Some("model-1".to_owned()),
        admitted_provider_id: Some("provider-1".to_owned()),
        admitted_model_id: Some("model-1".to_owned()),
        source_kind: None,
        provenance_kind: PricingAdmissionProvenanceKind::Runtime,
        selection_status: PricingSelectionStatus::Unpriced,
        selection_reason: reason,
        selection_digest: String::new(),
        selected_at: "2026-09-08T00:00:00Z".to_owned(),
        created_at: "2026-09-08T00:00:00Z".to_owned(),
    };
    selection.selection_digest = frozen_selection(&selection, None, reason)
        .expect("test selection freezes")
        .selection_digest;
    selection
}

fn invocation() -> UsageInvocation {
    UsageInvocation {
        id: "invocation-1".to_owned(),
        owner_user_id: None,
        project_id: None,
        domain_kind: PricingDomainKind::Execution,
        surface: UsageSurface::TaskExecution,
        source_id: "execution-1".to_owned(),
        execution_id: Some("execution-1".to_owned()),
        task_id: Some("task-1".to_owned()),
        domain_idempotency_key: "task-provider-call:invocation-1".to_owned(),
        candidate_key: Some("candidate-1".to_owned()),
        attempt_ordinal: 0,
        pricing_selection_id: "selection-1".to_owned(),
        admitted_provider_id: Some("provider-1".to_owned()),
        admitted_model_id: Some("model-1".to_owned()),
        admitted_runtime_model: Some("model-1".to_owned()),
        pricing_subject_id: None,
        pricing_subject_revision_id: None,
        subject_revision_digest: None,
        agent_id: None,
        profile_id: None,
        agent_name_snapshot: None,
        project_name_snapshot: None,
        executor_type: Some("codex".to_owned()),
        backend_kind: None,
        provenance_kind: PricingAdmissionProvenanceKind::Runtime,
        lifecycle: UsageInvocationLifecycle::Started,
        telemetry_state: UsageTelemetryState::Pending,
        terminal_reason: None,
        version: 2,
        admitted_at: "2026-09-08T00:00:00Z".to_owned(),
        started_at: Some("2026-09-08T00:00:01Z".to_owned()),
        settled_at: None,
        created_at: "2026-09-08T00:00:00Z".to_owned(),
        updated_at: "2026-09-08T00:00:01Z".to_owned(),
    }
}

async fn test_db() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("test pool creates");
    run_migrations(&pool).await.expect("test migrations apply");
    SqliteDb::new(pool)
}

#[test]
fn reported_decimal_is_strict_and_converted_to_nano_usd() {
    assert_eq!(
        parse_reported_nano_usd("1").expect("whole USD parses"),
        1_000_000_000
    );
    assert_eq!(
        parse_reported_nano_usd("0.000000001").expect("nano USD parses"),
        1
    );
    assert!(parse_reported_nano_usd("1.").is_err());
    assert!(parse_reported_nano_usd("1e-9").is_err());
    assert!(parse_reported_nano_usd(" 1").is_err());
    assert!(parse_reported_nano_usd("0.0000000001").is_err());
}

fn priced_selection(rate_id: &str) -> PricingSelection {
    let mut selection = selection(None);
    selection.selection_status = PricingSelectionStatus::Priced;
    selection.selection_reason = None;
    selection.source_kind = Some(PricingRateSourceKind::ManualOverride);
    selection.rate_revision_id = Some(rate_id.to_owned());
    selection.subject_id = Some("subject-1".to_owned());
    selection.subject_revision_id = Some("subject-revision-1".to_owned());
    selection.subject_revision_digest = Some("subject-digest-1".to_owned());
    selection
}

fn rate_revision(id: &str, cache_write: Option<i64>) -> db::PricingRateRevision {
    db::PricingRateRevision {
        id: id.to_owned(),
        source_kind: PricingRateSourceKind::ManualOverride,
        owner_user_id: None,
        catalog_snapshot_id: None,
        catalog_provider_id: None,
        catalog_model_id: None,
        pricing_subject_revision_id: None,
        pricing_subject_revision_digest: None,
        runtime_model: Some("model-1".to_owned()),
        source_model_key: None,
        source_last_updated: None,
        currency: "USD".to_owned(),
        // 1.25 / 10.00 / 0.125 USD per million, in nano-USD per million.
        rates: db::RateBuckets::new(
            Some(1_250_000_000),
            Some(10_000_000_000),
            Some(125_000_000),
            cache_write,
        ),
        tiers_json: "[]".to_owned(),
        legacy_context_over_200k_json: None,
        context_tier_state: "exact".to_owned(),
        received_rates_json: "{}".to_owned(),
        rate_digest: format!("digest-{id}"),
        effective_at: "2026-09-08T00:00:00Z".to_owned(),
        created_at: "2026-09-08T00:00:00Z".to_owned(),
    }
}

/// A provider reports only the buckets it bills — OpenAI never emits a
/// cache-write counter — so a bucket the frozen rate leaves unpriced must
/// not veto the estimate. A bucket the rate *does* price still must.
#[tokio::test]
async fn omitted_bucket_blocks_the_estimate_only_when_the_rate_prices_it() {
    let invocation = invocation();
    let report = UsageReport::metered(
        "openai-shaped",
        executors::UsageCounters {
            input_tokens: Some(1_000_000),
            output_tokens: Some(1_000_000),
            cache_read_tokens: Some(1_000_000),
            cache_write_tokens: None,
        },
    );

    let unpriced_write = rate_revision("rate-free-write", None);
    let mut selection = priced_selection(&unpriced_write.id);
    selection.selection_digest = frozen_selection(&selection, Some(&unpriced_write), None)
        .expect("priced selection freezes")
        .selection_digest;
    let event = usage_event_for_report_with_rate(
        &invocation,
        &selection,
        &report,
        "2026-09-08T00:00:02Z",
        Some(&unpriced_write),
    )
    .await
    .expect("report converts")
    .expect("evidence is retained");
    assert_eq!(event.cost_kind, db::UsageCostKind::Estimated);
    // 1.25 + 10.00 + 0.125 USD over one million tokens per bucket.
    assert_eq!(event.estimated_nano_usd, Some(11_375_000_000));
    assert_eq!(event.cache_write_tokens, None);

    // models.dev lists GLM with `cache_write: 0`: a zero rate is priced
    // as free, so omitting that counter hides no spend either.
    let zero_write = rate_revision("rate-zero-write", Some(0));
    let mut selection = priced_selection(&zero_write.id);
    selection.selection_digest = frozen_selection(&selection, Some(&zero_write), None)
        .expect("priced selection freezes")
        .selection_digest;
    let event = usage_event_for_report_with_rate(
        &invocation,
        &selection,
        &report,
        "2026-09-08T00:00:02Z",
        Some(&zero_write),
    )
    .await
    .expect("report converts")
    .expect("evidence is retained");
    assert_eq!(event.cost_kind, db::UsageCostKind::Estimated);
    assert_eq!(event.estimated_nano_usd, Some(11_375_000_000));

    let priced_write = rate_revision("rate-priced-write", Some(1_250_000_000));
    let mut selection = priced_selection(&priced_write.id);
    selection.selection_digest = frozen_selection(&selection, Some(&priced_write), None)
        .expect("priced selection freezes")
        .selection_digest;
    let event = usage_event_for_report_with_rate(
        &invocation,
        &selection,
        &report,
        "2026-09-08T00:00:02Z",
        Some(&priced_write),
    )
    .await
    .expect("report converts")
    .expect("evidence is retained");
    assert_eq!(event.cost_kind, db::UsageCostKind::None);
    assert_eq!(
        event.coverage_reason_code,
        Some(CostCoverageReasonCode::Unmetered)
    );
}

/// gpt-5.6-sol-shaped pricing: a context tier from 272K. The request's
/// prompt size picks the band; without it the estimate cannot be made.
#[tokio::test]
async fn context_tier_is_selected_from_the_request_prompt_size() {
    let invocation = invocation();
    let mut rate = rate_revision("rate-tiered", Some(1_250_000_000));
    rate.tiers_json = r#"[{"input":2.5,"output":20,"cache_read":0.25,"cache_write":2.5,"tier":{"type":"context","size":272000}}]"#.to_owned();
    let mut selection = priced_selection(&rate.id);
    selection.selection_digest = frozen_selection(&selection, Some(&rate), None)
        .expect("tiered selection freezes")
        .selection_digest;
    let counters = executors::UsageCounters {
        input_tokens: Some(1_000_000),
        output_tokens: Some(1_000_000),
        cache_read_tokens: Some(0),
        cache_write_tokens: Some(0),
    };
    let settle = |context_tokens: Option<u64>| {
        let mut report = UsageReport::metered("tiered", counters.clone());
        report.context_tokens = context_tokens;
        let selection = selection.clone();
        let rate = rate.clone();
        let invocation = invocation.clone();
        async move {
            usage_event_for_report_with_rate(
                &invocation,
                &selection,
                &report,
                "2026-09-08T00:00:02Z",
                Some(&rate),
            )
            .await
            .expect("report converts")
            .expect("evidence is retained")
        }
    };

    let small = settle(Some(100_000)).await;
    assert_eq!(small.cost_kind, db::UsageCostKind::Estimated);
    // Base band: 1.25 + 10.00 USD.
    assert_eq!(small.estimated_nano_usd, Some(11_250_000_000));
    assert_eq!(small.selected_tier, None);

    let large = settle(Some(300_000)).await;
    assert_eq!(large.cost_kind, db::UsageCostKind::Estimated);
    // Tier band: 2.50 + 20.00 USD.
    assert_eq!(large.estimated_nano_usd, Some(22_500_000_000));
    assert_eq!(large.selected_tier.as_deref(), Some("context_272000"));

    let unknown = settle(None).await;
    assert_ne!(unknown.cost_kind, db::UsageCostKind::Estimated);
}

#[tokio::test]
async fn sparse_and_explicit_zero_usage_reports_remain_distinguishable() {
    let db = test_db().await;
    let invocation = invocation();
    let selection = selection(Some(CostCoverageReasonCode::MissingBinding));
    let sparse = UsageReport::metered(
        "sparse",
        executors::UsageCounters {
            input_tokens: Some(7),
            output_tokens: None,
            cache_read_tokens: None,
            cache_write_tokens: None,
        },
    );
    let sparse_event = usage_event_for_report(
        &db,
        &invocation,
        &selection,
        &sparse,
        "2026-09-08T00:00:02Z",
    )
    .await
    .expect("sparse report converts")
    .expect("sparse evidence is retained");
    assert_eq!(sparse_event.input_tokens, Some(7));
    assert_eq!(sparse_event.output_tokens, None);
    assert_eq!(sparse_event.cost_kind, db::UsageCostKind::None);
    assert_eq!(
        sparse_event.coverage_reason_code,
        Some(CostCoverageReasonCode::Unmetered)
    );

    let zero = UsageReport::metered("zero", executors::UsageCounters::explicit_zero());
    let zero_event =
        usage_event_for_report(&db, &invocation, &selection, &zero, "2026-09-08T00:00:02Z")
            .await
            .expect("zero report converts")
            .expect("explicit zero evidence is retained");
    assert_eq!(zero_event.input_tokens, Some(0));
    assert_eq!(zero_event.output_tokens, Some(0));
    assert_eq!(zero_event.cache_read_tokens, Some(0));
    assert_eq!(zero_event.cache_write_tokens, Some(0));
    assert_eq!(zero_event.telemetry_state, UsageTelemetryState::Metered);

    let reported_only = UsageReport {
        report_id: "money".to_owned(),
        reported_cost_usd: Some("0.000000001".to_owned()),
        ..UsageReport::default()
    };
    let money_event = usage_event_for_report(
        &db,
        &invocation,
        &selection,
        &reported_only,
        "2026-09-08T00:00:02Z",
    )
    .await
    .expect("reported money converts")
    .expect("reported money evidence is retained");
    assert_eq!(money_event.provider_reported_nano_usd, Some(1));
    assert_eq!(money_event.estimated_nano_usd, None);
    assert_eq!(money_event.cost_kind, db::UsageCostKind::ProviderReported);
    assert!(!money_event.source_report_id.contains("money"));

    let sentinel = UsageReport {
        report_id: "SENTINEL-provider-secret-prompt".to_owned(),
        reported_cost_usd: Some("0".to_owned()),
        ..UsageReport::default()
    };
    let sentinel_event = usage_event_for_report(
        &db,
        &invocation,
        &selection,
        &sentinel,
        "2026-09-08T00:00:02Z",
    )
    .await
    .expect("sentinel report converts")
    .expect("sentinel money evidence is retained");
    assert!(!sentinel_event
        .source_report_id
        .contains("SENTINEL-provider-secret-prompt"));

    let identity_sentinel = UsageReport {
        provider_id: Some("Bearer sk-provider-secret".to_owned()),
        model_id: Some("model=secret prompt".to_owned()),
        counters: executors::UsageCounters {
            input_tokens: Some(1),
            output_tokens: Some(0),
            cache_read_tokens: Some(0),
            cache_write_tokens: Some(0),
        },
        telemetry_state: executors::UsageTelemetryState::Metered,
        ..UsageReport::default()
    };
    let identity_event = usage_event_for_report(
        &db,
        &invocation,
        &selection,
        &identity_sentinel,
        "2026-09-08T00:00:02Z",
    )
    .await
    .expect("identity sentinel converts")
    .expect("identity sentinel evidence is retained");
    assert_eq!(
        identity_event.coverage_reason_code,
        Some(CostCoverageReasonCode::IdentityMismatch)
    );
    assert_eq!(identity_event.cost_kind, db::UsageCostKind::None);
    assert_eq!(identity_event.provider_id.as_deref(), Some("provider-1"));
    assert_eq!(identity_event.model_id.as_deref(), Some("model-1"));

    let reported_mismatch = UsageReport {
        provider_id: Some("actual-provider".to_owned()),
        model_id: Some("actual/model-v2".to_owned()),
        reported_cost_usd: Some("0.25".to_owned()),
        ..UsageReport::default()
    };
    let reported_mismatch_event = usage_event_for_report(
        &db,
        &invocation,
        &selection,
        &reported_mismatch,
        "2026-09-08T00:00:02Z",
    )
    .await
    .expect("reported mismatch converts")
    .expect("reported mismatch evidence is retained");
    assert_eq!(
        reported_mismatch_event.provider_id.as_deref(),
        Some("actual-provider")
    );
    assert_eq!(
        reported_mismatch_event.model_id.as_deref(),
        Some("actual/model-v2")
    );
    assert_eq!(
        reported_mismatch_event.cost_kind,
        db::UsageCostKind::ProviderReported
    );
    assert_eq!(
        reported_mismatch_event.provider_reported_nano_usd,
        Some(250_000_000)
    );
    assert_eq!(reported_mismatch_event.estimated_nano_usd, None);

    let empty = UsageReport::unmetered("empty");
    assert!(
        usage_event_for_report(&db, &invocation, &selection, &empty, "2026-09-08T00:00:02Z",)
            .await
            .expect("empty report converts")
            .is_none()
    );
}

/// A Project and Agent without an owner are an anomaly, not a reason to
/// skip admission: the execution is still admitted, priced from the plain
/// models.dev catalogue row, and its usage report settles.
#[tokio::test]
async fn ownerless_execution_is_admitted_at_the_catalogue_price_and_settles() {
    use pricing::PricingCatalogRepository as _;

    let db = Arc::new(test_db().await);
    let now = db::now_rfc3339();
    // The first registered account is the instance administrator.
    db::UserRepo::create_user(
        &*db,
        &db::User {
            id: "instance-admin".to_owned(),
            email: "admin@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: None,
            is_admin: true,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("account creates");
    let when = std::time::SystemTime::now();
    let snapshot = pricing::parse_models_dev_catalog(
        br#"{"openai": {"id":"openai","name":"OpenAI","models":{
                "gpt-ownerless":{"id":"gpt-ownerless","last_updated":"2026-09-01",
                  "cost":{"input":1,"output":2}}}}}"#,
    )
    .expect("catalogue parses")
    .into_snapshot("ownerless-snapshot", None, when, when)
    .expect("snapshot materializes");
    pricing::sqlite::SqlitePricingRepository::new(Arc::clone(&db))
        .activate_catalog_snapshot(snapshot, "ownerless-refresh")
        .await
        .expect("catalogue activates");

    let project = db::ProjectRepo::create(
        &*db,
        db::CreateProject {
            id: db::new_uuid_v4(),
            name: "Ownerless".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("ownerless project creates");
    let agent = db::AgentRepo::create(
        &*db,
        db::CreateAgent {
            id: db::new_uuid_v4(),
            name: "Ownerless worker".to_owned(),
            description: None,
            executor_type: "codex".to_owned(),
            model: Some("gpt-ownerless".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: db::AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("ownerless agent creates");
    let task = db::TaskRepo::create(
        &*db,
        db::CreateTask {
            id: db::new_uuid_v4(),
            project_id: project.id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Ownerless work".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "in_progress".to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task creates");
    let snapshot = serde_json::json!({
        "executor_type": "codex",
        "config": {"model": "gpt-ownerless"},
        "daemon_id": "ownerless-daemon",
    });
    let execution = db::ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: db::new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent.id.clone()),
            role: "coder".to_owned(),
            status: db::ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(snapshot.to_string()),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("execution creates");

    assert!(
        ensure_task_execution_admission(&db, &task, &execution, &snapshot)
            .await
            .expect("ownerless admission succeeds")
    );
    let selections = db::UsageLedgerRepo::list_pricing_selections_for_source(&*db, &execution.id)
        .await
        .expect("selections load");
    assert_eq!(selections.len(), 1);
    let selection = &selections[0];
    assert_eq!(selection.owner_user_id.as_deref(), Some("instance-admin"));
    assert_eq!(selection.selection_status, PricingSelectionStatus::Priced);
    assert_eq!(
        selection.source_kind,
        Some(PricingRateSourceKind::ModelsDevCatalog)
    );
    assert_eq!(
        selection.catalog_snapshot_id.as_deref(),
        Some("ownerless-snapshot")
    );
    let rate: (Option<String>, Option<String>, Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT catalog_provider_id, owner_user_id, input_nano_usd_per_million,
                    output_nano_usd_per_million
             FROM pricing_rate_revision WHERE id = ?",
    )
    .bind(selection.rate_revision_id.as_deref().expect("frozen rate"))
    .fetch_one(db.pool())
    .await
    .expect("rate loads");
    // The global catalogue row, not an account's manual rate.
    assert_eq!(
        rate,
        (
            Some("openai".to_owned()),
            None,
            Some(1_000_000_000),
            Some(2_000_000_000)
        )
    );

    // The owner's terminal usage report finds its admitted candidate.
    let mut report = UsageReport::metered(
        "ownerless-report",
        executors::UsageCounters {
            input_tokens: Some(1_000_000),
            output_tokens: Some(1_000_000),
            cache_read_tokens: None,
            cache_write_tokens: None,
        },
    );
    report.candidate_key = selection.candidate_key.clone();
    let reports = [report];
    let mut tx = db::begin_immediate(db.pool()).await.expect("transaction");
    let prepared = ensure_remote_task_usage_invocations_in_tx(
        &db,
        &mut tx,
        &execution,
        &reports,
        Some(&snapshot),
        &now,
    )
    .await
    .expect("the report matches the admitted selection");
    assert_eq!(prepared.len(), 1);
    for input in prepared {
        let invocation = db::UsageLedgerRepo::create_usage_invocation_in_tx(&*db, &mut tx, input)
            .await
            .expect("invocation creates");
        db::UsageLedgerRepo::start_usage_invocation_in_tx(
            &*db,
            &mut tx,
            db::StartUsageInvocation {
                id: invocation.id,
                expected_version: invocation.version,
                started_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("invocation starts");
    }
    tx.commit().await.expect("invocation commits");
    settle_late_task_usage(&db, &execution.id, &reports, &now)
        .await
        .expect("usage settles");

    let invocations = db::UsageLedgerRepo::list_usage_invocations_for_source(&*db, &execution.id)
        .await
        .expect("invocations load");
    assert_eq!(invocations.len(), 1);
    assert_eq!(invocations[0].lifecycle, UsageInvocationLifecycle::Settled);
    let events = db::UsageLedgerRepo::list_usage_events_for_invocation(&*db, &invocations[0].id)
        .await
        .expect("events load");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].owner_user_id.as_deref(), Some("instance-admin"));
    assert_eq!(events[0].cost_kind, db::UsageCostKind::Estimated);
    // One million tokens each at 1 and 2 USD per million.
    assert_eq!(events[0].estimated_nano_usd, Some(3_000_000_000));
}

#[test]
fn admission_reason_survives_frozen_selection_reconstruction() {
    let selection = selection(Some(CostCoverageReasonCode::IdentityMismatch));
    let frozen = frozen_selection(&selection, None, None).expect("selection freezes");
    assert_eq!(
        frozen.reason,
        Some(pricing::PriceSelectionReasonCode::IdentityMismatch)
    );
    assert_eq!(
        frozen.catalog_freshness,
        pricing::CatalogFreshness::NotApplicable
    );
}

#[test]
fn usage_event_provenance_keeps_runtime_report_identity() {
    let selection = selection(Some(CostCoverageReasonCode::IdentityMismatch));
    assert_eq!(
        selection.provenance_kind,
        PricingAdmissionProvenanceKind::Runtime
    );
    assert_eq!(
        UsageEventProvenanceKind::RuntimeReport.to_string(),
        "runtime_report"
    );
}
