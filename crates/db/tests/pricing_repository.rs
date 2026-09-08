use db::{
    create_sqlite_pool, now_rfc3339, run_migrations, ActivatePricingCatalog,
    CostCoverageReasonCode, CreatePricingCatalogSnapshot, CreatePricingRateRevision,
    CreatePricingSelection, CreatePricingSubject, CreatePricingSubjectBinding,
    CreatePricingSubjectRevision, CreateUsageEvent, CreateUsageInvocation, NanoUsdPerMillion,
    PageRequest, PricingAdmissionProvenanceKind, PricingCatalogModelQuery, PricingCatalogRepo,
    PricingCatalogSourceKind, PricingCatalogStateKind, PricingDomainKind, PricingRateSourceKind,
    PricingSelectionStatus, PricingSubjectKind, PricingSubjectRepo, PricingSubjectState,
    RateBuckets, SettleUsageInvocation, SortBy, SortOrder, SqliteDb, StartUsageInvocation,
    TokenCounters, UsageEventProvenanceKind, UsageEventReportMode, UsageInvocationLifecycle,
    UsageLedgerRepo, UsageSurface, UsageTelemetryState,
};

async fn db() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations apply");
    SqliteDb::new(pool)
}

async fn seed_owner(db: &SqliteDb, id: &str) {
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
         VALUES (?, ?, 'test', 'Pricing owner', ?, ?)",
    )
    .bind(id)
    .bind(format!("{id}@example.test"))
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("owner inserts");
}

async fn seed_project(db: &SqliteDb, id: &str, owner_id: &str) {
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO project (id, name, settings, workflow_definition, owner_id, created_at, updated_at)
         VALUES (?, 'Pricing project', '{}', '{}', ?, ?, ?)",
    )
    .bind(id)
    .bind(owner_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("project inserts");
}

fn page(limit: i64) -> PageRequest {
    PageRequest {
        cursor: None,
        limit,
        include_total: true,
        sort_by: SortBy::CreatedAt,
        sort_order: SortOrder::Asc,
    }
}

fn selection_fixture(
    id: &str,
    source_kind: Option<PricingRateSourceKind>,
    catalog_snapshot_id: Option<&str>,
    catalog_freshness: Option<&str>,
) -> CreatePricingSelection {
    CreatePricingSelection {
        id: id.to_owned(),
        owner_user_id: Some("owner-1".to_owned()),
        project_id: Some("project-1".to_owned()),
        domain_kind: PricingDomainKind::Execution,
        surface: UsageSurface::TaskExecution,
        source_id: format!("execution-{id}"),
        execution_id: Some(format!("execution-{id}")),
        task_id: Some(format!("task-{id}")),
        candidate_key: Some(format!("candidate-{id}")),
        attempt_ordinal: 0,
        subject_id: None,
        subject_revision_id: None,
        subject_revision_digest: None,
        binding_id: None,
        rate_revision_id: None,
        catalog_snapshot_id: catalog_snapshot_id.map(str::to_owned),
        catalog_freshness: catalog_freshness.map(str::to_owned),
        runtime_model: Some("custom-model".to_owned()),
        admitted_provider_id: Some("custom".to_owned()),
        admitted_model_id: Some("custom-model".to_owned()),
        source_kind,
        provenance_kind: PricingAdmissionProvenanceKind::Runtime,
        selection_status: PricingSelectionStatus::Unpriced,
        selection_reason: None,
        selection_digest: format!("selection-digest-{id}"),
        selected_at: "2026-09-07T00:00:00Z".to_owned(),
        created_at: "2026-09-07T00:00:00Z".to_owned(),
    }
}

async fn window_invocation(db: &SqliteDb) -> String {
    let selection = UsageLedgerRepo::create_pricing_selection(
        db,
        selection_fixture("window-selection", None, None, None),
    )
    .await
    .expect("window selection creates");
    let invocation = UsageLedgerRepo::create_usage_invocation(
        db,
        CreateUsageInvocation {
            id: "window-invocation".to_owned(),
            owner_user_id: Some("owner-1".to_owned()),
            project_id: Some("project-1".to_owned()),
            domain_kind: PricingDomainKind::Execution,
            surface: UsageSurface::TaskExecution,
            source_id: "execution-window-selection".to_owned(),
            execution_id: Some("execution-window-selection".to_owned()),
            task_id: Some("task-window-selection".to_owned()),
            domain_idempotency_key: "window-invocation-key".to_owned(),
            candidate_key: Some("candidate-window-selection".to_owned()),
            attempt_ordinal: 0,
            pricing_selection_id: selection.id,
            admitted_provider_id: Some("custom".to_owned()),
            admitted_model_id: Some("custom-model".to_owned()),
            admitted_runtime_model: Some("custom-model".to_owned()),
            pricing_subject_id: None,
            pricing_subject_revision_id: None,
            subject_revision_digest: None,
            agent_id: None,
            profile_id: None,
            agent_name_snapshot: None,
            project_name_snapshot: None,
            executor_type: Some("test".to_owned()),
            backend_kind: Some("test".to_owned()),
            provenance_kind: PricingAdmissionProvenanceKind::Runtime,
            admitted_at: "2026-01-01T00:00:00Z".to_owned(),
            created_at: "2026-01-01T00:00:00Z".to_owned(),
            updated_at: "2026-01-01T00:00:00Z".to_owned(),
        },
    )
    .await
    .expect("window invocation creates");
    let started = UsageLedgerRepo::start_usage_invocation(
        db,
        StartUsageInvocation {
            id: invocation.id.clone(),
            expected_version: invocation.version,
            started_at: "2026-01-01T00:00:00Z".to_owned(),
            updated_at: "2026-01-01T00:00:00Z".to_owned(),
        },
    )
    .await
    .expect("window invocation starts");
    let settled = UsageLedgerRepo::settle_usage_invocation(
        db,
        SettleUsageInvocation {
            id: started.id,
            expected_version: started.version,
            telemetry_state: UsageTelemetryState::Metered,
            terminal_reason: None,
            settled_at: "2026-01-01T00:00:01Z".to_owned(),
            updated_at: "2026-01-01T00:00:01Z".to_owned(),
        },
    )
    .await
    .expect("window invocation settles");
    settled.id
}

fn window_event(
    invocation_id: &str,
    id: &str,
    report_sequence: i64,
    occurred_at: &str,
) -> CreateUsageEvent {
    CreateUsageEvent {
        id: id.to_owned(),
        invocation_id: invocation_id.to_owned(),
        owner_user_id: Some("owner-1".to_owned()),
        project_id: Some("project-1".to_owned()),
        surface: UsageSurface::TaskExecution,
        source_id: "execution-window-selection".to_owned(),
        execution_id: Some("execution-window-selection".to_owned()),
        task_id: Some("task-window-selection".to_owned()),
        event_idempotency_key: format!("{id}-key"),
        source_report_id: format!("{id}-report"),
        report_sequence,
        report_mode: UsageEventReportMode::Delta,
        provenance_kind: UsageEventProvenanceKind::RuntimeReport,
        legacy_source_table: None,
        legacy_source_id: None,
        legacy_provider_raw: None,
        legacy_provider_sqlite_type: None,
        legacy_provider_sql_literal: None,
        legacy_model_raw: None,
        legacy_model_sqlite_type: None,
        legacy_model_sql_literal: None,
        legacy_counter_values_json: "{}".to_owned(),
        legacy_cost_usd_raw: None,
        legacy_created_at_raw: None,
        legacy_project_owner_raw: None,
        legacy_invalid_usage: false,
        provider_id: Some("custom".to_owned()),
        model_id: Some("custom-model".to_owned()),
        runtime_model: Some("custom-model".to_owned()),
        candidate_key: Some("candidate-window-selection".to_owned()),
        attempt_ordinal: 0,
        agent_id: None,
        profile_id: None,
        agent_name_snapshot: None,
        project_name_snapshot: None,
        executor_type: Some("test".to_owned()),
        pricing_subject_revision_id: None,
        subject_revision_digest: None,
        telemetry_state: UsageTelemetryState::Metered,
        input_tokens: Some(1),
        output_tokens: Some(1),
        cache_read_tokens: None,
        cache_write_tokens: None,
        context_tokens: None,
        selected_tier: None,
        provider_reported_nano_usd: None,
        legacy_reported_cost_usd: None,
        estimated_nano_usd: None,
        cost_kind: db::UsageCostKind::None,
        rate_revision_id: None,
        catalog_snapshot_id: None,
        formula_revision: None,
        retrospective: false,
        coverage_reason_code: Some(CostCoverageReasonCode::MissingBinding),
        occurred_at: occurred_at.to_owned(),
        created_at: "2026-01-01T00:00:00Z".to_owned(),
    }
}

#[tokio::test]
async fn catalog_snapshot_rates_and_state_activate_atomically() {
    let db = db().await;
    let snapshot = PricingCatalogRepo::create_pricing_catalog_snapshot(
        &db,
        CreatePricingCatalogSnapshot {
            id: "snapshot-1".to_owned(),
            source_kind: PricingCatalogSourceKind::ModelsDevCatalog,
            source_url: "https://models.dev/api.json".to_owned(),
            http_etag: Some("etag-1".to_owned()),
            payload_sha256: "a".repeat(64),
            parser_revision: "models-dev-api-v1".to_owned(),
            revision_digest: "revision-1".to_owned(),
            payload_json: "{}".to_owned(),
            fetched_at: "2026-09-07T00:00:00Z".to_owned(),
            created_at: "2026-09-07T00:00:00Z".to_owned(),
        },
    )
    .await
    .expect("snapshot creates");
    assert_eq!(snapshot.id, "snapshot-1");

    let rate = PricingCatalogRepo::create_pricing_rate_revision(
        &db,
        CreatePricingRateRevision {
            id: "rate-1".to_owned(),
            source_kind: PricingRateSourceKind::ModelsDevCatalog,
            owner_user_id: None,
            catalog_snapshot_id: Some(snapshot.id.clone()),
            catalog_provider_id: Some("openai".to_owned()),
            catalog_model_id: Some("gpt-test".to_owned()),
            pricing_subject_revision_id: None,
            pricing_subject_revision_digest: None,
            runtime_model: None,
            source_model_key: Some("openai:gpt-test".to_owned()),
            source_last_updated: None,
            currency: "USD".to_owned(),
            rates: RateBuckets::new(Some(1_000_000_000), Some(2_000_000_000), Some(0), None),
            tiers_json: "[]".to_owned(),
            legacy_context_over_200k_json: None,
            context_tier_state: "none".to_owned(),
            received_rates_json: "{}".to_owned(),
            rate_digest: "rate-digest-1".to_owned(),
            effective_at: "2026-09-07T00:00:00Z".to_owned(),
            created_at: "2026-09-07T00:00:00Z".to_owned(),
        },
    )
    .await
    .expect("catalog rate creates");
    assert_eq!(rate.rates.input, Some(1_000_000_000));

    let initial = PricingCatalogRepo::ensure_pricing_catalog_state(&db, "2026-09-07T00:00:00Z")
        .await
        .expect("catalog state initializes");
    assert_eq!(initial.state, PricingCatalogStateKind::Absent);
    let active = PricingCatalogRepo::activate_pricing_catalog(
        &db,
        ActivatePricingCatalog {
            snapshot_id: snapshot.id,
            expected_version: initial.version,
            http_etag: Some("etag-1".to_owned()),
            checked_at: "2026-09-07T00:00:01Z".to_owned(),
            stale_after: Some("2026-09-14T00:00:01Z".to_owned()),
            idempotency_key: Some("refresh-1".to_owned()),
            updated_at: "2026-09-07T00:00:01Z".to_owned(),
        },
    )
    .await
    .expect("catalog activates");
    assert_eq!(active.state, PricingCatalogStateKind::Fresh);
    assert_eq!(active.active_snapshot_id.as_deref(), Some("snapshot-1"));

    let models = PricingCatalogRepo::list_pricing_catalog_models(
        &db,
        PricingCatalogModelQuery {
            page: page(10),
            provider_id: Some("openai".to_owned()),
            query: Some("gpt".to_owned()),
            snapshot_id: None,
        },
    )
    .await
    .expect("catalog models list");
    assert_eq!(models.total_count, Some(1));
    assert_eq!(models.items[0].model_id, "gpt-test");
}

#[tokio::test]
async fn subject_revision_binding_uses_optimistic_versions() {
    let db = db().await;
    seed_owner(&db, "owner-1").await;
    let subject = PricingSubjectRepo::create_pricing_subject(
        &db,
        CreatePricingSubject {
            id: "subject-1".to_owned(),
            owner_user_id: "owner-1".to_owned(),
            subject_kind: PricingSubjectKind::ProviderEntry,
            provider_entry_id: Some("provider-entry-1".to_owned()),
            daemon_id: None,
            executor_type: None,
            current_revision_id: None,
            state: PricingSubjectState::Active,
            last_idempotency_key: Some("same-subject-request-key".to_owned()),
            last_update_digest: None,
            created_at: "2026-09-07T00:00:00Z".to_owned(),
            updated_at: "2026-09-07T00:00:00Z".to_owned(),
        },
    )
    .await
    .expect("subject creates");
    let revision = PricingSubjectRepo::create_pricing_subject_revision(
        &db,
        CreatePricingSubjectRevision {
            id: "subject-revision-1".to_owned(),
            subject_id: subject.id.clone(),
            owner_user_id: "owner-1".to_owned(),
            revision: 1,
            revision_digest: "subject-digest-1".to_owned(),
            subject_kind: PricingSubjectKind::ProviderEntry,
            provider_entry_id: Some("provider-entry-1".to_owned()),
            daemon_id: None,
            executor_type: None,
            provider_kind: "openai".to_owned(),
            credential_method: "api_key".to_owned(),
            endpoint_class: "https://api.openai.com/v1".to_owned(),
            runtime_fingerprint: None,
            schema_revision: "pricing-subject-v1".to_owned(),
            non_secret_identity_json: "{}".to_owned(),
            created_at: "2026-09-07T00:00:00Z".to_owned(),
        },
    )
    .await
    .expect("subject revision creates");
    let second_subject = PricingSubjectRepo::create_pricing_subject(
        &db,
        CreatePricingSubject {
            id: "subject-2".to_owned(),
            owner_user_id: "owner-1".to_owned(),
            subject_kind: PricingSubjectKind::ProviderEntry,
            provider_entry_id: Some("provider-entry-2".to_owned()),
            daemon_id: None,
            executor_type: None,
            current_revision_id: None,
            state: PricingSubjectState::Active,
            last_idempotency_key: Some("same-subject-request-key".to_owned()),
            last_update_digest: None,
            created_at: "2026-09-07T00:00:00Z".to_owned(),
            updated_at: "2026-09-07T00:00:00Z".to_owned(),
        },
    )
    .await
    .expect("second subject creates");
    let second_revision = PricingSubjectRepo::create_pricing_subject_revision(
        &db,
        CreatePricingSubjectRevision {
            id: "subject-revision-2".to_owned(),
            subject_id: second_subject.id,
            owner_user_id: "owner-1".to_owned(),
            revision: 1,
            // The digest intentionally excludes subject identity, so the
            // same canonical revision may be used by a different subject.
            revision_digest: revision.revision_digest.clone(),
            subject_kind: PricingSubjectKind::ProviderEntry,
            provider_entry_id: Some("provider-entry-2".to_owned()),
            daemon_id: None,
            executor_type: None,
            provider_kind: "openai".to_owned(),
            credential_method: "api_key".to_owned(),
            endpoint_class: "https://api.openai.com/v1".to_owned(),
            runtime_fingerprint: None,
            schema_revision: "pricing-subject-v1".to_owned(),
            non_secret_identity_json: "{}".to_owned(),
            created_at: "2026-09-07T00:00:00Z".to_owned(),
        },
    )
    .await
    .expect("same revision digest is scoped per subject");
    assert_eq!(second_revision.revision_digest, revision.revision_digest);
    let selected = PricingSubjectRepo::update_pricing_subject(
        &db,
        db::UpdatePricingSubject {
            id: subject.id.clone(),
            expected_version: subject.version,
            current_revision_id: Some(Some(revision.id.clone())),
            state: None,
            last_idempotency_key: None,
            last_update_digest: None,
            updated_at: "2026-09-07T00:00:01Z".to_owned(),
        },
    )
    .await
    .expect("subject revision selected");
    assert_eq!(selected.version, 2);

    let rate = PricingCatalogRepo::create_pricing_rate_revision(
        &db,
        CreatePricingRateRevision {
            id: "manual-rate-1".to_owned(),
            source_kind: PricingRateSourceKind::ManualOverride,
            owner_user_id: Some("owner-1".to_owned()),
            catalog_snapshot_id: None,
            catalog_provider_id: None,
            catalog_model_id: None,
            pricing_subject_revision_id: Some(revision.id.clone()),
            pricing_subject_revision_digest: Some(revision.revision_digest.clone()),
            runtime_model: Some("custom-model".to_owned()),
            source_model_key: Some("custom-model".to_owned()),
            source_last_updated: None,
            currency: "USD".to_owned(),
            rates: RateBuckets::new(Some(10), Some(20), None, None),
            tiers_json: "[]".to_owned(),
            legacy_context_over_200k_json: None,
            context_tier_state: "none".to_owned(),
            received_rates_json: "{}".to_owned(),
            rate_digest: "manual-rate-digest-1".to_owned(),
            effective_at: "2026-09-07T00:00:01Z".to_owned(),
            created_at: "2026-09-07T00:00:01Z".to_owned(),
        },
    )
    .await
    .expect("manual rate creates");
    let oversized = PricingCatalogRepo::create_pricing_rate_revision(
        &db,
        CreatePricingRateRevision {
            id: "manual-rate-too-large".to_owned(),
            source_kind: PricingRateSourceKind::ManualOverride,
            owner_user_id: Some("owner-1".to_owned()),
            catalog_snapshot_id: None,
            catalog_provider_id: None,
            catalog_model_id: None,
            pricing_subject_revision_id: Some(revision.id.clone()),
            pricing_subject_revision_digest: Some(revision.revision_digest.clone()),
            runtime_model: Some("custom-model".to_owned()),
            source_model_key: None,
            source_last_updated: None,
            currency: "USD".to_owned(),
            rates: RateBuckets::new(Some(1_000_000_000_000_001), None, None, None),
            tiers_json: "[]".to_owned(),
            legacy_context_over_200k_json: None,
            context_tier_state: "none".to_owned(),
            received_rates_json: "{}".to_owned(),
            rate_digest: "manual-rate-digest-too-large".to_owned(),
            effective_at: "2026-09-07T00:00:01Z".to_owned(),
            created_at: "2026-09-07T00:00:01Z".to_owned(),
        },
    )
    .await;
    assert!(oversized.is_err(), "manual rate cap must be enforced by DB");
    let binding = PricingSubjectRepo::create_pricing_subject_binding(
        &db,
        CreatePricingSubjectBinding {
            id: "binding-1".to_owned(),
            owner_user_id: "owner-1".to_owned(),
            subject_id: subject.id.clone(),
            subject_revision_id: revision.id,
            subject_revision_digest: "subject-digest-1".to_owned(),
            runtime_model: "custom-model".to_owned(),
            source_kind: PricingRateSourceKind::ManualOverride,
            catalog_provider_id: None,
            catalog_model_id: None,
            rate_revision_id: rate.id,
            binding_digest: "binding-digest-1".to_owned(),
            effective_at: "2026-09-07T00:00:01Z".to_owned(),
            created_at: "2026-09-07T00:00:01Z".to_owned(),
            updated_at: "2026-09-07T00:00:01Z".to_owned(),
        },
    )
    .await
    .expect("binding creates");
    assert_eq!(binding.version, 1);
    let stale = PricingSubjectRepo::retire_pricing_subject_binding(
        &db,
        db::RetirePricingSubjectBinding {
            id: binding.id,
            expected_version: 0,
            retired_at: "2026-09-07T00:00:02Z".to_owned(),
            updated_at: "2026-09-07T00:00:02Z".to_owned(),
        },
    )
    .await;
    assert!(matches!(stale, Err(db::DbError::VersionConflict)));
}

#[tokio::test]
async fn invocation_settlement_and_event_replay_are_idempotent() {
    let db = db().await;
    seed_owner(&db, "owner-1").await;
    seed_project(&db, "project-1", "owner-1").await;
    let selection = UsageLedgerRepo::create_pricing_selection(
        &db,
        CreatePricingSelection {
            id: "selection-1".to_owned(),
            owner_user_id: Some("owner-1".to_owned()),
            project_id: Some("project-1".to_owned()),
            domain_kind: PricingDomainKind::Execution,
            surface: UsageSurface::TaskExecution,
            source_id: "execution-1".to_owned(),
            execution_id: Some("execution-1".to_owned()),
            task_id: Some("task-1".to_owned()),
            candidate_key: Some("candidate-1".to_owned()),
            attempt_ordinal: 0,
            subject_id: None,
            subject_revision_id: None,
            subject_revision_digest: None,
            binding_id: None,
            rate_revision_id: None,
            catalog_snapshot_id: None,
            catalog_freshness: None,
            runtime_model: Some("custom-model".to_owned()),
            admitted_provider_id: Some("custom".to_owned()),
            admitted_model_id: Some("custom-model".to_owned()),
            source_kind: None,
            provenance_kind: PricingAdmissionProvenanceKind::Runtime,
            selection_status: PricingSelectionStatus::Unpriced,
            selection_reason: None,
            selection_digest: "selection-digest-1".to_owned(),
            selected_at: "2026-09-07T00:00:00Z".to_owned(),
            created_at: "2026-09-07T00:00:00Z".to_owned(),
        },
    )
    .await
    .expect("selection creates");
    let invocation = UsageLedgerRepo::create_usage_invocation(
        &db,
        CreateUsageInvocation {
            id: "invocation-1".to_owned(),
            owner_user_id: Some("owner-1".to_owned()),
            project_id: Some("project-1".to_owned()),
            domain_kind: PricingDomainKind::Execution,
            surface: UsageSurface::TaskExecution,
            source_id: "execution-1".to_owned(),
            execution_id: Some("execution-1".to_owned()),
            task_id: Some("task-1".to_owned()),
            domain_idempotency_key: "execution-1:candidate-1:0".to_owned(),
            candidate_key: Some("candidate-1".to_owned()),
            attempt_ordinal: 0,
            pricing_selection_id: selection.id,
            admitted_provider_id: Some("custom".to_owned()),
            admitted_model_id: Some("custom-model".to_owned()),
            admitted_runtime_model: Some("custom-model".to_owned()),
            pricing_subject_id: None,
            pricing_subject_revision_id: None,
            subject_revision_digest: None,
            agent_id: None,
            profile_id: None,
            agent_name_snapshot: None,
            project_name_snapshot: None,
            executor_type: Some("test".to_owned()),
            backend_kind: Some("test".to_owned()),
            provenance_kind: PricingAdmissionProvenanceKind::Runtime,
            admitted_at: "2026-09-07T00:00:00Z".to_owned(),
            created_at: "2026-09-07T00:00:00Z".to_owned(),
            updated_at: "2026-09-07T00:00:00Z".to_owned(),
        },
    )
    .await
    .expect("invocation creates");
    assert_eq!(invocation.lifecycle, UsageInvocationLifecycle::Admitted);
    let started = UsageLedgerRepo::start_usage_invocation(
        &db,
        StartUsageInvocation {
            id: invocation.id.clone(),
            expected_version: invocation.version,
            started_at: "2026-09-07T00:00:01Z".to_owned(),
            updated_at: "2026-09-07T00:00:01Z".to_owned(),
        },
    )
    .await
    .expect("invocation starts");
    let settled = UsageLedgerRepo::settle_usage_invocation(
        &db,
        SettleUsageInvocation {
            id: invocation.id.clone(),
            expected_version: started.version,
            telemetry_state: UsageTelemetryState::Metered,
            terminal_reason: None,
            settled_at: "2026-09-07T00:00:02Z".to_owned(),
            updated_at: "2026-09-07T00:00:02Z".to_owned(),
        },
    )
    .await
    .expect("invocation settles");
    let event = CreateUsageEvent {
        id: "event-1".to_owned(),
        invocation_id: settled.id.clone(),
        owner_user_id: Some("owner-1".to_owned()),
        project_id: Some("project-1".to_owned()),
        surface: UsageSurface::TaskExecution,
        source_id: "execution-1".to_owned(),
        execution_id: Some("execution-1".to_owned()),
        task_id: Some("task-1".to_owned()),
        event_idempotency_key: "event-key-1".to_owned(),
        source_report_id: "report-1".to_owned(),
        report_sequence: 0,
        report_mode: UsageEventReportMode::FinalSnapshot,
        provenance_kind: UsageEventProvenanceKind::RuntimeReport,
        legacy_source_table: None,
        legacy_source_id: None,
        legacy_provider_raw: None,
        legacy_provider_sqlite_type: None,
        legacy_provider_sql_literal: None,
        legacy_model_raw: None,
        legacy_model_sqlite_type: None,
        legacy_model_sql_literal: None,
        legacy_counter_values_json: "{}".to_owned(),
        legacy_cost_usd_raw: None,
        legacy_created_at_raw: None,
        legacy_project_owner_raw: None,
        legacy_invalid_usage: false,
        provider_id: Some("custom".to_owned()),
        model_id: Some("custom-model".to_owned()),
        runtime_model: Some("custom-model".to_owned()),
        candidate_key: Some("candidate-1".to_owned()),
        attempt_ordinal: 0,
        agent_id: None,
        profile_id: None,
        agent_name_snapshot: None,
        project_name_snapshot: None,
        executor_type: Some("test".to_owned()),
        pricing_subject_revision_id: None,
        subject_revision_digest: None,
        telemetry_state: UsageTelemetryState::Metered,
        input_tokens: Some(1),
        output_tokens: Some(2),
        cache_read_tokens: Some(0),
        cache_write_tokens: Some(0),
        context_tokens: None,
        selected_tier: None,
        provider_reported_nano_usd: None,
        legacy_reported_cost_usd: None,
        estimated_nano_usd: None,
        cost_kind: db::UsageCostKind::None,
        rate_revision_id: None,
        catalog_snapshot_id: None,
        formula_revision: None,
        retrospective: false,
        coverage_reason_code: Some(CostCoverageReasonCode::MissingBinding),
        occurred_at: "2026-09-07T00:00:02Z".to_owned(),
        created_at: "2026-09-07T00:00:02Z".to_owned(),
    };
    let stored = UsageLedgerRepo::append_usage_event(&db, event.clone())
        .await
        .expect("event appends");
    let replay = UsageLedgerRepo::append_usage_event(&db, event.clone())
        .await
        .expect("identical event replays");
    assert_eq!(stored.id, replay.id);
    let mut conflicting = event.clone();
    conflicting.input_tokens = Some(3);
    let conflict = UsageLedgerRepo::append_usage_event(&db, conflicting).await;
    assert!(matches!(conflict, Err(db::DbError::IdempotencyConflict)));

    let mut snapshot_conflict = event.clone();
    snapshot_conflict.project_name_snapshot = Some("different project".to_owned());
    let conflict = UsageLedgerRepo::append_usage_event(&db, snapshot_conflict).await;
    assert!(matches!(conflict, Err(db::DbError::IdempotencyConflict)));

    let mut legacy_cost_conflict = event;
    legacy_cost_conflict.legacy_reported_cost_usd = Some(1.0);
    let conflict = UsageLedgerRepo::append_usage_event(&db, legacy_cost_conflict).await;
    assert!(matches!(conflict, Err(db::DbError::IdempotencyConflict)));
}

#[tokio::test]
async fn usage_event_windows_compare_rfc3339_instants_at_nanosecond_boundaries() {
    let db = db().await;
    seed_owner(&db, "owner-1").await;
    seed_project(&db, "project-1", "owner-1").await;
    let invocation_id = window_invocation(&db).await;

    for (id, sequence, occurred_at) in [
        ("event-before", 0, "2025-12-31T23:59:59.999999999Z"),
        ("event-equivalent", 1, "2026-01-01T05:30:00+05:30"),
        ("event-at-to", 2, "2026-01-01T05:30:00.000000001+05:30"),
        ("event-after", 3, "2026-01-01T00:00:00.000000002Z"),
    ] {
        UsageLedgerRepo::append_usage_event(
            &db,
            window_event(&invocation_id, id, sequence, occurred_at),
        )
        .await
        .expect("window event appends");
    }

    let events = UsageLedgerRepo::list_usage_events_for_project(
        &db,
        "project-1",
        Some("2025-12-31T23:59:59.999999998Z"),
        Some("2026-01-01T00:00:00.000000001Z"),
    )
    .await
    .expect("window events list");
    assert_eq!(
        events
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>(),
        vec!["event-before", "event-equivalent"]
    );

    let open_ended = UsageLedgerRepo::list_usage_events_for_project(
        &db,
        "project-1",
        None,
        Some("2026-01-01T00:00:00.000000001Z"),
    )
    .await
    .expect("open-ended window events list");
    assert_eq!(
        open_ended
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>(),
        vec!["event-before", "event-equivalent"]
    );
}

#[tokio::test]
async fn usage_event_window_rejects_invalid_legacy_timestamp() {
    let db = db().await;
    seed_owner(&db, "owner-1").await;
    seed_project(&db, "project-1", "owner-1").await;
    let invocation_id = window_invocation(&db).await;
    UsageLedgerRepo::append_usage_event(
        &db,
        window_event(
            &invocation_id,
            "event-invalid-timestamp",
            0,
            "legacy-not-an-rfc3339-timestamp",
        ),
    )
    .await
    .expect("window event appends");

    assert!(matches!(
        UsageLedgerRepo::list_usage_events_for_project(
            &db,
            "project-1",
            Some("2026-01-01T00:00:00Z"),
            Some("2026-01-02T00:00:00Z"),
        )
        .await,
        Err(db::DbError::InvalidTransition)
    ));
    assert!(matches!(
        UsageLedgerRepo::list_usage_events_for_project(&db, "project-1", None, None).await,
        Err(db::DbError::InvalidTransition)
    ));
}

#[tokio::test]
async fn selection_candidate_collision_with_new_digest_is_an_idempotency_conflict() {
    let db = db().await;
    seed_owner(&db, "owner-1").await;
    seed_project(&db, "project-1", "owner-1").await;
    let selection = CreatePricingSelection {
        id: "selection-1".to_owned(),
        owner_user_id: Some("owner-1".to_owned()),
        project_id: Some("project-1".to_owned()),
        domain_kind: PricingDomainKind::Execution,
        surface: UsageSurface::TaskExecution,
        source_id: "execution-1".to_owned(),
        execution_id: Some("execution-1".to_owned()),
        task_id: Some("task-1".to_owned()),
        candidate_key: Some("candidate-1".to_owned()),
        attempt_ordinal: 0,
        subject_id: None,
        subject_revision_id: None,
        subject_revision_digest: None,
        binding_id: None,
        rate_revision_id: None,
        catalog_snapshot_id: None,
        catalog_freshness: None,
        runtime_model: Some("custom-model".to_owned()),
        admitted_provider_id: Some("custom".to_owned()),
        admitted_model_id: Some("custom-model".to_owned()),
        source_kind: None,
        provenance_kind: PricingAdmissionProvenanceKind::Runtime,
        selection_status: PricingSelectionStatus::Unpriced,
        selection_reason: None,
        selection_digest: "selection-digest-1".to_owned(),
        selected_at: "2026-09-07T00:00:00Z".to_owned(),
        created_at: "2026-09-07T00:00:00Z".to_owned(),
    };
    UsageLedgerRepo::create_pricing_selection(&db, selection.clone())
        .await
        .expect("selection creates");
    let mut collision = selection;
    collision.id = "selection-2".to_owned();
    collision.selection_digest = "selection-digest-2".to_owned();
    let result = UsageLedgerRepo::create_pricing_selection(&db, collision).await;
    assert!(matches!(result, Err(db::DbError::IdempotencyConflict)));
}

#[tokio::test]
async fn selection_catalog_freshness_combinations_are_rejected() {
    let db = db().await;
    seed_owner(&db, "owner-1").await;
    seed_project(&db, "project-1", "owner-1").await;
    PricingCatalogRepo::create_pricing_catalog_snapshot(
        &db,
        CreatePricingCatalogSnapshot {
            id: "selection-snapshot".to_owned(),
            source_kind: PricingCatalogSourceKind::ModelsDevCatalog,
            source_url: "https://models.dev/api.json".to_owned(),
            http_etag: None,
            payload_sha256: "a".repeat(64),
            parser_revision: "models-dev-api-v1".to_owned(),
            revision_digest: "selection-snapshot-revision".to_owned(),
            payload_json: "{}".to_owned(),
            fetched_at: "2026-09-07T00:00:00Z".to_owned(),
            created_at: "2026-09-07T00:00:00Z".to_owned(),
        },
    )
    .await
    .expect("selection snapshot creates");

    for (id, source_kind, snapshot_id, freshness) in [
        (
            "catalog-missing-freshness",
            Some(PricingRateSourceKind::ModelsDevCatalog),
            Some("selection-snapshot"),
            None,
        ),
        (
            "manual-fresh",
            Some(PricingRateSourceKind::ManualOverride),
            None,
            Some("fresh"),
        ),
        (
            "source-null-snapshot",
            None,
            Some("selection-snapshot"),
            None,
        ),
        ("unpriced-freshness", None, None, Some("fresh")),
    ] {
        let result = UsageLedgerRepo::create_pricing_selection(
            &db,
            selection_fixture(id, source_kind, snapshot_id, freshness),
        )
        .await;
        assert!(
            result.is_err(),
            "inconsistent freshness combination unexpectedly inserted: {id}"
        );
    }
}

#[test]
fn fixed_point_boundaries_are_checked_and_non_binary() {
    let rate = NanoUsdPerMillion::parse_manual_usd_per_million("0.000000001")
        .expect("nine decimal places are representable");
    assert_eq!(rate.as_nano_usd_per_million(), 1);
    assert!(NanoUsdPerMillion::parse_manual_usd_per_million("0.0000000001").is_err());
    let estimate = db::calculate_event_cost(
        TokenCounters::new(Some(1_000_000), Some(0), Some(0), Some(0)),
        RateBuckets::new(
            Some(rate.as_nano_usd_per_million()),
            Some(0),
            Some(0),
            Some(0),
        ),
    )
    .expect("fixed point estimate");
    assert_eq!(estimate.amount().expect("complete").as_nano_usd(), 1);
    assert_eq!(
        NanoUsdPerMillion::parse_models_dev_json_number("0.0000000005")
            .expect("source decimal rounds at half")
            .as_nano_usd_per_million(),
        1
    );
    assert_eq!(
        NanoUsdPerMillion::parse_models_dev_json_number("5e-10")
            .expect("source exponent rounds at half")
            .as_nano_usd_per_million(),
        1
    );
    assert!(NanoUsdPerMillion::parse_models_dev_json_number("-1e-3").is_err());
}

#[tokio::test]
async fn identical_frozen_digests_are_scoped_per_execution() {
    let db = db().await;
    seed_owner(&db, "owner-1").await;
    seed_project(&db, "project-1", "owner-1").await;

    let mut first = selection_fixture(
        "digest-execution-one",
        Some(PricingRateSourceKind::ManualOverride),
        None,
        Some("not_applicable"),
    );
    first.selection_digest = "same-frozen-provenance".to_owned();
    let mut second = selection_fixture(
        "digest-execution-two",
        Some(PricingRateSourceKind::ManualOverride),
        None,
        Some("not_applicable"),
    );
    second.selection_digest = first.selection_digest.clone();

    let first_row = UsageLedgerRepo::create_pricing_selection(&db, first)
        .await
        .expect("first selection inserts");
    let second_row = UsageLedgerRepo::create_pricing_selection(&db, second)
        .await
        .expect("same digest may be reused by another execution");
    assert_ne!(first_row.id, second_row.id);
}
