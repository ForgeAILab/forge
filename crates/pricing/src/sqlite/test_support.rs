//! SQLite pricing fixtures and settled usage helpers.

use super::*;
use crate::{parse_models_dev_catalog, EventTokenCounts};
use db::{
    create_sqlite_pool, run_migrations, CreatePricingSubject, CreatePricingSubjectRevision,
    PricingSubjectKind, PricingSubjectRepo, PricingSubjectState, ProjectRepo, User, UserRepo,
};
use std::time::Duration;

pub async fn database() -> Arc<db::SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    Arc::new(db::SqliteDb::new(pool))
}

pub fn at(seconds: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)
}

pub fn catalog_body() -> Vec<u8> {
    br#"{
          "openai": {
            "id": "openai",
            "name": "OpenAI",
            "models": {
              "gpt-test": {
                "id": "gpt-test",
                "last_updated": "2026-09-01",
                "cost": {
                  "input": 1,
                  "output": 2,
                  "cache_read": 0
                }
              }
            }
          }
        }"#
    .to_vec()
}

pub fn snapshot(id: &str, when: SystemTime) -> CatalogSnapshot {
    parse_models_dev_catalog(&catalog_body())
        .expect("fixture parses")
        .into_snapshot(id, Some("etag-1".to_owned()), when, when)
        .expect("snapshot materializes")
}

pub async fn subject_fixture(db: &db::SqliteDb) -> (String, String, String) {
    let now = db_time(at(100));
    UserRepo::create_user(
        db,
        &User {
            id: "pricing-owner".to_owned(),
            email: "pricing-owner@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: Some("Pricing owner".to_owned()),
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("user creates");
    let subject_id = "pricing-subject".to_owned();
    PricingSubjectRepo::create_pricing_subject(
        db,
        CreatePricingSubject {
            id: subject_id.clone(),
            owner_user_id: "pricing-owner".to_owned(),
            subject_kind: PricingSubjectKind::ProviderEntry,
            provider_entry_id: Some("provider-entry".to_owned()),
            daemon_id: None,
            executor_type: None,
            current_revision_id: None,
            state: PricingSubjectState::Active,
            last_idempotency_key: None,
            last_update_digest: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("subject creates");
    let revision_id = "pricing-subject-revision".to_owned();
    let revision_digest =
        pricing::pricing_subject_revision_digest(&pricing::PricingSubjectIdentity {
            subject_kind: "provider_entry".to_owned(),
            provider_kind: "openai".to_owned(),
            credential_method: "api_key".to_owned(),
            endpoint_class: "https://api.openai.com/v1".to_owned(),
            runtime_fingerprint: None,
            schema_revision: "pricing-subject-v1".to_owned(),
        });
    PricingSubjectRepo::create_pricing_subject_revision(
        db,
        CreatePricingSubjectRevision {
            id: revision_id.clone(),
            subject_id: subject_id.clone(),
            owner_user_id: "pricing-owner".to_owned(),
            revision: 1,
            revision_digest: revision_digest.clone(),
            subject_kind: PricingSubjectKind::ProviderEntry,
            provider_entry_id: Some("provider-entry".to_owned()),
            daemon_id: None,
            executor_type: None,
            provider_kind: "openai".to_owned(),
            credential_method: "api_key".to_owned(),
            endpoint_class: "https://api.openai.com/v1".to_owned(),
            runtime_fingerprint: None,
            schema_revision: "pricing-subject-v1".to_owned(),
            non_secret_identity_json: "{}".to_owned(),
            created_at: now.clone(),
        },
    )
    .await
    .expect("subject revision creates");
    PricingSubjectRepo::update_pricing_subject(
        db,
        db::UpdatePricingSubject {
            id: subject_id.clone(),
            expected_version: 1,
            current_revision_id: Some(Some(revision_id.clone())),
            state: None,
            last_idempotency_key: None,
            last_update_digest: None,
            updated_at: now,
        },
    )
    .await
    .expect("subject points at revision");
    (subject_id, revision_id, revision_digest)
}

pub async fn project_fixture(db: &db::SqliteDb) -> String {
    let now = db_time(at(100));
    let project_id = "pricing-project".to_owned();
    ProjectRepo::create(
        db,
        db::CreateProject {
            id: project_id.clone(),
            name: "Pricing project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some("pricing-owner".to_owned()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("project creates");
    project_id
}

pub const FIXTURE_AGENT_ID: &str = "pricing-agent";

pub async fn usage_event_fixture(
    db: &db::SqliteDb,
    project_id: &str,
    suffix: &str,
    counters: Option<EventTokenCounts>,
    provider_reported_nano_usd: Option<i64>,
) -> String {
    let now = db_time(at(100));
    let source_id = format!("execution-{suffix}");
    let execution_id = format!("execution-row-{suffix}");
    let selection_id = format!("pricing-selection-{suffix}");
    db::UsageLedgerRepo::create_pricing_selection(
        db,
        db::CreatePricingSelection {
            id: selection_id.clone(),
            owner_user_id: Some("pricing-owner".to_owned()),
            project_id: Some(project_id.to_owned()),
            domain_kind: db::PricingDomainKind::Execution,
            surface: db::UsageSurface::TaskExecution,
            source_id: source_id.clone(),
            execution_id: Some(execution_id.clone()),
            task_id: None,
            candidate_key: Some("candidate-0".to_owned()),
            attempt_ordinal: 0,
            subject_id: None,
            subject_revision_id: None,
            subject_revision_digest: None,
            binding_id: None,
            rate_revision_id: None,
            catalog_snapshot_id: None,
            catalog_freshness: None,
            runtime_model: Some("gpt-test".to_owned()),
            admitted_provider_id: Some("openai".to_owned()),
            admitted_model_id: Some("gpt-test".to_owned()),
            source_kind: None,
            provenance_kind: db::PricingAdmissionProvenanceKind::Runtime,
            selection_status: db::PricingSelectionStatus::Unpriced,
            selection_reason: None,
            selection_digest: format!("selection-digest-{suffix}"),
            selected_at: now.clone(),
            created_at: now.clone(),
        },
    )
    .await
    .expect("selection creates");
    let invocation_id = format!("pricing-invocation-{suffix}");
    db::UsageLedgerRepo::create_usage_invocation(
        db,
        db::CreateUsageInvocation {
            id: invocation_id.clone(),
            owner_user_id: Some("pricing-owner".to_owned()),
            project_id: Some(project_id.to_owned()),
            domain_kind: db::PricingDomainKind::Execution,
            surface: db::UsageSurface::TaskExecution,
            source_id: source_id.clone(),
            execution_id: Some(execution_id.clone()),
            task_id: None,
            domain_idempotency_key: format!("invocation-key-{suffix}"),
            candidate_key: Some("candidate-0".to_owned()),
            attempt_ordinal: 0,
            pricing_selection_id: selection_id,
            admitted_provider_id: Some("openai".to_owned()),
            admitted_model_id: Some("gpt-test".to_owned()),
            admitted_runtime_model: Some("gpt-test".to_owned()),
            pricing_subject_id: None,
            pricing_subject_revision_id: None,
            subject_revision_digest: None,
            agent_id: Some(FIXTURE_AGENT_ID.to_owned()),
            profile_id: None,
            agent_name_snapshot: None,
            project_name_snapshot: Some("Pricing project".to_owned()),
            executor_type: Some("test".to_owned()),
            backend_kind: Some("fixture".to_owned()),
            provenance_kind: db::PricingAdmissionProvenanceKind::Runtime,
            admitted_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("invocation creates");
    db::UsageLedgerRepo::start_usage_invocation(
        db,
        db::StartUsageInvocation {
            id: invocation_id.clone(),
            expected_version: 1,
            started_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("invocation starts");

    let (telemetry_state, report_mode, cost_kind, reason) = if provider_reported_nano_usd.is_some()
    {
        (
            db::UsageTelemetryState::Unmetered,
            db::UsageEventReportMode::ReportedMoney,
            db::UsageCostKind::ProviderReported,
            None,
        )
    } else {
        (
            db::UsageTelemetryState::Metered,
            db::UsageEventReportMode::FinalSnapshot,
            db::UsageCostKind::None,
            Some(db::CostCoverageReasonCode::MissingBinding),
        )
    };
    let (input_tokens, output_tokens, cache_read_tokens, cache_write_tokens) =
        counters.map_or((None, None, None, None), |counts| {
            (
                Some(i64::try_from(counts.input).expect("input fits")),
                Some(i64::try_from(counts.output).expect("output fits")),
                Some(i64::try_from(counts.cache_read).expect("cache read fits")),
                Some(i64::try_from(counts.cache_write).expect("cache write fits")),
            )
        });
    let event_id = format!("pricing-event-{suffix}");
    db::UsageLedgerRepo::settle_usage_invocations_with_events(
        db,
        vec![db::UsageLedgerSettlement {
            invocation_id: invocation_id.clone(),
            expected_version: 2,
            telemetry_state,
            terminal_reason: Some("fixture complete".to_owned()),
            settled_at: now.clone(),
            updated_at: now.clone(),
            events: vec![db::CreateUsageEvent {
                id: event_id.clone(),
                invocation_id,
                owner_user_id: Some("pricing-owner".to_owned()),
                project_id: Some(project_id.to_owned()),
                surface: db::UsageSurface::TaskExecution,
                source_id,
                execution_id: Some(execution_id),
                task_id: None,
                event_idempotency_key: format!("event-key-{suffix}"),
                source_report_id: format!("report-{suffix}"),
                report_sequence: 0,
                report_mode,
                provenance_kind: db::UsageEventProvenanceKind::RuntimeReport,
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
                provider_id: Some("openai".to_owned()),
                model_id: Some("gpt-test".to_owned()),
                runtime_model: Some("gpt-test".to_owned()),
                candidate_key: Some("candidate-0".to_owned()),
                attempt_ordinal: 0,
                agent_id: Some(FIXTURE_AGENT_ID.to_owned()),
                profile_id: None,
                agent_name_snapshot: None,
                project_name_snapshot: Some("Pricing project".to_owned()),
                executor_type: Some("test".to_owned()),
                pricing_subject_revision_id: None,
                subject_revision_digest: None,
                telemetry_state,
                input_tokens,
                output_tokens,
                cache_read_tokens,
                cache_write_tokens,
                context_tokens: None,
                selected_tier: None,
                provider_reported_nano_usd,
                legacy_reported_cost_usd: None,
                estimated_nano_usd: None,
                cost_kind,
                rate_revision_id: None,
                catalog_snapshot_id: None,
                formula_revision: None,
                retrospective: false,
                coverage_reason_code: reason,
                occurred_at: now.clone(),
                created_at: now,
            }],
        }],
    )
    .await
    .expect("invocation settles");
    event_id
}
