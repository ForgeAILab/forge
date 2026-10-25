//! Provider credential acquisition, rotation, revocation, and pricing tests.

use super::super::tests::test_store;
use super::*;
use db::{
    CreatePricingRateRevision, CreatePricingSelection, CreatePricingSubject,
    CreatePricingSubjectBinding, CreatePricingSubjectRevision, CreateUsageEvent,
    CreateUsageInvocation, PricingAdmissionProvenanceKind, PricingCatalogRepo, PricingDomainKind,
    PricingRateSourceKind, PricingSelectionStatus, PricingSubjectKind, PricingSubjectRepo,
    PricingSubjectState, RateBuckets, SettleUsageInvocation, StartUsageInvocation, UsageCostKind,
    UsageEventProvenanceKind, UsageEventReportMode, UsageLedgerRepo, UsageSurface,
    UsageTelemetryState,
};

async fn seed_provider_pricing_history(db: &SqliteDb, provider_entry_id: &str) {
    let subject = PricingSubjectRepo::create_pricing_subject(
        db,
        CreatePricingSubject {
            id: "disconnect-pricing-subject".to_owned(),
            owner_user_id: "credential-owner".to_owned(),
            subject_kind: PricingSubjectKind::ProviderEntry,
            provider_entry_id: Some(provider_entry_id.to_owned()),
            daemon_id: None,
            executor_type: None,
            current_revision_id: None,
            state: PricingSubjectState::Active,
            last_idempotency_key: None,
            last_update_digest: None,
            created_at: "2026-09-08T00:00:00Z".to_owned(),
            updated_at: "2026-09-08T00:00:00Z".to_owned(),
        },
    )
    .await
    .expect("pricing subject creates");
    let revision = PricingSubjectRepo::create_pricing_subject_revision(
        db,
        CreatePricingSubjectRevision {
            id: "disconnect-pricing-revision".to_owned(),
            subject_id: subject.id.clone(),
            owner_user_id: "credential-owner".to_owned(),
            revision: 1,
            revision_digest: "disconnect-pricing-revision-digest".to_owned(),
            subject_kind: PricingSubjectKind::ProviderEntry,
            provider_entry_id: Some(provider_entry_id.to_owned()),
            daemon_id: None,
            executor_type: None,
            provider_kind: "xai".to_owned(),
            credential_method: "oauth_bundle".to_owned(),
            endpoint_class: "https://api.x.ai/v1".to_owned(),
            runtime_fingerprint: None,
            schema_revision: "pricing-subject-v1".to_owned(),
            non_secret_identity_json: "{}".to_owned(),
            created_at: "2026-09-08T00:00:00Z".to_owned(),
        },
    )
    .await
    .expect("pricing subject revision creates");
    PricingSubjectRepo::update_pricing_subject(
        db,
        db::UpdatePricingSubject {
            id: subject.id.clone(),
            expected_version: subject.version,
            current_revision_id: Some(Some(revision.id.clone())),
            state: None,
            last_idempotency_key: None,
            last_update_digest: None,
            updated_at: "2026-09-08T00:00:01Z".to_owned(),
        },
    )
    .await
    .expect("pricing subject revision selects");
    let rate = PricingCatalogRepo::create_pricing_rate_revision(
        db,
        CreatePricingRateRevision {
            id: "disconnect-pricing-rate".to_owned(),
            source_kind: PricingRateSourceKind::ManualOverride,
            owner_user_id: Some("credential-owner".to_owned()),
            catalog_snapshot_id: None,
            catalog_provider_id: None,
            catalog_model_id: None,
            pricing_subject_revision_id: Some(revision.id.clone()),
            pricing_subject_revision_digest: Some(revision.revision_digest.clone()),
            runtime_model: Some("grok-test".to_owned()),
            source_model_key: Some("grok-test".to_owned()),
            source_last_updated: None,
            currency: "USD".to_owned(),
            rates: RateBuckets::new(Some(10), Some(20), None, None),
            tiers_json: "[]".to_owned(),
            legacy_context_over_200k_json: None,
            context_tier_state: "none".to_owned(),
            received_rates_json: "{}".to_owned(),
            rate_digest: "disconnect-pricing-rate-digest".to_owned(),
            effective_at: "2026-09-08T00:00:01Z".to_owned(),
            created_at: "2026-09-08T00:00:01Z".to_owned(),
        },
    )
    .await
    .expect("pricing rate creates");
    let binding = PricingSubjectRepo::create_pricing_subject_binding(
        db,
        CreatePricingSubjectBinding {
            id: "disconnect-pricing-binding".to_owned(),
            owner_user_id: "credential-owner".to_owned(),
            subject_id: subject.id.clone(),
            subject_revision_id: revision.id.clone(),
            subject_revision_digest: revision.revision_digest.clone(),
            scope_key: String::new(),
            runtime_model: "grok-test".to_owned(),
            source_kind: PricingRateSourceKind::ManualOverride,
            catalog_provider_id: None,
            catalog_model_id: None,
            rate_revision_id: rate.id.clone(),
            binding_digest: "disconnect-pricing-binding-digest".to_owned(),
            effective_at: "2026-09-08T00:00:01Z".to_owned(),
            created_at: "2026-09-08T00:00:01Z".to_owned(),
            updated_at: "2026-09-08T00:00:01Z".to_owned(),
        },
    )
    .await
    .expect("pricing binding creates");
    let selection = UsageLedgerRepo::create_pricing_selection(
        db,
        CreatePricingSelection {
            id: "disconnect-pricing-selection".to_owned(),
            owner_user_id: Some("credential-owner".to_owned()),
            project_id: None,
            domain_kind: PricingDomainKind::Chat,
            surface: UsageSurface::MainChat,
            source_id: "disconnect-chat".to_owned(),
            execution_id: None,
            task_id: None,
            candidate_key: Some("disconnect-candidate".to_owned()),
            attempt_ordinal: 0,
            subject_id: Some(subject.id.clone()),
            subject_revision_id: Some(revision.id.clone()),
            subject_revision_digest: Some(revision.revision_digest.clone()),
            binding_id: Some(binding.id.clone()),
            rate_revision_id: Some(rate.id.clone()),
            catalog_snapshot_id: None,
            catalog_freshness: Some("not_applicable".to_owned()),
            runtime_model: Some("grok-test".to_owned()),
            admitted_provider_id: Some("xai".to_owned()),
            admitted_model_id: Some("grok-test".to_owned()),
            source_kind: Some(PricingRateSourceKind::ManualOverride),
            provenance_kind: PricingAdmissionProvenanceKind::Runtime,
            selection_status: PricingSelectionStatus::Priced,
            selection_reason: None,
            selection_digest: "disconnect-pricing-selection-digest".to_owned(),
            selected_at: "2026-09-08T00:00:02Z".to_owned(),
            created_at: "2026-09-08T00:00:02Z".to_owned(),
        },
    )
    .await
    .expect("pricing selection creates");
    let invocation = UsageLedgerRepo::create_usage_invocation(
        db,
        CreateUsageInvocation {
            id: "disconnect-pricing-invocation".to_owned(),
            owner_user_id: Some("credential-owner".to_owned()),
            project_id: None,
            domain_kind: PricingDomainKind::Chat,
            surface: UsageSurface::MainChat,
            source_id: "disconnect-chat".to_owned(),
            execution_id: None,
            task_id: None,
            domain_idempotency_key: "disconnect-pricing-invocation-key".to_owned(),
            candidate_key: Some("disconnect-candidate".to_owned()),
            attempt_ordinal: 0,
            pricing_selection_id: selection.id,
            admitted_provider_id: Some("xai".to_owned()),
            admitted_model_id: Some("grok-test".to_owned()),
            admitted_runtime_model: Some("grok-test".to_owned()),
            pricing_subject_id: Some(subject.id),
            pricing_subject_revision_id: Some(revision.id.clone()),
            subject_revision_digest: Some(revision.revision_digest.clone()),
            agent_id: None,
            profile_id: None,
            agent_name_snapshot: None,
            project_name_snapshot: None,
            executor_type: Some("api".to_owned()),
            backend_kind: Some("provider".to_owned()),
            provenance_kind: PricingAdmissionProvenanceKind::Runtime,
            admitted_at: "2026-09-08T00:00:02Z".to_owned(),
            created_at: "2026-09-08T00:00:02Z".to_owned(),
            updated_at: "2026-09-08T00:00:02Z".to_owned(),
        },
    )
    .await
    .expect("usage invocation creates");
    let started = UsageLedgerRepo::start_usage_invocation(
        db,
        StartUsageInvocation {
            id: invocation.id.clone(),
            expected_version: invocation.version,
            started_at: "2026-09-08T00:00:03Z".to_owned(),
            updated_at: "2026-09-08T00:00:03Z".to_owned(),
        },
    )
    .await
    .expect("usage invocation starts");
    let settled = UsageLedgerRepo::settle_usage_invocation(
        db,
        SettleUsageInvocation {
            id: started.id,
            expected_version: started.version,
            telemetry_state: UsageTelemetryState::Metered,
            terminal_reason: None,
            settled_at: "2026-09-08T00:00:04Z".to_owned(),
            updated_at: "2026-09-08T00:00:04Z".to_owned(),
        },
    )
    .await
    .expect("usage invocation settles");
    UsageLedgerRepo::append_usage_event(
        db,
        CreateUsageEvent {
            id: "disconnect-pricing-event".to_owned(),
            invocation_id: settled.id,
            owner_user_id: Some("credential-owner".to_owned()),
            project_id: None,
            surface: UsageSurface::MainChat,
            source_id: "disconnect-chat".to_owned(),
            execution_id: None,
            task_id: None,
            event_idempotency_key: "disconnect-pricing-event-key".to_owned(),
            source_report_id: "disconnect-pricing-report".to_owned(),
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
            provider_id: Some("xai".to_owned()),
            model_id: Some("grok-test".to_owned()),
            runtime_model: Some("grok-test".to_owned()),
            candidate_key: Some("disconnect-candidate".to_owned()),
            attempt_ordinal: 0,
            agent_id: None,
            profile_id: None,
            agent_name_snapshot: None,
            project_name_snapshot: None,
            executor_type: Some("api".to_owned()),
            pricing_subject_revision_id: Some(revision.id),
            subject_revision_digest: Some(revision.revision_digest),
            telemetry_state: UsageTelemetryState::Metered,
            input_tokens: Some(3),
            output_tokens: Some(2),
            cache_read_tokens: None,
            cache_write_tokens: None,
            context_tokens: None,
            selected_tier: None,
            provider_reported_nano_usd: Some(25),
            legacy_reported_cost_usd: None,
            estimated_nano_usd: None,
            cost_kind: UsageCostKind::ProviderReported,
            rate_revision_id: Some(rate.id),
            catalog_snapshot_id: None,
            formula_revision: None,
            retrospective: false,
            coverage_reason_code: None,
            occurred_at: "2026-09-08T00:00:04Z".to_owned(),
            created_at: "2026-09-08T00:00:04Z".to_owned(),
        },
    )
    .await
    .expect("usage event appends");
}

#[tokio::test]
async fn disconnect_retires_provider_subject_and_preserves_pricing_history() {
    let (store, db) = test_store().await;
    let created_at = "2026-09-08T00:00:00Z";
    store
        .create_oauth_credential(CreateOAuthCredential {
            id: "disconnect-pricing-handle",
            owner_user_id: "credential-owner",
            provider: "xai",
            label: "xAI login",
            bundle: &OAuthCredentialBundle {
                schema_version: 1,
                access_token: "access".to_owned(),
                refresh_token: "refresh".to_owned(),
                expires_at_ms: SystemClock.now().as_millis().saturating_add(60_000),
                token_endpoint: "https://auth.x.ai/token".to_owned(),
                client_id: "client".to_owned(),
                client_secret: None,
                scopes: vec![],
                provider_account_id: None,
            },
            metadata_json: "{}",
            now: created_at,
        })
        .await
        .expect("bundle stores");
    seed_provider_pricing_history(&db, "disconnect-pricing-handle").await;

    let before_counts: (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
                (SELECT COUNT(*) FROM pricing_subject_revision),
                (SELECT COUNT(*) FROM pricing_rate_revision),
                (SELECT COUNT(*) FROM pricing_subject_binding),
                (SELECT COUNT(*) FROM pricing_selection),
                (SELECT COUNT(*) FROM usage_event)",
    )
    .fetch_one(db.pool())
    .await
    .expect("pricing history counts before disconnect");
    assert_eq!(before_counts, (1, 1, 1, 1, 1));

    let disconnected_at = "2026-09-08T00:00:05Z";
    assert_eq!(
        store
            .revoke_credential_at_version(
                "disconnect-pricing-handle",
                "credential-owner",
                1,
                disconnected_at,
            )
            .await
            .expect("disconnect commits"),
        CredentialRevocationOutcome::NotSupported
    );

    let (credential_status, credential_version): (String, i64) = sqlx::query_as(
        "SELECT status, version FROM credential_handle
             WHERE id = 'disconnect-pricing-handle'",
    )
    .fetch_one(db.pool())
    .await
    .expect("credential metadata remains");
    assert_eq!(credential_status, "revoked");
    assert_eq!(credential_version, 2);

    let (subject_state, subject_version, subject_updated_at): (String, i64, String) =
        sqlx::query_as(
            "SELECT state, version, updated_at FROM pricing_subject
                 WHERE id = 'disconnect-pricing-subject'",
        )
        .fetch_one(db.pool())
        .await
        .expect("provider pricing subject remains");
    assert_eq!(subject_state, "retired");
    assert_eq!(subject_version, 3);
    assert_eq!(subject_updated_at, disconnected_at);

    let after_counts: (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
                (SELECT COUNT(*) FROM pricing_subject_revision),
                (SELECT COUNT(*) FROM pricing_rate_revision),
                (SELECT COUNT(*) FROM pricing_subject_binding),
                (SELECT COUNT(*) FROM pricing_selection),
                (SELECT COUNT(*) FROM usage_event)",
    )
    .fetch_one(db.pool())
    .await
    .expect("pricing history counts after disconnect");
    assert_eq!(after_counts, before_counts);

    let mut transaction = db::begin_immediate(db.pool())
        .await
        .expect("resolver transaction starts");
    let resolved = PricingSubjectRepo::resolve_active_pricing_subject_binding_in_tx(
        &*db,
        &mut transaction,
        "credential-owner",
        Some("disconnect-pricing-handle"),
        None,
        None,
        "",
        "grok-test",
    )
    .await
    .expect("retired subject resolves safely");
    transaction
        .rollback()
        .await
        .expect("resolver transaction rolls back");
    assert!(resolved.is_none());
}

#[tokio::test]
async fn disconnect_without_pricing_subject_is_a_noop_for_pricing() {
    let (store, db) = test_store().await;
    store
        .create_oauth_credential(CreateOAuthCredential {
            id: "disconnect-without-pricing",
            owner_user_id: "credential-owner",
            provider: "xai",
            label: "xAI login",
            bundle: &OAuthCredentialBundle {
                schema_version: 1,
                access_token: "access".to_owned(),
                refresh_token: "refresh".to_owned(),
                expires_at_ms: SystemClock.now().as_millis().saturating_add(60_000),
                token_endpoint: "https://auth.x.ai/token".to_owned(),
                client_id: "client".to_owned(),
                client_secret: None,
                scopes: vec![],
                provider_account_id: None,
            },
            metadata_json: "{}",
            now: "2026-09-08T00:00:00Z",
        })
        .await
        .expect("bundle stores");

    assert_eq!(
        store
            .revoke_credential_at_version(
                "disconnect-without-pricing",
                "credential-owner",
                1,
                "2026-09-08T00:00:01Z",
            )
            .await
            .expect("disconnect without pricing subject commits"),
        CredentialRevocationOutcome::NotSupported
    );
    let subject_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pricing_subject
             WHERE owner_user_id = 'credential-owner'
               AND subject_kind = 'provider_entry'
               AND provider_entry_id = 'disconnect-without-pricing'",
    )
    .fetch_one(db.pool())
    .await
    .expect("pricing subject count reads");
    assert_eq!(subject_count, 0);
    let status: String = sqlx::query_scalar(
        "SELECT status FROM credential_handle WHERE id = 'disconnect-without-pricing'",
    )
    .fetch_one(db.pool())
    .await
    .expect("credential status reads");
    assert_eq!(status, "revoked");
}

#[test]
fn oauth_bundle_debug_redacts_token_material() {
    let bundle = OAuthCredentialBundle {
        schema_version: 1,
        access_token: "sensitive-access".to_owned(),
        refresh_token: "sensitive-refresh".to_owned(),
        expires_at_ms: 42,
        token_endpoint: "https://example.com/token".to_owned(),
        client_id: "public-client".to_owned(),
        client_secret: Some("sensitive-client-secret".to_owned()),
        scopes: vec!["openid".to_owned()],
        provider_account_id: Some("account".to_owned()),
    };
    let debug = format!("{bundle:?}");
    assert!(!debug.contains("sensitive-access"));
    assert!(!debug.contains("sensitive-refresh"));
    assert!(!debug.contains("sensitive-client-secret"));
}

#[tokio::test]
async fn renewable_bundle_is_acquired_through_runtime_credential_contract() {
    let (store, db) = test_store().await;
    let now = db::now_rfc3339();
    store
        .create_oauth_credential(CreateOAuthCredential {
            id: "oauth-handle",
            owner_user_id: "credential-owner",
            provider: "xai",
            label: "xAI login",
            bundle: &OAuthCredentialBundle {
                schema_version: 1,
                access_token: "short-lived-access".to_owned(),
                refresh_token: "renewable-refresh".to_owned(),
                expires_at_ms: SystemClock.now().as_millis().saturating_add(60_000),
                token_endpoint: "https://auth.x.ai/token".to_owned(),
                client_id: "client".to_owned(),
                client_secret: None,
                scopes: vec!["openid".to_owned()],
                provider_account_id: None,
            },
            metadata_json: "{}",
            now: &now,
        })
        .await
        .expect("bundle stores");
    let secret = store
        .acquire_provider_credential("credential-owner", "oauth-handle", 30_000)
        .await
        .expect("lease acquires");
    assert_eq!(secret.expose(), "short-lived-access");
    let ciphertext: Vec<u8> = sqlx::query_scalar(
        "SELECT ciphertext FROM protected_credential_secret WHERE handle_id = 'oauth-handle'",
    )
    .fetch_one(db.pool())
    .await
    .expect("ciphertext reads");
    assert!(!String::from_utf8_lossy(&ciphertext).contains("short-lived-access"));
}

#[tokio::test]
async fn concurrent_expired_leases_single_flight_and_rotate_tokens_atomically() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("mock provider binds");
    let address = listener.local_addr().expect("mock address exists");
    let request_count = Arc::new(AtomicUsize::new(0));
    let server_count = Arc::clone(&request_count);
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("refresh request arrives");
        server_count.fetch_add(1, Ordering::SeqCst);
        let mut request = vec![0_u8; 4096];
        let _ = stream.read(&mut request).await.expect("request reads");
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        let body = r#"{"access_token":"rotated-access","refresh_token":"rotated-refresh","expires_in":3600,"scope":"openid profile"}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("response writes");
    });

    let (store, _) = test_store().await;
    let now = db::now_rfc3339();
    store
        .create_oauth_credential(CreateOAuthCredential {
            id: "refresh-handle",
            owner_user_id: "credential-owner",
            provider: "xai",
            label: "xAI login",
            bundle: &OAuthCredentialBundle {
                schema_version: 1,
                access_token: "expired-access".to_owned(),
                refresh_token: "initial-refresh".to_owned(),
                expires_at_ms: 0,
                token_endpoint: format!("http://{address}/token"),
                client_id: "client".to_owned(),
                client_secret: None,
                scopes: vec!["openid".to_owned()],
                provider_account_id: None,
            },
            metadata_json: "{}",
            now: &now,
        })
        .await
        .expect("expired bundle stores");
    let source = store.credential_source("credential-owner", "refresh-handle");
    let target = ProviderCredentialTarget::new("refresh-handle").expect("target is valid");
    let cancel = agent_runtime::core::cancel::Cancellation::new();
    let deadline = Deadline::after(&SystemClock, 5_000);
    let (first, second) = tokio::join!(
        source.acquire(&target, 30_000, &cancel, deadline),
        source.acquire(&target, 30_000, &cancel, deadline),
    );
    let first = first.expect("first lease refreshes");
    let second = second.expect("second lease reuses refresh");
    assert_eq!(first.secret().expose(), "rotated-access");
    assert_eq!(second.secret().expose(), "rotated-access");
    server.await.expect("mock provider completes");
    assert_eq!(request_count.load(Ordering::SeqCst), 1);

    let stored = store
        .load_stored_credential("refresh-handle", "credential-owner")
        .await
        .expect("rotated bundle loads");
    let bundle: OAuthCredentialBundle =
        serde_json::from_str(&stored.plaintext).expect("bundle decodes");
    assert_eq!(stored.version, 2);
    assert_eq!(bundle.refresh_token, "rotated-refresh");
    assert_eq!(bundle.scopes, vec!["openid", "profile"]);
}

#[tokio::test]
async fn stale_disconnect_preserves_the_current_credential() {
    let (store, db) = test_store().await;
    let now = db::now_rfc3339();
    store
        .create_oauth_credential(CreateOAuthCredential {
            id: "disconnect-handle",
            owner_user_id: "credential-owner",
            provider: "xai",
            label: "xAI login",
            bundle: &OAuthCredentialBundle {
                schema_version: 1,
                access_token: "access".to_owned(),
                refresh_token: "refresh".to_owned(),
                expires_at_ms: SystemClock.now().as_millis().saturating_add(60_000),
                token_endpoint: "https://auth.x.ai/token".to_owned(),
                client_id: "client".to_owned(),
                client_secret: None,
                scopes: vec![],
                provider_account_id: None,
            },
            metadata_json: "{}",
            now: &now,
        })
        .await
        .expect("bundle stores");
    assert!(matches!(
        store
            .revoke_credential_at_version("disconnect-handle", "credential-owner", 0, &now,)
            .await,
        Err(crate::AgentHostError::VersionConflict)
    ));
    let preserved = store
        .load_stored_credential("disconnect-handle", "credential-owner")
        .await
        .expect("stale disconnect preserves secret");
    assert_eq!(preserved.version, 1);
    let preserved_bundle: OAuthCredentialBundle =
        serde_json::from_str(&preserved.plaintext).expect("preserved bundle decodes");
    assert_eq!(preserved_bundle.access_token, "access");
    store
        .revoke_credential_at_version("disconnect-handle", "credential-owner", 1, &now)
        .await
        .expect("current disconnect succeeds");
    assert!(matches!(
        store
            .load_credential("disconnect-handle", "credential-owner")
            .await,
        Err(crate::AgentHostError::CredentialNotFound)
    ));
    let (status, version): (String, i64) = sqlx::query_as(
        "SELECT status, version FROM credential_handle WHERE id = 'disconnect-handle'",
    )
    .fetch_one(db.pool())
    .await
    .expect("handle remains as safe metadata");
    assert_eq!(status, "revoked");
    assert_eq!(version, 2);
}

#[tokio::test]
async fn provider_revocation_failure_never_rolls_back_local_disconnect() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("mock revocation provider binds");
    let address = listener.local_addr().expect("mock address exists");
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("revocation request arrives");
        let mut request = vec![0_u8; 4096];
        let size = stream.read(&mut request).await.expect("request reads");
        assert!(String::from_utf8_lossy(&request[..size]).contains("token=refresh"));
        stream
            .write_all(
                b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .await
            .expect("failure response writes");
    });

    let (mut store, db) = test_store().await;
    store.gemini_revocation_endpoint = Arc::from(format!("http://{address}/revoke"));
    let now = db::now_rfc3339();
    store
        .create_oauth_credential(CreateOAuthCredential {
            id: "gemini-disconnect-handle",
            owner_user_id: "credential-owner",
            provider: "gemini",
            label: "Google login",
            bundle: &OAuthCredentialBundle {
                schema_version: 1,
                access_token: "access".to_owned(),
                refresh_token: "refresh".to_owned(),
                expires_at_ms: SystemClock.now().as_millis().saturating_add(60_000),
                token_endpoint: "https://oauth2.googleapis.com/token".to_owned(),
                client_id: "client".to_owned(),
                client_secret: None,
                scopes: vec![],
                provider_account_id: None,
            },
            metadata_json: "{}",
            now: &now,
        })
        .await
        .expect("bundle stores");

    let outcome = store
        .revoke_credential_at_version("gemini-disconnect-handle", "credential-owner", 1, &now)
        .await
        .expect("local disconnect commits");
    assert_eq!(outcome, CredentialRevocationOutcome::Failed);
    server.await.expect("mock provider completes");

    assert!(matches!(
        store
            .load_credential("gemini-disconnect-handle", "credential-owner")
            .await,
        Err(crate::AgentHostError::CredentialNotFound)
    ));
    let status: String = sqlx::query_scalar(
        "SELECT status FROM credential_handle WHERE id = 'gemini-disconnect-handle'",
    )
    .fetch_one(db.pool())
    .await
    .expect("revoked metadata remains");
    assert_eq!(status, "revoked");
}
