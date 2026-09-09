#![allow(dead_code)]

mod common;

use api_types::{PricingCatalogModelsResponse, ProviderPricing};
use axum::http::{Method, StatusCode};
use serde_json::json;

#[tokio::test]
async fn identical_provider_identities_have_independent_pricing_subjects() {
    let workspace = common::TestDir::new("pricing-identical-provider-identities");
    let harness = common::test_app(workspace.path(), "pricing-identical-provider-identities").await;
    let now = db::now_rfc3339();

    // These entries intentionally have the same provider, credential method,
    // and endpoint.  The identity digest is allowed to match globally; the
    // subject and its bindings must still remain independent per entry.
    for (id, label) in [("provider-a", "Provider A"), ("provider-b", "Provider B")] {
        sqlx::query(
            "INSERT INTO credential_handle (
                 id, owner_user_id, provider, label, status, created_at, updated_at,
                 credential_method, metadata_json, version, enabled
             ) VALUES (?, 'test-user-id', 'openai', ?, 'configured', ?, ?,
                       'api_key', '{\"base_url\":\"https://api.openai.com/v1\"}', 1, 1)",
        )
        .bind(id)
        .bind(label)
        .bind(&now)
        .bind(&now)
        .execute(harness.state.db.pool())
        .await
        .expect("insert same-identity provider entry");
    }

    let token = common::test_jwt();
    // The static catalog path must win over `/providers/{id}/pricing`; this
    // request would otherwise be interpreted as provider id `pricing-catalog`.
    let catalog: PricingCatalogModelsResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/providers/pricing-catalog/models?limit=1",
        &token,
        StatusCode::OK,
    )
    .await;
    assert!(catalog.items.is_empty());
    assert!(!catalog.has_more);

    let first: ProviderPricing = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/providers/provider-a/pricing",
        &token,
        StatusCode::OK,
    )
    .await;
    let second: ProviderPricing = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/providers/provider-b/pricing",
        &token,
        StatusCode::OK,
    )
    .await;

    assert_ne!(first.subject_id, second.subject_id);
    assert_eq!(
        first.subject_revision_digest,
        second.subject_revision_digest
    );
    assert!(first.bindings.is_empty());
    assert!(second.bindings.is_empty());

    let updated: ProviderPricing = common::json_request_with_bearer(
        &harness.app,
        Method::PUT,
        "/api/v1/providers/provider-a/pricing",
        &token,
        json!({
            "expected_version": first.version,
            "idempotency_key": "same-identity-binding",
            "subject_revision_digest": first.subject_revision_digest,
            "bindings": [{
                "runtime_model": "gpt-shared",
                "source_kind": "manual_override",
                "catalog_provider_id": null,
                "catalog_model_id": null,
                "catalog_rate_revision_id": null,
                "manual_rates": {
                    "input": {"currency": "USD", "decimal_per_million": "1"},
                    "output": null,
                    "cache_read": null,
                    "cache_write": null
                }
            }]
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(updated.subject_id, first.subject_id);
    assert_eq!(updated.bindings.len(), 1);

    let first_after: ProviderPricing = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/providers/provider-a/pricing",
        &token,
        StatusCode::OK,
    )
    .await;
    let second_after: ProviderPricing = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/providers/provider-b/pricing",
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(first_after.bindings.len(), 1);
    assert!(second_after.bindings.is_empty());
    assert_ne!(first_after.subject_id, second_after.subject_id);
}
