#![allow(dead_code)]

mod common;

use api_types::{AgentPricing, PricingCatalogModelsResponse, PricingMode, SubjectPricingResponse};
use axum::http::{Method, StatusCode};
use db::{AgentRepo, AgentStatus, CreateAgentIdentity, CreateAgentProfile};
use serde_json::{json, Value};

async fn seed_provider(harness: &common::Harness, id: &str) {
    let now = db::now_rfc3339();
    sqlx::query(
        "INSERT INTO credential_handle (
             id, owner_user_id, provider, label, status, created_at, updated_at,
             credential_method, metadata_json, version, enabled
         ) VALUES (?, 'test-user-id', 'openai_compatible', ?, 'configured', ?, ?,
                   'api_key', '{\"base_url\":\"https://api.example.com/v1\"}', 1, 1)",
    )
    .bind(id)
    .bind(id)
    .bind(&now)
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("insert provider entry");
}

async fn seed_agent(harness: &common::Harness, provider_id: &str, id: &str) {
    let now = db::now_rfc3339();
    AgentRepo::create_identity_with_profile(
        &*harness.state.db,
        CreateAgentIdentity {
            id: id.to_owned(),
            name: "Pricing agent".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some("test-user-id".to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: format!("{id}-profile"),
            identity_id: id.to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "native".to_owned(),
            provider: None,
            model: Some("glm-5.3".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: Some(provider_id.to_owned()),
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("create agent");
}

#[tokio::test]
async fn provider_settings_default_update_conflict_validation_and_delete() {
    let workspace = common::TestDir::new("pricing-provider-settings");
    let harness = common::test_app(workspace.path(), "pricing-provider-settings").await;
    seed_provider(&harness, "provider-a").await;
    seed_provider(&harness, "provider-b").await;
    let token = common::test_jwt();

    let catalog: PricingCatalogModelsResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/providers/pricing-catalog/models?limit=1",
        &token,
        StatusCode::OK,
    )
    .await;
    assert!(catalog.items.is_empty());

    for id in ["provider-a", "provider-b"] {
        let response: SubjectPricingResponse = common::empty_request_with_bearer(
            &harness.app,
            Method::GET,
            &format!("/api/v1/providers/{id}/pricing"),
            &token,
            StatusCode::OK,
        )
        .await;
        assert!(response.settings.is_none());
    }

    let updated: SubjectPricingResponse = common::json_request_with_bearer(
        &harness.app,
        Method::PUT,
        "/api/v1/providers/provider-a/pricing",
        &token,
        json!({"mode":"discount", "discount_percent":"20", "expected_version":0}),
        StatusCode::OK,
    )
    .await;
    let settings = updated.settings.expect("discount settings");
    assert_eq!(settings.mode, PricingMode::Discount);
    assert_eq!(settings.discount_percent.as_deref(), Some("20"));
    assert_eq!(settings.version, 1);

    let loaded: SubjectPricingResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/providers/provider-a/pricing",
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(loaded.settings, Some(settings));
    let other: SubjectPricingResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/providers/provider-b/pricing",
        &token,
        StatusCode::OK,
    )
    .await;
    assert!(other.settings.is_none());

    let stale: Value = common::json_request_with_bearer(
        &harness.app,
        Method::PUT,
        "/api/v1/providers/provider-a/pricing",
        &token,
        json!({"mode":"discount", "discount_percent":"10", "expected_version":0}),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(stale["code"], "version_conflict");

    let invalid_discount: Value = common::json_request_with_bearer(
        &harness.app,
        Method::PUT,
        "/api/v1/providers/provider-a/pricing",
        &token,
        json!({"mode":"discount", "discount_percent":"101", "expected_version":1}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(invalid_discount["code"], "validation_error");

    let invalid_model_pin: Value = common::json_request_with_bearer(
        &harness.app,
        Method::PUT,
        "/api/v1/providers/provider-a/pricing",
        &token,
        json!({"mode":"list", "catalog_model_id":"glm-5.3", "expected_version":1}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(invalid_model_pin["code"], "validation_error");

    let deleted: SubjectPricingResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::DELETE,
        "/api/v1/providers/provider-a/pricing?version=1",
        &token,
        StatusCode::OK,
    )
    .await;
    assert!(deleted.settings.is_none());
}

#[tokio::test]
async fn agent_settings_override_and_reset_to_provider_settings() {
    let workspace = common::TestDir::new("pricing-agent-settings");
    let harness = common::test_app(workspace.path(), "pricing-agent-settings").await;
    seed_provider(&harness, "provider-a").await;
    seed_agent(&harness, "provider-a", "pricing-agent").await;
    let token = common::test_jwt();

    let initial: AgentPricing = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/agents/pricing-agent/pricing",
        &token,
        StatusCode::OK,
    )
    .await;
    assert!(initial.settings.is_none());
    assert!(initial.provider_settings.is_none());

    let _: SubjectPricingResponse = common::json_request_with_bearer(
        &harness.app,
        Method::PUT,
        "/api/v1/providers/provider-a/pricing",
        &token,
        json!({"mode":"discount", "discount_percent":"20", "expected_version":0}),
        StatusCode::OK,
    )
    .await;
    let inherited: AgentPricing = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/agents/pricing-agent/pricing",
        &token,
        StatusCode::OK,
    )
    .await;
    assert!(inherited.settings.is_none());
    assert_eq!(
        inherited
            .provider_settings
            .unwrap()
            .discount_percent
            .as_deref(),
        Some("20")
    );

    let updated: AgentPricing = common::json_request_with_bearer(
        &harness.app,
        Method::PUT,
        "/api/v1/agents/pricing-agent/pricing",
        &token,
        json!({"mode":"discount", "discount_percent":"15", "expected_version":0}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        updated
            .settings
            .as_ref()
            .unwrap()
            .discount_percent
            .as_deref(),
        Some("15")
    );
    assert_eq!(updated.settings.as_ref().unwrap().version, 1);
    assert_eq!(
        updated
            .provider_settings
            .as_ref()
            .unwrap()
            .discount_percent
            .as_deref(),
        Some("20")
    );

    let loaded: AgentPricing = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/agents/pricing-agent/pricing",
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(loaded.settings, updated.settings);

    let deleted: AgentPricing = common::empty_request_with_bearer(
        &harness.app,
        Method::DELETE,
        "/api/v1/agents/pricing-agent/pricing?version=1",
        &token,
        StatusCode::OK,
    )
    .await;
    assert!(deleted.settings.is_none());
    assert_eq!(
        deleted
            .provider_settings
            .unwrap()
            .discount_percent
            .as_deref(),
        Some("20")
    );
}
