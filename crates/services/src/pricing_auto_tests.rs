use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use db::{
    AgentRepo, AgentStatus, CreateAgentIdentity, CreateAgentProfile, PricingAdjustmentMode,
    PricingAdjustmentScope, PricingRateSourceKind, PricingSubjectRepo, RateBuckets,
    ResolvedPricingSubjectBinding, UpsertPricingAdjustment, User, UserRepo,
};

use crate::{
    pricing::{self, parse_models_dev_catalog, PricingCatalogRepository},
    pricing_auto::{prepare_price_in_tx, SubjectRef},
    pricing_db::SqlitePricingRepository,
};

const OWNER: &str = "pricing-auto-owner";
const PROVIDER: &str = "pricing-auto-provider";
const AGENT: &str = "pricing-auto-agent";
const MODEL: &str = "glm-5.3";

async fn database() -> Arc<db::SqliteDb> {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("in-memory pool");
    db::run_migrations(&pool).await.expect("migrations");
    Arc::new(db::SqliteDb::new(pool))
}

async fn seed(db: &Arc<db::SqliteDb>) {
    seed_with_catalog(
        db,
        br#"{
      "zai": {"id":"zai","name":"Z.ai","models":{
        "glm-5.3":{"id":"glm-5.3","last_updated":"2026-09-01",
          "cost":{"input":1,"output":2}}
      }},
      "requesty": {"id":"requesty","name":"Requesty","models":{
        "glm-5.3":{"id":"glm-5.3","last_updated":"2026-09-01",
          "cost":{"input":3,"output":4}}
      }}
    }"#,
    )
    .await;
}

async fn seed_with_catalog(db: &Arc<db::SqliteDb>, body: &[u8]) {
    let now = db::now_rfc3339();
    UserRepo::create_user(
        &**db,
        &User {
            id: OWNER.to_owned(),
            email: "pricing-auto@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("user");
    sqlx::query(
        "INSERT INTO credential_handle (
             id, owner_user_id, provider, label, status, created_at, updated_at,
             credential_method, metadata_json, version, enabled
         ) VALUES (?, ?, 'openai_compatible', 'Pricing provider', 'configured', ?, ?,
                   'api_key', '{\"base_url\":\"https://example.test/v1\"}', 1, 1)",
    )
    .bind(PROVIDER)
    .bind(OWNER)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("provider entry");
    AgentRepo::create_identity_with_profile(
        &**db,
        CreateAgentIdentity {
            id: AGENT.to_owned(),
            name: "Pricing auto agent".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some(OWNER.to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: "pricing-auto-profile".to_owned(),
            identity_id: AGENT.to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "native".to_owned(),
            provider: None,
            model: Some(MODEL.to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: Some(PROVIDER.to_owned()),
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("agent identity");

    let when = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    let snapshot = parse_models_dev_catalog(body)
        .expect("catalog parses")
        .into_snapshot(
            "pricing-auto-snapshot",
            Some("etag-auto".to_owned()),
            when,
            when,
        )
        .expect("snapshot materializes");
    SqlitePricingRepository::new(db.clone())
        .activate_catalog_snapshot(snapshot, "pricing-auto-refresh")
        .await
        .expect("catalog activates");
}

async fn prepare_and_resolve(
    db: &db::SqliteDb,
    agent_id: Option<&str>,
) -> (String, ResolvedPricingSubjectBinding) {
    let mut tx = db::begin_immediate(db.pool()).await.expect("transaction");
    let scope_key = prepare_price_in_tx(
        db,
        &mut tx,
        OWNER,
        &SubjectRef {
            provider_entry_id: Some(PROVIDER.to_owned()),
            daemon_id: None,
            executor_type: None,
        },
        agent_id,
        MODEL,
        &db::now_rfc3339(),
    )
    .await
    .expect("prepare price");
    let resolved = PricingSubjectRepo::resolve_active_pricing_subject_binding_in_tx(
        db,
        &mut tx,
        OWNER,
        Some(PROVIDER),
        None,
        None,
        &scope_key,
        MODEL,
    )
    .await
    .expect("resolve binding")
    .expect("pricing subject");
    tx.commit().await.expect("commit price");
    (scope_key, resolved)
}

#[tokio::test]
async fn materializes_catalog_discount_and_agent_fixed_rates_without_duplicate_bindings() {
    let db = database().await;
    seed(&db).await;

    let (scope, list) = prepare_and_resolve(&db, None).await;
    assert_eq!(scope, "");
    let list_binding = list.binding.expect("catalog binding");
    assert_eq!(list_binding.scope_key, "");
    assert_eq!(
        list_binding.source_kind,
        PricingRateSourceKind::ModelsDevCatalog
    );
    assert_eq!(list_binding.catalog_provider_id.as_deref(), Some("zai"));
    assert_eq!(list_binding.catalog_model_id.as_deref(), Some(MODEL));
    assert_eq!(
        list.rate.expect("catalog rate").rates,
        RateBuckets::new(Some(1_000_000_000), Some(2_000_000_000), None, None,)
    );

    PricingSubjectRepo::upsert_pricing_adjustment(
        &*db,
        UpsertPricingAdjustment {
            owner_user_id: OWNER.to_owned(),
            scope: PricingAdjustmentScope::ProviderEntry(PROVIDER.to_owned()),
            mode: PricingAdjustmentMode::Discount,
            discount_bps: Some(2_000),
            fixed_rates: RateBuckets::default(),
            catalog_provider_id: None,
            catalog_model_id: None,
            expected_version: 0,
            now: db::now_rfc3339(),
        },
    )
    .await
    .expect("provider discount");
    let (scope, discounted) = prepare_and_resolve(&db, None).await;
    assert_eq!(scope, "");
    assert_eq!(
        discounted.binding.as_ref().unwrap().source_kind,
        PricingRateSourceKind::ManualOverride
    );
    assert_eq!(
        discounted.rate.unwrap().rates,
        RateBuckets::new(Some(800_000_000), Some(1_600_000_000), None, None,)
    );

    let fixed = RateBuckets::new(Some(7_000_000_000), Some(9_000_000_000), None, None);
    PricingSubjectRepo::upsert_pricing_adjustment(
        &*db,
        UpsertPricingAdjustment {
            owner_user_id: OWNER.to_owned(),
            scope: PricingAdjustmentScope::Agent(AGENT.to_owned()),
            mode: PricingAdjustmentMode::Fixed,
            discount_bps: None,
            fixed_rates: fixed,
            catalog_provider_id: None,
            catalog_model_id: None,
            expected_version: 0,
            now: db::now_rfc3339(),
        },
    )
    .await
    .expect("agent fixed adjustment");
    let (scope, agent_price) = prepare_and_resolve(&db, Some(AGENT)).await;
    assert_eq!(scope, format!("agent:{AGENT}"));
    let agent_binding = agent_price.binding.expect("agent binding");
    assert_eq!(agent_binding.scope_key, scope);
    assert_eq!(
        agent_binding.source_kind,
        PricingRateSourceKind::ManualOverride
    );
    assert_eq!(agent_price.rate.unwrap().rates, fixed);

    let before: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pricing_subject_binding WHERE state = 'active' AND scope_key = ?",
    )
    .bind(&scope)
    .fetch_one(db.pool())
    .await
    .expect("active agent bindings before repeat");
    let (_, repeated) = prepare_and_resolve(&db, Some(AGENT)).await;
    assert_eq!(
        repeated.binding.expect("repeat binding").id,
        agent_binding.id
    );
    let after: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pricing_subject_binding WHERE state = 'active' AND scope_key = ?",
    )
    .bind(&scope)
    .fetch_one(db.pool())
    .await
    .expect("active agent bindings after repeat");
    assert_eq!((before, after), (1, 1));
}

/// A discount on a catalog row with context tiers (gpt-5.6's 272K band)
/// scales every band, and the manual price it materializes must still freeze
/// into a valid settlement selection.
#[tokio::test]
async fn discount_on_a_tiered_catalog_row_scales_every_band_and_freezes() {
    let db = database().await;
    seed_with_catalog(
        &db,
        br#"{
      "zai": {"id":"zai","name":"Z.ai","models":{
        "glm-5.3":{"id":"glm-5.3","last_updated":"2026-09-01",
          "cost":{"input":4,"output":20,"cache_read":0.4,
            "tiers":[{"input":8,"output":30,"cache_read":0.8,
              "tier":{"type":"context","size":272000}}]}}
      }}
    }"#,
    )
    .await;
    PricingSubjectRepo::upsert_pricing_adjustment(
        &*db,
        UpsertPricingAdjustment {
            owner_user_id: OWNER.to_owned(),
            scope: PricingAdjustmentScope::ProviderEntry(PROVIDER.to_owned()),
            mode: PricingAdjustmentMode::Discount,
            discount_bps: Some(2_500),
            fixed_rates: RateBuckets::default(),
            catalog_provider_id: None,
            catalog_model_id: None,
            expected_version: 0,
            now: db::now_rfc3339(),
        },
    )
    .await
    .expect("provider discount");

    let (_, resolved) = prepare_and_resolve(&db, None).await;
    let rate = resolved.rate.expect("discounted rate");
    assert_eq!(rate.source_kind, PricingRateSourceKind::ManualOverride);
    assert_eq!(
        rate.rates,
        RateBuckets::new(
            Some(3_000_000_000),
            Some(15_000_000_000),
            Some(300_000_000),
            None
        )
    );
    let tiers = pricing::parse_persisted_context_tiers(&rate.tiers_json).expect("tiers parse");
    assert_eq!(tiers.len(), 1);
    assert_eq!(tiers[0].threshold_tokens, 272_000);
    let band = tiers[0].rates;
    assert_eq!(
        [band.input, band.output, band.cache_read, band.cache_write]
            .map(|rate| rate.map(pricing::NanoUsdPerMillion::as_nano_usd_per_million)),
        [
            Some(6_000_000_000),
            Some(22_500_000_000),
            Some(600_000_000),
            None
        ]
    );

    let selection = pricing::FrozenPriceSelection::from_parts(
        Some("subject".to_owned()),
        Some("subject-digest".to_owned()),
        Some(MODEL.to_owned()),
        Some("candidate".to_owned()),
        0,
        None,
        None,
        None,
        Some(pricing::PricingSourceKind::ManualOverride),
        Some(rate.id.clone()),
        None,
        Some(pricing::EventBucketRates::new(
            Some(pricing::NanoUsdPerMillion::from_nano_usd(3_000_000_000).unwrap()),
            Some(pricing::NanoUsdPerMillion::from_nano_usd(15_000_000_000).unwrap()),
            Some(pricing::NanoUsdPerMillion::from_nano_usd(300_000_000).unwrap()),
            None,
        )),
        tiers,
        None,
        pricing::CatalogFreshness::NotApplicable,
        pricing::PriceSelectionStatus::Priced,
        None,
    );
    selection
        .validate()
        .expect("a discounted tiered price freezes into a valid selection");
}
