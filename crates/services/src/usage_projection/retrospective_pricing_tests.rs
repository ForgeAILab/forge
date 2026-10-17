//! Retrospective cost estimates as the usage projection reads them: the
//! pricing repository commits the run, the projection overlays it.

use std::time::Duration;

use db::PricingCatalogRepo;
use pricing::{
    preview_retrospective_estimates,
    sqlite::{
        test_support::{
            at, database, project_fixture, snapshot, subject_fixture, usage_event_fixture,
            FIXTURE_AGENT_ID,
        },
        SqlitePricingRepository,
    },
    EventTokenCounts, NanoUsd, PricingCatalogRepository, RetrospectiveCommitRequest,
    RetrospectiveEstimateRepository, RetrospectiveRepositoryError, RetrospectiveUsageEvent,
};

#[tokio::test]
async fn retrospective_commit_reconstructs_snapshot_rates_and_replays_without_touching_reported_cost(
) {
    let db = database().await;
    let _ = subject_fixture(&db).await;
    let project_id = project_fixture(&db).await;
    let repository = SqlitePricingRepository::new(db.clone());
    let snapshot = snapshot("catalog-retrospective", at(10));
    repository
        .activate_catalog_snapshot(snapshot.clone(), "catalog-retrospective-refresh")
        .await
        .expect("catalog activates");
    let stored_rate =
        PricingCatalogRepo::get_catalog_rate_revision(&*db, &snapshot.id, "openai", "gpt-test")
            .await
            .expect("catalog rate lookup")
            .expect("catalog rate exists");
    assert_eq!(stored_rate.id, snapshot.models[0].rate_digest(&snapshot.id));

    let estimated_event = usage_event_fixture(
        &db,
        &project_id,
        "retrospective-estimated",
        Some(EventTokenCounts::new(1, 0, 0, 0)),
        None,
    )
    .await;
    let reported_event =
        usage_event_fixture(&db, &project_id, "retrospective-reported", None, Some(42)).await;
    let preview = preview_retrospective_estimates(
        project_id.clone(),
        &snapshot,
        &[
            RetrospectiveUsageEvent {
                event_id: estimated_event.clone(),
                provider_id: Some("openai".to_owned()),
                model_id: Some("gpt-test".to_owned()),
                counters: Some(EventTokenCounts::new(1, 0, 0, 0)),
                provider_reported_amount: None,
                context_tokens: None,
                occurred_at: at(100),
            },
            RetrospectiveUsageEvent {
                event_id: reported_event.clone(),
                provider_id: Some("openai".to_owned()),
                model_id: Some("gpt-test".to_owned()),
                counters: None,
                provider_reported_amount: Some(NanoUsd::from_nano_usd(42).unwrap()),
                context_tokens: None,
                occurred_at: at(100),
            },
        ],
        at(110),
        Duration::from_secs(1_000),
    )
    .expect("preview builds");
    assert_eq!(preview.eligible_event_count, 1);
    assert_eq!(preview.unmatched_event_count, 0);
    assert_eq!(preview.already_reported_event_count, 1);
    assert_eq!(
        preview.projected_cost,
        Some(NanoUsd::from_nano_usd(1_000).unwrap())
    );

    let request = RetrospectiveCommitRequest {
        preview_id: preview.id.clone(),
        usage_set_digest: preview.usage_set_digest.clone(),
        idempotency_key: "retrospective-commit-1".to_owned(),
    };
    let mut tampered_preview = preview.clone();
    tampered_preview.eligible_events[0].amount = NanoUsd::ZERO;
    assert!(matches!(
        repository
            .commit_retrospective_preview(tampered_preview, request.clone(), at(111),)
            .await,
        Err(RetrospectiveRepositoryError::UsageSetConflict)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM cost_estimation_run")
            .fetch_one(db.pool())
            .await
            .expect("run count after rejected preview"),
        0
    );
    let index = super::UsageLedgerIndex::new(db.clone());
    let before_agent = index.agent(FIXTURE_AGENT_ID).await.unwrap();
    let _ = index.operations().await.unwrap();
    assert!(before_agent.cost.estimated.is_none());
    let committed = repository
        .commit_retrospective_preview(preview.clone(), request.clone(), at(111))
        .await
        .expect("retrospective commit");
    assert_eq!(committed.project_id, project_id);
    assert_eq!(committed.applied_event_count, 1);
    assert_eq!(committed.unmatched_event_count, 0);
    assert_eq!(committed.cost, preview.projected_cost);
    let stored_preview =
        db::RetrospectiveEstimateRepo::get_cost_estimation_preview(&*db, &preview.id)
            .await
            .expect("preview lookup")
            .expect("preview exists");
    assert_eq!(
        stored_preview.catalog_freshness,
        db::PricingCatalogFreshness::Fresh
    );

    let revisions =
        db::RetrospectiveEstimateRepo::list_cost_estimate_revisions_for_run(&*db, &committed.id)
            .await
            .expect("estimate revisions list");
    assert_eq!(revisions.len(), 1);
    assert_eq!(revisions[0].usage_event_id, estimated_event);
    assert_eq!(revisions[0].state, db::CostEstimateRevisionState::Applied);
    assert_eq!(revisions[0].rate_revision_id, Some(stored_rate.id));
    assert_eq!(revisions[0].catalog_snapshot_id, Some(snapshot.id.clone()));
    assert_eq!(revisions[0].estimated_nano_usd, Some(1_000));
    assert_eq!(
        revisions[0].formula_revision.as_deref(),
        Some(pricing::COST_FORMULA_REVISION)
    );

    // The public projection overlays the applied revision onto the
    // immutable event, including the preview's frozen provenance.
    let breakdowns = super::usage_breakdowns_for_source(&db, "execution-retrospective-estimated")
        .await
        .expect("breakdowns project");
    assert_eq!(breakdowns.len(), 1);
    let cost = &breakdowns[0].cost;
    assert_eq!(cost.kind, api_types::CostKind::Estimated);
    assert_eq!(cost.sources.len(), 1);
    assert!(cost.sources[0].retrospective);
    assert_eq!(
        cost.sources[0].rate_revision_id,
        revisions[0].rate_revision_id
    );
    assert_eq!(
        cost.sources[0].catalog_snapshot_id.as_deref(),
        Some(snapshot.id.as_str())
    );
    assert_eq!(
        cost.sources[0].freshness,
        api_types::CostSourceFreshness::Fresh
    );

    // Operations reads the same ledger in id batches. With an applied
    // revision next to a provider-reported event it must equal the
    // per-invocation walk, estimate and provenance included.
    let operations = super::usage_aggregate_for_operations(&db)
        .await
        .expect("operations aggregate");
    assert_eq!(
        serde_json::to_value(index.operations().await.unwrap()).unwrap(),
        serde_json::to_value(&operations).unwrap()
    );
    assert_eq!(
        serde_json::to_value(index.agent(FIXTURE_AGENT_ID).await.unwrap()).unwrap(),
        serde_json::to_value(
            super::usage_aggregate_for_agent(&db, FIXTURE_AGENT_ID)
                .await
                .unwrap()
        )
        .unwrap()
    );
    assert!(operations.cost.estimated.is_some());
    assert!(operations.cost.provider_reported.is_some());
    assert!(operations
        .cost
        .sources
        .iter()
        .any(|source| source.retrospective));
    assert_eq!(
        serde_json::to_value(
            super::per_invocation_usage_aggregate_for_operations(&db)
                .await
                .expect("per-invocation operations aggregate")
        )
        .unwrap(),
        serde_json::to_value(operations).unwrap()
    );

    let reported = db::UsageLedgerRepo::get_usage_event(&*db, &reported_event)
        .await
        .expect("reported event lookup")
        .expect("reported event exists");
    assert_eq!(reported.provider_reported_nano_usd, Some(42));
    assert_eq!(reported.estimated_nano_usd, None);
    assert_eq!(reported.cost_kind, db::UsageCostKind::ProviderReported);

    let replay = repository
        .commit_retrospective_preview(preview, request, at(999))
        .await
        .expect("idempotent replay");
    assert_eq!(replay, committed);
    assert_eq!(
        db::RetrospectiveEstimateRepo::list_cost_estimate_revisions_for_run(&*db, &committed.id,)
            .await
            .expect("revisions remain immutable")
            .len(),
        1
    );
}
