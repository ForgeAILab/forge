//! SQLite catalog refresh, replay, and activation checks.

use super::test_support::*;
use super::*;
use crate::{
    parse_models_dev_catalog, CatalogRefreshOutcome, CatalogRefreshRequest, ModelsDevClient,
    ModelsDevHttpResponse, ModelsDevTransport, ModelsDevTransportError, NanoUsdPerMillion,
};
use db::PricingCatalogRepo;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tokio::sync::Notify;

fn snapshot_variant(id: &str, when: SystemTime, model_id: &str) -> CatalogSnapshot {
    let body = String::from_utf8(catalog_body())
        .expect("fixture UTF-8")
        .replace("gpt-test", model_id)
        .into_bytes();
    parse_models_dev_catalog(&body)
        .expect("variant parses")
        .into_snapshot(id, Some(format!("etag-{model_id}")), when, when)
        .expect("variant materializes")
}

struct CountingTransport {
    responses: Mutex<Vec<Result<ModelsDevHttpResponse, ModelsDevTransportError>>>,
    fetch_count: AtomicUsize,
}

impl CountingTransport {
    fn new(responses: Vec<Result<ModelsDevHttpResponse, ModelsDevTransportError>>) -> Self {
        Self {
            responses: Mutex::new(responses),
            fetch_count: AtomicUsize::new(0),
        }
    }
}

#[async_trait::async_trait]
impl ModelsDevTransport for CountingTransport {
    async fn fetch(
        &self,
        _if_none_match: Option<&str>,
    ) -> Result<ModelsDevHttpResponse, ModelsDevTransportError> {
        self.fetch_count.fetch_add(1, Ordering::Relaxed);
        let mut responses = self.responses.lock().expect("transport response lock");
        if responses.is_empty() {
            Err(ModelsDevTransportError::Request(
                "unexpected third transport fetch".to_owned(),
            ))
        } else {
            responses.remove(0)
        }
    }
}

struct BlockingFirstTransport {
    response: Mutex<Option<Result<ModelsDevHttpResponse, ModelsDevTransportError>>>,
    first_started: Notify,
    release_first: Notify,
    fetch_count: AtomicUsize,
}

impl BlockingFirstTransport {
    fn new(response: Result<ModelsDevHttpResponse, ModelsDevTransportError>) -> Self {
        Self {
            response: Mutex::new(Some(response)),
            first_started: Notify::new(),
            release_first: Notify::new(),
            fetch_count: AtomicUsize::new(0),
        }
    }
}

#[async_trait::async_trait]
impl ModelsDevTransport for BlockingFirstTransport {
    async fn fetch(
        &self,
        _if_none_match: Option<&str>,
    ) -> Result<ModelsDevHttpResponse, ModelsDevTransportError> {
        let fetch_number = self.fetch_count.fetch_add(1, Ordering::Relaxed);
        if fetch_number == 0 {
            self.first_started.notify_one();
            self.release_first.notified().await;
        }
        self.response
            .lock()
            .expect("blocking transport response lock")
            .take()
            .unwrap_or_else(|| {
                Err(ModelsDevTransportError::Request(
                    "unexpected second transport fetch".to_owned(),
                ))
            })
    }
}

struct SignallingCatalogRepository {
    inner: Arc<SqlitePricingRepository>,
    status_calls: AtomicUsize,
    second_initial_status: Notify,
}

#[async_trait::async_trait]
impl PricingCatalogRepository for SignallingCatalogRepository {
    async fn catalog_status(&self) -> Result<CatalogStatus, CatalogRepositoryError> {
        if self.status_calls.fetch_add(1, Ordering::Relaxed) >= 2 {
            self.second_initial_status.notify_one();
        }
        self.inner.catalog_status().await
    }

    async fn replay_catalog_refresh(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<CatalogRefreshOutcome>, CatalogRepositoryError> {
        self.inner.replay_catalog_refresh(idempotency_key).await
    }

    async fn record_catalog_refresh_already_refreshed(
        &self,
        checked_at: SystemTime,
        idempotency_key: &str,
    ) -> Result<CatalogStatus, CatalogRepositoryError> {
        self.inner
            .record_catalog_refresh_already_refreshed(checked_at, idempotency_key)
            .await
    }

    async fn active_catalog_snapshot(
        &self,
    ) -> Result<Option<CatalogSnapshot>, CatalogRepositoryError> {
        self.inner.active_catalog_snapshot().await
    }

    async fn activate_catalog_snapshot(
        &self,
        snapshot: CatalogSnapshot,
        idempotency_key: &str,
    ) -> Result<CatalogStatus, CatalogRepositoryError> {
        self.inner
            .activate_catalog_snapshot(snapshot, idempotency_key)
            .await
    }

    async fn record_catalog_not_modified(
        &self,
        checked_at: SystemTime,
        etag: Option<String>,
        idempotency_key: &str,
    ) -> Result<CatalogStatus, CatalogRepositoryError> {
        self.inner
            .record_catalog_not_modified(checked_at, etag, idempotency_key)
            .await
    }

    async fn record_catalog_refresh_failure(
        &self,
        checked_at: SystemTime,
        code: CatalogRefreshErrorCode,
        idempotency_key: &str,
    ) -> Result<CatalogStatus, CatalogRepositoryError> {
        self.inner
            .record_catalog_refresh_failure(checked_at, code, idempotency_key)
            .await
    }
}

#[tokio::test]
async fn catalog_activation_304_failure_and_lkg_round_trip() {
    let db = database().await;
    let repository = SqlitePricingRepository::new(db.clone());
    let snapshot = snapshot("catalog-snapshot-1", at(10));
    let status = repository
        .activate_catalog_snapshot(snapshot.clone(), "refresh-1")
        .await
        .expect("activation succeeds");
    assert_eq!(status.state, CatalogState::Fresh);
    assert_eq!(
        status.active_snapshot_id.as_deref(),
        Some(snapshot.id.as_str())
    );
    assert_eq!(status.revision, Some(snapshot.revision_digest.clone()));

    let active = repository
        .active_catalog_snapshot()
        .await
        .expect("active snapshot reads")
        .expect("active snapshot exists");
    assert_eq!(active.raw_payload, snapshot.raw_payload);
    assert_eq!(active.models.len(), 1);
    assert_eq!(
        active.models[0].rates.input,
        Some(NanoUsdPerMillion::from_nano_usd(1_000_000_000).unwrap())
    );

    let failed = repository
        .record_catalog_refresh_failure(at(20), CatalogRefreshErrorCode::Transport, "refresh-2")
        .await
        .expect("failure records");
    assert_eq!(failed.state, CatalogState::RefreshFailed);
    assert_eq!(failed.active_snapshot_id, Some(snapshot.id.clone()));
    assert_eq!(failed.last_error_code.as_deref(), Some("transport"));
    assert_eq!(
        repository
            .active_catalog_snapshot()
            .await
            .expect("LKG reads")
            .expect("LKG exists")
            .id,
        snapshot.id
    );

    let refreshed = repository
        .record_catalog_not_modified(at(30), Some("etag-2".to_owned()), "refresh-3")
        .await
        .expect("304 records");
    assert_eq!(refreshed.state, CatalogState::Fresh);
    assert_eq!(refreshed.last_error_code, None);
    assert_eq!(refreshed.active_snapshot_id, Some(snapshot.id));
    assert_eq!(refreshed.etag.as_deref(), Some("etag-2"));
}

#[tokio::test]
async fn catalog_refresh_receipts_replay_original_key_after_a_later_activation() {
    let db = database().await;
    let repository = SqlitePricingRepository::new(db.clone());
    let first = snapshot("catalog-receipt-a", at(10));
    let first_status = repository
        .activate_catalog_snapshot(first.clone(), "refresh-a")
        .await
        .expect("first activation");
    let second = snapshot_variant("catalog-receipt-b", at(20), "gpt-other");
    repository
        .activate_catalog_snapshot(second.clone(), "refresh-b")
        .await
        .expect("second activation");

    let replay_status = repository
        .activate_catalog_snapshot(first, "refresh-a")
        .await
        .expect("first activation replays");
    assert_eq!(replay_status, first_status);
    assert_eq!(
        repository
            .active_catalog_snapshot()
            .await
            .expect("active snapshot reads")
            .expect("active snapshot")
            .id,
        second.id
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM pricing_operation_receipt
                 WHERE operation_scope = 'catalog_refresh'",
        )
        .fetch_one(db.pool())
        .await
        .expect("receipt count"),
        2
    );
}

#[tokio::test]
async fn catalog_client_replays_durable_refresh_outcome_without_transport_after_restart() {
    let db = database().await;
    let repository = Arc::new(SqlitePricingRepository::new(db));
    let transport = Arc::new(CountingTransport::new(vec![
        Ok(ModelsDevHttpResponse::ok(
            catalog_body(),
            Some("etag-a".to_owned()),
        )),
        Ok(ModelsDevHttpResponse::ok(
            snapshot_variant("unused", at(20), "gpt-other").raw_payload,
            Some("etag-b".to_owned()),
        )),
    ]));
    let client = ModelsDevClient::with_transport(repository.clone(), transport.clone());
    let first = client
        .refresh(CatalogRefreshRequest::at("refresh-a", at(10)))
        .await
        .expect("first refresh activates");
    let replayable_first = first.clone();
    let first_snapshot_id = match &first {
        CatalogRefreshOutcome::Activated { snapshot, .. } => snapshot.id.clone(),
        other => panic!("expected activated first refresh, got {other:?}"),
    };

    let second = client
        .refresh(CatalogRefreshRequest::at("refresh-b", at(20)))
        .await
        .expect("second refresh activates");
    assert!(matches!(second, CatalogRefreshOutcome::Activated { .. }));
    assert_eq!(transport.fetch_count.load(Ordering::Relaxed), 2);

    // A fresh client instance must consult the durable receipt before
    // attempting transport, and return the exact original status/snapshot
    // even though a later refresh has advanced the active pointer.
    let restarted = ModelsDevClient::with_transport(repository.clone(), transport.clone());
    let replay = restarted
        .refresh(CatalogRefreshRequest::at(
            "refresh-a",
            at(10 + pricing::MODELS_DEV_STALE_AFTER.as_secs() + 1),
        ))
        .await
        .expect("refresh receipt replays");
    assert_eq!(replay, replayable_first);
    assert_eq!(transport.fetch_count.load(Ordering::Relaxed), 2);
    assert_ne!(
        repository
            .active_catalog_snapshot()
            .await
            .expect("active snapshot reads")
            .expect("active snapshot")
            .id,
        first_snapshot_id
    );

    // A losing key can be durably recorded as a no-op and then replayed
    // without issuing a fetch on a later process/client instance.
    repository
        .record_catalog_refresh_already_refreshed(at(30), "refresh-loser")
        .await
        .expect("losing refresh receipt records");
    let loser = restarted
        .refresh(CatalogRefreshRequest::at("refresh-loser", at(31)))
        .await
        .expect("losing refresh receipt replays");
    assert!(matches!(
        loser,
        CatalogRefreshOutcome::AlreadyRefreshed { .. }
    ));
    assert_eq!(transport.fetch_count.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn transport_error_receipt_persists_only_the_stable_redacted_code() {
    let db = database().await;
    let repository = Arc::new(SqlitePricingRepository::new(db.clone()));
    let sensitive = "GET https://models.dev/api.json bearer=secret-token body=private-payload";
    let transport = Arc::new(CountingTransport::new(vec![Err(
        ModelsDevTransportError::Request(sensitive.to_owned()),
    )]));
    let client = ModelsDevClient::with_transport(repository, transport);
    let result = client
        .refresh(CatalogRefreshRequest::at("transport-redaction", at(10)))
        .await
        .expect("transport failures are represented as a failed outcome");
    assert!(matches!(
        result,
        CatalogRefreshOutcome::Failed {
            code: CatalogRefreshErrorCode::Transport,
            ..
        }
    ));

    let receipt: String = sqlx::query_scalar(
        "SELECT result_json FROM pricing_operation_receipt
             WHERE operation_scope = 'catalog_refresh'
               AND idempotency_key = 'transport-redaction'",
    )
    .fetch_one(db.pool())
    .await
    .expect("transport receipt");
    assert!(receipt.contains("transport"));
    assert!(!receipt.contains("secret-token"));
    assert!(!receipt.contains("private-payload"));
    assert!(!receipt.contains("models.dev/api.json"));
}

#[tokio::test]
async fn concurrent_refresh_loser_gets_durable_noop_and_replays_without_fetch() {
    let db = database().await;
    let inner = Arc::new(SqlitePricingRepository::new(db));
    let repository = Arc::new(SignallingCatalogRepository {
        inner,
        status_calls: AtomicUsize::new(0),
        second_initial_status: Notify::new(),
    });
    let transport = Arc::new(BlockingFirstTransport::new(Ok(ModelsDevHttpResponse::ok(
        catalog_body(),
        Some("etag-concurrent".to_owned()),
    ))));
    let client = ModelsDevClient::with_transport(repository.clone(), transport.clone());

    let first_client = client.clone();
    let first = tokio::spawn(async move {
        first_client
            .refresh(CatalogRefreshRequest::at("refresh-concurrent-a", at(10)))
            .await
    });
    transport.first_started.notified().await;

    let second_client = client.clone();
    let second = tokio::spawn(async move {
        second_client
            .refresh(CatalogRefreshRequest::at("refresh-concurrent-b", at(11)))
            .await
    });
    // Wait until B has captured the pre-A status and is queued behind the
    // single-flight gate before allowing A to commit its snapshot.
    repository.second_initial_status.notified().await;
    transport.release_first.notify_one();

    let first_outcome = first
        .await
        .expect("first refresh task")
        .expect("first refresh");
    assert!(matches!(
        first_outcome,
        CatalogRefreshOutcome::Activated { .. }
    ));
    let second_outcome = second
        .await
        .expect("second refresh task")
        .expect("second refresh");
    assert!(matches!(
        second_outcome,
        CatalogRefreshOutcome::AlreadyRefreshed { .. }
    ));
    assert_eq!(transport.fetch_count.load(Ordering::Relaxed), 1);

    let restarted = ModelsDevClient::with_transport(repository, transport.clone());
    let replay = restarted
        .refresh(CatalogRefreshRequest::at("refresh-concurrent-b", at(999)))
        .await
        .expect("losing refresh replays after restart");
    assert_eq!(replay, second_outcome);
    assert_eq!(transport.fetch_count.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn catalog_activation_rolls_back_snapshot_and_rates_on_failure() {
    let db = database().await;
    let repository = SqlitePricingRepository::new(db.clone());
    let mut invalid = snapshot("catalog-invalid", at(10));
    invalid.models[0].provider_id.clear();
    let error = repository
        .activate_catalog_snapshot(invalid, "refresh-invalid")
        .await
        .expect_err("invalid normalized row is rejected");
    assert!(matches!(error, CatalogRepositoryError::Unavailable(_)));
    assert!(db
        .get_pricing_catalog_snapshot("catalog-invalid")
        .await
        .expect("snapshot lookup")
        .is_none());
    assert!(db
        .get_pricing_catalog_state()
        .await
        .expect("state lookup")
        .is_none());
    let rate_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pricing_rate_revision")
        .fetch_one(db.pool())
        .await
        .expect("rate count");
    assert_eq!(rate_count, 0);
}
