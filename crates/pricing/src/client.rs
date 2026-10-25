//! Bounded catalog transport and conditional refresh.

use super::*;

/// Bounded, redacted failure code persisted for a catalog refresh attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CatalogRefreshErrorCode {
    /// DNS, TLS, timeout, or other transport failure.
    Transport,
    /// Non-200/non-304 HTTP response.
    HttpStatus,
    /// 200 response did not advertise JSON content.
    ContentType,
    /// Response body exceeded the hard bound.
    ResponseTooLarge,
    /// Empty, truncated, malformed, or semantically invalid payload.
    InvalidPayload,
    /// A 304 was received without an active retained payload.
    MissingSnapshot,
    /// Invalid request/idempotency data.
    InvalidRequest,
}

impl CatalogRefreshErrorCode {
    /// Stable persisted/API spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Transport => "transport",
            Self::HttpStatus => "http_status",
            Self::ContentType => "content_type",
            Self::ResponseTooLarge => "response_too_large",
            Self::InvalidPayload => "invalid_payload",
            Self::MissingSnapshot => "missing_snapshot",
            Self::InvalidRequest => "invalid_request",
        }
    }
}

/// A request to refresh the fixed models.dev catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogRefreshRequest {
    /// Stable retry key supplied by the authorized caller.
    pub idempotency_key: String,
    /// Injectable clock used for metadata and deterministic tests.
    pub requested_at: SystemTime,
}

impl CatalogRefreshRequest {
    /// Creates a request using the current wall clock.
    pub fn new(idempotency_key: impl Into<String>) -> Self {
        Self {
            idempotency_key: idempotency_key.into(),
            requested_at: SystemTime::now(),
        }
    }

    /// Creates a request with an explicit clock value.
    pub fn at(idempotency_key: impl Into<String>, requested_at: SystemTime) -> Self {
        Self {
            idempotency_key: idempotency_key.into(),
            requested_at,
        }
    }
}

/// HTTP result exposed by an injectable models.dev transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelsDevHttpResponse {
    /// HTTP status code.
    pub status: u16,
    /// ETag header, if valid and present.
    pub etag: Option<String>,
    /// Content-Type header value, if present.
    pub content_type: Option<String>,
    /// Bounded decoded response bytes.
    pub body: Vec<u8>,
}

impl ModelsDevHttpResponse {
    /// Constructs a successful JSON response for tests/transports.
    pub fn ok(body: impl Into<Vec<u8>>, etag: Option<String>) -> Self {
        Self {
            status: 200,
            etag,
            content_type: Some("application/json".to_owned()),
            body: body.into(),
        }
    }

    /// Constructs a conditional-not-modified response.
    pub fn not_modified(etag: Option<String>) -> Self {
        Self {
            status: 304,
            etag,
            content_type: None,
            body: Vec::new(),
        }
    }
}

/// Transport failure before a response can be validated.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ModelsDevTransportError {
    #[error("models.dev transport request failed: {0}")]
    Request(String),
    #[error("models.dev response exceeded the {limit}-byte limit")]
    ResponseTooLarge { limit: usize },
}

/// Injectable transport for deterministic catalog fixtures and local tests.
#[async_trait]
pub trait ModelsDevTransport: Send + Sync {
    /// Fetches the fixed endpoint with an optional conditional ETag.
    async fn fetch(
        &self,
        if_none_match: Option<&str>,
    ) -> Result<ModelsDevHttpResponse, ModelsDevTransportError>;
}

/// Production HTTPS transport for the fixed models.dev endpoint.
#[derive(Clone)]
pub struct ReqwestModelsDevTransport {
    client: reqwest::Client,
}

impl ReqwestModelsDevTransport {
    /// Builds a client with redirects disabled and the fixed 30-second bound.
    pub fn new() -> Result<Self, ModelsDevTransportError> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(MODELS_DEV_REQUEST_TIMEOUT)
            .build()
            .map_err(|error| ModelsDevTransportError::Request(error.to_string()))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl ModelsDevTransport for ReqwestModelsDevTransport {
    async fn fetch(
        &self,
        if_none_match: Option<&str>,
    ) -> Result<ModelsDevHttpResponse, ModelsDevTransportError> {
        let mut request = self.client.get(MODELS_DEV_ENDPOINT);
        if let Some(etag) = if_none_match {
            request = request.header(IF_NONE_MATCH, etag);
        }
        let mut response = request
            .send()
            .await
            .map_err(|error| ModelsDevTransportError::Request(error.to_string()))?;
        let status = response.status().as_u16();
        let etag = response
            .headers()
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        if status != 200 {
            return Ok(ModelsDevHttpResponse {
                status,
                etag,
                content_type,
                body: Vec::new(),
            });
        }

        // Reject a declared body larger than the hard bound before asking
        // reqwest to buffer or stream any bytes. The chunk-by-chunk check
        // below remains necessary because a server may omit or falsify this
        // header.
        if response_content_length_exceeds_limit(response.content_length()) {
            return Err(ModelsDevTransportError::ResponseTooLarge {
                limit: MODELS_DEV_MAX_RESPONSE_BYTES,
            });
        }

        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| ModelsDevTransportError::Request(error.to_string()))?
        {
            if body.len().saturating_add(chunk.len()) > MODELS_DEV_MAX_RESPONSE_BYTES {
                return Err(ModelsDevTransportError::ResponseTooLarge {
                    limit: MODELS_DEV_MAX_RESPONSE_BYTES,
                });
            }
            body.extend_from_slice(&chunk);
        }
        Ok(ModelsDevHttpResponse {
            status,
            etag,
            content_type,
            body,
        })
    }
}

/// Errors that prevent a refresh transaction from being attempted.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CatalogClientError {
    #[error("catalog refresh request is invalid: {0}")]
    InvalidRequest(String),
    #[error(transparent)]
    Repository(#[from] CatalogRepositoryError),
}

/// Result of one serialized conditional refresh.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogRefreshOutcome {
    /// A 200 payload was validated and atomically activated.
    Activated {
        snapshot: Box<CatalogSnapshot>,
        status: CatalogStatus,
    },
    /// A 304 updated successful-check metadata without a new payload revision.
    NotModified { status: CatalogStatus },
    /// A concurrent caller already completed a refresh while this call waited.
    AlreadyRefreshed { status: CatalogStatus },
    /// Refresh failed; the status contains the preserved LKG state.
    Failed {
        status: CatalogStatus,
        code: CatalogRefreshErrorCode,
    },
}

/// Bounded conditional catalog client.  Clones share a single-flight gate.
#[derive(Clone)]
pub struct ModelsDevClient {
    transport: Arc<dyn ModelsDevTransport>,
    repository: Arc<dyn PricingCatalogRepository>,
    refresh_gate: Arc<Mutex<()>>,
}

impl ModelsDevClient {
    /// Builds the production client with the fixed models.dev HTTPS transport.
    pub fn new(
        repository: Arc<dyn PricingCatalogRepository>,
    ) -> Result<Self, ModelsDevTransportError> {
        Ok(Self::with_transport(
            repository,
            Arc::new(ReqwestModelsDevTransport::new()?),
        ))
    }

    /// Builds a client with an injectable transport for fixtures/tests.
    pub fn with_transport(
        repository: Arc<dyn PricingCatalogRepository>,
        transport: Arc<dyn ModelsDevTransport>,
    ) -> Self {
        Self {
            transport,
            repository,
            refresh_gate: Arc::new(Mutex::new(())),
        }
    }

    /// Returns current status, deriving stale/fresh state at `now`.
    pub async fn status_at(&self, now: SystemTime) -> Result<CatalogStatus, CatalogClientError> {
        Ok(self.repository.catalog_status().await?.at(now))
    }

    /// Refreshes using the request's injectable clock.
    pub async fn refresh(
        &self,
        request: CatalogRefreshRequest,
    ) -> Result<CatalogRefreshOutcome, CatalogClientError> {
        if request.idempotency_key.trim().is_empty() {
            return Err(CatalogClientError::InvalidRequest(
                "idempotency key must not be empty".to_owned(),
            ));
        }
        if request.idempotency_key.len() > 256 {
            return Err(CatalogClientError::InvalidRequest(
                "idempotency key is too long".to_owned(),
            ));
        }

        if let Some(outcome) = self
            .repository
            .replay_catalog_refresh(&request.idempotency_key)
            .await?
        {
            return Ok(outcome);
        }
        let initial = self.repository.catalog_status().await?;
        let _gate = self.refresh_gate.lock().await;
        if let Some(outcome) = self
            .repository
            .replay_catalog_refresh(&request.idempotency_key)
            .await?
        {
            return Ok(outcome);
        }
        let current = self.repository.catalog_status().await?;
        if current.last_idempotency_key.as_deref() == Some(request.idempotency_key.as_str()) {
            let status = self
                .repository
                .record_catalog_refresh_already_refreshed(
                    request.requested_at,
                    &request.idempotency_key,
                )
                .await?;
            return Ok(CatalogRefreshOutcome::AlreadyRefreshed {
                status: status.at(request.requested_at),
            });
        }
        // A different concurrent caller may have completed while this call
        // waited.  Return its result without issuing a second network fetch.
        if current.version != initial.version || current.last_checked_at != initial.last_checked_at
        {
            let status = self
                .repository
                .record_catalog_refresh_already_refreshed(
                    request.requested_at,
                    &request.idempotency_key,
                )
                .await?;
            return Ok(CatalogRefreshOutcome::AlreadyRefreshed {
                status: status.at(request.requested_at),
            });
        }

        let response = match self.transport.fetch(current.etag.as_deref()).await {
            Ok(response) => response,
            Err(ModelsDevTransportError::ResponseTooLarge { .. }) => {
                return self
                    .failed(
                        current,
                        request.requested_at,
                        request.idempotency_key,
                        CatalogRefreshErrorCode::ResponseTooLarge,
                    )
                    .await;
            }
            Err(ModelsDevTransportError::Request(_)) => {
                return self
                    .failed(
                        current,
                        request.requested_at,
                        request.idempotency_key,
                        CatalogRefreshErrorCode::Transport,
                    )
                    .await;
            }
        };
        if let Some(etag) = response.etag.as_deref() {
            if etag.len() > MODELS_DEV_MAX_OPTIONAL_STRING_BYTES
                || etag.chars().any(char::is_control)
            {
                return self
                    .failed(
                        current,
                        request.requested_at,
                        request.idempotency_key,
                        CatalogRefreshErrorCode::InvalidPayload,
                    )
                    .await;
            }
        }
        match response.status {
            304 => self.handle_not_modified(current, response, request).await,
            200 => self.handle_ok(current, response, request).await,
            _ => {
                self.failed(
                    current,
                    request.requested_at,
                    request.idempotency_key,
                    CatalogRefreshErrorCode::HttpStatus,
                )
                .await
            }
        }
    }

    async fn handle_ok(
        &self,
        current: CatalogStatus,
        response: ModelsDevHttpResponse,
        request: CatalogRefreshRequest,
    ) -> Result<CatalogRefreshOutcome, CatalogClientError> {
        if response.body.len() > MODELS_DEV_MAX_RESPONSE_BYTES {
            return self
                .failed(
                    current,
                    request.requested_at,
                    request.idempotency_key,
                    CatalogRefreshErrorCode::ResponseTooLarge,
                )
                .await;
        }
        if !is_json_content_type(response.content_type.as_deref()) {
            let status = self
                .repository
                .record_catalog_refresh_failure(
                    request.requested_at,
                    CatalogRefreshErrorCode::ContentType,
                    &request.idempotency_key,
                )
                .await?;
            return Ok(CatalogRefreshOutcome::Failed {
                status: status.at(request.requested_at),
                code: CatalogRefreshErrorCode::ContentType,
            });
        }
        let parsed = match parse_models_dev_catalog(&response.body) {
            Ok(parsed) => parsed,
            Err(_) => {
                let current = self.repository.catalog_status().await?;
                return self
                    .failed(
                        current,
                        request.requested_at,
                        request.idempotency_key,
                        CatalogRefreshErrorCode::InvalidPayload,
                    )
                    .await;
            }
        };
        let snapshot = parsed
            .into_snapshot(
                uuid::Uuid::new_v4().to_string(),
                response.etag,
                request.requested_at,
                request.requested_at,
            )
            .map_err(|_| {
                CatalogClientError::InvalidRequest("snapshot materialization failed".to_owned())
            })?;
        let status = self
            .repository
            .activate_catalog_snapshot(snapshot.clone(), &request.idempotency_key)
            .await?;
        Ok(CatalogRefreshOutcome::Activated {
            snapshot: Box::new(snapshot),
            status: status.at(request.requested_at),
        })
    }

    async fn handle_not_modified(
        &self,
        current: CatalogStatus,
        response: ModelsDevHttpResponse,
        request: CatalogRefreshRequest,
    ) -> Result<CatalogRefreshOutcome, CatalogClientError> {
        let Some(active) = self.repository.active_catalog_snapshot().await? else {
            return self
                .failed(
                    current,
                    request.requested_at,
                    request.idempotency_key,
                    CatalogRefreshErrorCode::MissingSnapshot,
                )
                .await;
        };
        let etag = response.etag.or(active.etag.clone());
        if active.parser_revision == MODELS_DEV_PARSER_REVISION {
            let status = self
                .repository
                .record_catalog_not_modified(request.requested_at, etag, &request.idempotency_key)
                .await?;
            return Ok(CatalogRefreshOutcome::NotModified {
                status: status.at(request.requested_at),
            });
        }

        // A 304 with a changed parser revision is a local normalization
        // revision, not a new source payload revision.
        let parsed = match parse_models_dev_catalog(&active.raw_payload) {
            Ok(parsed) => parsed,
            Err(_) => {
                return self
                    .failed(
                        current,
                        request.requested_at,
                        request.idempotency_key,
                        CatalogRefreshErrorCode::InvalidPayload,
                    )
                    .await;
            }
        };
        let snapshot = parsed
            .into_snapshot(
                uuid::Uuid::new_v4().to_string(),
                etag,
                request.requested_at,
                request.requested_at,
            )
            .map_err(|_| {
                CatalogClientError::InvalidRequest("snapshot materialization failed".to_owned())
            })?;
        let status = self
            .repository
            .activate_catalog_snapshot(snapshot.clone(), &request.idempotency_key)
            .await?;
        Ok(CatalogRefreshOutcome::Activated {
            snapshot: Box::new(snapshot),
            status: status.at(request.requested_at),
        })
    }

    async fn failed(
        &self,
        _current: CatalogStatus,
        checked_at: SystemTime,
        idempotency_key: String,
        code: CatalogRefreshErrorCode,
    ) -> Result<CatalogRefreshOutcome, CatalogClientError> {
        let status = self
            .repository
            .record_catalog_refresh_failure(checked_at, code, &idempotency_key)
            .await?;
        Ok(CatalogRefreshOutcome::Failed {
            status: status.at(checked_at),
            code,
        })
    }
}

fn is_json_content_type(value: Option<&str>) -> bool {
    value
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
}

pub(super) fn response_content_length_exceeds_limit(content_length: Option<u64>) -> bool {
    content_length.is_some_and(|length| length > MODELS_DEV_MAX_RESPONSE_BYTES as u64)
}
