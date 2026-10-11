use std::net::SocketAddr;

use axum::{
    extract::{ConnectInfo, Request},
    http::{header::HeaderName, HeaderMap, HeaderValue},
    middleware::Next,
    response::{IntoResponse, Response},
};
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tracing::Instrument;

pub static REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

tokio::task_local! {
    static CURRENT_REQUEST_ID: String;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestId(String);

impl RequestId {
    fn new(value: String) -> Self {
        Self(value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

pub fn current_request_id() -> Option<String> {
    CURRENT_REQUEST_ID.try_with(Clone::clone).ok()
}

pub async fn request_id_middleware(mut req: Request, next: Next) -> Response {
    let request_id = request_id_from_headers(req.headers());
    let method = req.method().clone();
    let path = request_log_path(req.uri()).to_owned();
    req.extensions_mut()
        .insert(RequestId::new(request_id.clone()));

    let (client_addr, forwarded_for) = request_address_fields(&req);
    let span = tracing::info_span!(
        "http.request",
        request_id = %request_id,
        method = %method,
        path = %path,
        client_addr = client_addr.as_deref(),
        forwarded_for,
    );

    let mut response = CURRENT_REQUEST_ID
        .scope(request_id.clone(), next.run(req))
        .instrument(span)
        .await;

    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response
            .headers_mut()
            .insert(REQUEST_ID_HEADER.clone(), value);
    }

    response
}

pub(crate) fn request_log_path(uri: &axum::http::Uri) -> &str {
    uri.path()
}

pub(crate) fn request_address_fields(request: &Request) -> (Option<String>, Option<&str>) {
    let client_addr = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0.to_string());
    let forwarded_for = request
        .headers()
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            let end = value
                .char_indices()
                .nth(256)
                .map_or(value.len(), |(index, _)| index);
            &value[..end]
        });
    (client_addr, forwarded_for)
}

fn request_id_from_headers(headers: &HeaderMap) -> String {
    headers
        .get(&REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
}

pub fn cors_middleware(allowed_origins: &[String]) -> CorsLayer {
    let origins: Vec<axum::http::HeaderValue> = allowed_origins
        .iter()
        .filter_map(|o| axum::http::HeaderValue::from_str(o).ok())
        .collect();

    let allow_origin = if origins.is_empty() {
        AllowOrigin::any()
    } else {
        AllowOrigin::list(origins)
    };

    CorsLayer::new()
        .allow_origin(allow_origin)
        .allow_methods(Any)
        .allow_headers(Any)
}

pub async fn auth_middleware(
    state: axum::extract::State<crate::state::AppState>,
    mut req: Request,
    next: Next,
) -> Response {
    let path = req.uri().path().to_string();

    if is_auth_exempt(&path) {
        return next.run(req).await;
    }

    let token = extract_bearer_token(req.headers()).or_else(|| extract_query_token(req.uri()));

    let Some(token) = token else {
        return unauthorized_response("missing_token", "Authentication required");
    };

    let result = if token.starts_with("fg_") {
        state.auth_service.verify_pat(&token).await
    } else {
        state.auth_service.verify_token(&token)
    };

    match result {
        Ok((user_id, email, is_admin)) => {
            req.extensions_mut()
                .insert(crate::routes::auth::AuthenticatedUser {
                    user_id,
                    email,
                    is_admin,
                });
            next.run(req).await
        }
        Err(code) => unauthorized_response(&code, "Authentication failed"),
    }
}

pub async fn mcp_auth_middleware(
    state: axum::extract::State<crate::state::AppState>,
    mut req: Request,
    next: Next,
) -> Response {
    let token = extract_query_token(req.uri()).or_else(|| extract_bearer_token(req.headers()));

    let Some(token) = token else {
        return unauthorized_response_with_challenge(
            "missing_token",
            "Authentication required",
            state.effective_config.trusted_origin().as_str(),
        );
    };

    let result: Result<(String, String, bool), String> = if token.starts_with("fg_") {
        state.auth_service.verify_pat(&token).await
    } else if let Ok(claims) = state.auth_service.verify_token(&token) {
        Ok(claims)
    } else {
        match state.oauth_service.verify_mcp_access_token(&token) {
            Ok((user_id, email)) => Ok((user_id, email, false)),
            Err(_) => Err("invalid_token".to_string()),
        }
    };

    match result {
        Ok((user_id, _email, _is_admin)) => {
            req.extensions_mut().insert(mcp_server::McpUser { user_id });
            next.run(req).await
        }
        Err(_) => unauthorized_response_with_challenge(
            "invalid_token",
            "Authentication failed",
            state.effective_config.trusted_origin().as_str(),
        ),
    }
}

fn is_auth_exempt(path: &str) -> bool {
    matches!(
        path,
        "/api/v1/auth/register"
            | "/api/v1/auth/login"
            | "/api/v1/auth/refresh"
            | "/api/v1/auth/logout"
            | "/api/v1/health"
            | "/healthz"
    ) || path.starts_with("/api/v1/daemons/register")
        || path.starts_with("/api/v1/terminals/") && path.ends_with("/ws")
        || path.starts_with("/api/v1/daemons/") && path.ends_with("/report")
        || path.starts_with("/api/v1/daemons/") && path.ends_with("/connect")
        || path.starts_with("/.well-known")
        || path.starts_with("/oauth")
        || !path.starts_with("/api/")
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::to_owned)
}

fn extract_query_token(uri: &axum::http::Uri) -> Option<String> {
    uri.query().and_then(|q| {
        q.split('&')
            .find_map(|pair| pair.strip_prefix("token=").map(str::to_owned))
    })
}

fn unauthorized_response(code: &str, message: &str) -> Response {
    let body = serde_json::json!({
        "code": code,
        "message": message,
        "details": null,
        "request_id": crate::middleware::current_request_id().unwrap_or_default()
    });
    (axum::http::StatusCode::UNAUTHORIZED, axum::Json(body)).into_response()
}

fn unauthorized_response_with_challenge(
    code: &str,
    message: &str,
    trusted_origin: &str,
) -> Response {
    let body = serde_json::json!({
        "code": code,
        "message": message,
        "details": null,
        "request_id": current_request_id().unwrap_or_default(),
    });
    let challenge = format!(
        r#"Bearer resource_metadata="{trusted_origin}/.well-known/oauth-protected-resource", scope="mcp""#
    );
    let mut response = (axum::http::StatusCode::UNAUTHORIZED, axum::Json(body)).into_response();
    if let Ok(value) = HeaderValue::from_str(&challenge) {
        response
            .headers_mut()
            .insert(axum::http::header::WWW_AUTHENTICATE, value);
    }
    response
}

#[cfg(test)]
mod request_address_tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tower::ServiceExt;

    struct CapturedSpan {
        name: &'static str,
        fields: Vec<String>,
    }

    #[derive(Clone, Default)]
    struct SpanCapture(Arc<Mutex<Vec<CapturedSpan>>>);

    impl tracing::Subscriber for SpanCapture {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, attrs: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            let mut spans = self.0.lock().unwrap();
            spans.push(CapturedSpan {
                name: attrs.metadata().name(),
                fields: attrs
                    .metadata()
                    .fields()
                    .iter()
                    .map(|field| field.name().to_owned())
                    .collect(),
            });
            tracing::span::Id::from_u64(spans.len() as u64)
        }

        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, _: &tracing::Event<'_>) {}
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    #[test]
    fn request_address_fields_keep_peer_and_untrusted_forwarded_header_separate() {
        let peer: SocketAddr = "127.0.0.1:43210".parse().unwrap();
        let mut request = Request::builder()
            .header("x-forwarded-for", "198.51.100.8, 10.0.0.2")
            .body(axum::body::Body::empty())
            .unwrap();
        request.extensions_mut().insert(ConnectInfo(peer));
        let (client_addr, forwarded_for) = request_address_fields(&request);
        assert_eq!(client_addr.as_deref(), Some("127.0.0.1:43210"));
        assert_eq!(forwarded_for, Some("198.51.100.8, 10.0.0.2"));
    }

    #[test]
    fn request_address_fields_do_not_infer_peer_from_forwarded_header() {
        let request = Request::builder()
            .header("x-forwarded-for", "198.51.100.8")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            request_address_fields(&request),
            (None, Some("198.51.100.8"))
        );
        let request = Request::new(axum::body::Body::empty());
        assert_eq!(request_address_fields(&request), (None, None));
    }

    #[test]
    fn request_address_fields_bound_forwarded_for() {
        for length in [255, 256, 257, 4096] {
            let header = "a".repeat(length);
            let request = Request::builder()
                .header("x-forwarded-for", header.as_str())
                .body(axum::body::Body::empty())
                .unwrap();
            assert_eq!(
                request_address_fields(&request).1,
                Some(&header[..length.min(256)])
            );
        }
    }

    #[test]
    fn request_address_fields_ignore_invalid_forwarded_for() {
        let request = Request::builder()
            .header("x-forwarded-for", HeaderValue::from_bytes(b"\xff").unwrap())
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(request_address_fields(&request), (None, None));
    }

    #[test]
    fn request_address_fields_are_logged_on_one_span() {
        let capture = SpanCapture::default();
        // Keep two dispatchers alive so tracing registers callsites against its
        // subscriber registry. Its sole-dispatcher fast path uses the current
        // thread's subscriber, which may be absent in another parallel test.
        let _other_dispatch = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        let dispatch = tracing::Dispatch::new(capture.clone());
        // Cover router construction as well as every poll of the request. A
        // current-thread runtime keeps the scoped subscriber on this thread.
        tracing::dispatcher::with_default(&dispatch, || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let web_dist = crate::temp_web_dist();
                    let app = crate::build_router(crate::test_state().await, &web_dist);
                    let mut request = Request::builder()
                        .uri("/api/v1/auth/me")
                        .header("x-forwarded-for", "198.51.100.8")
                        .body(axum::body::Body::empty())
                        .unwrap();
                    request.extensions_mut().insert(ConnectInfo(
                        "127.0.0.1:43210".parse::<SocketAddr>().unwrap(),
                    ));

                    let response = app.oneshot(request).await.unwrap();
                    assert_eq!(response.status(), axum::http::StatusCode::UNAUTHORIZED);
                    std::fs::remove_dir_all(web_dist).unwrap();
                });
        });

        let spans = capture.0.lock().unwrap();
        assert!(spans.iter().any(|span| span.name == "http.request"));
        assert!(spans.iter().any(|span| span.name == "http.trace"));
        for address_field in ["client_addr", "forwarded_for"] {
            let address_spans: Vec<_> = spans
                .iter()
                .filter(|span| span.fields.iter().any(|field| field == address_field))
                .map(|span| span.name)
                .collect();
            assert_eq!(address_spans, ["http.request"]);
        }
    }
}
