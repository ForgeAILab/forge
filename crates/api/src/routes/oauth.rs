use api_types::{
    OAuthApproveRequest, OAuthApproveResponse, OAuthAuthorizeQuery, OAuthErrorResponse,
    OAuthRegisterRequest, OAuthTokenRequest,
};
use axum::{
    extract::{Form, Query, State},
    http::{header, HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Redirect, Response},
    Json,
};
use db::OAuthClientRepo;
use services::OAuthError;
use url::Url;

use crate::{errors::ApiError, routes::auth::AuthenticatedUser, state::AppState};

const OAUTH_AUTHORIZE_UI_PATH: &str = "/oauth/authorize/consent";

pub async fn protected_resource_metadata(State(state): State<AppState>) -> Response {
    Json(
        state
            .oauth_service
            .protected_resource_metadata(state.effective_config.trusted_origin().as_str()),
    )
    .into_response()
}

pub async fn authorization_server_metadata(State(state): State<AppState>) -> Response {
    Json(
        state
            .oauth_service
            .authorization_server_metadata(state.effective_config.trusted_origin().as_str()),
    )
    .into_response()
}

pub async fn register_public_client(
    State(state): State<AppState>,
    Json(body): Json<OAuthRegisterRequest>,
) -> Response {
    match state.oauth_service.register_public_client(body).await {
        Ok(response) => (StatusCode::CREATED, Json(response)).into_response(),
        Err(error) => oauth_error_response(&error),
    }
}

pub async fn authorize(
    State(state): State<AppState>,
    uri: Uri,
    Query(query): Query<OAuthAuthorizeQuery>,
) -> Response {
    match state.oauth_service.build_authorize_context(&query).await {
        Ok(_) => {
            let location = match uri.query() {
                Some(query_string) if !query_string.is_empty() => {
                    format!("{OAUTH_AUTHORIZE_UI_PATH}?{query_string}")
                }
                _ => OAUTH_AUTHORIZE_UI_PATH.to_string(),
            };
            Redirect::to(&location).into_response()
        }
        Err(error) => {
            if authorize_error_can_redirect(&state, &query, &error).await {
                let redirect_uri = query
                    .redirect_uri
                    .as_deref()
                    .expect("redirect URI was validated before redirecting");
                Redirect::to(&oauth_error_redirect_url(
                    redirect_uri,
                    &error,
                    query.state.as_deref(),
                ))
                .into_response()
            } else {
                oauth_error_response(&error)
            }
        }
    }
}

pub async fn authorize_context(
    State(state): State<AppState>,
    Query(query): Query<OAuthAuthorizeQuery>,
) -> Response {
    match state.oauth_service.build_authorize_context(&query).await {
        Ok(context) => Json(context).into_response(),
        Err(error) => oauth_error_to_api(error).into_response(),
    }
}

pub async fn authorize_approve(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    headers: HeaderMap,
    Json(body): Json<OAuthApproveRequest>,
) -> Response {
    let trusted_origin = state.effective_config.trusted_origin();
    let origin_matches = headers
        .get(header::ORIGIN)
        .is_some_and(|origin| origin_matches_trusted(origin, &trusted_origin));

    if !origin_matches {
        return ApiError::forbidden_with_code("origin_mismatch", "OAuth approval origin mismatch")
            .into_response();
    }

    match state
        .oauth_service
        .approve_or_deny(body, &user.user_id, &user.email)
        .await
    {
        Ok(redirect_to) => Json(OAuthApproveResponse { redirect_to }).into_response(),
        Err(error) => oauth_error_to_api(error).into_response(),
    }
}

pub async fn token(State(state): State<AppState>, Form(body): Form<OAuthTokenRequest>) -> Response {
    match state.oauth_service.exchange_token(body).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(error) => oauth_error_response(&error),
    }
}

fn oauth_error_response(error: &OAuthError) -> Response {
    (
        oauth_error_status(error),
        Json(OAuthErrorResponse {
            error: oauth_error_code(error).to_string(),
            error_description: Some(error.to_string()),
        }),
    )
        .into_response()
}

fn oauth_error_to_api(error: OAuthError) -> ApiError {
    match error {
        OAuthError::InvalidRequest(message) => {
            ApiError::bad_request_with_code("invalid_request", message)
        }
        OAuthError::InvalidClient => {
            ApiError::bad_request_with_code("invalid_client", "Invalid OAuth client")
        }
        OAuthError::InvalidRedirectUri => {
            ApiError::bad_request_with_code("invalid_request", "Invalid OAuth redirect URI")
        }
        OAuthError::InvalidGrant(message) => {
            ApiError::bad_request_with_code("invalid_grant", message)
        }
        OAuthError::InvalidScope => {
            ApiError::bad_request_with_code("invalid_scope", "Invalid OAuth scope")
        }
        OAuthError::ServerError(message) => ApiError::internal(message),
        OAuthError::RateLimited => ApiError::too_many_requests_with_code(
            "rate_limited",
            "OAuth client registration rate limit exceeded",
        ),
    }
}

fn oauth_error_status(error: &OAuthError) -> StatusCode {
    match error {
        OAuthError::ServerError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        OAuthError::RateLimited => StatusCode::TOO_MANY_REQUESTS,
        OAuthError::InvalidRequest(_)
        | OAuthError::InvalidClient
        | OAuthError::InvalidRedirectUri
        | OAuthError::InvalidGrant(_)
        | OAuthError::InvalidScope => StatusCode::BAD_REQUEST,
    }
}

fn oauth_error_code(error: &OAuthError) -> &'static str {
    match error {
        OAuthError::InvalidRequest(_) | OAuthError::InvalidRedirectUri => "invalid_request",
        OAuthError::InvalidClient => "invalid_client",
        OAuthError::InvalidGrant(_) => "invalid_grant",
        OAuthError::InvalidScope => "invalid_scope",
        OAuthError::ServerError(_) => "server_error",
        OAuthError::RateLimited => "rate_limited",
    }
}

async fn authorize_error_can_redirect(
    state: &AppState,
    query: &OAuthAuthorizeQuery,
    error: &OAuthError,
) -> bool {
    if matches!(
        error,
        OAuthError::InvalidClient | OAuthError::InvalidRedirectUri
    ) {
        return false;
    }

    let (Some(client_id), Some(redirect_uri)) =
        (query.client_id.as_deref(), query.redirect_uri.as_deref())
    else {
        return false;
    };

    let Ok(Some(client)) = OAuthClientRepo::get_client(&*state.db, client_id).await else {
        return false;
    };
    let Ok(redirect_uris) = serde_json::from_str::<Vec<String>>(&client.redirect_uris_json) else {
        return false;
    };

    redirect_uris
        .iter()
        .any(|registered_uri| registered_uri == redirect_uri)
}

fn oauth_error_redirect_url(redirect_uri: &str, error: &OAuthError, state: Option<&str>) -> String {
    if let Ok(mut url) = Url::parse(redirect_uri) {
        {
            let mut query_pairs = url.query_pairs_mut();
            query_pairs.append_pair("error", oauth_error_code(error));
            if let Some(state) = state {
                query_pairs.append_pair("state", state);
            }
        }
        return url.to_string();
    }

    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    serializer.append_pair("error", oauth_error_code(error));
    if let Some(state) = state {
        serializer.append_pair("state", state);
    }
    let separator = if redirect_uri.contains('?') { "&" } else { "?" };
    format!("{redirect_uri}{separator}{}", serializer.finish())
}

fn origin_matches_trusted(origin: &axum::http::HeaderValue, trusted_origin: &str) -> bool {
    let Ok(origin) = origin.to_str() else {
        return false;
    };
    let (Ok(origin), Ok(trusted)) = (Url::parse(origin), Url::parse(trusted_origin)) else {
        return false;
    };

    origin.scheme() == trusted.scheme()
        && origin.host_str() == trusted.host_str()
        && origin.port_or_known_default() == trusted.port_or_known_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use api_types::{OAuthRegisterResponse, OAuthTokenResponse};
    use axum::{
        body::{to_bytes, Body},
        http::{Method, Request},
    };
    use db::{now_rfc3339, OAuthRefreshTokenRepo, User, UserRepo};
    use serde_json::{json, Value};
    use sha2::{Digest, Sha256};
    use sqlx::Row;
    use tower::ServiceExt;

    const REDIRECT_URI: &str = "http://127.0.0.1/callback";
    const CODE_VERIFIER: &str = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
    const CODE_CHALLENGE: &str = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";

    #[tokio::test]
    async fn discovery_metadata_uses_trusted_origin() {
        let state = trusted_origin_state().await;

        let protected = response_json(
            protected_resource_metadata(State(state.clone())).await,
            StatusCode::OK,
        )
        .await;
        assert_eq!(protected["resource"], "https://forge.example.com/mcp");
        assert_eq!(
            protected["authorization_servers"],
            json!(["https://forge.example.com"])
        );

        let authorization = response_json(
            authorization_server_metadata(State(state)).await,
            StatusCode::OK,
        )
        .await;
        assert_eq!(authorization["issuer"], "https://forge.example.com");
        assert_eq!(
            authorization["authorization_endpoint"],
            "https://forge.example.com/oauth/authorize"
        );
        assert_eq!(
            authorization["token_endpoint"],
            "https://forge.example.com/oauth/token"
        );
        assert_eq!(
            authorization["registration_endpoint"],
            "https://forge.example.com/oauth/register"
        );
    }

    #[tokio::test]
    async fn discovery_metadata_ignores_hostile_host_header() {
        let state = trusted_origin_state().await;
        let app = crate::build_router(state, crate::temp_web_dist());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/.well-known/oauth-authorization-server")
                    .header(header::HOST, "evil.example")
                    .body(Body::empty())
                    .expect("build discovery request"),
            )
            .await
            .expect("router response");

        let body = response_json(response, StatusCode::OK).await;
        let body_text = body.to_string();
        assert!(body_text.contains("https://forge.example.com"));
        assert!(!body_text.contains("evil.example"));
    }

    #[tokio::test]
    async fn register_persists_client() {
        let state = crate::test_state().await;
        let app = crate::build_router(state.clone(), crate::temp_web_dist());

        let response = register_client(&app, REDIRECT_URI, StatusCode::CREATED).await;
        let client_id = response["client_id"].as_str().expect("client_id");
        let client = OAuthClientRepo::get_client(&*state.db, client_id)
            .await
            .expect("get client")
            .expect("client persisted");

        assert_eq!(client.client_id, client_id);
        assert_eq!(client.redirect_uris_json, json!([REDIRECT_URI]).to_string());
        assert_eq!(response["token_endpoint_auth_method"], "none");
    }

    #[tokio::test]
    async fn register_rejects_unsafe_redirect_uri() {
        let state = crate::test_state().await;
        let app = crate::build_router(state.clone(), crate::temp_web_dist());

        let response =
            register_client(&app, "http://example.com/cb", StatusCode::BAD_REQUEST).await;

        assert_eq!(response["error"], "invalid_request");
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM oauth_client")
            .fetch_one(state.db.pool())
            .await
            .expect("count clients");
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn register_rate_limit_returns_429() {
        let state = crate::test_state().await;
        let app = crate::build_router(state, crate::temp_web_dist());

        for _ in 0..30 {
            register_client(&app, REDIRECT_URI, StatusCode::CREATED).await;
        }
        let response = register_client(&app, REDIRECT_URI, StatusCode::TOO_MANY_REQUESTS).await;

        assert_eq!(response["error"], "rate_limited");
    }

    #[tokio::test]
    async fn authorize_redirects_valid_request_to_spa_consent_route() {
        let state = crate::test_state().await;
        let app = crate::build_router(state.clone(), crate::temp_web_dist());
        let client = register_client_response(&app, REDIRECT_URI).await;
        let resource = state.effective_config.mcp_resource_url();
        let mut query_builder = url::form_urlencoded::Serializer::new(String::new());
        query_builder
            .append_pair("response_type", "code")
            .append_pair("client_id", &client.client_id)
            .append_pair("redirect_uri", REDIRECT_URI)
            .append_pair("resource", &resource)
            .append_pair("scope", "mcp")
            .append_pair("state", "state-1")
            .append_pair("code_challenge", CODE_CHALLENGE)
            .append_pair("code_challenge_method", "S256");
        let query = query_builder.finish();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/oauth/authorize?{query}"))
                    .body(Body::empty())
                    .expect("build authorize request"),
            )
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        let location = response
            .headers()
            .get(header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .expect("location header");
        assert_eq!(location, format!("/oauth/authorize/consent?{query}"));
    }

    #[tokio::test]
    async fn approve_rejects_cross_origin() {
        let state = crate::test_state().await;
        let app = crate::build_router(state.clone(), crate::temp_web_dist());
        let client = register_client_response(&app, REDIRECT_URI).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/oauth/authorize/approve")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", crate::test_jwt(&state)),
                    )
                    .header(header::ORIGIN, "https://evil.example")
                    .body(Body::from(
                        serde_json::to_string(&approve_body(&state, &client.client_id, Some("s1")))
                            .expect("serialize approve body"),
                    ))
                    .expect("build approve request"),
            )
            .await
            .expect("router response");

        let body = response_json(response, StatusCode::FORBIDDEN).await;
        assert_eq!(body["code"], "origin_mismatch");
    }

    #[tokio::test]
    async fn approve_creates_code_and_redirect() {
        let state = state_with_seed_user().await;
        let app = crate::build_router(state.clone(), crate::temp_web_dist());
        let client = register_client_response(&app, REDIRECT_URI).await;

        let response = approve(
            &app,
            &state,
            &client.client_id,
            Some("state-1"),
            StatusCode::OK,
        )
        .await;
        let redirect_to = response["redirect_to"].as_str().expect("redirect_to");
        let redirect = Url::parse(redirect_to).expect("parse redirect");
        let pairs = redirect.query_pairs().collect::<Vec<_>>();

        assert!(pairs
            .iter()
            .any(|(key, value)| key.as_ref() == "code" && !value.is_empty()));
        assert!(pairs
            .iter()
            .any(|(key, value)| key.as_ref() == "state" && value == "state-1"));
    }

    #[tokio::test]
    async fn token_authorization_code_grant_succeeds() {
        let state = state_with_seed_user().await;
        let app = crate::build_router(state.clone(), crate::temp_web_dist());
        let client = register_client_response(&app, REDIRECT_URI).await;
        let code = approve_code(&app, &state, &client.client_id, Some("state-1")).await;

        let token_response = exchange_code(
            &app,
            &state,
            &client.client_id,
            &code,
            CODE_VERIFIER,
            StatusCode::OK,
        )
        .await;
        assert!(token_response["access_token"]
            .as_str()
            .is_some_and(|token| !token.is_empty()));
        assert!(token_response["refresh_token"]
            .as_str()
            .is_some_and(|token| !token.is_empty()));
        assert_eq!(token_response["expires_in"], 3600);
        assert_eq!(token_response["scope"], "mcp");

        let reuse = exchange_code(
            &app,
            &state,
            &client.client_id,
            &code,
            CODE_VERIFIER,
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_eq!(reuse["error"], "invalid_grant");
    }

    #[tokio::test]
    async fn token_pkce_mismatch_returns_invalid_grant() {
        let state = state_with_seed_user().await;
        let app = crate::build_router(state.clone(), crate::temp_web_dist());
        let client = register_client_response(&app, REDIRECT_URI).await;
        let code = approve_code(&app, &state, &client.client_id, None).await;

        let response = exchange_code(
            &app,
            &state,
            &client.client_id,
            &code,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            StatusCode::BAD_REQUEST,
        )
        .await;

        assert_eq!(response["error"], "invalid_grant");
    }

    #[tokio::test]
    async fn token_refresh_rotates_and_revokes_old() {
        let state = state_with_seed_user().await;
        let app = crate::build_router(state.clone(), crate::temp_web_dist());
        let client = register_client_response(&app, REDIRECT_URI).await;
        let code = approve_code(&app, &state, &client.client_id, None).await;
        let token_response: OAuthTokenResponse = serde_json::from_value(
            exchange_code(
                &app,
                &state,
                &client.client_id,
                &code,
                CODE_VERIFIER,
                StatusCode::OK,
            )
            .await,
        )
        .expect("token response");
        let old_refresh_token = token_response.refresh_token;
        let old_token_hash = sha256_hex(&old_refresh_token);

        let rotated =
            refresh_token(&app, &client.client_id, &old_refresh_token, StatusCode::OK).await;
        let new_refresh_token = rotated["refresh_token"]
            .as_str()
            .expect("new refresh_token");
        assert_ne!(new_refresh_token, old_refresh_token.as_str());

        let reused = refresh_token(
            &app,
            &client.client_id,
            &old_refresh_token,
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_eq!(reused["error"], "invalid_grant");

        let family_id: String =
            sqlx::query("SELECT family_id FROM oauth_refresh_token WHERE token_hash = ?")
                .bind(old_token_hash)
                .fetch_one(state.db.pool())
                .await
                .expect("refresh token row")
                .get("family_id");
        let active_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM oauth_refresh_token WHERE family_id = ? AND revoked_at IS NULL",
        )
        .bind(&family_id)
        .fetch_one(state.db.pool())
        .await
        .expect("count active refresh tokens");
        assert_eq!(active_count, 0);

        let old = OAuthRefreshTokenRepo::get_refresh_token_by_hash(
            &*state.db,
            &sha256_hex(&old_refresh_token),
        )
        .await
        .expect("get old refresh token")
        .expect("old refresh token row");
        assert!(old.revoked_at.is_some());
    }

    async fn trusted_origin_state() -> AppState {
        let state = crate::test_state().await;
        let mut config = (*state.effective_config).clone();
        config.server.public_base_url = Some("https://forge.example.com/app".to_string());
        state.with_effective_config(config)
    }

    async fn state_with_seed_user() -> AppState {
        let state = crate::test_state().await;
        seed_test_user(&state).await;
        state
    }

    async fn seed_test_user(state: &AppState) {
        let now = now_rfc3339();
        UserRepo::create_user(
            &*state.db,
            &User {
                id: "test-user-id".to_string(),
                email: "test@example.com".to_string(),
                password_hash: "$2b$04$placeholder".to_string(),
                display_name: None,
                is_admin: false,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("seed test user");
    }

    async fn register_client(
        app: &axum::Router,
        redirect_uri: &str,
        expected_status: StatusCode,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/oauth/register")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_string(&json!({
                            "client_name": "Test MCP Client",
                            "redirect_uris": [redirect_uri],
                        }))
                        .expect("serialize register body"),
                    ))
                    .expect("build register request"),
            )
            .await
            .expect("router response");

        response_json(response, expected_status).await
    }

    async fn register_client_response(
        app: &axum::Router,
        redirect_uri: &str,
    ) -> OAuthRegisterResponse {
        serde_json::from_value(register_client(app, redirect_uri, StatusCode::CREATED).await)
            .expect("register response")
    }

    fn approve_body(state: &AppState, client_id: &str, decision_state: Option<&str>) -> Value {
        json!({
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": REDIRECT_URI,
            "resource": state.effective_config.mcp_resource_url(),
            "scope": "mcp",
            "state": decision_state,
            "code_challenge": CODE_CHALLENGE,
            "code_challenge_method": "S256",
            "decision": "approve",
        })
    }

    async fn approve(
        app: &axum::Router,
        state: &AppState,
        client_id: &str,
        decision_state: Option<&str>,
        expected_status: StatusCode,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/oauth/authorize/approve")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", crate::test_jwt(state)),
                    )
                    .header(header::ORIGIN, state.effective_config.trusted_origin())
                    .body(Body::from(
                        serde_json::to_string(&approve_body(state, client_id, decision_state))
                            .expect("serialize approve body"),
                    ))
                    .expect("build approve request"),
            )
            .await
            .expect("router response");

        response_json(response, expected_status).await
    }

    async fn approve_code(
        app: &axum::Router,
        state: &AppState,
        client_id: &str,
        decision_state: Option<&str>,
    ) -> String {
        let response = approve(app, state, client_id, decision_state, StatusCode::OK).await;
        let redirect_to = response["redirect_to"].as_str().expect("redirect_to");
        let redirect = Url::parse(redirect_to).expect("parse redirect");
        redirect
            .query_pairs()
            .find_map(|(key, value)| (key.as_ref() == "code").then(|| value.into_owned()))
            .expect("code query param")
    }

    async fn exchange_code(
        app: &axum::Router,
        state: &AppState,
        client_id: &str,
        code: &str,
        code_verifier: &str,
        expected_status: StatusCode,
    ) -> Value {
        let resource = state.effective_config.mcp_resource_url();
        token_request(
            app,
            &[
                ("grant_type", "authorization_code"),
                ("code", code),
                ("redirect_uri", REDIRECT_URI),
                ("client_id", client_id),
                ("code_verifier", code_verifier),
                ("resource", resource.as_str()),
            ],
            expected_status,
        )
        .await
    }

    async fn refresh_token(
        app: &axum::Router,
        client_id: &str,
        refresh_token: &str,
        expected_status: StatusCode,
    ) -> Value {
        token_request(
            app,
            &[
                ("grant_type", "refresh_token"),
                ("client_id", client_id),
                ("refresh_token", refresh_token),
            ],
            expected_status,
        )
        .await
    }

    async fn token_request(
        app: &axum::Router,
        params: &[(&str, &str)],
        expected_status: StatusCode,
    ) -> Value {
        let body = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(params.iter().copied())
            .finish();
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/oauth/token")
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(Body::from(body))
                    .expect("build token request"),
            )
            .await
            .expect("router response");

        response_json(response, expected_status).await
    }

    async fn response_json(response: Response, expected_status: StatusCode) -> Value {
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");
        assert_eq!(
            status,
            expected_status,
            "unexpected response status with body: {}",
            String::from_utf8_lossy(&body)
        );
        serde_json::from_slice(&body).expect("parse JSON body")
    }

    fn sha256_hex(input: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(input.as_bytes());
        hex::encode(hasher.finalize())
    }
}
