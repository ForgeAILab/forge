use std::sync::Arc;

use api_types::{
    McpAccessTokenClaims, OAuthApproveRequest, OAuthAuthorizationServerMetadata,
    OAuthAuthorizeContext, OAuthAuthorizeQuery, OAuthDecision, OAuthProtectedResourceMetadata,
    OAuthRegisterRequest, OAuthRegisterResponse, OAuthTokenRequest, OAuthTokenResponse,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use db::{
    new_uuid_v4, CreateOAuthAuthorizationCode, CreateOAuthClient, CreateOAuthRefreshToken,
    OAuthAuthorizationCodeRepo, OAuthClientRepo, OAuthRefreshTokenRepo, SqliteDb, UserRepo,
};
use rand::RngCore;
use sha2::{Digest, Sha256};
use sqlx::{Sqlite, Transaction};
use url::Url;

use crate::AuthService;

const ACCESS_TOKEN_EXPIRES_IN_SECS: u64 = 3600;
const AUTHORIZATION_CODE_TTL_MINUTES: i64 = 5;
const REFRESH_TOKEN_TTL_DAYS: i64 = 7;
const REFRESH_TOKEN_FAMILY_TTL_DAYS: i64 = 30;

#[derive(Clone)]
pub struct OAuthService {
    db: Arc<SqliteDb>,
    auth_service: Arc<AuthService>,
    mcp_resource: String,
    registration_rate_limit_per_hour: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum OAuthError {
    #[error("invalid_request: {0}")]
    InvalidRequest(String),
    #[error("invalid_client")]
    InvalidClient,
    #[error("invalid_redirect_uri")]
    InvalidRedirectUri,
    #[error("invalid_grant: {0}")]
    InvalidGrant(String),
    #[error("invalid_scope")]
    InvalidScope,
    #[error("server_error: {0}")]
    ServerError(String),
    #[error("rate_limited")]
    RateLimited,
}

impl OAuthError {
    #[must_use]
    pub fn oauth_error_code(&self) -> &'static str {
        match self {
            Self::InvalidRequest(_) => "invalid_request",
            Self::InvalidClient => "invalid_client",
            Self::InvalidRedirectUri => "invalid_request",
            Self::InvalidGrant(_) => "invalid_grant",
            Self::InvalidScope => "invalid_scope",
            Self::ServerError(_) => "server_error",
            Self::RateLimited => "invalid_request",
        }
    }
}

impl From<db::DbError> for OAuthError {
    fn from(error: db::DbError) -> Self {
        Self::ServerError(error.to_string())
    }
}

impl From<sqlx::Error> for OAuthError {
    fn from(error: sqlx::Error) -> Self {
        Self::ServerError(error.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct AuthorizationGrant {
    pub user_id: String,
}

#[derive(Debug, Clone)]
pub struct IssuedTokenPair {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_in: u64,
    pub scope: String,
}

impl OAuthService {
    #[must_use]
    pub fn new(db: Arc<SqliteDb>, auth_service: Arc<AuthService>, mcp_resource: String) -> Self {
        Self {
            db,
            auth_service,
            mcp_resource,
            registration_rate_limit_per_hour: 30,
        }
    }

    #[must_use]
    pub fn mcp_resource(&self) -> &str {
        &self.mcp_resource
    }

    #[must_use]
    pub fn protected_resource_metadata(
        &self,
        trusted_origin: &str,
    ) -> OAuthProtectedResourceMetadata {
        OAuthProtectedResourceMetadata {
            resource: self.mcp_resource.clone(),
            authorization_servers: vec![trim_trailing_slash(trusted_origin).to_string()],
            scopes_supported: vec!["mcp".to_string()],
            bearer_methods_supported: vec!["header".to_string()],
            resource_documentation: None,
        }
    }

    #[must_use]
    pub fn authorization_server_metadata(
        &self,
        trusted_origin: &str,
    ) -> OAuthAuthorizationServerMetadata {
        let trusted_origin = trim_trailing_slash(trusted_origin);
        OAuthAuthorizationServerMetadata {
            issuer: trusted_origin.to_string(),
            authorization_endpoint: format!("{trusted_origin}/oauth/authorize"),
            token_endpoint: format!("{trusted_origin}/oauth/token"),
            registration_endpoint: format!("{trusted_origin}/oauth/register"),
            response_types_supported: vec!["code".to_string()],
            grant_types_supported: vec![
                "authorization_code".to_string(),
                "refresh_token".to_string(),
            ],
            code_challenge_methods_supported: vec!["S256".to_string()],
            token_endpoint_auth_methods_supported: vec!["none".to_string()],
            scopes_supported: vec!["mcp".to_string()],
        }
    }

    pub async fn register_public_client(
        &self,
        request: OAuthRegisterRequest,
    ) -> Result<OAuthRegisterResponse, OAuthError> {
        if request.redirect_uris.is_empty() {
            return Err(OAuthError::InvalidRequest("redirect_uris_required".into()));
        }
        if request
            .redirect_uris
            .iter()
            .any(|redirect_uri| !is_allowed_redirect_uri(redirect_uri))
        {
            return Err(OAuthError::InvalidRedirectUri);
        }
        if request
            .client_name
            .as_ref()
            .is_some_and(|client_name| client_name.chars().count() > 200)
        {
            return Err(OAuthError::InvalidRequest("client_name_too_long".into()));
        }

        let now = Utc::now();
        let window_start = format_rfc3339(now - Duration::hours(1));
        let registered_in_window =
            OAuthClientRepo::count_clients_created_since(&*self.db, &window_start).await?;
        if registered_in_window >= self.registration_rate_limit_per_hour {
            return Err(OAuthError::RateLimited);
        }

        let client_id = new_uuid_v4();
        let redirect_uris_json = serde_json::to_string(&request.redirect_uris)
            .map_err(|error| OAuthError::ServerError(error.to_string()))?;
        let created_at = format_rfc3339(now);
        let client = OAuthClientRepo::create_client(
            &*self.db,
            CreateOAuthClient {
                id: new_uuid_v4(),
                client_id: client_id.clone(),
                client_name: request.client_name.clone(),
                redirect_uris_json,
                token_endpoint_auth_method: "none".to_string(),
                created_at,
            },
        )
        .await?;

        Ok(OAuthRegisterResponse {
            client_id,
            client_id_issued_at: now.timestamp() as u64,
            client_name: client.client_name,
            redirect_uris: request.redirect_uris,
            grant_types: vec![
                "authorization_code".to_string(),
                "refresh_token".to_string(),
            ],
            response_types: vec!["code".to_string()],
            token_endpoint_auth_method: client.token_endpoint_auth_method,
            scope: "mcp".to_string(),
        })
    }

    pub async fn build_authorize_context(
        &self,
        query: &OAuthAuthorizeQuery,
    ) -> Result<OAuthAuthorizeContext, OAuthError> {
        let response_type = required(query.response_type.as_deref(), "response_type")?;
        if response_type != "code" {
            return Err(OAuthError::InvalidRequest(
                "unsupported_response_type".into(),
            ));
        }

        let client_id = required(query.client_id.as_deref(), "client_id")?;
        let redirect_uri = required(query.redirect_uri.as_deref(), "redirect_uri")?;
        let resource = required(query.resource.as_deref(), "resource")?;
        let scope = required(query.scope.as_deref(), "scope")?;
        let code_challenge = required(query.code_challenge.as_deref(), "code_challenge")?;
        let code_challenge_method = required(
            query.code_challenge_method.as_deref(),
            "code_challenge_method",
        )?;

        if code_challenge_method != "S256" {
            return Err(OAuthError::InvalidRequest(
                "invalid_code_challenge_method".into(),
            ));
        }
        if !(43..=128).contains(&code_challenge.len()) {
            return Err(OAuthError::InvalidRequest(
                "invalid_code_challenge_length".into(),
            ));
        }

        let scopes = parse_scopes(scope);
        if !scopes.iter().any(|scope| scope == "mcp") {
            return Err(OAuthError::InvalidScope);
        }
        if resource != self.mcp_resource {
            return Err(OAuthError::InvalidRequest("invalid_resource".into()));
        }

        let client = OAuthClientRepo::get_client(&*self.db, client_id)
            .await?
            .ok_or(OAuthError::InvalidClient)?;
        let redirect_uris: Vec<String> = serde_json::from_str(&client.redirect_uris_json)
            .map_err(|error| OAuthError::ServerError(error.to_string()))?;
        if !redirect_uris
            .iter()
            .any(|registered| registered == redirect_uri)
        {
            return Err(OAuthError::InvalidRedirectUri);
        }

        Ok(OAuthAuthorizeContext {
            client_id: client.client_id,
            client_name: client.client_name,
            redirect_uri: redirect_uri.to_string(),
            resource: resource.to_string(),
            scopes,
        })
    }

    pub async fn approve_or_deny(
        &self,
        request: OAuthApproveRequest,
        user_id: &str,
        _user_email: &str,
    ) -> Result<String, OAuthError> {
        let query = OAuthAuthorizeQuery {
            response_type: Some(request.response_type.clone()),
            client_id: Some(request.client_id.clone()),
            redirect_uri: Some(request.redirect_uri.clone()),
            resource: Some(request.resource.clone()),
            scope: Some(request.scope.clone()),
            state: request.state.clone(),
            code_challenge: Some(request.code_challenge.clone()),
            code_challenge_method: Some(request.code_challenge_method.clone()),
        };
        let context = self.build_authorize_context(&query).await?;

        if matches!(&request.decision, OAuthDecision::Deny) {
            let mut params = vec![("error", "access_denied")];
            if let Some(state) = request.state.as_deref() {
                params.push(("state", state));
            }
            return Ok(build_redirect_url(&context.redirect_uri, &params));
        }

        let raw_code = random_hex_32();
        let code_hash = sha256_hex(&raw_code);
        let now = Utc::now();
        let created_at = format_rfc3339(now);
        let expires_at = format_rfc3339(now + Duration::minutes(AUTHORIZATION_CODE_TTL_MINUTES));

        OAuthAuthorizationCodeRepo::create_code(
            &*self.db,
            CreateOAuthAuthorizationCode {
                id: new_uuid_v4(),
                code_hash,
                user_id: user_id.to_string(),
                client_id: context.client_id.clone(),
                redirect_uri: context.redirect_uri.clone(),
                code_challenge: request.code_challenge,
                code_challenge_method: request.code_challenge_method,
                resource: context.resource.clone(),
                scopes: context.scopes.join(" "),
                expires_at,
                created_at,
            },
        )
        .await?;

        let mut params = vec![("code", raw_code.as_str())];
        if let Some(state) = request.state.as_deref() {
            params.push(("state", state));
        }
        Ok(build_redirect_url(&context.redirect_uri, &params))
    }

    pub async fn exchange_token(
        &self,
        request: OAuthTokenRequest,
    ) -> Result<OAuthTokenResponse, OAuthError> {
        match request.grant_type.as_str() {
            "authorization_code" => self.exchange_authorization_code(request).await,
            "refresh_token" => self.exchange_refresh_token(request).await,
            _ => Err(OAuthError::InvalidRequest(
                "unsupported_grant_type".to_string(),
            )),
        }
    }

    pub fn verify_mcp_access_token(&self, bearer: &str) -> Result<(String, String), OAuthError> {
        let claims = self
            .auth_service
            .verify_mcp_token(bearer)
            .map_err(OAuthError::InvalidGrant)?;

        if claims.token_use != "mcp"
            || claims.aud != self.mcp_resource
            || !parse_scopes(&claims.scope)
                .iter()
                .any(|scope| scope == "mcp")
        {
            return Err(OAuthError::InvalidGrant("invalid_token".into()));
        }

        Ok((claims.sub, claims.email))
    }

    async fn exchange_authorization_code(
        &self,
        request: OAuthTokenRequest,
    ) -> Result<OAuthTokenResponse, OAuthError> {
        let code = required(request.code.as_deref(), "code")?;
        let client_id = required(request.client_id.as_deref(), "client_id")?;
        let redirect_uri = required(request.redirect_uri.as_deref(), "redirect_uri")?;
        let code_verifier = required(request.code_verifier.as_deref(), "code_verifier")?;

        if let Some(resource) = request.resource.as_deref() {
            if resource != self.mcp_resource {
                return Err(OAuthError::InvalidRequest("invalid_resource".into()));
            }
        }
        if !(43..=128).contains(&code_verifier.len()) {
            return Err(OAuthError::InvalidRequest("invalid_verifier_length".into()));
        }

        let code_hash = sha256_hex(code);
        let authorization_code =
            OAuthAuthorizationCodeRepo::get_code_by_hash(&*self.db, &code_hash)
                .await?
                .ok_or_else(|| OAuthError::InvalidGrant("invalid_code".into()))?;

        let now = format_rfc3339(Utc::now());
        if authorization_code.expires_at < now || authorization_code.consumed_at.is_some() {
            return Err(OAuthError::InvalidGrant("invalid_code".into()));
        }
        if authorization_code.client_id != client_id {
            return Err(OAuthError::InvalidGrant("client_mismatch".into()));
        }
        if authorization_code.redirect_uri != redirect_uri {
            return Err(OAuthError::InvalidGrant("redirect_mismatch".into()));
        }

        let challenge_digest = Sha256::digest(code_verifier.as_bytes());
        let expected_challenge = base64url_no_pad(&challenge_digest);
        if expected_challenge != authorization_code.code_challenge {
            return Err(OAuthError::InvalidGrant("pkce_mismatch".into()));
        }

        let consumed =
            OAuthAuthorizationCodeRepo::mark_code_consumed(&*self.db, &authorization_code.id, &now)
                .await?;
        if !consumed {
            return Err(OAuthError::InvalidGrant("invalid_code".into()));
        }

        let user = UserRepo::get_user_by_id(&*self.db, &authorization_code.user_id)
            .await?
            .ok_or_else(|| OAuthError::InvalidGrant("user_missing".into()))?;

        let pair = self
            .issue_token_pair(
                &user.id,
                &user.email,
                &authorization_code.client_id,
                &authorization_code.resource,
                &authorization_code.scopes,
                &new_uuid_v4(),
            )
            .await?;

        Ok(OAuthTokenResponse {
            access_token: pair.access_token,
            token_type: "Bearer".to_string(),
            expires_in: pair.expires_in,
            refresh_token: pair.refresh_token,
            scope: pair.scope,
        })
    }

    async fn exchange_refresh_token(
        &self,
        request: OAuthTokenRequest,
    ) -> Result<OAuthTokenResponse, OAuthError> {
        let refresh_token = required(request.refresh_token.as_deref(), "refresh_token")?;
        let client_id = required(request.client_id.as_deref(), "client_id")?;
        let token_hash = sha256_hex(refresh_token);
        let stored = OAuthRefreshTokenRepo::get_refresh_token_by_hash(&*self.db, &token_hash)
            .await?
            .ok_or_else(|| OAuthError::InvalidGrant("invalid_refresh_token".into()))?;

        let now_dt = Utc::now();
        let now = format_rfc3339(now_dt);
        if stored.revoked_at.is_some() {
            OAuthRefreshTokenRepo::revoke_refresh_token_family(&*self.db, &stored.family_id, &now)
                .await?;
            return Err(OAuthError::InvalidGrant("refresh_reused".into()));
        }
        if stored.expires_at < now {
            return Err(OAuthError::InvalidGrant("refresh_expired".into()));
        }

        let family_cutoff = format_rfc3339(now_dt - Duration::days(REFRESH_TOKEN_FAMILY_TTL_DAYS));
        let oldest_created_at = sqlx::query_scalar::<_, Option<String>>(
            "SELECT MIN(created_at) FROM oauth_refresh_token WHERE family_id = ?",
        )
        .bind(&stored.family_id)
        .fetch_one(self.db.pool())
        .await?;
        if let Some(oldest_created_at) = oldest_created_at {
            if oldest_created_at < family_cutoff {
                OAuthRefreshTokenRepo::revoke_refresh_token_family(
                    &*self.db,
                    &stored.family_id,
                    &now,
                )
                .await?;
                return Err(OAuthError::InvalidGrant("family_expired".into()));
            }
        }

        if stored.client_id != client_id {
            return Err(OAuthError::InvalidGrant("client_mismatch".into()));
        }

        let user = UserRepo::get_user_by_id(&*self.db, &stored.user_id)
            .await?
            .ok_or_else(|| OAuthError::InvalidGrant("user_missing".into()))?;

        let mut transaction = db::begin_immediate(self.db.pool()).await?;
        let claimed = OAuthRefreshTokenRepo::claim_refresh_token_for_rotation(
            &*self.db,
            &mut transaction,
            &stored.id,
            &now,
        )
        .await?;
        if !claimed {
            OAuthRefreshTokenRepo::revoke_refresh_token_family_in_tx(
                &*self.db,
                &mut transaction,
                &stored.family_id,
                &now,
            )
            .await?;
            transaction.commit().await?;
            return Err(OAuthError::InvalidGrant("refresh_reused".into()));
        }

        let pair = self
            .issue_token_pair_in_tx(
                &mut transaction,
                &user.id,
                &user.email,
                &stored.client_id,
                &stored.resource,
                &stored.scopes,
                &stored.family_id,
            )
            .await?;
        transaction.commit().await?;

        Ok(OAuthTokenResponse {
            access_token: pair.access_token,
            token_type: "Bearer".to_string(),
            expires_in: pair.expires_in,
            refresh_token: pair.refresh_token,
            scope: pair.scope,
        })
    }

    async fn issue_token_pair(
        &self,
        user_id: &str,
        user_email: &str,
        client_id: &str,
        resource: &str,
        scope: &str,
        family_id: &str,
    ) -> Result<IssuedTokenPair, OAuthError> {
        let (pair, refresh_token_input) =
            self.prepare_token_pair(user_id, user_email, client_id, resource, scope, family_id)?;
        OAuthRefreshTokenRepo::create_refresh_token(&*self.db, refresh_token_input).await?;

        Ok(pair)
    }

    #[allow(clippy::too_many_arguments)]
    async fn issue_token_pair_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        user_id: &str,
        user_email: &str,
        client_id: &str,
        resource: &str,
        scope: &str,
        family_id: &str,
    ) -> Result<IssuedTokenPair, OAuthError> {
        let (pair, refresh_token_input) =
            self.prepare_token_pair(user_id, user_email, client_id, resource, scope, family_id)?;
        OAuthRefreshTokenRepo::create_refresh_token_in_tx(
            &*self.db,
            transaction,
            refresh_token_input,
        )
        .await?;

        Ok(pair)
    }

    fn prepare_token_pair(
        &self,
        user_id: &str,
        user_email: &str,
        client_id: &str,
        resource: &str,
        scope: &str,
        family_id: &str,
    ) -> Result<(IssuedTokenPair, CreateOAuthRefreshToken), OAuthError> {
        let now = Utc::now();
        let iat = now.timestamp() as u64;
        let claims = McpAccessTokenClaims {
            sub: user_id.to_string(),
            email: user_email.to_string(),
            iat,
            exp: iat + ACCESS_TOKEN_EXPIRES_IN_SECS,
            aud: resource.to_string(),
            scope: scope.to_string(),
            client_id: client_id.to_string(),
            token_use: "mcp".to_string(),
        };
        let access_token = self
            .auth_service
            .issue_mcp_token(claims)
            .map_err(|error| OAuthError::ServerError(error.to_string()))?;

        let refresh_token = random_hex_32();
        let refresh_token_hash = sha256_hex(&refresh_token);

        Ok((
            IssuedTokenPair {
                access_token,
                refresh_token,
                expires_in: ACCESS_TOKEN_EXPIRES_IN_SECS,
                scope: scope.to_string(),
            },
            CreateOAuthRefreshToken {
                id: new_uuid_v4(),
                token_hash: refresh_token_hash,
                family_id: family_id.to_string(),
                user_id: user_id.to_string(),
                client_id: client_id.to_string(),
                resource: resource.to_string(),
                scopes: scope.to_string(),
                expires_at: format_rfc3339(now + Duration::days(REFRESH_TOKEN_TTL_DAYS)),
                created_at: format_rfc3339(now),
            },
        ))
    }
}

fn required<'a>(value: Option<&'a str>, field: &str) -> Result<&'a str, OAuthError> {
    value.ok_or_else(|| OAuthError::InvalidRequest(format!("missing_{field}")))
}

fn parse_scopes(scope: &str) -> Vec<String> {
    scope
        .split_whitespace()
        .filter(|scope| !scope.is_empty())
        .map(str::to_string)
        .collect()
}

fn is_allowed_redirect_uri(uri: &str) -> bool {
    let Ok(url) = Url::parse(uri) else {
        return false;
    };
    match url.scheme() {
        "https" => true,
        "http" => matches!(
            url.host_str(),
            Some("127.0.0.1") | Some("localhost") | Some("::1")
        ),
        _ => false,
    }
}

fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hex::encode(hasher.finalize())
}

fn base64url_no_pad(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

fn random_hex_32() -> String {
    let mut bytes = [0_u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

fn build_redirect_url(base_uri: &str, params: &[(&str, &str)]) -> String {
    if let Ok(mut url) = Url::parse(base_uri) {
        {
            let mut query_pairs = url.query_pairs_mut();
            for (key, value) in params {
                query_pairs.append_pair(key, value);
            }
        }
        return url.to_string();
    }

    let encoded = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(params.iter().copied())
        .finish();
    let separator = if base_uri.contains('?') { "&" } else { "?" };
    format!("{base_uri}{separator}{encoded}")
}

fn format_rfc3339(datetime: DateTime<Utc>) -> String {
    datetime.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn trim_trailing_slash(value: &str) -> &str {
    value.trim_end_matches('/')
}
