use axum::{
    extract::{FromRequestParts, Path, State},
    http::{request::Parts, HeaderMap, Method, StatusCode},
    Json,
};

use api_types::{
    AuthResponse, CreateTokenRequest, LoginRequest, LogoutRequest, RefreshRequest, RegisterRequest,
    TokenResponse, UpdateProfileRequest, UserResponse,
};
use db::{new_uuid_v4, now_rfc3339, CreatePersonalAccessToken, PersonalAccessTokenRepo};
use sha2::{Digest, Sha256};

use crate::{
    errors::{ApiError, ApiResult},
    session::{cookie, SessionBody, SessionContext, REFRESH_COOKIE},
    state::AppState,
};

pub(crate) async fn register(
    State(state): State<AppState>,
    session: SessionContext,
    Json(body): Json<RegisterRequest>,
) -> ApiResult<(StatusCode, HeaderMap, Json<AuthResponse>)> {
    let _user = state
        .auth_service
        .register(&body.email, &body.password, body.display_name.as_deref())
        .await
        .map_err(|e| match e.to_string().as_str() {
            s if s.contains("invalid_email") => {
                ApiError::unprocessable("invalid_email", "Invalid email format")
            }
            s if s.contains("password_too_weak") => ApiError::unprocessable(
                "password_too_weak",
                "Password must be at least 8 characters",
            ),
            s if s.contains("display_name_too_long") => ApiError::unprocessable(
                "display_name_too_long",
                "Display name must be 255 characters or less",
            ),
            s if s.contains("email_exists") => {
                ApiError::conflict("email_exists", "An account with this email already exists")
            }
            _ => ApiError::from(e),
        })?;

    let pair = state
        .auth_service
        .login(&body.email, &body.password)
        .await
        .map_err(ApiError::from)?;

    Ok((
        StatusCode::CREATED,
        session.token_cookies(&pair)?,
        Json(AuthResponse {
            access_token: pair.access_token,
            refresh_token: pair.refresh_token,
            token_type: "Bearer".to_string(),
            expires_in: pair.expires_in,
        }),
    ))
}

pub(crate) async fn login(
    State(state): State<AppState>,
    session: SessionContext,
    Json(body): Json<LoginRequest>,
) -> ApiResult<(HeaderMap, Json<AuthResponse>)> {
    let pair = state
        .auth_service
        .login(&body.email, &body.password)
        .await
        .map_err(|e| {
            if e.to_string().contains("invalid_credentials") {
                ApiError::unauthorized_with_code("invalid_credentials", "Invalid email or password")
            } else {
                ApiError::from(e)
            }
        })?;

    Ok((
        session.token_cookies(&pair)?,
        Json(AuthResponse {
            access_token: pair.access_token,
            refresh_token: pair.refresh_token,
            token_type: "Bearer".to_string(),
            expires_in: pair.expires_in,
        }),
    ))
}

pub(crate) async fn refresh(
    State(state): State<AppState>,
    session: SessionContext,
    headers: HeaderMap,
    SessionBody(body): SessionBody<RefreshRequest>,
) -> ApiResult<(HeaderMap, Json<AuthResponse>)> {
    let refresh_token = refresh_credential(body.refresh_token.as_deref(), &headers, &session)?;
    let pair = state
        .auth_service
        .refresh(refresh_token)
        .await
        .map_err(|e| {
            let msg = e.to_string();
            if msg.contains("invalid_refresh_token") {
                ApiError::unauthorized_with_code("invalid_refresh_token", "Invalid refresh token")
            } else if msg.contains("refresh_token_expired") {
                ApiError::unauthorized_with_code(
                    "refresh_token_expired",
                    "Refresh token has expired",
                )
            } else if msg.contains("user_not_found") {
                ApiError::unauthorized_with_code("user_not_found", "User no longer exists")
            } else {
                ApiError::from(e)
            }
        })?;

    Ok((
        session.token_cookies(&pair)?,
        Json(AuthResponse {
            access_token: pair.access_token,
            refresh_token: pair.refresh_token,
            token_type: "Bearer".to_string(),
            expires_in: pair.expires_in,
        }),
    ))
}

pub(crate) async fn logout(
    State(state): State<AppState>,
    session: SessionContext,
    headers: HeaderMap,
    SessionBody(body): SessionBody<LogoutRequest>,
) -> ApiResult<(StatusCode, HeaderMap)> {
    let refresh_token = refresh_credential(body.refresh_token.as_deref(), &headers, &session)?;
    state.auth_service.logout(refresh_token).await?;
    Ok((StatusCode::OK, session.clear_cookies()?))
}

fn refresh_credential<'a>(
    body_token: Option<&'a str>,
    headers: &'a HeaderMap,
    session: &SessionContext,
) -> ApiResult<&'a str> {
    if let Some(token) = body_token {
        return Ok(token);
    }
    // Require proof even if SameSite withheld the cookies: an anonymous
    // cross-site logout must not clear a browser's existing session cookies.
    session.check_csrf(headers, &Method::POST)?;
    Ok(cookie(headers, REFRESH_COOKIE).unwrap_or(""))
}

pub async fn me(
    State(state): State<AppState>,
    user: AuthenticatedUser,
) -> ApiResult<Json<UserResponse>> {
    let user = state
        .auth_service
        .get_user(&user.user_id)
        .await
        .map_err(|e| {
            if e.to_string().contains("user_not_found") {
                ApiError::unauthorized_with_code("user_not_found", "User no longer exists")
            } else {
                ApiError::from(e)
            }
        })?;

    Ok(Json(UserResponse {
        id: user.id,
        email: user.email,
        display_name: user.display_name,
        is_admin: user.is_admin,
        created_at: user.created_at,
    }))
}

pub async fn update_me(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Json(body): Json<UpdateProfileRequest>,
) -> ApiResult<Json<UserResponse>> {
    let email = body.email.as_deref();
    let display_name = body.display_name.as_ref().map(|value| value.as_deref());

    let updated = state
        .auth_service
        .update_profile(&user.user_id, email, display_name)
        .await
        .map_err(|e| match e.to_string().as_str() {
            s if s.contains("invalid_email") => {
                ApiError::unprocessable("invalid_email", "Invalid email format")
            }
            s if s.contains("display_name_too_long") => ApiError::unprocessable(
                "display_name_too_long",
                "Display name must be 255 characters or less",
            ),
            s if s.contains("email_exists") => {
                ApiError::conflict("email_exists", "An account with this email already exists")
            }
            _ => ApiError::from(e),
        })?;

    Ok(Json(UserResponse {
        id: updated.id,
        email: updated.email,
        display_name: updated.display_name,
        is_admin: updated.is_admin,
        created_at: updated.created_at,
    }))
}

pub async fn create_pat(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Json(body): Json<CreateTokenRequest>,
) -> ApiResult<(StatusCode, Json<TokenResponse>)> {
    let name = body.name.trim().to_string();
    if name.is_empty() {
        return Err(ApiError::bad_request("Token name is required"));
    }

    // Generate raw token: fg_ + 40 hex chars
    let random_bytes: [u8; 20] = rand::random();
    let raw_token = format!("fg_{}", hex::encode(random_bytes));
    let prefix = raw_token[..7].to_string(); // "fg_" + first 4 hex chars

    // Hash for storage
    let mut hasher = Sha256::new();
    hasher.update(raw_token.as_bytes());
    let token_hash = hex::encode(hasher.finalize());

    let now = now_rfc3339();
    let input = CreatePersonalAccessToken {
        id: new_uuid_v4(),
        user_id: user.user_id,
        name: name.clone(),
        token_hash,
        prefix: prefix.clone(),
        scopes: "*".to_string(),
        expires_at: body.expires_at.clone(),
        created_at: now.clone(),
    };

    let pat = PersonalAccessTokenRepo::create_pat(&*state.db, input)
        .await
        .map_err(ApiError::from)?;

    Ok((
        StatusCode::CREATED,
        Json(TokenResponse {
            id: pat.id,
            name: pat.name,
            token: Some(raw_token),
            prefix: pat.prefix,
            scopes: pat.scopes,
            expires_at: pat.expires_at,
            last_used_at: pat.last_used_at,
            created_at: pat.created_at,
        }),
    ))
}

pub async fn list_pats(
    State(state): State<AppState>,
    user: AuthenticatedUser,
) -> ApiResult<Json<Vec<TokenResponse>>> {
    let pats = PersonalAccessTokenRepo::list_pats_by_user(&*state.db, &user.user_id)
        .await
        .map_err(ApiError::from)?;

    let items = pats
        .into_iter()
        .map(|pat| TokenResponse {
            id: pat.id,
            name: pat.name,
            token: None,
            prefix: pat.prefix,
            scopes: pat.scopes,
            expires_at: pat.expires_at,
            last_used_at: pat.last_used_at,
            created_at: pat.created_at,
        })
        .collect();

    Ok(Json(items))
}

pub async fn delete_pat(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<StatusCode> {
    PersonalAccessTokenRepo::delete_pat(&*state.db, &id, &user.user_id)
        .await
        .map_err(|e| match e {
            db::DbError::NotFound => ApiError::not_found("token", id),
            other => ApiError::from(other),
        })?;

    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Clone)]
pub struct AuthenticatedUser {
    pub user_id: String,
    pub email: String,
    pub is_admin: bool,
}

impl<S: Send + Sync> FromRequestParts<S> for AuthenticatedUser {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<AuthenticatedUser>()
            .cloned()
            .ok_or_else(|| {
                ApiError::unauthorized_with_code("missing_token", "Authentication required")
            })
    }
}

pub struct RequireAdmin(pub AuthenticatedUser);

impl<S: Send + Sync> FromRequestParts<S> for RequireAdmin {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let user = AuthenticatedUser::from_request_parts(parts, state).await?;
        if !user.is_admin {
            return Err(ApiError::forbidden_with_code(
                "admin_required",
                "Admin access required",
            ));
        }
        Ok(RequireAdmin(user))
    }
}
