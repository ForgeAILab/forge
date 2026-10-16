//! Browser session cookies and their request-origin boundary.

use std::net::SocketAddr;

use axum::{
    body::HttpBody,
    extract::{rejection::JsonRejection, ConnectInfo, FromRequest, FromRequestParts, Request},
    http::{header, request::Parts, Extensions, HeaderMap, HeaderValue, Method, Uri},
};
use services::auth_service::{TokenPair, REFRESH_TOKEN_EXPIRY_SECS};
use url::Url;

use crate::{errors::ApiError, state::AppState};

pub(crate) const SESSION_COOKIE: &str = "__Host-forge_session";
pub(crate) const REFRESH_COOKIE: &str = "__Host-forge_refresh";

/// Cookie refresh/logout can omit the body entirely. Present JSON still goes
/// through the normal extractor so malformed credentials are never ignored.
pub(crate) struct SessionBody<T>(pub T);

impl<T> FromRequest<AppState> for SessionBody<T>
where
    T: serde::de::DeserializeOwned + Default,
{
    type Rejection = JsonRejection;

    async fn from_request(request: Request, state: &AppState) -> Result<Self, Self::Rejection> {
        if request.body().is_end_stream() {
            return Ok(Self(T::default()));
        }
        axum::Json::<T>::from_request(request, state)
            .await
            .map(|axum::Json(body)| Self(body))
    }
}

pub(crate) fn cookie<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get_all(header::COOKIE).iter().find_map(|header| {
        header.to_str().ok()?.split(';').find_map(|pair| {
            let (key, value) = pair.trim().split_once('=')?;
            (key == name).then_some(value)
        })
    })
}

pub(crate) struct SessionContext {
    secure: bool,
    origin: Option<url::Origin>,
}

impl SessionContext {
    pub(crate) fn from_request(
        state: &AppState,
        headers: &HeaderMap,
        uri: &Uri,
        extensions: &Extensions,
    ) -> Self {
        // Forwarded headers are otherwise client-controlled. Forge has no proxy
        // allowlist; only a loopback proxy is trusted to assert the scheme.
        // Remote/container proxies should configure FORGE_PUBLIC_BASE_URL.
        let forwarded_scheme = extensions
            .get::<ConnectInfo<SocketAddr>>()
            .filter(|peer| peer.0.ip().is_loopback())
            .and_then(|_| headers.get("x-forwarded-proto"))
            .and_then(|value| value.to_str().ok())
            .filter(|value| matches!(*value, "http" | "https"));
        let scheme = forwarded_scheme.or(uri.scheme_str()).unwrap_or("http");
        let public_origin = state.effective_config.trusted_origin();
        let public_url = Url::parse(&public_origin).ok();
        let secure = scheme == "https"
            || public_url
                .as_ref()
                .is_some_and(|url| url.scheme() == "https");
        let origin = if state.effective_config.server.public_base_url.is_some() {
            public_url.map(|url| url.origin())
        } else {
            let host = headers
                .get(header::HOST)
                .and_then(|value| value.to_str().ok())
                .or_else(|| uri.authority().map(|authority| authority.as_str()));
            host.and_then(|host| Url::parse(&format!("{scheme}://{host}")).ok())
                .or(public_url)
                .map(|url| url.origin())
        };
        Self { secure, origin }
    }

    pub(crate) fn check_csrf(&self, headers: &HeaderMap, method: &Method) -> Result<(), ApiError> {
        let safe = matches!(
            *method,
            Method::GET | Method::HEAD | Method::OPTIONS | Method::TRACE
        );
        let same_origin = match headers.get("sec-fetch-site") {
            // The browser's verdict binds every method: SameSite=Strict still
            // sends the cookie from another port on the same host or from a
            // sibling subdomain, e.g. for an <img> or a no-cors read.
            Some(value) => matches!(value.to_str(), Ok("same-origin" | "none")),
            None if safe => true,
            None => headers
                .get(header::ORIGIN)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| Url::parse(value).ok())
                .filter(|url| {
                    matches!(url.scheme(), "http" | "https")
                        && url.username().is_empty()
                        && url.password().is_none()
                        && url.path() == "/"
                        && url.query().is_none()
                        && url.fragment().is_none()
                })
                .is_some_and(|url| self.origin.as_ref() == Some(&url.origin())),
        };
        if same_origin {
            Ok(())
        } else {
            Err(ApiError::forbidden_with_code(
                "csrf_rejected",
                "Cookie authentication requires a same-origin request",
            ))
        }
    }

    pub(crate) fn token_cookies(&self, pair: &TokenPair) -> Result<HeaderMap, ApiError> {
        // Cookies ignore ports. On local HTTP another server on this host
        // could receive a live token, so only issue cookies over HTTPS.
        if !self.secure {
            return Ok(HeaderMap::new());
        }
        self.cookies(
            &pair.access_token,
            pair.expires_in,
            &pair.refresh_token,
            REFRESH_TOKEN_EXPIRY_SECS,
        )
    }

    pub(crate) fn clear_cookies(&self) -> Result<HeaderMap, ApiError> {
        self.cookies("", 0, "", 0)
    }

    fn cookies(
        &self,
        access: &str,
        access_max_age: u64,
        refresh: &str,
        refresh_max_age: u64,
    ) -> Result<HeaderMap, ApiError> {
        let mut headers = HeaderMap::new();
        // The __Host- prefix requires Secure, Path=/ and no Domain, including
        // when clearing cookies. Only refresh/logout read the refresh cookie.
        for (name, value, max_age) in [
            (SESSION_COOKIE, access, access_max_age),
            (REFRESH_COOKIE, refresh, refresh_max_age),
        ] {
            let value = HeaderValue::from_str(&format!(
                "{name}={value}; HttpOnly; SameSite=Strict; Path=/; Max-Age={max_age}; Secure"
            ))
            .map_err(|_| ApiError::internal("Invalid session cookie"))?;
            headers.append(header::SET_COOKIE, value);
        }
        Ok(headers)
    }
}

impl FromRequestParts<AppState> for SessionContext {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        Ok(Self::from_request(
            state,
            &parts.headers,
            &parts.uri,
            &parts.extensions,
        ))
    }
}
