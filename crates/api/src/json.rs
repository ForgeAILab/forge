//! The JSON extractor/response wrapper every route uses.
//!
//! `axum::Json`'s own rejection answers a malformed body with a bare
//! `422 Unprocessable Entity` and a plain-text reason. That contradicts this
//! API's error contract twice over: `422` here means an illegal *state
//! transition* (see `docs/api.md`), and every other failure carries the
//! `{ code, message }` envelope. A client parsing our envelope therefore had
//! to special-case one status that returned something else entirely.
//!
//! This wrapper delegates parsing and serialization to `axum::Json` and maps
//! only the rejection, so a malformed or contract-violating body is one
//! `400 validation_error` with the deserializer's own field path and expected
//! values preserved in `message`. That is what lets a closed vocabulary such
//! as `AdaptiveTaskOperation` reject `task.propose` at the REST edge with the
//! same outcome code the native adapter's shared validator returns for the
//! same input.

use axum::{
    extract::{FromRequest, OptionalFromRequest, Request},
    response::{IntoResponse, Response},
};

use crate::errors::ApiError;

pub struct Json<T>(pub T);

impl<S, T> FromRequest<S> for Json<T>
where
    axum::Json<T>: FromRequest<S, Rejection = axum::extract::rejection::JsonRejection>,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        match axum::Json::<T>::from_request(request, state).await {
            Ok(axum::Json(value)) => Ok(Self(value)),
            // `body_text` carries the deserializer's message, which already
            // names the exact field path and — for a closed enum — the
            // complete set of accepted values.
            Err(rejection) => Err(ApiError::validation(rejection.body_text())),
        }
    }
}

/// `Option<Json<T>>` for the handful of routes whose body is genuinely
/// optional. An absent body stays `None`; a body that is present but
/// malformed is still the same `400 validation_error` rather than being
/// silently treated as absent.
impl<S, T> OptionalFromRequest<S> for Json<T>
where
    axum::Json<T>: OptionalFromRequest<S, Rejection = axum::extract::rejection::JsonRejection>,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(request: Request, state: &S) -> Result<Option<Self>, Self::Rejection> {
        match axum::Json::<T>::from_request(request, state).await {
            Ok(Some(axum::Json(value))) => Ok(Some(Self(value))),
            Ok(None) => Ok(None),
            Err(rejection) => Err(ApiError::validation(rejection.body_text())),
        }
    }
}

impl<T> IntoResponse for Json<T>
where
    axum::Json<T>: IntoResponse,
{
    fn into_response(self) -> Response {
        axum::Json(self.0).into_response()
    }
}
