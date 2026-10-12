//! Error type for the price-resolution paths that read and write SQLite.
//!
//! Callers in `services` convert it into `ServiceError`, so the
//! `DbError → ServiceError → ApiError` mapping stays at the crate boundaries.

/// A price-resolution failure.
#[derive(Debug, thiserror::Error)]
pub enum PricingError {
    #[error(transparent)]
    Db(#[from] db::DbError),
    #[error("invalid operation: {message}")]
    InvalidOperation { message: String },
}

impl PricingError {
    pub fn invalid_operation(message: impl Into<String>) -> Self {
        Self::InvalidOperation {
            message: message.into(),
        }
    }
}

impl From<sqlx::Error> for PricingError {
    fn from(error: sqlx::Error) -> Self {
        Self::Db(error.into())
    }
}

pub type PricingResult<T> = std::result::Result<T, PricingError>;
