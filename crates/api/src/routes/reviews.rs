use api_types::ReviewResponse;
use axum::{
    extract::{Path, State},
    Json,
};
use db::ReviewRepo;

use crate::{
    errors::{ApiError, ApiResult},
    state::AppState,
};

/// Parse persisted review details strictly and classify malformed rows as
/// server corruption rather than caller input.
pub fn review_response_server_checked(review: db::Review) -> ApiResult<ReviewResponse> {
    let review_id = review.id.clone();
    let details = super::parse_review_details(&review.step_results_json).map_err(|_| {
        ApiError::internal(format!(
            "persisted review {review_id} has invalid step_results_json"
        ))
    })?;
    Ok(super::review_response_with_details(review, details))
}

pub async fn get_review(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ReviewResponse>> {
    let review = ReviewRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("review", id))?;
    Ok(Json(review_response_server_checked(review)?))
}
