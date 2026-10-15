use super::*;
use crate::routes::reviews::review_response_server_checked;

pub async fn list_reviews(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<Vec<api_types::ReviewResponse>>> {
    crate::routes::authorization::require_task_access(
        &state,
        &id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    let reviews = db::ReviewRepo::list_by_task(&*state.db, &id).await?;
    let responses = reviews
        .into_iter()
        .map(review_response_server_checked)
        .collect::<ApiResult<Vec<_>>>()?;
    Ok(Json(responses))
}

pub async fn list_findings(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<api_types::TaskFindingsResponse>> {
    crate::routes::authorization::require_task_access(
        &state,
        &id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    let window = state.db.review_window(&id).await?;
    // Match /reviews' corruption handling as well as its read authority.
    for review in &window.reviews {
        review_response_server_checked(review.clone())?;
    }
    let mut items = review::finding_ledger(&window.reviews).map_err(ApiError::internal)?;
    // The complete ledger is naturally sorted before the transport cap.
    items.truncate(200);
    Ok(Json(api_types::TaskFindingsResponse {
        items,
        window_started_at: window.window_started_at,
        review_attempts: window.reviews.len(),
    }))
}
