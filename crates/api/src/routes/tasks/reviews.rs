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
    let mut items =
        db::review_findings::finding_ledger(&window.reviews).map_err(ApiError::internal)?;
    // Preserve the ledger's natural ordering within each priority group.
    // Open blockers, including verify findings, must survive the read cap.
    items.sort_by_key(|entry| {
        let blocking =
            entry.status.is_open() && entry.severity == api_types::ReviewFindingSeverity::Blocking;
        (
            !blocking,
            blocking && entry.source != api_types::ReviewFindingSource::Verify,
        )
    });
    items.truncate(200);
    Ok(Json(api_types::TaskFindingsResponse {
        items,
        window_started_at: window.window_started_at,
        review_attempts: window.reviews.len(),
    }))
}
