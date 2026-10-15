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
