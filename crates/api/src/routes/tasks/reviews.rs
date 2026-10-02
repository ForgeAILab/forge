use super::*;
use crate::routes::reviews::review_response_server_checked;

pub async fn list_reviews(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Vec<api_types::ReviewResponse>>> {
    let reviews = db::ReviewRepo::list_by_task(&*state.db, &id).await?;
    let responses = reviews
        .into_iter()
        .map(review_response_server_checked)
        .collect::<ApiResult<Vec<_>>>()?;
    Ok(Json(responses))
}

