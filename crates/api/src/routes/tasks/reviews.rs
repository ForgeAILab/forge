use super::*;
use crate::routes::reviews::review_response_server_checked;

pub async fn trigger_review(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<TransitionTaskResponse>> {
    let task = TaskRepo::get_by_id(&*state.db, &id, false)
        .await?
        .ok_or_else(|| ApiError::not_found("task", id.clone()))?;
    if task.status != default_states::REVIEW {
        return Err(ApiError::invalid_operation_conflict(format!(
            "task {id} is in {} state; expected review",
            task.status
        )));
    }
    let task_id = Uuid::parse_str(&id)
        .map_err(|error| ApiError::bad_request(format!("invalid task id: {error}")))?;
    let (task, review) = state.task_service.rerun_review(task_id).await?;
    Ok(Json(TransitionTaskResponse {
        task: task_response(&state.db, task).await?,
        review: Some(review_response_server_checked(review)?),
    }))
}

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

pub async fn approve_review(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ReviewDecisionResponse>> {
    let (task, review) = state
        .task_service
        .approve_review(id)
        .await
        .map_err(map_manual_review_error)?;
    Ok(Json(ReviewDecisionResponse {
        task: task_response(&state.db, task).await?,
        review: review_response_server_checked(review)?,
    }))
}

pub async fn reject_review(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<RejectReviewRequest>,
) -> ApiResult<Json<ReviewDecisionResponse>> {
    let (task, review) = state
        .task_service
        .reject_review(id, request.reason)
        .await
        .map_err(map_manual_review_error)?;
    Ok(Json(ReviewDecisionResponse {
        task: task_response(&state.db, task).await?,
        review: review_response_server_checked(review)?,
    }))
}
