use super::*;
use api_types::{Actor, UserActionSource};

pub async fn transition_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<TransitionTaskRequest>,
) -> ApiResult<Json<TransitionTaskResponse>> {
    let mut options = TransitionOptions::from((request.version, request.reason));
    if request.source == Some(TransitionSource::BoardDrag) {
        options.triggered_by = Actor::user(UserActionSource::BoardDrag);
        options.defer_dispatch_seconds = Some(10);
    }
    let result = state
        .task_service
        .transition(id, request.status, options)
        .await?;
    let awaiting_human = state
        .task_service
        .is_task_awaiting_human(&result.task)
        .await?;
    let response =
        task_response_with_awaiting_human(&state.db, result.task, awaiting_human).await?;
    Ok(Json(TransitionTaskResponse {
        task: response,
        review: result.review.map(review_response),
    }))
}

pub async fn list_transitions(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<TransitionLogListResponse>> {
    let entries = TransitionLogRepo::list_by_task(&*state.db, &id)
        .await?
        .into_iter()
        .map(transition_log_entry)
        .collect::<ApiResult<Vec<_>>>()?;
    Ok(Json(TransitionLogListResponse { items: entries }))
}

#[derive(Serialize)]
pub struct TransitionLogListResponse {
    pub items: Vec<TransitionLogEntry>,
}
