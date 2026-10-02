use super::*;

#[derive(Debug, Deserialize)]
pub struct PromptPreviewQuery {
    pub role: String,
    pub trigger: Option<WorkflowTrigger>,
}

pub async fn prompt_preview(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(params): Query<PromptPreviewQuery>,
) -> ApiResult<Json<PromptPreviewResponse>> {
    if params.role.trim().is_empty() {
        return Err(ApiError::bad_request("role must not be empty"));
    }

    let prompt = services::preview_effective_prompt(
        std::sync::Arc::clone(&state.db),
        &state.task_service.workspace_backend_router(),
        &id,
        params.role.trim(),
        params.trigger,
    )
    .await?;

    Ok(Json(PromptPreviewResponse {
        system: prompt.system,
        user: prompt.user,
        tools: non_empty_tools(prompt.tools),
    }))
}

fn non_empty_tools(tools: Vec<String>) -> Option<Vec<String>> {
    if tools.is_empty() {
        None
    } else {
        Some(tools)
    }
}
