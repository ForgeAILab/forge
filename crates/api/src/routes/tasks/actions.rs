use super::*;

pub async fn list_task_actions(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<api_types::TaskActionsResponse>> {
    crate::routes::authorization::require_task_access(
        &state,
        &id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    Ok(Json(
        state
            .task_service
            .task_action_offers(&id, &Actor::user(api_types::UserActionSource::Api))
            .await?,
    ))
}

pub async fn apply_task_action(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Path(id): Path<String>,
    Json(request): Json<TaskActionRequest>,
) -> ApiResult<Json<TaskResponse>> {
    crate::routes::authorization::require_task_access(
        &state,
        &id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Write,
    )
    .await?;
    let needs_owner = match &request.action {
        api_types::TaskAction::Approve {
            override_checks: Some(value),
            ..
        } => *value,
        api_types::TaskAction::Approve {
            override_checks: None,
            ..
        } => {
            let offers = state
                .task_service
                .task_action_offers(&id, &Actor::user(api_types::UserActionSource::Api))
                .await?;
            offers
                .available_actions
                .iter()
                .find(|offer| offer.action.verb() == "approve")
                .is_some_and(|offer| {
                    offer
                        .parameters
                        .iter()
                        .find(|parameter| parameter.name == "override")
                        .and_then(|parameter| parameter.boolean_values.as_ref())
                        .is_some_and(|values| values.as_slice() == [true])
                        || matches!(
                            offer.action,
                            api_types::TaskAction::Approve {
                                override_checks: Some(true),
                                ..
                            }
                        )
                })
        }
        _ => false,
    };
    if needs_owner {
        crate::routes::authorization::require_task_access(
            &state,
            &id,
            &user.user_id,
            crate::routes::authorization::ProjectAccess::Owner,
        )
        .await?;
    }
    // The command resolves omitted fixed parameters from the live caller offer.
    let result = state
        .task_service
        .perform_task_action(id, request.action, request.version)
        .await?;
    Ok(Json(
        task_response(&state.db, &state.workspace_backend_router, result.task).await?,
    ))
}
