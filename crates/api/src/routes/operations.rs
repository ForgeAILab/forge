use api_types::{
    DeadLetterActionResponse, DeadLetterListResponse, DeadLetterState, DismissDeadLetterRequest,
    OperationsRefreshResponse, OperatorStatusResponse,
};
use axum::{
    extract::{Path, Query, State},
    Json,
};
use db::now_rfc3339;
use events::{event_timestamp, EventContext, ForgeEvent};

use crate::{errors::ApiResult, routes::auth::RequireAdmin, state::AppState};
use services::dead_letter_service::DeadLetterActor;

#[derive(serde::Deserialize)]
pub struct DeadLetterListQuery {
    pub consumer: Option<String>,
    #[serde(default)]
    pub state: DeadLetterState,
    pub cursor: Option<String>,
    pub limit: Option<usize>,
}

pub async fn list_dead_letters(
    RequireAdmin(admin): RequireAdmin,
    State(state): State<AppState>,
    Query(query): Query<DeadLetterListQuery>,
) -> ApiResult<Json<DeadLetterListResponse>> {
    Ok(Json(
        state
            .dead_letter_service
            .list(
                DeadLetterActor {
                    user_id: &admin.user_id,
                    is_admin: admin.is_admin,
                },
                query.consumer.as_deref(),
                query.state,
                query.cursor.as_deref(),
                query.limit.unwrap_or(50),
            )
            .await?,
    ))
}

pub async fn replay_dead_letter(
    RequireAdmin(admin): RequireAdmin,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<DeadLetterActionResponse>> {
    Ok(Json(
        state
            .dead_letter_service
            .replay(
                DeadLetterActor {
                    user_id: &admin.user_id,
                    is_admin: admin.is_admin,
                },
                &id,
            )
            .await?,
    ))
}

pub async fn dismiss_dead_letter(
    RequireAdmin(admin): RequireAdmin,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<DismissDeadLetterRequest>,
) -> ApiResult<Json<DeadLetterActionResponse>> {
    Ok(Json(
        state
            .dead_letter_service
            .dismiss(
                DeadLetterActor {
                    user_id: &admin.user_id,
                    is_admin: admin.is_admin,
                },
                &id,
                body.reason.as_deref(),
            )
            .await?,
    ))
}

/// Admin snapshot including live worker backlog, supervised relay status,
/// lasting dead-letter history, SQLite storage diagnostics and usage index charge.
pub async fn get_operations_status(
    _admin: RequireAdmin,
    State(state): State<AppState>,
) -> ApiResult<Json<OperatorStatusResponse>> {
    Ok(Json(state.operator_status_service.compute_status().await?))
}

pub async fn refresh_operations(
    _admin: RequireAdmin,
    State(state): State<AppState>,
) -> ApiResult<Json<OperationsRefreshResponse>> {
    let dispatched_tasks = if let Some(dispatcher) = state.task_dispatcher.as_ref() {
        dispatcher.check_once().await?
    } else {
        0
    };
    let refreshed_at = now_rfc3339();
    state.event_bus.publish(ForgeEvent {
        event_type: "operations.refreshed".to_owned(),
        entity_id: "operations".to_owned(),
        timestamp: event_timestamp(),
        context: EventContext::ReconciliationEvent {
            task_id: None,
            execution_id: None,
            reason: "manual refresh".to_owned(),
        },
    });
    Ok(Json(OperationsRefreshResponse {
        dispatched_tasks,
        refreshed_at,
    }))
}
