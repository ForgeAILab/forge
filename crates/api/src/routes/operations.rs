use api_types::{OperationsRefreshResponse, OperatorStatusResponse};
use axum::{extract::State, Json};
use db::now_rfc3339;
use events::{event_timestamp, EventContext, ForgeEvent};

use crate::{errors::ApiResult, routes::auth::RequireAdmin, state::AppState};

/// Admin snapshot including durable consumer lag/stall alerts and SQLite
/// incremental-vacuum mode/freelist diagnostics, computed by the shared service.
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
