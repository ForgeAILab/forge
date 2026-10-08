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

use crate::{
    errors::{ApiError, ApiResult},
    routes::auth::RequireAdmin,
    state::AppState,
};
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
    let service = state.dead_letter_service.clone();
    // Dropping the request's JoinHandle detaches the owned replay task, so a
    // disconnect after COMMIT cannot cancel the consumer's after_commit hook.
    let replay = tokio::spawn(async move {
        service
            .replay(
                DeadLetterActor {
                    user_id: &admin.user_id,
                    is_admin: admin.is_admin,
                },
                &id,
            )
            .await
    });
    Ok(Json(replay.await.map_err(|_| {
        ApiError::internal("dead-letter replay task failed")
    })??))
}

pub async fn dismiss_dead_letter(
    RequireAdmin(admin): RequireAdmin,
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Option<Json<DismissDeadLetterRequest>>,
) -> ApiResult<Json<DeadLetterActionResponse>> {
    let body = body.map(|Json(body)| body).unwrap_or_default();
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

/// Admin snapshot including live worker backlog, periodic tick/restart health, supervised relay status,
/// lasting dead-letter history, SQLite storage diagnostics and usage index charge.
pub async fn get_operations_status(
    _admin: RequireAdmin,
    State(state): State<AppState>,
) -> ApiResult<Json<OperatorStatusResponse>> {
    // Includes leased task_steps queue pressure alongside worker/consumer health.
    // Includes passive integration queue/import counts; no queue mutation route.
    Ok(Json(state.operator_status_service.compute_status().await?))
}

pub async fn refresh_operations(
    _admin: RequireAdmin,
    State(state): State<AppState>,
) -> ApiResult<Json<OperationsRefreshResponse>> {
    let dispatched_tasks = if let Some(dispatcher) = state.task_dispatcher.as_ref() {
        // A refresh reconciles every Task now, as it scanned every Task before.
        dispatcher.reconcile_all().await?
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
