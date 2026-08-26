//! Project-scoped reconciliation list/detail/resolve routes (design D15).
//!
//! Authorization and every domain rule live in
//! `services::ProjectReconciliationService`; these handlers only bind the
//! authenticated user and the URL path to that shared service so REST, a
//! future MCP adapter, and internal recovery jobs can never diverge on what
//! a reconciliation resolution means.

use crate::json::Json;
use api_types::{
    ProjectReconciliation, ProjectReconciliationListResponse, ResolveProjectReconciliationRequest,
    ResolveProjectReconciliationResponse,
};
use axum::extract::{Path, Query, State};
use serde::Deserialize;
use services::ProjectReconciliationService;

use crate::{errors::ApiResult, routes::auth::AuthenticatedUser, state::AppState};

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ReconciliationListQuery {
    pub cursor: Option<String>,
    pub limit: Option<i64>,
}

fn service(state: &AppState) -> ProjectReconciliationService {
    ProjectReconciliationService::new(state.db.clone(), state.event_bus.clone())
}

pub async fn list_project_reconciliations(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
    Query(query): Query<ReconciliationListQuery>,
) -> ApiResult<Json<ProjectReconciliationListResponse>> {
    let page = service(&state)
        .list(
            &project_id,
            &user.user_id,
            query.cursor.as_deref(),
            query.limit.unwrap_or(20),
        )
        .await?;
    Ok(Json(ProjectReconciliationListResponse {
        items: page.items,
        next_cursor: page.next_cursor,
        has_more: page.has_more,
    }))
}

pub async fn get_project_reconciliation(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, reconciliation_id)): Path<(String, String)>,
) -> ApiResult<Json<ProjectReconciliation>> {
    let reconciliation = service(&state)
        .get(&project_id, &user.user_id, &reconciliation_id)
        .await?;
    Ok(Json(reconciliation))
}

pub async fn resolve_project_reconciliation(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, reconciliation_id)): Path<(String, String)>,
    Json(request): Json<ResolveProjectReconciliationRequest>,
) -> ApiResult<Json<ResolveProjectReconciliationResponse>> {
    let response = service(&state)
        .resolve(&project_id, &user.user_id, &reconciliation_id, request)
        .await?;
    Ok(Json(response))
}
