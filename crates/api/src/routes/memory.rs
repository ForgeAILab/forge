use api_types::{MemoryGetQuery, MemorySearchQuery, MemorySearchResponse, MemorySearchResultDto};
use axum::{
    extract::{Path, Query, State},
    Json,
};
use db::MemoryScopeGrant;
use services::{MemoryAccessContext, MemorySearchResult};
use uuid::Uuid;

use crate::{
    errors::{ApiError, ApiResult},
    routes::auth::AuthenticatedUser,
    state::AppState,
};

pub async fn search_project_memory(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
    Query(params): Query<MemorySearchQuery>,
) -> ApiResult<Json<MemorySearchResponse>> {
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    if params.query.trim().is_empty() {
        return Err(ApiError::bad_request("query must not be empty"));
    }
    let project_uuid = parse_uuid(&project_id, "project_id")?;
    let normalized_project_id = project_uuid.to_string();
    crate::routes::authorization::require_project_access(
        &state,
        &normalized_project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    let layer = response_layer(params.layer, params.token_budget)?;
    let limit = params.limit.unwrap_or(20);
    let access = MemoryAccessContext::for_scope(
        None,
        "project",
        normalized_project_id.clone(),
        vec!["project".to_owned()],
    );
    let (results, has_more, next_cursor) = state
        .memory_service
        .search_scoped(&access, params.query, params.layer, limit, params.cursor)
        .await?;

    let mut items = Vec::with_capacity(results.len());
    for (index, result) in results.into_iter().enumerate() {
        items.push(memory_result_dto(
            result,
            layer,
            relevance_score(index),
            Some(normalized_project_id.clone()),
        ));
    }

    Ok(Json(MemorySearchResponse {
        items,
        has_more,
        next_cursor,
    }))
}

pub async fn get_memory_item(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Query(params): Query<MemoryGetQuery>,
) -> ApiResult<Json<MemorySearchResultDto>> {
    let item_uuid = parse_uuid(&id, "id")?;
    let layer = response_layer(params.layer, None)?;
    let grants = if let Some(project_id) = params.project_id.as_deref() {
        let project_uuid = parse_uuid(project_id, "project_id")?;
        let normalized_project_id = project_uuid.to_string();
        crate::routes::authorization::require_project_access(
            &state,
            &normalized_project_id,
            &user.user_id,
            crate::routes::authorization::ProjectAccess::Read,
        )
        .await?;
        vec![MemoryScopeGrant {
            scope_type: "project".to_owned(),
            scope_id: normalized_project_id,
            visibility: vec!["project".to_owned()],
            identity_id: None,
        }]
    } else {
        visible_project_grants(&state, &user).await?
    };
    let access = MemoryAccessContext {
        identity_id: None,
        grants,
    };
    let result = state
        .memory_service
        .get_scoped(&access, item_uuid, params.layer)
        .await
        .map_err(|error| match error {
            services::ServiceError::NotFound { .. } => {
                ApiError::not_found("memory_item", id.clone())
            }
            other => ApiError::from(other),
        })?;
    Ok(Json(memory_result_dto(result, layer, 1.0, None)))
}

fn memory_result_dto(
    result: MemorySearchResult,
    layer: u8,
    score: f32,
    fallback_project_id: Option<String>,
) -> MemorySearchResultDto {
    let source_id = result
        .references
        .as_ref()
        .map(|references| references.source_ref.clone())
        .unwrap_or_else(|| result.id.to_string());
    let creator = result.creator.as_ref().and_then(|creator| {
        creator
            .creator_id
            .clone()
            .or_else(|| Some(creator.creator_type.clone()))
    });
    let references = result.references.as_ref();
    MemorySearchResultDto {
        id: result.id.to_string(),
        layer,
        content: result.body.or(result.summary).unwrap_or(result.title),
        score,
        source_type: result.kind.to_string(),
        source_id,
        project_id: references
            .and_then(|references| references.project_id.clone())
            .or(fallback_project_id)
            .unwrap_or_default(),
        task_id: references.and_then(|references| references.task_id.clone()),
        created_at: result.created_at.unwrap_or_default(),
        creator,
    }
}

fn response_layer(layer: Option<u8>, token_budget: Option<u32>) -> ApiResult<u8> {
    match layer {
        Some(value @ 1..=3) => Ok(value),
        Some(other) => Err(ApiError::bad_request(format!(
            "invalid memory layer {other}; expected 1, 2, or 3"
        ))),
        None => Ok(match token_budget {
            Some(budget) if budget < 200 => 1,
            Some(budget) if budget <= 1000 => 2,
            _ => 3,
        }),
    }
}

fn relevance_score(index: usize) -> f32 {
    1.0 / (index as f32 + 1.0)
}

fn parse_uuid(value: &str, field: &'static str) -> ApiResult<Uuid> {
    Uuid::parse_str(value)
        .map_err(|error| ApiError::bad_request(format!("invalid {field} UUID: {error}")))
}

async fn visible_project_grants(
    state: &AppState,
    user: &AuthenticatedUser,
) -> ApiResult<Vec<MemoryScopeGrant>> {
    let rows = sqlx::query(
        "SELECT project.id FROM project WHERE project.owner_id IS NULL OR project.owner_id = ? OR EXISTS (SELECT 1 FROM project_member WHERE project_member.project_id = project.id AND project_member.user_id = ?)",
    )
    .bind(&user.user_id)
    .bind(&user.user_id)
    .fetch_all(state.db.pool())
    .await
    .map_err(db::DbError::from)?;
    use sqlx::Row;
    rows.into_iter()
        .map(|row| {
            Ok(MemoryScopeGrant {
                scope_type: "project".to_owned(),
                scope_id: row.try_get("id")?,
                visibility: vec!["project".to_owned()],
                identity_id: None,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()
        .map_err(|error| ApiError::from(db::DbError::from(error)))
}
