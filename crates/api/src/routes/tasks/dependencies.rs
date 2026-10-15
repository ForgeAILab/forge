use super::*;

pub async fn add_dependency(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Path(id): Path<String>,
    Json(request): Json<AddDependencyRequest>,
) -> ApiResult<StatusCode> {
    crate::routes::authorization::require_task_access(
        &state,
        &id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Write,
    )
    .await?;
    crate::routes::authorization::require_task_access(
        &state,
        &request.depends_on_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    state
        .task_service
        .add_task_dependency(&id, &request.depends_on_id)
        .await?;
    Ok(StatusCode::CREATED)
}

pub async fn remove_dependency(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Path((id, dep_id)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
    crate::routes::authorization::require_task_access(
        &state,
        &id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Write,
    )
    .await?;
    crate::routes::authorization::require_task_access(
        &state,
        &dep_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    state
        .task_service
        .remove_task_dependency(&id, &dep_id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn list_dependencies(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<Vec<TaskDependency>>> {
    crate::routes::authorization::require_task_access(
        &state,
        &id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    let dependencies = TaskDependencyRepo::list_dependencies(&*state.db, &id).await?;
    let mut visible = Vec::new();
    for linked_id in dependencies {
        if crate::routes::authorization::require_task_access(
            &state,
            &linked_id,
            &user.user_id,
            crate::routes::authorization::ProjectAccess::Read,
        )
        .await
        .is_ok()
        {
            visible.push(linked_id);
        }
    }
    let dependencies = visible;
    Ok(Json(
        dependencies_with_created_at(&state.db, id, dependencies).await?,
    ))
}

pub async fn list_dependents(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<Vec<TaskDependency>>> {
    crate::routes::authorization::require_task_access(
        &state,
        &id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    let dependents = TaskDependencyRepo::list_dependents(&*state.db, &id).await?;
    let mut visible = Vec::new();
    for linked_id in dependents {
        if crate::routes::authorization::require_task_access(
            &state,
            &linked_id,
            &user.user_id,
            crate::routes::authorization::ProjectAccess::Read,
        )
        .await
        .is_ok()
        {
            visible.push(linked_id);
        }
    }
    let dependents = visible;
    Ok(Json(
        dependents_with_created_at(&state.db, id, dependents).await?,
    ))
}

async fn dependencies_with_created_at(
    db: &db::SqliteDb,
    task_id: String,
    dependencies: Vec<String>,
) -> ApiResult<Vec<TaskDependency>> {
    let mut response = Vec::with_capacity(dependencies.len());
    for depends_on_id in dependencies {
        let created_at = dependency_created_at(db, &task_id, &depends_on_id).await?;
        response.push(TaskDependency {
            task_id: task_id.clone(),
            depends_on_id,
            created_at,
        });
    }
    Ok(response)
}

async fn dependents_with_created_at(
    db: &db::SqliteDb,
    depends_on_id: String,
    dependents: Vec<String>,
) -> ApiResult<Vec<TaskDependency>> {
    let mut response = Vec::with_capacity(dependents.len());
    for task_id in dependents {
        let created_at = dependency_created_at(db, &task_id, &depends_on_id).await?;
        response.push(TaskDependency {
            task_id,
            depends_on_id: depends_on_id.clone(),
            created_at,
        });
    }
    Ok(response)
}

async fn dependency_created_at(
    db: &db::SqliteDb,
    task_id: &str,
    depends_on_id: &str,
) -> ApiResult<String> {
    sqlx::query_scalar::<_, String>(
        "SELECT created_at FROM task_dependency WHERE task_id = ? AND depends_on_id = ?",
    )
    .bind(task_id)
    .bind(depends_on_id)
    .fetch_one(db.pool())
    .await
    .map_err(db::DbError::from)
    .map_err(ApiError::from)
}
