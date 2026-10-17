use super::*;
use api_types::{
    TaskDetailExecutionsPage, TaskDetailResponse, TaskRelationSummary, TaskRelationsResponse,
};

pub async fn get_task_detail(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Path(id): Path<String>,
    Query(params): Query<ListParams>,
) -> ApiResult<Json<TaskDetailResponse>> {
    crate::routes::authorization::require_task_access(
        &state,
        &id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    let task = TaskRepo::get_by_id(&*state.db, &id, false)
        .await?
        .ok_or_else(|| ApiError::not_found("task", id.clone()))?;
    let execution_page_request = page_request(&params)?;
    let task_id = task.id.clone();

    // Task projection and execution history read independent rows. Run their
    // database work together so this bootstrap request costs one network hop
    // without adding the two backend latencies together.
    let awaiting_human = state.task_service.is_task_awaiting_human(&task).await?;
    let task_response = task_response_and_workflow_with_awaiting_human(
        &state.db,
        &state.workspace_backend_router,
        task,
        awaiting_human,
    );
    let db = &*state.db;
    let executions = async move {
        ExecutionRepo::list_by_task(db, &task_id, execution_page_request)
            .await
            .map_err(ApiError::from)
    };
    let ((task, workflow), executions) = tokio::try_join!(task_response, executions)?;

    let items = executions
        .items
        .into_iter()
        .map(execution_summary_response)
        .collect();
    let has_more = executions.next_cursor.is_some();

    Ok(Json(TaskDetailResponse {
        task,
        workflow,
        executions: TaskDetailExecutionsPage {
            items,
            next_cursor: executions.next_cursor,
            has_more,
            total_count: executions
                .total_count
                .and_then(|count| u64::try_from(count).ok()),
        },
    }))
}

pub async fn get_task_relations(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<TaskRelationsResponse>> {
    crate::routes::authorization::require_task_access(
        &state,
        &id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    let task = TaskRepo::get_by_id(&*state.db, &id, false)
        .await?
        .ok_or_else(|| ApiError::not_found("task", id.clone()))?;
    let parent_task_id = task.parent_task_id;
    let db = &*state.db;
    let parent = async {
        match parent_task_id.as_deref() {
            Some(parent_id) => TaskRepo::get_by_id(db, parent_id, false)
                .await
                .map(|task| task.map(task_relation_summary))
                .map_err(ApiError::from),
            None => Ok(None),
        }
    };
    let subtasks = async {
        TaskRepo::list_subtasks_ordered(db, &id)
            .await
            .map_err(ApiError::from)
    };
    let dependencies = async {
        TaskDependencyRepo::list_dependency_tasks(db, &id)
            .await
            .map_err(ApiError::from)
    };
    let dependency_ids = async {
        TaskDependencyRepo::list_dependencies(db, &id)
            .await
            .map_err(ApiError::from)
    };
    let dependents = async {
        TaskDependencyRepo::list_dependent_tasks(db, &id)
            .await
            .map_err(ApiError::from)
    };
    let (mut parent, subtasks, dependencies, dependency_ids, dependents) =
        tokio::try_join!(parent, subtasks, dependencies, dependency_ids, dependents)?;
    let visible_dependency_ids: std::collections::HashSet<_> =
        dependencies.iter().map(|task| task.id.clone()).collect();
    let missing_dependency_ids = dependency_ids
        .into_iter()
        .filter(|dependency_id| !visible_dependency_ids.contains(dependency_id))
        .collect::<Vec<_>>();
    let mut missing_visible = Vec::new();
    for dependency_id in missing_dependency_ids {
        if let Some(dependency) = TaskRepo::get_by_id(&*state.db, &dependency_id, true).await? {
            if ProjectRepo::get_visible_by_id(&*state.db, &dependency.project_id, &user.user_id)
                .await?
                .is_some()
            {
                missing_visible.push(dependency_id);
            }
        }
    }
    let mut missing_dependency_ids = missing_visible;
    missing_dependency_ids.sort();
    if let Some(value) = &parent {
        if crate::routes::authorization::require_task_access(
            &state,
            &value.id,
            &user.user_id,
            crate::routes::authorization::ProjectAccess::Read,
        )
        .await
        .is_err()
        {
            parent = None;
        }
    }
    let subtasks = visible_relation_tasks(&state, subtasks, &user.user_id).await?;
    let dependencies = visible_relation_tasks(&state, dependencies, &user.user_id).await?;
    let dependents = visible_relation_tasks(&state, dependents, &user.user_id).await?;

    Ok(Json(TaskRelationsResponse {
        parent,
        subtasks: subtasks
            .into_iter()
            .filter(|subtask| subtask.archived_at.is_none())
            .map(task_relation_summary)
            .collect(),
        dependencies: dependencies
            .into_iter()
            .map(task_relation_summary)
            .collect(),
        missing_dependency_ids,
        dependents: dependents.into_iter().map(task_relation_summary).collect(),
    }))
}

fn task_relation_summary(task: db::Task) -> TaskRelationSummary {
    TaskRelationSummary {
        id: task.id,
        title: task.title,
        status: task.status,
        parent_task_id: task.parent_task_id,
        subtask_order: task.subtask_order,
        created_at: task.created_at,
    }
}

async fn visible_relation_tasks(
    state: &AppState,
    tasks: Vec<db::Task>,
    user_id: &str,
) -> ApiResult<Vec<db::Task>> {
    let mut visible = Vec::new();
    for task in tasks {
        if ProjectRepo::get_visible_by_id(&*state.db, &task.project_id, user_id)
            .await?
            .is_some()
        {
            visible.push(task);
        }
    }
    Ok(visible)
}
