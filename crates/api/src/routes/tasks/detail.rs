use super::*;
use api_types::{
    TaskDetailExecutionsPage, TaskDetailResponse, TaskRelationSummary, TaskRelationsResponse,
};
use futures_util::stream::{self, StreamExt, TryStreamExt};

const EXECUTION_USAGE_CONCURRENCY: usize = 8;

pub async fn get_task_detail(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(params): Query<ListParams>,
) -> ApiResult<Json<TaskDetailResponse>> {
    let task = TaskRepo::get_by_id(&*state.db, &id, false)
        .await?
        .ok_or_else(|| ApiError::not_found("task", id.clone()))?;
    let execution_page_request = page_request(&params)?;
    let task_id = task.id.clone();

    // Task projection and execution history read independent rows. Run their
    // database work together so this bootstrap request costs one network hop
    // without adding the two backend latencies together.
    let awaiting_human = state.task_service.is_task_awaiting_human(&task).await?;
    let task_response =
        task_response_and_workflow_with_awaiting_human(&state.db, task, awaiting_human);
    let db = &*state.db;
    let executions = async move {
        ExecutionRepo::list_by_task(db, &task_id, execution_page_request)
            .await
            .map_err(ApiError::from)
    };
    let ((task, workflow), executions) = tokio::try_join!(task_response, executions)?;

    // A page can contain 100 executions while SQLite has a much smaller
    // connection pool. `try_join_all` queued one usage query per row at once;
    // a small ordered buffer preserves parallelism without flooding the pool.
    let items = stream::iter(
        executions
            .items
            .into_iter()
            .map(|execution| execution_response_with_usage(&state.db, execution)),
    )
    .buffered(EXECUTION_USAGE_CONCURRENCY)
    .try_collect::<Vec<_>>()
    .await?;
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
    Path(id): Path<String>,
) -> ApiResult<Json<TaskRelationsResponse>> {
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
    let (parent, subtasks, dependencies, dependency_ids, dependents) =
        tokio::try_join!(parent, subtasks, dependencies, dependency_ids, dependents)?;
    let visible_dependency_ids: std::collections::HashSet<_> =
        dependencies.iter().map(|task| task.id.clone()).collect();
    let mut missing_dependency_ids = dependency_ids
        .into_iter()
        .filter(|dependency_id| !visible_dependency_ids.contains(dependency_id))
        .collect::<Vec<_>>();
    missing_dependency_ids.sort();

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
