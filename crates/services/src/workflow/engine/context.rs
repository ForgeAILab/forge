use db::{ExecutionRepo, ExecutionStatus, PageRequest, ReviewRepo, SortBy, SortOrder};

pub(super) async fn latest_review(
    db: &db::SqliteDb,
    task_id: &str,
) -> crate::Result<Option<db::Review>> {
    let reviews = ReviewRepo::list_by_task(db, task_id).await?;
    Ok(reviews
        .into_iter()
        .max_by_key(|review| review.attempt_number))
}

pub(super) async fn latest_execution_context(
    db: &db::SqliteDb,
    task_id: &str,
) -> crate::Result<Option<db::Execution>> {
    let page = ExecutionRepo::list_by_task(
        db,
        task_id,
        PageRequest {
            cursor: None,
            limit: 1,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await?;
    Ok(page.items.into_iter().next())
}

/// Whether the task currently has any running execution. Used by the
/// dispatch-failure fallback: a task may only be rolled back out of an active
/// state when no execution is actually driving it.
pub(super) async fn has_running_execution(db: &db::SqliteDb, task_id: &str) -> crate::Result<bool> {
    let page = ExecutionRepo::list_by_task(
        db,
        task_id,
        PageRequest {
            cursor: None,
            limit: 100,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await?;
    Ok(page
        .items
        .iter()
        .any(|execution| execution.status == ExecutionStatus::Running))
}

pub(super) async fn latest_executor_context(
    db: &db::SqliteDb,
    task_id: &str,
) -> crate::Result<Option<db::Execution>> {
    let page = ExecutionRepo::list_by_task(
        db,
        task_id,
        PageRequest {
            cursor: None,
            limit: 20,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await?;
    Ok(page
        .items
        .into_iter()
        .find(|execution| matches!(execution.role.as_str(), "coder" | "executor")))
}
