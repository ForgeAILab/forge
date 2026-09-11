use super::helpers::*;
use super::*;

#[tokio::test]
async fn test_workflow_health_stuck_no_execution() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let workflow = crate::workflow::default_workflow::default_workflow();
    let reviewer_id = seed_agent(&db).await;
    let stale_timestamp = "2000-01-01T00:00:00Z";
    let assigned_task = seed_task_with_status_at(
        &db,
        &project_id,
        crate::workflow::default_states::REVIEW,
        stale_timestamp,
    )
    .await;
    seed_role_assignment(
        &db,
        &assigned_task.id,
        crate::workflow::default_roles::REVIEWER,
        Some(&reviewer_id),
    )
    .await;
    let role_assignments = TaskRoleAssignmentRepo::list_by_task(&*db, &assigned_task.id)
        .await
        .expect("role assignments load");

    let health = crate::task_diagnostics::derive_workflow_health(
        &assigned_task,
        &workflow,
        &role_assignments,
        None,
        None,
        false,
        None,
    );

    assert_eq!(health.kind, api_types::WorkflowHealthKind::WaitingForAgent);
    assert_eq!(
        health.role.as_deref(),
        Some(crate::workflow::default_roles::REVIEWER)
    );

    let unassigned_task = seed_task_with_status_at(
        &db,
        &project_id,
        crate::workflow::default_states::REVIEW,
        stale_timestamp,
    )
    .await;
    let role_assignments = TaskRoleAssignmentRepo::list_by_task(&*db, &unassigned_task.id)
        .await
        .expect("role assignments load");

    let health = crate::task_diagnostics::derive_workflow_health(
        &unassigned_task,
        &workflow,
        &role_assignments,
        None,
        None,
        false,
        None,
    );

    assert_eq!(health.kind, api_types::WorkflowHealthKind::WaitingForAgent);
    assert_eq!(
        health.role.as_deref(),
        Some(crate::workflow::default_roles::REVIEWER)
    );
}

#[tokio::test]
async fn test_workflow_health_running_reviewer() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let workflow = crate::workflow::default_workflow::default_workflow();
    let reviewer_id = seed_agent(&db).await;
    let task =
        seed_task_with_status(&db, &project_id, crate::workflow::default_states::REVIEW).await;
    let execution = seed_execution(
        &db,
        &task.id,
        Some(&reviewer_id),
        crate::workflow::default_roles::REVIEWER,
        ExecutionStatus::Running,
        Some("review-session"),
        "2026-05-02T10:00:00Z",
    )
    .await;

    let health = crate::task_diagnostics::derive_workflow_health(
        &task,
        &workflow,
        &[],
        None,
        Some(&execution),
        false,
        None,
    );

    assert_eq!(health.kind, api_types::WorkflowHealthKind::Running);
    assert_eq!(health.execution_id.as_deref(), Some(execution.id.as_str()));
    assert_eq!(
        health.role.as_deref(),
        Some(crate::workflow::default_roles::REVIEWER)
    );
}

#[tokio::test]
async fn test_workflow_health_stuck_when_coder_completed_without_transition() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let workflow = crate::workflow::default_workflow::default_workflow();
    let coder_id = seed_agent(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        crate::workflow::default_states::IN_PROGRESS,
    )
    .await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        Some(&coder_id),
    )
    .await;
    let role_assignments = TaskRoleAssignmentRepo::list_by_task(&*db, &task.id)
        .await
        .expect("role assignments load");
    let execution = seed_execution(
        &db,
        &task.id,
        Some(&coder_id),
        crate::workflow::default_roles::CODER,
        ExecutionStatus::Completed,
        Some("coder-session"),
        "2026-05-02T10:00:00Z",
    )
    .await;

    let health = crate::task_diagnostics::derive_workflow_health(
        &task,
        &workflow,
        &role_assignments,
        None,
        Some(&execution),
        false,
        None,
    );

    assert_eq!(health.kind, api_types::WorkflowHealthKind::Stuck);
    assert_eq!(health.severity, api_types::HealthSeverity::Warning);
    assert_eq!(health.execution_id.as_deref(), Some(execution.id.as_str()));
    assert_eq!(
        health.stale_reason.as_deref(),
        Some("execution_completed_without_transition")
    );
}

#[tokio::test]
async fn test_workflow_health_failed_when_coder_failed_without_block_marker() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let workflow = crate::workflow::default_workflow::default_workflow();
    let coder_id = seed_agent(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        crate::workflow::default_states::IN_PROGRESS,
    )
    .await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        Some(&coder_id),
    )
    .await;
    let role_assignments = TaskRoleAssignmentRepo::list_by_task(&*db, &task.id)
        .await
        .expect("role assignments load");
    let execution = seed_execution(
        &db,
        &task.id,
        Some(&coder_id),
        crate::workflow::default_roles::CODER,
        ExecutionStatus::Failed,
        Some("coder-session"),
        "2026-05-02T10:00:00Z",
    )
    .await;

    let health = crate::task_diagnostics::derive_workflow_health(
        &task,
        &workflow,
        &role_assignments,
        None,
        Some(&execution),
        false,
        None,
    );

    assert_eq!(health.kind, api_types::WorkflowHealthKind::Failed);
    assert_eq!(health.severity, api_types::HealthSeverity::Error);
    assert_eq!(health.execution_id.as_deref(), Some(execution.id.as_str()));
    assert_eq!(
        health.stale_reason.as_deref(),
        Some("execution_failed_without_task_block")
    );
}
