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
async fn workflow_health_surfaces_interactive_execution() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let workflow = crate::workflow::default_workflow::default_workflow();
    let task = seed_task_with_status(
        &db,
        &project_id,
        crate::workflow::default_states::IN_PROGRESS,
    )
    .await;
    let execution = seed_execution(
        &db,
        &task.id,
        None,
        "interactive",
        ExecutionStatus::Running,
        Some("interactive-session"),
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
    assert_eq!(health.label, "Interactive");
    assert_eq!(health.role.as_deref(), Some("interactive"));
    assert_eq!(health.execution_id.as_deref(), Some(execution.id.as_str()));
}

#[tokio::test]
async fn workflow_health_interactive_execution_outranks_stale_failure_projection() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let workflow = crate::workflow::default_workflow::default_workflow();
    let task = seed_task_with_status(
        &db,
        &project_id,
        crate::workflow::default_states::IN_PROGRESS,
    )
    .await;
    let execution = seed_execution(
        &db,
        &task.id,
        None,
        "interactive",
        ExecutionStatus::Running,
        Some("interactive-recovery-session"),
        "2026-05-02T10:00:00Z",
    )
    .await;

    // Recovery opens the interactive session before its stale failure marker
    // is cleared. The live execution is the truthful board state.
    let mut stale_task = task;
    stale_task.failed_json = Some(
        serde_json::json!({
            "kind": "executor_failed",
            "reason": "stale failure from the interrupted attempt"
        })
        .to_string(),
    );
    stale_task.blocked_json = Some(
        serde_json::json!({
            "kind": "retry_exhausted",
            "reason": "stale retry projection"
        })
        .to_string(),
    );

    let health = crate::task_diagnostics::derive_workflow_health(
        &stale_task,
        &workflow,
        &[],
        None,
        Some(&execution),
        false,
        None,
    );

    assert_eq!(health.kind, api_types::WorkflowHealthKind::Running);
    assert_eq!(health.label, "Interactive");
    assert_eq!(health.role.as_deref(), Some("interactive"));
    assert_eq!(health.execution_id.as_deref(), Some(execution.id.as_str()));
}

#[tokio::test]
async fn workflow_health_surfaces_retry_waiting_for_capacity() {
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
    crate::deferred_dispatch::set(
        &db,
        &task,
        &task.status,
        "2000-01-01T00:00:00Z",
        "execution retry (attempt 1)",
    )
    .await
    .expect("retry deferral records");
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let role_assignments = TaskRoleAssignmentRepo::list_by_task(&*db, &task.id)
        .await
        .expect("role assignments load");

    let health = crate::task_diagnostics::derive_workflow_health(
        &task,
        &workflow,
        &role_assignments,
        None,
        None,
        false,
        None,
    );

    assert_eq!(health.kind, api_types::WorkflowHealthKind::WaitingForAgent);
    assert_eq!(health.label, "Retry Queued");
    assert_eq!(
        health.stale_reason.as_deref(),
        Some("execution_retry_waiting_for_capacity")
    );
    assert!(health
        .message
        .as_deref()
        .is_some_and(|message| message.contains("waiting for capacity")));
}

#[tokio::test]
async fn workflow_health_does_not_label_explicitly_nonretryable_execution_as_retry_queued() {
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
    let mut execution = seed_execution(
        &db,
        &task.id,
        Some(&coder_id),
        crate::workflow::default_roles::CODER,
        ExecutionStatus::Failed,
        Some("coder-session"),
        "2026-05-02T10:00:00Z",
    )
    .await;
    // `Some(None)` is the persisted, explicit "do not retry" policy. It is
    // distinct from the automatic retry path and must not be advertised as
    // waiting for capacity.
    execution.resume_policy = Some(db::ResumePolicy::None);
    let role_assignments = TaskRoleAssignmentRepo::list_by_task(&*db, &task.id)
        .await
        .expect("role assignments load");

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
    assert_eq!(health.label, "Execution Failed");
    assert_ne!(health.label, "Retry Queued");

    execution.resume_policy = Some(db::ResumePolicy::Auto);
    let health = crate::task_diagnostics::derive_workflow_health(
        &task,
        &workflow,
        &role_assignments,
        None,
        Some(&execution),
        false,
        None,
    );
    assert_eq!(health.kind, api_types::WorkflowHealthKind::WaitingForAgent);
    assert_eq!(health.label, "Retry Queued");
}

#[tokio::test]
async fn workflow_health_initial_queue_stops_at_user_owned_role() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let workflow = crate::workflow::default_workflow::default_workflow();
    let coder_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, crate::workflow::default_states::TODO).await;

    // The planner is explicitly owned by a human. The dispatcher treats that
    // as a stop condition, even though the later coder role has an Agent.
    seed_role_assignment(&db, &task.id, crate::workflow::default_roles::PLANNER, None).await;
    sqlx::query(
        "UPDATE task_role_assignment SET assignee_type = 'user', assignee_id = ? WHERE task_id = ? AND role_name = ?",
    )
    .bind("user-1")
    .bind(&task.id)
    .bind(crate::workflow::default_roles::PLANNER)
    .execute(db.pool())
    .await
    .expect("planner assignment becomes user-owned");
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

    let health = crate::task_diagnostics::derive_workflow_health(
        &task,
        &workflow,
        &role_assignments,
        None,
        None,
        false,
        None,
    );

    assert_eq!(health.kind, api_types::WorkflowHealthKind::Idle);
    assert_eq!(health.label, "Idle");
    assert_eq!(health.role, None);
}

#[tokio::test]
async fn workflow_health_ignores_malformed_deferred_timestamp() {
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
    crate::deferred_dispatch::set(
        &db,
        &task,
        &task.status,
        "not-a-timestamp",
        "malformed retry metadata",
    )
    .await
    .expect("malformed retry metadata records");
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let role_assignments = TaskRoleAssignmentRepo::list_by_task(&*db, &task.id)
        .await
        .expect("role assignments load");

    let health = crate::task_diagnostics::derive_workflow_health(
        &task,
        &workflow,
        &role_assignments,
        None,
        None,
        false,
        None,
    );

    assert_eq!(health.kind, api_types::WorkflowHealthKind::WaitingForAgent);
    assert_eq!(health.label, "Waiting for Agent");
    assert_ne!(
        health.stale_reason.as_deref(),
        Some("execution_retry_scheduled")
    );
}

#[tokio::test]
async fn workflow_health_ignores_malformed_deferred_target_state() {
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
    TaskRepo::mutate_metadata(
        &*db,
        &task.id,
        None,
        vec![db::TaskMetadataMutation::Set {
            key: "deferred_dispatch".to_owned(),
            value: serde_json::json!({
                "not_before": "2099-01-01T00:00:00Z",
                "reason": "malformed retry metadata",
                "target_state": 42,
            }),
        }],
        &db::now_rfc3339(),
    )
    .await
    .expect("malformed target state metadata records");
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let role_assignments = TaskRoleAssignmentRepo::list_by_task(&*db, &task.id)
        .await
        .expect("role assignments load");

    let health = crate::task_diagnostics::derive_workflow_health(
        &task,
        &workflow,
        &role_assignments,
        None,
        None,
        false,
        None,
    );

    assert_eq!(health.kind, api_types::WorkflowHealthKind::WaitingForAgent);
    assert_eq!(health.label, "Waiting for Agent");
    assert_ne!(
        health.stale_reason.as_deref(),
        Some("execution_retry_scheduled")
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
