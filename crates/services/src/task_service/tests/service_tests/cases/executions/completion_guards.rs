//! Completion guards and stale execution failure fences.
use super::*;

#[tokio::test]
async fn executor_completion_guard_rejection_follows_up_before_blocking() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service =
        TaskService::new(Arc::clone(&db), event_bus).with_task_executor(Arc::new(PendingExecutor));
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let workspace =
        seed_workspace_with_plan(&db, &task, &repo_id, "- [ ] finish implementation\n").await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        Some(&agent_id),
    )
    .await;
    let execution =
        seed_completed_coder_execution(&db, &task, &agent_id, Some(&workspace.id)).await;

    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("guard rejection dispatches follow-up");

    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 20,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions load");
    let resumed = executions
        .items
        .iter()
        .find(|candidate| candidate.status == ExecutionStatus::Running)
        .expect("lease-backed follow-up exists");
    assert_eq!(
        resumed.parent_execution_id.as_deref(),
        Some(execution.id.as_str())
    );
    let execution = ExecutionRepo::get_by_id(&*db, &resumed.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(execution.status, ExecutionStatus::Running);
    assert!(execution.summary.as_deref().is_some_and(|summary| {
        summary.contains("Workflow guard failed: require_plan_checklist_complete")
    }));
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(
        json!(
            db::budget::spent(db.pool(), &task.id, db::budget::Kind::WorkflowGuard.key())
                .await
                .unwrap()
        ),
        json!(1)
    );
    assert!(task.blocked_json.is_none());
}

#[tokio::test]
async fn superseded_project_revision_cannot_apply_completed_coder_guard_effects() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let workspace =
        seed_workspace_with_plan(&db, &task, &repo_id, "- [ ] finish implementation\n").await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        Some(&agent_id),
    )
    .await;
    let execution =
        seed_completed_coder_execution(&db, &task, &agent_id, Some(&workspace.id)).await;

    sqlx::query("UPDATE project SET version = version + 1 WHERE id = ?")
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project revision advances");

    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("superseded completion is inert");

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, "in_progress");
    let metadata = current.metadata().expect("metadata parses");
    assert!(metadata.extra.get("workflow_guard_retry_count").is_none());
    assert!(current.blocked_json.is_none());
    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 20,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions load");
    assert_eq!(executions.items.len(), 1, "no follow-up may be launched");
}

/// A Project edit, pause or resume moves the Project version. A run that
/// then fails must still be retried: dropping the failure left the Task
/// active with no run, no retry and no park (model-based workflow test,
/// `a_run_that_fails_after_a_project_pause_is_retried`).
#[tokio::test]
async fn failed_execution_from_a_superseded_project_revision_is_still_retried() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        Some(&agent_id),
    )
    .await;
    let execution = seed_completed_coder_execution(&db, &task, &agent_id, None).await;
    sqlx::query(
        "UPDATE execution
         SET status = 'failed', error = 'executor failed', resume_policy = 'manual'
         WHERE id = ?",
    )
    .bind(&execution.id)
    .execute(db.pool())
    .await
    .expect("execution fails");
    let execution = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");

    sqlx::query("UPDATE project SET version = version + 1 WHERE id = ?")
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project revision advances");

    service
        .annotate_executor_failure_block(&execution)
        .await
        .expect("the failure is handled under the current revision");

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let metadata = current.metadata().expect("metadata parses");
    assert!(
        metadata.extra.get("deferred_dispatch").is_some(),
        "the retry is scheduled"
    );
    assert_eq!(
        metadata
            .extra
            .get("last_execution_failure_execution_id")
            .and_then(Value::as_str),
        Some(execution.id.as_str()),
        "the failure is recorded once"
    );
    assert!(current.blocked_json.is_none());
    assert!(current.error_annotation.is_none());
}

/// The fence on a failure is "still the Task's current run", not the Project
/// revision: a late failure of a run that a newer run replaced must not
/// schedule a second run beside it, spend budget, or block the Task.
#[tokio::test]
async fn failure_of_a_run_replaced_by_a_newer_run_is_inert_on_every_path() {
    let (db, service, task, agent_id) = failure_fence_fixture().await;
    let old =
        seed_failed_coder_execution_from_an_older_revision(&db, &task, &agent_id, json!({})).await;
    sqlx::query("UPDATE execution SET created_at = '2020-01-01T00:00:00Z' WHERE id = ?")
        .bind(&old.id)
        .execute(db.pool())
        .await
        .expect("old run predates the new one");
    let newer = seed_completed_coder_execution(&db, &task, &agent_id, None).await;
    sqlx::query("UPDATE execution SET status = 'running' WHERE id = ?")
        .bind(&newer.id)
        .execute(db.pool())
        .await
        .expect("newer run is live");

    service
        .annotate_executor_failure_block(&old)
        .await
        .expect("server failure path");
    assert_failure_was_inert(&db, &task, "server failure path").await;
    service
        .annotate_executor_unavailable_block(&old, Some(now_rfc3339()), vec![])
        .await
        .expect("usage-limit path");
    assert_failure_was_inert(&db, &task, "usage-limit path").await;
    service
        .settle_remote_terminal_postcommit(&old, &failed_terminal_notification(&old), false)
        .await
        .expect("daemon terminal path");
    assert_failure_was_inert(&db, &task, "daemon terminal path").await;
}

/// A Task that left the state and came back (sent back, re-planned, reopened)
/// is on a new entry of it: the old entry's run cannot charge the new one.
#[tokio::test]
async fn failure_of_a_run_from_an_earlier_state_entry_is_inert_on_every_path() {
    let (db, service, task, agent_id) = failure_fence_fixture().await;
    let old = seed_failed_coder_execution_from_an_older_revision(
        &db,
        &task,
        &agent_id,
        json!({ "task_state": "in_progress", "state_entry_token": "an-earlier-entry" }),
    )
    .await;

    service
        .annotate_executor_failure_block(&old)
        .await
        .expect("server failure path");
    assert_failure_was_inert(&db, &task, "server failure path").await;
    service
        .annotate_executor_unavailable_block(&old, Some(now_rfc3339()), vec![])
        .await
        .expect("usage-limit path");
    assert_failure_was_inert(&db, &task, "usage-limit path").await;
    service
        .settle_remote_terminal_postcommit(&old, &failed_terminal_notification(&old), false)
        .await
        .expect("daemon terminal path");
    assert_failure_was_inert(&db, &task, "daemon terminal path").await;
}

/// A Task the Project moved on from (cancelled, or in a state another role
/// owns) is not blocked or retried by the old run's failure.
#[tokio::test]
async fn failure_of_a_run_whose_task_moved_on_is_inert() {
    for status in ["cancelled", "review", "todo"] {
        let (db, service, task, agent_id) = failure_fence_fixture().await;
        let old =
            seed_failed_coder_execution_from_an_older_revision(&db, &task, &agent_id, json!({}))
                .await;
        sqlx::query("UPDATE task SET status = ? WHERE id = ?")
            .bind(status)
            .bind(&task.id)
            .execute(db.pool())
            .await
            .expect("task moves on");
        service
            .annotate_executor_failure_block(&old)
            .await
            .expect("server failure path");
        service
            .settle_remote_terminal_postcommit(&old, &failed_terminal_notification(&old), false)
            .await
            .expect("daemon terminal path");
        assert_failure_was_inert(&db, &task, status).await;
    }
}

/// The daemon terminal path retries a failure from an older Project revision,
/// and a redelivered failure (or the dispatcher's healer after it) spends the
/// retry budget once.
#[tokio::test]
async fn daemon_reported_failure_from_an_older_revision_is_retried_and_charged_once() {
    let (db, service, task, agent_id) = failure_fence_fixture().await;
    let old =
        seed_failed_coder_execution_from_an_older_revision(&db, &task, &agent_id, json!({})).await;

    for _ in 0..2 {
        service
            .settle_remote_terminal_postcommit(&old, &failed_terminal_notification(&old), false)
            .await
            .expect("daemon terminal path");
        let old = ExecutionRepo::get_by_id(&*db, &old.id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        service
            .annotate_executor_failure_block(&old)
            .await
            .expect("healer path");
    }

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let metadata = current.metadata().expect("metadata parses");
    assert!(
        metadata.extra.get("deferred_dispatch").is_some(),
        "the retry is scheduled"
    );
    assert_eq!(
        db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
            .await
            .expect("budget loads"),
        1,
        "one failure spends one retry"
    );
    assert!(current.blocked_json.is_none());
}

#[tokio::test]
async fn superseded_execution_cannot_mutate_workflow_guard_retry_count() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let project_version = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists")
        .version;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let workspace =
        seed_workspace_with_plan(&db, &task, &repo_id, "- [ ] finish implementation\n").await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        Some(&agent_id),
    )
    .await;
    let old = seed_completed_coder_execution(&db, &task, &agent_id, Some(&workspace.id)).await;
    ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: crate::workflow::default_roles::CODER.to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: Some(old.id.clone()),
            agent_session_id: Some("newer-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                json!({
                    "executor_type": "shell",
                    "config": {},
                    "project_version": project_version,
                })
                .to_string(),
            ),
            workspace_id: Some(workspace.id),
            created_at: "9999-01-01T00:00:00Z".to_owned(),
            updated_at: "9999-01-01T00:00:00Z".to_owned(),
        },
    )
    .await
    .expect("newer execution creates");

    service
        .maybe_cascade_executor_completion(&old.id)
        .await
        .expect("superseded completion is inert");

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let metadata = current.metadata().expect("metadata parses");
    assert!(metadata.extra.get("workflow_guard_retry_count").is_none());
    assert!(current.blocked_json.is_none());
}

#[tokio::test]
async fn subtask_sequence_guard_rejection_never_resumes_root_coder() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service =
        TaskService::new(Arc::clone(&db), event_bus).with_task_executor(Arc::new(PendingExecutor));
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let _subtask = seed_subtask_with_status(&db, &task, "child", "todo".to_owned(), 0).await;
    let workspace = seed_workspace_with_plan(&db, &task, &repo_id, "- [x] parent work\n").await;
    let execution =
        seed_completed_coder_execution(&db, &task, &agent_id, Some(&workspace.id)).await;

    let result = service
        .maybe_cascade_executor_completion(&execution.id)
        .await;
    assert!(
        result.is_ok(),
        "a coder completion without coordination-root role authority is inert"
    );

    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 20,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions load");
    assert!(
        executions
            .items
            .iter()
            .all(|candidate| candidate.status != ExecutionStatus::Running),
        "coordination roots must not receive a coder follow-up"
    );

    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let metadata = task.metadata().expect("metadata parses");
    assert!(metadata.extra.get("workflow_guard_retry_count").is_none());
    assert!(task.blocked_json.is_none());
}

#[tokio::test]
async fn executor_completion_guard_rejection_blocks_when_retry_budget_exhausted() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let task = TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            task_state_config: Some(Some(r#"{"retry_budgets":{"execution":0}}"#.to_owned())),
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("task config updates");
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        Some(&agent_id),
    )
    .await;
    let workspace =
        seed_workspace_with_plan(&db, &task, &repo_id, "- [ ] finish implementation\n").await;
    let execution =
        seed_completed_coder_execution(&db, &task, &agent_id, Some(&workspace.id)).await;

    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("guard rejection blocks after exhausted budget");

    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let blocked = task.blocked_json.expect("task blocked");
    assert!(blocked.contains("workflow_guard_rejected"));
    let annotation = task.error_annotation.expect("annotation recorded");
    assert!(annotation.contains("require_plan_checklist_complete"));
}

#[tokio::test]
async fn executor_completion_comment_uses_execution_agent() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        Some(&agent_id),
    )
    .await;
    let now = now_rfc3339();
    let execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "executor".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("implemented the change".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                workflow_execution_snapshot(&db, &project_id).await,
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("execution creates");

    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("cascade check succeeds");

    let comments = TaskCommentRepo::list_comments(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Asc,
        },
    )
    .await
    .expect("comments list");
    let comment = comments
        .items
        .iter()
        .find(|comment| comment.content.contains("Agent completed execution"))
        .expect("executor completion comment exists");
    assert_eq!(comment.author_type, CommentAuthorType::Agent);
    assert_eq!(comment.author_id.as_deref(), Some(agent_id.as_str()));
    assert_eq!(comment.author_name, "shell");
}
