//! Manual-stop annotations, replacement races and merge-fix completion.
use super::*;

#[tokio::test]
async fn manual_stop_annotation_serializes_metadata_edits_without_cas_retry() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let execution = seed_running_coder_execution(&db, &task.id, None, None).await;
    let workflow_definition = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists")
        .workflow_definition;
    let stale_task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("stale task loads")
        .expect("task exists");

    let concurrently_updated = TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: stale_task.version,
            title: Some("concurrent task update".to_owned()),
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("concurrent task update commits");

    service
        .persist_manual_stop_annotation(
            &execution,
            &stale_task,
            "in_progress",
            None,
            Some(crate::workflow::default_roles::CODER),
            &workflow_definition,
            None,
            serde_json::json!({
                "type": "manual_stop",
                "blocked_execution_id": execution.id,
            })
            .to_string(),
            now_rfc3339(),
        )
        .await
        .expect("manual-stop annotation applies under the Task lease after a metadata edit");

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("current task loads")
        .expect("task exists");
    assert_eq!(current.title, concurrently_updated.title);
    assert!(current
        .error_annotation
        .as_deref()
        .is_some_and(|annotation| annotation.contains(&execution.id)));
}

#[tokio::test]
async fn manual_stop_annotation_skips_after_same_role_replacement_starts() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let execution = seed_running_coder_execution(&db, &task.id, None, None).await;
    let workflow_definition = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists")
        .workflow_definition;
    let stale_task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("stale task loads")
        .expect("task exists");
    TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: stale_task.version,
            title: Some("concurrent task update".to_owned()),
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("concurrent task update commits");
    let _replacement = seed_running_coder_execution(&db, &task.id, None, None).await;

    service
        .persist_manual_stop_annotation(
            &execution,
            &stale_task,
            "in_progress",
            None,
            Some(crate::workflow::default_roles::CODER),
            &workflow_definition,
            None,
            serde_json::json!({
                "type": "manual_stop",
                "blocked_execution_id": execution.id,
            })
            .to_string(),
            now_rfc3339(),
        )
        .await
        .expect("stale stop does not fail when replacement is running");

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("current task loads")
        .expect("task exists");
    assert!(current.error_annotation.is_none());
}

#[tokio::test]
async fn manual_stop_annotation_skips_after_same_role_reassignment() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let old_agent_id = seed_agent(&db).await;
    let new_agent_id = seed_agent(&db).await;
    TaskRoleAssignmentRepo::assign(
        &*db,
        role_assignment_input(
            &task.id,
            crate::workflow::default_roles::CODER,
            Some(old_agent_id),
            None,
        ),
    )
    .await
    .expect("initial coder assignment creates");
    let execution = seed_execution(
        &db,
        &task.id,
        None,
        "executor",
        ExecutionStatus::Completed,
        None,
        &now_rfc3339(),
    )
    .await;
    let workflow_definition = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists")
        .workflow_definition;
    let stale_task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("stale task loads")
        .expect("task exists");
    let old_assignment = TaskRoleAssignmentRepo::get_by_task_and_role(
        &*db,
        &task.id,
        crate::workflow::default_roles::CODER,
    )
    .await
    .expect("old assignment loads")
    .expect("old assignment exists");

    TaskRoleAssignmentRepo::assign(
        &*db,
        role_assignment_input(
            &task.id,
            crate::workflow::default_roles::CODER,
            Some(new_agent_id),
            None,
        ),
    )
    .await
    .expect("same-role reassignment commits");

    let db_result = TaskRepo::set_error_annotation_if_no_running_execution(
        &*db,
        &stale_task.id,
        stale_task.version,
        "in_progress",
        None,
        &workflow_definition,
        Some(crate::workflow::default_roles::CODER),
        Some(old_assignment.clone()),
        "stale-stop",
        &now_rfc3339(),
        &execution.id,
        None,
        Vec::new(),
    )
    .await;
    assert!(
        matches!(db_result, Err(db::DbError::VersionConflict)),
        "assignment CAS must reject a stale same-role stop: {db_result:?}"
    );

    service
        .persist_manual_stop_annotation(
            &execution,
            &stale_task,
            "in_progress",
            None,
            Some(crate::workflow::default_roles::CODER),
            &workflow_definition,
            Some(old_assignment),
            serde_json::json!({
                "type": "manual_stop",
                "blocked_execution_id": execution.id,
            })
            .to_string(),
            now_rfc3339(),
        )
        .await
        .expect("stale stop does not fail after reassignment");

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("current task loads")
        .expect("task exists");
    assert!(current.error_annotation.is_none());
}

#[tokio::test]
async fn manual_stop_annotation_rejects_stale_state_entry_after_cycle() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let execution = seed_running_coder_execution(&db, &task.id, None, None).await;
    let workflow_definition = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists")
        .workflow_definition;
    let stale_task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("stale task loads")
        .expect("task exists");

    let in_review = TaskRepo::update_status(
        &*db,
        db::UpdateTaskStatus {
            id: task.id.clone(),
            expected_version: stale_task.version,
            status: "review".to_owned(),
            assignee_id: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("first state transition commits");
    TransitionLogRepo::insert(
        &*db,
        db::CreateTransitionLog {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            from_state: "in_progress".to_owned(),
            to_state: "review".to_owned(),
            trigger_name: Some("test_cycle".to_owned()),
            triggered_by: "system:test".to_owned(),
            bridge: Default::default(),
            trigger_reason: "state-entry epoch regression".to_owned(),
            hook_results_json: None,
            rejection: false,
            created_at: "2099-01-01T00:00:01Z".to_owned(),
        },
    )
    .await
    .expect("first transition log commits");
    let back_in_progress = TaskRepo::update_status(
        &*db,
        db::UpdateTaskStatus {
            id: task.id.clone(),
            expected_version: in_review.version,
            status: "in_progress".to_owned(),
            assignee_id: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("return transition commits");
    TransitionLogRepo::insert(
        &*db,
        db::CreateTransitionLog {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            from_state: "review".to_owned(),
            to_state: "in_progress".to_owned(),
            trigger_name: Some("test_cycle".to_owned()),
            triggered_by: "system:test".to_owned(),
            bridge: Default::default(),
            trigger_reason: "state-entry epoch regression".to_owned(),
            hook_results_json: None,
            rejection: false,
            created_at: "2099-01-01T00:00:02Z".to_owned(),
        },
    )
    .await
    .expect("return transition log commits");

    // A caller that retries with the current Task version still cannot use the
    // initial/no-log token captured before the X -> Y -> X cycle.
    let db_result = TaskRepo::set_error_annotation_if_no_running_execution(
        &*db,
        &task.id,
        back_in_progress.version,
        "in_progress",
        None,
        &workflow_definition,
        Some(crate::workflow::default_roles::CODER),
        None,
        "stale-stop",
        &now_rfc3339(),
        &execution.id,
        None,
        Vec::new(),
    )
    .await;
    assert!(
        matches!(db_result, Err(db::DbError::VersionConflict)),
        "state-entry CAS must reject the old initial epoch: {db_result:?}"
    );

    service
        .persist_manual_stop_annotation(
            &execution,
            &stale_task,
            "in_progress",
            None,
            Some(crate::workflow::default_roles::CODER),
            &workflow_definition,
            None,
            serde_json::json!({
                "type": "manual_stop",
                "blocked_execution_id": execution.id,
            })
            .to_string(),
            now_rfc3339(),
        )
        .await
        .expect("stale stop does not fail after state cycle");

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("current task loads")
        .expect("task exists");
    assert!(current.error_annotation.is_none());
}

#[tokio::test]
async fn state_entry_authority_breaks_timestamp_ties_by_insertion_order() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let workflow_definition = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists")
        .workflow_definition;
    let tied_created_at = "2099-01-01T00:00:00Z";
    let older_id = "zzzz-older-state-entry";
    let newer_id = "aaaa-newer-state-entry";

    for id in [older_id, newer_id] {
        TransitionLogRepo::insert(
            &*db,
            db::CreateTransitionLog {
                id: id.to_owned(),
                task_id: task.id.clone(),
                from_state: "review".to_owned(),
                to_state: "in_progress".to_owned(),
                trigger_name: Some("test_tie".to_owned()),
                triggered_by: "system:test".to_owned(),
                bridge: Default::default(),
                trigger_reason: "state-entry timestamp tie regression".to_owned(),
                hook_results_json: None,
                rejection: false,
                created_at: tied_created_at.to_owned(),
            },
        )
        .await
        .expect("transition log commits");
    }

    let latest = crate::task_service::action_resolver::latest_state_entry_authority(
        &db,
        &task.id,
        "in_progress",
    )
    .await
    .expect("state-entry authority loads")
    .expect("state-entry authority exists");
    assert_eq!(latest.id, newer_id);

    let db_result = TaskRepo::set_error_annotation_if_no_running_execution(
        &*db,
        &task.id,
        task.version,
        "in_progress",
        Some(older_id),
        &workflow_definition,
        None,
        None,
        "stale-stop",
        &now_rfc3339(),
        "stopped-execution",
        None,
        Vec::new(),
    )
    .await;
    assert!(
        matches!(db_result, Err(db::DbError::VersionConflict)),
        "the transactional CAS must reject the older tied entry: {db_result:?}"
    );
}

#[tokio::test]
async fn failed_resume_does_not_restore_metadata_over_running_replacement() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let annotation = serde_json::json!({
        "type": "manual_stop",
        "blocked_execution_id": "stopped-execution"
    })
    .to_string();
    let annotated = TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(annotation.clone())),
            blocked_json: Some(Some(
                serde_json::json!({"reason": "manual stop"}).to_string(),
            )),
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("manual-stop metadata sets");
    let cleared = TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: annotated.id.clone(),
            expected_version: annotated.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(None),
            blocked_json: Some(None),
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("recovery clear commits");
    let _replacement = seed_running_coder_execution(&db, &task.id, None, None).await;

    let result = TaskRepo::update_recovery_metadata_if_no_running_execution(
        &*db,
        &cleared.id,
        cleared.version,
        annotated.error_annotation.clone(),
        annotated.blocked_json.clone(),
        annotated.failed_json.clone(),
        &now_rfc3339(),
        Some("workspace-after-reset"),
        Vec::new(),
        Vec::new(),
        None,
    )
    .await;
    assert!(
        matches!(
            result,
            Err(db::DbError::ExecutionAlreadyRunning {
                ref scope,
            ref execution_id,
            }) if scope == "repository" && execution_id == &_replacement.id
        ),
        "unexpected restore result: {result:?}"
    );
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("current task loads")
        .expect("task exists");
    assert!(current.error_annotation.is_none());
    assert!(current.blocked_json.is_none());
}

#[tokio::test]
async fn merge_fix_completion_invalidates_cached_review_before_reentering_review() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        crate::workflow::default_states::MERGE_FAILED.to_owned(),
    )
    .await;
    TaskRoleAssignmentRepo::assign(
        &*db,
        role_assignment_input(
            &task.id,
            crate::workflow::default_roles::REVIEWER,
            None,
            Some("review-owner".to_owned()),
        ),
    )
    .await
    .expect("human reviewer assignment creates");
    TaskRepo::set_review_passed_at(&*db, &task.id, Some(now_rfc3339()), &now_rfc3339())
        .await
        .expect("cached review authority seeds");
    let now = now_rfc3339();
    let execution_snapshot = workflow_execution_snapshot(&db, &project_id).await;
    let execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: None,
            role: crate::workflow::default_roles::CODER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(execution_snapshot),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("merge-fix execution creates");

    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("merge-fix completion enters a fresh review");

    service.drain(&task.id).await.unwrap();
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, crate::workflow::default_states::REVIEW);
    assert!(
        current.review_passed_at.is_none(),
        "the prior commit's review authority must be gone before review hooks run"
    );
    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::AwaitingHuman);
}
