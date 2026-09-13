use super::helpers::*;
use super::*;

#[tokio::test]
async fn stale_atomic_recovery_marker_is_not_inserted_on_status_cas_loss() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task =
        seed_task_with_status(&db, &project_id, crate::workflow::default_states::REVIEW).await;
    let stale_version = task.version;
    TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: stale_version,
            title: Some("newer title".to_owned()),
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
    .expect("newer task write commits");

    let marker_id = new_uuid_v4();
    let result = TaskRepo::update_status_with_recovery_marker(
        &*db,
        db::UpdateTaskStatus {
            id: task.id.clone(),
            expected_version: stale_version,
            status: crate::workflow::default_states::IN_PROGRESS.to_owned(),
            assignee_id: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            updated_at: now_rfc3339(),
        },
        db::CreateTransitionLog {
            id: marker_id.clone(),
            task_id: task.id.clone(),
            from_state: crate::workflow::default_states::REVIEW.to_owned(),
            to_state: crate::workflow::default_states::REVIEW.to_owned(),
            trigger_name: Some("resume_process".to_owned()),
            triggered_by: "user:recovery:resume_process".to_owned(),
            trigger_reason: "stale request".to_owned(),
            hook_results_json: None,
            rejection: false,
            created_at: now_rfc3339(),
        },
    )
    .await;

    assert!(matches!(result, Err(db::DbError::VersionConflict)));
    let logs = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .expect("transition logs load");
    assert!(!logs.iter().any(|log| log.id == marker_id));
}

#[tokio::test]
async fn test_reset_retry_window_preserves_history_and_refreshes_budget() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, repo_dir) = seed_project_repo(&db).await;
    initialize_primary_repository(&repo_dir);
    let task =
        seed_task_with_status(&db, &project_id, crate::workflow::default_states::REVIEW).await;

    let execution = seed_execution(
        &db,
        &task.id,
        None,
        crate::workflow::default_roles::REVIEWER,
        ExecutionStatus::Completed,
        Some("review-session"),
        "2026-05-02T10:00:00Z",
    )
    .await;
    let review_one = seed_failed_review(
        &db,
        &task.id,
        &execution.id,
        1,
        json!({ "ci_steps": [{"command": "cargo test", "exit_code": 1}] }),
    )
    .await;
    let review_two = seed_failed_review(
        &db,
        &task.id,
        &execution.id,
        2,
        json!({ "ci_steps": [{"command": "cargo clippy", "exit_code": 1}] }),
    )
    .await;
    let first_log_id = seed_review_rejection_log(&db, &task.id, "review failed once").await;
    let second_log_id = seed_review_rejection_log(&db, &task.id, "review failed twice").await;
    let task = set_retry_exhausted_metadata(&db, &task).await;

    let original_logs = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .expect("transition logs load");
    let original_reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load");
    assert_eq!(
        TransitionLogRepo::count_gate_rejections(
            &*db,
            &task.id,
            crate::workflow::default_states::REVIEW,
        )
        .await
        .expect("rejection count loads"),
        2
    );

    let recovered = service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::ResetRetryWindow,
            Some("reason".to_owned()),
            None,
        )
        .await
        .expect("reset retry window succeeds");

    let logs = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .expect("transition logs reload");
    assert!(logs.len() >= original_logs.len());
    assert!(logs.iter().any(|log| log.id == first_log_id));
    assert!(logs.iter().any(|log| log.id == second_log_id));

    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews reload");
    assert_eq!(reviews.len(), original_reviews.len());
    assert!(reviews.iter().any(|review| review.id == review_one.id));
    assert!(reviews.iter().any(|review| review.id == review_two.id));

    let marker = logs
        .iter()
        .find(|log| log.trigger_name.as_deref() == Some("reset_retry_window"))
        .expect("reset marker exists");
    assert_eq!(marker.from_state, crate::workflow::default_states::REVIEW);
    assert_eq!(marker.to_state, crate::workflow::default_states::REVIEW);
    assert!(!marker.rejection);

    assert_eq!(
        TransitionLogRepo::count_gate_rejections(
            &*db,
            &task.id,
            crate::workflow::default_states::REVIEW,
        )
        .await
        .expect("post-reset rejection count loads"),
        1
    );
    assert_eq!(
        recovered.status,
        crate::workflow::default_states::IN_PROGRESS
    );
    assert_eq!(recovered.error_annotation, None);
    assert_eq!(recovered.blocked_json, None);

    let resume_marker = logs
        .iter()
        .find(|log| log.trigger_name.as_deref() == Some("resume_process"))
        .expect("resume marker exists");
    assert_eq!(
        resume_marker.from_state,
        crate::workflow::default_states::REVIEW
    );
    assert_eq!(
        resume_marker.to_state,
        crate::workflow::default_states::REVIEW
    );
    assert!(!resume_marker.rejection);

    let resume_transition = logs
        .iter()
        .find(|log| {
            log.triggered_by == "user:recovery:resume_process"
                && log.from_state == crate::workflow::default_states::REVIEW
                && log.to_state == crate::workflow::default_states::IN_PROGRESS
        })
        .expect("resume transition exists");
    assert!(resume_transition.rejection);
}

#[tokio::test]
async fn test_reset_retry_window_resumes_an_exhausted_merging_gate() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, repo_dir) = seed_project_repo(&db).await;
    initialize_primary_repository(&repo_dir);
    let task =
        seed_task_with_status(&db, &project_id, crate::workflow::default_states::MERGING).await;

    TransitionLogRepo::insert(
        &*db,
        db::CreateTransitionLog {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            from_state: crate::workflow::default_states::MERGING.to_owned(),
            to_state: crate::workflow::default_states::MERGE_FAILED.to_owned(),
            trigger_name: Some("retry".to_owned()),
            triggered_by: api_types::Actor::system(api_types::SystemComponent::Test).display(),
            trigger_reason: "merge conflict".to_owned(),
            hook_results_json: None,
            rejection: true,
            created_at: now_rfc3339(),
        },
    )
    .await
    .expect("merge rejection log creates");
    let annotation = api_types::TaskAnnotation::Blocking(api_types::TaskBlockingAnnotation {
        annotation_type: api_types::FailureKind::MergeFixBudgetExhausted,
        blocking_reason: "merge-fix retry budget exhausted".to_owned(),
        blocked_by: Some("system".to_owned()),
        blocked_at: Some(now_rfc3339()),
        blocked_execution_id: None,
        artifact: None,
        message: Some("merge-fix retry budget exhausted".to_owned()),
        hook: None,
        recovery_actions: vec![api_types::RecoveryAction::ResetRetryWindow],
    });
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
            error_annotation: Some(Some(serde_json::to_string(&annotation).unwrap())),
            blocked_json: Some(Some(
                json!({
                    "reason": "merge-fix retry budget exhausted",
                    "created_at": now_rfc3339(),
                    "kind": "merge_fix_budget_exhausted"
                })
                .to_string(),
            )),
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("merge budget block sets");

    let actions = service
        .available_task_actions(&task.id)
        .await
        .expect("task actions resolve");
    assert!(
        !actions.contains(&api_types::TaskAction::Resume),
        "a blocked gate must advertise typed recovery instead of a no-op resume"
    );

    let recovered = service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::ResetRetryWindow,
            Some("retry the merge repair".to_owned()),
            None,
        )
        .await
        .expect("reset retry window resumes merging gate");

    assert_eq!(
        recovered.status,
        crate::workflow::default_states::MERGE_FAILED
    );
    assert_eq!(recovered.error_annotation, None);
    assert_eq!(recovered.blocked_json, None);
    let logs = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .expect("transition logs reload");
    assert!(logs
        .iter()
        .any(|log| log.trigger_name.as_deref() == Some("reset_retry_window")));
    assert!(logs.iter().any(|log| {
        log.triggered_by == "user:recovery:resume_process"
            && log.from_state == crate::workflow::default_states::MERGING
            && log.to_state == crate::workflow::default_states::MERGE_FAILED
            && log.rejection
    }));
}

#[tokio::test]
async fn reset_to_initial_starts_a_fresh_merge_retry_window() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        crate::workflow::default_states::MERGE_FAILED,
    )
    .await;
    TransitionLogRepo::insert(
        &*db,
        db::CreateTransitionLog {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            from_state: crate::workflow::default_states::MERGING.to_owned(),
            to_state: crate::workflow::default_states::MERGE_FAILED.to_owned(),
            trigger_name: Some("retry".to_owned()),
            triggered_by: "system:workflow".to_owned(),
            trigger_reason: "merge conflict".to_owned(),
            hook_results_json: None,
            rejection: true,
            created_at: "2000-01-01T00:00:00Z".to_owned(),
        },
    )
    .await
    .expect("merge rejection records");
    let annotation = api_types::TaskAnnotation::Blocking(api_types::TaskBlockingAnnotation {
        annotation_type: api_types::FailureKind::RecoveryRequired,
        blocking_reason: "crash_recovery".to_owned(),
        blocked_by: Some("system:crash_recovery".to_owned()),
        blocked_at: Some(now_rfc3339()),
        blocked_execution_id: None,
        artifact: None,
        message: Some("Recovered after server restart".to_owned()),
        hook: None,
        recovery_actions: vec![api_types::RecoveryAction::ResetToInitial],
    });
    TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(serde_json::to_string(&annotation).unwrap())),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("recovery annotation records");

    let recovered = service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::ResetToInitial,
            Some("restart from todo".to_owned()),
            None,
        )
        .await
        .expect("reset succeeds");

    assert_eq!(recovered.status, crate::workflow::default_states::TODO);
    assert_eq!(
        TransitionLogRepo::count_gate_rejections(
            &*db,
            &task.id,
            crate::workflow::default_states::MERGING,
        )
        .await
        .expect("merge retry count loads"),
        0
    );
    let logs = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .expect("transition logs load");
    assert!(logs.iter().any(|entry| {
        entry.from_state == crate::workflow::default_states::MERGING
            && entry.trigger_name.as_deref() == Some("reset_to_initial")
            && !entry.rejection
    }));
}

#[tokio::test]
async fn test_proceed_once_from_review_reject_target_preserves_exhausted_window() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, repo_dir) = seed_project_repo(&db).await;
    initialize_primary_repository(&repo_dir);
    let task = seed_task_with_status(
        &db,
        &project_id,
        crate::workflow::default_states::IN_PROGRESS,
    )
    .await;
    seed_review_rejection_log(&db, &task.id, "review failed once").await;
    seed_review_rejection_log(&db, &task.id, "review failed twice").await;
    let task = set_retry_exhausted_metadata(&db, &task).await;

    let recovered = service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::ProceedOnce,
            Some("allow one focused repair".to_owned()),
            Some("address the latest review finding".to_owned()),
        )
        .await
        .expect("proceed once succeeds from the review reject target");

    assert_eq!(
        recovered.status,
        crate::workflow::default_states::IN_PROGRESS
    );
    assert_eq!(recovered.error_annotation, None);
    assert_eq!(recovered.blocked_json, None);

    let logs = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .expect("transition logs reload");
    let marker = logs
        .iter()
        .find(|log| log.trigger_name.as_deref() == Some("proceed_once"))
        .expect("proceed-once marker exists");
    assert_eq!(marker.from_state, crate::workflow::default_states::REVIEW);
    assert_eq!(marker.to_state, crate::workflow::default_states::REVIEW);
    assert!(!marker.rejection);
    assert!(marker
        .trigger_reason
        .contains("Guidance: address the latest review finding"));
    assert_eq!(
        TransitionLogRepo::count_gate_rejections(
            &*db,
            &task.id,
            crate::workflow::default_states::REVIEW,
        )
        .await
        .expect("post-recovery rejection count loads"),
        2,
        "proceed once must not reset the exhausted retry window"
    );
}

#[tokio::test]
async fn test_resume_process_moves_failed_review_back_to_in_progress() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, repo_dir) = seed_project_repo(&db).await;
    initialize_primary_repository(&repo_dir);
    let task =
        seed_task_with_status(&db, &project_id, crate::workflow::default_states::REVIEW).await;
    let execution = seed_execution(
        &db,
        &task.id,
        None,
        crate::workflow::default_roles::REVIEWER,
        ExecutionStatus::Completed,
        Some("review-session"),
        "2026-05-02T10:00:00Z",
    )
    .await;
    seed_failed_review(
        &db,
        &task.id,
        &execution.id,
        1,
        json!({ "ci_steps": [{"command": "cargo test", "exit_code": 1}] }),
    )
    .await;

    let recovered = service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::ResumeProcess,
            Some("send failed review back to coder".to_owned()),
            None,
        )
        .await
        .expect("resume process succeeds");

    assert_eq!(
        recovered.status,
        crate::workflow::default_states::IN_PROGRESS
    );
    let logs = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .expect("transition logs reload");
    assert!(logs.iter().any(|log| {
        log.triggered_by == "user:recovery:resume_process"
            && log.from_state == crate::workflow::default_states::REVIEW
            && log.to_state == crate::workflow::default_states::IN_PROGRESS
            && log.rejection
    }));
}
