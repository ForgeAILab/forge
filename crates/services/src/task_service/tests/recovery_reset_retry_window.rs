use super::helpers::*;
use super::*;

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
