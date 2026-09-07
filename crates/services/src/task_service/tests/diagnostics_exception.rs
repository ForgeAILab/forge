use super::helpers::*;
use super::*;

#[tokio::test]
async fn test_derive_workflow_exception_review_failed_no_annotation() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        &repo_id,
        crate::workflow::default_states::REVIEW,
    )
    .await;
    assert_eq!(task.error_annotation, None);
    assert_eq!(task.blocked_json, None);
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
    let review = seed_failed_review(
        &db,
        &task.id,
        &execution.id,
        1,
        json!({
            "ci_steps": [{
                "command": "cargo test --workspace",
                "exit_code": 101,
                "output_tail": "test failure tail",
                "stderr_tail": "stderr failure tail"
            }]
        }),
    )
    .await;
    let mut remaining_retries = std::collections::HashMap::new();
    remaining_retries.insert(crate::workflow::default_states::REVIEW.to_owned(), 2);

    let exception = crate::task_diagnostics::derive_workflow_exception(
        &task,
        &crate::workflow::default_workflow::default_workflow(),
        &[],
        Some(&review),
        Some(&execution),
        &remaining_retries,
    )
    .expect("workflow exception derives");

    assert_eq!(exception.exception_type, "review_failed");
    let failing_step = exception.failing_step.expect("failing step exists");
    assert_eq!(
        failing_step.command.as_deref(),
        Some("cargo test --workspace")
    );
    assert_eq!(failing_step.exit_code, Some(101));
    assert_eq!(
        failing_step.output_tail.as_deref(),
        Some("test failure tail")
    );
    assert_eq!(
        failing_step.stderr_tail.as_deref(),
        Some("stderr failure tail")
    );

    let action_kinds = exception
        .actions
        .iter()
        .map(|action| {
            serde_json::to_value(action.kind)
                .expect("kind serializes")
                .as_str()
                .expect("kind serializes as string")
                .to_owned()
        })
        .collect::<Vec<_>>();
    assert!(action_kinds.iter().any(|kind| kind == "retry_hook"));
    assert!(action_kinds.iter().any(|kind| kind == "resume_process"));
    assert!(action_kinds.iter().any(|kind| kind == "proceed_once"));
    assert!(action_kinds.iter().any(|kind| kind == "open_interactive"));

    let retry_hook = exception
        .actions
        .iter()
        .find(|action| {
            serde_json::to_value(action.kind)
                .expect("kind serializes")
                .as_str()
                == Some("retry_hook")
        })
        .expect("retry_hook action exists");
    assert!(
        retry_hook.enabled,
        "retry_hook should be enabled for review gate with failed review and retries remaining"
    );

    let resume_process = exception
        .actions
        .iter()
        .find(|action| {
            serde_json::to_value(action.kind)
                .expect("kind serializes")
                .as_str()
                == Some("resume_process")
        })
        .expect("resume_process action exists");
    assert!(resume_process.enabled);
    assert_eq!(
        resume_process.target_state.as_deref(),
        Some(crate::workflow::default_states::IN_PROGRESS)
    );
    assert_eq!(
        resume_process.target_role.as_deref(),
        Some(crate::workflow::default_roles::CODER)
    );

    let proceed_once = exception
        .actions
        .iter()
        .find(|action| {
            serde_json::to_value(action.kind)
                .expect("kind serializes")
                .as_str()
                == Some("proceed_once")
        })
        .expect("proceed_once action exists");
    assert!(
        !proceed_once.enabled,
        "proceed_once should be disabled when retry budget is not exhausted"
    );
}

#[tokio::test]
async fn test_derive_workflow_exception_infers_actions_for_empty_exhausted_annotation() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        &repo_id,
        crate::workflow::default_states::REVIEW,
    )
    .await;
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
    let review =
        seed_failed_review(&db, &task.id, &execution.id, 2, json!({ "ci_steps": [] })).await;
    let annotation = api_types::TaskAnnotation::Blocking(api_types::TaskBlockingAnnotation {
        annotation_type: api_types::FailureKind::ReviewBudgetExhausted,
        blocking_reason: "review retry budget exhausted".to_owned(),
        blocked_by: Some("system".to_owned()),
        blocked_at: Some(now_rfc3339()),
        blocked_execution_id: None,
        artifact: None,
        message: Some("review retry budget exhausted".to_owned()),
        hook: None,
        recovery_actions: Vec::new(),
    });
    let task = db::TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(
                serde_json::to_string(&annotation).expect("annotation serializes"),
            )),
            blocked_json: Some(Some(
                json!({
                    "reason": "review retry budget exhausted",
                    "created_at": now_rfc3339(),
                    "kind": "review_gate_failed"
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
    .expect("task updates");

    let exception = crate::task_diagnostics::derive_workflow_exception(
        &task,
        &crate::workflow::default_workflow::default_workflow(),
        &[],
        Some(&review),
        Some(&execution),
        &std::collections::HashMap::new(),
    )
    .expect("workflow exception derives");

    let action_kinds = exception
        .actions
        .iter()
        .map(|action| {
            serde_json::to_value(action.kind)
                .expect("kind serializes")
                .as_str()
                .expect("kind serializes as string")
                .to_owned()
        })
        .collect::<Vec<_>>();
    assert!(action_kinds.iter().any(|kind| kind == "retry_hook"));
    assert!(action_kinds.iter().any(|kind| kind == "resume_process"));
    assert!(action_kinds.iter().any(|kind| kind == "reset_retry_window"));
    assert!(action_kinds.iter().any(|kind| kind == "proceed_once"));
    assert!(action_kinds.iter().any(|kind| kind == "open_interactive"));
}

#[tokio::test]
async fn test_retry_exhausted_blocked_metadata_takes_precedence_over_stale_error_annotation() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        &repo_id,
        crate::workflow::default_states::MERGING,
    )
    .await;
    let execution = seed_execution(
        &db,
        &task.id,
        None,
        crate::workflow::default_roles::CODER,
        ExecutionStatus::Completed,
        Some("coder-session"),
        "2026-05-02T10:00:00Z",
    )
    .await;
    let stale_annotation = api_types::TaskAnnotation::Blocking(api_types::TaskBlockingAnnotation {
        annotation_type: api_types::FailureKind::TargetRepoDirty,
        blocking_reason: String::new(),
        blocked_by: None,
        blocked_at: None,
        blocked_execution_id: None,
        artifact: None,
        message: Some("target repository has uncommitted changes".to_owned()),
        hook: None,
        recovery_actions: Vec::new(),
    });
    let task = db::TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(
                serde_json::to_string(&stale_annotation).expect("annotation serializes"),
            )),
            blocked_json: Some(Some(
                json!({
                    "reason": "gate rejection budget exhausted: 1/1",
                    "created_at": now_rfc3339(),
                    "kind": "retry_exhausted",
                    "execution_id": execution.id.clone()
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
    .expect("task updates");

    let exception = crate::task_diagnostics::derive_workflow_exception(
        &task,
        &crate::workflow::default_workflow::default_workflow(),
        &[],
        None,
        Some(&execution),
        &std::collections::HashMap::new(),
    )
    .expect("workflow exception derives");

    assert_eq!(exception.exception_type, "retry_exhausted");
    let action_kinds = exception
        .actions
        .iter()
        .map(|action| {
            serde_json::to_value(action.kind)
                .expect("kind serializes")
                .as_str()
                .expect("kind serializes as string")
                .to_owned()
        })
        .collect::<Vec<_>>();
    let retry_hook = exception
        .actions
        .iter()
        .find(|action| {
            serde_json::to_value(action.kind)
                .expect("kind serializes")
                .as_str()
                == Some("retry_hook")
        })
        .expect("retry_hook action exists");
    assert_eq!(retry_hook.label, "Retry Merge");
    assert!(
        retry_hook.enabled,
        "retry merge should reset the retry window and resume merge-fix work in one action"
    );
    assert_eq!(
        retry_hook.target_state.as_deref(),
        Some(crate::workflow::default_states::MERGE_FAILED)
    );
    assert_eq!(
        retry_hook.target_role.as_deref(),
        Some(crate::workflow::default_roles::CODER)
    );
    assert!(
        action_kinds.iter().any(|kind| kind == "resume_process"),
        "resume_process should be visible for exhausted merge gates"
    );
    let reset_retry_window = exception
        .actions
        .iter()
        .find(|action| {
            serde_json::to_value(action.kind)
                .expect("kind serializes")
                .as_str()
                == Some("reset_retry_window")
        })
        .expect("reset_retry_window action exists");
    assert!(
        reset_retry_window.propagates,
        "reset_retry_window should indicate that it resumes merge-fix work"
    );
    assert!(
        action_kinds.iter().any(|kind| kind == "reset_retry_window"),
        "reset_retry_window should be offered instead of falling back to cancel only"
    );
    assert!(
        action_kinds.iter().all(|kind| kind != "cancel_task"),
        "retry exhaustion actions should not collapse to cancel_task"
    );
}

#[tokio::test]
async fn test_merge_gate_stale_error_annotation_offers_retry_merge_when_window_available() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        &repo_id,
        crate::workflow::default_states::MERGING,
    )
    .await;
    let execution = seed_execution(
        &db,
        &task.id,
        None,
        crate::workflow::default_roles::CODER,
        ExecutionStatus::Completed,
        Some("coder-session"),
        "2026-05-02T10:00:00Z",
    )
    .await;
    let annotation = api_types::TaskAnnotation::Blocking(api_types::TaskBlockingAnnotation {
        annotation_type: api_types::FailureKind::TargetRepoDirty,
        blocking_reason: String::new(),
        blocked_by: None,
        blocked_at: None,
        blocked_execution_id: None,
        artifact: None,
        message: Some("target repository has uncommitted changes".to_owned()),
        hook: None,
        recovery_actions: Vec::new(),
    });
    let task = db::TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(
                serde_json::to_string(&annotation).expect("annotation serializes"),
            )),
            blocked_json: Some(None),
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("task updates");

    let exception = crate::task_diagnostics::derive_workflow_exception(
        &task,
        &crate::workflow::default_workflow::default_workflow(),
        &[],
        None,
        Some(&execution),
        &std::collections::HashMap::new(),
    )
    .expect("workflow exception derives");

    let retry_hook = exception
        .actions
        .iter()
        .find(|action| {
            serde_json::to_value(action.kind)
                .expect("kind serializes")
                .as_str()
                == Some("retry_hook")
        })
        .expect("retry_hook action exists");
    assert_eq!(retry_hook.label, "Retry Merge");
    assert!(retry_hook.enabled);
    assert_eq!(
        retry_hook.target_state.as_deref(),
        Some(crate::workflow::default_states::MERGING)
    );

    assert!(
        exception.actions.iter().all(|action| {
            serde_json::to_value(action.kind)
                .expect("kind serializes")
                .as_str()
                != Some("resume_process")
        }),
        "target-repo dirty recovery should retry the merge gate, not dispatch merge-fix work"
    );
}

#[tokio::test]
async fn test_reviewer_execution_failure_only_offers_retry_or_pass() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        &repo_id,
        crate::workflow::default_states::REVIEW,
    )
    .await;
    let execution = seed_execution(
        &db,
        &task.id,
        None,
        crate::workflow::default_roles::REVIEWER,
        ExecutionStatus::Failed,
        Some("review-session"),
        "2026-05-02T10:00:00Z",
    )
    .await;
    let review = seed_failed_review(
        &db,
        &task.id,
        &execution.id,
        1,
        json!({
            "ci_steps": [],
            "execution": {
                "id": execution.id,
                "status": "failed",
                "error": "reviewer exited before verdict"
            }
        }),
    )
    .await;
    let mut remaining_retries = std::collections::HashMap::new();
    remaining_retries.insert(crate::workflow::default_states::REVIEW.to_owned(), 2);

    let exception = crate::task_diagnostics::derive_workflow_exception(
        &task,
        &crate::workflow::default_workflow::default_workflow(),
        &[],
        Some(&review),
        Some(&execution),
        &remaining_retries,
    )
    .expect("workflow exception derives");

    let action_kinds = exception
        .actions
        .iter()
        .map(|action| {
            serde_json::to_value(action.kind)
                .expect("kind serializes")
                .as_str()
                .expect("kind serializes as string")
                .to_owned()
        })
        .collect::<Vec<_>>();
    assert_eq!(action_kinds, vec!["retry_hook", "mark_reviewed"]);
    assert_eq!(exception.actions[1].label, "Pass Review");
}

#[tokio::test]
async fn test_failed_task_supersedes_blocking_annotation() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        &repo_id,
        crate::workflow::default_states::IN_PROGRESS,
    )
    .await;
    let annotation = api_types::TaskAnnotation::Blocking(api_types::TaskBlockingAnnotation {
        annotation_type: api_types::FailureKind::RecoveryRequired,
        blocking_reason: "crash_recovery".to_owned(),
        blocked_by: Some("system".to_owned()),
        blocked_at: Some(now_rfc3339()),
        blocked_execution_id: None,
        artifact: None,
        message: Some("Recovered after server restart".to_owned()),
        hook: None,
        recovery_actions: vec![
            api_types::RecoveryAction::Reexecute,
            api_types::RecoveryAction::ResetToInitial,
            api_types::RecoveryAction::CancelTask,
        ],
    });
    let task = db::TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(
                serde_json::to_string(&annotation).expect("annotation serializes"),
            )),
            blocked_json: None,
            failed_json: Some(Some(
                json!({
                    "reason": "executor crashed",
                    "created_at": now_rfc3339(),
                    "kind": "crash"
                })
                .to_string(),
            )),
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("task updates");

    let exception = crate::task_diagnostics::derive_workflow_exception(
        &task,
        &crate::workflow::default_workflow::default_workflow(),
        &[],
        None,
        None,
        &std::collections::HashMap::new(),
    )
    .expect("workflow exception derives");

    // recover_task only accepts reset/cancel once failed_json is set, so the
    // annotation's retry actions must not surface.
    assert_eq!(exception.exception_type, "task_failed");
    let action_kinds = exception
        .actions
        .iter()
        .map(|action| {
            serde_json::to_value(action.kind)
                .expect("kind serializes")
                .as_str()
                .expect("kind serializes as string")
                .to_owned()
        })
        .collect::<Vec<_>>();
    assert_eq!(action_kinds, vec!["reset_to_initial", "cancel_task"]);
}

#[tokio::test]
async fn test_annotation_hook_details_surface_as_failing_step() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        &repo_id,
        crate::workflow::default_states::IN_PROGRESS,
    )
    .await;
    let annotation = api_types::TaskAnnotation::Blocking(api_types::TaskBlockingAnnotation {
        annotation_type: api_types::FailureKind::BeforeWorkHookFailed,
        blocking_reason: "before_work_hook_failed".to_owned(),
        blocked_by: Some("system".to_owned()),
        blocked_at: Some(now_rfc3339()),
        blocked_execution_id: None,
        artifact: None,
        message: Some("post-transition hook failed".to_owned()),
        hook: Some(json!({
            "command": "cargo test",
            "exit_code": 101,
            "stderr": "test failed: assertion",
            "stdout": ""
        })),
        recovery_actions: vec![api_types::RecoveryAction::RetryHook],
    });
    let task = db::TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(
                serde_json::to_string(&annotation).expect("annotation serializes"),
            )),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("task updates");

    let exception = crate::task_diagnostics::derive_workflow_exception(
        &task,
        &crate::workflow::default_workflow::default_workflow(),
        &[],
        None,
        None,
        &std::collections::HashMap::new(),
    )
    .expect("workflow exception derives");

    let step = exception
        .failing_step
        .expect("hook details map to failing step");
    assert_eq!(step.command.as_deref(), Some("cargo test"));
    assert_eq!(step.exit_code, Some(101));
    assert_eq!(step.stderr_tail.as_deref(), Some("test failed: assertion"));
    assert_eq!(step.output_tail, None);
}

#[tokio::test]
async fn test_reworded_reason_does_not_change_offered_actions() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let workflow = crate::workflow::default_workflow::default_workflow();

    let mut action_sets = Vec::new();
    for (suffix, reason) in [
        ("keyword", "review retry budget exhausted after 3 attempts"),
        ("reworded", "we ran out of automated attempts, human needed"),
    ] {
        let task = seed_task_with_status(
            &db,
            &project_id,
            &repo_id,
            crate::workflow::default_states::REVIEW,
        )
        .await;
        let task = db::TaskRepo::update(
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
                blocked_json: Some(Some(
                    json!({
                        "reason": reason,
                        "created_at": now_rfc3339(),
                        "kind": "retry_exhausted"
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
        .expect("task updates");

        let exception = crate::task_diagnostics::derive_workflow_exception(
            &task,
            &workflow,
            &[],
            None,
            None,
            &std::collections::HashMap::new(),
        )
        .unwrap_or_else(|| panic!("workflow exception derives for {suffix}"));
        assert_eq!(exception.message, reason);
        action_sets.push(
            exception
                .actions
                .iter()
                .map(|action| (action.kind, action.enabled))
                .collect::<Vec<_>>(),
        );
    }

    // Classification rides on the structured kind alone; the reason text is
    // display-only and must not change which actions are offered.
    assert_eq!(action_sets[0], action_sets[1]);
}

#[tokio::test]
async fn test_resume_session_requires_the_execution_agent_to_own_its_role() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        &repo_id,
        crate::workflow::default_states::REVIEW,
    )
    .await;
    let execution = seed_execution(
        &db,
        &task.id,
        Some(&agent_id),
        crate::workflow::default_roles::REVIEWER,
        ExecutionStatus::Failed,
        Some("review-session"),
        "2026-05-02T10:00:00Z",
    )
    .await;
    let annotation = api_types::TaskAnnotation::Blocking(api_types::TaskBlockingAnnotation {
        annotation_type: api_types::FailureKind::ExecutorFailed,
        blocking_reason: "reviewer execution stopped".to_owned(),
        blocked_by: Some("system".to_owned()),
        blocked_at: Some(now_rfc3339()),
        blocked_execution_id: Some(execution.id.clone()),
        artifact: None,
        message: None,
        hook: None,
        recovery_actions: vec![api_types::RecoveryAction::ResumeSession],
    });
    let task = db::TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(
                serde_json::to_string(&annotation).expect("annotation serializes"),
            )),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("task updates");

    let exception = crate::task_diagnostics::derive_workflow_exception(
        &task,
        &crate::workflow::default_workflow::default_workflow(),
        &[],
        None,
        Some(&execution),
        &std::collections::HashMap::new(),
    )
    .expect("workflow exception derives");
    let resume = exception
        .actions
        .iter()
        .find(|action| action.kind == api_types::RecoveryAction::ResumeSession)
        .expect("resume action remains visible for the legacy annotation");
    assert!(!resume.enabled);
    assert_eq!(
        resume.disabled_reason.as_deref(),
        Some("The stopped execution has no resumable assigned session")
    );

    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::REVIEWER,
        Some(&agent_id),
    )
    .await;
    let role_assignments = TaskRoleAssignmentRepo::list_by_task(&*db, &task.id)
        .await
        .expect("role assignments list");
    let exception = crate::task_diagnostics::derive_workflow_exception(
        &task,
        &crate::workflow::default_workflow::default_workflow(),
        &role_assignments,
        None,
        Some(&execution),
        &std::collections::HashMap::new(),
    )
    .expect("workflow exception derives");
    let resume = exception
        .actions
        .iter()
        .find(|action| action.kind == api_types::RecoveryAction::ResumeSession)
        .expect("resume action remains visible");
    assert!(resume.enabled);
    assert_eq!(resume.disabled_reason, None);
}

#[tokio::test]
async fn test_unknown_kind_is_info_only_and_rejects_recovery() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        &repo_id,
        crate::workflow::default_states::IN_PROGRESS,
    )
    .await;
    let task = db::TaskRepo::update(
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
            blocked_json: Some(Some(
                json!({
                    "reason": "blocked by something this build does not understand",
                    "created_at": now_rfc3339(),
                    "kind": "mystery_kind_from_the_future"
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
    .expect("task updates");

    // Info-only: no derived exception, so the UI falls back to the banner.
    let exception = crate::task_diagnostics::derive_workflow_exception(
        &task,
        &crate::workflow::default_workflow::default_workflow(),
        &[],
        None,
        None,
        &std::collections::HashMap::new(),
    );
    assert!(exception.is_none(), "unknown kind must not offer actions");

    // And recovery actions are rejected cleanly rather than misclassified.
    let error = service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::ResumeSession,
            None,
            None,
        )
        .await
        .expect_err("unknown-kind recovery must be rejected");
    assert!(
        matches!(error, ServiceError::InvalidOperation { .. }),
        "expected invalid_operation, got {error:?}"
    );
}
