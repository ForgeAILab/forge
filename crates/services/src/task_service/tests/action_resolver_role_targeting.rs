use super::helpers::*;
use super::*;

#[tokio::test]
async fn test_resolve_execution_actions_targets_current_role() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        crate::workflow::default_states::IN_PROGRESS,
    )
    .await;
    let agent_id = seed_agent(&db).await;
    let coder_execution = seed_execution(
        &db,
        &task.id,
        Some(&agent_id),
        crate::workflow::default_roles::CODER,
        ExecutionStatus::Completed,
        Some("coder-session"),
        "2026-05-02T10:00:00Z",
    )
    .await;
    let reviewer_execution = seed_execution(
        &db,
        &task.id,
        Some(&agent_id),
        crate::workflow::default_roles::REVIEWER,
        ExecutionStatus::Completed,
        Some("reviewer-session"),
        "2026-05-02T10:05:00Z",
    )
    .await;
    let workflow = crate::workflow::default_workflow::default_workflow();
    let executions = vec![coder_execution.clone(), reviewer_execution.clone()];
    let annotation = api_types::TaskBlockingAnnotation {
        annotation_type: api_types::FailureKind::ExecutorFailed,
        blocking_reason: "executor failed".to_owned(),
        blocked_by: Some("system".to_owned()),
        blocked_at: Some(now_rfc3339()),
        blocked_execution_id: Some(coder_execution.id.clone()),
        artifact: None,
        message: None,
        hook: None,
        recovery_actions: vec![api_types::RecoveryAction::ResumeSession],
    };

    let actions = crate::task_service::action_resolver::resolve_execution_actions(
        &task,
        &workflow,
        &executions,
        Some(&annotation),
        None,
    );

    let workflow_resume = actions
        .iter()
        .find(|action| action.action == api_types::ExecutionActionKind::WorkflowResume)
        .expect("workflow resume action exists");
    assert_eq!(
        workflow_resume.target_execution_id.as_deref(),
        Some(coder_execution.id.as_str())
    );
    assert_ne!(
        workflow_resume.target_execution_id.as_deref(),
        Some(reviewer_execution.id.as_str())
    );

    let reexecute = actions
        .iter()
        .find(|action| action.action == api_types::ExecutionActionKind::ReExecute)
        .expect("re-execute action exists");
    assert_eq!(
        reexecute.target_execution_id.as_deref(),
        Some(coder_execution.id.as_str())
    );
    assert_ne!(
        reexecute.target_execution_id.as_deref(),
        Some(reviewer_execution.id.as_str())
    );

    let follow_up = actions
        .iter()
        .find(|action| action.action == api_types::ExecutionActionKind::SessionFollowUp)
        .expect("session follow-up action exists");
    assert!(!follow_up.propagates);
    assert_eq!(
        follow_up.target_execution_id.as_deref(),
        Some(coder_execution.id.as_str()),
        "session follow-up must target the current role, not a newer unrelated execution"
    );

    let coder_without_session = seed_execution(
        &db,
        &task.id,
        Some(&agent_id),
        crate::workflow::default_roles::CODER,
        ExecutionStatus::Completed,
        None,
        "2026-05-02T10:10:00Z",
    )
    .await;
    let actions = crate::task_service::action_resolver::resolve_execution_actions(
        &task,
        &workflow,
        &[coder_without_session],
        Some(&api_types::TaskBlockingAnnotation {
            blocked_execution_id: Some("missing".to_owned()),
            ..annotation.clone()
        }),
        None,
    );
    let workflow_resume = actions
        .iter()
        .find(|action| action.action == api_types::ExecutionActionKind::WorkflowResume)
        .expect("workflow resume action exists");
    assert!(!workflow_resume.enabled);
    let reason = workflow_resume
        .disabled_reason
        .as_deref()
        .expect("disabled reason is present")
        .to_ascii_lowercase();
    assert!(reason.contains("no") && reason.contains("session"));

    let mut failed_task = task.clone();
    failed_task.failed_json = Some(r#"{"reason":"executor failed"}"#.to_owned());
    let failed_actions = crate::task_service::action_resolver::resolve_execution_actions(
        &failed_task,
        &workflow,
        &[coder_execution],
        Some(&annotation),
        None,
    );
    for kind in [
        api_types::ExecutionActionKind::ManualLaunch,
        api_types::ExecutionActionKind::SessionFollowUp,
        api_types::ExecutionActionKind::WorkflowResume,
        api_types::ExecutionActionKind::ReExecute,
    ] {
        let action = failed_actions
            .iter()
            .find(|action| action.action == kind)
            .expect("failed-task execution action exists");
        assert!(
            !action.enabled,
            "{kind:?} must honor hard-failure precedence"
        );
        assert!(action
            .disabled_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("reset or cancel")));
    }
}

#[tokio::test]
async fn test_open_interactive_target_ignores_newer_unrelated_role() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        crate::workflow::default_states::IN_PROGRESS,
    )
    .await;
    let agent_id = seed_agent(&db).await;
    let coder_execution = seed_execution(
        &db,
        &task.id,
        Some(&agent_id),
        crate::workflow::default_roles::CODER,
        ExecutionStatus::Completed,
        Some("coder-session"),
        "2026-05-02T10:00:00Z",
    )
    .await;
    let reviewer_execution = seed_execution(
        &db,
        &task.id,
        Some(&agent_id),
        crate::workflow::default_roles::REVIEWER,
        ExecutionStatus::Completed,
        Some("reviewer-session"),
        "2026-05-02T10:05:00Z",
    )
    .await;
    let executions = vec![coder_execution.clone(), reviewer_execution];

    let target = crate::task_service::action_resolver::select_open_interactive_target(
        &executions,
        Some(crate::workflow::default_roles::CODER),
        None,
    )
    .expect("current-role resumable target exists");
    assert_eq!(target.id, coder_execution.id);

    let blocked_target = crate::task_service::action_resolver::select_open_interactive_target(
        &executions,
        Some(crate::workflow::default_roles::CODER),
        Some(&coder_execution.id),
    )
    .expect("blocked current-role resumable target exists");
    assert_eq!(blocked_target.id, coder_execution.id);

    let mut launch_only_coder = coder_execution;
    launch_only_coder.agent_session_id = None;
    let mut newer_agentless_reviewer = executions[1].clone();
    newer_agentless_reviewer.agent_id = None;
    let launch_only_executions = vec![launch_only_coder, newer_agentless_reviewer];
    assert!(
        crate::task_service::action_resolver::select_open_interactive_target(
            &launch_only_executions,
            Some(crate::workflow::default_roles::CODER),
            None,
        )
        .is_none(),
        "no resumable session should produce no follow-up target"
    );
    assert!(
        crate::task_service::action_resolver::has_open_interactive_launch_authority(
            &launch_only_executions,
            &[],
            Some(crate::workflow::default_roles::CODER),
            None,
        )
    );
    assert!(
        crate::task_service::action_resolver::has_open_interactive_launch_authority(
            &[],
            &[db::TaskRoleAssignment {
                id: "assignment".to_owned(),
                task_id: task.id,
                role_name: crate::workflow::default_roles::CODER.to_owned(),
                assignee_type: Some(db::AssigneeKind::Agent),
                assignee_id: Some(agent_id),
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            }],
            Some(crate::workflow::default_roles::CODER),
            None,
        )
    );
}

#[tokio::test]
async fn test_resolve_execution_actions_disables_resume_for_terminal_bound_review() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task =
        seed_task_with_status(&db, &project_id, crate::workflow::default_states::REVIEW).await;
    let agent_id = seed_agent(&db).await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::REVIEWER,
        Some(&agent_id),
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
    let review = seed_failed_review(
        &db,
        &task.id,
        &execution.id,
        1,
        serde_json::json!({"ci_steps": []}),
    )
    .await;
    sqlx::query("UPDATE review SET reviewer_execution_id = ? WHERE id = ?")
        .bind(&execution.id)
        .bind(&review.id)
        .execute(db.pool())
        .await
        .expect("review binding updates");
    let review = ReviewRepo::get_by_id(&*db, &review.id)
        .await
        .expect("review reloads")
        .expect("review exists");
    let annotation = api_types::TaskBlockingAnnotation {
        annotation_type: api_types::FailureKind::ExecutorFailed,
        blocking_reason: "reviewer execution stopped".to_owned(),
        blocked_by: Some("system".to_owned()),
        blocked_at: Some(now_rfc3339()),
        blocked_execution_id: Some(execution.id.clone()),
        artifact: None,
        message: None,
        hook: None,
        recovery_actions: vec![api_types::RecoveryAction::ResumeSession],
    };

    let actions = crate::task_service::action_resolver::resolve_execution_actions(
        &task,
        &crate::workflow::default_workflow::default_workflow(),
        std::slice::from_ref(&execution),
        Some(&annotation),
        Some(&review),
    );
    let resume = actions
        .iter()
        .find(|action| action.action == api_types::ExecutionActionKind::WorkflowResume)
        .expect("workflow resume action exists");
    assert!(!resume.enabled);
    assert_eq!(
        resume.disabled_reason.as_deref(),
        Some("The bound reviewer Review attempt is terminal; start a fresh review attempt")
    );
}
