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
            ..annotation
        }),
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
}
