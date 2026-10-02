use super::helpers::*;
use super::*;

#[tokio::test]
async fn test_resolve_execution_actions_targets_current_role() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let mut task = seed_task_with_status(
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

    };

    task.error_annotation = Some(serde_json::to_string(&annotation).unwrap());
    let snapshot = crate::TaskSnapshot { task, workflow, executions: executions.clone(), latest_review: None, role_assignments: Vec::new(), transition_logs: Vec::new(), caller: crate::ActionCaller::owner(), has_agent: true, dependencies_satisfied: true, owner_supports_resume: true, coordination_root: false };
    let offers = crate::available_actions(&snapshot);
    assert!(offers.iter().any(|offer| offer.action.verb() == "retry"));
    assert!(!offers.iter().any(|offer| offer.action.verb() == "open_interactive"));
    let role = snapshot.workflow.states.iter().find(|state| state.name == snapshot.task.status).and_then(crate::workflow::effective_role);
    let target = crate::task_service::action_resolver::select_open_interactive_target(&snapshot.executions, role, annotation.blocked_execution_id.as_deref());
    if let Some(target) = target { assert!(role.is_some_and(|role| target.role == role || (role == "coder" && target.role == "executor"))); }
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
async fn test_list_execution_action_authority_loads_unique_rows_in_stable_order() {
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

    let executions = crate::task_service::action_resolver::list_execution_action_authority(
        &db,
        &task.id,
        Some(crate::workflow::default_roles::CODER),
        Some(&coder_execution.id),
    )
    .await
    .expect("execution authority loads");

    let ids = executions
        .iter()
        .map(|execution| execution.id.as_str())
        .collect::<Vec<_>>();
    assert!(ids.contains(&coder_execution.id.as_str()));
    assert!(ids.contains(&reviewer_execution.id.as_str()));
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
}

#[tokio::test]
async fn test_resolve_execution_actions_disables_resume_for_terminal_bound_review() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let mut task =
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

    };

    task.error_annotation = Some(serde_json::to_string(&annotation).unwrap());
    let snapshot = crate::TaskSnapshot { task, workflow: crate::workflow::default_workflow::default_workflow(), executions: vec![execution.clone()], latest_review: Some(review.clone()), role_assignments: Vec::new(), transition_logs: Vec::new(), caller: crate::ActionCaller::owner(), has_agent: true, dependencies_satisfied: true, owner_supports_resume: true, coordination_root: false };
    let offers = crate::available_actions(&snapshot);
    assert!(!offers.iter().any(|offer| matches!(offer.action, api_types::TaskAction::Retry { fresh_session: Some(false), .. })));
    assert!(!offers.iter().any(|offer| offer.action.verb() == "open_interactive"));
    let role = snapshot.workflow.states.iter().find(|state| state.name == snapshot.task.status).and_then(crate::workflow::effective_role);
    let target = crate::task_service::action_resolver::select_open_interactive_target(&snapshot.executions, role, annotation.blocked_execution_id.as_deref());
    if let Some(target) = target { assert!(role.is_some_and(|role| target.role == role || (role == "coder" && target.role == "executor"))); }
}
