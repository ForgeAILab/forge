use super::helpers::*;
use super::*;
use api_types::{FailureKind, TaskAction};

/// The diagnostic panel consumes exactly the pure offer set, including states
/// that previously had no annotation or carried obsolete stored allowlists.
async fn check_projection(state: &str, kind: Option<FailureKind>, failed_review: bool) -> crate::TaskSnapshot {
    let db = Arc::new(sqlite_db().await);
    let (project_id, _, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, state).await;
    seed_role_assignment(&db, &task.id, "coder", Some(&agent_id)).await;
    seed_role_assignment(&db, &task.id, "reviewer", Some(&agent_id)).await;
    let execution = seed_execution(&db, &task.id, Some(&agent_id), if state == "review" { "reviewer" } else { "coder" }, ExecutionStatus::Completed, Some("session"), "2026-10-02T00:00:00Z").await;
    if failed_review { seed_failed_review(&db, &task.id, &execution.id, 1, json!({"ci_steps":[{"command":"check","exit_code":1,"output_tail":"failure"}]})).await; }
    if let Some(kind) = kind {
        sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?").bind(json!({"type":kind,"blocking_reason":"fixture","blocked_execution_id":execution.id,"recovery_actions":["return_to_implementation","retry_pr_publication"]}).to_string()).bind(&task.id).execute(db.pool()).await.unwrap();
    }
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(16)));
    let snapshot = service.task_action_snapshot(&task.id, &Actor::user(UserActionSource::Test)).await.unwrap();
    let offers = crate::available_actions(&snapshot);
    let query = service.task_action_offers(&task.id, &Actor::user(UserActionSource::Test)).await.unwrap();
    assert_eq!(query.available_actions, offers);
    if let Some(exception) = crate::task_diagnostics::task_exception(&snapshot, offers.clone()) { assert_eq!(exception.actions, offers); }
    snapshot
}

#[tokio::test]
async fn test_derive_workflow_exception_review_failed_no_annotation() {
    let snapshot = check_projection("review", None, true).await;
    let offers = crate::available_actions(&snapshot);
    assert!(offers.iter().any(|offer| offer.reason == "review_failed"));
    let exception = crate::task_diagnostics::task_exception(&snapshot, offers).unwrap();
    assert_eq!(exception.failing_step.unwrap().command.as_deref(), Some("check"));
}

#[tokio::test]
async fn test_derive_workflow_exception_infers_actions_for_empty_exhausted_annotation() {
    let snapshot = check_projection("review", Some(FailureKind::ReviewBudgetExhausted), true).await;
    assert!(crate::available_actions(&snapshot).iter().any(|offer| matches!(offer.action, TaskAction::Retry { reset_budget: Some(true), .. })));
}

#[tokio::test]
async fn test_retry_exhausted_blocked_metadata_takes_precedence_over_stale_error_annotation() {
    let mut snapshot = check_projection("review", Some(FailureKind::Unknown), true).await;
    snapshot.task.blocked_json = Some(json!({"kind":"retry_exhausted","reason":"exhausted"}).to_string());
    assert!(crate::available_actions(&snapshot).iter().any(|offer| offer.reason == "retry_budget_exhausted"));
}

#[tokio::test]
async fn stored_action_lists_do_not_control_projection() {
    let snapshot = check_projection("review", Some(FailureKind::ReviewBudgetExhausted), true).await;
    let mut changed = snapshot.clone();
    let mut annotation: Value = serde_json::from_str(changed.task.error_annotation.as_deref().unwrap()).unwrap();
    annotation["recovery_actions"] = json!(["cancel_task"]);
    changed.task.error_annotation = Some(annotation.to_string());
    assert_eq!(crate::available_actions(&snapshot), crate::available_actions(&changed));
}

#[tokio::test]
async fn test_merge_gate_stale_error_annotation_offers_retry_merge_when_window_available() {
    let snapshot = check_projection("merging", Some(FailureKind::TargetRepoDirty), false).await;
    assert!(crate::available_actions(&snapshot).iter().any(|offer| offer.reason == "merge_gate_retry"));
}

#[tokio::test]
async fn test_reviewer_execution_failure_only_offers_retry_or_pass() {
    let mut snapshot = check_projection("review", None, true).await;
    snapshot.latest_review.as_mut().unwrap().step_results_json = json!({"execution":{"role":"reviewer","status":"failed"}}).to_string();
    let offers = crate::available_actions(&snapshot);
    assert!(offers.iter().any(|offer| offer.reason == "failed_review_override"));
    assert!(offers.iter().any(|offer| offer.action.verb() == "retry"));
}

#[tokio::test]
async fn review_blocked_annotation_routes_guidance_and_offers_manual_pass() {
    let snapshot = check_projection("review", Some(FailureKind::ReviewBlocked), true).await;
    let offers = crate::available_actions(&snapshot);
    assert!(offers.iter().any(|offer| offer.action.verb() == "approve"));
    assert!(offers.iter().any(|offer| offer.parameters.iter().any(|parameter| parameter.name == "guidance")));
}

#[tokio::test]
async fn test_failed_task_supersedes_blocking_annotation() {
    let mut snapshot = check_projection("in_progress", Some(FailureKind::BeforeWorkHookFailed), false).await;
    snapshot.task.failed_json = Some(json!({"kind":"executor_failed","reason":"hard failure"}).to_string());
    let verbs = crate::available_actions(&snapshot).into_iter().map(|offer| offer.action.verb()).collect::<Vec<_>>();
    assert_eq!(verbs, ["cancel", "restart"]);
}

#[tokio::test]
async fn test_annotation_hook_details_surface_as_failing_step() {
    let mut snapshot = check_projection("in_progress", Some(FailureKind::BeforeWorkHookFailed), false).await;
    let mut annotation = snapshot.annotation().unwrap();
    annotation.hook = Some(json!({"command":"check","exit_code":7,"stderr":"failure"}));
    snapshot.task.error_annotation = Some(serde_json::to_string(&annotation).unwrap());
    let exception = crate::task_diagnostics::task_exception(&snapshot, crate::available_actions(&snapshot)).unwrap();
    assert_eq!(exception.failing_step.unwrap().exit_code, Some(7));
}

#[tokio::test]
async fn test_reworded_reason_does_not_change_offered_actions() {
    let snapshot = check_projection("in_progress", Some(FailureKind::ExecutorFailed), false).await;
    let mut changed = snapshot.clone();
    let mut annotation = snapshot.annotation().unwrap(); annotation.blocking_reason = "different prose".to_owned();
    changed.task.error_annotation = Some(serde_json::to_string(&annotation).unwrap());
    assert_eq!(crate::available_actions(&snapshot), crate::available_actions(&changed));
}

#[tokio::test]
async fn test_resume_session_requires_the_execution_agent_to_own_its_role() {
    let mut snapshot = check_projection("in_progress", Some(FailureKind::ExecutorFailed), false).await;
    for execution in &mut snapshot.executions { execution.agent_id = Some("other-agent".to_owned()); }
    assert!(crate::available_actions(&snapshot).iter().any(|offer| matches!(offer.action, TaskAction::Retry { fresh_session: Some(true), .. })));
}

#[tokio::test]
async fn test_resume_session_rejects_stale_terminal_review_without_mutating_task() {
    let snapshot = check_projection("review", None, true).await;
    let before = snapshot.task.clone();
    let offers = crate::available_actions(&snapshot);
    assert!(!offers.iter().any(|offer| matches!(offer.action, TaskAction::Retry { fresh_session: Some(false), .. })));
    assert_eq!(before, snapshot.task);
}

#[tokio::test]
async fn test_unknown_kind_is_info_only_and_rejects_recovery() {
    let snapshot = check_projection("in_progress", Some(FailureKind::Unknown), false).await;
    assert_eq!(crate::available_actions(&snapshot).into_iter().map(|offer| offer.action.verb()).collect::<Vec<_>>(), ["cancel"]);
}

#[tokio::test]
async fn test_recovery_actions_disabled_while_role_execution_runs() {
    let mut snapshot = check_projection("review", None, true).await;
    snapshot.executions[0].status = ExecutionStatus::Running;
    let verbs = crate::available_actions(&snapshot).into_iter().map(|offer| offer.action.verb()).collect::<Vec<_>>();
    assert_eq!(verbs, ["cancel", "hold"]);
}
