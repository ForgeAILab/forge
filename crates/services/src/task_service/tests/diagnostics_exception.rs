fn assert_action_set(snapshot: &crate::TaskSnapshot, expected: &[&str]) {
    let mut actual = crate::available_actions(snapshot)
        .iter()
        .map(|offer| offer.action.verb())
        .collect::<Vec<_>>();
    let mut expected = expected.to_vec();
    actual.sort_unstable();
    expected.sort_unstable();
    assert_eq!(
        actual,
        expected,
        "exact offers for {} {:?}",
        snapshot.task.status,
        snapshot.condition()
    );
}

use super::helpers::*;
use super::*;
use api_types::{FailureKind, TaskAction};

/// The diagnostic panel consumes exactly the pure offer set, including states
/// that previously had no annotation or carried obsolete stored allowlists.
async fn check_projection(
    state: &str,
    kind: Option<FailureKind>,
    failed_review: bool,
) -> crate::TaskSnapshot {
    projection_fixture(state, kind, failed_review).await.2
}

async fn projection_fixture(
    state: &str,
    kind: Option<FailureKind>,
    failed_review: bool,
) -> (Arc<SqliteDb>, TaskService, crate::TaskSnapshot) {
    let db = Arc::new(sqlite_db().await);
    let (project_id, _, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, state).await;
    seed_role_assignment(&db, &task.id, "coder", Some(&agent_id)).await;
    seed_role_assignment(&db, &task.id, "reviewer", Some(&agent_id)).await;
    let execution = seed_execution(
        &db,
        &task.id,
        Some(&agent_id),
        if state == "review" && !failed_review {
            "reviewer"
        } else {
            "coder"
        },
        ExecutionStatus::Completed,
        Some("session"),
        "2026-10-02T00:00:00Z",
    )
    .await;
    if failed_review {
        seed_failed_review(&db, &task.id, &execution.id, 1, json!({"ci_steps":[{"command":"check","exit_code":1,"output_tail":"failure","stderr_tail":"check stderr"}]})).await;
    }
    if let Some(kind) = kind {
        sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?").bind(json!({"type":kind,"blocking_reason":"fixture","blocked_execution_id":execution.id,"recovery_actions":["return_to_implementation","retry_pr_publication"]}).to_string()).bind(&task.id).execute(db.pool()).await.unwrap();
        db.check_task_conditions_of(std::slice::from_ref(&task.id))
            .await
            .unwrap();
    }
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(16)));
    let snapshot = service
        .task_action_snapshot(&task.id, &Actor::user(UserActionSource::Test))
        .await
        .unwrap();
    let offers = crate::available_actions(&snapshot);
    let query = service
        .task_action_offers(&task.id, &Actor::user(UserActionSource::Test))
        .await
        .unwrap();
    assert_eq!(query.available_actions, offers);
    if let Some(exception) = crate::task_diagnostics::task_exception(&snapshot, offers.clone()) {
        assert_eq!(exception.actions, offers);
    }
    (db, service, snapshot)
}

/// Store one legacy field as an old writer left it, produce the Task's
/// condition from the stored row, and read the Task back as every reader does.
async fn stored(
    (db, service, snapshot): &(Arc<SqliteDb>, TaskService, crate::TaskSnapshot),
    column: &str,
    value: String,
) -> crate::TaskSnapshot {
    sqlx::query(&format!("UPDATE task SET {column} = ? WHERE id = ?"))
        .bind(value)
        .bind(&snapshot.task.id)
        .execute(db.pool())
        .await
        .unwrap();
    db.check_task_conditions_of(std::slice::from_ref(&snapshot.task.id))
        .await
        .unwrap();
    service
        .task_action_snapshot(&snapshot.task.id, &Actor::user(UserActionSource::Test))
        .await
        .unwrap()
}

#[tokio::test]
async fn test_derive_workflow_exception_review_failed_no_annotation() {
    let snapshot = check_projection("review", None, true).await;
    let offers = crate::available_actions(&snapshot);
    assert_action_set(&snapshot, &["cancel", "retry", "approve", "send_back"]);
    assert!(offers.iter().any(|offer| offer.reason == "review_failed"));
    let exception = crate::task_diagnostics::task_exception(&snapshot, offers).unwrap();
    assert_eq!(exception.exception_type, "review_failed");
    assert_eq!(
        exception.review_id.as_deref(),
        snapshot
            .latest_review
            .as_ref()
            .map(|review| review.id.as_str())
    );
    let step = exception.failing_step.unwrap();
    assert_eq!(step.command.as_deref(), Some("check"));
    assert_eq!(step.exit_code, Some(1));
    assert_eq!(step.output_tail.as_deref(), Some("failure"));
    assert_eq!(step.stderr_tail.as_deref(), Some("check stderr"));
}

#[tokio::test]
async fn test_derive_workflow_exception_infers_actions_for_empty_exhausted_annotation() {
    let snapshot = check_projection("review", Some(FailureKind::ReviewBudgetExhausted), true).await;
    assert_action_set(&snapshot, &["cancel", "retry", "approve"]);
    assert!(crate::available_actions(&snapshot)
        .iter()
        .any(|offer| matches!(
            offer.action,
            TaskAction::Retry {
                reset_budget: Some(true),
                ..
            }
        )));
}

#[tokio::test]
async fn test_retry_exhausted_blocked_metadata_takes_precedence_over_stale_error_annotation() {
    let fixture = projection_fixture("review", Some(FailureKind::Unknown), true).await;
    let snapshot = stored(
        &fixture,
        "blocked_json",
        json!({"kind":"retry_exhausted","reason":"exhausted"}).to_string(),
    )
    .await;
    assert_action_set(&snapshot, &["cancel", "retry", "restart", "approve"]);
    let exception =
        crate::task_diagnostics::task_exception(&snapshot, crate::available_actions(&snapshot))
            .unwrap();
    assert_eq!(exception.exception_type, "retry_exhausted");
    assert!(crate::available_actions(&snapshot)
        .iter()
        .any(|offer| offer.reason == "execution_retry_exhausted"));
}

#[tokio::test]
async fn stored_action_lists_do_not_control_projection() {
    let fixture =
        projection_fixture("review", Some(FailureKind::ReviewBudgetExhausted), true).await;
    let snapshot = fixture.2.clone();
    assert_action_set(&snapshot, &["cancel", "retry", "approve"]);
    let mut annotation: Value =
        serde_json::from_str(snapshot.task.error_annotation.as_deref().unwrap()).unwrap();
    annotation["recovery_actions"] = json!(["cancel_task"]);
    let changed = stored(&fixture, "error_annotation", annotation.to_string()).await;
    assert_eq!(
        crate::available_actions(&snapshot),
        crate::available_actions(&changed)
    );
    if let Some(exception) =
        crate::task_diagnostics::task_exception(&changed, crate::available_actions(&changed))
    {
        assert!(!exception.message.is_empty());
    }
}

#[tokio::test]
async fn test_merge_gate_stale_error_annotation_offers_retry_merge_when_window_available() {
    let snapshot = check_projection("merging", Some(FailureKind::TargetRepoDirty), false).await;
    assert_action_set(&snapshot, &["cancel", "retry", "approve"]);
    assert!(crate::available_actions(&snapshot)
        .iter()
        .any(|offer| offer.reason == "merge_gate_retry"));
}

#[tokio::test]
async fn test_reviewer_execution_failure_only_offers_retry_or_pass() {
    let mut snapshot = check_projection("review", None, true).await;
    snapshot.latest_review.as_mut().unwrap().step_results_json =
        json!({"execution":{"role":"reviewer","status":"failed"}}).to_string();
    assert_action_set(&snapshot, &["cancel", "retry", "approve", "send_back"]);
    let offers = crate::available_actions(&snapshot);
    assert!(offers
        .iter()
        .any(|offer| offer.reason == "failed_review_override"));
    assert!(offers.iter().any(|offer| offer.action.verb() == "retry"));
    let manual_pass = offers
        .iter()
        .find(|offer| offer.action.verb() == "approve")
        .unwrap();
    assert!(matches!(
        manual_pass.action,
        TaskAction::Approve {
            override_checks: Some(true),
            ..
        }
    ));
    assert!(manual_pass
        .parameters
        .iter()
        .any(|parameter| parameter.name == "reason" && parameter.required));
}

#[tokio::test]
async fn review_blocked_annotation_routes_guidance_and_offers_manual_pass() {
    let snapshot = check_projection("review", Some(FailureKind::ReviewBlocked), true).await;
    let offers = crate::available_actions(&snapshot);
    assert_action_set(&snapshot, &["cancel", "approve", "retry", "send_back"]);
    assert!(offers.iter().any(|offer| offer.action.verb() == "approve"));
    let approval = offers
        .iter()
        .find(|offer| offer.action.verb() == "approve")
        .unwrap();
    assert!(approval
        .parameters
        .iter()
        .any(|parameter| parameter.name == "reason" && parameter.required));
    let send_back = offers
        .iter()
        .find(|offer| offer.action.verb() == "send_back")
        .unwrap();
    assert!(
        matches!(&send_back.action, api_types::TaskAction::SendBack { guidance } if guidance.is_empty())
    );
    assert!(send_back
        .parameters
        .iter()
        .any(|parameter| parameter.name == "guidance" && parameter.required));
    assert!(offers.iter().any(|offer| offer
        .parameters
        .iter()
        .any(|parameter| parameter.name == "guidance")));
}

#[tokio::test]
async fn test_failed_task_supersedes_blocking_annotation() {
    let fixture = projection_fixture(
        "in_progress",
        Some(FailureKind::BeforeWorkHookFailed),
        false,
    )
    .await;
    let snapshot = stored(
        &fixture,
        "failed_json",
        json!({"kind":"executor_failed","reason":"hard failure"}).to_string(),
    )
    .await;
    let verbs = crate::available_actions(&snapshot)
        .into_iter()
        .map(|offer| offer.action.verb())
        .collect::<Vec<_>>();
    assert_eq!(verbs, ["cancel", "restart"]);
    let exception =
        crate::task_diagnostics::task_exception(&snapshot, crate::available_actions(&snapshot))
            .unwrap();
    assert_eq!(exception.exception_type, "task_failed");
}

#[tokio::test]
async fn test_annotation_hook_details_surface_as_failing_step() {
    let fixture = projection_fixture(
        "in_progress",
        Some(FailureKind::BeforeWorkHookFailed),
        false,
    )
    .await;
    let mut annotation = fixture.2.annotation().unwrap();
    annotation.hook = Some(json!({"command":"check","exit_code":7,"stderr":"failure"}));
    let snapshot = stored(
        &fixture,
        "error_annotation",
        serde_json::to_string(&annotation).unwrap(),
    )
    .await;
    let exception =
        crate::task_diagnostics::task_exception(&snapshot, crate::available_actions(&snapshot))
            .unwrap();
    let step = exception.failing_step.unwrap();
    assert_eq!(step.command.as_deref(), Some("check"));
    assert_eq!(step.exit_code, Some(7));
    assert_eq!(step.stderr_tail.as_deref(), Some("failure"));
    assert_eq!(step.output_tail, None);
}

#[tokio::test]
async fn test_reworded_reason_does_not_change_offered_actions() {
    let fixture = projection_fixture("in_progress", Some(FailureKind::ExecutorFailed), false).await;
    let snapshot = fixture.2.clone();
    assert_action_set(&snapshot, &["cancel", "retry", "restart", "approve"]);
    let mut annotation = snapshot.annotation().unwrap();
    annotation.blocking_reason = "different prose".to_owned();
    let changed = stored(
        &fixture,
        "error_annotation",
        serde_json::to_string(&annotation).unwrap(),
    )
    .await;
    assert_eq!(
        crate::available_actions(&snapshot),
        crate::available_actions(&changed)
    );
    if let Some(exception) =
        crate::task_diagnostics::task_exception(&changed, crate::available_actions(&changed))
    {
        assert_eq!(exception.message, "different prose");
    }
}

#[tokio::test]
async fn test_retry_existing_thread_requires_the_execution_agent_to_own_its_role() {
    let mut snapshot =
        check_projection("in_progress", Some(FailureKind::ExecutorFailed), false).await;
    for execution in &mut snapshot.executions {
        execution.agent_id = Some("other-agent".to_owned());
    }
    assert_action_set(&snapshot, &["cancel", "retry", "restart", "approve"]);
    assert!(!crate::available_actions(&snapshot)
        .iter()
        .any(|offer| matches!(
            offer.action,
            TaskAction::Retry {
                fresh_session: Some(false),
                ..
            }
        )));
    assert!(crate::available_actions(&snapshot)
        .iter()
        .any(|offer| matches!(
            offer.action,
            TaskAction::Retry {
                fresh_session: Some(true),
                ..
            }
        )));
    for execution in &mut snapshot.executions {
        execution.agent_id = snapshot.action_agent_id.clone();
    }
    assert!(crate::available_actions(&snapshot)
        .iter()
        .any(|offer| matches!(
            offer.action,
            TaskAction::Retry {
                fresh_session: Some(false),
                ..
            }
        )));
}

#[tokio::test]
async fn test_retry_existing_thread_rejects_stale_terminal_review_without_mutating_task() {
    let (db, service, snapshot) = projection_fixture("review", None, true).await;
    let reviewer = seed_execution(
        &db,
        &snapshot.task.id,
        snapshot.action_agent_id.as_deref(),
        "reviewer",
        ExecutionStatus::Completed,
        Some("settled-reviewer-session"),
        "2026-10-02T00:00:01Z",
    )
    .await;
    sqlx::query("UPDATE review SET reviewer_execution_id = ? WHERE id = ?")
        .bind(&reviewer.id)
        .bind(&snapshot.latest_review.as_ref().unwrap().id)
        .execute(db.pool())
        .await
        .unwrap();
    let snapshot = service
        .task_action_snapshot(&snapshot.task.id, &Actor::user(UserActionSource::Test))
        .await
        .unwrap();
    let before = snapshot.task.clone();
    let offers = crate::available_actions(&snapshot);
    assert_action_set(&snapshot, &["cancel", "retry", "approve", "send_back"]);
    assert!(!offers.iter().any(|offer| matches!(
        offer.action,
        TaskAction::Retry {
            fresh_session: Some(false),
            ..
        }
    )));
    let error = service
        .perform_task_action(
            &before.id,
            TaskAction::Retry {
                reason: None,
                fresh_session: Some(false),
                refresh_workspace: None,
                reset_budget: None,
                guidance: None,
            },
            before.version,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, ServiceError::TaskActionUnavailable { .. }));
    assert_eq!(
        TaskRepo::get_by_id(&*db, &before.id, false)
            .await
            .unwrap()
            .unwrap(),
        before
    );
    assert_eq!(
        ExecutionRepo::get_by_id(&*db, &reviewer.id)
            .await
            .unwrap()
            .unwrap(),
        reviewer
    );
}

#[tokio::test]
async fn test_unknown_kind_retains_legacy_reexecute_and_reset_authority() {
    let snapshot = check_projection("in_progress", Some(FailureKind::Unknown), false).await;
    assert_eq!(
        crate::available_actions(&snapshot)
            .into_iter()
            .map(|offer| offer.action.verb())
            .collect::<Vec<_>>(),
        ["cancel", "retry", "restart", "approve"]
    );
}

#[tokio::test]
async fn test_recovery_actions_disabled_while_role_execution_runs() {
    let mut snapshot = check_projection("review", None, true).await;
    snapshot.executions[0].status = ExecutionStatus::Running;
    let verbs = crate::available_actions(&snapshot)
        .into_iter()
        .map(|offer| offer.action.verb())
        .collect::<Vec<_>>();
    assert_action_set(&snapshot, &["cancel", "hold", "approve"]);
    assert_eq!(verbs, ["cancel", "hold", "approve"]);
}

#[tokio::test]
async fn decisions_require_operator_reason_and_guidance_and_reject_other_states() {
    let db = Arc::new(sqlite_db().await);
    let (project, _, _) = seed_project_repo(&db).await;
    let agent = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project, "review").await;
    seed_role_assignment(&db, &task.id, "reviewer", Some(&agent)).await;
    let candidate = seed_execution(
        &db,
        &task.id,
        Some(&agent),
        "coder",
        ExecutionStatus::Completed,
        None,
        "2026-10-02T00:00:00Z",
    )
    .await;
    seed_failed_review(&db, &task.id, &candidate.id, 1, json!({"ci_steps":[]})).await;
    // The Task as stored once its failed Review exists.
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(32)));
    for reason in [None, Some("   ".to_owned())] {
        let error = service
            .perform_task_action(
                &task.id,
                TaskAction::Approve {
                    override_checks: Some(true),
                    reason,
                },
                task.version,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("reason"));
    }
    for guidance in ["", " \t\n"] {
        let error = service
            .perform_task_action(
                &task.id,
                TaskAction::SendBack {
                    guidance: guidance.to_owned(),
                },
                task.version,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("guidance"));
    }
    let unchanged = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        unchanged, task,
        "invalid operator input must not mutate the Task"
    );
    let other = seed_task_with_status(&db, &project, "in_progress").await;
    let annotated = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(32)));
    sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
        .bind(json!({"type":"review_needs_owner","blocking_reason":"needs input"}).to_string())
        .bind(&other.id)
        .execute(db.pool())
        .await
        .unwrap();
    db.check_task_conditions_of(std::slice::from_ref(&other.id))
        .await
        .unwrap();
    let offers = annotated
        .task_action_offers(&other.id, &Actor::user(UserActionSource::Test))
        .await
        .unwrap();
    assert!(!offers
        .available_actions
        .iter()
        .any(|offer| offer.reason == "review_needs_owner"
            || matches!(
                offer.action,
                TaskAction::Approve {
                    override_checks: Some(false),
                    ..
                }
            )));
    assert!(matches!(
        annotated
            .perform_task_action(
                &other.id,
                TaskAction::Approve {
                    override_checks: Some(false),
                    reason: Some("owner decision".to_owned())
                },
                other.version
            )
            .await,
        Err(ServiceError::TaskActionUnavailable { .. })
    ));
}
