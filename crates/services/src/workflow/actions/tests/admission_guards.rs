//! Workflow action admission guards tests.

use super::*;

#[tokio::test]
async fn dependency_gate_skips_user_managed_transitions() {
    let mut ctx = build_test_ctx(
        "task-dependency-user-skip",
        default_states::TODO,
        default_states::PLANNING,
        None,
    )
    .await;
    ctx.triggered_by = api_types::Actor::user(api_types::UserActionSource::Api);

    let result = DependencyGate.execute(&ctx).await;

    assert!(matches!(
        result,
        HookResult::Skipped { reason } if reason.contains("user-managed transition")
    ));
}

#[tokio::test]
async fn check_retry_budget_allows_review_entry_when_rejections_reach_max() {
    let ctx = build_test_ctx(
        "task-retry-budget-cascade",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    seed_transition_log(
        &ctx.db,
        &ctx.task_id,
        default_states::REVIEW,
        default_states::IN_PROGRESS,
        true,
    )
    .await;
    seed_transition_log(
        &ctx.db,
        &ctx.task_id,
        default_states::REVIEW,
        default_states::IN_PROGRESS,
        true,
    )
    .await;
    let result = CheckRetryBudget.execute(&ctx).await;

    assert!(matches!(result, HookResult::Ok), "{result:?}");
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(task.blocked_json, None);
}

#[tokio::test]
async fn check_retry_budget_ok_when_below_max() {
    let ctx = build_test_ctx(
        "task-retry-budget-ok",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    seed_transition_log(
        &ctx.db,
        &ctx.task_id,
        default_states::REVIEW,
        default_states::IN_PROGRESS,
        true,
    )
    .await;

    let result = CheckRetryBudget.execute(&ctx).await;

    assert!(matches!(result, HookResult::Ok));
}

#[tokio::test]
async fn check_retry_budget_ok_without_gate_config() {
    let mut ctx = build_test_ctx(
        "task-retry-budget-no-config",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    ctx.gate_config = None;

    let result = CheckRetryBudget.execute(&ctx).await;

    assert!(matches!(result, HookResult::Ok));
}

#[tokio::test]
async fn auto_cascade_review_failure_at_budget_blocks_with_metadata() {
    let mut ctx = build_test_ctx(
        "task-review-budget-final-attempt",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    ctx.execution_id = Some(new_uuid_v4());
    seed_review(&ctx, ReviewStatus::Failed, 1).await;
    seed_transition_log_at(
        &ctx.db,
        &ctx.task_id,
        default_states::REVIEW,
        default_states::IN_PROGRESS,
        true,
        "2026-04-17T00:00:00Z",
    )
    .await;

    sqlx::query("INSERT INTO task_budget(task_id,kind,window_id,spent) VALUES(?,'review','fixture',2) ON CONFLICT(task_id,kind) DO UPDATE SET spent=2").bind(&ctx.task_id).execute(ctx.db.pool()).await.unwrap();
    match AutoCascadeOnReviewPass.execute(&ctx).await {
        HookResult::Ok => {}
        other => panic!("expected review budget block, got {other:?}"),
    }
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let blocked = task.blocked_json.expect("blocked metadata is set");
    let blocked: serde_json::Value = serde_json::from_str(&blocked).expect("blocked json parses");
    assert_eq!(blocked["kind"], "review_gate_failed");
    assert_eq!(blocked["reason"], "review retry budget exhausted");
}

#[tokio::test]
async fn auto_cascade_review_failure_budget_blocks_with_metadata() {
    let mut ctx = build_test_ctx(
        "task-review-budget-blocks",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    ctx.execution_id = Some(new_uuid_v4());
    seed_review(&ctx, ReviewStatus::Failed, 1).await;
    seed_transition_log_at(
        &ctx.db,
        &ctx.task_id,
        default_states::REVIEW,
        default_states::IN_PROGRESS,
        true,
        "2026-04-17T00:00:00Z",
    )
    .await;
    seed_transition_log_at(
        &ctx.db,
        &ctx.task_id,
        default_states::REVIEW,
        default_states::IN_PROGRESS,
        true,
        "2026-04-17T00:01:00Z",
    )
    .await;
    let mut rx = ctx.event_bus.subscribe();

    sqlx::query("INSERT INTO task_budget(task_id,kind,window_id,spent) VALUES(?,'review','fixture',2) ON CONFLICT(task_id,kind) DO UPDATE SET spent=2").bind(&ctx.task_id).execute(ctx.db.pool()).await.unwrap();
    match AutoCascadeOnReviewPass.execute(&ctx).await {
        HookResult::Ok => {}
        other => panic!("expected review budget block, got {other:?}"),
    }
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(task.status, default_states::IN_PROGRESS);
    let blocked = task.blocked_json.expect("blocked metadata is set");
    let blocked: serde_json::Value = serde_json::from_str(&blocked).expect("blocked json parses");
    assert_eq!(blocked["kind"], "review_gate_failed");
    assert_eq!(blocked["reason"], "review retry budget exhausted");
    let event = rx.try_recv().expect("blocked event emits");
    assert_eq!(event.event_type, "task.blocked");
    match event.context {
        EventContext::TaskBlocked { reason, kind, .. } => {
            assert_eq!(reason, "review retry budget exhausted");
            assert_eq!(kind, Some(api_types::FailureKind::ReviewGateFailed));
        }
        other => panic!("expected task blocked event, got {other:?}"),
    }
}

#[tokio::test]
async fn require_upstream_roles_completed_fails_when_planner_assigned_without_planning_log() {
    let ctx = build_test_ctx(
        "task-upstream-missing-planning",
        default_states::TODO,
        default_states::IN_PROGRESS,
        Some((default_roles::PLANNER, "agent-planner-missing-log")),
    )
    .await;

    let result = RequireUpstreamRolesCompleted.execute(&ctx).await;

    match result {
        HookResult::Failed { reason } => {
            assert!(reason.contains(default_roles::PLANNER));
            assert!(reason.contains(default_states::PLANNING));
        }
        other => panic!("expected failed result, got {other:?}"),
    }
}

#[tokio::test]
async fn require_upstream_roles_completed_ok_when_no_planner_assigned() {
    let ctx = build_test_ctx(
        "task-upstream-no-planner",
        default_states::TODO,
        default_states::IN_PROGRESS,
        None,
    )
    .await;

    let result = RequireUpstreamRolesCompleted.execute(&ctx).await;

    assert!(matches!(result, HookResult::Ok));
}

#[tokio::test]
async fn require_upstream_roles_completed_ok_when_planning_log_exists() {
    let ctx = build_test_ctx(
        "task-upstream-planning-log",
        default_states::TODO,
        default_states::IN_PROGRESS,
        Some((default_roles::PLANNER, "agent-planner-with-log")),
    )
    .await;
    seed_transition_log(
        &ctx.db,
        &ctx.task_id,
        default_states::TODO,
        default_states::PLANNING,
        false,
    )
    .await;

    let result = RequireUpstreamRolesCompleted.execute(&ctx).await;

    assert!(matches!(result, HookResult::Ok));
}
