//! Workflow action role dispatch tests.

use super::*;

#[tokio::test]
async fn dispatch_role_agent_skips_without_coder_assignment() {
    let ctx = build_test_ctx(
        "task-dispatch-no-coder",
        default_states::TODO,
        default_states::IN_PROGRESS,
        None,
    )
    .await;

    let result = DispatchRoleAgent.execute(&ctx).await;

    assert!(matches!(result, HookResult::Skipped { .. }));
}

#[tokio::test]
async fn dispatch_role_agent_emits_event_for_coder_assignment() {
    let agent_id = "agent-coder-dispatch";
    let mut harness = build_role_dispatch_harness_with_executor(
        "task-dispatch-coder",
        default_states::TODO,
        default_states::IN_PROGRESS,
        default_roles::CODER,
        agent_id,
        1,
        "codex",
    )
    .await;

    let ctx = harness.ctx.clone();
    let mut rx = ctx.event_bus.subscribe();

    let result = DispatchRoleAgent.execute(&ctx).await;

    assert!(matches!(result, HookResult::Ok));
    let event = rx.try_recv().expect("dispatch event emits");
    assert_eq!(event.event_type, "task.role_agent_dispatched");
    assert_eq!(event.entity_id, ctx.task_id);
    match event.context {
        EventContext::TaskRoleAgentDispatched {
            task_id,
            role,
            agent_id: dispatched_agent_id,
            state,
            prompt_system,
            prompt_user,
            parent_execution_id: _,
        } => {
            assert_eq!(task_id, ctx.task_id);
            assert_eq!(role, default_roles::CODER);
            assert_eq!(dispatched_agent_id, agent_id);
            assert_eq!(state, default_states::IN_PROGRESS);
            assert!(prompt_system.contains("coder"));
            assert!(prompt_user.contains("test task"));
        }
        other => panic!("unexpected event context: {other:?}"),
    }

    let execution_ctx = tokio::time::timeout(std::time::Duration::from_secs(30), harness.rx.recv())
        .await
        .expect("executor receives dispatch")
        .expect("executor context exists");
    assert!(execution_ctx
        .description
        .contains("Forge role contract (authoritative):"));
    assert!(execution_ctx.description.contains("Coder boundary:"));
    assert!(execution_ctx.description.contains("test task"));
}

#[tokio::test]
async fn dispatch_role_agent_uses_dirty_worktree_prompt_from_task_annotation() {
    let agent_id = "agent-coder-dirty-worktree";
    let mut harness = build_role_dispatch_harness_with_executor(
        "task-dispatch-dirty-worktree",
        default_states::MERGING,
        default_states::MERGE_FAILED,
        default_roles::CODER,
        agent_id,
        1,
        "codex",
    )
    .await;

    sqlx::query(
        "UPDATE task
         SET review_passed_at = ?, error_annotation = ?
         WHERE id = ?",
    )
    .bind("2026-04-17T10:00:00Z")
    .bind(
        json!({
            "type": "dirty_worktree",
            "message": "worktree has uncommitted changes: src/lib.rs"
        })
        .to_string(),
    )
    .bind(&harness.ctx.task_id)
    .execute(harness.ctx.db.pool())
    .await
    .expect("dirty worktree annotation persists");
    harness
        .ctx
        .db
        .check_task_conditions_of(std::slice::from_ref(&harness.ctx.task_id))
        .await
        .expect("the stored condition follows the raw write");

    let dispatch_result = DispatchRoleAgent.execute(&harness.ctx).await;
    assert!(
        matches!(dispatch_result, HookResult::Ok),
        "{dispatch_result:?}"
    );

    let execution_ctx = tokio::time::timeout(std::time::Duration::from_secs(30), harness.rx.recv())
        .await
        .expect("coder executor spawned in time")
        .expect("coder execution context received");
    assert!(execution_ctx.description.contains("uncommitted changes"));
    assert!(execution_ctx
        .description
        .contains("do not rebase or discard it"));
    assert!(!execution_ctx
        .description
        .contains("Rebase your worktree branch onto the latest default branch"));
}

#[tokio::test]
async fn review_refresh_bypasses_merge_fix_notification_and_dispatch() {
    let agent_id = "agent-coder-review-refresh";
    let mut harness = build_role_dispatch_harness(
        "task-review-refresh",
        default_states::MERGING,
        default_states::MERGE_FAILED,
        default_roles::CODER,
        agent_id,
        1,
    )
    .await;
    sqlx::query(
        "INSERT INTO transition_log
         (id, task_id, from_state, to_state, trigger_name, triggered_by,
          trigger_reason, hook_results_json, rejection, created_at, bridge_kind)
         VALUES (?, ?, ?, ?, 'retry', 'system:workflow', ?, NULL, 0, ?, 'review_refresh')",
    )
    .bind(new_uuid_v4())
    .bind(&harness.ctx.task_id)
    .bind(default_states::MERGING)
    .bind(default_states::MERGE_FAILED)
    .bind("Reviewed commit changed; fresh review required")
    .bind(now_rfc3339())
    .execute(harness.ctx.db.pool())
    .await
    .expect("review-refresh transition inserts");

    let notify = NotifyRoleHolder.execute(&harness.ctx).await;
    assert!(matches!(
        notify,
        HookResult::Skipped { reason } if reason.contains("requires review")
    ));

    let dispatch = DispatchRoleAgent.execute(&harness.ctx).await;
    assert!(matches!(
            dispatch,
            HookResult::Cascade { to, bridge,

    ..
    }
                if to == default_states::REVIEW
                    && bridge.bridge_kind == Some(api_types::TransitionBridgeKind::ReviewRefresh)
        ));
    assert!(harness.rx.try_recv().is_err());
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*harness.ctx.db,
            &harness.ctx.task_id,
            default_roles::CODER,
        )
        .await
        .expect("coder execution count loads"),
        0
    );
}

#[test]
fn review_refresh_bridge_does_not_reset_or_spend_merge_fix_window() {
    let task_id = "task-merge-fix-boundary".to_owned();
    let workflow_actor = api_types::Actor::system(api_types::SystemComponent::Workflow).display();
    let now = now_rfc3339();
    let rejection = |reason: &str| db::TransitionLog {
        id: new_uuid_v4(),
        task_id: task_id.clone(),
        from_state: default_states::MERGING.to_owned(),
        to_state: default_states::MERGE_FAILED.to_owned(),
        trigger_name: Some("retry".to_owned()),
        triggered_by: workflow_actor.clone(),
        bridge: Default::default(),
        trigger_reason: reason.to_owned(),
        hook_results_json: None,
        rejection: true,
        created_at: now.clone(),
    };
    let bridge = db::TransitionLog {
        id: new_uuid_v4(),
        task_id: task_id.clone(),
        from_state: default_states::MERGING.to_owned(),
        to_state: default_states::MERGE_FAILED.to_owned(),
        trigger_name: Some("retry".to_owned()),
        triggered_by: workflow_actor.clone(),
        bridge: api_types::TransitionBridge::new(api_types::TransitionBridgeKind::ReviewRefresh),
        trigger_reason: "Target advanced; re-review required".to_owned(),
        hook_results_json: None,
        // A direct caller may supply rejection=true; the engine normalizes the
        // durable row, and the counter remains defensive for pre-existing rows.
        rejection: true,
        created_at: now.clone(),
    };

    assert_eq!(
        crate::task_diagnostics::audit_gate_rejections_since_boundary(
            &[rejection("merge conflict"), bridge],
            default_states::MERGING,
        ),
        1,
        "a review-refresh bridge must not erase or consume the prior window"
    );
}

#[tokio::test]
async fn dispatch_role_agent_initial_dispatch_creates_execution_with_capacity() {
    let agent_id = "agent-coder-initial";
    let mut harness = build_initial_dispatch_harness("task-dispatch-initial", agent_id, 1).await;
    let ctx = harness.ctx.clone();

    let result = DispatchRoleAgent.execute(&ctx).await;

    assert!(matches!(result, HookResult::Ok), "{result:?}");
    let execution_ctx = tokio::time::timeout(std::time::Duration::from_secs(1), harness.rx.recv())
        .await
        .expect("executor spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, ctx.task_id);

    let executions = ExecutionRepo::list_by_task_and_role(
        &*ctx.db,
        &ctx.task_id,
        default_roles::CODER,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions load");
    assert_eq!(executions.items.len(), 1);
    assert_eq!(executions.items[0].status, ExecutionStatus::Running);
    assert_eq!(executions.items[0].agent_id.as_deref(), Some(agent_id));
}

#[tokio::test]
async fn dispatch_role_agent_fails_initial_dispatch_without_project_repo() {
    let agent_id = "agent-coder-no-repo";
    let mut harness = build_no_repo_dispatch_harness("task-dispatch-no-repo", agent_id, 1).await;
    let ctx = harness.ctx.clone();

    let result = DispatchRoleAgent.execute(&ctx).await;

    match result {
        HookResult::Failed { reason } => assert_eq!(
            reason,
            format!("project {} has no primary repo", ctx.project_id)
        ),
        other => panic!("expected failed result, got {other:?}"),
    }
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), harness.rx.recv())
            .await
            .is_err()
    );
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(&*ctx.db, &ctx.task_id, default_roles::CODER)
            .await
            .expect("execution count loads"),
        0
    );
}

#[tokio::test]
async fn dispatch_role_agent_initial_dispatch_skips_at_capacity() {
    let agent_id = "agent-coder-capacity";
    let mut harness = build_initial_dispatch_harness("task-dispatch-capacity", agent_id, 1).await;
    let ctx = harness.ctx.clone();
    let other_task_id = "task-dispatch-capacity-other";
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    TaskRepo::create(
        &*ctx.db,
        CreateTask {
            id: other_task_id.to_owned(),
            project_id: task.project_id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "other task".to_owned(),
            description: Some("echo other".to_owned()),
            task_type: "task".to_owned(),
            status: default_states::IN_PROGRESS.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("other task creates");
    assign_agent_role(&ctx.db, other_task_id, default_roles::CODER, agent_id).await;
    seed_running_execution_for_task(&ctx.db, other_task_id, agent_id, default_roles::CODER).await;
    crate::test_support::set_test_agent_capacity(&ctx.db, agent_id, 1).await;

    let result = DispatchRoleAgent.execute(&ctx).await;

    match result {
        HookResult::Skipped { reason } => assert_eq!(reason, "agent at capacity"),
        other => panic!("expected skipped result, got {other:?}"),
    }
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), harness.rx.recv())
            .await
            .is_err()
    );
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(&*ctx.db, &ctx.task_id, default_roles::CODER)
            .await
            .expect("execution count loads"),
        0
    );
}

#[tokio::test]
async fn dispatch_role_agent_initial_dispatch_skips_when_execution_already_running() {
    let agent_id = "agent-coder-running";
    let mut harness = build_initial_dispatch_harness("task-dispatch-running", agent_id, 1).await;
    let ctx = harness.ctx.clone();
    seed_running_execution_for_task(&ctx.db, &ctx.task_id, agent_id, "executor").await;

    let result = DispatchRoleAgent.execute(&ctx).await;

    match result {
        HookResult::Skipped { reason } => assert_eq!(reason, "execution already running"),
        other => panic!("expected skipped result, got {other:?}"),
    }
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), harness.rx.recv())
            .await
            .is_err()
    );
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(&*ctx.db, &ctx.task_id, default_roles::CODER)
            .await
            .expect("coder execution count loads"),
        0
    );
}

#[tokio::test]
async fn notify_role_holder_skips_without_coder_assignment() {
    let ctx = build_test_ctx(
        "task-notify-no-coder",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;

    let result = NotifyRoleHolder.execute(&ctx).await;

    assert!(matches!(result, HookResult::Skipped { .. }));
}

#[tokio::test]
async fn notify_role_holder_emits_event_for_coder_assignment() {
    let agent_id = "agent-coder-notify";
    let ctx = build_test_ctx(
        "task-notify-coder",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        Some((default_roles::CODER, agent_id)),
    )
    .await;
    let mut rx = ctx.event_bus.subscribe();

    let result = NotifyRoleHolder.execute(&ctx).await;

    assert!(matches!(result, HookResult::Ok));
    let event = rx.try_recv().expect("notification event emits");
    assert_eq!(event.event_type, "task.role_notified");
    assert_eq!(event.entity_id, ctx.task_id);
    match event.context {
        EventContext::TaskRoleNotified {
            task_id,
            role,
            notified_agent_id,
            notified_user_handle,
            state,
            reason,
        } => {
            assert_eq!(task_id, ctx.task_id);
            assert_eq!(role, default_roles::CODER);
            assert_eq!(notified_agent_id.as_deref(), Some(agent_id));
            assert_eq!(notified_user_handle, None);
            assert_eq!(state, default_states::REVIEW);
            assert!(reason.contains(default_states::REVIEW));
        }
        other => panic!("unexpected event context: {other:?}"),
    }
}
