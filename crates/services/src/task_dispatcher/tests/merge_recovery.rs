//! Tests for merge recovery in the task dispatcher.

use super::*;
#[tokio::test]
async fn pull_request_merge_wait_requires_human_retry_before_direct_merge() {
    let fixture = merge_gate_fixture(chrono::Duration::minutes(3)).await;
    run_git(fixture._repo_dir.path(), &["reset", "--hard", "HEAD~2"]);
    let candidate_sha = run_git(
        fixture._workspace_dir.path().join("worktree").as_path(),
        &["rev-parse", "HEAD"],
    );
    assert_ne!(
        run_git(fixture._repo_dir.path(), &["rev-parse", "HEAD"]),
        candidate_sha
    );
    sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
        .bind(
            serde_json::json!({
                "awaiting_human": true,
                "awaiting_human_reason": "pull_request_merge",
                "awaiting_human_marker_id": "legacy-pr-marker",
            })
            .to_string(),
        )
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();

    assert_eq!(fixture.dispatcher.check_once_and_drain().await.unwrap(), 0);
    let parked = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(parked.status, "merging");
    assert_eq!(
        fixture
            .dispatcher
            .task_service
            .test_action_values(&parked.id)
            .await
            .unwrap(),
        vec![
            api_types::TaskAction::Cancel { reason: None },
            api_types::TaskAction::retry(),
            api_types::TaskAction::Approve {
                override_checks: Some(true),
                reason: None
            }
        ]
    );

    let merged = fixture
        .dispatcher
        .task_service
        .perform_task_action(
            &parked.id,
            api_types::TaskAction::Retry {
                reason: Some("operator approved direct merge".to_owned()),
                fresh_session: None,
                refresh_workspace: None,
                reset_budget: None,
                guidance: None,
            },
            parked.version,
        )
        .await
        .expect("human merge retry integrates the reviewed candidate")
        .task;
    assert_eq!(merged.status, "merging");
    fixture.dispatcher.check_once_and_drain().await.unwrap();
    let merged = fixture
        .dispatcher
        .task_service
        .drain(&parked.id)
        .await
        .unwrap();
    assert_eq!(merged.status, "done");
    assert_eq!(
        run_git(fixture._repo_dir.path(), &["rev-parse", "HEAD"]),
        candidate_sha
    );
    let metadata = db::TaskMetadata::parse(merged.metadata_json.as_deref()).unwrap();
    assert!(metadata.extra.get("awaiting_human").is_none());
    assert!(metadata.extra.get("awaiting_human_reason").is_none());
    assert!(metadata.extra.get("awaiting_human_marker_id").is_none());
}

#[tokio::test]
async fn dispatcher_resumes_integration_deferred_by_project_pause() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let task = seed_task(&db, &project_id, "accepted review", "review", 0).await;
    crate::deferred_dispatch::defer_integration_for_pause(&db, &task)
        .await
        .expect("paused integration marker records");
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    // This fixture has no workspace, so the resumed merge hook is skipped.
    // Success-atomic recovery must keep the marker for a later retry instead
    // of reporting a dispatched integration that never ran.
    assert_eq!(
        dispatched, 1,
        "resumed hook intent is queued before it runs"
    );
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, crate::workflow::default_states::MERGING);
    assert!(crate::deferred_dispatch::paused_integration(&current).is_some());
    assert!(tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn losing_paused_integration_refresh_cannot_resurrect_after_clear() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let task = seed_task(&db, &project_id, "accepted review", "review", 0).await;
    let stale_before_marker = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("pre-marker task loads")
        .expect("pre-marker task exists");
    crate::deferred_dispatch::defer_integration_for_pause(&db, &task)
        .await
        .expect("paused integration marker records");
    let stale = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("stale task loads")
        .expect("stale task exists");
    let marker = crate::deferred_dispatch::paused_integration(&stale).expect("stale marker exists");

    crate::deferred_dispatch::clear_paused_integration(&db, &task.id, &marker)
        .await
        .expect("successful worker clears marker");
    crate::deferred_dispatch::defer_integration_for_pause(&db, &stale_before_marker)
        .await
        .expect("stale producer is conditionally ignored");
    crate::deferred_dispatch::refresh_paused_integration_for_pause(&db, &stale)
        .await
        .expect("losing worker refreshes conditionally");

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("current task loads")
        .expect("current task exists");
    assert!(crate::deferred_dispatch::paused_integration(&current).is_none());
}

#[tokio::test]
async fn reviewer_assignment_after_stopped_attempt_dispatches_without_separate_resume() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "review retry", "review", 0).await;
    // The reviewer retry reviews the implementation attempt that put this
    // Task in review.
    seed_completed_coder_execution(&db, &task.id).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::REVIEWER,
        &agent_id,
    )
    .await;
    seed_cancelled_execution(
        &db,
        &task.id,
        &agent_id,
        crate::workflow::default_roles::REVIEWER,
        None,
        None,
    )
    .await;

    TaskRoleAssignmentRepo::assign(
        &*db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            role_name: crate::workflow::default_roles::REVIEWER.to_owned(),
            assignee_type: Some(db::AssigneeKind::Agent),
            assignee_id: Some(agent_id.clone()),
            created_at: "2099-01-01T00:00:00Z".to_owned(),
            updated_at: "2099-01-01T00:00:00Z".to_owned(),
        },
    )
    .await
    .expect("reviewer assignment confirms after stopped attempt");

    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    let dispatched = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("reviewer execution spawned in time")
        .expect("reviewer execution context received");
    assert_eq!(ctx.task_id, task.id);
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::REVIEWER
        )
        .await
        .expect("reviewer execution count loads"),
        2
    );
}

#[tokio::test]
async fn coordination_root_target_moved_rebase_returns_to_aggregate_review() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;

    // Keep aggregate review parked for a human decision so this test observes
    // the recovery destination without needing a reviewer execution/workspace.
    let mut workflow = crate::workflow::default_workflow::default_workflow();
    let review = workflow
        .states
        .iter_mut()
        .find(|state| state.name == crate::workflow::default_states::REVIEW)
        .expect("default workflow has review state");
    review.role = None;
    review.dispatch = None;
    let review_gate = review.gate_config.as_mut().expect("review has gate config");
    review_gate.requires_user_approval = Some(true);
    review_gate.optional_when_unassigned = Some(false);
    sqlx::query(
        "UPDATE project SET workflow_definition = ?, workflow_template_name = ?, updated_at = ? WHERE id = ?",
    )
    .bind(serde_json::to_string(&workflow).expect("workflow serializes"))
    .bind("human-required")
    .bind(now_rfc3339())
    .bind(&project_id)
    .execute(db.pool())
    .await
    .expect("project workflow updates");

    let root = seed_task(
        &db,
        &project_id,
        "coordination root",
        crate::workflow::default_states::MERGE_FAILED,
        0,
    )
    .await;
    let now = now_rfc3339();
    TaskRepo::create(
        &*db,
        CreateTask {
            id: new_uuid_v4(),
            project_id: project_id.clone(),
            parent_task_id: Some(root.id.clone()),
            subtask_order: Some(0),
            assignee_type: None,
            assignee_id: None,
            title: "completed child".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: crate::workflow::default_states::DONE.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("completed child creates");
    TransitionLogRepo::insert(
        &*db,
        db::CreateTransitionLog {
            id: new_uuid_v4(),
            task_id: root.id.clone(),
            from_state: crate::workflow::default_states::MERGING.to_owned(),
            to_state: crate::workflow::default_states::MERGE_FAILED.to_owned(),
            trigger_name: Some("retry".to_owned()),
            triggered_by: api_types::Actor::system(api_types::SystemComponent::Workflow).display(),
            bridge: api_types::TransitionBridge::new(
                api_types::TransitionBridgeKind::TargetMovedRebase,
            ),
            trigger_reason: "Target advanced; re-review required".to_owned(),
            hook_results_json: None,
            rejection: false,
            created_at: now_rfc3339(),
        },
    )
    .await
    .expect("review-refresh transition records");

    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    let dispatched = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    let recovered = TaskRepo::get_by_id(&*db, &root.id, false)
        .await
        .expect("coordination root reloads")
        .expect("coordination root exists");
    assert_eq!(
        recovered.status,
        crate::workflow::default_states::REVIEW,
        "a clean target-moved rebase must return the root to aggregate review"
    );
    assert!(!crate::task_service::coordination_review_pending(
        &recovered
    ));
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &root.id,
            crate::workflow::default_roles::CODER,
        )
        .await
        .expect("root coder execution count loads"),
        0,
        "coordination roots must never receive a merge-fix coder turn"
    );
    assert!(
        rx.try_recv().is_err(),
        "aggregate review should wait for a user"
    );
}

/// Regression coverage for F10: a Task whose newest coder Execution is the
/// normal, *successful* run that preceded review/merge (e.g. a Task sitting
/// in `merge_failed` after the coder's own attempt completed cleanly) must
/// never be gated behind the "explicit retry required" rule. That rule
/// exists for genuinely stopped attempts, not for a completed one.
#[tokio::test]
async fn latest_stopped_execution_blocks_dispatch_ignores_completed_execution() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "merge failed", "merge_failed", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    seed_completed_coder_execution(&db, &task.id).await;

    let blocked = super::helpers::latest_stopped_execution_blocks_dispatch(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
    )
    .await
    .expect("guard evaluates");

    assert!(
        !blocked,
        "a completed coder execution must not gate re-dispatch out of merge_failed"
    );
}

/// The other half of the F10 fix: a genuinely stopped attempt (failed or
/// cancelled, with no auto-retry policy) must keep gating re-dispatch exactly
/// as before — only `Completed` is now exempt.
#[tokio::test]
async fn latest_stopped_execution_blocks_dispatch_still_gates_failed_and_cancelled_executions() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;

    let cancelled_task = seed_task(&db, &project_id, "cancelled coder", "merge_failed", 0).await;
    assign_role(
        &db,
        &cancelled_task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    seed_cancelled_execution(
        &db,
        &cancelled_task.id,
        &agent_id,
        crate::workflow::default_roles::CODER,
        Some(StopReason::AgentTimeout),
        None,
    )
    .await;

    let cancelled_blocked = super::helpers::latest_stopped_execution_blocks_dispatch(
        &db,
        &cancelled_task.id,
        crate::workflow::default_roles::CODER,
    )
    .await
    .expect("guard evaluates");
    assert!(
        cancelled_blocked,
        "a cancelled coder execution with no explicit retry must still gate re-dispatch"
    );

    let failed_task = seed_task(&db, &project_id, "failed coder", "merge_failed", 0).await;
    assign_role(
        &db,
        &failed_task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let now = now_rfc3339();
    ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: failed_task.id.clone(),
            agent_id: Some(agent_id.clone()),
            role: crate::workflow::default_roles::CODER.to_owned(),
            status: ExecutionStatus::Failed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some("intentional failure".to_owned()),
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("failed execution creates");

    let failed_blocked = super::helpers::latest_stopped_execution_blocks_dispatch(
        &db,
        &failed_task.id,
        crate::workflow::default_roles::CODER,
    )
    .await
    .expect("guard evaluates");
    assert!(
        failed_blocked,
        "a failed coder execution with no explicit retry must still gate re-dispatch"
    );
}

/// Eight busy passes must not park a healthy subtask: only a refusal of the
/// landed-merge transition counts toward the sweep's bound.
#[test]
fn only_a_refused_merge_transition_counts_toward_the_sweep_bound() {
    use crate::integration_steps::reconcile::counts_toward_merge_sweep_bound;
    assert!(!counts_toward_merge_sweep_bound(&ServiceError::Db(
        db::DbError::VersionConflict
    )));
    assert!(!counts_toward_merge_sweep_bound(&ServiceError::Db(
        db::DbError::Sqlx(sqlx::Error::PoolTimedOut)
    )));
    assert!(counts_toward_merge_sweep_bound(&ServiceError::Db(
        db::DbError::InvalidTransition
    )));
    assert!(counts_toward_merge_sweep_bound(
        &ServiceError::invalid_operation("no landed-merge edge")
    ));
}

#[tokio::test]
async fn subtask_merge_transition_refusal_parks_at_the_step_attempt_bound() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let root = seed_task(&db, &project_id, "root", "in_progress", 0).await;
    let child = seed_subtask(&db, &root, "refused child", "merging", 0).await;
    refuse_subtask_merge_target(&db, &project_id).await;
    let (dispatcher, _) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    for attempt in 1..=crate::worker_runtime::queue::MAX_STEP_ATTEMPTS {
        let current = TaskRepo::get_by_id(&*db, &child.id, false)
            .await
            .unwrap()
            .unwrap();
        let result = dispatcher
            .task_service
            .reconcile_integration_admission(&current)
            .await;
        if attempt < crate::worker_runtime::queue::MAX_STEP_ATTEMPTS {
            assert!(result.is_err(), "attempt {attempt}: {result:?}");
            assert!(TaskRepo::get_by_id(&*db, &child.id, false)
                .await
                .unwrap()
                .unwrap()
                .blocked_json
                .is_none());
        } else {
            assert!(!result.unwrap(), "the bound records a park");
        }
    }
    let parked = TaskRepo::get_by_id(&*db, &child.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(parked.condition, db::TaskCondition::Parked { .. }));
    assert!(parked
        .blocked_json
        .as_deref()
        .unwrap()
        .contains("integration_admission_failed"));
    let before = db.task_steps(&child.id).await.unwrap().len();
    for _ in 0..3 {
        dispatcher.check_once_and_drain().await.unwrap();
    }
    assert_eq!(
        db.task_steps(&child.id).await.unwrap().len(),
        before,
        "the dispatcher stops asking"
    );
    let offers = dispatcher
        .task_service
        .task_action_offers(&child.id, &Actor::user(api_types::UserActionSource::Api))
        .await
        .unwrap();
    assert!(offers
        .available_actions
        .iter()
        .any(|o| o.action.verb() == "retry"));
    let cancel = offers
        .available_actions
        .iter()
        .find(|o| o.action.verb() == "cancel")
        .unwrap();
    dispatcher
        .task_service
        .perform_task_action(&child.id, cancel.action.clone(), offers.version)
        .await
        .unwrap();
    assert_eq!(
        TaskRepo::get_by_id(&*db, &child.id, false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "cancelled"
    );
}

#[tokio::test]
async fn subtask_merge_transition_success_on_second_pass_does_not_park() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let root = seed_task(&db, &project_id, "root", "in_progress", 0).await;
    let child = seed_subtask(&db, &root, "transient refusal", "merging", 0).await;
    refuse_subtask_merge_target(&db, &project_id).await;
    let (dispatcher, _) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    assert!(dispatcher
        .task_service
        .reconcile_integration_admission(&child)
        .await
        .is_err());
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    ProjectRepo::update_workflow(
        &*db,
        &project_id,
        "{}",
        None,
        project.version,
        &now_rfc3339(),
    )
    .await
    .unwrap();
    let current = TaskRepo::get_by_id(&*db, &child.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(dispatcher
        .task_service
        .reconcile_integration_admission(&current)
        .await
        .unwrap());
    dispatcher.task_service.drain(&child.id).await.unwrap();
    let child = TaskRepo::get_by_id(&*db, &child.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(child.status, "done");
    assert!(child.blocked_json.is_none());
}

// A transient `run_merge` failure (here a disconnected daemon) does not
// settle the hook step: it retries on the step queue's back-off and merges.
#[tokio::test]
async fn transient_merge_failure_retries_the_hook_step_then_merges() {
    use db::TaskStepRepo;
    let fixture = merge_gate_fixture(chrono::Duration::minutes(3)).await;
    crate::workflow::actions::merge_test_faults::inject(
        &fixture.task.id,
        ServiceError::DaemonUnavailable {
            daemon_id: "daemon-offline".into(),
        },
    );
    enter_merging(&fixture).await;
    fixture.dispatcher.drain_steps().await.unwrap();
    let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.status, "done");
    assert!(current.error_annotation.is_none());
    let steps = fixture.db.task_steps(&fixture.task.id).await.unwrap();
    let merge = steps
        .iter()
        .find(|s| s.kind == "hooks" && s.expected_status == "merging")
        .unwrap();
    assert_eq!(merge.status, "done");
    assert_eq!(merge.attempts, 2, "one retry after the transient failure");
}

// A permanent `run_merge` failure under the default (Log) policy still
// annotates the Task as a failed merge: Attention shows it and the owner's
// Retry re-runs the merge.
#[tokio::test]
async fn permanent_merge_failure_annotates_and_owner_retry_recovers() {
    use db::TaskStepRepo;
    let fixture = merge_gate_fixture(chrono::Duration::minutes(3)).await;
    crate::workflow::actions::merge_test_faults::inject(
        &fixture.task.id,
        ServiceError::invalid_operation("merge target is unreadable"),
    );
    enter_merging(&fixture).await;
    fixture.dispatcher.drain_steps().await.unwrap();
    let failed = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.status, "merging");
    let annotation: serde_json::Value =
        serde_json::from_str(failed.error_annotation.as_deref().expect("merge failure")).unwrap();
    assert_eq!(annotation["type"], "workspace_error");
    assert!(annotation["message"]
        .as_str()
        .is_some_and(|message| message.contains("merge target is unreadable")));
    let steps = fixture.db.task_steps(&fixture.task.id).await.unwrap();
    assert!(steps
        .iter()
        .any(|s| s.kind == "hooks" && s.expected_status == "merging" && s.status == "failed"));
    assert!(super::helpers::has_blocking_annotation(&failed));

    let attention_service = crate::AttentionService::new(
        Arc::clone(&fixture.db),
        crate::test_support::noop_wake_turns(),
    );
    while attention_service
        .project_once(100)
        .await
        .unwrap()
        .processed_events
        > 0
    {}
    let attention: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM attention_projection WHERE status = 'open' AND json_extract(details_json, '$.entity_id') = ?",
    )
    .bind(&fixture.task.id)
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    assert!(attention >= 1, "the failed merge needs the owner");

    let offers = fixture
        .dispatcher
        .task_service
        .task_action_offers(
            &fixture.task.id,
            &Actor::user(api_types::UserActionSource::Test),
        )
        .await
        .unwrap();
    let retry = offers
        .available_actions
        .iter()
        .find(|offer| offer.reason == "merge_gate_retry")
        .expect("owner Retry re-runs the merge");
    fixture
        .dispatcher
        .task_service
        .perform_task_action(&fixture.task.id, retry.action.clone(), offers.version)
        .await
        .unwrap();
    // An accepted action is a queued recovery the dispatcher applies.
    fixture.dispatcher.check_once_and_drain().await.unwrap();
    let recovered = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.status, "done");
    assert!(recovered.error_annotation.is_none());
}

// Transient failures are bounded by the step retry budget; once it is spent
// the merge settles as a failed merge the owner can retry.
#[tokio::test]
async fn transient_merge_failures_settle_as_merge_failure_once_retries_are_spent() {
    use db::TaskStepRepo;
    let fixture = merge_gate_fixture(chrono::Duration::minutes(3)).await;
    crate::workflow::actions::merge_test_faults::inject(
        &fixture.task.id,
        ServiceError::DaemonTimeout {
            daemon_id: "daemon-slow".into(),
            method: "workspace.merge".into(),
        },
    );
    enter_merging(&fixture).await;
    // Seven earlier attempts already spent their back-off.
    sqlx::query("UPDATE task_step SET attempts = 7 WHERE task_id = ?")
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    fixture.dispatcher.drain_steps().await.unwrap();
    let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.status, "merging");
    let annotation: serde_json::Value =
        serde_json::from_str(current.error_annotation.as_deref().expect("merge failure")).unwrap();
    assert_eq!(annotation["type"], "workspace_error");
    let steps = fixture.db.task_steps(&fixture.task.id).await.unwrap();
    let merge: Vec<_> = steps
        .iter()
        .filter(|step| step.kind == "hooks" && step.expected_status == "merging")
        .collect();
    assert_eq!(merge.len(), 1);
    assert_eq!(merge[0].status, "failed");
    assert_eq!(merge[0].attempts, 8);
}
