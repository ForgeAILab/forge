//! Tests for review recovery in the task dispatcher.

use super::*;
#[tokio::test]
async fn dispatcher_recovers_undispatched_active_task() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "active", "in_progress", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("execution spawned in time")
        .expect("execution context received");
    assert_eq!(ctx.task_id, task.id);
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER
        )
        .await
        .expect("execution count loads"),
        1
    );
}

#[tokio::test]
async fn dispatcher_recovers_undispatched_reviewer_task() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "review", "review", 0).await;
    // Review is entered from a finished implementation attempt, and that
    // attempt is what the recovered reviewer reviews.
    seed_completed_coder_execution(&db, &task.id).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::REVIEWER,
        &agent_id,
    )
    .await;
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
    // The shell reviewer no longer emits a hardcoded PASS; the dispatcher must
    // thread the frozen review contract to it instead.
    assert!(ctx.description.contains("FORGE_REVIEW_CONTRACT"));
    assert!(ctx.description.contains("FORGE_GOVERNING_CONTEXT"));
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::REVIEWER
        )
        .await
        .expect("reviewer execution count loads"),
        1
    );
}

#[tokio::test]
async fn dispatcher_reconciles_completed_reviewer_and_launches_its_retry() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "stuck review", "review", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::REVIEWER,
        &agent_id,
    )
    .await;
    let candidate_execution = seed_completed_coder_execution(&db, &task.id).await;
    seed_running_review(&db, &task.id, &candidate_execution, r#"{"ci_steps":[]}"#).await;
    let execution =
        seed_completed_reviewer_execution(&db, &task.id, &agent_id, Some(&candidate_execution))
            .await;
    let review = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("review loads")
        .into_iter()
        .next()
        .expect("review exists");
    sqlx::query("UPDATE review SET reviewer_execution_id = ? WHERE id = ?")
        .bind(&execution.id)
        .bind(&review.id)
        .execute(db.pool())
        .await
        .expect("reviewer attempt binding records");
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let first = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher reconciles");

    assert_eq!(first, 0);
    let review = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load")
        .into_iter()
        .next()
        .expect("review exists");
    assert_eq!(review.status, ReviewStatus::Running);
    let details: serde_json::Value =
        serde_json::from_str(&review.step_results_json).expect("review details parse");
    assert_eq!(details["execution_retry"]["execution_id"], execution.id);
    let execution = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(execution.resume_policy, Some(ResumePolicy::Auto));

    let deferred = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(deferred_dispatch::pending_until(&deferred).is_some());
    deferred_dispatch::clear(&db, &deferred)
        .await
        .expect("retry backoff elapses");

    let second = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher launches retry");

    assert_eq!(second, 1);
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("review retry spawned in time")
        .expect("review retry context received");
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
async fn dispatcher_replaces_completed_custom_role_from_superseded_project_revision() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let custom_role = "analyst";
    let mut workflow = crate::workflow::default_workflow::default_workflow();
    let planner_role = workflow
        .roles
        .iter_mut()
        .find(|role| role.name == crate::workflow::default_roles::PLANNER)
        .expect("default workflow defines planner role");
    planner_role.name = custom_role.to_owned();
    planner_role.display_name = "Analyst".to_owned();
    workflow
        .states
        .iter_mut()
        .find(|state| state.name == crate::workflow::default_states::PLANNING)
        .expect("default workflow defines planning state")
        .role = Some(custom_role.to_owned());
    sqlx::query("UPDATE project SET workflow_definition = ?, updated_at = ? WHERE id = ?")
        .bind(serde_json::to_string(&workflow).expect("custom workflow serializes"))
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("custom workflow persists");

    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(
        &db,
        &project_id,
        "stale custom role",
        crate::workflow::default_states::PLANNING,
        0,
    )
    .await;
    assign_role(&db, &task.id, custom_role, &agent_id).await;
    let dispatch_project_version = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("Project loads")
        .expect("Project exists")
        .version;
    let now = now_rfc3339();
    let stale = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: custom_role.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: Some("custom-role-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("completed under old Project authority".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                serde_json::json!({
                    "backend_kind": "native",
                    "executor_type": "shell",
                    "config": {},
                    "plan_delivery": "execution_outbox",
                    "project_version": dispatch_project_version,
                    "task_state": crate::workflow::default_states::PLANNING,
                    "state_entry_token": null,
                })
                .to_string(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("completed custom-role execution creates");
    assert!(
        !crate::task_service::execution::execution_uses_brokered_plan(&stale),
        "custom roles do not own plan-publication authority"
    );
    assert!(
        crate::task_service::execution::execution_belongs_to_current_state_entry(
            &db, &task, &stale,
        )
        .await
        .expect("same-state entry authority resolves"),
        "the stale execution must reach project-authority reconciliation"
    );

    sqlx::query("UPDATE project SET version = version + 1, updated_at = ? WHERE id = ?")
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("Project revision advances");
    let current_project_version = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("Project loads")
        .expect("Project exists")
        .version;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("replacement custom-role execution spawned in time")
        .expect("replacement execution context received");
    assert_eq!(ctx.task_id, task.id);
    assert_ne!(ctx.execution_id, stale.id);
    let replacement = ExecutionRepo::get_by_id(&*db, &ctx.execution_id)
        .await
        .expect("replacement execution loads")
        .expect("replacement execution exists");
    assert_eq!(replacement.role, custom_role);
    assert_eq!(
        crate::task_service::execution_dispatch_project_version(&replacement),
        Some(current_project_version)
    );
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(&*db, &task.id, custom_role)
            .await
            .expect("custom-role execution count loads"),
        2
    );
}

#[tokio::test]
async fn dispatcher_replaces_completed_reviewer_from_superseded_project_revision() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "stale review", "review", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::REVIEWER,
        &agent_id,
    )
    .await;
    let candidate_execution = seed_completed_coder_execution(&db, &task.id).await;
    seed_running_review(&db, &task.id, &candidate_execution, r#"{"ci_steps":[]}"#).await;
    let stale =
        seed_completed_reviewer_execution(&db, &task.id, &agent_id, Some(&candidate_execution))
            .await;
    let review = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("review loads")
        .into_iter()
        .next()
        .expect("review exists");
    sqlx::query("UPDATE review SET reviewer_execution_id = ? WHERE id = ?")
        .bind(&stale.id)
        .bind(&review.id)
        .execute(db.pool())
        .await
        .expect("reviewer attempt binding records");

    sqlx::query("UPDATE project SET version = version + 1, updated_at = ? WHERE id = ?")
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("Project revision advances");
    let current_project_version = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("Project loads")
        .expect("Project exists")
        .version;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("replacement reviewer spawned in time")
        .expect("replacement reviewer execution context received");
    assert_eq!(ctx.task_id, task.id);
    assert_ne!(ctx.execution_id, stale.id);

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, crate::workflow::default_states::REVIEW);
    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::Running);
    assert_eq!(
        reviews[0].reviewer_execution_id.as_deref(),
        Some(ctx.execution_id.as_str())
    );
    let replacement = ExecutionRepo::get_by_id(&*db, &ctx.execution_id)
        .await
        .expect("replacement execution loads")
        .expect("replacement execution exists");
    assert_eq!(
        crate::task_service::execution_dispatch_project_version(&replacement),
        Some(current_project_version)
    );
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::REVIEWER,
        )
        .await
        .expect("reviewer execution count loads"),
        2
    );
}

#[tokio::test]
async fn dispatcher_never_reuses_reviewer_execution_for_newer_review_attempt() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "fresh review", "review", 0).await;
    sqlx::query("UPDATE task SET task_type = 'discovery' WHERE id = ?")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("task becomes read-only");
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::REVIEWER,
        &agent_id,
    )
    .await;
    let stale_candidate_execution = seed_completed_coder_execution(&db, &task.id).await;
    let stale = seed_completed_reviewer_execution(
        &db,
        &task.id,
        &agent_id,
        Some(&stale_candidate_execution),
    )
    .await;
    sqlx::query("UPDATE execution SET created_at = ?, updated_at = ?, stopped_at = ? WHERE id = ?")
        .bind("2000-01-01T00:00:00Z")
        .bind("2000-01-01T00:00:01Z")
        .bind("2000-01-01T00:00:01Z")
        .bind(&stale.id)
        .execute(db.pool())
        .await
        .expect("stale reviewer parent binding moves behind current review");
    let candidate_execution = seed_completed_coder_execution(&db, &task.id).await;
    seed_running_review(&db, &task.id, &candidate_execution, r#"{"ci_steps":[]}"#).await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("fresh reviewer spawned in time")
        .expect("reviewer execution context received");
    assert_eq!(ctx.task_id, task.id);
    assert_ne!(ctx.execution_id, stale.id);
    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1, "no synthetic review row is created");
    assert_eq!(reviews[0].status, ReviewStatus::Running);
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::REVIEWER,
        )
        .await
        .expect("reviewer execution count loads"),
        2
    );
}

#[tokio::test]
async fn reviewer_completion_guard_fails_closed_without_exact_binding() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "ambiguous review", "review", 0).await;
    let candidate_execution = seed_completed_coder_execution(&db, &task.id).await;
    seed_running_review(&db, &task.id, &candidate_execution, r#"{"ci_steps":[]}"#).await;
    let stale = seed_completed_reviewer_execution(&db, &task.id, &agent_id, None).await;

    // The reviewer is intentionally newer by wall-clock time, but it has no
    // exact Review-attempt binding. Candidate parentage and timestamps cannot
    // repair that missing identity.
    sqlx::query("UPDATE execution SET created_at = ? WHERE id = ?")
        .bind("2026-01-01T00:00:01Z")
        .bind(&stale.id)
        .execute(db.pool())
        .await
        .expect("execution timestamp updates");
    sqlx::query("UPDATE review SET started_at = ? WHERE task_id = ?")
        .bind("2026-01-01T00:00:00Z")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("review timestamp updates");
    let execution = ExecutionRepo::get_by_id(&*db, &stale.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    let review = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load")
        .into_iter()
        .next()
        .expect("review exists");
    assert!(
        crate::task_service::execution::reviewer_execution_lacks_exact_review_binding(
            &execution, &review,
        )
    );

    let mut rebound = review.clone();
    rebound.reviewer_execution_id = Some("replacement-reviewer".to_owned());
    assert!(
        crate::task_service::execution::reviewer_execution_lacks_exact_review_binding(
            &execution, &rebound,
        ),
        "replacing a Review attempt binding must reject the old execution"
    );

    sqlx::query("UPDATE execution SET created_at = ? WHERE id = ?")
        .bind("not-an-rfc3339-timestamp")
        .bind(&stale.id)
        .execute(db.pool())
        .await
        .expect("malformed execution timestamp updates");
    let execution = ExecutionRepo::get_by_id(&*db, &stale.id)
        .await
        .expect("execution reloads")
        .expect("execution exists");
    assert!(
        crate::task_service::execution::reviewer_execution_lacks_exact_review_binding(
            &execution, &review,
        )
    );

    sqlx::query("UPDATE execution SET created_at = ? WHERE id = ?")
        .bind("2026-01-01T00:00:01Z")
        .bind(&stale.id)
        .execute(db.pool())
        .await
        .expect("execution timestamp restores");
    sqlx::query("UPDATE review SET started_at = ? WHERE task_id = ?")
        .bind("not-an-rfc3339-timestamp")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("malformed review timestamp updates");
    let execution = ExecutionRepo::get_by_id(&*db, &stale.id)
        .await
        .expect("execution reloads")
        .expect("execution exists");
    let review = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews reload")
        .into_iter()
        .next()
        .expect("review exists");
    assert!(
        crate::task_service::execution::reviewer_execution_lacks_exact_review_binding(
            &execution, &review,
        )
    );
}

#[tokio::test]
async fn dispatcher_skips_reviewer_until_configured_ci_has_finished() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "review", "review", 0).await;
    let task = set_review_ci_config(&db, &task).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::REVIEWER,
        &agent_id,
    )
    .await;
    let coder_execution_id = seed_completed_coder_execution(&db, &task.id).await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    assert_eq!(dispatched, 0);
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::REVIEWER
        )
        .await
        .expect("execution count loads"),
        0
    );
    assert!(tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .is_err());

    seed_running_review(
        &db,
        &task.id,
        &coder_execution_id,
        r#"{"ci_steps":[{"index":0,"command":"test -d .","exit_code":0,"stderr_tail":"","output_tail":""}]}"#,
    )
    .await;

    let dispatched = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("execution spawned in time")
        .expect("execution context received");
    assert_eq!(ctx.task_id, task.id);
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::REVIEWER
        )
        .await
        .expect("execution count loads"),
        1
    );
}

#[tokio::test]
async fn dispatcher_dispatches_read_only_reviewer_without_ci_review_record() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "research review", "review", 0).await;
    let task = set_review_ci_config(&db, &task).await;
    sqlx::query("UPDATE task SET task_type = 'discovery' WHERE id = ?")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("task becomes read-only discovery work");
    seed_completed_coder_execution(&db, &task.id).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::REVIEWER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("execution spawned in time")
        .expect("execution context received");
    assert_eq!(ctx.task_id, task.id);
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::REVIEWER
        )
        .await
        .expect("execution count loads"),
        1
    );
}
