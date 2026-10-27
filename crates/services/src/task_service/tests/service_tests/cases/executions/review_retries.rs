//! Reviewer execution retries, check recovery and owner parking.
use super::*;

#[tokio::test]
async fn completed_reviewer_execution_keeps_the_task_retry_budget_it_has_spent() {
    // A reviewer process exiting cleanly says nothing about whether its verdict
    // was usable. Clearing the retry budget on that completion reset the counter
    // on every attempt, so `attempt > budget` never tripped and a reviewer whose
    // assessment could not be parsed was re-dispatched without bound. The
    // sibling bounded-retry tests pre-seed the Execution ledger and drive the
    // cascade directly, so none of them covered this reset.
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Reviewer completes without a usable verdict",
            Some("review the current worktree".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let claimed = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    sqlx::query("UPDATE execution SET role = 'reviewer' WHERE id = ?")
        .bind(&claimed.execution.id)
        .execute(db.pool())
        .await
        .expect("execution becomes reviewer-scoped");
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::REVIEWER,
        claimed.execution.agent_id.as_deref(),
    )
    .await;
    seed_budget_spent(&db, &task.id, db::budget::Kind::Execution, 2).await;
    sqlx::query(
        "UPDATE task SET status = 'review', metadata_json = ?, version = version + 1 WHERE id = ?",
    )
    .bind("{}")
    .bind(&task.id)
    .execute(db.pool())
    .await
    .expect("task enters review carrying spent budget");
    let now = now_rfc3339();
    ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: claimed.execution.id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("running review binds the reviewer attempt");

    let updated = service
        .run_execution(claimed.execution.id.clone(), &NoDiffExecutor)
        .await
        .expect("reviewer execution completes");
    assert_eq!(updated.status, ExecutionStatus::Completed);
    assert_eq!(updated.role, crate::workflow::default_roles::REVIEWER);

    let task_after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let metadata: serde_json::Value =
        serde_json::from_str(task_after.metadata_json.as_deref().unwrap_or("{}"))
            .expect("task metadata parses");
    assert_eq!(
        Some(
            db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
                .await
                .unwrap() as u64
        ),
        Some(2),
        "a completed reviewer must not refund the retry budget it has already spent: {metadata}"
    );
}

#[tokio::test]
async fn reviewer_provider_unavailability_uses_bounded_task_retry_budget() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Review after provider recovery",
            Some("review the current worktree".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let claimed = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    // The run is rewritten into the review state's run, snapshot included: a
    // failure only counts for the state entry its run was dispatched for.
    sqlx::query(
        "UPDATE execution
         SET role = 'reviewer',
             executor_config_snapshot_json = json_remove(
                 json_set(executor_config_snapshot_json, '$.task_state', 'review'),
                 '$.state_entry_token')
         WHERE id = ?",
    )
    .bind(&claimed.execution.id)
    .execute(db.pool())
    .await
    .expect("execution becomes reviewer-scoped");
    // A reviewer execution is only authorized while the Task is the
    // reviewer's to work, so move the Task with its assignment the way the
    // workflow would before the reviewer runs.
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::REVIEWER,
        claimed.execution.agent_id.as_deref(),
    )
    .await;
    sqlx::query("UPDATE task SET status = 'review', version = version + 1 WHERE id = ?")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("task enters review");
    let now = now_rfc3339();
    ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: claimed.execution.id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("running review binds the reviewer attempt");

    let updated = service
        .run_execution(claimed.execution.id.clone(), &ExecutorUnavailableExecutor)
        .await
        .expect("provider unavailability is persisted");

    assert_eq!(updated.status, ExecutionStatus::Failed);
    assert_eq!(updated.role, "reviewer");
    assert!(updated
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("provider unavailable"));

    let execution_after = ExecutionRepo::get_by_id(&*db, &claimed.execution.id)
        .await
        .expect("execution reloads")
        .expect("execution exists");
    assert_eq!(execution_after.resume_policy, Some(db::ResumePolicy::Auto));

    let task_after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let metadata: serde_json::Value =
        serde_json::from_str(task_after.metadata_json.as_deref().unwrap_or("{}"))
            .expect("task metadata parses");
    assert!(
        metadata.get("deferred_dispatch").is_some(),
        "reviewer execution should wait for provider recovery: {metadata}"
    );
    assert_eq!(
        json!(
            db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
                .await
                .unwrap()
        ),
        1
    );
    assert!(task_after.blocked_json.is_none());
}

#[tokio::test]
async fn dispatch_initial_role_execution_runs_reviewer_when_agent_is_busy_on_same_task() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::new(NoDiffExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;

    TaskRoleAssignmentRepo::assign(
        &*db,
        role_assignment_input(
            &task.id,
            crate::workflow::default_roles::REVIEWER,
            Some(agent_id.clone()),
            None,
        ),
    )
    .await
    .expect("reviewer assignment created");

    // The same agent already holds this Task's executor attempt, which is
    // also the candidate the review is bound to. A reviewer execution is
    // only admissible against that exact candidate, so build the review-bound
    // admission the workflow dispatcher builds.
    let candidate = seed_completed_coder_execution(&db, &task, &agent_id, None).await;
    let now = now_rfc3339();
    let review = ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate.id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("running review creates");
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    let agent = AgentRepo::get_by_id(&*db, &agent_id)
        .await
        .expect("agent loads")
        .expect("agent exists");
    let mut admission = crate::task_service::execution_admission_for_task(
        &db,
        &task,
        &project.workflow_definition,
        crate::workflow::default_roles::REVIEWER,
        Some(&agent),
        project.version,
    )
    .await
    .expect("reviewer admission builds");
    admission.expected_reviewer_parent_execution_id = Some(candidate.id.clone());
    admission.expected_latest_review_candidate_execution_id = Some(candidate.id.clone());
    admission.expected_reviewer_id = Some(review.id.clone());
    admission.expected_reviewer_attempt_number = Some(review.attempt_number);
    admission.expected_reviewer_status = Some(review.status.to_string());
    admission.expected_reviewer_updated_at = Some(review.updated_at.clone());

    let execution = service
        .dispatch_initial_role_execution_with_metadata_and_admission(
            &task.id,
            &agent_id,
            crate::workflow::default_roles::REVIEWER,
            "review the task".to_owned(),
            None,
            admission,
        )
        .await
        .expect("reviewer dispatch succeeds");

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let current = ExecutionRepo::get_by_id(&*db, &execution.id)
                .await
                .expect("execution loads")
                .expect("execution exists");
            if current.status == ExecutionStatus::Completed {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("reviewer execution completes");
}

#[tokio::test]
async fn failed_reviewer_execution_keeps_review_running_while_retry_waits() {
    assert_failed_reviewer_disposition(0, 3, true, false).await;
}

#[tokio::test]
async fn failed_reviewer_execution_blocks_when_retries_are_exhausted() {
    assert_failed_reviewer_disposition(3, 3, false, false).await;
}

#[tokio::test]
async fn failed_reviewer_execution_blocks_when_retries_are_disabled() {
    assert_failed_reviewer_disposition(0, 0, false, false).await;
}

#[tokio::test]
async fn precontract_reviewer_pass_uses_bounded_protocol_failure_dispositions() {
    for (count, budget, retry) in [(0, 3, true), (3, 3, false), (0, 0, false)] {
        assert_failed_reviewer_disposition(count, budget, retry, true).await;
    }
}

#[tokio::test]
async fn timed_out_review_checks_rerun_alone_then_park_without_a_new_reviewer() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review_with(
        r#"{"retry_budgets":{"execution":3,"review":3},"review":{"ci_steps":["sleep 3"],"check_timeout_seconds":1}}"#,
    )
    .await;
    // Live case (NK-28): the reviewer passed, but Forge's own clean-checkout
    // `cargo test` outran the limit while a sibling held the build lock, and
    // every timeout dispatched a whole new reviewer.
    complete_review_with(
        &db,
        &service,
        &execution,
        "All criteria met.\n\n{\"result\": \"pass\", \"reason\": \"criteria met\"}",
    )
    .await;

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, crate::workflow::default_states::REVIEW);
    let annotation: api_types::TaskBlockingAnnotation = serde_json::from_str(
        current
            .error_annotation
            .as_deref()
            .expect("the Task is parked for its owner"),
    )
    .expect("annotation parses");
    assert_eq!(
        annotation.annotation_type,
        api_types::FailureKind::ReviewBlocked
    );
    let message = annotation.message.unwrap_or_default();
    assert!(
        message.contains("`sleep 3` ran longer than 1s"),
        "{message}"
    );
    assert!(message.contains("3 attempts"), "{message}");

    let review = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load")
        .into_iter()
        .next()
        .expect("review exists");
    assert!(
        !review.step_results_json.contains("execution_retry"),
        "no replacement reviewer is scheduled: {}",
        review.step_results_json
    );
    assert!(
        db::ReviewConformanceRepo::review_conformance(&*db, &execution.id)
            .await
            .expect("conformance loads")
            .is_none()
    );

    // Duplicate terminal delivery is inert: the checks do not run again.
    let started = std::time::Instant::now();
    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("duplicate delivery is inert");
    assert!(started.elapsed() < std::time::Duration::from_secs(1));
}

#[tokio::test]
async fn reviewer_conformance_recovers_deleted_worktree_before_checks() {
    let (db, service, task, execution, repo_dir) = seed_admitted_review_with(
        r#"{"retry_budgets":{"execution":3,"review":3},"review":{"ci_steps":["test -f README.md"]}}"#,
    ).await;
    let workspace_root = TempDir::new().unwrap();
    let service = service.with_workspace_root(workspace_root.path().to_path_buf());
    let workspace = WorkspaceRepo::get_by_id(&*db, execution.workspace_id.as_deref().unwrap())
        .await
        .unwrap()
        .unwrap();
    let worktree = ::workspace::WorkspaceManager::new(workspace_root.path().to_path_buf())
        .create_worktree_named(repo_dir.path().to_str().unwrap(), &task.id, "forge", "main")
        .await
        .unwrap();
    sqlx::query("UPDATE workspace SET worktree_path = ?, branch = ? WHERE id = ?")
        .bind(worktree.to_string_lossy().as_ref())
        .bind(::workspace::task_branch_name(&task.id))
        .bind(&workspace.id)
        .execute(db.pool())
        .await
        .unwrap();
    let sha = git::get_current_sha(&worktree).await.unwrap();
    std::fs::remove_dir_all(&worktree).unwrap();
    run_git(repo_dir.path(), &["worktree", "prune"]);

    complete_review_with(
        &db,
        &service,
        &execution,
        r#"{"result":"pass","reason":"acceptance satisfied"}"#,
    )
    .await;
    let review = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(review.status, ReviewStatus::Passed);
    let details: api_types::ReviewDetails =
        serde_json::from_str(&review.step_results_json).unwrap();
    assert_eq!(
        details.conformance.status,
        api_types::ConformanceStatus::Passed
    );
    assert_eq!(git::get_current_sha(&worktree).await.unwrap(), sha);
    let recovered = WorkspaceRepo::get_by_task_id(&*db, &task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.id, workspace.id);
    assert_eq!(recovered.branch, ::workspace::task_branch_name(&task.id));
}

#[tokio::test]
async fn a_reply_without_a_result_uses_bounded_reviewer_execution_retry() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
    // A reply with no result block is unusable: Forge cannot tell pass from
    // fail, so neither the coder nor the Task's authority may act on it.
    complete_review_with(
        &db,
        &service,
        &execution,
        "I looked at the change and it seems mostly fine.",
    )
    .await;

    let review = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load")
        .into_iter()
        .next()
        .expect("review exists");
    assert_eq!(
        review.status,
        ReviewStatus::Running,
        "an unusable reply keeps the review on the bounded reviewer retry path"
    );
    assert!(review.finished_at.is_none());
    let details: api_types::ReviewDetails =
        serde_json::from_str(&review.step_results_json).expect("review details parse");
    let details_json: serde_json::Value =
        serde_json::from_str(&review.step_results_json).expect("review details json parses");
    assert_eq!(
        details.conformance.status,
        api_types::ConformanceStatus::Unverified
    );
    assert!(details.conformance.assessment.is_none());
    assert_eq!(
        details_json["execution_retry"]["execution_id"],
        execution.id
    );
    assert_eq!(details_json["execution_retry"]["status"], "scheduled");

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(
        current.status,
        crate::workflow::default_states::REVIEW,
        "an unusable reply must not route the coder into remediation"
    );
    assert!(current.blocked_json.is_none());
    assert!(current.error_annotation.is_none());
    let metadata: serde_json::Value =
        serde_json::from_str(current.metadata_json.as_deref().unwrap_or("{}"))
            .expect("metadata parses");
    assert_eq!(
        db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
            .await
            .unwrap(),
        1
    );
    assert!(metadata.get("deferred_dispatch").is_some());

    let completed_execution = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(
        completed_execution.resume_policy,
        Some(db::ResumePolicy::Auto)
    );
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER,
        )
        .await
        .expect("coder executions load"),
        1,
        "unverified reviewer evidence must never dispatch coder remediation \
         (the one coder execution is the seeded review candidate)"
    );
}

#[tokio::test]
async fn a_blocked_review_parks_the_task_for_its_owner_without_the_coder() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
    // Live case: the reviewer had no `tsc` or `tauri` and a read-only cargo
    // home. Neither the coder nor another reviewer can fix that.
    complete_review_with(
        &db,
        &service,
        &execution,
        "`npm run build` exits 127 (`tsc: not found`).\n\n\
         {\"result\": \"blocked\", \"reason\": \"tsc and tauri are not installed\"}",
    )
    .await;

    let review = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load")
        .into_iter()
        .next()
        .expect("review exists");
    assert_eq!(review.status, ReviewStatus::Failed);
    let details: api_types::ReviewDetails =
        serde_json::from_str(&review.step_results_json).expect("review details parse");
    assert_eq!(
        details.conformance.status,
        api_types::ConformanceStatus::Blocked
    );
    let assessment = details.conformance.assessment.expect("review is kept");
    assert_eq!(
        assessment.report,
        "`npm run build` exits 127 (`tsc: not found`)."
    );

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(
        current.status,
        crate::workflow::default_states::REVIEW,
        "a blocked review must not route the coder into remediation"
    );
    let annotation: api_types::TaskBlockingAnnotation = serde_json::from_str(
        current
            .error_annotation
            .as_deref()
            .expect("the Task is parked for its owner"),
    )
    .expect("annotation parses");
    assert_eq!(
        annotation.annotation_type,
        api_types::FailureKind::ReviewBlocked
    );
    assert!(annotation
        .message
        .as_deref()
        .is_some_and(|message| message.contains("tsc and tauri are not installed")));
    assert!(serde_json::to_value(&annotation)
        .unwrap()
        .get("recovery_actions")
        .is_none());
    assert!(serde_json::to_value(&annotation)
        .unwrap()
        .get("recovery_actions")
        .is_none());

    let missing_reason = service
        .test_apply_action(
            task.id.clone(),
            api_types::TaskAction::Approve {
                reason: None,
                override_checks: Some(true),
            },
            None,
            None,
        )
        .await
        .expect_err("manual pass requires a reason");
    assert!(missing_reason.to_string().contains("reason"));

    let recovered = service
        .test_apply_action(
            task.id.clone(),
            api_types::TaskAction::Approve {
                reason: None,
                override_checks: Some(true),
            },
            Some("Provider check was verified manually".to_owned()),
            None,
        )
        .await
        .expect("manual review pass succeeds");
    assert_ne!(recovered.status, crate::workflow::default_states::REVIEW);
    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews reload");
    assert_eq!(reviews.len(), 2);
    assert_eq!(reviews[0].id, review.id);
    assert_eq!(reviews[0].status, ReviewStatus::Failed);
    assert_eq!(reviews[0].step_results_json, review.step_results_json);
    assert_eq!(reviews[1].status, ReviewStatus::Passed);
    let override_details: serde_json::Value =
        serde_json::from_str(&reviews[1].step_results_json).expect("override details parse");
    assert_eq!(
        override_details["manual_override"]["reason"],
        "Provider check was verified manually"
    );
    assert_eq!(override_details["manual_override"]["actor_type"], "user");
    assert_eq!(
        override_details["manual_override"]["source_review_id"],
        review.id
    );

    let comments = TaskCommentRepo::list_comments(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 100,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Asc,
        },
    )
    .await
    .expect("comments reload");
    assert!(comments.items.iter().any(|comment| {
        comment
            .content
            .contains("Review passed manually (attempt 2): Provider check was verified manually")
    }));
    let transitions = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .expect("transitions reload");
    let manual_transition = transitions
        .iter()
        .find(|transition| transition.from_state == crate::workflow::default_states::REVIEW)
        .expect("manual pass transition is audited");
    assert_eq!(
        manual_transition.trigger_reason,
        "Provider check was verified manually"
    );
    assert!(manual_transition.triggered_by.starts_with("user:"));
}
