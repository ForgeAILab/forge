//! Review admission repository tests.
use super::*;

#[tokio::test]
async fn reviewer_execution_admission_binds_latest_review_candidate() {
    let db = sqlite_db().await;
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    sqlx::query("UPDATE agent_identity SET max_concurrent_tasks = ?, updated_at = ? WHERE id = ?")
        .bind(2_i64)
        .bind(now_rfc3339())
        .bind(&agent_id)
        .execute(db.pool())
        .await
        .expect("agent capacity updates");
    let task_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "review".to_owned(),
        "Reviewer admission candidate",
    )
    .await;
    let task = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let assignment = TaskRoleAssignmentRepo::assign(
        &db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            role_name: "reviewer".to_owned(),
            assignee_type: Some(crate::AssigneeKind::Agent),
            assignee_id: Some(agent_id.clone()),
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("reviewer assignment creates");
    let first_workspace = seed_workspace_for_task(&db, &task_id, &repo_id).await;
    let now = now_rfc3339();
    let make_execution =
        |id: String, status: ExecutionStatus, workspace_id: Option<String>| CreateExecution {
            id,
            task_id: task_id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "reviewer".to_owned(),
            status,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id,
            created_at: now.clone(),
            updated_at: now.clone(),
        };
    let first_parent_id = new_uuid_v4();
    let mut first_parent =
        make_execution(first_parent_id.clone(), ExecutionStatus::Completed, None);
    first_parent.role = "executor".to_owned();
    ExecutionRepo::create(&db, first_parent)
        .await
        .expect("first review parent creates");
    let first_review = ReviewRepo::create(
        &db,
        crate::CreateReview {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            execution_id: first_parent_id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: "[]".to_owned(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("first review creates");
    let make_admission = |parent_execution_id: Option<String>| ExecutionAdmission {
        purpose: None,
        expected_queued_recovery_id: None,
        expected_project_version: None,
        expected_task_version: task.version,
        expected_task_status: task.status.clone(),
        expected_effective_role: Some("reviewer".to_owned()),
        expected_agent_max_concurrent_tasks: Some(2),
        expected_agent_version: Some(1),
        expected_reviewer_parent_execution_id: parent_execution_id,
        expected_latest_review_candidate_execution_id: Some(first_parent_id.clone()),
        expected_reviewer_id: Some(first_review.id.clone()),
        expected_reviewer_attempt_number: Some(first_review.attempt_number),
        expected_reviewer_status: Some(first_review.status.to_string()),
        expected_reviewer_updated_at: Some(first_review.updated_at.clone()),
        expected_reviewer_execution_id: first_review.reviewer_execution_id.clone(),
        expected_auditor_execution_id: first_review.auditor_execution_id.clone(),
        expected_assignment_id: Some(assignment.id.clone()),
        expected_assignment_updated_at: Some(assignment.updated_at.clone()),
        expected_workflow_definition: Some("{}".to_owned()),
    };
    let first_id = new_uuid_v4();
    let first_lease = ClaimExecutionLease {
        execution_id: first_id.clone(),
        expected_version: 1,
        owner: "embedded:review-admission-1".to_owned(),
        lease_expires_at: "2099-01-01T00:00:30Z".to_owned(),
        hard_deadline_at: Some("2099-01-01T01:00:00Z".to_owned()),
        now: now.clone(),
    };
    ExecutionRepo::create_with_lease_and_admission(
        &db,
        {
            let mut execution = make_execution(
                first_id.clone(),
                ExecutionStatus::Running,
                Some(first_workspace.clone()),
            );
            execution.parent_execution_id = Some(first_parent_id.clone());
            execution
        },
        first_lease,
        Some(make_admission(Some(first_parent_id.clone()))),
    )
    .await
    .expect("first reviewer admission succeeds");
    sqlx::query("UPDATE execution SET status = 'completed', updated_at = ? WHERE id = ?")
        .bind(now_rfc3339())
        .bind(&first_id)
        .execute(db.pool())
        .await
        .expect("first reviewer execution settles");
    let bound_first_id: String =
        sqlx::query_scalar("SELECT reviewer_execution_id FROM review WHERE id = ?")
            .bind(&first_review.id)
            .fetch_one(db.pool())
            .await
            .expect("first reviewer binding persists");
    assert_eq!(bound_first_id, first_id, "first reviewer binds atomically");

    // A terminal reviewer may be replaced, but the admission must carry the
    // exact binding it observed so a stale retry cannot steal a live slot.
    let replacement_id = new_uuid_v4();
    let replacement_lease = ClaimExecutionLease {
        execution_id: replacement_id.clone(),
        expected_version: 1,
        owner: "embedded:review-admission-replacement".to_owned(),
        lease_expires_at: "2099-01-01T00:00:30Z".to_owned(),
        hard_deadline_at: Some("2099-01-01T01:00:00Z".to_owned()),
        now: now.clone(),
    };
    let mut replacement_admission = make_admission(Some(first_parent_id.clone()));
    replacement_admission.expected_reviewer_execution_id = Some(first_id.clone());
    let mut replacement_execution = make_execution(
        replacement_id.clone(),
        ExecutionStatus::Running,
        Some(first_workspace.clone()),
    );
    replacement_execution.parent_execution_id = Some(first_parent_id.clone());
    ExecutionRepo::create_with_lease_and_admission(
        &db,
        replacement_execution,
        replacement_lease,
        Some(replacement_admission),
    )
    .await
    .expect("terminal reviewer replacement succeeds");
    let rebound_id: String =
        sqlx::query_scalar("SELECT reviewer_execution_id FROM review WHERE id = ?")
            .bind(&first_review.id)
            .fetch_one(db.pool())
            .await
            .expect("replacement reviewer binding persists");
    assert_eq!(rebound_id, replacement_id);
    sqlx::query("UPDATE execution SET status = 'completed', updated_at = ? WHERE id = ?")
        .bind(now_rfc3339())
        .bind(&replacement_id)
        .execute(db.pool())
        .await
        .expect("replacement reviewer execution settles");

    let auditor_id = new_uuid_v4();
    let auditor_lease = ClaimExecutionLease {
        execution_id: auditor_id.clone(),
        expected_version: 1,
        owner: "embedded:review-admission-auditor".to_owned(),
        lease_expires_at: "2099-01-01T00:00:30Z".to_owned(),
        hard_deadline_at: Some("2099-01-01T01:00:00Z".to_owned()),
        now: now.clone(),
    };
    let mut auditor_execution = make_execution(
        auditor_id.clone(),
        ExecutionStatus::Running,
        Some(first_workspace.clone()),
    );
    auditor_execution.role = "auditor".to_owned();
    auditor_execution.parent_execution_id = Some(first_parent_id.clone());
    let mut auditor_admission = make_admission(Some(first_parent_id.clone()));
    auditor_admission.expected_reviewer_execution_id = Some(replacement_id.clone());
    auditor_admission.expected_auditor_execution_id = None;
    ExecutionRepo::create_with_lease_and_admission(
        &db,
        auditor_execution,
        auditor_lease,
        Some(auditor_admission),
    )
    .await
    .expect("first auditor binding succeeds");
    let bound_auditor_id: String =
        sqlx::query_scalar("SELECT auditor_execution_id FROM review WHERE id = ?")
            .bind(&first_review.id)
            .fetch_one(db.pool())
            .await
            .expect("first auditor binding persists");
    assert_eq!(bound_auditor_id, auditor_id);

    // A live auditor remains the owner; the failed insert must roll back its
    // execution row instead of stealing the Review binding.
    let live_replacement_id = new_uuid_v4();
    let mut live_replacement =
        make_execution(live_replacement_id.clone(), ExecutionStatus::Running, None);
    live_replacement.role = "auditor".to_owned();
    live_replacement.parent_execution_id = Some(first_parent_id.clone());
    let live_result = ExecutionRepo::create_with_lease_and_admission(
        &db,
        live_replacement,
        ClaimExecutionLease {
            execution_id: live_replacement_id.clone(),
            expected_version: 1,
            owner: "embedded:review-admission-auditor-live-retry".to_owned(),
            lease_expires_at: "2099-01-01T00:00:30Z".to_owned(),
            hard_deadline_at: Some("2099-01-01T01:00:00Z".to_owned()),
            now: now.clone(),
        },
        Some({
            let mut admission = make_admission(Some(first_parent_id.clone()));
            admission.expected_reviewer_execution_id = Some(replacement_id.clone());
            admission.expected_auditor_execution_id = Some(auditor_id.clone());
            admission
        }),
    )
    .await;
    assert!(matches!(
        live_result,
        Err(DbError::VersionConflict) | Err(DbError::ExecutionAlreadyRunning { .. })
    ));
    assert!(ExecutionRepo::get_by_id(&db, &live_replacement_id)
        .await
        .expect("live retry lookup succeeds")
        .is_none());

    sqlx::query("UPDATE execution SET status = 'completed', updated_at = ? WHERE id = ?")
        .bind(now_rfc3339())
        .bind(&auditor_id)
        .execute(db.pool())
        .await
        .expect("auditor execution settles");
    let auditor_retry_id = new_uuid_v4();
    let mut auditor_retry = make_execution(
        auditor_retry_id.clone(),
        ExecutionStatus::Running,
        Some(first_workspace.clone()),
    );
    auditor_retry.role = "auditor".to_owned();
    auditor_retry.parent_execution_id = Some(first_parent_id.clone());
    let mut auditor_retry_admission = make_admission(Some(first_parent_id.clone()));
    auditor_retry_admission.expected_reviewer_execution_id = Some(replacement_id);
    auditor_retry_admission.expected_auditor_execution_id = Some(auditor_id);
    ExecutionRepo::create_with_lease_and_admission(
        &db,
        auditor_retry,
        ClaimExecutionLease {
            execution_id: auditor_retry_id.clone(),
            expected_version: 1,
            owner: "embedded:review-admission-auditor-retry".to_owned(),
            lease_expires_at: "2099-01-01T00:00:30Z".to_owned(),
            hard_deadline_at: Some("2099-01-01T01:00:00Z".to_owned()),
            now: now.clone(),
        },
        Some(auditor_retry_admission),
    )
    .await
    .expect("terminal auditor replacement succeeds");
    let rebound_auditor_id: String =
        sqlx::query_scalar("SELECT auditor_execution_id FROM review WHERE id = ?")
            .bind(&first_review.id)
            .fetch_one(db.pool())
            .await
            .expect("replacement auditor binding persists");
    assert_eq!(rebound_auditor_id, auditor_retry_id);
    sqlx::query("UPDATE execution SET status = 'completed', updated_at = ? WHERE id = ?")
        .bind(now_rfc3339())
        .bind(&auditor_retry_id)
        .execute(db.pool())
        .await
        .expect("auditor retry settles");

    let second_parent_id = new_uuid_v4();
    let mut second_parent =
        make_execution(second_parent_id.clone(), ExecutionStatus::Completed, None);
    second_parent.role = "executor".to_owned();
    ExecutionRepo::create(&db, second_parent)
        .await
        .expect("second review parent creates");
    let second_review = ReviewRepo::create(
        &db,
        crate::CreateReview {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            execution_id: second_parent_id.clone(),
            attempt_number: 2,
            status: ReviewStatus::Failed,
            step_results_json: "[]".to_owned(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("second review creates");
    let terminal_id = new_uuid_v4();
    let mut terminal_execution = make_execution(
        terminal_id.clone(),
        ExecutionStatus::Running,
        Some(first_workspace.clone()),
    );
    terminal_execution.parent_execution_id = Some(second_parent_id.clone());
    let mut terminal_admission = make_admission(Some(second_parent_id.clone()));
    terminal_admission.expected_latest_review_candidate_execution_id =
        Some(second_parent_id.clone());
    terminal_admission.expected_reviewer_id = Some(second_review.id.clone());
    terminal_admission.expected_reviewer_attempt_number = Some(second_review.attempt_number);
    terminal_admission.expected_reviewer_status = Some(second_review.status.to_string());
    terminal_admission.expected_reviewer_updated_at = Some(second_review.updated_at.clone());
    terminal_admission.expected_reviewer_execution_id = second_review.reviewer_execution_id;
    terminal_admission.expected_auditor_execution_id = second_review.auditor_execution_id;
    let terminal_result = ExecutionRepo::create_with_lease_and_admission(
        &db,
        terminal_execution,
        ClaimExecutionLease {
            execution_id: terminal_id.clone(),
            expected_version: 1,
            owner: "embedded:review-admission-terminal-review".to_owned(),
            lease_expires_at: "2099-01-01T00:00:30Z".to_owned(),
            hard_deadline_at: Some("2099-01-01T01:00:00Z".to_owned()),
            now: now.clone(),
        },
        Some(terminal_admission),
    )
    .await;
    assert!(matches!(terminal_result, Err(DbError::VersionConflict)));
    assert!(ExecutionRepo::get_by_id(&db, &terminal_id)
        .await
        .expect("terminal Review execution lookup succeeds")
        .is_none());
    let stale_id = new_uuid_v4();
    let stale_lease = ClaimExecutionLease {
        execution_id: stale_id.clone(),
        expected_version: 1,
        owner: "embedded:review-admission-2".to_owned(),
        lease_expires_at: "2099-01-01T00:00:30Z".to_owned(),
        hard_deadline_at: Some("2099-01-01T01:00:00Z".to_owned()),
        now: now.clone(),
    };
    let mut stale_execution =
        make_execution(stale_id, ExecutionStatus::Running, Some(first_workspace));
    stale_execution.parent_execution_id = Some(first_parent_id.clone());
    let stale = ExecutionRepo::create_with_lease_and_admission(
        &db,
        stale_execution,
        stale_lease,
        Some(make_admission(Some(first_parent_id.clone()))),
    )
    .await;
    assert!(matches!(stale, Err(DbError::VersionConflict)));

    // A stale Task must not make a failed implementation reviewable: the
    // candidate status is checked in the same transaction before the child
    // reviewer execution, lease, and Review row can be inserted.
    let failed_candidate_id = new_uuid_v4();
    let mut failed_candidate =
        make_execution(failed_candidate_id.clone(), ExecutionStatus::Failed, None);
    failed_candidate.role = "executor".to_owned();
    failed_candidate.created_at = "2099-01-01T00:00:00Z".to_owned();
    failed_candidate.updated_at = failed_candidate.created_at.clone();
    ExecutionRepo::create(&db, failed_candidate)
        .await
        .expect("failed candidate creates");
    let failed_review_id = new_uuid_v4();
    let failed_review = ReviewRepo::create(
        &db,
        crate::CreateReview {
            id: failed_review_id.clone(),
            task_id: task_id.clone(),
            execution_id: failed_candidate_id.clone(),
            attempt_number: 3,
            status: ReviewStatus::Running,
            step_results_json: "[]".to_owned(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("failed candidate Review creates");
    let failed_binding_id = new_uuid_v4();
    let mut failed_binding_execution =
        make_execution(failed_binding_id.clone(), ExecutionStatus::Running, None);
    failed_binding_execution.parent_execution_id = Some(failed_candidate_id.clone());
    let mut failed_binding_admission = make_admission(Some(failed_candidate_id.clone()));
    failed_binding_admission.expected_reviewer_id = Some(failed_review.id.clone());
    failed_binding_admission.expected_latest_review_candidate_execution_id =
        Some(failed_candidate_id.clone());
    failed_binding_admission.expected_reviewer_attempt_number = Some(failed_review.attempt_number);
    failed_binding_admission.expected_reviewer_status = Some(failed_review.status.to_string());
    failed_binding_admission.expected_reviewer_updated_at = Some(failed_review.updated_at.clone());
    let failed_binding = ExecutionRepo::create_with_lease_and_admission(
        &db,
        failed_binding_execution,
        ClaimExecutionLease {
            execution_id: failed_binding_id.clone(),
            expected_version: 1,
            owner: "embedded:failed-candidate-binding".to_owned(),
            lease_expires_at: "2099-01-01T00:00:30Z".to_owned(),
            hard_deadline_at: Some("2099-01-01T01:00:00Z".to_owned()),
            now: now.clone(),
        },
        Some(failed_binding_admission),
    )
    .await;
    assert!(matches!(failed_binding, Err(DbError::VersionConflict)));
    assert!(ExecutionRepo::get_by_id(&db, &failed_binding_id)
        .await
        .expect("failed candidate binding lookup succeeds")
        .is_none());
    let current = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task lookup succeeds")
        .expect("task exists");
    let failed_attempt = ReviewRepo::create_with_task_authority(
        &db,
        crate::CreateReview {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            execution_id: failed_candidate_id.clone(),
            attempt_number: 0,
            status: ReviewStatus::Running,
            step_results_json: "[]".to_owned(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
        current.version,
        &current.status,
        None,
        None,
        Some(&failed_candidate_id),
    )
    .await;
    assert!(matches!(failed_attempt, Err(DbError::VersionConflict)));
    assert!(ReviewRepo::get_by_id(&db, &failed_review_id)
        .await
        .expect("failed candidate Review lookup succeeds")
        .is_some());
}

#[tokio::test]
async fn review_attempt_accepts_completed_direct_child_candidate_but_rejects_unrelated_task() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let root_id = seed_task(
        &db,
        &project_id,
        None,
        "in_progress".to_owned(),
        "coordination review root",
    )
    .await;
    let child_id = new_uuid_v4();
    TaskRepo::create(
        &db,
        CreateTask {
            id: child_id.clone(),
            project_id: project_id.clone(),
            parent_task_id: Some(root_id.clone()),
            subtask_order: Some(0),
            assignee_type: None,
            assignee_id: None,
            title: "completed child candidate".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "in_progress".to_owned(),
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
    .expect("child task creates");
    let unrelated_task_id = seed_task(
        &db,
        &project_id,
        None,
        "in_progress".to_owned(),
        "unrelated candidate task",
    )
    .await;
    let now = now_rfc3339();
    let make_candidate = |id: String, task_id: &str| CreateExecution {
        id,
        task_id: task_id.to_owned(),
        agent_id: None,
        role: "executor".to_owned(),
        status: ExecutionStatus::Completed,
        stop_reason: None,
        stopped_by: None,
        resume_policy: None,
        stopped_at: None,
        parent_execution_id: None,
        agent_session_id: None,
        agent_message_id: None,
        last_activity_at: None,
        summary: None,
        logs_path: None,
        before_sha: None,
        after_sha: None,
        error: None,
        executor_config_snapshot_json: None,
        workspace_id: None,
        created_at: now.clone(),
        updated_at: now.clone(),
    };
    // A coordination root can retain an older implementation execution, but
    // once non-deleted direct children exist the child execution set is the
    // authoritative candidate scope.
    let root_history_id = new_uuid_v4();
    ExecutionRepo::create(&db, make_candidate(root_history_id, &root_id))
        .await
        .expect("root history candidate creates");
    let child_candidate_id = new_uuid_v4();
    ExecutionRepo::create(&db, make_candidate(child_candidate_id.clone(), &child_id))
        .await
        .expect("completed child candidate creates");
    let root = TaskRepo::get_by_id(&db, &root_id, false)
        .await
        .expect("root lookup succeeds")
        .expect("root exists");
    let attempt = |candidate: &str| CreateReview {
        id: new_uuid_v4(),
        task_id: root_id.clone(),
        execution_id: candidate.to_owned(),
        attempt_number: 0,
        status: ReviewStatus::Running,
        step_results_json: "[]".to_owned(),
        started_at: now.clone(),
        created_at: now.clone(),
        updated_at: now.clone(),
    };
    let review = ReviewRepo::create_with_task_authority(
        &db,
        attempt(&child_candidate_id),
        root.version,
        &root.status,
        None,
        None,
        Some(&child_candidate_id),
    )
    .await
    .expect("root review accepts direct child candidate");
    assert_eq!(review.task_id, root_id);
    assert_eq!(review.execution_id, child_candidate_id);

    let unrelated_candidate_id = new_uuid_v4();
    ExecutionRepo::create(
        &db,
        make_candidate(unrelated_candidate_id.clone(), &unrelated_task_id),
    )
    .await
    .expect("unrelated completed candidate creates");
    let root = TaskRepo::get_by_id(&db, &root_id, false)
        .await
        .expect("root lookup succeeds")
        .expect("root exists");
    let unrelated_result = ReviewRepo::create_with_task_authority(
        &db,
        attempt(&unrelated_candidate_id),
        root.version,
        &root.status,
        None,
        None,
        Some(&unrelated_candidate_id),
    )
    .await;
    assert!(matches!(unrelated_result, Err(DbError::VersionConflict)));
    assert_eq!(
        ReviewRepo::list_by_task(&db, &root_id).await.unwrap().len(),
        1
    );
}

#[tokio::test]
async fn review_source_accepts_direct_child_execution_for_coordination_root() {
    let db = sqlite_db().await;
    let (project_id, repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let root_id = seed_task(
        &db,
        &project_id,
        None,
        "review".to_owned(),
        "review source root",
    )
    .await;
    let child_id = new_uuid_v4();
    TaskRepo::create(
        &db,
        CreateTask {
            id: child_id.clone(),
            project_id: project_id.clone(),
            parent_task_id: Some(root_id.clone()),
            subtask_order: Some(0),
            assignee_type: None,
            assignee_id: None,
            title: "review source child".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "in_progress".to_owned(),
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
    .expect("review source child creates");
    let workspace_id = seed_workspace_for_task(&db, &child_id, &repo_id).await;
    let candidate_id = new_uuid_v4();
    let now = now_rfc3339();
    ExecutionRepo::create(
        &db,
        CreateExecution {
            id: candidate_id.clone(),
            task_id: child_id,
            agent_id: None,
            role: "executor".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace_id),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("direct child candidate creates");

    let explicit = ReviewConformanceRepo::review_source(&db, &root_id, Some(&candidate_id))
        .await
        .expect("explicit direct child source loads");
    assert_eq!(explicit["task_id"], root_id);
    assert_eq!(explicit["repo_id"], repo_id);
    let implicit = ReviewConformanceRepo::review_source(&db, &root_id, None)
        .await
        .expect("latest direct child source loads");
    assert_eq!(implicit["repo_id"], repo_id);
}

#[tokio::test]
async fn review_authority_rejects_stale_candidate_when_newer_running_candidate_exists() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        None,
        "review".to_owned(),
        "review candidate authority",
    )
    .await;
    let task = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let project = ProjectRepo::get_by_id(&db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    let make_candidate = |id: String, status: ExecutionStatus, created_at: &str| CreateExecution {
        id,
        task_id: task_id.clone(),
        agent_id: None,
        role: "executor".to_owned(),
        status,
        stop_reason: None,
        stopped_by: None,
        resume_policy: None,
        stopped_at: None,
        parent_execution_id: None,
        agent_session_id: None,
        agent_message_id: None,
        last_activity_at: None,
        summary: None,
        logs_path: None,
        before_sha: None,
        after_sha: None,
        error: None,
        executor_config_snapshot_json: None,
        workspace_id: None,
        created_at: created_at.to_owned(),
        updated_at: created_at.to_owned(),
    };
    let old_candidate_id = new_uuid_v4();
    ExecutionRepo::create(
        &db,
        make_candidate(
            old_candidate_id.clone(),
            ExecutionStatus::Completed,
            "2020-01-01T00:00:00Z",
        ),
    )
    .await
    .expect("completed candidate creates");
    let now = now_rfc3339();
    ReviewRepo::create_with_task_authority(
        &db,
        CreateReview {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            execution_id: old_candidate_id.clone(),
            attempt_number: 0,
            status: ReviewStatus::Running,
            step_results_json: "[]".to_owned(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        task.version,
        task.status.as_str(),
        Some(project.version),
        Some(project.workflow_definition.as_str()),
        Some(&old_candidate_id),
    )
    .await
    .expect("review creates for completed candidate");

    // Failed/cancelled remediation executions are not review candidates. A
    // retry may still review the last completed implementation, while a
    // newer running execution below must continue to fence that stale retry.
    for (status, created_at) in [
        (ExecutionStatus::Failed, "2050-01-01T00:00:00Z"),
        (ExecutionStatus::Cancelled, "2051-01-01T00:00:00Z"),
    ] {
        ExecutionRepo::create(&db, make_candidate(new_uuid_v4(), status, created_at))
            .await
            .expect("terminal remediation candidate creates");
    }
    let retry_review = ReviewRepo::create_with_task_authority(
        &db,
        CreateReview {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            execution_id: old_candidate_id.clone(),
            attempt_number: 0,
            status: ReviewStatus::Running,
            step_results_json: "[]".to_owned(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        task.version,
        task.status.as_str(),
        Some(project.version),
        Some(project.workflow_definition.as_str()),
        Some(&old_candidate_id),
    )
    .await
    .expect("terminal remediation rows do not obscure completed candidate");
    assert_eq!(retry_review.execution_id, old_candidate_id);

    let newer_candidate_id = new_uuid_v4();
    ExecutionRepo::create(
        &db,
        make_candidate(
            newer_candidate_id,
            ExecutionStatus::Running,
            "2099-01-01T00:00:00Z",
        ),
    )
    .await
    .expect("newer running candidate creates");
    let stale_review = ReviewRepo::create_with_task_authority(
        &db,
        CreateReview {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            execution_id: old_candidate_id.clone(),
            attempt_number: 0,
            status: ReviewStatus::Running,
            step_results_json: "[]".to_owned(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
        task.version,
        task.status.as_str(),
        Some(project.version),
        Some(project.workflow_definition.as_str()),
        Some(&old_candidate_id),
    )
    .await;
    assert!(matches!(stale_review, Err(DbError::VersionConflict)));
    assert_eq!(
        ReviewRepo::list_by_task(&db, &task_id).await.unwrap().len(),
        2
    );
}
