//! Workflow admission repository tests.
use super::*;

#[tokio::test]
async fn execution_admission_uses_custom_root_and_inherited_subtask_workflows() {
    let db = sqlite_db().await;
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let custom_workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![StateDefinition {
            name: "in_progress".to_owned(),
            kind: StateKind::Active,
            column: "In Progress".to_owned(),
            display_name: "In Progress".to_owned(),
            role: Some("reviewer".to_owned()),
            hooks: StateHooks::default(),
            cleanup: None,
            canonical_phase: Some(CanonicalPhase::Working),
            gate_config: None,
            dispatch: None,
            triggers: std::collections::BTreeMap::new(),
            config: serde_json::json!({}),
        }],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    let custom_workflow_definition =
        serde_json::to_string(&custom_workflow).expect("custom workflow serializes");
    sqlx::query("UPDATE project SET workflow_definition = ?, updated_at = ? WHERE id = ?")
        .bind(&custom_workflow_definition)
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("custom workflow updates");
    sqlx::query("UPDATE agent_identity SET max_concurrent_tasks = ?, updated_at = ? WHERE id = ?")
        .bind(2_i64)
        .bind(now_rfc3339())
        .bind(&agent_id)
        .execute(db.pool())
        .await
        .expect("agent capacity updates");

    let root_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "in_progress".to_owned(),
        "Custom workflow root",
    )
    .await;
    let child = TaskRepo::create(
        &db,
        crate::CreateTask {
            id: new_uuid_v4(),
            project_id: project_id.clone(),
            parent_task_id: Some(root_id.clone()),
            subtask_order: Some(0),
            assignee_type: None,
            assignee_id: None,
            title: "Inherited workflow child".to_owned(),
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
    .expect("subtask creates");
    let assignment_now = now_rfc3339();
    let reviewer_assignment = TaskRoleAssignmentRepo::assign(
        &db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: root_id.clone(),
            role_name: "reviewer".to_owned(),
            assignee_type: Some(crate::AssigneeKind::Agent),
            assignee_id: Some(agent_id.clone()),
            created_at: assignment_now.clone(),
            updated_at: assignment_now.clone(),
        },
    )
    .await
    .expect("reviewer assignment creates");
    let child_assignment = TaskRoleAssignmentRepo::assign(
        &db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: child.id.clone(),
            role_name: "coder".to_owned(),
            assignee_type: Some(crate::AssigneeKind::Agent),
            assignee_id: Some(agent_id.clone()),
            created_at: assignment_now.clone(),
            updated_at: assignment_now,
        },
    )
    .await
    .expect("child assignment creates");
    let root = TaskRepo::get_by_id(&db, &root_id, false)
        .await
        .expect("root loads")
        .expect("root exists");
    let root_workspace = seed_workspace_for_task(&db, &root_id, &repo_id).await;
    let child_workspace = seed_workspace_for_task(&db, &child.id, &repo_id).await;
    let now = now_rfc3339();
    let make_execution =
        |id: String, task_id: &str, role: &str, workspace_id: &str| CreateExecution {
            id,
            task_id: task_id.to_owned(),
            agent_id: Some(agent_id.clone()),
            role: role.to_owned(),
            status: ExecutionStatus::Running,
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
            workspace_id: Some(workspace_id.to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        };
    let make_lease = |execution_id: String, owner: &str| ClaimExecutionLease {
        execution_id,
        expected_version: 1,
        owner: owner.to_owned(),
        lease_expires_at: "2099-01-01T00:00:30Z".to_owned(),
        hard_deadline_at: Some("2099-01-01T01:00:00Z".to_owned()),
        now: now.clone(),
    };

    let candidate_execution_id = new_uuid_v4();
    ExecutionRepo::create(
        &db,
        CreateExecution {
            id: candidate_execution_id.clone(),
            task_id: child.id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "coder".to_owned(),
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
            workspace_id: Some(child_workspace.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("completed child candidate creates");
    let review = ReviewRepo::create(
        &db,
        CreateReview {
            id: new_uuid_v4(),
            task_id: root.id.clone(),
            execution_id: candidate_execution_id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: "[]".to_owned(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("root Review creates");

    let root_execution_id = new_uuid_v4();
    let mut root_execution_input = make_execution(
        root_execution_id.clone(),
        &root.id,
        "reviewer",
        &root_workspace,
    );
    root_execution_input.parent_execution_id = Some(candidate_execution_id.clone());
    let root_execution = ExecutionRepo::create_with_lease_and_admission(
        &db,
        root_execution_input,
        make_lease(root_execution_id, "embedded:custom-root"),
        Some(ExecutionAdmission {
            purpose: None,
            expected_queued_recovery_id: None,
            expected_project_version: None,
            expected_task_version: root.version,
            expected_task_status: root.status.clone(),
            expected_effective_role: Some("reviewer".to_owned()),
            expected_agent_max_concurrent_tasks: Some(2),
            expected_agent_version: Some(1),
            expected_reviewer_parent_execution_id: Some(candidate_execution_id.clone()),
            expected_latest_review_candidate_execution_id: Some(candidate_execution_id),
            expected_reviewer_id: Some(review.id),
            expected_reviewer_attempt_number: Some(review.attempt_number),
            expected_reviewer_status: Some(review.status.to_string()),
            expected_reviewer_updated_at: Some(review.updated_at),
            expected_reviewer_execution_id: review.reviewer_execution_id,
            expected_auditor_execution_id: review.auditor_execution_id,
            expected_assignment_id: Some(reviewer_assignment.id),
            expected_assignment_updated_at: Some(reviewer_assignment.updated_at),
            expected_workflow_definition: Some(custom_workflow_definition.clone()),
        }),
    )
    .await
    .expect("custom root role admission succeeds");
    assert_eq!(root_execution.role, "reviewer");

    let child_execution_id = new_uuid_v4();
    let child_execution = ExecutionRepo::create_with_lease_and_admission(
        &db,
        make_execution(
            child_execution_id.clone(),
            &child.id,
            "coder",
            &child_workspace,
        ),
        make_lease(child_execution_id, "embedded:inherited-child"),
        Some(ExecutionAdmission {
            purpose: None,
            expected_queued_recovery_id: None,
            expected_project_version: None,
            expected_task_version: child.version,
            expected_task_status: child.status.clone(),
            expected_effective_role: Some("coder".to_owned()),
            expected_agent_max_concurrent_tasks: Some(2),
            expected_agent_version: Some(1),
            expected_reviewer_parent_execution_id: None,
            expected_latest_review_candidate_execution_id: None,
            expected_reviewer_id: None,
            expected_reviewer_attempt_number: None,
            expected_reviewer_status: None,
            expected_reviewer_updated_at: None,
            expected_reviewer_execution_id: None,
            expected_auditor_execution_id: None,
            expected_assignment_id: Some(child_assignment.id),
            expected_assignment_updated_at: Some(child_assignment.updated_at),
            expected_workflow_definition: None,
        }),
    )
    .await
    .expect("inherited subtask role admission succeeds");
    assert_eq!(child_execution.role, "coder");
}

#[tokio::test]
async fn execution_admission_rechecks_assignment_and_dependency_edges() {
    let db = sqlite_db().await;
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "in_progress".to_owned(),
        "Admission edge rechecks",
    )
    .await;
    let task = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let assignment = TaskRoleAssignmentRepo::get_by_task_and_role(&db, &task_id, "coder")
        .await
        .expect("task assignment loads")
        .expect("task assignment exists");
    let workspace_id = seed_workspace_for_task(&db, &task_id, &repo_id).await;
    let now = now_rfc3339();
    let make_execution = |id: String| CreateExecution {
        id,
        task_id: task_id.clone(),
        agent_id: Some(agent_id.clone()),
        role: "coder".to_owned(),
        status: ExecutionStatus::Running,
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
        workspace_id: Some(workspace_id.clone()),
        created_at: now.clone(),
        updated_at: now.clone(),
    };
    let make_lease = |execution_id: String| ClaimExecutionLease {
        execution_id,
        expected_version: 1,
        owner: "embedded:admission-edge-test".to_owned(),
        lease_expires_at: "2099-01-01T00:00:30Z".to_owned(),
        hard_deadline_at: Some("2099-01-01T01:00:00Z".to_owned()),
        now: now.clone(),
    };
    let stale_admission = ExecutionAdmission {
        purpose: None,
        expected_queued_recovery_id: None,
        expected_project_version: None,
        expected_task_version: task.version,
        expected_task_status: task.status.clone(),
        expected_effective_role: Some("coder".to_owned()),
        expected_agent_max_concurrent_tasks: Some(1),
        expected_agent_version: Some(1),
        expected_reviewer_parent_execution_id: None,
        expected_latest_review_candidate_execution_id: None,
        expected_reviewer_id: None,
        expected_reviewer_attempt_number: None,
        expected_reviewer_status: None,
        expected_reviewer_updated_at: None,
        expected_reviewer_execution_id: None,
        expected_auditor_execution_id: None,
        expected_assignment_id: Some(assignment.id.clone()),
        expected_assignment_updated_at: Some(assignment.updated_at.clone()),
        expected_workflow_definition: Some("{}".to_owned()),
    };

    TaskRoleAssignmentRepo::assign(
        &db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            role_name: "coder".to_owned(),
            assignee_type: Some(crate::AssigneeKind::Agent),
            assignee_id: Some(agent_id.clone()),
            created_at: "2099-01-01T00:00:00Z".to_owned(),
            updated_at: "2099-01-01T00:00:01Z".to_owned(),
        },
    )
    .await
    .expect("assignment changes");
    let changed_id = new_uuid_v4();
    let changed_result = ExecutionRepo::create_with_lease_and_admission(
        &db,
        make_execution(changed_id.clone()),
        make_lease(changed_id),
        Some(stale_admission),
    )
    .await;
    assert!(
        matches!(changed_result, Err(DbError::VersionConflict)),
        "unexpected assignment race result: {changed_result:?}"
    );

    let current_assignment = TaskRoleAssignmentRepo::get_by_task_and_role(&db, &task_id, "coder")
        .await
        .expect("changed assignment loads")
        .expect("changed assignment exists");
    let prerequisite_id = seed_task(
        &db,
        &project_id,
        None,
        "todo".to_owned(),
        "New prerequisite",
    )
    .await;
    TaskDependencyRepo::add_dependency(&db, &task_id, &prerequisite_id, &now)
        .await
        .expect("dependency adds");
    let dependency_id = new_uuid_v4();
    let dependency_result = ExecutionRepo::create_with_lease_and_admission(
        &db,
        make_execution(dependency_id.clone()),
        make_lease(dependency_id),
        Some(ExecutionAdmission {
            purpose: None,
            expected_queued_recovery_id: None,
            expected_project_version: None,
            expected_task_version: task.version,
            expected_task_status: task.status.clone(),
            expected_effective_role: Some("coder".to_owned()),
            expected_agent_max_concurrent_tasks: Some(1),
            expected_agent_version: Some(1),
            expected_reviewer_parent_execution_id: None,
            expected_latest_review_candidate_execution_id: None,
            expected_reviewer_id: None,
            expected_reviewer_attempt_number: None,
            expected_reviewer_status: None,
            expected_reviewer_updated_at: None,
            expected_reviewer_execution_id: None,
            expected_auditor_execution_id: None,
            expected_assignment_id: Some(current_assignment.id),
            expected_assignment_updated_at: Some(current_assignment.updated_at),
            expected_workflow_definition: Some("{}".to_owned()),
        }),
    )
    .await;
    assert!(matches!(dependency_result, Err(DbError::DependencyGate)));

    let interactive_id = new_uuid_v4();
    let mut interactive_execution = make_execution(interactive_id.clone());
    interactive_execution.role = "interactive".to_owned();
    let interactive_result = ExecutionRepo::create_with_lease_and_admission(
        &db,
        interactive_execution,
        make_lease(interactive_id),
        Some(ExecutionAdmission {
            purpose: None,
            expected_queued_recovery_id: None,
            expected_project_version: None,
            expected_task_version: task.version,
            expected_task_status: task.status.clone(),
            expected_effective_role: None,
            expected_agent_max_concurrent_tasks: Some(1),
            expected_agent_version: Some(1),
            expected_reviewer_parent_execution_id: None,
            expected_latest_review_candidate_execution_id: None,
            expected_reviewer_id: None,
            expected_reviewer_attempt_number: None,
            expected_reviewer_status: None,
            expected_reviewer_updated_at: None,
            expected_reviewer_execution_id: None,
            expected_auditor_execution_id: None,
            expected_assignment_id: None,
            expected_assignment_updated_at: None,
            expected_workflow_definition: Some("{}".to_owned()),
        }),
    )
    .await;
    assert!(
        matches!(interactive_result, Err(DbError::DependencyGate)),
        "interactive admission result: {interactive_result:?}"
    );
}

#[tokio::test]
async fn concurrent_execution_admission_has_one_winner_and_typed_loser() {
    // `sqlite::memory:` uses a single pooled connection, which serializes
    // callers before they can contend on BEGIN IMMEDIATE. Use a temporary
    // file-backed pool so both spawned admissions can reach the transaction
    // boundary concurrently and let SQLite choose the winner.
    let database_path = std::env::temp_dir().join(format!(
        "forge-execution-admission-{}.sqlite",
        new_uuid_v4()
    ));
    let database_url = format!("sqlite://{}", database_path.display());
    let pool = create_sqlite_pool(&database_url)
        .await
        .expect("concurrent admission pool creates");
    run_migrations(&pool)
        .await
        .expect("concurrent admission migrations run");
    let db = std::sync::Arc::new(SqliteDb::new(pool));
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "in_progress".to_owned(),
        "Concurrent execution admission",
    )
    .await;
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let assignment = TaskRoleAssignmentRepo::get_by_task_and_role(&*db, &task_id, "coder")
        .await
        .expect("task assignment loads")
        .expect("task assignment exists");
    let workspace_id = seed_workspace_for_task(&db, &task_id, &repo_id).await;
    let now = "2099-01-01T00:00:00Z".to_owned();
    let make_execution = |id: String, role: &str| CreateExecution {
        id,
        task_id: task_id.clone(),
        agent_id: Some(agent_id.clone()),
        role: role.to_owned(),
        status: ExecutionStatus::Running,
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
        workspace_id: Some(workspace_id.clone()),
        created_at: now.clone(),
        updated_at: now.clone(),
    };
    let make_lease = |execution_id: String, owner: &str| ClaimExecutionLease {
        execution_id,
        expected_version: 1,
        owner: owner.to_owned(),
        lease_expires_at: "2099-01-01T00:00:30Z".to_owned(),
        hard_deadline_at: Some("2099-01-01T01:00:00Z".to_owned()),
        now: now.clone(),
    };
    let first_id = new_uuid_v4();
    let second_id = new_uuid_v4();
    let first_execution = make_execution(first_id.clone(), "coder");
    let second_execution = make_execution(second_id.clone(), "coder");
    let first_lease = make_lease(first_id.clone(), "embedded:concurrent-1");
    let second_lease = make_lease(second_id.clone(), "embedded:concurrent-2");
    let first_db = std::sync::Arc::clone(&db);
    let second_db = std::sync::Arc::clone(&db);
    let first_admission = ExecutionAdmission {
        purpose: None,
        expected_queued_recovery_id: None,
        expected_project_version: None,
        expected_task_version: task.version,
        expected_task_status: task.status.clone(),
        expected_effective_role: Some("coder".to_owned()),
        expected_agent_max_concurrent_tasks: Some(1),
        expected_agent_version: Some(1),
        expected_reviewer_parent_execution_id: None,
        expected_latest_review_candidate_execution_id: None,
        expected_reviewer_id: None,
        expected_reviewer_attempt_number: None,
        expected_reviewer_status: None,
        expected_reviewer_updated_at: None,
        expected_reviewer_execution_id: None,
        expected_auditor_execution_id: None,
        expected_assignment_id: Some(assignment.id),
        expected_assignment_updated_at: Some(assignment.updated_at),
        expected_workflow_definition: Some("{}".to_owned()),
    };
    let second_admission = first_admission.clone();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(3));
    let first_barrier = std::sync::Arc::clone(&barrier);
    let second_barrier = std::sync::Arc::clone(&barrier);
    let first = tokio::spawn(async move {
        first_barrier.wait().await;
        ExecutionRepo::create_with_lease_and_admission(
            &*first_db,
            first_execution,
            first_lease,
            Some(first_admission),
        )
        .await
    });
    let second = tokio::spawn(async move {
        second_barrier.wait().await;
        ExecutionRepo::create_with_lease_and_admission(
            &*second_db,
            second_execution,
            second_lease,
            Some(second_admission),
        )
        .await
    });
    barrier.wait().await;
    let first_result = first.await.expect("first admission task joins");
    let second_result = second.await.expect("second admission task joins");
    let (winner, loser) = match (first_result, second_result) {
        (Ok(winner), Err(error)) => (winner, error),
        (Err(error), Ok(winner)) => (winner, error),
        (first, second) => panic!("expected one winner and one loser: {first:?}, {second:?}"),
    };
    assert!(matches!(
        loser,
        DbError::ExecutionAlreadyRunning {
            scope,
            execution_id,
        } if scope == "repository" && execution_id == winner.id
    ));
    drop(db);
    std::fs::remove_file(&database_path).expect("concurrent admission database removes");
    std::fs::remove_file(format!("{}-wal", database_path.display())).ok();
    std::fs::remove_file(format!("{}-shm", database_path.display())).ok();
}

#[tokio::test]
async fn concurrent_execution_admission_respects_agent_capacity() {
    let database_path = std::env::temp_dir().join(format!(
        "forge-execution-agent-capacity-{}.sqlite",
        new_uuid_v4()
    ));
    let database_url = format!("sqlite://{}", database_path.display());
    let pool = create_sqlite_pool(&database_url)
        .await
        .expect("capacity pool creates");
    run_migrations(&pool)
        .await
        .expect("capacity migrations run");
    let db = std::sync::Arc::new(SqliteDb::new(pool));
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let first_task_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "in_progress".to_owned(),
        "First capacity task",
    )
    .await;
    let second_task_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "in_progress".to_owned(),
        "Second capacity task",
    )
    .await;
    let first_task = TaskRepo::get_by_id(&*db, &first_task_id, false)
        .await
        .expect("first task loads")
        .expect("first task exists");
    let second_task = TaskRepo::get_by_id(&*db, &second_task_id, false)
        .await
        .expect("second task loads")
        .expect("second task exists");
    let first_assignment =
        TaskRoleAssignmentRepo::get_by_task_and_role(&*db, &first_task_id, "coder")
            .await
            .expect("first assignment loads")
            .expect("first assignment exists");
    let second_assignment =
        TaskRoleAssignmentRepo::get_by_task_and_role(&*db, &second_task_id, "coder")
            .await
            .expect("second assignment loads")
            .expect("second assignment exists");
    let first_workspace = seed_workspace_for_task(&db, &first_task_id, &repo_id).await;
    let second_workspace = seed_workspace_for_task(&db, &second_task_id, &repo_id).await;
    let now = "2099-01-01T00:00:00Z".to_owned();
    let make_execution = |id: String, task_id: &str, workspace_id: &str| CreateExecution {
        id,
        task_id: task_id.to_owned(),
        agent_id: Some(agent_id.clone()),
        role: "coder".to_owned(),
        status: ExecutionStatus::Running,
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
        workspace_id: Some(workspace_id.to_owned()),
        created_at: now.clone(),
        updated_at: now.clone(),
    };
    let make_lease = |execution_id: String, owner: &str| ClaimExecutionLease {
        execution_id,
        expected_version: 1,
        owner: owner.to_owned(),
        lease_expires_at: "2099-01-01T00:00:30Z".to_owned(),
        hard_deadline_at: Some("2099-01-01T01:00:00Z".to_owned()),
        now: now.clone(),
    };
    let first_id = new_uuid_v4();
    let second_id = new_uuid_v4();
    let first_db = std::sync::Arc::clone(&db);
    let second_db = std::sync::Arc::clone(&db);
    let first_admission = ExecutionAdmission {
        purpose: None,
        expected_queued_recovery_id: None,
        expected_project_version: None,
        expected_task_version: first_task.version,
        expected_task_status: first_task.status.clone(),
        expected_effective_role: Some("coder".to_owned()),
        expected_agent_max_concurrent_tasks: Some(1),
        expected_agent_version: Some(1),
        expected_reviewer_parent_execution_id: None,
        expected_latest_review_candidate_execution_id: None,
        expected_reviewer_id: None,
        expected_reviewer_attempt_number: None,
        expected_reviewer_status: None,
        expected_reviewer_updated_at: None,
        expected_reviewer_execution_id: None,
        expected_auditor_execution_id: None,
        expected_assignment_id: Some(first_assignment.id),
        expected_assignment_updated_at: Some(first_assignment.updated_at),
        expected_workflow_definition: Some("{}".to_owned()),
    };
    let second_admission = ExecutionAdmission {
        purpose: None,
        expected_queued_recovery_id: None,
        expected_project_version: None,
        expected_task_version: second_task.version,
        expected_task_status: second_task.status.clone(),
        expected_effective_role: Some("coder".to_owned()),
        expected_agent_max_concurrent_tasks: Some(1),
        expected_agent_version: Some(1),
        expected_reviewer_parent_execution_id: None,
        expected_latest_review_candidate_execution_id: None,
        expected_reviewer_id: None,
        expected_reviewer_attempt_number: None,
        expected_reviewer_status: None,
        expected_reviewer_updated_at: None,
        expected_reviewer_execution_id: None,
        expected_auditor_execution_id: None,
        expected_assignment_id: Some(second_assignment.id),
        expected_assignment_updated_at: Some(second_assignment.updated_at),
        expected_workflow_definition: Some("{}".to_owned()),
    };
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(3));
    let first_execution = make_execution(first_id.clone(), &first_task_id, &first_workspace);
    let second_execution = make_execution(second_id.clone(), &second_task_id, &second_workspace);
    let first_lease = make_lease(first_id.clone(), "embedded:capacity-1");
    let second_lease = make_lease(second_id.clone(), "embedded:capacity-2");
    let first_barrier = std::sync::Arc::clone(&barrier);
    let second_barrier = std::sync::Arc::clone(&barrier);
    let first = tokio::spawn(async move {
        first_barrier.wait().await;
        ExecutionRepo::create_with_lease_and_admission(
            &*first_db,
            first_execution,
            first_lease,
            Some(first_admission),
        )
        .await
    });
    let second = tokio::spawn(async move {
        second_barrier.wait().await;
        ExecutionRepo::create_with_lease_and_admission(
            &*second_db,
            second_execution,
            second_lease,
            Some(second_admission),
        )
        .await
    });
    barrier.wait().await;
    let first_result = first.await.expect("first capacity task joins");
    let second_result = second.await.expect("second capacity task joins");
    let successes = usize::from(first_result.is_ok()) + usize::from(second_result.is_ok());
    assert_eq!(successes, 1, "exactly one agent slot may be admitted");
    let errors = [first_result.err(), second_result.err()];
    assert!(errors
        .iter()
        .flatten()
        .any(|error| matches!(error, DbError::AgentAtCapacity)));
    drop(db);
    std::fs::remove_file(&database_path).expect("capacity database removes");
    std::fs::remove_file(format!("{}-wal", database_path.display())).ok();
    std::fs::remove_file(format!("{}-shm", database_path.display())).ok();
}

#[tokio::test]
async fn review_integration_lock_is_rejected_after_project_pause() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        None,
        "merging".to_owned(),
        "Paused integration guard",
    )
    .await;
    ProjectRepo::set_paused_at(&db, &project_id, Some(now_rfc3339()))
        .await
        .expect("project pauses");

    let result = ReviewConformanceRepo::lock_review_integration(&db, &task_id).await;

    assert!(matches!(
        result,
        Err(DbError::ProjectPaused { project_id: blocked }) if blocked == project_id
    ));
}
