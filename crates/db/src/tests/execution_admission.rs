//! Execution admission repository tests.
use super::*;

#[tokio::test]
async fn sqlite_execution_role_auditor_round_trips() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, _repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "review".to_string(),
        "Audit me",
    )
    .await;
    let execution_id = new_uuid_v4();

    let created = ExecutionRepo::create(
        &db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task_id.clone(),
            agent_id: Some(agent_id),
            role: "auditor".to_string(),
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
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("auditor execution creates");

    assert_eq!(created.role, "auditor".to_string());

    let loaded = ExecutionRepo::get_by_id(&db, &execution_id)
        .await
        .expect("auditor execution loads")
        .expect("auditor execution exists");
    assert_eq!(loaded.role, "auditor".to_string());
    assert_eq!(
        ExecutionRepo::list_by_task(&db, &task_id, page(10))
            .await
            .expect("executions list")
            .items
            .first()
            .map(|execution| &execution.role),
        Some(&"auditor".to_string())
    );
}

#[tokio::test]
async fn running_execution_creation_is_rejected_after_project_pause() {
    let db = sqlite_db().await;
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "in_progress".to_owned(),
        "Paused execution guard",
    )
    .await;
    let workspace_id = seed_workspace_for_task(&db, &task_id, &repo_id).await;
    let now = "2026-09-11T19:25:46Z";
    sqlx::query("UPDATE project SET paused_at = ?, updated_at = ? WHERE id = ?")
        .bind(now)
        .bind(now)
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project pauses");
    let execution_id = new_uuid_v4();

    let result = ExecutionRepo::create_with_lease(
        &db,
        CreateExecution {
            id: execution_id.clone(),
            task_id,
            agent_id: Some(agent_id),
            role: "executor".to_owned(),
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
            workspace_id: Some(workspace_id),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
        ClaimExecutionLease {
            execution_id: execution_id.clone(),
            expected_version: 1,
            owner: "embedded:paused-test".to_owned(),
            lease_expires_at: "2026-09-11T19:26:16Z".to_owned(),
            hard_deadline_at: Some("2026-09-11T20:25:46Z".to_owned()),
            now: now.to_owned(),
        },
    )
    .await;

    assert!(matches!(
        result,
        Err(DbError::ProjectPaused { project_id: blocked }) if blocked == project_id
    ));
    assert!(ExecutionRepo::get_by_id(&db, &execution_id)
        .await
        .expect("execution lookup succeeds")
        .is_none());
}

#[tokio::test]
async fn execution_admission_reports_occupant_and_rejects_stale_task_snapshot() {
    let db = sqlite_db().await;
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "in_progress".to_owned(),
        "Execution admission CAS",
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
        owner: "embedded:admission-test".to_owned(),
        lease_expires_at: "2099-01-01T00:00:30Z".to_owned(),
        hard_deadline_at: Some("2099-01-01T01:00:00Z".to_owned()),
        now: now.clone(),
    };
    let occupant = new_uuid_v4();
    ExecutionRepo::create_with_lease(
        &db,
        make_execution(occupant.clone()),
        make_lease(occupant.clone()),
    )
    .await
    .expect("occupying execution creates");

    let competing = new_uuid_v4();
    let result = ExecutionRepo::create_with_lease_and_admission(
        &db,
        make_execution(competing.clone()),
        make_lease(competing.clone()),
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
            expected_assignment_id: Some(assignment.id.clone()),
            expected_assignment_updated_at: Some(assignment.updated_at.clone()),
            expected_workflow_definition: Some("{}".to_owned()),
        }),
    )
    .await;
    assert!(
        matches!(
            result,
            Err(DbError::ExecutionAlreadyRunning {
                ref scope,
                ref execution_id,
            }) if scope == "repository" && execution_id == &occupant
        ),
        "execution admission result: {result:?}"
    );

    let interactive_id = new_uuid_v4();
    let mut interactive_execution = make_execution(interactive_id.clone());
    interactive_execution.role = "interactive".to_owned();
    let interactive_result =
        ExecutionRepo::create_with_lease(&db, interactive_execution, make_lease(interactive_id))
            .await;
    assert!(matches!(
        interactive_result,
        Err(DbError::ExecutionAlreadyRunning {
            scope,
            execution_id,
        }) if scope == "repository" && execution_id == occupant
    ));

    TaskRepo::update(
        &db,
        UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: Some("changed after dispatch read".to_owned()),
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("task version advances");
    let stale = new_uuid_v4();
    let stale_result = ExecutionRepo::create_with_lease_and_admission(
        &db,
        make_execution(stale.clone()),
        make_lease(stale),
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
            expected_assignment_id: Some(assignment.id),
            expected_assignment_updated_at: Some(assignment.updated_at),
            expected_workflow_definition: Some("{}".to_owned()),
        }),
    )
    .await;
    assert!(matches!(stale_result, Err(DbError::VersionConflict)));
    assert!(ExecutionRepo::get_by_id(&db, &competing)
        .await
        .expect("competing execution lookup succeeds")
        .is_none());

    let interactive_task_id = seed_task(
        &db,
        &project_id,
        None,
        "in_progress".to_owned(),
        "Interactive execution admission",
    )
    .await;
    let interactive_workspace_id =
        seed_workspace_for_task(&db, &interactive_task_id, &repo_id).await;
    let make_interactive_slot_execution = |id: String, role: &str| CreateExecution {
        id,
        task_id: interactive_task_id.clone(),
        agent_id: None,
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
        workspace_id: Some(interactive_workspace_id.clone()),
        created_at: now.clone(),
        updated_at: now.clone(),
    };
    let interactive_occupant = new_uuid_v4();
    ExecutionRepo::create_with_lease(
        &db,
        make_interactive_slot_execution(interactive_occupant.clone(), "interactive"),
        make_lease(interactive_occupant.clone()),
    )
    .await
    .expect("interactive occupying execution creates");
    let interactive_competing = new_uuid_v4();
    let interactive_slot_result = ExecutionRepo::create_with_lease(
        &db,
        make_interactive_slot_execution(interactive_competing.clone(), "coder"),
        make_lease(interactive_competing.clone()),
    )
    .await;
    assert!(matches!(
        interactive_slot_result,
        Err(DbError::ExecutionAlreadyRunning {
            scope,
            execution_id,
        }) if scope == "interactive" && execution_id == interactive_occupant
    ));
}

#[tokio::test]
async fn execution_admission_ignores_plan_claim_from_prior_same_state_entry() {
    let db = sqlite_db().await;
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "in_progress".to_owned(),
        "State-entry-scoped plan publication claim",
    )
    .await;
    let workspace_id = seed_workspace_for_task(&db, &task_id, &repo_id).await;
    let assignment = TaskRoleAssignmentRepo::get_by_task_and_role(&db, &task_id, "coder")
        .await
        .expect("task assignment loads")
        .expect("task assignment exists");
    let agent = AgentRepo::get_by_id(&db, &agent_id)
        .await
        .expect("agent loads")
        .expect("agent exists");
    let project = ProjectRepo::get_by_id(&db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");

    let claimed_entry_id = new_uuid_v4();
    TransitionLogRepo::insert(
        &db,
        CreateTransitionLog {
            id: claimed_entry_id.clone(),
            task_id: task_id.clone(),
            from_state: "review".to_owned(),
            to_state: "in_progress".to_owned(),
            trigger_name: Some("reject".to_owned()),
            triggered_by: "user:test".to_owned(),
            bridge: Default::default(),
            trigger_reason: "first entry".to_owned(),
            hook_results_json: None,
            rejection: true,
            created_at: "2026-01-01T00:00:00Z".to_owned(),
        },
    )
    .await
    .expect("claimed state entry records");
    sqlx::query(
        "UPDATE task
         SET metadata_json = ?, version = version + 1, updated_at = ?
         WHERE id = ?",
    )
    .bind(
        serde_json::json!({
            "plan_publication_claim": {
                "execution_id": new_uuid_v4(),
                "state": "in_progress",
                "state_entry_token": claimed_entry_id,
                "project_version": project.version,
            }
        })
        .to_string(),
    )
    .bind(now_rfc3339())
    .bind(&task_id)
    .execute(db.pool())
    .await
    .expect("plan publication claim records");
    let task = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

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
        owner: "embedded:state-entry-claim-test".to_owned(),
        lease_expires_at: "2099-01-01T00:00:30Z".to_owned(),
        hard_deadline_at: Some("2099-01-01T01:00:00Z".to_owned()),
        now: now.clone(),
    };
    let make_admission = || ExecutionAdmission {
        purpose: None,
        expected_queued_recovery_id: None,
        expected_project_version: Some(project.version),
        expected_task_version: task.version,
        expected_task_status: task.status.clone(),
        expected_effective_role: Some("coder".to_owned()),
        expected_agent_max_concurrent_tasks: Some(agent.max_concurrent_tasks),
        expected_agent_version: Some(agent.version),
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
        expected_workflow_definition: Some(project.workflow_definition.clone()),
    };

    let blocked_id = new_uuid_v4();
    let blocked = ExecutionRepo::create_with_lease_and_admission(
        &db,
        make_execution(blocked_id.clone()),
        make_lease(blocked_id.clone()),
        Some(make_admission()),
    )
    .await;
    assert!(
        matches!(blocked, Err(DbError::VersionConflict)),
        "a claim for the current state entry must block admission: {blocked:?}"
    );
    assert!(ExecutionRepo::get_by_id(&db, &blocked_id)
        .await
        .expect("blocked execution lookup succeeds")
        .is_none());

    TransitionLogRepo::insert(
        &db,
        CreateTransitionLog {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            from_state: "review".to_owned(),
            to_state: "in_progress".to_owned(),
            trigger_name: Some("reject".to_owned()),
            triggered_by: "user:test".to_owned(),
            bridge: Default::default(),
            trigger_reason: "later re-entry".to_owned(),
            hook_results_json: None,
            rejection: true,
            created_at: "2026-01-01T00:00:01Z".to_owned(),
        },
    )
    .await
    .expect("later same-state entry records");

    let admitted_id = new_uuid_v4();
    let admitted = ExecutionRepo::create_with_lease_and_admission(
        &db,
        make_execution(admitted_id.clone()),
        make_lease(admitted_id.clone()),
        Some(make_admission()),
    )
    .await
    .expect("claim from an older entry no longer blocks admission");
    assert_eq!(admitted.id, admitted_id);
}

#[tokio::test]
async fn recovery_execution_conflict_uses_canonical_slot_scope() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let now = now_rfc3339();

    let make_running = |task_id: String, role: &str| CreateExecution {
        id: new_uuid_v4(),
        task_id,
        agent_id: None,
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
        workspace_id: None,
        created_at: now.clone(),
        updated_at: now.clone(),
    };

    let repository_task_id = seed_task(
        &db,
        &project_id,
        None,
        "in_progress".to_owned(),
        "Repository recovery scope",
    )
    .await;
    let repository_execution = make_running(repository_task_id.clone(), "coder");
    let repository_execution_id = repository_execution.id.clone();
    ExecutionRepo::create(&db, repository_execution)
        .await
        .expect("repository execution creates");
    let repository_task = TaskRepo::get_by_id(&db, &repository_task_id, false)
        .await
        .expect("repository task loads")
        .expect("repository task exists");
    let repository_result = TaskRepo::update_recovery_metadata_if_no_running_execution(
        &db,
        &repository_task.id,
        repository_task.version,
        Some("stale repository recovery".to_owned()),
        None,
        None,
        &now,
        None,
        Vec::new(),
        Vec::new(),
        None,
    )
    .await;
    assert!(matches!(
        repository_result,
        Err(DbError::ExecutionAlreadyRunning {
            scope,
            execution_id,
        }) if scope == "repository" && execution_id == repository_execution_id
    ));

    let interactive_task_id = seed_task(
        &db,
        &project_id,
        None,
        "in_progress".to_owned(),
        "Interactive recovery scope",
    )
    .await;
    let interactive_execution = make_running(interactive_task_id.clone(), "interactive");
    let interactive_execution_id = interactive_execution.id.clone();
    ExecutionRepo::create(&db, interactive_execution)
        .await
        .expect("interactive execution creates");
    let interactive_task = TaskRepo::get_by_id(&db, &interactive_task_id, false)
        .await
        .expect("interactive task loads")
        .expect("interactive task exists");
    let interactive_result = TaskRepo::update_recovery_metadata_if_no_running_execution(
        &db,
        &interactive_task.id,
        interactive_task.version,
        Some("stale interactive recovery".to_owned()),
        None,
        None,
        &now,
        None,
        Vec::new(),
        Vec::new(),
        None,
    )
    .await;
    assert!(matches!(
        interactive_result,
        Err(DbError::ExecutionAlreadyRunning {
            scope,
            execution_id,
        }) if scope == "interactive" && execution_id == interactive_execution_id
    ));
}

#[tokio::test]
async fn queued_recovery_metadata_is_versioned_and_consumed_at_execution_admission() {
    let db = sqlite_db().await;
    let project_id = seed_project(&db, "Queued recovery", None).await;
    let task_id = seed_task(&db, &project_id, None, "in_progress".to_owned(), "Recover").await;
    let task = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let task = TaskRepo::update(
        &db,
        UpdateTask {
            id: task_id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(
                serde_json::json!({
                    "type": "recovery_required", "blocking_reason": "crash_recovery",
                    "recovery_actions": ["reexecute"],
                })
                .to_string(),
            )),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("interruption persists");
    let mutations = vec![
        crate::TaskMetadataMutation::Set {
            key: "queued_recovery".to_owned(),
            value: serde_json::json!({ "id": "queued-intent", "action": "reexecute" }),
        },
        crate::TaskMetadataMutation::Set {
            key: "deferred_dispatch".to_owned(),
            value: serde_json::json!({ "target_state": "in_progress" }),
        },
        crate::TaskMetadataMutation::Set {
            key: "unrelated".to_owned(),
            value: serde_json::json!("preserved"),
        },
    ];
    let queued = TaskRepo::update_recovery_metadata_if_no_running_execution(
        &db,
        &task_id,
        task.version,
        None,
        None,
        None,
        &now_rfc3339(),
        None,
        Vec::new(),
        mutations,
        None,
    )
    .await
    .expect("clear and queue commit together");
    assert_eq!(queued.version, task.version + 1);
    assert!(queued.error_annotation.is_none());
    let metadata: serde_json::Value =
        serde_json::from_str(queued.metadata_json.as_deref().expect("queue metadata"))
            .expect("metadata parses");
    assert!(metadata.get("queued_recovery").is_some());
    assert!(metadata.get("deferred_dispatch").is_some());
    let stale = TaskRepo::update_recovery_metadata_if_no_running_execution(
        &db,
        &task_id,
        task.version,
        task.error_annotation,
        None,
        None,
        &now_rfc3339(),
        None,
        Vec::new(),
        vec![crate::TaskMetadataMutation::Remove {
            key: "queued_recovery".to_owned(),
        }],
        None,
    )
    .await;
    assert!(matches!(stale, Err(DbError::VersionConflict)));
    let now = now_rfc3339();
    let make_execution = || CreateExecution {
        id: new_uuid_v4(),
        task_id: task_id.clone(),
        agent_id: None,
        role: "interactive".to_owned(),
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
        workspace_id: None,
        created_at: now.clone(),
        updated_at: now.clone(),
    };
    let unrelated = ExecutionRepo::create(&db, make_execution())
        .await
        .expect("unrelated execution admits");
    let current = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.metadata_json, queued.metadata_json);
    assert_eq!(current.version, queued.version);
    for (expected_version, queued_recovery_id) in [
        (queued.version - 1, "queued-intent"),
        (queued.version, "newer-intent"),
    ] {
        let result = TaskRepo::restore_queued_recovery(
            &db,
            crate::RestoreQueuedRecovery {
                task_id: task_id.clone(),
                expected_version,
                failed_json: None,
                queued_recovery_id: queued_recovery_id.to_owned(),
                error_annotation: Some("stale blocker".to_owned()),
                blocked_json: None,
                updated_at: now.clone(),
            },
        )
        .await;
        assert!(matches!(result, Err(DbError::VersionConflict)));
    }
    let current = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.metadata_json, queued.metadata_json);
    assert!(current.error_annotation.is_none());
    sqlx::query("UPDATE execution SET status = 'completed' WHERE id = ?")
        .bind(&unrelated.id)
        .execute(db.pool())
        .await
        .unwrap();
    let make_admission = |id: &str| ExecutionAdmission {
        purpose: None,
        expected_queued_recovery_id: Some(id.to_owned()),
        expected_project_version: None,
        expected_task_version: queued.version,
        expected_task_status: queued.status.clone(),
        expected_effective_role: None,
        expected_agent_version: None,
        expected_agent_max_concurrent_tasks: None,
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
        expected_workflow_definition: None,
    };
    let make_lease = |execution_id: String| ClaimExecutionLease {
        execution_id,
        expected_version: 1,
        owner: "embedded:queued-recovery".to_owned(),
        lease_expires_at: "2099-01-01T00:00:30Z".to_owned(),
        hard_deadline_at: None,
        now: now.clone(),
    };
    let stale_execution = make_execution();
    let stale_id = stale_execution.id.clone();
    let result = ExecutionRepo::create_with_lease_and_admission(
        &db,
        stale_execution,
        make_lease(stale_id.clone()),
        Some(make_admission("wrong-intent")),
    )
    .await;
    assert!(matches!(result, Err(DbError::VersionConflict)));
    assert!(ExecutionRepo::get_by_id(&db, &stale_id)
        .await
        .unwrap()
        .is_none());
    let execution = make_execution();
    let lease = make_lease(execution.id.clone());
    ExecutionRepo::create_with_lease_and_admission(
        &db,
        execution,
        lease,
        Some(make_admission("queued-intent")),
    )
    .await
    .expect("own replay admits and consumes queue");
    let admitted = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let metadata: serde_json::Value = serde_json::from_str(
        admitted
            .metadata_json
            .as_deref()
            .expect("remaining metadata"),
    )
    .expect("metadata parses");
    assert!(metadata.get("queued_recovery").is_none());
    assert!(metadata.get("deferred_dispatch").is_none());
    assert_eq!(metadata["unrelated"], "preserved");
}

#[tokio::test]
async fn execution_admission_rejects_agent_profile_reassignment_without_capacity_change() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "in_progress".to_owned(),
        "Agent profile admission CAS",
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
    let agent = AgentRepo::get_by_id(&db, &agent_id)
        .await
        .expect("agent loads")
        .expect("agent exists");
    assert!(
        agent.daemon_id.is_some(),
        "fixture starts with a daemon route"
    );

    let updated_agent = AgentRepo::update(
        &db,
        UpdateAgent {
            id: agent.id.clone(),
            expected_version: agent.version,
            name: None,
            description: None,
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: None,
            config_json: None,
            daemon_id: Some(None),
            max_concurrent_tasks: None,
            heartbeat_interval_seconds: None,
            max_missed_heartbeats: None,
            status: None,
            last_heartbeat_at: None,
            is_default: None,
            paused: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("agent daemon route changes");
    assert!(updated_agent.daemon_id.is_none());
    assert_eq!(
        updated_agent.max_concurrent_tasks,
        agent.max_concurrent_tasks
    );
    assert_eq!(updated_agent.version, agent.version + 1);

    let execution_id = new_uuid_v4();
    let now = "2099-01-01T00:00:00Z";
    let result = ExecutionRepo::create_with_lease_and_admission(
        &db,
        CreateExecution {
            id: execution_id.clone(),
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
            workspace_id: None,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
        ClaimExecutionLease {
            execution_id: execution_id.clone(),
            expected_version: 1,
            owner: "embedded:agent-profile-admission-test".to_owned(),
            lease_expires_at: "2099-01-01T00:00:30Z".to_owned(),
            hard_deadline_at: Some("2099-01-01T01:00:00Z".to_owned()),
            now: now.to_owned(),
        },
        Some(ExecutionAdmission {
            purpose: None,
            expected_queued_recovery_id: None,
            expected_project_version: None,
            expected_task_version: task.version,
            expected_task_status: task.status,
            expected_effective_role: Some("coder".to_owned()),
            expected_agent_version: Some(agent.version),
            expected_agent_max_concurrent_tasks: Some(agent.max_concurrent_tasks),
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
        }),
    )
    .await;
    assert!(matches!(result, Err(DbError::VersionConflict)));
    assert!(ExecutionRepo::get_by_id(&db, &execution_id)
        .await
        .expect("rejected execution lookup succeeds")
        .is_none());
}
