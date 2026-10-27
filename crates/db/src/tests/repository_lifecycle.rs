//! Repository lifecycle repository tests.
use super::*;

#[tokio::test]
async fn sqlite_repositories_create_update_list_and_get_logs() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;

    let project = ProjectRepo::update_at_version(
        &db,
        UpdateProject {
            id: project_id.clone(),
            name: Some("Forge DB".to_owned()),
            settings: None,
            primary_repo_id: None,
            paused_at: None,
            updated_at: now.clone(),
        },
        ProjectRepo::get_by_id(&db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project updates");
    assert_eq!(project.name, "Forge DB");

    let repo = RepoRepo::update(
        &db,
        UpdateRepo {
            id: repo_id.clone(),
            name: None,
            local_path: None,
            remote_url: None,
            default_branch: Some("trunk".to_owned()),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo updates");
    assert_eq!(repo.default_branch, "trunk");

    let skill_id = new_uuid_v4();
    SkillRepo::create(
        &db,
        CreateSkill {
            id: skill_id.clone(),
            project_id: project_id.clone(),
            name: "Rust".to_owned(),
            content: "Use cargo test".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("skill creates");
    let skill = SkillRepo::update(
        &db,
        UpdateSkill {
            id: skill_id,
            name: Some("SQLite".to_owned()),
            content: None,
            updated_at: now.clone(),
        },
    )
    .await
    .expect("skill updates");
    assert_eq!(skill.name, "SQLite");

    let agent = AgentRepo::update(
        &db,
        UpdateAgent {
            id: agent_id.clone(),
            expected_version: 1,
            name: Some("codex".to_owned()),
            description: None,
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            capabilities_json: None,
            config_json: None,
            daemon_id: None,
            max_concurrent_tasks: None,
            heartbeat_interval_seconds: None,
            max_missed_heartbeats: None,
            status: Some(AgentStatus::Busy),
            last_heartbeat_at: Some(Some(now.clone())),
            is_default: None,
            paused: None,
            prompt_template: None,
            updated_at: now.clone(),
        },
    )
    .await
    .expect("agent updates");
    assert_eq!(agent.version, 2);

    let task_id = new_uuid_v4();
    TaskRepo::create(
        &db,
        CreateTask {
            id: task_id.clone(),
            project_id: project_id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Implement repo".to_owned(),
            description: Some("SQLite".to_owned()),
            task_type: "task".to_owned(),
            status: "todo".to_string(),
            is_automation: false,
            priority: 10,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task creates");
    TaskRoleAssignmentRepo::assign(
        &db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            role_name: "coder".to_owned(),
            assignee_type: Some(crate::AssigneeKind::Agent),
            assignee_id: Some(agent_id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("role assignment creates");
    let task = TaskRepo::update(
        &db,
        UpdateTask {
            id: task_id.clone(),
            expected_version: 1,
            title: Some("Implement SQLite repo".to_owned()),
            description: None,
            priority: Some(20),
            merge_config: None,
            plan: Some(Some("Map rows manually".to_owned())),
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task updates");
    assert_eq!(task.version, 2);

    let execution_id = new_uuid_v4();
    ExecutionRepo::create(
        &db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task_id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "executor".to_string(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("initial prompt".to_owned()),
            logs_path: Some("logs/run.jsonl".to_owned()),
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("execution creates");
    let rejected_update = ExecutionRepo::update(
        &db,
        UpdateExecution {
            id: execution_id.clone(),
            status: Some(ExecutionStatus::Completed),
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            agent_session_id: Some(Some("session".to_owned())),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some(Some("done".to_owned())),
            logs_path: None,
            before_sha: None,
            after_sha: Some(Some("abc123".to_owned())),
            error: None,
            executor_config_snapshot_json: None,
            updated_at: now.clone(),
        },
    )
    .await;
    assert!(matches!(rejected_update, Err(DbError::InvalidTransition)));
    let execution = match ExecutionRepo::terminalize(
        &db,
        TerminalizeExecution {
            execution_id: execution_id.clone(),
            expected_version: 1,
            lease_owner: None,
            status: ExecutionStatus::Completed,
            stop_reason: Some(None),
            stopped_by: Some(Some("system".to_owned())),
            stopped_at: Some(Some(now.clone())),
            resume_policy: Some(Some(crate::ResumePolicy::None)),
            agent_session_id: Some(Some("session".to_owned())),
            agent_message_id: Some(None),
            last_activity_at: Some(None),
            last_progress_at: Some(None),
            summary: Some(Some("done".to_owned())),
            logs_path: None,
            before_sha: None,
            after_sha: Some(Some("abc123".to_owned())),
            error: None,
            executor_config_snapshot_json: None,
            updated_at: now.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            correlation_id: None,
            causation_id: None,
            causation_depth: 0,
            lease_disposition: ExecutionLeaseDisposition::Revoke,
        },
    )
    .await
    .expect("terminal execution commits")
    {
        crate::ExecutionTerminalOutcome::Committed { execution, .. } => execution,
        other => panic!("unexpected terminal execution result: {other:?}"),
    };
    assert_eq!(execution.status, ExecutionStatus::Completed);
    assert_eq!(execution.prompt.as_deref(), Some("initial prompt"));
    assert_eq!(execution.summary.as_deref(), Some("done"));
    assert_eq!(
        ExecutionRepo::get_logs_path(&db, &execution_id)
            .await
            .expect("logs path loads"),
        Some("logs/run.jsonl".to_owned())
    );

    // Terminal execution transitions land in the durable ledger at Project
    // scope so Attention can project incidents and wake the Project Agent.
    let completed_event: (String, String) =
        sqlx::query_as("SELECT scope_type, scope_id FROM domain_event WHERE dedupe_key = ?")
            .bind(format!("execution-terminal:{execution_id}:completed"))
            .fetch_one(db.pool())
            .await
            .expect("completed execution event exists");
    assert_eq!(completed_event.0, "project");
    assert_eq!(completed_event.1, project_id.clone());

    let failed_execution_id = new_uuid_v4();
    ExecutionRepo::create(
        &db,
        CreateExecution {
            id: failed_execution_id.clone(),
            task_id: task_id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "executor".to_string(),
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
        },
    )
    .await
    .expect("failing execution creates");
    let failed = ExecutionRepo::terminalize(
        &db,
        TerminalizeExecution {
            execution_id: failed_execution_id.clone(),
            expected_version: 1,
            lease_owner: None,
            status: ExecutionStatus::Failed,
            stop_reason: Some(Some(crate::StopReason::ExecutorFailed)),
            stopped_by: Some(Some("system".to_owned())),
            stopped_at: Some(Some(now.clone())),
            resume_policy: Some(Some(crate::ResumePolicy::Manual)),
            agent_session_id: Some(None),
            agent_message_id: Some(None),
            last_activity_at: Some(None),
            last_progress_at: Some(None),
            summary: Some(None),
            logs_path: Some(None),
            before_sha: Some(None),
            after_sha: Some(None),
            error: Some(Some("executor exploded".to_owned())),
            executor_config_snapshot_json: Some(None),
            updated_at: now.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            correlation_id: None,
            causation_id: None,
            causation_depth: 0,
            lease_disposition: ExecutionLeaseDisposition::Revoke,
        },
    )
    .await
    .expect("execution fails");
    assert!(matches!(
        failed,
        crate::ExecutionTerminalOutcome::Committed { .. }
    ));
    let failed_event: (String, String, String) = sqlx::query_as(
        "SELECT event_type, entity_id, payload_json FROM domain_event WHERE dedupe_key = ?",
    )
    .bind(format!("execution-terminal:{failed_execution_id}:failed"))
    .fetch_one(db.pool())
    .await
    .expect("failed execution event exists");
    assert_eq!(failed_event.0, "execution.failed");
    assert_eq!(failed_event.1, task_id.clone());
    assert!(failed_event.2.contains("executor exploded"));

    let cancelled_execution_id = new_uuid_v4();
    ExecutionRepo::create(
        &db,
        CreateExecution {
            id: cancelled_execution_id.clone(),
            task_id: task_id.clone(),
            agent_id: Some(agent_id.clone()),
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
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("cancelled execution creates");
    let cancelled = ExecutionRepo::terminalize(
        &db,
        TerminalizeExecution {
            execution_id: cancelled_execution_id,
            expected_version: 1,
            lease_owner: None,
            status: ExecutionStatus::Cancelled,
            stop_reason: Some(Some(crate::StopReason::UserCancelled)),
            stopped_by: Some(Some("user".to_owned())),
            stopped_at: Some(Some(now.clone())),
            resume_policy: Some(Some(crate::ResumePolicy::Manual)),
            agent_session_id: Some(None),
            agent_message_id: Some(None),
            last_activity_at: Some(None),
            last_progress_at: Some(None),
            summary: Some(None),
            logs_path: Some(None),
            before_sha: Some(None),
            after_sha: Some(None),
            error: Some(None),
            executor_config_snapshot_json: Some(None),
            updated_at: now.clone(),
            actor_type: "user".to_owned(),
            actor_id: Some("user-1".to_owned()),
            correlation_id: None,
            causation_id: None,
            causation_depth: 0,
            lease_disposition: ExecutionLeaseDisposition::Revoke,
        },
    )
    .await
    .expect("cancelled execution commits");
    match cancelled {
        crate::ExecutionTerminalOutcome::Committed { event, .. } => {
            assert_eq!(event.event_type, "execution.cancelled");
        }
        other => panic!("unexpected cancelled execution result: {other:?}"),
    }

    let review_id = new_uuid_v4();
    ReviewRepo::create(
        &db,
        CreateReview {
            id: review_id.clone(),
            task_id: task_id.clone(),
            execution_id: execution_id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: "[]".to_owned(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("review creates");
    assert_eq!(
        ReviewRepo::next_attempt_number(&db, &task_id)
            .await
            .expect("next attempt loads"),
        2
    );
    let review = ReviewRepo::update_status(
        &db,
        &review_id,
        ReviewStatus::Passed,
        r#"[{"index":0,"exit_code":0}]"#.to_owned(),
        Some(now.clone()),
        &now,
    )
    .await
    .expect("review updates");
    assert_eq!(review.status, ReviewStatus::Passed);
    assert_eq!(review.step_results_json, r#"[{"index":0,"exit_code":0}]"#);
    assert_eq!(review.finished_at, Some(now.clone()));
    let review_events = DomainEventRepo::list_events_after(&db, 0, 100)
        .await
        .expect("review outcome event lists");
    assert!(review_events.iter().any(|event| {
        event.event_type == "review.status_changed"
            && event.entity_id == review_id
            && event.scope_id == task_id
    }));

    let workspace_id = new_uuid_v4();
    let workspace = WorkspaceRepo::create(
        &db,
        CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task_id.clone(),
            repo_id: repo_id.clone(),
            worktree_path: "/tmp/forge/worktrees/task/forge".to_owned(),
            branch: ::workspace::task_branch_name(&task_id),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("workspace creates");
    assert_eq!(workspace.cleanup_after, None);
    let cleanup_after = "2026-04-14T01:00:00Z".to_owned();
    let workspace =
        WorkspaceRepo::set_cleanup_after(&db, &workspace_id, Some(cleanup_after.clone()), &now)
            .await
            .expect("cleanup deadline sets");
    assert_eq!(workspace.cleanup_after, Some(cleanup_after));
    let pending = WorkspaceRepo::list_pending_cleanup(&db, "2026-04-14T02:00:00Z")
        .await
        .expect("pending cleanup lists");
    assert_eq!(pending.len(), 1);
    let workspace = WorkspaceRepo::mark_cleaned(&db, &workspace_id, &now)
        .await
        .expect("workspace marks cleaned");
    assert_eq!(workspace.status, WorkspaceStatus::Cleaned);
    assert_eq!(workspace.cleanup_after, None);

    assert_eq!(
        ProjectRepo::list(&db, page(10)).await.unwrap().total_count,
        Some(1)
    );
    assert_eq!(
        RepoRepo::list_by_project(&db, &project_id, page(10))
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    assert_eq!(
        SkillRepo::list_by_project(&db, &project_id, page(10))
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    assert_eq!(
        AgentRepo::list(
            &db,
            AgentListQuery {
                status: Some(AgentStatus::Busy),
                executor_type: None,
                capabilities: vec!["rust".to_owned()],
                page: page(10),
            },
        )
        .await
        .unwrap()
        .total_count,
        Some(1)
    );
    assert_eq!(
        TaskRepo::list(
            &db,
            TaskListQuery {
                project_id,
                q: None,
                statuses: vec!["todo".to_string()],
                agent_ids: vec![agent_id],
                assignee_types: Vec::new(),
                assignee_ids: Vec::new(),
                priority: Some(20),
                include_archived: false,
                include_cancelled: false,
                include_deleted: false,
                page: page(10),
            },
        )
        .await
        .unwrap()
        .total_count,
        Some(1)
    );
    assert_eq!(
        ExecutionRepo::list_by_task(&db, &task_id, page(10))
            .await
            .unwrap()
            .items
            .len(),
        3
    );
    assert_eq!(
        ReviewRepo::list_by_task(&db, &task_id).await.unwrap().len(),
        1
    );
}

#[tokio::test]
async fn sqlite_repositories_enforce_versions_transitions_claims_and_cursors() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, _repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = new_uuid_v4();
    TaskRepo::create(
        &db,
        CreateTask {
            id: task_id.clone(),
            project_id,
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Claim me".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "todo".to_string(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task creates");
    TaskRoleAssignmentRepo::assign(
        &db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            role_name: "coder".to_owned(),
            assignee_type: Some(crate::AssigneeKind::Agent),
            assignee_id: Some(agent_id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("role assignment creates");

    let bad_update = TaskRepo::update(
        &db,
        UpdateTask {
            id: task_id.clone(),
            expected_version: 99,
            title: Some("stale".to_owned()),
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now.clone(),
        },
    )
    .await;
    assert!(matches!(bad_update, Err(DbError::VersionConflict)));

    let mut tx = crate::begin_immediate(db.pool())
        .await
        .expect("transaction starts");
    let execution_id = new_uuid_v4();
    let claimed = claim_in_test_step(
        &db,
        &mut tx,
        ClaimTask {
            task_id: task_id.clone(),
            assignee_type: "agent".to_owned(),
            assignee_id: Some(agent_id.clone()),
            expected_version: 1,
            source_status: "todo".to_owned(),
            target_status: "in_progress".to_owned(),
            execution_admission: None,
            expected_project_version: None,
            expected_workflow_definition: None,
            execution: CreateExecution {
                id: execution_id.clone(),
                task_id: task_id.clone(),
                agent_id: Some(agent_id.clone()),
                role: "executor".to_string(),
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
            },
            execution_lease: pending_claim_lease(&execution_id, &now),
            claimed_at: now.clone(),
        },
    )
    .await
    .expect("task claims");
    tx.commit().await.expect("claim commits");
    assert_eq!(claimed.task.status, "in_progress".to_string());
    assert_eq!(
        claimed.execution.lease_owner.as_deref(),
        Some(format!("dispatch-pending:{execution_id}").as_str())
    );
    assert_eq!(claimed.execution.execution_version, 2);
    assert!(claimed.execution.lease_expires_at.is_some());
    assert!(claimed.execution.hard_deadline_at.is_some());
    assert_eq!(
        AgentRepo::count_active_assigned_tasks(&db, &agent_id)
            .await
            .unwrap(),
        1
    );

    let invalid_cursor = ProjectRepo::list(
        &db,
        PageRequest {
            cursor: Some("not-base64-json".to_owned()),
            limit: 10,
            include_total: false,
            sort_by: SortBy::Id,
            sort_order: SortOrder::Asc,
        },
    )
    .await;
    assert!(matches!(invalid_cursor, Err(DbError::InvalidCursor)));
}
