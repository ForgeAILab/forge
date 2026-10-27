//! Claim capacity repository tests.
use super::*;

#[tokio::test]
async fn claim_capacity_ignores_assigned_but_not_running_tasks() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let assigned_task_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "in_progress".to_owned(),
        "Assigned without execution",
    )
    .await;
    let claim_task_id = seed_task(
        &db,
        &project_id,
        None,
        "todo".to_owned(),
        "Claim despite assigned work",
    )
    .await;
    let now = now_rfc3339();
    let execution_id = new_uuid_v4();
    let mut transaction = crate::begin_immediate(db.pool())
        .await
        .expect("claim transaction starts");
    let claimed = claim_in_test_step(
        &db,
        &mut transaction,
        ClaimTask {
            task_id: claim_task_id.clone(),
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
                task_id: claim_task_id.clone(),
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
            execution_lease: pending_claim_lease(&execution_id, &now),
            claimed_at: now.clone(),
        },
    )
    .await
    .expect("assigned work without a running execution leaves capacity");
    transaction
        .commit()
        .await
        .expect("claim transaction commits");

    assert_eq!(claimed.task.id, claim_task_id);
    assert_eq!(
        claimed.task,
        TaskRepo::get_by_id(&db, &claim_task_id, false)
            .await
            .expect("claimed Task reloads")
            .expect("claimed Task exists"),
        "claim returns the cache pair stored after entry and execution creation"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution WHERE task_id = ? AND status = 'running'",
        )
        .bind(&assigned_task_id)
        .fetch_one(db.pool())
        .await
        .expect("assigned task execution count reads"),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution WHERE agent_id = ? AND status = 'running'",
        )
        .bind(&agent_id)
        .fetch_one(db.pool())
        .await
        .expect("running execution count reads"),
        1
    );
}

#[tokio::test]
async fn claim_rejects_stale_project_workflow_authority_without_mutation() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(&db, &project_id, None, "todo".to_owned(), "stale claim").await;
    let project = ProjectRepo::get_by_id(&db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    let task = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    ProjectRepo::update_workflow(
        &db,
        &project_id,
        r#"{"stale":true}"#,
        None,
        project.version,
        &now_rfc3339(),
    )
    .await
    .expect("workflow update commits");

    let execution_id = new_uuid_v4();
    let mut tx = crate::begin_immediate(db.pool())
        .await
        .expect("claim transaction starts");
    let result = claim_in_test_step(
        &db,
        &mut tx,
        ClaimTask {
            task_id: task_id.clone(),
            assignee_type: "agent".to_owned(),
            assignee_id: Some(agent_id.clone()),
            expected_version: task.version,
            source_status: task.status.clone(),
            target_status: "in_progress".to_owned(),
            execution_admission: None,
            expected_project_version: Some(project.version),
            expected_workflow_definition: Some(project.workflow_definition.clone()),
            execution: CreateExecution {
                id: execution_id.clone(),
                task_id: task_id.clone(),
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
                workspace_id: None,
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
            execution_lease: pending_claim_lease(&execution_id, &now_rfc3339()),
            claimed_at: now_rfc3339(),
        },
    )
    .await;
    assert!(matches!(result, Err(DbError::VersionConflict)));
    drop(tx);

    let unchanged_task = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert_eq!(unchanged_task.status, task.status);
    assert_eq!(unchanged_task.version, task.version);
    assert!(ExecutionRepo::get_by_id(&db, &execution_id)
        .await
        .expect("execution lookup succeeds")
        .is_none());
}

#[tokio::test]
async fn machine_capacity_rejects_running_execution_at_daemon_limit() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let daemon_id: String = sqlx::query_scalar("SELECT daemon_id FROM agent_current WHERE id = ?")
        .bind(&agent_id)
        .fetch_one(db.pool())
        .await
        .expect("agent daemon loads");
    sqlx::query("UPDATE agent_identity SET max_concurrent_tasks = 2, updated_at = ? WHERE id = ?")
        .bind(now_rfc3339())
        .bind(&agent_id)
        .execute(db.pool())
        .await
        .expect("agent task capacity expands");
    sqlx::query("UPDATE daemon SET max_concurrent_runs = ?, updated_at = ? WHERE id = ?")
        .bind(1_i64)
        .bind(now_rfc3339())
        .bind(&daemon_id)
        .execute(db.pool())
        .await
        .expect("daemon session cap sets");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT max_concurrent_tasks FROM agent_current WHERE id = ?",
        )
        .bind(&agent_id)
        .fetch_one(db.pool())
        .await
        .expect("expanded task capacity reads"),
        2
    );

    let first_task_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "in_progress".to_owned(),
        "Daemon cap first task",
    )
    .await;
    let second_task_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "in_progress".to_owned(),
        "Daemon cap second task",
    )
    .await;
    let now = "2099-01-01T00:00:00Z";
    let make_execution = |id: String, task_id: &str| CreateExecution {
        id,
        task_id: task_id.to_owned(),
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
        created_at: now.to_owned(),
        updated_at: now.to_owned(),
    };
    let make_lease = |execution_id: String| ClaimExecutionLease {
        execution_id,
        expected_version: 1,
        owner: "embedded:daemon-cap-test".to_owned(),
        lease_expires_at: "2099-01-01T00:00:30Z".to_owned(),
        hard_deadline_at: Some("2099-01-01T01:00:00Z".to_owned()),
        now: now.to_owned(),
    };
    let first_id = new_uuid_v4();
    ExecutionRepo::create_with_lease(
        &db,
        make_execution(first_id.clone(), &first_task_id),
        make_lease(first_id),
    )
    .await
    .expect("first daemon session admits");

    let second_id = new_uuid_v4();
    let second_result = ExecutionRepo::create_with_lease(
        &db,
        make_execution(second_id.clone(), &second_task_id),
        make_lease(second_id.clone()),
    )
    .await;
    assert!(matches!(second_result, Err(DbError::MachineAtCapacity)));
    assert!(ExecutionRepo::get_by_id(&db, &second_id)
        .await
        .expect("rejected execution lookup succeeds")
        .is_none());
}

#[tokio::test]
async fn paused_agent_is_rejected_at_running_execution_insert() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "in_progress".to_owned(),
        "Paused agent task",
    )
    .await;
    sqlx::query("UPDATE agent_identity SET paused = 1, updated_at = ? WHERE id = ?")
        .bind(now_rfc3339())
        .bind(&agent_id)
        .execute(db.pool())
        .await
        .expect("agent pauses");

    let execution_id = new_uuid_v4();
    let now = "2099-01-01T00:00:00Z";
    let result = ExecutionRepo::create_with_lease(
        &db,
        CreateExecution {
            id: execution_id.clone(),
            task_id,
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
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
        ClaimExecutionLease {
            execution_id: execution_id.clone(),
            expected_version: 1,
            owner: "embedded:paused-agent-test".to_owned(),
            lease_expires_at: "2099-01-01T00:00:30Z".to_owned(),
            hard_deadline_at: Some("2099-01-01T01:00:00Z".to_owned()),
            now: now.to_owned(),
        },
    )
    .await;
    assert!(matches!(
        result,
        Err(DbError::AgentPaused { agent_id: ref rejected }) if rejected == &agent_id
    ));
    assert!(ExecutionRepo::get_by_id(&db, &execution_id)
        .await
        .expect("rejected execution lookup succeeds")
        .is_none());
}

#[tokio::test]
async fn agent_active_task_count_uses_workflow_state_kinds() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let workflow = serde_json::json!({
        "states": [
            { "name": "todo", "kind": "initial" },
            { "name": "running", "kind": "active" },
            { "name": "waiting_review", "kind": "gate" },
            { "name": "done", "kind": "terminal" }
        ]
    });

    sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
        .bind(workflow.to_string())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("workflow updates");

    seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "running".to_owned(),
        "custom active state",
    )
    .await;
    seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "waiting_review".to_owned(),
        "custom gate state",
    )
    .await;
    seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "done".to_owned(),
        "terminal state",
    )
    .await;

    assert_eq!(
        AgentRepo::count_active_assigned_tasks(&db, &agent_id)
            .await
            .unwrap(),
        2
    );
    let grouped = db
        .agent_list_active_assignments(&[agent_id.clone(), "missing".to_owned()])
        .await
        .unwrap();
    assert_eq!(grouped[&agent_id], 2);
    assert_eq!(grouped["missing"], 0);
}

#[tokio::test]
async fn agent_task_list_uses_execution_history() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let executed_task_id =
        seed_task(&db, &project_id, None, "done".to_owned(), "executed task").await;
    seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "done".to_owned(),
        "assigned only task",
    )
    .await;
    let now = now_rfc3339();
    ExecutionRepo::create(
        &db,
        CreateExecution {
            id: new_uuid_v4(),
            task_id: executed_task_id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "coder".to_owned(),
            status: ExecutionStatus::Completed,
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
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("execution creates");

    let page = TaskRepo::list_by_executing_agent(
        &db,
        AgentTaskListQuery {
            agent_id,
            user_id: "test-user".to_owned(),
            include_archived: false,
            include_cancelled: true,
            include_deleted: false,
            page: PageRequest {
                cursor: None,
                limit: 10,
                include_total: true,
                sort_by: SortBy::UpdatedAt,
                sort_order: SortOrder::Desc,
            },
        },
    )
    .await
    .expect("agent task list loads");

    assert_eq!(page.total_count, Some(1));
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].id, executed_task_id);
}

#[tokio::test]
async fn task_claim_rejects_active_entry_barrier() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, _repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = new_uuid_v4();
    let task = TaskRepo::create(
        &db,
        CreateTask {
            id: task_id.clone(),
            project_id,
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Barrier claim".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "todo".to_owned(),
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
    let task = TaskRepo::set_entry_barrier(
        &db,
        &task.id,
        task.version,
        Some(
            r#"{"state":"todo","status":"running","started_at":"2026-04-28T00:00:00Z"}"#.to_owned(),
        ),
        &now,
    )
    .await
    .expect("barrier sets");

    let mut tx = crate::begin_immediate(db.pool())
        .await
        .expect("transaction starts");
    let execution_id = new_uuid_v4();
    let result = claim_in_test_step(
        &db,
        &mut tx,
        ClaimTask {
            task_id: task_id.clone(),
            assignee_type: "agent".to_owned(),
            assignee_id: Some(agent_id.clone()),
            expected_version: task.version,
            source_status: "todo".to_owned(),
            target_status: "in_progress".to_owned(),
            execution_admission: None,
            expected_project_version: None,
            expected_workflow_definition: None,
            execution: CreateExecution {
                id: execution_id.clone(),
                task_id,
                agent_id: Some(agent_id),
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
                updated_at: now,
            },
            execution_lease: pending_claim_lease(&execution_id, "2026-08-21T00:00:00Z"),
            claimed_at: now_rfc3339(),
        },
    )
    .await;

    assert!(matches!(result, Err(DbError::InvalidTransition)));
}

#[tokio::test]
async fn machine_capacity_ready_start_is_rechecked_after_chat_arrives() {
    let db = sqlite_db().await;
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let daemon_id: String = sqlx::query_scalar("SELECT daemon_id FROM agent_current WHERE id = ?")
        .bind(&agent_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE daemon SET machine_id = ? WHERE id = ?")
        .bind("db-test-host")
        .bind(&daemon_id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE agent_identity SET max_concurrent_tasks = 10 WHERE id = ?")
        .bind(&agent_id)
        .execute(db.pool())
        .await
        .unwrap();
    db.server_run_cap.set(Some(1), 1, "db-test-host");
    let task_a = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "in_progress".to_owned(),
        "A",
    )
    .await;
    let task_b = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "in_progress".to_owned(),
        "B",
    )
    .await;
    let ws_a = seed_workspace_for_task(&db, &task_a, &repo_id).await;
    let ws_b = seed_workspace_for_task(&db, &task_b, &repo_id).await;
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(db.pool())
        .await
        .unwrap();
    for (ws, task, state) in [(&ws_a, &task_a, "ready"), (&ws_b, &task_b, "reserved")] {
        sqlx::query("DELETE FROM workspace_placement WHERE workspace_id = ?")
            .bind(ws)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO workspace_placement (id, workspace_id, task_id, agent_id, owner_kind, daemon_id, runtime_id, repo_location_id, execution_daemon_id, workspace_handle, generation, state, selected_by, selection_reason, reserved_until, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'server', NULL, NULL, 'audit-location', NULL, NULL, 1, ?, 'scheduler', '{}', '2099-01-01T00:10:00Z', '2099-01-01T00:00:00Z', '2099-01-01T00:00:00Z')")
            .bind(new_uuid_v4()).bind(ws).bind(task).bind(&agent_id).bind(state)
            .execute(db.pool()).await.unwrap();
    }
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(db.pool())
        .await
        .unwrap();
    let now = "2099-01-01T00:00:00Z";
    let attempt = |id: String| {
        (
            CreateExecution {
                id: id.clone(),
                task_id: task_a.clone(),
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
                workspace_id: Some(ws_a.clone()),
                created_at: now.to_owned(),
                updated_at: now.to_owned(),
            },
            ClaimExecutionLease {
                execution_id: id,
                expected_version: 1,
                owner: "embedded:audit".to_owned(),
                lease_expires_at: "2099-01-01T00:00:30Z".to_owned(),
                hard_deadline_at: Some("2099-01-01T01:00:00Z".to_owned()),
                now: now.to_owned(),
            },
        )
    };
    // Chat admission is deliberately independent: it may fill the machine
    // after the launch has reserved a slot. The held launch still starts.
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::raw_sql("DROP TABLE agent_chat_turn_job; CREATE TABLE agent_chat_turn_job (id TEXT PRIMARY KEY, responder_identity_id TEXT, status TEXT);").execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO agent_chat_turn_job VALUES ('chat', ?, 'running')")
        .bind(&agent_id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(db.pool())
        .await
        .unwrap();
    let (execution, lease) = attempt(new_uuid_v4());
    assert!(matches!(
        ExecutionRepo::create_with_lease(&db, execution, lease).await,
        Err(DbError::MachineAtCapacity)
    ));
    let state: String =
        sqlx::query_scalar("SELECT state FROM workspace_placement WHERE workspace_id = ?")
            .bind(&ws_a)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(state, "ready");
    let marker: Option<String> =
        sqlx::query_scalar("SELECT reserved_until FROM workspace_placement WHERE workspace_id = ?")
            .bind(&ws_a)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert!(marker.is_some()); // Legacy deadlines on Ready do not hold capacity.
    let mut tx = db.pool().begin().await.unwrap();
    let count =
        crate::machine_capacity::count_machine_capacity(&mut tx, None, Some(1), "db-test-host")
            .await
            .unwrap();
    assert_eq!(
        (
            count.running_executions,
            count.reservations,
            count.active_chat_turns
        ),
        (0, 1, 1)
    );
    assert!(!count.has_capacity());
}
