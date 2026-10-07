mod common;

use api_types::{
    ErrorResponse, TaskActionsResponse, TaskAnnotation, TaskBlockingAnnotation, TaskResponse,
    WorkflowDefinition,
};
use axum::http::{Method, StatusCode};
use db::{CreateExecution, ExecutionRepo, ExecutionStatus, TaskRepo, TransitionLogRepo};
use serde_json::{json, Value};

#[tokio::test]
async fn unavailable_actions_include_capabilities_for_both_workflows() {
    let workspace_root = common::TestDir::new("facade-invalid-actions");
    let repo_root = common::TestDir::new("facade-invalid-repo");
    let repo_path = common::setup_git_repo(repo_root.path());
    let harness = common::test_app(workspace_root.path(), "facade-invalid-actions").await;
    harness
        .state
        .workflow_template_service
        .initialize()
        .await
        .expect("workflow templates initialize");

    common::create_shell_agents(
        &harness.app,
        workspace_root.path(),
        "available-action-agents",
    )
    .await;
    let (autonomous_project, _) =
        common::create_project_and_repo(&harness.app, "Autonomous", &repo_path).await;
    set_workflow(&harness.app, &autonomous_project, "autonomous_v1").await;
    let (strict_project, _) =
        common::create_project_and_repo(&harness.app, "Strict", &repo_path).await;

    for project_id in [autonomous_project, strict_project] {
        let task = create_task(&harness.app, &project_id, "invalid action").await;
        let error: ErrorResponse = common::json_request(
            &harness.app,
            Method::POST,
            &format!("/api/v1/tasks/{}/actions", task.id),
            json!({ "version": common::task_action_version(&harness.app, &format!("/api/v1/tasks/{}/actions", task.id)).await, "action": {"verb":"approve","override":false} }),
            StatusCode::CONFLICT,
        )
        .await;

        assert_eq!(error.code, "action_unavailable");
        let details = error.details.expect("structured action details");
        assert!(details
            .get("reason")
            .and_then(Value::as_str)
            .is_some_and(|reason| reason.contains("approve")));
        let actions = details
            .get("available_actions")
            .and_then(Value::as_array)
            .expect("available actions array");
        assert!(actions
            .iter()
            .any(|offer| offer["action"]["verb"] == "start"));
        assert!(actions
            .iter()
            .any(|offer| offer["action"]["verb"] == "cancel"));
    }
}

#[tokio::test]
async fn task_actions_exposes_and_enforces_the_typed_recovery_contract() {
    let workspace_root = common::TestDir::new("facade-recovery-actions");
    let repo_root = common::TestDir::new("facade-recovery-repo");
    let repo_path = common::setup_git_repo(repo_root.path());
    let harness = common::test_app(workspace_root.path(), "facade-recovery-actions").await;
    let (project_id, _) =
        common::create_project_and_repo(&harness.app, "Recovery Actions", &repo_path).await;
    let task = create_task(&harness.app, &project_id, "closed recovery contract").await;
    let (agent, _) =
        common::create_shell_agents(&harness.app, workspace_root.path(), "recovery-contract").await;
    db::TaskRoleAssignmentRepo::assign(
        &*harness.state.db,
        db::CreateTaskRoleAssignment {
            id: db::new_uuid_v4(),
            task_id: task.id.clone(),
            role_name: "coder".to_owned(),
            assignee_type: Some(db::AssigneeKind::Agent),
            assignee_id: Some(agent),
            created_at: db::now_rfc3339(),
            updated_at: db::now_rfc3339(),
        },
    )
    .await
    .unwrap();
    let annotation = TaskAnnotation::Blocking(TaskBlockingAnnotation {
        annotation_type: api_types::FailureKind::RecoveryRequired,
        blocking_reason: "crash_recovery".to_owned(),
        blocked_by: Some("system:crash_recovery".to_owned()),
        blocked_at: Some(db::now_rfc3339()),
        blocked_execution_id: None,
        artifact: None,
        message: Some("Recovered after server restart".to_owned()),
        hook: None,
    });
    TaskRepo::update(
        &*harness.state.db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(
                serde_json::to_string(&annotation).expect("annotation serializes"),
            )),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: db::now_rfc3339(),
        },
    )
    .await
    .expect("recovery annotation persists");

    let actions: TaskActionsResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/actions", task.id),
        StatusCode::OK,
    )
    .await;
    assert!(actions
        .available_actions
        .iter()
        .any(|offer| offer.action.verb() == "retry"));

    let error: ErrorResponse = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/actions", task.id),
        json!({ "version": common::task_action_version(&harness.app, &format!("/api/v1/tasks/{}/actions", task.id)).await, "action": {"verb":"retry","fresh_session":false} }),
        StatusCode::CONFLICT,
    )
    .await;
    assert!(error.message.contains("unavailable"));

    let current: TaskResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", task.id),
        StatusCode::OK,
    )
    .await;
    let current_annotation = current
        .condition
        .details()
        .diagnostic
        .clone()
        .expect("annotation remains");
    assert!(serde_json::to_value(current_annotation)
        .unwrap()
        .get("recovery_actions")
        .is_none());
}

#[tokio::test]
async fn task_response_action_projection_paginates_execution_history() {
    let workspace_root = common::TestDir::new("facade-execution-history");
    let repo_root = common::TestDir::new("facade-execution-history-repo");
    let repo_path = common::setup_git_repo(repo_root.path());
    let harness = common::test_app(workspace_root.path(), "facade-execution-history").await;
    let (project_id, _) =
        common::create_project_and_repo(&harness.app, "Execution history", &repo_path).await;
    let (agent_id, _) =
        common::create_shell_agents(&harness.app, workspace_root.path(), "history-authority").await;
    let task = create_task(&harness.app, &project_id, "history action authority").await;
    sqlx::query("UPDATE task SET assignee_type = 'agent', assignee_id = ? WHERE id = ?")
        .bind(&agent_id)
        .bind(&task.id)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let task = TaskRepo::update_status(
        &*harness.state.db,
        db::UpdateTaskStatus {
            id: task.id.clone(),
            expected_version: task.version,
            status: "in_progress".to_owned(),
            assignee_id: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            updated_at: db::now_rfc3339(),
        },
    )
    .await
    .expect("task enters coder state");
    let old_execution_id = db::new_uuid_v4();
    let old_timestamp = "2026-01-01T00:00:00Z".to_owned();

    ExecutionRepo::create(
        &*harness.state.db,
        CreateExecution {
            id: old_execution_id.clone(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.clone()),
            // Older persisted attempts used the historical executor role;
            // the current coder projection must still find this resumable
            // session beyond the newer history rows.
            role: "executor".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: Some(old_timestamp.clone()),
            parent_execution_id: None,
            agent_session_id: Some("old-session".to_owned()),
            agent_message_id: None,
            last_activity_at: Some(old_timestamp.clone()),
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: old_timestamp.clone(),
            updated_at: old_timestamp,
        },
    )
    .await
    .expect("old resumable execution creates");

    // Put the resumable execution beyond the historical newest-100 limit and
    // a naive first page. Newer attempts deliberately have no session, so
    // omitting the older row changes SessionFollowUp authority.
    for index in 0..501 {
        let timestamp = format!("2026-01-02T{:02}:{:02}:00Z", index / 60, index % 60);
        ExecutionRepo::create(
            &*harness.state.db,
            CreateExecution {
                id: db::new_uuid_v4(),
                task_id: task.id.clone(),
                agent_id: None,
                role: "coder".to_owned(),
                status: ExecutionStatus::Completed,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: Some(timestamp.clone()),
                parent_execution_id: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: Some(timestamp.clone()),
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                workspace_id: None,
                created_at: timestamp.clone(),
                updated_at: timestamp,
            },
        )
        .await
        .expect("newer execution creates");
    }

    let response: Value = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", task.id),
        StatusCode::OK,
    )
    .await;
    let action = response["available_actions"]
        .as_array()
        .expect("available actions array")
        .iter()
        .find(|offer| offer["action"]["verb"] == "retry")
        .expect("session follow-up action exists");
    assert_eq!(action["action"]["fresh_session"], false);
    assert_eq!(action["target_execution_id"], old_execution_id);

    // The facade route uses the same bounded execution authority as the Task
    // response. The resumable historical executor row is beyond the newest
    // 100 attempts, but it must still make the Task's Resume action visible.
    let facade_actions: TaskActionsResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/actions", task.id),
        StatusCode::OK,
    )
    .await;
    assert!(facade_actions
        .available_actions
        .iter()
        .any(|offer| offer.action.verb() == "retry"));
}

#[tokio::test]
async fn recover_full_agent_returns_queued_task_and_preserves_other_errors() {
    let workspace_root = common::TestDir::new("facade-recovery-capacity");
    let repo_root = common::TestDir::new("facade-recovery-capacity-repo");
    let repo_path = common::setup_git_repo(repo_root.path());
    let harness = common::test_app(workspace_root.path(), "facade-recovery-capacity").await;
    let (project_id, repo_id) =
        common::create_project_and_repo(&harness.app, "Recovery capacity", &repo_path).await;
    let (agent_id, _) =
        common::create_shell_agents(&harness.app, workspace_root.path(), "recovery-capacity").await;
    common::configure_execution_test_setup(
        &harness.state.db,
        &project_id,
        &repo_id,
        &agent_id,
        &agent_id,
    )
    .await;
    let busy = create_task(&harness.app, &project_id, "occupies capacity").await;
    let now = db::now_rfc3339();
    ExecutionRepo::create(
        &*harness.state.db,
        CreateExecution {
            id: db::new_uuid_v4(),
            task_id: busy.id,
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
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("capacity occupant creates");
    sqlx::query("UPDATE agent_identity SET max_concurrent_tasks = 1, paused = 0, version = version + 1 WHERE id = ?")
        .bind(&agent_id).execute(harness.state.db.pool()).await.expect("agent capacity and pause persist");
    let task = create_task(&harness.app, &project_id, "recover").await;
    db::TaskRoleAssignmentRepo::assign(
        &*harness.state.db,
        db::CreateTaskRoleAssignment {
            id: db::new_uuid_v4(),
            task_id: task.id.clone(),
            role_name: "coder".to_owned(),
            assignee_type: Some(db::AssigneeKind::Agent),
            assignee_id: Some(agent_id.clone()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("coder assigns");
    let annotation = json!({
        "type": "recovery_required",
        "blocking_reason": "crash_recovery",
        "recovery_actions": ["reexecute"],
    })
    .to_string();
    let task = TaskRepo::update_status(
        &*harness.state.db,
        db::UpdateTaskStatus {
            id: task.id.clone(),
            expected_version: task.version,
            status: "in_progress".to_owned(),
            assignee_id: None,
            error_annotation: Some(Some(annotation.clone())),
            blocked_json: None,
            failed_json: None,
            updated_at: db::now_rfc3339(),
        },
    )
    .await
    .expect("recoverable task persists");
    let url = format!("/api/v1/tasks/{}/actions", task.id);
    let invalid: ErrorResponse = common::json_request(
        &harness.app,
        Method::POST,
        &url,
        json!({ "version": common::task_action_version(&harness.app, &url).await, "action": {"verb":"retry","fresh_session":false} }),
        StatusCode::CONFLICT,
    )
    .await;
    assert!(invalid.message.contains("unavailable"));
    let response: TaskResponse = common::json_request(
        &harness.app,
        Method::POST,
        &url,
        json!({ "version": common::task_action_version(&harness.app, &url).await, "action": {"verb":"retry","fresh_session":true,"guidance":"recovery guidance"} }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(response.id, task.id);
    assert_eq!(response.status, "in_progress");
    assert!(response.condition.details().diagnostic.is_none());
    assert!(response.condition.details().interruption.is_none());
    sqlx::query("UPDATE agent_identity SET paused = 0, version = version + 1 WHERE id = ?")
        .bind(&agent_id)
        .execute(harness.state.db.pool())
        .await
        .expect("agent unpauses");
    let queued = TaskRepo::get_by_id(&*harness.state.db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    let repeated: ErrorResponse = common::json_request(
        &harness.app,
        Method::POST,
        &url,
        json!({ "version": common::task_action_version(&harness.app, &url).await, "action": {"verb":"retry","fresh_session":true} }),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(repeated.code, "action_unavailable");
    let current = TaskRepo::get_by_id(&*harness.state.db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.metadata_json, queued.metadata_json);
    let health = response.workflow_health.expect("queued health returns");
    assert_eq!(health.kind, api_types::WorkflowHealthKind::WaitingForAgent);
    assert_eq!(health.label, "Retry Queued");
    assert!(health
        .message
        .as_deref()
        .is_some_and(|detail| detail.contains("waiting for capacity")));
    assert!(
        ExecutionRepo::list_running_by_task(&*harness.state.db, &task.id)
            .await
            .expect("task executions load")
            .is_empty()
    );
}

#[tokio::test]
async fn facade_transitions_are_attributed_to_api_users_for_both_workflows() {
    let workspace_root = common::TestDir::new("facade-actor");
    let repo_root = common::TestDir::new("facade-actor-repo");
    let repo_path = common::setup_git_repo(repo_root.path());
    let harness = common::test_app(workspace_root.path(), "facade-actor").await;
    harness
        .state
        .workflow_template_service
        .initialize()
        .await
        .expect("workflow templates initialize");

    let (autonomous_project, _) =
        common::create_project_and_repo(&harness.app, "Autonomous", &repo_path).await;
    set_workflow(&harness.app, &autonomous_project, "autonomous_v1").await;
    let (strict_project, _) =
        common::create_project_and_repo(&harness.app, "Strict", &repo_path).await;
    set_workflow(&harness.app, &strict_project, "human-required").await;

    for (
        project_id,
        initial_state,
        active_state,
        gate_state,
        request_changes_target,
        approval_target,
    ) in [
        (
            autonomous_project,
            "ready",
            "working",
            "review",
            "working",
            "merging",
        ),
        (
            strict_project,
            "todo",
            "in_progress",
            "review",
            "in_progress",
            "merging",
        ),
    ] {
        let submit_task = create_task(&harness.app, &project_id, "submit").await;
        set_status(&harness, &submit_task.id, active_state).await;
        // `submit` hands the agent's work to review, so it is only offered
        // once that work has actually completed.
        seed_completed_role_execution(&harness, &submit_task.id, active_state).await;
        let _submitted: TaskResponse = common::json_request(
            &harness.app,
            Method::POST,
            &format!("/api/v1/tasks/{}/actions", submit_task.id),
            json!({ "version": common::task_action_version(&harness.app, &format!("/api/v1/tasks/{}/actions", submit_task.id)).await, "action": {"verb":"approve","override":false}}),
            StatusCode::OK,
        )
        .await;
        assert_api_transition(&harness, &submit_task.id, active_state, "review", "approve").await;

        let gate_task = create_task(&harness.app, &project_id, "gate actions").await;
        set_status(&harness, &gate_task.id, gate_state).await;
        let _requested: TaskResponse = common::json_request(
            &harness.app,
            Method::POST,
            &format!("/api/v1/tasks/{}/actions", gate_task.id),
            json!({ "version": common::task_action_version(&harness.app, &format!("/api/v1/tasks/{}/actions", gate_task.id)).await, "action": {"verb":"send_back","guidance":"facade request changes"}}),
            StatusCode::OK,
        )
        .await;
        assert_api_transition(
            &harness,
            &gate_task.id,
            gate_state,
            request_changes_target,
            "send_back",
        )
        .await;

        set_status(&harness, &gate_task.id, gate_state).await;
        seed_completed_role_execution(&harness, &gate_task.id, active_state).await;
        let candidate: String = sqlx::query_scalar(
            "SELECT id FROM execution WHERE task_id = ? ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(&gate_task.id)
        .fetch_one(harness.state.db.pool())
        .await
        .unwrap();
        let now = db::now_rfc3339();
        db::ReviewRepo::create(
            &*harness.state.db,
            db::CreateReview {
                id: db::new_uuid_v4(),
                task_id: gate_task.id.clone(),
                execution_id: candidate,
                attempt_number: 1,
                status: db::ReviewStatus::AwaitingHuman,
                step_results_json: json!({"ci_steps":[]}).to_string(),
                started_at: now.clone(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .unwrap();
        let _approved: TaskResponse = common::json_request(
            &harness.app,
            Method::POST,
            &format!("/api/v1/tasks/{}/actions", gate_task.id),
            json!({ "version": common::task_action_version(&harness.app, &format!("/api/v1/tasks/{}/actions", gate_task.id)).await, "action": {"verb":"approve","override":false}}),
            StatusCode::OK,
        )
        .await;
        assert_api_transition(
            &harness,
            &gate_task.id,
            gate_state,
            approval_target,
            "approve",
        )
        .await;

        let cancel_task = create_task(&harness.app, &project_id, "cancel").await;
        let _cancelled: TaskResponse = common::json_request(
            &harness.app,
            Method::POST,
            &format!("/api/v1/tasks/{}/actions", cancel_task.id),
            json!({ "version": common::task_action_version(&harness.app, &format!("/api/v1/tasks/{}/actions", cancel_task.id)).await, "action": {"verb":"cancel"} }),
            StatusCode::OK,
        )
        .await;
        assert_api_transition(
            &harness,
            &cancel_task.id,
            initial_state,
            "cancelled",
            "cancel",
        )
        .await;
    }
}

#[tokio::test]
async fn execution_facade_actions_work_for_both_workflows() {
    let workspace_root = common::TestDir::new("facade-execution-actions");
    let repo_root = common::TestDir::new("facade-execution-repo");
    let repo_path = common::setup_git_repo(repo_root.path());
    let harness = common::test_app(workspace_root.path(), "facade-execution-actions").await;
    harness
        .state
        .workflow_template_service
        .initialize()
        .await
        .expect("workflow templates initialize");
    let (worker_id, reviewer_id) =
        common::create_shell_agents(&harness.app, workspace_root.path(), "facade-execution").await;

    let (autonomous_project, autonomous_repo) =
        common::create_project_and_repo(&harness.app, "Autonomous", &repo_path).await;
    set_workflow(&harness.app, &autonomous_project, "autonomous_v1").await;
    let (strict_project, strict_repo) =
        common::create_project_and_repo(&harness.app, "Strict", &repo_path).await;
    common::configure_execution_test_setup(
        &harness.state.db,
        &autonomous_project,
        &autonomous_repo,
        &worker_id,
        &reviewer_id,
    )
    .await;
    common::configure_execution_test_setup(
        &harness.state.db,
        &strict_project,
        &strict_repo,
        &worker_id,
        &reviewer_id,
    )
    .await;

    for project_id in [autonomous_project, strict_project] {
        let task = create_task_with_description(&harness.app, &project_id, "sleep 5").await;
        sqlx::query("UPDATE task SET assignee_type = 'agent', assignee_id = ? WHERE id = ?")
            .bind(&worker_id)
            .bind(&task.id)
            .execute(harness.state.db.pool())
            .await
            .expect("task assignee updates");

        let started: TaskResponse = common::json_request(
            &harness.app,
            Method::POST,
            &format!("/api/v1/tasks/{}/actions", task.id),
            json!({ "version": common::task_action_version(&harness.app, &format!("/api/v1/tasks/{}/actions", task.id)).await, "action": {"verb":"start"} }),
            StatusCode::OK,
        )
        .await;
        assert!(started.condition.details().diagnostic.is_none());
        harness
            .state
            .task_service
            .test_dispatch_task_action(&task.id)
            .await
            .unwrap();
        let started: TaskResponse = common::empty_request(
            &harness.app,
            Method::GET,
            &format!("/api/v1/tasks/{}", task.id),
            StatusCode::OK,
        )
        .await;
        assert_ne!(started.status, "ready");
        assert_ne!(started.status, "todo");

        let paused: TaskResponse = common::json_request(
            &harness.app,
            Method::POST,
            &format!("/api/v1/tasks/{}/actions", task.id),
            json!({ "version": common::task_action_version(&harness.app, &format!("/api/v1/tasks/{}/actions", task.id)).await, "action": {"verb":"hold"}}),
            StatusCode::OK,
        )
        .await;
        assert!(paused.condition.details().diagnostic.is_some());

        let _resumed: TaskResponse = common::json_request(
            &harness.app,
            Method::POST,
            &format!("/api/v1/tasks/{}/actions", task.id),
            json!({ "version": common::task_action_version(&harness.app, &format!("/api/v1/tasks/{}/actions", task.id)).await, "action": {"verb":"release"}}),
            StatusCode::OK,
        )
        .await;

        harness
            .state
            .task_service
            .test_dispatch_task_action(&task.id)
            .await
            .unwrap();
        let _paused_again: TaskResponse = common::json_request(
            &harness.app,
            Method::POST,
            &format!("/api/v1/tasks/{}/actions", task.id),
            json!({ "version": common::task_action_version(&harness.app, &format!("/api/v1/tasks/{}/actions", task.id)).await, "action": {"verb":"hold"} }),
            StatusCode::OK,
        )
        .await;

        let _cancelled: TaskResponse = common::json_request(
            &harness.app,
            Method::POST,
            &format!("/api/v1/tasks/{}/actions", task.id),
            json!({ "version": common::task_action_version(&harness.app, &format!("/api/v1/tasks/{}/actions", task.id)).await, "action": {"verb":"cancel"} }),
            StatusCode::OK,
        )
        .await;
    }
}

async fn set_workflow(app: &axum::Router, project_id: &str, template_name: &str) {
    let _: WorkflowDefinition = common::json_request(
        app,
        Method::PUT,
        &format!("/api/v1/projects/{project_id}/workflow"),
        json!({ "template_name": template_name }),
        StatusCode::OK,
    )
    .await;
}

async fn create_task(app: &axum::Router, project_id: &str, title: &str) -> TaskResponse {
    common::json_request(
        app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({
            "title": title,
            "description": "true",
            "review_config": { "ci_steps": [] }
        }),
        StatusCode::OK,
    )
    .await
}

async fn create_task_with_description(
    app: &axum::Router,
    project_id: &str,
    description: &str,
) -> TaskResponse {
    common::json_request(
        app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({
            "title": "execution actions",
            "description": description,
            "review_config": { "ci_steps": [] }
        }),
        StatusCode::OK,
    )
    .await
}

/// Record a finished implementation attempt for `task_id`.
///
/// The facade only offers `submit` when the current role's work has completed;
/// a Task whose agent has not run yet has nothing to hand to review.
async fn seed_completed_role_execution(
    harness: &common::Harness,
    task_id: &str,
    active_state: &str,
) {
    // Each workflow names its implementation role differently; the check is
    // against the role the Task's current state actually carries.
    let role = if active_state == "working" {
        "worker"
    } else {
        "coder"
    };
    let now = db::now_rfc3339();
    sqlx::query(
        "INSERT INTO execution (id, task_id, agent_id, role, status, summary, created_at, updated_at)
         VALUES (?, ?, NULL, ?, 'completed', 'seeded work', ?, ?)",
    )
    .bind(db::new_uuid_v4())
    .bind(task_id)
    .bind(role)
    .bind(&now)
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("completed execution seeds");
}

async fn set_status(harness: &common::Harness, task_id: &str, status: &str) {
    sqlx::query("UPDATE task SET status = ?, version = version + 1 WHERE id = ?")
        .bind(status)
        .bind(task_id)
        .execute(harness.state.db.pool())
        .await
        .expect("task status updates");
}

async fn assert_api_transition(
    harness: &common::Harness,
    task_id: &str,
    from_state: &str,
    to_state: &str,
    verb: &str,
) {
    let logs = TransitionLogRepo::list_by_task(&*harness.state.db, task_id)
        .await
        .expect("transition logs load");
    assert!(
        logs.iter().any(|log| {
            log.from_state == from_state
                && log.to_state == to_state
                && (log.triggered_by == format!("user:action:{verb}")
                    || log.triggered_by == format!("user:override:action:{verb}"))
        }),
        "expected {from_state} -> {to_state} by an API user, got {logs:?}"
    );
}

#[tokio::test]
async fn task_actions_rest_diagnostics_and_execution_controls_share_one_offer_set() {
    let workspace_root = common::TestDir::new("action-surface-parity");
    let harness = common::test_app(workspace_root.path(), "action-surface-parity").await;
    let repo = common::setup_git_repo(workspace_root.path());
    let (project, _) = common::create_project_and_repo(&harness.app, "Action parity", &repo).await;
    let _agents =
        common::create_shell_agents(&harness.app, workspace_root.path(), "action-parity").await;
    for scenario in ["failed_review", "stored_annotation", "blocked_budget"] {
        let task = create_task(&harness.app, &project, scenario).await;
        let state = if scenario == "failed_review" {
            "review"
        } else {
            "in_progress"
        };
        set_status(&harness, &task.id, state).await;
        seed_completed_role_execution(&harness, &task.id, "in_progress").await;
        if scenario == "failed_review" {
            let execution_id: String = sqlx::query_scalar(
                "SELECT id FROM execution WHERE task_id = ? ORDER BY created_at DESC LIMIT 1",
            )
            .bind(&task.id)
            .fetch_one(harness.state.db.pool())
            .await
            .unwrap();
            let now = db::now_rfc3339();
            db::ReviewRepo::create(
                &*harness.state.db,
                db::CreateReview {
                    id: db::new_uuid_v4(),
                    task_id: task.id.clone(),
                    execution_id,
                    attempt_number: 1,
                    status: db::ReviewStatus::Failed,
                    step_results_json: json!({"ci_steps":[{"command":"check","exit_code":1}]})
                        .to_string(),
                    started_at: now.clone(),
                    created_at: now.clone(),
                    updated_at: now,
                },
            )
            .await
            .unwrap();
        } else if scenario == "stored_annotation" {
            sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
                .bind(json!({"type":"executor_failed","blocking_reason":"fixture","recovery_actions":["return_to_implementation","retry_pr_publication"]}).to_string())
                .bind(&task.id).execute(harness.state.db.pool()).await.unwrap();
        } else {
            sqlx::query("UPDATE task SET blocked_json = ? WHERE id = ?")
                .bind(json!({"kind":"retry_exhausted","reason":"spent","recovery_actions":["return_to_implementation"]}).to_string())
                .bind(&task.id).execute(harness.state.db.pool()).await.unwrap();
        }
        harness
            .state
            .db
            .check_task_conditions_of(std::slice::from_ref(&task.id))
            .await
            .unwrap();
        let offers: TaskActionsResponse = common::empty_request(
            &harness.app,
            Method::GET,
            &format!("/api/v1/tasks/{}/actions", task.id),
            StatusCode::OK,
        )
        .await;
        let response: TaskResponse = common::empty_request(
            &harness.app,
            Method::GET,
            &format!("/api/v1/tasks/{}", task.id),
            StatusCode::OK,
        )
        .await;
        assert_eq!(response.available_actions, offers.available_actions);
        assert!(serde_json::to_value(&response)
            .unwrap()
            .get("execution_actions")
            .is_none());
        assert_eq!(
            response.workflow_exception.unwrap().actions,
            offers.available_actions
        );
        assert_eq!(response.version, offers.version);
    }
}

#[tokio::test]
async fn action_requests_require_an_explicit_version() {
    let workspace = common::TestDir::new("action-version-contract");
    let repo = common::TestDir::new("action-version-repo");
    let path = common::setup_git_repo(repo.path());
    let harness = common::test_app(workspace.path(), "action-version-contract").await;
    let (project, _) =
        common::create_project_and_repo(&harness.app, "Version contract", &path).await;
    let task = create_task(&harness.app, &project, "Version must be supplied").await;
    let response = common::raw_json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/actions", task.id),
        json!({"action":{"verb":"cancel"}}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let current = TaskRepo::get_by_id(&*harness.state.db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.version, task.version);
    assert_ne!(current.status, "cancelled");
}

#[tokio::test]
async fn paused_task_action_refusals_keep_wait_cause_without_erasing_the_condition() {
    let workspace = common::TestDir::new("action-pause-causes");
    let repo = common::TestDir::new("action-pause-repo");
    let path = common::setup_git_repo(repo.path());
    let harness = common::test_app(workspace.path(), "action-pause-causes").await;
    let (project, _) = common::create_project_and_repo(&harness.app, "Pause causes", &path).await;
    let task = create_task(&harness.app, &project, "Wait for dispatch").await;
    let (agent, _) =
        common::create_shell_agents(&harness.app, workspace.path(), "pause-causes").await;
    db::TaskRoleAssignmentRepo::assign(
        &*harness.state.db,
        db::CreateTaskRoleAssignment {
            id: db::new_uuid_v4(),
            task_id: task.id.clone(),
            role_name: "coder".to_owned(),
            assignee_type: Some(db::AssigneeKind::Agent),
            assignee_id: Some(agent.clone()),
            created_at: db::now_rfc3339(),
            updated_at: db::now_rfc3339(),
        },
    )
    .await
    .unwrap();
    sqlx::query("UPDATE task SET status = 'in_progress', error_annotation = ? WHERE id = ?")
        .bind(json!({"type":"executor_failed","blocking_reason":"fixture"}).to_string())
        .bind(&task.id)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    for cause in [
        "target_agent_paused",
        "project_paused(environment_not_ready)",
    ] {
        sqlx::query("UPDATE agent_identity SET paused = ? WHERE id = ?")
            .bind(cause == "target_agent_paused")
            .bind(&agent)
            .execute(harness.state.db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE project SET paused_at = ?, system_pause_reason = ? WHERE id = ?")
            .bind((cause != "target_agent_paused").then_some("now"))
            .bind((cause != "target_agent_paused").then_some("environment_not_ready"))
            .bind(&project)
            .execute(harness.state.db.pool())
            .await
            .unwrap();
        let error: ErrorResponse = common::json_request(
            &harness.app,
            Method::POST,
            &format!("/api/v1/tasks/{}/actions", task.id),
            json!({"version":task.version,"action":{"verb":"retry"}}),
            StatusCode::CONFLICT,
        )
        .await;
        assert_eq!(error.code, "action_unavailable");
        let details = error.details.unwrap();
        assert_eq!(details["denied_by"], cause);
        assert_eq!(details["retry"]["scope"], "turn");
        // Nothing is waiting to be dispatched and a Hold would overwrite the
        // live condition, so the refusal offers cancel but no Hold.
        let verbs = details["available_actions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|offer| offer["action"]["verb"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        assert!(verbs.iter().any(|verb| verb == "cancel"), "{verbs:?}");
        assert!(!verbs.iter().any(|verb| verb == "hold"), "{verbs:?}");
    }
}
