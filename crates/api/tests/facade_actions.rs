mod common;

use api_types::{
    ErrorResponse, RecoveryAction, TaskAction, TaskActionsResponse, TaskAnnotation,
    TaskBlockingAnnotation, TaskResponse, WorkflowDefinition,
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
            &format!("/api/v1/tasks/{}/submit", task.id),
            json!({}),
            StatusCode::CONFLICT,
        )
        .await;

        assert_eq!(error.code, "task_action.unavailable");
        let details = error.details.expect("structured action details");
        assert!(details
            .get("reason")
            .and_then(Value::as_str)
            .is_some_and(|reason| reason.contains("submit")));
        let actions = details
            .get("available_actions")
            .and_then(Value::as_array)
            .expect("available actions array");
        assert!(actions.iter().any(|action| action == "start"));
        assert!(actions.iter().any(|action| action == "cancel"));
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
    let annotation = TaskAnnotation::Blocking(TaskBlockingAnnotation {
        annotation_type: api_types::FailureKind::RecoveryRequired,
        blocking_reason: "crash_recovery".to_owned(),
        blocked_by: Some("system:crash_recovery".to_owned()),
        blocked_at: Some(db::now_rfc3339()),
        blocked_execution_id: None,
        artifact: None,
        message: Some("Recovered after server restart".to_owned()),
        hook: None,
        recovery_actions: vec![
            RecoveryAction::Reexecute,
            RecoveryAction::ResetToInitial,
            RecoveryAction::CancelTask,
        ],
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
    assert_eq!(
        actions.recovery_actions,
        vec![
            RecoveryAction::Reexecute,
            RecoveryAction::ResetToInitial,
            RecoveryAction::CancelTask,
        ]
    );

    let error: ErrorResponse = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/recover", task.id),
        json!({ "action": "retry_hook" }),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert!(error.message.contains("not allowed"));

    let current: TaskResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", task.id),
        StatusCode::OK,
    )
    .await;
    let TaskAnnotation::Blocking(current_annotation) =
        current.error_annotation.expect("annotation remains")
    else {
        panic!("expected typed blocking annotation");
    };
    assert_eq!(
        current_annotation.recovery_actions,
        vec![
            RecoveryAction::Reexecute,
            RecoveryAction::ResetToInitial,
            RecoveryAction::CancelTask,
        ]
    );
}

#[tokio::test]
async fn task_response_action_projection_paginates_execution_history() {
    let workspace_root = common::TestDir::new("facade-execution-history");
    let repo_root = common::TestDir::new("facade-execution-history-repo");
    let repo_path = common::setup_git_repo(repo_root.path());
    let harness = common::test_app(workspace_root.path(), "facade-execution-history").await;
    let (project_id, _) =
        common::create_project_and_repo(&harness.app, "Execution history", &repo_path).await;
    let task = create_task(&harness.app, &project_id, "history action authority").await;
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
            agent_id: None,
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
    let action = response["execution_actions"]
        .as_array()
        .expect("execution actions array")
        .iter()
        .find(|action| action["action"] == "session_follow_up")
        .expect("session follow-up action exists");
    assert_eq!(action["enabled"], true);
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
        .contains(&TaskAction::Resume));
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
            &format!("/api/v1/tasks/{}/submit", submit_task.id),
            json!({ "reason": "facade submit" }),
            StatusCode::OK,
        )
        .await;
        assert_api_transition(&harness, &submit_task.id, active_state, "review").await;

        let gate_task = create_task(&harness.app, &project_id, "gate actions").await;
        set_status(&harness, &gate_task.id, gate_state).await;
        let _requested: TaskResponse = common::json_request(
            &harness.app,
            Method::POST,
            &format!("/api/v1/tasks/{}/request-changes", gate_task.id),
            json!({ "reason": "facade request changes" }),
            StatusCode::OK,
        )
        .await;
        assert_api_transition(&harness, &gate_task.id, gate_state, request_changes_target).await;

        set_status(&harness, &gate_task.id, gate_state).await;
        let _approved: TaskResponse = common::json_request(
            &harness.app,
            Method::POST,
            &format!("/api/v1/tasks/{}/approve", gate_task.id),
            json!({ "reason": "facade approve" }),
            StatusCode::OK,
        )
        .await;
        assert_api_transition(&harness, &gate_task.id, gate_state, approval_target).await;

        let cancel_task = create_task(&harness.app, &project_id, "cancel").await;
        let _cancelled: TaskResponse = common::json_request(
            &harness.app,
            Method::POST,
            &format!("/api/v1/tasks/{}/cancel", cancel_task.id),
            json!({}),
            StatusCode::OK,
        )
        .await;
        assert_api_transition(&harness, &cancel_task.id, initial_state, "cancelled").await;
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
            &format!("/api/v1/tasks/{}/start", task.id),
            json!({}),
            StatusCode::OK,
        )
        .await;
        assert_ne!(started.status, "ready");
        assert_ne!(started.status, "todo");

        let paused: TaskResponse = common::json_request(
            &harness.app,
            Method::POST,
            &format!("/api/v1/tasks/{}/pause", task.id),
            json!({ "reason": "test pause" }),
            StatusCode::OK,
        )
        .await;
        assert!(paused.error_annotation.is_some());

        let _resumed: TaskResponse = common::json_request(
            &harness.app,
            Method::POST,
            &format!("/api/v1/tasks/{}/resume", task.id),
            json!({ "reason": "test resume" }),
            StatusCode::OK,
        )
        .await;

        let _paused_again: TaskResponse = common::json_request(
            &harness.app,
            Method::POST,
            &format!("/api/v1/tasks/{}/pause", task.id),
            json!({}),
            StatusCode::OK,
        )
        .await;

        let _cancelled: TaskResponse = common::json_request(
            &harness.app,
            Method::POST,
            &format!("/api/v1/tasks/{}/cancel", task.id),
            json!({}),
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
) {
    let logs = TransitionLogRepo::list_by_task(&*harness.state.db, task_id)
        .await
        .expect("transition logs load");
    assert!(
        logs.iter().any(|log| {
            log.from_state == from_state
                && log.to_state == to_state
                && matches!(log.triggered_by.as_str(), "user:api" | "user:override:api")
        }),
        "expected {from_state} -> {to_state} by an API user, got {logs:?}"
    );
}
