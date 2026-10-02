#![allow(dead_code, clippy::assertions_on_constants)]
mod common;

use api_types::{
    ErrorResponse, LaunchExecutionResponse, TaskAnnotation, TaskBlockingAnnotation, TaskResponse,
};
use axum::http::{Method, StatusCode};
use chrono::{Duration, Utc};
use db::{CreateReview, ExecutionRepo, ExecutionStatus, ReviewRepo, ReviewStatus, TaskRepo};
use serde_json::{json, Value};

#[tokio::test]
async fn cancelled_task_rejects_launch() {
    let workspace_root = common::TestDir::new("ec-launch");
    let harness = common::test_app(workspace_root.path(), "execution-controls-launch").await;
    let (project_id, _repo_id, agent_id) = setup(&harness, workspace_root.path()).await;
    let task = create_task(&harness, &project_id, "sleep 30").await;

    cancel_task(&harness, &task.id).await;

    let response = common::raw_json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/launch", task.id),
        json!({ "agent_id": agent_id }),
    )
    .await;
    let error: ErrorResponse = common::parse_response(response, StatusCode::CONFLICT).await;

    assert!(
        error.code.contains("terminal"),
        "expected terminal error code, got {}",
        error.code
    );
}

#[tokio::test]
async fn cancelled_task_rejects_recover() {
    let workspace_root = common::TestDir::new("ec-recover");
    let harness = common::test_app(workspace_root.path(), "execution-controls-recover").await;
    let (project_id, _repo_id, _) = setup(&harness, workspace_root.path()).await;
    let task = create_task(&harness, &project_id, "sleep 30").await;

    cancel_task(&harness, &task.id).await;

    let response = common::raw_json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/actions", task.id),
        json!({ "version": common::task_action_version(&harness.app, &format!("/api/v1/tasks/{}/actions", task.id)).await, "action": {"verb":"retry","fresh_session":false} }),
    )
    .await;
    let status = response.status();
    assert!(
        status == StatusCode::CONFLICT || status == StatusCode::BAD_REQUEST,
        "expected 409 or 400, got {status}"
    );
}

#[tokio::test]
async fn execution_stop_exposes_recovery_actions() {
    let workspace_root = common::TestDir::new("ec-stop-actions");
    let harness = common::test_app(workspace_root.path(), "execution-controls-stop-actions").await;
    let (project_id, _repo_id, agent_id) = setup(&harness, workspace_root.path()).await;
    let task = create_task(&harness, &project_id, "sleep 30").await;
    let launch = launch_task(&harness, &task.id, &agent_id).await;

    stop_execution(&harness, &launch.data.execution.id).await;

    let task: Value = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", task.id),
        StatusCode::OK,
    )
    .await;
    assert!(
        task["error_annotation"].is_null(),
        "stopping a side session leaves the Task condition unchanged"
    );
    let actions = task["available_actions"].as_array().expect("offers");
    assert!(actions
        .iter()
        .any(|offer| offer["action"]["verb"] == "retry"));
    assert!(actions
        .iter()
        .any(|offer| offer["action"]["verb"] == "cancel"));
}

#[tokio::test]
async fn task_response_includes_execution_observability() {
    let workspace_root = common::TestDir::new("ec-observability");
    let harness = common::test_app(workspace_root.path(), "execution-controls-observability").await;
    let (project_id, _repo_id, agent_id) = setup(&harness, workspace_root.path()).await;
    let task = create_task(&harness, &project_id, "echo ready").await;
    let execution_id = db::new_uuid_v4();
    let started_at = "2026-04-30T12:00:00+00:00".to_owned();
    let stopped_at = "2026-04-30T12:00:10+00:00".to_owned();

    ExecutionRepo::create(
        &*harness.state.db,
        db::CreateExecution {
            id: execution_id.clone(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "coder".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: Some(stopped_at.clone()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: Some(stopped_at.clone()),
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: started_at,
            updated_at: stopped_at,
        },
    )
    .await
    .expect("execution creates");
    let task: Value = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", task.id),
        StatusCode::OK,
    )
    .await;
    let observability = task
        .get("execution_observability")
        .expect("task includes execution_observability");

    assert_eq!(observability["counts"]["task_execution_count"], json!(1));
    assert_eq!(observability["counts"]["provider_attempt_count"], json!(0));
    assert_eq!(observability["latest_execution_id"], json!(execution_id));
    assert_eq!(observability["latest_execution_status"], json!("completed"));
    assert_eq!(observability["total_runtime_seconds"], json!(10.0));
    assert_eq!(observability["tokens"]["input_tokens"], json!(0));
    assert_eq!(observability["tokens"]["output_tokens"], json!(0));
    assert_eq!(observability["cost"]["coverage"], json!("no_usage"));
    assert_eq!(observability["cost"]["complete_total"], Value::Null);
}

#[tokio::test]
async fn task_health_uses_active_execution_when_newer_terminal_row_exists() {
    let workspace_root = common::TestDir::new("ec-health-active-execution");
    let harness = common::test_app(
        workspace_root.path(),
        "execution-controls-health-active-execution",
    )
    .await;
    let (project_id, _repo_id, agent_id) = setup(&harness, workspace_root.path()).await;
    let task = create_task(&harness, &project_id, "active execution health").await;
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

    let running_id = db::new_uuid_v4();
    ExecutionRepo::create(
        &*harness.state.db,
        db::CreateExecution {
            id: running_id.clone(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "coder".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("coder-session".to_owned()),
            agent_message_id: None,
            last_activity_at: Some("2026-04-30T12:00:00+00:00".to_owned()),
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: "2026-04-30T12:00:00+00:00".to_owned(),
            updated_at: "2026-04-30T12:00:00+00:00".to_owned(),
        },
    )
    .await
    .expect("running execution creates");

    let newer_terminal_id = db::new_uuid_v4();
    ExecutionRepo::create(
        &*harness.state.db,
        db::CreateExecution {
            id: newer_terminal_id,
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "reviewer".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: Some("2026-04-30T13:00:10+00:00".to_owned()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: Some("2026-04-30T13:00:10+00:00".to_owned()),
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: "2026-04-30T13:00:00+00:00".to_owned(),
            updated_at: "2026-04-30T13:00:10+00:00".to_owned(),
        },
    )
    .await
    .expect("newer terminal execution creates");

    let response: Value = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", task.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        response["execution_observability"]["latest_execution_status"],
        json!("completed")
    );
    assert_eq!(response["workflow_health"]["kind"], json!("running"));
    assert_eq!(response["workflow_health"]["label"], json!("Running"));
    assert_eq!(
        response["workflow_health"]["execution_id"],
        json!(running_id)
    );
    assert_task_list_diagnostics_match(&harness, &project_id, &response).await;
}

#[tokio::test]
async fn mixed_running_roles_keep_interactive_health_and_disable_duplicate_interactive_action() {
    let workspace_root = common::TestDir::new("ec-health-mixed-running-roles");
    let harness = common::test_app(
        workspace_root.path(),
        "execution-controls-health-mixed-running-roles",
    )
    .await;
    let (project_id, _repo_id, agent_id) = setup(&harness, workspace_root.path()).await;
    let task = create_task(&harness, &project_id, "mixed running roles").await;
    let task = TaskRepo::update_status(
        &*harness.state.db,
        db::UpdateTaskStatus {
            id: task.id.clone(),
            expected_version: task.version,
            status: "review".to_owned(),
            assignee_id: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            updated_at: db::now_rfc3339(),
        },
    )
    .await
    .expect("task enters review state");

    let interactive_id = db::new_uuid_v4();
    ExecutionRepo::create(
        &*harness.state.db,
        db::CreateExecution {
            id: interactive_id,
            task_id: task.id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "interactive".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("interactive-session".to_owned()),
            agent_message_id: None,
            last_activity_at: Some("2026-04-30T12:00:00+00:00".to_owned()),
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: "2026-04-30T12:00:00+00:00".to_owned(),
            updated_at: "2026-04-30T12:00:00+00:00".to_owned(),
        },
    )
    .await
    .expect("interactive execution creates");

    let reviewer_id = db::new_uuid_v4();
    ExecutionRepo::create(
        &*harness.state.db,
        db::CreateExecution {
            id: reviewer_id.clone(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "reviewer".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("reviewer-session".to_owned()),
            agent_message_id: None,
            last_activity_at: Some("2026-04-30T13:00:00+00:00".to_owned()),
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: "2026-04-30T13:00:00+00:00".to_owned(),
            updated_at: "2026-04-30T13:00:00+00:00".to_owned(),
        },
    )
    .await
    .expect("reviewer execution creates");

    ReviewRepo::create(
        &*harness.state.db,
        CreateReview {
            id: db::new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: reviewer_id,
            attempt_number: 1,
            status: ReviewStatus::Failed,
            step_results_json: "{}".to_owned(),
            started_at: "2026-04-30T13:00:00+00:00".to_owned(),
            created_at: "2026-04-30T13:00:00+00:00".to_owned(),
            updated_at: "2026-04-30T13:00:00+00:00".to_owned(),
        },
    )
    .await
    .expect("failed review creates");

    let response: Value = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", task.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(response["workflow_health"]["label"], json!("Interactive"));
    assert!(
        response["workflow_exception"]["actions"]
            .as_array()
            .unwrap()
            .iter()
            .all(|offer| offer["action"]["verb"] != "open_interactive"),
        "session launch is separate from Task actions"
    );
    assert_task_list_diagnostics_match(&harness, &project_id, &response).await;
}

#[tokio::test]
async fn open_interactive_targets_current_role_resumable_session_not_newest_reviewer() {
    let workspace_root = common::TestDir::new("ec-open-interactive-role-target");
    let harness = common::test_app(
        workspace_root.path(),
        "execution-controls-open-interactive-role-target",
    )
    .await;
    let (project_id, _repo_id, agent_id) = setup(&harness, workspace_root.path()).await;
    let task = create_task_with_role(
        &harness,
        &project_id,
        "role-aware interactive target",
        "coder",
        &agent_id,
    )
    .await;
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

    let coder_execution_id = db::new_uuid_v4();
    ExecutionRepo::create(
        &*harness.state.db,
        db::CreateExecution {
            id: coder_execution_id.clone(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "coder".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: Some("2026-04-30T12:00:10+00:00".to_owned()),
            parent_execution_id: None,
            agent_session_id: Some("coder-session".to_owned()),
            agent_message_id: None,
            last_activity_at: Some("2026-04-30T12:00:10+00:00".to_owned()),
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: "2026-04-30T12:00:00+00:00".to_owned(),
            updated_at: "2026-04-30T12:00:10+00:00".to_owned(),
        },
    )
    .await
    .expect("coder execution creates");

    let reviewer_execution_id = db::new_uuid_v4();
    ExecutionRepo::create(
        &*harness.state.db,
        db::CreateExecution {
            id: reviewer_execution_id.clone(),
            task_id: task.id.clone(),
            // The newest unrelated reviewer row is deliberately agentless;
            // it must not hide the older current-role resumable session.
            agent_id: None,
            role: "reviewer".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: Some("2026-04-30T13:00:10+00:00".to_owned()),
            parent_execution_id: None,
            agent_session_id: Some("reviewer-session".to_owned()),
            agent_message_id: None,
            last_activity_at: Some("2026-04-30T13:00:10+00:00".to_owned()),
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: "2026-04-30T13:00:00+00:00".to_owned(),
            updated_at: "2026-04-30T13:00:10+00:00".to_owned(),
        },
    )
    .await
    .expect("reviewer execution creates");

    let annotation = TaskAnnotation::Blocking(TaskBlockingAnnotation {
        annotation_type: api_types::FailureKind::RecoveryRequired,
        blocking_reason: "open interactive target test".to_owned(),
        blocked_by: Some("system:test".to_owned()),
        blocked_at: Some(db::now_rfc3339()),
        blocked_execution_id: None,
        artifact: None,
        message: None,
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
            blocked_json: Some(None),
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: db::now_rfc3339(),
        },
    )
    .await
    .expect("recovery annotation persists");

    let response: Value = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", task.id),
        StatusCode::OK,
    )
    .await;
    let retry = response["workflow_exception"]["actions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|offer| offer["action"]["verb"] == "retry")
        .expect("workflow retry offer");
    assert_eq!(
        response["execution_observability"]["latest_execution_id"],
        json!(reviewer_execution_id)
    );
    assert_eq!(
        retry["target_execution_id"],
        json!(coder_execution_id),
        "retry selects the current-role thread"
    );
    assert_task_list_diagnostics_match(&harness, &project_id, &response).await;
}

#[tokio::test]
async fn running_interactive_session_outranks_newer_open_interactive_lease_in_health() {
    let workspace_root = common::TestDir::new("ec-health-interactive-lease");
    let harness = common::test_app(
        workspace_root.path(),
        "execution-controls-health-interactive-lease",
    )
    .await;
    let (project_id, _repo_id, agent_id) = setup(&harness, workspace_root.path()).await;
    let task = create_task(&harness, &project_id, "interactive lease priority").await;
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

    let running_session_id = db::new_uuid_v4();
    ExecutionRepo::create(
        &*harness.state.db,
        db::CreateExecution {
            id: running_session_id.clone(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "interactive".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("live-interactive-session".to_owned()),
            agent_message_id: None,
            last_activity_at: Some("2026-04-30T12:00:00+00:00".to_owned()),
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: "2026-04-30T12:00:00+00:00".to_owned(),
            updated_at: "2026-04-30T12:00:00+00:00".to_owned(),
        },
    )
    .await
    .expect("running interactive session creates");

    let lease_execution_id = db::new_uuid_v4();
    ExecutionRepo::create(
        &*harness.state.db,
        db::CreateExecution {
            id: lease_execution_id.clone(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
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
            summary: Some("open_interactive dispatch lease".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: "2026-04-30T13:00:00+00:00".to_owned(),
            updated_at: "2026-04-30T13:00:00+00:00".to_owned(),
        },
    )
    .await
    .expect("open interactive lease creates");
    let lease_expires_at = (Utc::now() + Duration::seconds(30)).to_rfc3339();
    sqlx::query(
        "UPDATE execution
         SET lease_owner = 'open-interactive:lease', lease_expires_at = ?
         WHERE id = ?",
    )
    .bind(lease_expires_at)
    .bind(&lease_execution_id)
    .execute(harness.state.db.pool())
    .await
    .expect("open interactive lease records expiry");

    let response: Value = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", task.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        response["execution_observability"]["latest_execution_id"],
        json!(lease_execution_id)
    );
    assert_eq!(response["workflow_health"]["label"], json!("Interactive"));
    assert_eq!(
        response["workflow_health"]["execution_id"],
        json!(running_session_id)
    );
    assert_task_list_diagnostics_match(&harness, &project_id, &response).await;
}

#[tokio::test]
async fn launch_response_includes_execution_behavior() {
    let workspace_root = common::TestDir::new("ec-behavior");
    let harness = common::test_app(workspace_root.path(), "execution-controls-behavior").await;
    let (project_id, _repo_id, agent_id) = setup(&harness, workspace_root.path()).await;
    let task = create_task(&harness, &project_id, "sleep 30").await;

    let launch: Value = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/launch", task.id),
        json!({ "agent_id": agent_id }),
        StatusCode::OK,
    )
    .await;
    let behavior = launch
        .get("data")
        .and_then(|data| data.get("execution_behavior"))
        .expect("launch response includes execution_behavior");

    assert_eq!(
        behavior.get("kind").and_then(Value::as_str),
        Some("manual_launch")
    );
    assert_eq!(
        behavior.get("propagates").and_then(Value::as_bool),
        Some(false)
    );
}

#[tokio::test]
async fn blocked_metadata_retry_budget_disables_re_execute_action() {
    let workspace_root = common::TestDir::new("ec-blocked-actions");
    let harness = common::test_app(workspace_root.path(), "execution-controls-blocked").await;
    let (project_id, _repo_id, agent_id) = setup(&harness, workspace_root.path()).await;
    sqlx::query("UPDATE agent_identity SET paused = 0 WHERE id = ?")
        .bind(&agent_id)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let task = create_task_with_role(
        &harness,
        &project_id,
        "review blocked",
        "reviewer",
        &agent_id,
    )
    .await;
    let now = db::now_rfc3339();
    let task = TaskRepo::update_status(
        &*harness.state.db,
        db::UpdateTaskStatus {
            id: task.id.clone(),
            expected_version: task.version,
            status: "review".to_owned(),
            assignee_id: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task status updates");
    let execution_id = db::new_uuid_v4();
    ExecutionRepo::create(
        &*harness.state.db,
        db::CreateExecution {
            id: execution_id.clone(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "reviewer".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: Some("reviewer-session".to_owned()),
            agent_message_id: None,
            last_activity_at: Some(now.clone()),
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
    .expect("execution creates");
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
            error_annotation: None,
            blocked_json: Some(Some(
                json!({
                    "kind": "review_gate_failed",
                    "reason": "review retry budget exhausted",
                    "execution_id": execution_id,
                    "created_at": now,
                })
                .to_string(),
            )),
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: db::now_rfc3339(),
        },
    )
    .await
    .expect("blocked metadata updates");

    let task: Value = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", task.id),
        StatusCode::OK,
    )
    .await;
    let actions = task
        .get("available_actions")
        .and_then(Value::as_array)
        .expect("task includes available offers");
    let retry = actions
        .iter()
        .find(|offer| offer["action"]["verb"] == "retry")
        .expect("budget retry offer");
    assert_eq!(retry["action"]["reset_budget"], true);
    assert_eq!(retry["reason"], "retry_budget_exhausted");
    assert!(retry["parameters"]
        .as_array()
        .unwrap()
        .iter()
        .all(|parameter| parameter["name"] != "fresh_session"));
    assert_task_list_diagnostics_match(&harness, &project_id, &task).await;
}

#[tokio::test]
async fn re_execute_endpoint_launches_replacement_execution() {
    let workspace_root = common::TestDir::new("ec-reexecute-route");
    let harness = common::test_app(workspace_root.path(), "execution-controls-reexecute").await;
    let (project_id, _repo_id, agent_id) = setup(&harness, workspace_root.path()).await;
    let task =
        create_task_with_role(&harness, &project_id, "echo reexecute", "coder", &agent_id).await;
    let now = db::now_rfc3339();
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
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task status updates");
    let parent_execution_id = db::new_uuid_v4();
    ExecutionRepo::create(
        &*harness.state.db,
        db::CreateExecution {
            id: parent_execution_id.clone(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "coder".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: Some(now.clone()),
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
    .expect("parent execution creates");

    let accepted: TaskResponse = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/actions", task.id),
        json!({ "version": common::task_action_version(&harness.app, &format!("/api/v1/tasks/{}/actions", task.id)).await,"action":{"verb":"retry","fresh_session":true}}),
        StatusCode::OK,
    )
    .await;
    assert!(accepted.error_annotation.is_none());
    harness
        .state
        .task_service
        .test_dispatch_task_action(&task.id)
        .await
        .unwrap();
    let executions = ExecutionRepo::list_running_by_task(&*harness.state.db, &task.id)
        .await
        .unwrap();
    let replacement = executions
        .iter()
        .find(|execution| execution.id != parent_execution_id)
        .expect("replacement dispatched");
    assert_eq!(replacement.role, "coder");
    assert!(
        replacement.agent_session_id.is_none(),
        "fresh retry does not resume the old thread"
    );
}

#[tokio::test]
async fn retry_request_accepts_guidance() {
    let workspace_root = common::TestDir::new("ec-context");
    let harness = common::test_app(workspace_root.path(), "execution-controls-context").await;
    let (project_id, _repo_id, agent_id) = setup(&harness, workspace_root.path()).await;
    let task = create_task_with_role(&harness, &project_id, "sleep 30", "coder", &agent_id).await;
    sqlx::query("UPDATE task SET status = 'in_progress' WHERE id = ?")
        .bind(&task.id)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let launch = launch_task(&harness, &task.id, &agent_id).await;

    stop_execution(&harness, &launch.data.execution.id).await;

    let _: TaskResponse = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/actions", task.id),
        json!({ "version": common::task_action_version(&harness.app, &format!("/api/v1/tasks/{}/actions", task.id)).await,
            "action": {"verb":"retry","fresh_session":true,"guidance":"Continue after repair"}}),
        StatusCode::OK,
    )
    .await;
}

async fn setup(
    harness: &common::Harness,
    workspace_root: &std::path::Path,
) -> (String, String, String) {
    let repo_path = common::setup_git_repo(workspace_root);
    let (project_id, repo_id) =
        common::create_project_and_repo(&harness.app, "Execution Controls", &repo_path).await;
    let (agent_id, reviewer_id) =
        common::create_shell_agents(&harness.app, workspace_root, "execution-controls").await;
    common::configure_execution_test_setup(
        &harness.state.db,
        &project_id,
        &repo_id,
        &agent_id,
        &reviewer_id,
    )
    .await;
    // These fixtures exercise the interactive launch/recovery endpoints. Keep
    // the explicit Project-eligible identities, but do not also seed default
    // role dispatch, which would race the endpoint's own launch.
    clear_project_execution_role_defaults(&harness.state.db, &project_id).await;
    (project_id, repo_id, agent_id)
}

async fn clear_project_execution_role_defaults(db: &db::SqliteDb, project_id: &str) {
    let existing_settings: Option<String> =
        sqlx::query_scalar("SELECT settings FROM project WHERE id = ?")
            .bind(project_id)
            .fetch_optional(db.pool())
            .await
            .expect("test project settings lookup");
    let mut settings = existing_settings
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .filter(Value::is_object)
        .unwrap_or_else(|| json!({}));
    settings
        .as_object_mut()
        .expect("project settings object")
        .remove("default_role_assignments");
    sqlx::query("UPDATE project SET settings = ?, updated_at = ? WHERE id = ?")
        .bind(settings.to_string())
        .bind(db::now_rfc3339())
        .bind(project_id)
        .execute(db.pool())
        .await
        .expect("test execution role defaults clear");
}

async fn create_task(harness: &common::Harness, project_id: &str, title: &str) -> TaskResponse {
    common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": title }),
        StatusCode::OK,
    )
    .await
}

async fn create_task_with_role(
    harness: &common::Harness,
    project_id: &str,
    title: &str,
    role_name: &str,
    agent_id: &str,
) -> TaskResponse {
    common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({
            "title": title,
            "role_assignments": [{
                "role_name": role_name,
                "assignee_type": "agent",
                "assignee_id": agent_id,
            }]
        }),
        StatusCode::OK,
    )
    .await
}

async fn cancel_task(harness: &common::Harness, task_id: &str) -> TaskResponse {
    common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/actions"),
        json!({ "version": common::task_action_version(&harness.app, &format!("/api/v1/tasks/{task_id}/actions")).await,"action":{"verb":"cancel"}}),
        StatusCode::OK,
    )
    .await
}

async fn launch_task(
    harness: &common::Harness,
    task_id: &str,
    agent_id: &str,
) -> LaunchExecutionResponse {
    common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/launch"),
        json!({ "agent_id": agent_id }),
        StatusCode::OK,
    )
    .await
}

async fn stop_execution(harness: &common::Harness, execution_id: &str) {
    let _: api_types::ExecutionResponse = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/executions/{execution_id}/stop"),
        json!({"reason":"Stop this execution"}),
        StatusCode::OK,
    )
    .await;
    for _ in 0..100 {
        let current = ExecutionRepo::get_by_id(&*harness.state.db, execution_id)
            .await
            .unwrap()
            .unwrap();
        if current.status != ExecutionStatus::Running {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("execution {execution_id} did not stop");
}

async fn assert_task_list_diagnostics_match(
    harness: &common::Harness,
    project_id: &str,
    task: &Value,
) {
    let page: Value = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}/tasks"),
        StatusCode::OK,
    )
    .await;
    let listed = page["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == task["id"])
        .unwrap();
    for field in ["workflow_health", "remaining_retries", "role_assignments"] {
        assert_eq!(listed[field], task[field], "list/detail agree on {field}");
    }
    let mut listed_exception = listed["workflow_exception"].clone();
    let mut detail_exception = task["workflow_exception"].clone();
    if let Some(exception) = listed_exception.as_object_mut() {
        assert_eq!(
            exception.remove("actions"),
            Some(json!([])),
            "list rows omit offers"
        );
    }
    if let Some(exception) = detail_exception.as_object_mut() {
        assert_eq!(
            exception.remove("actions"),
            Some(task["available_actions"].clone()),
            "detail diagnostics use the shared live offers"
        );
    }
    assert_eq!(
        listed_exception, detail_exception,
        "list/detail diagnostic facts agree"
    );
    assert_eq!(
        listed["execution_observability"]["latest_execution_id"],
        task["execution_observability"]["latest_execution_id"]
    );
}

#[tokio::test]
async fn stop_route_stops_only_the_selected_side_session() {
    let root = common::TestDir::new("stop-side-session");
    let harness = common::test_app(root.path(), "stop-side-session").await;
    let (project, _, agent) = setup(&harness, root.path()).await;
    let task = create_task(&harness, &project, "Two independent executions").await;
    let now = db::now_rfc3339();
    let mut ids = Vec::new();
    for role in ["coder", "interactive"] {
        let execution = ExecutionRepo::create(
            &*harness.state.db,
            db::CreateExecution {
                id: db::new_uuid_v4(),
                task_id: task.id.clone(),
                agent_id: Some(agent.clone()),
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
            },
        )
        .await
        .unwrap();
        ids.push(execution.id);
    }
    let before = TaskRepo::get_by_id(&*harness.state.db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    stop_execution(&harness, &ids[1]).await;
    assert_eq!(
        ExecutionRepo::get_by_id(&*harness.state.db, &ids[0])
            .await
            .unwrap()
            .unwrap()
            .status,
        ExecutionStatus::Running
    );
    assert_eq!(
        ExecutionRepo::get_by_id(&*harness.state.db, &ids[1])
            .await
            .unwrap()
            .unwrap()
            .status,
        ExecutionStatus::Cancelled
    );
    let after = TaskRepo::get_by_id(&*harness.state.db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.error_annotation, before.error_annotation);
    assert_eq!(after.status, before.status);
}
