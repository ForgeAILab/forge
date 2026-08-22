#![allow(dead_code, clippy::assertions_on_constants)]
mod common;

use api_types::{ErrorResponse, ExecutionResponse, LaunchExecutionResponse, TaskResponse};
use axum::http::{Method, StatusCode};
use db::{ExecutionRepo, ExecutionStatus, ExecutionUsageRepo, TaskRepo};
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
        &format!("/api/v1/tasks/{}/recover", task.id),
        json!({ "action": "resume_session" }),
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
    let annotation = task
        .get("error_annotation")
        .expect("task includes error_annotation")
        .as_object()
        .expect("error_annotation is an object");
    let recovery_actions = annotation
        .get("recovery_actions")
        .and_then(Value::as_array)
        .expect("blocking annotation includes recovery_actions");

    assert_eq!(
        annotation.get("type").and_then(Value::as_str),
        Some("manual_stop")
    );
    assert!(
        recovery_actions
            .iter()
            .any(|action| action.as_str() == Some("reexecute")),
        "expected reexecute recovery action: {recovery_actions:?}"
    );
    assert!(
        recovery_actions
            .iter()
            .any(|action| action.as_str() == Some("cancel_task")),
        "expected cancel_task recovery action: {recovery_actions:?}"
    );
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
    ExecutionUsageRepo::upsert(
        &*harness.state.db,
        db::UpsertExecutionUsage {
            execution_id: execution_id.clone(),
            provider: "anthropic".to_owned(),
            model: "claude-test".to_owned(),
            input_tokens: 100,
            output_tokens: 50,
            cache_read_tokens: 10,
            cache_write_tokens: 5,
            cost_usd: Some(0.12),
        },
    )
    .await
    .expect("usage records");

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

    assert_eq!(observability["execution_count"], json!(1));
    assert_eq!(observability["latest_execution_id"], json!(execution_id));
    assert_eq!(observability["latest_execution_status"], json!("completed"));
    assert_eq!(observability["total_runtime_seconds"], json!(10.0));
    assert_eq!(observability["total_input_tokens"], json!(100));
    assert_eq!(observability["total_output_tokens"], json!(50));
    assert_eq!(observability["total_tokens"], json!(165));
    assert_eq!(observability["total_cost_usd"], json!(0.12));
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
        .get("execution_actions")
        .and_then(Value::as_array)
        .expect("task includes execution actions");
    let action = actions
        .iter()
        .find(|action| action.get("action").and_then(Value::as_str) == Some("re_execute"))
        .expect("re_execute action exists");

    assert_eq!(action.get("enabled").and_then(Value::as_bool), Some(false));
    assert!(
        action
            .get("disabled_reason")
            .and_then(Value::as_str)
            .is_some_and(|reason| reason.contains("Retry budget exhausted")),
        "expected retry budget disabled reason: {action:?}"
    );
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

    let launched: LaunchExecutionResponse = common::empty_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/executions/{parent_execution_id}/re-execute"),
        StatusCode::OK,
    )
    .await;

    assert_ne!(launched.data.execution.id, parent_execution_id);
    assert_eq!(launched.data.execution.role, "coder");
    assert_eq!(
        launched
            .data
            .execution_behavior
            .as_ref()
            .map(|behavior| behavior.kind),
        Some(api_types::ExecutionBehaviorKind::ReExecute)
    );
}

#[tokio::test]
async fn recover_request_accepts_context() {
    let workspace_root = common::TestDir::new("ec-context");
    let harness = common::test_app(workspace_root.path(), "execution-controls-context").await;
    let (project_id, _repo_id, agent_id) = setup(&harness, workspace_root.path()).await;
    let task = create_task(&harness, &project_id, "sleep 30").await;
    let launch = launch_task(&harness, &task.id, &agent_id).await;

    stop_execution(&harness, &launch.data.execution.id).await;

    let _: TaskResponse = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/recover", task.id),
        json!({
            "action": "reexecute",
            "reason": "test",
            "context": "extra info"
        }),
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
        &format!("/api/v1/tasks/{task_id}/cancel"),
        json!(null),
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
    let stopped: ExecutionResponse = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/executions/{execution_id}/cancel"),
        json!(null),
        StatusCode::OK,
    )
    .await;
    assert_ne!(
        stopped.status,
        api_types::ExecutionStatus::Running,
        "cancel response: {stopped:?}"
    );
    for _ in 0..50 {
        let execution: ExecutionResponse = common::empty_request(
            &harness.app,
            Method::GET,
            &format!("/api/v1/executions/{execution_id}"),
            StatusCode::OK,
        )
        .await;
        if execution.status != api_types::ExecutionStatus::Running {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("execution {execution_id} did not stop");
}

fn assert_action_enabled(actions: &[Value], action_name: &str, expected_enabled: bool) {
    let action = actions
        .iter()
        .find(|action| action.get("action").and_then(Value::as_str) == Some(action_name))
        .unwrap_or_else(|| panic!("missing {action_name} action in {actions:?}"));
    assert_eq!(
        action.get("enabled").and_then(Value::as_bool),
        Some(expected_enabled),
        "unexpected enabled state for {action_name}: {action:?}"
    );
}
