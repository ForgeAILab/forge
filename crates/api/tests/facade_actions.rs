mod common;

use api_types::{ErrorResponse, TaskResponse, WorkflowDefinition};
use axum::http::{Method, StatusCode};
use db::TransitionLogRepo;
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
