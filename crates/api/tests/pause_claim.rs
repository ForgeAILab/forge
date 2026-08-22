#![allow(dead_code, clippy::assertions_on_constants)]
mod common;

use api_types::{AgentResponse, TaskResponse};
use axum::http::{Method, StatusCode};
use serde_json::{json, Value};

#[tokio::test]
async fn claim_rejects_paused_agent() {
    let repo_dir = common::TestDir::new("pause-claim-agent-repo");
    let repo_path = common::setup_git_repo(repo_dir.path());
    let workspace_root = common::TestDir::new("pause-claim-agent-workspaces");
    let harness = common::test_app(workspace_root.path(), "pause-claim-agent").await;
    let (project_id, _repo_id) =
        common::create_project_and_repo(&harness.app, "Pause Claim Agent", &repo_path).await;
    let (agent_id, _) =
        common::create_shell_agents(&harness.app, workspace_root.path(), "pause-claim-agent").await;

    let _: Value = common::empty_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/agents/{agent_id}/pause"),
        StatusCode::OK,
    )
    .await;

    let task = create_task(&harness, &project_id, "Paused agent task").await;

    let response = common::raw_json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/claim", task.id),
        json!({ "agent_id": agent_id }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body: Value = common::parse_response(response, StatusCode::CONFLICT).await;
    assert_eq!(body["code"], "agent_paused");
}

#[tokio::test]
async fn claim_rejects_agent_on_paused_project() {
    let repo_dir = common::TestDir::new("pause-claim-project-repo");
    let repo_path = common::setup_git_repo(repo_dir.path());
    let workspace_root = common::TestDir::new("pause-claim-project-workspaces");
    let harness = common::test_app(workspace_root.path(), "pause-claim-project").await;
    let (project_id, _repo_id) =
        common::create_project_and_repo(&harness.app, "Pause Claim Project", &repo_path).await;
    let (agent_id, _) =
        common::create_shell_agents(&harness.app, workspace_root.path(), "pause-claim-project")
            .await;

    let _: Value = common::empty_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/pause"),
        StatusCode::OK,
    )
    .await;

    let task = create_task(&harness, &project_id, "Paused project task").await;

    let response = common::raw_json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/claim", task.id),
        json!({ "agent_id": agent_id }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body: Value = common::parse_response(response, StatusCode::CONFLICT).await;
    assert_eq!(body["code"], "project_paused");
}

#[tokio::test]
async fn claim_allows_human_on_paused_project() {
    let repo_dir = common::TestDir::new("pause-claim-human-repo");
    let repo_path = common::setup_git_repo(repo_dir.path());
    let workspace_root = common::TestDir::new("pause-claim-human-workspaces");
    let harness = common::test_app(workspace_root.path(), "pause-claim-human").await;
    let (project_id, _repo_id) =
        common::create_project_and_repo(&harness.app, "Pause Claim Human", &repo_path).await;

    let _: Value = common::empty_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/pause"),
        StatusCode::OK,
    )
    .await;

    let task = create_task(&harness, &project_id, "Human claim task").await;

    let response = common::raw_json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/cancel", task.id),
        json!({}),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn running_task_continues_after_pause() {
    let repo_dir = common::TestDir::new("pause-claim-running-repo");
    let repo_path = common::setup_git_repo(repo_dir.path());
    let workspace_root = common::TestDir::new("pause-claim-running-workspaces");
    let harness = common::test_app(workspace_root.path(), "pause-claim-running").await;
    let (project_id, repo_id) =
        common::create_project_and_repo(&harness.app, "Pause Claim Running", &repo_path).await;
    let (agent_id, reviewer_id) =
        common::create_shell_agents(&harness.app, workspace_root.path(), "pause-claim-running")
            .await;
    common::configure_execution_test_setup(
        &harness.state.db,
        &project_id,
        &repo_id,
        &agent_id,
        &reviewer_id,
    )
    .await;

    let task = create_task(&harness, &project_id, "Running task").await;

    let _: Value = common::json_request(
        &harness.app,
        Method::PUT,
        &format!("/api/v1/tasks/{}/roles/coder", task.id),
        json!({ "assignee_type": "agent", "assignee_id": agent_id }),
        StatusCode::OK,
    )
    .await;

    let claimed: TaskResponse = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/claim", task.id),
        json!({ "agent_id": agent_id }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(claimed.status, "in_progress");

    let _: AgentResponse = common::empty_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/agents/{agent_id}/pause"),
        StatusCode::OK,
    )
    .await;

    let _: Value = common::empty_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/pause"),
        StatusCode::OK,
    )
    .await;

    let task_after_pause: TaskResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", task.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(task_after_pause.status, "in_progress");

    let agent_after_pause: AgentResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/agents/{agent_id}"),
        StatusCode::OK,
    )
    .await;
    assert!(agent_after_pause.paused);
    assert_eq!(
        agent_after_pause.effective_status.as_deref(),
        Some("paused")
    );
}

async fn create_task(harness: &common::Harness, project_id: &str, title: &str) -> TaskResponse {
    common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": title
        }),
        StatusCode::OK,
    )
    .await
}
