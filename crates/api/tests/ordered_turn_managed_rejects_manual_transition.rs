#![allow(dead_code, clippy::assertions_on_constants)]
mod common;

use api_types::{TaskResponse, TransitionTaskResponse};
use axum::http::{Method, StatusCode};
use serde_json::json;

#[tokio::test]
async fn subtask_managed_allows_manual_status_transition() {
    let repo_dir = common::TestDir::new("forge-subtask-managed-repo");
    let repo_path = common::setup_git_repo(repo_dir.path());
    let workspace_root = common::TestDir::new("forge-subtask-managed-workspaces");
    let harness = common::test_app(workspace_root.path(), "forge-subtask-managed").await;
    let _ = &harness.state;
    let (project_id, repo_id) =
        common::create_project_and_repo(&harness.app, "Subtask Managed", &repo_path).await;
    let (agent_id, reviewer_id) =
        common::create_shell_agents(&harness.app, workspace_root.path(), "subtask-managed").await;
    common::configure_execution_test_setup(
        &harness.state.db,
        &project_id,
        &repo_id,
        &agent_id,
        &reviewer_id,
    )
    .await;

    let root: TaskResponse = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Root task",
            "description": "Root owns children"
        }),
        StatusCode::OK,
    )
    .await;
    let child: TaskResponse = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Subtask child",
            "description": "Subtask",
            "parent_task_id": root.id.clone()
        }),
        StatusCode::OK,
    )
    .await;

    let claimed_root: TaskResponse = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/claim", root.id),
        json!({ "agent_id": agent_id.clone(), "overrides": null }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(claimed_root.status, "in_progress");

    let managed_child = common::poll_task_status(&harness.app, &child.id, "in_progress").await;
    let moved_child: TransitionTaskResponse = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/transition", managed_child.id),
        json!({
            "status": "done",
            "version": managed_child.version,
            "reason": "manual move"
        }),
        StatusCode::OK,
    )
    .await;

    assert_eq!(moved_child.task.status, "done");
    assert_eq!(
        moved_child.task.parent_task_id.as_deref(),
        Some(root.id.as_str())
    );
}
