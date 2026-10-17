#![allow(dead_code, clippy::assertions_on_constants)]
use crate::common;

use api_types::{AgentResponse, ProjectResponse};
use axum::http::{Method, StatusCode};
use common::*;
use serde_json::json;

#[tokio::test]
async fn agent_pause_resume_idempotent() {
    let workspace_dir = TestDir::new("pause-test-workspace");
    let harness = test_app(workspace_dir.path(), "pause-test").await;
    let app = &harness.app;
    let (agent_id, _agent_b_id) =
        create_shell_agents(app, &harness.state, workspace_dir.path(), "pause-test").await;

    let agent: AgentResponse = empty_request(
        app,
        Method::GET,
        &format!("/api/v1/agents/{agent_id}"),
        StatusCode::OK,
    )
    .await;
    assert!(!agent.paused);
    assert_eq!(agent.effective_status.as_deref(), Some("active"));

    let paused: AgentResponse = empty_request(
        app,
        Method::POST,
        &format!("/api/v1/agents/{agent_id}/pause"),
        StatusCode::OK,
    )
    .await;
    assert!(paused.paused);
    assert_eq!(paused.effective_status.as_deref(), Some("paused"));

    let paused_again: AgentResponse = empty_request(
        app,
        Method::POST,
        &format!("/api/v1/agents/{agent_id}/pause"),
        StatusCode::OK,
    )
    .await;
    assert!(paused_again.paused);
    assert_eq!(paused_again.effective_status.as_deref(), Some("paused"));

    let resumed: AgentResponse = empty_request(
        app,
        Method::POST,
        &format!("/api/v1/agents/{agent_id}/resume"),
        StatusCode::OK,
    )
    .await;
    assert!(!resumed.paused);
    assert_ne!(resumed.effective_status.as_deref(), Some("paused"));

    let resumed_again: AgentResponse = empty_request(
        app,
        Method::POST,
        &format!("/api/v1/agents/{agent_id}/resume"),
        StatusCode::OK,
    )
    .await;
    assert!(!resumed_again.paused);
    assert_ne!(resumed_again.effective_status.as_deref(), Some("paused"));
}

#[tokio::test]
async fn project_pause_resume_idempotent() {
    let workspace_dir = TestDir::new("pause-test-workspace");
    let harness = test_app(workspace_dir.path(), "pause-test").await;
    let app = &harness.app;

    let project: ProjectResponse = json_request(
        app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "test" }),
        StatusCode::OK,
    )
    .await;
    let project_id = project.id;

    let project: ProjectResponse = empty_request(
        app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}"),
        StatusCode::OK,
    )
    .await;
    assert!(!project.paused);
    assert_eq!(project.paused_at, None);

    let paused: ProjectResponse = empty_request(
        app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/pause"),
        StatusCode::OK,
    )
    .await;
    assert!(paused.paused);
    let paused_at = paused
        .paused_at
        .clone()
        .expect("paused project has paused_at timestamp");
    assert!(!paused_at.is_empty());

    let paused_again: ProjectResponse = empty_request(
        app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/pause"),
        StatusCode::OK,
    )
    .await;
    assert!(paused_again.paused);
    assert_eq!(paused_again.paused_at.as_deref(), Some(paused_at.as_str()));

    let resumed: ProjectResponse = empty_request(
        app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/resume"),
        StatusCode::OK,
    )
    .await;
    assert!(!resumed.paused);
    assert_eq!(resumed.paused_at, None);

    let resumed_again: ProjectResponse = empty_request(
        app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/resume"),
        StatusCode::OK,
    )
    .await;
    assert!(!resumed_again.paused);
    assert_eq!(resumed_again.paused_at, None);
}
