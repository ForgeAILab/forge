use crate::common;

use api_types::{ErrorResponse, ExternalLinkResponse, IntegrationResponse, TaskResponse};
use axum::http::{Method, StatusCode};
use common::*;
use serde_json::json;

#[tokio::test]
async fn test_manual_link_existing_task_to_issue() {
    let repo_dir = TestDir::new("external-link-manual-repo");
    let repo_path = setup_git_repo(repo_dir.path());
    let workspace_root = TestDir::new("external-link-manual-workspaces");
    let harness = test_app(workspace_root.path(), "external-link-manual").await;
    let (project_id, _repo_id) =
        create_project_and_repo(&harness.app, "External Link Manual", &repo_path).await;

    let task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "My task", "description": "desc" }),
        StatusCode::OK,
    )
    .await;

    let integration: IntegrationResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/integration"),
        json!({
            "platform": "github",
            "base_url": "https://api.github.com",
            "owner": "org",
            "repo": "repo",
            "token_secret_ref": "GITHUB_TOKEN"
        }),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(integration.platform, "github");

    let link: ExternalLinkResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/external-links", task.id),
        json!({ "remote_issue_number": 42 }),
        StatusCode::CREATED,
    )
    .await;

    assert_eq!(link.task_id, task.id);
    assert_eq!(link.integration_id, integration.id);
    assert_eq!(link.global_id, "github:org/repo#42");
    assert_eq!(link.remote_url, "https://github.com/org/repo/issues/42");
    assert_eq!(link.remote_issue_number, 42);

    let task_detail: TaskResponse = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", task.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(task_detail.title, task.title);
    assert_eq!(task_detail.description, task.description);
    assert_eq!(task_detail.status, task.status);
    assert_eq!(task_detail.external_issue_number, Some(42));
    assert_eq!(
        task_detail.external_issue_url.as_deref(),
        Some("https://github.com/org/repo/issues/42")
    );

    let second_link: ExternalLinkResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/external-links", task.id),
        json!({ "remote_issue_number": 43 }),
        StatusCode::CREATED,
    )
    .await;

    let links: Vec<ExternalLinkResponse> = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/external-links", task.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(links.len(), 2);
    assert!(links.iter().any(|listed| listed.id == link.id));
    assert!(links.iter().any(|listed| listed.id == second_link.id));
}

#[tokio::test]
async fn test_duplicate_manual_link_returns_409() {
    let repo_dir = TestDir::new("external-link-duplicate-repo");
    let repo_path = setup_git_repo(repo_dir.path());
    let workspace_root = TestDir::new("external-link-duplicate-workspaces");
    let harness = test_app(workspace_root.path(), "external-link-duplicate").await;
    let (project_id, _repo_id) =
        create_project_and_repo(&harness.app, "External Link Duplicate", &repo_path).await;

    let task1: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "First task", "description": "first" }),
        StatusCode::OK,
    )
    .await;
    let task2: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Second task", "description": "second" }),
        StatusCode::OK,
    )
    .await;

    let _: IntegrationResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/integration"),
        json!({
            "platform": "github",
            "base_url": "https://api.github.com",
            "owner": "org",
            "repo": "repo",
            "token_secret_ref": "GITHUB_TOKEN"
        }),
        StatusCode::CREATED,
    )
    .await;

    let _: ExternalLinkResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/external-links", task1.id),
        json!({ "remote_issue_number": 42 }),
        StatusCode::CREATED,
    )
    .await;

    let response = raw_json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/external-links", task2.id),
        json!({ "remote_issue_number": 42 }),
    )
    .await;
    let error: ErrorResponse = parse_response(response, StatusCode::CONFLICT).await;
    assert_eq!(error.code, "duplicate_external_link");
}

#[tokio::test]
async fn test_unlink_external_link() {
    let repo_dir = TestDir::new("external-link-unlink-repo");
    let repo_path = setup_git_repo(repo_dir.path());
    let workspace_root = TestDir::new("external-link-unlink-workspaces");
    let harness = test_app(workspace_root.path(), "external-link-unlink").await;
    let (project_id, _repo_id) =
        create_project_and_repo(&harness.app, "External Link Unlink", &repo_path).await;

    let task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Linked task", "description": "linked" }),
        StatusCode::OK,
    )
    .await;

    let _: IntegrationResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/integration"),
        json!({
            "platform": "github",
            "base_url": "https://api.github.com",
            "owner": "org",
            "repo": "repo",
            "token_secret_ref": "GITHUB_TOKEN"
        }),
        StatusCode::CREATED,
    )
    .await;

    let link: ExternalLinkResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/external-links", task.id),
        json!({ "remote_issue_number": 42 }),
        StatusCode::CREATED,
    )
    .await;

    let response = raw_empty_request(
        &harness.app,
        Method::DELETE,
        &format!("/api/v1/tasks/{}/external-links/{}", task.id, link.id),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let task_detail: TaskResponse = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", task.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(task_detail.external_issue_number, None);
    assert_eq!(task_detail.external_issue_url, None);

    let links: Vec<ExternalLinkResponse> = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/external-links", task.id),
        StatusCode::OK,
    )
    .await;
    assert!(links.is_empty());
}

#[tokio::test]
async fn test_manual_link_no_integration_returns_404() {
    let repo_dir = TestDir::new("external-link-no-integration-repo");
    let repo_path = setup_git_repo(repo_dir.path());
    let workspace_root = TestDir::new("external-link-no-integration-workspaces");
    let harness = test_app(workspace_root.path(), "external-link-no-integration").await;
    let (project_id, _repo_id) =
        create_project_and_repo(&harness.app, "External Link No Integration", &repo_path).await;

    let task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Unlinked task", "description": "unlinked" }),
        StatusCode::OK,
    )
    .await;

    let response = raw_json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/external-links", task.id),
        json!({ "remote_issue_number": 42 }),
    )
    .await;
    let _: ErrorResponse = parse_response(response, StatusCode::NOT_FOUND).await;
}

#[tokio::test]
async fn test_gitea_manual_link_url_derivation() {
    let repo_dir = TestDir::new("external-link-gitea-repo");
    let repo_path = setup_git_repo(repo_dir.path());
    let workspace_root = TestDir::new("external-link-gitea-workspaces");
    let harness = test_app(workspace_root.path(), "external-link-gitea").await;
    let (project_id, _repo_id) =
        create_project_and_repo(&harness.app, "External Link Gitea", &repo_path).await;

    let task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Gitea task", "description": "gitea" }),
        StatusCode::OK,
    )
    .await;

    let _: IntegrationResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/integration"),
        json!({
            "platform": "gitea",
            "base_url": "https://gitea.example.com",
            "owner": "myorg",
            "repo": "myrepo",
            "token_secret_ref": "GITEA_TOKEN"
        }),
        StatusCode::CREATED,
    )
    .await;

    let link: ExternalLinkResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/external-links", task.id),
        json!({ "remote_issue_number": 7 }),
        StatusCode::CREATED,
    )
    .await;

    assert_eq!(
        link.remote_url,
        "https://gitea.example.com/myorg/myrepo/issues/7"
    );
    assert_eq!(link.global_id, "gitea:gitea.example.com:myorg/myrepo#7");
}

#[tokio::test]
async fn test_sync_endpoint_handles_missing_token_gracefully() {
    let repo_dir = TestDir::new("external-link-sync-missing-token-repo");
    let repo_path = setup_git_repo(repo_dir.path());
    let workspace_root = TestDir::new("external-link-sync-missing-token-workspaces");
    let harness = test_app(workspace_root.path(), "external-link-sync-missing-token").await;
    let (project_id, _repo_id) =
        create_project_and_repo(&harness.app, "External Link Sync Missing Token", &repo_path).await;

    let token_secret_ref = format!("FORGE_TEST_MISSING_TOKEN_{}", uuid::Uuid::new_v4().simple());
    let _: IntegrationResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/integration"),
        json!({
            "platform": "github",
            "base_url": "https://api.github.com",
            "owner": "org",
            "repo": "repo",
            "token_secret_ref": token_secret_ref
        }),
        StatusCode::CREATED,
    )
    .await;

    let response = raw_empty_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/integration/sync"),
    )
    .await;
    assert_ne!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn test_dedup_manual_links_no_duplicate_tasks() {
    let repo_dir = TestDir::new("external-link-dedup-manual-repo");
    let repo_path = setup_git_repo(repo_dir.path());
    let workspace_root = TestDir::new("external-link-dedup-manual-workspaces");
    let harness = test_app(workspace_root.path(), "external-link-dedup-manual").await;
    let (project_id, _repo_id) =
        create_project_and_repo(&harness.app, "External Link Dedup Manual", &repo_path).await;

    let task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Dedup task", "description": "dedup" }),
        StatusCode::OK,
    )
    .await;

    let _: IntegrationResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/integration"),
        json!({
            "platform": "github",
            "base_url": "https://api.github.com",
            "owner": "org",
            "repo": "repo",
            "token_secret_ref": "GITHUB_TOKEN"
        }),
        StatusCode::CREATED,
    )
    .await;

    let _: ExternalLinkResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/external-links", task.id),
        json!({ "remote_issue_number": 42 }),
        StatusCode::CREATED,
    )
    .await;

    let response = raw_json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/external-links", task.id),
        json!({ "remote_issue_number": 42 }),
    )
    .await;
    let _: ErrorResponse = parse_response(response, StatusCode::CONFLICT).await;

    let links: Vec<ExternalLinkResponse> = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/external-links", task.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(links.len(), 1);
}

#[tokio::test]
async fn test_external_link_appears_in_task_detail_and_list() {
    let repo_dir = TestDir::new("external-link-visibility-repo");
    let repo_path = setup_git_repo(repo_dir.path());
    let workspace_root = TestDir::new("external-link-visibility-workspaces");
    let harness = test_app(workspace_root.path(), "external-link-visibility").await;
    let (project_id, _repo_id) =
        create_project_and_repo(&harness.app, "External Link Visibility", &repo_path).await;

    let task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Visible task", "description": "visible" }),
        StatusCode::OK,
    )
    .await;

    let task_detail: TaskResponse = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", task.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(task_detail.external_issue_number, None);
    assert_eq!(task_detail.external_issue_url, None);

    let tasks: serde_json::Value = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}/tasks"),
        StatusCode::OK,
    )
    .await;
    let task_item = tasks
        .get("items")
        .and_then(|items| items.as_array())
        .expect("task list has items")
        .iter()
        .find(|item| item.get("id").and_then(|id| id.as_str()) == Some(task.id.as_str()))
        .expect("task appears in list");
    assert_eq!(
        task_item.get("external_issue_number"),
        Some(&serde_json::Value::Null)
    );
    assert_eq!(
        task_item.get("external_issue_url"),
        Some(&serde_json::Value::Null)
    );

    let _: IntegrationResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/integration"),
        json!({
            "platform": "github",
            "base_url": "https://api.github.com",
            "owner": "org",
            "repo": "repo",
            "token_secret_ref": "GITHUB_TOKEN"
        }),
        StatusCode::CREATED,
    )
    .await;

    let _: ExternalLinkResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/external-links", task.id),
        json!({ "remote_issue_number": 7 }),
        StatusCode::CREATED,
    )
    .await;

    let task_detail: TaskResponse = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", task.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(task_detail.external_issue_number, Some(7));
    assert!(task_detail.external_issue_url.is_some());

    let tasks: serde_json::Value = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}/tasks"),
        StatusCode::OK,
    )
    .await;
    let task_item = tasks
        .get("items")
        .and_then(|items| items.as_array())
        .expect("task list has items")
        .iter()
        .find(|item| item.get("id").and_then(|id| id.as_str()) == Some(task.id.as_str()))
        .expect("task appears in list");
    assert_eq!(
        task_item
            .get("external_issue_number")
            .and_then(|issue_number| issue_number.as_i64()),
        Some(7)
    );
    assert!(task_item
        .get("external_issue_url")
        .and_then(|issue_url| issue_url.as_str())
        .is_some());
}

#[tokio::test]
async fn test_integration_isolation_between_projects() {
    let repo_dir_a = TestDir::new("external-link-isolation-repo-a");
    let repo_path_a = setup_git_repo(repo_dir_a.path());
    let repo_dir_b = TestDir::new("external-link-isolation-repo-b");
    let repo_path_b = setup_git_repo(repo_dir_b.path());
    let workspace_root = TestDir::new("external-link-isolation-workspaces");
    let harness = test_app(workspace_root.path(), "external-link-isolation").await;
    let (project_id_a, _repo_id_a) =
        create_project_and_repo(&harness.app, "External Link Isolation A", &repo_path_a).await;
    let (project_id_b, _repo_id_b) =
        create_project_and_repo(&harness.app, "External Link Isolation B", &repo_path_b).await;

    let integration: IntegrationResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id_a}/integration"),
        json!({
            "platform": "github",
            "base_url": "https://api.github.com",
            "owner": "org",
            "repo": "repo",
            "token_secret_ref": "GITHUB_TOKEN"
        }),
        StatusCode::CREATED,
    )
    .await;

    let response = raw_empty_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id_b}/integration/sync"),
    )
    .await;
    let _: ErrorResponse = parse_response(response, StatusCode::NOT_FOUND).await;

    let fetched: IntegrationResponse = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{project_id_a}/integration"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(fetched.id, integration.id);
    assert_eq!(fetched.project_id, project_id_a);
    assert_eq!(fetched.platform, "github");
    assert_eq!(fetched.owner, "org");
    assert_eq!(fetched.repo, "repo");
}
