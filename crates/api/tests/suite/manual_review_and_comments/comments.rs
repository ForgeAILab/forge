//! Task comment creation, deletion and review-rejection system comments.

use super::*;

// ── Test 13.6: comment CRUD ──

#[tokio::test]
async fn comment_create_list_delete() {
    let workspace_root = common::TestDir::new("forge-comments-crud");
    let harness = test_app(workspace_root.path()).await;
    let (repo_path, _) = setup_git_repo(workspace_root.path(), "comments");

    let project: ProjectResponse = json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Comment Test" }),
        StatusCode::OK,
    )
    .await;

    let _repo: RepoResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{}/repos", project.id),
        json!({
            "name": "repo",
            "local_path": repo_path.to_string_lossy(),
            "remote_url": repo_path.to_string_lossy(),
            "default_branch": "main"
        }),
        StatusCode::OK,
    )
    .await;

    let task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{}/tasks", project.id),
        json!({ "title": "Comment test task",
            "description": "test"
        }),
        StatusCode::OK,
    )
    .await;

    // Create a user comment
    let comment: CommentResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/comments", task.id),
        json!({ "content": "Hello world", "author_name": "Tester" }),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(comment.content, "Hello world");
    assert_eq!(comment.author_name, "Tester");
    assert_eq!(comment.author_type, AuthorType::User);

    // List comments
    let listed: PaginatedResponse<CommentResponse> = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/comments", task.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(listed.items.len(), 1);
    assert_eq!(listed.items[0].content, "Hello world");

    // Delete user comment
    let delete_response = raw_request(
        &harness.app,
        Method::DELETE,
        &format!("/api/v1/comments/{}", comment.id),
        json!(null),
    )
    .await;
    assert_eq!(delete_response.status(), StatusCode::NO_CONTENT);

    // Verify it's gone
    let listed: PaginatedResponse<CommentResponse> = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/comments", task.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(listed.items.len(), 0);
}

#[tokio::test]
async fn system_comments_cannot_be_deleted() {
    let workspace_root = common::TestDir::new("forge-comments-system-delete");
    let harness = test_app(workspace_root.path()).await;

    let task_id = seed_task_with_system_comment(&harness.state.db).await;

    let listed: PaginatedResponse<CommentResponse> = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{task_id}/comments"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(listed.items.len(), 1);
    let system_comment_id = &listed.items[0].id;

    let delete_response = raw_request(
        &harness.app,
        Method::DELETE,
        &format!("/api/v1/comments/{system_comment_id}"),
        json!(null),
    )
    .await;
    assert_eq!(delete_response.status(), StatusCode::FORBIDDEN);
}

// ── Test 13.7: system comments auto-created ──

#[tokio::test]
async fn reject_review_creates_system_comment_with_reason() {
    let workspace_root = common::TestDir::new("forge-sys-comment-reject");
    let harness = test_app(workspace_root.path()).await;

    let (task_id, _review_id) = seed_awaiting_human_review(
        &harness.state.db,
        r#"{"retry_budgets":{"review":3,"merge_fix":1}}"#,
        workspace_root.path(),
    )
    .await;

    let _result: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/actions"),
        json!({ "version": common::task_action_version(&harness.app, &format!("/api/v1/tasks/{task_id}/actions")).await, "action": {"verb":"send_back","guidance":"needs refactoring"}}),
        StatusCode::OK,
    )
    .await;

    let comments: PaginatedResponse<CommentResponse> = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{task_id}/comments"),
        StatusCode::OK,
    )
    .await;

    let system_comments: Vec<_> = comments
        .items
        .iter()
        .filter(|c| c.author_type == AuthorType::System)
        .collect();
    assert!(
        system_comments.iter().any(
            |c| c.content.contains("Review failed") && c.content.contains("needs refactoring")
        ),
        "expected system comment with rejection reason, got: {:?}",
        system_comments
            .iter()
            .map(|c| &c.content)
            .collect::<Vec<_>>()
    );
}
