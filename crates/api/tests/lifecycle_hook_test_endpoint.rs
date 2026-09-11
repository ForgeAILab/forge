#![allow(dead_code, clippy::assertions_on_constants)]
mod common;

use axum::http::{Method, StatusCode};
use serde_json::{json, Value};

#[tokio::test]
async fn project_hook_test_endpoint_returns_debug_fields_without_launching_execution() {
    let workspace_root = common::TestDir::new("hook-test-endpoint");
    let harness = common::test_app(workspace_root.path(), "hook-test-endpoint").await;
    let repo_path = common::setup_git_repo(workspace_root.path());
    let (project_id, repo_id) =
        common::create_project_and_repo(&harness.app, "Hook Test Project", &repo_path).await;
    let project: Value = common::json_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}"),
        json!(null),
        StatusCode::OK,
    )
    .await;

    let update_body = json!({
      "version": project["version"],
      "settings": {
        "lifecycle_hooks": {
          "before_work": [{
            "type": "script",
            "command": "echo test-out && echo test-err >&2",
            "timeout_seconds": 5,
            "blocking": false
          }]
        }
      }
    });
    let _: Value = common::json_request(
        &harness.app,
        Method::PATCH,
        &format!("/api/v1/projects/{project_id}"),
        update_body,
        StatusCode::OK,
    )
    .await;
    let task: Value = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Hook Test Task" }),
        StatusCode::OK,
    )
    .await;
    let task_id = task["id"].as_str().expect("task id").to_owned();

    let result: Value = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/hooks/test"),
        json!({
          "task_id": task_id,
          "event": "before_work",
          "hook_index": 0
        }),
        StatusCode::OK,
    )
    .await;

    assert_eq!(result["status"], json!("success"));
    assert_eq!(result["exit_code"], json!(0));
    assert_eq!(result["timeout"], json!(false));
    assert!(result["stdout"]
        .as_str()
        .unwrap_or_default()
        .contains("test-out"));
    assert!(result["stderr"]
        .as_str()
        .unwrap_or_default()
        .contains("test-err"));
    assert!(result["duration_ms"].as_u64().is_some());
    assert!(result["working_dir"].as_str().unwrap_or_default().len() > 1);
    assert!(result["environment_preview"].is_object());
    assert!(result.get("hook_log_path").is_some());

    let executions = db::ExecutionRepo::list_by_task(
        &*harness.state.db,
        task["id"].as_str().expect("task id"),
        db::PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: db::SortBy::CreatedAt,
            sort_order: db::SortOrder::Desc,
        },
    )
    .await
    .expect("list executions");
    assert_eq!(
        executions.items.len(),
        0,
        "hook test must not launch execution"
    );

    let refreshed = db::TaskRepo::get_by_id(&*harness.state.db, &task_id, false)
        .await
        .expect("task fetch")
        .expect("task exists");
    assert_eq!(refreshed.status, "todo");
    let project = db::ProjectRepo::get_by_id(&*harness.state.db, &refreshed.project_id)
        .await
        .expect("project fetch")
        .expect("project exists");
    assert_eq!(project.primary_repo_id.as_deref(), Some(repo_id.as_str()));
}
