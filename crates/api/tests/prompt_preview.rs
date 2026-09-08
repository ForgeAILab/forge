#![allow(dead_code, clippy::assertions_on_constants)]

mod common;

use std::sync::Arc;

use api_types::{PromptPreviewResponse, TaskResponse, WorkflowTrigger};
use axum::http::{Method, StatusCode};
use db::{ExecutionRepo, PageRequest, ProjectRepo, SortBy, SortOrder, TaskRepo};
use serde_json::json;
use services::workflow::{
    default_roles,
    dispatch::{
        build_effective_prompt, dispatch_intent_from_workflow_dispatch, effective_prompt_selection,
        loader::load_agent_dispatch_context,
    },
    engine::WorkflowEngine,
};

#[tokio::test]
async fn prompt_preview_creates_no_execution_and_changes_no_task_state() {
    let workspace_root = common::TestDir::new("forge-prompt-preview-workspaces");
    let repo_root = common::TestDir::new("forge-prompt-preview-repo");
    let repo_path = common::setup_git_repo(repo_root.path());
    let harness = common::test_app(workspace_root.path(), "prompt-preview-state").await;
    let (project_id, _repo_id) =
        common::create_project_and_repo(&harness.app, "Prompt Preview", &repo_path).await;
    let task = create_task(&harness.app, &project_id).await;

    let before: TaskResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", task.id),
        StatusCode::OK,
    )
    .await;
    let before_execution_count = execution_count(&harness.state.db, &task.id).await;

    let preview: PromptPreviewResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/prompt-preview?role=coder", task.id),
        StatusCode::OK,
    )
    .await;

    assert!(!preview.system.trim().is_empty());
    assert!(!preview.user.trim().is_empty());

    let after: TaskResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", task.id),
        StatusCode::OK,
    )
    .await;
    let after_execution_count = execution_count(&harness.state.db, &task.id).await;

    assert_eq!(after.status, before.status);
    assert_eq!(after.version, before.version);
    assert_eq!(after_execution_count, before_execution_count);
}

#[tokio::test]
async fn prompt_preview_matches_direct_effective_prompt_build() {
    let workspace_root = common::TestDir::new("forge-prompt-preview-workspaces");
    let repo_root = common::TestDir::new("forge-prompt-preview-repo");
    let repo_path = common::setup_git_repo(repo_root.path());
    let harness = common::test_app(workspace_root.path(), "prompt-preview-equality").await;
    let (project_id, _repo_id) =
        common::create_project_and_repo(&harness.app, "Prompt Preview", &repo_path).await;
    let task = create_task(&harness.app, &project_id).await;

    let preview: PromptPreviewResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!(
            "/api/v1/tasks/{}/prompt-preview?role=planner&trigger=accept",
            task.id
        ),
        StatusCode::OK,
    )
    .await;

    let expected =
        direct_prompt_for_accept_to_planning(Arc::clone(&harness.state.db), &task.id).await;
    assert_eq!(preview.system, expected.system);
    assert_eq!(preview.user, expected.user);
    let expected_tools = if expected.tools.is_empty() {
        None
    } else {
        Some(expected.tools)
    };
    assert_eq!(preview.tools, expected_tools);
}

async fn create_task(app: &axum::Router, project_id: &str) -> TaskResponse {
    common::json_request(
        app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({
            "title": "Preview prompt",
            "description": "Implement prompt preview without dispatching"
        }),
        StatusCode::OK,
    )
    .await
}

async fn execution_count(db: &db::SqliteDb, task_id: &str) -> usize {
    let page = ExecutionRepo::list_by_task(
        db,
        task_id,
        PageRequest {
            cursor: None,
            limit: 100,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions list");
    page.items.len()
}

async fn direct_prompt_for_accept_to_planning(
    db: Arc<db::SqliteDb>,
    task_id: &str,
) -> services::workflow::AgentPrompt {
    let task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let project = ProjectRepo::get_by_id(&*db, &task.project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    let workflow = WorkflowEngine::resolve_workflow_for_task(
        &task,
        &project.workflow_definition,
        &api_types::Actor::system(api_types::SystemComponent::General),
    );
    let current_state = workflow
        .states
        .iter()
        .find(|state| state.name == task.status)
        .expect("current state exists");
    let trigger_definition = current_state
        .triggers
        .get(&WorkflowTrigger::Accept)
        .expect("todo accepts into planning");
    let target_state = workflow
        .states
        .iter()
        .find(|state| state.name == trigger_definition.to)
        .expect("target state exists");
    let trigger_dispatch =
        dispatch_intent_from_workflow_dispatch(trigger_definition.dispatch.as_ref());
    let state_dispatch = dispatch_intent_from_workflow_dispatch(target_state.dispatch.as_ref());
    let selection = effective_prompt_selection(
        default_roles::PLANNER,
        trigger_dispatch.as_ref(),
        state_dispatch.as_ref(),
    );
    let dispatch_ctx = load_agent_dispatch_context(
        Arc::clone(&db),
        task_id,
        default_roles::PLANNER,
        &target_state.name,
        target_state.config.clone(),
        Some(selection.execution_policy.as_str()),
        &workflow,
    )
    .await
    .expect("dispatch context loads");
    let (mut prompt, _selection) = build_effective_prompt(
        &dispatch_ctx,
        trigger_dispatch.as_ref(),
        state_dispatch.as_ref(),
    );
    let context = review::contract::load_context(&db, task_id)
        .await
        .expect("governing context loads");
    prompt
        .user
        .push_str(&review::contract::governing_prompt(&context));
    prompt
}
