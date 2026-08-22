#![allow(dead_code, clippy::assertions_on_constants)]
mod common;

use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

use api::{build_router, AppState};
use api_types::{
    AgentAvailabilityResponse, AgentResponse, ErrorResponse, MoveTaskResponse, PaginatedResponse,
    ProjectResponse, RepoResponse, TaskDependency, TaskResponse, TasksResponse,
};
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tower::ServiceExt;

#[tokio::test]
async fn forge_mvp_rest_api_flow() {
    let (app, state) = test_app_with_state().await;

    let project: ProjectResponse = json_request(
        &app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "E2E Test" }),
        StatusCode::OK,
    )
    .await;
    let project_id = project.id;

    let repo_dir = TestDir::new("forge-e2e-repo");
    let repo_path = setup_git_repo(repo_dir.path());
    let default_branch = run_git(&repo_path, &["symbolic-ref", "--short", "HEAD"]);
    let repo: RepoResponse = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/repos"),
        json!({
            "name": "forge",
            "local_path": repo_path.to_string_lossy(),
            "remote_url": repo_path.to_string_lossy(),
            "default_branch": default_branch
        }),
        StatusCode::OK,
    )
    .await;
    let repo_id = repo.id;

    let daemon_id = existing_daemon_id(&app).await;
    let agent: AgentResponse = json_request(
        &app,
        Method::POST,
        "/api/v1/agents",
        json!({ "name": "shell-agent", "executor_type": "shell", "daemon_id": daemon_id }),
        StatusCode::OK,
    )
    .await;
    let agent_id = agent.id;
    common::configure_execution_test_setup(&state.db, &project_id, &repo_id, &agent_id, &agent_id)
        .await;

    let created_task: TaskResponse = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        // The shell executor uses the task description as the command. Keep it
        // running long enough for the cancellation request to be deterministic.
        json!({ "title": "E2E test task", "description": "sleep 5" }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(created_task.status, "todo".to_owned());
    assert_eq!(created_task.version, 1);
    let task_id = created_task.id;

    let tasks: PaginatedResponse<TaskResponse> = empty_request(
        &app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}/tasks"),
        StatusCode::OK,
    )
    .await;
    assert!(tasks.items.iter().any(|task| task.id == task_id));

    let claimed_task: TaskResponse = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/claim"),
        json!({ "agent_id": agent_id, "overrides": null }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(claimed_task.status, "in_progress".to_owned());
    assert_eq!(claimed_task.version, 2);

    let cancelled_task: TaskResponse = empty_request(
        &app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/cancel"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(cancelled_task.status, "cancelled".to_owned());
    assert_eq!(cancelled_task.version, 3);

    let terminal_error = raw_json_request(
        &app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/transition"),
        json!({ "status": "in_progress", "version": cancelled_task.version }),
    )
    .await;
    assert_eq!(terminal_error.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let delete_response =
        raw_empty_request(&app, Method::DELETE, &format!("/api/v1/tasks/{task_id}")).await;
    assert_eq!(delete_response.status(), StatusCode::NO_CONTENT);

    let tasks_after_delete: PaginatedResponse<TaskResponse> = empty_request(
        &app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}/tasks"),
        StatusCode::OK,
    )
    .await;
    assert!(!tasks_after_delete
        .items
        .iter()
        .any(|task| task.id == task_id));

    let second_task: TaskResponse = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "E2E cancellable task" }),
        StatusCode::OK,
    )
    .await;
    let second_task_id = second_task.id;

    let cancelled_task: TaskResponse = empty_request(
        &app,
        Method::POST,
        &format!("/api/v1/tasks/{second_task_id}/cancel"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(cancelled_task.status, "cancelled".to_owned());

    let cancelled_again: TaskResponse = empty_request(
        &app,
        Method::POST,
        &format!("/api/v1/tasks/{second_task_id}/cancel"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(cancelled_again.status, "cancelled".to_owned());
    assert_eq!(cancelled_again.version, cancelled_task.version);
}

#[tokio::test]
async fn add_dependency_succeeds() {
    let app = test_app().await;
    let (project_id, _repo_id, _repo_dir) = create_project_and_repo(&app).await;
    let (task_id, depends_on_id) = create_task_pair(&app, &project_id).await;

    let response = raw_json_request(
        &app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/dependencies"),
        json!({ "depends_on_id": depends_on_id }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn add_dependency_cycle_returns_unprocessable_entity() {
    let app = test_app().await;
    let (project_id, _repo_id, _repo_dir) = create_project_and_repo(&app).await;
    let (task_id, depends_on_id) = create_task_pair(&app, &project_id).await;

    let response = raw_json_request(
        &app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/dependencies"),
        json!({ "depends_on_id": depends_on_id }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let cycle_response = raw_json_request(
        &app,
        Method::POST,
        &format!("/api/v1/tasks/{depends_on_id}/dependencies"),
        json!({ "depends_on_id": task_id }),
    )
    .await;
    assert_eq!(cycle_response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn remove_dependency_succeeds() {
    let app = test_app().await;
    let (project_id, _repo_id, _repo_dir) = create_project_and_repo(&app).await;
    let (task_id, depends_on_id) = create_task_pair(&app, &project_id).await;

    let response = raw_json_request(
        &app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/dependencies"),
        json!({ "depends_on_id": depends_on_id }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = raw_empty_request(
        &app,
        Method::DELETE,
        &format!("/api/v1/tasks/{task_id}/dependencies/{depends_on_id}"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let dependencies: Vec<TaskDependency> = empty_request(
        &app,
        Method::GET,
        &format!("/api/v1/tasks/{task_id}/dependencies"),
        StatusCode::OK,
    )
    .await;
    assert!(dependencies.is_empty());
}

#[tokio::test]
async fn move_task_endpoint_updates_board_order_replays_and_reports_conflicts() {
    let app = test_app().await;
    let (project_id, _repo_id, _repo_dir) = create_project_and_repo(&app).await;
    let first: TaskResponse = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "First" }),
        StatusCode::OK,
    )
    .await;
    let second: TaskResponse = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Second" }),
        StatusCode::OK,
    )
    .await;
    let third: TaskResponse = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Third" }),
        StatusCode::OK,
    )
    .await;
    let _: Value = json_request(
        &app,
        Method::PUT,
        &format!("/api/v1/tasks/{}/roles/coder", third.id),
        json!({ "assignee_type": "user", "assignee_id": "test-user-id" }),
        StatusCode::OK,
    )
    .await;

    let initial_page: TasksResponse = empty_request(
        &app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}/tasks"),
        StatusCode::OK,
    )
    .await;
    let operation_id = uuid::Uuid::new_v4().to_string();
    let move_body = json!({
        "operation_id": operation_id,
        "task_version": third.version,
        "board_revision": initial_page.board_revision,
        "target_status": third.status,
        "before_id": first.id,
        "after_id": second.id,
    });
    let response: MoveTaskResponse = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/tasks/{}/move", third.id),
        move_body.clone(),
        StatusCode::OK,
    )
    .await;

    assert_eq!(response.task.id, third.id);
    assert_eq!(response.operation_id, operation_id);
    assert!(response.board_revision > initial_page.board_revision);
    assert!((response.task.board_position - 1.5).abs() < 1e-9);
    assert!(response.task.role_assignments.iter().any(|assignment| {
        assignment.role_name == "coder" && assignment.assignee_id.as_deref() == Some("test-user-id")
    }));
    let replay: MoveTaskResponse = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/tasks/{}/move", third.id),
        move_body,
        StatusCode::OK,
    )
    .await;
    assert_eq!(replay.task.version, response.task.version);
    assert_eq!(replay.board_revision, response.board_revision);

    let operation_conflict = raw_json_request(
        &app,
        Method::POST,
        &format!("/api/v1/tasks/{}/move", third.id),
        json!({
            "operation_id": operation_id,
            "task_version": third.version,
            "board_revision": initial_page.board_revision,
            "target_status": third.status,
            "before_id": second.id,
            "after_id": null,
        }),
    )
    .await;
    let operation_error: ErrorResponse =
        parse_response(operation_conflict, StatusCode::CONFLICT).await;
    assert_eq!(operation_error.code, "operation_conflict");
    assert_eq!(
        operation_error.details,
        Some(json!({ "operation_id": operation_id }))
    );

    let version_conflict = raw_json_request(
        &app,
        Method::POST,
        &format!("/api/v1/tasks/{}/move", third.id),
        json!({
            "operation_id": uuid::Uuid::new_v4().to_string(),
            "task_version": third.version,
            "board_revision": response.board_revision,
            "target_status": third.status,
            "before_id": first.id,
            "after_id": second.id,
        }),
    )
    .await;
    let version_error: ErrorResponse = parse_response(version_conflict, StatusCode::CONFLICT).await;
    assert_eq!(version_error.code, "version_conflict");
    assert_eq!(
        version_error.details,
        Some(json!({
            "expected_task_version": third.version,
            "actual_task_version": response.task.version,
        }))
    );

    let board_conflict = raw_json_request(
        &app,
        Method::POST,
        &format!("/api/v1/tasks/{}/move", third.id),
        json!({
            "operation_id": uuid::Uuid::new_v4().to_string(),
            "task_version": response.task.version,
            "board_revision": initial_page.board_revision,
            "target_status": third.status,
            "before_id": first.id,
            "after_id": second.id,
        }),
    )
    .await;
    let board_error: ErrorResponse = parse_response(board_conflict, StatusCode::CONFLICT).await;
    assert_eq!(board_error.code, "board_revision_conflict");
    assert_eq!(
        board_error.details,
        Some(json!({
            "expected_board_revision": initial_page.board_revision,
            "actual_board_revision": response.board_revision,
        }))
    );

    let tasks: TasksResponse = empty_request(
        &app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}/tasks"),
        StatusCode::OK,
    )
    .await;
    let ids = tasks
        .items
        .iter()
        .map(|task| task.id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        ids,
        vec![first.id.as_str(), third.id.as_str(), second.id.as_str()]
    );

    let removed_endpoint = raw_json_request(
        &app,
        Method::PUT,
        &format!("/api/v1/tasks/{}/position", third.id),
        json!({ "before_id": first.id, "after_id": second.id }),
    )
    .await;
    assert_eq!(removed_endpoint.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn project_task_pages_include_revision_tokens_for_pagination() {
    let app = test_app().await;
    let (project_id, _repo_id, _repo_dir) = create_project_and_repo(&app).await;
    for title in ["First", "Second", "Third"] {
        let _: TaskResponse = json_request(
            &app,
            Method::POST,
            &format!("/api/v1/projects/{project_id}/tasks"),
            json!({ "title": title }),
            StatusCode::OK,
        )
        .await;
    }

    let first_page: TasksResponse = empty_request(
        &app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}/tasks?limit=1"),
        StatusCode::OK,
    )
    .await;
    assert!(first_page.has_more);
    let cursor = first_page.next_cursor.clone().expect("next page cursor");
    let second_page: TasksResponse = empty_request(
        &app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}/tasks?limit=1&cursor={cursor}"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(second_page.board_revision, first_page.board_revision);

    let _: TaskResponse = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Concurrent insert" }),
        StatusCode::OK,
    )
    .await;
    let changed_page: TasksResponse = empty_request(
        &app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}/tasks?limit=1&cursor={cursor}"),
        StatusCode::OK,
    )
    .await;
    assert!(changed_page.board_revision > first_page.board_revision);
}

#[tokio::test]
async fn list_dependencies_returns_task_dependencies() {
    let app = test_app().await;
    let (project_id, _repo_id, _repo_dir) = create_project_and_repo(&app).await;
    let (task_id, depends_on_id) = create_task_pair(&app, &project_id).await;

    let response = raw_json_request(
        &app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/dependencies"),
        json!({ "depends_on_id": depends_on_id }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let dependencies: Vec<TaskDependency> = empty_request(
        &app,
        Method::GET,
        &format!("/api/v1/tasks/{task_id}/dependencies"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(dependencies.len(), 1);
    assert_eq!(dependencies[0].task_id, task_id);
    assert_eq!(dependencies[0].depends_on_id, depends_on_id);
}

#[tokio::test]
async fn claim_blocked_by_dependency_gate_returns_conflict() {
    let app = test_app().await;
    let (project_id, _repo_id, _repo_dir) = create_project_and_repo(&app).await;
    let daemon_id = existing_daemon_id(&app).await;
    let agent: AgentResponse = json_request(
        &app,
        Method::POST,
        "/api/v1/agents",
        json!({ "name": "blocked-agent", "executor_type": "shell", "daemon_id": daemon_id }),
        StatusCode::OK,
    )
    .await;
    let (task_id, depends_on_id) = create_task_pair(&app, &project_id).await;

    let response = raw_json_request(
        &app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/dependencies"),
        json!({ "depends_on_id": depends_on_id }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = raw_json_request(
        &app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/claim"),
        json!({ "agent_id": agent.id, "overrides": null }),
    )
    .await;
    let error: ErrorResponse = parse_response(response, StatusCode::CONFLICT).await;
    assert_eq!(error.code, "dependency_gate");
}

#[tokio::test]
async fn agent_claim_succeeds() {
    let (app, state) = test_app_with_state().await;
    let (project_id, repo_id, _repo_dir) = create_project_and_repo(&app).await;
    let daemon_id = existing_daemon_id(&app).await;
    let agent: AgentResponse = json_request(
        &app,
        Method::POST,
        "/api/v1/agents",
        json!({ "name": "claim-agent", "executor_type": "shell", "daemon_id": daemon_id }),
        StatusCode::OK,
    )
    .await;
    common::configure_execution_test_setup(&state.db, &project_id, &repo_id, &agent.id, &agent.id)
        .await;
    let task: TaskResponse = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Agent claimed task" }),
        StatusCode::OK,
    )
    .await;

    let claimed: TaskResponse = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/tasks/{}/claim", task.id),
        json!({ "agent_id": agent.id.clone(), "overrides": null }),
        StatusCode::OK,
    )
    .await;

    assert_eq!(claimed.status, "in_progress".to_owned());
    assert_eq!(claimed.assignee_type.as_deref(), Some("agent"));
    assert!(claimed.role_assignments.iter().any(|assignment| {
        assignment.role_name == "coder"
            && assignment.assignee_type.as_deref() == Some("agent")
            && assignment.assignee_id.as_deref() == Some(agent.id.as_str())
    }));
    assert_eq!(claimed.assignee_id.as_deref(), Some(agent.id.as_str()));
}

#[tokio::test]
async fn shell_agent_availability_returns_active() {
    let app = test_app().await;
    let agent: AgentResponse = json_request(
        &app,
        Method::POST,
        "/api/v1/agents",
        json!({ "name": "availability-agent", "executor_type": "shell" }),
        StatusCode::OK,
    )
    .await;

    let availability: AgentAvailabilityResponse = empty_request(
        &app,
        Method::GET,
        &format!("/api/v1/agents/{}/availability", agent.id),
        StatusCode::OK,
    )
    .await;

    assert!(availability.available);
    assert_eq!(availability.effective_status, "active");
}

#[tokio::test]
async fn scoped_mcp_endpoint_creates_task_without_project_id_argument() {
    let app = test_app().await;
    let (project_id, _repo_id, _repo_dir) = create_project_and_repo(&app).await;
    let title = format!("Scoped MCP task {}", uuid::Uuid::new_v4());

    let response: Value = json_request(
        &app,
        Method::POST,
        &format!("/mcp?project_id={project_id}"),
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "forge_create_task",
                "arguments": {
                    "title": title,
                    "description": "created by scoped MCP endpoint e2e"
                }
            }
        }),
        StatusCode::OK,
    )
    .await;

    let text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("MCP result text is present");
    let task: Value = serde_json::from_str(text).expect("MCP result text is JSON");
    assert_eq!(task["project_id"], project_id);
    assert_eq!(task["title"], title);
    assert_eq!(task["description"], "created by scoped MCP endpoint e2e");

    let tasks: PaginatedResponse<TaskResponse> = empty_request(
        &app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}/tasks"),
        StatusCode::OK,
    )
    .await;
    assert!(tasks.items.iter().any(|task| task.title == title));
}

async fn test_app() -> Router {
    test_app_with_state().await.0
}

async fn test_app_with_state() -> (Router, AppState) {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");
    let db_instance = db::SqliteDb::new(pool);

    // Seed test user matching test_jwt() claims
    let now = db::now_rfc3339();
    db::UserRepo::create_user(
        &db_instance,
        &db::User {
            id: "test-user-id".to_owned(),
            email: "test@example.com".to_owned(),
            password_hash: "not-a-real-hash".to_owned(),
            display_name: Some("Test User".to_owned()),
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("user seeds");

    // Seed local host and runtime
    let daemon_id = db::new_uuid_v4();
    db::DaemonRepo::upsert_by_machine_id(
        &db_instance,
        db::UpsertDaemon {
            id: daemon_id.clone(),
            machine_id: services::embedded_daemon::embedded_machine_id(),
            hostname: "test-host".to_owned(),
            os: "linux".to_owned(),
            arch: "x86_64".to_owned(),
            status: db::DaemonStatus::Online,
            agent_version: None,
            labels_json: "{}".to_owned(),
            registration_token_hash: None,
            owner_id: None,
            visibility: "global".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("daemon seeds");
    db::DaemonRepo::update_report(
        &db_instance,
        db::UpdateDaemonReport {
            id: daemon_id.clone(),
            last_report_at: now.clone(),
            status: db::DaemonStatus::Online,
            detected_clis_json: serde_json::json!([
                {"kind": "shell", "availability": "authenticated", "path": "/bin/sh"}
            ])
            .to_string(),
            labels_json: None,
            updated_at: now.clone(),
        },
    )
    .await
    .expect("daemon report seeds");
    db::RuntimeRepo::create(
        &db_instance,
        db::CreateRuntime {
            id: db::new_uuid_v4(),
            daemon_id,
            kind: "local_process".to_owned(),
            workspace_root: "/tmp/forge-test".to_owned(),
            status: db::RuntimeStatus::Ready,
            labels_json: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("runtime seeds");

    let db = Arc::new(db_instance);
    let event_bus = Arc::new(events::EventBus::new(16));
    let state = AppState::with_adapter_registry(
        db,
        event_bus,
        true,
        Arc::new(cli_adapters::default_registry()),
    );

    let web_dist_dir = std::env::temp_dir().join(format!("forge-api-e2e-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&web_dist_dir).expect("create web dist dir");
    std::fs::write(web_dist_dir.join("index.html"), "<html></html>").expect("write index");

    let app = build_router(state.clone(), web_dist_dir);
    (app, state)
}

async fn existing_daemon_id(app: &Router) -> String {
    let hosts: Value = empty_request_with_bearer(
        app,
        Method::GET,
        "/api/v1/daemons",
        &admin_jwt(),
        StatusCode::OK,
    )
    .await;
    hosts["items"][0]["id"].as_str().unwrap().to_owned()
}

async fn create_project_and_repo(app: &Router) -> (String, String, TestDir) {
    let project: ProjectResponse = json_request(
        app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Dependency Test" }),
        StatusCode::OK,
    )
    .await;
    let repo_dir = TestDir::new("forge-e2e-repo");
    let repo_path = setup_git_repo(repo_dir.path());
    let default_branch = run_git(&repo_path, &["symbolic-ref", "--short", "HEAD"]);
    let repo: RepoResponse = json_request(
        app,
        Method::POST,
        &format!("/api/v1/projects/{}/repos", project.id),
        json!({
            "name": "forge",
            "local_path": repo_path.to_string_lossy(),
            "remote_url": repo_path.to_string_lossy(),
            "default_branch": default_branch
        }),
        StatusCode::OK,
    )
    .await;
    (project.id, repo.id, repo_dir)
}

async fn create_task_pair(app: &Router, project_id: &str) -> (String, String) {
    let first: TaskResponse = json_request(
        app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Dependent task" }),
        StatusCode::OK,
    )
    .await;
    let second: TaskResponse = json_request(
        app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Dependency task" }),
        StatusCode::OK,
    )
    .await;
    (first.id, second.id)
}

fn setup_git_repo(path: &Path) -> PathBuf {
    let repo_path = path.to_path_buf();
    run_git(&repo_path, &["init"]);
    run_git(&repo_path, &["config", "user.email", "test@forge.dev"]);
    run_git(&repo_path, &["config", "user.name", "Forge Test"]);
    std::fs::write(repo_path.join("README.md"), "# Test").expect("write README");
    run_git(&repo_path, &["add", "-A"]);
    run_git(&repo_path, &["commit", "-m", "initial commit"]);
    repo_path
}

fn run_git(path: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .expect("run git command");
    assert!(
        output.status.success(),
        "git {:?} failed\nstdout: {}\nstderr: {}",
        args,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(prefix: &str) -> Self {
        let path = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

async fn json_request<T>(
    app: &Router,
    method: Method,
    uri: &str,
    body: Value,
    expected_status: StatusCode,
) -> T
where
    T: DeserializeOwned,
{
    let response = raw_json_request(app, method, uri, body).await;
    parse_response(response, expected_status).await
}

async fn empty_request<T>(app: &Router, method: Method, uri: &str, expected_status: StatusCode) -> T
where
    T: DeserializeOwned,
{
    let response = raw_empty_request(app, method, uri).await;
    parse_response(response, expected_status).await
}

fn test_jwt() -> String {
    test_jwt_with_admin(true)
}

fn admin_jwt() -> String {
    test_jwt_with_admin(true)
}

fn test_jwt_with_admin(is_admin: bool) -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let claims = serde_json::json!({
        "sub": "test-user-id",
        "email": "test@example.com",
        "is_admin": is_admin,
        "iat": now,
        "exp": now + 900,
    });
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(b"test-jwt-secret-for-development"),
    )
    .expect("encode test jwt")
}

async fn empty_request_with_bearer<T>(
    app: &Router,
    method: Method,
    uri: &str,
    token: &str,
    expected_status: StatusCode,
) -> T
where
    T: serde::de::DeserializeOwned,
{
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("build empty request"),
        )
        .await
        .expect("router response");
    parse_response(response, expected_status).await
}

async fn raw_json_request(
    app: &Router,
    method: Method,
    uri: &str,
    body: Value,
) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {}", test_jwt()))
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .expect("build JSON request"),
        )
        .await
        .expect("router response")
}

async fn raw_empty_request(app: &Router, method: Method, uri: &str) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {}", test_jwt()))
                .body(Body::empty())
                .expect("build empty request"),
        )
        .await
        .expect("router response")
}

async fn parse_response<T>(response: axum::response::Response, expected_status: StatusCode) -> T
where
    T: DeserializeOwned,
{
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response body");
    assert_eq!(
        status,
        expected_status,
        "unexpected response status with body: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).expect("parse JSON response")
}
