#![allow(dead_code)]

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use api::{build_router, AppState};
use api_types::{
    TERMINAL_ACTIVE_EXECUTION, TERMINAL_DISABLED, TERMINAL_INVALID_INPUT, TERMINAL_NOT_FOUND,
    TERMINAL_SESSION_LIMIT, TERMINAL_USER_LIMIT, TERMINAL_WORKSPACE_NOT_READY,
};
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use config::{ForgeConfig, TerminalConfig};
use db::{new_uuid_v4, now_rfc3339};
use events::EventBus;
use serde_json::{json, Value};
use tower::ServiceExt;

const TEST_USER_ID: &str = "test-user-id";
const TEST_EMAIL: &str = "test@example.com";

struct Harness {
    app: Router,
    state: Arc<AppState>,
    workspace_root: TestDir,
    _web_dist_dir: TestDir,
}

struct SeededTask {
    task_id: String,
    repo_id: String,
}

async fn setup(terminal: TerminalConfig) -> Harness {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = Arc::new(db::SqliteDb::new(pool));
    seed_user(&db, TEST_USER_ID, TEST_EMAIL).await;

    let workspace_root = TestDir::new("forge-terminal-workspaces");
    let event_bus = Arc::new(EventBus::new(256));
    let adapter_registry = Arc::new(cli_adapters::default_registry());
    let merge_service = Arc::new(services::MergeService::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        workspace_root.path().to_path_buf(),
    ));
    let cleanup_scheduler = Arc::new(services::WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        workspace_root.path().to_path_buf(),
    ));
    let review_runner = Arc::new(review::ReviewRunner::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        Arc::clone(&adapter_registry),
    ));
    let mut state = AppState::with_adapter_registry_services_and_shutdown(
        db,
        event_bus,
        true,
        adapter_registry,
        merge_service,
        cleanup_scheduler,
        review_runner,
        api::state::ShutdownSignal::new(),
        api::state::test_workflows_dir(),
        api::state::test_jwt_secret(),
        api::state::test_bcrypt_cost(),
    );
    let mut config = ForgeConfig::default();
    config.workspace.root = workspace_root.path().to_path_buf();
    config.terminal = terminal;
    state = state.with_effective_config(config);

    let state = Arc::new(state);
    let web_dist_dir = TestDir::new("forge-terminal-web");
    std::fs::write(web_dist_dir.path().join("index.html"), "<html></html>").unwrap();
    let app = build_router((*state).clone(), web_dist_dir.path().to_path_buf());
    Harness {
        app,
        state,
        workspace_root,
        _web_dist_dir: web_dist_dir,
    }
}

async fn request(
    app: &Router,
    method: Method,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    request_with_bearer(app, method, uri, body, &test_jwt()).await
}

async fn request_with_bearer(
    app: &Router,
    method: Method,
    uri: &str,
    body: Option<Value>,
    bearer: &str,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {bearer}"));
    let body = if let Some(body) = body {
        builder = builder.header(header::CONTENT_TYPE, "application/json");
        Body::from(serde_json::to_string(&body).unwrap())
    } else {
        Body::empty()
    };
    let response = app
        .clone()
        .oneshot(builder.body(body).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

async fn sse_request(app: &Router) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/api/v1/events?token={}", test_jwt()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
}

fn enabled_terminal() -> TerminalConfig {
    TerminalConfig {
        enabled: true,
        ..TerminalConfig::default()
    }
}

async fn seed_user(db: &db::SqliteDb, id: &str, email: &str) {
    let now = now_rfc3339();
    db::UserRepo::create_user(
        db,
        &db::User {
            id: id.to_owned(),
            email: email.to_owned(),
            password_hash: "$2b$04$placeholder".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
}

async fn seed_task(harness: &Harness) -> SeededTask {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let task_id = new_uuid_v4();
    db::ProjectRepo::create(
        &*harness.state.db,
        db::CreateProject {
            id: project_id.clone(),
            name: "Terminal Project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(TEST_USER_ID.to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    db::ProjectMemberRepo::add_member(
        &*harness.state.db,
        db::CreateProjectMember {
            id: new_uuid_v4(),
            project_id: project_id.clone(),
            user_id: TEST_USER_ID.to_owned(),
            role: "owner".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    db::RepoRepo::create(
        &*harness.state.db,
        db::CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "repo".to_owned(),
            remote_url: "file:///tmp/repo".to_owned(),
            local_path: None,
            work_mode: db::WorkMode::DirectMerge,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    db::TaskRepo::create(
        &*harness.state.db,
        db::CreateTask {
            id: task_id.clone(),
            project_id,
            repo_id: Some(repo_id.clone()),
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "Terminal task".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "todo".to_owned(),
            is_automation: false,
            priority: 0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    SeededTask { task_id, repo_id }
}

async fn seed_ready_workspace(harness: &Harness, task: &SeededTask) -> String {
    let now = now_rfc3339();
    let workspace_id = new_uuid_v4();
    let worktree_path = harness
        .workspace_root
        .path()
        .join(&task.task_id)
        .join("repo");
    std::fs::create_dir_all(&worktree_path).unwrap();
    db::WorkspaceRepo::create(
        &*harness.state.db,
        db::CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task.task_id.clone(),
            repo_id: task.repo_id.clone(),
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch: workspace::task_branch_name(&task.task_id),
            status: db::WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    workspace_id
}

async fn seed_running_session(harness: &Harness, task: &SeededTask, workspace_id: &str) -> String {
    let now = now_rfc3339();
    let created = db::TerminalSessionRepo::create_terminal_session(
        &*harness.state.db,
        db::CreateTerminalSession {
            id: new_uuid_v4(),
            task_id: task.task_id.clone(),
            workspace_id: workspace_id.to_owned(),
            daemon_id: None,
            created_by_user_id: TEST_USER_ID.to_owned(),
            rows: 24,
            cols: 80,
            created_at: now.clone(),
        },
    )
    .await
    .unwrap();
    db::TerminalSessionRepo::update_terminal_session_status(
        &*harness.state.db,
        &created.id,
        created.version,
        db::UpdateTerminalSessionStatus {
            status: db::TerminalSessionStatus::Running,
            started_at: Some(now.clone()),
            last_activity_at: Some(now),
            ended_at: None,
            pid: None,
            exit_code: None,
            exit_signal: None,
            exit_reason: None,
        },
    )
    .await
    .unwrap()
    .id
}

fn assert_error_code(body: &Value, code: &str) {
    assert_eq!(body["code"], code, "unexpected error body: {body}");
}

fn test_jwt() -> String {
    test_jwt_for(TEST_USER_ID, TEST_EMAIL, false)
}

fn test_jwt_for(user_id: &str, email: &str, is_admin: bool) -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let claims = json!({
        "sub": user_id,
        "email": email,
        "is_admin": is_admin,
        "iat": now,
        "exp": now + 900,
    });
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(b"test-jwt-secret-for-development"),
    )
    .unwrap()
}

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(prefix: &str) -> Self {
        let path = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
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

#[tokio::test]
async fn terminal_disabled_returns_403() {
    let harness = setup(TerminalConfig::default()).await;
    let task = seed_task(&harness).await;

    let (status, body) = request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/terminals", task.task_id),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_error_code(&body, TERMINAL_DISABLED);

    let (status, body) = request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/terminals/availability", task.task_id),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["enabled"], false);
    assert_eq!(body["can_create"], false);
}

#[tokio::test]
async fn create_session_requires_workspace() {
    let harness = setup(enabled_terminal()).await;
    let task = seed_task(&harness).await;

    let (status, body) = request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/terminals", task.task_id),
        Some(json!({})),
    )
    .await;

    assert!(status.is_client_error());
    assert_error_code(&body, TERMINAL_WORKSPACE_NOT_READY);
}

#[tokio::test]
async fn create_session_succeeds_with_attach_token() {
    let harness = setup(enabled_terminal()).await;
    let task = seed_task(&harness).await;
    seed_ready_workspace(&harness, &task).await;

    let (status, body) = request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/terminals", task.task_id),
        Some(json!({ "rows": 30, "cols": 100 })),
    )
    .await;

    assert!(matches!(status, StatusCode::OK | StatusCode::CREATED));
    let session_id = body["session"]["id"].as_str().unwrap();
    assert_eq!(body["session"]["status"], "running");
    assert!(!body["attach"]["attach_token"].as_str().unwrap().is_empty());
    assert!(body["attach"]["ws_url"]
        .as_str()
        .unwrap()
        .contains(session_id));
    assert!(
        db::TerminalSessionRepo::get_terminal_session(&*harness.state.db, session_id)
            .await
            .unwrap()
            .is_some()
    );
    harness
        .state
        .terminal_service
        .terminate_session(session_id, TEST_USER_ID, Some("test cleanup".to_owned()))
        .await
        .unwrap();
}

#[tokio::test]
async fn create_session_rejects_too_small_terminal_size() {
    let harness = setup(enabled_terminal()).await;
    let task = seed_task(&harness).await;
    seed_ready_workspace(&harness, &task).await;

    let (status, body) = request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/terminals", task.task_id),
        Some(json!({ "rows": 1, "cols": 80 })),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_error_code(&body, TERMINAL_INVALID_INPUT);
}

#[tokio::test]
async fn resize_session_rejects_too_small_terminal_size() {
    let harness = setup(enabled_terminal()).await;
    let task = seed_task(&harness).await;
    let workspace_id = seed_ready_workspace(&harness, &task).await;
    let session_id = seed_running_session(&harness, &task, &workspace_id).await;

    let (status, body) = request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/terminals/{session_id}/resize"),
        Some(json!({ "rows": 24, "cols": 1 })),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_error_code(&body, TERMINAL_INVALID_INPUT);
}

#[tokio::test]
async fn unauthorized_user_rejected() {
    let harness = setup(enabled_terminal()).await;
    let task = seed_task(&harness).await;
    seed_ready_workspace(&harness, &task).await;
    let token = test_jwt_for("second-user-id", "second@example.com", false);

    let (status, _) = request_with_bearer(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/terminals", task.task_id),
        Some(json!({})),
        &token,
    )
    .await;

    assert!(matches!(
        status,
        StatusCode::FORBIDDEN | StatusCode::NOT_FOUND
    ));
}

#[tokio::test]
async fn session_limit_enforced() {
    let harness = setup(enabled_terminal()).await;
    let task = seed_task(&harness).await;
    let workspace_id = seed_ready_workspace(&harness, &task).await;
    seed_running_session(&harness, &task, &workspace_id).await;
    seed_running_session(&harness, &task, &workspace_id).await;

    let (status, body) = request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/terminals", task.task_id),
        Some(json!({})),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_error_code(&body, TERMINAL_SESSION_LIMIT);
}

#[tokio::test]
async fn user_session_limit_enforced() {
    let mut terminal = enabled_terminal();
    terminal.max_sessions_per_task = 10;
    let harness = setup(terminal).await;
    for _ in 0..4 {
        let task = seed_task(&harness).await;
        let workspace_id = seed_ready_workspace(&harness, &task).await;
        seed_running_session(&harness, &task, &workspace_id).await;
    }
    let task = seed_task(&harness).await;
    seed_ready_workspace(&harness, &task).await;

    let (status, body) = request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/terminals", task.task_id),
        Some(json!({})),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_error_code(&body, TERMINAL_USER_LIMIT);
    assert_eq!(body["details"]["scope"], "user");
}

#[tokio::test]
async fn lifecycle_event_emitted() {
    let harness = setup(enabled_terminal()).await;
    let task = seed_task(&harness).await;
    seed_ready_workspace(&harness, &task).await;
    let sse = sse_request(&harness.app).await;
    assert_eq!(sse.status(), StatusCode::OK);
    let sse_body =
        tokio::spawn(async move { to_bytes(sse.into_body(), usize::MAX).await.unwrap() });

    let (_, body) = request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/terminals", task.task_id),
        Some(json!({})),
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    harness.state.shutdown_signal.request();
    let bytes = sse_body.await.unwrap();
    let stream = String::from_utf8_lossy(&bytes);

    assert!(
        stream.contains("\"event_type\":\"task.terminal.session_changed\""),
        "missing terminal SSE event in {stream}"
    );
    assert!(
        !stream.contains("event: "),
        "SSE frames must stay unnamed so every frame reaches `onmessage`: {stream}"
    );
    assert!(
        stream.contains("\"kind\":\"created\""),
        "missing created kind in {stream}"
    );
    harness
        .state
        .terminal_service
        .terminate_session(
            body["session"]["id"].as_str().unwrap(),
            TEST_USER_ID,
            Some("test cleanup".to_owned()),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn terminate_session_marks_status() {
    let harness = setup(enabled_terminal()).await;
    let task = seed_task(&harness).await;
    seed_ready_workspace(&harness, &task).await;
    let (_, created) = request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/terminals", task.task_id),
        Some(json!({})),
    )
    .await;
    let session_id = created["session"]["id"].as_str().unwrap();

    let (status, body) = request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/terminals/{session_id}/terminate"),
        Some(json!({ "reason": "done" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "terminated");

    let (status, body) = request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/terminals/{session_id}/attach-token"),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_error_code(&body, TERMINAL_NOT_FOUND);
}

#[tokio::test]
async fn list_sessions_includes_ended_when_requested() {
    let harness = setup(enabled_terminal()).await;
    let task = seed_task(&harness).await;
    seed_ready_workspace(&harness, &task).await;
    let (_, created) = request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/terminals", task.task_id),
        Some(json!({})),
    )
    .await;
    let session_id = created["session"]["id"].as_str().unwrap();
    request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/terminals/{session_id}/terminate"),
        Some(json!({ "reason": "done" })),
    )
    .await;

    let (_, active) = request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/terminals", task.task_id),
        None,
    )
    .await;
    let (_, all) = request(
        &harness.app,
        Method::GET,
        &format!(
            "/api/v1/tasks/{}/terminals?include_ended=true",
            task.task_id
        ),
        None,
    )
    .await;

    assert!(!active
        .as_array()
        .unwrap()
        .iter()
        .any(|s| s["id"] == session_id));
    assert!(all
        .as_array()
        .unwrap()
        .iter()
        .any(|s| s["id"] == session_id));
}

#[tokio::test]
async fn active_execution_blocks_terminal_creation() {
    let harness = setup(enabled_terminal()).await;
    let task = seed_task(&harness).await;
    let workspace_id = seed_ready_workspace(&harness, &task).await;
    let _execution_guard = harness
        .state
        .workspace_exec_locks
        .try_acquire(&workspace_id)
        .unwrap();

    let (status, body) = request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/terminals", task.task_id),
        Some(json!({})),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_error_code(&body, TERMINAL_ACTIVE_EXECUTION);
}
