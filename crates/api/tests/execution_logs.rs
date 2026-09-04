#![allow(dead_code, clippy::assertions_on_constants)]
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use api::{build_router, AppState};
use api_types::ErrorResponse;
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use db::{
    new_uuid_v4, now_rfc3339, CreateExecution, CreateProject, CreateRepo, CreateTask,
    ExecutionRepo, ExecutionStatus, ProjectRepo, RepoRepo, TaskRepo, UpdateProject,
};
use events::EventBus;
use executors::{LogKind, LogStream, LogWriter};
use serde_json::{json, Value};
use tower::ServiceExt;

#[tokio::test]
async fn execution_logs_are_read_from_stored_logs_path() {
    let workspace_root = TestDir::new("forge-execution-logs-workspaces");
    let harness = test_app(workspace_root.path()).await;
    let logs_dir = TestDir::new("forge-execution-logs");
    let logs_path = logs_dir.path().join("execution.jsonl");

    let execution_id = seed_execution(&harness.state.db, Some(logs_path.clone())).await;
    write_log_entries(&logs_path, &execution_id).await;

    let body: Value = json_empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/executions/{execution_id}/logs?tail=10"),
        StatusCode::OK,
    )
    .await;
    let entries = body["items"].as_array().expect("items is array");
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0]["sequence"], json!(0));
    assert_eq!(entries[1]["sequence"], json!(1));
    assert_eq!(entries[2]["sequence"], json!(2));
    assert_eq!(entries[0]["execution_id"], execution_id);
    assert_eq!(body["next_sequence"], json!(3));

    let body: Value = json_empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/executions/{execution_id}/logs?from_sequence=1&limit=1"),
        StatusCode::OK,
    )
    .await;
    let entries = body["items"].as_array().expect("items is array");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["sequence"], json!(1));
    assert_eq!(body["has_more"], json!(true));
    assert_eq!(body["next_sequence"], json!(2));

    let _: ErrorResponse = json_empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/executions/{}/logs", new_uuid_v4()),
        StatusCode::NOT_FOUND,
    )
    .await;

    std::fs::remove_file(&logs_path).expect("delete log file");
    let error: ErrorResponse = json_empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/executions/{execution_id}/logs"),
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(error.code, "execution.logs_unavailable");
}

struct TestHarness {
    app: Router,
    state: Arc<AppState>,
    _web_dist_dir: TestDir,
}

async fn test_app(workspace_root: &Path) -> TestHarness {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");

    let db = Arc::new(db::SqliteDb::new(pool));
    let adapter_registry = Arc::new(cli_adapters::default_registry());
    services::ensure_default_agents(db.as_ref(), &adapter_registry)
        .await
        .expect("default agents upsert");
    let event_bus = Arc::new(EventBus::new(256));
    let merge_service = Arc::new(services::MergeService::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        workspace_root.to_path_buf(),
    ));
    let cleanup_scheduler = Arc::new(services::WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        workspace_root.to_path_buf(),
    ));
    let review_runner = Arc::new(review::ReviewRunner::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        Arc::clone(&adapter_registry),
    ));
    let state = Arc::new(AppState::with_adapter_registry_services_and_shutdown(
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
    ));

    let web_dist_dir = TestDir::new("forge-execution-logs-web");
    std::fs::write(web_dist_dir.path().join("index.html"), "<html></html>").expect("write index");
    let app = build_router((*state).clone(), web_dist_dir.path().to_path_buf());

    TestHarness {
        app,
        state,
        _web_dist_dir: web_dist_dir,
    }
}

async fn seed_execution(db: &db::SqliteDb, logs_path: Option<PathBuf>) -> String {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let task_id = new_uuid_v4();
    let execution_id = new_uuid_v4();

    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "Execution Logs".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_string(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");
    RepoRepo::create(
        db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "repo".to_owned(),
            local_path: Some("/tmp/forge-execution-logs-repo".to_owned()),
            work_mode: db::WorkMode::DirectMerge,
            remote_url: String::new(),
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project primary repo updates");
    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.clone(),
            project_id,
            repo_id: Some(repo_id),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Read logs".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "in_progress".to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task creates");
    ExecutionRepo::create(
        db,
        CreateExecution {
            id: execution_id.clone(),
            task_id,
            agent_id: None,
            role: "executor".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: logs_path.map(|path| path.to_string_lossy().into_owned()),
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("execution creates");

    execution_id
}

async fn write_log_entries(path: &Path, execution_id: &str) {
    let mut writer = LogWriter::new(path, execution_id.to_owned(), 1024 * 1024);
    writer
        .write(LogKind::Stdout, LogStream::Main, json!({ "line": "first" }))
        .await
        .expect("first log entry writes");
    writer
        .write(
            LogKind::Stderr,
            LogStream::Main,
            json!({ "line": "second" }),
        )
        .await
        .expect("second log entry writes");
    writer
        .write(
            LogKind::Assistant,
            LogStream::Main,
            json!({ "message": "third" }),
        )
        .await
        .expect("third log entry writes");
}

async fn json_empty_request<T>(
    app: &Router,
    method: Method,
    uri: &str,
    expected_status: StatusCode,
) -> T
where
    T: serde::de::DeserializeOwned,
{
    let response = raw_empty_request(app, method, uri).await;
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

async fn raw_empty_request(app: &Router, method: Method, uri: &str) -> axum::response::Response {
    let token = {
        use jsonwebtoken::{Algorithm, EncodingKey, Header};
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let claims = serde_json::json!({
            "sub": "test-user-id",
            "email": "test@example.com",
            "iat": now,
            "exp": now + 900,
        });
        jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(b"test-jwt-secret-for-development"),
        )
        .expect("encode test jwt")
    };
    app.clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("build empty request"),
        )
        .await
        .expect("router response")
}

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(prefix: &str) -> Self {
        let path = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).expect("temp dir creates");
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
