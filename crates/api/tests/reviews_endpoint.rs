#![allow(dead_code, clippy::assertions_on_constants)]
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use api::{build_router, AppState};
use api_types::{ErrorResponse, ReviewResponse};
use axum::{
    body::{to_bytes, Body},
    http::{Method, Request, StatusCode},
    Router,
};
use db::{
    new_uuid_v4, now_rfc3339, CreateExecution, CreateProject, CreateRepo, CreateReview, CreateTask,
    ExecutionRepo, ExecutionStatus, ProjectRepo, RepoRepo, ReviewRepo, ReviewStatus, TaskRepo,
    UpdateProject,
};
use events::EventBus;
use serde::de::DeserializeOwned;
use serde_json::json;
use tower::ServiceExt;

#[tokio::test]
async fn review_detail_returns_auditor_verdict() {
    let workspace_root = TestDir::new("forge-reviews-workspaces");
    let harness = test_app(workspace_root.path()).await;
    let step_results_json = json!({
        "ci_steps": [{
            "index": 0,
            "command": "cargo test",
            "exit_code": 0,
            "stderr_tail": ""
        }],
        "auditor": {
            "verdict": "pass",
            "reason": null
        }
    })
    .to_string();
    let review_id = seed_review(&harness.state.db, step_results_json).await;

    let review: ReviewResponse = json_empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/reviews/{review_id}"),
        StatusCode::OK,
    )
    .await;

    assert_eq!(
        review.details.auditor.expect("auditor details").verdict,
        "pass"
    );
    assert_eq!(review.details.ci_steps.len(), 1);
    assert_eq!(review.step_results.len(), 1);
}

#[tokio::test]
async fn review_detail_returns_null_auditor_for_ci_only_results() {
    let workspace_root = TestDir::new("forge-reviews-workspaces");
    let harness = test_app(workspace_root.path()).await;
    let step_results_json = json!([{
        "index": 0,
        "command": "npm test",
        "exit_code": 0,
        "stderr_tail": ""
    }])
    .to_string();
    let review_id = seed_review(&harness.state.db, step_results_json).await;

    let review: ReviewResponse = json_empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/reviews/{review_id}"),
        StatusCode::OK,
    )
    .await;

    assert!(review.details.auditor.is_none());
    assert_eq!(review.details.ci_steps.len(), 1);
    assert_eq!(review.step_results.len(), 1);
}

#[tokio::test]
async fn unknown_review_returns_standard_not_found() {
    let workspace_root = TestDir::new("forge-reviews-workspaces");
    let harness = test_app(workspace_root.path()).await;

    let error: ErrorResponse = json_empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/reviews/{}", new_uuid_v4()),
        StatusCode::NOT_FOUND,
    )
    .await;

    assert_eq!(error.code, "not_found");
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

    let web_dist_dir = TestDir::new("forge-reviews-web");
    std::fs::write(web_dist_dir.path().join("index.html"), "<html></html>").expect("write index");
    let app = build_router((*state).clone(), web_dist_dir.path().to_path_buf());

    TestHarness {
        app,
        state,
        _web_dist_dir: web_dist_dir,
    }
}

async fn seed_review(db: &db::SqliteDb, step_results_json: String) -> String {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let task_id = new_uuid_v4();
    let execution_id = new_uuid_v4();
    let review_id = new_uuid_v4();

    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "Reviews".to_owned(),
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
            local_path: Some("/tmp/forge-reviews-repo".to_owned()),
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
            title: "Review details".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "review".to_owned(),
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
            task_id: task_id.clone(),
            agent_id: None,
            role: "reviewer".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("execution creates");
    ReviewRepo::create(
        db,
        CreateReview {
            id: review_id.clone(),
            task_id,
            execution_id,
            attempt_number: 1,
            status: ReviewStatus::Passed,
            step_results_json,
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("review creates");

    review_id
}

async fn json_empty_request<T>(
    app: &Router,
    method: Method,
    uri: &str,
    expected_status: StatusCode,
) -> T
where
    T: DeserializeOwned,
{
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("authorization", format!("Bearer {}", test_jwt()))
                .body(Body::empty())
                .expect("build empty request"),
        )
        .await
        .expect("router response");
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

fn test_jwt() -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let claims = serde_json::json!({
        "sub": "test-user-id",
        "email": "test@example.com",
        "is_admin": true,
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
