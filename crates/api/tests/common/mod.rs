#![allow(dead_code)]

pub mod fake_daemon;

use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::Duration,
};

use api::{build_router, AppState};
use api_types::{
    AgentResponse, DaemonRegisterResponse, DaemonResponse, ProjectResponse, RepoResponse,
    TaskResponse,
};
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tower::ServiceExt;

pub struct Harness {
    pub app: Router,
    pub state: Arc<AppState>,
    _web_dist_dir: TestDir,
}

pub async fn test_app(workspace_root: &Path, prefix: &str) -> Harness {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");

    let db = Arc::new(db::SqliteDb::new(pool));

    // Seed the JWT test user so project membership FK constraints succeed.
    let now = db::now_rfc3339();
    db::UserRepo::create_user(
        &*db,
        &db::User {
            id: "test-user-id".to_owned(),
            email: "test@example.com".to_owned(),
            password_hash: "$2b$04$placeholder".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("seed test user");

    let adapter_registry = Arc::new(cli_adapters::default_registry());
    services::ensure_default_agents(db.as_ref(), &adapter_registry)
        .await
        .expect("default agents upsert");
    let event_bus = Arc::new(events::EventBus::new(64));
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

    let web_dist_dir = TestDir::new(&format!("{prefix}-web"));
    std::fs::write(web_dist_dir.path().join("index.html"), "<html></html>").expect("write index");
    let app = build_router((*state).clone(), web_dist_dir.path().to_path_buf());

    Harness {
        app,
        state,
        _web_dist_dir: web_dist_dir,
    }
}

pub fn setup_git_repo(root: &Path) -> PathBuf {
    let repo_path = root.join("repo");
    std::fs::create_dir_all(&repo_path).expect("repo dir creates");
    run_git(&repo_path, &["init"]);
    run_git(&repo_path, &["config", "user.email", "test@forge.dev"]);
    run_git(&repo_path, &["config", "user.name", "Forge Test"]);
    std::fs::write(repo_path.join("README.md"), "# Forge\n").expect("README writes");
    run_git(&repo_path, &["add", "-A"]);
    run_git(&repo_path, &["commit", "-m", "initial commit"]);
    repo_path
}

pub async fn create_project_and_repo(
    app: &Router,
    name: &str,
    repo_path: &Path,
) -> (String, String) {
    let project: ProjectResponse = json_request(
        app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": name }),
        StatusCode::OK,
    )
    .await;
    let default_branch = run_git(repo_path, &["symbolic-ref", "--short", "HEAD"]);
    let repo: RepoResponse = json_request(
        app,
        Method::POST,
        &format!("/api/v1/projects/{}/repos", project.id),
        json!({
            "name": "repo",
            "local_path": repo_path.to_string_lossy(),
            "remote_url": repo_path.to_string_lossy(),
            "default_branch": default_branch
        }),
        StatusCode::OK,
    )
    .await;
    (project.id, repo.id)
}

/// Complete the repository execution facts that legacy API fixtures create
/// directly.  The production setup flow records these facts through its
/// command endpoints; tests that exercise the older project/task routes need
/// to make the same worker, role, and repository choices explicit.
pub async fn configure_execution_test_setup(
    db: &db::SqliteDb,
    project_id: &str,
    repo_id: &str,
    worker_id: &str,
    reviewer_id: &str,
) {
    let worker = db::AgentRepo::get_by_id(db, worker_id)
        .await
        .expect("test worker lookup")
        .expect("test worker exists");
    assert_eq!(
        services::agent_service::compute_effective_status(db, &worker)
            .await
            .expect("test worker effective status"),
        services::agent_service::EffectiveStatus::Active,
        "test worker must be active before execution setup is configured"
    );
    if reviewer_id != worker_id {
        let reviewer = db::AgentRepo::get_by_id(db, reviewer_id)
            .await
            .expect("test reviewer lookup")
            .expect("test reviewer exists");
        assert_eq!(
            services::agent_service::compute_effective_status(db, &reviewer)
                .await
                .expect("test reviewer effective status"),
            services::agent_service::EffectiveStatus::Active,
            "test reviewer must be active before execution setup is configured"
        );
    }

    // Lease admission re-checks effective status after the execution row is
    // visible. Leave one slot free so the fixture identity remains active
    // during that check (the production default of one slot would report it
    // as busy and make a valid fixture look unconfigured).
    for identity_id in [worker_id, reviewer_id] {
        sqlx::query(
            "UPDATE agent_identity
             SET max_concurrent_tasks = 2, updated_at = ?
             WHERE id = ?",
        )
        .bind(db::now_rfc3339())
        .bind(identity_id)
        .execute(db.pool())
        .await
        .expect("test execution identity capacity update");
    }

    let repo_project_id: String = sqlx::query_scalar("SELECT project_id FROM repo WHERE id = ?")
        .bind(repo_id)
        .fetch_one(db.pool())
        .await
        .expect("test repository lookup");
    assert_eq!(
        repo_project_id, project_id,
        "test primary repository belongs to the configured project"
    );

    let existing_settings: Option<String> =
        sqlx::query_scalar("SELECT settings FROM project WHERE id = ?")
            .bind(project_id)
            .fetch_optional(db.pool())
            .await
            .expect("test project settings lookup");
    let mut settings = existing_settings
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .filter(Value::is_object)
        .unwrap_or_else(|| json!({}));
    settings["default_role_assignments"] = json!([
        {
            "role_name": "planner",
            "assignee_type": "agent",
            "assignee_id": worker_id
        },
        {
            "role_name": "coder",
            "assignee_type": "agent",
            "assignee_id": worker_id
        },
        {
            "role_name": "reviewer",
            "assignee_type": "agent",
            "assignee_id": reviewer_id
        },
        {
            "role_name": "worker",
            "assignee_type": "agent",
            "assignee_id": worker_id
        }
    ]);
    let now = db::now_rfc3339();
    sqlx::query(
        "UPDATE project
         SET settings = ?, primary_repo_id = ?, updated_at = ?
         WHERE id = ?",
    )
    .bind(settings.to_string())
    .bind(repo_id)
    .bind(&now)
    .bind(project_id)
    .execute(db.pool())
    .await
    .expect("test project execution setup update");

    let operation_id: String =
        sqlx::query_scalar("SELECT id FROM project_provisioning_operation WHERE project_id = ?")
            .bind(project_id)
            .fetch_one(db.pool())
            .await
            .expect("test project provisioning operation lookup");
    sqlx::query(
        "UPDATE project_provisioning_operation
         SET status = 'ready', current_checkpoint = 'completed',
             lease_owner = NULL, lease_expires_at = NULL, next_retry_at = NULL,
             retryable = 0, completed_at = ?, updated_at = ?, version = version + 1
         WHERE id = ? AND project_id = ?",
    )
    .bind(&now)
    .bind(&now)
    .bind(&operation_id)
    .bind(project_id)
    .execute(db.pool())
    .await
    .expect("test project provisioning operation ready update");
    for checkpoint in [
        "preflight",
        "repository_scaffolded",
        "repository_initialized",
        "repository_registered",
        "repository_linked",
        "roles_assigned",
    ] {
        sqlx::query(
            "UPDATE project_provisioning_checkpoint
             SET status = 'completed', completed_at = ?, updated_at = ?,
                 version = version + 1
             WHERE operation_id = ? AND checkpoint = ?",
        )
        .bind(&now)
        .bind(&now)
        .bind(&operation_id)
        .bind(checkpoint)
        .execute(db.pool())
        .await
        .expect("test project provisioning checkpoint ready update");
    }
}

pub async fn create_shell_agents(
    app: &Router,
    workspace_root: &Path,
    prefix: &str,
) -> (String, String) {
    let registration: DaemonRegisterResponse = json_request(
        app,
        Method::POST,
        "/api/v1/daemons/register",
        json!({
            "machine_id": services::embedded_daemon::embedded_machine_id(),
            "hostname": format!("{prefix}-host"),
            "os": "linux",
            "arch": "x86_64",
            "agent_version": "test"
        }),
        StatusCode::OK,
    )
    .await;
    let daemon_id = registration.daemon_id;

    let _: DaemonResponse = json_request_with_bearer(
        app,
        Method::POST,
        &format!("/api/v1/daemons/{daemon_id}/report"),
        &registration.registration_token,
        json!({
            "detected_clis": [{
                "kind": "shell",
                "availability": "authenticated",
                "path": "/bin/sh"
            }],
            "runtimes": [{
                "kind": "local",
                "workspace_root": workspace_root.to_string_lossy(),
                "status": "ready"
            }]
        }),
        StatusCode::OK,
    )
    .await;

    let admin_token = admin_jwt();
    let agent_a: AgentResponse = json_request_with_bearer(
        app,
        Method::POST,
        "/api/v1/agents",
        &admin_token,
        json!({
            "name": format!("{prefix}-agent-a"),
            "executor_type": "shell",
            "daemon_id": daemon_id,
        }),
        StatusCode::OK,
    )
    .await;
    let agent_b: AgentResponse = json_request_with_bearer(
        app,
        Method::POST,
        "/api/v1/agents",
        &admin_token,
        json!({
            "name": format!("{prefix}-agent-b"),
            "executor_type": "shell",
            "daemon_id": daemon_id,
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(agent_a.effective_status.as_deref(), Some("active"));
    assert_eq!(agent_b.effective_status.as_deref(), Some("active"));
    (agent_a.id, agent_b.id)
}

pub async fn poll_task_status(app: &Router, task_id: &str, expected: &str) -> TaskResponse {
    for _ in 0..100 {
        let task: TaskResponse = empty_request(
            app,
            Method::GET,
            &format!("/api/v1/tasks/{task_id}"),
            StatusCode::OK,
        )
        .await;
        if task.status == expected {
            return task;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("task {task_id} did not reach {expected}");
}

pub fn role_assignee<'a>(task: &'a TaskResponse, role_name: &str) -> Option<&'a str> {
    task.role_assignments
        .iter()
        .find(|assignment| assignment.role_name == role_name)
        .and_then(|assignment| assignment.assignee_id.as_deref())
}

pub async fn json_request<T>(
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

pub async fn json_request_with_bearer<T>(
    app: &Router,
    method: Method,
    uri: &str,
    token: &str,
    body: Value,
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
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .expect("build authorized JSON request"),
        )
        .await
        .expect("router response");
    parse_response(response, expected_status).await
}

pub async fn empty_request<T>(
    app: &Router,
    method: Method,
    uri: &str,
    expected_status: StatusCode,
) -> T
where
    T: DeserializeOwned,
{
    let response = raw_empty_request(app, method, uri).await;
    parse_response(response, expected_status).await
}

pub async fn empty_request_with_bearer<T>(
    app: &Router,
    method: Method,
    uri: &str,
    token: &str,
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
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("build authorized empty request"),
        )
        .await
        .expect("router response");
    parse_response(response, expected_status).await
}

pub fn test_jwt() -> String {
    test_jwt_with_admin(false)
}

pub fn admin_jwt() -> String {
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

/// Create an API-key provider entry (the first half of the split connect
/// contract).
#[allow(dead_code)]
pub async fn create_provider_entry(
    app: &Router,
    token: &str,
    provider: &str,
    label: &str,
    credential: &str,
    base_url: &str,
) -> api_types::ProviderEntryResponse {
    json_request_with_bearer(
        app,
        Method::POST,
        "/api/v1/providers",
        token,
        json!({
            "provider": provider,
            "label": label,
            "credential": credential,
            "base_url": base_url,
        }),
        StatusCode::OK,
    )
    .await
}

/// Provider entry + direct embedded agent in one helper: the canonical
/// two-step replacement for the removed single-shot connect endpoint.
#[allow(dead_code)]
pub async fn connect_embedded_agent(
    app: &Router,
    token: &str,
    name: &str,
    credential_label: &str,
    credential: &str,
    account_permission_ceiling: Value,
    tool_policy: Value,
) -> api_types::ConnectedEmbeddedAgentResponse {
    let entry = create_provider_entry(
        app,
        token,
        "openai_compatible",
        credential_label,
        credential,
        "https://8.8.8.8",
    )
    .await;
    json_request_with_bearer(
        app,
        Method::POST,
        "/api/v1/embedded-agents",
        token,
        json!({
            "name": name,
            "credential_id": entry.id,
            "model": "test-model",
            "account_permission_ceiling": account_permission_ceiling,
            "tool_policy": tool_policy,
        }),
        StatusCode::OK,
    )
    .await
}

pub async fn raw_json_request(
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

pub async fn raw_empty_request(
    app: &Router,
    method: Method,
    uri: &str,
) -> axum::response::Response {
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

pub async fn parse_response<T>(response: axum::response::Response, expected_status: StatusCode) -> T
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

fn run_git(path: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .expect("git command runs");
    assert!(
        output.status.success(),
        "git {} failed\nstdout: {}\nstderr: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

pub struct TestDir {
    path: PathBuf,
}

impl TestDir {
    pub fn new(prefix: &str) -> Self {
        Self::new_in(&std::env::temp_dir(), prefix)
    }

    pub fn new_in(root: &Path, prefix: &str) -> Self {
        let path = root.join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).expect("temp dir creates");
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
