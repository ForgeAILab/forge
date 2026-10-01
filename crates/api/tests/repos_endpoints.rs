#![allow(dead_code, clippy::assertions_on_constants)]
use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

use api::{build_router, AppState};
use api_types::{ProjectResponse, RepoResponse};
use axum::{
    body::{to_bytes, Body},
    http::{Method, Request, StatusCode},
    Router,
};
use db::ProjectRepo;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tower::ServiceExt;

#[tokio::test]
async fn create_local_repo_normalizes_blank_remote_url() {
    let (app, db) = test_app_with_db().await;
    let local_path = env::current_dir().expect("current directory");
    for remote_url in [Value::Null, json!(""), json!(" \t\r\n"), json!("\u{2003}")] {
        let project: ProjectResponse = json_request(
            &app,
            Method::POST,
            "/api/v1/projects",
            json!({"name": "local-only"}),
            StatusCode::OK,
        )
        .await;
        let repo: RepoResponse = json_request(
            &app,
            Method::POST,
            &format!("/api/v1/projects/{}/repos", project.id),
            json!({"name": "local-only", "local_path": local_path, "remote_url": remote_url}),
            StatusCode::OK,
        )
        .await;
        assert_eq!(repo.remote_url, None);
        let stored: Option<String> = sqlx::query_scalar("SELECT remote_url FROM repo WHERE id = ?")
            .bind(&repo.id)
            .fetch_one(db.pool())
            .await
            .expect("stored remote loads");
        assert_eq!(stored, None);
        let reloaded: RepoResponse = json_request(
            &app,
            Method::GET,
            &format!("/api/v1/repos/{}", repo.id),
            Value::Null,
            StatusCode::OK,
        )
        .await;
        assert_eq!(reloaded.remote_url, None);
    }
    let project: ProjectResponse = json_request(
        &app,
        Method::POST,
        "/api/v1/projects",
        json!({"name": "local-only-omitted"}),
        StatusCode::OK,
    )
    .await;
    let repo: RepoResponse = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{}/repos", project.id),
        json!({"local_path": local_path}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(repo.remote_url, None);
    assert!(!repo.name.is_empty());
}

#[test]
fn remote_url_update_serialization_preserves_omission() {
    let omitted: api_types::UpdateRepoRequest =
        serde_json::from_value(json!({})).expect("omitted update decodes");
    assert_eq!(omitted.remote_url, None);
    assert!(serde_json::to_value(&omitted)
        .expect("update serializes")
        .get("remote_url")
        .is_none());
    let clear: api_types::UpdateRepoRequest =
        serde_json::from_value(json!({"remote_url": null})).expect("clear update decodes");
    assert_eq!(clear.remote_url, Some(None));
    assert_eq!(
        serde_json::to_value(clear).expect("clear serializes")["remote_url"],
        Value::Null
    );
}

#[tokio::test]
async fn update_repo_normalizes_blank_remote_url_and_preserves_omission() {
    let app = test_app().await;
    let project: ProjectResponse = json_request(
        &app,
        Method::POST,
        "/api/v1/projects",
        json!({"name": "remote-update"}),
        StatusCode::OK,
    )
    .await;
    let repo: RepoResponse = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{}/repos", project.id),
        json!({"remote_url": "https://example.com/repo.git"}),
        StatusCode::OK,
    )
    .await;
    let uri = format!("/api/v1/repos/{}", repo.id);
    let updated: RepoResponse = json_request(
        &app,
        Method::PATCH,
        &uri,
        json!({"name": "renamed"}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(updated.remote_url, repo.remote_url);
    for remote_url in [Value::Null, json!(""), json!(" \t\r\n")] {
        let _: RepoResponse = json_request(
            &app,
            Method::PATCH,
            &uri,
            json!({"remote_url": "https://example.com/repo.git"}),
            StatusCode::OK,
        )
        .await;
        let updated: RepoResponse = json_request(
            &app,
            Method::PATCH,
            &uri,
            json!({"remote_url": remote_url}),
            StatusCode::OK,
        )
        .await;
        assert_eq!(updated.remote_url, None);
    }
}

#[tokio::test]
async fn create_local_repo_normalizes_relative_path() {
    let app = test_app().await;
    let project: ProjectResponse = json_request(
        &app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "repo-normalize" }),
        StatusCode::OK,
    )
    .await;

    let cwd = env::current_dir().expect("current dir");
    let repo_dir = ScopedDir::new_under(&cwd, "forge-api-repo-relative");
    init_git_repo(repo_dir.path());
    let relative_path = repo_dir
        .path()
        .file_name()
        .expect("repo dir name")
        .to_string_lossy()
        .into_owned();

    let repo: RepoResponse = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{}/repos", project.id),
        json!({
            "name": "relative-repo",
            "local_path": relative_path.clone(),
            "remote_url": relative_path,
            "default_branch": "main"
        }),
        StatusCode::OK,
    )
    .await;

    assert_eq!(
        repo.local_path.as_deref(),
        Some(repo_dir.canonical_path().to_string_lossy().as_ref())
    );
}

#[tokio::test]
async fn update_local_repo_normalizes_relative_path() {
    let app = test_app().await;
    let project: ProjectResponse = json_request(
        &app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "repo-update-normalize" }),
        StatusCode::OK,
    )
    .await;

    let initial_repo_dir = ScopedDir::new("forge-api-repo-initial");
    init_git_repo(initial_repo_dir.path());
    let repo: RepoResponse = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{}/repos", project.id),
        json!({
            "name": "update-repo",
            "local_path": initial_repo_dir.path().to_string_lossy(),
            "remote_url": initial_repo_dir.path().to_string_lossy(),
            "default_branch": "main"
        }),
        StatusCode::OK,
    )
    .await;

    let cwd = env::current_dir().expect("current dir");
    let updated_repo_dir = ScopedDir::new_under(&cwd, "forge-api-repo-updated");
    init_git_repo(updated_repo_dir.path());
    let relative_path = updated_repo_dir
        .path()
        .file_name()
        .expect("updated repo dir name")
        .to_string_lossy()
        .into_owned();

    let updated: RepoResponse = json_request(
        &app,
        Method::PATCH,
        &format!("/api/v1/repos/{}", repo.id),
        json!({
            "name": null,
            "local_path": relative_path.clone(),
            "remote_url": relative_path,
            "default_branch": null
        }),
        StatusCode::OK,
    )
    .await;

    assert_eq!(
        updated.local_path.as_deref(),
        Some(updated_repo_dir.canonical_path().to_string_lossy().as_ref())
    );
}

#[tokio::test]
async fn update_repo_clears_local_path_for_remote_clone_mode() {
    let app = test_app().await;
    let project: ProjectResponse = json_request(
        &app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "repo-clear-local-path" }),
        StatusCode::OK,
    )
    .await;

    let repo_dir = ScopedDir::new("forge-api-repo-clear-local");
    init_git_repo(repo_dir.path());
    let repo: RepoResponse = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{}/repos", project.id),
        json!({
            "name": "clear-local-repo",
            "local_path": repo_dir.path().to_string_lossy(),
            "remote_url": repo_dir.path().to_string_lossy(),
            "default_branch": "main"
        }),
        StatusCode::OK,
    )
    .await;

    let updated: RepoResponse = json_request(
        &app,
        Method::PATCH,
        &format!("/api/v1/repos/{}", repo.id),
        json!({
            "local_path": null,
            "remote_url": "https://example.com/acme/clone-me.git"
        }),
        StatusCode::OK,
    )
    .await;

    assert_eq!(updated.local_path, None);
    assert_eq!(
        updated.remote_url.as_deref(),
        Some("https://example.com/acme/clone-me.git")
    );
}

#[tokio::test]
async fn create_repo_rejects_removed_pull_request_work_mode() {
    let app = test_app().await;
    let project: ProjectResponse = json_request(
        &app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "repo-pr-provider" }),
        StatusCode::OK,
    )
    .await;

    let error: Value = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{}/repos", project.id),
        json!({
            "name": "pr-repo",
            "remote_url": "https://gitlab.example.com/acme/pr-repo.git",
            "default_branch": "main",
            "work_mode": "pull_request",
        }),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(error["code"], "validation_error");
    assert!(error["message"]
        .as_str()
        .is_some_and(|message| message.contains("work_mode")));
}

#[tokio::test]
async fn create_repo_sets_and_returns_primary_repo_id() {
    let (app, db) = test_app_with_db().await;
    let project: ProjectResponse = json_request(
        &app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "repo-primary-selection" }),
        StatusCode::OK,
    )
    .await;

    let repo: RepoResponse = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{}/repos", project.id),
        json!({
            "name": "primary-repo",
            "remote_url": "https://example.com/acme/primary-repo.git",
            "default_branch": "main"
        }),
        StatusCode::OK,
    )
    .await;

    let fetched: ProjectResponse = empty_request(
        &app,
        Method::GET,
        &format!("/api/v1/projects/{}", project.id),
        StatusCode::OK,
    )
    .await;

    assert_eq!(fetched.primary_repo_id.as_deref(), Some(repo.id.as_str()));
    let stored = ProjectRepo::get_by_id(&*db, &project.id)
        .await
        .expect("load project")
        .expect("project exists");
    assert_eq!(stored.primary_repo_id.as_deref(), Some(repo.id.as_str()));
}

#[tokio::test]
async fn task_repo_id_is_rejected_and_never_exposed_before_or_after_attach() {
    let (app, _db) = test_app_with_db().await;
    let project: ProjectResponse = json_request(
        &app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "task-repo-selection" }),
        StatusCode::OK,
    )
    .await;

    let _rejected: Value = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{}/tasks", project.id),
        json!({
            "title": "Repository selected by project",
            "repo_id": "client-owned-binding"
        }),
        StatusCode::BAD_REQUEST,
    )
    .await;

    let created: Value = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{}/tasks", project.id),
        json!({ "title": "Repository selected by project" }),
        StatusCode::OK,
    )
    .await;
    assert!(
        created.get("repo_id").is_none(),
        "Task responses must not expose a repository binding: {created}"
    );
    let task_id = created["id"]
        .as_str()
        .expect("created Task has an id")
        .to_owned();

    let _repo: RepoResponse = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{}/repos", project.id),
        json!({
            "name": "primary-repo",
            "remote_url": "https://example.com/acme/primary-repo.git",
            "default_branch": "main"
        }),
        StatusCode::OK,
    )
    .await;

    let fetched: Value = empty_request(
        &app,
        Method::GET,
        &format!("/api/v1/tasks/{task_id}"),
        StatusCode::OK,
    )
    .await;
    assert!(
        fetched.get("repo_id").is_none(),
        "attaching a Project repository must not add a Task binding: {fetched}"
    );
}

async fn test_app() -> Router {
    test_app_with_db().await.0
}

async fn test_app_with_db() -> (Router, Arc<db::SqliteDb>) {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(db::SqliteDb::new(pool));
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
    let event_bus = Arc::new(events::EventBus::new(16));
    let state = AppState::new(db.clone(), event_bus, true);

    let web_dist_dir =
        std::env::temp_dir().join(format!("forge-api-repos-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&web_dist_dir).expect("create web dist dir");
    fs::write(web_dist_dir.join("index.html"), "<html></html>").expect("write index");

    (build_router(state, web_dist_dir), db)
}

async fn json_request<T: DeserializeOwned>(
    app: &Router,
    method: Method,
    uri: &str,
    body: Value,
    expected_status: StatusCode,
) -> T {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {}", test_jwt()))
                .body(Body::from(body.to_string()))
                .expect("build request"),
        )
        .await
        .expect("router response");

    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    assert_eq!(
        status,
        expected_status,
        "unexpected response: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).expect("parse response")
}

async fn empty_request<T: DeserializeOwned>(
    app: &Router,
    method: Method,
    uri: &str,
    expected_status: StatusCode,
) -> T {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("authorization", format!("Bearer {}", test_jwt()))
                .body(Body::empty())
                .expect("build request"),
        )
        .await
        .expect("router response");

    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    assert_eq!(
        status,
        expected_status,
        "unexpected response: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).expect("parse response")
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
        "is_admin": false,
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

fn init_git_repo(path: &Path) {
    fs::create_dir_all(path).expect("create repo dir");
    run_git(path, &["init", "-b", "main"]);
    run_git(path, &["config", "user.email", "test@forge.dev"]);
    run_git(path, &["config", "user.name", "Forge Test"]);
    fs::write(path.join("README.md"), "# Repo\n").expect("write readme");
    run_git(path, &["add", "-A"]);
    run_git(path, &["commit", "-m", "initial"]);
}

fn run_git(path: &Path, args: &[&str]) {
    let output = Command::new("git")
        .args(args)
        .current_dir(path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git {:?} failed\nstdout: {}\nstderr: {}",
        args,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

struct ScopedDir {
    path: PathBuf,
}

impl ScopedDir {
    fn new(prefix: &str) -> Self {
        Self::new_under(&std::env::temp_dir(), prefix)
    }

    fn new_under(root: &Path, prefix: &str) -> Self {
        let path = root.join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn canonical_path(&self) -> PathBuf {
        self.path.canonicalize().expect("canonical path")
    }
}

impl Drop for ScopedDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
