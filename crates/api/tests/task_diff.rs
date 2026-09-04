#![allow(dead_code, clippy::assertions_on_constants)]
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

use api::{build_router, AppState};
use api_types::{DiffEnvelope, ErrorResponse, TaskResponse};
use axum::{
    body::{to_bytes, Body},
    http::{Method, Request, StatusCode},
    Router,
};
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentRepo, CreateAgent,
    CreateProject, CreateRepo, DaemonRepo, DaemonStatus, ProjectRepo, RepoRepo, SqliteDb,
    UpdateDaemonReport, UpdateProject, UpsertDaemon,
};
use serde::de::DeserializeOwned;
use serde_json::json;
use tower::ServiceExt;

#[tokio::test]
async fn task_diff_endpoint_returns_workspace_diff_and_handles_missing_workspace() {
    let app = test_app().await;
    let (project_id, _repo_id, repo_dir, default_branch) = create_project_and_repo(&app).await;
    let agent_id = create_agent(&app).await;

    let task: TaskResponse = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Task with diff" }),
        StatusCode::OK,
    )
    .await;
    let missing_workspace_error: ErrorResponse = empty_request(
        &app,
        Method::GET,
        &format!("/api/v1/tasks/{}/diff", task.id),
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(missing_workspace_error.code, "workspace.not_found");

    let launched = app
        .state
        .task_service
        .launch_execution(task.id.clone(), agent_id, None, None)
        .await
        .expect("launch succeeds");
    let workspace_path = PathBuf::from(&launched.workspace.worktree_path);
    std::fs::write(workspace_path.join("README.md"), "# Task diff\nupdated\n")
        .expect("workspace file writes");

    let diff: DiffEnvelope = empty_request(
        &app,
        Method::GET,
        &format!("/api/v1/tasks/{}/diff", task.id),
        StatusCode::OK,
    )
    .await;
    let base_sha = launched
        .workspace
        .before_sha
        .clone()
        .expect("workspace records base sha");
    assert_eq!(diff.data.base_sha, base_sha);
    assert_eq!(diff.data.base_ref, base_ref(&default_branch, &base_sha));
    assert!(
        diff.data.files.iter().any(|file| file.path == "README.md"),
        "diff response should include the modified file",
    );
    assert!(diff.data.diff.contains("README.md"));

    let workspace_diff: DiffEnvelope = empty_request(
        &app,
        Method::GET,
        &format!("/api/v1/workspaces/{}/diff", launched.workspace.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(workspace_diff.data.files.len(), diff.data.files.len());

    let _ = repo_dir;
}

#[tokio::test]
async fn task_diff_uses_merge_base_when_default_branch_advances() {
    let app = test_app().await;
    let (project_id, _repo_id, repo_dir, default_branch) = create_project_and_repo(&app).await;
    let agent_id = create_agent(&app).await;

    let task: TaskResponse = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Task with stable diff base" }),
        StatusCode::OK,
    )
    .await;
    let launched = app
        .state
        .task_service
        .launch_execution(task.id.clone(), agent_id, None, None)
        .await
        .expect("launch succeeds");
    let base_sha = launched
        .workspace
        .before_sha
        .clone()
        .expect("workspace records base sha");

    std::fs::write(repo_dir.join("main-only.txt"), "landed on default branch\n")
        .expect("default branch file writes");
    run_git(&repo_dir, &["add", "-A"]);
    run_git(&repo_dir, &["commit", "-m", "advance default branch"]);

    let workspace_path = PathBuf::from(&launched.workspace.worktree_path);
    std::fs::write(
        workspace_path.join("README.md"),
        "# Task diff\nworkspace update\n",
    )
    .expect("workspace file writes");

    let diff: DiffEnvelope = empty_request(
        &app,
        Method::GET,
        &format!("/api/v1/tasks/{}/diff", task.id),
        StatusCode::OK,
    )
    .await;

    assert_eq!(diff.data.base_sha, base_sha);
    assert_eq!(diff.data.base_ref, base_ref(&default_branch, &base_sha));
    assert!(
        diff.data.files.iter().any(|file| file.path == "README.md"),
        "diff response should include the task work",
    );
    assert!(
        !diff
            .data
            .files
            .iter()
            .any(|file| file.path == "main-only.txt"),
        "diff response should not include default-branch-only changes",
    );
    assert!(!diff.data.diff.contains("main-only.txt"));
}

struct Harness {
    app: Router,
    state: AppState,
}

async fn test_app() -> Harness {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(SqliteDb::new(pool));
    let state = AppState::new(db, Arc::new(events::EventBus::new(32)), true);
    let web_dist_dir = std::env::temp_dir().join(format!("forge-task-diff-web-{}", new_uuid_v4()));
    std::fs::create_dir_all(&web_dist_dir).expect("web dir creates");
    std::fs::write(
        web_dist_dir.join("index.html"),
        "<!doctype html><html></html>",
    )
    .expect("index writes");
    let app = build_router(state.clone(), web_dist_dir);
    Harness { app, state }
}

async fn create_project_and_repo(harness: &Harness) -> (String, String, PathBuf, String) {
    let now = now_rfc3339();
    let project = ProjectRepo::create(
        &*harness.state.db,
        CreateProject {
            id: new_uuid_v4(),
            name: "Task Diff".to_owned(),
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

    let repo_dir = std::env::temp_dir().join(format!("forge-task-diff-repo-{}", new_uuid_v4()));
    std::fs::create_dir_all(&repo_dir).expect("repo dir creates");
    setup_git_repo(&repo_dir);
    let default_branch = run_git(&repo_dir, &["symbolic-ref", "--short", "HEAD"]);
    let repo = RepoRepo::create(
        &*harness.state.db,
        CreateRepo {
            id: new_uuid_v4(),
            project_id: project.id.clone(),
            name: "task-diff".to_owned(),
            local_path: Some(repo_dir.to_string_lossy().to_string()),
            work_mode: db::WorkMode::DirectMerge,
            remote_url: repo_dir.to_string_lossy().to_string(),
            default_branch: default_branch.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        &*harness.state.db,
        UpdateProject {
            id: project.id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo.id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(&*harness.state.db, &project.id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project primary repo updates");

    (project.id, repo.id, repo_dir, default_branch)
}

async fn create_agent(harness: &Harness) -> String {
    let now = now_rfc3339();
    let daemon = DaemonRepo::upsert_by_machine_id(
        &*harness.state.db,
        UpsertDaemon {
            id: new_uuid_v4(),
            machine_id: services::embedded_daemon::embedded_machine_id(),
            hostname: "localhost".to_owned(),
            os: "linux".to_owned(),
            arch: "x86_64".to_owned(),
            agent_version: None,
            labels_json: "{}".to_owned(),
            status: DaemonStatus::Online,
            registration_token_hash: None,
            owner_id: None,
            visibility: "global".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("daemon creates");
    DaemonRepo::update_report(
        &*harness.state.db,
        UpdateDaemonReport {
            id: daemon.id.clone(),
            last_report_at: now.clone(),
            status: DaemonStatus::Online,
            detected_clis_json: r#"[{"kind":"shell","availability":"authenticated","version":null,"path":"/bin/sh"}]"#.to_owned(),
            labels_json: None,
            updated_at: now.clone(),
        },
    )
    .await
    .expect("daemon report updates");

    let agent_id = new_uuid_v4();
    AgentRepo::create(
        &*harness.state.db,
        CreateAgent {
            id: agent_id.clone(),
            name: "shell-agent".to_owned(),
            description: None,
            executor_type: "shell".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: Some(daemon.id),
            max_concurrent_tasks: 2,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: db::AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("agent creates");
    agent_id
}

fn setup_git_repo(path: &Path) {
    run_git(path, &["init"]);
    run_git(path, &["config", "user.email", "test@forge.dev"]);
    run_git(path, &["config", "user.name", "Forge Test"]);
    std::fs::write(path.join("README.md"), "# Task diff\n").expect("README writes");
    run_git(path, &["add", "-A"]);
    run_git(path, &["commit", "-m", "init"]);
}

fn run_git(path: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .expect("git runs");
    assert!(
        output.status.success(),
        "git {:?} failed\nstdout:\n{}\nstderr:\n{}",
        args,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    String::from_utf8(output.stdout)
        .expect("utf8")
        .trim()
        .to_owned()
}

fn base_ref(default_branch: &str, base_sha: &str) -> String {
    format!(
        "{default_branch}@{}",
        base_sha.get(..12).unwrap_or(base_sha)
    )
}

async fn json_request<T: DeserializeOwned>(
    harness: &Harness,
    method: Method,
    uri: &str,
    payload: serde_json::Value,
    expected_status: StatusCode,
) -> T {
    let response = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {}", test_jwt()))
                .body(Body::from(payload.to_string()))
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), expected_status);
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body reads");
    serde_json::from_slice(&bytes).expect("json parses")
}

async fn empty_request<T: DeserializeOwned>(
    harness: &Harness,
    method: Method,
    uri: &str,
    expected_status: StatusCode,
) -> T {
    let response = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("authorization", format!("Bearer {}", test_jwt()))
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), expected_status);
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body reads");
    serde_json::from_slice(&bytes).expect("json parses")
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
