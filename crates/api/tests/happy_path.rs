#![allow(dead_code, clippy::assertions_on_constants)]
mod common;

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use api::{build_router, AppState};
use api_types::{
    AgentResponse, CreateTerminalSessionResponse, DaemonRegisterResponse, DaemonResponse,
    ExecutionResponse, ExecutionStatus, PaginatedResponse, ProjectResponse, RepoResponse,
    TaskResponse, TaskStatus, TerminalAttachTokenResponse, TerminalServerFrame,
    TerminalSessionResponse, TerminalSessionStatus,
};
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use db::{ProjectHookRunRepo, ReviewRepo, TaskRepo};
use events::{EventBus, EventContext, ForgeEvent, PROJECT_HOOK_RUN_CHANGED_EVENT};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tower::ServiceExt;

#[tokio::test]
async fn forge_happy_path_end_to_end() {
    let repo_dir = TestDir::new("forge-happy-repo");
    let repo_path = setup_git_repo(repo_dir.path()).await;
    let default_branch = run_git(&repo_path, &["symbolic-ref", "--short", "HEAD"]);

    let workspaces_root = TestDir::new("forge-happy-workspaces");
    let harness = test_app(workspaces_root.path()).await;
    let mut events_rx = harness.event_bus.subscribe();

    let project: ProjectResponse = json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Happy Path" }),
        StatusCode::OK,
    )
    .await;
    let project_id = project.id;

    let repo: RepoResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/repos"),
        json!({
            "name": "repo",
            "local_path": repo_path.to_string_lossy(),
            "remote_url": repo_path.to_string_lossy(),
            "default_branch": default_branch
        }),
        StatusCode::OK,
    )
    .await;
    let repo_id = repo.id;
    assert!(repo.local_path.is_some());
    let expected_local_path = repo_path.canonicalize().expect("canonical repo path");
    let returned_local_path = repo
        .local_path
        .as_deref()
        .map(std::path::PathBuf::from)
        .and_then(|path| path.canonicalize().ok());
    assert_eq!(
        returned_local_path.as_deref(),
        Some(expected_local_path.as_path())
    );
    assert_eq!(repo.remote_url, repo_path.to_string_lossy().as_ref());

    let daemon_id = register_daemon_and_report_shell(&harness.app, workspaces_root.path()).await;
    let agent: AgentResponse = json_request(
        &harness.app,
        Method::POST,
        "/api/v1/agents",
        json!({
            "name": "shell-agent",
            "executor_type": "shell",
            "daemon_id": daemon_id,
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(agent.effective_status.as_deref(), Some("active"));
    let agent_id = agent.id;
    common::configure_execution_test_setup(
        &harness.state.db,
        &project_id,
        &repo_id,
        &agent_id,
        &agent_id,
    )
    .await;

    let created_task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Happy path task",
            "description": "echo hello > greeting.txt && git add . && git commit -m 'hi'",
            "review_config": { "ci_steps": ["test -f greeting.txt"] }
        }),
        StatusCode::OK,
    )
    .await;
    let task_id = created_task.id;
    assert_eq!(created_task.status, "todo".to_owned());
    assert_eq!(created_task.version, 1);

    let claimed: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/claim"),
        json!({ "agent_id": agent_id, "overrides": null }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(claimed.status, "in_progress".to_owned());
    assert_eq!(claimed.version, 2);

    let execution = single_execution_for_task(&harness.app, &task_id).await;
    let execution_id = execution.id.clone();

    let worktree_path = workspaces_root.path().join(&task_id).join("repo");
    let greeting_path = worktree_path.join("greeting.txt");
    poll_until_workspace_written(&harness.app, &task_id, &greeting_path).await;
    poll_until_execution_completed(&harness.state.db, &execution_id).await;

    let completed = poll_until_task_status(&harness.app, &task_id, "done".to_owned()).await;
    assert_eq!(
        completed.status,
        "done".to_owned(),
        "review-pass auto-cascades through merging to done"
    );

    let latest_subject = run_git(&repo_path, &["log", "-1", "--format=%s"]);
    assert!(
        latest_subject.contains("hi") || latest_subject.contains(&task_id),
        "latest git subject references the task change: {latest_subject}"
    );
    assert!(
        !workspaces_root.path().join(&task_id).exists(),
        "workspace task directory is cleaned"
    );

    let reviews = ReviewRepo::list_by_task(&*harness.state.db, &task_id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, db::ReviewStatus::Passed);
    assert_eq!(reviews[0].attempt_number, 1);
    let step_results: serde_json::Value =
        serde_json::from_str(&reviews[0].step_results_json).expect("step results parse");
    let ci_steps = step_results
        .get("ci_steps")
        .cloned()
        .unwrap_or(step_results.clone());
    assert_eq!(ci_steps.as_array().expect("step results array").len(), 1);
    assert_eq!(ci_steps[0]["exit_code"], 0);

    let listed_tasks: PaginatedResponse<TaskResponse> = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}/tasks"),
        StatusCode::OK,
    )
    .await;
    assert!(
        listed_tasks.items.iter().any(|task| task.id == task_id),
        "normal non-automation task remains visible in project task list"
    );
    let persisted_task = TaskRepo::get_by_id(&*harness.state.db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(
        !persisted_task.is_automation,
        "happy-path user-created task defaults to non-automation"
    );

    let hook_runs =
        ProjectHookRunRepo::list_recent_for_project(&*harness.state.db, &project_id, 10)
            .await
            .expect("project hook runs load");
    assert!(
        hook_runs.is_empty(),
        "projects without configured hooks must not create project hook runs"
    );

    exercise_terminal_session(&harness, workspaces_root.path(), &project_id, &repo_id).await;

    let events = drain_events(&mut events_rx).await;
    assert_event_type(&events, "task.created");
    assert_event_type(&events, "task.assigned");
    assert_status_event(&events, &task_id, "in_progress");
    assert_event_type(&events, "task.auto_transitioned");
    assert_event_type(&events, "review.passed");
    assert_status_event(&events, &task_id, "review");
    assert_status_event(&events, &task_id, "merging");
    assert_status_event(&events, &task_id, "done");
    assert_event_type(&events, "workspace.cleaned");
    assert!(
        events
            .iter()
            .all(|event| event.event_type != PROJECT_HOOK_RUN_CHANGED_EVENT),
        "no project_hook.run_changed events are emitted when no hooks are configured"
    );
}

#[tokio::test]
async fn autonomous_workflow_requires_human_review_and_resumes_worker_on_reject() {
    let repo_dir = TestDir::new("forge-autonomous-repo");
    let repo_path = setup_git_repo(repo_dir.path()).await;
    let default_branch = run_git(&repo_path, &["symbolic-ref", "--short", "HEAD"]);
    let workspaces_root = TestDir::new("forge-autonomous-workspaces");
    let harness = test_app(workspaces_root.path()).await;
    harness
        .state
        .workflow_template_service
        .initialize()
        .await
        .expect("builtin workflow templates initialize");

    let project: ProjectResponse = json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Autonomous End to End" }),
        StatusCode::OK,
    )
    .await;
    let project_id = project.id;
    let repo: RepoResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/repos"),
        json!({
            "name": "repo",
            "local_path": repo_path.to_string_lossy(),
            "remote_url": repo_path.to_string_lossy(),
            "default_branch": default_branch
        }),
        StatusCode::OK,
    )
    .await;
    let _: Value = json_request(
        &harness.app,
        Method::PUT,
        &format!("/api/v1/projects/{project_id}/workflow"),
        json!({ "template_name": "autonomous_v1" }),
        StatusCode::OK,
    )
    .await;

    let daemon_id = register_daemon_and_report_shell(&harness.app, workspaces_root.path()).await;
    let agent: AgentResponse = json_request(
        &harness.app,
        Method::POST,
        "/api/v1/agents",
        json!({
            "name": "autonomous-shell-agent",
            "executor_type": "shell",
            "daemon_id": daemon_id,
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(agent.effective_status.as_deref(), Some("active"));
    common::configure_execution_test_setup(
        &harness.state.db,
        &project_id,
        &repo.id,
        &agent.id,
        &agent.id,
    )
    .await;

    let task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({
            "title": "Autonomous delivery",
            "description": "printf 'autonomous\\n' > autonomous.txt && git add autonomous.txt && git commit -m autonomous",
            "review_config": { "ci_steps": [] }
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(task.status, "ready".to_owned());

    let claimed: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/claim", task.id),
        json!({ "agent_id": agent.id, "overrides": null }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(claimed.status, "working".to_owned());
    let first_execution = single_execution_for_task(&harness.app, &task.id).await;
    assert_eq!(first_execution.role.to_string(), "worker");

    let review_task = poll_until_task_awaiting_human(&harness.app, &task.id).await;

    let approved: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/gates/review/approve", task.id),
        json!({ "version": review_task.version, "reason": "human approval" }),
        StatusCode::OK,
    )
    .await;
    assert!(matches!(approved.status.as_str(), "merging" | "done"));
    let completed = poll_until_task_status(&harness.app, &task.id, "done".to_owned()).await;
    assert_eq!(completed.status, "done".to_owned());

    let ci_failure_task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({
            "title": "Autonomous validation retry",
            "description": "printf 'validation retry\\n' > autonomous-ci-retry.txt && git add autonomous-ci-retry.txt && git commit --allow-empty -m autonomous-ci-retry",
            "review_config": { "ci_steps": ["false"] }
        }),
        StatusCode::OK,
    )
    .await;
    let claimed_ci_failure: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/claim", ci_failure_task.id),
        json!({ "agent_id": agent.id, "overrides": null }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(claimed_ci_failure.status, "working".to_owned());
    let first_ci_execution = single_execution_for_task(&harness.app, &ci_failure_task.id).await;

    let failed_follow_up =
        poll_until_follow_up_execution(&harness.app, &ci_failure_task.id, &first_ci_execution.id)
            .await;
    assert_eq!(
        failed_follow_up.parent_execution_id.as_deref(),
        Some(first_ci_execution.id.as_str())
    );
    let after_ci_failure: TaskResponse = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", ci_failure_task.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(after_ci_failure.status, "working".to_owned());
    assert!(!after_ci_failure.awaiting_human);

    let passing_review_config = serde_json::to_string(&json!({
        "review": { "ci_steps": ["true"] }
    }))
    .expect("passing review config serializes");
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(passing_review_config)
        .bind(&ci_failure_task.id)
        .execute(harness.state.db.pool())
        .await
        .expect("passing review config updates");
    let _: Value = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/transition", ci_failure_task.id),
        json!({
            "status": "review",
            "version": after_ci_failure.version,
            "reason": "retry validation after CI configuration was corrected"
        }),
        StatusCode::OK,
    )
    .await;

    let ci_review = poll_until_task_awaiting_human(&harness.app, &ci_failure_task.id).await;
    let ci_approved: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/gates/review/approve", ci_failure_task.id),
        json!({ "version": ci_review.version, "reason": "human approval" }),
        StatusCode::OK,
    )
    .await;
    assert!(matches!(ci_approved.status.as_str(), "merging" | "done"));
    let ci_completed =
        poll_until_task_status(&harness.app, &ci_failure_task.id, "done".to_owned()).await;
    assert_eq!(ci_completed.status, "done".to_owned());
    let ci_reviews = ReviewRepo::list_by_task(&*harness.state.db, &ci_failure_task.id)
        .await
        .expect("CI retry reviews load");
    assert!(
        ci_reviews
            .iter()
            .any(|review| review.status == db::ReviewStatus::Failed),
        "the first validation attempt should be recorded as failed"
    );

    let second_task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({
            "title": "Autonomous requested changes",
            "description": "printf 'requested changes\\n' > autonomous-rejected.txt && git add autonomous-rejected.txt && git commit -m requested-changes",
            "review_config": { "ci_steps": [] }
        }),
        StatusCode::OK,
    )
    .await;
    let claimed_second: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/claim", second_task.id),
        json!({ "agent_id": agent.id, "overrides": null }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(claimed_second.status, "working".to_owned());
    let second_execution = single_execution_for_task(&harness.app, &second_task.id).await;
    let second_review = poll_until_task_awaiting_human(&harness.app, &second_task.id).await;

    let rejected: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/gates/review/reject", second_task.id),
        json!({
            "version": second_review.version,
            "reason": "Please add evidence for the requested behavior"
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(rejected.status, "working".to_owned());

    let resumed =
        poll_until_follow_up_execution(&harness.app, &second_task.id, &second_execution.id).await;
    assert_eq!(resumed.role.to_string(), "worker");
    assert_eq!(
        resumed.parent_execution_id.as_deref(),
        Some(second_execution.id.as_str())
    );
}

struct TestHarness {
    app: Router,
    state: Arc<AppState>,
    event_bus: Arc<EventBus>,
    _web_dist_dir: TestDir,
}

async fn test_app(workspace_root: &Path) -> TestHarness {
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
            is_admin: true,
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
    let mut state = AppState::with_adapter_registry_services_and_shutdown(
        db,
        Arc::clone(&event_bus),
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
    let mut config = (*state.effective_config).clone();
    config.terminal.enabled = true;
    state = state.with_effective_config(config);
    let state = Arc::new(state);

    let web_dist_dir = TestDir::new("forge-happy-web");
    std::fs::write(web_dist_dir.path().join("index.html"), "<html></html>").expect("write index");
    let app = build_router((*state).clone(), web_dist_dir.path().to_path_buf());

    TestHarness {
        app,
        state,
        event_bus,
        _web_dist_dir: web_dist_dir,
    }
}

async fn exercise_terminal_session(
    harness: &TestHarness,
    workspace_root: &Path,
    project_id: &str,
    repo_id: &str,
) {
    let task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Terminal happy path", "description": "terminal smoke" }),
        StatusCode::OK,
    )
    .await;
    seed_ready_workspace(&harness.state.db, workspace_root, &task.id, repo_id).await;

    let created: CreateTerminalSessionResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/terminals", task.id),
        json!({ "rows": 24, "cols": 80 }),
        StatusCode::CREATED,
    )
    .await;
    let refreshed: TerminalAttachTokenResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/terminals/{}/attach-token", created.session.id),
        json!({}),
        StatusCode::OK,
    )
    .await;
    assert_ne!(created.attach.attach_token, refreshed.attach_token);

    let mut live_rx = harness
        .state
        .terminal_service
        .attach_client(&created.session.id)
        .await;
    harness
        .state
        .terminal_service
        .handle_terminal_input(&created.session.id, "ZWNobyBmb3JnZS10ZXJtaW5hbC1vawo=")
        .await
        .expect("terminal input accepted");
    wait_for_terminal_output(&mut live_rx, "forge-terminal-ok").await;
    drop(live_rx);

    let mut replay_rx = harness
        .state
        .terminal_service
        .attach_client(&created.session.id)
        .await;
    wait_for_terminal_output(&mut replay_rx, "forge-terminal-ok").await;

    let terminated: TerminalSessionResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/terminals/{}/terminate", created.session.id),
        json!({ "reason": "happy_path" }),
        StatusCode::OK,
    )
    .await;
    assert!(matches!(
        terminated.status,
        TerminalSessionStatus::Terminated
    ));
}

async fn seed_ready_workspace(
    db: &Arc<db::SqliteDb>,
    workspace_root: &Path,
    task_id: &str,
    repo_id: &str,
) {
    let now = db::now_rfc3339();
    let worktree_path = workspace_root.join(task_id).join("terminal-repo");
    std::fs::create_dir_all(&worktree_path).expect("terminal worktree creates");
    db::WorkspaceRepo::create(
        &**db,
        db::CreateWorkspace {
            id: db::new_uuid_v4(),
            task_id: task_id.to_owned(),
            repo_id: repo_id.to_owned(),
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch: workspace::task_branch_name(task_id),
            status: db::WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("terminal workspace creates");
}

async fn wait_for_terminal_output(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<TerminalServerFrame>,
    needle: &str,
) {
    let mut collected = String::new();
    for _ in 0..20 {
        if let Ok(Some(TerminalServerFrame::Output { data })) =
            tokio::time::timeout(Duration::from_millis(250), rx.recv()).await
        {
            collected.push_str(&String::from_utf8_lossy(&decode_base64_standard(&data)));
            if collected.contains(needle) {
                return;
            }
        }
    }
    panic!("terminal output did not contain {needle}; collected {collected:?}");
}

fn decode_base64_standard(input: &str) -> Vec<u8> {
    let mut out = Vec::new();
    let mut buffer = 0_u32;
    let mut bits = 0_u8;
    for byte in input.bytes() {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => break,
            _ => continue,
        };
        buffer = (buffer << 6) | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(((buffer >> bits) & 0xff) as u8);
        }
    }
    out
}

async fn setup_git_repo(path: &Path) -> std::path::PathBuf {
    let repo_path = path.join("repo");
    std::fs::create_dir_all(&repo_path).expect("repo dir creates");
    run_git(&repo_path, &["init"]);
    run_git(&repo_path, &["config", "user.email", "test@forge.dev"]);
    run_git(&repo_path, &["config", "user.name", "Forge Test"]);
    std::fs::write(repo_path.join("README.md"), "# Happy Path\n").expect("README writes");
    run_git(&repo_path, &["add", "-A"]);
    run_git(&repo_path, &["commit", "-m", "initial commit"]);
    repo_path
}

async fn register_daemon_and_report_shell(app: &Router, workspace_root: &Path) -> String {
    let registration: DaemonRegisterResponse = json_request(
        app,
        Method::POST,
        "/api/v1/daemons/register",
        json!({
            "machine_id": services::embedded_daemon::embedded_machine_id(),
            "hostname": "happy-path-host",
            "os": "linux",
            "arch": "x86_64",
            "agent_version": "happy-path-test",
            "labels": { "suite": "happy_path" }
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

    daemon_id
}

async fn poll_until_execution_completed(db: &Arc<db::SqliteDb>, execution_id: &str) {
    for _ in 0..100 {
        if let Some(execution) = db::ExecutionRepo::get_by_id(&**db, execution_id)
            .await
            .expect("execution lookup")
        {
            if execution.status == db::ExecutionStatus::Completed {
                return;
            }
            if execution.status != db::ExecutionStatus::Running {
                panic!(
                    "execution ended in unexpected status: {:?}",
                    execution.status
                );
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("execution did not complete within timeout");
}

async fn poll_until_workspace_written(app: &Router, task_id: &str, greeting_path: &Path) {
    for _ in 0..100 {
        if greeting_path.exists() {
            return;
        }
        let execution = single_execution_for_task(app, task_id).await;
        if execution.status != ExecutionStatus::Running {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        greeting_path.exists(),
        "greeting.txt was not written before execution stopped"
    );
}

async fn poll_until_task_status(
    app: &Router,
    task_id: &str,
    expected_status: TaskStatus,
) -> TaskResponse {
    for _ in 0..100 {
        let task: TaskResponse = empty_request(
            app,
            Method::GET,
            &format!("/api/v1/tasks/{task_id}"),
            StatusCode::OK,
        )
        .await;
        if task.status == expected_status {
            return task;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("task did not reach {expected_status:?} within timeout");
}

async fn poll_until_task_awaiting_human(app: &Router, task_id: &str) -> TaskResponse {
    for _ in 0..100 {
        let task: TaskResponse = empty_request(
            app,
            Method::GET,
            &format!("/api/v1/tasks/{task_id}"),
            StatusCode::OK,
        )
        .await;
        if task.status == "review" && task.awaiting_human {
            return task;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("task did not reach human review within timeout");
}

async fn single_execution_for_task(app: &Router, task_id: &str) -> ExecutionResponse {
    let executions: PaginatedResponse<ExecutionResponse> = empty_request(
        app,
        Method::GET,
        &format!("/api/v1/tasks/{task_id}/executions"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(executions.items.len(), 1);
    executions.items.into_iter().next().unwrap()
}

async fn poll_until_follow_up_execution(
    app: &Router,
    task_id: &str,
    parent_execution_id: &str,
) -> ExecutionResponse {
    for _ in 0..100 {
        let executions: PaginatedResponse<ExecutionResponse> = empty_request(
            app,
            Method::GET,
            &format!("/api/v1/tasks/{task_id}/executions"),
            StatusCode::OK,
        )
        .await;
        if let Some(execution) = executions
            .items
            .into_iter()
            .find(|execution| execution.parent_execution_id.as_deref() == Some(parent_execution_id))
        {
            return execution;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("worker follow-up execution was not recorded");
}

async fn drain_events(rx: &mut tokio::sync::broadcast::Receiver<ForgeEvent>) -> Vec<ForgeEvent> {
    let mut events = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Ok(event)) => events.push(event),
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => continue,
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) | Err(_) => break,
        }
    }
    events
}

fn assert_event_type(events: &[ForgeEvent], event_type: &str) {
    assert!(
        events.iter().any(|event| event.event_type == event_type),
        "missing event {event_type}; got {:?}",
        events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>()
    );
}

fn assert_status_event(events: &[ForgeEvent], task_id: &str, expected_status: &str) {
    assert!(
        events.iter().any(|event| {
            event.event_type == "task.status_changed"
                && event.entity_id == task_id
                && matches!(
                    &event.context,
                    EventContext::TaskStatusChanged { new_status, .. }
                        if new_status == expected_status
                )
        }),
        "missing task.status_changed to {expected_status}"
    );
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

async fn json_request_with_bearer<T>(
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

async fn empty_request<T>(app: &Router, method: Method, uri: &str, expected_status: StatusCode) -> T
where
    T: DeserializeOwned,
{
    let response = raw_empty_request(app, method, uri).await;
    parse_response(response, expected_status).await
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

fn run_git(path: &Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
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
