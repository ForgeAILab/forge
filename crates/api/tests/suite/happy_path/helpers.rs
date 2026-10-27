//! Shared happy-path harness, terminal, lead and review-repair helpers.

use super::*;

/// Every producer in the end-to-end flow agrees with its legacy fields and
/// the durable entry, execution, ledger, operation and child witnesses.
pub(super) async fn assert_task_conditions_match_legacy(db: &db::SqliteDb) {
    assert_eq!(
        db.task_condition_violations()
            .await
            .expect("condition sweep runs"),
        Vec::<String>::new(),
        "a Task writer skipped the condition dual-write seam"
    );
}

pub(super) struct TestHarness {
    pub(super) _step_worker: common::StepWorkerGuard,
    pub(super) app: Router,
    pub(super) state: Arc<AppState>,
    pub(super) event_bus: Arc<EventBus>,
    pub(super) _web_dist_dir: common::TestDir,
}

pub(super) async fn test_app(workspace_root: &Path) -> TestHarness {
    test_app_with_registry(workspace_root, cli_adapters::test_support::test_registry()).await
}

pub(super) async fn test_app_with_registry(
    workspace_root: &Path,
    registry: executors::AdapterRegistry,
) -> TestHarness {
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
    let adapter_registry = Arc::new(registry);
    services_agents::ensure_default_agents(db.as_ref(), &adapter_registry)
        .await
        .expect("default agents upsert");
    let event_bus = Arc::new(EventBus::new(256));
    let merge_service = Arc::new(services::MergeService::new_for_test(
        Arc::clone(&db),
        workspace_root.to_path_buf(),
    ));
    let cleanup_scheduler = Arc::new(services::WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        workspace_root.to_path_buf(),
    ));
    let mut state = AppState::with_adapter_registry_services_and_shutdown(
        db,
        Arc::clone(&event_bus),
        true,
        adapter_registry,
        merge_service,
        cleanup_scheduler,
        api::state::ShutdownSignal::new(),
        api::state::test_workflows_dir(),
        api::state::test_jwt_secret(),
        api::state::test_bcrypt_cost(),
    );
    let mut config = (*state.effective_config).clone();
    config.terminal.enabled = true;
    state = state.with_effective_config(config);
    let state = Arc::new(state);

    let web_dist_dir = common::TestDir::new("forge-happy-web");
    std::fs::write(web_dist_dir.path().join("index.html"), "<html></html>").expect("write index");
    let app = build_router((*state).clone(), web_dist_dir.path().to_path_buf());

    TestHarness {
        _step_worker: common::StepWorkerGuard::start(&state),
        app,
        state,
        event_bus,
        _web_dist_dir: web_dist_dir,
    }
}

pub(super) async fn exercise_terminal_session(
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

pub(super) async fn seed_ready_workspace(
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

pub(super) async fn wait_for_terminal_output(
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

pub(super) fn decode_base64_standard(input: &str) -> Vec<u8> {
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

pub(super) async fn setup_git_repo(path: &Path) -> std::path::PathBuf {
    let repo_path = path.join("repo");
    std::fs::create_dir_all(&repo_path).expect("repo dir creates");
    run_git(&repo_path, &["init", "--initial-branch=main"]);
    run_git(&repo_path, &["config", "user.email", "test@forge.dev"]);
    run_git(&repo_path, &["config", "user.name", "Forge Test"]);
    std::fs::write(repo_path.join("README.md"), "# Happy Path\n").expect("README writes");
    run_git(&repo_path, &["add", "-A"]);
    run_git(&repo_path, &["commit", "-m", "initial commit"]);
    repo_path
}

pub(super) async fn register_daemon_and_report_shell(
    state: &AppState,
    workspace_root: &Path,
) -> String {
    common::register_embedded_daemon(state, workspace_root, "happy-path-host", "shell")
        .await
        .daemon_id
}

pub(super) async fn poll_until_execution_completed(db: &Arc<db::SqliteDb>, execution_id: &str) {
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
                    "execution ended in unexpected status: {:?}; error: {:?}",
                    execution.status, execution.error
                );
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("execution did not complete within timeout");
}

pub(super) async fn poll_until_execution_stopped(db: &Arc<db::SqliteDb>, execution_id: &str) {
    for _ in 0..100 {
        if let Some(execution) = db::ExecutionRepo::get_by_id(&**db, execution_id)
            .await
            .expect("execution lookup")
        {
            if execution.status != db::ExecutionStatus::Running {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("execution did not stop within timeout");
}

pub(super) async fn poll_until_workspace_written(
    app: &Router,
    task_id: &str,
    greeting_path: &Path,
) {
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

pub(super) async fn drain_until_task_status(
    harness: &TestHarness,
    task_id: &str,
    expected_status: TaskStatus,
) -> TaskResponse {
    // Executions finish independently; every observation settles the durable
    // cascade queue instead of polling for inline cascade side effects.
    for _ in 0..100 {
        let task = common::drain(&harness.state, &harness.app, task_id).await;
        if task.status == expected_status {
            return task;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let last = common::drain(&harness.state, &harness.app, task_id).await;
    panic!("task did not reach {expected_status:?}; last Task: {last:?}");
}

pub(super) async fn poll_until_task_awaiting_human(app: &Router, task_id: &str) -> TaskResponse {
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

pub(super) async fn single_execution_for_task(
    app: &Router,
    task_id: &str,
) -> ExecutionSummaryResponse {
    let executions: PaginatedResponse<ExecutionSummaryResponse> = empty_request(
        app,
        Method::GET,
        &format!("/api/v1/tasks/{task_id}/executions"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(executions.items.len(), 1);
    executions.items.into_iter().next().unwrap()
}

pub(super) async fn drain_until_follow_up_execution(
    harness: &TestHarness,
    task_id: &str,
    parent_execution_id: &str,
) -> ExecutionSummaryResponse {
    for _ in 0..100 {
        common::drain(&harness.state, &harness.app, task_id).await;
        let executions: PaginatedResponse<ExecutionSummaryResponse> = empty_request(
            &harness.app,
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

pub(super) async fn drain_events(
    rx: &mut tokio::sync::broadcast::Receiver<ForgeEvent>,
) -> Vec<ForgeEvent> {
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

pub(super) fn assert_event_type(events: &[ForgeEvent], event_type: &str) {
    assert!(
        events.iter().any(|event| event.event_type == event_type),
        "missing event {event_type}; got {:?}",
        events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>()
    );
}

pub(super) fn assert_status_event(events: &[ForgeEvent], task_id: &str, expected_status: &str) {
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

pub(super) async fn json_request<T>(
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

pub(super) async fn json_request_with_bearer<T>(
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

/// Posts a versioned Task mutation, refetching the version when a background
/// writer wins the race. Task writes are optimistically concurrent, so a 409
/// here means the snapshot this call was built from went stale — exactly what a
/// real client refetches and resubmits — rather than a product failure.
pub(super) async fn versioned_task_request<T>(
    app: &Router,
    uri: &str,
    task_id: &str,
    mut body: Value,
) -> T
where
    T: DeserializeOwned,
{
    let mut last_snapshot = None;
    for _ in 0..40 {
        let response = raw_json_request(app, Method::POST, uri, body.clone()).await;
        if response.status() != StatusCode::CONFLICT {
            return parse_response(response, StatusCode::OK).await;
        }
        let current: TaskResponse = empty_request(
            app,
            Method::GET,
            &format!("/api/v1/tasks/{task_id}"),
            StatusCode::OK,
        )
        .await;
        body["version"] = json!(current.version);
        last_snapshot = Some(current);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "versioned request to {uri} never won the version race; last snapshot: {last_snapshot:?}"
    );
}

pub(super) async fn empty_request<T>(
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

pub(super) fn test_jwt() -> String {
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

pub(super) async fn raw_json_request(
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

pub(super) async fn raw_empty_request(
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

pub(super) async fn parse_response<T>(
    response: axum::response::Response,
    expected_status: StatusCode,
) -> T
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

pub(super) fn run_git(path: &Path, args: &[&str]) -> String {
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

/// Like model_workflow's scripted native lead: the native profile names the
/// test adapter, so planning and step checks run without a provider. The kickoff
/// stays live until the test releases it; the checks return fail, pass, pass.
pub(super) struct HappyPathLead {
    pub(super) kickoff: Arc<tokio::sync::Semaphore>,
    pub(super) checks: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl executors::CodingExecutorAdapter for HappyPathLead {
    fn kind(&self) -> executors::ExecutorKind {
        executors::ExecutorKind::Codex
    }

    fn check_availability(&self) -> executors::AvailabilityInfo {
        executors::AvailabilityInfo {
            status: executors::AvailabilityStatus::Authenticated,
            authenticated_at: None,
            config_path: None,
        }
    }

    async fn discover_options(
        &self,
        _ctx: executors::DiscoverContext,
    ) -> Result<executors::DiscoveredOptions, executors::ExecutorError> {
        Ok(executors::DiscoveredOptions::default())
    }

    async fn execute(
        &self,
        ctx: executors::ExecutionContext,
    ) -> Result<executors::ExecutionResult, executors::ExecutorError> {
        let reply = if ctx.description.contains("Step-check doctrine:") {
            if self
                .checks
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                == 0
            {
                "The first step needs its second line.\n{\"result\":\"fail\",\"reason\":\"append the second line\"}"
            } else {
                "The step is accepted.\n{\"result\":\"pass\",\"reason\":\"deliverable verified\"}"
            }
        } else {
            assert!(ctx
                .description
                .contains("You are the lead of this Forge coordination root"));
            self.kickoff.acquire().await.unwrap().forget();
            "The breakdown is ready; run the two steps in order."
        };
        if let Some(parent) = Path::new(&ctx.logs_path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        executors::LogWriter::new(&ctx.logs_path, ctx.execution_id, 1024 * 1024)
            .write(
                executors::LogKind::Assistant,
                executors::LogStream::Main,
                json!({"text": reply}),
            )
            .await?;
        Ok(executors::ExecutionResult {
            status: executors::ExecutionOutcome::Completed,
            summary: Some(reply.to_owned()),
            agent_session_id: Some("happy-path-lead-session".to_owned()),
            ..Default::default()
        })
    }

    async fn cancel(&self, _execution_id: &str) -> Result<(), executors::ExecutorError> {
        Ok(())
    }
}

pub(super) async fn drive_lead_happy_path(
    harness: &TestHarness,
    dispatcher: &services::TaskDispatcher,
    task_id: &str,
    status: &str,
) -> TaskResponse {
    for _ in 0..200 {
        dispatcher
            .check_once()
            .await
            .expect("dispatcher reconciles");
        let task = common::drain(&harness.state, &harness.app, task_id).await;
        if task.status == status {
            return task;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let last = common::drain(&harness.state, &harness.app, task_id).await;
    panic!("lead happy path did not reach {status}; last Task: {last:?}");
}

/// Real shell workers and the durable review/integration owners, with no model
/// provider: each named case supplies its own candidate and reviewer scripts.
pub(super) async fn repair_release_project(
    harness: &TestHarness,
    repo_path: &Path,
    workspaces: &Path,
) -> (ProjectResponse, AgentResponse) {
    let project: ProjectResponse = json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({"name":"Repair release"}),
        StatusCode::OK,
    )
    .await;
    let repo: RepoResponse = json_request(&harness.app, Method::POST, &format!("/api/v1/projects/{}/repos", project.id), json!({"name":"repo","local_path":repo_path.to_string_lossy(),"remote_url":repo_path.to_string_lossy(),"default_branch":"main"}), StatusCode::OK).await;
    let daemon = register_daemon_and_report_shell(&harness.state, workspaces).await;
    let agent: AgentResponse = json_request(
        &harness.app,
        Method::POST,
        "/api/v1/agents",
        json!({"name":"repair-release-shell","executor_type":"shell","daemon_id":daemon}),
        StatusCode::OK,
    )
    .await;
    common::configure_execution_test_setup(
        &harness.state.db,
        &project.id,
        &repo.id,
        &agent.id,
        &agent.id,
    )
    .await;
    (project, agent)
}
