use std::{
    collections::{BTreeMap, HashMap},
    io::ErrorKind,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use api_types::{
    AgentResponse, AgentStatus, CanonicalPhase, ClaimTaskRequest, CreateAgentRequest,
    CreateProjectRequest, CreateRepoRequest, CreateTaskRequest, DaemonResponse,
    ExecutionEvidenceSummary, PaginatedResponse, ProjectResponse, RepoResponse,
    TaskExecutionObservability, TaskResponse, TaskType, WorkMode,
};
use axum::{
    extract::{Path as AxumPath, State},
    http::StatusCode,
    routing::{delete, get, post},
    Json, Router,
};
use forge_client::client::ForgeClient;

#[tokio::test]
async fn forge_client_creates_and_gets_project() {
    let Some(server) = TestServer::spawn().await else {
        return;
    };

    let created: ProjectResponse = server
        .client
        .post(
            "/api/v1/projects",
            &CreateProjectRequest {
                name: "Client Project".to_owned(),
                settings: None,
                default_review_config: None,
                paused: None,
                project_agent_identity_id: None,
                project_agent_profile_id: None,
            },
        )
        .await
        .expect("create project");

    let fetched: ProjectResponse = server
        .client
        .get(&format!("/api/v1/projects/{}", created.id))
        .await
        .expect("get project");

    assert_eq!(fetched.id, created.id);
    assert_eq!(fetched.name, "Client Project");
}

#[tokio::test]
async fn forge_client_lists_projects_when_project_hooks_is_missing() {
    let Some(server) = TestServer::spawn().await else {
        return;
    };
    let created = create_project(&server.client, "Legacy List Project").await;

    let projects: PaginatedResponse<ProjectResponse> = server
        .client
        .get("/api/v1/projects")
        .await
        .expect("list projects");

    let project = projects
        .items
        .iter()
        .find(|project| project.id == created.id)
        .expect("created project should be listed");
    assert_eq!(project.name, "Legacy List Project");
    assert!(project.project_hooks.is_empty());
}

#[tokio::test]
async fn forge_client_creates_repo_and_lists_repos() {
    let Some(server) = TestServer::spawn().await else {
        return;
    };
    let project = create_project(&server.client, "Repo List Project").await;
    let local_path = server
        ._repo_dir
        .path()
        .to_str()
        .expect("repo path is UTF-8");

    let repo = create_repo(&server.client, &project.id, local_path).await;

    let repos: PaginatedResponse<RepoResponse> = server
        .client
        .get(&format!("/api/v1/projects/{}/repos", project.id))
        .await
        .expect("list repos");

    assert!(repos.items.iter().any(|item| item.id == repo.id));
}

#[tokio::test]
async fn forge_client_runs_task_flow_and_deletes_task() {
    let Some(server) = TestServer::spawn().await else {
        return;
    };
    let project = create_project(&server.client, "Task Flow Project").await;
    let local_path = server
        ._repo_dir
        .path()
        .to_str()
        .expect("repo path is UTF-8");
    let _repo = create_repo(&server.client, &project.id, local_path).await;
    let agent = create_agent(&server.client).await;

    let task: TaskResponse = server
        .client
        .post(
            &format!("/api/v1/projects/{}/tasks", project.id),
            &CreateTaskRequest {
                title: "Client task".to_owned(),
                description: None,
                parent_task_id: None,
                task_type: None,
                priority: None,
                review_config: None,
                merge_config: None,
                role_assignments: None,
                governance: None,
            },
        )
        .await
        .expect("create task");
    assert_eq!(task.status, "todo");

    let claimed: TaskResponse = server
        .client
        .post(
            &format!("/api/v1/tasks/{}/claim", task.id),
            &ClaimTaskRequest {
                agent_id: agent.id,
                overrides: None,
            },
        )
        .await
        .expect("claim task");
    assert_eq!(claimed.status, "in_progress");

    let cancelled: TaskResponse = server
        .client
        .post(
            &format!("/api/v1/tasks/{}/cancel", task.id),
            &serde_json::json!({}),
        )
        .await
        .expect("cancel task");
    assert_eq!(cancelled.status, "cancelled");

    server
        .client
        .delete(&format!("/api/v1/tasks/{}", task.id))
        .await
        .expect("delete task");
}

#[derive(Clone)]
struct TestState {
    inner: Arc<Mutex<TestData>>,
}

struct TestData {
    next_id: u64,
    projects: BTreeMap<String, ProjectResponse>,
    repos: BTreeMap<String, RepoResponse>,
    tasks: BTreeMap<String, TaskResponse>,
    daemon: DaemonResponse,
}

impl Default for TestData {
    fn default() -> Self {
        Self {
            next_id: 1,
            projects: BTreeMap::new(),
            repos: BTreeMap::new(),
            tasks: BTreeMap::new(),
            daemon: daemon_response(),
        }
    }
}

impl TestData {
    fn next_id(&mut self, prefix: &str) -> String {
        let id = format!("{prefix}-{}", self.next_id);
        self.next_id += 1;
        id
    }
}

struct TestServer {
    client: ForgeClient,
    handle: tokio::task::JoinHandle<()>,
    _repo_dir: TempDir,
}

impl TestServer {
    async fn spawn() -> Option<Self> {
        let state = TestState {
            inner: Arc::new(Mutex::new(TestData::default())),
        };
        let repo_dir = make_local_git_repo();

        let listener = match tokio::net::TcpListener::bind("0.0.0.0:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == ErrorKind::PermissionDenied => {
                eprintln!("skipping HTTP client integration test: TCP bind is denied");
                return None;
            }
            Err(error) => panic!("bind test server: {error}"),
        };
        let addr = listener.local_addr().expect("read test server addr");
        let router = test_router(state);
        let handle = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("serve test app");
        });

        Some(Self {
            client: ForgeClient::new(localhost_url(addr)),
            handle,
            _repo_dir: repo_dir,
        })
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

fn test_router(state: TestState) -> Router {
    Router::new()
        .route(
            "/api/v1/projects",
            get(list_projects_route).post(create_project_route),
        )
        .route("/api/v1/projects/{project_id}", get(get_project_route))
        .route(
            "/api/v1/projects/{project_id}/repos",
            get(list_repos_route).post(create_repo_route),
        )
        .route(
            "/api/v1/projects/{project_id}/tasks",
            post(create_task_route),
        )
        .route("/api/v1/agents", post(create_agent_route))
        .route("/api/v1/daemons", get(list_daemons_route))
        .route("/api/v1/tasks/{task_id}/claim", post(claim_task_route))
        .route("/api/v1/tasks/{task_id}/cancel", post(cancel_task_route))
        .route("/api/v1/tasks/{task_id}", delete(delete_task_route))
        .with_state(state)
}

async fn create_project_route(
    State(state): State<TestState>,
    Json(request): Json<CreateProjectRequest>,
) -> Json<ProjectResponse> {
    let mut data = state.inner.lock().expect("lock test state");
    let project = ProjectResponse {
        id: data.next_id("project"),
        name: request.name,
        settings: request.settings.unwrap_or_else(|| serde_json::json!({})),
        default_review_config: request.default_review_config,
        primary_repo_id: None,
        owner_id: None,
        created_at: now(),
        updated_at: now(),
        workflow_template_name: None,
        paused_at: None,
        paused: request.paused.unwrap_or(false),
        project_hooks: vec![],
        charter_status: "legacy_unverified".to_owned(),
        charter_setup_required: true,
        current_charter_id: None,
        current_charter_revision_id: None,
        current_charter_version: 0,
        primary_milestone_id: None,
        version: 1,
        execution_setup: None,
    };
    data.projects.insert(project.id.clone(), project.clone());
    Json(project)
}

async fn list_projects_route(State(state): State<TestState>) -> Json<serde_json::Value> {
    let items: Vec<_> = state
        .inner
        .lock()
        .expect("lock test state")
        .projects
        .values()
        .cloned()
        .map(|project| {
            let mut value = serde_json::to_value(project).expect("serialize project");
            value
                .as_object_mut()
                .expect("project serializes to object")
                .remove("project_hooks");
            value
        })
        .collect();

    Json(serde_json::to_value(paginated(items)).expect("serialize project list"))
}

async fn get_project_route(
    State(state): State<TestState>,
    AxumPath(project_id): AxumPath<String>,
) -> Result<Json<ProjectResponse>, StatusCode> {
    state
        .inner
        .lock()
        .expect("lock test state")
        .projects
        .get(&project_id)
        .cloned()
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn create_repo_route(
    State(state): State<TestState>,
    AxumPath(project_id): AxumPath<String>,
    Json(request): Json<CreateRepoRequest>,
) -> Json<RepoResponse> {
    let mut data = state.inner.lock().expect("lock test state");
    let repo = RepoResponse {
        id: data.next_id("repo"),
        project_id,
        name: request.name.unwrap_or_else(|| "forge".to_owned()),
        local_path: request.local_path,
        remote_url: request.remote_url,
        default_branch: request.default_branch.unwrap_or_else(|| "main".to_owned()),
        work_mode: request.work_mode.unwrap_or(WorkMode::DirectMerge),
        pr_provider: request.pr_provider,
        pr_provider_status: None,
        created_at: now(),
        updated_at: now(),
    };
    data.repos.insert(repo.id.clone(), repo.clone());
    Json(repo)
}

async fn list_repos_route(
    State(state): State<TestState>,
    AxumPath(project_id): AxumPath<String>,
) -> Json<PaginatedResponse<RepoResponse>> {
    let items: Vec<_> = state
        .inner
        .lock()
        .expect("lock test state")
        .repos
        .values()
        .filter(|repo| repo.project_id == project_id)
        .cloned()
        .collect();
    Json(paginated(items))
}

async fn create_task_route(
    State(state): State<TestState>,
    AxumPath(project_id): AxumPath<String>,
    Json(request): Json<CreateTaskRequest>,
) -> Json<TaskResponse> {
    let mut data = state.inner.lock().expect("lock test state");
    let task = task_response(
        data.next_id("task"),
        project_id,
        request.title,
        request.task_type.unwrap_or(TaskType::Task),
        "todo",
    );
    data.tasks.insert(task.id.clone(), task.clone());
    Json(task)
}

async fn claim_task_route(
    State(state): State<TestState>,
    AxumPath(task_id): AxumPath<String>,
    Json(_request): Json<ClaimTaskRequest>,
) -> Result<Json<TaskResponse>, StatusCode> {
    update_task_status(&state, &task_id, "in_progress")
}

async fn cancel_task_route(
    State(state): State<TestState>,
    AxumPath(task_id): AxumPath<String>,
    Json(_request): Json<serde_json::Value>,
) -> Result<Json<TaskResponse>, StatusCode> {
    update_task_status(&state, &task_id, "cancelled")
}

async fn delete_task_route(
    State(state): State<TestState>,
    AxumPath(task_id): AxumPath<String>,
) -> StatusCode {
    state
        .inner
        .lock()
        .expect("lock test state")
        .tasks
        .remove(&task_id);
    StatusCode::NO_CONTENT
}

async fn create_agent_route(
    State(state): State<TestState>,
    Json(request): Json<CreateAgentRequest>,
) -> Json<AgentResponse> {
    let mut data = state.inner.lock().expect("lock test state");
    Json(AgentResponse {
        id: data.next_id("agent"),
        name: request.name,
        description: request.description,
        profile_id: data.next_id("profile"),
        backend_kind: "cli".to_owned(),
        executor_type: request.executor_type,
        provider: None,
        model: request.model,
        reasoning_effort: request.reasoning_effort,
        permission_policy: request.permission_policy,
        prompt_template: request.prompt_template,
        capabilities: request.capabilities.unwrap_or_default(),
        config_json: request.config_json.unwrap_or_else(|| serde_json::json!({})),
        credential_handle_id: None,
        daemon_id: request.daemon_id,
        max_concurrent_tasks: request.max_concurrent_tasks.unwrap_or(1),
        status: AgentStatus::Idle,
        active_task_count: Some(0),
        effective_status: Some("idle".to_owned()),
        total_runs: 0,
        avg_duration_ms: None,
        success_rate: None,
        total_input_tokens: 0,
        total_output_tokens: 0,
        total_cache_read_tokens: 0,
        total_cache_write_tokens: 0,
        total_tokens: 0,
        total_cost_usd: None,
        is_default: request.is_default.unwrap_or(false),
        paused: false,
        owner_id: None,
        visibility: "global".to_owned(),
        version: 1,
        created_at: now(),
        updated_at: now(),
    })
}

async fn list_daemons_route(
    State(state): State<TestState>,
) -> Json<PaginatedResponse<DaemonResponse>> {
    let daemon = state.inner.lock().expect("lock test state").daemon.clone();
    Json(paginated(vec![daemon]))
}

fn update_task_status(
    state: &TestState,
    task_id: &str,
    status: &str,
) -> Result<Json<TaskResponse>, StatusCode> {
    let mut data = state.inner.lock().expect("lock test state");
    let task = data.tasks.get_mut(task_id).ok_or(StatusCode::NOT_FOUND)?;
    task.status = status.to_owned();
    task.updated_at = now();
    task.version += 1;
    Ok(Json(task.clone()))
}

async fn create_project(client: &ForgeClient, name: &str) -> ProjectResponse {
    client
        .post(
            "/api/v1/projects",
            &CreateProjectRequest {
                name: name.to_owned(),
                settings: None,
                default_review_config: None,
                paused: None,
                project_agent_identity_id: None,
                project_agent_profile_id: None,
            },
        )
        .await
        .expect("create project")
}

async fn create_repo(client: &ForgeClient, project_id: &str, local_path: &str) -> RepoResponse {
    client
        .post(
            &format!("/api/v1/projects/{project_id}/repos"),
            &CreateRepoRequest {
                remote_url: local_path.to_owned(),
                local_path: Some(local_path.to_owned()),
                name: Some("forge".to_owned()),
                default_branch: None,
                work_mode: None,
                pr_provider: None,
                pr_provider_config: None,
            },
        )
        .await
        .expect("create repo")
}

async fn create_agent(client: &ForgeClient) -> AgentResponse {
    let daemons: PaginatedResponse<DaemonResponse> = client
        .get("/api/v1/daemons?limit=1")
        .await
        .expect("list daemons");
    let daemon_id = daemons
        .items
        .first()
        .expect("seeded daemon exists")
        .id
        .clone();

    client
        .post(
            "/api/v1/agents",
            &CreateAgentRequest {
                name: "client-agent".to_owned(),
                description: None,
                executor_type: "shell".to_owned(),
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities: None,
                config_json: None,
                daemon_id: Some(daemon_id),
                max_concurrent_tasks: None,
                heartbeat_interval_seconds: None,
                max_missed_heartbeats: None,
                is_default: None,
                credential_id: None,
            },
        )
        .await
        .expect("create agent")
}

fn task_response(
    id: String,
    project_id: String,
    title: String,
    task_type: TaskType,
    status: &str,
) -> TaskResponse {
    TaskResponse {
        id,
        project_id,
        repo_id: None,
        parent_task_id: None,
        assignee_type: None,
        assignee_id: None,
        title,
        description: None,
        task_type,
        status: status.to_owned(),
        canonical_phase: canonical_phase_for_status(status),
        awaiting_human: false,
        priority: 0,
        board_position: 0.0,
        subtask_order: None,
        role_assignments: Vec::new(),
        remaining_retries: HashMap::new(),
        execution_actions: Vec::new(),
        error_annotation: None,
        blocked: None,
        failed: None,
        workflow_health: None,
        workflow_exception: None,
        execution_observability: TaskExecutionObservability {
            execution_count: 0,
            active_execution_id: None,
            active_role: None,
            active_started_at: None,
            active_elapsed_seconds: None,
            latest_execution_id: None,
            latest_execution_status: None,
            latest_role: None,
            latest_started_at: None,
            latest_stopped_at: None,
            latest_runtime_seconds: None,
            total_runtime_seconds: 0.0,
            total_input_tokens: 0,
            total_output_tokens: 0,
            total_cache_read_tokens: 0,
            total_cache_write_tokens: 0,
            total_tokens: 0,
            total_cost_usd: None,
        },
        task_state_config: None,
        review_passed_at: None,
        archived_at: None,
        workspace: None,
        plan_progress: None,
        plan_artifact: None,
        external_issue_number: None,
        external_issue_url: None,
        execution_evidence: ExecutionEvidenceSummary::from_counts(0, 0, false, None),
        execution_blocker: None,
        version: 1,
        created_at: now(),
        updated_at: now(),
    }
}

fn canonical_phase_for_status(status: &str) -> CanonicalPhase {
    match status {
        "backlog" => CanonicalPhase::Backlog,
        "todo" => CanonicalPhase::Ready,
        "review" | "merging" | "merge_failed" => CanonicalPhase::Review,
        "done" | "cancelled" => CanonicalPhase::Done,
        _ => CanonicalPhase::Working,
    }
}

fn daemon_response() -> DaemonResponse {
    DaemonResponse {
        id: "daemon-1".to_owned(),
        machine_id: "machine-daemon-1".to_owned(),
        hostname: "test-host".to_owned(),
        os: "linux".to_owned(),
        arch: "x86_64".to_owned(),
        agent_version: None,
        status: "online".to_owned(),
        last_report_at: Some(now()),
        detected_clis: serde_json::json!([
            { "kind": "shell", "availability": "authenticated", "path": "/bin/sh" }
        ]),
        labels: serde_json::json!({}),
        owner_id: None,
        visibility: "global".to_owned(),
        version: 1,
        created_at: now(),
        updated_at: now(),
    }
}

fn paginated<T>(items: Vec<T>) -> PaginatedResponse<T> {
    let total_count = items.len() as u64;
    PaginatedResponse {
        items,
        next_cursor: None,
        has_more: false,
        total_count: Some(total_count),
    }
}

fn now() -> String {
    "2026-05-14T00:00:00Z".to_owned()
}

fn localhost_url(addr: SocketAddr) -> String {
    format!("http://127.0.0.1:{}", addr.port())
}

fn make_local_git_repo() -> TempDir {
    let repo_dir = TempDir::new("forge-client-repo");
    run_git(repo_dir.path(), &["init", "-b", "main"]);
    run_git(repo_dir.path(), &["config", "user.email", "test@forge.dev"]);
    run_git(repo_dir.path(), &["config", "user.name", "Forge Test"]);
    std::fs::write(repo_dir.path().join("README.md"), "# Forge Client\n").expect("write README");
    run_git(repo_dir.path(), &["add", "-A"]);
    run_git(repo_dir.path(), &["commit", "-m", "initial commit"]);
    repo_dir
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

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(prefix: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "{prefix}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).expect("temp dir creates");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
