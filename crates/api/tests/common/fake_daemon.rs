#![allow(dead_code)]

use std::{sync::Arc, time::Duration};

use api::{serve_with_listener, AppState};
use api_types::{
    DaemonFrame, DaemonRegisterResponse, DaemonResponse, ExecutionLogNotification,
    ExecutionStartResult, ExecutionTerminalNotification, FsEntry, FsListResult,
    TerminalClientFrame, TerminalServerFrame, METHOD_EXECUTION_LOG, METHOD_EXECUTION_START,
    METHOD_EXECUTION_TERMINAL, METHOD_FS_LIST,
};
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use chrono::{Duration as ChronoDuration, Utc};
use db::{
    AgentRepo, AgentStatus, AssigneeKind, ClaimExecutionLease, CreateAgent, CreateExecution,
    CreateProject, CreateRepo, CreateTask, CreateTaskRoleAssignment, CreateWorkspace,
    CreateWorkspaceLease, DaemonRepo, DaemonStatus, Execution, ExecutionLeaseMutation,
    ExecutionRepo, ExecutionStatus, ProjectRepo, RepoRepo, TaskRepo, TaskRoleAssignmentRepo,
    UpdateDaemonReport, UpsertDaemon, UserRepo, WorkMode, WorkspaceLeaseRepo, WorkspaceRepo,
    WorkspaceStatus,
};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        client::IntoClientRequest, http::HeaderValue, Error as WsError, Message as WsMessage,
    },
    MaybeTlsStream, WebSocketStream,
};
use tower::ServiceExt;

use super::{json_request, json_request_with_bearer, TestDir};

pub const FAKE_DAEMON_USER_ID: &str = "test-user-id";

pub type ClientSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

pub struct TestServer {
    pub addr: std::net::SocketAddr,
    pub state: Arc<AppState>,
    handle: tokio::task::JoinHandle<()>,
    _web_dist_dir: TestDir,
}

impl TestServer {
    pub async fn start(state: Arc<AppState>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let addr = listener.local_addr().expect("listener addr");
        let web_dist_dir = TestDir::new("forge-api-fake-daemon-web");
        std::fs::write(web_dist_dir.path().join("index.html"), "<html></html>")
            .expect("write index");
        let web_dist_path = web_dist_dir.path().to_path_buf();
        let server_state = (*state).clone();
        let shutdown_signal = state.shutdown_signal.clone();

        let handle = tokio::spawn(async move {
            serve_with_listener(listener, server_state, web_dist_path, async move {
                shutdown_signal.wait().await;
            })
            .await
            .expect("test API server serves");
        });

        Self {
            addr,
            state,
            handle,
            _web_dist_dir: web_dist_dir,
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.state.shutdown_signal.request();
        self.handle.abort();
    }
}

pub async fn register_daemon(
    app: &Router,
    machine_id: &str,
    suite: &str,
) -> DaemonRegisterResponse {
    json_request(
        app,
        Method::POST,
        "/api/v1/daemons/register",
        json!({
            "machine_id": machine_id,
            "hostname": "remote-test-host",
            "os": "linux",
            "arch": "x86_64",
            "agent_version": "fake-daemon-test",
            "labels": { "suite": suite }
        }),
        StatusCode::OK,
    )
    .await
}

pub async fn report_remote_daemon_shell(
    app: &Router,
    daemon_id: &str,
    registration_token: &str,
    workspace_root: &std::path::Path,
    suite: &str,
) {
    let _: DaemonResponse = json_request_with_bearer(
        app,
        Method::POST,
        &format!("/api/v1/daemons/{daemon_id}/report"),
        registration_token,
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
            }],
            "labels": { "suite": suite }
        }),
        StatusCode::OK,
    )
    .await;
}

pub async fn connect_daemon(
    server: &TestServer,
    daemon_id: &str,
    token: Option<&str>,
) -> Result<ClientSocket, WsError> {
    let url = format!("ws://{}/api/v1/daemons/{daemon_id}/connect", server.addr);
    let request = if let Some(token) = token {
        let mut request = url.into_client_request()?;
        request.headers_mut().insert(
            header::AUTHORIZATION.as_str(),
            HeaderValue::from_str(&format!("Bearer {token}")).expect("authorization header"),
        );
        request
    } else {
        url.into_client_request()?
    };

    connect_async(request).await.map(|(socket, _)| socket)
}

pub async fn connect_terminal(
    server: &TestServer,
    session_id: &str,
    attach_token: &str,
) -> Result<ClientSocket, WsError> {
    let url = format!(
        "ws://{}/api/v1/terminals/{session_id}/ws?attach_token={attach_token}",
        server.addr
    );
    connect_async(url).await.map(|(socket, _)| socket)
}

pub async fn next_daemon_request(
    socket: &mut ClientSocket,
    expected_method: &str,
) -> (String, Value) {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .expect("daemon request arrives")
            .expect("daemon websocket remains open")
            .expect("daemon request is valid");
        let WsMessage::Text(text) = message else {
            continue;
        };
        let frame: DaemonFrame = serde_json::from_str(text.as_ref()).expect("daemon frame parses");
        match frame {
            DaemonFrame::Request { id, method, params } => {
                assert_eq!(method, expected_method);
                return (id, params);
            }
            DaemonFrame::Heartbeat { .. } => {}
            other => panic!("expected daemon request frame, got {other:?}"),
        }
    }
}

pub async fn send_daemon_response<T: Serialize>(socket: &mut ClientSocket, id: String, result: T) {
    let frame = DaemonFrame::Response {
        id,
        result: serde_json::to_value(result).expect("daemon result serializes"),
    };
    socket
        .send(WsMessage::Text(
            serde_json::to_string(&frame)
                .expect("daemon response serializes")
                .into(),
        ))
        .await
        .expect("send daemon response");
}

pub async fn send_daemon_notification<T: Serialize>(
    socket: &mut ClientSocket,
    method: &str,
    params: T,
) {
    let frame = DaemonFrame::Notification {
        method: method.to_owned(),
        params: serde_json::to_value(params).expect("daemon notification serializes"),
    };
    socket
        .send(WsMessage::Text(
            serde_json::to_string(&frame)
                .expect("daemon notification frame serializes")
                .into(),
        ))
        .await
        .expect("send daemon notification");
}

pub async fn send_terminal_client_frame(socket: &mut ClientSocket, frame: TerminalClientFrame) {
    socket
        .send(WsMessage::Text(
            serde_json::to_string(&frame)
                .expect("terminal client frame serializes")
                .into(),
        ))
        .await
        .expect("send terminal client frame");
}

pub async fn next_terminal_server_frame(socket: &mut ClientSocket) -> TerminalServerFrame {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .expect("terminal server frame arrives")
            .expect("terminal websocket remains open")
            .expect("terminal frame is valid");
        let WsMessage::Text(text) = message else {
            continue;
        };
        return serde_json::from_str(text.as_ref()).expect("terminal server frame parses");
    }
}

pub async fn wait_until_connected(state: &AppState, daemon_id: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if state.daemon_connections.is_connected(daemon_id) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "daemon connection was not registered"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub async fn wait_until_disconnected(state: &AppState, daemon_id: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if !state.daemon_connections.is_connected(daemon_id) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "daemon connection remained registered"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub async fn assert_heartbeat_echo(socket: &mut ClientSocket, seq: u64) {
    let heartbeat = DaemonFrame::Heartbeat { seq };
    socket
        .send(WsMessage::Text(
            serde_json::to_string(&heartbeat)
                .expect("heartbeat serializes")
                .into(),
        ))
        .await
        .expect("send heartbeat");
    let message = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("heartbeat response arrives")
        .expect("websocket remains open")
        .expect("heartbeat response is valid");
    let WsMessage::Text(text) = message else {
        panic!("expected heartbeat text frame, got {message:?}");
    };
    let frame: DaemonFrame = serde_json::from_str(&text).expect("heartbeat frame parses");
    assert!(matches!(frame, DaemonFrame::Heartbeat { seq: received } if received == seq));
}

pub async fn respond_to_fs_list(
    registry: Arc<services::daemon_transport::DaemonConnectionRegistry>,
    daemon_id: String,
    mut outbound: tokio::sync::mpsc::Receiver<DaemonFrame>,
    expected_path: String,
) {
    let frame = outbound.recv().await.expect("server sends request frame");
    let DaemonFrame::Request { id, method, params } = frame else {
        panic!("expected daemon request frame");
    };

    assert_eq!(method, METHOD_FS_LIST);
    assert_eq!(params["path"], expected_path);

    let result = serde_json::to_value(FsListResult {
        path: expected_path.clone(),
        entries: vec![FsEntry {
            name: "remote.txt".to_owned(),
            path: format!("{expected_path}/remote.txt"),
            is_dir: false,
            is_git_repo: false,
        }],
    })
    .expect("serialize fs list result");
    registry.dispatch_incoming(&daemon_id, DaemonFrame::Response { id, result });
}

pub async fn accept_execution_start(
    socket: &mut ClientSocket,
    execution_id: &str,
) -> api_types::ExecutionStartParams {
    let (start_id, params) = next_daemon_request(socket, METHOD_EXECUTION_START).await;
    let start_params: api_types::ExecutionStartParams =
        serde_json::from_value(params).expect("execution.start params parse");
    assert_eq!(start_params.execution_id, execution_id);
    send_daemon_response(
        socket,
        start_id,
        ExecutionStartResult {
            execution_id: execution_id.to_owned(),
            accepted: true,
        },
    )
    .await;
    start_params
}

pub async fn send_execution_log(
    socket: &mut ClientSocket,
    execution_id: &str,
    seq: u64,
    line: &str,
) {
    send_daemon_notification(
        socket,
        METHOD_EXECUTION_LOG,
        ExecutionLogNotification {
            execution_id: execution_id.to_owned(),
            seq,
            stream: "stdout".to_owned(),
            line: line.to_owned(),
            ts: db::now_rfc3339(),
            kind: Some("stdout".to_owned()),
            log_stream: Some("main".to_owned()),
            payload: Some(json!({ "line": line })),
            truncated: Some(false),
        },
    )
    .await;
}

pub async fn send_execution_terminal_completed(
    socket: &mut ClientSocket,
    execution_id: &str,
    summary: Option<&str>,
) {
    send_daemon_notification(
        socket,
        METHOD_EXECUTION_TERMINAL,
        ExecutionTerminalNotification {
            execution_id: execution_id.to_owned(),
            exit_code: Some(0),
            signal: None,
            error: None,
            ts: db::now_rfc3339(),
            status: Some("completed".to_owned()),
            agent_session_id: None,
            summary: summary.map(str::to_owned),
            after_sha: None,
            usage: None,
            failure_class: None,
            retry_at: None,
            resolved_candidate: None,
            route_attempts: None,
        },
    )
    .await;
}

pub async fn send_execution_terminal_failed(
    socket: &mut ClientSocket,
    execution_id: &str,
    error: &str,
) {
    send_daemon_notification(
        socket,
        METHOD_EXECUTION_TERMINAL,
        ExecutionTerminalNotification {
            execution_id: execution_id.to_owned(),
            exit_code: Some(1),
            signal: None,
            error: Some(error.to_owned()),
            ts: db::now_rfc3339(),
            status: Some("failed".to_owned()),
            agent_session_id: None,
            summary: None,
            after_sha: None,
            usage: None,
            failure_class: None,
            retry_at: None,
            resolved_candidate: None,
            route_attempts: None,
        },
    )
    .await;
}

pub async fn seed_running_execution_for_daemon(state: &AppState, daemon_id: &str) -> Execution {
    let now = db::now_rfc3339();
    let project_id = uuid::Uuid::new_v4().to_string();
    let task_id = uuid::Uuid::new_v4().to_string();
    let agent_id = uuid::Uuid::new_v4().to_string();

    ProjectRepo::create(
        &*state.db,
        CreateProject {
            id: project_id.clone(),
            name: "Execution Ownership".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(FAKE_DAEMON_USER_ID.to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");
    TaskRepo::create(
        &*state.db,
        CreateTask {
            id: task_id.clone(),
            project_id: project_id.clone(),
            repo_id: None,
            parent_task_id: None,
            assignee_type: Some("agent".to_owned()),
            assignee_id: Some(agent_id.clone()),
            title: "Owned execution".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "in_progress".to_owned(),
            is_automation: false,
            priority: 0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task creates");
    AgentRepo::create(
        &*state.db,
        CreateAgent {
            id: agent_id.clone(),
            name: "Owner Agent".to_owned(),
            description: None,
            executor_type: "codex".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: Some(daemon_id.to_owned()),
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Busy,
            last_heartbeat_at: Some(now.clone()),
            is_default: false,
            paused: false,
            owner_id: Some(FAKE_DAEMON_USER_ID.to_owned()),
            visibility: "account".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("agent creates");
    let execution = ExecutionRepo::create(
        &*state.db,
        CreateExecution {
            id: uuid::Uuid::new_v4().to_string(),
            task_id,
            agent_id: Some(agent_id),
            role: "coder".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: Some(now.clone()),
            summary: None,
            logs_path: None,
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

    // Remote execution notifications are authorized by the authenticated
    // socket incarnation's lease owner.  Keep this fixture explicit about
    // that scheduler-owned fact so it exercises the owner boundary rather
    // than relying on the removed mutable Agent daemon binding fallback.
    let connection = state
        .daemon_connections
        .get(daemon_id)
        .expect("daemon connection is registered before seeding execution");
    let now = Utc::now();
    let mutation = ExecutionRepo::claim_lease(
        &*state.db,
        ClaimExecutionLease {
            execution_id: execution.id.clone(),
            expected_version: execution.execution_version,
            owner: services::execution_lease_owner(daemon_id, connection.id()),
            lease_expires_at: (now + ChronoDuration::seconds(30)).to_rfc3339(),
            hard_deadline_at: (now + ChronoDuration::minutes(5)).to_rfc3339(),
            now: now.to_rfc3339(),
        },
    )
    .await
    .expect("remote execution lease claims");
    let ExecutionLeaseMutation::Updated(execution) = mutation else {
        panic!("remote execution lease claim unexpectedly lost");
    };
    execution
}

pub struct StartableExecutionFixture {
    pub task_id: String,
    pub workspace_path: String,
    pub execution: Execution,
}

pub async fn seed_startable_execution_for_daemon(
    state: &AppState,
    daemon_id: &str,
    prompt: &str,
) -> StartableExecutionFixture {
    let now = db::now_rfc3339();
    let project_id = uuid::Uuid::new_v4().to_string();
    let repo_id = uuid::Uuid::new_v4().to_string();
    let task_id = uuid::Uuid::new_v4().to_string();
    let agent_id = uuid::Uuid::new_v4().to_string();
    let workspace_id = uuid::Uuid::new_v4().to_string();
    let workspace_path = state
        .cleanup_scheduler
        .workspace_root()
        .join("execution-start")
        .join(&task_id);
    std::fs::create_dir_all(&workspace_path).expect("workspace path creates");

    // A startable remote execution needs the same daemon onboarding fact as
    // production setup: the pinned daemon has reported an authenticated
    // shell adapter.  Registration alone intentionally does not imply this.
    let report_at = db::now_rfc3339();
    DaemonRepo::update_report(
        &*state.db,
        UpdateDaemonReport {
            id: daemon_id.to_owned(),
            last_report_at: report_at.clone(),
            status: DaemonStatus::Online,
            detected_clis_json:
                r#"[{"kind":"shell","availability":"authenticated","path":"/bin/sh"}]"#.to_owned(),
            labels_json: None,
            updated_at: report_at,
        },
    )
    .await
    .expect("remote daemon shell report updates");

    ProjectRepo::create(
        &*state.db,
        CreateProject {
            id: project_id.clone(),
            name: "Execution Start".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(FAKE_DAEMON_USER_ID.to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");
    RepoRepo::create(
        &*state.db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "execution-repo".to_owned(),
            remote_url: "file:///tmp/execution-repo".to_owned(),
            local_path: None,
            work_mode: WorkMode::DirectMerge,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    let task = TaskRepo::create(
        &*state.db,
        CreateTask {
            id: task_id.clone(),
            project_id: project_id.clone(),
            repo_id: Some(repo_id.clone()),
            parent_task_id: None,
            assignee_type: Some("agent".to_owned()),
            assignee_id: Some(agent_id.clone()),
            title: "Start remote execution".to_owned(),
            description: Some(prompt.to_owned()),
            task_type: "task".to_owned(),
            status: "in_progress".to_owned(),
            is_automation: false,
            priority: 0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task creates");
    AgentRepo::create(
        &*state.db,
        CreateAgent {
            id: agent_id.clone(),
            name: "Remote Shell".to_owned(),
            description: None,
            executor_type: "shell".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: Some(daemon_id.to_owned()),
            // The fixture already has the running execution visible when
            // lease admission re-checks identity capacity.
            max_concurrent_tasks: 2,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Busy,
            last_heartbeat_at: Some(now.clone()),
            is_default: false,
            paused: false,
            owner_id: Some(FAKE_DAEMON_USER_ID.to_owned()),
            visibility: "account".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("agent creates");
    WorkspaceRepo::create(
        &*state.db,
        CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task_id.clone(),
            repo_id: repo_id.clone(),
            worktree_path: workspace_path.to_string_lossy().into_owned(),
            branch: "forge/task/start".to_owned(),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("workspace creates");
    let execution = ExecutionRepo::create(
        &*state.db,
        CreateExecution {
            id: uuid::Uuid::new_v4().to_string(),
            task_id: task_id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "coder".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: Some(now.clone()),
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                serde_json::json!({ "executor_type": "shell", "config": {} }).to_string(),
            ),
            workspace_id: Some(workspace_id),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("execution creates");
    let capability_profile_revision = "forge.capability-profile/v1";
    let capability_class = "repository_write";
    let mut capability_digest = Sha256::new();
    capability_digest.update(capability_profile_revision.as_bytes());
    capability_digest.update([0]);
    capability_digest.update(capability_class.as_bytes());
    WorkspaceLeaseRepo::issue(
        &*state.db,
        CreateWorkspaceLease {
            id: uuid::Uuid::new_v4().to_string(),
            project_id,
            task_id: task_id.clone(),
            task_version: task.version,
            execution_id: execution.id.clone(),
            operation_idempotency_key: execution.id.clone(),
            repository_binding_id: repo_id,
            base_ref: "main".to_owned(),
            role: "worker".to_owned(),
            capabilities_json: serde_json::to_string(&[capability_class])
                .expect("lease capabilities serialize"),
            assigned_principal_type: "agent".to_owned(),
            assigned_principal_id: agent_id,
            capability_profile_revision: capability_profile_revision.to_owned(),
            capability_profile_digest: format!(
                "sha256:{}",
                hex::encode(capability_digest.finalize())
            ),
            issuing_principal_type: "system".to_owned(),
            issuing_principal_id: "task-service-scheduler".to_owned(),
            issued_at: now.clone(),
            expires_at: "2099-01-01T00:00:00+00:00".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("workspace lease creates");

    StartableExecutionFixture {
        task_id,
        workspace_path: workspace_path.to_string_lossy().into_owned(),
        execution,
    }
}

pub struct TerminalTaskFixture {
    pub task_id: String,
    pub workspace_path: String,
}

pub async fn seed_terminal_task_for_daemon(
    state: &AppState,
    daemon_id: &str,
) -> TerminalTaskFixture {
    let now = db::now_rfc3339();
    let project_id = uuid::Uuid::new_v4().to_string();
    let repo_id = uuid::Uuid::new_v4().to_string();
    let task_id = uuid::Uuid::new_v4().to_string();
    let agent_id = uuid::Uuid::new_v4().to_string();
    let workspace_id = uuid::Uuid::new_v4().to_string();

    ProjectRepo::create(
        &*state.db,
        CreateProject {
            id: project_id.clone(),
            name: "Terminal Ownership".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");
    RepoRepo::create(
        &*state.db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "terminal-repo".to_owned(),
            remote_url: "file:///tmp/terminal-repo".to_owned(),
            local_path: None,
            work_mode: WorkMode::DirectMerge,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    TaskRepo::create(
        &*state.db,
        CreateTask {
            id: task_id.clone(),
            project_id,
            repo_id: Some(repo_id.clone()),
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "Daemon terminal".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "in_progress".to_owned(),
            is_automation: false,
            priority: 0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task creates");
    AgentRepo::create(
        &*state.db,
        CreateAgent {
            id: agent_id.clone(),
            name: "Terminal Owner Agent".to_owned(),
            description: None,
            executor_type: "codex".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: Some(daemon_id.to_owned()),
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: Some(now.clone()),
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "account".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("agent creates");
    TaskRoleAssignmentRepo::assign(
        &*state.db,
        CreateTaskRoleAssignment {
            id: uuid::Uuid::new_v4().to_string(),
            task_id: task_id.clone(),
            role_name: services::workflow::default_roles::CODER.to_owned(),
            assignee_type: Some(AssigneeKind::Agent),
            assignee_id: Some(agent_id),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("role assignment creates");

    let workspace_root = state.cleanup_scheduler.workspace_root().to_path_buf();
    std::fs::create_dir_all(&workspace_root).expect("workspace root exists");
    let worktree_path = workspace_root
        .join("terminal-daemon-routing")
        .join(&task_id)
        .join("repo");
    std::fs::create_dir_all(&worktree_path).expect("worktree path exists");
    WorkspaceRepo::create(
        &*state.db,
        CreateWorkspace {
            id: workspace_id,
            task_id: task_id.clone(),
            repo_id,
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch: workspace::task_branch_name(&task_id),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("workspace creates");

    TerminalTaskFixture {
        task_id,
        workspace_path: worktree_path.to_string_lossy().into_owned(),
    }
}

pub async fn assert_execution_status_remains(
    state: &AppState,
    execution_id: &str,
    expected_status: ExecutionStatus,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(250);
    loop {
        let execution = ExecutionRepo::get_by_id(&*state.db, execution_id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        assert_eq!(
            execution.status, expected_status,
            "forged daemon terminal notification changed execution status"
        );
        if tokio::time::Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub async fn wait_for_execution_status(
    state: &AppState,
    execution_id: &str,
    expected_status: ExecutionStatus,
) -> Execution {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let execution = ExecutionRepo::get_by_id(&*state.db, execution_id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        if execution.status == expected_status {
            return execution;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "execution did not reach {expected_status}, current status was {}",
            execution.status
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub async fn poll_until_execution_status(
    state: &AppState,
    execution_id: &str,
    expected_status: ExecutionStatus,
) -> Execution {
    for _ in 0..100 {
        if let Some(execution) = ExecutionRepo::get_by_id(&*state.db, execution_id)
            .await
            .expect("execution lookup")
        {
            if execution.status == expected_status {
                return execution;
            }
            if expected_status == ExecutionStatus::Running
                && execution.status != ExecutionStatus::Running
            {
                panic!(
                    "execution left running early with status {:?}",
                    execution.status
                );
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("execution {execution_id} did not reach {expected_status:?}");
}

pub async fn seed_embedded_daemon(state: &AppState) -> String {
    let now = db::now_rfc3339();
    let daemon = DaemonRepo::upsert_by_machine_id(
        &*state.db,
        UpsertDaemon {
            id: uuid::Uuid::new_v4().to_string(),
            machine_id: services::embedded_daemon::embedded_machine_id(),
            hostname: "embedded-test-host".to_owned(),
            os: std::env::consts::OS.to_owned(),
            arch: std::env::consts::ARCH.to_owned(),
            agent_version: None,
            labels_json: r#"{"mode":"embedded"}"#.to_owned(),
            status: DaemonStatus::Online,
            registration_token_hash: None,
            owner_id: Some(FAKE_DAEMON_USER_ID.to_owned()),
            visibility: "account".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("seed embedded daemon");
    daemon.id
}

pub async fn seed_test_user(db: &db::SqliteDb) {
    let now = db::now_rfc3339();
    UserRepo::create_user(
        db,
        &db::User {
            id: FAKE_DAEMON_USER_ID.to_owned(),
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
}

pub async fn fetch_execution_logs(app: &Router, execution_id: &str) -> Value {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/api/v1/executions/{execution_id}/logs?tail=50"))
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", super::test_jwt()),
                )
                .body(Body::empty())
                .expect("build logs request"),
        )
        .await
        .expect("router response");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response body");
    assert_eq!(
        status,
        StatusCode::OK,
        "unexpected logs response: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).expect("parse logs response")
}
