#![allow(dead_code)]

mod common;

use std::{sync::Arc, time::Duration};

use api_types::{AgentResponse, TaskResponse, METHOD_EXECUTION_START};
use axum::http::{Method, StatusCode};
use db::{ExecutionRepo, ExecutionStatus as DbExecutionStatus, StopReason, TaskRepo};
use futures_util::SinkExt;
use serde_json::{json, Value};
use services::HeartbeatMonitor;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use common::{
    admin_jwt, create_project_and_repo,
    fake_daemon::{
        connect_daemon, fetch_execution_logs, next_daemon_request, poll_until_execution_status,
        register_daemon, report_remote_daemon_shell, send_daemon_response, send_execution_log,
        send_execution_terminal_completed, send_execution_terminal_failed,
        wait_for_execution_status, wait_until_connected, wait_until_disconnected, TestServer,
    },
    json_request, json_request_with_bearer, setup_git_repo, test_app, TestDir,
};

async fn poll_task_status_after_execution(
    db: &Arc<db::SqliteDb>,
    task_id: &str,
    expected: &str,
) -> db::Task {
    for _ in 0..200 {
        if let Some(task) = TaskRepo::get_by_id(&**db, task_id, false)
            .await
            .expect("task lookup")
        {
            if task.status == expected {
                return task;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let task = TaskRepo::get_by_id(&**db, task_id, false)
        .await
        .expect("task lookup")
        .expect("task exists");
    panic!(
        "task {task_id} did not reach {expected}; status={} blocked={:?} error_annotation={:?}",
        task.status, task.blocked_json, task.error_annotation
    );
}

async fn poll_failed_task_recovery_state(db: &Arc<db::SqliteDb>, task_id: &str) -> db::Task {
    for _ in 0..200 {
        if let Some(task) = TaskRepo::get_by_id(&**db, task_id, false)
            .await
            .expect("task lookup")
        {
            if task.status == "in_progress"
                && task.blocked_json.is_some()
                && task.error_annotation.is_some()
            {
                return task;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    TaskRepo::get_by_id(&**db, task_id, false)
        .await
        .expect("task lookup")
        .expect("task exists")
}

async fn poll_execution_logs(
    app: &axum::Router,
    execution_id: &str,
    minimum_entries: usize,
) -> Value {
    for _ in 0..200 {
        let logs = fetch_execution_logs(app, execution_id).await;
        if logs
            .get("items")
            .and_then(Value::as_array)
            .is_some_and(|entries| entries.len() >= minimum_entries)
        {
            return logs;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    fetch_execution_logs(app, execution_id).await
}

struct RemoteRoundtripFixture {
    harness: common::Harness,
    registration: api_types::DaemonRegisterResponse,
    server: TestServer,
    daemon_socket: common::fake_daemon::ClientSocket,
    project_id: String,
    repo_id: String,
    agent_id: String,
    reviewer_id: String,
    _repo_dir: TestDir,
    _workspaces_root: TestDir,
}

async fn setup_remote_roundtrip(prefix: &str) -> RemoteRoundtripFixture {
    let repo_dir = TestDir::new(&format!("{prefix}-repo"));
    let repo_path = setup_git_repo(repo_dir.path());
    let workspaces_root = TestDir::new(&format!("{prefix}-workspaces"));
    let harness = test_app(workspaces_root.path(), prefix).await;

    let registration = register_daemon(&harness.app, &format!("{prefix}-machine"), prefix).await;
    let server = TestServer::start(Arc::clone(&harness.state)).await;
    let daemon_socket = connect_daemon(
        &server,
        &registration.daemon_id,
        Some(&registration.registration_token),
    )
    .await
    .expect("daemon websocket upgrade succeeds");
    wait_until_connected(&harness.state, &registration.daemon_id).await;
    report_remote_daemon_shell(
        &harness.app,
        &registration.daemon_id,
        &registration.registration_token,
        workspaces_root.path(),
        prefix,
    )
    .await;

    let (project_id, repo_id) =
        create_project_and_repo(&harness.app, &format!("{prefix} Project"), &repo_path).await;

    let agent: AgentResponse = json_request_with_bearer(
        &harness.app,
        Method::POST,
        "/api/v1/agents",
        &admin_jwt(),
        json!({
            "name": format!("{prefix}-shell-agent"),
            "executor_type": "shell",
            "daemon_id": registration.daemon_id,
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(agent.effective_status.as_deref(), Some("active"));

    // Remote execution now enforces the same Project execution setup gate as
    // the production scheduler. Keep the Worker and independent Reviewer
    // explicit, active, and owned by the daemon used by this round-trip so
    // the test exercises the transport/liveness path after real admission.
    let reviewer: AgentResponse = json_request_with_bearer(
        &harness.app,
        Method::POST,
        "/api/v1/agents",
        &admin_jwt(),
        json!({
            "name": format!("{prefix}-shell-reviewer"),
            "executor_type": "shell",
            "daemon_id": registration.daemon_id,
            "config_json": {
                "command": "printf",
                "args": ["Looks good.\\n===REVIEW: PASS===\\n"]
            },
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(reviewer.effective_status.as_deref(), Some("active"));
    assert_ne!(agent.id, reviewer.id, "reviewer must be independent");
    assert_eq!(
        reviewer.daemon_id.as_deref(),
        Some(registration.daemon_id.as_str())
    );
    common::configure_execution_test_setup(
        &harness.state.db,
        &project_id,
        &repo_id,
        &agent.id,
        &reviewer.id,
    )
    .await;

    RemoteRoundtripFixture {
        harness,
        registration,
        server,
        daemon_socket,
        project_id,
        repo_id,
        agent_id: agent.id,
        reviewer_id: reviewer.id,
        _repo_dir: repo_dir,
        _workspaces_root: workspaces_root,
    }
}

async fn claim_task_with_accepted_start(
    harness: &common::Harness,
    daemon_socket: &mut common::fake_daemon::ClientSocket,
    project_id: &str,
    agent_id: &str,
    title: &str,
    description: &str,
    zero_execution_retry_budget: bool,
) -> (String, String) {
    let mut created_task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({
            "title": title,
            "description": description,
        }),
        StatusCode::OK,
    )
    .await;
    if zero_execution_retry_budget {
        created_task = json_request(
            &harness.app,
            Method::PATCH,
            &format!("/api/v1/tasks/{}", created_task.id),
            json!({
                "version": created_task.version,
                "task_state_config": { "retry_budgets": { "execution": 0 } }
            }),
            StatusCode::OK,
        )
        .await;
    }
    let task_id = created_task.id.clone();

    let claim_app = harness.app.clone();
    let claim_agent_id = agent_id.to_owned();
    let claim_task_id = task_id.clone();
    let claim_handle = tokio::spawn(async move {
        json_request::<TaskResponse>(
            &claim_app,
            Method::POST,
            &format!("/api/v1/tasks/{claim_task_id}/claim"),
            json!({ "agent_id": claim_agent_id, "overrides": null }),
            StatusCode::OK,
        )
        .await
    });

    let (start_id, start_params) = next_daemon_request(daemon_socket, METHOD_EXECUTION_START).await;
    let execution_id = start_params["execution_id"]
        .as_str()
        .expect("execution id in start params")
        .to_owned();
    send_daemon_response(
        daemon_socket,
        start_id,
        api_types::ExecutionStartResult {
            execution_id: execution_id.clone(),
            accepted: true,
        },
    )
    .await;

    let claimed = claim_handle.await.expect("claim task joins");
    assert_eq!(claimed.status, "in_progress".to_owned());

    (task_id, execution_id)
}

async fn complete_remote_reviewer(daemon_socket: &mut common::fake_daemon::ClientSocket) {
    let (start_id, start_params) = next_daemon_request(daemon_socket, METHOD_EXECUTION_START).await;
    let review_execution_id = start_params["execution_id"]
        .as_str()
        .expect("review execution id in start params")
        .to_owned();
    send_daemon_response(
        daemon_socket,
        start_id,
        api_types::ExecutionStartResult {
            execution_id: review_execution_id.clone(),
            accepted: true,
        },
    )
    .await;
    send_execution_terminal_completed(
        daemon_socket,
        &review_execution_id,
        Some("Looks good.\n===REVIEW: PASS===\n"),
    )
    .await;
}

#[tokio::test]
async fn remote_execution_completes_and_transitions_task() {
    let mut fixture = setup_remote_roundtrip("remote-roundtrip-success").await;

    let (task_id, execution_id) = claim_task_with_accepted_start(
        &fixture.harness,
        &mut fixture.daemon_socket,
        &fixture.project_id,
        &fixture.agent_id,
        "Remote roundtrip success",
        "echo remote success",
        false,
    )
    .await;

    send_execution_log(
        &mut fixture.daemon_socket,
        &execution_id,
        1,
        "remote line one",
    )
    .await;
    let _ = poll_execution_logs(&fixture.harness.app, &execution_id, 1).await;
    send_execution_log(
        &mut fixture.daemon_socket,
        &execution_id,
        2,
        "remote line two",
    )
    .await;
    let _ = poll_execution_logs(&fixture.harness.app, &execution_id, 2).await;
    send_execution_log(
        &mut fixture.daemon_socket,
        &execution_id,
        3,
        "remote line three",
    )
    .await;
    // The daemon socket preserves frame order, but notification handling is
    // dispatched asynchronously. Wait until all three log rows are durable
    // before sending the terminal frame so the terminal CAS cannot race the
    // final log notification.
    let _ = poll_execution_logs(&fixture.harness.app, &execution_id, 3).await;
    send_execution_terminal_completed(
        &mut fixture.daemon_socket,
        &execution_id,
        Some("remote shell finished"),
    )
    .await;

    let completed = poll_until_execution_status(
        &fixture.harness.state,
        &execution_id,
        DbExecutionStatus::Completed,
    )
    .await;
    assert_eq!(completed.status, DbExecutionStatus::Completed);

    // The configured independent reviewer is also remote. Complete its
    // daemon-owned execution so the normal review-to-merge cascade remains
    // part of this round-trip instead of leaving the task at the review gate.
    complete_remote_reviewer(&mut fixture.daemon_socket).await;

    let reviewed =
        poll_task_status_after_execution(&fixture.harness.state.db, &task_id, "done").await;
    assert_eq!(reviewed.status, "done");

    let logs = poll_execution_logs(&fixture.harness.app, &execution_id, 3).await;
    let entries = logs["items"].as_array().expect("log items array");
    assert!(
        entries.len() >= 3,
        "expected persisted execution logs, got {}",
        entries.len()
    );
    let lines: Vec<&str> = entries
        .iter()
        .filter_map(|entry| {
            entry
                .get("payload")
                .and_then(|payload| payload.get("line"))
                .and_then(Value::as_str)
        })
        .collect();
    assert!(lines.iter().any(|line| line.contains("remote line one")));
    assert!(lines.iter().any(|line| line.contains("remote line three")));
}

#[tokio::test]
async fn remote_execution_failure_takes_failure_path() {
    let mut fixture = setup_remote_roundtrip("remote-roundtrip-failure").await;

    let (task_id, execution_id) = claim_task_with_accepted_start(
        &fixture.harness,
        &mut fixture.daemon_socket,
        &fixture.project_id,
        &fixture.agent_id,
        "Remote roundtrip failure",
        "this should fail remotely",
        true,
    )
    .await;

    send_execution_terminal_failed(
        &mut fixture.daemon_socket,
        &execution_id,
        "remote executor exploded",
    )
    .await;

    let failed = wait_for_execution_status(
        &fixture.harness.state,
        &execution_id,
        DbExecutionStatus::Failed,
    )
    .await;
    assert_eq!(failed.stop_reason, Some(StopReason::ExecutorFailed));
    assert!(
        failed
            .error
            .as_deref()
            .is_some_and(|message| message.contains("remote executor exploded")),
        "unexpected execution error: {:?}",
        failed.error
    );

    // The daemon terminal message persists the execution outcome before the
    // task recovery projection is applied. Wait for both durable records so
    // this assertion remains deterministic under workspace-wide test load.
    let task = poll_failed_task_recovery_state(&fixture.harness.state.db, &task_id).await;
    assert_eq!(task.status, "in_progress");
    assert!(
        task.blocked_json.is_some(),
        "failed remote execution should block the task for recovery"
    );
    assert!(
        task.error_annotation.is_some(),
        "failed remote execution should expose error annotation"
    );
}

#[tokio::test]
async fn remote_daemon_disconnect_fails_running_execution() {
    let mut fixture = setup_remote_roundtrip("remote-roundtrip-disconnect").await;

    let (task_id, execution_id) = claim_task_with_accepted_start(
        &fixture.harness,
        &mut fixture.daemon_socket,
        &fixture.project_id,
        &fixture.agent_id,
        "Remote disconnect",
        "running until disconnect",
        true,
    )
    .await;

    fixture
        .daemon_socket
        .send(WsMessage::Close(None))
        .await
        .expect("close daemon websocket");
    drop(fixture.server);
    wait_until_disconnected(&fixture.harness.state, &fixture.registration.daemon_id).await;

    let monitor = HeartbeatMonitor::new(
        Arc::clone(&fixture.harness.state.db),
        Arc::clone(&fixture.harness.state.event_bus),
    )
    .with_task_service(fixture.harness.state.task_service.clone())
    .with_daemon_connections(fixture.harness.state.daemon_connections.clone())
    .with_daemon_disconnect_grace(Duration::ZERO);

    let interrupted = monitor.check_once().await.expect("heartbeat monitor runs");
    assert_eq!(interrupted, 1);

    let execution = ExecutionRepo::get_by_id(&*fixture.harness.state.db, &execution_id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(execution.status, DbExecutionStatus::Failed);
    assert_eq!(execution.stop_reason, Some(StopReason::DaemonDisconnected));

    let task = TaskRepo::get_by_id(&*fixture.harness.state.db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(
        task.blocked_json.is_some(),
        "disconnect failure should block the task for recovery"
    );
    assert!(
        task.error_annotation.is_some(),
        "disconnect failure should expose recovery annotation"
    );
}

/// Fallback-chain round-trip over the daemon protocol: the snapshot carries
/// the route to the daemon, and the structured terminal notification carries
/// disposition, attempts, and the winner back for persistence.
#[tokio::test]
async fn remote_executor_unavailable_defers_and_persists_route() {
    let mut fixture = setup_remote_roundtrip("remote-unavailable").await;

    // A routed agent: shell primary with a null-executor fallback.
    let routed_agent: AgentResponse = json_request_with_bearer(
        &fixture.harness.app,
        Method::POST,
        "/api/v1/agents",
        &admin_jwt(),
        json!({
            "name": "remote-unavailable-routed-agent",
            "executor_type": "shell",
            "daemon_id": fixture.registration.daemon_id,
            "config_json": {
                "fallbacks": [ { "executor_type": "null", "config": {} } ]
            },
        }),
        StatusCode::OK,
    )
    .await;
    common::configure_execution_test_setup(
        &fixture.harness.state.db,
        &fixture.project_id,
        &fixture.repo_id,
        &routed_agent.id,
        &fixture.reviewer_id,
    )
    .await;

    let created_task: TaskResponse = json_request(
        &fixture.harness.app,
        Method::POST,
        &format!("/api/v1/projects/{}/tasks", fixture.project_id),
        json!({
            "title": "Remote unavailable roundtrip",
            "description": "exhaust every candidate",
            "role_assignments": [{
                "role_name": "coder",
                "assignee_type": "agent",
                "assignee_id": routed_agent.id
            }],
        }),
        StatusCode::OK,
    )
    .await;
    let task_id = created_task.id.clone();

    let claim_app = fixture.harness.app.clone();
    let claim_agent_id = routed_agent.id.clone();
    let claim_task_id = task_id.clone();
    let claim_handle = tokio::spawn(async move {
        json_request::<TaskResponse>(
            &claim_app,
            Method::POST,
            &format!("/api/v1/tasks/{claim_task_id}/claim"),
            json!({ "agent_id": claim_agent_id, "overrides": null }),
            StatusCode::OK,
        )
        .await
    });

    let (start_id, start_params) =
        next_daemon_request(&mut fixture.daemon_socket, METHOD_EXECUTION_START).await;
    let execution_id = start_params["execution_id"]
        .as_str()
        .expect("execution id in start params")
        .to_owned();
    // Server → daemon: the snapshot carries the full route.
    let routing = &start_params["executor_config"]["routing"];
    assert_eq!(routing["policy"], "ordered_fallback_v1");
    assert_eq!(
        routing["candidates"].as_array().expect("candidates").len(),
        2
    );

    send_daemon_response(
        &mut fixture.daemon_socket,
        start_id,
        api_types::ExecutionStartResult {
            execution_id: execution_id.clone(),
            accepted: true,
        },
    )
    .await;
    claim_handle.await.expect("claim task joins");

    // Daemon → server: every candidate exhausted, retry known in ~90s.
    let retry_at = (chrono::Utc::now() + chrono::Duration::seconds(90)).to_rfc3339();
    common::fake_daemon::send_daemon_notification(
        &mut fixture.daemon_socket,
        api_types::METHOD_EXECUTION_TERMINAL,
        api_types::ExecutionTerminalNotification {
            execution_id: execution_id.clone(),
            exit_code: Some(1),
            signal: None,
            error: Some("no executor candidate available".to_owned()),
            ts: db::now_rfc3339(),
            status: Some("failed".to_owned()),
            agent_session_id: None,
            summary: None,
            after_sha: None,
            usage: None,
            failure_class: Some(api_types::RemoteExecutionFailureClass::ExecutorUnavailable),
            retry_at: Some(retry_at),
            resolved_candidate: None,
            route_attempts: Some(vec![
                api_types::RemoteRouteAttempt {
                    candidate_key: "shell#primary".to_owned(),
                    outcome: "usage_exhausted".to_owned(),
                },
                api_types::RemoteRouteAttempt {
                    candidate_key: "null#fallback".to_owned(),
                    outcome: "unavailable".to_owned(),
                },
            ]),
        },
    )
    .await;

    let failed = poll_until_execution_status(
        &fixture.harness.state,
        &execution_id,
        DbExecutionStatus::Failed,
    )
    .await;
    assert_eq!(failed.status, DbExecutionStatus::Failed);

    // Transient unavailability: deferred dispatch scheduled, no retry budget
    // consumed, task not blocked.
    let mut deferred_seen = false;
    for _ in 0..200 {
        let task = TaskRepo::get_by_id(&*fixture.harness.state.db, &task_id, false)
            .await
            .expect("task lookup")
            .expect("task exists");
        let metadata: Value = task
            .metadata_json
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok())
            .unwrap_or_else(|| json!({}));
        if metadata.get("deferred_dispatch").is_some() {
            assert!(
                metadata.get("execution_retry_count").is_none(),
                "executor unavailability must not consume the retry budget; metadata: {metadata}"
            );
            assert!(
                task.blocked_json.is_none(),
                "transient unavailability must not block the task; blocked: {:?}",
                task.blocked_json
            );
            deferred_seen = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        deferred_seen,
        "expected a deferred dispatch to be scheduled"
    );

    // Attempts and disposition are persisted on the execution snapshot.
    let stored = ExecutionRepo::get_by_id(&*fixture.harness.state.db, &execution_id)
        .await
        .expect("execution lookup")
        .expect("execution exists");
    let snapshot: Value = serde_json::from_str(
        stored
            .executor_config_snapshot_json
            .as_deref()
            .expect("snapshot present"),
    )
    .expect("snapshot parses");
    assert_eq!(
        snapshot["routing"]["attempts"][0]["outcome"],
        "usage_exhausted"
    );
    assert_eq!(
        snapshot["routing"]["disposition"]["failure_class"],
        "executor_unavailable"
    );
}
