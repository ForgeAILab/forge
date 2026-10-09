use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use db::{
    new_uuid_v4, now_rfc3339, AgentRepo, AgentStatus, ClaimExecutionLease, CreateAgent,
    CreateExecution, CreateProject, CreateRepo, CreateTask, DaemonRepo, DaemonStatus,
    ExecutionLeaseMutation, ExecutionRepo, ExecutionStatus, ProjectRepo, RepoRepo, TaskRepo,
    UpsertDaemon,
};
use events::EventBus;
use serde::Deserialize;
use serde_json::json;

use super::{
    execution_lease_owner, DaemonConnection, DaemonConnectionRegistry, DaemonExecutionEventHandler,
    DaemonTerminalDisposition, ServerExecutionEventSink,
};
use crate::ServiceError;

#[test]
fn socket_incarnations_are_random_numeric_tokens_across_new_allocators() {
    let ids: std::collections::HashSet<_> = (0..128)
        .map(|_| DaemonConnection::new("same-daemon".into()).0.id())
        .collect();
    assert_eq!(ids.len(), 128);
    assert!(ids.iter().all(|id| *id > 0 && *id <= i64::MAX as u64));
    // No process-local sequence/boot state can restart at the previous token.
    let token = super::new_connection_id();
    assert!(!ids.contains(&token));
}

struct NoopHandler;

#[async_trait]
impl DaemonExecutionEventHandler for NoopHandler {
    async fn handle_log(
        &self,
        _daemon_id: &str,
        _connection_id: u64,
        _notification: api_types::ExecutionLogNotification,
    ) -> Result<(), ServiceError> {
        Ok(())
    }

    async fn handle_terminal(
        &self,
        _daemon_id: &str,
        _connection_id: u64,
        _notification: api_types::ExecutionTerminalNotification,
    ) -> Result<(), ServiceError> {
        Ok(())
    }

    async fn handle_terminal_with_ack(
        &self,
        _daemon_id: &str,
        _connection_id: u64,
        _notification: api_types::ExecutionTerminalNotification,
    ) -> Result<DaemonTerminalDisposition, ServiceError> {
        Ok(DaemonTerminalDisposition::Acknowledge)
    }
}

struct ConflictHandler;

#[async_trait]
impl DaemonExecutionEventHandler for ConflictHandler {
    async fn handle_log(
        &self,
        _daemon_id: &str,
        _connection_id: u64,
        _notification: api_types::ExecutionLogNotification,
    ) -> Result<(), ServiceError> {
        Ok(())
    }

    async fn handle_terminal(
        &self,
        _daemon_id: &str,
        _connection_id: u64,
        _notification: api_types::ExecutionTerminalNotification,
    ) -> Result<(), ServiceError> {
        Ok(())
    }

    async fn handle_terminal_with_ack(
        &self,
        _daemon_id: &str,
        _connection_id: u64,
        _notification: api_types::ExecutionTerminalNotification,
    ) -> Result<DaemonTerminalDisposition, ServiceError> {
        Ok(DaemonTerminalDisposition::Conflict)
    }
}

struct IgnoreHandler;

#[async_trait]
impl DaemonExecutionEventHandler for IgnoreHandler {
    async fn handle_log(
        &self,
        _daemon_id: &str,
        _connection_id: u64,
        _notification: api_types::ExecutionLogNotification,
    ) -> Result<(), ServiceError> {
        Ok(())
    }

    async fn handle_terminal(
        &self,
        _daemon_id: &str,
        _connection_id: u64,
        _notification: api_types::ExecutionTerminalNotification,
    ) -> Result<(), ServiceError> {
        Ok(())
    }

    async fn handle_terminal_with_ack(
        &self,
        _daemon_id: &str,
        _connection_id: u64,
        _notification: api_types::ExecutionTerminalNotification,
    ) -> Result<DaemonTerminalDisposition, ServiceError> {
        Ok(DaemonTerminalDisposition::Ignore)
    }
}

#[derive(Default)]
struct AwaitingCascadeHandler {
    calls: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl DaemonExecutionEventHandler for AwaitingCascadeHandler {
    async fn handle_log(
        &self,
        _daemon_id: &str,
        _connection_id: u64,
        _notification: api_types::ExecutionLogNotification,
    ) -> Result<(), ServiceError> {
        Ok(())
    }

    async fn handle_terminal(
        &self,
        _daemon_id: &str,
        _connection_id: u64,
        _notification: api_types::ExecutionTerminalNotification,
    ) -> Result<(), ServiceError> {
        Ok(())
    }

    async fn handle_terminal_with_ack(
        &self,
        _daemon_id: &str,
        _connection_id: u64,
        _notification: api_types::ExecutionTerminalNotification,
    ) -> Result<DaemonTerminalDisposition, ServiceError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Ok(DaemonTerminalDisposition::AwaitingCascade)
    }
}

#[derive(Default)]
struct JournalReadinessHandler {
    ready: std::sync::atomic::AtomicBool,
    committed: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl DaemonExecutionEventHandler for JournalReadinessHandler {
    async fn handle_log(
        &self,
        _daemon_id: &str,
        _connection_id: u64,
        _notification: api_types::ExecutionLogNotification,
    ) -> Result<(), ServiceError> {
        Ok(())
    }

    async fn handle_terminal(
        &self,
        _daemon_id: &str,
        _connection_id: u64,
        _notification: api_types::ExecutionTerminalNotification,
    ) -> Result<(), ServiceError> {
        Ok(())
    }

    async fn handle_terminal_with_ack(
        &self,
        _daemon_id: &str,
        _connection_id: u64,
        _notification: api_types::ExecutionTerminalNotification,
    ) -> Result<DaemonTerminalDisposition, ServiceError> {
        Ok(if self.ready.load(std::sync::atomic::Ordering::Acquire) {
            DaemonTerminalDisposition::Acknowledge
        } else if self.committed.load(std::sync::atomic::Ordering::Acquire) {
            DaemonTerminalDisposition::AwaitingCascade
        } else {
            DaemonTerminalDisposition::Pending
        })
    }
}

fn make_registry() -> Arc<DaemonConnectionRegistry> {
    let event_bus = Arc::new(EventBus::new(16));
    let handler = Arc::new(NoopHandler) as Arc<dyn DaemonExecutionEventHandler>;
    Arc::new(DaemonConnectionRegistry::new(event_bus, handler))
}

async fn sqlite_db() -> Arc<db::SqliteDb> {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");
    Arc::new(db::SqliteDb::new(pool))
}

async fn seed_daemon(db: &db::SqliteDb, machine_id: &str) -> String {
    let now = now_rfc3339();
    let daemon_id = new_uuid_v4();
    DaemonRepo::upsert_by_machine_id(
        db,
        UpsertDaemon {
            max_concurrent_runs: None,
            id: daemon_id.clone(),
            machine_id: machine_id.to_owned(),
            hostname: "test-host".to_owned(),
            os: "linux".to_owned(),
            arch: "x86_64".to_owned(),
            agent_version: None,
            labels_json: "{}".to_owned(),
            status: DaemonStatus::Online,
            registration_token_hash: None,
            owner_id: None,
            visibility: "global".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("daemon creates");
    daemon_id
}

async fn seed_running_execution(
    db: &db::SqliteDb,
    owner_daemon_id: &str,
) -> (String, db::Execution) {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let task_id = new_uuid_v4();
    let agent_id = new_uuid_v4();

    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "Execution Events".to_owned(),
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
        db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "repo".to_owned(),
            remote_url: Some("file:///tmp/repo".to_owned()),
            local_path: None,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.clone(),
            project_id,
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
        db,
        CreateAgent {
            id: agent_id.clone(),
            name: "Owner Agent".to_owned(),
            description: None,
            executor_type: "shell".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: Some(owner_daemon_id.to_owned()),
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Busy,
            last_heartbeat_at: Some(now.clone()),
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("agent creates");
    let execution = ExecutionRepo::create(
        db,
        CreateExecution {
            id: new_uuid_v4(),
            task_id,
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
            last_activity_at: Some("1970-01-01T00:00:00Z".to_owned()),
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

    (agent_id.clone(), execution)
}

fn execution_event_sink(
    db: Arc<db::SqliteDb>,
    event_bus: Arc<EventBus>,
    workspace_root: std::path::PathBuf,
) -> Arc<ServerExecutionEventSink> {
    Arc::new(ServerExecutionEventSink::new(db, event_bus, workspace_root))
}

fn accept_protocol_handshake(
    registry: &DaemonConnectionRegistry,
    daemon_id: &str,
    connection_id: u64,
) {
    assert!(registry.dispatch_incoming_for_connection(
        daemon_id,
        connection_id,
        api_types::DaemonFrame::Notification {
            method: api_types::METHOD_DAEMON_HANDSHAKE.to_owned(),
            params: json!({
                "protocol_revision": api_types::DAEMON_PROTOCOL_REVISION,
                "capabilities": api_types::DAEMON_REQUIRED_CAPABILITIES,
            }),
        },
    ));
}

#[derive(Debug, Deserialize)]
struct TestResponse {
    message: String,
}

#[tokio::test]
async fn daemon_transport_registry_happy_path_completes_typed_response() {
    let registry = make_registry();
    let (connection, mut outbound) = DaemonConnection::new("daemon-1".to_owned());
    let connection_id = connection.id();
    registry.register("daemon-1".to_owned(), connection);
    accept_protocol_handshake(&registry, "daemon-1", connection_id);

    let dispatcher = registry.clone();
    let handle = tokio::spawn(async move {
        let frame = outbound.recv().await.expect("request frame sent");
        let api_types::DaemonFrame::Request { id, method, params } = frame else {
            panic!("expected request frame");
        };
        assert_eq!(method, "test.echo");
        assert_eq!(params["name"], "forge");
        dispatcher.dispatch_incoming(
            "daemon-1",
            api_types::DaemonFrame::Response {
                id,
                result: json!({ "message": "ok" }),
            },
        );
    });

    let result: TestResponse = registry
        .send_request("daemon-1", "test.echo", json!({ "name": "forge" }), 1)
        .await
        .expect("daemon request succeeds");

    assert_eq!(result.message, "ok");
    handle.await.expect("dispatcher task joins");
}

#[tokio::test]
async fn daemon_transport_registry_timeout_returns_daemon_timeout() {
    let registry = make_registry();
    let (connection, _outbound) = DaemonConnection::new("daemon-1".to_owned());
    let connection_id = connection.id();
    registry.register("daemon-1".to_owned(), connection);
    accept_protocol_handshake(&registry, "daemon-1", connection_id);

    let result: Result<TestResponse, ServiceError> = registry
        .send_request_with_timeout(
            "daemon-1",
            "test.timeout",
            json!({}),
            Duration::from_millis(50),
        )
        .await;

    assert!(matches!(
        result,
        Err(ServiceError::DaemonTimeout { daemon_id, method })
            if daemon_id == "daemon-1" && method == "test.timeout"
    ));
}

#[tokio::test]
async fn below_minimum_handshake_is_visible_upgrade_refusal() {
    let registry = make_registry();
    let (connection, mut outbound) = DaemonConnection::new("daemon-incompatible".to_owned());
    let connection_id = connection.id();
    registry.register("daemon-incompatible".to_owned(), connection);

    assert!(registry.dispatch_incoming_for_connection(
        "daemon-incompatible",
        connection_id,
        api_types::DaemonFrame::Notification {
            method: api_types::METHOD_DAEMON_HANDSHAKE.to_owned(),
            params: json!({
                "protocol_revision": 1,
                "capabilities": []
            }),
        },
    ));

    let rejection = outbound.recv().await.expect("protocol rejection frame");
    let api_types::DaemonFrame::Error { error, .. } = rejection else {
        panic!("expected protocol rejection error");
    };
    assert_eq!(error.code, api_types::DAEMON_UPGRADE_REQUIRED);
    assert!(registry.is_connected("daemon-incompatible"));
    assert!(registry.get("daemon-incompatible").unwrap().needs_upgrade());

    let result: Result<TestResponse, ServiceError> = registry
        .send_request("daemon-incompatible", "execution.start", json!({}), 1)
        .await;
    assert!(matches!(
        result,
        Err(ServiceError::DaemonUpgradeRequired { daemon_id }) if daemon_id == "daemon-incompatible"
    ));
}

#[tokio::test]
async fn pre_handshake_dispatch_is_rejected_without_sending_a_request() {
    let registry = make_registry();
    let (connection, mut outbound) = DaemonConnection::new("daemon-pre-handshake".to_owned());
    registry.register("daemon-pre-handshake".to_owned(), connection);

    let result: Result<TestResponse, ServiceError> = registry
        .send_request("daemon-pre-handshake", "execution.start", json!({}), 1)
        .await;
    assert!(matches!(
        result,
        Err(ServiceError::DaemonNotReady { daemon_id })
            if daemon_id == "daemon-pre-handshake"
    ));
    assert!(matches!(
        outbound.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn terminal_success_sends_ack_and_conflict_does_not() {
    let event_bus = Arc::new(EventBus::new(16));
    let handler = Arc::new(NoopHandler) as Arc<dyn DaemonExecutionEventHandler>;
    let registry = Arc::new(DaemonConnectionRegistry::new(event_bus, handler));
    let (connection, mut outbound) = DaemonConnection::new("daemon-terminal-ack".to_owned());
    let connection_id = connection.id();
    registry.register("daemon-terminal-ack".to_owned(), connection);
    accept_protocol_handshake(&registry, "daemon-terminal-ack", connection_id);
    let notification = json!({
        "terminal_report_id": "terminal-report-1",
        "execution_id": "execution-1",
        "exit_code": 0,
        "signal": null,
        "error": null,
        "ts": now_rfc3339(),
        "status": "completed",
        "usage_reports": []
    });

    registry.dispatch_incoming_for_connection(
        "daemon-terminal-ack",
        connection_id,
        api_types::DaemonFrame::Notification {
            method: api_types::METHOD_EXECUTION_TERMINAL.to_owned(),
            params: notification,
        },
    );
    let ack = outbound.recv().await.expect("terminal ack request");
    let api_types::DaemonFrame::Request { id, method, params } = ack else {
        panic!("expected terminal acknowledgement request");
    };
    assert_eq!(method, api_types::METHOD_JOURNAL_ACK);
    assert_eq!(params["entry_id"], "terminal-report-1");
    registry.dispatch_incoming_for_connection(
        "daemon-terminal-ack",
        connection_id,
        api_types::DaemonFrame::Response {
            id,
            result: json!({
                "entry_id": "terminal-report-1",
                "acknowledged": true
            }),
        },
    );

    let conflict_event_bus = Arc::new(EventBus::new(16));
    let conflict_handler = Arc::new(ConflictHandler) as Arc<dyn DaemonExecutionEventHandler>;
    let conflict_registry = Arc::new(DaemonConnectionRegistry::new(
        conflict_event_bus,
        conflict_handler,
    ));
    let (conflict_connection, mut conflict_outbound) =
        DaemonConnection::new("daemon-terminal-conflict".to_owned());
    let conflict_connection_id = conflict_connection.id();
    conflict_registry.register("daemon-terminal-conflict".to_owned(), conflict_connection);
    accept_protocol_handshake(
        &conflict_registry,
        "daemon-terminal-conflict",
        conflict_connection_id,
    );
    conflict_registry.dispatch_incoming_for_connection(
        "daemon-terminal-conflict",
        conflict_connection_id,
        api_types::DaemonFrame::Notification {
            method: api_types::METHOD_EXECUTION_TERMINAL.to_owned(),
            params: json!({
                "terminal_report_id": "terminal-report-conflict",
                "execution_id": "execution-1",
                "exit_code": 0,
                "signal": null,
                "error": null,
                "ts": now_rfc3339(),
                "status": "completed",
                "usage_reports": []
            }),
        },
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), conflict_outbound.recv())
            .await
            .is_err(),
        "conflicting terminal sink result must not send an acknowledgement"
    );
}

#[tokio::test]
async fn stale_connection_cannot_resolve_current_connection_request() {
    let registry = make_registry();
    let (first, _first_outbound) = DaemonConnection::new("daemon-incarnation".to_owned());
    let first_id = first.id();
    registry.register("daemon-incarnation".to_owned(), first);
    let (second, mut second_outbound) = DaemonConnection::new("daemon-incarnation".to_owned());
    let second_id = second.id();
    registry.register("daemon-incarnation".to_owned(), second);
    accept_protocol_handshake(&registry, "daemon-incarnation", second_id);

    let dispatcher = registry.clone();
    let mut request = tokio::spawn(async move {
        dispatcher
            .send_request::<_, TestResponse>("daemon-incarnation", "test.echo", json!({}), 1)
            .await
    });
    let frame = second_outbound.recv().await.expect("current request sent");
    let api_types::DaemonFrame::Request { id, .. } = frame else {
        panic!("expected current request frame");
    };

    assert!(!registry.dispatch_incoming_for_connection(
        "daemon-incarnation",
        first_id,
        api_types::DaemonFrame::Response {
            id: id.clone(),
            result: json!({"message": "stale"}),
        },
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut request)
            .await
            .is_err()
    );

    assert!(registry.dispatch_incoming_for_connection(
        "daemon-incarnation",
        second_id,
        api_types::DaemonFrame::Response {
            id,
            result: json!({"message": "current"}),
        },
    ));
    let response = request
        .await
        .expect("request task joins")
        .expect("response succeeds");
    assert_eq!(response.message, "current");
}

#[tokio::test]
async fn daemon_transport_registry_unknown_daemon_returns_unavailable() {
    let registry = make_registry();

    let result: Result<TestResponse, ServiceError> = registry
        .send_request("missing-daemon", "test.echo", json!({}), 1)
        .await;

    assert!(matches!(
        result,
        Err(ServiceError::DaemonUnavailable { daemon_id })
            if daemon_id == "missing-daemon"
    ));
}

#[test]
fn daemon_transport_register_returns_prior_connection_on_second_call() {
    let registry = make_registry();
    let (first, _first_outbound) = DaemonConnection::new("daemon-1".to_owned());
    let (second, _second_outbound) = DaemonConnection::new("daemon-1".to_owned());

    let first_prior = registry.register("daemon-1".to_owned(), first);
    assert!(first_prior.is_none());

    let second_prior = registry.register("daemon-1".to_owned(), second);
    let prior = second_prior.expect("second register returns prior connection");
    assert_eq!(prior.daemon_id, "daemon-1");
    assert!(registry.is_connected("daemon-1"));
}

fn register_daemon_connection(registry: &DaemonConnectionRegistry, daemon_id: &str) -> u64 {
    let (connection, _outbound) = DaemonConnection::new(daemon_id.to_owned());
    let connection_id = connection.id();
    registry.register(daemon_id.to_owned(), connection);
    accept_protocol_handshake(registry, daemon_id, connection_id);
    connection_id
}

async fn claim_remote_execution(
    db: &db::SqliteDb,
    execution: &db::Execution,
    daemon_id: &str,
    connection_id: u64,
) -> db::Execution {
    let now = Utc::now();
    let mutation = ExecutionRepo::claim_lease(
        db,
        ClaimExecutionLease {
            execution_id: execution.id.clone(),
            expected_version: execution.execution_version,
            owner: execution_lease_owner(daemon_id, connection_id),
            lease_expires_at: (now + ChronoDuration::seconds(30)).to_rfc3339(),
            hard_deadline_at: Some((now + ChronoDuration::minutes(5)).to_rfc3339()),
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

#[tokio::test]
async fn execution_log_from_non_owner_daemon_is_rejected() {
    let db = sqlite_db().await;
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = std::env::temp_dir().join(format!(
        "forge-daemon-transport-events-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&workspace_root).expect("workspace root creates");
    let owner_daemon_id = seed_daemon(&db, "owner-machine").await;
    let other_daemon_id = seed_daemon(&db, "other-machine").await;
    let (_agent_id, execution) = seed_running_execution(&db, &owner_daemon_id).await;
    let sink = execution_event_sink(Arc::clone(&db), Arc::clone(&event_bus), workspace_root);
    let registry = Arc::new(DaemonConnectionRegistry::new(
        Arc::clone(&event_bus),
        sink.clone() as Arc<dyn DaemonExecutionEventHandler>,
    ));
    register_daemon_connection(&registry, &owner_daemon_id);
    register_daemon_connection(&registry, &other_daemon_id);

    registry.dispatch_incoming(
        &other_daemon_id,
        api_types::DaemonFrame::Notification {
            method: api_types::METHOD_EXECUTION_LOG.to_owned(),
            params: json!({
                "execution_id": execution.id,
                "seq": 1,
                "stream": "stdout",
                "line": "forged log",
                "ts": now_rfc3339(),
            }),
        },
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    let unchanged = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(unchanged.status, ExecutionStatus::Running);
    assert_eq!(
        unchanged.last_activity_at.as_deref(),
        Some("1970-01-01T00:00:00Z")
    );
}

#[tokio::test]
async fn execution_log_for_unknown_execution_is_ignored() {
    let db = sqlite_db().await;
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = std::env::temp_dir().join(format!(
        "forge-daemon-transport-unknown-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&workspace_root).expect("workspace root creates");
    let owner_daemon_id = seed_daemon(&db, "owner-machine-unknown").await;
    let sink = execution_event_sink(Arc::clone(&db), Arc::clone(&event_bus), workspace_root);
    let registry = Arc::new(DaemonConnectionRegistry::new(
        Arc::clone(&event_bus),
        sink as Arc<dyn DaemonExecutionEventHandler>,
    ));
    register_daemon_connection(&registry, &owner_daemon_id);

    registry.dispatch_incoming(
        &owner_daemon_id,
        api_types::DaemonFrame::Notification {
            method: api_types::METHOD_EXECUTION_LOG.to_owned(),
            params: json!({
                "execution_id": new_uuid_v4(),
                "seq": 1,
                "stream": "stdout",
                "line": "missing execution",
                "ts": now_rfc3339(),
            }),
        },
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn execution_log_from_owner_records_semantic_progress_without_heartbeat() {
    let db = sqlite_db().await;
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = std::env::temp_dir().join(format!(
        "forge-daemon-transport-activity-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&workspace_root).expect("workspace root creates");
    let owner_daemon_id = seed_daemon(&db, "owner-machine-activity").await;
    let (_agent_id, execution) = seed_running_execution(&db, &owner_daemon_id).await;
    let sink = execution_event_sink(Arc::clone(&db), Arc::clone(&event_bus), workspace_root);
    let registry = Arc::new(DaemonConnectionRegistry::new(
        Arc::clone(&event_bus),
        sink.clone() as Arc<dyn DaemonExecutionEventHandler>,
    ));
    let connection_id = register_daemon_connection(&registry, &owner_daemon_id);
    sink.set_connection_registry(Arc::downgrade(&registry));
    let execution = claim_remote_execution(&db, &execution, &owner_daemon_id, connection_id).await;
    let activity_ts = now_rfc3339();

    registry.dispatch_incoming(
        &owner_daemon_id,
        api_types::DaemonFrame::Notification {
            method: api_types::METHOD_EXECUTION_LOG.to_owned(),
            params: json!({
                "execution_id": execution.id,
                "seq": 1,
                "stream": "stdout",
                "line": "activity bump",
                "ts": activity_ts,
            }),
        },
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let updated = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        // Progress and log-path persistence are separate asynchronous writes.
        // Wait for both rather than asserting inside their completion window.
        if updated.last_progress_at.is_some() && updated.logs_path.is_some() {
            assert_eq!(
                updated.last_activity_at.as_deref(),
                Some("1970-01-01T00:00:00Z")
            );
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "execution log progress and path were not both persisted"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn remote_transport_heartbeat_renews_silent_execution() {
    let db = sqlite_db().await;
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = std::env::temp_dir().join(format!(
        "forge-daemon-transport-heartbeat-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&workspace_root).expect("workspace root creates");
    let owner_daemon_id = seed_daemon(&db, "owner-machine-heartbeat").await;
    let (_agent_id, execution) = seed_running_execution(&db, &owner_daemon_id).await;
    let sink = execution_event_sink(Arc::clone(&db), Arc::clone(&event_bus), workspace_root);
    let registry = Arc::new(DaemonConnectionRegistry::new(
        Arc::clone(&event_bus),
        sink.clone() as Arc<dyn DaemonExecutionEventHandler>,
    ));
    let connection_id = register_daemon_connection(&registry, &owner_daemon_id);
    sink.set_connection_registry(Arc::downgrade(&registry));
    let execution = claim_remote_execution(&db, &execution, &owner_daemon_id, connection_id).await;
    let prior_heartbeat = execution.last_heartbeat_at.clone();

    registry.dispatch_incoming(
        &owner_daemon_id,
        api_types::DaemonFrame::Heartbeat { seq: 1 },
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let updated = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        if updated.last_heartbeat_at != prior_heartbeat {
            assert!(updated.lease_expires_at.is_some());
            assert_eq!(updated.status, ExecutionStatus::Running);
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "remote heartbeat did not renew the silent execution"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn execution_terminal_from_non_owner_daemon_is_rejected() {
    let db = sqlite_db().await;
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = std::env::temp_dir().join(format!(
        "forge-daemon-transport-terminal-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&workspace_root).expect("workspace root creates");
    let owner_daemon_id = seed_daemon(&db, "owner-machine-terminal").await;
    let other_daemon_id = seed_daemon(&db, "other-machine-terminal").await;
    let (_agent_id, execution) = seed_running_execution(&db, &owner_daemon_id).await;
    let sink = execution_event_sink(Arc::clone(&db), Arc::clone(&event_bus), workspace_root);
    let registry = Arc::new(DaemonConnectionRegistry::new(
        Arc::clone(&event_bus),
        sink as Arc<dyn DaemonExecutionEventHandler>,
    ));
    register_daemon_connection(&registry, &owner_daemon_id);
    register_daemon_connection(&registry, &other_daemon_id);

    registry.dispatch_incoming(
        &other_daemon_id,
        api_types::DaemonFrame::Notification {
            method: api_types::METHOD_EXECUTION_TERMINAL.to_owned(),
            params: json!({
                "execution_id": execution.id,
                "exit_code": 0,
                "ts": now_rfc3339(),
                "status": "completed",
            }),
        },
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    let unchanged = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(unchanged.status, ExecutionStatus::Running);
}

#[tokio::test]
async fn connection_snapshot_retains_handshake_facts_only_for_current_incarnation() {
    let registry = make_registry();
    let (connection, _outbound) = DaemonConnection::new("snapshot-owner".to_owned());
    let first_id = connection.id();
    registry.register("snapshot-owner".to_owned(), connection.clone());
    let handshake = json!({"protocol_revision": api_types::DAEMON_PROTOCOL_REVISION,
        "capabilities": [api_types::DAEMON_CAPABILITY_USAGE_REPORTS, api_types::DAEMON_CAPABILITY_JOURNAL_ACK, "workspace.v1", api_types::DAEMON_CAPABILITY_PLAN_TRANSPORT],
        "executor_capabilities": {"codex": {"resume": true, "usage": true}},
        "workspace_run_policy": {"allowed_purposes": ["ci_step", "hook"]}});
    registry.dispatch_incoming_for_connection(
        "snapshot-owner",
        first_id,
        api_types::DaemonFrame::Notification {
            method: api_types::METHOD_DAEMON_HANDSHAKE.to_owned(),
            params: handshake.clone(),
        },
    );
    let snapshots = registry.connection_snapshots();
    let facts = &snapshots["snapshot-owner"];
    assert_eq!(facts.connection_id, first_id);
    assert!(!facts.workspace_incapable);
    assert!(facts.handshake.executor_capabilities["codex"].resume);
    assert_eq!(
        facts.handshake.workspace_run_policy.allowed_purposes,
        vec![
            api_types::WorkspaceRunPurpose::CiStep,
            api_types::WorkspaceRunPurpose::Hook
        ]
    );
    let (replacement, _replacement_outbound) = DaemonConnection::new("snapshot-owner".to_owned());
    registry.register("snapshot-owner".to_owned(), replacement);
    assert!(registry.connection_snapshots().is_empty());
    assert!(connection.snapshot().is_none());
    assert!(!registry.dispatch_incoming_for_connection(
        "snapshot-owner",
        first_id,
        api_types::DaemonFrame::Notification {
            method: api_types::METHOD_DAEMON_HANDSHAKE.to_owned(),
            params: handshake,
        }
    ));
    assert!(registry.connection_snapshots().is_empty());
}

#[tokio::test]
async fn connection_snapshot_revision_two_needs_upgrade_and_cannot_dispatch() {
    let registry = make_registry();
    let (connection, _outbound) = DaemonConnection::new("old-owner".to_owned());
    let id = connection.id();
    registry.register("old-owner".to_owned(), connection);
    registry.dispatch_incoming_for_connection("old-owner", id, api_types::DaemonFrame::Notification {
        method: api_types::METHOD_DAEMON_HANDSHAKE.to_owned(),
        params: json!({"protocol_revision": 2, "capabilities": [api_types::DAEMON_CAPABILITY_USAGE_REPORTS, "execution.terminal.ack", "workspace.v1"]}),
    });
    assert!(registry.is_connected("old-owner"));
    assert!(registry.connection_snapshots().is_empty());
    let connection = registry.get("old-owner").unwrap();
    assert!(connection.needs_upgrade());
    assert!(!connection.protocol_allows_dispatch());
    let result: Result<TestResponse, ServiceError> = registry
        .send_request("old-owner", "execution.start", json!({}), 1)
        .await;
    assert!(matches!(
        result,
        Err(ServiceError::DaemonUpgradeRequired { .. })
    ));
}

#[tokio::test]
async fn journal_drain_waits_for_terminal_outbox_and_owner_ack() {
    let handler = Arc::new(JournalReadinessHandler::default());
    let registry = Arc::new(DaemonConnectionRegistry::new(
        Arc::new(EventBus::new(16)),
        handler.clone(),
    ));
    let (connection, mut outbound) = DaemonConnection::new("journal-owner".to_owned());
    let connection_id = connection.id();
    registry.register("journal-owner".to_owned(), connection);
    accept_protocol_handshake(&registry, "journal-owner", connection_id);
    let notification: api_types::ExecutionTerminalNotification = serde_json::from_value(json!({
        "terminal_report_id": "report-journal", "execution_id": "execution-journal", "exit_code": 0,
        "ts": now_rfc3339(), "status": "completed", "usage_reports": [],
    }))
    .unwrap();
    super::lock(&registry.inner.journal_terminals).insert(
        (
            "journal-owner".to_owned(),
            connection_id,
            notification.execution_id.clone(),
        ),
        notification,
    );
    let execution_ids = vec!["execution-journal".to_owned()];
    assert!(!registry
        .drain_execution_journal("journal-owner", connection_id, &execution_ids)
        .await
        .unwrap());
    assert!(outbound.try_recv().is_err());
    // Durable terminal settlement allows reconciliation to continue, while
    // the owner retains its report until the cascade can commit.
    handler
        .committed
        .store(true, std::sync::atomic::Ordering::Release);
    assert!(registry
        .drain_execution_journal("journal-owner", connection_id, &execution_ids)
        .await
        .unwrap());
    assert!(outbound.try_recv().is_err());
    assert!(
        super::lock(&registry.inner.journal_terminals).contains_key(&(
            "journal-owner".to_owned(),
            connection_id,
            "execution-journal".to_owned()
        ))
    );
    handler
        .ready
        .store(true, std::sync::atomic::Ordering::Release);
    let responder = {
        let registry = registry.clone();
        tokio::spawn(async move {
            let api_types::DaemonFrame::Request { id, method, params } =
                outbound.recv().await.unwrap()
            else {
                panic!("journal ack");
            };
            assert_eq!(method, api_types::METHOD_JOURNAL_ACK);
            assert_eq!(params["entry_id"], "report-journal");
            registry.dispatch_incoming_for_connection(
                "journal-owner",
                connection_id,
                api_types::DaemonFrame::Response {
                    id,
                    result: json!({"entry_id": "report-journal", "acknowledged": true}),
                },
            );
        })
    };
    assert!(registry
        .drain_execution_journal("journal-owner", connection_id, &execution_ids)
        .await
        .unwrap());
    responder.await.unwrap();
}

#[tokio::test]
async fn exact_terminal_replay_does_not_compete_with_reconciliation_drain() {
    let handler = Arc::new(AwaitingCascadeHandler::default());
    let registry = Arc::new(DaemonConnectionRegistry::new(
        Arc::new(EventBus::new(16)),
        handler.clone(),
    ));
    let (connection, _outbound) = DaemonConnection::new("journal-owner".to_owned());
    let connection_id = connection.id();
    registry.register("journal-owner".to_owned(), connection);
    accept_protocol_handshake(&registry, "journal-owner", connection_id);
    let notification: api_types::ExecutionTerminalNotification = serde_json::from_value(json!({
        "terminal_report_id": "report-journal", "execution_id": "execution-journal", "exit_code": 0,
        "ts": now_rfc3339(), "status": "completed", "usage_reports": [],
    }))
    .unwrap();
    for _ in 0..2 {
        registry.dispatch_incoming_for_connection(
            "journal-owner",
            connection_id,
            api_types::DaemonFrame::Notification {
                method: api_types::METHOD_EXECUTION_TERMINAL.to_owned(),
                params: serde_json::to_value(&notification).unwrap(),
            },
        );
    }
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        handler.calls.load(std::sync::atomic::Ordering::Acquire),
        1,
        "the describe-triggered exact replay reuses the retained notification"
    );

    assert!(registry
        .drain_execution_journal("journal-owner", connection_id, &[notification.execution_id])
        .await
        .unwrap());
    assert_eq!(
        handler.calls.load(std::sync::atomic::Ordering::Acquire),
        2,
        "reconciliation owns the next application of the retained report"
    );
}

#[tokio::test]
async fn cancelled_transport_request_removes_pending_sender_for_each_request_path() {
    for pinned in [false, true] {
        let registry = make_registry();
        let (connection, mut outbound) = DaemonConnection::new("cancelled-owner".to_owned());
        let connection_id = connection.id();
        registry.register("cancelled-owner".to_owned(), connection.clone());
        accept_protocol_handshake(&registry, "cancelled-owner", connection_id);
        let request = {
            let registry = registry.clone();
            tokio::spawn(async move {
                if pinned {
                    registry
                        .send_request_for_connection::<_, TestResponse>(
                            "cancelled-owner",
                            connection_id,
                            "test.echo",
                            json!({}),
                            30,
                        )
                        .await
                } else {
                    registry
                        .send_request::<_, TestResponse>(
                            "cancelled-owner",
                            "test.echo",
                            json!({}),
                            30,
                        )
                        .await
                }
            })
        };
        outbound.recv().await.unwrap();
        assert_eq!(super::lock(&connection.pending).len(), 1);
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        assert!(super::lock(&connection.pending).is_empty());
    }
}

#[tokio::test]
async fn daemon_transport_ignored_terminal_is_acknowledged() {
    let registry = Arc::new(DaemonConnectionRegistry::new(
        Arc::new(EventBus::new(16)),
        Arc::new(IgnoreHandler),
    ));
    let daemon_id = "ignored-owner";
    let (connection, mut outbound) = DaemonConnection::new(daemon_id.into());
    let connection_id = connection.id();
    registry.register(daemon_id.into(), connection);
    accept_protocol_handshake(&registry, daemon_id, connection_id);
    let notification = serde_json::from_value(json!({"terminal_report_id": "ignored-report",
        "execution_id": "late-execution", "exit_code": 0, "status": "completed", "ts": now_rfc3339(), "usage_reports": []})).unwrap();
    let attempt = {
        let registry = registry.clone();
        tokio::spawn(async move {
            registry
                .apply_journal_terminal(daemon_id, connection_id, notification)
                .await
                .unwrap()
        })
    };
    let api_types::DaemonFrame::Request { id, method, params } =
        tokio::time::timeout(Duration::from_secs(30), outbound.recv())
            .await
            .unwrap()
            .unwrap()
    else {
        panic!("expected acknowledgement");
    };
    assert_eq!(method, api_types::METHOD_JOURNAL_ACK);
    assert_eq!(params["entry_id"], "ignored-report");
    registry.dispatch_incoming_for_connection(
        daemon_id,
        connection_id,
        api_types::DaemonFrame::Response {
            id,
            result: json!({"entry_id": "ignored-report", "acknowledged": true}),
        },
    );
    assert_eq!(attempt.await.unwrap(), DaemonTerminalDisposition::Ignore);
}

#[tokio::test]
async fn revision_two_refuses_all_commands_but_an_unknown_handshake_is_not_an_upgrade() {
    let registry = make_registry();
    let (connection, mut outbound) = DaemonConnection::new("old-owner".into());
    let id = connection.id();
    registry.register("old-owner".into(), connection);
    let not_ready: Result<serde_json::Value, _> = registry
        .send_request("old-owner", api_types::METHOD_FS_LIST, json!({}), 1)
        .await;
    assert!(matches!(
        not_ready,
        Err(ServiceError::DaemonNotReady { .. })
    ));
    assert!(!registry.get("old-owner").unwrap().needs_upgrade());
    assert!(outbound.try_recv().is_err());
    registry.dispatch_incoming_for_connection("old-owner", id, api_types::DaemonFrame::Notification {
        method: api_types::METHOD_DAEMON_HANDSHAKE.into(),
        params: json!({"protocol_revision":2,"capabilities":["execution.terminal.usage_reports","execution.terminal.ack"]}),
    });
    let api_types::DaemonFrame::Error { error, .. } = outbound.recv().await.unwrap() else {
        panic!("upgrade notice");
    };
    assert_eq!(error.code, api_types::DAEMON_UPGRADE_REQUIRED);
    for method in [
        api_types::METHOD_EXECUTION_START,
        api_types::METHOD_REPO_LOCATION_VERIFY,
        api_types::METHOD_FS_LIST,
        api_types::METHOD_FS_BRANCHES,
        api_types::METHOD_TERMINAL_START,
    ] {
        let result: Result<serde_json::Value, _> = registry
            .send_request("old-owner", method, json!({}), 1)
            .await;
        let error = result.unwrap_err();
        assert!(matches!(error, ServiceError::DaemonUpgradeRequired { .. }));
        assert!(error.to_string().contains("upgrade the daemon"));
    }
    assert!(outbound.try_recv().is_err());
}

#[tokio::test]
async fn former_revision_three_handshake_requires_daemon_upgrade() {
    let registry = make_registry();
    let (connection, mut outbound) = DaemonConnection::new("revision-three".into());
    let id = connection.id();
    registry.register("revision-three".into(), connection);
    assert!(registry.dispatch_incoming_for_connection("revision-three", id, api_types::DaemonFrame::Notification {
        method: api_types::METHOD_DAEMON_HANDSHAKE.into(),
        params: json!({"protocol_revision":3,"capabilities":api_types::DAEMON_REQUIRED_CAPABILITIES}),
    }));
    let api_types::DaemonFrame::Error { error, .. } = outbound.recv().await.unwrap() else {
        panic!("old daemon accepted");
    };
    assert_eq!(error.code, api_types::DAEMON_UPGRADE_REQUIRED);
    assert!(error.message.contains("upgrade the daemon"));
    assert!(error.message.contains("revision 5"));
    assert!(registry.get("revision-three").unwrap().needs_upgrade());
    assert!(!registry
        .get("revision-three")
        .unwrap()
        .protocol_allows_dispatch());
}

#[tokio::test]
async fn revision_four_check_owner_is_refused_before_any_check_rpc() {
    let registry = make_registry();
    let (connection, mut outbound) = DaemonConnection::new("revision-four".into());
    let id = connection.id();
    registry.register("revision-four".into(), connection);
    assert!(registry.dispatch_incoming_for_connection("revision-four",id,api_types::DaemonFrame::Notification {method:api_types::METHOD_DAEMON_HANDSHAKE.into(),params:json!({"protocol_revision":4,"capabilities":api_types::DAEMON_REQUIRED_CAPABILITIES})}));
    let api_types::DaemonFrame::Error { error, .. } = outbound.recv().await.unwrap() else {
        panic!("old daemon accepted")
    };
    assert_eq!(error.code, api_types::DAEMON_UPGRADE_REQUIRED);
    for method in [
        api_types::METHOD_CHECK_RUN,
        api_types::METHOD_CHECK_LOOKUP,
        api_types::METHOD_CHECK_CANCEL,
    ] {
        let result: Result<serde_json::Value, _> = registry
            .send_request("revision-four", method, json!({}), 1)
            .await;
        assert!(matches!(
            result,
            Err(ServiceError::DaemonUpgradeRequired { .. })
        ));
    }
    assert!(outbound.try_recv().is_err());
}
