use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use db::{
    new_uuid_v4, now_rfc3339, AgentRepo, AgentStatus, ClaimExecutionLease, CreateAgent,
    CreateExecution, CreateProject, CreateRepo, CreateTask, DaemonRepo, DaemonStatus,
    ExecutionLeaseMutation, ExecutionRepo, ExecutionStatus, ProjectRepo, RepoRepo, TaskRepo,
    UpsertDaemon, WorkMode,
};
use events::EventBus;
use serde::Deserialize;
use serde_json::json;

use super::{
    execution_lease_owner, DaemonConnection, DaemonConnectionRegistry, DaemonExecutionEventHandler,
    ServerExecutionEventSink,
};
use crate::ServiceError;

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
            remote_url: "file:///tmp/repo".to_owned(),
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
        db,
        CreateTask {
            id: task_id.clone(),
            project_id,
            repo_id: Some(repo_id),
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

#[derive(Debug, Deserialize)]
struct TestResponse {
    message: String,
}

#[tokio::test]
async fn daemon_transport_registry_happy_path_completes_typed_response() {
    let registry = make_registry();
    let (connection, mut outbound) = DaemonConnection::new("daemon-1".to_owned());
    registry.register("daemon-1".to_owned(), connection);

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
    registry.register("daemon-1".to_owned(), connection);

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
async fn stale_connection_cannot_resolve_current_connection_request() {
    let registry = make_registry();
    let (first, _first_outbound) = DaemonConnection::new("daemon-incarnation".to_owned());
    let first_id = first.id();
    registry.register("daemon-incarnation".to_owned(), first);
    let (second, mut second_outbound) = DaemonConnection::new("daemon-incarnation".to_owned());
    let second_id = second.id();
    registry.register("daemon-incarnation".to_owned(), second);

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
        if updated.last_progress_at.is_some() {
            assert_eq!(
                updated.last_activity_at.as_deref(),
                Some("1970-01-01T00:00:00Z")
            );
            assert!(updated.logs_path.is_some());
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "execution last_progress_at was not updated"
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
