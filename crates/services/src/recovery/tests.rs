//! Shared recovery test fixtures and topic modules.

use super::*;
use crate::recovery::test_support::{
    seed_project_repo, seed_task, seed_task_with_assignee, sqlite_db,
};
use crate::{
    daemon_service::{DaemonReportInput, DaemonService, DetectedCliInput},
    daemon_transport::DaemonConnectionRegistry,
    workflow::{default_roles, default_states},
    TaskService,
};
use db::{
    new_uuid_v4, AgentContextScopeRepo, AgentProfileRepo, AgentSession, CreateAgent,
    CreateAgentContextScope, CreateAgentProfile, CreateAgentSession, CreateExecution, CreateReview,
    CreateTaskRoleAssignment, CreateWorkspace, CreateWorkspaceLease, DaemonRepo, DaemonStatus,
    DomainEventRepo, ReviewRepo, ReviewStatus, TaskRoleAssignmentRepo, UpsertDaemon, WorkspaceRepo,
    WorkspaceStatus,
};
use executors::{ExecutionContext, ExecutionOutcome, ExecutionResult, ExecutorError};
use serde_json::Value;

#[derive(Default)]
struct RecordingCancelExecutor {
    cancelled: std::sync::Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl TaskExecutor for RecordingCancelExecutor {
    async fn execute(
        &self,
        _ctx: ExecutionContext,
    ) -> std::result::Result<ExecutionResult, ExecutorError> {
        Ok(ExecutionResult {
            status: ExecutionOutcome::Completed,
            after_sha: None,
            agent_session_id: None,
            summary: None,
            error: None,
            usage_reports: Vec::new(),
            ..Default::default()
        })
    }

    async fn cancel(&self, execution_id: &str) -> std::result::Result<(), ExecutorError> {
        self.cancelled
            .lock()
            .expect("cancel log lock")
            .push(execution_id.to_owned());
        Ok(())
    }
}

async fn seed_agent(
    db: &SqliteDb,
    status: AgentStatus,
    last_heartbeat_at: Option<String>,
) -> Agent {
    let now = now_rfc3339();
    let daemon_id = new_uuid_v4();
    DaemonRepo::upsert_by_machine_id(
        db,
        UpsertDaemon {
            max_concurrent_runs: None,
            id: daemon_id.clone(),
            machine_id: format!("machine-{daemon_id}"),
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
            updated_at: now.clone(),
        },
    )
    .await
    .expect("daemon creates");

    AgentRepo::create(
        db,
        CreateAgent {
            id: new_uuid_v4(),
            name: "codex".to_owned(),
            description: None,
            executor_type: "shell".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: Some(daemon_id),
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 1,
            max_missed_heartbeats: 1,
            status,
            last_heartbeat_at,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            prompt_template: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("agent creates")
}

async fn seed_running_execution(
    db: &SqliteDb,
    task_id: String,
    agent_id: String,
    agent_session_id: Option<String>,
) -> db::Execution {
    seed_running_execution_in_workspace(db, task_id, agent_id, agent_session_id, None).await
}

async fn seed_running_execution_in_workspace(
    db: &SqliteDb,
    task_id: String,
    agent_id: String,
    agent_session_id: Option<String>,
    workspace_id: Option<String>,
) -> db::Execution {
    let project_version: i64 = sqlx::query_scalar(
        "SELECT project.version
             FROM project
             JOIN task ON task.project_id = project.id
             WHERE task.id = ?",
    )
    .bind(&task_id)
    .fetch_one(db.pool())
    .await
    .expect("execution Project version loads");
    let now = now_rfc3339();
    ExecutionRepo::create(
        db,
        CreateExecution {
            id: new_uuid_v4(),
            task_id,
            agent_id: Some(agent_id),
            role: "coder".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                json!({
                    "executor_type": "shell",
                    "config": {},
                    "project_version": project_version,
                })
                .to_string(),
            ),
            workspace_id,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("execution creates")
}

#[allow(clippy::too_many_arguments)]
async fn seed_agent_session(
    db: &SqliteDb,
    identity_id: &str,
    profile_id: &str,
    backend_kind: &str,
    scope_tag: &str,
    status: &str,
    connection_status: &str,
) -> AgentSession {
    let now = now_rfc3339();
    let scope = AgentContextScopeRepo::create_context_scope(
        db,
        CreateAgentContextScope {
            id: new_uuid_v4(),
            identity_id: identity_id.to_owned(),
            scope_type: "account".to_owned(),
            scope_id: scope_tag.to_owned(),
            project_id: None,
            task_id: None,
            task_role: None,
            workspace_access: "deny".to_owned(),
            workspace_path: None,
            authority_json: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("context scope creates");
    AgentSessionRepo::create_agent_session(
        db,
        CreateAgentSession {
            id: new_uuid_v4(),
            identity_id: identity_id.to_owned(),
            profile_id: profile_id.to_owned(),
            context_scope_id: scope.id,
            backend_kind: backend_kind.to_owned(),
            runtime_session_id: Some(new_uuid_v4()),
            status: status.to_owned(),
            capabilities_json: "{}".to_owned(),
            connection_status: connection_status.to_owned(),
            predecessor_session_id: None,
            last_activity_at: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("agent session creates")
}

async fn load_session(db: &SqliteDb, id: &str) -> AgentSession {
    AgentSessionRepo::get_agent_session(db, id)
        .await
        .expect("session loads")
        .expect("session exists")
}

async fn stamp_recovery_annotation(
    db: &SqliteDb,
    task: &Task,
    blocked_execution_id: Option<&str>,
) -> Task {
    let annotation = json!({
        "type": api_types::FailureKind::RecoveryRequired,
        "blocking_reason": "crash_recovery",
        "blocked_by": "system:crash_recovery",
        "blocked_at": now_rfc3339(),
        "blocked_execution_id": blocked_execution_id,
        "artifact": blocked_execution_id.map(|execution_id| json!({
            "kind": "execution",
            "id": execution_id,
            "log_path": null,
        })),
        "message": "Recovered after server restart",
    })
    .to_string();
    TaskRepo::update_status(
        db,
        UpdateTaskStatus {
            id: task.id.clone(),
            expected_version: task.version,
            status: task.status.clone(),
            assignee_id: None,
            error_annotation: Some(Some(annotation)),
            blocked_json: None,
            failed_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("annotation stamps")
}

async fn seed_agent_with_daemon(
    db: &SqliteDb,
    machine_id: &str,
    status: AgentStatus,
) -> (String, Agent) {
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
            updated_at: now.clone(),
        },
    )
    .await
    .expect("daemon creates");

    let agent = AgentRepo::create(
        db,
        CreateAgent {
            id: new_uuid_v4(),
            name: "remote".to_owned(),
            description: None,
            executor_type: "shell".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: Some(daemon_id),
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            prompt_template: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("agent creates");
    (agent.id.clone(), agent)
}

async fn seed_running_execution_with_created_at(
    db: &SqliteDb,
    task_id: String,
    agent_id: String,
    created_at: &str,
) -> db::Execution {
    let execution = seed_running_execution(db, task_id, agent_id, None).await;
    ExecutionRepo::update(
        db,
        UpdateExecution {
            id: execution.id.clone(),
            status: None,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            updated_at: created_at.to_owned(),
        },
    )
    .await
    .expect("execution timestamp updates");
    sqlx::query("UPDATE execution SET created_at = ? WHERE id = ?")
        .bind(created_at)
        .bind(&execution.id)
        .execute(db.pool())
        .await
        .expect("execution created_at updates");
    ExecutionRepo::get_by_id(db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists")
}

fn dispatch_marker_execution(
    lease_owner: Option<&str>,
    created_at: &str,
    hard_deadline_at: Option<&str>,
) -> Execution {
    Execution {
        id: "execution-1".to_owned(),
        task_id: "task-1".to_owned(),
        agent_id: None,
        role: "reviewer".to_owned(),
        status: ExecutionStatus::Running,
        parent_execution_id: None,
        agent_session_id: None,
        agent_message_id: None,
        summary: None,
        logs_path: None,
        before_sha: None,
        after_sha: None,
        error: None,
        executor_config_snapshot_json: None,
        workspace_id: None,
        created_at: created_at.to_owned(),
        updated_at: created_at.to_owned(),
        stop_reason: None,
        stopped_by: None,
        resume_policy: None,
        stopped_at: None,
        prompt: None,
        last_activity_at: None,
        execution_version: 1,
        lease_owner: lease_owner.map(str::to_owned),
        lease_expires_at: Some(created_at.to_owned()),
        hard_deadline_at: hard_deadline_at.map(str::to_owned),
        last_heartbeat_at: None,
        last_progress_at: None,
    }
}

pub(crate) async fn daemon_owned_fixture(db: &SqliteDb) -> (Task, WorkspacePlacement, Execution) {
    let (project_id, repo_id) = seed_project_repo(db).await;
    let agent = seed_agent(db, AgentStatus::Idle, Some(now_rfc3339())).await;
    let daemon_id = agent.daemon_id.clone().unwrap();
    let task = seed_task(
        db,
        project_id,
        "in_progress".to_owned(),
        Some(agent.id.clone()),
    )
    .await;
    let now = Utc::now();
    let runtime = db::RuntimeRepo::create(
        db,
        db::CreateRuntime {
            id: new_uuid_v4(),
            daemon_id: daemon_id.clone(),
            kind: "local".to_owned(),
            workspace_root: "/owner-only".to_owned(),
            status: db::RuntimeStatus::Ready,
            labels_json: "{}".to_owned(),
            created_at: now.to_rfc3339(),
            updated_at: now.to_rfc3339(),
        },
    )
    .await
    .unwrap();
    let workspace = db::WorkspaceRepo::create(
        db,
        CreateWorkspace {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            repo_id: repo_id.clone(),
            worktree_path: "opaque-owner-handle".to_owned(),
            branch: "task/remote".to_owned(),
            status: db::WorkspaceStatus::Ready,
            before_sha: Some("base-head".to_owned()),
            created_at: now.to_rfc3339(),
            updated_at: now.to_rfc3339(),
        },
    )
    .await
    .unwrap();
    let location = db::RepoLocationRepo::create(
        db,
        db::CreateRepoLocation {
            id: new_uuid_v4(),
            repo_id,
            owner_kind: db::RepoLocationOwnerKind::Daemon,
            daemon_id: Some(daemon_id.clone()),
            runtime_id: Some(runtime.id.clone()),
            path: "/owner-only/repo".to_owned(),
            kind: db::RepoLocationKind::PrimaryCheckout,
            is_default: true,
            status: db::RepoLocationStatus::Ready,
            last_verified_at: Some(now.to_rfc3339()),
            last_error: None,
            created_at: now.to_rfc3339(),
            updated_at: now.to_rfc3339(),
        },
    )
    .await
    .unwrap();
    let placement = WorkspacePlacementRepo::create(
        db,
        db::CreateWorkspacePlacement {
            id: new_uuid_v4(),
            workspace_id: workspace.id.clone(),
            task_id: task.id.clone(),
            agent_id: Some(agent.id.clone()),
            owner_kind: PlacementOwnerKind::Daemon,
            daemon_id: Some(daemon_id.clone()),
            runtime_id: Some(runtime.id),
            repo_location_id: location.id,
            execution_daemon_id: Some(daemon_id.clone()),
            workspace_handle: Some("opaque-owner-handle".to_owned()),
            generation: 1,
            state: PlacementState::Disconnected,
            selected_by: db::PlacementSelectedBy::Scheduler,
            selection_reason: "{}".to_owned(),
            reserved_until: None,
            disconnected_at: Some((now - ChronoDuration::minutes(10)).to_rfc3339()),
            failure_cause: Some(PlacementFailureCause::OwnerDisconnected),
            created_at: now.to_rfc3339(),
            updated_at: now.to_rfc3339(),
        },
    )
    .await
    .unwrap();
    let execution = seed_running_execution_in_workspace(
        db,
        task.id.clone(),
        agent.id,
        None,
        Some(workspace.id),
    )
    .await;
    let mutation = ExecutionRepo::claim_lease(
        db,
        db::ClaimExecutionLease {
            execution_id: execution.id.clone(),
            expected_version: execution.execution_version,
            owner: crate::daemon_transport::execution_lease_owner(&daemon_id, 1),
            lease_expires_at: (now - ChronoDuration::minutes(9)).to_rfc3339(),
            hard_deadline_at: Some((now + ChronoDuration::hours(2)).to_rfc3339()),
            now: (now - ChronoDuration::minutes(10)).to_rfc3339(),
        },
    )
    .await
    .unwrap();
    let db::ExecutionLeaseMutation::Updated(execution) = mutation else {
        panic!("lease fixture claims");
    };
    // The Task as stored now: its condition witnesses the running
    // execution seeded above.
    let task = TaskRepo::get_by_id(db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    (task, placement, execution)
}

pub(crate) fn owner_connection(
    registry: &DaemonConnectionRegistry,
    daemon_id: &str,
    resume: bool,
) -> (u64, tokio::sync::mpsc::Receiver<api_types::DaemonFrame>) {
    let (connection, outbound) =
        crate::daemon_transport::DaemonConnection::new(daemon_id.to_owned());
    let id = connection.id();
    registry.register(daemon_id.to_owned(), connection);
    registry.dispatch_incoming_for_connection(daemon_id, id, api_types::DaemonFrame::Notification {
            method: api_types::METHOD_DAEMON_HANDSHAKE.to_owned(),
            params: json!({"protocol_revision": api_types::DAEMON_PROTOCOL_REVISION,
                "capabilities": [api_types::DAEMON_CAPABILITY_USAGE_REPORTS, api_types::DAEMON_CAPABILITY_JOURNAL_ACK, "workspace.v1", api_types::DAEMON_CAPABILITY_PLAN_TRANSPORT],
                "executor_capabilities": {"shell": {"resume": resume}}, "workspace_run_policy": {"allowed_purposes": ["ci_step"]}}),
        });
    (id, outbound)
}

async fn expired_owner_grant(
    db: &SqliteDb,
    task: &Task,
    placement: &WorkspacePlacement,
    execution: &Execution,
) -> db::WorkspaceLease {
    let workspace = WorkspaceRepo::get_by_id(db, &placement.workspace_id)
        .await
        .unwrap()
        .unwrap();
    let now = Utc::now();
    WorkspaceLeaseRepo::issue(
            db,
            CreateWorkspaceLease {
                id: new_uuid_v4(),
                project_id: task.project_id.clone(),
                task_id: task.id.clone(),
                task_version: task.version,
                execution_id: execution.id.clone(),
                operation_idempotency_key: new_uuid_v4(),
                repository_binding_id: workspace.repo_id,
                base_ref: "main".to_owned(),
                role: "worker".to_owned(),
                capabilities_json: r#"["repository_write"]"#.to_owned(),
                assigned_principal_type: "agent".to_owned(),
                assigned_principal_id: execution.agent_id.clone().unwrap(),
                capability_profile_revision: "forge.capability-profile/v1".to_owned(),
                capability_profile_digest:
                    "sha256:eeb061a14ab862e1a7b16989ef637293ba538f46122ff28b30313d330dbae4a8"
                        .to_owned(),
                issuing_principal_type: "system".to_owned(),
                issuing_principal_id: "task-service-scheduler".to_owned(),
                issued_at: (now - ChronoDuration::minutes(10)).to_rfc3339(),
                expires_at: (now - ChronoDuration::minutes(9)).to_rfc3339(),
                created_at: (now - ChronoDuration::minutes(10)).to_rfc3339(),
                updated_at: (now - ChronoDuration::minutes(10)).to_rfc3339(),
            },
        )
        .await
        .unwrap()
}

async fn assert_owner_disconnect_order(socket_first: bool) {
    use crate::daemon_transport::{
        DaemonExecutionEventHandler, DaemonTerminalDisposition, ServerExecutionEventSink,
    };
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(32));
    let mut events = bus.subscribe();
    let (task, placement, execution) = daemon_owned_fixture(&db).await;
    let grant = expired_owner_grant(&db, &task, &placement, &execution).await;
    let mut update = placement_update(&placement, PlacementState::Ready, None);
    update.disconnected_at = Some(None);
    let ready = WorkspacePlacementRepo::update(&*db, update).await.unwrap();
    let root = tempfile::tempdir().unwrap();
    let sink = Arc::new(ServerExecutionEventSink::new(
        db.clone(),
        bus.clone(),
        root.path().to_path_buf(),
    ));
    let registry = Arc::new(DaemonConnectionRegistry::new(bus.clone(), sink.clone()));
    sink.set_connection_registry(Arc::downgrade(&registry));
    let service = Arc::new(
        TaskService::new(db.clone(), bus.clone()).with_daemon_connections(registry.clone()),
    );
    sink.set_task_service(Arc::downgrade(&service));
    let daemon_id = ready.daemon_id.as_deref().unwrap();
    let (connection_id, mut outbound) = owner_connection(&registry, daemon_id, false);
    let db::ExecutionLeaseMutation::Updated(execution) = ExecutionRepo::claim_lease(
        &*db,
        db::ClaimExecutionLease {
            execution_id: execution.id.clone(),
            expected_version: execution.execution_version,
            owner: crate::daemon_transport::execution_lease_owner(daemon_id, connection_id),
            lease_expires_at: execution.lease_expires_at.clone().unwrap(),
            hard_deadline_at: execution.hard_deadline_at.clone(),
            now: now_rfc3339(),
        },
    )
    .await
    .unwrap() else {
        panic!("expired owner claims");
    };
    let monitor =
        HeartbeatMonitor::new(db.clone(), bus.clone()).with_daemon_connections(registry.clone());
    if socket_first {
        sink.handle_disconnected(daemon_id).await.unwrap();
    }
    // No close frame or responder exists. The registered socket is still
    // connected; stale heartbeats alone must suspend the owner.
    assert!(registry.is_connected(daemon_id));
    tokio::time::timeout(Duration::from_secs(5), monitor.check_once())
        .await
        .unwrap()
        .unwrap();
    let disconnected = WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(disconnected.state, PlacementState::Disconnected);
    assert_eq!(
        disconnected.failure_cause,
        Some(PlacementFailureCause::OwnerDisconnected)
    );
    assert!(disconnected.disconnected_at.is_some());
    assert_eq!(disconnected.version, ready.version + 1);
    assert_eq!(
        ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap(),
        execution
    );
    assert_eq!(
        WorkspaceLeaseRepo::get_by_id(&*db, &grant.id)
            .await
            .unwrap()
            .unwrap(),
        grant
    );
    assert_eq!(
        TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap(),
        task
    );
    sink.handle_disconnected(daemon_id).await.unwrap();
    assert_eq!(
        WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
            .await
            .unwrap()
            .unwrap(),
        disconnected
    );
    let mut disconnect_events = 0;
    while let Ok(event) = events.try_recv() {
        disconnect_events += u64::from(event.event_type == "workspace.disconnected");
        assert_ne!(event.event_type, "execution.stalled");
    }
    assert_eq!(disconnect_events, 1);

    let notification: api_types::ExecutionTerminalNotification = serde_json::from_value(json!({
        "terminal_report_id": new_uuid_v4(), "execution_id": execution.id, "exit_code": 0,
        "ts": now_rfc3339(), "status": "completed", "after_sha": "owner-head", "usage_reports": [],
    }))
    .unwrap();
    // This also covers an owner thawing on the same still-open socket.
    for _ in 0..2 {
        assert_eq!(
            sink.handle_terminal_with_ack(daemon_id, connection_id, notification.clone())
                .await
                .unwrap(),
            DaemonTerminalDisposition::AwaitingCascade
        );
    }
    let responder = {
        let registry = registry.clone();
        let daemon_id = daemon_id.to_owned();
        tokio::spawn(async move {
            let api_types::DaemonFrame::Request { id, method, params } =
                outbound.recv().await.unwrap()
            else {
                panic!("describe request");
            };
            assert_eq!(method, api_types::METHOD_WORKSPACE_DESCRIBE);
            registry.dispatch_incoming_for_connection(&daemon_id, connection_id, api_types::DaemonFrame::Response {
                    id, result: json!({"workspace_handle": params["workspace_handle"], "generation": 1,
                        "exists": true, "head_sha": "owner-head", "dirty": false, "branch": "task/remote", "locked": false,
                        "active_execution_ids": [], "journaled_execution_ids": []}),
                });
        })
    };
    assert!(
        reconcile_workspace_placement(&db, &bus, &registry, None, &disconnected)
            .await
            .unwrap()
    );
    responder.await.unwrap();
    assert_eq!(
        sink.handle_terminal_with_ack(daemon_id, connection_id, notification)
            .await
            .unwrap(),
        DaemonTerminalDisposition::Acknowledge
    );
    let finished = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(finished.status, ExecutionStatus::Completed);
    assert_eq!(finished.after_sha.as_deref(), Some("owner-head"));
    assert_eq!(
        WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        PlacementState::Ready
    );
    let terminal_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM domain_event WHERE event_type = 'execution.completed'
             AND json_extract(payload_json, '$.execution_id') = ?",
    )
    .bind(&execution.id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(terminal_events, 1);
    let executions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM execution WHERE workspace_id = ?")
            .bind(&ready.workspace_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(executions, 1);
}

async fn assert_daemon_launch_recreates_worktree(damaged: bool) {
    let db = Arc::new(sqlite_db().await);
    let (task, placement, execution) = daemon_owned_fixture(&db).await;
    sqlx::query("UPDATE execution SET status = 'completed', after_sha = 'owner-head', lease_owner = NULL, lease_expires_at = NULL WHERE id = ?")
            .bind(&execution.id).execute(db.pool()).await.unwrap();
    let ready = WorkspacePlacementRepo::update(
        &*db,
        placement_update(&placement, PlacementState::Ready, None),
    )
    .await
    .unwrap();
    let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
    let daemon_id = ready.daemon_id.clone().unwrap();
    let (connection_id, mut outbound) = owner_connection(&registry, &daemon_id, false);
    let responder = {
        let registry = registry.clone();
        tokio::spawn(async move {
            for step in 0..4 {
                let api_types::DaemonFrame::Request { id, method, params } =
                    outbound.recv().await.unwrap()
                else {
                    panic!("owner launch request")
                };
                if step == 0 && damaged {
                    assert_eq!(method, api_types::METHOD_WORKSPACE_DESCRIBE);
                    registry.dispatch_incoming_for_connection(
                        &daemon_id,
                        connection_id,
                        api_types::DaemonFrame::Error {
                            id: Some(id),
                            error: api_types::DaemonErrorPayload {
                                code: "workspace_error".into(),
                                message: "git: not a git repository".into(),
                                details: None,
                            },
                        },
                    );
                    break;
                }
                let result = match step {
                    0 | 3 => {
                        assert_eq!(method, api_types::METHOD_WORKSPACE_DESCRIBE);
                        json!({"workspace_handle": params["workspace_handle"], "generation": 1, "exists": step == 3,
                                "head_sha": if step == 3 { Some("owner-head") } else { None }, "dirty": false,
                                "branch": "task/remote", "locked": false, "active_execution_ids": [], "journaled_execution_ids": []})
                    }
                    1 => {
                        assert_eq!(method, api_types::METHOD_WORKSPACE_PREPARE);
                        assert_eq!(params["expected"]["sha"], "base-head");
                        assert_eq!(params["branch"], "task/remote");
                        json!({"entry_id": "launch-recover-entry", "operation_id": params["operation_id"],
                                "workspace_handle": "opaque-owner-handle", "workspace_path": "/owner-only/rebuilt", "generation": 1,
                                "base_sha": "base-head", "branch": "task/remote"})
                    }
                    _ => {
                        assert_eq!(method, api_types::METHOD_JOURNAL_ACK);
                        json!({"entry_id": params["entry_id"], "acknowledged": true})
                    }
                };
                registry.dispatch_incoming_for_connection(
                    &daemon_id,
                    connection_id,
                    api_types::DaemonFrame::Response { id, result },
                );
            }
            outbound
        })
    };
    let root = tempfile::TempDir::new().unwrap();
    let service = TaskService::new(db.clone(), Arc::new(EventBus::new(32)))
        .with_workspace_root(root.path().to_path_buf());
    let router = (*service.workspace_backend_router())
        .clone()
        .with_daemon(Arc::new(
            crate::workspace_backend::DaemonWorkspaceBackend::new(db.clone(), registry),
        ));
    let prepared = crate::task_service::workspace::prepare_workspace(
        &db,
        root.path(),
        &task,
        &task.id,
        None,
        &router,
    )
    .await;
    if damaged {
        assert!(
            matches!(prepared, Err(ServiceError::Git(_))),
            "{prepared:?}"
        );
        assert!(
            responder.await.unwrap().try_recv().is_err(),
            "damaged describe must not send prepare"
        );
        assert_eq!(
            WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
                .await
                .unwrap()
                .unwrap(),
            ready
        );
        return;
    }
    let workspace = prepared.unwrap();
    let reused = crate::task_service::workspace::prepare_workspace(
        &db,
        root.path(),
        &task,
        &task.id,
        None,
        &router,
    )
    .await
    .unwrap();
    responder.await.unwrap();
    assert_eq!(workspace.id, ready.workspace_id);
    assert_eq!(reused.id, workspace.id);
    assert_eq!(reused.branch, "task/remote");
    let stored = WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.daemon_id, ready.daemon_id);
    assert_eq!(
        stored.workspace_handle.as_deref(),
        Some("opaque-owner-handle")
    );
    assert_eq!(ExecutionRepo::list_running(&*db).await.unwrap().len(), 0);
}

async fn assert_ready_owner_reset(task_reset: bool) {
    use crate::workspace_backend::{
        DaemonWorkspaceBackend, EmbeddedWorkspaceBackend, WorkspaceBackendRouter,
    };
    let db = Arc::new(sqlite_db().await);
    let (task, placement, execution) = daemon_owned_fixture(&db).await;
    sqlx::query("UPDATE execution SET status = 'completed', lease_owner = NULL, lease_expires_at = NULL WHERE id = ?")
            .bind(&execution.id).execute(db.pool()).await.unwrap();
    let placement = WorkspacePlacementRepo::update(
        &*db,
        placement_update(&placement, PlacementState::Ready, None),
    )
    .await
    .unwrap();
    annotate_owner_workspace_reset(&db, &placement, "reset owner workspace")
        .await
        .unwrap();
    let bus = Arc::new(EventBus::new(32));
    let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
    let daemon_id = placement.daemon_id.clone().unwrap();
    let (connection_id, mut outbound) = owner_connection(&registry, &daemon_id, false);
    let root = tempfile::tempdir().unwrap();
    let merge = Arc::new(crate::MergeService::new(
        db.clone(),
        root.path().to_path_buf(),
    ));
    let router = Arc::new(
        WorkspaceBackendRouter::new(Arc::new(EmbeddedWorkspaceBackend::new(
            db.clone(),
            merge,
            root.path().to_path_buf(),
        )))
        .with_daemon(Arc::new(DaemonWorkspaceBackend::new(
            db.clone(),
            registry.clone(),
        ))),
    );
    let service = TaskService::new(db.clone(), bus)
        .with_daemon_connections(registry.clone())
        .with_workspace_backend_router(router);
    let responder = {
        let registry = registry.clone();
        let expected_placement = placement.clone();
        tokio::spawn(async move {
            for step in 0..3 {
                let api_types::DaemonFrame::Request { id, method, params } =
                    outbound.recv().await.unwrap()
                else {
                    panic!("expected owner reset request");
                };
                let result = match step {
                    0 => {
                        assert_eq!(method, api_types::METHOD_WORKSPACE_DESCRIBE);
                        assert_eq!(params["generation"], 1);
                        json!({"workspace_handle": params["workspace_handle"], "generation": 1, "exists": true,
                                "head_sha": "candidate-head", "dirty": true, "branch": "task/remote", "locked": false,
                                "active_execution_ids": [], "journaled_execution_ids": []})
                    }
                    1 => {
                        assert_eq!(method, api_types::METHOD_WORKSPACE_RESET);
                        assert_eq!(
                            params["daemon_id"],
                            expected_placement.daemon_id.as_deref().unwrap()
                        );
                        assert_eq!(
                            params["runtime_id"],
                            expected_placement.runtime_id.as_deref().unwrap()
                        );
                        assert_eq!(params["placement_id"], expected_placement.id);
                        assert_eq!(params["workspace_handle"], "opaque-owner-handle");
                        assert_eq!(params["generation"], 2);
                        assert_eq!(params["expected"]["sha"], "candidate-head");
                        assert_eq!(params["base_ref"], "base-head");
                        json!({"entry_id": "reset-entry", "operation_id": params["operation_id"],
                                "workspace_handle": "opaque-owner-handle", "workspace_path": "/owner-only/rebuilt",
                                "generation": 2, "base_sha": "base-head", "branch": "task/remote"})
                    }
                    _ => {
                        assert_eq!(method, api_types::METHOD_JOURNAL_ACK);
                        json!({"entry_id": params["entry_id"], "acknowledged": true})
                    }
                };
                registry.dispatch_incoming_for_connection(
                    &daemon_id,
                    connection_id,
                    api_types::DaemonFrame::Response { id, result },
                );
            }
        })
    };
    if task_reset {
        let recovered = service
            .test_apply_action(
                &task.id,
                api_types::TaskAction::Restart { reason: None },
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(recovered.status, crate::workflow::default_states::TODO);
        assert!(recovered.error_annotation.is_none());
    } else {
        let workspace = service.reset_task_workspace(&task.id).await.unwrap();
        assert_eq!(workspace.id, placement.workspace_id);
        assert_eq!(workspace.before_sha.as_deref(), Some("base-head"));
    }
    responder.await.unwrap();
    let ready = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ready.generation, 2);
    assert_eq!(ready.version, placement.version + 1);
    assert_eq!(
        ready.workspace_handle.as_deref(),
        Some("opaque-owner-handle")
    );
    assert_eq!(ready.owner_kind, placement.owner_kind);
    assert_eq!(ready.daemon_id, placement.daemon_id);
    assert_eq!(ready.runtime_id, placement.runtime_id);
    assert_eq!(ready.state, PlacementState::Ready);
    assert!(!root.path().join(&task.id).exists());
}

mod execution_owner;
mod heartbeat;
mod owner_reconciliation;
mod placements;
mod startup;
mod workspace_leases;
mod workspace_reset;
