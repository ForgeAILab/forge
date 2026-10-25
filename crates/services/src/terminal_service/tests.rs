//! Terminal session lifecycle, PTY control, and attach token tests.

use super::*;
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, CreateProject, CreateRepo,
    CreateTask, CreateWorkspace, ProjectRepo, RepoRepo, TaskRepo, TerminalSessionRepo,
    UpdateTerminalSessionStatus, UserRepo, WorkspaceRepo, WorkspaceStatus,
};
use tempfile::TempDir;

const TEST_USER_ID: &str = "test-user-id";

struct NoopExecutionHandler;

#[async_trait::async_trait]
impl crate::daemon_transport::DaemonExecutionEventHandler for NoopExecutionHandler {
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

struct SeededTerminal {
    session_id: String,
    workspace_id: String,
}

async fn test_service(
    terminal_config: TerminalConfig,
) -> (TerminalService, Arc<SqliteDb>, TempDir) {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(SqliteDb::new(pool));
    seed_user(&db).await;
    let event_bus = Arc::new(EventBus::new(32));
    let daemon_connections = Arc::new(DaemonConnectionRegistry::new(
        Arc::clone(&event_bus),
        Arc::new(NoopExecutionHandler),
    ));
    let workspace_root = TempDir::new().expect("workspace root");
    let service = TerminalService::new_for_test(
        Arc::clone(&db),
        event_bus,
        daemon_connections,
        Arc::new(WorkspaceExecutionLockManager::default()),
        terminal_config,
        workspace_root.path().to_path_buf(),
    );
    (service, db, workspace_root)
}

async fn seed_user(db: &SqliteDb) {
    let now = now_rfc3339();
    UserRepo::create_user(
        db,
        &db::User {
            id: TEST_USER_ID.to_owned(),
            email: "test@example.com".to_owned(),
            password_hash: "$2b$04$placeholder".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("user creates");
}

async fn seed_running_terminal(db: &SqliteDb, workspace_root: &Path) -> SeededTerminal {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let task_id = new_uuid_v4();
    let workspace_id = new_uuid_v4();
    let worktree_path = workspace_root.join(&task_id).join("repo");
    std::fs::create_dir_all(&worktree_path).expect("worktree creates");

    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "Terminal Project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(TEST_USER_ID.to_owned()),
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
            assignee_type: None,
            assignee_id: None,
            title: "Terminal task".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "todo".to_owned(),
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
    WorkspaceRepo::create(
        db,
        CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task_id.clone(),
            repo_id,
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch: workspace::task_branch_name(&task_id),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("workspace creates");
    let created = TerminalSessionRepo::create_terminal_session(
        db,
        CreateTerminalSession {
            id: new_uuid_v4(),
            task_id,
            workspace_id: workspace_id.clone(),
            daemon_id: None,
            created_by_user_id: TEST_USER_ID.to_owned(),
            rows: 24,
            cols: 80,
            created_at: now.clone(),
        },
    )
    .await
    .expect("terminal session creates");
    let running = TerminalSessionRepo::update_terminal_session_status(
        db,
        &created.id,
        created.version,
        UpdateTerminalSessionStatus {
            status: DbTerminalSessionStatus::Running,
            started_at: Some(now.clone()),
            last_activity_at: Some(now),
            ended_at: None,
            pid: None,
            exit_code: None,
            exit_signal: None,
            exit_reason: None,
        },
    )
    .await
    .expect("terminal session runs");
    SeededTerminal {
        session_id: running.id,
        workspace_id,
    }
}

fn enabled_terminal() -> TerminalConfig {
    TerminalConfig {
        enabled: true,
        ..TerminalConfig::default()
    }
}

#[tokio::test]
async fn embedded_terminal_command_channel_close_kills_the_child() {
    let pair = native_pty_system().openpty(pty_size(24, 80)).unwrap();
    // A reader keeps the master open during shutdown, as the blocking
    // output reader does in the service. Dropping control alone is insufficient.
    let reader = pair.master.try_clone_reader().unwrap();
    let writer = pair.master.take_writer().unwrap();
    let mut command = CommandBuilder::new("/bin/sh");
    command.args(["-c", "exec sleep 30"]);
    let child: SharedChild = Arc::new(StdMutex::new(pair.slave.spawn_command(command).unwrap()));
    let (sender, receiver) = mpsc::unbounded_channel();
    spawn_embedded_control(
        "shutdown-test".to_owned(),
        pair.master,
        writer,
        receiver,
        Arc::clone(&child),
        Arc::new(AtomicU64::new(unix_timestamp_secs())),
        Arc::new(StdMutex::new(None)),
    );
    drop(sender);
    let exit = tokio_time::timeout(StdDuration::from_secs(2), async {
        loop {
            if let Some(status) = child.lock().unwrap().try_wait().unwrap() {
                return status;
            }
            tokio_time::sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await;
    if exit.is_err() {
        child.lock().unwrap().kill().unwrap();
    }
    drop(reader);
    assert!(exit.is_ok(), "closing terminal control must stop the child");
}

#[tokio::test]
async fn embedded_terminal_watcher_cancellation_kills_the_child_with_live_senders() {
    let pair = native_pty_system().openpty(pty_size(24, 80)).unwrap();
    let reader = pair.master.try_clone_reader().unwrap();
    let writer = pair.master.take_writer().unwrap();
    let mut command = CommandBuilder::new("/bin/sh");
    command.args(["-c", "exec sleep 30"]);
    let child: SharedChild = Arc::new(StdMutex::new(pair.slave.spawn_command(command).unwrap()));
    let (sender, receiver) = mpsc::unbounded_channel();
    spawn_embedded_control(
        "watcher-cancellation-test".to_owned(),
        pair.master,
        writer,
        receiver,
        Arc::clone(&child),
        Arc::new(AtomicU64::new(unix_timestamp_secs())),
        Arc::new(StdMutex::new(None)),
    );
    let cancellation = EmbeddedTerminalCancellation {
        command_tx: sender.clone(),
    };
    let watcher = tokio::spawn(async move {
        let _cancellation = cancellation;
        std::future::pending::<()>().await;
    });
    watcher.abort();
    assert!(watcher.await.unwrap_err().is_cancelled());
    let exit = tokio_time::timeout(StdDuration::from_secs(2), async {
        loop {
            if let Some(status) = child.lock().unwrap().try_wait().unwrap() {
                return status;
            }
            tokio_time::sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await;
    if exit.is_err() {
        child.lock().unwrap().kill().unwrap();
    }
    drop(sender);
    drop(reader);
    assert!(exit.is_ok(), "cancelling the watcher must stop the child");
}

#[tokio::test]
async fn attach_token_reuse_rejected() {
    let (service, db, root) = test_service(enabled_terminal()).await;
    let seeded = seed_running_terminal(&db, root.path()).await;

    // Service-level because a full WebSocket attach roundtrip is too heavy for this
    // single-use token invariant and Tower oneshot does not exercise upgraded sockets.
    let token = service
        .issue_attach_token(&seeded.session_id, TEST_USER_ID)
        .await
        .expect("token issues");
    service
        .consume_attach_token(&seeded.session_id, &token.attach_token)
        .await
        .expect("first consume succeeds");
    let error = service
        .consume_attach_token(&seeded.session_id, &token.attach_token)
        .await
        .expect_err("second consume rejects");

    assert!(matches!(error, ServiceError::TerminalAttachTokenInvalid));
}

#[tokio::test]
async fn attach_token_expiry_rejected() {
    let mut config = enabled_terminal();
    config.attach_token_ttl_secs = 0;
    let (service, db, root) = test_service(config).await;
    let seeded = seed_running_terminal(&db, root.path()).await;

    let token = service
        .issue_attach_token(&seeded.session_id, TEST_USER_ID)
        .await
        .expect("token issues");
    let error = service
        .consume_attach_token(&seeded.session_id, &token.attach_token)
        .await
        .expect_err("expired token rejects");

    assert!(matches!(error, ServiceError::TerminalAttachTokenInvalid));
}

#[tokio::test]
async fn expired_attach_tokens_are_swept_when_issuing_new_tokens() {
    let mut config = enabled_terminal();
    config.attach_token_ttl_secs = 0;
    let (service, db, root) = test_service(config).await;
    let seeded = seed_running_terminal(&db, root.path()).await;

    service
        .issue_attach_token(&seeded.session_id, TEST_USER_ID)
        .await
        .expect("first token issues");
    service
        .issue_attach_token(&seeded.session_id, TEST_USER_ID)
        .await
        .expect("second token issues");

    let state = service.state.lock().await;
    assert_eq!(state.attach_tokens.len(), 1);
}

#[tokio::test]
async fn detach_closed_clients_drops_scrollback_when_no_clients_remain() {
    let (service, _db, _root) = test_service(enabled_terminal()).await;
    let rx = service.attach_client("session-1").await;
    service
        .handle_daemon_output(TerminalOutputNotification {
            session_id: "session-1".to_owned(),
            data: STANDARD.encode("scrollback"),
            ts: now_rfc3339(),
        })
        .await;
    drop(rx);

    service.detach_closed_clients("session-1").await;
    service
        .handle_daemon_output(TerminalOutputNotification {
            session_id: "session-1".to_owned(),
            data: STANDARD.encode("after-detach"),
            ts: now_rfc3339(),
        })
        .await;

    let state = service.state.lock().await;
    assert!(!state.attached_clients.contains_key("session-1"));
    assert!(!state.scrollback.contains_key("session-1"));
}

#[tokio::test]
async fn resize_rejects_too_small_terminal_size() {
    let (service, db, root) = test_service(enabled_terminal()).await;
    let seeded = seed_running_terminal(&db, root.path()).await;

    let error = service
        .resize_session(&seeded.session_id, TEST_USER_ID, 1, 80)
        .await
        .expect_err("too-small resize rejects");

    assert!(matches!(error, ServiceError::TerminalInvalidInput { .. }));
}

#[tokio::test]
async fn cleanup_terminates_session_on_workspace_delete() {
    let (service, db, root) = test_service(enabled_terminal()).await;
    let seeded = seed_running_terminal(&db, root.path()).await;

    service
        .cleanup_workspace_terminals(&seeded.workspace_id)
        .await
        .expect("cleanup termination succeeds");
    let session = TerminalSessionRepo::get_terminal_session(&*db, &seeded.session_id)
        .await
        .expect("session loads")
        .expect("session remains queryable");

    assert_eq!(session.workspace_id, seeded.workspace_id);
    assert_eq!(session.status, DbTerminalSessionStatus::CleanupTerminated);
}
