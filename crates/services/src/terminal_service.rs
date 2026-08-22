use std::{
    collections::{HashMap, VecDeque},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::{Duration as StdDuration, Instant, SystemTime, UNIX_EPOCH},
};

use api_types::{
    StateKind, TerminalAttachTokenResponse, TerminalAvailability, TerminalExitedNotification,
    TerminalInputParams, TerminalInputResult, TerminalOutputNotification, TerminalResizeParams,
    TerminalResizeResult, TerminalServerFrame, TerminalSessionResponse,
    TerminalSessionStatus as ApiTerminalSessionStatus, TerminalStartParams, TerminalStartResult,
    TerminalTerminateParams, TerminalTerminateResult, DEFAULT_DAEMON_COMMAND_TIMEOUT_SECS,
    METHOD_TERMINAL_INPUT, METHOD_TERMINAL_RESIZE, METHOD_TERMINAL_START,
    METHOD_TERMINAL_TERMINATE,
};
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat, Utc};
use config::TerminalConfig;
use db::{
    new_uuid_v4, now_rfc3339, AgentRepo, AssigneeKind, CreateTerminalSession, ProjectRepo,
    SqliteDb, Task, TaskRepo, TaskRoleAssignmentRepo, TerminalSession, TerminalSessionRepo,
    TerminalSessionStatus as DbTerminalSessionStatus, UpdateTerminalSessionStatus, Workspace,
    WorkspaceRepo, WorkspaceStatus,
};
use events::{event_timestamp, EventBus, EventContext, ForgeEvent};
use portable_pty::{native_pty_system, Child, CommandBuilder, MasterPty, PtySize};
use sha2::{Digest, Sha256};
use tokio::{
    sync::{mpsc, Mutex, OwnedMutexGuard},
    time as tokio_time,
};

use crate::{
    daemon_transport::{DaemonConnectionRegistry, DaemonTerminalEventHandler},
    workflow::{effective_role, engine::WorkflowEngine},
    workspace_cleanup::WorkspaceCleanupObserver,
    ServiceError, WorkspaceExecutionLockManager,
};

const DEFAULT_ROWS: u16 = 24;
const DEFAULT_COLS: u16 = 80;
const MIN_TERMINAL_DIMENSION: u16 = 2;
const TERMINAL_SESSION_CHANGED_EVENT: &str = "task.terminal.session_changed";
const EMBEDDED_WATCHDOG_MAX_INTERVAL: StdDuration = StdDuration::from_secs(30);
const EMBEDDED_EXIT_POLL_INTERVAL: StdDuration = StdDuration::from_millis(100);
const TERMINAL_REASON_IDLE_TIMEOUT: &str = "idle_timeout";
const TERMINAL_REASON_MAX_LIFETIME: &str = "max_lifetime";
const TERMINAL_REASON_WORKSPACE_CLEANUP: &str = "workspace_cleanup";

type SharedChild = Arc<StdMutex<Box<dyn Child + Send>>>;

#[derive(Clone)]
struct AttachTokenRecord {
    session_id: String,
    user_id: String,
    task_id: String,
    expires_at: DateTime<Utc>,
}

#[derive(Clone)]
struct EmbeddedTerminalHandle {
    command_tx: mpsc::UnboundedSender<EmbeddedTerminalCommand>,
}

enum EmbeddedTerminalCommand {
    Input(Vec<u8>),
    Resize { rows: u16, cols: u16 },
    Terminate { reason: Option<String> },
}

#[derive(Default)]
struct TerminalState {
    attach_tokens: HashMap<String, AttachTokenRecord>,
    scrollback: HashMap<String, VecDeque<u8>>,
    attached_clients: HashMap<String, Vec<mpsc::UnboundedSender<TerminalServerFrame>>>,
    embedded_terminals: HashMap<String, EmbeddedTerminalHandle>,
    workspace_lock_guards: HashMap<String, TerminalWorkspaceLockGuard>,
}

struct TerminalWorkspaceLockGuard {
    workspace_id: String,
    _guard: OwnedMutexGuard<()>,
}

#[derive(Debug, Default)]
pub struct TerminalActivityTracker {
    active: Mutex<HashMap<String, u32>>,
}

impl TerminalActivityTracker {
    pub async fn try_mark_active(&self, workspace_id: &str) -> bool {
        let mut active = self.active.lock().await;
        if active.get(workspace_id).copied().unwrap_or_default() > 0 {
            return false;
        }
        active.insert(workspace_id.to_owned(), 1);
        true
    }

    pub async fn release(&self, workspace_id: &str) {
        let mut active = self.active.lock().await;
        if let Some(count) = active.get_mut(workspace_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                active.remove(workspace_id);
            }
        }
    }

    pub async fn count(&self, workspace_id: &str) -> u32 {
        self.active
            .lock()
            .await
            .get(workspace_id)
            .copied()
            .unwrap_or_default()
    }

    pub async fn workspace_has_active_terminal(&self, workspace_id: &str) -> bool {
        self.count(workspace_id).await > 0
    }
}

pub struct TerminalService {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
    daemon_connections: Arc<DaemonConnectionRegistry>,
    workspace_exec_locks: Arc<WorkspaceExecutionLockManager>,
    terminal_config: TerminalConfig,
    workspace_root: PathBuf,
    state: Arc<Mutex<TerminalState>>,
    terminal_activity: Arc<TerminalActivityTracker>,
}

impl TerminalService {
    pub fn new(
        db: Arc<SqliteDb>,
        event_bus: Arc<EventBus>,
        daemon_connections: Arc<DaemonConnectionRegistry>,
        workspace_exec_locks: Arc<WorkspaceExecutionLockManager>,
        terminal_config: TerminalConfig,
        workspace_root: PathBuf,
    ) -> Self {
        Self::new_with_activity_tracker(
            db,
            event_bus,
            daemon_connections,
            workspace_exec_locks,
            terminal_config,
            workspace_root,
            Arc::new(TerminalActivityTracker::default()),
        )
    }

    pub fn new_with_activity_tracker(
        db: Arc<SqliteDb>,
        event_bus: Arc<EventBus>,
        daemon_connections: Arc<DaemonConnectionRegistry>,
        workspace_exec_locks: Arc<WorkspaceExecutionLockManager>,
        terminal_config: TerminalConfig,
        workspace_root: PathBuf,
        terminal_activity: Arc<TerminalActivityTracker>,
    ) -> Self {
        Self {
            db,
            event_bus,
            daemon_connections,
            workspace_exec_locks,
            terminal_config,
            workspace_root,
            state: Arc::new(Mutex::new(TerminalState::default())),
            terminal_activity,
        }
    }

    pub fn activity_tracker(&self) -> Arc<TerminalActivityTracker> {
        Arc::clone(&self.terminal_activity)
    }

    pub async fn availability(
        &self,
        task_id: &str,
        user_id: &str,
    ) -> Result<TerminalAvailability, ServiceError> {
        let task_sessions =
            TerminalSessionRepo::list_running_terminal_sessions_for_task(&*self.db, task_id)
                .await?;
        let user_sessions =
            TerminalSessionRepo::list_running_terminal_sessions_for_user(&*self.db, user_id)
                .await?;

        let mut availability = TerminalAvailability {
            enabled: self.terminal_config.enabled,
            workspace_ready: false,
            daemon_reachable: false,
            active_execution: false,
            session_count_for_task: task_sessions.len() as u32,
            session_count_for_user: user_sessions.len() as u32,
            max_sessions_per_task: self.terminal_config.max_sessions_per_task,
            max_sessions_per_user: self.terminal_config.max_sessions_per_user,
            can_create: false,
            reason: None,
        };

        if !self.terminal_config.enabled {
            availability.reason = Some(api_types::TERMINAL_DISABLED.to_owned());
            return Ok(availability);
        }

        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        let Some(workspace) = WorkspaceRepo::get_by_task_id(&*self.db, task_id).await? else {
            availability.reason = Some(api_types::TERMINAL_WORKSPACE_NOT_READY.to_owned());
            return Ok(availability);
        };

        availability.workspace_ready = self.workspace_ready_for_terminal(&task, &workspace).await?;
        if !availability.workspace_ready {
            availability.reason = Some(api_types::TERMINAL_WORKSPACE_NOT_READY.to_owned());
            return Ok(availability);
        }

        let daemon_id = self.resolve_terminal_daemon_id(&task, &workspace).await?;
        availability.daemon_reachable = daemon_id
            .as_deref()
            .is_none_or(|daemon_id| self.daemon_connections.is_connected(daemon_id));
        if !availability.daemon_reachable {
            availability.reason = Some(api_types::TERMINAL_DAEMON_UNAVAILABLE.to_owned());
            return Ok(availability);
        }

        availability.active_execution = self
            .workspace_exec_locks
            .try_acquire_async(&workspace.id)
            .await
            .is_none()
            || self.active_terminal_count(&workspace.id).await > 0;
        if availability.active_execution {
            availability.reason = Some(api_types::TERMINAL_ACTIVE_EXECUTION.to_owned());
            return Ok(availability);
        }

        if availability.session_count_for_task >= self.terminal_config.max_sessions_per_task {
            availability.reason = Some(api_types::TERMINAL_SESSION_LIMIT.to_owned());
            return Ok(availability);
        }
        if availability.session_count_for_user >= self.terminal_config.max_sessions_per_user {
            availability.reason = Some(api_types::TERMINAL_USER_LIMIT.to_owned());
            return Ok(availability);
        }

        availability.can_create = true;
        Ok(availability)
    }

    pub async fn create_session(
        &self,
        task_id: &str,
        user_id: &str,
        rows: Option<u16>,
        cols: Option<u16>,
    ) -> Result<(TerminalSessionResponse, TerminalAttachTokenResponse), ServiceError> {
        if !self.terminal_config.enabled {
            return Err(ServiceError::TerminalDisabled);
        }

        let rows = rows.unwrap_or(DEFAULT_ROWS);
        let cols = cols.unwrap_or(DEFAULT_COLS);
        validate_terminal_size(rows, cols)?;

        let (task, workspace) = self.require_terminal_workspace(task_id).await?;

        let task_sessions =
            TerminalSessionRepo::list_running_terminal_sessions_for_task(&*self.db, task_id)
                .await?;
        if task_sessions.len() as u32 >= self.terminal_config.max_sessions_per_task {
            return Err(ServiceError::TerminalSessionLimit {
                scope: "task".to_owned(),
            });
        }

        let user_sessions =
            TerminalSessionRepo::list_running_terminal_sessions_for_user(&*self.db, user_id)
                .await?;
        if user_sessions.len() as u32 >= self.terminal_config.max_sessions_per_user {
            return Err(ServiceError::TerminalSessionLimit {
                scope: "user".to_owned(),
            });
        }

        if self.active_terminal_count(&workspace.id).await > 0 {
            return Err(ServiceError::TerminalActiveExecution {
                workspace_id: workspace.id.clone(),
            });
        }

        self.validate_workspace_path(&workspace.worktree_path)
            .await?;
        let daemon_id = self.resolve_terminal_daemon_id(&task, &workspace).await?;
        if let Some(daemon_id) = daemon_id.as_deref() {
            if !self.daemon_connections.is_connected(daemon_id) {
                return Err(ServiceError::TerminalDaemonUnavailable {
                    daemon_id: daemon_id.to_owned(),
                });
            }
        }

        if !self.terminal_activity.try_mark_active(&workspace.id).await {
            return Err(ServiceError::TerminalActiveExecution {
                workspace_id: workspace.id.clone(),
            });
        }
        let Some(exec_lock_guard) = self
            .workspace_exec_locks
            .try_acquire_async(&workspace.id)
            .await
        else {
            self.terminal_activity.release(&workspace.id).await;
            return Err(ServiceError::TerminalActiveExecution {
                workspace_id: workspace.id.clone(),
            });
        };

        let session_id = new_uuid_v4();
        let now = now_rfc3339();
        let created_result = TerminalSessionRepo::create_terminal_session(
            &*self.db,
            CreateTerminalSession {
                id: session_id.clone(),
                task_id: task_id.to_owned(),
                workspace_id: workspace.id.clone(),
                daemon_id: daemon_id.clone(),
                created_by_user_id: user_id.to_owned(),
                rows: i64::from(rows),
                cols: i64::from(cols),
                created_at: now,
            },
        )
        .await;
        let created = match created_result {
            Ok(created) => created,
            Err(error) => {
                self.terminal_activity.release(&workspace.id).await;
                return Err(error.into());
            }
        };

        self.store_workspace_lock_guard(&created.id, &workspace.id, exec_lock_guard)
            .await;

        let start_result = self
            .start_terminal_process(&created, &workspace.worktree_path, rows, cols)
            .await;
        let start_result = match start_result {
            Ok(start_result) => start_result,
            Err(error) => {
                self.release_session_resources(&created.id, &workspace.id)
                    .await;
                return Err(error);
            }
        };
        let running = TerminalSessionRepo::update_terminal_session_status(
            &*self.db,
            &created.id,
            created.version,
            UpdateTerminalSessionStatus {
                status: DbTerminalSessionStatus::Running,
                started_at: Some(start_result.started_at.clone()),
                last_activity_at: Some(start_result.started_at),
                ended_at: None,
                pid: start_result.pid.map(i64::from),
                exit_code: None,
                exit_signal: None,
                exit_reason: None,
            },
        )
        .await?;

        self.publish_session_changed(&running, "created", None);
        let attach = self.issue_attach_token(&running.id, user_id).await?;

        Ok((terminal_session_response(running), attach))
    }

    pub async fn list_sessions(
        &self,
        task_id: &str,
        include_ended: bool,
    ) -> Result<Vec<TerminalSessionResponse>, ServiceError> {
        let sessions =
            TerminalSessionRepo::list_terminal_sessions_for_task(&*self.db, task_id, include_ended)
                .await?;
        Ok(sessions
            .into_iter()
            .map(terminal_session_response)
            .collect())
    }

    pub async fn get_session(
        &self,
        session_id: &str,
    ) -> Result<TerminalSessionResponse, ServiceError> {
        let session = self.load_session(session_id).await?;
        Ok(terminal_session_response(session))
    }

    pub async fn issue_attach_token(
        &self,
        session_id: &str,
        user_id: &str,
    ) -> Result<TerminalAttachTokenResponse, ServiceError> {
        let session = self.load_session(session_id).await?;
        if !terminal_session_is_active(&session.status) {
            return Err(ServiceError::TerminalNotFound);
        }

        let raw_token = uuid::Uuid::new_v4().to_string();
        let token_hash = hash_attach_token(&raw_token);
        let expires_at =
            Utc::now() + ChronoDuration::seconds(self.terminal_config.attach_token_ttl_secs as i64);
        {
            let mut state = self.state.lock().await;
            sweep_attach_tokens(&mut state, Utc::now());
            state.attach_tokens.insert(
                token_hash,
                AttachTokenRecord {
                    session_id: session.id.clone(),
                    user_id: user_id.to_owned(),
                    task_id: session.task_id.clone(),
                    expires_at,
                },
            );
        }

        Ok(TerminalAttachTokenResponse {
            attach_token: raw_token.clone(),
            expires_at: expires_at.to_rfc3339_opts(SecondsFormat::Secs, true),
            ws_url: format!(
                "/api/v1/terminals/{}/ws?attach_token={}",
                session.id, raw_token
            ),
            session_id: session.id,
        })
    }

    pub async fn consume_attach_token(
        &self,
        session_id: &str,
        token: &str,
    ) -> Result<String, ServiceError> {
        let token_hash = hash_attach_token(token);
        let (user_id, record_session_id, record_task_id) = {
            let mut state = self.state.lock().await;
            sweep_attach_tokens(&mut state, Utc::now());
            let Some(record) = state.attach_tokens.remove(&token_hash) else {
                return Err(ServiceError::TerminalAttachTokenInvalid);
            };
            if record.session_id != session_id || record.expires_at <= Utc::now() {
                return Err(ServiceError::TerminalAttachTokenInvalid);
            }
            (
                record.user_id.clone(),
                record.session_id.clone(),
                record.task_id.clone(),
            )
        };

        let session = self.load_session(&record_session_id).await?;
        if session.task_id != record_task_id {
            return Err(ServiceError::TerminalAttachTokenInvalid);
        }
        self.publish_session_changed(&session, "attached", None);

        Ok(user_id)
    }

    pub async fn resize_session(
        &self,
        session_id: &str,
        _user_id: &str,
        rows: u16,
        cols: u16,
    ) -> Result<TerminalSessionResponse, ServiceError> {
        validate_terminal_size(rows, cols)?;

        let session = self.load_session(session_id).await?;
        if !terminal_session_is_active(&session.status) {
            return Err(ServiceError::TerminalNotFound);
        }

        if let Some(daemon_id) = session.daemon_id.as_deref() {
            let _: TerminalResizeResult = self
                .daemon_connections
                .send_request(
                    daemon_id,
                    METHOD_TERMINAL_RESIZE,
                    TerminalResizeParams {
                        session_id: session.id.clone(),
                        rows,
                        cols,
                    },
                    DEFAULT_DAEMON_COMMAND_TIMEOUT_SECS,
                )
                .await
                .map_err(|error| map_terminal_daemon_error(error, daemon_id))?;
        } else if let Some(handle) = self.embedded_terminal(session_id).await {
            handle
                .command_tx
                .send(EmbeddedTerminalCommand::Resize { rows, cols })
                .map_err(|_| ServiceError::TerminalNotFound)?;
        } else {
            return Err(ServiceError::TerminalNotFound);
        }

        let now = now_rfc3339();
        let resized = TerminalSessionRepo::update_terminal_session_size(
            &*self.db,
            session_id,
            i64::from(rows),
            i64::from(cols),
            &now,
        )
        .await?;
        self.publish_session_changed(&resized, "resized", None);
        Ok(terminal_session_response(resized))
    }

    pub async fn terminate_session(
        &self,
        session_id: &str,
        _user_id: &str,
        reason: Option<String>,
    ) -> Result<TerminalSessionResponse, ServiceError> {
        self.terminate_session_with_status(
            session_id,
            reason,
            DbTerminalSessionStatus::Terminated,
            "terminated",
        )
        .await
    }

    async fn terminate_session_with_status(
        &self,
        session_id: &str,
        reason: Option<String>,
        status: DbTerminalSessionStatus,
        event_kind: &str,
    ) -> Result<TerminalSessionResponse, ServiceError> {
        let session = self.load_session(session_id).await?;
        if !terminal_session_is_active(&session.status) {
            return Ok(terminal_session_response(session));
        }

        if let Some(daemon_id) = session.daemon_id.as_deref() {
            let _: TerminalTerminateResult = self
                .daemon_connections
                .send_request(
                    daemon_id,
                    METHOD_TERMINAL_TERMINATE,
                    TerminalTerminateParams {
                        session_id: session.id.clone(),
                        reason: reason.clone(),
                    },
                    DEFAULT_DAEMON_COMMAND_TIMEOUT_SECS,
                )
                .await
                .map_err(|error| map_terminal_daemon_error(error, daemon_id))?;
        } else if let Some(handle) = self.embedded_terminal(session_id).await {
            let _ = handle.command_tx.send(EmbeddedTerminalCommand::Terminate {
                reason: reason.clone(),
            });
        }

        let now = now_rfc3339();
        let terminated = TerminalSessionRepo::update_terminal_session_status(
            &*self.db,
            session_id,
            session.version,
            UpdateTerminalSessionStatus {
                status,
                started_at: session.started_at.clone(),
                last_activity_at: Some(now.clone()),
                ended_at: Some(now),
                pid: session.pid,
                exit_code: session.exit_code,
                exit_signal: session.exit_signal.clone(),
                exit_reason: reason.clone(),
            },
        )
        .await?;
        self.fanout_frame(
            session_id,
            TerminalServerFrame::Exit {
                exit_code: None,
                signal: None,
                reason: reason.clone(),
            },
        )
        .await;
        self.publish_session_changed(&terminated, event_kind, reason);
        Ok(terminal_session_response(terminated))
    }

    pub async fn handle_terminal_input(
        &self,
        session_id: &str,
        data_b64: &str,
    ) -> Result<(), ServiceError> {
        let session = self.load_session(session_id).await?;
        if !terminal_session_is_active(&session.status) {
            return Err(ServiceError::TerminalNotFound);
        }
        if let Some(daemon_id) = session.daemon_id.as_deref() {
            let _: TerminalInputResult = self
                .daemon_connections
                .send_request(
                    daemon_id,
                    METHOD_TERMINAL_INPUT,
                    TerminalInputParams {
                        session_id: session.id.clone(),
                        data: data_b64.to_owned(),
                    },
                    DEFAULT_DAEMON_COMMAND_TIMEOUT_SECS,
                )
                .await
                .map_err(|error| map_terminal_daemon_error(error, daemon_id))?;
        } else if let Some(handle) = self.embedded_terminal(session_id).await {
            let decoded = STANDARD
                .decode(data_b64)
                .map_err(|_| ServiceError::TerminalAttachTokenInvalid)?;
            handle
                .command_tx
                .send(EmbeddedTerminalCommand::Input(decoded))
                .map_err(|_| ServiceError::TerminalNotFound)?;
        } else {
            return Err(ServiceError::TerminalNotFound);
        }

        TerminalSessionRepo::touch_terminal_session_activity(&*self.db, session_id, &now_rfc3339())
            .await?;
        Ok(())
    }

    pub async fn handle_daemon_output(&self, notification: TerminalOutputNotification) {
        record_terminal_output(
            Arc::clone(&self.db),
            Arc::clone(&self.state),
            self.terminal_config.reconnect_scrollback_bytes,
            notification.session_id,
            notification.data,
        )
        .await;
    }

    pub async fn handle_daemon_exited(&self, notification: TerminalExitedNotification) {
        handle_terminal_exited(
            Arc::clone(&self.db),
            Arc::clone(&self.event_bus),
            Arc::clone(&self.state),
            Arc::clone(&self.terminal_activity),
            notification.session_id,
            notification.exit_code,
            notification.signal,
            notification.reason,
            notification.ts,
        )
        .await;
    }

    pub async fn attach_client(
        &self,
        session_id: &str,
    ) -> mpsc::UnboundedReceiver<TerminalServerFrame> {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut state = self.state.lock().await;
        if let Some(scrollback) = state.scrollback.get(session_id) {
            if !scrollback.is_empty() {
                let bytes: Vec<u8> = scrollback.iter().copied().collect();
                let _ = tx.send(TerminalServerFrame::Output {
                    data: STANDARD.encode(bytes),
                });
            }
        }
        state
            .attached_clients
            .entry(session_id.to_owned())
            .or_default()
            .push(tx);
        rx
    }

    pub async fn detach_closed_clients(&self, session_id: &str) {
        let mut state = self.state.lock().await;
        let remove_scrollback = match state.attached_clients.get_mut(session_id) {
            Some(clients) => {
                clients.retain(|sender| !sender.is_closed());
                clients.is_empty()
            }
            None => true,
        };
        if remove_scrollback {
            state.attached_clients.remove(session_id);
            state.scrollback.remove(session_id);
        }
    }

    pub async fn workspace_has_active_terminal(&self, workspace_id: &str) -> bool {
        self.terminal_activity
            .workspace_has_active_terminal(workspace_id)
            .await
    }

    pub async fn cleanup_workspace_terminals(
        &self,
        workspace_id: &str,
    ) -> Result<(), ServiceError> {
        let sessions = TerminalSessionRepo::list_running_terminal_sessions_for_workspace(
            &*self.db,
            workspace_id,
        )
        .await?;
        for session in sessions {
            self.terminate_session_with_status(
                &session.id,
                Some(TERMINAL_REASON_WORKSPACE_CLEANUP.to_owned()),
                DbTerminalSessionStatus::CleanupTerminated,
                "cleanup_terminated",
            )
            .await?;
        }
        Ok(())
    }

    async fn require_terminal_workspace(
        &self,
        task_id: &str,
    ) -> Result<(Task, Workspace), ServiceError> {
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        let workspace = WorkspaceRepo::get_by_task_id(&*self.db, task_id)
            .await?
            .ok_or(ServiceError::TerminalWorkspaceNotReady)?;
        if !self.workspace_ready_for_terminal(&task, &workspace).await? {
            return Err(ServiceError::TerminalWorkspaceNotReady);
        }
        Ok((task, workspace))
    }

    async fn workspace_ready_for_terminal(
        &self,
        task: &Task,
        workspace: &Workspace,
    ) -> Result<bool, ServiceError> {
        if workspace.status != WorkspaceStatus::Ready || workspace.worktree_path.trim().is_empty() {
            return Ok(false);
        }

        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::General),
        );
        let state_kind = workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
            .map(|state| &state.kind);

        // Decision: the spec says "ready/active"; current workflows model ready work
        // as the Initial state kind and active work as the Active state kind.
        Ok(matches!(
            state_kind,
            Some(StateKind::Initial | StateKind::Active)
        ))
    }

    async fn validate_workspace_path(&self, worktree_path: &str) -> Result<(), ServiceError> {
        let worktree_path = Path::new(worktree_path);
        let workspace_root = tokio::fs::canonicalize(&self.workspace_root)
            .await
            .map_err(|_| ServiceError::TerminalPathGuardrail)?;
        let worktree_path = tokio::fs::canonicalize(worktree_path)
            .await
            .map_err(|_| ServiceError::TerminalPathGuardrail)?;
        if !worktree_path.starts_with(workspace_root) {
            return Err(ServiceError::TerminalPathGuardrail);
        }
        Ok(())
    }

    async fn resolve_terminal_daemon_id(
        &self,
        task: &Task,
        _workspace: &Workspace,
    ) -> Result<Option<String>, ServiceError> {
        if task.assignee_type.as_deref() == Some("agent") {
            return self.agent_daemon_id(task.assignee_id.as_deref()).await;
        }

        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::General),
        );
        let Some(role_name) = workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
            .and_then(effective_role)
        else {
            return Ok(None);
        };
        let Some(assignment) =
            TaskRoleAssignmentRepo::get_by_task_and_role(&*self.db, &task.id, role_name).await?
        else {
            return Ok(None);
        };
        if assignment.assignee_type != Some(AssigneeKind::Agent) {
            return Ok(None);
        }

        self.agent_daemon_id(assignment.assignee_id.as_deref())
            .await
    }

    async fn agent_daemon_id(
        &self,
        agent_id: Option<&str>,
    ) -> Result<Option<String>, ServiceError> {
        let Some(agent_id) = agent_id else {
            return Ok(None);
        };
        let agent = AgentRepo::get_by_id(&*self.db, agent_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent", agent_id.to_owned()))?;
        Ok(agent.daemon_id)
    }

    async fn start_terminal_process(
        &self,
        session: &TerminalSession,
        workspace_path: &str,
        rows: u16,
        cols: u16,
    ) -> Result<TerminalStartResult, ServiceError> {
        if let Some(daemon_id) = session.daemon_id.as_deref() {
            return self
                .daemon_connections
                .send_request(
                    daemon_id,
                    METHOD_TERMINAL_START,
                    TerminalStartParams {
                        session_id: session.id.clone(),
                        workspace_path: workspace_path.to_owned(),
                        rows,
                        cols,
                        shell: None,
                        env: None,
                        idle_timeout_secs: self.terminal_config.idle_timeout_secs,
                        max_lifetime_secs: self.terminal_config.max_lifetime_secs,
                    },
                    DEFAULT_DAEMON_COMMAND_TIMEOUT_SECS,
                )
                .await
                .map_err(|error| map_terminal_daemon_error(error, daemon_id));
        }

        self.start_embedded_terminal(
            session.id.clone(),
            PathBuf::from(workspace_path),
            rows,
            cols,
        )
        .await
    }

    async fn start_embedded_terminal(
        &self,
        session_id: String,
        workspace_path: PathBuf,
        rows: u16,
        cols: u16,
    ) -> Result<TerminalStartResult, ServiceError> {
        let pty_system = native_pty_system();
        let pair = pty_system.openpty(pty_size(rows, cols)).map_err(|error| {
            ServiceError::invalid_operation(format!("failed to open terminal PTY: {error}"))
        })?;
        let reader = pair.master.try_clone_reader().map_err(|error| {
            ServiceError::invalid_operation(format!("failed to clone terminal PTY reader: {error}"))
        })?;
        let writer = pair.master.take_writer().map_err(|error| {
            ServiceError::invalid_operation(format!("failed to take terminal PTY writer: {error}"))
        })?;
        let mut command = command_builder(None);
        command.cwd(workspace_path.as_os_str());
        let child = pair.slave.spawn_command(command).map_err(|error| {
            ServiceError::invalid_operation(format!("failed to start terminal shell: {error}"))
        })?;

        let pid = child.process_id();
        let child: Box<dyn Child + Send> = child;
        let child = Arc::new(StdMutex::new(child));
        let last_activity = Arc::new(AtomicU64::new(unix_timestamp_secs()));
        let started_at = Instant::now();
        let terminate_reason = Arc::new(StdMutex::new(None::<String>));
        let (command_tx, command_rx) = mpsc::unbounded_channel::<EmbeddedTerminalCommand>();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel::<String>();

        {
            let mut state = self.state.lock().await;
            state.embedded_terminals.insert(
                session_id.clone(),
                EmbeddedTerminalHandle {
                    command_tx: command_tx.clone(),
                },
            );
        }

        spawn_embedded_reader(session_id.clone(), reader, output_tx);
        spawn_embedded_control(
            session_id.clone(),
            pair.master,
            writer,
            command_rx,
            Arc::clone(&child),
            Arc::clone(&last_activity),
            Arc::clone(&terminate_reason),
        );

        let output_db = Arc::clone(&self.db);
        let output_state = Arc::clone(&self.state);
        let output_session_id = session_id.clone();
        let reconnect_scrollback_bytes = self.terminal_config.reconnect_scrollback_bytes;
        tokio::spawn(async move {
            while let Some(data) = output_rx.recv().await {
                record_terminal_output(
                    Arc::clone(&output_db),
                    Arc::clone(&output_state),
                    reconnect_scrollback_bytes,
                    output_session_id.clone(),
                    data,
                )
                .await;
            }
        });

        let db = Arc::clone(&self.db);
        let event_bus = Arc::clone(&self.event_bus);
        let state = Arc::clone(&self.state);
        let terminal_activity = Arc::clone(&self.terminal_activity);
        let wait_session_id = session_id.clone();
        let wait_child = Arc::clone(&child);
        let wait_reason = Arc::clone(&terminate_reason);
        tokio::spawn(async move {
            wait_for_embedded_child_exit(
                db,
                event_bus,
                state,
                terminal_activity,
                wait_session_id,
                wait_child,
                wait_reason,
            )
            .await;
        });

        let watchdog_command_tx = command_tx;
        let watchdog_session_id = session_id.clone();
        let watchdog_last_activity = Arc::clone(&last_activity);
        let idle_timeout_secs = self.terminal_config.idle_timeout_secs;
        let max_lifetime_secs = self.terminal_config.max_lifetime_secs;
        tokio::spawn(async move {
            embedded_watchdog(
                watchdog_session_id,
                watchdog_command_tx,
                watchdog_last_activity,
                started_at,
                idle_timeout_secs,
                max_lifetime_secs,
            )
            .await;
        });

        Ok(TerminalStartResult {
            session_id,
            pid,
            started_at: now_rfc3339(),
        })
    }

    async fn active_terminal_count(&self, workspace_id: &str) -> u32 {
        self.terminal_activity.count(workspace_id).await
    }

    async fn embedded_terminal(&self, session_id: &str) -> Option<EmbeddedTerminalHandle> {
        self.state
            .lock()
            .await
            .embedded_terminals
            .get(session_id)
            .cloned()
    }

    async fn store_workspace_lock_guard(
        &self,
        session_id: &str,
        workspace_id: &str,
        guard: OwnedMutexGuard<()>,
    ) {
        self.state.lock().await.workspace_lock_guards.insert(
            session_id.to_owned(),
            TerminalWorkspaceLockGuard {
                workspace_id: workspace_id.to_owned(),
                _guard: guard,
            },
        );
    }

    async fn release_session_resources(&self, session_id: &str, workspace_id: &str) {
        release_session_resources(
            Arc::clone(&self.state),
            Arc::clone(&self.terminal_activity),
            session_id,
            workspace_id,
        )
        .await;
    }

    async fn load_session(&self, session_id: &str) -> Result<TerminalSession, ServiceError> {
        TerminalSessionRepo::get_terminal_session(&*self.db, session_id)
            .await?
            .ok_or(ServiceError::TerminalNotFound)
    }

    async fn fanout_frame(&self, session_id: &str, frame: TerminalServerFrame) {
        fanout_frame(Arc::clone(&self.state), session_id.to_owned(), frame).await;
    }

    fn publish_session_changed(
        &self,
        session: &TerminalSession,
        kind: &str,
        reason: Option<String>,
    ) {
        publish_session_changed(&self.event_bus, session, kind, reason);
    }
}

#[async_trait]
impl DaemonTerminalEventHandler for TerminalService {
    async fn handle_terminal_output(
        &self,
        _daemon_id: &str,
        notification: TerminalOutputNotification,
    ) -> Result<(), ServiceError> {
        self.handle_daemon_output(notification).await;
        Ok(())
    }

    async fn handle_terminal_exited(
        &self,
        _daemon_id: &str,
        notification: TerminalExitedNotification,
    ) -> Result<(), ServiceError> {
        self.handle_daemon_exited(notification).await;
        Ok(())
    }
}

#[async_trait]
impl WorkspaceCleanupObserver for TerminalService {
    async fn cleanup_workspace_terminals(&self, workspace_id: &str) -> Result<(), ServiceError> {
        TerminalService::cleanup_workspace_terminals(self, workspace_id).await
    }
}

fn spawn_embedded_reader(
    session_id: String,
    mut reader: Box<dyn Read + Send>,
    output_tx: mpsc::UnboundedSender<String>,
) {
    tokio::task::spawn_blocking(move || {
        let mut buffer = [0_u8; 8192];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => return,
                Ok(count) => {
                    if output_tx.send(STANDARD.encode(&buffer[..count])).is_err() {
                        return;
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, session_id = %session_id, "embedded terminal PTY reader stopped");
                    return;
                }
            }
        }
    });
}

fn spawn_embedded_control(
    session_id: String,
    master: Box<dyn MasterPty + Send>,
    mut writer: Box<dyn Write + Send>,
    mut command_rx: mpsc::UnboundedReceiver<EmbeddedTerminalCommand>,
    child: SharedChild,
    last_activity: Arc<AtomicU64>,
    terminate_reason: Arc<StdMutex<Option<String>>>,
) {
    tokio::task::spawn_blocking(move || {
        while let Some(command) = command_rx.blocking_recv() {
            match command {
                EmbeddedTerminalCommand::Input(bytes) => {
                    if let Err(error) = writer.write_all(&bytes) {
                        tracing::warn!(%error, session_id = %session_id, "embedded terminal input write failed");
                        return;
                    }
                    last_activity.store(unix_timestamp_secs(), Ordering::Relaxed);
                }
                EmbeddedTerminalCommand::Resize { rows, cols } => {
                    if let Err(error) = master.resize(pty_size(rows, cols)) {
                        tracing::warn!(%error, session_id = %session_id, "embedded terminal resize failed");
                    } else {
                        last_activity.store(unix_timestamp_secs(), Ordering::Relaxed);
                    }
                }
                EmbeddedTerminalCommand::Terminate { reason } => {
                    if let Ok(mut stored_reason) = terminate_reason.lock() {
                        *stored_reason = reason;
                    }
                    match child.lock() {
                        Ok(mut child) => {
                            if let Err(error) = child.kill() {
                                tracing::warn!(%error, session_id = %session_id, "failed to kill embedded terminal child");
                            }
                        }
                        Err(error) => {
                            tracing::warn!(%error, session_id = %session_id, "embedded terminal child lock poisoned during termination");
                        }
                    }
                    return;
                }
            }
        }
    });
}

#[allow(clippy::too_many_arguments)]
async fn wait_for_embedded_child_exit(
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
    state: Arc<Mutex<TerminalState>>,
    terminal_activity: Arc<TerminalActivityTracker>,
    session_id: String,
    child: SharedChild,
    terminate_reason: Arc<StdMutex<Option<String>>>,
) {
    let mut interval = tokio_time::interval(EMBEDDED_EXIT_POLL_INTERVAL);
    loop {
        interval.tick().await;
        let status = match child.lock() {
            Ok(mut child) => match child.try_wait() {
                Ok(status) => status,
                Err(error) => {
                    tracing::warn!(%error, session_id = %session_id, "failed to poll embedded terminal child status");
                    None
                }
            },
            Err(error) => {
                tracing::warn!(%error, session_id = %session_id, "embedded terminal child lock poisoned");
                None
            }
        };

        if let Some(status) = status {
            let reason = terminate_reason
                .lock()
                .ok()
                .and_then(|reason| reason.clone());
            handle_terminal_exited(
                db,
                event_bus,
                state,
                terminal_activity,
                session_id,
                i32::try_from(status.exit_code()).ok(),
                status.signal().map(ToOwned::to_owned),
                reason,
                now_rfc3339(),
            )
            .await;
            return;
        }
    }
}

async fn embedded_watchdog(
    session_id: String,
    command_tx: mpsc::UnboundedSender<EmbeddedTerminalCommand>,
    last_activity: Arc<AtomicU64>,
    started_at: Instant,
    idle_timeout_secs: u64,
    max_lifetime_secs: u64,
) {
    let mut interval =
        tokio_time::interval(watchdog_interval(idle_timeout_secs, max_lifetime_secs));
    loop {
        interval.tick().await;
        let now = unix_timestamp_secs();
        let last = last_activity.load(Ordering::Relaxed);
        let reason = if idle_timeout_secs > 0 && now.saturating_sub(last) >= idle_timeout_secs {
            Some(TERMINAL_REASON_IDLE_TIMEOUT)
        } else if max_lifetime_secs > 0 && started_at.elapsed().as_secs() >= max_lifetime_secs {
            Some(TERMINAL_REASON_MAX_LIFETIME)
        } else {
            None
        };

        if let Some(reason) = reason {
            let _ = command_tx.send(EmbeddedTerminalCommand::Terminate {
                reason: Some(reason.to_owned()),
            });
            tracing::info!(session_id = %session_id, reason, "embedded terminal watchdog terminated session");
            return;
        }

        if command_tx.is_closed() {
            return;
        }
    }
}

async fn record_terminal_output(
    db: Arc<SqliteDb>,
    state: Arc<Mutex<TerminalState>>,
    reconnect_scrollback_bytes: usize,
    session_id: String,
    data_b64: String,
) {
    match STANDARD.decode(&data_b64) {
        Ok(decoded) => {
            let mut state_guard = state.lock().await;
            let has_attached_client = state_guard
                .attached_clients
                .get(&session_id)
                .is_some_and(|clients| clients.iter().any(|sender| !sender.is_closed()));
            if has_attached_client {
                let scrollback = state_guard
                    .scrollback
                    .entry(session_id.clone())
                    .or_default();
                scrollback.extend(decoded);
                while scrollback.len() > reconnect_scrollback_bytes {
                    scrollback.pop_front();
                }
            } else {
                state_guard.scrollback.remove(&session_id);
            }
        }
        Err(error) => {
            tracing::warn!(
                %error,
                session_id = %session_id,
                "terminal output notification was not valid base64"
            );
        }
    }

    fanout_frame(
        Arc::clone(&state),
        session_id.clone(),
        TerminalServerFrame::Output { data: data_b64 },
    )
    .await;

    if let Err(error) =
        TerminalSessionRepo::touch_terminal_session_activity(&*db, &session_id, &now_rfc3339())
            .await
    {
        tracing::warn!(%error, session_id = %session_id, "failed to touch terminal activity");
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_terminal_exited(
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
    state: Arc<Mutex<TerminalState>>,
    terminal_activity: Arc<TerminalActivityTracker>,
    session_id: String,
    exit_code: Option<i32>,
    signal: Option<String>,
    reason: Option<String>,
    ts: String,
) {
    let session = match TerminalSessionRepo::get_terminal_session(&*db, &session_id).await {
        Ok(Some(session)) => session,
        Ok(None) => {
            tracing::warn!(session_id = %session_id, "terminal exit for unknown session");
            release_session_resources(
                Arc::clone(&state),
                Arc::clone(&terminal_activity),
                &session_id,
                "",
            )
            .await;
            return;
        }
        Err(error) => {
            tracing::warn!(%error, session_id = %session_id, "failed to load exited terminal");
            return;
        }
    };

    release_session_resources(
        Arc::clone(&state),
        terminal_activity,
        &session_id,
        &session.workspace_id,
    )
    .await;

    if terminal_session_is_active(&session.status) {
        let (status, event_kind) = exited_status_and_event_kind(reason.as_deref());
        match TerminalSessionRepo::update_terminal_session_status(
            &*db,
            &session_id,
            session.version,
            UpdateTerminalSessionStatus {
                status,
                started_at: session.started_at.clone(),
                last_activity_at: Some(ts.clone()),
                ended_at: Some(ts),
                pid: session.pid,
                exit_code: exit_code.map(i64::from),
                exit_signal: signal.clone(),
                exit_reason: reason.clone(),
            },
        )
        .await
        {
            Ok(updated) => {
                publish_session_changed(&event_bus, &updated, event_kind, reason.clone());
            }
            Err(error) => {
                tracing::warn!(%error, session_id = %session_id, "failed to mark terminal exited");
            }
        }
    }

    fanout_frame(
        state,
        session_id,
        TerminalServerFrame::Exit {
            exit_code,
            signal,
            reason,
        },
    )
    .await;
}

async fn release_session_resources(
    state: Arc<Mutex<TerminalState>>,
    terminal_activity: Arc<TerminalActivityTracker>,
    session_id: &str,
    workspace_id: &str,
) {
    let released_workspace_id = {
        let mut state_guard = state.lock().await;
        state_guard.embedded_terminals.remove(session_id);
        state_guard
            .workspace_lock_guards
            .remove(session_id)
            .map(|guard| guard.workspace_id)
    };
    if let Some(released_workspace_id) = released_workspace_id {
        terminal_activity
            .release(if released_workspace_id.is_empty() {
                workspace_id
            } else {
                &released_workspace_id
            })
            .await;
    }
}

async fn fanout_frame(
    state: Arc<Mutex<TerminalState>>,
    session_id: String,
    frame: TerminalServerFrame,
) {
    let mut state = state.lock().await;
    if let Some(clients) = state.attached_clients.get_mut(&session_id) {
        clients.retain(|sender| sender.send(frame.clone()).is_ok());
    }
}

fn publish_session_changed(
    event_bus: &EventBus,
    session: &TerminalSession,
    kind: &str,
    reason: Option<String>,
) {
    event_bus.publish(ForgeEvent {
        event_type: TERMINAL_SESSION_CHANGED_EVENT.to_owned(),
        entity_id: session.id.clone(),
        timestamp: event_timestamp(),
        context: EventContext::TaskTerminalSessionChanged {
            task_id: session.task_id.clone(),
            session_id: session.id.clone(),
            workspace_id: session.workspace_id.clone(),
            kind: kind.to_owned(),
            status: session.status.to_string(),
            reason,
        },
    });
}

fn terminal_session_response(session: TerminalSession) -> TerminalSessionResponse {
    TerminalSessionResponse {
        id: session.id,
        task_id: session.task_id,
        workspace_id: session.workspace_id,
        daemon_id: session.daemon_id,
        status: api_terminal_status(session.status),
        rows: u16::try_from(session.rows).unwrap_or(u16::MAX),
        cols: u16::try_from(session.cols).unwrap_or(u16::MAX),
        exit_code: session
            .exit_code
            .and_then(|exit_code| i32::try_from(exit_code).ok()),
        exit_signal: session.exit_signal,
        exit_reason: session.exit_reason,
        created_at: session.created_at,
        started_at: session.started_at,
        last_activity_at: session.last_activity_at,
        ended_at: session.ended_at,
        created_by_user_id: session.created_by_user_id,
    }
}

fn api_terminal_status(status: DbTerminalSessionStatus) -> ApiTerminalSessionStatus {
    match status {
        DbTerminalSessionStatus::Starting => ApiTerminalSessionStatus::Starting,
        DbTerminalSessionStatus::Running => ApiTerminalSessionStatus::Running,
        DbTerminalSessionStatus::Exited => ApiTerminalSessionStatus::Exited,
        DbTerminalSessionStatus::Terminated => ApiTerminalSessionStatus::Terminated,
        DbTerminalSessionStatus::TimedOut => ApiTerminalSessionStatus::TimedOut,
        DbTerminalSessionStatus::Orphaned => ApiTerminalSessionStatus::Orphaned,
        DbTerminalSessionStatus::CleanupTerminated => ApiTerminalSessionStatus::CleanupTerminated,
    }
}

fn terminal_session_is_active(status: &DbTerminalSessionStatus) -> bool {
    matches!(
        status,
        DbTerminalSessionStatus::Starting | DbTerminalSessionStatus::Running
    )
}

fn exited_status_and_event_kind(reason: Option<&str>) -> (DbTerminalSessionStatus, &'static str) {
    match reason {
        Some(TERMINAL_REASON_IDLE_TIMEOUT | TERMINAL_REASON_MAX_LIFETIME) => {
            (DbTerminalSessionStatus::TimedOut, "timed_out")
        }
        _ => (DbTerminalSessionStatus::Exited, "exited"),
    }
}

fn sweep_attach_tokens(state: &mut TerminalState, now: DateTime<Utc>) {
    state
        .attach_tokens
        .retain(|_, record| record.expires_at > now);
}

fn command_builder(shell: Option<String>) -> CommandBuilder {
    let shell = shell.unwrap_or_else(default_shell);
    if shell.split_whitespace().nth(1).is_some() {
        command_builder_for_command_line(shell)
    } else {
        CommandBuilder::new(shell)
    }
}

#[cfg(unix)]
fn default_shell() -> String {
    std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_owned())
}

#[cfg(windows)]
fn default_shell() -> String {
    "cmd.exe".to_owned()
}

#[cfg(unix)]
fn command_builder_for_command_line(command_line: String) -> CommandBuilder {
    let mut command = CommandBuilder::new("/bin/sh");
    command.arg("-c");
    command.arg(command_line);
    command
}

#[cfg(windows)]
fn command_builder_for_command_line(command_line: String) -> CommandBuilder {
    let mut command = CommandBuilder::new("cmd.exe");
    command.arg("/C");
    command.arg(command_line);
    command
}

fn pty_size(rows: u16, cols: u16) -> PtySize {
    PtySize {
        rows,
        cols,
        pixel_width: 0,
        pixel_height: 0,
    }
}

fn validate_terminal_size(rows: u16, cols: u16) -> Result<(), ServiceError> {
    if rows < MIN_TERMINAL_DIMENSION || cols < MIN_TERMINAL_DIMENSION {
        return Err(ServiceError::terminal_invalid_input(format!(
            "terminal rows and cols must each be at least {MIN_TERMINAL_DIMENSION}"
        )));
    }
    Ok(())
}

fn watchdog_interval(idle_timeout_secs: u64, max_lifetime_secs: u64) -> StdDuration {
    let shortest = [idle_timeout_secs, max_lifetime_secs]
        .into_iter()
        .filter(|value| *value > 0)
        .min()
        .unwrap_or(EMBEDDED_WATCHDOG_MAX_INTERVAL.as_secs());
    StdDuration::from_secs(shortest.clamp(1, EMBEDDED_WATCHDOG_MAX_INTERVAL.as_secs()))
}

fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn hash_attach_token(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

fn map_terminal_daemon_error(error: ServiceError, daemon_id: &str) -> ServiceError {
    match error {
        ServiceError::DaemonUnavailable { .. } | ServiceError::DaemonTimeout { .. } => {
            ServiceError::TerminalDaemonUnavailable {
                daemon_id: daemon_id.to_owned(),
            }
        }
        error => error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{
        create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, CreateProject, CreateRepo,
        CreateTask, CreateWorkspace, ProjectRepo, RepoRepo, TaskRepo, TerminalSessionRepo,
        UpdateTerminalSessionStatus, UserRepo, WorkMode, WorkspaceRepo, WorkspaceStatus,
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
        let service = TerminalService::new(
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
                repo_id: Some(repo_id.clone()),
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
}
