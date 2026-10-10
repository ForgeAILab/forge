use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use ::time::{format_description::well_known::Rfc3339, OffsetDateTime};
use anyhow::Result;
use api_types::{
    DaemonErrorPayload, DaemonFrame, ExecutionCancelParams, ExecutionCancelResult,
    ExecutionStartParams, ExecutionStartResult, ExecutionTerminalNotification, FsBranchesParams,
    FsListParams, JournalAckParams, JournalAckResult, RemoteExecutionFailureClass,
    RemoteResolvedCandidate, RemoteRouteAttempt, RemoteUsageReport, UsageTelemetryState,
    DAEMON_CAPABILITY_JOURNAL_ACK, DAEMON_CAPABILITY_USAGE_REPORTS, DAEMON_PROTOCOL_REVISION,
    INVALID_FRAME, METHOD_DAEMON_HANDSHAKE, METHOD_EXECUTION_CANCEL, METHOD_EXECUTION_LOG,
    METHOD_EXECUTION_START, METHOD_EXECUTION_TERMINAL, METHOD_FS_BRANCHES, METHOD_FS_LIST,
    METHOD_JOURNAL_ACK, METHOD_TERMINAL_INPUT, METHOD_TERMINAL_RESIZE, METHOD_TERMINAL_START,
    METHOD_TERMINAL_TERMINATE, TERMINAL_REPORT_CONFLICT, UNSUPPORTED_METHOD,
};
use executors::{
    ExecutionContext, ExecutionFailureClass, ExecutionOutcome, ExecutionResult, ExecutorError,
    FallbackExecutor, LogEntry, TaskExecutor,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use tokio::sync::{mpsc, watch};

use crate::{
    daemon_fs,
    daemon_link::{run_dispatch_loop, run_with_reconnect, DaemonClient},
    daemon_outbox,
    daemon_workspace::DaemonWorkspaceBackend,
};

pub use crate::daemon_persistence::{DaemonJournal, JournalEntry, MAX_TERMINAL_REPORT_SIZE};

const TERMINAL_UNAVAILABLE: &str = "terminal_unavailable";
const EXECUTION_ERROR: &str = "execution_error";

/// A finished execution's terminal notification is only queued for the command
/// stream when its guard drops, so a report snapshot taken right after could
/// omit the id before the server has processed the completion — and the server
/// would reconcile the execution as daemon_disconnected. Finished ids therefore
/// stay in reports for this long after the guard drops.
const FINISHED_EXECUTION_LINGER: Duration = Duration::from_secs(120);

#[derive(Clone)]
pub struct ActiveExecutionTracker {
    inner: Arc<Mutex<TrackerInner>>,
    finished_linger: Duration,
}

#[derive(Default)]
struct TrackerInner {
    active: HashSet<String>,
    recently_finished: HashMap<String, Instant>,
}

impl Default for ActiveExecutionTracker {
    fn default() -> Self {
        Self::with_finished_linger(FINISHED_EXECUTION_LINGER)
    }
}

impl ActiveExecutionTracker {
    pub fn running_ids(&self) -> Vec<String> {
        let mut ids: Vec<_> = self
            .inner
            .lock()
            .expect("active execution tracker lock")
            .active
            .iter()
            .cloned()
            .collect();
        ids.sort();
        ids
    }

    pub fn with_finished_linger(finished_linger: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(TrackerInner::default())),
            finished_linger,
        }
    }

    pub fn track(&self, execution_id: String) -> ActiveExecutionGuard {
        {
            let mut inner = self.inner.lock().expect("active execution tracker lock");
            inner.recently_finished.remove(&execution_id);
            inner.active.insert(execution_id.clone());
        }
        ActiveExecutionGuard {
            tracker: self.clone(),
            execution_id,
        }
    }

    pub fn active_ids(&self) -> Vec<String> {
        let now = Instant::now();
        let linger = self.finished_linger;
        let mut inner = self.inner.lock().expect("active execution tracker lock");
        inner
            .recently_finished
            .retain(|_, finished_at| now.duration_since(*finished_at) < linger);
        let mut ids: Vec<String> = inner
            .active
            .iter()
            .chain(inner.recently_finished.keys())
            .cloned()
            .collect();
        ids.sort();
        ids.dedup();
        ids
    }
}

pub struct ActiveExecutionGuard {
    tracker: ActiveExecutionTracker,
    execution_id: String,
}

impl Drop for ActiveExecutionGuard {
    fn drop(&mut self) {
        let mut inner = self
            .tracker
            .inner
            .lock()
            .expect("active execution tracker lock");
        if inner.active.remove(&self.execution_id) {
            inner
                .recently_finished
                .insert(self.execution_id.clone(), Instant::now());
        }
    }
}

type CommandResult<T> = std::result::Result<T, DaemonErrorPayload>;

#[derive(Clone)]
struct RuntimeOutbound(Arc<Mutex<mpsc::UnboundedSender<DaemonFrame>>>);

impl RuntimeOutbound {
    fn send(
        &self,
        frame: DaemonFrame,
    ) -> std::result::Result<(), mpsc::error::SendError<DaemonFrame>> {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).send(frame)
    }
}

fn adapter_capability_facts(
    registry: &executors::AdapterRegistry,
) -> BTreeMap<String, api_types::ExecutorAdapterCapabilityFacts> {
    use executors::{AvailabilityStatus, ExecutorKind};

    let mut facts = BTreeMap::new();
    for kind in registry.kinds() {
        let Some(adapter) = registry.get(&kind) else {
            continue;
        };
        if matches!(
            adapter.check_availability().status,
            AvailabilityStatus::NotFound
        ) {
            continue;
        }
        // Facts describe the installed adapter path we actually use. No
        // capability is inferred for an absent or unknown executor.
        facts.insert(
            kind.to_string(),
            api_types::ExecutorAdapterCapabilityFacts {
                structured_events: matches!(
                    kind,
                    ExecutorKind::Codex
                        | ExecutorKind::ClaudeCode
                        | ExecutorKind::Cursor
                        | ExecutorKind::Opencode
                        | ExecutorKind::Gemini
                        | ExecutorKind::Smith
                ),
                usage: matches!(
                    kind,
                    ExecutorKind::Codex | ExecutorKind::ClaudeCode | ExecutorKind::Smith
                ),
                resume: matches!(
                    kind,
                    ExecutorKind::Codex
                        | ExecutorKind::ClaudeCode
                        | ExecutorKind::Cursor
                        | ExecutorKind::Opencode
                        | ExecutorKind::Gemini
                        | ExecutorKind::Smith
                ),
                cancel_ack: true,
                terminal_observed: true,
            },
        );
    }
    facts
}

pub async fn run_command_stream(
    client: Arc<DaemonClient>,
    workspace_root: PathBuf,
    shutdown: watch::Receiver<bool>,
    active_executions: ActiveExecutionTracker,
    run_policy: api_types::WorkspaceRunPolicy,
) -> Result<()> {
    let (initial_tx, _initial_rx) = mpsc::unbounded_channel();
    let daemon_id = client
        .daemon_id
        .clone()
        .ok_or_else(|| anyhow::anyhow!("daemon credentials are missing"))?;
    let runtime = DaemonRuntime::new_owned(
        initial_tx,
        workspace_root,
        active_executions,
        daemon_id,
        run_policy,
    )?;
    run_with_reconnect(client, move |stream| {
        let runtime = Arc::clone(&runtime);
        let shutdown = shutdown.clone();
        async move {
            let (responses_tx, responses_rx) = mpsc::unbounded_channel();
            runtime.attach(responses_tx.clone());
            let handler = {
                let runtime = Arc::clone(&runtime);
                move |frame| {
                    let runtime = Arc::clone(&runtime);
                    async move { runtime.handle_request(frame).await }
                }
            };
            run_dispatch_loop(stream, handler, shutdown, responses_tx, responses_rx).await
        }
    })
    .await
}

pub struct DaemonRuntime {
    workspace_root: PathBuf,
    outbound: RuntimeOutbound,
    executor: Arc<FallbackExecutor>,
    active_executions: ActiveExecutionTracker,
    journal: Arc<DaemonJournal>,
    workspace: Option<DaemonWorkspaceBackend>,
    registry: Arc<executors::AdapterRegistry>,
    run_policy: api_types::WorkspaceRunPolicy,
}

impl DaemonRuntime {
    pub fn new(outbound: mpsc::UnboundedSender<DaemonFrame>, workspace_root: PathBuf) -> Arc<Self> {
        Self::new_with_tracker(outbound, workspace_root, ActiveExecutionTracker::default())
    }

    pub fn new_with_tracker(
        outbound: mpsc::UnboundedSender<DaemonFrame>,
        workspace_root: PathBuf,
        active_executions: ActiveExecutionTracker,
    ) -> Arc<Self> {
        Self::build(
            outbound,
            workspace_root,
            active_executions,
            None,
            crate::daemon_config::DaemonConfig::default().run_policy(),
            Arc::new(cli_adapters::default_registry()),
        )
        .expect("initialize daemon journal")
    }

    pub fn new_owned(
        outbound: mpsc::UnboundedSender<DaemonFrame>,
        workspace_root: PathBuf,
        active_executions: ActiveExecutionTracker,
        daemon_id: String,
        run_policy: api_types::WorkspaceRunPolicy,
    ) -> Result<Arc<Self>> {
        Self::build(
            outbound,
            workspace_root,
            active_executions,
            Some(daemon_id),
            run_policy,
            Arc::new(cli_adapters::default_registry()),
        )
    }

    fn build(
        outbound: mpsc::UnboundedSender<DaemonFrame>,
        workspace_root: PathBuf,
        active_executions: ActiveExecutionTracker,
        daemon_id: Option<String>,
        run_policy: api_types::WorkspaceRunPolicy,
        registry: Arc<executors::AdapterRegistry>,
    ) -> Result<Arc<Self>> {
        let journal = Arc::new(DaemonJournal::new(&workspace_root));
        journal.initialize()?;
        let workspace = daemon_id
            .map(|id| {
                DaemonWorkspaceBackend::new(
                    workspace_root.clone(),
                    id,
                    run_policy.clone(),
                    Arc::clone(&journal),
                )
            })
            .transpose()?;
        let runtime = Arc::new(Self {
            workspace_root,
            outbound: RuntimeOutbound(Arc::new(Mutex::new(outbound))),
            executor: Arc::new(FallbackExecutor::new(Arc::clone(&registry))),
            active_executions,
            journal,
            workspace,
            registry,
            run_policy,
        });
        runtime.announce_protocol();
        runtime.replay_pending_journal();
        runtime.spawn_workspace_gc();
        Ok(runtime)
    }

    /// Sweep the workspace root on a timer for as long as this runtime
    /// lives. The first pass waits one interval: start-up already removed
    /// what a previous process left, and nothing is quarantined on sight.
    fn spawn_workspace_gc(self: &Arc<Self>) {
        if self.workspace.is_none() {
            return;
        }
        // Constructed outside a Tokio runtime (some tools do): no timer.
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let runtime = Arc::downgrade(self);
        handle.spawn(async move {
            use crate::daemon_workspace::gc::{
                GC_DISK_CHECK_INTERVAL, GC_INTERVAL, GC_PRESSURE_INTERVAL,
            };
            let mut since_sweep = Duration::ZERO;
            loop {
                tokio::time::sleep(GC_DISK_CHECK_INTERVAL).await;
                since_sweep += GC_DISK_CHECK_INTERVAL;
                let Some(runtime) = runtime.upgrade() else {
                    return;
                };
                let Some(workspace) = &runtime.workspace else {
                    return;
                };
                // Short of disk: the sweep does not wait for its timer, so
                // the next report already carries what it reclaimed.
                let due = since_sweep >= GC_INTERVAL
                    || (since_sweep >= GC_PRESSURE_INTERVAL && workspace.disk_is_short());
                if due {
                    since_sweep = Duration::ZERO;
                    workspace.gc_sweep(&runtime.active_execution_ids()).await;
                }
            }
        });
    }

    /// Keep the executor, handle registry and write locks alive across sockets.
    /// In-flight completions send through the current socket, or remain retained.
    pub fn attach(&self, outbound: mpsc::UnboundedSender<DaemonFrame>) {
        *self.outbound.0.lock().unwrap_or_else(|p| p.into_inner()) = outbound;
        self.announce_protocol();
        self.replay_pending_journal();
    }

    pub fn active_execution_ids(&self) -> Vec<String> {
        self.active_executions.active_ids()
    }

    /// The durable queue is shared by every runtime created during command
    /// stream reconnects. Keeping this accessor public gives the daemon host a
    /// narrow inspection point for diagnostics without exposing raw payload
    /// files or credentials.
    pub fn journal(&self) -> &DaemonJournal {
        &self.journal
    }

    fn announce_protocol(&self) {
        let mut capabilities = vec![
            DAEMON_CAPABILITY_USAGE_REPORTS.to_owned(),
            DAEMON_CAPABILITY_JOURNAL_ACK.to_owned(),
            api_types::DAEMON_CAPABILITY_PLAN_TRANSPORT.to_owned(),
        ];
        if self.workspace.is_some() {
            capabilities.push(api_types::DAEMON_CAPABILITY_WORKSPACE.to_owned());
            capabilities.push(api_types::DAEMON_CAPABILITY_MACHINE_PROBE.to_owned());
            capabilities.push(api_types::DAEMON_CAPABILITY_REPO_PROVISION.to_owned());
        }
        let handshake = api_types::DaemonHandshakeNotification {
            protocol_revision: DAEMON_PROTOCOL_REVISION,
            capabilities,
            executor_capabilities: adapter_capability_facts(&self.registry),
            workspace_run_policy: self.run_policy.clone(),
        };
        emit_notification(&self.outbound, METHOD_DAEMON_HANDSHAKE, handshake);
    }

    fn replay_pending_journal(&self) {
        match self.journal.pending() {
            Ok(entries) => {
                for entry in entries {
                    if let Some((method, params)) = entry.replay_notification() {
                        emit_notification(&self.outbound, method, params);
                    }
                }
            }
            Err(error) => {
                tracing::error!(%error, "failed to replay retained daemon journal entries")
            }
        }
    }

    pub async fn handle_request(self: &Arc<Self>, frame: DaemonFrame) -> DaemonFrame {
        let DaemonFrame::Request { id, method, params } = frame else {
            return error_frame(
                None,
                INVALID_FRAME,
                "daemon command handler expected a request frame",
                None,
            );
        };

        match method.as_str() {
            METHOD_FS_LIST => match decode_params::<FsListParams>(&id, params) {
                Ok(params) => match daemon_fs::list_entries(params, &self.workspace_root).await {
                    Ok(result) => response_frame(id, result),
                    Err(error) => DaemonFrame::Error {
                        id: Some(id),
                        error,
                    },
                },
                Err(frame) => frame,
            },
            METHOD_FS_BRANCHES => match decode_params::<FsBranchesParams>(&id, params) {
                Ok(params) => match daemon_fs::list_branches(params, &self.workspace_root).await {
                    Ok(result) => response_frame(id, result),
                    Err(error) => DaemonFrame::Error {
                        id: Some(id),
                        error,
                    },
                },
                Err(frame) => frame,
            },
            METHOD_EXECUTION_START => match decode_params::<ExecutionStartParams>(&id, params) {
                Ok(params) => match self.start(params).await {
                    Ok(result) => response_frame(id, result),
                    Err(error) => DaemonFrame::Error {
                        id: Some(id),
                        error,
                    },
                },
                Err(frame) => frame,
            },
            METHOD_EXECUTION_CANCEL => match decode_params::<ExecutionCancelParams>(&id, params) {
                Ok(params) => match self.cancel(params).await {
                    Ok(result) => response_frame(id, result),
                    Err(error) => DaemonFrame::Error {
                        id: Some(id),
                        error,
                    },
                },
                Err(frame) => frame,
            },
            METHOD_JOURNAL_ACK => match decode_params::<JournalAckParams>(&id, params) {
                Ok(params) => match self.acknowledge_journal(params).await {
                    Ok(result) => response_frame(id, result),
                    Err(error) => DaemonFrame::Error {
                        id: Some(id),
                        error,
                    },
                },
                Err(frame) => frame,
            },
            method if DaemonWorkspaceBackend::supports(method) => match &self.workspace {
                Some(workspace) => match workspace
                    .handle(method, params, || self.active_executions.running_ids())
                    .await
                {
                    Ok(result) => {
                        if method == api_types::METHOD_WORKSPACE_DESCRIBE {
                            self.replay_pending_journal();
                        }
                        if method == api_types::METHOD_WORKSPACE_CLEANUP {
                            emit_notification(&self.outbound, method, result.clone());
                        }
                        response_frame(id, result)
                    }
                    Err(error) => DaemonFrame::Error {
                        id: Some(id),
                        error,
                    },
                },
                None => error_frame(
                    Some(id),
                    UNSUPPORTED_METHOD,
                    "workspace ownership is not configured for this runtime",
                    None,
                ),
            },
            METHOD_TERMINAL_START
            | METHOD_TERMINAL_INPUT
            | METHOD_TERMINAL_RESIZE
            | METHOD_TERMINAL_TERMINATE => terminal_unavailable_frame(id),
            _ => error_frame(
                Some(id),
                UNSUPPORTED_METHOD,
                format!("unsupported daemon command method: {method}"),
                None,
            ),
        }
    }

    pub async fn start(
        self: &Arc<Self>,
        params: ExecutionStartParams,
    ) -> CommandResult<ExecutionStartResult> {
        let workspace_root = self
            .workspace_root
            .to_str()
            .ok_or_else(|| DaemonErrorPayload {
                code: api_types::INVALID_INPUT.to_owned(),
                message: "workspace root is not UTF-8".to_owned(),
                details: None,
            })?;
        let worktree_path = daemon_fs::validate_within_root(
            Path::new(params.workspace_path.trim()),
            &self.workspace_root,
        )?;
        let seed = executors::execution_plan_seed(
            executors::task_role(&params.executor_config),
            None,
            params.plan_text.as_deref(),
        )
        .map(str::to_owned);
        if params
            .executor_config
            .get("_forge_plan_transport")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
            && executors::task_role_can_write_plan(executors::task_role(&params.executor_config))
        {
            crate::daemon_plan::seed(&worktree_path, &params.execution_id, seed.as_deref())
                .map_err(|error| DaemonErrorPayload {
                    code: api_types::INVALID_INPUT.into(),
                    message: format!("failed to prepare execution plan: {error}"),
                    details: None,
                })?;
        }
        let active_guard = self.active_executions.track(params.execution_id.clone());
        if let Some(workspace) = &self.workspace {
            workspace
                .register_execution(&params.execution_id, &worktree_path)
                .await?;
        }
        let logs_path = local_execution_log_path(&self.workspace_root, &params.execution_id);
        let description = prompt_description(&params.prompt);
        let mut executor_config = params.executor_config;
        if let Some(config) = executor_config.as_object_mut() {
            config.insert(
                "_forge_workspace_root".into(),
                serde_json::Value::String(workspace_root.to_owned()),
            );
        }
        let environment = executors::environment::task_environment(&executor_config);
        if params.executor_type == "shell"
            && executor_config
                .get("_forge_plan_transport")
                .and_then(Value::as_bool)
                == Some(true)
            && executors::task_role_can_write_plan(executors::task_role(&executor_config))
        {
            let outbox = executors::execution_outbox_path(&worktree_path, &params.execution_id)
                .ok_or_else(|| execution_error("invalid execution outbox"))?;
            // Runtime scope survives routing without changing the admitted
            // candidate's authored config or accounting identity.
            let mut shell_environment = environment.clone();
            shell_environment.extend([
                ("FORGE_TASK_ID".into(), params.task_id.clone()),
                ("FORGE_EXECUTION_ID".into(), params.execution_id.clone()),
                ("FORGE_OUTBOX".into(), outbox.to_string_lossy().into_owned()),
                (
                    "FORGE_PLAN_PATH".into(),
                    outbox
                        .join(executors::OUTBOX_PLAN_FILE)
                        .to_string_lossy()
                        .into_owned(),
                ),
            ]);
            executors::environment::mark_task_environment(&mut executor_config, &shell_environment);
        }
        let ctx = ExecutionContext {
            task_id: params.task_id.clone(),
            execution_id: params.execution_id.clone(),
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            description,
            agent_config: executor_config,
            logs_path: logs_path.to_string_lossy().into_owned(),
            heartbeat_interval_seconds: 30,
            max_turns: params.max_turns,
            log_sender: None,
        };

        let execution_id = params.execution_id.clone();
        let executor = Arc::clone(&self.executor);
        let outbound = self.outbound.clone();
        let journal = Arc::clone(&self.journal);
        tokio::spawn(async move {
            run_execution_task(
                executor,
                outbound,
                ctx,
                active_guard,
                journal,
                seed,
                environment,
            )
            .await;
        });

        Ok(ExecutionStartResult {
            execution_id,
            accepted: true,
        })
    }

    pub async fn cancel(
        &self,
        params: ExecutionCancelParams,
    ) -> CommandResult<ExecutionCancelResult> {
        self.executor
            .cancel(&params.execution_id)
            .await
            .map_err(|error| execution_error(format!("failed to cancel execution: {error}")))?;
        Ok(ExecutionCancelResult {
            execution_id: params.execution_id,
            cancelled: true,
        })
    }

    pub async fn acknowledge_journal(
        &self,
        params: JournalAckParams,
    ) -> CommandResult<JournalAckResult> {
        if let Some(workspace) = &self.workspace {
            return workspace.acknowledge_journal(&params).await;
        }
        self.journal.acknowledge(&params).map_err(|error| {
            let message = error.to_string();
            let code = if message.contains(TERMINAL_REPORT_CONFLICT) {
                TERMINAL_REPORT_CONFLICT
            } else {
                EXECUTION_ERROR
            };
            DaemonErrorPayload {
                code: code.to_owned(),
                message,
                details: None,
            }
        })
    }
}

async fn run_execution_task(
    executor: Arc<FallbackExecutor>,
    outbound: RuntimeOutbound,
    mut ctx: ExecutionContext,
    _active_guard: ActiveExecutionGuard,
    journal: Arc<DaemonJournal>,
    seed: Option<String>,
    environment: BTreeMap<String, String>,
) {
    let (log_tx, mut log_rx) = mpsc::unbounded_channel::<LogEntry>();
    ctx.log_sender = Some(log_tx);
    let log_outbound = outbound.clone();
    let mut log_forwarder = tokio::spawn(async move {
        while let Some(entry) = log_rx.recv().await {
            emit_execution_log(&log_outbound, entry);
        }
    });

    emit_execution_log(
        &outbound,
        daemon_system_log(&ctx.execution_id, "remote daemon execution started"),
    );

    let execution_id = ctx.execution_id.clone();
    let worktree_path = PathBuf::from(&ctx.worktree_path);
    let workspace_root = journal
        .directory()
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf);
    let plan_writing_role = ctx
        .agent_config
        .get("_forge_plan_transport")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
        && executors::task_role_can_write_plan(executors::task_role(&ctx.agent_config));
    let mut plan_text = None;
    let mut plan_error = None;
    let mut outbox_entries = Vec::new();
    let read_only_path = executors::is_worktree_read_only(&ctx.agent_config)
        .then(|| PathBuf::from(&ctx.worktree_path));
    let read_only_head = match read_only_path.as_deref() {
        Some(path) => git::get_current_sha(path).await.map(Some).map_err(|error| {
            ExecutorError::Other(format!(
                "failed to capture read-only worktree state: {error}"
            ))
        }),
        None => Ok(None),
    };
    let result = match read_only_head {
        Ok(read_only_head) => {
            let mut execution_result = executor.execute(ctx).await;
            if read_only_head.is_none() {
                if let Ok(result) = &mut execution_result {
                    if result.status == ExecutionOutcome::Completed && result.after_sha.is_none() {
                        result.after_sha = git::get_current_sha(&worktree_path).await.ok();
                    }
                }
            }
            if let Some(root) = &workspace_root {
                outbox_entries = daemon_outbox::harvest(&worktree_path, &execution_id, root);
            }
            if plan_writing_role {
                match crate::daemon_plan::harvest(&worktree_path, &execution_id) {
                    Ok(content) => plan_text = content,
                    Err(error) => {
                        plan_error = Some(format!("execution plan transport failed: {error}"))
                    }
                }
            }
            let restore_result = match (read_only_path.as_deref(), read_only_head.as_deref()) {
                (Some(path), Some(head)) => {
                    git::restore_worktree(path, head).await.map_err(|error| {
                        ExecutorError::Other(format!(
                            "failed to restore read-only worktree state: {error}"
                        ))
                    })
                }
                _ => Ok(()),
            };
            match (execution_result, restore_result) {
                (_, Err(error)) => Err(error),
                (Ok(mut result), Ok(())) => {
                    if let Some(head) = read_only_head {
                        result.after_sha = Some(head);
                    }
                    Ok(result)
                }
                (Err(error), Ok(())) => Err(error),
            }
        }
        Err(error) => Err(error),
    };
    // The executor owns the only log sender in ctx, so completion should close the
    // channel and let the forwarder drain. If an executor holds a sender clone or
    // emits a very large trailing burst, the timeout favors terminal notification
    // over complete best-effort log delivery.
    if tokio::time::timeout(Duration::from_secs(2), &mut log_forwarder)
        .await
        .is_err()
    {
        log_forwarder.abort();
        let _ = log_forwarder.await;
    }

    let mut notification = match result {
        Ok(result) => terminal_notification_from_result(execution_id, result),
        Err(error) => ExecutionTerminalNotification {
            terminal_report_id: terminal_report_id_for_execution(&execution_id),
            execution_id,
            exit_code: Some(1),
            signal: None,
            error: Some(error.to_string()),
            ts: rfc3339_now(),
            status: Some("failed".to_owned()),
            agent_session_id: None,
            summary: None,
            after_sha: None,
            usage_reports: Vec::new(),
            outbox_entries: Vec::new(),
            plan_text: None,
            failure_class: None,
            retry_at: None,
            resolved_candidate: None,
            route_attempts: None,
        },
    };
    notification.outbox_entries = outbox_entries;
    notification.plan_text = plan_text;
    if notification.status.as_deref() == Some("completed") && notification.plan_text == seed {
        notification.plan_text = None;
    }
    if let Some(error) = plan_error.filter(|_| notification.status.as_deref() == Some("completed"))
    {
        notification.status = Some("failed".into());
        notification.exit_code = Some(1);
        notification.error = Some(error);
    }
    crate::daemon_persistence::sanitize_terminal_report(&mut notification, &environment);
    daemon_outbox::fit_report(&mut notification);
    match journal.retain(&notification) {
        Ok(()) => emit_notification(&outbound, METHOD_EXECUTION_TERMINAL, notification),
        Err(error) => tracing::error!(
            %error,
            execution_id = %notification.execution_id,
            terminal_report_id = %notification.terminal_report_id,
            "failed to durably retain daemon terminal report"
        ),
    }
}

fn terminal_notification_from_result(
    execution_id: String,
    result: ExecutionResult,
) -> ExecutionTerminalNotification {
    let (status, exit_code, signal, error) = match result.status {
        ExecutionOutcome::Completed => ("completed", Some(0), None, None),
        ExecutionOutcome::Failed => ("failed", Some(1), None, result.error),
        ExecutionOutcome::Cancelled => ("cancelled", None, Some("cancelled".to_owned()), None),
    };
    ExecutionTerminalNotification {
        terminal_report_id: terminal_report_id_for_execution(&execution_id),
        execution_id,
        exit_code,
        signal,
        error,
        ts: rfc3339_now(),
        status: Some(status.to_owned()),
        agent_session_id: result.agent_session_id,
        summary: result.summary,
        after_sha: result.after_sha,
        usage_reports: result
            .usage_reports
            .into_iter()
            .map(remote_usage_report_from_executor)
            .collect(),
        outbox_entries: Vec::new(),
        plan_text: None,
        failure_class: result.failure_class.map(|class| match class {
            ExecutionFailureClass::TaskFailed => RemoteExecutionFailureClass::TaskFailed,
            ExecutionFailureClass::ExecutorUnavailable => {
                RemoteExecutionFailureClass::ExecutorUnavailable
            }
        }),
        retry_at: result.retry_after.and_then(|retry_after| {
            (OffsetDateTime::now_utc() + retry_after)
                .format(&Rfc3339)
                .ok()
        }),
        resolved_candidate: result
            .resolved_candidate
            .map(|candidate| RemoteResolvedCandidate {
                candidate_key: candidate.candidate_key,
                executor_type: candidate.executor_type.to_string(),
                config: candidate.config,
            }),
        route_attempts: if result.route_attempts.is_empty() {
            None
        } else {
            Some(
                result
                    .route_attempts
                    .into_iter()
                    .map(|attempt| RemoteRouteAttempt {
                        candidate_key: attempt.candidate_key,
                        outcome: attempt.outcome.as_str().to_owned(),
                    })
                    .collect(),
            )
        },
    }
}

fn remote_usage_report_from_executor(report: executors::UsageReport) -> RemoteUsageReport {
    RemoteUsageReport {
        report_id: report.report_id,
        request_id: report.request_id,
        report_sequence: report.report_sequence,
        candidate_key: report.candidate_key,
        attempt_ordinal: report.attempt_ordinal,
        provider_id: report.provider_id,
        model_id: report.model_id,
        input_tokens: report.counters.input_tokens,
        output_tokens: report.counters.output_tokens,
        cache_read_tokens: report.counters.cache_read_tokens,
        cache_write_tokens: report.counters.cache_write_tokens,
        telemetry_state: match report.telemetry_state {
            executors::UsageTelemetryState::Metered => UsageTelemetryState::Metered,
            executors::UsageTelemetryState::Unmetered => UsageTelemetryState::Unmetered,
            executors::UsageTelemetryState::Pending => UsageTelemetryState::Pending,
            executors::UsageTelemetryState::Unsettled => UsageTelemetryState::Unsettled,
        },
        context_tokens: report.context_tokens,
        selected_tier: report.selected_tier,
        reported_cost_usd: report.reported_cost_usd,
        partial: report.partial,
    }
}

fn terminal_report_id_for_execution(execution_id: &str) -> String {
    format!("forge:terminal:{execution_id}")
}

fn daemon_system_log(execution_id: &str, line: &str) -> LogEntry {
    LogEntry {
        schema_version: 1,
        sequence: 0,
        timestamp: rfc3339_now(),
        execution_id: execution_id.to_owned(),
        kind: executors::LogKind::System,
        stream: executors::LogStream::Main,
        payload: serde_json::json!({ "line": line }),
        truncated: false,
    }
}

fn emit_execution_log(outbound: &RuntimeOutbound, entry: LogEntry) {
    let line = entry
        .payload
        .get("line")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| entry.payload.to_string());
    let stream = match entry.kind {
        executors::LogKind::Stderr => "stderr",
        _ => "stdout",
    };
    let notification = api_types::ExecutionLogNotification {
        execution_id: entry.execution_id.clone(),
        seq: entry.sequence,
        stream: stream.to_owned(),
        line,
        ts: entry.timestamp.clone(),
        kind: Some(entry.kind.to_string()),
        log_stream: Some(
            match entry.stream {
                executors::LogStream::Heartbeat => "heartbeat",
                executors::LogStream::Main => "main",
            }
            .to_owned(),
        ),
        payload: Some(entry.payload),
        truncated: Some(entry.truncated),
    };
    emit_notification(outbound, METHOD_EXECUTION_LOG, notification);
}

fn emit_notification<T: Serialize>(outbound: &RuntimeOutbound, method: &str, notification: T) {
    match serde_json::to_value(notification) {
        Ok(params) => {
            let _ = outbound.send(DaemonFrame::Notification {
                method: method.to_owned(),
                params,
            });
        }
        Err(error) => {
            tracing::warn!(%error, method, "failed to serialize daemon notification");
        }
    }
}

fn terminal_unavailable_frame(id: String) -> DaemonFrame {
    error_frame(
        Some(id),
        TERMINAL_UNAVAILABLE,
        "terminal support is not available in this daemon command context",
        None,
    )
}

fn local_execution_log_path(workspace_root: &Path, execution_id: &str) -> PathBuf {
    workspace_root
        .join(".forge-daemon")
        .join("execution-logs")
        .join(format!("{}.jsonl", safe_path_component(execution_id)))
}

fn safe_path_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn prompt_description(prompt: &Value) -> String {
    prompt
        .get("description")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| prompt.to_string())
}

fn execution_error(message: impl Into<String>) -> DaemonErrorPayload {
    DaemonErrorPayload {
        code: EXECUTION_ERROR.to_owned(),
        message: message.into(),
        details: None,
    }
}

fn decode_params<T: DeserializeOwned>(
    id: &str,
    params: serde_json::Value,
) -> std::result::Result<T, DaemonFrame> {
    serde_json::from_value(params).map_err(|error| {
        error_frame(
            Some(id.to_owned()),
            INVALID_FRAME,
            format!("invalid daemon command params: {error}"),
            None,
        )
    })
}

fn response_frame<T: Serialize>(id: String, result: T) -> DaemonFrame {
    match serde_json::to_value(result) {
        Ok(result) => DaemonFrame::Response { id, result },
        Err(error) => error_frame(
            Some(id),
            INVALID_FRAME,
            format!("failed to serialize daemon command result: {error}"),
            None,
        ),
    }
}

fn error_frame(
    id: Option<String>,
    code: impl Into<String>,
    message: impl Into<String>,
    details: Option<serde_json::Value>,
) -> DaemonFrame {
    DaemonFrame::Error {
        id,
        error: DaemonErrorPayload {
            code: code.into(),
            message: message.into(),
            details,
        },
    }
}

fn rfc3339_now() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use api_types::{
        ExecutionLogNotification, FsListResult, METHOD_EXECUTION_LOG, METHOD_EXECUTION_TERMINAL,
        METHOD_FS_LIST,
    };
    use tokio::sync::mpsc;

    fn test_runtime(
        outbound: mpsc::UnboundedSender<DaemonFrame>,
        workspace_root: PathBuf,
    ) -> Arc<DaemonRuntime> {
        DaemonRuntime::build(
            outbound,
            workspace_root,
            ActiveExecutionTracker::default(),
            None,
            crate::daemon_config::DaemonConfig::default().run_policy(),
            Arc::new(cli_adapters::test_support::test_registry()),
        )
        .expect("initialize test daemon journal")
    }

    fn test_owned_runtime(
        outbound: mpsc::UnboundedSender<DaemonFrame>,
        workspace_root: PathBuf,
        active_executions: ActiveExecutionTracker,
        daemon_id: String,
        run_policy: api_types::WorkspaceRunPolicy,
    ) -> Result<Arc<DaemonRuntime>> {
        DaemonRuntime::build(
            outbound,
            workspace_root,
            active_executions,
            Some(daemon_id),
            run_policy,
            Arc::new(cli_adapters::test_support::test_registry()),
        )
    }

    #[tokio::test]
    async fn fs_list_returns_entries_under_workspace_root() {
        let dir = tempfile::tempdir().expect("temp dir creates");
        fs::create_dir_all(dir.path().join("src")).expect("src creates");
        fs::write(dir.path().join("README.md"), "readme").expect("readme writes");
        let (tx, _rx) = mpsc::unbounded_channel();
        let runtime = test_runtime(tx, dir.path().to_path_buf());

        let frame = DaemonFrame::Request {
            id: "fs-1".to_owned(),
            method: METHOD_FS_LIST.to_owned(),
            params: serde_json::json!({ "path": "." }),
        };
        let response = runtime.handle_request(frame).await;

        let DaemonFrame::Response { result, .. } = response else {
            panic!("expected response");
        };
        let result: FsListResult = serde_json::from_value(result).expect("fs result parses");
        let names = result
            .entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, ["src", "README.md"]);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn non_utf8_workspace_root_rejects_execution_without_panicking() {
        use std::os::unix::ffi::OsStringExt;
        let dir = tempfile::tempdir().unwrap();
        let root = dir
            .path()
            .join(std::ffi::OsString::from_vec(b"root-\xff".to_vec()));
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut runtime = test_runtime(tx, dir.path().to_path_buf());
        // Reject the configured path before filesystem access; some filesystems
        // cannot create a non-UTF-8 directory at all.
        Arc::get_mut(&mut runtime).unwrap().workspace_root = root;
        let result = runtime
            .start(ExecutionStartParams {
                plan_text: None,
                task_id: "task".into(),
                execution_id: "execution".into(),
                workspace_path: "unused".into(),
                executor_type: "shell".into(),
                executor_config: serde_json::json!({}),
                prompt: serde_json::json!({}),
                max_turns: None,
            })
            .await;
        assert_eq!(result.unwrap_err().code, api_types::INVALID_INPUT);
        assert!(runtime.active_executions.running_ids().is_empty());
    }

    #[tokio::test]
    async fn terminal_runtime_redacts_execution_environment_without_changing_ids() {
        let dir = tempfile::tempdir().unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let runtime = test_runtime(tx, dir.path().to_path_buf());
        let mut config = serde_json::json!({"executor_type":"shell","config":{}});
        executors::environment::mark_task_environment(
            &mut config,
            &BTreeMap::from([("CI".into(), "1".into())]),
        );
        runtime
            .start(ExecutionStartParams {
                plan_text: None,
                task_id: "task-1".into(),
                execution_id: "execution-1".into(),
                workspace_path: dir.path().to_string_lossy().into_owned(),
                executor_type: "shell".into(),
                executor_config: config,
                prompt: serde_json::json!({"description":"exit 1"}),
                max_turns: None,
            })
            .await
            .unwrap();
        let report = next_terminal_notification(&mut rx, "execution-1").await;
        assert_eq!(report.execution_id, "execution-1");
        assert_eq!(report.exit_code, Some(1));
        assert!(report.error.as_deref().unwrap().contains("[REDACTED]"));
        let JournalEntry::Terminal { report: retained } =
            runtime.journal.pending().unwrap().remove(0)
        else {
            panic!("terminal report");
        };
        assert_eq!(retained, report);
    }

    #[tokio::test]
    async fn shell_execution_reports_completion_notification() {
        let dir = tempfile::tempdir().expect("temp dir creates");
        let (tx, mut rx) = mpsc::unbounded_channel();
        let runtime = test_runtime(tx, dir.path().to_path_buf());
        let execution_id = "exec-shell-ok".to_owned();

        let result = runtime
            .start(ExecutionStartParams {
                plan_text: None,
                task_id: "task-1".to_owned(),
                execution_id: execution_id.clone(),
                workspace_path: dir.path().to_string_lossy().into_owned(),
                executor_type: "shell".to_owned(),
                executor_config: serde_json::json!({
                    "executor_type": "shell",
                    "config": {}
                }),
                prompt: serde_json::json!({ "description": "printf ok > marker.txt" }),
                max_turns: None,
            })
            .await
            .expect("execution starts");
        assert!(result.accepted);

        let notification = next_terminal_notification(&mut rx, &execution_id).await;
        assert_eq!(notification.status.as_deref(), Some("completed"));
        assert_eq!(
            fs::read_to_string(dir.path().join("marker.txt")).expect("marker exists"),
            "ok"
        );
    }

    #[tokio::test]
    async fn shell_execution_reports_committed_head_when_adapter_omits_it() {
        let dir = tempfile::tempdir().expect("temp dir creates");
        git::init(dir.path()).await.expect("repository initializes");
        fs::write(dir.path().join("base.txt"), "base\n").expect("base file writes");
        let base_sha = git::commit_all(dir.path(), "base")
            .await
            .expect("base commit creates");
        let (tx, mut rx) = mpsc::unbounded_channel();
        let runtime = test_runtime(tx, dir.path().to_path_buf());

        runtime
            .start(ExecutionStartParams {
            plan_text: None,
                task_id: "task-1".to_owned(),
                execution_id: "exec-shell-commit".to_owned(),
                workspace_path: dir.path().to_string_lossy().into_owned(),
                executor_type: "shell".to_owned(),
                executor_config: serde_json::json!({
                    "executor_type": "shell",
                    "config": {}
                }),
                prompt: serde_json::json!({
                    "description": "printf 'candidate\\n' > candidate.txt; git add candidate.txt; git commit -m candidate"
                }),
                max_turns: None,
            })
            .await
            .expect("execution starts");

        let notification = next_terminal_notification(&mut rx, "exec-shell-commit").await;
        let after_sha = notification.after_sha.expect("committed HEAD is reported");
        assert_ne!(after_sha, base_sha);
        assert_eq!(after_sha, git::get_current_sha(dir.path()).await.unwrap());
    }

    #[tokio::test]
    async fn outbox_entries_are_present_in_replayed_terminal_report() {
        use api_types::*;

        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("primary");
        fs::create_dir(&repo).unwrap();
        git::init(&repo).await.unwrap();
        fs::write(repo.join("README.md"), "base").unwrap();
        let sha = git::commit_all(&repo, "base").await.unwrap();
        let policy = crate::daemon_config::DaemonConfig::default().run_policy();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let runtime = test_owned_runtime(
            tx,
            dir.path().to_owned(),
            ActiveExecutionTracker::default(),
            "daemon-1".into(),
            policy.clone(),
        )
        .unwrap();
        let DaemonFrame::Notification { method, params } = rx.recv().await.unwrap() else {
            panic!("expected handshake");
        };
        assert_eq!(method, METHOD_DAEMON_HANDSHAKE);
        let handshake: DaemonHandshakeNotification = serde_json::from_value(params).unwrap();
        assert!(handshake
            .capabilities
            .contains(&DAEMON_CAPABILITY_WORKSPACE.to_owned()));
        assert_eq!(handshake.workspace_run_policy, policy);
        assert!(handshake.executor_capabilities["shell"].terminal_observed);
        assert!(!handshake.executor_capabilities["shell"].resume);
        let mut workspace_path = None;
        let mut workspace_handle = None;
        for (method, params) in [
            (
                METHOD_REPO_LOCATION_VERIFY,
                serde_json::json!({"repo_location_id":"location-1", "daemon_id":"daemon-1", "runtime_id":"runtime-1", "path":repo, "kind":"primary_checkout", "default_branch":"main", "remote_url":null, "expected_version":0}),
            ),
            (
                METHOD_WORKSPACE_PREPARE,
                serde_json::json!({"integration":{"kind":"task_step"}, "daemon_id":"daemon-1", "runtime_id":"runtime-1", "placement_id":"placement-1", "operation_id":"prepare-1", "generation":1, "expected":{"kind":"base_sha", "sha":sha}, "repo_location_id":"location-1", "workspace_id":"workspace-1", "task_id":"task-1", "base_ref":"main", "branch":"task/outbox"}),
            ),
        ] {
            let response = runtime
                .handle_request(DaemonFrame::Request {
                    id: method.into(),
                    method: method.into(),
                    params,
                })
                .await;
            match response {
                DaemonFrame::Response { result, .. } => {
                    if let Some(path) = result.get("workspace_path").and_then(Value::as_str) {
                        workspace_path = Some(path.to_owned());
                    }
                    if let Some(handle) = result.get("workspace_handle").and_then(Value::as_str) {
                        workspace_handle = Some(handle.to_owned());
                    }
                }
                other => panic!("workspace request failed: {other:?}"),
            }
        }
        runtime.start(ExecutionStartParams {
            plan_text: Some("- [ ] revision".into()),
            task_id: "task-1".into(), execution_id: "exec-outbox".into(), workspace_path: workspace_path.unwrap(),
            executor_type: "shell".into(), executor_config: serde_json::json!({"executor_type":"shell", "config":{}, "_forge_task_role":"planner", "_forge_plan_transport":true}),
            prompt: serde_json::json!({"description": r#"test ! -e ../.forge-outbox/exec-outbox/plan.md || exit 13; printf '%s\n' '- [ ] remote plan' > ../.forge-outbox/exec-outbox/plan.md; mkdir -p ../.forge-outbox/exec-outbox; printf '%s\n' '{"kind":"progress","summary":"remote progress"}' > ../.forge-outbox/exec-outbox/worklog.jsonl; printf '%s\n' '{"kind":"report","caption":"remote evidence","content":"captured remotely"}' > ../.forge-outbox/exec-outbox/evidence.jsonl"#}),
            max_turns: None,
        }).await.unwrap();
        let report = next_terminal_notification(&mut rx, "exec-outbox").await;
        assert_eq!(report.outbox_entries.len(), 2);
        assert!(
            matches!(&report.outbox_entries[0], ExecutionOutboxEntry::Worklog { summary, .. } if summary == "remote progress")
        );
        assert_eq!(report.plan_text.as_deref(), Some("- [ ] remote plan\n"));
        drop(runtime);
        let (tx, mut replay_rx) = mpsc::unbounded_channel();
        let restarted = test_owned_runtime(
            tx,
            dir.path().to_owned(),
            ActiveExecutionTracker::default(),
            "daemon-1".into(),
            policy,
        )
        .unwrap();
        let replayed = next_terminal_notification(&mut replay_rx, "exec-outbox").await;
        assert_eq!(replayed, report);
        let describe = restarted.handle_request(DaemonFrame::Request {
            id: "describe-replay".to_owned(), method: METHOD_WORKSPACE_DESCRIBE.to_owned(),
            params: serde_json::json!({"daemon_id": "daemon-1", "runtime_id": "runtime-1", "placement_id": "placement-1",
                "workspace_handle": workspace_handle.unwrap(), "generation": 1}),
        }).await;
        let DaemonFrame::Response { result, .. } = describe else {
            panic!("describe response");
        };
        let described: WorkspaceDescribeResult = serde_json::from_value(result).unwrap();
        assert!(described.active_execution_ids.is_empty());
        assert_eq!(described.journaled_execution_ids, ["exec-outbox"]);
        assert_eq!(
            next_terminal_notification(&mut replay_rx, "exec-outbox").await,
            report
        );
        restarted
            .acknowledge_journal(JournalAckParams {
                entry_id: replayed.terminal_report_id,
            })
            .await
            .unwrap();
        assert!(!restarted
            .journal()
            .pending()
            .unwrap()
            .iter()
            .any(|entry| matches!(entry, JournalEntry::Terminal { .. })));
    }

    #[tokio::test]
    async fn terminal_report_is_retained_until_acknowledged() {
        let dir = tempfile::tempdir().expect("temp dir creates");
        let (tx, mut rx) = mpsc::unbounded_channel();
        let runtime = test_runtime(tx, dir.path().to_path_buf());
        let execution_id = "exec-shell-retained".to_owned();

        runtime
            .start(ExecutionStartParams {
                plan_text: None,
                task_id: "task-1".to_owned(),
                execution_id: execution_id.clone(),
                workspace_path: dir.path().to_string_lossy().into_owned(),
                executor_type: "shell".to_owned(),
                executor_config: serde_json::json!({
                    "executor_type": "shell",
                    "config": {}
                }),
                prompt: serde_json::json!({ "description": "printf retained" }),
                max_turns: None,
            })
            .await
            .expect("execution starts");

        let notification = next_terminal_notification(&mut rx, &execution_id).await;
        let retained = runtime.journal().pending().expect("pending reports");
        assert_eq!(
            retained[0].replay_notification().unwrap().1,
            serde_json::to_value(&notification).unwrap()
        );

        let response = runtime
            .handle_request(DaemonFrame::Request {
                id: "terminal-ack-1".to_owned(),
                method: METHOD_JOURNAL_ACK.to_owned(),
                params: serde_json::to_value(JournalAckParams {
                    entry_id: notification.terminal_report_id.clone(),
                })
                .expect("ack params serialize"),
            })
            .await;
        let DaemonFrame::Response { result, .. } = response else {
            panic!("expected ack response");
        };
        let result: JournalAckResult = serde_json::from_value(result).expect("ack result parses");
        assert!(result.acknowledged);
        assert!(runtime
            .journal()
            .pending()
            .expect("empty reports")
            .is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shell_execution_can_be_cancelled() {
        let dir = tempfile::tempdir().expect("temp dir creates");
        let (tx, mut rx) = mpsc::unbounded_channel();
        let runtime = test_runtime(tx, dir.path().to_path_buf());
        let execution_id = "exec-shell-cancel".to_owned();

        runtime
            .start(ExecutionStartParams {
                plan_text: None,
                task_id: "task-1".to_owned(),
                execution_id: execution_id.clone(),
                workspace_path: dir.path().to_string_lossy().into_owned(),
                executor_type: "shell".to_owned(),
                executor_config: serde_json::json!({
                    "executor_type": "shell",
                    "config": {}
                }),
                prompt: serde_json::json!({ "description": "printf 'started\\n'; sleep 30" }),
                max_turns: None,
            })
            .await
            .expect("execution starts");
        next_execution_log_line(&mut rx, &execution_id, "started").await;
        runtime
            .cancel(ExecutionCancelParams {
                execution_id: execution_id.clone(),
                reason: Some("test".to_owned()),
            })
            .await
            .expect("execution cancels");

        let notification = next_terminal_notification(&mut rx, &execution_id).await;
        assert_eq!(notification.status.as_deref(), Some("cancelled"));
    }

    async fn next_execution_log_line(
        rx: &mut mpsc::UnboundedReceiver<DaemonFrame>,
        execution_id: &str,
        expected_line: &str,
    ) -> ExecutionLogNotification {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let frame = tokio::time::timeout_at(deadline, rx.recv())
                .await
                .expect("execution log arrives")
                .expect("runtime keeps sender open");
            let DaemonFrame::Notification { method, params } = frame else {
                continue;
            };
            if method != METHOD_EXECUTION_LOG {
                continue;
            }
            let notification: ExecutionLogNotification =
                serde_json::from_value(params).expect("execution log parses");
            if notification.execution_id == execution_id && notification.line == expected_line {
                return notification;
            }
        }
    }

    async fn next_terminal_notification(
        rx: &mut mpsc::UnboundedReceiver<DaemonFrame>,
        execution_id: &str,
    ) -> ExecutionTerminalNotification {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let frame = tokio::time::timeout_at(deadline, rx.recv())
                .await
                .expect("terminal notification arrives")
                .expect("runtime keeps sender open");
            let DaemonFrame::Notification { method, params } = frame else {
                continue;
            };
            if method != METHOD_EXECUTION_TERMINAL {
                continue;
            }
            let notification: ExecutionTerminalNotification =
                serde_json::from_value(params).expect("terminal notification parses");
            if notification.execution_id == execution_id {
                return notification;
            }
        }
    }

    #[test]
    fn tracker_lingers_finished_executions_in_active_ids() {
        let tracker = ActiveExecutionTracker::default();
        let guard = tracker.track("exec-1".to_owned());
        assert_eq!(tracker.active_ids(), ["exec-1"]);

        drop(guard);
        assert_eq!(
            tracker.active_ids(),
            ["exec-1"],
            "finished execution must linger in reports until the terminal notification has settled"
        );
    }

    #[test]
    fn tracker_prunes_finished_executions_after_linger() {
        let tracker = ActiveExecutionTracker::with_finished_linger(Duration::ZERO);
        let guard = tracker.track("exec-1".to_owned());
        drop(guard);
        assert!(tracker.active_ids().is_empty());
    }

    #[test]
    fn tracker_retrack_moves_id_back_to_active() {
        let tracker = ActiveExecutionTracker::with_finished_linger(Duration::ZERO);
        let first = tracker.track("exec-1".to_owned());
        drop(first);
        let _second = tracker.track("exec-1".to_owned());
        assert_eq!(tracker.active_ids(), ["exec-1"]);
    }

    #[test]
    fn terminal_mapping_preserves_each_usage_report_without_flattening() {
        let mut report = executors::UsageReport::metered(
            "provider-report-1",
            executors::UsageCounters {
                input_tokens: Some(10),
                output_tokens: Some(4),
                cache_read_tokens: None,
                cache_write_tokens: Some(2),
            },
        );
        report.request_id = Some("provider-request-1".to_owned());
        report.candidate_key = Some("codex#primary".to_owned());
        report.attempt_ordinal = 1;
        report.provider_id = Some("openai".to_owned());
        report.model_id = Some("gpt-5".to_owned());
        report.context_tokens = Some(128);
        report.selected_tier = Some("long".to_owned());
        report.reported_cost_usd = Some("0.000000123".to_owned());
        report.partial = true;

        let notification = terminal_notification_from_result(
            "execution-1".to_owned(),
            ExecutionResult {
                status: ExecutionOutcome::Completed,
                usage_reports: vec![report],
                ..ExecutionResult::default()
            },
        );

        assert_eq!(
            notification.terminal_report_id,
            "forge:terminal:execution-1"
        );
        assert_eq!(notification.usage_reports.len(), 1);
        let remote = &notification.usage_reports[0];
        assert_eq!(remote.report_id, "provider-report-1");
        assert_eq!(remote.request_id.as_deref(), Some("provider-request-1"));
        assert_eq!(remote.input_tokens, Some(10));
        assert_eq!(remote.output_tokens, Some(4));
        assert_eq!(remote.cache_read_tokens, None);
        assert_eq!(remote.cache_write_tokens, Some(2));
        assert_eq!(remote.reported_cost_usd.as_deref(), Some("0.000000123"));
        assert!(remote.partial);
    }

    async fn run_remote_plan_role(
        role: &str,
        seed: Option<&str>,
        command: &str,
    ) -> ExecutionTerminalNotification {
        let dir = tempfile::tempdir().unwrap();
        let worktree = dir.path().join("task").join("repo");
        fs::create_dir_all(&worktree).unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let runtime = test_runtime(tx, dir.path().to_path_buf());
        runtime.start(ExecutionStartParams {
            plan_text: seed.map(str::to_owned), task_id:"task-1".into(), execution_id:"exec-plan-role".into(),
            workspace_path:worktree.to_string_lossy().into_owned(), executor_type:"shell".into(),
            executor_config:serde_json::json!({"executor_type":"shell","config":{},"_forge_task_role":role,"_forge_plan_transport":true}),
            prompt:serde_json::json!({"description":command}), max_turns:None,
        }).await.unwrap();
        next_terminal_notification(&mut rx, "exec-plan-role").await
    }

    #[tokio::test]
    async fn remote_plan_seeds_match_local_and_unchanged_seed_is_absent() {
        for (role, seed) in [
            ("coder", Some("prose")),
            ("coder", Some("")),
            ("planner", Some("- [ ] old\n")),
            ("planner", None),
            ("coder", Some("- [ ] unchanged\n")),
        ] {
            let report = run_remote_plan_role(role, seed, "true").await;
            assert_eq!(report.status.as_deref(), Some("completed"));
            assert!(report.error.is_none());
            assert!(report.plan_text.is_none());
        }
    }

    #[tokio::test]
    async fn remote_plan_capture_errors_preserve_failed_and_cancelled_outcomes() {
        let command = "printf '\\377\\376' > \"$FORGE_PLAN_PATH\"; exit 3";
        let failed = run_remote_plan_role("coder", Some("- [ ] seeded\n"), command).await;
        assert_eq!(failed.status.as_deref(), Some("failed"));
        assert!(!failed
            .error
            .as_deref()
            .unwrap_or("")
            .contains("execution plan transport failed"));
        let dir = tempfile::tempdir().unwrap();
        let worktree = dir.path().join("task/repo");
        fs::create_dir_all(&worktree).unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let runtime = test_runtime(tx, dir.path().to_path_buf());
        runtime.start(ExecutionStartParams {
            plan_text:None,task_id:"task-1".into(),execution_id:"exec-plan-cancel".into(),
            workspace_path:worktree.to_string_lossy().into_owned(),executor_type:"shell".into(),
            executor_config:serde_json::json!({"executor_type":"shell","config":{},"_forge_task_role":"planner","_forge_plan_transport":true}),
            prompt:serde_json::json!({"description":"printf '\\377\\376' > \"$FORGE_PLAN_PATH\"; printf 'ready\\n'; sleep 30"}),max_turns:None,
        }).await.unwrap();
        next_execution_log_line(&mut rx, "exec-plan-cancel", "ready").await;
        runtime
            .cancel(ExecutionCancelParams {
                execution_id: "exec-plan-cancel".into(),
                reason: Some("operator cancellation".into()),
            })
            .await
            .unwrap();
        let report = next_terminal_notification(&mut rx, "exec-plan-cancel").await;
        assert_eq!(report.status.as_deref(), Some("cancelled"));
        assert!(report.error.is_none());
        let mut cancelled = failed;
        cancelled.status = Some("cancelled".into());
        cancelled.error = Some("original cancellation".into());
        cancelled.plan_text = Some("x".repeat(api_types::MAX_EXECUTION_PLAN_BYTES as usize + 1));
        daemon_outbox::fit_report(&mut cancelled);
        assert_eq!(cancelled.status.as_deref(), Some("cancelled"));
        assert_eq!(cancelled.error.as_deref(), Some("original cancellation"));
        let oversized = run_remote_plan_role(
            "planner",
            None,
            "head -c 200000 /dev/zero > \"$FORGE_PLAN_PATH\"",
        )
        .await;
        assert_eq!(oversized.status.as_deref(), Some("failed"));
        let error = oversized.error.unwrap();
        assert!(error.contains("131072"));
        assert!(error.contains("200000"));
        let invalid = run_remote_plan_role(
            "planner",
            None,
            "printf '\\377\\376' > \"$FORGE_PLAN_PATH\"",
        )
        .await;
        assert_eq!(invalid.status.as_deref(), Some("failed"));
    }
}
