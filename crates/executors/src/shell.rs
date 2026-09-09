use crate::{
    build_shell_command_plan, ExecutionContext, ExecutionOutcome, ExecutionResult, ExecutorError,
    LogKind, LogStream, LogWriter, ShellConfig, TaskExecutor,
};
use async_trait::async_trait;
use std::{
    collections::HashMap,
    process::Stdio,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, BufReader},
    process::{Child, Command},
    sync::{mpsc, Mutex as AsyncMutex},
    time::{self, MissedTickBehavior},
};

const DEFAULT_MAX_OUTPUT_BYTES: u64 = 10 * 1024 * 1024;
const COMPLETION_POLL_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_CANCEL_GRACE_PERIOD: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct ShellExecutor {
    processes: Arc<Mutex<HashMap<String, Arc<RunningProcess>>>>,
    cancel_grace_period: Duration,
    shell_program: Option<String>,
}

impl Default for ShellExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl ShellExecutor {
    pub fn new() -> Self {
        Self {
            processes: Arc::new(Mutex::new(HashMap::new())),
            cancel_grace_period: DEFAULT_CANCEL_GRACE_PERIOD,
            shell_program: None,
        }
    }

    pub fn with_cancel_grace_period(mut self, grace: Duration) -> Self {
        self.cancel_grace_period = grace;
        self
    }

    #[doc(hidden)]
    pub fn with_shell_program(mut self, program: impl Into<String>) -> Self {
        self.shell_program = Some(program.into());
        self
    }

    #[doc(hidden)]
    pub fn has_process(&self, execution_id: &str) -> bool {
        self.lock_processes()
            .map(|processes| processes.contains_key(execution_id))
            .unwrap_or(false)
    }

    #[doc(hidden)]
    pub async fn running_child_pid(&self, execution_id: &str) -> Option<u32> {
        let process = self.get_process(execution_id).ok()??;
        let child = process.child.lock().await;
        child.id()
    }
}

struct RunningProcess {
    child: AsyncMutex<Child>,
    cancellation_requested: AtomicBool,
}

enum OutputEvent {
    Line { kind: LogKind, line: String },
    ReaderError { kind: LogKind, error: String },
}

#[async_trait]
impl TaskExecutor for ShellExecutor {
    async fn execute(&self, ctx: ExecutionContext) -> Result<ExecutionResult, ExecutorError> {
        let mut writer = LogWriter::new(
            &ctx.logs_path,
            ctx.execution_id.clone(),
            DEFAULT_MAX_OUTPUT_BYTES,
        );
        if let Some(sender) = ctx.log_sender.clone() {
            writer.set_log_sender(sender);
        }

        let shell_config: ShellConfig =
            serde_json::from_value(ctx.agent_config.clone()).unwrap_or_default();
        let mut plan = build_shell_command_plan(
            &ctx.description,
            &ctx.worktree_path,
            ctx.max_turns,
            Some(&shell_config),
        );
        if let Some(shell_program) = &self.shell_program {
            plan.program = shell_program.clone();
        }

        let mut command = Command::new(&plan.program);
        command
            .args(&plan.args)
            .current_dir(&plan.cwd)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for key in &plan.env_remove {
            command.env_remove(key);
        }
        for (key, value) in &plan.env_set {
            command.env(key, value);
        }
        configure_process_group(&mut command);

        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                return Err(ExecutorError::Io(error));
            }
        };

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ExecutorError::Other("failed to capture child stdout".to_string()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| ExecutorError::Other("failed to capture child stderr".to_string()))?;

        let process = Arc::new(RunningProcess {
            child: AsyncMutex::new(child),
            cancellation_requested: AtomicBool::new(false),
        });

        self.insert_process(ctx.execution_id.clone(), process.clone())?;

        let result = self
            .supervise_process(&ctx, process.clone(), stdout, stderr, &mut writer)
            .await;

        self.remove_process(&ctx.execution_id)?;

        result
    }

    async fn cancel(&self, execution_id: &str) -> Result<(), ExecutorError> {
        // Idempotent: the process may have finished (and been reaped) between the
        // caller's decision to cancel and this call.
        let Some(process) = self.get_process(execution_id)? else {
            return Ok(());
        };

        process.cancellation_requested.store(true, Ordering::SeqCst);

        let child_id = {
            let child = process.child.lock().await;
            child.id()
        };

        if let Some(child_id) = child_id {
            let _ = send_sigterm(child_id).await;
        }

        let deadline = time::Instant::now() + self.cancel_grace_period;
        let direct_child_exited = loop {
            {
                let mut child = process.child.lock().await;
                if child.try_wait()?.is_some() {
                    break true;
                }
            }

            if time::Instant::now() >= deadline {
                break false;
            }

            time::sleep(COMPLETION_POLL_INTERVAL).await;
        };

        // Always SIGKILL the process group, even when the direct child exited
        // during the grace period: TERM-ignoring descendants in the group can
        // outlive it and keep the output pipes open, which would stall the
        // supervisor's drain loop until they exit on their own. Signalling an
        // already-dead group is a no-op (ESRCH).
        if let Some(child_id) = child_id {
            let _ = send_sigkill(child_id).await;
        }
        if !direct_child_exited {
            // Tolerate the child exiting between the deadline check and here;
            // start_kill errors on an already-reaped process.
            let mut child = process.child.lock().await;
            let _ = child.start_kill();
        }

        Ok(())
    }
}

#[cfg(unix)]
fn configure_process_group(command: &mut Command) {
    command.process_group(0);
}

#[cfg(not(unix))]
fn configure_process_group(_command: &mut Command) {}

impl ShellExecutor {
    fn insert_process(
        &self,
        execution_id: String,
        process: Arc<RunningProcess>,
    ) -> Result<(), ExecutorError> {
        let mut processes = self.lock_processes()?;
        processes.insert(execution_id, process);
        Ok(())
    }

    fn get_process(
        &self,
        execution_id: &str,
    ) -> Result<Option<Arc<RunningProcess>>, ExecutorError> {
        let processes = self.lock_processes()?;
        Ok(processes.get(execution_id).cloned())
    }

    fn remove_process(&self, execution_id: &str) -> Result<(), ExecutorError> {
        let mut processes = self.lock_processes()?;
        processes.remove(execution_id);
        Ok(())
    }

    fn lock_processes(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, HashMap<String, Arc<RunningProcess>>>, ExecutorError>
    {
        self.processes
            .lock()
            .map_err(|_| ExecutorError::Other("shell process map lock poisoned".to_string()))
    }

    async fn supervise_process(
        &self,
        ctx: &ExecutionContext,
        process: Arc<RunningProcess>,
        stdout: impl AsyncRead + Unpin + Send + 'static,
        stderr: impl AsyncRead + Unpin + Send + 'static,
        writer: &mut LogWriter,
    ) -> Result<ExecutionResult, ExecutorError> {
        let (tx, mut rx) = mpsc::channel(256);
        tokio::spawn(read_output_lines(stdout, LogKind::Stdout, tx.clone()));
        tokio::spawn(read_output_lines(stderr, LogKind::Stderr, tx));

        let mut completion_interval = time::interval(COMPLETION_POLL_INTERVAL);
        completion_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let heartbeat_interval = Duration::from_secs(ctx.heartbeat_interval_seconds.max(1));
        let mut heartbeat = time::interval(heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let status = loop {
            tokio::select! {
                Some(event) = rx.recv() => {
                    write_output_event(writer, event).await?;
                }
                _ = completion_interval.tick() => {
                    let status = {
                        let mut child = process.child.lock().await;
                        child.try_wait()?
                    };

                    if let Some(status) = status {
                        break status;
                    }
                }
                _ = heartbeat.tick() => {
                    let status = {
                        let mut child = process.child.lock().await;
                        child.try_wait()?
                    };

                    if let Some(status) = status {
                        break status;
                    }

                    writer
                        .write(
                            LogKind::System,
                            LogStream::Heartbeat,
                            serde_json::json!({
                                "status": "alive",
                                "task_id": ctx.task_id,
                                "execution_id": ctx.execution_id,
                            }),
                        )
                        .await?;
                }
            }
        };

        while let Some(event) = rx.recv().await {
            write_output_event(writer, event).await?;
        }

        if process.cancellation_requested.load(Ordering::SeqCst) {
            return Ok(ExecutionResult {
                status: ExecutionOutcome::Cancelled,
                after_sha: None,
                agent_session_id: None,
                summary: None,
                error: None,
                usage_reports: Vec::new(),
                ..Default::default()
            });
        }

        if status.success() {
            Ok(ExecutionResult {
                status: ExecutionOutcome::Completed,
                after_sha: None,
                agent_session_id: None,
                summary: None,
                error: None,
                usage_reports: Vec::new(),
                ..Default::default()
            })
        } else {
            Ok(ExecutionResult {
                status: ExecutionOutcome::Failed,
                after_sha: None,
                agent_session_id: None,
                summary: None,
                error: Some(format!("shell command exited with status {status}")),
                usage_reports: Vec::new(),
                ..Default::default()
            })
        }
    }
}

async fn read_output_lines<R>(reader: R, kind: LogKind, tx: mpsc::Sender<OutputEvent>)
where
    R: AsyncRead + Unpin,
{
    let mut lines = BufReader::new(reader).lines();

    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                if tx
                    .send(OutputEvent::Line {
                        kind: kind.clone(),
                        line,
                    })
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Ok(None) => break,
            Err(error) => {
                let _ = tx
                    .send(OutputEvent::ReaderError {
                        kind,
                        error: error.to_string(),
                    })
                    .await;
                break;
            }
        }
    }
}

async fn write_output_event(
    writer: &mut LogWriter,
    event: OutputEvent,
) -> Result<(), ExecutorError> {
    match event {
        OutputEvent::Line { kind, line } => {
            writer
                .write(kind, LogStream::Main, serde_json::json!({ "line": line }))
                .await?;
        }
        OutputEvent::ReaderError { kind, error } => {
            writer
                .write(
                    LogKind::System,
                    LogStream::Main,
                    serde_json::json!({
                        "error": error,
                        "source": match kind {
                            LogKind::Stdout => "stdout",
                            LogKind::Stderr => "stderr",
                            _ => "output",
                        },
                    }),
                )
                .await?;
        }
    }

    Ok(())
}

async fn send_sigterm(pid: u32) -> std::io::Result<()> {
    send_signal(pid, "TERM").await
}

async fn send_sigkill(pid: u32) -> std::io::Result<()> {
    send_signal(pid, "KILL").await
}

#[cfg(unix)]
async fn send_signal(pid: u32, signal: &str) -> std::io::Result<()> {
    Command::new("kill")
        .arg(format!("-{signal}"))
        .arg("--")
        .arg(format!("-{pid}"))
        .status()
        .await
        .map(|_| ())
}

#[cfg(not(unix))]
async fn send_signal(pid: u32, signal: &str) -> std::io::Result<()> {
    Command::new("kill")
        .arg(format!("-{signal}"))
        .arg(pid.to_string())
        .status()
        .await
        .map(|_| ())
}

#[cfg(unix)]
#[doc(hidden)]
pub fn is_pid_alive(pid: u32) -> bool {
    std::process::Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

#[cfg(not(unix))]
#[doc(hidden)]
pub fn is_pid_alive(pid: u32) -> bool {
    std::process::Command::new("tasklist")
        .arg("/FI")
        .arg(format!("PID eq {pid}"))
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}
