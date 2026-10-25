//! Embedded terminal PTY tasks, output delivery, and session resource cleanup.

use super::*;

pub(super) fn spawn_embedded_reader(
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

pub(super) fn spawn_embedded_control(
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
                        break;
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
                    break;
                }
            }
        }
        // Runtime shutdown drops the command senders too. Kill the shell so
        // the blocking PTY reader observes EOF instead of holding shutdown.
        match child.lock() {
            Ok(mut child) => {
                if !matches!(child.try_wait(), Ok(Some(_))) {
                    if let Err(error) = child.kill() {
                        tracing::warn!(%error, session_id = %session_id, "failed to kill embedded terminal child");
                    }
                }
            }
            Err(error) => {
                tracing::warn!(%error, session_id = %session_id, "embedded terminal child lock poisoned during termination");
            }
        }
    });
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn wait_for_embedded_child_exit(
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

pub(super) async fn embedded_watchdog(
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

pub(super) async fn record_terminal_output(
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
pub(super) async fn handle_terminal_exited(
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

pub(super) async fn release_session_resources(
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

pub(super) async fn fanout_frame(
    state: Arc<Mutex<TerminalState>>,
    session_id: String,
    frame: TerminalServerFrame,
) {
    let mut state = state.lock().await;
    if let Some(clients) = state.attached_clients.get_mut(&session_id) {
        clients.retain(|sender| sender.send(frame.clone()).is_ok());
    }
}

pub(super) fn publish_session_changed(
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
