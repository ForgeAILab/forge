//! Tokio controller and reducer bridge for Forge Solo.
//!
//! `SoloController` owns orchestration only: terminal/backend sources are
//! polled, backend work is spawned, and typed events are handed to a pure
//! reducer.  Rendering is intentionally absent from this module.  The TUI
//! model can implement [`SoloReducer`] without importing service or database
//! types, while a local [`crate::backend::SoloBackend`] adapter performs all
//! authoritative reads and domain mutations.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{self, MissedTickBehavior},
};

use crate::backend::{
    ensure_activity_scope, ensure_snapshot_scope, ActivityBatch, ActivityReadRequest,
    ActivityTarget, BackendCommand, BackendCommandResult, BackendError, BackendEvent,
    BackendEventKind, BackendEventPoll, BackendEventSource, BackendFuture, BackendResult,
    ChannelBackendEventSource, IdempotencyKey, ShutdownIntent, ShutdownOutcome, SnapshotLimits,
    SnapshotRequest, SoloBackend, SoloSnapshot,
};

/// Keyboard-independent key code used by the reducer boundary.  The terminal
/// adapter can map crossterm (or a test source) into this small vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyCode {
    Char(char),
    Enter,
    Esc,
    Backspace,
    Delete,
    Left,
    Right,
    Up,
    Down,
    Home,
    End,
    PageUp,
    PageDown,
    Tab,
    BackTab,
    F(u8),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct KeyModifiers {
    pub ctrl: bool,
    pub alt: bool,
    pub shift: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyEvent {
    pub code: KeyCode,
    pub modifiers: KeyModifiers,
}

impl KeyEvent {
    pub fn new(code: KeyCode) -> Self {
        Self {
            code,
            modifiers: KeyModifiers::default(),
        }
    }

    pub fn ctrl(code: KeyCode) -> Self {
        Self {
            code,
            modifiers: KeyModifiers {
                ctrl: true,
                ..KeyModifiers::default()
            },
        }
    }
}

/// Input delivered to the pure reducer.  `Interrupt` is kept distinct from a
/// regular Ctrl-C key so the controller can enforce the two-stage shutdown
/// behavior while a graceful shutdown is already in progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputEvent {
    Key(KeyEvent),
    Resize { width: u16, height: u16 },
    Interrupt,
    Quit,
    Closed,
}

/// Poll result for the terminal source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputSourcePoll {
    Event(InputEvent),
    Closed,
    Unavailable { detail: String },
}

/// A source of keyboard/resize events.  A physical terminal adapter can
/// block in `recv`; tests generally use [`channel_input_source`].
pub trait TerminalEventSource: Send {
    fn recv(&mut self) -> BackendFuture<'_, InputSourcePoll>;
}

pub struct ChannelTerminalEventSource {
    receiver: mpsc::Receiver<InputEvent>,
}

impl ChannelTerminalEventSource {
    pub fn new(receiver: mpsc::Receiver<InputEvent>) -> Self {
        Self { receiver }
    }
}

impl TerminalEventSource for ChannelTerminalEventSource {
    fn recv(&mut self) -> BackendFuture<'_, InputSourcePoll> {
        Box::pin(async move {
            match self.receiver.recv().await {
                Some(event) => InputSourcePoll::Event(event),
                None => InputSourcePoll::Closed,
            }
        })
    }
}

/// Construct a bounded input channel and its receiving source.
pub fn channel_input_source(
    capacity: usize,
) -> (mpsc::Sender<InputEvent>, ChannelTerminalEventSource) {
    let (sender, receiver) = mpsc::channel(capacity.max(1));
    (sender, ChannelTerminalEventSource::new(receiver))
}

/// Events consumed by a pure Solo model/reducer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControllerEvent {
    Input(InputEvent),
    Backend(BackendEvent),
    BackendLagged {
        skipped: u64,
    },
    BackendSourceClosed,
    BackendSourceUnavailable {
        detail: String,
    },
    SnapshotUpdated {
        request: SnapshotRequest,
        result: BackendResult<SoloSnapshot>,
    },
    ActivityUpdated {
        request: ActivityReadRequest,
        result: BackendResult<ActivityBatch>,
    },
    CommandCompleted {
        id: u64,
        command: BackendCommand,
        result: BackendResult<BackendCommandResult>,
    },
    ShutdownCompleted {
        intent: ShutdownIntent,
        result: BackendResult<ShutdownOutcome>,
    },
    EffectRejected {
        effect: ControllerEffect,
        error: BackendError,
    },
    BackendError(BackendError),
    Tick,
    ActivityPollDue,
}

/// Effects emitted by the pure reducer.  Effects are the only way the TUI
/// asks for domain I/O; render functions need not know about this enum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControllerEffect {
    Command(BackendCommand),
    Refresh,
    ReadActivity(ActivityReadRequest),
    RequestShutdown(ShutdownIntent),
    Noop,
}

/// Small reducer contract consumed by [`SoloController`].  Reducers should
/// keep composer drafts and modal targets in their own state, and preserve
/// them when a `CommandCompleted` event carries a conflict/error.
pub trait SoloReducer: Send {
    fn reduce(&mut self, event: ControllerEvent) -> Vec<ControllerEffect>;
}

/// Synchronous state-change hook used by the binary to redraw Ratatui.
///
/// The hook receives an immutable reducer reference and a copy of controller
/// status after each reducer event. It must remain presentation-only: backend
/// calls belong in reducer effects, and the hook must not call back into the
/// controller. Keeping the callback inside `run` means no second task can own
/// or mutate the reducer concurrently.
pub type RenderHook<R> = Box<dyn FnMut(&R, &ControllerStatus) + Send + 'static>;

/// Controller limits and scheduling knobs.  All read sizes are bounded again
/// at the effect boundary so a buggy reducer cannot request an unbounded log or
/// transcript page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerConfig {
    pub tick_interval: Duration,
    pub refresh_interval: Duration,
    pub activity_interval: Duration,
    pub snapshot_limits: SnapshotLimits,
    pub max_activity_entries: usize,
    pub max_in_flight_commands: usize,
    pub shutdown_timeout: Duration,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            tick_interval: Duration::from_millis(100),
            refresh_interval: Duration::from_millis(250),
            activity_interval: Duration::from_millis(100),
            snapshot_limits: SnapshotLimits::default(),
            max_activity_entries: 128,
            max_in_flight_commands: 8,
            shutdown_timeout: Duration::from_secs(5),
        }
    }
}

impl ControllerConfig {
    fn normalized(mut self) -> Self {
        if self.tick_interval.is_zero() {
            self.tick_interval = Duration::from_millis(1);
        }
        if self.refresh_interval.is_zero() {
            self.refresh_interval = self.tick_interval;
        }
        if self.activity_interval.is_zero() {
            self.activity_interval = self.tick_interval;
        }
        if self.shutdown_timeout.is_zero() {
            self.shutdown_timeout = Duration::from_millis(1);
        }
        self.snapshot_limits = self.snapshot_limits.bounded();
        self.max_activity_entries = self.max_activity_entries.clamp(1, 2_000);
        self.max_in_flight_commands = self.max_in_flight_commands.clamp(1, 256);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControllerLifecycle {
    Running,
    ShuttingDown,
    Forced,
}

/// Read-only operational state, useful to a view/status line and controller
/// tests without exposing internal task handles.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerStatus {
    pub lifecycle: ControllerLifecycle,
    pub accepting_input: bool,
    pub pending_commands: usize,
    pub pending_activity_reads: usize,
    pub refresh_in_flight: bool,
    pub shutdown_intent: Option<ShutdownIntent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControllerExit {
    Graceful(ShutdownOutcome),
    Forced { active_operations: usize },
    ShutdownFailed(BackendError),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ActivityKey {
    project_id: String,
    turn_id: Option<String>,
    task_id: Option<String>,
    execution_id: String,
    attempt: u32,
}

impl From<&ActivityTarget> for ActivityKey {
    fn from(target: &ActivityTarget) -> Self {
        Self {
            project_id: target.project_id.clone(),
            turn_id: target.turn_id.clone(),
            task_id: target.task_id.clone(),
            execution_id: target.execution_id.clone(),
            attempt: target.attempt,
        }
    }
}

struct PendingCommand {
    key: IdempotencyKey,
    handle: JoinHandle<()>,
}

struct PendingActivity {
    handle: JoinHandle<()>,
}

enum InternalEvent {
    Snapshot {
        request: SnapshotRequest,
        result: BackendResult<Box<SoloSnapshot>>,
    },
    Activity {
        request: ActivityReadRequest,
        result: BackendResult<Box<ActivityBatch>>,
    },
    Command {
        id: u64,
        command: BackendCommand,
        result: BackendResult<Box<BackendCommandResult>>,
    },
    Shutdown {
        intent: ShutdownIntent,
        result: BackendResult<ShutdownOutcome>,
    },
}

enum PollResult {
    Input(InputSourcePoll),
    Backend(BackendEventPoll),
    Internal(Option<InternalEvent>),
    Tick,
}

/// Reducer-driven Tokio loop for one bound Solo backend.
pub struct SoloController<B, I, E, R>
where
    B: SoloBackend,
    I: TerminalEventSource,
    E: BackendEventSource,
    R: SoloReducer,
{
    backend: Arc<B>,
    input: I,
    events: E,
    reducer: R,
    config: ControllerConfig,
    lifecycle: ControllerLifecycle,
    accepting_input: bool,
    input_open: bool,
    events_open: bool,
    refresh_pending: bool,
    refresh_handle: Option<JoinHandle<()>>,
    activity: HashMap<ActivityKey, PendingActivity>,
    activity_pending: HashMap<ActivityKey, ActivityReadRequest>,
    commands: HashMap<u64, PendingCommand>,
    command_keys: HashSet<IdempotencyKey>,
    next_command_id: u64,
    shutdown_handle: Option<JoinHandle<()>>,
    shutdown_intent: Option<ShutdownIntent>,
    shutdown_started: Option<Instant>,
    last_refresh_scheduled: Option<Instant>,
    last_activity_poll: Option<Instant>,
    completion_tx: mpsc::Sender<InternalEvent>,
    completion_rx: mpsc::Receiver<InternalEvent>,
    render_hook: Option<RenderHook<R>>,
}

impl<B, I, E, R> SoloController<B, I, E, R>
where
    B: SoloBackend,
    I: TerminalEventSource,
    E: BackendEventSource,
    R: SoloReducer,
{
    pub fn new(backend: B, input: I, events: E, reducer: R, config: ControllerConfig) -> Self {
        Self::new_shared(Arc::new(backend), input, events, reducer, config)
    }

    pub fn new_shared(
        backend: Arc<B>,
        input: I,
        events: E,
        reducer: R,
        config: ControllerConfig,
    ) -> Self {
        let (completion_tx, completion_rx) = mpsc::channel(128);
        Self {
            backend,
            input,
            events,
            reducer,
            config: config.normalized(),
            lifecycle: ControllerLifecycle::Running,
            accepting_input: false,
            input_open: true,
            events_open: true,
            refresh_pending: false,
            refresh_handle: None,
            activity: HashMap::new(),
            activity_pending: HashMap::new(),
            commands: HashMap::new(),
            command_keys: HashSet::new(),
            next_command_id: 1,
            shutdown_handle: None,
            shutdown_intent: None,
            shutdown_started: None,
            last_refresh_scheduled: None,
            last_activity_poll: None,
            completion_tx,
            completion_rx,
            render_hook: None,
        }
    }

    /// Install a synchronous redraw hook and return the controller for fluent
    /// setup. The callback runs on the controller task after reducer events.
    pub fn with_render_hook<F>(mut self, hook: F) -> Self
    where
        F: FnMut(&R, &ControllerStatus) + Send + 'static,
    {
        self.render_hook = Some(Box::new(hook));
        self
    }

    /// Replace or clear the redraw hook after construction.
    pub fn set_render_hook(&mut self, hook: Option<RenderHook<R>>) {
        self.render_hook = hook;
    }

    pub fn reducer(&self) -> &R {
        &self.reducer
    }

    pub fn reducer_mut(&mut self) -> &mut R {
        &mut self.reducer
    }

    pub fn backend(&self) -> Arc<B> {
        Arc::clone(&self.backend)
    }

    pub fn status(&self) -> ControllerStatus {
        ControllerStatus {
            lifecycle: self.lifecycle,
            accepting_input: self.accepting_input,
            pending_commands: self.commands.len(),
            pending_activity_reads: self.activity.len(),
            refresh_in_flight: self.refresh_handle.is_some(),
            shutdown_intent: self.shutdown_intent,
        }
    }

    /// Run until the reducer requests graceful shutdown, the terminal emits a
    /// close, or a second interrupt forces exit.  The first durable snapshot is
    /// fetched before input is admitted, ensuring restart/recovery state is
    /// visible before a new message can be sent.
    pub async fn run(&mut self) -> ControllerExit {
        self.schedule_refresh();
        let mut ticker = time::interval(self.config.tick_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            if let Some(started) = self.shutdown_started {
                if started.elapsed() >= self.config.shutdown_timeout {
                    let active = self.active_operation_count();
                    self.abort_in_flight();
                    self.lifecycle = ControllerLifecycle::Forced;
                    return ControllerExit::Graceful(ShutdownOutcome::timed_out(active));
                }
            }

            let poll = tokio::select! {
                input = self.input.recv(), if self.input_open && (self.accepting_input || self.lifecycle == ControllerLifecycle::ShuttingDown) => {
                    PollResult::Input(input)
                }
                event = self.events.recv(), if self.events_open => {
                    PollResult::Backend(event)
                }
                internal = self.completion_rx.recv() => {
                    PollResult::Internal(internal)
                }
                _ = ticker.tick() => PollResult::Tick,
            };

            match poll {
                PollResult::Input(input) => {
                    if let Some(exit) = self.handle_input(input).await {
                        return exit;
                    }
                }
                PollResult::Backend(event) => {
                    self.handle_backend_event(event).await;
                }
                PollResult::Internal(internal) => {
                    if let Some(internal) = internal {
                        if let Some(exit) = self.handle_internal(internal).await {
                            return exit;
                        }
                    }
                }
                PollResult::Tick => {
                    let now = Instant::now();
                    let mut events = Vec::new();
                    if self
                        .last_refresh_scheduled
                        .is_none_or(|last| now.duration_since(last) >= self.config.refresh_interval)
                    {
                        self.schedule_refresh();
                    }
                    if self.last_activity_poll.is_none_or(|last| {
                        now.duration_since(last) >= self.config.activity_interval
                    }) {
                        self.last_activity_poll = Some(now);
                        events.push(ControllerEvent::ActivityPollDue);
                    }
                    events.push(ControllerEvent::Tick);
                    self.reduce_events(events).await;
                }
            }

            if self.lifecycle == ControllerLifecycle::Forced {
                return ControllerExit::Forced {
                    active_operations: self.active_operation_count(),
                };
            }
        }
    }

    async fn handle_input(&mut self, poll: InputSourcePoll) -> Option<ControllerExit> {
        match poll {
            InputSourcePoll::Event(InputEvent::Interrupt)
                if self.lifecycle == ControllerLifecycle::ShuttingDown =>
            {
                let active = self.active_operation_count();
                self.abort_in_flight();
                self.lifecycle = ControllerLifecycle::Forced;
                Some(ControllerExit::Forced {
                    active_operations: active,
                })
            }
            InputSourcePoll::Event(event) => {
                self.reduce_events(vec![ControllerEvent::Input(event)])
                    .await;
                None
            }
            InputSourcePoll::Closed => {
                self.input_open = false;
                if self.lifecycle == ControllerLifecycle::Running {
                    self.reduce_events(vec![ControllerEvent::Input(InputEvent::Closed)])
                        .await;
                    if self.lifecycle == ControllerLifecycle::Running {
                        self.begin_shutdown(ShutdownIntent::InputClosed);
                    }
                }
                None
            }
            InputSourcePoll::Unavailable { detail } => {
                self.input_open = false;
                self.reduce_events(vec![ControllerEvent::BackendError(
                    BackendError::unavailable(detail),
                )])
                .await;
                if self.lifecycle == ControllerLifecycle::Running {
                    self.begin_shutdown(ShutdownIntent::InputClosed);
                }
                None
            }
        }
    }

    async fn handle_backend_event(&mut self, poll: BackendEventPoll) {
        match poll {
            BackendEventPoll::Event(event) => {
                let expected = self.backend.scope();
                if !expected.same_project(&event.scope) {
                    self.reduce_events(vec![ControllerEvent::BackendError(
                        BackendError::scope_violation(
                            "received an invalidation for another Solo Project",
                        ),
                    )])
                    .await;
                    return;
                }
                if let BackendEventKind::ActivityAvailable { target } = &event.kind {
                    if let Err(error) = ensure_activity_scope(&expected, target) {
                        self.reduce_events(vec![ControllerEvent::BackendError(error)])
                            .await;
                        return;
                    }
                }
                let should_refresh = matches!(
                    &event.kind,
                    BackendEventKind::SnapshotInvalidated { .. }
                        | BackendEventKind::RuntimeStateChanged { .. }
                );
                self.reduce_events(vec![ControllerEvent::Backend(event)])
                    .await;
                if should_refresh {
                    self.schedule_refresh();
                }
            }
            BackendEventPoll::Lagged { skipped } => {
                self.reduce_events(vec![ControllerEvent::BackendLagged { skipped }])
                    .await;
                self.schedule_refresh();
            }
            BackendEventPoll::Closed => {
                self.events_open = false;
                self.reduce_events(vec![ControllerEvent::BackendSourceClosed])
                    .await;
            }
            BackendEventPoll::Unavailable { detail } => {
                self.events_open = false;
                self.reduce_events(vec![ControllerEvent::BackendSourceUnavailable { detail }])
                    .await;
                self.schedule_refresh();
            }
        }
    }

    async fn handle_internal(&mut self, internal: InternalEvent) -> Option<ControllerExit> {
        match internal {
            InternalEvent::Snapshot { request, result } => {
                self.refresh_handle = None;
                let result = result.map(|snapshot| *snapshot).and_then(|snapshot| {
                    ensure_snapshot_scope(&self.backend.scope(), &snapshot).map(|()| snapshot)
                });
                // Input is admitted only after the first authoritative read
                // succeeds and passes the scope checks above.  A failed
                // startup refresh must not let a user mutate an unknown
                // Project or make the controller depend on a stale model.
                if result.is_ok() && self.lifecycle == ControllerLifecycle::Running {
                    self.accepting_input = true;
                }
                self.reduce_events(vec![ControllerEvent::SnapshotUpdated { request, result }])
                    .await;
                if self.refresh_pending {
                    self.refresh_pending = false;
                    self.schedule_refresh();
                }
            }
            InternalEvent::Activity { request, result } => {
                let key = ActivityKey::from(&request.target);
                self.activity.remove(&key);
                let result = result.map(|batch| *batch).and_then(|batch| {
                    ensure_activity_scope(&self.backend.scope(), &batch.target).and_then(|()| {
                        if ActivityKey::from(&batch.target) == key {
                            Ok(batch)
                        } else {
                            Err(BackendError::invalid_input(
                                "backend returned activity for a different execution target",
                            ))
                        }
                    })
                });
                self.reduce_events(vec![ControllerEvent::ActivityUpdated { request, result }])
                    .await;
                if let Some(next) = self.activity_pending.remove(&key) {
                    self.schedule_activity(next);
                }
            }
            InternalEvent::Command {
                id,
                command,
                result,
            } => {
                let result = result.map(|result| *result);
                let refresh_after_conflict =
                    result.as_ref().is_err_and(|error| error.is_conflict());
                if let Some(pending) = self.commands.remove(&id) {
                    self.command_keys.remove(&pending.key);
                    pending.handle.abort();
                }
                self.reduce_events(vec![ControllerEvent::CommandCompleted {
                    id,
                    command,
                    result,
                }])
                .await;
                if refresh_after_conflict {
                    self.schedule_refresh();
                }
            }
            InternalEvent::Shutdown { intent, result } => {
                self.shutdown_handle = None;
                let exit = match result.clone() {
                    Ok(outcome) => {
                        self.reduce_events(vec![ControllerEvent::ShutdownCompleted {
                            intent,
                            result,
                        }])
                        .await;
                        self.abort_in_flight();
                        Some(ControllerExit::Graceful(outcome))
                    }
                    Err(error) => {
                        self.reduce_events(vec![ControllerEvent::ShutdownCompleted {
                            intent,
                            result: Err(error.clone()),
                        }])
                        .await;
                        self.abort_in_flight();
                        Some(ControllerExit::ShutdownFailed(error))
                    }
                };
                self.lifecycle = ControllerLifecycle::ShuttingDown;
                return exit;
            }
        }
        None
    }

    async fn reduce_events(&mut self, events: Vec<ControllerEvent>) {
        let mut queue = VecDeque::from(events);
        while let Some(event) = queue.pop_front() {
            // A reducer may surface an effect rejection as a notification.
            // Do not immediately retry the same rejected command from that
            // notification, otherwise a full command queue can spin forever.
            let rejection_event = matches!(&event, ControllerEvent::EffectRejected { .. });
            let effects = self.reducer.reduce(event);
            self.notify_state_change();
            for effect in effects {
                if rejection_event && matches!(effect, ControllerEffect::Command(_)) {
                    continue;
                }
                if let Some(rejected) = self.apply_effect(effect) {
                    queue.push_back(rejected);
                }
            }
        }
    }

    fn apply_effect(&mut self, effect: ControllerEffect) -> Option<ControllerEvent> {
        match effect {
            ControllerEffect::Command(command) => self.schedule_command(command),
            ControllerEffect::Refresh => {
                self.schedule_refresh();
                None
            }
            ControllerEffect::ReadActivity(request) => {
                self.schedule_activity(request);
                None
            }
            ControllerEffect::RequestShutdown(intent) => {
                self.begin_shutdown(intent);
                None
            }
            ControllerEffect::Noop => None,
        }
    }

    fn notify_state_change(&mut self) {
        let status = self.status();
        if let Some(hook) = self.render_hook.as_mut() {
            hook(&self.reducer, &status);
        }
    }

    fn schedule_refresh(&mut self) {
        if self.lifecycle != ControllerLifecycle::Running {
            return;
        }
        if self.refresh_handle.is_some() {
            self.refresh_pending = true;
            return;
        }

        let request = SnapshotRequest {
            limits: self.config.snapshot_limits,
        }
        .bounded();
        let backend = Arc::clone(&self.backend);
        let sender = self.completion_tx.clone();
        self.last_refresh_scheduled = Some(Instant::now());
        self.refresh_handle = Some(tokio::spawn(async move {
            let result = backend.refresh(request).await.map(Box::new);
            let _ = sender
                .send(InternalEvent::Snapshot { request, result })
                .await;
        }));
    }

    fn schedule_activity(&mut self, request: ActivityReadRequest) {
        if self.lifecycle != ControllerLifecycle::Running {
            return;
        }
        let request = request.bounded(self.config.max_activity_entries);
        let key = ActivityKey::from(&request.target);
        if let Some(existing) = self.activity.get(&key) {
            let _ = existing;
            self.activity_pending.insert(key, request);
            return;
        }
        let backend = Arc::clone(&self.backend);
        let sender = self.completion_tx.clone();
        let request_for_task = request.clone();
        let handle = tokio::spawn(async move {
            let result = backend
                .read_activity(request_for_task.clone())
                .await
                .map(Box::new);
            let _ = sender
                .send(InternalEvent::Activity {
                    request: request_for_task,
                    result,
                })
                .await;
        });
        self.activity.insert(key, PendingActivity { handle });
    }

    fn schedule_command(&mut self, command: BackendCommand) -> Option<ControllerEvent> {
        if self.lifecycle != ControllerLifecycle::Running {
            return Some(ControllerEvent::EffectRejected {
                effect: ControllerEffect::Command(command),
                error: BackendError::unavailable("Solo is shutting down"),
            });
        }
        let key = command.idempotency_key().clone();
        if key.is_empty() {
            return Some(ControllerEvent::EffectRejected {
                effect: ControllerEffect::Command(command),
                error: BackendError::invalid_input("mutation requires a stable idempotency key"),
            });
        }
        // A repeated send while its result is unknown is intentionally
        // coalesced here; the same key is also passed to the backend so a
        // retry after completion remains idempotent at the durable boundary.
        if self.command_keys.contains(&key) {
            return None;
        }
        if self.commands.len() >= self.config.max_in_flight_commands {
            return Some(ControllerEvent::EffectRejected {
                effect: ControllerEffect::Command(command),
                error: BackendError::unavailable("too many Solo commands are in flight"),
            });
        }

        let id = self.next_command_id;
        self.next_command_id = self.next_command_id.wrapping_add(1).max(1);
        let backend = Arc::clone(&self.backend);
        let sender = self.completion_tx.clone();
        let command_for_task = command.clone();
        let handle = tokio::spawn(async move {
            let result = backend
                .execute(command_for_task.clone())
                .await
                .map(Box::new);
            let _ = sender
                .send(InternalEvent::Command {
                    id,
                    command: command_for_task,
                    result,
                })
                .await;
        });
        self.command_keys.insert(key.clone());
        self.commands.insert(id, PendingCommand { key, handle });
        None
    }

    fn begin_shutdown(&mut self, intent: ShutdownIntent) {
        if self.lifecycle != ControllerLifecycle::Running {
            return;
        }
        self.lifecycle = ControllerLifecycle::ShuttingDown;
        self.accepting_input = false;
        self.shutdown_intent = Some(intent);
        self.shutdown_started = Some(Instant::now());
        let backend = Arc::clone(&self.backend);
        let sender = self.completion_tx.clone();
        let deadline = self.config.shutdown_timeout;
        self.shutdown_handle = Some(tokio::spawn(async move {
            let result = backend.shutdown(intent, deadline).await;
            let _ = sender
                .send(InternalEvent::Shutdown { intent, result })
                .await;
        }));
    }

    fn active_operation_count(&self) -> usize {
        self.commands.len()
            + self.activity.len()
            + usize::from(self.refresh_handle.is_some())
            + usize::from(self.shutdown_handle.is_some())
    }

    fn abort_in_flight(&mut self) {
        for (_, pending) in self.commands.drain() {
            pending.handle.abort();
        }
        self.command_keys.clear();
        for (_, pending) in self.activity.drain() {
            pending.handle.abort();
        }
        self.activity_pending.clear();
        if let Some(handle) = self.refresh_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.shutdown_handle.take() {
            handle.abort();
        }
    }
}

/// Convenience adapter for callers that already have a channel-backed event
/// source but want the concrete type visible in signatures.
pub type SoloChannelEventSource = ChannelBackendEventSource;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{
        ActivityCursor, ActivityEntry, ActivityKind, AgentSnapshot, BackendErrorKind, ChatSnapshot,
        CheckSnapshot, CommitEvidence, MessageRole, MessageSnapshot, MessageStatus,
        ProjectReadiness, ProjectSnapshot, RepositorySnapshot, RuntimeState, SoloScope,
        TaskSnapshot, TaskState, TurnSnapshot, TurnState,
    };
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex,
    };

    fn scope() -> SoloScope {
        SoloScope::new("owner", "project", "repo", "chat")
    }

    fn snapshot() -> SoloSnapshot {
        SoloSnapshot {
            scope: scope(),
            repository: RepositorySnapshot {
                id: "repo".into(),
                name: "repo".into(),
                root: "/repo".into(),
                default_branch: "main".into(),
            },
            project: ProjectSnapshot {
                id: "project".into(),
                name: "Project".into(),
                readiness: ProjectReadiness::Operational,
                runtime: RuntimeState::Ready,
                workflow: "autonomous_v1".into(),
                selected_agent: Some(AgentSnapshot {
                    id: "agent".into(),
                    name: "Codex".into(),
                    harness: "codex".into(),
                }),
            },
            chat: ChatSnapshot {
                id: "chat".into(),
                messages: vec![MessageSnapshot {
                    id: "message".into(),
                    sequence: 0,
                    role: MessageRole::Assistant,
                    content: "ready".into(),
                    created_at: "now".into(),
                    turn_id: None,
                    status: MessageStatus::Complete,
                }],
                has_older_messages: false,
                active_turns: Vec::new(),
                interactions: Vec::new(),
            },
            setup_agents: Vec::new(),
            live_activity: Vec::new(),
            tasks: vec![TaskSnapshot {
                id: "task".into(),
                title: "task".into(),
                state: TaskState::Done,
                version: 1,
                worker: None,
                reviewer: None,
                checks: vec![CheckSnapshot {
                    name: "tests".into(),
                    status: crate::backend::CheckStatus::Passed,
                    summary: None,
                }],
                commit: Some(CommitEvidence {
                    commit: Some("abc".into()),
                    changed_files: vec!["file".into()],
                    merged: true,
                    summary: None,
                }),
                blocker: None,
                retryable: false,
            }],
            attention: Vec::new(),
            approvals: Vec::new(),
            refreshed_at: "now".into(),
        }
    }

    struct FakeBackend {
        scope: SoloScope,
        refreshes: AtomicUsize,
        refresh_failures: AtomicUsize,
        activity_limits: Mutex<Vec<usize>>,
        commands: Mutex<Vec<BackendCommand>>,
        shutdown_intents: Mutex<Vec<ShutdownIntent>>,
        gate_commands: AtomicBool,
        conflict_commands: AtomicBool,
    }

    impl FakeBackend {
        fn new() -> Self {
            Self {
                scope: scope(),
                refreshes: AtomicUsize::new(0),
                refresh_failures: AtomicUsize::new(0),
                activity_limits: Mutex::new(Vec::new()),
                commands: Mutex::new(Vec::new()),
                shutdown_intents: Mutex::new(Vec::new()),
                gate_commands: AtomicBool::new(false),
                conflict_commands: AtomicBool::new(false),
            }
        }
    }

    impl SoloBackend for FakeBackend {
        fn scope(&self) -> SoloScope {
            self.scope.clone()
        }

        fn refresh(
            &self,
            _request: SnapshotRequest,
        ) -> BackendFuture<'_, BackendResult<SoloSnapshot>> {
            self.refreshes.fetch_add(1, Ordering::SeqCst);
            if self.refresh_failures.load(Ordering::SeqCst) > 0 {
                self.refresh_failures.fetch_sub(1, Ordering::SeqCst);
                return Box::pin(async {
                    Err(BackendError::unavailable("startup snapshot unavailable"))
                });
            }
            let snapshot = snapshot();
            Box::pin(async move { Ok(snapshot) })
        }

        fn read_activity(
            &self,
            request: ActivityReadRequest,
        ) -> BackendFuture<'_, BackendResult<ActivityBatch>> {
            self.activity_limits.lock().unwrap().push(request.limit);
            let target = request.target.clone();
            let next_cursor = ActivityCursor {
                next_sequence: request.cursor.next_sequence + request.limit as u64,
                ..request.cursor.clone()
            };
            Box::pin(async move {
                Ok(ActivityBatch {
                    target,
                    entries: vec![ActivityEntry {
                        sequence: request.cursor.next_sequence,
                        attempt: request.target.attempt,
                        kind: ActivityKind::System,
                        summary: "ok".into(),
                        preview: None,
                    }],
                    next_cursor,
                    has_more: false,
                    cursor_reset: false,
                    finished: true,
                })
            })
        }

        fn execute(
            &self,
            command: BackendCommand,
        ) -> BackendFuture<'_, BackendResult<BackendCommandResult>> {
            self.commands.lock().unwrap().push(command.clone());
            let gate = self.gate_commands.load(Ordering::SeqCst);
            let conflict = self.conflict_commands.load(Ordering::SeqCst);
            let turn = TurnSnapshot {
                id: "turn".into(),
                triggering_message_id: "message".into(),
                state: TurnState::Succeeded,
                version: 2,
                attempt: 1,
                reply: None,
                assistant_message_id: Some("assistant".into()),
                failure: None,
                retryable: false,
            };
            Box::pin(async move {
                if gate {
                    time::sleep(Duration::from_secs(60)).await;
                }
                if conflict {
                    return Err(BackendError::conflict("stale turn", None));
                }
                Ok(BackendCommandResult::TurnRetried {
                    turn,
                    replayed: false,
                })
            })
        }

        fn shutdown(
            &self,
            intent: ShutdownIntent,
            _deadline: Duration,
        ) -> BackendFuture<'_, BackendResult<ShutdownOutcome>> {
            self.shutdown_intents.lock().unwrap().push(intent);
            let gate = self.gate_commands.load(Ordering::SeqCst);
            Box::pin(async move {
                if gate {
                    time::sleep(Duration::from_secs(60)).await;
                }
                Ok(ShutdownOutcome::completed())
            })
        }
    }

    struct RecordingReducer {
        events: Vec<ControllerEvent>,
        effects: VecDeque<ControllerEffect>,
        emit_send_on_input: bool,
        emit_activity_on_poll: bool,
        draft: String,
    }

    impl RecordingReducer {
        fn new() -> Self {
            Self {
                events: Vec::new(),
                effects: VecDeque::new(),
                emit_send_on_input: false,
                emit_activity_on_poll: false,
                draft: "draft".into(),
            }
        }
    }

    impl SoloReducer for RecordingReducer {
        fn reduce(&mut self, event: ControllerEvent) -> Vec<ControllerEffect> {
            let mut effects = Vec::new();
            if let ControllerEvent::Input(InputEvent::Key(KeyEvent {
                code: KeyCode::Enter,
                ..
            })) = event
            {
                if self.emit_send_on_input {
                    effects.push(ControllerEffect::Command(BackendCommand::SendMessage(
                        crate::backend::SendMessageRequest::new(
                            self.draft.clone(),
                            IdempotencyKey::from("send-1"),
                        ),
                    )));
                }
            }
            if matches!(event, ControllerEvent::ActivityPollDue) && self.emit_activity_on_poll {
                let target = ActivityTarget {
                    project_id: "project".into(),
                    turn_id: Some("turn".into()),
                    task_id: None,
                    execution_id: "execution".into(),
                    attempt: 1,
                };
                effects.push(ControllerEffect::ReadActivity(ActivityReadRequest {
                    cursor: ActivityCursor::beginning(&target),
                    target,
                    limit: usize::MAX,
                }));
            }
            if matches!(event, ControllerEvent::Input(InputEvent::Quit)) {
                effects.push(ControllerEffect::RequestShutdown(ShutdownIntent::UserQuit));
            }
            if !self.effects.is_empty() {
                effects.extend(self.effects.drain(..));
            }
            self.events.push(event);
            effects
        }
    }

    struct UnavailableInput {
        detail: String,
    }

    impl TerminalEventSource for UnavailableInput {
        fn recv(&mut self) -> BackendFuture<'_, InputSourcePoll> {
            let detail = self.detail.clone();
            Box::pin(async move {
                // Keep this source permanently unavailable.  That makes the
                // test independent of select! branch tie-breaking while
                // still exercising the one-shot shutdown transition.
                time::sleep(Duration::from_millis(1)).await;
                InputSourcePoll::Unavailable { detail }
            })
        }
    }

    #[tokio::test]
    async fn initial_refresh_precedes_input_and_send_is_deduplicated_while_in_flight() {
        let backend = FakeBackend::new();
        backend.gate_commands.store(true, Ordering::SeqCst);
        let (input_tx, input) = channel_input_source(16);
        let (_event_tx, events) = crate::backend::backend_event_channel(16);
        let mut reducer = RecordingReducer::new();
        reducer.emit_send_on_input = true;
        let mut controller = SoloController::new(
            backend,
            input,
            events,
            reducer,
            ControllerConfig {
                tick_interval: Duration::from_millis(10),
                refresh_interval: Duration::from_secs(10),
                activity_interval: Duration::from_secs(10),
                shutdown_timeout: Duration::from_millis(100),
                ..ControllerConfig::default()
            },
        );
        let input_task = tokio::spawn(async move {
            input_tx
                .send(InputEvent::Key(KeyEvent::new(KeyCode::Enter)))
                .await
                .unwrap();
            input_tx.send(InputEvent::Quit).await.unwrap();
        });
        let run = tokio::spawn(async move { controller.run().await });
        // The first refresh is immediate, so input is admitted before the
        // command is sent.  The command remains gated and shutdown times out.
        let exit = run.await.unwrap();
        input_task.await.unwrap();
        assert!(matches!(exit, ControllerExit::Graceful(outcome) if outcome.timed_out));
    }

    #[tokio::test]
    async fn failed_initial_snapshot_keeps_input_closed_until_a_valid_refresh() {
        let backend = FakeBackend::new();
        backend.refresh_failures.store(1, Ordering::SeqCst);
        let (input_tx, input) = channel_input_source(8);
        let (_event_tx, events) = crate::backend::backend_event_channel(8);
        let controller = SoloController::new(
            backend,
            input,
            events,
            RecordingReducer::new(),
            ControllerConfig {
                tick_interval: Duration::from_millis(5),
                refresh_interval: Duration::from_millis(5),
                activity_interval: Duration::from_secs(60),
                ..ControllerConfig::default()
            },
        );
        input_tx
            .send(InputEvent::Resize {
                width: 80,
                height: 24,
            })
            .await
            .unwrap();
        input_tx.send(InputEvent::Quit).await.unwrap();

        let mut controller = controller;
        let exit = controller.run().await;
        assert!(matches!(exit, ControllerExit::Graceful(_)));
        let events = &controller.reducer().events;
        let failed_snapshot = events
            .iter()
            .position(|event| {
                matches!(
                    event,
                    ControllerEvent::SnapshotUpdated { result: Err(_), .. }
                )
            })
            .expect("failed initial snapshot should reach reducer");
        let resize = events
            .iter()
            .position(|event| matches!(event, ControllerEvent::Input(InputEvent::Resize { .. })))
            .expect("resize should eventually be admitted");
        assert!(resize > failed_snapshot);
    }

    #[tokio::test]
    async fn unavailable_input_surfaces_error_and_requests_bounded_shutdown() {
        let backend = FakeBackend::new();
        let (_event_tx, events) = crate::backend::backend_event_channel(8);
        let input = UnavailableInput {
            detail: "terminal read failed".into(),
        };
        let mut controller = SoloController::new(
            backend,
            input,
            events,
            RecordingReducer::new(),
            ControllerConfig {
                tick_interval: Duration::from_millis(5),
                refresh_interval: Duration::from_secs(60),
                activity_interval: Duration::from_secs(60),
                shutdown_timeout: Duration::from_millis(100),
                ..ControllerConfig::default()
            },
        );
        let exit = controller.run().await;
        assert!(matches!(
            exit,
            ControllerExit::Graceful(ShutdownOutcome {
                completed: true,
                timed_out: false,
                ..
            })
        ));
        assert_eq!(
            controller
                .backend()
                .shutdown_intents
                .lock()
                .unwrap()
                .as_slice(),
            &[ShutdownIntent::InputClosed]
        );
        assert!(controller.reducer().events.iter().any(|event| {
            matches!(
                event,
                ControllerEvent::BackendError(error)
                    if error.kind == BackendErrorKind::Unavailable
            )
        }));
    }

    #[tokio::test]
    async fn invalidation_refreshes_and_activity_reads_are_bounded() {
        let backend = FakeBackend::new();
        let (input_tx, input) = channel_input_source(8);
        let (event_tx, events) = crate::backend::backend_event_channel(8);
        let mut reducer = RecordingReducer::new();
        reducer.emit_activity_on_poll = true;
        let mut controller = SoloController::new(
            backend,
            input,
            events,
            reducer,
            ControllerConfig {
                tick_interval: Duration::from_millis(5),
                refresh_interval: Duration::from_secs(60),
                activity_interval: Duration::from_millis(5),
                max_activity_entries: 4,
                ..ControllerConfig::default()
            },
        );
        let scope = scope();
        event_tx
            .send(BackendEvent::invalidate(
                scope,
                1,
                crate::backend::InvalidationReason::Task,
            ))
            .await
            .unwrap();
        let quit_task = tokio::spawn(async move {
            time::sleep(Duration::from_millis(30)).await;
            input_tx.send(InputEvent::Quit).await.unwrap();
        });
        let exit = controller.run().await;
        quit_task.await.unwrap();
        assert!(matches!(exit, ControllerExit::Graceful(_)));
        let limits = controller.backend().activity_limits.lock().unwrap().clone();
        assert!(!limits.is_empty());
        assert!(limits.iter().all(|limit| *limit <= 4));
        assert!(controller.backend().refreshes.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn conflict_events_reach_reducer_without_losing_draft_or_shutdown_intent() {
        let backend = FakeBackend::new();
        backend.conflict_commands.store(true, Ordering::SeqCst);
        let (input_tx, input) = channel_input_source(8);
        let (_event_tx, events) = crate::backend::backend_event_channel(8);
        let mut reducer = RecordingReducer::new();
        reducer
            .effects
            .push_back(ControllerEffect::Command(BackendCommand::CancelTurn(
                crate::backend::TurnMutationRequest {
                    turn_id: "turn".into(),
                    expected_version: 3,
                    idempotency_key: IdempotencyKey::from("cancel-3"),
                },
            )));
        let mut controller =
            SoloController::new(backend, input, events, reducer, ControllerConfig::default());
        let quit_task = tokio::spawn(async move {
            time::sleep(Duration::from_millis(30)).await;
            input_tx.send(InputEvent::Quit).await.unwrap();
        });
        let exit = controller.run().await;
        quit_task.await.unwrap();
        assert!(matches!(exit, ControllerExit::Graceful(_)));
        assert_eq!(controller.reducer().draft, "draft");
        assert!(controller.reducer().events.iter().any(|event| {
            matches!(
                event,
                ControllerEvent::CommandCompleted {
                    result: Err(error), ..
                } if error.is_conflict()
            )
        }));
        assert!(controller.backend().refreshes.load(Ordering::SeqCst) >= 2);
        assert!(controller
            .backend()
            .commands
            .lock()
            .unwrap()
            .iter()
            .any(|command| matches!(command, BackendCommand::CancelTurn(request) if request.expected_version == 3)));
        assert_eq!(
            controller
                .backend()
                .shutdown_intents
                .lock()
                .unwrap()
                .as_slice(),
            &[ShutdownIntent::UserQuit]
        );
    }

    #[tokio::test]
    async fn resize_and_interrupt_are_dispatched_and_second_interrupt_forces_exit() {
        let backend = FakeBackend::new();
        backend.gate_commands.store(true, Ordering::SeqCst);
        let (input_tx, input) = channel_input_source(16);
        let (_event_tx, events) = crate::backend::backend_event_channel(8);
        let mut reducer = RecordingReducer::new();
        reducer.emit_send_on_input = true;
        let mut controller = SoloController::new(
            backend,
            input,
            events,
            reducer,
            ControllerConfig {
                shutdown_timeout: Duration::from_secs(60),
                ..ControllerConfig::default()
            },
        );
        let run = tokio::spawn(async move {
            input_tx
                .send(InputEvent::Resize {
                    width: 80,
                    height: 24,
                })
                .await
                .unwrap();
            input_tx
                .send(InputEvent::Key(KeyEvent::new(KeyCode::Enter)))
                .await
                .unwrap();
            input_tx.send(InputEvent::Quit).await.unwrap();
            input_tx.send(InputEvent::Interrupt).await.unwrap();
            controller.run().await
        });
        let exit = run.await.unwrap();
        assert!(matches!(exit, ControllerExit::Forced { .. }));
    }

    #[tokio::test]
    async fn render_hook_runs_after_reducer_events_on_controller_task() {
        let backend = FakeBackend::new();
        let (input_tx, input) = channel_input_source(8);
        let (_event_tx, events) = crate::backend::backend_event_channel(8);
        let redraws = Arc::new(AtomicUsize::new(0));
        let redraws_for_hook = Arc::clone(&redraws);
        let controller = SoloController::new(
            backend,
            input,
            events,
            RecordingReducer::new(),
            ControllerConfig::default(),
        )
        .with_render_hook(move |_reducer, status| {
            assert_ne!(status.lifecycle, ControllerLifecycle::Forced);
            redraws_for_hook.fetch_add(1, Ordering::SeqCst);
        });
        let mut controller = controller;
        input_tx.send(InputEvent::Quit).await.unwrap();
        let exit = controller.run().await;
        assert!(matches!(exit, ControllerExit::Graceful(_)));
        assert!(redraws.load(Ordering::SeqCst) >= 2);
    }

    #[test]
    fn conflict_error_is_typed_for_reducer_recovery() {
        let error = BackendError::conflict(
            "turn changed",
            Some(crate::backend::ConflictTarget {
                kind: "turn".into(),
                id: "turn".into(),
                expected_version: Some(3),
                expected_digest: None,
            }),
        );
        assert_eq!(error.kind, BackendErrorKind::Conflict);
        assert!(error.is_conflict());
    }
}
