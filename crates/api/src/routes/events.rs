use db::{AgentChatRepo, DomainEventRepo, IntegrationRepo, ProjectRepo, SqliteDb, TaskRepo};
use std::{
    collections::{HashMap, VecDeque},
    convert::Infallible,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    extract::State,
    http::HeaderMap,
    response::sse::{Event, KeepAlive, Sse},
};
use serde_json::json;
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

use crate::{routes::auth::AuthenticatedUser, state::AppState};

const MAX_REPLAY_EVENTS: i64 = 1_000;
const REPLAY_PAGE_SIZE: i64 = 100;
const DURABLE_ID_PREFIX: &str = "domain-event:";
/// How long one connection trusts a Project visibility answer. A membership
/// added or removed reaches open streams within this window.
const VISIBILITY_TTL: Duration = Duration::from_secs(10);
/// Bound on each per-connection lookup cache.
const AUDIENCE_CACHE_LIMIT: usize = 10_000;

enum Replay {
    None,
    Events {
        after: i64,
        through: i64,
    },
    Resync {
        reason: &'static str,
        reset_to: Option<i64>,
    },
}

pub async fn stream_events(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    headers: HeaderMap,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    // Register before the snapshot so an append during replay is still live.
    // Plain and unrecognized connects are live-only and never read the ledger.
    let receiver = state.event_bus.subscribe();
    let resume = headers
        .get("last-event-id")
        .and_then(|id| id.to_str().ok())
        .and_then(durable_sequence);
    let (replay, replay_through) = match resume {
        None => (Replay::None, None),
        Some(after) => match state.db.domain_event_head().await {
            Ok(through) if after > through => (
                Replay::Resync {
                    reason: "resume cursor beyond ledger head",
                    reset_to: Some(through),
                },
                Some(through),
            ),
            Ok(through) => {
                let replay = match state
                    .db
                    .domain_event_replay_exceeds_limit(after, through, MAX_REPLAY_EVENTS)
                    .await
                {
                    Ok(false) => Replay::Events { after, through },
                    Ok(true) => Replay::Resync {
                        reason: "replay limit exceeded",
                        reset_to: None,
                    },
                    Err(error) => {
                        tracing::warn!(%error, "SSE replay bound query failed");
                        Replay::Resync {
                            reason: "ledger replay failed",
                            reset_to: None,
                        }
                    }
                };
                (replay, Some(through))
            }
            Err(error) => {
                tracing::warn!(%error, "SSE replay snapshot failed");
                (
                    Replay::Resync {
                        reason: "ledger replay failed",
                        reset_to: None,
                    },
                    None,
                )
            }
        },
    };
    let replay = replay_events(Arc::clone(&state.db), replay);
    let mut shutdown = state.shutdown_signal.subscribe();
    let shutdown_requested = async move {
        if *shutdown.borrow_and_update() {
            return;
        }
        while shutdown.changed().await.is_ok() {
            if *shutdown.borrow_and_update() {
                return;
            }
        }
    };

    // Default message frames retain the canonical JSON envelope. Clients route
    // payload.event_type; SSE IDs are transport cursors, not entity references.
    let stream = BroadcastStream::new(receiver).filter_map(move |event| match event {
        Ok(event) => {
            if matches!(&event.context, events::EventContext::DomainEventCommitted { sequence, .. }
                if replay_through.is_some_and(|through| *sequence <= through))
            {
                return None;
            }
            Some(Item::Event(event))
        }
        Err(error) => Some(Item::Frame(resync_frame(&error.to_string(), None))),
    });
    // Replay and live frames pass the same per-subscriber filter: an event
    // of a Project the caller cannot see never reaches them.
    let audience = Arc::new(tokio::sync::Mutex::new(Audience::new(
        Arc::clone(&state.db),
        user.user_id,
    )));
    let frames = futures_util::StreamExt::filter_map(
        tokio_stream::StreamExt::chain(replay, stream),
        move |item| {
            let audience = Arc::clone(&audience);
            async move {
                match item {
                    Item::Frame(frame) => Some(Ok(frame)),
                    Item::Event(event) => {
                        if audience.lock().await.admits(&event).await {
                            frame(event).map(Ok)
                        } else {
                            None
                        }
                    }
                }
            }
        },
    );
    Sse::new(futures_util::StreamExt::take_until(
        frames,
        shutdown_requested,
    ))
    .keep_alive(KeepAlive::default())
}

/// A stream item before the subscriber filter: an event still to be judged,
/// or a transport frame (resync) every subscriber receives.
enum Item {
    Event(events::ForgeEvent),
    Frame(Event),
}

/// What an event is about, for deciding who may receive it.
enum Subject<'a> {
    Project(&'a str),
    Projects(&'a [String]),
    Task(&'a str),
    Chat(&'a str),
    Account(&'a str),
    Integration(&'a str),
    /// Instance-level events (Agents, profiles, daemons, operations status):
    /// no Project data, delivered to every subscriber as before.
    Unscoped,
}

fn subject(event: &events::ForgeEvent) -> Subject<'_> {
    use events::EventContext as C;
    match &event.context {
        C::TaskCreated { project_id, .. }
        | C::TaskStatusChanged { project_id, .. }
        | C::TaskAssigned { project_id, .. }
        | C::TaskBlocked { project_id, .. }
        | C::TaskUnblocked { project_id, .. }
        | C::ProjectAutonomyStalled { project_id, .. }
        | C::TaskFailed { project_id, .. }
        | C::TaskRestarted { project_id, .. }
        | C::TaskCancelled { project_id, .. }
        | C::TaskRecovered { project_id, .. }
        | C::RecoveryApplied { project_id, .. }
        | C::TaskUpdated { project_id, .. }
        | C::TaskDeleted { project_id, .. }
        | C::NotificationCreated { project_id, .. }
        | C::ProjectHookRunChanged { project_id, .. } => Subject::Project(project_id),
        C::TaskMoved(payload) => Subject::Project(&payload.project_id),
        C::ProjectCreated { .. }
        | C::ProjectUpdated { .. }
        | C::ProjectDeleted { .. }
        | C::ProjectPaused { .. }
        | C::ProjectResumed { .. } => Subject::Project(&event.entity_id),
        C::TaskAutoTransitioned { task_id, .. }
        | C::TaskRoleReassigned { task_id, .. }
        | C::TaskDependencySatisfied { task_id, .. }
        | C::ExecutionStarted { task_id, .. }
        | C::ExecutionLog { task_id, .. }
        | C::ExecutionCompleted { task_id, .. }
        | C::ExecutionFailed { task_id, .. }
        | C::ExecutionCancelled { task_id, .. }
        | C::TaskExecutionRetry { task_id, .. }
        | C::ExecutionStalled { task_id, .. }
        | C::ExecutionDaemonDisconnected { task_id, .. }
        | C::TaskExecutionCancelled { task_id, .. }
        | C::WorkspaceCreated { task_id, .. }
        | C::WorkspaceExecutionWaiting { task_id, .. }
        | C::WorkspaceCleaned { task_id, .. }
        | C::TaskTerminalSessionChanged { task_id, .. }
        | C::ReviewStarted { task_id, .. }
        | C::ReviewDecided { task_id, .. }
        | C::ReviewPassed { task_id, .. }
        | C::ReviewFailed { task_id, .. }
        | C::ReviewApproved { task_id, .. }
        | C::ReviewRejected { task_id, .. }
        | C::MergeStarted { task_id, .. }
        | C::MergeSucceeded { task_id, .. }
        | C::MergeFailed { task_id, .. }
        | C::CommentCreated { task_id, .. }
        | C::TaskMediaUploaded { task_id, .. }
        | C::TaskMediaDeleted { task_id, .. }
        | C::FollowUpDispatched { task_id, .. }
        | C::TaskRoleAgentDispatched { task_id, .. }
        | C::TaskAwaitingHuman { task_id, .. }
        | C::TransitionEffectFailed { task_id, .. }
        | C::TransitionGuardRejected { task_id, .. }
        | C::TransitionLoopDetected { task_id, .. }
        | C::TaskRoleNotified { task_id, .. }
        | C::ReconciliationEvent {
            task_id: Some(task_id),
            ..
        } => Subject::Task(task_id),
        C::IntegrationUpdated { project_ids, .. } => Subject::Projects(project_ids),
        C::AgentChatMessageAdmitted { chat_id, .. }
        | C::AgentChatTurnProgress { chat_id, .. }
        | C::AgentChatResponseCompleted { chat_id, .. }
        | C::AgentChatUpdated { chat_id, .. } => Subject::Chat(chat_id),
        C::ExternalSyncCompleted { integration_id, .. }
        | C::ExternalSyncFailed { integration_id, .. } => Subject::Integration(integration_id),
        C::DomainEventCommitted {
            scope_type,
            scope_id,
            ..
        } => match scope_type.as_str() {
            "project" => Subject::Project(scope_id),
            "task" => Subject::Task(scope_id),
            "agent_chat" => Subject::Chat(scope_id),
            "account" => Subject::Account(scope_id),
            _ => Subject::Unscoped,
        },
        _ => Subject::Unscoped,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Visibility {
    Visible,
    Hidden,
    /// No such Project (never created, or already deleted): nothing to
    /// protect, so the event is delivered as before.
    Unknown,
}

/// One SSE connection's view of which events its user may receive. Project
/// visibility answers expire after `VISIBILITY_TTL` (membership changes reach
/// open streams within that window); a Task's or Chat's Project never changes
/// and is cached for the connection. A Project that disappears keeps its last
/// answer, so its own deletion frames reach exactly the subscribers who could
/// see it. A lookup error withholds the event.
struct Audience {
    db: Arc<SqliteDb>,
    user_id: String,
    projects: HashMap<String, (Visibility, Instant)>,
    task_projects: HashMap<String, String>,
    chats: HashMap<String, db::AgentChat>,
}

impl Audience {
    fn new(db: Arc<SqliteDb>, user_id: String) -> Self {
        Self {
            db,
            user_id,
            projects: HashMap::new(),
            task_projects: HashMap::new(),
            chats: HashMap::new(),
        }
    }

    async fn admits(&mut self, event: &events::ForgeEvent) -> bool {
        match subject(event) {
            Subject::Unscoped => true,
            Subject::Account(account_id) => account_id == self.user_id,
            Subject::Project(project_id) => self.project(project_id).await != Visibility::Hidden,
            Subject::Projects(project_ids) => {
                if project_ids.is_empty() {
                    return true;
                }
                let mut unknown = false;
                for project_id in project_ids {
                    match self.project(project_id).await {
                        Visibility::Visible => return true,
                        Visibility::Unknown => unknown = true,
                        Visibility::Hidden => {}
                    }
                }
                unknown
            }
            Subject::Task(task_id) => match self.task_project(task_id).await {
                Ok(Some(project_id)) => self.project(&project_id).await != Visibility::Hidden,
                Ok(None) => true,
                Err(()) => false,
            },
            Subject::Chat(chat_id) => match self.chat(chat_id).await {
                Ok(Some(chat)) => match (chat.account_id.as_deref(), chat.project_id.as_deref()) {
                    (_, Some(project_id)) => self.project(project_id).await != Visibility::Hidden,
                    (Some(account_id), None) => account_id == self.user_id,
                    (None, None) => true,
                },
                Ok(None) => true,
                Err(()) => false,
            },
            Subject::Integration(integration_id) => {
                match IntegrationRepo::get_by_id(&*self.db, integration_id).await {
                    Ok(Some(integration)) => {
                        self.project(&integration.project_id).await != Visibility::Hidden
                    }
                    Ok(None) => true,
                    Err(error) => {
                        tracing::warn!(%error, "SSE integration lookup failed");
                        false
                    }
                }
            }
        }
    }

    async fn project(&mut self, project_id: &str) -> Visibility {
        let cached = self.projects.get(project_id).copied();
        if let Some((visibility, at)) = cached {
            if at.elapsed() < VISIBILITY_TTL {
                return visibility;
            }
        }
        let fresh = match ProjectRepo::get_visible_by_id(&*self.db, project_id, &self.user_id).await
        {
            Ok(Some(_)) => Visibility::Visible,
            Ok(None) => match ProjectRepo::get_by_id(&*self.db, project_id).await {
                Ok(Some(_)) => Visibility::Hidden,
                Ok(None) => Visibility::Unknown,
                Err(error) => {
                    tracing::warn!(%error, "SSE Project lookup failed");
                    return Visibility::Hidden;
                }
            },
            Err(error) => {
                tracing::warn!(%error, "SSE Project visibility lookup failed");
                return Visibility::Hidden;
            }
        };
        let answer = match (fresh, cached) {
            (Visibility::Unknown, Some((previous, _))) => previous,
            _ => fresh,
        };
        if self.projects.len() >= AUDIENCE_CACHE_LIMIT {
            self.projects.clear();
        }
        self.projects
            .insert(project_id.to_owned(), (answer, Instant::now()));
        answer
    }

    async fn task_project(&mut self, task_id: &str) -> Result<Option<String>, ()> {
        if let Some(project_id) = self.task_projects.get(task_id) {
            return Ok(Some(project_id.clone()));
        }
        match TaskRepo::get_by_id(&*self.db, task_id, true).await {
            Ok(Some(task)) => {
                if self.task_projects.len() >= AUDIENCE_CACHE_LIMIT {
                    self.task_projects.clear();
                }
                self.task_projects
                    .insert(task_id.to_owned(), task.project_id.clone());
                Ok(Some(task.project_id))
            }
            Ok(None) => Ok(None),
            Err(error) => {
                tracing::warn!(%error, "SSE Task lookup failed");
                Err(())
            }
        }
    }

    async fn chat(&mut self, chat_id: &str) -> Result<Option<db::AgentChat>, ()> {
        if let Some(chat) = self.chats.get(chat_id) {
            return Ok(Some(chat.clone()));
        }
        match AgentChatRepo::get_agent_chat(&*self.db, chat_id).await {
            Ok(Some(chat)) => {
                if self.chats.len() >= AUDIENCE_CACHE_LIMIT {
                    self.chats.clear();
                }
                self.chats.insert(chat_id.to_owned(), chat.clone());
                Ok(Some(chat))
            }
            Ok(None) => Ok(None),
            Err(error) => {
                tracing::warn!(%error, "SSE Agent Chat lookup failed");
                Err(())
            }
        }
    }
}

fn durable_sequence(id: &str) -> Option<i64> {
    let sequence = id.strip_prefix(DURABLE_ID_PREFIX)?;
    if sequence.is_empty() || !sequence.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    sequence.parse().ok()
}

fn frame(event: events::ForgeEvent) -> Option<Event> {
    let mut frame = Event::default().data(serde_json::to_string(&event).ok()?);
    // Omitting id preserves the client's last durable cursor. An empty id
    // would reset it, and a bus-only id would replace it with a non-cursor.
    if let events::EventContext::DomainEventCommitted { sequence, .. } = &event.context {
        frame = frame.id(format!("{DURABLE_ID_PREFIX}{sequence}"));
    }
    Some(frame)
}

fn resync_frame(reason: &str, reset_to: Option<i64>) -> Event {
    let data = json!({ "event_type": "events.resync_required", "entity_id": "events.resync_required", "timestamp": events::event_timestamp(), "reason": reason });
    let frame = Event::default().data(data.to_string());
    if let Some(sequence) = reset_to {
        frame.id(format!("{DURABLE_ID_PREFIX}{sequence}"))
    } else {
        frame
    }
}

fn replay_events(db: Arc<SqliteDb>, replay: Replay) -> impl tokio_stream::Stream<Item = Item> {
    // Preflight seeks at most 1,001 sequences; the stream keeps just one page.
    futures_util::stream::unfold(
        (db, replay, VecDeque::new(), MAX_REPLAY_EVENTS),
        |(db, replay, mut pending, mut remaining)| async move {
            match replay {
                Replay::None => None,
                Replay::Resync { reason, reset_to } => Some((
                    Item::Frame(resync_frame(reason, reset_to)),
                    (db, Replay::None, pending, 0),
                )),
                Replay::Events { after, through } => {
                    if remaining == 0 {
                        return None;
                    }
                    if pending.is_empty() && after < through {
                        match db
                            .list_events_after(after, remaining.min(REPLAY_PAGE_SIZE))
                            .await
                        {
                            Ok(rows) => pending
                                .extend(rows.into_iter().take_while(|row| row.sequence <= through)),
                            Err(error) => {
                                tracing::warn!(%error, "SSE ledger replay failed");
                                return Some((
                                    Item::Frame(resync_frame("ledger replay failed", None)),
                                    (db, Replay::None, pending, 0),
                                ));
                            }
                        }
                    }
                    let event = pending.pop_front()?;
                    let after = event.sequence;
                    remaining -= 1;
                    Some((
                        Item::Event(services::DomainEventService::committed_frame(&event)),
                        (db, Replay::Events { after, through }, pending, remaining),
                    ))
                }
            }
        },
    )
}
