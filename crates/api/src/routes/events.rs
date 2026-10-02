use db::{DomainEventRepo, SqliteDb};
use std::{collections::VecDeque, convert::Infallible, sync::Arc};

use axum::{
    extract::State,
    http::HeaderMap,
    response::sse::{Event, KeepAlive, Sse},
};
use serde_json::json;
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

use crate::state::AppState;

const MAX_REPLAY_EVENTS: i64 = 1_000;
const REPLAY_PAGE_SIZE: i64 = 100;
const DURABLE_ID_PREFIX: &str = "domain-event:";

enum Replay {
    None,
    Events { after: i64, through: i64 },
    Resync { reason: &'static str },
}

pub async fn stream_events(
    State(state): State<AppState>,
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
            Ok(through) => {
                let replay = match state
                    .db
                    .domain_event_replay_exceeds_limit(after, through, MAX_REPLAY_EVENTS)
                    .await
                {
                    Ok(false) => Replay::Events { after, through },
                    Ok(true) => Replay::Resync {
                        reason: "replay limit exceeded",
                    },
                    Err(error) => {
                        tracing::warn!(%error, "SSE replay bound query failed");
                        Replay::Resync {
                            reason: "ledger replay failed",
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
            frame(event).map(Ok)
        }
        Err(error) => Some(Ok(resync_frame(&error.to_string()))),
    });
    Sse::new(futures_util::StreamExt::take_until(
        tokio_stream::StreamExt::chain(replay, stream),
        shutdown_requested,
    ))
    .keep_alive(KeepAlive::default())
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

fn resync_frame(reason: &str) -> Event {
    let data = json!({ "event_type": "events.resync_required", "entity_id": "events.resync_required", "timestamp": events::event_timestamp(), "reason": reason });
    Event::default().data(data.to_string())
}

fn replay_events(
    db: Arc<SqliteDb>,
    replay: Replay,
) -> impl tokio_stream::Stream<Item = Result<Event, Infallible>> {
    // Preflight seeks at most 1,001 sequences; the stream keeps just one page.
    futures_util::stream::unfold(
        (db, replay, VecDeque::new(), MAX_REPLAY_EVENTS),
        |(db, replay, mut pending, mut remaining)| async move {
            match replay {
                Replay::None => None,
                Replay::Resync { reason } => {
                    Some((Ok(resync_frame(reason)), (db, Replay::None, pending, 0)))
                }
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
                                    Ok(resync_frame("ledger replay failed")),
                                    (db, Replay::None, pending, 0),
                                ));
                            }
                        }
                    }
                    let event = pending.pop_front()?;
                    let after = event.sequence;
                    remaining -= 1;
                    let frame = frame(services::DomainEventService::committed_frame(&event))
                        .expect("committed event envelope serializes");
                    Some((
                        Ok(frame),
                        (db, Replay::Events { after, through }, pending, remaining),
                    ))
                }
            }
        },
    )
}
