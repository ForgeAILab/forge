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

pub async fn stream_events(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    // Subscribe before taking the ledger snapshot: live appends cannot fall in
    // the seam between replay and subscription. Only generic durable frames in
    // that snapshot are filtered out of the live stream.
    let receiver = state.event_bus.subscribe();
    let after = if let Some(id) = headers.get("last-event-id").and_then(|v| v.to_str().ok()) {
        state
            .db
            .get_event(id)
            .await
            .ok()
            .flatten()
            .map_or(0, |event| event.sequence)
    } else {
        0
    };
    let through = state.db.domain_event_head().await.unwrap_or(0);
    let replay = replay_events(Arc::clone(&state.db), after, through);
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

    // One canonical envelope per frame (D20): the frame carries no SSE `event:`
    // name and the payload's `event_type` is the sole routing discriminator.
    // A named frame is delivered only to a listener registered under that exact
    // name, never to `onmessage`, so naming frames here meant the web client —
    // which routes every frame through `onmessage` — saw none of them and the
    // whole UI went stale until a reload.
    let stream =
        BroadcastStream::new(receiver).filter_map(move |event| match event {
            Ok(event) => {
                if matches!(&event.context, events::EventContext::DomainEventCommitted { sequence, .. } if *sequence <= through) { return None; }
                let entity_id = event.entity_id.clone();
                // EventContext is flattened and Serialize-derived, so review/cleanup/merge
                // contexts pass through SSE without variant-specific routing here.
                let data = serde_json::to_string(&event).ok()?;
                Some(Ok(Event::default().id(entity_id).data(data)))
            }
            Err(error) => {
                let event_type = "events.resync_required";
                let data = json!({
                    "event_type": event_type,
                    "entity_id": event_type,
                    "timestamp": events::event_timestamp(),
                    "reason": error.to_string(),
                });
                Some(Ok(Event::default().id(event_type).data(data.to_string())))
            }
        });
    Sse::new(futures_util::StreamExt::take_until(
        tokio_stream::StreamExt::chain(replay, stream),
        shutdown_requested,
    ))
    .keep_alive(KeepAlive::default())
}

fn replay_events(
    db: Arc<SqliteDb>,
    after: i64,
    through: i64,
) -> impl tokio_stream::Stream<Item = Result<Event, Infallible>> {
    futures_util::stream::unfold(
        (db, after, through, VecDeque::new(), false),
        |(db, mut position, through, mut pending, failed)| async move {
            if failed {
                return None;
            }
            if pending.is_empty() && position < through {
                match db.list_events_after(position, 100).await {
                    Ok(rows) => {
                        pending.extend(rows.into_iter().take_while(|row| row.sequence <= through))
                    }
                    Err(error) => {
                        tracing::warn!(%error, "SSE ledger replay failed");
                        let data = json!({ "event_type": "events.resync_required", "entity_id": "events.resync_required", "timestamp": events::event_timestamp(), "reason": "ledger replay failed" });
                        return Some((
                            Ok(Event::default()
                                .id("events.resync_required")
                                .data(data.to_string())),
                            (db, through, through, pending, true),
                        ));
                    }
                }
            }
            let event = pending.pop_front()?;
            position = event.sequence;
            let frame = services::DomainEventService::committed_frame(&event);
            let data = serde_json::to_string(&frame).expect("committed event envelope serializes");
            Some((
                Ok(Event::default().id(&event.id).data(data)),
                (db, position, through, pending, false),
            ))
        },
    )
}
