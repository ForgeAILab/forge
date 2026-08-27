use std::convert::Infallible;

use axum::{
    extract::State,
    response::sse::{Event, KeepAlive, Sse},
};
use serde_json::json;
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

use crate::state::AppState;

pub async fn stream_events(
    State(state): State<AppState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
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
        BroadcastStream::new(state.event_bus.subscribe()).filter_map(|event| match event {
            Ok(event) => {
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
        stream,
        shutdown_requested,
    ))
    .keep_alive(KeepAlive::default())
}
