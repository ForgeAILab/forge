mod common;

use axum::{
    body::{Body, BodyDataStream},
    http::Request,
};
use db::{CreateDomainEvent, DomainEvent, DomainEventRepo};
use events::{EventContext, ForgeEvent};
use futures_util::StreamExt;
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tower::ServiceExt;

fn input(id: &str) -> CreateDomainEvent {
    CreateDomainEvent {
        id: id.into(),
        event_type: "test.committed".into(),
        entity_type: "project".into(),
        entity_id: "p".into(),
        actor_type: "system".into(),
        actor_id: None,
        scope_type: "project".into(),
        scope_id: "p".into(),
        correlation_id: id.into(),
        causation_id: None,
        causation_depth: 0,
        dedupe_key: Some(id.into()),
        payload_json: "{}".into(),
        created_at: db::now_rfc3339(),
    }
}
async fn append(db: &db::SqliteDb, id: &str) -> DomainEvent {
    db.append_event(input(id)).await.unwrap()
}
async fn append_many(db: &db::SqliteDb, count: usize) -> Vec<DomainEvent> {
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    let mut events = Vec::new();
    for index in 0..count {
        events.push(
            db.append_event_in_tx(&mut tx, &input(&format!("missed-{index}")))
                .await
                .unwrap(),
        );
    }
    tx.commit().await.unwrap();
    events
}
fn publish(h: &common::Harness, event: &DomainEvent) {
    h.state
        .event_bus
        .publish(services::DomainEventService::committed_frame(event));
}
async fn response_stream(h: &common::Harness, resume: Option<&str>) -> BodyDataStream {
    let mut request =
        Request::builder().uri(format!("/api/v1/events?token={}", common::test_jwt()));
    if let Some(id) = resume {
        request = request.header("Last-Event-ID", id);
    }
    let response = h
        .app
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    response.into_body().into_data_stream()
}
async fn next_event(stream: &mut BodyDataStream) -> (Option<String>, Value) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let bytes = stream
                .next()
                .await
                .expect("stream open")
                .expect("body frame");
            let frame = std::str::from_utf8(&bytes).unwrap();
            let Some(data) = frame.lines().find_map(|line| line.strip_prefix("data:")) else {
                continue;
            };
            let id = frame
                .lines()
                .find_map(|line| line.strip_prefix("id:"))
                .map(|id| id.trim().to_owned());
            return (id, serde_json::from_str(data.trim()).unwrap());
        }
    })
    .await
    .expect("SSE event arrives")
}
async fn read_until(socket: &mut tokio::net::TcpStream, buffer: &mut String, needle: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !buffer.contains(needle) {
            let mut bytes = [0; 16384];
            let n = socket.read(&mut bytes).await.unwrap();
            assert!(n > 0, "SSE socket closed before {needle}: {buffer}");
            buffer.push_str(std::str::from_utf8(&bytes[..n]).unwrap());
        }
    })
    .await
    .expect("SSE frame arrives");
}

#[tokio::test]
async fn plain_connect_is_live_only_and_frame_ids_distinguish_durable_and_bus_events() {
    let workspace = common::TestDir::new("events-live");
    let h = common::test_app(workspace.path(), "events-live").await;
    let historical = append(&h.state.db, "historical").await;
    let relay = services::DomainEventBroadcastConsumer::new(
        Arc::clone(&h.state.db),
        Arc::clone(&h.state.event_bus),
        Some(historical.sequence),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = h.app.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
    socket
        .write_all(
            format!(
                "GET /api/v1/events?token={} HTTP/1.1\r\nHost: {address}\r\n\r\n",
                common::test_jwt()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut buffer = String::new();
    read_until(&mut socket, &mut buffer, "200 OK").await;
    assert!(
        !buffer.contains("data:"),
        "plain connect must not replay history"
    );
    let mut bytes = [0; 1024];
    assert!(
        tokio::time::timeout(Duration::from_millis(100), socket.read(&mut bytes))
            .await
            .is_err()
    );
    let live = append(&h.state.db, "live").await;
    assert_eq!(relay.broadcast_once(100).await.unwrap(), 1);
    read_until(
        &mut socket,
        &mut buffer,
        &format!("id: domain-event:{}", live.sequence),
    )
    .await;
    assert!(!buffer.contains("historical"));
    assert!(buffer.contains("\"entity_id\":\"live\""));
    buffer.clear();
    h.state.event_bus.publish(ForgeEvent {
        event_type: "test.bus".into(),
        entity_id: "domain-event:123".into(),
        timestamp: events::event_timestamp(),
        context: EventContext::Empty {},
    });
    read_until(
        &mut socket,
        &mut buffer,
        "\"entity_id\":\"domain-event:123\"",
    )
    .await;
    assert!(
        !buffer.lines().any(|line| line.starts_with("id:")),
        "bus-only frame must omit id even when its entity looks like a cursor"
    );
    assert!(buffer.contains("\"entity_id\":\"domain-event:123\""));
    assert!(!buffer.contains("\nevent:"));
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn durable_resume_replays_missed_pages_once_in_order_with_append_during_replay() {
    let workspace = common::TestDir::new("events-resume");
    let h = common::test_app(workspace.path(), "events-resume").await;
    let seen = append(&h.state.db, "seen").await;
    let missed = append_many(&h.state.db, 250).await;
    let mut stream = response_stream(&h, Some(&format!("domain-event:{}", seen.sequence))).await;
    // Poll just one replay frame, keeping later pages unpolled. This forces the
    // append to occur during replay, rather than relying on socket timing.
    let first = next_event(&mut stream).await;
    assert_eq!(
        first.0,
        Some(format!("domain-event:{}", missed[0].sequence))
    );
    let concurrent = append(&h.state.db, "concurrent").await;
    publish(&h, &missed[0]); // delayed relay overlaps the captured snapshot
    publish(&h, &concurrent);
    let mut actual = vec![first];
    for _ in 0..missed.len() {
        actual.push(next_event(&mut stream).await);
    }
    let expected: Vec<_> = missed
        .iter()
        .chain(std::iter::once(&concurrent))
        .map(|event| {
            (
                format!("domain-event:{}", event.sequence),
                event.id.as_str(),
            )
        })
        .collect();
    assert_eq!(actual.len(), expected.len());
    for ((id, payload), (expected_id, entity)) in actual.iter().zip(expected) {
        assert_eq!(id.as_ref(), Some(&expected_id));
        assert_eq!(payload["entity_id"], entity);
        assert_eq!(payload["event_type"], "domain_event.committed");
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), stream.next())
            .await
            .is_err(),
        "no overlap duplicate remains"
    );
}

#[tokio::test]
async fn exactly_1000_missed_events_can_resume() {
    let workspace = common::TestDir::new("events-resume-boundary");
    let h = common::test_app(workspace.path(), "events-resume-boundary").await;
    let missed = append_many(&h.state.db, 1000).await;
    let mut stream = response_stream(&h, Some("domain-event:0")).await;
    for event in missed {
        let (id, payload) = next_event(&mut stream).await;
        assert_eq!(id, Some(format!("domain-event:{}", event.sequence)));
        assert_eq!(payload["entity_id"], event.id);
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), stream.next())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn more_than_1000_missed_events_sends_one_resync_without_replay_then_goes_live() {
    let workspace = common::TestDir::new("events-resync");
    let h = common::test_app(workspace.path(), "events-resync").await;
    let missed = append_many(&h.state.db, 1001).await;
    let mut stream = response_stream(&h, Some("domain-event:0")).await;
    let (id, payload) = next_event(&mut stream).await;
    assert_eq!(
        id, None,
        "resync frame must omit the id line, not reset the durable cursor"
    );
    assert_eq!(payload["event_type"], "events.resync_required");
    assert_eq!(payload["reason"], "replay limit exceeded");
    publish(&h, &missed[0]); // snapshot frames cannot become implicit replay
    let live = append(&h.state.db, "after-resync").await;
    publish(&h, &live);
    let (id, payload) = next_event(&mut stream).await;
    assert_eq!(id, Some(format!("domain-event:{}", live.sequence)));
    assert_eq!(payload["entity_id"], live.id);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), stream.next())
            .await
            .is_err(),
        "only one resync and no historical frames"
    );
}

#[tokio::test]
async fn entity_and_garbage_resume_ids_are_live_only() {
    let workspace = common::TestDir::new("events-invalid-resume");
    let h = common::test_app(workspace.path(), "events-invalid-resume").await;
    let historical = append(&h.state.db, &db::new_uuid_v4()).await;
    for resume in [
        historical.id.as_str(),
        "p",
        "garbage",
        "domain-event:",
        "domain-event:-1",
        "domain-event:+1",
        "domain-event:999999999999999999999",
    ] {
        let mut stream = response_stream(&h, Some(resume)).await;
        let live = append(&h.state.db, &db::new_uuid_v4()).await;
        publish(&h, &live);
        let (id, payload) = next_event(&mut stream).await;
        assert_eq!(
            id,
            Some(format!("domain-event:{}", live.sequence)),
            "invalid resume {resume}"
        );
        assert_eq!(payload["entity_id"], live.id);
        assert!(
            tokio::time::timeout(Duration::from_millis(25), stream.next())
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn durable_cursor_survives_bus_only_frame_and_reconnect_replays_missed_events() {
    let workspace = common::TestDir::new("events-cursor-preservation");
    let h = common::test_app(workspace.path(), "events-cursor-preservation").await;
    let mut stream = response_stream(&h, None).await;
    let delivered = append(&h.state.db, "delivered").await;
    publish(&h, &delivered);
    let (id, _) = next_event(&mut stream).await;
    let mut last_event_id = id.expect("durable frame supplies a cursor");
    assert_eq!(
        last_event_id,
        format!("domain-event:{}", delivered.sequence)
    );
    h.state.event_bus.publish(ForgeEvent {
        event_type: "test.bus".into(),
        entity_id: "domain-event:123".into(),
        timestamp: events::event_timestamp(),
        context: EventContext::Empty {},
    });
    let (bus_id, payload) = next_event(&mut stream).await;
    assert_eq!(bus_id, None, "bus-only frame has no id line");
    assert_eq!(payload["entity_id"], "domain-event:123");
    // Follow EventSource's rule: only a frame with an id changes its cursor.
    if let Some(id) = bus_id {
        last_event_id = id;
    }
    drop(stream);
    let missed = [
        append(&h.state.db, "missed-after-bus-a").await,
        append(&h.state.db, "missed-after-bus-b").await,
    ];
    let mut resumed = response_stream(&h, Some(&last_event_id)).await;
    for event in missed {
        let (id, payload) = next_event(&mut resumed).await;
        assert_eq!(id, Some(format!("domain-event:{}", event.sequence)));
        assert_eq!(payload["entity_id"], event.id);
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), resumed.next())
            .await
            .is_err(),
        "missed durable events replay exactly once"
    );
}

#[tokio::test]
async fn audit_resume_beyond_head_requests_resync_then_goes_live() {
    let workspace = common::TestDir::new("events-beyond-head");
    let h = common::test_app(workspace.path(), "events-beyond-head").await;
    append(&h.state.db, "only").await;
    let mut stream = response_stream(&h, Some("domain-event:5000")).await;
    let (id, payload) = next_event(&mut stream).await;
    assert_eq!(id, None);
    assert_eq!(payload["event_type"], "events.resync_required");
    assert_eq!(payload["reason"], "resume cursor beyond ledger head");
    let live = append(&h.state.db, "live-after").await;
    publish(&h, &live);
    assert_eq!(
        next_event(&mut stream).await.0,
        Some(format!("domain-event:{}", live.sequence))
    );
}
#[tokio::test]
async fn audit_composite_and_service_commits_are_published_in_order_by_the_relay() {
    let workspace = common::TestDir::new("events-ordered-publisher");
    let h = common::test_app(workspace.path(), "events-ordered-publisher").await;
    let mut stream = response_stream(&h, None).await;
    let pending = append_many(&h.state.db, 4).await;
    let service = services::DomainEventService::new(Arc::clone(&h.state.db));
    let fifth = service.append(input("fifth")).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), stream.next())
            .await
            .is_err(),
        "service append must not bypass the relay"
    );
    let relay = services::DomainEventBroadcastConsumer::new(
        Arc::clone(&h.state.db),
        Arc::clone(&h.state.event_bus),
        Some(0),
    );
    assert_eq!(relay.broadcast_once(100).await.unwrap(), 5);
    for event in pending.iter().chain(std::iter::once(&fifth)) {
        assert_eq!(
            next_event(&mut stream).await.0,
            Some(format!("domain-event:{}", event.sequence))
        );
    }
}
