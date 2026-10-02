mod common;
use db::{CreateDomainEvent, DomainEventRepo};
use std::{sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn append(db: &db::SqliteDb, id: &str) {
    db.append_event(CreateDomainEvent {
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
    })
    .await
    .unwrap();
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
async fn connect(address: std::net::SocketAddr, resume: Option<&str>) -> tokio::net::TcpStream {
    let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
    let header = resume.map_or(String::new(), |id| format!("Last-Event-ID: {id}\r\n"));
    socket
        .write_all(
            format!(
                "GET /api/v1/events?token={} HTTP/1.1\r\nHost: {address}\r\n{header}\r\n",
                common::test_jwt()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    socket
}
#[tokio::test]
async fn ledger_replay_preserves_downtime_events_and_last_event_id_without_live_overlap() {
    let workspace = common::TestDir::new("events-replay");
    let h = common::test_app(workspace.path(), "events-replay").await;
    append(&h.state.db, "replay-a").await;
    append(&h.state.db, "replay-b").await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, h.app).await.unwrap();
    });
    let relay = services::DomainEventBroadcastConsumer::new(
        Arc::clone(&h.state.db),
        Arc::clone(&h.state.event_bus),
        0,
    );
    let mut first = connect(address, None).await;
    let mut buffer = String::new();
    read_until(&mut first, &mut buffer, "id: replay-b").await;
    assert!(buffer.contains("200 OK"));
    assert!(buffer.find("id: replay-a").unwrap() < buffer.find("id: replay-b").unwrap());
    relay.broadcast_once(100).await.unwrap(); // replay/live seam: old frames are filtered
    append(&h.state.db, "replay-c").await;
    relay.broadcast_once(100).await.unwrap();
    read_until(&mut first, &mut buffer, "id: replay-c").await;
    assert_eq!(buffer.matches("id: replay-a").count(), 1);
    assert_eq!(buffer.matches("id: replay-b").count(), 1);
    assert!(!buffer.contains("\nevent:"));
    drop(first);
    let mut resumed = connect(address, Some("replay-b")).await;
    let mut resumed_buffer = String::new();
    read_until(&mut resumed, &mut resumed_buffer, "id: replay-c").await;
    assert!(!resumed_buffer.contains("id: replay-a"));
    assert!(!resumed_buffer.contains("id: replay-b"));
    append(&h.state.db, "replay-d").await;
    relay.broadcast_once(100).await.unwrap();
    read_until(&mut resumed, &mut resumed_buffer, "id: replay-d").await;
    assert_eq!(resumed_buffer.matches("id: replay-c").count(), 1);
    assert_eq!(resumed_buffer.matches("id: replay-d").count(), 1);
    server.abort();
    let _ = server.await;
}
