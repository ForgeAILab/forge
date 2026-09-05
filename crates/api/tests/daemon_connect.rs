mod common;

use std::{path::PathBuf, sync::Arc, time::Duration};

use api::{build_router, serve_with_listener, AppState};
use api_types::{DaemonFrame, DaemonRegisterResponse};
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        client::IntoClientRequest, http::HeaderValue, Error as WsError, Message as WsMessage,
    },
    MaybeTlsStream, WebSocketStream,
};
use tower::ServiceExt;

type ClientSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

#[tokio::test]
async fn daemon_connect_with_bearer_token_upgrades_and_registers_connection() {
    let state = test_state().await;
    let app = test_app(&state);
    let registration = register_daemon(&app, "connect-valid-token").await;
    let server = TestServer::start(Arc::clone(&state)).await;

    let _socket = connect_daemon(
        &server,
        &registration.daemon_id,
        Some(&registration.registration_token),
    )
    .await
    .expect("websocket upgrade succeeds");

    wait_until_connected(&state, &registration.daemon_id).await;
    assert!(state
        .daemon_connections
        .is_connected(&registration.daemon_id));
}

#[tokio::test]
async fn daemon_connect_rejects_missing_or_invalid_token() {
    let state = test_state().await;
    let app = test_app(&state);
    let registration = register_daemon(&app, "connect-reject-token").await;
    let server = TestServer::start(Arc::clone(&state)).await;

    let missing = match connect_daemon(&server, &registration.daemon_id, None).await {
        Ok(_) => panic!("missing token unexpectedly upgraded websocket"),
        Err(error) => error,
    };
    assert_ws_http_status(missing, StatusCode::UNAUTHORIZED);

    let invalid =
        match connect_daemon(&server, &registration.daemon_id, Some("not-the-token")).await {
            Ok(_) => panic!("invalid token unexpectedly upgraded websocket"),
            Err(error) => error,
        };
    assert_ws_http_status(invalid, StatusCode::UNAUTHORIZED);

    assert!(!state
        .daemon_connections
        .is_connected(&registration.daemon_id));
}

#[tokio::test]
async fn daemon_connect_replaces_stale_connection() {
    let state = test_state().await;
    let app = test_app(&state);
    let registration = register_daemon(&app, "connect-replace-stale").await;
    let server = TestServer::start(Arc::clone(&state)).await;

    let mut first_socket = connect_daemon(
        &server,
        &registration.daemon_id,
        Some(&registration.registration_token),
    )
    .await
    .expect("first websocket upgrade succeeds");
    wait_until_connected(&state, &registration.daemon_id).await;
    let first_connection_id = state
        .daemon_connections
        .get(&registration.daemon_id)
        .expect("first connection is registered")
        .id();

    let _second_socket = connect_daemon(
        &server,
        &registration.daemon_id,
        Some(&registration.registration_token),
    )
    .await
    .expect("second websocket upgrade succeeds");
    let second_connection_id =
        wait_for_replacement(&state, &registration.daemon_id, first_connection_id).await;

    assert!(!state
        .daemon_connections
        .is_current(&registration.daemon_id, first_connection_id));
    assert!(state
        .daemon_connections
        .is_current(&registration.daemon_id, second_connection_id));

    let first_close = tokio::time::timeout(Duration::from_secs(2), first_socket.next())
        .await
        .expect("first connection closes after replacement");
    match first_close {
        None | Some(Err(_)) | Some(Ok(WsMessage::Close(_))) => {}
        other => panic!("first connection should not remain active, got {other:?}"),
    }
}

#[tokio::test]
async fn daemon_connect_updates_status_for_socket_lifecycle() {
    let state = test_state().await;
    let app = test_app(&state);
    let registration = register_daemon(&app, "connect-status-lifecycle").await;
    db::DaemonRepo::mark_offline(&*state.db, &registration.daemon_id, &db::now_rfc3339())
        .await
        .expect("daemon starts offline");
    let server = TestServer::start(Arc::clone(&state)).await;

    let mut socket = connect_daemon(
        &server,
        &registration.daemon_id,
        Some(&registration.registration_token),
    )
    .await
    .expect("websocket upgrade succeeds");

    wait_until_status(&state, &registration.daemon_id, db::DaemonStatus::Online).await;
    assert!(state
        .daemon_connections
        .is_connected(&registration.daemon_id));

    socket
        .send(WsMessage::Close(None))
        .await
        .expect("close frame sends");

    wait_until_status(&state, &registration.daemon_id, db::DaemonStatus::Offline).await;
    assert!(!state
        .daemon_connections
        .is_connected(&registration.daemon_id));
}

#[tokio::test]
async fn daemon_heartbeat_refreshes_last_report_at() {
    let state = test_state().await;
    let app = test_app(&state);
    let registration = register_daemon(&app, "connect-heartbeat-touch").await;
    let server = TestServer::start(Arc::clone(&state)).await;

    let mut socket = connect_daemon(
        &server,
        &registration.daemon_id,
        Some(&registration.registration_token),
    )
    .await
    .expect("websocket upgrade succeeds");

    let connected =
        wait_until_status(&state, &registration.daemon_id, db::DaemonStatus::Online).await;
    let prior_last_report_at = connected.last_report_at;
    tokio::time::sleep(Duration::from_millis(10)).await;

    let heartbeat =
        serde_json::to_string(&DaemonFrame::Heartbeat { seq: 7 }).expect("heartbeat serializes");
    socket
        .send(WsMessage::Text(heartbeat.into()))
        .await
        .expect("heartbeat sends");

    let echoed = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("heartbeat echo arrives")
        .expect("socket remains open")
        .expect("heartbeat echo succeeds");
    match echoed {
        WsMessage::Text(text) => {
            let frame: DaemonFrame =
                serde_json::from_str(text.as_ref()).expect("heartbeat echo parses");
            assert!(matches!(frame, DaemonFrame::Heartbeat { seq: 7 }));
        }
        other => panic!("expected heartbeat echo text frame, got {other:?}"),
    }

    let touched =
        wait_until_last_report_change(&state, &registration.daemon_id, prior_last_report_at).await;
    assert_eq!(touched.status, db::DaemonStatus::Online);
}

async fn test_state() -> Arc<AppState> {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");

    let db = Arc::new(db::SqliteDb::new(pool));
    let event_bus = Arc::new(events::EventBus::new(16));
    Arc::new(AppState::new(db, event_bus, true))
}

fn test_app(state: &AppState) -> Router {
    build_router(state.clone(), temp_web_dist())
}

async fn register_daemon(app: &Router, machine_id: &str) -> DaemonRegisterResponse {
    json_request(
        app,
        Method::POST,
        "/api/v1/daemons/register",
        json!({
            "machine_id": machine_id,
            "hostname": "daemon-connect-test-host",
            "os": "linux",
            "arch": "x86_64",
            "agent_version": "daemon-connect-test",
            "labels": { "suite": "daemon_connect" }
        }),
        StatusCode::OK,
    )
    .await
}

async fn connect_daemon(
    server: &TestServer,
    daemon_id: &str,
    token: Option<&str>,
) -> Result<ClientSocket, WsError> {
    let url = format!("ws://{}/api/v1/daemons/{daemon_id}/connect", server.addr);
    let request = if let Some(token) = token {
        let mut request = url.into_client_request()?;
        request.headers_mut().insert(
            header::AUTHORIZATION.as_str(),
            HeaderValue::from_str(&format!("Bearer {token}")).expect("authorization header"),
        );
        request
    } else {
        url.into_client_request()?
    };

    connect_async(request).await.map(|(socket, _)| socket)
}

async fn wait_until_connected(state: &AppState, daemon_id: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if state.daemon_connections.is_connected(daemon_id) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "daemon connection was not registered"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_until_status(
    state: &AppState,
    daemon_id: &str,
    expected: db::DaemonStatus,
) -> db::Daemon {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let daemon = db::DaemonRepo::get_by_id(&*state.db, daemon_id)
            .await
            .expect("daemon loads")
            .expect("daemon exists");
        if daemon.status == expected {
            return daemon;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "daemon status did not become {expected}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_until_last_report_change(
    state: &AppState,
    daemon_id: &str,
    prior: Option<String>,
) -> db::Daemon {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let daemon = db::DaemonRepo::get_by_id(&*state.db, daemon_id)
            .await
            .expect("daemon loads")
            .expect("daemon exists");
        if daemon.last_report_at.is_some() && daemon.last_report_at != prior {
            return daemon;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "daemon last_report_at did not change"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_replacement(state: &AppState, daemon_id: &str, stale_connection_id: u64) -> u64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(connection) = state.daemon_connections.get(daemon_id) {
            if connection.id() != stale_connection_id {
                return connection.id();
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "daemon connection was not replaced"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn assert_ws_http_status(error: WsError, expected: StatusCode) {
    match error {
        WsError::Http(response) => assert_eq!(response.status().as_u16(), expected.as_u16()),
        other => panic!("expected HTTP websocket error {expected}, got {other:?}"),
    }
}

async fn json_request<T>(
    app: &Router,
    method: Method,
    uri: &str,
    body: Value,
    expected_status: StatusCode,
) -> T
where
    T: DeserializeOwned,
{
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .expect("build JSON request"),
        )
        .await
        .expect("router response");
    parse_response(response, expected_status).await
}

async fn parse_response<T>(response: axum::response::Response, expected_status: StatusCode) -> T
where
    T: DeserializeOwned,
{
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response body");
    assert_eq!(
        status,
        expected_status,
        "unexpected response status with body: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).expect("parse JSON response")
}

struct TestServer {
    addr: std::net::SocketAddr,
    state: Arc<AppState>,
    handle: tokio::task::JoinHandle<()>,
    _web_dist_dir: common::TestDir,
}

impl TestServer {
    async fn start(state: Arc<AppState>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let addr = listener.local_addr().expect("listener addr");
        let web_dist_dir = common::TestDir::new("forge-api-daemon-connect-web");
        std::fs::write(web_dist_dir.path().join("index.html"), "<html></html>")
            .expect("write index");
        let web_dist_path = web_dist_dir.path().to_path_buf();
        let server_state = (*state).clone();
        let shutdown_signal = state.shutdown_signal.clone();

        let handle = tokio::spawn(async move {
            serve_with_listener(listener, server_state, web_dist_path, async move {
                shutdown_signal.wait().await;
            })
            .await
            .expect("test API server serves");
        });

        Self {
            addr,
            state,
            handle,
            _web_dist_dir: web_dist_dir,
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.state.shutdown_signal.request();
        self.handle.abort();
    }
}

fn temp_web_dist() -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "forge-api-daemon-connect-router-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&path).expect("create temp web dist");
    std::fs::write(path.join("index.html"), "<html></html>").expect("write index");
    path
}
