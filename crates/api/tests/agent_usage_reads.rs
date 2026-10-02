#![allow(dead_code)]
mod common;
use api_types::ProjectResponse;
use axum::http::{Method, StatusCode};
use serde_json::{json, Value};

#[tokio::test]
async fn both_agent_lists_hide_daemon_pins_from_non_admins() {
    let workspace = common::TestDir::new("agent-list-pins");
    let harness = common::test_app(workspace.path(), "agent-list-pins").await;
    let (agent, _) = common::create_shell_agents(&harness.app, workspace.path(), "pins").await;
    let project: ProjectResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({"name":"Pinned Agent visibility"}),
        StatusCode::OK,
    )
    .await;
    // Make this server-created identity visible to the Project owner. The
    // actual daemon pin stays on its immutable selected Profile revision.
    sqlx::query(
        "UPDATE agent_identity SET owner_id='test-user-id',visibility='account' WHERE id=?",
    )
    .bind(&agent)
    .execute(harness.state.db.pool())
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = harness.app.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    for path in [
        "/api/v1/agents?limit=100".to_owned(),
        format!("/api/v1/projects/{}/agents", project.id),
    ] {
        let user: Value =
            common::empty_request(&harness.app, Method::GET, &path, StatusCode::OK).await;
        let items = user.get("items").unwrap_or(&user).as_array().unwrap();
        let row = items.iter().find(|r| r["id"] == agent).unwrap();
        assert!(row["daemon_id"].is_null(), "{path}: {row}");
        let admin: Value = common::empty_request_with_bearer(
            &harness.app,
            Method::GET,
            &path,
            &common::admin_jwt(),
            StatusCode::OK,
        )
        .await;
        let items = admin.get("items").unwrap_or(&admin).as_array().unwrap();
        let row = items.iter().find(|r| r["id"] == agent).unwrap();
        assert!(row["daemon_id"].is_string(), "{path}: {row}");

        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
        socket.write_all(format!("GET {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {}\r\nConnection: close\r\n\r\n",common::test_jwt()).as_bytes()).await.unwrap();
        let mut bytes = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            socket.read_to_end(&mut bytes),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(bytes.starts_with(b"HTTP/1.1 200"));
        let body = bytes
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .unwrap()
            + 4;
        let value: Value = serde_json::from_slice(&bytes[body..]).unwrap();
        let items = value.get("items").unwrap_or(&value).as_array().unwrap();
        assert!(items.iter().find(|r| r["id"] == agent).unwrap()["daemon_id"].is_null());
    }
    server.abort();
}
