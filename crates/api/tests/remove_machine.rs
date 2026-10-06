mod common;

use api_types::{
    AuthResponse, DaemonRegisterResponse, DaemonResponse, PaginatedResponse, RemoveDaemonResponse,
};
use axum::http::{Method, StatusCode};
use db::DaemonRepo;
use serde_json::{json, Value};

async fn register(harness: &common::Harness, machine_id: &str) -> DaemonRegisterResponse {
    common::json_request_with_bearer(&harness.app,Method::POST,"/api/v1/daemons/register",&common::test_jwt(),
        json!({"machine_id":machine_id,"hostname":"Retired workstation","os":"linux","arch":"x86_64"}),StatusCode::OK).await
}

async fn remove(harness: &common::Harness, id: &str, token: &str, status: StatusCode) -> Value {
    common::empty_request_with_bearer(
        &harness.app,
        Method::DELETE,
        &format!("/api/v1/daemons/{id}"),
        token,
        status,
    )
    .await
}

#[tokio::test]
async fn disconnected_owner_can_remove_and_audit_orphan_cancellations() {
    let dir = tempfile::tempdir().unwrap();
    let harness = common::test_app(dir.path(), "remove-machine").await;
    let registration = register(&harness, "retired-host").await;
    harness
        .state
        .daemon_service
        .mark_disconnected(&registration.daemon_id)
        .await
        .unwrap();
    sqlx::query("INSERT INTO pending_remote_cancel VALUES ('operation','deleted-step','deleted-workspace','deleted-placement',?,'old-runtime',1,0,?)")
        .bind(&registration.daemon_id).bind(db::now_rfc3339()).execute(harness.state.db.pool()).await.unwrap();
    let result: RemoveDaemonResponse = serde_json::from_value(
        remove(
            &harness,
            &registration.daemon_id,
            &common::test_jwt(),
            StatusCode::OK,
        )
        .await,
    )
    .unwrap();
    assert_eq!(result.hostname, "Retired workstation");
    assert_eq!(result.pending_remote_cancels_cleared, 1);
    assert!(harness
        .state
        .db
        .pending_remote_cancels(None, None)
        .await
        .unwrap()
        .is_empty());
    let list: PaginatedResponse<DaemonResponse> = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/daemons?include_total=true",
        &common::admin_jwt(),
        StatusCode::OK,
    )
    .await;
    assert!(list.items.is_empty());
    let event:(String,Option<String>,String)=sqlx::query_as("SELECT actor_type,actor_id,payload_json FROM domain_event WHERE event_type='machine.removed'")
        .fetch_one(harness.state.db.pool()).await.unwrap();
    assert_eq!(event.0, "user");
    assert_eq!(event.1.as_deref(), Some("test-user-id"));
    assert_eq!(
        serde_json::from_str::<Value>(&event.2).unwrap()["removal"]
            ["pending_remote_cancels_cleared"],
        1
    );
    assert_eq!(
        DaemonRepo::get_by_id(&*harness.state.db, &registration.daemon_id)
            .await
            .unwrap()
            .unwrap()
            .hostname,
        "Retired workstation"
    );
    remove(
        &harness,
        &registration.daemon_id,
        &common::test_jwt(),
        StatusCode::NOT_FOUND,
    )
    .await;
}

#[tokio::test]
async fn connected_machine_returns_typed_conflict_without_revoking_token() {
    let dir = tempfile::tempdir().unwrap();
    let harness = common::test_app(dir.path(), "remove-connected").await;
    let registration = register(&harness, "connected-host").await;
    let error = remove(
        &harness,
        &registration.daemon_id,
        &common::test_jwt(),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(error["code"], "machine_connected");
    assert!(error["message"]
        .as_str()
        .unwrap()
        .contains("Stop the daemon"));
    harness
        .state
        .daemon_service
        .authenticate(&registration.daemon_id, &registration.registration_token)
        .await
        .unwrap();
    // Cover a transport still registered while the persisted status is offline.
    harness
        .state
        .daemon_service
        .mark_disconnected(&registration.daemon_id)
        .await
        .unwrap();
    let (connection, _outbound) = services::DaemonConnection::new(registration.daemon_id.clone());
    harness
        .state
        .daemon_connections
        .register(registration.daemon_id.clone(), connection);
    assert_eq!(
        remove(
            &harness,
            &registration.daemon_id,
            &common::test_jwt(),
            StatusCode::CONFLICT
        )
        .await["code"],
        "machine_connected"
    );
}

#[tokio::test]
async fn embedded_machine_returns_typed_conflict_even_while_offline() {
    let dir = tempfile::tempdir().unwrap();
    let harness = common::test_app(dir.path(), "remove-local").await;
    let registration = register(&harness, &services::embedded_daemon::embedded_machine_id()).await;
    harness
        .state
        .daemon_service
        .mark_disconnected(&registration.daemon_id)
        .await
        .unwrap();
    assert_eq!(
        remove(
            &harness,
            &registration.daemon_id,
            &common::test_jwt(),
            StatusCode::CONFLICT
        )
        .await["code"],
        "local_machine"
    );
    assert!(!harness
        .state
        .db
        .daemon_removed(&registration.daemon_id)
        .await
        .unwrap());
}

#[tokio::test]
async fn another_owner_gets_not_found_without_machine_state_disclosure() {
    let dir = tempfile::tempdir().unwrap();
    let harness = common::test_app(dir.path(), "remove-owner").await;
    let registration = register(&harness, "owner-host").await;
    let auth: AuthResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/auth/register",
        json!({"email":"other-owner@example.com","password":"password123"}),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(
        remove(
            &harness,
            &registration.daemon_id,
            &auth.access_token,
            StatusCode::NOT_FOUND
        )
        .await["code"],
        "not_found"
    );
    assert!(!harness
        .state
        .db
        .daemon_removed(&registration.daemon_id)
        .await
        .unwrap());
    harness
        .state
        .daemon_service
        .mark_disconnected(&registration.daemon_id)
        .await
        .unwrap();
    remove(
        &harness,
        &registration.daemon_id,
        &auth.access_token,
        StatusCode::NOT_FOUND,
    )
    .await;
}

#[tokio::test]
async fn removed_machine_rejects_old_report_and_websocket_credential_after_fresh_registration() {
    use tokio_tungstenite::tungstenite::Error;
    let dir = tempfile::tempdir().unwrap();
    let harness = common::test_app(dir.path(), "remove-auth").await;
    let original = register(&harness, "re-register-host").await;
    harness
        .state
        .daemon_service
        .mark_disconnected(&original.daemon_id)
        .await
        .unwrap();
    remove(
        &harness,
        &original.daemon_id,
        &common::test_jwt(),
        StatusCode::OK,
    )
    .await;
    let fresh = register(&harness, "re-register-host").await;
    assert_ne!(fresh.daemon_id, original.daemon_id);
    let error: Value = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &format!("/api/v1/daemons/{}/report", original.daemon_id),
        &original.registration_token,
        json!({"detected_clis":[]}),
        StatusCode::UNAUTHORIZED,
    )
    .await;
    assert_eq!(error["code"], "unauthorized");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = harness.app.clone();
    let job = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let result = tokio_tungstenite::connect_async(format!(
        "ws://{addr}/api/v1/daemons/{}/connect?token={}",
        original.daemon_id, original.registration_token
    ))
    .await;
    match result {
        Err(Error::Http(response)) => assert_eq!(response.status(), StatusCode::UNAUTHORIZED),
        other => panic!("removed identity unexpectedly reconnects: {other:?}"),
    }
    assert!(!harness
        .state
        .daemon_connections
        .is_connected(&original.daemon_id));
    harness
        .state
        .daemon_service
        .authenticate(&fresh.daemon_id, &fresh.registration_token)
        .await
        .unwrap();
    job.abort();
}
