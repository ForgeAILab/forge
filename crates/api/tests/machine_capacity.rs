#![allow(dead_code, clippy::assertions_on_constants)]
mod common;

use api::{build_router, AppState};
use api_types::{DaemonResponse, SettingsResponse, TaskResponse};
use axum::http::{Method, StatusCode};
use common::{empty_request_with_bearer, json_request, json_request_with_bearer, TestDir};
use serde_json::{json, Value};
use std::sync::Arc;

async fn settings_app(root: &std::path::Path) -> (axum::Router, Arc<AppState>) {
    let harness = common::test_app(root, "machine-capacity").await;
    let config_path = root.join("forge.yaml");
    let mut file = serde_yaml::to_value(harness.state.effective_config.as_ref()).unwrap();
    file["project"] = file["project"]["values"].clone();
    file["providers"] = file["providers"]["entries"].clone();
    std::fs::write(&config_path, serde_yaml::to_string(&file).unwrap()).unwrap();
    let state = Arc::new((*harness.state).clone().with_config_path(config_path));
    let web = root.join("web");
    std::fs::create_dir_all(&web).unwrap();
    std::fs::write(web.join("index.html"), "<html></html>").unwrap();
    (build_router((*state).clone(), web), state)
}

#[tokio::test]
async fn machine_capacity_settings_live_update_controls_next_admission() {
    let root = TestDir::new("machine-live-setting");
    let (app, state) = settings_app(root.path()).await;
    let repo_root = TestDir::new("machine-live-repo");
    let repo = common::setup_git_repo(repo_root.path());
    let (project, repo_id) = common::create_project_and_repo(&app, "Capacity", &repo).await;
    let (worker, reviewer) = common::create_shell_agents(&app, root.path(), "capacity").await;
    common::configure_execution_test_setup(&state.db, &project, &repo_id, &worker, &reviewer).await;
    sqlx::query("UPDATE agent_identity SET max_concurrent_tasks = 10 WHERE id = ?")
        .bind(&worker)
        .execute(state.db.pool())
        .await
        .unwrap();
    let mut tasks = Vec::new();
    for title in ["First", "Second"] {
        let task: TaskResponse = json_request(
            &app,
            Method::POST,
            &format!("/api/v1/projects/{project}/tasks"),
            json!({"title":title,"description":"echo capacity"}),
            StatusCode::OK,
        )
        .await;
        tasks.push(task);
    }
    let admin = common::admin_jwt();
    let response: SettingsResponse = json_request_with_bearer(
        &app,
        Method::PUT,
        "/api/v1/settings",
        &admin,
        json!({"server":{"max_concurrent_runs":1}}),
        StatusCode::OK,
    )
    .await;
    let cap = response
        .settings
        .iter()
        .find(|setting| setting.key == "server.max_concurrent_runs")
        .unwrap();
    assert_eq!(cap.value, json!(1));
    assert_eq!(cap.effective_value, json!(1));
    assert!(!cap.restart_required);
    state
        .task_service
        .claim_task(
            &tasks[0].id,
            services::Assignee::Agent(worker.clone()),
            None,
        )
        .await
        .unwrap();
    let error = state
        .task_service
        .claim_task(
            &tasks[1].id,
            services::Assignee::Agent(worker.clone()),
            None,
        )
        .await
        .err()
        .unwrap();
    assert!(
        matches!(&error, services::ServiceError::PlacementUnavailable(refusal)
        if refusal.rejected_candidates.iter().any(|candidate| candidate.filter_codes == [services::placement::PlacementFilterCode::MachineCapacity])),
        "{error:?}"
    );
    let _: SettingsResponse = json_request_with_bearer(
        &app,
        Method::PUT,
        "/api/v1/settings",
        &admin,
        json!({"server":{"max_concurrent_runs":2}}),
        StatusCode::OK,
    )
    .await;
    state
        .task_service
        .claim_task(&tasks[1].id, services::Assignee::Agent(worker), None)
        .await
        .unwrap();
    let status = state
        .operator_status_service
        .compute_status()
        .await
        .unwrap();
    let host = status
        .daemon_pressure
        .iter()
        .find(|machine| machine.daemon_id == "server_host")
        .unwrap();
    assert_eq!(host.active_runs, 2);
    assert_eq!(host.max_concurrent_runs, Some(2));
    assert!(host.at_capacity);
    let _: SettingsResponse = json_request_with_bearer(
        &app,
        Method::PUT,
        "/api/v1/settings",
        &admin,
        json!({"server":{"max_concurrent_runs":4}}),
        StatusCode::OK,
    )
    .await;
    let status = state
        .operator_status_service
        .compute_status()
        .await
        .unwrap();
    let host = status
        .daemon_pressure
        .iter()
        .find(|machine| machine.daemon_id == "server_host")
        .unwrap();
    assert_eq!(host.active_runs, 2);
    assert_eq!(host.max_concurrent_runs, Some(4));
    assert!(!host.at_capacity);
    let response: SettingsResponse = empty_request_with_bearer(
        &app,
        Method::GET,
        "/api/v1/settings",
        &admin,
        StatusCode::OK,
    )
    .await;
    let cap = response
        .settings
        .iter()
        .find(|setting| setting.key == "server.max_concurrent_runs")
        .unwrap();
    assert_eq!(cap.effective_value, json!(4));
    assert!(!cap.restart_required);
    let _: Value = json_request_with_bearer(
        &app,
        Method::PUT,
        "/api/v1/settings",
        &common::test_jwt(),
        json!({"server":{"max_concurrent_runs":0}}),
        StatusCode::FORBIDDEN,
    )
    .await;
    assert_eq!(state.db.server_run_cap.effective(), Some(4));
    let response: SettingsResponse = json_request_with_bearer(
        &app,
        Method::PUT,
        "/api/v1/settings",
        &admin,
        json!({"server":{"max_concurrent_runs":null}}),
        StatusCode::OK,
    )
    .await;
    let cap = response
        .settings
        .iter()
        .find(|setting| setting.key == "server.max_concurrent_runs")
        .unwrap();
    assert_eq!(cap.value, Value::Null);
    assert!(cap.effective_value.as_u64().unwrap() >= 2);
}

#[tokio::test]
async fn machine_capacity_daemon_reports_and_admin_limit_over_local_socket() {
    let root = TestDir::new("machine-daemon-caps");
    let (app, state) = settings_app(root.path()).await;
    let registration: api_types::DaemonRegisterResponse = json_request(&app, Method::POST, "/api/v1/daemons/register", json!({"machine_id":"remote-capacity","hostname":"Remote","os":"linux","arch":"x64","max_concurrent_runs":3}), StatusCode::OK).await;
    let recorded: DaemonResponse = empty_request_with_bearer(
        &app,
        Method::GET,
        &format!("/api/v1/daemons/{}", registration.daemon_id),
        &common::admin_jwt(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(recorded.max_concurrent_runs, Some(3));
    let server = common::fake_daemon::TestServer::start(state.clone()).await;
    let client = forge_client::daemon_link::DaemonClient::new(format!("http://{}", server.addr))
        .unwrap()
        .http;
    let url = format!(
        "http://{}/api/v1/daemons/{}",
        server.addr, registration.daemon_id
    );
    let response = client
        .post(format!("{url}/report"))
        .bearer_auth(&registration.registration_token)
        .json(&json!({"detected_clis":[],"max_concurrent_runs":3}))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let daemon: DaemonResponse = response.json().await.unwrap();
    assert_eq!(daemon.max_concurrent_runs, Some(3));
    let old_report = client
        .post(format!("{url}/report"))
        .bearer_auth(&registration.registration_token)
        .json(&json!({"detected_clis":[],"labels":{"max_sessions":9}}))
        .send()
        .await
        .unwrap();
    let unchanged: DaemonResponse = old_report.json().await.unwrap();
    assert_eq!(unchanged.max_concurrent_runs, Some(3));
    let admin = common::admin_jwt();
    let mut daemon = unchanged;
    for (limit, expected) in [(Some(2), Some(2)), (Some(8), Some(3)), (None, Some(3))] {
        let next: DaemonResponse = json_request_with_bearer(
            &app,
            Method::PATCH,
            &format!("/api/v1/daemons/{}", daemon.id),
            &admin,
            json!({"version":daemon.version,"run_limit":limit}),
            StatusCode::OK,
        )
        .await;
        assert_eq!(next.effective_max_concurrent_runs, expected);
        let _: Value = json_request_with_bearer(
            &app,
            Method::PATCH,
            &format!("/api/v1/daemons/{}", daemon.id),
            &admin,
            json!({"version":daemon.version,"run_limit":limit}),
            StatusCode::CONFLICT,
        )
        .await;
        daemon = next;
    }
    let _: Value = json_request_with_bearer(
        &app,
        Method::PATCH,
        &format!("/api/v1/daemons/{}", daemon.id),
        &common::test_jwt(),
        json!({"version":daemon.version,"run_limit":2}),
        StatusCode::FORBIDDEN,
    )
    .await;
    let _: Value = json_request_with_bearer(
        &app,
        Method::PATCH,
        &format!("/api/v1/daemons/{}", daemon.id),
        &admin,
        json!({"version":daemon.version,"run_limit":0}),
        StatusCode::BAD_REQUEST,
    )
    .await;
}
