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

/// A daemon's report carries the disk facts of its workspace root; the reply
/// carries the floor back; the machine reads and operator status show each
/// machine's reading with the floor applied.
#[tokio::test]
async fn machine_disk_facts_arrive_with_the_report_and_show_on_every_machine_read() {
    use api_types::{DiskFloor, DiskPressureKind, MachineDiskFacts};
    let root = TestDir::new("machine-daemon-disk");
    let (app, state) = settings_app(root.path()).await;
    let registration: api_types::DaemonRegisterResponse = json_request(
        &app,
        Method::POST,
        "/api/v1/daemons/register",
        json!({"machine_id":"remote-disk","hostname":"Remote","os":"linux","arch":"x64"}),
        StatusCode::OK,
    )
    .await;
    let report_url = format!("/api/v1/daemons/{}/report", registration.daemon_id);
    let reading = json!({"free_bytes":40,"total_bytes":1000,"free_inodes":5,"total_inodes":100,"measured_at":"2026-10-10T00:00:00Z","gc_state":"claimed_by_other"});

    // Nothing built for a test turns admission on: the reading is kept, and
    // no floor is applied or sent.
    let quiet: DaemonResponse = json_request_with_bearer(
        &app,
        Method::POST,
        &report_url,
        &registration.registration_token,
        json!({"detected_clis":[],"disk":reading}),
        StatusCode::OK,
    )
    .await;
    assert!(quiet.disk.is_none() && quiet.workspace_floor.is_none());

    // The running server configures the floor and reads its own root.
    let floor = DiskFloor::of_bytes(100, 0);
    state.db.disk_admission.configure(
        floor,
        Arc::new(|| {
            Some(MachineDiskFacts {
                free_bytes: 900,
                total_bytes: 1_000,
                free_inodes: None,
                total_inodes: None,
                measured_at: "2026-10-10T00:00:00Z".to_owned(),
                gc_state: Some("owned".to_owned()),
                compiler_cache_bytes: None,
            })
        }),
    );
    let reported: DaemonResponse = json_request_with_bearer(
        &app,
        Method::POST,
        &report_url,
        &registration.registration_token,
        json!({"detected_clis":[],"disk":reading}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        reported.workspace_floor,
        Some(floor),
        "the floor travels back"
    );
    let disk = reported
        .disk
        .expect("the reply shows the reading it carried");
    assert_eq!(
        (
            disk.facts.free_bytes,
            disk.facts.free_inodes,
            disk.floor_bytes
        ),
        (40, Some(5), 100)
    );
    assert_eq!(disk.pressure, Some(DiskPressureKind::Bytes));
    assert_eq!(disk.facts.gc_state.as_deref(), Some("claimed_by_other"));

    // A report without a reading keeps the last one.
    let kept: DaemonResponse = json_request_with_bearer(
        &app,
        Method::POST,
        &report_url,
        &registration.registration_token,
        json!({"detected_clis":[]}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(kept.disk, Some(disk.clone()));
    let read: DaemonResponse = empty_request_with_bearer(
        &app,
        Method::GET,
        &format!("/api/v1/daemons/{}", registration.daemon_id),
        &common::admin_jwt(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(read.disk, Some(disk));

    // Operator status: the machine under its floor is visible as such, the
    // server host is not.
    let status = state
        .operator_status_service
        .compute_status()
        .await
        .unwrap();
    let pressure = |id: &str| {
        status
            .daemon_pressure
            .iter()
            .find(|machine| machine.daemon_id == id)
            .and_then(|machine| machine.disk.clone())
            .map(|disk| disk.pressure)
    };
    assert_eq!(pressure("server_host"), Some(None));
    assert_eq!(
        pressure(&registration.daemon_id),
        Some(Some(DiskPressureKind::Bytes))
    );

    // The next report shows the disk recovered: nothing else is needed.
    let recovered: DaemonResponse = json_request_with_bearer(
        &app,
        Method::POST,
        &report_url,
        &registration.registration_token,
        json!({"detected_clis":[],"disk":{"free_bytes":800,"total_bytes":1000,"measured_at":"2026-10-10T00:01:00Z"}}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(recovered.disk.unwrap().pressure, None);
}
