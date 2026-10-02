#![allow(dead_code)]
mod common;
use api_types::{AgentResponse, ProjectEnvironmentRecheckResponse, ProjectResponse};
use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use serde_json::json;
use tower::ServiceExt;

#[tokio::test]
async fn recheck_machine_selection_empty_project_and_authentication() {
    let root = tempfile::tempdir().unwrap();
    let repo = common::setup_git_repo(root.path());
    let harness = common::test_app(root.path(), "environment-surfaces").await;
    let (id, _) = common::create_project_and_repo(&harness.app, "Surface", &repo).await;
    let project: ProjectResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{id}"),
        StatusCode::OK,
    )
    .await;
    assert!(project.environment_readiness.is_empty());
    let path = format!("/api/v1/projects/{id}/environment/recheck");
    let empty: ProjectEnvironmentRecheckResponse =
        common::json_request(&harness.app, Method::POST, &path, json!({}), StatusCode::OK).await;
    assert!(empty.machines.is_empty());
    let _: api_types::ErrorResponse = common::json_request(
        &harness.app,
        Method::POST,
        &path,
        json!({"machine":"missing"}),
        StatusCode::NOT_FOUND,
    )
    .await;
    let project: ProjectResponse = common::json_request(&harness.app,Method::PATCH,&format!("/api/v1/projects/{id}"),json!({"version":project.version,"settings":{"environment":{"checks":[{"name":"toolchain","command":"printf ready"}]}}}),StatusCode::OK).await;
    let checked: ProjectEnvironmentRecheckResponse = common::json_request(
        &harness.app,
        Method::POST,
        &path,
        json!({"machine":"server"}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(checked.machines.len(), 1);
    assert_eq!(checked.machines[0].machine.name, "Server host");
    assert!(checked.machines[0].checks[0].passed);
    assert!(checked.machines[0].error.is_none());
    let visible: ProjectResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{id}"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(visible.environment_readiness.len(), 1);
    assert_eq!(
        visible.environment_readiness[0].status,
        api_types::EnvironmentReadinessStatus::Ready
    );
    assert!(visible.environment_readiness[0].checked_at.is_some());
    assert!(visible.environment_readiness[0].next_check_at.is_none());
    assert_eq!(visible.version, project.version);
    let response = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(&path)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn agent_runnable_on_identities_are_admin_only_and_pin_can_be_cleared() {
    let root = tempfile::tempdir().unwrap();
    let harness = common::test_app(root.path(), "agent-machines").await;
    let agent: AgentResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        "/api/v1/agents",
        &common::admin_jwt(),
        json!({"name":"Shell","executor_type":"shell"}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(agent.runnable_on.count, 1);
    let machines = agent.runnable_on.machines.unwrap();
    assert_eq!(machines[0].name, "Server host");
    assert!(agent.daemon_id.is_none());
    let path = format!("/api/v1/agents/{}", agent.id);
    let hidden: serde_json::Value =
        common::empty_request(&harness.app, Method::GET, &path, StatusCode::OK).await;
    assert_eq!(hidden["runnable_on"]["count"], 1);
    assert!(hidden["runnable_on"].get("machines").is_none());
    assert!(hidden["daemon_id"].is_null());
    let _: api_types::ErrorResponse = common::json_request(
        &harness.app,
        Method::PATCH,
        &path,
        json!({"version":agent.version,"daemon_id":null}),
        StatusCode::FORBIDDEN,
    )
    .await;
    let (pinned_id, _) = common::create_shell_agents(&harness.app, root.path(), "pin-clear").await;
    let row = db::AgentRepo::get_by_id(&*harness.state.db, &pinned_id)
        .await
        .unwrap()
        .unwrap();
    let pinned: AgentResponse = common::json_request_with_bearer(
        &harness.app,
        Method::PATCH,
        &format!("/api/v1/agents/{pinned_id}"),
        &common::admin_jwt(),
        json!({"version":row.version}),
        StatusCode::OK,
    )
    .await;
    assert!(pinned.daemon_id.is_some());
    let cleared: AgentResponse = common::json_request_with_bearer(
        &harness.app,
        Method::PATCH,
        &format!("/api/v1/agents/{pinned_id}"),
        &common::admin_jwt(),
        json!({"version":pinned.version,"daemon_id":null}),
        StatusCode::OK,
    )
    .await;
    assert!(cleared.daemon_id.is_none());
    assert_eq!(cleared.runnable_on.count, 1);
    let disabled: AgentResponse = common::json_request_with_bearer(
        &harness.app,
        Method::PATCH,
        &path,
        &common::admin_jwt(),
        json!({"version":agent.version,"paused":true,"daemon_id":null}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(disabled.runnable_on.count, 0);
    assert!(disabled.runnable_on.machines.unwrap().is_empty());
}

#[tokio::test]
async fn task_response_exposes_recorded_environment_and_capacity_waits() {
    let root = tempfile::tempdir().unwrap();
    let repo = common::setup_git_repo(root.path());
    let harness = common::test_app(root.path(), "placement-diagnostics").await;
    let (id, _) = common::create_project_and_repo(&harness.app, "Placement", &repo).await;
    let task: api_types::TaskResponse = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{id}/tasks"),
        json!({"title":"Wait"}),
        StatusCode::OK,
    )
    .await;
    sqlx::query("UPDATE task SET metadata_json=? WHERE id=?").bind(json!({"environment_wait":{"machine":{"owner_kind":"server"},"checks":["cargo"]},"deferred_dispatch":{"kind":"environment_probe_pending"},"dispatch_disposition":{"task_version":task.version-1,"capability":"machine_capacity","blocker_digest":"test","recorded_at":db::now_rfc3339(),"safe_message":"Machine run capacity reached"}}).to_string()).bind(&task.id).execute(harness.state.db.pool()).await.unwrap();
    let read: api_types::TaskResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", task.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        read.placement_diagnostics.len(),
        3,
        "{:?}",
        read.placement_diagnostics
    );
    assert_eq!(read.placement_diagnostics[0].failing_checks, vec!["cargo"]);
    assert_eq!(
        read.placement_diagnostics[1].machine.as_ref().unwrap().name,
        "Server host"
    );
    assert_eq!(
        read.placement_diagnostics[2].filter_codes,
        vec!["machine_capacity"]
    );
}

#[tokio::test]
async fn grouped_recheck_never_substitutes_host_results_for_an_unavailable_daemon() {
    use db::{ProjectMachineReadinessRepo, ProjectRepo};
    let root = tempfile::tempdir().unwrap();
    let repo = common::setup_git_repo(root.path());
    let harness = common::test_app(root.path(), "grouped-recheck").await;
    let (id, _) = common::create_project_and_repo(&harness.app, "Grouped", &repo).await;
    let project: ProjectResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{id}"),
        StatusCode::OK,
    )
    .await;
    let _: ProjectResponse = common::json_request(&harness.app,Method::PATCH,&format!("/api/v1/projects/{id}"),json!({"version":project.version,"settings":{"environment":{"checks":[{"name":"cargo","command":"printf host-ready"}]}}}),StatusCode::OK).await;
    let project = ProjectRepo::get_by_id(&*harness.state.db, &id)
        .await
        .unwrap()
        .unwrap();
    let env: api_types::ProjectSettings = serde_json::from_str(&project.settings).unwrap();
    let machine = db::EnvironmentMachine::Daemon {
        daemon_id: "missing-daemon".into(),
        runtime_id: "runtime-mac".into(),
    };
    let row = db::ProjectMachineReadiness {
        project_id: id.clone(),
        machine: machine.clone(),
        status: db::EnvironmentReadinessStatus::NotReady,
        checks_digest: db::environment_checks_digest(&env.environment),
        failing_checks: vec![db::ReadinessCheckFailure {
            name: "cargo".into(),
            output_tail: "not installed".into(),
        }],
        check_results: vec![],
        output_tail: "not installed".into(),
        scope_covered: "full".into(),
        role: Some("coder".into()),
        workspace_id: None,
        checked_at: Some("2026-10-02T00:00:00Z".into()),
        next_check_at: Some("2000-01-01T00:00:00Z".into()),
        version: 1,
    };
    harness.state.db.put_readiness(row, None).await.unwrap();
    let path = format!("/api/v1/projects/{id}/environment/recheck");
    let response: ProjectEnvironmentRecheckResponse = common::json_request(
        &harness.app,
        Method::POST,
        &path,
        json!({"machine":"runtime-mac"}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(response.machines.len(), 1);
    assert_eq!(response.machines[0].machine.id, "runtime-mac");
    assert!(response.machines[0].checks.is_empty());
    assert!(response.machines[0].error.is_some());
    let retained = harness
        .state
        .db
        .get_readiness(&id, &machine)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.output_tail, "not installed");
    assert_eq!(retained.checked_at.as_deref(), Some("2026-10-02T00:00:00Z"));
    assert_ne!(
        retained.next_check_at.as_deref(),
        Some("2000-01-01T00:00:00Z")
    );
    let all: ProjectEnvironmentRecheckResponse =
        common::json_request(&harness.app, Method::POST, &path, json!({}), StatusCode::OK).await;
    assert_eq!(all.machines.len(), 2);
    let host = all
        .machines
        .iter()
        .find(|entry| entry.machine.id == "server")
        .unwrap();
    assert_eq!(host.checks[0].output_tail, "host-ready");
    let remote = all
        .machines
        .iter()
        .find(|entry| entry.machine.id == "runtime-mac")
        .unwrap();
    assert!(remote.error.is_some());
}

#[tokio::test]
async fn server_recheck_without_repository_explains_missing_project_location() {
    let root = tempfile::tempdir().unwrap();
    let harness = common::test_app(root.path(), "server-no-repository").await;
    let project: ProjectResponse = common::json_request(&harness.app,Method::POST,"/api/v1/projects",
        json!({"name":"No repository","settings":{"environment":{"checks":[{"name":"cargo","command":"true"}]}}}),StatusCode::OK).await;
    let path = format!("/api/v1/projects/{}/environment/recheck", project.id);
    let error: api_types::ErrorResponse = common::json_request(
        &harness.app,
        Method::POST,
        &path,
        json!({"machine":"server"}),
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(error.code, "not_found");
    assert!(
        error
            .message
            .contains("repository location on server for project"),
        "{}",
        error.message
    );
    assert!(error.message.contains(&project.id));
    assert!(!error.message.contains("machine not found"));
    let unknown: api_types::ErrorResponse = common::json_request(
        &harness.app,
        Method::POST,
        &path,
        json!({"machine":"unknown-runtime"}),
        StatusCode::NOT_FOUND,
    )
    .await;
    assert!(unknown.message.contains("machine not found"));
    let current: ProjectResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{}", project.id),
        StatusCode::OK,
    )
    .await;
    assert!(current.environment_readiness.is_empty());
}
