#![allow(dead_code)]

mod common;

use api_types::{ErrorResponse, ProjectExecutionSetupResponse, ProjectResponse};
use axum::{http::Method, http::StatusCode, Router};
use db::{
    now_rfc3339, AgentConnectionHealthRepo, AgentRepo, AgentStatus, CreateAgentIdentity,
    CreateAgentProfile, CreateRepo, RepoRepo, UpsertAgentConnectionHealth, WorkMode,
};
use serde_json::json;

async fn create_native_agent(app: &common::Harness, name: &str) -> String {
    let identity_id = uuid::Uuid::new_v4().to_string();
    let profile_id = uuid::Uuid::new_v4().to_string();
    let now = now_rfc3339();
    AgentRepo::create_identity_with_profile(
        &*app.state.db,
        CreateAgentIdentity {
            id: identity_id.clone(),
            name: name.to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: profile_id.clone(),
            identity_id: identity_id.clone(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("same-provider".to_owned()),
            model: Some("same-model".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("native agent creates");
    AgentConnectionHealthRepo::upsert_connection_health(
        &*app.state.db,
        UpsertAgentConnectionHealth {
            profile_id,
            status: "healthy".to_owned(),
            capability_status_json: "{}".to_owned(),
            checked_at: Some(now.clone()),
            error_code: None,
            updated_at: now,
        },
    )
    .await
    .expect("native agent health creates");
    identity_id
}

async fn create_remote_repo(app: &common::Harness, project_id: &str) -> String {
    let now = now_rfc3339();
    RepoRepo::create(
        &*app.state.db,
        CreateRepo {
            id: uuid::Uuid::new_v4().to_string(),
            project_id: project_id.to_owned(),
            name: "remote-setup-repo".to_owned(),
            remote_url: "https://example.invalid/forge/setup".to_owned(),
            local_path: None,
            work_mode: WorkMode::DirectMerge,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("remote repository creates")
    .id
}

#[tokio::test]
async fn execution_setup_routes_enforce_idempotency_policy_and_ready_projection() {
    let workspace = common::TestDir::new("execution-setup-routes");
    let harness = common::test_app(workspace.path(), "execution-setup-routes").await;
    let app: &Router = &harness.app;
    let token = common::test_jwt();

    let project: ProjectResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({ "name": "Execution setup API" }),
        StatusCode::OK,
    )
    .await;
    let created_setup = project
        .execution_setup
        .clone()
        .expect("direct Project creation returns current setup projection");
    assert_eq!(created_setup.project_id, project.id);
    assert_eq!(created_setup.project_version, project.version);
    assert_eq!(
        created_setup.coordination_state,
        api_types::CoordinationState::SetupRequired
    );
    assert_eq!(
        created_setup.execution_setup_state,
        api_types::ExecutionSetupState::SetupRequired
    );
    assert_eq!(
        created_setup.execution_gate,
        api_types::ExecutionGate::Active
    );
    let worker_id = create_native_agent(&harness, "API Worker").await;
    let reviewer_id = create_native_agent(&harness, "API Reviewer").await;

    let initial: ProjectExecutionSetupResponse = common::empty_request_with_bearer(
        app,
        Method::GET,
        &format!("/api/v1/projects/{}/execution-setup", project.id),
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(initial.project_version, project.version);
    assert_eq!(initial.project_id, created_setup.project_id);
    assert_eq!(initial.coordination_state, created_setup.coordination_state);
    assert_eq!(
        initial.execution_setup_state,
        created_setup.execution_setup_state
    );
    assert_eq!(initial.execution_gate, created_setup.execution_gate);

    let selected_worker: ProjectExecutionSetupResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!("/api/v1/projects/{}/execution-setup/worker", project.id),
        &token,
        json!({
            "identity_id": worker_id,
            "expected_project_version": initial.project_version,
            "idempotency_key": "api-worker-1"
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        selected_worker
            .worker
            .as_ref()
            .map(|agent| &agent.identity_id),
        Some(&worker_id)
    );

    let replayed_worker: ProjectExecutionSetupResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!("/api/v1/projects/{}/execution-setup/worker", project.id),
        &token,
        json!({
            "identity_id": worker_id,
            "expected_project_version": initial.project_version,
            "idempotency_key": "api-worker-1"
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(replayed_worker.worker, selected_worker.worker);
    assert_eq!(
        replayed_worker.project_version,
        selected_worker.project_version
    );

    let changed_input: ErrorResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!("/api/v1/projects/{}/execution-setup/worker", project.id),
        &token,
        json!({
            "identity_id": reviewer_id,
            "expected_project_version": initial.project_version,
            "idempotency_key": "api-worker-1"
        }),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(changed_input.code, "idempotency_conflict");

    let same_identity: ProjectExecutionSetupResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/execution-setup/independent-reviewer",
            project.id
        ),
        &token,
        json!({
            "identity_id": worker_id,
            "expected_project_version": selected_worker.project_version,
            "idempotency_key": "api-reviewer-same"
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        same_identity
            .independent_reviewer
            .as_ref()
            .map(|agent| &agent.identity_id),
        Some(&worker_id)
    );

    let selected_reviewer: ProjectExecutionSetupResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/execution-setup/independent-reviewer",
            project.id
        ),
        &token,
        json!({
            "identity_id": reviewer_id,
            "expected_project_version": same_identity.project_version,
            "idempotency_key": "api-reviewer-1"
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        selected_reviewer
            .independent_reviewer
            .as_ref()
            .map(|agent| &agent.identity_id),
        Some(&reviewer_id)
    );

    let repo_id = create_remote_repo(&harness, &project.id).await;
    let attached: ProjectExecutionSetupResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!("/api/v1/projects/{}/execution-setup/repository", project.id),
        &token,
        json!({
            "repo_id": repo_id,
            "expected_project_version": selected_reviewer.project_version,
            "idempotency_key": "api-repository-1"
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        attached.execution_setup_state,
        api_types::ExecutionSetupState::Ready
    );
    assert_eq!(
        attached
            .provisioning
            .as_ref()
            .map(|operation| operation.status.as_str()),
        Some("ready")
    );
    assert_eq!(
        attached
            .worker
            .as_ref()
            .and_then(|agent| agent.provider.as_deref()),
        Some("same-provider")
    );
    assert_eq!(
        attached
            .independent_reviewer
            .as_ref()
            .and_then(|agent| agent.provider.as_deref()),
        Some("same-provider")
    );
}
