#![allow(dead_code)]

mod common;

use api_types::{
    ErrorResponse, PaginatedResponse, ProjectEnvironmentPause, ProjectEnvironmentRecheckResponse,
    ProjectResponse,
};
use axum::http::{Method, StatusCode};
use db::{now_rfc3339, ProjectRepo, UpdateProject};
use serde_json::json;

async fn environment_project(
    harness: &common::Harness,
    repo_path: &std::path::Path,
) -> ProjectResponse {
    let (id, repo_id) =
        common::create_project_and_repo(&harness.app, "Environment", repo_path).await;
    let project = ProjectRepo::get_by_id(&*harness.state.db, &id)
        .await
        .unwrap()
        .unwrap();
    ProjectRepo::update_at_version(&*harness.state.db, UpdateProject {
        id: id.clone(), name: None, settings: Some(json!({"environment": {
            "env": {"TOKEN": "private-value"},
            "checks": [
                {"name": "disk", "command": "if test -f recovered; then echo 'root free: 17G'; else echo 'root free: 7G'; exit 1; fi", "roles": ["coder"]},
                {"name": "browser", "command": "touch browser-checked; printf '%s ready' \"$TOKEN\"", "roles": ["reviewer"]}
            ]
        }}).to_string()), primary_repo_id: Some(Some(repo_id)), paused_at: None, updated_at: now_rfc3339(),
    }, project.version, None).await.unwrap();
    common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{id}"),
        StatusCode::OK,
    )
    .await
}

async fn pause_environment(harness: &common::Harness, id: &str) {
    let project = ProjectRepo::get_by_id(&*harness.state.db, id)
        .await
        .unwrap()
        .unwrap();
    let now = now_rfc3339();
    let detail = ProjectEnvironmentPause {
        workspace_id: None,
        checks: vec!["disk".to_owned()],
        role: Some("coder".to_owned()),
        output: "root free: 7G".to_owned(),
        paused_at: now.clone(),
        last_checked_at: now.clone(),
        next_check_at: "2099-01-01T00:00:00Z".to_owned(),
    };
    assert!(ProjectRepo::set_environment_pause_if_unchanged(
        &*harness.state.db,
        id,
        project.version,
        &now,
        &serde_json::to_string(&detail).unwrap(),
    )
    .await
    .unwrap());
}

#[tokio::test]
async fn owner_environment_recheck_reports_all_checks_and_resumes() {
    let root = common::TestDir::new("environment-recheck-api");
    let repo = common::setup_git_repo(root.path());
    let harness = common::test_app(root.path(), "environment-recheck-api").await;
    let project = environment_project(&harness, &repo).await;
    pause_environment(&harness, &project.id).await;
    sqlx::query("UPDATE project SET environment_pause_json = json_set(environment_pause_json, '$.workspace_id', 'deleted-workspace') WHERE id = ?")
        .bind(&project.id)
        .execute(harness.state.db.pool()).await.unwrap();
    let visible: ProjectResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{}", project.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        visible.environment_pause.as_ref().unwrap().detail.checks,
        vec!["disk"]
    );
    assert_eq!(
        visible
            .environment_pause
            .as_ref()
            .unwrap()
            .detail
            .workspace_id
            .as_deref(),
        Some("deleted-workspace")
    );
    let list: PaginatedResponse<ProjectResponse> = common::empty_request(
        &harness.app,
        Method::GET,
        "/api/v1/projects",
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        list.items
            .iter()
            .find(|item| item.id == project.id)
            .unwrap()
            .environment_pause,
        visible.environment_pause
    );
    let uri = format!("/api/v1/projects/{}/environment/recheck", project.id);
    let mut events = harness.state.event_bus.subscribe();
    let failed: ProjectEnvironmentRecheckResponse =
        common::empty_request(&harness.app, Method::POST, &uri, StatusCode::OK).await;
    assert_eq!(failed.machines.len(), 1);
    assert_eq!(failed.machines[0].machine.id, "server");
    assert_eq!(failed.project.environment_readiness.len(), 1);
    assert_eq!(
        failed.project.environment_readiness[0].status,
        api_types::EnvironmentReadinessStatus::NotReady
    );
    assert_eq!(failed.machines[0].checks.len(), 2);
    assert!(!failed.machines[0].checks[0].passed);
    assert_eq!(failed.machines[0].checks[0].exit_code, Some(1));
    assert!(failed.machines[0].checks[0]
        .output_tail
        .contains("root free: 7G"));
    assert!(
        failed.machines[0].checks[1].passed,
        "role-scoped checks run regardless of role"
    );
    assert_eq!(failed.machines[0].checks[1].exit_code, Some(0));
    assert_eq!(failed.machines[0].checks[1].output_tail, "[REDACTED] ready");
    assert!(repo.join("browser-checked").exists());
    assert!(failed.project.paused);
    assert_eq!(
        failed
            .project
            .environment_pause
            .as_ref()
            .unwrap()
            .detail
            .checks,
        vec!["disk"]
    );
    assert_eq!(events.try_recv().unwrap().event_type, "project.updated");
    std::fs::write(repo.join("recovered"), "yes").unwrap();
    let passed: ProjectEnvironmentRecheckResponse =
        common::empty_request(&harness.app, Method::POST, &uri, StatusCode::OK).await;
    assert!(passed.machines[0].checks.iter().all(|check| check.passed));
    assert!(passed.machines[0].checks[0]
        .output_tail
        .contains("root free: 17G"));
    assert!(!passed.project.paused);
    assert!(passed.project.environment_pause.is_none());
    assert!(passed.project.system_pause_reason.is_none());
    assert_eq!(events.try_recv().unwrap().event_type, "project.resumed");
}

#[tokio::test]
async fn manual_resume_clears_environment_pause_and_check_now_preserves_user_pause() {
    let root = common::TestDir::new("environment-manual-resume-api");
    let repo = common::setup_git_repo(root.path());
    let harness = common::test_app(root.path(), "environment-manual-resume-api").await;
    let project = environment_project(&harness, &repo).await;
    pause_environment(&harness, &project.id).await;
    let resumed: ProjectResponse = common::empty_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{}/resume", project.id),
        StatusCode::OK,
    )
    .await;
    assert!(!resumed.paused);
    assert!(resumed.environment_pause.is_none());
    assert!(resumed.system_pause_reason.is_none());
    let paused: ProjectResponse = common::empty_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{}/pause", project.id),
        StatusCode::OK,
    )
    .await;
    std::fs::write(repo.join("recovered"), "yes").unwrap();
    let checked: ProjectEnvironmentRecheckResponse = common::empty_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{}/environment/recheck", project.id),
        StatusCode::OK,
    )
    .await;
    assert!(checked.machines[0].checks.iter().all(|check| check.passed));
    assert_eq!(checked.project.paused_at, paused.paused_at);
    assert!(checked.project.system_pause_reason.is_none());
    assert!(checked.project.environment_pause.is_none());
}

#[tokio::test]
async fn project_patch_refuses_invalid_environment_recheck_interval() {
    let root = common::TestDir::new("environment-interval-api");
    let harness = common::test_app(root.path(), "environment-interval-api").await;
    let project: ProjectResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({"name": "Interval"}),
        StatusCode::OK,
    )
    .await;
    for interval in [5, 59, 86401] {
        let error: ErrorResponse = common::json_request(&harness.app, Method::PATCH, &format!("/api/v1/projects/{}", project.id), json!({"version": project.version, "settings": {"environment": {"recheck_interval_seconds": interval}}}), StatusCode::BAD_REQUEST).await;
        assert!(error.message.contains("recheck_interval_seconds"));
    }
    let accepted: ProjectResponse = common::json_request(&harness.app, Method::PATCH, &format!("/api/v1/projects/{}", project.id), json!({"version": project.version, "settings": {"environment": {"recheck_interval_seconds": 60}}}), StatusCode::OK).await;
    assert_eq!(
        accepted.settings["environment"]["recheck_interval_seconds"],
        60
    );
}

#[tokio::test]
async fn project_patch_refuses_unbounded_environment_check_timeout() {
    let root = common::TestDir::new("environment-timeout-api");
    let harness = common::test_app(root.path(), "environment-timeout-api").await;
    let project: ProjectResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({"name":"Timeout"}),
        StatusCode::OK,
    )
    .await;
    for timeout in [0, 301, u64::MAX] {
        let error: ErrorResponse = common::json_request(&harness.app, Method::PATCH,
            &format!("/api/v1/projects/{}", project.id), json!({"version":project.version,
                "settings":{"environment":{"checks":[{"name":"disk", "command":"true", "timeout_seconds":timeout}]}}}),
            StatusCode::BAD_REQUEST).await;
        assert!(error.message.contains("timeout_seconds"));
    }
}
