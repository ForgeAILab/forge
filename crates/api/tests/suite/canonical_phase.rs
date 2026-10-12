#![allow(clippy::assertions_on_constants)]

use crate::common;

use std::collections::HashMap;

use api_types::{CanonicalPhase, TaskResponse, TasksResponse};
use axum::{http::Method, Router};
use common::{
    create_project_and_repo, empty_request, json_request, setup_git_repo, test_app, TestDir,
};
use serde_json::{json, Value};

const STRICT_WORKFLOW: &str =
    include_str!("../../../services/tests/fixtures/default_strict_workflow.json");
const LEGACY_CUSTOM_WORKFLOW: &str =
    include_str!("../../../services/tests/fixtures/legacy_custom_workflow.json");
const STRICT_TASK_STATES: &[(&str, CanonicalPhase)] = &[
    ("backlog", CanonicalPhase::Backlog),
    ("todo", CanonicalPhase::Ready),
    ("in_progress", CanonicalPhase::Working),
    ("review", CanonicalPhase::Review),
    ("done", CanonicalPhase::Done),
    ("cancelled", CanonicalPhase::Done),
];
const CUSTOM_TASK_STATES: &[(&str, CanonicalPhase)] = &[
    ("ideas", CanonicalPhase::Backlog),
    ("triage", CanonicalPhase::Ready),
    ("building", CanonicalPhase::Working),
    ("verification", CanonicalPhase::Review),
    ("released", CanonicalPhase::Done),
];
const LEGACY_TASK_STATES: &[(&str, CanonicalPhase)] = &[
    ("backlog", CanonicalPhase::Backlog),
    ("todo", CanonicalPhase::Ready),
    ("in_progress", CanonicalPhase::Working),
    ("review", CanonicalPhase::Review),
    ("done", CanonicalPhase::Done),
];

#[tokio::test]
async fn task_responses_and_phase_filters_cover_strict_custom_and_legacy_workflows() {
    let workspace_root = TestDir::new("forge-canonical-phase-workspaces");
    let repo_root = TestDir::new("forge-canonical-phase-repo");
    let repo_path = setup_git_repo(repo_root.path());
    let harness = test_app(workspace_root.path(), "canonical-phase").await;

    let workflows = [
        ("strict", STRICT_WORKFLOW, STRICT_TASK_STATES),
        ("custom", LEGACY_CUSTOM_WORKFLOW, CUSTOM_TASK_STATES),
        ("legacy-default", "{}", LEGACY_TASK_STATES),
    ];

    for (name, workflow, task_states) in workflows {
        let (project_id, _) = create_project_and_repo(
            &harness.app,
            &format!("Canonical phases {name}"),
            &repo_path,
        )
        .await;
        set_project_workflow(&harness, &project_id, workflow).await;

        let mut task_ids = Vec::new();
        for &(status, expected_phase) in task_states {
            let task = create_task(&harness.app, &project_id, &format!("{name}-{status}")).await;
            set_task_status(&harness, &task.id, status).await;
            task_ids.push((task.id, status, expected_phase));
        }

        let response: TasksResponse = empty_request(
            &harness.app,
            Method::GET,
            &format!(
                "/api/v1/projects/{project_id}/tasks?include_cancelled=true&sort_by=id&sort_order=asc"
            ),
            axum::http::StatusCode::OK,
        )
        .await;
        let expected_by_status: HashMap<_, _> = task_ids
            .iter()
            .map(|(_, status, phase)| (*status, *phase))
            .collect();
        for task in response.items {
            assert_eq!(
                task.canonical_phase,
                expected_by_status[task.status.as_str()],
                "{name} task {} has the wrong canonical phase",
                task.status
            );
        }

        for phase in [
            CanonicalPhase::Backlog,
            CanonicalPhase::Ready,
            CanonicalPhase::Working,
            CanonicalPhase::Review,
            CanonicalPhase::Done,
        ] {
            let phase_name = phase_name(phase);
            let filtered: TasksResponse = empty_request(
                &harness.app,
                Method::GET,
                &format!(
                    "/api/v1/projects/{project_id}/tasks?canonical_phase={phase_name}&include_cancelled=true&sort_by=id&sort_order=asc"
                ),
                axum::http::StatusCode::OK,
            )
            .await;
            assert!(
                !filtered.items.is_empty(),
                "{name} has no {phase_name} tasks"
            );
            assert!(
                filtered
                    .items
                    .iter()
                    .all(|task| task.canonical_phase == phase),
                "{name} phase filter returned a different phase"
            );
        }

        let (status_a, phase_a) = task_states[0];
        let (status_b, phase_b) = task_states[1];
        let composed: TasksResponse = empty_request(
            &harness.app,
            Method::GET,
            &format!(
                "/api/v1/projects/{project_id}/tasks?status={status_a},{status_b}&canonical_phase={},{}&include_cancelled=true",
                phase_name(phase_a),
                phase_name(phase_b)
            ),
            axum::http::StatusCode::OK,
        )
        .await;
        assert!(
            composed
                .items
                .iter()
                .all(|task| task.status == status_a || task.status == status_b),
            "status and canonical phase filters were not intersected"
        );
        assert_eq!(composed.items.len(), 2);

        if name == "strict" {
            assert!(
                empty_request::<TasksResponse>(
                    &harness.app,
                    Method::GET,
                    &format!(
                        "/api/v1/projects/{project_id}/tasks?canonical_phase=done&sort_by=id&sort_order=asc"
                    ),
                    axum::http::StatusCode::OK,
                )
                .await
                .items
                .iter()
                .any(|task| task.status == "cancelled"),
                "canonical done intentionally includes cancelled tasks"
            );
        }
    }
}

#[tokio::test]
async fn canonical_phase_filter_preserves_ordered_pagination() {
    let workspace_root = TestDir::new("forge-canonical-phase-pagination-workspaces");
    let repo_root = TestDir::new("forge-canonical-phase-pagination-repo");
    let repo_path = setup_git_repo(repo_root.path());
    let harness = test_app(workspace_root.path(), "canonical-phase-pagination").await;
    let (project_id, _) =
        create_project_and_repo(&harness.app, "Canonical phase pagination", &repo_path).await;

    let mut working_ids = Vec::new();
    for index in 0..5 {
        let task = create_task(&harness.app, &project_id, &format!("working-{index}")).await;
        set_task_status(&harness, &task.id, "in_progress").await;
        working_ids.push(task.id);
    }
    let backlog = create_task(&harness.app, &project_id, "backlog").await;
    set_task_status(&harness, &backlog.id, "backlog").await;

    let full: TasksResponse = empty_request(
        &harness.app,
        Method::GET,
        &format!(
            "/api/v1/projects/{project_id}/tasks?canonical_phase=working&sort_by=id&sort_order=asc&limit=100"
        ),
        axum::http::StatusCode::OK,
    )
    .await;
    let first: TasksResponse = empty_request(
        &harness.app,
        Method::GET,
        &format!(
            "/api/v1/projects/{project_id}/tasks?canonical_phase=working&sort_by=id&sort_order=asc&limit=2"
        ),
        axum::http::StatusCode::OK,
    )
    .await;
    let cursor = first
        .next_cursor
        .as_deref()
        .expect("first page has a cursor");
    let second: TasksResponse = empty_request(
        &harness.app,
        Method::GET,
        &format!(
            "/api/v1/projects/{project_id}/tasks?canonical_phase=working&sort_by=id&sort_order=asc&limit=2&cursor={cursor}"
        ),
        axum::http::StatusCode::OK,
    )
    .await;
    let third_cursor = second
        .next_cursor
        .as_deref()
        .expect("second page has a cursor");
    let third: TasksResponse = empty_request(
        &harness.app,
        Method::GET,
        &format!(
            "/api/v1/projects/{project_id}/tasks?canonical_phase=working&sort_by=id&sort_order=asc&limit=2&cursor={third_cursor}"
        ),
        axum::http::StatusCode::OK,
    )
    .await;

    let paged_ids = first
        .items
        .iter()
        .chain(second.items.iter())
        .chain(third.items.iter())
        .map(|task| task.id.clone())
        .collect::<Vec<_>>();
    let full_ids = full
        .items
        .iter()
        .map(|task| task.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(paged_ids, full_ids);
    assert_eq!(full_ids.len(), working_ids.len());
}

#[tokio::test]
async fn unmapped_custom_state_uses_working_fallback() {
    let workspace_root = TestDir::new("forge-canonical-phase-fallback-workspaces");
    let repo_root = TestDir::new("forge-canonical-phase-fallback-repo");
    let repo_path = setup_git_repo(repo_root.path());
    let harness = test_app(workspace_root.path(), "canonical-phase-fallback").await;
    let (project_id, _) =
        create_project_and_repo(&harness.app, "Canonical phase fallback", &repo_path).await;

    let mut workflow: Value = serde_json::from_str(LEGACY_CUSTOM_WORKFLOW).unwrap();
    workflow["states"].as_array_mut().unwrap().push(json!({
        "name": "mystery",
        "kind": "custom",
        "column": "Somewhere",
        "display_name": "Mystery",
        "role": null,
        "hooks": {
            "before_exit": [], "on_exit": [], "before_enter": [],
            "on_enter": [], "after_enter": []
        },
        "cleanup": null,
        "gate_config": null,
        "dispatch": null,
        "triggers": {},
        "config": {}
    }));
    set_project_workflow(
        &harness,
        &project_id,
        &serde_json::to_string(&workflow).unwrap(),
    )
    .await;

    let task = create_task(&harness.app, &project_id, "mystery state").await;
    set_task_status(&harness, &task.id, "mystery").await;
    let response: TaskResponse = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", task.id),
        axum::http::StatusCode::OK,
    )
    .await;
    assert_eq!(response.canonical_phase, CanonicalPhase::Working);
}

async fn create_task(app: &Router, project_id: &str, title: &str) -> TaskResponse {
    json_request(
        app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": title }),
        axum::http::StatusCode::OK,
    )
    .await
}

async fn set_project_workflow(harness: &common::Harness, project_id: &str, workflow: &str) {
    sqlx::query("UPDATE project SET workflow_definition = ?, updated_at = ? WHERE id = ?")
        .bind(workflow)
        .bind(db::now_rfc3339())
        .bind(project_id)
        .execute(harness.state.db.pool())
        .await
        .expect("project workflow updates");
}

async fn set_task_status(harness: &common::Harness, task_id: &str, status: &str) {
    sqlx::query("UPDATE task SET status = ?, updated_at = ? WHERE id = ?")
        .bind(status)
        .bind(db::now_rfc3339())
        .bind(task_id)
        .execute(harness.state.db.pool())
        .await
        .expect("task status updates");
}

fn phase_name(phase: CanonicalPhase) -> &'static str {
    match phase {
        CanonicalPhase::Backlog => "backlog",
        CanonicalPhase::Ready => "ready",
        CanonicalPhase::Working => "working",
        CanonicalPhase::Review => "review",
        CanonicalPhase::Done => "done",
    }
}
