mod common;

use api_types::{ErrorResponse, PaginatedResponse, ProjectResponse, ProjectSlots};
use axum::http::{Method, StatusCode};
use db::{now_rfc3339, CreateTask, TaskRepo};
use serde_json::{json, Value};

async fn slot_task(harness: &common::Harness, project_id: &str, status: &str) -> db::Task {
    let now = now_rfc3339();
    TaskRepo::create(
        &*harness.state.db,
        CreateTask {
            id: db::new_uuid_v4(),
            project_id: project_id.to_owned(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: status.to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: status.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn project_get_and_list_report_slots_and_task_capacity_reason() {
    let root = common::TestDir::new("project-slots-api");
    let harness = common::test_app(root.path(), "project-slots-api").await;
    let project: ProjectResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({"name": "Slots"}),
        StatusCode::OK,
    )
    .await;
    for status in [
        "in_progress",
        "in_progress",
        "review",
        "review",
        "merge_failed",
    ] {
        slot_task(&harness, &project.id, status).await;
    }
    let parked = slot_task(&harness, &project.id, "review").await;
    sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
        .bind(r#"{"type":"review_needs_owner","message":"needs macOS"}"#)
        .bind(&parked.id)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let queued = slot_task(&harness, &project.id, "todo").await;
    sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
        .bind(
            json!({"dispatch_disposition": {
                "task_version": queued.version,
                "capability": "project_capacity",
                "blocker_digest": "capacity",
                "recorded_at": now_rfc3339(),
                "safe_message": "project_at_capacity: waiting for a slot (5/5 active)"
            }})
            .to_string(),
        )
        .bind(&queued.id)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let expected = ProjectSlots {
        limit: 5,
        active: 5,
        parked: 1,
        queued: 1,
    };
    let response: ProjectResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{}", project.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(response.slots, expected);
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
            .slots,
        expected
    );

    let task: Value = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", queued.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(task["workflow_health"]["kind"], "waiting_for_agent");
    assert_eq!(
        task["workflow_health"]["stale_reason"],
        "project_at_capacity"
    );
    assert_eq!(
        task["workflow_health"]["message"],
        "waiting for a slot (5/5 active)"
    );
    let task_list: PaginatedResponse<Value> = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{}/tasks", project.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        task_list
            .items
            .iter()
            .find(|task| task["id"] == queued.id)
            .unwrap()["workflow_health"]["message"],
        "waiting for a slot (5/5 active)"
    );

    sqlx::query("UPDATE task SET metadata_json = json_set(metadata_json, '$.dispatch_disposition.safe_message', ?) WHERE id = ?")
        .bind("project_waiting_on_owner: 10 parked tasks waiting on the owner")
        .bind(&queued.id).execute(harness.state.db.pool()).await.unwrap();
    let owner_wait: Value = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", queued.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(owner_wait["workflow_health"]["label"], "Waiting on Owner");
    assert_eq!(
        owner_wait["workflow_health"]["stale_reason"],
        "project_waiting_on_owner"
    );
    assert_eq!(
        owner_wait["workflow_health"]["message"],
        "10 parked tasks waiting on the owner"
    );
    let detail: Value = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/detail", queued.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        detail["task"]["workflow_health"]["stale_reason"],
        "project_waiting_on_owner"
    );
}

#[tokio::test]
async fn project_patch_validates_active_task_limit() {
    let root = common::TestDir::new("project-slot-limit-api");
    let harness = common::test_app(root.path(), "project-slot-limit-api").await;
    let mut project: ProjectResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({"name": "Limit"}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(project.slots.limit, 5);
    for limit in [
        json!(1001),
        json!(-1),
        json!(1.5),
        json!("5"),
        json!(4294967296_u64),
    ] {
        let error: ErrorResponse = common::json_request(
            &harness.app,
            Method::PATCH,
            &format!("/api/v1/projects/{}", project.id),
            json!({"version": project.version, "settings": {"max_active_tasks": limit}}),
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert!(!error.message.is_empty());
    }
    let unchanged: ProjectResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{}", project.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(unchanged.version, project.version);
    assert_eq!(unchanged.slots.limit, 5);
    for limit in [0, 1000, 5] {
        let updated: ProjectResponse = common::json_request(
            &harness.app,
            Method::PATCH,
            &format!("/api/v1/projects/{}", project.id),
            json!({"version": project.version, "settings": {"max_active_tasks": limit}}),
            StatusCode::OK,
        )
        .await;
        assert_eq!(updated.slots.limit, limit);
        assert_eq!(updated.settings["max_active_tasks"], limit);
        assert!(updated.version > project.version);
        project = updated;
    }
}
