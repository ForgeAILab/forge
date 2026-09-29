#![allow(dead_code)]
mod common;

use api_types::{
    EffectiveCoderSource, ExecutionResponse, ExecutionSummaryResponse, PaginatedResponse,
    ProjectResponse, TaskDetailResponse, TaskRelationsResponse, TaskResponse,
};
use axum::http::{Method, StatusCode};
use db::{
    new_uuid_v4, now_rfc3339, AssigneeKind, CreateExecution, CreateTaskRoleAssignment,
    ExecutionRepo, ExecutionStatus, TaskDependencyRepo, TaskRoleAssignmentRepo,
};
use serde_json::json;

#[tokio::test]
async fn task_response_exposes_inherited_and_own_effective_coder_sources() {
    let workspace_root = common::TestDir::new("task-effective-coder");
    let harness = common::test_app(workspace_root.path(), "task-effective-coder").await;
    let project: ProjectResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Task effective coder" }),
        StatusCode::OK,
    )
    .await;
    let root = create_task(&harness.app, &project.id, "Root", None).await;
    let child = create_task(&harness.app, &project.id, "Child", Some(root.id.clone())).await;
    let now = now_rfc3339();
    TaskRoleAssignmentRepo::assign(
        &*harness.state.db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: root.id.clone(),
            role_name: "coder".to_owned(),
            assignee_type: Some(AssigneeKind::User),
            assignee_id: Some("root-worker".to_owned()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("root default worker assigns");

    let inherited: TaskResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", child.id),
        StatusCode::OK,
    )
    .await;
    assert!(inherited.role_assignments.is_empty());
    assert_eq!(
        inherited.effective_coder_source,
        Some(EffectiveCoderSource::InheritedFromRoot)
    );
    let inherited_coder = inherited
        .effective_coder
        .expect("effective coder is present");
    assert_eq!(inherited_coder.task_id, root.id);
    assert_eq!(inherited_coder.assignee_id.as_deref(), Some("root-worker"));

    let now = now_rfc3339();
    TaskRoleAssignmentRepo::assign(
        &*harness.state.db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: child.id.clone(),
            role_name: "coder".to_owned(),
            assignee_type: Some(AssigneeKind::User),
            assignee_id: Some("child-worker".to_owned()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("child override assigns");

    let overridden: TaskResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", child.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        overridden.effective_coder_source,
        Some(EffectiveCoderSource::Own)
    );
    let own_coder = overridden.effective_coder.expect("own coder is present");
    assert_eq!(own_coder.task_id, child.id);
    assert_eq!(own_coder.assignee_id.as_deref(), Some("child-worker"));
}

#[tokio::test]
async fn task_detail_bootstrap_matches_task_and_execution_page_semantics() {
    let workspace_root = common::TestDir::new("task-detail-bootstrap");
    let harness = common::test_app(workspace_root.path(), "task-detail-bootstrap").await;
    let project: ProjectResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Task detail bootstrap" }),
        StatusCode::OK,
    )
    .await;
    let root: TaskResponse = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{}/tasks", project.id),
        json!({ "title": "Task detail bootstrap root" }),
        StatusCode::OK,
    )
    .await;
    let task: TaskResponse = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{}/tasks", project.id),
        json!({
            "title": "Task detail bootstrap child",
            "parent_task_id": root.id
        }),
        StatusCode::OK,
    )
    .await;

    sqlx::query("UPDATE task SET blocked_json = ? WHERE id = ?")
        .bind(r#"{"reason":"waiting for a person"}"#)
        .bind(&task.id)
        .execute(harness.state.db.pool())
        .await
        .expect("task is marked as waiting for a person");

    let older =
        create_completed_execution(&harness.state.db, &task.id, "2026-09-21T00:00:00Z").await;
    let newer =
        create_completed_execution(&harness.state.db, &task.id, "2026-09-22T00:00:00Z").await;
    let oversized_summary = "界".repeat(600);
    sqlx::query(
        "UPDATE execution SET prompt = ?, summary = ?, executor_config_snapshot_json = ? WHERE id = ?",
    )
    .bind("large private prompt")
    .bind(&oversized_summary)
    .bind(
        r#"{"executor_type":"codex","config":{"resume_thread_in_place":true,"private":"large config"}}"#,
    )
    .bind(&newer.id)
    .execute(harness.state.db.pool())
    .await
    .expect("newer execution carries full-only fields");

    let standalone_task: TaskResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", task.id),
        StatusCode::OK,
    )
    .await;
    let detail: TaskDetailResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!(
            "/api/v1/tasks/{}/detail?limit=1&include_total=true",
            task.id
        ),
        StatusCode::OK,
    )
    .await;
    let executions: PaginatedResponse<ExecutionSummaryResponse> = common::empty_request(
        &harness.app,
        Method::GET,
        &format!(
            "/api/v1/tasks/{}/executions?limit=1&include_total=true",
            task.id
        ),
        StatusCode::OK,
    )
    .await;

    assert!(standalone_task.awaiting_human);
    assert_eq!(detail.task.awaiting_human, standalone_task.awaiting_human);
    assert_eq!(detail.task.id, standalone_task.id);
    let expected_subtask_workflow = services::workflow::inherited_subtask_workflow();
    let actual_state_names = detail
        .workflow
        .states
        .iter()
        .map(|state| state.name.as_str())
        .collect::<Vec<_>>();
    let expected_state_names = expected_subtask_workflow
        .states
        .iter()
        .map(|state| state.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(actual_state_names, expected_state_names);
    assert_eq!(detail.executions.items.len(), 1);
    assert_eq!(detail.executions.items[0].id, newer.id);
    assert!(detail.executions.has_more);
    assert_eq!(detail.executions.total_count, Some(2));
    assert!(detail.executions.next_cursor.is_some());
    assert_eq!(detail.executions.items[0].id, executions.items[0].id);
    assert_eq!(detail.executions.next_cursor, executions.next_cursor);
    assert_eq!(detail.executions.total_count, executions.total_count);
    assert_ne!(detail.executions.items[0].id, older.id);
    assert!(detail.executions.items[0].is_resume);
    assert_eq!(
        detail.executions.items[0]
            .summary
            .as_deref()
            .expect("summary preview")
            .chars()
            .count(),
        500
    );

    let raw_page: serde_json::Value = common::parse_response(
        common::raw_empty_request(
            &harness.app,
            Method::GET,
            &format!("/api/v1/tasks/{}/executions?limit=1", task.id),
        )
        .await,
        StatusCode::OK,
    )
    .await;
    let raw_item = &raw_page["items"][0];
    for full_only_field in [
        "prompt",
        "executor_config_snapshot",
        "usage",
        "logs_path",
        "error",
        "lease_owner",
        "last_progress_at",
    ] {
        assert!(
            raw_item.get(full_only_field).is_none(),
            "collection item must omit {full_only_field}"
        );
    }

    let full_execution: ExecutionResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/executions/{}", newer.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        full_execution.prompt.as_deref(),
        Some("large private prompt")
    );
    assert_eq!(
        full_execution
            .summary
            .as_deref()
            .expect("full summary")
            .chars()
            .count(),
        600
    );
    assert!(full_execution.executor_config_snapshot.is_some());
}

#[tokio::test]
async fn task_relations_returns_direct_rows_in_order_including_cancelled_targets() {
    let workspace_root = common::TestDir::new("task-relations");
    let harness = common::test_app(workspace_root.path(), "task-relations").await;
    let project: ProjectResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Task relations" }),
        StatusCode::OK,
    )
    .await;
    let task = create_task(&harness.app, &project.id, "Relations center", None).await;
    let first_subtask = create_task(
        &harness.app,
        &project.id,
        "First subtask",
        Some(task.id.clone()),
    )
    .await;
    let second_subtask = create_task(
        &harness.app,
        &project.id,
        "Second subtask",
        Some(task.id.clone()),
    )
    .await;
    let archived_subtask = create_task(
        &harness.app,
        &project.id,
        "Archived subtask",
        Some(task.id.clone()),
    )
    .await;
    let dependency = create_task(&harness.app, &project.id, "Cancelled dependency", None).await;
    let dependent = create_task(&harness.app, &project.id, "Cancelled dependent", None).await;
    let deleted_dependency =
        create_task(&harness.app, &project.id, "Deleted dependency", None).await;
    let deleted_dependent = create_task(&harness.app, &project.id, "Deleted dependent", None).await;

    sqlx::query("UPDATE task SET subtask_order = ? WHERE id = ?")
        .bind(9_i64)
        .bind(&first_subtask.id)
        .execute(harness.state.db.pool())
        .await
        .expect("first subtask order is set");
    sqlx::query("UPDATE task SET subtask_order = ? WHERE id = ?")
        .bind(2_i64)
        .bind(&second_subtask.id)
        .execute(harness.state.db.pool())
        .await
        .expect("second subtask order is set");
    sqlx::query("UPDATE task SET status = 'cancelled' WHERE id IN (?, ?)")
        .bind(&dependency.id)
        .bind(&dependent.id)
        .execute(harness.state.db.pool())
        .await
        .expect("related tasks are cancelled");
    sqlx::query("UPDATE task SET archived_at = ? WHERE id = ?")
        .bind("2026-09-22T00:00:00Z")
        .bind(&archived_subtask.id)
        .execute(harness.state.db.pool())
        .await
        .expect("third subtask is archived");
    sqlx::query("UPDATE task SET deleted_at = ? WHERE id IN (?, ?)")
        .bind("2026-09-22T00:00:00Z")
        .bind(&deleted_dependency.id)
        .bind(&deleted_dependent.id)
        .execute(harness.state.db.pool())
        .await
        .expect("related tasks are soft-deleted");

    for target in [&dependency, &deleted_dependency] {
        TaskDependencyRepo::add_dependency(
            &*harness.state.db,
            &task.id,
            &target.id,
            "2026-09-22T00:00:00Z",
        )
        .await
        .expect("direct dependency is created");
    }
    TaskDependencyRepo::add_dependency(
        &*harness.state.db,
        &dependent.id,
        &task.id,
        "2026-09-22T00:00:01Z",
    )
    .await
    .expect("direct dependent is created");
    TaskDependencyRepo::add_dependency(
        &*harness.state.db,
        &deleted_dependent.id,
        &task.id,
        "2026-09-22T00:00:02Z",
    )
    .await
    .expect("soft-deleted dependent edge is created");

    let relations: TaskRelationsResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/relations", task.id),
        StatusCode::OK,
    )
    .await;

    assert!(relations.parent.is_none());
    assert_eq!(
        relations
            .subtasks
            .iter()
            .map(|subtask| subtask.id.as_str())
            .collect::<Vec<_>>(),
        vec![second_subtask.id.as_str(), first_subtask.id.as_str()]
    );
    assert_eq!(relations.subtasks[0].subtask_order, Some(2));
    assert_eq!(relations.subtasks[1].subtask_order, Some(9));
    assert_eq!(relations.dependencies.len(), 1);
    assert_eq!(relations.dependencies[0].id, dependency.id);
    assert_eq!(relations.dependencies[0].status, "cancelled");
    assert_eq!(
        relations.missing_dependency_ids,
        vec![deleted_dependency.id]
    );
    assert_eq!(relations.dependents.len(), 1);
    assert_eq!(relations.dependents[0].id, dependent.id);
    assert_eq!(relations.dependents[0].status, "cancelled");

    let child_relations: TaskRelationsResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/relations", first_subtask.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(child_relations.parent.as_ref().unwrap().id, task.id);
    assert_eq!(child_relations.parent.as_ref().unwrap().title, task.title);
}

async fn create_completed_execution(
    db: &db::SqliteDb,
    task_id: &str,
    created_at: &str,
) -> db::Execution {
    ExecutionRepo::create(
        db,
        CreateExecution {
            id: db::new_uuid_v4(),
            task_id: task_id.to_owned(),
            agent_id: None,
            role: "coder".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: Some(created_at.to_owned()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("completed execution".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: created_at.to_owned(),
            updated_at: created_at.to_owned(),
        },
    )
    .await
    .expect("completed execution is created")
}

async fn create_task(
    app: &axum::Router,
    project_id: &str,
    title: &str,
    parent_task_id: Option<String>,
) -> TaskResponse {
    let uri = format!("/api/v1/projects/{project_id}/tasks");
    common::json_request(
        app,
        Method::POST,
        &uri,
        json!({ "title": title, "parent_task_id": parent_task_id }),
        StatusCode::OK,
    )
    .await
}
