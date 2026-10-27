//! MCP task creation, dependencies, hierarchy, updates and transitions.

use super::*;

#[test]
fn forge_create_task_persists_validated_task_type() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        for (task_type, title) in [
            ("discovery", "Discovery task"),
            ("planning_task", "Planning task"),
        ] {
            let result = call_tool(
                &state,
                "forge_create_task",
                json!({
                    "project_id": project_id,
                    "title": title,
                    "type": task_type
                }),
            )
            .await;
            let task_id = result["id"].as_str().expect("task id");
            let task = TaskRepo::get_by_id(&*state.db, task_id, false)
                .await
                .expect("task lookup succeeds")
                .expect("task persists");
            assert_eq!(task.task_type, task_type);
        }
    });
}

#[test]
fn forge_create_task_atomically_persists_prerequisites() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let first = seed_task_in_project(&state, project_id.clone()).await;
        let second = seed_task_in_project(&state, project_id.clone()).await;

        let result = call_tool(
            &state,
            "forge_create_task",
            json!({
                "project_id": project_id,
                "title": "Dependent task",
                "depends_on_ids": [first.id, second.id]
            }),
        )
        .await;

        let task_id = result["id"].as_str().expect("task id");
        let dependency_ids = result["depends_on_ids"]
            .as_array()
            .expect("creation returns dependency ids");
        assert_eq!(dependency_ids.len(), 2);
        let stored = TaskDependencyRepo::list_dependencies(&*state.db, task_id)
            .await
            .expect("dependencies load");
        assert_eq!(stored.len(), 2);

        let listed = call_tool(
            &state,
            "forge_list_task_dependencies",
            json!({ "task_id": task_id }),
        )
        .await;
        assert_eq!(listed["depends_on_ids"].as_array().map(Vec::len), Some(2));

        let reverse = call_tool(
            &state,
            "forge_list_task_dependents",
            json!({ "task_id": stored[0] }),
        )
        .await;
        assert_eq!(reverse["dependent_task_ids"], json!([task_id]));
    });
}

#[test]
fn forge_create_task_rejects_invalid_prerequisites_without_creating_task() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let before = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task WHERE project_id = ?")
            .bind(&project_id)
            .fetch_one(state.db.pool())
            .await
            .expect("task count loads");

        let error = call_tool_error(
            &state,
            "forge_create_task",
            json!({
                "project_id": project_id.clone(),
                "title": "Must not persist",
                "depends_on_ids": ["missing-task"]
            }),
        )
        .await;

        assert_eq!(error.code, -32004);
        let after = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task WHERE project_id = ?")
            .bind(&project_id)
            .fetch_one(state.db.pool())
            .await
            .expect("task count reloads");
        assert_eq!(after, before);
    });
}

#[test]
fn forge_create_task_keeps_parentage_distinct_from_dependencies() {
    run_async(async {
        let state = sqlite_state().await;
        let root = seed_task(&state).await;
        let before = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task")
            .fetch_one(state.db.pool())
            .await
            .expect("task count loads");

        let error = call_tool_error(
            &state,
            "forge_create_task",
            json!({
                "project_id": root.project_id,
                "title": "Impossible child",
                "parent_task_id": root.id,
                "depends_on_ids": [root.id]
            }),
        )
        .await;

        assert_eq!(error.code, -32602);
        assert_eq!(
            error.data.as_ref().and_then(|data| data["field"].as_str()),
            Some("depends_on_ids")
        );
        let after = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task")
            .fetch_one(state.db.pool())
            .await
            .expect("task count reloads");
        assert_eq!(after, before);
    });
}

#[test]
fn forge_create_task_rejects_invalid_priority_with_field_error() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let error = call_tool_error(
            &state,
            "forge_create_task",
            json!({
                "project_id": project_id,
                "title": "Bad priority",
                "priority": "high"
            }),
        )
        .await;

        assert_eq!(error.code, -32602);
        let data = error.data.expect("error data");
        assert_eq!(data["field"], "priority");
        assert_eq!(data["accepted"]["type"], "integer");
    });
}

#[test]
fn forge_create_task_rejects_invalid_type_with_field_error() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let error = call_tool_error(
            &state,
            "forge_create_task",
            json!({
                "project_id": project_id,
                "title": "Bad type",
                "type": "epic"
            }),
        )
        .await;

        assert_eq!(error.code, -32602);
        let data = error.data.expect("error data");
        assert_eq!(data["field"], "type");
        assert_eq!(
            data["accepted"]["enum"],
            json!(["task", "planning_task", "sub_task", "discovery"])
        );
    });
}

#[test]
fn forge_create_task_rejects_task_repository_selector() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, repo_id) = seed_project_repo(&state).await;
        let before = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task")
            .fetch_one(state.db.pool())
            .await
            .expect("Task count loads");

        let error = call_tool_error(
            &state,
            "forge_create_task",
            json!({
                "project_id": project_id,
                "title": "Client-selected repository",
                "repo_id": repo_id
            }),
        )
        .await;

        assert_eq!(error.code, -32602);
        let after = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task")
            .fetch_one(state.db.pool())
            .await
            .expect("Task count reloads");
        assert_eq!(after, before, "rejected selector must not create a Task");
    });
}

#[test]
fn forge_create_task_rejects_missing_project_id_with_field_error() {
    run_async(async {
        let state = sqlite_state().await;
        let error = call_tool_error(
            &state,
            "forge_create_task",
            json!({
                "title": "Missing project"
            }),
        )
        .await;

        assert_eq!(error.code, -32602);
        let data = error.data.expect("error data");
        assert_eq!(data["field"], "project_id");
        assert_eq!(data["accepted"]["type"], "string");
    });
}

#[test]
fn forge_create_task_hides_unknown_project_before_dispatch() {
    run_async(async {
        let state = sqlite_state().await;
        let error = call_tool_error(
            &state,
            "forge_create_task",
            json!({
                "project_id": "does-not-exist",
                "title": "Unknown project"
            }),
        )
        .await;

        assert_eq!(error.code, -32004);
    });
}

#[test]
fn forge_create_sub_tasks_rejects_malformed_subtasks() {
    run_async(async {
        let state = sqlite_state().await;
        let root = seed_task(&state).await;
        let error = call_tool_error(
            &state,
            "forge_create_sub_tasks",
            json!({
                "parent_task_id": root.id,
                "subtasks": [{"description": "missing title"}]
            }),
        )
        .await;

        assert_eq!(error.code, -32602);
        let data = error.data.expect("error data");
        assert!(data["details"]
            .as_str()
            .expect("details string")
            .contains("title"));
    });
}

#[test]
fn forge_update_task_updates_mutable_fields() {
    run_async(async {
        let state = sqlite_state().await;
        let task = seed_task(&state).await;
        let result = call_tool(
            &state,
            "forge_update_task",
            json!({
                "task_id": task.id,
                "title": "Updated MCP",
                "description": "changed",
                "priority": 9,
                "plan": "test it",
                "version": task.version,
            }),
        )
        .await;

        assert_eq!(result["title"], "Updated MCP");
        assert_eq!(result["description"], "changed");
        assert_eq!(result["priority"], 9);
        assert_eq!(result["plan"], "test it");
        assert_eq!(result["version"], 2);
    });
}

#[test]
fn forge_transition_task_changes_status() {
    run_async(async {
        let state = sqlite_state().await;
        let task = seed_task(&state).await;
        let result = call_tool(
            &state,
            "forge_transition_task",
            json!({
                "task_id": task.id,
                "status": "in_progress",
                "version": task.version,
            }),
        )
        .await;

        assert_eq!(result["status"], "in_progress");
        assert_eq!(
            result["pending_steps"], 1,
            "the committed entry has a durable hook phase"
        );
        assert!(
            result["version"].as_i64().expect("version is an integer") > task.version,
            "transition should advance task version"
        );
    });
}

#[test]
fn forge_create_sub_tasks_creates_subtasks() {
    run_async(async {
        let state = sqlite_state().await;
        let root = seed_task(&state).await;
        let agent = seed_agent(&state, "child-coder").await;
        let result = call_tool(
            &state,
            "forge_create_sub_tasks",
            json!({
                "parent_task_id": root.id.clone(),
                "subtasks": [
                    { "title": "One", "assignee_id": agent.id },
                    { "title": "Two" },
                    { "title": "Three" }
                ]
            }),
        )
        .await;

        let subtasks = result["subtasks"].as_array().expect("subtasks array");
        assert_eq!(subtasks.len(), 3);
        for (index, subtask) in subtasks.iter().enumerate() {
            assert_eq!(subtask["subtask_order"], index as i64);
        }
        assert_eq!(subtasks[0]["role_assignments"][0]["assignee_id"], agent.id);
        assert_eq!(subtasks[0]["role_assignments"][0]["role_name"], "coder");

        let listed = call_tool(
            &state,
            "forge_list_sub_tasks",
            json!({ "parent_task_id": root.id.clone() }),
        )
        .await;
        assert_eq!(
            listed["subtasks"]
                .as_array()
                .expect("listed subtasks")
                .iter()
                .map(|task| task["title"].as_str().expect("title"))
                .collect::<Vec<_>>(),
            vec!["One", "Two", "Three"]
        );

        let ordered_ids = [
            subtasks[0]["id"].as_str().expect("first subtask id"),
            subtasks[2]["id"].as_str().expect("third subtask id"),
            subtasks[1]["id"].as_str().expect("second subtask id"),
        ];
        let reordered = call_tool(
            &state,
            "forge_reorder_sub_tasks",
            json!({
                "parent_task_id": root.id,
                "ordered_ids": ordered_ids
            }),
        )
        .await;
        assert_eq!(
            reordered["subtasks"]
                .as_array()
                .expect("reordered subtasks")
                .iter()
                .map(|task| task["title"].as_str().expect("title"))
                .collect::<Vec<_>>(),
            vec!["One", "Three", "Two"]
        );
    });
}

#[test]
fn forge_create_sub_tasks_nested_rejected() {
    run_async(async {
        let state = sqlite_state().await;
        let root = seed_task(&state).await;
        let now = now_rfc3339();
        let subtask = TaskRepo::create(
            &*state.db,
            CreateTask {
                id: new_uuid_v4(),
                project_id: root.project_id.clone(),
                parent_task_id: Some(root.id.clone()),
                assignee_type: None,
                assignee_id: None,
                title: "Existing subtask".to_owned(),
                description: None,
                task_type: "task".to_owned(),
                status: "todo".to_owned(),
                is_automation: false,
                priority: 0,
                subtask_order: Some(0),
                task_state_config: None,
                merge_config: None,
                plan: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("subtask creates");

        let error = dispatch(
            &state,
            "tools/call",
            json!({
                "name": "forge_create_sub_tasks",
                "arguments": {
                    "parent_task_id": subtask.id,
                    "subtasks": [
                        { "title": "Nested" }
                    ]
                }
            }),
        )
        .await
        .expect_err("nested subtask errors");
        let response = error.into_response(json!(1));
        let error = response.error.expect("error response");
        assert_eq!(error.code, -32602);
        assert_eq!(
            error
                .data
                .as_ref()
                .and_then(|data| data.get("code"))
                .and_then(Value::as_str),
            Some("NESTED_SUBTASK_UNSUPPORTED")
        );
    });
}
