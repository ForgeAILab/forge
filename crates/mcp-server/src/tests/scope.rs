//! Project-scoped MCP tool injection and cross-project rejection.

use super::*;

#[test]
fn scoped_mcp_injects_project_id_for_project_tools() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let result = call_tool_scoped(
            &state,
            &project_id,
            "forge_create_task",
            json!({
                "title": "Scoped task",
                "description": "created through scoped MCP"
            }),
        )
        .await;

        assert_eq!(result["project_id"], project_id);
        assert_eq!(result["title"], "Scoped task");
    });
}

#[test]
fn scoped_mcp_rejects_mismatched_project_id() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let error = dispatch_with_context(
            &state,
            &McpContext {
                project_id: Some(project_id),
                user_id: Some("mcp-test-user".to_owned()),
            },
            "tools/call",
            json!({
                "name": "forge_list_tasks",
                "arguments": {
                    "project_id": "other-project"
                }
            }),
        )
        .await
        .expect_err("mismatched project scope errors");

        assert_eq!(error.code, -32602);
    });
}

#[test]
fn scoped_mcp_allows_task_id_tool_for_same_project() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let task = seed_task_in_project(&state, project_id.clone()).await;

        let result = call_tool_scoped(
            &state,
            &project_id,
            "forge_get_task",
            json!({ "task_id": task.id }),
        )
        .await;

        assert_eq!(result["id"], task.id);
        assert_eq!(result["project_id"], project_id);
        assert!(
            result.get("repo_id").is_none(),
            "MCP Task values must not expose a repository binding: {result}"
        );
    });
}

#[test]
fn scoped_mcp_rejects_task_id_tool_for_other_project() {
    run_async(async {
        let state = sqlite_state().await;
        let (scoped_project_id, _) = seed_project_repo(&state).await;
        let other_task = seed_task(&state).await;

        let error = dispatch_with_context(
            &state,
            &McpContext {
                project_id: Some(scoped_project_id),
                user_id: Some("mcp-test-user".to_owned()),
            },
            "tools/call",
            json!({
                "name": "forge_get_task",
                "arguments": {
                    "task_id": other_task.id,
                }
            }),
        )
        .await
        .expect_err("cross-project task handle errors");

        // Outside the bound Project answers exactly like a missing id.
        assert_eq!(error.code, -32004);
    });
}

#[test]
fn scoped_mcp_rejects_parent_task_id_tool_for_other_project() {
    run_async(async {
        let state = sqlite_state().await;
        let (scoped_project_id, _) = seed_project_repo(&state).await;
        let other_task = seed_task(&state).await;

        let error = dispatch_with_context(
            &state,
            &McpContext {
                project_id: Some(scoped_project_id),
                user_id: Some("mcp-test-user".to_owned()),
            },
            "tools/call",
            json!({
                "name": "forge_create_sub_tasks",
                "arguments": {
                    "parent_task_id": other_task.id,
                    "subtasks": [{
                        "title": "Cross-project child",
                        "description": "should not be created"
                    }]
                }
            }),
        )
        .await
        .expect_err("cross-project parent task handle errors");

        // Outside the bound Project answers exactly like a missing id.
        assert_eq!(error.code, -32004);
    });
}

#[test]
fn scoped_mcp_rejects_execution_id_tool_for_other_project() {
    run_async(async {
        let state = sqlite_state().await;
        let (scoped_project_id, _) = seed_project_repo(&state).await;
        let other_task = seed_task(&state).await;
        let execution_id = seed_execution(&state, other_task.id).await;

        let error = dispatch_with_context(
            &state,
            &McpContext {
                project_id: Some(scoped_project_id),
                user_id: Some("mcp-test-user".to_owned()),
            },
            "tools/call",
            json!({
                "name": "forge_follow_up_execution",
                "arguments": {
                    "execution_id": execution_id,
                    "message": "continue"
                }
            }),
        )
        .await
        .expect_err("cross-project execution handle errors");

        // Outside the bound Project answers exactly like a missing id.
        assert_eq!(error.code, -32004);
    });
}
