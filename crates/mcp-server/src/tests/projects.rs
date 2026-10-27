//! MCP project creation, listing, updates and lifecycle-hook validation.

use super::*;

#[test]
fn forge_list_projects_returns_paginated_projects() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let result = call_tool(
            &state,
            "forge_list_projects",
            json!({
                "limit": 10,
            }),
        )
        .await;
        let projects = result["data"].as_array().expect("projects array");

        assert_eq!(projects.len(), 1);
        assert_eq!(projects[0]["id"], project_id);
        assert_eq!(projects[0]["name"], "Forge");
    });
}

#[test]
fn forge_create_project_creates_project() {
    run_async(async {
        let state = sqlite_state().await;
        let result = call_tool(
            &state,
            "forge_create_project",
            json!({
                "name": "MCP",
            }),
        )
        .await;

        assert_eq!(result["name"], "MCP");
        assert!(result["id"].as_str().is_some());
        assert_eq!(result["settings"], json!({}));
        assert_eq!(result["paused"], false);

        let project_id = result["id"].as_str().expect("project id");
        let member = ProjectMemberRepo::get_member(&*state.db, project_id, "mcp-test-user")
            .await
            .expect("owner membership lookup succeeds")
            .expect("MCP creator is a Project member");
        assert_eq!(member.role, "owner");

        let updated = call_tool(
            &state,
            "forge_update_project",
            json!({
                "project_id": project_id,
                "version": result["version"],
                "name": "MCP managed"
            }),
        )
        .await;
        assert_eq!(updated["name"], "MCP managed");
    });
}

#[test]
fn forge_get_project_returns_project_details() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let result = call_tool(
            &state,
            "forge_get_project",
            json!({
                "project_id": project_id,
            }),
        )
        .await;

        assert_eq!(result["name"], "Forge");
        assert_eq!(result["settings"], json!({}));
        assert_eq!(result["paused"], false);
        assert!(result.get("workflow_template_name").is_some());
    });
}

#[test]
fn forge_update_project_updates_mutable_fields() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let result = call_tool(
            &state,
            "forge_update_project",
            json!({
                "project_id": project_id,
                "version": ProjectRepo::get_by_id(&*state.db, &project_id).await.unwrap().unwrap().version,
                "name": "Updated Forge",
                "settings": {
                    "retry_budgets": {
                        "review": 5,
                        "merge_fix": 2
                    }
                },
                "paused": true
            }),
        )
        .await;

        assert_eq!(result["name"], "Updated Forge");
        assert_eq!(result["settings"]["retry_budgets"]["review"], 5);
        assert_eq!(result["paused"], true);
        assert!(result["paused_at"].as_str().is_some());
    });
}

#[test]
fn forge_update_project_lifecycle_hooks_replaces_hooks_only() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        sqlx::query("UPDATE project SET owner_id='mcp-test-user' WHERE id=?")
            .bind(&project_id)
            .execute(state.db.pool())
            .await
            .unwrap();
        call_tool(
            &state,
            "forge_update_project",
            json!({
                "project_id": project_id,
                "version": ProjectRepo::get_by_id(&*state.db, &project_id).await.unwrap().unwrap().version,
                "settings": {
                    "retry_budgets": {
                        "review": 4,
                        "merge_fix": 1
                    }
                }
            }),
        )
        .await;

        let result = call_tool(
            &state,
            "forge_update_project_lifecycle_hooks",
            json!({
                "project_id": project_id,
                "version": ProjectRepo::get_by_id(&*state.db, &project_id).await.unwrap().unwrap().version,
                "lifecycle_hooks": {
                    "before_work": [
                        {
                            "type": "script",
                            "command": "pnpm test",
                            "timeout_seconds": 60,
                            "blocking": true
                        }
                    ],
                    "on_task_done": [
                        {
                            "type": "plugin",
                            "name": "notify",
                            "enabled": true,
                            "config": { "channel": "dev" }
                        }
                    ]
                }
            }),
        )
        .await;

        assert_eq!(result["settings"]["retry_budgets"]["review"], 4);
        assert_eq!(
            result["settings"]["lifecycle_hooks"]["before_work"][0]["command"],
            "pnpm test"
        );
        assert_eq!(
            result["settings"]["lifecycle_hooks"]["on_task_done"][0]["name"],
            "notify"
        );
    });
}

#[test]
fn forge_update_project_lifecycle_hooks_validates_blocking_event() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        sqlx::query("UPDATE project SET owner_id='mcp-test-user' WHERE id=?")
            .bind(&project_id)
            .execute(state.db.pool())
            .await
            .unwrap();
        let error = dispatch(
            &state,
            "tools/call",
            json!({
                "name": "forge_update_project_lifecycle_hooks",
                "arguments": {
                    "project_id": project_id,
                    "version": ProjectRepo::get_by_id(&*state.db, &project_id).await.unwrap().unwrap().version,
                    "lifecycle_hooks": {
                        "on_work_start": [
                            {
                                "type": "script",
                                "command": "echo no",
                                "blocking": true
                            }
                        ]
                    }
                }
            }),
        )
        .await
        .expect_err("invalid lifecycle hooks error");

        assert_eq!(error.code, -32602);
    });
}
