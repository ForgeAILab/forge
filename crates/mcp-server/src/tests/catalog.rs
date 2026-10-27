//! MCP tool advertisement and project-constrained catalog authority.

use super::*;

#[test]
fn tools_list_returns_descriptors() {
    run_async(async {
        let state = sqlite_state().await;
        let result = dispatch(&state, "tools/list", json!({}))
            .await
            .expect("tools/list succeeds");
        let tools = result["tools"]
            .as_array()
            .expect("tools/list returns {tools:[...]}");
        let names = tools
            .iter()
            .map(|tool| tool["name"].as_str().expect("tool name").to_owned())
            .collect::<std::collections::BTreeSet<_>>();
        let expected = [
            "forge_add_task_dependency",
            "forge_assign_agent",
            "forge_task_action",
            "forge_create_agent_handoff",
            "forge_create_project",
            "forge_create_sub_tasks",
            "forge_create_task",
            "forge_follow_up_execution",
            "forge_get_project",
            "forge_get_agent_session",
            "forge_get_agent_chat",
            "forge_get_agent_handoff",
            "forge_get_integration_queue",
            "forge_get_main_agent",
            "forge_get_project_agent",
            "forge_get_task",
            "forge_get_task_diff",
            "forge_list_agent_profiles",
            "forge_list_agent_chat_messages",
            "forge_list_agent_chats",
            "forge_list_agent_handoffs",
            "forge_list_agent_sessions",
            "forge_list_agents",
            "forge_list_executions",
            "forge_list_projects",
            "forge_list_sub_tasks",
            "forge_list_task_dependencies",
            "forge_list_task_dependents",
            "forge_list_tasks",
            "forge_preview_prompt",
            "forge_register_agent",
            "forge_remove_task_dependency",
            "forge_reorder_sub_tasks",
            "forge_send_agent_chat_message",
            "forge_set_main_agent",
            "forge_set_project_agent",
            "forge_transition_task",
            "forge_update_project",
            "forge_update_project_lifecycle_hooks",
            "forge_update_task",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(names, expected);
        assert!(tools
            .iter()
            .any(|tool| tool.get("name").is_some() && tool.get("inputSchema").is_some()));

        let create_task = tools
            .iter()
            .find(|tool| tool["name"] == "forge_create_task")
            .expect("create task descriptor");
        assert_eq!(
            create_task["inputSchema"]["properties"]["depends_on_ids"]["type"],
            "array"
        );
        assert!(create_task["description"]
            .as_str()
            .unwrap()
            .contains("parent shares a workspace"));
        assert!(create_task["description"]
            .as_str()
            .unwrap()
            .contains("depends_on_ids?"));
        let create_subtasks = tools
            .iter()
            .find(|tool| tool["name"] == "forge_create_sub_tasks")
            .unwrap();
        assert!(create_subtasks["description"]
            .as_str()
            .unwrap()
            .contains("run serially"));
        assert!(create_subtasks["description"]
            .as_str()
            .unwrap()
            .contains("subtasks"));
    });
}

#[test]
fn migrated_orchestration_contracts_are_not_shadowed_by_legacy_mcp_descriptors() {
    run_async(async {
        let state = sqlite_state().await;
        let result = dispatch(&state, "tools/list", json!({}))
            .await
            .expect("tools/list succeeds");
        let mcp_names = result["tools"]
            .as_array()
            .expect("tools list")
            .iter()
            .filter_map(|tool| tool["name"].as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let overlap = forge_agent_host::MIGRATED_OPERATION_CONTRACTS
            .iter()
            .map(|contract| contract.operation)
            .filter(|operation| mcp_names.contains(operation))
            .collect::<Vec<_>>();

        assert!(
            overlap.is_empty(),
            "a migrated operation may enter MCP only through a descriptor derived from its canonical contract: {overlap:?}"
        );
        assert!(mcp_names.contains("forge_create_project"));
        assert!(!mcp_names.contains(forge_agent_host::MAIN_PROJECT_CREATE_OPERATION));
        assert!(mcp_names.contains("forge_create_task"));
        assert!(!mcp_names.contains(forge_agent_host::TASK_PROPOSE_OPERATION));
    });
}

#[test]
fn revised_public_surface_does_not_advertise_retired_collaboration_tools() {
    run_async(async {
        let state = sqlite_state().await;
        let result = dispatch(&state, "tools/list", json!({}))
            .await
            .expect("tools/list succeeds");
        let names = result["tools"]
            .as_array()
            .expect("tools list")
            .iter()
            .filter_map(|tool| tool["name"].as_str())
            .collect::<Vec<_>>();
        assert!(names.iter().all(|name| !name.contains("room")));
        assert!(names.iter().all(|name| !name.contains("membership")));
        assert!(names.contains(&"forge_list_agent_chats"));
        assert!(names.contains(&"forge_send_agent_chat_message"));
        assert!(names.contains(&"forge_create_agent_handoff"));
    });
}

#[test]
fn scoped_tools_list_marks_project_id_optional() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let result = dispatch_with_context(
            &state,
            &McpContext {
                project_id: Some(project_id),
                user_id: Some("mcp-test-user".to_owned()),
            },
            "tools/list",
            json!({}),
        )
        .await
        .expect("tools/list succeeds");
        let tools = result["tools"]
            .as_array()
            .expect("tools/list returns {tools:[...]}");
        let create_task = tools
            .iter()
            .find(|tool| tool["name"] == "forge_create_task")
            .expect("create task descriptor exists");
        let required = create_task["inputSchema"]["required"]
            .as_array()
            .expect("required is an array");

        assert!(required.iter().any(|value| value == "title"));
        assert!(!required.iter().any(|value| value == "project_id"));

        let memory_search = tools
            .iter()
            .find(|tool| tool["name"] == "forge_memory_search")
            .expect("memory search descriptor exists");
        let memory_required = memory_search["inputSchema"]["required"]
            .as_array()
            .expect("required is an array");
        assert!(!memory_required.iter().any(|value| value == "project_id"));
        assert!(memory_required.iter().any(|value| value == "query"));
    });
}

#[test]
fn project_constrained_catalog_matches_call_authority_and_projects_only_the_bound_project() {
    run_async(async {
        use operation_registry::authority::McpScopeRule;
        use operation_registry::mcp::CATALOG;
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let (other_id, _) = seed_project_repo(&state).await;
        assert_ne!(project_id, other_id);
        let context = McpContext {
            user_id: Some("mcp-test-user".into()),
            project_id: Some(project_id.clone()),
        };
        let account = dispatch(&state, "tools/list", json!({})).await.unwrap();
        let names = account["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            names,
            CATALOG
                .iter()
                .filter(|spec| registry_contract::admitted_after_consolidation(
                    "account", spec.name
                ))
                .map(|spec| spec.name)
                .collect(),
            "every MCP tool needs an explicit scope classification"
        );
        let scoped = dispatch_with_context(&state, &context, "tools/list", json!({}))
            .await
            .unwrap();
        let scoped_names = scoped["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect::<std::collections::BTreeSet<_>>();
        for spec in CATALOG.iter() {
            let name = spec.name;
            let rule = spec.scope;
            assert_eq!(
                scoped_names.contains(name),
                rule == McpScopeRule::BoundProject
                    && registry_contract::admitted_after_consolidation("project_public", name),
                "{name}"
            );
            if rule == McpScopeRule::AccountInspection {
                // Authority denial precedes malformed tool arguments.
                let error = dispatch_with_context(
                    &state,
                    &context,
                    "tools/call",
                    json!({"name":name,"arguments":7}),
                )
                .await
                .unwrap_err();
                assert_eq!(error.code, -32001, "{name}");
            }
        }
        let result = dispatch_with_context(
            &state,
            &context,
            "tools/call",
            json!({"name":"forge_list_projects","arguments":{}}),
        )
        .await
        .unwrap();
        let body: Value =
            serde_json::from_str(result["content"][0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(body["data"].as_array().unwrap().len(), 1);
        assert_eq!(body["data"][0]["id"], project_id);
        assert_eq!(body["total_count"], 1);
        let error = dispatch_with_context(
            &state,
            &context,
            "tools/call",
            json!({"name":"forge_list_projects","arguments":{"cursor":"not-a-cursor"}}),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, -32602);
    });
}
