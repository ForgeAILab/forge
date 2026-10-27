//! MCP agent registration, assignment, listing and daemon-pin authority.

use super::*;

#[test]
fn forge_register_agent_registers_agent() {
    run_async(async {
        let state = sqlite_state().await;
        let (executor_type, _daemon_id) = seed_agent_registration_deps(&state).await;
        let result = call_tool(
            &state,
            "forge_register_agent",
            json!({
                "name": "codex",
                "executor_type": executor_type.clone(),
            }),
        )
        .await;

        assert_eq!(result["name"], "codex");
        assert_eq!(result["executor_type"], executor_type);
        assert_eq!(result["daemon_id"], Value::Null);
        assert_eq!(result["status"], "idle");

        let stored = AgentRepo::get_by_id(
            &*state.db,
            result["id"].as_str().expect("registered agent id"),
        )
        .await
        .expect("registered agent lookup succeeds")
        .expect("registered agent exists");
        assert_eq!(stored.owner_id.as_deref(), Some("mcp-test-user"));
        assert_eq!(stored.visibility, "account");
    });
}

#[test]
fn forge_register_agent_rejects_daemon_pin_for_non_admin() {
    run_async(async {
        let state = sqlite_state().await;
        let (executor_type, daemon_id) = seed_agent_registration_deps(&state).await;
        let error = call_tool_error(
            &state,
            "forge_register_agent",
            json!({
                "name": "unprivileged",
                "executor_type": executor_type,
                "daemon_id": daemon_id,
            }),
        )
        .await;

        assert_eq!(error.code, -32003);
        assert_eq!(error.data, Some(json!({ "code": "admin_required" })));
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_identity WHERE owner_id = 'mcp-test-user'",
        )
        .fetch_one(state.db.pool())
        .await
        .expect("agent count loads");
        assert_eq!(count, 0);
    });
}

#[test]
fn forge_register_agent_allows_admin_daemon_pin() {
    run_async(async {
        let state = sqlite_state().await;
        let (executor_type, daemon_id) = seed_agent_registration_deps(&state).await;
        UserRepo::set_admin(&*state.db, "mcp-test-user", true)
            .await
            .expect("test user becomes admin");
        let result = call_tool(
            &state,
            "forge_register_agent",
            json!({
                "name": "privileged",
                "executor_type": executor_type,
                "daemon_id": daemon_id.clone(),
            }),
        )
        .await;

        assert_eq!(result["daemon_id"], daemon_id);
    });
}

#[test]
fn forge_assign_agent_only_updates_the_task_assignment() {
    run_async(async {
        let state = sqlite_state().await;
        let task = seed_task(&state).await;
        let agent = seed_agent(&state, "luna").await;

        let result = call_tool(
            &state,
            "forge_assign_agent",
            json!({
                "task_id": task.id,
                "agent_id": agent.id,
            }),
        )
        .await;

        assert_eq!(result["execution_started"], false);
        assert_eq!(result["assignment"]["task_id"], task.id);
        assert_eq!(result["assignment"]["assignee_id"], agent.id);
        assert_eq!(result["assignment"]["assignee_type"], "agent");

        let executions = ExecutionRepo::list_by_task(
            &*state.db,
            &task.id,
            PageRequest {
                cursor: None,
                limit: 20,
                include_total: false,
                sort_by: SortBy::CreatedAt,
                sort_order: SortOrder::Desc,
            },
        )
        .await
        .expect("execution lookup succeeds");
        assert!(executions.items.is_empty());
    });
}

#[test]
fn forge_list_agents_returns_paginated_agents() {
    run_async(async {
        let state = sqlite_state().await;
        let (executor_type, _daemon_id) = seed_agent_registration_deps(&state).await;
        call_tool(
            &state,
            "forge_register_agent",
            json!({
                "name": "codex",
                "executor_type": executor_type.clone(),
            }),
        )
        .await;

        let result = call_tool(
            &state,
            "forge_list_agents",
            json!({
                "status": "idle",
                "limit": 10,
            }),
        )
        .await;
        let agents = result["data"].as_array().expect("agents array");
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0]["name"], "codex");
        assert_eq!(result["has_more"], false);
    });
}

#[test]
fn forge_list_agents_filters_before_pagination_and_redacts_daemon_id() {
    run_async(async {
        let state = sqlite_state().await;
        let (_executor_type, daemon_id) = seed_agent_registration_deps(&state).await;
        for (id, name, owner_id, visibility, created_at) in [
            (
                "mcp-hidden-agent",
                "hidden",
                Some("another-user"),
                "account",
                "2026-09-10T00:00:03Z",
            ),
            (
                "mcp-owned-agent",
                "owned",
                Some("mcp-test-user"),
                "account",
                "2026-09-10T00:00:02Z",
            ),
            (
                "mcp-global-agent",
                "global",
                None,
                "global",
                "2026-09-10T00:00:01Z",
            ),
        ] {
            AgentRepo::create(
                &*state.db,
                CreateAgent {
                    id: id.to_owned(),
                    name: name.to_owned(),
                    description: None,
                    executor_type: "shell".to_owned(),
                    model: None,
                    reasoning_effort: None,
                    permission_policy: None,
                    capabilities_json: "[]".to_owned(),
                    config_json: "{}".to_owned(),
                    credential_ref: None,
                    daemon_id: Some(daemon_id.clone()),
                    max_concurrent_tasks: 1,
                    heartbeat_interval_seconds: 30,
                    max_missed_heartbeats: 3,
                    status: AgentStatus::Idle,
                    last_heartbeat_at: None,
                    is_default: false,
                    paused: false,
                    owner_id: owner_id.map(str::to_owned),
                    visibility: visibility.to_owned(),
                    prompt_template: None,
                    created_at: created_at.to_owned(),
                    updated_at: created_at.to_owned(),
                },
            )
            .await
            .expect("agent creates");
        }

        let page = AgentRepo::list_visible(
            &*state.db,
            "mcp-test-user",
            db::AgentListQuery {
                status: None,
                executor_type: None,
                capabilities: Vec::new(),
                page: PageRequest {
                    cursor: None,
                    limit: 1,
                    include_total: true,
                    sort_by: SortBy::CreatedAt,
                    sort_order: SortOrder::Desc,
                },
            },
        )
        .await
        .expect("visible agent page loads");
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].name, "owned");
        assert_eq!(page.total_count, Some(2));
        assert!(page.next_cursor.is_some());

        let result = call_tool(&state, "forge_list_agents", json!({ "limit": 1 })).await;
        assert_eq!(result["data"].as_array().map(Vec::len), Some(1));
        assert_eq!(result["data"][0]["name"], "owned");
        assert_eq!(result["data"][0]["daemon_id"], Value::Null);
        assert_eq!(result["has_more"], true);

        let next = call_tool(
            &state,
            "forge_list_agents",
            json!({
                "limit": 1,
                "cursor": result["next_cursor"].clone(),
            }),
        )
        .await;
        assert_eq!(next["data"].as_array().map(Vec::len), Some(1));
        assert_eq!(next["data"][0]["name"], "global");
        assert_eq!(next["data"][0]["daemon_id"], Value::Null);
        assert_eq!(next["has_more"], false);
    });
}
