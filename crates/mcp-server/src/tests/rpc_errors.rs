//! MCP initialization, content envelopes and structured error mapping.

use super::*;

#[test]
fn initialize_returns_correct_protocol_version() {
    run_async(async {
        let state = sqlite_state().await;
        let result = dispatch(&state, "initialize", json!({}))
            .await
            .expect("initialize succeeds");
        assert_eq!(result["protocolVersion"], "2025-03-26");
        assert_eq!(result["serverInfo"]["version"], env!("CARGO_PKG_VERSION"));
    });
}

#[test]
fn known_tool_version_conflict_is_an_in_band_structured_outcome() {
    let response = McpToolError::from(db::DbError::VersionConflict)
        .with_call_context("forge_update_task", Some("project-1"), Some("user-1"))
        .into_tool_response(json!(1));
    let result = response.result.expect("tool failures use a success result");
    assert_eq!(result["isError"], true);
    assert_eq!(result["structuredContent"]["code"], "version_conflict");
    assert_eq!(result["structuredContent"]["status"], "failed");
    assert_eq!(
        result["structuredContent"]["scope"],
        json!({"scope_type": "project", "scope_id": "project-1"})
    );
    assert_eq!(
        result["structuredContent"]["retry"]["action"],
        "refresh_and_retry"
    );
    let content = result["content"][0]["text"]
        .as_str()
        .expect("structured error includes text content");
    assert_eq!(
        serde_json::from_str::<Value>(content).expect("text content is JSON"),
        result["structuredContent"]
    );

    let db_response = McpToolError::from(db::DbError::ExecutionAlreadyRunning {
        scope: "repository".to_owned(),
        execution_id: "execution-2".to_owned(),
    })
    .with_call_context("forge_task_action", Some("project-1"), Some("user-1"))
    .into_tool_response(json!(2));
    let db_result = db_response
        .result
        .expect("database tool failures use a success result");
    assert_eq!(
        db_result["structuredContent"]["details"],
        json!({
            "code": "execution_already_running",
            "scope": "repository",
            "execution_id": "execution-2",
        })
    );
}

#[test]
fn known_tool_execution_conflict_preserves_safe_details() {
    let response = McpToolError::from(ServiceError::ExecutionAlreadyRunning {
        scope: "interactive".to_owned(),
        execution_id: "execution-1".to_owned(),
    })
    .with_call_context(
        "forge_follow_up_execution",
        Some("project-1"),
        Some("user-1"),
    )
    .into_tool_response(json!(1));
    let result = response.result.expect("tool failures use a success result");
    assert_eq!(result["isError"], true);
    assert_eq!(result["structuredContent"]["code"], "transient_failure");
    assert_eq!(
        result["structuredContent"]["details"],
        json!({
            "code": "execution_already_running",
            "scope": "interactive",
            "execution_id": "execution-1",
        })
    );
    let content = result["content"][0]["text"]
        .as_str()
        .expect("structured error includes text content");
    assert_eq!(
        serde_json::from_str::<Value>(content).expect("text content is JSON"),
        result["structuredContent"]
    );
}

#[test]
fn known_tool_dispatch_preserves_execution_conflict_details() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, repo_id) = seed_project_repo(&state).await;
        let task = seed_task_in_project(&state, project_id.clone()).await;
        let agent = seed_agent(&state, "dispatch-conflict-agent").await;
        let now = now_rfc3339();
        let workspace_id = new_uuid_v4();
        WorkspaceRepo::create(
            &*state.db,
            CreateWorkspace {
                id: workspace_id.clone(),
                task_id: task.id.clone(),
                repo_id,
                worktree_path: "/tmp/mcp-dispatch-conflict".to_owned(),
                branch: "mcp-dispatch-conflict".to_owned(),
                status: WorkspaceStatus::Ready,
                before_sha: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("workspace creates");

        let parent_execution_id = new_uuid_v4();
        ExecutionRepo::create(
            &*state.db,
            CreateExecution {
                id: parent_execution_id.clone(),
                task_id: task.id.clone(),
                agent_id: Some(agent.id.clone()),
                role: "interactive".to_owned(),
                status: ExecutionStatus::Completed,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: None,
                agent_session_id: Some("mcp-parent-session".to_owned()),
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: Some(
                    r#"{"executor_type":"shell","config":{}}"#.to_owned(),
                ),
                workspace_id: Some(workspace_id.clone()),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("parent execution creates");
        let running_execution_id = new_uuid_v4();
        ExecutionRepo::create(
            &*state.db,
            CreateExecution {
                id: running_execution_id.clone(),
                task_id: task.id,
                agent_id: Some(agent.id),
                role: "interactive".to_owned(),
                status: ExecutionStatus::Running,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: Some(
                    r#"{"executor_type":"shell","config":{}}"#.to_owned(),
                ),
                workspace_id: Some(workspace_id),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("running execution creates");

        let error = dispatch_with_context(
            &state,
            &McpContext {
                project_id: Some(project_id),
                user_id: Some("mcp-test-user".to_owned()),
            },
            "tools/call",
            json!({
                "name": "forge_follow_up_execution",
                "arguments": {
                    "execution_id": parent_execution_id,
                    "message": "continue"
                }
            }),
        )
        .await
        .expect_err("known tool dispatch should report the running workspace");
        let result = error
            .into_tool_response(json!(1))
            .result
            .expect("known tool failures use an in-band result");
        assert_eq!(result["isError"], true);
        assert_eq!(result["structuredContent"]["code"], "transient_failure");
        let outcome: api_types::OrchestrationOutcome =
            serde_json::from_value(result["structuredContent"].clone())
                .expect("known-tool outcome matches the shared strict envelope");
        assert_eq!(
            outcome.details,
            Some(json!({
                "code": "execution_already_running",
                "scope": "repository",
                "execution_id": running_execution_id,
            }))
        );
    });
}

#[test]
fn known_tool_internal_failure_redacts_provider_details() {
    const PROVIDER_DETAIL_SENTINEL: &str = "provider-detail-must-not-leak-7f83c6d2-4b11-49a4-a851";
    let response = McpToolError::new(-32603, PROVIDER_DETAIL_SENTINEL)
        .with_data(json!({ "details": PROVIDER_DETAIL_SENTINEL }))
        .with_call_context("forge_get_task", None, Some("user-1"))
        .into_tool_response(json!(1));
    let result = response.result.expect("tool failures use a success result");
    assert_eq!(result["structuredContent"]["code"], "internal_failure");
    assert_eq!(
        result["structuredContent"]["safe_message"],
        "the operation failed"
    );
    let serialized = serde_json::to_string(&result).expect("result serializes");
    assert!(!serialized.contains(PROVIDER_DETAIL_SENTINEL));
    assert!(!serialized.contains("must-not-leak"));
}

#[test]
fn cycle_detected_maps_to_client_validation_error() {
    let error = McpToolError::from(db::DbError::CycleDetected);
    assert_eq!(error.code, -32602);
    let response = error
        .with_call_context(
            "forge_add_task_dependency",
            Some("project-1"),
            Some("user-1"),
        )
        .into_tool_response(json!(1));
    let result = response.result.expect("tool failures use a success result");
    assert_eq!(result["isError"], true);
    assert_eq!(result["structuredContent"]["code"], "validation_error");
    assert_eq!(result["structuredContent"]["status"], "failed");
    assert_ne!(result["structuredContent"]["code"], "internal_failure");
}

#[test]
fn malformed_and_unknown_mcp_failures_remain_json_rpc_errors() {
    for (code, message) in [(-32700, "parse error"), (-32601, "method not found")] {
        let response = McpToolError::protocol(code, message).into_response(json!(1));
        assert!(response.result.is_none());
        assert_eq!(response.error.expect("protocol error").code, code);
    }
}

#[test]
fn unknown_method_returns_method_not_found() {
    run_async(async {
        let state = sqlite_state().await;
        let error = dispatch(&state, "unknown", json!({}))
            .await
            .expect_err("unknown method errors");
        assert_eq!(error.code, -32601);
    });
}

#[test]
fn embedded_read_tools_require_authenticated_server_identity() {
    run_async(async {
        let state = sqlite_state().await;
        let error = dispatch_with_context(
            &state,
            &McpContext::default(),
            "tools/call",
            json!({
                "name": "forge_list_agent_profiles",
                "arguments": { "identity_id": "identity-1" }
            }),
        )
        .await
        .expect_err("profile reads must not accept an unbound caller identity");
        assert_eq!(error.code, -32001);

        let error = dispatch_with_context(
            &state,
            &McpContext::default(),
            "tools/call",
            json!({
                "name": "forge_get_main_agent",
                "arguments": {}
            }),
        )
        .await
        .expect_err("binding reads must bind to the authenticated account");
        assert_eq!(error.code, -32001);
    });
}

#[test]
fn tools_call_returns_mcp_content_envelope() {
    run_async(async {
        let state = sqlite_state().await;
        let task = seed_task(&state).await;
        let result = dispatch(
            &state,
            "tools/call",
            json!({
                "name": "forge_update_task",
                "arguments": {
                    "task_id": task.id,
                    "title": "Updated MCP",
                    "version": task.version,
                }
            }),
        )
        .await
        .expect("tool succeeds");

        let content = result["content"].as_array().expect("content array");
        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["type"], "text");

        let payload: Value = serde_json::from_str(
            content[0]["text"]
                .as_str()
                .expect("text content is a string"),
        )
        .expect("text content is JSON");
        assert_eq!(payload["title"], "Updated MCP");
    });
}
