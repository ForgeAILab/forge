//! MCP chat admission, usage visibility and atomic handoff delivery.

use super::*;

#[test]
fn revised_chat_mutations_fail_closed_for_unknown_chat_without_room_fallback() {
    run_async(async {
        let state = sqlite_state().await;
        let (_identity_id, _profile_id, user_id) = seed_chat_account(&state).await;
        let context = McpContext {
            project_id: None,
            user_id: Some(user_id),
        };
        let error = dispatch_with_context(
            &state,
            &context,
            "tools/call",
            json!({
                "name": "forge_send_agent_chat_message",
                "arguments": { "chat_id": "chat-1", "content": "hello" }
            }),
        )
        .await
        .expect_err("chat send must not fall back to Room persistence");
        assert_eq!(error.code, -32004);
    });
}

#[test]
fn mcp_chat_send_uses_atomic_admission_and_deduplicates() {
    run_async(async {
        let state = sqlite_state().await;
        let (_identity_id, _profile_id, user_id) = seed_chat_account(&state).await;
        let chat = state
            .agent_chat_service
            .ensure_main_chat(&user_id)
            .await
            .expect("main chat");
        let context = McpContext {
            project_id: None,
            user_id: Some(user_id.clone()),
        };

        let rejected = dispatch_with_context(
            &state,
            &context,
            "tools/call",
            json!({
                "name": "forge_send_agent_chat_message",
                "arguments": {
                    "chat_id": chat.id,
                    "content": "Authorization: Bearer should-not-persist"
                }
            }),
        )
        .await
        .expect_err("protected content is rejected before admission");
        assert_eq!(rejected.code, -32602);

        let messages = AgentChatMessageRepo::list_agent_chat_messages(
            &*state.db,
            AgentChatMessageListQuery {
                chat_id: chat.id.clone(),
                before_sequence: None,
                page: PageRequest {
                    cursor: None,
                    limit: 100,
                    include_total: false,
                    sort_by: SortBy::CreatedAt,
                    sort_order: SortOrder::Asc,
                },
            },
        )
        .await
        .expect("message count");
        assert!(messages.items.is_empty());

        let request = json!({
            "name": "forge_send_agent_chat_message",
            "arguments": {
                "chat_id": chat.id.clone(),
                "content": "Continue with the accepted brief",
                "dedupe_key": "mcp-send-once"
            }
        });
        let first = dispatch_with_context(&state, &context, "tools/call", request.clone())
            .await
            .expect("atomic MCP send");
        let first_payload: Value = serde_json::from_str(
            first["content"][0]["text"]
                .as_str()
                .expect("MCP text result"),
        )
        .expect("MCP JSON result");
        assert!(first_payload["message"].get("token_usage_json").is_none());
        assert_eq!(first_payload["message"]["usage"], json!([]));
        let second = dispatch_with_context(&state, &context, "tools/call", request)
            .await
            .expect("idempotent MCP send replay");
        let second_payload: Value = serde_json::from_str(
            second["content"][0]["text"]
                .as_str()
                .expect("MCP text result"),
        )
        .expect("MCP JSON result");
        assert_eq!(
            first_payload["message"]["id"],
            second_payload["message"]["id"]
        );

        let messages = AgentChatMessageRepo::list_agent_chat_messages(
            &*state.db,
            AgentChatMessageListQuery {
                chat_id: chat.id.clone(),
                before_sequence: None,
                page: PageRequest {
                    cursor: None,
                    limit: 100,
                    include_total: false,
                    sort_by: SortBy::CreatedAt,
                    sort_order: SortOrder::Asc,
                },
            },
        )
        .await
        .expect("message count");
        let turns = AgentChatTurnJobRepo::list_agent_chat_turn_jobs(&*state.db, &chat.id)
            .await
            .expect("turn count");
        assert_eq!(messages.items.len(), 1);
        assert_eq!(turns.len(), 1);
    });
}

#[test]
fn mcp_chat_reads_typed_usage_without_crossing_account_or_project_scope() {
    run_async(async {
        let state = sqlite_state().await;
        let (identity_id, profile_id, user_id) = seed_chat_account(&state).await;
        let main_chat = state
            .agent_chat_service
            .ensure_main_chat(&user_id)
            .await
            .expect("main chat");
        let message =
            append_chat_agent_message(&state, &main_chat.id, &identity_id, &profile_id).await;
        seed_chat_usage_event(&state, &message, &identity_id, &profile_id, &user_id).await;

        let account_context = McpContext {
            project_id: None,
            user_id: Some(user_id.clone()),
        };
        let result = dispatch_with_context(
            &state,
            &account_context,
            "tools/call",
            json!({
                "name": "forge_list_agent_chat_messages",
                "arguments": { "chat_id": main_chat.id.clone() },
            }),
        )
        .await
        .expect("authorized account chat read");
        let payload: Value = serde_json::from_str(
            result["content"][0]["text"]
                .as_str()
                .expect("MCP text result"),
        )
        .expect("MCP JSON result");
        let assistant = payload["items"]
            .as_array()
            .expect("message items")
            .iter()
            .find(|item| item["id"] == message.id)
            .expect("seeded assistant message");
        assert!(assistant.get("token_usage_json").is_none());
        assert_eq!(assistant["usage"][0]["surface"], "main_chat");
        assert_eq!(assistant["usage"][0]["counters"]["input_tokens"], 12);
        assert_eq!(assistant["usage"][0]["counters"]["output_tokens"], 3);
        assert!(assistant["usage"][0]["cost"].is_object());

        let project_id = seed_chat_project(&state, &identity_id).await;
        let project_chat = AgentChatRepo::get_project_chat(&*state.db, &project_id)
            .await
            .expect("project chat lookup")
            .expect("project chat exists");
        let project_context = McpContext {
            project_id: Some(project_id.clone()),
            user_id: Some(user_id.clone()),
        };
        dispatch_with_context(
            &state,
            &project_context,
            "tools/call",
            json!({
                "name": "forge_list_agent_chat_messages",
                "arguments": { "chat_id": project_chat.id },
            }),
        )
        .await
        .expect("authorized Project chat read");

        let wrong_project_scope = dispatch_with_context(
            &state,
            &project_context,
            "tools/call",
            json!({
                "name": "forge_list_agent_chat_messages",
                "arguments": { "chat_id": main_chat.id.clone() },
            }),
        )
        .await
        .expect_err("Project scope cannot read account Main Chat");
        assert_eq!(wrong_project_scope.code, -32602);

        let wrong_account = dispatch_with_context(
            &state,
            &McpContext {
                project_id: None,
                user_id: Some("mcp-test-user".to_owned()),
            },
            "tools/call",
            json!({
                "name": "forge_list_agent_chat_messages",
                "arguments": { "chat_id": main_chat.id.clone() },
            }),
        )
        .await
        .expect_err("another account cannot read Main Chat");
        assert_eq!(wrong_account.code, -32004);
    });
}

#[test]
fn mcp_handoff_admits_delivery_and_target_turn_atomically() {
    run_async(async {
        let state = sqlite_state().await;
        let (identity_id, _profile_id, user_id) = seed_chat_account(&state).await;
        let project_id = seed_chat_project(&state, &identity_id).await;
        let context = McpContext {
            project_id: None,
            user_id: Some(user_id.clone()),
        };
        let request = json!({
            "name": "forge_create_agent_handoff",
            "arguments": {
                "project_id": project_id,
                "content": "Approved brief for the Project Agent",
                "dedupe_key": "mcp-handoff-once"
            }
        });

        let first = dispatch_with_context(&state, &context, "tools/call", request.clone())
            .await
            .expect("atomic handoff");
        let first_payload: Value = serde_json::from_str(
            first["content"][0]["text"]
                .as_str()
                .expect("MCP text result"),
        )
        .expect("MCP JSON result");
        assert_eq!(first_payload["status"], "delivered");
        assert!(first_payload["target_message_id"].as_str().is_some());
        assert!(first_payload["target_turn_job_id"].as_str().is_some());

        let second = dispatch_with_context(&state, &context, "tools/call", request)
            .await
            .expect("idempotent handoff replay");
        let second_payload: Value = serde_json::from_str(
            second["content"][0]["text"]
                .as_str()
                .expect("MCP text result"),
        )
        .expect("MCP JSON result");
        assert_eq!(
            first_payload["id"], second_payload["id"],
            "dedupe returns the original handoff"
        );

        let handoff_id = first_payload["id"].as_str().expect("handoff id");
        let target_chat = AgentChatRepo::get_project_chat(&*state.db, &project_id)
            .await
            .expect("target chat lookup")
            .expect("target chat exists");
        let handoffs = AgentHandoffRepo::list_agent_handoffs(&*state.db, &target_chat.id)
            .await
            .expect("handoff count");
        let target_messages = AgentChatMessageRepo::list_agent_chat_messages(
            &*state.db,
            AgentChatMessageListQuery {
                chat_id: target_chat.id.clone(),
                before_sequence: None,
                page: PageRequest {
                    cursor: None,
                    limit: 100,
                    include_total: false,
                    sort_by: SortBy::CreatedAt,
                    sort_order: SortOrder::Asc,
                },
            },
        )
        .await
        .expect("target message count");
        let target_turns =
            AgentChatTurnJobRepo::list_agent_chat_turn_jobs(&*state.db, &target_chat.id)
                .await
                .expect("target turn count");
        assert_eq!(handoffs.len(), 1);
        assert_eq!(handoffs[0].id, handoff_id);
        assert_eq!(
            handoffs[0].target_message_id.as_deref(),
            first_payload["target_message_id"].as_str()
        );
        assert_eq!(
            handoffs[0].target_turn_job_id.as_deref(),
            first_payload["target_turn_job_id"].as_str()
        );
        assert_eq!(
            target_messages
                .items
                .iter()
                .filter(|message| message.handoff_id.as_deref() == Some(handoff_id))
                .count(),
            1
        );
        assert_eq!(
            target_turns
                .iter()
                .filter(|turn| turn.causation_id.as_deref() == Some(handoff_id))
                .count(),
            1
        );
    });
}
