//! The frozen oracle records 33e2a974's scope and moved-handler roles,
//! independently of the current registry. H's domain roles remain in H.
use super::*;
use std::collections::BTreeSet;

pub(super) fn base_admitted(grant: &str, name: &str) -> bool {
    let oracle: Value =
        serde_json::from_str(include_str!("../../tests/fixtures/mcp_base_authority.json")).unwrap();
    oracle["rows"][name][grant]
        .as_bool()
        .expect("every grant/role/tool has a frozen row")
}

#[test]
fn every_tool_has_a_frozen_scope_and_role_row_and_rpc_advertisement_equals_admission() {
    run_async(async {
        use operation_registry::authority::MCP_OPERATIONS;
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let oracle: Value =
            serde_json::from_str(include_str!("../../tests/fixtures/mcp_base_authority.json"))
                .unwrap();
        assert_eq!(
            oracle["rows"]
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>(),
            MCP_OPERATIONS.iter().map(|(name, _)| *name).collect()
        );
        let user_id = "mcp-test-user";
        for (grant, role) in [
            ("account", None),
            ("project_public", None),
            ("project_member", Some("member")),
            ("project_admin", Some("admin")),
            ("project_owner", Some("owner")),
        ] {
            if let Some(role) = role {
                if ProjectMemberRepo::get_member(&*state.db, &project_id, user_id)
                    .await
                    .unwrap()
                    .is_some()
                {
                    ProjectMemberRepo::remove_member(&*state.db, &project_id, user_id)
                        .await
                        .unwrap();
                }
                let now = now_rfc3339();
                ProjectMemberRepo::add_member(
                    &*state.db,
                    CreateProjectMember {
                        id: new_uuid_v4(),
                        project_id: project_id.clone(),
                        user_id: user_id.into(),
                        role: role.into(),
                        created_at: now.clone(),
                        updated_at: now,
                    },
                )
                .await
                .unwrap();
            }
            let context = McpContext {
                user_id: Some(user_id.into()),
                project_id: (grant != "account").then(|| project_id.clone()),
            };
            let listed = dispatch_with_context(&state, &context, "tools/list", json!({}))
                .await
                .unwrap();
            let names = listed["tools"]
                .as_array()
                .unwrap()
                .iter()
                .map(|tool| tool["name"].as_str().unwrap())
                .collect::<BTreeSet<_>>();
            for (name, _) in MCP_OPERATIONS {
                let expected = base_admitted(grant, name);
                assert_eq!(
                    names.contains(name),
                    expected,
                    "no capability widening/narrowing: {grant}/{name}"
                );
                let call = dispatch_with_context(
                    &state,
                    &context,
                    "tools/call",
                    json!({"name":name,"arguments":7}),
                )
                .await;
                let admitted = match call {
                    Ok(_) => true,
                    Err(error) => error
                        .into_response(Value::Null)
                        .error
                        .unwrap()
                        .data
                        .is_none_or(|data| data["code"] != "mcp_scope_denied"),
                };
                assert_eq!(
                    admitted, expected,
                    "list/call admission parity: {grant}/{name}"
                );
            }
        }
    });
}

#[test]
fn each_moved_rpc_refuses_unknown_fields_with_the_one_typed_contract_error() {
    run_async(async {
        let state = sqlite_state().await;
        for spec in operation_registry::mcp::CATALOG.iter() {
            let error = dispatch(
                &state,
                "tools/call",
                json!({"name":spec.name,"arguments":{"unexpected":true}}),
            )
            .await
            .unwrap_err();
            assert_eq!(error.code, -32602, "{}", spec.name);
            let data = error
                .into_response(Value::Null)
                .error
                .unwrap()
                .data
                .unwrap();
            assert_eq!(data["code"], "mcp_contract_invalid", "{}", spec.name);
            assert_eq!(data["operation"], spec.name);
            assert!(
                data["details"].as_str().unwrap().contains("unexpected"),
                "{}: {}",
                spec.name,
                data
            );
        }
    });
}

#[test]
fn conditional_daemon_field_is_advertised_and_enforced_by_the_evaluator() {
    run_async(async {
        let state = sqlite_state().await;
        let list = dispatch(&state, "tools/list", json!({})).await.unwrap();
        let register = list["tools"]
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == "forge_register_agent")
            .unwrap();
        assert_eq!(
            register["inputSchema"]["properties"]["daemon_id"]["type"],
            "null"
        );
        let denied = dispatch(
            &state,
            "tools/call",
            json!({"name":"forge_register_agent","arguments":{"daemon_id":"d","unexpected":true}}),
        )
        .await
        .unwrap_err();
        assert_eq!(denied.code, -32003);
        assert_eq!(
            denied
                .into_response(Value::Null)
                .error
                .unwrap()
                .data
                .unwrap()["code"],
            "admin_required"
        );
        UserRepo::set_admin(&*state.db, "mcp-test-user", true)
            .await
            .unwrap();
        let list = dispatch(&state, "tools/list", json!({})).await.unwrap();
        let register = list["tools"]
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == "forge_register_agent")
            .unwrap();
        assert_eq!(
            register["inputSchema"]["properties"]["daemon_id"]["type"],
            json!(["string", "null"])
        );
    });
}

#[test]
fn owned_identity_denial_precedes_contract_details_and_stays_redacted() {
    run_async(async {
        let state = sqlite_state().await;
        let (identity_id, _, _) = seed_chat_account(&state).await;
        for id in [identity_id.as_str(), "unknown-identity"] {
            let error = dispatch(&state, "tools/call", json!({"name":"forge_list_agent_profiles","arguments":{"identity_id":id,"unexpected":true}})).await.unwrap_err();
            assert_eq!(error.code, -32004);
            assert_ne!(
                error
                    .into_response(Value::Null)
                    .error
                    .unwrap()
                    .data
                    .unwrap_or(Value::Null)["code"],
                "mcp_contract_invalid"
            );
        }
    });
}

#[test]
fn registry_contract_correction_survives_the_in_band_error_projection() {
    run_async(async {
        let state = sqlite_state().await;
        let error = dispatch(
            &state,
            "tools/call",
            json!({"name":"forge_get_main_agent","arguments":{"unexpected":true}}),
        )
        .await
        .unwrap_err();
        let response = error.into_tool_response(json!(1));
        let result = response.result.unwrap();
        assert_eq!(result["isError"], true);
        assert_eq!(result["structuredContent"]["code"], "validation_error");
        let correction = &result["structuredContent"]["details"];
        assert_eq!(correction["code"], "mcp_contract_invalid");
        assert!(correction["details"]
            .as_str()
            .unwrap()
            .contains("unexpected"));
        assert!(correction["details"]
            .as_str()
            .unwrap()
            .contains("expected no arguments"));
    });
}

#[test]
fn handlers_recheck_identity_ownership_and_daemon_pinning_at_the_effect() {
    run_async(async {
        let state = sqlite_state().await;
        let (foreign_identity, _, _) = seed_chat_account(&state).await;
        let context = McpContext {
            project_id: None,
            user_id: Some("mcp-test-user".into()),
        };
        let error = crate::tools::dispatch_handler_only(
            &state,
            "forge_set_main_agent",
            json!({"identity_id": foreign_identity, "expected_version": 0}),
            &context,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, -32004);
        let bound: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM account_main_agent_binding WHERE account_id = 'mcp-test-user'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(bound, 0, "a foreign identity is never bound");

        let (executor_type, daemon_id) = seed_agent_registration_deps(&state).await;
        let error = crate::tools::dispatch_handler_only(
            &state,
            "forge_register_agent",
            json!({"name": "pinned", "executor_type": executor_type, "daemon_id": daemon_id}),
            &context,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, -32003);
        let registered: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM agent_identity WHERE owner_id = 'mcp-test-user'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(registered, 0, "a refused pin registers nothing");
    });
}

#[test]
fn a_foreign_identity_answers_exactly_like_a_missing_one() {
    run_async(async {
        let state = sqlite_state().await;
        let (foreign_identity, _, _) = seed_chat_account(&state).await;
        for (name, extra) in [
            ("forge_list_agent_profiles", json!({})),
            ("forge_list_agent_sessions", json!({})),
            ("forge_set_main_agent", json!({"expected_version": 0})),
        ] {
            let mut answers = Vec::new();
            for id in [foreign_identity.as_str(), "missing-identity"] {
                let mut arguments = extra.clone();
                arguments["identity_id"] = json!(id);
                let error = call_tool_error(&state, name, arguments).await;
                answers.push((error.code, error.message.replace(id, "<id>"), error.data));
            }
            assert_eq!(answers[0], answers[1], "{name}");
            assert_eq!(answers[0].0, -32004, "{name}");
        }
    });
}

#[test]
fn an_account_grant_reaches_project_tools_only_with_the_project_role() {
    run_async(async {
        let state = sqlite_state().await;
        // Ownerless Project: visible to the user, who holds no role in it.
        let (project_id, _) = seed_project_repo(&state).await;
        let calls = |project_id: &str| {
            [
                ("forge_get_project_agent", json!({"project_id": project_id})),
                (
                    "forge_set_project_agent",
                    json!({"project_id": project_id, "identity_id": "i", "expected_version": 0}),
                ),
                (
                    "forge_list_agent_handoffs",
                    json!({"project_id": project_id}),
                ),
                (
                    "forge_get_agent_handoff",
                    json!({"project_id": project_id, "handoff_id": "missing"}),
                ),
                (
                    "forge_create_agent_handoff",
                    json!({"project_id": project_id, "content": "c", "dedupe_key": "k"}),
                ),
            ]
        };
        for (name, arguments) in calls(&project_id) {
            let error = call_tool_error(&state, name, arguments).await;
            assert_eq!(error.code, -32001, "{name}");
            assert_eq!(error.data.unwrap()["code"], "mcp_scope_denied", "{name}");
        }
        let now = now_rfc3339();
        ProjectMemberRepo::add_member(
            &*state.db,
            CreateProjectMember {
                id: new_uuid_v4(),
                project_id: project_id.clone(),
                user_id: "mcp-test-user".into(),
                role: "member".into(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .unwrap();
        for (name, arguments) in calls(&project_id) {
            let denied = dispatch(
                &state,
                "tools/call",
                json!({"name": name, "arguments": arguments}),
            )
            .await
            .err()
            .and_then(|error| error.into_response(Value::Null).error.unwrap().data)
            .is_some_and(|data| data["code"] == "mcp_scope_denied");
            assert_eq!(
                denied,
                name == "forge_set_project_agent",
                "an ordinary member passes every role check but the binding setter: {name}"
            );
        }
    });
}

#[test]
fn contract_corrections_do_not_echo_argument_values_and_need_an_object() {
    run_async(async {
        let state = sqlite_state().await;
        let error = call_tool_error(
            &state,
            "forge_list_agents",
            json!({"limit": "s3cret-value"}),
        )
        .await;
        assert_eq!(error.code, -32602);
        let data = error.data.unwrap();
        assert_eq!(data["code"], "mcp_contract_invalid");
        let details = data["details"].as_str().unwrap();
        assert!(!details.contains("s3cret"), "{details}");
        assert!(
            details.contains("invalid type: string, expected"),
            "{details}"
        );

        let (foreign_identity, _, _) = seed_chat_account(&state).await;
        let error = call_tool_error(
            &state,
            "forge_list_agent_profiles",
            json!([foreign_identity]),
        )
        .await;
        assert_eq!(error.code, -32602);
    });
}
