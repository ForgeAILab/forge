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
