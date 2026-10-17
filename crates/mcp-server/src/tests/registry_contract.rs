//! Freeze bab42a97 advertisement separately from effective grant/role
//! acceptance; H repairs advertised refusals without widening authority.
use super::*;
use std::collections::BTreeSet;

pub(super) fn base_admitted(grant: &str, name: &str) -> bool {
    let oracle: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/mcp_h_base_authority.json"
    ))
    .unwrap();
    oracle["rows"][name][grant]["callable"]
        .as_bool()
        .expect("every grant/role/tool has a frozen row")
}

pub(super) fn admitted_after_consolidation(grant: &str, name: &str) -> bool {
    if name == "forge_update_project_lifecycle_hooks" {
        matches!(
            grant,
            "account"
                | "account_admin"
                | "project_admin"
                | "project_member_owner"
                | "project_owner"
        )
    } else {
        base_admitted(grant, name)
    }
}

#[test]
fn every_tool_has_a_frozen_scope_and_role_row_and_rpc_advertisement_equals_admission() {
    run_async(async {
        use operation_registry::mcp::CATALOG;
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let oracle: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/mcp_h_base_authority.json"
        ))
        .unwrap();
        assert_eq!(
            oracle["rows"]
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>(),
            CATALOG.iter().map(|spec| spec.name).collect()
        );
        seed_chat_account(&state).await;
        let user_id = "mcp-test-user";
        for (grant, role) in [
            ("account", None),
            ("account_admin", None),
            ("project_public", None),
            ("project_global_admin", None),
            ("project_member", Some("member")),
            ("project_admin", Some("admin")),
            ("project_member_owner", Some("owner")),
            ("project_owner", None),
            ("project_invisible", None),
        ] {
            UserRepo::set_admin(
                &*state.db,
                user_id,
                matches!(grant, "account_admin" | "project_global_admin"),
            )
            .await
            .unwrap();
            if ProjectMemberRepo::get_member(&*state.db, &project_id, user_id)
                .await
                .unwrap()
                .is_some()
            {
                ProjectMemberRepo::remove_member(&*state.db, &project_id, user_id)
                    .await
                    .unwrap();
            }
            let owner = match grant {
                "project_owner" => Some(user_id),
                "project_invisible" => Some("chat-user"),
                _ => None,
            };
            sqlx::query("UPDATE project SET owner_id=? WHERE id=?")
                .bind(owner)
                .bind(&project_id)
                .execute(state.db.pool())
                .await
                .unwrap();
            if let Some(role) = role {
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
                project_id: (!grant.starts_with("account")).then(|| project_id.clone()),
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
            let baseline: Value = serde_json::from_str(include_str!(
                "../../tests/fixtures/mcp_h_base_definitions.json"
            ))
            .unwrap();
            let surface = if grant.starts_with("account") {
                "mcp_account"
            } else {
                "mcp_project"
            };
            let before = baseline[surface]
                .as_array()
                .unwrap()
                .iter()
                .filter(|tool| {
                    oracle["rows"][tool["name"].as_str().unwrap()][grant]["listed"] == true
                })
                .cloned()
                .map(|mut tool| {
                    if tool["name"] == "forge_register_agent" && grant != "account_admin" {
                        tool["inputSchema"]["properties"]["daemon_id"] = json!({"type":"null"});
                    }
                    tool
                })
                .collect::<Vec<_>>();
            let before_bytes = serde_json::to_vec(&before).unwrap().len();
            let after_bytes = serde_json::to_vec(&listed["tools"]).unwrap().len();
            println!(
                "MCP_ROLE_PREFIX {grant} before_tools={} after_tools={} before_bytes={before_bytes} after_bytes={after_bytes} before_tokens={} after_tokens={}",
                before.len(),
                names.len(),
                before_bytes.div_ceil(4),
                after_bytes.div_ceil(4)
            );
            println!(
                "TOOL_DEFINITIONS mcp_grant_{grant} {}",
                serde_json::to_string(&listed["tools"]).unwrap()
            );
            for spec in CATALOG.iter() {
                let name = spec.name;
                let expected = admitted_after_consolidation(grant, name);
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
        let (project_id, _) = seed_project_repo(&state).await;
        sqlx::query("UPDATE project SET owner_id='mcp-test-user' WHERE id=?")
            .bind(&project_id)
            .execute(state.db.pool())
            .await
            .unwrap();
        for spec in operation_registry::mcp::CATALOG.iter() {
            let bound_only = matches!(
                spec.role,
                operation_registry::mcp::RequiredRole::Owner
                    | operation_registry::mcp::RequiredRole::BoundProject
            );
            let context = McpContext {
                user_id: Some("mcp-test-user".into()),
                project_id: bound_only.then(|| project_id.clone()),
            };
            let error = dispatch_with_context(
                &state,
                &context,
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

#[test]
fn task_transition_keeps_the_mcp_system_policy_and_gains_no_user_override() {
    run_async(async {
        let state = sqlite_state().await;
        let task = seed_task(&state).await;
        let execution_id = seed_execution(&state, task.id.clone()).await;
        // No edge leads from the initial state straight to `done`. An
        // interactive user may override that; an MCP credential may not.
        let error = call_tool_error(
            &state,
            "forge_transition_task",
            json!({"task_id":task.id,"status":"done","version":task.version}),
        )
        .await;
        assert_eq!(error.code, -32010, "{}", error.message);
        let unchanged = TaskRepo::get_by_id(&*state.db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(unchanged.status, task.status);
        assert_eq!(unchanged.version, task.version);

        call_tool(
            &state,
            "forge_transition_task",
            json!({"task_id":task.id,"status":"in_progress","version":task.version}),
        )
        .await;
        let payload: String = sqlx::query_scalar("SELECT payload_json FROM task_step WHERE task_id=? AND kind='hooks' ORDER BY seq DESC LIMIT 1")
            .bind(&task.id).fetch_one(state.db.pool()).await.unwrap();
        let payload: Value = serde_json::from_str(&payload).unwrap();
        let actor: api_types::Actor = serde_json::from_value(payload["actor"].clone()).unwrap();
        assert_eq!(
            actor,
            api_types::Actor::system(api_types::SystemComponent::Mcp),
            "the transition runs under the MCP system policy"
        );
        let execution = ExecutionRepo::get_by_id(&*state.db, &execution_id)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(
            execution.status,
            ExecutionStatus::Cancelled,
            "only an interactive user's state move stops the displaced run"
        );
    });
}

#[test]
fn both_settings_paths_validate_agent_usability_for_the_authenticated_user() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        sqlx::query("UPDATE project SET owner_id='mcp-test-user' WHERE id=?")
            .bind(&project_id)
            .execute(state.db.pool())
            .await
            .unwrap();
        let project = ProjectRepo::get_by_id(&*state.db, &project_id)
            .await
            .unwrap()
            .unwrap();
        let settings = json!({"default_role_assignments":[{"role_name":"coder","assignee_type":"agent","assignee_id":"not-usable"}]});
        let error = call_tool_error(
            &state,
            "forge_update_project",
            json!({"project_id":project_id,"version":project.version,"settings":settings}),
        )
        .await;
        assert!(
            error.message.contains("agent not usable in this project"),
            "{}",
            error.message
        );
        let after = ProjectRepo::get_by_id(&*state.db, &project_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.version, project.version);
        assert_eq!(after.settings, project.settings);
        // An existing, structurally valid assignment is rechecked even when
        // only lifecycle hooks are being replaced.
        sqlx::query("UPDATE project SET settings=? WHERE id=?")
            .bind(settings.to_string())
            .bind(&project_id)
            .execute(state.db.pool())
            .await
            .unwrap();
        let error = call_tool_error(
            &state,
            "forge_update_project_lifecycle_hooks",
            json!({"project_id":project_id,"version":project.version,"lifecycle_hooks":{}}),
        )
        .await;
        assert!(
            error.message.contains("agent not usable in this project"),
            "{}",
            error.message
        );
        let after = ProjectRepo::get_by_id(&*state.db, &project_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.version, project.version);
        assert_eq!(
            serde_json::from_str::<Value>(&after.settings).unwrap(),
            settings
        );
    });
}

#[test]
fn unknown_nested_fields_fail_before_any_handler_effect() {
    run_async(async {
        let state = sqlite_state().await;
        let root = seed_task(&state).await;
        let (project_id, _) = seed_project_repo(&state).await;
        sqlx::query("UPDATE project SET owner_id='mcp-test-user' WHERE id=?")
            .bind(&project_id)
            .execute(state.db.pool())
            .await
            .unwrap();
        let execution_id = seed_execution(&state, root.id.clone()).await;
        for (tool, arguments) in [
            (
                "forge_create_sub_tasks",
                json!({"parent_task_id":root.id,"subtasks":[{"title":"child","unknown":true}]}),
            ),
            (
                "forge_follow_up_execution",
                json!({"execution_id":execution_id,"message":"continue","overrides":{"unknown":true}}),
            ),
            (
                "forge_update_project_lifecycle_hooks",
                json!({"project_id":project_id,"version":1,"lifecycle_hooks":{"before_work":[{"type":"script","command":"true","unknown":true}]}}),
            ),
            (
                "forge_task_action",
                json!({"task_id":root.id,"version":root.version,"action":{"verb":"retry","unknown":true}}),
            ),
        ] {
            let before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task")
                .fetch_one(state.db.pool())
                .await
                .unwrap();
            let error = call_tool_error(&state, tool, arguments).await;
            assert_eq!(error.code, -32602, "{tool}: {}", error.message);
            assert_eq!(
                error.data.unwrap()["code"],
                "mcp_contract_invalid",
                "{tool}"
            );
            let after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task")
                .fetch_one(state.db.pool())
                .await
                .unwrap();
            assert_eq!(before, after);
        }
    });
}

/// Every id-taking Task/Project tool, with the id at the root and nested. The
/// reference is refused before the handler, so the other arguments only have
/// to satisfy nothing: the answer must not depend on whether the id exists.
fn reference_calls(own_project: &str, own_task: &str, id: &str) -> Vec<(&'static str, Value)> {
    let mut calls = vec![
        ("forge_preview_prompt", json!({"task_id":id,"role":"coder"})),
        ("forge_assign_agent", json!({"task_id":id,"agent_id":"a"})),
        (
            "forge_task_action",
            json!({"task_id":id,"action":{"verb":"start"},"version":1}),
        ),
        ("forge_list_executions", json!({"task_id":id})),
        ("forge_update_task", json!({"task_id":id,"version":1})),
        (
            "forge_transition_task",
            json!({"task_id":id,"status":"in_progress","version":1}),
        ),
        (
            "forge_add_task_dependency",
            json!({"task_id":id,"depends_on_id":own_task}),
        ),
        (
            "forge_add_task_dependency",
            json!({"task_id":own_task,"depends_on_id":id}),
        ),
        (
            "forge_remove_task_dependency",
            json!({"task_id":id,"depends_on_id":own_task}),
        ),
        (
            "forge_remove_task_dependency",
            json!({"task_id":own_task,"depends_on_id":id}),
        ),
        (
            "forge_create_sub_tasks",
            json!({"parent_task_id":id,"subtasks":[{"title":"child"}]}),
        ),
        (
            "forge_reorder_sub_tasks",
            json!({"parent_task_id":id,"ordered_ids":[]}),
        ),
        (
            "forge_reorder_sub_tasks",
            json!({"parent_task_id":own_task,"ordered_ids":[id]}),
        ),
        (
            "forge_create_task",
            json!({"project_id":own_project,"title":"t","parent_task_id":id}),
        ),
        (
            "forge_create_task",
            json!({"project_id":own_project,"title":"t","depends_on_ids":[id]}),
        ),
        (
            "forge_follow_up_execution",
            json!({"execution_id":id,"message":"continue"}),
        ),
    ];
    for name in [
        "forge_get_task",
        "forge_get_task_diff",
        "forge_list_task_dependencies",
        "forge_list_task_dependents",
    ] {
        calls.push((name, json!({"task_id":id})));
    }
    calls.push(("forge_list_sub_tasks", json!({"parent_task_id":id})));
    for name in ["forge_get_project", "forge_list_tasks"] {
        calls.push((name, json!({"project_id":id})));
    }
    calls.push(("forge_update_project", json!({"project_id":id,"version":1})));
    calls.push((
        "forge_update_project_lifecycle_hooks",
        json!({"project_id":id,"version":1,"lifecycle_hooks":{}}),
    ));
    calls.push(("forge_memory_search", json!({"project_id":id,"query":"q"})));
    calls
}

#[test]
fn a_foreign_task_execution_or_project_answers_exactly_like_a_missing_one() {
    run_async(async {
        let state = sqlite_state().await;
        seed_chat_account(&state).await;
        let own = seed_task(&state).await;
        let foreign = seed_task(&state).await;
        let foreign_execution = seed_execution(&state, foreign.id.clone()).await;
        let before: (i64, i64) = (
            sqlx::query_scalar("SELECT COUNT(*) FROM task")
                .fetch_one(state.db.pool())
                .await
                .unwrap(),
            sqlx::query_scalar("SELECT COUNT(*) FROM task_dependency")
                .fetch_one(state.db.pool())
                .await
                .unwrap(),
        );
        // A grant constrained to `own`'s Project while the other Project is
        // visible to the user; then an account grant while it is not.
        for constrained in [true, false] {
            if !constrained {
                sqlx::query("UPDATE project SET owner_id='chat-user' WHERE id=?")
                    .bind(&foreign.project_id)
                    .execute(state.db.pool())
                    .await
                    .unwrap();
            }
            let context = McpContext {
                user_id: Some("mcp-test-user".into()),
                project_id: constrained.then(|| own.project_id.clone()),
            };
            let missing = new_uuid_v4();
            let mut compared = BTreeSet::new();
            for (kind, existing) in [
                ("task", foreign.id.as_str()),
                ("execution", foreign_execution.as_str()),
                ("project", foreign.project_id.as_str()),
            ] {
                let answers = |id: &str| {
                    reference_calls(&own.project_id, &own.id, id)
                        .into_iter()
                        .filter(|(name, arguments)| match kind {
                            "execution" => *name == "forge_follow_up_execution",
                            "project" => arguments.get("project_id") == Some(&json!(id)),
                            _ => {
                                *name != "forge_follow_up_execution"
                                    && arguments.get("project_id") != Some(&json!(id))
                            }
                        })
                        .collect::<Vec<_>>()
                };
                for ((name, there), (_, absent)) in
                    answers(existing).into_iter().zip(answers(&missing))
                {
                    let mut seen = Vec::new();
                    for (id, arguments) in [(existing, there), (missing.as_str(), absent)] {
                        let error = dispatch_with_context(
                            &state,
                            &context,
                            "tools/call",
                            json!({"name":name,"arguments":arguments}),
                        )
                        .await
                        .expect_err("a reference outside the grant is refused");
                        let error = error.into_response(Value::Null).error.unwrap();
                        seen.push((
                            error.code,
                            error.message.replace(id, "<id>"),
                            error.data.map(|data| data.to_string().replace(id, "<id>")),
                        ));
                    }
                    assert_eq!(seen[0], seen[1], "constrained={constrained} {name} {kind}");
                    assert!(
                        !format!("{:?}", seen[0]).contains(&foreign.id)
                            && !format!("{:?}", seen[0]).contains(&foreign.project_id),
                        "{name} names nothing but the id the caller sent"
                    );
                    compared.insert(name);
                }
            }
            // Every Task/Project tool that takes an id is in the table.
            let id_taking = operation_registry::mcp::CATALOG
                .iter()
                .filter(|spec| {
                    let fields = spec.input.schema["properties"].as_object().unwrap();
                    spec.resource != operation_registry::mcp::ResourceRule::None
                        && ["task_id", "parent_task_id", "execution_id"]
                            .iter()
                            .any(|field| fields.contains_key(*field))
                        || matches!(
                            spec.name,
                            "forge_get_project"
                                | "forge_list_tasks"
                                | "forge_update_project"
                                | "forge_update_project_lifecycle_hooks"
                                | "forge_memory_search"
                        )
                })
                .map(|spec| spec.name)
                .collect::<BTreeSet<_>>();
            assert!(
                id_taking.is_subset(&compared),
                "untested id-taking tools: {:?}",
                id_taking.difference(&compared).collect::<Vec<_>>()
            );
        }
        let after: (i64, i64) = (
            sqlx::query_scalar("SELECT COUNT(*) FROM task")
                .fetch_one(state.db.pool())
                .await
                .unwrap(),
            sqlx::query_scalar("SELECT COUNT(*) FROM task_dependency")
                .fetch_one(state.db.pool())
                .await
                .unwrap(),
        );
        assert_eq!(before, after, "no refused reference had an effect");
    });
}

#[test]
fn lifecycle_scripts_require_project_admin_on_both_mcp_paths_and_shared_rest_validation() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let hooks = json!({"before_work":[{"type":"script","command":"echo proof","timeout_seconds":60,"blocking":false}]});
        let settings = json!({"lifecycle_hooks":hooks});
        let before = ProjectRepo::get_by_id(&*state.db, &project_id)
            .await
            .unwrap()
            .unwrap();
        let context = McpContext {
            user_id: Some("mcp-test-user".into()),
            project_id: Some(project_id.clone()),
        };
        for name in [
            "forge_update_project",
            "forge_update_project_lifecycle_hooks",
        ] {
            let mut arguments = json!({"project_id":project_id,"version":before.version});
            arguments[if name == "forge_update_project" {
                "settings"
            } else {
                "lifecycle_hooks"
            }] = if name == "forge_update_project" {
                settings.clone()
            } else {
                hooks.clone()
            };
            assert!(
                dispatch_with_context(
                    &state,
                    &context,
                    "tools/call",
                    json!({"name":name,"arguments":arguments})
                )
                .await
                .is_err(),
                "{name}: ownerless visibility grants no command authority"
            );
        }
        let workflow = services::workflow::engine::WorkflowEngine::resolve_workflow(
            &before.workflow_definition,
        );
        assert!(
            matches!(
                services::project_settings::validate_project_settings(
                    &state.db,
                    &settings,
                    &workflow,
                    Some(&project_id),
                    Some("mcp-test-user")
                )
                .await,
                Err(services::ServiceError::AuthorizationDenied { .. })
            ),
            "REST uses this same effect validator"
        );
        let after = ProjectRepo::get_by_id(&*state.db, &project_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after, before, "nothing persisted on denial");
        // An account grant (no bound Project) held by a plain member names
        // the Project in its arguments: membership is still not command
        // authority, on either tool.
        let member_id = new_uuid_v4();
        ProjectMemberRepo::add_member(
            &*state.db,
            CreateProjectMember {
                id: member_id.clone(),
                project_id: project_id.clone(),
                user_id: "mcp-test-user".into(),
                role: "member".into(),
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
        let account = McpContext {
            user_id: Some("mcp-test-user".into()),
            project_id: None,
        };
        for grant in [&account, &context] {
            for name in [
                "forge_update_project",
                "forge_update_project_lifecycle_hooks",
            ] {
                let mut arguments = json!({"project_id":project_id,"version":before.version});
                if name == "forge_update_project" {
                    arguments["settings"] = settings.clone();
                } else {
                    arguments["lifecycle_hooks"] = hooks.clone();
                }
                assert!(
                    dispatch_with_context(
                        &state,
                        grant,
                        "tools/call",
                        json!({"name":name,"arguments":arguments})
                    )
                    .await
                    .is_err(),
                    "{name}: a plain member holds no command authority (bound Project {:?})",
                    grant.project_id
                );
            }
        }
        assert_eq!(
            ProjectRepo::get_by_id(&*state.db, &project_id)
                .await
                .unwrap()
                .unwrap(),
            before,
            "nothing persisted on a member's denial"
        );
        sqlx::query("DELETE FROM project_member WHERE id = ?")
            .bind(&member_id)
            .execute(state.db.pool())
            .await
            .unwrap();
        ProjectMemberRepo::add_member(
            &*state.db,
            CreateProjectMember {
                id: new_uuid_v4(),
                project_id: project_id.clone(),
                user_id: "mcp-test-user".into(),
                role: "admin".into(),
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
        for name in [
            "forge_update_project",
            "forge_update_project_lifecycle_hooks",
        ] {
            let version = ProjectRepo::get_by_id(&*state.db, &project_id)
                .await
                .unwrap()
                .unwrap()
                .version;
            let mut arguments = json!({"project_id":project_id,"version":version});
            arguments[if name == "forge_update_project" {
                "settings"
            } else {
                "lifecycle_hooks"
            }] = if name == "forge_update_project" {
                settings.clone()
            } else {
                hooks.clone()
            };
            dispatch_with_context(
                &state,
                &context,
                "tools/call",
                json!({"name":name,"arguments":arguments}),
            )
            .await
            .unwrap();
        }
    });
}

#[test]
fn assignment_subtask_and_follow_up_agent_references_match_rest_usability_and_hide_foreign_ids() {
    run_async(async {
        let state = sqlite_state().await;
        seed_chat_account(&state).await;
        let task = seed_task(&state).await;
        let execution = seed_execution(&state, task.id.clone()).await;
        let agent = seed_agent(&state, "foreign-reference").await;
        sqlx::query(
            "UPDATE agent_identity SET owner_id='chat-user',visibility='account' WHERE id=?",
        )
        .bind(&agent.id)
        .execute(state.db.pool())
        .await
        .unwrap();
        assert!(!state
            .db
            .list_agents_usable_in_project(&task.project_id, "mcp-test-user")
            .await
            .unwrap()
            .iter()
            .any(|a| a.id == agent.id));
        for name in [
            "forge_assign_agent",
            "forge_create_sub_tasks",
            "forge_follow_up_execution",
        ] {
            let mut errors = Vec::new();
            for id in [agent.id.as_str(), "missing-agent"] {
                let arguments = match name {
                    "forge_assign_agent" => json!({"task_id":task.id,"agent_id":id}),
                    "forge_create_sub_tasks" => {
                        json!({"parent_task_id":task.id,"subtasks":[{"title":"Child","assignee_id":id}]})
                    }
                    _ => json!({"execution_id":execution,"message":"continue","agent_id":id}),
                };
                let error = call_tool_error(&state, name, arguments).await;
                assert_eq!(error.code, -32004, "{name}");
                errors.push(error.message.replace(id, "<id>"));
            }
            assert_eq!(
                errors[0], errors[1],
                "{name}: inaccessible and missing agent are indistinguishable"
            );
        }
        assert_eq!(
            TaskRepo::list_subtasks_ordered(&*state.db, &task.id)
                .await
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            TaskRoleAssignmentRepo::list_by_task(&*state.db, &task.id)
                .await
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            ExecutionRepo::list_by_task(
                &*state.db,
                &task.id,
                PageRequest {
                    cursor: None,
                    limit: 10,
                    include_total: false,
                    sort_by: SortBy::CreatedAt,
                    sort_order: SortOrder::Desc
                }
            )
            .await
            .unwrap()
            .items
            .len(),
            1
        );
        sqlx::query("UPDATE agent_identity SET owner_id='mcp-test-user' WHERE id=?")
            .bind(&agent.id)
            .execute(state.db.pool())
            .await
            .unwrap();
        assert!(state
            .db
            .list_agents_usable_in_project(&task.project_id, "mcp-test-user")
            .await
            .unwrap()
            .iter()
            .any(|a| a.id == agent.id));
        sqlx::query("UPDATE project SET owner_id='mcp-test-user' WHERE id=?")
            .bind(&task.project_id)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE execution SET status='completed' WHERE id=?")
            .bind(&execution)
            .execute(state.db.pool())
            .await
            .unwrap();
        call_tool(
            &state,
            "forge_assign_agent",
            json!({"task_id":task.id,"agent_id":agent.id}),
        )
        .await;
    });
}

#[test]
fn scoped_dependency_listing_never_names_a_foreign_id_loaded_from_storage() {
    run_async(async {
        let state = sqlite_state().await;
        let task = seed_task(&state).await;
        let foreign = seed_task(&state).await;
        TaskDependencyRepo::add_dependency(&*state.db, &task.id, &foreign.id, &now_rfc3339())
            .await
            .unwrap();
        let context = McpContext {
            user_id: Some("mcp-test-user".into()),
            project_id: Some(task.project_id.clone()),
        };
        let error = dispatch_with_context(
            &state,
            &context,
            "tools/call",
            json!({"name":"forge_list_task_dependencies","arguments":{"task_id":task.id}}),
        )
        .await
        .unwrap_err();
        let error = error.into_response(Value::Null).error.unwrap();
        assert_eq!(error.code, -32004);
        assert_eq!(error.message, format!("task not found: {}", task.id));
        assert!(!serde_json::to_string(&error).unwrap().contains(&foreign.id));
    });
}

#[test]
fn transition_and_get_project_actions_use_the_same_delegated_actor() {
    run_async(async {
        let state = sqlite_state().await;
        let task = seed_task(&state).await;
        let changed = call_tool(
            &state,
            "forge_transition_task",
            json!({"task_id":task.id,"status":"in_progress","version":task.version}),
        )
        .await;
        let read = call_tool(&state, "forge_get_task", json!({"task_id":task.id})).await;
        let actor =
            api_types::Actor::delegated_user("mcp-test-user", api_types::UserActionSource::Api);
        let snapshot = state
            .task_service
            .task_action_snapshot(&task.id, &actor)
            .await
            .unwrap();
        assert_eq!(
            changed["available_actions"],
            json!(services::available_actions(&snapshot))
        );
        assert_eq!(changed["available_actions"], read["available_actions"]);
    });
}
