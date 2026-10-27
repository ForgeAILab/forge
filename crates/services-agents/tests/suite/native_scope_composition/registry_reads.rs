//! Tests for registry reads in native scope composition.

use super::*;
#[tokio::test]
async fn registry_reads_use_real_handlers_and_unmoved_reads_keep_the_hand_path() {
    let fixture = fixture(false).await;
    let permissions = broad_permissions();
    for (scope, operation, input, tool_name, expected_field, expected_value) in [
        (
            fixture.main_scope.clone(),
            "account.summary",
            json!({}),
            "forge_scope_read",
            "id",
            AGENT_ID,
        ),
        (
            CanonicalScope {
                scope_type: CanonicalScopeType::AgentChat,
                scope_id: MAIN_CHAT_ID.to_owned(),
                workspace_access: WorkspaceAccess::Deny,
            },
            "agent_chat.summary",
            json!({}),
            "forge_scope_read",
            "id",
            MAIN_CHAT_ID,
        ),
        (
            fixture.project_scope.clone(),
            "project.charter",
            json!({}),
            FORGE_PROJECT_ORCHESTRATION_READ_TOOL,
            "charter_id",
            PROJECT_CHARTER_ID,
        ),
        (
            fixture.project_scope.clone(),
            "skill.section",
            json!({"section":"research"}),
            FORGE_PROJECT_ORCHESTRATION_READ_TOOL,
            "section",
            "research",
        ),
        (
            fixture.project_scope.clone(),
            "repo.integration_queue",
            json!({"target_branch":"main","limit":5}),
            FORGE_PROJECT_ORCHESTRATION_READ_TOOL,
            "project_id",
            PROJECT_ID,
        ),
    ] {
        assert!(operation_registry::READ_CATALOG.lookup(operation).is_some());
        let composition = ScopeToolComposition::for_scope_with_permissions(
            AGENT_ID,
            scope,
            None,
            None,
            &permissions,
            Some(Arc::new(fixture.provider.clone())),
        )
        .unwrap();
        for arguments in [
            json!({"operation":operation,"arguments":input}),
            json!({"parameters":{"operation":operation,"arguments":input}}),
        ] {
            let outcome = invoke_tool(&composition, tool_name, arguments, operation)
                .await
                .unwrap();
            assert!(!outcome.is_error, "{operation}: {}", outcome.value);
            let result = if matches!(
                operation,
                "project.charter" | "skill.section" | "repo.integration_queue"
            ) {
                &outcome.value["result"]
            } else {
                &outcome.value
            };
            assert_eq!(
                result[expected_field], expected_value,
                "{operation}: {}",
                outcome.value
            );
        }
    }
    // A contract violation is refused before any handler runs, as a tool
    // error naming the operation and the field; the provider enforces the
    // same contract if it is reached directly.
    let composition = ScopeToolComposition::for_scope_with_permissions(
        AGENT_ID,
        fixture.project_scope.clone(),
        None,
        None,
        &permissions,
        Some(Arc::new(fixture.provider.clone())),
    )
    .unwrap();
    for (arguments, expected) in [
        (
            json!({"operation":"skill.section"}),
            "skill.section: argument `section` is required",
        ),
        (
            json!({"parameters":{"operation":"project.charter","arguments":{"limit":1}}}),
            "project.charter: argument `limit` is not admitted",
        ),
    ] {
        let error = invoke_tool(
            &composition,
            FORGE_PROJECT_ORCHESTRATION_READ_TOOL,
            arguments,
            "violation",
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains(expected), "{error}");
    }
    let direct = forge_agent_host::ForgeToolProvider::read(
        &fixture.provider,
        AGENT_ID,
        &fixture.project_scope,
        "skill.section",
        json!({"section":"everything"}),
    )
    .await
    .unwrap_err();
    assert!(
        format!("{direct:?}").contains("skill.section: argument `section` must be one of"),
        "{direct:?}"
    );

    // `inbox.read` moved onto the registry with the other scope-bound
    // coordination reads; a Main Chat reads it through the same tool.
    assert!(operation_registry::READ_CATALOG
        .lookup("inbox.read")
        .is_some());
    let composition = ScopeToolComposition::for_scope_with_permissions(
        AGENT_ID,
        fixture.main_scope,
        None,
        None,
        &permissions,
        Some(Arc::new(fixture.provider)),
    )
    .unwrap();
    let outcome = invoke_tool(
        &composition,
        "forge_scope_read",
        json!({"operation":"inbox.read","arguments":{"limit":1}}),
        "unmoved",
    )
    .await
    .unwrap();
    assert!(!outcome.is_error);
    assert!(outcome.value["items"].is_array());
}

#[tokio::test]
async fn main_registry_reads_preserve_payloads_and_scope_on_each_surface() {
    use forge_agent_host::ForgeToolProvider;
    let fixture = fixture(false).await;
    let runner = Arc::new(RegistryInquiryRunner::default());
    fixture.provider.set_inquiry_runner(runner.clone());
    for scope in [
        fixture.main_scope.clone(),
        CanonicalScope {
            scope_type: CanonicalScopeType::AgentChat,
            scope_id: MAIN_CHAT_ID.to_owned(),
            workspace_access: WorkspaceAccess::Deny,
        },
    ] {
        let composition = ScopeToolComposition::for_scope_with_permissions(
            AGENT_ID,
            scope.clone(),
            None,
            None,
            &broad_permissions(),
            Some(Arc::new(fixture.provider.clone())),
        )
        .unwrap();
        for id in operation_registry::main_reads::IDS {
            if *id == "inquiry.run" && scope.scope_type == CanonicalScopeType::Account {
                continue;
            }
            let input = main_registered_input(id);
            let tool_name = operation_registry::READ_CATALOG
                .lookup(id)
                .unwrap()
                .surfaces[0]
                .native_aggregate;
            let direct = fixture
                .provider
                .read(AGENT_ID, &scope, id, input.clone())
                .await
                .unwrap();
            // The only two values excluded from equality: the fresh server
            // correlation id each read mints for its outcome envelope, and a
            // readiness evaluation's wall-clock timestamp. Everything else in
            // the envelope and the domain payload must be equal.
            let comparable = |mut outcome: Value| {
                if outcome.get("result").is_some() {
                    assert!(outcome["correlation_id"].as_str().is_some(), "{id}");
                    outcome.as_object_mut().unwrap().remove("correlation_id");
                }
                let payload = if outcome.get("result").is_some() {
                    &mut outcome["result"]
                } else {
                    &mut outcome
                };
                if let Some(readiness) = payload.get_mut("readiness") {
                    assert!(readiness["evaluated_at"].as_str().is_some(), "{id}");
                    readiness.as_object_mut().unwrap().remove("evaluated_at");
                }
                outcome
            };
            // The domain payload inside the outcome envelope, where the
            // operation has one.
            let payload = |outcome: &Value| match outcome.get("result") {
                Some(result) => result.clone(),
                None => outcome.clone(),
            };
            // Call the same domain handlers the base dispatcher selected,
            // independently of the registry's handler binding.
            let queries = services::MainOrchestrationQueryService::new(fixture.db.clone());
            let reference = match *id {
                "genesis.project_agents.read" => Some(
                    queries
                        .project_agents(
                            AGENT_ID,
                            &scope,
                            serde_json::from_value(input.clone()).unwrap(),
                        )
                        .await
                        .unwrap(),
                ),
                "charter.read" => Some(
                    queries
                        .charter_read(
                            AGENT_ID,
                            &scope,
                            serde_json::from_value(input.clone()).unwrap(),
                        )
                        .await
                        .unwrap(),
                ),
                "charter.readiness" => Some(
                    queries
                        .charter_readiness(
                            AGENT_ID,
                            &scope,
                            serde_json::from_value(input.clone()).unwrap(),
                        )
                        .await
                        .unwrap(),
                ),
                "charter.diff" => Some(
                    queries
                        .charter_diff(
                            AGENT_ID,
                            &scope,
                            serde_json::from_value(input.clone()).unwrap(),
                        )
                        .await
                        .unwrap(),
                ),
                "charter.approval_target" => Some(
                    queries
                        .charter_approval_target(
                            AGENT_ID,
                            &scope,
                            serde_json::from_value(input.clone()).unwrap(),
                        )
                        .await
                        .unwrap(),
                ),
                "discovery.read" => {
                    assert_eq!(direct["items"][0]["id"], MAIN_GENESIS_ID);
                    None
                }
                "portfolio.read" => {
                    assert_eq!(direct["items"][0]["id"], PROJECT_ID);
                    None
                }
                "inquiry.run" => None,
                _ => unreachable!(),
            };
            if let Some(reference) = reference {
                assert_eq!(
                    comparable(payload(&direct)),
                    comparable(reference),
                    "{id} handler parity"
                );
            }
            for raw in [
                json!({"operation":id,"arguments":input}),
                json!({"parameters":{"operation":id,"arguments":input}}),
            ] {
                let outcome = invoke_tool(&composition, tool_name, raw, "main-registry")
                    .await
                    .unwrap();
                assert!(!outcome.is_error, "{id}: {}", outcome.value);
                assert_eq!(
                    comparable(outcome.value),
                    comparable(direct.clone()),
                    "{id}"
                );
            }
            for denied_scope in [
                fixture.project_scope.clone(),
                CanonicalScope {
                    scope_type: CanonicalScopeType::Task,
                    scope_id: "foreign-task".into(),
                    workspace_access: WorkspaceAccess::Deny,
                },
            ] {
                assert!(
                    fixture
                        .provider
                        .read(AGENT_ID, &denied_scope, id, input.clone())
                        .await
                        .is_err(),
                    "{id}"
                );
            }
            // Same owner, but no active Main binding for this identity. The
            // denial is the same whether or not the arguments satisfy the
            // contract: a denied caller is told nothing about it.
            let denial = |error: forge_agent_host::AgentHostError| match error {
                forge_agent_host::AgentHostError::StructuredOutcome(outcome) => {
                    let mut outcome = serde_json::to_value(&*outcome).unwrap();
                    outcome.as_object_mut().unwrap().remove("correlation_id");
                    outcome.to_string()
                }
                other => format!("{other:?}"),
            };
            let denied = denial(
                fixture
                    .provider
                    .read(PROJECT_AGENT_CANDIDATE_ID, &scope, id, input.clone())
                    .await
                    .unwrap_err(),
            );
            for malformed in [json!({"foreign_project":"x"}), json!(null), json!([])] {
                let probed = denial(
                    fixture
                        .provider
                        .read(PROJECT_AGENT_CANDIDATE_ID, &scope, id, malformed)
                        .await
                        .unwrap_err(),
                );
                assert!(
                    !probed.contains("expected") && !probed.contains("foreign_project"),
                    "{id}: {probed}"
                );
                assert_eq!(probed, denied, "{id}");
            }
            let error = invoke_tool(
                &composition,
                tool_name,
                json!({"operation":id,"arguments":{"foreign_project":"x"}}),
                "invalid-registry",
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(
                error.contains(id)
                    && error.contains("foreign_project")
                    && error.contains("expected"),
                "{error}"
            );
            let error = fixture
                .provider
                .read(AGENT_ID, &scope, id, json!({"foreign_project":"x"}))
                .await
                .unwrap_err();
            let error = format!("{error:?}");
            assert!(
                error.contains(id)
                    && error.contains("foreign_project")
                    && error.contains("expected"),
                "{error}"
            );
        }
    }
    // An integer sent as a string or a float reaches the real handler as the
    // integer: one row, as `limit: 1` returns, not the default page.
    for id in ["discovery.read", "portfolio.read"] {
        let page = |limit: Value| {
            let provider = fixture.provider.clone();
            let scope = fixture.main_scope.clone();
            async move {
                provider
                    .read(AGENT_ID, &scope, id, json!({"limit":limit}))
                    .await
            }
        };
        let one = page(json!(1)).await.unwrap();
        assert_eq!(one["items"].as_array().unwrap().len(), 1, "{id}");
        for spelling in [json!("1"), json!(1.0)] {
            assert_eq!(page(spelling).await.unwrap(), one, "{id}");
        }
        for malformed in [json!("one"), json!(1.5), json!(-1), json!(true)] {
            let error = format!("{:?}", page(malformed).await.unwrap_err());
            assert!(
                error.contains(id) && error.contains("limit") && error.contains("expected"),
                "{error}"
            );
        }
        // Clamping is the handler's: 0 and 1000 are admitted.
        for clamped in [json!(0), json!(1000), json!(null)] {
            page(clamped).await.unwrap();
        }
    }
    {
        let requests = runner.0.lock().unwrap();
        assert_eq!(requests.len(), 3);
        for request in requests.iter() {
            assert_eq!(request.chat_id, MAIN_CHAT_ID);
            assert_eq!(request.identity_id, AGENT_ID);
            assert_eq!(request.account_id, USER_ID);
            assert_eq!(request.title, "Question");
            assert_eq!(request.question, "Find the answer");
            assert_eq!(request.context.as_deref(), Some("Supporting material"));
        }
    }
    assert_eq!(
        fixture
            .provider
            .read(
                AGENT_ID,
                &CanonicalScope {
                    scope_type: CanonicalScopeType::AgentChat,
                    scope_id: MAIN_CHAT_ID.into(),
                    workspace_access: WorkspaceAccess::Deny
                },
                "inquiry.run",
                main_registered_input("inquiry.run")
            )
            .await
            .unwrap()["result"],
        json!({
            "inquiry_id":"registry-inquiry","status":"succeeded","findings":"Bounded findings",
            "findings_path":"inquiries/registry-inquiry/findings.md","duration_ms":15,
            "token_usage":{"input_tokens":11,"output_tokens":12,"cache_read_tokens":13,"cache_write_tokens":14}
        })
    );
}
