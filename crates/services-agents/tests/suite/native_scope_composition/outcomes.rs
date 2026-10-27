//! Tests for outcomes in native scope composition.

use super::*;
#[tokio::test]
async fn scope_composition_preserves_replay_approval_policy_version_and_idempotency_outcomes() {
    let fixture = fixture(true).await;
    let permissions = broad_permissions();
    let project = ScopeToolComposition::for_scope_with_permissions(
        AGENT_ID,
        fixture.project_scope.clone(),
        None,
        None,
        &permissions,
        Some(Arc::new(fixture.provider.clone())),
    )
    .expect("Project composition");

    let first = invoke_tool(
        &project,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        document_arguments(
            "composition-replay",
            "Replay document",
            "scope-composition-document",
        ),
        "composition-replay-1",
    )
    .await
    .expect("first document command");
    assert!(!first.is_error);
    assert_outcome_operation(&first, PROJECT_DOCUMENT_OPERATION);
    assert_eq!(first.value["code"], "ok");
    assert_eq!(first.value["replayed"], false);

    let replay = invoke_tool(
        &project,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        document_arguments(
            "composition-replay",
            "Replay document",
            "scope-composition-document",
        ),
        "composition-replay-2",
    )
    .await
    .expect("document replay");
    assert!(!replay.is_error);
    assert_eq!(replay.value["code"], "ok");
    assert_eq!(replay.value["replayed"], true);
    assert_eq!(replay.value["receipt_id"], first.value["receipt_id"]);

    let approval = invoke_tool(
        &project,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        release_arguments("composition-approval"),
        "composition-approval",
    )
    .await
    .expect("approval proposal");
    assert!(!approval.is_error);
    assert_eq!(approval.value["code"], "approval_required");
    assert_eq!(approval.value["status"], "approval_required");
    assert_eq!(
        approval.value["approval_target"]["operation"],
        PROJECT_RELEASE_OPERATION
    );

    sqlx::query(
        "UPDATE project_agent_binding SET permission_ceiling_json = ?
         WHERE project_id = ? AND state = 'active'",
    )
    .bind(r#"{"allowed":["read_project"]}"#)
    .bind(PROJECT_ID)
    .execute(fixture.db.pool())
    .await
    .expect("restrict Project policy");
    let denied = invoke_tool(
        &project,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        document_arguments(
            "composition-policy",
            "Policy document",
            "scope-composition-document",
        ),
        "composition-policy",
    )
    .await
    .expect("policy denial remains in-band");
    assert_structured_error(&denied, PROJECT_DOCUMENT_OPERATION, "policy_denied");

    // Restoring live policy does not lift an operation-wide denial in its
    // existing turn. Start a new turn composition for subsequent cases.
    sqlx::query(
        "UPDATE project_agent_binding SET permission_ceiling_json = ?
         WHERE project_id = ? AND state = 'active'",
    )
    .bind(broad_permission_json())
    .bind(PROJECT_ID)
    .execute(fixture.db.pool())
    .await
    .expect("restore Project policy");

    let still_denied = invoke_tool(
        &project,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        document_arguments(
            "composition-restored",
            "Restored policy",
            "another-document",
        ),
        "composition-restored",
    )
    .await
    .expect("same-turn denial remains in-band");
    assert_eq!(still_denied.value, denied.value);
    let project = ScopeToolComposition::for_scope_with_permissions(
        AGENT_ID,
        fixture.project_scope.clone(),
        None,
        None,
        &permissions,
        Some(Arc::new(fixture.provider.clone())),
    )
    .expect("next-turn Project composition");

    let stale = invoke_tool(
        &project,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        document_arguments(
            "composition-stale",
            "Stale document",
            "scope-composition-document",
        ),
        "composition-stale",
    )
    .await
    .expect("first stale-document write");
    assert!(!stale.is_error);
    let stale_retry = invoke_tool(
        &project,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        document_arguments(
            "composition-stale-retry",
            "Stale document",
            stale.value["result"]["domain_result"]["document_id"]
                .as_str()
                .expect("created document id"),
        ),
        "composition-stale-retry",
    )
    .await
    .expect("stale-document conflict");
    assert_structured_error(&stale_retry, PROJECT_DOCUMENT_OPERATION, "version_conflict");
    assert_eq!(stale_retry.value["retry"]["action"], "refresh_and_retry");
    assert!(stale_retry.value["current_version_or_revision"]["version"].is_number());

    let mismatch = invoke_tool(
        &project,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        document_arguments(
            "composition-idempotency",
            "Initial input",
            "scope-composition-idempotency-document",
        ),
        "composition-idempotency-1",
    )
    .await
    .expect("idempotency first command");
    assert!(!mismatch.is_error);
    let mismatch_input = document_arguments(
        "composition-idempotency",
        "Changed input",
        "scope-composition-idempotency-document",
    );
    let mismatch = invoke_tool(
        &project,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        mismatch_input,
        "composition-idempotency-2",
    )
    .await
    .expect("idempotency mismatch");
    assert_structured_error(
        &mismatch,
        PROJECT_DOCUMENT_OPERATION,
        "idempotency_conflict",
    );
    assert!(mismatch.value["current_version_or_revision"].is_null());
    assert_eq!(mismatch.value["retry"]["action"], "use_new_idempotency_key");
}

#[tokio::test]
async fn scope_composition_keeps_setup_not_found_and_internal_failures_structured() {
    let setup_fixture = fixture(false).await;
    let permissions = broad_permissions();
    let project = ScopeToolComposition::for_scope_with_permissions(
        AGENT_ID,
        setup_fixture.project_scope.clone(),
        None,
        None,
        &permissions,
        Some(Arc::new(setup_fixture.provider.clone())),
    )
    .expect("Project composition");
    let setup = invoke_tool(
        &project,
        "forge_scope_propose",
        task_arguments("composition-setup"),
        TASK_PROPOSE_OPERATION,
    )
    .await
    .expect("missing TaskService is a structured setup outcome");
    assert_structured_error(&setup, TASK_PROPOSE_OPERATION, "setup_required");
    assert!(setup.value["setup_requirements"].is_array());

    let not_found = ScopeToolComposition::for_scope_with_permissions(
        AGENT_ID,
        setup_fixture.main_scope.clone(),
        None,
        None,
        &permissions,
        Some(Arc::new(setup_fixture.provider.clone())),
    )
    .expect("Main composition");
    let not_found = invoke_tool(
        &not_found,
        FORGE_MAIN_ORCHESTRATION_READ_TOOL,
        json!({
            "operation": MAIN_CHARTER_READINESS_OPERATION,
            "arguments": {
                "charter_id": "missing-charter",
                "revision_id": MAIN_REVISION_ID,
                "content_digest": "main-content-1",
                "render_digest": "main-render-1",
                "expected_charter_version": 1,
                "genesis_session_id": MAIN_GENESIS_ID
            }
        }),
        "composition-not-found",
    )
    .await
    .expect("missing Charter is a structured not-found outcome");
    assert_structured_error(&not_found, MAIN_CHARTER_READINESS_OPERATION, "not_found");

    setup_fixture.db.pool().close().await;
    let internal = invoke_tool(
        &project,
        FORGE_PROJECT_ORCHESTRATION_READ_TOOL,
        json!({"operation": PROJECT_CURRENT_STATE_OPERATION, "arguments": {"limit": 10}}),
        "composition-internal",
    )
    .await
    .expect("persistence failure is a structured internal outcome");
    assert_structured_error(
        &internal,
        PROJECT_CURRENT_STATE_OPERATION,
        "internal_failure",
    );
    let rendered = internal.value.to_string();
    assert!(!rendered.contains("no such table"));
    assert!(!rendered.contains("sqlx"));
}

/// Reaching the public web is authority, so it is gated twice: the host must
/// supply a transport, and the scope must hold the same permission that gates
/// the search tool. `fetch` differs from search in needing no configured
/// endpoint, which would otherwise make it the one web surface that appears
/// without the owner deciding anything.
#[tokio::test]
async fn fetch_needs_both_a_host_transport_and_the_scopes_web_permission() {
    let fixture = fixture(false).await;
    let compose = |permissions: BTreeSet<String>, transport: bool| {
        ScopeToolComposition::for_scope_with_permissions_and_project_context(
            AGENT_ID,
            fixture.main_scope.clone(),
            None,
            None,
            &permissions,
            ProjectChatToolContext::default(),
            Some(Arc::new(fixture.provider.clone())),
            ScopeToolRuntime {
                environment: Default::default(),
                command_allowlist: None,
                fetch_transport: transport
                    .then(|| Arc::new(ForgeFetchTransport::new()) as Arc<dyn FetchTransport>),
            },
        )
        .expect("Main chat composition")
        .tools()
        .into_iter()
        .any(|tool| tool.spec().name == FETCH_TOOL_NAME)
    };

    assert!(
        compose(broad_permissions(), true),
        "a Main chat that may propose discovery reaches the public web"
    );
    assert!(
        !compose(broad_permissions(), false),
        "without a host transport there is nothing to compose"
    );
    let mut narrowed = broad_permissions();
    narrowed.remove("propose_discovery");
    assert!(
        !compose(narrowed, true),
        "a scope without the web permission must not receive fetch"
    );
}

/// A Task worker writes code against real dependencies, so it reads their
/// documentation through the same bounded tool — gated on the Task read
/// permission, because a Task that may not read its own record has no
/// business reaching the network either.
#[tokio::test]
async fn a_task_worker_reaches_documentation_only_with_its_read_permission() {
    let fixture = fixture(false).await;
    let compose = |permissions: BTreeSet<String>| {
        ScopeToolComposition::for_scope_with_permissions_and_project_context(
            AGENT_ID,
            CanonicalScope {
                scope_type: CanonicalScopeType::Task,
                scope_id: "scope-composition-task".to_owned(),
                workspace_access: WorkspaceAccess::TaskWrite,
            },
            Some("coder"),
            Some("/tmp/forge-scope-composition-worktree"),
            &permissions,
            ProjectChatToolContext::default(),
            Some(Arc::new(fixture.provider.clone())),
            ScopeToolRuntime {
                environment: Default::default(),
                command_allowlist: None,
                fetch_transport: Some(Arc::new(ForgeFetchTransport::new())),
            },
        )
        .expect("Task composition")
        .tools()
        .into_iter()
        .any(|tool| tool.spec().name == FETCH_TOOL_NAME)
    };

    let mut worker = broad_permissions();
    worker.insert("task_read".to_owned());
    worker.insert("task_write".to_owned());
    assert!(compose(worker.clone()), "a coder may read documentation");

    worker.remove("task_read");
    assert!(
        !compose(worker),
        "a Task without read authority does not get a network tool"
    );
}
