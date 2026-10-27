//! Tests for operation matrix in native scope composition.

use super::*;
#[tokio::test]
async fn scope_composition_drives_every_migrated_main_project_and_task_operation() {
    let fixture = fixture(true).await;
    let permissions = broad_permissions();
    let main = ScopeToolComposition::for_scope_with_permissions(
        AGENT_ID,
        fixture.main_scope.clone(),
        None,
        None,
        &permissions,
        Some(Arc::new(fixture.provider.clone())),
    )
    .expect("Main composition");
    let project = ScopeToolComposition::for_scope_with_permissions(
        AGENT_ID,
        fixture.project_scope.clone(),
        None,
        None,
        &permissions,
        Some(Arc::new(fixture.provider.clone())),
    )
    .expect("Project composition");
    let mut covered_operations = BTreeSet::new();

    let read_cases = [
        (
            MAIN_GENESIS_PROJECT_AGENTS_READ_OPERATION,
            json!({"genesis_session_id": MAIN_GENESIS_ID}),
        ),
        (
            MAIN_CHARTER_READ_OPERATION,
            json!({"charter_id": MAIN_CHARTER_ID, "genesis_session_id": MAIN_GENESIS_ID}),
        ),
        (
            MAIN_CHARTER_READINESS_OPERATION,
            json!({
                "charter_id": MAIN_CHARTER_ID,
                "revision_id": MAIN_REVISION_ID,
                "content_digest": "main-content-1",
                "render_digest": "main-render-1",
                "expected_charter_version": 1,
                "genesis_session_id": MAIN_GENESIS_ID
            }),
        ),
        (
            MAIN_CHARTER_DIFF_OPERATION,
            json!({
                "charter_id": MAIN_CHARTER_ID,
                "base_revision_id": MAIN_REVISION_ID,
                "candidate_revision_id": MAIN_REVISION_ID,
                "genesis_session_id": MAIN_GENESIS_ID
            }),
        ),
        (
            MAIN_CHARTER_APPROVAL_TARGET_OPERATION,
            json!({
                "charter_id": MAIN_CHARTER_ID,
                "revision_id": MAIN_REVISION_ID,
                "content_digest": "main-content-1",
                "render_digest": "main-render-1",
                "expected_charter_version": 1,
                "genesis_session_id": MAIN_GENESIS_ID
            }),
        ),
        (PROJECT_CURRENT_STATE_OPERATION, json!({"limit": 10})),
    ];
    for (operation, arguments) in read_cases {
        let target = if operation == PROJECT_CURRENT_STATE_OPERATION {
            &project
        } else {
            &main
        };
        let tool_name = if operation == PROJECT_CURRENT_STATE_OPERATION {
            FORGE_PROJECT_ORCHESTRATION_READ_TOOL
        } else {
            FORGE_MAIN_ORCHESTRATION_READ_TOOL
        };
        let outcome = invoke_tool(
            target,
            tool_name,
            json!({"operation": operation, "arguments": arguments}),
            operation,
        )
        .await
        .unwrap_or_else(|error| panic!("{operation} failed at composition boundary: {error:?}"));
        assert_outcome_operation(&outcome, operation);
        assert!(
            !outcome.is_error,
            "successful query outcome: {}",
            outcome.value
        );
        covered_operations.insert(operation.to_owned());
    }

    let main_proposals = [
        (
            MAIN_GENESIS_START_OPERATION,
            genesis_start_arguments("matrix-genesis-start"),
        ),
        (
            MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
            json!({
                "operation": MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
                "payload": {
                    "action": "select",
                    "genesis_session_id": MAIN_GENESIS_ID,
                    "expected_session_version": 1,
                    "project_agent_identity_id": PROJECT_AGENT_CANDIDATE_ID
                },
                "dedupe_key": "matrix-project-agent-select",
                "correlation_id": "correlation-matrix-project-agent-select"
            }),
        ),
        (
            MAIN_CHARTER_DRAFT_OPERATION,
            main_draft_arguments("matrix-main-draft"),
        ),
        (
            MAIN_PROJECT_CREATE_OPERATION,
            // A new Project-create call must name its Charter approval.
            json!({
                "operation": MAIN_PROJECT_CREATE_OPERATION,
                "payload": {"action": "create_from_approval", "approval_id": "matrix-approval"},
                "dedupe_key": "matrix-main-project",
                "correlation_id": "correlation-matrix-main-project"
            }),
        ),
    ];
    for (operation, arguments) in main_proposals {
        let outcome = invoke_tool(
            &main,
            FORGE_MAIN_ORCHESTRATION_PROPOSE_TOOL,
            arguments,
            operation,
        )
        .await
        .unwrap_or_else(|error| panic!("{operation} failed at composition boundary: {error:?}"));
        assert_outcome_operation(&outcome, operation);
        if operation == MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION {
            assert!(
                !outcome.is_error,
                "successful Main Project-Agent selection outcome: {}",
                outcome.value
            );
        }
        covered_operations.insert(operation.to_owned());
    }

    let ready_propose = project
        .tools()
        .into_iter()
        .find(|tool| tool.spec().name == FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL)
        .expect("ready Project proposal tool");
    let ready_spec = ready_propose.spec();
    let ready_operations = ready_spec.input_schema["properties"]["operation"]["enum"]
        .as_array()
        .expect("ready Project operation enum");
    assert!(
        ready_operations
            .iter()
            .any(|value| value == PROJECT_CHARTER_ADOPTION_OPERATION),
        "amendment drafting must be present in a ready Project scope"
    );

    let project_proposals = [
        (
            PROJECT_REVIEW_CONFIG_OPERATION,
            project_proposal_arguments(
                PROJECT_REVIEW_CONFIG_OPERATION,
                "set_ci_steps",
                "matrix-review-config",
            ),
        ),
        (
            PROJECT_DOCUMENT_OPERATION,
            document_arguments(
                "matrix-document",
                "Matrix document",
                "scope-composition-document",
            ),
        ),
        (
            PROJECT_DECISION_OPERATION,
            project_proposal_arguments(
                PROJECT_DECISION_OPERATION,
                "record_candidate",
                "matrix-decision",
            ),
        ),
        (
            PROJECT_MILESTONE_OPERATION,
            project_proposal_arguments(PROJECT_MILESTONE_OPERATION, "revise", "matrix-milestone"),
        ),
        (
            PROJECT_EVIDENCE_OPERATION,
            project_proposal_arguments(PROJECT_EVIDENCE_OPERATION, "attach", "matrix-evidence"),
        ),
        (
            PROJECT_VALIDATION_OPERATION,
            project_proposal_arguments(PROJECT_VALIDATION_OPERATION, "record", "matrix-validation"),
        ),
        (
            PROJECT_READINESS_OPERATION,
            project_proposal_arguments(PROJECT_READINESS_OPERATION, "evaluate", "matrix-readiness"),
        ),
        (
            PROJECT_RELEASE_OPERATION,
            release_arguments("matrix-release"),
        ),
    ];
    let migrated_inputs: Value = serde_json::from_str(include_str!(
        "../../../../operation-registry/tests/project_inputs.json"
    ))
    .unwrap();
    for (operation, mut arguments) in project_proposals {
        if operation_registry::project_proposals::IDS.contains(&operation)
            && operation != PROJECT_DOCUMENT_OPERATION
        {
            arguments["payload"] = migrated_inputs[operation].clone();
        }
        if operation == PROJECT_EVIDENCE_OPERATION {
            arguments["payload"] = json!({"action":"attach","milestone_id":"scope-composition-milestone","expected_milestone_version":1,"acceptance_check_ids":[],"caption":"Proof","kind":"report","asset_id":"missing-asset","checksum":"a".repeat(64)});
        }
        if operation == PROJECT_READINESS_OPERATION {
            arguments["payload"] = json!({"action":"evaluate","milestone_id":"scope-composition-milestone","milestone_version":1});
        }
        let outcome = invoke_tool(
            &project,
            FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
            arguments,
            operation,
        )
        .await
        .unwrap_or_else(|error| panic!("{operation} failed at composition boundary: {error:?}"));
        assert_outcome_operation(&outcome, operation);
        covered_operations.insert(operation.to_owned());
    }

    let task = invoke_tool(
        &project,
        "forge_scope_propose",
        task_arguments("matrix-task"),
        TASK_PROPOSE_OPERATION,
    )
    .await
    .expect("task.propose composition call");
    assert_outcome_operation(&task, TASK_PROPOSE_OPERATION);
    covered_operations.insert(TASK_PROPOSE_OPERATION.to_owned());
    assert!(
        !task.is_error,
        "Task proposal should commit: {}",
        task.value
    );

    let source_task_id = task.value["result"]["domain_result"]["task_id"]
        .as_str()
        .expect("task.propose source Task id");
    let source_task_version: i64 = sqlx::query_scalar("SELECT version FROM task WHERE id = ?")
        .bind(source_task_id)
        .fetch_one(fixture.db.pool())
        .await
        .expect("adaptive source Task version");
    let board_revision: i64 = sqlx::query_scalar("SELECT board_revision FROM project WHERE id = ?")
        .bind(PROJECT_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("adaptive board revision");
    let adaptive = invoke_tool(
        &project,
        "forge_scope_propose",
        adaptive_task_arguments(
            "matrix-task-adaptive",
            source_task_id,
            source_task_version,
            board_revision,
        ),
        TASK_ADAPTIVE_OPERATION,
    )
    .await
    .expect("task.adaptive composition call");
    assert_outcome_operation(&adaptive, TASK_ADAPTIVE_OPERATION);
    assert!(
        !adaptive.is_error,
        "adaptive Task should commit: {}",
        adaptive.value
    );
    covered_operations.insert(TASK_ADAPTIVE_OPERATION.to_owned());

    let cancellable = invoke_tool(
        &project,
        "forge_scope_propose",
        task_arguments("matrix-cancellable-task"),
        TASK_PROPOSE_OPERATION,
    )
    .await
    .expect("cancellable Task proposal");
    assert!(
        !cancellable.is_error,
        "Task proposal: {}",
        cancellable.value
    );
    let cancellable_task_id = cancellable.value["result"]["domain_result"]["task_id"]
        .as_str()
        .expect("cancellable Task id");
    let cancellable_task_version: i64 = sqlx::query_scalar("SELECT version FROM task WHERE id = ?")
        .bind(cancellable_task_id)
        .fetch_one(fixture.db.pool())
        .await
        .expect("cancellable Task version");
    // The graph command: point the cancellable Task at the Task proposed
    // earlier, then take the edge away again. Neither id changes, which is the
    // whole reason this operation exists.
    let edge_added = invoke_tool(
        &project,
        "forge_scope_propose",
        task_dependency_arguments(
            "matrix-task-dependency-add",
            "add",
            cancellable_task_id,
            source_task_id,
        ),
        TASK_DEPENDENCY_OPERATION,
    )
    .await
    .expect("task.dependency add composition call");
    assert_outcome_operation(&edge_added, TASK_DEPENDENCY_OPERATION);
    assert!(
        !edge_added.is_error,
        "adding a prerequisite edge should commit: {}",
        edge_added.value
    );
    let edge_removed = invoke_tool(
        &project,
        "forge_scope_propose",
        task_dependency_arguments(
            "matrix-task-dependency-remove",
            "remove",
            cancellable_task_id,
            source_task_id,
        ),
        TASK_DEPENDENCY_OPERATION,
    )
    .await
    .expect("task.dependency remove composition call");
    assert_outcome_operation(&edge_removed, TASK_DEPENDENCY_OPERATION);
    assert!(
        !edge_removed.is_error,
        "removing a prerequisite edge should commit: {}",
        edge_removed.value
    );
    covered_operations.insert(TASK_DEPENDENCY_OPERATION.to_owned());

    let cancelled = invoke_tool(
        &project,
        "forge_scope_propose",
        cancel_task_arguments(
            "matrix-task-cancel",
            cancellable_task_id,
            cancellable_task_version,
        ),
        TASK_ACTION_OPERATION,
    )
    .await
    .expect("task.action composition call");
    assert_outcome_operation(&cancelled, TASK_ACTION_OPERATION);
    assert!(
        !cancelled.is_error,
        "healthy Task cancellation should commit: {}",
        cancelled.value
    );
    assert_eq!(cancelled.value["result"]["task_status"], "cancelled");
    covered_operations.insert(TASK_ACTION_OPERATION.to_owned());

    let mut human_review_workflow = services::workflow::default_workflow::default_workflow();
    let review_state = human_review_workflow
        .states
        .iter_mut()
        .find(|state| state.name == "review")
        .expect("default review state");
    review_state
        .gate_config
        .as_mut()
        .expect("default review gate")
        .requires_user_approval = Some(true);
    let reject = review_state
        .triggers
        .get_mut(&WorkflowTrigger::Reject)
        .expect("default review rejection");
    reject.to = "cancelled".to_owned();
    reject.dispatch = None;
    sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
        .bind(serde_json::to_string(&human_review_workflow).expect("workflow serializes"))
        .bind(PROJECT_ID)
        .execute(fixture.db.pool())
        .await
        .expect("human-required workflow installs");
    sqlx::query(
        "UPDATE task SET status = 'review', version = version + 1, updated_at = ? WHERE id = ?",
    )
    .bind(db::now_rfc3339())
    .bind(source_task_id)
    .execute(fixture.db.pool())
    .await
    .expect("Task enters human-required review");
    let review_task_version: i64 = sqlx::query_scalar("SELECT version FROM task WHERE id = ?")
        .bind(source_task_id)
        .fetch_one(fixture.db.pool())
        .await
        .expect("review Task version");
    let review = invoke_tool(
        &project,
        "forge_scope_propose",
        json!({
            "operation": TASK_ACTION_OPERATION,
            "payload": {
                "task_id": source_task_id,
                "action": {"verb":"send_back","guidance":"Exercise the Project Agent human-review decision."},
                "version": review_task_version
            },
            "dedupe_key": "matrix-task-review",
            "correlation_id": "correlation-matrix-task-review"
        }),
        TASK_ACTION_OPERATION,
    )
    .await
    .expect("task.review composition call");
    assert_outcome_operation(&review, TASK_ACTION_OPERATION);
    assert!(
        !review.is_error,
        "Task review should commit: {}",
        review.value
    );
    assert_eq!(review.value["result"]["task_status"], "cancelled");
    covered_operations.insert(TASK_ACTION_OPERATION.to_owned());

    let setup = ScopeToolComposition::for_scope_with_permissions_and_project_context(
        AGENT_ID,
        fixture.project_scope.clone(),
        None,
        None,
        &permissions,
        ProjectChatToolContext {
            is_project_agent_chat: true,
            charter_setup_required: true,
        },
        Some(Arc::new(fixture.provider.clone())),
        ScopeToolRuntime::default(),
    )
    .expect("setup Project composition");
    let setup_propose = setup
        .tools()
        .into_iter()
        .find(|tool| tool.spec().name == FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL)
        .expect("setup adoption proposal tool");
    let setup_spec = setup_propose.spec();
    let setup_operations = setup_spec.input_schema["properties"]["operation"]["enum"]
        .as_array()
        .expect("setup operation enum");
    assert_eq!(
        setup_operations,
        &[json!(PROJECT_CHARTER_ADOPTION_OPERATION)]
    );
    let adoption = invoke_tool(
        &setup,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        adoption_arguments("matrix-adoption"),
        PROJECT_CHARTER_ADOPTION_OPERATION,
    )
    .await
    .unwrap_or_else(|error| {
        panic!("{PROJECT_CHARTER_ADOPTION_OPERATION} failed at composition boundary: {error:?}")
    });
    assert_outcome_operation(&adoption, PROJECT_CHARTER_ADOPTION_OPERATION);
    covered_operations.insert(PROJECT_CHARTER_ADOPTION_OPERATION.to_owned());

    // The remaining migrated contracts are asserted by composition exposure
    // rather than by invocation. `project.observations` and `task.action` are
    // Project-scoped; `task.plan`, `task.worklog`, and `task.evidence` are
    // Task-scoped and need a leased Task session this fixture does not build.
    // Exposure is the property this test is named for: every migrated contract
    // must be surfaced by scope composition in a scope that supports it.
    let project_read = project
        .tools()
        .into_iter()
        .find(|tool| tool.spec().name == "forge_project_orchestration_read")
        .expect("Project read tool");
    let project_read_operations = project_read.spec().input_schema["properties"]["operation"]
        ["enum"]
        .as_array()
        .expect("Project read operation enum")
        .clone();
    for operation in [
        PROJECT_OBSERVATIONS_OPERATION,
        PROJECT_CHARTER_READ_OPERATION,
        PROJECT_SKILL_SECTION_OPERATION,
        forge_agent_host::REPO_INTEGRATION_QUEUE_OPERATION,
    ] {
        assert!(
            project_read_operations
                .iter()
                .any(|value| value == operation),
            "Project read composition must expose {operation}"
        );
        covered_operations.insert(operation.to_owned());
    }

    let project_propose_operations = project
        .tools()
        .into_iter()
        .find(|tool| tool.spec().name == "forge_scope_propose")
        .expect("Project propose tool")
        .spec()
        .input_schema["properties"]["operation"]["enum"]
        .as_array()
        .expect("Project propose operation enum")
        .clone();
    assert!(
        project_propose_operations
            .iter()
            .any(|value| value == TASK_ACTION_OPERATION),
        "Project propose composition must expose {TASK_ACTION_OPERATION}"
    );
    covered_operations.insert(TASK_ACTION_OPERATION.to_owned());

    let task_composition = ScopeToolComposition::for_scope_with_permissions_and_project_context(
        AGENT_ID,
        CanonicalScope {
            scope_type: CanonicalScopeType::Task,
            scope_id: source_task_id.to_owned(),
            workspace_access: WorkspaceAccess::TaskWrite,
        },
        Some("coder"),
        Some("/tmp/forge-scope-composition-worktree"),
        &{
            let mut permissions = broad_permissions();
            // Task-scope evidence capture is admitted at read level; the
            // session's write authority is the bounded worktree tools.
            permissions.insert("task_read".to_owned());
            permissions
        },
        ProjectChatToolContext {
            is_project_agent_chat: false,
            charter_setup_required: false,
        },
        Some(Arc::new(fixture.provider.clone())),
        ScopeToolRuntime::default(),
    )
    .expect("Task composition");
    let task_tool_names = task_composition
        .tools()
        .into_iter()
        .map(|tool| tool.spec().name.clone())
        .collect::<Vec<_>>();
    let task_propose_operations = task_composition
        .tools()
        .into_iter()
        .find(|tool| tool.spec().name == "forge_scope_propose")
        .unwrap_or_else(|| panic!("Task propose tool among {task_tool_names:?}"))
        .spec()
        .input_schema["properties"]["operation"]["enum"]
        .as_array()
        .expect("Task propose operation enum")
        .clone();
    for operation in [
        TASK_PLAN_OPERATION,
        TASK_WORKLOG_OPERATION,
        TASK_EVIDENCE_OPERATION,
    ] {
        assert!(
            task_propose_operations
                .iter()
                .any(|value| value == operation),
            "Task propose composition must expose {operation}"
        );
        covered_operations.insert(operation.to_owned());
    }

    // The lead operations are offered only to a run that leads a
    // coordination root, never to this worker session. They are driven
    // through this same provider boundary, as a lead, in
    // `task_dispatcher::tests::lead_operations`.
    for operation in operation_registry::task_lead::IDS {
        assert!(
            !task_propose_operations
                .iter()
                .any(|value| value == operation),
            "a Task session that leads nothing must not be offered {operation}"
        );
        covered_operations.insert((*operation).to_owned());
    }

    // Dispatching an inquiry belongs to a Main *Chat*, not to the bare
    // Account scope above: the Account scope is what an inquiry sub-agent
    // itself runs under, and withholding the operation there is what caps
    // dispatch depth at one. So the coverage for it lives on its own
    // composition rather than on `main`.
    let main_chat_composition =
        ScopeToolComposition::for_scope_with_permissions_and_project_context(
            AGENT_ID,
            CanonicalScope {
                scope_type: CanonicalScopeType::AgentChat,
                scope_id: "main-chat-scope-composition".to_owned(),
                workspace_access: WorkspaceAccess::Deny,
            },
            None,
            None,
            &broad_permissions(),
            ProjectChatToolContext {
                is_project_agent_chat: false,
                charter_setup_required: false,
            },
            Some(Arc::new(fixture.provider.clone())),
            ScopeToolRuntime::default(),
        )
        .expect("Main Chat composition");
    let main_chat_reads = main_chat_composition
        .tools()
        .into_iter()
        .find(|tool| tool.spec().name == FORGE_MAIN_ORCHESTRATION_READ_TOOL)
        .expect("Main Chat orchestration read tool")
        .spec()
        .input_schema["properties"]["operation"]["enum"]
        .as_array()
        .expect("Main Chat read operation enum")
        .clone();
    assert!(
        main_chat_reads
            .iter()
            .any(|value| value == MAIN_INQUIRY_RUN_OPERATION),
        "Main Chat composition must expose {MAIN_INQUIRY_RUN_OPERATION}"
    );
    covered_operations.insert(MAIN_INQUIRY_RUN_OPERATION.to_owned());

    let escalation = invoke_tool(&project, FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        json!({"operation":"project.escalate","payload":{"need":"Free disk before recovery","task_ids":[]},"dedupe_key":"matrix-escalation","correlation_id":"matrix-escalation"}),"matrix-escalation").await.expect("native Project escalation");
    assert!(!escalation.is_error, "escalation: {}", escalation.value);
    assert_eq!(
        escalation.value["result"]["domain_result"]["need"],
        "Free disk before recovery"
    );
    let notifications: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM notification WHERE project_id=? AND event_type='project.escalated'",
    )
    .bind(PROJECT_ID)
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    assert_eq!(notifications, 1);
    covered_operations.insert("project.escalate".to_owned());

    let expected_operations = MIGRATED_OPERATION_CONTRACTS
        .iter()
        .map(|contract| contract.operation.to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(covered_operations, expected_operations);
}

#[tokio::test]
async fn project_agent_sets_replay_safe_default_ci_steps_without_erasing_review_policy() {
    let fixture = fixture(false).await;
    let project = ProjectRepo::get_by_id(&*fixture.db, PROJECT_ID)
        .await
        .expect("Project lookup")
        .expect("Project exists");
    let project = ProjectRepo::update_at_version(
        &*fixture.db,
        UpdateProject {
            id: PROJECT_ID.to_owned(),
            name: None,
            settings: Some(
                json!({
                    "retry_budgets": {"review": 5},
                    "default_review_config": {
                        "review_prompt": "Preserve this reviewer instruction.",
                        "requirement_ids": ["project-requirement"]
                    }
                })
                .to_string(),
            ),
            primary_repo_id: None,
            paused_at: None,
            updated_at: db::now_rfc3339(),
        },
        project.version,
        None,
    )
    .await
    .expect("seed existing Project review policy");
    let expected_project_version = project.version;
    let composition = ScopeToolComposition::for_scope_with_permissions(
        AGENT_ID,
        fixture.project_scope.clone(),
        None,
        None,
        &broad_permissions(),
        Some(Arc::new(fixture.provider.clone())),
    )
    .expect("Project composition");
    let mut arguments = review_config_arguments(
        "project-review-config",
        expected_project_version,
        &[
            "cargo test --workspace",
            "cargo clippy --workspace --all-targets -- -D warnings",
        ],
    );
    arguments["payload"]["setup_steps"] = json!(["cargo fetch --locked"]);

    let first = invoke_tool(
        &composition,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        arguments.clone(),
        "project-review-config-first",
    )
    .await
    .expect("Project review config command");
    assert!(!first.is_error, "review config outcome: {}", first.value);
    assert_eq!(
        first.value["result"]["domain_result"]["project_version"],
        expected_project_version + 1
    );
    assert_eq!(
        first.value["result"]["domain_result"]["ci_steps"],
        json!([
            "cargo test --workspace",
            "cargo clippy --workspace --all-targets -- -D warnings"
        ])
    );
    assert_eq!(
        first.value["result"]["domain_result"]["setup_steps"],
        json!(["cargo fetch --locked"])
    );

    let updated = ProjectRepo::get_by_id(&*fixture.db, PROJECT_ID)
        .await
        .expect("updated Project lookup")
        .expect("updated Project exists");
    assert_eq!(updated.version, expected_project_version + 1);
    let settings: Value = serde_json::from_str(&updated.settings).expect("Project settings JSON");
    assert_eq!(settings["retry_budgets"]["review"], 5);
    assert_eq!(
        settings["default_review_config"]["review_prompt"],
        "Preserve this reviewer instruction."
    );
    assert_eq!(
        settings["default_review_config"]["requirement_ids"],
        json!(["project-requirement"])
    );
    assert_eq!(
        settings["default_review_config"]["ci_steps"],
        json!([
            "cargo test --workspace",
            "cargo clippy --workspace --all-targets -- -D warnings"
        ])
    );
    assert_eq!(
        settings["default_review_config"]["setup_steps"],
        json!(["cargo fetch --locked"])
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_action WHERE operation = 'project.review_config'"
        )
        .fetch_one(fixture.db.pool())
        .await
        .expect("AgentAction count"),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt WHERE operation = 'project.review_config'"
        )
        .fetch_one(fixture.db.pool())
        .await
        .expect("command receipt count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT event_type FROM domain_event
             WHERE entity_id = ? AND event_type = 'project.review_config.updated'"
        )
        .bind(PROJECT_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("review config event"),
        "project.review_config.updated"
    );

    let replay = invoke_tool(
        &composition,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        arguments.clone(),
        "project-review-config-replay",
    )
    .await
    .expect("Project review config replay");
    assert!(!replay.is_error, "replay outcome: {}", replay.value);
    assert_eq!(replay.value["result"]["replayed"], true);
    assert_eq!(
        replay.value["result"]["receipt_id"],
        first.value["result"]["receipt_id"]
    );
    assert_eq!(
        ProjectRepo::get_by_id(&*fixture.db, PROJECT_ID)
            .await
            .expect("replayed Project lookup")
            .expect("replayed Project exists")
            .version,
        expected_project_version + 1
    );

    let stale = invoke_tool(
        &composition,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        review_config_arguments(
            "project-review-config-stale",
            expected_project_version,
            &["cargo test --workspace"],
        ),
        "project-review-config-stale",
    )
    .await
    .expect("stale Project review config outcome");
    assert_structured_error(&stale, PROJECT_REVIEW_CONFIG_OPERATION, "version_conflict");
    assert_eq!(
        stale.value["retry"]["arguments"]["expected_project_version"],
        expected_project_version + 1
    );

    let mismatch = invoke_tool(
        &composition,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
        review_config_arguments(
            "project-review-config",
            expected_project_version,
            &["cargo test -p services"],
        ),
        "project-review-config-mismatch",
    )
    .await
    .expect("Project review config idempotency outcome");
    assert_structured_error(
        &mismatch,
        PROJECT_REVIEW_CONFIG_OPERATION,
        "idempotency_conflict",
    );

    let current = invoke_tool(
        &composition,
        FORGE_PROJECT_ORCHESTRATION_READ_TOOL,
        json!({"operation": PROJECT_CURRENT_STATE_OPERATION, "arguments": {"limit": 10}}),
        "project-review-config-current-state",
    )
    .await
    .expect("Project current state");
    assert!(
        !current.is_error,
        "current-state outcome: {}",
        current.value
    );
    assert_eq!(
        current.value["result"]["effective_state"]["project"]["default_review_ci_steps"],
        json!([
            "cargo test --workspace",
            "cargo clippy --workspace --all-targets -- -D warnings"
        ])
    );
    assert_eq!(
        current.value["result"]["effective_state"]["project"]["default_review_setup_steps"],
        json!(["cargo fetch --locked"])
    );
}

#[tokio::test]
async fn project_task_cancel_is_scoped_versioned_and_outcome_idempotent() {
    let fixture = fixture(true).await;
    let project = ScopeToolComposition::for_scope_with_permissions(
        AGENT_ID,
        fixture.project_scope.clone(),
        None,
        None,
        &broad_permissions(),
        Some(Arc::new(fixture.provider.clone())),
    )
    .expect("Project composition");
    let proposed = invoke_tool(
        &project,
        "forge_scope_propose",
        task_arguments("cancel-versioned-task"),
        TASK_PROPOSE_OPERATION,
    )
    .await
    .expect("Task proposal");
    assert!(!proposed.is_error, "Task proposal: {}", proposed.value);
    let task_id = proposed.value["result"]["domain_result"]["task_id"]
        .as_str()
        .expect("Task id")
        .to_owned();
    let original_version: i64 = sqlx::query_scalar("SELECT version FROM task WHERE id = ?")
        .bind(&task_id)
        .fetch_one(fixture.db.pool())
        .await
        .expect("Task version");
    sqlx::query(
        "UPDATE task SET priority = priority + 1, version = version + 1, updated_at = ? WHERE id = ?",
    )
    .bind(db::now_rfc3339())
    .bind(&task_id)
    .execute(fixture.db.pool())
    .await
    .expect("concurrent Task update");
    let current_version = original_version + 1;

    let stale = invoke_tool(
        &project,
        "forge_scope_propose",
        cancel_task_arguments("cancel-stale", &task_id, original_version),
        TASK_ACTION_OPERATION,
    )
    .await
    .expect("stale cancellation is structured");
    assert_structured_error(&stale, TASK_ACTION_OPERATION, "version_conflict");
    assert_eq!(
        stale.value["current_version_or_revision"]["resource_type"],
        "task"
    );
    assert_eq!(
        stale.value["current_version_or_revision"]["version"],
        current_version
    );
    assert_eq!(
        stale.value["retry"]["arguments"]["version"],
        current_version
    );

    let other_project_id = "scope-composition-other-project";
    ProjectRepo::create(
        &*fixture.db,
        CreateProject {
            id: other_project_id.to_owned(),
            name: "Other Project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(USER_ID.to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("other Project");
    let other_task =
        TaskService::new_for_test(Arc::clone(&fixture.db), Arc::new(EventBus::new(16)))
            .create_task(
                other_project_id,
                "Other Project Task",
                None,
                None,
                None,
                Some("planning_task".to_owned()),
                None,
                None,
                None,
            )
            .await
            .expect("other Project Task");
    let cross_project = invoke_tool(
        &project,
        "forge_scope_propose",
        cancel_task_arguments("cancel-cross-project", &other_task.id, other_task.version),
        TASK_ACTION_OPERATION,
    )
    .await
    .expect("cross-Project cancellation is structured");
    assert_structured_error(&cross_project, TASK_ACTION_OPERATION, "validation_error");

    let cancelled = invoke_tool(
        &project,
        "forge_scope_propose",
        cancel_task_arguments("cancel-current", &task_id, current_version),
        TASK_ACTION_OPERATION,
    )
    .await
    .expect("current cancellation");
    assert!(!cancelled.is_error, "cancellation: {}", cancelled.value);
    assert_eq!(cancelled.value["result"]["task_status"], "cancelled");

    let retry = invoke_tool(
        &project,
        "forge_scope_propose",
        cancel_task_arguments("cancel-current", &task_id, current_version),
        TASK_ACTION_OPERATION,
    )
    .await
    .expect("response-loss retry");
    // An exact Task action version is still required after response loss. The
    // cancelled outcome remains unchanged, and its fresh offer set is empty.
    assert_structured_error(&retry, TASK_ACTION_OPERATION, "version_conflict");
    let cancelled_task = db::TaskRepo::get_by_id(&*fixture.db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cancelled_task.status, "cancelled");
    assert_eq!(cancelled_task.version, current_version + 1);
    let terminal = invoke_tool(
        &project,
        "forge_scope_propose",
        cancel_task_arguments("cancel-terminal", &task_id, cancelled_task.version),
        TASK_ACTION_OPERATION,
    )
    .await
    .unwrap();
    assert_structured_error(&terminal, TASK_ACTION_OPERATION, "action_unavailable");
    assert_eq!(terminal.value["details"]["available_actions"], json!([]));
    let unchanged = db::TaskRepo::get_by_id(&*fixture.db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unchanged.version, cancelled_task.version);
    assert_eq!(unchanged.updated_at, cancelled_task.updated_at);
}
