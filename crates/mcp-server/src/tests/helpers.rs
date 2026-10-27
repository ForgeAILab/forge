//! Shared MCP state, account, project, task, agent and dispatch fixtures.

use super::*;

pub(super) fn run_async<T>(future: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime builds")
        .block_on(future)
}

pub(super) async fn sqlite_state() -> AppState {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    let state = AppState::new(Arc::new(SqliteDb::new(pool)), Arc::new(EventBus::new(16)));
    let now = now_rfc3339();
    UserRepo::create_user(
        &*state.db,
        &db::User {
            id: "mcp-test-user".to_owned(),
            email: "mcp-test@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("MCP fixture user");
    state
}

pub(super) async fn seed_chat_account(state: &AppState) -> (String, String, String) {
    let now = now_rfc3339();
    UserRepo::create_user(
        &*state.db,
        &db::User {
            id: "chat-user".to_owned(),
            email: "chat-user@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("chat user creates");

    let identity_id = new_uuid_v4();
    let profile_id = new_uuid_v4();
    AgentRepo::create_identity_with_profile(
        &*state.db,
        CreateAgentIdentity {
            id: identity_id.clone(),
            name: "MCP Chat Agent".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some("chat-user".to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: profile_id.clone(),
            identity_id: identity_id.clone(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("test".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("chat identity creates");

    state
        .agent_chat_service
        .set_main_binding(SetMainAgentBindingInput {
            actor_user_id: "chat-user".to_owned(),
            account_id: "chat-user".to_owned(),
            identity_id: identity_id.clone(),
            autonomy_policy_json: "{}".to_owned(),
            tool_policy_revision: "test".to_owned(),
            expected_version: None,
            replacement_reason: None,
        })
        .await
        .expect("main binding creates");
    (identity_id, profile_id, "chat-user".to_owned())
}

pub(super) async fn seed_chat_project(state: &AppState, identity_id: &str) -> String {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    ProjectRepo::create(
        &*state.db,
        CreateProject {
            id: project_id.clone(),
            name: "MCP Chat Project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some("chat-user".to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("chat project creates");
    let expected_version =
        ProjectAgentBindingRepo::get_active_project_binding(&*state.db, &project_id)
            .await
            .expect("project binding lookup")
            .map(|binding| binding.version);
    state
        .agent_chat_service
        .set_project_binding(SetProjectAgentBindingInput {
            actor_user_id: "chat-user".to_owned(),
            project_id: project_id.clone(),
            identity_id: Some(identity_id.to_owned()),
            state: "active".to_owned(),
            autonomy_policy_json: "{}".to_owned(),
            permission_ceiling_json: "{}".to_owned(),
            subscriptions_json: "[]".to_owned(),
            wake_budget: 1,
            expected_version,
            replacement_reason: None,
        })
        .await
        .expect("project binding creates");
    project_id
}

pub(super) async fn append_chat_agent_message(
    state: &AppState,
    chat_id: &str,
    identity_id: &str,
    profile_id: &str,
) -> db::AgentChatMessage {
    let now = now_rfc3339();
    AgentChatMessageRepo::append_agent_chat_message(
        &*state.db,
        CreateAgentChatMessage {
            id: new_uuid_v4(),
            chat_id: chat_id.to_owned(),
            sequence: 0,
            author_type: db::AgentChatMessageAuthorType::Agent,
            author_id: Some(identity_id.to_owned()),
            content: "typed usage response".to_owned(),
            content_guard_json: "{}".to_owned(),
            sensitivity: "public".to_owned(),
            status: db::AgentChatMessageStatus::Complete,
            outcome: Some("completed".to_owned()),
            model: Some("test-model".to_owned()),
            profile_id: Some(profile_id.to_owned()),
            session_id: None,
            context_manifest_id: None,
            // This value is intentionally populated to prove the public MCP
            // projection never forwards the retired raw field.
            token_usage_json: Some(
                serde_json::json!({
                    "input_tokens": 12,
                    "output_tokens": 3,
                    "cache_read_tokens": 5,
                    "cache_write_tokens": 0,
                })
                .to_string(),
            ),
            duration_ms: Some(25),
            error: None,
            correlation_id: new_uuid_v4(),
            causation_id: None,
            handoff_id: None,
            source_type: "native".to_owned(),
            source_id: Some(new_uuid_v4()),
            source_message_id: None,
            source_room_id: None,
            source_conversation_id: None,
            source_sequence: None,
            source_metadata_json: "{}".to_owned(),
            created_at: now,
        },
    )
    .await
    .expect("agent chat message creates")
}

pub(super) async fn seed_chat_usage_event(
    state: &AppState,
    message: &db::AgentChatMessage,
    identity_id: &str,
    profile_id: &str,
    owner_user_id: &str,
) {
    let now = now_rfc3339();
    let selection_id = new_uuid_v4();
    let candidate_key = "chat-test-candidate".to_owned();
    let selection = UsageLedgerRepo::create_pricing_selection(
        &*state.db,
        CreatePricingSelection {
            id: selection_id.clone(),
            owner_user_id: Some(owner_user_id.to_owned()),
            project_id: None,
            domain_kind: PricingDomainKind::Chat,
            surface: UsageSurface::MainChat,
            source_id: message.id.clone(),
            execution_id: None,
            task_id: None,
            candidate_key: Some(candidate_key.clone()),
            attempt_ordinal: 0,
            subject_id: None,
            subject_revision_id: None,
            subject_revision_digest: None,
            binding_id: None,
            rate_revision_id: None,
            catalog_snapshot_id: None,
            catalog_freshness: None,
            runtime_model: Some("test-model".to_owned()),
            admitted_provider_id: Some("test".to_owned()),
            admitted_model_id: Some("test-model".to_owned()),
            source_kind: None,
            provenance_kind: PricingAdmissionProvenanceKind::Runtime,
            selection_status: PricingSelectionStatus::Unpriced,
            selection_reason: Some(CostCoverageReasonCode::MissingRate),
            selection_digest: new_uuid_v4(),
            selected_at: now.clone(),
            created_at: now.clone(),
        },
    )
    .await
    .expect("chat pricing selection creates");
    let invocation = UsageLedgerRepo::create_usage_invocation(
        &*state.db,
        CreateUsageInvocation {
            id: new_uuid_v4(),
            owner_user_id: Some(owner_user_id.to_owned()),
            project_id: None,
            domain_kind: PricingDomainKind::Chat,
            surface: UsageSurface::MainChat,
            source_id: message.id.clone(),
            execution_id: None,
            task_id: None,
            domain_idempotency_key: new_uuid_v4(),
            candidate_key: Some(candidate_key.clone()),
            attempt_ordinal: 0,
            pricing_selection_id: selection.id.clone(),
            admitted_provider_id: Some("test".to_owned()),
            admitted_model_id: Some("test-model".to_owned()),
            admitted_runtime_model: Some("test-model".to_owned()),
            pricing_subject_id: None,
            pricing_subject_revision_id: None,
            subject_revision_digest: None,
            agent_id: Some(identity_id.to_owned()),
            profile_id: Some(profile_id.to_owned()),
            agent_name_snapshot: Some("MCP Chat Agent".to_owned()),
            project_name_snapshot: None,
            executor_type: Some("embedded".to_owned()),
            backend_kind: Some("native".to_owned()),
            provenance_kind: PricingAdmissionProvenanceKind::Runtime,
            admitted_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("chat usage invocation creates");
    let started = UsageLedgerRepo::start_usage_invocation(
        &*state.db,
        StartUsageInvocation {
            id: invocation.id.clone(),
            expected_version: invocation.version,
            started_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("chat usage invocation starts");
    let event = CreateUsageEvent {
        id: new_uuid_v4(),
        invocation_id: started.id.clone(),
        owner_user_id: Some(owner_user_id.to_owned()),
        project_id: None,
        surface: UsageSurface::MainChat,
        source_id: message.id.clone(),
        execution_id: None,
        task_id: None,
        event_idempotency_key: new_uuid_v4(),
        source_report_id: new_uuid_v4(),
        report_sequence: 0,
        report_mode: UsageEventReportMode::FinalSnapshot,
        provenance_kind: UsageEventProvenanceKind::RuntimeReport,
        legacy_source_table: None,
        legacy_source_id: None,
        legacy_provider_raw: None,
        legacy_provider_sqlite_type: None,
        legacy_provider_sql_literal: None,
        legacy_model_raw: None,
        legacy_model_sqlite_type: None,
        legacy_model_sql_literal: None,
        legacy_counter_values_json: "{}".to_owned(),
        legacy_cost_usd_raw: None,
        legacy_created_at_raw: None,
        legacy_project_owner_raw: None,
        legacy_invalid_usage: false,
        provider_id: Some("test".to_owned()),
        model_id: Some("test-model".to_owned()),
        runtime_model: Some("test-model".to_owned()),
        candidate_key: Some(candidate_key),
        attempt_ordinal: 0,
        agent_id: Some(identity_id.to_owned()),
        profile_id: Some(profile_id.to_owned()),
        agent_name_snapshot: Some("MCP Chat Agent".to_owned()),
        project_name_snapshot: None,
        executor_type: Some("embedded".to_owned()),
        pricing_subject_revision_id: None,
        subject_revision_digest: None,
        telemetry_state: UsageTelemetryState::Metered,
        input_tokens: Some(12),
        output_tokens: Some(3),
        cache_read_tokens: Some(5),
        cache_write_tokens: Some(0),
        context_tokens: Some(17),
        selected_tier: None,
        provider_reported_nano_usd: None,
        legacy_reported_cost_usd: None,
        estimated_nano_usd: None,
        cost_kind: UsageCostKind::None,
        rate_revision_id: None,
        catalog_snapshot_id: None,
        formula_revision: None,
        retrospective: false,
        coverage_reason_code: Some(CostCoverageReasonCode::MissingRate),
        occurred_at: now.clone(),
        created_at: now.clone(),
    };
    UsageLedgerRepo::settle_usage_invocations_with_events(
        &*state.db,
        vec![UsageLedgerSettlement {
            invocation_id: started.id,
            expected_version: started.version,
            telemetry_state: UsageTelemetryState::Metered,
            terminal_reason: None,
            settled_at: now.clone(),
            updated_at: now,
            events: vec![event],
        }],
    )
    .await
    .expect("chat usage settles");
}

pub(super) async fn seed_project_repo(state: &AppState) -> (String, String) {
    let now = now_rfc3339();
    let project = ProjectRepo::create(
        &*state.db,
        CreateProject {
            id: new_uuid_v4(),
            name: "Forge".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_string(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");
    let repo = RepoRepo::create(
        &*state.db,
        CreateRepo {
            id: new_uuid_v4(),
            project_id: project.id.clone(),
            name: "forge".to_owned(),
            local_path: None,
            remote_url: Some("https://example.com/forge.git".to_owned()),
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        &*state.db,
        UpdateProject {
            id: project.id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo.id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(&*state.db, &project.id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project primary repo updates");
    (project.id, repo.id)
}

pub(super) async fn seed_task(state: &AppState) -> Task {
    let (project_id, _repo_id) = seed_project_repo(state).await;
    seed_task_in_project(state, project_id).await
}

pub(super) async fn seed_task_in_project(state: &AppState, project_id: String) -> Task {
    state
        .task_service
        .create_task(
            project_id,
            "Ship MCP",
            Some("initial".to_owned()),
            None,
            Some(1),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates")
}

pub(super) async fn seed_execution(state: &AppState, task_id: String) -> String {
    let now = now_rfc3339();
    let execution_id = new_uuid_v4();
    ExecutionRepo::create(
        &*state.db,
        CreateExecution {
            id: execution_id.clone(),
            task_id,
            agent_id: None,
            role: "coder".to_owned(),
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
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("execution creates");
    execution_id
}

pub(super) async fn seed_agent_registration_deps(state: &AppState) -> (String, String) {
    let now = now_rfc3339();
    let daemon_id = new_uuid_v4();
    let host = DaemonRepo::upsert_by_machine_id(
        &*state.db,
        UpsertDaemon {
            max_concurrent_runs: None,
            id: daemon_id.clone(),
            machine_id: format!("machine-{daemon_id}"),
            hostname: "test-host".to_owned(),
            os: "linux".to_owned(),
            arch: "x86_64".to_owned(),
            agent_version: None,
            labels_json: "{}".to_owned(),
            status: DaemonStatus::Online,
            registration_token_hash: None,
            owner_id: None,
            visibility: "global".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("daemon creates");

    ("shell".to_owned(), host.id)
}

#[allow(dead_code)]
pub(super) async fn seed_agent(state: &AppState, name: &str) -> Agent {
    let now = now_rfc3339();
    let daemon_id = new_uuid_v4();
    let daemon = DaemonRepo::upsert_by_machine_id(
        &*state.db,
        UpsertDaemon {
            max_concurrent_runs: None,
            id: daemon_id.clone(),
            machine_id: format!("machine-{daemon_id}"),
            hostname: "test-host".to_owned(),
            os: "linux".to_owned(),
            arch: "x86_64".to_owned(),
            agent_version: None,
            labels_json: "{}".to_owned(),
            status: DaemonStatus::Online,
            registration_token_hash: None,
            owner_id: None,
            visibility: "global".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("daemon creates");

    DaemonRepo::update_report(
        &*state.db,
        UpdateDaemonReport {
            max_concurrent_runs: None,
            id: daemon.id.clone(),
            last_report_at: now.clone(),
            status: DaemonStatus::Online,
            detected_clis_json: r#"[{"kind":"shell","availability":"authenticated"}]"#.to_owned(),
            labels_json: None,
            updated_at: now.clone(),
        },
    )
    .await
    .expect("daemon report updates");

    AgentRepo::create(
        &*state.db,
        CreateAgent {
            id: new_uuid_v4(),
            name: name.to_owned(),
            description: None,
            executor_type: "shell".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: Some(daemon.id),
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            prompt_template: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("agent creates")
}

#[allow(dead_code)]
pub(super) async fn assign_coder(state: &AppState, task_id: &str, agent_id: &str) {
    let now = now_rfc3339();
    TaskRoleAssignmentRepo::assign(
        &*state.db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            role_name: "coder".to_owned(),
            assignee_type: Some(AssigneeKind::Agent),
            assignee_id: Some(agent_id.to_owned()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("coder assignment creates");
}

pub(super) async fn call_tool(state: &AppState, name: &str, arguments: Value) -> Value {
    let result = dispatch(
        state,
        "tools/call",
        json!({
            "name": name,
            "arguments": arguments,
        }),
    )
    .await
    .expect("tool succeeds");
    let text = result["content"][0]["text"]
        .as_str()
        .expect("tool result includes text content");
    serde_json::from_str(text).expect("tool text content is JSON")
}

pub(super) async fn call_tool_scoped(
    state: &AppState,
    project_id: &str,
    name: &str,
    arguments: Value,
) -> Value {
    let result = dispatch_with_context(
        state,
        &McpContext {
            project_id: Some(project_id.to_owned()),
            user_id: Some("mcp-test-user".to_owned()),
        },
        "tools/call",
        json!({
            "name": name,
            "arguments": arguments,
        }),
    )
    .await
    .expect("tool succeeds");
    let text = result["content"][0]["text"]
        .as_str()
        .expect("tool result includes text content");
    serde_json::from_str(text).expect("tool text content is JSON")
}

pub(super) async fn call_tool_error(
    state: &AppState,
    name: &str,
    arguments: Value,
) -> crate::protocol::McpError {
    let error = dispatch(
        state,
        "tools/call",
        json!({
            "name": name,
            "arguments": arguments,
        }),
    )
    .await
    .expect_err("tool call should fail");
    let response = error.into_response(json!(1));
    response.error.expect("error payload present")
}
