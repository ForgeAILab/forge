use std::{future::Future, sync::Arc};

use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, Agent, AgentChatMessageListQuery,
    AgentChatMessageRepo, AgentChatRepo, AgentChatTurnJobRepo, AgentHandoffRepo, AgentRepo,
    AgentStatus, AssigneeKind, CostCoverageReasonCode, CreateAgent, CreateAgentChatMessage,
    CreateAgentIdentity, CreateAgentProfile, CreateExecution, CreatePricingSelection,
    CreateProject, CreateProjectMember, CreateRepo, CreateTask, CreateTaskRoleAssignment,
    CreateUsageEvent, CreateUsageInvocation, DaemonRepo, DaemonStatus, ExecutionRepo,
    ExecutionStatus, PageRequest, PricingAdmissionProvenanceKind, PricingDomainKind,
    PricingSelectionStatus, ProjectAgentBindingRepo, ProjectMemberRepo, ProjectRepo, RepoRepo,
    SortBy, SortOrder, SqliteDb, StartUsageInvocation, Task, TaskRepo, TaskRoleAssignmentRepo,
    UpdateProject, UpsertDaemon, UsageCostKind, UsageEventProvenanceKind, UsageEventReportMode,
    UsageLedgerRepo, UsageLedgerSettlement, UsageSurface, UsageTelemetryState, UserRepo,
};
use events::EventBus;
use serde_json::{json, Value};
use services::{SetMainAgentBindingInput, SetProjectAgentBindingInput};

use crate::{
    error::McpToolError,
    protocol::McpContext,
    rpc::{dispatch, dispatch_with_context},
    AppState,
};

#[path = "tests/project_contract.rs"]
mod project_contract;

fn run_async<T>(future: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime builds")
        .block_on(future)
}

async fn sqlite_state() -> AppState {
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

async fn seed_chat_account(state: &AppState) -> (String, String, String) {
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

async fn seed_chat_project(state: &AppState, identity_id: &str) -> String {
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
    ProjectMemberRepo::add_member(
        &*state.db,
        CreateProjectMember {
            id: new_uuid_v4(),
            project_id: project_id.clone(),
            user_id: "chat-user".to_owned(),
            role: "owner".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("chat project member creates");
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

async fn append_chat_agent_message(
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

async fn seed_chat_usage_event(
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

async fn seed_project_repo(state: &AppState) -> (String, String) {
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
            remote_url: "https://example.com/forge.git".to_owned(),
            work_mode: db::WorkMode::DirectMerge,
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

async fn seed_task(state: &AppState) -> Task {
    let (project_id, _repo_id) = seed_project_repo(state).await;
    seed_task_in_project(state, project_id).await
}

async fn seed_task_in_project(state: &AppState, project_id: String) -> Task {
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

async fn seed_execution(state: &AppState, task_id: String) -> String {
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

async fn seed_agent_registration_deps(state: &AppState) -> (String, String) {
    let now = now_rfc3339();
    let daemon_id = new_uuid_v4();
    let host = DaemonRepo::upsert_by_machine_id(
        &*state.db,
        UpsertDaemon {
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
async fn seed_agent(state: &AppState, name: &str) -> Agent {
    let now = now_rfc3339();
    let daemon_id = new_uuid_v4();
    let daemon = DaemonRepo::upsert_by_machine_id(
        &*state.db,
        UpsertDaemon {
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
async fn assign_coder(state: &AppState, task_id: &str, agent_id: &str) {
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

async fn call_tool(state: &AppState, name: &str, arguments: Value) -> Value {
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

async fn call_tool_scoped(
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

async fn call_tool_error(
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
}

#[test]
fn known_tool_internal_failure_redacts_provider_details() {
    let response = McpToolError::new(-32603, "database secret: 123")
        .with_data(json!({ "details": "database secret: 123" }))
        .with_call_context("forge_get_task", None, Some("user-1"))
        .into_tool_response(json!(1));
    let result = response.result.expect("tool failures use a success result");
    assert_eq!(result["structuredContent"]["code"], "internal_failure");
    assert_eq!(
        result["structuredContent"]["safe_message"],
        "the operation failed"
    );
    let serialized = serde_json::to_string(&result).expect("result serializes");
    assert!(!serialized.contains("database secret"));
    assert!(!serialized.contains("123"));
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
fn tools_list_returns_descriptors() {
    run_async(async {
        let state = sqlite_state().await;
        let result = dispatch(&state, "tools/list", json!({}))
            .await
            .expect("tools/list succeeds");
        let tools = result["tools"]
            .as_array()
            .expect("tools/list returns {tools:[...]}");
        let names = tools
            .iter()
            .map(|tool| tool["name"].as_str().expect("tool name").to_owned())
            .collect::<std::collections::BTreeSet<_>>();
        let expected = [
            "forge_add_task_dependency",
            "forge_assign_agent",
            "forge_cancel_task",
            "forge_create_agent_handoff",
            "forge_create_project",
            "forge_create_sub_tasks",
            "forge_create_task",
            "forge_follow_up_execution",
            "forge_memory_get",
            "forge_memory_search",
            "forge_get_project",
            "forge_get_agent_session",
            "forge_get_agent_chat",
            "forge_get_agent_handoff",
            "forge_get_main_agent",
            "forge_get_project_agent",
            "forge_get_task",
            "forge_get_task_diff",
            "forge_list_agent_profiles",
            "forge_list_agent_chat_messages",
            "forge_list_agent_chats",
            "forge_list_agent_handoffs",
            "forge_list_agent_sessions",
            "forge_list_agents",
            "forge_list_executions",
            "forge_list_projects",
            "forge_list_task_dependencies",
            "forge_list_tasks",
            "forge_preview_prompt",
            "forge_register_agent",
            "forge_remove_task_dependency",
            "forge_send_agent_chat_message",
            "forge_set_main_agent",
            "forge_set_project_agent",
            "forge_transition_task",
            "forge_update_project",
            "forge_update_project_lifecycle_hooks",
            "forge_update_task",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(names, expected);
        assert!(tools
            .iter()
            .any(|tool| tool.get("name").is_some() && tool.get("inputSchema").is_some()));
    });
}

#[test]
fn migrated_orchestration_contracts_are_not_shadowed_by_legacy_mcp_descriptors() {
    run_async(async {
        let state = sqlite_state().await;
        let result = dispatch(&state, "tools/list", json!({}))
            .await
            .expect("tools/list succeeds");
        let mcp_names = result["tools"]
            .as_array()
            .expect("tools list")
            .iter()
            .filter_map(|tool| tool["name"].as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let overlap = forge_agent_host::MIGRATED_OPERATION_CONTRACTS
            .iter()
            .map(|contract| contract.operation)
            .filter(|operation| mcp_names.contains(operation))
            .collect::<Vec<_>>();

        assert!(
            overlap.is_empty(),
            "a migrated operation may enter MCP only through a descriptor derived from its canonical contract: {overlap:?}"
        );
        assert!(mcp_names.contains("forge_create_project"));
        assert!(!mcp_names.contains(forge_agent_host::MAIN_PROJECT_CREATE_OPERATION));
        assert!(mcp_names.contains("forge_create_task"));
        assert!(!mcp_names.contains(forge_agent_host::TASK_PROPOSE_OPERATION));
    });
}

#[test]
fn revised_public_surface_does_not_advertise_retired_collaboration_tools() {
    run_async(async {
        let state = sqlite_state().await;
        let result = dispatch(&state, "tools/list", json!({}))
            .await
            .expect("tools/list succeeds");
        let names = result["tools"]
            .as_array()
            .expect("tools list")
            .iter()
            .filter_map(|tool| tool["name"].as_str())
            .collect::<Vec<_>>();
        assert!(names.iter().all(|name| !name.contains("room")));
        assert!(names.iter().all(|name| !name.contains("membership")));
        assert!(names.contains(&"forge_list_agent_chats"));
        assert!(names.contains(&"forge_send_agent_chat_message"));
        assert!(names.contains(&"forge_create_agent_handoff"));
    });
}

#[test]
fn scoped_tools_list_marks_project_id_optional() {
    run_async(async {
        let state = sqlite_state().await;
        let result = dispatch_with_context(
            &state,
            &McpContext {
                project_id: Some("project-1".to_owned()),
                user_id: Some("mcp-test-user".to_owned()),
            },
            "tools/list",
            json!({}),
        )
        .await
        .expect("tools/list succeeds");
        let tools = result["tools"]
            .as_array()
            .expect("tools/list returns {tools:[...]}");
        let create_task = tools
            .iter()
            .find(|tool| tool["name"] == "forge_create_task")
            .expect("create task descriptor exists");
        let required = create_task["inputSchema"]["required"]
            .as_array()
            .expect("required is an array");

        assert!(required.iter().any(|value| value == "title"));
        assert!(!required.iter().any(|value| value == "project_id"));

        let memory_search = tools
            .iter()
            .find(|tool| tool["name"] == "forge_memory_search")
            .expect("memory search descriptor exists");
        let memory_required = memory_search["inputSchema"]["required"]
            .as_array()
            .expect("required is an array");
        assert!(memory_required.iter().any(|value| value == "project_id"));
        assert!(memory_required.iter().any(|value| value == "query"));
    });
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

#[test]
fn scoped_mcp_injects_project_id_for_project_tools() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let result = call_tool_scoped(
            &state,
            &project_id,
            "forge_create_task",
            json!({
                "title": "Scoped task",
                "description": "created through scoped MCP"
            }),
        )
        .await;

        assert_eq!(result["project_id"], project_id);
        assert_eq!(result["title"], "Scoped task");
    });
}

#[test]
fn forge_create_task_persists_validated_task_type() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        for (task_type, title) in [
            ("discovery", "Discovery task"),
            ("planning_task", "Planning task"),
        ] {
            let result = call_tool(
                &state,
                "forge_create_task",
                json!({
                    "project_id": project_id,
                    "title": title,
                    "type": task_type
                }),
            )
            .await;
            let task_id = result["id"].as_str().expect("task id");
            let task = TaskRepo::get_by_id(&*state.db, task_id, false)
                .await
                .expect("task lookup succeeds")
                .expect("task persists");
            assert_eq!(task.task_type, task_type);
        }
    });
}

#[test]
fn forge_create_task_rejects_invalid_priority_with_field_error() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let error = call_tool_error(
            &state,
            "forge_create_task",
            json!({
                "project_id": project_id,
                "title": "Bad priority",
                "priority": "high"
            }),
        )
        .await;

        assert_eq!(error.code, -32602);
        let data = error.data.expect("error data");
        assert_eq!(data["field"], "priority");
        assert_eq!(data["accepted"]["type"], "integer");
    });
}

#[test]
fn forge_create_task_rejects_invalid_type_with_field_error() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let error = call_tool_error(
            &state,
            "forge_create_task",
            json!({
                "project_id": project_id,
                "title": "Bad type",
                "type": "epic"
            }),
        )
        .await;

        assert_eq!(error.code, -32602);
        let data = error.data.expect("error data");
        assert_eq!(data["field"], "type");
        assert_eq!(
            data["accepted"]["enum"],
            json!(["task", "planning_task", "sub_task", "discovery"])
        );
    });
}

#[test]
fn forge_create_task_rejects_missing_project_id_with_field_error() {
    run_async(async {
        let state = sqlite_state().await;
        let error = call_tool_error(
            &state,
            "forge_create_task",
            json!({
                "title": "Missing project"
            }),
        )
        .await;

        assert_eq!(error.code, -32602);
        let data = error.data.expect("error data");
        assert_eq!(data["field"], "project_id");
        assert_eq!(data["accepted"]["type"], "string");
    });
}

#[test]
fn forge_create_task_hides_unknown_project_before_dispatch() {
    run_async(async {
        let state = sqlite_state().await;
        let error = call_tool_error(
            &state,
            "forge_create_task",
            json!({
                "project_id": "does-not-exist",
                "title": "Unknown project"
            }),
        )
        .await;

        assert_eq!(error.code, -32004);
    });
}

#[test]
fn forge_create_sub_tasks_rejects_malformed_subtasks() {
    run_async(async {
        let state = sqlite_state().await;
        let root = seed_task(&state).await;
        let error = call_tool_error(
            &state,
            "forge_create_sub_tasks",
            json!({
                "parent_task_id": root.id,
                "subtasks": [{"description": "missing title"}]
            }),
        )
        .await;

        assert_eq!(error.code, -32602);
        let data = error.data.expect("error data");
        assert!(data["details"]
            .as_str()
            .expect("details string")
            .contains("title"));
    });
}

#[test]
fn scoped_mcp_rejects_mismatched_project_id() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let error = dispatch_with_context(
            &state,
            &McpContext {
                project_id: Some(project_id),
                user_id: Some("mcp-test-user".to_owned()),
            },
            "tools/call",
            json!({
                "name": "forge_list_tasks",
                "arguments": {
                    "project_id": "other-project"
                }
            }),
        )
        .await
        .expect_err("mismatched project scope errors");

        assert_eq!(error.code, -32602);
    });
}

#[test]
fn scoped_mcp_allows_task_id_tool_for_same_project() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let task = seed_task_in_project(&state, project_id.clone()).await;

        let result = call_tool_scoped(
            &state,
            &project_id,
            "forge_get_task",
            json!({ "task_id": task.id }),
        )
        .await;

        assert_eq!(result["id"], task.id);
        assert_eq!(result["project_id"], project_id);
    });
}

#[test]
fn scoped_mcp_rejects_task_id_tool_for_other_project() {
    run_async(async {
        let state = sqlite_state().await;
        let (scoped_project_id, _) = seed_project_repo(&state).await;
        let other_task = seed_task(&state).await;

        let error = dispatch_with_context(
            &state,
            &McpContext {
                project_id: Some(scoped_project_id),
                user_id: Some("mcp-test-user".to_owned()),
            },
            "tools/call",
            json!({
                "name": "forge_get_task",
                "arguments": {
                    "task_id": other_task.id,
                }
            }),
        )
        .await
        .expect_err("cross-project task handle errors");

        assert_eq!(error.code, -32602);
    });
}

#[test]
fn scoped_mcp_rejects_parent_task_id_tool_for_other_project() {
    run_async(async {
        let state = sqlite_state().await;
        let (scoped_project_id, _) = seed_project_repo(&state).await;
        let other_task = seed_task(&state).await;

        let error = dispatch_with_context(
            &state,
            &McpContext {
                project_id: Some(scoped_project_id),
                user_id: Some("mcp-test-user".to_owned()),
            },
            "tools/call",
            json!({
                "name": "forge_create_sub_tasks",
                "arguments": {
                    "parent_task_id": other_task.id,
                    "subtasks": [{
                        "title": "Cross-project child",
                        "description": "should not be created"
                    }]
                }
            }),
        )
        .await
        .expect_err("cross-project parent task handle errors");

        assert_eq!(error.code, -32602);
    });
}

#[test]
fn scoped_mcp_rejects_execution_id_tool_for_other_project() {
    run_async(async {
        let state = sqlite_state().await;
        let (scoped_project_id, _) = seed_project_repo(&state).await;
        let other_task = seed_task(&state).await;
        let execution_id = seed_execution(&state, other_task.id).await;

        let error = dispatch_with_context(
            &state,
            &McpContext {
                project_id: Some(scoped_project_id),
                user_id: Some("mcp-test-user".to_owned()),
            },
            "tools/call",
            json!({
                "name": "forge_follow_up_execution",
                "arguments": {
                    "execution_id": execution_id,
                    "message": "continue"
                }
            }),
        )
        .await
        .expect_err("cross-project execution handle errors");

        assert_eq!(error.code, -32602);
    });
}

#[test]
fn forge_update_task_updates_mutable_fields() {
    run_async(async {
        let state = sqlite_state().await;
        let task = seed_task(&state).await;
        let result = call_tool(
            &state,
            "forge_update_task",
            json!({
                "task_id": task.id,
                "title": "Updated MCP",
                "description": "changed",
                "priority": 9,
                "plan": "test it",
                "version": task.version,
            }),
        )
        .await;

        assert_eq!(result["title"], "Updated MCP");
        assert_eq!(result["description"], "changed");
        assert_eq!(result["priority"], 9);
        assert_eq!(result["plan"], "test it");
        assert_eq!(result["version"], 2);
    });
}

#[test]
fn forge_transition_task_changes_status() {
    run_async(async {
        let state = sqlite_state().await;
        let task = seed_task(&state).await;
        let result = call_tool(
            &state,
            "forge_transition_task",
            json!({
                "task_id": task.id,
                "status": "in_progress",
                "version": task.version,
            }),
        )
        .await;

        assert_eq!(result["status"], "in_progress");
        assert!(
            result["version"].as_i64().expect("version is an integer") > task.version,
            "transition should advance task version"
        );
    });
}

#[test]
fn forge_register_agent_registers_agent() {
    run_async(async {
        let state = sqlite_state().await;
        let (executor_type, daemon_id) = seed_agent_registration_deps(&state).await;
        let result = call_tool(
            &state,
            "forge_register_agent",
            json!({
                "name": "codex",
                "executor_type": executor_type.clone(),
                "daemon_id": daemon_id.clone(),
            }),
        )
        .await;

        assert_eq!(result["name"], "codex");
        assert_eq!(result["executor_type"], executor_type);
        assert_eq!(result["daemon_id"], daemon_id);
        assert_eq!(result["status"], "idle");
    });
}

#[test]
fn forge_list_agents_returns_paginated_agents() {
    run_async(async {
        let state = sqlite_state().await;
        let (executor_type, daemon_id) = seed_agent_registration_deps(&state).await;
        call_tool(
            &state,
            "forge_register_agent",
            json!({
                "name": "codex",
                "executor_type": executor_type.clone(),
                "daemon_id": daemon_id.clone(),
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
fn forge_list_projects_returns_paginated_projects() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let result = call_tool(
            &state,
            "forge_list_projects",
            json!({
                "limit": 10,
            }),
        )
        .await;
        let projects = result["data"].as_array().expect("projects array");

        assert_eq!(projects.len(), 1);
        assert_eq!(projects[0]["id"], project_id);
        assert_eq!(projects[0]["name"], "Forge");
    });
}

#[test]
fn forge_create_project_creates_project() {
    run_async(async {
        let state = sqlite_state().await;
        let result = call_tool(
            &state,
            "forge_create_project",
            json!({
                "name": "MCP",
            }),
        )
        .await;

        assert_eq!(result["name"], "MCP");
        assert!(result["id"].as_str().is_some());
        assert_eq!(result["settings"], json!({}));
        assert_eq!(result["paused"], false);
    });
}

#[test]
fn forge_get_project_returns_project_details() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let result = call_tool(
            &state,
            "forge_get_project",
            json!({
                "project_id": project_id,
            }),
        )
        .await;

        assert_eq!(result["name"], "Forge");
        assert_eq!(result["settings"], json!({}));
        assert_eq!(result["paused"], false);
        assert!(result.get("workflow_template_name").is_some());
    });
}

#[test]
fn forge_update_project_updates_mutable_fields() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let result = call_tool(
            &state,
            "forge_update_project",
            json!({
                "project_id": project_id,
                "version": ProjectRepo::get_by_id(&*state.db, &project_id).await.unwrap().unwrap().version,
                "name": "Updated Forge",
                "settings": {
                    "retry_budgets": {
                        "review": 5,
                        "merge_fix": 2
                    }
                },
                "paused": true
            }),
        )
        .await;

        assert_eq!(result["name"], "Updated Forge");
        assert_eq!(result["settings"]["retry_budgets"]["review"], 5);
        assert_eq!(result["paused"], true);
        assert!(result["paused_at"].as_str().is_some());
    });
}

#[test]
fn forge_update_project_lifecycle_hooks_replaces_hooks_only() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        call_tool(
            &state,
            "forge_update_project",
            json!({
                "project_id": project_id,
                "version": ProjectRepo::get_by_id(&*state.db, &project_id).await.unwrap().unwrap().version,
                "settings": {
                    "retry_budgets": {
                        "review": 4,
                        "merge_fix": 1
                    }
                }
            }),
        )
        .await;

        let result = call_tool(
            &state,
            "forge_update_project_lifecycle_hooks",
            json!({
                "project_id": project_id,
                "version": ProjectRepo::get_by_id(&*state.db, &project_id).await.unwrap().unwrap().version,
                "lifecycle_hooks": {
                    "before_work": [
                        {
                            "type": "script",
                            "command": "pnpm test",
                            "timeout_seconds": 60,
                            "blocking": true
                        }
                    ],
                    "on_task_done": [
                        {
                            "type": "plugin",
                            "name": "notify",
                            "enabled": true,
                            "config": { "channel": "dev" }
                        }
                    ]
                }
            }),
        )
        .await;

        assert_eq!(result["settings"]["retry_budgets"]["review"], 4);
        assert_eq!(
            result["settings"]["lifecycle_hooks"]["before_work"][0]["command"],
            "pnpm test"
        );
        assert_eq!(
            result["settings"]["lifecycle_hooks"]["on_task_done"][0]["name"],
            "notify"
        );
    });
}

#[test]
fn forge_update_project_lifecycle_hooks_validates_blocking_event() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let error = dispatch(
            &state,
            "tools/call",
            json!({
                "name": "forge_update_project_lifecycle_hooks",
                "arguments": {
                    "project_id": project_id,
                    "version": ProjectRepo::get_by_id(&*state.db, &project_id).await.unwrap().unwrap().version,
                    "lifecycle_hooks": {
                        "on_work_start": [
                            {
                                "type": "script",
                                "command": "echo no",
                                "blocking": true
                            }
                        ]
                    }
                }
            }),
        )
        .await
        .expect_err("invalid lifecycle hooks error");

        assert_eq!(error.code, -32602);
    });
}

#[test]
fn forge_create_sub_tasks_creates_subtasks() {
    run_async(async {
        let state = sqlite_state().await;
        let root = seed_task(&state).await;
        let result = call_tool(
            &state,
            "forge_create_sub_tasks",
            json!({
                "parent_task_id": root.id.clone(),
                "subtasks": [
                    { "title": "One" },
                    { "title": "Two" },
                    { "title": "Three" }
                ]
            }),
        )
        .await;

        let subtasks = result["subtasks"].as_array().expect("subtasks array");
        assert_eq!(subtasks.len(), 3);
        for (index, subtask) in subtasks.iter().enumerate() {
            assert_eq!(subtask["subtask_order"], index as i64);
        }
    });
}

#[test]
fn forge_create_sub_tasks_nested_rejected() {
    run_async(async {
        let state = sqlite_state().await;
        let root = seed_task(&state).await;
        let now = now_rfc3339();
        let subtask = TaskRepo::create(
            &*state.db,
            CreateTask {
                id: new_uuid_v4(),
                project_id: root.project_id.clone(),
                repo_id: root.repo_id.clone(),
                parent_task_id: Some(root.id.clone()),
                assignee_type: None,
                assignee_id: None,
                title: "Existing subtask".to_owned(),
                description: None,
                task_type: "task".to_owned(),
                status: "todo".to_owned(),
                is_automation: false,
                priority: 0,
                subtask_order: Some(0),
                task_state_config: None,
                merge_config: None,
                plan: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("subtask creates");

        let error = dispatch(
            &state,
            "tools/call",
            json!({
                "name": "forge_create_sub_tasks",
                "arguments": {
                    "parent_task_id": subtask.id,
                    "subtasks": [
                        { "title": "Nested" }
                    ]
                }
            }),
        )
        .await
        .expect_err("nested subtask errors");
        let response = error.into_response(json!(1));
        let error = response.error.expect("error response");
        assert_eq!(error.code, -32602);
        assert_eq!(
            error
                .data
                .as_ref()
                .and_then(|data| data.get("code"))
                .and_then(Value::as_str),
            Some("NESTED_SUBTASK_UNSUPPORTED")
        );
    });
}
