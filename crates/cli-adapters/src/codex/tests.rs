//! Codex configuration, session resumption, discovery, and execution tests.

use super::*;
use executors::CommandOverrides;
use std::fs;

#[test]
fn sub_agent_resume_target_falls_back_to_a_fresh_thread() {
    let sub_agent = ExecutorError::Other(
            "thread/resume failed: cannot resume an unloaded multi-agent v2 sub-agent through its parent; resume the parent first, or use thread/read to inspect it (-32600)".to_owned(),
        );
    assert!(is_unresumable_codex_thread_error(&sub_agent));
    assert!(!is_unresumable_codex_thread_error(&ExecutorError::Other(
        "thread/resume failed: rate limited".to_owned()
    )));
}

#[test]
fn command_builder_uses_codex_default_app_server() {
    let config = CodexConfig {
        permission_policy: Some(PermissionPolicy::Supervised),
        command_overrides: CommandOverrides {
            additional_params: Some(vec!["--verbose".to_owned()]),
            ..CommandOverrides::default()
        },
        ..CodexConfig::default()
    };

    let cmd = CodexAdapter::build_command(&config, None);
    assert_eq!(cmd.as_std().get_program(), "npx");
    let args: Vec<_> = cmd
        .as_std()
        .get_args()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        args,
        vec!["-y", "@openai/codex@0.159.1", "app-server", "--verbose"]
    );
}

#[tokio::test]
async fn discovery_advertises_current_models_and_per_model_efforts() {
    let discovered = CodexAdapter::new()
        .discover_options(DiscoverContext { project_path: None })
        .await
        .expect("Codex options should be discoverable");

    assert_eq!(
        discovered.models,
        vec![
            "gpt-6.1-sol",
            "gpt-6-astra",
            "gpt-6-sol",
            "gpt-6-luna",
            "gpt-reserve",
            "gpt-5.6-sol",
            "gpt-5.6-terra",
            "gpt-5.6-luna",
            "gpt-5.5",
            "codex-auto-review",
        ]
    );
    assert_eq!(discovered.cli_specific["codex_version"], "0.159.1");
    assert_eq!(
        discovered.cli_specific["model_reasoning_efforts"]["gpt-6.1-sol"],
        json!(["low", "medium", "high", "xhigh", "max", "ultra"])
    );
    assert_eq!(
        discovered.cli_specific["model_reasoning_efforts"]["gpt-5.6-sol"],
        json!(["low", "medium", "high", "xhigh", "max", "ultra"])
    );
    assert_eq!(
        discovered.cli_specific["model_reasoning_efforts"]["gpt-5.6-luna"],
        json!(["low", "medium", "high", "xhigh", "max"])
    );
    assert_eq!(
        discovered.cli_specific["model_reasoning_efforts"]["gpt-6-sol"],
        json!(["low", "medium", "high", "xhigh", "max", "ultra"])
    );
    assert_eq!(
        discovered.cli_specific["model_reasoning_efforts"]["gpt-6-luna"],
        json!(["low", "medium", "high", "xhigh", "max"])
    );
    assert_eq!(
        codex_reasoning_efforts_for_model("gpt-5.5"),
        ["low", "medium", "high", "xhigh"]
    );
    assert_eq!(
        codex_reasoning_efforts_for_model("gpt-reserve"),
        ["low", "medium", "high", "xhigh", "max"]
    );
    assert_eq!(
        codex_reasoning_efforts_for_model("future-codex-model"),
        CODEX_REASONING_EFFORTS
    );
}

#[test]
fn thread_start_params_maps_policy() {
    let config = CodexConfig {
        model: Some("gpt-6.1-sol".to_owned()),
        permission_policy: Some(PermissionPolicy::Plan),
        model_reasoning_effort: Some("high".to_owned()),
        ..CodexConfig::default()
    };

    let params = CodexAdapter::thread_start_params(&config, "/tmp/worktree", &json!({}), &[], &[]);

    assert_eq!(params.model.as_deref(), Some("gpt-6.1-sol"));
    assert!(matches!(params.sandbox, Some(SandboxMode::ReadOnly)));
    assert!(matches!(
        params.approval_policy,
        Some(AskForApproval::OnRequest)
    ));
    assert_eq!(
        params
            .config
            .as_ref()
            .and_then(|config| config.get("model_reasoning_effort"))
            .and_then(Value::as_str),
        Some("high")
    );
}

#[test]
fn thread_start_params_maps_yolo_to_full_access_without_approval() {
    let config = CodexConfig {
        permission_policy: Some(PermissionPolicy::Yolo),
        sandbox: Some("read-only".to_owned()),
        ask_for_approval: Some("on-request".to_owned()),
        ..CodexConfig::default()
    };

    let params = CodexAdapter::thread_start_params(&config, "/tmp/worktree", &json!({}), &[], &[]);

    assert!(matches!(
        params.sandbox,
        Some(SandboxMode::DangerFullAccess)
    ));
    assert!(matches!(
        params.approval_policy,
        Some(AskForApproval::Never)
    ));
}

#[test]
fn managed_task_scope_overrides_sandbox_and_profile_approval_settings() {
    // A raw `sandbox` setting cannot widen a managed Task; only the
    // explicit `yolo` permission policy does.
    let config = CodexConfig {
        permission_policy: Some(PermissionPolicy::Auto),
        sandbox: Some("danger-full-access".to_owned()),
        ask_for_approval: Some("on-request".to_owned()),
        profile: Some("ambient-profile".to_owned()),
        ..CodexConfig::default()
    };
    let mut runtime_scope = json!({});
    executors::mark_task_role(&mut runtime_scope, "worker");

    let params = CodexAdapter::thread_start_params(
        &config,
        "/tmp/worktree",
        &runtime_scope,
        &["project-local".to_owned()],
        &[PathBuf::from("/tmp/forge-task-scratch")],
    );

    assert!(matches!(params.sandbox, Some(SandboxMode::WorkspaceWrite)));
    assert!(matches!(
        params.approval_policy,
        Some(AskForApproval::Never)
    ));
    let overrides = params.config.expect("managed overrides are present");
    assert_eq!(overrides.get("profile"), None);
    assert_eq!(overrides["features"]["apps"], json!(false));
    assert_eq!(overrides["features"]["plugins"], json!(false));
    assert_eq!(overrides["features"]["hooks"], json!(false));
    assert_eq!(overrides["features"]["skill_search"], json!(false));
    assert_eq!(
        overrides["features"]["skip_host_skill_discovery"],
        json!(true)
    );
    assert_eq!(overrides["skills"]["include_instructions"], json!(false));
    assert_eq!(overrides["web_search"], json!("disabled"));
    assert_eq!(
        overrides["mcp_servers"]["project-local"]["enabled"],
        json!(false)
    );
    assert_eq!(
        overrides["sandbox_workspace_write"],
        json!({
            "network_access": true,
            "exclude_slash_tmp": true,
            "exclude_tmpdir_env_var": true,
            "writable_roots": ["/tmp/forge-task-scratch"],
        })
    );
    assert_eq!(
        params.developer_instructions.as_deref(),
        Some(MANAGED_WRITE_DELIVERY_INSTRUCTIONS)
    );
}

#[test]
fn managed_read_only_role_gets_verification_sandbox_without_delivery_instructions() {
    let config = CodexConfig {
        permission_policy: Some(PermissionPolicy::Auto),
        ..CodexConfig::default()
    };
    let mut runtime_scope = json!({});
    executors::mark_task_role(&mut runtime_scope, "reviewer");
    executors::mark_worktree_read_only(&mut runtime_scope);

    let params = CodexAdapter::thread_start_params(
        &config,
        "/tmp/worktree",
        &runtime_scope,
        &[],
        &[PathBuf::from("/home/u/.npm")],
    );

    // Without yolo, writes stay confined to the worktree and the declared
    // roots, and Forge's post-run gate discards them.
    assert!(matches!(params.sandbox, Some(SandboxMode::WorkspaceWrite)));
    assert!(matches!(
        params.approval_policy,
        Some(AskForApproval::Never)
    ));
    let overrides = params.config.expect("managed overrides are present");
    assert_eq!(
        overrides["sandbox_workspace_write"]["network_access"],
        json!(true)
    );
    assert_eq!(
        overrides["sandbox_workspace_write"]["writable_roots"],
        json!(["/home/u/.npm"])
    );
    assert_eq!(params.developer_instructions, None);
}

#[test]
fn managed_yolo_role_gets_full_access() {
    let config = CodexConfig {
        permission_policy: Some(PermissionPolicy::Yolo),
        ..CodexConfig::default()
    };
    for read_only in [false, true] {
        let mut runtime_scope = json!({});
        executors::mark_task_role(&mut runtime_scope, "reviewer");
        if read_only {
            executors::mark_worktree_read_only(&mut runtime_scope);
        }

        let params =
            CodexAdapter::thread_start_params(&config, "/tmp/worktree", &runtime_scope, &[], &[]);

        assert!(matches!(
            params.sandbox,
            Some(SandboxMode::DangerFullAccess)
        ));
        assert!(matches!(
            params.approval_policy,
            Some(AskForApproval::Never)
        ));
    }
}

#[test]
fn chat_thread_start_params_advertises_only_read_only_scoped_tools() {
    let config = CodexConfig {
        model: Some("gpt-5-codex".to_owned()),
        permission_policy: Some(PermissionPolicy::Auto),
        include_apply_patch_tool: Some(false),
        ..CodexConfig::default()
    };
    let tools = vec![DynamicToolSpec {
        name: "forge_echo".to_owned(),
        description: "Echo JSON".to_owned(),
        input_schema: json!({
            "type": "object",
            "properties": { "value": { "type": "string" } },
        }),
    }];
    let params = CodexAdapter::chat_thread_start_params(
        &config,
        "/tmp/worktree",
        &json!({}),
        tools,
        &["inherited-server".to_owned()],
    );
    let wire = serde_json::to_value(&params).expect("thread params serialize");

    assert_eq!(wire["approvalPolicy"], "never");
    assert_eq!(wire["sandbox"], "read-only");
    assert_eq!(wire["environments"], json!([]));
    assert_eq!(wire["dynamicTools"][0]["type"], "function");
    assert_eq!(wire["dynamicTools"][0]["name"], "forge_echo");
    assert_eq!(wire["dynamicTools"][0]["inputSchema"]["type"], "object");
    assert_eq!(wire["config"]["features"]["code_mode"], false);
    assert_eq!(wire["config"]["features"]["code_mode_only"], false);
    assert_eq!(wire["config"]["features"]["shell_tool"], false);
    assert_eq!(wire["config"]["features"]["apply_patch_freeform"], false);
    assert_eq!(wire["config"]["features"]["apps"], false);
    assert_eq!(wire["config"]["features"]["plugins"], false);
    assert_eq!(wire["config"]["features"]["hooks"], false);
    assert_eq!(wire["config"]["features"]["skill_search"], false);
    assert_eq!(
        wire["config"]["features"]["skip_host_skill_discovery"],
        true
    );
    assert_eq!(wire["config"]["features"]["connectors"], false);
    assert_eq!(wire["config"]["features"]["browser_use"], false);
    assert_eq!(wire["config"]["features"]["computer_use"], false);
    assert_eq!(wire["config"]["features"]["enable_mcp_apps"], false);
    assert_eq!(wire["config"]["features"]["web_search"], false);
    assert_eq!(wire["config"]["web_search"], "disabled");
    assert_eq!(wire["config"]["skills"]["include_instructions"], false);
    assert_eq!(
        wire["config"]["mcp_servers"]["inherited-server"]["enabled"],
        false
    );
}

#[test]
fn chat_dynamic_tool_promotes_nested_payload_guidance() {
    let guidance = "project.review_config requires action=set_ci_steps and string ci_steps";
    let params = CodexAdapter::chat_thread_start_params(
        &CodexConfig::default(),
        "/tmp/worktree",
        &json!({}),
        vec![DynamicToolSpec {
            name: "forge_project_orchestration_propose".to_owned(),
            description: "Submit a typed Forge proposal.".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "payload": {
                        "type": "object",
                        "description": guidance,
                    },
                },
            }),
        }],
        &[],
    );
    let wire = serde_json::to_value(params).expect("thread params serialize");

    assert_eq!(
        wire["dynamicTools"][0]["description"],
        format!("Submit a typed Forge proposal.\n\n{guidance}")
    );
    assert_eq!(
        wire["dynamicTools"][0]["inputSchema"]["properties"]["payload"]["description"],
        guidance
    );
}

#[test]
fn config_read_mcp_names_include_top_level_and_profile_servers() {
    let response = json!({
        "config": {
            "mcp_servers": { "root-server": {} },
            "profiles": {
                "chat": { "mcp_servers": { "profile-server": {} } },
                "review": { "mcp_servers": { "root-server": {} } },
            },
        },
    });

    assert_eq!(
        configured_mcp_server_names(&response),
        vec!["profile-server", "root-server"]
    );
}

#[test]
fn codex_error_notification_extracts_nested_json_message() {
    let raw = json!({
        "jsonrpc": "2.0",
        "method": "error",
        "params": {
            "error": {
                "additionalDetails": null,
                "codexErrorInfo": "other",
                "message": "{\"type\":\"error\",\"status\":400,\"error\":{\"type\":\"invalid_request_error\",\"message\":\"The 'gpt-5.5' model requires a newer version of Codex. Please upgrade to the latest app or CLI and try again.\"}}"
            },
            "threadId": "thread-1",
            "turnId": "turn-1",
            "willRetry": false
        }
    });

    assert_eq!(
        codex_event_error_message(&raw).as_deref(),
        Some(
            "The 'gpt-5.5' model requires a newer version of Codex. Please upgrade to the latest app or CLI and try again."
        )
    );
}

#[test]
fn bare_system_error_status_uses_fallback_message() {
    let raw = json!({
        "jsonrpc": "2.0",
        "method": "thread/status/changed",
        "params": {
            "status": { "type": "systemError" },
            "threadId": "thread-1"
        }
    });

    assert_eq!(
        codex_event_error_message(&raw).as_deref(),
        Some(CODEX_SYSTEM_ERROR_FALLBACK)
    );
}

#[derive(Default)]
struct StubCodexSessionClient {
    calls: Vec<&'static str>,
    start_params: Option<ThreadStartParams>,
    fork_params: Option<ThreadForkParams>,
    resume_params: Option<ThreadResumeParams>,
    turn_thread_id: Option<String>,
    turn_prompt: Option<String>,
    fail_resume_with_missing_thread: bool,
    fail_fork_with_missing_thread: bool,
    turn_attempts: usize,
}

#[async_trait]
impl CodexSessionClient for StubCodexSessionClient {
    async fn thread_start(
        &mut self,
        params: ThreadStartParams,
    ) -> Result<ThreadStartResponse, ExecutorError> {
        self.calls.push("thread_start");
        self.start_params = Some(params);
        Ok(ThreadStartResponse {
            thread: Some(protocol::ThreadInfo {
                id: "fresh-thread".to_owned(),
            }),
            thread_id: None,
            model: None,
        })
    }

    async fn thread_fork(
        &mut self,
        params: ThreadForkParams,
    ) -> Result<ThreadForkResponse, ExecutorError> {
        self.calls.push("thread_fork");
        self.fork_params = Some(params);
        if self.fail_fork_with_missing_thread {
            return Err(ExecutorError::Other(
                "thread/fork failed: thread not found: source-thread (-32600)".to_owned(),
            ));
        }
        Ok(ThreadForkResponse {
            thread: Some(protocol::ThreadInfo {
                id: "forked-thread".to_owned(),
            }),
            thread_id: None,
            model: None,
        })
    }

    async fn thread_resume(
        &mut self,
        params: ThreadResumeParams,
    ) -> Result<ThreadResumeResponse, ExecutorError> {
        self.calls.push("thread_resume");
        self.resume_params = Some(params);
        if self.fail_resume_with_missing_thread {
            return Err(ExecutorError::Other(
                "thread/resume failed: thread not found: source-thread (-32600)".to_owned(),
            ));
        }
        Ok(ThreadResumeResponse {
            thread: Some(protocol::ThreadInfo {
                id: "source-thread".to_owned(),
            }),
            thread_id: None,
            model: None,
        })
    }

    async fn turn_start(
        &mut self,
        thread_id: String,
        prompt: String,
    ) -> Result<TurnHandle, ExecutorError> {
        self.calls.push("turn_start");
        self.turn_attempts += 1;
        self.turn_thread_id = Some(thread_id);
        self.turn_prompt = Some(prompt);
        Ok(TurnHandle {
            turn_id: Some("turn-1".to_owned()),
        })
    }
}

#[tokio::test]
async fn resume_review_forks_thread_then_starts_turn() {
    let mut client = StubCodexSessionClient::default();
    let config = CodexConfig {
        resume_thread_id: Some("source-thread".to_owned()),
        model: Some("gpt-5-codex".to_owned()),
        permission_policy: Some(PermissionPolicy::Supervised),
        ..CodexConfig::default()
    };
    let ctx = ExecutionContext {
        task_id: "task-1".to_owned(),
        execution_id: "exec-1".to_owned(),
        worktree_path: "/tmp/forge-codex-worktree".to_owned(),
        description: "Review the changes".to_owned(),
        agent_config: json!({}),
        logs_path: "/tmp/forge-codex.log".to_owned(),
        heartbeat_interval_seconds: 30,
        max_turns: None,
        log_sender: None,
    };

    let (thread_id, turn_id) =
        CodexAdapter::start_codex_session(&config, &ctx, &mut client, &[], &[])
            .await
            .expect("session starts");

    assert_eq!(thread_id, "forked-thread");
    assert_eq!(turn_id.as_deref(), Some("turn-1"));
    assert_eq!(client.calls, vec!["thread_fork", "turn_start"]);
    assert!(!client.calls.contains(&"start_review"));
    assert_eq!(client.turn_thread_id.as_deref(), Some("forked-thread"));
    assert_eq!(client.turn_prompt.as_deref(), Some("Review the changes"));

    let fork_params = client.fork_params.expect("fork params captured");
    assert_eq!(fork_params.thread_id, "source-thread");
    assert_eq!(fork_params.cwd.as_deref(), Some(ctx.worktree_path.as_str()));
    assert_eq!(fork_params.model.as_deref(), Some("gpt-5-codex"));
}

#[test]
fn unresumable_codex_thread_wordings_are_recognized() {
    for message in [
        "thread/fork failed: thread not found: source-thread (-32600)",
        "thread/fork failed: no rollout found for thread id 01a0db3a-7bb0-7fc2-87f4-4cd2253b52e1 (-32600)",
        "thread/resume failed: no rollout found for thread id 01a0db3a-7bb0-7fc2-87f4-4cd2253b52e1 (-32600)",
        "thread/resume failed: cannot resume an unloaded multi-agent v2 sub-agent through its parent; resume the parent first, or use thread/read to inspect it (-32600)",
    ] {
        assert!(
            is_unresumable_codex_thread_error(&ExecutorError::Other(message.to_owned())),
            "{message}"
        );
    }
    assert!(!is_unresumable_codex_thread_error(&ExecutorError::Other(
        "thread/fork failed: rate limited (-32000)".to_owned()
    )));
}

#[tokio::test]
async fn resume_review_starts_fresh_when_legacy_source_thread_is_missing() {
    let mut client = StubCodexSessionClient {
        fail_fork_with_missing_thread: true,
        ..StubCodexSessionClient::default()
    };
    let config = CodexConfig {
        resume_thread_id: Some("source-thread".to_owned()),
        model: Some("gpt-5-codex".to_owned()),
        permission_policy: Some(PermissionPolicy::Supervised),
        ..CodexConfig::default()
    };
    let ctx = ExecutionContext {
        task_id: "task-1".to_owned(),
        execution_id: "exec-1".to_owned(),
        worktree_path: "/tmp/forge-codex-worktree".to_owned(),
        description: "Review the changes".to_owned(),
        agent_config: json!({}),
        logs_path: "/tmp/forge-codex.log".to_owned(),
        heartbeat_interval_seconds: 30,
        max_turns: None,
        log_sender: None,
    };

    let (thread_id, turn_id) =
        CodexAdapter::start_codex_session(&config, &ctx, &mut client, &[], &[])
            .await
            .expect("missing legacy session starts fresh");

    assert_eq!(thread_id, "fresh-thread");
    assert_eq!(turn_id.as_deref(), Some("turn-1"));
    assert_eq!(
        client.calls,
        vec!["thread_fork", "thread_start", "turn_start"]
    );
    assert_eq!(client.turn_thread_id.as_deref(), Some("fresh-thread"));
    assert_eq!(client.turn_prompt.as_deref(), Some("Review the changes"));
}

#[tokio::test]
async fn resume_conversation_starts_turn_on_existing_thread() {
    let mut client = StubCodexSessionClient::default();
    let config = CodexConfig {
        resume_thread_id: Some("source-thread".to_owned()),
        resume_thread_in_place: Some(true),
        resume_fallback_prompt: Some("Full reconstructed prompt".to_owned()),
        model: Some("gpt-5-codex".to_owned()),
        permission_policy: Some(PermissionPolicy::Plan),
        ..CodexConfig::default()
    };
    let ctx = ExecutionContext {
        task_id: "conversation-1".to_owned(),
        execution_id: "message-1".to_owned(),
        worktree_path: "/tmp/forge-codex-worktree".to_owned(),
        description: "Continue the conversation".to_owned(),
        agent_config: json!({}),
        logs_path: "/tmp/forge-codex.log".to_owned(),
        heartbeat_interval_seconds: 30,
        max_turns: None,
        log_sender: None,
    };

    let (thread_id, turn_id) =
        CodexAdapter::start_codex_session(&config, &ctx, &mut client, &[], &[])
            .await
            .expect("session starts");

    assert_eq!(thread_id, "source-thread");
    assert_eq!(turn_id.as_deref(), Some("turn-1"));
    assert_eq!(client.calls, vec!["thread_resume", "turn_start"]);
    assert_eq!(client.turn_thread_id.as_deref(), Some("source-thread"));
    assert_eq!(
        client.turn_prompt.as_deref(),
        Some("Continue the conversation")
    );
    assert!(client.fork_params.is_none());

    let resume_params = client.resume_params.expect("resume params captured");
    assert_eq!(resume_params.thread_id, "source-thread");
    assert_eq!(
        resume_params.cwd.as_deref(),
        Some(ctx.worktree_path.as_str())
    );
    assert_eq!(resume_params.model.as_deref(), Some("gpt-5-codex"));
    assert_eq!(resume_params.exclude_turns, Some(true));
}

#[tokio::test]
async fn resume_conversation_falls_back_to_new_thread_when_source_thread_is_missing() {
    let mut client = StubCodexSessionClient {
        fail_resume_with_missing_thread: true,
        ..StubCodexSessionClient::default()
    };
    let config = CodexConfig {
        resume_thread_id: Some("source-thread".to_owned()),
        resume_thread_in_place: Some(true),
        resume_fallback_prompt: Some("Full reconstructed prompt".to_owned()),
        model: Some("gpt-5-codex".to_owned()),
        permission_policy: Some(PermissionPolicy::Plan),
        ..CodexConfig::default()
    };
    let ctx = ExecutionContext {
        task_id: "conversation-1".to_owned(),
        execution_id: "message-1".to_owned(),
        worktree_path: "/tmp/forge-codex-worktree".to_owned(),
        description: "Continue the conversation".to_owned(),
        agent_config: json!({}),
        logs_path: "/tmp/forge-codex.log".to_owned(),
        heartbeat_interval_seconds: 30,
        max_turns: None,
        log_sender: None,
    };

    let (thread_id, turn_id) =
        CodexAdapter::start_codex_session(&config, &ctx, &mut client, &[], &[])
            .await
            .expect("session starts");

    assert_eq!(thread_id, "fresh-thread");
    assert_eq!(turn_id.as_deref(), Some("turn-1"));
    assert_eq!(
        client.calls,
        vec!["thread_resume", "thread_start", "turn_start"]
    );
    assert_eq!(client.turn_thread_id.as_deref(), Some("fresh-thread"));
    assert_eq!(
        client.turn_prompt.as_deref(),
        Some("Full reconstructed prompt")
    );
}

#[tokio::test]
async fn chat_session_always_starts_fresh_thread_even_when_resume_is_configured() {
    let mut client = StubCodexSessionClient::default();
    let config = CodexConfig {
        resume_thread_id: Some("old-thread".to_owned()),
        resume_thread_in_place: Some(true),
        model: Some("gpt-5-codex".to_owned()),
        ..CodexConfig::default()
    };
    let ctx = ExecutionContext {
        task_id: "chat-1".to_owned(),
        execution_id: "turn-1".to_owned(),
        worktree_path: "/tmp/forge-chat-worktree".to_owned(),
        description: "Answer the user".to_owned(),
        agent_config: json!({}),
        logs_path: "/tmp/forge-chat.log".to_owned(),
        heartbeat_interval_seconds: 30,
        max_turns: None,
        log_sender: None,
    };

    let (thread_id, turn_id) = CodexAdapter::start_chat_session(
        &config,
        &ctx,
        &mut client,
        vec![DynamicToolSpec {
            name: "forge_echo".to_owned(),
            description: "Echo JSON".to_owned(),
            input_schema: json!({"type": "object"}),
        }],
        &["inherited-server".to_owned()],
    )
    .await
    .expect("chat session starts");

    assert_eq!(thread_id, "fresh-thread");
    assert_eq!(turn_id.as_deref(), Some("turn-1"));
    assert_eq!(client.calls, vec!["thread_start", "turn_start"]);
    assert!(client.resume_params.is_none());
    let start_params = client.start_params.expect("chat start params captured");
    assert_eq!(start_params.dynamic_tools.as_ref().map(Vec::len), Some(1));
    assert_eq!(start_params.environments, Some(Vec::new()));
}

#[test]
fn availability_reports_authenticated_from_mock_auth_file() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("auth.json"), "{}").unwrap();

    let availability = availability_from_codex_home(dir.path());

    assert!(matches!(
        availability.status,
        AvailabilityStatus::Authenticated
    ));
    assert!(
        availability
            .config_path
            .as_deref()
            .unwrap()
            .ends_with("auth.json")
    );
}

#[tokio::test]
async fn codex_capacity_limit_uses_fallback_chain() {
    use executors::{AdapterRegistry, FallbackExecutor, RouteAttemptOutcome, TaskExecutor};

    struct FakeCodex(CodexAdapter);

    #[async_trait]
    impl CodingExecutorAdapter for FakeCodex {
        fn kind(&self) -> ExecutorKind {
            ExecutorKind::Codex
        }
        fn check_availability(&self) -> AvailabilityInfo {
            AvailabilityInfo {
                status: AvailabilityStatus::Authenticated,
                authenticated_at: None,
                config_path: None,
            }
        }
        async fn discover_options(
            &self,
            ctx: DiscoverContext,
        ) -> Result<DiscoveredOptions, ExecutorError> {
            self.0.discover_options(ctx).await
        }
        async fn execute(&self, ctx: ExecutionContext) -> Result<ExecutionResult, ExecutorError> {
            self.0.execute(ctx).await
        }
        async fn cancel(&self, execution_id: &str) -> Result<(), ExecutorError> {
            self.0.cancel(execution_id).await
        }
    }

    for (rpc_failure, with_fallback) in [(false, true), (true, true), (false, false)] {
        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("capacity-codex.sh");
        let error = json!({"message": "You've hit your usage limit", "retry_after_seconds": 90});
        let terminal = if rpc_failure {
            json!({"jsonrpc": "2.0", "id": 3, "error": {"code": -32000, "message": "HTTP 429", "data": error}})
        } else {
            json!({"jsonrpc": "2.0", "method": "turn/completed", "params": {"threadId": "thread-1", "turn": {"id": "turn-1", "error": error}}})
        };
        let turn_response = if rpc_failure {
            ""
        } else {
            "printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":3,\"result\":{\"turn\":{\"id\":\"turn-1\"}}}'"
        };
        process_supervisor::executable::write_executable(
            &script,
            format!(
                r#"#!/bin/sh
read line || exit 1
printf '%s\n' '{{"jsonrpc":"2.0","id":1,"result":{{}}}}'
read line || exit 1
read line || exit 1
printf '%s\n' '{{"jsonrpc":"2.0","id":2,"result":{{"thread":{{"id":"thread-1"}}}}}}'
read line || exit 1
{turn_response}
printf '%s\n' '{}'
"#,
                terminal.to_string().replace('\'', "'\\''")
            ),
        )
        .unwrap();
        let primary = json!({"executor_type": "codex", "config": {"base_command_override": script.display().to_string(), "auto_commit": false}});
        let mut candidates = vec![primary.clone()];
        if with_fallback {
            candidates.push(json!({"executor_type": "shell", "config": {}}));
        }
        let mut registry = AdapterRegistry::new();
        registry.register(Box::new(FakeCodex(CodexAdapter::new())));
        registry.register(Box::new(crate::ShellAdapter::new()));
        let result = FallbackExecutor::new(Arc::new(registry)).execute(ExecutionContext {
                task_id: "task-1".to_owned(), execution_id: "exec-capacity".to_owned(),
                worktree_path: dir.path().display().to_string(), description: "printf fallback-ok".to_owned(),
                agent_config: json!({"executor_type": "codex", "config": primary["config"], "routing": {"policy": "ordered_fallback_v1", "candidates": candidates}}),
                logs_path: dir.path().join("capacity.jsonl").display().to_string(), heartbeat_interval_seconds: 30, max_turns: None, log_sender: None,
            }).await.unwrap();
        assert_eq!(
            result.route_attempts[0].outcome,
            RouteAttemptOutcome::UsageExhausted
        );
        if with_fallback {
            assert_eq!(result.status, ExecutionOutcome::Completed);
            assert_eq!(result.route_attempts.len(), 2);
            assert_eq!(
                result.route_attempts[1].outcome,
                RouteAttemptOutcome::Completed
            );
            assert_eq!(
                result.resolved_candidate.unwrap().executor_type,
                ExecutorKind::Shell
            );
        } else {
            assert_eq!(result.status, ExecutionOutcome::Failed);
            assert_eq!(
                result.failure_class,
                Some(executors::ExecutionFailureClass::ExecutorUnavailable)
            );
            assert_eq!(result.retry_after, Some(std::time::Duration::from_secs(90)));
        }
    }
}

#[tokio::test]
async fn turn_completed_error_returns_failed_without_commit_or_session() {
    let dir = tempfile::tempdir().expect("tempdir creates");
    let script_path = dir.path().join("fake-codex.sh");
    process_supervisor::executable::write_executable(
            &script_path,
            r#"#!/bin/sh
read line || exit 1
printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{}}'
read line || exit 1
read line || exit 1
printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"thread":{"id":"thread-1"}}}'
read line || exit 1
printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"turn":{"id":"turn-1"}}}'
printf '%s\n' '{"jsonrpc":"2.0","method":"thread/status/changed","params":{"threadId":"thread-1","status":{"type":"systemError"}}}'
printf '%s\n' '{"jsonrpc":"2.0","method":"error","params":{"threadId":"thread-1","turnId":"turn-1","willRetry":false,"error":{"message":"{\"type\":\"error\",\"status\":400,\"error\":{\"type\":\"invalid_request_error\",\"message\":\"model rejected\"}}"}}}'
printf '%s\n' '{"jsonrpc":"2.0","method":"turn/completed","params":{"threadId":"thread-1","turn":{"id":"turn-1","error":{"message":"model rejected"}}}}'
"#,
        )
        .expect("script writes");

    let adapter = CodexAdapter::new();
    let result = adapter
        .execute(ExecutionContext {
            task_id: "task-1".to_owned(),
            execution_id: "exec-1".to_owned(),
            worktree_path: dir.path().display().to_string(),
            description: "Do the task".to_owned(),
            agent_config: json!({
                "base_command_override": script_path.display().to_string()
            }),
            logs_path: dir.path().join("codex.jsonl").display().to_string(),
            heartbeat_interval_seconds: 30,
            max_turns: None,
            log_sender: None,
        })
        .await
        .expect("adapter returns execution result");

    assert_eq!(result.status, ExecutionOutcome::Failed);
    assert_eq!(result.after_sha, None);
    assert_eq!(result.agent_session_id.as_deref(), Some("thread-1"));
    assert_eq!(result.error.as_deref(), Some("model rejected"));

    let logs = fs::read_to_string(dir.path().join("codex.jsonl")).expect("logs written");
    assert!(logs.contains("codex_protocol_error"));
    assert!(logs.contains("model rejected"));
}
