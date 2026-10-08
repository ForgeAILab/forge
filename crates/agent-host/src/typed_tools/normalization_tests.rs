use super::tests::{ConfiguredSearchProvider, all_permissions, scope};
use super::*;

/// Print actual compact provider definitions, not a reconstructed schema.
/// Capture with `cargo test -p forge-agent-host --lib serialized_tool_definitions -- --nocapture`.
#[test]
fn serialized_tool_definitions() {
    let permissions = all_permissions()
        .into_iter()
        .chain(
            [
                "propose_discovery",
                "propose_charter",
                "propose_project",
                "propose_adoption",
            ]
            .map(str::to_owned),
        )
        .collect();
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().to_str().unwrap();
    for (name, scope_type, access, role, project, setup) in [
        (
            "main",
            CanonicalScopeType::AgentChat,
            WorkspaceAccess::AccountScratch,
            None,
            false,
            false,
        ),
        (
            "inquiry",
            CanonicalScopeType::Account,
            WorkspaceAccess::AccountScratch,
            None,
            false,
            false,
        ),
        (
            "project",
            CanonicalScopeType::AgentChat,
            WorkspaceAccess::Deny,
            None,
            true,
            false,
        ),
        (
            "project_verify_solo",
            CanonicalScopeType::AgentChat,
            WorkspaceAccess::ProjectVerify,
            None,
            true,
            false,
        ),
        (
            "project_setup",
            CanonicalScopeType::AgentChat,
            WorkspaceAccess::Deny,
            None,
            true,
            true,
        ),
        (
            "worker",
            CanonicalScopeType::Task,
            WorkspaceAccess::TaskWrite,
            Some("worker"),
            false,
            false,
        ),
        (
            "reviewer",
            CanonicalScopeType::Task,
            WorkspaceAccess::TaskRead,
            Some("reviewer"),
            false,
            false,
        ),
        (
            "planner",
            CanonicalScopeType::Task,
            WorkspaceAccess::TaskRead,
            Some("planner"),
            false,
            false,
        ),
    ] {
        let composition = ScopeToolComposition::for_scope_with_permissions_and_project_context(
            "identity",
            scope(scope_type, access),
            role,
            (access != WorkspaceAccess::Deny).then_some(root),
            &permissions,
            ProjectChatToolContext {
                is_project_agent_chat: project,
                charter_setup_required: setup,
            },
            Some(Arc::new(ConfiguredSearchProvider)),
            ScopeToolRuntime {
                fetch_transport: Some(Arc::new(crate::ForgeFetchTransport::new())),
                ..Default::default()
            },
        )
        .unwrap();
        let definitions: Vec<_> = composition
            .tools()
            .iter()
            .map(|tool| tool.spec().to_schema())
            .collect();
        let snapshot: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/tool_definitions_normalized.json"
        ))
        .unwrap();
        assert_eq!(
            serde_json::to_value(&definitions).unwrap(),
            snapshot[name],
            "{name} schema snapshot"
        );
        for definition in &definitions {
            let schema = &definition.input_schema;
            if definition.name.contains("scope_") || definition.name.contains("orchestration_") {
                assert!(
                    schema["properties"].get("parameters").is_none(),
                    "{} has an envelope copy",
                    definition.name
                );
                assert_eq!(
                    schema["required"],
                    if definition.name.ends_with("read") {
                        json!(["operation"])
                    } else {
                        json!(["operation", "payload", "dedupe_key", "correlation_id"])
                    }
                );
            }
        }
        println!(
            "TOOL_DEFINITIONS {name} {}",
            serde_json::to_string(&definitions).unwrap()
        );
    }
}

/// Estimated-prefix ceilings per native surface, in compact UTF-8 bytes (the
/// unit `scripts/measure-tool-definitions.py` prints; tokens = ceil(bytes / 4)).
/// A surface may shrink freely. Raise a ceiling only deliberately, in the
/// change that explains why the prefix has to grow.
const SURFACE_BYTE_CEILINGS: &[(&str, usize)] = &[
    // 8,860 and 4,127 at 8bc736f5, less 46 and 38: the Main read lines state
    // which fields are required instead of `optional {...}` for all of them.
    // Main less 256 more: generated proposal contract lines replace the hand
    // summaries of `genesis.project_agent.select` and `project.create`.
    ("main", 8_558),
    ("inquiry", 4_089),
    // 24,048 and 25,297 at 172338b3, plus 56 each: the `skill.section`
    // argument line states the required enum instead of `optional {section}`.
    ("project", 24_104),
    ("project_verify_solo", 25_353),
    ("project_setup", 15_572),
    ("worker", 6_062),
    ("reviewer", 4_829),
    ("planner", 5_437),
];

/// Schema keywords that provider function-declaration dialects do not
/// reliably accept. Advertised native schemas must not use them; a
/// per-operation contract is enforced by the operation registry instead.
const FORBIDDEN_SCHEMA_KEYWORDS: &[&str] = &[
    "allOf",
    "anyOf",
    "not",
    "if",
    "then",
    "else",
    "$ref",
    "$defs",
    "dependentSchemas",
    "patternProperties",
];
/// `oneOf` predates the registry: four nullable-variant fields of the Charter
/// draft content in the setup surface's `forge_project_orchestration_propose`
/// at 172338b3. Tolerated exactly there, by count, until that schema moves.
const PRE_EXISTING_ONE_OF: &[(&str, usize)] = &[("project_setup", 4)];

fn schema_keyword_uses(value: &Value, keyword: &str) -> usize {
    match value {
        Value::Object(map) => map
            .iter()
            .map(|(key, value)| {
                // A property may legitimately be *named* like a keyword; only
                // the members of a `properties` map are names, not keywords.
                let nested = if key == "properties" {
                    value
                        .as_object()
                        .into_iter()
                        .flat_map(|properties| properties.values())
                        .map(|schema| schema_keyword_uses(schema, keyword))
                        .sum()
                } else {
                    schema_keyword_uses(value, keyword)
                };
                usize::from(key == keyword && key != "properties") + nested
            })
            .sum(),
        Value::Array(items) => items
            .iter()
            .map(|item| schema_keyword_uses(item, keyword))
            .sum(),
        _ => 0,
    }
}

fn native_tool_definition_snapshot() -> serde_json::Map<String, Value> {
    serde_json::from_str(include_str!(
        "../../tests/fixtures/tool_definitions_normalized.json"
    ))
    .unwrap()
}

/// `serialized_tool_definitions` pins this fixture to the live definitions.
#[test]
fn advertised_native_schemas_use_only_the_portable_keyword_set() {
    let snapshot = native_tool_definition_snapshot();
    assert_eq!(snapshot.len(), SURFACE_BYTE_CEILINGS.len());
    for (surface, definitions) in &snapshot {
        for definition in definitions.as_array().unwrap() {
            let schema = &definition["input_schema"];
            for keyword in FORBIDDEN_SCHEMA_KEYWORDS {
                assert_eq!(
                    schema_keyword_uses(schema, keyword),
                    0,
                    "{surface} {} advertises `{keyword}`",
                    definition["name"]
                );
            }
        }
        let allowed = PRE_EXISTING_ONE_OF
            .iter()
            .find(|(name, _)| name == surface)
            .map_or(0, |(_, count)| *count);
        assert_eq!(
            schema_keyword_uses(definitions, "oneOf"),
            allowed,
            "{surface} `oneOf` uses"
        );
    }
    // The walker itself: keywords are found at any depth, property names are not.
    let probe = json!({"properties":{"if":{"type":"string"},"x":{"items":{"allOf":[{"not":{}}]}}}});
    assert_eq!(schema_keyword_uses(&probe, "if"), 0);
    assert_eq!(schema_keyword_uses(&probe, "allOf"), 1);
    assert_eq!(schema_keyword_uses(&probe, "not"), 1);
}

#[test]
fn native_tool_prefix_stays_within_its_byte_ceilings() {
    let snapshot = native_tool_definition_snapshot();
    for (surface, ceiling) in SURFACE_BYTE_CEILINGS {
        let bytes = serde_json::to_string(&snapshot[*surface]).unwrap().len();
        assert!(
            bytes <= *ceiling,
            "{surface} tool definitions are {bytes} bytes, over the {ceiling} byte ceiling"
        );
    }
}

use agent_runtime::core::{
    approval::{AllowAll, ApprovalDecision, ApprovalPolicy, ApprovalRequest},
    check_set::{EnforcementLimits, SecurityCheckSetBuilder},
    content::ToolCall,
};
use agent_runtime::tool::{ConflictPolicy, SecurityConfig, ToolExecutor, ToolRegistry};

#[derive(Debug, Default)]
struct EchoProvider(Mutex<Vec<Value>>);
#[async_trait]
impl ForgeToolProvider for EchoProvider {
    async fn read(
        &self,
        _: &str,
        _: &CanonicalScope,
        _: &str,
        arguments: Value,
    ) -> Result<Value, AgentHostError> {
        self.0.lock().unwrap().push(arguments.clone());
        Ok(arguments)
    }
    async fn propose(
        &self,
        _: &str,
        _: &CanonicalScope,
        _: &str,
        _: &str,
        arguments: Value,
    ) -> Result<Value, AgentHostError> {
        self.0.lock().unwrap().push(arguments.clone());
        Ok(arguments)
    }
}

fn composition(provider: Arc<dyn ForgeToolProvider>) -> ScopeToolComposition {
    ScopeToolComposition::for_scope_with_permissions_and_project_chat(
        "identity",
        scope(CanonicalScopeType::AgentChat, WorkspaceAccess::Deny),
        None,
        None,
        &all_permissions(),
        true,
        Some(provider),
    )
    .unwrap()
}

fn proposal() -> Value {
    json!({"operation":"task.plan", "payload":{"action":"write","content":"# Plan\n- [ ] Implement"}, "dedupe_key":"key", "correlation_id":"corr"})
}

fn proposal_tool(provider: Arc<dyn ForgeToolProvider>) -> Arc<dyn Tool> {
    Arc::new(ForgeScopeProposeTool::new(
        "identity".to_owned(),
        scope(CanonicalScopeType::AgentChat, WorkspaceAccess::Deny),
        vec!["task.plan".to_owned()],
        provider,
    ))
}

fn executor(
    composition: &ScopeToolComposition,
    approval: Arc<dyn ApprovalPolicy>,
    require_approval: bool,
) -> ToolExecutor {
    let mut registry = ToolRegistry::new();
    registry.register_all(composition.tools()).unwrap();
    let clock = Arc::new(SystemClock);
    let mut checks = SecurityCheckSetBuilder::new(EnforcementLimits::default(), clock.clone());
    let check: Arc<dyn SecurityCheck> = if require_approval {
        Arc::new(ApprovalCheck(composition.coverage()))
    } else {
        composition.security_check.clone()
    };
    checks.register(
        check,
        SecurityCheckMode::Authoritative,
        composition.coverage(),
        ActionClass::new("test"),
    );
    ToolExecutor::new(
        registry.seal(),
        approval,
        Arc::new(DenyAllWorkspace),
        clock,
        128 * 1024,
        ConflictPolicy::ScopeOverlap,
        SecurityConfig {
            check_set: Arc::new(checks.seal().unwrap()),
            subject: SecuritySubject::new("identity"),
            tenant: TenantId::new("scope-1"),
        },
    )
}

async fn native_call(
    composition: &ScopeToolComposition,
    name: &str,
    arguments: Value,
) -> agent_runtime::core::content::ToolResultBlock {
    executor(composition, Arc::new(AllowAll), false)
        .execute(
            &[ToolCall {
                id: ToolCallId::new("call"),
                name: name.to_owned(),
                arguments,
            }],
            &RequestId::new("request"),
            &SessionId::new("session"),
            &Cancellation::new(),
            Deadline::never(),
        )
        .await
        .remove(0)
}

async fn accepted_proposal(arguments: Value) {
    let provider = Arc::new(EchoProvider::default());
    let mut composition = composition(provider.clone());
    composition.tools = vec![proposal_tool(provider.clone())];
    let native = native_call(&composition, "forge_scope_propose", arguments.clone()).await;
    assert!(!native.is_error, "{native:?}");
    let cli = composition
        .invoke_denied_chat_tool("session", "turn", "call", "forge_scope_propose", arguments)
        .await
        .unwrap();
    assert_eq!(cli, proposal());
    assert_eq!(*provider.0.lock().unwrap(), vec![proposal(), proposal()]);
}

#[tokio::test]
async fn canonical_input_on_native_and_cli() {
    accepted_proposal(proposal()).await;
}
#[tokio::test]
async fn complete_parameters_wrapper_on_native_and_cli() {
    accepted_proposal(json!({"parameters":proposal()})).await;
}
#[tokio::test]
async fn mixed_parameters_wrapper_on_native_and_cli() {
    let mut arguments = proposal();
    let payload = arguments
        .as_object_mut()
        .unwrap()
        .remove("payload")
        .unwrap();
    arguments["parameters"] = json!({"payload":payload});
    accepted_proposal(arguments).await;
}
#[tokio::test]
async fn identical_envelope_duplicates_on_native_and_cli() {
    let mut arguments = proposal();
    arguments["parameters"] = proposal();
    accepted_proposal(arguments).await;
}
#[tokio::test]
async fn flat_aliases_with_absent_payload_on_native_and_cli() {
    let mut arguments = proposal();
    let payload = arguments
        .as_object_mut()
        .unwrap()
        .remove("payload")
        .unwrap();
    arguments
        .as_object_mut()
        .unwrap()
        .extend(payload.as_object().unwrap().clone());
    accepted_proposal(arguments).await;
}
#[tokio::test]
async fn flat_aliases_with_null_payload_on_native_and_cli() {
    let mut arguments = proposal();
    let payload = arguments["payload"].take();
    arguments
        .as_object_mut()
        .unwrap()
        .extend(payload.as_object().unwrap().clone());
    accepted_proposal(arguments).await;
}
#[tokio::test]
async fn mixed_flat_and_nested_payload_on_native_and_cli() {
    let mut arguments = proposal();
    arguments["action"] = arguments["payload"]
        .as_object_mut()
        .unwrap()
        .remove("action")
        .unwrap();
    accepted_proposal(arguments).await;
}
#[tokio::test]
async fn identical_flat_and_nested_duplicates_on_native_and_cli() {
    let mut arguments = proposal();
    arguments["action"] = json!("write");
    accepted_proposal(arguments).await;
}
#[tokio::test]
async fn null_flat_aliases_mean_omission_on_native_and_cli() {
    let mut arguments = proposal();
    arguments["action"] = Value::Null;
    arguments["content"] = Value::Null;
    accepted_proposal(arguments).await;
}
#[tokio::test]
async fn wrapped_flat_aliases_on_native_and_cli() {
    let mut arguments = proposal();
    let payload = arguments
        .as_object_mut()
        .unwrap()
        .remove("payload")
        .unwrap();
    arguments["parameters"] = payload;
    accepted_proposal(arguments).await;
}

#[tokio::test]
async fn all_read_names_normalize_on_native_and_cli_and_preserve_nested_parameters() {
    for name in [
        "forge_scope_read",
        FORGE_MAIN_ORCHESTRATION_READ_TOOL,
        FORGE_PROJECT_ORCHESTRATION_READ_TOOL,
    ] {
        let provider = Arc::new(EchoProvider::default());
        let mut composition = composition(provider.clone());
        // Generic read arguments are open; named reads use their existing closed operation contract.
        let operation = if name == "forge_scope_read" {
            "agent_chat.current"
        } else {
            "project.current_state"
        };
        composition.tools = vec![if name == "forge_scope_read" {
            Arc::new(ForgeScopeReadTool::new(
                "identity".into(),
                composition.scope.clone(),
                vec![operation.into()],
                provider.clone(),
            ))
        } else {
            Arc::new(ForgeScopeReadTool::named(
                "identity".into(),
                composition.scope.clone(),
                vec![operation.into()],
                provider.clone(),
                name,
                "test scope",
            ))
        }];
        let nested = if name == "forge_scope_read" {
            json!({"parameters":{"literal":true}})
        } else {
            json!({"limit":1})
        };
        for canonical in [
            json!({"operation":operation}),
            json!({"operation":operation,"arguments":nested}),
        ] {
            for arguments in [
                canonical.clone(),
                json!({"parameters":canonical.clone()}),
                json!({"operation":operation,"parameters":canonical.clone()}),
            ] {
                assert!(
                    !native_call(&composition, name, arguments.clone())
                        .await
                        .is_error
                );
                composition
                    .invoke_denied_chat_tool("session", "turn", "call", name, arguments)
                    .await
                    .unwrap();
                let expected = canonical.get("arguments").cloned().unwrap_or(json!({}));
                assert_eq!(provider.0.lock().unwrap().pop().unwrap(), expected);
                assert_eq!(provider.0.lock().unwrap().pop().unwrap(), expected);
            }
        }
    }
}

#[tokio::test]
async fn named_proposals_normalize_on_native_and_cli() {
    for name in [
        "forge_scope_propose",
        FORGE_MAIN_ORCHESTRATION_PROPOSE_TOOL,
        FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
    ] {
        let provider = Arc::new(EchoProvider::default());
        let mut composition = composition(provider.clone());
        composition.tools = vec![Arc::new(ForgeScopeProposeTool::named(
            "identity".into(),
            composition.scope.clone(),
            vec!["project.readiness".into()],
            provider.clone(),
            name,
            "test scope",
        ))];
        let canonical = json!({"operation":"project.readiness", "payload":{"action":"evaluate","milestone_id":"m-1","milestone_version":1}, "dedupe_key":"key", "correlation_id":"corr"});
        for arguments in [canonical.clone(), json!({"parameters":canonical.clone()})] {
            assert!(
                !native_call(&composition, name, arguments.clone())
                    .await
                    .is_error
            );
            let cli = composition
                .invoke_denied_chat_tool("session", "turn", "call", name, arguments)
                .await
                .unwrap();
            assert_eq!(cli, canonical);
        }
    }
}

#[derive(Debug)]
struct IdentityFilter;
#[async_trait]
impl ToolResultFilter for IdentityFilter {
    async fn filter(
        &self,
        _: &ToolCallId,
        _: &Value,
        outcome: ToolOutcome,
    ) -> Result<ToolOutcome, RuntimeError> {
        Ok(outcome)
    }
}

#[tokio::test]
async fn all_three_wrappers_forward_normalization() {
    let provider = Arc::new(EchoProvider::default());
    let mut composition = composition(provider.clone());
    // Include the actual denial wrapper, then result filter, then observer.
    let inner = proposal_tool(provider.clone());
    composition.tools = vec![Arc::new(TerminalDenialTool {
        inner,
        denials: Default::default(),
        provider: provider.clone(),
        actor_identity_id: "identity".into(),
        scope: composition.scope.clone(),
    })];
    let observations = Arc::new(Mutex::new(0));
    let observed = observations.clone();
    let composition = composition
        .filter_results(Arc::new(IdentityFilter))
        .observe_results(Arc::new(move |_, _| {
            *observed.lock().unwrap() += 1;
        }));
    let arguments = json!({"parameters":proposal()});
    assert!(
        !native_call(&composition, "forge_scope_propose", arguments.clone())
            .await
            .is_error
    );
    composition
        .invoke_denied_chat_tool("session", "turn", "call", "forge_scope_propose", arguments)
        .await
        .unwrap();
    assert_eq!(*observations.lock().unwrap(), 2);
    assert_eq!(*provider.0.lock().unwrap(), vec![proposal(), proposal()]);
}

/// Audit 38-a M5: the tool list is exactly what the production builder
/// returns for a ready Project Agent (denial wrapper applied by the builder),
/// then wrapped by the result filter and the observer in the order
/// `NativeBackend` applies them. Nothing in `composition.tools` is replaced.
#[tokio::test]
async fn production_project_agent_composition_accepts_enveloped_calls() {
    let provider = Arc::new(EchoProvider::default());
    let permissions = all_permissions()
        .into_iter()
        .chain(
            [
                "propose_discovery",
                "propose_charter",
                "propose_project",
                "propose_adoption",
            ]
            .map(str::to_owned),
        )
        .collect();
    let observations = Arc::new(Mutex::new(0));
    let observed = observations.clone();
    let composition = ScopeToolComposition::for_scope_with_permissions_and_project_context(
        "identity",
        scope(CanonicalScopeType::AgentChat, WorkspaceAccess::Deny),
        None,
        None,
        &permissions,
        ProjectChatToolContext {
            is_project_agent_chat: true,
            charter_setup_required: false,
        },
        Some(provider.clone()),
        ScopeToolRuntime {
            fetch_transport: Some(Arc::new(crate::ForgeFetchTransport::new())),
            ..Default::default()
        },
    )
    .unwrap()
    .filter_results(Arc::new(IdentityFilter))
    .observe_results(Arc::new(move |_, _| {
        *observed.lock().unwrap() += 1;
    }));
    // Same surface the schema snapshot records for a ready Project Agent,
    // less web search: the builder offers it only when the provider reports a
    // configured search backend, and the echo provider reports none.
    let snapshot: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/tool_definitions_normalized.json"
    ))
    .unwrap();
    let names: Vec<Value> = composition
        .tools()
        .iter()
        .map(|tool| json!(tool.spec().name))
        .collect();
    let expected: Vec<Value> = snapshot["project"]
        .as_array()
        .unwrap()
        .iter()
        .map(|definition| definition["name"].clone())
        .filter(|name| name != FORGE_PUBLIC_WEB_SEARCH_TOOL)
        .collect();
    assert_eq!(names, expected);

    let read_arguments = json!({"limit": 1});
    let read = json!({"operation": "work.read", "arguments": read_arguments.clone()});
    let message = json!({
        "operation": "message.send",
        "payload": {"body": "hello"},
        "dedupe_key": "key",
        "correlation_id": "corr"
    });
    let readiness = json!({
        "operation": "project.readiness",
        "payload": {"action": "evaluate", "milestone_id": "m-1", "milestone_version": 1},
        "dedupe_key": "key",
        "correlation_id": "corr"
    });
    for (name, canonical, received) in [
        ("forge_scope_read", read, read_arguments),
        ("forge_scope_propose", message.clone(), message),
        (
            FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
            readiness.clone(),
            readiness,
        ),
    ] {
        let result = native_call(&composition, name, json!({"parameters": canonical})).await;
        assert!(!result.is_error, "{name}: {result:?}");
        // The provider is handed the prepared arguments, so this is the
        // canonical form the hook produced, not the envelope.
        assert_eq!(
            provider.0.lock().unwrap().pop().unwrap(),
            received,
            "{name}"
        );
    }
    assert!(provider.0.lock().unwrap().is_empty());
    assert_eq!(*observations.lock().unwrap(), 3);
}

#[tokio::test]
async fn normalization_errors_never_fall_back_on_native_or_cli() {
    let provider = Arc::new(EchoProvider::default());
    let mut composition = composition(provider.clone());
    composition.tools = vec![proposal_tool(provider.clone())];
    for arguments in [
        json!({"parameters":null}),
        {
            let mut v = proposal();
            v["parameters"] = json!({"operation":"different"});
            v
        },
        {
            let mut v = proposal();
            v["action"] = json!("different");
            v
        },
    ] {
        let native = native_call(&composition, "forge_scope_propose", arguments.clone()).await;
        assert!(native.is_error);
        assert!(
            composition
                .invoke_denied_chat_tool(
                    "session",
                    "turn",
                    "call",
                    "forge_scope_propose",
                    arguments
                )
                .await
                .is_err()
        );
    }
    assert!(provider.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn missing_required_fields_and_unknown_properties_fail_both_paths() {
    let provider = Arc::new(EchoProvider::default());
    let mut composition = composition(provider.clone());
    composition.tools = vec![proposal_tool(provider.clone())];
    let mut cases = Vec::new();
    for field in ["operation", "payload", "dedupe_key", "correlation_id"] {
        let mut arguments = proposal();
        arguments.as_object_mut().unwrap().remove(field);
        cases.push(arguments);
    }
    let mut extra = proposal();
    extra["unknown"] = json!(true);
    cases.push(extra);
    for arguments in cases {
        assert!(
            native_call(&composition, "forge_scope_propose", arguments.clone())
                .await
                .is_error
        );
        assert!(
            composition
                .invoke_denied_chat_tool(
                    "session",
                    "turn",
                    "call",
                    "forge_scope_propose",
                    arguments
                )
                .await
                .is_err()
        );
    }
    assert!(provider.0.lock().unwrap().is_empty());
}

#[derive(Debug)]
struct ApprovalCheck(PermissionSet);
#[async_trait]
impl SecurityCheck for ApprovalCheck {
    fn id(&self) -> &SecurityCheckId {
        static ID: std::sync::LazyLock<SecurityCheckId> =
            std::sync::LazyLock::new(|| SecurityCheckId::new("approval"));
        &ID
    }
    fn revision(&self) -> &SecurityCheckRevision {
        static REV: std::sync::LazyLock<SecurityCheckRevision> =
            std::sync::LazyLock::new(|| SecurityCheckRevision::new("test"));
        &REV
    }
    async fn evaluate(
        &self,
        request: &AuthorizationRequest,
        _: &Cancellation,
    ) -> SecurityCheckOutcome {
        assert!(request.requested.is_subset(&self.0));
        SecurityCheckOutcome::RequireApproval {
            constraints: GrantConstraints::unconstrained(),
        }
    }
}
#[derive(Debug)]
struct EditThenAllow(Mutex<Vec<PreparedToolCall>>);
#[async_trait]
impl ApprovalPolicy for EditThenAllow {
    async fn decide(&self, request: &ApprovalRequest) -> ApprovalDecision {
        let mut seen = self.0.lock().unwrap();
        seen.push(request.prepared().clone());
        if seen.len() == 1 {
            let mut edited = proposal();
            edited["dedupe_key"] = json!("edited-key");
            ApprovalDecision::Edit {
                arguments: json!({"parameters":edited}),
            }
        } else {
            ApprovalDecision::Allow
        }
    }
}
#[tokio::test]
async fn approval_edits_are_normalized_and_prepared_again_before_invocation() {
    let provider = Arc::new(EchoProvider::default());
    let mut composition = composition(provider.clone());
    composition.tools = vec![proposal_tool(provider.clone())];
    let approval = Arc::new(EditThenAllow(Mutex::new(Vec::new())));
    let executor = executor(&composition, approval.clone(), true);
    let results = executor
        .execute(
            &[ToolCall {
                id: ToolCallId::new("call"),
                name: "forge_scope_propose".into(),
                arguments: json!({"parameters":proposal()}),
            }],
            &RequestId::new("request"),
            &SessionId::new("session"),
            &Cancellation::new(),
            Deadline::never(),
        )
        .await;
    assert!(!results[0].is_error, "{:?}", results[0]);
    let seen = approval.0.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert_ne!(seen[0].fingerprint(), seen[1].fingerprint());
    assert_eq!(seen[1].arguments()["dedupe_key"], "edited-key");
    assert!(seen[1].arguments().get("parameters").is_none());
    assert_eq!(
        *provider.0.lock().unwrap(),
        vec![seen[1].arguments().clone()]
    );
}

#[derive(Debug)]
struct ParametersTool;
#[async_trait]
impl Tool for ParametersTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec::new(
            "literal_parameters",
            "A legitimate parameters field",
            json!({"type":"object","properties":{"parameters":{"type":"object"}},"required":["parameters"],"additionalProperties":false}),
            ToolEffects::default(),
        )
    }
    async fn prepare(
        &self,
        arguments: Value,
        ctx: &PreparationContext,
    ) -> Result<PreparedToolCall, RuntimeError> {
        Ok(PreparedToolCall::new(
            ctx.call_id.clone(),
            "literal_parameters",
            arguments,
            PermissionSet::default(),
            SecurityResource::other("forge.scope", "agent_chat:scope-1"),
            ToolEffects::default(),
            ToolCallDisplay::new("Literal parameters"),
        ))
    }
    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        _: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Ok(ToolOutcome::json(prepared.into_arguments()))
    }
}
#[tokio::test]
async fn legitimate_parameters_property_uses_identity_hook_on_both_paths() {
    let mut composition = composition(Arc::new(EchoProvider::default()));
    composition.tools = vec![Arc::new(ParametersTool)];
    let arguments = json!({"parameters":{"literal":true}});
    let native = native_call(&composition, "literal_parameters", arguments.clone()).await;
    assert!(!native.is_error);
    assert_eq!(
        serde_json::from_str::<Value>(native.content[0].as_text().unwrap()).unwrap(),
        arguments
    );
    assert_eq!(
        composition
            .invoke_denied_chat_tool(
                "session",
                "turn",
                "call",
                "literal_parameters",
                arguments.clone()
            )
            .await
            .unwrap(),
        arguments
    );
}
#[tokio::test]
async fn legitimate_payload_parameters_and_nested_nulls_survive_both_paths() {
    let provider = Arc::new(EchoProvider::default());
    let mut composition = composition(provider.clone());
    composition.tools = vec![Arc::new(ForgeScopeProposeTool::new(
        "identity".into(),
        composition.scope.clone(),
        vec!["memory.publish".into()],
        provider.clone(),
    ))];
    let arguments = json!({"operation":"memory.publish","payload":{"parameters":{"literal":true},"value":null},"dedupe_key":"key","correlation_id":"corr"});
    assert!(
        !native_call(
            &composition,
            "forge_scope_propose",
            json!({"parameters":arguments.clone()})
        )
        .await
        .is_error
    );
    let cli = composition
        .invoke_denied_chat_tool(
            "session",
            "turn",
            "call",
            "forge_scope_propose",
            json!({"parameters":arguments.clone()}),
        )
        .await
        .unwrap();
    assert_eq!(cli, arguments);
    assert_eq!(
        *provider.0.lock().unwrap(),
        vec![arguments.clone(), arguments]
    );
}

#[derive(Debug)]
struct FailingHook {
    inner: Arc<dyn Tool>,
}
#[async_trait]
impl Tool for FailingHook {
    fn spec(&self) -> ToolSpec {
        self.inner.spec()
    }
    fn normalize_arguments(&self, _: Value) -> Result<Value, RuntimeError> {
        Err(RuntimeError::tool("normalization failed"))
    }
    async fn prepare(
        &self,
        _: Value,
        _: &PreparationContext,
    ) -> Result<PreparedToolCall, RuntimeError> {
        panic!("failed hook must never reach prepare")
    }
    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        self.inner.invoke(prepared, ctx).await
    }
}
#[tokio::test]
async fn hook_failure_does_not_retry_schema_valid_raw_arguments() {
    let provider = Arc::new(EchoProvider::default());
    let mut composition = composition(provider.clone());
    composition.tools = vec![Arc::new(FailingHook {
        inner: proposal_tool(provider.clone()),
    })];
    let result = native_call(&composition, "forge_scope_propose", proposal()).await;
    assert!(result.is_error);
    assert!(
        result.content[0]
            .as_text()
            .unwrap()
            .contains("normalization failed")
    );
    let error = composition
        .invoke_denied_chat_tool("session", "turn", "call", "forge_scope_propose", proposal())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("normalization failed"));
    assert!(provider.0.lock().unwrap().is_empty());
}

use agent_runtime::core::checkpoint::{CheckpointStore, TurnCheckpoint, TurnState};
#[derive(Debug)]
struct PreparedCheckpointStore {
    saved: Mutex<TurnCheckpoint>,
    terminal: tokio::sync::Notify,
}
#[async_trait]
impl CheckpointStore for PreparedCheckpointStore {
    async fn load_latest(&self, _: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        // Exercise the protected wire round-trip rather than an in-memory clone.
        Ok(Some(
            serde_json::from_slice(&serde_json::to_vec(&*self.saved.lock().unwrap()).unwrap())
                .unwrap(),
        ))
    }
    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        checkpoint.validate()?;
        *self.saved.lock().unwrap() = checkpoint.clone();
        if checkpoint.state.is_terminal() {
            self.terminal.notify_one();
        }
        Ok(())
    }
}
#[tokio::test]
async fn runtime_recovers_an_exact_prepared_checkpoint_with_a_now_failing_hook() {
    use agent_runtime::core::{
        catalog::{ModelLimits, ResolvedModelProfile},
        clock::Timestamp,
        provider::ModelId,
        store::SessionSnapshot,
    };
    use agent_runtime::provider::fake::FakeProvider;
    use agent_runtime::runtime::StartSession;
    for require_approval in [false, true] {
        let provider = Arc::new(EchoProvider::default());
        let tool = proposal_tool(provider.clone());
        let ctx = PreparationContext {
            session: SessionId::new("session"),
            turn: Some(TurnId::new("turn")),
            call_id: ToolCallId::new("call"),
            request: RequestId::new("request"),
            workspace: Arc::new(DenyAllWorkspace),
            clock: Arc::new(SystemClock),
            cancel: Cancellation::new(),
            deadline: Deadline::never(),
        };
        // Old preparations have the same exact canonical arguments and authority.
        let prepared = tool.prepare(proposal(), &ctx).await.unwrap();
        let fingerprint = prepared.fingerprint().clone();
        let snapshot: SessionSnapshot =
            serde_json::from_value(json!({"id":"session","history":[],"updated":0})).unwrap();
        let call = ToolCall {
            id: ctx.call_id.clone(),
            name: "forge_scope_propose".into(),
            arguments: json!({"parameters":proposal()}),
        };
        let accepted = TurnCheckpoint::local_action(
            ctx.turn.unwrap(),
            ctx.request.clone(),
            call.clone(),
            snapshot.clone(),
            Deadline::never(),
            1,
            0,
            Timestamp(0),
        )
        .unwrap();
        let checkpoint = accepted
            .transition(
                TurnState::LocalActionPrepared {
                    request_id: ctx.request,
                    call,
                    prepared,
                },
                snapshot,
                0,
                Timestamp(0),
            )
            .unwrap();
        let store = Arc::new(PreparedCheckpointStore {
            saved: Mutex::new(checkpoint),
            terminal: Default::default(),
        });
        let mut composition = composition(provider.clone());
        composition.tools = vec![Arc::new(FailingHook { inner: tool })];
        if require_approval {
            composition.security_check = Arc::new(ApprovalCheck(composition.coverage()));
        }
        let approval = Arc::new(ApproveExact(Mutex::new(Vec::new())));
        let fake = Arc::new(FakeProvider::text_reply("must stay idle"));
        let runtime = composition
            .apply(RuntimeBuilder::new(ModelId::new("fake")))
            .provider(fake.clone())
            .model_profile(ResolvedModelProfile::explicit(
                "fake",
                ModelId::new("fake"),
                ModelLimits::new(128_000, 128_000, 4_096),
            ))
            .checkpoint_store(store.clone())
            .approval(approval.clone())
            .build()
            .unwrap();
        let session = runtime
            .start_session(StartSession::resume(SessionId::new("session")))
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), store.terminal.notified())
            .await
            .unwrap();
        assert!(session.resumed());
        assert!(store.saved.lock().unwrap().state.is_terminal());
        assert_eq!(*provider.0.lock().unwrap(), vec![proposal()]);
        // The saved preparation is not recalculated by recovery; its fingerprint
        // remains bound to the original arguments, not to the raw wrapper.
        let canonical = proposal_tool(provider.clone())
            .prepare(
                proposal(),
                &PreparationContext {
                    session: SessionId::new("session"),
                    turn: None,
                    call_id: ToolCallId::new("call"),
                    request: RequestId::new("request"),
                    workspace: Arc::new(DenyAllWorkspace),
                    clock: Arc::new(SystemClock),
                    cancel: Cancellation::new(),
                    deadline: Deadline::never(),
                },
            )
            .await
            .unwrap();
        assert_eq!(canonical.fingerprint(), &fingerprint);
        assert!(fake.requests().is_empty());
        let approvals = approval.0.lock().unwrap().clone();
        assert_eq!(approvals.len(), usize::from(require_approval));
        for approved in approvals {
            assert_eq!(approved.fingerprint(), &fingerprint);
            assert_eq!(approved.arguments(), &proposal());
        }
        session.shutdown().await.unwrap();
    }
}

#[derive(Debug)]
struct ApproveExact(Mutex<Vec<PreparedToolCall>>);
#[async_trait]
impl ApprovalPolicy for ApproveExact {
    async fn decide(&self, request: &ApprovalRequest) -> ApprovalDecision {
        self.0.lock().unwrap().push(request.prepared().clone());
        ApprovalDecision::Allow
    }
}

#[tokio::test]
async fn empty_parameters_wrapper_keeps_canonical_fields_on_both_paths() {
    let mut arguments = proposal();
    arguments["parameters"] = json!({});
    accepted_proposal(arguments).await;
}
#[tokio::test]
async fn nullable_generic_causation_fields_survive_both_paths() {
    let provider = Arc::new(EchoProvider::default());
    let mut composition = composition(provider.clone());
    composition.tools = vec![proposal_tool(provider.clone())];
    let mut arguments = proposal();
    arguments["causation_id"] = Value::Null;
    arguments["causation_depth"] = Value::Null;
    assert!(
        !native_call(
            &composition,
            "forge_scope_propose",
            json!({"parameters":arguments.clone()})
        )
        .await
        .is_error
    );
    let cli = composition
        .invoke_denied_chat_tool(
            "session",
            "turn",
            "call",
            "forge_scope_propose",
            json!({"parameters":arguments.clone()}),
        )
        .await
        .unwrap();
    assert_eq!(cli, arguments);
    assert_eq!(
        *provider.0.lock().unwrap(),
        vec![arguments.clone(), arguments]
    );
}

/// Hand-written gate forms, including literal prepared/checkpoint fingerprints
/// calculated with the unchanged runtime pin. No current Tool.prepare or
/// normalization implementation is used to manufacture historical authority.
#[tokio::test]
async fn main_proposals_resume_pre_change_prepared_pending_and_edited_checkpoint_bytes() {
    use agent_runtime::core::{
        catalog::{ModelLimits, ResolvedModelProfile},
        provider::ModelId,
    };
    use agent_runtime::provider::fake::FakeProvider;
    use agent_runtime::runtime::StartSession;
    let fixtures: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/main_proposal_checkpoints_pre_change.json"
    ))
    .unwrap();
    for (name, bytes) in fixtures.as_object().unwrap() {
        let checkpoint: TurnCheckpoint = serde_json::from_value(bytes.clone()).unwrap();
        checkpoint
            .validate()
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        let prepared = match &checkpoint.state {
            TurnState::LocalActionPrepared { prepared, .. } => prepared.clone(),
            TurnState::AwaitingApproval { slots, .. } => match &slots[0] {
                agent_runtime::core::checkpoint::ToolSlotCheckpoint::Prepared(prepared) => {
                    prepared.clone()
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        };
        assert!(prepared.verify_fingerprint(), "{name}");
        let provider = Arc::new(EchoProvider::default());
        let inner = ForgeScopeProposeTool::named(
            "identity".into(),
            CanonicalScope {
                scope_type: CanonicalScopeType::AgentChat,
                scope_id: "chat".into(),
                workspace_access: WorkspaceAccess::Deny,
            },
            vec![prepared.arguments()["operation"].as_str().unwrap().into()],
            provider.clone(),
            FORGE_MAIN_ORCHESTRATION_PROPOSE_TOOL,
            "Main scope",
        );
        let store = Arc::new(PreparedCheckpointStore {
            saved: Mutex::new(checkpoint),
            terminal: Default::default(),
        });
        let mut composition = composition(provider.clone());
        composition.tools = vec![Arc::new(FailingHook {
            inner: Arc::new(inner),
        })];
        composition.security_check = Arc::new(ApprovalCheck(composition.coverage()));
        let approval = Arc::new(ApproveExact(Mutex::new(Vec::new())));
        let fake = Arc::new(FakeProvider::text_reply("Recorded action completed."));
        let runtime = composition
            .apply(RuntimeBuilder::new(ModelId::new("fake")))
            .provider(fake.clone())
            .model_profile(ResolvedModelProfile::explicit(
                "fake",
                ModelId::new("fake"),
                ModelLimits::new(128_000, 128_000, 4_096),
            ))
            .checkpoint_store(store.clone())
            .approval(approval.clone())
            .build()
            .unwrap();
        let session = runtime
            .start_session(StartSession::resume(SessionId::new("session")))
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), store.terminal.notified())
            .await
            .unwrap();
        assert!(session.resumed(), "{name}");
        assert!(store.saved.lock().unwrap().state.is_terminal(), "{name}");
        assert_eq!(
            provider.0.lock().unwrap().as_slice(),
            &[prepared.arguments().clone()],
            "{name}"
        );
        assert_eq!(
            approval.0.lock().unwrap().as_slice(),
            &[prepared],
            "{name}: approve exact recorded authority"
        );
        session.shutdown().await.unwrap();
    }
}
