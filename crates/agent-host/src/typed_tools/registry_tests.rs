use super::tests::{all_permissions, scope, test_invocation_context, test_preparation_context};
use super::*;
use operation_registry::READ_CATALOG;

#[test]
fn spec_authority_agrees_with_existing_permission_check_on_every_surface() {
    use operation_registry::authority::{AuthorityFacts, EffectiveAuthority, Principal};
    // A = Main account, M = Main Chat, P = Project, C = Project Chat.
    let rows = [
        ("account.summary", "A", false),
        ("agent_chat.summary", "MC", false),
        ("charter.approval_target", "AM", false),
        ("charter.diff", "AM", false),
        ("charter.read", "AM", false),
        ("charter.readiness", "AM", false),
        ("discovery.read", "AM", false),
        ("genesis.project_agents.read", "AM", false),
        ("inquiry.run", "M", false),
        ("portfolio.read", "AM", false),
        ("project.charter", "PC", true),
        ("project.current_state", "PC", false),
        ("project.observations", "PC", true),
        ("skill.section", "PC", true),
        ("genesis.project_agent.select", "AM", false),
        ("project.create", "AM", false),
        ("project.review_config", "PC", true),
        ("project.document", "PC", true),
        ("project.decision", "PC", true),
        ("project.milestone", "PC", true),
        ("project.validation", "PC", true),
        ("project.release.request", "PC", true),
        ("project.escalate", "PC", true),
        ("message.send", "PC", false),
        ("commitment.update", "PC", true),
        ("memory.publish", "PC", true),
        ("memory.supersede", "PC", true),
        ("review.request", "P", true),
        ("session.action", "PC", true),
    ];
    let specs = READ_CATALOG
        .iter()
        .chain(operation_registry::PROPOSAL_CATALOG.iter())
        .collect::<Vec<_>>();
    assert_eq!(
        specs.iter().map(|s| s.id).collect::<BTreeSet<_>>(),
        rows.iter().map(|r| r.0).collect::<BTreeSet<_>>(),
        "every moved operation needs an authority row"
    );
    let granted_permissions = specs
        .iter()
        .flat_map(|spec| {
            spec.authority
                .permissions
                .iter()
                .map(|(_, permission)| (*permission).to_owned())
        })
        .collect::<BTreeSet<_>>();
    // The registered permission and effect class must still equal the host
    // operation catalog's for every scope: the unregistered surfaces and the
    // denial vocabulary read that catalog.
    for spec in READ_CATALOG.iter() {
        for scope_type in [
            CanonicalScopeType::Account,
            CanonicalScopeType::AgentChat,
            CanonicalScopeType::Project,
            CanonicalScopeType::Task,
        ] {
            let existing = crate::operation_permission(scope_type, spec.id);
            assert_eq!(
                spec.authority.permission(scope_type_name(scope_type)),
                existing,
                "{} {scope_type:?}",
                spec.id
            );
            if existing.is_some() {
                assert_eq!(
                    spec.effect == operation_registry::EffectClass::Query,
                    crate::operation_descriptor(scope_type, spec.id, None).classification
                        == crate::OperationClassification::Query,
                    "{} {scope_type:?}",
                    spec.id
                );
            }
        }
    }
    for spec in specs {
        let (_, admitted_scopes, ready_only) = rows.iter().find(|r| r.0 == spec.id).unwrap();
        for (key, scope_type, project_chat) in [
            ('A', CanonicalScopeType::Account, false),
            ('M', CanonicalScopeType::AgentChat, false),
            ('P', CanonicalScopeType::Project, false),
            ('C', CanonicalScopeType::AgentChat, true),
            ('T', CanonicalScopeType::Task, false),
        ] {
            for granted in [false, true] {
                for setup in [false, true] {
                    let permissions = if granted {
                        granted_permissions.clone()
                    } else {
                        BTreeSet::new()
                    };
                    let context = ProjectChatToolContext {
                        is_project_agent_chat: project_chat,
                        charter_setup_required: setup,
                    };
                    for kind in 0..6 {
                        if (kind == 0 && matches!(key, 'P' | 'C'))
                            || (kind == 1 && matches!(key, 'A' | 'M'))
                        {
                            continue;
                        }
                        let principal = match kind {
                            0 => Principal::MainAgent {
                                identity_id: "main".into(),
                            },
                            1 => Principal::ProjectAgent {
                                identity_id: "project".into(),
                                project_id: "p".into(),
                            },
                            2 => Principal::InteractiveUser {
                                user_id: "user".into(),
                            },
                            3 => Principal::DelegatedUser {
                                user_id: "user".into(),
                            },
                            4 => Principal::TaskAgent {
                                identity_id: "task".into(),
                                execution_id: "exec".into(),
                            },
                            _ => Principal::SystemComponent {
                                name: "system".into(),
                            },
                        };
                        let authority = EffectiveAuthority::resolve(AuthorityFacts {
                            principal,
                            scope_type: scope_type_name(scope_type).into(),
                            scope_id: "scope".into(),
                            profile_id: "profile".into(),
                            layers: vec![permissions.clone()],
                            binding_id: Some("projection".into()),
                            setup_required: setup,
                            active: true,
                        });
                        let native_principal = if matches!(key, 'P' | 'C') {
                            kind == 1
                        } else {
                            kind == 0
                        };
                        let expected = native_principal
                            && admitted_scopes.contains(key)
                            && granted
                            && (!ready_only || !setup);
                        assert_eq!(
                            authority.evaluate(spec).is_ok(),
                            expected,
                            "{} {key} principal={kind} profile={granted} setup={setup}",
                            spec.id
                        );
                        if native_principal {
                            let advertised = !filter_operations(
                                scope_type,
                                &[spec.id.into()],
                                &permissions,
                                context,
                                None,
                            )
                            .is_empty();
                            assert_eq!(advertised, expected, "advertisement {} {key}", spec.id);
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug, Default)]
struct RecordingProvider(std::sync::Mutex<Vec<(String, Value)>>);
#[async_trait]
impl ForgeToolProvider for RecordingProvider {
    async fn read(
        &self,
        _: &str,
        _: &CanonicalScope,
        operation: &str,
        input: Value,
    ) -> Result<Value, AgentHostError> {
        self.0.lock().unwrap().push((operation.to_owned(), input));
        Ok(json!({"operation":operation}))
    }
    async fn propose(
        &self,
        _: &str,
        _: &CanonicalScope,
        _: &str,
        _: &str,
        _: Value,
    ) -> Result<Value, AgentHostError> {
        unreachable!()
    }
}

#[tokio::test]
async fn moved_reads_normalize_validate_and_dispatch_both_forms() {
    for spec in READ_CATALOG.iter() {
        let provider = Arc::new(RecordingProvider::default());
        let tool = ForgeScopeReadTool::named(
            "actor".into(),
            scope(CanonicalScopeType::AgentChat, WorkspaceAccess::Deny),
            vec![spec.id.to_owned()],
            provider.clone(),
            spec.surfaces[0].native_aggregate,
            "test scope",
        );
        let inputs: Value = serde_json::from_str(include_str!(
            "../../../operation-registry/tests/read_inputs.json"
        ))
        .unwrap();
        let input = inputs[spec.id].clone();
        let flat = json!({"operation":spec.id,"arguments":input});
        for raw in [
            flat.clone(),
            json!({"parameters":flat}),
            json!({"operation":spec.id,"parameters":{"arguments":input}}),
        ] {
            let canonical = tool.normalize_arguments(raw).unwrap();
            let prepared = tool
                .prepare(canonical, &test_preparation_context("registry"))
                .await
                .unwrap();
            assert!(
                !tool
                    .invoke(prepared, &test_invocation_context("registry"))
                    .await
                    .unwrap()
                    .is_error
            );
        }
        // The advertised schema carries no per-operation contract, so the
        // registry is what refuses a violation, as an in-turn tool error
        // naming the operation and the field. Nothing reaches the handler.
        let invalid = json!({"operation":spec.id,"arguments":{"foreign_project":"x"}});
        let error = tool
            .prepare(invalid, &test_preparation_context("invalid"))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(&format!(
                "{}: argument `foreign_project` is not admitted",
                spec.id
            )),
            "{error}"
        );
        for field in spec.input.schema["properties"].as_object().unwrap().keys() {
            let mut invalid = input.clone();
            invalid[field] = json!({});
            let error = tool
                .prepare(
                    json!({"operation":spec.id,"arguments":invalid}),
                    &test_preparation_context("invalid-field"),
                )
                .await
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(spec.id) && error.contains(field) && error.contains("expected"),
                "{error}"
            );
        }
        assert_eq!(
            provider.0.lock().unwrap().as_slice(),
            vec![(spec.id.to_owned(), input); 3]
        );
    }
}

/// Availability in the spec agrees with the catalog's setup exposure for the
/// operations the catalog describes.
#[test]
fn spec_availability_agrees_with_the_setup_exposure() {
    use crate::operation_catalog::OperationSetupExposure;
    use operation_registry::AvailabilityRule;
    for spec in READ_CATALOG.iter() {
        let Some(contract) = crate::operation_contract(spec.id) else {
            assert_eq!(spec.availability, AvailabilityRule::Always, "{}", spec.id);
            continue;
        };
        if spec.id == MAIN_INQUIRY_RUN_OPERATION {
            assert_eq!(spec.availability, AvailabilityRule::MainChatOnly);
            assert!(dispatches_inquiries(CanonicalScopeType::AgentChat));
            assert!(!dispatches_inquiries(CanonicalScopeType::Account));
            continue;
        }
        assert_eq!(
            spec.availability,
            match contract.setup {
                OperationSetupExposure::Always => AvailabilityRule::Always,
                OperationSetupExposure::SetupOnly => AvailabilityRule::SetupOnly,
                OperationSetupExposure::ReadyOnly => AvailabilityRule::ReadyOnly,
            },
            "{}",
            spec.id
        );
    }
}

/// The CLI chat callback runs the same normalization and registry validation
/// as native execution: a contract violation is a tool error naming the
/// operation and the field, and the handler is never reached.
#[tokio::test]
async fn cli_callback_enforces_the_registered_contract() {
    let provider = Arc::new(RecordingProvider::default());
    let composition = ScopeToolComposition::for_scope_with_permissions_and_project_chat(
        "identity-chat",
        scope(CanonicalScopeType::AgentChat, WorkspaceAccess::Deny),
        None,
        None,
        &all_permissions(),
        true,
        Some(provider.clone()),
    )
    .unwrap();
    for (tool, arguments, expected) in [
        (
            FORGE_PROJECT_ORCHESTRATION_READ_TOOL,
            json!({"operation":"skill.section"}),
            "skill.section: argument `section` is required",
        ),
        (
            FORGE_PROJECT_ORCHESTRATION_READ_TOOL,
            json!({"parameters":{"operation":"skill.section","arguments":{"section":"everything"}}}),
            "skill.section: argument `section` must be one of: research,",
        ),
        (
            FORGE_PROJECT_ORCHESTRATION_READ_TOOL,
            json!({"operation":"project.charter","arguments":{"project_id":"other"}}),
            "project.charter: argument `project_id` is not admitted",
        ),
        (
            "forge_scope_read",
            json!({"operation":"agent_chat.summary","arguments":{"limit":1}}),
            "agent_chat.summary: argument `limit` is not admitted",
        ),
    ] {
        let error = composition
            .invoke_denied_chat_tool("session", "turn", "call", tool, arguments)
            .await
            .unwrap_err();
        assert!(
            matches!(error, AgentHostError::Runtime(ref message) if message.contains(expected)),
            "{expected}: {error:?}"
        );
    }
    assert!(provider.0.lock().unwrap().is_empty());
    for (tool, arguments) in [
        (
            FORGE_PROJECT_ORCHESTRATION_READ_TOOL,
            json!({"parameters":{"operation":"skill.section","arguments":{"section":"release"}}}),
        ),
        (
            FORGE_PROJECT_ORCHESTRATION_READ_TOOL,
            json!({"operation":"project.charter"}),
        ),
        (
            "forge_scope_read",
            json!({"operation":"agent_chat.summary"}),
        ),
    ] {
        composition
            .invoke_denied_chat_tool("session", "turn", "call", tool, arguments)
            .await
            .unwrap();
    }
    assert_eq!(provider.0.lock().unwrap().len(), 3);
}

#[test]
fn main_registered_reads_have_no_hand_schema_validator_or_dispatch_arm() {
    let schemas = include_str!("../operation_contract.rs")
        .split("pub(crate) fn orchestration_read_arguments_schema")
        .nth(1)
        .unwrap()
        .split("pub(crate) fn orchestration_read_schema")
        .next()
        .unwrap();
    // Read-classified operations never reach the proposal payload schema;
    // an arm for one there would be a second, unreachable contract.
    let payloads = include_str!("../operation_contract.rs")
        .split("pub(crate) fn orchestration_payload_schema")
        .nth(1)
        .unwrap()
        .split("pub(crate) fn portable_const_schema")
        .next()
        .unwrap();
    assert!(payloads.contains("MAIN_CHARTER_DRAFT_OPERATION =>"));
    let validators = include_str!("../typed_tools.rs")
        .split("fn validate_orchestration_read_arguments")
        .nth(1)
        .unwrap()
        .split("fn ")
        .next()
        .unwrap();
    let dispatcher = include_str!("../../../services/src/native_tools.rs")
        .split("let result = if let Some(spec) = registered_reads::CATALOG")
        .nth(1)
        .unwrap()
        .split("if operation_contract(operation).is_some()")
        .next()
        .unwrap();
    for marker in [
        "MAIN_GENESIS_PROJECT_AGENTS_READ_OPERATION",
        "MAIN_CHARTER_READ_OPERATION",
        "MAIN_CHARTER_READINESS_OPERATION",
        "MAIN_CHARTER_DIFF_OPERATION",
        "MAIN_CHARTER_APPROVAL_TARGET_OPERATION",
        "MAIN_INQUIRY_RUN_OPERATION",
        "\"discovery.read\"",
        "\"portfolio.read\"",
    ] {
        for source in [schemas, payloads, validators, dispatcher] {
            assert!(!source.contains(marker), "remaining hand layer: {marker}");
        }
    }
    let queries = include_str!("../../../services/src/main_orchestration_queries.rs");
    assert!(!queries.contains("pub async fn execute("));
    assert!(!queries.contains("parse_query"));
    for id in operation_registry::main_reads::IDS {
        assert!(
            READ_CATALOG.lookup(id).is_some(),
            "{id} must dispatch through the registry"
        );
    }
}

/// A forged authority field is refused exactly as before the operation was
/// registered: by the generic guard, before any contract detail is returned.
/// The generic scope surface never ran that guard; there the closed contract
/// is what refuses the field.
#[tokio::test]
async fn authority_fields_are_refused_by_the_guard_before_the_main_contract() {
    const GUARD: &str = "Forge orchestration scope and authority are server-derived";
    let inputs: Value = serde_json::from_str(include_str!(
        "../../../operation-registry/tests/read_inputs.json"
    ))
    .unwrap();
    for id in operation_registry::main_reads::IDS {
        let spec = READ_CATALOG.lookup(id).unwrap();
        let provider = Arc::new(RecordingProvider::default());
        let aggregate = spec.surfaces[0].native_aggregate;
        let guarded = aggregate == FORGE_MAIN_ORCHESTRATION_READ_TOOL;
        let tool = if guarded {
            ForgeScopeReadTool::named(
                "actor".into(),
                scope(CanonicalScopeType::AgentChat, WorkspaceAccess::Deny),
                vec![id.to_string()],
                provider.clone(),
                aggregate,
                "Main scope",
            )
        } else {
            assert_eq!(aggregate, "forge_scope_read");
            ForgeScopeReadTool::new(
                "actor".into(),
                scope(CanonicalScopeType::AgentChat, WorkspaceAccess::Deny),
                vec![id.to_string()],
                provider.clone(),
            )
        };
        let mut forged_field = inputs[*id].clone();
        forged_field["identity_id"] = json!("forged");
        let mut cases = vec![("identity_id".to_owned(), forged_field)];
        for field in spec.input.schema["properties"].as_object().unwrap().keys() {
            let mut nested = inputs[*id].clone();
            nested[field] = json!({"authority":"forged"});
            cases.push((field.clone(), nested));
        }
        for (field, input) in cases {
            for raw in [
                json!({"operation":id,"arguments":input}),
                json!({"parameters":{"operation":id,"arguments":input}}),
            ] {
                let canonical = tool.normalize_arguments(raw).unwrap();
                let error = tool
                    .prepare(canonical, &test_preparation_context("authority-contract"))
                    .await
                    .unwrap_err()
                    .to_string();
                if guarded {
                    assert!(
                        error.contains(GUARD) && !error.contains("expected"),
                        "{id} {field}: {error}"
                    );
                } else {
                    assert!(
                        error.contains(id) && error.contains(&field) && error.contains("expected"),
                        "{id} {field}: {error}"
                    );
                }
            }
        }
        // An operation this tool was not granted is denied before either.
        let error = tool
            .prepare(
                json!({"operation":"task.summary","arguments":{"unexpected":true}}),
                &test_preparation_context("ungranted"),
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("Forge read operation is outside this scope")
                && !error.contains("expected"),
            "{error}"
        );
        assert!(provider.0.lock().unwrap().is_empty());
    }
}

/// An integer sent as a string or a float is admitted at preparation; the
/// advertised aggregate schema still carries no per-field types to widen.
#[tokio::test]
async fn integer_spellings_are_admitted_at_preparation() {
    for (id, field, base) in [
        ("discovery.read", "limit", json!({})),
        ("portfolio.read", "limit", json!({})),
        (
            "charter.readiness",
            "expected_charter_version",
            json!({"charter_id":"c","revision_id":"r","content_digest":"d","render_digest":"d"}),
        ),
    ] {
        let spec = READ_CATALOG.lookup(id).unwrap();
        let provider = Arc::new(RecordingProvider::default());
        let tool = ForgeScopeReadTool::named(
            "actor".into(),
            scope(CanonicalScopeType::AgentChat, WorkspaceAccess::Deny),
            vec![id.to_string()],
            provider.clone(),
            spec.surfaces[0].native_aggregate,
            "test scope",
        );
        assert_eq!(
            tool.spec().input_schema["properties"]["arguments"]["type"],
            "object"
        );
        for spelling in [json!(3), json!("3"), json!(3.0)] {
            let mut input = base.clone();
            input[field] = spelling.clone();
            tool.prepare(
                json!({"operation":id,"arguments":input}),
                &test_preparation_context("integer-spelling"),
            )
            .await
            .unwrap_or_else(|error| panic!("{id} {spelling}: {error}"));
        }
        for malformed in [json!("three"), json!(1.5), json!(-1), json!(true)] {
            let mut input = base.clone();
            input[field] = malformed.clone();
            let error = tool
                .prepare(
                    json!({"operation":id,"arguments":input}),
                    &test_preparation_context("integer-malformed"),
                )
                .await
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(id) && error.contains(field) && error.contains("expected"),
                "{id} {malformed}: {error}"
            );
        }
    }
}

#[test]
fn each_registered_proposal_projects_only_its_own_fields() {
    for spec in operation_registry::PROPOSAL_CATALOG.iter() {
        let one = BTreeSet::from([spec.id.to_owned()]);
        let fields = coordination_payload_properties(&one).unwrap();
        assert_eq!(
            fields.as_object().unwrap().keys().collect::<Vec<_>>(),
            spec.input.schema["properties"]
                .as_object()
                .unwrap()
                .keys()
                .collect::<Vec<_>>(),
            "{}",
            spec.id
        );
        let tool = ForgeScopeProposeTool::new(
            "actor".into(),
            scope(CanonicalScopeType::Project, WorkspaceAccess::Deny),
            vec![spec.id.into()],
            Arc::new(RecordingProvider::default()),
        );
        let schema = tool.spec().input_schema;
        for field in ["dedupe_key", "correlation_id"] {
            assert_eq!(schema["properties"][field]["type"], "string");
        }
        assert_eq!(schema["properties"]["payload"]["type"], "object");
    }
}

#[tokio::test]
async fn moved_proposals_refuse_unknown_and_forged_envelope_fields_before_dispatch() {
    let inputs: Value = serde_json::from_str(include_str!(
        "../../../operation-registry/tests/project_inputs.json"
    ))
    .unwrap();
    for id in operation_registry::project_proposals::IDS
        .iter()
        .chain(operation_registry::legacy_proposals::IDS)
    {
        let tool = ForgeScopeProposeTool::named(
            "actor".into(),
            scope(CanonicalScopeType::Project, WorkspaceAccess::Deny),
            vec![(*id).into()],
            Arc::new(RecordingProvider::default()),
            FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
            "Project",
        );
        let args = json!({"operation":id,"payload":inputs[*id],"dedupe_key":"key","correlation_id":"correlation"});
        tool.prepare(args.clone(), &test_preparation_context("valid-proposal"))
            .await
            .unwrap();
        let mut unknown = args.clone();
        unknown["payload"]["unexpected"] = json!(true);
        let error = tool
            .prepare(unknown, &test_preparation_context("unknown-proposal"))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(id) && error.contains("unexpected"),
            "{error}"
        );
        for field in crate::operation_catalog::SERVER_DERIVED_FIELDS
            .iter()
            .chain(&["project_id"])
        {
            let mut forged = args.clone();
            forged[*field] = json!("forged");
            let error = tool
                .prepare(forged, &test_preparation_context("forged-proposal"))
                .await
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("server-derived") && !error.contains("expected"),
                "{id} {field}: {error}"
            );
        }
        let mut forged = args.clone();
        forged["extra"] = json!({"authority":"forged"});
        let error = tool
            .prepare(forged, &test_preparation_context("nested-forgery"))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("server-derived") && !error.contains("expected"));
    }
}
