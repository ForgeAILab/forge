use super::tests::{all_permissions, scope, test_invocation_context, test_preparation_context};
use super::*;
use operation_registry::READ_CATALOG;

#[test]
fn spec_authority_agrees_with_existing_permission_check_on_every_surface() {
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
            for granted in [
                BTreeSet::new(),
                existing
                    .map(|p| BTreeSet::from([p.to_owned()]))
                    .unwrap_or_default(),
            ] {
                assert_eq!(
                    spec.authority.allows(scope_type_name(scope_type), &granted),
                    !filter_operations(scope_type, &[spec.id.to_owned()], &granted).is_empty(),
                    "{} {scope_type:?}",
                    spec.id
                );
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
        for source in [schemas, validators, dispatcher] {
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

#[tokio::test]
async fn main_contract_errors_name_authority_fields_before_the_generic_guard() {
    let inputs: Value = serde_json::from_str(include_str!(
        "../../../operation-registry/tests/read_inputs.json"
    ))
    .unwrap();
    for id in operation_registry::main_reads::IDS {
        let spec = READ_CATALOG.lookup(id).unwrap();
        let provider = Arc::new(RecordingProvider::default());
        let mut tool = ForgeScopeReadTool::named(
            "actor".into(),
            scope(CanonicalScopeType::AgentChat, WorkspaceAccess::Deny),
            vec![id.to_string()],
            provider.clone(),
            spec.surfaces[0].native_aggregate,
            "Main scope",
        );
        // These two use the generic scope surface in production.
        if spec.surfaces[0].native_aggregate == "forge_scope_read" {
            tool.reject_authority_overrides = false;
        }
        let mut cases = vec![("identity_id".to_owned(), inputs[*id].clone())];
        cases[0].1["identity_id"] = json!("forged");
        for field in spec.input.schema["properties"].as_object().unwrap().keys() {
            let mut input = inputs[*id].clone();
            input[field] = json!({"authority":"forged"});
            cases.push((field.clone(), input));
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
                assert!(
                    error.contains(id) && error.contains(&field) && error.contains("expected"),
                    "{error}"
                );
            }
        }
        assert!(provider.0.lock().unwrap().is_empty());
    }
}
