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
        let input = if spec.id == "skill.section" {
            json!({"section":"research"})
        } else {
            json!({})
        };
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
