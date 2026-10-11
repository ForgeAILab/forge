use super::tests::{scope, test_invocation_context, test_preparation_context};
use super::*;
use crate::OperationClassification;
use operation_registry::main_proposals::CATALOG;

#[derive(Debug, Default)]
struct Provider(std::sync::Mutex<Vec<Value>>);
#[async_trait]
impl ForgeToolProvider for Provider {
    async fn read(
        &self,
        _: &str,
        _: &CanonicalScope,
        _: &str,
        _: Value,
    ) -> Result<Value, AgentHostError> {
        unreachable!()
    }
    async fn proposal_denial(
        &self,
        actor: &str,
        _: &CanonicalScope,
        _: &str,
    ) -> Result<(), AgentHostError> {
        if actor == "unbound" {
            Err(AgentHostError::Authority("unbound Main identity".into()))
        } else {
            Ok(())
        }
    }
    async fn propose(
        &self,
        _: &str,
        _: &CanonicalScope,
        _: &str,
        _: &str,
        _: Value,
    ) -> Result<Value, AgentHostError> {
        panic!("prepared invocation must use the prepared boundary")
    }
    async fn propose_prepared(
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
fn tool(provider: Arc<Provider>, actor: &str, id: &str, aggregate: &str) -> ForgeScopeProposeTool {
    let scope = scope(CanonicalScopeType::AgentChat, WorkspaceAccess::Deny);
    if aggregate == "forge_scope_propose" {
        ForgeScopeProposeTool::new(actor.into(), scope, vec![id.into()], provider)
    } else {
        ForgeScopeProposeTool::named(
            actor.into(),
            scope,
            vec![id.into()],
            provider,
            FORGE_MAIN_ORCHESTRATION_PROPOSE_TOOL,
            "Main scope",
        )
    }
}
fn arguments(id: &str) -> Value {
    let payload = if id == "project.create" {
        json!({"action":"create_from_approval","approval_id":"approval"})
    } else {
        json!({"action":"select","expected_session_version":1,"project_agent_identity_id":"project-agent"})
    };
    json!({"operation":id,"payload":payload,"dedupe_key":"key","correlation_id":"corr","causation_id":"cause","causation_depth":1})
}
#[tokio::test]
async fn proposal_registry_normalizes_flat_and_parameters_envelopes_on_both_aggregates() {
    for spec in CATALOG.iter() {
        for binding in spec.surfaces {
            let provider = Arc::new(Provider::default());
            let tool = tool(provider.clone(), "actor", spec.id, binding.native_aggregate);
            let mut flat = arguments(spec.id);
            if spec.id == "genesis.project_agent.select" {
                flat["payload"].as_object_mut().unwrap().remove("action");
            }
            for raw in [
                flat.clone(),
                json!({"parameters":flat}),
                json!({"operation":spec.id,"parameters":{"payload":flat["payload"],"dedupe_key":"key","correlation_id":"corr","causation_id":"cause","causation_depth":1}}),
            ] {
                let canonical = tool.normalize_arguments(raw).unwrap();
                let prepared = tool
                    .prepare(canonical, &test_preparation_context("proposal-registry"))
                    .await
                    .unwrap();
                assert_eq!(prepared.arguments(), &flat);
                tool.invoke(prepared, &test_invocation_context("proposal-registry"))
                    .await
                    .unwrap();
            }
            assert_eq!(
                provider.0.lock().unwrap().as_slice(),
                &[flat.clone(), flat.clone(), flat]
            );
        }
    }
}
#[tokio::test]
async fn proposal_denial_precedes_forged_fields_then_contract_detail() {
    for spec in CATALOG.iter() {
        for binding in spec.surfaces {
            for actor in ["actor", "unbound"] {
                let provider = Arc::new(Provider::default());
                let tool = tool(provider.clone(), actor, spec.id, binding.native_aggregate);
                for field in ["identity_id", "authority"] {
                    let mut input = arguments(spec.id);
                    input["payload"] = json!({field:"forged"});
                    let error = tool
                        .prepare(input, &test_preparation_context("denial"))
                        .await
                        .unwrap_err()
                        .to_string();
                    assert!(
                        error.contains(if actor == "unbound" {
                            "unbound Main identity"
                        } else {
                            "scope and authority are server-derived"
                        }),
                        "{error}"
                    );
                    assert!(!error.contains("expected "), "{error}");
                }
                let mut malformed = arguments(spec.id);
                malformed["payload"] = json!(null);
                let error = tool
                    .prepare(malformed, &test_preparation_context("denial"))
                    .await
                    .unwrap_err()
                    .to_string();
                assert!(
                    error.contains(if actor == "unbound" {
                        "unbound Main identity"
                    } else {
                        spec.id
                    }),
                    "{error}"
                );
                assert_eq!(error.contains("expected "), actor == "actor");
                assert!(provider.0.lock().unwrap().is_empty());
            }
            let outside = tool(
                Arc::new(Provider::default()),
                "actor",
                "other",
                binding.native_aggregate,
            );
            let error = outside
                .prepare(arguments(spec.id), &test_preparation_context("scope"))
                .await
                .unwrap_err();
            assert!(error.to_string().contains("outside this scope"));
        }
    }
}
#[test]
fn proposal_authority_and_availability_match_base_catalog() {
    for spec in CATALOG.iter() {
        assert_eq!(
            spec.availability,
            operation_registry::AvailabilityRule::Always
        );
        for scope in [
            CanonicalScopeType::Account,
            CanonicalScopeType::AgentChat,
            CanonicalScopeType::Project,
            CanonicalScopeType::Task,
        ] {
            let permission = crate::operation_permission(scope, spec.id);
            assert_eq!(
                spec.authority.permission(scope_type_name(scope)),
                permission
            );
            for granted in [
                BTreeSet::new(),
                permission
                    .map(|p| BTreeSet::from([p.into()]))
                    .unwrap_or_default(),
            ] {
                assert_eq!(
                    permission.is_some_and(|permission| granted.contains(permission))
                        && matches!(
                            scope,
                            CanonicalScopeType::Account | CanonicalScopeType::AgentChat
                        ),
                    !filter_operations(
                        scope,
                        &[spec.id.into()],
                        &granted,
                        ProjectChatToolContext::default(),
                        None
                    )
                    .is_empty()
                );
            }
            if permission.is_some() {
                let expected = if spec.effect == operation_registry::EffectClass::DirectCommand {
                    OperationClassification::DirectCommand
                } else {
                    OperationClassification::ApprovalRequiredAction
                };
                assert_eq!(
                    crate::operation_descriptor(scope, spec.id, None).classification,
                    expected
                );
            }
        }
    }
}
#[test]
fn pre_change_prepared_fixture_fingerprints_are_exact() {
    let fixtures: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/main_proposals_pre_change.json"
    ))
    .unwrap();
    for (name, value) in fixtures.as_object().unwrap() {
        let prepared: PreparedToolCall = serde_json::from_value(value.clone()).unwrap();
        assert!(prepared.verify_fingerprint(), "{name}");
        assert_eq!(
            prepared.arguments()["operation"].as_str().unwrap(),
            name.split(':').next().unwrap()
        );
    }
}
