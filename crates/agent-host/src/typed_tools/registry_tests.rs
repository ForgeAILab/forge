use super::tests::{scope, test_invocation_context, test_preparation_context};
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
        let schema = tool.spec().input_schema;
        let validator = jsonschema::validator_for(&schema).unwrap();
        for raw in [
            flat.clone(),
            json!({"parameters":flat}),
            json!({"operation":spec.id,"parameters":{"arguments":input}}),
        ] {
            let canonical = tool.normalize_arguments(raw).unwrap();
            assert!(validator.is_valid(&canonical), "{} {canonical}", spec.id);
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
        assert_eq!(
            provider.0.lock().unwrap().as_slice(),
            vec![(spec.id.to_owned(), input); 3]
        );
        let invalid = json!({"operation":spec.id,"arguments":{"foreign_project":"x"}});
        assert!(!validator.is_valid(&invalid));
        assert!(
            tool.prepare(invalid, &test_preparation_context("invalid"))
                .await
                .is_err()
        );
    }
}
