use super::*;
use project_reads::SectionArguments;
use scope_reads::NoArguments;

#[test]
fn catalog_is_complete_unique_and_deterministic() {
    let ids = READ_CATALOG.iter().map(|s| s.id).collect::<Vec<_>>();
    assert_eq!(ids, MOVED_OPERATIONS);
    assert!(READ_CATALOG.lookup("project.current_state").is_none());
    assert!(OperationCatalog::<()>::new(scope_reads::specs(), MOVED_OPERATIONS).is_err());
    assert!(OperationCatalog::<()>::new(
        scope_reads::specs().into_iter().chain(scope_reads::specs()),
        &["account.summary", "agent_chat.summary"]
    )
    .unwrap_err_message()
    .contains("collision"));
}
// Avoid a Debug requirement on the erased handlers in error assertions.
trait ErrorMessage {
    fn unwrap_err_message(self) -> String;
}
impl<E> ErrorMessage for Result<OperationCatalog<E>, String> {
    fn unwrap_err_message(self) -> String {
        match self {
            Err(e) => e,
            Ok(_) => panic!("expected catalog error"),
        }
    }
}

#[test]
fn every_declared_constraint_matches_schema_and_decoder() {
    for spec in READ_CATALOG.iter() {
        let valid = if spec.id == "skill.section" {
            json!({"section":"research"})
        } else {
            json!({})
        };
        let validator = jsonschema::validator_for(&spec.canonical_schema()).unwrap();
        assert!(validator.is_valid(&valid), "{}", spec.id);
        spec.input.validate(&valid).unwrap();
        for invalid in [json!(null), json!([]), json!({"foreign_project":"x"})] {
            assert!(!validator.is_valid(&invalid), "{} {invalid}", spec.id);
            assert!(
                spec.input.validate(&invalid).is_err(),
                "{} {invalid}",
                spec.id
            );
        }
        if spec.id == "skill.section" {
            for invalid in [
                json!({}),
                json!({"section":null}),
                json!({"section":7}),
                json!({"section":"unknown"}),
            ] {
                assert!(!validator.is_valid(&invalid));
                assert!(spec.input.validate(&invalid).is_err());
            }
            for section in project_reads::SECTION_NAMES {
                spec.input.validate(&json!({"section":section})).unwrap();
            }
        }
    }
}

#[test]
fn canonical_schemas_are_pinned() {
    let snapshot: Value =
        serde_json::from_str(include_str!("../tests/read_contracts.json")).unwrap();
    for spec in READ_CATALOG.iter() {
        assert_eq!(spec.canonical_schema(), snapshot[spec.id], "{}", spec.id);
    }
}

#[test]
fn projection_only_contains_selected_bindings_and_preserves_hand_schema() {
    let schema = json!({"type":"object", "properties":{"operation":{"type":"string"}}, "required":["operation"]});
    let selected = BTreeSet::from([
        "skill.section".into(),
        "account.summary".into(),
        "work.read".into(),
    ]);
    let projected = READ_CATALOG.project_aggregate(
        schema.clone(),
        "forge_project_orchestration_read",
        &selected,
    );
    assert_eq!(projected["allOf"].as_array().unwrap().len(), 1);
    assert_eq!(projected["properties"], schema["properties"]);
    let validator = jsonschema::validator_for(&projected).unwrap();
    assert!(!validator.is_valid(&json!({"operation":"skill.section"})));
    assert!(
        validator.is_valid(&json!({"operation":"skill.section","arguments":{"section":"release"}}))
    );
    assert!(validator.is_valid(&json!({"operation":"work.read","arguments":{"limit":10}})));
    assert_eq!(
        READ_CATALOG.project_aggregate(schema.clone(), "forge_scope_read", &BTreeSet::new()),
        schema
    );
}

struct RecordingContext(std::sync::Mutex<Vec<String>>);
impl RecordingContext {
    fn record(&self, name: &str) -> Result<Value, &'static str> {
        self.0.lock().unwrap().push(name.to_owned());
        Ok(json!({"handler":name}))
    }
}
#[async_trait::async_trait]
impl scope_reads::ScopeReadContext<&'static str> for RecordingContext {
    async fn account_summary(&self, _: NoArguments) -> Result<Value, &'static str> {
        self.record("account.summary")
    }
    async fn chat_summary(&self, _: NoArguments) -> Result<Value, &'static str> {
        self.record("agent_chat.summary")
    }
}
#[async_trait::async_trait]
impl project_reads::ProjectReadContext<&'static str> for RecordingContext {
    async fn project_charter(&self, _: NoArguments) -> Result<Value, &'static str> {
        self.record("project.charter")
    }
    async fn skill_section(&self, input: SectionArguments) -> Result<Value, &'static str> {
        self.record(&format!("skill.section:{}", input.section.as_str()))
    }
}
#[tokio::test]
async fn typed_dispatch_selects_each_handler_and_never_dispatches_invalid_input() {
    let catalog = read_catalog();
    let context = RecordingContext(std::sync::Mutex::new(Vec::new()));
    for spec in catalog.iter() {
        let input = if spec.id == "skill.section" {
            json!({"section":"research"})
        } else {
            json!({})
        };
        let expected = if spec.id == "skill.section" {
            "skill.section:research"
        } else {
            spec.id
        };
        assert_eq!(
            spec.dispatch(&context, input).await.unwrap()["handler"],
            expected
        );
        assert!(matches!(
            spec.dispatch(&context, json!({"foreign_project":"x"}))
                .await,
            Err(DispatchError::InvalidInput(_))
        ));
    }
    assert_eq!(context.0.lock().unwrap().len(), MOVED_OPERATIONS.len());
}
