use super::*;
use project_reads::SectionArguments;
use scope_reads::NoArguments;

#[test]
fn catalog_is_complete_unique_and_deterministic() {
    let ids = READ_CATALOG.iter().map(|s| s.id).collect::<Vec<_>>();
    assert_eq!(ids, registered_operations());
    assert_eq!(ids.len(), scope_reads::IDS.len() + project_reads::IDS.len());
    assert!(READ_CATALOG.lookup("project.current_state").is_none());
    assert!(OperationCatalog::<()>::new(scope_reads::specs(), &registered_operations()).is_err());
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
        spec.validate_arguments(&valid).unwrap();
        for invalid in [json!(null), json!([]), json!({"foreign_project":"x"})] {
            assert!(!validator.is_valid(&invalid), "{} {invalid}", spec.id);
            assert!(
                spec.validate_arguments(&invalid).is_err(),
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
                assert!(spec.validate_arguments(&invalid).is_err());
            }
            for section in project_reads::SECTION_NAMES {
                spec.validate_arguments(&json!({"section":section}))
                    .unwrap();
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

/// The line a model is shown is generated from the spec. Pinned here in full:
/// it is the whole advertised argument contract of a registered operation.
#[test]
fn contract_lines_are_generated_from_the_spec() {
    let lines = READ_CATALOG
        .iter()
        .map(OperationSpec::contract_line)
        .collect::<Vec<_>>();
    assert_eq!(
        lines,
        [
            "account.summary: no arguments",
            "agent_chat.summary: no arguments",
            "project.charter: no arguments",
            "skill.section: {section: one of research|documents|scope_change|tasks|milestones|release}",
        ]
    );
    // Derived from the canonical schema, so the line cannot state a field,
    // requiredness or value set the validator does not enforce.
    for spec in READ_CATALOG.iter() {
        let schema = spec.canonical_schema();
        for (field, property) in schema["properties"].as_object().unwrap() {
            assert!(spec.contract_line().contains(field.as_str()), "{}", spec.id);
            for value in property["enum"].as_array().into_iter().flatten() {
                assert!(spec.contract_line().contains(value.as_str().unwrap()));
            }
        }
    }
    let optional = TypedInputContract {
        rust_type: "test",
        schema: json!({"properties":{"limit":{"type":"integer"},"task_id":{"type":"string"}},"required":["task_id"]}),
        constraints: &[],
        decode: |_| Ok(()),
    };
    assert_eq!(optional.contract_line(), "{limit?, task_id}");
}

#[test]
fn a_violation_names_the_operation_the_field_and_the_contract() {
    let section = READ_CATALOG.lookup("skill.section").unwrap();
    assert_eq!(
        section.validate_arguments(&json!({})).unwrap_err(),
        "skill.section: argument `section` is required; expected skill.section: {section: one of research|documents|scope_change|tasks|milestones|release}"
    );
    assert_eq!(
        section.validate_arguments(&json!({"section":"unknown"})).unwrap_err(),
        "skill.section: argument `section` must be one of: research, documents, scope_change, tasks, milestones, release; expected skill.section: {section: one of research|documents|scope_change|tasks|milestones|release}"
    );
    assert_eq!(
        READ_CATALOG
            .lookup("project.charter")
            .unwrap()
            .validate_arguments(&json!({"project_id":"x"}))
            .unwrap_err(),
        "project.charter: argument `project_id` is not admitted; expected project.charter: no arguments"
    );
    assert_eq!(
        READ_CATALOG
            .lookup("account.summary")
            .unwrap()
            .validate_arguments(&json!(null))
            .unwrap_err(),
        "account.summary: arguments must be an object; expected account.summary: no arguments"
    );
}

/// The declared constraints restate the Serde type; they must not widen or
/// narrow it. The section list is also the doctrine section list.
#[test]
fn declared_constraints_agree_with_the_serde_type() {
    let derive = |settings: schemars::gen::SchemaSettings| settings.into_generator();
    let settings = || schemars::gen::SchemaSettings::draft07().with(|s| s.inline_subschemas = true);
    let derived =
        serde_json::to_value(derive(settings()).into_root_schema_for::<NoArguments>()).unwrap();
    assert_eq!(derived["additionalProperties"], json!(false));
    assert!(derived.get("properties").is_none());
    let derived =
        serde_json::to_value(derive(settings()).into_root_schema_for::<SectionArguments>())
            .unwrap();
    assert_eq!(derived["additionalProperties"], json!(false));
    assert_eq!(derived["required"], json!(["section"]));
    assert_eq!(
        derived["properties"]["section"]["enum"],
        json!(project_reads::SECTION_NAMES)
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
    assert_eq!(
        context.0.lock().unwrap().len(),
        registered_operations().len()
    );
}
