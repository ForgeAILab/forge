use super::*;
use project_reads::SectionArguments;
use scope_reads::NoArguments;

#[test]
fn catalog_is_complete_unique_and_deterministic() {
    let ids = READ_CATALOG.iter().map(|s| s.id).collect::<Vec<_>>();
    assert_eq!(ids, registered_operations());
    assert_eq!(
        ids.len(),
        scope_reads::IDS.len() + project_reads::IDS.len() + main_reads::IDS.len()
    );
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
        let valid = valid_input(spec.id);
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
            "charter.approval_target: {charter_id, content_digest, expected_charter_version, genesis_session_id?, render_digest, revision_id}",
            "charter.diff: {base_revision_id, candidate_revision_id, charter_id, genesis_session_id?}",
            "charter.read: {charter_id?, genesis_session_id?, revision_id?}",
            "charter.readiness: {charter_id, content_digest, expected_charter_version, genesis_session_id?, render_digest, revision_id}",
            "discovery.read: {limit?}",
            "genesis.project_agents.read: {genesis_session_id?}",
            "inquiry.run: {context?, question, title}",
            "portfolio.read: {limit?}",
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
        let input = valid_input(spec.id);
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

fn valid_input(id: &str) -> Value {
    let inputs: Value = serde_json::from_str(include_str!("../tests/read_inputs.json")).unwrap();
    inputs[id].clone()
}

#[async_trait::async_trait]
impl main_reads::MainReadContext<&'static str> for RecordingContext {
    async fn genesis_project_agents(
        &self,
        _: main_reads::GenesisProjectAgentsQuery,
    ) -> Result<Value, &'static str> {
        self.record("genesis.project_agents.read")
    }
    async fn charter_read(&self, _: main_reads::CharterReadQuery) -> Result<Value, &'static str> {
        self.record("charter.read")
    }
    async fn charter_readiness(
        &self,
        _: main_reads::CharterProjectionQuery,
    ) -> Result<Value, &'static str> {
        self.record("charter.readiness")
    }
    async fn charter_diff(&self, _: main_reads::CharterDiffQuery) -> Result<Value, &'static str> {
        self.record("charter.diff")
    }
    async fn charter_approval_target(
        &self,
        _: main_reads::CharterProjectionQuery,
    ) -> Result<Value, &'static str> {
        self.record("charter.approval_target")
    }
    async fn discovery_read(&self, _: main_reads::BoundedListQuery) -> Result<Value, &'static str> {
        self.record("discovery.read")
    }
    async fn portfolio_read(&self, _: main_reads::BoundedListQuery) -> Result<Value, &'static str> {
        self.record("portfolio.read")
    }
    async fn inquiry_run(&self, _: main_reads::InquiryQuery) -> Result<Value, &'static str> {
        self.record("inquiry.run")
    }
}

#[test]
fn scalar_contract_violations_name_each_field_and_never_widen_the_schema() {
    for spec in READ_CATALOG.iter() {
        let valid = valid_input(spec.id);
        let schema = spec.canonical_schema();
        let validator = jsonschema::validator_for(&schema).unwrap();
        for (field, property) in schema["properties"].as_object().unwrap() {
            let mut cases = vec![json!([]), json!({}), json!(false), json!(1.5)];
            if property["type"] == "string" {
                cases.extend([json!(null), json!(42)]);
            }
            if let Some(min) = property["minLength"].as_u64() {
                if min > 0 {
                    cases.push(json!(""));
                }
            }
            if let Some(max) = property["maxLength"].as_u64() {
                cases.push(json!("é".repeat(max as usize + 1)));
            }
            if let Some(min) = property["minimum"].as_f64() {
                cases.push(json!(min as i64 - 1));
            }
            if property["format"] == "int64" {
                cases.push(json!(u64::MAX));
            }
            for value in cases {
                let mut invalid = valid.clone();
                invalid[field] = value;
                if validator.is_valid(&invalid) && property["format"] != "int64" {
                    continue;
                }
                let error = spec.validate_arguments(&invalid).unwrap_err();
                assert!(
                    error.contains(spec.id) && error.contains(field) && error.contains("expected"),
                    "{error}"
                );
            }
            if schema["required"]
                .as_array()
                .unwrap()
                .contains(&json!(field))
            {
                let mut invalid = valid.clone();
                invalid.as_object_mut().unwrap().remove(field);
                let error = spec.validate_arguments(&invalid).unwrap_err();
                assert!(
                    error.contains(spec.id) && error.contains(field) && error.contains("expected"),
                    "{error}"
                );
            }
        }
    }
}

#[test]
fn main_list_limits_preserve_defaults_null_and_clamping_inputs() {
    for id in ["discovery.read", "portfolio.read"] {
        let spec = READ_CATALOG.lookup(id).unwrap();
        for input in [
            json!({}),
            json!({"limit":null}),
            json!({"limit":0}),
            json!({"limit":100}),
        ] {
            spec.validate_arguments(&input).unwrap();
        }
    }
}
