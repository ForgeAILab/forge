use super::*;
use project_reads::SectionArguments;
use scope_reads::NoArguments;

#[test]
fn catalog_is_complete_unique_and_deterministic() {
    let ids = READ_CATALOG.iter().map(|s| s.id).collect::<Vec<_>>();
    assert_eq!(ids, registered_reads());
    assert_eq!(
        registered_operations().len(),
        registered_reads().len()
            + main_proposals::IDS.len()
            + project_proposals::IDS.len()
            + legacy_proposals::IDS.len()
            + hand_proposals::IDS.len()
    );
    assert_eq!(
        ids.len(),
        scope_reads::IDS.len() + project_reads::IDS.len() + main_reads::IDS.len()
    );
    assert!(READ_CATALOG.lookup("project.current_state").is_some());
    assert!(OperationCatalog::<()>::new(scope_reads::specs(), &registered_reads()).is_err());
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
            "project.current_state: {limit?}",
            "project.observations: {limit?, task_id?}",
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
    /// Also reports the integer the typed handler was handed.
    fn record_decoded(
        &self,
        name: &str,
        integer: impl serde::Serialize,
    ) -> Result<Value, &'static str> {
        let mut recorded = self.record(name)?;
        recorded["integer"] = json!(integer);
        Ok(recorded)
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
    async fn current_state(
        &self,
        input: project_reads::CurrentStateArguments,
    ) -> Result<Value, &'static str> {
        self.record_decoded("project.current_state", input.limit)
    }
    async fn observations(
        &self,
        input: project_reads::ObservationsArguments,
    ) -> Result<Value, &'static str> {
        self.record_decoded("project.observations", input.limit)
    }
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
    assert_eq!(context.0.lock().unwrap().len(), registered_reads().len());
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
        input: main_reads::CharterProjectionQuery,
    ) -> Result<Value, &'static str> {
        self.record_decoded("charter.readiness", input.expected_charter_version)
    }
    async fn charter_diff(&self, _: main_reads::CharterDiffQuery) -> Result<Value, &'static str> {
        self.record("charter.diff")
    }
    async fn charter_approval_target(
        &self,
        input: main_reads::CharterProjectionQuery,
    ) -> Result<Value, &'static str> {
        self.record_decoded("charter.approval_target", input.expected_charter_version)
    }
    async fn discovery_read(
        &self,
        input: main_reads::BoundedListQuery,
    ) -> Result<Value, &'static str> {
        self.record_decoded("discovery.read", input.limit)
    }
    async fn portfolio_read(
        &self,
        input: main_reads::BoundedListQuery,
    ) -> Result<Value, &'static str> {
        self.record_decoded("portfolio.read", input.limit)
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

/// Every integer field of every registered operation, with a value its
/// bounds admit.
fn registered_integer_fields() -> Vec<(&'static str, String, u64)> {
    let mut fields = Vec::new();
    for spec in READ_CATALOG.iter() {
        for (field, property) in spec.input.schema["properties"].as_object().unwrap() {
            let integer = match &property["type"] {
                Value::String(kind) => kind == "integer",
                Value::Array(kinds) => kinds.contains(&json!("integer")),
                _ => false,
            };
            if integer {
                let admitted = property["minimum"].as_f64().unwrap_or(0.0).max(7.0) as u64;
                fields.push((spec.id, field.clone(), admitted));
            }
        }
    }
    fields
}

#[tokio::test]
async fn integer_fields_decode_integer_valued_strings_and_floats_as_the_integer() {
    let fields = registered_integer_fields();
    assert_eq!(
        fields
            .iter()
            .map(|(id, field, _)| (*id, field.as_str()))
            .collect::<Vec<_>>(),
        [
            ("charter.approval_target", "expected_charter_version"),
            ("charter.readiness", "expected_charter_version"),
            ("discovery.read", "limit"),
            ("portfolio.read", "limit"),
            ("project.current_state", "limit"),
            ("project.observations", "limit"),
        ]
    );
    let catalog = read_catalog();
    let context = RecordingContext(std::sync::Mutex::new(Vec::new()));
    let mut dispatched = 0;
    for (id, field, admitted) in &fields {
        let spec = catalog.lookup(id).unwrap();
        let advertised = spec.canonical_schema();
        for spelling in [
            json!(admitted),
            json!(admitted.to_string()),
            json!(*admitted as f64),
        ] {
            let mut input = valid_input(id);
            input[field] = spelling.clone();
            let normalized = spec.normalize_arguments(&input).unwrap();
            assert!(normalized[field].is_u64(), "{id} {field} {spelling}");
            assert_eq!(
                normalized[field],
                json!(admitted),
                "{id} {field} {spelling}"
            );
            // The handler receives the integer, not the spelling.
            let outcome = spec.dispatch(&context, input).await.unwrap();
            assert_eq!(
                outcome["integer"],
                json!(admitted),
                "{id} {field} {spelling}"
            );
            dispatched += 1;
        }
        // Coercion widens neither the canonical schema nor the contract line.
        assert_eq!(advertised, spec.canonical_schema());
        let kinds = &advertised["properties"][field]["type"];
        assert!(
            kinds == "integer" || kinds == &json!(["integer", "null"]),
            "{id} {field} {kinds}"
        );
    }
    assert_eq!(context.0.lock().unwrap().len(), dispatched);
}

#[tokio::test]
async fn malformed_integers_are_refused_with_the_named_contract_error() {
    let catalog = read_catalog();
    let context = RecordingContext(std::sync::Mutex::new(Vec::new()));
    for (id, field, _) in registered_integer_fields() {
        let spec = catalog.lookup(id).unwrap();
        for malformed in [
            json!("ten"),
            json!(""),
            json!(" 10"),
            json!("10.0"),
            json!("1.5"),
            json!(1.5),
            json!(-1),
            json!(-1.0),
            json!("-1"),
            json!(1e300),
            json!(true),
            json!(false),
            json!([10]),
            json!({"value":10}),
        ] {
            let mut input = valid_input(id);
            input[&field] = malformed.clone();
            let error = match spec.dispatch(&context, input).await {
                Err(DispatchError::InvalidInput(error)) => error,
                other => panic!("{id} {field} {malformed}: {other:?}"),
            };
            assert!(
                error.starts_with(&format!("{id}: argument `{field}` "))
                    && error.ends_with(&format!("; expected {}", spec.contract_line())),
                "{error}"
            );
        }
    }
    assert!(context.0.lock().unwrap().is_empty());
    let discovery = catalog.lookup("discovery.read").unwrap();
    assert_eq!(
        discovery
            .validate_arguments(&json!({"limit":"ten"}))
            .unwrap_err(),
        "discovery.read: argument `limit` must have type [\"integer\",\"null\"]; expected discovery.read: {limit?}"
    );
    assert_eq!(
        discovery.validate_arguments(&json!({"limit":-1})).unwrap_err(),
        "discovery.read: argument `limit` must be a non-negative integer; expected discovery.read: {limit?}"
    );
    let readiness = catalog.lookup("charter.readiness").unwrap();
    let mut stale = valid_input("charter.readiness");
    stale["expected_charter_version"] = json!("0");
    assert!(readiness
        .validate_arguments(&stale)
        .unwrap_err()
        .starts_with("charter.readiness: argument `expected_charter_version` violates minimum 1;"));
}

/// A field that also admits strings or numbers keeps what it was sent.
#[test]
fn only_integer_fields_are_coerced() {
    let contract = |kinds: Value| TypedInputContract {
        rust_type: "test",
        schema: json!({"properties":{"value":{"type":kinds}},"required":[]}),
        constraints: &[],
        decode: |_| Ok(()),
    };
    let sent = json!({"value":"10"});
    assert_eq!(
        contract(json!("integer")).normalize(&sent).unwrap(),
        json!({"value":10})
    );
    for kinds in [json!("string"), json!(["integer", "string"])] {
        assert_eq!(contract(kinds).normalize(&sent).unwrap(), sent);
    }
    let sent = json!({"value":10.0});
    assert_eq!(contract(json!("number")).normalize(&sent).unwrap(), sent);
    assert!(contract(json!("boolean")).normalize(&sent).is_err());
}

/// The CHANGELOG `Breaking` list for the Main reads, one example each.
#[test]
fn inputs_that_stay_refused() {
    let refused = |id: &str, input: Value, field: &str| {
        let spec = READ_CATALOG.lookup(id).unwrap();
        let error = spec.validate_arguments(&input).unwrap_err();
        assert!(
            error.starts_with(&format!("{id}: "))
                && error.contains(field)
                && error.ends_with(&format!("; expected {}", spec.contract_line())),
            "{error}"
        );
    };
    // Unknown fields.
    refused("discovery.read", json!({"unexpected":true}), "`unexpected`");
    refused("portfolio.read", json!({"unexpected":true}), "`unexpected`");
    refused("charter.read", json!({"limit":1}), "`limit`");
    refused(
        "inquiry.run",
        json!({"title":"Q","question":"Q","unexpected":true}),
        "`unexpected`",
    );
    // Non-object and sequence-form arguments.
    for id in main_reads::IDS {
        for input in [
            json!(null),
            json!("text"),
            json!(10),
            json!([]),
            json!(["charter", "revision", "genesis"]),
        ] {
            refused(id, input, "arguments must be an object");
        }
    }
    // Wrong-typed strings.
    for context in [json!(42), json!(true), json!(["x"]), json!({"x":1})] {
        refused(
            "inquiry.run",
            json!({"title":"Q","question":"Q","context":context}),
            "`context`",
        );
    }
    refused("charter.read", json!({"charter_id":42}), "`charter_id`");
    // Malformed limits.
    for limit in [json!("ten"), json!(1.5), json!(-1), json!(true)] {
        refused("discovery.read", json!({"limit":limit}), "`limit`");
        refused("portfolio.read", json!({"limit":limit}), "`limit`");
    }
    // Over-length and empty inquiry text.
    let inquiry = |title: String, question: String, context: Option<String>| json!({"title":title,"question":question,"context":context});
    refused(
        "inquiry.run",
        inquiry("t".repeat(121), "Q".into(), None),
        "`title` violates maxLength 120",
    );
    refused(
        "inquiry.run",
        inquiry("T".into(), "q".repeat(4001), None),
        "`question` violates maxLength 4000",
    );
    refused(
        "inquiry.run",
        inquiry("T".into(), "Q".into(), Some("c".repeat(8001))),
        "`context` violates maxLength 8000",
    );
    refused(
        "inquiry.run",
        inquiry(String::new(), "Q".into(), None),
        "`title` violates minLength 1",
    );
    // At the bound, and blank after trimming, still reach the handler.
    let spec = READ_CATALOG.lookup("inquiry.run").unwrap();
    spec.validate_arguments(&inquiry(
        "t".repeat(120),
        "q".repeat(4000),
        Some("c".repeat(8000)),
    ))
    .unwrap();
    spec.validate_arguments(&inquiry("  ".into(), "  ".into(), Some(String::new())))
        .unwrap();
}

#[async_trait::async_trait]
impl main_proposals::MainProposalContext<&'static str> for RecordingContext {
    async fn select_project_agent(
        &self,
        input: main_proposals::ProjectAgentSelection,
    ) -> Result<Value, &'static str> {
        self.record_decoded(
            "genesis.project_agent.select",
            input.expected_session_version,
        )
    }
    async fn propose_project_create(
        &self,
        _: main_proposals::ProjectCreate,
    ) -> Result<Value, &'static str> {
        self.record("project.create")
    }
}

#[test]
fn main_proposal_schemas_and_contract_lines_match_gate_fixtures() {
    let fixture: Value =
        serde_json::from_str(include_str!("../tests/main_proposal_contracts.json")).unwrap();
    let catalog = &main_proposals::CATALOG;
    assert_eq!(
        catalog.iter().map(|s| s.id).collect::<Vec<_>>(),
        main_proposals::IDS
    );
    for spec in catalog.iter() {
        assert_eq!(spec.canonical_schema(), fixture[spec.id]);
        assert!(spec
            .surfaces
            .iter()
            .all(|s| s.projection == FieldProjection::ProposalPayload));
    }
    assert_eq!(catalog.lookup("genesis.project_agent.select").unwrap().contract_line(), "genesis.project_agent.select: {expected_session_version, genesis_session_id?, project_agent_identity_id}");
    assert_eq!(
        catalog.lookup("project.create").unwrap().contract_line(),
        "project.create: {approval_id}"
    );
}
fn proposal_input(id: &str) -> Value {
    match id {
        "genesis.project_agent.select" => {
            json!({"action":"select","expected_session_version":1,"project_agent_identity_id":"project-agent"})
        }
        "project.create" => json!({"action":"create_from_approval","approval_id":"approval"}),
        _ => unreachable!(),
    }
}
#[tokio::test]
async fn main_proposal_contracts_check_every_field_and_dispatch_typed_inputs() {
    let catalog = main_proposals::catalog();
    let context = RecordingContext(std::sync::Mutex::new(Vec::new()));
    for spec in catalog.iter() {
        let input = proposal_input(spec.id);
        assert_eq!(
            spec.dispatch(&context, input.clone()).await.unwrap()["handler"],
            spec.id
        );
        let mut invalids = vec![
            ("arguments".to_owned(), json!(null)),
            ("arguments".to_owned(), json!([])),
        ];
        for field in spec.input.schema["properties"].as_object().unwrap().keys() {
            for value in [json!([]), json!({}), json!(false), json!(1.5)] {
                let mut invalid = input.clone();
                invalid[field] = value;
                invalids.push((field.clone(), invalid));
            }
            if spec.input.is_required(field) {
                let mut invalid = input.clone();
                invalid.as_object_mut().unwrap().remove(field);
                invalids.push((field.clone(), invalid));
            }
        }
        for (field, input) in invalids {
            let error = spec.normalize_arguments(&input).unwrap_err();
            assert!(
                error.starts_with(spec.id)
                    && error.contains(&field)
                    && error.ends_with(&format!("; expected {}", spec.contract_line())),
                "{error}"
            );
        }
        for field in ["unexpected", "ignored", "name", "slug"] {
            let mut invalid = input.clone();
            invalid[field] = json!("ignored before");
            if spec.id == "project.create" {
                spec.validate_arguments(&invalid).unwrap();
            } else {
                assert!(spec
                    .validate_arguments(&invalid)
                    .unwrap_err()
                    .contains(field));
            }
        }
    }
    let select = catalog.lookup("genesis.project_agent.select").unwrap();
    for value in [json!(1), json!("1"), json!(1.0)] {
        let mut input = proposal_input(select.id);
        input["expected_session_version"] = value;
        assert_eq!(
            select.dispatch(&context, input).await.unwrap()["integer"],
            1
        );
    }
    let create = catalog.lookup("project.create").unwrap();
    let mut old = proposal_input("project.create");
    old["ignored"] = json!("historic");
    assert!(create.validate_arguments(&old).is_ok());
    assert_eq!(
        create.dispatch_prepared(&context, old).await.unwrap()["handler"],
        "project.create"
    );
    // A blank reference is refused on a new call. An action prepared before
    // this contract replays its stored arguments without the contract check,
    // and the unchanged user executor still refuses it at execution.
    for stored in [
        json!({}),
        json!({"approval_id":null}),
        json!({"approval_id":""}),
    ] {
        let error = create.normalize_arguments(&stored).unwrap_err();
        assert!(
            error.starts_with("project.create: argument `approval_id`")
                && error.ends_with("; expected project.create: {approval_id}"),
            "{error}"
        );
        assert_eq!(
            create.dispatch_prepared(&context, stored).await.unwrap()["handler"],
            "project.create"
        );
    }
}

#[test]
fn ignored_action_fields_preserve_the_base_handler_acceptance_set() {
    for id in main_proposals::IDS {
        let spec = main_proposals::CATALOG.lookup(id).unwrap();
        assert!(spec.canonical_schema()["properties"]
            .get("action")
            .is_none());
        let mut without = proposal_input(id);
        without.as_object_mut().unwrap().remove("action");
        spec.validate_arguments(&without).unwrap();
        for value in [
            json!(null),
            json!(false),
            json!(12),
            json!([]),
            json!({"ignored":true}),
            json!("other action"),
        ] {
            let mut input = without.clone();
            input["action"] = value;
            let normalized = spec.normalize_arguments(&input).unwrap();
            if *id == "genesis.project_agent.select" {
                assert_eq!(normalized, without);
            } else {
                assert_eq!(normalized, input);
            }
        }
    }
}

#[async_trait::async_trait]
impl project_proposals::ProjectProposalContext<&'static str> for RecordingContext {
    async fn review_config(
        &self,
        _: project_proposals::ReviewConfig,
    ) -> Result<Value, &'static str> {
        self.record("project.review_config")
    }
    async fn document(&self, _: project_proposals::Document) -> Result<Value, &'static str> {
        self.record("project.document")
    }
    async fn decision(&self, _: project_proposals::Decision) -> Result<Value, &'static str> {
        self.record("project.decision")
    }
    async fn milestone(&self, _: project_proposals::Milestone) -> Result<Value, &'static str> {
        self.record("project.milestone")
    }
    async fn validation(&self, _: project_proposals::Validation) -> Result<Value, &'static str> {
        self.record("project.validation")
    }
    async fn release_request(
        &self,
        _: project_proposals::ReleaseRequest,
    ) -> Result<Value, &'static str> {
        self.record("project.release.request")
    }
    async fn escalate(&self, _: project_proposals::Escalation) -> Result<Value, &'static str> {
        self.record("project.escalate")
    }
}
#[async_trait::async_trait]
impl legacy_proposals::LegacyProposalContext<&'static str> for RecordingContext {
    async fn pending_message(&self, _: legacy_proposals::Message) -> Result<Value, &'static str> {
        self.record("message.send")
    }
    async fn pending_commitment(
        &self,
        _: legacy_proposals::Commitment,
    ) -> Result<Value, &'static str> {
        self.record("commitment.update")
    }
    async fn pending_memory_publish(
        &self,
        _: legacy_proposals::Memory,
    ) -> Result<Value, &'static str> {
        self.record("memory.publish")
    }
    async fn pending_memory_supersede(
        &self,
        _: legacy_proposals::Memory,
    ) -> Result<Value, &'static str> {
        self.record("memory.supersede")
    }
    async fn pending_review(&self, _: legacy_proposals::Review) -> Result<Value, &'static str> {
        self.record("review.request")
    }
    async fn pending_session(&self, _: legacy_proposals::Session) -> Result<Value, &'static str> {
        self.record("session.action")
    }
}

#[tokio::test]
async fn project_contracts_close_fields_and_pending_payloads_stay_open() {
    let inputs: Value = serde_json::from_str(include_str!("../tests/project_inputs.json")).unwrap();
    let catalog = proposal_catalog();
    let context = RecordingContext(std::sync::Mutex::new(Vec::new()));
    for id in project_proposals::IDS.iter().chain(legacy_proposals::IDS) {
        let spec = catalog.lookup(id).unwrap();
        let input = inputs[*id].clone();
        assert!(
            jsonschema::validator_for(&spec.canonical_schema())
                .unwrap()
                .is_valid(&input),
            "{id}"
        );
        assert_eq!(
            spec.normalize_arguments(&input).unwrap(),
            input,
            "{id} preserves receipt bytes"
        );
        assert_eq!(
            spec.dispatch(&context, input.clone()).await.unwrap()["handler"],
            *id
        );
        let mut unknown = input.clone();
        unknown["unexpected"] = json!(true);
        if legacy_proposals::IDS.contains(id) {
            // A pending proposal's payload was always an open object that
            // is stored as sent; undeclared fields are kept, not refused.
            assert_eq!(
                spec.normalize_arguments(&unknown).unwrap(),
                unknown,
                "{id} keeps undeclared pending fields"
            );
        } else {
            let error = spec.normalize_arguments(&unknown).unwrap_err();
            assert!(
                error.contains("unexpected") && error.contains(id),
                "{error}"
            );
        }
        // Historical preparations skip the new field contract, just as the
        // old hand handlers ignored fields they did not consume.
        assert!(
            spec.dispatch_prepared(&context, unknown).await.is_ok(),
            "{id}"
        );
        for field in spec.input.schema["properties"].as_object().unwrap().keys() {
            assert!(spec.contract_line().contains(field), "{id}: {field}");
        }
    }
}

#[test]
fn project_variants_refuse_unadvertised_fields_and_enforce_limits() {
    let inputs: Value = serde_json::from_str(include_str!("../tests/project_inputs.json")).unwrap();
    let doc = project_proposals::CATALOG
        .lookup("project.document")
        .unwrap();
    let mut draft = inputs["project.document"].clone();
    draft["revision_id"] = json!("other-action");
    assert!(doc
        .normalize_arguments(&draft)
        .unwrap_err()
        .contains("revision_id"));
    let approval = json!({"action":"approve","document_id":"d","revision_id":"r","content_digest":"c","render_digest":"r","expected_document_version":1});
    doc.validate_arguments(&approval).unwrap();
    for field in ["content", "base_revision_id"] {
        let mut wrong = approval.clone();
        wrong[field] = json!("draft-only");
        assert!(doc.validate_arguments(&wrong).is_err(), "{field}");
    }
    // `approve` was advertised with `kind`, `title` and `envelope_digest`
    // before the registry move; a caller that still sends them is accepted.
    let mut advertised = approval.clone();
    advertised["kind"] = json!("design");
    advertised["title"] = json!("Design");
    advertised["envelope_digest"] = Value::Null;
    assert_eq!(doc.normalize_arguments(&advertised).unwrap(), advertised);
    let milestone = project_proposals::CATALOG
        .lookup("project.milestone")
        .unwrap();
    let primary =
        json!({"action":"set_primary","expected_milestone_version":1,"primary_milestone_id":null});
    milestone.validate_arguments(&primary).unwrap();
    // The milestone fields were advertised for every action before the
    // registry move, so they stay accepted across actions. Fields that were
    // never advertised for the operation are still refused.
    let mut advertised = primary.clone();
    advertised["content"] = json!({"name":"N","outcome":"O"});
    advertised["milestone_id"] = Value::Null;
    advertised["display_label"] = json!("Primary");
    assert_eq!(
        milestone.normalize_arguments(&advertised).unwrap(),
        advertised
    );
    let mut define = inputs["project.milestone"].clone();
    define["milestone_id"] = Value::Null;
    define["primary_milestone_id"] = Value::Null;
    milestone.validate_arguments(&define).unwrap();
    let mut wrong = primary;
    wrong["base_revision_id"] = json!("revise-only");
    assert!(milestone.validate_arguments(&wrong).is_err());
    let validation = project_proposals::CATALOG
        .lookup("project.validation")
        .unwrap();
    let mut governed = inputs["project.validation"].clone();
    governed["governing_revision_ids"] = json!(["charter-revision"]);
    assert_eq!(validation.normalize_arguments(&governed).unwrap(), governed);
    let ci = project_proposals::CATALOG
        .lookup("project.review_config")
        .unwrap();
    for steps in [
        json!(["cargo test", "cargo test"]),
        json!(["x".repeat(2049)]),
        json!(vec!["x"; 17]),
    ] {
        let mut input = inputs[ci.id].clone();
        input["ci_steps"] = steps;
        assert!(ci.validate_arguments(&input).is_err());
    }
    let mut input = inputs[ci.id].clone();
    input["setup_steps"] = Value::Null;
    assert!(
        ci.validate_arguments(&input).is_err(),
        "omitted setup is accepted, explicit null is not"
    );
    let pending = legacy_proposals::CATALOG.lookup("message.send").unwrap();
    let utf8 = json!({"content":"é".repeat(32768)});
    assert!(pending
        .validate_arguments(&utf8)
        .unwrap_err()
        .contains("serialized UTF-8 bytes"));
    pending
        .validate_arguments(&json!({"content":"é".repeat(32750)}))
        .unwrap();
}

#[async_trait::async_trait]
impl hand_proposals::HandProposalContext<&'static str> for RecordingContext {
    async fn genesis_start(&self, _: hand_proposals::GenesisStart) -> Result<Value, &'static str> {
        self.record("genesis.start")
    }
    async fn charter_draft(&self, _: hand_proposals::CharterDraft) -> Result<Value, &'static str> {
        self.record("charter.draft")
    }
}
#[tokio::test]
async fn hand_contracts_decode_close_fields_and_preserve_receipt_inputs() {
    let inputs: Value = serde_json::from_str(include_str!("../tests/hand_inputs.json")).unwrap();
    let context = RecordingContext(std::sync::Mutex::new(Vec::new()));
    let catalog = proposal_catalog();
    for id in hand_proposals::IDS {
        let spec = catalog.lookup(id).unwrap();
        let mut input = inputs[*id].clone();
        input.as_object_mut().unwrap().remove("action");
        assert_eq!(spec.normalize_arguments(&input).unwrap(), input);
        assert_eq!(
            spec.dispatch(&context, input.clone()).await.unwrap()["handler"],
            *id
        );
        let mut unknown = input.clone();
        unknown["unexpected"] = json!(true);
        let error = spec.normalize_arguments(&unknown).unwrap_err();
        assert_eq!(
            error,
            format!(
                "{id}: argument `unexpected` is not admitted; expected {}",
                spec.contract_line()
            )
        );
        assert!(spec.dispatch_prepared(&context, unknown).await.is_ok());
    }
}

#[test]
fn hand_main_payload_byte_limits_and_ignored_actions_are_explicit() {
    let inputs: Value = serde_json::from_str(include_str!("../tests/hand_inputs.json")).unwrap();
    for id in hand_proposals::IDS {
        let catalog = proposal_catalog::<std::convert::Infallible>();
        let spec = catalog.lookup(id).unwrap();
        let mut input = inputs[*id].clone();
        input.as_object_mut().unwrap().remove("action");
        let plain = spec.normalize_arguments(&input).unwrap();
        for action in [
            json!(null),
            json!(false),
            json!("other"),
            json!({"ignored":true}),
        ] {
            input["action"] = action;
            assert_eq!(spec.normalize_arguments(&input).unwrap(), plain);
        }
        if *id == "genesis.start" {
            // The idea text comes from the leased user message; a caller's
            // copy was always discarded and is still accepted silently.
            input["initial_idea"] = json!("Build a note app");
            assert_eq!(spec.normalize_arguments(&input).unwrap(), plain);
            assert!(!spec.contract_line().contains("initial_idea"));
        }
        if *id == "charter.draft" {
            input["content"]["identity"]["working_name"] = json!("é".repeat(32768));
        } else {
            input["preferred_project_agent_identity_id"] = json!("é".repeat(32768));
        }
        assert!(spec
            .normalize_arguments(&input)
            .unwrap_err()
            .contains("serialized UTF-8 bytes"));
    }
}
