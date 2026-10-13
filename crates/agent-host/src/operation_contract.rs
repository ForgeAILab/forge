//! Canonical JSON contracts for migrated native orchestration operations.
//!
//! This module owns the provider-facing schemas and preparation-time
//! structural validation. Domain and lifecycle validation remains in the
//! service command boundary.

use std::collections::BTreeSet;

use agent_runtime::core::prelude::RuntimeError;
use serde_json::{Value, json};

use crate::operation_catalog::{PROJECT_CHARTER_ADOPTION_OPERATION, TASK_ADAPTIVE_OPERATION};
#[cfg(test)]
use crate::{
    MAIN_GENESIS_START_OPERATION, PROJECT_DOCUMENT_OPERATION, PROJECT_MILESTONE_OPERATION,
    PROJECT_REVIEW_CONFIG_OPERATION, PROJECT_VALIDATION_OPERATION, TASK_ACTION_OPERATION,
    TASK_DEPENDENCY_OPERATION, TASK_EVIDENCE_OPERATION, TASK_PLAN_OPERATION,
    TASK_PROPOSE_OPERATION, TASK_WORKLOG_OPERATION,
};

pub(crate) fn object_schema(properties: Value, required: &[&str]) -> Value {
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false,
    })
}
pub(crate) fn string_or_null_schema() -> Value {
    json!({"type":["string","null"]})
}

fn declares_structure(schema: &Value) -> bool {
    schema.get("properties").is_some()
        || schema.get("items").is_some_and(declares_structure)
        || ["anyOf", "oneOf"].iter().any(|keyword| {
            schema
                .get(*keyword)
                .and_then(Value::as_array)
                .is_some_and(|variants| variants.iter().any(declares_structure))
        })
}

fn advertised_proposal_line(
    spec: &operation_registry::OperationSpec<std::convert::Infallible>,
) -> String {
    if spec
        .surfaces
        .iter()
        .any(|surface| surface.native_aggregate == "forge_main_orchestration_propose")
    {
        // Field names alone cannot be called when a field is itself a
        // structure: a Charter draft needs every section's shape, and the
        // tool description is the only place a Main model is shown it.
        let schema = &spec.input.schema;
        let nested = schema["properties"]
            .as_object()
            .is_some_and(|properties| properties.values().any(declares_structure));
        if nested {
            return format!("{}: {}", spec.id, schema_signature(schema, 0));
        }
        return spec.contract_line();
    }
    if operation_registry::task_proposals::IDS.contains(&spec.id)
        || operation_registry::task_lead::IDS.contains(&spec.id)
    {
        return format!(
            "{}: {}; {} Payload <=65536 serialized UTF-8 bytes.",
            spec.id,
            schema_signature(&spec.input.schema, 0),
            spec.guidance
        );
    }
    if operation_registry::task_records::IDS.contains(&spec.id) {
        return format!(
            "{}; {} {}",
            spec.contract_line(),
            spec.summary,
            spec.guidance
        );
    }
    format!(
        "{}; {} Payload <=65536 serialized UTF-8 bytes.",
        spec.contract_line(),
        spec.summary
    )
}

pub(crate) fn orchestration_payload_schema(operation: &str) -> Value {
    if let Some(spec) = operation_registry::PROPOSAL_CATALOG.lookup(operation) {
        return spec.canonical_schema();
    }
    object_schema(json!({}), &[])
}

/// Adds `type`/`enum` beside every string `const` in a JSON schema (and a
/// `type` beside bare string enums). JSON Schema `const` alone falls outside
/// the OpenAPI-style subset some provider function-calling APIs accept —
/// Gemini drops the constraint entirely and models then emit `{}` for the
/// field — so every string const also carries the equivalent one-value enum.
pub(crate) fn portable_const_schema(mut schema: Value) -> Value {
    fn walk(value: &mut Value) {
        match value {
            Value::Object(map) => {
                if let Some(constant) = map.get("const").and_then(Value::as_str).map(str::to_owned)
                {
                    map.entry("type").or_insert_with(|| json!("string"));
                    map.entry("enum").or_insert_with(|| json!([constant]));
                } else if !map.contains_key("type") {
                    let all_strings =
                        map.get("enum")
                            .and_then(Value::as_array)
                            .is_some_and(|options| {
                                !options.is_empty() && options.iter().all(Value::is_string)
                            });
                    if all_strings {
                        map.insert("type".to_owned(), json!("string"));
                    }
                }
                for entry in map.values_mut() {
                    walk(entry);
                }
            }
            Value::Array(values) => values.iter_mut().for_each(walk),
            _ => {}
        }
    }
    walk(&mut schema);
    schema
}

/// Renders one JSON-schema node as a compact structural signature.
///
/// Provider function-calling APIs only reliably deliver flat object schemas,
/// so a nested payload contract cannot travel as a schema. It travels here
/// instead, in the payload description. Naming only the top-level required
/// keys is not enough: an operation like `charter.draft` carries a deeply
/// nested `content` object, and a model that cannot see those field names
/// has to rediscover them one rejected call at a time.
fn schema_signature(schema: &Value, depth: usize) -> String {
    const MAX_DEPTH: usize = 8;
    if depth > MAX_DEPTH {
        return "object".to_owned();
    }
    // Derived registry schemas spell a nullable structure as `anyOf`.
    if let Some(variants) = ["oneOf", "anyOf"]
        .iter()
        .find_map(|keyword| schema.get(*keyword).and_then(Value::as_array))
    {
        return variants
            .iter()
            .map(|variant| schema_signature(variant, depth))
            .collect::<Vec<_>>()
            .join("|");
    }
    if let Some(constant) = schema.get("const").and_then(Value::as_str) {
        return format!("\"{constant}\"");
    }
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        return values
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join("|");
    }
    let type_names: Vec<&str> = match schema.get("type") {
        Some(Value::String(name)) => vec![name.as_str()],
        Some(Value::Array(names)) => names.iter().filter_map(Value::as_str).collect(),
        _ => Vec::new(),
    };
    let nullable = type_names.contains(&"null");
    let Some(primary) = type_names
        .iter()
        .find(|name| **name != "null")
        .copied()
        .or_else(|| type_names.is_empty().then_some("object"))
    else {
        // A `null`-only node (the null arm of a `oneOf`) carries no shape.
        return "null".to_owned();
    };
    let rendered = match primary {
        "object" => {
            let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
                return if nullable {
                    "object|null".to_owned()
                } else {
                    "object".to_owned()
                };
            };
            let required: Vec<&str> = schema
                .get("required")
                .and_then(Value::as_array)
                .map(|values| values.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default();
            let fields = properties
                .iter()
                .map(|(name, property)| {
                    // `?` marks an optional field so the model can tell what it
                    // must supply from what it may omit.
                    let optional = if required.contains(&name.as_str()) {
                        ""
                    } else {
                        "?"
                    };
                    format!(
                        "{name}{optional}: {}",
                        schema_signature(property, depth + 1)
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("{{{fields}}}")
        }
        "array" => {
            let items = schema
                .get("items")
                .map(|items| schema_signature(items, depth + 1))
                .unwrap_or_else(|| "any".to_owned());
            format!("[{items}]")
        }
        other => other.to_owned(),
    };
    if nullable {
        format!("{rendered}|null")
    } else {
        rendered
    }
}

/// Summarizes one operation's payload contract as a guidance line for the
/// declared tool schema. Handles both single-variant payload schemas and
/// `oneOf` payloads (e.g. document draft vs. approval).
pub(crate) fn orchestration_payload_summary(schema: &Value) -> String {
    fn variant_summary(variant: &Value) -> Option<String> {
        let properties = variant.get("properties")?;
        let action = properties.get("action").map(|action| {
            action
                .get("const")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| {
                    action.get("enum").and_then(Value::as_array).map(|values| {
                        values
                            .iter()
                            .filter_map(Value::as_str)
                            .collect::<Vec<_>>()
                            .join("|")
                    })
                })
                .unwrap_or_default()
        });
        let required = variant
            .get("required")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();
        let shape = schema_signature(variant, 0);
        Some(match action {
            Some(action) if !action.is_empty() => {
                format!("action={action}; required: [{required}]; shape: {shape}")
            }
            _ => format!("required: [{required}]; shape: {shape}"),
        })
    }
    if let Some(variants) = schema.get("oneOf").and_then(Value::as_array) {
        return variants
            .iter()
            .filter_map(variant_summary)
            .collect::<Vec<_>>()
            .join(" OR ");
    }
    variant_summary(schema).unwrap_or_default()
}

/// Payload guidance for the generic coordination proposal surface. The
/// payload stays a free-form object at the schema level (provider
/// function-calling APIs only reliably deliver flat object schemas), so the
/// contract for operations with a typed server-side executor is carried in
/// the property description; the exact shape stays enforced server-side and
/// errors return to the model in-turn.
pub(crate) fn coordination_payload_guidance(operations: &BTreeSet<String>) -> String {
    let mut lines = Vec::new();
    if operations.contains("task.propose") || operations.contains(TASK_ADAPTIVE_OPERATION) {
        lines.push(crate::MERGE_FRIENDLY_TASK_GUIDANCE);
    }
    let mut lines = lines.into_iter().map(str::to_owned).collect::<Vec<_>>();
    lines.extend(
        operations
            .iter()
            .filter_map(|operation| operation_registry::PROPOSAL_CATALOG.lookup(operation))
            .map(advertised_proposal_line),
    );
    if lines.is_empty() {
        String::new()
    } else {
        format!(
            "Payload shape by operation (exact contract enforced server-side):\n- {}",
            lines.join("\n- ")
        )
    }
}

/// Portable aliases preserve nested fields for providers that strip undeclared
/// object properties. Only the admitted specs contribute fields; exact selected
/// contracts and offers still drive enforcement.
pub(crate) fn coordination_payload_properties(operations: &BTreeSet<String>) -> Option<Value> {
    fn projection(schemas: &[&Value], depth: usize) -> Value {
        if depth > 8 {
            return json!({});
        }
        let variants = schemas
            .iter()
            .flat_map(|schema| {
                schema
                    .get("oneOf")
                    .or_else(|| schema.get("anyOf"))
                    .and_then(Value::as_array)
                    .map_or_else(|| vec![*schema], |values| values.iter().collect())
            })
            .collect::<Vec<_>>();
        if variants.len() != schemas.len()
            || variants
                .iter()
                .any(|v| v.get("oneOf").is_some() || v.get("anyOf").is_some())
        {
            return projection(&variants, depth + 1);
        }
        let mut kinds = BTreeSet::new();
        let mut fields: std::collections::BTreeMap<&str, Vec<&Value>> =
            std::collections::BTreeMap::new();
        let mut items = Vec::new();
        for schema in &variants {
            match &schema["type"] {
                Value::String(kind) => {
                    kinds.insert(kind.clone());
                }
                Value::Array(types) => {
                    kinds.extend(types.iter().filter_map(Value::as_str).map(str::to_owned))
                }
                _ => {
                    kinds.insert("object".into());
                }
            }
            for (field, property) in schema["properties"].as_object().into_iter().flatten() {
                fields.entry(field).or_default().push(property);
            }
            if let Some(item) = schema.get("items") {
                items.push(item);
            }
        }
        let mut result = json!({"type":if kinds.len() == 1 { json!(kinds.iter().next().unwrap()) } else { json!(kinds) }});
        if !fields.is_empty() {
            result["properties"] = Value::Object(
                fields
                    .into_iter()
                    .map(|(field, schemas)| (field.into(), projection(&schemas, depth + 1)))
                    .collect(),
            );
        }
        if kinds.contains("array") {
            result["items"] = if items.is_empty() {
                json!({})
            } else {
                projection(&items, depth + 1)
            };
        }
        if variants.iter().all(|schema| schema["enum"].is_array()) {
            let values = variants
                .iter()
                .flat_map(|schema| schema["enum"].as_array().unwrap().iter().cloned())
                .collect::<Vec<_>>();
            result["enum"] = json!(values);
        }
        result
    }
    let mut fields: std::collections::BTreeMap<&str, Vec<&Value>> =
        std::collections::BTreeMap::new();
    for operation in operations {
        if !operation_registry::task_records::IDS.contains(&operation.as_str())
            && !operation_registry::task_proposals::IDS.contains(&operation.as_str())
            && !operation_registry::task_lead::IDS.contains(&operation.as_str())
        {
            continue;
        }
        let Some(spec) = operation_registry::PROPOSAL_CATALOG.lookup(operation) else {
            continue;
        };
        for (field, schema) in spec.input.schema["properties"].as_object().unwrap() {
            fields.entry(field).or_default().push(schema);
        }
    }
    let properties = fields
        .into_iter()
        .map(|(field, schemas)| {
            let mut schema = projection(&schemas, 0);
            if schema["type"].is_string() {
                schema["type"] = json!([schema["type"].clone()]);
            }
            let kinds = schema["type"].as_array_mut().unwrap();
            if !kinds.contains(&json!("null")) {
                kinds.push(json!("null"));
            }
            kinds.sort_by(|a, b| a.as_str().cmp(&b.as_str()));
            if let Some(values) = schema.get_mut("enum").and_then(Value::as_array_mut) {
                values.push(Value::Null);
            }
            (field.into(), schema)
        })
        .collect::<serde_json::Map<_, _>>();
    (!properties.is_empty()).then_some(Value::Object(properties))
}

// The declared envelope is a plain `type: object` schema: several provider
// function-calling APIs (notably Gemini) accept only an OpenAPI-style object
// at the parameters root and silently drop `oneOf`, leaving the model blind
// to the envelope — it then emits flattened payload fields and the whole
// attempt dies at provider-side schema validation. Per-operation payload
// shapes are summarized in the description instead, and the exact contracts
// stay enforced by `validate_orchestration_proposal_arguments` plus the
// server-side validators, whose errors return to the model in-turn.
pub(crate) fn orchestration_proposal_schema(operations: &BTreeSet<String>) -> Value {
    let mut guidance =
        vec!["Payload shape by operation (exact contract enforced server-side):".to_owned()];
    for operation in operations {
        if let Some(spec) = operation_registry::PROPOSAL_CATALOG.lookup(operation) {
            guidance.push(format!("- {}", advertised_proposal_line(spec)));
            continue;
        }
        let summary = orchestration_payload_summary(&orchestration_payload_schema(operation));
        guidance.push(format!("- {operation}: {summary}"));
    }
    let payload =
        if operations.len() == 1 && operations.contains(PROJECT_CHARTER_ADOPTION_OPERATION) {
            let mut schema = orchestration_payload_schema(PROJECT_CHARTER_ADOPTION_OPERATION);
            let operation_description = schema
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let description = if operation_description.is_empty() {
                guidance.join("\n")
            } else {
                format!("{}\n\n{operation_description}", guidance.join("\n"))
            };
            schema["description"] = Value::String(description);
            schema
        } else {
            json!({"type":"object","description":guidance.join("\n")})
        };
    json!({
        "type":"object",
        "required":["operation","payload","dedupe_key","correlation_id"],
        "properties":{
            "operation":{"type":"string","enum":operations.iter().collect::<Vec<_>>()},
            "payload":payload,
            "dedupe_key":{"type":"string","minLength":1},
            "correlation_id":{"type":"string","minLength":1},
            "causation_id":string_or_null_schema(),
            "causation_depth":{"type":"integer","minimum":0,"maximum":8},
        },
        "additionalProperties":false,
    })
}

#[cfg(test)]
pub(crate) fn orchestration_read_arguments_schema(operation: &str) -> Value {
    if let Some(spec) = operation_registry::READ_CATALOG.lookup(operation) {
        return spec.canonical_schema();
    }
    object_schema(json!({}), &[])
}

pub(crate) fn orchestration_read_schema(operations: &BTreeSet<String>) -> Value {
    let compact = operations.iter().all(|id| {
        operation_registry::READ_CATALOG
            .lookup(id)
            .is_some_and(|spec| {
                spec.surfaces
                    .iter()
                    .any(|surface| surface.native_aggregate == "forge_scope_read")
            })
    });
    let mut guidance = Vec::new();
    for operation in operations {
        if let Some(spec) = operation_registry::READ_CATALOG.lookup(operation) {
            if !compact {
                guidance.push(format!("- {}", spec.contract_line()));
            } else if !spec.guidance.is_empty() {
                guidance.push(format!("- {}; {}", spec.contract_line(), spec.guidance));
            }
        }
    }
    let mut arguments = json!({"type":"object"});
    if !guidance.is_empty() {
        arguments["description"] = json!(guidance.join("\n"));
    }
    json!({
        "type":"object",
        "required":["operation"],
        "properties":{
            "operation":{"type":"string","enum":operations.iter().collect::<Vec<_>>()},
            "arguments":arguments,
        },
        "additionalProperties":false,
    })
}
/// Apply the closed discriminant portion of the named orchestration schema at
/// preparation time as well as in the provider.  Runtime model tool calls do
/// not necessarily pass through a JSON-Schema validator, so accepting a
/// mismatched operation/action pair here would make the registry appear more
/// permissive than its advertised contract.
pub(crate) fn validate_orchestration_proposal_arguments(
    operation: &str,
    arguments: &Value,
) -> Result<(), RuntimeError> {
    let object = arguments
        .as_object()
        .ok_or_else(|| RuntimeError::tool("Forge orchestration proposal must be an object"))?;
    const ALLOWED_FIELDS: &[&str] = &[
        "operation",
        "payload",
        "dedupe_key",
        "correlation_id",
        "causation_id",
        "causation_depth",
    ];
    if let Some(field) = object
        .keys()
        .find(|field| !ALLOWED_FIELDS.contains(&field.as_str()))
    {
        return Err(RuntimeError::tool(
            match operation_registry::PROPOSAL_CATALOG.lookup(operation) {
                Some(spec) => format!(
                    "{operation}: envelope field `{field}` is not admitted; expected {}",
                    spec.contract_line()
                ),
                None => format!("Forge orchestration proposal field `{field}` is not admitted"),
            },
        ));
    }
    if operation_registry::PROPOSAL_CATALOG
        .lookup(operation)
        .is_none()
    {
        let payload = object
            .get("payload")
            .and_then(Value::as_object)
            .ok_or_else(|| RuntimeError::tool("Forge orchestration payload must be an object"))?;
        let action = payload
            .get("action")
            .and_then(Value::as_str)
            .ok_or_else(|| RuntimeError::tool("Forge orchestration payload action is required"))?;
        let schema = orchestration_payload_schema(operation);
        let action_schema = schema
            .get("properties")
            .and_then(|properties| properties.get("action"))
            .ok_or_else(|| {
                RuntimeError::tool("Forge orchestration action schema is unavailable")
            })?;
        if let Some(expected) = action_schema.get("const").and_then(Value::as_str) {
            if action != expected {
                return Err(RuntimeError::tool(format!(
                    "Forge orchestration action must be {expected}"
                )));
            }
        } else if let Some(actions) = action_schema.get("enum").and_then(Value::as_array) {
            if !actions
                .iter()
                .any(|candidate| candidate.as_str() == Some(action))
            {
                return Err(RuntimeError::tool(
                    "Forge orchestration action is outside this typed contract",
                ));
            }
        } else {
            return Err(RuntimeError::tool(
                "Forge orchestration action schema is not closed",
            ));
        }
    }
    if let Some(spec) = operation_registry::PROPOSAL_CATALOG.lookup(operation) {
        spec.validate_arguments(&arguments["payload"])
            .map_err(RuntimeError::tool)?;
    }
    if let Some(value) = object.get("causation_id") {
        if !value.is_null() && !value.is_string() {
            return Err(RuntimeError::tool("causation_id must be a string or null"));
        }
    }
    if let Some(value) = object.get("causation_depth") {
        let depth = value
            .as_i64()
            .ok_or_else(|| RuntimeError::tool("causation_depth must be an integer"))?;
        if !(0..=8).contains(&depth) {
            return Err(RuntimeError::tool(
                "causation_depth must be between 0 and 8",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation_catalog::{
        MIGRATED_OPERATION_CONTRACTS, OperationInputContract, OperationOutputContract,
        SHARED_ORCHESTRATION_OUTCOME,
    };
    use crate::{
        MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION, MAIN_GENESIS_PROJECT_AGENTS_READ_OPERATION,
    };

    #[test]
    fn project_escalate_descriptor_is_small_and_project_only() {
        let schema = orchestration_payload_schema(crate::PROJECT_ESCALATE_OPERATION);
        let description = schema["description"].as_str().unwrap();
        assert!(description.split_whitespace().count() <= 25);
        let c = crate::operation_contract(crate::PROJECT_ESCALATE_OPERATION).unwrap();
        assert!(
            !c.supported_scopes
                .contains(&crate::CanonicalScopeType::Account)
        );
        assert!(
            !c.supported_scopes
                .contains(&crate::CanonicalScopeType::Task)
        );
        assert_eq!(
            crate::classify_operation(
                crate::PROJECT_ESCALATE_OPERATION,
                Some(&serde_json::json!({"need":"disk","task_ids":[]}))
            ),
            crate::OperationClassification::DirectCommand
        );
    }

    #[test]
    fn every_migrated_contract_resolves_to_its_input_and_shared_output() {
        for contract in MIGRATED_OPERATION_CONTRACTS.iter() {
            let schema = match contract.input {
                OperationInputContract::ReadArguments => {
                    orchestration_read_arguments_schema(contract.operation)
                }
                OperationInputContract::ProposalEnvelope => {
                    orchestration_payload_schema(contract.operation)
                }
            };

            assert_eq!(schema.get("type").and_then(Value::as_str), Some("object"));
            if contract.operation == crate::MAIN_PROJECT_CREATE_OPERATION {
                // The enqueuer keeps extra payload fields (the user executor
                // ignores them), so this one contract is open. The Charter
                // approval reference itself is required: the agent is its
                // only source and the executor refuses an action without it.
                assert!(schema.get("additionalProperties").is_none());
                assert_eq!(schema["required"], json!(["approval_id"]));
                assert_eq!(
                    schema["properties"]["approval_id"],
                    json!({"type":"string","minLength":1})
                );
                let spec = operation_registry::PROPOSAL_CATALOG
                    .lookup(contract.operation)
                    .unwrap();
                spec.validate_arguments(
                    &json!({"action":"ignored", "approval_id":"approval", "unknown":"preserved"}),
                )
                .unwrap();
                for refused in [
                    json!({}),
                    json!({"approval_id":null}),
                    json!({"approval_id":""}),
                ] {
                    assert!(
                        spec.validate_arguments(&refused)
                            .unwrap_err()
                            .contains("approval_id")
                    );
                }
            } else {
                assert_eq!(
                    schema.get("additionalProperties").and_then(Value::as_bool),
                    Some(false),
                    "{} must remain a closed input contract",
                    contract.operation
                );
            }
            // Every contract must resolve past the unhandled-operation
            // fallthrough: either operation-specific properties or an
            // explicit root description marking a deliberate zero-argument
            // read (the fallthrough produces neither).
            let has_properties = schema
                .get("properties")
                .and_then(Value::as_object)
                .is_some_and(|properties| !properties.is_empty());
            let has_description = schema
                .get("description")
                .and_then(Value::as_str)
                .is_some_and(|description| !description.trim().is_empty());
            assert!(
                has_properties || has_description,
                "{} must resolve to operation-specific input metadata",
                contract.operation
            );
            assert_eq!(
                contract.output,
                OperationOutputContract {
                    envelope: "api_types::OrchestrationOutcome",
                    in_band_errors: true,
                    replay_field: "replayed",
                }
            );
            assert_eq!(contract.output, SHARED_ORCHESTRATION_OUTCOME);
        }
    }

    #[test]
    fn milestone_contract_exposes_exact_acceptance_evidence_shape() {
        let milestone = orchestration_payload_schema(PROJECT_MILESTONE_OPERATION);
        let check = &milestone["properties"]["content"]["properties"]["acceptance_checks"]["items"];
        assert_eq!(
            check["properties"]["source_kind"]["enum"],
            json!(["manual", "policy_waiver", "task_validation"])
        );
        let evidence =
            &milestone["properties"]["content"]["properties"]["evidence_requirements"]["items"];
        assert_eq!(
            evidence["properties"]["evidence_kind"]["enum"],
            json!(["screenshot", "walkthrough_video", "log", "report", "other"])
        );
        assert!(
            evidence["properties"]["evidence_kind"]["type"]
                .as_array()
                .is_some_and(|kinds| kinds.contains(&json!("null")))
        );
    }

    /// The Main description is the only place a model is shown the Charter
    /// structure, so the generated draft line must carry every nested shape,
    /// and the same shapes the hand adoption schema declares.
    #[test]
    fn main_charter_draft_line_advertises_the_nested_charter_shape() {
        let operations = BTreeSet::from(["charter.draft".to_owned(), "genesis.start".to_owned()]);
        let schema = orchestration_proposal_schema(&operations);
        let description = schema["properties"]["payload"]["description"]
            .as_str()
            .expect("payload description");
        let line = |operation: &str| {
            description
                .lines()
                .find(|line| line.starts_with(&format!("- {operation}: ")))
                .unwrap_or_else(|| panic!("{operation} line"))
        };
        let draft = line("charter.draft");
        for (field, hand) in [
            (
                "content",
                orchestration_payload_schema("charter.draft")["properties"]["content"].clone(),
            ),
            (
                "provenance",
                orchestration_payload_schema("charter.draft")["properties"]["provenance"].clone(),
            ),
        ] {
            assert!(
                draft.contains(&format!("{field}: {}", schema_signature(&hand, 0))),
                "{field} shape is missing from: {draft}"
            );
        }
        for field in [
            "charter_id: string",
            "expected_charter_version?: integer|null",
            "project_mode: compact|standard",
            "source_refs?: [{",
        ] {
            assert!(draft.contains(field), "{field}");
        }
        assert!(!draft.contains("{action") && !draft.contains(" action"));
        assert_eq!(
            line("genesis.start"),
            "- genesis.start: {maturity?: one of prototype|mvp|production|critical, \
             preferred_project_agent_identity_id?}"
        );
    }

    #[test]
    fn genesis_start_contract_is_closed_and_carries_no_source_authority() {
        let schema = orchestration_payload_schema(MAIN_GENESIS_START_OPERATION);
        assert_eq!(schema["additionalProperties"], false);
        assert!(schema["properties"].get("action").is_none());
        let properties = schema["properties"].as_object().expect("properties");
        assert_eq!(
            properties.keys().cloned().collect::<Vec<_>>(),
            [
                "maturity".to_owned(),
                "preferred_project_agent_identity_id".to_owned(),
            ]
        );
        for forbidden in [
            "account_id",
            "chat_id",
            "initial_idea",
            "source_message_id",
            "source_turn_id",
        ] {
            assert!(!properties.contains_key(forbidden));
        }
    }

    #[test]
    fn project_agent_selection_contract_is_structured_and_has_no_charter_prose() {
        let read = orchestration_read_arguments_schema(MAIN_GENESIS_PROJECT_AGENTS_READ_OPERATION);
        assert_eq!(read["additionalProperties"], false);
        assert!(read["properties"].get("genesis_session_id").is_some());

        let select = orchestration_payload_schema(MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION);
        assert_eq!(select["additionalProperties"], false);
        assert!(select["properties"].get("action").is_none());
        assert_eq!(
            select["required"],
            json!(["expected_session_version", "project_agent_identity_id"])
        );
        let properties = select["properties"].as_object().expect("properties");
        assert!(!properties.contains_key("charter_content"));
        assert!(!properties.contains_key("charter_prose"));
    }

    #[test]
    fn singleton_adoption_proposal_exposes_nested_payload_contract() {
        let operations = BTreeSet::from([PROJECT_CHARTER_ADOPTION_OPERATION.to_owned()]);
        let schema = orchestration_proposal_schema(&operations);
        let payload = &schema["properties"]["payload"];

        assert_eq!(payload["additionalProperties"], false);
        assert_eq!(payload["properties"]["action"]["const"], "draft_revision");
        assert_eq!(payload["properties"]["content"]["type"], "object");
        for required in [
            "action",
            "expected_charter_version",
            "project_mode",
            "maturity",
            "content",
            "provenance",
        ] {
            assert!(
                payload["required"]
                    .as_array()
                    .expect("adoption payload required fields")
                    .iter()
                    .any(|value| value == required),
                "adoption payload must expose required field {required}"
            );
        }
        for required in [
            "identity",
            "problem_and_people",
            "core_experience",
            "scope",
            "success",
            "constraints_and_risks",
            "knowledge_ledger",
        ] {
            assert!(
                payload["properties"]["content"]["required"]
                    .as_array()
                    .expect("Charter content required fields")
                    .iter()
                    .any(|value| value == required),
                "adoption payload content must expose required field {required}"
            );
        }
        assert!(
            payload["description"]
                .as_str()
                .expect("adoption payload description")
                .contains("Payload shape by operation")
        );
    }

    #[test]
    fn multi_operation_proposal_keeps_provider_portable_payload_envelope() {
        let operations = BTreeSet::from([
            PROJECT_CHARTER_ADOPTION_OPERATION.to_owned(),
            PROJECT_DOCUMENT_OPERATION.to_owned(),
        ]);
        let payload = orchestration_proposal_schema(&operations)["properties"]["payload"].clone();

        assert_eq!(
            payload,
            json!({
                "type": "object",
                "description": payload["description"],
            })
        );
        assert!(payload.get("properties").is_none());
        assert!(payload.get("required").is_none());
    }

    #[test]
    fn task_shaping_guidance_is_shared_once_for_each_admitted_surface() {
        let rule = crate::MERGE_FRIENDLY_TASK_GUIDANCE;
        assert_eq!(
            rule,
            "Split work along module boundaries, name the files each Task owns, and avoid Tasks that all edit one shared file."
        );
        assert!(rule.split_whitespace().count() <= 25);
        for operations in [
            BTreeSet::from([TASK_PROPOSE_OPERATION.to_owned()]),
            BTreeSet::from([TASK_ADAPTIVE_OPERATION.to_owned()]),
            BTreeSet::from([
                TASK_PROPOSE_OPERATION.to_owned(),
                TASK_ADAPTIVE_OPERATION.to_owned(),
            ]),
        ] {
            let guidance = coordination_payload_guidance(&operations);
            assert_eq!(guidance.matches(rule).count(), 1);
            assert!(!guidance.contains(crate::MERGE_FRIENDLY_LAYOUT_GUIDANCE));
        }
        assert!(
            !coordination_payload_guidance(&BTreeSet::from([TASK_WORKLOG_OPERATION.to_owned()]))
                .contains(rule)
        );
    }

    #[test]
    fn task_proposal_requires_an_explicit_review_scope_decision() {
        let schema = orchestration_payload_schema(TASK_PROPOSE_OPERATION);
        assert!(
            schema["required"]
                .as_array()
                .expect("required fields")
                .iter()
                .any(|field| field == "review_requirement_ids")
        );
        assert_eq!(
            schema["properties"]["review_requirement_ids"]["type"],
            "array"
        );
        let prerequisite_description = schema["properties"]["depends_on_task_ids"]["description"]
            .as_str()
            .expect("prerequisite guidance");
        for phrase in [
            "prerequisite DAG edges only",
            "parent/child hierarchy",
            "workspace sharing",
        ] {
            assert!(
                prerequisite_description.contains(phrase),
                "task.propose prerequisite guidance must distinguish {phrase}: {prerequisite_description}"
            );
        }
        let operations = BTreeSet::from([TASK_PROPOSE_OPERATION.to_owned()]);
        let guidance = coordination_payload_guidance(&operations);
        assert!(guidance.contains("owned repository-relative paths"));
        assert!(guidance.contains("parallel Tasks disjoint files"));
        assert!(guidance.contains("dependencies to order shared-file edits"));
        assert!(guidance.contains("prerequisite DAG edges only"));
        assert!(guidance.contains("not a parent/child hierarchy or workspace sharing"));
        let properties = coordination_payload_properties(&operations).expect("flat properties");
        assert_eq!(
            properties["depends_on_task_ids"]["type"],
            json!(["array", "null"])
        );
        assert!(
            schema["properties"]["description"]["description"]
                .as_str()
                .unwrap()
                .contains("owned repository-relative paths")
        );
        // A field the server requires but the flat surface never declares is
        // a field a declared-properties-only provider cannot send at all:
        // the Task can then never be created, however the model writes it.
        for required in required_payload_fields(TASK_PROPOSE_OPERATION) {
            assert!(
                properties.get(&required).is_some(),
                "task.propose requires `{required}`, so the flat payload surface must declare it"
            );
        }
    }

    /// The payload fields `operation` declares as required, minus `action`,
    /// which the envelope derives from the operation itself.
    fn required_payload_fields(operation: &str) -> Vec<String> {
        orchestration_payload_schema(operation)["required"]
            .as_array()
            .expect("required fields")
            .iter()
            .filter_map(|field| field.as_str())
            .filter(|field| *field != "action")
            .map(str::to_owned)
            .collect()
    }

    #[test]
    fn project_review_config_replaces_a_bounded_ci_step_list() {
        let schema = orchestration_payload_schema(PROJECT_REVIEW_CONFIG_OPERATION);
        assert_eq!(schema["properties"]["action"]["const"], "set_ci_steps");
        assert_eq!(schema["properties"]["ci_steps"]["maxItems"], 16);
        assert_eq!(schema["properties"]["ci_steps"]["uniqueItems"], true);
        assert_eq!(schema["properties"]["setup_steps"]["maxItems"], 16);
        assert_eq!(schema["properties"]["setup_steps"]["uniqueItems"], true);
        for required in ["action", "expected_project_version", "ci_steps"] {
            assert!(
                schema["required"]
                    .as_array()
                    .expect("required fields")
                    .iter()
                    .any(|field| field == required)
            );
        }
    }

    /// An operation constant that is not imported becomes a binding pattern in
    /// `match`, silently swallowing every arm after it -- the payload schema
    /// for one operation is then served for all of them. Rust only warns. This
    /// asserts distinct operations keep distinct schemas, which a wildcard
    /// cannot satisfy.
    #[test]
    fn every_coordination_operation_declares_its_required_fields_on_the_flat_surface() {
        // The failure this prevents has happened twice. A field the server
        // requires but the flat envelope never declares cannot be sent at all
        // by a provider that surfaces only declared properties — Gemini and
        // z.ai both strip the rest — so the operation is unusable however the
        // model writes the call, and nothing in the error says which field is
        // missing. Checking one operation by hand is what let the second case
        // through, so this walks the catalog.
        let coordination: Vec<&str> = MIGRATED_OPERATION_CONTRACTS
            .iter()
            .map(|contract| contract.operation)
            .filter(|operation| {
                crate::operation_catalog::is_coordination_generic_proposal(operation)
            })
            .collect();
        for required in [
            TASK_PROPOSE_OPERATION,
            TASK_ADAPTIVE_OPERATION,
            TASK_DEPENDENCY_OPERATION,
            TASK_ACTION_OPERATION,
        ] {
            assert!(
                coordination.contains(&required),
                "the coordination envelope must carry {required}: {coordination:?}"
            );
        }

        for operation in coordination {
            let alone: BTreeSet<String> = [operation.to_owned()].into_iter().collect();
            let properties = coordination_payload_properties(&alone).unwrap_or_else(|| {
                panic!("{operation} rides the flat envelope, so it must have a payload surface")
            });
            for required in required_payload_fields(operation) {
                assert!(
                    properties.get(&required).is_some(),
                    "{operation} requires `{required}`, so the flat payload surface must \
                     declare it — otherwise a declared-properties-only provider cannot send it"
                );
            }
        }
    }

    #[test]
    fn the_flat_surface_serves_task_dependency_even_when_it_is_the_only_operation() {
        // The flat/legacy coordination surface returns early unless it
        // recognizes at least one operation. `task.dependency` reaching a
        // scope without `task.propose` beside it would otherwise get a
        // payload surface with none of its fields declared — the same shape
        // that made a required field invisible to Gemini and z.ai before.
        let only_dependency: BTreeSet<String> =
            [TASK_DEPENDENCY_OPERATION.to_owned()].into_iter().collect();
        let properties = coordination_payload_properties(&only_dependency)
            .expect("the flat surface must serve this operation on its own");
        for field in ["action", "task_id", "depends_on_task_id"] {
            assert!(
                properties.get(field).is_some(),
                "{field} must be declared on the flat surface: {properties}"
            );
        }
        let guidance = coordination_payload_guidance(&only_dependency);
        assert!(
            guidance.contains("task.dependency"),
            "the flat surface must explain the operation it serves: {guidance}"
        );
    }

    #[test]
    fn every_operation_resolves_its_own_payload_schema() {
        let cases = [
            (TASK_ADAPTIVE_OPERATION, "split"),
            (TASK_ACTION_OPERATION, "cancel"),
            (TASK_DEPENDENCY_OPERATION, "add"),
            (TASK_ACTION_OPERATION, "retry"),
            (TASK_PLAN_OPERATION, "write"),
            (TASK_WORKLOG_OPERATION, "append"),
            (TASK_EVIDENCE_OPERATION, "capture"),
            (PROJECT_VALIDATION_OPERATION, "record"),
            (PROJECT_REVIEW_CONFIG_OPERATION, "set_ci_steps"),
        ];
        for (operation, expected_action) in cases {
            let schema = orchestration_payload_schema(operation);
            let action = &schema["properties"]["action"];
            let admits = if operation == TASK_ACTION_OPERATION {
                jsonschema::validator_for(action)
                    .expect("closed Task action schema")
                    .is_valid(&json!({"verb": expected_action}))
            } else {
                action
                    .get("const")
                    .and_then(Value::as_str)
                    .map(|value| value == expected_action)
                    .unwrap_or_else(|| {
                        action["enum"]
                            .as_array()
                            .is_some_and(|values| values.iter().any(|v| v == expected_action))
                    })
            };
            assert!(
                admits,
                "{operation} resolved a schema whose action does not admit \
                 `{expected_action}` -- an unimported constant is matching everything"
            );
        }
    }

    #[test]
    fn task_plan_contract_requires_write_and_nonempty_content() {
        let schema = orchestration_payload_schema(TASK_PLAN_OPERATION);
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(schema["properties"]["action"]["const"], "write");
        assert_eq!(schema["properties"]["content"]["type"], "string");
        assert_eq!(schema["properties"]["content"]["minLength"], 1);
        assert!(schema["properties"]["content"].get("maxLength").is_none());
        assert_eq!(schema["required"], json!(["action", "content"]));

        let validator = jsonschema::validator_for(&schema).expect("Task plan schema compiles");
        assert!(validator.is_valid(&json!({
            "action": "write",
            "content": "- [ ] implement"
        })));
        assert!(!validator.is_valid(&json!({"action": "write", "content": ""})));
        let plan = operation_registry::PROPOSAL_CATALOG
            .lookup(TASK_PLAN_OPERATION)
            .unwrap();
        assert!(
            plan.validate_arguments(&json!({"action":"write", "content":"é".repeat(524_289)}))
                .is_err()
        );
        assert!(!validator.is_valid(&json!({
            "action": "append",
            "content": "- [ ] implement"
        })));

        let valid = json!({
            "operation": TASK_PLAN_OPERATION,
            "payload": {"action": "write", "content": "- [ ] implement"},
            "dedupe_key": "plan-1",
            "correlation_id": "corr-1"
        });
        validate_orchestration_proposal_arguments(TASK_PLAN_OPERATION, &valid)
            .expect("write is the admitted Task plan action");

        let invalid = json!({
            "operation": TASK_PLAN_OPERATION,
            "payload": {"action": "append", "content": "- [ ] implement"},
            "dedupe_key": "plan-1",
            "correlation_id": "corr-1"
        });
        assert!(validate_orchestration_proposal_arguments(TASK_PLAN_OPERATION, &invalid).is_err());
    }

    #[test]
    fn adaptive_contract_is_closed_and_has_exactly_three_action_variants() {
        let schema = orchestration_payload_schema(TASK_ADAPTIVE_OPERATION);
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(
            schema["properties"]["action"]["enum"],
            json!(["split", "sequence", "replace"])
        );
        let variants = schema["oneOf"].as_array().expect("adaptive variants");
        assert_eq!(variants.len(), 3);
        for variant in variants {
            assert_eq!(variant["additionalProperties"], false);
        }
        let adaptive_description = schema["description"].as_str().expect("adaptive guidance");
        for phrase in [
            "non-executing coordination root",
            "share the root workspace",
            "independent agents, executions, and lifecycles",
            "execute serially",
            "does not create a dependency graph",
        ] {
            assert!(
                adaptive_description.contains(phrase),
                "adaptive guidance must distinguish {phrase}: {adaptive_description}"
            );
        }
        assert!(
            variants[0]["description"]
                .as_str()
                .expect("split variant guidance")
                .contains("ordered direct children")
        );
        assert!(
            variants[1]["description"]
                .as_str()
                .expect("sequence variant guidance")
                .contains("does not create a dependency graph")
        );
        let properties = schema["properties"].as_object().expect("properties");
        for forbidden in [
            "project_id",
            "scope_id",
            "actor_id",
            "governance",
            "fixed_boundary_digest",
        ] {
            assert!(!properties.contains_key(forbidden));
        }
    }

    #[test]
    fn adaptive_guidance_and_flat_properties_are_declared_for_coordination() {
        let operations = [TASK_ADAPTIVE_OPERATION.to_owned()]
            .into_iter()
            .collect::<BTreeSet<_>>();
        let guidance = coordination_payload_guidance(&operations);
        assert!(guidance.contains("task.adaptive"));
        assert!(guidance.contains("split"));
        for phrase in [
            "non-executing coordination root",
            "share the root workspace",
            "independent agents, executions, and lifecycles",
            "execute serially",
            "does not create a dependency graph",
        ] {
            assert!(
                guidance.contains(phrase),
                "coordination guidance must distinguish {phrase}: {guidance}"
            );
        }
        let properties = coordination_payload_properties(&operations).expect("flat properties");
        assert!(properties.get("action").is_some());
        assert!(properties.get("source_task_id").is_some());
        assert!(properties.get("ordered_task_ids").is_some());
        assert_eq!(properties["items"]["type"], json!(["array", "null"]));
        assert_eq!(
            properties["ordered_task_ids"]["type"],
            json!(["array", "null"])
        );
    }

    #[test]
    fn combined_task_command_action_guidance_names_every_discriminator() {
        let operations = [
            TASK_ADAPTIVE_OPERATION.to_owned(),
            TASK_ACTION_OPERATION.to_owned(),
        ]
        .into_iter()
        .collect::<BTreeSet<_>>();
        let properties = coordination_payload_properties(&operations).expect("flat properties");
        assert_eq!(
            properties["action"]["type"],
            json!(["null", "object", "string"])
        );
        let guidance = coordination_payload_guidance(&operations);
        for discriminator in [
            "task.action",
            "retry",
            "task.adaptive",
            "split",
            "sequence",
            "replace",
            "cancel",
        ] {
            assert!(guidance.contains(discriminator), "{guidance}");
        }
    }
    #[test]
    fn task_action_flat_payload_has_version_and_no_retired_envelope_fields() {
        let operations = [TASK_ACTION_OPERATION.to_owned()].into_iter().collect();
        let properties = coordination_payload_properties(&operations).unwrap();
        for required in ["task_id", "action", "version"] {
            assert!(properties.get(required).is_some());
        }
        for retired in ["reason", "decision", "expected_task_version"] {
            assert!(properties.get(retired).is_none());
        }
    }
    #[tokio::test]
    async fn task_action_flat_schema_survives_gemini_and_openai_conversion() {
        use agent_runtime::{
            core::{
                cancel::Cancellation,
                clock::Deadline,
                content::Message,
                ids::{AttemptId, RequestId, SessionId},
                provider::{
                    ModelId, Provider, ProviderAttemptPurpose, ProviderCallContext, ProviderError,
                    ProviderRequest, ToolSchema,
                },
                store::Secret,
            },
            provider::{
                gemini::{GeminiInteractionsConfig, GeminiInteractionsProvider},
                openai::{OpenAiConfig, OpenAiProvider},
                transport::{ByteStream, HttpRequest, HttpTransport},
            },
        };
        use futures_util::StreamExt;
        use std::sync::{Arc, Mutex};
        #[derive(Clone, Debug, Default)]
        struct Capture(Arc<Mutex<Vec<Value>>>);
        #[async_trait::async_trait]
        impl HttpTransport for Capture {
            async fn post_stream(&self, request: HttpRequest) -> Result<ByteStream, ProviderError> {
                self.0
                    .lock()
                    .unwrap()
                    .push(serde_json::from_slice(&request.body).unwrap());
                Ok(Box::pin(futures_util::stream::empty()))
            }
        }
        #[derive(Debug)]
        struct Noop;
        #[async_trait::async_trait]
        impl crate::ForgeToolProvider for Noop {
            async fn read(
                &self,
                _: &str,
                _: &crate::CanonicalScope,
                _: &str,
                _: Value,
            ) -> Result<Value, crate::AgentHostError> {
                Ok(json!({}))
            }
            async fn propose(
                &self,
                _: &str,
                _: &crate::CanonicalScope,
                _: &str,
                _: &str,
                _: Value,
            ) -> Result<Value, crate::AgentHostError> {
                Ok(json!({}))
            }
        }
        let composition = crate::ScopeToolComposition::for_scope_with_permissions_and_project_chat(
            "project-agent",
            crate::CanonicalScope {
                scope_type: crate::CanonicalScopeType::AgentChat,
                scope_id: "chat".to_owned(),
                workspace_access: crate::WorkspaceAccess::Deny,
            },
            None,
            None,
            &["read_project".to_owned(), "propose_task".to_owned()]
                .into_iter()
                .collect(),
            true,
            Some(Arc::new(Noop)),
        )
        .unwrap();
        let tool = composition
            .tools()
            .into_iter()
            .find(|tool| tool.spec().name == "forge_scope_propose")
            .unwrap();
        let schema = tool.spec().input_schema.clone();
        assert!(
            schema["properties"]["payload"].get("properties").is_none(),
            "registered payload fields are declared once as generated root aliases"
        );
        let action = &schema["properties"]["action"];
        for field in [
            "verb",
            "reason",
            "guidance",
            "override",
            "fresh_session",
            "refresh_workspace",
            "reset_budget",
        ] {
            assert!(action["properties"].get(field).is_some(), "{field}");
        }
        let capture = Capture::default();
        let gemini = GeminiInteractionsProvider::new(
            capture.clone(),
            GeminiInteractionsConfig::new("https://example.test/v1beta", "gemini-test")
                .with_api_key(Secret::new("offline-test")),
        )
        .unwrap();
        let openai = OpenAiProvider::new(
            capture.clone(),
            OpenAiConfig::new("https://example.test/v1", "openai-test")
                .with_api_key(Secret::new("offline-test")),
        );
        for (provider, model) in [
            (&gemini as &dyn Provider, "gemini-test"),
            (&openai as &dyn Provider, "openai-test"),
        ] {
            let mut request = ProviderRequest::new(
                ModelId::new(model),
                vec![Message::user("Read the offered action")],
            );
            request.tools.push(ToolSchema {
                name: "forge_scope_propose".to_owned(),
                description: "Apply offered action".to_owned(),
                input_schema: schema.clone(),
            });
            let context = ProviderCallContext {
                session: SessionId::new("test"),
                request_id: RequestId::new("test"),
                attempt_id: AttemptId::new("test"),
                cache_identity: None,
                purpose: ProviderAttemptPurpose::Ordinary,
                cancel: Cancellation::new(),
                deadline: Deadline::never(),
            };
            let mut stream = provider.stream(request, context).await.unwrap();
            while stream.next().await.is_some() {}
        }
        let requests = capture.0.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0]["tools"][0]["parameters"], schema);
        assert_eq!(requests[1]["tools"][0]["function"]["parameters"], schema);
    }
}
