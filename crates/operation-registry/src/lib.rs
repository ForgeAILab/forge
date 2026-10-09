//! Transport-neutral operation contracts. Domain modules bind typed inputs to
//! handlers; consumers use the same catalog for schemas, decoding and dispatch.

pub mod authority;
mod input_check;
pub mod legacy_proposals;
pub mod main_proposals;
pub mod main_reads;
pub mod mcp;
pub mod project_proposals;
pub mod project_reads;
pub mod scope_reads;

use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    sync::LazyLock,
};

pub type HandlerFuture<'a, E> = Pin<Box<dyn Future<Output = Result<Value, E>> + Send + 'a>>;
pub type TypedHandler<I, E> = for<'a> fn(&'a dyn OperationContext<E>, I) -> HandlerFuture<'a, E>;
type ErasedHandler<E> = dyn for<'a> Fn(
        &'a dyn OperationContext<E>,
        Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, DispatchError<E>>> + Send + 'a>>
    + Send
    + Sync;

/// A service context implements the domain interfaces in this catalog.
/// This marker has no operation declarations or domain behaviour.
pub trait OperationContext<E>:
    scope_reads::ScopeReadContext<E>
    + project_reads::ProjectReadContext<E>
    + main_reads::MainReadContext<E>
    + main_proposals::MainProposalContext<E>
    + project_proposals::ProjectProposalContext<E>
    + legacy_proposals::LegacyProposalContext<E>
{
}
impl<
        E,
        T: scope_reads::ScopeReadContext<E>
            + project_reads::ProjectReadContext<E>
            + main_reads::MainReadContext<E>
            + main_proposals::MainProposalContext<E>
            + project_proposals::ProjectProposalContext<E>
            + legacy_proposals::LegacyProposalContext<E>,
    > OperationContext<E> for T
{
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectClass {
    Query,
    DirectCommand,
    ApprovalRequired,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AvailabilityRule {
    Always,
    ReadyOnly,
    SetupOnly,
    MainChatOnly,
}
/// Declarative requirements consumed by the shared EffectiveAuthority evaluator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorityRule {
    pub permissions: &'static [(&'static str, &'static str)],
    pub principal: authority::PrincipalRule,
    pub binding: &'static str,
}
impl AuthorityRule {
    pub fn permission(self, scope: &str) -> Option<&'static str> {
        self.permissions
            .iter()
            .find(|(s, _)| *s == scope)
            .map(|(_, p)| *p)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldProjection {
    ReadArguments,
    ProposalPayload,
}
#[derive(Debug, Clone, Copy)]
pub struct SurfaceBinding {
    pub native_aggregate: &'static str,
    pub projection: FieldProjection,
}
#[derive(Debug, Clone, Copy)]
pub enum StructuralConstraint {
    ClosedObject,
    MaxSerializedBytes(usize),
    NonNullableOptional(&'static str),
    UniqueItems(&'static str),
    AtLeastOne(&'static [&'static str]),
    /// A field the existing transport adapter discards before decoding.
    IgnoredField(&'static str),
    Required(&'static str),
    StringEnum {
        field: &'static str,
        values: &'static [&'static str],
    },
}

/// Erased at the catalog boundary after construction from a Serde + JsonSchema
/// input type and a handler that accepts exactly that type.
pub struct TypedInputContract {
    pub rust_type: &'static str,
    pub schema: Value,
    pub constraints: &'static [StructuralConstraint],
    decode: fn(Value) -> Result<(), String>,
}
impl TypedInputContract {
    fn fields(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.schema
            .get("properties")
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
    }
    fn is_required(&self, field: &str) -> bool {
        self.schema["required"]
            .as_array()
            .is_some_and(|required| required.iter().any(|name| name == field))
    }
    /// The whole argument contract on one line, for the advertised aggregate
    /// description and for validation errors: `no arguments`, or
    /// `{section: one of a|b, limit?}` (`?` marks an optional field).
    pub fn contract_line(&self) -> String {
        if let Some(variants) = self.schema["oneOf"].as_array() {
            return variants
                .iter()
                .map(|schema| {
                    let action = schema["properties"]["action"]["enum"][0]
                        .as_str()
                        .expect("action tag");
                    let fields = schema["properties"]
                        .as_object()
                        .unwrap()
                        .keys()
                        .filter(|field| field.as_str() != "action")
                        .map(|field| {
                            let required = schema["required"]
                                .as_array()
                                .is_some_and(|names| names.contains(&json!(field)));
                            format!("{field}{}", if required { "" } else { "?" })
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("action={action} {{{fields}}}")
                })
                .collect::<Vec<_>>()
                .join("; ");
        }
        let fields = self
            .fields()
            .map(|(name, schema)| {
                let values = schema.get("enum").and_then(Value::as_array).map(|values| {
                    values
                        .iter()
                        .map(|value| value.as_str().map_or(value.to_string(), str::to_owned))
                        .collect::<Vec<_>>()
                        .join("|")
                });
                let optional = if self.is_required(name) { "" } else { "?" };
                match values {
                    Some(values) => format!("{name}{optional}: one of {values}"),
                    None => format!("{name}{optional}"),
                }
            })
            .collect::<Vec<_>>();
        if fields.is_empty() {
            "no arguments".to_owned()
        } else {
            format!("{{{}}}", fields.join(", "))
        }
    }
}

pub struct OperationSpec<E> {
    pub id: &'static str,
    pub input: TypedInputContract,
    pub authority: AuthorityRule,
    pub effect: EffectClass,
    pub availability: AvailabilityRule,
    pub surfaces: &'static [SurfaceBinding],
    pub summary: &'static str,
    pub guidance: &'static str,
    handler: Box<ErasedHandler<E>>,
}
#[derive(Debug)]
pub enum DispatchError<E> {
    InvalidInput(String),
    Handler(E),
}

impl<E: Send + 'static> OperationSpec<E> {
    #[allow(clippy::too_many_arguments)]
    pub fn typed<I: DeserializeOwned + JsonSchema + Send + 'static>(
        id: &'static str,
        authority: AuthorityRule,
        effect: EffectClass,
        availability: AvailabilityRule,
        surfaces: &'static [SurfaceBinding],
        summary: &'static str,
        guidance: &'static str,
        constraints: &'static [StructuralConstraint],
        handler: TypedHandler<I, E>,
    ) -> Self {
        assert!(
            summary.len() <= 160 && guidance.len() <= 2048,
            "bounded operation guidance"
        );
        let mut schema = serde_json::to_value(
            schemars::gen::SchemaSettings::draft07()
                .with(|settings| settings.inline_subschemas = true)
                .into_generator()
                .into_root_schema_for::<I>(),
        )
        .expect("derived schema");
        // Canonical contracts are embedded, so root schema/title annotations are redundant.
        schema.as_object_mut().unwrap().remove("$schema");
        schema.as_object_mut().unwrap().remove("title");
        if let Some(variants) = schema["oneOf"].as_array() {
            let mut fields = serde_json::Map::new();
            for variant in variants {
                for (key, value) in variant["properties"]
                    .as_object()
                    .expect("tagged input properties")
                {
                    if key == "action" {
                        if let Some(existing) = fields.get_mut(key) {
                            let existing: &mut Value = existing;
                            existing["enum"]
                                .as_array_mut()
                                .unwrap()
                                .extend(value["enum"].as_array().unwrap().iter().cloned());
                            continue;
                        }
                    }
                    fields.insert(key.clone(), value.clone());
                }
            }
            schema["properties"] = Value::Object(fields);
            schema["type"] = json!("object");
        }
        schema["properties"] = schema.get("properties").cloned().unwrap_or(json!({}));
        schema["required"] = schema.get("required").cloned().unwrap_or(json!([]));
        for constraint in constraints {
            match constraint {
                StructuralConstraint::ClosedObject => {
                    schema["additionalProperties"] = json!(false);
                    if let Some(variants) = schema.get_mut("oneOf").and_then(Value::as_array_mut) {
                        for variant in variants {
                            variant["additionalProperties"] = json!(false);
                        }
                    }
                }
                StructuralConstraint::MaxSerializedBytes(_) => {}
                StructuralConstraint::NonNullableOptional(field) => {
                    if let Some(kinds) = schema["properties"][*field]["type"].as_array_mut() {
                        kinds.retain(|kind| kind != "null");
                        if kinds.len() == 1 {
                            schema["properties"][*field]["type"] = kinds[0].clone();
                        }
                    }
                }
                StructuralConstraint::UniqueItems(field) => {
                    schema["properties"][*field]["uniqueItems"] = json!(true)
                }
                StructuralConstraint::AtLeastOne(fields) => {
                    schema["anyOf"] = json!(fields.iter().map(|field| json!({"required":[field],"properties":{*field:{"type":"integer"}}})).collect::<Vec<_>>());
                }
                StructuralConstraint::IgnoredField(field) => {
                    schema["properties"].as_object_mut().unwrap().remove(*field);
                }
                StructuralConstraint::Required(field) => {
                    let required = schema["required"].as_array_mut().unwrap();
                    if !required.contains(&json!(field)) {
                        required.push(json!(field));
                    }
                }
                StructuralConstraint::StringEnum { field, values } => {
                    schema["properties"][*field] = json!({"type":"string","enum":values})
                }
            }
        }
        fn scalar_enums(value: &mut Value) {
            if let Some(object) = value.as_object_mut() {
                if let Some(values) = object.get("enum").and_then(Value::as_array) {
                    if values.len() == 1 {
                        object.insert("const".into(), values[0].clone());
                    }
                }
                for value in object.values_mut() {
                    scalar_enums(value);
                }
            } else if let Some(values) = value.as_array_mut() {
                for value in values {
                    scalar_enums(value);
                }
            }
        }
        scalar_enums(&mut schema);
        schema["required"]
            .as_array_mut()
            .unwrap()
            .sort_by(|a, b| a.as_str().cmp(&b.as_str()));
        Self {
            id,
            authority,
            effect,
            availability,
            surfaces,
            summary,
            guidance,
            input: TypedInputContract {
                rust_type: std::any::type_name::<I>(),
                schema,
                constraints,
                decode: |value| {
                    serde_json::from_value::<I>(value)
                        .map(|_| ())
                        .map_err(|e| e.to_string())
                },
            },
            handler: Box::new(move |context, mut value| {
                Box::pin(async move {
                    input_check::strip_ignored_fields(&mut value, constraints);
                    let input = serde_json::from_value::<I>(value)
                        .map_err(|e| DispatchError::InvalidInput(e.to_string()))?;
                    handler(context, input)
                        .await
                        .map_err(DispatchError::Handler)
                })
            }),
        }
    }
    pub fn canonical_schema(&self) -> Value {
        let mut schema = self.input.schema.clone();
        if !self.guidance.is_empty() {
            schema["description"] = json!(self.guidance);
        }
        schema
    }
    /// The one generated line a model is shown for this operation.
    pub fn contract_line(&self) -> String {
        format!("{}: {}", self.id, self.input.contract_line())
    }
    /// Enforce the argument contract on envelope-normalized arguments and
    /// return them in canonical form (see [`TypedInputContract::normalize`]).
    /// A violation names the operation, the offending field and the expected
    /// contract, and is returned to the model as an ordinary tool error.
    pub fn normalize_arguments(&self, arguments: &Value) -> Result<Value, String> {
        self.input
            .normalize(arguments)
            .map_err(|error| format!("{}: {error}; expected {}", self.id, self.contract_line()))
    }
    pub fn validate_arguments(&self, arguments: &Value) -> Result<(), String> {
        self.normalize_arguments(arguments).map(|_| ())
    }
    pub async fn dispatch(
        &self,
        context: &dyn OperationContext<E>,
        input: Value,
    ) -> Result<Value, DispatchError<E>> {
        let input = self
            .normalize_arguments(&input)
            .map_err(DispatchError::InvalidInput)?;
        self.dispatch_prepared(context, input).await
    }
    /// Invoke exact stored arguments without rechecking the current contract.
    /// Domain checks and the typed decoder still run.
    pub async fn dispatch_prepared(
        &self,
        context: &dyn OperationContext<E>,
        input: Value,
    ) -> Result<Value, DispatchError<E>> {
        (self.handler)(context, input).await
    }
}

pub struct OperationCatalog<E> {
    entries: BTreeMap<&'static str, OperationSpec<E>>,
}
impl<E: Send + 'static> OperationCatalog<E> {
    pub fn new(
        specs: impl IntoIterator<Item = OperationSpec<E>>,
        expected: &[&str],
    ) -> Result<Self, String> {
        let mut entries = BTreeMap::new();
        for spec in specs {
            let id = spec.id;
            if entries.insert(id, spec).is_some() {
                return Err(format!("operation id collision: {id}"));
            }
        }
        let actual = entries.keys().copied().collect::<BTreeSet<_>>();
        let expected_set = expected.iter().copied().collect::<BTreeSet<_>>();
        if actual != expected_set || expected_set.len() != expected.len() {
            return Err(format!(
                "incomplete catalog: expected {expected:?}, actual {actual:?}"
            ));
        }
        Ok(Self { entries })
    }
    pub fn lookup(&self, id: &str) -> Option<&OperationSpec<E>> {
        self.entries.get(id)
    }
    pub fn iter(&self) -> impl Iterator<Item = &OperationSpec<E>> {
        self.entries.values()
    }
}

/// Every registered read, from the domain modules' own id lists.
/// A domain module adds its ids next to its specs; the catalog refuses a
/// module whose specs and ids disagree and any id declared twice.
pub fn registered_reads() -> Vec<&'static str> {
    let mut ids = [scope_reads::IDS, project_reads::IDS, main_reads::IDS].concat();
    ids.sort_unstable();
    ids
}
/// Every operation in the registry, independent of effect class.
pub fn registered_operations() -> Vec<&'static str> {
    let mut ids = [
        registered_reads(),
        main_proposals::IDS.to_vec(),
        project_proposals::IDS.to_vec(),
        legacy_proposals::IDS.to_vec(),
    ]
    .concat();
    ids.sort_unstable();
    ids
}
pub fn read_catalog<E: Send + 'static>() -> OperationCatalog<E> {
    OperationCatalog::new(
        scope_reads::specs()
            .into_iter()
            .chain(project_reads::specs())
            .chain(main_reads::specs()),
        &registered_reads(),
    )
    .expect("complete read catalog")
}
pub static READ_CATALOG: LazyLock<OperationCatalog<std::convert::Infallible>> =
    LazyLock::new(read_catalog);

/// All proposal projections and dispatch use this same domain concatenation.
pub fn proposal_catalog<E: Send + 'static>() -> OperationCatalog<E> {
    let ids = [
        main_proposals::IDS,
        project_proposals::IDS,
        legacy_proposals::IDS,
    ]
    .concat();
    OperationCatalog::new(
        main_proposals::specs()
            .into_iter()
            .chain(project_proposals::specs())
            .chain(legacy_proposals::specs()),
        &ids,
    )
    .expect("complete proposal catalog")
}
pub static PROPOSAL_CATALOG: LazyLock<OperationCatalog<std::convert::Infallible>> =
    LazyLock::new(proposal_catalog);

#[cfg(test)]
mod tests;
