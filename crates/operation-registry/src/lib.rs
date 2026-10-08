//! Transport-neutral operation contracts. Domain modules bind typed inputs to
//! handlers; consumers use the same catalog for schemas, decoding and dispatch.

pub mod main_reads;
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
pub type TypedHandler<I, E> = for<'a> fn(&'a dyn ReadContext<E>, I) -> HandlerFuture<'a, E>;
type ErasedHandler<E> = dyn for<'a> Fn(
        &'a dyn ReadContext<E>,
        Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, DispatchError<E>>> + Send + 'a>>
    + Send
    + Sync;

/// A service context implements the domain interfaces in this catalog.
/// This marker has no operation declarations or domain behaviour.
pub trait ReadContext<E>:
    scope_reads::ScopeReadContext<E>
    + project_reads::ProjectReadContext<E>
    + main_reads::MainReadContext<E>
{
}
impl<
        E,
        T: scope_reads::ScopeReadContext<E>
            + project_reads::ProjectReadContext<E>
            + main_reads::MainReadContext<E>,
    > ReadContext<E> for T
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
/// The existing permission checks enforce these facts until EffectiveAuthority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorityRule {
    pub permissions: &'static [(&'static str, &'static str)],
    pub binding: &'static str,
}
impl AuthorityRule {
    pub fn permission(self, scope: &str) -> Option<&'static str> {
        self.permissions
            .iter()
            .find(|(s, _)| *s == scope)
            .map(|(_, p)| *p)
    }
    pub fn allows(self, scope: &str, permissions: &BTreeSet<String>) -> bool {
        self.permission(scope)
            .is_some_and(|p| permissions.contains(p))
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldProjection {
    ReadArguments,
}
#[derive(Debug, Clone, Copy)]
pub struct SurfaceBinding {
    pub native_aggregate: &'static str,
    pub projection: FieldProjection,
}
#[derive(Debug, Clone, Copy)]
pub enum StructuralConstraint {
    ClosedObject,
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
    /// Structural validation of already-normalized arguments. The error names
    /// the offending field; [`OperationSpec::validate_arguments`] adds the
    /// operation.
    fn validate(&self, value: &Value) -> Result<(), String> {
        let object = value.as_object().ok_or("arguments must be an object")?;
        for constraint in self.constraints {
            match constraint {
                StructuralConstraint::ClosedObject => {
                    if let Some(key) = object
                        .keys()
                        .find(|key| !self.fields().any(|(name, _)| name == *key))
                    {
                        return Err(format!("argument `{key}` is not admitted"));
                    }
                }
                StructuralConstraint::Required(field) if !object.contains_key(*field) => {
                    return Err(format!("argument `{field}` is required"))
                }
                StructuralConstraint::StringEnum { field, values }
                    if !object
                        .get(*field)
                        .and_then(Value::as_str)
                        .is_some_and(|v| values.contains(&v)) =>
                {
                    return Err(format!(
                        "argument `{field}` must be one of: {}",
                        values.join(", ")
                    ));
                }
                _ => {}
            }
        }
        for required in self.schema["required"].as_array().into_iter().flatten() {
            let field = required.as_str().expect("schema field name");
            if !object.contains_key(field) {
                return Err(format!("argument `{field}` is required"));
            }
        }
        for (field, schema) in self.fields() {
            let Some(value) = object.get(field) else {
                continue;
            };
            let accepts_type = |kind: &str| match kind {
                "null" => value.is_null(),
                "string" => value.is_string(),
                "boolean" => value.is_boolean(),
                "array" => value.is_array(),
                "object" => value.is_object(),
                "number" => value.is_number(),
                "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
                _ => false,
            };
            let type_ok = match &schema["type"] {
                Value::String(kind) => accepts_type(kind),
                Value::Array(kinds) => kinds
                    .iter()
                    .any(|kind| kind.as_str().is_some_and(accepts_type)),
                _ => true,
            };
            if !type_ok {
                return Err(format!(
                    "argument `{field}` must have type {}",
                    schema["type"]
                ));
            }
            if !value.is_null()
                && match schema["format"].as_str() {
                    Some("int64") => value.as_i64().is_none(),
                    Some("uint64") => value.as_u64().is_none(),
                    _ => false,
                }
            {
                return Err(format!(
                    "argument `{field}` is outside {}",
                    schema["format"]
                ));
            }
            if let Some(text) = value.as_str() {
                let length = text.chars().count() as u64;
                for (keyword, violates) in [("minLength", true), ("maxLength", false)] {
                    if let Some(bound) = schema[keyword].as_u64() {
                        if if violates {
                            length < bound
                        } else {
                            length > bound
                        } {
                            return Err(format!("argument `{field}` violates {keyword} {bound}"));
                        }
                    }
                }
            }
            if let Some(number) = value.as_f64() {
                for (keyword, lower) in [("minimum", true), ("maximum", false)] {
                    if let Some(bound) = schema[keyword].as_f64() {
                        if if lower {
                            number < bound
                        } else {
                            number > bound
                        } {
                            return Err(format!("argument `{field}` violates {keyword} {bound}"));
                        }
                    }
                }
            }
        }
        (self.decode)(value.clone())
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
        schema["properties"] = schema.get("properties").cloned().unwrap_or(json!({}));
        schema["required"] = schema.get("required").cloned().unwrap_or(json!([]));
        for constraint in constraints {
            match constraint {
                StructuralConstraint::ClosedObject => schema["additionalProperties"] = json!(false),
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
            handler: Box::new(move |context, value| {
                Box::pin(async move {
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
    /// Enforce the argument contract on normalized arguments. A violation
    /// names the operation, the offending field and the expected contract, and
    /// is returned to the model as an ordinary tool error.
    pub fn validate_arguments(&self, arguments: &Value) -> Result<(), String> {
        self.input
            .validate(arguments)
            .map_err(|error| format!("{}: {error}; expected {}", self.id, self.contract_line()))
    }
    pub async fn dispatch(
        &self,
        context: &dyn ReadContext<E>,
        input: Value,
    ) -> Result<Value, DispatchError<E>> {
        self.validate_arguments(&input)
            .map_err(DispatchError::InvalidInput)?;
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

/// Every registered read operation, from the domain modules' own id lists.
/// A domain module adds its ids next to its specs; the catalog refuses a
/// module whose specs and ids disagree and any id declared twice.
pub fn registered_operations() -> Vec<&'static str> {
    let mut ids = [scope_reads::IDS, project_reads::IDS, main_reads::IDS].concat();
    ids.sort_unstable();
    ids
}
pub fn read_catalog<E: Send + 'static>() -> OperationCatalog<E> {
    OperationCatalog::new(
        scope_reads::specs()
            .into_iter()
            .chain(project_reads::specs())
            .chain(main_reads::specs()),
        &registered_operations(),
    )
    .expect("complete read catalog")
}
pub static READ_CATALOG: LazyLock<OperationCatalog<std::convert::Infallible>> =
    LazyLock::new(read_catalog);

#[cfg(test)]
mod tests;
