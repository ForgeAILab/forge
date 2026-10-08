//! Enforcement of a typed input contract from its canonical schema: closed
//! object, required fields, scalar types, string lengths and numeric bounds,
//! then the Serde decode. Generic over every registered operation.
use crate::{StructuralConstraint, TypedInputContract};
use serde_json::Value;

/// Largest magnitude at which every integer is exactly representable as f64.
const MAX_EXACT_FLOAT_INTEGER: f64 = 9_007_199_254_740_992.0;

fn admits(schema: &Value, kind: &str) -> bool {
    match &schema["type"] {
        Value::String(declared) => declared == kind,
        Value::Array(declared) => declared.iter().any(|declared| declared == kind),
        _ => false,
    }
}

/// Providers routinely send an integer as a string (`"10"`) or as a float
/// (`10.0`). Both are the integer; nothing else is rewritten, so `"ten"`,
/// `1.5` and `true` still fail the type check that follows.
fn integer_spelling(value: &Value) -> Option<Value> {
    match value {
        Value::String(text) => text
            .parse::<i64>()
            .map(Value::from)
            .or_else(|_| text.parse::<u64>().map(Value::from))
            .ok(),
        Value::Number(number) if !number.is_i64() && !number.is_u64() => {
            let float = number.as_f64()?;
            if float.fract() != 0.0 || float.abs() > MAX_EXACT_FLOAT_INTEGER {
                None
            } else if float < 0.0 {
                Some(Value::from(float as i64))
            } else {
                Some(Value::from(float as u64))
            }
        }
        _ => None,
    }
}

pub(crate) fn strip_ignored_fields(value: &mut Value, constraints: &[StructuralConstraint]) {
    if let Some(object) = value.as_object_mut() {
        for constraint in constraints {
            if let StructuralConstraint::IgnoredField(field) = constraint {
                object.remove(*field);
            }
        }
    }
}

impl TypedInputContract {
    /// Validate envelope-normalized arguments and return their canonical
    /// form: an integer field sent as an integer-valued string or float
    /// becomes the integer. The advertised and canonical schemas are not
    /// widened; the field stays an integer. The error names the offending
    /// field; [`crate::OperationSpec::normalize_arguments`] adds the operation.
    pub(crate) fn normalize(&self, value: &Value) -> Result<Value, String> {
        let mut value = value.clone();
        strip_ignored_fields(&mut value, self.constraints);
        let mut object = value
            .as_object()
            .ok_or("arguments must be an object")?
            .clone();
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
            let Some(value) = object.get_mut(field) else {
                continue;
            };
            if admits(schema, "integer") && !admits(schema, "string") && !admits(schema, "number") {
                if let Some(integer) = integer_spelling(value) {
                    *value = integer;
                }
            }
            let value = &*value;
            let accepts_type = |kind: &str| match kind {
                "null" => value.is_null(),
                "string" => value.is_string(),
                "boolean" => value.is_boolean(),
                "array" => value.is_array(),
                "object" => value.is_object(),
                "number" => value.is_number(),
                "integer" => value.is_i64() || value.is_u64(),
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
            if !value.is_null() {
                match schema["format"].as_str() {
                    Some("int64") if !value.is_i64() => {
                        return Err(format!("argument `{field}` is outside int64"));
                    }
                    Some("uint64") if !value.is_u64() => {
                        return Err(format!("argument `{field}` must be a non-negative integer"));
                    }
                    _ => {}
                }
            }
            if let Some(text) = value.as_str() {
                let length = text.chars().count() as u64;
                if let Some(bound) = schema["minLength"].as_u64() {
                    if length < bound {
                        return Err(format!("argument `{field}` violates minLength {bound}"));
                    }
                }
                if let Some(bound) = schema["maxLength"].as_u64() {
                    if length > bound {
                        return Err(format!("argument `{field}` violates maxLength {bound}"));
                    }
                }
            }
            if let Some(number) = value.as_f64() {
                if let Some(bound) = schema["minimum"].as_f64() {
                    if number < bound {
                        return Err(format!("argument `{field}` violates minimum {bound}"));
                    }
                }
                if let Some(bound) = schema["maximum"].as_f64() {
                    if number > bound {
                        return Err(format!("argument `{field}` violates maximum {bound}"));
                    }
                }
            }
        }
        let normalized = Value::Object(object);
        (self.decode)(normalized.clone())?;
        Ok(normalized)
    }
}
