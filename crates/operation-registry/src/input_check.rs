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

/// Recursively enforce the derived contract, including the selected tagged
/// variant. Conditional schema stays server-side; providers see one line.
fn normalize_schema(schema: &Value, value: &Value, field: &str) -> Result<Value, String> {
    let label = if field.is_empty() {
        "arguments".into()
    } else {
        format!("argument `{field}`")
    };
    if let Some(variants) = schema["oneOf"].as_array() {
        if variants
            .iter()
            .all(|variant| variant["properties"]["action"]["enum"].is_array())
        {
            let action = value
                .get("action")
                .and_then(Value::as_str)
                .ok_or("argument `action` is required")?;
            let variant = variants
                .iter()
                .find(|variant| {
                    variant["properties"]["action"]["enum"]
                        .as_array()
                        .unwrap()
                        .contains(&Value::from(action))
                })
                .ok_or("argument `action` is outside this typed contract")?;
            return normalize_schema(variant, value, field);
        }
    }
    if let Some(variants) = schema["anyOf"].as_array() {
        let mut last = None;
        let mut found = None;
        for variant in variants {
            match normalize_schema(variant, value, field) {
                Ok(value) => {
                    found = Some(value);
                    break;
                }
                Err(error) => last = Some(error),
            }
        }
        if found.is_none() {
            return Err(last.unwrap_or_else(|| format!("{label} has no admitted variant")));
        }
    }
    let mut value = value.clone();
    if admits(schema, "integer") && !admits(schema, "string") && !admits(schema, "number") {
        if let Some(integer) = integer_spelling(&value) {
            value = integer;
        }
    }
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
        return Err(format!("{label} must have type {}", schema["type"]));
    }
    if let Some(values) = schema["enum"].as_array() {
        if !values.contains(&value) {
            return Err(format!(
                "{label} must be one of: {}",
                values
                    .iter()
                    .map(Value::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    }
    if !value.is_null() {
        match schema["format"].as_str() {
            Some("int64") if !value.is_i64() => return Err(format!("{label} is outside int64")),
            Some("uint64") if !value.is_u64() => {
                return Err(format!("{label} must be a non-negative integer"))
            }
            _ => {}
        }
    }
    if let Some(text) = value.as_str() {
        let length = text.chars().count() as u64;
        if let Some(bound) = schema["minLength"].as_u64() {
            if length < bound {
                return Err(format!("{label} violates minLength {bound}"));
            }
        }
        if let Some(bound) = schema["maxLength"].as_u64() {
            if length > bound {
                return Err(format!("{label} violates maxLength {bound}"));
            }
        }
    }
    if let Some(number) = value.as_f64() {
        if let Some(bound) = schema["minimum"].as_f64() {
            if number < bound {
                return Err(format!("{label} violates minimum {bound}"));
            }
        }
        if let Some(bound) = schema["maximum"].as_f64() {
            if number > bound {
                return Err(format!("{label} violates maximum {bound}"));
            }
        }
    }
    if let Some(values) = value.as_array_mut() {
        for keyword in ["minItems", "maxItems"] {
            if let Some(bound) = schema[keyword].as_u64() {
                if (keyword == "minItems" && values.len() < bound as usize)
                    || (keyword == "maxItems" && values.len() > bound as usize)
                {
                    return Err(format!("{label} violates {keyword} {bound}"));
                }
            }
        }
        for (index, item) in values.iter_mut().enumerate() {
            *item = normalize_schema(&schema["items"], item, &format!("{field}[{index}]"))?;
        }
        if schema["uniqueItems"] == true {
            for (index, item) in values.iter().enumerate() {
                if values[..index].contains(item) {
                    return Err(format!("{label} must contain unique items"));
                }
            }
        }
    }
    if let Some(object) = value.as_object_mut() {
        let properties = schema["properties"].as_object();
        if schema["additionalProperties"] == false {
            if let Some(key) = object
                .keys()
                .find(|key| !properties.is_some_and(|properties| properties.contains_key(*key)))
            {
                let path = if field.is_empty() {
                    key.clone()
                } else {
                    format!("{field}.{key}")
                };
                return Err(format!("argument `{path}` is not admitted"));
            }
        }
        for required in schema["required"].as_array().into_iter().flatten() {
            let key = required.as_str().expect("schema field name");
            if !object.contains_key(key) {
                return Err(format!("argument `{key}` is required"));
            }
        }
        for (key, property) in properties.into_iter().flatten() {
            if let Some(item) = object.get_mut(key) {
                let path = if field.is_empty() {
                    key.clone()
                } else {
                    format!("{field}.{key}")
                };
                *item = normalize_schema(property, item, &path)?;
            }
        }
    }
    Ok(value)
}
impl TypedInputContract {
    pub(crate) fn normalize(&self, value: &Value) -> Result<Value, String> {
        let mut value = value.clone();
        strip_ignored_fields(&mut value, self.constraints);
        value.as_object().ok_or("arguments must be an object")?;
        for constraint in self.constraints {
            match constraint {
                StructuralConstraint::ClosedObject => {
                    if let Some(field) = value.as_object().unwrap().keys().find(|key| {
                        !self.schema["properties"]
                            .as_object()
                            .is_some_and(|fields| fields.contains_key(*key))
                    }) {
                        return Err(format!("argument `{field}` is not admitted"));
                    }
                }
                StructuralConstraint::Required(field) if value.get(*field).is_none() => {
                    return Err(format!("argument `{field}` is required"))
                }
                StructuralConstraint::StringEnum { field, values }
                    if !value
                        .get(*field)
                        .and_then(Value::as_str)
                        .is_some_and(|v| values.contains(&v)) =>
                {
                    return Err(format!(
                        "argument `{field}` must be one of: {}",
                        values.join(", ")
                    ))
                }
                StructuralConstraint::MaxSerializedBytes(bound)
                    if serde_json::to_vec(&value).map_err(|e| e.to_string())?.len() > *bound =>
                {
                    return Err(format!("payload exceeds {bound} serialized UTF-8 bytes"))
                }
                StructuralConstraint::AtLeastOne(fields)
                    if !fields
                        .iter()
                        .any(|field| value.get(*field).is_some_and(|value| !value.is_null())) =>
                {
                    return Err(format!(
                        "at least one argument `{}` is required",
                        fields.join("` or `")
                    ))
                }
                _ => {}
            }
        }
        let normalized = normalize_schema(&self.schema, &value, "")?;
        (self.decode)(normalized.clone())?;
        Ok(normalized)
    }
}
