use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use ts_rs::TS;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[ts(export)]
pub struct ReviewVerifyConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub start: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub ready_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub ready_command: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub ready_timeout_seconds: Option<u32>,
    pub checks: Vec<ReviewVerifyCheck>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub timeout_seconds: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[ts(export)]
pub struct ReviewVerifyCheck {
    pub name: String,
    pub command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub timeout_seconds: Option<u32>,
}

impl ReviewVerifyConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.ready_path.is_some() && self.ready_command.is_some() {
            return Err("verify.ready_command: cannot be combined with ready_path".into());
        }
        for (field, command) in [
            ("start", &self.start),
            ("ready_command", &self.ready_command),
        ] {
            if command
                .as_ref()
                .is_some_and(|command| command.trim().is_empty())
            {
                return Err(format!("verify.{field}: must be non-empty"));
            }
        }
        if self
            .ready_path
            .as_ref()
            .is_some_and(|path| !path.starts_with('/') || path.contains(['\r', '\n']))
        {
            return Err(
                "verify.ready_path: must be a URL path starting with / without line breaks".into(),
            );
        }
        validate_timeout(
            "verify.ready_timeout_seconds",
            self.ready_timeout_seconds,
            900,
        )?;
        validate_timeout("verify.timeout_seconds", self.timeout_seconds, 3600)?;
        if !(1..=16).contains(&self.checks.len()) {
            return Err("verify.checks: must contain 1–16 checks".into());
        }
        let mut names = BTreeSet::new();
        for (index, check) in self.checks.iter().enumerate() {
            if !(1..=32).contains(&check.name.len())
                || !check
                    .name
                    .bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
                || !names.insert(&check.name)
            {
                return Err(format!(
                    "verify.checks.{index}.name: must be unique and match [a-z0-9-]{{1,32}}"
                ));
            }
            if check.command.trim().is_empty() {
                return Err(format!("verify.checks.{index}.command: must be non-empty"));
            }
            validate_timeout(
                &format!("verify.checks.{index}.timeout_seconds"),
                check.timeout_seconds,
                3600,
            )?;
        }
        Ok(())
    }
}

fn validate_timeout(field: &str, value: Option<u32>, maximum: u32) -> Result<(), String> {
    if value.is_some_and(|value| !(1..=maximum).contains(&value)) {
        return Err(format!("{field}: must be between 1 and {maximum}"));
    }
    Ok(())
}

/// Validate opaque state/config documents at every write boundary. State
/// configs may nest the review configuration under `review`.
pub fn validate_review_verify_config(value: &Value) -> Result<(), String> {
    let config = value.get("review").unwrap_or(value);
    if let Some(verify) = config.get("verify").filter(|value| !value.is_null()) {
        // Inspect each field first so even a type error names its full path.
        let object = verify.as_object().ok_or("verify: must be an object")?;
        for field in ["start", "ready_path", "ready_command"] {
            if object
                .get(field)
                .is_some_and(|v| !v.is_null() && !v.is_string())
            {
                return Err(format!("verify.{field}: must be a string"));
            }
        }
        for field in ["ready_timeout_seconds", "timeout_seconds"] {
            if object
                .get(field)
                .is_some_and(|v| !v.is_null() && v.as_u64().is_none())
            {
                return Err(format!("verify.{field}: must be an integer"));
            }
        }
        for (field, maximum) in [("ready_timeout_seconds", 900), ("timeout_seconds", 3600)] {
            if object
                .get(field)
                .and_then(Value::as_u64)
                .is_some_and(|value| value == 0 || value > maximum)
            {
                return Err(format!("verify.{field}: must be between 1 and {maximum}"));
            }
        }
        let checks = object
            .get("checks")
            .and_then(Value::as_array)
            .ok_or("verify.checks: must be an array")?;
        for (index, check) in checks.iter().enumerate() {
            for field in ["name", "command"] {
                if check.get(field).and_then(Value::as_str).is_none() {
                    return Err(format!("verify.checks.{index}.{field}: must be a string"));
                }
            }
            if let Some(timeout) = check.get("timeout_seconds").filter(|v| !v.is_null()) {
                if timeout
                    .as_u64()
                    .is_none_or(|value| value == 0 || value > 3600)
                {
                    return Err(format!(
                        "verify.checks.{index}.timeout_seconds: must be between 1 and 3600"
                    ));
                }
            }
        }
        let parsed: ReviewVerifyConfig =
            serde_json::from_value(verify.clone()).map_err(|e| format!("verify.checks: {e}"))?;
        parsed.validate()?;
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ReviewVerifyStatus {
    Pass,
    Fail,
    Inconclusive,
    NotRun,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[ts(export)]
pub struct ReviewVerifyCheckResult {
    pub name: String,
    pub result: ReviewVerifyStatus,
    pub summary: String,
    #[serde(default)]
    pub evidence: Vec<String>,
    #[serde(default)]
    pub evidence_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub observed: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[ts(export)]
pub struct ReviewVerifyDetails {
    pub status: ReviewVerifyStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub reason: Option<String>,
    #[serde(default)]
    pub checks: Vec<ReviewVerifyCheckResult>,
    #[serde(default)]
    pub app_log_tail: String,
    #[serde(default)]
    pub notes: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn review_verify_config_validates_fields_names_and_limits() {
        let valid = json!({"start":"run app","ready_path":"/health","checks":[{"name":"smoke","command":"verify"}]});
        validate_review_verify_config(&json!({"verify":valid})).unwrap();
        validate_review_verify_config(&json!({"review":{"verify":valid}})).unwrap();
        for (patch, field) in [
            (json!({"ready_command":"true"}), "verify.ready_command"),
            (
                json!({"ready_timeout_seconds":0}),
                "verify.ready_timeout_seconds",
            ),
            (
                json!({"ready_timeout_seconds":901}),
                "verify.ready_timeout_seconds",
            ),
            (json!({"timeout_seconds":3601}), "verify.timeout_seconds"),
            (
                json!({"ready_path":"http://elsewhere/"}),
                "verify.ready_path",
            ),
            (json!({"checks":[]}), "verify.checks"),
            (
                json!({"checks":[{"name":"BAD","command":"x"}]}),
                "verify.checks.0.name",
            ),
            (
                json!({"checks":[{"name":"a","command":""}]}),
                "verify.checks.0.command",
            ),
            (
                json!({"checks":[{"name":"a","command":"x","timeout_seconds":0}]}),
                "verify.checks.0.timeout_seconds",
            ),
            (
                json!({"checks":[{"name":"a","command":"x"},{"name":"a","command":"y"}]}),
                "verify.checks.1.name",
            ),
        ] {
            let mut invalid = valid.clone();
            invalid
                .as_object_mut()
                .unwrap()
                .extend(patch.as_object().unwrap().clone());
            let error = validate_review_verify_config(&json!({"verify":invalid})).unwrap_err();
            assert!(error.contains(field), "{error}");
        }
        let mut config: ReviewVerifyConfig = serde_json::from_value(valid).unwrap();
        config.checks = vec![config.checks[0].clone(); 17];
        assert!(config.validate().unwrap_err().contains("1–16"));
    }
}
