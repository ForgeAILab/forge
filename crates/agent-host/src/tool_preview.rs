//! Bounded, credential-masked preview of a tool call's raw arguments, for
//! the chat activity log's `tool_call` entry.
//!
//! The runtime withholds tool-call argument *values* by default (only key
//! names cross the event boundary); Forge opts back in via
//! `RuntimeBuilder::emit_raw_tool_arguments(true)` and then reduces the raw
//! `serde_json::Value` down to this bounded shape before it is ever
//! persisted or rendered. The exact contract below is shared with the web
//! chat renderer, which is being built against it directly — do not change
//! it without updating both sides.

use std::sync::LazyLock;

use regex::Regex;
use serde_json::{Map, Value};

/// At most this many fields survive into a preview.
const MAX_FIELDS: usize = 8;
/// Values are truncated to this many characters before the trailing "…".
const MAX_VALUE_CHARS: usize = 160;
const TRUNCATION_MARK: char = '…';

/// Leaf keys (case-insensitive) whose values are dropped outright: not
/// secrets, just too large or too content-shaped to summarize into one
/// preview line.
const CONTENT_DENYLIST: &[&str] = &[
    "content", "contents", "text", "body", "patch", "diff", "data", "base64", "message", "prompt",
    "input", "output",
];

/// Leaf keys matching this (case-insensitive) are dropped outright: they
/// name a credential, not a value safe to echo into a chat log.
static SECRET_KEY_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)secret|token|password|passwd|credential|api[_-]?key|authorization|cookie|private",
    )
    .expect("secret key pattern is a valid regex")
});

/// `Bearer <token>` -> `Bearer ***`.
static BEARER_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(bearer)(\s+)\S+").expect("bearer pattern is valid"));

/// `token=<x>`, `token: <x>`, `password=<x>`, `api_key=<x>` -> label kept,
/// value replaced.
static LABELED_VALUE_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(token|api[_-]?key|password)(\s*[:=]\s*)\S+")
        .expect("labeled value pattern is valid")
});

/// `--token <x>` -> `--token ***`.
static FLAG_VALUE_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)(--token)(\s+)\S+").expect("flag value pattern is valid"));

/// Turns a tool call's raw JSON arguments into a bounded, flat preview map
/// safe to persist in the chat activity log and render verbatim in the UI.
///
/// - Non-object input (including a missing/`None` `arguments` value mapped
///   to `Value::Null` by the caller) yields an empty map.
/// - A top-level scalar (string/number/bool) argument becomes a string
///   value. A nested object contributes its own scalar children one level
///   down as dotted keys (`params.task_id`); deeper nesting and arrays of
///   objects are skipped. An array of scalars is joined with ", ".
/// - A leaf key matching [`CONTENT_DENYLIST`] or the secret-ish pattern is
///   dropped before the field cap is applied.
/// - Inline secrets that survive in a value (`Bearer <x>`, `token=<x>`, ...)
///   are masked to `***`, keeping the label.
/// - A surviving string is truncated to 160 chars plus a trailing "…".
/// - At most 8 fields land in the result, in the input map's own order.
pub fn build_tool_argument_preview(arguments: &Value) -> Map<String, Value> {
    let mut preview = Map::new();
    let Some(object) = arguments.as_object() else {
        return preview;
    };

    for (key, value) in object.iter() {
        if preview.len() >= MAX_FIELDS {
            break;
        }
        if is_denylisted_leaf(key) {
            continue;
        }
        if let Some(rendered) = scalar_preview(value) {
            preview.insert(key.clone(), Value::String(rendered));
            continue;
        }
        if let Value::Object(nested) = value {
            for (nested_key, nested_value) in nested.iter() {
                if preview.len() >= MAX_FIELDS {
                    break;
                }
                if is_denylisted_leaf(nested_key) {
                    continue;
                }
                if let Some(rendered) = scalar_preview(nested_value) {
                    preview.insert(format!("{key}.{nested_key}"), Value::String(rendered));
                }
            }
        }
    }

    preview
}

/// Renders a scalar, or an array made up only of scalars, to a finalized
/// (masked + truncated) string. Objects, arrays containing anything but
/// scalars, and `null` are not previewable and yield `None`.
fn scalar_preview(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(finalize(text)),
        Value::Number(number) => Some(finalize(&number.to_string())),
        Value::Bool(flag) => Some(finalize(&flag.to_string())),
        Value::Array(items) => {
            let mut rendered = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::String(text) => rendered.push(text.clone()),
                    Value::Number(number) => rendered.push(number.to_string()),
                    Value::Bool(flag) => rendered.push(flag.to_string()),
                    _ => return None,
                }
            }
            Some(finalize(&rendered.join(", ")))
        }
        Value::Null | Value::Object(_) => None,
    }
}

fn is_denylisted_leaf(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    CONTENT_DENYLIST.contains(&lower.as_str()) || SECRET_KEY_PATTERN.is_match(&lower)
}

fn finalize(raw: &str) -> String {
    truncate_chars(&mask_secrets(raw), MAX_VALUE_CHARS)
}

fn mask_secrets(input: &str) -> String {
    let masked = BEARER_PATTERN.replace_all(input, "$1$2***");
    let masked = LABELED_VALUE_PATTERN.replace_all(&masked, "$1$2***");
    let masked = FLAG_VALUE_PATTERN.replace_all(&masked, "$1$2***");
    masked.into_owned()
}

fn truncate_chars(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_owned();
    }
    let mut truncated: String = input.chars().take(max_chars).collect();
    truncated.push(TRUNCATION_MARK);
    truncated
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_object_input_yields_an_empty_map() {
        assert!(build_tool_argument_preview(&Value::Null).is_empty());
        assert!(build_tool_argument_preview(&serde_json::json!("plain string")).is_empty());
        assert!(build_tool_argument_preview(&serde_json::json!([1, 2, 3])).is_empty());
    }

    #[test]
    fn top_level_scalars_become_string_values() {
        let preview = build_tool_argument_preview(&serde_json::json!({
            "command": "cargo test -p db",
            "timeout_secs": 120,
            "verbose": true,
        }));

        assert_eq!(preview["command"], "cargo test -p db");
        assert_eq!(preview["timeout_secs"], "120");
        assert_eq!(preview["verbose"], "true");
    }

    #[test]
    fn dotted_nesting_pulls_one_level_of_scalar_children() {
        let preview = build_tool_argument_preview(&serde_json::json!({
            "params": {
                "task_id": "task-1",
                "nested": { "too_deep": "hidden" },
            },
        }));

        assert_eq!(preview["params.task_id"], "task-1");
        assert!(!preview.contains_key("params.nested"));
        assert!(!preview.contains_key("params.nested.too_deep"));
    }

    #[test]
    fn arrays_of_scalars_are_joined() {
        let preview = build_tool_argument_preview(&serde_json::json!({
            "tags": ["alpha", "beta", "gamma"],
            "mixed": ["alpha", {"nested": true}],
        }));

        assert_eq!(preview["tags"], "alpha, beta, gamma");
        assert!(!preview.contains_key("mixed"));
    }

    #[test]
    fn field_cap_keeps_only_the_first_eight_in_order() {
        let mut object = serde_json::Map::new();
        for index in 0..12 {
            object.insert(
                format!("field_{index:02}"),
                Value::String(index.to_string()),
            );
        }
        let preview = build_tool_argument_preview(&Value::Object(object));

        assert_eq!(preview.len(), 8);
        assert!(preview.contains_key("field_00"));
        assert!(preview.contains_key("field_07"));
        assert!(!preview.contains_key("field_08"));
    }

    #[test]
    fn content_ish_keys_are_dropped() {
        let preview = build_tool_argument_preview(&serde_json::json!({
            "command": "ls",
            "content": "should never appear",
            "diff": "should never appear either",
        }));

        assert!(preview.contains_key("command"));
        assert!(!preview.contains_key("content"));
        assert!(!preview.contains_key("diff"));
    }

    #[test]
    fn secret_ish_keys_are_dropped() {
        let preview = build_tool_argument_preview(&serde_json::json!({
            "command": "curl",
            "api_key": "sk-super-secret",
            "Authorization": "Bearer abc",
            "params": { "task_id": "task-1", "password": "hunter2" },
        }));

        assert!(preview.contains_key("command"));
        assert!(!preview.contains_key("api_key"));
        assert!(!preview.contains_key("Authorization"));
        assert!(preview.contains_key("params.task_id"));
        assert!(!preview.contains_key("params.password"));
    }

    #[test]
    fn inline_secrets_in_surviving_values_are_masked() {
        let preview = build_tool_argument_preview(&serde_json::json!({
            "command": "http-client -H 'Authorization: Bearer test-bearer-value' --token xyz789 https://example.com",
            "note": "token=abc123 and password=hunter2 and api_key=test-api-key-value",
        }));

        let command = preview["command"].as_str().expect("command is a string");
        assert!(command.contains("Bearer ***"));
        assert!(command.contains("--token ***"));
        assert!(!command.contains("sk-abc123"));
        assert!(!command.contains("xyz789"));

        let note = preview["note"].as_str().expect("note is a string");
        assert_eq!(note, "token=*** and password=*** and api_key=***");
    }

    #[test]
    fn long_strings_are_truncated_with_an_ellipsis() {
        let long_value = "a".repeat(200);
        let preview = build_tool_argument_preview(&serde_json::json!({ "blob": long_value }));

        let rendered = preview["blob"].as_str().expect("blob is a string");
        assert_eq!(rendered.chars().count(), MAX_VALUE_CHARS + 1);
        assert!(rendered.ends_with(TRUNCATION_MARK));
        assert!(rendered.starts_with(&"a".repeat(MAX_VALUE_CHARS)));
    }

    #[test]
    fn short_strings_are_left_untouched() {
        let preview = build_tool_argument_preview(&serde_json::json!({ "short": "hello" }));
        assert_eq!(preview["short"], "hello");
    }
}
