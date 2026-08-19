use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::any::Any;
use std::collections::HashMap;

use crate::{ExecutionOverrides, ExecutorError, ExecutorKind};

/// Shared command override fields embedded in every typed config.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct CommandOverrides {
    pub base_command_override: Option<String>,
    pub additional_params: Option<Vec<String>>,
    pub env: Option<HashMap<String, String>>,
}

/// Forge-hosted Agent Runtime configuration.
///
/// Embedded profiles are resolved here so task snapshots have the same typed,
/// deterministic normalization as CLI profiles.  Credential handles and
/// authority grants deliberately do not belong in this config: the native
/// host resolves those from the selected immutable profile and canonical Task
/// scope.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct EmbeddedConfig {
    pub base_url: Option<String>,
    pub context_tokens: Option<u32>,
    pub max_input_tokens: Option<u32>,
    pub max_output_tokens: Option<u32>,
    pub runtime_revision: Option<String>,
    pub prompt_template: Option<String>,
}

/// Cross-executor permission abstraction.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PermissionPolicy {
    Auto,
    #[default]
    Supervised,
    Plan,
}

impl std::fmt::Display for PermissionPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Auto => write!(f, "auto"),
            Self::Supervised => write!(f, "supervised"),
            Self::Plan => write!(f, "plan"),
        }
    }
}

impl std::str::FromStr for PermissionPolicy {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "auto" => Ok(Self::Auto),
            "supervised" => Ok(Self::Supervised),
            "plan" => Ok(Self::Plan),
            other => Err(format!("unknown permission policy: {other}")),
        }
    }
}

/// Shell executor config.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct ShellConfig {
    pub command: Option<String>,
    pub args: Option<Vec<String>>,
    pub timeout_seconds: Option<u64>,
    pub permission_policy: Option<PermissionPolicy>,
    #[serde(flatten)]
    pub command_overrides: CommandOverrides,
}

/// Codex executor config. Field names compatible with Vibe Kanban.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct CodexConfig {
    pub model: Option<String>,
    pub sandbox: Option<String>,
    pub ask_for_approval: Option<String>,
    pub model_reasoning_effort: Option<String>,
    pub model_reasoning_summary: Option<String>,
    pub profile: Option<String>,
    pub base_instructions: Option<String>,
    pub developer_instructions: Option<String>,
    pub include_apply_patch_tool: Option<bool>,
    pub resume_thread_id: Option<String>,
    /// Start the next turn on `resume_thread_id` instead of forking a derived thread.
    ///
    /// Coding/chat follow-ups should keep the same agent session so Codex can reuse
    /// thread history and cache state. Review-style runs may intentionally omit this
    /// and fork from the source thread to inspect the prior work in a separate run.
    pub resume_thread_in_place: Option<bool>,
    /// Prompt used only when an in-place resume cannot find the stored Codex thread.
    pub resume_fallback_prompt: Option<String>,
    pub auto_commit: Option<bool>,
    pub permission_policy: Option<PermissionPolicy>,
    pub prompt_template: Option<String>,
    #[serde(flatten)]
    pub command_overrides: CommandOverrides,
}

/// Claude Code executor config. Field names compatible with Vibe Kanban.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct ClaudeCodeConfig {
    pub model: Option<String>,
    pub plan: Option<bool>,
    pub approvals: Option<String>,
    pub effort: Option<String>,
    pub agent: Option<String>,
    /// Claude Code session id used for follow-up turns.
    pub resume_session_id: Option<String>,
    pub dangerously_skip_permissions: Option<bool>,
    pub claude_code_router: Option<bool>,
    pub disable_api_key: Option<bool>,
    pub permission_policy: Option<PermissionPolicy>,
    pub prompt_template: Option<String>,
    #[serde(flatten)]
    pub command_overrides: CommandOverrides,
}

/// Cursor Agent CLI executor config.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct CursorConfig {
    pub model: Option<String>,
    pub force: Option<bool>,
    pub resume_session_id: Option<String>,
    pub permission_policy: Option<PermissionPolicy>,
    pub prompt_template: Option<String>,
    #[serde(flatten)]
    pub command_overrides: CommandOverrides,
}

/// OpenCode executor config. Field names compatible with Vibe Kanban.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct OpencodeConfig {
    pub model: Option<String>,
    pub variant: Option<String>,
    pub agent: Option<String>,
    pub auto_approve: Option<bool>,
    pub auto_compact: Option<bool>,
    pub permission_policy: Option<PermissionPolicy>,
    pub prompt_template: Option<String>,
    pub resume_session_id: Option<String>,
    #[serde(flatten)]
    pub command_overrides: CommandOverrides,
}

/// Gemini CLI executor config.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct GeminiConfig {
    pub model: Option<String>,
    pub sandbox: Option<String>,
    pub yolo: Option<bool>,
    pub check_every_n: Option<u32>,
    pub permission_policy: Option<PermissionPolicy>,
    pub prompt_template: Option<String>,
    /// Gemini CLI session UUID to resume (`--resume <id>`). Set by the
    /// workflow-guard follow-up path so bounce-back turns keep the session.
    pub resume_session_id: Option<String>,
    #[serde(flatten)]
    pub command_overrides: CommandOverrides,
}

/// Smith CLI executor config.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct SmithConfig {
    pub model: Option<String>,
    pub provider: Option<String>,
    pub profile: Option<String>,
    /// Reasoning effort name, forwarded as `--effort`. Populated from the agent's
    /// `reasoning_effort`. Smith validates the value against the selected
    /// provider/model ladder and refuses an unsupported one.
    pub effort: Option<String>,
    pub yolo: Option<bool>,
    pub approval: Option<String>,
    pub resume_session_id: Option<String>,
    pub permission_policy: Option<PermissionPolicy>,
    pub prompt_template: Option<String>,
    #[serde(flatten)]
    pub command_overrides: CommandOverrides,
}

/// Null executor config. Completes after a configurable delay.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct NullConfig {
    #[serde(default = "default_delay_seconds")]
    pub delay_seconds: u64,
}

fn default_delay_seconds() -> u64 {
    5
}

impl Default for NullConfig {
    fn default() -> Self {
        Self {
            delay_seconds: default_delay_seconds(),
        }
    }
}

/// Deserialize a raw JSON config into the typed config struct for an executor kind.
pub fn deserialize_config(
    kind: ExecutorKind,
    json: &Value,
) -> Result<Box<dyn Any + Send + Sync>, ExecutorError> {
    match kind {
        ExecutorKind::Embedded => deserialize_typed::<EmbeddedConfig>(kind, json),
        ExecutorKind::Shell => deserialize_typed::<ShellConfig>(kind, json),
        ExecutorKind::Codex => deserialize_typed::<CodexConfig>(kind, json),
        ExecutorKind::ClaudeCode => deserialize_typed::<ClaudeCodeConfig>(kind, json),
        ExecutorKind::Cursor => deserialize_typed::<CursorConfig>(kind, json),
        ExecutorKind::Opencode => deserialize_typed::<OpencodeConfig>(kind, json),
        ExecutorKind::Gemini => deserialize_typed::<GeminiConfig>(kind, json),
        ExecutorKind::Smith => deserialize_typed::<SmithConfig>(kind, json),
        ExecutorKind::Null => deserialize_typed::<NullConfig>(kind, json),
    }
}

/// Apply per-execution overrides to a config JSON object in-place.
pub fn merge_overrides(
    config: &mut Value,
    overrides: &ExecutionOverrides,
) -> Result<(), ExecutorError> {
    let Value::Object(map) = config else {
        return Err(ExecutorError::Other(
            "profile config_json must be a JSON object".to_owned(),
        ));
    };

    if let Some(model_id) = &overrides.model_id {
        map.insert("model".to_owned(), Value::String(model_id.clone()));
    }
    if let Some(reasoning_effort) = &overrides.reasoning_effort {
        map.insert(
            "model_reasoning_effort".to_owned(),
            Value::String(reasoning_effort.clone()),
        );
        map.insert("effort".to_owned(), Value::String(reasoning_effort.clone()));
    }
    if let Some(permission_policy) = &overrides.permission_policy {
        map.insert(
            "permission_policy".to_owned(),
            Value::String(permission_policy.clone()),
        );
    }

    Ok(())
}

/// Resolve config JSON by applying overrides, deserializing into the typed struct,
/// and serializing back to normalized JSON.
pub fn resolve_config_value(
    kind: ExecutorKind,
    json: &Value,
    overrides: &ExecutionOverrides,
) -> Result<Value, ExecutorError> {
    let mut merged = json.clone();
    merge_overrides(&mut merged, overrides)?;
    match kind {
        ExecutorKind::Embedded => normalize_typed::<EmbeddedConfig>(kind, &merged),
        ExecutorKind::Shell => normalize_typed::<ShellConfig>(kind, &merged),
        ExecutorKind::Codex => normalize_typed::<CodexConfig>(kind, &merged),
        ExecutorKind::ClaudeCode => normalize_typed::<ClaudeCodeConfig>(kind, &merged),
        ExecutorKind::Cursor => normalize_typed::<CursorConfig>(kind, &merged),
        ExecutorKind::Opencode => normalize_typed::<OpencodeConfig>(kind, &merged),
        ExecutorKind::Gemini => normalize_typed::<GeminiConfig>(kind, &merged),
        ExecutorKind::Smith => normalize_typed::<SmithConfig>(kind, &merged),
        ExecutorKind::Null => normalize_typed::<NullConfig>(kind, &merged),
    }
}

fn deserialize_typed<T>(
    kind: ExecutorKind,
    json: &Value,
) -> Result<Box<dyn Any + Send + Sync>, ExecutorError>
where
    T: for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    serde_json::from_value::<T>(json.clone())
        .map(|config| Box::new(config) as Box<dyn Any + Send + Sync>)
        .map_err(|error| {
            ExecutorError::Other(format!("Failed to deserialize {} config: {error}", kind))
        })
}

fn normalize_typed<T>(kind: ExecutorKind, json: &Value) -> Result<Value, ExecutorError>
where
    T: for<'de> Deserialize<'de> + Serialize,
{
    let config = serde_json::from_value::<T>(json.clone()).map_err(|error| {
        ExecutorError::Other(format!("Failed to deserialize {} config: {error}", kind))
    })?;
    serde_json::to_value(config).map_err(|error| {
        ExecutorError::Other(format!("Failed to serialize {} config: {error}", kind))
    })
}

/// Authored agent-config key holding the ordered fallback candidates.
/// Extracted before config normalization (which drops unknown fields).
pub const FALLBACKS_CONFIG_KEY: &str = "fallbacks";

/// Snapshot key carrying the resolved route.
pub const ROUTING_SNAPSHOT_KEY: &str = "routing";

/// The only routing policy currently defined.
pub const ROUTING_POLICY_ORDERED_FALLBACK_V1: &str = "ordered_fallback_v1";

/// Config fields that bind a session to a prior run. Excluded from candidate
/// identity so an injected resume id does not change which candidate a config
/// belongs to.
const SESSION_SCOPED_CONFIG_KEYS: &[&str] = &[
    "resume_session_id",
    "resume_thread_id",
    "resume_thread_in_place",
    "resume_fallback_prompt",
];

/// One executor candidate in an ordered fallback route.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorCandidate {
    pub executor_type: ExecutorKind,
    pub config: Value,
}

/// Outcome of one candidate attempt, persisted for route provenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteAttemptOutcome {
    UsageExhausted,
    Unavailable,
    SkippedCooldown,
    Failed,
    Cancelled,
    Completed,
}

impl RouteAttemptOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UsageExhausted => "usage_exhausted",
            Self::Unavailable => "unavailable",
            Self::SkippedCooldown => "skipped_cooldown",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Completed => "completed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteAttempt {
    pub candidate_key: String,
    pub outcome: RouteAttemptOutcome,
}

/// First-class route carried on the executor config snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorRouting {
    pub policy: String,
    pub candidates: Vec<ExecutorCandidate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_candidate_key: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attempts: Vec<RouteAttempt>,
}

/// Stable identity of a candidate: executor kind + human-readable
/// discriminators + a hash over the session-stripped config. Keys ordering,
/// sticky selection, and session compatibility.
pub fn candidate_key(kind: &ExecutorKind, config: &Value) -> String {
    let mut discriminators = vec![kind.to_string()];
    for field in ["profile", "provider", "model"] {
        if let Some(value) = config.get(field).and_then(Value::as_str) {
            discriminators.push(format!("{field}={value}"));
        }
    }
    format!(
        "{}#{:08x}",
        discriminators.join(":"),
        stable_config_hash(config)
    )
}

/// Identity of the quota pool a candidate consumes. Candidates sharing an
/// account key share cooldowns. For Smith the pool is the provider (Smith
/// rotates that provider's credentials natively); for Codex it is the
/// profile; other executors have one machine-level account.
pub fn account_key(kind: &ExecutorKind, config: &Value) -> String {
    let discriminator = match kind {
        ExecutorKind::Smith => config
            .get("provider")
            .and_then(Value::as_str)
            .or_else(|| config.get("profile").and_then(Value::as_str)),
        ExecutorKind::Codex => config.get("profile").and_then(Value::as_str),
        _ => None,
    };
    match discriminator {
        Some(value) => format!("{kind}:{value}"),
        None => kind.to_string(),
    }
}

/// FNV-1a over a canonical (key-sorted, session-stripped) rendering of the
/// config. Deliberately not `DefaultHasher`, whose output may change across
/// releases — these keys persist in execution snapshots.
fn stable_config_hash(config: &Value) -> u32 {
    fn canonicalize(value: &Value, out: &mut String, strip_session_keys: bool) {
        match value {
            Value::Object(map) => {
                out.push('{');
                let mut keys: Vec<&String> = map.keys().collect();
                keys.sort();
                for key in keys {
                    if strip_session_keys && SESSION_SCOPED_CONFIG_KEYS.contains(&key.as_str()) {
                        continue;
                    }
                    out.push_str(key);
                    out.push(':');
                    canonicalize(&map[key], out, false);
                    out.push(',');
                }
                out.push('}');
            }
            Value::Array(items) => {
                out.push('[');
                for item in items {
                    canonicalize(item, out, false);
                    out.push(',');
                }
                out.push(']');
            }
            other => out.push_str(&other.to_string()),
        }
    }

    let mut canonical = String::new();
    canonicalize(config, &mut canonical, true);
    let mut hash: u32 = 0x811c9dc5;
    for byte in canonical.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

/// Build and validate an ordered-fallback route from a normalized primary
/// candidate plus the raw authored `fallbacks` entries.
pub fn build_ordered_fallback_routing(
    primary_kind: ExecutorKind,
    primary_config: Value,
    fallbacks: &[Value],
) -> Result<ExecutorRouting, ExecutorError> {
    if primary_kind == ExecutorKind::Embedded {
        return Err(ExecutorError::Other(
            "embedded executor is hosted by Forge and cannot use CLI fallback routing".to_owned(),
        ));
    }
    let mut candidates = vec![ExecutorCandidate {
        executor_type: primary_kind,
        config: primary_config,
    }];
    for (index, entry) in fallbacks.iter().enumerate() {
        let object = entry.as_object().ok_or_else(|| {
            ExecutorError::Other(format!("fallbacks[{index}] must be a JSON object"))
        })?;
        let executor_type = object
            .get("executor_type")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ExecutorError::Other(format!("fallbacks[{index}] is missing executor_type"))
            })?;
        let kind = executor_type.parse::<ExecutorKind>().map_err(|_| {
            ExecutorError::Other(format!(
                "fallbacks[{index}] names unknown executor type: {executor_type}"
            ))
        })?;
        if kind == ExecutorKind::Embedded {
            return Err(ExecutorError::Other(
                "embedded executor is hosted by Forge and cannot be a CLI fallback candidate"
                    .to_owned(),
            ));
        }
        let raw_config = object
            .get("config")
            .cloned()
            .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
        if !raw_config.is_object() {
            return Err(ExecutorError::Other(format!(
                "fallbacks[{index}] config must be a JSON object"
            )));
        }
        let normalized =
            resolve_config_value(kind.clone(), &raw_config, &ExecutionOverrides::default())?;
        candidates.push(ExecutorCandidate {
            executor_type: kind,
            config: normalized,
        });
    }

    let mut seen = std::collections::HashSet::new();
    for candidate in &candidates {
        let key = candidate_key(&candidate.executor_type, &candidate.config);
        if !seen.insert(key.clone()) {
            return Err(ExecutorError::Other(format!(
                "duplicate executor candidate: {key}"
            )));
        }
    }

    Ok(ExecutorRouting {
        policy: ROUTING_POLICY_ORDERED_FALLBACK_V1.to_owned(),
        candidates,
        selected_candidate_key: None,
        attempts: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codex_config_round_trips_and_drops_unknown_fields() {
        let value = serde_json::json!({
            "model": "o3",
            "sandbox": "danger-full-access",
            "resume_thread_id": "thread-1",
            "auto_commit": false,
            "unknown_field": true,
            "additional_params": ["--verbose"]
        });

        let resolved =
            resolve_config_value(ExecutorKind::Codex, &value, &ExecutionOverrides::default())
                .expect("config resolves");

        assert_eq!(resolved["model"], "o3");
        assert_eq!(resolved["sandbox"], "danger-full-access");
        assert_eq!(resolved["resume_thread_id"], "thread-1");
        assert_eq!(resolved["auto_commit"], false);
        assert!(resolved.get("unknown_field").is_none());
        assert_eq!(resolved["additional_params"][0], "--verbose");
    }

    #[test]
    fn embedded_config_round_trips_without_cli_fields() {
        let value = serde_json::json!({
            "base_url": "https://api.example.test/v1",
            "context_tokens": 32_000,
            "max_input_tokens": 24_000,
            "max_output_tokens": 4_000,
            "runtime_revision": "agent-runtime-rev",
            "model": "model-from-agent",
            "credential_ref": "opaque-handle",
            "permission_policy": "scoped_proposals",
        });
        let resolved = resolve_config_value(
            ExecutorKind::Embedded,
            &value,
            &ExecutionOverrides::default(),
        )
        .expect("embedded config resolves");

        assert_eq!(resolved["base_url"], "https://api.example.test/v1");
        assert_eq!(resolved["context_tokens"], 32_000);
        assert_eq!(resolved["runtime_revision"], "agent-runtime-rev");
        assert!(resolved.get("model").is_none());
        assert!(resolved.get("credential_ref").is_none());
        assert!(resolved.get("permission_policy").is_none());
    }

    #[test]
    fn embedded_executor_is_not_admitted_to_cli_fallback_routes() {
        let error = build_ordered_fallback_routing(
            ExecutorKind::Shell,
            serde_json::json!({}),
            &[serde_json::json!({
                "executor_type": "embedded",
                "config": {}
            })],
        )
        .expect_err("embedded fallback should be rejected");
        assert!(error.to_string().contains("hosted by Forge"));

        let error =
            build_ordered_fallback_routing(ExecutorKind::Embedded, serde_json::json!({}), &[])
                .expect_err("embedded primary should not enter CLI routing");
        assert!(error.to_string().contains("hosted by Forge"));
    }

    #[test]
    fn override_merge_preserves_unset_fields() {
        let value = serde_json::json!({
            "model": "o3",
            "sandbox": "danger-full-access"
        });
        let overrides = ExecutionOverrides {
            model_id: Some("o3-mini".to_owned()),
            reasoning_effort: None,
            permission_policy: Some("supervised".to_owned()),
        };

        let resolved =
            resolve_config_value(ExecutorKind::Codex, &value, &overrides).expect("config resolves");

        assert_eq!(resolved["model"], "o3-mini");
        assert_eq!(resolved["sandbox"], "danger-full-access");
        assert_eq!(resolved["permission_policy"], "supervised");
    }

    #[test]
    fn reasoning_override_resolves_to_executor_specific_key() {
        let overrides = ExecutionOverrides {
            model_id: None,
            reasoning_effort: Some("high".to_owned()),
            permission_policy: None,
        };

        let codex = resolve_config_value(ExecutorKind::Codex, &serde_json::json!({}), &overrides)
            .expect("codex config resolves");
        assert_eq!(codex["model_reasoning_effort"], "high");
        assert!(codex.get("effort").is_none());

        let claude =
            resolve_config_value(ExecutorKind::ClaudeCode, &serde_json::json!({}), &overrides)
                .expect("claude config resolves");
        assert_eq!(claude["effort"], "high");
        assert!(claude.get("model_reasoning_effort").is_none());
    }

    #[test]
    fn shell_config_accepts_permission_policy_override() {
        let overrides = ExecutionOverrides {
            model_id: None,
            reasoning_effort: None,
            permission_policy: Some("auto".to_owned()),
        };

        let resolved =
            resolve_config_value(ExecutorKind::Shell, &serde_json::json!({}), &overrides)
                .expect("shell config resolves");

        assert_eq!(resolved["permission_policy"], "auto");
    }

    #[test]
    fn routing_normalizes_each_candidate_and_preserves_order() {
        let routing = build_ordered_fallback_routing(
            ExecutorKind::Smith,
            serde_json::json!({"profile": "acct-1"}),
            &[
                serde_json::json!({"executor_type": "smith", "config": {"profile": "acct-2", "unknown_field": true}}),
                serde_json::json!({"executor_type": "claude_code", "config": {}}),
            ],
        )
        .expect("routing builds");

        assert_eq!(routing.policy, ROUTING_POLICY_ORDERED_FALLBACK_V1);
        assert_eq!(routing.candidates.len(), 3);
        assert_eq!(routing.candidates[0].executor_type, ExecutorKind::Smith);
        assert_eq!(routing.candidates[1].config["profile"], "acct-2");
        assert!(routing.candidates[1].config.get("unknown_field").is_none());
        assert_eq!(
            routing.candidates[2].executor_type,
            ExecutorKind::ClaudeCode
        );
    }

    #[test]
    fn routing_rejects_unknown_executor_type_and_non_object_config() {
        let unknown = build_ordered_fallback_routing(
            ExecutorKind::Smith,
            serde_json::json!({}),
            &[serde_json::json!({"executor_type": "warp", "config": {}})],
        )
        .expect_err("unknown type rejects");
        assert!(unknown.to_string().contains("unknown executor type"));

        let non_object = build_ordered_fallback_routing(
            ExecutorKind::Smith,
            serde_json::json!({}),
            &[serde_json::json!({"executor_type": "smith", "config": "profile"})],
        )
        .expect_err("non-object config rejects");
        assert!(non_object.to_string().contains("must be a JSON object"));
    }

    #[test]
    fn routing_rejects_duplicate_candidates() {
        let error = build_ordered_fallback_routing(
            ExecutorKind::Smith,
            resolve_config_value(
                ExecutorKind::Smith,
                &serde_json::json!({"profile": "acct-1"}),
                &ExecutionOverrides::default(),
            )
            .expect("primary resolves"),
            &[serde_json::json!({"executor_type": "smith", "config": {"profile": "acct-1"}})],
        )
        .expect_err("duplicate rejects");
        assert!(error.to_string().contains("duplicate executor candidate"));
    }

    #[test]
    fn candidate_key_ignores_session_scoped_fields() {
        let base = resolve_config_value(
            ExecutorKind::Smith,
            &serde_json::json!({"profile": "acct-1"}),
            &ExecutionOverrides::default(),
        )
        .expect("config resolves");
        let mut with_session = base.clone();
        with_session["resume_session_id"] = serde_json::json!("session-9");

        assert_eq!(
            candidate_key(&ExecutorKind::Smith, &base),
            candidate_key(&ExecutorKind::Smith, &with_session)
        );
        assert!(candidate_key(&ExecutorKind::Smith, &base).starts_with("smith:profile=acct-1#"));
    }

    #[test]
    fn account_key_pools_by_provider_for_smith() {
        let glm_sonnet = serde_json::json!({"provider": "zai", "model": "glm-5"});
        let glm_flash = serde_json::json!({"provider": "zai", "model": "glm-5-flash"});
        let other = serde_json::json!({"provider": "google", "model": "gemini-3.6-flash"});

        assert_eq!(
            account_key(&ExecutorKind::Smith, &glm_sonnet),
            account_key(&ExecutorKind::Smith, &glm_flash)
        );
        assert_ne!(
            account_key(&ExecutorKind::Smith, &glm_sonnet),
            account_key(&ExecutorKind::Smith, &other)
        );
        assert_eq!(
            account_key(
                &ExecutorKind::ClaudeCode,
                &serde_json::json!({"model": "opus"})
            ),
            "claude_code"
        );
    }

    #[test]
    fn invalid_permission_policy_is_rejected() {
        let value = serde_json::json!({ "permission_policy": "root" });

        let error = resolve_config_value(
            ExecutorKind::ClaudeCode,
            &value,
            &ExecutionOverrides::default(),
        )
        .expect_err("invalid policy rejects");

        assert!(error
            .to_string()
            .contains("Failed to deserialize claude_code config"));
    }
}
