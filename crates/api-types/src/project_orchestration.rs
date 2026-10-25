//! Public API/domain contracts for Charter-backed Project orchestration.
//!
//! These types deliberately contain only closed, revision-addressable data.
//! Free-form JSON is not used for canonical Project artifacts: every value
//! which can affect approval, execution, readiness, or release is represented
//! by a named field or a closed enum.  The service layer remains responsible
//! for authorization and cross-record validation; this crate owns the wire
//! shape shared by Rust and TypeScript clients.

mod charter;
mod documents;
mod milestones;
mod mutations;
mod overview;

pub use charter::*;
pub use documents::*;
pub use milestones::*;
pub use mutations::*;
pub use overview::*;

#[cfg(test)]
mod tests;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use ts_rs::TS;

use crate::ProductMaturity;

/// Schema used by the canonical JSON and digest helpers in this module.
pub const PROJECT_ORCHESTRATION_SCHEMA_VERSION: &str = "forge.project-orchestration/v1";
pub const CANONICAL_JSON_SCHEMA_VERSION: &str = "forge.canonical-json/v1";

// ---------------------------------------------------------------------------
// Shared provenance and references
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum PrincipalKind {
    User,
    Agent,
    Worker,
    Reviewer,
    Service,
    System,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct PrincipalRef {
    pub kind: PrincipalKind,
    pub id: String,
    #[serde(default)]
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationProvenance {
    pub principal: PrincipalRef,
    pub authorization_basis: String,
    pub action: String,
    pub event_id: String,
    pub occurred_at: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ProvenanceSourceKind {
    User,
    MainChat,
    ProjectChat,
    Research,
    Task,
    Validation,
    Document,
    Decision,
    Milestone,
    Release,
    System,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ProvenanceRef {
    pub source_kind: ProvenanceSourceKind,
    pub source_id: String,
    #[serde(default)]
    pub revision_id: Option<String>,
    #[serde(default)]
    pub digest: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub observed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRef {
    pub artifact_id: String,
    pub revision_id: String,
    pub content_digest: String,
    #[serde(default)]
    pub render_version: Option<String>,
    #[serde(default)]
    pub render_digest: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct VersionedDigest {
    pub schema_version: String,
    pub digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct RevisionProvenance {
    pub author: PrincipalRef,
    #[serde(default)]
    pub profile_revision: Option<String>,
    #[serde(default)]
    pub operating_skill_revision: Option<String>,
    #[serde(default)]
    pub source_refs: Vec<ProvenanceRef>,
    pub change_summary: String,
    #[serde(default)]
    pub material_diff: Option<String>,
}

// ---------------------------------------------------------------------------
// Canonical JSON / digest helpers
// ---------------------------------------------------------------------------

/// Recursively sort object keys and leave array order untouched.
///
/// `serde_json::Value` uses an implementation-defined map representation when
/// feature flags change.  Converting through a `BTreeMap` makes the ordering
/// explicit at every object depth and keeps digests independent of input map
/// insertion order.
pub fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let sorted: BTreeMap<&str, Value> = object
                .iter()
                .map(|(key, value)| (key.as_str(), canonicalize_json(value)))
                .collect();
            let mut canonical = Map::new();
            for (key, value) in sorted {
                canonical.insert(key.to_owned(), value);
            }
            Value::Object(canonical)
        }
        Value::Array(values) => Value::Array(values.iter().map(canonicalize_json).collect()),
        scalar => scalar.clone(),
    }
}

/// Serialize a value into compact, recursively key-sorted JSON.
pub fn canonical_json<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    let value = serde_json::to_value(value)?;
    serde_json::to_string(&canonicalize_json(&value))
}

/// Serialize a schema-versioned canonical envelope.  The schema participates
/// in the digest domain so changing the wire contract cannot accidentally
/// reuse an old digest.
pub fn canonical_json_with_schema<T: Serialize>(
    schema_version: &str,
    value: &T,
) -> Result<String, serde_json::Error> {
    let value = serde_json::to_value(value)?;
    let mut envelope = Map::new();
    envelope.insert(
        "schema_version".to_owned(),
        Value::String(schema_version.to_owned()),
    );
    envelope.insert("value".to_owned(), canonicalize_json(&value));
    serde_json::to_string(&canonicalize_json(&Value::Object(envelope)))
}

/// SHA-256 digest of the default schema-versioned canonical JSON, encoded as
/// lowercase hexadecimal for use in API fields and optimistic comparisons.
pub fn canonical_digest<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    canonical_digest_with_schema(CANONICAL_JSON_SCHEMA_VERSION, value)
}

/// SHA-256 digest of a schema-versioned canonical JSON envelope.
pub fn canonical_digest_with_schema<T: Serialize>(
    schema_version: &str,
    value: &T,
) -> Result<String, serde_json::Error> {
    let canonical = canonical_json_with_schema(schema_version, value)?;
    let digest = Sha256::digest(canonical.as_bytes());
    Ok(hex_lower(&digest))
}

/// Digest a rendered view with the render version in the canonical payload.
pub fn canonical_render_digest(
    render_version: &str,
    rendered_view: &str,
) -> Result<String, serde_json::Error> {
    #[derive(Serialize)]
    struct Render<'a> {
        render_version: &'a str,
        rendered_view: &'a str,
    }

    canonical_digest_with_schema(
        PROJECT_ORCHESTRATION_SCHEMA_VERSION,
        &Render {
            render_version,
            rendered_view,
        },
    )
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

// ---------------------------------------------------------------------------
// Main Chat topic boundary (design D21, live-acceptance finding F18)
// ---------------------------------------------------------------------------

/// Who started a Main Chat topic. `System` is used only for the migration
/// backfill's one initial topic per existing Main Chat; every topic a user
/// starts from the product is `User`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum AgentChatTopicPrincipalType {
    User,
    System,
}

/// One durable Main Chat topic (D21). A topic is a context epoch *inside*
/// the one account Main Chat -- it is never a second chat, binding, or
/// authority scope. `starting_message_sequence` is the sequence of the
/// visible divider message that opens it; a new Main turn's episodic context
/// is bounded to messages at or after this value in the current topic.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct AgentChatTopicResponse {
    pub id: String,
    pub chat_id: String,
    pub sequence: i64,
    pub label: String,
    pub summary: Option<String>,
    pub starting_message_id: Option<String>,
    pub starting_message_sequence: i64,
    pub principal_type: AgentChatTopicPrincipalType,
    pub principal_id: Option<String>,
    pub created_at: String,
    /// True for exactly one topic per chat: the most recently started one.
    pub is_current: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct AgentChatTopicListResponse {
    pub items: Vec<AgentChatTopicResponse>,
}

/// `label`/`summary` are optional -- an empty request still starts a topic
/// with a server-assigned default label.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct StartAgentChatTopicRequest {
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub summary: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct StartAgentChatTopicResponse {
    pub topic: Option<AgentChatTopicResponse>,
    pub rotation_pending: bool,
    /// The visible divider message appended to the chat timeline at the
    /// start of this topic.
    pub divider_message_id: Option<String>,
}
