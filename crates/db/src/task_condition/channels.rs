//! Strict storage values for new condition writes. Existing rows continue through
//! the lenient mapper; these decoders are never installed on a read path.
use api_types::FailureKind;
use serde::{Deserialize, Deserializer, Serialize};

mod annotation;
mod metadata;
pub use annotation::*;
pub use metadata::*;

/// Alphabetical fields reproduce json!-built interruption bytes. The queue's
/// workflow-loop writer omits kind and created_at, using blocked_at instead.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Interruption {
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub blocked_at: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub created_at: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub details: Option<Option<InterruptionDetails>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub execution_id: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "strict_kind",
        skip_serializing_if = "Option::is_none"
    )]
    pub kind: Option<Option<InterruptionKind>>,
    #[serde(deserialize_with = "nullable")]
    pub reason: Option<String>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub source: Option<Option<String>>,
}

// Outer None is absent; Some(None) retains an explicit JSON null.
fn present<'de, D, T>(decoder: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(decoder).map(Some)
}

fn strict_kind<'de, D: Deserializer<'de>>(
    decoder: D,
) -> Result<Option<Option<InterruptionKind>>, D::Error> {
    let kind = present(decoder)?;
    if kind == Some(Some(InterruptionKind::Failure(FailureKind::Unknown))) {
        return Err(serde::de::Error::custom("unknown interruption kind"));
    }
    Ok(kind)
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InterruptionDetails {
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    // Reader/legacy-only: no production setting writer on the baseline.
    pub assignment_guidance: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    // Reader/legacy-only: no production setting writer on the baseline.
    pub assignment_required: Option<Option<bool>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub attempts: Option<Option<Vec<ExecutorRouteAttempt>>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub cancelled_dependency_ids: Option<Option<Vec<String>>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub retry_at: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub superseded: Option<Option<SupersededValue>>,
}

/// Dependency re-blocking may carry a caller's existing superseded object.
/// Fresh snapshots use the closed shape; carried objects retain copied JSON
/// without construction limits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SupersededValue {
    Condition(SupersededCondition),
    Carried(CopiedConditionObject),
}

/// Saved channel bytes are opaque text: historical values must survive restore.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupersededCondition {
    #[serde(deserialize_with = "nullable")]
    pub blocked_json: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub error_annotation: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub failed_json: Option<String>,
    pub status: String,
}

/// Both local route settlement and remote terminal notifications write these
/// two string fields. Remote outcomes are caller-supplied strings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutorRouteAttempt {
    pub candidate_key: String,
    pub outcome: String,
}

fn optional<'de, D, T>(decoder: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(decoder).map(Some)
}

/// Integration admission uses its own stored kind, outside FailureKind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum InterruptionKind {
    Admission(IntegrationAdmissionKind),
    Failure(#[serde(deserialize_with = "failure_kind")] FailureKind),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntegrationAdmissionKind {
    #[serde(rename = "integration_admission_failed")]
    Failed,
}
impl From<FailureKind> for InterruptionKind {
    fn from(kind: FailureKind) -> Self {
        Self::Failure(kind)
    }
}

fn nullable<'de, D, T>(decoder: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::deserialize(decoder)
}

fn failure_kind<'de, D: Deserializer<'de>>(decoder: D) -> Result<FailureKind, D::Error> {
    let kind = FailureKind::deserialize(decoder)?;
    if kind == FailureKind::Unknown {
        return Err(serde::de::Error::custom("unknown failure kind"));
    }
    Ok(kind)
}

#[cfg(test)]
mod tests;
