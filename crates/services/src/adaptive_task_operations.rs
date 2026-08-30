//! Closed-vocabulary validation for the adaptive task-operation envelope.
//!
//! A Task's adaptive envelope grants a bounded set of restructuring verbs
//! (split/sequence/replace). This module is the single validator every entry
//! point shares, so draft authoring, governance admission, and persisted-row
//! replay all reject the same malformed authority with the same diagnostic.

use api_types::{AdaptiveEnvelope, AdaptiveTaskOperation};
use std::collections::HashSet;

/// The exact field path named in every adaptive-operation diagnostic. Kept as
/// one constant so every entry point points a caller at the identical
/// location rather than at three near-miss spellings of the same field.
pub const ADAPTIVE_ALLOWED_TASK_OPERATIONS_FIELD: &str =
    "adaptive_envelope.allowed_task_operations";

pub fn adaptive_task_operation_supported_values() -> String {
    AdaptiveTaskOperation::supported_values()
}

/// Validate an already-typed closed vocabulary. The Rust type makes an
/// unsupported verb unrepresentable, but a caller can still submit the same
/// verb twice; an envelope that grants `split` twice is malformed authority,
/// not a grant of something extra.
pub fn validate_adaptive_task_operations(
    values: &[AdaptiveTaskOperation],
) -> std::result::Result<(), String> {
    let mut seen = HashSet::new();
    for value in values {
        if !seen.insert(*value) {
            return Err(format!(
                "`{ADAPTIVE_ALLOWED_TASK_OPERATIONS_FIELD}` contains a duplicate operation '{value}'; supported: {}",
                adaptive_task_operation_supported_values()
            ));
        }
    }
    Ok(())
}

/// Parse a persisted `adaptive_envelope_json` through the one shared
/// validator used by authoring and by Task governance admission. A value
/// outside the closed split/sequence/replace vocabulary -- including a legacy
/// value from before the vocabulary was closed -- fails here naming the exact
/// field and the allowed verbs; it never silently admits an unrecognized verb.
pub fn parse_persisted_adaptive_envelope(
    value: &str,
) -> std::result::Result<AdaptiveEnvelope, String> {
    let envelope = serde_json::from_str::<AdaptiveEnvelope>(value).map_err(|error| {
        format!(
            "adaptive envelope is invalid: {error} (`{ADAPTIVE_ALLOWED_TASK_OPERATIONS_FIELD}` must be one of: {})",
            adaptive_task_operation_supported_values()
        )
    })?;
    validate_adaptive_task_operations(&envelope.allowed_task_operations)?;
    Ok(envelope)
}
