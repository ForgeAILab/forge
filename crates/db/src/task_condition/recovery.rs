//! Preserve unreadable condition bytes in the diagnostic text store.
use super::*;

#[cfg(test)]
pub(super) fn unreadable(condition: &TaskCondition) -> bool {
    condition.reasons().any(|reason| {
        matches!(reason, ParkReason::UnknownCondition {
        source: ConditionSource { field: LegacyConditionField::ConditionJson, .. },
        problem,
    } if *problem != UnknownConditionProblem::UnknownKind)
    })
}

pub(super) async fn record_raw(
    c: &mut SqliteConnection,
    task_id: &str,
    raw: &[u8],
) -> Result<String> {
    use base64::Engine as _;
    let text = match std::str::from_utf8(raw) {
        Ok(text) => text.to_owned(),
        Err(_) => format!(
            "base64:{}",
            base64::engine::general_purpose::STANDARD.encode(raw)
        ),
    };
    diagnostics::record(
        c,
        task_id,
        RecordTaskDiagnosticText {
            kind: "unreadable_condition",
            source: "condition_repair",
            owner: &DiagnosticOwner::StoredCondition,
            text: &text,
        },
    )
    .await
}
