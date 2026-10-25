//! Bound the stored projection; only durable, reloadable collections may be omitted.
use super::*;

pub const CONDITION_JSON_LIMIT: usize = 32 * 1024;

/// Fingerprint of complete authority/witness sets and counts of omitted durable sets.
/// Entry, execution, step, integration, check and owner identities remain exact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionOverflow {
    pub digest: String,
    pub byte_length: usize,
    pub witness_count: usize,
    pub reason_count: usize,
    pub collection_counts: BTreeMap<String, usize>,
}

fn strip_legacy(e: &mut ConditionEvidence) {
    e.error_annotation = None;
    e.blocked_json = None;
    e.failed_json = None;
    e.entry_barrier_json = None;
    e.metadata.clear();
    e.unparsed_metadata = None;
    e.legacy_debug_omitted = true;
}

fn shrink_reason(reason: &mut ParkReason, path: &str, counts: &mut BTreeMap<String, usize>) {
    let ids = match reason {
        ParkReason::Children { remaining, .. } => remaining,
        ParkReason::Dependencies { dependency_ids, .. } if dependency_ids.len() > 16 => {
            dependency_ids
        }
        _ => return,
    };
    if !ids.is_empty() {
        counts.insert(path.into(), ids.len());
        ids.clear();
    }
}

fn shrink_resume(
    resume: &mut ConditionContinuation,
    path: &str,
    counts: &mut BTreeMap<String, usize>,
) {
    if let ConditionContinuation::AdvanceAggregateReview { child_ids } = resume {
        if !child_ids.is_empty() {
            counts.insert(path.into(), child_ids.len());
            child_ids.clear();
        }
    }
}

fn shrink_state(state: &mut StatedCondition, path: &str, counts: &mut BTreeMap<String, usize>) {
    match state {
        StatedCondition::Parked { reason, resume, .. }
        | StatedCondition::Failed { reason, resume, .. } => {
            shrink_reason(reason, &format!("{path}.reason"), counts);
            shrink_resume(resume, &format!("{path}.resume"), counts);
        }
        StatedCondition::Deferred { resume, .. } => shrink_resume(resume, path, counts),
    }
}

fn shrink_witnesses(condition: &mut TaskCondition, counts: &mut BTreeMap<String, usize>) {
    let e = condition.evidence_mut();
    e.witnesses.retain(|w| {
        let family = match w {
            ConditionWitness::Child { .. } => "children",
            ConditionWitness::Operation { .. } => "operations",
            ConditionWitness::Budget { .. } => "budgets",
            _ => return true,
        };
        *counts.entry(format!("witnesses.{family}")).or_default() += 1;
        false
    });
}

fn shrink_collections(condition: &mut TaskCondition, counts: &mut BTreeMap<String, usize>) {
    match condition {
        TaskCondition::Parked {
            primary: reason,
            additional,
            resume,
            ..
        }
        | TaskCondition::Failed {
            failure: reason,
            additional,
            resume,
            ..
        } => {
            shrink_reason(reason, "primary", counts);
            for (i, reason) in additional.iter_mut().enumerate() {
                shrink_reason(reason, &format!("additional.{i}"), counts);
            }
            shrink_resume(resume, "resume", counts);
        }
        TaskCondition::Deferred { resume, .. } => shrink_resume(resume, "resume", counts),
        _ => {}
    }
    let e = condition.evidence_mut();
    for (i, reason) in e.observations.iter_mut().enumerate() {
        shrink_reason(reason, &format!("observations.{i}"), counts);
    }
    for (i, owner) in e.ownership.iter_mut().enumerate() {
        if let Some(reason) = &mut owner.reason {
            shrink_reason(reason, &format!("ownership.{i}.reason"), counts);
        }
        if let Some(state) = &mut owner.state {
            shrink_state(state, &format!("ownership.{i}.state"), counts);
        }
    }
}

fn cut(text: &mut String, limit: usize) {
    if text.len() > limit {
        *text = diagnostics::truncate(text, limit);
    }
}
fn cut_optional(text: &mut Option<String>, limit: usize) {
    if let Some(text) = text {
        cut(text, limit);
    }
}
fn presentation(condition: &mut TaskCondition, limit: usize) {
    let evidence = condition.evidence_mut();
    // Tighten free text only under whole-record pressure. The operator JSON,
    // material, enum tags, dates and authority identities remain exact.
    if limit < PRESENTATION_TEXT_LIMIT {
        if let Some(read) = &mut evidence.presentation {
            cut_optional(&mut read.failure_message, limit);
            cut_optional(&mut read.blocked_message, limit);
            cut_optional(&mut read.exception_message, limit);
            if let Some(d) = &mut read.diagnostic {
                cut(&mut d.blocking_reason, limit);
                cut_optional(&mut d.message, limit);
            }
            if let Some(i) = &mut read.interruption {
                cut(&mut i.reason, limit);
            }
            if let Some(r) = &mut read.retry {
                cut(&mut r.reason, limit);
            }
            if let Some(r) = &mut read.retry_display {
                cut_optional(&mut r.reason, limit);
            }
            if let Some(r) = &mut read.refusal {
                cut(&mut r.safe_message, limit);
            }
        }
    }
    for owner in &mut evidence.ownership {
        if let Some(state) = &mut owner.state {
            let p = match state {
                StatedCondition::Parked { presentation, .. }
                | StatedCondition::Failed { presentation, .. }
                | StatedCondition::Deferred { presentation, .. } => presentation,
            };
            if let Some(d) = &mut p.diagnostic {
                cut(&mut d.blocking_reason, limit);
                cut_optional(&mut d.message, limit);
            }
            if let Some(i) = &mut p.interruption {
                cut(&mut i.reason, limit);
            }
            cut_optional(&mut p.failure_message, limit);
            if let Some(r) = &mut p.retry {
                cut(&mut r.reason, limit);
            }
            if let Some(r) = &mut p.refusal {
                cut(&mut r.safe_message, limit);
            }
        }
    }
}

pub(super) fn json_error(error: serde_json::Error) -> DbError {
    DbError::Check(format!("Task condition encoding: {error}"))
}

pub(super) fn try_bounded_condition(condition: &TaskCondition) -> Result<TaskCondition> {
    let mut result = condition.clone();
    // Legacy import and typed statements have already bounded their display fields.
    // Do not bound a second time: Operations' JSON document has a separate limit.
    // Fact projections already bounded their source text before adding recovery
    // guidance. Keep that base ordering; direct claims without facts still bound here.
    if !result
        .evidence()
        .witnesses
        .iter()
        .any(|w| matches!(w, ConditionWitness::Entry { .. }))
    {
        if let Some(read) = result.evidence_mut().presentation.take() {
            result.evidence_mut().presentation = Some(readers::bounded_read(read));
        }
    }
    presentation(&mut result, PRESENTATION_TEXT_LIMIT);
    if serde_json::to_vec(&result).map_err(json_error)?.len() <= CONDITION_JSON_LIMIT {
        return Ok(result);
    }
    strip_legacy(result.evidence_mut());
    if serde_json::to_vec(&result).map_err(json_error)?.len() <= CONDITION_JSON_LIMIT {
        return Ok(result);
    }
    // The fingerprint describes authority and complete witness sets, independent
    // of display text. Attention material is untouched in the stored result.
    let mut complete = serde_json::to_value(result.typed()).map_err(json_error)?;
    complete["evidence"] = serde_json::json!({ "witnesses": [] });
    complete["evidence"]["witnesses"] =
        serde_json::to_value(&result.evidence().witnesses).map_err(json_error)?;
    let original = serde_json::to_vec(&complete).map_err(json_error)?;
    let mut summary = ConditionOverflow {
        digest: diagnostics::digest(&original),
        byte_length: original.len(),
        witness_count: result.evidence().witnesses.len(),
        reason_count: result.reasons().count(),
        collection_counts: BTreeMap::new(),
    };
    // Keep public identity lists intact when omitting redundant durable witnesses is enough.
    shrink_witnesses(&mut result, &mut summary.collection_counts);
    result.evidence_mut().overflow = Some(summary.clone());
    if serde_json::to_vec(&result).map_err(json_error)?.len() <= CONDITION_JSON_LIMIT {
        return Ok(result);
    }
    shrink_collections(&mut result, &mut summary.collection_counts);
    result.evidence_mut().overflow = Some(summary);
    for limit in [PRESENTATION_TEXT_LIMIT, 256, 64, 32] {
        presentation(&mut result, limit);
        if serde_json::to_vec(&result).map_err(json_error)?.len() <= CONDITION_JSON_LIMIT {
            if result
                .evidence()
                .overflow
                .as_ref()
                .is_some_and(|s| s.collection_counts.is_empty())
            {
                result.evidence_mut().overflow = None;
            }
            return Ok(result);
        }
    }
    // Non-reloadable authority is never discarded or replaced by a hash.
    Err(DbError::Check(
        "Task condition authority exceeds 32 KiB".into(),
    ))
}

pub(super) fn bounded_condition(condition: &TaskCondition) -> TaskCondition {
    // Comparison-only callers cannot make an unrepresentable claim agree.
    try_bounded_condition(condition).unwrap_or_else(|_| condition.clone())
}

pub(super) async fn prepare(
    _c: &mut SqliteConnection,
    _task_id: &str,
    condition: &TaskCondition,
) -> Result<TaskCondition> {
    try_bounded_condition(condition)
}
