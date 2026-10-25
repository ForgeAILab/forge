//! Diagnostic-slot replacement while its legacy annotation/interruption is dual-written.
use super::*;

fn from_diagnostic(reason: &ParkReason) -> bool {
    match reason {
        ParkReason::Held { .. } | ParkReason::Failure { .. } | ParkReason::AgentTimeout { .. } => {
            true
        }
        ParkReason::BudgetExhausted { source, .. }
        | ParkReason::UnknownCondition { source, .. }
        | ParkReason::Dependencies { source, .. }
        | ParkReason::RemoteCancelPending { source } => matches!(
            source.field,
            LegacyConditionField::ErrorAnnotation
                | LegacyConditionField::BlockedJson
                | LegacyConditionField::FailedJson
        ),
        _ => false,
    }
}

fn interruption_reason(reason: &ParkReason, read: &ConditionRead) -> bool {
    let kind = read.interruption.as_ref().and_then(|i| i.kind);
    match reason {
        ParkReason::Held { .. } => kind == Some(api_types::FailureKind::ManualStop),
        ParkReason::Failure { failure_kind } => kind == Some(*failure_kind),
        ParkReason::BudgetExhausted { source, .. }
        | ParkReason::UnknownCondition { source, .. }
        | ParkReason::Dependencies { source, .. }
        | ParkReason::RemoteCancelPending { source } => matches!(
            source.field,
            LegacyConditionField::BlockedJson | LegacyConditionField::FailedJson
        ),
        _ => false,
    }
}

pub(super) fn apply(
    stored: &TaskCondition,
    replacement: &ConditionReplacement,
    preserve_interruption: bool,
) -> Result<TaskCondition> {
    let StatedCondition::Parked {
        reason,
        resume,
        since,
        presentation,
    } = &replacement.condition
    else {
        return Err(DbError::Check(
            "diagnostic replacement must state its park".into(),
        ));
    };
    if intent::owner_of(reason) != Some(ConditionReasonOwner::Diagnostic) {
        return Err(DbError::Check(
            "diagnostic replacement must own its reason".into(),
        ));
    }
    let prior = stored.read();
    let mut read = prior.clone();
    let incoming = presentation.read();
    if !preserve_interruption {
        read.interruption = incoming.interruption.clone();
        read.interruption_present = incoming.interruption_present;
        read.interruption_execution_id = incoming.interruption_execution_id.clone();
        read.failure_message = incoming.failure_message.clone();
        read.blocked_message = incoming.blocked_message.clone();
        read.exception_message = incoming.exception_message.clone();
        read.hard_failure = incoming.hard_failure;
    }
    read.diagnostic = incoming.diagnostic;
    read.diagnostic_present = incoming.diagnostic_present;
    read.failure_kind = if read.hard_failure {
        prior.failure_kind
    } else {
        incoming.failure_kind
    };
    read.slot_blocker = incoming.slot_blocker;
    read.operator_reason = incoming.operator_reason;
    read.owner = None;
    read.recovery = None;
    read.failed_step_id = None;
    let read = readers::bounded_read(read);
    let mut reasons: Vec<_> = stored
        .reasons()
        .filter(|r| preserve_interruption && from_diagnostic(r) && interruption_reason(r, &prior))
        .cloned()
        .collect();
    if !reasons.contains(reason) {
        reasons.push(reason.clone());
    }
    reasons.extend(
        stored
            .reasons()
            .filter(|r| !from_diagnostic(r) && !matches!(r, ParkReason::Children { .. }))
            .cloned(),
    );
    reasons.sort_by_key(|r| match r {
        ParkReason::Held { .. } => 0,
        ParkReason::BudgetExhausted { .. } => 1,
        _ => 2,
    });
    let mut reasons = reasons.into_iter();
    let primary = reasons
        .next()
        .ok_or_else(|| DbError::Check("diagnostic replacement has no reason".into()))?;
    let mut evidence = stored.evidence().clone();
    evidence.error_annotation = None;
    evidence.blocked_json = None;
    evidence.failed_json = None;
    evidence.entry_barrier_json = None;
    evidence.metadata.clear();
    evidence.unparsed_metadata = None;
    evidence.stated = true;
    evidence.material =
        if preserve_interruption && (prior.interruption_present || prior.hard_failure) {
            stored.evidence().material.clone()
        } else {
            // Capture Attention input before presentation text is bounded.
            owner_projection::material(&replacement.condition)
        };
    evidence.presentation = Some(read);
    evidence.ownership.retain(|o| {
        !matches!(
            o.owner,
            ConditionReasonOwner::Hold | ConditionReasonOwner::Diagnostic
        )
    });
    evidence.ownership.push(ConditionOwnership {
        owner: ConditionReasonOwner::Diagnostic,
        identity: replacement.identity.clone(),
        reason: Some(reason.clone()),
        state: Some(replacement.condition.clone()),
        material: evidence.material.clone(),
    });
    Ok(TaskCondition::Parked {
        primary,
        additional: reasons.collect(),
        resume: match stored {
            TaskCondition::Parked { resume: prior, .. }
            | TaskCondition::Failed { resume: prior, .. }
            | TaskCondition::Deferred { resume: prior, .. } => match prior {
                ConditionContinuation::RetryEntry { .. } => read_resume(&evidence, resume),
                _ => prior.clone(),
            },
            _ => resume.clone(),
        },
        since: if preserve_interruption {
            prior
                .interruption
                .as_ref()
                .map(|i| i.created_at.clone())
                .or_else(|| since.clone())
        } else {
            since.clone()
        },
        evidence,
    })
}

fn read_resume(
    evidence: &ConditionEvidence,
    fallback: &ConditionContinuation,
) -> ConditionContinuation {
    evidence
        .presentation
        .as_ref()
        .and_then(|read| read.retry.as_ref())
        .map(|retry| ConditionContinuation::Dispatch {
            target_state: retry.target_state.clone(),
        })
        .unwrap_or_else(|| fallback.clone())
}
