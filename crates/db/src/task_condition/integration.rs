//! Integration's durable statement witness, independent of legacy columns.
use super::*;
use api_types::{ConditionOwner, ConditionRecovery, FailureKind, InterruptionMetadata};
use serde_json::json;

pub(super) fn validate(condition: &TaskCondition) -> Result<()> {
    let mut witnesses = condition
        .evidence()
        .witnesses
        .iter()
        .filter_map(|w| match w {
            ConditionWitness::Integration {
                reason,
                handoff_ready,
            } => Some((reason, *handoff_ready)),
            _ => None,
        });
    let witness = witnesses.next();
    if witnesses.next().is_some() {
        return Err(DbError::Check("duplicate integration witness".into()));
    }
    if let Some((reason, handoff_ready)) = witness {
        if handoff_ready && !reason.hands_off() {
            return Err(DbError::Check(
                "integration reason cannot hand off to a role".into(),
            ));
        }
        let valid_id = |id: &IntegrationAttemptId| {
            !id.as_str().is_empty() && id.as_str().len() <= TYPED_TEXT_LIMIT
        };
        if !valid_id(reason.attempt_id()) {
            return Err(DbError::Check(
                "invalid integration attempt identity".into(),
            ));
        }
        let text_valid = match reason {
            IntegrationReason::ReviewRequired {
                authority_reason, ..
            } => authority_reason.len() <= TYPED_TEXT_LIMIT,
            IntegrationReason::CandidateCheckFailed { check, message, .. } => {
                check.len() <= TYPED_TEXT_LIMIT && message.len() <= TYPED_TEXT_LIMIT
            }
            IntegrationReason::Deferred {
                message, owner_id, ..
            } => {
                message.len() <= TYPED_TEXT_LIMIT
                    && owner_id
                        .as_ref()
                        .is_none_or(|id| id.len() <= TYPED_TEXT_LIMIT)
            }
            _ => true,
        };
        if !text_valid {
            return Err(DbError::Check(
                "integration statement text exceeds its bound".into(),
            ));
        }
        match reason {
            IntegrationReason::Waiting { blocked_by, .. }
                if blocked_by
                    .iter()
                    .any(|id| !valid_id(id) || id == reason.attempt_id()) =>
            {
                return Err(DbError::Check("invalid integration wait lineage".into()));
            }
            IntegrationReason::Repair {
                conflict_paths,
                repair_paths,
                predecessor_attempt_id,
                ..
            } => {
                let paths: Vec<&String> = conflict_paths
                    .iter()
                    .flatten()
                    .chain(repair_paths)
                    .collect();
                if predecessor_attempt_id
                    .as_ref()
                    .is_some_and(|id| !valid_id(id) || id == reason.attempt_id())
                    || paths.len() > 4096
                    || paths.iter().map(|p| p.len()).sum::<usize>() > 64 * 1024
                    || paths.iter().any(|p| {
                        p.is_empty()
                            || p.len() > 4096
                            || p.starts_with('/')
                            || p.contains('\0')
                            || p.split('/')
                                .any(|part| part == ".." || part == "." || part.is_empty())
                    })
                {
                    return Err(DbError::Check(
                        "invalid integration repair paths or lineage".into(),
                    ));
                }
            }
            IntegrationReason::Deferred {
                retry_at: Some(at), ..
            } if chrono::DateTime::parse_from_rfc3339(at).is_err() => {
                return Err(DbError::Check("invalid integration retry deadline".into()));
            }
            _ => {}
        }
    }
    let mut reasons = condition.reasons().filter_map(|r| match r {
        ParkReason::Integration { reason } => Some(reason),
        _ => None,
    });
    if let Some(reason) = reasons.next() {
        if Some(reason) != witness.map(|(reason, _)| reason) || reasons.next().is_some() {
            return Err(DbError::Check(
                "integration reason differs from its witness".into(),
            ));
        }
    }
    Ok(())
}

impl TaskCondition {
    /// Survives lifecycle settlement and coder/reviewer handoff as a witness.
    pub fn integration_reason(&self) -> Option<&IntegrationReason> {
        self.evidence().witnesses.iter().find_map(|w| match w {
            ConditionWitness::Integration { reason, .. } => Some(reason),
            _ => None,
        })
    }
    pub fn integration_handoff_ready(&self) -> bool {
        self.evidence().witnesses.iter().any(|w| {
            matches!(
                w,
                ConditionWitness::Integration {
                    handoff_ready: true,
                    ..
                }
            )
        })
    }
    /// An actual scheduling wait; handoff lineage on a live run is not a wait.
    pub fn integration_wait(&self) -> Option<&IntegrationReason> {
        self.reasons().find_map(|r| match r {
            ParkReason::Integration { reason } => Some(reason),
            _ => None,
        })
    }
}

/// Remove only integration's previous reason before restating its witness.
pub(super) fn without_reason(mut condition: TaskCondition) -> TaskCondition {
    if matches!(
        &condition,
        TaskCondition::Parked {
            primary: ParkReason::Integration { .. },
            ..
        }
    ) {
        let blocker = condition
            .integration_wait()
            .is_some_and(IntegrationReason::requires_intervention);
        let evidence = condition.evidence_mut();
        if let Some(read) = &mut evidence.presentation {
            read.owner = None;
            read.recovery = None;
            read.slot_blocker = false;
            if blocker {
                read.failure_kind = None;
                read.diagnostic_present = false;
                read.diagnostic = None;
                read.interruption_present = false;
                read.interruption = None;
                read.blocked_message = None;
                read.exception_message = None;
                read.human_wait = read.explicit_human_wait;
                evidence.material = None;
            }
        }
    }
    match &mut condition {
        TaskCondition::Parked {
            primary,
            additional,
            ..
        }
        | TaskCondition::Failed {
            failure: primary,
            additional,
            ..
        } => {
            additional.retain(|r| !matches!(r, ParkReason::Integration { .. }));
            if matches!(primary, ParkReason::Integration { .. }) {
                if additional.is_empty() {
                    return TaskCondition::Clear {
                        evidence: std::mem::take(condition.evidence_mut()),
                    };
                }
                *primary = additional.remove(0);
            }
        }
        _ => {}
    }
    condition
}

pub(super) fn overlay(
    mut condition: TaskCondition,
    reason: &IntegrationReason,
    handoff_ready: bool,
) -> TaskCondition {
    // Real lifecycle ownership wins after repair/review handoff. The witness
    // stays on the evidence, including after terminal settlement.
    if handoff_ready || matches!(condition, TaskCondition::Settled { .. }) {
        return condition;
    }
    let integration = ParkReason::Integration {
        reason: reason.clone(),
    };
    if let TaskCondition::Parked { additional, .. } | TaskCondition::Failed { additional, .. } =
        &mut condition
    {
        // Another owner's park remains primary; integration is secondary.
        additional.push(integration);
        return condition;
    }
    let mut evidence = std::mem::take(condition.evidence_mut());
    let entry_since = evidence
        .witnesses
        .iter()
        .find_map(|w| match w {
            ConditionWitness::Entry { since, .. } => Some(since.clone()),
            _ => None,
        })
        .unwrap_or_default();
    let read = evidence.presentation.get_or_insert_with(Default::default);
    read.owner = Some(ConditionOwner::IntegrationWorker);
    read.recovery = Some(if reason.requires_intervention() {
        ConditionRecovery::RepairIntegration
    } else {
        ConditionRecovery::WaitForIntegration
    });
    read.slot_blocker = matches!(
        reason,
        IntegrationReason::Deferred {
            cause: IntegrationDeferralCause::OwnerOffline,
            ..
        }
    ) || reason.requires_intervention();
    if let IntegrationReason::Deferred {
        cause,
        owner_id,
        message,
        ..
    } = reason
    {
        if cause.requires_intervention() {
            let kind = match cause {
                IntegrationDeferralCause::TargetDirty => FailureKind::TargetRepoDirty,
                IntegrationDeferralCause::BudgetExhausted => FailureKind::RetryExhausted,
                _ => FailureKind::ReviewNeedsOwner,
            };
            read.failure_kind = Some(kind);
            read.diagnostic_present = true;
            read.interruption_present = true;
            read.blocked_message = Some(message.clone());
            read.exception_message = Some(message.clone());
            read.diagnostic = Some(api_types::TaskBlockingAnnotation {
                annotation_type: kind,
                blocking_reason: message.clone(),
                blocked_by: owner_id.clone(),
                blocked_at: None,
                blocked_execution_id: None,
                artifact: None,
                message: Some(message.clone()),
                hook: None,
            });
            read.interruption = Some(InterruptionMetadata {
                kind: Some(kind),
                reason: message.clone(),
                created_at: entry_since,
                source: Some("integration".into()),
                execution_id: None,
                details: None,
            });
            // Only owner-fixable cause, owner and message are material. Attempt
            // replacements and retry deadlines do not create fresh incidents.
            evidence.material = Some(MaterialBlocker {
                requires_intervention: true,
                interruption: Some(
                    json!({"source":"integration","kind":kind,"cause":cause,"owner_id":owner_id,"reason":message}),
                ),
            });
        }
    }
    TaskCondition::Parked {
        primary: integration,
        additional: Vec::new(),
        resume: ConditionContinuation::Integration {
            attempt_id: reason.attempt_id().clone(),
        },
        since: None,
        evidence,
    }
}
