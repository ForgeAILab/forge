//! Compose independently owned reasons and their typed presentations.
use super::*;

fn strip_legacy(e: &mut ConditionEvidence) {
    e.error_annotation = None;
    e.blocked_json = None;
    e.failed_json = None;
    e.entry_barrier_json = None;
    e.metadata.clear();
    e.unparsed_metadata = None;
    e.stated = true;
}

fn priority(reason: &ParkReason) -> u8 {
    match reason {
        ParkReason::Held { .. } => 0,
        ParkReason::BudgetExhausted { .. } => 1,
        _ => 2,
    }
}

pub(super) fn material(state: &StatedCondition) -> Option<MaterialBlocker> {
    let read = state.presentation().read();
    let source = if matches!(state, StatedCondition::Failed { .. }) {
        "failed"
    } else if read.interruption_present {
        "blocked"
    } else {
        "annotation"
    };
    let value = if read.interruption.is_some() {
        serde_json::to_value(read.interruption).ok()?
    } else {
        serde_json::to_value(read.diagnostic).ok()?
    };
    if value.is_null() {
        return None;
    }
    let mut interruption = crate::repository::event_interruption_details(source, &value);
    crate::strip_attention_delivery_metadata(&mut interruption);
    Some(MaterialBlocker {
        requires_intervention: source != "annotation"
            || read.failure_kind.is_some_and(|kind| {
                !matches!(
                    kind,
                    api_types::FailureKind::ManualStop | api_types::FailureKind::Unknown
                )
            }),
        interruption: Some(interruption),
    })
}

fn replace_diagnostic(read: &mut ConditionRead, incoming: ConditionRead) {
    read.owner = None;
    read.recovery = None;
    read.diagnostic = incoming.diagnostic;
    read.diagnostic_present = incoming.diagnostic_present;
    read.interruption = incoming.interruption;
    read.interruption_present = incoming.interruption_present;
    read.failure_message = incoming.failure_message;
    read.blocked_message = incoming.blocked_message;
    read.exception_message = incoming.exception_message;
    read.failure_kind = incoming.failure_kind;
    read.interruption_execution_id = incoming.interruption_execution_id;
    read.hard_failure = incoming.hard_failure;
    read.slot_blocker = incoming.slot_blocker;
    read.operator_reason = incoming.operator_reason;
}

fn clear_owner_read(read: &mut ConditionRead, owner: ConditionReasonOwner) {
    match owner {
        ConditionReasonOwner::Hold
        | ConditionReasonOwner::Diagnostic
        | ConditionReasonOwner::Budget { .. } => replace_diagnostic(read, ConditionRead::default()),
        ConditionReasonOwner::Entry => {
            read.entry = None;
            read.entry_recorded = false;
        }
        ConditionReasonOwner::Human => {
            read.human_wait = false;
            read.explicit_human_wait = false;
            read.external_merge = false;
        }
        ConditionReasonOwner::Environment => {
            read.environment = None;
            read.environment_recorded = false;
        }
        ConditionReasonOwner::Placement => {
            read.placement = None;
            read.placement_matches_diagnostic = false;
        }
        ConditionReasonOwner::Dispatch => read.refusal = None,
        ConditionReasonOwner::Retry | ConditionReasonOwner::PlanTransport => {
            read.retry = None;
            read.retry_recorded = false;
            read.retry_display = None;
            read.retry_kind = None;
        }
        _ => {}
    }
}

pub(super) fn apply(
    edit: &ConditionOwnerEdit,
    stored: &TaskCondition,
    epoch: Option<i64>,
) -> TaskCondition {
    let mut evidence = stored.evidence().clone();
    strip_legacy(&mut evidence);
    // Lift an existing typed primary view once. No legacy column is parsed,
    // and no opaque entry/environment/placement object enters the owner record.
    if let Some(reason) = stored.reasons().next() {
        if let Some(owner) = intent::owner_of(reason).filter(|owner| *owner != edit.owner) {
            if !evidence
                .ownership
                .iter()
                .any(|o| o.owner == owner && o.state.is_some())
            {
                let (resume, since) = match stored {
                    TaskCondition::Parked { resume, since, .. }
                    | TaskCondition::Failed { resume, since, .. } => {
                        (resume.clone(), since.clone())
                    }
                    _ => (ConditionContinuation::Reconcile, None),
                };
                let presentation = ConditionPresentation::from_read(&stored.read());
                let state = if matches!(stored, TaskCondition::Failed { .. }) {
                    StatedCondition::Failed {
                        reason: reason.clone(),
                        resume,
                        since,
                        presentation,
                    }
                } else {
                    StatedCondition::Parked {
                        reason: reason.clone(),
                        resume,
                        since,
                        presentation,
                    }
                };
                let identity = evidence
                    .ownership
                    .iter()
                    .find(|o| o.owner == owner)
                    .map(|o| o.identity.clone())
                    .or_else(|| {
                        evidence
                            .witnesses
                            .iter()
                            .find_map(|w| match w {
                                ConditionWitness::Entry {
                                    epoch,
                                    transition_id,
                                    ..
                                } => Some(ConditionIdentity::Entry {
                                    epoch: *epoch,
                                    transition_id: transition_id.clone(),
                                }),
                                _ => None,
                            })
                            .or_else(|| {
                                epoch.map(|epoch| ConditionIdentity::Entry {
                                    epoch,
                                    transition_id: None,
                                })
                            })
                    });
                if let Some(identity) = identity {
                    evidence.ownership.retain(|o| o.owner != owner);
                    evidence.ownership.push(ConditionOwnership {
                        owner,
                        identity,
                        reason: Some(reason.clone()),
                        state: Some(state),
                        material: evidence.material.clone(),
                    });
                }
            }
        }
    }
    evidence.ownership.retain(|o| o.owner != edit.owner);
    let mut reasons: Vec<_> = stored
        .reasons()
        .filter(|r| intent::owner_of(r) != Some(edit.owner))
        .cloned()
        .collect();
    clear_owner_read(
        evidence.presentation.get_or_insert_with(Default::default),
        edit.owner,
    );
    if stored
        .evidence()
        .ownership
        .iter()
        .any(|o| o.owner == edit.owner && matches!(o.state, Some(StatedCondition::Deferred { .. })))
    {
        clear_owner_read(
            evidence.presentation.get_or_insert_with(Default::default),
            ConditionReasonOwner::Retry,
        );
    }

    if let Some(replacement) = &edit.replacement {
        let state = &replacement.condition;
        evidence.ownership.push(ConditionOwnership {
            owner: edit.owner,
            identity: replacement.identity.clone(),
            reason: state.reason().cloned(),
            state: Some(state.clone()),
            material: material(state),
        });
        if let Some(reason) = state.reason() {
            reasons.insert(0, reason.clone());
        }
        let incoming = state.presentation().read();
        let read = evidence.presentation.get_or_insert_with(Default::default);
        if incoming.retry.is_some() {
            read.retry = incoming.retry;
            read.retry_recorded = true;
        }
        if incoming.refusal.is_some() {
            read.refusal = incoming.refusal;
        }
    }
    reasons.sort_by_key(priority);
    if !reasons.is_empty() {
        let primary = reasons.remove(0);
        let selected = intent::owner_of(&primary)
            .and_then(|owner| evidence.ownership.iter().find(|o| o.owner == owner))
            .cloned();
        let state = selected.as_ref().and_then(|o| o.state.as_ref());
        let (resume, since) = match state {
            Some(
                StatedCondition::Parked { resume, since, .. }
                | StatedCondition::Failed { resume, since, .. },
            ) => (resume.clone(), since.clone()),
            _ => match stored {
                TaskCondition::Parked { resume, since, .. }
                | TaskCondition::Failed { resume, since, .. } => (resume.clone(), since.clone()),
                _ => (ConditionContinuation::Reconcile, None),
            },
        };
        if let Some(state) = state {
            replace_diagnostic(
                evidence.presentation.get_or_insert_with(Default::default),
                state.presentation().read(),
            );
            evidence.material = selected.as_ref().and_then(|o| o.material.clone());
        }
        if matches!(primary, ParkReason::HumanDecision { .. }) {
            let read = evidence.presentation.get_or_insert_with(Default::default);
            read.human_wait = true;
            read.explicit_human_wait = true;
        }
        if let Some(read) = evidence.presentation.take() {
            evidence.presentation = Some(readers::bounded_read(read));
        }
        return if matches!(state, Some(StatedCondition::Failed { .. })) {
            TaskCondition::Failed {
                failure: primary,
                additional: reasons,
                resume,
                since,
                evidence,
            }
        } else {
            TaskCondition::Parked {
                primary,
                additional: reasons,
                resume,
                since,
                evidence,
            }
        };
    }
    if let Some(StatedCondition::Deferred {
        reason,
        until,
        resume,
        presentation,
    }) = evidence
        .ownership
        .iter()
        .filter_map(|o| o.state.as_ref())
        .find(|s| matches!(s, StatedCondition::Deferred { .. }))
        .cloned()
    {
        let incoming = presentation.read();
        let read = evidence.presentation.get_or_insert_with(Default::default);
        read.retry = incoming.retry;
        read.retry_recorded = read.retry.is_some();
        read.retry_display = read.retry.as_ref().map(|r| readers::ConditionRetryDisplay {
            reason: Some(r.reason.clone()),
            not_before: Some(r.not_before.clone()),
        });
        if let Some(read) = evidence.presentation.take() {
            evidence.presentation = Some(readers::bounded_read(read));
        }
        return TaskCondition::Deferred {
            reason,
            until,
            resume,
            evidence,
        };
    }
    if matches!(stored, TaskCondition::Deferred { .. })
        && edit.owner != ConditionReasonOwner::Retry
        && !stored.evidence().ownership.iter().any(|o| {
            o.owner == edit.owner && matches!(o.state, Some(StatedCondition::Deferred { .. }))
        })
        && edit.replacement.is_none()
    {
        let mut carried = stored.clone();
        *carried.evidence_mut() = evidence;
        return carried;
    }
    evidence.material = None;
    TaskCondition::Clear { evidence }
}
