//! A Task's wait on the durable check runner, stated like integration's: a
//! typed witness the consuming Task step writes, no legacy column or marker.
//!
//! The witness is bound to the status entry that asked for the check. Once
//! the Task leaves that entry nothing waits for the result any more (the
//! consumer is stale), so the witness is dropped by the next write.
use super::*;
pub use api_types::{CheckWait, CheckWaitPhase};
use api_types::{ConditionOwner, ConditionRecovery};

pub(super) fn validate(condition: &TaskCondition) -> Result<()> {
    let mut witnesses = condition
        .evidence()
        .witnesses
        .iter()
        .filter_map(|w| match w {
            ConditionWitness::Check { wait, epoch } => Some((wait, *epoch)),
            _ => None,
        });
    let witness = witnesses.next();
    if witnesses.next().is_some() {
        return Err(DbError::Check("duplicate check witness".into()));
    }
    if let Some((wait, epoch)) = witness {
        if wait.consumer_id.is_empty()
            || wait.consumer_id.len() > TYPED_TEXT_LIMIT
            || wait.origin.is_empty()
            || wait.origin.len() > TYPED_TEXT_LIMIT
            || epoch < 0
        {
            return Err(DbError::Check("invalid check wait".into()));
        }
        if matches!(condition, TaskCondition::Settled { .. }) {
            return Err(DbError::Check("a settled Task waits on no check".into()));
        }
    }
    let mut reasons = condition.reasons().filter_map(|r| match r {
        ParkReason::Check { wait } => Some(wait),
        _ => None,
    });
    if let Some(wait) = reasons.next() {
        if Some(wait) != witness.map(|(wait, _)| wait) || reasons.next().is_some() {
            return Err(DbError::Check(
                "check reason differs from its witness".into(),
            ));
        }
    }
    Ok(())
}

impl TaskCondition {
    /// The check this Task's current status entry waits on, as witnessed.
    pub fn check_witness(&self) -> Option<(&CheckWait, i64)> {
        self.evidence().witnesses.iter().find_map(|w| match w {
            ConditionWitness::Check { wait, epoch } => Some((wait, *epoch)),
            _ => None,
        })
    }
    /// The check wait as a reason holding the Task (primary or secondary).
    pub fn check_wait(&self) -> Option<&CheckWait> {
        self.reasons().find_map(|r| match r {
            ParkReason::Check { wait } => Some(wait),
            _ => None,
        })
    }
}

/// Remove only the check runner's previous reason before restating it.
pub(super) fn without_reason(mut condition: TaskCondition) -> TaskCondition {
    if matches!(
        &condition,
        TaskCondition::Parked {
            primary: ParkReason::Check { .. },
            ..
        }
    ) {
        if let Some(read) = &mut condition.evidence_mut().presentation {
            read.owner = None;
            read.recovery = None;
            read.slot_blocker = false;
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
            additional.retain(|r| !matches!(r, ParkReason::Check { .. }));
            if matches!(primary, ParkReason::Check { .. }) {
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

pub(super) fn overlay(mut condition: TaskCondition, wait: &CheckWait) -> TaskCondition {
    // Nothing waits once the Task is settled, and a live execution is shown
    // as running: the witness stays on the evidence and parks again when the
    // run ends inside the same status entry.
    if matches!(
        condition,
        TaskCondition::Settled { .. } | TaskCondition::Running { .. }
    ) {
        return condition;
    }
    let check = ParkReason::Check { wait: wait.clone() };
    if let TaskCondition::Parked { additional, .. } | TaskCondition::Failed { additional, .. } =
        &mut condition
    {
        // Another owner's park stays primary; the check wait is secondary.
        additional.push(check);
        return condition;
    }
    // `Entering`, `Deferred` and `Clear` are replaced: the entry's hooks step
    // asked for this check and has nothing left to run until it answers.
    let mut evidence = std::mem::take(condition.evidence_mut());
    let since = evidence.witnesses.iter().find_map(|w| match w {
        ConditionWitness::Entry { since, .. } => Some(since.clone()),
        _ => None,
    });
    let read = evidence.presentation.get_or_insert_with(Default::default);
    let exhausted = wait.requires_intervention();
    read.owner = Some(if exhausted {
        ConditionOwner::User
    } else {
        ConditionOwner::CheckRunner
    });
    read.recovery = Some(if exhausted {
        ConditionRecovery::RetryCheck
    } else {
        ConditionRecovery::WaitForCheck
    });
    // Only the exhausted park holds its place for an owner decision.
    read.slot_blocker = exhausted;
    TaskCondition::Parked {
        primary: check,
        additional: Vec::new(),
        resume: ConditionContinuation::Reconcile,
        since,
        evidence,
    }
}
