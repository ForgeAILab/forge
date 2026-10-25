//! Rebuild omitted identity lists from the durable rows loaded with a bounded claim.
use super::*;

pub(super) fn restore(condition: &mut TaskCondition, facts: &ConditionFacts) {
    let Some(summary) = condition.evidence().overflow.clone() else {
        return;
    };
    let Some(dependencies) = &facts.collection_dependencies else {
        return;
    };
    fn reason(
        reason: &mut ParkReason,
        path: &str,
        counts: &BTreeMap<String, usize>,
        facts: &ConditionFacts,
        dependencies: &[(String, String)],
    ) {
        if !counts.contains_key(path) {
            return;
        }
        match reason {
            ParkReason::Children { root_id, remaining } if *root_id == facts.task_id => {
                *remaining = facts
                    .children
                    .iter()
                    .filter(|(_, _, settled)| !settled)
                    .map(|(id, _, _)| id.clone())
                    .collect();
            }
            ParkReason::Dependencies {
                cancelled,
                dependency_ids,
                ..
            } => {
                *dependency_ids = dependencies
                    .iter()
                    .filter(|(_, state)| {
                        if *cancelled {
                            state == "cancelled"
                        } else {
                            state != "done"
                        }
                    })
                    .map(|(id, _)| id.clone())
                    .collect();
            }
            _ => {}
        }
    }
    fn resume(
        resume: &mut ConditionContinuation,
        path: &str,
        counts: &BTreeMap<String, usize>,
        facts: &ConditionFacts,
    ) {
        if counts.contains_key(path) {
            if let ConditionContinuation::AdvanceAggregateReview { child_ids } = resume {
                *child_ids = facts.children.iter().map(|(id, _, _)| id.clone()).collect();
            }
        }
    }
    match condition {
        TaskCondition::Parked {
            primary,
            additional,
            resume: r,
            ..
        }
        | TaskCondition::Failed {
            failure: primary,
            additional,
            resume: r,
            ..
        } => {
            reason(
                primary,
                "primary",
                &summary.collection_counts,
                facts,
                dependencies,
            );
            for (i, item) in additional.iter_mut().enumerate() {
                reason(
                    item,
                    &format!("additional.{i}"),
                    &summary.collection_counts,
                    facts,
                    dependencies,
                );
            }
            resume(r, "resume", &summary.collection_counts, facts);
        }
        TaskCondition::Deferred { resume: r, .. } => {
            resume(r, "resume", &summary.collection_counts, facts)
        }
        _ => {}
    }
    let e = condition.evidence_mut();
    for (i, item) in e.observations.iter_mut().enumerate() {
        reason(
            item,
            &format!("observations.{i}"),
            &summary.collection_counts,
            facts,
            dependencies,
        );
    }
    for (i, owner) in e.ownership.iter_mut().enumerate() {
        if let Some(item) = &mut owner.reason {
            reason(
                item,
                &format!("ownership.{i}.reason"),
                &summary.collection_counts,
                facts,
                dependencies,
            );
        }
        match &mut owner.state {
            Some(
                StatedCondition::Parked {
                    reason: item,
                    resume: r,
                    ..
                }
                | StatedCondition::Failed {
                    reason: item,
                    resume: r,
                    ..
                },
            ) => {
                reason(
                    item,
                    &format!("ownership.{i}.state.reason"),
                    &summary.collection_counts,
                    facts,
                    dependencies,
                );
                resume(
                    r,
                    &format!("ownership.{i}.state.resume"),
                    &summary.collection_counts,
                    facts,
                );
            }
            Some(StatedCondition::Deferred { resume: r, .. }) => resume(
                r,
                &format!("ownership.{i}.state"),
                &summary.collection_counts,
                facts,
            ),
            None => {}
        }
    }
    // Witnesses are replaced by apply_lifecycle from these same complete facts.
    e.overflow = None;
}
