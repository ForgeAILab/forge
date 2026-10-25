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

/// A legacy-backed writer edits complete reader inputs, not pressure-shortened text.
pub(super) fn restore_writer_read(
    condition: &mut TaskCondition,
    facts: &ConditionFacts,
    input: &LegacyConditionInput,
) {
    let mapped = (condition.evidence().overflow.is_some()
        && condition.evidence().legacy_debug_omitted
        && condition.evidence().ownership.is_empty())
    .then(|| facts.condition(input));
    restore(condition, facts);
    if let Some(mapped) = mapped {
        let read = mapped.read();
        condition.evidence_mut().public_diagnostic =
            if read.diagnostic.is_none() && read.diagnostic_present {
                mapped.public().details().diagnostic.clone()
            } else {
                None
            };
        condition.evidence_mut().presentation = Some(read);
    }
}

/// Public reads restore reloadable sets and pressure-shortened presentation in
/// the same database snapshot. Nothing is written and diagnostic text is not read.
pub(crate) async fn restore_read(
    c: &mut SqliteConnection,
    task_id: &str,
    condition: &mut TaskCondition,
) -> Result<()> {
    if condition.evidence().overflow.is_none() {
        return Ok(());
    }
    let snapshot = facts::Snapshot::read(c, task_id, facts::Families::ENTRY)
        .await?
        .ok_or(DbError::NotFound)?;
    let mut facts = ConditionFacts::load_all(c, &snapshot).await?;
    facts.load_collection_dependencies(c).await?;
    let mapped = facts.condition(&snapshot.input);
    restore(condition, &facts);
    condition.evidence_mut().witnesses = mapped.evidence().witnesses.clone();
    if condition.evidence().legacy_debug_omitted {
        // All writers still dual-write these inputs under LegacyOracle.
        let read = mapped.read();
        condition.evidence_mut().public_diagnostic =
            if read.diagnostic.is_none() && read.diagnostic_present {
                mapped.public().details().diagnostic.clone()
            } else {
                None
            };
        condition.evidence_mut().presentation = Some(read);
    }
    Ok(())
}

impl SqliteDb {
    /// Restore the public condition of a committed writer reply without
    /// replacing the Task's status, version or other captured fields.
    pub async fn restore_task_read(&self, task: &mut Task) -> Result<()> {
        if task.condition.evidence().overflow.is_some() {
            let mut c = self.pool().begin().await?;
            restore_read(&mut c, &task.id, &mut task.condition).await?;
            c.rollback().await?;
        }
        Ok(())
    }
}
