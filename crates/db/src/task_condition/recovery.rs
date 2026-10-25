//! The legacy oracle remains active until unit R removes dispatcher fallback.
use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum UnreadablePolicy {
    LegacyOracle,
    DurableOwners,
}
// Unit R flips this in the same change that removes the dispatcher legacy path.
pub(super) const UNREADABLE_POLICY: UnreadablePolicy = UnreadablePolicy::LegacyOracle;

pub(super) fn unreadable(condition: &TaskCondition) -> bool {
    condition.reasons().any(|reason| {
        matches!(reason, ParkReason::UnknownCondition {
        source: ConditionSource { field: LegacyConditionField::ConditionJson, .. },
        problem,
    } if *problem != UnknownConditionProblem::UnknownKind)
    })
}

/// Remove previously projected lifecycle facts before applying current facts.
/// Owner reasons and diagnostic references survive; expired work never does.
pub(super) fn owner_base(condition: &TaskCondition) -> TaskCondition {
    let mut evidence = condition.evidence().clone();
    evidence.error_annotation = None;
    evidence.blocked_json = None;
    evidence.failed_json = None;
    evidence.entry_barrier_json = None;
    evidence.metadata.clear();
    evidence.unparsed_metadata = None;
    evidence.observations.retain(|r| !matches!(r,
        ParkReason::UnknownCondition { source, problem: UnknownConditionProblem::UnownedEntry }
            if source.field == LegacyConditionField::MetadataJson && source.key.as_deref() == Some("coordination_review_pending")));

    let mut reasons: Vec<_> = condition
        .reasons()
        .filter(|r| {
            !matches!(r, ParkReason::Children { .. })
                && !matches!(r, ParkReason::RemoteCancelPending { source }
                    if source.field == LegacyConditionField::PendingRemoteCancel)
        })
        .cloned()
        .collect();
    match condition {
        TaskCondition::Parked { resume, since, .. }
        | TaskCondition::Failed { resume, since, .. }
            if !reasons.is_empty() =>
        {
            let primary = reasons.remove(0);
            if matches!(condition, TaskCondition::Failed { .. }) {
                TaskCondition::Failed {
                    failure: primary,
                    additional: reasons,
                    resume: resume.clone(),
                    since: since.clone(),
                    evidence,
                }
            } else {
                TaskCondition::Parked {
                    primary,
                    additional: reasons,
                    resume: resume.clone(),
                    since: since.clone(),
                    evidence,
                }
            }
        }
        TaskCondition::Deferred { .. } => {
            let mut carried = condition.clone();
            *carried.evidence_mut() = evidence;
            carried
        }
        _ => TaskCondition::Clear { evidence },
    }
}

/// A recognizable independent owner decides its own fact. A current integration
/// attempt supplies identity even when its former condition witness was lost.
pub(super) async fn load_owners(
    c: &mut SqliteConnection,
    facts: &mut ConditionFacts,
) -> Result<()> {
    if facts.terminal.is_some() {
        return Ok(());
    }
    let attempt: Option<(String, String)> = sqlx::query_as("SELECT id,state FROM integration_attempt WHERE task_id=? AND current=1 AND expected_status=? AND expected_epoch=? AND state NOT IN ('completed','cancelled','superseded') ORDER BY attempt_number DESC LIMIT 1")
        .bind(&facts.task_id).bind(&facts.state).bind(facts.epoch).fetch_optional(&mut *c).await?;
    if let Some((id, state)) = attempt {
        let attempt_id = IntegrationAttemptId::new(id);
        facts.integration = Some(match state.as_str() {
            "queued" | "awaiting_task_step" => IntegrationReason::Waiting { attempt_id },
            "applied" => IntegrationReason::Applied { attempt_id },
            "validating" | "rebasing" | "checking" | "ready_ff" | "ff_inflight" | "reconciling" => {
                IntegrationReason::Owned {
                    attempt_id,
                    phase: match state.as_str() {
                        "validating" => IntegrationPhase::Validating,
                        "rebasing" => IntegrationPhase::Rebasing,
                        "checking" => IntegrationPhase::Checking,
                        "ready_ff" | "ff_inflight" => IntegrationPhase::FastForwarding,
                        _ => IntegrationPhase::Reconciling,
                    },
                }
            }
            _ => IntegrationReason::Deferred {
                attempt_id,
                cause: IntegrationDeferralCause::OwnerRequired,
                owner_id: None,
                message:
                    "Stored Task condition is unreadable; inspect the current integration attempt"
                        .into(),
            },
        });
    }
    // Only a step of this entry can identify its outstanding check. An old
    // consumer row alone is historical and must not resurrect a wait.
    let check: Option<(String, String)> = sqlx::query_as("SELECT c.id,c.origin FROM task_step s JOIN check_consumer c ON c.id=s.awaited_consumer_id WHERE s.task_id=? AND s.expected_status=? AND s.expected_epoch=? AND s.status='suspended' AND c.status_epoch=? AND c.cancelled_at IS NULL AND c.applied_at IS NULL ORDER BY s.created_at DESC LIMIT 1")
        .bind(&facts.task_id).bind(&facts.state).bind(facts.epoch).bind(facts.epoch).fetch_optional(&mut *c).await?;
    if let Some((consumer_id, origin)) = check {
        facts.check = Some((
            CheckWait {
                consumer_id,
                origin,
                phase: CheckWaitPhase::Result,
            },
            facts.epoch,
        ));
    }
    Ok(())
}

pub(super) fn project(
    facts: &ConditionFacts,
    problem: UnknownConditionProblem,
    prior: Option<&TaskCondition>,
) -> TaskCondition {
    let mut evidence = ConditionEvidence {
        stated: true,
        ..Default::default()
    };
    if let Some(prior) = prior {
        evidence.diagnostic_ids = prior.evidence().diagnostic_ids.clone();
    }
    let condition = facts.apply(TaskCondition::Clear { evidence });
    if !matches!(condition, TaskCondition::Clear { .. }) {
        return condition;
    }
    let reason = if facts.human_wait || facts.review_wait {
        ParkReason::HumanDecision {
            boundary: HumanBoundary::Legacy,
            source: ConditionSource {
                field: LegacyConditionField::ConditionJson,
                key: Some("durable_human_owner".into()),
            },
        }
    } else {
        ParkReason::UnknownCondition {
            source: ConditionSource {
                field: LegacyConditionField::ConditionJson,
                key: None,
            },
            problem,
        }
    };
    let evidence = condition.evidence().clone();
    TaskCondition::Parked {
        primary: reason,
        additional: Vec::new(),
        resume: ConditionContinuation::Reconcile,
        since: None,
        evidence,
    }
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

/// Converted owners and unreadable parks are checked against current durable
/// facts. Unconverted writers retain their legacy oracle until their gate.
pub(super) fn expected(
    facts: &ConditionFacts,
    input: &LegacyConditionInput,
    stored: Option<&TaskCondition>,
) -> TaskCondition {
    match stored {
        Some(stored)
            if unreadable(stored) && UNREADABLE_POLICY == UnreadablePolicy::DurableOwners =>
        {
            let problem = stored
                .reasons()
                .find_map(|r| match r {
                    ParkReason::UnknownCondition { source, problem }
                        if source.field == LegacyConditionField::ConditionJson =>
                    {
                        Some(problem.clone())
                    }
                    _ => None,
                })
                .unwrap_or(UnknownConditionProblem::InvalidShape);
            project(facts, problem, Some(stored))
        }
        Some(stored) if !stored.evidence().ownership.is_empty() => {
            let mut full = stored.clone();
            collections::restore(&mut full, facts);
            facts.apply(owner_base(&full))
        }
        _ => facts.condition(input),
    }
}
