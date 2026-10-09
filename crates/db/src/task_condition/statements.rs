//! Typed statements: a writer says what its write means for the Task and the
//! condition is edited from that, never re-read from the fields it wrote.
//!
//! A statement owns the reasons its writer owns. Every other reason on the
//! Task (an entry block, a placement denial, a human gate, a Project pause)
//! belongs to another writer and is carried from the stored condition as it
//! stands. No legacy column or metadata key is read here, and none is copied
//! into the result.
use super::readers::{bound_operator_reason, bounded_read};
use super::*;
use api_types::{FailureKind, InterruptionMetadata, TaskBlockingAnnotation};

/// What a Task writer states about the condition its write causes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "statement", rename_all = "snake_case")]
pub enum ConditionStatement {
    /// Replace only integration's reason, preserving every other owner.
    /// Fenced on the attempt: the owning attempt restates freely; another
    /// attempt is refused unless none owns the row (never stated, cleared,
    /// or handed off to a role).
    Integration { reason: IntegrationReason },
    /// The Task step has sent repair/review back to its configured role.
    /// Retain lineage while allowing ordinary role admission and recovery.
    IntegrationHandedOff { attempt_id: IntegrationAttemptId },
    /// Clear only the named attempt; a stale consumer cannot clear a successor.
    IntegrationCleared { attempt_id: IntegrationAttemptId },
    /// The owner holds a Task that no execution is running for. The hold
    /// replaces the Task's diagnostic, interruption and failure, and drops
    /// its queued command, retry timer, dispatch refusal, environment wait
    /// and owner wait.
    Hold {
        actor: String,
        reason: String,
        at: String,
    },
    /// The owner releases the Task: its diagnostic, interruption and failure
    /// are cleared. Whatever else holds the Task still does.
    Release,
}

impl ConditionStatement {
    /// The hold as the operator reads it, and as the legacy annotation has
    /// always stored it: one rendering of the typed fields, used for both.
    pub fn hold_operator_text(actor: &str, reason: &str, at: &str) -> String {
        serde_json::json!({
            "type": "manual_stop",
            "blocking_reason": reason,
            "blocked_by": actor,
            "blocked_at": at,
            "blocked_execution_id": null,
        })
        .to_string()
    }
    /// The diagnostic a hold shows.
    pub fn hold_diagnostic(actor: &str, reason: &str, at: &str) -> TaskBlockingAnnotation {
        TaskBlockingAnnotation {
            annotation_type: FailureKind::ManualStop,
            blocking_reason: reason.to_owned(),
            blocked_by: Some(actor.to_owned()),
            blocked_at: Some(at.to_owned()),
            blocked_execution_id: None,
            artifact: None,
            message: None,
            hook: None,
        }
    }
    /// The interruption a hold records.
    pub fn hold_interruption(reason: &str, at: &str) -> InterruptionMetadata {
        InterruptionMetadata {
            reason: reason.to_owned(),
            created_at: at.to_owned(),
            kind: Some(FailureKind::ManualStop),
            source: None,
            execution_id: None,
            details: None,
        }
    }

    /// Whether another writer's reason survives this statement.
    fn carries(&self, reason: &ParkReason) -> bool {
        use LegacyConditionField as Field;
        match reason {
            ParkReason::Integration { .. } => true,
            ParkReason::EntryBlocked { .. }
            | ParkReason::PlacementDenied { .. }
            | ParkReason::DaemonUpgradeRequired { .. }
            | ParkReason::ProjectPaused { .. }
            | ParkReason::PlanSettlementWait { .. } => true,
            ParkReason::BudgetExhausted { source, .. } => source.field == Field::EntryBarrierJson,
            ParkReason::HumanDecision { source, .. } => source.field == Field::MetadataJson,
            ParkReason::UnknownCondition { source, .. } => {
                matches!(source.field, Field::EntryBarrierJson | Field::MetadataJson)
            }
            // Waits a hold drops and a release leaves alone.
            ParkReason::OwnerOffline { .. }
            | ParkReason::Environment { .. }
            | ParkReason::Capacity { .. }
            | ParkReason::DispatchRefusal { .. } => matches!(self, Self::Release),
            // The diagnostic, interruption and failure both statements
            // replace, and the reasons the durable facts restate on their own.
            ParkReason::Held { .. }
            | ParkReason::Failure { .. }
            | ParkReason::AgentTimeout { .. }
            | ParkReason::RemoteCancelPending { .. }
            | ParkReason::Dependencies { .. }
            | ParkReason::Children { .. }
            | ParkReason::Parent { .. }
            | ParkReason::Agent { .. }
            | ParkReason::WorkflowInvalid { .. } => false,
        }
    }
    fn carries_observation(&self, observation: &ParkReason) -> bool {
        use LegacyConditionField as Field;
        let ParkReason::UnknownCondition { source, problem } = observation else {
            return false;
        };
        match (&source.field, source.key.as_deref()) {
            (Field::EntryBarrierJson, _) => true,
            // Restated from the durable child facts on every write.
            (Field::MetadataJson, Some("coordination_review_pending")) => {
                *problem != UnknownConditionProblem::UnownedEntry
            }
            (
                Field::MetadataJson,
                Some(
                    "deferred_dispatch"
                    | "dispatch_disposition"
                    | "environment_wait"
                    | "owner_wait",
                ),
            ) => matches!(self, Self::Release),
            (Field::MetadataJson, _) => true,
            _ => false,
        }
    }

    /// The condition after this statement, before the durable facts (entry,
    /// hooks, execution, children, cancellations) are laid over it.
    pub(super) fn apply(&self, stored: &TaskCondition) -> TaskCondition {
        if matches!(
            self,
            Self::Integration { .. }
                | Self::IntegrationHandedOff { .. }
                | Self::IntegrationCleared { .. }
        ) {
            let mut stated = stored.typed();
            stated.evidence_mut().stated = true;
            return stated;
        }
        use LegacyConditionField as Field;
        let prior = stored.evidence();
        let mut read = prior.presentation.clone().unwrap_or_default();
        let observations: Vec<ParkReason> = prior
            .observations
            .iter()
            .filter(|observation| self.carries_observation(observation))
            .cloned()
            .collect();
        // Whether the entry record is read as a park here (it is not from an
        // initial state, where it is an observation only).
        let entry_parks = !observations.iter().any(|observation| {
            matches!(observation, ParkReason::UnknownCondition { source, .. } if source.field == Field::EntryBarrierJson)
        });
        let mut reasons: Vec<ParkReason> = stored
            .reasons()
            .filter(|reason| self.carries(reason))
            .cloned()
            .collect();
        let (stored_resume, stored_cause) = match stored {
            TaskCondition::Deferred { resume, reason, .. } => (Some(resume), Some(reason)),
            TaskCondition::Parked { resume, .. } | TaskCondition::Failed { resume, .. } => {
                (Some(resume), None)
            }
            _ => (None, None),
        };
        // A blocked entry that a CI-infrastructure retry timer was standing
        // in for parks again once the diagnostic naming that retry is gone.
        if stored_cause == Some(&RetryCause::ReviewCiInfrastructure) && entry_parks {
            let entry = read.entry.as_ref();
            let field = |key: &str| entry.and_then(|e| e.get(key)).and_then(|v| v.as_str());
            if field("status") == Some("blocked")
                && field("blocking_reason") != Some("review retry budget exhausted")
            {
                reasons.insert(
                    0,
                    ParkReason::EntryBlocked {
                        state: field("state").map(str::to_owned),
                        source: source(&Field::EntryBarrierJson, None),
                    },
                );
            }
        }

        // Owned by the durable facts, restated when they are laid over.
        read.owner = None;
        read.recovery = None;
        read.failure_message = None;
        read.interruption_execution_id = None;
        read.hard_failure = false;
        read.failed_step_id = None;

        let material;
        match self {
            Self::Integration { .. }
            | Self::IntegrationHandedOff { .. }
            | Self::IntegrationCleared { .. } => unreachable!(),
            Self::Hold { actor, reason, at } => {
                // The retry timer goes, so a plan settlement it was standing
                // in for is a wait of its own.
                if let Some(ConditionContinuation::SettlePlan { execution_id }) = stored_resume {
                    if !reasons
                        .iter()
                        .any(|r| matches!(r, ParkReason::PlanSettlementWait { .. }))
                    {
                        reasons.push(ParkReason::PlanSettlementWait {
                            execution_id: execution_id.clone(),
                        });
                    }
                }
                let diagnostic = Self::hold_diagnostic(actor, reason, at);
                let interruption = Self::hold_interruption(reason, at);
                let operator = bound_operator_reason(&Self::hold_operator_text(actor, reason, at));
                let mut details = crate::repository::event_interruption_details(
                    "blocked",
                    &serde_json::to_value(&interruption).expect("interruption serializes"),
                );
                crate::strip_attention_delivery_metadata(&mut details);
                material = Some(MaterialBlocker {
                    requires_intervention: true,
                    interruption: Some(details),
                });
                read.diagnostic = Some(diagnostic);
                read.diagnostic_present = true;
                read.interruption = Some(interruption);
                read.interruption_present = true;
                read.blocked_message = Some(reason.clone());
                read.exception_message = Some(reason.clone());
                read.failure_kind = Some(FailureKind::ManualStop);
                read.slot_blocker = true;
                read.placement_matches_diagnostic = false;
                read.queued_command = false;
                read.retry_recorded = false;
                read.retry = None;
                read.retry_display = None;
                read.retry_kind = None;
                read.refusal = None;
                read.environment = None;
                read.environment_recorded = false;
                let mut read = bounded_read(read);
                read.operator_reason = Some(operator);
                let resume = reasons
                    .iter()
                    .find_map(|r| match r {
                        ParkReason::ProjectPaused { state } => {
                            Some(ConditionContinuation::Integrate {
                                state: state.clone(),
                            })
                        }
                        _ => None,
                    })
                    .or_else(|| {
                        reasons.iter().find_map(|r| match r {
                            ParkReason::PlanSettlementWait { execution_id } => {
                                Some(ConditionContinuation::SettlePlan {
                                    execution_id: execution_id.clone(),
                                })
                            }
                            _ => None,
                        })
                    })
                    .unwrap_or(ConditionContinuation::Reconcile);
                TaskCondition::Parked {
                    primary: ParkReason::Held {
                        actor: actor.clone(),
                    },
                    additional: reasons,
                    resume,
                    since: Some(at.clone()),
                    evidence: ConditionEvidence {
                        observations,
                        presentation: Some(read),
                        material,
                        stated: true,
                        ..Default::default()
                    },
                }
            }
            Self::Release => {
                read.diagnostic = None;
                read.diagnostic_present = false;
                read.interruption = None;
                read.interruption_present = false;
                read.blocked_message = None;
                read.exception_message = None;
                read.failure_kind = None;
                read.slot_blocker = false;
                read.operator_reason = None;
                // A placement refusal names the diagnostic it wrote; with
                // none stored, only a refusal that names none still matches.
                read.placement_matches_diagnostic = read
                    .placement
                    .as_ref()
                    .is_some_and(|placement| placement["annotation"].is_null());
                let retry = read.retry.clone().filter(|retry| {
                    chrono::DateTime::parse_from_rfc3339(&retry.not_before).is_ok()
                });
                let plan_wait = reasons
                    .iter()
                    .find_map(|r| match r {
                        ParkReason::PlanSettlementWait { execution_id } => {
                            Some(execution_id.clone())
                        }
                        _ => None,
                    })
                    .or_else(|| match stored_resume {
                        Some(ConditionContinuation::SettlePlan { execution_id }) => {
                            Some(execution_id.clone())
                        }
                        _ => None,
                    });
                let resume = if read.queued_command {
                    match stored_resume {
                        Some(queued @ ConditionContinuation::ResumeQueuedCommand { .. }) => {
                            queued.clone()
                        }
                        _ => ConditionContinuation::ResumeQueuedCommand { intent_id: None },
                    }
                } else if let Some(state) = reasons.iter().find_map(|r| match r {
                    ParkReason::ProjectPaused { state } => Some(state.clone()),
                    _ => None,
                }) {
                    ConditionContinuation::Integrate { state }
                } else if let Some(execution_id) = plan_wait.clone() {
                    ConditionContinuation::SettlePlan { execution_id }
                } else if let Some(retry) = &retry {
                    ConditionContinuation::Dispatch {
                        target_state: retry.target_state.clone(),
                    }
                } else {
                    ConditionContinuation::Reconcile
                };
                let since = (entry_parks)
                    .then(|| {
                        read.entry
                            .as_ref()
                            .and_then(|entry| entry.get("started_at"))
                            .and_then(|v| v.as_str())
                            .map(str::to_owned)
                    })
                    .flatten()
                    .or_else(|| {
                        reasons.iter().find_map(|r| match r {
                            ParkReason::OwnerOffline { started_at, .. } => started_at.clone(),
                            _ => None,
                        })
                    });
                let environment_retry = read.retry_display.is_some()
                    && read
                        .retry_kind
                        .as_deref()
                        .is_some_and(|kind| kind.starts_with("environment_"));
                let evidence = ConditionEvidence {
                    observations,
                    presentation: Some(read),
                    material: None,
                    stated: true,
                    ..Default::default()
                };
                let mut reasons = reasons.into_iter();
                match reasons.next() {
                    Some(primary) => TaskCondition::Parked {
                        primary,
                        additional: reasons.collect(),
                        resume,
                        since,
                        evidence,
                    },
                    None => match retry {
                        Some(retry) => TaskCondition::Deferred {
                            until: Some(retry.not_before),
                            reason: if plan_wait.is_some() {
                                RetryCause::PlanTransport
                            } else {
                                match stored_cause {
                                    Some(
                                        cause @ (RetryCause::ExecutionFailure
                                        | RetryCause::WorkflowGuard
                                        | RetryCause::PlanTransport),
                                    ) => cause.clone(),
                                    _ => RetryCause::Legacy,
                                }
                            },
                            resume,
                            evidence,
                        },
                        None if environment_retry => TaskCondition::Deferred {
                            until: None,
                            reason: RetryCause::Environment,
                            resume: ConditionContinuation::Reconcile,
                            evidence,
                        },
                        None => TaskCondition::Clear { evidence },
                    },
                }
            }
        }
    }
}
