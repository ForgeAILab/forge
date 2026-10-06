//! Total, side-effect-free scheduling. Admission rechecks the same fences when
//! the selected payload reaches the existing Task queue.
use api_types::{StateKind, WorkflowDefinition};
use db::{ParkReason, TaskCondition};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Step {
    Publication,
    RefreshPlacement,
    QueuedRecovery,
    ExpireOwnerWait,
    Integrate,
    RetryReviewCi,
    ReviewRefresh {
        target: String,
    },
    SettleExecution {
        execution_id: String,
    },
    FailedReview,
    ClearPlanningWait,
    AdvanceRoot,
    ApplyDefaults,
    Initial {
        target: String,
        role: String,
        agent_id: String,
    },
    Role {
        role: String,
        agent_id: String,
    },
    MergeEntry,
}
impl Step {
    pub fn identity(&self) -> String {
        // This is an internal causation identity, never a public queue name.
        serde_json::to_string(self).expect("Step serializes")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Park {
    pub reason: Reason,
    pub owner: Owner,
    pub recovery: Action,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Reason {
    Condition(ParkReason),
    InFlight,
    QueueOwned,
    RetryDeadline,
    ReviewGrace,
    ProjectPaused,
    Dependencies,
    Children,
    HumanWork,
    AgentUnavailable,
    Capacity,
    ExecutionStopped,
    ReviewChecks,
    WorkflowInvalid { state: String, cause: String },
    UnknownCondition { owner: String },
    Settled,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Owner {
    User,
    ProjectAgent,
    Worker,
    Workflow,
    Machine,
    Scheduler,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action {
    ReleaseHold,
    RepairAndRetry,
    AssignRole,
    ApproveOrMove,
    EditWorkflow,
    ReconnectOrRemoveMachine,
    WaitForOwner,
    WaitForDeadline,
    CompleteDependencies,
    SettleChildren,
    ReconcileEntry,
    RetryChecks,
    ResumeProject,
    FreeCapacity,
    AcknowledgeCancellation,
    None,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Next {
    Step(Step),
    Park(Park),
}

/// All facts are read before calling `next_step`, in one read snapshot. Time is
/// an input; neither wall clock, database nor asynchronous work enters resolution.
#[derive(Debug, Clone, Default)]
pub struct Facts {
    pub publication: bool,
    pub refresh_placement: bool,
    pub publication_owned: bool,
    pub paused: bool,
    pub queue_owned: bool,
    pub in_flight: bool,
    pub agent_unavailable: bool,
    pub agent_full: bool,
    pub owner_expired: bool,
    pub integrate: bool,
    pub placement_unavailable: bool,
    pub environment_manual: bool,
    pub retry_pending: bool,
    pub review_ci_retry: bool,
    pub queued_recovery: bool,
    pub review_refresh: Option<String>,
    pub terminal_execution: Option<String>,
    pub failed_review: bool,
    pub review_grace: bool,
    pub blocking: bool,
    pub human_wait: bool,
    pub stale_human_wait: bool,
    pub root: bool,
    pub root_advance: bool,
    pub root_role_allowed: bool,
    pub dependencies_ready: bool,
    pub child_ready: bool,
    pub disposition_current: bool,
    pub stopped_execution: bool,
    pub reviewer_ready: bool,
    pub apply_defaults: bool,
    pub initial_target: Option<(String, String, String)>,
    pub role_target: Option<(String, String)>,
    pub missing_merge_entry: bool,
    pub unsafe_merge_entry: bool,
    pub merge_witness: bool,
}
pub struct Snapshot<'a> {
    pub state: &'a str,
    pub condition: &'a TaskCondition,
    pub workflow: &'a WorkflowDefinition,
    pub facts: &'a Facts,
}
fn park(reason: Reason, owner: Owner, recovery: Action) -> Next {
    Next::Park(Park {
        reason,
        owner,
        recovery,
    })
}
fn condition_park(condition: &TaskCondition) -> Next {
    let reason = match condition {
        TaskCondition::Parked { primary, .. } => primary,
        TaskCondition::Failed { failure, .. } => failure,
        TaskCondition::Entering { step_id, .. } => {
            return park(
                Reason::UnknownCondition {
                    owner: format!("entry hooks: {step_id}"),
                },
                Owner::Workflow,
                Action::ReconcileEntry,
            )
        }
        _ => {
            return park(
                Reason::UnknownCondition {
                    owner: "condition producer".into(),
                },
                Owner::Workflow,
                Action::RepairAndRetry,
            )
        }
    };
    let (owner, action) = match reason {
        ParkReason::Held { .. } => (Owner::User, Action::ReleaseHold),
        ParkReason::HumanDecision { .. } => (Owner::User, Action::ApproveOrMove),
        ParkReason::Capacity { .. } => (Owner::Scheduler, Action::FreeCapacity),
        ParkReason::ProjectPaused { .. } => (Owner::User, Action::ResumeProject),
        ParkReason::OwnerOffline { .. } | ParkReason::DaemonUpgradeRequired { .. } => {
            (Owner::Machine, Action::ReconnectOrRemoveMachine)
        }
        ParkReason::RemoteCancelPending { .. } => (Owner::Machine, Action::AcknowledgeCancellation),
        ParkReason::Dependencies { .. } => (Owner::ProjectAgent, Action::CompleteDependencies),
        ParkReason::Children { .. } => (Owner::ProjectAgent, Action::SettleChildren),
        ParkReason::Environment { .. } | ParkReason::PlacementDenied { .. } => {
            (Owner::Machine, Action::RepairAndRetry)
        }
        ParkReason::PlanSettlementWait { .. } => (Owner::Worker, Action::WaitForOwner),
        ParkReason::EntryBlocked { .. } | ParkReason::UnknownCondition { .. } => {
            (Owner::Workflow, Action::ReconcileEntry)
        }
        ParkReason::WorkflowInvalid { .. } => (Owner::User, Action::EditWorkflow),
        _ => (Owner::User, Action::RepairAndRetry),
    };
    park(Reason::Condition(reason.clone()), owner, action)
}

/// Every effective status, including malformed/custom definitions, resolves.
/// Priority mirrors the dispatcher: identity effects precede Project pause;
/// active recovery precedes new admission; arbitrary custom hooks never replay.
pub fn next_step(s: &Snapshot<'_>) -> Next {
    let f = s.facts;
    if f.publication {
        return if f.publication_owned {
            Next::Step(Step::Publication)
        } else {
            park(
                Reason::UnknownCondition {
                    owner: "plan publication cleanup".into(),
                },
                Owner::Workflow,
                Action::ReconcileEntry,
            )
        };
    }
    let kind = s.workflow.state_kind(s.state);
    if kind == Some(StateKind::Terminal) {
        return park(Reason::Settled, Owner::Workflow, Action::None);
    }
    if f.paused {
        return park(Reason::ProjectPaused, Owner::User, Action::ResumeProject);
    }
    if f.queue_owned {
        return park(Reason::QueueOwned, Owner::Worker, Action::WaitForOwner);
    }
    if f.refresh_placement {
        return Next::Step(Step::RefreshPlacement);
    }
    if f.environment_manual {
        return condition_park(s.condition);
    }
    if f.owner_expired {
        return Next::Step(Step::ExpireOwnerWait);
    }
    if f.placement_unavailable {
        return park(
            Reason::AgentUnavailable,
            Owner::Machine,
            Action::ReconnectOrRemoveMachine,
        );
    }
    if f.review_ci_retry {
        return if f.retry_pending {
            park(
                Reason::RetryDeadline,
                Owner::Scheduler,
                Action::WaitForDeadline,
            )
        } else {
            Next::Step(Step::RetryReviewCi)
        };
    }
    if f.queued_recovery {
        return Next::Step(Step::QueuedRecovery);
    }
    if f.integrate && !f.blocking {
        return Next::Step(Step::Integrate);
    }
    if let Some(target) = &f.review_refresh {
        return Next::Step(Step::ReviewRefresh {
            target: target.clone(),
        });
    }
    if f.failed_review {
        return Next::Step(Step::FailedReview);
    }
    if f.review_grace {
        return park(
            Reason::ReviewGrace,
            Owner::Scheduler,
            Action::WaitForDeadline,
        );
    }
    if f.blocking {
        return condition_park(s.condition);
    }
    if let Some(id) = &f.terminal_execution {
        return Next::Step(Step::SettleExecution {
            execution_id: id.clone(),
        });
    }
    if f.human_wait {
        return park(Reason::HumanWork, Owner::User, Action::ApproveOrMove);
    }
    if f.stale_human_wait {
        return Next::Step(Step::ClearPlanningWait);
    }
    if f.root_advance
        && matches!(
            kind,
            Some(StateKind::Initial | StateKind::Active | StateKind::Gate)
        )
    {
        return Next::Step(Step::AdvanceRoot);
    }
    if f.disposition_current {
        return condition_park(s.condition);
    }
    if f.retry_pending {
        return park(
            Reason::RetryDeadline,
            Owner::Scheduler,
            Action::WaitForDeadline,
        );
    }
    if f.in_flight {
        return park(Reason::InFlight, Owner::Worker, Action::WaitForOwner);
    }
    if !f.dependencies_ready {
        return park(
            Reason::Dependencies,
            Owner::ProjectAgent,
            Action::CompleteDependencies,
        );
    }
    if !f.child_ready || (f.root && !f.root_role_allowed) {
        return park(
            Reason::Children,
            Owner::ProjectAgent,
            Action::SettleChildren,
        );
    }
    if f.agent_unavailable {
        return park(
            Reason::AgentUnavailable,
            Owner::User,
            Action::RepairAndRetry,
        );
    }
    if f.agent_full {
        return park(Reason::Capacity, Owner::Scheduler, Action::FreeCapacity);
    }
    match kind {
        Some(StateKind::Initial) => {
            if f.root {
                return park(
                    Reason::Children,
                    Owner::ProjectAgent,
                    Action::SettleChildren,
                );
            }
            if f.apply_defaults {
                return Next::Step(Step::ApplyDefaults);
            }
            if let Some((target, role, agent_id)) = &f.initial_target {
                return Next::Step(Step::Initial {
                    target: target.clone(),
                    role: role.clone(),
                    agent_id: agent_id.clone(),
                });
            }
            park(Reason::HumanWork, Owner::ProjectAgent, Action::AssignRole)
        }
        Some(StateKind::Active | StateKind::Gate) => {
            if f.stopped_execution {
                return park(
                    Reason::ExecutionStopped,
                    Owner::User,
                    Action::RepairAndRetry,
                );
            }
            if !f.reviewer_ready {
                return park(Reason::ReviewChecks, Owner::Workflow, Action::RetryChecks);
            }
            if let Some((target, role, agent_id)) = &f.initial_target {
                return Next::Step(Step::Initial {
                    target: target.clone(),
                    role: role.clone(),
                    agent_id: agent_id.clone(),
                });
            }
            if let Some((role, agent_id)) = &f.role_target {
                return Next::Step(Step::Role {
                    role: role.clone(),
                    agent_id: agent_id.clone(),
                });
            }
            if f.missing_merge_entry {
                if f.unsafe_merge_entry {
                    return park(
                        Reason::WorkflowInvalid {
                            state: s.state.into(),
                            cause: "entry includes custom hooks without a safe continuation".into(),
                        },
                        Owner::ProjectAgent,
                        Action::EditWorkflow,
                    );
                }
                return if f.merge_witness {
                    Next::Step(Step::MergeEntry)
                } else {
                    park(
                        Reason::UnknownCondition {
                            owner: "entry hooks".into(),
                        },
                        Owner::Workflow,
                        Action::ReconcileEntry,
                    )
                };
            }
            park(
                Reason::WorkflowInvalid {
                    state: s.state.into(),
                    cause: "no safe automatic continuation or assigned execution role".into(),
                },
                Owner::ProjectAgent,
                Action::EditWorkflow,
            )
        }
        Some(StateKind::Backlog) => park(
            Reason::HumanWork,
            Owner::ProjectAgent,
            Action::ApproveOrMove,
        ),
        _ => park(
            Reason::WorkflowInvalid {
                state: s.state.into(),
                cause: "state has no automatic scheduling semantics".into(),
            },
            Owner::ProjectAgent,
            Action::EditWorkflow,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::engine::WorkflowEngine;
    use db::{ConditionEvidence, LegacyConditionInput};
    fn facts(bits: u32) -> Facts {
        let bit = |n: u32| (bits & (1_u32 << n)) != 0_u32;
        Facts {
            publication: bit(0),
            publication_owned: bit(1),
            paused: bit(2),
            queue_owned: bit(3),
            in_flight: bit(4),
            owner_expired: bit(5),
            placement_unavailable: bit(6),
            environment_manual: bit(7),
            retry_pending: bit(8),
            review_ci_retry: bit(9),
            queued_recovery: bit(10),
            blocking: bit(11),
            human_wait: bit(12),
            root: bit(13),
            root_advance: bit(14),
            root_role_allowed: bit(15),
            dependencies_ready: bit(16),
            child_ready: bit(17),
            reviewer_ready: bit(18),
            initial_target: Some(("in_progress".into(), "coder".into(), "agent".into())),
            role_target: Some(("coder".into(), "agent".into())),
            ..Facts::default()
        }
    }
    fn conditions() -> Vec<TaskCondition> {
        let evidence = ConditionEvidence::default();
        vec![
            TaskCondition::Clear {
                evidence: evidence.clone(),
            },
            TaskCondition::Entering {
                state: "review".into(),
                epoch: 1,
                step_id: "step".into(),
                phase: "entry".into(),
                since: "2026-10-06T00:00:00Z".into(),
                evidence: evidence.clone(),
            },
            TaskCondition::Running {
                execution_id: "execution".into(),
                role: "coder".into(),
                epoch: 1,
                since: "2026-10-06T00:00:00Z".into(),
                evidence: evidence.clone(),
            },
            TaskCondition::Deferred {
                until: Some("2026-10-06T00:00:00Z".into()),
                reason: db::RetryCause::Legacy,
                resume: db::ConditionContinuation::Reconcile,
                evidence: evidence.clone(),
            },
            db::map_legacy_condition(&LegacyConditionInput {
                blocked_json: Some("{}".into()),
                ..Default::default()
            }),
            TaskCondition::Failed {
                failure: ParkReason::Failure {
                    failure_kind: api_types::FailureKind::DispatchFailed,
                },
                additional: vec![],
                resume: db::ConditionContinuation::Reconcile,
                since: None,
                evidence: evidence.clone(),
            },
            TaskCondition::Settled {
                outcome: db::TerminalOutcome::Completed,
                evidence,
            },
        ]
    }
    #[test]
    fn every_default_and_custom_status_has_an_owned_outcome() {
        let default = WorkflowEngine::resolve_workflow("{}");
        let mut custom = default.clone();
        for state in &mut custom.states {
            if state.kind != StateKind::Terminal {
                state.name = format!("custom_{}", state.name);
            }
        }
        let mut custom_state = custom.states[0].clone();
        custom_state.name = "custom_without_continuation".into();
        custom_state.kind = StateKind::Custom;
        custom.states.push(custom_state);
        let conditions = conditions();
        for workflow in [&default, &custom] {
            for state in workflow
                .states
                .iter()
                .filter(|s| s.kind != StateKind::Terminal)
                .map(|s| s.name.as_str())
                .chain(["missing_state"])
            {
                for condition in &conditions {
                    // Enumerate all boolean combinations of the independent
                    // ownership/admission facts, including contradictory input.
                    for bits in 0..(1 << 19) {
                        let f = facts(bits);
                        match next_step(&Snapshot {
                            state,
                            condition,
                            workflow,
                            facts: &f,
                        }) {
                            Next::Step(step) => assert!(!step.identity().is_empty()),
                            Next::Park(p) => assert_ne!(p.recovery, Action::None),
                        }
                    }
                }
            }
        }
    }
    /// Frozen legacy dispatch decision (D5-D7). Repair effects are separate
    /// from an execution/transition target, as in the original dispatcher.
    fn old_dispatch(kind: Option<StateKind>, f: &Facts) -> Option<Step> {
        if f.publication
            || f.paused
            || f.queue_owned
            || f.environment_manual
            || f.owner_expired
            || f.placement_unavailable
            || f.review_ci_retry
            || f.queued_recovery
            || f.blocking
            || f.human_wait
            || f.root_advance
            || f.retry_pending
            || f.in_flight
            || !f.dependencies_ready
            || !f.child_ready
            || (f.root && !f.root_role_allowed)
        {
            return None;
        }
        match kind {
            Some(StateKind::Initial) if !f.root => {
                f.initial_target
                    .as_ref()
                    .map(|(target, role, agent_id)| Step::Initial {
                        target: target.clone(),
                        role: role.clone(),
                        agent_id: agent_id.clone(),
                    })
            }
            Some(StateKind::Active | StateKind::Gate)
                if !f.stopped_execution && f.reviewer_ready =>
            {
                f.initial_target
                    .as_ref()
                    .map(|(target, role, agent_id)| Step::Initial {
                        target: target.clone(),
                        role: role.clone(),
                        agent_id: agent_id.clone(),
                    })
                    .or_else(|| {
                        f.role_target.as_ref().map(|(role, agent_id)| Step::Role {
                            role: role.clone(),
                            agent_id: agent_id.clone(),
                        })
                    })
            }
            _ => None,
        }
    }
    #[test]
    fn enumerated_legacy_and_condition_dispatch_targets_match() {
        let workflow = WorkflowEngine::resolve_workflow("{}");
        let conditions = conditions();
        for state in &workflow.states {
            for condition in &conditions {
                for bits in 0..(1 << 19) {
                    let base = facts(bits);
                    for targets in 0..4 {
                        let mut f = base.clone();
                        if targets & 1 == 0 {
                            f.initial_target = None;
                        }
                        if targets & 2 == 0 {
                            f.role_target = None;
                        }
                        let new = match next_step(&Snapshot {
                            state: &state.name,
                            condition,
                            workflow: &workflow,
                            facts: &f,
                        }) {
                            Next::Step(s @ (Step::Initial { .. } | Step::Role { .. })) => Some(s),
                            _ => None,
                        };
                        assert_eq!(
                            old_dispatch(Some(state.kind), &f),
                            new,
                            "{} facts={bits} targets={targets}",
                            state.name
                        );
                    }
                }
            }
        }
        // Named permitted differences: ExplicitOwnerPark, MissedWakeRepair,
        // IdleTickQuiescence. None changes an old execution target.
    }
    #[test]
    fn missing_merge_owner_parks_without_replaying_custom_hooks() {
        let workflow = WorkflowEngine::resolve_workflow("{}");
        let condition = TaskCondition::Clear {
            evidence: ConditionEvidence::default(),
        };
        let f = Facts {
            missing_merge_entry: true,
            dependencies_ready: true,
            child_ready: true,
            root_role_allowed: true,
            reviewer_ready: true,
            ..Default::default()
        };
        assert!(matches!(
            next_step(&Snapshot {
                state: "merging",
                condition: &condition,
                workflow: &workflow,
                facts: &f
            }),
            Next::Park(Park {
                reason: Reason::UnknownCondition { .. },
                owner: Owner::Workflow,
                recovery: Action::ReconcileEntry
            })
        ));
    }
    #[test]
    fn custom_entry_cannot_replay_even_with_transition_witness() {
        let workflow = WorkflowEngine::resolve_workflow("{}");
        let condition = TaskCondition::Clear {
            evidence: ConditionEvidence::default(),
        };
        let facts = Facts {
            missing_merge_entry: true,
            unsafe_merge_entry: true,
            merge_witness: true,
            dependencies_ready: true,
            child_ready: true,
            root_role_allowed: true,
            reviewer_ready: true,
            ..Default::default()
        };
        assert!(matches!(
            next_step(&Snapshot {
                state: "merging",
                condition: &condition,
                workflow: &workflow,
                facts: &facts
            }),
            Next::Park(Park {
                reason: Reason::WorkflowInvalid { .. },
                recovery: Action::EditWorkflow,
                ..
            })
        ));
    }
    #[test]
    fn backlog_root_stays_parked_after_a_manual_move() {
        let workflow = WorkflowEngine::resolve_workflow("{}");
        let condition = TaskCondition::Clear {
            evidence: ConditionEvidence::default(),
        };
        let facts = Facts {
            root: true,
            root_advance: true,
            root_role_allowed: true,
            dependencies_ready: true,
            child_ready: true,
            ..Default::default()
        };
        assert!(matches!(
            next_step(&Snapshot {
                state: "backlog",
                workflow: &workflow,
                condition: &condition,
                facts: &facts
            }),
            Next::Park(Park {
                reason: Reason::HumanWork,
                recovery: Action::ApproveOrMove,
                ..
            })
        ));
    }
}
