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
    /// Admit a Task that is in a merge state, and that nothing owns there,
    /// to the integration queue (`TaskService::reconcile_integration_admission`).
    AdmitIntegration,
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
    /// Send a coordination root with a lead back to its planning state: its
    /// current subtask `child_id` parked needing a decision. `park`
    /// identifies that park; one re-plan is made for it.
    ReplanRoot {
        child_id: String,
        park: String,
    },
    ApplyDefaults,
    /// Leave an optional gate that nobody holds for the terminal state it
    /// accepts into (a step check whose root has no lead any more).
    SkipGate {
        target: String,
    },
    Initial {
        target: String,
        role: String,
        agent_id: String,
    },
    Role {
        role: String,
        agent_id: String,
    },
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
    Children,
    /// A subtask whose parent does not let it run. Visible: the stored
    /// condition names the parent and the cause.
    ParentWait {
        parent_id: String,
        cause: String,
    },
    HumanWork,
    AgentUnavailable,
    /// The Agent that would run the Task is paused or cannot be reached.
    /// Visible: the stored condition names the Agent and its status.
    AgentWait {
        agent_id: String,
        status: String,
    },
    Capacity,
    /// A started Task whose Agent is at its run limit. Visible: the stored
    /// condition is the `capacity` reason with the `agent` scope. A Task
    /// still waiting for its first slot keeps the unstored `Capacity`.
    AgentCapacity {
        agent_id: String,
    },
    ExecutionStopped,
    ReviewChecks,
    WorkflowInvalid {
        state: String,
        cause: String,
    },
    UnknownCondition {
        owner: String,
    },
    Settled,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Owner {
    IntegrationWorker,
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
    /// The Agent that cannot take the Task now (unavailable, or at its run
    /// limit) and its effective status.
    pub agent_wait: Option<(String, String)>,
    /// An accepted action waits behind an unfinished dependency.
    pub dependency_wait: bool,
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
    /// A coordination root with a lead whose current subtask parked needing
    /// a decision, and that has not re-planned for that park: the subtask
    /// and the identity of its park
    /// (`RootRolePolicy::replan_due`).
    pub root_replan: Option<(String, String)>,
    pub child_ready: bool,
    /// The parent that does not let this subtask run, and why.
    pub parent_wait: Option<(String, String)>,
    pub disposition_current: bool,
    pub stopped_execution: bool,
    pub reviewer_ready: bool,
    pub apply_defaults: bool,
    pub initial_target: Option<(String, String, String)>,
    /// The terminal state an optional gate nobody holds is skipped to. Set
    /// only where `initial_target` would otherwise be asked, never with it.
    pub gate_skip: Option<String>,
    pub role_target: Option<(String, String)>,
    /// The state's role is assigned to a person.
    pub role_user: bool,
    /// The state has a role and nobody holds it.
    pub role_open: bool,
    /// A root Task in a state whose entry admits to the integration queue,
    /// with no integration attempt anything owns for its present entry (none,
    /// an ended one, or an imported row that waited for a block its owner
    /// has since lifted), and that neither its owner nor a pending step
    /// stops: the reconciler admits it.
    pub missing_merge_entry: bool,
}
pub struct Snapshot<'a> {
    pub state: &'a str,
    pub condition: &'a TaskCondition,
    pub workflow: &'a WorkflowDefinition,
    pub facts: &'a Facts,
}
/// The two missing owners a park can name for a Task nothing else owns.
pub const PUBLICATION_OWNER: &str = "plan publication cleanup";
pub const ENTRY_HOOKS_OWNER: &str = "entry hooks";

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
        ParkReason::Integration { .. } => (Owner::IntegrationWorker, Action::WaitForOwner),
        // The check runner answers a result or slot wait through the Task's
        // own delivery step; only exhausted retries wait for the owner.
        ParkReason::Check { wait } if wait.requires_intervention() => {
            (Owner::User, Action::RetryChecks)
        }
        ParkReason::Check { .. } => (Owner::Worker, Action::WaitForOwner),
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
        ParkReason::Parent { cause, .. } if cause == "held" => (Owner::User, Action::ReleaseHold),
        // The root's lead is planning; its success releases the subtask.
        ParkReason::Parent { cause, .. } if cause == "planning" => {
            (Owner::Worker, Action::WaitForOwner)
        }
        ParkReason::Parent { .. } => (Owner::ProjectAgent, Action::SettleChildren),
        ParkReason::Agent { .. } => (Owner::User, Action::RepairAndRetry),
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

/// Whether an integration wait decides this Task's next step.
///
/// It does when integration is the primary reason, or when the primary is a
/// real owner blocker (a hold, a blocked entry, a failure, a human decision):
/// nothing the scheduler does clears those, and the integration wait stays
/// behind them. It does not under a self-clearing primary: a capacity wait,
/// a dispatch refusal, an offline owner, an environment wait, a placement
/// refresh or an owner-wait expiry. Those keep their normal step, which is
/// what clears them, with integration retained as a secondary reason. So a
/// capacity park is only ever the capacity reason's own, never one an
/// integration wait raised.
pub fn integration_decides(condition: &TaskCondition, f: &Facts) -> bool {
    owner_wait_decides(condition, f, condition.integration_wait().is_some())
}

/// Whether a wait on the durable check runner decides this Task's next step,
/// by the same rule as an integration wait: it does as the primary reason or
/// behind a real owner blocker, and never under a self-clearing primary. A
/// result or slot wait is therefore never a dispatch, an entry-hooks replay,
/// a capacity-unpark demand or a human blocker; the delivery step ends it.
pub fn check_decides(condition: &TaskCondition, f: &Facts) -> bool {
    owner_wait_decides(condition, f, condition.check_wait().is_some())
}

fn owner_wait_decides(condition: &TaskCondition, f: &Facts, waits: bool) -> bool {
    if !waits {
        return false;
    }
    let primary = match condition {
        TaskCondition::Parked { primary, .. } => primary,
        TaskCondition::Failed { .. } => return true,
        _ => return false,
    };
    match primary {
        ParkReason::Integration { .. } | ParkReason::Check { .. } => true,
        ParkReason::Capacity { .. }
        | ParkReason::DispatchRefusal { .. }
        | ParkReason::OwnerOffline { .. }
        | ParkReason::Environment { .. } => false,
        _ => !(f.refresh_placement || f.owner_expired),
    }
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
                    owner: PUBLICATION_OWNER.into(),
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
    // Integration owns this wait, or waits behind a real owner blocker: the
    // park names the primary's owner. It is never an execution, entry-hooks
    // lease or paused-integration retry. A self-clearing primary falls
    // through to its normal step (see `integration_decides`).
    // A merge state whose Task has no attempt anything owns is the
    // reconciler's (below, behind every self-clearing wait): a reason a
    // superseded or imported attempt left on the Task does not decide it.
    if !f.missing_merge_entry
        && (integration_decides(s.condition, f) || check_decides(s.condition, f))
    {
        return condition_park(s.condition);
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
        // Behind an unfinished dependency the accepted action is not
        // replayed (and refused) again: the dependency's completion, or the
        // removal of its link, wakes the Task.
        if f.dependency_wait {
            return condition_park(s.condition);
        }
        return Next::Step(Step::QueuedRecovery);
    }
    if f.integrate && !f.blocking {
        return Next::Step(Step::Integrate);
    }
    // Level-triggered admission. It replaces the entry replay
    // (`Step::MergeEntry`) the queue cutover removed: whatever left the Task
    // in its merge state without an attempt, the sweep admits it through
    // the function the `run_merge` hook calls. An accepted owner command
    // (above) runs first; a Task its owner stopped is not admitted
    // (`missing_merge_entry` is false under the Task gate).
    if f.missing_merge_entry {
        return Next::Step(Step::AdmitIntegration);
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
        // A refused advance is recorded on the root and holds like any
        // other recorded refusal.
        return if f.disposition_current {
            condition_park(s.condition)
        } else {
            Next::Step(Step::AdvanceRoot)
        };
    }
    // Level-triggered re-plan, beside the root advance: the root implements
    // (`Active`), so the two never hold together. A paused Project, an
    // integration wait, a held or blocked root and a human wait all resolved
    // above. A refused re-plan is recorded and holds like any refusal.
    if let (Some((child_id, park)), Some(StateKind::Active)) = (&f.root_replan, kind) {
        return if f.disposition_current {
            condition_park(s.condition)
        } else {
            Next::Step(Step::ReplanRoot {
                child_id: child_id.clone(),
                park: park.clone(),
            })
        };
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
    // An unfinished or cancelled dependency is not a park of its own: the
    // admission attempt meets the dependency gate, which writes the visible
    // refusal (and blocks on a cancelled dependency) exactly as before.
    if let Some((parent_id, cause)) = &f.parent_wait {
        let (owner, recovery) = match cause.as_str() {
            "held" => (Owner::User, Action::ReleaseHold),
            // The root's lead is planning; its success releases the subtask.
            "planning" => (Owner::Worker, Action::WaitForOwner),
            _ => (Owner::ProjectAgent, Action::SettleChildren),
        };
        return park(
            Reason::ParentWait {
                parent_id: parent_id.clone(),
                cause: cause.clone(),
            },
            owner,
            recovery,
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
            match &f.agent_wait {
                Some((agent_id, status)) => Reason::AgentWait {
                    agent_id: agent_id.clone(),
                    status: status.clone(),
                },
                None => Reason::AgentUnavailable,
            },
            Owner::User,
            Action::RepairAndRetry,
        );
    }
    if f.agent_full {
        // A Task that already started and has no run must show why: its
        // Agent is busy with other Tasks. It is dispatched when a run ends.
        if let (Some((agent_id, _)), false) = (&f.agent_wait, kind == Some(StateKind::Initial)) {
            return park(
                Reason::AgentCapacity {
                    agent_id: agent_id.clone(),
                },
                Owner::Scheduler,
                Action::FreeCapacity,
            );
        }
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
            // Before the stopped-run park: a run that nobody holds the role
            // for any more is not waited on.
            if let Some(target) = &f.gate_skip {
                return Next::Step(Step::SkipGate {
                    target: target.clone(),
                });
            }
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
            // Nothing automatic continues from here, and that is not a
            // defect: a person holds the role, nobody was given it yet, or the
            // state is worked by hand. The base left these alone too.
            if f.role_open && !f.role_user {
                return park(Reason::HumanWork, Owner::ProjectAgent, Action::AssignRole);
            }
            park(Reason::HumanWork, Owner::User, Action::ApproveOrMove)
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
            stopped_execution: bit(16),
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
    /// The gate-skip fact is not one of the enumerated bits: it is set only
    /// for an optional gate nobody holds, in place of `initial_target`.
    #[test]
    fn an_optional_gate_nobody_holds_is_skipped_unless_something_owns_the_task() {
        let workflow = WorkflowEngine::resolve_subtask_workflow();
        let condition = TaskCondition::Clear {
            evidence: ConditionEvidence::default(),
        };
        let skip = Facts {
            gate_skip: Some("done".into()),
            child_ready: true,
            root_role_allowed: true,
            reviewer_ready: true,
            role_open: true,
            stopped_execution: true,
            ..Default::default()
        };
        let resolve = |facts: &Facts| {
            next_step(&Snapshot {
                state: "lead_check",
                condition: &condition,
                workflow: &workflow,
                facts,
            })
        };
        assert_eq!(
            resolve(&skip),
            Next::Step(Step::SkipGate {
                target: "done".into()
            })
        );
        // A paused Project, a running check, a hold on the root and a
        // blocked subtask each come first.
        for held in [
            Facts {
                paused: true,
                ..skip.clone()
            },
            Facts {
                in_flight: true,
                ..skip.clone()
            },
            Facts {
                blocking: true,
                ..skip.clone()
            },
            Facts {
                parent_wait: Some(("root".into(), "planning".into())),
                ..skip.clone()
            },
        ] {
            assert!(matches!(resolve(&held), Next::Park(_)), "{held:?}");
        }
    }

    #[test]
    fn handed_off_integration_lineage_allows_idle_role_dispatch() {
        let workflow = WorkflowEngine::resolve_workflow("{}");
        for condition in crate::task_actions::tests::integration_conditions()
            .into_iter()
            .filter(|condition| condition.integration_reason().unwrap().hands_off())
        {
            let reason = condition.integration_reason().unwrap().clone();
            let state = if matches!(reason, api_types::IntegrationReason::ReviewRequired { .. }) {
                "review"
            } else {
                "merge_failed"
            };
            let condition = db::ConditionFacts {
                task_id: "task".into(),
                state: state.into(),
                integration: Some(reason),
                integration_handoff_ready: true,
                ..Default::default()
            }
            .condition(&db::LegacyConditionInput::default());
            let f = Facts {
                role_target: Some(("assigned_role".into(), "agent".into())),
                child_ready: true,
                root_role_allowed: true,
                reviewer_ready: true,
                ..Default::default()
            };
            assert!(matches!(
                next_step(&Snapshot {
                    state,
                    condition: &condition,
                    workflow: &workflow,
                    facts: &f
                }),
                Next::Step(Step::Role { .. })
            ));
            assert!(matches!(condition, TaskCondition::Clear { .. }));
        }
    }

    #[test]
    fn check_waits_park_on_their_owner_and_never_dispatch_or_demand_capacity() {
        let workflow = WorkflowEngine::resolve_workflow("{}");
        for state in workflow
            .states
            .iter()
            .filter(|state| state.kind != StateKind::Terminal)
        {
            for condition in crate::task_actions::tests::check_conditions() {
                let exhausted = condition.check_wait().unwrap().requires_intervention();
                // Everything that would otherwise pick a step, replay entry
                // hooks or raise a capacity park is set: the check wait wins.
                let f = Facts {
                    integrate: true,
                    queued_recovery: true,
                    review_ci_retry: true,
                    role_target: Some(("coder".into(), "agent".into())),
                    refresh_placement: true,
                    owner_expired: true,
                    agent_full: true,
                    disposition_current: true,
                    ..Default::default()
                };
                assert!(check_decides(&condition, &f));
                let next = next_step(&Snapshot {
                    state: &state.name,
                    condition: &condition,
                    workflow: &workflow,
                    facts: &f,
                });
                let Next::Park(park) = next else {
                    panic!("{}: a check wait is never a step: {next:?}", state.name);
                };
                assert!(matches!(
                    park.reason,
                    Reason::Condition(ParkReason::Check { .. })
                ));
                assert_eq!(
                    (park.owner, park.recovery),
                    if exhausted {
                        (Owner::User, Action::RetryChecks)
                    } else {
                        (Owner::Worker, Action::WaitForOwner)
                    },
                    "{}",
                    state.name
                );
            }
        }
        // Behind a self-clearing primary the wait is retained and that
        // primary keeps its own step, as for integration.
        for mut condition in crate::task_actions::tests::check_conditions() {
            let TaskCondition::Parked {
                primary,
                additional,
                ..
            } = &mut condition
            else {
                unreachable!()
            };
            let check = std::mem::replace(
                primary,
                ParkReason::Capacity {
                    scope: db::ConditionCapacityScope::Machine,
                },
            );
            additional.push(check);
            assert!(condition.check_wait().is_some());
            assert!(!check_decides(&condition, &Facts::default()));
        }
        // A Task with no check wait is unaffected.
        assert!(!check_decides(&TaskCondition::default(), &Facts::default()));
    }

    #[test]
    fn integration_waits_have_an_integration_owner_in_every_custom_state() {
        let default = WorkflowEngine::resolve_workflow("{}");
        // A workflow that differs in structure, not in spelling: one more
        // working state ahead of the merge, and a merge state by another name.
        let mut custom = default.clone();
        let merge = custom
            .states
            .iter()
            .position(|state| state.name == "merging")
            .expect("the default workflow merges in `merging`");
        custom.states[merge].name = "ship".into();
        let mut extra = custom
            .states
            .iter()
            .find(|state| state.kind == StateKind::Active)
            .expect("an active state")
            .clone();
        extra.name = "qa".into();
        extra.display_name = "QA".into();
        custom.states.insert(merge, extra);
        assert_eq!(custom.states.len(), default.states.len() + 1);
        assert!(custom.state_kind("merging").is_none());
        assert_eq!(custom.state_kind("qa"), Some(StateKind::Active));
        for workflow in [&default, &custom] {
            for state in workflow
                .states
                .iter()
                .filter(|state| state.kind != StateKind::Terminal)
            {
                for condition in crate::task_actions::tests::integration_conditions() {
                    // Everything that would otherwise pick a step or a
                    // capacity park is set: integration-primary ignores it.
                    let f = Facts {
                        integrate: true,
                        queued_recovery: true,
                        role_target: Some(("coder".into(), "agent".into())),
                        refresh_placement: true,
                        owner_expired: true,
                        agent_full: true,
                        disposition_current: true,
                        ..Default::default()
                    };
                    assert!(matches!(
                        next_step(&Snapshot {
                            state: &state.name,
                            condition: &condition,
                            workflow,
                            facts: &f
                        }),
                        Next::Park(Park {
                            owner: Owner::IntegrationWorker,
                            recovery: Action::WaitForOwner,
                            ..
                        })
                    ));
                    let mut held = condition.clone();
                    if let TaskCondition::Parked {
                        primary,
                        additional,
                        ..
                    } = &mut held
                    {
                        additional.insert(0, primary.clone());
                        *primary = ParkReason::Held {
                            actor: "user".into(),
                        };
                    }
                    // A real owner blocker is not cleared by a placement
                    // refresh or an owner-wait expiry of its own.
                    let f = Facts {
                        refresh_placement: false,
                        owner_expired: false,
                        ..f
                    };
                    assert!(matches!(
                        next_step(&Snapshot {
                            state: &state.name,
                            condition: &held,
                            workflow,
                            facts: &f
                        }),
                        Next::Park(Park {
                            owner: Owner::User,
                            recovery: Action::ReleaseHold,
                            ..
                        })
                    ));
                }
            }
        }
    }

    /// M2: integration retained as a secondary reason does not freeze a
    /// self-clearing primary. The Task resolves exactly as it would without
    /// the integration reason, so the step that clears the primary runs, and
    /// a capacity park is only ever the capacity reason's own.
    #[test]
    fn integration_behind_a_self_clearing_primary_keeps_the_primarys_step() {
        use db::{ConditionCapacityScope, ConditionEnvironmentKind, ConditionSource};
        let workflow = WorkflowEngine::resolve_workflow("{}");
        let source = || ConditionSource {
            field: db::LegacyConditionField::MetadataJson,
            key: None,
        };
        let self_clearing = [
            ParkReason::Capacity {
                scope: ConditionCapacityScope::Agent,
            },
            ParkReason::DispatchRefusal {
                capability: None,
                blocker_digest: None,
            },
            ParkReason::OwnerOffline {
                daemon_id: Some("daemon".into()),
                started_at: None,
            },
            ParkReason::Environment {
                wait_kind: ConditionEnvironmentKind::EnvironmentNotReady,
                source: source(),
            },
        ];
        let facts = [
            Facts::default(),
            Facts {
                refresh_placement: true,
                ..Default::default()
            },
            Facts {
                owner_expired: true,
                ..Default::default()
            },
            Facts {
                disposition_current: true,
                ..Default::default()
            },
            Facts {
                placement_unavailable: true,
                ..Default::default()
            },
            Facts {
                agent_full: true,
                child_ready: true,
                ..Default::default()
            },
            Facts {
                role_target: Some(("coder".into(), "agent".into())),
                child_ready: true,
                reviewer_ready: true,
                ..Default::default()
            },
        ];
        let secondary = |condition: &TaskCondition, primary: &ParkReason, keep: bool| {
            let mut condition = condition.clone();
            let TaskCondition::Parked {
                primary: integration,
                additional,
                ..
            } = &mut condition
            else {
                panic!("an integration wait parks");
            };
            let integration = std::mem::replace(integration, primary.clone());
            if keep {
                additional.push(integration);
            }
            condition
        };
        let mut steps = 0;
        for integration in crate::task_actions::tests::integration_conditions() {
            for primary in &self_clearing {
                let with = secondary(&integration, primary, true);
                let without = secondary(&integration, primary, false);
                assert!(with.integration_wait().is_some(), "integration is retained");
                for f in &facts {
                    assert!(!integration_decides(&with, f));
                    let resolve = |condition: &TaskCondition| {
                        next_step(&Snapshot {
                            state: "in_progress",
                            condition,
                            workflow: &workflow,
                            facts: f,
                        })
                    };
                    let next = resolve(&with);
                    assert_eq!(next, resolve(&without), "{primary:?} {f:?}");
                    assert!(
                        !matches!(
                            &next,
                            Next::Park(Park {
                                owner: Owner::IntegrationWorker,
                                ..
                            })
                        ),
                        "{primary:?}: the primary's owner, not integration's"
                    );
                    steps += usize::from(matches!(next, Next::Step(_)));
                    // A capacity park here is the capacity reason's own: the
                    // same Task with integration as its primary raises none.
                    assert!(!matches!(
                        next_step(&Snapshot {
                            state: "in_progress",
                            condition: &integration,
                            workflow: &workflow,
                            facts: f,
                        }),
                        Next::Park(Park {
                            recovery: Action::FreeCapacity,
                            ..
                        }) | Next::Step(_)
                    ));
                }
            }
            // Real owner blockers keep integration waiting behind them, and
            // name their own owner.
            for blocker in [
                ParkReason::EntryBlocked {
                    state: Some("in_progress".into()),
                    source: source(),
                },
                ParkReason::Failure {
                    failure_kind: api_types::FailureKind::ManualStop,
                },
                ParkReason::HumanDecision {
                    boundary: db::HumanBoundary::Legacy,
                    source: source(),
                },
            ] {
                let with = secondary(&integration, &blocker, true);
                let f = Facts {
                    integrate: true,
                    queued_recovery: true,
                    role_target: Some(("coder".into(), "agent".into())),
                    child_ready: true,
                    reviewer_ready: true,
                    ..Default::default()
                };
                assert!(integration_decides(&with, &f));
                let next = next_step(&Snapshot {
                    state: "in_progress",
                    condition: &with,
                    workflow: &workflow,
                    facts: &f,
                });
                assert!(
                    matches!(&next, Next::Park(Park { reason: Reason::Condition(reason), owner, .. })
                        if *reason == blocker && *owner != Owner::IntegrationWorker),
                    "{blocker:?}: {next:?}"
                );
            }
            // A placement refresh or an owner-wait expiry clears its own
            // park even behind a denial: those steps still run.
            let denied = secondary(
                &integration,
                &ParkReason::PlacementDenied { source: source() },
                true,
            );
            for (f, step) in [
                (
                    Facts {
                        refresh_placement: true,
                        ..Default::default()
                    },
                    Step::RefreshPlacement,
                ),
                (
                    Facts {
                        owner_expired: true,
                        ..Default::default()
                    },
                    Step::ExpireOwnerWait,
                ),
            ] {
                assert_eq!(
                    next_step(&Snapshot {
                        state: "in_progress",
                        condition: &denied,
                        workflow: &workflow,
                        facts: &f,
                    }),
                    Next::Step(step)
                );
            }
        }
        assert!(
            steps > 0,
            "a self-clearing primary reaches its clearing step"
        );
    }

    /// A merge state with no attempt anything owns is admitted by the
    /// reconciler's step. No entry is replayed (custom hooks never run
    /// twice), and a reason a dead attempt left on the Task does not hide it.
    #[test]
    fn a_merge_state_nothing_owns_is_admitted_without_replaying_its_entry() {
        let workflow = WorkflowEngine::resolve_workflow("{}");
        let clear = TaskCondition::Clear {
            evidence: ConditionEvidence::default(),
        };
        let mut conditions = crate::task_actions::tests::integration_conditions();
        conditions.push(clear);
        for condition in conditions {
            let f = Facts {
                missing_merge_entry: true,
                child_ready: true,
                root_role_allowed: true,
                reviewer_ready: true,
                ..Default::default()
            };
            assert_eq!(
                next_step(&Snapshot {
                    state: "merging",
                    condition: &condition,
                    workflow: &workflow,
                    facts: &f
                }),
                Next::Step(Step::AdmitIntegration),
                "{condition:?}"
            );
            // A paused Project, a queued step of the Task and an accepted
            // owner command all come first.
            for stopped in [
                Facts {
                    paused: true,
                    ..f.clone()
                },
                Facts {
                    queue_owned: true,
                    ..f.clone()
                },
                Facts {
                    queued_recovery: true,
                    ..f.clone()
                },
            ] {
                assert_ne!(
                    next_step(&Snapshot {
                        state: "merging",
                        condition: &condition,
                        workflow: &workflow,
                        facts: &stopped
                    }),
                    Next::Step(Step::AdmitIntegration)
                );
            }
        }
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
    /// `ReplanRoot` sits beside the root advance: it is the step of an
    /// implementing root, and everything that stops a root stops it first.
    #[test]
    fn a_due_replan_is_the_step_of_an_implementing_root_and_nothing_else() {
        let workflow = WorkflowEngine::resolve_workflow("{}");
        let condition = TaskCondition::Clear {
            evidence: ConditionEvidence::default(),
        };
        let base = Facts {
            root: true,
            child_ready: true,
            reviewer_ready: true,
            root_replan: Some(("child".into(), "park".into())),
            ..Facts::default()
        };
        let resolve = |state: &str, facts: &Facts| {
            next_step(&Snapshot {
                state,
                condition: &condition,
                workflow: &workflow,
                facts,
            })
        };
        let replan = Next::Step(Step::ReplanRoot {
            child_id: "child".into(),
            park: "park".into(),
        });
        assert_eq!(resolve("in_progress", &base), replan);
        // Without the fact the root waits on its subtasks, as before.
        let waits = resolve(
            "in_progress",
            &Facts {
                root_replan: None,
                ..base.clone()
            },
        );
        assert!(matches!(
            &waits,
            Next::Park(Park {
                reason: Reason::Children,
                ..
            })
        ));
        // Never from a gate, an initial or a terminal state.
        for state in ["todo", "planning", "review", "merging", "done"] {
            assert_ne!(resolve(state, &base), replan, "{state}");
        }
        // A paused Project, a blocked root, a human wait, a queue-owned root
        // and an owner command all come first.
        for stop in [
            Facts {
                paused: true,
                ..base.clone()
            },
            Facts {
                blocking: true,
                ..base.clone()
            },
            Facts {
                human_wait: true,
                ..base.clone()
            },
            Facts {
                queue_owned: true,
                ..base.clone()
            },
            Facts {
                queued_recovery: true,
                ..base.clone()
            },
        ] {
            assert_ne!(resolve("in_progress", &stop), replan, "{stop:?}");
        }
        // A refused re-plan is recorded and holds like any other refusal.
        assert!(matches!(
            resolve(
                "in_progress",
                &Facts {
                    disposition_current: true,
                    ..base
                }
            ),
            Next::Park(_)
        ));
    }
}
