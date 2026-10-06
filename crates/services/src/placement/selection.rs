use std::collections::{BTreeMap, BTreeSet};

use api_types::{
    DaemonHandshakeNotification, ExecutorAdapterCapabilityFacts, LifecycleHookDef, ProjectSettings,
    ReviewConfig, WorkspaceRunPurpose,
};
use db::{
    Agent, EnvironmentMachine, EnvironmentReadinessStatus, PlacementOwnerKind, PlacementSelectedBy,
    PlacementState, ProjectMachineReadiness, ProjectMachineReadinessRepo, Repo, RepoLocation,
    RepoLocationKind, RepoLocationOwnerKind, RepoLocationStatus, SqliteDb, Task,
    WorkspacePlacement, WorkspacePlacementRepo,
};
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqliteRow, Row, Sqlite, Transaction};

use super::capacity::{
    count_agent_capacity, count_machine_capacity, AgentCapacity, MachineCapacity,
};
use crate::daemon_transport::DaemonConnectionRegistry;
use crate::Result;

#[derive(Debug, Clone)]
pub struct WorktreeAgent {
    pub role: String,
    pub agent: Agent,
    pub required_capabilities: ExecutorAdapterCapabilityFacts,
}

#[derive(Debug, Clone, Default)]
pub struct ExecutorFacts {
    pub installed: bool,
    pub authenticated: bool,
    pub enabled: bool,
    pub capabilities: ExecutorAdapterCapabilityFacts,
}

impl ExecutorFacts {
    fn available(&self) -> bool {
        self.installed && self.authenticated && self.enabled
    }

    fn covers(&self, required: &ExecutorAdapterCapabilityFacts) -> bool {
        let actual = &self.capabilities;
        (!required.structured_events || actual.structured_events)
            && (!required.usage || actual.usage)
            && (!required.resume || actual.resume)
            && (!required.cancel_ack || actual.cancel_ack)
            && (!required.terminal_observed || actual.terminal_observed)
    }
}

#[derive(Debug, Clone)]
pub struct PlacementCandidate {
    pub environment_readiness: Option<ProjectMachineReadiness>,
    pub location: RepoLocation,
    pub execution_daemon_id: Option<String>,
    /// Only this server's registered embedded machine may execute a server
    /// checkout without a verified shared-mount location.
    pub embedded_execution: bool,
    pub connected: bool,
    pub negotiated_revision: Option<u32>,
    pub workspace_v1: bool,
    pub plan_transport: bool,
    pub machine_probe: bool,
    pub repo_provision: bool,
    /// This location is proposed, or belongs to an unfinished provisioning job.
    pub provisioning: bool,
    pub provision_exhausted: bool,
    pub runtime_ready: bool,
    pub visible: bool,
    /// Availability is resolved per Agent, including its owner-scoped enable
    /// policy. Capability facts come from that Agent's executor on this owner.
    pub executors: BTreeMap<String, ExecutorFacts>,
    pub allowed_run_purposes: Vec<WorkspaceRunPurpose>,
    pub machine_capacity: Option<MachineCapacity>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EnvironmentAdmission {
    Dispatcher,
    LaunchPreflight,
}

#[derive(Debug, Clone)]
pub struct SelectionContext {
    pub(crate) environment_admission: EnvironmentAdmission,
    pub(crate) environment_has_assets: bool,
    /// None means this Project has no checks; no rows or probes are needed.
    pub environment_digest: Option<String>,
    pub environment_checks: Vec<api_types::EnvironmentCheck>,
    pub task: Task,
    pub repo: Repo,
    pub claiming_agent: WorktreeAgent,
    pub worktree_agents: Vec<WorktreeAgent>,
    pub candidates: Vec<PlacementCandidate>,
    pub existing_placement: Option<WorkspacePlacement>,
    pub inherited_root_placement: Option<WorkspacePlacement>,
    pub needed_run_purposes: Vec<WorkspaceRunPurpose>,
    pub agent_capacity: AgentCapacity,
}

impl SelectionContext {
    pub(crate) fn agents(&self) -> impl Iterator<Item = &WorktreeAgent> {
        std::iter::once(&self.claiming_agent).chain(self.worktree_agents.iter())
    }

    pub(crate) fn binding(&self) -> Option<&WorkspacePlacement> {
        self.existing_placement
            .as_ref()
            .filter(|placement| {
                matches!(
                    placement.state,
                    PlacementState::Reserved
                        | PlacementState::Preparing
                        | PlacementState::Ready
                        | PlacementState::Disconnected
                        | PlacementState::Cleaning
                ) || (placement.state == PlacementState::Failed
                    && placement.workspace_handle.is_some())
            })
            .or(self.inherited_root_placement.as_ref())
    }
}

pub use api_types::PlacementFilterCode;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateRejection {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failing_checks: Vec<String>,
    pub repo_location_id: String,
    pub owner_kind: String,
    pub daemon_id: Option<String>,
    pub runtime_id: Option<String>,
    pub filter_codes: Vec<PlacementFilterCode>,
}

impl CandidateRejection {
    fn blocked_only_for_upgrade(&self) -> bool {
        use PlacementFilterCode::*;
        self.filter_codes.contains(&DaemonUpgradeRequired)
            && self.filter_codes.iter().all(|code| {
                // These facts are absent from the revision-2 handshake.
                matches!(
                    code,
                    DaemonUpgradeRequired | CapabilityMissing | RunPurposeDenied
                )
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectionRule {
    ExistingPlacement,
    InheritedRootPlacement,
    AgentPin,
    OnlyEligibleLocation,
    DefaultLocation,
    ServerOwned,
    DeterministicOrder,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectionReason {
    pub rule: SelectionRule,
    pub rejected_candidates: Vec<CandidateRejection>,
}

#[derive(Debug, Clone)]
pub struct PlacementSelection {
    pub candidate: PlacementCandidate,
    pub reused_placement: Option<WorkspacePlacement>,
    pub selected_by: PlacementSelectedBy,
    pub selection_reason: SelectionReason,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("no compatible workspace placement for task {task_id}{hint}", hint = self.upgrade_hint())]
pub struct PlacementUnavailable {
    pub task_id: String,
    pub repo_id: String,
    pub rejected_candidates: Vec<CandidateRejection>,
}

impl PlacementUnavailable {
    pub fn is_deterministic(&self) -> bool {
        !self.rejected_candidates.is_empty()
            && self.rejected_candidates.iter().all(|candidate| {
                !candidate.filter_codes.is_empty()
                    && (candidate.filter_codes.iter().any(|code| {
                        matches!(
                            code,
                            PlacementFilterCode::EnvironmentUnverified
                                | PlacementFilterCode::ProvisionFailed
                        )
                    }) || candidate.filter_codes.iter().all(|code| {
                        // Unverified always needs an owner action, even while
                        // transport facts are temporarily absent. Other
                        // capacity, reachability and environment readiness
                        // change without the Task changing, so a refusal for
                        // them is retried on the next scan.
                        !matches!(
                            code,
                            PlacementFilterCode::OwnerUnreachable
                                | PlacementFilterCode::AgentCapacity
                                | PlacementFilterCode::MachineCapacity
                                | PlacementFilterCode::EnvironmentProbePending
                                | PlacementFilterCode::EnvironmentNotReady
                        )
                    }))
            })
    }

    pub fn needs_daemon_upgrade(&self) -> bool {
        use PlacementFilterCode::*;
        let upgrade_only = self
            .rejected_candidates
            .iter()
            .any(CandidateRejection::blocked_only_for_upgrade);
        let can_retry = self.rejected_candidates.iter().any(|candidate| {
            let handshake_missing = candidate.filter_codes.contains(&OwnerUnreachable)
                && !candidate.filter_codes.contains(&DaemonUpgradeRequired);
            !candidate.filter_codes.is_empty()
                && candidate.filter_codes.iter().all(|code| {
                    matches!(
                        code,
                        OwnerUnreachable
                            | LocationNotReady
                            | AgentCapacity
                            | MachineCapacity
                            | EnvironmentProbePending
                            | EnvironmentNotReady
                    ) || (handshake_missing
                        && matches!(
                            code,
                            CapabilityMissing
                                | WorkspaceProtocolMissing
                                | RunPurposeDenied
                                | ExecutorUnavailable
                        ))
                })
        });
        upgrade_only && !can_retry
    }

    pub(crate) fn upgrade_daemon_ids(&self) -> impl Iterator<Item = &str> {
        self.rejected_candidates
            .iter()
            .filter(|candidate| candidate.blocked_only_for_upgrade())
            .filter_map(|candidate| candidate.daemon_id.as_deref())
    }

    fn upgrade_hint(&self) -> String {
        if self.needs_daemon_upgrade() {
            format!(
                ": {}: {}",
                api_types::DAEMON_UPGRADE_REQUIRED,
                api_types::DAEMON_UPGRADE_REQUIRED_MESSAGE
            )
        } else if self.rejected_candidates.iter().any(|candidate| {
            candidate
                .filter_codes
                .contains(&PlacementFilterCode::EnvironmentUnverified)
        }) {
            ": environment_unverified: declare a machine check or register a repository location"
                .to_owned()
        } else {
            String::new()
        }
    }
}

#[derive(Debug, Clone)]
pub enum SelectionOutcome {
    Selected(Box<PlacementSelection>),
    Unavailable(PlacementUnavailable),
}

impl SelectionOutcome {
    pub fn into_result(self) -> std::result::Result<PlacementSelection, PlacementUnavailable> {
        match self {
            Self::Selected(selection) => Ok(*selection),
            Self::Unavailable(error) => Err(error),
        }
    }
}

/// Pure admission decision. Active reservations, prepared placements and
/// inherited placements pin admission until their lifecycle releases them.
pub fn select_placement(context: &SelectionContext) -> SelectionOutcome {
    let mut candidates = context.candidates.iter().collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        (&left.location.created_at, &left.location.id)
            .cmp(&(&right.location.created_at, &right.location.id))
    });
    let mut eligible = Vec::new();
    let mut rejected_candidates = Vec::new();
    for candidate in candidates {
        if candidate.provisioning && ready_location_can_run(context) {
            continue;
        }
        let filter_codes = filter_candidate(context, candidate);
        if filter_codes.is_empty() {
            eligible.push(candidate);
        } else {
            rejected_candidates.push(CandidateRejection {
                failing_checks: applicable_failing_checks(context, candidate),
                repo_location_id: candidate.location.id.clone(),
                owner_kind: candidate.location.owner_kind.to_string(),
                daemon_id: candidate
                    .execution_daemon_id
                    .clone()
                    .or_else(|| candidate.location.daemon_id.clone()),
                runtime_id: candidate.location.runtime_id.clone(),
                filter_codes,
            });
        }
    }
    let unavailable = || {
        SelectionOutcome::Unavailable(PlacementUnavailable {
            task_id: context.task.id.clone(),
            repo_id: context.repo.id.clone(),
            rejected_candidates: rejected_candidates.clone(),
        })
    };
    if eligible.is_empty() {
        return unavailable();
    }

    let existing = context.existing_placement.as_ref().filter(|placement| {
        placement.state != PlacementState::Cleaned
            && (placement.state != PlacementState::Failed || placement.workspace_handle.is_some())
    });
    let preferred = existing
        .and_then(|placement| {
            eligible
                .iter()
                .find(|candidate| matches_placement(candidate, placement))
                .map(|candidate| {
                    (
                        *candidate,
                        SelectionRule::ExistingPlacement,
                        Some(placement.clone()),
                        placement.selected_by.clone(),
                    )
                })
        })
        .or_else(|| {
            context
                .inherited_root_placement
                .as_ref()
                .and_then(|placement| {
                    eligible
                        .iter()
                        .find(|candidate| matches_placement(candidate, placement))
                        .map(|candidate| {
                            (
                                *candidate,
                                SelectionRule::InheritedRootPlacement,
                                Some(placement.clone()),
                                PlacementSelectedBy::Inherited,
                            )
                        })
                })
        });
    let (candidate, rule, reused_placement, selected_by) = if let Some(preferred) = preferred {
        preferred
    } else if context.binding().is_some() {
        return unavailable();
    } else {
        let pinned = context.agents().any(|role| role.agent.daemon_id.is_some());
        let (candidate, rule) = if pinned {
            let candidate = eligible
                .iter()
                .find(|candidate| candidate.location.is_default)
                .or_else(|| {
                    eligible.iter().find(|candidate| {
                        candidate.location.owner_kind == RepoLocationOwnerKind::Server
                    })
                })
                .copied()
                .unwrap_or(eligible[0]);
            (candidate, SelectionRule::AgentPin)
        } else if eligible.len() == 1 {
            (eligible[0], SelectionRule::OnlyEligibleLocation)
        } else if let Some(candidate) = eligible
            .iter()
            .find(|candidate| candidate.location.is_default)
        {
            (*candidate, SelectionRule::DefaultLocation)
        } else if let Some(candidate) = eligible
            .iter()
            .find(|candidate| candidate.location.owner_kind == RepoLocationOwnerKind::Server)
        {
            (*candidate, SelectionRule::ServerOwned)
        } else {
            (eligible[0], SelectionRule::DeterministicOrder)
        };
        (
            candidate,
            rule,
            None,
            if pinned {
                PlacementSelectedBy::Pin
            } else {
                PlacementSelectedBy::Scheduler
            },
        )
    };
    // Unknown readiness does not demote a preferred owner. Wait for its
    // server probe rather than changing the established placement order.
    if context.candidates.iter().any(|pending| {
        !pending.provisioning
            && filter_candidate(context, pending) == [PlacementFilterCode::EnvironmentProbePending]
            && preference_key(context, pending) < preference_key(context, candidate)
    }) {
        return unavailable();
    }
    SelectionOutcome::Selected(Box::new(PlacementSelection {
        candidate: candidate.clone(),
        reused_placement,
        selected_by,
        selection_reason: SelectionReason {
            rule,
            rejected_candidates,
        },
    }))
}

/// Probe-capable owners need current full readiness for automatic dispatch.
/// Legacy/denied owners, asset-backed Projects and direct claims use launch
/// preflight on existing locations. Provisioning always needs machine checks.
pub(crate) fn environment_filter(
    context: &SelectionContext,
    candidate: &PlacementCandidate,
) -> Option<PlacementFilterCode> {
    if candidate.provisioning {
        if candidate.provision_exhausted {
            return Some(PlacementFilterCode::ProvisionFailed);
        }
        if !context.environment_checks.iter().any(|check| {
            check.scope == api_types::EnvironmentCheckScope::Machine
                && check.applies_to(&context.claiming_agent.role)
        }) {
            return Some(PlacementFilterCode::EnvironmentUnverified);
        }
        if !applicable_failing_checks(context, candidate).is_empty() {
            return Some(PlacementFilterCode::EnvironmentNotReady);
        }
        return Some(PlacementFilterCode::EnvironmentProbePending);
    }
    if candidate.location.owner_kind == RepoLocationOwnerKind::Daemon
        && candidate.location.status != RepoLocationStatus::Ready
    {
        // Full probes need a verified checkout. Disabled provisioning must not
        // leave an unfinished clone behind a probe that can never start.
        return None;
    }
    let digest = context.environment_digest.as_ref()?;
    let current = candidate
        .environment_readiness
        .as_ref()
        .filter(|row| &row.checks_digest == digest);
    match current {
        Some(row)
            if row.status == EnvironmentReadinessStatus::Ready && row.scope_covered == "full" =>
        {
            None
        }
        Some(row)
            if row.status == EnvironmentReadinessStatus::NotReady
                && (!context.environment_has_assets
                    || row.workspace_id.is_some()
                    || row.role.is_some()) =>
        {
            let unnamed_applies = row.failing_checks.is_empty()
                && row
                    .role
                    .as_ref()
                    .is_none_or(|role| &context.claiming_agent.role == role);
            if unnamed_applies || !applicable_failing_checks(context, candidate).is_empty() {
                Some(PlacementFilterCode::EnvironmentNotReady)
            } else if row.scope_covered != "full"
                && context.environment_admission == EnvironmentAdmission::Dispatcher
                && !context.environment_has_assets
                && (candidate.location.owner_kind == RepoLocationOwnerKind::Server
                    || (candidate.machine_probe
                        && candidate
                            .allowed_run_purposes
                            .contains(&WorkspaceRunPurpose::EnvironmentProbe)))
            {
                Some(PlacementFilterCode::EnvironmentProbePending)
            } else {
                None
            }
        }
        _ if (candidate.location.owner_kind == RepoLocationOwnerKind::Daemon
            && (!candidate.machine_probe
                || !candidate
                    .allowed_run_purposes
                    .contains(&WorkspaceRunPurpose::EnvironmentProbe)))
            || context.environment_has_assets
            || context.environment_admission == EnvironmentAdmission::LaunchPreflight =>
        {
            None
        }
        _ => Some(PlacementFilterCode::EnvironmentProbePending),
    }
}

pub(crate) fn applicable_failing_checks(
    context: &SelectionContext,
    candidate: &PlacementCandidate,
) -> Vec<String> {
    let Some(row) = candidate
        .environment_readiness
        .as_ref()
        .filter(|row| context.environment_digest.as_ref() == Some(&row.checks_digest))
    else {
        return Vec::new();
    };
    let names: BTreeSet<&str> = row
        .failing_checks
        .iter()
        .map(|failure| failure.name.as_str())
        .chain(
            row.check_results
                .iter()
                .filter(|result| !result.passed)
                .map(|result| result.name.as_str()),
        )
        .collect();
    names
        .into_iter()
        .filter(|name| {
            context
                .environment_checks
                .iter()
                .find(|check| check.name == *name)
                .is_none_or(|check| check.applies_to(&context.claiming_agent.role))
        })
        .map(str::to_owned)
        .collect()
}

/// Project pause decisions ignore a Task's sticky/pinned restriction, but use
/// the same concrete executor, visibility, policy, roles and readiness filters.
pub(crate) fn environment_pause_candidates(context: &SelectionContext) -> Vec<&PlacementCandidate> {
    let mut unrestricted = context.clone();
    unrestricted.existing_placement = None;
    unrestricted.inherited_root_placement = None;
    unrestricted.claiming_agent.agent.daemon_id = None;
    for role in &mut unrestricted.worktree_agents {
        role.agent.daemon_id = None;
    }
    let specific =
        context.binding().is_some() || context.agents().any(|role| role.agent.daemon_id.is_some());
    if specific
        && context.candidates.iter().any(|candidate| {
            candidate.location.status == RepoLocationStatus::Ready
                && candidate.connected
                && candidate.runtime_ready
                && candidate.visible
                && (candidate.location.owner_kind == RepoLocationOwnerKind::Server
                    || (candidate.workspace_v1
                        && candidate.negotiated_revision.is_some_and(|revision| {
                            revision >= api_types::DAEMON_MIN_PROTOCOL_REVISION
                        })))
                && environment_filter(context, candidate)
                    != Some(PlacementFilterCode::EnvironmentNotReady)
        })
    {
        // A pinned/placed Task cannot pause work using another Agent/executor
        // on a healthy owner. Task-specific executor and policy mismatches on
        // that other owner therefore cannot establish a whole-Project pause.
        return Vec::new();
    }
    let mut unfit = Vec::new();
    for candidate in &context.candidates {
        if candidate.provisioning
            || candidate.location.status != RepoLocationStatus::Ready
            || !candidate.connected
        {
            continue;
        }
        let codes = filter_candidate(&unrestricted, candidate);
        // Capacity/outage/pending are independent transient possibilities,
        // never evidence for an environment-owned whole-Project pause.
        if codes.is_empty()
            || codes.iter().all(|code| {
                matches!(
                    code,
                    PlacementFilterCode::EnvironmentNotReady
                        | PlacementFilterCode::EnvironmentProbePending
                        | PlacementFilterCode::AgentCapacity
                        | PlacementFilterCode::MachineCapacity
                        | PlacementFilterCode::OwnerUnreachable
                )
            }) && codes != [PlacementFilterCode::EnvironmentNotReady]
        {
            return Vec::new();
        }
        if codes == [PlacementFilterCode::EnvironmentNotReady] {
            unfit.push(candidate);
        }
    }
    unfit
}

fn preference_key<'a>(
    context: &SelectionContext,
    candidate: &'a PlacementCandidate,
) -> (u8, bool, bool, &'a str, &'a str) {
    let existing = context
        .existing_placement
        .as_ref()
        .filter(|placement| {
            placement.state != PlacementState::Cleaned
                && (placement.state != PlacementState::Failed
                    || placement.workspace_handle.is_some())
        })
        .is_some_and(|placement| matches_placement(candidate, placement));
    let inherited = context
        .inherited_root_placement
        .as_ref()
        .is_some_and(|placement| matches_placement(candidate, placement));
    let pinned = context.agents().any(|role| role.agent.daemon_id.is_some());
    let rank = if existing {
        0
    } else if inherited {
        1
    } else if pinned {
        2
    } else if candidate.location.is_default {
        3
    } else if candidate.location.owner_kind == RepoLocationOwnerKind::Server {
        4
    } else {
        5
    };
    (
        rank,
        !candidate.location.is_default,
        candidate.location.owner_kind != RepoLocationOwnerKind::Server,
        &candidate.location.created_at,
        &candidate.location.id,
    )
}

fn matches_placement(candidate: &PlacementCandidate, placement: &WorkspacePlacement) -> bool {
    candidate.location.id == placement.repo_location_id
        && candidate.location.owner_kind.to_string() == placement.owner_kind.to_string()
        && (placement.owner_kind == PlacementOwnerKind::Server
            || (candidate.location.daemon_id == placement.daemon_id
                && candidate.location.runtime_id == placement.runtime_id))
        && candidate.execution_daemon_id.as_deref()
            == placement
                .execution_daemon_id
                .as_deref()
                .or(placement.daemon_id.as_deref())
}

/// Capacity waits and pending readiness on an existing checkout never justify
/// copying code elsewhere. Provisioning jobs themselves consume no run slots.
pub(crate) fn ready_location_can_run(context: &SelectionContext) -> bool {
    context
        .candidates
        .iter()
        .filter(|candidate| {
            !candidate.provisioning && candidate.location.status == RepoLocationStatus::Ready
        })
        .any(|candidate| {
            filter_candidate(context, candidate).iter().all(|code| {
                matches!(
                    code,
                    PlacementFilterCode::AgentCapacity
                        | PlacementFilterCode::MachineCapacity
                        | PlacementFilterCode::EnvironmentProbePending
                )
            })
        })
}

pub(crate) fn filter_candidate(
    context: &SelectionContext,
    candidate: &PlacementCandidate,
) -> Vec<PlacementFilterCode> {
    use PlacementFilterCode::*;

    let mut filters = BTreeSet::new();
    let daemon_owned = candidate.location.owner_kind == RepoLocationOwnerKind::Daemon;
    // An offline owner's missing handshake is unknown capability/policy
    // evidence, not a permanent refusal. Admission still fails closed below.
    let owner_facts_known = candidate.embedded_execution || candidate.negotiated_revision.is_some();
    let needs_upgrade = !candidate.embedded_execution
        && candidate.execution_daemon_id.is_some()
        && candidate
            .negotiated_revision
            .is_some_and(|revision| revision < api_types::DAEMON_MIN_PROTOCOL_REVISION);
    if needs_upgrade {
        filters.insert(DaemonUpgradeRequired);
    }
    if (!candidate.connected && !needs_upgrade)
        || !candidate.runtime_ready
        || (daemon_owned
            && (candidate.location.daemon_id.is_none()
                || candidate.location.runtime_id.is_none()
                || candidate.execution_daemon_id != candidate.location.daemon_id))
    {
        filters.insert(OwnerUnreachable);
    }
    if daemon_owned
        && !needs_upgrade
        && (candidate
            .negotiated_revision
            .is_none_or(|revision| revision < 3)
            || !candidate.workspace_v1)
    {
        filters.insert(WorkspaceProtocolMissing);
    }
    if daemon_owned
        && owner_facts_known
        && !needs_upgrade
        && !candidate.plan_transport
        && executors::task_role_can_write_plan(Some(&context.claiming_agent.role))
    {
        filters.insert(CapabilityMissing);
    }
    if (!candidate.provisioning && candidate.location.status != RepoLocationStatus::Ready)
        || candidate.location.repo_id != context.repo.id
        || (candidate.location.owner_kind == RepoLocationOwnerKind::Server
            && candidate.location.kind != RepoLocationKind::SharedMount
            && (candidate.location.daemon_id.is_some() || candidate.location.runtime_id.is_some()))
        || (candidate.location.kind == RepoLocationKind::SharedMount
            && (daemon_owned
                || candidate.location.daemon_id.is_none()
                || candidate.location.runtime_id.is_none()
                || candidate.execution_daemon_id != candidate.location.daemon_id))
        || (!daemon_owned
            && candidate.location.kind != RepoLocationKind::SharedMount
            && !candidate.embedded_execution)
    {
        filters.insert(LocationNotReady);
    }
    if let Some(binding) = context.binding() {
        if !matches_placement(candidate, binding) {
            filters.insert(PinMismatch);
        } else if matches!(binding.state, PlacementState::Disconnected) {
            // Reconnection must reconcile the durable placement before a new
            // execution can start, even if its command socket is already up.
            filters.insert(OwnerUnreachable);
        } else if binding.state == PlacementState::Cleaning
            || binding.state == PlacementState::Failed
        {
            filters.insert(LocationNotReady);
        }
    }
    for role in context.agents() {
        if role
            .agent
            .daemon_id
            .as_deref()
            .is_some_and(|pin| Some(pin) != candidate.execution_daemon_id.as_deref())
        {
            filters.insert(PinMismatch);
        }
        if daemon_owned && role.agent.backend_kind != "cli" {
            filters.insert(NativeBackendUnsupported);
            continue;
        }
        match candidate.executors.get(&role.agent.id) {
            Some(facts) if facts.available() && owner_facts_known => {
                if !facts.covers(&role.required_capabilities) {
                    filters.insert(CapabilityMissing);
                }
            }
            _ => {
                filters.insert(ExecutorUnavailable);
            }
        }
    }
    if !candidate.provisioning
        && !context
            .agent_capacity
            .has_capacity(context.claiming_agent.agent.max_concurrent_tasks)
    {
        filters.insert(AgentCapacity);
    }
    if !candidate.provisioning
        && candidate
            .machine_capacity
            .is_none_or(|capacity| !capacity.has_capacity())
    {
        filters.insert(MachineCapacity);
    }
    if (!daemon_owned || owner_facts_known)
        && context
            .needed_run_purposes
            .iter()
            .any(|purpose| !candidate.allowed_run_purposes.contains(purpose))
    {
        filters.insert(RunPurposeDenied);
    }
    if candidate.provisioning {
        if candidate.location.status == RepoLocationStatus::Unavailable {
            filters.insert(LocationNotReady);
        }
        if !candidate.machine_probe || !candidate.repo_provision {
            filters.insert(CapabilityMissing);
        }
        if ![
            WorkspaceRunPurpose::EnvironmentProbe,
            WorkspaceRunPurpose::RepoProvision,
        ]
        .iter()
        .all(|purpose| candidate.allowed_run_purposes.contains(purpose))
        {
            filters.insert(RunPurposeDenied);
        }
    }
    if !candidate.visible {
        filters.insert(NotVisible);
    }
    // Readiness gates otherwise eligible owners. Hard executor/capability
    // failures stay deterministic; capacity and absent transport facts retry.
    if filters.iter().all(|code| {
        matches!(
            code,
            LocationNotReady
                | OwnerUnreachable
                | WorkspaceProtocolMissing
                | ExecutorUnavailable
                | CapabilityMissing
                | RunPurposeDenied
        ) && filters.contains(&OwnerUnreachable)
    }) || filters
        .iter()
        .all(|code| matches!(code, LocationNotReady | AgentCapacity | MachineCapacity))
    {
        if let Some(code) = environment_filter(context, candidate) {
            filters.insert(code);
        }
    }
    filters.into_iter().collect()
}

/// Only shell operations need workspace.run permission. Environment values
/// and copied assets alone do not require environment_setup permission.
pub fn needed_run_purposes(
    review: &ReviewConfig,
    settings: &ProjectSettings,
    worktree_roles: &[&str],
) -> Vec<WorkspaceRunPurpose> {
    let mut purposes = Vec::new();
    if settings.environment.checks.iter().any(|check| {
        check.roles.is_empty()
            || check
                .roles
                .iter()
                .any(|role| worktree_roles.contains(&role.as_str()))
    }) {
        purposes.push(WorkspaceRunPurpose::EnvironmentSetup);
    }
    if settings
        .lifecycle_hooks
        .values()
        .flatten()
        .any(|hook| matches!(hook, LifecycleHookDef::Script { .. }))
    {
        purposes.push(WorkspaceRunPurpose::Hook);
    }
    if !review.ci_steps.is_empty()
        || !review.setup_steps.is_empty()
        || !review.conformance_checks.is_empty()
    {
        purposes.push(WorkspaceRunPurpose::CiStep);
    }
    purposes
}

#[derive(Debug, Clone, Default)]
pub struct ServerFacts {
    /// The optional embedded execution provider, not the workspace owner.
    pub execution_daemon_id: Option<String>,
    /// Facts keyed by Agent id, including native profile health.
    pub executors: BTreeMap<String, ExecutorFacts>,
}

#[derive(Debug, Clone)]
pub struct ConnectionHandshake {
    pub connection_id: u64,
    pub handshake: DaemonHandshakeNotification,
}

pub struct SelectionLoadInput<'a> {
    pub task: &'a Task,
    pub repo: &'a Repo,
    pub claiming_agent: &'a WorktreeAgent,
    pub worktree_agents: &'a [WorktreeAgent],
    pub task_owner_id: Option<&'a str>,
    pub workspace_id: Option<&'a str>,
    pub inherited_root_workspace_id: Option<&'a str>,
    pub review_config: &'a ReviewConfig,
    pub project_settings: &'a ProjectSettings,
    pub server: &'a ServerFacts,
    /// Retained negotiated facts, accepted only for the current compatible
    /// connection incarnation.
    pub handshakes: &'a BTreeMap<String, ConnectionHandshake>,
    pub fallback_server_location: Option<RepoLocation>,
}

/// Load without opening a second transaction or doing owner I/O. The caller
/// keeps its BEGIN IMMEDIATE transaction through reservation/start admission.
pub async fn load_selection_context(
    db: &SqliteDb,
    transaction: &mut Transaction<'_, Sqlite>,
    registry: &DaemonConnectionRegistry,
    input: SelectionLoadInput<'_>,
) -> Result<SelectionContext> {
    let environment = &input.project_settings.environment;
    let environment_digest =
        (!environment.checks.is_empty()).then(|| db::environment_checks_digest(environment));
    let readiness = if environment_digest.is_some() {
        db.list_readiness_in_tx(transaction, &input.task.project_id)
            .await?
    } else {
        Vec::new()
    };
    let mut existing_placement = match input.workspace_id {
        Some(id) => WorkspacePlacementRepo::get_by_workspace_id_in_tx(db, transaction, id).await?,
        None => None,
    };
    let mut inherited_root_placement = match input.inherited_root_workspace_id {
        Some(id) => WorkspacePlacementRepo::get_by_workspace_id_in_tx(db, transaction, id).await?,
        None => None,
    };
    // Backfilled and directly inserted server workspaces have no recorded
    // provider yet. Admission binds the optional embedded provider once.
    for placement in existing_placement
        .iter_mut()
        .chain(inherited_root_placement.iter_mut())
    {
        if placement.owner_kind == PlacementOwnerKind::Server
            && placement.execution_daemon_id.is_none()
        {
            placement.execution_daemon_id = input.server.execution_daemon_id.clone();
        }
    }
    // Include non-ready rows for rejection evidence; the pure filter can never
    // select them. Reading all locations also avoids truncating admission at
    // an arbitrary API page boundary.
    let rows = sqlx::query("SELECT * FROM repo_location WHERE repo_id = ? ORDER BY created_at, id")
        .bind(&input.repo.id)
        .fetch_all(&mut **transaction)
        .await?;
    let mut locations = rows
        .into_iter()
        .map(map_location)
        .collect::<Result<Vec<_>>>()?;
    if locations.is_empty() {
        locations.extend(input.fallback_server_location);
    }
    let mut provisioning_ids = BTreeSet::new();
    let mut exhausted_ids = BTreeSet::new();
    if input.project_settings.placement.provision == api_types::PlacementProvision::WhenVerified
        && input
            .repo
            .remote_url
            .as_deref()
            .is_some_and(|remote| !remote.trim().is_empty())
        && existing_placement.as_ref().is_none_or(|placement| {
            placement.state == PlacementState::Cleaned
                || (placement.state == PlacementState::Failed
                    && placement.workspace_handle.is_none())
        })
        && inherited_root_placement.is_none()
    {
        let runtimes = sqlx::query("SELECT r.*, j.runtime_id AS retry_runtime, j.location_id AS retry_location, j.attempts AS retry_attempts, j.checks_digest AS retry_digest, j.connection_id AS retry_connection FROM runtime r JOIN daemon d ON d.id = r.daemon_id LEFT JOIN repo_provision_retry j ON j.runtime_id = r.id AND j.repo_id = ? WHERE d.removed_at IS NULL AND d.machine_id <> ? ORDER BY d.created_at, d.id, r.id")
            .bind(&input.repo.id).bind(db.server_run_cap.embedded_machine_id()).fetch_all(&mut **transaction).await?;
        for runtime in runtimes {
            let id: String = runtime.try_get("id")?;
            let daemon: String = runtime.try_get("daemon_id")?;
            let retry_exists = runtime
                .try_get::<Option<String>, _>("retry_runtime")?
                .is_some();
            let retry_location: Option<String> = runtime.try_get("retry_location")?;
            let exhausted = super::provisioning::ProvisionRetryState {
                attempts: runtime
                    .try_get::<Option<i64>, _>("retry_attempts")?
                    .unwrap_or_default(),
                inputs_digest: runtime
                    .try_get::<Option<String>, _>("retry_digest")?
                    .unwrap_or_default(),
                connection_id: runtime
                    .try_get::<Option<i64>, _>("retry_connection")?
                    .unwrap_or_default(),
            }
            .exhausted(
                input.project_settings,
                input
                    .handshakes
                    .get(&daemon)
                    .map(|facts| facts.connection_id),
            );
            let capable = input.handshakes.get(&daemon).is_some_and(|facts| {
                [
                    api_types::DAEMON_CAPABILITY_MACHINE_PROBE,
                    api_types::DAEMON_CAPABILITY_REPO_PROVISION,
                ]
                .iter()
                .all(|capability| {
                    facts
                        .handshake
                        .capabilities
                        .iter()
                        .any(|fact| fact == capability)
                }) && [
                    WorkspaceRunPurpose::EnvironmentProbe,
                    WorkspaceRunPurpose::RepoProvision,
                ]
                .iter()
                .all(|purpose| {
                    facts
                        .handshake
                        .workspace_run_policy
                        .allowed_purposes
                        .contains(purpose)
                })
            });
            if !capable && !retry_exists {
                continue;
            }
            if let Some(location) = locations
                .iter()
                .find(|location| location.runtime_id.as_deref() == Some(&id))
            {
                if retry_location.as_deref() == Some(&location.id) {
                    provisioning_ids.insert(location.id.clone());
                    if exhausted {
                        exhausted_ids.insert(location.id.clone());
                    }
                }
                continue;
            }
            let location_id = format!("provision-{id}");
            provisioning_ids.insert(location_id.clone());
            if exhausted {
                exhausted_ids.insert(location_id.clone());
            }
            locations.push(RepoLocation {
                id: location_id,
                repo_id: input.repo.id.clone(),
                owner_kind: RepoLocationOwnerKind::Daemon,
                daemon_id: Some(daemon),
                runtime_id: Some(id),
                path: std::path::Path::new(&runtime.try_get::<String, _>("workspace_root")?)
                    .join("repos")
                    .join(&input.repo.id)
                    .to_string_lossy()
                    .into_owned(),
                kind: RepoLocationKind::ManagedClone,
                is_default: false,
                status: RepoLocationStatus::Unverified,
                last_verified_at: None,
                last_error: None,
                version: 1,
                created_at: runtime.try_get("created_at")?,
                updated_at: runtime.try_get("updated_at")?,
            });
        }
    }
    let mut candidates = Vec::with_capacity(locations.len());
    for location in locations {
        let daemon_owned = location.owner_kind == RepoLocationOwnerKind::Daemon;
        let reused = existing_placement
            .as_ref()
            .or(inherited_root_placement.as_ref())
            .filter(|placement| {
                placement.repo_location_id == location.id
                    && placement.state != PlacementState::Cleaned
                    && (placement.state != PlacementState::Failed
                        || placement.workspace_handle.is_some())
            });
        let execution_daemon_id = if let Some(placement) = reused {
            placement
                .execution_daemon_id
                .clone()
                .or_else(|| placement.daemon_id.clone())
        } else if daemon_owned || location.kind == RepoLocationKind::SharedMount {
            location.daemon_id.clone()
        } else {
            input.server.execution_daemon_id.clone()
        };
        let daemon = match execution_daemon_id.as_deref() {
            Some(id) => sqlx::query("SELECT machine_id, owner_id, visibility, max_concurrent_runs, run_limit, detected_clis_json FROM daemon WHERE id = ? AND removed_at IS NULL")
                .bind(id)
                .fetch_optional(&mut **transaction)
                .await?,
            None => None,
        };
        let embedded_execution = !daemon_owned
            && (execution_daemon_id.is_none()
                || daemon.as_ref().is_some_and(|row| {
                    row.try_get::<String, _>("machine_id")
                        .is_ok_and(|id| id == db.server_run_cap.embedded_machine_id())
                }));
        let remote_execution =
            daemon_owned || location.kind == RepoLocationKind::SharedMount || !embedded_execution;
        let connection = execution_daemon_id
            .as_deref()
            .and_then(|id| registry.get(id));
        let handshake = execution_daemon_id.as_deref().and_then(|id| {
            let connection = connection.as_ref()?;
            input.handshakes.get(id).filter(|facts| {
                facts.connection_id == connection.id()
                    && !connection.is_stale()
                    && connection.protocol_allows_dispatch()
            })
        });
        let runtime_ready = if remote_execution {
            match (
                location.runtime_id.as_deref(),
                execution_daemon_id.as_deref(),
            ) {
                (Some(runtime_id), Some(daemon_id)) => sqlx::query_scalar::<_, bool>(
                    "SELECT status = 'ready' FROM runtime WHERE id = ? AND daemon_id = ?",
                )
                .bind(runtime_id)
                .bind(daemon_id)
                .fetch_optional(&mut **transaction)
                .await?
                .unwrap_or(false),
                _ => false,
            }
        } else {
            true
        };
        let visible = match daemon.as_ref() {
            Some(row) => {
                let owner: Option<String> = row.try_get("owner_id")?;
                let visibility: String = row.try_get("visibility")?;
                visibility == "global" || owner.is_none() || owner.as_deref() == input.task_owner_id
            }
            None => !remote_execution && execution_daemon_id.is_none(),
        };
        let detected: serde_json::Value = daemon
            .as_ref()
            .map(|row| row.try_get::<String, _>("detected_clis_json"))
            .transpose()?
            .and_then(|json| serde_json::from_str(&json).ok())
            .unwrap_or_default();
        let mut executors = BTreeMap::new();
        for role in std::iter::once(input.claiming_agent).chain(input.worktree_agents.iter()) {
            let agent = &role.agent;
            if executors.contains_key(&agent.id) {
                continue;
            }
            let mut facts =
                if !remote_execution || (agent.backend_kind == "native" && !daemon_owned) {
                    input
                        .server
                        .executors
                        .get(&agent.id)
                        .cloned()
                        .unwrap_or_default()
                } else {
                    let cli = detected.as_array().and_then(|clis| {
                        clis.iter().find(|cli| {
                            cli.get("kind").and_then(serde_json::Value::as_str)
                                == Some(agent.executor_type.as_str())
                        })
                    });
                    ExecutorFacts {
                        installed: cli.is_some_and(|cli| {
                            cli.get("availability").and_then(serde_json::Value::as_str)
                                != Some("not_installed")
                        }),
                        authenticated: cli.is_some_and(|cli| {
                            cli.get("availability").and_then(serde_json::Value::as_str)
                                == Some("authenticated")
                        }),
                        enabled: true,
                        capabilities: handshake
                            .and_then(|facts| {
                                facts
                                    .handshake
                                    .executor_capabilities
                                    .get(&agent.executor_type)
                            })
                            .cloned()
                            .unwrap_or_default(),
                    }
                };
            facts.enabled &= !agent.paused;
            if agent.backend_kind == "cli" {
                if let Some(daemon_id) = execution_daemon_id.as_deref() {
                    let enabled = match agent.owner_id.as_deref() {
                        Some(owner) => sqlx::query_scalar::<_, bool>(
                            "SELECT enabled FROM cli_runtime_policy WHERE owner_user_id = ? AND daemon_id = ? AND executor_type = ?",
                        )
                        .bind(owner).bind(daemon_id).bind(&agent.executor_type)
                        .fetch_optional(&mut **transaction).await?.unwrap_or(true),
                        None => sqlx::query_scalar::<_, Option<bool>>(
                            "SELECT MIN(enabled) FROM cli_runtime_policy WHERE daemon_id = ? AND executor_type = ?",
                        )
                        .bind(daemon_id).bind(&agent.executor_type)
                        .fetch_one(&mut **transaction).await?.unwrap_or(true),
                    };
                    facts.enabled &= enabled;
                }
            }
            executors.insert(agent.id.clone(), facts);
        }
        let machine_id = if execution_daemon_id.is_none()
            || daemon.as_ref().is_some_and(|row| {
                row.try_get::<String, _>("machine_id")
                    .is_ok_and(|id| id == db.server_run_cap.embedded_machine_id())
            }) {
            None
        } else {
            execution_daemon_id.as_deref()
        };
        let cap = if machine_id.is_none() {
            super::capacity::server_machine_cap(db, transaction).await?
        } else {
            let reported = daemon
                .as_ref()
                .map(|row| row.try_get::<Option<i64>, _>("max_concurrent_runs"))
                .transpose()?
                .flatten();
            let admin = daemon
                .as_ref()
                .map(|row| row.try_get::<Option<u32>, _>("run_limit"))
                .transpose()?
                .flatten();
            super::capacity::effective_machine_cap(reported, admin)
        };
        let machine_capacity = Some(
            count_machine_capacity(
                transaction,
                machine_id,
                cap,
                &db.server_run_cap.embedded_machine_id(),
            )
            .await?,
        );
        let machine = EnvironmentMachine::from_location(&location);
        let environment_readiness = readiness.iter().find(|row| row.machine == machine).cloned();
        candidates.push(PlacementCandidate {
            environment_readiness,
            provisioning: provisioning_ids.contains(&location.id),
            provision_exhausted: exhausted_ids.contains(&location.id),
            machine_probe: handshake.is_some_and(|facts| {
                facts
                    .handshake
                    .capabilities
                    .iter()
                    .any(|fact| fact == api_types::DAEMON_CAPABILITY_MACHINE_PROBE)
            }),
            repo_provision: handshake.is_some_and(|facts| {
                facts
                    .handshake
                    .capabilities
                    .iter()
                    .any(|fact| fact == api_types::DAEMON_CAPABILITY_REPO_PROVISION)
            }),
            location,
            execution_daemon_id,
            embedded_execution,
            connected: !remote_execution
                || connection.as_ref().is_some_and(|connection| {
                    !connection.is_stale() && connection.protocol_allows_dispatch()
                }),
            negotiated_revision: connection
                .as_ref()
                .and_then(|connection| connection.negotiated_revision()),
            workspace_v1: handshake.is_some_and(|facts| {
                facts
                    .handshake
                    .capabilities
                    .iter()
                    .any(|capability| capability == "workspace.v1")
            }),
            plan_transport: handshake.is_some_and(|facts| {
                facts
                    .handshake
                    .capabilities
                    .iter()
                    .any(|capability| capability == api_types::DAEMON_CAPABILITY_PLAN_TRANSPORT)
            }),
            runtime_ready,
            visible,
            executors,
            allowed_run_purposes: if daemon_owned {
                handshake
                    .map(|facts| {
                        facts
                            .handshake
                            .workspace_run_policy
                            .allowed_purposes
                            .clone()
                    })
                    .unwrap_or_default()
            } else {
                vec![
                    WorkspaceRunPurpose::EnvironmentSetup,
                    WorkspaceRunPurpose::Hook,
                    WorkspaceRunPurpose::CiStep,
                ]
            },
            machine_capacity,
        });
    }
    Ok(SelectionContext {
        environment_admission: EnvironmentAdmission::Dispatcher,
        environment_has_assets: !environment.assets.is_empty(),
        environment_checks: environment.checks.clone(),
        environment_digest,
        task: input.task.clone(),
        repo: input.repo.clone(),
        claiming_agent: input.claiming_agent.clone(),
        worktree_agents: input.worktree_agents.to_vec(),
        candidates,
        existing_placement,
        inherited_root_placement,
        needed_run_purposes: needed_run_purposes(
            input.review_config,
            input.project_settings,
            &[input.claiming_agent.role.as_str()],
        ),
        agent_capacity: count_agent_capacity(transaction, &input.claiming_agent.agent.id).await?,
    })
}

fn map_location(row: SqliteRow) -> Result<RepoLocation> {
    Ok(RepoLocation {
        id: row.try_get("id")?,
        repo_id: row.try_get("repo_id")?,
        owner_kind: row
            .try_get::<String, _>("owner_kind")?
            .parse()
            .map_err(db::DbError::Check)?,
        daemon_id: row.try_get("daemon_id")?,
        runtime_id: row.try_get("runtime_id")?,
        path: row.try_get("path")?,
        kind: row
            .try_get::<String, _>("kind")?
            .parse()
            .map_err(db::DbError::Check)?,
        is_default: row.try_get::<i64, _>("is_default")? != 0,
        status: row
            .try_get::<String, _>("status")?
            .parse()
            .map_err(db::DbError::Check)?,
        last_verified_at: row.try_get("last_verified_at")?,
        last_error: row.try_get("last_error")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn partial_machine_facts_do_not_skip_full_checks_for_another_role() {
        let mut context = context();
        context.environment_digest = Some("current".into());
        context.environment_checks = vec![serde_json::from_value(serde_json::json!({"name":"cargo","command":"false","scope":"machine","roles":["reviewer"]})).unwrap()];
        context.candidates[0].machine_probe = true;
        context.candidates[0]
            .allowed_run_purposes
            .push(WorkspaceRunPurpose::EnvironmentProbe);
        let mut row = readiness(&context, 0, EnvironmentReadinessStatus::NotReady);
        row.scope_covered = "machine".into();
        context.candidates[0].environment_readiness = Some(row);
        rejected(&context, PlacementFilterCode::EnvironmentProbePending);
        context.candidates[0]
            .environment_readiness
            .as_mut()
            .unwrap()
            .scope_covered = "full".into();
        selected(&context);
    }

    #[test]
    fn probe_capable_daemon_defers_but_legacy_daemon_uses_preflight() {
        let mut context = context();
        context.environment_digest = Some("current".into());
        context.candidates[0].machine_probe = true;
        context.candidates[0]
            .allowed_run_purposes
            .push(api_types::WorkspaceRunPurpose::EnvironmentProbe);
        rejected(&context, PlacementFilterCode::EnvironmentProbePending);
        context.candidates[0].machine_probe = false;
        selected(&context);
        context.candidates[0].machine_probe = true;
        context.environment_admission = EnvironmentAdmission::LaunchPreflight;
        selected(&context);
        context.environment_admission = EnvironmentAdmission::Dispatcher;
        context.environment_has_assets = true;
        selected(&context);
    }
    #[test]
    fn provisioning_requires_machine_checks_and_never_follows_capacity_only_refusal() {
        let mut context = context();
        let mut proposed = context.candidates[0].clone();
        proposed.location.id = "proposed".into();
        proposed.provisioning = true;
        proposed.machine_probe = true;
        proposed.repo_provision = true;
        proposed.allowed_run_purposes.extend([
            api_types::WorkspaceRunPurpose::EnvironmentProbe,
            api_types::WorkspaceRunPurpose::RepoProvision,
        ]);
        context.candidates[0]
            .machine_capacity
            .as_mut()
            .unwrap()
            .max_concurrent_runs = Some(1);
        context.candidates[0]
            .machine_capacity
            .as_mut()
            .unwrap()
            .running_executions = 1;
        context.candidates.push(proposed);
        assert!(ready_location_can_run(&context));
        let refusal = rejected(&context, PlacementFilterCode::MachineCapacity);
        assert_eq!(refusal.rejected_candidates.len(), 1);
        context.candidates[0].executors.clear();
        assert!(!ready_location_can_run(&context));
        rejected(&context, PlacementFilterCode::EnvironmentUnverified);
        context.environment_checks = vec![serde_json::from_value(
            serde_json::json!({"name":"cargo","command":"true","scope":"machine"}),
        )
        .unwrap()];
        context.environment_digest = Some("current".into());
        rejected(&context, PlacementFilterCode::EnvironmentProbePending);
    }

    #[test]
    fn exhausted_provisioning_machine_is_unreachable_and_cannot_pause_project() {
        let mut context = context();
        context.environment_checks = vec![serde_json::from_value(
            serde_json::json!({"name":"cargo","command":"true","scope":"machine"}),
        )
        .unwrap()];
        context.environment_digest = Some("current".into());
        context.candidates[0].provisioning = true;
        context.candidates[0].provision_exhausted = true;
        context.candidates[0].machine_probe = true;
        context.candidates[0].repo_provision = true;
        context.candidates[0].allowed_run_purposes.extend([
            WorkspaceRunPurpose::EnvironmentProbe,
            WorkspaceRunPurpose::RepoProvision,
        ]);
        let refusal = rejected(&context, PlacementFilterCode::ProvisionFailed);
        assert!(refusal.is_deterministic());
        assert!(!crate::placement::is_retryable_admission_refusal(
            &crate::ServiceError::PlacementUnavailable(refusal)
        ));
        assert!(environment_pause_candidates(&context).is_empty());
        context.candidates.push(candidate(&context, "server", None));
        context.candidates[1].environment_readiness =
            Some(readiness(&context, 1, EnvironmentReadinessStatus::NotReady));
        let paused = environment_pause_candidates(&context);
        assert_eq!(paused.len(), 1);
        assert_eq!(paused[0].location.id, "server");
    }

    #[test]
    fn unverified_requires_action_even_when_connection_facts_are_missing() {
        let mut context = context();
        context.candidates[0].provisioning = true;
        context.candidates[0].machine_probe = true;
        context.candidates[0].repo_provision = true;
        context.candidates[0].allowed_run_purposes.extend([
            WorkspaceRunPurpose::EnvironmentProbe,
            WorkspaceRunPurpose::RepoProvision,
        ]);
        context.candidates[0].connected = false;
        context.candidates[0].negotiated_revision = None;
        context.candidates[0].workspace_v1 = false;
        context.candidates[0].allowed_run_purposes.clear();
        let refusal = rejected(&context, PlacementFilterCode::EnvironmentUnverified);
        assert!(refusal.is_deterministic());
        assert!(!crate::placement::is_retryable_admission_refusal(
            &crate::ServiceError::PlacementUnavailable(refusal)
        ));
    }

    #[test]
    fn unfinished_daemon_location_waits_for_verification_instead_of_an_impossible_probe() {
        let mut context = context();
        context.environment_digest = Some("current".into());
        context.candidates[0].location.status = RepoLocationStatus::Unverified;
        context.candidates[0].machine_probe = true;
        context.candidates[0]
            .allowed_run_purposes
            .push(WorkspaceRunPurpose::EnvironmentProbe);
        let mut row = readiness(&context, 0, EnvironmentReadinessStatus::Ready);
        row.scope_covered = "machine".into();
        context.candidates[0].environment_readiness = Some(row);
        let refusal = rejected(&context, PlacementFilterCode::LocationNotReady);
        assert_eq!(
            refusal.rejected_candidates[0].filter_codes,
            [PlacementFilterCode::LocationNotReady]
        );
        assert!(refusal.is_deterministic());
    }

    use super::*;
    use db::AgentStatus;

    const NOW: &str = "2026-09-29T00:00:00Z";

    fn capabilities() -> ExecutorAdapterCapabilityFacts {
        ExecutorAdapterCapabilityFacts {
            structured_events: true,
            usage: true,
            resume: true,
            cancel_ack: true,
            terminal_observed: true,
        }
    }

    fn executor() -> ExecutorFacts {
        ExecutorFacts {
            installed: true,
            authenticated: true,
            enabled: true,
            capabilities: capabilities(),
        }
    }

    fn agent(id: &str, role: &str) -> WorktreeAgent {
        WorktreeAgent {
            role: role.to_owned(),
            required_capabilities: capabilities(),
            agent: Agent {
                id: id.to_owned(),
                name: id.to_owned(),
                description: None,
                profile_id: format!("profile-{id}"),
                backend_kind: "cli".to_owned(),
                executor_type: "codex".to_owned(),
                provider: None,
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: "[]".to_owned(),
                tool_policy_json: "{}".to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: None,
                daemon_id: None,
                max_concurrent_tasks: 1,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status: AgentStatus::Idle,
                last_heartbeat_at: None,
                is_default: false,
                paused: false,
                owner_id: Some("owner".to_owned()),
                visibility: "private".to_owned(),
                version: 1,
                created_at: NOW.to_owned(),
                updated_at: NOW.to_owned(),
            },
        }
    }

    fn context() -> SelectionContext {
        let mut context = SelectionContext {
            environment_admission: EnvironmentAdmission::Dispatcher,
            environment_has_assets: false,
            environment_digest: None,
            environment_checks: Vec::new(),
            task: Task {
                id: "task".to_owned(),
                project_id: "project".to_owned(),
                parent_task_id: None,
                assignee_type: None,
                assignee_id: None,
                title: "Task".to_owned(),
                description: None,
                task_type: "implementation".to_owned(),
                status: "todo".to_owned(),
                is_automation: false,
                priority: 0,
                board_position: 0.0,
                subtask_order: None,
                task_state_config: None,
                merge_config: None,
                metadata_json: None,
                plan: None,
                error_annotation: None,
                blocked_json: None,
                failed_json: None,
                entry_barrier_json: None,
                review_passed_at: None,
                archived_at: None,
                deleted_at: None,
                version: 1,
                created_at: NOW.to_owned(),
                updated_at: NOW.to_owned(),
            },
            repo: Repo {
                id: "repo".to_owned(),
                project_id: "project".to_owned(),
                name: "Repo".to_owned(),
                remote_url: Some("https://example.test/repo.git".to_owned()),
                local_path: None,
                default_branch: "main".to_owned(),
                created_at: NOW.to_owned(),
                updated_at: NOW.to_owned(),
            },
            claiming_agent: agent("coder", "coder"),
            worktree_agents: vec![agent("reviewer", "reviewer"), agent("planner", "planner")],
            candidates: Vec::new(),
            existing_placement: None,
            inherited_root_placement: None,
            needed_run_purposes: vec![WorkspaceRunPurpose::CiStep],
            agent_capacity: AgentCapacity::default(),
        };
        context
            .candidates
            .push(candidate(&context, "mac-location", Some("mac")));
        context
    }

    fn candidate(context: &SelectionContext, id: &str, daemon: Option<&str>) -> PlacementCandidate {
        PlacementCandidate {
            machine_probe: false,
            repo_provision: false,
            provisioning: false,
            provision_exhausted: false,
            environment_readiness: None,
            location: RepoLocation {
                id: id.to_owned(),
                repo_id: context.repo.id.clone(),
                owner_kind: if daemon.is_some() {
                    RepoLocationOwnerKind::Daemon
                } else {
                    RepoLocationOwnerKind::Server
                },
                daemon_id: daemon.map(str::to_owned),
                runtime_id: daemon.map(|id| format!("runtime-{id}")),
                path: format!("/checkout/{id}"),
                kind: RepoLocationKind::PrimaryCheckout,
                is_default: false,
                status: RepoLocationStatus::Ready,
                last_verified_at: Some(NOW.to_owned()),
                last_error: None,
                version: 1,
                created_at: NOW.to_owned(),
                updated_at: NOW.to_owned(),
            },
            execution_daemon_id: daemon.map(str::to_owned),
            embedded_execution: daemon.is_none(),
            connected: true,
            negotiated_revision: Some(api_types::DAEMON_PROTOCOL_REVISION),
            workspace_v1: true,
            plan_transport: true,
            runtime_ready: true,
            visible: true,
            executors: context
                .agents()
                .map(|role| (role.agent.id.clone(), executor()))
                .collect(),
            allowed_run_purposes: vec![WorkspaceRunPurpose::CiStep],
            machine_capacity: Some(MachineCapacity {
                max_concurrent_runs: Some(2),
                ..MachineCapacity::default()
            }),
        }
    }

    #[test]
    fn machine_capacity_full_server_selects_free_daemon_and_zero_is_unlimited() {
        let mut context = context();
        let mut server = candidate(&context, "server", None);
        server.machine_capacity.as_mut().unwrap().running_executions = 2;
        context.candidates.insert(0, server);
        let result = selected(&context);
        assert_eq!(result.candidate.execution_daemon_id.as_deref(), Some("mac"));
        assert_eq!(
            result.selection_reason.rejected_candidates[0].filter_codes,
            vec![PlacementFilterCode::MachineCapacity]
        );
        context.candidates.remove(1);
        context.candidates[0]
            .machine_capacity
            .as_mut()
            .unwrap()
            .max_concurrent_runs = Some(0);
        assert!(matches!(
            select_placement(&context),
            SelectionOutcome::Selected(_)
        ));
    }

    #[test]
    fn machine_capacity_selection_enforces_effective_caps() {
        for (reported, admin, expected) in [
            (Some(6), Some(2), 2),
            (Some(3), Some(8), 3),
            (Some(3), None, 3),
        ] {
            let mut context = context();
            let cap = super::super::capacity::effective_machine_cap(reported, admin);
            let capacity = context.candidates[0].machine_capacity.as_mut().unwrap();
            capacity.max_concurrent_runs = cap;
            capacity.running_executions = expected;
            rejected(&context, PlacementFilterCode::MachineCapacity);
            context.candidates[0]
                .machine_capacity
                .as_mut()
                .unwrap()
                .running_executions -= 1;
            selected(&context);
        }
    }

    #[test]
    fn machine_capacity_start_rechecks_ready_placement() {
        let mut context = context();
        let capacity = context.candidates[0].machine_capacity.as_mut().unwrap();
        capacity.max_concurrent_runs = Some(1);
        capacity.active_chat_turns = 1;
        context.existing_placement = Some(placement(&context.candidates[0]));
        rejected(&context, PlacementFilterCode::MachineCapacity);
        context.candidates[0]
            .machine_capacity
            .as_mut()
            .unwrap()
            .active_chat_turns = 0;
        selected(&context);
    }

    fn placement(candidate: &PlacementCandidate) -> WorkspacePlacement {
        WorkspacePlacement {
            id: "placement".to_owned(),
            workspace_id: "workspace".to_owned(),
            task_id: "task".to_owned(),
            agent_id: Some("coder".to_owned()),
            owner_kind: if candidate.location.owner_kind == RepoLocationOwnerKind::Daemon {
                PlacementOwnerKind::Daemon
            } else {
                PlacementOwnerKind::Server
            },
            daemon_id: candidate.location.daemon_id.clone(),
            runtime_id: candidate.location.runtime_id.clone(),
            repo_location_id: candidate.location.id.clone(),
            execution_daemon_id: candidate.execution_daemon_id.clone(),
            workspace_handle: Some("opaque-workspace-handle".to_owned()),
            generation: 4,
            state: PlacementState::Ready,
            selected_by: PlacementSelectedBy::Scheduler,
            selection_reason: "{}".to_owned(),
            reserved_until: None,
            disconnected_at: None,
            failure_cause: None,
            version: 8,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        }
    }

    fn selected(context: &SelectionContext) -> PlacementSelection {
        select_placement(context).into_result().unwrap()
    }

    fn readiness(
        context: &SelectionContext,
        index: usize,
        status: EnvironmentReadinessStatus,
    ) -> ProjectMachineReadiness {
        let mut row = super::super::environment::unknown_record(
            &context.task.project_id,
            EnvironmentMachine::from_location(&context.candidates[index].location),
            &api_types::ProjectEnvironment::default(),
        );
        row.checks_digest = context.environment_digest.clone().unwrap_or_default();
        row.status = status;
        row.failing_checks = vec![db::ReadinessCheckFailure {
            name: "cargo".into(),
            output_tail: "command not found".into(),
        }];
        row
    }

    #[test]
    fn readiness_ready_machine_passes() {
        let mut context = context();
        context.environment_digest = Some("current".into());
        context.candidates[0].environment_readiness =
            Some(readiness(&context, 0, EnvironmentReadinessStatus::Ready));
        assert_eq!(selected(&context).candidate.location.id, "mac-location");
    }

    #[test]
    fn readiness_unfit_machine_rejected_with_checks_and_server_wins() {
        let mut context = context();
        context.environment_digest = Some("current".into());
        context.candidates[0].environment_readiness =
            Some(readiness(&context, 0, EnvironmentReadinessStatus::NotReady));
        context
            .candidates
            .push(candidate(&context, "server-location", None));
        context.candidates[1].environment_readiness =
            Some(readiness(&context, 1, EnvironmentReadinessStatus::Ready));
        let selection = selected(&context);
        assert_eq!(selection.candidate.location.id, "server-location");
        let rejection = &selection.selection_reason.rejected_candidates[0];
        assert_eq!(
            rejection.filter_codes,
            vec![PlacementFilterCode::EnvironmentNotReady]
        );
        assert_eq!(rejection.failing_checks, vec!["cargo"]);
        assert_eq!(
            serde_json::to_value(rejection).unwrap()["filter_codes"][0],
            "environment_not_ready"
        );
    }

    #[test]
    fn machine_capacity_and_environment_failure_do_not_pause_project() {
        let mut context = context();
        context.environment_digest = Some("current".into());
        context.candidates[0].environment_readiness =
            Some(readiness(&context, 0, EnvironmentReadinessStatus::NotReady));
        context.candidates[0]
            .machine_capacity
            .as_mut()
            .unwrap()
            .running_executions = 2;
        let refusal = select_placement(&context).into_result().unwrap_err();
        assert_eq!(
            refusal.rejected_candidates[0].filter_codes,
            vec![
                PlacementFilterCode::MachineCapacity,
                PlacementFilterCode::EnvironmentNotReady
            ]
        );
        assert!(environment_pause_candidates(&context).is_empty());
        assert!(!super::super::machine_precheck::capacity_only_wait(
            &refusal
        ));
        context.candidates[0]
            .machine_capacity
            .as_mut()
            .unwrap()
            .running_executions = 0;
        assert_eq!(environment_pause_candidates(&context).len(), 1);
    }

    #[test]
    fn machine_capacity_wait_ignores_environment_failed_alternative() {
        let mut context = context();
        context.environment_digest = Some("current".into());
        context.candidates[0].environment_readiness =
            Some(readiness(&context, 0, EnvironmentReadinessStatus::NotReady));
        context
            .candidates
            .push(candidate(&context, "server-location", None));
        context.candidates[1].environment_readiness =
            Some(readiness(&context, 1, EnvironmentReadinessStatus::Ready));
        context.candidates[1]
            .machine_capacity
            .as_mut()
            .unwrap()
            .running_executions = 2;
        let refusal = select_placement(&context).into_result().unwrap_err();
        assert!(environment_pause_candidates(&context).is_empty());
        assert!(super::super::machine_precheck::capacity_only_wait(&refusal));
        context.candidates[0].environment_readiness =
            Some(readiness(&context, 0, EnvironmentReadinessStatus::Unknown));
        // Remote unknown readiness is resolved at launch, so it remains a
        // possible free candidate and cannot establish a machine wait.
        assert!(matches!(
            select_placement(&context),
            SelectionOutcome::Selected(_)
        ));
    }

    #[test]
    fn readiness_missing_unknown_and_stale_records_defer() {
        let mut context = context();
        context.candidates = vec![candidate(&context, "server-location", None)];
        context.environment_digest = Some("current".into());
        for row in [
            None,
            Some(readiness(&context, 0, EnvironmentReadinessStatus::Unknown)),
            Some({
                let mut row = readiness(&context, 0, EnvironmentReadinessStatus::Ready);
                row.checks_digest = "old".into();
                row
            }),
        ] {
            context.candidates[0].environment_readiness = row;
            let refusal = select_placement(&context).into_result().unwrap_err();
            assert_eq!(
                refusal.rejected_candidates[0].filter_codes,
                vec![PlacementFilterCode::EnvironmentProbePending]
            );
        }
    }

    #[test]
    fn readiness_no_checks_preserves_selection() {
        let mut context = context();
        context.candidates[0].environment_readiness =
            Some(readiness(&context, 0, EnvironmentReadinessStatus::NotReady));
        assert_eq!(selected(&context).candidate.location.id, "mac-location");
    }

    #[test]
    fn readiness_pinned_unfit_machine_has_no_fallback() {
        let mut context = context();
        context.environment_digest = Some("current".into());
        context.claiming_agent.agent.daemon_id = Some("mac".into());
        context.candidates[0].environment_readiness =
            Some(readiness(&context, 0, EnvironmentReadinessStatus::NotReady));
        context
            .candidates
            .push(candidate(&context, "server-location", None));
        context.candidates[1].environment_readiness =
            Some(readiness(&context, 1, EnvironmentReadinessStatus::Ready));
        let refusal = select_placement(&context).into_result().unwrap_err();
        assert!(refusal
            .rejected_candidates
            .iter()
            .any(|rejection| rejection.daemon_id.as_deref() == Some("mac")
                && rejection.filter_codes == vec![PlacementFilterCode::EnvironmentNotReady]));
    }

    #[test]
    fn readiness_daemon_missing_unknown_and_stale_pass_at_reserve_and_claim() {
        let mut context = context();
        context.environment_digest = Some("current".into());
        for row in [
            None,
            Some(readiness(&context, 0, EnvironmentReadinessStatus::Unknown)),
            Some({
                let mut row = readiness(&context, 0, EnvironmentReadinessStatus::NotReady);
                row.checks_digest = "old".into();
                row
            }),
        ] {
            context.candidates[0].environment_readiness = row;
            context.existing_placement = None;
            let reserved = selected(&context);
            assert_eq!(reserved.candidate.location.id, "mac-location");
            context.existing_placement = Some(placement(&reserved.candidate));
            assert_eq!(
                selected(&context).selection_reason.rule,
                SelectionRule::ExistingPlacement
            );
        }
        context.candidates[0].environment_readiness =
            Some(readiness(&context, 0, EnvironmentReadinessStatus::NotReady));
        for placed in [false, true] {
            context.existing_placement = placed.then(|| placement(&context.candidates[0]));
            assert_eq!(
                select_placement(&context)
                    .into_result()
                    .unwrap_err()
                    .rejected_candidates[0]
                    .filter_codes,
                vec![PlacementFilterCode::EnvironmentNotReady]
            );
        }
    }

    #[test]
    fn readiness_pending_preserves_preference_order() {
        let mut context = context();
        context.environment_digest = Some("current".into());
        context
            .candidates
            .push(candidate(&context, "server-location", None));
        assert!(
            select_placement(&context).into_result().is_err(),
            "preferred server pending cannot divert to daemon"
        );
        context.candidates[0].location.is_default = true;
        assert_eq!(
            selected(&context).candidate.location.id,
            "mac-location",
            "a lower-preference pending host does not block the default daemon"
        );
        context.candidates[1].location.is_default = true;
        context.candidates[0].location.is_default = false;
        assert!(
            select_placement(&context).into_result().is_err(),
            "default server pending preserves preference"
        );
        context.candidates[0].location.is_default = true;
        context.candidates[1].location.is_default = false;
        let mut released = placement(&context.candidates[1]);
        released.state = PlacementState::Failed;
        released.workspace_handle = None;
        context.existing_placement = Some(released);
        assert_eq!(
            selected(&context).candidate.location.id,
            "mac-location",
            "a released failed reservation cannot promote a pending server over the default daemon"
        );
    }

    #[test]
    fn readiness_failures_and_last_resort_pause_apply_to_task_roles() {
        let mut context = context();
        context.environment_digest = Some("current".into());
        context.environment_checks = serde_json::from_value(
            serde_json::json!([{"name":"cargo","command":"false","roles":["reviewer"]}]),
        )
        .unwrap();
        context.candidates[0].environment_readiness =
            Some(readiness(&context, 0, EnvironmentReadinessStatus::NotReady));
        assert_eq!(context.claiming_agent.role, "coder");
        assert!(select_placement(&context).into_result().is_ok());
        assert!(environment_pause_candidates(&context).is_empty());
        context.claiming_agent.role = "reviewer".into();
        assert_eq!(
            select_placement(&context)
                .into_result()
                .unwrap_err()
                .rejected_candidates[0]
                .failing_checks,
            vec!["cargo"]
        );
        assert_eq!(environment_pause_candidates(&context).len(), 1);
    }

    #[test]
    fn readiness_pinned_wait_does_not_pause_an_owner_available_to_other_agents() {
        let mut context = context();
        context.environment_digest = Some("current".into());
        context.claiming_agent.agent.daemon_id = Some("mac".into());
        context.candidates[0].environment_readiness =
            Some(readiness(&context, 0, EnvironmentReadinessStatus::NotReady));
        context.candidates.push(candidate(&context, "server", None));
        context.candidates[1].environment_readiness =
            Some(readiness(&context, 1, EnvironmentReadinessStatus::Ready));
        for executor in context.candidates[1].executors.values_mut() {
            executor.installed = false;
        }
        assert!(
            select_placement(&context).into_result().is_err(),
            "the pinned Task still cannot fall back"
        );
        assert!(
            environment_pause_candidates(&context).is_empty(),
            "other Agents can run on the healthy server"
        );
    }

    #[test]
    fn readiness_asset_projects_use_launch_facts_and_direct_claims_skip_pending() {
        let mut context = context();
        context.environment_digest = Some("current".into());
        context.candidates = vec![candidate(&context, "server", None)];
        context.environment_admission = EnvironmentAdmission::LaunchPreflight;
        assert!(select_placement(&context).into_result().is_ok());
        context.environment_admission = EnvironmentAdmission::Dispatcher;
        assert!(select_placement(&context).into_result().is_err());
        context.environment_has_assets = true;
        assert!(select_placement(&context).into_result().is_ok());
        context.candidates[0].environment_readiness =
            Some(readiness(&context, 0, EnvironmentReadinessStatus::NotReady));
        assert!(
            select_placement(&context).into_result().is_ok(),
            "a primary checkout cannot validate staged assets"
        );
        context.candidates[0]
            .environment_readiness
            .as_mut()
            .unwrap()
            .role = Some("coder".into());
        assert!(
            select_placement(&context).into_result().is_err(),
            "actual launch failures remain placement facts"
        );
    }

    #[test]
    fn readiness_offline_alternative_never_changes_pause_with_cached_handshake() {
        let mut context = context();
        context.environment_digest = Some("current".into());
        context.candidates = vec![
            candidate(&context, "server", None),
            candidate(&context, "offline", Some("offline")),
        ];
        context.candidates[0].environment_readiness =
            Some(readiness(&context, 0, EnvironmentReadinessStatus::NotReady));
        context.candidates[1].connected = false;
        assert_eq!(environment_pause_candidates(&context).len(), 1);
        context.candidates[1].negotiated_revision = None;
        context.candidates[1].workspace_v1 = false;
        context.candidates[1].executors.clear();
        assert_eq!(environment_pause_candidates(&context).len(), 1);
    }

    fn rejected(context: &SelectionContext, code: PlacementFilterCode) -> PlacementUnavailable {
        let error = select_placement(context).into_result().unwrap_err();
        assert!(
            error
                .rejected_candidates
                .iter()
                .any(|candidate| candidate.filter_codes.contains(&code)),
            "{error:?}"
        );
        error
    }

    #[test]
    fn placement_upgrade_refusal_is_transient_with_an_offline_owner_missing_handshake() {
        let mut context = context();
        let upgrade = &mut context.candidates[0];
        upgrade.negotiated_revision = Some(2);
        upgrade.workspace_v1 = false;
        upgrade.allowed_run_purposes.clear();
        for facts in upgrade.executors.values_mut() {
            facts.capabilities = ExecutorAdapterCapabilityFacts::default();
        }
        let mut offline = candidate(&context, "offline-location", Some("offline"));
        offline.connected = false;
        offline.negotiated_revision = None;
        offline.workspace_v1 = false;
        offline.allowed_run_purposes.clear();
        for facts in offline.executors.values_mut() {
            facts.capabilities = ExecutorAdapterCapabilityFacts::default();
        }
        let codes = filter_candidate(&context, &offline);
        assert_eq!(
            codes,
            vec![
                PlacementFilterCode::OwnerUnreachable,
                PlacementFilterCode::WorkspaceProtocolMissing,
                PlacementFilterCode::ExecutorUnavailable,
            ]
        );
        context.candidates.push(offline);
        let refusal = select_placement(&context).into_result().unwrap_err();
        assert!(!refusal.needs_daemon_upgrade());
    }

    #[test]
    fn only_daemon_location_selects_authenticated_cli_roles() {
        let result = selected(&context());
        assert_eq!(result.candidate.execution_daemon_id.as_deref(), Some("mac"));
        assert_eq!(
            result.selection_reason.rule,
            SelectionRule::OnlyEligibleLocation
        );
        assert_eq!(
            serde_json::to_value(result.selection_reason).unwrap(),
            serde_json::json!({"rule": "only_eligible_location", "rejected_candidates": []})
        );
    }

    #[test]
    fn no_compatible_owner_reports_executor_unavailable_without_fallback() {
        let mut context = context();
        context.candidates[0]
            .executors
            .get_mut("coder")
            .unwrap()
            .authenticated = false;
        let error = rejected(&context, PlacementFilterCode::ExecutorUnavailable);
        assert_eq!(error.rejected_candidates.len(), 1);
        assert_eq!(
            error.rejected_candidates[0].daemon_id.as_deref(),
            Some("mac")
        );
        assert_eq!(
            error.rejected_candidates[0].repo_location_id,
            "mac-location"
        );
    }

    #[test]
    fn pinned_agent_selects_its_daemon_and_records_server_rejection() {
        let mut context = context();
        context.claiming_agent.agent.daemon_id = Some("mac".to_owned());
        let mut server = candidate(&context, "server", None);
        server.location.is_default = true;
        context.candidates.push(server);
        let result = selected(&context);
        assert_eq!(result.candidate.execution_daemon_id.as_deref(), Some("mac"));
        assert_eq!(result.selection_reason.rule, SelectionRule::AgentPin);
        assert_eq!(result.selected_by, PlacementSelectedBy::Pin);
        assert_eq!(
            result.selection_reason.rejected_candidates[0].filter_codes,
            vec![PlacementFilterCode::PinMismatch]
        );
    }

    #[test]
    fn pinned_daemon_uses_default_to_break_location_ties() {
        let mut context = context();
        context.claiming_agent.agent.daemon_id = Some("mac".to_owned());
        let mut default = candidate(&context, "z-default", Some("mac"));
        default.location.is_default = true;
        context.candidates.push(default);
        assert_eq!(selected(&context).candidate.location.id, "z-default");
    }

    #[test]
    fn incompatible_pins_of_future_roles_reject_every_owner() {
        let mut context = context();
        context.claiming_agent.agent.daemon_id = Some("mac".to_owned());
        context.worktree_agents[0].agent.daemon_id = Some("linux".to_owned());
        context
            .candidates
            .push(candidate(&context, "linux", Some("linux")));
        let error = rejected(&context, PlacementFilterCode::PinMismatch);
        assert_eq!(error.rejected_candidates.len(), 2);
        assert!(error.rejected_candidates.iter().all(|candidate| candidate
            .filter_codes
            .contains(&PlacementFilterCode::PinMismatch)));
    }

    #[test]
    fn native_reviewer_and_planner_are_rejected_on_daemon_placements() {
        for index in 0..2 {
            let mut context = context();
            context.worktree_agents[index].agent.backend_kind = "native".to_owned();
            rejected(&context, PlacementFilterCode::NativeBackendUnsupported);
        }
        let mut context = context();
        context.claiming_agent.agent.backend_kind = "native".to_owned();
        rejected(&context, PlacementFilterCode::NativeBackendUnsupported);
    }

    #[test]
    fn preparing_reservation_refuses_a_second_agent_claim() {
        let mut context = context();
        context.agent_capacity.reservations = 1;
        rejected(&context, PlacementFilterCode::AgentCapacity);
    }

    #[test]
    fn two_unpinned_running_placements_fill_daemon_session_cap() {
        let mut context = context();
        context.candidates[0]
            .machine_capacity
            .as_mut()
            .unwrap()
            .running_executions = 2;
        assert!(context.agents().all(|role| role.agent.daemon_id.is_none()));
        rejected(&context, PlacementFilterCode::MachineCapacity);
    }

    #[test]
    fn daemon_reservations_and_chat_turns_share_the_session_cap() {
        let mut context = context();
        let capacity = context.candidates[0].machine_capacity.as_mut().unwrap();
        capacity.reservations = 1;
        capacity.active_chat_turns = 1;
        rejected(&context, PlacementFilterCode::MachineCapacity);
    }

    #[test]
    fn all_run_purposes_must_be_allowed() {
        for purpose in [
            WorkspaceRunPurpose::EnvironmentSetup,
            WorkspaceRunPurpose::Hook,
            WorkspaceRunPurpose::CiStep,
        ] {
            let mut context = context();
            context.needed_run_purposes = vec![purpose];
            context.candidates[0]
                .allowed_run_purposes
                .retain(|allowed| *allowed != purpose);
            rejected(&context, PlacementFilterCode::RunPurposeDenied);
            context.candidates[0].allowed_run_purposes.push(purpose);
            selected(&context);
        }
    }

    #[test]
    fn required_run_purposes_follow_review_hooks_and_matching_environment_checks() {
        let settings: ProjectSettings = serde_json::from_value(serde_json::json!({
            "lifecycle_hooks": {"before_work": [{"type": "script", "command": "make setup"}]},
            "environment": {"checks": [{"name": "probe", "command": "command -v rustc", "roles": ["coder"]}]}
        })).unwrap();
        let mut review = ReviewConfig::default();
        review.ci_steps.push("make test".to_owned());
        assert_eq!(
            needed_run_purposes(&review, &settings, &["coder", "reviewer"]),
            vec![
                WorkspaceRunPurpose::EnvironmentSetup,
                WorkspaceRunPurpose::Hook,
                WorkspaceRunPurpose::CiStep
            ]
        );
        assert_eq!(
            needed_run_purposes(&review, &settings, &["planner"]),
            vec![WorkspaceRunPurpose::Hook, WorkspaceRunPurpose::CiStep]
        );
        let settings: ProjectSettings = serde_json::from_value(serde_json::json!({
            "environment": {"env": {"MODE": "test"}, "assets": [{"source": "/model", "target": "model"}]}
        })).unwrap();
        assert!(needed_run_purposes(&ReviewConfig::default(), &settings, &["coder"]).is_empty());
    }

    #[test]
    fn every_required_adapter_fact_is_checked_for_future_roles() {
        for index in 0..5 {
            let mut context = context();
            let facts = &mut context.candidates[0]
                .executors
                .get_mut("reviewer")
                .unwrap()
                .capabilities;
            match index {
                0 => facts.structured_events = false,
                1 => facts.usage = false,
                2 => facts.resume = false,
                3 => facts.cancel_ack = false,
                _ => facts.terminal_observed = false,
            }
            rejected(&context, PlacementFilterCode::CapabilityMissing);
        }
        let mut context = context();
        context.candidates[0]
            .executors
            .get_mut("coder")
            .unwrap()
            .capabilities = ExecutorAdapterCapabilityFacts::default();
        rejected(&context, PlacementFilterCode::CapabilityMissing);
    }

    #[test]
    fn revision_three_non_plan_roles_do_not_require_plan_transport() {
        for role in ["reviewer", "auditor", "interactive"] {
            let mut context = context();
            context.candidates[0].negotiated_revision = Some(3);
            context.candidates[0].plan_transport = false;
            context.worktree_agents.push(context.claiming_agent.clone());
            context.claiming_agent.role = role.into();
            assert!(
                select_placement(&context).into_result().is_ok(),
                "{role} remains supported with a coder assigned"
            );
        }
        let mut context = context();
        context.candidates[0].plan_transport = false;
        rejected(&context, PlacementFilterCode::CapabilityMissing);
    }

    #[test]
    fn offline_owner_missing_handshake_is_a_retryable_placement_refusal() {
        let mut context = context();
        let owner = &mut context.candidates[0];
        owner.connected = false;
        owner.negotiated_revision = None;
        owner.workspace_v1 = false;
        owner.allowed_run_purposes.clear();
        for facts in owner.executors.values_mut() {
            facts.capabilities = ExecutorAdapterCapabilityFacts::default();
        }
        let refusal = select_placement(&context).into_result().unwrap_err();
        assert!(super::super::is_retryable_admission_refusal(
            &crate::ServiceError::PlacementUnavailable(refusal)
        ));

        // Once the handshake arrives, an actual capability/policy refusal
        // requires intervention and must not stay queued indefinitely.
        let owner = &mut context.candidates[0];
        owner.connected = true;
        owner.negotiated_revision = Some(api_types::DAEMON_PROTOCOL_REVISION);
        owner.workspace_v1 = true;
        let refusal = select_placement(&context).into_result().unwrap_err();
        assert!(!super::super::is_retryable_admission_refusal(
            &crate::ServiceError::PlacementUnavailable(refusal)
        ));
    }

    #[test]
    fn owner_protocol_location_executor_and_visibility_filters() {
        type RejectionCase = (PlacementFilterCode, fn(&mut SelectionContext));
        let cases: [RejectionCase; 8] = [
            (PlacementFilterCode::OwnerUnreachable, |context| {
                context.candidates[0].connected = false
            }),
            (PlacementFilterCode::OwnerUnreachable, |context| {
                context.candidates[0].runtime_ready = false
            }),
            (PlacementFilterCode::DaemonUpgradeRequired, |context| {
                context.candidates[0].negotiated_revision = Some(2);
                context.candidates[0].connected = false;
            }),
            (PlacementFilterCode::WorkspaceProtocolMissing, |context| {
                context.candidates[0].workspace_v1 = false
            }),
            (PlacementFilterCode::LocationNotReady, |context| {
                context.candidates[0].location.status = RepoLocationStatus::Unverified
            }),
            (PlacementFilterCode::ExecutorUnavailable, |context| {
                context.candidates[0]
                    .executors
                    .get_mut("coder")
                    .unwrap()
                    .installed = false
            }),
            (PlacementFilterCode::ExecutorUnavailable, |context| {
                context.candidates[0]
                    .executors
                    .get_mut("coder")
                    .unwrap()
                    .enabled = false
            }),
            (PlacementFilterCode::NotVisible, |context| {
                context.candidates[0].visible = false
            }),
        ];
        for (filter, mutate) in cases {
            let mut context = context();
            mutate(&mut context);
            rejected(&context, filter);
        }
    }

    #[test]
    fn shared_mount_requires_current_revision_and_a_ready_location() {
        let mut context = context();
        context.candidates[0].location.owner_kind = RepoLocationOwnerKind::Server;
        context.candidates[0].location.kind = RepoLocationKind::SharedMount;
        context.candidates[0].plan_transport = false;
        context.candidates[0].negotiated_revision = Some(2);
        context.candidates[0].workspace_v1 = false;
        context.candidates[0].connected = false;
        rejected(&context, PlacementFilterCode::DaemonUpgradeRequired);
        context.candidates[0].negotiated_revision = Some(api_types::DAEMON_PROTOCOL_REVISION);
        context.candidates[0].connected = true;
        selected(&context);
        context.candidates[0].location.status = RepoLocationStatus::Unverified;
        rejected(&context, PlacementFilterCode::LocationNotReady);
        context.candidates[0].location.status = RepoLocationStatus::Ready;
        context.candidates[0].location.kind = RepoLocationKind::PrimaryCheckout;
        rejected(&context, PlacementFilterCode::LocationNotReady);
    }

    #[test]
    fn an_embedded_pin_keeps_the_server_as_workspace_owner() {
        let mut context = context();
        let mut server = candidate(&context, "server", None);
        server.execution_daemon_id = Some("embedded".to_owned());
        server.machine_capacity = Some(MachineCapacity::default());
        context.candidates = vec![server];
        context.claiming_agent.agent.daemon_id = Some("embedded".to_owned());
        let selection = selected(&context);
        assert_eq!(
            selection.candidate.location.owner_kind,
            RepoLocationOwnerKind::Server
        );
        assert_eq!(
            selection.candidate.execution_daemon_id.as_deref(),
            Some("embedded")
        );
        assert_eq!(selection.selection_reason.rule, SelectionRule::AgentPin);
    }

    #[test]
    fn a_remote_provider_cannot_reuse_a_server_checkout_without_a_shared_mount() {
        let mut context = context();
        let mut server = candidate(&context, "server", None);
        server.execution_daemon_id = Some("mac".to_owned());
        server.embedded_execution = false;
        server.machine_capacity = Some(MachineCapacity::default());
        context.existing_placement = Some(placement(&server));
        context.candidates = vec![server];
        context.claiming_agent.agent.daemon_id = Some("mac".to_owned());
        rejected(&context, PlacementFilterCode::LocationNotReady);
    }

    #[test]
    fn preference_order_is_default_then_server_then_created_at_and_id() {
        let mut context = context();
        let mut default = candidate(&context, "default", Some("linux"));
        default.location.is_default = true;
        let server = candidate(&context, "server", None);
        context.candidates.extend([default, server]);
        let result = selected(&context);
        assert_eq!(result.candidate.location.id, "default");
        assert_eq!(result.selection_reason.rule, SelectionRule::DefaultLocation);
        context.candidates[1].location.is_default = false;
        let result = selected(&context);
        assert_eq!(result.candidate.location.id, "server");
        assert_eq!(result.selection_reason.rule, SelectionRule::ServerOwned);
        context.candidates.pop();
        context.candidates[1].location.created_at = "2026-09-28T00:00:00Z".to_owned();
        assert_eq!(selected(&context).candidate.location.id, "default");
        context.candidates[1].location.created_at = NOW.to_owned();
        let result = selected(&context);
        assert_eq!(result.candidate.location.id, "default");
        assert_eq!(
            result.selection_reason.rule,
            SelectionRule::DeterministicOrder
        );
        context.candidates.reverse();
        assert_eq!(selected(&context).candidate.location.id, "default");
    }

    #[test]
    fn re_review_reuses_the_existing_owner_handle_and_generation() {
        let mut context = context();
        let existing = placement(&context.candidates[0]);
        context.existing_placement = Some(existing.clone());
        let mut default = candidate(&context, "server", None);
        default.location.is_default = true;
        context.candidates.push(default);
        let result = selected(&context);
        assert_eq!(result.candidate.execution_daemon_id.as_deref(), Some("mac"));
        assert_eq!(result.reused_placement, Some(existing));
        assert_eq!(
            result.selection_reason.rule,
            SelectionRule::ExistingPlacement
        );
    }

    #[test]
    fn subtask_inherits_root_placement_and_rejects_an_incompatible_agent() {
        let mut context = context();
        context.task.parent_task_id = Some("root".to_owned());
        let inherited = placement(&context.candidates[0]);
        context.inherited_root_placement = Some(inherited.clone());
        context.candidates.push(candidate(&context, "server", None));
        let result = selected(&context);
        assert_eq!(result.reused_placement, Some(inherited));
        assert_eq!(
            result.selection_reason.rule,
            SelectionRule::InheritedRootPlacement
        );
        assert_eq!(result.selected_by, PlacementSelectedBy::Inherited);
        context.claiming_agent.agent.daemon_id = Some("other".to_owned());
        rejected(&context, PlacementFilterCode::PinMismatch);
    }

    #[test]
    fn existing_precedes_inherited_and_pin_and_does_not_fall_back_when_offline() {
        let mut context = context();
        context.existing_placement = Some(placement(&context.candidates[0]));
        context.claiming_agent.agent.daemon_id = Some("mac".to_owned());
        let server = candidate(&context, "server", None);
        context.inherited_root_placement = Some(placement(&server));
        context.candidates.push(server);
        assert_eq!(
            selected(&context).selection_reason.rule,
            SelectionRule::ExistingPlacement
        );
        context.candidates[0].connected = false;
        let error = rejected(&context, PlacementFilterCode::OwnerUnreachable);
        assert_eq!(error.rejected_candidates.len(), 2);
        assert!(error
            .rejected_candidates
            .iter()
            .find(|candidate| candidate.repo_location_id == "server")
            .unwrap()
            .filter_codes
            .contains(&PlacementFilterCode::PinMismatch));
    }

    #[test]
    fn disconnected_placement_waits_for_reconciliation_even_with_a_live_socket() {
        let mut context = context();
        let mut existing = placement(&context.candidates[0]);
        existing.state = PlacementState::Disconnected;
        context.existing_placement = Some(existing);
        context.candidates.push(candidate(&context, "server", None));
        rejected(&context, PlacementFilterCode::OwnerUnreachable);
    }

    #[test]
    fn failed_unprepared_reservation_can_be_reselected() {
        let mut context = context();
        let mut failed = placement(&context.candidates[0]);
        failed.state = PlacementState::Failed;
        failed.workspace_handle = None;
        context.existing_placement = Some(failed);
        context.candidates[0].connected = false;
        context.candidates.push(candidate(&context, "server", None));
        assert_eq!(selected(&context).candidate.location.id, "server");
    }

    #[test]
    fn in_flight_reservation_cannot_move_before_its_state_releases_it() {
        for state in [PlacementState::Reserved, PlacementState::Preparing] {
            let mut context = context();
            let mut existing = placement(&context.candidates[0]);
            existing.state = state;
            existing.workspace_handle = None;
            context.existing_placement = Some(existing);
            context.candidates[0].connected = false;
            context.candidates.push(candidate(&context, "server", None));
            rejected(&context, PlacementFilterCode::OwnerUnreachable);
        }
    }

    #[test]
    fn rejection_evidence_includes_all_failed_candidates_and_filter_codes() {
        let mut context = context();
        context.candidates[0].connected = false;
        context.candidates[0].workspace_v1 = false;
        context.candidates[0].visible = false;
        context.candidates.push(candidate(&context, "server", None));
        let result = selected(&context);
        assert_eq!(result.selection_reason.rejected_candidates.len(), 1);
        assert_eq!(
            result.selection_reason.rejected_candidates[0].filter_codes,
            vec![
                PlacementFilterCode::OwnerUnreachable,
                PlacementFilterCode::WorkspaceProtocolMissing,
                PlacementFilterCode::NotVisible
            ]
        );
        context.candidates[1].location.status = RepoLocationStatus::Invalid;
        let error = rejected(&context, PlacementFilterCode::LocationNotReady);
        assert_eq!(error.rejected_candidates.len(), 2);
    }

    #[test]
    fn an_empty_candidate_set_is_structured_unavailable() {
        let mut context = context();
        context.candidates.clear();
        let error = select_placement(&context).into_result().unwrap_err();
        assert_eq!(error.task_id, "task");
        assert_eq!(error.repo_id, "repo");
        assert!(error.rejected_candidates.is_empty());
    }

    fn load_input<'a>(
        context: &'a SelectionContext,
        server: &'a ServerFacts,
        review: &'a ReviewConfig,
        settings: &'a ProjectSettings,
        handshakes: &'a BTreeMap<String, ConnectionHandshake>,
    ) -> SelectionLoadInput<'a> {
        SelectionLoadInput {
            task: &context.task,
            repo: &context.repo,
            claiming_agent: &context.claiming_agent,
            worktree_agents: &context.worktree_agents,
            task_owner_id: Some("owner"),
            workspace_id: None,
            inherited_root_workspace_id: None,
            review_config: review,
            project_settings: settings,
            server,
            handshakes,
            fallback_server_location: None,
        }
    }

    #[tokio::test]
    async fn loader_uses_transactional_runtime_facts_and_current_connection_handshake() {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = SqliteDb::new(pool);
        db.server_run_cap
            .set(Some(0), 0, &config::embedded_machine_id());
        sqlx::raw_sql(
            "INSERT INTO daemon (id, machine_id, hostname, os, arch, status, owner_id,
                                 visibility, detected_clis_json, labels_json, created_at, updated_at, max_concurrent_runs)
             VALUES ('mac', 'mac-machine', 'Mac', 'macos', 'aarch64', 'online', NULL,
                     'global', '[{\"kind\":\"codex\",\"availability\":\"authenticated\"}]',
                     '{}', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z', 2);
             INSERT INTO runtime (id, daemon_id, kind, workspace_root, status, created_at, updated_at)
             VALUES ('runtime-mac', 'mac', 'local', '/checkout', 'ready',
                     '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z');",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let context = context();
        let location = &context.candidates[0].location;
        db::RepoLocationRepo::create(
            &db,
            db::CreateRepoLocation {
                id: location.id.clone(),
                repo_id: location.repo_id.clone(),
                owner_kind: location.owner_kind.clone(),
                daemon_id: location.daemon_id.clone(),
                runtime_id: location.runtime_id.clone(),
                path: location.path.clone(),
                kind: location.kind.clone(),
                is_default: false,
                status: RepoLocationStatus::Ready,
                last_verified_at: Some(NOW.to_owned()),
                last_error: None,
                created_at: NOW.to_owned(),
                updated_at: NOW.to_owned(),
            },
        )
        .await
        .unwrap();
        let registry = DaemonConnectionRegistry::without_handlers();
        let (connection, _outbound) =
            crate::daemon_transport::DaemonConnection::new("mac".to_owned());
        registry.register("mac".to_owned(), connection.clone());
        let handshake = DaemonHandshakeNotification {
            protocol_revision: api_types::DAEMON_PROTOCOL_REVISION,
            capabilities: api_types::DAEMON_REQUIRED_CAPABILITIES
                .iter()
                .map(|name| (*name).to_owned())
                .chain([
                    api_types::DAEMON_CAPABILITY_WORKSPACE.to_owned(),
                    api_types::DAEMON_CAPABILITY_PLAN_TRANSPORT.to_owned(),
                ])
                .collect(),
            executor_capabilities: BTreeMap::from([("codex".to_owned(), capabilities())]),
            workspace_run_policy: api_types::WorkspaceRunPolicy {
                allowed_purposes: vec![WorkspaceRunPurpose::CiStep],
            },
        };
        registry.dispatch_incoming_for_connection(
            "mac",
            connection.id(),
            api_types::DaemonFrame::Notification {
                method: api_types::METHOD_DAEMON_HANDSHAKE.to_owned(),
                params: serde_json::to_value(&handshake).unwrap(),
            },
        );
        let handshakes = BTreeMap::from([(
            "mac".to_owned(),
            ConnectionHandshake {
                connection_id: connection.id(),
                handshake,
            },
        )]);
        let server = ServerFacts::default();
        let settings: ProjectSettings = serde_json::from_str("{}").unwrap();
        let review = ReviewConfig {
            ci_steps: vec!["make test".to_owned()],
            ..ReviewConfig::default()
        };
        let mut transaction = db::begin_immediate(db.pool()).await.unwrap();
        let loaded = load_selection_context(
            &db,
            &mut transaction,
            &registry,
            load_input(&context, &server, &review, &settings, &handshakes),
        )
        .await
        .unwrap();
        assert_eq!(
            selected(&loaded).candidate.execution_daemon_id.as_deref(),
            Some("mac")
        );
        assert_eq!(
            loaded.candidates[0]
                .machine_capacity
                .unwrap()
                .max_concurrent_runs,
            Some(2)
        );

        let mut stale_handshakes = handshakes.clone();
        stale_handshakes.get_mut("mac").unwrap().connection_id += 1;
        let loaded = load_selection_context(
            &db,
            &mut transaction,
            &registry,
            load_input(&context, &server, &review, &settings, &stale_handshakes),
        )
        .await
        .unwrap();
        rejected(&loaded, PlacementFilterCode::WorkspaceProtocolMissing);

        sqlx::query("UPDATE runtime SET status = 'degraded' WHERE id = 'runtime-mac'")
            .execute(&mut *transaction)
            .await
            .unwrap();
        let loaded = load_selection_context(
            &db,
            &mut transaction,
            &registry,
            load_input(&context, &server, &review, &settings, &handshakes),
        )
        .await
        .unwrap();
        rejected(&loaded, PlacementFilterCode::OwnerUnreachable);
    }
    #[test]
    fn environment_and_capacity_refusals_are_not_deterministic() {
        let refusal = |codes: Vec<PlacementFilterCode>| PlacementUnavailable {
            task_id: "task".to_owned(),
            repo_id: "repo".to_owned(),
            rejected_candidates: vec![CandidateRejection {
                failing_checks: Vec::new(),
                repo_location_id: "location".to_owned(),
                owner_kind: "server".to_owned(),
                daemon_id: None,
                runtime_id: None,
                filter_codes: codes,
            }],
        };
        for code in [
            PlacementFilterCode::EnvironmentProbePending,
            PlacementFilterCode::EnvironmentNotReady,
            PlacementFilterCode::AgentCapacity,
            PlacementFilterCode::MachineCapacity,
            PlacementFilterCode::OwnerUnreachable,
        ] {
            assert!(!refusal(vec![code]).is_deterministic(), "{code:?}");
        }
        assert!(refusal(vec![PlacementFilterCode::CapabilityMissing]).is_deterministic());
    }
}
